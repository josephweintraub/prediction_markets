"""Read-only exhaustive census of published Polymarket wallet-pair structure.

Only scalar aggregates leave DuckDB. Published values retain their exact strings,
numbers and multiplicities; they never become native execution identities.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import audit_polymarket_lineage as lineage

SPEC_PATH = "docs/analysis_specs/polymarket_wallet_pair_census_v1.json"
SOURCE_PATHS = ("scripts/audit_polymarket_wallet_pairs.py",
                "tests/test_polymarket_wallet_pairs.py",
                "scripts/audit_polymarket_lineage.py", SPEC_PATH)
EXPECTED_ROWS = {"root": 2_114_623_452, "clean": 2_036_128_538}
MAX_ROWS = 25_000_000
MAX_PAYLOAD = 16 * 1024**3
MAX_OUTPUT = 256 * 1024**2
MAX_SUMMARY = 1024**2
MAX_READ_FOOTPRINT = 8 * 1024**4
MAX_ADMISSION_NODES = 4096
COMMON = ("timestamp", "conditionId", "usdcSize", "price", "outcome", "eventSlug", "year_month")
WALLETS = ("proxyWallet", "counterparty", "side")
CATEGORIES = ("correct_only", "copied_only", "both_compatible", "neither")
OPTIMIZERS_DISABLED = "common_subplan"
CAPS = {"duckdb_memory_limit": "64GB", "threads": 4, "disk_spill": "0B",
        "maximum_rows_per_relation_leaf": MAX_ROWS,
        "maximum_combined_logical_payload_bytes_per_leaf": MAX_PAYLOAD,
        "maximum_compact_output_bytes": MAX_OUTPUT,
        "maximum_planned_read_footprint_bytes": MAX_READ_FOOTPRINT,
        "maximum_admission_nodes": MAX_ADMISSION_NODES,
        "no_arrow_body_materialization": True,
        "disabled_optimizers": OPTIMIZERS_DISABLED}


def columns(fields) -> str:
    return ",".join(lineage.qname(field) for field in fields)


def exact_json(value) -> bytes:
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False).encode("utf-8") + b"\n"


def require_expected_head(actual: str, expected: str) -> None:
    if len(expected) != 40 or any(c not in "0123456789abcdef" for c in expected):
        raise lineage.AuditBlocked("expected HEAD must be a full lowercase commit hash")
    if actual != expected:
        raise lineage.AuditBlocked("canonical HEAD differs from the root-approved expected HEAD")


def read_reviewed_manifest(path: Path, expected_sha256: str) -> tuple[dict, str]:
    if len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256):
        raise lineage.AuditBlocked("approved manifest SHA256 must be 64 lowercase hexadecimal characters")
    encoded = path.read_bytes()
    actual = hashlib.sha256(encoded).hexdigest()
    if actual != expected_sha256:
        raise lineage.AuditBlocked("reviewed manifest differs from approved SHA256")
    value = json.loads(encoded)
    if not isinstance(value, dict):
        raise lineage.AuditBlocked("reviewed manifest must be a JSON object")
    return value, actual


def load_contract() -> tuple[dict, str]:
    encoded = (ROOT / SPEC_PATH).read_bytes()
    contract = json.loads(encoded)
    for key, value in CAPS.items():
        if key == "disabled_optimizers":
            continue  # Defensive runtime setting is recorded independently of the spec.
        if contract["execution"].get(key) != value:
            raise lineage.AuditBlocked("runtime caps differ from the frozen contract")
    if contract["units"]["common_class"] != list(COMMON):
        raise lineage.AuditBlocked("common class differs from the frozen contract")
    return contract, hashlib.sha256(encoded).hexdigest()


def source_snapshot() -> dict:
    hashes = {}
    for path in SOURCE_PATHS:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", path], cwd=ROOT,
                                 capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", path], cwd=ROOT)
        if tracked.returncode or unchanged.returncode:
            raise lineage.AuditBlocked("census sources and contract must be committed and unchanged")
        hashes[path] = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
    return hashes


def connection():
    con = lineage.connection("64GB")
    # DuckDB 1.5.0 can alias wallet projections in a shared UNION subplan. Keep
    # this census independent of that reproduced transformation defect.
    available = con.execute("SELECT name FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone()
    if available:
        con.execute(f"SET disabled_optimizers='{OPTIMIZERS_DISABLED}'")
    return con


def runtime_settings(con) -> dict:
    return lineage.scalar(con, """SELECT current_setting('memory_limit') memory_limit,
        current_setting('threads') threads,current_setting('max_temp_directory_size') max_temp_directory_size,
        current_setting('temp_directory') temp_directory,current_setting('disabled_optimizers') disabled_optimizers,
        current_setting('TimeZone') timezone,
        EXISTS(SELECT 1 FROM duckdb_optimizers() WHERE name='common_subplan') common_subplan_available""")


def scan_plan(con, query: str, *, empty_overlap: bool = False) -> dict:
    encoded = con.execute("EXPLAIN (FORMAT JSON) " + query).fetchone()[1]
    parsed = json.loads(encoded)

    def operators(value, name):
        if isinstance(value, list):
            return sum(operators(item, name) for item in value)
        if isinstance(value, dict):
            return int(value.get("name", "").strip().upper() == name) + sum(
                operators(item, name) for key, item in value.items() if key == "children")
        return 0

    count = operators(parsed, "READ_PARQUET")
    proved_empty = count == 0 and empty_overlap and operators(parsed, "EMPTY_RESULT") > 0
    if count != 1 and not proved_empty:
        raise lineage.AuditBlocked("original-data query requires one Parquet scan, or a footer-proved EMPTY_RESULT")
    return {"parquet_scan_operators": count, "footer_proved_empty_result": proved_empty,
            "physical_plan_sha256": hashlib.sha256(encoded.encode()).hexdigest()}


def verify_frozen(infos: list[dict]) -> None:
    lineage.verify_snapshots(infos)
    for info in infos:
        if lineage.pq.ParquetFile(info["path"]).metadata.created_by != info["created_by"]:
            raise lineage.AuditBlocked("frozen Parquet creator changed after reads")


def overlap(infos: list[dict], lower: int, upper: int) -> dict:
    references, rows, compressed = [], 0, 0
    for info in infos:
        indices = []
        for group in info["row_groups"]:
            stat = group["stats"]["timestamp"]
            if stat["min"] < upper and stat["max"] >= lower:
                indices.append(group["index"])
                rows += group["rows"]
                compressed += group["compressed_bytes"]
        if indices:
            references.append({"path": info["path"], "row_group_indices": indices})
    return {"references": references, "overlapping_physical_rows": rows,
            "overlapping_compressed_bytes": compressed}


def preflight(inputs: dict, expected_head: str, source_hashes: dict, contract: dict,
              contract_sha256: str) -> dict:
    inventories, proofs, layouts, totals = {}, {}, {}, {}
    for name, relation in (("root", "root_transformed"), ("clean", "clean")):
        infos, proof, layout = lineage.published_month_inventory(inputs[name], relation)
        for info in infos:
            info["created_by"] = lineage.pq.ParquetFile(info["path"]).metadata.created_by
        if len(infos) != len(lineage.canonical_months()):
            raise lineage.AuditBlocked("frozen published layout requires exactly one file per month")
        inventories[name], proofs[name], layouts[name] = infos, proof, layout
        totals[name] = sum(info["rows"] for info in infos)
        if totals[name] != EXPECTED_ROWS[name]:
            raise lineage.AuditBlocked("published footer row total differs from the frozen count")
    plan = []
    for month in lineage.canonical_months():
        current = {name: [info for info in infos if info["partition_month"] == month]
                   for name, infos in inventories.items()}
        start, end = lineage.month_bounds(month)
        small = all(sum(info["rows"] for info in infos) <= MAX_ROWS for infos in current.values())
        intervals = [(start, end)] if small else [(value, min(value + 86400, end))
                                                   for value in range(start, end, 86400)]
        for lower, upper in intervals:
            selection = {name: overlap(infos, lower, upper) for name, infos in current.items()}
            footprint = sum(item["overlapping_compressed_bytes"] for item in selection.values())
            seconds = upper - lower
            plan.append({"month": month, "lower_inclusive": lower, "upper_exclusive": upper,
                         "initial_unit": "month" if small else "day", "selections": selection,
                         "planned_initial_two_pass_compressed_bytes": 2 * footprint,
                         "maximum_bisection_depth": math.ceil(math.log2(seconds)),
                         # Each count-tree node and each admitted leaf can, in
                         # the worst case, overlap every initially selected group.
                         "conservative_recursive_compressed_upper_bound_bytes": (3 * seconds - 1) * footprint})
    verify_frozen([info for infos in inventories.values() for info in infos])
    con = connection()
    try:
        settings = runtime_settings(con)
        for item in plan:
            leaf_views(con, inventories, item["month"], item["lower_inclusive"], item["upper_exclusive"])
            item["source_scan_plans"] = {name: {
                "admission": scan_plan(con, support_query(name + "_leaf", item["month"]),
                    empty_overlap=item["selections"][name]["overlapping_physical_rows"] == 0),
                "grouping": scan_plan(con, grouping_query(name),
                    empty_overlap=item["selections"][name]["overlapping_physical_rows"] == 0)} for name in ("root", "clean")}
    finally:
        con.close()
    return {"schema_version": 1, "status": "preflight_complete", "data_certified": False,
            "contract": contract, "contract_sha256": contract_sha256,
            "expected_head": expected_head, "source_sha256": source_hashes, "inputs": inputs,
            "caps": CAPS, "inventories": inventories, "published_month_proofs": proofs,
            "published_directory_layouts": layouts, "footer_rows": totals, "initial_plan": plan,
            "environment": {"python": sys.version, "platform": sys.platform,
                            "duckdb": lineage.duckdb.__version__, "pyarrow": lineage.pa.__version__,
                            "runtime_settings": settings},
            "io_plan": {"planned_initial_two_pass_compressed_bytes": sum(
                item["planned_initial_two_pass_compressed_bytes"] for item in plan),
                "conservative_recursive_compressed_upper_bound_bytes": sum(
                item["conservative_recursive_compressed_upper_bound_bytes"] for item in plan),
                "note": "Overlapping compressed footprints are planned reads, not measured physical I/O. Admission reads each original relation once; admitted grouping reads it once more. Bisection adds count passes, recorded during execution; the worst-case bound assumes every integer second needs a leaf. Footers and final reopen are separate."},
            "preflight_peak_rss_bytes": lineage.peak_rss_bytes()}


def verify_reviewed(reviewed: dict, fresh: dict) -> None:
    if reviewed.get("status") != "preflight_complete" or fresh.get("status") != "preflight_complete":
        raise lineage.AuditBlocked("body requires a complete reviewed preflight")
    for field in ("schema_version", "data_certified", "contract", "contract_sha256", "expected_head",
                  "source_sha256", "inputs", "caps", "inventories", "published_month_proofs",
                  "published_directory_layouts", "footer_rows", "initial_plan", "environment", "io_plan"):
        if reviewed.get(field) != fresh.get(field):
            raise lineage.AuditBlocked(f"fresh {field} differs from the reviewed preflight")


def leaf_views(con, infos: dict, month: str, lower: int, upper: int) -> None:
    for name in ("root", "clean"):
        current = [info for info in infos[name] if info["partition_month"] == month]
        if len(current) != 1:
            raise lineage.AuditBlocked("leaf requires one frozen file per relation/month")
        info = current[0]
        selected = ",".join(lineage.qname(field) if field != "year_month" or field in info["fields"]
                            else lineage.literal(month) + ' AS "year_month"'
                            for field in lineage.VALUE_FIELDS)
        con.execute(f"DROP VIEW IF EXISTS {name}_leaf")
        con.execute(f"CREATE TEMP VIEW {name}_leaf AS SELECT {selected} FROM "
                    f"read_parquet({lineage.literal(info['path'])},hive_partitioning=false) "
                    f"WHERE timestamp>={lower} AND timestamp<{upper}")


def support_query(relation: str, month: str) -> str:
    strings = [field for field in lineage.VALUE_FIELDS if field not in
               {"timestamp", "usdcSize", "price", "is_maker"}]
    lengths = "+".join(f"coalesce(octet_length(encode({lineage.qname(field)})),0)" for field in strings)
    return f"""SELECT count(*) row_count,
        count(*) FILTER(WHERE is_maker) maker_rows,
        count(*) FILTER(WHERE NOT is_maker) nonmaker_rows,
        count(*) FILTER(WHERE is_maker IS NULL) missing_role_rows,
        count(*) FILTER(WHERE proxyWallet=counterparty) self_wallet_rows,
        count(*) FILTER(WHERE eventSlug IS NULL) null_event_label_rows,
        count(*) FILTER(WHERE eventSlug IS NOT NULL AND trim(eventSlug)='') blank_event_label_rows,
        coalesce(fsum(usdcSize),0) gross_recorded_cash,
        coalesce(sum(({lengths})::HUGEINT),0)+83*count(*) logical_payload_bytes,
        count(*) FILTER(WHERE proxyWallet IS NULL OR trim(proxyWallet)='' OR
          counterparty IS NULL OR trim(counterparty)='' OR conditionId IS NULL OR trim(conditionId)='' OR
          outcome IS NULL OR trim(outcome)='' OR timestamp IS NULL OR timestamp<0 OR
          side IS NULL OR side NOT IN ('BUY','SELL') OR is_maker IS NULL OR
          usdcSize IS NULL OR NOT isfinite(usdcSize) OR usdcSize<=0 OR
          price IS NULL OR NOT isfinite(price) OR price<=0 OR price>1 OR
          year_month IS DISTINCT FROM {lineage.literal(month)}) invalid_rows
        FROM {relation}"""


def leaf_support(con, relation: str, month: str) -> dict:
    result = lineage.scalar(con, support_query(relation, month))
    if not math.isfinite(result["gross_recorded_cash"]):
        result["gross_recorded_cash"] = None
        result["invalid_rows"] += 1
    return result


def grouping_query(name: str) -> str:
    values = columns(lineage.VALUE_FIELDS)
    return f"SELECT {values},count(*)::BIGINT multiplicity FROM {name}_leaf GROUP BY {values}"


def admitted(support: dict) -> bool:
    if any(value["invalid_rows"] for value in support.values()):
        raise lineage.AuditBlocked("published row integrity failed; irregular rows were counted")
    return (all(value["row_count"] <= MAX_ROWS for value in support.values()) and
            sum(value["logical_payload_bytes"] for value in support.values()) <= MAX_PAYLOAD)


def split_seconds(lower: int, upper: int) -> tuple[tuple[int, int], tuple[int, int]]:
    if upper - lower <= 1:
        raise lineage.AuditBlocked("one complete UTC second exceeds the leaf resource caps")
    middle = (lower + upper) // 2
    return (lower, middle), (middle, upper)


def pair_metrics(con, relation: str, *, omit_label: bool = False) -> dict:
    """Compare exact class capacities; never pair individual published rows."""
    common = tuple(field for field in COMMON if not omit_label or field != "eventSlug")
    fields = common + WALLETS
    selected = columns(fields)
    prefix = columns(common)
    # The grouped input is materialized; no Parquet body is reread here.
    con.execute("DROP TABLE IF EXISTS pair_capacity")
    con.execute(f"""CREATE TEMP TABLE pair_capacity AS SELECT {selected},
      sum(n)::BIGINT observed_n,sum(c)::BIGINT correct_n,sum(u)::BIGINT copied_n,
      sum(maker_classes)::BIGINT maker_value_classes FROM (
      SELECT {prefix},proxyWallet,counterparty,side,multiplicity n,0::BIGINT c,0::BIGINT u,
        0::BIGINT maker_classes FROM {relation} WHERE NOT is_maker
      UNION ALL SELECT {prefix},counterparty proxyWallet,proxyWallet counterparty,
        CASE WHEN side='BUY' THEN 'SELL' ELSE 'BUY' END side,0,multiplicity,0,1
        FROM {relation} WHERE is_maker
      UNION ALL SELECT {prefix},proxyWallet,counterparty,
        CASE WHEN side='BUY' THEN 'SELL' ELSE 'BUY' END side,0,0,multiplicity,0
        FROM {relation} WHERE is_maker) GROUP BY {selected}""")
    try:
        row_metrics = lineage.scalar(con, """SELECT
          coalesce(sum(observed_n),0) nonmaker_rows,coalesce(sum(correct_n),0) maker_rows,
          coalesce(sum(greatest(observed_n-correct_n,0)),0) excess_observed_correct,
          coalesce(sum(greatest(correct_n-observed_n,0)),0) missing_observed_correct,
          coalesce(sum(greatest(observed_n-copied_n,0)),0) excess_observed_copied,
          coalesce(sum(greatest(copied_n-observed_n,0)),0) missing_observed_copied,
          coalesce(sum(least(observed_n,correct_n)),0) matched_correct,
          coalesce(sum(least(observed_n,copied_n)),0) matched_copied,
          coalesce(sum(least(correct_n,copied_n)),0) shared_candidate_capacity,
          coalesce(sum(least(observed_n,correct_n,copied_n)),0) observed_shared_candidate_capacity,
          coalesce(sum(observed_n) FILTER(WHERE correct_n>0 AND copied_n>0),0) observed_rows_in_shared_candidate_classes,
          count(*) FILTER(WHERE observed_n>0 AND correct_n>0 AND copied_n>0) observed_shared_candidate_classes
          FROM pair_capacity""")
        con.execute("DROP TABLE IF EXISTS common_capacity")
        con.execute(f"""CREATE TEMP TABLE common_capacity AS SELECT {prefix},
          sum(observed_n)::BIGINT nonmaker_rows,sum(correct_n)::BIGINT maker_rows,
          count(*) FILTER(WHERE correct_n>0)::BIGINT maker_value_classes,
          count(*) FILTER(WHERE observed_n>0)::BIGINT nonmaker_value_classes,
          sum(greatest(observed_n-correct_n,0)) correct_excess,
          sum(greatest(correct_n-observed_n,0)) correct_missing,
          sum(greatest(observed_n-copied_n,0)) copied_excess,
          sum(greatest(copied_n-observed_n,0)) copied_missing FROM pair_capacity GROUP BY {prefix}""")
        common_metrics = lineage.scalar(con, """SELECT count(*) common_classes,
          count(*) FILTER(WHERE maker_rows<>nonmaker_rows) unequal_role_count_classes,
          count(*) FILTER(WHERE maker_rows=0) empty_maker_classes,
          count(*) FILTER(WHERE nonmaker_rows=0) empty_nonmaker_classes,
          count(*) FILTER(WHERE maker_rows=1 AND nonmaker_rows=1) singleton_role_classes,
          count(*) FILTER(WHERE maker_rows>1 OR nonmaker_rows>1) multioccurrence_classes,
          count(*) FILTER(WHERE maker_value_classes>1) multiple_maker_value_classes,
          count(*) FILTER(WHERE nonmaker_value_classes>1) multiple_nonmaker_value_classes
          FROM common_capacity""")
        categories = {}
        for category, predicate in (
            ("correct_only", "correct_excess=0 AND correct_missing=0 AND (copied_excess<>0 OR copied_missing<>0)"),
            ("copied_only", "copied_excess=0 AND copied_missing=0 AND (correct_excess<>0 OR correct_missing<>0)"),
            ("both_compatible", "correct_excess=0 AND correct_missing=0 AND copied_excess=0 AND copied_missing=0"),
            ("neither", "(correct_excess<>0 OR correct_missing<>0) AND (copied_excess<>0 OR copied_missing<>0)")):
            categories[category] = lineage.scalar(con, f"""SELECT count(*) common_classes,
                coalesce(sum(maker_rows),0) maker_rows,coalesce(sum(nonmaker_rows),0) nonmaker_rows,
                count(*) FILTER(WHERE maker_rows=1 AND nonmaker_rows=1) singleton_role_classes
                FROM common_capacity WHERE {predicate}""")
        result = {**row_metrics, **common_metrics, "compatibility": categories}
        if (sum(value["common_classes"] for value in categories.values()) != result["common_classes"] or
                sum(value["nonmaker_rows"] for value in categories.values()) != result["nonmaker_rows"] or
                sum(value["maker_rows"] for value in categories.values()) != result["maker_rows"] or
                result["matched_correct"] + result["excess_observed_correct"] != result["nonmaker_rows"] or
                result["matched_correct"] + result["missing_observed_correct"] != result["maker_rows"] or
                result["matched_copied"] + result["excess_observed_copied"] != result["nonmaker_rows"] or
                result["matched_copied"] + result["missing_observed_copied"] != result["maker_rows"]):
            raise lineage.AuditBlocked("pair-class count reconciliation failed")
        return result
    finally:
        con.execute("DROP TABLE IF EXISTS common_capacity")
        con.execute("DROP TABLE IF EXISTS pair_capacity")


def begin_admission(budget: dict) -> None:
    if budget["admission_nodes"] >= MAX_ADMISSION_NODES:
        raise lineage.AuditBlocked("admission-node budget exhausted before the next original-data query")
    budget["admission_nodes"] += 1


def reserve_read(budget: dict, stage: str, name: str, footprint: int) -> None:
    used = budget["admission_compressed_footprint_bytes"] + budget["grouping_compressed_footprint_bytes"]
    if footprint < 0 or stage not in {"admission", "grouping"}:
        raise lineage.AuditBlocked("invalid original-data read budget charge")
    if used + footprint > MAX_READ_FOOTPRINT:
        budget["blocked_original_query"] = {"stage": stage, "relation": name,
            "requested_compressed_footprint_bytes": footprint,
            "previously_reserved_compressed_footprint_bytes": used}
        raise lineage.AuditBlocked("planned original-data read budget exhausted before the next query")
    budget[stage + "_compressed_footprint_bytes"] += footprint
    budget["planned_original_read_footprint_bytes"] = used + footprint


def audit_leaf(con, support: dict, *, read_budget: dict | None = None,
               footprints: dict | None = None) -> dict:
    if not admitted(support):
        raise lineage.AuditBlocked("leaf was not admitted by scalar resource gates")
    values = columns(lineage.VALUE_FIELDS)
    try:
        for name in ("root", "clean"):
            if read_budget is not None:
                reserve_read(read_budget, "grouping", name, footprints[name])
            con.execute(f"CREATE TEMP TABLE {name}_groups AS " + grouping_query(name))
        cleaning = lineage.scalar(con, f"""SELECT
          (SELECT coalesce(sum(multiplicity-1),0) FROM root_groups) root_value_row_surplus,
          (SELECT coalesce(sum(multiplicity-1),0) FROM clean_groups) clean_value_row_surplus,
          (SELECT count(*) FROM root_groups) root_distinct_rows,
          (SELECT coalesce(fsum(usdcSize),0) FROM root_groups) root_distinct_gross_recorded_cash,
          (SELECT count(*) FROM (SELECT {values} FROM root_groups EXCEPT SELECT {values} FROM clean_groups)) expected_clean_only_rows,
          (SELECT count(*) FROM (SELECT {values} FROM clean_groups EXCEPT SELECT {values} FROM root_groups)) clean_only_value_classes,
          (SELECT coalesce(sum(multiplicity),0) FROM clean_groups)-
            (SELECT count(*) FROM clean_groups WHERE EXISTS(SELECT 1 FROM root_groups r WHERE
              {" AND ".join(f'r.{lineage.qname(f)} IS NOT DISTINCT FROM clean_groups.{lineage.qname(f)}' for f in lineage.VALUE_FIELDS)})) clean_only_rows""")
        if (cleaning["root_distinct_rows"] + cleaning["root_value_row_surplus"] != support["root"]["row_count"] or
                support["root"]["row_count"] - cleaning["root_value_row_surplus"] -
                cleaning["expected_clean_only_rows"] + cleaning["clean_only_rows"] != support["clean"]["row_count"]):
            raise lineage.AuditBlocked("root DISTINCT to clean count reconciliation failed")
        cleaning["failed_full11_reconciliation_leaves"] = int(bool(
            cleaning["expected_clean_only_rows"] or cleaning["clean_only_rows"]))
        result = {"support": support, "cleaning": cleaning}
        for name in ("root", "clean"):
            result[name] = {"full_label": pair_metrics(con, name + "_groups"),
                            "label_omitted_diagnostic": pair_metrics(con, name + "_groups", omit_label=True)}
            for mode in result[name].values():
                if mode["maker_rows"] != support[name]["maker_rows"] or mode["nonmaker_rows"] != support[name]["nonmaker_rows"]:
                    raise lineage.AuditBlocked("pair metrics do not conserve published role counts")
        return result
    finally:
        for name in ("root", "clean"):
            con.execute(f"DROP TABLE IF EXISTS {name}_groups")


def sum_records(records: list[dict]) -> dict:
    """All census fields are additive; cash uses a stable floating sum."""
    if not records:
        return {}
    result = {}
    for key in records[0]:
        values = [record[key] for record in records]
        if isinstance(values[0], dict):
            result[key] = sum_records(values)
        elif isinstance(values[0], float):
            result[key] = math.fsum(values)
        else:
            result[key] = sum(values)
    return result


def run_census(manifest: dict) -> dict:
    frozen = [info for infos in manifest["inventories"].values() for info in infos]
    result = {"status": "incomplete", "data_certified": False, "leaves": [], "splits": [],
              "months": {}, "completed_months": [], "final_input_identity_reopened": False,
              "admission_compressed_footprint_bytes": 0, "grouping_compressed_footprint_bytes": 0,
              "planned_original_read_footprint_bytes": 0, "admission_nodes": 0}
    started, usage = time.monotonic(), resource.getrusage(resource.RUSAGE_SELF)
    con = connection()
    try:
        if runtime_settings(con) != manifest["environment"]["runtime_settings"]:
            raise lineage.AuditBlocked("body runtime settings differ from reviewed preflight")
        verify_frozen(frozen)
        for month in lineage.canonical_months():
            month_results = []
            for item in [item for item in manifest["initial_plan"] if item["month"] == month]:
                pending = [(item["lower_inclusive"], item["upper_exclusive"])]
                while pending:
                    lower, upper = pending.pop()
                    result["active_leaf"] = {"month": month, "lower_inclusive": lower, "upper_exclusive": upper}
                    current = {name: [info for info in infos if info["partition_month"] == month]
                               for name, infos in manifest["inventories"].items()}
                    selections = {name: overlap(infos, lower, upper) for name, infos in current.items()}
                    footprints = {name: item["overlapping_compressed_bytes"] for name, item in selections.items()}
                    result.pop("active_admission", None)
                    begin_admission(result)
                    leaf_views(con, manifest["inventories"], month, lower, upper)
                    plans = {name: {"admission": scan_plan(con, support_query(name + "_leaf", month),
                                      empty_overlap=selections[name]["overlapping_physical_rows"] == 0)}
                             for name in ("root", "clean")}
                    support = {}
                    result["active_admission"] = support
                    for name in ("root", "clean"):
                        reserve_read(result, "admission", name, footprints[name])
                        support[name] = leaf_support(con, name + "_leaf", month)
                    if not admitted(support):
                        if (lower, upper) == lineage.month_bounds(month):
                            children = [(value, min(value+86400, upper)) for value in range(lower, upper, 86400)]
                            split_kind = "complete_utc_days"
                        else:
                            children = list(split_seconds(lower, upper))
                            split_kind = "integer_second_bisection"
                        result["splits"].append({**result["active_leaf"], "support": support,
                            "source_scan_plans": plans, "split_kind": split_kind})
                        pending.extend(reversed(children))
                        continue
                    for name in ("root", "clean"):
                        plans[name]["grouping"] = scan_plan(con, grouping_query(name),
                            empty_overlap=selections[name]["overlapping_physical_rows"] == 0)
                    leaf = audit_leaf(con, support, read_budget=result, footprints=footprints)
                    result["leaves"].append({**result["active_leaf"], "metrics": leaf, "source_scan_plans": plans})
                    month_results.append(leaf)
                    if len(exact_json(result)) > MAX_OUTPUT:
                        raise lineage.AuditBlocked("complete scalar census exceeds the output cap; no truncation")
            month_summary = sum_records(month_results)
            for name in ("root", "clean"):
                expected = sum(info["rows"] for info in manifest["inventories"][name] if info["partition_month"] == month)
                if month_summary["support"][name]["row_count"] != expected:
                    raise lineage.AuditBlocked("complete UTC leaves do not reconcile to monthly footer rows")
            result["months"][month] = month_summary
            result["completed_months"].append(month)
            # The shared verifier proves the complete directory layout, so a
            # month-only inventory would falsely treat neighboring months as new.
            verify_frozen(frozen)
            print(json.dumps({"stage": "month_complete", "month": month,
                              "leaf_count": len(month_results), "elapsed_seconds": round(time.monotonic()-started, 3),
                              "planned_original_read_footprint_bytes": result["planned_original_read_footprint_bytes"],
                              "admission_nodes": result["admission_nodes"]}),
                  file=sys.stderr, flush=True)
        result["global"] = sum_records(list(result["months"].values()))
        for name in ("root", "clean"):
            if result["global"]["support"][name]["row_count"] != manifest["footer_rows"][name]:
                raise lineage.AuditBlocked("global row count does not reconcile to frozen footer total")
        verify_frozen(frozen)
        result["root_distinct_to_clean_reconciled"] = not bool(
            result["global"]["cleaning"]["failed_full11_reconciliation_leaves"])
        result.update(status="published_pair_census_complete", final_input_identity_reopened=True)
        result.pop("active_leaf", None)
        result.pop("active_admission", None)
    except (lineage.AuditBlocked, lineage.duckdb.Error, MemoryError, KeyboardInterrupt) as error:
        result.update(status="interrupted" if isinstance(error, KeyboardInterrupt) else "blocked_census",
                      error_class=type(error).__name__)
        if isinstance(error, lineage.AuditBlocked):
            result["failure_reason"] = str(error)
    finally:
        con.close()
        after = resource.getrusage(resource.RUSAGE_SELF)
        result["resource_profile"] = {"wall_seconds": time.monotonic() - started,
            "user_cpu_seconds": after.ru_utime - usage.ru_utime,
            "system_cpu_seconds": after.ru_stime - usage.ru_stime,
            "peak_rss_bytes": lineage.peak_rss_bytes(),
            "note": "Observed process high-water mark; DuckDB memory limit is not a hard RSS cap. Read footprints are not measured physical I/O."}
    return result


def write_immutable(destination: Path, manifest: dict) -> None:
    summary = {key: manifest[key] for key in ("schema_version", "status", "data_certified", "expected_head",
               "source_sha256", "contract_sha256", "caps", "inputs", "footer_rows", "environment", "io_plan")}
    if "census" in manifest:
        summary["census"] = {key: value for key, value in manifest["census"].items() if key not in {"leaves", "splits"}}
    encoded = exact_json(manifest)
    summary.update(manifest_bytes=len(encoded), manifest_sha256=hashlib.sha256(encoded).hexdigest())
    # summary.json is the completion barrier. A manifest without its summary is
    # an interrupted publication; publish complete reader-facing status last.
    outputs = {"manifest.json": encoded, "summary.json": exact_json(summary)}
    if len(outputs["summary.json"]) > MAX_SUMMARY:
        raise lineage.AuditBlocked("complete compact scalar summary exceeds 1MiB; no truncation")
    if sum(len(value) for value in outputs.values()) > MAX_OUTPUT:
        raise lineage.AuditBlocked("complete manifest and scalar summary exceed the output cap; no truncation")
    destination.mkdir(parents=True, exist_ok=False)
    for name, value in outputs.items():
        partial = destination / (name + ".partial")
        with partial.open("xb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, destination / name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--reviewed-preflight")
    parser.add_argument("--reviewed-preflight-sha256")
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--root", default="/mnt/data/pipeline_root_output/trades.parquet")
    parser.add_argument("--clean", default="/mnt/data/pipeline_output/trades_clean.parquet")
    args = parser.parse_args()
    from production_guard import require_production_host
    require_production_host()
    actual = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                            capture_output=True, text=True).stdout.strip()
    require_expected_head(actual, args.expected_head)
    if args.run_dir.exists():
        raise lineage.AuditBlocked("immutable run directory already exists")
    inputs = {name: str(Path(getattr(args, name)).resolve()) for name in ("root", "clean")}
    destination = args.run_dir.resolve()
    for source in map(Path, inputs.values()):
        if destination == source or destination in source.parents or source in destination.parents:
            raise lineage.AuditBlocked("output directory overlaps an input")
    reviewed, digest = None, None
    if not args.preflight:
        if not args.reviewed_preflight or not args.reviewed_preflight_sha256:
            raise lineage.AuditBlocked("body requires reviewed preflight and approved manifest SHA256")
        reviewed, digest = read_reviewed_manifest(Path(args.reviewed_preflight), args.reviewed_preflight_sha256)
    contract, contract_hash = load_contract()
    result = preflight(inputs, args.expected_head, source_snapshot(), contract, contract_hash)
    if reviewed is not None:
        verify_reviewed(reviewed, result)
        result["reviewed_preflight_path"] = str(Path(args.reviewed_preflight))
        result["reviewed_preflight_sha256"] = digest
        result["census"] = run_census(result)
        result["status"] = result["census"]["status"]
    result["command"] = sys.argv
    write_immutable(destination, result)
    print(json.dumps({"status": result["status"], "run_dir": str(destination), "data_certified": False}))
    return 0 if result["status"] in {"preflight_complete", "published_pair_census_complete"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
