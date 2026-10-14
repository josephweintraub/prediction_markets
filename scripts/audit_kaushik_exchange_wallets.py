#!/usr/bin/env python3
"""Bounded source-wallet masks and counts; no new analysis base or estimates.

Masks contain only original source locators, public exchange addresses and flags.
The accepted full-date base is the reference for both full/cutoff diagnostics.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from analysis.kaushik_polymarket_replication import build_inputs as inputs
from scripts import audit_kaushik_full_sample_subset as subset

EXCHANGES = ("0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
             "0xc5d563a36ae78145c45a50134d48a1215220f80a",
             "0xe111180000d2663c0091e4f400237545b87b996b",
             "0xe2222d279d744050d28e00520010520000310f59")
DIAGNOSTIC_EXCHANGES = ("0xe3333700ca9d93003f00f0f71f8515005f6c00aa",)
POLICY = {"policy": "exclude_source_exchange_wallets", "addresses": list(EXCHANGES),
          "fields": ["proxyWallet", "counterparty"], "comparison": "lowercase_exact_no_trim",
          "operator": "either", "null_blank_match": False,
          "diagnostic_only_addresses": list(DIAGNOSTIC_EXCHANGES),
          "unsupported_eligible_match_blocks_admission": True,
          "emitting_contract_filter": False, "other_filters_added": False}
CAPS = {"memory_limit": "192GB", "threads": 4, "spill_bytes": 16_000_000_000,
        "minimum_available_ram_bytes": 240_000_000_000, "minimum_free_bytes": 20_000_000_000,
        "maximum_output_bytes": 8_000_000_000, "maximum_mask_file_bytes": 1_000_000_000,
        "maximum_read_bytes": 1_000_000_000_000, "maximum_json_bytes": 16_000_000}
FIELDS = subset.FIELDS
SOURCE_MONTHS = subset.SOURCE_MONTHS
MASK_FIELDS = ("source_month", "source_ordinal", "matched_proxy_address", "matched_counterparty_address",
               "proxy_match", "counterparty_match", "known_exchange_match", "unsupported_candidate_match")
MASK_SCHEMA = pa.schema([(name, kind) for name, kind in zip(MASK_FIELDS,
    (pa.string(), pa.int64(), pa.string(), pa.string(), pa.bool_(), pa.bool_(), pa.bool_(), pa.bool_()))])
SOURCE_FILES = ("scripts/audit_kaushik_exchange_wallets.py", "tests/test_kaushik_exchange_wallets.py",
                "tests/test_kaushik_exchange_wallets_independent_qa.py",
                "scripts/audit_kaushik_full_sample_subset.py",
                "analysis/kaushik_polymarket_replication/build_inputs.py", "production_guard.py",
                "docs/methods_reference.md")
METRICS = ("baseline", "tails", "common_duration", "lifespan_gt1", "remaining_gt1")
JOIN_PLANNING_POLICY = {"disabled_optimizers": "join_order,build_side_probe_side"}


def require(condition, message):
    if not condition:
        raise inputs.InputBlocked("exchange-wallet audit blocked: " + message)


class ReadLedger:
    def __init__(self):
        self.bytes = 0
        self.scans = {}

    def charge(self, size, label="metadata"):
        require(type(size) is int and size >= 0 and self.bytes + size <= CAPS["maximum_read_bytes"],
                "read ceiling before " + label)
        self.bytes += size
        self.scans[label] = self.scans.get(label, 0) + size


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "committed expected HEAD required")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()
    require(git("rev-parse", "HEAD") == expected_head, "audit HEAD differs")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    require(not git("status", "--porcelain", "--", *SOURCE_FILES), "audit source/dependencies uncommitted")
    return {"head": expected_head, "files": {name: inputs.sha256(REPO / name) for name in SOURCE_FILES}}


def configure(con, spill):
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit=" + inputs.literal(CAPS["memory_limit"]))
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute("SET max_temp_directory_size=" + inputs.literal(str(CAPS["spill_bytes"]) + "B"))
    con.execute("SET temp_directory=" + inputs.literal(spill))
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_object_cache=false")
    con.execute("SET disabled_optimizers='join_order,build_side_probe_side'")
    return planning_policy(con)


def planning_policy(con):
    observed = {"disabled_optimizers": con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0]}
    require(observed == JOIN_PLANNING_POLICY, "physical join planning setting drift")
    return observed


def preflight(reference_dir, manifest_sha, acceptance_sha, target, source):
    reads = ReadLedger()
    reference = subset.load_stage(reference_dir, manifest_sha, acceptance_sha, "all_available_dates", reads)
    manifest = reference["manifest"]
    require(reference["months"] == list(SOURCE_MONTHS), "reference must contain the complete 44-month inventory")
    require(manifest.get("schema_version") == "kaushik_replication_inputs_v1" and
            not any(key in manifest.get("definitions", {}) for key in ("wallet_policy", "wallet_filter")),
            "reference must be the accepted original unfiltered input generation")
    contract_id = manifest.get("binding", {}).get("contract", {})
    reads.charge(inputs.stat_identity(contract_id["path"])["bytes"], "input_contract")
    contract, raw_files, binding = inputs.load_contract(contract_id["path"])
    require(binding["contract"]["sha256"] == contract_id.get("sha256") and
            contract == manifest["input_preflight"]["contract"], "accepted input contract binding differs")
    require(binding["repair_manifest"]["sha256"] == manifest["binding"]["repair_manifest"]["sha256"],
            "repaired source generation differs")
    reads.charge(binding["repair_manifest"]["stat"]["bytes"], "repair_manifest")
    raw, metadata = [], []
    old_raw = {item["month"]: item for item in manifest["input_preflight"]["trades"]}
    require([item["month"] for item in raw_files] == list(SOURCE_MONTHS) and set(old_raw) == set(SOURCE_MONTHS),
            "repaired source months missing or mismatched")
    for item in raw_files:
        reads.charge(item["bytes"], "source_footer")
        info = inputs.footer(item["path"], "trades")
        schema = pq.ParquetFile(item["path"]).schema_arrow
        require(all(str(schema.field(field).type) == "string" for field in POLICY["fields"]),
                "source wallet fields must be strings")
        original = old_raw[item["month"]]
        require(info["rows"] == item["rows"] == original["rows"] and
                info["stat"]["bytes"] == item["bytes"] == original["stat"]["bytes"] and
                info["schema"] == original["schema"] and info["footer_sha256"] == original["footer_sha256"] and
                item["sha256"] == original["expected_sha256"] ==
                manifest["inputs"].get(item["path"]), "accepted source footer/schema/hash binding differs")
        raw.append({**info, "month": item["month"], "expected_sha256": item["sha256"]})
    require(sum(i["rows"] for i in raw) == manifest["rows"]["source"], "raw source/reference population count differs")
    for name, item in contract["metadata"].items():
        info = inputs.footer(item["path"], name)
        reads.charge(info["stat"]["bytes"], "native_metadata_footer")
        original = manifest["input_preflight"]["metadata"][name]
        require(item["sha256"] == manifest["inputs"].get(item["path"]) and
                all(info[key] == original[key] for key in ("schema", "rows", "row_groups", "footer_sha256")) and
                info["stat"]["bytes"] == original["stat"]["bytes"],
                "accepted native metadata generation/schema differs")
        metadata.append({**info, "kind": name, "expected_sha256": item["sha256"]})
    inputs.validate_destination(target, [reference_dir, *(i["path"] for i in raw), *(i["path"] for i in metadata),
                                         *(i["path"] for i in binding.values())])
    reserve = CAPS["maximum_output_bytes"] + CAPS["spill_bytes"] + CAPS["minimum_free_bytes"]
    free = shutil.disk_usage(Path(target).parent).free
    require(free >= reserve, "44GB output/spill/free-floor reservation fails")
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    ram = int(available.group(1)) * 1024 if available else 0
    require(ram >= CAPS["minimum_available_ram_bytes"] and (os.cpu_count() or 0) >= CAPS["threads"],
            "240GB available RAM/four-thread admission fails for 192GB DuckDB limit")
    planned = (8 * sum(i["stat"]["bytes"] for i in raw) +
               16 * sum(i["info"]["bytes"] for i in reference["files"].values()) +
               6 * len(raw) * reference["files"]["claims.parquet"]["info"]["bytes"] +
               8 * sum(i["stat"]["bytes"] for i in metadata) +
               16 * CAPS["maximum_output_bytes"] + 16 * CAPS["maximum_json_bytes"])
    require(planned <= CAPS["maximum_read_bytes"], "conservative source/reference/output read plan exceeds 1TB")
    with duckdb.connect() as con:
        physical_policy = configure(con, Path(target) / "spill")
    return {"schema_version": "kaushik_exchange_wallet_preflight_v1", "status": "preflight_complete",
            "target": str(Path(target).resolve()), "source": source, "reference": reference,
            "raw_sources": raw, "metadata": metadata, "input_binding": binding,
            "policy": POLICY, "caps": CAPS, "required_free_bytes": reserve,
            "join_planning_policy": physical_policy,
            "planned_read_bytes": planned, "metadata_read_bytes": reads.bytes,
            "observed_free_bytes": free, "observed_available_ram_bytes": ram,
            "limits": "Footer/resource admission only. No Parquet bodies read; no new base or estimates."}


def create_source_view(con, path, month):
    require(re.fullmatch(r"\d{4}-\d{2}", month or ""), "source month syntax")
    con.execute(f"CREATE OR REPLACE TEMP VIEW source_month AS SELECT *,file_row_number::BIGINT source_ordinal "
                f"FROM read_parquet({inputs.literal(path)},hive_partitioning=false,file_row_number=true)")
    addresses = ",".join(inputs.literal(a) for a in (*EXCHANGES, *DIAGNOSTIC_EXCHANGES))
    con.execute(f"""CREATE OR REPLACE TEMP VIEW source_matches AS SELECT source_ordinal,year_month,
        timestamp,side,is_maker,
        CASE WHEN lower(proxyWallet) IN ({addresses}) THEN lower(proxyWallet) END matched_proxy_address,
        CASE WHEN lower(counterparty) IN ({addresses}) THEN lower(counterparty) END matched_counterparty_address
        FROM source_month""")


def source_mask_sql(month):
    known = ",".join(inputs.literal(a) for a in EXCHANGES)
    diagnostic = ",".join(inputs.literal(a) for a in DIAGNOSTIC_EXCHANGES)
    return f"""SELECT {inputs.literal(month)}::VARCHAR source_month,source_ordinal,
        matched_proxy_address,matched_counterparty_address,
        matched_proxy_address IS NOT NULL proxy_match,matched_counterparty_address IS NOT NULL counterparty_match,
        coalesce(matched_proxy_address IN ({known}) OR matched_counterparty_address IN ({known}),false) known_exchange_match,
        coalesce(matched_proxy_address IN ({diagnostic}) OR matched_counterparty_address IN ({diagnostic}),false) unsupported_candidate_match
        FROM source_matches WHERE matched_proxy_address IS NOT NULL OR matched_counterparty_address IS NOT NULL"""


def validate_mask(con, mask_path, month, source_rows, *, ledger=None, mask_bytes=0):
    require(pq.ParquetFile(mask_path).schema_arrow.equals(MASK_SCHEMA), "mask schema differs")
    if ledger: ledger.charge(mask_bytes, "mask_validate")
    con.execute(f"CREATE OR REPLACE TEMP VIEW wallet_mask AS SELECT * FROM read_parquet({inputs.literal(mask_path)},hive_partitioning=false)")
    allowed = ",".join(inputs.literal(a) for a in (*EXCHANGES, *DIAGNOSTIC_EXCHANGES))
    known = ",".join(inputs.literal(a) for a in EXCHANGES)
    diagnostic = ",".join(inputs.literal(a) for a in DIAGNOSTIC_EXCHANGES)
    n, unique, bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),count(*) FILTER(WHERE
        source_month IS DISTINCT FROM {inputs.literal(month)} OR source_ordinal IS NULL OR source_ordinal<0 OR source_ordinal>={source_rows}
        OR (matched_proxy_address IS NOT NULL AND matched_proxy_address NOT IN ({allowed}))
        OR (matched_counterparty_address IS NOT NULL AND matched_counterparty_address NOT IN ({allowed}))
        OR proxy_match IS DISTINCT FROM (matched_proxy_address IS NOT NULL)
        OR counterparty_match IS DISTINCT FROM (matched_counterparty_address IS NOT NULL)
        OR NOT(proxy_match OR counterparty_match)
        OR known_exchange_match IS DISTINCT FROM coalesce(matched_proxy_address IN ({known}) OR matched_counterparty_address IN ({known}),false)
        OR unsupported_candidate_match IS DISTINCT FROM coalesce(matched_proxy_address IN ({diagnostic}) OR matched_counterparty_address IN ({diagnostic}),false))
        FROM wallet_mask""").fetchone()
    require(n == unique and bad == 0, "mask locator/address/flag contract differs")
    return n


def compare_source_mask(con, mask_path, month, *, ledger=None, raw_bytes=0, mask_bytes=0):
    if ledger: ledger.charge(raw_bytes + mask_bytes, "source_mask_exact_locator_fields")
    fields = [name for name in MASK_FIELDS if name != "source_ordinal"]
    mismatch = " OR ".join(f"s.{inputs.ident(f)} IS DISTINCT FROM m.{inputs.ident(f)}" for f in fields)
    count = con.execute(f"""SELECT count(*) FROM ({source_mask_sql(month)}) s FULL OUTER JOIN
        read_parquet({inputs.literal(mask_path)},hive_partitioning=false) m USING(source_ordinal)
        WHERE s.source_ordinal IS NULL OR m.source_ordinal IS NULL OR {mismatch}""").fetchone()[0]
    require(count == 0, "source/mask missing, added or changed locator evidence")


def _rows(con, sql):
    cursor = con.execute(sql)
    return [dict(zip([item[0] for item in cursor.description], row)) for row in cursor.fetchall()]


def raw_match_counts(con, month, *, ledger=None, raw_bytes=0):
    if ledger: ledger.charge(raw_bytes, "raw_match_support")
    return _rows(con, f"""SELECT CASE WHEN side IN ('BUY','SELL') THEN side ELSE 'invalid_or_missing' END side,is_maker,
        matched_proxy_address,matched_counterparty_address,count(*) full_rows,
        count(*) FILTER(WHERE timestamp IS NOT NULL AND timestamp>=0 AND timestamp<{inputs.CUTOFF_SECONDS}) cutoff_rows
        FROM source_matches WHERE matched_proxy_address IS NOT NULL OR matched_counterparty_address IS NOT NULL
        GROUP BY ALL ORDER BY side,is_maker,matched_proxy_address,matched_counterparty_address""")


def audit_month(con, base_path, mask_path, month, source_rows, *, ledger=None, base_bytes=0, mask_bytes=0,
                claims_bytes=0, fail_on_unsupported=True):
    """Reference-base partition; fixtures provide a claims relation and mask file."""
    mask_n = validate_mask(con, mask_path, month, source_rows, ledger=ledger, mask_bytes=mask_bytes)
    require(pq.ParquetFile(base_path).schema_arrow.names == list(FIELDS), "reference 12-column schema differs")
    con.execute(f"CREATE OR REPLACE TEMP VIEW prior_base AS SELECT * FROM read_parquet({inputs.literal(base_path)},hive_partitioning=false)")
    if ledger: ledger.charge(base_bytes, "reference_locator_validity")
    n, unique, bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),count(*) FILTER(WHERE
        source_month IS DISTINCT FROM {inputs.literal(month)} OR source_ordinal IS NULL OR source_ordinal<0 OR source_ordinal>={source_rows}
        OR timestamp IS NULL OR timestamp<0 OR is_maker IS NULL OR side IS NULL OR side NOT IN ('BUY','SELL')
        OR recorded_price IS NULL OR NOT isfinite(recorded_price) OR NOT(recorded_price>0 AND recorded_price<1)
        OR P IS NULL OR NOT isfinite(P) OR NOT(P>0 AND P<1) OR Y IS NULL OR Y NOT IN (0,1)
        OR usdcSize IS NULL OR NOT isfinite(usdcSize) OR usdcSize<=0 OR claim_code IS NULL OR recorded_token_code IS NULL
        OR duration_eligible IS NULL OR P<>CASE WHEN side='SELL' THEN 1-recorded_price ELSE recorded_price END)
        FROM prior_base""").fetchone()
    require(n == unique and bad == 0, "reference locator/analytical eligibility contract differs")
    con.execute("""CREATE OR REPLACE TEMP VIEW partitioned AS SELECT b.*,m.matched_proxy_address,m.matched_counterparty_address,
        coalesce(m.known_exchange_match,false) removed,coalesce(m.unsupported_candidate_match,false) unsupported,
        c.claim_code clock_claim_code,c.opening_seconds,c.endpoint_seconds,
        (c.endpoint_seconds-c.opening_seconds)/86400.0 lifespan,
        (c.endpoint_seconds-b.timestamp)/86400.0 remaining
        FROM prior_base b LEFT JOIN wallet_mask m USING(source_month,source_ordinal) LEFT JOIN claims c USING(claim_code)""")
    if ledger: ledger.charge(base_bytes + mask_bytes + claims_bytes, "reference_duration_clock_mapping")
    bad = con.execute("""SELECT count(*) FROM partitioned WHERE clock_claim_code IS NULL OR
        duration_eligible IS DISTINCT FROM coalesce(isfinite(opening_seconds) AND isfinite(endpoint_seconds)
        AND endpoint_seconds>opening_seconds AND timestamp>=opening_seconds AND timestamp<=endpoint_seconds,false)""").fetchone()[0]
    require(bad == 0, "reference claims/duration eligibility mapping differs")
    tail = "(P<.1 OR P>=.9)"
    predicates = ("true", tail, f"duration_eligible AND {tail}",
                  f"duration_eligible AND {tail} AND lifespan>1", f"duration_eligible AND {tail} AND remaining>1")
    projections = []
    for scope, scope_predicate in (("full", "true"), ("cutoff", f"timestamp<{inputs.CUTOFF_SECONDS}")):
        for metric, predicate in zip(METRICS, predicates):
            projections.append(f"count(*) FILTER(WHERE {scope_predicate} AND {predicate}) {scope}_{metric}")
    if ledger: ledger.charge(base_bytes + mask_bytes + claims_bytes, "reference_matched_and_retained_support")
    groups = _rows(con, "SELECT side,is_maker,removed,unsupported,matched_proxy_address,matched_counterparty_address," +
                   ",".join(projections) + " FROM partitioned GROUP BY ALL ORDER BY side,is_maker,removed,unsupported,matched_proxy_address,matched_counterparty_address")
    require(sum(r["full_baseline"] for r in groups) == n, "claims/mask join fanout or loss")
    fields = ",".join(inputs.ident(f) for f in FIELDS)
    union = f"SELECT {fields} FROM partitioned WHERE NOT removed UNION ALL SELECT {fields} FROM partitioned WHERE removed"
    if mask_n == 0:
        require(all(r["matched_proxy_address"] is None and r["matched_counterparty_address"] is None
                    and r["removed"] is False and r["unsupported"] is False for r in groups) and
                sum(r["full_baseline"] for r in groups if r["removed"]) == 0,
                "empty mask observed match/removal flags differ")
        # b.* preserves the original twelve fields. The validated empty LEFT
        # JOIN supplies no matches; count=n above rules out claims fanout. Thus
        # retained is the exact original multiset and removed is empty, without
        # executing the redundant full-row EXCEPT ALL comparisons.
        differences = [0, 0]
        partition_proof_method = "empty_mask_exact_identity"
    else:
        differences = []
        for left, right in ((f"SELECT {fields} FROM prior_base", union), (union, f"SELECT {fields} FROM prior_base")):
            if ledger: ledger.charge(3 * base_bytes + 2 * mask_bytes + 2 * claims_bytes, "reference_exact_partition")
            differences.append(con.execute(f"SELECT count(*) FROM ({left} EXCEPT ALL ({right}))").fetchone()[0])
        partition_proof_method = "bidirectional_except_all"
    require(differences == [0, 0], "reference baseline retained/removed multiset partition differs")
    totals = {scope: {metric: {part: sum(r[f"{scope}_{metric}"] for r in groups if
        part == "original" or (part == "removed") == r["removed"]) for part in ("original", "retained", "removed")}
        for metric in METRICS} for scope in ("full", "cutoff")}
    require(all(v["original"] == v["retained"] + v["removed"] for s in totals.values() for v in s.values()),
            "retained plus removed metric partition")
    unsupported = sum(r["full_baseline"] for r in groups if r["unsupported"])
    if fail_on_unsupported:
        require(unsupported == 0, "diagnostic-only V3 address has eligible reference matches: " + str(unsupported))
    return {"month": month, "mask_rows": mask_n, "eligible_groups": groups, "counts": totals,
            "unsupported_eligible_rows": unsupported, "EXCEPT_ALL_differences": differences,
            "partition_proof_method": partition_proof_method,
            "EXCEPT_ALL_queries_executed": partition_proof_method == "bidirectional_except_all"}


def address_field_counts(groups, prefix):
    values = {}
    for row in groups:
        n = row[prefix]
        for field, key in (("proxyWallet", "matched_proxy_address"), ("counterparty", "matched_counterparty_address")):
            if row[key] is not None:
                group = (row["side"], row["is_maker"], row[key], field)
                values[group] = values.get(group, 0) + n
    return [{"side": side, "is_maker": maker, "address": address, "field": field, "rows": n}
            for (side, maker, address, field), n in sorted(values.items(), key=lambda i: str(i[0]))]


def exclusion_intersection(con, path, source_rows, *, ledger=None, exclusion_bytes=0, base_bytes=0, mask_bytes=0):
    """Unmatched-to-BASE mask rows must belong to original source exclusions."""
    require(pq.ParquetFile(path).schema_arrow.names == ["source_ordinal", "eligibility_reason", "side", "is_maker"],
            "original exclusion schema differs")
    if ledger: ledger.charge(exclusion_bytes, "original_exclusion_locators")
    con.execute(f"CREATE OR REPLACE TEMP VIEW prior_exclusions AS SELECT * FROM read_parquet({inputs.literal(path)},hive_partitioning=false)")
    n, unique, bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),count(*) FILTER(WHERE
        source_ordinal IS NULL OR source_ordinal<0 OR source_ordinal>={source_rows} OR eligibility_reason IS NULL
        OR eligibility_reason='' OR eligibility_reason='eligible') FROM prior_exclusions""").fetchone()
    require(n == unique and bad == 0, "original exclusion locator/reason contract differs")
    if ledger: ledger.charge(exclusion_bytes + base_bytes, "base_exclusion_disjoint")
    overlap = con.execute("SELECT count(*) FROM prior_base JOIN prior_exclusions USING(source_ordinal)").fetchone()[0]
    baseline_n = con.execute("SELECT count(*) FROM prior_base").fetchone()[0]
    if ledger: ledger.charge(base_bytes, "base_partition_count")
    require(overlap == 0 and n + baseline_n == source_rows, "original BASE/exclusion source locator partition differs")
    if ledger: ledger.charge(exclusion_bytes + mask_bytes, "raw_noneligible_match_reasons")
    reasons = _rows(con, """SELECT eligibility_reason,side,is_maker,count(*) AS "rows"
        FROM prior_exclusions JOIN wallet_mask USING(source_ordinal) GROUP BY ALL ORDER BY eligibility_reason,side,is_maker""")
    return {"original_exclusion_rows": n, "matched_noneligible_rows": sum(row["rows"] for row in reasons),
            "first_failure_counts": reasons}


def role_counts(groups, scope, metric, part):
    selected = [g for g in groups if part == "original" or (part == "removed") == g["removed"]]
    conditions = (lambda g: True, lambda g: not g["is_maker"], lambda g: g["side"] == "BUY",
                  lambda g: g["side"] == "BUY" and g["is_maker"],
                  lambda g: g["side"] == "BUY" and not g["is_maker"])
    return {name: sum(g[f"{scope}_{metric}"] for g in selected if predicate(g))
            for name, predicate in zip(subset.PARTITIONS, conditions)}


def _reserve(stage, ceiling=0):
    used = sum(p.stat().st_size for p in Path(stage).rglob("*") if p.is_file() and "spill" not in p.relative_to(stage).parts)
    require(used + ceiling <= CAPS["maximum_output_bytes"], "total output ceiling")
    spill_used = sum(min(p.stat().st_size, p.stat().st_blocks * 512) for p in (Path(stage) / "spill").rglob("*") if p.is_file())
    required = CAPS["maximum_output_bytes"] - used + max(0, CAPS["spill_bytes"] - spill_used) + CAPS["minimum_free_bytes"]
    require(shutil.disk_usage(stage).free >= required,
            "output/spill/free-floor reservation")


def _hash_reopen(items, reads, label):
    for item in items:
        info = item.get("info", item)
        stat = item["stat"]
        digest = item.get("expected_sha256", info.get("sha256"))
        reads.charge(stat["bytes"], label)
        require(inputs.stat_identity(item["path"]) == stat and inputs.sha256(item["path"]) == digest,
                "bound file identity/content drift")


def _footer_reopen(fresh, reads):
    for item in [*fresh["raw_sources"], *fresh["metadata"]]:
        reads.charge(item["stat"]["bytes"], "input_footer_after")
        footer = inputs.footer(item["path"], item.get("kind", "trades"))
        require(footer == {key: item[key] for key in footer}, "source/metadata footer drift after body")
    for item in fresh["reference"]["files"].values():
        reads.charge(item["info"]["bytes"], "reference_footer_after")
        parquet = pq.ParquetFile(item["path"])
        require(parquet.metadata.num_rows == item["info"]["rows"] and
                parquet.metadata.num_row_groups == item["info"]["row_groups"] and
                str(parquet.schema_arrow) == item["info"]["schema"], "reference footer drift after body")


def audit_stage(reviewed, fresh, reviewed_identity=None, command=None):
    require(fresh["caps"] == CAPS and fresh["policy"] == POLICY, "caps/policy drift")
    require(fresh.get("join_planning_policy") == JOIN_PLANNING_POLICY, "reviewed physical policy drift")
    stable = set(fresh) - {"observed_free_bytes", "observed_available_ram_bytes"}
    require(set(reviewed) == set(fresh) and all(reviewed.get(k) == fresh[k] for k in stable),
            "reviewed preflight/source/input drift")
    require(isinstance(reviewed_identity, dict), "reviewed preflight identity required")
    review, identity = inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])
    require(review == reviewed and identity == reviewed_identity, "reviewed preflight JSON drift")
    target = Path(fresh["target"])
    require(not target.exists(), "immutable target already exists")
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    reads, con, published = ReadLedger(), None, False
    reads.charge(fresh["metadata_read_bytes"], "preflight_metadata")
    months, outputs = [], {}
    files = [*fresh["raw_sources"], *fresh["metadata"], *fresh["reference"]["files"].values()]
    bindings = [*fresh["reference"]["bindings"].values(), *fresh["input_binding"].values(), reviewed_identity]
    try:
        for binding in bindings:
            reads.charge(binding["stat"]["bytes"], "binding_before")
            require(inputs.read_json(binding["path"], binding["sha256"])[1] == binding, "bound JSON drift")
        _hash_reopen(files, reads, "input_hash_before")
        con = duckdb.connect()
        runtime_policy = configure(con, stage / "spill")
        require(runtime_policy == fresh["join_planning_policy"], "runtime physical policy differs from preflight")
        claims = fresh["reference"]["files"]["claims.parquet"]
        reads.charge(claims["info"]["bytes"], "claims_uniqueness")
        con.execute(f"CREATE TEMP VIEW claims AS SELECT * FROM read_parquet({inputs.literal(claims['path'])})")
        n, unique = con.execute("SELECT count(*),count(DISTINCT claim_code) FROM claims").fetchone()
        require(n == unique == claims["info"]["rows"], "claims code grain differs")
        for raw in fresh["raw_sources"]:
            month = raw["month"]
            create_source_view(con, raw["path"], month)
            reads.charge(raw["stat"]["bytes"], "source_month_validity")
            year, number = map(int, month.split("-"))
            begin = int(datetime(year, number, 1, tzinfo=timezone.utc).timestamp())
            end = int(datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=timezone.utc).timestamp())
            count, unique, first, last, bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),min(source_ordinal),max(source_ordinal),count(*) FILTER(WHERE
                year_month IS DISTINCT FROM {inputs.literal(month)} OR
                (timestamp IS NOT NULL AND (timestamp<{begin} OR timestamp>={end}))) FROM source_month""").fetchone()
            require(count == unique == raw["rows"] and bad == 0 and
                    (count == 0 or (first == 0 and last == count - 1)), "source physical locator/month count differs")
            folder = stage / ("year_month=" + month)
            folder.mkdir()
            path = folder / "wallet_mask.parquet"
            _reserve(stage, CAPS["maximum_mask_file_bytes"])
            reads.charge(raw["stat"]["bytes"], "source_mask_copy")
            with inputs.copy_ceiling(CAPS["maximum_mask_file_bytes"]):
                con.execute(f"COPY ({source_mask_sql(month)}) TO {inputs.literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD)")
            require(path.stat().st_size <= CAPS["maximum_mask_file_bytes"], "monthly mask file ceiling")
            reads.charge(path.stat().st_size, "mask_hash")
            info = inputs._output_info(path)
            validate_mask(con, path, month, raw["rows"], ledger=reads, mask_bytes=info["bytes"])
            compare_source_mask(con, path, month, ledger=reads, raw_bytes=raw["stat"]["bytes"], mask_bytes=info["bytes"])
            base = fresh["reference"]["files"][f"year_month={month}/base.parquet"]
            result = audit_month(con, base["path"], path, month, raw["rows"], ledger=reads,
                                 base_bytes=base["info"]["bytes"], mask_bytes=info["bytes"],
                                 claims_bytes=claims["info"]["bytes"], fail_on_unsupported=False)
            raw_counts = raw_match_counts(con, month, ledger=reads, raw_bytes=raw["stat"]["bytes"])
            result["raw_groups"] = raw_counts
            result["raw_minus_eligible"] = {}
            for scope in ("full", "cutoff"):
                raw_n = sum(g["full_rows" if scope == "full" else "cutoff_rows"] for g in raw_counts)
                eligible_n = sum(g[scope + "_baseline"] for g in result["eligible_groups"]
                                 if g["matched_proxy_address"] is not None or g["matched_counterparty_address"] is not None)
                require(raw_n >= eligible_n and (scope != "full" or raw_n == info["rows"]),
                        "raw/eligible mask intersection count differs")
                result["raw_minus_eligible"][scope] = {"raw_matched": raw_n, "eligible_matched": eligible_n,
                                                       "noneligible_matched": raw_n - eligible_n}
            excluded = fresh["reference"]["files"][f"year_month={month}/exclusions.parquet"]
            intersection = exclusion_intersection(con, excluded["path"], raw["rows"], ledger=reads,
                exclusion_bytes=excluded["info"]["bytes"], base_bytes=base["info"]["bytes"], mask_bytes=info["bytes"])
            require(intersection["matched_noneligible_rows"] == result["raw_minus_eligible"]["full"]["noneligible_matched"],
                    "raw minus eligible does not reconcile original source exclusions")
            result["original_exclusion_intersection"] = intersection
            result["address_field_counts"] = {scope: {
                "raw": address_field_counts(raw_counts, "full_rows" if scope == "full" else "cutoff_rows"),
                "eligible": address_field_counts(result["eligible_groups"], scope + "_baseline")}
                for scope in ("full", "cutoff")}
            months.append(result)
            outputs[str(path.relative_to(stage))] = info
            require(result["unsupported_eligible_rows"] == 0, "diagnostic-only V3 address has eligible reference matches")
            _reserve(stage)
            print(json.dumps({"stage": "wallet_month_complete", "month": month,
                "mask_rows": info["rows"], "removed_full": result["counts"]["full"]["baseline"]["removed"],
                "removed_cutoff": result["counts"]["cutoff"]["baseline"]["removed"]}), flush=True)
        totals = {scope: {metric: {part: sum(m["counts"][scope][metric][part] for m in months)
            for part in ("original", "retained", "removed")} for metric in METRICS} for scope in ("full", "cutoff")}
        reference = fresh["reference"]["manifest"]
        require(totals["full"]["baseline"]["original"] == reference["rows"]["baseline_all_roles"], "accepted baseline total differs")
        groups = [g for m in months for g in m["eligible_groups"]]
        role_totals = {scope: {metric: {part: role_counts(groups, scope, metric, part)
            for part in ("original", "retained", "removed")} for metric in METRICS} for scope in ("full", "cutoff")}
        require(sum(g["full_baseline"] for g in groups if not g["is_maker"]) == reference["rows"]["primary_taker"],
                "accepted primary role total differs")
        require(role_totals["full"]["common_duration"]["original"]["primary_taker"] ==
                sum(g["duration_tail_rows"] for g in reference["support"] if not g["is_maker"]),
                "accepted common duration primary support differs")
        for scope in ("full", "cutoff"):
            for part in ("original", "retained", "removed"):
                selected = [g for g in groups if part == "original" or (part == "removed") == g["removed"]]
                buy = sum(g[scope + "_baseline"] for g in selected if g["side"] == "BUY")
                require(buy == sum(g[scope + "_baseline"] for g in selected if g["side"] == "BUY" and g["is_maker"]) +
                        sum(g[scope + "_baseline"] for g in selected if g["side"] == "BUY" and not g["is_maker"]), "BUY role partition differs")
        _hash_reopen(files, reads, "input_hash_after")
        _footer_reopen(fresh, reads)
        for binding in bindings:
            reads.charge(binding["stat"]["bytes"], "binding_after")
            require(inputs.read_json(binding["path"], binding["sha256"])[1] == binding, "JSON drift after body")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "source drift before publication")
        require(planning_policy(con) == runtime_policy, "runtime physical setting drift before publication")
        # Charge the remaining declared passes before sealing immutable evidence.
        for info in outputs.values():
            reads.charge(info["bytes"], "mask_reopen_before_publish")
            reads.charge(info["bytes"], "mask_reopen_after_publish")
        reads.charge(2 * CAPS["maximum_json_bytes"], "published_JSON_reopen_reserved_ceiling")
        require(reads.bytes <= fresh["planned_read_bytes"], "charged reads exceed reviewed plan")
        result = {"schema_version": "kaushik_exchange_wallet_audit_v1", "status": "exchange_wallet_audit_complete",
            "source": fresh["source"], "policy": POLICY, "caps": CAPS, "reviewed_preflight": reviewed_identity,
            "reference_binding": fresh["reference"]["bindings"], "input_binding": fresh["input_binding"],
            "months": months, "totals": totals, "outputs": outputs, "command": command,
            "role_totals": role_totals, "join_planning_policy": runtime_policy,
            "definitions": {"common_duration": "duration-eligible fixed-tail rows; primary_taker is the model population",
                "lifespan_gt1": "common duration with native lifespan >1 day", "remaining_gt1": "common duration with remaining maturity >1 day",
                "address_field_counts": "Field hits overlap when both source wallets match; removed counts count each original record once."},
            "exact_no_change": {s: totals[s]["baseline"]["removed"] == 0 for s in ("full", "cutoff")},
            "scientific_admission": True, "read_ledger": {"charged_bytes": reads.bytes, "scans": reads.scans,
                "ceiling_bytes": CAPS["maximum_read_bytes"], "method": "Complete-file bytes conservatively charged before declared passes."},
            "checks": {"source_mask_locator_and_fields_exact": True, "reference_multiset_partition": True,
                "BUY_role_partition": True, "source_inputs_reopened_unchanged": True, "unsupported_eligible_matches_absent": True},
            "limitations": "Counts and masks only; no input-base rewrite, coefficients, raw-action certification or complete-exchange census."}
        _reserve(stage, CAPS["maximum_json_bytes"])
        inputs.write_json(stage / "audit.json", result)
        con.close(); con = None
        for relative, info in outputs.items():
            require(inputs._output_info(stage / relative) == info, "mask schema/hash/count drift before publication")
        require(reads.bytes <= fresh["planned_read_bytes"], "final charged reads exceed reviewed plan")
        inputs.atomic_publish(stage, target); published = True
        saved, identity = inputs.read_json(target / "audit.json")
        require(saved == result, "published audit differs")
        for relative, info in outputs.items():
            require(inputs._output_info(target / relative) == info, "published mask schema/hash/count differs")
        require(reads.bytes <= fresh["planned_read_bytes"], "published read ledger exceeds reviewed plan")
        _reserve(target, CAPS["maximum_json_bytes"])
        inputs.write_json(target / "acceptance.json", {"schema_version": "kaushik_exchange_wallet_acceptance_v1",
            "status": "exchange_wallet_masks_reopened_accepted", "audit_sha256": identity["sha256"],
            "source_head": fresh["source"]["head"], "policy": POLICY, "scientific_admission": True,
            "all_masks_reopened": True, "totals": totals, "charged_read_bytes": reads.bytes,
            "accepted_at_utc": datetime.now(timezone.utc).isoformat()})
        receipt, _ = inputs.read_json(target / "acceptance.json")
        require(receipt["audit_sha256"] == identity["sha256"] and receipt["all_masks_reopened"] is True,
                "published acceptance receipt differs")
        return result
    except BaseException as error:
        failure = target if published else stage
        if not (failure / "failure.json").exists():
            inputs.write_json(failure / "failure.json", {"status": "exchange_wallet_audit_incomplete",
                "error_type": type(error).__name__, "reason": str(error), "months": months, "outputs": outputs,
                "scientific_admission": False, "charged_read_bytes": reads.bytes})
        raise
    finally:
        if con is not None: con.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dir", type=Path, required=True)
    parser.add_argument("--reference-manifest-sha256", required=True)
    parser.add_argument("--reference-acceptance-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--preflight-dir", type=Path)
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    source = source_snapshot(args.expected_head)
    fresh = preflight(args.reference_dir, args.reference_manifest_sha256, args.reference_acceptance_sha256,
                      args.run_dir, source)
    if args.preflight_dir:
        require(not args.reviewed_preflight and not args.reviewed_preflight_sha256, "preflight/body modes exclusive")
        inputs.validate_destination(args.preflight_dir, [args.reference_dir, args.run_dir, *(i["path"] for i in fresh["raw_sources"])])
        args.preflight_dir.mkdir(exist_ok=False)
        inputs.write_json(args.preflight_dir / "manifest.json", fresh)
        print(json.dumps({"status": "preflight_complete", "planned_read_bytes": fresh["planned_read_bytes"],
                          "required_free_bytes": fresh["required_free_bytes"]}))
        return 0
    require(args.reviewed_preflight and args.reviewed_preflight_sha256, "body requires separately reviewed preflight")
    review, identity = inputs.read_json(args.reviewed_preflight, args.reviewed_preflight_sha256)
    result = audit_stage(review, fresh, identity, [sys.executable, str(Path(__file__).resolve()),
                                                *(sys.argv[1:] if argv is None else argv)])
    print(json.dumps({"status": result["status"], "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
