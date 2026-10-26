"""One bounded two-column raw projection audit, not final data admission.

Counts five reviewed exchange-address buckets and one unsupported bucket.
The known positive V3 probe is bound separately; no wallet, order, trade body
or native identity values are exported by this command.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_protocol_activity as activity

MAX_PROJECTED_BYTES = 1024**3
MAX_OUTPUT_BYTES = 1024**2
EXCHANGES = planner.EXCHANGES
BUCKETS = tuple(label for label, _ in EXCHANGES) + ("unsupported",)
PROJECTED = ("exchange_address", "block_number")
RESOURCES = {"memory_limit": "4GB", "threads": 4, "spill": "0B",
             "maximum_projected_compressed_bytes": MAX_PROJECTED_BYTES,
             "maximum_manual_footer_bytes": inventory.LIMITS["maximum_total_footer_bytes"]}


class CoverageBlocked(ValueError):
    """Fixed non-sensitive failure code."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise CoverageBlocked(code)


def read_known_probe(path: Path, expected_sha: str, plan_binding: dict, plan: dict) -> tuple[dict, dict]:
    require(type(expected_sha) is str and re.fullmatch(r"[0-9a-f]{64}", expected_sha), "invalid_probe_digest")
    before = inventory.path_identity(path)
    require(before["status"] == "present" and not before["is_symlink"], "activity_probe_not_regular")
    require(0 < before["target"]["bytes"] <= MAX_OUTPUT_BYTES, "activity_probe_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_OUTPUT_BYTES + 1)
    require(len(raw) <= MAX_OUTPUT_BYTES and hashlib.sha256(raw).hexdigest() == expected_sha,
            "activity_probe_digest_differs")
    require(before == inventory.path_identity(path), "activity_probe_changed_during_read")
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(CoverageBlocked("activity_probe_nonfinite_json")))
    require(type(value) is dict and value.get("schema_version") == "polymarket_protocol_activity_probe_v1"
            and value.get("status") == "bounded_activity_probe_complete"
            and value.get("source_plan_binding") == plan_binding
            and value.get("address") == activity.ADDRESS and value.get("topic0") == activity.TOPIC0
            and value.get("final_input_identity_unchanged") is True
            and value.get("collection_approved") is False and value.get("exchange_abi_admitted") is False,
            "activity_probe_binding_or_status_differs")
    tail = plan["raw_footer_tail_header"]["number"]
    windows = value.get("windows")
    require(type(windows) is list, "activity_probe_windows_absent")
    selected = [item for item in windows if type(item) is dict and item.get("label") == "historical_tail"]
    require(len(selected) == 1, "historical_probe_window_not_unique")
    window = selected[0]
    require(type(window.get("start_block")) is int and type(window.get("end_block")) is int
            and window["start_block"] == max(0, tail - 99) and window["end_block"] == tail
            and type(window.get("matching_log_count")) is int and window["matching_log_count"] > 0
            and window.get("presence_observed") is True, "historical_positive_probe_window_differs")
    evidence = {"start_block": window["start_block"], "end_block": tail,
                "probe_matching_logs": window["matching_log_count"], "address": activity.ADDRESS,
                "topic0": activity.TOPIC0}
    return evidence, {"path": str(path), "sha256": expected_sha, "identity": before}


def inspect_projection(item: dict, budget: inventory.Budget) -> dict:
    """Decode only a bounded footer buffer, never a Parquet column body."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(item["path"])
    before = inventory.path_identity(path)
    require(before == item["identity"], "raw_identity_differs_from_inventory")
    size = before["target"]["bytes"]
    require(size >= 12 and budget.files < inventory.LIMITS["maximum_files"], "raw_file_or_count_limit")
    budget.files += 1
    with path.open("rb") as stream:
        budget.metadata_read_bytes += 4
        require(stream.read(4) == b"PAR1", "invalid_raw_parquet_header")
        stream.seek(-8, os.SEEK_END)
        budget.metadata_read_bytes += 8
        trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "invalid_raw_parquet_trailer")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= size - 12 and length <= inventory.LIMITS["maximum_single_footer_bytes"],
                "raw_footer_length_or_single_limit")
        require(budget.footer_bytes + length <= inventory.LIMITS["maximum_total_footer_bytes"],
                "raw_total_footer_byte_limit")
        budget.footer_bytes += length
        budget.metadata_read_bytes += length
        stream.seek(-8 - length, os.SEEK_END)
        body = stream.read(length)
        require(len(body) == length, "short_raw_footer_read")
    footer_sha = hashlib.sha256(body + trailer).hexdigest()
    require(footer_sha == item["footer_sha256"], "raw_footer_digest_differs_from_inventory")
    metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + body + trailer))
    budget.row_groups += metadata.num_row_groups
    require(budget.row_groups <= inventory.LIMITS["maximum_row_groups"], "raw_row_group_limit")
    require(metadata.num_rows == item["rows_declared"], "raw_footer_rows_differ_from_inventory")
    schema = metadata.schema.to_arrow_schema()
    require(len(schema.names) == len(set(schema.names)) and all(name in schema.names for name in PROJECTED),
            "raw_projected_columns_absent_or_duplicate")
    require(pa.types.is_string(schema.field("exchange_address").type)
            and pa.types.is_integer(schema.field("block_number").type), "raw_projection_types_differ")
    sizes = {}
    for name in PROJECTED:
        indices = [index for index in range(metadata.num_columns) if metadata.schema.column(index).path == name]
        require(len(indices) == 1, "raw_projection_not_flat")
        chunks = [metadata.row_group(group).column(indices[0]) for group in range(metadata.num_row_groups)]
        require(all(type(chunk.total_compressed_size) is int and chunk.total_compressed_size >= 0 for chunk in chunks),
                "raw_projected_byte_stat_invalid")
        sizes[name] = sum(chunk.total_compressed_size for chunk in chunks)
    require(before == inventory.path_identity(path), "raw_changed_during_footer_read")
    return {"path": str(path), "identity": before, "rows_declared": metadata.num_rows,
            "footer_sha256": footer_sha, "footer_bytes": length,
            "projected_compressed_bytes": sizes}


def projection_plan(source_inventory: dict, budget: inventory.Budget, maximum_bytes: int) -> dict:
    require(type(maximum_bytes) is int and 0 < maximum_bytes <= MAX_PROJECTED_BYTES,
            "projected_byte_contract_invalid")
    raw = source_inventory["inputs"]["raw_events"]
    require(raw["status"] == "inspected", "raw_source_missing")
    files = [inspect_projection(item, budget) for item in raw["files"]]
    total = sum(sum(item["projected_compressed_bytes"].values()) for item in files)
    require(sum(item["rows_declared"] for item in files) == raw["rows_declared"], "raw_inventory_rows_do_not_reconcile")
    return {"files": files, "expected_rows": raw["rows_declared"], "projected_columns": list(PROJECTED),
            "projected_compressed_bytes": total, "maximum_projected_compressed_bytes": maximum_bytes,
            "projected_footprint_admitted": total <= maximum_bytes,
            "manual_footer_budget": dict(vars(budget)),
            "io_note": "One two-column body projection is planned. Column-compressed bytes and manual footer reads are not measured physical I/O; DuckDB metadata and OS overhead are additional."}


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def execute_scan(plan: dict, window: dict) -> list[dict]:
    import duckdb

    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='4GB'")
        con.execute("SET threads=4")
        con.execute("SET max_temp_directory_size='0B'")
        con.execute("SET temp_directory=''")
        con.execute("SET preserve_insertion_order=false")
        files = "[" + ",".join(literal(item["path"]) for item in plan["files"]) + "]"
        case = "CASE " + " ".join(f"WHEN lower(exchange_address)={literal(address)} THEN {literal(label)}"
                                   for label, address in EXCHANGES) + " ELSE 'unsupported' END"
        # Only these two fields are projected from the source, in one aggregate
        # query with six possible groups. No native identities or wallet bodies.
        sql = f"""WITH projected AS (
            SELECT exchange_address,block_number FROM read_parquet({files},hive_partitioning=false)),
            classified AS (SELECT *,{case} bucket FROM projected)
            SELECT bucket,count(*) AS "rows",
                count(*) FILTER(WHERE exchange_address IS NULL) null_address_rows,
                count(*) FILTER(WHERE exchange_address IS NOT NULL AND
                    NOT regexp_full_match(lower(exchange_address),'0x[0-9a-f]{{40}}')) invalid_address_rows,
                count(*) FILTER(WHERE block_number IS NULL) null_block_rows,
                count(*) FILTER(WHERE block_number IS NOT NULL AND block_number<=0) invalid_block_rows,
                min(block_number) minimum_nonnull_block,max(block_number) maximum_nonnull_block,
                count(*) FILTER(WHERE lower(exchange_address)={literal(activity.ADDRESS)} AND
                    block_number>={window['start_block']} AND block_number<={window['end_block']}) known_probe_window_rows
            FROM classified GROUP BY bucket"""
        cursor = con.execute(sql)
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        con.close()


def reconcile_counts(records: list[dict], expected_rows: int, evidence: dict) -> dict:
    require(type(records) is list and len(records) <= len(BUCKETS), "scan_group_limit")
    observed = {}
    counts = ("rows", "null_address_rows", "invalid_address_rows", "null_block_rows",
              "invalid_block_rows", "known_probe_window_rows")
    for item in records:
        require(item["bucket"] in BUCKETS and item["bucket"] not in observed, "scan_bucket_not_unique_or_unknown")
        require(all(type(item[name]) is int and 0 <= item[name] <= item["rows"] for name in counts),
                "scan_count_invalid")
        observed[item["bucket"]] = item
    require(sum(item["rows"] for item in records) == expected_rows, "scan_rows_do_not_match_footer")
    filled = [observed.get(name, {"bucket": name, **{field: 0 for field in counts},
                                 "minimum_nonnull_block": None, "maximum_nonnull_block": None}) for name in BUCKETS]
    invalid = {name: sum(item[name] for item in filled) for name in counts[1:-1]}
    known = sum(item["known_probe_window_rows"] for item in filled)
    missing_known = known == 0
    below_known = known < evidence["probe_matching_logs"]
    unsupported = sum(item["rows"] for item in filled if item["bucket"] == "unsupported")
    return {"buckets": filled, "total_rows": expected_rows, "invalidity_counts": invalid,
            "unsupported_address_rows": unsupported,
            "known_positive_probe": {**evidence, "archive_address_window_rows": known,
                "known_activity_absent_from_archive": missing_known,
                "archive_count_below_known_probe": below_known,
                "count_matches_probe": known == evidence["probe_matching_logs"],
                "interpretation": ("The archive has no V3-address records in a window containing independently observed V3-topic logs. Those known observations are missing from this archive."
                    if missing_known else
                    "The archive has fewer V3-address/window records than the independently observed unique topic logs; it cannot account for all known observations."
                    if below_known else "V3-address records occur in the probed window. Counts alone do not establish native-identity or payload agreement.")},
            "audit_checks_passed": not below_known and not any(invalid.values()) and unsupported == 0,
            "source_row_conservation_passed": True}


def audit(plan_path: Path, plan_sha: str, plan_head: str, probe_path: Path, probe_sha: str,
          expected_head: str, *, maximum_projected_bytes: int = MAX_PROJECTED_BYTES,
          output: Path | None = None) -> dict:
    result = {"schema_version": "polymarket_exchange_coverage_audit_v1", "status": "blocked",
              "created_utc": datetime.now(timezone.utc).isoformat(), "raw_final_admission": False,
              "resource_contract": dict(RESOURCES), "large_body_projection_queries": 0,
              "rpc_calls_attempted": 0, "manual_footer_budget": {},
              "limitations": ["This audit classifies address/block coverage only, not native identities, outcomes, wallets or economic directions.",
                  "Zero V3 rows in a known-positive probe window establish missing known observations, not whole-history completeness or the full missing quantity.",
                  "Positive count agreement does not prove log-identity agreement.",
                  "Extrema include invalid nonnull block values; null block rows are counted separately."]}
    budget = inventory.Budget()
    try:
        plan, binding, source_inventory = activity.read_plan(plan_path, plan_sha, plan_head)
        evidence, probe_binding = read_known_probe(probe_path, probe_sha, binding, plan)
        if output is not None:
            activity.validate_output(output, plan_path, Path.home() / ".polygon_rpc_url", source_inventory, plan)
            require(output.resolve() != probe_path.resolve() and probe_path.resolve() not in output.resolve().parents,
                    "output_overlaps_activity_probe")
        source = inventory.code_state()
        require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head)
                and source.get("status") == "inspected" and source.get("head") == expected_head
                and source.get("tracked_worktree_clean") is True, "audit_source_head_or_clean_state_differs")
        result.update(source_plan_binding=binding, activity_probe_binding=probe_binding, audit_source=source)
        projection = projection_plan(source_inventory, budget, maximum_projected_bytes)
        result["projection_plan"] = projection
        require(projection["projected_footprint_admitted"], "projected_compressed_byte_limit")
        # Reserve the second verification pass before opening any trade body.
        require(2 * budget.footer_bytes <= inventory.LIMITS["maximum_total_footer_bytes"]
                and 2 * budget.files <= inventory.LIMITS["maximum_files"]
                and 2 * budget.row_groups <= inventory.LIMITS["maximum_row_groups"],
                "footer_reopen_budget_not_admitted")
        result["large_body_projection_queries"] = 1
        result["counts"] = reconcile_counts(execute_scan(projection, evidence), projection["expected_rows"], evidence)
        reopened = projection_plan(source_inventory, budget, maximum_projected_bytes)
        require(projection["files"] == reopened["files"], "raw_projection_changed_during_scan")
        planner.reopen_inputs(source_inventory)
        inv = plan["inventory_binding"]
        require(inventory.path_identity(Path(inv["path"])) == inv["identity"], "inventory_changed_during_audit")
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_changed_during_audit")
        require(inventory.path_identity(probe_path) == probe_binding["identity"], "activity_probe_changed_during_audit")
        require(source == inventory.code_state(), "audit_source_changed_during_scan")
        result.update(status="exchange_coverage_audit_complete", final_input_identity_unchanged=True,
                      audit_checks_passed=result["counts"]["audit_checks_passed"])
    except (CoverageBlocked, activity.ProbeBlocked, planner.PlanBlocked, inventory.ReadinessError) as exc:
        result["error_code"] = str(exc)
    except Exception:
        result["error_code"] = "exchange_coverage_input_or_execution_failure"
    finally:
        result["manual_footer_budget"] = dict(vars(budget))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", required=True, type=Path)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--source-plan-source-head", required=True)
    parser.add_argument("--activity-probe", required=True, type=Path)
    parser.add_argument("--activity-probe-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production audit requires the canonical mounted EC2 checkout.")
    plan, binding, source_inventory = activity.read_plan(args.source_plan, args.source_plan_sha256, args.source_plan_source_head)
    read_known_probe(args.activity_probe, args.activity_probe_sha256, binding, plan)
    activity.validate_output(args.output, args.source_plan, Path.home() / ".polygon_rpc_url", source_inventory, plan)
    require(args.output.resolve() != args.activity_probe.resolve(), "output_overlaps_activity_probe")
    result = audit(args.source_plan, args.source_plan_sha256, args.source_plan_source_head,
                   args.activity_probe, args.activity_probe_sha256, args.expected_head, output=args.output)
    require(len((json.dumps(result, sort_keys=True, allow_nan=False) + "\n").encode()) <= MAX_OUTPUT_BYTES,
            "audit_output_byte_limit")
    inventory.write_exclusive(args.output, result)
    print(json.dumps({"status": result["status"], "audit_checks_passed": result.get("audit_checks_passed", False),
                      "large_body_projection_queries": result["large_body_projection_queries"], "raw_final_admission": False}))
    return 0 if result["status"] == "exchange_coverage_audit_complete" and result["audit_checks_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
