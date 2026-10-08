"""Read-only scalar integrity checks for the frozen published Polymarket view.

Production runs are serialized by month. This checks published wallet rows, not
native log completeness or the economic correctness of inferred counterparties.
Only metadata and scalar aggregates are exported; wallet/token IDs stay internal.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any


CANONICAL_HEAD = "e7ce00d070ae7055de5c23ab51f03b4d6124efef"
CANONICAL_ROWS = 2_036_128_538
CANONICAL_MONTHS = tuple(
    f"{year:04d}-{month:02d}" for year in range(2022, 2027)
    for month in range(1, 13) if "2022-11" <= f"{year:04d}-{month:02d}" <= "2026-06"
)
CLEAN_SCHEMA = (
    ("proxyWallet", "string"), ("timestamp", "int64"), ("conditionId", "string"),
    ("usdcSize", "double"), ("price", "double"), ("side", "string"),
    ("outcome", "string"), ("eventSlug", "string"), ("is_maker", "bool"),
    ("counterparty", "string"), ("year_month", "string"),
)
FLAGS_SCHEMA = (
    ("token_id", "string"), ("market_id", "string"), ("winning_outcome", "string"),
    ("is_updown", "bool"), ("question", "string"),
)
TOKEN_SCHEMA = (
    ("token_id", "large_string"), ("condition_id", "large_string"),
    ("outcome", "large_string"), ("market_slug", "large_string"),
    ("event_slug", "large_string"), ("question", "large_string"),
)


class AuditBlocked(ValueError):
    """A contract gate failed; prior scalar evidence remains available."""


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    """Replace one checkpoint atomically, only inside a freshly-created run."""
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def footer(path: Path, expected_schema: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    import pyarrow.parquet as pq

    if not path.is_file() or path.is_symlink():
        raise AuditBlocked("Input must be a regular, nonsymlink Parquet file")
    observed = pq.ParquetFile(path)
    schema = tuple((field.name, str(field.type)) for field in observed.schema_arrow)
    if schema != expected_schema:
        raise AuditBlocked("Frozen physical schema differs")
    stat = path.stat()
    with path.open("rb") as handle:
        handle.seek(-8, os.SEEK_END)
        trailer = handle.read(8)
        if trailer[4:] != b"PAR1":
            raise AuditBlocked("Invalid Parquet footer trailer")
        footer_length = int.from_bytes(trailer[:4], "little")
        handle.seek(-8 - footer_length, os.SEEK_END)
        footer_hash = hashlib.sha256(handle.read(footer_length) + trailer).hexdigest()
    return {"path": str(path), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "footer_sha256": footer_hash, "rows": observed.metadata.num_rows,
            "row_groups": observed.metadata.num_row_groups, "schema": [list(x) for x in schema]}


def inspect_inventory(clean: Path, flags: Path, token_map: Path,
                      expected_months: tuple[str, ...], expected_rows: int) -> dict[str, Any]:
    if not clean.is_dir() or clean.is_symlink():
        raise AuditBlocked("Clean input must be a partition directory")
    files = []
    months = []
    for partition in sorted(clean.iterdir()):
        match = re.fullmatch(r"year_month=(\d{4}-(?:0[1-9]|1[0-2]))", partition.name)
        if not match or not partition.is_dir() or partition.is_symlink():
            raise AuditBlocked("Unexpected clean input path")
        entries = list(partition.iterdir())
        if len(entries) != 1 or entries[0].name != "data.parquet":
            raise AuditBlocked("Frozen partition file inventory differs")
        months.append(match[1])
        files.append({"month": match[1], **footer(entries[0], CLEAN_SCHEMA)})
    if tuple(months) != expected_months:
        raise AuditBlocked("Frozen month coverage differs")
    total = sum(item["rows"] for item in files)
    if total != expected_rows:
        raise AuditBlocked("Frozen clean footer row count differs")
    return {"clean_files": files, "months": months, "clean_footer_rows": total,
            "flags": footer(flags, FLAGS_SCHEMA), "token_map": footer(token_map, TOKEN_SCHEMA),
            "identity": "file size, mtime and complete footer hash; whole data content hash not verified"}


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _record(con: Any, sql: str) -> dict[str, Any]:
    cursor = con.execute(sql)
    return dict(zip((item[0] for item in cursor.description), cursor.fetchone()))


def prepare_spines(con: Any, flags: Path, token_map: Path, *,
                   published_view_only: bool = False) -> dict[str, Any]:
    con.execute(f"CREATE TEMP VIEW flags_source AS SELECT * FROM read_parquet('{_quoted(flags)}')")
    con.execute(f"CREATE TEMP VIEW tokens_source AS SELECT * FROM read_parquet('{_quoted(token_map)}')")
    # Blank cache token rows are inventoried, never admitted to the lookup.
    con.execute("""CREATE TEMP TABLE tokens AS SELECT token_id,condition_id market_id,outcome
        FROM tokens_source WHERE token_id IS NOT NULL AND trim(token_id)<>''""")
    con.execute("""CREATE TEMP TABLE flags AS SELECT token_id,market_id,winning_outcome,is_updown
        FROM flags_source""")
    checks = _record(con, """SELECT
        (SELECT count(*) FROM tokens_source) token_map_rows,
        (SELECT count(*) FROM tokens_source WHERE token_id IS NULL) token_map_null_token_rows,
        (SELECT count(*) FROM tokens_source WHERE token_id IS NOT NULL AND trim(token_id)='') token_map_blank_token_rows,
        (SELECT count(*) FROM tokens) valid_token_rows,
        (SELECT count(*) FROM (SELECT token_id FROM tokens GROUP BY 1 HAVING count(*)<>1)) duplicate_valid_token_keys,
        (SELECT count(*) FROM tokens WHERE market_id IS NULL OR trim(market_id)='' OR outcome IS NULL OR trim(outcome)='') invalid_valid_token_metadata,
        (SELECT count(*) FROM tokens WHERE market_id IS NULL OR trim(market_id)='') invalid_valid_token_market_rows,
        (SELECT count(*) FROM tokens WHERE outcome IS NULL OR trim(outcome)='') invalid_valid_token_outcome_rows,
        (SELECT count(*) FROM flags f WHERE EXISTS (SELECT 1 FROM tokens t
            WHERE t.token_id=f.token_id AND (t.market_id IS NULL OR trim(t.market_id)='' OR t.outcome IS NULL OR trim(t.outcome)=''))) invalid_token_metadata_affected_flag_rows,
        (SELECT count(DISTINCT t.token_id) FROM tokens t WHERE
            (t.market_id IS NULL OR trim(t.market_id)='' OR t.outcome IS NULL OR trim(t.outcome)='')
            AND EXISTS (SELECT 1 FROM flags f WHERE f.token_id=t.token_id)) invalid_token_metadata_affected_flag_keys,
        (SELECT count(*) FROM tokens t WHERE
            (t.market_id IS NULL OR trim(t.market_id)='' OR t.outcome IS NULL OR trim(t.outcome)='')
            AND NOT EXISTS (SELECT 1 FROM flags f WHERE f.token_id=t.token_id)) invalid_token_metadata_unpublished_rows,
        (SELECT count(*) FROM flags) flag_rows,
        (SELECT count(*) FROM flags WHERE token_id IS NULL OR trim(token_id)='' OR market_id IS NULL
            OR trim(market_id)='' OR winning_outcome IS NULL OR trim(winning_outcome)='' OR is_updown IS NULL) invalid_flag_rows,
        (SELECT count(*) FROM (SELECT token_id FROM flags GROUP BY 1 HAVING count(*)<>1)) duplicate_flag_keys,
        (SELECT count(*) FROM (SELECT market_id FROM flags GROUP BY 1
            HAVING min(winning_outcome) IS DISTINCT FROM max(winning_outcome)
               OR min(is_updown) IS DISTINCT FROM max(is_updown))) conflicting_market_flag_groups
    """)
    structural_gates = [
        "duplicate_valid_token_keys", "invalid_flag_rows",
        "duplicate_flag_keys", "conflicting_market_flag_groups"]
    published_gates = structural_gates + ["invalid_token_metadata_affected_flag_rows"]
    if all(checks[key] == 0 for key in structural_gates):
        # Join only after both key sets passed uniqueness, so this cannot fan out.
        checks.update(_record(con, """SELECT count(*) flag_lookup_rows,
            count(*) FILTER(WHERE t.token_id IS NULL) flags_missing_token_map,
            count(*) FILTER(WHERE f.market_id IS DISTINCT FROM t.market_id) flags_market_mismatch
            FROM flags f LEFT JOIN tokens t USING(token_id)"""))
        checks.update(_record(con, """SELECT count(*) flags_winner_absent_from_token_map
            FROM flags f WHERE NOT EXISTS (SELECT 1 FROM tokens t
                WHERE t.market_id=f.market_id AND t.outcome=f.winning_outcome)"""))
        published_gates.extend(("flags_missing_token_map", "flags_market_mismatch", "flags_winner_absent_from_token_map"))
    checks["source_metadata_health_scope"] = "All nonblank cached token IDs; null/blank token rows are separately inventoried and excluded from lookup"
    checks["source_metadata_healthy"] = checks["invalid_valid_token_metadata"] == checks["duplicate_valid_token_keys"] == 0
    checks["published_metadata_gates_passed"] = all(checks[key] == 0 for key in published_gates)
    checks["strict_metadata_gates_passed"] = checks["published_metadata_gates_passed"] and checks["source_metadata_healthy"]
    checks["audit_scope"] = "published_view_only" if published_view_only else "strict_cached_metadata"
    checks["gates_passed"] = checks["published_metadata_gates_passed"] if published_view_only else checks["strict_metadata_gates_passed"]
    gate_names = published_gates if published_view_only else published_gates + ["invalid_valid_token_metadata"]
    checks["failed_gate_counts"] = {key: checks[key] for key in gate_names if checks[key] != 0}
    return checks


def scan_partition(con: Any, item: dict[str, Any]) -> dict[str, Any]:
    month = item["month"]
    start = datetime.strptime(month, "%Y-%m").replace(tzinfo=timezone.utc)
    end = datetime(start.year + (start.month == 12), start.month % 12 + 1, 1, tzinfo=timezone.utc)
    lower, upper = int(start.timestamp()), int(end.timestamp())
    con.execute("DROP VIEW IF EXISTS partition_rows")
    # Stored year_month must not be replaced by DuckDB's Hive directory column.
    con.execute(f"CREATE TEMP VIEW partition_rows AS SELECT * FROM read_parquet('{_quoted(Path(item['path']))}', hive_partitioning=false)")
    checks = _record(con, f"""SELECT count(*) row_count,
        count(*) FILTER(WHERE proxyWallet IS NULL OR trim(proxyWallet)='') invalid_wallet,
        count(*) FILTER(WHERE counterparty IS NULL OR trim(counterparty)='') invalid_counterparty,
        count(*) FILTER(WHERE conditionId IS NULL OR trim(conditionId)='') invalid_token,
        count(*) FILTER(WHERE outcome IS NULL OR trim(outcome)='') invalid_outcome,
        count(*) FILTER(WHERE timestamp IS NULL OR timestamp<0) invalid_timestamp,
        count(*) FILTER(WHERE timestamp IS NOT NULL AND (timestamp<{lower} OR timestamp>={upper})) timestamp_outside_month,
        count(*) FILTER(WHERE year_month IS DISTINCT FROM '{month}') stored_partition_mismatch,
        count(*) FILTER(WHERE side IS NULL OR side NOT IN ('BUY','SELL')) invalid_side,
        count(*) FILTER(WHERE is_maker IS NULL) invalid_maker,
        count(*) FILTER(WHERE usdcSize IS NULL OR NOT isfinite(usdcSize) OR usdcSize<=0) invalid_cash,
        count(*) FILTER(WHERE price IS NULL OR NOT isfinite(price) OR price<=0 OR price>1) invalid_price,
        count(*) FILTER(WHERE eventSlug IS NULL) null_event_slug,
        count(*) FILTER(WHERE eventSlug IS NOT NULL AND trim(eventSlug)='') blank_event_slug,
        min(timestamp) minimum_timestamp, max(timestamp) maximum_timestamp,
        coalesce(fsum(CASE WHEN isfinite(usdcSize) AND usdcSize>0 THEN usdcSize ELSE 0 END),0) positive_finite_cash
        FROM partition_rows""")
    checks.update(_record(con, """SELECT count(*) joined_rows,
        count(*) FILTER(WHERE f.token_id IS NULL) missing_flag_rows,
        count(*) FILTER(WHERE t.token_id IS NULL) missing_token_map_rows,
        count(*) FILTER(WHERE t.token_id IS NOT NULL AND p.outcome IS DISTINCT FROM t.outcome) token_outcome_mismatch,
        count(*) FILTER(WHERE f.token_id IS NOT NULL AND t.token_id IS NOT NULL
            AND f.market_id IS DISTINCT FROM t.market_id) token_market_mismatch
        FROM partition_rows p LEFT JOIN flags f ON p.conditionId=f.token_id
        LEFT JOIN tokens t ON p.conditionId=t.token_id"""))
    groups = con.execute("""SELECT
        CASE WHEN p.side IN ('BUY','SELL') THEN p.side ELSE 'INVALID' END side,
        CASE WHEN p.is_maker THEN 'maker' WHEN NOT p.is_maker THEN 'taker' ELSE 'missing' END maker_status,
        CASE WHEN f.token_id IS NULL THEN 'missing' WHEN f.is_updown THEN 'updown' ELSE 'not_updown' END flag_status,
        CASE WHEN f.token_id IS NULL OR p.outcome IS NULL THEN 'unknown'
             WHEN p.outcome=f.winning_outcome THEN 'winning' ELSE 'losing' END resolved_label_status,
        count(*) row_count,
        coalesce(fsum(CASE WHEN isfinite(p.usdcSize) AND p.usdcSize>0 THEN p.usdcSize ELSE 0 END),0) positive_finite_cash
        FROM partition_rows p LEFT JOIN flags f ON p.conditionId=f.token_id
        GROUP BY 1,2,3,4 ORDER BY 1,2,3,4""").fetchall()
    groups = [dict(zip(("side", "maker_status", "flag_status", "resolved_label_status",
                        "row_count", "positive_finite_cash"), row)) for row in groups]
    checks["footer_count_matches"] = checks["row_count"] == item["rows"]
    checks["join_preserves_rows"] = checks["joined_rows"] == checks["row_count"]
    checks["group_count_matches"] = sum(group["row_count"] for group in groups) == checks["row_count"]
    checks["group_cash_matches"] = math.isfinite(checks["positive_finite_cash"]) and math.isclose(
        math.fsum(group["positive_finite_cash"] for group in groups), checks["positive_finite_cash"],
        rel_tol=1e-12, abs_tol=1e-6)
    failing = [key for key in checks if key.startswith("invalid_") or key.endswith("_mismatch") or key in (
        "timestamp_outside_month", "missing_flag_rows", "missing_token_map_rows")]
    passed = all(checks[key] == 0 for key in failing) and all(checks[key] for key in (
        "footer_count_matches", "join_preserves_rows", "group_count_matches", "group_cash_matches"))
    if not math.isfinite(checks["positive_finite_cash"]):
        checks["positive_finite_cash"] = None
    for group in groups:
        if not math.isfinite(group["positive_finite_cash"]):
            group["positive_finite_cash"] = None
    return {"month": month, "status": "passed" if passed else "blocked_integrity",
            "checks": checks, "groups": groups}


def run_audit(clean: Path, flags: Path, token_map: Path, run_dir: Path, *,
              months: tuple[str, ...] | None = None, inventory_only: bool = False,
              spine_only: bool = False,
              published_view_only: bool = False,
              expected_months: tuple[str, ...] = CANONICAL_MONTHS,
              expected_rows: int = CANONICAL_ROWS, provenance: dict[str, Any] | None = None) -> dict[str, Any]:
    clean, flags, token_map, run_dir = (path.resolve() for path in (clean, flags, token_map, run_dir))
    if inventory_only and spine_only:
        raise AuditBlocked("Metadata-only and spine-only scopes cannot be combined")
    for source in (clean, flags, token_map):
        if run_dir == source or source in run_dir.parents or run_dir in source.parents:
            raise AuditBlocked("Output overlaps an input")
    run_dir.mkdir(parents=True, exist_ok=False)
    summary: dict[str, Any] = {
        "schema_version": 2, "stage": "polymarket_published_clean_integrity_v2", "status": "incomplete",
        "certification_status": "not_certified", "scanned_months": [], "scanned_rows": 0,
        "audit_scope": "published_view_only" if published_view_only else "strict_cached_metadata",
        "source_metadata_healthy": None,
        "positive_finite_cash": 0.0, "resource_contract": {"threads": 4, "memory_limit": "8GB", "spill": "0B"},
        "provenance": provenance or {"execution": "local_fixture"},
        "limitations": ["Published legacy wallet rows do not certify native economic direction or collection completeness.",
                        "Token-map/flag agreement does not independently certify on-chain resolution.",
                        "Empty/null eventSlug is counted but is not a gate; native flags supply market classification.",
                        "Cash sums count published wallet rows, not unique native economic executions."],
    }
    atomic_json(run_dir / "summary.json", summary)
    con = None
    stage = "footer_inventory"
    started = time.monotonic()
    try:
        inventory = inspect_inventory(clean, flags, token_map, expected_months, expected_rows)
        atomic_json(run_dir / "inventory.json", inventory)
        chosen = expected_months if months is None else months
        if not chosen or len(set(chosen)) != len(chosen) or not set(chosen).issubset(expected_months):
            raise AuditBlocked("Requested month coverage is invalid")
        summary["requested_months"] = list(chosen)
        summary["full_universe_scan_requested"] = set(chosen) == set(expected_months)
        summary["full_universe_scan_completed"] = False
        if inventory_only:
            summary["status"] = "metadata_complete_rows_unscanned"
            return summary
        import duckdb

        con = duckdb.connect()
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        con.execute("SET max_temp_directory_size='0B'")
        con.execute("SET temp_directory=''")
        con.execute("SET TimeZone='UTC'")
        stage = "spine_preflight"
        spine_checks = prepare_spines(con, flags, token_map, published_view_only=published_view_only)
        atomic_json(run_dir / "spine_checks.json", spine_checks)
        summary["source_metadata_healthy"] = spine_checks["source_metadata_healthy"]
        summary["source_metadata_health_scope"] = spine_checks["source_metadata_health_scope"]
        summary["source_invalid_valid_token_metadata"] = spine_checks["invalid_valid_token_metadata"]
        summary["source_invalid_token_metadata_affected_flag_rows"] = spine_checks["invalid_token_metadata_affected_flag_rows"]
        summary["published_metadata_gates_passed"] = spine_checks["published_metadata_gates_passed"]
        if not spine_checks["gates_passed"]:
            summary["blocking_gate_counts"] = spine_checks["failed_gate_counts"]
            raise AuditBlocked("Canonical spine preflight failed")
        if spine_only:
            stage = "spine_inventory_reopen"
            if inspect_inventory(clean, flags, token_map, expected_months, expected_rows) != inventory:
                raise AuditBlocked("Frozen input inventory changed during spine preflight")
            summary["status"] = "published_spine_complete_clean_rows_unscanned" if published_view_only else "spine_complete_clean_rows_unscanned"
            return summary
        for item in inventory["clean_files"]:
            if item["month"] not in chosen:
                continue
            stage = "partition_" + item["month"]
            summary["active_month"] = item["month"]
            atomic_json(run_dir / "summary.json", summary)
            result = scan_partition(con, item)
            if footer(Path(item["path"]), CLEAN_SCHEMA) != {key: value for key, value in item.items() if key != "month"}:
                raise AuditBlocked("Partition input changed during scan")
            atomic_json(run_dir / (item["month"] + ".json"), result)
            summary["scanned_months"].append(item["month"])
            summary["scanned_rows"] += result["checks"]["row_count"]
            cash = result["checks"]["positive_finite_cash"]
            summary["positive_finite_cash"] = (summary["positive_finite_cash"] + cash
                if summary["positive_finite_cash"] is not None and cash is not None else None)
            if result["status"] != "passed":
                raise AuditBlocked("Published partition integrity failed")
            atomic_json(run_dir / "summary.json", summary)
        stage = "final_inventory_reopen"
        if inspect_inventory(clean, flags, token_map, expected_months, expected_rows) != inventory:
            raise AuditBlocked("Frozen input inventory changed during audit")
        summary["full_universe_scan_completed"] = summary["full_universe_scan_requested"]
        if published_view_only:
            summary["status"] = "published_view_integrity_complete" if summary["full_universe_scan_completed"] else "published_view_pilot_complete_full_scan_incomplete"
        else:
            summary["status"] = "published_integrity_complete" if summary["full_universe_scan_completed"] else "pilot_complete_full_scan_incomplete"
        summary["certification_status"] = "published_view_integrity_only" if summary["full_universe_scan_completed"] else "not_certified_pilot_only"
        summary.pop("active_month", None)
    except BaseException as error:
        summary["status"] = ("blocked_integrity" if isinstance(error, AuditBlocked) else
                             "interrupted" if isinstance(error, KeyboardInterrupt) else "incomplete_execution_error")
        summary["failure_stage"] = stage
        # Exception text can contain source values; export class and safe stage only.
        summary["error_class"] = type(error).__name__
        if isinstance(error, AuditBlocked):
            summary["failure_reason"] = str(error)
    finally:
        if con is not None:
            con.close()
        summary["elapsed_seconds"] = time.monotonic() - started
        atomic_json(run_dir / "summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clean", type=Path, default=Path("/mnt/data/pipeline_output/trades_clean.parquet"))
    parser.add_argument("--flags", type=Path, default=Path("/mnt/data/pipeline_output/market_flags.parquet"))
    parser.add_argument("--token-map", type=Path, default=Path("/mnt/data/pipeline_data/token_map.parquet"))
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--month", action="append", help="Only these complete months; omission requests every frozen month")
    parser.add_argument("--published-view-only", action="store_true",
                        help="Audit published rows/required spines; retain and report unhealthy unflagged cached metadata separately")
    scopes = parser.add_mutually_exclusive_group()
    scopes.add_argument("--inventory-only", action="store_true")
    scopes.add_argument("--spine-only", action="store_true", help="Read skinny metadata spines only; no clean trade bodies")
    parser.add_argument("--expected-head", default=CANONICAL_HEAD)
    args = parser.parse_args(argv)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from production_guard import require_production_host

    require_production_host()
    canonical = Path("/home/ubuntu/prediction_markets")
    if Path.cwd().resolve() != canonical:
        raise RuntimeError("Run from the canonical checkout")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=canonical, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_head) or head != args.expected_head:
        raise RuntimeError("Canonical HEAD differs from the frozen audit expectation")
    summary = run_audit(args.clean, args.flags, args.token_map, args.run_dir,
        months=tuple(args.month) if args.month else None, inventory_only=args.inventory_only,
        spine_only=args.spine_only,
        published_view_only=args.published_view_only,
        provenance={"canonical_head": head, "script_path": str(Path(__file__).resolve()),
                    "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "command": sys.argv})
    print(json.dumps({"status": summary["status"], "scanned_rows": summary["scanned_rows"],
                      "run_dir": str(args.run_dir)}, allow_nan=False))
    if summary["status"] == "interrupted":
        return 130
    return 0 if summary["status"] in ("published_integrity_complete", "pilot_complete_full_scan_incomplete",
        "metadata_complete_rows_unscanned", "spine_complete_clean_rows_unscanned",
        "published_spine_complete_clean_rows_unscanned", "published_view_integrity_complete",
        "published_view_pilot_complete_full_scan_incomplete") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, FileExistsError, AuditBlocked):
        print("Audit startup refused: production host, frozen HEAD, input/output scope or no-overwrite gate failed.", file=sys.stderr)
        raise SystemExit(2)
