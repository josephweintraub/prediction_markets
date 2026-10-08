"""Read-only exact cached-timestamp coverage for every frozen native trade row.

This certifies the required-key coverage and internal clock consistency of the
cache, not independent chain truth, log completeness, or old published clocks.
Only scalar evidence and footer metadata leave the production inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any


RESOLVED_PATH = Path("/mnt/data/pipeline_data/resolved_trades.parquet")
CACHE_PATH = Path("/mnt/data/pipeline_data/block_timestamps.parquet")
RESOLVED_ROWS = 1_057_311_726
CACHE_ROWS = 27_117_476
RESOLVED_SCHEMA = (
    ("order_hash", "string"), ("maker", "string"), ("taker", "string"),
    ("maker_asset_id", "string"), ("taker_asset_id", "string"),
    ("maker_amount_filled", "int64"), ("taker_amount_filled", "int64"),
    ("fee", "int64"), ("block_number", "int64"),
    ("transaction_hash", "string"), ("log_index", "int64"),
    ("exchange_address", "string"), ("condition_id", "string"),
    ("outcome", "string"), ("market_slug", "string"), ("event_slug", "string"),
    ("question", "string"), ("outcome_token_side", "string"),
    ("winning_outcome", "string"),
)
CACHE_SCHEMA = (("block_number", "int64"), ("timestamp", "int64"))
RESOURCE_CONTRACT = {"memory_limit": "8GB", "threads": 4, "spill": "0B"}
LIMITATIONS = [
    "Coverage is against the frozen local cache, not independent on-chain clock truth.",
    "Native log completeness and economic direction are outside this audit.",
    "Historical published timestamp equality is not checked and no timestamp fallback is used.",
    "Input identity uses size, modification time and full footer hash, not full body-content hashes.",
]


class AuditBlocked(ValueError):
    """A scalar contract gate failed; completed checkpoints remain available."""


def atomic_json(path: Path, value: dict[str, Any]) -> None:
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


def footer(path: Path, schema: tuple[tuple[str, str], ...],
           projected_columns: tuple[str, ...], expected_rows: int) -> dict[str, Any]:
    import pyarrow.parquet as pq

    if not path.is_file() or path.is_symlink():
        raise AuditBlocked("Input must be a regular nonsymlink Parquet file")
    initial_stat = path.stat()
    parquet = pq.ParquetFile(path)
    observed = tuple((field.name, str(field.type)) for field in parquet.schema_arrow)
    if observed != schema:
        raise AuditBlocked("Frozen physical schema differs")
    if parquet.metadata.num_rows != expected_rows:
        raise AuditBlocked("Frozen footer row count differs")
    with path.open("rb") as handle:
        handle.seek(-8, os.SEEK_END)
        trailer = handle.read(8)
        if trailer[4:] != b"PAR1":
            raise AuditBlocked("Invalid Parquet footer trailer")
        length = int.from_bytes(trailer[:4], "little")
        if length <= 0 or length > initial_stat.st_size - 8:
            raise AuditBlocked("Invalid Parquet footer length")
        handle.seek(-8 - length, os.SEEK_END)
        footer_sha256 = hashlib.sha256(handle.read(length) + trailer).hexdigest()
    final_stat = path.stat()
    if (initial_stat.st_size, initial_stat.st_mtime_ns) != (final_stat.st_size, final_stat.st_mtime_ns):
        raise AuditBlocked("Input changed during footer read")
    projected = {}
    names = [field.name for field in parquet.schema_arrow]
    for name in projected_columns:
        column = names.index(name)
        chunks = [parquet.metadata.row_group(i).column(column)
                  for i in range(parquet.metadata.num_row_groups)]
        projected[name] = {
            "compressed_bytes": sum(chunk.total_compressed_size for chunk in chunks),
            "encoded_uncompressed_bytes": sum(chunk.total_uncompressed_size for chunk in chunks),
        }
    return {"path": str(path), "bytes": initial_stat.st_size,
            "mtime_ns": initial_stat.st_mtime_ns, "footer_sha256": footer_sha256,
            "rows": parquet.metadata.num_rows, "row_groups": parquet.metadata.num_row_groups,
            "schema": [list(field) for field in observed], "projected_columns": projected}


def inspect_inventory(resolved: Path, cache: Path, expected_resolved_rows: int,
                      expected_cache_rows: int) -> dict[str, Any]:
    return {
        "resolved": footer(resolved, RESOLVED_SCHEMA, ("block_number",), expected_resolved_rows),
        "cache": footer(cache, CACHE_SCHEMA, ("block_number", "timestamp"), expected_cache_rows),
        "planned_parquet_body_passes": {"resolved_block_number": 1, "cache_block_number_timestamp": 1},
        "planned_footer_opens": 4,
        "physical_io_note": "Projection byte sums are not total physical I/O; footer reads and engine/OS overhead are additional.",
    }


def _quoted(path: Path) -> str:
    return str(path).replace("'", "''")


def _record(con: Any, sql: str) -> dict[str, Any]:
    cursor = con.execute(sql)
    return dict(zip((item[0] for item in cursor.description), cursor.fetchone()))


def prepare_cache(con: Any, cache: Path, expected_rows: int) -> dict[str, Any]:
    # Exactly one cache Parquet body pass; all later cache scans use this skinny table.
    con.execute(f"CREATE TEMP TABLE timestamp_cache AS SELECT block_number,timestamp FROM read_parquet('{_quoted(cache)}',hive_partitioning=false)")
    checks = _record(con, """SELECT count(*) cache_rows,
        count(*) FILTER(WHERE block_number IS NULL) cache_null_block_rows,
        count(*) FILTER(WHERE block_number<=0) cache_nonpositive_block_rows,
        count(*) FILTER(WHERE timestamp IS NULL) cache_null_timestamp_rows,
        count(*) FILTER(WHERE timestamp<=0) cache_nonpositive_timestamp_rows
        FROM timestamp_cache""")
    checks.update(_record(con, """SELECT count(*) cache_duplicate_block_keys,
        coalesce(sum(key_rows-1),0)::BIGINT cache_duplicate_excess_rows
        FROM (SELECT block_number,count(*) key_rows FROM timestamp_cache
              GROUP BY block_number HAVING count(*)>1)"""))
    checks["cache_footer_count_matches"] = checks["cache_rows"] == expected_rows
    zero_gates = ["cache_null_block_rows", "cache_nonpositive_block_rows", "cache_null_timestamp_rows",
                  "cache_nonpositive_timestamp_rows", "cache_duplicate_block_keys", "cache_duplicate_excess_rows"]
    checks["clock_order_checked"] = False
    if checks["cache_footer_count_matches"] and all(checks[key] == 0 for key in zero_gates):
        checks.update(_record(con, """SELECT
            count(*) FILTER(WHERE timestamp<previous_timestamp) cache_clock_reversals,
            count(*) FILTER(WHERE timestamp=previous_timestamp) cache_equal_timestamp_steps
            FROM (SELECT timestamp,lag(timestamp) OVER(ORDER BY block_number) previous_timestamp
                  FROM timestamp_cache)"""))
        checks["clock_order_checked"] = True
        zero_gates.append("cache_clock_reversals")
    checks["failed_gate_counts"] = {key: checks[key] for key in zero_gates if checks[key] != 0}
    if not checks["cache_footer_count_matches"]:
        checks["failed_gate_counts"]["cache_footer_count_matches"] = False
    checks["gates_passed"] = checks["clock_order_checked"] and not checks["failed_gate_counts"]
    return checks


def scan_required_rows(con: Any, resolved: Path, expected_rows: int) -> dict[str, Any]:
    # One native Parquet body pass. DISTINCT stores only missing block keys, not
    # every required native key or row. NULL keys are separately counted below.
    con.execute(f"CREATE TEMP VIEW required_native AS SELECT block_number FROM read_parquet('{_quoted(resolved)}',hive_partitioning=false)")
    checks = _record(con, """SELECT count(*) required_rows,
        count(*) FILTER(WHERE n.block_number IS NULL) required_null_block_rows,
        count(*) FILTER(WHERE n.block_number<=0) required_nonpositive_block_rows,
        count(*) FILTER(WHERE c.block_number IS NOT NULL) matched_required_rows,
        count(*) FILTER(WHERE c.block_number IS NULL) missing_required_rows,
        count(DISTINCT CASE WHEN c.block_number IS NULL THEN n.block_number END) missing_required_blocks,
        count(*) FILTER(WHERE c.block_number IS NULL AND n.block_number IS NULL) missing_required_null_block_rows
        FROM required_native n LEFT JOIN timestamp_cache c USING(block_number)""")
    checks["missing_block_count_semantics"] = "Distinct missing nonnull native keys, including nonpositive invalid keys; NULL rows counted separately"
    # With independently unique cache keys, the left join has at most one result
    # per native row. The joined count must still match the native footer exactly.
    checks["joined_rows"] = checks["required_rows"]
    checks["required_footer_count_matches"] = checks["required_rows"] == expected_rows
    checks["join_preserves_rows"] = checks["joined_rows"] == expected_rows
    checks["row_coverage_conserved"] = checks["matched_required_rows"] + checks["missing_required_rows"] == expected_rows
    zero_gates = ["required_null_block_rows", "required_nonpositive_block_rows", "missing_required_rows",
                  "missing_required_blocks", "missing_required_null_block_rows"]
    checks["failed_gate_counts"] = {key: checks[key] for key in zero_gates if checks[key] != 0}
    for key in ("required_footer_count_matches", "join_preserves_rows", "row_coverage_conserved"):
        if not checks[key]:
            checks["failed_gate_counts"][key] = False
    checks["gates_passed"] = not checks["failed_gate_counts"]
    return checks


def _profile(started: float) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # Linux reports KiB; macOS reports bytes. Keep the platform and native value.
    factor = 1 if sys.platform == "darwin" else 1024
    return {"elapsed_seconds": time.monotonic() - started,
            "user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime,
            "peak_rss_native": usage.ru_maxrss, "peak_rss_bytes": usage.ru_maxrss * factor,
            "scope": "Whole current process; CPU/RSS include startup and earlier fixture work if any"}


def run_audit(resolved: Path, cache: Path, run_dir: Path, *,
              expected_resolved_rows: int = RESOLVED_ROWS, expected_cache_rows: int = CACHE_ROWS,
              provenance: dict[str, Any] | None = None) -> dict[str, Any]:
    if resolved.is_symlink() or cache.is_symlink():
        raise AuditBlocked("Input must be a regular nonsymlink Parquet file")
    resolved, cache, run_dir = (path.resolve() for path in (resolved, cache, run_dir))
    for source in (resolved, cache):
        if run_dir == source or source in run_dir.parents or run_dir in source.parents:
            raise AuditBlocked("Output overlaps an input")
    if resolved == cache:
        raise AuditBlocked("Native source and timestamp cache must be distinct")
    run_dir.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    summary: dict[str, Any] = {
        "schema_version": 1, "stage": "polymarket_required_block_timestamp_coverage_v1",
        "status": "incomplete", "certification_status": "not_certified",
        "audit_scope": "frozen_native_required_key_cache_coverage_only",
        "resource_contract": RESOURCE_CONTRACT, "limitations": LIMITATIONS,
        "provenance": provenance or {"execution": "local_fixture"},
        "environment": {"python": sys.version, "platform": platform.platform()},
        "cache_preflight_completed": False, "required_rows_scan_completed": False,
        "final_input_identity_verified": False,
    }
    atomic_json(run_dir / "summary.json", summary)
    con = None
    inventory = None
    stage = "footer_inventory"
    completed = False
    try:
        inventory = inspect_inventory(resolved, cache, expected_resolved_rows, expected_cache_rows)
        atomic_json(run_dir / "inventory.json", inventory)
        import duckdb
        import pyarrow

        summary["environment"].update({"duckdb": duckdb.__version__, "pyarrow": pyarrow.__version__})
        con = duckdb.connect()
        con.execute("SET threads=4")
        con.execute("SET memory_limit='8GB'")
        con.execute("SET max_temp_directory_size='0B'")
        con.execute("SET temp_directory=''")
        con.execute("SET TimeZone='UTC'")
        summary["engine_settings"] = _record(con, """SELECT
            current_setting('memory_limit') memory_limit,
            current_setting('threads') threads,
            current_setting('max_temp_directory_size') max_temp_directory_size,
            current_setting('temp_directory') temp_directory,
            current_setting('TimeZone') timezone""")
        stage = "cache_preflight"
        summary["active_stage"] = stage
        atomic_json(run_dir / "summary.json", summary)
        stage_started = time.monotonic()
        cache_checks = prepare_cache(con, cache, expected_cache_rows)
        cache_checks["elapsed_seconds"] = time.monotonic() - stage_started
        atomic_json(run_dir / "cache_checks.json", cache_checks)
        summary["cache_preflight_completed"] = True
        summary["cache_gates_passed"] = cache_checks["gates_passed"]
        if not cache_checks["gates_passed"]:
            summary["blocking_gate_counts"] = cache_checks["failed_gate_counts"]
            raise AuditBlocked("Cache preflight failed before native row join")
        stage = "required_native_rows"
        summary["active_stage"] = stage
        atomic_json(run_dir / "summary.json", summary)
        stage_started = time.monotonic()
        required_checks = scan_required_rows(con, resolved, expected_resolved_rows)
        required_checks["elapsed_seconds"] = time.monotonic() - stage_started
        atomic_json(run_dir / "required_checks.json", required_checks)
        summary["required_rows_scan_completed"] = True
        summary["required_gates_passed"] = required_checks["gates_passed"]
        for key in ("required_rows", "matched_required_rows", "missing_required_rows", "missing_required_blocks"):
            summary[key] = required_checks[key]
        if not required_checks["gates_passed"]:
            summary["blocking_gate_counts"] = required_checks["failed_gate_counts"]
            raise AuditBlocked("Required native block coverage failed")
        completed = True
    except BaseException as error:
        summary["status"] = ("blocked_integrity" if isinstance(error, AuditBlocked) else
                             "interrupted" if isinstance(error, KeyboardInterrupt) else "incomplete_execution_error")
        summary["failure_stage"] = stage
        summary["error_class"] = type(error).__name__
        if isinstance(error, AuditBlocked):
            summary["failure_reason"] = str(error)
    finally:
        if inventory is not None:
            try:
                reopened = inspect_inventory(resolved, cache, expected_resolved_rows, expected_cache_rows)
                if reopened != inventory:
                    raise AuditBlocked("Frozen input identity changed during audit")
                summary["final_input_identity_verified"] = True
            except BaseException as error:
                completed = False
                summary["status"] = ("blocked_integrity" if isinstance(error, AuditBlocked) else
                                     "interrupted" if isinstance(error, KeyboardInterrupt) else "incomplete_execution_error")
                summary["failure_stage"] = "final_inventory_reopen"
                summary["error_class"] = type(error).__name__
                if isinstance(error, AuditBlocked):
                    summary["failure_reason"] = str(error)
        if con is not None:
            con.close()
        if completed and summary["final_input_identity_verified"]:
            summary["status"] = "required_block_cache_coverage_complete"
            summary["certification_status"] = "cache_coverage_and_internal_clock_consistency_only"
            summary.pop("active_stage", None)
        summary["resource_profile"] = _profile(started)
        atomic_json(run_dir / "summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--external-profile-path", type=Path,
                        help="Provenance only: path of the external resource profile, not an input")
    args = parser.parse_args(argv)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from production_guard import require_production_host

    require_production_host()
    canonical = Path("/home/ubuntu/prediction_markets")
    script = Path(__file__).resolve()
    if Path.cwd().resolve() != canonical or script != canonical / "scripts/audit_polymarket_timestamp_coverage.py":
        raise RuntimeError("Run the committed script from the canonical checkout")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=canonical, text=True).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_head) or head != args.expected_head:
        raise RuntimeError("Canonical HEAD differs from the frozen audit expectation")
    script_bytes = script.read_bytes()
    committed = subprocess.check_output(["git", "show", "HEAD:scripts/audit_polymarket_timestamp_coverage.py"], cwd=canonical)
    if committed != script_bytes:
        raise RuntimeError("Executed audit script differs from its committed source")
    def interrupt(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupt)
    summary = run_audit(RESOLVED_PATH, CACHE_PATH, args.run_dir,
        provenance={"canonical_head": head, "script_path": str(script),
                    "script_sha256": hashlib.sha256(script_bytes).hexdigest(),
                    "command": sys.argv if argv is None else [str(script), *argv],
                    "external_profile_path": str(args.external_profile_path) if args.external_profile_path else None})
    print(json.dumps({"status": summary["status"], "run_dir": str(args.run_dir),
                      "required_rows": summary.get("required_rows")}, allow_nan=False))
    return 0 if summary["status"] == "required_block_cache_coverage_complete" else 130 if summary["status"] == "interrupted" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, FileExistsError, AuditBlocked):
        print("Audit startup refused: production host, committed source, frozen HEAD, input/output or no-overwrite gate failed.", file=sys.stderr)
        raise SystemExit(2)
