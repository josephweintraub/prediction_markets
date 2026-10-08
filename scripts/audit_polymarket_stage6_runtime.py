"""Reproduce frozen Stage 6 SQL using synthetic Parquet only.

The production builder is parsed as text and never imported or executed. Its
exact COPY f-string is rendered with bounded temporary fixture paths. Default
optimizer behavior is an observation, not an assertion of correct execution.
No production dataset, credential, network service, or pipeline cache is read.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import sys
import tempfile
from urllib.parse import urlsplit

FROZEN_BUILDER_BLOB = "19bacf3c87494b782ca1c87213f0e8d72191ecb5"
MAX_SCRATCH_BYTES = 1024**2
MAX_REPORT_BYTES = 1024**2
SUBSTITUTIONS = frozenset({"ts_expr", "ts_join", "usdc_scale", "ctf_scale",
                           "RESOLVED_TRADES_PATH", "TRADES_OUTPUT_DIR"})
VALUE_FIELDS = ("proxyWallet", "timestamp", "conditionId", "usdcSize", "price",
                "side", "outcome", "eventSlug", "is_maker", "counterparty", "year_month")
FIXTURE_FIELDS = (
    "order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
    "maker_amount_filled", "taker_amount_filled", "fee", "block_number",
    "transaction_hash", "log_index", "exchange_address", "condition_id",
    "outcome", "market_slug", "event_slug", "question", "outcome_token_side", "winning_outcome",
)
FIXTURE_ROWS = [
    dict(zip(FIXTURE_FIELDS, (
        "synthetic_order_buy", "wallet_A", "wallet_B", "0", "100000000000000000001",
        3871000, 3950000, 0, 83633985, "synthetic_tx_buy", 1, "synthetic_exchange",
        "synthetic_market_buy", "Down", "synthetic_buy", "synthetic_buy_event",
        "Synthetic maker BUY", "taker", "Down"))),
    dict(zip(FIXTURE_FIELDS, (
        "synthetic_order_sell", "wallet_C", "wallet_D", "100000000000000000002", "0",
        10000000, 4000000, 0, 83633986, "synthetic_tx_sell", 2, "synthetic_exchange",
        "synthetic_market_sell", "Up", "synthetic_sell", "synthetic_sell_event",
        "Synthetic maker SELL", "maker", "Down"))),
]
TIMESTAMP_ROWS = [{"block_number": 83633985, "timestamp": 1772388001},
                  {"block_number": 83633986, "timestamp": 1772388003}]
APPROX_EXPR = "(1667260800 + (rt.block_number - 21000000) * 1.676312)::BIGINT"


class RuntimeAuditBlocked(ValueError):
    pass


def git_blob(source: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(source)).encode() + b"\0" + source).hexdigest()


def exact_json(value: dict) -> bytes:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True,
                      allow_nan=False).encode("utf-8")


def extract_frozen_copy(builder: Path) -> tuple[ast.JoinedStr, dict]:
    """Fail closed on the builder identity and unexpected SQL interpolation."""
    source = builder.read_bytes()
    blob = git_blob(source)
    if blob != FROZEN_BUILDER_BLOB:
        raise RuntimeAuditBlocked("builder differs from the approved frozen Git blob")
    tree = ast.parse(source)
    functions = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name == "run_stage6"]
    if len(functions) != 1:
        raise RuntimeAuditBlocked("expected exactly one run_stage6 function")
    candidates = [node for node in ast.walk(functions[0])
                  if isinstance(node, ast.JoinedStr) and any(
                      isinstance(part, ast.Constant) and isinstance(part.value, str)
                      and "WITH expanded AS" in part.value for part in node.values)]
    if len(candidates) != 1:
        raise RuntimeAuditBlocked("expected exactly one Stage 6 expansion COPY f-string")
    template = candidates[0]
    # Validate all expression nodes without evaluating any builder expression.
    render_copy(template, {name: name for name in SUBSTITUTIONS})
    segment = ast.get_source_segment(source.decode("utf-8"), template)
    return template, {"path": str(builder), "git_blob": blob,
                      "source_sha256": hashlib.sha256(source).hexdigest(),
                      "sql_source_start_line": template.lineno,
                      "sql_source_end_line": template.end_lineno,
                      "sql_template_sha256": hashlib.sha256(segment.encode()).hexdigest(),
                      "sql_template_source": segment,
                      "builder_imported_or_executed": False}


def render_copy(template: ast.JoinedStr, values: dict) -> str:
    pieces, used = [], set()
    for part in template.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            pieces.append(part.value)
        elif (isinstance(part, ast.FormattedValue) and isinstance(part.value, ast.Name)
              and part.conversion == -1 and part.format_spec is None
              and part.value.id in SUBSTITUTIONS):
            pieces.append(str(values[part.value.id]))
            used.add(part.value.id)
        else:
            raise RuntimeAuditBlocked("unapproved expression in Stage 6 SQL f-string")
    if used != SUBSTITUTIONS:
        raise RuntimeAuditBlocked("Stage 6 SQL substitution contract differs")
    sql = "".join(pieces)
    if not sql.lstrip().startswith("COPY (") or "WITH expanded AS" not in sql:
        raise RuntimeAuditBlocked("expected the frozen Stage 6 COPY shape")
    return sql


def expected_rows(timestamp_mode: str) -> list[dict]:
    timestamps = {row["block_number"]: row["timestamp"] for row in TIMESTAMP_ROWS}
    result = []
    for native in FIXTURE_ROWS:
        maker_sell = native["outcome_token_side"] == "maker"
        cash_micro = native["taker_amount_filled"] if maker_sell else native["maker_amount_filled"]
        shares_micro = native["maker_amount_filled"] if maker_sell else native["taker_amount_filled"]
        timestamp = timestamps[native["block_number"]] if timestamp_mode == "cached" else round(
            1667260800 + (native["block_number"] - 21000000) * 1.676312)
        token = native["maker_asset_id"] if maker_sell else native["taker_asset_id"]
        for is_maker in (True, False):
            side = "SELL" if maker_sell == is_maker else "BUY"
            result.append(dict(zip(VALUE_FIELDS, (
                native["maker"] if is_maker else native["taker"], timestamp, token,
                cash_micro / 1000000.0, (cash_micro / 1000000.0) / (shares_micro / 1000000.0),
                side, native["outcome"], native["event_slug"], is_maker,
                native["taker"] if is_maker else native["maker"],
                datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m")))))
    return sorted(result, key=lambda row: (row["conditionId"], not row["is_maker"]))


def scratch_bytes(root: Path) -> int:
    total = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    if total > MAX_SCRATCH_BYTES:
        raise RuntimeAuditBlocked("synthetic scratch exceeds 1MiB; no production inputs are allowed")
    return total


def run_case(template, fixture: Path, scratch: Path, optimizer: str,
             timestamp_mode: str, threads: int) -> dict:
    import duckdb

    name = f"{optimizer}_{timestamp_mode}_threads{threads}"
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='64MB'")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET threads={threads}")
        available = [row[0] for row in con.execute("SELECT name FROM duckdb_optimizers()").fetchall()]
        metadata = {"name": name, "optimizer": optimizer, "timestamp_mode": timestamp_mode,
                    "threads": threads, "memory_limit": "64MB", "preserve_insertion_order": False,
                    "default_behavior_is_observational": optimizer == "default"}
        if optimizer == "common_subplan_disabled":
            if "common_subplan" not in available:
                return {**metadata, "status": "optimizer_unavailable", "unavailable_optimizer": "common_subplan"}
            con.execute("SET disabled_optimizers='common_subplan'")
        elif optimizer == "all_disabled":
            con.execute("PRAGMA disable_optimizer")
        elif optimizer != "default":
            raise RuntimeAuditBlocked("unknown optimizer case")
        con.execute("CREATE TABLE block_ts(block_number BIGINT,timestamp BIGINT)")
        con.executemany("INSERT INTO block_ts VALUES (?,?)",
                        [(row["block_number"], row["timestamp"]) for row in TIMESTAMP_ROWS])
        output = scratch / name
        output.mkdir()
        substitutions = {"usdc_scale": 1000000, "ctf_scale": 1000000,
                         "RESOLVED_TRADES_PATH": fixture, "TRADES_OUTPUT_DIR": output,
                         "ts_expr": f"COALESCE(bt.timestamp, {APPROX_EXPR})" if timestamp_mode == "cached" else APPROX_EXPR,
                         "ts_join": "LEFT JOIN block_ts bt ON rt.block_number = bt.block_number" if timestamp_mode == "cached" else ""}
        sql = render_copy(template, substitutions)
        # EXPLAIN the same COPY command; EXPLAIN without ANALYZE does not write it.
        plans = dict(con.execute("EXPLAIN " + sql).fetchall())
        con.execute(sql)
        scratch_bytes(scratch)
        cursor = con.execute("SELECT " + ",".join(VALUE_FIELDS) +
                             " FROM read_parquet(?) ORDER BY conditionId,is_maker DESC",
                             [str(output / "**/*.parquet")])
        actual = [dict(zip(VALUE_FIELDS, row)) for row in cursor.fetchall()]
        expected = expected_rows(timestamp_mode)
        mismatches = [{"row": index, "fields": [field for field in VALUE_FIELDS
                        if observed[field] != wanted[field]]}
                      for index, (observed, wanted) in enumerate(zip(actual, expected))
                      if observed != wanted]
        correct = actual == expected
        normalized = sql.replace(str(scratch), "<synthetic_scratch>")
        return {**metadata, "status": "complete", "exact_rows_match": correct,
                "output_rows": actual, "expected_rows": expected, "row_mismatches": mismatches,
                "row_count_matches": len(actual) == len(expected),
                "maker_rows_match": [row for row in actual if row["is_maker"]] ==
                                    [row for row in expected if row["is_maker"]],
                "output_float_hex": [{field: row[field].hex() for field in ("usdcSize", "price")}
                                     for row in actual],
                "rendered_sql_normalized": normalized,
                "rendered_sql_normalized_sha256": hashlib.sha256(normalized.encode()).hexdigest(),
                "explain": plans,
                "output_files": [{"relative_path": str(path.relative_to(scratch)),
                                  "bytes": path.stat().st_size,
                                  "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                                 for path in sorted(output.rglob("*.parquet"))]}
    finally:
        con.close()


def build_report(builder: Path) -> dict:
    import duckdb

    template, source = extract_frozen_copy(builder)
    con = duckdb.connect()
    version = con.execute("PRAGMA version").fetchall()
    optimizers = [row[0] for row in con.execute("SELECT name FROM duckdb_optimizers()").fetchall()]
    con.close()
    with tempfile.TemporaryDirectory(prefix="polymarket_stage6_synthetic_") as temporary:
        scratch = Path(temporary)
        fixture = scratch / "resolved_synthetic.parquet"
        con = duckdb.connect()
        try:
            integer_fields = {"maker_amount_filled", "taker_amount_filled", "fee", "block_number", "log_index"}
            con.execute("CREATE TABLE synthetic_resolved(" + ",".join(
                field + (" BIGINT" if field in integer_fields else " VARCHAR")
                for field in FIXTURE_FIELDS) + ")")
            con.executemany("INSERT INTO synthetic_resolved VALUES (" + ",".join("?" for _ in FIXTURE_FIELDS) + ")",
                            [tuple(row[field] for field in FIXTURE_FIELDS) for row in FIXTURE_ROWS])
            con.execute("COPY synthetic_resolved TO ? (FORMAT PARQUET,COMPRESSION ZSTD)", [str(fixture)])
        finally:
            con.close()
        fixture_identity = {"bytes": fixture.stat().st_size,
                            "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()}
        cases = [run_case(template, fixture, scratch, optimizer, timestamp_mode, threads)
                 for timestamp_mode in ("cached", "approx") for threads in (1, 4)
                 for optimizer in ("default", "common_subplan_disabled", "all_disabled")]
        maximum_scratch = scratch_bytes(scratch)
    defaults = [case for case in cases if case["status"] == "complete" and case["optimizer"] == "default"]
    controls = [case for case in cases if case["status"] == "complete" and case["optimizer"] != "default"]
    control_failures = [case["name"] for case in controls if not case["exact_rows_match"]]
    discrepant_defaults = [case["name"] for case in defaults if not case["exact_rows_match"]]
    status = "blocked_synthetic_control_failure" if control_failures else (
        "complete_synthetic_default_discrepancy" if discrepant_defaults else "complete_synthetic_default_matches")
    return {"schema_version": "polymarket_stage6_runtime_v1", "status": status,
            "data_certified": False, "historical_writer_or_engine_version_proven": False,
            "scope": "Exact frozen SQL on two synthetic resolved rows only; no historical prevalence or production repair.",
            "environment": {"python": platform.python_version(), "python_executable": sys.executable,
                            "system": platform.system(), "machine": platform.machine(),
                            "duckdb_version": duckdb.__version__, "duckdb_build": version,
                            "available_optimizers": optimizers},
            "source_snapshot": source,
            "settings_comparison": {
                "frozen_get_con": {"memory_limit": "200GB", "threads": 12,
                                   "temp_directory": "/mnt/data/tmp", "max_temp_directory_size": "200GB",
                                   "preserve_insertion_order": False},
                "synthetic": {"memory_limit": "64MB", "threads": [1, 4],
                              "temp_directory": "default; bounded two-row fixture does not require spill",
                              "preserve_insertion_order": False},
                "note": "Memory and threads are deliberately bounded for synthetic execution. Current runtime identity does not establish the historical build runtime."},
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "input_fixture": {"resolved_rows": FIXTURE_ROWS, "timestamp_rows": TIMESTAMP_ROWS,
                              "resolved_parquet": fixture_identity, "all_wallet_pairs_distinct": True},
            "caps": {"max_scratch_bytes": MAX_SCRATCH_BYTES, "max_report_bytes": MAX_REPORT_BYTES},
            "actual_scratch_bytes": maximum_scratch, "scratch_removed": True,
            "production_inputs_read": False, "production_builder_imported_or_executed": False,
            "default_discrepancy_cases": discrepant_defaults, "control_failure_cases": control_failures,
            "cases": cases}


def write_immutable(destination: Path, report: dict) -> None:
    encoded = exact_json(report)
    if len(encoded) > MAX_REPORT_BYTES:
        raise RuntimeAuditBlocked("complete report exceeds 1MiB; no truncation")
    destination.mkdir(parents=True, exist_ok=False)
    temporary = destination / "report.json.partial"
    with temporary.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, destination / "report.json")


def package_provenance(path: Path, actual_version: str) -> dict:
    """Keep only public binary-wheel identity from an isolated pip report."""
    encoded = path.read_bytes()
    installs = json.loads(encoded)["install"]
    if len(installs) != 1 or installs[0]["metadata"]["name"].lower() != "duckdb":
        raise RuntimeAuditBlocked("package provenance must describe one DuckDB wheel")
    package = installs[0]
    download = package["download_info"]
    url = urlsplit(download["url"])
    if (url.scheme != "https" or url.hostname != "files.pythonhosted.org"
            or url.username or url.password or url.query or url.fragment
            or not url.path.endswith(".whl")):
        raise RuntimeAuditBlocked("package source must be a public PyPI binary-wheel URL")
    version = package["metadata"]["version"]
    if version != actual_version:
        raise RuntimeAuditBlocked("isolated package version differs from loaded DuckDB")
    digest = download["archive_info"]["hashes"]["sha256"]
    if len(digest) != 64 or any(letter not in "0123456789abcdef" for letter in digest):
        raise RuntimeAuditBlocked("invalid package SHA256")
    return {"pip_report_path": str(path), "pip_report_sha256": hashlib.sha256(encoded).hexdigest(),
            "name": "duckdb", "version": version, "wheel_url": download["url"], "wheel_sha256": digest}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--builder", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--package-provenance", type=Path)
    args = parser.parse_args()
    if args.run_dir.exists():
        raise RuntimeAuditBlocked("immutable report destination already exists")
    report = build_report(args.builder)
    if args.package_provenance:
        report["isolated_package_provenance"] = package_provenance(
            args.package_provenance, report["environment"]["duckdb_version"])
    report["command"] = sys.argv
    write_immutable(args.run_dir, report)
    print(json.dumps({"status": report["status"], "report": str(args.run_dir / "report.json"),
                      "default_discrepancy_cases": report["default_discrepancy_cases"],
                      "control_failure_cases": report["control_failure_cases"]}))
    return 2 if report["control_failure_cases"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
