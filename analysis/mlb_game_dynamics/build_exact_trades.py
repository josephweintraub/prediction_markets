#!/usr/bin/env python3
"""Build an MLB BUY-trade extract with exact Polygon block timestamps.

This intentionally starts from ``resolved_trades.parquet`` rather than the
Stage-6 trade artifact because Stage 6 permits a linear timestamp fallback.
Only candidate MLB markets are read into the working table.  Ingestion
replays are removed by the immutable event identity
``(transaction_hash, log_index, exchange_address)``; two different events
with otherwise identical economics remain separate fills.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from timestamp_provenance import (
    load_timestamp_provenance,
    validate_timestamp_provenance,
)


USDC_SCALE = 1_000_000.0
TOKEN_SCALE = 1_000_000.0
INTEGER_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
}
RAW_COLUMNS = (
    "order_hash",
    "maker",
    "taker",
    "maker_asset_id",
    "taker_asset_id",
    "maker_amount_filled",
    "taker_amount_filled",
    "fee",
    "block_number",
    "transaction_hash",
    "log_index",
    "exchange_address",
    "condition_id",
    "outcome",
    "winning_outcome",
    "outcome_token_side",
)
IDENTITY_COLUMNS = ("transaction_hash", "log_index", "exchange_address")
OUTPUT_COLUMNS = (
    "market_id",
    "token_id",
    "block_number",
    "timestamp",
    "transaction_hash",
    "log_index",
    "exchange_address",
    "proxyWallet",
    "counterparty",
    "is_maker",
    "outcome",
    "winning_outcome",
    "price",
    "usdcSize",
)


class ExactTradeBuildError(ValueError):
    """Raised when the exact-timestamp extract cannot be proven valid."""


def _path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _quoted(path: str | Path) -> str:
    return str(_path(path)).replace("'", "''")


def _schema(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ExactTradeBuildError(f"Unsafe DuckDB relation name: {relation!r}")
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    columns: tuple[str, ...] | set[str],
    label: str,
) -> dict[str, str]:
    schema = _schema(con, relation)
    missing = sorted(set(columns) - schema.keys())
    if missing:
        raise ExactTradeBuildError(f"{label} is missing required columns: {missing}")
    return schema


def verify_output_timestamps(
    con: duckdb.DuckDBPyConnection,
    output_relation: str,
    cache_relation: str,
) -> dict[str, int]:
    """Verify that every output row contains its cache's exact timestamp."""

    output_schema = _require_columns(
        con,
        output_relation,
        {"block_number", "timestamp"},
        "Exact MLB trade output",
    )
    for column in ("block_number", "timestamp"):
        if output_schema[column] not in INTEGER_TYPES:
            raise ExactTradeBuildError(
                f"Output {column} must be an integer; found {output_schema[column]}"
            )

    rows, null_keys = con.execute(
        f"""
        SELECT COUNT(*), COUNT(*) FILTER (
            WHERE block_number IS NULL OR timestamp IS NULL
        )
        FROM {output_relation}
        """
    ).fetchone()
    if null_keys:
        raise ExactTradeBuildError("Exact MLB trade output has null block/timestamp keys")

    missing = int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM {output_relation} output
            ANTI JOIN {cache_relation} cache USING (block_number)
            """
        ).fetchone()[0]
    )
    mismatches = int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM {output_relation} output
            JOIN {cache_relation} cache USING (block_number)
            WHERE output.timestamp != cache.timestamp
            """
        ).fetchone()[0]
    )
    if missing or mismatches:
        raise ExactTradeBuildError(
            "Exact output timestamp verification failed: "
            f"missing cache rows={missing}, timestamp mismatches={mismatches}"
        )
    return {
        "rows_checked": int(rows),
        "missing_cache_rows": 0,
        "timestamp_mismatches": 0,
    }


def _validate_inputs(con: duckdb.DuckDBPyConnection) -> None:
    raw_schema = _require_columns(
        con, "raw_input", set(RAW_COLUMNS), "Resolved-trade source"
    )
    for column in (
        "maker_amount_filled",
        "taker_amount_filled",
        "fee",
        "block_number",
        "log_index",
    ):
        if raw_schema[column] not in INTEGER_TYPES:
            raise ExactTradeBuildError(
                f"Resolved-trade source {column} must be an integer; "
                f"found {raw_schema[column]}"
            )
    _require_columns(con, "candidate_input", {"market_id"}, "Candidate markets")
    flag_schema = _require_columns(
        con, "wallet_flag_input", {"proxyWallet", "is_nonhuman"}, "Wallet flags"
    )
    if flag_schema["is_nonhuman"] != "BOOLEAN":
        raise ExactTradeBuildError(
            "Wallet flags is_nonhuman must be BOOLEAN; "
            f"found {flag_schema['is_nonhuman']}"
        )


def _assert_valid_candidates(con: duckdb.DuckDBPyConnection) -> None:
    rows, unique_ids, null_or_blank = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT market_id),
               COUNT(*) FILTER (WHERE market_id IS NULL OR TRIM(market_id) = '')
        FROM candidate_input
        """
    ).fetchone()
    if rows == 0:
        raise ExactTradeBuildError("Candidate market input must be nonempty")
    if null_or_blank:
        raise ExactTradeBuildError("Candidate markets contain null or blank market_id values")
    if rows != unique_ids:
        raise ExactTradeBuildError("Candidate market_id values must be unique")


def _assert_valid_candidate_source(con: duckdb.DuckDBPyConnection) -> None:
    source_rows = int(con.execute("SELECT COUNT(*) FROM candidate_source").fetchone()[0])
    if source_rows == 0:
        raise ExactTradeBuildError(
            "Candidate source is empty; no candidate market_id matched a resolved-trade row"
        )

    missing_count = int(
        con.execute(
            """
            SELECT COUNT(*)
            FROM candidate_markets candidate
            ANTI JOIN (
                SELECT DISTINCT condition_id AS market_id FROM candidate_source
            ) source USING (market_id)
            """
        ).fetchone()[0]
    )
    if missing_count:
        sample = [
            row[0]
            for row in con.execute(
                """
                SELECT candidate.market_id
                FROM candidate_markets candidate
                ANTI JOIN (
                    SELECT DISTINCT condition_id AS market_id FROM candidate_source
                ) source USING (market_id)
                ORDER BY candidate.market_id
                LIMIT 10
                """
            ).fetchall()
        ]
        raise ExactTradeBuildError(
            f"{missing_count} candidate markets have no resolved-trade source rows; "
            f"sample={sample}"
        )

    null_checks = " OR ".join(f"{column} IS NULL" for column in RAW_COLUMNS)
    null_count = int(
        con.execute(f"SELECT COUNT(*) FROM candidate_source WHERE {null_checks}").fetchone()[0]
    )
    if null_count:
        raise ExactTradeBuildError(
            f"Candidate source contains {null_count} rows with null required payload fields"
        )
    bad_side = int(
        con.execute(
            """
            SELECT COUNT(*) FROM candidate_source
            WHERE outcome_token_side NOT IN ('maker', 'taker')
            """
        ).fetchone()[0]
    )
    if bad_side:
        raise ExactTradeBuildError(
            f"Candidate source contains {bad_side} rows with invalid outcome_token_side"
        )


def _deduplicate_fills(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    payload_columns = ", ".join(RAW_COLUMNS)
    identity_columns = ", ".join(IDENTITY_COLUMNS)
    con.execute(
        f"CREATE TEMP TABLE unique_payloads AS "
        f"SELECT DISTINCT {payload_columns} FROM candidate_source"
    )
    contradictions = con.execute(
        f"""
        SELECT {identity_columns}, COUNT(*) AS payload_count
        FROM unique_payloads
        GROUP BY {identity_columns}
        HAVING COUNT(*) > 1
        ORDER BY transaction_hash, log_index, exchange_address
        LIMIT 10
        """
    ).fetchall()
    if contradictions:
        raise ExactTradeBuildError(
            "One or more immutable event identities map to contradictory payloads; "
            f"sample={contradictions}"
        )

    con.execute("CREATE TEMP TABLE fills AS SELECT * FROM unique_payloads")
    raw_rows = int(con.execute("SELECT COUNT(*) FROM candidate_source").fetchone()[0])
    distinct_fills = int(con.execute("SELECT COUNT(*) FROM fills").fetchone()[0])
    return raw_rows, distinct_fills, raw_rows - distinct_fills


def _assert_block_coverage(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    source_blocks = int(
        con.execute("SELECT COUNT(DISTINCT block_number) FROM fills").fetchone()[0]
    )
    missing_blocks = int(
        con.execute(
            """
            SELECT COUNT(*)
            FROM (SELECT DISTINCT block_number FROM fills) source
            ANTI JOIN exact_cache cache USING (block_number)
            """
        ).fetchone()[0]
    )
    matched_blocks = int(
        con.execute(
            """
            SELECT COUNT(*)
            FROM (SELECT DISTINCT block_number FROM fills) source
            SEMI JOIN exact_cache cache USING (block_number)
            """
        ).fetchone()[0]
    )
    if missing_blocks:
        sample = [
            row[0]
            for row in con.execute(
                """
                SELECT source.block_number
                FROM (SELECT DISTINCT block_number FROM fills) source
                ANTI JOIN exact_cache cache USING (block_number)
                ORDER BY source.block_number LIMIT 10
                """
            ).fetchall()
        ]
        raise ExactTradeBuildError(
            f"Exact timestamp cache is missing {missing_blocks} candidate-source blocks; "
            f"sample={sample}"
        )
    if matched_blocks != source_blocks:
        raise ExactTradeBuildError("Candidate-source block coverage did not reconcile")
    return source_blocks, matched_blocks, missing_blocks


def _create_buy_relations(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE buy_all AS
        SELECT
            condition_id::VARCHAR AS market_id,
            (CASE WHEN outcome_token_side = 'maker'
                  THEN maker_asset_id ELSE taker_asset_id END)::VARCHAR AS token_id,
            block_number::BIGINT AS block_number,
            cache.timestamp::BIGINT AS timestamp,
            transaction_hash::VARCHAR AS transaction_hash,
            log_index::INTEGER AS log_index,
            exchange_address::VARCHAR AS exchange_address,
            (CASE WHEN outcome_token_side = 'maker' THEN taker ELSE maker END)::VARCHAR
                AS proxyWallet,
            (CASE WHEN outcome_token_side = 'maker' THEN maker ELSE taker END)::VARCHAR
                AS counterparty,
            (outcome_token_side = 'taker')::BOOLEAN AS is_maker,
            outcome::VARCHAR AS outcome,
            winning_outcome::VARCHAR AS winning_outcome,
            (CASE WHEN outcome_token_side = 'maker'
                  THEN (taker_amount_filled / {USDC_SCALE})
                       / NULLIF(maker_amount_filled / {TOKEN_SCALE}, 0)
                  ELSE (maker_amount_filled / {USDC_SCALE})
                       / NULLIF(taker_amount_filled / {TOKEN_SCALE}, 0)
             END)::DOUBLE AS price,
            (CASE WHEN outcome_token_side = 'maker'
                  THEN taker_amount_filled / {USDC_SCALE}
                  ELSE maker_amount_filled / {USDC_SCALE}
             END)::DOUBLE AS usdcSize
        FROM fills
        INNER JOIN exact_cache cache USING (block_number)
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE price_eligible AS
        SELECT * FROM buy_all WHERE price > 0.01 AND price < 0.99
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE output_rows AS
        SELECT eligible.*
        FROM price_eligible eligible
        WHERE NOT EXISTS (
            SELECT 1 FROM bot_wallets bot
            WHERE bot.proxyWallet = eligible.proxyWallet
        )
        """
    )


def _reconciled_counts(
    con: duckdb.DuckDBPyConnection,
    raw_rows: int,
    distinct_fills: int,
    duplicate_replays: int,
    source_blocks: int,
    matched_blocks: int,
    missing_blocks: int,
) -> dict[str, int | float]:
    price_eligible = int(con.execute("SELECT COUNT(*) FROM price_eligible").fetchone()[0])
    output_rows, output_dollars = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(usdcSize), 0.0) FROM output_rows"
    ).fetchone()
    output_rows = int(output_rows)
    price_exclusions = distinct_fills - price_eligible
    bot_exclusions = price_eligible - output_rows
    if raw_rows != distinct_fills + duplicate_replays:
        raise ExactTradeBuildError("Replay-deduplication counts did not reconcile")
    if distinct_fills != price_exclusions + bot_exclusions + output_rows:
        raise ExactTradeBuildError("Filter attrition counts did not reconcile")
    return {
        "raw_candidate_rows": raw_rows,
        "distinct_fills": distinct_fills,
        "duplicate_replays": duplicate_replays,
        "source_blocks": source_blocks,
        "matched_blocks": matched_blocks,
        "missing_blocks": missing_blocks,
        "price_exclusions": price_exclusions,
        "bot_exclusions": bot_exclusions,
        "output_buy_rows": output_rows,
        "output_buy_dollars": float(output_dollars),
    }


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _validate_run_destination(run_dir: Path, input_paths: list[Path]) -> None:
    """Reject an existing, broad, or input-overlapping output directory."""

    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if run_dir in dangerous:
        raise ExactTradeBuildError(f"Refusing dangerous run directory: {run_dir}")
    for input_path in input_paths:
        if _paths_overlap(run_dir, input_path):
            raise ExactTradeBuildError(
                f"Run directory must not collide with an input path: {input_path}"
            )
    if run_dir.exists():
        raise FileExistsError(f"Immutable run directory already exists: {run_dir}")


def _create_input_views(
    con: duckdb.DuckDBPyConnection,
    raw_path: Path,
    candidates_path: Path,
    cache_path: Path,
    wallet_flags_path: Path,
) -> None:
    con.execute(
        f"CREATE TEMP VIEW raw_input AS SELECT * FROM read_parquet('{_quoted(raw_path)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW candidate_input AS "
        f"SELECT * FROM read_parquet('{_quoted(candidates_path)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW exact_cache AS SELECT * FROM read_parquet('{_quoted(cache_path)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW wallet_flag_input AS "
        f"SELECT * FROM read_parquet('{_quoted(wallet_flags_path)}')"
    )


def _write_and_verify_staging_run(
    con: duckdb.DuckDBPyConnection,
    staging_dir: Path,
    final_run_dir: Path,
    report_base: dict[str, Any],
    counts: dict[str, int | float],
) -> dict[str, Any]:
    output = staging_dir / "exact_trades.parquet"
    audit = staging_dir / "build_audit.json"
    con.execute(
        f"""
        COPY (
            SELECT {', '.join(OUTPUT_COLUMNS)} FROM output_rows
            ORDER BY timestamp, block_number, transaction_hash,
                     log_index, exchange_address
        ) TO '{_quoted(output)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.execute(
        f"CREATE TEMP VIEW written_output AS "
        f"SELECT * FROM read_parquet('{_quoted(output)}')"
    )
    verification = verify_output_timestamps(con, "written_output", "exact_cache")
    written_rows, written_dollars, duplicate_identities = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(usdcSize), 0.0),
               COUNT(*) - COUNT(DISTINCT (
                   transaction_hash, log_index, exchange_address
               ))
        FROM written_output
        """
    ).fetchone()
    if int(written_rows) != counts["output_buy_rows"]:
        raise ExactTradeBuildError("Written output row count did not reconcile")
    if abs(float(written_dollars) - counts["output_buy_dollars"]) > max(
        1e-9, abs(counts["output_buy_dollars"]) * 1e-12
    ):
        raise ExactTradeBuildError("Written output dollar total did not reconcile")
    if duplicate_identities:
        raise ExactTradeBuildError("Written output contains duplicate event identities")

    report = {
        **report_base,
        "counts": counts,
        "timestamp_verification": verification,
        "run_directory": str(final_run_dir),
        "outputs": {
            "exact_trades": str(final_run_dir / "exact_trades.parquet"),
            "build_audit": str(final_run_dir / "build_audit.json"),
        },
    }
    audit.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with audit.open(encoding="utf-8") as handle:
        written_report = json.load(handle)
    if written_report != report:
        raise ExactTradeBuildError("Written build audit did not round-trip exactly")
    if not output.is_file() or not audit.is_file():
        raise ExactTradeBuildError("Staging run is missing a required artifact")
    return report


def build_exact_trades(
    con: duckdb.DuckDBPyConnection,
    raw_path: str | Path,
    candidates_path: str | Path,
    timestamp_provenance_path: str | Path,
    wallet_flags_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    """Build and atomically publish one fresh immutable exact-trade run."""

    raw = _path(raw_path)
    candidates = _path(candidates_path)
    provenance_path = _path(timestamp_provenance_path)
    wallet_flags = _path(wallet_flags_path)
    final_run_dir = _path(run_dir)
    initial_inputs = [raw, candidates, provenance_path, wallet_flags]
    _validate_run_destination(final_run_dir, initial_inputs)

    declaration = load_timestamp_provenance(provenance_path)
    provenance_report = validate_timestamp_provenance(declaration, con)
    cache = _path(provenance_report["cache"]["path"])
    all_inputs = [*initial_inputs, cache]
    _validate_run_destination(final_run_dir, all_inputs)

    _create_input_views(con, raw, candidates, cache, wallet_flags)
    _validate_inputs(con)
    _assert_valid_candidates(con)
    con.execute(
        """
        CREATE TEMP TABLE candidate_markets AS
        SELECT market_id::VARCHAR AS market_id FROM candidate_input
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE candidate_source AS
        SELECT
            raw.order_hash::VARCHAR AS order_hash,
            LOWER(raw.maker)::VARCHAR AS maker,
            LOWER(raw.taker)::VARCHAR AS taker,
            raw.maker_asset_id::VARCHAR AS maker_asset_id,
            raw.taker_asset_id::VARCHAR AS taker_asset_id,
            raw.maker_amount_filled,
            raw.taker_amount_filled,
            raw.fee,
            raw.block_number,
            LOWER(raw.transaction_hash)::VARCHAR AS transaction_hash,
            raw.log_index,
            LOWER(raw.exchange_address)::VARCHAR AS exchange_address,
            raw.condition_id::VARCHAR AS condition_id,
            raw.outcome::VARCHAR AS outcome,
            raw.winning_outcome::VARCHAR AS winning_outcome,
            raw.outcome_token_side::VARCHAR AS outcome_token_side
        FROM raw_input raw
        SEMI JOIN candidate_markets candidate
          ON raw.condition_id = candidate.market_id
        """
    )
    _assert_valid_candidate_source(con)
    raw_rows, distinct_fills, duplicate_replays = _deduplicate_fills(con)
    source_blocks, matched_blocks, missing_blocks = _assert_block_coverage(con)

    con.execute(
        """
        CREATE TEMP TABLE bot_wallets AS
        SELECT DISTINCT LOWER(proxyWallet)::VARCHAR AS proxyWallet
        FROM wallet_flag_input
        WHERE is_nonhuman IS TRUE AND proxyWallet IS NOT NULL
        """
    )
    _create_buy_relations(con)
    counts = _reconciled_counts(
        con,
        raw_rows,
        distinct_fills,
        duplicate_replays,
        source_blocks,
        matched_blocks,
        missing_blocks,
    )

    report_base: dict[str, Any] = {
        "schema_version": 1,
        "method": "exact_polygon_block_timestamp_inner_join",
        "inputs": {
            "raw_resolved_trades": str(raw),
            "candidate_markets": str(candidates),
            "timestamp_provenance": str(provenance_path),
            "timestamp_provenance_validation": provenance_report,
            "wallet_flags": str(wallet_flags),
        },
        "filters": {
            "side": "BUY",
            "price": "0.01 < price < 0.99",
            "bot_exclusion": "wallet_flags.is_nonhuman on buyer proxyWallet",
            "timestamp_fallback_rows": 0,
        },
    }

    final_run_dir.parent.mkdir(parents=True, exist_ok=True)
    _validate_run_destination(final_run_dir, all_inputs)
    staging_dir = Path(
        tempfile.mkdtemp(
            prefix=f".{final_run_dir.name}.staging-", dir=final_run_dir.parent
        )
    ).resolve()
    try:
        report = _write_and_verify_staging_run(
            con, staging_dir, final_run_dir, report_base, counts
        )
        if final_run_dir.exists():
            raise FileExistsError(
                f"Immutable run directory appeared during build: {final_run_dir}"
            )
        os.rename(staging_dir, final_run_dir)
        return report
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, help="resolved_trades Parquet")
    parser.add_argument("--candidates", required=True, help="MLB candidate-market Parquet")
    parser.add_argument(
        "--timestamp-provenance",
        required=True,
        help="Validated exact-timestamp provenance JSON",
    )
    parser.add_argument("--wallet-flags", required=True, help="wallet_flags Parquet")
    parser.add_argument(
        "--run-dir", required=True, help="Fresh immutable output run directory"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    con = duckdb.connect()
    try:
        report = build_exact_trades(
            con,
            args.raw,
            args.candidates,
            args.timestamp_provenance,
            args.wallet_flags,
            args.run_dir,
        )
    finally:
        con.close()
    counts = report["counts"]
    print(
        f"Wrote {counts['output_buy_rows']:,} exact-timestamp MLB BUY rows "
        f"(${counts['output_buy_dollars']:,.2f}); "
        f"removed {counts['duplicate_replays']:,} ingestion replays."
    )


if __name__ == "__main__":
    main()
