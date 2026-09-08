#!/usr/bin/env python3
"""Build primary and bot-buyer-excluded MLB closing prices from raw fills.

The primary close is the final exact-timestamp BUY before the observed first
plate appearance with ``0 < price < 1``.  The sensitivity close preserves the
previous published-C definition: ``0.01 < price < 0.99`` and the BUY-side
``proxyWallet`` must not be flagged ``is_nonhuman``.  A flagged counterparty
does not exclude a fill.

The builder starts from resolved fills and the declared exact Polygon block
cache.  It never uses the old linear block-time approximation and publishes a
fresh immutable run directory atomically.
"""
from __future__ import annotations

import argparse
import hashlib
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
from validate_moneylines import ACCEPTED_MLB_LABEL_TO_TEAM_ID


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
NUMERIC_TYPES = INTEGER_TYPES | {"FLOAT", "DOUBLE", "REAL"}
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
ELIGIBLE_COLUMNS = (
    "market_id",
    "game_pk",
    "official_date",
    "game_type",
    "slug_orientation",
    "away_team_id",
    "away_team_name",
    "home_team_id",
    "home_team_name",
    "away_token_id",
    "home_token_id",
    "winning_team_id",
    "winning_token_id",
    "winning_outcome",
    "actual_start_utc",
)
WALLET_COLUMNS = ("proxyWallet", "is_nonhuman")


class DualCloseBuildError(ValueError):
    """Raised when dual closes cannot be derived without guessing."""


def _path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _quoted(value: str | Path) -> str:
    return str(_path(value)).replace("'", "''")


def _fingerprint(path: Path) -> dict[str, str | int]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _schema(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise DualCloseBuildError(f"Unsafe DuckDB relation name: {relation!r}")
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    required: tuple[str, ...],
    label: str,
) -> dict[str, str]:
    schema = _schema(con, relation)
    missing = sorted(set(required) - schema.keys())
    if missing:
        raise DualCloseBuildError(f"{label} is missing required columns: {missing}")
    return schema


def _assert_no_nulls(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    columns: tuple[str, ...],
    label: str,
) -> None:
    schema = _schema(con, relation)
    checks = []
    for column in columns:
        check = f'"{column}" IS NULL'
        if schema[column] == "VARCHAR":
            check += f' OR TRIM("{column}") = \'\''
        checks.append(f"({check})")
    count = int(
        con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE {' OR '.join(checks)}"
        ).fetchone()[0]
    )
    if count:
        raise DualCloseBuildError(
            f"{label} contains {count} rows with null or blank required values"
        )


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _validate_destination(run_dir: Path, inputs: tuple[Path, ...]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if run_dir in dangerous:
        raise DualCloseBuildError(f"Refusing dangerous run directory: {run_dir}")
    for input_path in inputs:
        if _paths_overlap(run_dir, input_path):
            raise DualCloseBuildError(
                f"Run directory must not collide with an input path: {input_path}"
            )
    if run_dir.exists():
        raise FileExistsError(f"Immutable run directory already exists: {run_dir}")


def _create_input_views(
    con: duckdb.DuckDBPyConnection,
    raw: Path,
    eligible: Path,
    wallet_flags: Path,
    exact_cache: Path,
) -> None:
    con.execute(
        f"CREATE TEMP VIEW raw_input AS SELECT * FROM read_parquet('{_quoted(raw)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW eligible_input AS "
        f"SELECT * FROM read_parquet('{_quoted(eligible)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW wallet_input AS "
        f"SELECT * FROM read_parquet('{_quoted(wallet_flags)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW exact_cache AS "
        f"SELECT * FROM read_parquet('{_quoted(exact_cache)}')"
    )


def _validate_schemas(con: duckdb.DuckDBPyConnection) -> None:
    raw = _require_columns(con, "raw_input", RAW_COLUMNS, "Resolved-trade source")
    eligible = _require_columns(
        con, "eligible_input", ELIGIBLE_COLUMNS, "Eligible-moneyline dimension"
    )
    wallets = _require_columns(con, "wallet_input", WALLET_COLUMNS, "Wallet flags")
    for column in (
        "maker_amount_filled",
        "taker_amount_filled",
        "fee",
        "block_number",
        "log_index",
    ):
        if raw[column] not in INTEGER_TYPES:
            raise DualCloseBuildError(
                f"Resolved-trade source {column} must be an integer; found {raw[column]}"
            )
    for column in ("game_pk", "away_team_id", "home_team_id", "winning_team_id"):
        if eligible[column] not in INTEGER_TYPES:
            raise DualCloseBuildError(
                f"Eligible-moneyline {column} must be an integer; found {eligible[column]}"
            )
    if eligible["official_date"] != "DATE":
        raise DualCloseBuildError("Eligible-moneyline official_date must be DATE")
    if eligible["actual_start_utc"] != "TIMESTAMP WITH TIME ZONE":
        raise DualCloseBuildError(
            "Eligible-moneyline actual_start_utc must be TIMESTAMP WITH TIME ZONE"
        )
    if wallets["is_nonhuman"] != "BOOLEAN":
        raise DualCloseBuildError("Wallet flags is_nonhuman must be BOOLEAN")


def _prepare_eligible(con: duckdb.DuckDBPyConnection) -> int:
    _assert_no_nulls(
        con,
        "eligible_input",
        tuple(sorted(ELIGIBLE_COLUMNS)),
        "Eligible-moneyline dimension",
    )
    con.execute(
        """
        CREATE TEMP TABLE eligible AS
        SELECT
            LOWER(market_id)::VARCHAR AS market_id,
            game_pk::BIGINT AS game_pk,
            official_date::DATE AS official_date,
            game_type::VARCHAR AS game_type,
            slug_orientation::VARCHAR AS slug_orientation,
            away_team_id::INTEGER AS away_team_id,
            away_team_name::VARCHAR AS away_team_name,
            home_team_id::INTEGER AS home_team_id,
            home_team_name::VARCHAR AS home_team_name,
            away_token_id::VARCHAR AS away_token_id,
            home_token_id::VARCHAR AS home_token_id,
            winning_team_id::INTEGER AS winning_team_id,
            winning_token_id::VARCHAR AS winning_token_id,
            winning_outcome::VARCHAR AS winning_outcome,
            actual_start_utc
        FROM eligible_input
        """
    )
    rows, markets, games = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT market_id), COUNT(DISTINCT game_pk)
        FROM eligible
        """
    ).fetchone()
    if not rows:
        raise DualCloseBuildError("Eligible-moneyline dimension must be nonempty")
    if rows != markets or rows != games:
        raise DualCloseBuildError(
            "Eligible market_id and game_pk assignments must each be one-to-one"
        )
    duplicate_tokens = con.execute(
        """
        SELECT token_id, COUNT(*) AS assignments
        FROM (
            SELECT away_token_id AS token_id FROM eligible
            UNION ALL
            SELECT home_token_id AS token_id FROM eligible
        ) tokens
        GROUP BY token_id HAVING COUNT(*) > 1
        ORDER BY token_id LIMIT 10
        """
    ).fetchall()
    if duplicate_tokens:
        raise DualCloseBuildError(
            "Eligible token assignments must be globally unique; "
            f"sample={duplicate_tokens}"
        )
    bad_winner = con.execute(
        """
        SELECT market_id FROM eligible
        WHERE away_team_id = home_team_id
           OR away_token_id = home_token_id
           OR winning_team_id NOT IN (away_team_id, home_team_id)
           OR winning_token_id NOT IN (away_token_id, home_token_id)
           OR (winning_team_id = away_team_id) != (winning_token_id = away_token_id)
        ORDER BY market_id LIMIT 10
        """
    ).fetchall()
    if bad_winner:
        raise DualCloseBuildError(
            f"Eligible winner/team/token assignments are inconsistent; sample={bad_winner}"
        )
    return int(rows)


def _prepare_wallet_flags(con: duckdb.DuckDBPyConnection) -> int:
    _assert_no_nulls(
        con, "wallet_input", WALLET_COLUMNS, "Wallet flags"
    )
    con.execute(
        """
        CREATE TEMP TABLE normalized_wallet_flags AS
        SELECT LOWER(proxyWallet)::VARCHAR AS wallet, is_nonhuman
        FROM wallet_input
        """
    )
    conflicts = con.execute(
        """
        SELECT wallet, COUNT(DISTINCT is_nonhuman) AS flag_versions
        FROM normalized_wallet_flags
        GROUP BY wallet HAVING COUNT(DISTINCT is_nonhuman) > 1
        ORDER BY wallet LIMIT 10
        """
    ).fetchall()
    if conflicts:
        raise DualCloseBuildError(
            f"Wallet flags contain contradictory normalized assignments; sample={conflicts}"
        )
    con.execute(
        """
        CREATE TEMP TABLE flagged_bot_wallets AS
        SELECT DISTINCT wallet FROM normalized_wallet_flags WHERE is_nonhuman
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM flagged_bot_wallets").fetchone()[0])


def _prepare_candidate_fills(
    con: duckdb.DuckDBPyConnection,
) -> tuple[int, int, int]:
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
            LOWER(raw.condition_id)::VARCHAR AS condition_id,
            raw.outcome::VARCHAR AS outcome,
            raw.winning_outcome::VARCHAR AS winning_outcome,
            LOWER(raw.outcome_token_side)::VARCHAR AS outcome_token_side
        FROM raw_input raw
        SEMI JOIN eligible e ON LOWER(raw.condition_id) = e.market_id
        """
    )
    raw_rows = int(con.execute("SELECT COUNT(*) FROM candidate_source").fetchone()[0])
    if not raw_rows:
        raise DualCloseBuildError("No resolved fills match any eligible market")
    _assert_no_nulls(
        con, "candidate_source", RAW_COLUMNS, "Eligible resolved-fill source"
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
        raise DualCloseBuildError(
            f"Eligible resolved-fill source contains {bad_side} invalid token sides"
        )
    con.execute("CREATE TEMP TABLE unique_payloads AS SELECT DISTINCT * FROM candidate_source")
    contradictions = con.execute(
        """
        SELECT transaction_hash, log_index, exchange_address, COUNT(*) AS payloads
        FROM unique_payloads
        GROUP BY transaction_hash, log_index, exchange_address
        HAVING COUNT(*) > 1
        ORDER BY transaction_hash, log_index, exchange_address LIMIT 10
        """
    ).fetchall()
    if contradictions:
        raise DualCloseBuildError(
            "Immutable EVM identities map to contradictory payloads; "
            f"sample={contradictions}"
        )
    con.execute("CREATE TEMP TABLE fills AS SELECT * FROM unique_payloads")
    distinct_fills = int(con.execute("SELECT COUNT(*) FROM fills").fetchone()[0])
    return raw_rows, distinct_fills, raw_rows - distinct_fills


def _validate_exact_block_coverage(con: duckdb.DuckDBPyConnection) -> int:
    source_blocks = int(
        con.execute("SELECT COUNT(DISTINCT block_number) FROM fills").fetchone()[0]
    )
    missing = int(
        con.execute(
            """
            SELECT COUNT(*) FROM (SELECT DISTINCT block_number FROM fills) source
            ANTI JOIN exact_cache cache USING (block_number)
            """
        ).fetchone()[0]
    )
    if missing:
        sample = con.execute(
            """
            SELECT source.block_number
            FROM (SELECT DISTINCT block_number FROM fills) source
            ANTI JOIN exact_cache cache USING (block_number)
            ORDER BY source.block_number LIMIT 10
            """
        ).fetchall()
        raise DualCloseBuildError(
            f"Exact timestamp cache is missing {missing} eligible-source blocks; "
            f"sample={sample}"
        )
    reversals = con.execute(
        """
        WITH blocks AS (
            SELECT source.block_number, cache.timestamp
            FROM (SELECT DISTINCT block_number FROM fills) source
            JOIN exact_cache cache USING (block_number)
        ), ordered AS (
            SELECT *, LAG(timestamp) OVER (ORDER BY block_number) AS prior_timestamp
            FROM blocks
        )
        SELECT block_number, timestamp, prior_timestamp FROM ordered
        WHERE timestamp < prior_timestamp ORDER BY block_number LIMIT 10
        """
    ).fetchall()
    if reversals:
        raise DualCloseBuildError(
            "Exact block timestamps are not nondecreasing by block number; "
            f"sample={reversals}"
        )
    return source_blocks


def _create_buys(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE buys AS
        SELECT
            e.*,
            CASE WHEN f.outcome_token_side = 'maker'
                 THEN f.maker_asset_id ELSE f.taker_asset_id END::VARCHAR AS token_id,
            CASE WHEN f.outcome_token_side = 'maker'
                 THEN f.taker ELSE f.maker END::VARCHAR AS buyer,
            CASE WHEN f.outcome_token_side = 'maker'
                 THEN f.maker ELSE f.taker END::VARCHAR AS counterparty,
            buyer_bot.wallet IS NOT NULL AS buyer_is_flagged_bot,
            counterparty_bot.wallet IS NOT NULL AS counterparty_is_flagged_bot,
            f.outcome,
            f.winning_outcome AS trade_winning_outcome,
            (CASE WHEN f.outcome_token_side = 'maker'
                  THEN (f.taker_amount_filled / {USDC_SCALE}) /
                       NULLIF(f.maker_amount_filled / {TOKEN_SCALE}, 0)
                  ELSE (f.maker_amount_filled / {USDC_SCALE}) /
                       NULLIF(f.taker_amount_filled / {TOKEN_SCALE}, 0)
             END)::DOUBLE AS price,
            (CASE WHEN f.outcome_token_side = 'maker'
                  THEN f.taker_amount_filled / {USDC_SCALE}
                  ELSE f.maker_amount_filled / {USDC_SCALE}
             END)::DOUBLE AS usdc,
            f.block_number::BIGINT AS block_number,
            cache.timestamp::BIGINT AS timestamp,
            f.transaction_hash,
            f.log_index::INTEGER AS log_index,
            f.exchange_address
        FROM fills f
        JOIN exact_cache cache USING (block_number)
        JOIN eligible e ON f.condition_id = e.market_id
        LEFT JOIN flagged_bot_wallets buyer_bot
          ON buyer_bot.wallet = CASE WHEN f.outcome_token_side = 'maker'
                                     THEN f.taker ELSE f.maker END
        LEFT JOIN flagged_bot_wallets counterparty_bot
          ON counterparty_bot.wallet = CASE WHEN f.outcome_token_side = 'maker'
                                            THEN f.maker ELSE f.taker END
        """
    )
    token_mismatches = con.execute(
        """
        SELECT market_id, token_id, transaction_hash, log_index
        FROM buys WHERE token_id NOT IN (away_token_id, home_token_id)
        ORDER BY market_id, block_number, log_index LIMIT 10
        """
    ).fetchall()
    if token_mismatches:
        raise DualCloseBuildError(
            "Resolved fills contain token IDs absent from the eligible dimension; "
            f"sample={token_mismatches}"
        )
    outcome_mismatches = con.execute(
        """
        WITH normalized AS (
            SELECT *,
                LOWER(TRIM(REGEXP_REPLACE(outcome, '\\s+', ' ', 'g'))) AS outcome_norm,
                LOWER(TRIM(REGEXP_REPLACE(trade_winning_outcome, '\\s+', ' ', 'g')))
                    AS trade_winner_norm,
                LOWER(TRIM(REGEXP_REPLACE(winning_outcome, '\\s+', ' ', 'g')))
                    AS dimension_winner_norm
            FROM buys
        )
        SELECT n.market_id, n.token_id, n.outcome, n.trade_winning_outcome,
               n.transaction_hash, n.log_index
        FROM normalized n
        LEFT JOIN accepted_mlb_outcomes outcome_map
          ON n.outcome_norm = outcome_map.outcome_norm
        LEFT JOIN accepted_mlb_outcomes winner_map
          ON n.trade_winner_norm = winner_map.outcome_norm
        WHERE outcome_map.team_id IS NULL
           OR outcome_map.team_id != CASE WHEN n.token_id = n.home_token_id
                                          THEN n.home_team_id ELSE n.away_team_id END
           OR winner_map.team_id IS NULL
           OR winner_map.team_id != n.winning_team_id
           OR n.trade_winner_norm != n.dimension_winner_norm
           OR ((n.token_id = n.winning_token_id) !=
               (n.outcome_norm = n.dimension_winner_norm))
        ORDER BY n.market_id, n.block_number, n.log_index LIMIT 10
        """
    ).fetchall()
    if outcome_mismatches:
        raise DualCloseBuildError(
            "Resolved-fill outcomes disagree with eligible token/winner assignments; "
            f"sample={outcome_mismatches}"
        )
    ambiguous_order = con.execute(
        """
        SELECT market_id, block_number, log_index, COUNT(*) AS fills
        FROM buys GROUP BY market_id, block_number, log_index
        HAVING COUNT(*) > 1
        ORDER BY market_id, block_number, log_index LIMIT 10
        """
    ).fetchall()
    if ambiguous_order:
        raise DualCloseBuildError(
            "Closing order requires unique block_number + log_index within a market; "
            f"sample={ambiguous_order}"
        )


def _create_close_relations(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE per_game_counts AS
        SELECT
            e.market_id,
            COUNT(b.transaction_hash)::BIGINT AS raw_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0 AND b.price < 1
            )::BIGINT AS economically_valid_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0 AND b.price < 1
                  AND to_timestamp(b.timestamp) < e.actual_start_utc
            )::BIGINT AS primary_pregame_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0.01 AND b.price < 0.99
                  AND to_timestamp(b.timestamp) < e.actual_start_utc
            )::BIGINT AS strict_price_pregame_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0.01 AND b.price < 0.99
                  AND b.buyer_is_flagged_bot
                  AND to_timestamp(b.timestamp) < e.actual_start_utc
            )::BIGINT AS flagged_bot_buyer_pregame_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0.01 AND b.price < 0.99
                  AND b.counterparty_is_flagged_bot
                  AND to_timestamp(b.timestamp) < e.actual_start_utc
            )::BIGINT AS flagged_bot_counterparty_pregame_fill_count,
            COUNT(b.transaction_hash) FILTER (
                WHERE isfinite(b.price) AND b.price > 0.01 AND b.price < 0.99
                  AND NOT b.buyer_is_flagged_bot
                  AND to_timestamp(b.timestamp) < e.actual_start_utc
            )::BIGINT AS sensitivity_pregame_fill_count
        FROM eligible e LEFT JOIN buys b USING (market_id)
        GROUP BY e.market_id
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE primary_ranked AS
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY market_id
            ORDER BY block_number DESC, log_index DESC, transaction_hash DESC
        ) AS close_rank
        FROM buys
        WHERE isfinite(price) AND price > 0 AND price < 1
          AND to_timestamp(timestamp) < actual_start_utc
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE sensitivity_ranked AS
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY market_id
            ORDER BY block_number DESC, log_index DESC, transaction_hash DESC
        ) AS close_rank
        FROM buys
        WHERE isfinite(price) AND price > 0.01 AND price < 0.99
          AND NOT buyer_is_flagged_bot
          AND to_timestamp(timestamp) < actual_start_utc
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE primary_close AS
        SELECT * EXCLUDE (close_rank) FROM primary_ranked WHERE close_rank = 1
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE sensitivity_close AS
        SELECT * EXCLUDE (close_rank) FROM sensitivity_ranked WHERE close_rank = 1
        """
    )

    def fields(alias: str, prefix: str) -> str:
        return f"""
            {alias}.timestamp AS {prefix}_close_timestamp,
            to_timestamp({alias}.timestamp) AS {prefix}_close_utc,
            epoch(e.actual_start_utc) - {alias}.timestamp
                AS {prefix}_close_age_seconds,
            {alias}.token_id AS {prefix}_token_id,
            CASE WHEN {alias}.token_id = e.home_token_id THEN 'home'
                 WHEN {alias}.token_id = e.away_token_id THEN 'away' END::VARCHAR
                AS {prefix}_bought_side,
            {alias}.outcome AS {prefix}_outcome,
            {alias}.price AS {prefix}_price,
            CASE WHEN {alias}.token_id = e.home_token_id THEN {alias}.price
                 ELSE 1.0 - {alias}.price END::DOUBLE
                AS {prefix}_home_probability,
            {alias}.usdc AS {prefix}_usdc,
            {alias}.buyer AS {prefix}_buyer,
            {alias}.counterparty AS {prefix}_counterparty,
            {alias}.buyer_is_flagged_bot AS {prefix}_buyer_is_flagged_bot,
            {alias}.counterparty_is_flagged_bot AS {prefix}_counterparty_is_flagged_bot,
            ({alias}.price > 0.01 AND {alias}.price < 0.99)::BOOLEAN
                AS {prefix}_strict_price_eligible,
            {alias}.block_number AS {prefix}_block_number,
            {alias}.transaction_hash AS {prefix}_transaction_hash,
            {alias}.log_index AS {prefix}_log_index,
            {alias}.exchange_address AS {prefix}_exchange_address
        """

    con.execute(
        f"""
        CREATE TEMP TABLE game_closes AS
        SELECT
            e.market_id, e.game_pk, e.official_date, e.game_type,
            e.slug_orientation,
            e.away_team_id, e.away_team_name, e.home_team_id, e.home_team_name,
            e.away_token_id, e.home_token_id,
            e.winning_team_id, e.winning_token_id, e.winning_outcome,
            (e.winning_team_id = e.home_team_id)::TINYINT AS home_won,
            e.actual_start_utc,
            counts.raw_fill_count,
            counts.economically_valid_fill_count,
            counts.primary_pregame_fill_count,
            counts.strict_price_pregame_fill_count,
            counts.flagged_bot_buyer_pregame_fill_count,
            counts.flagged_bot_counterparty_pregame_fill_count,
            counts.sensitivity_pregame_fill_count,
            (primary_close.market_id IS NOT NULL)::BOOLEAN AS primary_has_close,
            CASE
                WHEN primary_close.market_id IS NOT NULL THEN NULL
                WHEN counts.raw_fill_count = 0 THEN 'no_resolved_fill'
                WHEN counts.economically_valid_fill_count = 0
                    THEN 'no_economically_valid_fill'
                ELSE 'no_economically_valid_pregame_fill'
            END::VARCHAR AS primary_missing_reason,
            {fields('primary_close', 'primary')},
            (sensitivity_close.market_id IS NOT NULL)::BOOLEAN
                AS sensitivity_has_close,
            CASE
                WHEN sensitivity_close.market_id IS NOT NULL THEN NULL
                WHEN counts.raw_fill_count = 0 THEN 'no_resolved_fill'
                WHEN counts.primary_pregame_fill_count = 0
                    THEN 'no_economically_valid_pregame_fill'
                WHEN counts.strict_price_pregame_fill_count = 0
                    THEN 'no_strict_price_pregame_fill'
                ELSE 'all_strict_price_pregame_fills_have_flagged_bot_buyer'
            END::VARCHAR AS sensitivity_missing_reason,
            {fields('sensitivity_close', 'sensitivity')}
        FROM eligible e
        JOIN per_game_counts counts USING (market_id)
        LEFT JOIN primary_close USING (market_id)
        LEFT JOIN sensitivity_close USING (market_id)
        """
    )


def _validate_game_closes(con: duckdb.DuckDBPyConnection, expected_rows: int) -> None:
    rows, markets, games = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT market_id), COUNT(DISTINCT game_pk)
        FROM game_closes
        """
    ).fetchone()
    if rows != expected_rows or rows != markets or rows != games:
        raise DualCloseBuildError(
            "Dual-close output must contain exactly one row per eligible market/game"
        )
    bad = con.execute(
        """
        SELECT market_id FROM game_closes
        WHERE primary_has_close != (primary_close_timestamp IS NOT NULL)
           OR sensitivity_has_close != (sensitivity_close_timestamp IS NOT NULL)
           OR (primary_has_close AND primary_missing_reason IS NOT NULL)
           OR (NOT primary_has_close AND primary_missing_reason IS NULL)
           OR (sensitivity_has_close AND sensitivity_missing_reason IS NOT NULL)
           OR (NOT sensitivity_has_close AND sensitivity_missing_reason IS NULL)
           OR (primary_has_close AND NOT (
                primary_price > 0 AND primary_price < 1
                AND primary_close_utc < actual_start_utc
                AND primary_close_age_seconds > 0
                AND primary_token_id IN (away_token_id, home_token_id)
                AND primary_home_probability = CASE
                    WHEN primary_token_id = home_token_id THEN primary_price
                    ELSE 1.0 - primary_price END
           ))
           OR (sensitivity_has_close AND NOT (
                sensitivity_price > 0.01 AND sensitivity_price < 0.99
                AND sensitivity_close_utc < actual_start_utc
                AND sensitivity_close_age_seconds > 0
                AND NOT sensitivity_buyer_is_flagged_bot
                AND sensitivity_token_id IN (away_token_id, home_token_id)
                AND sensitivity_home_probability = CASE
                    WHEN sensitivity_token_id = home_token_id THEN sensitivity_price
                    ELSE 1.0 - sensitivity_price END
           ))
        ORDER BY market_id LIMIT 10
        """
    ).fetchall()
    if bad:
        raise DualCloseBuildError(
            f"Dual-close status/value invariants failed; sample={bad}"
        )


def _reason_counts(
    con: duckdb.DuckDBPyConnection, column: str
) -> dict[str, int]:
    if column not in {"primary_missing_reason", "sensitivity_missing_reason"}:
        raise DualCloseBuildError(f"Unsafe missing-reason column: {column}")
    return {
        str(reason): int(count)
        for reason, count in con.execute(
            f"""
            SELECT {column}, COUNT(*) FROM game_closes
            WHERE {column} IS NOT NULL GROUP BY {column} ORDER BY {column}
            """
        ).fetchall()
    }


def _build_reconciliation(
    con: duckdb.DuckDBPyConnection,
    *,
    raw_rows: int,
    distinct_fills: int,
    duplicate_replays: int,
    source_blocks: int,
    eligible_games: int,
    bot_wallets: int,
    inputs: dict[str, Any],
    final_run_dir: Path,
) -> dict[str, Any]:
    summed_raw_fills = int(
        con.execute("SELECT SUM(raw_fill_count) FROM game_closes").fetchone()[0]
    )
    (
        primary,
        sensitivity,
        both_closes,
        primary_only,
        sensitivity_only,
        same_identity,
        different_identity,
        sensitivity_bot_counterparty,
    ) = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE primary_has_close),
            COUNT(*) FILTER (WHERE sensitivity_has_close),
            COUNT(*) FILTER (WHERE primary_has_close AND sensitivity_has_close),
            COUNT(*) FILTER (WHERE primary_has_close AND NOT sensitivity_has_close),
            COUNT(*) FILTER (WHERE NOT primary_has_close AND sensitivity_has_close),
            COUNT(*) FILTER (
                WHERE primary_has_close AND sensitivity_has_close
                  AND (primary_transaction_hash, primary_log_index,
                       primary_exchange_address) =
                      (sensitivity_transaction_hash, sensitivity_log_index,
                       sensitivity_exchange_address)
            ),
            COUNT(*) FILTER (
                WHERE primary_has_close AND sensitivity_has_close
                  AND (primary_transaction_hash, primary_log_index,
                       primary_exchange_address) <>
                      (sensitivity_transaction_hash, sensitivity_log_index,
                       sensitivity_exchange_address)
            ),
            COUNT(*) FILTER (
                WHERE sensitivity_has_close AND sensitivity_counterparty_is_flagged_bot
            )
        FROM game_closes
        """
    ).fetchone()
    counts = {
        "eligible_games": eligible_games,
        "raw_candidate_rows": raw_rows,
        "distinct_fills": distinct_fills,
        "duplicate_ingestion_replays": duplicate_replays,
        "source_distinct_blocks": source_blocks,
        "flagged_bot_wallets": bot_wallets,
        "primary_closes": int(primary),
        "primary_missing": eligible_games - int(primary),
        "sensitivity_closes": int(sensitivity),
        "sensitivity_missing": eligible_games - int(sensitivity),
        "both_closes": int(both_closes),
        "primary_only": int(primary_only),
        "sensitivity_only": int(sensitivity_only),
        "same_close_identity": int(same_identity),
        "different_close_identity": int(different_identity),
        "sensitivity_closes_with_flagged_bot_counterparty": int(
            sensitivity_bot_counterparty
        ),
    }
    reconciliation = {
        "raw_equals_distinct_plus_replays": raw_rows
        == distinct_fills + duplicate_replays,
        "distinct_fills_equal_per_game_raw_sum": distinct_fills == summed_raw_fills,
        "output_one_row_per_eligible_game": True,
        "all_close_timestamps_from_exact_cache": True,
        "close_availability_partitions_reconcile": int(primary)
        == int(both_closes) + int(primary_only)
        and int(sensitivity) == int(both_closes) + int(sensitivity_only),
        "same_and_different_identity_partition_both_closes": int(both_closes)
        == int(same_identity) + int(different_identity),
        "sensitivity_is_a_subset_of_primary": int(sensitivity_only) == 0,
    }
    if not all(reconciliation.values()):
        raise DualCloseBuildError(
            f"Dual-close source/output counts did not reconcile: {reconciliation}"
        )
    return {
        "schema_version": 1,
        "method": "exact_polygon_dual_pregame_close",
        "definitions": {
            "primary": "last BUY before actual_start_utc with 0 < price < 1",
            "sensitivity": (
                "last BUY before actual_start_utc with 0.01 < price < 0.99 "
                "and buyer proxyWallet not flagged is_nonhuman"
            ),
            "bot_semantics": (
                "only the outcome-token buyer is filtered; a flagged counterparty "
                "does not exclude a fill"
            ),
            "close_order": (
                "block_number DESC, log_index DESC, transaction_hash DESC"
            ),
            "home_normalization": (
                "home-token price; 1 - price for an away-token BUY"
            ),
            "timestamp_fallback_rows": 0,
        },
        "inputs": inputs,
        "counts": counts,
        "missing_reasons": {
            "primary": _reason_counts(con, "primary_missing_reason"),
            "sensitivity": _reason_counts(con, "sensitivity_missing_reason"),
        },
        "reconciliation": reconciliation,
        "outputs": {
            "game_closes": str(final_run_dir / "game_closes.parquet"),
            "reconciliation": str(final_run_dir / "reconciliation.json"),
        },
    }


def _write_and_verify(
    con: duckdb.DuckDBPyConnection,
    staging_dir: Path,
    report: dict[str, Any],
) -> None:
    parquet_path = staging_dir / "game_closes.parquet"
    json_path = staging_dir / "reconciliation.json"
    con.execute(
        f"""
        COPY (
            SELECT * FROM game_closes ORDER BY official_date, game_pk, market_id
        ) TO '{_quoted(parquet_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    con.execute(
        f"CREATE TEMP VIEW written_closes AS "
        f"SELECT * FROM read_parquet('{_quoted(parquet_path)}')"
    )
    differences = int(
        con.execute(
            """
            SELECT COUNT(*) FROM (
                (SELECT * FROM game_closes EXCEPT ALL SELECT * FROM written_closes)
                UNION ALL
                (SELECT * FROM written_closes EXCEPT ALL SELECT * FROM game_closes)
            ) differences
            """
        ).fetchone()[0]
    )
    if differences:
        raise DualCloseBuildError(
            f"Staged game_closes differs from its source relation in {differences} rows"
        )
    _validate_game_closes(con, report["counts"]["eligible_games"])
    with json_path.open(encoding="utf-8") as handle:
        if json.load(handle) != report:
            raise DualCloseBuildError("Staged reconciliation JSON did not round-trip")
    if sorted(path.name for path in staging_dir.iterdir()) != [
        "game_closes.parquet",
        "reconciliation.json",
    ]:
        raise DualCloseBuildError("Staged run has an unexpected artifact set")


def build_dual_closes(
    con: duckdb.DuckDBPyConnection,
    raw_path: str | Path,
    eligible_path: str | Path,
    timestamp_provenance_path: str | Path,
    wallet_flags_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    """Build and atomically publish an immutable dual-close run."""

    raw = _path(raw_path)
    eligible_path_resolved = _path(eligible_path)
    provenance_path = _path(timestamp_provenance_path)
    wallet_flags = _path(wallet_flags_path)
    final_run_dir = _path(run_dir)
    initial_inputs = (raw, eligible_path_resolved, provenance_path, wallet_flags)
    for path, label in zip(
        initial_inputs,
        ("Resolved fills", "Eligible moneylines", "Timestamp provenance", "Wallet flags"),
        strict=True,
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} input does not exist: {path}")
    _validate_destination(final_run_dir, initial_inputs)

    con.execute("SET TimeZone='UTC'")
    declaration = load_timestamp_provenance(provenance_path)
    timestamp_report = validate_timestamp_provenance(declaration, con)
    exact_cache = _path(timestamp_report["cache"]["path"])
    inputs = (*initial_inputs, exact_cache)
    _validate_destination(final_run_dir, inputs)
    _create_input_views(con, raw, eligible_path_resolved, wallet_flags, exact_cache)
    _validate_schemas(con)
    con.execute(
        "CREATE TEMP TABLE accepted_mlb_outcomes(outcome_norm VARCHAR, team_id INTEGER)"
    )
    con.executemany(
        "INSERT INTO accepted_mlb_outcomes VALUES (?, ?)",
        sorted(ACCEPTED_MLB_LABEL_TO_TEAM_ID.items()),
    )
    eligible_games = _prepare_eligible(con)
    bot_wallets = _prepare_wallet_flags(con)
    raw_rows, distinct_fills, duplicate_replays = _prepare_candidate_fills(con)
    source_blocks = _validate_exact_block_coverage(con)
    _create_buys(con)
    _create_close_relations(con)
    _validate_game_closes(con, eligible_games)
    report = _build_reconciliation(
        con,
        raw_rows=raw_rows,
        distinct_fills=distinct_fills,
        duplicate_replays=duplicate_replays,
        source_blocks=source_blocks,
        eligible_games=eligible_games,
        bot_wallets=bot_wallets,
        inputs={
            "raw_resolved_fills": _fingerprint(raw),
            "eligible_moneylines": _fingerprint(eligible_path_resolved),
            "timestamp_provenance": _fingerprint(provenance_path),
            "timestamp_provenance_validation": timestamp_report,
            "wallet_flags": _fingerprint(wallet_flags),
        },
        final_run_dir=final_run_dir,
    )

    final_run_dir.parent.mkdir(parents=True, exist_ok=True)
    _validate_destination(final_run_dir, inputs)
    staging_dir = Path(
        tempfile.mkdtemp(
            prefix=f".{final_run_dir.name}.staging-", dir=final_run_dir.parent
        )
    ).resolve()
    try:
        _write_and_verify(con, staging_dir, report)
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
    parser.add_argument(
        "--eligible-moneylines", required=True, help="eligible_moneylines Parquet"
    )
    parser.add_argument(
        "--timestamp-provenance",
        required=True,
        help="validated exact-timestamp provenance JSON",
    )
    parser.add_argument("--wallet-flags", required=True, help="wallet_flags Parquet")
    parser.add_argument(
        "--run-dir", required=True, help="fresh immutable output run directory"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    con = duckdb.connect()
    try:
        report = build_dual_closes(
            con,
            args.raw,
            args.eligible_moneylines,
            args.timestamp_provenance,
            args.wallet_flags,
            args.run_dir,
        )
    finally:
        con.close()
    counts = report["counts"]
    print(
        f"Wrote {counts['eligible_games']:,} game rows: "
        f"{counts['primary_closes']:,} primary closes and "
        f"{counts['sensitivity_closes']:,} sensitivity closes."
    )


if __name__ == "__main__":
    main()
