#!/usr/bin/env python3
"""Build the audited MLB trade-phase and pregame-closing datasets.

This is a pre-estimation step.  It assigns exact-timestamp BUY fills to fixed
game phases and records closing-line and boundary diagnostics, but deliberately
does not estimate calibration profiles.
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
    verify_extract_timestamps,
)
from validate_moneylines import ACCEPTED_MLB_LABEL_TO_TEAM_ID


PHASES = ("pregame", "innings_1_3", "innings_4_6", "innings_7_plus", "post_final")
BOUNDARIES = (
    ("first_play", "actual_start_utc"),
    ("top_4", "inning_4_start_utc"),
    ("top_7", "inning_7_start_utc"),
    ("final_play", "actual_end_utc"),
)
BOUNDARY_WINDOWS_SECONDS = (5, 10, 30)

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

TRADE_COLUMNS = {
    "market_id",
    "token_id",
    "block_number",
    "timestamp",
    "transaction_hash",
    "log_index",
    "exchange_address",
    "proxyWallet",
    "outcome",
    "winning_outcome",
    "price",
    "usdcSize",
}
ELIGIBLE_COLUMNS = {
    "market_id",
    "game_pk",
    "official_date",
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
    "inning_4_start_utc",
    "inning_7_start_utc",
    "actual_end_utc",
    "away_final_score",
    "home_final_score",
    "away_is_winner",
    "home_is_winner",
}
TIMING_BOUNDARY_COLUMNS = (
    "actual_start_utc",
    "inning_4_start_utc",
    "inning_7_start_utc",
    "actual_end_utc",
)


class PhaseDatasetBuildError(ValueError):
    """Raised when a phase dataset cannot be built without guessing."""


def _path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _quote_path(value: str | Path) -> str:
    return str(_path(value)).replace("'", "''")


def _fingerprint(path: Path) -> dict[str, str | int]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def _schema(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise PhaseDatasetBuildError(f"Unsafe DuckDB relation name: {relation!r}")
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    required: set[str],
    label: str,
) -> dict[str, str]:
    schema = _schema(con, relation)
    missing = sorted(required - schema.keys())
    if missing:
        raise PhaseDatasetBuildError(f"{label} is missing required columns: {missing}")
    return schema


def _assert_no_nulls_or_blank_strings(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    columns: tuple[str, ...],
    label: str,
) -> None:
    expressions = []
    schema = _schema(con, relation)
    for column in columns:
        expression = f'"{column}" IS NULL'
        if schema[column] == "VARCHAR":
            expression += f' OR TRIM("{column}") = \'\''
        expressions.append(f"({expression})")
    count = int(
        con.execute(
            f"SELECT COUNT(*) FROM {relation} WHERE {' OR '.join(expressions)}"
        ).fetchone()[0]
    )
    if count:
        raise PhaseDatasetBuildError(
            f"{label} contains {count} rows with null or blank required values"
        )


def _validate_schemas(con: duckdb.DuckDBPyConnection) -> None:
    trades = _require_columns(con, "trades_input", TRADE_COLUMNS, "Exact trades")
    eligible = _require_columns(
        con, "eligible_input", ELIGIBLE_COLUMNS, "Eligible-moneyline dimension"
    )
    for column in ("block_number", "timestamp", "log_index"):
        if trades[column] not in INTEGER_TYPES:
            raise PhaseDatasetBuildError(
                f"Exact trades {column} must be an integer; found {trades[column]}"
            )
    for column in ("price", "usdcSize"):
        if trades[column] not in NUMERIC_TYPES and not trades[column].startswith("DECIMAL"):
            raise PhaseDatasetBuildError(
                f"Exact trades {column} must be numeric; found {trades[column]}"
            )
    if eligible["game_pk"] not in INTEGER_TYPES:
        raise PhaseDatasetBuildError(
            "Eligible-moneyline dimension game_pk must be an integer; "
            f"found {eligible['game_pk']}"
        )
    for column in (
        "away_team_id",
        "home_team_id",
        "winning_team_id",
        "away_final_score",
        "home_final_score",
    ):
        if eligible[column] not in INTEGER_TYPES:
            raise PhaseDatasetBuildError(
                f"Eligible-moneyline dimension {column} must be an integer; "
                f"found {eligible[column]}"
            )
    for column in ("away_is_winner", "home_is_winner"):
        if eligible[column] != "BOOLEAN":
            raise PhaseDatasetBuildError(
                f"Eligible-moneyline dimension {column} must be BOOLEAN; "
                f"found {eligible[column]}"
            )
    if eligible["official_date"] != "DATE":
        raise PhaseDatasetBuildError(
            "Eligible-moneyline dimension official_date must have DATE type"
        )
    for column in TIMING_BOUNDARY_COLUMNS:
        if eligible[column] != "TIMESTAMP WITH TIME ZONE":
            raise PhaseDatasetBuildError(
                f"Eligible-moneyline dimension {column} must be "
                f"TIMESTAMP WITH TIME ZONE; found {eligible[column]}"
            )


def _validate_eligible_dimension(con: duckdb.DuckDBPyConnection) -> int:
    row_count = int(con.execute("SELECT COUNT(*) FROM eligible_input").fetchone()[0])
    if row_count == 0:
        raise PhaseDatasetBuildError("Eligible-moneyline dimension must be nonempty")
    _assert_no_nulls_or_blank_strings(
        con,
        "eligible_input",
        tuple(sorted(ELIGIBLE_COLUMNS)),
        "Eligible-moneyline dimension",
    )
    rows, markets, games = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT market_id), COUNT(DISTINCT game_pk)
        FROM eligible_input
        """
    ).fetchone()
    if rows != markets:
        raise PhaseDatasetBuildError("Eligible market_id assignments must be unique")
    if rows != games:
        raise PhaseDatasetBuildError("Eligible game_pk assignments must be unique")

    duplicate_tokens = con.execute(
        """
        SELECT token_id, COUNT(*) AS assignments
        FROM (
            SELECT away_token_id AS token_id FROM eligible_input
            UNION ALL
            SELECT home_token_id AS token_id FROM eligible_input
        ) tokens
        GROUP BY token_id
        HAVING COUNT(*) > 1
        ORDER BY token_id
        LIMIT 10
        """
    ).fetchall()
    if duplicate_tokens:
        raise PhaseDatasetBuildError(
            "Eligible token assignments must be globally unique; "
            f"sample={duplicate_tokens}"
        )
    bad_winner = con.execute(
        """
        SELECT market_id
        FROM eligible_input
        WHERE away_team_id = home_team_id
           OR away_token_id = home_token_id
           OR winning_team_id NOT IN (away_team_id, home_team_id)
           OR winning_token_id NOT IN (away_token_id, home_token_id)
           OR (winning_team_id = away_team_id) != (winning_token_id = away_token_id)
        ORDER BY market_id
        LIMIT 10
        """
    ).fetchall()
    if bad_winner:
        raise PhaseDatasetBuildError(
            "Eligible winner/team/token assignments are inconsistent; "
            f"sample={bad_winner}"
        )
    bad_official_result = con.execute(
        """
        SELECT market_id, away_final_score, home_final_score,
               away_is_winner, home_is_winner, winning_team_id
        FROM eligible_input
        WHERE away_final_score < 0 OR home_final_score < 0
           OR away_final_score = home_final_score
           OR away_is_winner = home_is_winner
           OR away_is_winner != (away_final_score > home_final_score)
           OR home_is_winner != (home_final_score > away_final_score)
           OR winning_team_id != CASE
               WHEN away_is_winner THEN away_team_id ELSE home_team_id
           END
        ORDER BY market_id
        LIMIT 10
        """
    ).fetchall()
    if bad_official_result:
        raise PhaseDatasetBuildError(
            "Eligible official score/winner fields are inconsistent; "
            f"sample={bad_official_result}"
        )
    bad_boundaries = con.execute(
        """
        SELECT market_id
        FROM eligible_input
        WHERE NOT (
            actual_start_utc < inning_4_start_utc
            AND inning_4_start_utc < inning_7_start_utc
            AND inning_7_start_utc <= actual_end_utc
        )
        ORDER BY market_id
        LIMIT 10
        """
    ).fetchall()
    if bad_boundaries:
        raise PhaseDatasetBuildError(
            "Eligible games require ordered, non-null first-play/top-4/top-7/final "
            f"boundaries; sample={bad_boundaries}"
        )
    bad_winner_labels = con.execute(
        """
        SELECT eligible.market_id, eligible.winning_outcome, eligible.winning_team_id
        FROM eligible_input eligible
        LEFT JOIN accepted_mlb_outcomes accepted
          ON LOWER(TRIM(REGEXP_REPLACE(eligible.winning_outcome, '\\s+', ' ', 'g')))
             = accepted.outcome_norm
        WHERE accepted.team_id IS NULL OR accepted.team_id != eligible.winning_team_id
        ORDER BY eligible.market_id
        LIMIT 10
        """
    ).fetchall()
    if bad_winner_labels:
        raise PhaseDatasetBuildError(
            "Eligible winning outcomes disagree with winning team IDs; "
            f"sample={bad_winner_labels}"
        )
    return row_count


def _validate_trades(con: duckdb.DuckDBPyConnection) -> None:
    _assert_no_nulls_or_blank_strings(
        con,
        "trades_input",
        (
            "market_id",
            "token_id",
            "block_number",
            "timestamp",
            "transaction_hash",
            "log_index",
            "exchange_address",
            "proxyWallet",
            "outcome",
            "winning_outcome",
            "price",
            "usdcSize",
        ),
        "Exact trades",
    )
    bad_values = int(
        con.execute(
            """
            SELECT COUNT(*) FROM trades_input
            WHERE block_number < 0 OR timestamp < 0 OR log_index < 0
               OR NOT isfinite(price::DOUBLE) OR price <= 0.01 OR price >= 0.99
               OR NOT isfinite(usdcSize::DOUBLE) OR usdcSize <= 0
            """
        ).fetchone()[0]
    )
    if bad_values:
        raise PhaseDatasetBuildError(
            f"Exact trades contains {bad_values} invalid numeric rows"
        )
    duplicate_identities = con.execute(
        """
        SELECT transaction_hash, log_index, exchange_address, COUNT(*)
        FROM trades_input
        GROUP BY transaction_hash, log_index, exchange_address
        HAVING COUNT(*) > 1
        ORDER BY transaction_hash, log_index, exchange_address
        LIMIT 10
        """
    ).fetchall()
    if duplicate_identities:
        raise PhaseDatasetBuildError(
            "Exact trades contains duplicate EVM event identities; "
            f"sample={duplicate_identities}"
        )
    contradictory_block_times = con.execute(
        """
        SELECT block_number, COUNT(DISTINCT timestamp)
        FROM trades_input
        GROUP BY block_number
        HAVING COUNT(DISTINCT timestamp) != 1
        ORDER BY block_number
        LIMIT 10
        """
    ).fetchall()
    if contradictory_block_times:
        raise PhaseDatasetBuildError(
            "Exact trades maps a block to multiple timestamps; "
            f"sample={contradictory_block_times}"
        )
    timestamp_reversals = con.execute(
        """
        WITH blocks AS (
            SELECT block_number, MIN(timestamp) AS timestamp
            FROM trades_input
            GROUP BY block_number
        ), ordered AS (
            SELECT *, LAG(timestamp) OVER (ORDER BY block_number) AS prior_timestamp
            FROM blocks
        )
        SELECT block_number, timestamp, prior_timestamp
        FROM ordered
        WHERE timestamp < prior_timestamp
        ORDER BY block_number
        LIMIT 10
        """
    ).fetchall()
    if timestamp_reversals:
        raise PhaseDatasetBuildError(
            "Exact trade block timestamps must be nondecreasing by block number; "
            f"sample={timestamp_reversals}"
        )


def _create_core_relations(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE eligible_games AS
        SELECT
            eligible.market_id::VARCHAR AS market_id,
            eligible.game_pk::BIGINT AS game_pk,
            eligible.official_date::DATE AS official_date,
            eligible.away_team_id::INTEGER AS away_team_id,
            eligible.away_team_name::VARCHAR AS away_team_name,
            eligible.home_team_id::INTEGER AS home_team_id,
            eligible.home_team_name::VARCHAR AS home_team_name,
            eligible.away_token_id::VARCHAR AS away_token_id,
            eligible.home_token_id::VARCHAR AS home_token_id,
            eligible.winning_team_id::INTEGER AS winning_team_id,
            eligible.winning_token_id::VARCHAR AS winning_token_id,
            eligible.winning_outcome::VARCHAR AS winning_outcome,
            eligible.actual_start_utc,
            eligible.inning_4_start_utc,
            eligible.inning_7_start_utc,
            eligible.actual_end_utc
        FROM eligible_input eligible
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE eligible_trade_source AS
        SELECT trades.*, games.* EXCLUDE (market_id)
        FROM trades_input trades
        JOIN eligible_games games USING (market_id)
        """
    )


def _validate_joined_trades(con: duckdb.DuckDBPyConnection) -> None:
    token_mismatches = con.execute(
        """
        SELECT market_id, token_id, transaction_hash, log_index
        FROM eligible_trade_source
        WHERE token_id NOT IN (away_token_id, home_token_id)
        ORDER BY market_id, block_number, log_index
        LIMIT 10
        """
    ).fetchall()
    if token_mismatches:
        raise PhaseDatasetBuildError(
            "Eligible-market trades contain token IDs absent from the eligible "
            f"dimension; sample={token_mismatches}"
        )

    outcome_mismatches = con.execute(
        """
        WITH normalized AS (
            SELECT *,
                   LOWER(TRIM(REGEXP_REPLACE(outcome, '\\s+', ' ', 'g'))) AS outcome_norm,
                   LOWER(TRIM(REGEXP_REPLACE(winning_outcome, '\\s+', ' ', 'g')))
                       AS trade_winner_norm,
                   LOWER(TRIM(REGEXP_REPLACE(eligible_trade_source.winning_outcome_1,
                                             '\\s+', ' ', 'g')))
                       AS dimension_winner_norm
            FROM eligible_trade_source
        )
        SELECT normalized.market_id, normalized.token_id, normalized.outcome,
               normalized.winning_outcome,
               normalized.winning_outcome_1 AS dimension_winning_outcome,
               normalized.transaction_hash, normalized.log_index
        FROM normalized
        LEFT JOIN accepted_mlb_outcomes accepted
          ON normalized.outcome_norm = accepted.outcome_norm
        WHERE normalized.trade_winner_norm != normalized.dimension_winner_norm
           OR ((normalized.token_id = normalized.winning_token_id)
               != (normalized.outcome_norm = normalized.dimension_winner_norm))
           OR accepted.team_id IS NULL
           OR accepted.team_id != CASE
               WHEN normalized.token_id = normalized.home_token_id
               THEN normalized.home_team_id ELSE normalized.away_team_id
           END
        ORDER BY normalized.market_id, normalized.block_number, normalized.log_index
        LIMIT 10
        """
    ).fetchall()
    if outcome_mismatches:
        raise PhaseDatasetBuildError(
            "Eligible-market trade outcome/winner fields disagree with token assignments; "
            f"sample={outcome_mismatches}"
        )

    ambiguous_order = con.execute(
        """
        SELECT block_number, log_index, COUNT(*)
        FROM eligible_trade_source
        GROUP BY block_number, log_index
        HAVING COUNT(*) > 1
        ORDER BY block_number, log_index
        LIMIT 10
        """
    ).fetchall()
    if ambiguous_order:
        raise PhaseDatasetBuildError(
            "Eligible fills require unique block_number + log_index closing order "
            f"within each game; sample={ambiguous_order}"
        )


def _create_phase_rows(con: duckdb.DuckDBPyConnection) -> None:
    # DuckDB appends ``_1`` to the trade-side winning_outcome collision above.
    con.execute(
        """
        CREATE TEMP TABLE phase_rows AS
        SELECT
            market_id,
            game_pk,
            official_date,
            away_team_id,
            away_team_name,
            home_team_id,
            home_team_name,
            proxyWallet,
            (timestamp // 86400)::INTEGER AS day,
            CAST(to_timestamp(timestamp) AS DATE) AS trade_day_utc,
            token_id,
            CASE WHEN token_id = home_token_id THEN 'home' ELSE 'away' END::VARCHAR
                AS bought_side,
            outcome,
            winning_outcome AS trade_winning_outcome,
            winning_outcome_1 AS winning_outcome,
            winning_token_id,
            price::DOUBLE AS price,
            usdcSize::DOUBLE AS usdc,
            (token_id = winning_token_id)::TINYINT AS won,
            ((token_id = winning_token_id)::INTEGER - price::DOUBLE)::DOUBLE
                AS calibration_error,
            (CASE WHEN token_id = home_token_id THEN price ELSE 1.0 - price END)::DOUBLE
                AS home_probability,
            (winning_team_id = home_team_id)::TINYINT AS home_won,
            CASE
                WHEN to_timestamp(timestamp) < actual_start_utc THEN 'pregame'
                WHEN to_timestamp(timestamp) < inning_4_start_utc THEN 'innings_1_3'
                WHEN to_timestamp(timestamp) < inning_7_start_utc THEN 'innings_4_6'
                WHEN to_timestamp(timestamp) <= actual_end_utc THEN 'innings_7_plus'
                ELSE 'post_final'
            END::VARCHAR AS phase,
            (to_timestamp(timestamp) <= actual_end_utc)::BOOLEAN AS analysis_eligible,
            block_number::BIGINT AS block_number,
            timestamp::BIGINT AS timestamp,
            transaction_hash::VARCHAR AS transaction_hash,
            log_index::INTEGER AS log_index,
            exchange_address::VARCHAR AS exchange_address,
            actual_start_utc,
            inning_4_start_utc,
            inning_7_start_utc,
            actual_end_utc
        FROM eligible_trade_source
        """
    )


def _create_closing_rows(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE closing_ranked AS
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY game_pk
            ORDER BY block_number DESC, log_index DESC, transaction_hash DESC
        ) AS closing_rank
        FROM phase_rows
        WHERE phase = 'pregame'
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE closing_lines AS
        SELECT
            market_id,
            game_pk,
            official_date,
            away_team_id,
            away_team_name,
            home_team_id,
            home_team_name,
            timestamp AS close_timestamp,
            trade_day_utc AS close_trade_day_utc,
            proxyWallet,
            token_id,
            bought_side,
            outcome,
            price AS close_price,
            home_probability AS closing_home_probability,
            home_won,
            epoch(actual_start_utc) - timestamp AS close_age_seconds,
            usdc AS close_usdc,
            block_number,
            transaction_hash,
            log_index,
            exchange_address,
            (epoch(actual_start_utc) - timestamp > 300)::BOOLEAN AS stale_over_5m,
            (epoch(actual_start_utc) - timestamp > 1800)::BOOLEAN AS stale_over_30m,
            (epoch(actual_start_utc) - timestamp > 7200)::BOOLEAN AS stale_over_2h,
            (epoch(actual_start_utc) - timestamp > 86400)::BOOLEAN AS stale_over_24h
        FROM closing_ranked
        WHERE closing_rank = 1
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE closing_audit AS
        SELECT
            games.market_id,
            games.game_pk,
            games.official_date,
            (closing.game_pk IS NOT NULL)::BOOLEAN AS has_pregame_close,
            CASE WHEN closing.game_pk IS NULL THEN 'no_pregame_trade' ELSE NULL END::VARCHAR
                AS exclusion_reason,
            COUNT(rows.transaction_hash)::BIGINT AS eligible_joined_trade_count,
            COUNT(rows.transaction_hash) FILTER (WHERE rows.phase = 'pregame')::BIGINT
                AS pregame_trade_count,
            closing.close_timestamp,
            closing.close_trade_day_utc,
            closing.proxyWallet,
            closing.token_id,
            closing.bought_side,
            closing.outcome,
            closing.close_price,
            closing.closing_home_probability,
            closing.home_won,
            closing.close_age_seconds,
            closing.close_usdc,
            closing.block_number,
            closing.transaction_hash,
            closing.log_index,
            closing.exchange_address,
            closing.stale_over_5m,
            closing.stale_over_30m,
            closing.stale_over_2h,
            closing.stale_over_24h
        FROM eligible_games games
        LEFT JOIN phase_rows rows USING (market_id, game_pk)
        LEFT JOIN closing_lines closing USING (market_id, game_pk)
        GROUP BY ALL
        """
    )


def _create_boundary_audit(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE boundary_rows AS
        SELECT market_id, game_pk, official_date,
               boundary_name::VARCHAR AS boundary_name,
               boundary_utc::TIMESTAMPTZ AS boundary_utc
        FROM eligible_games
        CROSS JOIN LATERAL (
            VALUES
                ('first_play', actual_start_utc),
                ('top_4', inning_4_start_utc),
                ('top_7', inning_7_start_utc),
                ('final_play', actual_end_utc)
        ) boundaries(boundary_name, boundary_utc)
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE boundary_audit AS
        SELECT
            boundaries.market_id,
            boundaries.game_pk,
            boundaries.official_date,
            boundaries.boundary_name,
            boundaries.boundary_utc,
            windows.window_seconds::INTEGER AS window_seconds,
            COUNT(rows.transaction_hash)::BIGINT AS trade_count,
            COALESCE(SUM(rows.usdc), 0.0)::DOUBLE AS trade_dollars
        FROM boundary_rows boundaries
        CROSS JOIN (VALUES (5), (10), (30)) windows(window_seconds)
        LEFT JOIN phase_rows rows
          ON rows.market_id = boundaries.market_id
         AND ABS(rows.timestamp - epoch(boundaries.boundary_utc))
             <= windows.window_seconds
        GROUP BY ALL
        """
    )


def _measure(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, int | float]:
    rows, dollars = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(usdc), 0.0)::DOUBLE FROM {relation}"
    ).fetchone()
    return {"rows": int(rows), "dollars": float(dollars)}


def _build_reconciliation(
    con: duckdb.DuckDBPyConnection,
    trades_path: Path,
    eligible_path: Path,
    provenance_path: Path,
    timestamp_validation: dict[str, Any],
    final_run_dir: Path,
) -> dict[str, Any]:
    con.execute(
        """
        CREATE TEMP TABLE input_accounting AS
        SELECT trades.*,
               (eligible.market_id IS NOT NULL)::BOOLEAN AS is_eligible_market
        FROM trades_input trades
        LEFT JOIN eligible_games eligible USING (market_id)
        """
    )
    input_rows, input_dollars = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(usdcSize), 0.0) FROM input_accounting"
    ).fetchone()
    ineligible_rows, ineligible_dollars = con.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(usdcSize), 0.0)
        FROM input_accounting WHERE NOT is_eligible_market
        """
    ).fetchone()
    joined = _measure(con, "phase_rows")
    phases: dict[str, dict[str, int | float]] = {}
    for phase in PHASES:
        rows, dollars, games = con.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(usdc), 0.0), COUNT(DISTINCT game_pk)
            FROM phase_rows WHERE phase = ?
            """,
            [phase],
        ).fetchone()
        phases[phase] = {
            "rows": int(rows),
            "dollars": float(dollars),
            "games": int(games),
        }

    row_partition = int(input_rows) == int(ineligible_rows) + int(joined["rows"])
    phase_row_partition = int(joined["rows"]) == sum(
        int(values["rows"]) for values in phases.values()
    )
    dollar_tolerance = max(1e-9, abs(float(input_dollars)) * 1e-12)
    dollar_partition = abs(
        float(input_dollars) - float(ineligible_dollars) - float(joined["dollars"])
    ) <= dollar_tolerance
    phase_dollar_partition = abs(
        float(joined["dollars"])
        - sum(float(values["dollars"]) for values in phases.values())
    ) <= dollar_tolerance
    if not all((row_partition, phase_row_partition, dollar_partition, phase_dollar_partition)):
        raise PhaseDatasetBuildError("Input/eligibility/phase accounting did not reconcile")

    eligible_markets, markets_with_trades = con.execute(
        """
        SELECT COUNT(*), COUNT(*) FILTER (WHERE trade_count > 0)
        FROM (
            SELECT games.market_id, COUNT(rows.transaction_hash) AS trade_count
            FROM eligible_games games
            LEFT JOIN phase_rows rows USING (market_id, game_pk)
            GROUP BY games.market_id
        ) counts
        """
    ).fetchone()
    closes, no_close = con.execute(
        """
        SELECT COUNT(*) FILTER (WHERE has_pregame_close),
               COUNT(*) FILTER (WHERE NOT has_pregame_close)
        FROM closing_audit
        """
    ).fetchone()
    return {
        "schema_version": 1,
        "method": "unbuffered_exact_block_timestamp_game_phases",
        "inputs": {
            "exact_trades": _fingerprint(trades_path),
            "eligible_moneylines": _fingerprint(eligible_path),
            "timestamp_provenance": _fingerprint(provenance_path),
        },
        "timestamp_provenance_validation": timestamp_validation,
        "phase_intervals": {
            "pregame": "t < actual_start_utc",
            "innings_1_3": "actual_start_utc <= t < inning_4_start_utc",
            "innings_4_6": "inning_4_start_utc <= t < inning_7_start_utc",
            "innings_7_plus": "inning_7_start_utc <= t <= actual_end_utc",
            "post_final": "t > actual_end_utc",
        },
        "counts": {
            "eligible_markets": int(eligible_markets),
            "eligible_markets_with_trades": int(markets_with_trades),
            "closing_lines": int(closes),
            "games_without_pregame_trade": int(no_close),
            "input": {"rows": int(input_rows), "dollars": float(input_dollars)},
            "ineligible_market": {
                "rows": int(ineligible_rows),
                "dollars": float(ineligible_dollars),
            },
            "eligible_joined": joined,
            "phases": phases,
        },
        "reconciliation": {
            "input_equals_ineligible_plus_eligible": True,
            "eligible_equals_all_five_phases": True,
            "dollars_reconciled": True,
        },
        "outputs": {
            "phase_trades": str(final_run_dir / "phase_trades.parquet"),
            "closing_lines": str(final_run_dir / "closing_lines.parquet"),
            "closing_audit": str(final_run_dir / "closing_audit.parquet"),
            "boundary_audit": str(final_run_dir / "boundary_audit.parquet"),
            "reconciliation": str(final_run_dir / "reconciliation.json"),
        },
    }


def _copy_relation(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    output: Path,
    order_by: str,
) -> None:
    con.execute(
        f"COPY (SELECT * FROM {relation} ORDER BY {order_by}) "
        f"TO '{_quote_path(output)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def _verify_staged_run(
    con: duckdb.DuckDBPyConnection,
    staging_dir: Path,
    reconciliation: dict[str, Any],
) -> None:
    expected = {
        "phase_trades.parquet",
        "closing_lines.parquet",
        "closing_audit.parquet",
        "boundary_audit.parquet",
        "reconciliation.json",
    }
    present = {path.name for path in staging_dir.iterdir() if path.is_file()}
    if present != expected:
        raise PhaseDatasetBuildError(
            f"Staged phase run artifacts differ from expectation: {sorted(present)}"
        )
    phase_path = _quote_path(staging_dir / "phase_trades.parquet")
    closing_path = _quote_path(staging_dir / "closing_lines.parquet")
    closing_audit_path = _quote_path(staging_dir / "closing_audit.parquet")
    boundary_path = _quote_path(staging_dir / "boundary_audit.parquet")
    phase_rows, phase_dollars = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(usdc), 0.0) FROM read_parquet('{phase_path}')"
    ).fetchone()
    expected_joined = reconciliation["counts"]["eligible_joined"]
    if int(phase_rows) != expected_joined["rows"] or abs(
        float(phase_dollars) - expected_joined["dollars"]
    ) > max(1e-9, abs(expected_joined["dollars"]) * 1e-12):
        raise PhaseDatasetBuildError("Written phase trades did not reconcile")
    closing_rows = int(
        con.execute(f"SELECT COUNT(*) FROM read_parquet('{closing_path}')").fetchone()[0]
    )
    closing_audit_rows = int(
        con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{closing_audit_path}')"
        ).fetchone()[0]
    )
    boundary_rows = int(
        con.execute(f"SELECT COUNT(*) FROM read_parquet('{boundary_path}')").fetchone()[0]
    )
    if closing_rows != reconciliation["counts"]["closing_lines"]:
        raise PhaseDatasetBuildError("Written closing lines did not reconcile")
    if closing_audit_rows != reconciliation["counts"]["eligible_markets"]:
        raise PhaseDatasetBuildError("Written closing audit did not reconcile")
    expected_boundary_rows = (
        reconciliation["counts"]["eligible_markets"]
        * len(BOUNDARIES)
        * len(BOUNDARY_WINDOWS_SECONDS)
    )
    if boundary_rows != expected_boundary_rows:
        raise PhaseDatasetBuildError("Written boundary audit did not reconcile")
    with (staging_dir / "reconciliation.json").open(encoding="utf-8") as handle:
        disk_reconciliation = json.load(handle)
    if disk_reconciliation != reconciliation:
        raise PhaseDatasetBuildError("Written reconciliation JSON did not round-trip")


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _validate_destination(run_dir: Path, inputs: tuple[Path, ...]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if run_dir in dangerous:
        raise PhaseDatasetBuildError(f"Refusing dangerous run directory: {run_dir}")
    for input_path in inputs:
        if _paths_overlap(run_dir, input_path):
            raise PhaseDatasetBuildError(
                f"Run directory must not collide with input path: {input_path}"
            )
    if run_dir.exists():
        raise FileExistsError(f"Immutable run directory already exists: {run_dir}")


def build_phase_dataset(
    con: duckdb.DuckDBPyConnection,
    trades_path: str | Path,
    eligible_path: str | Path,
    timestamp_provenance_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    """Build and atomically publish one immutable pre-estimation phase run."""

    trades = _path(trades_path)
    eligible = _path(eligible_path)
    provenance = _path(timestamp_provenance_path)
    final_run_dir = _path(run_dir)
    initial_inputs = (trades, eligible, provenance)
    for path, label in zip(
        initial_inputs,
        ("Exact trades", "Eligible-moneyline dimension", "Timestamp provenance"),
        strict=True,
    ):
        if not path.is_file():
            raise FileNotFoundError(f"{label} input does not exist: {path}")
    _validate_destination(final_run_dir, initial_inputs)

    con.execute("SET TimeZone='UTC'")
    con.execute(
        f"CREATE TEMP VIEW trades_input AS SELECT * FROM read_parquet('{_quote_path(trades)}')"
    )
    con.execute(
        f"CREATE TEMP VIEW eligible_input AS "
        f"SELECT * FROM read_parquet('{_quote_path(eligible)}')"
    )
    declaration = load_timestamp_provenance(provenance)
    cache_validation = validate_timestamp_provenance(declaration, con)
    cache_path = _path(cache_validation["cache"]["path"])
    inputs = (*initial_inputs, cache_path)
    _validate_destination(final_run_dir, inputs)
    timestamp_validation = verify_extract_timestamps(
        declaration, con, "trades_input"
    )
    con.execute(
        "CREATE TEMP TABLE accepted_mlb_outcomes(outcome_norm VARCHAR, team_id INTEGER)"
    )
    con.executemany(
        "INSERT INTO accepted_mlb_outcomes VALUES (?, ?)",
        sorted(ACCEPTED_MLB_LABEL_TO_TEAM_ID.items()),
    )
    _validate_schemas(con)
    _validate_eligible_dimension(con)
    _validate_trades(con)
    _create_core_relations(con)
    _validate_joined_trades(con)
    _create_phase_rows(con)
    _create_closing_rows(con)
    _create_boundary_audit(con)
    reconciliation = _build_reconciliation(
        con,
        trades,
        eligible,
        provenance,
        timestamp_validation,
        final_run_dir,
    )

    final_run_dir.parent.mkdir(parents=True, exist_ok=True)
    _validate_destination(final_run_dir, inputs)
    staging_dir = Path(
        tempfile.mkdtemp(
            prefix=f".{final_run_dir.name}.staging-", dir=final_run_dir.parent
        )
    ).resolve()
    try:
        _copy_relation(
            con,
            "phase_rows",
            staging_dir / "phase_trades.parquet",
            "official_date, game_pk, timestamp, block_number, log_index, transaction_hash",
        )
        _copy_relation(
            con,
            "closing_lines",
            staging_dir / "closing_lines.parquet",
            "official_date, game_pk",
        )
        _copy_relation(
            con,
            "closing_audit",
            staging_dir / "closing_audit.parquet",
            "official_date, game_pk",
        )
        _copy_relation(
            con,
            "boundary_audit",
            staging_dir / "boundary_audit.parquet",
            "official_date, game_pk, boundary_name, window_seconds",
        )
        (staging_dir / "reconciliation.json").write_text(
            json.dumps(reconciliation, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _verify_staged_run(con, staging_dir, reconciliation)
        if final_run_dir.exists():
            raise FileExistsError(
                f"Immutable run directory appeared during build: {final_run_dir}"
            )
        os.rename(staging_dir, final_run_dir)
        return reconciliation
    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades", required=True, help="Exact MLB BUY-trade Parquet")
    parser.add_argument(
        "--eligible-moneylines", required=True, help="Eligible moneyline dimension Parquet"
    )
    parser.add_argument(
        "--timestamp-provenance",
        required=True,
        help="Validated exact-timestamp provenance JSON",
    )
    parser.add_argument("--run-dir", required=True, help="Fresh immutable output run directory")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    con = duckdb.connect()
    try:
        report = build_phase_dataset(
            con,
            args.trades,
            args.eligible_moneylines,
            args.timestamp_provenance,
            args.run_dir,
        )
    finally:
        con.close()
    counts = report["counts"]
    print(
        f"Wrote {counts['eligible_joined']['rows']:,} joined phase trades and "
        f"{counts['closing_lines']:,} closing lines."
    )


if __name__ == "__main__":
    main()
