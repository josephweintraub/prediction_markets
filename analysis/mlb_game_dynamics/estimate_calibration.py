#!/usr/bin/env python3
"""Minimal descriptive calibration estimator for audited MLB phase artifacts.

The estimator deliberately stops at fixed-width price profiles.  It does not
fit regressions, construct complexity proxies, or use dollar weighting as an
estimand.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

import duckdb


MIN_CELL_N = 50

PHASE_REQUIRED = (
    "market_id",
    "game_pk",
    "official_date",
    "proxyWallet",
    "day",
    "trade_day_utc",
    "price",
    "usdc",
    "won",
    "calibration_error",
    "home_won",
    "phase",
    "analysis_eligible",
    "block_number",
    "timestamp",
    "transaction_hash",
    "log_index",
    "exchange_address",
    "actual_start_utc",
    "inning_4_start_utc",
    "inning_7_start_utc",
    "actual_end_utc",
)

# Narrow v1 contract for the dual-close artifact. Each close group is wholly
# present or wholly null, and sensitivity availability is a subset of primary.
DUAL_CLOSE_REQUIRED = (
    "market_id",
    "game_pk",
    "official_date",
    "home_won",
    "actual_start_utc",
    "primary_has_close",
    "primary_missing_reason",
    "primary_close_timestamp",
    "primary_home_probability",
    "primary_block_number",
    "primary_transaction_hash",
    "primary_log_index",
    "primary_exchange_address",
    "sensitivity_has_close",
    "sensitivity_missing_reason",
    "sensitivity_close_timestamp",
    "sensitivity_home_probability",
    "sensitivity_block_number",
    "sensitivity_transaction_hash",
    "sensitivity_log_index",
    "sensitivity_exchange_address",
)
DUAL_CLOSE_TYPES = {
    "market_id": "VARCHAR",
    "game_pk": "BIGINT",
    "official_date": "DATE",
    "home_won": "TINYINT",
    "actual_start_utc": "TIMESTAMP WITH TIME ZONE",
    "primary_has_close": "BOOLEAN",
    "primary_missing_reason": "VARCHAR",
    "primary_close_timestamp": "BIGINT",
    "primary_home_probability": "DOUBLE",
    "primary_block_number": "BIGINT",
    "primary_transaction_hash": "VARCHAR",
    "primary_log_index": "INTEGER",
    "primary_exchange_address": "VARCHAR",
    "sensitivity_has_close": "BOOLEAN",
    "sensitivity_missing_reason": "VARCHAR",
    "sensitivity_close_timestamp": "BIGINT",
    "sensitivity_home_probability": "DOUBLE",
    "sensitivity_block_number": "BIGINT",
    "sensitivity_transaction_hash": "VARCHAR",
    "sensitivity_log_index": "INTEGER",
    "sensitivity_exchange_address": "VARCHAR",
}


class CalibrationEstimatorError(RuntimeError):
    """Raised when an audited input cannot satisfy the estimator contract."""


def _quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {row[1] for row in con.execute(f"PRAGMA table_info('{relation}')").fetchall()}


def _types(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


def _require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    required: tuple[str, ...],
) -> None:
    missing = sorted(set(required) - _columns(con, relation))
    if missing:
        raise CalibrationEstimatorError(
            f"{relation} is missing required columns: {missing}"
        )


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def _sample(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


def _register_inputs(
    con: duckdb.DuckDBPyConnection, phase_path: Path, dual_close_path: Path
) -> None:
    for path in (phase_path, dual_close_path):
        if not path.is_file():
            raise CalibrationEstimatorError(f"Input does not exist: {path}")
    con.execute(
        f"CREATE VIEW phase_input AS SELECT * FROM read_parquet('{_quote_path(phase_path)}')"
    )
    con.execute(
        f"CREATE VIEW close_input AS SELECT * FROM read_parquet('{_quote_path(dual_close_path)}')"
    )
    _require_columns(con, "phase_input", PHASE_REQUIRED)
    _require_columns(con, "close_input", DUAL_CLOSE_REQUIRED)
    actual_types = _types(con, "close_input")
    mismatches = {
        column: {"expected": expected, "actual": actual_types[column]}
        for column, expected in DUAL_CLOSE_TYPES.items()
        if actual_types[column] != expected
    }
    if mismatches:
        raise CalibrationEstimatorError(
            f"close_input does not match the dual-close schema types: {mismatches}"
        )


def _validate_phase_input(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = int(_scalar(con, "SELECT count(*) FROM phase_input"))
    if rows == 0:
        raise CalibrationEstimatorError("Phase-trade input is empty")
    invalid = _sample(
        con,
        """
        SELECT market_id, game_pk, transaction_hash, log_index
        FROM phase_input
        WHERE market_id IS NULL OR game_pk IS NULL OR official_date IS NULL
           OR proxyWallet IS NULL OR day IS NULL OR trade_day_utc IS NULL
           OR block_number IS NULL OR timestamp IS NULL OR transaction_hash IS NULL
           OR log_index IS NULL OR exchange_address IS NULL
           OR actual_start_utc IS NULL OR inning_4_start_utc IS NULL
           OR inning_7_start_utc IS NULL OR actual_end_utc IS NULL
           OR day::BIGINT IS DISTINCT FROM (timestamp::BIGINT // 86400)
           OR trade_day_utc IS DISTINCT FROM CAST(to_timestamp(timestamp) AS DATE)
           OR NOT (actual_start_utc < inning_4_start_utc
                   AND inning_4_start_utc < inning_7_start_utc
                   AND inning_7_start_utc < actual_end_utc)
           OR NOT isfinite(price::DOUBLE) OR price <= 0.01 OR price >= 0.99
           OR NOT isfinite(usdc::DOUBLE) OR usdc <= 0
           OR won NOT IN (0, 1) OR home_won NOT IN (0, 1)
           OR abs(calibration_error::DOUBLE - (won::DOUBLE - price::DOUBLE)) > 1e-12
        LIMIT 10
        """,
    )
    if invalid:
        raise CalibrationEstimatorError(f"Invalid phase-trade rows; sample={invalid}")
    duplicates = _sample(
        con,
        """
        SELECT transaction_hash, log_index, exchange_address, count(*)
        FROM phase_input GROUP BY 1, 2, 3 HAVING count(*) <> 1 LIMIT 10
        """,
    )
    if duplicates:
        raise CalibrationEstimatorError(
            f"Phase trades do not have unique EVM event identities; sample={duplicates}"
        )
    phase_mismatch = _sample(
        con,
        """
        SELECT market_id, game_pk, timestamp, phase, analysis_eligible
        FROM phase_input
        WHERE phase NOT IN ('pregame','innings_1_3','innings_4_6',
                            'innings_7_plus','post_final')
           OR phase != CASE
                WHEN to_timestamp(timestamp) < actual_start_utc THEN 'pregame'
                WHEN to_timestamp(timestamp) < inning_4_start_utc THEN 'innings_1_3'
                WHEN to_timestamp(timestamp) < inning_7_start_utc THEN 'innings_4_6'
                WHEN to_timestamp(timestamp) <= actual_end_utc THEN 'innings_7_plus'
                ELSE 'post_final' END
           OR analysis_eligible IS DISTINCT FROM
                (to_timestamp(timestamp) <= actual_end_utc)
        LIMIT 10
        """,
    )
    if phase_mismatch:
        raise CalibrationEstimatorError(
            f"Serialized phase/boundary assignments are inconsistent; sample={phase_mismatch}"
        )
    inconsistent_games = _sample(
        con,
        """
        SELECT market_id, game_pk
        FROM phase_input
        GROUP BY 1, 2
        HAVING count(DISTINCT official_date) <> 1
            OR count(DISTINCT home_won) <> 1
            OR count(DISTINCT actual_start_utc) <> 1
            OR count(DISTINCT inning_4_start_utc) <> 1
            OR count(DISTINCT inning_7_start_utc) <> 1
            OR count(DISTINCT actual_end_utc) <> 1
        LIMIT 10
        """,
    )
    if inconsistent_games:
        raise CalibrationEstimatorError(
            f"Phase game dimensions are inconsistent; sample={inconsistent_games}"
        )
    return {
        "rows": rows,
        "games": int(_scalar(con, "SELECT count(DISTINCT game_pk) FROM phase_input")),
        "dollars": float(_scalar(con, "SELECT sum(usdc)::DOUBLE FROM phase_input")),
    }


def _validate_dual_closes(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = int(_scalar(con, "SELECT count(*) FROM close_input"))
    if rows == 0:
        raise CalibrationEstimatorError("Dual-close input is empty")
    duplicates = _sample(
        con,
        """
        SELECT market_id, game_pk, count(*)
        FROM close_input GROUP BY 1, 2 HAVING count(*) <> 1 LIMIT 10
        """,
    )
    duplicate_market = _sample(
        con,
        "SELECT market_id, count(*) FROM close_input GROUP BY 1 HAVING count(*) <> 1 LIMIT 10",
    )
    duplicate_game = _sample(
        con,
        "SELECT game_pk, count(*) FROM close_input GROUP BY 1 HAVING count(*) <> 1 LIMIT 10",
    )
    if duplicates or duplicate_market or duplicate_game:
        raise CalibrationEstimatorError(
            "Dual closes must be one-to-one by market and game; "
            f"samples={duplicates + duplicate_market + duplicate_game}"
        )
    invalid = _sample(
        con,
        """
        SELECT market_id, game_pk
        FROM close_input
        WHERE market_id IS NULL OR game_pk IS NULL OR official_date IS NULL
           OR actual_start_utc IS NULL OR home_won NOT IN (0, 1)
           OR primary_has_close IS NULL OR sensitivity_has_close IS NULL
           OR ((primary_close_timestamp IS NULL)::INTEGER
             + (primary_home_probability IS NULL)::INTEGER
             + (primary_block_number IS NULL)::INTEGER
             + (primary_transaction_hash IS NULL)::INTEGER
             + (primary_log_index IS NULL)::INTEGER
             + (primary_exchange_address IS NULL)::INTEGER) NOT IN (0, 6)
           OR ((sensitivity_close_timestamp IS NULL)::INTEGER
             + (sensitivity_home_probability IS NULL)::INTEGER
             + (sensitivity_block_number IS NULL)::INTEGER
             + (sensitivity_transaction_hash IS NULL)::INTEGER
             + (sensitivity_log_index IS NULL)::INTEGER
             + (sensitivity_exchange_address IS NULL)::INTEGER) NOT IN (0, 6)
           OR primary_has_close IS DISTINCT FROM (primary_close_timestamp IS NOT NULL)
           OR sensitivity_has_close IS DISTINCT FROM
                (sensitivity_close_timestamp IS NOT NULL)
           OR (primary_has_close AND primary_missing_reason IS NOT NULL)
           OR (NOT primary_has_close AND primary_missing_reason IS NULL)
           OR (sensitivity_has_close AND sensitivity_missing_reason IS NOT NULL)
           OR (NOT sensitivity_has_close AND sensitivity_missing_reason IS NULL)
           OR (sensitivity_has_close AND NOT primary_has_close)
           OR (primary_has_close AND
               (primary_close_timestamp < 0
                OR NOT isfinite(primary_home_probability::DOUBLE)
                OR primary_home_probability <= 0
                OR primary_home_probability >= 1))
           OR (sensitivity_has_close AND
               (sensitivity_close_timestamp < 0
                OR NOT isfinite(sensitivity_home_probability::DOUBLE)
                OR sensitivity_home_probability <= 0.01
                OR sensitivity_home_probability >= 0.99
                OR sensitivity_close_timestamp > primary_close_timestamp
                OR (sensitivity_block_number, sensitivity_log_index,
                    sensitivity_transaction_hash) >
                   (primary_block_number, primary_log_index,
                    primary_transaction_hash)))
        LIMIT 10
        """,
    )
    if invalid:
        raise CalibrationEstimatorError(f"Invalid dual-close rows; sample={invalid}")
    duplicate_identity = _sample(
        con,
        """
        WITH identities AS (
          SELECT 'primary' definition, primary_transaction_hash transaction_hash,
                 primary_log_index log_index,
                 primary_exchange_address exchange_address
          FROM close_input WHERE primary_has_close
          UNION ALL
          SELECT 'sensitivity', sensitivity_transaction_hash,
                 sensitivity_log_index, sensitivity_exchange_address
          FROM close_input WHERE sensitivity_has_close
        )
        SELECT definition,transaction_hash,log_index,exchange_address,count(*)
        FROM identities GROUP BY 1,2,3,4 HAVING count(*)<>1 LIMIT 10
        """,
    )
    if duplicate_identity:
        raise CalibrationEstimatorError(
            f"Dual-close identities are not unique within definition; sample={duplicate_identity}"
        )
    inconsistent_same_event = _sample(
        con,
        """
        SELECT market_id,game_pk FROM close_input
        WHERE primary_has_close AND sensitivity_has_close
          AND (primary_transaction_hash,primary_log_index,primary_exchange_address)
            = (sensitivity_transaction_hash,sensitivity_log_index,
               sensitivity_exchange_address)
          AND (primary_close_timestamp IS DISTINCT FROM sensitivity_close_timestamp
               OR primary_block_number IS DISTINCT FROM sensitivity_block_number
               OR primary_home_probability IS DISTINCT FROM
                  sensitivity_home_probability)
        LIMIT 10
        """,
    )
    if inconsistent_same_event:
        raise CalibrationEstimatorError(
            "The same close identity has conflicting A/C values; "
            f"sample={inconsistent_same_event}"
        )
    close_only = _sample(
        con,
        """
        SELECT market_id, game_pk FROM close_input
        ANTI JOIN (SELECT DISTINCT market_id, game_pk FROM phase_input)
        USING (market_id, game_pk) ORDER BY market_id, game_pk LIMIT 10
        """,
    )
    missing_from_closes = _sample(
        con,
        """
        SELECT market_id, game_pk FROM (SELECT DISTINCT market_id, game_pk FROM phase_input)
        ANTI JOIN close_input USING (market_id, game_pk) LIMIT 10
        """,
    )
    dimension_mismatch = _sample(
        con,
        """
        SELECT c.market_id, c.game_pk
        FROM close_input c
        JOIN (SELECT market_id, game_pk, min(official_date) official_date,
                    min(home_won) home_won, min(actual_start_utc) actual_start_utc
              FROM phase_input GROUP BY 1, 2) p USING (market_id, game_pk)
        WHERE c.official_date IS DISTINCT FROM p.official_date
           OR c.home_won IS DISTINCT FROM p.home_won
           OR c.actual_start_utc IS DISTINCT FROM p.actual_start_utc
        LIMIT 10
        """,
    )
    if missing_from_closes or dimension_mismatch:
        raise CalibrationEstimatorError(
            "Phase games must be a dimension-consistent subset of dual closes; "
            f"phase-only={missing_from_closes}, "
            f"dimension={dimension_mismatch}"
        )
    close_only_count = int(
        _scalar(
            con,
            """
            SELECT count(*) FROM close_input
            ANTI JOIN (SELECT DISTINCT market_id, game_pk FROM phase_input)
            USING (market_id, game_pk)
            """,
        )
    )
    nonpregame = _sample(
        con,
        """
        SELECT c.market_id,c.game_pk,c.primary_close_timestamp,
               c.sensitivity_close_timestamp,epoch(c.actual_start_utc) start_timestamp
        FROM close_input c
        WHERE c.primary_close_timestamp>=epoch(c.actual_start_utc)
           OR c.sensitivity_close_timestamp>=epoch(c.actual_start_utc)
        LIMIT 10
        """,
    )
    if nonpregame:
        raise CalibrationEstimatorError(
            f"Dual closes must be strictly pregame; sample={nonpregame}"
        )
    primary_count = int(
        _scalar(con, "SELECT count(*) FROM close_input WHERE primary_has_close")
    )
    sensitivity_count = int(
        _scalar(con, "SELECT count(*) FROM close_input WHERE sensitivity_has_close")
    )
    return {
        "games": rows,
        "primary_coverage": primary_count,
        "primary_missing": rows - primary_count,
        "sensitivity_coverage": sensitivity_count,
        "sensitivity_missing": rows - sensitivity_count,
        "close_only_games": close_only_count,
        "close_only_game_sample": [
            {"market_id": market_id, "game_pk": int(game_pk)}
            for market_id, game_pk in close_only
        ],
        "primary_missing_games": [
            {"market_id": market_id, "game_pk": int(game_pk)}
            for market_id, game_pk in _sample(
                con,
                """SELECT market_id, game_pk FROM close_input
                     WHERE NOT primary_has_close ORDER BY official_date, game_pk""",
            )
        ],
        "sensitivity_missing_games": [
            {"market_id": market_id, "game_pk": int(game_pk)}
            for market_id, game_pk in _sample(
                con,
                """SELECT market_id, game_pk FROM close_input
                     WHERE NOT sensitivity_has_close
                     ORDER BY official_date, game_pk""",
            )
        ],
    }


def _create_closing_outputs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE close_observations AS
        SELECT 'primary'::VARCHAR close_definition, market_id, game_pk, official_date,
               home_won::DOUBLE home_won,
               primary_home_probability::DOUBLE probability,
               primary_close_timestamp::BIGINT close_timestamp,
               primary_transaction_hash::VARCHAR transaction_hash,
               primary_log_index::BIGINT log_index,
               primary_exchange_address::VARCHAR exchange_address
        FROM close_input WHERE primary_has_close
        UNION ALL
        SELECT 'sensitivity', market_id, game_pk, official_date,
               home_won::DOUBLE, sensitivity_home_probability::DOUBLE,
               sensitivity_close_timestamp::BIGINT,
               sensitivity_transaction_hash::VARCHAR,
               sensitivity_log_index::BIGINT,
               sensitivity_exchange_address::VARCHAR
        FROM close_input WHERE sensitivity_has_close
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE close_cells AS
        SELECT close_definition, 'overall'::VARCHAR profile_scope, NULL::INTEGER price_decile,
               count(*)::BIGINT game_count,
               avg(probability)::DOUBLE mean_probability,
               avg(home_won)::DOUBLE win_rate,
               avg(home_won-probability)::DOUBLE mean_calibration,
               avg(pow(home_won-probability,2))::DOUBLE brier_score
        FROM close_observations GROUP BY 1
        UNION ALL
        SELECT close_definition, 'price_decile',
               least(floor(probability*10)::INTEGER,9)+1,
               count(*)::BIGINT, avg(probability)::DOUBLE, avg(home_won)::DOUBLE,
               avg(home_won-probability)::DOUBLE,
               avg(pow(home_won-probability,2))::DOUBLE
        FROM close_observations GROUP BY 1, 3
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE close_date_variance AS
        WITH scored AS (
          SELECT o.*, c.profile_scope, c.price_decile, c.mean_calibration,
                 (o.home_won-o.probability-c.mean_calibration) score
          FROM close_observations o JOIN close_cells c USING (close_definition)
          WHERE c.profile_scope='overall' OR
                c.price_decile=least(floor(o.probability*10)::INTEGER,9)+1
        ), date_scores AS (
          SELECT close_definition, profile_scope, price_decile, official_date, sum(score) score
          FROM scored GROUP BY 1,2,3,4
        )
        SELECT close_definition, profile_scope, price_decile,
               sum(score*score)::DOUBLE date_variance
        FROM date_scores GROUP BY 1,2,3
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE closing_calibration AS
        WITH definitions(close_definition) AS (VALUES ('primary'),('sensitivity')),
        bins(price_decile) AS (SELECT * FROM range(1,11)),
        grid AS (
          SELECT close_definition, 'overall'::VARCHAR profile_scope, NULL::BIGINT price_decile
          FROM definitions
          UNION ALL
          SELECT close_definition, 'price_decile', price_decile FROM definitions CROSS JOIN bins
        )
        SELECT g.close_definition, g.profile_scope, g.price_decile,
          CASE WHEN g.price_decile IS NULL THEN 'overall'
               ELSE printf('[%.1f,%.1f%s',(g.price_decile-1)/10.0,
                    g.price_decile/10.0, CASE WHEN g.price_decile=10 THEN ']' ELSE ')' END)
          END::VARCHAR AS price_bin,
          coalesce(c.game_count,0)::BIGINT game_count,
          (coalesce(c.game_count,0) < {MIN_CELL_N})::BOOLEAN suppressed,
          CASE WHEN coalesce(c.game_count,0) < {MIN_CELL_N}
               THEN 'suppressed_n_lt_50' ELSE 'reported' END::VARCHAR status,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N} THEN c.mean_probability END
            ::DOUBLE mean_probability,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N} THEN c.win_rate END
            ::DOUBLE win_rate,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N} THEN c.mean_calibration END
            ::DOUBLE mean_calibration,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N}
               THEN sqrt(greatest(v.date_variance,0))/c.game_count END
            ::DOUBLE calibration_se,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N}
               THEN c.mean_calibration-1.96*sqrt(greatest(v.date_variance,0))/c.game_count END
            ::DOUBLE calibration_ci95_low,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N}
               THEN c.mean_calibration+1.96*sqrt(greatest(v.date_variance,0))/c.game_count END
            ::DOUBLE calibration_ci95_high,
          CASE WHEN coalesce(c.game_count,0) >= {MIN_CELL_N} THEN c.brier_score END
            ::DOUBLE brier_score
        FROM grid g
        LEFT JOIN close_cells c
          ON g.close_definition=c.close_definition AND g.profile_scope=c.profile_scope
         AND g.price_decile IS NOT DISTINCT FROM c.price_decile
        LEFT JOIN close_date_variance v
          ON g.close_definition=v.close_definition AND g.profile_scope=v.profile_scope
         AND g.price_decile IS NOT DISTINCT FROM v.price_decile
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE paired_observations AS
        SELECT market_id, game_pk, official_date, home_won::DOUBLE home_won,
          primary_home_probability::DOUBLE primary_probability,
          sensitivity_home_probability::DOUBLE sensitivity_probability,
          primary_home_probability::DOUBLE-sensitivity_home_probability::DOUBLE
            probability_difference,
          abs(sensitivity_home_probability::DOUBLE-primary_home_probability::DOUBLE)
            absolute_probability_difference,
          (primary_transaction_hash,primary_log_index,primary_exchange_address)
            = (sensitivity_transaction_hash,sensitivity_log_index,
               sensitivity_exchange_address) same_close_event,
          primary_close_timestamp=sensitivity_close_timestamp same_close_timestamp,
          least(floor(primary_home_probability*10)::INTEGER,9)+1 primary_price_decile,
          (pow(home_won::DOUBLE-primary_home_probability::DOUBLE,2)
           -pow(home_won::DOUBLE-sensitivity_home_probability::DOUBLE,2)) brier_difference
        FROM close_input WHERE primary_has_close AND sensitivity_has_close
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE paired_cells AS
        SELECT 'overall'::VARCHAR profile_scope, NULL::INTEGER primary_price_decile,
          count(*)::BIGINT common_games,
          count(*) FILTER (WHERE same_close_event)::BIGINT same_close_event_games,
          count(*) FILTER (WHERE NOT same_close_event)::BIGINT different_close_event_games,
          count(*) FILTER (WHERE same_close_timestamp)::BIGINT same_close_timestamp_games,
          count(*) FILTER (WHERE NOT same_close_timestamp)::BIGINT different_close_timestamp_games,
          avg(primary_probability)::DOUBLE primary_mean_probability,
          avg(sensitivity_probability)::DOUBLE sensitivity_mean_probability,
          avg(probability_difference)::DOUBLE mean_probability_difference,
          avg(absolute_probability_difference)::DOUBLE mean_absolute_probability_difference,
          avg(home_won-primary_probability)::DOUBLE primary_mean_calibration,
          avg(home_won-sensitivity_probability)::DOUBLE sensitivity_mean_calibration,
          avg(pow(home_won-primary_probability,2))::DOUBLE primary_brier_score,
          avg(pow(home_won-sensitivity_probability,2))::DOUBLE sensitivity_brier_score,
          avg(brier_difference)::DOUBLE mean_brier_difference
        FROM paired_observations
        UNION ALL
        SELECT 'primary_price_decile', primary_price_decile, count(*)::BIGINT,
          count(*) FILTER (WHERE same_close_event)::BIGINT,
          count(*) FILTER (WHERE NOT same_close_event)::BIGINT,
          count(*) FILTER (WHERE same_close_timestamp)::BIGINT,
          count(*) FILTER (WHERE NOT same_close_timestamp)::BIGINT,
          avg(primary_probability)::DOUBLE, avg(sensitivity_probability)::DOUBLE,
          avg(probability_difference)::DOUBLE,
          avg(absolute_probability_difference)::DOUBLE,
          avg(home_won-primary_probability)::DOUBLE,
          avg(home_won-sensitivity_probability)::DOUBLE,
          avg(pow(home_won-primary_probability,2))::DOUBLE,
          avg(pow(home_won-sensitivity_probability,2))::DOUBLE,
          avg(brier_difference)::DOUBLE
        FROM paired_observations GROUP BY primary_price_decile
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE closing_paired_sensitivity AS
        WITH bins(primary_price_decile) AS (SELECT * FROM range(1,11)), grid AS (
          SELECT 'overall'::VARCHAR profile_scope,NULL::BIGINT primary_price_decile
          UNION ALL SELECT 'primary_price_decile',primary_price_decile FROM bins
        )
        SELECT g.profile_scope,g.primary_price_decile,
          CASE WHEN g.primary_price_decile IS NULL THEN 'overall'
               ELSE printf('[%.1f,%.1f%s',(g.primary_price_decile-1)/10.0,
                    g.primary_price_decile/10.0,
                    CASE WHEN g.primary_price_decile=10 THEN ']' ELSE ')' END) END
            ::VARCHAR primary_price_bin,
          coalesce(c.common_games,0)::BIGINT common_games,
          coalesce(c.common_games,0)<{MIN_CELL_N} AS suppressed,
          CASE WHEN coalesce(c.common_games,0)<{MIN_CELL_N}
               THEN 'suppressed_n_lt_50' ELSE 'reported' END::VARCHAR status,
          coalesce(c.same_close_event_games,0)::BIGINT same_close_event_games,
          coalesce(c.different_close_event_games,0)::BIGINT different_close_event_games,
          coalesce(c.same_close_timestamp_games,0)::BIGINT same_close_timestamp_games,
          coalesce(c.different_close_timestamp_games,0)::BIGINT different_close_timestamp_games,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N} THEN c.primary_mean_probability END
            ::DOUBLE primary_mean_probability,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.sensitivity_mean_probability END::DOUBLE sensitivity_mean_probability,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.mean_probability_difference END::DOUBLE mean_probability_difference,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.mean_absolute_probability_difference END
            ::DOUBLE mean_absolute_probability_difference,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N} THEN c.primary_mean_calibration END
            ::DOUBLE primary_mean_calibration,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.sensitivity_mean_calibration END::DOUBLE sensitivity_mean_calibration,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.primary_mean_calibration-c.sensitivity_mean_calibration END
            ::DOUBLE mean_calibration_difference,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N} THEN c.primary_brier_score END
            ::DOUBLE primary_brier_score,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.sensitivity_brier_score END::DOUBLE sensitivity_brier_score,
          CASE WHEN coalesce(c.common_games,0)>={MIN_CELL_N}
               THEN c.mean_brier_difference END::DOUBLE mean_brier_difference
        FROM grid g LEFT JOIN paired_cells c
          ON g.profile_scope=c.profile_scope
         AND g.primary_price_decile IS NOT DISTINCT FROM c.primary_price_decile
        """
    )


def _create_trade_output(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE trade_observations AS
        SELECT 'literal'::VARCHAR boundary_sample, market_id, game_pk, phase,
          least(floor(price*10)::INTEGER,9)+1 price_decile,
          day, proxyWallet, price::DOUBLE price, won::DOUBLE won,
          calibration_error::DOUBLE calibration_error, usdc::DOUBLE usdc
        FROM phase_input WHERE analysis_eligible
        UNION ALL
        SELECT 'exclude_within_30s', market_id, game_pk, phase,
          least(floor(price*10)::INTEGER,9)+1,
          day, proxyWallet, price::DOUBLE, won::DOUBLE,
          calibration_error::DOUBLE, usdc::DOUBLE
        FROM phase_input
        WHERE analysis_eligible
          AND abs(timestamp-epoch(actual_start_utc))>30
          AND abs(timestamp-epoch(inning_4_start_utc))>30
          AND abs(timestamp-epoch(inning_7_start_utc))>30
          AND abs(timestamp-epoch(actual_end_utc))>30
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE trade_cells AS
        SELECT boundary_sample,phase,price_decile,count(*)::BIGINT trade_count,
          count(DISTINCT game_pk)::BIGINT game_count,sum(usdc)::DOUBLE dollars,
          avg(price)::DOUBLE mean_price,avg(won)::DOUBLE win_rate,
          avg(calibration_error)::DOUBLE mean_calibration
        FROM trade_observations GROUP BY 1,2,3
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE trade_scores AS
        SELECT o.*,o.calibration_error-c.mean_calibration score
        FROM trade_observations o JOIN trade_cells c
          USING (boundary_sample,phase,price_decile)
        """
    )
    variance_tables: list[str] = []
    cluster_dimensions = {
        "day": "day",
        "wallet": "proxyWallet",
        "game": "game_pk",
        "day_wallet": "day,proxyWallet",
        "day_game": "day,game_pk",
        "wallet_game": "proxyWallet,game_pk",
        "day_wallet_game": "day,proxyWallet,game_pk",
    }
    for name, dimensions in cluster_dimensions.items():
        table = f"variance_{name}"
        variance_tables.append(table)
        con.execute(
            f"""
            CREATE TEMP TABLE {table} AS
            SELECT boundary_sample,phase,price_decile,
                   sum(cluster_score*cluster_score)::DOUBLE v_{name}
            FROM (
              SELECT boundary_sample,phase,price_decile,{dimensions},sum(score) cluster_score
              FROM trade_scores GROUP BY boundary_sample,phase,price_decile,{dimensions}
            ) GROUP BY 1,2,3
            """
        )
    joins = "\n".join(
        f"LEFT JOIN {table} USING (boundary_sample,phase,price_decile)"
        for table in variance_tables
    )
    cgm = "greatest(v_day+v_wallet+v_game-v_day_wallet-v_day_game-v_wallet_game+v_day_wallet_game,0)"
    con.execute(
        f"""
        CREATE TEMP TABLE trade_estimates AS
        SELECT c.*,sqrt({cgm})/c.trade_count AS calibration_se
        FROM trade_cells c {joins}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE trade_phase_calibration AS
        WITH samples(boundary_sample) AS (VALUES ('literal'),('exclude_within_30s')),
        phases(phase,phase_order) AS (VALUES
          ('pregame',1),('innings_1_3',2),('innings_4_6',3),('innings_7_plus',4)),
        bins(price_decile) AS (SELECT * FROM range(1,11))
        SELECT s.boundary_sample,p.phase,p.phase_order,b.price_decile,
          printf('[%.1f,%.1f%s',(b.price_decile-1)/10.0,b.price_decile/10.0,
                 CASE WHEN b.price_decile=10 THEN ']' ELSE ')' END)::VARCHAR price_bin,
          coalesce(e.trade_count,0)::BIGINT trade_count,
          coalesce(e.game_count,0)::BIGINT game_count,
          coalesce(e.dollars,0)::DOUBLE dollars,
          coalesce(e.trade_count,0)<{MIN_CELL_N} AS suppressed,
          CASE WHEN coalesce(e.trade_count,0)<{MIN_CELL_N}
               THEN 'suppressed_n_lt_50' ELSE 'reported' END::VARCHAR status,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N} THEN e.mean_price END
            ::DOUBLE mean_price,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N} THEN e.win_rate END
            ::DOUBLE win_rate,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N} THEN e.mean_calibration END
            ::DOUBLE mean_calibration,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N} THEN e.calibration_se END
            ::DOUBLE calibration_se,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N}
               THEN e.mean_calibration-1.96*e.calibration_se END
            ::DOUBLE calibration_ci95_low,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_CELL_N}
               THEN e.mean_calibration+1.96*e.calibration_se END
            ::DOUBLE calibration_ci95_high
        FROM samples s CROSS JOIN phases p CROSS JOIN bins b
        LEFT JOIN trade_estimates e USING (boundary_sample,phase,price_decile)
        """
    )


def _measure_trade_samples(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    cur = con.execute(
        """
        SELECT boundary_sample,phase,count(*) AS trade_rows,
               count(DISTINCT game_pk) AS games, sum(usdc)::DOUBLE AS dollars
        FROM trade_observations GROUP BY 1,2
        ORDER BY CASE boundary_sample WHEN 'literal' THEN 1 ELSE 2 END,
                 CASE phase WHEN 'pregame' THEN 1 WHEN 'innings_1_3' THEN 2
                            WHEN 'innings_4_6' THEN 3 ELSE 4 END
        """
    )
    names = [item[0] for item in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def _paths_overlap(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def _validate_destination(run_dir: Path, inputs: tuple[Path, ...]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if run_dir in dangerous:
        raise CalibrationEstimatorError(f"Refusing dangerous run directory: {run_dir}")
    if any(_paths_overlap(run_dir, path) for path in inputs):
        raise CalibrationEstimatorError("Run directory collides with an input path")
    if run_dir.exists():
        raise FileExistsError(f"Immutable run directory already exists: {run_dir}")


def estimate_calibration(
    con: duckdb.DuckDBPyConnection,
    phase_trades: str | Path,
    dual_closes: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    phase_path = Path(phase_trades).resolve()
    close_path = Path(dual_closes).resolve()
    destination = Path(run_dir).resolve()
    _validate_destination(destination, (phase_path, close_path))
    con.execute("SET TimeZone='UTC'")
    _register_inputs(con, phase_path, close_path)
    phase_audit = _validate_phase_input(con)
    close_audit = _validate_dual_closes(con)
    _create_closing_outputs(con)
    _create_trade_output(con)
    summary = {
        "schema_version": 1,
        "method": "descriptive_fixed_width_calibration_v1",
        "inputs": {
            "phase_trades": _fingerprint(phase_path),
            "dual_closes": _fingerprint(close_path),
        },
        "definitions": {
            "calibration_error": "won - price",
            "price_decile": "least(floor(price * 10), 9) + 1",
            "close_timestamp_unit": "Unix seconds",
            "primary_close": "last raw pregame BUY fill with 0 < price < 1; no wallet filter",
            "close_sensitivity": "last pregame BUY fill with 0.01 < price < 0.99 after buyer-bot exclusion",
            "paired_probability_difference": "primary probability - sensitivity probability (A - C)",
            "paired_calibration_difference": "primary calibration - sensitivity calibration (A - C)",
            "paired_brier_difference": "primary Brier score - sensitivity Brier score (A - C)",
            "close_weighting": "one equal-weight observation per game",
            "trade_weighting": "one equal-weight observation per eligible BUY fill",
            "boundary_primary": "literal audited half-open phase boundaries",
            "boundary_sensitivity": "exclude abs(trade timestamp - any boundary) <= 30 seconds",
            "suppression": f"estimate fields null when n < {MIN_CELL_N}; counts remain",
            "closing_se": "one-way official-date clustered normal SE; 95% CI = estimate +/- 1.96 SE",
            "trade_se": "existing CGM day x wallet x game clustered normal SE; 95% CI = estimate +/- 1.96 SE",
        },
        "counts": {
            "phase_input": phase_audit,
            "closing_coverage": close_audit,
            "trade_samples": _measure_trade_samples(con),
            "output_rows": {
                "closing_calibration": 22,
                "closing_paired_sensitivity": 11,
                "trade_phase_calibration": 80,
            },
        },
        "outputs": {
            "closing_calibration": "closing_calibration.parquet",
            "closing_paired_sensitivity": "closing_paired_sensitivity.parquet",
            "trade_phase_calibration": "trade_phase_calibration.parquet",
            "summary": "estimator_summary.json",
        },
        "interpretation_status": "exploratory_descriptive",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        outputs = (
            ("closing_calibration", "close_definition,profile_scope,price_decile"),
            ("closing_paired_sensitivity", "profile_scope,primary_price_decile"),
            ("trade_phase_calibration", "boundary_sample,phase_order,price_decile"),
        )
        for relation, order in outputs:
            con.execute(
                f"COPY (SELECT * FROM {relation} ORDER BY {order}) TO "
                f"'{_quote_path(staging / (relation + '.parquet'))}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        (staging / "estimator_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        for relation, _ in outputs:
            path = staging / f"{relation}.parquet"
            actual = int(
                _scalar(con, f"SELECT count(*) FROM read_parquet('{_quote_path(path)}')")
            )
            if actual != summary["counts"]["output_rows"][relation]:
                raise CalibrationEstimatorError(
                    f"Serialized {relation} row count mismatch: {actual}"
                )
        json.loads((staging / "estimator_summary.json").read_text(encoding="utf-8"))
        os.rename(staging, destination)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase-trades", required=True, type=Path)
    parser.add_argument("--dual-closes", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect()
    try:
        summary = estimate_calibration(
            con, args.phase_trades, args.dual_closes, args.run_dir
        )
    finally:
        con.close()
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
