#!/usr/bin/env python3
"""Minimal fixed-bin MLB favorite-longshot tail summaries.

This stage narrows the audited Stage-08 calibration profiles to D1 and D10.
It does not replace the full profiles, fit a slope or regression, or introduce
additional weighting schemes.
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


MIN_TAIL_N = 50
CLOSE_DEFINITIONS = ("primary", "sensitivity")
BOUNDARY_SAMPLES = ("literal", "exclude_within_30s")
PHASES = ("pregame", "innings_1_3", "innings_4_6", "innings_7_plus")
ESTIMATE_COLUMNS = (
    "d1_mean_probability",
    "d1_win_rate",
    "d1_mean_calibration",
    "d1_calibration_se",
    "d1_calibration_ci95_low",
    "d1_calibration_ci95_high",
    "d10_mean_probability",
    "d10_win_rate",
    "d10_mean_calibration",
    "d10_calibration_se",
    "d10_calibration_ci95_low",
    "d10_calibration_ci95_high",
    "spread_d10_minus_d1",
    "spread_se",
    "spread_ci95_low",
    "spread_ci95_high",
)
OUTPUT_COLUMNS = (
    "analysis_scope",
    "close_definition",
    "boundary_sample",
    "phase",
    "d1_n",
    "d1_games",
    "d1_dollars",
    "d10_n",
    "d10_games",
    "d10_dollars",
    "suppressed",
    "status",
    "point_pattern",
    *ESTIMATE_COLUMNS,
)
OUTPUT_TYPES = {
    "analysis_scope": "VARCHAR",
    "close_definition": "VARCHAR",
    "boundary_sample": "VARCHAR",
    "phase": "VARCHAR",
    "d1_n": "BIGINT",
    "d1_games": "BIGINT",
    "d1_dollars": "DOUBLE",
    "d10_n": "BIGINT",
    "d10_games": "BIGINT",
    "d10_dollars": "DOUBLE",
    "suppressed": "BOOLEAN",
    "status": "VARCHAR",
    "point_pattern": "VARCHAR",
    **{name: "DOUBLE" for name in ESTIMATE_COLUMNS},
}


class FLBTailEstimatorError(RuntimeError):
    """Raised when the frozen Stage-08 contract cannot be reconciled."""


def _quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _same_fingerprint(declared: dict[str, Any], actual: dict[str, Any]) -> bool:
    return (
        declared.get("bytes") == actual["bytes"]
        and declared.get("sha256") == actual["sha256"]
    )


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {
        row[1] for row in con.execute(f"PRAGMA table_info('{relation}')").fetchall()
    }


def _types(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    return {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }


def _require_columns(
    con: duckdb.DuckDBPyConnection, relation: str, required: set[str]
) -> None:
    missing = sorted(required - _columns(con, relation))
    if missing:
        raise FLBTailEstimatorError(
            f"{relation} is missing required columns: {missing}"
        )


def _paths_overlap(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def _validate_destination(destination: Path, inputs: tuple[Path, ...]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if destination in dangerous:
        raise FLBTailEstimatorError(f"Refusing dangerous run directory: {destination}")
    if any(_paths_overlap(destination, source) for source in inputs):
        raise FLBTailEstimatorError("Run directory collides with an input path")
    if destination.exists():
        raise FileExistsError(f"Immutable run directory already exists: {destination}")


def _load_stage08_summary(path: Path) -> dict[str, Any]:
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FLBTailEstimatorError(
            f"Cannot load Stage-08 estimator summary: {exc}"
        ) from exc
    required = {
        "schema_version",
        "method",
        "inputs",
        "definitions",
        "counts",
        "outputs",
        "interpretation_status",
    }
    missing = sorted(required - set(summary))
    if missing:
        raise FLBTailEstimatorError(f"Stage-08 summary is missing keys: {missing}")
    if (
        summary["schema_version"] != 1
        or summary["method"] != "descriptive_fixed_width_calibration_v1"
        or summary["interpretation_status"] != "exploratory_descriptive"
    ):
        raise FLBTailEstimatorError("Unsupported Stage-08 estimator declaration")
    definitions = summary["definitions"]
    expected = {
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
        "suppression": "estimate fields null when n < 50; counts remain",
        "closing_se": "one-way official-date clustered normal SE; 95% CI = estimate +/- 1.96 SE",
        "trade_se": "existing CGM day x wallet x game clustered normal SE; 95% CI = estimate +/- 1.96 SE",
    }
    if definitions != expected:
        raise FLBTailEstimatorError("Stage-08 definitions changed")
    if set(summary["inputs"]) != {"dual_closes", "phase_trades"}:
        raise FLBTailEstimatorError("Stage-08 input declaration changed")
    if summary["outputs"] != {
        "closing_calibration": "closing_calibration.parquet",
        "closing_paired_sensitivity": "closing_paired_sensitivity.parquet",
        "trade_phase_calibration": "trade_phase_calibration.parquet",
        "summary": "estimator_summary.json",
    }:
        raise FLBTailEstimatorError("Stage-08 output declaration changed")
    if summary["counts"].get("output_rows") != {
        "closing_calibration": 22,
        "closing_paired_sensitivity": 11,
        "trade_phase_calibration": 80,
    }:
        raise FLBTailEstimatorError("Stage-08 output-row declaration changed")
    return summary


def _register_inputs(
    con: duckdb.DuckDBPyConnection,
    closing_profile: Path,
    phase_profile: Path,
    game_closes: Path,
    phase_trades: Path,
) -> None:
    paths = (closing_profile, phase_profile, game_closes, phase_trades)
    for path in paths:
        if not path.is_file():
            raise FLBTailEstimatorError(f"Input does not exist: {path}")
    names = ("closing_profile", "phase_profile", "close_input", "phase_input")
    for name, path in zip(names, paths):
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{_quote_path(path)}')"
        )
    _require_columns(
        con,
        "closing_profile",
        {
            "close_definition",
            "profile_scope",
            "price_decile",
            "price_bin",
            "game_count",
            "suppressed",
            "status",
            "mean_probability",
            "win_rate",
            "mean_calibration",
            "calibration_se",
            "calibration_ci95_low",
            "calibration_ci95_high",
            "brier_score",
        },
    )
    _require_columns(
        con,
        "phase_profile",
        {
            "boundary_sample",
            "phase",
            "phase_order",
            "price_decile",
            "price_bin",
            "trade_count",
            "game_count",
            "dollars",
            "suppressed",
            "status",
            "mean_price",
            "win_rate",
            "mean_calibration",
            "calibration_se",
            "calibration_ci95_low",
            "calibration_ci95_high",
        },
    )
    _require_columns(
        con,
        "close_input",
        {
            "market_id",
            "game_pk",
            "official_date",
            "home_won",
            "primary_has_close",
            "primary_home_probability",
            "primary_usdc",
            "sensitivity_has_close",
            "sensitivity_home_probability",
            "sensitivity_usdc",
        },
    )
    _require_columns(
        con,
        "phase_input",
        {
            "market_id",
            "game_pk",
            "proxyWallet",
            "day",
            "price",
            "usdc",
            "won",
            "calibration_error",
            "phase",
            "analysis_eligible",
            "timestamp",
            "actual_start_utc",
            "inning_4_start_utc",
            "inning_7_start_utc",
            "actual_end_utc",
        },
    )


def _validate_source_fingerprints(
    summary: dict[str, Any], game_closes: Path, phase_trades: Path
) -> None:
    declared = summary.get("inputs", {})
    comparisons = (
        ("dual_closes", game_closes),
        ("phase_trades", phase_trades),
    )
    for key, path in comparisons:
        if not isinstance(declared.get(key), dict) or not _same_fingerprint(
            declared[key], _fingerprint(path)
        ):
            raise FLBTailEstimatorError(
                f"{key} does not match the Stage-08 source fingerprint"
            )


def _create_observations(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE close_observations AS
        SELECT 'primary'::VARCHAR close_definition, market_id, game_pk, official_date,
               home_won::DOUBLE won, primary_home_probability::DOUBLE probability,
               (home_won::DOUBLE-primary_home_probability::DOUBLE) calibration_error,
               primary_usdc::DOUBLE dollars,
               least(floor(primary_home_probability*10)::INTEGER,9)+1 price_decile
        FROM close_input WHERE primary_has_close
        UNION ALL
        SELECT 'sensitivity', market_id, game_pk, official_date, home_won::DOUBLE,
               sensitivity_home_probability::DOUBLE,
               home_won::DOUBLE-sensitivity_home_probability::DOUBLE,
               sensitivity_usdc::DOUBLE,
               least(floor(sensitivity_home_probability*10)::INTEGER,9)+1
        FROM close_input WHERE sensitivity_has_close
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE trade_observations AS
        SELECT 'literal'::VARCHAR boundary_sample, market_id, game_pk, phase,
               least(floor(price*10)::INTEGER,9)+1 price_decile,
               day, proxyWallet, price::DOUBLE probability, won::DOUBLE won,
               calibration_error::DOUBLE calibration_error, usdc::DOUBLE dollars
        FROM phase_input WHERE analysis_eligible
        UNION ALL
        SELECT 'exclude_within_30s', market_id, game_pk, phase,
               least(floor(price*10)::INTEGER,9)+1, day, proxyWallet,
               price::DOUBLE, won::DOUBLE, calibration_error::DOUBLE, usdc::DOUBLE
        FROM phase_input
        WHERE analysis_eligible
          AND abs(timestamp-epoch(actual_start_utc))>30
          AND abs(timestamp-epoch(inning_4_start_utc))>30
          AND abs(timestamp-epoch(inning_7_start_utc))>30
          AND abs(timestamp-epoch(actual_end_utc))>30
        """
    )


def _create_recomputed_profiles(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE close_cells AS
        SELECT close_definition,'overall'::VARCHAR profile_scope,NULL::INTEGER price_decile,
               count(*)::BIGINT game_count,avg(probability)::DOUBLE mean_probability,
               avg(won)::DOUBLE win_rate,avg(calibration_error)::DOUBLE mean_calibration,
               avg(calibration_error*calibration_error)::DOUBLE brier_score
        FROM close_observations GROUP BY 1
        UNION ALL
        SELECT close_definition,'price_decile',price_decile,count(*)::BIGINT,
               avg(probability)::DOUBLE,avg(won)::DOUBLE,avg(calibration_error)::DOUBLE,
               avg(calibration_error*calibration_error)::DOUBLE
        FROM close_observations GROUP BY 1,3
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE close_date_variance AS
        WITH scores AS (
          SELECT o.close_definition,c.profile_scope,c.price_decile,o.official_date,
                 o.calibration_error-c.mean_calibration score
          FROM close_observations o JOIN close_cells c USING(close_definition)
          WHERE c.profile_scope='overall' OR c.price_decile=o.price_decile
        ), clusters AS (
          SELECT close_definition,profile_scope,price_decile,official_date,sum(score) score
          FROM scores GROUP BY 1,2,3,4
        )
        SELECT close_definition,profile_scope,price_decile,
               sum(score*score)::DOUBLE date_variance
        FROM clusters GROUP BY 1,2,3
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE recomputed_closing AS
        WITH definitions(close_definition) AS (VALUES ('primary'),('sensitivity')),
        bins(price_decile) AS (SELECT * FROM range(1,11)), grid AS (
          SELECT close_definition,'overall'::VARCHAR profile_scope,NULL::BIGINT price_decile FROM definitions
          UNION ALL SELECT close_definition,'price_decile',price_decile FROM definitions CROSS JOIN bins
        )
        SELECT g.close_definition,g.profile_scope,g.price_decile,
          CASE WHEN g.price_decile IS NULL THEN 'overall'
               ELSE printf('[%.1f,%.1f%s',(g.price_decile-1)/10.0,g.price_decile/10.0,
                    CASE WHEN g.price_decile=10 THEN ']' ELSE ')' END) END::VARCHAR price_bin,
          coalesce(c.game_count,0)::BIGINT game_count,
          coalesce(c.game_count,0)<{MIN_TAIL_N} AS suppressed,
          CASE WHEN coalesce(c.game_count,0)<{MIN_TAIL_N} THEN 'suppressed_n_lt_50' ELSE 'reported' END::VARCHAR status,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.mean_probability END::DOUBLE mean_probability,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.win_rate END::DOUBLE win_rate,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.mean_calibration END::DOUBLE mean_calibration,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN sqrt(greatest(v.date_variance,0))/c.game_count END::DOUBLE calibration_se,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.mean_calibration-1.96*sqrt(greatest(v.date_variance,0))/c.game_count END::DOUBLE calibration_ci95_low,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.mean_calibration+1.96*sqrt(greatest(v.date_variance,0))/c.game_count END::DOUBLE calibration_ci95_high,
          CASE WHEN coalesce(c.game_count,0)>={MIN_TAIL_N} THEN c.brier_score END::DOUBLE brier_score
        FROM grid g LEFT JOIN close_cells c
          ON g.close_definition=c.close_definition AND g.profile_scope=c.profile_scope
         AND g.price_decile IS NOT DISTINCT FROM c.price_decile
        LEFT JOIN close_date_variance v
          ON g.close_definition=v.close_definition AND g.profile_scope=v.profile_scope
         AND g.price_decile IS NOT DISTINCT FROM v.price_decile
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE trade_cells AS
        SELECT boundary_sample,phase,price_decile,count(*)::BIGINT trade_count,
               count(DISTINCT game_pk)::BIGINT game_count,sum(dollars)::DOUBLE dollars,
               avg(probability)::DOUBLE mean_price,avg(won)::DOUBLE win_rate,
               avg(calibration_error)::DOUBLE mean_calibration
        FROM trade_observations GROUP BY 1,2,3
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE trade_scores AS
        SELECT o.*,o.calibration_error-c.mean_calibration score
        FROM trade_observations o JOIN trade_cells c USING(boundary_sample,phase,price_decile)
        """
    )
    dimensions = {
        "day": "day",
        "wallet": "proxyWallet",
        "game": "game_pk",
        "day_wallet": "day,proxyWallet",
        "day_game": "day,game_pk",
        "wallet_game": "proxyWallet,game_pk",
        "day_wallet_game": "day,proxyWallet,game_pk",
    }
    joins = []
    for name, keys in dimensions.items():
        con.execute(
            f"""
            CREATE TEMP TABLE profile_variance_{name} AS
            SELECT boundary_sample,phase,price_decile,sum(cluster_score*cluster_score)::DOUBLE v_{name}
            FROM (SELECT boundary_sample,phase,price_decile,{keys},sum(score) cluster_score
                  FROM trade_scores GROUP BY boundary_sample,phase,price_decile,{keys})
            GROUP BY 1,2,3
            """
        )
        joins.append(
            f"LEFT JOIN profile_variance_{name} USING(boundary_sample,phase,price_decile)"
        )
    cgm = "greatest(v_day+v_wallet+v_game-v_day_wallet-v_day_game-v_wallet_game+v_day_wallet_game,0)"
    con.execute(
        f"""
        CREATE TEMP TABLE trade_estimates AS
        SELECT c.*,sqrt({cgm})/c.trade_count calibration_se FROM trade_cells c {' '.join(joins)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE recomputed_phase AS
        WITH samples(boundary_sample) AS (VALUES ('literal'),('exclude_within_30s')),
        phases(phase,phase_order) AS (VALUES ('pregame',1),('innings_1_3',2),('innings_4_6',3),('innings_7_plus',4)),
        bins(price_decile) AS (SELECT * FROM range(1,11))
        SELECT s.boundary_sample,p.phase,p.phase_order,b.price_decile,
          printf('[%.1f,%.1f%s',(b.price_decile-1)/10.0,b.price_decile/10.0,
                 CASE WHEN b.price_decile=10 THEN ']' ELSE ')' END)::VARCHAR price_bin,
          coalesce(e.trade_count,0)::BIGINT trade_count,coalesce(e.game_count,0)::BIGINT game_count,
          coalesce(e.dollars,0)::DOUBLE dollars,coalesce(e.trade_count,0)<{MIN_TAIL_N} AS suppressed,
          CASE WHEN coalesce(e.trade_count,0)<{MIN_TAIL_N} THEN 'suppressed_n_lt_50' ELSE 'reported' END::VARCHAR status,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.mean_price END::DOUBLE mean_price,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.win_rate END::DOUBLE win_rate,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.mean_calibration END::DOUBLE mean_calibration,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.calibration_se END::DOUBLE calibration_se,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.mean_calibration-1.96*e.calibration_se END::DOUBLE calibration_ci95_low,
          CASE WHEN coalesce(e.trade_count,0)>={MIN_TAIL_N} THEN e.mean_calibration+1.96*e.calibration_se END::DOUBLE calibration_ci95_high
        FROM samples s CROSS JOIN phases p CROSS JOIN bins b
        LEFT JOIN trade_estimates e USING(boundary_sample,phase,price_decile)
        """
    )


def _numeric_distinct(left: str, right: str) -> str:
    return f"(({left} IS NULL) <> ({right} IS NULL) OR ({left} IS NOT NULL AND abs({left}-{right}) > 1e-10*greatest(1.0,abs({right}))))"


def _validate_profiles(con: duckdb.DuckDBPyConnection) -> None:
    if int(_scalar(con, "SELECT count(*) FROM closing_profile")) != 22:
        raise FLBTailEstimatorError(
            "Stage-08 closing profile must contain exactly 22 rows"
        )
    if int(_scalar(con, "SELECT count(*) FROM phase_profile")) != 80:
        raise FLBTailEstimatorError(
            "Stage-08 phase profile must contain exactly 80 rows"
        )
    close_numeric = (
        "mean_probability",
        "win_rate",
        "mean_calibration",
        "calibration_se",
        "calibration_ci95_low",
        "calibration_ci95_high",
        "brier_score",
    )
    close_diff = " OR ".join(
        _numeric_distinct(f"a.{name}", f"e.{name}") for name in close_numeric
    )
    close_bad = con.execute(
        f"""
        SELECT coalesce(a.close_definition,e.close_definition),coalesce(a.profile_scope,e.profile_scope),
               coalesce(a.price_decile,e.price_decile)
        FROM closing_profile a FULL JOIN recomputed_closing e
          ON a.close_definition=e.close_definition AND a.profile_scope=e.profile_scope
         AND a.price_decile IS NOT DISTINCT FROM e.price_decile
        WHERE a.close_definition IS NULL OR e.close_definition IS NULL
           OR a.price_bin IS DISTINCT FROM e.price_bin OR a.game_count IS DISTINCT FROM e.game_count
           OR a.suppressed IS DISTINCT FROM e.suppressed OR a.status IS DISTINCT FROM e.status
           OR {close_diff}
        LIMIT 10
        """
    ).fetchall()
    if close_bad:
        raise FLBTailEstimatorError(
            f"Stage-08 closing profile does not recompute; sample={close_bad}"
        )
    phase_numeric = (
        "dollars",
        "mean_price",
        "win_rate",
        "mean_calibration",
        "calibration_se",
        "calibration_ci95_low",
        "calibration_ci95_high",
    )
    phase_diff = " OR ".join(
        _numeric_distinct(f"a.{name}", f"e.{name}") for name in phase_numeric
    )
    phase_bad = con.execute(
        f"""
        SELECT coalesce(a.boundary_sample,e.boundary_sample),coalesce(a.phase,e.phase),
               coalesce(a.price_decile,e.price_decile)
        FROM phase_profile a FULL JOIN recomputed_phase e
          ON a.boundary_sample=e.boundary_sample AND a.phase=e.phase AND a.price_decile=e.price_decile
        WHERE a.boundary_sample IS NULL OR e.boundary_sample IS NULL
           OR a.phase_order IS DISTINCT FROM e.phase_order OR a.price_bin IS DISTINCT FROM e.price_bin
           OR a.trade_count IS DISTINCT FROM e.trade_count OR a.game_count IS DISTINCT FROM e.game_count
           OR a.suppressed IS DISTINCT FROM e.suppressed OR a.status IS DISTINCT FROM e.status
           OR {phase_diff}
        LIMIT 10
        """
    ).fetchall()
    if phase_bad:
        raise FLBTailEstimatorError(
            f"Stage-08 trade-phase profile does not recompute; sample={phase_bad}"
        )


def _create_tail_output(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TEMP TABLE tail_cells AS
        SELECT 'closing'::VARCHAR analysis_scope,close_definition,NULL::VARCHAR boundary_sample,
               'pregame_close'::VARCHAR phase,price_decile,count(*)::BIGINT n,
               count(DISTINCT game_pk)::BIGINT games,sum(dollars)::DOUBLE dollars,
               avg(probability)::DOUBLE mean_probability,avg(won)::DOUBLE win_rate,
               avg(calibration_error)::DOUBLE mean_calibration
        FROM close_observations WHERE price_decile IN (1,10) GROUP BY 2,5
        UNION ALL
        SELECT 'trade_phase',NULL,boundary_sample,phase,price_decile,count(*)::BIGINT,
               count(DISTINCT game_pk)::BIGINT,sum(dollars)::DOUBLE,avg(probability)::DOUBLE,
               avg(won)::DOUBLE,avg(calibration_error)::DOUBLE
        FROM trade_observations WHERE price_decile IN (1,10) GROUP BY 3,4,5
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE close_tail_se AS
        WITH scores AS (
          SELECT o.close_definition,o.price_decile,o.official_date,
                 o.calibration_error-c.mean_calibration score,c.n
          FROM close_observations o JOIN tail_cells c
            ON c.analysis_scope='closing' AND c.close_definition=o.close_definition
           AND c.price_decile=o.price_decile
          WHERE o.price_decile IN (1,10)
        ), clusters AS (
          SELECT close_definition,price_decile,official_date,max(n) n,sum(score) score
          FROM scores GROUP BY 1,2,3
        )
        SELECT close_definition,price_decile,sqrt(sum(score*score))/max(n) calibration_se
        FROM clusters GROUP BY 1,2
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE close_spread AS
        WITH means AS (
          SELECT close_definition,
                 max(mean_calibration) FILTER (WHERE price_decile=1) d1,
                 max(mean_calibration) FILTER (WHERE price_decile=10) d10,
                 max(n) FILTER (WHERE price_decile=1) n1,
                 max(n) FILTER (WHERE price_decile=10) n10
          FROM tail_cells WHERE analysis_scope='closing' GROUP BY 1
        ), clusters AS (
          SELECT o.close_definition,o.official_date,
                 sum(CASE WHEN o.price_decile=1 THEN -(o.calibration_error-m.d1)/m.n1
                          WHEN o.price_decile=10 THEN (o.calibration_error-m.d10)/m.n10 END) score
          FROM close_observations o JOIN means m USING(close_definition)
          WHERE o.price_decile IN (1,10) GROUP BY 1,2
        )
        SELECT m.close_definition,(m.d10-m.d1)::DOUBLE spread,
               sqrt(sum(c.score*c.score))::DOUBLE spread_se
        FROM means m JOIN clusters c USING(close_definition) GROUP BY 1,m.d1,m.d10
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE phase_tail_scores AS
        SELECT o.*,o.calibration_error-c.mean_calibration score
        FROM trade_observations o JOIN tail_cells c
          ON c.analysis_scope='trade_phase' AND c.boundary_sample=o.boundary_sample
         AND c.phase=o.phase AND c.price_decile=o.price_decile
        WHERE o.price_decile IN (1,10)
        """
    )
    dimensions = {
        "day": "day",
        "wallet": "proxyWallet",
        "game": "game_pk",
        "day_wallet": "day,proxyWallet",
        "day_game": "day,game_pk",
        "wallet_game": "proxyWallet,game_pk",
        "day_wallet_game": "day,proxyWallet,game_pk",
    }
    marginal_joins = []
    spread_joins = []
    for name, keys in dimensions.items():
        con.execute(
            f"""
            CREATE TEMP TABLE tail_variance_{name} AS
            SELECT boundary_sample,phase,price_decile,sum(cluster_score*cluster_score)::DOUBLE v_{name}
            FROM (SELECT boundary_sample,phase,price_decile,{keys},sum(score) cluster_score
                  FROM phase_tail_scores GROUP BY boundary_sample,phase,price_decile,{keys})
            GROUP BY 1,2,3
            """
        )
        marginal_joins.append(
            f"LEFT JOIN tail_variance_{name} USING(boundary_sample,phase,price_decile)"
        )
        con.execute(
            f"""
            CREATE TEMP TABLE spread_variance_{name} AS
            SELECT boundary_sample,phase,sum(cluster_score*cluster_score)::DOUBLE v_{name}
            FROM (
              SELECT s.boundary_sample,s.phase,{keys},
                     sum(CASE WHEN s.price_decile=1 THEN -s.score/c.n
                              WHEN s.price_decile=10 THEN s.score/c.n END) cluster_score
              FROM phase_tail_scores s JOIN tail_cells c
                ON c.analysis_scope='trade_phase' AND c.boundary_sample=s.boundary_sample
               AND c.phase=s.phase AND c.price_decile=s.price_decile
              GROUP BY s.boundary_sample,s.phase,{keys}
            ) GROUP BY 1,2
            """
        )
        spread_joins.append(
            f"LEFT JOIN spread_variance_{name} USING(boundary_sample,phase)"
        )
    cgm = "greatest(v_day+v_wallet+v_game-v_day_wallet-v_day_game-v_wallet_game+v_day_wallet_game,0)"
    con.execute(
        f"""
        CREATE TEMP TABLE phase_tail_se AS
        SELECT c.boundary_sample,c.phase,c.price_decile,sqrt({cgm})/c.n calibration_se
        FROM tail_cells c {' '.join(marginal_joins)} WHERE c.analysis_scope='trade_phase'
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE phase_spread AS
        WITH means AS (
          SELECT boundary_sample,phase,
                 max(mean_calibration) FILTER(WHERE price_decile=1) d1,
                 max(mean_calibration) FILTER(WHERE price_decile=10) d10
          FROM tail_cells WHERE analysis_scope='trade_phase' GROUP BY 1,2
        )
        SELECT m.boundary_sample,m.phase,(m.d10-m.d1)::DOUBLE spread,
               sqrt({cgm})::DOUBLE spread_se
        FROM means m {' '.join(spread_joins)}
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE tail_with_se AS
        SELECT c.*,s.calibration_se FROM tail_cells c JOIN close_tail_se s
          ON c.analysis_scope='closing' AND c.close_definition=s.close_definition
         AND c.price_decile=s.price_decile
        UNION ALL
        SELECT c.*,s.calibration_se FROM tail_cells c JOIN phase_tail_se s
          ON c.analysis_scope='trade_phase' AND c.boundary_sample=s.boundary_sample
         AND c.phase=s.phase AND c.price_decile=s.price_decile
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE flb_tail_summary AS
        WITH close_grid AS (
          SELECT 'closing'::VARCHAR analysis_scope,definition::VARCHAR close_definition,
                 NULL::VARCHAR boundary_sample,'pregame_close'::VARCHAR phase
          FROM (VALUES ('primary'),('sensitivity')) d(definition)
        ), phase_grid AS (
          SELECT 'trade_phase'::VARCHAR analysis_scope,
                 NULL::VARCHAR close_definition,sample::VARCHAR boundary_sample,
                 phase::VARCHAR phase
          FROM (VALUES ('literal'),('exclude_within_30s')) s(sample)
          CROSS JOIN (VALUES ('pregame',1),('innings_1_3',2),('innings_4_6',3),('innings_7_plus',4)) p(phase,phase_order)
        ), grid AS (SELECT * FROM close_grid UNION ALL SELECT * FROM phase_grid),
        wide AS (
          SELECT g.analysis_scope,g.close_definition,g.boundary_sample,g.phase,
                 coalesce(max(t.n) FILTER(WHERE t.price_decile=1),0)::BIGINT d1_n,
                 coalesce(max(t.games) FILTER(WHERE t.price_decile=1),0)::BIGINT d1_games,
                 coalesce(max(t.dollars) FILTER(WHERE t.price_decile=1),0)::DOUBLE d1_dollars,
                 coalesce(max(t.n) FILTER(WHERE t.price_decile=10),0)::BIGINT d10_n,
                 coalesce(max(t.games) FILTER(WHERE t.price_decile=10),0)::BIGINT d10_games,
                 coalesce(max(t.dollars) FILTER(WHERE t.price_decile=10),0)::DOUBLE d10_dollars,
                 max(t.mean_probability) FILTER(WHERE t.price_decile=1) d1_mean_probability,
                 max(t.win_rate) FILTER(WHERE t.price_decile=1) d1_win_rate,
                 max(t.mean_calibration) FILTER(WHERE t.price_decile=1) d1_mean_calibration,
                 max(t.calibration_se) FILTER(WHERE t.price_decile=1) d1_calibration_se,
                 max(t.mean_probability) FILTER(WHERE t.price_decile=10) d10_mean_probability,
                 max(t.win_rate) FILTER(WHERE t.price_decile=10) d10_win_rate,
                 max(t.mean_calibration) FILTER(WHERE t.price_decile=10) d10_mean_calibration,
                 max(t.calibration_se) FILTER(WHERE t.price_decile=10) d10_calibration_se
          FROM grid g LEFT JOIN tail_with_se t
            ON g.analysis_scope=t.analysis_scope
           AND g.close_definition IS NOT DISTINCT FROM t.close_definition
           AND g.boundary_sample IS NOT DISTINCT FROM t.boundary_sample AND g.phase=t.phase
          GROUP BY 1,2,3,4
        ), joined AS (
          SELECT w.*,coalesce(cs.spread,ps.spread) spread,coalesce(cs.spread_se,ps.spread_se) spread_se
          FROM wide w LEFT JOIN close_spread cs
            ON w.analysis_scope='closing' AND w.close_definition=cs.close_definition
          LEFT JOIN phase_spread ps
            ON w.analysis_scope='trade_phase' AND w.boundary_sample=ps.boundary_sample AND w.phase=ps.phase
        ), flagged AS (SELECT *,d1_n<{MIN_TAIL_N} OR d10_n<{MIN_TAIL_N} suppressed FROM joined)
        SELECT analysis_scope,close_definition,boundary_sample,phase,d1_n,d1_games,d1_dollars,
               d10_n,d10_games,d10_dollars,suppressed::BOOLEAN AS suppressed,
               CASE WHEN suppressed THEN 'suppressed_tail_n_lt_50' ELSE 'reported' END::VARCHAR status,
               CASE WHEN suppressed THEN 'suppressed'
                    WHEN d1_mean_calibration<0 AND d10_mean_calibration>0 THEN 'classic_flb_signs'
                    WHEN d1_mean_calibration>0 AND d10_mean_calibration<0 THEN 'reverse_flb_signs'
                    WHEN d1_mean_calibration>0 AND d10_mean_calibration>0 THEN 'both_positive'
                    WHEN d1_mean_calibration<0 AND d10_mean_calibration<0 THEN 'both_negative'
                    ELSE 'mixed_or_zero' END::VARCHAR point_pattern,
               CASE WHEN NOT suppressed THEN d1_mean_probability END::DOUBLE d1_mean_probability,
               CASE WHEN NOT suppressed THEN d1_win_rate END::DOUBLE d1_win_rate,
               CASE WHEN NOT suppressed THEN d1_mean_calibration END::DOUBLE d1_mean_calibration,
               CASE WHEN NOT suppressed THEN d1_calibration_se END::DOUBLE d1_calibration_se,
               CASE WHEN NOT suppressed THEN d1_mean_calibration-1.96*d1_calibration_se END::DOUBLE d1_calibration_ci95_low,
               CASE WHEN NOT suppressed THEN d1_mean_calibration+1.96*d1_calibration_se END::DOUBLE d1_calibration_ci95_high,
               CASE WHEN NOT suppressed THEN d10_mean_probability END::DOUBLE d10_mean_probability,
               CASE WHEN NOT suppressed THEN d10_win_rate END::DOUBLE d10_win_rate,
               CASE WHEN NOT suppressed THEN d10_mean_calibration END::DOUBLE d10_mean_calibration,
               CASE WHEN NOT suppressed THEN d10_calibration_se END::DOUBLE d10_calibration_se,
               CASE WHEN NOT suppressed THEN d10_mean_calibration-1.96*d10_calibration_se END::DOUBLE d10_calibration_ci95_low,
               CASE WHEN NOT suppressed THEN d10_mean_calibration+1.96*d10_calibration_se END::DOUBLE d10_calibration_ci95_high,
               CASE WHEN NOT suppressed THEN spread END::DOUBLE spread_d10_minus_d1,
               CASE WHEN NOT suppressed THEN spread_se END::DOUBLE spread_se,
               CASE WHEN NOT suppressed THEN spread-1.96*spread_se END::DOUBLE spread_ci95_low,
               CASE WHEN NOT suppressed THEN spread+1.96*spread_se END::DOUBLE spread_ci95_high
        FROM flagged
        """
    )


def _validate_tail_output(con: duckdb.DuckDBPyConnection) -> None:
    actual_types = _types(con, "flb_tail_summary")
    if tuple(actual_types) != OUTPUT_COLUMNS or actual_types != OUTPUT_TYPES:
        raise FLBTailEstimatorError(f"Unexpected tail output schema: {actual_types}")
    if int(_scalar(con, "SELECT count(*) FROM flb_tail_summary")) != 10:
        raise FLBTailEstimatorError("Tail output must contain exactly 10 rows")
    bad_keys = con.execute(
        """
        WITH expected AS (
          SELECT 'closing' analysis_scope,definition close_definition,NULL boundary_sample,'pregame_close' phase
          FROM (VALUES ('primary'),('sensitivity')) d(definition)
          UNION ALL
          SELECT 'trade_phase',NULL,sample,phase FROM (VALUES ('literal'),('exclude_within_30s')) s(sample)
          CROSS JOIN (VALUES ('pregame'),('innings_1_3'),('innings_4_6'),('innings_7_plus')) p(phase)
        )
        SELECT coalesce(a.analysis_scope,e.analysis_scope),coalesce(a.phase,e.phase)
        FROM flb_tail_summary a FULL JOIN expected e
          ON a.analysis_scope=e.analysis_scope AND a.close_definition IS NOT DISTINCT FROM e.close_definition
         AND a.boundary_sample IS NOT DISTINCT FROM e.boundary_sample AND a.phase=e.phase
        WHERE a.analysis_scope IS NULL OR e.analysis_scope IS NULL
        """
    ).fetchall()
    if bad_keys:
        raise FLBTailEstimatorError(f"Tail output grid mismatch: {bad_keys}")
    estimates_null = " AND ".join(f"{name} IS NULL" for name in ESTIMATE_COLUMNS)
    estimates_full = " AND ".join(f"{name} IS NOT NULL" for name in ESTIMATE_COLUMNS)
    bad_suppression = con.execute(
        f"""
        SELECT analysis_scope,close_definition,boundary_sample,phase FROM flb_tail_summary
        WHERE suppressed IS DISTINCT FROM (d1_n<{MIN_TAIL_N} OR d10_n<{MIN_TAIL_N})
           OR (suppressed AND (status!='suppressed_tail_n_lt_50' OR point_pattern!='suppressed' OR NOT ({estimates_null})))
           OR (NOT suppressed AND (status!='reported' OR point_pattern NOT IN
               ('classic_flb_signs','reverse_flb_signs','both_positive','both_negative','mixed_or_zero') OR NOT ({estimates_full})))
        """
    ).fetchall()
    if bad_suppression:
        raise FLBTailEstimatorError(
            f"Tail suppression/status mismatch: {bad_suppression}"
        )
    support_bad = con.execute(
        """
        WITH supports AS (
          SELECT 'closing' analysis_scope,close_definition,NULL boundary_sample,'pregame_close' phase,
                 game_count n,price_decile FROM closing_profile WHERE profile_scope='price_decile' AND price_decile IN (1,10)
          UNION ALL
          SELECT 'trade_phase',NULL,boundary_sample,phase,trade_count,price_decile
          FROM phase_profile WHERE price_decile IN (1,10)
        )
        SELECT t.analysis_scope,t.close_definition,t.boundary_sample,t.phase
        FROM flb_tail_summary t JOIN supports d1
         ON d1.price_decile=1 AND t.analysis_scope=d1.analysis_scope
         AND t.close_definition IS NOT DISTINCT FROM d1.close_definition
         AND t.boundary_sample IS NOT DISTINCT FROM d1.boundary_sample AND t.phase=d1.phase
        JOIN supports d10
         ON d10.price_decile=10 AND t.analysis_scope=d10.analysis_scope
         AND t.close_definition IS NOT DISTINCT FROM d10.close_definition
         AND t.boundary_sample IS NOT DISTINCT FROM d10.boundary_sample AND t.phase=d10.phase
        WHERE t.d1_n<>d1.n OR t.d10_n<>d10.n
        """
    ).fetchall()
    if support_bad:
        raise FLBTailEstimatorError(
            f"Tail support does not match Stage-08 profiles: {support_bad}"
        )


def estimate_flb_tails(
    con: duckdb.DuckDBPyConnection,
    closing_calibration: str | Path,
    trade_phase_calibration: str | Path,
    estimator_summary: str | Path,
    game_closes: str | Path,
    phase_trades: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    close_profile_path = Path(closing_calibration).resolve()
    phase_profile_path = Path(trade_phase_calibration).resolve()
    stage08_summary_path = Path(estimator_summary).resolve()
    game_closes_path = Path(game_closes).resolve()
    phase_trades_path = Path(phase_trades).resolve()
    destination = Path(run_dir).resolve()
    inputs = (
        close_profile_path,
        phase_profile_path,
        stage08_summary_path,
        game_closes_path,
        phase_trades_path,
    )
    _validate_destination(destination, inputs)
    for path in inputs:
        if not path.is_file():
            raise FLBTailEstimatorError(f"Input does not exist: {path}")
    con.execute("SET TimeZone='UTC'")
    source_summary = _load_stage08_summary(stage08_summary_path)
    _validate_source_fingerprints(source_summary, game_closes_path, phase_trades_path)
    _register_inputs(
        con, close_profile_path, phase_profile_path, game_closes_path, phase_trades_path
    )
    _create_observations(con)
    _create_recomputed_profiles(con)
    _validate_profiles(con)
    _create_tail_output(con)
    _validate_tail_output(con)
    reported = int(
        _scalar(con, "SELECT count(*) FROM flb_tail_summary WHERE NOT suppressed")
    )
    summary = {
        "schema_version": 1,
        "method": "mlb_fixed_bin_flb_tail_summary_v1",
        "inputs": {
            "closing_calibration": _fingerprint(close_profile_path),
            "trade_phase_calibration": _fingerprint(phase_profile_path),
            "estimator_summary": _fingerprint(stage08_summary_path),
            "game_closes": _fingerprint(game_closes_path),
            "phase_trades": _fingerprint(phase_trades_path),
        },
        "source_estimator": {
            "schema_version": source_summary["schema_version"],
            "method": source_summary["method"],
            "interpretation_status": source_summary.get("interpretation_status"),
        },
        "definitions": {
            "calibration_error": "outcome - price (won - probability)",
            "fixed_bins": "Dk is [(k-1)/10,k/10), except D10 is [0.9,1.0]",
            "d1": "fixed probability bin [0.0,0.1)",
            "d10": "fixed probability bin [0.9,1.0]",
            "spread": "D10 mean calibration - D1 mean calibration",
            "classic_flb": "D1 calibration < 0 and D10 calibration > 0",
            "closing_weighting": "one equal-weight close observation per game",
            "phase_weighting": "one equal-weight eligible BUY fill",
            "closing_uncertainty": "one-way official-date clustered normal SE; 95% CI = estimate +/- 1.96 SE",
            "phase_uncertainty": "joint CGM day x wallet x game clustered normal SE; 95% CI = estimate +/- 1.96 SE",
            "boundary_samples": {
                "literal": "literal audited half-open phase boundaries",
                "exclude_within_30s": "exclude abs(trade timestamp - any boundary) <= 30 seconds",
            },
            "close_definitions": {
                "primary": source_summary["definitions"].get("primary_close"),
                "sensitivity": source_summary["definitions"].get("close_sensitivity"),
            },
            "suppression": "entire tail assessment is null when either D1 or D10 n < 50; support counts and dollars remain",
            "point_pattern": "descriptive signs only: classic_flb_signs, reverse_flb_signs, both_positive, both_negative, mixed_or_zero; suppressed when tails are thin",
        },
        "counts": {
            "closing_profile_rows": 22,
            "trade_phase_profile_rows": 80,
            "tail_summary_rows": 10,
            "closing_tail_rows": 2,
            "trade_phase_tail_rows": 8,
            "reported_tail_rows": reported,
            "suppressed_tail_rows": 10 - reported,
        },
        "reconciliation": {
            "source_fingerprints_match_stage08": True,
            "closing_profile_grid_complete": True,
            "trade_phase_profile_grid_complete": True,
            "closing_profile_cells_recomputed": True,
            "trade_phase_profile_cells_recomputed": True,
            "tail_support_matches_profiles": True,
            "tail_rows_partition_expected_grid": True,
            "suppression_is_fail_closed": True,
            "serialized_output_verified": True,
        },
        "outputs": {
            "flb_tail_summary": "flb_tail_summary.parquet",
            "summary": "flb_summary.json",
        },
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        parquet_path = staging / "flb_tail_summary.parquet"
        con.execute(
            f"COPY (SELECT * FROM flb_tail_summary ORDER BY CASE analysis_scope WHEN 'closing' THEN 1 ELSE 2 END, CASE close_definition WHEN 'primary' THEN 1 WHEN 'sensitivity' THEN 2 ELSE 0 END, CASE boundary_sample WHEN 'literal' THEN 1 WHEN 'exclude_within_30s' THEN 2 ELSE 0 END, CASE phase WHEN 'pregame_close' THEN 0 WHEN 'pregame' THEN 1 WHEN 'innings_1_3' THEN 2 WHEN 'innings_4_6' THEN 3 ELSE 4 END) TO '{_quote_path(parquet_path)}' (FORMAT PARQUET,COMPRESSION ZSTD)"
        )
        (staging / "flb_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        con.execute(
            f"CREATE VIEW serialized_tail AS SELECT * FROM read_parquet('{_quote_path(parquet_path)}')"
        )
        if (
            int(_scalar(con, "SELECT count(*) FROM serialized_tail")) != 10
            or _types(con, "serialized_tail") != OUTPUT_TYPES
        ):
            raise FLBTailEstimatorError("Serialized tail output verification failed")
        serialized_difference = int(
            _scalar(
                con,
                """
                SELECT count(*) FROM (
                  (SELECT * FROM flb_tail_summary EXCEPT ALL SELECT * FROM serialized_tail)
                  UNION ALL
                  (SELECT * FROM serialized_tail EXCEPT ALL SELECT * FROM flb_tail_summary)
                )
                """,
            )
        )
        if serialized_difference:
            raise FLBTailEstimatorError(
                "Serialized tail rows differ from verified rows"
            )
        if (
            json.loads((staging / "flb_summary.json").read_text(encoding="utf-8"))
            != summary
        ):
            raise FLBTailEstimatorError("Serialized summary verification failed")
        os.rename(staging, destination)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--closing-calibration", required=True, type=Path)
    parser.add_argument("--trade-phase-calibration", required=True, type=Path)
    parser.add_argument("--estimator-summary", required=True, type=Path)
    parser.add_argument("--game-closes", required=True, type=Path)
    parser.add_argument("--phase-trades", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect()
    try:
        result = estimate_flb_tails(
            con,
            args.closing_calibration,
            args.trade_phase_calibration,
            args.estimator_summary,
            args.game_closes,
            args.phase_trades,
            args.run_dir,
        )
    finally:
        con.close()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
