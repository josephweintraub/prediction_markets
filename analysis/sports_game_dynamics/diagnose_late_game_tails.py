"""Describe the trades driving late-game D1 and D10 calibration estimates.

The diagnostic intentionally preserves the estimands used by the published
figures: MLB bins the bought contract's price and outcome, while NFL and NBA
bin the home-normalized probability and outcome.  It performs no modeling and
does not alter the source phase-trade artifacts.

Example:
    python -m analysis.sports_game_dynamics.diagnose_late_game_tails \
      MLB=/path/to/mlb/phase_trades.parquet \
      NFL=/path/to/nfl/phase_trades.parquet \
      NBA=/path/to/nba/phase_trades.parquet \
      --eligible NFL=/path/to/nfl/eligible_moneylines.parquet \
      --eligible NBA=/path/to/nba/eligible_moneylines.parquet \
      --run-dir /path/to/fresh/output
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sys
from typing import Any, Mapping, Sequence

import duckdb


SPORTS = ("MLB", "NFL", "NBA")
LATE_PHASE = {
    "MLB": "innings_7_plus",
    "NFL": "quarter_4_plus",
    "NBA": "quarter_4_plus",
}
PROBABILITY_DEFINITION = {
    "MLB": "bought_contract_probability",
    "NFL": "home_win_probability",
    "NBA": "home_win_probability",
}
OUTPUT_TABLES = (
    "tail_overview",
    "probability_band_concentration",
    "game_contributions",
    "wallet_concentration",
    "timing_thirds",
    "leave_top_k_games",
    "outcome_decomposition",
)


class LateGameDiagnosticError(RuntimeError):
    """Raised when an input cannot satisfy the diagnostic contract."""


def _path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _quote_path(value: str | Path) -> str:
    return str(_path(value)).replace("'", "''")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _fingerprint(path: Path) -> dict[str, Any]:
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
    return {
        row[0]: row[1]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM {_quote_identifier(relation)}"
        ).fetchall()
    }


def _require_columns(
    schema: Mapping[str, str], required: set[str], label: str
) -> None:
    missing = sorted(required - set(schema))
    if missing:
        raise LateGameDiagnosticError(f"{label} is missing columns: {missing}")


def _parse_assignments(
    values: Sequence[str], *, require_all_sports: bool, label: str
) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for value in values:
        if "=" not in value:
            raise LateGameDiagnosticError(
                f"{label} must use SPORT=PATH syntax, found {value!r}"
            )
        sport, raw_path = value.split("=", 1)
        sport = sport.strip().upper()
        if sport not in SPORTS:
            raise LateGameDiagnosticError(f"Unsupported sport in {label}: {sport!r}")
        if sport in parsed:
            raise LateGameDiagnosticError(f"Duplicate {label} for {sport}")
        path = _path(raw_path)
        if not path.is_file():
            raise LateGameDiagnosticError(f"{label} does not exist for {sport}: {path}")
        parsed[sport] = path
    if require_all_sports and set(parsed) != set(SPORTS):
        raise LateGameDiagnosticError(
            f"{label} must contain exactly MLB, NFL, and NBA; found {sorted(parsed)}"
        )
    return parsed


def _validate_destination(destination: Path, inputs: Sequence[Path]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if destination in dangerous:
        raise LateGameDiagnosticError(f"Refusing dangerous run directory: {destination}")
    if destination.exists():
        raise FileExistsError(f"Immutable run directory already exists: {destination}")
    if any(
        destination == source
        or destination in source.parents
        or source in destination.parents
        for source in inputs
    ):
        raise LateGameDiagnosticError("Run directory overlaps an input path")


def _register_parquet(
    con: duckdb.DuckDBPyConnection, relation: str, path: Path
) -> dict[str, str]:
    con.execute(
        f"CREATE VIEW {_quote_identifier(relation)} AS "
        f"SELECT * FROM read_parquet('{_quote_path(path)}')"
    )
    return _schema(con, relation)


def _metadata_expressions(
    con: duckdb.DuckDBPyConnection,
    sport: str,
    phase_schema: Mapping[str, str],
    eligible_path: Path | None,
) -> tuple[str, str, str, str]:
    if {"away_team_name", "home_team_name"} <= set(phase_schema):
        return (
            "p.away_team_name",
            "p.home_team_name",
            "",
            "TRUE",
        )
    if eligible_path is None:
        raise LateGameDiagnosticError(
            f"{sport} phase input lacks matchup names; provide --eligible {sport}=PATH"
        )
    mapping_relation = f"eligible_{sport.lower()}"
    mapping_schema = _register_parquet(con, mapping_relation, eligible_path)
    _require_columns(
        mapping_schema,
        {
            "market_id",
            "game_id",
            "official_date",
            "away_team_name",
            "home_team_name",
        },
        f"{sport} eligible mapping",
    )
    duplicate = con.execute(
        f"""
        SELECT market_id, CAST(game_id AS VARCHAR), count(*)
        FROM {_quote_identifier(mapping_relation)}
        GROUP BY 1, 2 HAVING count(*) <> 1 LIMIT 1
        """
    ).fetchone()
    if duplicate:
        raise LateGameDiagnosticError(
            f"{sport} eligible mapping is not one row per market/game: {duplicate}"
        )
    join = (
        f"LEFT JOIN {_quote_identifier(mapping_relation)} m "
        "ON p.market_id=m.market_id "
        "AND CAST(p.game_id AS VARCHAR)=CAST(m.game_id AS VARCHAR)"
    )
    valid = (
        "m.market_id IS NOT NULL "
        "AND p.official_date IS NOT DISTINCT FROM m.official_date"
    )
    return "m.away_team_name", "m.home_team_name", join, valid


def _build_sport_observations(
    con: duckdb.DuckDBPyConnection,
    sport: str,
    phase_path: Path,
    eligible_path: Path | None,
) -> str:
    phase_relation = f"phase_{sport.lower()}"
    phase_schema = _register_parquet(con, phase_relation, phase_path)
    shared_required = {
        "market_id",
        "official_date",
        "proxyWallet",
        "price",
        "usdc",
        "calibration_error",
        "phase",
        "analysis_eligible",
        "timestamp",
        "transaction_hash",
        "log_index",
        "exchange_address",
        "actual_end_utc",
    }
    if sport == "MLB":
        required = shared_required | {
            "game_pk",
            "won",
            "inning_7_start_utc",
        }
        game_column = "game_pk"
        probability = "p.price"
        outcome = "CAST(p.won AS DOUBLE)"
        phase_start = "p.inning_7_start_utc"
    else:
        required = shared_required | {
            "game_id",
            "token_id",
            "home_token_id",
            "home_won",
            "home_probability",
            "period_4_start_utc",
        }
        game_column = "game_id"
        probability = "p.home_probability"
        outcome = "CAST(p.home_won AS DOUBLE)"
        phase_start = "p.period_4_start_utc"
    _require_columns(phase_schema, required, f"{sport} phase trades")

    if "bought_side" in phase_schema:
        bought_side = "LOWER(p.bought_side)"
    elif {"token_id", "home_token_id"} <= set(phase_schema):
        bought_side = (
            "CASE WHEN p.token_id=p.home_token_id THEN 'home' ELSE 'away' END"
        )
    else:
        bought_side = "'unavailable'"

    away_name, home_name, mapping_join, metadata_valid = _metadata_expressions(
        con, sport, phase_schema, eligible_path
    )
    phase_name = LATE_PHASE[sport]
    published_decile = f"(least(floor(({probability})*10)::INTEGER,9)+1)"
    output_relation = f"observations_{sport.lower()}"
    con.execute(
        f"""
        CREATE TEMP TABLE {_quote_identifier(output_relation)} AS
        WITH selected AS (
          SELECT
            '{sport}'::VARCHAR AS sport,
            '{PROBABILITY_DEFINITION[sport]}'::VARCHAR AS probability_definition,
            '{phase_name}'::VARCHAR AS phase,
            p.market_id::VARCHAR AS market_id,
            CAST(p.{_quote_identifier(game_column)} AS VARCHAR) AS game_id,
            p.official_date::DATE AS official_date,
            {away_name}::VARCHAR AS away_team_name,
            {home_name}::VARCHAR AS home_team_name,
            ({away_name} || ' at ' || {home_name})::VARCHAR AS matchup,
            p.proxyWallet::VARCHAR AS wallet,
            {bought_side}::VARCHAR AS bought_side,
            p.price::DOUBLE AS raw_bought_price,
            {probability}::DOUBLE AS analysis_probability,
            {published_decile}::INTEGER AS published_price_decile,
            {outcome}::DOUBLE AS outcome,
            p.calibration_error::DOUBLE AS calibration_error,
            p.usdc::DOUBLE AS dollars,
            p.timestamp::BIGINT AS "timestamp",
            epoch({phase_start})::DOUBLE AS phase_start_epoch,
            epoch(p.actual_end_utc)::DOUBLE AS phase_end_epoch,
            p.transaction_hash::VARCHAR AS transaction_hash,
            p.log_index::INTEGER AS log_index,
            p.exchange_address::VARCHAR AS exchange_address,
            ({metadata_valid})::BOOLEAN AS metadata_mapping_valid,
            {('p.home_probability::DOUBLE AS serialized_home_probability,' if sport != 'MLB' else 'NULL::DOUBLE AS serialized_home_probability,')}
            {('p.token_id::VARCHAR AS token_id, p.home_token_id::VARCHAR AS home_token_id' if sport != 'MLB' else 'NULL::VARCHAR AS token_id, NULL::VARCHAR AS home_token_id')}
          FROM {_quote_identifier(phase_relation)} p
          {mapping_join}
          WHERE p.analysis_eligible
            AND p.phase='{phase_name}'
            AND {published_decile} IN (1,10)
        )
        SELECT *,
          CASE WHEN published_price_decile=1 THEN 'D1' ELSE 'D10' END::VARCHAR AS tail,
          floor(analysis_probability*100+1e-12)::INTEGER AS probability_band_pp,
          floor(raw_bought_price*100+1e-12)::INTEGER AS raw_price_band_pp,
          CASE
            WHEN (timestamp-phase_start_epoch)/(phase_end_epoch-phase_start_epoch) < 1.0/3.0 THEN 1
            WHEN (timestamp-phase_start_epoch)/(phase_end_epoch-phase_start_epoch) < 2.0/3.0 THEN 2
            ELSE 3
          END::INTEGER AS timing_third,
          ((timestamp-phase_start_epoch)/(phase_end_epoch-phase_start_epoch))::DOUBLE
            AS phase_elapsed_fraction
        FROM selected
        """
    )

    relation = _quote_identifier(output_relation)
    invalid = con.execute(
        f"""
        SELECT market_id, game_id, transaction_hash, log_index
        FROM {relation}
        WHERE market_id IS NULL OR trim(market_id)=''
           OR game_id IS NULL OR trim(game_id)=''
           OR official_date IS NULL
           OR away_team_name IS NULL OR trim(away_team_name)=''
           OR home_team_name IS NULL OR trim(home_team_name)=''
           OR wallet IS NULL OR trim(wallet)=''
           OR bought_side NOT IN ('home','away','unavailable')
           OR NOT isfinite(raw_bought_price) OR raw_bought_price<=0.01 OR raw_bought_price>=0.99
           OR NOT isfinite(analysis_probability)
           OR analysis_probability<0 OR analysis_probability>1
           OR published_price_decile NOT IN (1,10)
           OR tail IS DISTINCT FROM
                CASE WHEN published_price_decile=1 THEN 'D1' ELSE 'D10' END
           OR outcome NOT IN (0.0,1.0)
           OR NOT isfinite(calibration_error)
           OR abs(calibration_error-(outcome-analysis_probability))>1e-12
           OR NOT isfinite(dollars) OR dollars<=0
           OR phase_end_epoch<=phase_start_epoch
           OR phase_elapsed_fraction<0 OR phase_elapsed_fraction>1
           OR transaction_hash IS NULL OR trim(transaction_hash)=''
           OR exchange_address IS NULL OR trim(exchange_address)=''
           OR metadata_mapping_valid IS NOT TRUE
        LIMIT 1
        """
    ).fetchone()
    if invalid:
        raise LateGameDiagnosticError(
            f"{sport} selected late-game tail row violates the contract: {invalid}"
        )
    if sport != "MLB":
        normalization_bad = con.execute(
            f"""
            SELECT market_id, game_id, transaction_hash, log_index
            FROM {relation}
            WHERE token_id IS NULL OR home_token_id IS NULL
               OR (
                 token_id<>home_token_id
                 AND abs(serialized_home_probability-(1.0-raw_bought_price))>1e-12
               )
               OR (
                 token_id=home_token_id
                 AND abs(serialized_home_probability-raw_bought_price)>1e-12
               )
            LIMIT 1
            """
        ).fetchone()
        if normalization_bad:
            raise LateGameDiagnosticError(
                f"{sport} home-probability normalization is inconsistent: {normalization_bad}"
            )
    duplicates = con.execute(
        f"""
        SELECT transaction_hash, log_index, exchange_address, count(*)
        FROM {relation} GROUP BY 1,2,3 HAVING count(*)<>1 LIMIT 1
        """
    ).fetchone()
    if duplicates:
        raise LateGameDiagnosticError(
            f"{sport} selected rows duplicate an immutable fill identity: {duplicates}"
        )
    identity_mismatch = con.execute(
        f"""
        SELECT market_id,game_id
        FROM {relation}
        GROUP BY 1,2
        HAVING count(DISTINCT official_date)<>1
            OR count(DISTINCT away_team_name)<>1
            OR count(DISTINCT home_team_name)<>1
        UNION ALL
        SELECT market_id,min(game_id)
        FROM {relation} GROUP BY market_id HAVING count(DISTINCT game_id)<>1
        UNION ALL
        SELECT min(market_id),game_id
        FROM {relation} GROUP BY game_id HAVING count(DISTINCT market_id)<>1
        LIMIT 1
        """
    ).fetchone()
    if identity_mismatch:
        raise LateGameDiagnosticError(
            f"{sport} market/game metadata is not one-to-one: {identity_mismatch}"
        )
    counts = con.execute(
        f"SELECT tail, count(*) FROM {relation} GROUP BY tail ORDER BY tail"
    ).fetchall()
    if {row[0] for row in counts} != {"D1", "D10"}:
        raise LateGameDiagnosticError(
            f"{sport} must have both late-game tails; found {counts}"
        )
    return output_relation


def _create_output_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE TEMP TABLE observations AS "
        "SELECT * FROM observations_mlb UNION ALL "
        "SELECT * FROM observations_nfl UNION ALL "
        "SELECT * FROM observations_nba"
    )
    con.execute(
        """
        CREATE TEMP TABLE tail_overview AS
        WITH fills AS (
          SELECT sport, probability_definition, phase, tail,
                 count(*)::BIGINT AS trade_count,
                 count(DISTINCT game_id)::BIGINT AS game_count,
                 count(DISTINCT wallet)::BIGINT AS wallet_count,
                 sum(dollars)::DOUBLE AS dollars,
                 avg(analysis_probability)::DOUBLE AS mean_probability,
                 avg(outcome)::DOUBLE AS win_rate,
                 avg(calibration_error)::DOUBLE AS equal_fill_calibration
          FROM observations GROUP BY 1,2,3,4
        ), game_means AS (
          SELECT sport, tail, game_id, avg(calibration_error)::DOUBLE AS game_mean_error
          FROM observations GROUP BY 1,2,3
        ), games AS (
          SELECT sport, tail, avg(game_mean_error)::DOUBLE AS equal_game_calibration
          FROM game_means GROUP BY 1,2
        )
        SELECT f.sport, f.probability_definition, f.phase, f.tail,
               CASE WHEN f.tail='D1' THEN '[0.0,0.1)' ELSE '[0.9,1.0]' END::VARCHAR
                 AS probability_bin,
               f.trade_count, f.game_count, f.wallet_count, f.dollars,
               f.mean_probability, f.win_rate, f.equal_fill_calibration,
               g.equal_game_calibration,
               (f.equal_fill_calibration-g.equal_game_calibration)::DOUBLE
                 AS equal_fill_minus_equal_game
        FROM fills f JOIN games g USING(sport,tail)
        ORDER BY CASE f.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE f.tail WHEN 'D1' THEN 1 ELSE 2 END
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE probability_band_concentration AS
        WITH grouped AS (
          SELECT sport, probability_definition, phase, tail,
                 probability_band_pp, raw_price_band_pp, bought_side,
                 count(*)::BIGINT AS trade_count,
                 count(DISTINCT game_id)::BIGINT AS game_count,
                 count(DISTINCT wallet)::BIGINT AS wallet_count,
                 sum(dollars)::DOUBLE AS dollars,
                 avg(raw_bought_price)::DOUBLE AS mean_raw_bought_price,
                 avg(analysis_probability)::DOUBLE AS mean_analysis_probability,
                 avg(outcome)::DOUBLE AS win_rate,
                 avg(calibration_error)::DOUBLE AS mean_calibration,
                 sum(calibration_error)::DOUBLE AS calibration_numerator
          FROM observations GROUP BY 1,2,3,4,5,6,7
        ), totals AS (
          SELECT sport, tail, sum(trade_count)::DOUBLE AS tail_trades,
                 sum(dollars)::DOUBLE AS tail_dollars
          FROM grouped GROUP BY 1,2
        )
        SELECT g.sport, g.probability_definition, g.phase, g.tail,
               g.probability_band_pp,
               printf('[%.2f,%.2f)',g.probability_band_pp/100.0,
                      (g.probability_band_pp+1)/100.0)::VARCHAR
                 AS probability_band,
               g.raw_price_band_pp,
               printf('[%.2f,%.2f)',g.raw_price_band_pp/100.0,
                      (g.raw_price_band_pp+1)/100.0)::VARCHAR
                 AS raw_bought_price_band,
               g.bought_side, g.trade_count,
               (g.trade_count/t.tail_trades)::DOUBLE AS tail_trade_share,
               g.game_count, g.wallet_count, g.dollars,
               (g.dollars/t.tail_dollars)::DOUBLE AS tail_dollar_share,
               g.mean_raw_bought_price, g.mean_analysis_probability,
               g.win_rate, g.mean_calibration, g.calibration_numerator,
               (g.calibration_numerator/t.tail_trades)::DOUBLE
                 AS contribution_to_equal_fill_mean,
               row_number() OVER (
                 PARTITION BY g.sport,g.tail
                 ORDER BY g.trade_count DESC,g.probability_band_pp,
                          g.raw_price_band_pp,g.bought_side
               )::BIGINT AS trade_count_rank
        FROM grouped g JOIN totals t USING(sport,tail)
        ORDER BY CASE g.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE g.tail WHEN 'D1' THEN 1 ELSE 2 END, trade_count_rank
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE game_contributions AS
        WITH grouped AS (
          SELECT sport, probability_definition, phase, tail, market_id, game_id,
                 min(official_date)::DATE AS official_date,
                 min(away_team_name)::VARCHAR AS away_team_name,
                 min(home_team_name)::VARCHAR AS home_team_name,
                 min(matchup)::VARCHAR AS matchup,
                 count(*)::BIGINT AS trade_count,
                 count(DISTINCT wallet)::BIGINT AS wallet_count,
                 sum(dollars)::DOUBLE AS dollars,
                 avg(analysis_probability)::DOUBLE AS mean_probability,
                 avg(outcome)::DOUBLE AS win_rate,
                 avg(calibration_error)::DOUBLE AS mean_calibration,
                 sum(calibration_error)::DOUBLE AS calibration_numerator
          FROM observations GROUP BY 1,2,3,4,5,6
        ), totals AS (
          SELECT sport, tail, sum(trade_count)::DOUBLE AS tail_trades,
                 sum(dollars)::DOUBLE AS tail_dollars,
                 count(*)::DOUBLE AS tail_games
          FROM grouped GROUP BY 1,2
        )
        SELECT g.*,
               (g.trade_count/t.tail_trades)::DOUBLE AS tail_trade_share,
               (g.dollars/t.tail_dollars)::DOUBLE AS tail_dollar_share,
               (g.calibration_numerator/t.tail_trades)::DOUBLE
                 AS contribution_to_equal_fill_mean,
               (g.mean_calibration/t.tail_games)::DOUBLE
                 AS contribution_to_equal_game_mean,
               row_number() OVER (
                 PARTITION BY g.sport,g.tail
                 ORDER BY abs(g.calibration_numerator/t.tail_trades) DESC,
                          g.game_id,g.market_id
               )::BIGINT AS absolute_fill_contribution_rank
        FROM grouped g JOIN totals t USING(sport,tail)
        ORDER BY CASE g.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE g.tail WHEN 'D1' THEN 1 ELSE 2 END,
                 absolute_fill_contribution_rank
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE wallet_concentration AS
        WITH wallet AS (
          SELECT sport, probability_definition, phase, tail, wallet,
                 count(*)::DOUBLE AS trades, sum(dollars)::DOUBLE AS dollars,
                 sum(calibration_error)::DOUBLE AS error_numerator
          FROM observations GROUP BY 1,2,3,4,5
        ), ranked AS (
          SELECT *,
            row_number() OVER (PARTITION BY sport,tail ORDER BY trades DESC,wallet) r_trades,
            row_number() OVER (PARTITION BY sport,tail ORDER BY dollars DESC,wallet) r_dollars,
            row_number() OVER (
              PARTITION BY sport,tail ORDER BY abs(error_numerator) DESC,wallet
            ) r_abs_error,
            sum(trades) OVER (PARTITION BY sport,tail) total_trades,
            sum(dollars) OVER (PARTITION BY sport,tail) total_dollars,
            sum(abs(error_numerator)) OVER (PARTITION BY sport,tail) total_abs_error
          FROM wallet
        )
        SELECT sport, min(probability_definition)::VARCHAR AS probability_definition,
               min(phase)::VARCHAR AS phase, tail,
               count(*)::BIGINT AS wallet_count,
               sum(pow(trades/total_trades,2))::DOUBLE AS fill_share_hhi,
               sum(CASE WHEN r_trades<=1 THEN trades ELSE 0 END)/max(total_trades)
                 AS top1_fill_share,
               sum(CASE WHEN r_trades<=5 THEN trades ELSE 0 END)/max(total_trades)
                 AS top5_fill_share,
               sum(CASE WHEN r_trades<=10 THEN trades ELSE 0 END)/max(total_trades)
                 AS top10_fill_share,
               sum(CASE WHEN r_dollars<=1 THEN dollars ELSE 0 END)/max(total_dollars)
                 AS top1_dollar_share,
               sum(CASE WHEN r_dollars<=5 THEN dollars ELSE 0 END)/max(total_dollars)
                 AS top5_dollar_share,
               sum(CASE WHEN r_dollars<=10 THEN dollars ELSE 0 END)/max(total_dollars)
                 AS top10_dollar_share,
               CASE WHEN max(total_abs_error)=0 THEN 0 ELSE
                 sum(CASE WHEN r_abs_error<=1 THEN abs(error_numerator) ELSE 0 END)
                   /max(total_abs_error) END::DOUBLE AS top1_absolute_error_share,
               CASE WHEN max(total_abs_error)=0 THEN 0 ELSE
                 sum(CASE WHEN r_abs_error<=5 THEN abs(error_numerator) ELSE 0 END)
                   /max(total_abs_error) END::DOUBLE AS top5_absolute_error_share,
               CASE WHEN max(total_abs_error)=0 THEN 0 ELSE
                 sum(CASE WHEN r_abs_error<=10 THEN abs(error_numerator) ELSE 0 END)
                   /max(total_abs_error) END::DOUBLE AS top10_absolute_error_share
        FROM ranked GROUP BY sport,tail
        ORDER BY CASE sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE tail WHEN 'D1' THEN 1 ELSE 2 END
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE timing_thirds AS
        WITH grid AS (
          SELECT o.sport, min(o.probability_definition)::VARCHAR probability_definition,
                 min(o.phase)::VARCHAR phase, o.tail, r.range::INTEGER timing_third
          FROM observations o CROSS JOIN range(1,4) r GROUP BY o.sport,o.tail,r.range
        ), grouped AS (
          SELECT sport, tail, timing_third,
                 count(*)::BIGINT AS trade_count,
                 count(DISTINCT game_id)::BIGINT AS game_count,
                 count(DISTINCT wallet)::BIGINT AS wallet_count,
                 sum(dollars)::DOUBLE AS dollars,
                 avg(analysis_probability)::DOUBLE AS mean_probability,
                 avg(outcome)::DOUBLE AS win_rate,
                 avg(calibration_error)::DOUBLE AS mean_calibration,
                 sum(calibration_error)::DOUBLE AS calibration_numerator
          FROM observations GROUP BY 1,2,3
        ), totals AS (
          SELECT sport,tail,count(*)::DOUBLE tail_trades,sum(dollars)::DOUBLE tail_dollars
          FROM observations GROUP BY 1,2
        )
        SELECT x.sport,x.probability_definition,x.phase,x.tail,x.timing_third,
               CASE x.timing_third WHEN 1 THEN 'early final phase'
                    WHEN 2 THEN 'middle final phase' ELSE 'late final phase' END::VARCHAR
                 AS timing_label,
               ((x.timing_third-1)/3.0)::DOUBLE AS elapsed_fraction_low,
               (x.timing_third/3.0)::DOUBLE AS elapsed_fraction_high,
               coalesce(g.trade_count,0)::BIGINT AS trade_count,
               (coalesce(g.trade_count,0)/t.tail_trades)::DOUBLE AS tail_trade_share,
               coalesce(g.game_count,0)::BIGINT AS game_count,
               coalesce(g.wallet_count,0)::BIGINT AS wallet_count,
               coalesce(g.dollars,0)::DOUBLE AS dollars,
               (coalesce(g.dollars,0)/t.tail_dollars)::DOUBLE AS tail_dollar_share,
               g.mean_probability,g.win_rate,g.mean_calibration,
               coalesce(g.calibration_numerator,0)::DOUBLE AS calibration_numerator,
               (coalesce(g.calibration_numerator,0)/t.tail_trades)::DOUBLE
                 AS contribution_to_equal_fill_mean
        FROM grid x LEFT JOIN grouped g USING(sport,tail,timing_third)
        JOIN totals t USING(sport,tail)
        ORDER BY CASE x.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE x.tail WHEN 'D1' THEN 1 ELSE 2 END,x.timing_third
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE leave_top_k_games AS
        WITH ks(k) AS (VALUES (1),(5),(10)),
        tail_full AS (
          SELECT sport,tail,count(*)::DOUBLE full_trades,
                 count(DISTINCT game_id)::BIGINT full_games,
                 avg(calibration_error)::DOUBLE full_equal_fill_calibration
          FROM observations GROUP BY 1,2
        ), game_full AS (
          SELECT sport,tail,avg(mean_calibration)::DOUBLE full_equal_game_calibration
          FROM game_contributions GROUP BY 1,2
        ), ranked_observations AS (
          SELECT o.*,g.absolute_fill_contribution_rank
          FROM observations o JOIN game_contributions g
            ON g.sport=o.sport AND g.tail=o.tail AND g.market_id=o.market_id
           AND g.game_id=o.game_id
        ), remaining AS (
          SELECT f.sport,f.tail,k.k,
                 count(o.calibration_error)::BIGINT remaining_trades,
                 count(DISTINCT o.game_id)::BIGINT remaining_games,
                 avg(o.calibration_error)::DOUBLE remaining_equal_fill_calibration
          FROM tail_full f CROSS JOIN ks k
          LEFT JOIN ranked_observations o
            ON o.sport=f.sport AND o.tail=f.tail
           AND o.absolute_fill_contribution_rank>k.k
          GROUP BY f.sport,f.tail,k.k
        ), remaining_games AS (
          SELECT f.sport,f.tail,k.k,
                 avg(g.mean_calibration)::DOUBLE AS remaining_equal_game_calibration
          FROM tail_full f CROSS JOIN ks k
          LEFT JOIN game_contributions g
            ON g.sport=f.sport AND g.tail=f.tail
           AND g.absolute_fill_contribution_rank>k.k
          GROUP BY f.sport,f.tail,k.k
        )
        SELECT r.sport,o.probability_definition,o.phase,r.tail,r.k::INTEGER AS k,
               least(r.k,f.full_games)::BIGINT AS removed_games,
               (f.full_trades-r.remaining_trades)::BIGINT AS removed_trades,
               ((f.full_trades-r.remaining_trades)/f.full_trades)::DOUBLE
                 AS removed_trade_share,
               r.remaining_games,r.remaining_trades,
               f.full_equal_fill_calibration,r.remaining_equal_fill_calibration,
               (r.remaining_equal_fill_calibration-f.full_equal_fill_calibration)::DOUBLE
                 AS equal_fill_change,
               gf.full_equal_game_calibration,rg.remaining_equal_game_calibration,
               (rg.remaining_equal_game_calibration-gf.full_equal_game_calibration)::DOUBLE
                 AS equal_game_change
        FROM remaining r JOIN tail_full f USING(sport,tail)
        JOIN game_full gf USING(sport,tail)
        JOIN remaining_games rg USING(sport,tail,k)
        JOIN tail_overview o USING(sport,tail)
        ORDER BY CASE r.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE r.tail WHEN 'D1' THEN 1 ELSE 2 END,r.k
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE outcome_decomposition AS
        WITH grouped AS (
          SELECT sport, probability_definition, phase, tail,
                 outcome::INTEGER AS outcome,
                 count(*)::BIGINT AS trade_count,
                 count(DISTINCT game_id)::BIGINT AS game_count,
                 count(DISTINCT wallet)::BIGINT AS wallet_count,
                 sum(dollars)::DOUBLE AS dollars,
                 avg(analysis_probability)::DOUBLE AS mean_probability,
                 avg(calibration_error)::DOUBLE AS mean_calibration,
                 sum(calibration_error)::DOUBLE AS calibration_numerator
          FROM observations GROUP BY 1,2,3,4,5
        ), totals AS (
          SELECT sport,tail,count(*)::DOUBLE tail_trades,sum(dollars)::DOUBLE tail_dollars
          FROM observations GROUP BY 1,2
        )
        SELECT g.sport,g.probability_definition,g.phase,g.tail,g.outcome,
               CASE g.outcome WHEN 1 THEN 'won' ELSE 'lost' END::VARCHAR AS outcome_label,
               g.trade_count,(g.trade_count/t.tail_trades)::DOUBLE AS tail_trade_share,
               g.game_count,g.wallet_count,g.dollars,
               (g.dollars/t.tail_dollars)::DOUBLE AS tail_dollar_share,
               g.mean_probability,g.mean_calibration,g.calibration_numerator,
               (g.calibration_numerator/t.tail_trades)::DOUBLE
                 AS contribution_to_equal_fill_mean
        FROM grouped g JOIN totals t USING(sport,tail)
        ORDER BY CASE g.sport WHEN 'MLB' THEN 1 WHEN 'NFL' THEN 2 ELSE 3 END,
                 CASE g.tail WHEN 'D1' THEN 1 ELSE 2 END,g.outcome
        """
    )


def _write_parquet(
    con: duckdb.DuckDBPyConnection, relation: str, path: Path
) -> None:
    con.execute(
        f"COPY (SELECT * FROM {_quote_identifier(relation)}) "
        f"TO '{_quote_path(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def run_diagnostic(
    phase_inputs: Mapping[str, str | Path],
    run_dir: str | Path,
    eligible_inputs: Mapping[str, str | Path] | None = None,
    *,
    command: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build an immutable bundle of late-game D1/D10 driver summaries."""

    normalized_phases = {sport.upper(): _path(path) for sport, path in phase_inputs.items()}
    if set(normalized_phases) != set(SPORTS):
        raise LateGameDiagnosticError(
            "phase_inputs must contain exactly MLB, NFL, and NBA"
        )
    normalized_eligible = {
        sport.upper(): _path(path)
        for sport, path in (eligible_inputs or {}).items()
    }
    if not set(normalized_eligible) <= set(SPORTS):
        raise LateGameDiagnosticError("eligible_inputs contains an unsupported sport")
    all_inputs = [*normalized_phases.values(), *normalized_eligible.values()]
    for path in all_inputs:
        if not path.is_file():
            raise LateGameDiagnosticError(f"Input does not exist: {path}")
    destination = _path(run_dir)
    _validate_destination(destination, all_inputs)
    staging = destination.with_name(f".{destination.name}.staging-{os.getpid()}")
    if staging.exists():
        raise FileExistsError(f"Staging directory already exists: {staging}")
    staging.mkdir(parents=True)
    con = duckdb.connect()
    try:
        for sport in SPORTS:
            _build_sport_observations(
                con,
                sport,
                normalized_phases[sport],
                normalized_eligible.get(sport),
            )
        _create_output_tables(con)
        outputs: dict[str, dict[str, Any]] = {}
        counts: dict[str, int] = {}
        for relation in OUTPUT_TABLES:
            path = staging / f"{relation}.parquet"
            _write_parquet(con, relation, path)
            counts[relation] = int(
                con.execute(
                    f"SELECT count(*) FROM {_quote_identifier(relation)}"
                ).fetchone()[0]
            )
            outputs[relation] = _fingerprint(path)
            outputs[relation]["path"] = path.name
        manifest = {
            "schema_version": 1,
            "method": "late_game_tail_driver_diagnostic_v1",
            "interpretation_status": "exploratory_descriptive",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "command": list(command) if command is not None else ["library_call"],
            "environment": {
                "python_version": platform.python_version(),
                "python_implementation": platform.python_implementation(),
                "duckdb_version": duckdb.__version__,
                "platform": platform.platform(),
            },
            "code": {
                "script": _fingerprint(Path(__file__).resolve()),
            },
            "definitions": {
                "sample": (
                    "analysis_eligible BUY fills in the literal final phase; no "
                    "30-second boundary exclusion is applied"
                ),
                "source_filters": (
                    "phase-trade inputs already apply 0.01 < raw bought price < 0.99 "
                    "and exclude flagged outcome-token buyers"
                ),
                "late_phase": LATE_PHASE,
                "probability_and_outcome": {
                    "MLB": "raw bought-contract price and bought-contract eventual outcome",
                    "NFL": "home-win probability and eventual home-win outcome",
                    "NBA": "home-win probability and eventual home-win outcome",
                },
                "tails": {
                    "price_decile": "least(floor(P*10)::INTEGER,9)+1",
                    "D1": "published price_decile = 1",
                    "D10": "published price_decile = 10",
                },
                "equal_fill_calibration": "mean(Y-P) over eligible fills",
                "equal_game_calibration": (
                    "mean across games of each game's within-tail mean(Y-P)"
                ),
                "probability_band": (
                    "one-percentage-point left-closed, right-open bands formed with "
                    "floor(P*100+1e-12); raw bought price is banded separately"
                ),
                "bought_side": (
                    "home or away outcome token when serialized or derivable; otherwise unavailable"
                ),
                "equal_fill_contribution": (
                    "subgroup sum(Y-P) divided by the sport-tail fill count; subgroup "
                    "contributions sum to the equal-fill tail mean"
                ),
                "game_ranking": (
                    "descending absolute game sum(Y-P) divided by the sport-tail fill count"
                ),
                "wallet_concentration": (
                    "fill-share HHI and top-k shares by fills, dollars, and absolute "
                    "wallet-level sum(Y-P)"
                ),
                "timing_thirds": (
                    "elapsed wall-clock intervals [0,1/3), [1/3,2/3), and [2/3,1] "
                    "from the start of inning 7 or quarter 4 through the recorded final "
                    "play; exact cut points enter the later interval"
                ),
                "leave_top_k_games": (
                    "remove k=1,5,10 games ranked by absolute equal-fill contribution, "
                    "then recompute equal-fill and equal-game calibration"
                ),
                "outcome_decomposition": (
                    "partition each sport-tail by Y=0 versus Y=1 and report each "
                    "outcome group's contribution to the equal-fill calibration mean"
                ),
            },
            "inputs": {
                sport: {
                    "phase_trades": _fingerprint(normalized_phases[sport]),
                    "eligible_moneylines": (
                        _fingerprint(normalized_eligible[sport])
                        if sport in normalized_eligible
                        else None
                    ),
                }
                for sport in SPORTS
            },
            "counts": counts,
            "outputs": outputs,
            "completion_status": "complete",
        }
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        staging.rename(destination)
        return manifest
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        con.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="+",
        metavar="SPORT=PHASE_TRADES",
        help="exactly one finalized phase-trade parquet for MLB, NFL, and NBA",
    )
    parser.add_argument(
        "--eligible",
        action="append",
        default=[],
        metavar="SPORT=ELIGIBLE_MONEYLINES",
        help="matchup mapping required when phase trades do not include team names",
    )
    parser.add_argument("--run-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    phase_inputs = _parse_assignments(
        args.inputs, require_all_sports=True, label="phase input"
    )
    eligible_inputs = _parse_assignments(
        args.eligible, require_all_sports=False, label="eligible mapping"
    )
    manifest = run_diagnostic(
        phase_inputs,
        args.run_dir,
        eligible_inputs,
        command=[
            sys.executable,
            "-m",
            "analysis.sports_game_dynamics.diagnose_late_game_tails",
            *(argv or sys.argv[1:]),
        ],
    )
    print(json.dumps({"run_dir": str(_path(args.run_dir)), "counts": manifest["counts"]}))


if __name__ == "__main__":
    main()
