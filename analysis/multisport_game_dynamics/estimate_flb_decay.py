"""Estimate how bought-contract calibration changes over normalized event time.

The primary tail model is restricted to fixed bought-price bins D1 and D10.  It
uses exact block timestamps and the frozen nine-sport moneyline phase datasets.
Outputs are immutable Parquet summaries plus a machine-facing manifest.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from typing import Any, Iterable, Sequence

import duckdb
import numpy as np

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
    write_parquet,
)


SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
MIN_N = 500
CLUSTERS = ("trade_day", "proxyWallet", "event_cluster")

COEFFICIENT_SCHEMA = (
    ("model_id", "VARCHAR"), ("family", "VARCHAR"), ("scope", "VARCHAR"),
    ("sport", "VARCHAR"), ("sample", "VARCHAR"),
    ("time_normalization", "VARCHAR"), ("weighting", "VARCHAR"),
    ("adjustment", "VARCHAR"), ("term_order", "INTEGER"), ("term", "VARCHAR"),
    ("estimate", "DOUBLE"), ("standard_error", "DOUBLE"),
    ("t_statistic", "DOUBLE"), ("p_value", "DOUBLE"),
    ("ci95_low", "DOUBLE"), ("ci95_high", "DOUBLE"),
)
MODEL_SCHEMA = (
    ("model_id", "VARCHAR"), ("family", "VARCHAR"), ("scope", "VARCHAR"),
    ("sport", "VARCHAR"), ("sample", "VARCHAR"),
    ("time_normalization", "VARCHAR"), ("window_low", "DOUBLE"),
    ("window_high", "DOUBLE"), ("weighting", "VARCHAR"),
    ("adjustment", "VARCHAR"), ("n_obs", "BIGINT"), ("n_events", "BIGINT"),
    ("n_days", "BIGINT"), ("n_wallets", "BIGINT"),
    ("n_event_clusters", "BIGINT"), ("n_parameters", "INTEGER"),
    ("rank", "INTEGER"), ("r_squared", "DOUBLE"),
    ("condition_number", "DOUBLE"), ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
)
SUPPORT_SCHEMA = (
    ("sample", "VARCHAR"), ("time_normalization", "VARCHAR"),
    ("sport", "VARCHAR"), ("segment", "VARCHAR"), ("tail", "VARCHAR"),
    ("n_obs", "BIGINT"), ("n_events", "BIGINT"),
)
DURATION_SCHEMA = (
    ("sport", "VARCHAR"), ("event_count", "BIGINT"),
    ("median_duration_seconds", "DOUBLE"),
    ("median_duration_minutes", "DOUBLE"),
)
ESTIMAND_SCHEMA = (
    ("estimand_id", "VARCHAR"), ("source_model_id", "VARCHAR"),
    ("family", "VARCHAR"), ("scope", "VARCHAR"), ("sport", "VARCHAR"),
    ("sample", "VARCHAR"), ("time_normalization", "VARCHAR"),
    ("weighting", "VARCHAR"), ("adjustment", "VARCHAR"),
    ("estimand", "VARCHAR"), ("estimate", "DOUBLE"),
    ("standard_error", "DOUBLE"), ("t_statistic", "DOUBLE"),
    ("p_value", "DOUBLE"), ("ci95_low", "DOUBLE"),
    ("ci95_high", "DOUBLE"), ("n_obs", "BIGINT"),
    ("n_events", "BIGINT"), ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
)
TIME_BIN_SCHEMA = (
    ("scope", "VARCHAR"), ("sport", "VARCHAR"), ("weighting", "VARCHAR"),
    ("time_bin", "INTEGER"), ("time_low", "DOUBLE"), ("time_high", "DOUBLE"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
    ("d1_mean_calibration", "DOUBLE"), ("d10_mean_calibration", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"), ("spread_standard_error", "DOUBLE"),
    ("spread_ci95_low", "DOUBLE"), ("spread_ci95_high", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
)


@dataclass(frozen=True)
class FitResult:
    beta: np.ndarray
    covariance: np.ndarray
    n_obs: int
    n_events: int
    n_days: int
    n_wallets: int
    n_event_clusters: int
    rank: int
    r_squared: float
    condition_number: float


def _rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _normal_p(t_statistic: float) -> float:
    return 2.0 * (1.0 - NormalDist().cdf(abs(t_statistic)))


def _linear_result(
    beta: np.ndarray, covariance: np.ndarray, contrast: np.ndarray
) -> tuple[float, float, float, float, float, float]:
    estimate = float(contrast @ beta)
    variance = float(contrast @ covariance @ contrast)
    standard_error = math.sqrt(max(variance, 0.0))
    t_statistic = estimate / standard_error if standard_error > 0 else math.nan
    p_value = _normal_p(t_statistic) if math.isfinite(t_statistic) else math.nan
    return (
        estimate,
        standard_error,
        t_statistic,
        p_value,
        estimate - 1.96 * standard_error,
        estimate + 1.96 * standard_error,
    )


def _create_observations(
    con: duckdb.DuckDBPyConnection,
    new_phase: Path,
    mlb_phase: Path,
    nfl_phase: Path,
    nba_phase: Path,
) -> None:
    retained = ",".join(f"'{sport}'" for sport in SPORTS[3:])
    con.execute(
        f"""
        CREATE TEMP TABLE observations AS
        SELECT sport,event_slug::VARCHAR event_id,market_id::VARCHAR market_id,
               market_date::DATE market_date,"timestamp"::BIGINT trade_timestamp,
               price::DOUBLE price,won::DOUBLE won,
               (won::DOUBLE-price)::DOUBLE calibration_error,usdc::DOUBLE usdc,
               proxyWallet::VARCHAR proxyWallet,trade_day::DATE trade_day,
               actual_start_utc,actual_end_utc
        FROM read_parquet('{quoted(new_phase)}')
        WHERE sport IN ({retained})
        UNION ALL
        SELECT 'mlb',game_pk::VARCHAR,market_id::VARCHAR,official_date::DATE,
               "timestamp"::BIGINT,price::DOUBLE,won::DOUBLE,
               (won::DOUBLE-price)::DOUBLE,usdc::DOUBLE,proxyWallet::VARCHAR,
               trade_day_utc::DATE,actual_start_utc,actual_end_utc
        FROM read_parquet('{quoted(mlb_phase)}') WHERE analysis_eligible
        UNION ALL
        SELECT 'nfl',game_id::VARCHAR,market_id::VARCHAR,official_date::DATE,
               "timestamp"::BIGINT,price::DOUBLE,
               (token_id=winning_token_id)::DOUBLE,
               ((token_id=winning_token_id)::DOUBLE-price)::DOUBLE,
               usdc::DOUBLE,proxyWallet::VARCHAR,trade_day::DATE,
               actual_start_utc,actual_end_utc
        FROM read_parquet('{quoted(nfl_phase)}') WHERE analysis_eligible
        UNION ALL
        SELECT 'nba',game_id::VARCHAR,market_id::VARCHAR,official_date::DATE,
               "timestamp"::BIGINT,price::DOUBLE,
               (token_id=winning_token_id)::DOUBLE,
               ((token_id=winning_token_id)::DOUBLE-price)::DOUBLE,
               usdc::DOUBLE,proxyWallet::VARCHAR,trade_day::DATE,
               actual_start_utc,actual_end_utc
        FROM read_parquet('{quoted(nba_phase)}') WHERE analysis_eligible
        """
    )
    con.execute("ALTER TABLE observations ADD COLUMN price_decile INTEGER")
    con.execute("ALTER TABLE observations ADD COLUMN event_cluster VARCHAR")
    con.execute("ALTER TABLE observations ADD COLUMN realized_time DOUBLE")
    con.execute(
        """
        UPDATE observations SET
          price_decile=least(floor(price*10)::INTEGER+1,10),
          event_cluster=sport || ':' || event_id,
          realized_time=(trade_timestamp-epoch(actual_start_utc)) /
                        (epoch(actual_end_utc)-epoch(actual_start_utc))
        """
    )
    invalid = con.execute(
        """
        SELECT count(*) FROM observations
        WHERE sport NOT IN ('mlb','nfl','nba','nhl','cbb','atp','epl','cfb','wnba')
           OR event_id IS NULL OR trim(event_id)='' OR market_id IS NULL
           OR trade_timestamp IS NULL OR proxyWallet IS NULL OR trim(proxyWallet)=''
           OR trade_day IS NULL OR price<=0.01 OR price>=0.99
           OR won NOT IN (0,1) OR usdc<=0 OR NOT isfinite(usdc)
           OR abs(calibration_error-(won-price))>1e-12
           OR actual_end_utc<=actual_start_utc
           OR NOT isfinite(realized_time) OR realized_time>1+1e-9
        """
    ).fetchone()[0]
    if invalid:
        raise ValueError(f"Invalid normalized observations: {invalid}")
    observed_sports = tuple(
        row[0] for row in con.execute("SELECT DISTINCT sport FROM observations ORDER BY sport").fetchall()
    )
    if set(observed_sports) != set(SPORTS):
        raise ValueError(f"Sport domain mismatch: {observed_sports}")
    con.execute(
        """
        CREATE TEMP TABLE duration_reference AS
        SELECT sport,count(*)::BIGINT event_count,
               median(duration_seconds)::DOUBLE median_duration_seconds
        FROM (
          SELECT DISTINCT sport,event_cluster,
                 epoch(actual_end_utc)-epoch(actual_start_utc) AS duration_seconds
          FROM observations
        ) GROUP BY sport
        """
    )
    con.execute("ALTER TABLE observations ADD COLUMN fixed_time DOUBLE")
    con.execute(
        """
        UPDATE observations AS o
        SET fixed_time=(o.trade_timestamp-epoch(o.actual_start_utc))/d.median_duration_seconds
        FROM duration_reference d WHERE d.sport=o.sport
        """
    )
    con.execute(
        """
        CREATE TEMP VIEW weighted_observations AS
        SELECT * FROM observations
        """
    )


def _feature_sql(feature_expressions: Sequence[tuple[str, str]]) -> str:
    return ",".join(f"({expression})::DOUBLE x{index}" for index, (_, expression) in enumerate(feature_expressions))


def _fit_ols(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    feature_expressions: Sequence[tuple[str, str]],
    weight_expression: str,
) -> FitResult:
    k = len(feature_expressions)
    feature_sql = _feature_sql(feature_expressions)
    con.execute("DROP VIEW IF EXISTS fit_base")
    con.execute("DROP VIEW IF EXISTS fit_rows")
    con.execute(
        f"""
        CREATE TEMP VIEW fit_base AS
        SELECT sport,calibration_error::DOUBLE y,usdc,event_cluster,trade_day,
               proxyWallet,{feature_sql}
        FROM weighted_observations WHERE {where_sql}
        """
    )
    weight_sql = (
        "(1.0/count(*) OVER(PARTITION BY sport))::DOUBLE"
        if weight_expression == "equal_sport_sample"
        else f"({weight_expression})::DOUBLE"
    )
    con.execute(
        f"""
        CREATE TEMP VIEW fit_rows AS
        SELECT y,{weight_sql} w,event_cluster,trade_day,proxyWallet,
               {','.join(f'x{index}' for index in range(k))}
        FROM fit_base
        """
    )
    counts = con.execute(
        """
        SELECT count(*)::BIGINT,count(DISTINCT event_cluster)::BIGINT,
               count(DISTINCT trade_day)::BIGINT,count(DISTINCT proxyWallet)::BIGINT
        FROM fit_rows
        """
    ).fetchone()
    n_obs, n_events, n_days, n_wallets = map(int, counts)
    if n_obs == 0:
        raise ValueError("Regression sample is empty")
    moments: list[str] = []
    for left in range(k):
        for right in range(left, k):
            moments.append(f"sum(w*x{left}*x{right})::DOUBLE m_{left}_{right}")
    moments.extend(f"sum(w*x{index}*y)::DOUBLE b_{index}" for index in range(k))
    moment = con.execute("SELECT " + ",".join(moments) + " FROM fit_rows").fetchone()
    xtwx = np.zeros((k, k), dtype=float)
    position = 0
    for left in range(k):
        for right in range(left, k):
            value = float(moment[position])
            xtwx[left, right] = value
            xtwx[right, left] = value
            position += 1
    xtwy = np.array(moment[position:position + k], dtype=float)
    rank = int(np.linalg.matrix_rank(xtwx))
    if rank != k:
        raise ValueError(f"Rank-deficient design: rank={rank}, parameters={k}")
    condition_number = float(np.linalg.cond(xtwx))
    bread = np.linalg.inv(xtwx)
    beta = bread @ xtwy
    residual_expression = "y-(" + "+".join(
        f"({float(beta[index]):.17g})*x{index}" for index in range(k)
    ) + ")"
    con.execute("DROP VIEW IF EXISTS residual_rows")
    con.execute(
        f"CREATE TEMP VIEW residual_rows AS SELECT *,({residual_expression})::DOUBLE u FROM fit_rows"
    )
    meat = np.zeros((k, k), dtype=float)
    for size in range(1, len(CLUSTERS) + 1):
        sign = 1.0 if size % 2 else -1.0
        for subset in itertools.combinations(CLUSTERS, size):
            scores = ",".join(f"sum(w*x{index}*u)::DOUBLE s{index}" for index in range(k))
            cross = []
            for left in range(k):
                for right in range(left, k):
                    cross.append(f"sum(s{left}*s{right})::DOUBLE c_{left}_{right}")
            values = con.execute(
                "WITH cluster_scores AS (SELECT "
                + ",".join(subset) + "," + scores
                + " FROM residual_rows GROUP BY " + ",".join(subset)
                + ") SELECT " + ",".join(cross) + " FROM cluster_scores"
            ).fetchone()
            offset = 0
            for left in range(k):
                for right in range(left, k):
                    value = 0.0 if values[offset] is None else float(values[offset])
                    meat[left, right] += sign * value
                    if left != right:
                        meat[right, left] += sign * value
                    offset += 1
    covariance = bread @ meat @ bread
    covariance = (covariance + covariance.T) / 2.0
    fit_stats = con.execute(
        "SELECT sum(w*u*u)::DOUBLE, sum(w*y*y)::DOUBLE, sum(w*y)::DOUBLE, sum(w)::DOUBLE FROM residual_rows"
    ).fetchone()
    sse, sum_y2, sum_y, sum_w = map(float, fit_stats)
    tss = sum_y2 - sum_y * sum_y / sum_w
    r_squared = 1.0 - sse / tss if tss > 0 else math.nan
    return FitResult(
        beta=beta,
        covariance=covariance,
        n_obs=n_obs,
        n_events=n_events,
        n_days=n_days,
        n_wallets=n_wallets,
        n_event_clusters=n_events,
        rank=rank,
        r_squared=r_squared,
        condition_number=condition_number,
    )


def _sample_clause(sample: str, time_column: str) -> tuple[str, float, float]:
    windows = {
        "unified": (-1.0, 1.0),
        "live_only": (0.0, 1.0),
        "wider_pregame": (-2.0, 1.0),
    }
    low, high = windows[sample]
    return f"{time_column}>={low} AND {time_column}<={high}", low, high


def _tail_features(time_column: str) -> list[tuple[str, str]]:
    return [
        ("Intercept", "1"),
        ("D10", "(price_decile=10)::INTEGER"),
        ("Time", time_column),
        ("D10 x time", f"(price_decile=10)::INTEGER*{time_column}"),
    ]


def _continuous_features(time_column: str) -> list[tuple[str, str]]:
    return [
        ("Intercept", "1"),
        ("Price centered", "price-0.5"),
        ("Time", time_column),
        ("Price x time", f"(price-0.5)*{time_column}"),
    ]


def _piecewise_features(time_column: str) -> list[tuple[str, str]]:
    negative = f"least({time_column},0.0)"
    positive = f"greatest({time_column},0.0)"
    return [
        ("Intercept", "1"),
        ("D10", "(price_decile=10)::INTEGER"),
        ("Pregame time", negative),
        ("Live time", positive),
        ("D10 x pregame time", f"(price_decile=10)::INTEGER*{negative}"),
        ("D10 x live time", f"(price_decile=10)::INTEGER*{positive}"),
    ]


def _pooled_tail_features(
    time_column: str, adjustment: str, sports: Sequence[str] = SPORTS
) -> list[tuple[str, str]]:
    features = _tail_features(time_column)
    if adjustment == "none":
        return features
    if adjustment == "sport_intercepts":
        return features + [
            (f"{sport.upper()} FE", f"(sport='{sport}')::INTEGER") for sport in sports[1:]
        ]
    if adjustment == "sport_composition":
        additions: list[tuple[str, str]] = []
        for sport in sports[1:]:
            indicator = f"(sport='{sport}')::INTEGER"
            additions.extend(
                [
                    (f"{sport.upper()} FE", indicator),
                    (f"{sport.upper()} x D10", f"{indicator}*(price_decile=10)::INTEGER"),
                    (f"{sport.upper()} x time", f"{indicator}*{time_column}"),
                ]
            )
        return features + additions
    if adjustment == "fully_interacted":
        additions = []
        for sport in sports[1:]:
            indicator = f"(sport='{sport}')::INTEGER"
            additions.extend(
                [
                    (f"{sport.upper()} FE", indicator),
                    (f"{sport.upper()} x D10", f"{indicator}*(price_decile=10)::INTEGER"),
                    (f"{sport.upper()} x time", f"{indicator}*{time_column}"),
                    (f"{sport.upper()} x D10 x time", f"{indicator}*(price_decile=10)::INTEGER*{time_column}"),
                ]
            )
        return features + additions
    raise ValueError(f"Unknown adjustment: {adjustment}")


def _pooled_continuous_features(time_column: str, adjustment: str) -> list[tuple[str, str]]:
    features = _continuous_features(time_column)
    if adjustment == "none":
        return features
    if adjustment != "sport_composition":
        raise ValueError(adjustment)
    additions: list[tuple[str, str]] = []
    for sport in SPORTS[1:]:
        indicator = f"(sport='{sport}')::INTEGER"
        additions.extend(
            [
                (f"{sport.upper()} FE", indicator),
                (f"{sport.upper()} x price", f"{indicator}*(price-0.5)"),
                (f"{sport.upper()} x time", f"{indicator}*{time_column}"),
            ]
        )
    return features + additions


def _pooled_piecewise_features(
    time_column: str, sports: Sequence[str] = SPORTS
) -> list[tuple[str, str]]:
    features = _piecewise_features(time_column)
    negative = f"least({time_column},0.0)"
    positive = f"greatest({time_column},0.0)"
    for sport in sports[1:]:
        indicator = f"(sport='{sport}')::INTEGER"
        features.extend(
            [
                (f"{sport.upper()} FE", indicator),
                (f"{sport.upper()} x D10", f"{indicator}*(price_decile=10)::INTEGER"),
                (f"{sport.upper()} x pregame time", f"{indicator}*{negative}"),
                (f"{sport.upper()} x live time", f"{indicator}*{positive}"),
            ]
        )
    return features


def _mean_sport_slope_contrast(
    features: Sequence[tuple[str, str]], sports: Sequence[str]
) -> np.ndarray:
    """Return the literal equal-weight mean of interaction-coded sport slopes."""
    contrast = np.zeros(len(features))
    base_index = next(
        index for index, item in enumerate(features) if item[0] == "D10 x time"
    )
    contrast[base_index] = 1.0
    for sport in sports[1:]:
        term = f"{sport.upper()} x D10 x time"
        contrast[
            next(index for index, item in enumerate(features) if item[0] == term)
        ] = 1.0 / len(sports)
    return contrast


def _support_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    output: list[tuple[Any, ...]] = []
    definitions = (
        ("unified", "realized_time", -1.0, 1.0),
        ("live_only", "realized_time", 0.0, 1.0),
        ("wider_pregame", "realized_time", -2.0, 1.0),
        ("unified", "fixed_time", -1.0, 1.0),
    )
    for sample, time_column, low, high in definitions:
        rows = _rows(
            con,
            f"""
            SELECT sport,CASE WHEN {time_column}<0 THEN 'pregame' ELSE 'live' END segment,
                   CASE WHEN price_decile=1 THEN 'D1' ELSE 'D10' END tail,
                   count(*)::BIGINT n_obs,count(DISTINCT event_cluster)::BIGINT n_events
            FROM weighted_observations
            WHERE price_decile IN (1,10) AND {time_column}>={low} AND {time_column}<={high}
            GROUP BY 1,2,3
            """,
        )
        time_name = "realized_duration" if time_column == "realized_time" else "sport_median_duration"
        observed = {(row["sport"], row["segment"], row["tail"]): row for row in rows}
        segments = ("live",) if sample == "live_only" else ("pregame", "live")
        for sport in SPORTS:
            for segment in segments:
                for tail in ("D1", "D10"):
                    row = observed.get((sport, segment, tail), {})
                    output.append(
                        (sample, time_name, sport, segment, tail, int(row.get("n_obs", 0)), int(row.get("n_events", 0)))
                    )
    return output


def _time_bin_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    """Return live-time decile tail spreads with joint three-way clustered SEs."""

    output: list[tuple[Any, ...]] = []
    specifications = (
        ("sport", "equal_fill", "1.0", True),
        ("pooled", "equal_fill", "1.0", False),
        ("pooled", "equal_sport", "equal_sport_sample", False),
    )
    for scope, weighting, weight_expression, by_sport in specifications:
        con.execute("DROP VIEW IF EXISTS binned_tail")
        con.execute("DROP TABLE IF EXISTS binned_means")
        weight_sql = (
            "(1.0/count(*) OVER(PARTITION BY sport))::DOUBLE"
            if weight_expression == "equal_sport_sample"
            else f"({weight_expression})::DOUBLE"
        )
        con.execute(
            f"""
            CREATE TEMP VIEW binned_tail AS
            SELECT sport,event_cluster,trade_day,proxyWallet,price_decile,
                   calibration_error,{weight_sql} w,
                   least(floor(realized_time*10)::INTEGER+1,10) AS time_bin
            FROM weighted_observations
            WHERE price_decile IN (1,10) AND realized_time>=0 AND realized_time<=1
            """
        )
        keys = ("sport", "time_bin") if by_sport else ("time_bin",)
        key_sql = ",".join(keys)
        con.execute(
            f"""
            CREATE TEMP TABLE binned_means AS
            SELECT {key_sql},
                   count(*) FILTER(WHERE price_decile=1)::BIGINT d1_n,
                   count(*) FILTER(WHERE price_decile=10)::BIGINT d10_n,
                   count(DISTINCT event_cluster) FILTER(WHERE price_decile=1)::BIGINT d1_events,
                   count(DISTINCT event_cluster) FILTER(WHERE price_decile=10)::BIGINT d10_events,
                   sum(w) FILTER(WHERE price_decile=1)::DOUBLE d1_weight,
                   sum(w) FILTER(WHERE price_decile=10)::DOUBLE d10_weight,
                   (sum(w*calibration_error) FILTER(WHERE price_decile=1) /
                    sum(w) FILTER(WHERE price_decile=1))::DOUBLE d1_mean,
                   (sum(w*calibration_error) FILTER(WHERE price_decile=10) /
                    sum(w) FILTER(WHERE price_decile=10))::DOUBLE d10_mean
            FROM binned_tail GROUP BY {key_sql}
            """
        )
        variances: dict[tuple[Any, ...], float] = {}
        for size in range(1, len(CLUSTERS) + 1):
            sign = 1.0 if size % 2 else -1.0
            for subset in itertools.combinations(CLUSTERS, size):
                cluster_sql = ",".join(f"b.{name}" for name in subset)
                rows = _rows(
                    con,
                    f"""
                    WITH cluster_scores AS (
                      SELECT {','.join(f'b.{key}' for key in keys)},{cluster_sql},sum(
                        CASE WHEN b.price_decile=10
                          THEN b.w*(b.calibration_error-m.d10_mean)/m.d10_weight
                          ELSE -b.w*(b.calibration_error-m.d1_mean)/m.d1_weight END
                      )::DOUBLE score
                      FROM binned_tail b JOIN binned_means m USING({key_sql})
                      WHERE m.d1_n>0 AND m.d10_n>0
                      GROUP BY {','.join(f'b.{key}' for key in keys)},{cluster_sql}
                    )
                    SELECT {key_sql},sum(score*score)::DOUBLE component
                    FROM cluster_scores GROUP BY {key_sql}
                    """,
                )
                for row in rows:
                    key = tuple(row[name] for name in keys)
                    variances[key] = variances.get(key, 0.0) + sign * float(row["component"])
        means = {tuple(row[name] for name in keys): row for row in _rows(con, "SELECT * FROM binned_means")}
        sports = SPORTS if by_sport else ("all",)
        for sport in sports:
            for time_bin in range(1, 11):
                key = (sport, time_bin) if by_sport else (time_bin,)
                row = means.get(key)
                d1_n = int(row["d1_n"]) if row else 0
                d10_n = int(row["d10_n"]) if row else 0
                suppressed = d1_n < MIN_N or d10_n < MIN_N
                values: list[Any] = [None] * 6
                if row and not suppressed:
                    d1 = float(row["d1_mean"])
                    d10 = float(row["d10_mean"])
                    spread = d10 - d1
                    standard_error = math.sqrt(max(variances.get(key, 0.0), 0.0))
                    values = [d1, d10, spread, standard_error,
                              spread - 1.96 * standard_error, spread + 1.96 * standard_error]
                output.append(
                    (scope, sport, weighting, time_bin, (time_bin - 1) / 10.0,
                     time_bin / 10.0, d1_n, d10_n,
                     int(row["d1_events"]) if row else 0,
                     int(row["d10_events"]) if row else 0,
                     *values, suppressed,
                     f"withheld_tail_n_lt_{MIN_N}" if suppressed else "reported")
                )
    return output


def _tail_supported(
    con: duckdb.DuckDBPyConnection,
    sport: str | None,
    sample: str,
    time_column: str,
    *,
    piecewise: bool = False,
) -> tuple[bool, str]:
    clause, _, _ = _sample_clause(sample, time_column)
    sport_clause = "" if sport is None else f" AND sport='{sport}'"
    rows = con.execute(
        f"""
        SELECT CASE WHEN {time_column}<0 THEN 'pregame' ELSE 'live' END segment,
               price_decile,count(*)::BIGINT n
        FROM weighted_observations
        WHERE price_decile IN (1,10) AND {clause}{sport_clause}
        GROUP BY 1,2
        """
    ).fetchall()
    counts = {(row[0], int(row[1])): int(row[2]) for row in rows}
    if piecewise:
        required = (("pregame", 1), ("pregame", 10), ("live", 1), ("live", 10))
    elif sample == "live_only":
        required = (("live", 1), ("live", 10))
    else:
        required = tuple((segment, tail) for segment in ("pregame", "live") for tail in (1, 10))
    thin = [f"{segment}-D{tail}={counts.get((segment, tail), 0)}" for segment, tail in required if counts.get((segment, tail), 0) < MIN_N]
    return not thin, "reported" if not thin else "withheld_support:" + ",".join(thin)


def _append_fit(
    coefficient_rows: list[tuple[Any, ...]],
    model_rows: list[tuple[Any, ...]],
    estimand_rows: list[tuple[Any, ...]],
    *,
    model_id: str,
    family: str,
    scope: str,
    sport: str,
    sample: str,
    time_normalization: str,
    window_low: float,
    window_high: float,
    weighting: str,
    adjustment: str,
    features: Sequence[tuple[str, str]],
    fit: FitResult | None,
    status: str,
    estimand_terms: Sequence[tuple[str, np.ndarray]],
) -> None:
    suppressed = fit is None
    if fit is None:
        model_rows.append(
            (model_id, family, scope, sport, sample, time_normalization, window_low,
             window_high, weighting, adjustment, 0, 0, 0, 0, 0, len(features), 0,
             None, None, True, status)
        )
        for order, (term, _) in enumerate(features, 1):
            coefficient_rows.append(
                (model_id, family, scope, sport, sample, time_normalization, weighting,
                 adjustment, order, term, None, None, None, None, None, None)
            )
        for estimand_name, _ in estimand_terms:
            estimand_rows.append(
                (f"{model_id}:{estimand_name}", model_id, family, scope, sport, sample,
                 time_normalization, weighting, adjustment, estimand_name,
                 None, None, None, None, None, None, 0, 0, True, status)
            )
        return
    model_rows.append(
        (model_id, family, scope, sport, sample, time_normalization, window_low,
         window_high, weighting, adjustment, fit.n_obs, fit.n_events, fit.n_days,
         fit.n_wallets, fit.n_event_clusters, len(features), fit.rank, fit.r_squared,
         fit.condition_number, False, status)
    )
    for order, (term, _) in enumerate(features, 1):
        contrast = np.zeros(len(features), dtype=float)
        contrast[order - 1] = 1.0
        values = _linear_result(fit.beta, fit.covariance, contrast)
        coefficient_rows.append(
            (model_id, family, scope, sport, sample, time_normalization, weighting,
             adjustment, order, term, *values)
        )
    for estimand_name, contrast in estimand_terms:
        values = _linear_result(fit.beta, fit.covariance, contrast)
        estimand_rows.append(
            (f"{model_id}:{estimand_name}", model_id, family, scope, sport, sample,
             time_normalization, weighting, adjustment, estimand_name, *values,
             fit.n_obs, fit.n_events, False, status)
        )


def estimate_flb_decay(
    new_phase: str | Path,
    mlb_phase: str | Path,
    nfl_phase: str | Path,
    nba_phase: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    paths = [Path(value).expanduser().resolve() for value in (new_phase, mlb_phase, nfl_phase, nba_phase)]
    if any(not path.is_file() for path in paths):
        raise FileNotFoundError([str(path) for path in paths if not path.is_file()])
    target = Path(run_dir).expanduser().resolve()
    coefficient_rows: list[tuple[Any, ...]] = []
    model_rows: list[tuple[Any, ...]] = []
    estimand_rows: list[tuple[Any, ...]] = []
    with fresh_run(target, paths) as staging:
        con = duckdb.connect()
        try:
            _create_observations(con, *paths)
            support_rows = _support_rows(con)
            duration_rows = [
                (row["sport"], int(row["event_count"]), float(row["median_duration_seconds"]),
                 float(row["median_duration_seconds"]) / 60.0)
                for row in _rows(con, "SELECT * FROM duration_reference ORDER BY sport")
            ]

            # Sport-specific tail models: primary, live-only, wider-window, fixed-duration, piecewise.
            sport_variants = (
                ("unified", "realized_time", "realized_duration", False),
                ("live_only", "realized_time", "realized_duration", False),
                ("wider_pregame", "realized_time", "realized_duration", False),
                ("unified", "fixed_time", "sport_median_duration", False),
                ("unified", "realized_time", "realized_duration", True),
            )
            for sport in SPORTS:
                for sample, time_column, time_name, piecewise in sport_variants:
                    family = "tail_piecewise" if piecewise else "tail_linear"
                    model_id = f"sport_{sport}_{family}_{sample}_{time_name}"
                    clause, low, high = _sample_clause(sample, time_column)
                    features = _piecewise_features(time_column) if piecewise else _tail_features(time_column)
                    supported, status = _tail_supported(
                        con, sport, sample, time_column, piecewise=piecewise
                    )
                    fit = None
                    if supported:
                        fit = _fit_ols(
                            con,
                            f"sport='{sport}' AND price_decile IN (1,10) AND {clause}",
                            features,
                            "1.0",
                        )
                    estimands: list[tuple[str, np.ndarray]] = []
                    if piecewise:
                        for label, index in (("pregame_tail_spread_change", 4), ("live_tail_spread_change", 5)):
                            contrast = np.zeros(len(features)); contrast[index] = 1.0
                            estimands.append((label, contrast))
                    else:
                        contrast = np.zeros(len(features)); contrast[3] = 1.0
                        estimands.append(("tail_spread_time_slope", contrast))
                        if sample == "unified":
                            estimands.append(("tail_spread_change_window", 2.0 * contrast))
                    _append_fit(
                        coefficient_rows, model_rows, estimand_rows, model_id=model_id,
                        family=family, scope="sport", sport=sport, sample=sample,
                        time_normalization=time_name, window_low=low, window_high=high,
                        weighting="equal_fill", adjustment="none", features=features,
                        fit=fit, status=status, estimand_terms=estimands,
                    )

            # Sport-specific continuous-price model for the primary unified window.
            for sport in SPORTS:
                features = _continuous_features("realized_time")
                fit = _fit_ols(
                    con,
                    f"sport='{sport}' AND realized_time>=-1 AND realized_time<=1",
                    features,
                    "1.0",
                )
                contrast = np.zeros(len(features)); contrast[3] = 1.0
                _append_fit(
                    coefficient_rows, model_rows, estimand_rows,
                    model_id=f"sport_{sport}_continuous_unified_realized_duration",
                    family="continuous_price", scope="sport", sport=sport,
                    sample="unified", time_normalization="realized_duration",
                    window_low=-1.0, window_high=1.0, weighting="equal_fill",
                    adjustment="none", features=features, fit=fit, status="reported",
                    estimand_terms=(("price_gradient_time_slope", contrast),),
                )

            # Pooled tail variants.  The fully interacted model identifies the literal
            # equal-weight average of sport-specific slopes with joint clustered inference.
            pooled_variants = (
                ("unified", "realized_time", "realized_duration", "none", "1.0", "equal_fill"),
                ("unified", "realized_time", "realized_duration", "sport_intercepts", "1.0", "equal_fill"),
                ("unified", "realized_time", "realized_duration", "sport_composition", "1.0", "equal_fill"),
                ("unified", "realized_time", "realized_duration", "sport_composition", "equal_sport_sample", "equal_sport"),
                ("unified", "realized_time", "realized_duration", "sport_composition", "usdc", "dollar"),
                ("live_only", "realized_time", "realized_duration", "sport_composition", "1.0", "equal_fill"),
                ("live_only", "realized_time", "realized_duration", "sport_composition", "equal_sport_sample", "equal_sport"),
                ("wider_pregame", "realized_time", "realized_duration", "sport_composition", "1.0", "equal_fill"),
                ("wider_pregame", "realized_time", "realized_duration", "sport_composition", "equal_sport_sample", "equal_sport"),
                ("unified", "fixed_time", "sport_median_duration", "sport_composition", "1.0", "equal_fill"),
                ("unified", "fixed_time", "sport_median_duration", "sport_composition", "equal_sport_sample", "equal_sport"),
            )
            for sample, time_column, time_name, adjustment, weight_expression, weighting in pooled_variants:
                features = _pooled_tail_features(time_column, adjustment)
                clause, low, high = _sample_clause(sample, time_column)
                fit = _fit_ols(
                    con, f"price_decile IN (1,10) AND {clause}", features, weight_expression
                )
                model_id = f"pooled_tail_{sample}_{time_name}_{adjustment}_{weighting}"
                estimands = []
                base_index = next(index for index, item in enumerate(features) if item[0] == "D10 x time")
                contrast = np.zeros(len(features)); contrast[base_index] = 1.0
                estimands.append(("tail_spread_time_slope", contrast))
                if sample == "unified":
                    estimands.append(("tail_spread_change_window", 2.0 * contrast))
                _append_fit(
                    coefficient_rows, model_rows, estimand_rows, model_id=model_id,
                    family="tail_linear", scope="pooled", sport="all", sample=sample,
                    time_normalization=time_name, window_low=low, window_high=high,
                    weighting=weighting, adjustment=adjustment, features=features,
                    fit=fit, status="reported", estimand_terms=estimands,
                )

            # Balanced-support pools enforce the 500-fill floor separately for D1 and
            # D10 on every segment needed by the requested time window.
            balanced_variants = (
                ("unified", "realized_time", "realized_duration"),
                ("live_only", "realized_time", "realized_duration"),
                ("wider_pregame", "realized_time", "realized_duration"),
                ("unified", "fixed_time", "sport_median_duration"),
            )
            for sample, time_column, time_name in balanced_variants:
                active_sports = tuple(
                    sport for sport in SPORTS
                    if _tail_supported(con, sport, sample, time_column)[0]
                )
                if len(active_sports) < 2:
                    raise ValueError(f"Too few supported sports for {sample}/{time_name}: {active_sports}")
                sport_sql = ",".join(f"'{sport}'" for sport in active_sports)
                clause, low, high = _sample_clause(sample, time_column)
                for adjustment, weight_expression, weighting in (
                    ("sport_composition", "1.0", "equal_fill"),
                    ("sport_composition", "equal_sport_sample", "equal_sport"),
                    ("fully_interacted", "1.0", "equal_fill"),
                ):
                    features = _pooled_tail_features(time_column, adjustment, active_sports)
                    fit = _fit_ols(
                        con,
                        f"sport IN ({sport_sql}) AND price_decile IN (1,10) AND {clause}",
                        features,
                        weight_expression,
                    )
                    base_index = next(index for index, item in enumerate(features) if item[0] == "D10 x time")
                    if adjustment == "fully_interacted":
                        contrast = _mean_sport_slope_contrast(features, active_sports)
                        estimands = (("equal_weight_mean_sport_tail_slope", contrast),)
                    else:
                        contrast = np.zeros(len(features)); contrast[base_index] = 1.0
                        estimands = (("tail_spread_time_slope", contrast),)
                    _append_fit(
                        coefficient_rows, model_rows, estimand_rows,
                        model_id=f"pooled_supported_tail_{sample}_{time_name}_{adjustment}_{weighting}",
                        family="tail_linear", scope="pooled_supported",
                        sport="+".join(active_sports), sample=sample,
                        time_normalization=time_name, window_low=low, window_high=high,
                        weighting=weighting, adjustment=adjustment, features=features,
                        fit=fit, status="reported", estimand_terms=estimands,
                    )

            # Piecewise pooled models use the same per-sport tail support gate as the
            # unified linear fit, avoiding weakly identified sport interactions.
            piecewise_sports = tuple(
                sport for sport in SPORTS
                if _tail_supported(
                    con, sport, "unified", "realized_time", piecewise=True
                )[0]
            )
            if len(piecewise_sports) < 2:
                raise ValueError(f"Too few supported sports for piecewise fit: {piecewise_sports}")
            piecewise_sport_sql = ",".join(f"'{sport}'" for sport in piecewise_sports)
            for weight_expression, weighting in (("1.0", "equal_fill"), ("equal_sport_sample", "equal_sport")):
                features = _pooled_piecewise_features("realized_time", piecewise_sports)
                fit = _fit_ols(
                    con,
                    f"sport IN ({piecewise_sport_sql}) AND price_decile IN (1,10) "
                    "AND realized_time>=-1 AND realized_time<=1",
                    features,
                    weight_expression,
                )
                estimands = []
                for label, term in (
                    ("pregame_tail_spread_change", "D10 x pregame time"),
                    ("live_tail_spread_change", "D10 x live time"),
                ):
                    contrast = np.zeros(len(features))
                    contrast[next(index for index, item in enumerate(features) if item[0] == term)] = 1.0
                    estimands.append((label, contrast))
                _append_fit(
                    coefficient_rows, model_rows, estimand_rows,
                    model_id=f"pooled_supported_tail_piecewise_realized_duration_sport_composition_{weighting}",
                    family="tail_piecewise", scope="pooled_supported",
                    sport="+".join(piecewise_sports), sample="unified",
                    time_normalization="realized_duration", window_low=-1.0, window_high=1.0,
                    weighting=weighting, adjustment="sport_composition", features=features,
                    fit=fit, status="reported", estimand_terms=estimands,
                )

            # Pooled continuous-price versions for the same contribution comparison.
            for adjustment, weight_expression, weighting in (
                ("none", "1.0", "equal_fill"),
                ("sport_composition", "1.0", "equal_fill"),
                ("sport_composition", "equal_sport_sample", "equal_sport"),
            ):
                features = _pooled_continuous_features("realized_time", adjustment)
                fit = _fit_ols(
                    con, "realized_time>=-1 AND realized_time<=1", features, weight_expression
                )
                contrast = np.zeros(len(features))
                contrast[next(index for index, item in enumerate(features) if item[0] == "Price x time")] = 1.0
                _append_fit(
                    coefficient_rows, model_rows, estimand_rows,
                    model_id=f"pooled_continuous_unified_realized_duration_{adjustment}_{weighting}",
                    family="continuous_price", scope="pooled", sport="all", sample="unified",
                    time_normalization="realized_duration", window_low=-1.0, window_high=1.0,
                    weighting=weighting, adjustment=adjustment, features=features,
                    fit=fit, status="reported",
                    estimand_terms=(("price_gradient_time_slope", contrast),),
                )
            observation_counts = dict(
                con.execute("SELECT sport,count(*)::BIGINT FROM observations GROUP BY 1 ORDER BY 1").fetchall()
            )
            time_bin_rows = _time_bin_rows(con)
        finally:
            con.close()

        write_parquet(staging / "coefficients.parquet", COEFFICIENT_SCHEMA, coefficient_rows, ("model_id", "term_order"))
        write_parquet(staging / "model_summary.parquet", MODEL_SCHEMA, model_rows, ("model_id",))
        write_parquet(staging / "estimands.parquet", ESTIMAND_SCHEMA, estimand_rows, ("estimand_id",))
        write_parquet(staging / "support.parquet", SUPPORT_SCHEMA, support_rows, ("sample", "time_normalization", "sport", "segment", "tail"))
        write_parquet(staging / "duration_reference.parquet", DURATION_SCHEMA, duration_rows, ("sport",))
        write_parquet(staging / "time_bin_spreads.parquet", TIME_BIN_SCHEMA, time_bin_rows, ("scope", "sport", "weighting", "time_bin"))
        manifest = {
            "schema_version": 1,
            "stage": "multisport_flb_time_regressions_v1",
            "estimand": "change in bought-contract D10-minus-D1 calibration spread over normalized event time",
            "sports": list(SPORTS),
            "observation_unit": "eligible BUY fill",
            "calibration": "eventual bought-contract outcome minus purchase price",
            "time": {
                "primary": "(exact trade timestamp - event start) / realized event duration",
                "primary_window": [-1.0, 1.0],
                "live_only_window": [0.0, 1.0],
                "wider_pregame_window": [-2.0, 1.0],
                "fixed_duration_sensitivity": "sport-specific median realized duration",
            },
            "support_floor": MIN_N,
            "uncertainty": "Cameron-Gelbach-Miller three-way clustered by UTC trade day, buyer wallet, and event",
            "observation_counts": observation_counts,
            "inputs": {f"input_{index:02d}": fingerprint(path) for index, path in enumerate(paths, 1)},
            "outputs": {
                name: artifact_fingerprint(staging / name)
                for name in (
                    "coefficients.parquet", "model_summary.parquet", "estimands.parquet",
                    "support.parquet", "duration_reference.parquet", "time_bin_spreads.parquet",
                )
            },
        }
        write_json(staging / "manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("new_phase", "mlb_phase", "nfl_phase", "nba_phase", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    print(json.dumps(estimate_flb_decay(**vars(parse_args(argv))), sort_keys=True))


if __name__ == "__main__":
    main()
