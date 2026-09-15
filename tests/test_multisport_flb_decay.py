from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import numpy as np

from analysis.multisport_game_dynamics.estimate_flb_decay import (
    SPORTS,
    _create_observations,
    _fit_ols,
    _linear_result,
    _mean_sport_slope_contrast,
    _pooled_tail_features,
    _tail_features,
)


def _write_parquet(
    path: Path,
    schema: tuple[tuple[str, str], ...],
    rows: list[tuple[object, ...]],
) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE artifact(" + ",".join(f'\"{name}\" {kind}' for name, kind in schema) + ")"
        )
        con.executemany(
            "INSERT INTO artifact VALUES (" + ",".join("?" for _ in schema) + ")", rows
        )
        destination = str(path.resolve()).replace("'", "''")
        con.execute(f"COPY artifact TO '{destination}' (FORMAT PARQUET)")
    finally:
        con.close()


def test_clustered_ols_recovers_tail_time_interaction() -> None:
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE weighted_observations(
              calibration_error DOUBLE,equal_sport_weight DOUBLE,event_cluster VARCHAR,
              trade_day DATE,proxyWallet VARCHAR,price_decile INTEGER,
              realized_time DOUBLE,price DOUBLE,usdc DOUBLE,sport VARCHAR
            )
            """
        )
        rows = []
        for event in range(24):
            for tail, decile in ((0, 1), (1, 10)):
                for time in (-1.0, -0.5, 0.0, 0.5, 1.0):
                    # y = .01 + .02 H + .03 T - .04 H*T exactly.
                    outcome = 0.01 + 0.02 * tail + 0.03 * time - 0.04 * tail * time
                    rows.append(
                        (
                            outcome,
                            1.0,
                            f"event-{event}",
                            date(2025, 1, event % 20 + 1),
                            f"wallet-{event % 7}",
                            decile,
                            time,
                            0.05 if decile == 1 else 0.95,
                            1.0,
                            "mlb",
                        )
                    )
        con.executemany("INSERT INTO weighted_observations VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        features = _tail_features("realized_time")
        fit = _fit_ols(
            con,
            "price_decile IN (1,10) AND realized_time>=-1 AND realized_time<=1",
            features,
            "1.0",
        )
        assert np.allclose(fit.beta, [0.01, 0.02, 0.03, -0.04], atol=1e-12)
        assert fit.n_obs == 240
        assert fit.n_events == 24
        assert fit.rank == 4
        assert fit.r_squared == 1.0
        contrast = np.array([0.0, 0.0, 0.0, 1.0])
        estimate, *_ = _linear_result(fit.beta, fit.covariance, contrast)
        assert np.isclose(estimate, -0.04, atol=1e-12)
    finally:
        con.close()


def test_equal_sport_weights_are_defined_on_final_fit_sample() -> None:
    con = duckdb.connect()
    try:
        con.execute(
            """
            CREATE TABLE weighted_observations(
              calibration_error DOUBLE,event_cluster VARCHAR,trade_day DATE,
              proxyWallet VARCHAR,price_decile INTEGER,realized_time DOUBLE,
              price DOUBLE,usdc DOUBLE,sport VARCHAR
            )
            """
        )
        rows = [
            (0.0, "a-1", date(2025, 1, 1), "wa", 1, 0.5, 0.05, 1.0, "a"),
            (1.0, "b-1", date(2025, 1, 2), "wb1", 1, 0.5, 0.05, 1.0, "b"),
            (1.0, "b-2", date(2025, 1, 3), "wb2", 1, 0.5, 0.05, 1.0, "b"),
            (1.0, "b-3", date(2025, 1, 4), "wb3", 1, 0.5, 0.05, 1.0, "b"),
        ]
        con.executemany("INSERT INTO weighted_observations VALUES (?,?,?,?,?,?,?,?,?)", rows)
        equal_fill = _fit_ols(con, "TRUE", [("Intercept", "1")], "1.0")
        equal_sport = _fit_ols(
            con, "TRUE", [("Intercept", "1")], "equal_sport_sample"
        )
        assert np.isclose(equal_fill.beta[0], 0.75)
        assert np.isclose(equal_sport.beta[0], 0.5)
    finally:
        con.close()


def test_mean_sport_slope_contrast_averages_level_slopes() -> None:
    sports = ("mlb", "nba", "nfl")
    features = _pooled_tail_features("realized_time", "fully_interacted", sports)
    contrast = _mean_sport_slope_contrast(features, sports)
    beta = np.zeros(len(features))
    beta[next(i for i, item in enumerate(features) if item[0] == "D10 x time")] = 0.06
    beta[next(i for i, item in enumerate(features) if item[0] == "NBA x D10 x time")] = -0.03
    beta[next(i for i, item in enumerate(features) if item[0] == "NFL x D10 x time")] = 0.06
    # Sport slopes are 0.06, 0.03, and 0.12; their literal mean is 0.07.
    assert np.isclose(contrast @ beta, 0.07)


def test_observation_normalization_uses_exact_bought_contract_outcome(tmp_path: Path) -> None:
    start = datetime(2025, 1, 2, 20, tzinfo=timezone.utc)
    end = datetime(2025, 1, 3, 0, tzinfo=timezone.utc)
    timestamp = int(start.timestamp())
    trade_day = date(2025, 1, 2)
    new_phase = tmp_path / "new.parquet"
    new_schema = (
        ("sport", "VARCHAR"), ("event_slug", "VARCHAR"), ("market_id", "VARCHAR"),
        ("market_date", "DATE"), ("timestamp", "BIGINT"), ("price", "DOUBLE"),
        ("won", "BOOLEAN"), ("usdc", "DOUBLE"), ("proxyWallet", "VARCHAR"),
        ("trade_day", "DATE"), ("actual_start_utc", "TIMESTAMPTZ"),
        ("actual_end_utc", "TIMESTAMPTZ"),
    )
    _write_parquet(
        new_phase,
        new_schema,
        [
            (sport, f"{sport}-event", f"{sport}-market", trade_day, timestamp,
             0.95, True, 1.0, f"{sport}-wallet", trade_day, start, end)
            for sport in SPORTS[3:]
        ],
    )
    mlb_phase = tmp_path / "mlb.parquet"
    _write_parquet(
        mlb_phase,
        (("game_pk", "BIGINT"), ("market_id", "VARCHAR"), ("official_date", "DATE"),
         ("timestamp", "BIGINT"), ("price", "DOUBLE"), ("won", "BOOLEAN"),
         ("usdc", "DOUBLE"), ("proxyWallet", "VARCHAR"), ("trade_day_utc", "DATE"),
         ("actual_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ"),
         ("analysis_eligible", "BOOLEAN")),
        [(1, "mlb-market", trade_day, timestamp, 0.05, False, 1.0, "mlb-wallet",
          trade_day, start, end, True)],
    )
    standard_schema = (
        ("game_id", "VARCHAR"), ("market_id", "VARCHAR"), ("official_date", "DATE"),
        ("timestamp", "BIGINT"), ("price", "DOUBLE"), ("token_id", "VARCHAR"),
        ("winning_token_id", "VARCHAR"), ("usdc", "DOUBLE"),
        ("proxyWallet", "VARCHAR"), ("trade_day", "DATE"),
        ("actual_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ"),
        ("analysis_eligible", "BOOLEAN"),
    )
    nfl_phase = tmp_path / "nfl.parquet"
    nba_phase = tmp_path / "nba.parquet"
    for sport, path in (("nfl", nfl_phase), ("nba", nba_phase)):
        # The losing bought token verifies that the common outcome is token-normalized,
        # not inherited from a home-normalized calibration column.
        _write_parquet(
            path,
            standard_schema,
            [(f"{sport}-event", f"{sport}-market", trade_day, timestamp, 0.95,
              "away-token", "home-token", 1.0, f"{sport}-wallet", trade_day,
              start, end, True)],
        )
    con = duckdb.connect()
    try:
        _create_observations(con, new_phase, mlb_phase, nfl_phase, nba_phase)
        assert con.execute("SELECT count(*) FROM observations").fetchone()[0] == 9
        assert con.execute(
            "SELECT calibration_error FROM observations WHERE sport='nfl'"
        ).fetchone()[0] == -0.95
        assert con.execute(
            "SELECT realized_time,fixed_time FROM observations WHERE sport='mlb'"
        ).fetchone() == (0.0, 0.0)
    finally:
        con.close()
