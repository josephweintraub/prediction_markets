from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from analysis.multisport_game_dynamics.contracts import SPORT_CONFIGS
from analysis.multisport_game_dynamics.estimate_combined import (
    LEGACY_PHASES,
    MIN_CELL_N,
    REPORT_SPORTS,
    RETAINED_NEW_SPORTS,
    estimate_combined,
)


def _write_parquet(
    path: Path,
    schema: tuple[tuple[str, str], ...],
    rows: list[tuple[object, ...]],
) -> None:
    con = duckdb.connect()
    try:
        columns = ",".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE artifact({columns})")
        placeholders = ",".join("?" for _ in schema)
        con.executemany(f"INSERT INTO artifact VALUES ({placeholders})", rows)
        destination = str(path.resolve()).replace("'", "''")
        con.execute(
            f"COPY artifact TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def _count(path: Path) -> int:
    con = duckdb.connect()
    try:
        source = str(path.resolve()).replace("'", "''")
        return int(con.execute(f"SELECT count(*) FROM read_parquet('{source}')").fetchone()[0])
    finally:
        con.close()


def _distinct_count(path: Path, columns: str) -> int:
    con = duckdb.connect()
    try:
        source = str(path.resolve()).replace("'", "''")
        query = f"SELECT count(*) FROM (SELECT DISTINCT {columns} FROM read_parquet('{source}'))"
        return int(con.execute(query).fetchone()[0])
    finally:
        con.close()


def _query(path: Path, sql: str) -> list[tuple[object, ...]]:
    con = duckdb.connect()
    try:
        source = str(path.resolve()).replace("'", "''")
        return con.execute(sql.format(source=source)).fetchall()
    finally:
        con.close()


def test_combined_estimator_writes_complete_fixed_grids(tmp_path: Path) -> None:
    """Exercise every estimator input path with the smallest valid support."""

    day = date(2025, 1, 1)
    timestamp = 1_735_776_000
    new_run = tmp_path / "new_trades"
    new_run.mkdir()
    _write_parquet(
        new_run / "phase_trades.parquet",
        (
            ("sport", "VARCHAR"),
            ("event_slug", "VARCHAR"),
            ("market_id", "VARCHAR"),
            ("market_date", "DATE"),
            ("phase", "VARCHAR"),
            ("phase_order", "INTEGER"),
            ("price", "DOUBLE"),
            ("won", "BOOLEAN"),
            ("calibration_error", "DOUBLE"),
            ("usdc", "DOUBLE"),
            ("proxyWallet", "VARCHAR"),
            ("trade_day", "DATE"),
        ),
        [
            (
                "nhl",f"nhl-d1-event-{index}",f"nhl-d1-market-{index}",day,
                "pregame",1,0.05,False,-0.05,1.0,"new-wallet",day,
            )
            for index in range(500)
        ]
        + [
            (
                "nhl",f"nhl-d10-event-{index}",f"nhl-d10-market-{index}",day,
                "pregame",1,0.95,True,0.05,1.0,"new-wallet",day,
            )
            for index in range(499)
        ]
        + [
            (sport,f"{sport}-event",f"{sport}-market",day,"pregame",1,
             0.05,False,-0.05,1.0,"excluded-wallet",day)
            for sport in ("wta","ufc")
        ],
    )
    _write_parquet(
        new_run / "closing_lines.parquet",
        (
            ("sport", "VARCHAR"),
            ("event_slug", "VARCHAR"),
            ("market_id", "VARCHAR"),
            ("market_date", "DATE"),
            ("close_sample", "VARCHAR"),
            ("price", "DOUBLE"),
            ("won", "BOOLEAN"),
            ("usdc", "DOUBLE"),
            ("proxyWallet", "VARCHAR"),
            ("timestamp", "BIGINT"),
            ("calibration_error", "DOUBLE"),
        ),
        [
            (
                "nhl",f"nhl-d1-event-{index}",f"nhl-d1-market-{index}",day,sample,
                0.05,False,1.0,"new-wallet",timestamp,-0.05,
            )
            for sample in ("all_trades","filtered_trades")
            for index in range(500)
        ]
        + [
            (
                "nhl",f"nhl-d10-event-{index}",f"nhl-d10-market-{index}",day,sample,
                0.95,True,1.0,"new-wallet",timestamp,0.05,
            )
            for sample in ("all_trades","filtered_trades")
            for index in range(499)
        ]
        + [
            (sport,f"{sport}-event",f"{sport}-market",day,sample,
             0.05,False,1.0,"excluded-wallet",timestamp,-0.05)
            for sport in ("wta","ufc")
            for sample in ("all_trades","filtered_trades")
        ],
    )

    mlb_phase = tmp_path / "mlb_phase.parquet"
    _write_parquet(
        mlb_phase,
        (
            ("game_pk", "BIGINT"),
            ("market_id", "VARCHAR"),
            ("official_date", "DATE"),
            ("phase", "VARCHAR"),
            ("price", "DOUBLE"),
            ("won", "BOOLEAN"),
            ("calibration_error", "DOUBLE"),
            ("usdc", "DOUBLE"),
            ("proxyWallet", "VARCHAR"),
            ("trade_day_utc", "DATE"),
            ("analysis_eligible", "BOOLEAN"),
        ),
        [(1, "mlb-market", day, "pregame", 0.05, False, -0.05,
          1.0, "mlb-wallet", day, True)],
    )
    mlb_closes = tmp_path / "mlb_closes.parquet"
    _write_parquet(
        mlb_closes,
        (
            ("game_pk", "BIGINT"),
            ("market_id", "VARCHAR"),
            ("official_date", "DATE"),
            ("winning_token_id", "VARCHAR"),
            ("primary_has_close", "BOOLEAN"),
            ("primary_price", "DOUBLE"),
            ("primary_token_id", "VARCHAR"),
            ("primary_usdc", "DOUBLE"),
            ("primary_buyer", "VARCHAR"),
            ("primary_close_timestamp", "BIGINT"),
            ("sensitivity_has_close", "BOOLEAN"),
            ("sensitivity_price", "DOUBLE"),
            ("sensitivity_token_id", "VARCHAR"),
            ("sensitivity_usdc", "DOUBLE"),
            ("sensitivity_buyer", "VARCHAR"),
            ("sensitivity_close_timestamp", "BIGINT"),
        ),
        [(1, "mlb-market", day, "mlb-loser", True, 0.05, "mlb-loser",
          1.0, "mlb-wallet", timestamp, True, 0.05, "mlb-loser",
          1.0, "mlb-wallet", timestamp)],
    )

    standard_phase_schema = (
        ("sport", "VARCHAR"),
        ("game_id", "VARCHAR"),
        ("market_id", "VARCHAR"),
        ("official_date", "DATE"),
        ("phase", "VARCHAR"),
        ("price", "DOUBLE"),
        ("token_id", "VARCHAR"),
        ("winning_token_id", "VARCHAR"),
        ("usdc", "DOUBLE"),
        ("proxyWallet", "VARCHAR"),
        ("trade_day", "DATE"),
        ("analysis_eligible", "BOOLEAN"),
    )
    standard_close_schema = (
        ("game_id", "VARCHAR"),
        ("market_id", "VARCHAR"),
        ("official_date", "DATE"),
        ("primary_has_close", "BOOLEAN"),
        ("primary_transaction_hash", "VARCHAR"),
        ("primary_log_index", "INTEGER"),
        ("primary_exchange_address", "VARCHAR"),
        ("sensitivity_has_close", "BOOLEAN"),
        ("sensitivity_transaction_hash", "VARCHAR"),
        ("sensitivity_log_index", "INTEGER"),
        ("sensitivity_exchange_address", "VARCHAR"),
    )
    standard_exact_schema = (
        ("transaction_hash", "VARCHAR"),
        ("log_index", "INTEGER"),
        ("exchange_address", "VARCHAR"),
        ("price", "DOUBLE"),
        ("token_id", "VARCHAR"),
        ("usdc", "DOUBLE"),
        ("proxyWallet", "VARCHAR"),
        ("timestamp", "BIGINT"),
    )
    standard_eligible_schema = (
        ("market_id", "VARCHAR"),
        ("winning_token_id", "VARCHAR"),
    )

    standard_paths: dict[str, Path] = {}
    for sport in ("nfl", "nba"):
        market = f"{sport}-market"
        game = f"{sport}-game"
        transaction = f"{sport}-transaction"
        token = f"{sport}-loser"
        phase_path = tmp_path / f"{sport}_phase.parquet"
        close_path = tmp_path / f"{sport}_closes.parquet"
        exact_path = tmp_path / f"{sport}_exact.parquet"
        eligible_path = tmp_path / f"{sport}_eligible.parquet"
        _write_parquet(
            phase_path,
            standard_phase_schema,
            [(sport, game, market, day, "pregame", 0.05, token, token,
              1.0, f"{sport}-wallet", day, True)],
        )
        _write_parquet(
            close_path,
            standard_close_schema,
            [(game, market, day, True, transaction, 1, "exchange",
              True, transaction, 1, "exchange")],
        )
        _write_parquet(
            exact_path,
            standard_exact_schema,
            [(transaction, 1, "exchange", 0.05, token,
              1.0, f"{sport}-wallet", timestamp)],
        )
        _write_parquet(eligible_path, standard_eligible_schema, [(market, token)])
        standard_paths.update(
            {
                f"{sport}_phase": phase_path,
                f"{sport}_closes": close_path,
                f"{sport}_exact": exact_path,
                f"{sport}_eligible": eligible_path,
            }
        )

    output = tmp_path / "combined"
    manifest = estimate_combined(
        new_run,
        mlb_phase,
        mlb_closes,
        standard_paths["nfl_phase"],
        standard_paths["nfl_closes"],
        standard_paths["nfl_exact"],
        standard_paths["nfl_eligible"],
        standard_paths["nba_phase"],
        standard_paths["nba_closes"],
        standard_paths["nba_exact"],
        standard_paths["nba_eligible"],
        output,
    )

    phase_count = sum(len(phases) for phases in LEGACY_PHASES.values()) + sum(
        len(SPORT_CONFIGS[sport].phases) for sport in RETAINED_NEW_SPORTS
    )
    sport_count = len(REPORT_SPORTS)
    expected_phase_rows = phase_count * 10
    expected_closing_rows = sport_count * 2 * 10
    expected_spread_rows = phase_count + sport_count * 2
    assert (expected_phase_rows, expected_closing_rows, expected_spread_rows) == (380, 180, 56)

    phase_output = output / "phase_calibration.parquet"
    closing_output = output / "closing_calibration.parquet"
    spread_output = output / "flb_spreads.parquet"
    assert _count(phase_output) == expected_phase_rows
    assert _count(closing_output) == expected_closing_rows
    assert _count(spread_output) == expected_spread_rows
    assert _distinct_count(phase_output, "sport,phase,price_decile") == expected_phase_rows
    assert _distinct_count(closing_output, "sport,close_sample,price_decile") == expected_closing_rows
    assert _distinct_count(
        spread_output,
        "analysis_scope,sport,sample,coalesce(phase,'')",
    ) == expected_spread_rows
    assert manifest["counts"]["phase_profile_rows"] == expected_phase_rows
    assert manifest["counts"]["closing_profile_rows"] == expected_closing_rows
    assert manifest["counts"]["tail_rows"] == expected_spread_rows
    assert manifest["included_sports"] == list(REPORT_SPORTS)
    assert manifest["excluded_sports"] == ["wta","ufc"]
    assert manifest["suppression_threshold"] == MIN_CELL_N == 500

    for name in (
        "normalized_phase_trades.parquet",
        "normalized_closing_lines.parquet",
        "phase_calibration.parquet",
        "closing_calibration.parquet",
        "flb_spreads.parquet",
    ):
        assert _query(
            output / name,
            "SELECT count(*) FROM read_parquet('{source}') WHERE sport IN ('wta','ufc')",
        ) == [(0,)]

    phase_boundary = _query(
        phase_output,
        """SELECT trade_count,suppressed,status,mean_calibration
             FROM read_parquet('{source}')
            WHERE sport='nhl' AND phase='pregame' AND price_decile IN (1,10)
            ORDER BY price_decile""",
    )
    assert phase_boundary[0][:3] == (500,False,"reported")
    assert phase_boundary[0][3] == pytest.approx(-0.05)
    assert phase_boundary[1] == (499,True,"suppressed_n_lt_500",None)
    closing_boundary = _query(
        closing_output,
        """SELECT close_count,suppressed,status,mean_calibration
             FROM read_parquet('{source}')
            WHERE sport='nhl' AND close_sample='all_trades' AND price_decile IN (1,10)
            ORDER BY price_decile""",
    )
    assert closing_boundary[0][:3] == (500,False,"reported")
    assert closing_boundary[0][3] == pytest.approx(-0.05)
    assert closing_boundary[1] == (499,True,"suppressed_n_lt_500",None)
    assert _query(
        spread_output,
        """SELECT d1_n,d10_n,suppressed,status,spread_d10_minus_d1
             FROM read_parquet('{source}')
            WHERE analysis_scope='trade_phase' AND sport='nhl' AND phase='pregame'""",
    ) == [(500,499,True,"suppressed_tail_n_lt_500",None)]
