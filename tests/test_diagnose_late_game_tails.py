from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from analysis.sports_game_dynamics.diagnose_late_game_tails import (
    LateGameDiagnosticError,
    run_diagnostic,
)


def _copy_sql(path: Path, select_sql: str) -> None:
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{str(path).replace(chr(39), chr(39)*2)}' "
            "(FORMAT PARQUET)"
        )
    finally:
        con.close()


def _mlb_rows(path: Path) -> None:
    rows = [
        ("m1", 1, "2026-06-01", "Away A", "Home A", "w1", "home", .02, 10., 1, .98, 1050, "a1", 1),
        ("m1", 1, "2026-06-01", "Away A", "Home A", "w1", "home", .03, 20., 1, .97, 1100, "a2", 2),
        ("m2", 2, "2026-06-02", "Away B", "Home B", "w2", "away", .08, 30., 0, -.08, 1200, "a3", 3),
        ("m1", 1, "2026-06-01", "Away A", "Home A", "w1", "away", .95, 40., 0, -.95, 1050, "a4", 4),
        ("m2", 2, "2026-06-02", "Away B", "Home B", "w2", "home", .96, 50., 1, .04, 1150, "a5", 5),
        ("m3", 3, "2026-06-03", "Away C", "Home C", "w3", "home", .94, 60., 1, .06, 1250, "a6", 6),
        ("m4", 4, "2026-06-04", "Away D", "Home D", "w4", "home", .3 + .6, 70., 1, 1-(.3+.6), 1250, "a7", 7),
    ]
    values = ",".join(
        "(" + ",".join(
            [
                repr(market), str(game), repr(date), repr(away), repr(home), repr(wallet),
                repr(side), str(price), str(dollars), str(won), str(error), str(ts),
                repr(tx), str(log_index),
            ]
        ) + ")"
        for market, game, date, away, home, wallet, side, price, dollars, won, error, ts, tx, log_index in rows
    )
    _copy_sql(
        path,
        f"""
        SELECT col0::VARCHAR market_id,col1::BIGINT game_pk,col2::DATE official_date,
               col3::VARCHAR away_team_name,col4::VARCHAR home_team_name,
               col5::VARCHAR proxyWallet,col6::VARCHAR bought_side,col7::DOUBLE price,
               col8::DOUBLE usdc,col9::TINYINT won,col10::DOUBLE calibration_error,
               'innings_7_plus'::VARCHAR phase,true::BOOLEAN analysis_eligible,
               col11::BIGINT "timestamp",col12::VARCHAR transaction_hash,
               col13::INTEGER log_index,'exchange'::VARCHAR exchange_address,
               to_timestamp(1300)::TIMESTAMPTZ actual_end_utc,
               to_timestamp(1000)::TIMESTAMPTZ inning_7_start_utc
        FROM (VALUES {values})
        """,
    )


def _shared_rows(path: Path, sport: str) -> None:
    prefix = sport.lower()
    rows = [
        ("g1", "home-token", .02, .02, 1, .98, 1050, "w1"),
        ("g1", "away-token", .97, .03, 1, .97, 1150, "w1"),
        ("g2", "home-token", .08, .08, 0, -.08, 1250, "w2"),
        ("g1", "home-token", .95, .95, 0, -.95, 1050, "w1"),
        ("g2", "away-token", .04, .96, 1, .04, 1150, "w2"),
        ("g3", "home-token", .94, .94, 1, .06, 1250, "w3"),
        ("g4", "home-token", .3 + .6, .3 + .6, 1, 1-(.3+.6), 1250, "w4"),
    ]
    values = ",".join(
        f"({game!r},{token!r},{price},{home_p},{home_won},{error},{ts},{wallet!r},{(prefix+str(i))!r},{i})"
        for i, (game, token, price, home_p, home_won, error, ts, wallet) in enumerate(rows, 1)
    )
    _copy_sql(
        path,
        f"""
        SELECT {sport!r}::VARCHAR sport,('market-'||col0)::VARCHAR market_id,
               col0::VARCHAR game_id,DATE '2026-06-01'+(right(col0,1)::INTEGER-1) official_date,
               col1::VARCHAR token_id,'home-token'::VARCHAR home_token_id,
               col7::VARCHAR proxyWallet,col2::DOUBLE price,col3::DOUBLE home_probability,
               col4::BOOLEAN home_won,col5::DOUBLE calibration_error,
               10.0::DOUBLE usdc,'quarter_4_plus'::VARCHAR phase,true::BOOLEAN analysis_eligible,
               col6::BIGINT "timestamp",col8::VARCHAR transaction_hash,
               col9::INTEGER log_index,'exchange'::VARCHAR exchange_address,
               to_timestamp(1000)::TIMESTAMPTZ period_4_start_utc,
               to_timestamp(1300)::TIMESTAMPTZ actual_end_utc
        FROM (VALUES {values})
        """,
    )


def _eligible_rows(path: Path) -> None:
    _copy_sql(
        path,
        """
        SELECT ('market-g'||i)::VARCHAR market_id,('g'||i)::VARCHAR game_id,
               DATE '2026-06-01'+(i-1)::INTEGER official_date,
               ('Away '||i)::VARCHAR away_team_name,('Home '||i)::VARCHAR home_team_name
        FROM range(1,5) t(i)
        """,
    )


def _inputs(tmp_path: Path):
    phases = {sport: tmp_path / f"{sport.lower()}_phases.parquet" for sport in ("MLB", "NFL", "NBA")}
    eligible = {sport: tmp_path / f"{sport.lower()}_eligible.parquet" for sport in ("NFL", "NBA")}
    _mlb_rows(phases["MLB"])
    _shared_rows(phases["NFL"], "NFL")
    _shared_rows(phases["NBA"], "NBA")
    _eligible_rows(eligible["NFL"])
    _eligible_rows(eligible["NBA"])
    return phases, eligible


def test_diagnostic_outputs_and_definitions(tmp_path: Path) -> None:
    phases, eligible = _inputs(tmp_path)
    run_dir = tmp_path / "diagnostic"
    manifest = run_diagnostic(phases, run_dir, eligible, command=["synthetic-test"])

    assert manifest["counts"]["tail_overview"] == 6
    assert manifest["counts"]["timing_thirds"] == 18
    assert manifest["counts"]["leave_top_k_games"] == 18
    assert manifest["counts"]["outcome_decomposition"] == 12
    assert manifest["environment"]["duckdb_version"] == duckdb.__version__
    assert manifest["code"]["script"]["path"].endswith("diagnose_late_game_tails.py")
    assert manifest["definitions"]["probability_and_outcome"]["MLB"].startswith("raw bought")
    assert manifest["definitions"]["probability_and_outcome"]["NFL"].startswith("home-win")
    for name, details in manifest["outputs"].items():
        output = run_dir / details["path"]
        assert output.is_file(), name
        assert hashlib.sha256(output.read_bytes()).hexdigest() == details["sha256"]

    con = duckdb.connect()
    try:
        stored_edge = con.execute(
            f"""
            SELECT home_probability,home_probability<0.9,
                   least(floor(home_probability*10)::INTEGER,9)+1
            FROM read_parquet('{phases['NFL']}') WHERE game_id='g4'
            """
        ).fetchone()
        assert stored_edge[0] == pytest.approx(.3 + .6)
        assert stored_edge[1:] == (True, 10)

        overview = con.execute(
            f"""
            SELECT probability_definition,equal_fill_calibration,equal_game_calibration
            FROM read_parquet('{run_dir / 'tail_overview.parquet'}')
            WHERE sport='MLB' AND tail='D1'
            """
        ).fetchone()
        assert overview[0] == "bought_contract_probability"
        assert overview[1] == pytest.approx((.98 + .97 - .08) / 3)
        assert overview[2] == pytest.approx(((.98 + .97) / 2 - .08) / 2)

        nfl_bands = con.execute(
            f"""
            SELECT probability_band_pp,raw_price_band_pp,bought_side
            FROM read_parquet('{run_dir / 'probability_band_concentration.parquet'}')
            WHERE sport='NFL' AND tail='D1' ORDER BY raw_price_band_pp
            """
        ).fetchall()
        assert (2, 2, "home") in nfl_bands
        assert (3, 97, "away") in nfl_bands

        edge_counts = con.execute(
            f"""
            SELECT sport,trade_count
            FROM read_parquet('{run_dir / 'tail_overview.parquet'}')
            WHERE tail='D10' ORDER BY sport
            """
        ).fetchall()
        assert edge_counts == [("MLB", 4), ("NBA", 4), ("NFL", 4)]
        edge_bands = con.execute(
            f"""
            SELECT sport,probability_band_pp,raw_price_band_pp,trade_count
            FROM read_parquet('{run_dir / 'probability_band_concentration.parquet'}')
            WHERE tail='D10' AND probability_band_pp=90 ORDER BY sport
            """
        ).fetchall()
        assert edge_bands == [("MLB", 90, 90, 1), ("NBA", 90, 90, 1), ("NFL", 90, 90, 1)]

        top_game = con.execute(
            f"""
            SELECT matchup,absolute_fill_contribution_rank
            FROM read_parquet('{run_dir / 'game_contributions.parquet'}')
            WHERE sport='NBA' AND tail='D1' ORDER BY absolute_fill_contribution_rank LIMIT 1
            """
        ).fetchone()
        assert top_game == ("Away 1 at Home 1", 1)

        thirds = con.execute(
            f"""
            SELECT timing_third,trade_count
            FROM read_parquet('{run_dir / 'timing_thirds.parquet'}')
            WHERE sport='MLB' AND tail='D1' ORDER BY timing_third
            """
        ).fetchall()
        assert thirds == [(1, 1), (2, 1), (3, 1)]

        outcome_sum = con.execute(
            f"""
            SELECT sum(contribution_to_equal_fill_mean)
            FROM read_parquet('{run_dir / 'outcome_decomposition.parquet'}')
            WHERE sport='MLB' AND tail='D1'
            """
        ).fetchone()[0]
        assert outcome_sum == pytest.approx(overview[1])

        leave_one = con.execute(
            f"""
            SELECT removed_games,remaining_games,remaining_equal_fill_calibration
            FROM read_parquet('{run_dir / 'leave_top_k_games.parquet'}')
            WHERE sport='MLB' AND tail='D1' AND k=1
            """
        ).fetchone()
        assert leave_one == pytest.approx((1, 1, -.08))
    finally:
        con.close()

    saved = json.loads((run_dir / "manifest.json").read_text())
    assert saved["completion_status"] == "complete"
    with pytest.raises(FileExistsError, match="Immutable run directory"):
        run_diagnostic(phases, run_dir, eligible)


def test_mapping_is_required_when_phase_names_are_absent(tmp_path: Path) -> None:
    phases, eligible = _inputs(tmp_path)
    with pytest.raises(LateGameDiagnosticError, match="provide --eligible NFL=PATH"):
        run_diagnostic(phases, tmp_path / "missing_mapping", {"NBA": eligible["NBA"]})
