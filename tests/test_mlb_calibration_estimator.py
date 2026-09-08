from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "calibration_heterogeneity"
TEST_DIR = Path(__file__).parent
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(ENGINE_DIR))
sys.path.insert(0, str(TEST_DIR))

from estimate_calibration import (  # noqa: E402
    CalibrationEstimatorError,
    DUAL_CLOSE_TYPES,
    estimate_calibration,
)
from flb_engine import cluster_se_mean  # noqa: E402
from test_mlb_dual_closes import (  # noqa: E402
    _build as build_dual_close_fixture,
    _write_inputs as write_dual_close_fixture,
)


BASE = datetime(2025, 7, 1, 18, 0, tzinfo=timezone.utc)


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    phase_rows: list[dict[str, object]] = []
    close_rows: list[dict[str, object]] = []
    phases = (
        ("pregame", -31, -30),
        ("innings_1_3", 31, 30),
        ("innings_4_6", 1031, 1030),
        ("innings_7_plus", 2031, 2030),
    )
    identity = 0
    for game_index in range(60):
        game_pk = 1000 + game_index
        market_id = f"market-{game_index:02d}"
        start = BASE + timedelta(days=game_index % 6, seconds=game_index * 10_000)
        top4 = start + timedelta(seconds=1000)
        top7 = start + timedelta(seconds=2000)
        end = start + timedelta(seconds=3000)
        official_date = start.date()
        home_won = game_index % 2
        for phase, far_offset, near_offset in phases:
            for kind, offset in (("far", far_offset), ("near", near_offset)):
                identity += 1
                timestamp = int(start.timestamp()) + offset
                price = 0.55
                won = home_won
                phase_rows.append(
                    {
                        "market_id": market_id,
                        "game_pk": game_pk,
                        "official_date": official_date,
                        "proxyWallet": f"wallet-{identity % 13}",
                        "day": timestamp // 86400,
                        "trade_day_utc": datetime.fromtimestamp(
                            timestamp, timezone.utc
                        ).date(),
                        "price": price,
                        "usdc": 2.0 if kind == "far" else 3.0,
                        "won": won,
                        "calibration_error": won - price,
                        "home_won": home_won,
                        "phase": phase,
                        "analysis_eligible": True,
                        "block_number": identity,
                        "timestamp": timestamp,
                        "transaction_hash": f"trade-{identity}",
                        "log_index": identity,
                        "exchange_address": "exchange",
                        "actual_start_utc": start,
                        "inning_4_start_utc": top4,
                        "inning_7_start_utc": top7,
                        "actual_end_utc": end,
                    }
                )
        primary_probability = 0.4 if game_index < 30 else 0.6
        sensitivity_present = game_index >= 2
        same = game_index % 2 == 0
        close_rows.append(
            {
                "market_id": market_id,
                "game_pk": game_pk,
                "official_date": official_date,
                "home_won": home_won,
                "actual_start_utc": start,
                "primary_has_close": True,
                "primary_missing_reason": None,
                "primary_close_timestamp": int(start.timestamp()) - 20,
                "primary_home_probability": primary_probability,
                "primary_block_number": 10_000 + 2 * game_index,
                "primary_transaction_hash": f"a-close-{game_index}",
                "primary_log_index": game_index,
                "primary_exchange_address": "exchange",
                "sensitivity_has_close": sensitivity_present,
                "sensitivity_missing_reason": (
                    None if sensitivity_present else "no_eligible_sensitivity_fill"
                ),
                "sensitivity_close_timestamp": (
                    int(start.timestamp()) - (20 if same else 100)
                    if sensitivity_present
                    else None
                ),
                "sensitivity_home_probability": (
                    primary_probability if same and sensitivity_present
                    else primary_probability + 0.01 if sensitivity_present
                    else None
                ),
                "sensitivity_block_number": (
                    10_000 + 2 * game_index - (0 if same else 1)
                    if sensitivity_present
                    else None
                ),
                "sensitivity_transaction_hash": (
                    f"a-close-{game_index}" if same and sensitivity_present else
                    f"c-close-{game_index}" if sensitivity_present else None
                ),
                "sensitivity_log_index": game_index if sensitivity_present else None,
                "sensitivity_exchange_address": (
                    "exchange" if sensitivity_present else None
                ),
            }
        )
    phase_path = tmp_path / "phase_trades.parquet"
    close_path = tmp_path / "dual_closes.parquet"
    pd.DataFrame(phase_rows).to_parquet(phase_path, index=False)
    close_frame = pd.DataFrame(close_rows)
    close_frame["home_won"] = close_frame["home_won"].astype("int8")
    for column in (
        "primary_close_timestamp",
        "primary_block_number",
        "sensitivity_close_timestamp",
        "sensitivity_block_number",
    ):
        close_frame[column] = close_frame[column].astype("Int64")
    for column in ("primary_log_index", "sensitivity_log_index"):
        close_frame[column] = close_frame[column].astype("Int32")
    for column in ("primary_missing_reason", "sensitivity_missing_reason"):
        close_frame[column] = close_frame[column].astype("string")
    close_frame.to_parquet(close_path, index=False)
    return phase_path, close_path


def _run(tmp_path: Path) -> tuple[dict[str, object], Path]:
    phase_path, close_path = _inputs(tmp_path)
    run_dir = tmp_path / "estimate_run"
    con = duckdb.connect()
    try:
        summary = estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()
    return summary, run_dir


def test_closing_profiles_pairing_suppression_and_brier(tmp_path: Path) -> None:
    summary, run_dir = _run(tmp_path)

    assert sorted(path.name for path in run_dir.iterdir()) == [
        "closing_calibration.parquet",
        "closing_paired_sensitivity.parquet",
        "estimator_summary.json",
        "trade_phase_calibration.parquet",
    ]
    assert summary == json.loads((run_dir / "estimator_summary.json").read_text())
    coverage = summary["counts"]["closing_coverage"]
    assert coverage["primary_coverage"] == 60
    assert coverage["primary_missing"] == 0
    assert coverage["sensitivity_coverage"] == 58
    assert coverage["sensitivity_missing"] == 2

    closing = pd.read_parquet(run_dir / "closing_calibration.parquet")
    primary_overall = closing[
        (closing["close_definition"] == "primary")
        & (closing["profile_scope"] == "overall")
    ].iloc[0]
    assert primary_overall["game_count"] == 60
    assert primary_overall["mean_probability"] == pytest.approx(0.5)
    assert primary_overall["win_rate"] == pytest.approx(0.5)
    assert primary_overall["mean_calibration"] == pytest.approx(0.0)
    assert primary_overall["brier_score"] == pytest.approx(0.26)
    thin = closing[
        (closing["close_definition"] == "primary")
        & (closing["price_decile"] == 5)
    ].iloc[0]
    assert thin["game_count"] == 30
    assert bool(thin["suppressed"])
    assert pd.isna(thin["mean_calibration"])

    paired = pd.read_parquet(run_dir / "closing_paired_sensitivity.parquet")
    overall = paired[paired["profile_scope"] == "overall"].iloc[0]
    assert overall["common_games"] == 58
    assert overall["same_close_event_games"] == 29
    assert overall["different_close_event_games"] == 29
    assert overall["same_close_timestamp_games"] == 29
    assert overall["different_close_timestamp_games"] == 29
    assert overall["mean_probability_difference"] == pytest.approx(-0.005)
    assert overall["mean_calibration_difference"] == pytest.approx(0.005)


def test_phase_profiles_and_symmetric_boundary_exclusion(tmp_path: Path) -> None:
    _, run_dir = _run(tmp_path)
    profile = pd.read_parquet(run_dir / "trade_phase_calibration.parquet")

    for phase in ("pregame", "innings_1_3", "innings_4_6", "innings_7_plus"):
        literal = profile[
            (profile["boundary_sample"] == "literal")
            & (profile["phase"] == phase)
            & (profile["price_decile"] == 6)
        ].iloc[0]
        buffered = profile[
            (profile["boundary_sample"] == "exclude_within_30s")
            & (profile["phase"] == phase)
            & (profile["price_decile"] == 6)
        ].iloc[0]
        assert literal["trade_count"] == 120
        assert literal["game_count"] == 60
        assert literal["dollars"] == pytest.approx(300.0)
        assert literal["mean_price"] == pytest.approx(0.55)
        assert literal["win_rate"] == pytest.approx(0.5)
        assert literal["mean_calibration"] == pytest.approx(-0.05)
        assert not bool(literal["suppressed"])
        assert buffered["trade_count"] == 60
        assert buffered["dollars"] == pytest.approx(120.0)
        assert not bool(buffered["suppressed"])
    empty = profile[
        (profile["boundary_sample"] == "literal")
        & (profile["phase"] == "pregame")
        & (profile["price_decile"] == 1)
    ].iloc[0]
    assert empty["trade_count"] == 0
    assert bool(empty["suppressed"])
    assert pd.isna(empty["mean_price"])

    source = pd.read_parquet(tmp_path / "phase_trades.parquet")
    pregame = source[source["phase"] == "pregame"]
    expected_se = cluster_se_mean(
        pregame["calibration_error"],
        pregame["day"],
        pregame["proxyWallet"],
        pregame["game_pk"],
    )
    reported_se = profile[
        (profile["boundary_sample"] == "literal")
        & (profile["phase"] == "pregame")
        & (profile["price_decile"] == 6)
    ].iloc[0]["calibration_se"]
    assert reported_se == pytest.approx(expected_se, abs=1e-15)


def test_invalid_phase_and_partial_close_fail_without_publication(tmp_path: Path) -> None:
    phase_path, close_path = _inputs(tmp_path)
    phase = pd.read_parquet(phase_path)
    phase.loc[0, "calibration_error"] += 0.1
    phase.to_parquet(phase_path, index=False)
    run_dir = tmp_path / "invalid_phase_run"
    con = duckdb.connect()
    try:
        with pytest.raises(CalibrationEstimatorError, match="Invalid phase-trade"):
            estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()
    assert not run_dir.exists()

    phase_path, close_path = _inputs(tmp_path / "partial")
    closes = pd.read_parquet(close_path)
    closes.loc[2, "sensitivity_transaction_hash"] = None
    closes.to_parquet(close_path, index=False)
    second_run = tmp_path / "partial_run"
    con = duckdb.connect()
    try:
        with pytest.raises(CalibrationEstimatorError, match="Invalid dual-close"):
            estimate_calibration(con, phase_path, close_path, second_run)
    finally:
        con.close()
    assert not second_run.exists()


def test_phase_cluster_keys_and_boundaries_must_match_audited_values(
    tmp_path: Path,
) -> None:
    phase_path, close_path = _inputs(tmp_path / "bad_day")
    phase = pd.read_parquet(phase_path)
    phase.loc[0, "day"] += 1
    phase.to_parquet(phase_path, index=False)
    con = duckdb.connect()
    try:
        with pytest.raises(CalibrationEstimatorError, match="Invalid phase-trade"):
            estimate_calibration(con, phase_path, close_path, tmp_path / "bad_day_run")
    finally:
        con.close()

    phase_path, close_path = _inputs(tmp_path / "changing_boundary")
    phase = pd.read_parquet(phase_path)
    phase.loc[0, "inning_4_start_utc"] += timedelta(seconds=1)
    phase.to_parquet(phase_path, index=False)
    con = duckdb.connect()
    try:
        with pytest.raises(CalibrationEstimatorError, match="dimensions are inconsistent"):
            estimate_calibration(
                con, phase_path, close_path, tmp_path / "changing_boundary_run"
            )
    finally:
        con.close()


def test_existing_run_is_immutable(tmp_path: Path) -> None:
    phase_path, close_path = _inputs(tmp_path)
    run_dir = tmp_path / "existing"
    run_dir.mkdir()
    con = duckdb.connect()
    try:
        with pytest.raises(FileExistsError, match="already exists"):
            estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()


def test_real_dual_close_builder_output_runs_end_to_end(tmp_path: Path) -> None:
    builder_dir = tmp_path / "builder"
    builder_dir.mkdir()
    builder_paths = write_dual_close_fixture(builder_dir)
    build_dual_close_fixture(builder_paths)
    close_path = builder_paths["run"] / "game_closes.parquet"
    closes = pd.read_parquet(close_path)

    con = duckdb.connect()
    try:
        schema = {
            row[0]: row[1]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{close_path}')"
            ).fetchall()
        }
    finally:
        con.close()
    assert {column: schema[column] for column in DUAL_CLOSE_TYPES} == DUAL_CLOSE_TYPES

    phase_rows = []
    for index, close in closes.iterrows():
        start = close["actual_start_utc"].to_pydatetime()
        timestamp = int(start.timestamp()) - 60
        won = int(close["home_won"])
        phase_rows.append(
            {
                "market_id": close["market_id"],
                "game_pk": int(close["game_pk"]),
                "official_date": close["official_date"],
                "proxyWallet": f"fixture-wallet-{index}",
                "day": timestamp // 86400,
                "trade_day_utc": datetime.fromtimestamp(
                    timestamp, timezone.utc
                ).date(),
                "price": 0.5,
                "usdc": 1.0,
                "won": won,
                "calibration_error": won - 0.5,
                "home_won": won,
                "phase": "pregame",
                "analysis_eligible": True,
                "block_number": 20_000 + index,
                "timestamp": timestamp,
                "transaction_hash": f"phase-trade-{index}",
                "log_index": index,
                "exchange_address": "0xphase",
                "actual_start_utc": start,
                "inning_4_start_utc": start + timedelta(seconds=1_000),
                "inning_7_start_utc": start + timedelta(seconds=2_000),
                "actual_end_utc": start + timedelta(seconds=3_000),
            }
        )
    phase_path = tmp_path / "builder_phase_trades.parquet"
    pd.DataFrame(phase_rows).to_parquet(phase_path, index=False)
    run_dir = tmp_path / "builder_estimate_run"
    con = duckdb.connect()
    try:
        summary = estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()

    coverage = summary["counts"]["closing_coverage"]
    assert coverage["games"] == 5
    assert coverage["primary_coverage"] == 4
    assert coverage["primary_missing"] == 1
    assert coverage["sensitivity_coverage"] == 3
    assert coverage["sensitivity_missing"] == 2
    output = pd.read_parquet(run_dir / "closing_calibration.parquet")
    primary = output[
        (output["close_definition"] == "primary")
        & (output["profile_scope"] == "overall")
    ].iloc[0]
    sensitivity = output[
        (output["close_definition"] == "sensitivity")
        & (output["profile_scope"] == "overall")
    ].iloc[0]
    assert primary["game_count"] == 4
    assert sensitivity["game_count"] == 3


def test_close_universe_may_strictly_contain_phase_games(tmp_path: Path) -> None:
    phase_path, close_path = _inputs(tmp_path)
    phase = pd.read_parquet(phase_path)
    phase = phase[phase["game_pk"] != 1002]
    phase.to_parquet(phase_path, index=False)

    run_dir = tmp_path / "close_superset_run"
    con = duckdb.connect()
    try:
        summary = estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()

    assert summary["counts"]["phase_input"]["games"] == 59
    coverage = summary["counts"]["closing_coverage"]
    assert coverage["games"] == 60
    assert coverage["close_only_games"] == 1
    assert coverage["close_only_game_sample"] == [
        {"market_id": "market-02", "game_pk": 1002}
    ]
    closing = pd.read_parquet(run_dir / "closing_calibration.parquet")
    primary = closing[
        (closing["close_definition"] == "primary")
        & (closing["profile_scope"] == "overall")
    ].iloc[0]
    assert primary["game_count"] == 60
    sensitivity = closing[
        (closing["close_definition"] == "sensitivity")
        & (closing["profile_scope"] == "overall")
    ].iloc[0]
    assert sensitivity["game_count"] == 58
    trade = pd.read_parquet(run_dir / "trade_phase_calibration.parquet")
    pregame = trade[
        (trade["boundary_sample"] == "literal")
        & (trade["phase"] == "pregame")
        & (trade["price_decile"] == 6)
    ].iloc[0]
    assert pregame["game_count"] == 59
    assert pregame["trade_count"] == 118
