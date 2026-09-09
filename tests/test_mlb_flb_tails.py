from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "calibration_heterogeneity"
TEST_DIR = Path(__file__).parent
sys.path.insert(0, str(MODULE_DIR))
sys.path.insert(0, str(ENGINE_DIR))
sys.path.insert(0, str(TEST_DIR))

from estimate_calibration import estimate_calibration  # noqa: E402
from estimate_flb_tails import (  # noqa: E402
    ESTIMATE_COLUMNS,
    FLBTailEstimatorError,
    OUTPUT_COLUMNS,
    OUTPUT_TYPES,
    estimate_flb_tails,
)
from flb_engine import cluster_se_difference  # noqa: E402
from test_mlb_calibration_estimator import _inputs  # noqa: E402


def _source_with_close_dollars(tmp_path: Path) -> tuple[Path, Path]:
    phase_path, close_path = _inputs(tmp_path)
    closes = pd.read_parquet(close_path)
    closes["primary_usdc"] = 1.0
    closes["sensitivity_usdc"] = closes["sensitivity_has_close"].map(
        lambda present: 1.0 if present else None
    )
    closes.to_parquet(close_path, index=False)
    return phase_path, close_path


def _rich_source(tmp_path: Path) -> tuple[Path, Path]:
    phase_path, close_path = _source_with_close_dollars(tmp_path)
    closes = pd.read_parquet(close_path)
    original_close = closes.copy()
    original_close["primary_home_probability"] = np.where(
        original_close["game_pk"] < 1030, 0.05, 0.95
    )
    original_close["sensitivity_home_probability"] = np.where(
        original_close["sensitivity_has_close"],
        original_close["primary_home_probability"]
        + np.where(original_close.index % 2, 0.01, 0.0),
        np.nan,
    )
    copied_close = original_close.copy()
    copied_close["market_id"] = copied_close["market_id"] + "-copy"
    copied_close["game_pk"] = copied_close["game_pk"] + 10_000
    copied_close["primary_home_probability"] = np.where(
        original_close["game_pk"] < 1030, 0.95, 0.05
    )
    copied_close["sensitivity_home_probability"] = np.where(
        copied_close["sensitivity_has_close"],
        copied_close["primary_home_probability"]
        + np.where(copied_close.index % 2, 0.01, 0.0),
        np.nan,
    )
    for definition in ("primary", "sensitivity"):
        copied_close[f"{definition}_transaction_hash"] = (
            copied_close[f"{definition}_transaction_hash"].astype("string") + "-copy"
        )
        copied_close[f"{definition}_block_number"] = (
            copied_close[f"{definition}_block_number"] + 100_000
        )
    closes = pd.concat([original_close, copied_close], ignore_index=True)
    closes["home_won"] = closes["home_won"].astype("int8")
    for column in (
        "primary_close_timestamp",
        "primary_block_number",
        "sensitivity_close_timestamp",
        "sensitivity_block_number",
    ):
        closes[column] = closes[column].astype("Int64")
    for column in ("primary_log_index", "sensitivity_log_index"):
        closes[column] = closes[column].astype("Int32")
    closes.to_parquet(close_path, index=False)

    phase = pd.read_parquet(phase_path)
    phase = phase.sort_values(["game_pk", "phase", "timestamp"]).copy()
    within_game_phase = phase.groupby(["game_pk", "phase"]).cumcount()
    far_is_d1 = phase["game_pk"] % 2 == 0
    phase["price"] = np.where(
        within_game_phase == 0,
        np.where(far_is_d1, 0.05, 0.95),
        np.where(far_is_d1, 0.95, 0.05),
    )
    phase["calibration_error"] = phase["won"] - phase["price"]
    copied_phase = phase.copy()
    copied_phase["market_id"] = copied_phase["market_id"] + "-copy"
    copied_phase["game_pk"] = copied_phase["game_pk"] + 10_000
    copied_phase["proxyWallet"] = copied_phase["proxyWallet"] + "-copy"
    copied_phase["block_number"] = copied_phase["block_number"] + 100_000
    copied_phase["transaction_hash"] = copied_phase["transaction_hash"] + "-copy"
    copied_phase["log_index"] = copied_phase["log_index"] + 100_000
    phase = pd.concat([phase, copied_phase], ignore_index=True)
    phase.to_parquet(phase_path, index=False)
    return phase_path, close_path


def _stage08(tmp_path: Path, *, rich: bool) -> tuple[Path, Path, Path]:
    phase_path, close_path = (
        _rich_source(tmp_path / "source")
        if rich
        else _source_with_close_dollars(tmp_path / "source")
    )
    run_dir = tmp_path / "stage08"
    con = duckdb.connect()
    try:
        estimate_calibration(con, phase_path, close_path, run_dir)
    finally:
        con.close()
    return run_dir, phase_path, close_path


def _run_stage09(
    tmp_path: Path, *, rich: bool
) -> tuple[dict[str, object], Path, Path, Path, Path]:
    stage08, phase_path, close_path = _stage08(tmp_path, rich=rich)
    run_dir = tmp_path / "stage09"
    con = duckdb.connect()
    try:
        summary = estimate_flb_tails(
            con,
            stage08 / "closing_calibration.parquet",
            stage08 / "trade_phase_calibration.parquet",
            stage08 / "estimator_summary.json",
            close_path,
            phase_path,
            run_dir,
        )
    finally:
        con.close()
    return summary, run_dir, stage08, phase_path, close_path


def test_reported_tail_schema_supports_and_joint_clustered_spreads(
    tmp_path: Path,
) -> None:
    summary, run_dir, _, phase_path, close_path = _run_stage09(tmp_path, rich=True)
    assert sorted(path.name for path in run_dir.iterdir()) == [
        "flb_summary.json",
        "flb_tail_summary.parquet",
    ]
    assert summary == json.loads((run_dir / "flb_summary.json").read_text())
    output = pd.read_parquet(run_dir / "flb_tail_summary.parquet")
    assert tuple(output.columns) == OUTPUT_COLUMNS
    con = duckdb.connect()
    try:
        schema = {
            row[0]: row[1]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{run_dir / 'flb_tail_summary.parquet'}')"
            ).fetchall()
        }
    finally:
        con.close()
    assert schema == OUTPUT_TYPES
    assert len(output) == 10
    assert set(output["status"]) == {"reported"}
    assert summary["counts"]["reported_tail_rows"] == 10
    assert all(summary["reconciliation"].values())

    primary = output[
        (output["analysis_scope"] == "closing")
        & (output["close_definition"] == "primary")
    ].iloc[0]
    assert primary["d1_n"] == primary["d1_games"] == 60
    assert primary["d10_n"] == primary["d10_games"] == 60
    assert primary["d1_dollars"] == pytest.approx(60.0)
    assert primary["d10_dollars"] == pytest.approx(60.0)
    assert primary["d1_mean_calibration"] == pytest.approx(0.45)
    assert primary["d10_mean_calibration"] == pytest.approx(-0.45)
    assert primary["spread_d10_minus_d1"] == pytest.approx(-0.9)
    assert primary["point_pattern"] == "reverse_flb_signs"

    closes = pd.read_parquet(close_path)
    close_obs = closes[closes["primary_has_close"]].copy()
    close_obs["price"] = close_obs["primary_home_probability"]
    close_obs["ret"] = close_obs["home_won"] - close_obs["price"]
    low = close_obs[close_obs["price"] < 0.1]
    high = close_obs[close_obs["price"] >= 0.9]
    influence = pd.concat(
        [
            pd.DataFrame(
                {
                    "official_date": low["official_date"],
                    "score": -(low["ret"] - low["ret"].mean()) / len(low),
                }
            ),
            pd.DataFrame(
                {
                    "official_date": high["official_date"],
                    "score": (high["ret"] - high["ret"].mean()) / len(high),
                }
            ),
        ]
    )
    expected_close_se = float(
        np.sqrt((influence.groupby("official_date")["score"].sum() ** 2).sum())
    )
    assert primary["spread_se"] == pytest.approx(expected_close_se, abs=1e-15)

    pregame = output[
        (output["analysis_scope"] == "trade_phase")
        & (output["boundary_sample"] == "literal")
        & (output["phase"] == "pregame")
    ].iloc[0]
    assert pregame["d1_n"] == pregame["d10_n"] == 120
    assert pregame["d1_games"] == pregame["d10_games"] == 120
    phase = pd.read_parquet(phase_path)
    phase = phase[phase["phase"] == "pregame"]
    low = phase[phase["price"] < 0.1]
    high = phase[phase["price"] >= 0.9]
    expected_phase_se = cluster_se_difference(
        low["calibration_error"],
        high["calibration_error"],
        low["day"],
        low["proxyWallet"],
        low["game_pk"],
        high["day"],
        high["proxyWallet"],
        high["game_pk"],
    )
    assert pregame["spread_se"] == pytest.approx(expected_phase_se, abs=1e-15)
    assert pregame["spread_ci95_low"] == pytest.approx(
        pregame["spread_d10_minus_d1"] - 1.96 * pregame["spread_se"]
    )


def test_entire_assessment_is_suppressed_when_either_tail_is_thin(
    tmp_path: Path,
) -> None:
    stage08, phase_path, close_path = _stage08(tmp_path, rich=True)
    closes = pd.read_parquet(close_path)
    high = closes.index[
        (closes["primary_home_probability"] >= 0.9)
        & (closes["primary_transaction_hash"] != closes["sensitivity_transaction_hash"])
    ]
    closes.loc[high[:11], "primary_home_probability"] = 0.5
    closes.to_parquet(close_path, index=False)

    # Rebuild Stage-08 because the underlying source and profile must agree.
    replacement = tmp_path / "stage08_rebuilt"
    con = duckdb.connect()
    try:
        estimate_calibration(con, phase_path, close_path, replacement)
    finally:
        con.close()
    con = duckdb.connect()
    try:
        summary = estimate_flb_tails(
            con,
            replacement / "closing_calibration.parquet",
            replacement / "trade_phase_calibration.parquet",
            replacement / "estimator_summary.json",
            close_path,
            phase_path,
            tmp_path / "stage09",
        )
    finally:
        con.close()
    assert summary["counts"]["suppressed_tail_rows"] == 1
    output = pd.read_parquet(tmp_path / "stage09" / "flb_tail_summary.parquet")
    row = output[
        (output["analysis_scope"] == "closing")
        & (output["close_definition"] == "primary")
    ].iloc[0]
    assert row["d1_n"] == 60
    assert row["d10_n"] == 49
    assert bool(row["suppressed"])
    assert row["status"] == "suppressed_tail_n_lt_50"
    assert row["point_pattern"] == "suppressed"
    assert row[list(ESTIMATE_COLUMNS)].isna().all()


def test_stage08_corruption_fingerprint_and_existing_run_fail_closed(
    tmp_path: Path,
) -> None:
    stage08, phase_path, close_path = _stage08(tmp_path, rich=True)
    profile = pd.read_parquet(stage08 / "closing_calibration.parquet")
    target = (profile["close_definition"] == "primary") & (profile["price_decile"] == 1)
    profile.loc[target, "game_count"] += 1
    profile.to_parquet(stage08 / "closing_calibration.parquet", index=False)
    run_dir = tmp_path / "corrupt_run"
    con = duckdb.connect()
    try:
        with pytest.raises(FLBTailEstimatorError, match="does not recompute"):
            estimate_flb_tails(
                con,
                stage08 / "closing_calibration.parquet",
                stage08 / "trade_phase_calibration.parquet",
                stage08 / "estimator_summary.json",
                close_path,
                phase_path,
                run_dir,
            )
    finally:
        con.close()
    assert not run_dir.exists()

    clean_stage08, clean_phase, clean_close = _stage08(
        tmp_path / "fingerprint", rich=False
    )
    phase = pd.read_parquet(clean_phase)
    phase.loc[0, "usdc"] += 0.01
    phase.to_parquet(clean_phase, index=False)
    con = duckdb.connect()
    try:
        with pytest.raises(FLBTailEstimatorError, match="source fingerprint"):
            estimate_flb_tails(
                con,
                clean_stage08 / "closing_calibration.parquet",
                clean_stage08 / "trade_phase_calibration.parquet",
                clean_stage08 / "estimator_summary.json",
                clean_close,
                clean_phase,
                tmp_path / "fingerprint_run",
            )
    finally:
        con.close()
    assert not (tmp_path / "fingerprint_run").exists()

    existing = tmp_path / "existing"
    existing.mkdir()
    con = duckdb.connect()
    try:
        with pytest.raises(FileExistsError, match="already exists"):
            estimate_flb_tails(
                con,
                clean_stage08 / "closing_calibration.parquet",
                clean_stage08 / "trade_phase_calibration.parquet",
                clean_stage08 / "estimator_summary.json",
                clean_close,
                clean_phase,
                existing,
            )
    finally:
        con.close()
