from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

import build_phase_dataset as phase_module  # noqa: E402
from build_phase_dataset import (  # noqa: E402
    PhaseDatasetBuildError,
    build_phase_dataset,
)
from timestamp_provenance import TimestampProvenanceError  # noqa: E402


BASE = datetime(2025, 7, 4, 17, 0, tzinfo=timezone.utc)


def _eligible(
    market_id: str,
    game_pk: int,
    *,
    home_wins: bool,
    offset: int = 0,
) -> dict[str, object]:
    away_token = f"{market_id}-away"
    home_token = f"{market_id}-home"
    start = BASE + timedelta(seconds=offset)
    return {
        "market_id": market_id,
        "game_pk": game_pk,
        "official_date": date(2025, 7, 4),
        "away_team_id": 147,
        "away_team_name": "New York Yankees",
        "home_team_id": 111,
        "home_team_name": "Boston Red Sox",
        "away_token_id": away_token,
        "home_token_id": home_token,
        "winning_team_id": 111 if home_wins else 147,
        "winning_token_id": home_token if home_wins else away_token,
        "winning_outcome": "Red Sox" if home_wins else "Yankees",
        "actual_start_utc": start,
        "inning_4_start_utc": start + timedelta(seconds=1_000),
        "inning_7_start_utc": start + timedelta(seconds=2_000),
        "actual_end_utc": start + timedelta(seconds=3_000),
        "away_final_score": 2 if home_wins else 5,
        "home_final_score": 5 if home_wins else 2,
        "away_is_winner": not home_wins,
        "home_is_winner": home_wins,
    }


def _trade(
    identity: str,
    market_id: str,
    timestamp: int,
    block_number: int,
    log_index: int,
    *,
    side: str,
    home_wins: bool,
    price: float = 0.6,
    usdc: float = 1.0,
) -> dict[str, object]:
    home = side == "home"
    return {
        "market_id": market_id,
        "token_id": f"{market_id}-{'home' if home else 'away'}",
        "block_number": block_number,
        "timestamp": timestamp,
        "transaction_hash": f"tx-{identity}",
        "log_index": log_index,
        "exchange_address": "0xexchange",
        "proxyWallet": f"0xwallet-{identity}",
        "counterparty": "0xcounterparty",
        "is_maker": False,
        "outcome": "Red Sox" if home else "Yankees",
        "winning_outcome": "Red Sox" if home_wins else "Yankees",
        "price": price,
        "usdcSize": usdc,
    }


def _base_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    first_start = int(BASE.timestamp())
    second_start = first_start + 10_000
    eligible = [
        _eligible("market-one", 1001, home_wins=True),
        _eligible("market-two", 1002, home_wins=False, offset=10_000),
    ]
    trades = [
        _trade(
            "old-close", "market-one", first_start - 20, 10, 1,
            side="away", home_wins=True,
        ),
        _trade(
            "close-low-log", "market-one", first_start - 1, 11, 1,
            side="away", home_wins=True, price=0.7, usdc=2,
        ),
        _trade(
            "close", "market-one", first_start - 1, 11, 2,
            side="home", home_wins=True, price=0.8, usdc=3,
        ),
        _trade(
            "start", "market-one", first_start, 12, 1,
            side="away", home_wins=True,
        ),
        _trade(
            "top4", "market-one", first_start + 1_000, 13, 1,
            side="home", home_wins=True,
        ),
        _trade(
            "top7", "market-one", first_start + 2_000, 14, 1,
            side="away", home_wins=True,
        ),
        _trade(
            "final", "market-one", first_start + 3_000, 15, 1,
            side="home", home_wins=True,
        ),
        _trade(
            "post", "market-one", first_start + 3_001, 16, 1,
            side="away", home_wins=True,
        ),
        _trade(
            "second-start", "market-two", second_start, 17, 1,
            side="away", home_wins=False, price=0.4, usdc=4,
        ),
        _trade(
            "ineligible", "not-eligible", second_start + 1, 18, 1,
            side="home", home_wins=True, usdc=5,
        ),
    ]
    trades[-1]["token_id"] = "other-token"
    return trades, eligible


def _write_inputs(
    tmp_path: Path,
    *,
    trades: list[dict[str, object]] | None = None,
    eligible: list[dict[str, object]] | None = None,
) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    default_trades, default_eligible = _base_rows()
    trade_rows = trades if trades is not None else default_trades
    paths = {
        "trades": tmp_path / "exact_trades.parquet",
        "eligible": tmp_path / "eligible_moneylines.parquet",
        "cache": tmp_path / "block_timestamps.parquet",
        "provenance": tmp_path / "timestamp_provenance.json",
        "run": tmp_path / "phase_run",
    }
    pd.DataFrame(trade_rows).to_parquet(paths["trades"], index=False)
    pd.DataFrame(eligible if eligible is not None else default_eligible).to_parquet(
        paths["eligible"], index=False
    )
    cache = (
        pd.DataFrame(trade_rows)[["block_number", "timestamp"]]
        .drop_duplicates()
        .sort_values("block_number")
    )
    cache.to_parquet(paths["cache"], index=False)
    cache_hash = hashlib.sha256(paths["cache"].read_bytes()).hexdigest()
    declaration = {
        "schema_version": 1,
        "method": "polygon_rpc_block_timestamp",
        "timestamp_unit": "unix_seconds",
        "cache": {
            "path": str(paths["cache"]),
            "format": "parquet",
            "rows": len(cache),
            "sha256": cache_hash,
            "required_columns": ["block_number", "timestamp"],
        },
        "build_metadata": {
            "used_exact_cache": True,
            "source_distinct_blocks": len(cache),
            "cache_distinct_blocks": len(cache),
            "missing_blocks": 0,
            "fallback_rows": 0,
        },
    }
    paths["provenance"].write_text(json.dumps(declaration), encoding="utf-8")
    return paths


def _build(paths: dict[str, Path]) -> dict[str, object]:
    con = duckdb.connect()
    try:
        return build_phase_dataset(
            con,
            paths["trades"],
            paths["eligible"],
            paths["provenance"],
            paths["run"],
        )
    finally:
        con.close()


def test_phase_boundaries_close_home_normalization_and_reconciliation(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    report = _build(paths)

    assert sorted(path.name for path in paths["run"].iterdir()) == [
        "boundary_audit.parquet",
        "closing_audit.parquet",
        "closing_lines.parquet",
        "phase_trades.parquet",
        "reconciliation.json",
    ]
    assert report == json.loads((paths["run"] / "reconciliation.json").read_text())
    validation = report["timestamp_provenance_validation"]
    assert validation["analysis_extract_verified"] is True
    assert validation["extract"]["rows"] == 10
    for name in ("exact_trades", "eligible_moneylines", "timestamp_provenance"):
        source = report["inputs"][name]
        assert source["bytes"] > 0
        assert source["sha256"] == hashlib.sha256(
            Path(source["path"]).read_bytes()
        ).hexdigest()

    phase = pd.read_parquet(paths["run"] / "phase_trades.parquet").set_index(
        "transaction_hash"
    )
    assert phase.loc["tx-start", "phase"] == "innings_1_3"
    assert phase.loc["tx-top4", "phase"] == "innings_4_6"
    assert phase.loc["tx-top7", "phase"] == "innings_7_plus"
    assert phase.loc["tx-final", "phase"] == "innings_7_plus"
    assert phase.loc["tx-post", "phase"] == "post_final"
    assert bool(phase.loc["tx-final", "analysis_eligible"]) is True
    assert bool(phase.loc["tx-post", "analysis_eligible"]) is False
    assert phase.loc["tx-start", "home_probability"] == pytest.approx(0.4)
    assert phase.loc["tx-start", "calibration_error"] == pytest.approx(-0.6)
    assert phase.loc["tx-top4", "home_probability"] == pytest.approx(0.6)
    assert phase.loc["tx-top4", "calibration_error"] == pytest.approx(0.4)

    closing = pd.read_parquet(paths["run"] / "closing_lines.parquet")
    assert len(closing) == 1
    assert closing.iloc[0]["transaction_hash"] == "tx-close"
    assert closing.iloc[0]["closing_home_probability"] == pytest.approx(0.8)
    assert closing.iloc[0]["close_age_seconds"] == pytest.approx(1.0)
    assert closing.iloc[0]["close_usdc"] == pytest.approx(3.0)

    closing_audit = pd.read_parquet(paths["run"] / "closing_audit.parquet").set_index(
        "market_id"
    )
    assert not bool(closing_audit.loc["market-two", "has_pregame_close"])
    assert closing_audit.loc["market-two", "exclusion_reason"] == "no_pregame_trade"

    counts = report["counts"]
    assert counts["input"] == {"rows": 10, "dollars": 20.0}
    assert counts["ineligible_market"] == {"rows": 1, "dollars": 5.0}
    assert counts["eligible_joined"] == {"rows": 9, "dollars": 15.0}
    assert counts["phases"]["pregame"] == {"rows": 3, "dollars": 6.0, "games": 1}
    assert counts["phases"]["innings_1_3"] == {
        "rows": 2,
        "dollars": 5.0,
        "games": 2,
    }
    assert counts["phases"]["innings_4_6"]["dollars"] == 1.0
    assert counts["phases"]["innings_7_plus"]["dollars"] == 2.0
    assert counts["phases"]["post_final"]["dollars"] == 1.0

    boundary = pd.read_parquet(paths["run"] / "boundary_audit.parquet")
    first_five = boundary[
        (boundary["market_id"] == "market-one")
        & (boundary["boundary_name"] == "first_play")
        & (boundary["window_seconds"] == 5)
    ].iloc[0]
    assert first_five["trade_count"] == 3
    assert first_five["trade_dollars"] == pytest.approx(6.0)
    final_five = boundary[
        (boundary["market_id"] == "market-one")
        & (boundary["boundary_name"] == "final_play")
        & (boundary["window_seconds"] == 5)
    ].iloc[0]
    assert final_five["trade_count"] == 2
    assert final_five["trade_dollars"] == pytest.approx(2.0)


def test_staleness_uses_chronologically_valid_block_order(tmp_path: Path) -> None:
    _, eligible = _base_rows()
    first_start = int(BASE.timestamp())
    second_start = first_start + 10_000
    trades = [
        _trade(
            "exact-5m", "market-one", first_start - 300, 1, 1,
            side="away", home_wins=True,
        ),
        _trade(
            "over-5m", "market-two", second_start - 301, 2, 1,
            side="away", home_wins=False,
        ),
    ]
    paths = _write_inputs(tmp_path, trades=trades, eligible=eligible)
    _build(paths)

    closes = pd.read_parquet(paths["run"] / "closing_lines.parquet").set_index(
        "market_id"
    )
    assert closes.loc["market-one", "close_age_seconds"] == pytest.approx(300.0)
    assert not bool(closes.loc["market-one", "stale_over_5m"])
    assert closes.loc["market-two", "close_age_seconds"] == pytest.approx(301.0)
    assert bool(closes.loc["market-two", "stale_over_5m"])


def test_block_timestamps_must_be_nondecreasing(tmp_path: Path) -> None:
    trades, eligible = _base_rows()
    trades[-1]["timestamp"] = int(BASE.timestamp())
    paths = _write_inputs(tmp_path, trades=trades, eligible=eligible)

    with pytest.raises(PhaseDatasetBuildError, match="nondecreasing by block number"):
        _build(paths)
    assert not paths["run"].exists()


@pytest.mark.parametrize("price", [0.01, 0.99])
def test_off_spec_prices_fail(tmp_path: Path, price: float) -> None:
    trades, eligible = _base_rows()
    trades[0]["price"] = price
    paths = _write_inputs(tmp_path, trades=trades, eligible=eligible)

    with pytest.raises(PhaseDatasetBuildError, match="invalid numeric rows"):
        _build(paths)


def test_provenance_unit_and_extract_timestamp_failures(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path / "unit")
    declaration = json.loads(paths["provenance"].read_text())
    declaration["timestamp_unit"] = "milliseconds"
    paths["provenance"].write_text(json.dumps(declaration), encoding="utf-8")
    with pytest.raises(TimestampProvenanceError, match="unix_seconds"):
        _build(paths)

    altered = _write_inputs(tmp_path / "altered")
    frame = pd.read_parquet(altered["trades"])
    frame.loc[0, "timestamp"] += 1
    frame.to_parquet(altered["trades"], index=False)
    with pytest.raises(TimestampProvenanceError, match="altered/non-exact"):
        _build(altered)


def test_production_eligible_schema_and_result_consistency_are_required(
    tmp_path: Path,
) -> None:
    trades, eligible = _base_rows()
    missing_score = [{key: value for key, value in row.items() if key != "away_final_score"}
                     for row in eligible]
    paths = _write_inputs(tmp_path / "schema", trades=trades, eligible=missing_score)
    with pytest.raises(PhaseDatasetBuildError, match="away_final_score"):
        _build(paths)

    inconsistent = [dict(row) for row in eligible]
    inconsistent[0]["away_final_score"] = 9
    bad_paths = _write_inputs(tmp_path / "winner", trades=trades, eligible=inconsistent)
    with pytest.raises(PhaseDatasetBuildError, match="score/winner fields"):
        _build(bad_paths)


def test_duplicate_block_log_and_joined_token_mismatches_fail(tmp_path: Path) -> None:
    trades, eligible = _base_rows()
    duplicate_order = dict(trades[0])
    duplicate_order["transaction_hash"] = "tx-distinct-event"
    trades.append(duplicate_order)
    paths = _write_inputs(tmp_path / "order", trades=trades, eligible=eligible)
    with pytest.raises(PhaseDatasetBuildError, match=r"unique block_number \+ log_index"):
        _build(paths)

    token_trades, token_eligible = _base_rows()
    token_trades[0]["token_id"] = "unknown-token"
    token_paths = _write_inputs(
        tmp_path / "token", trades=token_trades, eligible=token_eligible
    )
    with pytest.raises(PhaseDatasetBuildError, match="token IDs absent"):
        _build(token_paths)


def test_unordered_authoritative_boundaries_fail(tmp_path: Path) -> None:
    trades, eligible = _base_rows()
    eligible[0]["inning_4_start_utc"] = eligible[0]["inning_7_start_utc"]
    paths = _write_inputs(tmp_path, trades=trades, eligible=eligible)

    with pytest.raises(PhaseDatasetBuildError, match="ordered, non-null"):
        _build(paths)


def test_failed_staging_verification_cleans_up_without_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _write_inputs(tmp_path)

    def injected_failure(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected staged verification failure")

    monkeypatch.setattr(phase_module, "_verify_staged_run", injected_failure)
    with pytest.raises(RuntimeError, match="injected staged verification failure"):
        _build(paths)
    assert not paths["run"].exists()
    assert list(tmp_path.glob(".phase_run.staging-*")) == []


def test_existing_immutable_destination_is_never_overwritten(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    paths["run"].mkdir()
    sentinel = paths["run"] / "keep.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    with pytest.raises(FileExistsError, match="already exists"):
        _build(paths)
    assert sentinel.read_text(encoding="utf-8") == "preserve"
