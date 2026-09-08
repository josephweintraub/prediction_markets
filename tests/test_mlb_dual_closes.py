from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

from build_dual_closes import (  # noqa: E402
    DualCloseBuildError,
    build_dual_closes,
)


START_TIMESTAMP = 1_750_001_000
START = datetime.fromtimestamp(START_TIMESTAMP, tz=timezone.utc)


def _eligible(market_id: str, game_pk: int) -> dict[str, object]:
    return {
        "market_id": market_id,
        "game_pk": game_pk,
        "official_date": date(2025, 7, 4),
        "game_type": "R",
        "slug_orientation": "official",
        "away_team_id": 147,
        "away_team_name": "New York Yankees",
        "home_team_id": 111,
        "home_team_name": "Boston Red Sox",
        "away_token_id": f"{market_id}-away",
        "home_token_id": f"{market_id}-home",
        "winning_team_id": 111,
        "winning_token_id": f"{market_id}-home",
        "winning_outcome": "Red Sox",
        "actual_start_utc": START,
    }


def _fill(
    identity: str,
    market_id: str,
    *,
    block_number: int,
    buyer: str,
    counterparty: str,
    price: float,
    side: str = "home",
    log_index: int = 1,
) -> dict[str, object]:
    token = f"{market_id}-{side}"
    return {
        "order_hash": f"order-{identity}",
        "maker": counterparty,
        "taker": buyer,
        "maker_asset_id": token,
        "taker_asset_id": "0",
        "maker_amount_filled": 1_000_000,
        "taker_amount_filled": round(price * 1_000_000),
        "fee": 0,
        "block_number": block_number,
        "transaction_hash": f"tx-{identity}",
        "log_index": log_index,
        "exchange_address": "0xexchange",
        "condition_id": market_id,
        "outcome": "Red Sox" if side == "home" else "Yankees",
        "winning_outcome": "Red Sox",
        "outcome_token_side": "maker",
    }


def _base_rows() -> tuple[list[dict[str, object]], dict[int, int]]:
    rows = [
        _fill(
            "m1-human",
            "market-one",
            block_number=100,
            buyer="0xhuman1",
            counterparty="0xseller1",
            price=0.40,
            side="away",
        ),
        _fill(
            "m1-bot",
            "market-one",
            block_number=102,
            buyer="0xbot",
            counterparty="0xseller2",
            price=0.70,
        ),
        _fill(
            "m2-bot-counterparty",
            "market-two",
            block_number=104,
            buyer="0xhuman2",
            counterparty="0xbotcounterparty",
            price=0.25,
            side="away",
        ),
        _fill(
            "m3-only-bot",
            "market-three",
            block_number=103,
            buyer="0xbot",
            counterparty="0xseller3",
            price=0.55,
        ),
        _fill(
            "m4-strict",
            "market-four",
            block_number=101,
            buyer="0xhuman4",
            counterparty="0xseller4",
            price=0.60,
        ),
        _fill(
            "m4-extreme",
            "market-four",
            block_number=105,
            buyer="0xhuman4",
            counterparty="0xseller4",
            price=0.995,
        ),
    ]
    timestamps = {
        100: START_TIMESTAMP - 20,
        101: START_TIMESTAMP - 10,
        102: START_TIMESTAMP - 5,
        103: START_TIMESTAMP - 3,
        104: START_TIMESTAMP - 2,
        105: START_TIMESTAMP - 1,
    }
    return rows, timestamps


def _write_inputs(
    tmp_path: Path,
    *,
    rows: list[dict[str, object]] | None = None,
    timestamps: dict[int, int] | None = None,
) -> dict[str, Path]:
    base_rows, base_timestamps = _base_rows()
    raw_rows = rows if rows is not None else base_rows
    cache_values = timestamps if timestamps is not None else base_timestamps
    paths = {
        "raw": tmp_path / "resolved_trades.parquet",
        "eligible": tmp_path / "eligible_moneylines.parquet",
        "cache": tmp_path / "block_timestamps.parquet",
        "provenance": tmp_path / "timestamp_provenance.json",
        "wallets": tmp_path / "wallet_flags.parquet",
        "run": tmp_path / "dual_close_run",
    }
    pd.DataFrame(raw_rows).to_parquet(paths["raw"], index=False)
    pd.DataFrame(
        [_eligible(f"market-{name}", index) for index, name in enumerate(
            ("one", "two", "three", "four", "five"), start=1001
        )]
    ).to_parquet(paths["eligible"], index=False)
    pd.DataFrame(
        sorted(cache_values.items()), columns=["block_number", "timestamp"]
    ).to_parquet(paths["cache"], index=False)
    pd.DataFrame(
        [
            {"proxyWallet": "0xbot", "is_nonhuman": True},
            {"proxyWallet": "0xbotcounterparty", "is_nonhuman": True},
            {"proxyWallet": "0xhuman1", "is_nonhuman": False},
        ]
    ).to_parquet(paths["wallets"], index=False)
    declaration = {
        "schema_version": 1,
        "method": "polygon_rpc_block_timestamp",
        "timestamp_unit": "unix_seconds",
        "cache": {
            "path": str(paths["cache"]),
            "format": "parquet",
            "rows": len(cache_values),
            "sha256": hashlib.sha256(paths["cache"].read_bytes()).hexdigest(),
            "required_columns": ["block_number", "timestamp"],
        },
        "build_metadata": {
            "used_exact_cache": True,
            "source_distinct_blocks": len(cache_values),
            "cache_distinct_blocks": len(cache_values),
            "missing_blocks": 0,
            "fallback_rows": 0,
        },
    }
    paths["provenance"].write_text(json.dumps(declaration), encoding="utf-8")
    return paths


def _build(paths: dict[str, Path]) -> dict[str, object]:
    con = duckdb.connect()
    try:
        return build_dual_closes(
            con,
            paths["raw"],
            paths["eligible"],
            paths["provenance"],
            paths["wallets"],
            paths["run"],
        )
    finally:
        con.close()


def test_dual_closes_preserve_primary_and_published_c_semantics(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)
    report = _build(paths)
    output = pd.read_parquet(paths["run"] / "game_closes.parquet").set_index(
        "market_id"
    )

    assert list(sorted(path.name for path in paths["run"].iterdir())) == [
        "game_closes.parquet",
        "reconciliation.json",
    ]
    assert len(output) == 5
    assert report == json.loads((paths["run"] / "reconciliation.json").read_text())

    first = output.loc["market-one"]
    assert first["primary_transaction_hash"] == "tx-m1-bot"
    assert first["primary_home_probability"] == pytest.approx(0.70)
    assert bool(first["primary_buyer_is_flagged_bot"]) is True
    assert first["sensitivity_transaction_hash"] == "tx-m1-human"
    assert first["sensitivity_home_probability"] == pytest.approx(0.60)

    counterparty = output.loc["market-two"]
    assert counterparty["primary_transaction_hash"] == "tx-m2-bot-counterparty"
    assert counterparty["sensitivity_transaction_hash"] == "tx-m2-bot-counterparty"
    assert bool(counterparty["sensitivity_buyer_is_flagged_bot"]) is False
    assert bool(counterparty["sensitivity_counterparty_is_flagged_bot"]) is True
    assert counterparty["sensitivity_home_probability"] == pytest.approx(0.75)

    only_bot = output.loc["market-three"]
    assert bool(only_bot["primary_has_close"]) is True
    assert bool(only_bot["sensitivity_has_close"]) is False
    assert only_bot["sensitivity_missing_reason"] == (
        "all_strict_price_pregame_fills_have_flagged_bot_buyer"
    )

    extreme = output.loc["market-four"]
    assert extreme["primary_price"] == pytest.approx(0.995)
    assert bool(extreme["primary_strict_price_eligible"]) is False
    assert extreme["sensitivity_price"] == pytest.approx(0.60)

    no_fill = output.loc["market-five"]
    assert bool(no_fill["primary_has_close"]) is False
    assert no_fill["primary_missing_reason"] == "no_resolved_fill"
    assert bool(no_fill["sensitivity_has_close"]) is False
    assert no_fill["sensitivity_missing_reason"] == "no_resolved_fill"

    assert report["counts"]["primary_closes"] == 4
    assert report["counts"]["sensitivity_closes"] == 3
    assert report["counts"]["both_closes"] == 3
    assert report["counts"]["primary_only"] == 1
    assert report["counts"]["sensitivity_only"] == 0
    assert report["counts"]["same_close_identity"] == 1
    assert report["counts"]["different_close_identity"] == 2
    assert report["counts"]["sensitivity_closes_with_flagged_bot_counterparty"] == 1
    assert report["reconciliation"] == {
        "all_close_timestamps_from_exact_cache": True,
        "close_availability_partitions_reconcile": True,
        "distinct_fills_equal_per_game_raw_sum": True,
        "output_one_row_per_eligible_game": True,
        "raw_equals_distinct_plus_replays": True,
        "same_and_different_identity_partition_both_closes": True,
        "sensitivity_is_a_subset_of_primary": True,
    }


def test_dual_closes_reject_contradictory_immutable_identity(tmp_path: Path) -> None:
    rows, timestamps = _base_rows()
    contradiction = rows[0].copy()
    contradiction["taker_amount_filled"] = 450_000
    paths = _write_inputs(
        tmp_path, rows=[*rows, contradiction], timestamps=timestamps
    )

    with pytest.raises(DualCloseBuildError, match="Immutable EVM identities"):
        _build(paths)
    assert not paths["run"].exists()


def test_dual_closes_reject_token_mapping_mismatch(tmp_path: Path) -> None:
    rows, timestamps = _base_rows()
    rows[0]["maker_asset_id"] = "not-an-eligible-token"
    paths = _write_inputs(tmp_path, rows=rows, timestamps=timestamps)

    with pytest.raises(DualCloseBuildError, match="token IDs absent"):
        _build(paths)
    assert not paths["run"].exists()


def test_dual_closes_fail_when_exact_cache_lacks_source_block(tmp_path: Path) -> None:
    rows, timestamps = _base_rows()
    del timestamps[105]
    paths = _write_inputs(tmp_path, rows=rows, timestamps=timestamps)

    with pytest.raises(DualCloseBuildError, match="missing 1 eligible-source blocks"):
        _build(paths)
    assert not paths["run"].exists()


def test_dual_closes_are_atomic_and_never_overwrite(tmp_path: Path) -> None:
    paths = _write_inputs(tmp_path)
    _build(paths)
    before = {
        path.name: path.read_bytes() for path in paths["run"].iterdir() if path.is_file()
    }

    with pytest.raises(FileExistsError, match="already exists"):
        _build(paths)

    after = {
        path.name: path.read_bytes() for path in paths["run"].iterdir() if path.is_file()
    }
    assert after == before
