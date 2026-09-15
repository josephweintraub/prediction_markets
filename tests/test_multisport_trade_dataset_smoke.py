from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from analysis.multisport_game_dynamics.build_trade_dataset import build_trade_dataset
from analysis.sports_game_dynamics.artifacts import write_parquet


RAW_SCHEMA = (
    ("maker", "VARCHAR"),
    ("taker", "VARCHAR"),
    ("maker_asset_id", "VARCHAR"),
    ("taker_asset_id", "VARCHAR"),
    ("maker_amount_filled", "BIGINT"),
    ("taker_amount_filled", "BIGINT"),
    ("block_number", "BIGINT"),
    ("transaction_hash", "VARCHAR"),
    ("log_index", "INTEGER"),
    ("exchange_address", "VARCHAR"),
    ("condition_id", "VARCHAR"),
    ("outcome_token_side", "VARCHAR"),
)
TIMESTAMP_SCHEMA = (("block_number", "BIGINT"), ("timestamp", "BIGINT"))
WALLET_SCHEMA = (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN"))
MARKET_SCHEMA = (
    ("sport", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("market_id", "VARCHAR"),
    ("market_date", "DATE"),
)
TOKEN_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("token_id", "VARCHAR"),
    ("outcome", "VARCHAR"),
    ("won", "BOOLEAN"),
)
TIMING_SCHEMA = (
    ("sport", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("game_id", "VARCHAR"),
    ("market_date", "DATE"),
    ("actual_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("actual_end_utc", "TIMESTAMP WITH TIME ZONE"),
)
BOUNDARY_SCHEMA = (
    ("sport", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("phase", "VARCHAR"),
    ("phase_order", "INTEGER"),
    ("start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("end_utc", "TIMESTAMP WITH TIME ZONE"),
)


def _fill(
    *,
    block: int,
    market: str,
    token: str,
    buyer: str,
    price: float,
    token_side: str,
) -> tuple[Any, ...]:
    token_amount = 1_000_000
    usdc_amount = int(round(price * 1_000_000))
    if token_side == "taker":
        maker, taker = buyer, f"seller-{block}"
        maker_asset, taker_asset = "0", token
        maker_amount, taker_amount = usdc_amount, token_amount
    elif token_side == "maker":
        maker, taker = f"seller-{block}", buyer
        maker_asset, taker_asset = token, "0"
        maker_amount, taker_amount = token_amount, usdc_amount
    else:  # test fixture construction must itself be exact
        raise ValueError(token_side)
    return (
        maker,
        taker,
        maker_asset,
        taker_asset,
        maker_amount,
        taker_amount,
        block,
        f"transaction-{block}",
        0,
        "exchange",
        market,
        token_side,
    )


def _rows(path: Path, select: str, suffix: str = "") -> list[tuple[Any, ...]]:
    con = duckdb.connect()
    try:
        source = str(path.resolve()).replace("'", "''")
        return con.execute(
            f"SELECT {select} FROM read_parquet('{source}') {suffix}"
        ).fetchall()
    finally:
        con.close()


def test_trade_dataset_end_to_end_semantics_and_reconciliation(tmp_path: Path) -> None:
    sport = "atp"
    event = "atp-player-a-player-b-2025-01-01"
    market = "market-1"
    winner = "token-player-a"
    loser = "token-player-b"
    game = "provider-match-1"
    market_day = date(2025, 1, 1)
    start = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)
    end = start + timedelta(minutes=90)

    timed_fills = [
        # Nonbot filtered close and ordinary pregame phase row.
        (1, start - timedelta(minutes=15), loser, "human-pregame", 0.20, "taker"),
        # Later bot close: retained by all-trades close, excluded elsewhere.
        (2, start - timedelta(minutes=5), winner, "bot-wallet", 0.80, "maker"),
        # Exact start enters the first live phase.
        (3, start, winner, "human-live-1", 0.60, "taker"),
        # Half-open boundary equality enters the later live phase.
        (4, start + timedelta(minutes=30), loser, "human-live-2", 0.30, "maker"),
        # Exact final boundary remains in the final live phase.
        (5, end, winner, "human-live-3", 0.70, "taker"),
        # Post-final activity is retained only in exact_buys.
        (6, end + timedelta(seconds=1), loser, "human-post", 0.40, "maker"),
    ]
    raw_rows = [
        _fill(
            block=block,
            market=market,
            token=token,
            buyer=buyer,
            price=price,
            token_side=side,
        )
        for block, _, token, buyer, price, side in timed_fills
    ]
    raw_rows.append(raw_rows[0])  # one exact ingestion replay

    raw = tmp_path / "raw.parquet"
    timestamps = tmp_path / "timestamps.parquet"
    wallets = tmp_path / "wallets.parquet"
    candidates = tmp_path / "candidates"
    timing = tmp_path / "timing"
    candidates.mkdir()
    timing.mkdir()
    write_parquet(raw, RAW_SCHEMA, raw_rows, ("block_number", "transaction_hash"))
    write_parquet(
        timestamps,
        TIMESTAMP_SCHEMA,
        [(block, int(when.timestamp())) for block, when, *_ in timed_fills],
        ("block_number",),
    )
    write_parquet(
        wallets,
        WALLET_SCHEMA,
        [("bot-wallet", True), ("human-pregame", False)],
        ("proxyWallet",),
    )
    write_parquet(
        candidates / "candidate_markets.parquet",
        MARKET_SCHEMA,
        [(sport, event, market, market_day)],
        ("market_id",),
    )
    write_parquet(
        candidates / "candidate_tokens.parquet",
        TOKEN_SCHEMA,
        [
            (market, winner, "Player A", True),
            (market, loser, "Player B", False),
        ],
        ("market_id", "token_id"),
    )
    write_parquet(
        timing / "event_timing.parquet",
        TIMING_SCHEMA,
        [(sport, event, game, market_day, start, end)],
        ("sport", "event_slug"),
    )
    write_parquet(
        timing / "phase_boundaries.parquet",
        BOUNDARY_SCHEMA,
        [
            (sport, event, "elapsed_1", 2, start, start + timedelta(minutes=30)),
            (
                sport,
                event,
                "elapsed_2",
                3,
                start + timedelta(minutes=30),
                start + timedelta(minutes=60),
            ),
            (sport, event, "elapsed_3", 4, start + timedelta(minutes=60), end),
        ],
        ("sport", "event_slug", "phase_order"),
    )

    output = tmp_path / "trade_run"
    manifest = build_trade_dataset(raw, timestamps, wallets, candidates, timing, output)

    exact = output / "exact_buys.parquet"
    phase = output / "phase_trades.parquet"
    closes = output / "closing_lines.parquet"
    exact_rows = _rows(
        exact,
        "block_number,token_id,outcome,won,proxyWallet,buyer_is_flagged_nonhuman,price,usdc",
        "ORDER BY block_number",
    )
    assert len(exact_rows) == 6
    assert exact_rows[0] == (
        1,
        loser,
        "Player B",
        False,
        "human-pregame",
        False,
        pytest.approx(0.20),
        pytest.approx(0.20),
    )
    assert exact_rows[1] == (
        2,
        winner,
        "Player A",
        True,
        "bot-wallet",
        True,
        pytest.approx(0.80),
        pytest.approx(0.80),
    )

    phase_rows = _rows(
        phase,
        "block_number,phase,phase_order,won,price,calibration_error",
        "ORDER BY block_number",
    )
    assert phase_rows == [
        (1, "pregame", 1, False, pytest.approx(0.20), pytest.approx(-0.20)),
        (3, "elapsed_1", 2, True, pytest.approx(0.60), pytest.approx(0.40)),
        (4, "elapsed_2", 3, False, pytest.approx(0.30), pytest.approx(-0.30)),
        (5, "elapsed_3", 4, True, pytest.approx(0.70), pytest.approx(0.30)),
    ]
    assert 2 not in {row[0] for row in phase_rows}
    assert 6 not in {row[0] for row in phase_rows}

    close_rows = _rows(
        closes,
        "close_sample,block_number,token_id,outcome,won,proxyWallet,"
        "buyer_is_flagged_nonhuman,price,calibration_error,close_age_seconds",
        "ORDER BY close_sample",
    )
    assert close_rows == [
        (
            "all_trades",
            2,
            winner,
            "Player A",
            True,
            "bot-wallet",
            True,
            pytest.approx(0.80),
            pytest.approx(0.20),
            pytest.approx(300.0),
        ),
        (
            "filtered_trades",
            1,
            loser,
            "Player B",
            False,
            "human-pregame",
            False,
            pytest.approx(0.20),
            pytest.approx(-0.20),
            pytest.approx(900.0),
        ),
    ]

    counts = manifest["counts"]
    assert counts["raw_scoped_rows"] == 7
    assert counts["distinct_fills"] == 6
    assert counts["replay_duplicates"] == 1
    assert counts["exact_buy_rows"] == 6
    assert counts["missing_exact_blocks"] == 0
    assert counts["phase"] == {
        "rows": 4,
        "markets": 1,
        "events": 1,
        "dollars": pytest.approx(1.80),
    }
    assert counts["closing"] == {
        "rows": 2,
        "markets": 1,
        "events": 1,
        "dollars": pytest.approx(1.00),
    }
    assert counts["phase_rows_by_sport"] == {sport: 4}
    assert counts["closing_rows_by_sport"] == {sport: 2}
    assert counts["near_event_position_by_sport"] == {
        sport: {"pregame_6h": 2, "live": 3, "post_final_6h": 1}
    }
    assert counts["close_age_by_sport_sample"][f"{sport}:all_trades"] == {
        "n": 1,
        "median_seconds": pytest.approx(300.0),
        "p90_seconds": pytest.approx(300.0),
    }
    assert counts["close_age_by_sport_sample"][f"{sport}:filtered_trades"]["n"] == 1
    assert set(manifest["outputs"]) == {
        "exact_buys.parquet",
        "phase_trades.parquet",
        "closing_lines.parquet",
    }
