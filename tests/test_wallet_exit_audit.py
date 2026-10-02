from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from analysis.diagnostics.wallet_exit_audit import (
    build_maker_actions, create_prior_links, create_summaries,
    parse_args, run_audit, sources_from_manifest,
)
from analysis.sports_game_dynamics.artifacts import write_parquet


RAW_SCHEMA = (
    ("maker", "VARCHAR"), ("taker", "VARCHAR"), ("maker_asset_id", "VARCHAR"),
    ("taker_asset_id", "VARCHAR"), ("maker_amount_filled", "BIGINT"),
    ("taker_amount_filled", "BIGINT"), ("fee", "BIGINT"), ("block_number", "BIGINT"),
    ("transaction_hash", "VARCHAR"), ("log_index", "INTEGER"),
    ("exchange_address", "VARCHAR"), ("condition_id", "VARCHAR"),
    ("outcome", "VARCHAR"), ("winning_outcome", "VARCHAR"),
)
TOKEN_SCHEMA = (("token_id", "VARCHAR"), ("condition_id", "VARCHAR"), ("outcome", "VARCHAR"))
CLOCK_SCHEMA = (
    ("sport", "VARCHAR"), ("event_id", "VARCHAR"), ("market_id", "VARCHAR"),
    ("market_date", "DATE"), ("actual_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ"),
)


def fill(wallet: str, side: str, token: str, block: int, price: float,
         *, tx: str | None = None, log: int = 0, fee: int = 0) -> tuple:
    amount = int(round(price * 1_000_000))
    return (
        wallet, "unclassified-counterparty", "0" if side == "BUY" else token,
        token if side == "BUY" else "0", amount if side == "BUY" else 1_000_000,
        1_000_000 if side == "BUY" else amount, fee, block, tx or f"tx-{block}-{log}",
        log, "exchange", "market", "winner" if token == "win" else "loser", "winner",
    )


def fixture_paths(tmp_path: Path, rows: list[tuple], timestamps: dict[int, int],
                  *, tokens: list[tuple] | None = None) -> tuple[Path, Path, Path, Path]:
    raw, token_map, cache, flags = (tmp_path/name for name in ("raw.parquet", "tokens.parquet", "cache.parquet", "flags.parquet"))
    write_parquet(raw, RAW_SCHEMA, rows, ("block_number", "log_index"))
    write_parquet(token_map, TOKEN_SCHEMA, tokens or [("win", "market", "winner"), ("lose", "market", "loser")], ("token_id",))
    write_parquet(cache, (("block_number", "BIGINT"), ("timestamp", "BIGINT")), list(timestamps.items()), ("block_number",))
    write_parquet(flags, (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN")), [("bot", True)], ("proxyWallet",))
    return raw, token_map, cache, flags


def clocks(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET TimeZone='UTC'")
    con.execute("""CREATE TABLE market_clocks AS SELECT 'atp' sport,'event' event_id,
       'market' market_id,DATE '1970-01-01' market_date,
       to_timestamp(1000) actual_start_utc,to_timestamp(2000) actual_end_utc""")


def test_own_maker_action_fees_and_partial_history_links(tmp_path: Path) -> None:
    rows = [
        # Boundary-price and bot actions are allowed in history.
        fill("BOT", "BUY", "win", 1, 1.0, fee=50_000),
        fill("bot", "SELL", "win", 2, 0.95, fee=10_000),
        fill("hedger", "BUY", "win", 3, 0.4),
        fill("hedger", "BUY", "lose", 4, 0.05),
        # An acquisition in the focal transaction is explicitly not a predecessor.
        fill("same-tx", "BUY", "win", 5, 0.4, tx="shared", log=3),
        fill("same-tx", "BUY", "lose", 5, 0.05, tx="shared", log=4),
        # A later BUY never links to an earlier SELL.
        fill("future", "SELL", "win", 6, 0.95),
        fill("future", "BUY", "win", 7, 0.4),
    ]
    rows.append(rows[0])  # Exact ingestion replay, not a second economic action.
    paths = fixture_paths(tmp_path, rows, {1: 900, 2: 1990, 3: 1500, 4: 1980, 5: 1990, 6: 1991, 7: 2010})
    con = duckdb.connect()
    try:
        clocks(con)
        counts = build_maker_actions(con, *paths)
        assert counts["distinct_source_fills"] == 8
        assert counts["exact_replay_rows_removed"] == 1
        assert con.execute("SELECT side,maker_token_delta,maker_cash_delta FROM maker_actions WHERE block_number=1").fetchone() == ("BUY", 0.95, -1.0)
        assert con.execute("SELECT side,maker_token_delta,maker_cash_delta FROM maker_actions WHERE block_number=2").fetchone() == ("SELL", -1.0, 0.94)
        assert con.execute("SELECT count(*) FROM maker_actions WHERE wallet='unclassified-counterparty'").fetchone()[0] == 0
        create_prior_links(con)
        assert con.execute("SELECT linked_prior_winner_buy FROM maker_prior_links WHERE block_number=4").fetchone()[0]
        assert not con.execute("SELECT linked_prior_winner_buy FROM maker_prior_links WHERE transaction_hash='shared' AND token_id='lose'").fetchone()[0]
        assert con.execute("SELECT prior_same_token_buy_count FROM maker_prior_links WHERE block_number=6").fetchone()[0] == 0
        assert con.execute("SELECT prior_same_token_buy_count FROM maker_prior_links WHERE block_number=2").fetchone()[0] == 1
        create_summaries(con)
        assert con.execute("SELECT n_fills FROM terminal_maker_summary WHERE sample='filtered_trades' AND window_id='t99_100' AND action_group='maker_winner_sells'").fetchone()[0] == 1
        assert con.execute("SELECT n_fills FROM terminal_maker_summary WHERE sample='all_trades' AND window_id='t99_100' AND action_group='maker_winner_sells'").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM linked_buy_profile").fetchone()[0] == 3 * 5 * 3 * 10
        assert con.execute("SELECT count(*) FROM linked_buy_profile WHERE suppressed AND calibration_equal_fill IS NOT NULL").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM linked_buy_tails WHERE suppressed AND (d1_error IS NOT NULL OR d10_error IS NOT NULL)").fetchone()[0] == 0
    finally:
        con.close()


@pytest.mark.parametrize("failure", ["conflicting_identity", "missing_block", "duplicate_block", "ambiguous_token", "winner", "exchange_facing"])
def test_reconciliation_gates_fail_closed(tmp_path: Path, failure: str) -> None:
    rows = [fill("wallet", "BUY", "win", 1, 0.4)]
    token_rows = None
    timestamps = {1: 1900}
    if failure == "conflicting_identity":
        rows.append(fill("other-wallet", "BUY", "win", 1, 0.4))
    elif failure == "missing_block":
        timestamps = {2: 1900}
    elif failure == "ambiguous_token":
        token_rows = [("win", "market", "winner"), ("lose", "market", "loser"), ("third", "market", "third")]
    elif failure == "winner":
        row = list(rows[0]); row[-1] = "unknown"; rows[0] = tuple(row)
    elif failure == "exchange_facing":
        row = list(rows[0]); row[1] = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"; rows[0] = tuple(row)
    paths = fixture_paths(tmp_path, rows, timestamps, tokens=token_rows)
    if failure == "duplicate_block":
        write_parquet(paths[2], (("block_number", "BIGINT"), ("timestamp", "BIGINT")), [(1, 1900), (1, 1901)], ("block_number",))
    con = duckdb.connect()
    try:
        clocks(con)
        with pytest.raises(ValueError):
            build_maker_actions(con, *paths)
    finally:
        con.close()


def test_same_block_distinct_transactions_link_and_literal_window_edges(tmp_path: Path) -> None:
    rows = [
        fill("wallet", "BUY", "win", 1, 0.4, tx="earlier", log=10),
        fill("wallet", "BUY", "lose", 1, 0.05, tx="later", log=20),
        fill("boundary", "BUY", "win", 2, 0.95),
        fill("boundary", "BUY", "win", 3, 0.95),
        fill("boundary", "BUY", "win", 4, 0.95),
        fill("boundary", "BUY", "win", 5, 0.95),
    ]
    paths = fixture_paths(tmp_path, rows, {1: 1800, 2: 1900, 3: 1950, 4: 1990, 5: 2000})
    con = duckdb.connect()
    try:
        clocks(con); build_maker_actions(con, *paths); create_prior_links(con); create_summaries(con)
        assert con.execute("SELECT linked_prior_winner_buy FROM maker_prior_links WHERE transaction_hash='later'").fetchone()[0]
        assert con.execute("SELECT seconds_since_latest_complement_buy FROM maker_prior_links WHERE transaction_hash='later'").fetchone()[0] == 0
        expected = {"t80_90": 2, "t90_95": 1, "t95_99": 1, "t99_100": 2}
        observed = dict(con.execute("SELECT window_id,n_fills FROM terminal_maker_summary WHERE sample='all_trades' AND action_group='all_maker_actions'").fetchall())
        for window, n in expected.items():
            assert observed[window] == n
        assert observed["last_120s"] == 4
    finally:
        con.close()


def test_supported_full_bins_and_equal_event_spread(tmp_path: Path) -> None:
    rows = [fill(f"wallet-{i}", "BUY", token, i * 2 + offset, price)
            for i in range(500) for token, offset, price in (("lose", 1, 0.05), ("win", 2, 0.95))]
    paths = fixture_paths(tmp_path, rows, {row[7]: 1990 for row in rows})
    con = duckdb.connect()
    try:
        clocks(con); build_maker_actions(con, *paths); create_prior_links(con); create_summaries(con)
        row = con.execute("""SELECT d1_n,d10_n,d1_error,d10_error,spread_equal_fill,
                  spread_dollar,paired_event_count,spread_equal_paired_event,suppressed
                  FROM linked_buy_tails WHERE sample='all_trades' AND window_id='t99_100'
                  AND buy_group='all_maker_buys'""").fetchone()
        assert row[:2] == (500, 500)
        assert row[2:6] == pytest.approx((-0.05, 0.05, 0.10, 0.10))
        assert row[6:8] == pytest.approx((1, 0.10))
        assert row[8] is False
    finally:
        con.close()


@pytest.mark.parametrize("flag_rows,valid", [
    ([("bot", True), ("BOT", True)], True),
    ([("bot", True), ("BOT", False)], False),
    ([("bot", None)], False),
])
def test_normalized_actor_flags_are_unambiguous(tmp_path: Path, flag_rows: list[tuple], valid: bool) -> None:
    paths = fixture_paths(tmp_path, [fill("wallet", "BUY", "win", 1, 0.4)], {1: 1900})
    write_parquet(paths[3], (("proxyWallet", "VARCHAR"), ("is_nonhuman", "BOOLEAN")), flag_rows, ("proxyWallet",))
    con = duckdb.connect()
    try:
        clocks(con)
        if valid:
            build_maker_actions(con, *paths)
            assert con.execute("SELECT count(*) FROM flags").fetchone()[0] == 1
        else:
            with pytest.raises(ValueError, match="flags|actor"):
                build_maker_actions(con, *paths)
    finally:
        con.close()


def test_paired_event_floor_uses_only_paired_fill_support(tmp_path: Path) -> None:
    rows = [fill(f"wallet-{i}", "BUY", token, i * 2 + offset, price)
            for i in range(500) for token, offset, price in (("lose", 1, 0.05), ("win", 2, 0.95))]
    paths = fixture_paths(tmp_path, rows, {row[7]: 1990 for row in rows})
    con = duckdb.connect()
    try:
        clocks(con); build_maker_actions(con, *paths); create_prior_links(con)
        con.execute("""UPDATE maker_prior_links SET event_cluster=CASE WHEN block_number<=200
                       THEN 'paired' WHEN token_id='win' THEN 'winner-only' ELSE 'loser-only' END""")
        create_summaries(con)
        row = con.execute("""SELECT d1_n,d10_n,suppressed,paired_d1_n,paired_d10_n,
                  paired_suppressed,spread_equal_paired_event,spread_equal_fill
                  FROM linked_buy_tails WHERE sample='all_trades' AND window_id='t99_100'
                  AND buy_group='all_maker_buys'""").fetchone()
        assert row[:6] == (500, 500, False, 100, 100, True)
        assert row[6] is None
        assert row[7] == pytest.approx(0.1)
    finally:
        con.close()


def test_manifest_resolution_and_atomic_full_run(tmp_path: Path) -> None:
    rows = [fill("wallet", "BUY", "win", 1, 0.4), fill("wallet", "BUY", "lose", 2, 0.05)]
    raw, token_map, cache, flags = fixture_paths(tmp_path, rows, {1: 1500, 2: 1990})
    start, end = datetime.fromtimestamp(1000, timezone.utc), datetime.fromtimestamp(2000, timezone.utc)
    new_dir = tmp_path/"new"; new_dir.mkdir()
    new = new_dir/"exact_buys.parquet"
    write_parquet(new, (("sport", "VARCHAR"), ("event_slug", "VARCHAR"), ("market_id", "VARCHAR"),
                       ("market_date", "DATE"), ("actual_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ")),
                  [("atp", "event", "market", start.date(), start, end)], ("market_id",))
    phases = []
    for sport, game in (("mlb", "game_pk"), ("nfl", "game_id"), ("nba", "game_id")):
        directory = tmp_path/sport; directory.mkdir()
        path = directory/"phase_trades.parquet"
        write_parquet(path, ((game, "VARCHAR"), ("market_id", "VARCHAR"), ("official_date", "DATE"),
                             ("actual_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ")), [], ("market_id",))
        phases.append(path)
    new_phase = new_dir/"phase_trades.parquet"
    ordered = [new_phase, *phases, new, raw, raw, raw, flags]
    manifest = tmp_path/"decay.json"
    manifest.write_text(json.dumps({"inputs": {f"input_{i:02d}": {"path": str(path)} for i,path in enumerate(ordered,1)}}))
    assert sources_from_manifest(manifest)["new_exact"] == new
    args = parse_args(["--decay-manifest", str(manifest), "--resolved-trades", str(raw),
                       "--token-map", str(token_map), "--block-timestamps", str(cache),
                       "--run-dir", str(tmp_path/"run"), "--memory-limit", "1GB", "--threads", "1"])
    result = run_audit(args)
    assert result["completion_status"] == "complete"
    assert result["counts"]["all_history_maker_actions"] == 2
    assert result["output_rows"]["linked_buy_tails"] == 3 * 5 * 3
    assert (tmp_path/"run"/"manifest.json").is_file()
    with pytest.raises(FileExistsError):
        run_audit(args)
