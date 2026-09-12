from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import pytest


from analysis.nba_game_dynamics.build_market_universe import (
    CANDIDATE_OUTPUT,
    DIAGNOSTIC_OUTPUT,
    MANIFEST_OUTPUT,
    build_market_universe,
    verify_market_universe_run,
)


def _market(market_id: str, slug: str, question: str, tokens: int = 2) -> dict:
    return {
        "market_id": market_id, "event_slug": slug, "question": question,
        "n_tokens": tokens, "n_trades_raw": 100, "n_buy_filtered": 40,
        "usd_buy_filtered": 500.0,
        "first_trade_at": pd.Timestamp("2025-03-01T00:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-03-13T00:00:00Z"),
    }


def test_selector_excludes_totals_props_bad_tokens_and_keeps_diagnostics(tmp_path: Path) -> None:
    rows = [
        _market("moneyline", "nba-nyk-por-2025-03-12", "Knicks vs. Trail Blazers"),
        _market("total", "nba-nyk-por-2025-03-12", "Over 224.5"),
        _market("prop", "nba-nyk-por-2025-03-12", "Knicks vs. Trail Blazers: Points"),
        _market("one-token", "nba-bos-lal-2025-03-12", "Celtics vs. Lakers", 1),
        _market("other", "mlb-nyy-bos-2025-03-12", "Yankees vs. Red Sox"),
    ]
    source = tmp_path / "markets.parquet"
    frame = pd.DataFrame(rows)
    frame.to_parquet(source)
    con = duckdb.connect()
    con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source}')")
    run = tmp_path / "universe"
    stats = build_market_universe(con, "markets", source, run)
    selected = con.execute(f"SELECT * FROM read_parquet('{run / CANDIDATE_OUTPUT}')").fetchdf()
    reasons = con.execute(
        f"SELECT market_id, exclusion_reason FROM read_parquet('{run / DIAGNOSTIC_OUTPUT}')"
    ).fetchdf().set_index("market_id")["exclusion_reason"].to_dict()
    con.close()

    assert stats == {"candidate_markets": 1, "diagnostic_rows": 4, "excluded_rows": 3}
    assert selected["market_id"].tolist() == ["moneyline"]
    assert (selected.loc[0, "away"], selected.loc[0, "home"]) == ("nyk", "por")
    assert reasons["total"] == "question_not_two_team_matchup"
    assert reasons["prop"] == "question_not_two_team_matchup"
    assert reasons["one-token"] == "not_two_tokens"
    assert {path.name for path in run.iterdir()} == {
        CANDIDATE_OUTPUT, DIAGNOSTIC_OUTPUT, MANIFEST_OUTPUT,
    }
    assert verify_market_universe_run(run) == stats


def test_selector_retains_and_excludes_both_duplicate_moneylines(tmp_path: Path) -> None:
    rows = [
        _market("a", "nba-den-okc-2025-05-07", "Nuggets vs Thunder"),
        _market("b", "nba-den-okc-2025-05-07", "Nuggets vs. Thunder"),
    ]
    source = tmp_path / "markets.parquet"
    pd.DataFrame(rows).to_parquet(source)
    con = duckdb.connect()
    con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source}')")
    run = tmp_path / "universe"
    stats = build_market_universe(con, "markets", source, run)
    candidates = con.execute(
        f"SELECT market_id FROM read_parquet('{run / CANDIDATE_OUTPUT}')"
    ).fetchall()
    diagnostics = con.execute(
        f"SELECT market_id,exclusion_reason,is_candidate "
        f"FROM read_parquet('{run / DIAGNOSTIC_OUTPUT}') ORDER BY market_id"
    ).fetchall()
    con.close()
    assert stats == {"candidate_markets": 0, "diagnostic_rows": 2, "excluded_rows": 2}
    assert candidates == []
    assert diagnostics == [
        ("a", "duplicate_event_candidate", False),
        ("b", "duplicate_event_candidate", False),
    ]
    assert verify_market_universe_run(run) == stats


def test_stage_one_rejects_extra_serialized_column_and_cleans_staging(tmp_path: Path) -> None:
    source = tmp_path / "markets.parquet"
    pd.DataFrame([
        _market("moneyline", "nba-nyk-por-2025-03-12", "Knicks vs. Trail Blazers")
    ]).to_parquet(source)
    con = duckdb.connect()
    con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{source}')")
    run = tmp_path / "universe"
    build_market_universe(con, "markets", source, run)
    con.close()
    replacement = tmp_path / "extra.parquet"
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT *, 1 AS extra FROM read_parquet('{run / CANDIDATE_OUTPUT}')) "
        f"TO '{replacement}' (FORMAT PARQUET)"
    )
    con.close()
    replacement.replace(run / CANDIDATE_OUTPUT)
    with pytest.raises(ValueError, match="Fingerprint mismatch|schema mismatch"):
        verify_market_universe_run(run)
