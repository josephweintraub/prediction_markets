from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

from build_market_universe import build_market_universe  # noqa: E402


def _market(
    market_id: str,
    event_slug: str | None,
    question: str | None,
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "event_slug": event_slug,
        "question": question,
        "n_tokens": 2,
        "n_trades_raw": 100,
        "n_buy_filtered": 60,
        "usd_buy_filtered": 1_250.0,
        "first_trade_at": pd.Timestamp("2025-03-01T12:00:00Z"),
        "last_trade_at": pd.Timestamp("2025-04-01T23:00:00Z"),
    }


def test_mlb_market_builder_keeps_simple_moneylines_and_diagnostics(tmp_path: Path) -> None:
    markets = pd.DataFrame(
        [
            _market("moneyline", "mlb-nyy-bos-2025-04-01", "Yankees vs. Red Sox"),
            _market("prop", "mlb-nyy-bos-2025-04-01", "Yankees vs. Red Sox: Runs"),
            _market("bad-shape", "mlb-new-york-bos-2025-04-02", "New York vs. Boston"),
            _market("bad-date", "mlb-nyy-bos-2025-02-31", "Yankees vs. Red Sox"),
            _market("missing-q", "mlb-lad-sf-2025-04-03", None),
            _market("other-sport", "nba-ny-bos-2025-04-01", "Knicks vs. Celtics"),
        ]
    )
    con = duckdb.connect()
    con.register("markets", markets)
    output = tmp_path / "mlb_moneylines.parquet"
    diagnostics = tmp_path / "mlb_market_diagnostics.parquet"

    stats = build_market_universe(con, "markets", output, diagnostics)
    selected = con.execute(f"SELECT * FROM read_parquet('{output}')").fetchdf()
    audited = con.execute(
        f"SELECT market_id, exclusion_reason, is_candidate "
        f"FROM read_parquet('{diagnostics}') ORDER BY market_id"
    ).fetchdf()
    con.close()

    assert stats == {"candidate_markets": 1, "diagnostic_rows": 5, "excluded_rows": 4}
    assert selected["market_id"].tolist() == ["moneyline"]
    assert selected["away"].tolist() == ["nyy"]
    assert selected["home"].tolist() == ["bos"]
    assert str(selected.loc[0, "date"].date()) == "2025-04-01"
    reasons = audited.set_index("market_id")["exclusion_reason"].to_dict()
    assert reasons["prop"] == "question_contains_colon"
    assert reasons["bad-shape"] == "slug_pattern_mismatch"
    assert reasons["bad-date"] == "invalid_date"
    assert reasons["missing-q"] == "missing_question"


def test_mlb_market_builder_fails_on_duplicate_event_candidates(tmp_path: Path) -> None:
    markets = pd.DataFrame(
        [
            _market("market-a", "mlb-nyy-bos-2025-04-01", "Yankees vs. Red Sox"),
            _market("market-b", "mlb-nyy-bos-2025-04-01", "New York vs. Boston"),
        ]
    )
    con = duckdb.connect()
    con.register("markets", markets)

    with pytest.raises(ValueError, match="refusing to guess.*market-a.*market-b"):
        build_market_universe(
            con,
            "markets",
            tmp_path / "candidates.parquet",
            tmp_path / "diagnostics.parquet",
        )
    con.close()

    assert not (tmp_path / "candidates.parquet").exists()
    assert not (tmp_path / "diagnostics.parquet").exists()
