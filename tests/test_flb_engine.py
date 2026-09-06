from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "embedding_difficulty"
sys.path.insert(0, str(ENGINE_DIR))

from flb_engine import cluster_se_difference, compute_slice  # noqa: E402


def _tail_frame(n_low: int = 50, n_high: int = 50) -> pd.DataFrame:
    low_won = (np.arange(n_low) % 20 == 0).astype(float)
    high_won = (np.arange(n_high) % 20 != 0).astype(float)
    low = pd.DataFrame(
        {
            "price": 0.05,
            "ret": low_won - 0.05,
            "won": low_won,
            "usdc": 1.0,
            "day": np.arange(n_low),
            "wallet_code": np.arange(n_low),
            "market_code": np.arange(n_low),
            "token_code": np.arange(n_low),
            "decile": 1,
        }
    )
    high = pd.DataFrame(
        {
            "price": 0.95,
            "ret": high_won - 0.95,
            "won": high_won,
            "usdc": 1.0,
            "day": np.arange(n_high),
            "wallet_code": np.arange(n_high),
            "market_code": np.arange(n_high),
            "token_code": np.arange(n_high),
            "decile": 10,
        }
    )
    return pd.concat([low, high], ignore_index=True)


def test_spread_se_preserves_shared_cluster_covariance() -> None:
    signal = np.tile([-0.1, 0.1], 25)
    clusters = np.arange(50)

    se = cluster_se_difference(
        signal,
        signal + 0.05,
        clusters,
        clusters,
        clusters,
        clusters,
        clusters,
        clusters,
    )

    assert np.isclose(se, 0.0, atol=1e-15)


def test_summary_suppresses_spread_when_either_tail_is_thin() -> None:
    frame = _tail_frame(n_low=49, n_high=50)

    deciles, summary = compute_slice(frame)

    assert np.isnan(deciles[0]["cal_error"])
    assert np.isnan(summary["spread"])
    assert np.isnan(summary["spread_se"])
    assert summary["d1_n"] == 49
    assert summary["d10_n"] == 50


def test_summary_spread_matches_reported_tail_errors() -> None:
    frame = _tail_frame()

    deciles, summary = compute_slice(frame)

    expected = deciles[9]["cal_error"] - deciles[0]["cal_error"]
    assert np.isclose(summary["spread"], expected)
    assert summary["d1_n"] == deciles[0]["n"]
    assert summary["d10_n"] == deciles[9]["n"]
