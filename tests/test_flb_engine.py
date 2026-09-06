from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd


ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "calibration_heterogeneity"
sys.path.insert(0, str(ENGINE_DIR))

from flb_engine import (  # noqa: E402
    add_multiple_testing_columns,
    adjust_pvalues,
    cluster_se_difference,
    compute_slice,
)


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


def test_equal_market_vwap_is_invariant_to_replicating_one_markets_trades() -> None:
    frame = pd.DataFrame(
        {
            "price": [0.1, 0.1, 0.1, 0.1],
            "ret": [-0.1, -0.1, 0.3, 0.3],
            "won": [0.0, 0.0, 0.4, 0.4],
            "usdc": [1.0, 3.0, 1.0, 3.0],
            "day": [1, 2, 1, 2],
            "wallet_code": [1, 2, 3, 4],
            "market_code": [1, 1, 2, 2],
            "token_code": [1, 1, 2, 2],
            "decile": [1, 1, 1, 1],
        }
    )
    replicated = pd.concat(
        [frame, *[frame[frame["market_code"] == 1]] * 4], ignore_index=True
    )

    original, _ = compute_slice(frame, min_decile_trades=1)
    expanded, _ = compute_slice(replicated, min_decile_trades=1)

    assert not np.isclose(original[0]["cal_error"], expanded[0]["cal_error"])
    assert np.isclose(original[0]["cal_error_mkt"], 0.1)
    assert np.isclose(expanded[0]["cal_error_mkt"], 0.1)
    assert expanded[0]["n_markets"] == 2


def test_multiple_testing_columns_use_declared_families() -> None:
    assert np.allclose(
        adjust_pvalues([0.01, 0.04, np.nan], "bonferroni"),
        [0.02, 0.08, np.nan],
        equal_nan=True,
    )
    assert np.allclose(
        adjust_pvalues([0.01, 0.04, 0.03], "fdr_bh"),
        [0.03, 0.04, 0.04],
    )

    deciles = pd.DataFrame(
        {
            "scheme": ["a", "a"],
            "cal_error": [0.2, 0.1], "se": [0.1, 0.1],
            "cal_error_dol": [0.2, 0.1], "se_dol": [0.1, 0.1],
            "cal_error_mkt": [0.2, 0.1], "se_mkt": [0.1, 0.1],
        }
    )
    summaries = pd.DataFrame(
        {
            "scheme": ["a", "a"],
            "slope": [0.2, 0.1], "slope_se": [0.1, 0.1],
            "slope_dol": [0.2, 0.1], "slope_se_dol": [0.1, 0.1],
            "spread": [0.2, 0.1], "spread_se": [0.1, 0.1],
            "spread_dol": [0.2, 0.1], "spread_se_dol": [0.1, 0.1],
            "spread_mkt": [0.2, 0.1], "spread_se_mkt": [0.1, 0.1],
        }
    )

    adjusted_deciles, adjusted_summaries = add_multiple_testing_columns(
        deciles, summaries
    )

    assert "cal_mkt_p_bonferroni" in adjusted_deciles
    assert "spread_mkt_p_fdr_bh" in adjusted_summaries
    assert np.all(
        adjusted_deciles["cal_p_bonferroni"] >= adjusted_deciles["cal_p"]
    )
