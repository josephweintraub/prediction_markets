"""v5 dimension additions on top of v4 contract_dimensions.

Changes from v4:
- Replace dim_text_novelty quintile-of-best_sim with FIXED-threshold bins
- Add dim_market_type (updown vs non_updown) as a permanent dim

Reuses everything else from v4 contract_dimensions parquet.
"""
from __future__ import annotations
import re
import numpy as np
import pandas as pd

UPDOWN_RE = re.compile(r"updown|up-or-down", re.IGNORECASE)

# Fixed semantic thresholds replace the empirical-quintile bins
TEXT_NOVELTY_BINS = [-np.inf, 0.50, 0.75, 0.90, 0.95, np.inf]
TEXT_NOVELTY_LABELS = [
    "<0.50 genuinely isolated",
    "0.50-0.75 mod isolated",
    "0.75-0.90 has neighbor",
    "0.90-0.95 close lex match",
    ">0.95 near duplicate",
]


def add_dim_text_novelty_v5(df: pd.DataFrame) -> pd.DataFrame:
    """Replace dim_text_novelty with fixed-threshold bins on best_sim."""
    df["dim_text_novelty"] = pd.cut(
        df["best_sim"], bins=TEXT_NOVELTY_BINS, labels=TEXT_NOVELTY_LABELS
    ).astype(str)
    # Keep dim_text_neighbors_strict from v4 unchanged
    return df


def add_dim_market_type(df: pd.DataFrame) -> pd.DataFrame:
    """Up/down marker. Excluded from primary trades view in v5; reported
    separately as a sensitivity slice."""
    df["dim_market_type"] = df["event_template"].fillna("").apply(
        lambda s: "updown" if UPDOWN_RE.search(s) else "non_updown"
    )
    return df


# v5 keeps the full list of v3 + v4 addon dims, with text_novelty replaced.
V5_DIMS = [
    # v3 (10)
    "dim_resolution_type",
    "dim_info_type_supergroup",
    "dim_primary_category",
    "dim_subject_specificity",
    "dim_event_family_size",
    "dim_outcomes_per_event",
    "dim_market_specificity",
    "dim_dollar_volume_tier",
    "dim_contract_horizon",
    "dim_recurrence_class",
    # v4 addons (8)
    "dim_group_strict_size",
    "dim_event_slug_size",
    "dim_family_vol_tier",
    "dim_family_size_x_vol",
    "dim_vol_per_contract_tier",
    "dim_vol_per_contract_residualized",
    "dim_text_novelty",  # NOW: fixed-threshold bins
    "dim_text_neighbors_strict",
    # v4 prior settlements (3)
    "dim_prior_settlements_bin__event_template",
    "dim_prior_settlements_bin__event_slug",
    "dim_prior_settlements_bin__dim_group_strict",
    # v5 new
    "dim_market_type",
]
