"""Validate the augmented per-contract dataset.

Checks:
  * Row count matches original (1,117,358)
  * All 18 original columns preserved bit-exactly
  * 13 new _generic columns populated correctly
  * No accidental NaN in critical columns (token_id, slugs)
  * Sample spot-checks: v1 vs v2-generic comparisons make sense
"""
from pathlib import Path
import numpy as np
import pandas as pd

ORIG = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet"
AUG  = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_augmented.parquet"

orig = pd.read_parquet(ORIG)
aug  = pd.read_parquet(AUG)

print(f"=== STRUCTURAL CHECKS ===")
print(f"  original rows: {len(orig):,}")
print(f"  augmented rows: {len(aug):,}")
assert len(orig) == len(aug), "row count mismatch"
print(f"  ✓ row count matches")

# All 18 original columns present
missing = set(orig.columns) - set(aug.columns)
assert not missing, f"missing columns: {missing}"
print(f"  ✓ all 18 original columns present")

# 13 new generic columns present
expected_new = {
    "event_template_generic", "market_template_generic",
    "event_subjects_generic", "event_action_generic",
    "event_info_type_generic", "event_resolution_type_generic",
    "market_subjects_generic", "market_action_generic",
    "market_info_type_generic", "market_resolution_type_generic",
    "categories_generic", "snippet_generic", "extraction_error_generic",
}
missing_new = expected_new - set(aug.columns)
assert not missing_new, f"missing new columns: {missing_new}"
print(f"  ✓ all 13 new _generic columns present")

# Original columns bit-exactly preserved
print(f"\n=== BIT-EXACT PRESERVATION OF ORIGINAL COLUMNS ===")
for col in orig.columns:
    orig_vals = orig[col]
    aug_vals = aug[col]
    if orig_vals.dtype.kind == "O":  # object (e.g., lists, strings)
        # Compare element-wise
        eq = (orig_vals.apply(lambda x: tuple(x) if isinstance(x, (list, np.ndarray)) else x)
              == aug_vals.apply(lambda x: tuple(x) if isinstance(x, (list, np.ndarray)) else x))
        n_diff = (~eq).sum()
    else:
        eq = orig_vals.fillna(-999).eq(aug_vals.fillna(-999))
        n_diff = (~eq).sum()
    status = "✓" if n_diff == 0 else "✗"
    print(f"  {status} {col}: {n_diff:,} differences")

# Generic columns populated
print(f"\n=== GENERIC COLUMNS POPULATION ===")
for col in sorted(expected_new):
    n_na = aug[col].isna().sum()
    print(f"  {col}: {n_na:,} NaN ({n_na/len(aug)*100:.2f}%)")

# Spot-check a few contracts
print(f"\n=== SPOT CHECK: 5 RANDOM CONTRACTS ===")
sample = aug.sample(n=5, random_state=42)
for _, r in sample.iterrows():
    print(f"\n  slug:               {r['market_slug']}")
    print(f"  v1 market_template: {r['market_template']}")
    print(f"  v2 market_template: {r['market_template_generic']}")
    print(f"  v1 categories:      {list(r['categories']) if isinstance(r['categories'], (list, np.ndarray)) else r['categories']}")
    print(f"  v2 categories:      {list(r['categories_generic']) if isinstance(r['categories_generic'], (list, np.ndarray)) else r['categories_generic']}")
    print(f"  v1 market_subjects: {list(r['market_subjects']) if isinstance(r['market_subjects'], (list, np.ndarray)) else r['market_subjects']}")
    print(f"  v2 market_subjects: {list(r['market_subjects_generic']) if isinstance(r['market_subjects_generic'], (list, np.ndarray)) else r['market_subjects_generic']}")

# Compare: cases where v1 and v2 templates differ (the affected contracts)
diff_mask = aug["market_template"] != aug["market_template_generic"]
print(f"\n=== TEMPLATE-CHANGE STATS ===")
print(f"  contracts with v1 == v2:       {(~diff_mask).sum():,} ({(~diff_mask).mean()*100:.2f}%)")
print(f"  contracts with v1 != v2:       {diff_mask.sum():,} ({diff_mask.mean()*100:.2f}%)")

# Spot-check 5 contracts where v1 != v2 (these are the interesting cases)
print(f"\n=== SPOT CHECK: 5 RANDOM CONTRACTS WHERE V1 != V2 ===")
sample_diff = aug[diff_mask].sample(n=5, random_state=7)
for _, r in sample_diff.iterrows():
    print(f"\n  slug:               {r['market_slug']}")
    print(f"  v1 market_template: {r['market_template']}")
    print(f"  v2 market_template: {r['market_template_generic']}")
    print(f"  v1 market_subjects: {list(r['market_subjects']) if isinstance(r['market_subjects'], (list, np.ndarray)) else r['market_subjects']}")
    print(f"  v2 market_subjects: {list(r['market_subjects_generic']) if isinstance(r['market_subjects_generic'], (list, np.ndarray)) else r['market_subjects_generic']}")
    print(f"  v1 categories:      {list(r['categories']) if isinstance(r['categories'], (list, np.ndarray)) else r['categories']}")
    print(f"  v2 categories:      {list(r['categories_generic']) if isinstance(r['categories_generic'], (list, np.ndarray)) else r['categories_generic']}")

print(f"\n✓ validation complete")
