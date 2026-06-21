"""Rigorous delta analysis: apply v1 and v2 to all 620K unique market_slugs and
characterize every change.

Outputs:
  * Counts: total / unchanged / affected
  * Per-category breakdown of changes (MERGE / SPLIT / RENAME)
  * Sample affected slugs per change type
  * For each v1 template that maps to MULTIPLE v2 templates: is the split correct?
  * For each v2 template that catches MULTIPLE v1 templates: is the merge correct?
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import importlib, stage0_v1, stage0_v2
importlib.reload(stage0_v1)
importlib.reload(stage0_v2)
from stage0_v1 import normalize as norm_v1
from stage0_v2 import normalize as norm_v2

import pandas as pd
from collections import Counter

print("Loading per-contract dataset ...")
df = pd.read_parquet(
    "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet",
    columns=["market_slug", "market_template"]
).dropna()

mkt = df[["market_slug","market_template"]].drop_duplicates().copy()
print(f"unique slugs: {len(mkt):,}")

print("applying v1 ...")
mkt["v1"] = mkt["market_slug"].apply(norm_v1)
print("applying v2 ...")
mkt["v2"] = mkt["market_slug"].apply(norm_v2)

mkt["changed"] = mkt["v1"] != mkt["v2"]
print(f"\n=== SLUG-LEVEL DELTA ===")
print(f"  total slugs:    {len(mkt):,}")
print(f"  unchanged:      {(~mkt['changed']).sum():,} ({(~mkt['changed']).mean()*100:.2f}%)")
print(f"  CHANGED by v2:  {mkt['changed'].sum():,} ({mkt['changed'].mean()*100:.2f}%)")

# Map back to per-contract counts (1 slug can have many contracts)
slug_to_contracts = df.groupby("market_slug").size()
mkt_with_contracts = mkt.copy()
mkt_with_contracts["n_contracts"] = mkt_with_contracts["market_slug"].map(slug_to_contracts)
total_changed_contracts = mkt_with_contracts.loc[mkt_with_contracts["changed"], "n_contracts"].sum()
print(f"\n=== CONTRACT-LEVEL IMPACT ===")
print(f"  total contracts:  {len(df):,}")
print(f"  CHANGED contracts: {int(total_changed_contracts):,} ({total_changed_contracts/len(df)*100:.2f}%)")

# Template-level deltas
v1_tmpls = set(mkt["v1"].unique())
v2_tmpls = set(mkt["v2"].unique())
print(f"\n=== TEMPLATE COUNTS ===")
print(f"  v1 distinct templates: {len(v1_tmpls):,}")
print(f"  v2 distinct templates: {len(v2_tmpls):,}")
print(f"  net change:            {len(v2_tmpls) - len(v1_tmpls):+,}")
print(f"  templates only in v1:  {len(v1_tmpls - v2_tmpls):,}")
print(f"  templates only in v2:  {len(v2_tmpls - v1_tmpls):,}")
print(f"  templates in both:     {len(v1_tmpls & v2_tmpls):,}")

# === MERGE analysis: distinct v1 templates that now share a v2 template ===
print(f"\n=== MERGES (multiple v1 templates collapse into one v2 template) ===")
v2_to_v1set = mkt[mkt["changed"]].groupby("v2")["v1"].nunique().sort_values(ascending=False)
merges = v2_to_v1set[v2_to_v1set > 1]
print(f"  v2 templates that catch multiple v1 templates: {len(merges):,}")
print(f"  top 20 merges by # v1 templates absorbed:")
for v2_tmpl, n_v1s in merges.head(20).items():
    print(f"\n  V2: {v2_tmpl}  (absorbs {n_v1s} v1 templates)")
    v1_options = mkt[mkt["v2"]==v2_tmpl]["v1"].drop_duplicates().head(4).tolist()
    for v1t in v1_options:
        # sample raw slug
        sample = mkt[(mkt["v2"]==v2_tmpl) & (mkt["v1"]==v1t)]["market_slug"].iloc[0]
        print(f"    v1: {v1t}")
        print(f"        e.g. {sample}")

# === SPLIT analysis: a v1 template that now produces multiple v2 templates ===
print(f"\n=== SPLITS (one v1 template fragmenting into multiple v2 templates) ===")
v1_to_v2set = mkt[mkt["changed"]].groupby("v1")["v2"].nunique().sort_values(ascending=False)
splits = v1_to_v2set[v1_to_v2set > 1]
print(f"  v1 templates that fragment into multiple v2 templates: {len(splits):,}")
print(f"  top 20 splits by # v2 fragments:")
for v1_tmpl, n_v2s in splits.head(20).items():
    print(f"\n  V1: {v1_tmpl}  (splits into {n_v2s} v2 templates)")
    v2_options = mkt[mkt["v1"]==v1_tmpl]["v2"].drop_duplicates().head(4).tolist()
    for v2t in v2_options:
        sample = mkt[(mkt["v1"]==v1_tmpl) & (mkt["v2"]==v2t)]["market_slug"].iloc[0]
        print(f"    v2: {v2t}")
        print(f"        e.g. {sample}")

# === PURE RENAMES: v1 != v2 but neither in a merge nor a split ===
print(f"\n=== PURE RENAMES (1:1 template change without merging or splitting) ===")
changed = mkt[mkt["changed"]].copy()
merged_v2s = set(merges.index)
split_v1s = set(splits.index)
pure_renames = changed[~changed["v2"].isin(merged_v2s) & ~changed["v1"].isin(split_v1s)]
print(f"  slugs in pure renames: {len(pure_renames):,}")
rename_pairs = pure_renames[["v1","v2"]].drop_duplicates()
print(f"  distinct (v1, v2) rename pairs: {len(rename_pairs):,}")
print(f"  showing 20:")
for _, r in rename_pairs.head(20).iterrows():
    sample = pure_renames[(pure_renames["v1"]==r["v1"]) & (pure_renames["v2"]==r["v2"])]["market_slug"].iloc[0]
    print(f"    V1: {r['v1']}")
    print(f"    V2: {r['v2']}")
    print(f"    EX: {sample}\n")
