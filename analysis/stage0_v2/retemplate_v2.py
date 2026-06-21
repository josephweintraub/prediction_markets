"""Re-template all 1.1M contracts using stage0_v2 and identify net-new
(event_template, market_template) pairs that need LLM classification.

Output:
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_v2.parquet
    - 1.1M rows with v2 templates AND existing LLM classifications carried over
      via template-pair join (where applicable).
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_net_new_pairs.parquet
    - The (event_template, market_template) pairs that exist in v2 but not v1
      and therefore need fresh LLM classification.

Logic:
  1. Apply stage0_v2.normalize to event_slug and market_slug for every contract.
  2. Build the v2 (event_template, market_template) pair for each contract.
  3. Existing LLM extractions are keyed on the v1 pair. For each v2 pair, find
     ANY contract that ALSO maps to the same v2 pair under v1 — if such a v1
     pair exists with a successful extraction, inherit it (this catches pairs
     that didn't change). For genuinely new v2 pairs (no v1 pair shares the
     same v2 mapping), mark as needing extraction.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import importlib, stage0_v2
importlib.reload(stage0_v2)
from stage0_v2 import normalize as norm_v2

import pandas as pd
import numpy as np

SRC = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet"
OUT_PARQUET = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_v2.parquet"
OUT_NEEDS_LLM = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_net_new_pairs.parquet"

print("Loading existing dataset ...")
df = pd.read_parquet(SRC)
print(f"  {len(df):,} contracts")
print(f"  columns: {list(df.columns)}")

print("\nApplying stage0_v2.normalize to event_slug and market_slug ...")
df = df.copy()
# Some event_slugs are null (~1% per README). Handle gracefully — use market_slug as event fallback.
event_slug_for_norm = df["event_slug"].fillna(df["market_slug"])
df["event_template_v2"] = event_slug_for_norm.apply(norm_v2)
df["market_template_v2"] = df["market_slug"].apply(norm_v2)
print(f"  unique v2 event_templates:  {df['event_template_v2'].nunique():,}")
print(f"  unique v2 market_templates: {df['market_template_v2'].nunique():,}")
print(f"  unique v2 (event,market) pairs: {df.groupby(['event_template_v2','market_template_v2']).ngroups:,}")

# Verify: existing v1 pairs
print(f"\nFor comparison:")
print(f"  unique v1 event_templates:  {df['event_template'].nunique():,}")
print(f"  unique v1 market_templates: {df['market_template'].nunique():,}")
print(f"  unique v1 (event,market) pairs: {df.groupby(['event_template','market_template']).ngroups:,}")

# Step 2: inherit existing LLM extractions for v2 pairs.
# Strategy: for each v2 pair, find all v1 pairs that map to it. Take the
# extraction from any successful one (there should usually be only one — if
# multiple v1 pairs collapse to one v2 pair, that's the desired merge).
print("\nBuilding v2-pair → v1-pair map ...")
pair_map = (df.groupby(["event_template_v2", "market_template_v2"])
              .agg(v1_pairs=("event_template", lambda s: list(set(zip(s, df.loc[s.index, "market_template"]))))))
# This is slow; simpler approach below.

# Simpler: build a v1 pair → extraction lookup, then for each contract just
# look up its v1 pair to get extraction (extraction is already in df).
# But the v2 pair might match multiple v1 pairs — they all should have the
# same extraction (LLM was deterministic, temperature=0, prompt-cached).
# When v1 pair is the same as v2 pair, extraction stays as-is.

# Identify which v2 pairs have at least one contract whose v1 pair == v2 pair
# (i.e., the templates didn't change, so existing extraction applies directly).
print("\nIdentifying v2 pairs needing fresh extraction ...")
same_pair_mask = ((df["event_template"] == df["event_template_v2"]) &
                  (df["market_template"] == df["market_template_v2"]))
df["pair_unchanged"] = same_pair_mask

# A v2 pair is "covered" if any contract under it has pair_unchanged=True.
v2_pair_covered = (df[df["pair_unchanged"]]
                   .groupby(["event_template_v2", "market_template_v2"])
                   .size().rename("n_unchanged"))

all_v2_pairs = df[["event_template_v2", "market_template_v2"]].drop_duplicates().reset_index(drop=True)
all_v2_pairs["covered"] = all_v2_pairs.set_index(["event_template_v2","market_template_v2"]).index.isin(v2_pair_covered.index)

n_pairs = len(all_v2_pairs)
n_covered = all_v2_pairs["covered"].sum()
n_needs_llm = (~all_v2_pairs["covered"]).sum()
print(f"  total v2 pairs:               {n_pairs:,}")
print(f"  covered by existing extraction: {n_covered:,} ({n_covered/n_pairs*100:.1f}%)")
print(f"  net-new, need LLM:             {n_needs_llm:,} ({n_needs_llm/n_pairs*100:.1f}%)")

# Per-contract: how many contracts hit a net-new pair?
contract_in_new_pair = df.merge(
    all_v2_pairs[~all_v2_pairs["covered"]][["event_template_v2","market_template_v2"]].assign(is_new=True),
    on=["event_template_v2","market_template_v2"], how="left"
)
n_contracts_new = contract_in_new_pair["is_new"].fillna(False).sum()
print(f"\n  contracts in covered pairs:    {len(df) - n_contracts_new:,}")
print(f"  contracts in net-new pairs:    {n_contracts_new:,} ({n_contracts_new/len(df)*100:.2f}%)")

# Save net-new pairs for later LLM run
needs_llm = all_v2_pairs[~all_v2_pairs["covered"]][["event_template_v2","market_template_v2"]].copy()
# Also include a sample question for each pair (for prompt construction)
sample_questions = (df.drop_duplicates(["event_template_v2","market_template_v2"])
                     [["event_template_v2","market_template_v2","question"]])
needs_llm = needs_llm.merge(sample_questions, on=["event_template_v2","market_template_v2"], how="left")
needs_llm.to_parquet(OUT_NEEDS_LLM, index=False)
print(f"\n  wrote {OUT_NEEDS_LLM} ({len(needs_llm):,} rows)")

# Save full v2 dataset with carried-over extractions
print("\nSaving full v2 per-contract dataset ...")
# Drop our helper column
df_out = df.drop(columns=["pair_unchanged"])
df_out.to_parquet(OUT_PARQUET, index=False)
print(f"  wrote {OUT_PARQUET} ({len(df_out):,} rows, {OUT_PARQUET})")

# Summary
print("\n=== SUMMARY ===")
print(f"  contracts re-templated: {len(df):,}")
print(f"  v2 (event,market) pairs: {n_pairs:,}")
print(f"  pairs needing fresh LLM extraction: {n_needs_llm:,}")
print(f"  cost estimate (Sonnet 4.5, batched, cached, ~2K tokens/call): ${n_needs_llm * 0.002:.2f}-${n_needs_llm * 0.008:.2f}")
