"""Re-template all 1.1M contracts using stage0_v2 and refine which v2 pairs
genuinely need fresh LLM extraction vs. can safely inherit.

Categorization:
  UNCHANGED:  (e_v1, m_v1) == (e_v2, m_v2) for at least one contract. Extraction
              is bit-exact; no LLM needed.
  RENAME:     all contracts under this v2 pair share a single v1 pair, but that
              v1 pair differs from the v2 pair. Templates are 1:1 renamed
              (typically a trailing <DATE> added). Inherit safely from that v1
              pair — content unchanged, just placeholder added.
  MERGE:      multiple distinct v1 pairs collapse to this v2 pair. The v2
              template is more abstract (e.g., lal-mad-bar → lal-<TEAM>-<TEAM>).
              Inheriting any single v1 extraction would attach team-specific
              subjects to a generic template. **Needs fresh LLM.**
  SPLIT:      multiple v2 pairs come from a single v1 pair. Each v2 pair is
              more specific; inheriting the v1 extraction is reasonable but
              flag for review.

Outputs:
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_v2.parquet
  /Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_needs_llm.parquet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import importlib, stage0_v2
importlib.reload(stage0_v2)
from stage0_v2 import normalize as norm_v2

import pandas as pd

SRC = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet"
OUT_PARQUET = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract_v2.parquet"
OUT_NEEDS_LLM = "/Users/josephweintraub/prediction_markets/analysis/output/stage2_v2_needs_llm.parquet"

print("Loading existing dataset ...")
df = pd.read_parquet(SRC)
print(f"  {len(df):,} contracts")

print("\nApplying stage0_v2.normalize ...")
event_slug_for_norm = df["event_slug"].fillna(df["market_slug"])
df["event_template_v2"] = event_slug_for_norm.apply(norm_v2)
df["market_template_v2"] = df["market_slug"].apply(norm_v2)

# Build v2 → set(v1 pairs) map
print("\nCategorizing v2 pairs ...")
v2_to_v1pairs = (df.groupby(["event_template_v2","market_template_v2"])
                   .apply(lambda g: g[["event_template","market_template"]].drop_duplicates()
                          .apply(tuple, axis=1).tolist(), include_groups=False)
                   .reset_index(name="v1_pairs"))
v2_to_v1pairs["n_v1_pairs"] = v2_to_v1pairs["v1_pairs"].apply(len)
v2_to_v1pairs["has_unchanged"] = v2_to_v1pairs.apply(
    lambda r: (r["event_template_v2"], r["market_template_v2"]) in set(r["v1_pairs"]), axis=1)

def categorize(row):
    if row["has_unchanged"] and row["n_v1_pairs"] == 1:
        return "UNCHANGED"
    if row["has_unchanged"] and row["n_v1_pairs"] > 1:
        # Multiple v1 pairs collapse but one is identical to v2 — still a merge,
        # though we can carry the matching one's extraction (it's bit-exact).
        return "MERGE_with_match"
    if not row["has_unchanged"] and row["n_v1_pairs"] == 1:
        return "RENAME"
    return "MERGE"

v2_to_v1pairs["category"] = v2_to_v1pairs.apply(categorize, axis=1)
print(f"\nv2 pair categorization (count of v2 pairs):")
print(v2_to_v1pairs["category"].value_counts())

# Also break down by # of contracts
v2_pair_size = df.groupby(["event_template_v2","market_template_v2"]).size().rename("n_contracts").reset_index()
v2_to_v1pairs = v2_to_v1pairs.merge(v2_pair_size, on=["event_template_v2","market_template_v2"])

print(f"\nContract counts per category:")
print(v2_to_v1pairs.groupby("category")["n_contracts"].agg(["sum","count"]))

# Identify which v2 pairs need fresh LLM extraction:
# - MERGE: yes (different abstract template, generic subjects needed)
# - MERGE_with_match: technically have a matching v1 pair, but the same v2
#   template absorbed other v1 pairs. The matching v1's extraction is for the
#   SPECIFIC instance, not the generic template. To be safe, treat as needing
#   re-LLM (consistent with MERGE).
# - RENAME: safe to inherit (content unchanged, just placeholder added)
# - UNCHANGED: bit-exact inheritance
needs_llm_mask = v2_to_v1pairs["category"].isin(["MERGE", "MERGE_with_match"])
needs_llm = v2_to_v1pairs[needs_llm_mask][["event_template_v2","market_template_v2"]].copy()
print(f"\n=== NET-NEW PAIRS NEEDING FRESH LLM ===")
print(f"  v2 pairs requiring fresh LLM:    {len(needs_llm):,}")

# Get a sample question and slug for each (for prompt construction)
sample_per_pair = (df.drop_duplicates(["event_template_v2","market_template_v2"])
                    [["event_template_v2","market_template_v2","question","market_slug","event_slug"]])
needs_llm = needs_llm.merge(sample_per_pair, on=["event_template_v2","market_template_v2"], how="left")
needs_llm.to_parquet(OUT_NEEDS_LLM, index=False)
print(f"  wrote {OUT_NEEDS_LLM}")

# Carry over existing extractions to v2 dataset
# For each v2 pair, decide what extraction to carry:
# - UNCHANGED: the v1 pair == v2 pair extraction (bit-exact)
# - RENAME: the single v1 pair's extraction (carried 1:1)
# - MERGE / MERGE_with_match: leave extraction blank for now (will be filled by LLM rerun)

# For UNCHANGED and RENAME, build a v2 pair → v1 pair map (single v1 source per v2 pair).
inherit_map = (v2_to_v1pairs[v2_to_v1pairs["category"].isin(["UNCHANGED","RENAME"])]
               [["event_template_v2","market_template_v2","v1_pairs"]])
inherit_map["src_event_template"] = inherit_map["v1_pairs"].apply(lambda l: l[0][0])
inherit_map["src_market_template"] = inherit_map["v1_pairs"].apply(lambda l: l[0][1])
inherit_map = inherit_map.drop(columns=["v1_pairs"])

# Build v1-pair → extraction-row lookup
ext_cols = ["event_subjects","event_action","event_info_type","event_resolution_type",
            "market_subjects","market_action","market_info_type","market_resolution_type",
            "categories","snippet","extraction_error"]
v1_extractions = df.drop_duplicates(["event_template","market_template"])[
    ["event_template","market_template"] + ext_cols
].rename(columns={"event_template":"src_event_template",
                  "market_template":"src_market_template"})

# Join inherit_map with v1 extractions
inherit_map = inherit_map.merge(v1_extractions, on=["src_event_template","src_market_template"], how="left")

# Now apply to df: drop old extraction columns from df, then merge by (e_v2, m_v2)
df_out = df.drop(columns=ext_cols + ["event_template","market_template"]).rename(
    columns={"event_template_v2":"event_template","market_template_v2":"market_template"})

# Build new extractions table indexed on v2 pair
new_ext = inherit_map.rename(columns={"event_template_v2":"event_template",
                                       "market_template_v2":"market_template"})
new_ext = new_ext[["event_template","market_template"] + ext_cols]
df_out = df_out.merge(new_ext, on=["event_template","market_template"], how="left")

# Contracts under MERGE pairs will have NaN extraction columns (since we didn't
# inherit). They need to be filled in by LLM rerun.
n_missing = df_out["categories"].isna().sum()
print(f"\nContracts with missing extraction (under MERGE pairs): {n_missing:,} ({n_missing/len(df_out)*100:.2f}%)")

df_out.to_parquet(OUT_PARQUET, index=False)
print(f"\nwrote {OUT_PARQUET} ({len(df_out):,} rows)")

print(f"\n=== SUMMARY ===")
print(f"  contracts:                   {len(df_out):,}")
print(f"  v2 pairs total:              {len(v2_to_v1pairs):,}")
print(f"  UNCHANGED pairs:             {(v2_to_v1pairs['category']=='UNCHANGED').sum():,}")
print(f"  RENAME pairs (inherit):      {(v2_to_v1pairs['category']=='RENAME').sum():,}")
print(f"  MERGE pairs (need LLM):      {(v2_to_v1pairs['category']=='MERGE').sum():,}")
print(f"  MERGE_with_match (need LLM): {(v2_to_v1pairs['category']=='MERGE_with_match').sum():,}")
print(f"  TOTAL needing LLM:           {len(needs_llm):,}")
print(f"  est cost (batched+cached):   ${len(needs_llm) * 200 / 108000:.2f}")
