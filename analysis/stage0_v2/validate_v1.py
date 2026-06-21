"""Measure how well stage0_v1.normalize matches the existing templates."""
import importlib, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import stage0_v1
importlib.reload(stage0_v1)
from stage0_v1 import normalize

import pandas as pd

df = pd.read_parquet(
    "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet",
    columns=["market_slug", "market_template", "event_slug", "event_template"]
).dropna(subset=["market_slug","market_template"])

mkt = df[["market_slug","market_template"]].drop_duplicates()
print(f"unique (slug, template) pairs: {len(mkt):,}")

# Apply v1 normalizer
print("applying v1 normalizer...")
mkt = mkt.copy()
mkt["v1_template"] = mkt["market_slug"].apply(normalize)

# Compare
mkt["match"] = (mkt["v1_template"] == mkt["market_template"])
match_rate = mkt["match"].mean()
print(f"\nMATCH RATE: {match_rate*100:.2f}%  ({mkt['match'].sum():,} / {len(mkt):,})")

# Top 30 mismatches by frequency
print("\n=== TOP 30 MISMATCH PATTERNS (by raw count) ===")
mismatches = mkt[~mkt["match"]].copy()
mismatch_pairs = (mismatches.groupby(["market_template", "v1_template"]).size()
                  .reset_index(name="n").sort_values("n", ascending=False).head(30))
for _, r in mismatch_pairs.iterrows():
    print(f"\n  CUR : {r['market_template']}")
    print(f"  V1  : {r['v1_template']}")
    print(f"  COUNT: {r['n']:,}")
    sample = mismatches[(mismatches['market_template']==r['market_template']) & (mismatches['v1_template']==r['v1_template'])]['market_slug'].iloc[0]
    print(f"  EX  : {sample}")

# Diagnostic categorization
print("\n=== DIAGNOSTIC: what kinds of mismatches? ===")
mismatches["v1_has_team"] = mismatches["v1_template"].str.contains("<TEAM>", na=False)
mismatches["cur_has_team"] = mismatches["market_template"].str.contains("<TEAM>", na=False)
print(f"  CUR has <TEAM>, V1 doesn't:  {((~mismatches['v1_has_team']) & mismatches['cur_has_team']).sum():,}")
print(f"  V1 has <TEAM>, CUR doesn't:  {((mismatches['v1_has_team']) & (~mismatches['cur_has_team'])).sum():,}")
print(f"  Neither has <TEAM>:           {((~mismatches['v1_has_team']) & (~mismatches['cur_has_team'])).sum():,}")
print(f"  Both have <TEAM> but differ:  {((mismatches['v1_has_team']) & (mismatches['cur_has_team'])).sum():,}")
