"""Inspect the existing dataset to recover the current Stage 0 rules empirically.

We have 620K unique (raw_slug, template) pairs. By examining which substitutions
occurred, we can recover the full rule set."""
import re
from collections import Counter
import pandas as pd

df = pd.read_parquet(
    "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet",
    columns=["market_slug", "market_template", "event_slug", "event_template"]
)

# Build distinct (slug, template) pairs for market level
mkt = df[["market_slug","market_template"]].drop_duplicates().dropna()
print(f"unique market (slug,template) pairs: {len(mkt):,}")

# === Known sports leagues (anything with <TEAM>-<TEAM>) ===
print("\n=== KNOWN-LEAGUES list (template prefix before <TEAM>-<TEAM>) ===")
team_tmpls = df[df["market_template"].str.contains("<TEAM>-<TEAM>", na=False)]["market_template"].drop_duplicates()
prefixes = Counter()
for t in team_tmpls:
    m = re.match(r"^([a-z0-9]+)-<TEAM>-<TEAM>", t)
    if m:
        prefixes[m.group(1)] += 1
for p, c in prefixes.most_common(60):
    # Get sample slug for this prefix
    sample = df[df["market_template"].str.startswith(f"{p}-<TEAM>-<TEAM>", na=False)]["market_slug"].iloc[0]
    n_markets = df[df["market_template"].str.startswith(f"{p}-<TEAM>-<TEAM>", na=False)].shape[0]
    print(f"  {p:>10}  ({c:>4} templates, {n_markets:>6} markets)  e.g. {sample}")

# === Date token rules: find substitutions involving <DATE> ===
print("\n=== <DATE> substitution evidence (slug, template) pairs where <DATE> appears in template ===")
date_rows = mkt[mkt["market_template"].str.contains("<DATE>", na=False)].head(50)
sample_pairs = date_rows.sample(20, random_state=1)
for _, r in sample_pairs.iterrows():
    print(f"  RAW: {r['market_slug']}")
    print(f"  TMPL: {r['market_template']}")
    print()

# === Compare slug vs template character by character on a few cases ===
print("\n=== Sample slug → template transformations (numeric / date / time substitutions) ===")
for _, r in mkt.sample(20, random_state=42).iterrows():
    if r['market_slug'] != r['market_template']:
        print(f"  RAW : {r['market_slug']}")
        print(f"  TMPL: {r['market_template']}")
        print()

# === What patterns become <NUM>? ===
print("\n=== Templates with <NUM>: what came from the raw slug? ===")
num_examples = []
for _, r in mkt.sample(2000, random_state=42).iterrows():
    if "<NUM>" in r['market_template']:
        num_examples.append((r['market_slug'], r['market_template']))
print(f"  {len(num_examples)} examples with <NUM>:")
for s, t in num_examples[:8]:
    print(f"  RAW : {s}")
    print(f"  TMPL: {t}")

# === What patterns become <TIME>? ===
print("\n=== Templates with <TIME>: what came from the raw slug? ===")
time_examples = []
for _, r in mkt.sample(20000, random_state=42).iterrows():
    if "<TIME>" in r['market_template']:
        time_examples.append((r['market_slug'], r['market_template']))
print(f"  {len(time_examples)} examples found:")
for s, t in time_examples[:8]:
    print(f"  RAW : {s}")
    print(f"  TMPL: {t}")

# === Check duplicate- prefix removal ===
print("\n=== duplicate- prefix examples ===")
dup_examples = []
for _, r in mkt.iterrows():
    if isinstance(r['market_slug'], str) and r['market_slug'].startswith("duplicate-"):
        dup_examples.append((r['market_slug'], r['market_template']))
        if len(dup_examples) > 5:
            break
print(f"  {len(dup_examples)} examples with duplicate- prefix:")
for s, t in dup_examples[:5]:
    print(f"  RAW : {s}")
    print(f"  TMPL: {t}")

# === Trailing hash suffix examples ===
print("\n=== Trailing hash-suffix (multi-digit) examples ===")
hash_examples = []
for _, r in mkt.sample(50000, random_state=42).iterrows():
    if isinstance(r['market_slug'], str) and re.search(r"-\d{2,}-\d{2,}-\d{2,}", r['market_slug']):
        hash_examples.append((r['market_slug'], r['market_template']))
        if len(hash_examples) > 5:
            break
for s, t in hash_examples[:5]:
    print(f"  RAW : {s}")
    print(f"  TMPL: {t}")

# === top5, top10 collapsing ===
print("\n=== top<NUM> examples ===")
top_examples = []
for _, r in mkt.iterrows():
    if isinstance(r['market_template'], str) and "top<NUM>" in r['market_template']:
        top_examples.append((r['market_slug'], r['market_template']))
        if len(top_examples) > 5:
            break
for s, t in top_examples[:5]:
    print(f"  RAW : {s}")
    print(f"  TMPL: {t}")

# === Identity (raw == template) rate ===
identity = (mkt['market_slug'] == mkt['market_template']).sum()
print(f"\n=== Identity rate: {identity:,}/{len(mkt):,} = {identity/len(mkt)*100:.1f}% slugs unchanged ===")
