"""Comprehensive quality audit of all 7,153 prefix-level LLM inputs.

For each (event_template, market_template, question) triple that will be
submitted to the LLM, flag potential quality issues:

  F1. ABSTRACTION_MISS: multi-template prefix (n_templates >= 5) where the
      representative market_template has no <PLACEHOLDER> tokens. Suggests we
      missed a normalization pattern.
  F2. UNNORMALIZED_DATE: representative template still contains a bare
      month name not wrapped in <DATE>, e.g., "January 14, 2025" leaked
      through.
  F3. UNNORMALIZED_ISO_DATE: representative contains ISO datetime that
      didn't get caught.
  F4. UNNORMALIZED_NUMBER: representative template still has raw
      4+ digit numbers (likely a strike that should be <NUM>).
  F5. DOUBLE_SPACE_QUESTION: raw question has runs of 2+ spaces (cosmetic
      cleaned at build time, but flagged here for visibility).
  F6. VERY_LONG: market_template > 200 chars (often compound, hard for LLM
      to abstract).
  F7. CONCENTRATION_LOW: dominant template covers < 25% of prefix tickers.
      Suggests the prefix is genuinely heterogeneous and per-prefix LLM
      classification may lose signal.
  F8. NO_QUESTION: representative question empty / null.
  F9. PARTLY_NORMALIZED_GAME: template still says 'Spread Total Points' /
      'Total Points Total Points' (data-quality leak).

Output:
  kalshi_quality_flags.csv — one row per prefix with all flags set.
  Summary: top counts per flag.
"""
import re
import sys
from pathlib import Path

import pandas as pd

OUT_CSV = Path("/home/ubuntu/kalshi/kalshi_quality_flags.csv")
PAIRS_PARQUET = Path("/mnt/data/kalshi/kalshi_prefix_pairs.parquet")
TEMPLATES_PARQUET = Path("/mnt/data/kalshi/kalshi_templates.parquet")

# Load pair-level data
print(f">>> loading {PAIRS_PARQUET}")
df = pd.read_parquet(PAIRS_PARQUET)
print(f"    {len(df):,} prefix-level rows")

# Load full templates for concentration calculation
print(f">>> loading {TEMPLATES_PARQUET}")
all_templates = pd.read_parquet(TEMPLATES_PARQUET)
prefix_total = all_templates.groupby("event_template")["n_tickers"].sum().to_dict()
prefix_dominant = (all_templates.sort_values(["event_template", "n_tickers"], ascending=[True, False])
                   .groupby("event_template")["n_tickers"].first().to_dict())

# Flag detectors
PLACEHOLDER_RE   = re.compile(r"<[A-Z_]+>")
MONTH_RE         = re.compile(r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b")
ISO_DATETIME_RE  = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")
ISO_DATE_RE      = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
LARGE_NUM_RE     = re.compile(r"\b\d{4,}(?:\.\d+)?\b")
DOUBLE_SPACE_RE  = re.compile(r"\s{2,}")
DUP_PHRASE_RE    = re.compile(r"(Spread )?Total Points Total Points|Spread Total Points")

flags = []
for _, r in df.iterrows():
    pfx = r["event_template"]
    mt = r.get("market_template") or ""
    q  = r.get("question") or ""
    n_templates = int(r["prefix_n_templates"])
    n_tickers = int(r["prefix_n_tickers"])
    dom_tickers = prefix_dominant.get(pfx, 0)
    total = prefix_total.get(pfx, n_tickers)
    concentration = dom_tickers / total if total > 0 else 1.0

    rec = {
        "prefix": pfx,
        "n_tickers": n_tickers,
        "n_templates": n_templates,
        "concentration": round(concentration, 3),
        "market_template": mt[:200],
        "question": q[:200],
    }

    # F1 ABSTRACTION_MISS — multi-template + no placeholder
    if n_templates >= 5 and not PLACEHOLDER_RE.search(mt):
        rec["F1_abstraction_miss"] = True
    # F2 UNNORMALIZED_DATE — bare month in template
    if MONTH_RE.search(mt):
        rec["F2_unnormalized_date"] = True
    # F3 UNNORMALIZED_ISO_DATE
    if ISO_DATETIME_RE.search(mt) or ISO_DATE_RE.search(mt):
        rec["F3_unnormalized_iso"] = True
    # F4 UNNORMALIZED_NUMBER — raw 4+ digit number in template
    if LARGE_NUM_RE.search(mt):
        rec["F4_unnormalized_number"] = True
    # F5 DOUBLE_SPACE_QUESTION — checks raw question (will be cleaned in build)
    if DOUBLE_SPACE_RE.search(q):
        rec["F5_double_space"] = True
    # F6 VERY_LONG
    if len(mt) > 200:
        rec["F6_very_long"] = True
    # F7 CONCENTRATION_LOW
    if n_templates > 1 and concentration < 0.25:
        rec["F7_concentration_low"] = True
    # F8 NO_QUESTION
    if not q.strip():
        rec["F8_no_question"] = True
    # F9 PARTLY_NORMALIZED_GAME
    if DUP_PHRASE_RE.search(mt):
        rec["F9_dup_phrase"] = True

    flags.append(rec)

out = pd.DataFrame(flags)
flag_cols = [c for c in out.columns if c.startswith("F")]
out["any_flag"] = out[flag_cols].any(axis=1)

print(f"\n=== AUDIT SUMMARY ===")
print(f"  Total prefixes: {len(out):,}")
print(f"  Prefixes with any flag: {int(out['any_flag'].sum()):,} ({out['any_flag'].mean()*100:.1f}%)")
print()
print("  Flag counts:")
for c in flag_cols:
    n = int(out[c].sum())
    tickers_affected = int(out.loc[out[c].fillna(False), "n_tickers"].sum())
    print(f"    {c:30s} {n:>6,} prefixes  ({tickers_affected:>10,} tickers)")

# Top 30 flagged prefixes by ticker count, per category
for c in flag_cols:
    sub = out.loc[out[c].fillna(False)].sort_values("n_tickers", ascending=False).head(30)
    if len(sub) == 0:
        continue
    print(f"\n=== Top 30 by tickers — {c} ===")
    for _, r in sub.iterrows():
        mt = r["market_template"]
        q = r["question"]
        if len(mt) > 90: mt = mt[:90] + "..."
        if len(q) > 90: q = q[:90] + "..."
        print(f"  [{r['n_tickers']:>10,}|{r['n_templates']:>5}t] {r['prefix']:35s} mt={mt}")
        print(f"  {'':50s} q={q}")

out.to_csv(OUT_CSV, index=False)
print(f"\n>>> wrote {OUT_CSV}")
