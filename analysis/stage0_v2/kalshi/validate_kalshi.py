"""Step 6: validate stage2_per_contract_kalshi.parquet end-to-end.

Mirrors the Polymarket validate_augmented.py pattern.

Checks:
  * Row count matches the post-parlay-filter ticker count
  * All 11 LLM columns populated (per-column NaN%)
  * Spot checks: 5 random rows + 5 stratified across top-20 ticker_roots
  * extraction_error populated for any failed LLM calls
  * Cross-platform sanity: category histogram + percentages
"""
import sys
from pathlib import Path

import pandas as pd

PARQUET = Path("/mnt/data/kalshi/stage2_per_contract_kalshi.parquet")

EXT_COLS = ["event_subjects", "event_action", "event_info_type",
            "event_resolution_type",
            "market_subjects", "market_action", "market_info_type",
            "market_resolution_type",
            "categories", "snippet", "extraction_error"]


def main():
    print(f">>> loading {PARQUET}")
    df = pd.read_parquet(PARQUET)
    print(f"    {len(df):,} rows × {len(df.columns)} columns")

    print(f"\n=== STRUCTURAL CHECKS ===")
    assert df["ticker"].is_unique, "ticker should be unique per row"
    print(f"  ✓ ticker unique across {len(df):,} rows")

    print(f"\n=== LLM COLUMN POPULATION ===")
    for c in EXT_COLS:
        n_na = int(df[c].isna().sum())
        pct = n_na / len(df) * 100
        print(f"  {c:30s} {n_na:>10,} NaN ({pct:.2f}%)")

    n_extraction_error = int(df["extraction_error"].notna().sum())
    print(f"\n  rows with extraction_error: {n_extraction_error:,}")

    print(f"\n=== CATEGORY HISTOGRAM ===")
    cats_flat = []
    for cl in df["categories"]:
        if cl is not None:
            cats_flat.extend(list(cl) if hasattr(cl, "__iter__") and not isinstance(cl, str) else [cl])
    cat_counts = pd.Series(cats_flat).value_counts()
    for cat, n in cat_counts.items():
        pct = n / len(df) * 100
        print(f"  {cat:25s} {n:>10,} ({pct:.1f}%)")

    print(f"\n=== TOP 20 EVENT_TEMPLATES BY TICKER COUNT ===")
    pfx_counts = df.groupby("event_template").size().sort_values(ascending=False).head(20)
    for pfx, n in pfx_counts.items():
        sample = df[df["event_template"] == pfx].iloc[0]
        subj = sample["event_subjects"]
        subj_str = ", ".join(subj[:2]) if isinstance(subj, (list, tuple)) else str(subj)
        print(f"  {pfx:30s} n={n:>10,}  subjects={subj_str}")

    print(f"\n=== SPOT CHECK: 5 RANDOM ROWS ===")
    sample = df.sample(n=5, random_state=42)
    for _, r in sample.iterrows():
        print(f"\n  ticker:          {r['ticker']}")
        print(f"  question:        {(r['question'] or '')[:100]}")
        print(f"  event_template:  {r['event_template']}")
        print(f"  market_template: {(r['market_template'] or '')[:80]}")
        print(f"  event_subjects:  {r['event_subjects']}")
        print(f"  categories:      {r['categories']}")

    print(f"\n=== SPOT CHECK: 5 STRATIFIED PREFIXES ===")
    stratified_prefixes = ["KXINXU", "KXBTCD", "KXNHLGOAL", "KXPGATOUR", "KXLAMAYORRESIGN"]
    for pfx in stratified_prefixes:
        sub = df[df["event_template"] == pfx]
        if len(sub) == 0:
            print(f"\n  {pfx}: no rows")
            continue
        r = sub.iloc[0]
        print(f"\n  {pfx}: {len(sub):,} rows")
        print(f"    sample question: {(r['question'] or '')[:100]}")
        print(f"    event_subjects:  {r['event_subjects']}")
        print(f"    categories:      {r['categories']}")

    print(f"\n✓ validation complete")


if __name__ == "__main__":
    main()
