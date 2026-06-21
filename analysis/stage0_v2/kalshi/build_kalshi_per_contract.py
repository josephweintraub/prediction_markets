"""Step 5: assemble the final per-Kalshi-contract output.

Inputs:
  * /mnt/data/kalshi/kalshi_per_ticker.parquet  — one row per ticker after
    parlay filter, with event_template + market_template added.
  * /mnt/data/kalshi/kalshi_full_extracted.jsonl — LLM extractions, one row
    per event_template (prefix-level grain).

Output:
  /mnt/data/kalshi/stage2_per_contract_kalshi.parquet
    one row per single-contract ticker (~6.52M rows) with the original
    Kalshi columns plus event_template, market_template, and 11 LLM-extracted
    columns matching the Polymarket `_generic` set:
      event_subjects, event_action, event_info_type, event_resolution_type,
      market_subjects, market_action, market_info_type, market_resolution_type,
      categories, snippet, extraction_error
"""
import json
import sys
from pathlib import Path

import pandas as pd

PER_TICKER = Path("/mnt/data/kalshi/kalshi_per_ticker.parquet")
EXTRACTED  = Path("/mnt/data/kalshi/kalshi_full_extracted.jsonl")
OUT        = Path("/mnt/data/kalshi/stage2_per_contract_kalshi.parquet")

EXT_COLS = ["event_subjects", "event_action", "event_info_type",
            "event_resolution_type",
            "market_subjects", "market_action", "market_info_type",
            "market_resolution_type",
            "categories", "snippet"]


def main():
    print(f">>> loading {PER_TICKER}")
    df = pd.read_parquet(PER_TICKER)
    print(f"    {len(df):,} tickers")

    print(f">>> loading LLM extractions from {EXTRACTED}")
    rows = []
    n_err = 0
    for line in EXTRACTED.read_text().splitlines():
        r = json.loads(line)
        e = r.get("extracted", {})
        row = {"event_template": r["event_template"]}
        if "_error" in e:
            row["extraction_error"] = e["_error"]
            for c in EXT_COLS:
                row[c] = None
            n_err += 1
        else:
            row["extraction_error"] = None
            for c in EXT_COLS:
                row[c] = e.get(c)
        rows.append(row)
    ext_df = pd.DataFrame(rows).drop_duplicates("event_template")
    print(f"    {len(ext_df):,} extractions ({n_err} errors)")

    print(">>> left-joining extractions to per-ticker dataset on event_template")
    out = df.merge(ext_df, on="event_template", how="left")
    n_unmatched = out["extraction_error"].isna().sum() - df["event_template"].isin(ext_df["event_template"]).sum()
    # better: count rows whose event_template wasn't in ext_df
    matched_prefixes = set(ext_df["event_template"])
    in_extraction = out["event_template"].isin(matched_prefixes)
    print(f"    {int(in_extraction.sum()):,} tickers got LLM extraction")
    print(f"    {int((~in_extraction).sum()):,} tickers had no matching prefix in extractions")

    print(f">>> writing {OUT}")
    out.to_parquet(OUT, index=False)
    print(f"    {OUT} ({OUT.stat().st_size/1024/1024:.1f} MB)")

    print(f"\n=== final dataset: {len(out):,} rows × {len(out.columns)} columns ===")
    print(f"columns: {list(out.columns)}")
    print(f"\n=== category histogram (top 20) ===")
    cats_flat = []
    for cl in out["categories"]:
        if cl is not None:
            cats_flat.extend(cl if isinstance(cl, (list, tuple)) else [cl])
    cat_counts = pd.Series(cats_flat).value_counts()
    for cat, n in cat_counts.head(20).items():
        print(f"  {cat:25s} {n:>10,}")


if __name__ == "__main__":
    main()
