"""Dedup the 214K template pairs down to 7,166 prefix-level pairs for LLM input.

For each ticker prefix (event_template), pick:
  * The most-frequent market_template (the dominant question template for that
    prefix — represents the "typical" contract in that family).
  * The representative question and event_ticker for context.
  * Total n_tickers covered by that prefix.

Output: `kalshi_prefix_pairs.parquet` — one row per prefix → LLM input.
"""
from pathlib import Path
import pandas as pd

SRC = Path("/mnt/data/kalshi/kalshi_templates.parquet")
OUT = Path("/mnt/data/kalshi/kalshi_prefix_pairs.parquet")


def main():
    print(f">>> loading {SRC}")
    df = pd.read_parquet(SRC)
    print(f"    {len(df):,} template pairs, {df['event_template'].nunique():,} prefixes")

    # For each prefix, pick a representative template by:
    #   1. Prefer templates containing placeholders (`<TEAM>`, `<PLAYER>`,
    #      `<CITY>`, `<NUM>`, `<DATE>`, etc.) — those carry the abstraction
    #      signal we want the LLM to use.
    #   2. Within those, prefer the most-frequent (n_tickers descending) so
    #      degenerate one-off templates don't win.
    print(">>> selecting representative template per prefix (placeholder-rich, n_tickers tiebreak)")
    df["_n_placeholders"] = df["market_template"].fillna("").str.count(r"<[A-Z_]+>")
    dom = (df.sort_values(["event_template", "_n_placeholders", "n_tickers"],
                          ascending=[True, False, False])
             .groupby("event_template", as_index=False)
             .first()
             .drop(columns=["_n_placeholders"]))
    # Add a total n_tickers per prefix (for downstream weighting and audit).
    per_prefix_total = df.groupby("event_template", as_index=False)["n_tickers"].sum()
    per_prefix_total = per_prefix_total.rename(columns={"n_tickers": "prefix_n_tickers"})
    dom = dom.merge(per_prefix_total, on="event_template")
    # Also count distinct templates per prefix (for audit).
    per_prefix_ntemplate = df.groupby("event_template", as_index=False).size().rename(
        columns={"size": "prefix_n_templates"})
    dom = dom.merge(per_prefix_ntemplate, on="event_template")
    dom = dom.sort_values("prefix_n_tickers", ascending=False).reset_index(drop=True)

    print(f">>> {len(dom):,} prefix-level rows")
    print("\nTop 20 prefixes:")
    cols = ["event_template", "prefix_n_tickers", "prefix_n_templates",
            "market_template", "question"]
    for _, r in dom[cols].head(20).iterrows():
        mt = r["market_template"]
        if mt and len(mt) > 60: mt = mt[:60] + "..."
        q = r["question"] or ""
        if len(q) > 80: q = q[:80] + "..."
        print(f"  {r['event_template']:32s} n={r['prefix_n_tickers']:>10,} "
              f"({r['prefix_n_templates']:>5,}t) mt={mt}")
        print(f"  {'':32s} q={q}")

    print(f"\n>>> writing {OUT}")
    dom.to_parquet(OUT, index=False)
    print(f"    {OUT} ({OUT.stat().st_size/1024/1024:.2f} MB)")


if __name__ == "__main__":
    main()
