"""Apply the Stage 0 normalizer to all 34.5M Kalshi tickers and emit:

  * `kalshi_per_ticker.parquet` — one row per ticker with original columns
    plus `event_template` and `market_template`. (This becomes the basis of
    the final per-contract output.)
  * `kalshi_templates.parquet`  — one row per unique (event_template,
    market_template) pair with a representative event_ticker, question, and
    n_tickers count. Input to the LLM batch.

Runs on EC2 with DuckDB for the heavy IO + Python apply for the regex pass.
"""
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from kalshi_normalize import normalize, ticker_prefix_of, is_parlay_prefix

SRC = "/mnt/data/kalshi/kalshi_contract_questions_dates_available.parquet"
OUT_PER_TICKER = "/mnt/data/kalshi/kalshi_per_ticker.parquet"
OUT_TEMPLATES  = "/mnt/data/kalshi/kalshi_templates.parquet"


def main():
    print(">>> loading source (DuckDB streaming → pandas via parquet, projecting key cols)")
    con = duckdb.connect()
    con.execute("SET memory_limit='40GB'")
    con.execute("SET threads=8")

    t0 = time.time()
    # Project only the columns we need; keep ticker as primary key.
    # DuckDB → pyarrow → pandas in chunks would be ideal but for 34.5M rows
    # of mostly-strings we have enough RAM to do this in one go.
    df = con.execute(f"""
        SELECT ticker, event_ticker, question, contract_subtitle, yes_sub_title,
               no_sub_title, market_type, strike_type, floor_strike, cap_strike,
               status, result, open_time, close_time, primary_contract_date,
               volume, open_interest, dollar_volume, dollar_open_interest
        FROM '{SRC}'
    """).fetchdf()
    print(f"    {len(df):,} rows loaded in {time.time()-t0:.1f}s")

    print(">>> computing event_template (ticker prefix)")
    t0 = time.time()
    df["event_template"] = df["ticker"].apply(ticker_prefix_of).astype("string")
    print(f"    done in {time.time()-t0:.1f}s")

    print(">>> computing market_template (normalized question)")
    t0 = time.time()
    # Vectorize via tuple iteration — Python apply on 34M rows is ~1-2 min.
    df["market_template"] = [
        normalize(q, p) for q, p in zip(df["question"].fillna("").tolist(),
                                        df["event_template"].fillna("").tolist())
    ]
    df["market_template"] = df["market_template"].astype("string")
    print(f"    done in {time.time()-t0:.1f}s")

    print(">>> dropping parlay-family prefixes (KXMVE*, KXOSCARWINNERS, KXCITIESWEATHER)")
    t0 = time.time()
    parlay_mask = df["event_template"].apply(is_parlay_prefix)
    n_parlay = int(parlay_mask.sum())
    n_single = int((~parlay_mask).sum())
    print(f"    parlay tickers dropped: {n_parlay:,}")
    print(f"    single-contract tickers kept: {n_single:,}")
    parlay_prefix_counts = (df.loc[parlay_mask, "event_template"]
                              .value_counts().to_dict())
    print(f"    parlay prefix breakdown:")
    for p, n in parlay_prefix_counts.items():
        print(f"      {p}: {n:,}")
    df = df.loc[~parlay_mask].reset_index(drop=True)
    print(f"    filtered in {time.time()-t0:.1f}s")

    print(">>> writing per-ticker parquet")
    t0 = time.time()
    df.to_parquet(OUT_PER_TICKER, index=False)
    print(f"    {OUT_PER_TICKER} ({Path(OUT_PER_TICKER).stat().st_size/1024/1024:.0f} MB) in {time.time()-t0:.1f}s")

    print(">>> deduping to template pairs")
    t0 = time.time()
    tdf = (df.groupby(["event_template", "market_template"], as_index=False)
             .agg(event_ticker_sample=("event_ticker", "first"),
                  ticker_sample=("ticker", "first"),
                  question=("question", "first"),
                  yes_sub_title=("yes_sub_title", "first"),
                  market_type=("market_type", "first"),
                  strike_type=("strike_type", "first"),
                  n_tickers=("ticker", "count")))
    tdf = tdf.sort_values("n_tickers", ascending=False).reset_index(drop=True)
    print(f"    {len(tdf):,} unique (event_template, market_template) pairs in {time.time()-t0:.1f}s")

    tdf.to_parquet(OUT_TEMPLATES, index=False)
    print(f"    {OUT_TEMPLATES} ({Path(OUT_TEMPLATES).stat().st_size/1024/1024:.1f} MB)")

    print("\n=== Top 20 pairs by n_tickers ===")
    for _, r in tdf.head(20).iterrows():
        print(f"  {r['event_template']:30s}  n={r['n_tickers']:>10,}  mt={r['market_template'][:80]}")

    print("\n=== Distribution of n_tickers ===")
    cuts = [1, 2, 5, 10, 100, 1000, 10000, 100000, 1_000_000, 10_000_000]
    for lo, hi in zip(cuts[:-1], cuts[1:]):
        n = ((tdf["n_tickers"] >= lo) & (tdf["n_tickers"] < hi)).sum()
        cov = tdf.loc[(tdf["n_tickers"] >= lo) & (tdf["n_tickers"] < hi), "n_tickers"].sum()
        print(f"  [{lo:>10,}, {hi:>10,}):  {n:>6,} pairs  covering {cov:>13,} tickers")
    big = (tdf["n_tickers"] >= cuts[-1]).sum()
    big_cov = tdf.loc[tdf["n_tickers"] >= cuts[-1], "n_tickers"].sum()
    print(f"  [>={cuts[-1]:>10,}, ∞):      {big:>6,} pairs  covering {big_cov:>13,} tickers")

    print("\n=== 50 random (raw → template) samples ===")
    sample = df.sample(n=50, random_state=42)
    for _, r in sample.iterrows():
        q = (r["question"] or "")
        if len(q) > 120: q = q[:120] + "..."
        print(f"  T: {r['ticker']}")
        print(f"  Q: {q}")
        mt = r["market_template"]
        if len(mt) > 120: mt = mt[:120] + "..."
        print(f"  → ({r['event_template']}, {mt})")
        print()


if __name__ == "__main__":
    main()
