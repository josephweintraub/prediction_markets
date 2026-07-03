"""Approach B slices: novelty / precedent-density deciles.

Reads novelty.parquet (compute_novelty.py). Deciles are formed over markets
with at least one standard-filtered trade (so slices are trade-viable), but
the novelty values themselves were computed against ALL predecessors.

Schemes written (schemes/):
  scheme_nov_k25.parquet        deciles of sim_k25 (incl. same-series precedents)
  scheme_nov_k25x.parquet       deciles of sim_k25_x (EXCL. same-series/event)
  scheme_nov_k25x_vint.parquet  sim_k25_x deciles WITHIN birth-year, pooled
                                (vintage-controlled novelty gradient)
  scheme_nov_cnt.parquet        precedent-count bins: cnt_tau_x in
                                {0, 1-9, 10-99, 100-999, 1000+}
  scheme_series_membership.parquet  in-series vs standalone (recurrence axis)
"""
from __future__ import annotations
import os

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet") \
        .sort_values("market_id").reset_index(drop=True)
nov = pd.read_parquet(f"{BASE}/novelty.parquet")
df = uni.merge(nov, on="market_id", validate="1:1")
df = df[df["n_buy_filtered"].fillna(0) > 0].copy()
print(f"{len(df):,} trade-viable markets", flush=True)


def decile_scheme(values: pd.Series, name: str, frame: pd.DataFrame) -> None:
    m = values.notna()
    q = pd.qcut(values[m].rank(method="first"), 10, labels=False)
    out = pd.DataFrame({"market_id": frame.loc[m, "market_id"],
                        "slice": [f"{name}_d{int(x)+1:02d}" for x in q]})
    out.to_parquet(f"{BASE}/schemes/scheme_{name}.parquet", index=False)
    print(f"scheme_{name}: {m.sum():,} markets", flush=True)


decile_scheme(df["sim_k25"], "nov_k25", df)
decile_scheme(df["sim_k25_x"], "nov_k25x", df)

# vintage-controlled: decile within birth year, pooled across years
year = pd.to_datetime(df["birth_at"]).dt.year
vals = df["sim_k25_x"]
m = vals.notna() & year.notna()
q = vals[m].groupby(year[m]).transform(
    lambda s: pd.qcut(s.rank(method="first"), 10, labels=False))
out = pd.DataFrame({"market_id": df.loc[m, "market_id"],
                    "slice": [f"novx_vint_d{int(x)+1:02d}" for x in q]})
out.to_parquet(f"{BASE}/schemes/scheme_nov_k25x_vint.parquet", index=False)
print(f"scheme_nov_k25x_vint: {m.sum():,} markets", flush=True)

# precedent-count bins (log-spaced, includes a true-zero bucket)
cnt = df["cnt_tau_x"].fillna(0)
labels = np.select(
    [cnt == 0, cnt < 10, cnt < 100, cnt < 1000],
    ["cnt_0", "cnt_1_9", "cnt_10_99", "cnt_100_999"], default="cnt_1000p")
pd.DataFrame({"market_id": df["market_id"], "slice": labels}) \
    .to_parquet(f"{BASE}/schemes/scheme_nov_cnt.parquet", index=False)

# recurrence axis: native series membership
ser = np.where(df["series_slug"].notna(), "in_series", "standalone")
pd.DataFrame({"market_id": df["market_id"], "slice": ser}) \
    .to_parquet(f"{BASE}/schemes/scheme_series_membership.parquet", index=False)
print("novelty schemes written", flush=True)
