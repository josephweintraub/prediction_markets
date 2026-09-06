"""Equal-USD-volume liquidity buckets (KV request, 2026-08-28).

Markets are ordered by volume RATE ($/day, ascending) and cut so each bucket
holds ~25% of TOTAL standard-filtered dollar volume — so uq1 contains a huge
number of thin markets and uq4 a small number of deep ones, but every bucket
carries the same dollar mass (equal economic weight, roughly equal
dollar-weighted power).

Schemes written (schemes/):
  scheme_liqrate_usdq.parquet   uq1..uq4 (pooled)
  scheme_hor_x_usdq.parquet     horizon bin x WITHIN-bin equal-USD bucket
Also writes usdq_stats.parquet: per-bucket market counts, rate boundaries,
median total volume, trade shares (dollar shares ~0.25 by construction).
"""
from __future__ import annotations
import os

import duckdb
import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

con = duckdb.connect()
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at",
                               "last_trade_at", "n_buy_filtered",
                               "usd_buy_filtered"])
nat = con.execute(f"""
    SELECT condition_id AS market_id,
           TRY_CAST(closed_time AS TIMESTAMP) AS closed_time
    FROM read_parquet('{NATIVE_META}')
""").fetchdf()
df = uni.merge(nat, on="market_id", how="left")


def naive(s):
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s


start = naive(df["created_at"]).fillna(naive(df["first_trade_at"]))
end = naive(df["closed_time"]).fillna(naive(df["last_trade_at"]))
df["horizon_days"] = ((end - start).dt.total_seconds() / 86400).where(
    lambda s: s > 0)
df = df[(df["n_buy_filtered"].fillna(0) > 0) & df["horizon_days"].notna()].copy()
df["usd"] = df["usd_buy_filtered"].fillna(0)
df["vol_rate"] = df["usd"] / df["horizon_days"].clip(lower=1 / 24)

HBINS = ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d", "h5_ge90d"]
df["hbin"] = pd.Series(np.select(
    [df["horizon_days"] < 1, df["horizon_days"] < 7,
     df["horizon_days"] < 30, df["horizon_days"] < 90],
    HBINS[:4], default=HBINS[4]), index=df.index)


def usd_quartile(frame: pd.DataFrame, prefix: str) -> pd.Series:
    """Cut rate-ordered markets at 25/50/75% of cumulative USD volume."""
    f = frame.sort_values(["vol_rate", "market_id"])
    cum = f["usd"].cumsum() / max(f["usd"].sum(), 1e-9)
    lab = np.minimum((cum * 4).apply(np.ceil).clip(lower=1), 4).astype(int)
    return pd.Series([f"{prefix}{int(x)}" for x in lab], index=f.index)


df["uq"] = usd_quartile(df, "uq")
pd.DataFrame({"market_id": df["market_id"], "slice": df["uq"]}) \
    .to_parquet(f"{BASE}/schemes/scheme_liqrate_usdq.parquet", index=False)

hx = df.groupby("hbin", group_keys=False).apply(
    lambda g: usd_quartile(g, "uq"))
pd.DataFrame({"market_id": df["market_id"],
              "slice": df["hbin"] + "|" + hx.reindex(df.index)}) \
    .to_parquet(f"{BASE}/schemes/scheme_hor_x_usdq.parquet", index=False)

rows = []
tot_tr = df["n_buy_filtered"].sum()
for q, g in df.groupby("uq"):
    rows.append({"bucket": q, "n_markets": len(g),
                 "rate_min": float(g["vol_rate"].min()),
                 "rate_max": float(g["vol_rate"].max()),
                 "rate_p50": float(g["vol_rate"].median()),
                 "totusd_p50": float(g["usd"].median()),
                 "share_trades": float(g["n_buy_filtered"].sum() / tot_tr),
                 "share_usd": float(g["usd"].sum() / df["usd"].sum())})
stats = pd.DataFrame(rows).sort_values("bucket")
stats.to_parquet(f"{BASE}/usdq_stats.parquet", index=False)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
print(stats.to_string(index=False))
