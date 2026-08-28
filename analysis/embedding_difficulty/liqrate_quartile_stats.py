"""Distributional summary of the volume-rate (rq) quartile splits.

Question (JW, 2026-08-28): are rq1-rq3 economically distinct, or are the
bottom three quartiles all "basically the same" negligible markets with rq4
holding everything?

Recomputes vol_rate exactly as make_liq_horizon_slices.py does (standard-
filtered BUY dollars / day of market life, denominator floored at 1h),
joins each market's rq assignment from schemes/scheme_liqrate.parquet, and
reports per quartile: market counts, vol_rate distribution (p10/25/50/75/90,
min/max at the boundaries), TOTAL market volume distribution, horizon
distribution, and each quartile's share of trades and dollars.

Output: liqrate_quartile_stats.parquet (+ printed table).
"""
from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"

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
sch = pd.read_parquet(f"{BASE}/schemes/scheme_liqrate.parquet")
df = uni.merge(nat, on="market_id", how="left").merge(sch, on="market_id",
                                                      how="inner")


def naive(s):
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s


start = naive(df["created_at"]).fillna(naive(df["first_trade_at"]))
end = naive(df["closed_time"]).fillna(naive(df["last_trade_at"]))
df["horizon_days"] = ((end - start).dt.total_seconds() / 86400).where(
    lambda s: s > 0)
df["usd"] = df["usd_buy_filtered"].fillna(0)
df["vol_rate"] = df["usd"] / df["horizon_days"].clip(lower=1 / 24)

rows = []
tot_usd = df["usd"].sum()
tot_trades = df["n_buy_filtered"].sum()
for q, g in df.groupby("slice"):
    r = {"quartile": q, "n_markets": len(g)}
    for col, tag in (("vol_rate", "rate"), ("usd", "totusd"),
                     ("horizon_days", "hor")):
        v = g[col]
        r[f"{tag}_p10"] = float(np.nanpercentile(v, 10))
        r[f"{tag}_p50"] = float(np.nanpercentile(v, 50))
        r[f"{tag}_p90"] = float(np.nanpercentile(v, 90))
    r["rate_min"] = float(g["vol_rate"].min())
    r["rate_max"] = float(g["vol_rate"].max())
    r["share_trades"] = float(g["n_buy_filtered"].sum() / tot_trades)
    r["share_usd"] = float(g["usd"].sum() / tot_usd)
    rows.append(r)
out = pd.DataFrame(rows).sort_values("quartile")
out.to_parquet(f"{BASE}/liqrate_quartile_stats.parquet", index=False)
pd.set_option("display.width", 250)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
print(out.to_string(index=False))
