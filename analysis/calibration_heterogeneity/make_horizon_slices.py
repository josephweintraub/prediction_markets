"""Horizon, recurrence, anchorability, and per-category novelty schemes.

Built to answer collaborator follow-ups (2026-08-15) on the FIXED plumbing
(fresh spine, market-level up/down exclusion) so results are comparable with
everything else in this workstream.

Horizon = contract lifetime: native closed_time − created_at where both
exist, else last_trade_at − created_at, else last − first trade (flagged).
This is time-on-market, not time-to-event; it is also the "feedback speed"
proxy (same field) — named horizon throughout.

Schemes written (schemes/):
  scheme_horizon.parquet         h1_lt1d / h2_1_7d / h3_7_30d / h4_30_90d / h5_ge90d
  scheme_horizon_cat.parquet     "<Category>|<horizon bin>" cross-slices
  scheme_recurrence.parquet      native series recurrence: daily/weekly/monthly/
                                 annual/none (series_slug w/o recurrence -> in_series_other)
  scheme_anchor.parquet          anchorability from native resolution_source:
                                 sourced vs judgment(blank); UNKNOWN if no native meta
  scheme_novtail_cat.parquet     within each category: novelty tail (d01 of
                                 within-vintage sim_k25_x deciles) vs rest ->
                                 "<Category>|tail" / "<Category>|rest"
Also writes horizon_volume.parquet: per horizon bin, trade/dollar shares and
trade-size stats (collaborator Q: is #trades a good proxy for dollars across
horizons?).
"""
from __future__ import annotations
import os

import duckdb
import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"
CATS = "/mnt/data/learnability/native/market_native_categories.parquet"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

con = duckdb.connect()
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at",
                               "last_trade_at", "series_slug", "recurrence",
                               "n_buy_filtered", "usd_buy_filtered"])
nat = con.execute(f"""
    SELECT condition_id AS market_id,
           TRY_CAST(closed_time AS TIMESTAMP) AS closed_time,
           NULLIF(TRIM(resolution_source), '') AS resolution_source
    FROM read_parquet('{NATIVE_META}')
""").fetchdf()
cat = pd.read_parquet(CATS).rename(columns={"mkt": "market_id", "prim": "category"})
df = uni.merge(nat, on="market_id", how="left") \
        .merge(cat, on="market_id", how="left")


def naive(s):
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s


created = naive(df["created_at"])
first_t = naive(df["first_trade_at"])
last_t = naive(df["last_trade_at"])
closed = naive(df["closed_time"])

start = created.fillna(first_t)
end = closed.fillna(last_t)
horizon_days = (end - start).dt.total_seconds() / 86400
horizon_days = horizon_days.where(horizon_days > 0)
fallback_end = closed.isna()
print(f"horizon: {horizon_days.notna().sum():,} defined "
      f"({fallback_end.sum():,} used last-trade fallback for end)", flush=True)

hbin = pd.Series(np.select(
    [horizon_days < 1, horizon_days < 7, horizon_days < 30, horizon_days < 90],
    ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d"], default="h5_ge90d"),
    index=df.index).where(horizon_days.notna())

m = hbin.notna()
pd.DataFrame({"market_id": df.loc[m, "market_id"], "slice": hbin[m]}) \
    .to_parquet(f"{BASE}/schemes/scheme_horizon.parquet", index=False)

catv = df["category"].fillna("UNKNOWN")
mc = m & (catv != "UNKNOWN")
pd.DataFrame({"market_id": df.loc[mc, "market_id"],
              "slice": catv[mc] + "|" + hbin[mc]}) \
    .to_parquet(f"{BASE}/schemes/scheme_horizon_cat.parquet", index=False)

rec = df["recurrence"].str.lower()
rec = rec.where(rec.isin(["daily", "weekly", "monthly", "annual"]))
rec = rec.fillna(pd.Series(
    np.where(df["series_slug"].notna(), "in_series_other", "none"),
    index=df.index))
pd.DataFrame({"market_id": df["market_id"], "slice": "rec_" + rec}) \
    .to_parquet(f"{BASE}/schemes/scheme_recurrence.parquet", index=False)

has_native = df["closed_time"].notna() | df["resolution_source"].notna()
anchor = pd.Series(
    np.where(has_native,
             np.where(df["resolution_source"].notna(), "sourced", "judgment"),
             "UNKNOWN"), index=df.index)
pd.DataFrame({"market_id": df["market_id"], "slice": anchor}) \
    .to_parquet(f"{BASE}/schemes/scheme_anchor.parquet", index=False)
print(pd.Series(anchor).value_counts().to_string(), flush=True)

# per-category novelty tail (within-vintage d01 vs rest), viable markets only
nov = pd.read_parquet(f"{BASE}/novelty.parquet",
                      columns=["market_id", "sim_k25_x", "birth_at"])
dv = df.merge(nov, on="market_id").query("n_buy_filtered > 0")
dv = dv[dv["sim_k25_x"].notna() & (dv["category"].notna())]
year = pd.to_datetime(dv["birth_at"]).dt.year
dec = dv.groupby(year)["sim_k25_x"].transform(
    lambda s: pd.qcut(s.rank(method="first"), 10, labels=False))
tail = np.where(dec == 0, "tail", "rest")
pd.DataFrame({"market_id": dv["market_id"],
              "slice": dv["category"] + "|" + tail}) \
    .to_parquet(f"{BASE}/schemes/scheme_novtail_cat.parquet", index=False)
print(f"novtail_cat: {len(dv):,} markets", flush=True)

# trades-vs-dollars by horizon bin (market-level aggregates, filtered BUY)
hv = pd.DataFrame({"bin": hbin[m],
                   "n": df.loc[m, "n_buy_filtered"].fillna(0),
                   "usd": df.loc[m, "usd_buy_filtered"].fillna(0)})
g = hv.groupby("bin").agg(markets=("n", "size"), trades=("n", "sum"),
                          usd=("usd", "sum"))
g["share_trades"] = g["trades"] / g["trades"].sum()
g["share_usd"] = g["usd"] / g["usd"].sum()
g["usd_per_trade"] = g["usd"] / g["trades"].replace(0, np.nan)
g.reset_index().to_parquet(f"{BASE}/horizon_volume.parquet", index=False)
print(g.reset_index().to_string(index=False), flush=True)
