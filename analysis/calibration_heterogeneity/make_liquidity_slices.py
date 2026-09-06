"""Liquidity schemes: FLB-vs-volume gradient, inclusion floors, rolling-median
rule, and the novelty x liquidity interaction.

Liquidity proxy: usd_buy_filtered = market-level dollar volume under the
standard trade filters (BUY side, price bounds, bots excluded). This is
VOLUME, not order-book depth — the native `liquidity` field is unreliable on
closed markets. All schemes below are built over trade-viable markets
(>= 1 filtered trade); markets with zero filtered trades never enter FLB runs.

Schemes written (schemes/):
  scheme_liq_tier.parquet        absolute tiers: <$1k / 1-10k / 10-100k /
                                 100k-1M / >=1M (digit-prefixed for ordering)
  scheme_liq_pctl_vint.parquet   volume quintiles WITHIN birth month
                                 (era-relative liquidity; controls platform growth)
  scheme_all_f{1k,10k,100k}.parquet  pooled slice restricted to volume >= floor
                                 (floor-sensitivity for the headline calibration)
  scheme_novx_vint_f10k.parquet  novelty (sim_k25_x) deciles within birth year,
                                 REBUILT on the >=$10k subset — does the
                                 novelty-tail result survive a liquidity floor?
  scheme_rollmed25.parquet       pooled slice under the rolling-median rule:
                                 include markets with volume >= 25% of the
                                 median volume of markets born in the trailing
                                 90 days (strictly earlier; markets with no
                                 trailing window are included)
Also writes liquidity_meta.json (floor/rule exclusion shares) and
rollmed_stats.parquet (per-year exclusion under the rolling rule).
"""
from __future__ import annotations
import json
import os

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
os.makedirs(f"{BASE}/schemes", exist_ok=True)

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at",
                               "n_buy_filtered", "usd_buy_filtered"])
nov = pd.read_parquet(f"{BASE}/novelty.parquet",
                      columns=["market_id", "sim_k25_x", "birth_at"])
df = uni.merge(nov, on="market_id", validate="1:1")
df = df[df["n_buy_filtered"].fillna(0) > 0].copy()
df["usd"] = df["usd_buy_filtered"].fillna(0.0)
df["birth"] = pd.to_datetime(df["birth_at"])
n_viable = len(df)
meta: dict = {"viable_markets": int(n_viable),
              "viable_trades": int(df["n_buy_filtered"].sum())}
print(f"{n_viable:,} trade-viable markets", flush=True)

# ---- absolute tiers -------------------------------------------------------
tier = pd.Series(np.select(
    [df["usd"] < 1e3, df["usd"] < 1e4, df["usd"] < 1e5, df["usd"] < 1e6],
    ["liq1_lt1k", "liq2_1k_10k", "liq3_10k_100k", "liq4_100k_1m"],
    default="liq5_ge1m"), index=df.index)
pd.DataFrame({"market_id": df["market_id"], "slice": tier}) \
    .to_parquet(f"{BASE}/schemes/scheme_liq_tier.parquet", index=False)
meta["tier_market_counts"] = tier.value_counts().to_dict()

# ---- era-relative percentile (within birth month) --------------------------
mth = df["birth"].dt.to_period("M")
q = df.groupby(mth)["usd"].transform(
    lambda s: pd.qcut(s.rank(method="first"), 5, labels=False, duplicates="drop"))
ok = q.notna()
pd.DataFrame({"market_id": df.loc[ok, "market_id"],
              "slice": [f"liqvint_q{int(x)+1}" for x in q[ok]]}) \
    .to_parquet(f"{BASE}/schemes/scheme_liq_pctl_vint.parquet", index=False)

# ---- absolute floors (pooled) ----------------------------------------------
for name, floor in (("1k", 1e3), ("10k", 1e4), ("100k", 1e5)):
    sub = df[df["usd"] >= floor]
    pd.DataFrame({"market_id": sub["market_id"], "slice": f"ALL_ge{name}"}) \
        .to_parquet(f"{BASE}/schemes/scheme_all_f{name}.parquet", index=False)
    meta[f"floor_{name}_markets_kept"] = int(len(sub))
    meta[f"floor_{name}_trades_kept"] = int(sub["n_buy_filtered"].sum())

# ---- novelty deciles within year, on the >=$10k subset ---------------------
sub = df[(df["usd"] >= 1e4) & df["sim_k25_x"].notna()].copy()
year = sub["birth"].dt.year
qq = sub.groupby(year)["sim_k25_x"].transform(
    lambda s: pd.qcut(s.rank(method="first"), 10, labels=False))
pd.DataFrame({"market_id": sub["market_id"],
              "slice": [f"novxf10k_d{int(x)+1:02d}" for x in qq]}) \
    .to_parquet(f"{BASE}/schemes/scheme_novx_vint_f10k.parquet", index=False)
meta["novx_f10k_markets"] = int(len(sub))

# ---- rolling-median rule ----------------------------------------------------
d = df.sort_values("birth").set_index("birth")
rollmed = d["usd"].rolling("90D", closed="left").median()
thresh = 0.25 * rollmed
keep = d["usd"] >= thresh.fillna(0)  # no trailing window -> keep
kept = d.loc[keep.to_numpy()]
pd.DataFrame({"market_id": kept["market_id"], "slice": "ALL_rollmed25"}) \
    .to_parquet(f"{BASE}/schemes/scheme_rollmed25.parquet", index=False)
meta["rollmed_markets_kept"] = int(keep.sum())
meta["rollmed_share_excluded"] = float(1 - keep.mean())
stats = (pd.DataFrame({"year": d.index.year, "kept": keep.to_numpy(),
                       "usd": d["usd"].to_numpy(),
                       "median90": rollmed.to_numpy()})
         .groupby("year")
         .agg(markets=("kept", "size"), share_excluded=("kept", lambda s: 1 - s.mean()),
              median_market_usd=("usd", "median"),
              median_roll90=("median90", "median")).reset_index())
stats.to_parquet(f"{BASE}/rollmed_stats.parquet", index=False)
print(stats.to_string(index=False), flush=True)

with open(f"{BASE}/liquidity_meta.json", "w") as f:
    json.dump(meta, f, indent=2)
print(json.dumps(meta, indent=2), flush=True)
