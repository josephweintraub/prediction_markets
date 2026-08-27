"""Disentangle liquidity from maturity/horizon (collaborator direction,
2026-08-27), and fix maturity measurement for multi-choice events.

Motivation:
  (1) Total volume mechanically accumulates with time-open, so total-volume
      liquidity and horizon are confounded. Liquidity here = VOLUME RATE:
      standard-filtered BUY dollars per day of market life
      (usd_buy_filtered / horizon_days, denominator floored at 1 hour).
  (2) In multi-outcome events (elections), dropout candidates resolve No
      early, contaminating per-market horizon. Fixes implemented:
      "final two" (keep the two markets per multi-market event that survived
      latest, ties broken by volume) and "always-binary standalone" (events
      with exactly one market — no dropouts possible).

Schemes written (schemes/):
  scheme_liqrate.parquet          vol-rate quartiles (viable markets), rq1..rq4
  scheme_liqrate_vint.parquet     vol-rate quartiles WITHIN birth month
  scheme_hor_x_liqrate.parquet    horizon bin x vol-rate quartile, where the
                                  quartiles are formed WITHIN each horizon bin
                                  (marginals balanced by construction): 20 cells
  scheme_horizon_binary.parquet   horizon bins, standalone-binary markets only
  scheme_horizon_final2.parquet   horizon bins, standalone binaries + the
                                  final-two markets of each multi-market event
                                  (dropouts excluded)
Also writes liq_horizon_meta.json: horizon~volume correlations (total vs
rate), dropout-contamination stats, sample sizes.
"""
from __future__ import annotations
import json
import os

import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
os.makedirs(f"{BASE}/schemes", exist_ok=True)
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"

import duckdb
con = duckdb.connect()
uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at",
                               "last_trade_at", "event_slug",
                               "n_buy_filtered", "usd_buy_filtered"])
nat = con.execute(f"""
    SELECT condition_id AS market_id,
           TRY_CAST(closed_time AS TIMESTAMP) AS closed_time,
           neg_risk
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
df["end_time"] = end
df["birth"] = start

HBINS = ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d", "h5_ge90d"]
df["hbin"] = pd.Series(np.select(
    [df["horizon_days"] < 1, df["horizon_days"] < 7,
     df["horizon_days"] < 30, df["horizon_days"] < 90],
    HBINS[:4], default=HBINS[4]), index=df.index).where(
    df["horizon_days"].notna())

viable = df[(df["n_buy_filtered"].fillna(0) > 0)
            & df["horizon_days"].notna()].copy()
viable["vol_rate"] = (viable["usd_buy_filtered"].fillna(0)
                      / viable["horizon_days"].clip(lower=1 / 24))
meta: dict = {"viable_markets": int(len(viable))}

# de-confounding check: horizon vs total volume vs volume rate
lh = np.log(viable["horizon_days"])
meta["corr_loghorizon_logtotalusd"] = float(np.corrcoef(
    lh, np.log1p(viable["usd_buy_filtered"].fillna(0)))[0, 1])
meta["corr_loghorizon_logvolrate"] = float(np.corrcoef(
    lh, np.log1p(viable["vol_rate"]))[0, 1])

# ---- vol-rate quartiles (pooled + within birth month) ----------------------
q = pd.qcut(viable["vol_rate"].rank(method="first"), 4, labels=False)
pd.DataFrame({"market_id": viable["market_id"],
              "slice": [f"rq{int(x)+1}" for x in q]}) \
    .to_parquet(f"{BASE}/schemes/scheme_liqrate.parquet", index=False)
mth = viable["birth"].dt.to_period("M")
qv = viable.groupby(mth)["vol_rate"].transform(
    lambda s: pd.qcut(s.rank(method="first"), 4, labels=False,
                      duplicates="drop"))
ok = qv.notna()
pd.DataFrame({"market_id": viable.loc[ok, "market_id"],
              "slice": [f"rqv{int(x)+1}" for x in qv[ok]]}) \
    .to_parquet(f"{BASE}/schemes/scheme_liqrate_vint.parquet", index=False)

# ---- horizon x within-bin vol-rate quartile --------------------------------
qh = viable.groupby("hbin")["vol_rate"].transform(
    lambda s: pd.qcut(s.rank(method="first"), 4, labels=False,
                      duplicates="drop"))
ok = qh.notna()
pd.DataFrame({"market_id": viable.loc[ok, "market_id"],
              "slice": viable.loc[ok, "hbin"] + "|rq"
              + (qh[ok].astype(int) + 1).astype(str)}) \
    .to_parquet(f"{BASE}/schemes/scheme_hor_x_liqrate.parquet", index=False)

# ---- event structure: standalone binaries, final two, dropouts -------------
ev = df[df["event_slug"].notna()].copy()
gsz = ev.groupby("event_slug")["market_id"].transform("size")
ev["n_in_event"] = gsz
df = df.merge(ev[["market_id", "n_in_event"]], on="market_id", how="left")
df["n_in_event"] = df["n_in_event"].fillna(1)

standalone = df["n_in_event"] == 1
meta["standalone_markets"] = int(standalone.sum())
meta["multi_event_markets"] = int((~standalone).sum())

multi = df[~standalone & df["end_time"].notna()].copy()
evmax = multi.groupby("event_slug")["end_time"].transform("max")
multi["days_before_event_end"] = (evmax - multi["end_time"]).dt.total_seconds() / 86400
multi["is_dropout"] = multi["days_before_event_end"] > 2
meta["multi_markets_with_end"] = int(len(multi))
meta["dropout_markets"] = int(multi["is_dropout"].sum())
meta["dropout_share_of_multi"] = float(multi["is_dropout"].mean())
drop_by_hbin = multi.groupby("hbin")["is_dropout"].mean().round(3).to_dict()
meta["dropout_share_by_hbin"] = {str(k): float(v)
                                 for k, v in drop_by_hbin.items()}

# final two per multi-market event: latest end_time, ties by volume
multi["_vol"] = multi["usd_buy_filtered"].fillna(0)
multi = multi.sort_values(["event_slug", "end_time", "_vol"],
                          ascending=[True, False, False])
multi["rank_in_event"] = multi.groupby("event_slug").cumcount()
final2_ids = set(multi.loc[multi["rank_in_event"] < 2, "market_id"])
meta["final2_markets"] = int(len(final2_ids))

hb = df["hbin"].notna() & (df["n_buy_filtered"].fillna(0) > 0)
bin_mask = hb & standalone
pd.DataFrame({"market_id": df.loc[bin_mask, "market_id"],
              "slice": df.loc[bin_mask, "hbin"]}) \
    .to_parquet(f"{BASE}/schemes/scheme_horizon_binary.parquet", index=False)
meta["horizon_binary_markets"] = int(bin_mask.sum())

f2_mask = hb & (standalone | df["market_id"].isin(final2_ids))
pd.DataFrame({"market_id": df.loc[f2_mask, "market_id"],
              "slice": df.loc[f2_mask, "hbin"]}) \
    .to_parquet(f"{BASE}/schemes/scheme_horizon_final2.parquet", index=False)
meta["horizon_final2_markets"] = int(f2_mask.sum())

with open(f"{BASE}/liq_horizon_meta.json", "w") as f:
    json.dump(meta, f, indent=2)
print(json.dumps(meta, indent=2), flush=True)
