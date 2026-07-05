"""Individual learning curves and trader-relative novelty (two-sided tape).

All designs use every wallet position (buys, and sells as complement-buys),
ordered within wallet by trade day (deterministic tie-break; sub-day ordering
noise is negligible at the e-fold scale used here).

A. Canonical within-wallet experience: does the SAME wallet's calibration
   drift with its own cumulative position count? Continuous (ln 1+prior
   positions) and bucketed (own-history ordinals 1-19 base, 20-99, 100-999,
   1000+), wallet FE x [1,p] absorbed.
B. Trader-relative novelty: within wallet, positions in text families
   (k=200 clusters) the wallet has never traded before vs familiar families,
   controlling for general experience.
C. Dimension tilts by wallet size: the Table-5 within-wallet joint model
   estimated separately for wallets with 20-99 / 100-999 / 1,000-9,999 /
   10,000+ total positions.
D. Cross-sectional slope by wallet-size tier (composition-caveated
   complement to A).
Outputs: experience_{walletfe,sizebuckets,tiers}.parquet
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from horizon_flb_v2 import threeway_cluster_slope  # noqa: E402
from horse_race_v1 import joint_model  # noqa: E402

SIDE_INT = "/mnt/data/learnability/output/side_symmetry_trades.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


t0 = time.time()
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
df = con.execute(f"""
    SELECT s.price, s.won, s.usdc, s.trade_day, s.proxyWallet, s.market_id,
           d.cluster_k200, d.life_d, d.usd_full,
           (d.novelty_vint_decile = 1) AS nov_tail,
           ROW_NUMBER() OVER (PARTITION BY s.proxyWallet
               ORDER BY s.trade_day, s.market_id, s.price, s.usdc) AS exp_ord,
           ROW_NUMBER() OVER (PARTITION BY s.proxyWallet, d.cluster_k200
               ORDER BY s.trade_day, s.market_id, s.price, s.usdc) AS clu_ord
    FROM read_parquet('{SIDE_INT}') s
    JOIN read_parquet('{DIMS}') d ON s.market_id = d.condition_id
    WHERE d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
      AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
""").fetchdf()
log(f"sample: {len(df):,} positions, {df['proxyWallet'].nunique():,} wallets")

df["ln_exp"] = np.log1p(df["exp_ord"] - 1)
df["first_enc"] = (df["clu_ord"] == 1).astype(float)
df["ln_clu"] = np.log1p(df["clu_ord"] - 1)
for b, lo, hi in [("e20", 20, 99), ("e100", 100, 999), ("e1000", 1000, 10**9)]:
    df[b] = ((df["exp_ord"] >= lo) & (df["exp_ord"] <= hi)).astype(float)
tot = df.groupby("proxyWallet", observed=True)["price"].transform("size")

frames = []
# A. canonical experience
log("=== A: within-wallet experience ===")
frames.append(joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", None,
                          ["ln_exp"], "expFE|count|lnexp"))
frames.append(joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", "usdc",
                          ["ln_exp"], "expFE|dollar|lnexp"))
frames.append(joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", None,
                          ["e20", "e100", "e1000"], "expFE|count|buckets"))

# B. trader-relative novelty
log("=== B: trader-relative novelty (first encounter with family) ===")
frames.append(joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", None,
                          ["first_enc", "ln_clu", "ln_exp"],
                          "famFE|count|first_enc"))
frames.append(joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", "usdc",
                          ["first_enc", "ln_clu", "ln_exp"],
                          "famFE|dollar|first_enc"))
pd.concat(frames, ignore_index=True).to_parquet(OUT / "experience_walletfe.parquet")

# C. dimension tilts by wallet size
log("=== C: dimension tilts by wallet size ===")
mk = df.groupby("market_id", observed=True).agg(
    life_d=("life_d", "first"), usd_full=("usd_full", "first"),
    nov=("nov_tail", "first"))
zl = np.log(np.maximum(mk["life_d"], 1e-5)); zl = (zl - zl.mean()) / zl.std()
zu = np.log1p(mk["usd_full"]); zu = (zu - zu.mean()) / zu.std()
df["z_ln_life"] = zl.reindex(df["market_id"]).to_numpy()
df["z_ln_usd"] = zu.reindex(df["market_id"]).to_numpy()
df["nov_tail_f"] = mk["nov"].astype(float).reindex(df["market_id"]).to_numpy()
sz_frames = []
for lo, hi, nm in [(20, 99, "w20-99"), (100, 999, "w100-999"),
                   (1000, 9999, "w1000-9999"), (10000, 10**9, "w10000+")]:
    sub = df[(tot >= lo) & (tot <= hi)]
    if len(sub) < 100_000:
        continue
    log(f"  bucket {nm}: {len(sub):,} positions, "
        f"{sub['proxyWallet'].nunique():,} wallets")
    r = joint_model(sub.assign(_fe=sub["proxyWallet"]), "_fe", None,
                    ["nov_tail_f", "z_ln_usd", "z_ln_life"],
                    f"sizeFE|count|{nm}")
    r["n_positions"] = len(sub)
    r["n_wallets"] = sub["proxyWallet"].nunique()
    sz_frames.append(r)
pd.concat(sz_frames, ignore_index=True).to_parquet(
    OUT / "experience_sizebuckets.parquet")

# D. cross-sectional slope by wallet-size tier
log("=== D: cross-sectional tiers ===")
rows = []
for lo, hi, nm in [(1, 19, "1-19"), (20, 99, "20-99"), (100, 999, "100-999"),
                   (1000, 9999, "1000-9999"), (10000, 10**9, "10000+")]:
    sub = df[(tot >= lo) & (tot <= hi)]
    if len(sub) < 5000:
        continue
    y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
    b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                   sub["proxyWallet"], sub["market_id"])
    bd, sed = threeway_cluster_slope(y, sub["price"], sub["usdc"],
                                     sub["trade_day"], sub["proxyWallet"],
                                     sub["market_id"])
    log(f"  tier {nm:10s} n={len(sub):>11,} dev={b:+.4f} (t={b/se:+.2f})")
    rows.append({"tier": nm, "n": len(sub),
                 "n_wallets": sub["proxyWallet"].nunique(),
                 "slope_dev": b, "se": se, "t": b / se if se > 0 else np.nan,
                 "slope_dev_dol": bd, "se_dol": sed,
                 "t_dol": bd / sed if sed > 0 else np.nan})
pd.DataFrame(rows).to_parquet(OUT / "experience_tiers.parquet")
log(f"DONE experience_curves in {(time.time()-t0)/60:.1f} min")
