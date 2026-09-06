"""Rerun the market-wide comparisons under junk floors on rolling relative
volume: baseline (no floor), x = 0.25 (proposed), and the two estimated
thresholds from junk_threshold.py. For each floor: pooled horizon classes,
pooled topics, the three long-horizon period cells, and the cluster-FE
count-weighted joint model. Output: junk_filtered_results.parquet."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from horizon_flb_v2 import threeway_cluster_slope  # noqa: E402
from horse_race_v1 import joint_model  # noqa: E402

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
REL = "/mnt/data/learnability/output/junk_rel_volume.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def slope_rows(df, mask, floor_nm, analysis, label):
    sub = df[mask]
    if len(sub) < 5000:
        return None
    y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
    b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                   sub["proxyWallet"], sub["market_id"])
    log(f"  [{floor_nm}] {analysis}/{label}: n={len(sub):,} dev={b:+.4f} "
        f"(t={b/se:+.2f})")
    return {"floor": floor_nm, "analysis": analysis, "slice": label,
            "n": len(sub), "n_markets": sub["market_id"].nunique(),
            "beta": b, "se": se, "t": b / se if se > 0 else np.nan}


t0 = time.time()
summ = json.loads((OUT / "junk_threshold_summary.json").read_text())
floors = [("baseline", 0.0), ("x0.25", 0.25),
          ("x_skill", summ["x_skill"]), ("x_thresh", summ["x_thresh"])]
floors = [(n, x) for n, x in floors if np.isfinite(x)]
seen = set()
floors = [(n, x) for n, x in floors
          if not (round(x, 2) in seen or seen.add(round(x, 2)))]
log(f"floors: {floors}")

con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
df = con.execute(f"""
    SELECT t.price, t.won, t.usdc, t.trade_day, t.proxyWallet, t.market_id,
           t.hclass, t.tb, coalesce(d.topic,'UNKNOWN') AS topic,
           d.cluster_k200, d.life_d, d.usd_full, d.sim_k25_x,
           (d.novelty_vint_decile=1) AS nov_tail, d.anchor_class,
           d.prior_instances, coalesce(d.rules_len,0) AS rules_len,
           coalesce(d.neg_risk, FALSE) AS neg_risk, d.vintage_year,
           r.r_vol
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    LEFT JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
    JOIN read_parquet('{REL}') r ON t.market_id = r.market_id
    WHERE t.pos BETWEEN 0.25 AND 0.80
""").fetchdf()
log(f"loaded {len(df):,} trades")

DIMS8 = ["z_ln_life", "z_ln_usd", "nov_tail_f", "anchored",
         "z_ln_prior", "z_ln_rules", "neg_risk_f", "z_vintage"]
results = []
for fname, x in floors:
    d = df[df["r_vol"] >= x]
    log(f"=== floor {fname} (x={x}): {len(d):,} trades, "
        f"{d['market_id'].nunique():,} markets ===")
    for h in ["a ≤1d", "b 1-7d", "c 7-30d", "d 30-120d", "e >120d"]:
        r = slope_rows(d, d.hclass == h, fname, "pooled_horizon", h)
        if r:
            results.append(r)
    for tp in ["Economy", "Tech", "Politics", "Finance", "Crypto", "Sports",
               "Weather", "Esports", "Geopolitics", "Culture"]:
        r = slope_rows(d, d.topic == tp, fname, "pooled_topic", tp)
        if r:
            results.append(r)
    for tb in ["3 · 2025", "4 · 2026 Jan-Apr", "5 · 2026 May-Jun"]:
        r = slope_rows(d, (d.tb == tb) & (d.hclass == "e >120d"), fname,
                       "e120_by_period", tb)
        if r:
            results.append(r)
    # horse race, cluster FE, count-weighted
    hr = d[d.sim_k25_x.notna() & d.cluster_k200.notna() & d.life_d.notna()
           & d.vintage_year.notna()].copy()
    mk = hr.groupby("market_id", observed=True).agg(
        life_d=("life_d", "first"), usd_full=("usd_full", "first"),
        nov=("nov_tail", "first"), anch=("anchor_class", "first"),
        prior=("prior_instances", "first"), rules=("rules_len", "first"),
        neg=("neg_risk", "first"), vint=("vintage_year", "first"))
    z = pd.DataFrame(index=mk.index)
    for nm, v in [("z_ln_life", np.log(np.maximum(mk.life_d, 1e-5))),
                  ("z_ln_usd", np.log1p(mk.usd_full)),
                  ("z_ln_prior", np.log1p(mk.prior)),
                  ("z_ln_rules", np.log1p(mk.rules)),
                  ("z_vintage", mk.vint.astype(float))]:
        z[nm] = (v - v.mean()) / v.std()
    z["nov_tail_f"] = mk.nov.astype(float)
    z["anchored"] = mk.anch.isin(["data_feed", "official_scorer"]).astype(float)
    z["neg_risk_f"] = mk.neg.astype(float)
    for c in DIMS8:
        hr[c] = z[c].reindex(hr["market_id"]).to_numpy()
    jm = joint_model(hr, "cluster_k200", None, DIMS8, f"clusterFE|count|{fname}")
    for _, r in jm[jm.term.str.startswith("px_")].iterrows():
        results.append({"floor": fname, "analysis": "horse_race",
                        "slice": r.term[3:], "n": len(hr),
                        "n_markets": hr["market_id"].nunique(),
                        "beta": r.beta, "se": r.se, "t": r.t})
pd.DataFrame(results).to_parquet(OUT / "junk_filtered_results.parquet")
log(f"DONE junk_filtered_rerun in {(time.time()-t0)/60:.1f} min")
