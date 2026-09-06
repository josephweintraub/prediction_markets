"""MP-novelty robustness check: does the novelty-tail result survive
mutual-proximity (hubness-corrected) rescaling?

Uses novelty_mp_q.parquet from compute_novelty_mp.py. Re-derives the
within-vintage tail flag under MP, reports agreement with the raw-cosine
flag, the univariate tail slope, and the cluster-FE joint model with the
MP tail swapped in for the raw tail.
Outputs: mp_check_results.parquet + prints.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "learnability"))
from horizon_flb_v2 import threeway_cluster_slope  # noqa: E402
from horse_race_v1 import joint_model  # noqa: E402

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
MP = "/mnt/data/embedding_difficulty/novelty_mp_q.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

    mkt = con.execute(f"""
        SELECT d.condition_id, d.vintage_year, d.novelty_vint_decile,
               d.life_d, d.usd_full, d.anchor_class, d.prior_instances,
               d.rules_len, d.neg_risk, m.mp_k25_x
        FROM read_parquet('{DIMS}') d
        JOIN read_parquet('{MP}') m ON d.condition_id = m.market_id
        WHERE d.n_trades_full >= 1 AND d.sim_k25_x IS NOT NULL
          AND d.cluster_k200 IS NOT NULL AND d.life_d IS NOT NULL
          AND d.vintage_year IS NOT NULL AND m.mp_k25_x IS NOT NULL
    """).fetchdf()
    mkt["mp_decile"] = (mkt.groupby("vintage_year")["mp_k25_x"]
                        .transform(lambda s: np.ceil(
                            s.rank(pct=True, method="first") * 10)
                            .clip(1, 10)))
    mkt["mp_tail"] = (mkt["mp_decile"] == 1).astype(float)
    raw_tail = (mkt["novelty_vint_decile"] == 1)
    agree = pd.crosstab(raw_tail, mkt["mp_tail"] == 1)
    jacc = ((raw_tail & (mkt["mp_tail"] == 1)).sum()
            / (raw_tail | (mkt["mp_tail"] == 1)).sum())
    log(f"markets: {len(mkt):,}; raw-tail {raw_tail.sum():,}, "
        f"mp-tail {(mkt['mp_tail']==1).sum():,}, jaccard={jacc:.3f}")
    log("\n" + agree.to_string())

    df = con.execute(f"""
        SELECT t.price, t.won, t.usdc, t.trade_day, t.proxyWallet, t.market_id,
               d.cluster_k200
        FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
        JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
        WHERE t.pos BETWEEN 0.25 AND 0.80
          AND d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
          AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
    """).fetchdf()
    mkti = mkt.set_index("condition_id")
    df = df[df["market_id"].isin(mkti.index)]
    log(f"trades: {len(df):,}")

    # market-level z dims, mirroring horse_race_v1
    z = pd.DataFrame(index=mkti.index)
    for name, v in [("z_ln_life", np.log(np.maximum(mkti["life_d"], 1e-5))),
                    ("z_ln_usd", np.log1p(mkti["usd_full"])),
                    ("z_ln_prior", np.log1p(mkti["prior_instances"])),
                    ("z_ln_rules", np.log1p(mkti["rules_len"].fillna(0))),
                    ("z_vintage", mkti["vintage_year"].astype(float))]:
        z[name] = (v - v.mean()) / v.std()
    z["mp_tail"] = mkti["mp_tail"]
    z["anchored"] = mkti["anchor_class"].isin(
        ["data_feed", "official_scorer"]).astype(float)
    z["neg_risk"] = mkti["neg_risk"].fillna(False).astype(float)
    for c in z.columns:
        df[c] = z[c].reindex(df["market_id"]).to_numpy()

    # univariate MP tail slope
    rows = []
    for flag, nm in [(1.0, "mp_tail=1"), (0.0, "mp_tail=0")]:
        sub = df[df["mp_tail"] == flag]
        y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
        b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                       sub["proxyWallet"], sub["market_id"])
        log(f"  {nm}: n={len(sub):,} dev={b:+.4f} (t={b/se:+.2f})")
        rows.append({"spec": "univariate", "term": nm, "beta": b, "se": se,
                     "t": b / se if se > 0 else np.nan})

    # cluster-FE joint model with mp_tail
    MP_DIMS = ["z_ln_life", "z_ln_usd", "mp_tail", "anchored",
               "z_ln_prior", "z_ln_rules", "neg_risk", "z_vintage"]
    r = joint_model(df, "cluster_k200", None, MP_DIMS, "clusterFE|count|mp")
    rows.append(None)
    res = pd.concat([pd.DataFrame([x for x in rows if x is not None]), r],
                    ignore_index=True)
    res.to_parquet(OUT / "mp_check_results.parquet")
    log(f"DONE mp_check in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
