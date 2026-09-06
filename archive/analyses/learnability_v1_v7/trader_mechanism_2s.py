"""Trader-composition mechanism tests on the TWO-SIDED tape (primary).

Same designs as trader_mechanism.py, but each wallet's activity includes all
its positions: buys as-is, sells as complement-buys at 1-p (from
side_symmetry_trades.parquet). Within-wallet experience/mechanism designs must
not condition on the buy side only — a wallet that mostly provides liquidity
by selling would otherwise be invisible or half-observed.

A. within-wallet joint model (wallet FE x [1,p]): does the SAME wallet trade
   more miscalibrated in tail/thin/long markets?
B. participant splits: are tail/thin participants biased OUTSIDE those cells?
Outputs: trader_mechanism2s_*.parquet under /mnt/data/learnability/output/.
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


def slope_row(sub, label):
    y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
    b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                   sub["proxyWallet"], sub["market_id"])
    bd, sed = threeway_cluster_slope(y, sub["price"], sub["usdc"],
                                     sub["trade_day"], sub["proxyWallet"],
                                     sub["market_id"])
    log(f"  {label:46s} n={len(sub):>11,} dev={b:+.4f} "
        f"(t={b/se if se>0 else float('nan'):+.2f})")
    return {"slice": label, "n": len(sub), "usd": float(sub["usdc"].sum()),
            "n_wallets": sub["proxyWallet"].nunique(),
            "slope_dev": b, "se": se, "t": b / se if se > 0 else np.nan,
            "slope_dev_dol": bd, "se_dol": sed,
            "t_dol": bd / sed if sed > 0 else np.nan}


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
    df = con.execute(f"""
        SELECT s.price, s.won, s.usdc, s.trade_day, s.proxyWallet, s.market_id,
               s.side, d.life_d, d.usd_full,
               (d.novelty_vint_decile = 1) AS nov_tail
        FROM read_parquet('{SIDE_INT}') s
        JOIN read_parquet('{DIMS}') d ON s.market_id = d.condition_id
        WHERE d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
          AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
    """).fetchdf()
    log(f"two-sided sample: {len(df):,} rows, "
        f"{df['proxyWallet'].nunique():,} wallets "
        f"(buy {(df['side']=='buy').mean():.1%})")

    mk = df.groupby("market_id", observed=True).agg(
        life_d=("life_d", "first"), usd_full=("usd_full", "first"),
        nov_tail=("nov_tail", "first"))
    z_life = np.log(np.maximum(mk["life_d"], 1e-5))
    z_life = (z_life - z_life.mean()) / z_life.std()
    ln_usd = np.log1p(mk["usd_full"])
    liq_q = np.ceil(ln_usd.rank(pct=True) * 5).clip(1, 5)
    z_usd = (ln_usd - ln_usd.mean()) / ln_usd.std()
    df["z_ln_life"] = z_life.reindex(df["market_id"]).to_numpy()
    df["z_ln_usd"] = z_usd.reindex(df["market_id"]).to_numpy()
    df["nov_tail"] = mk["nov_tail"].astype(float).reindex(
        df["market_id"]).to_numpy()
    df["liq_q"] = liq_q.reindex(df["market_id"]).to_numpy()

    W_DIMS = ["nov_tail", "z_ln_usd", "z_ln_life"]

    log("=== A: within-wallet, two-sided ===")
    frames = [joint_model(df.assign(_fe=df["proxyWallet"]), "_fe", None,
                          W_DIMS, "walletFE|count|all|2s")]
    ntr = df.groupby("proxyWallet", observed=True)["price"].transform("size")
    act = df[ntr >= 20]
    log(f"  active-wallet subsample: {len(act):,} rows, "
        f"{act['proxyWallet'].nunique():,} wallets")
    frames.append(joint_model(act.assign(_fe=act["proxyWallet"]), "_fe", None,
                              W_DIMS, "walletFE|count|ge20|2s"))
    frames.append(joint_model(act.assign(_fe=act["proxyWallet"]), "_fe",
                              "usdc", W_DIMS, "walletFE|dollar|ge20|2s"))
    pd.concat(frames, ignore_index=True).to_parquet(
        OUT / "trader_mechanism2s_walletfe.parquet")

    log("=== B: participant splits, two-sided ===")
    rows = []
    tail_ct = df[df["nov_tail"] == 1].groupby("proxyWallet", observed=True).size()
    thin_ct = df[df["liq_q"] <= 2].groupby("proxyWallet", observed=True).size()
    for name, ct, in_mask, out_mask in [
        ("tail", tail_ct, df["nov_tail"] == 1, df["nov_tail"] == 0),
        ("thin", thin_ct, df["liq_q"] <= 2, df["liq_q"] >= 4),
    ]:
        for thr in (1, 5):
            part = set(ct[ct >= thr].index)
            is_part = df["proxyWallet"].isin(part)
            rows.append(slope_row(df[out_mask & is_part],
                                  f"{name}-participants(>={thr}) OUTSIDE {name}"))
            rows.append(slope_row(df[out_mask & ~is_part],
                                  f"non-participants OUTSIDE {name} (thr={thr})"))
        rows.append(slope_row(df[in_mask], f"all INSIDE {name}"))
    pd.DataFrame(rows).to_parquet(OUT / "trader_mechanism2s_splits.parquet")

    part5 = set(tail_ct[tail_ct >= 5].index)
    is5 = df["proxyWallet"].isin(part5)
    desc = {
        "n_wallets_total": int(df["proxyWallet"].nunique()),
        "n_wallets_tail_ge1": int(len(tail_ct)),
        "n_wallets_tail_ge5": int(len(part5)),
        "tail_dollar_share_from_ge5": float(
            df[(df["nov_tail"] == 1) & is5]["usdc"].sum()
            / max(df[df["nov_tail"] == 1]["usdc"].sum(), 1)),
        "ge5_outside_share_of_their_usd": float(
            df[is5 & (df["nov_tail"] == 0)]["usdc"].sum()
            / max(df[is5]["usdc"].sum(), 1)),
    }
    pd.DataFrame([desc]).to_parquet(OUT / "trader_mechanism2s_desc.parquet")
    log(str(desc))
    log(f"DONE trader_mechanism_2s in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
