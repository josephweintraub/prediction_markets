"""What drives the negative liquidity-calibration tilt?

Decomposes the liquidity gradient (higher $ volume -> slope below calibrated)
to test, among others, the near-certainty-trading hypothesis: are liquid
markets' slopes dragged down by clustered high-price trading once outcomes
are effectively decided (within the mature window)?

Per liquidity quintile (market-level ln $ volume over the estimation sample):
  1. full 10-decile calibration table (count + dollar)
  2. slope by lifecycle band within the mature window (25-40 / 40-60 / 60-80)
  3. interior slope (0.10 <= p <= 0.90) vs full slope  <- certainty-trade test
  4. the p>=0.90 band: share of trades/dollars, win rate, cal error
  5. within top quintile: slope by topic (top 8 by trades)
Outputs: liq_diagnostic_*.parquet under /mnt/data/learnability/output/.
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
from flb_per_slice import compute_3way_decile_table  # noqa: E402

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def slope_row(sub, label, extra=None):
    y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
    b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                   sub["proxyWallet"], sub["market_id"])
    bd, sed = threeway_cluster_slope(y, sub["price"], sub["usdc"],
                                     sub["trade_day"], sub["proxyWallet"],
                                     sub["market_id"])
    r = {"slice": label, "n": len(sub), "usd": float(sub["usdc"].sum()),
         "slope_dev": b, "se": se, "t": b / se if se > 0 else np.nan,
         "slope_dev_dol": bd, "se_dol": sed,
         "t_dol": bd / sed if sed > 0 else np.nan}
    if extra:
        r.update(extra)
    log(f"  {label:34s} n={len(sub):>10,} dev={b:+.4f} (t={r['t']:+.2f}) "
        f"$dev={bd:+.4f} (t={r['t_dol']:+.2f})")
    return r


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
    df = con.execute(f"""
        SELECT t.price, t.won, t.usdc, t.trade_day, t.proxyWallet, t.market_id,
               t.pos, coalesce(d.topic,'UNKNOWN') AS topic, d.usd_full
        FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
        JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
        WHERE t.pos BETWEEN 0.25 AND 0.80
          AND d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
          AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
    """).fetchdf()
    log(f"sample: {len(df):,} trades")
    df["ret"] = df["won"] - df["price"]  # engine decile table expects `ret`

    mk_usd = df.groupby("market_id", observed=True)["usd_full"].first()
    liq_q = np.ceil(np.log1p(mk_usd).rank(pct=True) * 5).clip(1, 5)
    df["liq_q"] = liq_q.reindex(df["market_id"]).to_numpy()
    df["decile"] = np.minimum(np.floor(df["price"] * 10), 9).astype(int) + 1

    dec_rows, slp_rows, top_rows = [], [], []
    for q in range(1, 6):
        sub = df[df["liq_q"] == q]
        # 1. decile table
        dec, _ = compute_3way_decile_table(sub, n_bins=10)
        dec.insert(0, "liq_q", q)
        dec_rows.append(dec)
        # 2. lifecycle bands
        for lo, hi, nm in [(0.25, 0.40, "pos25-40"), (0.40, 0.60, "pos40-60"),
                           (0.60, 0.80, "pos60-80")]:
            s = sub[(sub["pos"] >= lo) & (sub["pos"] < hi)]
            if len(s) >= 5000:
                slp_rows.append(slope_row(s, f"q{q}|{nm}", {"liq_q": q,
                                                            "cut": nm}))
        # 3. full vs interior slope
        slp_rows.append(slope_row(sub, f"q{q}|all", {"liq_q": q, "cut": "all"}))
        inter = sub[(sub["price"] >= 0.10) & (sub["price"] <= 0.90)]
        slp_rows.append(slope_row(inter, f"q{q}|interior_10_90",
                                  {"liq_q": q, "cut": "interior"}))
        # 4. the p>=0.90 band
        hp = sub[sub["price"] >= 0.90]
        top_rows.append({
            "liq_q": q, "n_hp": len(hp), "share_n": len(hp) / len(sub),
            "share_usd": float(hp["usdc"].sum()) / float(sub["usdc"].sum()),
            "mean_price": float(hp["price"].mean()),
            "win_rate": float(hp["won"].mean()),
            "cal_err": float((hp["won"] - hp["price"]).mean()),
            "win_rate_dol": float((hp["usdc"] * hp["won"]).sum()
                                  / hp["usdc"].sum()),
        })
        log(f"  q{q} p>=.90: share_n={top_rows[-1]['share_n']:.1%} "
            f"share_usd={top_rows[-1]['share_usd']:.1%} "
            f"win={top_rows[-1]['win_rate']:.3f} "
            f"vs price={top_rows[-1]['mean_price']:.3f}")

    # 5. topic decomposition inside q5
    q5 = df[df["liq_q"] == 5]
    topics = q5["topic"].value_counts().head(8).index
    trows = []
    for tp in topics:
        s = q5[q5["topic"] == tp]
        if len(s) >= 5000:
            trows.append(slope_row(s, f"q5|{tp}", {"topic": tp}))

    pd.concat(dec_rows, ignore_index=True).to_parquet(
        OUT / "liq_diagnostic_deciles.parquet")
    pd.DataFrame(slp_rows).to_parquet(OUT / "liq_diagnostic_slopes.parquet")
    pd.DataFrame(top_rows).to_parquet(OUT / "liq_diagnostic_hpband.parquet")
    pd.DataFrame(trows).to_parquet(OUT / "liq_diagnostic_q5topics.parquet")
    log(f"DONE liq_diagnostic in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
