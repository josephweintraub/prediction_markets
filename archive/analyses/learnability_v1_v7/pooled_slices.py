"""Pooled (whole-window) slices for the write-up + the <=1d composition check.

1. Horizon-class slope deviations pooled over the full 2022-2026 sample.
2. Topic slope deviations pooled over the full sample.
3. Composition of the <=1d horizon class (topic x mechanic mix) to answer
   whether it is 'just crypto' (up/down series are already excluded at the
   market level; what remains is measured here).
Outputs: pooled_slices_{horizon,topics,short_comp}.parquet
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

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
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
    log(f"  {label:30s} n={len(sub):>10,} dev={b:+.4f} (t={b/se:+.2f})")
    return {"slice": label, "n": len(sub), "n_markets": sub["market_id"].nunique(),
            "usd": float(sub["usdc"].sum()),
            "slope_dev": b, "se": se, "t": b / se if se > 0 else np.nan,
            "slope_dev_dol": bd, "se_dol": sed,
            "t_dol": bd / sed if sed > 0 else np.nan}


con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
df = con.execute(f"""
    SELECT t.price, t.won, t.usdc, t.trade_day, t.proxyWallet, t.market_id,
           t.hclass, coalesce(d.topic,'UNKNOWN') AS topic
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    LEFT JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
    WHERE t.pos BETWEEN 0.25 AND 0.80
""").fetchdf()
log(f"pooled mature sample: {len(df):,}")

rows = [slope_row(df[df.hclass == h], h) for h in
        ["a ≤1d", "b 1-7d", "c 7-30d", "d 30-120d", "e >120d"]]
rows.append(slope_row(df, "all horizons pooled"))
pd.DataFrame(rows).to_parquet(OUT / "pooled_slices_horizon.parquet")

trows = []
for tp, sub in df.groupby("topic", observed=True):
    if len(sub) >= 5000 and tp not in ("Other",):
        trows.append(slope_row(sub, tp))
pd.DataFrame(trows).to_parquet(OUT / "pooled_slices_topics.parquet")

comp = con.execute(f"""
    SELECT coalesce(d.topic,'UNKNOWN') AS topic, coalesce(d.mechanic,'?') AS mechanic,
           count(*) n, count(DISTINCT t.market_id) n_mkts, sum(t.usdc) usd
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    LEFT JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
    WHERE t.pos BETWEEN 0.25 AND 0.80 AND t.hclass = 'a ≤1d'
    GROUP BY 1,2 ORDER BY 3 DESC LIMIT 15
""").fetchdf()
comp.to_parquet(OUT / "pooled_slices_short_comp.parquet")
log("<=1d composition:\n" + comp.to_string())
log("DONE pooled_slices")
