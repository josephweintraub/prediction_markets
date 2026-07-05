"""Side symmetry: does the BUY-side convention select on the aggressive side?

Every fill appears twice in the tape (buyer row + seller row). A seller of
outcome X at price p holds the complement position: economically a buyer of
(1-X) at 1-p, with return (1-won_X) - (1-p). Pooling both sides therefore
adds only mirror-image observations and preserves any slope by construction —
the informative object is the CONTRAST between the buyer-side calibration
curve and the seller-as-complement-buyer curve (plus the maker/taker split,
which the tape's is_maker flag gives directly). Divergence = the standard
BUY-side convention has been measuring one side of a selection.

Build: one scan of trades_clean producing a mature-window two-sided table
(standard filters applied to each row's own wallet; token lifecycle from the
canonical BUY-based definition so windows mean the same thing on both sides).
Analysis: CGM 3-way clustered slope deviations for side x {all, maker, taker},
side x liquidity quintile, side x novelty tail.
Outputs: side_symmetry_trades.parquet (intermediate), side_symmetry_results.parquet.
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

TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
MARKET_FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
WALLET_FLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = Path("/mnt/data/learnability/output")
INTERMEDIATE = OUT / "side_symmetry_trades.parquet"


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def build(con):
    if INTERMEDIATE.exists():
        log("side intermediate exists, reusing")
        return
    log("building two-sided mature intermediate (one heavy scan) ...")
    con.execute(f"""
        CREATE OR REPLACE VIEW trades_all AS
        SELECT * FROM read_parquet('{TRADES_GLOB}', hive_partitioning=1)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nonhuman AS
        SELECT proxyWallet FROM read_parquet('{WALLET_FLAGS}') WHERE is_nonhuman
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE flags AS
        SELECT token_id, market_id, winning_outcome
        FROM read_parquet('{MARKET_FLAGS}')
        WHERE winning_outcome IS NOT NULL AND NOT is_updown
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE tok_life AS
        SELECT conditionId,
               MIN(timestamp) AS tok_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS tok_dur
        FROM trades_all
        WHERE side = 'BUY'
          AND coalesce(eventSlug,'') NOT LIKE '%updown%'
          AND coalesce(eventSlug,'') NOT LIKE '%up-or-down%'
          AND proxyWallet NOT IN (SELECT proxyWallet FROM nonhuman)
        GROUP BY conditionId
    """)
    con.execute(f"""
        COPY (
            SELECT
                t.proxyWallet, f.market_id,
                DATE_TRUNC('day', to_timestamp(t.timestamp)) AS trade_day,
                CASE WHEN t.side='BUY' THEN t.price ELSE 1.0 - t.price END AS price,
                CASE WHEN t.side='BUY'
                     THEN CAST(t.outcome = f.winning_outcome AS INT)
                     ELSE 1 - CAST(t.outcome = f.winning_outcome AS INT)
                END AS won,
                t.usdcSize AS usdc,
                lower(t.side) AS side,
                t.is_maker
            FROM trades_all t
            JOIN flags f    ON t.conditionId = f.token_id
            JOIN tok_life l ON t.conditionId = l.conditionId
            WHERE coalesce(t.eventSlug,'') NOT LIKE '%updown%'
              AND coalesce(t.eventSlug,'') NOT LIKE '%up-or-down%'
              AND t.proxyWallet NOT IN (SELECT proxyWallet FROM nonhuman)
              AND t.price > 0.01 AND t.price < 0.99
              AND (t.timestamp - l.tok_start)::FLOAT / l.tok_dur
                  BETWEEN 0.25 AND 0.80
        ) TO '{INTERMEDIATE}' (FORMAT PARQUET)
    """)
    log("intermediate written")


def slope_row(sub, label):
    y = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
    b, se = threeway_cluster_slope(y, sub["price"], None, sub["trade_day"],
                                   sub["proxyWallet"], sub["market_id"])
    bd, sed = threeway_cluster_slope(y, sub["price"], sub["usdc"],
                                     sub["trade_day"], sub["proxyWallet"],
                                     sub["market_id"])
    log(f"  {label:36s} n={len(sub):>11,} dev={b:+.4f} "
        f"(t={b/se if se>0 else float('nan'):+.2f}) $dev={bd:+.4f}")
    return {"slice": label, "n": len(sub), "usd": float(sub["usdc"].sum()),
            "mean_ret": float(y.mean()),
            "mean_ret_dol": float((sub["usdc"].to_numpy() * y).sum()
                                  / sub["usdc"].sum()),
            "slope_dev": b, "se": se, "t": b / se if se > 0 else np.nan,
            "slope_dev_dol": bd, "se_dol": sed,
            "t_dol": bd / sed if sed > 0 else np.nan}


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
    build(con)

    df = con.execute(f"""
        SELECT s.*, (d.novelty_vint_decile = 1) AS nov_tail, d.usd_full
        FROM read_parquet('{INTERMEDIATE}') s
        JOIN read_parquet('{DIMS}') d ON s.market_id = d.condition_id
        WHERE d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
          AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
    """).fetchdf()
    log(f"two-sided sample: {len(df):,} rows "
        f"(buy {(df['side']=='buy').sum():,} / sell {(df['side']=='sell').sum():,})")

    mk_usd = df.groupby("market_id", observed=True)["usd_full"].first()
    liq_q = np.ceil(np.log1p(mk_usd).rank(pct=True) * 5).clip(1, 5)
    df["liq_q"] = liq_q.reindex(df["market_id"]).to_numpy()

    rows = []
    for side in ("buy", "sell"):
        s = df[df["side"] == side]
        lbl = "buyer" if side == "buy" else "seller-as-complement"
        rows.append(slope_row(s, f"{lbl}|all"))
        rows.append(slope_row(s[s["is_maker"]], f"{lbl}|maker"))
        rows.append(slope_row(s[~s["is_maker"]], f"{lbl}|taker"))
        for q in (1, 2, 3, 4, 5):
            sub = s[s["liq_q"] == q]
            if len(sub) >= 5000:
                rows.append(slope_row(sub, f"{lbl}|liq_q{q}"))
        rows.append(slope_row(s[s["nov_tail"] == True], f"{lbl}|nov_tail"))  # noqa: E712
        rows.append(slope_row(s[s["nov_tail"] == False], f"{lbl}|not_tail"))  # noqa: E712
    pd.DataFrame(rows).to_parquet(OUT / "side_symmetry_results.parquet")
    log(f"DONE side_symmetry in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
