"""Study A — per-lifecycle-decile FLB shape.

For headline dims, slice trades into 10 lifecycle-position bins and compute
the D10-D1 spread per (slice, lifecycle_bin). One trades scan; no 3-way SE
(rough exploration of the SHAPE — rigorous comparison comes in Study B).
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
from pathlib import Path
import numpy as np
import pandas as pd
from config import OUTPUT_DIR as PIPELINE_OUTPUT_DIR
from data_loader import get_connection

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600

HEADLINE_DIMS = [
    "dim_text_novelty",
    "dim_family_size_x_vol",
    "dim_prior_settlements_bin__event_template",
    "dim_event_family_size",
    "dim_primary_category",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log("=== Study A — per-lifecycle-decile FLB shape ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        SELECT * FROM read_parquet('{WF_CACHE}')
    """)
    con.execute("DROP VIEW IF EXISTS trades")
    con.execute("DROP VIEW IF EXISTS trades_buy")
    con.execute(f"""
        CREATE VIEW trades AS
        SELECT *, usdcSize / NULLIF(price, 0) AS size
        FROM trades_raw
        WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
          AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman = 1)
    """)
    con.execute("CREATE VIEW trades_buy AS SELECT * FROM trades WHERE side = 'BUY'")

    ENRICHED = Path("/home/ubuntu/pipeline/output/market_resolutions_enriched.parquet")
    res_enriched = pd.read_parquet(ENRICHED)
    con.register("_res_enriched_df", res_enriched)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _closed_markets AS
        SELECT conditionId, winning_outcome
        FROM _res_enriched_df
        WHERE winning_outcome IS NOT NULL
    """)
    con.unregister("_res_enriched_df")

    df = pd.read_parquet(V4_DIMS)
    cols = ["token_id", "condition_id"] + HEADLINE_DIMS
    cd = df[cols].copy()
    for c in HEADLINE_DIMS:
        cd[c] = cd[c].astype("string")
    con.register("_contract_dims_df", cd)
    con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
    con.unregister("_contract_dims_df")
    log(f"  {len(cd):,} contracts in dims")

    all_results = []
    for dim in HEADLINE_DIMS:
        log(f"--- Scanning {dim} per lifecycle-decile ---")
        t0 = time.time()
        # Per-trade lifecycle bin = floor((t.ts - mkt_start) / mkt_duration * 10)
        sql = f"""
        WITH mkt_life AS (
            SELECT conditionId,
                   MIN(timestamp) AS mkt_start,
                   GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
            FROM trades_buy
            GROUP BY conditionId
        ),
        td AS (
            SELECT
                cd.{dim} AS slice,
                LEAST(GREATEST(FLOOR((t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration * 10)::INT, 0), 9) AS lifecycle_bin,
                LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS price_decile,
                CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END AS ret
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            INNER JOIN mkt_life ml ON t.conditionId = ml.conditionId
            INNER JOIN _contract_dims cd ON t.conditionId = cd.token_id
            WHERE t.price > 0.01 AND t.price < 0.99
              AND cd.{dim} IS NOT NULL
        )
        SELECT slice, lifecycle_bin, price_decile,
               AVG(ret) AS mean_ret,
               COUNT(*) AS n
        FROM td
        GROUP BY slice, lifecycle_bin, price_decile
        """
        grid = con.execute(sql).fetchdf()
        log(f"  done in {time.time()-t0:.1f}s, {len(grid):,} cells")
        grid["dim"] = dim
        all_results.append(grid)

    combined = pd.concat(all_results, ignore_index=True)
    combined.to_parquet(OUT / "audit_lifecycle_decile_shape.parquet", index=False)
    log(f"Saved {len(combined):,} (dim, slice, lifecycle_bin, price_decile) cells")

    # Compute spreads
    pivot = combined.pivot_table(
        index=["dim", "slice", "lifecycle_bin"],
        columns="price_decile",
        values="mean_ret",
        aggfunc="first",
    ).reset_index()
    pivot["spread"] = pivot[10] - pivot[1]
    n_by_bin = combined.groupby(["dim", "slice", "lifecycle_bin"])["n"].sum().reset_index()
    spread_summary = pivot[["dim", "slice", "lifecycle_bin", "spread"]].merge(
        n_by_bin, on=["dim", "slice", "lifecycle_bin"]
    )
    spread_summary.to_parquet(OUT / "audit_lifecycle_decile_spreads.parquet", index=False)
    spread_summary.to_csv(OUT / "audit_lifecycle_decile_spreads.csv", index=False)
    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
