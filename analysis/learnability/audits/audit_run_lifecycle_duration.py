"""Phase 2.2 audit — lifecycle window sensitivity by contract duration.

Hypothesis: the "50-80% canonical window" means different things for a 1-hour
crypto-Up/Down contract vs a 6-month election. Test by running FLB on
dim_text_novelty separately within each contract_horizon bucket (<1h, 1h-1d,
1d-1w, 1w-1m, >1m) at the 50-80% window.

If the spread changes sharply across duration buckets, the unified "50-80%"
interpretation is broken.
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
from learnability import flb_per_slice as flb

PREFIX = os.environ.get("AUDIT_PREFIX", "audit_lifecycle_duration")
LO = float(os.environ.get("AUDIT_LO", "0.50"))
HI = float(os.environ.get("AUDIT_HI", "0.80"))

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10

DURATION_BUCKETS = ["<1h", "1h-1d", "1d-1w", "1wk-1mo", ">1mo"]
DIMS_TO_AUDIT = ["dim_text_novelty", "dim_text_neighbors_strict", "dim_family_size_x_vol"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log(f"=== audit_run_lifecycle_duration — prefix={PREFIX}, window=[{LO},{HI}] ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    log(f"Loading cached wallet_flags…")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        SELECT * FROM read_parquet('{WF_CACHE}')
    """)

    log("Replacing trades view (bots out, updown KEPT for this audit)…")
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

    log("Loading market resolutions…")
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

    log(f"Loading v4 contract_dimensions…")
    df = pd.read_parquet(V4_DIMS)
    log(f"  {len(df):,} contracts")

    # Run FLB per duration bucket
    all_calib, all_summary = [], []
    for bucket in DURATION_BUCKETS:
        log(f"\n=== Duration bucket: {bucket} ===")
        df_bucket = df[df["dim_contract_horizon"] == bucket].copy()
        log(f"  {len(df_bucket):,} contracts in this bucket")
        if len(df_bucket) < 1000:
            log(f"  SKIP — too few contracts")
            continue

        # Build a composite dim that combines duration with the target dim
        for target_dim in DIMS_TO_AUDIT:
            composite_dim = f"dim_{bucket.replace('-','_to_').replace('<','lt').replace('>','gt')}__x__{target_dim}"
            df_bucket[composite_dim] = df_bucket[target_dim]

            cols = ["token_id", "condition_id", composite_dim]
            contract_dims_for_sql = df_bucket[cols].copy()
            contract_dims_for_sql[composite_dim] = contract_dims_for_sql[composite_dim].astype("string")
            con.register("_contract_dims_df", contract_dims_for_sql)
            con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
            con.unregister("_contract_dims_df")

            t0 = time.time()
            try:
                calib, summary = flb.run_flb_per_slice(
                    con, composite_dim,
                    lo=LO, hi=HI,
                    min_trades=MIN_TRADES_PER_SLICE,
                    n_bins=N_BINS, verbose=True,
                )
                log(f"  {bucket} × {target_dim} done in {time.time()-t0:.1f}s")
                if isinstance(calib, pd.DataFrame) and len(calib):
                    calib["duration_bucket"] = bucket
                    all_calib.append(calib)
                if isinstance(summary, pd.DataFrame) and len(summary):
                    summary["duration_bucket"] = bucket
                    all_summary.append(summary)
            except Exception as e:
                log(f"  ERROR: {e}")

    if all_calib:
        calib_df = pd.concat(all_calib, ignore_index=True)
        summary_df = pd.concat(all_summary, ignore_index=True)
        calib_df.to_parquet(OUT / f"{PREFIX}_flb_per_slice.parquet", index=False)
        summary_df.to_parquet(OUT / f"{PREFIX}_spread_summary.parquet", index=False)
        summary_df.to_csv(OUT / f"{PREFIX}_spread_summary.csv", index=False)
    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
