"""Study B — rigorous 3-way SE FLB across candidate lifecycle windows.

For headline dims, run the existing flb_per_slice_v3.run_flb_per_slice across
several candidate windows. Output side-by-side spread comparison.

Candidate windows (lo, hi):
  full  : 0.00, 1.00  — no selection
  10-90 : 0.10, 0.90  — exclude extreme tails (selection-resistant)
  25-75 : 0.25, 0.75  — middle half
  50-80 : 0.50, 0.80  — current canonical
  80-100: 0.80, 1.00  — closing tail
  20-50 : 0.20, 0.50  — early-mid

Each window × each dim → one FLB call. Output saved with window label.
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
sys.path.insert(0, "/home/ubuntu/learnability")
from pathlib import Path
import numpy as np
import pandas as pd
from config import OUTPUT_DIR as PIPELINE_OUTPUT_DIR
from data_loader import get_connection
from learnability import flb_per_slice_v3 as flb

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10

CANDIDATE_WINDOWS = [
    ("full",    0.00, 1.00),
    ("10-90",   0.10, 0.90),
    ("25-75",   0.25, 0.75),
    ("50-80",   0.50, 0.80),
    ("80-100",  0.80, 1.00),
    ("20-50",   0.20, 0.50),
]

HEADLINE_DIMS = [
    "dim_text_novelty",
    "dim_family_size_x_vol",
    "dim_prior_settlements_bin__event_template",
    "dim_event_family_size",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log("=== Study B — rigorous FLB across candidate lifecycle windows ===")

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

    all_summary = []
    for win_name, lo, hi in CANDIDATE_WINDOWS:
        log(f"\n=== Window {win_name} ({lo}, {hi}) ===")
        for dim in HEADLINE_DIMS:
            log(f"--- FLB on {dim} ---")
            t0 = time.time()
            try:
                calib, summary = flb.run_flb_per_slice(
                    con, dim,
                    lo=lo, hi=hi,
                    min_trades=MIN_TRADES_PER_SLICE,
                    n_bins=N_BINS, verbose=True,
                )
                log(f"  done in {time.time()-t0:.1f}s")
                if isinstance(summary, pd.DataFrame) and len(summary):
                    summary["window"] = win_name
                    summary["lo"] = lo
                    summary["hi"] = hi
                    all_summary.append(summary)
            except Exception as e:
                log(f"  ERROR: {e}")

    if all_summary:
        out_df = pd.concat(all_summary, ignore_index=True)
        out_df.to_parquet(OUT / "audit_lifecycle_window_compare.parquet", index=False)
        out_df.to_csv(OUT / "audit_lifecycle_window_compare.csv", index=False)
        log(f"\nSaved {len(out_df):,} rows")

    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
