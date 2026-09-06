"""Phase 1 v5 driver — full implementation of audit recommendations.

Changes from v4:
- UP/DOWN MARKETS EXCLUDED from primary trades view (audit Phase 2.1 finding).
- New `dim_text_novelty` uses fixed-threshold bins (audit §9).
- New `dim_market_type` reports up/down vs non-up/down separately.
- Run for 3 lifecycle windows: 25-80% (mature), 80-100% (closing), 0-100% (baseline).
- All 14 dims + dim_market_type = 22 dims total per window.

ENV VARS:
  V5_PREFIX   : output prefix (e.g. "phase1_v5_25_80")
  V5_LO       : lifecycle low
  V5_HI       : lifecycle high
  V5_INCLUDE_UPDOWN : if "1", keep up/down in (for sensitivity only)

2026-07-03: resolutions spine switched to market_flags.parquet
(scripts/build_market_flags.py) — the old enriched file covered only ~49% of
extended-set trade rows, and trades' eventSlug is empty on newer markets so
the eventSlug-based up/down exclusion no longer works alone; up/down is now
excluded market-level via the flag inside _closed_markets.
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
from pathlib import Path
import numpy as np
import pandas as pd

from config import OUTPUT_DIR as PIPELINE_OUTPUT_DIR
from data_loader import get_connection
from learnability import flb_per_slice as flb
from learnability import dimensions_v5 as v5

PREFIX = os.environ.get("V5_PREFIX", "phase1_v5_25_80")
LO = float(os.environ.get("V5_LO", "0.25"))
HI = float(os.environ.get("V5_HI", "0.80"))
INCLUDE_UPDOWN = os.environ.get("V5_INCLUDE_UPDOWN", "0") == "1"

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
V5_DIMS_PATH = OUT / "phase1_v5_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log(f"=== run_phase1_v5 — prefix={PREFIX}, window=[{LO},{HI}], updown={'KEPT' if INCLUDE_UPDOWN else 'EXCLUDED'} ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        SELECT * FROM read_parquet('{WF_CACHE}')
    """)

    log("Replacing trades view…")
    con.execute("DROP VIEW IF EXISTS trades")
    con.execute("DROP VIEW IF EXISTS trades_buy")
    if INCLUDE_UPDOWN:
        view_sql = f"""
            CREATE VIEW trades AS
            SELECT *, usdcSize / NULLIF(price, 0) AS size
            FROM trades_raw
            WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
              AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman = 1)
        """
    else:
        view_sql = f"""
            CREATE VIEW trades AS
            SELECT *, usdcSize / NULLIF(price, 0) AS size
            FROM trades_raw
            WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
              AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman = 1)
              AND eventSlug NOT LIKE '%updown%'
              AND eventSlug NOT LIKE '%up-or-down%'
        """
    con.execute(view_sql)
    con.execute("CREATE VIEW trades_buy AS SELECT * FROM trades WHERE side = 'BUY'")
    n_buy = con.execute("SELECT COUNT(*) FROM trades_buy").fetchone()[0]
    log(f"  trades_buy: {n_buy:,}")

    # 2026-07-03: market_flags.parquet replaces market_resolutions_enriched
    # (stale — covered only ~49% of extended-set trade rows) and carries a
    # MARKET-level up/down flag from Gamma metadata (trades' eventSlug is
    # empty on newer markets, so the eventSlug LIKE filter above no longer
    # excludes the up/down series on its own). Built by
    # scripts/build_market_flags.py; the INNER JOIN against _closed_markets
    # in every calibration query is what enforces the exclusion.
    FLAGS = Path("/mnt/data/pipeline_output/market_flags.parquet")
    updown_clause = "" if INCLUDE_UPDOWN else "AND NOT is_updown"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _closed_markets AS
        SELECT token_id AS conditionId, winning_outcome
        FROM read_parquet('{FLAGS}')
        WHERE winning_outcome IS NOT NULL {updown_clause}
    """)

    # Build v5 dims: load v4, replace dim_text_novelty with fixed bins, add dim_market_type
    if V5_DIMS_PATH.exists():
        log(f"Loading cached v5 dims from {V5_DIMS_PATH}…")
        df = pd.read_parquet(V5_DIMS_PATH)
    else:
        log(f"Building v5 dims from v4 base…")
        df = pd.read_parquet(V4_DIMS)
        df = v5.add_dim_text_novelty_v5(df)
        df = v5.add_dim_market_type(df)
        df.to_parquet(V5_DIMS_PATH, index=False)
        log(f"  saved v5 dims to {V5_DIMS_PATH}")
    log(f"  {len(df):,} contracts × {len(df.columns)} cols")
    log(f"  dim_text_novelty distribution:\n{df['dim_text_novelty'].value_counts().to_string()}")

    # Register all dims
    cols = ["token_id", "condition_id"] + v5.V5_DIMS
    cd = df[cols].copy()
    for c in v5.V5_DIMS:
        cd[c] = cd[c].astype("string")
    con.register("_contract_dims_df", cd)
    con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
    con.unregister("_contract_dims_df")

    # FLB per dim
    all_calib, all_summary = [], []
    for dim_col in v5.V5_DIMS:
        log(f"--- FLB on {dim_col} ---")
        t0 = time.time()
        try:
            calib, summary = flb.run_flb_per_slice(
                con, dim_col,
                lo=LO, hi=HI,
                min_trades=MIN_TRADES_PER_SLICE,
                n_bins=N_BINS, verbose=True,
            )
            log(f"  done in {time.time()-t0:.1f}s")
            if isinstance(calib, pd.DataFrame) and len(calib):
                all_calib.append(calib)
            if isinstance(summary, pd.DataFrame) and len(summary):
                all_summary.append(summary)
        except Exception as e:
            log(f"  ERROR: {e}")

    if all_calib:
        calib_df = pd.concat(all_calib, ignore_index=True)
        summary_df = pd.concat(all_summary, ignore_index=True)
        calib_df.to_parquet(OUT / f"{PREFIX}_flb_per_slice.parquet", index=False)
        summary_df.to_parquet(OUT / f"{PREFIX}_spread_summary.parquet", index=False)
        summary_df.to_csv(OUT / f"{PREFIX}_spread_summary.csv", index=False)
        calib_df.to_csv(OUT / f"{PREFIX}_flb_per_slice.csv", index=False)

    (OUT / f"{PREFIX}_meta.json").write_text(json.dumps({
        "lifecycle_lo": LO,
        "lifecycle_hi": HI,
        "include_updown": INCLUDE_UPDOWN,
        "n_trades_buy": int(n_buy),
        "n_contracts_in_dims": int(len(df)),
        "v5_dims": v5.V5_DIMS,
    }, indent=2))
    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
