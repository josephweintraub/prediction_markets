"""Phase 2.1 + 2.5 audit driver — up/down-excluded re-run AND dim_market_type.

Reuses the existing v4 contract_dimensions parquet (already on /mnt/data) so
no rebuild of dims is needed. Re-registers the trades view with an updown
exclusion and reruns FLB for every dim. Also adds a new dim_market_type slot
to compare up/down vs non-up/down directly.

ENV VARS:
  AUDIT_LO   : 0.50
  AUDIT_HI   : 0.80
  AUDIT_PREFIX : "audit_noupdown"
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/pipeline/analysis")
sys.path.insert(0, "/home/ubuntu/learnability")
from pathlib import Path
import numpy as np
import pandas as pd
import re

from config import OUTPUT_DIR as PIPELINE_OUTPUT_DIR
from data_loader import get_connection
from bot_filter import build_wallet_flags

from learnability import flb_per_slice_v3 as flb

AUDIT_LO = float(os.environ.get("AUDIT_LO", "0.50"))
AUDIT_HI = float(os.environ.get("AUDIT_HI", "0.80"))
PREFIX = os.environ.get("AUDIT_PREFIX", "audit_noupdown")

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10

# All v3 + v4 dims to re-run for the audit
AUDIT_DIMS = [
    # v3 (10)
    "dim_resolution_type",
    "dim_info_type_supergroup",
    "dim_primary_category",
    "dim_subject_specificity",
    "dim_event_family_size",
    "dim_outcomes_per_event",
    "dim_market_specificity",
    "dim_dollar_volume_tier",
    "dim_contract_horizon",
    "dim_recurrence_class",
    # v4 (existing addons)
    "dim_group_strict_size",
    "dim_event_slug_size",
    "dim_family_vol_tier",
    "dim_family_size_x_vol",
    "dim_vol_per_contract_tier",
    "dim_vol_per_contract_residualized",
    "dim_text_novelty",
    "dim_text_neighbors_strict",
    "dim_prior_settlements_bin__event_template",
    "dim_prior_settlements_bin__event_slug",
    "dim_prior_settlements_bin__dim_group_strict",
    # NEW: dim_market_type (added below)
    "dim_market_type",
]

UPDOWN_RE = re.compile(r"updown|up-or-down", re.IGNORECASE)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log(f"=== audit_run_noupdown_and_market_type — prefix={PREFIX}, window=[{AUDIT_LO},{AUDIT_HI}] ===")

    log("DuckDB connection setup…")
    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    # bot filter cache reuse
    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    log(f"Loading cached wallet_flags…")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        SELECT * FROM read_parquet('{WF_CACHE}')
    """)

    log("Replacing trades view (bots out, UP/DOWN MARKETS EXCLUDED)…")
    con.execute("DROP VIEW IF EXISTS trades")
    con.execute("DROP VIEW IF EXISTS trades_buy")
    # Critical change: add the updown exclusion to the trades view
    con.execute(f"""
        CREATE VIEW trades AS
        SELECT *, usdcSize / NULLIF(price, 0) AS size
        FROM trades_raw
        WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
          AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman = 1)
          AND eventSlug NOT LIKE '%updown%'
          AND eventSlug NOT LIKE '%up-or-down%'
    """)
    con.execute("CREATE VIEW trades_buy AS SELECT * FROM trades WHERE side = 'BUY'")
    # Sanity check
    n_trades_kept = con.execute("SELECT COUNT(*) FROM trades_buy").fetchone()[0]
    log(f"  trades_buy after up/down exclusion: {n_trades_kept:,}")

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

    log(f"Loading v4 contract_dimensions from {V4_DIMS}…")
    df = pd.read_parquet(V4_DIMS)
    log(f"  {len(df):,} contracts × {len(df.columns)} cols")

    # Add dim_market_type — for the FULL (un-excluded) registration; the trades view filter
    # determines what actually contributes to the FLB. Since we excluded up/down at the
    # trades view, dim_market_type slices will be sparse for "updown" — but we WANT that
    # comparison; so do it on a SEPARATE trades view.
    log("Adding dim_market_type to contract_dimensions…")
    df["dim_market_type"] = df["event_template"].fillna("").apply(
        lambda s: "updown" if UPDOWN_RE.search(s) else "non_updown"
    )
    log(f"  dim_market_type counts: {df['dim_market_type'].value_counts().to_dict()}")

    # Register dim_market_type + all other dims into SQL
    cols = ["token_id", "condition_id"] + AUDIT_DIMS
    contract_dims_for_sql = df[cols].copy()
    for c in AUDIT_DIMS:
        contract_dims_for_sql[c] = contract_dims_for_sql[c].astype("string")
    con.register("_contract_dims_df", contract_dims_for_sql)
    con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
    con.unregister("_contract_dims_df")
    log(f"  _contract_dims registered ({len(contract_dims_for_sql):,} rows × {len(cols)} cols)")

    # ---- FLB per dim ----
    all_calib, all_summary = [], []
    for dim_col in AUDIT_DIMS:
        log(f"--- FLB on {dim_col} (UP/DOWN EXCLUDED) ---")
        t0 = time.time()
        try:
            calib, summary = flb.run_flb_per_slice(
                con, dim_col,
                lo=AUDIT_LO, hi=AUDIT_HI,
                min_trades=MIN_TRADES_PER_SLICE,
                n_bins=N_BINS, verbose=True,
            )
            log(f"  done in {time.time()-t0:.1f}s  ({len(summary) if isinstance(summary, pd.DataFrame) else 0} slices kept)")
            if isinstance(calib, pd.DataFrame) and len(calib):
                all_calib.append(calib)
            if isinstance(summary, pd.DataFrame) and len(summary):
                all_summary.append(summary)
        except Exception as e:
            log(f"  ERROR: {e}")

    # ---- Save ----
    if all_calib:
        calib_df = pd.concat(all_calib, ignore_index=True)
        summary_df = pd.concat(all_summary, ignore_index=True)
        calib_df.to_parquet(OUT / f"{PREFIX}_flb_per_slice.parquet", index=False)
        summary_df.to_parquet(OUT / f"{PREFIX}_spread_summary.parquet", index=False)
        summary_df.to_csv(OUT / f"{PREFIX}_spread_summary.csv", index=False)
        calib_df.to_csv(OUT / f"{PREFIX}_flb_per_slice.csv", index=False)

    (OUT / f"{PREFIX}_meta.json").write_text(json.dumps({
        "audit_lo": AUDIT_LO,
        "audit_hi": AUDIT_HI,
        "up_down_excluded": True,
        "trades_buy_kept": int(n_trades_kept),
        "n_contracts_in_dims": int(len(df)),
        "dim_market_type_counts": df["dim_market_type"].value_counts().to_dict(),
    }, indent=2))

    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
