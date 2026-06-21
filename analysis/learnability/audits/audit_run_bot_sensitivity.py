"""Phase 2.4 audit — bot threshold sensitivity sweep.

Re-derives `is_nonhuman` from the existing wallet_flags parquet using LOOSER
and TIGHTER thresholds on the two load-bearing criteria (ITI median and
trades-per-active-day). Avoids re-scanning trades to rebuild flag_c (hour HHI)
and flag_e (size CV) — keeps those from the original.

ENV VARS:
  BOT_VARIANT = current | looser | tighter
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

BOT_VARIANT = os.environ.get("BOT_VARIANT", "current")
PREFIX = os.environ.get("AUDIT_PREFIX", f"audit_bot_{BOT_VARIANT}")
LO = float(os.environ.get("AUDIT_LO", "0.50"))
HI = float(os.environ.get("AUDIT_HI", "0.80"))

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10

# Four load-bearing dims
DIMS_TO_AUDIT = [
    "dim_text_novelty",
    "dim_group_strict_size",
    "dim_family_size_x_vol",
    "dim_prior_settlements_bin__event_template",
]

# Threshold variants
VARIANTS = {
    # ITI A-definite, A-likely upper bound; B-definite, B-likely (trades-per-active-day)
    "current": {"iti_def": 1.0,  "iti_lik": 10.0,  "tpd_def": 500, "tpd_lik": 200},
    "looser":  {"iti_def": 0.75, "iti_lik": 7.5,   "tpd_def": 625, "tpd_lik": 250},
    "tighter": {"iti_def": 1.25, "iti_lik": 12.5,  "tpd_def": 375, "tpd_lik": 150},
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    if BOT_VARIANT not in VARIANTS:
        raise SystemExit(f"Unknown BOT_VARIANT: {BOT_VARIANT}")
    th = VARIANTS[BOT_VARIANT]
    log(f"=== audit_run_bot_sensitivity — variant={BOT_VARIANT}, thresholds={th}, window=[{LO},{HI}] ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")

    # Re-derive is_nonhuman with new thresholds from existing wallet_flags
    WF_CACHE = Path("/mnt/data/learnability/cache/wallet_flags.parquet")
    log("Re-deriving is_nonhuman with new thresholds…")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        WITH base AS (
            SELECT
                proxyWallet, n_trades, trades_per_active_day, median_iti,
                flag_c, flag_e,
                COALESCE(median_iti < {th["iti_def"]}, FALSE) AS flag_a_def_new,
                COALESCE(median_iti >= {th["iti_def"]} AND median_iti < {th["iti_lik"]}, FALSE) AS flag_a_lik_new,
                (trades_per_active_day > {th["tpd_def"]}) AS flag_b_def_new,
                (trades_per_active_day > {th["tpd_lik"]}) AS flag_b_lik_new
            FROM read_parquet('{WF_CACHE}')
        )
        SELECT
            *,
            (
                flag_a_def_new
                OR (flag_a_lik_new AND (flag_b_def_new OR flag_b_lik_new OR flag_c OR flag_e))
                OR (flag_b_def_new AND flag_c)
                OR (CAST(flag_b_lik_new AS INT) + CAST(flag_c AS INT) + CAST(flag_e AS INT) >= 2)
            ) AS is_nonhuman
        FROM base
    """)
    row = con.execute("""
        SELECT COUNT(*), SUM(n_trades),
               SUM(CASE WHEN is_nonhuman THEN 1 ELSE 0 END),
               SUM(CASE WHEN is_nonhuman THEN n_trades ELSE 0 END)
        FROM wallet_flags
    """).fetchone()
    log(f"  Variant '{BOT_VARIANT}': {row[2]:,} non-human wallets ({100*row[2]/row[0]:.2f}%), "
        f"{row[3]:,} non-human trades ({100*row[3]/row[1]:.2f}%)")

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
    cols = ["token_id", "condition_id"] + DIMS_TO_AUDIT
    contract_dims_for_sql = df[cols].copy()
    for c in DIMS_TO_AUDIT:
        contract_dims_for_sql[c] = contract_dims_for_sql[c].astype("string")
    con.register("_contract_dims_df", contract_dims_for_sql)
    con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
    con.unregister("_contract_dims_df")

    all_calib, all_summary = [], []
    for dim_col in DIMS_TO_AUDIT:
        log(f"--- FLB on {dim_col} (variant={BOT_VARIANT}) ---")
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

    (OUT / f"{PREFIX}_meta.json").write_text(json.dumps({
        "bot_variant": BOT_VARIANT,
        "thresholds": th,
        "n_nonhuman_wallets": int(row[2]),
        "n_nonhuman_trades": int(row[3]),
        "pct_nonhuman_wallets": round(100*row[2]/row[0], 2),
        "pct_nonhuman_trades": round(100*row[3]/row[1], 2),
    }, indent=2))
    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
