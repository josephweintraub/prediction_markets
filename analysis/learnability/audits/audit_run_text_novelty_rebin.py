"""Phase 2.3 audit — text novelty re-binning at meaningful thresholds.

The original dim_text_novelty used quintiles of best_sim, but q20=0.896 means
"Q1 most isolated" is actually "best_sim < 0.90" — not "isolated." Re-bin at
fixed semantic thresholds and re-run FLB.

New bins:
  <0.50  — genuinely isolated (no semantic neighbor)
  0.50-0.75  — moderately isolated
  0.75-0.90  — has similar neighbor
  0.90-0.95  — close lexical match
  >0.95  — near-duplicate
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

PREFIX = os.environ.get("AUDIT_PREFIX", "audit_text_novelty_rebin")
LO = float(os.environ.get("AUDIT_LO", "0.50"))
HI = float(os.environ.get("AUDIT_HI", "0.80"))

OUT = Path("/mnt/data/learnability/output")
V4_DIMS = OUT / "phase1_v4_contract_dimensions.parquet"
POLYMARKET_START_TIMESTAMP = 1590969600
MIN_TRADES_PER_SLICE = 5000
N_BINS = 10


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    t_total = time.time()
    log(f"=== audit_run_text_novelty_rebin — prefix={PREFIX}, window=[{LO},{HI}] ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")

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
    log(f"Loaded {len(df):,} contracts; best_sim distribution:")
    log(f"  min={df['best_sim'].min():.4f}, q05={df['best_sim'].quantile(0.05):.4f}, "
        f"median={df['best_sim'].median():.4f}, q95={df['best_sim'].quantile(0.95):.4f}, "
        f"max={df['best_sim'].max():.4f}")

    # New binning at fixed semantic thresholds
    bins = [-np.inf, 0.50, 0.75, 0.90, 0.95, np.inf]
    labels = ["<0.50 genuinely isolated", "0.50-0.75 mod isolated",
              "0.75-0.90 has neighbor", "0.90-0.95 close lex match",
              ">0.95 near duplicate"]
    df["dim_text_novelty_v2"] = pd.cut(df["best_sim"], bins=bins, labels=labels).astype(str)

    counts = df["dim_text_novelty_v2"].value_counts()
    log(f"New bin counts (contracts):\n{counts.to_string()}")

    cols = ["token_id", "condition_id", "dim_text_novelty_v2"]
    contract_dims_for_sql = df[cols].copy()
    contract_dims_for_sql["dim_text_novelty_v2"] = contract_dims_for_sql["dim_text_novelty_v2"].astype("string")
    con.register("_contract_dims_df", contract_dims_for_sql)
    con.execute("CREATE OR REPLACE TEMP TABLE _contract_dims AS SELECT * FROM _contract_dims_df")
    con.unregister("_contract_dims_df")

    log("Running FLB on dim_text_novelty_v2…")
    t0 = time.time()
    calib, summary = flb.run_flb_per_slice(
        con, "dim_text_novelty_v2",
        lo=LO, hi=HI,
        min_trades=MIN_TRADES_PER_SLICE,
        n_bins=N_BINS, verbose=True,
    )
    log(f"  done in {time.time()-t0:.1f}s")

    if isinstance(calib, pd.DataFrame) and len(calib):
        calib.to_parquet(OUT / f"{PREFIX}_flb_per_slice.parquet", index=False)
        summary.to_parquet(OUT / f"{PREFIX}_spread_summary.parquet", index=False)
        summary.to_csv(OUT / f"{PREFIX}_spread_summary.csv", index=False)

    (OUT / f"{PREFIX}_meta.json").write_text(json.dumps({
        "audit_lo": LO,
        "audit_hi": HI,
        "bin_thresholds": [-1, 0.50, 0.75, 0.90, 0.95, 1.0],
        "bin_labels": labels,
        "contract_counts_per_bin": counts.to_dict(),
    }, indent=2))

    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")


if __name__ == "__main__":
    main()
