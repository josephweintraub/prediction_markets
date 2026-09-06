"""Trade-level data quality audit.

Runs on the full raw trades parquet on EC2. Tests:

TIER 1 (definitive corruption checks)
 1. Schema + null rate per column for critical fields
 2. Price out of bounds (<=0 or >=1 — impossible probabilities)
 3. Timestamp out of plausible range (before Polymarket launch or far future)
 4. BUY/SELL row parity (every transaction logs both sides)
 5. Exact duplicate rows (same wallet/condition/timestamp/side/price)
 6. Trades on unresolved markets (currently filtered by FLB pipeline)

TIER 2 (quality issues)
 7. Trade size outliers (mega > $1M, micro < $0.01, zero-size)
 8. usdcSize ≈ size × price consistency check
 9. Wallet concentration (Pareto)
10. Price jump detection (consecutive trades on same contract with >0.3 jump)
11. Trades AFTER market resolution (using resolution timestamps if available)

TIER 3 (behavioral oddities)
12. Same-block same-wallet self-trading (wash trade signature)
13. Same wallet trading both YES and NO of same market within 60s
14. Bot signature: trades at exactly round-prices (0.50, 0.25, etc.) — possible algo trading

Output: /mnt/data/learnability/output/data_quality_audit.json
"""
import sys, os, json, time
sys.path.insert(0, "/home/ubuntu")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
from pathlib import Path
import numpy as np
import pandas as pd
from data_loader import get_connection

OUT = Path("/mnt/data/learnability/output")
POLYMARKET_START_TIMESTAMP = 1590969600  # 2020-06-01 UTC
NOW_TIMESTAMP = int(time.time())


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def q(con, sql, label=None):
    """Run a query and return single-row dict."""
    df = con.execute(sql).fetchdf()
    if len(df) == 1:
        result = df.iloc[0].to_dict()
        # Convert numpy types to plain Python
        return {k: (int(v) if isinstance(v, (np.integer, np.int64))
                    else float(v) if isinstance(v, (np.floating, np.float64))
                    else str(v) if isinstance(v, np.ndarray)
                    else v) for k, v in result.items()}
    return df.to_dict(orient="records")


def main():
    t_total = time.time()
    log("=== DATA QUALITY AUDIT — raw trades parquet ===")

    con = get_connection(memory_limit="200GB", threads=16, force_new=True)
    con.execute("SET temp_directory = '/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size = '400GiB'")

    results = {}

    # ============================================================
    # TIER 1 — definitive corruption checks
    # ============================================================
    log("\n--- TIER 1: definitive corruption ---")

    log("1. Schema + null rates")
    t0 = time.time()
    # First describe the schema
    schema = con.execute("DESCRIBE SELECT * FROM trades_raw LIMIT 1").fetchdf()
    results["1_schema"] = schema[["column_name", "column_type"]].to_dict(orient="records")

    # Null rates on critical fields (the columns we know to be there)
    nulls = q(con, """
    SELECT
        COUNT(*) AS total_rows,
        COUNT(*) FILTER (WHERE timestamp IS NULL)    AS null_timestamp,
        COUNT(*) FILTER (WHERE conditionId IS NULL)  AS null_conditionId,
        COUNT(*) FILTER (WHERE proxyWallet IS NULL)  AS null_proxyWallet,
        COUNT(*) FILTER (WHERE side IS NULL)         AS null_side,
        COUNT(*) FILTER (WHERE outcome IS NULL)      AS null_outcome,
        COUNT(*) FILTER (WHERE price IS NULL)        AS null_price,
        COUNT(*) FILTER (WHERE usdcSize IS NULL)     AS null_usdcSize,
        COUNT(*) FILTER (WHERE eventSlug IS NULL)    AS null_eventSlug
    FROM trades_raw
    """)
    results["1_nulls"] = nulls
    log(f"  done in {time.time()-t0:.1f}s. Total rows: {nulls['total_rows']:,}")
    for k, v in nulls.items():
        if k.startswith("null_") and v > 0:
            log(f"    {k}: {v:,} ({100*v/nulls['total_rows']:.4f}%)")

    log("2. Price out of bounds")
    t0 = time.time()
    pb = q(con, """
    SELECT
        COUNT(*) FILTER (WHERE price IS NULL)              AS null_price,
        COUNT(*) FILTER (WHERE price <= 0)                 AS zero_or_neg,
        COUNT(*) FILTER (WHERE price >= 1)                 AS one_or_above,
        COUNT(*) FILTER (WHERE price > 0 AND price < 0.005) AS below_0_005,
        COUNT(*) FILTER (WHERE price > 0.995 AND price < 1) AS above_0_995,
        MIN(price)                                          AS min_price,
        MAX(price)                                          AS max_price,
        AVG(price)                                          AS avg_price,
        MEDIAN(price)                                       AS median_price
    FROM trades_raw
    """)
    results["2_price_bounds"] = pb
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Out-of-bound: <=0: {pb['zero_or_neg']:,}, >=1: {pb['one_or_above']:,}")
    log(f"  Microscopic (0-0.005): {pb['below_0_005']:,}, near-1 (0.995-1): {pb['above_0_995']:,}")
    log(f"  Range: [{pb['min_price']}, {pb['max_price']}], avg={pb['avg_price']:.4f}, median={pb['median_price']:.4f}")

    log("3. Timestamp out of plausible range")
    t0 = time.time()
    ts = q(con, f"""
    SELECT
        MIN(timestamp)                                  AS min_ts,
        MAX(timestamp)                                  AS max_ts,
        COUNT(*) FILTER (WHERE timestamp < {POLYMARKET_START_TIMESTAMP}) AS before_polymarket,
        COUNT(*) FILTER (WHERE timestamp > {NOW_TIMESTAMP})              AS after_now,
        COUNT(*) FILTER (WHERE timestamp = 0)            AS exactly_zero
    FROM trades_raw
    """)
    results["3_timestamp"] = ts
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Range: epoch [{ts['min_ts']}, {ts['max_ts']}]")
    log(f"  Before Polymarket launch (1590969600): {ts['before_polymarket']:,}")
    log(f"  After now ({NOW_TIMESTAMP}): {ts['after_now']:,}")

    log("4. BUY/SELL row parity")
    t0 = time.time()
    bs = q(con, """
    SELECT
        COUNT(*) FILTER (WHERE side = 'BUY')   AS n_buy,
        COUNT(*) FILTER (WHERE side = 'SELL')  AS n_sell,
        COUNT(*) FILTER (WHERE side NOT IN ('BUY','SELL') OR side IS NULL) AS n_other
    FROM trades_raw
    """)
    results["4_buy_sell_parity"] = bs
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  BUY: {bs['n_buy']:,}, SELL: {bs['n_sell']:,}, OTHER/NULL: {bs['n_other']:,}")
    log(f"  Diff |BUY - SELL|: {abs(bs['n_buy']-bs['n_sell']):,} ({100*abs(bs['n_buy']-bs['n_sell'])/max(bs['n_buy'],1):.4f}%)")

    log("5. Exact duplicate detection — same (wallet, conditionId, ts, side, price)")
    t0 = time.time()
    dups = q(con, """
    WITH dup_groups AS (
        SELECT proxyWallet, conditionId, timestamp, side, price, COUNT(*) AS n
        FROM trades_raw
        GROUP BY proxyWallet, conditionId, timestamp, side, price
        HAVING COUNT(*) > 1
    )
    SELECT
        COUNT(*) AS n_dup_groups,
        SUM(n) AS total_rows_in_dups,
        SUM(n - 1) AS excess_rows,
        MAX(n) AS max_dup_count
    FROM dup_groups
    """)
    results["5_exact_dups"] = dups
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Dup groups: {dups['n_dup_groups']:,}, excess rows: {dups['excess_rows']:,}, max dup count: {dups['max_dup_count']}")

    log("6. Unresolved markets — trades on contracts with no winning_outcome")
    t0 = time.time()
    # Load resolutions
    res_enriched = pd.read_parquet("/home/ubuntu/pipeline/output/market_resolutions_enriched.parquet")
    con.register("_res_df", res_enriched)
    con.execute("""
    CREATE OR REPLACE TEMP TABLE _resolved AS
    SELECT DISTINCT conditionId FROM _res_df WHERE winning_outcome IS NOT NULL
    """)
    con.unregister("_res_df")
    unres = q(con, """
    SELECT
        COUNT(*) AS n_unresolved_trades,
        COUNT(DISTINCT conditionId) AS n_unresolved_contracts
    FROM trades_raw t
    WHERE NOT EXISTS (SELECT 1 FROM _resolved r WHERE r.conditionId = t.conditionId)
    """)
    results["6_unresolved"] = unres
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Unresolved trades: {unres['n_unresolved_trades']:,} on {unres['n_unresolved_contracts']:,} contracts")

    # ============================================================
    # TIER 2 — quality issues
    # ============================================================
    log("\n--- TIER 2: quality issues ---")

    log("7. Trade-size distribution + outliers")
    t0 = time.time()
    sz = q(con, """
    SELECT
        MIN(usdcSize)                              AS min_size,
        MAX(usdcSize)                              AS max_size,
        AVG(usdcSize)                              AS avg_size,
        MEDIAN(usdcSize)                           AS median_size,
        QUANTILE_CONT(usdcSize, 0.99)              AS p99_size,
        QUANTILE_CONT(usdcSize, 0.999)             AS p999_size,
        QUANTILE_CONT(usdcSize, 0.9999)            AS p9999_size,
        COUNT(*) FILTER (WHERE usdcSize >= 1000000) AS n_over_1m,
        COUNT(*) FILTER (WHERE usdcSize >= 100000)  AS n_over_100k,
        COUNT(*) FILTER (WHERE usdcSize < 0.01)     AS n_micro,
        COUNT(*) FILTER (WHERE usdcSize = 0)        AS n_zero,
        COUNT(*) FILTER (WHERE usdcSize < 0)        AS n_negative
    FROM trades_raw
    """)
    results["7_size_dist"] = sz
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Size: min={sz['min_size']}, median={sz['median_size']:.2f}, avg={sz['avg_size']:.2f}, max={sz['max_size']:.2f}")
    log(f"  P99={sz['p99_size']:.2f}, P99.9={sz['p999_size']:.2f}, P99.99={sz['p9999_size']:.2f}")
    log(f"  Over $1M: {sz['n_over_1m']:,}; over $100K: {sz['n_over_100k']:,}; micro <$0.01: {sz['n_micro']:,}")
    log(f"  Zero-size: {sz['n_zero']:,}; negative: {sz['n_negative']:,}")

    log("8. usdcSize ≈ size × price consistency (using shares = usdcSize/price)")
    t0 = time.time()
    # Note: trades_raw doesn't necessarily have a 'size' column; the trade view defines size = usdcSize/price.
    # For consistency we check the implied shares match a sane value.
    consistency = q(con, """
    SELECT
        COUNT(*)                                                              AS total,
        COUNT(*) FILTER (WHERE usdcSize > 0 AND price > 0)                    AS pricable,
        COUNT(*) FILTER (WHERE usdcSize > 0 AND price <= 0)                   AS bad_price_with_size,
        COUNT(*) FILTER (WHERE usdcSize <= 0 AND price > 0)                   AS bad_size_with_price,
        COUNT(*) FILTER (WHERE usdcSize > 0 AND price > 0 AND (usdcSize/price) > 1e9) AS impossible_shares
    FROM trades_raw
    """)
    results["8_consistency"] = consistency
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Pricable: {consistency['pricable']:,}; bad price w/ size: {consistency['bad_price_with_size']:,}")
    log(f"  Bad size w/ price: {consistency['bad_size_with_price']:,}; impossible shares: {consistency['impossible_shares']:,}")

    log("9. Wallet concentration (Pareto check)")
    t0 = time.time()
    wallet_concentration = q(con, """
    WITH wv AS (
        SELECT proxyWallet, SUM(usdcSize) AS vol, COUNT(*) AS n FROM trades_raw GROUP BY proxyWallet
    ),
    ranked AS (
        SELECT proxyWallet, vol, n,
               PERCENT_RANK() OVER (ORDER BY vol) AS prank
        FROM wv
    )
    SELECT
        (SELECT COUNT(*) FROM wv)                                    AS n_wallets,
        (SELECT SUM(vol) FROM wv)                                    AS total_vol,
        (SELECT SUM(vol) FROM ranked WHERE prank >= 0.999) / (SELECT SUM(vol) FROM wv) AS top_0_1pct_share,
        (SELECT SUM(vol) FROM ranked WHERE prank >= 0.99)  / (SELECT SUM(vol) FROM wv) AS top_1pct_share,
        (SELECT SUM(vol) FROM ranked WHERE prank >= 0.90)  / (SELECT SUM(vol) FROM wv) AS top_10pct_share,
        (SELECT SUM(n)   FROM ranked WHERE prank >= 0.999) AS top_0_1pct_trades,
        (SELECT COUNT(*) FROM ranked WHERE prank >= 0.999) AS n_top_0_1pct_wallets
    """)
    results["9_wallet_concentration"] = wallet_concentration
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  n_wallets={wallet_concentration['n_wallets']:,}, total_vol=${wallet_concentration['total_vol']/1e9:.2f}B")
    log(f"  Top 0.1%: {100*wallet_concentration['top_0_1pct_share']:.2f}%, Top 1%: {100*wallet_concentration['top_1pct_share']:.2f}%, Top 10%: {100*wallet_concentration['top_10pct_share']:.2f}%")

    log("10. Skipping price-jump detection (expensive — only run if needed)")
    results["10_price_jump"] = "skipped"

    # ============================================================
    # TIER 3 — behavioral oddities
    # ============================================================
    log("\n--- TIER 3: behavioral ---")

    log("11. Round-price clustering (potential algo signature)")
    t0 = time.time()
    round_prices = q(con, """
    SELECT
        COUNT(*)                                       AS total,
        COUNT(*) FILTER (WHERE price = 0.50)           AS n_exact_50,
        COUNT(*) FILTER (WHERE price = 0.25)           AS n_exact_25,
        COUNT(*) FILTER (WHERE price = 0.75)           AS n_exact_75,
        COUNT(*) FILTER (WHERE price = 0.10)           AS n_exact_10,
        COUNT(*) FILTER (WHERE price = 0.90)           AS n_exact_90,
        -- Many round-cent prices?
        COUNT(*) FILTER (WHERE price * 100 = FLOOR(price * 100)) AS n_round_cents,
        COUNT(*) FILTER (WHERE price * 1000 = FLOOR(price * 1000)) AS n_round_mils,
        COUNT(*) FILTER (WHERE price * 10000 = FLOOR(price * 10000)) AS n_round_centmils
    FROM trades_raw
    """)
    results["11_round_prices"] = round_prices
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Exact 0.50: {round_prices['n_exact_50']:,}; 0.25: {round_prices['n_exact_25']:,}; 0.75: {round_prices['n_exact_75']:,}")
    log(f"  Round to cent: {round_prices['n_round_cents']:,} ({100*round_prices['n_round_cents']/round_prices['total']:.2f}%)")
    log(f"  Round to 0.001: {round_prices['n_round_mils']:,}")

    log("12. Self-trade detection (same wallet on both sides — wash trade signature)")
    t0 = time.time()
    self_trade = q(con, """
    WITH same_block AS (
        SELECT conditionId, timestamp, COUNT(DISTINCT proxyWallet) AS n_distinct_wallets, COUNT(*) AS n_rows
        FROM trades_raw
        GROUP BY conditionId, timestamp
    )
    SELECT
        COUNT(*) FILTER (WHERE n_rows = 2 AND n_distinct_wallets = 1) AS n_self_trade_pairs,
        COUNT(*) FILTER (WHERE n_rows >= 2 AND n_distinct_wallets = 1) AS n_same_wallet_grouped
    FROM same_block
    """)
    results["12_self_trade"] = self_trade
    log(f"  done in {time.time()-t0:.1f}s")
    log(f"  Self-trade pairs (same wallet, same conditionId/ts, n=2 rows): {self_trade['n_self_trade_pairs']:,}")
    log(f"  Same-wallet groupings (n>=2 rows, single wallet): {self_trade['n_same_wallet_grouped']:,}")

    # ============================================================
    # Save
    # ============================================================
    log(f"\nALL DONE in {(time.time()-t_total)/60:.1f} min")
    (OUT / "data_quality_audit.json").write_text(json.dumps(results, indent=2, default=str))
    log(f"Saved → {OUT}/data_quality_audit.json")


if __name__ == "__main__":
    main()
