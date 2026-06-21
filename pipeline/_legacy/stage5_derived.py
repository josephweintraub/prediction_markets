"""
Stage 5: Compute derived variables (paper replication).

This stage computes:
  5a. Maker vs taker identification (already in raw data)
  5b. Trade-level P&L
  5c. Wallet-level aggregates
  5d. Wallet classification (Bot, Sophisticated, Active Retail, Casual, One-Shot)
  5e. Return decomposition (Total Edge = Directional + Execution)

These are NOT part of the final parquet schema — they're for paper replication
analysis. Stage 6 handles the schema-conformant output.
"""

import logging

import duckdb

from config import (
    DUCKDB_PATH,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    USDC_DECIMALS,
    CTF_TOKEN_DECIMALS,
)

log = logging.getLogger(__name__)


def run_stage5(con: duckdb.DuckDBPyConnection | None = None) -> dict:
    """
    Compute derived variables for paper replication.
    Returns summary statistics as a dict.

    Prerequisite: resolved_trades table must exist (from Stage 4).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    own_con = False
    if con is None:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads = {DUCKDB_THREADS}")
        own_con = True

    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "resolved_trades" not in tables:
        log.error("Table 'resolved_trades' not found. Run Stage 4 first.")
        if own_con:
            con.close()
        return {}

    usdc_scale = 10 ** USDC_DECIMALS
    ctf_scale = 10 ** CTF_TOKEN_DECIMALS

    # -----------------------------------------------------------------------
    # 5a + 5b: Expand to wallet-trade observations with P&L
    # -----------------------------------------------------------------------
    # Each resolved_trade has a maker and a taker. We produce TWO rows per
    # trade — one for each wallet. We also compute:
    #   - usdc_amount (scaled)
    #   - token_quantity (scaled)
    #   - price = usdc_amount / token_quantity
    #   - side (BUY if acquiring outcome tokens, SELL if disposing)
    #   - pnl
    log.info("5a/5b: Expanding to wallet-trade observations with P&L...")
    con.execute("DROP TABLE IF EXISTS wallet_trades")
    con.execute(f"""
        CREATE TABLE wallet_trades AS

        -- MAKER side
        SELECT
            maker AS wallet,
            'maker' AS role,
            block_number,
            transaction_hash,
            condition_id,
            outcome,
            event_slug,
            winning_outcome,

            -- Determine direction: if outcome_token_side='maker', the maker's
            -- asset is the outcome token → maker is SELLING outcome tokens
            CASE
                WHEN outcome_token_side = 'maker' THEN 'SELL'
                ELSE 'BUY'
            END AS side,

            -- USDC amount (the non-token side)
            CASE
                WHEN outcome_token_side = 'maker'
                    THEN taker_amount_filled / {usdc_scale}.0
                ELSE maker_amount_filled / {usdc_scale}.0
            END AS usdc_amount,

            -- Token quantity
            CASE
                WHEN outcome_token_side = 'maker'
                    THEN maker_amount_filled / {ctf_scale}.0
                ELSE taker_amount_filled / {ctf_scale}.0
            END AS token_quantity,

        FROM resolved_trades
        WHERE condition_id IS NOT NULL

        UNION ALL

        -- TAKER side
        SELECT
            taker AS wallet,
            'taker' AS role,
            block_number,
            transaction_hash,
            condition_id,
            outcome,
            event_slug,
            winning_outcome,

            -- Taker direction is opposite of maker
            CASE
                WHEN outcome_token_side = 'maker' THEN 'BUY'
                ELSE 'SELL'
            END AS side,

            CASE
                WHEN outcome_token_side = 'maker'
                    THEN taker_amount_filled / {usdc_scale}.0
                ELSE maker_amount_filled / {usdc_scale}.0
            END AS usdc_amount,

            CASE
                WHEN outcome_token_side = 'maker'
                    THEN maker_amount_filled / {ctf_scale}.0
                ELSE taker_amount_filled / {ctf_scale}.0
            END AS token_quantity,

        FROM resolved_trades
        WHERE condition_id IS NOT NULL
    """)

    # Add price and P&L columns
    con.execute("""
        ALTER TABLE wallet_trades ADD COLUMN price DOUBLE;
    """)
    con.execute("""
        UPDATE wallet_trades
        SET price = CASE
            WHEN token_quantity > 0 THEN usdc_amount / token_quantity
            ELSE NULL
        END
    """)

    # P&L computation
    # BUY winning token: pnl = token_quantity - usdc_amount
    # BUY losing token:  pnl = -usdc_amount
    # SELL winning token: pnl = usdc_amount - token_quantity
    # SELL losing token:  pnl = usdc_amount
    con.execute("""
        ALTER TABLE wallet_trades ADD COLUMN pnl DOUBLE;
    """)
    con.execute("""
        UPDATE wallet_trades
        SET pnl = CASE
            WHEN side = 'BUY' AND outcome = winning_outcome
                THEN token_quantity - usdc_amount
            WHEN side = 'BUY' AND outcome != winning_outcome
                THEN -usdc_amount
            WHEN side = 'SELL' AND outcome = winning_outcome
                THEN usdc_amount - token_quantity
            WHEN side = 'SELL' AND outcome != winning_outcome
                THEN usdc_amount
            ELSE 0
        END
    """)

    wt_count = con.execute("SELECT COUNT(*) FROM wallet_trades").fetchone()[0]
    log.info("Wallet-trade observations: %d", wt_count)

    # -----------------------------------------------------------------------
    # 5c: Wallet-level aggregates
    # -----------------------------------------------------------------------
    log.info("5c: Computing wallet-level aggregates...")
    con.execute("DROP TABLE IF EXISTS wallet_stats")
    con.execute("""
        CREATE TABLE wallet_stats AS
        SELECT
            wallet,
            COUNT(*) AS total_trades,
            SUM(usdc_amount) AS total_volume,
            COUNT(DISTINCT condition_id) AS n_markets,
            SUM(pnl) AS total_pnl,
            -- Maker share
            SUM(CASE WHEN role = 'maker' THEN 1 ELSE 0 END)::DOUBLE
                / COUNT(*)::DOUBLE AS maker_share,
            -- Directional accuracy (bought winner or sold loser)
            SUM(CASE
                WHEN (side='BUY' AND outcome=winning_outcome)
                  OR (side='SELL' AND outcome!=winning_outcome)
                THEN 1 ELSE 0
            END)::DOUBLE / COUNT(*)::DOUBLE AS accuracy,
            -- ROI
            CASE WHEN SUM(usdc_amount) > 0
                THEN SUM(pnl) / SUM(usdc_amount)
                ELSE 0
            END AS roi,
            -- Active days (approx from distinct block_number ranges)
            COUNT(DISTINCT block_number // 43200) AS active_days,  -- ~43200 blocks/day on Polygon
            -- HHI of market concentration
            -- (computed below as a separate step since it needs a subquery)
        FROM wallet_trades
        GROUP BY wallet
    """)

    # -----------------------------------------------------------------------
    # 5d: Wallet classification
    # -----------------------------------------------------------------------
    log.info("5d: Classifying wallets...")
    con.execute("""
        ALTER TABLE wallet_stats ADD COLUMN wallet_type VARCHAR;
    """)
    # Trades per day (approximate)
    con.execute("""
        ALTER TABLE wallet_stats ADD COLUMN trades_per_day DOUBLE;
    """)
    con.execute("""
        UPDATE wallet_stats
        SET trades_per_day = CASE
            WHEN active_days > 0 THEN total_trades::DOUBLE / active_days
            ELSE total_trades
        END
    """)

    # Sequential classification (first match wins)
    # 1. Bot: >50 trades/day OR >1,000 total trades
    con.execute("""
        UPDATE wallet_stats SET wallet_type = 'Bot'
        WHERE wallet_type IS NULL
          AND (trades_per_day > 50 OR total_trades > 1000)
    """)
    # 2. Sophisticated: >$10K volume AND n_markets spread AND >30 active days
    con.execute("""
        UPDATE wallet_stats SET wallet_type = 'Sophisticated'
        WHERE wallet_type IS NULL
          AND total_volume > 10000
          AND n_markets >= 5
          AND active_days > 30
    """)
    # 3. Active Retail: 10–1,000 total trades
    con.execute("""
        UPDATE wallet_stats SET wallet_type = 'Active Retail'
        WHERE wallet_type IS NULL
          AND total_trades BETWEEN 10 AND 1000
    """)
    # 4. Casual: 2–9 trades
    con.execute("""
        UPDATE wallet_stats SET wallet_type = 'Casual'
        WHERE wallet_type IS NULL
          AND total_trades BETWEEN 2 AND 9
    """)
    # 5. One-Shot: exactly 1 trade
    con.execute("""
        UPDATE wallet_stats SET wallet_type = 'One-Shot'
        WHERE wallet_type IS NULL
    """)

    # Print summary
    type_counts = con.execute("""
        SELECT wallet_type, COUNT(*) as n_wallets,
               SUM(total_volume) as total_vol,
               AVG(roi) as avg_roi,
               AVG(accuracy) as avg_accuracy
        FROM wallet_stats
        GROUP BY wallet_type
        ORDER BY n_wallets DESC
    """).fetchdf()
    log.info("Wallet classification:\n%s", type_counts.to_string(index=False))

    # -----------------------------------------------------------------------
    # 5e: Return decomposition
    # -----------------------------------------------------------------------
    log.info("5e: Computing return decomposition...")
    # Fair price per market = volume-weighted avg price of buy-side trades
    con.execute("DROP TABLE IF EXISTS fair_prices")
    con.execute("""
        CREATE TABLE fair_prices AS
        SELECT
            condition_id,
            SUM(price * usdc_amount) / NULLIF(SUM(usdc_amount), 0) AS fair_price
        FROM wallet_trades
        WHERE side = 'BUY' AND price IS NOT NULL AND price > 0 AND price < 1
        GROUP BY condition_id
    """)

    # Join and compute decomposition for buy-side trades
    con.execute("DROP TABLE IF EXISTS return_decomposition")
    con.execute("""
        CREATE TABLE return_decomposition AS
        SELECT
            wt.wallet,
            wt.condition_id,
            wt.price AS entry_price,
            fp.fair_price,
            CASE WHEN wt.outcome = wt.winning_outcome THEN 1.0 ELSE 0.0 END AS outcome_value,
            -- Total Edge = Outcome - Entry Price
            CASE WHEN wt.outcome = wt.winning_outcome THEN 1.0 ELSE 0.0 END - wt.price AS total_edge,
            -- Directional = Outcome - Fair Price
            CASE WHEN wt.outcome = wt.winning_outcome THEN 1.0 ELSE 0.0 END - fp.fair_price AS directional,
            -- Execution = Fair Price - Entry Price
            fp.fair_price - wt.price AS execution,
            wt.usdc_amount
        FROM wallet_trades wt
        JOIN fair_prices fp ON wt.condition_id = fp.condition_id
        WHERE wt.side = 'BUY' AND wt.price IS NOT NULL
    """)

    rd_stats = con.execute("""
        SELECT
            AVG(total_edge) AS avg_total_edge,
            AVG(directional) AS avg_directional,
            AVG(execution) AS avg_execution,
            COUNT(*) AS n_obs
        FROM return_decomposition
    """).fetchone()

    log.info("Return decomposition (buy-side):")
    log.info("  Avg Total Edge:  %.4f", rd_stats[0] or 0)
    log.info("  Avg Directional: %.4f", rd_stats[1] or 0)
    log.info("  Avg Execution:   %.4f", rd_stats[2] or 0)
    log.info("  N observations:  %d", rd_stats[3])

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    total_pnl = con.execute("SELECT SUM(pnl) FROM wallet_trades").fetchone()[0]
    n_wallets = con.execute("SELECT COUNT(*) FROM wallet_stats").fetchone()[0]

    summary = {
        "wallet_trade_observations": wt_count,
        "unique_wallets": n_wallets,
        "total_pnl": total_pnl,
        "zero_sum_check": abs(total_pnl or 0) < 1000,  # should be ~0
    }
    log.info("Stage 5 complete. Total P&L (should be ~0): $%.2f", total_pnl or 0)
    log.info("Unique wallets: %d", n_wallets)

    if own_con:
        con.close()

    return summary


if __name__ == "__main__":
    run_stage5()
