import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
from data_loader import get_connection, OUTPUT_DIR
from config import ANALYSIS_END_DATE

def determine_market_outcomes_efficient(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Optimized: Uses arg_max to find final prices without expensive window functions.
    """
    print("Determining market outcomes (optimized)...")
    
    # Use arg_max to get the very last price per market/outcome in one scan
    # This is O(N) instead of O(N log N) for window functions
    result = con.execute("""
        WITH market_finals AS (
            SELECT 
                conditionId,
                outcome,
                arg_max(price, timestamp) as final_price,
                MAX(timestamp) as last_timestamp
            FROM trades
            GROUP BY conditionId, outcome
        )
        SELECT 
            conditionId,
            outcome,
            final_price,
            CASE 
                WHEN final_price >= 0.95 THEN 1.0
                WHEN final_price <= 0.05 THEN 0.0
                ELSE NULL 
            END as outcome_won,
            to_timestamp(last_timestamp) as last_trade_time,
            CASE 
                WHEN final_price >= 0.95 OR final_price <= 0.05 THEN true 
                ELSE false 
            END as is_closed
        FROM market_finals
        ORDER BY last_timestamp DESC
    """).df()
    
    return result

def compute_trader_pnl_efficient(con: duckdb.DuckDBPyConnection,
                                 valuation_date: str = ANALYSIS_END_DATE) -> pd.DataFrame:
    """
    Memory-Efficient PnL Calculation.
    
    Strategy: "Map-Reduce"
    1. Map: Aggregate trades into Net Positions and Net Cashflow (reduces 136M rows -> ~1M rows).
    2. Reduce: Join aggregated positions with market prices to compute PnL.
    """
    print(f"Computing trader PnL efficiently (valuation date: {valuation_date})...")
    
    # 1. Configuration (preserve_insertion_order already set at connection creation)
    
    valuation_ts = int(datetime.strptime(valuation_date, "%Y-%m-%d").timestamp())
    
    # 2. Create Market Outcomes Table
    # We create a lightweight mapping table for the join
    print("  Creating market price lookup table...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE market_prices AS
        SELECT 
            conditionId,
            outcome,
            arg_max(price, timestamp) as current_price,
            CASE 
                WHEN arg_max(price, timestamp) >= 0.95 THEN 1.0
                WHEN arg_max(price, timestamp) <= 0.05 THEN 0.0
                ELSE arg_max(price, timestamp)
            END as valuation_price,
            CASE 
                WHEN arg_max(price, timestamp) >= 0.95 OR arg_max(price, timestamp) <= 0.05 THEN true 
                ELSE false 
            END as is_closed
        FROM trades
        WHERE timestamp <= {valuation_ts}
        GROUP BY conditionId, outcome
    """)

    # 3. Aggregate Trader Positions (The Key Optimization)
    # Instead of calculating PnL per trade, we calculate:
    # Net Position = (Shares Bought - Shares Sold)
    # Net Cashflow = (Cash from Sells - Cash Spent on Buys)
    print("  Aggregating trader positions (compressing data)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE trader_positions AS
        SELECT 
            proxyWallet,
            conditionId,
            outcome,
            -- Net Position: How many shares do they hold?
            SUM(CASE WHEN side = 'BUY' THEN size ELSE -size END) as net_position,
            
            -- Net Cash Flow: How much cash did they spend/receive?
            -- BUY: Spend money (negative flow)
            -- SELL: Receive money (positive flow)
            SUM(CASE 
                WHEN side = 'BUY' THEN -(size * price) 
                WHEN side = 'SELL' THEN (size * price)
            END) as net_cashflow,
            
            SUM(usdcSize) as volume,
            COUNT(*) as trade_count
        FROM trades
        WHERE timestamp <= {valuation_ts}
        GROUP BY proxyWallet, conditionId, outcome
        HAVING volume > 0  -- Filter out empty interactions
    """)

    # 4. Final Calculation
    # Now we join the small 'trader_positions' table with 'market_prices'
    # Formula: PnL = (Position * Current_Price) + Net_Cashflow
    print("  Calculating final PnL...")
    result = con.execute("""
        SELECT 
            t.proxyWallet as trader,
            
            -- Total PnL
            SUM(
                (t.net_position * COALESCE(m.valuation_price, 0)) + t.net_cashflow
            ) as total_pnl,
            
            -- Realized PnL (Closed markets only)
            SUM(CASE 
                WHEN m.is_closed THEN (t.net_position * m.valuation_price) + t.net_cashflow
                ELSE 0 
            END) as realized_pnl,
            
            -- Unrealized PnL (Open markets only)
            SUM(CASE 
                WHEN NOT m.is_closed THEN (t.net_position * m.valuation_price) + t.net_cashflow
                ELSE 0 
            END) as unrealized_pnl,
            
            SUM(t.trade_count) as num_trades,
            COUNT(DISTINCT t.conditionId) as num_markets,
            SUM(t.volume) as total_volume
            
        FROM trader_positions t
        JOIN market_prices m 
            ON t.conditionId = m.conditionId 
            AND t.outcome = m.outcome
        GROUP BY t.proxyWallet
        ORDER BY total_pnl DESC
    """).df()
    
    print(f"  Computed PnL for {len(result):,} traders")
    print(f"  Top Trader PnL: ${result['total_pnl'].max():,.2f}")

    for _t in ['market_prices', 'trader_positions']:
        con.execute(f"DROP TABLE IF EXISTS {_t}")

    return result

if __name__ == "__main__":
    con = get_connection()
    
    # 1. Market Outcomes
    outcomes = determine_market_outcomes_efficient(con)
    outcomes.to_parquet(OUTPUT_DIR / "market_outcomes.parquet")
    print("Saved market outcomes.")

    # 2. Trader PnL
    pnl = compute_trader_pnl_efficient(con)
    pnl.to_parquet(OUTPUT_DIR / "trader_pnl.parquet")
    print("Saved trader PnL.")