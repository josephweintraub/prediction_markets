import duckdb
import pandas as pd
from data_loader import get_connection, OUTPUT_DIR

def compute_trader_characteristics_efficient(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Optimized: Calculates all characteristics in a single pass without 
    materializing an 'enriched' intermediate table.
    """
    print("Computing trader characteristics (streaming optimization)...")
    
    # 1. Pre-calculate Market Metadata (Small Table)
    # We only need one row per market, not per trade.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE market_meta AS
        SELECT 
            conditionId,
            MIN(timestamp) as start_ts,
            MAX(timestamp) - MIN(timestamp) as duration
        FROM trades
        GROUP BY conditionId
        HAVING duration > 0
    """)
    
    # 2. Single-Pass Aggregation
    # We perform the logic (Buy=Price, Sell=1-Price) INSIDE the aggregation.
    # We use the E[X^2] - (E[X])^2 trick for variance.
    # Write to parquet first to avoid OOM on .df() materialization.
    con.execute("""
        COPY (
            SELECT
                t.proxyWallet as trader,

                -- Activity Metrics
                COUNT(*) as num_trades,
                COUNT(DISTINCT t.conditionId) as num_markets,
                SUM(t.usdcSize) as total_volume_usdc,
                SUM(t.size) as total_contracts,

                -- ---------------------------------------------------------
                -- PROBABILITY PREFERENCES (Calculated on the fly)
                -- ---------------------------------------------------------
                SUM(t.size * (CASE WHEN t.side = 'BUY' THEN t.price ELSE 1-t.price END))
                    / NULLIF(SUM(t.size), 0) as mean_implied_prob,

                SUM(t.size * POWER(CASE WHEN t.side = 'BUY' THEN t.price ELSE 1-t.price END, 2))
                    / NULLIF(SUM(t.size), 0) as mean_squared_prob,

                SUM(CASE
                    WHEN (CASE WHEN t.side='BUY' THEN t.price ELSE 1-t.price END) < 0.05 THEN t.size
                    ELSE 0
                END) / NULLIF(SUM(t.size), 0) as frac_prob_lt_5pct,

                SUM(CASE
                    WHEN (CASE WHEN t.side='BUY' THEN t.price ELSE 1-t.price END) > 0.95 THEN t.size
                    ELSE 0
                END) / NULLIF(SUM(t.size), 0) as frac_prob_gt_95pct,

                -- ---------------------------------------------------------
                -- TIMING PREFERENCES (Calculated on the fly)
                -- ---------------------------------------------------------
                AVG(CAST(t.timestamp - m.start_ts AS DOUBLE) / m.duration) as avg_market_position,

                SUM(CASE
                    WHEN (CAST(t.timestamp - m.start_ts AS DOUBLE) / m.duration) < 0.1 THEN t.size
                    ELSE 0
                END) / NULLIF(SUM(t.size), 0) as frac_first_10pct,

                SUM(CASE
                    WHEN (CAST(t.timestamp - m.start_ts AS DOUBLE) / m.duration) > 0.9 THEN t.size
                    ELSE 0
                END) / NULLIF(SUM(t.size), 0) as frac_last_10pct,

                -- ---------------------------------------------------------
                -- DIRECTION & OUTCOME
                -- ---------------------------------------------------------
                SUM(CASE WHEN t.side = 'BUY' THEN t.size ELSE 0 END) / NULLIF(SUM(t.size), 0) as frac_buys,
                SUM(CASE WHEN t.outcome = 'Yes' THEN t.size ELSE 0 END) / NULLIF(SUM(t.size), 0) as frac_yes_bets

            FROM trades t
            JOIN market_meta m ON t.conditionId = m.conditionId
            GROUP BY t.proxyWallet
        ) TO '/mnt/data/tmp/trader_chars.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print("  Written to parquet, reading back...")
    result = pd.read_parquet('/mnt/data/tmp/trader_chars.parquet')
    
    # 3. Post-Processing in Pandas (Fast)
    # Calculate Variance using: E[X^2] - (E[X])^2
    result['var_implied_prob'] = result['mean_squared_prob'] - (result['mean_implied_prob'] ** 2)
    # Clean up small negative epsilons from floating point math
    result['var_implied_prob'] = result['var_implied_prob'].clip(lower=0)

    # Clean up intermediate column
    result = result.drop(columns=['mean_squared_prob'])

    print(f"  Computed characteristics for {len(result):,} traders")
    con.execute("DROP TABLE IF EXISTS market_meta")
    return result


def compute_trader_segments(chars: pd.DataFrame) -> pd.DataFrame:
    """
    Segment traders based on the computed characteristics.
    """
    print("Computing trader segments...")
    
    # Volume Segments
    chars['volume_segment'] = pd.qcut(
        chars['total_volume_usdc'], 
        q=[0, 0.5, 0.9, 0.99, 1.0], 
        labels=['small', 'medium', 'large', 'whale']
    )
    
    # Timing Segments
    chars['timing_segment'] = pd.cut(
        chars['avg_market_position'],
        bins=[0, 0.33, 0.67, 1.0],
        labels=['early', 'balanced', 'late']
    )
    
    return chars


def compute_probability_bin_stats_efficient(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Optimized: Computes calibration stats (Predicted vs Actual) without expensive self-joins.
    """
    print("Computing probability bin statistics (optimized)...")
    
    # 1. Get Market Outcomes efficiently (using arg_max)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE market_outcomes AS
        SELECT 
            conditionId,
            outcome,
            CASE 
                WHEN arg_max(price, timestamp) >= 0.95 THEN 1.0
                WHEN arg_max(price, timestamp) <= 0.05 THEN 0.0
                ELSE NULL 
            END as result
        FROM trades
        GROUP BY conditionId, outcome
        HAVING result IS NOT NULL -- Only look at closed markets
    """)
    
    # 2. Binning Aggregation
    # We aggregate directly into bins.
    result = con.execute("""
        SELECT 
            -- Bin logic: 0.00-0.05 is Bin 0, 0.05-0.10 is Bin 1, etc.
            FLOOR((CASE WHEN t.side='BUY' THEN t.price ELSE 1-t.price END) * 20) / 20 as prob_bin,
            
            COUNT(*) as num_trades,
            SUM(t.size) as total_contracts,
            AVG(CASE WHEN t.side='BUY' THEN t.price ELSE 1-t.price END) as avg_implied_prob,
            
            -- Did the trade win?
            -- BUY wins if result=1. SELL wins if result=0.
            AVG(CASE 
                WHEN t.side='BUY' AND m.result=1.0 THEN 1.0
                WHEN t.side='SELL' AND m.result=0.0 THEN 1.0
                ELSE 0.0
            END) as empirical_win_rate,
            
            SUM(t.usdcSize) as volume_usdc

        FROM trades t
        JOIN market_outcomes m 
            ON t.conditionId = m.conditionId 
            AND t.outcome = m.outcome
        GROUP BY 1
        ORDER BY 1
    """).df()
    
    result['calibration_error'] = result['empirical_win_rate'] - result['avg_implied_prob']

    con.execute("DROP TABLE IF EXISTS market_outcomes")
    return result

if __name__ == "__main__":
    con = get_connection()
    
    # 1. Characteristics
    chars = compute_trader_characteristics_efficient(con)
    chars = compute_trader_segments(chars)
    chars.to_parquet(OUTPUT_DIR / "trader_characteristics.parquet")
    
    # 2. Calibration
    bins = compute_probability_bin_stats_efficient(con)
    bins.to_parquet(OUTPUT_DIR / "probability_bin_stats.parquet")

    # 3. Output
    print("\nSegments:")
    print(chars['volume_segment'].value_counts())
    print("\nCalibration:")
    print(bins[['prob_bin', 'avg_implied_prob', 'empirical_win_rate']].head())