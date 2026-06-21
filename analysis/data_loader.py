"""
Optimized data loading using DuckDB for 135M+ trades.

DuckDB can query parquet files directly without loading into memory,
making it ideal for large datasets. It's typically 10-100x faster than pandas.

Usage:
    from data_loader import get_connection, quick_stats

    con = get_connection()
    df = con.execute("SELECT * FROM trades WHERE timestamp > 1700000000 LIMIT 1000").df()
"""

import duckdb
from pathlib import Path
from config import TRADES_PARQUET_GLOB, DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS, OUTPUT_DIR

# Global connection (lazy init)
_connection = None


def reset_connection():
    """Force close and reset the connection to pick up new settings."""
    global _connection
    if _connection is not None:
        try:
            _connection.close()
        except:
            pass
        _connection = None
    print("✓ Connection reset")


def get_connection(memory_limit: str = DUCKDB_MEMORY_LIMIT, threads: int = DUCKDB_THREADS, force_new: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Get a DuckDB connection with trades table pre-registered.
    
    The trades table is a VIEW over all parquet files, so queries are lazy
    and only read the data actually needed.
    
    Args:
        memory_limit: Memory limit for DuckDB (e.g., "16GB")
        threads: Number of threads to use
        force_new: If True, close existing connection and create new one
    """
    global _connection
    
    if force_new and _connection is not None:
        reset_connection()
    
    if _connection is not None:
        return _connection
    
    # Create connection with performance settings
    _connection = duckdb.connect(database=':memory:')
    _connection.execute(f"SET memory_limit = '{memory_limit}'")
    _connection.execute(f"SET threads = {threads}")
    
    # Enable parallel parquet reading
    _connection.execute("SET enable_object_cache = true")

    # Prevent OOM on large joins — allow DuckDB to spill more aggressively
    _connection.execute("SET preserve_insertion_order = false")
    _connection.execute("SET max_temp_directory_size = '15GiB'")
    
    # Create a VIEW over all parquet files (lazy - no data loaded yet)
    # Filter out invalid timestamps (before Polymarket existed - June 2020)
    # Unix timestamp for 2020-06-01 = 1590969600
    POLYMARKET_START_TIMESTAMP = 1590969600  # June 1, 2020
    
    _connection.execute(f"""
        CREATE VIEW trades_raw AS 
        SELECT * FROM read_parquet('{TRADES_PARQUET_GLOB}', union_by_name=true)
    """)
    
    _connection.execute(f"""
        CREATE VIEW trades AS 
        SELECT * FROM trades_raw
        WHERE timestamp >= {POLYMARKET_START_TIMESTAMP}
    """)
    
    print(f"✓ DuckDB connected (memory_limit={memory_limit}, threads={threads})")
    print(f"✓ Trades view created from: {TRADES_PARQUET_GLOB}")
    
    return _connection


def quick_stats(con: duckdb.DuckDBPyConnection = None) -> dict:
    """
    Get quick summary statistics without loading all data.
    Single-pass query to avoid multiple full scans.
    """
    if con is None:
        con = get_connection()

    print("Computing quick stats (single-pass aggregation)...")

    row = con.execute("""
        SELECT COUNT(*) AS total_trades,
               COUNT(DISTINCT proxyWallet) AS unique_traders,
               COUNT(DISTINCT conditionId) AS unique_markets,
               MIN(to_timestamp(timestamp)) AS min_date,
               MAX(to_timestamp(timestamp)) AS max_date,
               SUM(usdcSize) AS total_volume_usdc
        FROM trades
    """).fetchone()

    return {
        'total_trades': row[0],
        'unique_traders': row[1],
        'unique_markets': row[2],
        'min_date': row[3],
        'max_date': row[4],
        'total_volume_usdc': row[5],
    }


def get_schema(con: duckdb.DuckDBPyConnection = None) -> list:
    """Get the schema of the trades table."""
    if con is None:
        con = get_connection()
    
    return con.execute("DESCRIBE trades").fetchall()


def sample_data(con: duckdb.DuckDBPyConnection = None, n: int = 100) -> "pd.DataFrame":
    """Get a random sample of trades for exploration."""
    if con is None:
        con = get_connection()
    
    return con.execute(f"SELECT * FROM trades USING SAMPLE {n}").df()


def query_to_parquet(con: duckdb.DuckDBPyConnection, query: str, output_file: str):
    """
    Execute a query and save results directly to parquet.
    Useful for creating intermediate datasets without loading into memory.
    """
    output_path = OUTPUT_DIR / output_file
    con.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")
    print(f"✓ Saved to {output_path}")
    return output_path


# ============================================================================
# Convenience functions for common queries
# ============================================================================

def get_trades_for_market(con: duckdb.DuckDBPyConnection, condition_id: str) -> "pd.DataFrame":
    """Get all trades for a specific market."""
    return con.execute("""
        SELECT * FROM trades 
        WHERE conditionId = ?
        ORDER BY timestamp
    """, [condition_id]).df()


def get_trades_for_trader(con: duckdb.DuckDBPyConnection, wallet: str) -> "pd.DataFrame":
    """Get all trades for a specific trader."""
    return con.execute("""
        SELECT * FROM trades 
        WHERE proxyWallet = ?
        ORDER BY timestamp
    """, [wallet]).df()


def get_monthly_volume(con: duckdb.DuckDBPyConnection) -> "pd.DataFrame":
    """Get monthly trading volume."""
    return con.execute("""
        SELECT 
            DATE_TRUNC('month', to_timestamp(timestamp)) as month,
            COUNT(*) as num_trades,
            COUNT(DISTINCT proxyWallet) as unique_traders,
            COUNT(DISTINCT conditionId) as unique_markets,
            SUM(usdcSize) as total_volume_usdc
        FROM trades
        GROUP BY 1
        ORDER BY 1
    """).df()


def get_closed_markets(con: duckdb.DuckDBPyConnection) -> "pd.DataFrame":
    """
    Identify markets that have closed (outcome determined).
    A market is considered closed if all recent trades are at price ~0 or ~1.
    """
    return con.execute("""
        WITH market_last_prices AS (
            SELECT 
                conditionId,
                outcome,
                price,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY conditionId ORDER BY timestamp DESC) as rn
            FROM trades
        ),
        market_status AS (
            SELECT 
                conditionId,
                outcome,
                AVG(price) as avg_last_price,
                MAX(timestamp) as last_trade_time
            FROM market_last_prices
            WHERE rn <= 10  -- Last 10 trades
            GROUP BY conditionId, outcome
        )
        SELECT 
            conditionId,
            outcome,
            avg_last_price,
            to_timestamp(last_trade_time) as last_trade_time,
            CASE 
                WHEN avg_last_price > 0.95 THEN 'YES_WON'
                WHEN avg_last_price < 0.05 THEN 'NO_WON'
                ELSE 'OPEN'
            END as market_status
        FROM market_status
        ORDER BY last_trade_time DESC
    """).df()


if __name__ == "__main__":
    # Quick test
    con = get_connection()
    
    print("\n" + "="*60)
    print("TRADES TABLE SCHEMA")
    print("="*60)
    for col in get_schema(con):
        print(f"  {col[0]:25} {col[1]}")
    
    print("\n" + "="*60)
    print("QUICK STATS")
    print("="*60)
    stats = quick_stats(con)
    for k, v in stats.items():
        if isinstance(v, float):
            print(f"  {k:25} ${v:,.2f}" if 'volume' in k else f"  {k:25} {v:,.2f}")
        elif isinstance(v, int):
            print(f"  {k:25} {v:,}")
        else:
            print(f"  {k:25} {v}")
    
    print("\n" + "="*60)
    print("SAMPLE DATA (5 rows)")
    print("="*60)
    print(sample_data(con, 5).to_string())
