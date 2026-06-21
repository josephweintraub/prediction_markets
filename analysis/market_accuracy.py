"""
Market Accuracy Analysis (Paper Section 4.1)

Uses ACTUAL market resolution data from Polymarket Gamma API.
Only includes markets that are confirmed closed with clear 0/1 outcomes.

Replicates Figure 2: Market accuracy across probability levels over time.
"""

import duckdb
import pandas as pd
import numpy as np
import json
import gc
from pathlib import Path

# Default sample size - 20K markets is statistically robust and memory-safe
DEFAULT_SAMPLE_SIZE = 20000


def load_verified_closed_markets(output_dir: Path) -> pd.DataFrame:
    """
    Load market resolution data from Gamma API and filter to VERIFIED closed markets.
    
    A market is considered "cleanly resolved" if:
    - closed = True (from API)
    - One outcome price is ~0 (<=0.01) and the other is ~1 (>=0.99)
    
    Returns DataFrame with columns: conditionId, winning_outcome, price1, price2
    """
    resolutions_path = output_dir / "market_resolutions.parquet"
    
    if not resolutions_path.exists():
        raise FileNotFoundError(
            f"Market resolutions not found at {resolutions_path}.\n"
            "Please run the market resolution fetch in the notebook first."
        )
    
    print("Loading verified closed markets...")
    df = pd.read_parquet(resolutions_path)

    # Support both old Gamma API format and new on-chain pipeline format
    if 'outcomePrices' in df.columns:
        # Old Gamma API format
        def parse_prices(s):
            if pd.isna(s): return None, None
            try:
                prices = json.loads(s)
                return float(prices[0]), float(prices[1])
            except:
                return None, None

        df[['price1', 'price2']] = df['outcomePrices'].apply(lambda x: pd.Series(parse_prices(x)))

        def parse_outcomes(s):
            if pd.isna(s): return None, None
            try:
                outcomes = json.loads(s)
                return outcomes[0], outcomes[1]
            except:
                return None, None

        df[['outcome1', 'outcome2']] = df['outcomes'].apply(lambda x: pd.Series(parse_outcomes(x)))

        closed = df[df['closed'] == True].copy()
        print(f"  Total closed markets from API: {len(closed):,}")

        clean_resolved = closed[
            ((closed['price1'] <= 0.01) & (closed['price2'] >= 0.99)) |
            ((closed['price1'] >= 0.99) & (closed['price2'] <= 0.01))
        ].copy()

        clean_resolved['winning_outcome'] = np.where(
            clean_resolved['price1'] >= 0.99,
            clean_resolved['outcome1'],
            clean_resolved['outcome2']
        )

        print(f"  Cleanly resolved (one outcome = 0, other = 1): {len(clean_resolved):,}")

        voided = closed[(closed['price1'] <= 0.01) & (closed['price2'] <= 0.01)]
        print(f"  Voided/cancelled (excluded): {len(voided):,}")

        ambiguous = len(closed) - len(clean_resolved) - len(voided)
        print(f"  Ambiguous (excluded): {ambiguous:,}")

        return clean_resolved[['conditionId', 'winning_outcome', 'outcome1', 'outcome2', 'price1', 'price2']]
    else:
        # New on-chain format: already has conditionId + winning_outcome
        if 'condition_id' in df.columns and 'conditionId' not in df.columns:
            df = df.rename(columns={'condition_id': 'conditionId'})
        result = df[['conditionId', 'winning_outcome']].drop_duplicates()
        print(f"  Resolved markets (on-chain format): {len(result):,}")
        return result


def compute_figure2_data(con: duckdb.DuckDBPyConnection, 
                         output_dir: Path,
                         sample_size: int = DEFAULT_SAMPLE_SIZE) -> pd.DataFrame:
    """
    Compute Figure 2 data using VERIFIED closed markets from Gamma API.
    
    Steps:
    1. Load verified closed markets (from API, NOT inferred from trades)
    2. Sample from those markets
    3. For each time decile, compute market price vs actual outcome
    """
    
    # Step 1: Load verified closed markets from API
    closed_markets = load_verified_closed_markets(output_dir)
    
    # Register as DuckDB table
    con.register('closed_markets_df', closed_markets)
    
    # Step 2: Filter to markets with enough trades AND in our verified list
    print(f"\nStep 2: Filtering to markets with sufficient trades...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE sampled_metadata AS
        WITH market_bounds AS (
            SELECT conditionId, 
                   MIN(timestamp) as start_t, 
                   MAX(timestamp) as end_t, 
                   COUNT(*) as n
            FROM trades 
            GROUP BY conditionId 
            HAVING n >= 30 AND end_t > start_t
        ),
        -- Join with verified closed markets
        verified_markets AS (
            SELECT b.conditionId, b.start_t, b.end_t, c.winning_outcome
            FROM market_bounds b
            INNER JOIN closed_markets_df c ON b.conditionId = c.conditionId
        ),
        -- Sample from verified closed markets
        sampled AS (
            SELECT * FROM verified_markets
            ORDER BY RANDOM()
            LIMIT {sample_size}
        )
        SELECT * FROM sampled;
    """)
    
    check = con.execute("SELECT COUNT(*) FROM sampled_metadata").fetchone()[0]
    print(f"   ✓ Sampled {check:,} VERIFIED closed markets (from Gamma API)")
    
    # Step 3: Create trades table with outcome won/lost info
    print("Step 3: Indexing trades with verified outcomes...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE sampled_trades AS
        SELECT 
            t.conditionId, 
            t.outcome, 
            t.price, 
            t.timestamp,
            (CAST(t.timestamp - s.start_t AS DOUBLE) / (s.end_t - s.start_t)) as pos,
            -- DID THIS OUTCOME WIN? (Based on API data, not trade prices)
            CASE WHEN t.outcome = s.winning_outcome THEN 1 ELSE 0 END as won
        FROM trades t
        JOIN sampled_metadata s ON t.conditionId = s.conditionId;
    """)
    
    trade_count = con.execute("SELECT COUNT(*) FROM sampled_trades").fetchone()[0]
    print(f"   ✓ Indexed {trade_count:,} trades")
    
    # Step 4: Compute accuracy for each time decile
    all_results = []
    timing_deciles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    
    print("Step 4: Computing accuracy by time decile...")
    
    for decile in timing_deciles:
        print(f"   Processing t = {int(decile*100)}%...", end=" ")
        
        # Get average price of last 10 trades before this decile point
        df = con.execute(f"""
            WITH window_subset AS (
                SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY conditionId, outcome ORDER BY timestamp DESC) as rn
                FROM sampled_trades
                WHERE pos <= {decile} 
                  AND pos >= {max(0, decile - 0.1)}  -- Only trades in this window
            )
            SELECT outcome, won, AVG(price) as avg_price
            FROM window_subset
            WHERE rn <= 10
            GROUP BY conditionId, outcome, won
        """).df()
        
        if len(df) == 0:
            print("no data")
            continue
        
        # Identify default outcome (Yes, Over, etc.)
        df['is_default'] = df['outcome'].str.lower().isin(['yes', 'over'])
        
        for is_def in [True, False]:
            sub = df[df['is_default'] == is_def].copy()
            if len(sub) < 50: 
                continue
            
            try:
                sub['grp'] = pd.qcut(sub['avg_price'], q=40, labels=False, duplicates='drop')
                stats = sub.groupby('grp').agg({'avg_price': 'mean', 'won': 'mean'})
                stats.columns = ['avg_implied_prob', 'empirical_win_rate']
                stats['difference'] = stats['empirical_win_rate'] - stats['avg_implied_prob']
                stats['decile'] = decile
                stats['is_default'] = is_def
                all_results.append(stats.reset_index())
            except Exception as e:
                pass  # Skip if qcut fails
        
        print(f"{len(df):,} observations")
    
    if not all_results:
        print("WARNING: No data computed for any decile!")
        return pd.DataFrame()
    
    return pd.concat(all_results, ignore_index=True)


def plot_figure2(results_df: pd.DataFrame, output_path: str = None):
    """
    Create Figure 2: Market accuracy across time deciles.
    
    Shows difference between implied probability (market price) and 
    actual win rate for 50 equally-sized probability groups.
    """
    import matplotlib.pyplot as plt
    
    if results_df.empty:
        print("No data to plot")
        return None
    
    timing_deciles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    
    fig, axes = plt.subplots(5, 2, figsize=(12, 16))
    axes = axes.flatten()
    
    for i, decile in enumerate(timing_deciles):
        ax = axes[i]
        d = results_df[results_df['decile'] == decile]
        
        # Default (Yes) - black
        default_data = d[d['is_default'] == True]
        if len(default_data) > 0:
            ax.scatter(default_data['avg_implied_prob'], default_data['difference'], 
                       c='black', s=20, alpha=0.7, label='Default (Yes)')
        
        # Alternative (No) - gray
        alt_data = d[d['is_default'] == False]
        if len(alt_data) > 0:
            ax.scatter(alt_data['avg_implied_prob'], alt_data['difference'], 
                       c='lightgray', s=20, alpha=0.7, label='Alternative (No)')
        
        ax.axhline(0, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
        ax.set_xlim(0, 1)
        ax.set_ylim(-0.3, 0.3)
        ax.set_title(f'({i+1}) t = {int(decile*100)}%', fontsize=10)
        
        if i >= 8:
            ax.set_xlabel('Implied Probability')
        if i % 2 == 0:
            ax.set_ylabel('Observed - Implied')
        if i == 0:
            ax.legend(fontsize=8, loc='upper right')
    
    plt.suptitle('Figure 2: Market Accuracy Over Time\n(Using API-Verified Closed Markets Only)', 
                 fontsize=12, fontweight='bold', y=1.01)
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"✓ Saved to {output_path}")
    
    return fig


if __name__ == "__main__":
    from data_loader import get_connection
    from config import OUTPUT_DIR
    
    con = get_connection()
    
    # Compute Figure 2 data using verified closed markets
    results = compute_figure2_data(con, OUTPUT_DIR)
    
    if not results.empty:
        results.to_parquet(OUTPUT_DIR / "figure2_data.parquet")
        plot_figure2(results, str(OUTPUT_DIR / "figure2_market_accuracy.png"))
    else:
        print("Failed to compute Figure 2 data")
