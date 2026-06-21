#!/usr/bin/env python3
"""
Main analysis runner - executes all paper replication analyses.

Usage:
    python run_analysis.py [--quick]

Options:
    --quick    Run quick stats only (skip full analysis)
"""

import sys
import time
from datetime import datetime

# Make sure we can import our modules
sys.path.insert(0, str(__file__).rsplit('/', 1)[0])

from data_loader import get_connection, quick_stats, get_schema, get_monthly_volume
from config import OUTPUT_DIR


def print_header(text: str):
    print("\n" + "="*70)
    print(text)
    print("="*70)


def run_quick_analysis(con):
    """Run quick stats without heavy computation."""
    print_header("QUICK DATA OVERVIEW")
    
    # Schema
    print("\nTable Schema:")
    for col in get_schema(con):
        print(f"  {col[0]:25} {col[1]}")
    
    # Stats
    print("\nDataset Statistics:")
    stats = quick_stats(con)
    for k, v in stats.items():
        if isinstance(v, float):
            if 'volume' in k.lower():
                print(f"  {k:30} ${v:,.2f}")
            else:
                print(f"  {k:30} {v:,.2f}")
        elif isinstance(v, int):
            print(f"  {k:30} {v:,}")
        else:
            print(f"  {k:30} {v}")
    
    # Monthly summary
    print("\nMonthly Trading Summary (last 12 months):")
    monthly = get_monthly_volume(con)
    if not monthly.empty:
        monthly['month'] = monthly['month'].dt.strftime('%Y-%m')
        monthly['total_volume_usdc'] = monthly['total_volume_usdc'].apply(lambda x: f"${x:,.0f}")
        monthly['num_trades'] = monthly['num_trades'].apply(lambda x: f"{x:,}")
        monthly['unique_traders'] = monthly['unique_traders'].apply(lambda x: f"{x:,}")
        monthly['unique_markets'] = monthly['unique_markets'].apply(lambda x: f"{x:,}")
        print(monthly.tail(12).to_string(index=False))
    
    return stats


def run_full_analysis(con):
    """Run all paper replication analyses."""
    
    # Import analysis modules
    from pnl_analysis import save_pnl_results, compute_monthly_pnl
    from trader_characteristics import save_trader_characteristics
    from market_accuracy import save_market_accuracy_results
    
    results = {}
    
    # 1. PnL Analysis (Section 3.2)
    print_header("SECTION 3.2: PROFIT AND LOSS ANALYSIS")
    start = time.time()
    
    trader_pnl, monthly_pnl, market_outcomes = save_pnl_results(con)
    results['pnl'] = {
        'trader_pnl': trader_pnl,
        'monthly_pnl': monthly_pnl,
        'market_outcomes': market_outcomes
    }
    
    print(f"\nPnL Analysis completed in {time.time() - start:.1f}s")
    
    # Key findings
    profitable = (trader_pnl['total_pnl'] > 0).sum()
    losing = (trader_pnl['total_pnl'] < 0).sum()
    print(f"\nKey Findings:")
    print(f"  Profitable traders: {profitable:,} ({profitable/len(trader_pnl)*100:.1f}%)")
    print(f"  Losing traders: {losing:,} ({losing/len(trader_pnl)*100:.1f}%)")
    print(f"  Total platform PnL: ${trader_pnl['total_pnl'].sum():,.2f}")
    
    # 2. Trader Characteristics (Section 3.3)
    print_header("SECTION 3.3: TRADER CHARACTERISTICS")
    start = time.time()
    
    chars, segments, prob_bins = save_trader_characteristics(con)
    results['trader_chars'] = {
        'characteristics': chars,
        'segments': segments,
        'probability_bins': prob_bins
    }
    
    print(f"\nTrader Characteristics completed in {time.time() - start:.1f}s")
    
    # Key findings
    print(f"\nKey Findings:")
    print(f"  Longshot hunters: {(segments['prob_segment'] == 'longshot_hunter').sum():,}")
    print(f"  Favorite buyers: {(segments['prob_segment'] == 'favorite_buyer').sum():,}")
    print(f"  Whales (top 1%): {(segments['volume_segment'] == 'whale').sum():,}")
    
    # 3. Market Accuracy (Section 4.1)
    print_header("SECTION 4.1: MARKET ACCURACY ANALYSIS")
    start = time.time()
    
    observations, calibration, default_bias, longshot, efficiency = save_market_accuracy_results(con)
    results['market_accuracy'] = {
        'observations': observations,
        'calibration': calibration,
        'default_bias': default_bias,
        'longshot_bias': longshot,
        'efficiency': efficiency
    }
    
    print(f"\nMarket Accuracy completed in {time.time() - start:.1f}s")
    
    # Key findings
    print(f"\nKey Findings:")
    if len(longshot) > 0:
        ls_bias = longshot[longshot['prob_category'].isin(['0-10%', '10-20%'])]['bias'].mean()
        fav_bias = longshot[longshot['prob_category'].isin(['80-90%', '90-100%'])]['bias'].mean()
        print(f"  Longshot bias (0-20%): {ls_bias:+.4f}")
        print(f"  Favorite bias (80-100%): {fav_bias:+.4f}")
    
    return results


def main():
    """Main entry point."""
    quick_mode = '--quick' in sys.argv
    
    print_header("POLYMARKET PAPER REPLICATION ANALYSIS")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Mode: {'Quick stats only' if quick_mode else 'Full analysis'}")
    
    total_start = time.time()
    
    # Initialize connection
    print("\nInitializing DuckDB connection...")
    con = get_connection()
    
    # Run quick analysis
    stats = run_quick_analysis(con)
    
    if quick_mode:
        print("\n" + "="*70)
        print("Quick analysis complete! Use --full for complete paper replication.")
        return
    
    # Run full analysis
    results = run_full_analysis(con)
    
    # Final summary
    print_header("ANALYSIS COMPLETE")
    print(f"Total runtime: {time.time() - total_start:.1f}s")
    print(f"\nOutput files saved to: {OUTPUT_DIR}")
    
    import os
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith('.parquet'):
            size = os.path.getsize(OUTPUT_DIR / f) / (1024*1024)
            print(f"  {f:40} {size:.1f} MB")
    
    print("\n✅ All analyses complete! Results ready for visualization.")


if __name__ == "__main__":
    main()
