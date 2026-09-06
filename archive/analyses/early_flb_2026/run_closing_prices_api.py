"""
Runner script for the expanded closing prices pipeline (API-sourced metadata).
Uses createdAt from Gamma API as start date instead of first_trade_ts from trades.
Run from the analysis/ directory:
    python run_closing_prices_api.py
"""
import sys
import time
sys.path.insert(0, '.')

import pandas as pd
from closing_prices import fetch_closing_prices, save_closing_prices
from pathlib import Path

OUTPUT_DIR = Path('output')

t0 = time.time()

# Load the API-sourced snap table
snap = pd.read_parquet(OUTPUT_DIR / 'snap_table_api.parquet')
print(f"Loaded API snap table: {len(snap):,} rows")

# Use a separate partial file so we don't clobber the trades-based one
# We need to temporarily point fetch_closing_prices at a different partial file
# Easiest: use a subdirectory
api_dir = OUTPUT_DIR / 'api_closing_prices'
api_dir.mkdir(exist_ok=True)

# Copy existing partial if any
partial_path = api_dir / 'closing_prices_partial.parquet'
if not partial_path.exists():
    # Check if there's a previous run
    print("Starting fresh fetch")

snap_with_prices = fetch_closing_prices(
    snap, api_dir,
    fidelity_minutes=60,
    window_seconds=7200,
    n_workers=50,
    save_every=5000,
)

# Save final
out_path = api_dir / 'closing_prices.parquet'
snap_with_prices.to_parquet(out_path, index=False)
print(f"Saved to {out_path}")

elapsed = time.time() - t0
print(f"\nTotal time: {elapsed/3600:.1f} hours")
