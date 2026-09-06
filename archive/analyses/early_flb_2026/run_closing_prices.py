"""
Runner script for the closing prices pipeline.
Run from the analysis/ directory:
    python run_closing_prices.py
"""
import sys
import time
sys.path.insert(0, '.')

from closing_prices import load_snap_table, fetch_closing_prices, save_closing_prices
from config import OUTPUT_DIR

t0 = time.time()

# Load cached snap table
snap = load_snap_table(OUTPUT_DIR)

# Fetch all prices (resumable — will skip already-fetched)
snap_with_prices = fetch_closing_prices(
    snap, OUTPUT_DIR,
    fidelity_minutes=60,
    window_seconds=7200,
    n_workers=50,
    save_every=5000,
)

# Save final
save_closing_prices(snap_with_prices, OUTPUT_DIR)

elapsed = time.time() - t0
print(f"\nTotal time: {elapsed/3600:.1f} hours")
