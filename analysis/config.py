"""
Configuration for Polymarket analysis — EC2 version.
"""
from pathlib import Path

# On-chain data
# Raw trades -- UNTOUCHED, includes ~4% full-row ingestion-replay duplicates
TRADES_RAW_GLOB = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
# Canonical CLEAN trades: full-row dedup applied (1.377B rows, 4.06% replays removed),
# re-sorted by conditionId,timestamp. USE THIS for all analysis going forward.
TRADES_PARQUET_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"

# Analysis parameters
ANALYSIS_END_DATE = "2026-03-15"
POLYMARKET_START_DATE = "2023-08-17"

# Output paths
ANALYSIS_ROOT = Path(__file__).parent
OUTPUT_DIR = ANALYSIS_ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# DuckDB settings — EC2 r6i.2xlarge has 64GB RAM
DUCKDB_MEMORY_LIMIT = "200GB"
DUCKDB_THREADS = 16
