"""
Pipeline configuration for Polymarket on-chain dataset replication.

Before running, set your RPC endpoint:
    export POLYGON_RPC_URL="https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY"

Or edit RPC_URL below directly.

Test mode (small slice):
    export PIPELINE_TEST_MODE=1
    python run_pipeline.py
"""
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Test mode: pull ~50K blocks (~1 hour of Polygon) instead of 61M
# Set PIPELINE_TEST_MODE=1 env var or pass --test to run_pipeline.py
# ---------------------------------------------------------------------------
TEST_MODE = os.environ.get("PIPELINE_TEST_MODE", "0") == "1"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PIPELINE_ROOT = Path(__file__).parent
DATA_DIR = PIPELINE_ROOT / "data"          # intermediate storage
OUTPUT_DIR = PIPELINE_ROOT / "output"      # final parquet output
CHECKPOINT_DIR = DATA_DIR / "checkpoints"  # resumable progress

for d in (DATA_DIR, OUTPUT_DIR, CHECKPOINT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# DuckDB database for intermediate pipeline storage
DUCKDB_PATH = str(DATA_DIR / "pipeline.duckdb")

# ---------------------------------------------------------------------------
# Polygon RPC
# ---------------------------------------------------------------------------
RPC_URL = os.environ.get(
    "POLYGON_RPC_URL",
    "https://polygon-mainnet.g.alchemy.com/v2/NSDVafyFOpXs26a2uN3Z_",
)

# Block range: Nov 2022 (Polymarket CTF Exchange deploy) → Feb 2026
# In test mode: 1K blocks from a highly active period (block 80M, ~Jan 2026)
# — verified to have ~100+ OrderFilled events per 10-block chunk
# Alchemy free tier limits eth_getLogs to 10 blocks per request
if TEST_MODE:
    START_BLOCK = 80_000_000
    END_BLOCK = 80_001_000
else:
    START_BLOCK = 21_000_000
    END_BLOCK = 84_400_000

# Chunk size for eth_getLogs
# Alchemy free tier: 10 blocks max. Paid/PAYG: 2K–10K.
if TEST_MODE:
    LOG_CHUNK_SIZE = 10
else:
    LOG_CHUNK_SIZE = 2_000

# RPC rate-limiting
# Alchemy free tier: ~330 compute units/sec. getLogs is 75 CU each → ~4 req/s
# Conservative defaults work on free tier; bump for paid plans
if TEST_MODE:
    RPC_MAX_RETRIES = 5
    RPC_BACKOFF_BASE = 2.0
    RPC_BACKOFF_MAX = 30
    RPC_REQUESTS_PER_SECOND = 4   # safe for Alchemy free tier
    RPC_BATCH_SIZE = 1            # no batching on free tier (simpler)
else:
    RPC_MAX_RETRIES = 8
    RPC_BACKOFF_BASE = 1.5
    RPC_BACKOFF_MAX = 60
    RPC_REQUESTS_PER_SECOND = 10  # 10K CU/s limit → ~10 req/s safe for heavy responses
    RPC_BATCH_SIZE = 5

# ---------------------------------------------------------------------------
# Polymarket contract addresses
# ---------------------------------------------------------------------------
# CTF Exchange v1
EXCHANGE_V1 = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
# CTF Exchange v2 / NegRisk
EXCHANGE_V2 = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

ALL_EXCHANGE_ADDRESSES = [EXCHANGE_V1, EXCHANGE_V2]

# OrderFilled event signature
# keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
# Indexed: orderHash (topic1), maker (topic2), taker (topic3)
# Data:    makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee
ORDER_FILLED_TOPIC0 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# USDC on Polygon has 6 decimals
USDC_DECIMALS = 6
# CTF tokens also have 6 decimals
CTF_TOKEN_DECIMALS = 6

# Known USDC-like asset IDs (asset ID 0 is typically USDC in the CTF framework)
# The actual USDC token address on Polygon:
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ---------------------------------------------------------------------------
# Gamma API
# ---------------------------------------------------------------------------
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
GAMMA_PAGE_LIMIT = 1000
GAMMA_RATE_LIMIT = 5   # requests/second (be polite)

# ---------------------------------------------------------------------------
# DuckDB tuning
# ---------------------------------------------------------------------------
DUCKDB_MEMORY_LIMIT = "48GB"
DUCKDB_THREADS = 8

# ---------------------------------------------------------------------------
# Pipeline parameters
# ---------------------------------------------------------------------------
# Polymarket launch unix timestamp (June 1, 2020)
POLYMARKET_START_TIMESTAMP = 1_590_969_600

# Block timestamp cache (fetched once, reused)
BLOCK_TIMESTAMPS_PATH = DATA_DIR / "block_timestamps.parquet"

# Stage output paths
RAW_EVENTS_PATH = DATA_DIR / "raw_events.parquet"
DEDUPED_EVENTS_PATH = DATA_DIR / "deduped_events.parquet"
TOKEN_MAP_PATH = DATA_DIR / "token_map.parquet"
GAMMA_MARKETS_PATH = DATA_DIR / "gamma_markets.parquet"
RESOLVED_MARKETS_PATH = DATA_DIR / "resolved_markets.parquet"

# Final output
TRADES_OUTPUT_DIR = OUTPUT_DIR / "trades.parquet"
RESOLUTIONS_OUTPUT_PATH = OUTPUT_DIR / "market_resolutions.parquet"
