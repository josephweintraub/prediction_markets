#!/usr/bin/env python3
"""
Polymarket On-Chain Dataset Replication Pipeline

Orchestrates all 6 stages to produce a complete trades.parquet and
market_resolutions.parquet from raw Polygon blockchain event logs.

Usage:
    # Test with free-tier RPC (50K blocks, ~5-10 min, validates full pipeline)
    python run_pipeline.py --test

    # Run full pipeline (stages 1-6)
    python run_pipeline.py

    # Run specific stages
    python run_pipeline.py --stages 1 2 3 4 6
    python run_pipeline.py --stages 3 4 6          # re-run mapping + output only
    python run_pipeline.py --stages 6              # re-build output from existing DB

    # Use real block timestamps (requires RPC calls, slower but accurate)
    python run_pipeline.py --real-timestamps

    # Skip Stage 5 (derived variables — not needed for notebook schema)
    python run_pipeline.py --stages 1 2 3 4 6

Environment:
    POLYGON_RPC_URL  — Polygon archive node RPC endpoint (required for Stage 1)
                       e.g. https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
"""

import argparse
import logging
import sys
import time

import duckdb

from config import (
    DUCKDB_PATH,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
)

log = logging.getLogger("pipeline")


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a shared DuckDB connection for the pipeline."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    con.execute("SET enable_object_cache = true")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET max_temp_directory_size = '50GiB'")
    return con


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket on-chain dataset replication pipeline"
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        type=int,
        default=[1, 2, 3, 4, 5, 6],
        help="Which stages to run (default: all). Stage 5 is optional.",
    )
    parser.add_argument(
        "--real-timestamps",
        action="store_true",
        help="Fetch real block timestamps from RPC (slower but accurate)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: pull ~50K blocks (~5-10 min on free tier) to validate pipeline",
    )
    args = parser.parse_args()

    # Activate test mode via CLI flag (also works via env var PIPELINE_TEST_MODE=1)
    if args.test:
        import config
        config.TEST_MODE = True
        config.START_BLOCK = 80_000_000
        config.END_BLOCK = 80_001_000
        config.LOG_CHUNK_SIZE = 10
        config.RPC_REQUESTS_PER_SECOND = 4
        config.RPC_BATCH_SIZE = 1
        config.RPC_MAX_RETRIES = 5
        config.RPC_BACKOFF_BASE = 2.0

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("pipeline.log"),
        ],
    )

    stages_to_run = set(args.stages)
    from config import TEST_MODE, START_BLOCK, END_BLOCK
    mode_str = "TEST MODE (50K blocks)" if TEST_MODE else "FULL MODE"
    log.info("Pipeline starting [%s]. Stages: %s", mode_str, sorted(stages_to_run))
    log.info("Block range: %d – %d (%d blocks)", START_BLOCK, END_BLOCK, END_BLOCK - START_BLOCK)
    log.info("DuckDB: %s (memory=%s, threads=%s)", DUCKDB_PATH, DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS)

    con = get_connection()
    results = {}

    try:
        # ---- Stage 1: Extract ----
        if 1 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 1: Extract OrderFilled events")
            log.info("=" * 60)
            t0 = time.time()
            from stage1_extract import run_stage1
            raw_count = run_stage1(con)
            results["stage1_raw_events"] = raw_count
            log.info("Stage 1 done in %.1f min. Raw events: %d",
                     (time.time() - t0) / 60, raw_count)

        # ---- Stage 2: Deduplicate ----
        if 2 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 2: Deduplicate")
            log.info("=" * 60)
            t0 = time.time()
            from stage2_dedup import run_stage2
            deduped_count = run_stage2(con)
            results["stage2_deduped"] = deduped_count
            log.info("Stage 2 done in %.1f min. Deduped: %d",
                     (time.time() - t0) / 60, deduped_count)

        # ---- Stage 3: Map tokens ----
        if 3 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 3: Map tokens to markets")
            log.info("=" * 60)
            t0 = time.time()
            from stage3_map_tokens import run_stage3
            mapped_count = run_stage3(con)
            results["stage3_mapped"] = mapped_count
            log.info("Stage 3 done in %.1f min. Mapped: %d",
                     (time.time() - t0) / 60, mapped_count)

        # ---- Stage 4: Resolve markets ----
        if 4 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 4: Resolve markets")
            log.info("=" * 60)
            t0 = time.time()
            from stage4_resolve import run_stage4
            resolved_count = run_stage4(con)
            results["stage4_resolved"] = resolved_count
            log.info("Stage 4 done in %.1f min. Resolved trades: %d",
                     (time.time() - t0) / 60, resolved_count)

        # ---- Stage 5: Derived variables (optional) ----
        if 5 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 5: Compute derived variables (paper replication)")
            log.info("=" * 60)
            t0 = time.time()
            from stage5_derived import run_stage5
            summary = run_stage5(con)
            results["stage5_summary"] = summary
            log.info("Stage 5 done in %.1f min.", (time.time() - t0) / 60)

        # ---- Stage 6: Transform to target schema ----
        if 6 in stages_to_run:
            log.info("=" * 60)
            log.info("STAGE 6: Transform to target schema")
            log.info("=" * 60)
            t0 = time.time()
            from stage6_transform import run_stage6
            output = run_stage6(con, use_real_timestamps=args.real_timestamps)
            results["stage6_output"] = output
            log.info("Stage 6 done in %.1f min. Trades: %d, Resolutions: %d",
                     (time.time() - t0) / 60,
                     output.get("trade_rows", 0),
                     output.get("resolution_rows", 0))

    except Exception as e:
        log.exception("Pipeline failed: %s", e)
        raise
    finally:
        con.close()

    # ---- Summary ----
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)
    for k, v in results.items():
        log.info("  %s: %s", k, v)

    return results


if __name__ == "__main__":
    main()
