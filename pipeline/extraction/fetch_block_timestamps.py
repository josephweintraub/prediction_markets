#!/usr/bin/env python3
"""
Fetch real block timestamps from Alchemy RPC for all unique blocks in the dataset.
Parallelized with multiprocessing, writes chunk files to disk.

Usage:
    python3 fetch_timestamps.py --workers 50
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import logging
import os
import sys
import time
from multiprocessing import Process, Queue
from pathlib import Path

import duckdb
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] worker-%(process)d: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("fetch_timestamps.log"),
    ],
)
log = logging.getLogger(__name__)

ALCHEMY_URL = os.environ.get(
    "POLYGON_RPC_URL",
    "https://polygon-mainnet.g.alchemy.com/v2/NSDVafyFOpXs26a2uN3Z_",
)
# Free public Polygon RPCs we round-robin on rate-limit. Order matters — most
# reliable first. Each worker is bound to one URL but falls back on 429.
RPC_URLS = [
    ALCHEMY_URL,  # publicnode dropped — far too slow under sustained load
]

DATA_DIR = Path(__file__).parent / "data"
RESOLVED_TRADES_PATH = DATA_DIR / "resolved_trades.parquet"
TIMESTAMPS_DIR = DATA_DIR / "timestamp_chunks"
TIMESTAMPS_PATH = DATA_DIR / "block_timestamps.parquet"

MAX_RETRIES = 6
BACKOFF_BASE = 1.5
BACKOFF_MAX = 30
BATCH_SIZE = 100         # blocks per RPC batch call (was 10)
FLUSH_THRESHOLD = 15_000  # write chunks more often (was 100K) so kills cost less


def fetch_batch(session, block_numbers, rpc_url):
    """Fetch timestamps for a batch via the given RPC. Returns (results_dict, was_rate_limited)."""
    payloads = [
        {"jsonrpc": "2.0", "id": i, "method": "eth_getBlockByNumber", "params": [hex(bn), False]}
        for i, bn in enumerate(block_numbers)
    ]

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(rpc_url, json=payloads, timeout=60)
            if resp.status_code == 429:
                wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_MAX)
                if attempt == 0:
                    log.warning("429 from %s — waiting %.1fs", rpc_url[:40], wait)
                time.sleep(wait)
                if attempt >= 2:
                    return {}, True  # signal rate-limit, caller may rotate URL
                continue
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list) and data and isinstance(data[0], dict) and "error" in data[0]:
                err_code = data[0]["error"].get("code", 0)
                if err_code in (429, -32029):
                    wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_MAX)
                    if attempt == 0:
                        log.warning("JSON-level 429/29 from %s — waiting %.1fs", rpc_url[:40], wait)
                    time.sleep(wait)
                    if attempt >= 2:
                        return {}, True
                    continue

            results = {}
            if isinstance(data, list):
                for item in data:
                    idx = item.get("id")
                    if isinstance(idx, int) and 0 <= idx < len(block_numbers):
                        result = item.get("result")
                        if result and result.get("timestamp"):
                            bn = block_numbers[idx]
                            ts = int(result["timestamp"], 16)
                            results[bn] = ts
            return results, False
        except Exception as e:
            wait = min(BACKOFF_BASE ** (attempt + 1), BACKOFF_MAX)
            if attempt < MAX_RETRIES - 1:
                if attempt == 0:
                    log.warning("Attempt %d/%d failed on %s: %s — retry in %.1fs",
                                attempt + 1, MAX_RETRIES, rpc_url[:40], e, wait)
                time.sleep(wait)
            else:
                log.error("All retries exhausted for %d-%d on %s: %s",
                          block_numbers[0], block_numbers[-1], rpc_url[:40], e)
                return {}, True
    return {}, True


def worker(worker_id, block_list, primary_url, all_urls, result_queue):
    """Fetch timestamps for a slice of blocks. Falls back across RPC URLs on rate-limit."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    label = f"w{worker_id}"
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"

    TIMESTAMPS_DIR.mkdir(parents=True, exist_ok=True)

    rpc_idx = all_urls.index(primary_url) if primary_url in all_urls else 0

    all_blocks: list[int] = []
    all_ts: list[int] = []
    total = len(block_list)
    flush_count = 0
    last_log = time.time()
    failed = 0

    log.info("[%s] Starting: %d blocks via %s", label, total, primary_url[:40])

    for i in range(0, total, BATCH_SIZE):
        batch = block_list[i:i + BATCH_SIZE]
        url = primary_url  # workers stay on their assigned provider — no rotation
        results, rate_limited = fetch_batch(session, batch, url)
        if rate_limited or not results:
            failed += len(batch)
            continue

        for bn, ts in results.items():
            all_blocks.append(bn)
            all_ts.append(ts)

        if len(all_blocks) >= FLUSH_THRESHOLD:
            chunk_path = TIMESTAMPS_DIR / f"ts_{worker_id:03d}_{flush_count:04d}.parquet"
            table = pa.table({"block_number": all_blocks, "timestamp": all_ts})
            pq.write_table(table, chunk_path, compression="zstd")
            flush_count += 1
            all_blocks.clear()
            all_ts.clear()

        if time.time() - last_log > 30:
            progress = min(i + BATCH_SIZE, total) / total * 100
            log.info("[%s] %.1f%% | %d/%d", label, progress, min(i + BATCH_SIZE, total), total)
            last_log = time.time()

    if all_blocks:
        chunk_path = TIMESTAMPS_DIR / f"ts_{worker_id:03d}_{flush_count:04d}.parquet"
        table = pa.table({"block_number": all_blocks, "timestamp": all_ts})
        pq.write_table(table, chunk_path, compression="zstd")
        flush_count += 1

    log.info("[%s] Done. failed=%d", label, failed)
    result_queue.put({"worker_id": worker_id, "failed": failed})


def main():
    parser = argparse.ArgumentParser(description="Fetch real block timestamps (incremental)")
    parser.add_argument("--workers", type=int, default=16,
                        help="Total worker processes split across RPC providers")
    parser.add_argument("--max-iters", type=int, default=8,
                        help="Max retry passes — stops early on full coverage or no progress")
    args = parser.parse_args()

    TIMESTAMPS_DIR.mkdir(parents=True, exist_ok=True)

    # All blocks we ever need timestamps for (from full resolved_trades)
    log.info("Getting unique block numbers from resolved trades...")
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    needed = set(con.execute(f"""
        SELECT DISTINCT block_number
        FROM read_parquet('{RESOLVED_TRADES_PATH}')
    """).fetchdf()["block_number"].tolist())
    log.info("Total unique blocks needed: %d", len(needed))

    last_missing = -1
    for it in range(1, args.max_iters + 1):
        # What do we already have?
        if TIMESTAMPS_PATH.exists():
            have = set(con.execute(f"""
                SELECT block_number FROM read_parquet('{TIMESTAMPS_PATH}')
            """).fetchdf()["block_number"].tolist())
        else:
            have = set()

        missing = sorted(needed - have)
        coverage = (len(have) / max(len(needed), 1)) * 100
        log.info("=" * 60)
        log.info("Iteration %d/%d: have %d, missing %d (%.2f%% coverage)",
                 it, args.max_iters, len(have), len(missing), coverage)
        log.info("=" * 60)

        if not missing:
            log.info("Full coverage reached — done.")
            break

        if len(missing) == last_missing:
            log.warning("No progress in this iteration (still %d missing) — stopping.",
                        len(missing))
            break
        last_missing = len(missing)

        # Split missing across workers
        n_workers = args.workers
        blocks_per_worker = max(1, len(missing) // n_workers)
        result_queue = Queue()
        processes = []

        for i in range(n_workers):
            start = i * blocks_per_worker
            end = start + blocks_per_worker if i < n_workers - 1 else len(missing)
            slice_blocks = missing[start:end]
            if not slice_blocks:
                continue
            primary_url = RPC_URLS[i % len(RPC_URLS)]
            p = Process(target=worker, args=(i, slice_blocks, primary_url, RPC_URLS, result_queue))
            processes.append(p)

        t0 = time.time()
        for p in processes:
            p.start()
            time.sleep(0.05)
        for p in processes:
            p.join()
        log.info("Iter %d workers done in %.1f min. Merging chunks...",
                 it, (time.time() - t0) / 60)

        # Merge: existing block_timestamps.parquet + new chunk files → block_timestamps.parquet
        glob_pattern = str(TIMESTAMPS_DIR / "ts_*.parquet")
        new_chunks = list(TIMESTAMPS_DIR.glob("ts_*.parquet"))
        if not new_chunks:
            log.warning("No new chunks produced this iteration — possible heavy throttling")
            continue

        sources = [f"read_parquet('{glob_pattern}')"]
        if TIMESTAMPS_PATH.exists():
            sources.append(f"read_parquet('{TIMESTAMPS_PATH}')")

        union_sql = " UNION ALL ".join(f"SELECT block_number, timestamp FROM {s}" for s in sources)
        tmp_path = TIMESTAMPS_PATH.with_suffix(".parquet.tmp")
        con.execute(f"""
            COPY (
                SELECT DISTINCT block_number, timestamp FROM ({union_sql})
                ORDER BY block_number
            ) TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        if TIMESTAMPS_PATH.exists():
            TIMESTAMPS_PATH.unlink()
        tmp_path.rename(TIMESTAMPS_PATH)
        # Delete consumed chunks
        for c in new_chunks:
            c.unlink()
    con.close()

    if TIMESTAMPS_PATH.exists():
        con = duckdb.connect()
        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{TIMESTAMPS_PATH}')").fetchone()[0]
        log.info("Block timestamps saved: %d → %s", total, TIMESTAMPS_PATH)
        con.close()

    # Cleanup chunks dir if empty
    try:
        TIMESTAMPS_DIR.rmdir()
    except OSError:
        log.info("Some chunk files remain in %s", TIMESTAMPS_DIR)
    log.info("Done.")


if __name__ == "__main__":
    main()
