#!/usr/bin/env python3
"""
Parallel Stage 1: Extract OrderFilled events using concurrent workers.

Splits the block range into N slices and runs them simultaneously using
multiprocessing. Each worker writes to its own parquet file, then results
are merged at the end.

Usage:
    python3 stage1_parallel.py                     # 20 workers, full range, both contracts
    python3 stage1_parallel.py --workers 30        # 30 workers
    python3 stage1_parallel.py --start 68700000    # resume from specific block
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

import requests
from eth_abi import decode

# ---------------------------------------------------------------------------
# Config (inline to keep this self-contained on EC2)
# ---------------------------------------------------------------------------

RPC_URL = os.environ.get(
    "POLYGON_RPC_URL",
    "https://polygon-mainnet.g.alchemy.com/v2/NSDVafyFOpXs26a2uN3Z_",
)

EXCHANGE_V1 = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
EXCHANGE_V2 = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
ORDER_FILLED_TOPIC0 = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

CHUNK_SIZE = 2_000       # blocks per eth_getLogs call
MAX_RETRIES = 8
BACKOFF_BASE = 1.5
BACKOFF_MAX = 60

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] worker-%(process)d: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DATA_DIR / "parallel_extract.log"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ABI decoding
# ---------------------------------------------------------------------------

ORDER_FILLED_DATA_TYPES = ["uint256", "uint256", "uint256", "uint256", "uint256"]


def decode_order_filled(log_entry: dict) -> dict | None:
    try:
        topics = log_entry["topics"]
        order_hash = topics[1]
        maker = "0x" + topics[2][-40:]
        taker = "0x" + topics[3][-40:]

        data_hex = log_entry["data"]
        if data_hex.startswith("0x"):
            data_hex = data_hex[2:]
        data_bytes = bytes.fromhex(data_hex)
        decoded = decode(ORDER_FILLED_DATA_TYPES, data_bytes)

        return {
            "order_hash": order_hash,
            "maker": maker.lower(),
            "taker": taker.lower(),
            "maker_asset_id": str(decoded[0]),
            "taker_asset_id": str(decoded[1]),
            "maker_amount_filled": int(decoded[2]),
            "taker_amount_filled": int(decoded[3]),
            "fee": int(decoded[4]),
            "block_number": int(log_entry["blockNumber"], 16),
            "transaction_hash": log_entry["transactionHash"],
            "log_index": int(log_entry["logIndex"], 16),
            "exchange_address": log_entry["address"].lower(),
        }
    except Exception as e:
        log.warning("Decode failed: %s — %s", log_entry.get("transactionHash", "?"), e)
        return None


# ---------------------------------------------------------------------------
# RPC helper (per-worker, no shared state)
# ---------------------------------------------------------------------------

class ChunkTooLargeError(Exception):
    """Raised when Alchemy returns 400 — response too large for chunk size."""
    pass


def rpc_get_logs(session, from_block, to_block, address, rpc_id=1):
    """Single eth_getLogs call with retries. Raises ChunkTooLargeError on 400."""
    payload = {
        "jsonrpc": "2.0",
        "id": rpc_id,
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": address,
            "topics": [ORDER_FILLED_TOPIC0],
        }],
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(RPC_URL, json=payload, timeout=60)
            if resp.status_code == 400:
                raise ChunkTooLargeError(f"400 at blocks {from_block}-{to_block}")
            resp.raise_for_status()
            result = resp.json()
            if "error" in result:
                raise RuntimeError(f"RPC error: {result['error']}")
            return result["result"]
        except ChunkTooLargeError:
            raise  # don't retry — caller will reduce chunk size
        except Exception as e:
            wait = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
            if attempt < MAX_RETRIES - 1:
                log.warning("Attempt %d/%d failed: %s — retry in %.1fs",
                            attempt + 1, MAX_RETRIES, e, wait)
                time.sleep(wait)
            else:
                log.error("All retries exhausted at blocks %d-%d: %s", from_block, to_block, e)
                raise


# ---------------------------------------------------------------------------
# Worker function (runs in separate process)
# ---------------------------------------------------------------------------

def _flush_to_parquet(events, output_dir, worker_id, flush_num):
    """Write events to a new parquet chunk file (no read-back, constant RAM)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "order_hash": [e["order_hash"] for e in events],
        "maker": [e["maker"] for e in events],
        "taker": [e["taker"] for e in events],
        "maker_asset_id": [e["maker_asset_id"] for e in events],
        "taker_asset_id": [e["taker_asset_id"] for e in events],
        "maker_amount_filled": [e["maker_amount_filled"] for e in events],
        "taker_amount_filled": [e["taker_amount_filled"] for e in events],
        "fee": [e["fee"] for e in events],
        "block_number": [e["block_number"] for e in events],
        "transaction_hash": [e["transaction_hash"] for e in events],
        "log_index": [e["log_index"] for e in events],
        "exchange_address": [e["exchange_address"] for e in events],
    })

    chunk_path = output_dir / f"chunk_{worker_id:03d}_{flush_num:04d}.parquet"
    pq.write_table(table, chunk_path, compression="zstd")
    return table.num_rows


FLUSH_THRESHOLD = 50_000  # flush to disk every 50K events to limit RAM


def worker(worker_id, exchange_address, start_block, end_block, result_queue):
    """Extract events for a block range, flush to parquet chunk files periodically."""
    label = f"w{worker_id}"
    session = requests.Session()
    session.headers["Content-Type"] = "application/json"

    # Each worker writes chunks to a subdirectory
    chunk_dir = DATA_DIR / f"chunks_{exchange_address[-8:]}"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    events = []
    current = start_block
    total_blocks = end_block - start_block
    total_events = 0
    flush_num = 0
    rpc_id = 0
    last_log_time = time.time()

    log.info("[%s] Starting: blocks %d → %d (%d blocks) for %s",
             label, start_block, end_block, total_blocks, exchange_address[-8:])

    current_chunk_size = CHUNK_SIZE

    while current <= end_block:
        chunk_end = min(current + current_chunk_size - 1, end_block)
        rpc_id += 1

        try:
            logs = rpc_get_logs(session, current, chunk_end, exchange_address, rpc_id)
            # Success — gradually restore chunk size back toward default
            if current_chunk_size < CHUNK_SIZE:
                current_chunk_size = min(current_chunk_size * 2, CHUNK_SIZE)
        except ChunkTooLargeError:
            # Response too large — halve chunk size and retry same range
            current_chunk_size = max(current_chunk_size // 2, 10)
            log.info("[%s] 400 error at block %d, reducing chunk to %d",
                     label, current, current_chunk_size)
            continue
        except Exception:
            log.error("[%s] Fatal error at block %d, saving partial results", label, current)
            break

        for entry in logs:
            decoded = decode_order_filled(entry)
            if decoded:
                events.append(decoded)

        current = chunk_end + 1

        # Flush to disk periodically — writes a new file each time (no read-back)
        if len(events) >= FLUSH_THRESHOLD:
            _flush_to_parquet(events, chunk_dir, worker_id, flush_num)
            total_events += len(events)
            flush_num += 1
            log.info("[%s] Flushed %d events to disk (total: %d)", label, len(events), total_events)
            events.clear()

        # Log progress every 30 seconds
        if time.time() - last_log_time > 30:
            progress = (current - start_block) / max(total_blocks, 1) * 100
            log.info("[%s] %.1f%% | Block %d/%d | Events: %d",
                     label, progress, current, end_block, total_events + len(events))
            last_log_time = time.time()

    # Final flush
    if events:
        _flush_to_parquet(events, chunk_dir, worker_id, flush_num)
        total_events += len(events)
        events.clear()

    log.info("[%s] Done. Total events: %d", label, total_events)
    result_queue.put({"worker_id": worker_id, "events": total_events})


# ---------------------------------------------------------------------------
# Merge helper
# ---------------------------------------------------------------------------

def merge_chunk_files(exchange_suffix, output_path):
    """Merge all worker chunk parquet files into one using DuckDB (streaming, low RAM)."""
    import duckdb
    import shutil

    chunk_dir = DATA_DIR / f"chunks_{exchange_suffix}"
    files = sorted(chunk_dir.glob("chunk_*.parquet"))
    if not files:
        log.warning("No chunk files in %s", chunk_dir)
        return 0

    log.info("Merging %d chunk files from %s via DuckDB ...", len(files), chunk_dir)

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    glob_pattern = str(chunk_dir / "chunk_*.parquet")
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{glob_pattern}'))
        TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
    con.close()

    log.info("Merged → %s (%d total rows)", output_path, total)

    # Clean up chunk directory
    shutil.rmtree(chunk_dir)

    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Parallel Stage 1 extraction")
    parser.add_argument("--workers", type=int, default=20,
                        help="Number of parallel workers (default: 20)")
    parser.add_argument("--start", type=int, default=21_000_000,
                        help="Start block (default: 21000000)")
    parser.add_argument("--end", type=int, default=84_400_000,
                        help="End block (default: 84400000)")
    parser.add_argument("--contracts", nargs="+",
                        default=[EXCHANGE_V1, EXCHANGE_V2],
                        help="Contract addresses to extract")
    args = parser.parse_args()

    n_workers = args.workers
    start = args.start
    end = args.end
    total_blocks = end - start

    log.info("=" * 60)
    log.info("PARALLEL STAGE 1: %d workers, blocks %d → %d", n_workers, start, end)
    log.info("Contracts: %s", [a[-8:] for a in args.contracts])
    log.info("=" * 60)

    for exchange_address in args.contracts:
        log.info("--- Extracting %s ---", exchange_address)
        t0 = time.time()

        # Density-weighted block assignment:
        # Based on observed event counts from prior runs.
        # Weight = relative event density per block in each zone.
        ZONES = [
            # (zone_start, zone_end, weight)
            (21_000_000, 35_000_000, 1),     # empty — near zero events
            (35_000_000, 55_000_000, 5),     # light — ~50K events/M blocks
            (55_000_000, 65_000_000, 30),    # medium — ~200K events/M blocks
            (65_000_000, 75_000_000, 80),    # heavy — ~500K events/M blocks
            (75_000_000, 84_400_000, 120),   # very heavy — ~1M events/M blocks
            (84_400_000, 200_000_000, 120),  # ongoing — same density as previous zone
        ]

        # Clip zones to requested range
        clipped = []
        for z_start, z_end, w in ZONES:
            cs = max(z_start, start)
            ce = min(z_end, end)
            if cs < ce:
                clipped.append((cs, ce, w))

        # Calculate total weighted blocks
        total_weight = sum((ce - cs) * w for cs, ce, w in clipped)

        # Assign workers proportionally to weight
        result_queue = Queue()
        processes = []
        worker_id = 0

        for cs, ce, w in clipped:
            zone_weight = (ce - cs) * w
            zone_workers = max(1, round(n_workers * zone_weight / total_weight))
            zone_blocks = ce - cs
            blocks_per = zone_blocks // zone_workers

            log.info("  Zone %dM–%dM: %d workers (%d blocks each, weight=%d)",
                     cs // 1_000_000, ce // 1_000_000, zone_workers, blocks_per, w)

            for i in range(zone_workers):
                w_start = cs + i * blocks_per
                w_end = cs + (i + 1) * blocks_per - 1 if i < zone_workers - 1 else ce
                p = Process(target=worker, args=(worker_id, exchange_address, w_start, w_end, result_queue))
                processes.append(p)
                worker_id += 1

        # Launch all workers
        for p in processes:
            p.start()
            time.sleep(0.1)  # slight stagger to avoid burst

        # Wait for all to finish
        for p in processes:
            p.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        total_events = sum(r["events"] for r in results)
        elapsed = (time.time() - t0) / 60

        log.info("All workers done for %s. Total events: %d in %.1f min",
                 exchange_address[-8:], total_events, elapsed)

        # Merge worker outputs
        output_path = DATA_DIR / f"raw_events_{exchange_address[-8:]}.parquet"
        merge_chunk_files(exchange_address[-8:], output_path)

    # Final merge: combine V1 + V2 into single raw_events.parquet
    log.info("--- Final merge ---")
    import duckdb as _ddb

    all_files = [
        str(DATA_DIR / f"raw_events_{a[-8:]}.parquet")
        for a in args.contracts
        if (DATA_DIR / f"raw_events_{a[-8:]}.parquet").exists()
    ]
    if all_files:
        file_list = ", ".join(f"'{f}'" for f in all_files)
        final_path = DATA_DIR / "raw_events.parquet"
        con = _ddb.connect()
        con.execute("SET memory_limit='8GB'")
        con.execute(f"""
            COPY (SELECT * FROM read_parquet([{file_list}]))
            TO '{final_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{final_path}')").fetchone()[0]
        con.close()
        log.info("FINAL: %d total raw events → %s", total, final_path)

    log.info("=" * 60)
    log.info("PARALLEL STAGE 1 COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
