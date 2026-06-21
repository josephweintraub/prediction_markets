"""
Stage 1: Extract raw OrderFilled events from the Polygon blockchain.

Queries both Polymarket CTF Exchange contracts (v1 + v2/NegRisk) for all
OrderFilled events across the full block range (~21M–82.3M).

Features:
  - Checkpointing: saves progress per contract so extraction resumes on restart
  - Batch JSON-RPC: sends multiple getLogs in one HTTP call (configurable)
  - Rate limiting with exponential backoff
  - Streams results to DuckDB incrementally (not held in memory)

Expected output: ~394.6M raw events → stored in pipeline DuckDB + parquet.
"""

import json
import time
import logging
from pathlib import Path

import duckdb
import requests
from eth_abi import decode

import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ABI decoding helpers
# ---------------------------------------------------------------------------

# OrderFilled event layout:
#   topic0 = event signature hash
#   topic1 = orderHash   (indexed, bytes32)
#   topic2 = maker       (indexed, address)
#   topic3 = taker       (indexed, address)
#   data   = (makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee)
#            5 × uint256 = 160 bytes

ORDER_FILLED_DATA_TYPES = [
    "uint256",   # makerAssetId
    "uint256",   # takerAssetId
    "uint256",   # makerAmountFilled
    "uint256",   # takerAmountFilled
    "uint256",   # fee
]


def decode_order_filled(log_entry: dict) -> dict | None:
    """Decode a single OrderFilled event log into a flat dict."""
    try:
        topics = log_entry["topics"]
        # topics[0] = event sig, topics[1] = orderHash, topics[2] = maker, topics[3] = taker
        order_hash = topics[1]
        maker = "0x" + topics[2][-40:]   # address is right-padded in 32-byte topic
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
        log.warning("Failed to decode log: %s — %s", log_entry.get("transactionHash"), e)
        return None


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

class RPCClient:
    """Minimal JSON-RPC client with batching, retries, and rate limiting."""

    def __init__(self, url: str, requests_per_second: int = 25):
        self.url = url
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        self.min_interval = 1.0 / requests_per_second
        self._last_request_time = 0.0
        self._rpc_id = 0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request_time = time.time()

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    def call(self, method: str, params: list, retries: int | None = None) -> dict:
        """Single JSON-RPC call with retries."""
        if retries is None:
            retries = config.RPC_MAX_RETRIES
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params,
        }
        for attempt in range(retries):
            self._rate_limit()
            try:
                resp = self.session.post(self.url, json=payload, timeout=30)
                resp.raise_for_status()
                result = resp.json()
                if "error" in result:
                    raise RuntimeError(f"RPC error: {result['error']}")
                return result["result"]
            except Exception as e:
                wait = min(config.RPC_BACKOFF_BASE ** attempt, config.RPC_BACKOFF_MAX)
                log.warning("RPC attempt %d/%d failed: %s — retrying in %.1fs",
                            attempt + 1, retries, e, wait)
                time.sleep(wait)
        raise RuntimeError(f"RPC call failed after {retries} retries: {method} {params}")

    def batch_call(self, calls: list[tuple[str, list]], retries: int | None = None) -> list:
        """Batch JSON-RPC call. Each call is (method, params)."""
        if retries is None:
            retries = config.RPC_MAX_RETRIES
        if len(calls) == 1:
            return [self.call(calls[0][0], calls[0][1], retries)]

        payloads = []
        for method, params in calls:
            payloads.append({
                "jsonrpc": "2.0",
                "id": self._next_id(),
                "method": method,
                "params": params,
            })

        for attempt in range(retries):
            self._rate_limit()
            try:
                resp = self.session.post(self.url, json=payloads, timeout=60)
                resp.raise_for_status()
                results = resp.json()
                results.sort(key=lambda r: r["id"])
                for r in results:
                    if "error" in r:
                        raise RuntimeError(f"RPC batch error: {r['error']}")
                return [r["result"] for r in results]
            except Exception as e:
                wait = min(config.RPC_BACKOFF_BASE ** attempt, config.RPC_BACKOFF_MAX)
                log.warning("Batch RPC attempt %d/%d failed: %s — retrying in %.1fs",
                            attempt + 1, retries, e, wait)
                time.sleep(wait)
        raise RuntimeError(f"Batch RPC failed after {retries} retries")

    def get_logs(self, from_block: int, to_block: int,
                 address: str, topics: list[str]) -> list[dict]:
        """eth_getLogs for a block range."""
        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": address,
            "topics": topics,
        }]
        return self.call("eth_getLogs", params)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _ckpt_path(exchange_address: str) -> Path:
    return config.CHECKPOINT_DIR / f"stage1_{exchange_address[-8:]}.json"


def load_checkpoint(exchange_address: str) -> int:
    """Return the last fully-processed block for this contract, or START_BLOCK."""
    path = _ckpt_path(exchange_address)
    if path.exists():
        data = json.loads(path.read_text())
        return data.get("last_block", config.START_BLOCK)
    return config.START_BLOCK


def save_checkpoint(exchange_address: str, last_block: int):
    path = _ckpt_path(exchange_address)
    path.write_text(json.dumps({"last_block": last_block, "address": exchange_address}))


# ---------------------------------------------------------------------------
# DuckDB storage
# ---------------------------------------------------------------------------

def init_db(con: duckdb.DuckDBPyConnection):
    """Create the raw_events table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_events (
            order_hash        VARCHAR,
            maker             VARCHAR,
            taker             VARCHAR,
            maker_asset_id    VARCHAR,
            taker_asset_id    VARCHAR,
            maker_amount_filled BIGINT,
            taker_amount_filled BIGINT,
            fee               BIGINT,
            block_number      INTEGER,
            transaction_hash  VARCHAR,
            log_index         INTEGER,
            exchange_address  VARCHAR
        )
    """)


def insert_batch(con: duckdb.DuckDBPyConnection, events: list[dict]):
    """Insert a batch of decoded events into DuckDB."""
    if not events:
        return
    con.executemany(
        """INSERT INTO raw_events VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )""",
        [
            (
                e["order_hash"], e["maker"], e["taker"],
                e["maker_asset_id"], e["taker_asset_id"],
                e["maker_amount_filled"], e["taker_amount_filled"],
                e["fee"], e["block_number"], e["transaction_hash"],
                e["log_index"], e["exchange_address"],
            )
            for e in events
        ],
    )


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def extract_events_for_contract(
    rpc: RPCClient,
    con: duckdb.DuckDBPyConnection,
    exchange_address: str,
) -> int:
    """
    Extract all OrderFilled events for one exchange contract.
    Returns total events extracted (including previously checkpointed).
    """
    batch_size = config.RPC_BATCH_SIZE
    resume_block = load_checkpoint(exchange_address)
    end_block = config.END_BLOCK
    chunk_size = config.LOG_CHUNK_SIZE

    log.info("Extracting %s from block %d to %d (chunk=%d)",
             exchange_address, resume_block, end_block, chunk_size)

    total_events = 0
    chunk_count = 0
    pending_events = []
    flush_threshold = 5_000

    current_block = resume_block
    while current_block <= end_block:
        # Build a batch of getLogs calls
        batch_calls = []
        batch_ranges = []
        for _ in range(batch_size):
            if current_block > end_block:
                break
            cb_end = min(current_block + chunk_size - 1, end_block)
            batch_calls.append((
                "eth_getLogs",
                [{
                    "fromBlock": hex(current_block),
                    "toBlock": hex(cb_end),
                    "address": exchange_address,
                    "topics": [config.ORDER_FILLED_TOPIC0],
                }],
            ))
            batch_ranges.append((current_block, cb_end))
            current_block = cb_end + 1

        if not batch_calls:
            break

        # Execute batch
        try:
            if len(batch_calls) == 1:
                results = [rpc.call(batch_calls[0][0], batch_calls[0][1])]
            else:
                results = rpc.batch_call(batch_calls)
        except Exception as e:
            log.error("Fatal RPC error at block %d: %s", batch_ranges[0][0], e)
            if batch_ranges:
                save_checkpoint(exchange_address, batch_ranges[0][0])
            raise

        # Decode and accumulate
        for log_entries in results:
            for entry in log_entries:
                decoded = decode_order_filled(entry)
                if decoded:
                    pending_events.append(decoded)
                    total_events += 1

        # Flush to DB periodically
        if len(pending_events) >= flush_threshold:
            insert_batch(con, pending_events)
            pending_events.clear()

        chunk_count += len(batch_calls)
        last_processed_block = batch_ranges[-1][1]

        # Checkpoint and log progress every 50 chunks
        if chunk_count % 50 == 0:
            save_checkpoint(exchange_address, last_processed_block)
            progress = (last_processed_block - resume_block) / max(end_block - resume_block, 1) * 100
            log.info(
                "[%s] Progress: %.1f%% | Block %d/%d | Events so far: %d",
                exchange_address[-8:],
                progress,
                last_processed_block,
                end_block,
                total_events,
            )

    # Final flush
    if pending_events:
        insert_batch(con, pending_events)
        pending_events.clear()

    save_checkpoint(exchange_address, end_block)
    log.info("[%s] Done. Total events: %d", exchange_address[-8:], total_events)
    return total_events


def run_stage1(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """
    Run Stage 1: extract all OrderFilled events from both exchange contracts.
    Returns total raw event count.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if "YOUR_KEY" in config.RPC_URL:
        raise ValueError(
            "Set your Polygon RPC URL: export POLYGON_RPC_URL='https://...'"
        )

    own_con = False
    if con is None:
        con = duckdb.connect(config.DUCKDB_PATH)
        con.execute(f"SET memory_limit = '{config.DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads = {config.DUCKDB_THREADS}")
        own_con = True

    init_db(con)
    rpc = RPCClient(config.RPC_URL, config.RPC_REQUESTS_PER_SECOND)

    total = 0
    for address in config.ALL_EXCHANGE_ADDRESSES:
        total += extract_events_for_contract(rpc, con, address)

    # Export to parquet for downstream stages
    log.info("Exporting raw_events to %s ...", config.RAW_EVENTS_PATH)
    con.execute(f"""
        COPY (SELECT * FROM raw_events)
        TO '{config.RAW_EVENTS_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    row_count = con.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    log.info("Stage 1 complete. Total raw events in DB: %d", row_count)

    if own_con:
        con.close()

    return row_count


if __name__ == "__main__":
    run_stage1()
