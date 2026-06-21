"""
Pull ALL OrderFilledEvents from Polymarket's Goldsky orderbook subgraph.
Streams batches to per-chunk JSON files to survive interruption,
then consolidates to a single parquet at the end.

Run on EC2 in background:
  cd /home/ubuntu/pipeline && nohup /home/ubuntu/venv/bin/python goldsky_full_pull.py > goldsky_full.log 2>&1 &

Output:
  /mnt/data/goldsky/chunks/chunk_<ts>.json  — raw batches (resumable)
  /mnt/data/goldsky/orderfilled_all.parquet — final consolidated file
"""

import gzip
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

OUTPUT_DIR = Path(os.environ.get("GOLDSKY_OUT", "/mnt/data/goldsky"))
CHUNK_DIR = OUTPUT_DIR / "chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw"
    "/subgraphs/orderbook-subgraph/0.0.1/gn"
)

# Match on-chain Stage 1 block range [21_000_000, 84_400_000].
# Real timestamps from Polygon RPC for these blocks:
# Defaults cover the full range; override via env vars for parallel workers.
START_TS = int(os.environ.get("GOLDSKY_START_TS", "1636090065"))  # 2021-11-05
END_TS   = int(os.environ.get("GOLDSKY_END_TS",   "1773920033"))  # 2026-03-19

QUERY = """
query GetFills($timestampGt: BigInt!, $timestampLt: BigInt!, $first: Int!, $skip: Int!) {
  orderFilledEvents(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
    where: { timestamp_gt: $timestampGt, timestamp_lt: $timestampLt }
  ) {
    id
    transactionHash
    timestamp
    orderHash
    maker
    taker
    makerAssetId
    takerAssetId
    makerAmountFilled
    takerAmountFilled
    fee
  }
}
"""

BATCH = 1000
SKIP_MAX = 5000  # Goldsky / The Graph hard limit


def _read_chunk(p: Path) -> list:
    if p.suffix == ".gz":
        with gzip.open(p, "rt") as f:
            return json.load(f)
    return json.loads(p.read_text())


def latest_chunk_cursor() -> int:
    """Resume from the highest timestamp seen across existing chunks (.json or .json.gz)."""
    chunks = sorted(list(CHUNK_DIR.glob("chunk_*.json")) + list(CHUNK_DIR.glob("chunk_*.json.gz")))
    if not chunks:
        return START_TS
    last = chunks[-1]
    try:
        data = _read_chunk(last)
        if data:
            return int(data[-1]["timestamp"])
    except Exception:
        pass
    return START_TS


def fetch_anchor(session: requests.Session, cur_ts: int) -> tuple[list, int]:
    """Pull up to SKIP_MAX events with timestamp_gt=cur_ts. Return (events, last_ts)."""
    out = []
    skip = 0
    last_ts = cur_ts
    while skip < SKIP_MAX:
        t0 = time.time()
        variables = {
            "timestampGt": str(cur_ts),
            "timestampLt": str(END_TS),
            "first": BATCH,
            "skip": skip,
        }
        for attempt in range(5):
            try:
                r = session.post(
                    GOLDSKY_URL,
                    json={"query": QUERY, "variables": variables},
                    timeout=120,
                )
                r.raise_for_status()
                data = r.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")
                fills = data.get("data", {}).get("orderFilledEvents", [])
                break
            except Exception as e:
                wait = 2 ** attempt
                print(f"  retry {attempt+1}/5 after {wait}s: {e}", flush=True)
                time.sleep(wait)
        else:
            raise RuntimeError("max retries exceeded")

        if not fills:
            break
        out.extend(fills)
        last_ts = int(fills[-1]["timestamp"])
        skip += BATCH
        elapsed = time.time() - t0
        if elapsed < 0.2:
            time.sleep(0.2 - elapsed)
        if len(fills) < BATCH:
            break
    return out, last_ts


def main():
    cur_ts = latest_chunk_cursor()
    print(
        f"Resuming from ts={cur_ts} "
        f"({datetime.fromtimestamp(cur_ts, tz=timezone.utc):%Y-%m-%d %H:%M:%S})",
        flush=True,
    )

    session = requests.Session()
    total = sum(
        1 for _ in CHUNK_DIR.glob("chunk_*.json")
    )  # number of chunks already on disk
    chunks_written = total
    started = time.time()

    while cur_ts < END_TS:
        events, last_ts = fetch_anchor(session, cur_ts)
        if not events:
            print(f"No more events at ts={cur_ts}. Done.", flush=True)
            break
        if last_ts <= cur_ts:
            print(f"No progress at ts={cur_ts}; stopping (clock-collision).", flush=True)
            break

        # Write gzipped chunk (named by anchor ts so chunks sort lexicographically when zero-padded)
        chunk_path = CHUNK_DIR / f"chunk_{cur_ts:012d}.json.gz"
        with gzip.open(chunk_path, "wt", compresslevel=6) as f:
            json.dump(events, f)
        chunks_written += 1

        elapsed = time.time() - started
        rate = chunks_written / elapsed if elapsed > 0 else 0
        print(
            f"  anchor={cur_ts:>12} → {last_ts:>12} "
            f"({datetime.fromtimestamp(last_ts, tz=timezone.utc):%Y-%m-%d}) "
            f"events={len(events):>5}  chunks={chunks_written}  rate={rate:.2f}/s",
            flush=True,
        )
        cur_ts = last_ts

    print(f"\nFetch complete. {chunks_written} chunks in {CHUNK_DIR}", flush=True)
    print("Run consolidation step separately to merge chunks → parquet.", flush=True)


if __name__ == "__main__":
    main()
