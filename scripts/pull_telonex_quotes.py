#!/usr/bin/env python
"""Bulk-download Telonex Polymarket quote files (best bid/ask ticks).

Reads /mnt/data/telonex/pull_candidates.parquet (built by
scope_telonex_pull.py: one token per non-updown market with quote coverage,
market-level USD volume since 2025-10-11), filters by --floor, expands each
token's quotes_from..quotes_to window into per-day download tasks, and pulls
them concurrently from api.telonex.io into
/mnt/data/telonex/quotes_raw/token_id=<id>/<date>.parquet.

Resume-safe: existing files are skipped, so rerunning continues where it
left off (and lowering --floor later extends the same tree).

Stops hard on HTTP 403 (quota exhausted / entitlement) and reports the
X-Downloads-Remaining header. 404s (no file for that contract-day) are
logged and skipped. Progress goes to /mnt/data/telonex/pull_progress.txt.

Usage:
  pull_telonex_quotes.py --floor 500000            # the real pull
  pull_telonex_quotes.py --floor 500000 --limit 12 # canary
"""

import argparse
import asyncio
import datetime as dt
import os
import sys
import time

import duckdb
import httpx

BASE = "https://api.telonex.io/v1/downloads/polymarket/quotes"
CAND = "/mnt/data/telonex/pull_candidates.parquet"
OUT_ROOT = "/mnt/data/telonex/quotes_raw"
PROGRESS = "/mnt/data/telonex/pull_progress.txt"
MISSING_LOG = "/mnt/data/telonex/pull_missing.log"
ERROR_LOG = "/mnt/data/telonex/pull_errors.log"
KEY_PATH = os.path.expanduser("~/.telonex_api_key")


def build_tasks(floor: float, ceiling: float | None, limit: int | None):
    con = duckdb.connect()
    ceil_clause = f"AND market_usd_vol < {ceiling}" if ceiling is not None else ""
    rows = con.execute(f"""
        SELECT token_id, quotes_from, quotes_to
        FROM read_parquet('{CAND}')
        WHERE market_usd_vol >= {floor} {ceil_clause}
        ORDER BY market_usd_vol DESC
    """).fetchall()
    tasks = []
    for token_id, qfrom, qto in rows:
        d = dt.date.fromisoformat(qfrom)
        end = dt.date.fromisoformat(qto)
        while d <= end:
            path = os.path.join(OUT_ROOT, f"token_id={token_id}", f"{d.isoformat()}.parquet")
            if not os.path.exists(path):
                tasks.append((token_id, d.isoformat(), path))
            d += dt.timedelta(days=1)
            if limit and len(tasks) >= limit:
                return tasks, len(rows)
    return tasks, len(rows)


class Stats:
    def __init__(self, total):
        self.total = total
        self.done = self.missing = self.failed = 0
        self.bytes = 0
        self.t0 = time.time()

    def line(self):
        el = time.time() - self.t0
        rate = (self.done + self.missing) / el if el > 0 else 0
        return (f"{self.done} ok, {self.missing} missing404, {self.failed} failed "
                f"of {self.total} | {self.bytes/1e9:.2f} GB | {rate:.1f} files/s | "
                f"{el/3600:.2f} h elapsed")


async def fetch(client, tok, date, path, stats, abort):
        if abort.is_set():
            return
        for attempt in range(4):
            try:
                r = await client.get(f"{BASE}/{date}", params={"asset_id": tok})
                if r.status_code == 200:
                    tmp = path + ".tmp"
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(tmp, "wb") as f:
                        f.write(r.content)
                    os.replace(tmp, path)
                    stats.done += 1
                    stats.bytes += len(r.content)
                    return
                if r.status_code == 404:
                    stats.missing += 1
                    with open(MISSING_LOG, "a") as f:
                        f.write(f"{tok}\t{date}\n")
                    return
                if r.status_code == 403:
                    remaining = r.headers.get("X-Downloads-Remaining", "?")
                    print(f"\nABORT: 403 entitlement (downloads remaining: {remaining}) "
                          f"on {tok} {date}", flush=True)
                    abort.set()
                    return
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", "30"))
                    await asyncio.sleep(min(wait, 120))
                    continue
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt == 3:
                    stats.failed += 1
                    with open(ERROR_LOG, "a") as f:
                        f.write(f"{tok}\t{date}\t{e}\n")
                    return
                await asyncio.sleep(2 ** attempt)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--floor", type=float, required=True)
    ap.add_argument("--ceiling", type=float, default=None,
                    help="exclusive upper volume bound (tranche mode)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--concurrency", type=int, default=10)
    args = ap.parse_args()

    with open(KEY_PATH) as f:
        key = f.read().strip()

    tasks, n_markets = build_tasks(args.floor, args.ceiling, args.limit)
    print(f"{n_markets} markets at floor {args.floor:,.0f}; {len(tasks)} files to fetch",
          flush=True)
    if not tasks:
        print("nothing to do")
        return

    stats = Stats(len(tasks))
    abort = asyncio.Event()
    queue = asyncio.Queue()
    for t in tasks:
        queue.put_nowait(t)

    async def reporter():
        while not abort.is_set():
            await asyncio.sleep(30)
            line = stats.line()
            print(line, flush=True)
            with open(PROGRESS, "w") as f:
                f.write(line + "\n")

    async def worker(client):
        while not abort.is_set():
            try:
                tok, date, path = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            await fetch(client, tok, date, path, stats, abort)

    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {key}"},
        follow_redirects=True,
        timeout=httpx.Timeout(120),
        limits=httpx.Limits(max_connections=args.concurrency + 4),
    ) as client:
        rep = asyncio.create_task(reporter())
        await asyncio.gather(*(worker(client) for _ in range(args.concurrency)))
        rep.cancel()

    final = "ABORTED(403) " + stats.line() if abort.is_set() else "COMPLETE " + stats.line()
    print(final, flush=True)
    with open(PROGRESS, "w") as f:
        f.write(final + "\n")
    if abort.is_set():
        sys.exit(3)


if __name__ == "__main__":
    asyncio.run(main())
