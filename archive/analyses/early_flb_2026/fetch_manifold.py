"""
Fetch all Manifold Markets data via API.

Downloads:
  1. All resolved binary markets with full metadata (groupSlugs for classification)
  2. All bets for those markets

Uses limit=5000 per request. No auth needed.
Output: output/manifold/all_markets.json, output/manifold/all_bets.json

Usage:
    python3 fetch_manifold.py

Run on EC2 for speed (~30-60 min total).
"""

import requests
import time
import json
import sys
from pathlib import Path
from datetime import datetime

API_BASE = 'https://api.manifold.markets/v0'
OUTPUT_DIR = Path('output/manifold')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_all_resolved_markets():
    """Fetch all resolved binary markets with full metadata via /v0/search-markets."""
    cache = OUTPUT_DIR / 'all_markets.json'
    if cache.exists():
        print(f'[markets] Loading from cache ({cache.stat().st_size / 1024 / 1024:.0f} MB)')
        with open(cache) as f:
            return json.load(f)

    session = requests.Session()
    all_markets = []
    before_time = None
    batch = 0
    t_start = time.time()

    print('[markets] Fetching all resolved binary markets...')
    while True:
        t0 = time.time()
        params = {
            'term': '',
            'filter': 'resolved',
            'contractType': 'BINARY',
            'sort': 'newest',
            'limit': 1000,  # search-markets caps at 1000
        }
        if before_time is not None:
            params['beforeTime'] = before_time

        for attempt in range(5):
            try:
                resp = session.get(f'{API_BASE}/search-markets', params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                print(f'  Retry {attempt + 1}: {e} — waiting {wait}s')
                time.sleep(wait)
        else:
            print(f'  Failed after 5 retries at batch {batch}')
            break

        page = resp.json()
        if not page:
            break

        all_markets.extend(page)
        batch += 1
        before_time = page[-1].get('createdTime')

        if batch % 25 == 0:
            elapsed = time.time() - t_start
            print(f'  Batch {batch}: {len(all_markets):,} markets ({elapsed:.0f}s)')

        if len(page) < 1000:
            break

    print(f'[markets] Total: {len(all_markets):,} ({time.time() - t_start:.0f}s)')

    # Now fetch full details (with groupSlugs) for each market
    # search-markets doesn't return groupSlugs, so we need /v0/market/{id}
    print(f'[markets] Fetching full details with groupSlugs for {len(all_markets):,} markets...')
    full_markets = []
    t_start2 = time.time()

    for i, m in enumerate(all_markets):
        mid = m['id']
        for attempt in range(3):
            try:
                resp = session.get(f'{API_BASE}/market/{mid}', timeout=15)
                resp.raise_for_status()
                full_markets.append(resp.json())
                break
            except Exception as e:
                if attempt == 2:
                    # Fall back to search-markets data (no groupSlugs)
                    full_markets.append(m)
                else:
                    time.sleep(1)

        if (i + 1) % 2000 == 0:
            elapsed = time.time() - t_start2
            rate = (i + 1) / elapsed
            eta = (len(all_markets) - i - 1) / rate / 60
            print(f'  {i + 1}/{len(all_markets):,} ({elapsed:.0f}s, ETA {eta:.0f}min)')

    print(f'[markets] Full details fetched: {len(full_markets):,} ({time.time() - t_start2:.0f}s)')

    with open(cache, 'w') as f:
        json.dump(full_markets, f)
    print(f'[markets] Saved to {cache} ({cache.stat().st_size / 1024 / 1024:.0f} MB)')
    return full_markets


def fetch_all_bets(market_ids):
    """Fetch all bets via /v0/bets using afterTime pagination with limit=5000."""
    cache = OUTPUT_DIR / 'all_bets.json'
    if cache.exists():
        print(f'[bets] Loading from cache ({cache.stat().st_size / 1024 / 1024:.0f} MB)')
        with open(cache) as f:
            return json.load(f)

    target_ids = set(market_ids)
    session = requests.Session()
    all_bets = []
    after_time = 0  # start from the beginning
    batch = 0
    t_start = time.time()

    print(f'[bets] Fetching all bets (target: {len(target_ids):,} markets)...')
    while True:
        t0 = time.time()
        params = {'limit': 5000, 'afterTime': after_time, 'order': 'asc'}

        for attempt in range(5):
            try:
                resp = session.get(f'{API_BASE}/bets', params=params, timeout=60)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                print(f'  Retry {attempt + 1}: {e}')
                time.sleep(wait)
        else:
            print(f'  Failed after 5 retries at batch {batch}')
            break

        page = resp.json()
        if not page:
            break

        # Only keep bets for our target markets
        kept = [b for b in page if b.get('contractId') in target_ids]
        all_bets.extend(kept)
        batch += 1
        after_time = page[-1]['createdTime']

        elapsed = time.time() - t0
        if batch % 200 == 0:
            total_elapsed = time.time() - t_start
            ts = datetime.utcfromtimestamp(after_time / 1000).strftime('%Y-%m-%d')
            print(f'  Batch {batch}: scanned {batch * 5000:,}, kept {len(all_bets):,}, '
                  f'up to {ts} ({total_elapsed:.0f}s, {elapsed:.2f}s/req)')

        if len(page) < 5000:
            break

    total_time = time.time() - t_start
    print(f'[bets] Total: {len(all_bets):,} bets kept from {batch * 5000:,} scanned ({total_time:.0f}s)')

    with open(cache, 'w') as f:
        json.dump(all_bets, f)
    print(f'[bets] Saved to {cache} ({cache.stat().st_size / 1024 / 1024:.0f} MB)')
    return all_bets


def main():
    print(f'Start: {datetime.now():%Y-%m-%d %H:%M:%S}')
    print(f'Output: {OUTPUT_DIR.resolve()}\n')

    # Step 1: Markets
    markets = fetch_all_resolved_markets()

    # Filter to YES/NO resolved with minimum activity
    valid_ids = set()
    for m in markets:
        if m.get('resolution') not in ('YES', 'NO'):
            continue
        if (m.get('uniqueBettorCount') or 0) < 5:
            continue
        if (m.get('volume') or 0) < 100:
            continue
        ct = m.get('createdTime')
        clt = m.get('closeTime')
        if not ct or not clt or clt <= ct:
            continue
        valid_ids.add(m['id'])

    print(f'\nFiltered markets: {len(valid_ids):,}')

    # Step 2: Bets
    bets = fetch_all_bets(valid_ids)

    print(f'\nDone: {datetime.now():%Y-%m-%d %H:%M:%S}')
    print(f'Markets: {len(markets):,} total, {len(valid_ids):,} filtered')
    print(f'Bets: {len(bets):,}')


if __name__ == '__main__':
    main()
