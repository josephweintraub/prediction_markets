"""
Closing Price Pipeline for Prediction Markets

Fetches "closing line" prices from the Polymarket CLOB API at standardized
snap points in each contract's lifetime. This avoids the VWAP survivorship
bias where losing contracts' prices are anchored to the uncertain middle
period because volume dries up near resolution.

Pipeline:
  1. build_snap_table()     — compute snap timestamps from trades + endDate
  2. fetch_closing_prices() — fetch prices from CLOB API (resumable)
  3. compute_flb_from_closing_prices() — FLB calibration using fetched prices

The CLOB API endpoint:
  GET https://clob.polymarket.com/prices-history
  Params: market (token_id), startTs, endTs, fidelity (minutes)
"""

import duckdb
import pandas as pd
import numpy as np
import requests
import time
import json
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


CLOB_BASE = "https://clob.polymarket.com"
DEFAULT_SNAP_PCTS = [0.5, 0.7, 0.8, 0.9]


# ---------------------------------------------------------------------------
# Step 1: Build snap table from trades data
# ---------------------------------------------------------------------------

def build_snap_table(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    snap_pcts: list = None,
    min_lifetime_hours: float = 24,
    min_trades: int = 10,
) -> pd.DataFrame:
    """
    Compute snap timestamps and token IDs for each (conditionId, outcome).

    For each contract, defines lifetime = endDate - first_trade_timestamp.
    At each snap_pct, computes the unix timestamp of the snap point.

    Returns DataFrame with columns:
        conditionId, outcome, token_id, winning_outcome,
        first_trade_ts, end_ts, lifetime_hours, n_trades,
        snap_pct, snap_ts
    """
    if snap_pcts is None:
        snap_pcts = DEFAULT_SNAP_PCTS

    # Load closed markets with endDate
    from favorite_longshot import _load_closed_markets_with_end_ts
    markets = _load_closed_markets_with_end_ts(output_dir)
    con.register('_cp_markets', markets)

    print("Building snap table...")

    # Get lifecycle + token_id for each (conditionId, outcome)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cp_lifecycle AS
        SELECT
            t.conditionId,
            t.outcome,
            -- Take the most common asset (token_id) for this pair
            MODE(t.asset) AS token_id,
            m.winning_outcome,
            m.end_ts,
            MIN(t.timestamp) AS first_trade_ts,
            COUNT(*) AS n_trades,
            SUM(t.usdcSize) AS total_volume,
            (m.end_ts - MIN(t.timestamp)) / 3600.0 AS lifetime_hours
        FROM trades t
        INNER JOIN _cp_markets m ON t.conditionId = m.conditionId
        WHERE m.end_ts > 0
        GROUP BY t.conditionId, t.outcome, m.winning_outcome, m.end_ts
        HAVING COUNT(*) >= {min_trades}
          AND m.end_ts > MIN(t.timestamp)
          AND (m.end_ts - MIN(t.timestamp)) / 3600.0 >= {min_lifetime_hours}
    """)

    n = con.execute("SELECT COUNT(*) FROM _cp_lifecycle").fetchone()[0]
    print(f"  Contract-outcome pairs: {n:,}")

    # Create one row per (contract-outcome, snap_pct)
    snap_values = ", ".join(f"({p})" for p in snap_pcts)

    df = con.execute(f"""
        SELECT
            l.conditionId,
            l.outcome,
            l.token_id,
            l.winning_outcome,
            l.first_trade_ts,
            l.end_ts,
            l.lifetime_hours,
            l.n_trades,
            l.total_volume,
            s.snap_pct,
            CAST(l.first_trade_ts + (l.end_ts - l.first_trade_ts) * s.snap_pct
                 AS BIGINT) AS snap_ts
        FROM _cp_lifecycle l
        CROSS JOIN (VALUES {snap_values}) AS s(snap_pct)
    """).df()

    print(f"  Total snap points to fetch: {len(df):,} "
          f"({len(snap_pcts)} snap_pcts × {n:,} pairs)")

    # Cache snap table to parquet for fast resume
    cache_path = output_dir / "snap_table.parquet"
    df.to_parquet(cache_path, index=False)
    print(f"  Cached snap table to {cache_path}")

    return df


def load_snap_table(output_dir: Path) -> pd.DataFrame:
    """Load cached snap table, or raise if not found."""
    path = output_dir / "snap_table.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Snap table not found at {path}. "
            "Run build_snap_table() first."
        )
    df = pd.read_parquet(path)
    print(f"  Loaded cached snap table: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Step 2: Fetch closing prices from CLOB API
# ---------------------------------------------------------------------------

def _fetch_price_at_timestamp(
    token_id: str,
    snap_ts: int,
    fidelity_minutes: int = 60,
    window_seconds: int = 7200,
    session: requests.Session = None,
) -> Optional[float]:
    """
    Fetch the CLOB-quoted price closest to (but before) snap_ts.

    Queries a window of [snap_ts - window_seconds, snap_ts] at the given
    fidelity. Returns the last price in the window, or None on failure.
    """
    getter = session.get if session else requests.get
    try:
        r = getter(
            f"{CLOB_BASE}/prices-history",
            params={
                'market': str(token_id),
                'startTs': snap_ts - window_seconds,
                'endTs': snap_ts,
                'fidelity': fidelity_minutes,
            },
            timeout=15,
        )
        if r.status_code == 429:
            # Rate limited — back off and retry once
            time.sleep(2)
            r = getter(
                f"{CLOB_BASE}/prices-history",
                params={
                    'market': str(token_id),
                    'startTs': snap_ts - window_seconds,
                    'endTs': snap_ts,
                    'fidelity': fidelity_minutes,
                },
                timeout=15,
            )
        if r.status_code != 200:
            return None

        data = r.json()
        history = data.get('history', [])
        if not history:
            return None

        # Return the last price in the window (closest to snap_ts)
        return float(history[-1]['p'])

    except Exception:
        return None


def fetch_closing_prices(
    snap_table: pd.DataFrame,
    output_dir: Path,
    fidelity_minutes: int = 60,
    window_seconds: int = 7200,
    save_every: int = 2000,
    max_requests: Optional[int] = None,
    n_workers: int = 15,
) -> pd.DataFrame:
    """
    Fetch closing prices from CLOB API for each row in snap_table.

    Uses concurrent requests for speed (~15 workers).
    Resumable: checks for existing partial results in
    output_dir/closing_prices_partial.parquet and skips already-fetched rows.

    Args:
        snap_table: DataFrame from build_snap_table()
        fidelity_minutes: API price granularity (60 = hourly)
        window_seconds: how far back to look from snap_ts (default 2h)
        save_every: save partial results every N completed requests
        max_requests: stop after this many API calls (None = all)
        n_workers: number of concurrent threads

    Returns DataFrame with additional 'closing_price' column.
    """
    partial_path = output_dir / "closing_prices_partial.parquet"

    # Build unique (token_id, snap_ts) pairs to fetch
    snap_table = snap_table.copy()
    snap_table['fetch_key'] = (
        snap_table['token_id'].astype(str) + '_' +
        snap_table['snap_ts'].astype(str)
    )

    # Load partial results if they exist
    already_fetched = {}
    if partial_path.exists():
        partial = pd.read_parquet(partial_path)
        if 'fetch_key' in partial.columns and 'closing_price' in partial.columns:
            already_fetched = dict(
                zip(partial['fetch_key'], partial['closing_price'])
            )
            print(f"  Resuming: {len(already_fetched):,} prices already fetched")

    # Determine which unique (token_id, snap_ts) pairs still need fetching
    unique_pairs = snap_table[['token_id', 'snap_ts', 'fetch_key']].drop_duplicates()
    to_fetch = unique_pairs[~unique_pairs['fetch_key'].isin(already_fetched)]

    if max_requests is not None:
        to_fetch = to_fetch.head(max_requests)

    total = len(to_fetch)
    print(f"  Fetching {total:,} prices from CLOB API "
          f"(fidelity={fidelity_minutes}min, window={window_seconds}s, "
          f"workers={n_workers})...")

    results = dict(already_fetched)  # start from existing
    n_success = 0
    n_fail = 0
    completed = 0

    # Build work items as list of tuples
    work_items = [
        (row['token_id'], int(row['snap_ts']), row['fetch_key'])
        for _, row in to_fetch.iterrows()
    ]

    _thread_local = threading.local()

    def _get_session():
        if not hasattr(_thread_local, 'session'):
            _thread_local.session = requests.Session()
        return _thread_local.session

    def _do_fetch(item):
        token_id, snap_ts, fetch_key = item
        price = _fetch_price_at_timestamp(
            token_id, snap_ts, fidelity_minutes, window_seconds,
            session=_get_session()
        )
        return fetch_key, price

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_do_fetch, item): item for item in work_items}

        for future in as_completed(futures):
            fetch_key, price = future.result()
            completed += 1

            if price is not None:
                results[fetch_key] = price
                n_success += 1
            else:
                results[fetch_key] = np.nan
                n_fail += 1

            if completed % 200 == 0 or completed == total:
                pct = completed / total * 100
                print(f"    {completed:,}/{total:,} ({pct:.1f}%) — "
                      f"{n_success:,} ok, {n_fail:,} failed", flush=True)

            # Save partial results periodically
            if completed % save_every == 0:
                _save_partial(results, partial_path)

    # Final save
    _save_partial(results, partial_path)

    # Map results back to snap_table
    snap_table['closing_price'] = snap_table['fetch_key'].map(results)
    snap_table = snap_table.drop(columns=['fetch_key'])

    n_with_price = snap_table['closing_price'].notna().sum()
    print(f"  Done: {n_with_price:,}/{len(snap_table):,} rows have closing prices "
          f"({n_with_price/len(snap_table)*100:.1f}%)")

    return snap_table


def _save_partial(results: dict, path: Path):
    """Save partial fetch results to parquet."""
    df = pd.DataFrame([
        {'fetch_key': k, 'closing_price': v}
        for k, v in results.items()
    ])
    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Step 3: Save final closing prices
# ---------------------------------------------------------------------------

def save_closing_prices(snap_table_with_prices: pd.DataFrame, output_dir: Path):
    """Save the final closing prices table to parquet."""
    out_path = output_dir / "closing_prices.parquet"
    snap_table_with_prices.to_parquet(out_path, index=False)
    print(f"  Saved to {out_path}")
    return out_path


def load_closing_prices(output_dir: Path) -> pd.DataFrame:
    """Load previously saved closing prices."""
    path = output_dir / "closing_prices.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Closing prices not found at {path}. "
            "Run fetch_closing_prices() first."
        )
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# Step 4: FLB analysis using closing prices
# ---------------------------------------------------------------------------

def compute_flb_from_closing_prices(
    output_dir: Path,
    n_bins: int = 20,
    snap_pcts: list = None,
    exclude_near_resolved: bool = True,
    near_resolved_threshold: float = 0.05,
) -> pd.DataFrame:
    """
    Compute FLB calibration bins using API-fetched closing prices.

    Loads closing_prices.parquet, filters to valid prices, bins by
    closing_price, and computes calibration stats.

    Returns DataFrame with 'category' column (snap label) for
    plot_favorite_longshot_by_category.
    """
    if snap_pcts is None:
        snap_pcts = DEFAULT_SNAP_PCTS

    df = load_closing_prices(output_dir)

    # Filter to valid prices
    df = df[df['closing_price'].notna()].copy()
    df = df[(df['closing_price'] >= 0) & (df['closing_price'] <= 1)].copy()

    if exclude_near_resolved:
        before = len(df)
        df = df[
            (df['closing_price'] >= near_resolved_threshold) &
            (df['closing_price'] <= 1 - near_resolved_threshold)
        ].copy()
        print(f"  Excluded {before - len(df):,} near-resolved "
              f"(price < {near_resolved_threshold} or "
              f"> {1 - near_resolved_threshold})")

    df['won'] = (df['outcome'] == df['winning_outcome']).astype(int)
    df['contract_return'] = np.where(
        df['won'] == 1,
        1.0 - df['closing_price'],
        -df['closing_price'],
    )
    df['bin_idx'] = np.floor(df['closing_price'] * n_bins).astype(int)
    df = df[df['bin_idx'] >= 0].copy()

    all_results = []
    for pct in snap_pcts:
        sub = df[np.isclose(df['snap_pct'], pct)].copy()
        if sub.empty:
            continue

        remaining = round((1 - pct) * 100)
        label = f"{remaining}% remaining"

        agg = sub.groupby('bin_idx').agg(
            count=('won', 'size'),
            mean_implied_prob=('closing_price', 'mean'),
            empirical_win_rate=('won', 'mean'),
            mean_return=('contract_return', 'mean'),
        ).reset_index()
        agg['category'] = label

        n_contracts = agg['count'].sum()
        print(f"  {label}: {n_contracts:,} contract-outcomes")

        all_results.append(agg)

    combined = pd.concat(all_results, ignore_index=True)
    return combined


# ---------------------------------------------------------------------------
# Convenience: full pipeline
# ---------------------------------------------------------------------------

def run_full_pipeline(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    snap_pcts: list = None,
    min_lifetime_hours: float = 24,
    min_trades: int = 10,
    fidelity_minutes: int = 60,
    max_requests: Optional[int] = None,
    n_workers: int = 15,
    rebuild_snap_table: bool = False,
) -> pd.DataFrame:
    """
    Run the complete closing price pipeline:
    1. Build snap table from trades (or load cached)
    2. Fetch prices from CLOB API (concurrent, resumable)
    3. Save results
    4. Compute FLB calibration

    Returns the FLB DataFrame ready for plotting.
    """
    if snap_pcts is None:
        snap_pcts = DEFAULT_SNAP_PCTS

    print("=" * 60)
    print("CLOSING PRICE PIPELINE")
    print("=" * 60)

    # Step 1: Build or load snap table
    snap_cache = output_dir / "snap_table.parquet"
    if snap_cache.exists() and not rebuild_snap_table:
        snap_table = load_snap_table(output_dir)
    else:
        snap_table = build_snap_table(
            con, output_dir, snap_pcts,
            min_lifetime_hours, min_trades,
        )

    # Step 2
    snap_with_prices = fetch_closing_prices(
        snap_table, output_dir,
        fidelity_minutes=fidelity_minutes,
        max_requests=max_requests,
        n_workers=n_workers,
    )

    # Step 3
    save_closing_prices(snap_with_prices, output_dir)

    # Step 4
    print("\nComputing FLB calibration from closing prices...")
    flb = compute_flb_from_closing_prices(
        output_dir, snap_pcts=snap_pcts,
    )

    return flb
