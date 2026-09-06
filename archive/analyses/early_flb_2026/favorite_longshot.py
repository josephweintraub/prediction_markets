"""
Favorite-Longshot Bias Analysis

Shows that low implied probability (longshot) bets have negative returns on average,
and high implied probability (favorite) bets have better returns — classic
favorite-longshot bias first documented by Griffith (1949) and formalized by
Thaler & Ziemba (1988).

Functions:
  - compute_favorite_longshot_bins: bin trades by implied prob, compute win rates & returns
  - compute_favorite_longshot_tails: same but zoomed into <10% and >90% tails
  - compute_favorite_longshot_by_category: segmented by market category
  - compute_favorite_longshot_bins_by_volume_tier_trade_level: trade-level FLB by volume tier
  - compute_favorite_longshot_bins_by_outcome_count: trade-level FLB by binary vs multi-outcome
  - compute_favorite_longshot_bins_by_outcome_count_contract_level: contract-level FLB by outcome count
  - compute_favorite_longshot_bins_by_volume_tier: contract-level FLB by volume tier
  - compute_favorite_longshot_bins_snap_price: closing-line FLB at multiple lifetime snap points
  - compute_flb_by_trader_volume_tier: trade-level FLB grouped by TRADER's lifetime volume
  - compute_trader_flb_attribution: for each probability bin, % of trades/volume from each trader tier
  - plot_favorite_longshot: calibration plot (implied vs actual) + return plot
  - plot_favorite_longshot_tails: histograms of returns in extreme tails
  - plot_favorite_longshot_by_category: per-category calibration plots
  - plot_trader_flb_attribution: stacked bar chart of trader tier composition per probability bin
  - fetch_and_save_market_categories: fetch categories from Gamma API
  - get_market_categories: load saved category mapping
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_resolutions_have_condition_id(df: pd.DataFrame) -> pd.DataFrame:
    """Handle parquet that might use condition_id vs conditionId."""
    if 'condition_id' in df.columns and 'conditionId' not in df.columns:
        df = df.rename(columns={'condition_id': 'conditionId'})
    return df


# Module-level cache: avoids re-reading parquet + re-parsing JSON on every call.
# Keyed by resolved str(output_dir) so multiple dirs are supported.
_closed_markets_cache: dict = {}


def load_verified_closed_markets(output_dir: Path) -> pd.DataFrame:
    """
    Load API-verified closed markets from market_resolutions.parquet.
    Returns DataFrame with conditionId, winning_outcome columns.
    Cached after first call per output_dir.
    """
    import json

    cache_key = str(output_dir)
    if cache_key in _closed_markets_cache:
        return _closed_markets_cache[cache_key]

    resolutions_path = output_dir / "market_resolutions.parquet"
    if not resolutions_path.exists():
        raise FileNotFoundError(
            f"Market resolutions not found at {resolutions_path}.\n"
            "Run the market resolution fetch first."
        )

    df = pd.read_parquet(resolutions_path)
    df = _ensure_resolutions_have_condition_id(df)

    # Support both old Gamma API format and new on-chain pipeline format
    if 'outcomePrices' in df.columns:
        # Old format: parse outcomePrices, outcomes, filter by closed
        import json as _json

        valid_prices = df['outcomePrices'].dropna()
        valid_prices = valid_prices[valid_prices.astype(str).str.strip().astype(bool)]
        parsed_prices = valid_prices.map(_json.loads)
        df.loc[valid_prices.index, 'price1'] = parsed_prices.str[0].astype(float)
        df.loc[valid_prices.index, 'price2'] = parsed_prices.str[1].astype(float)

        valid_outcomes = df['outcomes'].dropna()
        valid_outcomes = valid_outcomes[valid_outcomes.astype(str).str.strip().astype(bool)]
        parsed_outcomes = valid_outcomes.map(_json.loads)
        df.loc[valid_outcomes.index, 'outcome1'] = parsed_outcomes.str[0]
        df.loc[valid_outcomes.index, 'outcome2'] = parsed_outcomes.str[1]

        closed = df[df['closed'] == True].copy()
        clean = closed[
            ((closed['price1'] <= 0.01) & (closed['price2'] >= 0.99)) |
            ((closed['price1'] >= 0.99) & (closed['price2'] <= 0.01))
        ].copy()

        clean['winning_outcome'] = np.where(
            clean['price1'] >= 0.99, clean['outcome1'], clean['outcome2']
        )

        result = clean[['conditionId', 'winning_outcome', 'outcome1', 'outcome2',
                         'price1', 'price2']]
    else:
        # New on-chain format: already has conditionId + winning_outcome
        result = df[['conditionId', 'winning_outcome']].drop_duplicates()

    _closed_markets_cache[cache_key] = result
    return result


def _ensure_closed_markets_registered(con, output_dir: Path):
    """Register closed_markets as a DuckDB temp table (once per session).
    Subsequent calls are no-ops. All FLB functions should call this
    instead of load + register individually."""
    try:
        con.execute("SELECT 1 FROM _closed_markets LIMIT 1")
        return  # already registered
    except Exception:
        pass
    cm = load_verified_closed_markets(output_dir)
    con.register('_cm_reg_tmp', cm)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE _closed_markets AS "
        "SELECT * FROM _cm_reg_tmp"
    )
    con.unregister('_cm_reg_tmp')


def _ensure_mkt_lifecycle(con):
    """Create _mkt_lifecycle temp table if it doesn't already exist."""
    try:
        con.execute("SELECT 1 FROM _mkt_lifecycle LIMIT 1")
        return
    except Exception:
        pass
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _mkt_lifecycle AS
        SELECT conditionId,
               MIN(timestamp) AS mkt_start,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
        FROM trades
        GROUP BY conditionId
    """)


def _ensure_contract_vwap(con):
    """Create fl_vol_contracts (per-contract VWAP + volume) if not exists.
    Shared by all volume-tier FLB functions."""
    try:
        con.execute("SELECT 1 FROM fl_vol_contracts LIMIT 1")
        return
    except Exception:
        pass
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_vol_contracts AS
        SELECT
            t.conditionId, t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            SUM(t.usdcSize) AS total_volume,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.conditionId, t.outcome, c.winning_outcome
    """)


def _ensure_trader_vol_lookup(con):
    """Create fl_tv_lookup (per-trader lifetime volume) if not exists."""
    try:
        con.execute("SELECT 1 FROM fl_tv_lookup LIMIT 1")
        return
    except Exception:
        pass
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_tv_lookup AS
        SELECT proxyWallet, SUM(usdcSize) AS trader_vol
        FROM trades
        GROUP BY proxyWallet
    """)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_favorite_longshot_bins(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    max_trades_sample: int = 10_000_000,
) -> pd.DataFrame:
    """
    Compute implied probability bins vs actual return and win rate across all
    trades on verified closed markets.

    Returns DataFrame with columns: bin_idx, mean_implied_prob,
    empirical_win_rate, mean_return, count.
    """
    _ensure_closed_markets_registered(con, output_dir)

    limit_clause = f"LIMIT {max_trades_sample}" if max_trades_sample else ""

    print("Computing trade-level outcomes on verified closed markets...")
    agg = con.execute(f"""
        WITH fl AS (
            SELECT
                t.price AS implied_prob,
                CAST(t.outcome = c.winning_outcome AS DOUBLE) AS won,
                CASE WHEN t.outcome = c.winning_outcome
                     THEN (1.0 - t.price) ELSE (-t.price) END AS trade_return
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            WHERE t.price >= 0 AND t.price <= 1
            AND t.side = 'BUY'
            {limit_clause}
        )
        SELECT
            FLOOR(implied_prob * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(implied_prob) AS mean_implied_prob,
            AVG(won) AS empirical_win_rate,
            AVG(trade_return) AS mean_return
        FROM fl
        WHERE FLOOR(implied_prob * {n_bins}) >= 0
        GROUP BY bin_idx
        ORDER BY bin_idx
    """).df()

    n_trades = int(agg['count'].sum())
    print(f"  Trades matched to verified closed markets: {n_trades:,}")

    # Add human-readable labels
    agg['bin_label'] = agg['bin_idx'].apply(
        lambda i: f"{i/n_bins*100:.0f}-{(i+1)/n_bins*100:.0f}%"
    )

    return agg


def compute_favorite_longshot_tails(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    left_cutoff: float = 0.10,
    right_cutoff: float = 0.90,
    n_bins_per_tail: int = 20,
    max_trades_sample: int = 10_000_000,
) -> pd.DataFrame:
    """
    Compute bins for extreme tails only: implied prob < left_cutoff (e.g. <10%)
    and implied prob > right_cutoff (e.g. >90%). Finer bins within each tail.

    Returns DataFrame with columns: tail ('left'|'right'), bin_idx, bin_center,
    bin_label, count, mean_implied_prob, empirical_win_rate, mean_return.
    """
    _ensure_closed_markets_registered(con, output_dir)

    limit_clause = f"LIMIT {max_trades_sample}" if max_trades_sample else ""

    left_bin_sql = f"FLOOR(t.price / {left_cutoff} * {n_bins_per_tail})"
    right_bin_sql = f"FLOOR((t.price - {right_cutoff}) / {1.0 - right_cutoff} * {n_bins_per_tail})"

    print(f"Computing tail-only trades (implied prob < {left_cutoff*100:.0f}% or > {right_cutoff*100:.0f}%)...")
    agg = con.execute(f"""
        WITH fl_tail AS (
            SELECT 'left' AS tail,
                   {left_bin_sql} AS bin_idx,
                   t.price AS implied_prob,
                   CAST(t.outcome = c.winning_outcome AS DOUBLE) AS won,
                   CASE WHEN t.outcome = c.winning_outcome
                        THEN (1.0 - t.price) ELSE (-t.price) END AS trade_return
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            WHERE t.price < {left_cutoff}
            {limit_clause}

            UNION ALL

            SELECT 'right' AS tail,
                   {right_bin_sql} AS bin_idx,
                   t.price AS implied_prob,
                   CAST(t.outcome = c.winning_outcome AS DOUBLE) AS won,
                   CASE WHEN t.outcome = c.winning_outcome
                        THEN (1.0 - t.price) ELSE (-t.price) END AS trade_return
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            WHERE t.price > {right_cutoff}
            {limit_clause}
        )
        SELECT tail, bin_idx,
               COUNT(*) AS count,
               AVG(implied_prob) AS mean_implied_prob,
               AVG(won) AS empirical_win_rate,
               AVG(trade_return) AS mean_return
        FROM fl_tail
        WHERE bin_idx >= 0
        GROUP BY tail, bin_idx
        ORDER BY tail, bin_idx
    """).df()

    n_trades = int(agg['count'].sum())
    print(f"  Tail trades: {n_trades:,}")

    # Compute bin centers and labels (vectorized)
    is_left = agg['tail'] == 'left'
    lo = np.where(is_left,
                  agg['bin_idx'] / n_bins_per_tail * left_cutoff,
                  right_cutoff + agg['bin_idx'] / n_bins_per_tail * (1 - right_cutoff))
    hi = np.where(is_left,
                  (agg['bin_idx'] + 1) / n_bins_per_tail * left_cutoff,
                  right_cutoff + (agg['bin_idx'] + 1) / n_bins_per_tail * (1 - right_cutoff))
    agg['bin_center'] = (lo + hi) / 2
    agg['bin_label'] = [f"{l*100:.1f}-{h*100:.1f}%" for l, h in zip(lo, hi)]

    return agg


def compute_flb_basic_batch(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    left_cutoff: float = 0.15,
    right_cutoff: float = 0.85,
    n_bins_per_tail: int = 20,
    max_trades_sample: int = 10_000_000,
    categories: Optional[list] = None,
) -> tuple:
    """
    Compute aggregate FLB bins, tails, and category FLB in ONE trades scan.
    Returns (bins_df, tails_df, by_category_df).

    Replaces three separate calls to:
      compute_favorite_longshot_bins()
      compute_favorite_longshot_tails()
      compute_favorite_longshot_by_category()
    """
    _ensure_closed_markets_registered(con, output_dir)

    limit_clause = f"LIMIT {max_trades_sample}" if max_trades_sample else ""

    case_sql = _CATEGORY_CASE_SQL_T

    left_bin_sql = f"FLOOR(price / {left_cutoff} * {n_bins_per_tail})"
    right_bin_sql = f"FLOOR((price - {right_cutoff}) / {1.0 - right_cutoff} * {n_bins_per_tail})"

    cat_filter = ""
    if categories:
        cat_list = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND category IN ({cat_list})"

    print("Computing aggregate FLB, tails, and category FLB in a single scan...")
    raw = con.execute(f"""
        WITH base AS (
            SELECT t.price,
                   CAST(t.outcome = c.winning_outcome AS DOUBLE) AS won,
                   CASE WHEN t.outcome = c.winning_outcome
                        THEN 1.0 - t.price ELSE -t.price END AS trade_return,
                   {case_sql} AS category
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            WHERE t.price >= 0 AND t.price <= 1
            AND t.side = 'BUY'
            {limit_clause}
        ),
        agg_bins AS (
            SELECT 'bins' AS result_set, NULL AS tail, NULL AS category,
                   FLOOR(price * {n_bins}) AS bin_idx,
                   COUNT(*) AS count,
                   AVG(price) AS mean_implied_prob,
                   AVG(won) AS empirical_win_rate,
                   AVG(trade_return) AS mean_return
            FROM base
            WHERE FLOOR(price * {n_bins}) >= 0
            GROUP BY FLOOR(price * {n_bins})
        ),
        agg_left AS (
            SELECT 'tails' AS result_set, 'left' AS tail, NULL AS category,
                   {left_bin_sql} AS bin_idx,
                   COUNT(*) AS count,
                   AVG(price) AS mean_implied_prob,
                   AVG(won) AS empirical_win_rate,
                   AVG(trade_return) AS mean_return
            FROM base WHERE price < {left_cutoff} AND {left_bin_sql} >= 0
            GROUP BY {left_bin_sql}
        ),
        agg_right AS (
            SELECT 'tails' AS result_set, 'right' AS tail, NULL AS category,
                   {right_bin_sql} AS bin_idx,
                   COUNT(*) AS count,
                   AVG(price) AS mean_implied_prob,
                   AVG(won) AS empirical_win_rate,
                   AVG(trade_return) AS mean_return
            FROM base WHERE price > {right_cutoff} AND {right_bin_sql} >= 0
            GROUP BY {right_bin_sql}
        ),
        agg_cat AS (
            SELECT 'cat' AS result_set, NULL AS tail, category,
                   FLOOR(price * {n_bins}) AS bin_idx,
                   COUNT(*) AS count,
                   AVG(price) AS mean_implied_prob,
                   AVG(won) AS empirical_win_rate,
                   AVG(trade_return) AS mean_return
            FROM base
            WHERE FLOOR(price * {n_bins}) >= 0
            {cat_filter}
            GROUP BY category, FLOOR(price * {n_bins})
        )
        SELECT * FROM agg_bins
        UNION ALL SELECT * FROM agg_left
        UNION ALL SELECT * FROM agg_right
        UNION ALL SELECT * FROM agg_cat
    """).df()

    # --- Split into three result DataFrames ---

    # 1. Bins
    bins_df = raw[raw['result_set'] == 'bins'][
        ['bin_idx', 'count', 'mean_implied_prob', 'empirical_win_rate', 'mean_return']
    ].copy().sort_values('bin_idx').reset_index(drop=True)
    bins_df['bin_label'] = bins_df['bin_idx'].apply(
        lambda i: f"{i/n_bins*100:.0f}-{(i+1)/n_bins*100:.0f}%"
    )
    n_trades = int(bins_df['count'].sum())
    print(f"  Trades matched to verified closed markets: {n_trades:,}")

    # 2. Tails
    tails_df = raw[raw['result_set'] == 'tails'][
        ['tail', 'bin_idx', 'count', 'mean_implied_prob', 'empirical_win_rate', 'mean_return']
    ].copy().sort_values(['tail', 'bin_idx']).reset_index(drop=True)
    is_left = tails_df['tail'] == 'left'
    lo = np.where(is_left,
                  tails_df['bin_idx'] / n_bins_per_tail * left_cutoff,
                  right_cutoff + tails_df['bin_idx'] / n_bins_per_tail * (1 - right_cutoff))
    hi = np.where(is_left,
                  (tails_df['bin_idx'] + 1) / n_bins_per_tail * left_cutoff,
                  right_cutoff + (tails_df['bin_idx'] + 1) / n_bins_per_tail * (1 - right_cutoff))
    tails_df['bin_center'] = (lo + hi) / 2
    tails_df['bin_label'] = [f"{l*100:.1f}-{h*100:.1f}%" for l, h in zip(lo, hi)]
    left_n = int(tails_df[is_left]['count'].sum())
    right_n = int(tails_df[~is_left]['count'].sum())
    print(f"  Tail trades: {left_n + right_n:,} (left: {left_n:,}, right: {right_n:,})")

    # 3. By category
    cat_df = raw[raw['result_set'] == 'cat'][
        ['category', 'bin_idx', 'count', 'mean_implied_prob', 'empirical_win_rate', 'mean_return']
    ].copy().sort_values(['category', 'bin_idx']).reset_index(drop=True)
    cat_dist = cat_df.groupby('category')['count'].sum().reset_index().sort_values('count', ascending=False)
    print("  Category distribution (trades):")
    for _, row in cat_dist.iterrows():
        print(f"    {row['category']:20} {row['count']:>12,}")

    return bins_df, tails_df, cat_df


def compute_favorite_longshot_bins_by_volume_tier_trade_level(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    Trade-level FLB segmented by contract volume tier.

    First computes total USDC volume per (conditionId, outcome) and assigns a
    volume tier. Then joins back to individual trades so each trade is one
    observation (binned by trade price), but grouped by its contract's tier.

    Returns DataFrame with 'category' column (volume tier) compatible with
    plot_favorite_longshot_by_category.
    """
    if thresholds is None:
        thresholds = [121, 7468]
    if labels is None:
        labels = ['<$121 (Q1)', '$121–$7.5K (Q2–Q3)', '>$7.5K (Q4)']

    _ensure_closed_markets_registered(con, output_dir)

    # Build CASE WHEN for volume tiers
    case_parts = []
    for i, threshold in enumerate(thresholds):
        case_parts.append(f"WHEN total_volume < {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    volume_case_sql = "CASE " + " ".join(case_parts) + " END"

    print(f"Computing trade-level FLB by volume tier (thresholds={thresholds})...")

    # Step 1: compute total volume per contract-outcome pair
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_vol_lookup AS
        SELECT
            t.conditionId,
            t.outcome,
            SUM(t.usdcSize) AS total_volume
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.conditionId, t.outcome
    """)

    # Step 2: aggregate directly from the join — never materialize the full
    # 136M-row joined table, which would blow up memory.
    print("  Aggregating directly from join (no intermediate temp table)...")
    agg = con.execute(f"""
        SELECT
            {volume_case_sql.replace('total_volume', 'v.total_volume')} AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN fl_vol_lookup v
            ON t.conditionId = v.conditionId AND t.outcome = v.outcome
        WHERE t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()

    n_trades = int(agg['count'].sum())
    print(f"  Trades matched: {n_trades:,}")

    tier_dist = (
        agg.groupby('category')['count'].sum()
        .reset_index()
        .sort_values('count', ascending=False)
    )
    print("  Volume tier distribution (trades):")
    for _, row in tier_dist.iterrows():
        print(f"    {row['category']:25} {row['count']:>12,} trades")

    con.execute("DROP TABLE IF EXISTS fl_vol_lookup")

    return agg


def compute_favorite_longshot_bins_by_outcome_count(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    Trade-level FLB segmented by the number of outcomes per event.

    Uses eventSlug to count how many distinct conditionIds belong to the same
    parent event. Events with <= 2 outcomes are 'Binary'; events with more are
    multi-outcome markets (e.g. "Who will win the NBA MVP?").

    Each trade is one observation, binned by trade price.

    Args:
        thresholds: cutpoints on outcome count, e.g. [2, 10] → 3 tiers
        labels: tier names (len = len(thresholds) + 1)

    Returns DataFrame with 'category' column for plot_favorite_longshot_by_category.
    """
    if thresholds is None:
        thresholds = [2, 10]
    if labels is None:
        labels = ['Binary (1–2)', 'Small multi (3–10)', 'Large multi (10+)']

    _ensure_closed_markets_registered(con, output_dir)

    # Build CASE WHEN
    case_parts = []
    for i, threshold in enumerate(thresholds):
        case_parts.append(f"WHEN n_outcomes <= {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    case_sql = "CASE " + " ".join(case_parts) + " END"

    print(f"Computing trade-level FLB by outcome count (thresholds={thresholds})...")

    # Step 1: count distinct conditionIds per eventSlug (among closed markets)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_event_outcomes AS
        SELECT
            t.eventSlug,
            COUNT(DISTINCT t.conditionId) AS n_outcomes
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.eventSlug
    """)

    event_dist = con.execute("""
        SELECT
            CASE WHEN n_outcomes <= 2 THEN 'Binary (1-2)'
                 WHEN n_outcomes <= 10 THEN 'Small multi (3-10)'
                 ELSE 'Large multi (10+)' END AS tier,
            COUNT(*) AS n_events,
            SUM(n_outcomes) AS total_contracts
        FROM fl_event_outcomes
        GROUP BY 1 ORDER BY 1
    """).df()
    print("  Event distribution:")
    for _, row in event_dist.iterrows():
        print(f"    {row['tier']:25} {row['n_events']:>8,} events  "
              f"({row['total_contracts']:>10,} contracts)")

    # Step 2: aggregate directly from the join — never materialize the full
    # 136M-row joined table, which would blow up memory.
    print("  Aggregating directly from join (no intermediate temp table)...")
    agg = con.execute(f"""
        SELECT
            {case_sql.replace('n_outcomes', 'oe.n_outcomes')} AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN fl_event_outcomes oe ON t.eventSlug = oe.eventSlug
        WHERE t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()

    n_trades = int(agg['count'].sum())
    print(f"  Trades matched: {n_trades:,}")

    tier_dist = (
        agg.groupby('category')['count'].sum()
        .reset_index()
        .sort_values('category')
    )
    print("  Trade distribution by tier:")
    for _, row in tier_dist.iterrows():
        print(f"    {row['category']:25} {row['count']:>12,} trades")

    con.execute("DROP TABLE IF EXISTS fl_event_outcomes")

    return agg


# ---------------------------------------------------------------------------
# Contract-level (equal-weight) versions
# ---------------------------------------------------------------------------

def compute_favorite_longshot_bins_contract_level(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
) -> pd.DataFrame:
    """
    Contract-level FLB: one observation per (conditionId, outcome).

    For each contract-outcome pair, compute the VWAP (volume-weighted average
    price) and a single won/lost flag. Then bin by VWAP and compute win rates
    and returns. Every contract gets equal weight regardless of trade count.

    Returns DataFrame with columns: bin_idx, mean_implied_prob,
    empirical_win_rate, mean_return, count, bin_label.
    """
    _ensure_closed_markets_registered(con, output_dir)

    print("Computing contract-level outcomes (one obs per conditionId x outcome)...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_contracts AS
        SELECT
            t.conditionId,
            t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.conditionId, t.outcome, c.winning_outcome
    """)

    n_contracts = con.execute("SELECT COUNT(*) FROM fl_contracts").fetchone()[0]
    print(f"  Unique contract-outcome pairs: {n_contracts:,}")

    # Compute return from VWAP
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_contracts_with_return AS
        SELECT *,
            CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END AS contract_return
        FROM fl_contracts
        WHERE vwap >= 0 AND vwap <= 1 AND vwap IS NOT NULL
    """)

    # Bin by VWAP
    agg = con.execute(f"""
        SELECT
            FLOOR(vwap * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(vwap) AS mean_implied_prob,
            AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
            AVG(contract_return) AS mean_return
        FROM fl_contracts_with_return
        WHERE FLOOR(vwap * {n_bins}) >= 0
        GROUP BY FLOOR(vwap * {n_bins})
        ORDER BY 1
    """).df()

    agg['bin_label'] = agg['bin_idx'].apply(
        lambda i: f"{i/n_bins*100:.0f}-{(i+1)/n_bins*100:.0f}%"
    )

    for _t in ['fl_contracts', 'fl_contracts_with_return']:
        con.execute(f"DROP TABLE IF EXISTS {_t}")

    return agg


def compute_favorite_longshot_tails_contract_level(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    left_cutoff: float = 0.15,
    right_cutoff: float = 0.85,
    n_bins_per_tail: int = 20,
) -> pd.DataFrame:
    """
    Contract-level tails: one obs per (conditionId, outcome), VWAP-based,
    zoomed into the extreme tails.
    """
    _ensure_closed_markets_registered(con, output_dir)

    print(f"Computing contract-level tail outcomes (VWAP < {left_cutoff} or > {right_cutoff})...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fl_contracts_tails AS
        SELECT
            t.conditionId,
            t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.conditionId, t.outcome, c.winning_outcome
        HAVING vwap < {left_cutoff} OR vwap > {right_cutoff}
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_ct_with_return AS
        SELECT *,
            CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END AS contract_return
        FROM fl_contracts_tails
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
    """)

    n_contracts = con.execute("SELECT COUNT(*) FROM fl_ct_with_return").fetchone()[0]
    print(f"  Contract-outcome pairs in tails: {n_contracts:,}")

    left_bin_sql = f"FLOOR(vwap / {left_cutoff} * {n_bins_per_tail})"
    right_bin_sql = f"FLOOR((vwap - {right_cutoff}) / {1.0 - right_cutoff} * {n_bins_per_tail})"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fl_ct_binned AS
        SELECT 'left' AS tail, {left_bin_sql} AS bin_idx, vwap, won, contract_return
        FROM fl_ct_with_return WHERE vwap < {left_cutoff}
        UNION ALL
        SELECT 'right' AS tail, {right_bin_sql} AS bin_idx, vwap, won, contract_return
        FROM fl_ct_with_return WHERE vwap > {right_cutoff}
    """)

    agg = con.execute("""
        SELECT
            tail, bin_idx,
            COUNT(*) AS count,
            AVG(vwap) AS mean_implied_prob,
            AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
            AVG(contract_return) AS mean_return
        FROM fl_ct_binned WHERE bin_idx >= 0
        GROUP BY tail, bin_idx
        ORDER BY tail, bin_idx
    """).df()

    def center_and_label(row):
        if row['tail'] == 'left':
            lo = row['bin_idx'] / n_bins_per_tail * left_cutoff
            hi = (row['bin_idx'] + 1) / n_bins_per_tail * left_cutoff
        else:
            lo = right_cutoff + row['bin_idx'] / n_bins_per_tail * (1 - right_cutoff)
            hi = right_cutoff + (row['bin_idx'] + 1) / n_bins_per_tail * (1 - right_cutoff)
        return pd.Series({'bin_center': (lo + hi) / 2,
                          'bin_label': f"{lo*100:.1f}-{hi*100:.1f}%"})

    agg[['bin_center', 'bin_label']] = agg.apply(center_and_label, axis=1)

    for _t in ['fl_contracts_tails', 'fl_ct_with_return', 'fl_ct_binned']:
        con.execute(f"DROP TABLE IF EXISTS {_t}")

    return agg


def compute_favorite_longshot_by_category_contract_level(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    categories: Optional[list] = None,
) -> pd.DataFrame:
    """
    Contract-level FLB segmented by category (slug-based).
    One observation per (conditionId, outcome) using VWAP.
    """
    _ensure_closed_markets_registered(con, output_dir)

    case_sql = _CATEGORY_CASE_SQL_T

    print("  Contract-level category FLB (one obs per contract-outcome, VWAP)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fl_cat_contracts AS
        SELECT
            {case_sql} AS category,
            t.conditionId,
            t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY 1, t.conditionId, t.outcome, c.winning_outcome
    """)

    cat_dist = con.execute("""
        SELECT category, COUNT(*) as n
        FROM fl_cat_contracts
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
        GROUP BY category ORDER BY n DESC
    """).df()
    print("  Category distribution (contracts):")
    for _, row in cat_dist.iterrows():
        print(f"    {row['category']:20} {row['n']:>10,}")

    cat_filter = ""
    if categories:
        cat_list = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND category IN ({cat_list})"

    agg = con.execute(f"""
        SELECT
            category,
            FLOOR(vwap * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(vwap) AS mean_implied_prob,
            AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END) AS mean_return
        FROM fl_cat_contracts
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
          AND FLOOR(vwap * {n_bins}) >= 0
        {cat_filter}
        GROUP BY category, FLOOR(vwap * {n_bins})
        ORDER BY category, 2
    """).df()

    con.execute("DROP TABLE IF EXISTS fl_cat_contracts")

    return agg


def compute_flb_contract_level_batch(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    left_cutoff: float = 0.15,
    right_cutoff: float = 0.85,
    n_bins_per_tail: int = 20,
    categories: Optional[list] = None,
) -> tuple:
    """
    Compute contract-level FLB (all bins, tails, by-category) in ONE trades scan.
    Returns (cl_bins_df, cl_tails_df, cl_by_cat_df).

    Replaces three separate calls to:
      compute_favorite_longshot_bins_contract_level()
      compute_favorite_longshot_tails_contract_level()
      compute_favorite_longshot_by_category_contract_level()
    """
    _ensure_closed_markets_registered(con, output_dir)

    case_sql = _CATEGORY_CASE_SQL_T

    print("Computing contract-level VWAP table (single trades scan for all 3 analyses)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _cl_batch_vwap AS
        SELECT
            {case_sql} AS category,
            t.conditionId,
            t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY 1, t.conditionId, t.outcome, c.winning_outcome
    """)

    n_contracts = con.execute("SELECT COUNT(*) FROM _cl_batch_vwap WHERE vwap IS NOT NULL").fetchone()[0]
    print(f"  Contract-outcome pairs: {n_contracts:,}")

    # --- 1. All bins ---
    cl_bins = con.execute(f"""
        SELECT FLOOR(vwap * {n_bins}) AS bin_idx,
               COUNT(*) AS count,
               AVG(vwap) AS mean_implied_prob,
               AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
               AVG(CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END) AS mean_return
        FROM _cl_batch_vwap
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
          AND FLOOR(vwap * {n_bins}) >= 0
        GROUP BY 1 ORDER BY 1
    """).df()
    cl_bins['bin_label'] = cl_bins['bin_idx'].apply(
        lambda i: f"{i/n_bins*100:.0f}-{(i+1)/n_bins*100:.0f}%"
    )

    # --- 2. Tails ---
    left_bin_sql = f"FLOOR(vwap / {left_cutoff} * {n_bins_per_tail})"
    right_bin_sql = f"FLOOR((vwap - {right_cutoff}) / {1.0 - right_cutoff} * {n_bins_per_tail})"

    cl_tails = con.execute(f"""
        SELECT 'left' AS tail,
               {left_bin_sql} AS bin_idx,
               COUNT(*) AS count,
               AVG(vwap) AS mean_implied_prob,
               AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
               AVG(CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END) AS mean_return
        FROM _cl_batch_vwap
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap < {left_cutoff}
          AND {left_bin_sql} >= 0
        GROUP BY {left_bin_sql}

        UNION ALL

        SELECT 'right' AS tail,
               {right_bin_sql} AS bin_idx,
               COUNT(*) AS count,
               AVG(vwap) AS mean_implied_prob,
               AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
               AVG(CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END) AS mean_return
        FROM _cl_batch_vwap
        WHERE vwap IS NOT NULL AND vwap > {right_cutoff} AND vwap <= 1
          AND {right_bin_sql} >= 0
        GROUP BY {right_bin_sql}
        ORDER BY 1, 2
    """).df()

    # Vectorized bin centers/labels for tails
    is_left = cl_tails['tail'] == 'left'
    lo = np.where(is_left,
                  cl_tails['bin_idx'] / n_bins_per_tail * left_cutoff,
                  right_cutoff + cl_tails['bin_idx'] / n_bins_per_tail * (1 - right_cutoff))
    hi = np.where(is_left,
                  (cl_tails['bin_idx'] + 1) / n_bins_per_tail * left_cutoff,
                  right_cutoff + (cl_tails['bin_idx'] + 1) / n_bins_per_tail * (1 - right_cutoff))
    cl_tails['bin_center'] = (lo + hi) / 2
    cl_tails['bin_label'] = [f"{l*100:.1f}-{h*100:.1f}%" for l, h in zip(lo, hi)]

    print(f"  Tail contracts: {int(cl_tails['count'].sum()):,}")

    # --- 3. By category ---
    cat_filter = ""
    if categories:
        cat_list = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND category IN ({cat_list})"

    cl_by_cat = con.execute(f"""
        SELECT category,
               FLOOR(vwap * {n_bins}) AS bin_idx,
               COUNT(*) AS count,
               AVG(vwap) AS mean_implied_prob,
               AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
               AVG(CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END) AS mean_return
        FROM _cl_batch_vwap
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
          AND FLOOR(vwap * {n_bins}) >= 0
        {cat_filter}
        GROUP BY category, FLOOR(vwap * {n_bins})
        ORDER BY category, 2
    """).df()

    cat_dist = cl_by_cat.groupby('category')['count'].sum().reset_index().sort_values('count', ascending=False)
    print("  Category distribution (contracts):")
    for _, row in cat_dist.iterrows():
        print(f"    {row['category']:20} {row['count']:>10,}")

    con.execute("DROP TABLE IF EXISTS _cl_batch_vwap")

    return cl_bins, cl_tails, cl_by_cat


def compute_favorite_longshot_bins_by_volume_tier(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    Contract-level FLB segmented by volume tier.

    For each (conditionId, outcome) pair, compute VWAP and total USDC volume.
    Assign a volume tier via thresholds, then bin by VWAP within each tier.

    Args:
        thresholds: list of cutpoints, e.g. [1000, 50000] creates 3 tiers
        labels: tier names, e.g. ['<$1K', '$1K–$50K', '>$50K'] (len = len(thresholds) + 1)

    Returns DataFrame with 'category' column (volume tier) so it can be
    passed directly to plot_favorite_longshot_by_category.
    """
    if thresholds is None:
        thresholds = [1000, 50000]
    if labels is None:
        labels = ['<$1K', '$1K–$50K', '>$50K']

    _ensure_closed_markets_registered(con, output_dir)

    print(f"Computing contract-level FLB by volume tier (thresholds={thresholds})...")

    # Build CASE WHEN for volume tiers
    case_parts = []
    for i, threshold in enumerate(thresholds):
        if i == 0:
            case_parts.append(f"WHEN total_volume < {threshold} THEN '{labels[i]}'")
        else:
            case_parts.append(f"WHEN total_volume < {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    volume_case_sql = "CASE " + " ".join(case_parts) + " END"

    # Reuse contract VWAP table if already built (cells 29/30/31 share this)
    _ensure_contract_vwap(con)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fl_vol_contracts_ret AS
        SELECT *,
            {volume_case_sql} AS volume_tier,
            CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END AS contract_return
        FROM fl_vol_contracts
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
    """)

    # Show tier distribution
    tier_dist = con.execute("""
        SELECT volume_tier, COUNT(*) AS n,
               AVG(total_volume) AS avg_vol,
               MEDIAN(total_volume) AS median_vol
        FROM fl_vol_contracts_ret
        GROUP BY volume_tier ORDER BY MIN(total_volume)
    """).df()
    print("  Volume tier distribution:")
    for _, row in tier_dist.iterrows():
        print(f"    {row['volume_tier']:20} {row['n']:>10,} contracts  "
              f"(avg ${row['avg_vol']:,.0f}, median ${row['median_vol']:,.0f})")

    agg = con.execute(f"""
        SELECT
            volume_tier AS category,
            FLOOR(vwap * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(vwap) AS mean_implied_prob,
            AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
            AVG(contract_return) AS mean_return
        FROM fl_vol_contracts_ret
        WHERE FLOOR(vwap * {n_bins}) >= 0
        GROUP BY volume_tier, FLOOR(vwap * {n_bins})
        ORDER BY volume_tier, 2
    """).df()

    con.execute("DROP TABLE IF EXISTS fl_vol_contracts_ret")

    return agg


def compute_favorite_longshot_bins_by_outcome_count_contract_level(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    Contract-level FLB segmented by the number of outcomes per event.

    One observation per (conditionId, outcome) using VWAP, grouped by
    whether the parent event is binary or multi-outcome.

    Returns DataFrame with 'category' column for plot_favorite_longshot_by_category.
    """
    if thresholds is None:
        thresholds = [2, 10]
    if labels is None:
        labels = ['Binary (1–2)', 'Small multi (3–10)', 'Large multi (10+)']

    _ensure_closed_markets_registered(con, output_dir)

    # Build CASE WHEN
    case_parts = []
    for i, threshold in enumerate(thresholds):
        case_parts.append(f"WHEN n_outcomes <= {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    case_sql = "CASE " + " ".join(case_parts) + " END"

    print(f"Computing contract-level FLB by outcome count (thresholds={thresholds})...")

    # Step 1: count distinct conditionIds per eventSlug
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_oc_event_lookup AS
        SELECT
            t.eventSlug,
            COUNT(DISTINCT t.conditionId) AS n_outcomes
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        GROUP BY t.eventSlug
    """)

    # Step 2: contract-level VWAP with outcome tier
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fl_oc_contracts AS
        SELECT
            {case_sql.replace('n_outcomes', 'oe.n_outcomes')} AS outcome_tier,
            t.conditionId,
            t.outcome,
            SUM(t.price * t.usdcSize) / NULLIF(SUM(t.usdcSize), 0) AS vwap,
            CASE WHEN t.outcome = c.winning_outcome THEN 1 ELSE 0 END AS won
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN fl_oc_event_lookup oe ON t.eventSlug = oe.eventSlug
        GROUP BY 1, t.conditionId, t.outcome, c.winning_outcome
    """)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE fl_oc_contracts_ret AS
        SELECT *,
            CASE WHEN won = 1 THEN (1.0 - vwap) ELSE (-vwap) END AS contract_return
        FROM fl_oc_contracts
        WHERE vwap IS NOT NULL AND vwap >= 0 AND vwap <= 1
    """)

    tier_dist = con.execute("""
        SELECT outcome_tier, COUNT(*) AS n
        FROM fl_oc_contracts_ret GROUP BY outcome_tier ORDER BY outcome_tier
    """).df()
    print("  Contract distribution by tier:")
    for _, row in tier_dist.iterrows():
        print(f"    {row['outcome_tier']:25} {row['n']:>10,} contract-outcome pairs")

    agg = con.execute(f"""
        SELECT
            outcome_tier AS category,
            FLOOR(vwap * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(vwap) AS mean_implied_prob,
            AVG(CAST(won AS DOUBLE)) AS empirical_win_rate,
            AVG(contract_return) AS mean_return
        FROM fl_oc_contracts_ret
        WHERE FLOOR(vwap * {n_bins}) >= 0
        GROUP BY outcome_tier, FLOOR(vwap * {n_bins})
        ORDER BY outcome_tier, 2
    """).df()

    for _t in ['fl_oc_event_lookup', 'fl_oc_contracts', 'fl_oc_contracts_ret']:
        con.execute(f"DROP TABLE IF EXISTS {_t}")

    return agg


# ---------------------------------------------------------------------------
# Snap-price ("closing line") analysis
# ---------------------------------------------------------------------------

def _load_closed_markets_with_end_ts(output_dir: Path) -> pd.DataFrame:
    """
    Load verified closed markets with end timestamp.
    Returns DataFrame with conditionId, winning_outcome, end_ts columns.

    Supports both old Gamma API format (with endDate) and new on-chain format.
    For on-chain format, end_ts is derived from the trades data (last trade time).
    """
    import json as _json

    resolutions_path = output_dir / "market_resolutions.parquet"
    df = pd.read_parquet(resolutions_path)
    df = _ensure_resolutions_have_condition_id(df)

    if 'outcomePrices' in df.columns:
        # Old Gamma API format
        valid_prices = df['outcomePrices'].dropna()
        valid_prices = valid_prices[valid_prices.astype(str).str.strip().astype(bool)]
        parsed_prices = valid_prices.map(_json.loads)
        df['price1'] = np.nan
        df['price2'] = np.nan
        df.loc[parsed_prices.index, 'price1'] = parsed_prices.str[0].astype(float)
        df.loc[parsed_prices.index, 'price2'] = parsed_prices.str[1].astype(float)

        valid_outcomes = df['outcomes'].dropna()
        valid_outcomes = valid_outcomes[valid_outcomes.astype(str).str.strip().astype(bool)]
        parsed_outcomes = valid_outcomes.map(_json.loads)
        df['outcome1'] = None
        df['outcome2'] = None
        df.loc[parsed_outcomes.index, 'outcome1'] = parsed_outcomes.str[0]
        df.loc[parsed_outcomes.index, 'outcome2'] = parsed_outcomes.str[1]

        closed = df[df['closed'] == True].copy()
        clean = closed[
            ((closed['price1'] <= 0.01) & (closed['price2'] >= 0.99)) |
            ((closed['price1'] >= 0.99) & (closed['price2'] <= 0.01))
        ].copy()

        clean['winning_outcome'] = np.where(
            clean['price1'] >= 0.99, clean['outcome1'], clean['outcome2']
        )

        clean['endDate_parsed'] = pd.to_datetime(clean['endDate'], errors='coerce', utc=True)
        clean['end_ts'] = (
            clean['endDate_parsed'].astype(np.int64) // 10**9
        )
        clean = clean[clean['endDate_parsed'].notna()].copy()
        return clean[['conditionId', 'winning_outcome', 'end_ts']]
    else:
        # New on-chain format: no endDate, so we need end_ts from trades
        # Return without end_ts — caller must derive from trades data
        result = df[['conditionId', 'winning_outcome']].drop_duplicates()
        result['end_ts'] = None  # will be filled by caller from trades
        return result


def compute_favorite_longshot_bins_snap_price(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    snap_pcts: list = None,
    window_seconds: int = 3600,
    min_lifetime_hours: float = 24,
    min_trades: int = 10,
    exclude_near_resolved: bool = True,
    near_resolved_threshold: float = 0.05,
) -> pd.DataFrame:
    """
    Contract-level FLB using "closing line" snap prices instead of VWAP.

    For each contract, defines lifetime = endDate - first_trade_timestamp.
    At each snap point (fraction of lifetime elapsed), takes the VWAP of
    trades in a window before that point as the "closing price."

    This avoids the VWAP bias where losing contracts' prices are anchored
    to the uncertain middle period because volume dries up near resolution.

    Args:
        snap_pcts: list of fractions of lifetime elapsed, e.g. [0.5, 0.7, 0.8, 0.9]
        window_seconds: VWAP window size in seconds (default 1 hour)
        min_lifetime_hours: exclude contracts shorter than this
        min_trades: exclude contracts with fewer total trades
        exclude_near_resolved: if True, drop contracts whose snap price is
            within near_resolved_threshold of 0 or 1
        near_resolved_threshold: threshold for "already resolved" (default 0.05)

    Returns DataFrame with 'category' column (snap label) for
    plot_favorite_longshot_by_category.
    """
    if snap_pcts is None:
        snap_pcts = [0.5, 0.7, 0.8, 0.9]

    markets = _load_closed_markets_with_end_ts(output_dir)

    # If end_ts is missing (on-chain format), derive from trades
    if markets['end_ts'].isna().all():
        print("  Deriving market end timestamps from last trade per conditionId...")
        end_ts_df = con.execute("""
            SELECT conditionId, MAX(timestamp) AS end_ts
            FROM trades_buy
            GROUP BY conditionId
        """).df()
        markets = markets.drop(columns=['end_ts']).merge(end_ts_df, on='conditionId', how='inner')

    con.register('snap_markets_df', markets)

    print(f"Computing snap-price FLB (thresholds={snap_pcts}, "
          f"window={window_seconds}s, min_life={min_lifetime_hours}h)...")

    # Build lifecycle table
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE snap_lifecycle AS
        SELECT
            t.conditionId,
            m.end_ts,
            m.winning_outcome,
            MIN(t.timestamp) AS first_trade_ts,
            COUNT(*) AS n_trades,
            (m.end_ts - MIN(t.timestamp)) / 3600.0 AS lifetime_hours
        FROM trades_buy t
        INNER JOIN snap_markets_df m ON t.conditionId = m.conditionId
        WHERE m.end_ts > 0
        GROUP BY t.conditionId, m.end_ts, m.winning_outcome
        HAVING COUNT(*) >= {min_trades}
          AND m.end_ts > MIN(t.timestamp)
          AND (m.end_ts - MIN(t.timestamp)) / 3600.0 >= {min_lifetime_hours}
    """)

    n_markets = con.execute(
        "SELECT COUNT(*) FROM snap_lifecycle"
    ).fetchone()[0]
    print(f"  Markets with {min_trades}+ trades and {min_lifetime_hours}h+ lifetime: "
          f"{n_markets:,}")

    # Build all snap points in a single trades scan using UNION ALL.
    # This replaces the per-snap-point loop that scanned trades N times
    # (plus N more for excluded counts = 2N total).
    snap_unions = []
    for pct in snap_pcts:
        remaining = round((1 - pct) * 100)
        label = f"{remaining}% remaining"
        snap_unions.append(f"""
            SELECT '{label}' AS snap_label,
                   t.conditionId, t.outcome, t.price, t.usdcSize,
                   CASE WHEN t.outcome = l.winning_outcome
                        THEN 1 ELSE 0 END AS won
            FROM trades_buy t
            INNER JOIN snap_lifecycle l ON t.conditionId = l.conditionId
            WHERE t.timestamp <= l.first_trade_ts
                    + (l.end_ts - l.first_trade_ts) * {pct}
              AND t.timestamp > l.first_trade_ts
                    + (l.end_ts - l.first_trade_ts) * {pct}
                    - {window_seconds}
        """)

    union_sql = "\nUNION ALL\n".join(snap_unions)

    # Single query: one trades scan, computes VWAPs + bins for all snap points
    agg = con.execute(f"""
        WITH all_snap_trades AS (
            {union_sql}
        ),
        snap_vwaps AS (
            SELECT snap_label, conditionId, outcome, won,
                   SUM(price * usdcSize)
                       / NULLIF(SUM(usdcSize), 0) AS snap_price
            FROM all_snap_trades
            GROUP BY snap_label, conditionId, outcome, won
        ),
        binned AS (
            SELECT snap_label AS category,
                   FLOOR(snap_price * {n_bins}) AS bin_idx,
                   snap_price, won,
                   -- flag near-resolved for counting
                   CASE WHEN snap_price < {near_resolved_threshold}
                             OR snap_price > {1 - near_resolved_threshold}
                        THEN 1 ELSE 0 END AS near_resolved
            FROM snap_vwaps
            WHERE snap_price >= 0 AND snap_price <= 1
              AND FLOOR(snap_price * {n_bins}) >= 0
        )
        SELECT category, bin_idx,
               COUNT(*) FILTER (WHERE {'near_resolved = 0' if exclude_near_resolved else 'TRUE'}) AS count,
               AVG(snap_price) FILTER (WHERE {'near_resolved = 0' if exclude_near_resolved else 'TRUE'}) AS mean_implied_prob,
               AVG(CAST(won AS DOUBLE)) FILTER (WHERE {'near_resolved = 0' if exclude_near_resolved else 'TRUE'}) AS empirical_win_rate,
               AVG(CASE WHEN won = 1 THEN (1.0 - snap_price) ELSE (-snap_price) END)
                   FILTER (WHERE {'near_resolved = 0' if exclude_near_resolved else 'TRUE'}) AS mean_return,
               COUNT(*) AS total_count
        FROM binned
        GROUP BY category, bin_idx
        ORDER BY category, bin_idx
    """).df()

    # Remove rows where the filtered count is zero
    agg = agg[agg['count'] > 0].copy()

    # Print summary per snap point
    for pct in snap_pcts:
        remaining = round((1 - pct) * 100)
        label = f"{remaining}% remaining"
        mask = agg['category'] == label
        n_contracts = int(agg.loc[mask, 'count'].sum())
        n_total = int(agg.loc[mask, 'total_count'].sum())
        n_excluded = n_total - n_contracts if exclude_near_resolved else 0
        print(f"  {label}: {n_contracts:,} contract-outcomes"
              f" ({n_excluded:,} near-resolved excluded)")

    agg = agg.drop(columns=['total_count'])

    con.execute("DROP TABLE IF EXISTS snap_lifecycle")
    try:
        con.unregister('snap_markets_df')
    except Exception:
        pass

    return agg


# ---------------------------------------------------------------------------
# Category helpers
# ---------------------------------------------------------------------------

# Keyword-based classifier: maps eventSlug patterns to categories.
# Order matters: first match wins.
_CATEGORY_RULES = [
    # Sports leagues / events
    ('Sports', [
        'nba-', 'nfl-', 'nhl-', 'mlb-', 'epl-', 'ucl-', 'uel-',
        'mls-', 'atp-', 'wta-', 'cbb-', 'cfb-', 'ligue-1',
        'serie-a', 'la-liga', 'bundesliga', 'super-bowl', 'nba-champion',
        'nba-mvp', 'premier-league', 'champions-league', 'world-cup',
        'olympics', 'mma-', 'ufc-', 'boxing-', 'cricket-', 'tennis-',
        'f1-', 'formula-1', 'nascar-', 'pga-', 'golf-', 'rugby-',
        'wrestle', '-vs-', 'bun-', 'cs2-', 'esports', 'valorant',
        'lol-', 'dota', 'match-between',
    ]),
    # Crypto / DeFi
    ('Crypto', [
        'bitcoin', 'btc-', 'ethereum', 'eth-', 'solana', 'sol-',
        'crypto', 'defi', 'nft', 'airdrop', 'token', 'dogecoin',
        'xrp-', 'cardano', 'polygon', 'avalanche', 'chainlink',
        'litecoin', 'stablecoin', 'memecoin', 'meme-coin',
        'satoshi', 'web3', 'blockchain',
    ]),
    # Politics / elections
    ('Politics', [
        'president', 'election', 'trump', 'biden', 'kamala',
        'republican', 'democrat', 'senate', 'congress', 'governor',
        'prime-minister', 'parliament', 'vote-', 'ballot',
        'inaugur', 'impeach', 'cabinet', 'political', 'party-',
        'midterm', 'electoral', 'nomination', 'nominee',
        'speaker-of', 'supreme-court', 'fed-chair', 'vp-pick',
        'maduro', 'zelensky', 'ukraine', 'russia-', 'nato',
        'china-', 'xi-jinping', 'north-korea',
    ]),
    # Finance / economics (non-crypto)
    ('Finance', [
        'stock', 's-p-500', 'sp500', 'nasdaq', 'dow-jones',
        'interest-rate', 'fed-rate', 'gdp-', 'inflation',
        'recession', 'unemployment', 'cpi-', 'fomc',
        'treasury', 'bond-', 'oil-price', 'gold-price',
        'housing', 'trade-war', 'tariff',
    ]),
    # Pop culture / entertainment
    ('Pop Culture', [
        'oscar', 'grammy', 'emmy', 'golden-globe', 'movie-',
        'album-', 'song-', 'spotify', 'netflix', 'disney',
        'youtube', 'tiktok', 'instagram', 'celebrity',
        'bachelor', 'survivor', 'reality-tv', 'eurovision',
        'music-', 'film-', 'box-office', 'award-',
    ]),
    # Science / Tech / AI
    ('Science & Tech', [
        'ai-', 'gpt-', 'openai', 'chatgpt', 'artificial-intelligence',
        'spacex', 'nasa', 'launch', 'mars-', 'moon-',
        'apple-', 'google-', 'microsoft-', 'meta-', 'amazon-',
        'fda-', 'vaccine', 'covid', 'coronavirus',
        'climate', 'earthquake', 'hurricane', 'weather',
        'lighter-', 'tech-',
    ]),
]


def _build_category_case_sql(table_alias: str = 't') -> str:
    """Build the CASE WHEN SQL for category classification from _CATEGORY_RULES."""
    case_parts = []
    for category, keywords in _CATEGORY_RULES:
        conditions = " OR ".join(
            f"LOWER({table_alias}.eventSlug) LIKE '%{kw}%'" for kw in keywords
        )
        case_parts.append(f"WHEN ({conditions}) THEN '{category}'")
    return "CASE " + " ".join(case_parts) + " ELSE 'Other' END"


# Pre-built for the common 't' alias — avoids rebuilding on every function call
_CATEGORY_CASE_SQL_T = _build_category_case_sql('t')


def classify_slug(slug: str) -> str:
    """Classify an eventSlug string into a market category."""
    if not slug:
        return 'Other'
    slug_lower = slug.lower()
    for category, keywords in _CATEGORY_RULES:
        for kw in keywords:
            if kw in slug_lower:
                return category
    return 'Other'


def get_market_categories(output_dir: Path) -> pd.DataFrame:
    """
    Get conditionId -> category. Uses market_resolutions.parquet if it has
    a 'category' column; otherwise tries market_categories.parquet; otherwise
    returns empty (caller can fetch from API).
    """
    # First try market_resolutions.parquet
    resolutions_path = output_dir / "market_resolutions.parquet"
    if resolutions_path.exists():
        df = pd.read_parquet(resolutions_path)
        df = _ensure_resolutions_have_condition_id(df)
        cols = [c for c in df.columns if c.lower() == 'category']
        id_col = 'conditionId'
        if cols:
            out = df[[id_col, cols[0]]].rename(columns={cols[0]: 'category'})
            return out.dropna(subset=['category'])

    # Try dedicated categories file
    cat_path = output_dir / "market_categories.parquet"
    if cat_path.exists():
        return pd.read_parquet(cat_path)

    return pd.DataFrame(columns=['conditionId', 'category'])


def fetch_and_save_market_categories(output_dir: Path) -> pd.DataFrame:
    """
    Fetch all markets from Gamma API (paginated), extract conditionId and category,
    save to market_categories.parquet. Returns DataFrame conditionId, category.
    """
    import requests
    import time

    base = "https://gamma-api.polymarket.com/markets"
    rows = []
    offset = 0
    page_size = 500

    print("Fetching market categories from Gamma API...")
    while True:
        r = requests.get(base, params={
            'limit': page_size,
            'offset': offset,
        }, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break

        for m in data:
            cid = m.get('conditionId') or m.get('condition_id')
            cat = m.get('category', None)
            if cid and cat:
                rows.append({'conditionId': cid, 'category': cat})

        print(f"  Fetched {offset + len(data)} markets...", end='\r')
        offset += page_size
        if len(data) < page_size:
            break
        time.sleep(0.1)  # gentle rate limit

    print(f"\n  Total markets with categories: {len(rows):,}")
    df = pd.DataFrame(rows).drop_duplicates(subset='conditionId')
    df.to_parquet(output_dir / "market_categories.parquet", index=False)
    print(f"  Saved to {output_dir / 'market_categories.parquet'}")
    return df


def compute_favorite_longshot_by_category(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    max_trades_per_category: int = 2_000_000,
    categories: Optional[list] = None,
) -> pd.DataFrame:
    """
    Compute FLB bins segmented by market category, using slug-based classification
    directly from the trades table (eventSlug column) for full coverage.
    Returns DataFrame with category column plus bin stats.
    """
    _ensure_closed_markets_registered(con, output_dir)

    case_sql = _CATEGORY_CASE_SQL_T

    # Optional: filter to requested categories (applied inside the CTE)
    cat_filter = ""
    if categories:
        cat_list = ", ".join(f"'{c}'" for c in categories)
        cat_filter = f"AND category IN ({cat_list})"

    # Classify and aggregate in one streaming pass — no intermediate temp tables.
    # The CTE computes the CASE WHEN expression once per row; DuckDB pipelines
    # the GROUP BY without materialising the 136M-row intermediate result.
    print("  Classifying and aggregating trades (streaming, no temp table)...")
    agg = con.execute(f"""
        WITH classified AS (
            SELECT
                {case_sql} AS category,
                t.price,
                CAST(t.outcome = c.winning_outcome AS DOUBLE) AS won,
                CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END AS trade_return
            FROM trades_buy t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            WHERE t.price >= 0 AND t.price <= 1
        )
        SELECT
            category,
            FLOOR(price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(price) AS mean_implied_prob,
            AVG(won) AS empirical_win_rate,
            AVG(trade_return) AS mean_return
        FROM classified
        WHERE FLOOR(price * {n_bins}) >= 0
        {cat_filter}
        GROUP BY category, FLOOR(price * {n_bins})
        ORDER BY category, 2
    """).df()

    cat_dist = (
        agg.groupby('category')['count'].sum()
        .reset_index()
        .sort_values('count', ascending=False)
    )
    print("  Category distribution (trades):")
    for _, row in cat_dist.iterrows():
        print(f"    {row['category']:20} {row['count']:>12,}")

    return agg


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_favorite_longshot(
    bins_df: pd.DataFrame,
    output_path: Optional[str] = None,
    figsize: Tuple[int, int] = (16, 5),
):
    """
    Three-panel figure:
      (1) Histogram of trade count by implied probability bin.
      (2) Calibration: implied prob (x) vs actual win rate (y), with dashed
          green diagonal and solid blue actual line.
      (3) Mispricing: implied prob (x) vs mean return (y), with fair line at 0.
    """
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=figsize)
    x = bins_df['mean_implied_prob']

    # --- Panel 1: histogram ---
    ax1 = axes[0]
    ax1.bar(x, bins_df['count'], width=0.045, color='steelblue', alpha=0.8,
            edgecolor='navy', linewidth=0.3)
    ax1.set_xlabel('Implied Probability', fontsize=10)
    ax1.set_ylabel('Trade Count', fontsize=10)
    ax1.set_title('Trade count by implied probability', fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)

    # --- Panel 2: calibration (implied vs actual win rate) ---
    ax2 = axes[1]
    ax2.plot([0, 1], [0, 1], '--', color='green', linewidth=2, label='Implied (fair)', zorder=1)
    ax2.plot(x, bins_df['empirical_win_rate'], '-o', color='royalblue',
             linewidth=2, markersize=5, label='Actual', zorder=2)
    ax2.set_xlabel('Contract Price (Implied Probability)', fontsize=10)
    ax2.set_ylabel('Actual Win Rate', fontsize=10)
    ax2.set_title('Implied probability vs actual win rate', fontweight='bold')
    ax2.legend(fontsize=9)
    ax2.set_xlim(0, 1)
    ax2.set_ylim(0, 1)
    ax2.set_aspect('equal')
    ax2.grid(alpha=0.3)

    # --- Panel 3: implied prob vs mean return ---
    ax3 = axes[2]
    colors = ['#d32f2f' if r < 0 else '#388e3c' for r in bins_df['mean_return']]
    ax3.bar(x, bins_df['mean_return'], width=0.045, color=colors, alpha=0.8,
            edgecolor='black', linewidth=0.3)
    ax3.axhline(0, color='gray', linestyle='--', linewidth=1)
    ax3.set_xlabel('Implied Probability', fontsize=10)
    ax3.set_ylabel('Mean Return (Actual − Implied)', fontsize=10)
    ax3.set_title('Implied probability vs actual return', fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)

    plt.suptitle(
        'Favorite-longshot bias across all trades (verified closed markets)',
        fontsize=12, fontweight='bold', y=1.02
    )
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"  Saved to {output_path}")

    return fig


def plot_favorite_longshot_tails(
    tails_df: pd.DataFrame,
    output_path: Optional[str] = None,
    figsize: Tuple[int, int] = (14, 10),
):
    """
    Two rows: left tail (longshots) and right tail (favorites). Each row has a
    histogram of trade counts and a bar chart of mean return per bin.
    """
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 2, figsize=figsize)

    # Determine cutoffs from the data
    left_sub = tails_df[tails_df['tail'] == 'left']
    right_sub = tails_df[tails_df['tail'] == 'right']
    left_max = left_sub['bin_center'].max() if len(left_sub) else 0.10
    right_min = right_sub['bin_center'].min() if len(right_sub) else 0.90

    for row, tail in enumerate(['left', 'right']):
        sub = tails_df[tails_df['tail'] == tail].copy()
        if sub.empty:
            continue

        # Compute bar width from actual bin spacing (80% of spacing to leave gaps)
        centers = sub['bin_center'].sort_values().values
        if len(centers) > 1:
            bar_w = (centers[1] - centers[0]) * 0.85
        else:
            bar_w = 0.005

        pct = int(round(left_max * 100)) if tail == 'left' else int(round(right_min * 100))
        tail_label = (
            f'Longshots: implied < {pct}%'
            if tail == 'left'
            else f'Favorites: implied > {pct}%'
        )

        # --- Column 1: histogram of trade counts ---
        ax1 = axes[row, 0]
        ax1.bar(sub['bin_center'], sub['count'], width=bar_w,
                color='steelblue', alpha=0.9, edgecolor='black', linewidth=0.7)
        ax1.set_xlabel('Implied Probability', fontsize=10)
        ax1.set_ylabel('Trade Count', fontsize=10)
        ax1.set_title(f'{tail_label} — trade counts', fontweight='bold', fontsize=10)
        ax1.grid(axis='y', alpha=0.3)

        # --- Column 2: return per bin ---
        ax2 = axes[row, 1]
        colors = ['#d32f2f' if r < 0 else '#388e3c' for r in sub['mean_return']]
        ax2.bar(sub['bin_center'], sub['mean_return'], width=bar_w,
                color=colors, alpha=0.9, edgecolor='black', linewidth=0.7)
        ax2.axhline(0, color='gray', linestyle='--', linewidth=1)
        ax2.set_xlabel('Implied Probability', fontsize=10)
        ax2.set_ylabel('Mean Return', fontsize=10)
        ax2.set_title(f'{tail_label} — mean return per bin', fontweight='bold', fontsize=10)
        ax2.grid(axis='y', alpha=0.3)

    plt.suptitle(
        'Favorite-longshot bias: extreme tails',
        fontsize=12, fontweight='bold', y=1.02
    )
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"  Saved to {output_path}")

    return fig


def plot_favorite_longshot_by_category(
    by_cat_df: pd.DataFrame,
    output_path: Optional[str] = None,
    max_categories: int = 6,
):
    """
    One calibration plot per category (implied vs actual with diagonal).
    Uses up to max_categories with most trades.
    """
    import matplotlib.pyplot as plt

    # Pick top categories by trade count, excluding "Other" (uninformative catch-all)
    cat_counts = by_cat_df[by_cat_df['category'] != 'Other'].groupby('category')['count'].sum()
    top_cats = cat_counts.nlargest(max_categories).index.tolist()

    n_plots = len(top_cats)
    ncol = min(3, n_plots)
    nrow = (n_plots + ncol - 1) // ncol
    fig, axes = plt.subplots(nrow, ncol, figsize=(5 * ncol, 4 * nrow))
    if n_plots == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for i, cat in enumerate(top_cats):
        ax = axes[i]
        sub = by_cat_df[by_cat_df['category'] == cat].copy()
        x = sub['mean_implied_prob']

        # Diagonal
        ax.plot([0, 1], [0, 1], '--', color='green', linewidth=1.5,
                label='Implied (fair)', zorder=1)
        # Actual win rate
        ax.plot(x, sub['empirical_win_rate'], '-o', color='royalblue',
                linewidth=2, markersize=4, label='Actual', zorder=2)

        ax.set_title(cat, fontweight='bold', fontsize=11)
        ax.set_xlabel('Implied Probability')
        ax.set_ylabel('Actual Win Rate')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_aspect('equal')
        ax.grid(alpha=0.3)
        n_trades = sub['count'].sum()
        ax.text(0.05, 0.92, f'n = {n_trades:,.0f}', transform=ax.transAxes,
                fontsize=8, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        if i == 0:
            ax.legend(fontsize=8, loc='lower right')

    # Hide unused axes
    for j in range(i + 1, len(axes)):
        axes[j].set_visible(False)

    plt.suptitle(
        'Favorite-longshot bias by market category',
        fontsize=12, fontweight='bold', y=1.02
    )
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"  Saved to {output_path}")

    return fig


# ---------------------------------------------------------------------------
# Trader typology: who drives the FLB?
# ---------------------------------------------------------------------------

def compute_flb_by_trader_volume_tier(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    Trade-level FLB segmented by each *trader's* lifetime trading volume.

    Each trader is classified by their total USDC volume across all trades
    (a proxy for sophistication). We then compute the FLB calibration curve
    for trades made by each tier, testing whether less sophisticated
    (low-volume) traders exhibit stronger favorite-longshot bias.

    Args:
        thresholds: cutpoints on per-trader total volume, e.g. [1000, 10000, 100000]
        labels: tier names (len = len(thresholds) + 1)

    Returns DataFrame with 'category' column (tier label) compatible with
    plot_favorite_longshot_by_category.
    """
    if thresholds is None:
        thresholds = [1_000, 10_000, 100_000]
    if labels is None:
        labels = ['Retail (<$1K)', 'Small ($1K-$10K)', 'Medium ($10K-$100K)', 'Whale (>$100K)']

    _ensure_closed_markets_registered(con, output_dir)

    # Build CASE WHEN for volume tiers
    case_parts = []
    for i, threshold in enumerate(thresholds):
        case_parts.append(f"WHEN trader_vol < {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    volume_case_sql = "CASE " + " ".join(case_parts) + " END"

    print(f"Computing FLB by TRADER volume tier (thresholds={thresholds})...")

    # Step 1: per-trader total volume (reuse if already computed)
    _ensure_trader_vol_lookup(con)

    # Step 2: join trades to closed markets + trader volume, bin by implied prob
    agg = con.execute(f"""
        SELECT
            {volume_case_sql.replace('trader_vol', 'tv.trader_vol')} AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN fl_tv_lookup tv ON t.proxyWallet = tv.proxyWallet
        WHERE t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
        GROUP BY category, FLOOR(t.price * {n_bins})
        ORDER BY category, 2
    """).df()

    # Show tier distribution
    tier_dist = agg.groupby('category')[['count']].sum().reset_index()
    n_total = tier_dist['count'].sum()
    print("  Trader tier distribution (trades on closed markets):")
    for _, row in tier_dist.iterrows():
        print(f"    {row['category']:30} {row['count']:>12,} trades ({row['count']/n_total*100:.1f}%)")

    return agg


def compute_trader_flb_attribution(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 10,
    thresholds: list = None,
    labels: list = None,
) -> pd.DataFrame:
    """
    For each probability bin, compute what fraction of trades and dollar volume
    came from each trader volume tier.

    Returns a long-form DataFrame with columns:
        bin_idx, bin_label, tier, n_trades, vol_usdc, pct_trades, pct_vol
    Ready for stacked bar chart visualization.
    """
    if thresholds is None:
        thresholds = [1_000, 10_000, 100_000]
    if labels is None:
        labels = ['Retail (<$1K)', 'Small ($1K-$10K)', 'Medium ($10K-$100K)', 'Whale (>$100K)']

    _ensure_closed_markets_registered(con, output_dir)

    case_parts = []
    for i, threshold in enumerate(thresholds):
        case_parts.append(f"WHEN trader_vol < {threshold} THEN '{labels[i]}'")
    case_parts.append(f"ELSE '{labels[-1]}'")
    volume_case_sql = "CASE " + " ".join(case_parts) + " END"

    print(f"Computing trader FLB attribution (n_bins={n_bins})...")

    _ensure_trader_vol_lookup(con)

    raw = con.execute(f"""
        SELECT
            {volume_case_sql.replace('trader_vol', 'tv.trader_vol')} AS tier,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS n_trades,
            SUM(t.usdcSize) AS vol_usdc
        FROM trades_buy t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN fl_tv_lookup tv ON t.proxyWallet = tv.proxyWallet
        WHERE t.price >= 0.05 AND t.price <= 0.95
          AND FLOOR(t.price * {n_bins}) >= 0
        GROUP BY tier, FLOOR(t.price * {n_bins})
        ORDER BY tier, 2
    """).df()

    # Compute pct within each bin (vectorized)
    bin_totals = raw.groupby('bin_idx').agg(
        total_trades=('n_trades', 'sum'), total_vol=('vol_usdc', 'sum')
    )
    raw = raw.merge(bin_totals, on='bin_idx', how='left')
    raw['pct_trades'] = raw['n_trades'] / raw['total_trades'] * 100
    raw['pct_vol'] = raw['vol_usdc'] / raw['total_vol'] * 100
    raw = raw.drop(columns=['total_trades', 'total_vol'])

    # Add human-readable bin label
    raw['bin_label'] = raw['bin_idx'].apply(
        lambda i: f"{int(i/n_bins*100)}-{int((i+1)/n_bins*100)}%"
    )

    print(f"  Attribution computed for {raw['bin_idx'].nunique()} bins x {raw['tier'].nunique()} tiers")
    return raw


def plot_trader_flb_attribution(
    attr_df: pd.DataFrame,
    tier_order: list = None,
    tier_colors: list = None,
    output_path: Optional[str] = None,
    figsize: tuple = (14, 6),
):
    """
    Stacked bar chart showing, for each probability bin, what fraction of
    trades (left panel) and dollar volume (right panel) came from each
    trader volume tier.
    """
    import matplotlib.pyplot as plt

    if tier_order is None:
        tier_order = sorted(attr_df['tier'].unique())
    if tier_colors is None:
        tier_colors = ['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4']

    bins = sorted(attr_df['bin_idx'].unique())
    bin_labels = [attr_df[attr_df['bin_idx'] == b]['bin_label'].iloc[0] for b in bins]

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    for ax, metric, ylabel, title in [
        (axes[0], 'pct_trades', '% of Trades', 'Trade Count Composition by Trader Tier'),
        (axes[1], 'pct_vol', '% of Dollar Volume', 'Dollar Volume Composition by Trader Tier'),
    ]:
        bottoms = np.zeros(len(bins))
        for tier, color in zip(tier_order, tier_colors):
            heights = []
            for b in bins:
                row = attr_df[(attr_df['bin_idx'] == b) & (attr_df['tier'] == tier)]
                heights.append(row[metric].iloc[0] if len(row) > 0 else 0.0)
            heights = np.array(heights)
            ax.bar(range(len(bins)), heights, bottom=bottoms,
                   color=color, label=tier, edgecolor='white', linewidth=0.5)
            bottoms += heights

        ax.set_xticks(range(len(bins)))
        ax.set_xticklabels(bin_labels, rotation=45, ha='right', fontsize=8)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(title, fontweight='bold', fontsize=10)
        ax.set_ylim(0, 105)
        ax.axhline(100, color='gray', linewidth=0.5, linestyle='--')
        ax.grid(axis='y', alpha=0.3)

    # Shared legend on right panel
    axes[1].legend(loc='upper right', fontsize=8, title='Trader tier', title_fontsize=8)

    plt.suptitle(
        'Who trades at each probability level? Composition by trader lifetime volume',
        fontsize=11, fontweight='bold', y=1.02
    )
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"  Saved to {output_path}")

    return fig
