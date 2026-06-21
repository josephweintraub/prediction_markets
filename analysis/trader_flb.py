"""
Trader Profiling and Segment-Level FLB Analysis (Section 5.11)

Functions:
  - compute_all_trader_profiles: one-pass aggregation of per-trader behavioral features
  - compute_category_specialization: dominant market category + HHI per trader
  - compute_flb_by_segment: generic FLB calibration by any trader segment column
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from favorite_longshot import (
    _ensure_closed_markets_registered,
    _ensure_mkt_lifecycle,
    load_verified_closed_markets,
)


def _ensure_tc_counts(con):
    """Create _tc_counts (trade count per trader-contract pair) if not exists."""
    try:
        con.execute("SELECT 1 FROM _tc_counts LIMIT 1")
        return
    except Exception:
        pass
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _tc_counts AS
        SELECT proxyWallet, conditionId, COUNT(*) AS n_tc
        FROM trades
        GROUP BY proxyWallet, conditionId
    """)


def _ensure_tc_buy_stats(con):
    """Pre-aggregate BUY trades per (trader, contract). One trades scan.

    Creates _tc_buy_stats with:
        proxyWallet, conditionId, n_buys, first_buy_ts,
        first_buy_price, first_buy_outcome, avg_buy_price, total_buy_vol

    This table is ~30M rows (one per BUY-side position) and replaces
    the need for multiple inline CTEs (min_ts, tc_counts) plus multi-way
    joins that cause OOM on large datasets.
    """
    try:
        con.execute("SELECT 1 FROM _tc_buy_stats LIMIT 1")
        return
    except Exception:
        pass
    print("Building _tc_buy_stats (single trades scan)...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _tc_buy_stats AS
        SELECT proxyWallet, conditionId,
               COUNT(*) AS n_buys,
               MIN(timestamp) AS first_buy_ts,
               ARG_MIN(price, timestamp) AS first_buy_price,
               ARG_MIN(outcome, timestamp) AS first_buy_outcome,
               AVG(price) AS avg_buy_price,
               SUM(usdcSize) AS total_buy_vol
        FROM trades
        WHERE side = 'BUY'
        GROUP BY proxyWallet, conditionId
    """)
    n = con.execute("SELECT COUNT(*) FROM _tc_buy_stats").fetchone()[0]
    print(f"  _tc_buy_stats ready: {n:,} BUY-side positions.")


def compute_flb_by_segments_batch(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    trader_df: pd.DataFrame,
    segment_cols: list,
    n_bins: int = 20,
) -> dict:
    """
    Compute FLB for multiple segment columns in a SINGLE trades scan.
    Returns dict of {segment_col: DataFrame}.

    Much faster than calling compute_flb_by_segment N times (N scans → 1 scan).
    """
    _ensure_closed_markets_registered(con, output_dir)

    # Build combined segment mapping with all columns
    cols_needed = ['trader'] + segment_cols
    seg_df = trader_df[cols_needed].rename(columns={'trader': 'proxyWallet'}).copy()
    for col in segment_cols:
        seg_df[col] = seg_df[col].astype(str)
    con.register('_trader_seg_batch', seg_df)

    # Build UNION ALL of one SELECT per segment column — single trades scan
    # Includes both BUY and SELL, with SELL converted to buy-equivalent perspective
    selects = []
    for col in segment_cols:
        selects.append(f"""
            SELECT '{col}' AS dim, ts."{col}" AS category,
                   FLOOR((CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) * {n_bins}) AS bin_idx,
                   CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END AS implied_prob,
                   t.side, t.outcome, c.winning_outcome
            FROM trades t
            INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
            INNER JOIN _trader_seg_batch ts ON t.proxyWallet = ts.proxyWallet
            WHERE (CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) BETWEEN 0.0 AND 1.0
              AND FLOOR((CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) * {n_bins}) >= 0
              AND ts."{col}" != 'nan'
        """)

    union_sql = "\nUNION ALL\n".join(selects)

    print(f"Computing FLB for {len(segment_cols)} segments in single scan (BUY+SELL)...")
    agg = con.execute(f"""
        WITH all_segs AS ({union_sql})
        SELECT dim, category, bin_idx,
               COUNT(*) AS count,
               AVG(implied_prob) AS mean_implied_prob,
               AVG(CASE WHEN side = 'BUY' AND outcome = winning_outcome THEN 1.0
                        WHEN side = 'SELL' AND outcome != winning_outcome THEN 1.0
                        ELSE 0.0 END) AS empirical_win_rate,
               AVG(CASE WHEN side = 'BUY' AND outcome = winning_outcome THEN 1.0 - implied_prob
                        WHEN side = 'BUY' THEN -implied_prob
                        WHEN side = 'SELL' AND outcome != winning_outcome THEN 1.0 - implied_prob
                        ELSE -implied_prob END) AS mean_return
        FROM all_segs
        GROUP BY dim, category, bin_idx
        ORDER BY dim, category, bin_idx
    """).df()

    con.unregister('_trader_seg_batch')

    results = {}
    for col in segment_cols:
        df = agg[agg['dim'] == col].drop(columns=['dim']).reset_index(drop=True)
        results[col] = df
        dist = df.groupby('category')['count'].sum().reset_index().sort_values('count', ascending=False)
        n_total = dist['count'].sum()
        print(f"  {col}: {n_total:,} trades across {len(dist)} segments")
        for _, row in dist.iterrows():
            print(f"    {str(row['category']):40} {row['count']:>12,} ({row['count']/n_total*100:.1f}%)")

    return results


def _ensure_tc_profile_agg(con: duckdb.DuckDBPyConnection) -> None:
    """Pre-aggregate trades to (trader, conditionId) level. ONE trades scan.

    Creates _tc_profile_agg (~25M rows) with per-position stats + category.
    This replaces COUNT(DISTINCT) and avoids multiple 136M-row scans.
    All downstream profile/category queries operate on this 25M-row table.
    """
    try:
        con.execute("SELECT 1 FROM _tc_profile_agg LIMIT 1")
        return
    except Exception:
        pass

    from favorite_longshot import _CATEGORY_CASE_SQL_T
    case_sql = _CATEGORY_CASE_SQL_T

    print("Pre-aggregating trades to (trader, contract) level (~3 min)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tc_profile_agg AS
        SELECT
            t.proxyWallet,
            t.conditionId,
            {case_sql} AS category,
            COUNT(*) AS n_trades,
            SUM(t.usdcSize) AS total_vol,
            SUM(CASE WHEN t.side = 'BUY' THEN 1 ELSE 0 END) AS n_buys,
            SUM(t.usdcSize * CASE WHEN t.side = 'BUY'
                                  THEN t.price ELSE 1.0 - t.price END) AS vol_wt_sum,
            MIN(t.timestamp) AS first_ts
        FROM trades t
        GROUP BY t.proxyWallet, t.conditionId, category
    """)
    n = con.execute("SELECT COUNT(*) FROM _tc_profile_agg").fetchone()[0]
    print(f"  _tc_profile_agg ready: {n:,} positions")


def compute_all_trader_profiles(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Per-trader behavioral profiles derived from pre-aggregated position table.
    No COUNT(DISTINCT), no large joins on 136M rows.

    Returns one row per trader with columns:
        trader, n_trades, n_markets, total_vol, frac_buys, trades_per_market,
        vol_wt_prob, avg_market_position, buyer_type, freq_tier, timing_type, ptype
    """
    _ensure_tc_profile_agg(con)
    _ensure_mkt_lifecycle(con)

    print("Deriving per-trader profiles from pre-aggregated table...")
    profiles = con.execute("""
        SELECT
            a.proxyWallet AS trader,
            SUM(a.n_trades) AS n_trades,
            COUNT(*) AS n_markets,
            SUM(a.total_vol) AS total_vol,
            SUM(a.n_buys) * 1.0 / SUM(a.n_trades) AS frac_buys,
            SUM(a.vol_wt_sum) / NULLIF(SUM(a.total_vol), 0) AS vol_wt_prob,
            AVG((a.first_ts - m.mkt_start) * 1.0 / m.mkt_duration) AS avg_market_position
        FROM _tc_profile_agg a
        LEFT JOIN _mkt_lifecycle m ON a.conditionId = m.conditionId
        GROUP BY a.proxyWallet
    """).df()

    print(f"  Traders profiled: {len(profiles):,}")

    # Derived columns (thresholds from data)
    profiles['trades_per_market'] = profiles['n_trades'] / profiles['n_markets']

    profiles['buyer_type'] = np.where(
        profiles['frac_buys'] >= 0.95, 'Pure Buyer', 'Two-Sided'
    )

    log_tpm = np.log10(profiles['trades_per_market'].clip(lower=1.0))
    tpm_cuts = np.unique(np.percentile(log_tpm.dropna(), [50, 90, 99]))
    freq_labels = ['Casual', 'Regular', 'Active', 'Heavy'][: len(tpm_cuts) + 1]
    profiles['freq_tier'] = pd.cut(
        log_tpm,
        bins=[-np.inf] + list(tpm_cuts) + [np.inf],
        labels=freq_labels,
        include_lowest=True,
    )
    print(f"  freq_tier cuts (trades/contract): "
          + " | ".join(f"{l}<={10**t:.1f}" for l, t in zip(freq_labels, tpm_cuts)))

    p25_t, p75_t = np.percentile(profiles['avg_market_position'].dropna(), [25, 75])
    profiles['timing_type'] = pd.cut(
        profiles['avg_market_position'],
        bins=[-np.inf, p25_t, p75_t, np.inf],
        labels=['Early Bird', 'Balanced', 'Late Joiner'],
        include_lowest=True,
    )
    print(f"  timing_type cuts (avg_market_position): p25={p25_t:.3f}, p75={p75_t:.3f}")

    p25_p, p75_p = np.percentile(profiles['vol_wt_prob'].dropna(), [25, 75])
    profiles['ptype'] = pd.cut(
        profiles['vol_wt_prob'],
        bins=[-np.inf, p25_p, p75_p, np.inf],
        labels=['Longshot Lover', 'Balanced', 'Favorite Chaser'],
        include_lowest=True,
    )
    print(f"  ptype cuts (vol_wt_prob): p25={p25_p:.3f}, p75={p75_p:.3f}")

    # Note: _tc_profile_agg kept alive for compute_category_specialization().
    # Caller should drop it after both functions have run.

    return profiles


def compute_category_specialization(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Dominant category + HHI per trader, derived from _tc_profile_agg.
    No trades scan — reads pre-aggregated table only.

    Returns DataFrame: trader, top_category, category_hhi, specialization.
    """
    _ensure_tc_profile_agg(con)

    print("Computing category specialization from pre-aggregated table...")
    cat_df = con.execute("""
        WITH per_cat AS (
            SELECT proxyWallet, category, SUM(n_trades) AS cat_trades
            FROM _tc_profile_agg
            GROUP BY proxyWallet, category
        )
        SELECT
            proxyWallet AS trader,
            arg_max(category, cat_trades) AS top_category,
            SUM(CAST(cat_trades AS DOUBLE) * CAST(cat_trades AS DOUBLE))
                / POWER(SUM(CAST(cat_trades AS DOUBLE)), 2) AS category_hhi
        FROM per_cat
        GROUP BY proxyWallet
    """).df()

    print(f"  Traders with category data: {len(cat_df):,}")

    cat_df['specialization'] = np.where(
        cat_df['category_hhi'] > 0.49,
        cat_df['top_category'] + ' Specialist',
        'Generalist',
    )

    # Drop _tc_profile_agg now that both profile functions have consumed it
    con.execute("DROP TABLE IF EXISTS _tc_profile_agg")

    return cat_df


def compute_flb_by_segment(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    trader_df: pd.DataFrame,
    segment_col: str,
    n_bins: int = 20,
) -> pd.DataFrame:
    """
    Generic FLB calibration for any trader segment.

    Args:
        trader_df:   DataFrame with 'trader' column (proxyWallet) and segment_col.
                     Pass a filtered subset to restrict (e.g., discretionary-only).
        segment_col: column in trader_df to segment by.
        n_bins:      number of probability bins.

    Returns DataFrame with 'category' column (= segment value) directly
    compatible with plot_favorite_longshot_by_category.
    """
    _ensure_closed_markets_registered(con, output_dir)

    # Build the segment mapping table
    seg_df = (
        trader_df[['trader', segment_col]]
        .rename(columns={'trader': 'proxyWallet', segment_col: 'segment'})
        .dropna(subset=['segment'])
        .copy()
    )
    seg_df['segment'] = seg_df['segment'].astype(str)
    con.register('_trader_seg_df', seg_df)

    n_segs = seg_df['segment'].nunique()
    print(f"Computing FLB by segment '{segment_col}' "
          f"({n_segs} segments, {len(seg_df):,} traders)...")

    agg = con.execute(f"""
        SELECT
            ts.segment AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _trader_seg_df ts ON t.proxyWallet = ts.proxyWallet
        WHERE t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
          AND t.side = 'BUY'
        GROUP BY ts.segment, FLOOR(t.price * {n_bins})
        ORDER BY ts.segment, 2
    """).df()

    # Distribution summary
    dist = (
        agg.groupby('category')['count'].sum()
        .reset_index()
        .sort_values('count', ascending=False)
    )
    n_total = dist['count'].sum()
    print(f"  Trades matched: {n_total:,}")
    for _, row in dist.iterrows():
        print(f"    {str(row['category']):40} {row['count']:>12,} trades "
              f"({row['count'] / n_total * 100:.1f}%)")

    return agg


def compute_flb_by_contract_intensity(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    n_bins: int = 20,
) -> pd.DataFrame:
    """
    FLB calibration segmented by per-(trader, contract) trade count.

    Unlike compute_flb_by_segment (which uses trader-level averages like
    trades_per_market), this annotates each individual trade with how many
    times that specific trader traded that specific contract, then bins FLB
    by that count.

    Thresholds are percentile-based (p50/p90/p99 on log scale of the
    (trader, contract) trade count distribution) so they adapt to dataset size.

    Returns DataFrame compatible with plot_favorite_longshot_by_category.
    """
    _ensure_closed_markets_registered(con, output_dir)

    # Materialize tc_counts once (reused for thresholds and main query)
    print("Computing per-(trader, contract) trade counts...")
    _ensure_tc_counts(con)

    # Compute log-percentile thresholds in SQL — avoids fetching 25M rows to Python
    _pct = con.execute("""
        SELECT
            POWER(10, percentile_cont(0.50) WITHIN GROUP
                  (ORDER BY LOG10(GREATEST(n_tc, 1)))) AS p50_raw,
            POWER(10, percentile_cont(0.90) WITHIN GROUP
                  (ORDER BY LOG10(GREATEST(n_tc, 1)))) AS p90_raw,
            POWER(10, percentile_cont(0.99) WITHIN GROUP
                  (ORDER BY LOG10(GREATEST(n_tc, 1)))) AS p99_raw
        FROM _tc_counts
    """).fetchone()
    t50 = max(2, int(round(_pct[0])))
    t90 = max(t50 + 1, int(round(_pct[1])))
    t99 = max(t90 + 1, int(round(_pct[2])))
    print(f"  contract intensity thresholds: single=1 | occasional<={t50} "
          f"| repeat<={t90} | active<={t99} | concentrated>{t99}")

    # Pre-filter _tc_counts to closed-market contracts only — reduces the join
    # hash table from ~25M rows to ~10-12M, cutting peak join memory by ~50%
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _tc_counts_closed AS
        SELECT tc.proxyWallet, tc.conditionId, tc.n_tc
        FROM _tc_counts tc
        WHERE tc.conditionId IN (SELECT conditionId FROM _closed_markets)
    """)
    n_closed = con.execute("SELECT COUNT(*) FROM _tc_counts_closed").fetchone()[0]
    print(f"  Hash table (closed-market pairs): {n_closed:,} rows")

    agg = con.execute(f"""
        SELECT
            CASE
                WHEN tc.n_tc = 1          THEN '1. Single'
                WHEN tc.n_tc <= {t50}     THEN '2. Occasional'
                WHEN tc.n_tc <= {t90}     THEN '3. Repeat'
                WHEN tc.n_tc <= {t99}     THEN '4. Active'
                ELSE                           '5. Concentrated'
            END AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _tc_counts_closed tc
            ON t.proxyWallet = tc.proxyWallet AND t.conditionId = tc.conditionId
        WHERE t.side = 'BUY'
          AND t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
        GROUP BY 1, FLOOR(t.price * {n_bins})
        ORDER BY 1, 2
    """).df()

    con.execute("DROP TABLE IF EXISTS _tc_counts_closed")
    # _tc_counts is not used downstream — free it now (~3 GB) to avoid OOM
    # in the next cell's large aggregate (_pos_buy_agg / _tc_buy_stats).
    con.execute("DROP TABLE IF EXISTS _tc_counts")

    dist = agg.groupby('category')['count'].sum().reset_index().sort_values('category')
    n_total = dist['count'].sum()
    print(f"  Trades matched: {n_total:,}")
    for _, row in dist.iterrows():
        print(f"    {row['category']:30} {row['count']:>12,} trades "
              f"({row['count'] / n_total * 100:.1f}%)")

    return agg


def build_experience_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    One-time setup: materialize global and within-category experience for all
    (trader, contract) pairs in a single pass over trades.

    Creates _exp_all with columns:
        proxyWallet, conditionId, category,
        n_prior_global       — # distinct contracts across ALL categories
                               traded before this one (per trader)
        n_prior_within_cat   — # distinct contracts in THIS category only
                               traded before this one (per trader)

    Call once before compute_flb_by_experience to avoid repeated full scans.
    Subsequent calls to compute_flb_by_experience auto-detect the table.
    """
    from favorite_longshot import _CATEGORY_CASE_SQL_T
    case_sql = _CATEGORY_CASE_SQL_T

    print("Building experience lookup table (1 pass over trades)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tc_first_all AS
        SELECT
            t.proxyWallet,
            t.conditionId,
            {case_sql} AS category,
            MIN(t.timestamp) AS first_ts
        FROM trades t
        GROUP BY t.proxyWallet, t.conditionId, category
    """)

    # Two window functions in one statement = two simultaneous sorts → OOM.
    # Split into separate steps, dropping each intermediate to free temp before next.

    # Step 2: n_prior_global (sort by proxyWallet + first_ts)
    print("  Computing n_prior_global...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _exp_step1 AS
        SELECT
            proxyWallet,
            conditionId,
            category,
            first_ts,
            (ROW_NUMBER() OVER (
                PARTITION BY proxyWallet
                ORDER BY first_ts
            ) - 1) AS n_prior_global
        FROM _tc_first_all
    """)
    con.execute("DROP TABLE IF EXISTS _tc_first_all")

    # Step 3: n_prior_within_cat (sort by proxyWallet + category + first_ts)
    print("  Computing n_prior_within_cat...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _exp_all AS
        SELECT
            proxyWallet,
            conditionId,
            category,
            first_ts,
            n_prior_global,
            (ROW_NUMBER() OVER (
                PARTITION BY proxyWallet, category
                ORDER BY first_ts
            ) - 1) AS n_prior_within_cat
        FROM _exp_step1
    """)
    con.execute("DROP TABLE IF EXISTS _exp_step1")

    n = con.execute("SELECT COUNT(*) FROM _exp_all").fetchone()[0]
    print(f"  _exp_all ready: {n:,} (trader, contract) pairs.")


def compute_flb_by_experience(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    category_name: str = None,
    category_label: str = None,
    n_bins: int = 20,
) -> pd.DataFrame:
    """
    FLB calibration segmented by trader experience at the time of each trade.

    Experience is defined as: the number of distinct contracts a trader had
    participated in BEFORE their first trade on the current contract.

    Args:
        category_name: None → global experience (all categories combined).
                       'Sports', 'Crypto', etc. → within-category experience
                       (only prior contracts in that category count).
        category_label: display label; defaults to category_name or 'global'.

    Thresholds are percentile-based (p50/p90 on log scale) per call.

    Returns DataFrame compatible with plot_favorite_longshot_by_category.

    Performance: calls build_experience_tables(con) automatically on first use
    to materialize _exp_all. Subsequent calls read from that table — no
    repeated full scans of trades.
    """
    if category_label is None:
        category_label = category_name or 'global'

    # Auto-build experience tables on first call
    try:
        con.execute("SELECT 1 FROM _exp_all LIMIT 1")
    except Exception:
        build_experience_tables(con)

    _ensure_closed_markets_registered(con, output_dir)

    if category_name is None:
        exp_col = 'n_prior_global'
        cat_filter = ''
    else:
        exp_col = 'n_prior_within_cat'
        cat_filter = f"AND e.category = '{category_name}'"

    print(f"Building experience table [{category_label}] ...")

    # Thresholds from _exp_all — no trades scan needed
    exp_dist = con.execute(f"""
        SELECT {exp_col} AS n_prior
        FROM _exp_all e
        WHERE 1=1 {cat_filter}
    """).df()

    n_first = (exp_dist['n_prior'] == 0).sum()
    print(f"  First-timers (n_prior=0): {n_first:,} "
          f"({n_first / len(exp_dist) * 100:.1f}% of positions)")

    nz = exp_dist.loc[exp_dist['n_prior'] > 0, 'n_prior']
    log_nz = np.log10(nz.clip(lower=1))
    p50 = max(1, int(round(10 ** np.percentile(log_nz, 50))))
    p90 = max(p50 + 1, int(round(10 ** np.percentile(log_nz, 90))))
    print(f"  Experience thresholds: novice<={p50} | experienced<={p90} | veteran>{p90}")

    # FLB aggregation — single scan of trades, join to pre-built _exp_all
    agg = con.execute(f"""
        SELECT
            CASE
                WHEN e.{exp_col} = 0        THEN '1. First-Timer'
                WHEN e.{exp_col} <= {p50}   THEN '2. Novice'
                WHEN e.{exp_col} <= {p90}   THEN '3. Experienced'
                ELSE                             '4. Veteran'
            END AS category,
            FLOOR(t.price * {n_bins}) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _exp_all e
            ON t.proxyWallet = e.proxyWallet AND t.conditionId = e.conditionId
        WHERE t.side = 'BUY'
          AND t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * {n_bins}) >= 0
          {cat_filter}
        GROUP BY 1, FLOOR(t.price * {n_bins})
        ORDER BY 1, 2
    """).df()

    dist = agg.groupby('category')['count'].sum().reset_index().sort_values('category')
    n_total = dist['count'].sum()
    print(f"  Trades matched [{category_label}]: {n_total:,}")
    for _, row in dist.iterrows():
        print(f"    {row['category']:30} {row['count']:>12,} trades "
              f"({row['count'] / n_total * 100:.1f}%)")

    return agg


def compute_flb_by_experience_batch(
    con, output_dir, category_names=None, n_bins=20,
):
    """Compute global + per-category experience FLB with minimal memory.

    Strategy: ONE streaming trades scan with inline tier computation.
    DuckDB builds a hash table on _exp_all (~3 GB) and _closed_markets (~5 MB),
    streams 136M trades through both, and aggregates to ~2000 rows.
    No intermediate temp tables are materialized — total memory ~4 GB.

    Previous approach materialized _trade_bins (2.4 GB) + _exp_tiers (3.75 GB)
    + hash table (5 GB) = 11 GB, exceeding 10 GB limit and causing disk spill.

    Returns (flb_global, {cat_name: DataFrame}).
    """
    import pandas as pd
    from favorite_longshot import _CATEGORY_RULES

    if category_names is None:
        category_names = [name for name, _ in _CATEGORY_RULES]

    try:
        con.execute("SELECT 1 FROM _exp_all LIMIT 1")
    except Exception:
        build_experience_tables(con)

    _ensure_closed_markets_registered(con, output_dir)

    # --- Step 1: Compute ALL thresholds in ONE _exp_all scan ---
    print("  Computing experience thresholds...")
    thresh_df = con.execute("""
        SELECT '__global__' AS key,
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY LOG10(GREATEST(n_prior_global, 1))) AS p50_log,
               PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY LOG10(GREATEST(n_prior_global, 1))) AS p90_log
        FROM _exp_all WHERE n_prior_global > 0

        UNION ALL

        SELECT category AS key,
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY LOG10(GREATEST(n_prior_within_cat, 1))) AS p50_log,
               PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY LOG10(GREATEST(n_prior_within_cat, 1))) AS p90_log
        FROM _exp_all WHERE n_prior_within_cat > 0
        GROUP BY category
    """).df()

    thresholds = {}
    for _, row in thresh_df.iterrows():
        p50 = max(1, int(round(10 ** row['p50_log'])))
        p90 = max(p50 + 1, int(round(10 ** row['p90_log'])))
        thresholds[row['key']] = (p50, p90)
        print(f"    [{row['key']}]: novice<={p50}, experienced<={p90}")

    g_p50, g_p90 = thresholds.get('__global__', (3, 15))

    # --- Step 2: Build inline CASE SQL for tiers (no temp tables) ---
    within_case_parts = []
    cats_with_thresholds = []
    for cat in category_names:
        if cat not in thresholds:
            continue
        cats_with_thresholds.append(cat)
        p50, p90 = thresholds[cat]
        within_case_parts.append(
            f"WHEN ea.category = '{cat}' AND ea.n_prior_within_cat = 0 THEN '1. First-Timer' "
            f"WHEN ea.category = '{cat}' AND ea.n_prior_within_cat <= {p50} THEN '2. Novice' "
            f"WHEN ea.category = '{cat}' AND ea.n_prior_within_cat <= {p90} THEN '3. Experienced' "
            f"WHEN ea.category = '{cat}' THEN '4. Veteran'"
        )

    within_tier_sql = "NULL"
    if within_case_parts:
        within_tier_sql = "CASE " + " ".join(within_case_parts) + " ELSE NULL END"

    # --- Step 3: Single streaming query (1 trades scan, ~3-5 min) ---
    # Hash tables: _exp_all ~3 GB + _closed_markets ~5 MB.
    # Output: ~2000 rows (4 tiers × ~6 cats × ~4 within-tiers × 20 bins).
    # No temp tables materialized.
    print("  Streaming experience FLB (1 trades scan, BUY+SELL, no temp tables)...")
    raw = con.execute(f"""
        SELECT
            CASE WHEN ea.n_prior_global = 0      THEN '1. First-Timer'
                 WHEN ea.n_prior_global <= {g_p50} THEN '2. Novice'
                 WHEN ea.n_prior_global <= {g_p90} THEN '3. Experienced'
                 ELSE '4. Veteran' END AS global_tier,
            ea.category AS exp_category,
            {within_tier_sql} AS within_tier,
            -- Implied prob from trader's perspective: BUY=price, SELL=1-price
            FLOOR((CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) * {n_bins}) AS bin_idx,
            COUNT(*) AS n,
            SUM(CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) AS sum_p,
            -- Did the trader's bet win? BUY wins if outcome=winner, SELL wins if outcome!=winner
            SUM(CASE WHEN t.side = 'BUY' AND t.outcome = c.winning_outcome THEN 1.0
                     WHEN t.side = 'SELL' AND t.outcome != c.winning_outcome THEN 1.0
                     ELSE 0.0 END) AS sum_w,
            -- Return from trader's perspective
            SUM(CASE WHEN t.side = 'BUY' AND t.outcome = c.winning_outcome THEN 1.0 - t.price
                     WHEN t.side = 'BUY' THEN -t.price
                     WHEN t.side = 'SELL' AND t.outcome != c.winning_outcome THEN t.price
                     ELSE -(1.0 - t.price) END) AS sum_r
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _exp_all ea
            ON t.proxyWallet = ea.proxyWallet
            AND t.conditionId = ea.conditionId
        WHERE (CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) BETWEEN 0.0 AND 1.0
          AND FLOOR((CASE WHEN t.side = 'BUY' THEN t.price ELSE 1.0 - t.price END) * {n_bins}) >= 0
        GROUP BY 1, 2, 3, 4
    """).df()
    print(f"    Raw result: {len(raw)} rows, {raw['n'].sum():,} trades total")

    # --- Step 4: Python-side split into global + per-category ---
    # Global: re-aggregate across categories (partial sums are additive)
    g = raw.groupby(['global_tier', 'bin_idx'], as_index=False).agg(
        n=('n', 'sum'), sum_p=('sum_p', 'sum'),
        sum_w=('sum_w', 'sum'), sum_r=('sum_r', 'sum'),
    )
    flb_global = pd.DataFrame({
        'category': g['global_tier'],
        'bin_idx': g['bin_idx'].astype(int),
        'count': g['n'],
        'mean_implied_prob': g['sum_p'] / g['n'],
        'empirical_win_rate': g['sum_w'] / g['n'],
        'mean_return': g['sum_r'] / g['n'],
    })
    print(f"    Global: {flb_global['count'].sum():,} trades")

    # Per-category: filter rows with within_tier, split by exp_category
    cat_results = {}
    cat_raw = raw[raw['within_tier'].notna()]
    for cat in category_names:
        cdf = cat_raw[cat_raw['exp_category'] == cat]
        if len(cdf) == 0:
            continue
        cg = cdf.groupby(['within_tier', 'bin_idx'], as_index=False).agg(
            n=('n', 'sum'), sum_p=('sum_p', 'sum'),
            sum_w=('sum_w', 'sum'), sum_r=('sum_r', 'sum'),
        )
        cat_results[cat] = pd.DataFrame({
            'category': cg['within_tier'],
            'bin_idx': cg['bin_idx'].astype(int),
            'count': cg['n'],
            'mean_implied_prob': cg['sum_p'] / cg['n'],
            'empirical_win_rate': cg['sum_w'] / cg['n'],
            'mean_return': cg['sum_r'] / cg['n'],
        })
        print(f"    {cat}: {cg['n'].sum():,} trades")

    return flb_global, cat_results


def compute_category_experience_table(
    flb_global: pd.DataFrame,
    cat_results: dict,
    n_bins: int = 20,
    ls_range: tuple = None,
    fav_range: tuple = None,
) -> pd.DataFrame:
    """
    Build a summary table of FLB by category × experience tier.

    Each row = one (category, experience_tier) pair with:
        ls_return, fav_return, spread, n_ls_trades, n_fav_trades

    Args:
        flb_global:  DataFrame from compute_flb_by_experience_batch (global).
        cat_results: dict from compute_flb_by_experience_batch (per-category).
        n_bins:      number of probability bins used (default 20).
        ls_range:    (lo, hi) bin_idx range for longshots. Default (1, 4) = 5-25%.
        fav_range:   (lo, hi) bin_idx range for favorites. Default (15, 18) = 75-95%.

    Returns:
        DataFrame with columns: category, tier, ls_return, fav_return, spread,
        n_ls_trades, n_fav_trades.
    """
    if ls_range is None:
        ls_range = (1, 4)
    if fav_range is None:
        fav_range = (15, 18)

    tiers = ['1. First-Timer', '2. Novice', '3. Experienced', '4. Veteran']
    tier_labels = ['First-Timer', 'Novice', 'Experienced', 'Veteran']

    def _extract(df, tier):
        grp = df[df['category'] == tier]
        ls = grp[grp['bin_idx'].between(*ls_range)]
        fv = grp[grp['bin_idx'].between(*fav_range)]
        ls_ret = (ls['mean_return'] * ls['count']).sum() / ls['count'].sum() if ls['count'].sum() > 0 else np.nan
        fv_ret = (fv['mean_return'] * fv['count']).sum() / fv['count'].sum() if fv['count'].sum() > 0 else np.nan
        return ls_ret, fv_ret, ls['count'].sum(), fv['count'].sum()

    rows = []
    # Global
    for tier, label in zip(tiers, tier_labels):
        ls_r, fv_r, n_ls, n_fv = _extract(flb_global, tier)
        rows.append({
            'category': 'Global', 'tier': label,
            'ls_return': ls_r, 'fav_return': fv_r,
            'spread': fv_r - ls_r if pd.notna(ls_r) and pd.notna(fv_r) else np.nan,
            'n_ls_trades': int(n_ls), 'n_fav_trades': int(n_fv),
        })
    # Per-category
    for cat_name, flb_df in cat_results.items():
        for tier, label in zip(tiers, tier_labels):
            ls_r, fv_r, n_ls, n_fv = _extract(flb_df, tier)
            rows.append({
                'category': cat_name, 'tier': label,
                'ls_return': ls_r, 'fav_return': fv_r,
                'spread': fv_r - ls_r if pd.notna(ls_r) and pd.notna(fv_r) else np.nan,
                'n_ls_trades': int(n_ls), 'n_fav_trades': int(n_fv),
            })

    return pd.DataFrame(rows)


def compute_cross_substitution(
    con: 'duckdb.DuckDBPyConnection',
    output_dir: Path,
    exp_tier_thresholds: tuple = None,
) -> dict:
    """
    Analyze whether the same wallets drive FLB across multiple categories.

    Returns dict with:
        'overlap_matrix':   DataFrame — fraction of category-A longshot traders
                            who also trade longshots in category B.
        'trader_category_counts': DataFrame — how many categories each trader
                            participates in, by experience tier.
        'substitution_panel': DataFrame — weekly panel of trader counts per
                            category, for exogenous variation analysis.
        'flb_by_ncats':     DataFrame — FLB calibration by number of categories
                            a trader participates in (longshot tail).

    Args:
        exp_tier_thresholds: (p50, p90) for experience tiers. If None, computed
                             from _exp_all.
    """
    from favorite_longshot import _CATEGORY_CASE_SQL_T, _CATEGORY_RULES

    _ensure_closed_markets_registered(con, output_dir)

    # Ensure _exp_all exists
    try:
        con.execute("SELECT 1 FROM _exp_all LIMIT 1")
    except Exception:
        build_experience_tables(con)

    # Get experience thresholds
    if exp_tier_thresholds is None:
        thresh = con.execute("""
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP
                    (ORDER BY LOG10(GREATEST(n_prior_global, 1))) AS p50_log,
                PERCENTILE_CONT(0.90) WITHIN GROUP
                    (ORDER BY LOG10(GREATEST(n_prior_global, 1))) AS p90_log
            FROM _exp_all WHERE n_prior_global > 0
        """).fetchone()
        p50 = max(1, int(round(10 ** thresh[0])))
        p90 = max(p50 + 1, int(round(10 ** thresh[1])))
    else:
        p50, p90 = exp_tier_thresholds

    case_sql = _CATEGORY_CASE_SQL_T
    tier_case = f"""
        CASE WHEN ea.n_prior_global = 0      THEN 'First-Timer'
             WHEN ea.n_prior_global <= {p50}  THEN 'Novice'
             WHEN ea.n_prior_global <= {p90}  THEN 'Experienced'
             ELSE 'Veteran' END
    """

    # --- 1. Which traders trade longshots in which categories? ---
    # Longshot = price < 0.20 (generous definition for cross-category overlap)
    print("Computing cross-category longshot participation...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _ls_traders AS
        SELECT DISTINCT
            t.proxyWallet,
            {case_sql} AS category,
            {tier_case} AS exp_tier
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _exp_all ea
            ON t.proxyWallet = ea.proxyWallet
            AND t.conditionId = ea.conditionId
        WHERE t.side = 'BUY' AND t.price < 0.20
    """)
    n_ls = con.execute("SELECT COUNT(*) FROM _ls_traders").fetchone()[0]
    print(f"  Longshot trader-category pairs: {n_ls:,}")

    # --- 2. How many categories does each longshot trader appear in? ---
    trader_cat_counts = con.execute("""
        SELECT
            proxyWallet,
            exp_tier,
            COUNT(DISTINCT category) AS n_categories,
            STRING_AGG(DISTINCT category, ', ' ORDER BY category) AS categories
        FROM _ls_traders
        GROUP BY proxyWallet, exp_tier
    """).df()
    print(f"  Unique longshot traders: {len(trader_cat_counts):,}")

    # Summary: distribution of n_categories by tier
    cat_count_summary = trader_cat_counts.groupby(['exp_tier', 'n_categories']).size() \
        .reset_index(name='n_traders')

    # --- 3. Overlap matrix: P(trades LS in cat B | trades LS in cat A) ---
    categories = [name for name, _ in _CATEGORY_RULES] + ['Other']
    overlap_df = con.execute("""
        SELECT a.category AS cat_a, b.category AS cat_b,
               COUNT(DISTINCT a.proxyWallet) AS n_overlap
        FROM _ls_traders a
        INNER JOIN _ls_traders b ON a.proxyWallet = b.proxyWallet
        GROUP BY a.category, b.category
    """).df()

    # Pivot to matrix form
    diag = overlap_df[overlap_df['cat_a'] == overlap_df['cat_b']] \
        .set_index('cat_a')['n_overlap'].to_dict()
    overlap_df['overlap_pct'] = overlap_df.apply(
        lambda r: r['n_overlap'] / diag.get(r['cat_a'], 1), axis=1
    )

    # --- 4. Weekly panel: trader counts per category (for exogenous variation) ---
    print("Building weekly substitution panel...")
    sub_panel = con.execute(f"""
        SELECT
            DATE_TRUNC('week', to_timestamp(t.timestamp)) AS week,
            {case_sql} AS category,
            {tier_case} AS exp_tier,
            COUNT(DISTINCT t.proxyWallet) AS n_traders,
            COUNT(*) AS n_trades,
            SUM(t.usdcSize) AS total_vol
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _exp_all ea
            ON t.proxyWallet = ea.proxyWallet
            AND t.conditionId = ea.conditionId
        WHERE t.side = 'BUY' AND t.price < 0.20
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """).df()
    print(f"  Weekly panel: {len(sub_panel):,} rows "
          f"({sub_panel['week'].nunique()} weeks)")

    # --- 5. FLB by number of categories traded (longshot traders only) ---
    print("Computing FLB by cross-category participation...")
    # Register n_categories per trader
    ncat_df = trader_cat_counts[['proxyWallet', 'n_categories']].copy()
    ncat_df['ncat_label'] = np.where(
        ncat_df['n_categories'] == 1, '1 category',
        np.where(ncat_df['n_categories'] == 2, '2 categories',
        np.where(ncat_df['n_categories'] <= 4, '3-4 categories', '5+ categories'))
    )
    con.register('_ncat_map', ncat_df)

    flb_by_ncats = con.execute(f"""
        SELECT
            nc.ncat_label AS category,
            FLOOR(t.price * 20) AS bin_idx,
            COUNT(*) AS count,
            AVG(t.price) AS mean_implied_prob,
            AVG(CAST(t.outcome = c.winning_outcome AS DOUBLE)) AS empirical_win_rate,
            AVG(CASE WHEN t.outcome = c.winning_outcome
                     THEN 1.0 - t.price ELSE -t.price END) AS mean_return
        FROM trades t
        INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
        INNER JOIN _ncat_map nc ON t.proxyWallet = nc.proxyWallet
        WHERE t.side = 'BUY' AND t.price >= 0 AND t.price <= 1
          AND FLOOR(t.price * 20) >= 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()

    con.unregister('_ncat_map')
    con.execute("DROP TABLE IF EXISTS _ls_traders")

    return {
        'overlap_matrix': overlap_df,
        'cat_count_summary': cat_count_summary,
        'trader_cat_counts': trader_cat_counts,
        'substitution_panel': sub_panel,
        'flb_by_ncats': flb_by_ncats,
    }


def compute_cross_substitution_subprocess(output_dir):
    """Subprocess target: compute cross-substitution analysis."""
    from data_loader import get_connection
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    # Register _exp_all from parquet
    exp_path = Path(output_dir) / '_exp_all.parquet'
    con.execute(
        f"CREATE OR REPLACE VIEW _exp_all AS "
        f"SELECT * FROM read_parquet('{exp_path}')"
    )
    return compute_cross_substitution(con, Path(output_dir))


# ============================================================================
# Subprocess-safe standalone functions
# ============================================================================
# These are designed to be called via subprocess_runner.sp_run().
# Each creates its own DuckDB connection so no state is shared with the
# parent Jupyter kernel. When the subprocess exits the OS reclaims all memory.
# ============================================================================


def build_experience_tables_to_parquet(output_path):
    """
    Subprocess target: build _exp_all and save to parquet.

    Creates a fresh DuckDB connection, runs build_experience_tables (which
    cleans up its own intermediates _tc_first_all and _exp_step1), then
    writes _exp_all to parquet and closes the connection.

    Args:
        output_path: Path-like where the parquet file should be written.

    Returns:
        str(output_path)
    """
    from data_loader import get_connection
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    build_experience_tables(con)
    con.execute(f"COPY _exp_all TO '{output_path}' (FORMAT PARQUET)")
    n = con.execute("SELECT COUNT(*) FROM _exp_all").fetchone()[0]
    print(f"  build_experience_tables_to_parquet: {n:,} rows → {output_path}")
    con.close()
    return str(output_path)


def build_tc_tail_stats_to_parquet(
    output_path,
    ls_lo_main: float = 0.02,
    ls_hi: float = 0.05,
    fav_lo: float = 0.95,
    fav_hi_main: float = 0.98,
    ls_lo_backup: float = 0.01,
    fav_hi_backup: float = 0.99,
):
    """
    Subprocess target: build _tc_tail_stats and save to parquet.

    Single trades scan with conditional aggregation across 4 tail domains.
    Equivalent to the inline SQL in notebook cell 91.

    Args:
        output_path:   Path-like where the parquet file should be written.
        ls_lo_main:    Longshot main lower bound (default 0.02).
        ls_hi:         Longshot upper bound (default 0.05).
        fav_lo:        Favorite lower bound (default 0.95).
        fav_hi_main:   Favorite main upper bound (default 0.98).
        ls_lo_backup:  Longshot backup lower bound (default 0.01).
        fav_hi_backup: Favorite backup upper bound (default 0.99).

    Returns:
        str(output_path)
    """
    from data_loader import get_connection
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")

    print("Building _tc_tail_stats (single trades scan)...")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _tc_tail_stats AS
        SELECT
            proxyWallet, conditionId,
            -- Longshot main
            SUM(CASE WHEN price BETWEEN {ls_lo_main} AND {ls_hi}
                     THEN usdcSize * price ELSE 0 END)
              / NULLIF(SUM(CASE WHEN price BETWEEN {ls_lo_main} AND {ls_hi}
                                THEN usdcSize ELSE 0 END), 0) AS vwap_ls_main,
            SUM(CASE WHEN price BETWEEN {ls_lo_main} AND {ls_hi}
                     THEN usdcSize ELSE 0 END) AS vol_ls_main,
            -- Longshot backup
            SUM(CASE WHEN price BETWEEN {ls_lo_backup} AND {ls_hi}
                     THEN usdcSize * price ELSE 0 END)
              / NULLIF(SUM(CASE WHEN price BETWEEN {ls_lo_backup} AND {ls_hi}
                                THEN usdcSize ELSE 0 END), 0) AS vwap_ls_backup,
            SUM(CASE WHEN price BETWEEN {ls_lo_backup} AND {ls_hi}
                     THEN usdcSize ELSE 0 END) AS vol_ls_backup,
            -- Favorite main
            SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_main}
                     THEN usdcSize * price ELSE 0 END)
              / NULLIF(SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_main}
                                THEN usdcSize ELSE 0 END), 0) AS vwap_fav_main,
            SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_main}
                     THEN usdcSize ELSE 0 END) AS vol_fav_main,
            -- Favorite backup
            SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_backup}
                     THEN usdcSize * price ELSE 0 END)
              / NULLIF(SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_backup}
                                THEN usdcSize ELSE 0 END), 0) AS vwap_fav_backup,
            SUM(CASE WHEN price BETWEEN {fav_lo} AND {fav_hi_backup}
                     THEN usdcSize ELSE 0 END) AS vol_fav_backup
        FROM trades
        WHERE side = 'BUY'
          AND (price BETWEEN {ls_lo_backup} AND {ls_hi}
               OR price BETWEEN {fav_lo} AND {fav_hi_backup})
        GROUP BY proxyWallet, conditionId
    """)
    n = con.execute("SELECT COUNT(*) FROM _tc_tail_stats").fetchone()[0]
    print(f"  _tc_tail_stats: {n:,} (trader, contract) pairs → {output_path}")
    con.execute(f"COPY _tc_tail_stats TO '{output_path}' (FORMAT PARQUET)")
    con.close()
    return str(output_path)


def compute_flb_by_segments_batch_subprocess(
    output_dir,
    trader_df,
    segment_cols: list,
    n_bins: int = 20,
) -> dict:
    """
    Subprocess target: compute batch segment FLB in a fresh DuckDB connection.

    Equivalent to calling compute_flb_by_segments_batch directly, but designed
    to run in a subprocess so all working memory (136M-row UNION ALL scan) is
    reclaimed by the OS on exit.

    Args:
        output_dir:   Path to output directory (for _ensure_closed_markets_registered).
        trader_df:    DataFrame with 'trader' column and all segment_cols.
        segment_cols: List of segment column names.
        n_bins:       Number of probability bins.

    Returns:
        dict of {segment_col: DataFrame} — same as compute_flb_by_segments_batch.
    """
    from data_loader import get_connection
    from pathlib import Path
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    return compute_flb_by_segments_batch(con, Path(output_dir), trader_df, segment_cols, n_bins)


def compute_flb_by_experience_batch_subprocess(
    output_dir,
    exp_all_parquet_path: str,
    n_bins: int = 20,
):
    """
    Subprocess target: compute experience FLB in a fresh connection.

    Registers _exp_all from a parquet file (previously built by
    build_experience_tables_to_parquet) instead of rebuilding it.

    Args:
        output_dir:            Path to output directory.
        exp_all_parquet_path:  Path to _exp_all.parquet.
        n_bins:                Number of probability bins.

    Returns:
        (flb_global_df, cat_results_dict) — same as compute_flb_by_experience_batch.
    """
    from data_loader import get_connection
    from pathlib import Path
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    # Register _exp_all as a VIEW from parquet — no need to rebuild.
    con.execute(
        f"CREATE OR REPLACE VIEW _exp_all AS "
        f"SELECT * FROM read_parquet('{exp_all_parquet_path}')"
    )
    # Read row count from parquet metadata (footer only — no data scan)
    try:
        n = con.execute(
            f"SELECT SUM(num_rows) FROM parquet_file_metadata('{exp_all_parquet_path}')"
        ).fetchone()[0]
    except Exception:
        n = '?'
    print(f"  _exp_all loaded from parquet: {n:,} rows")
    return compute_flb_by_experience_batch(con, Path(output_dir), n_bins=n_bins)


def compute_flb_by_contract_intensity_subprocess(
    output_dir,
    n_bins: int = 20,
):
    """
    Subprocess target: compute contract-intensity FLB in a fresh connection.

    Equivalent to compute_flb_by_contract_intensity but runs in subprocess
    so the ~3 GB _tc_counts working memory is reclaimed by the OS on exit.
    (compute_flb_by_contract_intensity already drops _tc_counts before returning.)

    Args:
        output_dir: Path to output directory.
        n_bins:     Number of probability bins.

    Returns:
        DataFrame compatible with plot_favorite_longshot_by_category.
    """
    from data_loader import get_connection
    from pathlib import Path
    con = get_connection(force_new=True)
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    return compute_flb_by_contract_intensity(con, Path(output_dir), n_bins)
