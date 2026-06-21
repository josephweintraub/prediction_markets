"""
Bot / HFT detection for Polymarket on-chain trade data.

Flags wallets as non-human using behavioral heuristics adapted from the
Polymarket Reputation Study brief.  All heavy lifting runs in DuckDB SQL
so the function works comfortably at 100M+ trade scale.

Criteria
--------
A  Inter-trade interval (ITI):  median < 1 s  → definite;  1-10 s → likely
B  Trades per active day:       > 500 → definite;  > 200 → likely
C  Round-the-clock activity:    hour-of-day HHI < 0.06  AND  > 500 trades
E  Fixed trade size:            CV(usdcSize) < 0.05  AND  > 50 trades

Criterion G (cross-market simultaneity) is excluded because approximate
block timestamps lack sub-second precision.

Composite classification
------------------------
Definite Bot   : flag_a_definite
                 OR  (flag_a_likely  AND  any one of {B_definite, B_likely, C, E})
Likely Algo    : flag_b_definite  AND  flag_c
Possible HFT   : >= 2 of {flag_b_likely, flag_c, flag_e}

Usage
-----
    from bot_filter import build_wallet_flags, print_attrition_log
    stats = build_wallet_flags(con)
    print_attrition_log(con)
"""

from __future__ import annotations
from typing import Dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(msg: str, verbose: bool) -> None:
    if verbose:
        print(msg)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_wallet_flags(con, verbose: bool = True) -> Dict[str, object]:
    """
    Compute bot/HFT flags for all wallets using behavioral criteria.

    Creates temp table ``wallet_flags`` with columns:

        proxyWallet, n_trades, trades_per_active_day, active_days,
        median_iti, flag_a_definite, flag_a_likely,
        flag_b_definite, flag_b_likely, flag_c, flag_e, is_nonhuman

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Must already have a ``trades`` view registered.
    verbose : bool
        Print progress and attrition summary.

    Returns
    -------
    dict
        Keys: total_wallets, total_trades, nonhuman_wallets,
        nonhuman_trades, pct_flagged.
    """

    # ------------------------------------------------------------------
    # 0.  Base wallet stats
    # ------------------------------------------------------------------
    _log("Building base wallet stats ...", verbose)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_base AS
        SELECT
            proxyWallet,
            COUNT(*)                                          AS n_trades,
            MIN(timestamp)                                    AS min_ts,
            MAX(timestamp)                                    AS max_ts,
            COUNT(DISTINCT CAST(epoch_ms(timestamp * 1000) AS DATE))
                                                              AS active_days,
            -- cheap ITI proxy: span / (n-1), only meaningful when n >= 2
            CASE WHEN COUNT(*) >= 2
                 THEN (MAX(timestamp) - MIN(timestamp))
                      / (COUNT(*) - 1)
                 ELSE NULL END                                AS mean_iti_approx
        FROM trades
        GROUP BY proxyWallet
    """)

    # trades per active day (used by criterion B)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_base2 AS
        SELECT
            *,
            n_trades * 1.0 / GREATEST(active_days, 1)        AS trades_per_active_day
        FROM _bf_base
    """)

    # ------------------------------------------------------------------
    # A.  Inter-trade interval
    # ------------------------------------------------------------------
    _log("Computing inter-trade intervals ...", verbose)

    # Only compute exact median for wallets whose approximate mean ITI is
    # below 120 s — this avoids a global window sort.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_iti AS
        WITH candidates AS (
            SELECT proxyWallet
            FROM _bf_base2
            WHERE mean_iti_approx IS NOT NULL
              AND mean_iti_approx < 120
        ),
        lagged AS (
            SELECT
                t.proxyWallet,
                t.timestamp - LAG(t.timestamp)
                    OVER (PARTITION BY t.proxyWallet ORDER BY t.timestamp)
                    AS iti
            FROM trades t
            INNER JOIN candidates c USING (proxyWallet)
        )
        SELECT
            proxyWallet,
            MEDIAN(iti)  AS median_iti
        FROM lagged
        WHERE iti IS NOT NULL
        GROUP BY proxyWallet
    """)

    # ------------------------------------------------------------------
    # B.  Trades per active day
    # ------------------------------------------------------------------
    _log("Evaluating trades-per-day criterion ...", verbose)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_crit_b AS
        SELECT
            proxyWallet,
            trades_per_active_day,
            (trades_per_active_day > 500)   AS flag_b_definite,
            (trades_per_active_day > 200)   AS flag_b_likely
        FROM _bf_base2
    """)

    # ------------------------------------------------------------------
    # C.  Round-the-clock activity  (hour HHI < 0.06 AND > 500 trades)
    # ------------------------------------------------------------------
    _log("Computing hourly activity concentration ...", verbose)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_crit_c AS
        WITH hourly AS (
            SELECT
                proxyWallet,
                EXTRACT(HOUR FROM epoch_ms(timestamp * 1000)) AS hr,
                COUNT(*) AS cnt
            FROM trades
            GROUP BY proxyWallet, hr
        ),
        totals AS (
            SELECT proxyWallet, SUM(cnt) AS total
            FROM hourly
            GROUP BY proxyWallet
        ),
        hhi AS (
            SELECT
                h.proxyWallet,
                SUM( (h.cnt * 1.0 / t.total) * (h.cnt * 1.0 / t.total) ) AS hour_hhi
            FROM hourly h
            JOIN totals t USING (proxyWallet)
            GROUP BY h.proxyWallet
        )
        SELECT
            hhi.proxyWallet,
            hhi.hour_hhi,
            (hhi.hour_hhi < 0.06 AND b.n_trades > 500) AS flag_c
        FROM hhi
        JOIN _bf_base2 b USING (proxyWallet)
    """)

    # ------------------------------------------------------------------
    # E.  Fixed trade size  (CV < 0.05 AND > 50 trades)
    # ------------------------------------------------------------------
    _log("Computing trade-size variability ...", verbose)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _bf_crit_e AS
        SELECT
            proxyWallet,
            STDDEV_POP(usdcSize) / NULLIF(AVG(usdcSize), 0) AS size_cv,
            COUNT(*)                                          AS n_for_cv,
            (STDDEV_POP(usdcSize) / NULLIF(AVG(usdcSize), 0) < 0.05
             AND COUNT(*) > 50)                               AS flag_e
        FROM trades
        GROUP BY proxyWallet
    """)

    # ------------------------------------------------------------------
    # Combine into wallet_flags
    # ------------------------------------------------------------------
    _log("Assembling composite flags ...", verbose)

    con.execute("""
        CREATE OR REPLACE TEMP TABLE wallet_flags AS
        WITH combined AS (
            SELECT
                b.proxyWallet,
                b.n_trades,
                b.trades_per_active_day,
                b.active_days,
                i.median_iti,

                -- Criterion A
                COALESCE(i.median_iti < 1,   FALSE)  AS flag_a_definite,
                COALESCE(i.median_iti >= 1
                     AND i.median_iti < 10,  FALSE)  AS flag_a_likely,

                -- Criterion B
                COALESCE(cb.flag_b_definite, FALSE)   AS flag_b_definite,
                COALESCE(cb.flag_b_likely,   FALSE)   AS flag_b_likely,

                -- Criterion C
                COALESCE(cc.flag_c,          FALSE)   AS flag_c,

                -- Criterion E
                COALESCE(ce.flag_e,          FALSE)   AS flag_e

            FROM _bf_base2       b
            LEFT JOIN _bf_iti    i  USING (proxyWallet)
            LEFT JOIN _bf_crit_b cb USING (proxyWallet)
            LEFT JOIN _bf_crit_c cc USING (proxyWallet)
            LEFT JOIN _bf_crit_e ce USING (proxyWallet)
        )
        SELECT
            *,
            -- Composite: is_nonhuman
            (
                flag_a_definite                                           -- definite: ITI < 1s
                OR (flag_a_likely AND (flag_b_definite OR flag_b_likely
                                       OR flag_c OR flag_e))             -- ITI 1-10s + 1 other
                OR (flag_b_definite AND flag_c)                          -- likely algo
                OR (  (CAST(flag_b_likely AS INT)
                     + CAST(flag_c       AS INT)
                     + CAST(flag_e       AS INT)) >= 2 )                -- possible HFT: 2 of 3
            ) AS is_nonhuman
        FROM combined
    """)

    # ------------------------------------------------------------------
    # Clean up intermediates
    # ------------------------------------------------------------------
    for tbl in ("_bf_base", "_bf_base2", "_bf_iti",
                "_bf_crit_b", "_bf_crit_c", "_bf_crit_e"):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    # ------------------------------------------------------------------
    # Summary stats
    # ------------------------------------------------------------------
    stats_row = con.execute("""
        SELECT
            COUNT(*)                                          AS total_wallets,
            SUM(n_trades)                                     AS total_trades,
            SUM(CASE WHEN is_nonhuman THEN 1    ELSE 0 END)  AS nonhuman_wallets,
            SUM(CASE WHEN is_nonhuman THEN n_trades ELSE 0 END)
                                                              AS nonhuman_trades
        FROM wallet_flags
    """).fetchone()

    total_wallets, total_trades, nh_wallets, nh_trades = stats_row

    stats = {
        "total_wallets":    total_wallets,
        "total_trades":     total_trades,
        "nonhuman_wallets": nh_wallets,
        "nonhuman_trades":  nh_trades,
        "pct_flagged":      round(100.0 * nh_wallets / total_wallets, 2)
                            if total_wallets else 0.0,
    }

    if verbose:
        print_attrition_log(con)
        _log(f"\nDone.  {nh_wallets:,} wallets flagged as non-human "
             f"({stats['pct_flagged']:.2f}% of {total_wallets:,}).",
             verbose)

    return stats


# ---------------------------------------------------------------------------
# Attrition log
# ---------------------------------------------------------------------------

def print_attrition_log(con) -> None:
    """Print the filtering attrition log from the wallet_flags table."""

    rows = con.execute("""
        SELECT
            'All wallets'          AS stage,
            COUNT(*)               AS wallets,
            SUM(n_trades)          AS trades
        FROM wallet_flags

        UNION ALL
        SELECT 'A-definite (ITI < 1s)',
            SUM(CAST(flag_a_definite AS INT)),
            SUM(CASE WHEN flag_a_definite THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'A-likely (ITI 1-10s)',
            SUM(CAST(flag_a_likely AS INT)),
            SUM(CASE WHEN flag_a_likely THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'B-definite (>500 tpd)',
            SUM(CAST(flag_b_definite AS INT)),
            SUM(CASE WHEN flag_b_definite THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'B-likely (>200 tpd)',
            SUM(CAST(flag_b_likely AS INT)),
            SUM(CASE WHEN flag_b_likely THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'C (round-the-clock)',
            SUM(CAST(flag_c AS INT)),
            SUM(CASE WHEN flag_c THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'E (fixed trade size)',
            SUM(CAST(flag_e AS INT)),
            SUM(CASE WHEN flag_e THEN n_trades ELSE 0 END)
        FROM wallet_flags

        UNION ALL
        SELECT 'COMPOSITE: is_nonhuman',
            SUM(CAST(is_nonhuman AS INT)),
            SUM(CASE WHEN is_nonhuman THEN n_trades ELSE 0 END)
        FROM wallet_flags
    """).fetchall()

    total_w = rows[0][1] or 1
    total_t = rows[0][2] or 1

    print("\n{:<28s} {:>12s} {:>8s} {:>14s} {:>8s}".format(
        "Criterion", "Wallets", "(%)", "Trades", "(%)"))
    print("-" * 74)
    for stage, w, t in rows:
        pct_w = 100.0 * (w or 0) / total_w
        pct_t = 100.0 * (t or 0) / total_t
        print("{:<28s} {:>12,d} {:>7.2f}% {:>14,d} {:>7.2f}%".format(
            stage, w or 0, pct_w, t or 0, pct_t))
