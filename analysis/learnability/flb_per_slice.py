"""Per-slice FLB calibration — v3.

Changes vs v2:
  - 3-way clustered SE (day × trader × market) via Cameron-Gelbach-Miller
    inclusion-exclusion (Var = V1+V2+V3 − V12−V13−V23 + V123).
  - "market" = per-market condition_id (0x hex) from augmented parquet,
    NOT the per-outcome token id. So YES/NO contracts of a binary market
    cluster together (their prices are mechanically correlated).
  - Fama-MacBeth dropped entirely (caller no longer requests it).
  - Full 10-decile breakdown returned per slice; both saved to disk and
    surfaced in the writeup.

  Updown markets are INCLUDED here — the upstream view-building code in
  run_phase1_v3.py removes the updown exclusion that was in v1/v2.
"""
from __future__ import annotations
import numpy as np
import pandas as pd


# ---------- clustered SE helpers ----------

def oneway_cluster_se(returns, clusters):
    N = len(returns)
    if N == 0:
        return 0.0
    resids = (returns - returns.mean()).values
    codes, _ = pd.factorize(clusters, sort=False)
    groups = pd.Series(resids).groupby(codes).sum()
    return float(np.sqrt(max((groups ** 2).sum() / (N ** 2), 0)))


def threeway_cluster_se(returns, c1, c2, c3):
    """3-way clustered SE — Cameron-Gelbach-Miller (2011) inclusion-exclusion.

    Var = V1 + V2 + V3 − V12 − V13 − V23 + V123

    All clusters are factorize-encoded to integer codes and intersections are
    formed by integer arithmetic, so a 50M-row slice costs ~7 grouped sums
    instead of string concatenation.
    """
    N = len(returns)
    if N == 0:
        return 0.0
    resids = (returns - returns.mean()).values

    def cv(codes):
        groups = pd.Series(resids).groupby(codes).sum()
        return (groups ** 2).sum() / (N ** 2)

    c1c, _ = pd.factorize(c1, sort=False)
    c2c, _ = pd.factorize(c2, sort=False)
    c3c, _ = pd.factorize(c3, sort=False)

    c1c64 = c1c.astype(np.int64)
    c2c64 = c2c.astype(np.int64)
    c3c64 = c3c.astype(np.int64)

    n2 = int(c2c64.max()) + 1 if N else 1
    n3 = int(c3c64.max()) + 1 if N else 1

    c12 = c1c64 * n2 + c2c64
    c13 = c1c64 * n3 + c3c64
    c23 = c2c64 * n3 + c3c64
    c123 = c12 * n3 + c3c64  # safe: max ≈ n1*n2*n3 ≪ 2^63 for any real slice

    v1 = cv(c1c64); v2 = cv(c2c64); v3 = cv(c3c64)
    v12 = cv(c12);  v13 = cv(c13);  v23 = cv(c23)
    v123 = cv(c123)

    var = v1 + v2 + v3 - v12 - v13 - v23 + v123
    return float(np.sqrt(max(var, 0)))


def threeway_cluster_se_weighted(returns, weights, c1, c2, c3):
    """3-way clustered SE for a WEIGHTED mean theta = sum(w*r)/sum(w).

    Same Cameron-Gelbach-Miller inclusion-exclusion as the unweighted form,
    but residuals are the weighted scores e_i = w_i*(r_i - theta) and the
    normalizer is W^2 = (sum w)^2 instead of N^2.
    """
    N = len(returns)
    if N == 0:
        return 0.0
    r = returns.values if hasattr(returns, "values") else np.asarray(returns, float)
    w = weights.values if hasattr(weights, "values") else np.asarray(weights, float)
    W = w.sum()
    if W <= 0:
        return 0.0
    theta = (w * r).sum() / W
    resids = w * (r - theta)

    def cv(codes):
        groups = pd.Series(resids).groupby(codes).sum()
        return (groups ** 2).sum() / (W ** 2)

    c1c, _ = pd.factorize(c1, sort=False)
    c2c, _ = pd.factorize(c2, sort=False)
    c3c, _ = pd.factorize(c3, sort=False)
    c1c64 = c1c.astype(np.int64); c2c64 = c2c.astype(np.int64); c3c64 = c3c.astype(np.int64)
    n2 = int(c2c64.max()) + 1 if N else 1
    n3 = int(c3c64.max()) + 1 if N else 1
    c12 = c1c64 * n2 + c2c64
    c13 = c1c64 * n3 + c3c64
    c23 = c2c64 * n3 + c3c64
    c123 = c12 * n3 + c3c64
    var = (cv(c1c64) + cv(c2c64) + cv(c3c64)
           - cv(c12) - cv(c13) - cv(c23) + cv(c123))
    return float(np.sqrt(max(var, 0)))


def sig_stars(t):
    if not np.isfinite(t):
        return ""
    a = abs(t)
    if a > 3.29: return "***"
    if a > 2.58: return "**"
    if a > 1.96: return "*"
    return ""


# ---------- per-decile + spread computation, 3-way SE ----------

def _wmean(x, w):
    x = x.values if hasattr(x, "values") else np.asarray(x, float)
    w = w.values if hasattr(w, "values") else np.asarray(w, float)
    W = w.sum()
    return float((w * x).sum() / W) if W > 0 else np.nan


def compute_3way_decile_table(df, n_bins=10):
    """For each decile 1..n_bins: count- AND dollar-weighted impl_prob, win_rate,
    cal_error, se_3w, t_3w, plus n and total usd. Plus a spread row: D{n_bins} − D1,
    both weightings. Dollar = weighted by usdcSize; count = each trade equal.
    """
    decile_results = []
    for d in range(1, n_bins + 1):
        sub = df[df["decile"] == d]
        if len(sub) < 50:
            decile_results.append({
                "decile": d, "impl_prob": np.nan, "win_rate": np.nan,
                "cal_error": np.nan, "se_3w": np.nan, "t_3w": np.nan,
                "impl_prob_dol": np.nan, "win_rate_dol": np.nan,
                "cal_error_dol": np.nan, "se_3w_dol": np.nan, "t_3w_dol": np.nan,
                "n": len(sub), "usd": float(sub["usdc"].sum()) if len(sub) else 0.0,
            })
            continue
        w = sub["usdc"]
        # count-weighted
        mean_ret = sub["ret"].mean()
        se = threeway_cluster_se(sub["ret"], sub["trade_day"],
                                 sub["proxyWallet"], sub["market_id"])
        t = mean_ret / se if se > 0 else 0.0
        # dollar-weighted
        mean_ret_d = _wmean(sub["ret"], w)
        se_d = threeway_cluster_se_weighted(sub["ret"], w, sub["trade_day"],
                                            sub["proxyWallet"], sub["market_id"])
        t_d = mean_ret_d / se_d if se_d > 0 else 0.0
        decile_results.append({
            "decile": d, "impl_prob": sub["price"].mean(),
            "win_rate": sub["won"].mean(), "cal_error": mean_ret,
            "se_3w": se, "t_3w": t,
            "impl_prob_dol": _wmean(sub["price"], w),
            "win_rate_dol": _wmean(sub["won"], w),
            "cal_error_dol": mean_ret_d, "se_3w_dol": se_d, "t_3w_dol": t_d,
            "n": len(sub), "usd": float(w.sum()),
        })
    result = pd.DataFrame(decile_results)

    d1 = df[df["decile"] == 1]
    dn = df[df["decile"] == n_bins]
    # count-weighted spread
    spread = (dn["ret"].mean() - d1["ret"].mean()
              if len(d1) and len(dn) else np.nan)
    se1 = (threeway_cluster_se(d1["ret"], d1["trade_day"],
                               d1["proxyWallet"], d1["market_id"]) if len(d1) else 0)
    sen = (threeway_cluster_se(dn["ret"], dn["trade_day"],
                               dn["proxyWallet"], dn["market_id"]) if len(dn) else 0)
    spread_se = float(np.sqrt(se1**2 + sen**2))
    spread_t = spread / spread_se if spread_se > 0 else 0.0
    d1_ret = float(d1["ret"].mean()) if len(d1) else np.nan
    dn_ret = float(dn["ret"].mean()) if len(dn) else np.nan
    # dollar-weighted spread
    d1_ret_d = _wmean(d1["ret"], d1["usdc"]) if len(d1) else np.nan
    dn_ret_d = _wmean(dn["ret"], dn["usdc"]) if len(dn) else np.nan
    spread_d = (dn_ret_d - d1_ret_d) if (len(d1) and len(dn)) else np.nan
    se1_d = (threeway_cluster_se_weighted(d1["ret"], d1["usdc"], d1["trade_day"],
                                          d1["proxyWallet"], d1["market_id"]) if len(d1) else 0)
    sen_d = (threeway_cluster_se_weighted(dn["ret"], dn["usdc"], dn["trade_day"],
                                          dn["proxyWallet"], dn["market_id"]) if len(dn) else 0)
    spread_se_d = float(np.sqrt(se1_d**2 + sen_d**2))
    spread_t_d = spread_d / spread_se_d if spread_se_d > 0 else 0.0
    return result, {
        "spread": float(spread) if np.isfinite(spread) else np.nan,
        "spread_se": spread_se, "spread_t": spread_t,
        "d1_ret": d1_ret, "d10_ret": dn_ret,
        "spread_dol": float(spread_d) if np.isfinite(spread_d) else np.nan,
        "spread_se_dol": spread_se_d, "spread_t_dol": spread_t_d,
        "d1_ret_dol": d1_ret_d, "d10_ret_dol": dn_ret_d,
        "n": int(len(df)),
    }


# ---------- per-dim trade-level builder ----------

TRADE_LEVEL_SQL_TEMPLATE = """
WITH mkt_life AS (
    SELECT conditionId,
           MIN(timestamp) AS mkt_start,
           GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS mkt_duration
    FROM trades_buy
    GROUP BY conditionId
)
SELECT
    t.proxyWallet,
    t.conditionId AS token_id,
    cd.condition_id AS market_id,
    t.price,
    CAST(t.outcome = c.winning_outcome AS INT) AS won,
    CASE WHEN t.outcome = c.winning_outcome
         THEN 1.0 - t.price ELSE -t.price END AS ret,
    t.usdcSize AS usdc,
    DATE_TRUNC('day',   to_timestamp(t.timestamp)) AS trade_day,
    DATE_TRUNC('month', to_timestamp(t.timestamp)) AS trade_month,
    LEAST(FLOOR(t.price * 10)::INT, 9) + 1 AS decile,
    cd.{dim_col} AS dim_slice
FROM trades_buy t
INNER JOIN _closed_markets c ON t.conditionId = c.conditionId
INNER JOIN mkt_life ml ON t.conditionId = ml.conditionId
INNER JOIN _contract_dims cd ON t.conditionId = cd.token_id
WHERE t.price > 0.01 AND t.price < 0.99
  AND (t.timestamp - ml.mkt_start)::FLOAT / ml.mkt_duration BETWEEN {lo} AND {hi}
  AND cd.{dim_col} IS NOT NULL
"""


def fetch_trade_level_for_dim(con, dim_col, lo=0.0, hi=1.0):
    sql = TRADE_LEVEL_SQL_TEMPLATE.format(dim_col=dim_col, lo=lo, hi=hi)
    return con.execute(sql).fetchdf()


def run_flb_per_slice(con, dim_col, lo=0.0, hi=1.0, min_trades=5000,
                      n_bins=10, verbose=True):
    """Returns (calib_long_df, summary_df).

    calib_long_df has one row per (dim, slice, decile) with cal_error, se_3w, t_3w, n_trades.
    summary_df has one row per (dim, slice) with spread + 3w SE/t + N counts.
    """
    df = fetch_trade_level_for_dim(con, dim_col, lo=lo, hi=hi)
    if verbose:
        print(f"  [{dim_col}] fetched {len(df):,} trades across "
              f"{df['dim_slice'].nunique():,} slices", flush=True)
    if len(df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    out_calib = []
    out_summary = []
    for slc, sub in df.groupby("dim_slice", sort=False):
        if len(sub) < min_trades:
            if verbose:
                print(f"    {slc}: SKIP ({len(sub):,} trades < {min_trades})", flush=True)
            continue
        r3, s3 = compute_3way_decile_table(sub, n_bins=n_bins)

        for _, row in r3.iterrows():
            out_calib.append({
                "dim": dim_col, "slice": str(slc), "decile": int(row["decile"]),
                "impl_prob": row["impl_prob"], "win_rate": row["win_rate"],
                "cal_error": row["cal_error"], "se_3w": row["se_3w"],
                "t_3w": row["t_3w"], "n_trades_bin": int(row["n"]),
                "impl_prob_dol": row["impl_prob_dol"], "win_rate_dol": row["win_rate_dol"],
                "cal_error_dol": row["cal_error_dol"], "se_3w_dol": row["se_3w_dol"],
                "t_3w_dol": row["t_3w_dol"], "usd_bin": float(row["usd"]),
            })

        out_summary.append({
            "dim": dim_col, "slice": str(slc),
            "n_trades": int(len(sub)),
            "n_contracts": int(sub["token_id"].nunique()),
            "n_markets": int(sub["market_id"].nunique()),
            "total_usd": float(sub["usdc"].sum()),
            "spread_3w": s3["spread"], "se_3w": s3["spread_se"], "t_3w": s3["spread_t"],
            "d1_ret_3w": s3["d1_ret"], "d10_ret_3w": s3["d10_ret"],
            "spread_dol": s3["spread_dol"], "se_dol": s3["spread_se_dol"], "t_dol": s3["spread_t_dol"],
            "d1_ret_dol": s3["d1_ret_dol"], "d10_ret_dol": s3["d10_ret_dol"],
        })
        if verbose:
            star = sig_stars(s3["spread_t"])
            print(f"    {str(slc)[:35]:35s} N={len(sub):>10,}  "
                  f"D1={s3['d1_ret']:+.4f}  D10={s3['d10_ret']:+.4f}  "
                  f"spread={s3['spread']:+.4f} (SE={s3['spread_se']:.4f}, "
                  f"t={s3['spread_t']:+.2f}{star})", flush=True)
    return pd.DataFrame(out_calib), pd.DataFrame(out_summary)


ALL_PHASE1_DIMS = [
    "dim_resolution_type",
    "dim_info_type_supergroup",
    "dim_primary_category",
    "dim_subject_specificity",
    "dim_event_family_size",
    "dim_outcomes_per_event",
    "dim_market_specificity",
    "dim_dollar_volume_tier",
    "dim_contract_horizon",
    "dim_recurrence_class",
]
