"""FLB measurement engine for the embedding-difficulty workstream.

Operates on the compact base tables from build_flb_base.py plus a
market_id -> slice map. Measurement follows the project spec
(docs/methods_reference.md):
  - 10 price deciles per slice; count- AND dollar-weighted stats
  - D10 - D1 calibration-error spread (secondary)
  - SIGNED CALIBRATION SLOPE (primary): per-slice OLS of ret on price,
    ret = won - price, so slope > 0 <=> classic FLB direction
    (longshots overpriced / favorites underpriced); slope = 0 <=> calibrated.
    Implemented count-weighted (OLS) and dollar-weighted (WLS, w = usdc).
  - Cameron-Gelbach-Miller 3-way clustered SEs (day x wallet x market) for
    decile means, spread, and slope. CGM helpers match the canonical engine
    (analysis/learnability/flb_per_slice.py) exactly; the slope SE applies the
    same inclusion-exclusion to the OLS/WLS scores z_i = w_i * xtilde_i * e_i
    with normalizer sum(w * xtilde^2).
  - Slice floor: 5,000 trades (caller can override); dropped slices reported.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ---------- CGM clustered variance core ----------

def _cgm_var(scores: np.ndarray, c1, c2, c3) -> float:
    """CGM inclusion-exclusion variance of sum(scores) over 3 cluster dims.

    Returns Var = V1+V2+V3-V12-V13-V23+V123 where Vg = sum over groups of
    (within-group score sum)^2. Caller divides by its own normalizer^2.
    """
    n = len(scores)
    if n == 0:
        return 0.0
    s = pd.Series(scores)

    def v(codes):
        return float((s.groupby(codes).sum() ** 2).sum())

    c1c = pd.factorize(c1, sort=False)[0].astype(np.int64)
    c2c = pd.factorize(c2, sort=False)[0].astype(np.int64)
    c3c = pd.factorize(c3, sort=False)[0].astype(np.int64)
    n2 = int(c2c.max()) + 1
    n3 = int(c3c.max()) + 1
    c12 = c1c * n2 + c2c
    c13 = c1c * n3 + c3c
    c23 = c2c * n3 + c3c
    c123 = c12 * n3 + c3c
    var = (v(c1c) + v(c2c) + v(c3c) - v(c12) - v(c13) - v(c23) + v(c123))
    return max(var, 0.0)


def cluster_se_mean(ret, c1, c2, c3, weights=None) -> float:
    """3-way clustered SE of the (weighted) mean of ret."""
    r = np.asarray(ret, float)
    n = len(r)
    if n == 0:
        return 0.0
    if weights is None:
        scores = r - r.mean()
        norm = float(n)
    else:
        w = np.asarray(weights, float)
        W = w.sum()
        if W <= 0:
            return 0.0
        theta = (w * r).sum() / W
        scores = w * (r - theta)
        norm = W
    return float(np.sqrt(_cgm_var(scores, c1, c2, c3)) / norm)


def slope_and_se(price, ret, c1, c2, c3, weights=None):
    """(Weighted) OLS slope of ret on price with 3-way clustered SE."""
    x = np.asarray(price, float)
    y = np.asarray(ret, float)
    n = len(x)
    if n < 2:
        return np.nan, np.nan
    w = np.ones(n) if weights is None else np.asarray(weights, float)
    W = w.sum()
    if W <= 0:
        return np.nan, np.nan
    xbar = (w * x).sum() / W
    ybar = (w * y).sum() / W
    xt = x - xbar
    sxx = (w * xt * xt).sum()
    if sxx <= 0:
        return np.nan, np.nan
    b = (w * xt * (y - ybar)).sum() / sxx
    a = ybar - b * xbar
    e = y - a - b * x
    scores = w * xt * e
    se = float(np.sqrt(_cgm_var(scores, c1, c2, c3)) / sxx)
    return float(b), se


def sig_stars(t):
    if not np.isfinite(t):
        return ""
    a = abs(t)
    if a > 3.29:
        return "***"
    if a > 2.58:
        return "**"
    if a > 1.96:
        return "*"
    return ""


# ---------- per-slice computation ----------

def compute_slice(sub: pd.DataFrame, n_bins: int = 10):
    """sub: trade-level frame with price, ret, won, usdc, day, wallet_code,
    market_code (one slice). Returns (decile_rows list, summary dict)."""
    cl = (sub["day"], sub["wallet_code"], sub["market_code"])
    dec_rows = []
    for d in range(1, n_bins + 1):
        s = sub[sub["decile"] == d]
        if len(s) < 50:
            dec_rows.append({"decile": d, "n": len(s), "usd": float(s["usdc"].sum()),
                             "impl_prob": np.nan, "win_rate": np.nan,
                             "cal_error": np.nan, "se": np.nan,
                             "impl_prob_dol": np.nan, "win_rate_dol": np.nan,
                             "cal_error_dol": np.nan, "se_dol": np.nan})
            continue
        scl = (s["day"], s["wallet_code"], s["market_code"])
        w = s["usdc"].to_numpy(float)
        r = s["ret"].to_numpy(float)
        W = w.sum()
        dec_rows.append({
            "decile": d, "n": int(len(s)), "usd": float(W),
            "impl_prob": float(s["price"].mean()),
            "win_rate": float(s["won"].mean()),
            "cal_error": float(r.mean()),
            "se": cluster_se_mean(r, *scl),
            "impl_prob_dol": float((w * s["price"].to_numpy(float)).sum() / W),
            "win_rate_dol": float((w * s["won"].to_numpy(float)).sum() / W),
            "cal_error_dol": float((w * r).sum() / W),
            "se_dol": cluster_se_mean(r, *scl, weights=w),
        })

    # D10 - D1 spread (secondary summary)
    d1 = sub[sub["decile"] == 1]
    dn = sub[sub["decile"] == n_bins]

    def _spread(weighted: bool):
        if not (len(d1) and len(dn)):
            return np.nan, np.nan
        if weighted:
            m1 = (d1["usdc"] * d1["ret"]).sum() / d1["usdc"].sum()
            mn = (dn["usdc"] * dn["ret"]).sum() / dn["usdc"].sum()
            s1 = cluster_se_mean(d1["ret"], d1["day"], d1["wallet_code"],
                                 d1["market_code"], weights=d1["usdc"])
            sn = cluster_se_mean(dn["ret"], dn["day"], dn["wallet_code"],
                                 dn["market_code"], weights=dn["usdc"])
        else:
            m1, mn = d1["ret"].mean(), dn["ret"].mean()
            s1 = cluster_se_mean(d1["ret"], d1["day"], d1["wallet_code"], d1["market_code"])
            sn = cluster_se_mean(dn["ret"], dn["day"], dn["wallet_code"], dn["market_code"])
        return float(mn - m1), float(np.sqrt(s1 ** 2 + sn ** 2))

    spread, spread_se = _spread(False)
    spread_d, spread_se_d = _spread(True)

    # signed slope (primary)
    slope, slope_se = slope_and_se(sub["price"], sub["ret"], *cl)
    slope_d, slope_se_d = slope_and_se(sub["price"], sub["ret"], *cl,
                                       weights=sub["usdc"])

    summary = {
        "n_trades": int(len(sub)),
        "n_contracts": int(sub["token_code"].nunique()) if "token_code" in sub else np.nan,
        "n_markets": int(sub["market_code"].nunique()),
        "total_usd": float(sub["usdc"].sum()),
        "slope": slope, "slope_se": slope_se,
        "slope_t": slope / slope_se if slope_se and slope_se > 0 else np.nan,
        "slope_dol": slope_d, "slope_se_dol": slope_se_d,
        "slope_t_dol": slope_d / slope_se_d if slope_se_d and slope_se_d > 0 else np.nan,
        "spread": spread, "spread_se": spread_se,
        "spread_t": spread / spread_se if spread_se and spread_se > 0 else np.nan,
        "spread_dol": spread_d, "spread_se_dol": spread_se_d,
        "spread_t_dol": spread_d / spread_se_d if spread_se_d and spread_se_d > 0 else np.nan,
    }
    return dec_rows, summary


def run_scheme(con, base_parquet: str, slice_map: pd.DataFrame, scheme: str,
               min_trades: int = 5000, n_bins: int = 10, verbose: bool = True):
    """Run FLB per slice for one slicing scheme.

    slice_map: DataFrame[market_code:int, slice] (slice may be str or int).
    Returns (deciles_df, summary_df, dropped_df).
    """
    con.register("_slice_map", slice_map[["market_code", "slice"]])
    df = con.execute(f"""
        SELECT b.*, LEAST(FLOOR(b.price * 10)::INT, 9) + 1 AS decile, m.slice
        FROM read_parquet('{base_parquet}') b
        JOIN _slice_map m USING (market_code)
    """).fetchdf()
    con.unregister("_slice_map")
    if verbose:
        print(f"  [{scheme}] {len(df):,} trades, {df['slice'].nunique()} slices",
              flush=True)

    out_dec, out_sum, dropped = [], [], []
    for slc, sub in df.groupby("slice", sort=True):
        if len(sub) < min_trades:
            dropped.append({"scheme": scheme, "slice": str(slc), "n_trades": int(len(sub))})
            continue
        dec_rows, summary = compute_slice(sub, n_bins=n_bins)
        for r in dec_rows:
            out_dec.append({"scheme": scheme, "slice": str(slc), **r})
        out_sum.append({"scheme": scheme, "slice": str(slc), **summary})
        if verbose:
            s = out_sum[-1]
            print(f"    {str(slc)[:40]:40s} N={s['n_trades']:>11,} "
                  f"slope={s['slope']:+.4f} (t={s['slope_t']:+.2f}"
                  f"{sig_stars(s['slope_t'])}) spread={s['spread']:+.4f}"
                  f"(t={s['spread_t']:+.2f})", flush=True)
    return pd.DataFrame(out_dec), pd.DataFrame(out_sum), pd.DataFrame(dropped)
