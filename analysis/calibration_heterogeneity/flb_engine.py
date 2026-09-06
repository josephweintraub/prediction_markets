"""FLB measurement engine for the calibration-heterogeneity workstream.

Operates on the compact base tables from build_flb_base.py plus a
market_id -> slice map. Measurement follows the project spec
(docs/methods_reference.md):
  - 10 price deciles per slice; count- AND dollar-weighted stats
  - D1 and D10 tail errors plus their D10 - D1 spread (headline summaries)
  - Signed calibration slope (auxiliary): per-slice OLS of ret on price,
    ret = won - price, so slope > 0 <=> classic FLB direction
    (longshots overpriced / favorites underpriced); slope = 0 <=> calibrated.
    Implemented count-weighted (OLS) and dollar-weighted (WLS, w = usdc).
  - Cameron-Gelbach-Miller 3-way clustered SEs (day x wallet x market) for
    decile means, spread, and slope. CGM helpers match the canonical engine
    (archived v7 learnability engine) exactly; the slope SE applies the
    same inclusion-exclusion to the OLS/WLS scores z_i = w_i * xtilde_i * e_i
    with normalizer sum(w * xtilde^2).
  - Slice floor: 5,000 trades (caller can override); dropped slices reported.
"""
from __future__ import annotations

import math

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


def cluster_se_difference(
    low_ret,
    high_ret,
    low_c1,
    low_c2,
    low_c3,
    high_c1,
    high_c2,
    high_c3,
    low_weights=None,
    high_weights=None,
) -> float:
    """Three-way clustered SE for ``mean(high_ret) - mean(low_ret)``.

    The two means share day, wallet, and market clusters. Computing their standard errors
    separately and adding their variances assumes zero covariance and is generally wrong.
    This function stacks the two groups' influence scores before applying the CGM
    inclusion-exclusion calculation, preserving that covariance.
    """
    low = np.asarray(low_ret, float)
    high = np.asarray(high_ret, float)
    if len(low) == 0 or len(high) == 0:
        return np.nan

    low_w = np.ones(len(low)) if low_weights is None else np.asarray(low_weights, float)
    high_w = (
        np.ones(len(high)) if high_weights is None else np.asarray(high_weights, float)
    )
    low_norm = float(low_w.sum())
    high_norm = float(high_w.sum())
    if low_norm <= 0 or high_norm <= 0:
        return np.nan

    low_mean = float((low_w * low).sum() / low_norm)
    high_mean = float((high_w * high).sum() / high_norm)
    scores = np.concatenate(
        [
            -low_w * (low - low_mean) / low_norm,
            high_w * (high - high_mean) / high_norm,
        ]
    )
    c1 = np.concatenate([np.asarray(low_c1), np.asarray(high_c1)])
    c2 = np.concatenate([np.asarray(low_c2), np.asarray(high_c2)])
    c3 = np.concatenate([np.asarray(low_c3), np.asarray(high_c3)])
    return float(np.sqrt(_cgm_var(scores, c1, c2, c3)))


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


def equal_market_weights(frame: pd.DataFrame) -> np.ndarray:
    """Trade weights for an equal-market average of within-market dollar VWAPs.

    Within each market, trades retain their economic-size weights. Each market then
    contributes total weight one, irrespective of its trade count or total volume.
    """
    market_usd = frame.groupby("market_code", sort=False)["usdc"].transform("sum")
    denominator = market_usd.to_numpy(float)
    weights = np.divide(
        frame["usdc"].to_numpy(float),
        denominator,
        out=np.zeros(len(frame), dtype=float),
        where=denominator > 0,
    )
    return weights


def two_sided_p_value(t: float) -> float:
    """Normal-approximation two-sided p-value for a t/z statistic."""
    if not np.isfinite(t):
        return np.nan
    return float(math.erfc(abs(float(t)) / math.sqrt(2.0)))


def adjust_pvalues(values, method: str) -> np.ndarray:
    """Adjust finite p-values using Bonferroni or Benjamini-Hochberg FDR."""
    p = np.asarray(values, dtype=float)
    adjusted = np.full(len(p), np.nan, dtype=float)
    valid = np.flatnonzero(np.isfinite(p))
    if len(valid) == 0:
        return adjusted
    pv = np.clip(p[valid], 0.0, 1.0)
    if method == "bonferroni":
        adjusted[valid] = np.minimum(pv * len(pv), 1.0)
    elif method == "fdr_bh":
        order = np.argsort(pv)
        ranked = pv[order]
        corrected = ranked * len(ranked) / np.arange(1, len(ranked) + 1)
        corrected = np.minimum.accumulate(corrected[::-1])[::-1]
        restored = np.empty(len(ranked), dtype=float)
        restored[order] = np.minimum(corrected, 1.0)
        adjusted[valid] = restored
    else:
        raise ValueError(f"Unknown p-value adjustment method: {method}")
    return adjusted


def add_multiple_testing_columns(
    deciles: pd.DataFrame, summaries: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add raw and adjusted p-values within each scheme/effect family.

    Decile families contain every slice x decile cell for one weighting. Summary
    families contain every slice for one estimand and weighting. Both Bonferroni and
    Benjamini-Hochberg values are retained; exploratory displays should use Bonferroni.
    """
    deciles = deciles.copy()
    summaries = summaries.copy()

    decile_effects = [
        ("cal_error", "se", "cal"),
        ("cal_error_dol", "se_dol", "cal_dol"),
        ("cal_error_mkt", "se_mkt", "cal_mkt"),
    ]
    summary_effects = [
        ("slope", "slope_se", "slope"),
        ("slope_dol", "slope_se_dol", "slope_dol"),
        ("spread", "spread_se", "spread"),
        ("spread_dol", "spread_se_dol", "spread_dol"),
        ("spread_mkt", "spread_se_mkt", "spread_mkt"),
    ]

    def add(frame: pd.DataFrame, effects) -> pd.DataFrame:
        if frame.empty:
            return frame
        groups = frame.groupby("scheme", sort=False).groups
        for estimate, se, prefix in effects:
            denom = frame[se].to_numpy(float)
            numer = frame[estimate].to_numpy(float)
            t = np.divide(
                numer,
                denom,
                out=np.full(len(frame), np.nan),
                where=np.isfinite(denom) & (denom > 0),
            )
            frame[f"{prefix}_t"] = t
            frame[f"{prefix}_p"] = [two_sided_p_value(value) for value in t]
            frame[f"{prefix}_p_bonferroni"] = np.nan
            frame[f"{prefix}_p_fdr_bh"] = np.nan
            for indices in groups.values():
                idx = np.asarray(indices, dtype=int)
                raw = frame.loc[idx, f"{prefix}_p"].to_numpy(float)
                frame.loc[idx, f"{prefix}_p_bonferroni"] = adjust_pvalues(
                    raw, "bonferroni"
                )
                frame.loc[idx, f"{prefix}_p_fdr_bh"] = adjust_pvalues(raw, "fdr_bh")
        return frame

    return add(deciles, decile_effects), add(summaries, summary_effects)


# ---------- per-slice computation ----------

def compute_slice(sub: pd.DataFrame, n_bins: int = 10, min_decile_trades: int = 50):
    """sub: trade-level frame with price, ret, won, usdc, day, wallet_code,
    market_code (one slice). Returns (decile_rows list, summary dict)."""
    cl = (sub["day"], sub["wallet_code"], sub["market_code"])
    dec_rows = []
    for d in range(1, n_bins + 1):
        s = sub[sub["decile"] == d]
        if len(s) < min_decile_trades:
            dec_rows.append({"decile": d, "n": len(s), "usd": float(s["usdc"].sum()),
                             "n_markets": int(s["market_code"].nunique()),
                             "impl_prob": np.nan, "win_rate": np.nan,
                             "cal_error": np.nan, "se": np.nan,
                             "impl_prob_dol": np.nan, "win_rate_dol": np.nan,
                             "cal_error_dol": np.nan, "se_dol": np.nan,
                             "impl_prob_mkt": np.nan, "win_rate_mkt": np.nan,
                             "cal_error_mkt": np.nan, "se_mkt": np.nan})
            continue
        scl = (s["day"], s["wallet_code"], s["market_code"])
        w = s["usdc"].to_numpy(float)
        w_mkt = equal_market_weights(s)
        r = s["ret"].to_numpy(float)
        W = w.sum()
        W_mkt = w_mkt.sum()
        dec_rows.append({
            "decile": d, "n": int(len(s)), "usd": float(W),
            "n_markets": int((s.groupby("market_code")["usdc"].sum() > 0).sum()),
            "impl_prob": float(s["price"].mean()),
            "win_rate": float(s["won"].mean()),
            "cal_error": float(r.mean()),
            "se": cluster_se_mean(r, *scl),
            "impl_prob_dol": float((w * s["price"].to_numpy(float)).sum() / W),
            "win_rate_dol": float((w * s["won"].to_numpy(float)).sum() / W),
            "cal_error_dol": float((w * r).sum() / W),
            "se_dol": cluster_se_mean(r, *scl, weights=w),
            "impl_prob_mkt": float(
                (w_mkt * s["price"].to_numpy(float)).sum() / W_mkt
            ),
            "win_rate_mkt": float(
                (w_mkt * s["won"].to_numpy(float)).sum() / W_mkt
            ),
            "cal_error_mkt": float((w_mkt * r).sum() / W_mkt),
            "se_mkt": cluster_se_mean(r, *scl, weights=w_mkt),
        })

    # D10 - D1 spread. Apply the same tail floor used by the decile table and estimate
    # its clustered variance jointly so shared-cluster covariance is retained.
    d1 = sub[sub["decile"] == 1]
    dn = sub[sub["decile"] == n_bins]

    def _spread(weighted: bool):
        if len(d1) < min_decile_trades or len(dn) < min_decile_trades:
            return np.nan, np.nan
        if weighted:
            m1 = (d1["usdc"] * d1["ret"]).sum() / d1["usdc"].sum()
            mn = (dn["usdc"] * dn["ret"]).sum() / dn["usdc"].sum()
            se = cluster_se_difference(
                d1["ret"], dn["ret"],
                d1["day"], d1["wallet_code"], d1["market_code"],
                dn["day"], dn["wallet_code"], dn["market_code"],
                low_weights=d1["usdc"], high_weights=dn["usdc"],
            )
        else:
            m1, mn = d1["ret"].mean(), dn["ret"].mean()
            se = cluster_se_difference(
                d1["ret"], dn["ret"],
                d1["day"], d1["wallet_code"], d1["market_code"],
                dn["day"], dn["wallet_code"], dn["market_code"],
            )
        return float(mn - m1), se

    def _spread_equal_market():
        if len(d1) < min_decile_trades or len(dn) < min_decile_trades:
            return np.nan, np.nan
        w1 = equal_market_weights(d1)
        wn = equal_market_weights(dn)
        if w1.sum() <= 0 or wn.sum() <= 0:
            return np.nan, np.nan
        m1 = np.average(d1["ret"].to_numpy(float), weights=w1)
        mn = np.average(dn["ret"].to_numpy(float), weights=wn)
        se = cluster_se_difference(
            d1["ret"], dn["ret"],
            d1["day"], d1["wallet_code"], d1["market_code"],
            dn["day"], dn["wallet_code"], dn["market_code"],
            low_weights=w1, high_weights=wn,
        )
        return float(mn - m1), se

    spread, spread_se = _spread(False)
    spread_d, spread_se_d = _spread(True)
    spread_m, spread_se_m = _spread_equal_market()

    # Signed slope (auxiliary summary).
    slope, slope_se = slope_and_se(sub["price"], sub["ret"], *cl)
    slope_d, slope_se_d = slope_and_se(sub["price"], sub["ret"], *cl,
                                       weights=sub["usdc"])

    summary = {
        "n_trades": int(len(sub)),
        "n_contracts": int(sub["token_code"].nunique()) if "token_code" in sub else np.nan,
        "n_markets": int(sub["market_code"].nunique()),
        "total_usd": float(sub["usdc"].sum()),
        "d1_n": int(len(d1)), "d10_n": int(len(dn)),
        "d1_markets": int(d1["market_code"].nunique()),
        "d10_markets": int(dn["market_code"].nunique()),
        "d1_usd": float(d1["usdc"].sum()), "d10_usd": float(dn["usdc"].sum()),
        "slope": slope, "slope_se": slope_se,
        "slope_t": slope / slope_se if slope_se and slope_se > 0 else np.nan,
        "slope_dol": slope_d, "slope_se_dol": slope_se_d,
        "slope_t_dol": slope_d / slope_se_d if slope_se_d and slope_se_d > 0 else np.nan,
        "spread": spread, "spread_se": spread_se,
        "spread_t": spread / spread_se if spread_se and spread_se > 0 else np.nan,
        "spread_dol": spread_d, "spread_se_dol": spread_se_d,
        "spread_t_dol": spread_d / spread_se_d if spread_se_d and spread_se_d > 0 else np.nan,
        "spread_mkt": spread_m, "spread_se_mkt": spread_se_m,
        "spread_t_mkt": spread_m / spread_se_m if spread_se_m and spread_se_m > 0 else np.nan,
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
        SELECT b.*, LEAST(FLOOR(b.price * {n_bins})::INT, {n_bins - 1}) + 1 AS decile,
               m.slice
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
