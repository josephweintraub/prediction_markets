"""Horse race v1 — joint calibration model over the learnability dimensions.

The question every one-dimension-at-a-time slice left open: which market
characteristics carry INDEPENDENT calibration signal once the others are held
fixed? Model (linear probability calibration form):

    y_i = won_i - p_i = FE_g + FE_g * p_i + sum_d e_d * x_d,m
                        + sum_d c_d * (p_i * x_d,m) + eps_i

Absorbing FE_g and FE_g*p (per-group [1, p] projection, Frisch-Waugh) makes
c_d the WITHIN-GROUP calibration-slope tilt per unit of dimension d,
conditional on all other dimensions. c_d > 0 = classic-FLB direction.

Pre-specified dimensions (market-level; z = z-score over estimation-sample
markets, each market weighted once):
    z_ln_life   ln(market lifetime, days)          horizon channel
    z_ln_usd    ln(1 + std-filtered $ volume)      liquidity channel
    nov_tail    within-vintage novelty decile 1    difficulty channel (primary)
    anchored    resolution source is a data feed or official scorer
    z_ln_prior  ln(1 + earlier instances in same series)   repetition channel
    z_ln_rules  ln(1 + rules text length)          complexity channel
    neg_risk    multi-outcome negRisk market       complexity channel
    z_vintage   birth year                          era control

FE variants: none / topic / cluster_k200 (headline). Weights: count / dollar.
Robustness: z_nov (continuous -sim_k25_x) swapped for nov_tail, cluster FE.
SEs: Cameron-Gelbach-Miller 3-way clustered (day x wallet x market) on every
coefficient. Window: mature (25-80% of token lifetime). Also produced here:
dimension correlation/VIF table, within-cluster identification shares,
univariate quintile gradients per dimension (raw-vs-conditional contrast),
the within-series test (series FE, slope drift on ln instances), and the
dollar-sizing table. Outputs land in /mnt/data/learnability/output/.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from horizon_flb_v2 import threeway_cluster_slope  # noqa: E402

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT_DIR = Path("/mnt/data/learnability/output")

DIM_COLS = ["z_ln_life", "z_ln_usd", "nov_tail", "anchored",
            "z_ln_prior", "z_ln_rules", "neg_risk", "z_vintage"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------- FWL absorption + clustered OLS ----------------

def absorb_group_1p(code, n_groups, p, w, cols):
    """Residualize each column in `cols` (dict name->array) on [1, p] within
    group, weighted by w. Returns dict of residualized arrays."""
    Sw = np.bincount(code, weights=w, minlength=n_groups)
    Swp = np.bincount(code, weights=w * p, minlength=n_groups)
    Swpp = np.bincount(code, weights=w * p * p, minlength=n_groups)
    det = Sw * Swpp - Swp ** 2
    ok = det > 1e-12 * np.maximum(Sw * Swpp, 1e-300)
    out = {}
    for name, v in cols.items():
        Swv = np.bincount(code, weights=w * v, minlength=n_groups)
        Swpv = np.bincount(code, weights=w * p * v, minlength=n_groups)
        b1 = np.where(ok, (Sw * Swpv - Swp * Swv) / np.where(det == 0, 1, det), 0.0)
        b0 = np.where(Sw > 0, (Swv - Swp * b1) / np.where(Sw == 0, 1, Sw), 0.0)
        out[name] = v - b0[code] - b1[code] * p
    return out


def cgm_meat(U, c1, c2, c3, n1, n2, n3):
    """Sum over the 7 CGM cluster structures of sum_g s_g s_g^T for score
    matrix U (n x K)."""
    K = U.shape[1]

    def S(code, G):
        acc = np.zeros((G, K))
        for k in range(K):
            acc[:, k] = np.bincount(code, weights=U[:, k], minlength=G)
        return acc.T @ acc

    c12 = c1.astype(np.int64) * n2 + c2
    c13 = c1.astype(np.int64) * n3 + c3
    c23 = c2.astype(np.int64) * n3 + c3
    c123 = c12 * n3 + c3
    # re-factorize composites to keep bincount arrays small
    def refac(x):
        codes, uniq = pd.factorize(x, sort=False)
        return codes.astype(np.int64), len(uniq)
    r12, g12 = refac(c12); r13, g13 = refac(c13)
    r23, g23 = refac(c23); r123, g123 = refac(c123)
    return (S(c1, n1) + S(c2, n2) + S(c3, n3)
            - S(r12, g12) - S(r13, g13) - S(r23, g23) + S(r123, g123))


def joint_model(df, fe_col, weights, dim_cols, label):
    """Absorb per-FE [1,p]; OLS of y~ on [x_d~, (p*x_d)~]; CGM 3-way SEs."""
    n = len(df)
    p = df["price"].to_numpy(np.float64)
    y = (df["won"].to_numpy(np.float64) - p)
    w = np.ones(n) if weights is None else df[weights].to_numpy(np.float64)

    code, uniq = pd.factorize(df[fe_col], sort=False)
    code = code.astype(np.int64)
    cols = {}
    for d in dim_cols:
        x = df[d].to_numpy(np.float64)
        cols[d] = x
        cols["px_" + d] = p * x
    cols["_y"] = y
    t0 = time.time()
    res = absorb_group_1p(code, len(uniq), p, w, cols)
    yt = res.pop("_y")
    names = list(res.keys())
    X = np.column_stack([res[k] for k in names])
    A = (X * w[:, None]).T @ X
    b = np.linalg.solve(A, (X * w[:, None]).T @ yt)
    e = yt - X @ b
    U = X * (w * e)[:, None]

    d1, _ = pd.factorize(df["trade_day"], sort=False)
    d2, _ = pd.factorize(df["proxyWallet"], sort=False)
    d3, _ = pd.factorize(df["market_id"], sort=False)
    meat = cgm_meat(U, d1.astype(np.int64), d2.astype(np.int64),
                    d3.astype(np.int64), d1.max() + 1, d2.max() + 1, d3.max() + 1)
    Ainv = np.linalg.inv(A)
    V = Ainv @ meat @ Ainv
    se = np.sqrt(np.maximum(np.diag(V), 0))
    rows = []
    for i, nm in enumerate(names):
        rows.append({"spec": label, "term": nm, "beta": b[i], "se": se[i],
                     "t": b[i] / se[i] if se[i] > 0 else np.nan})
    log(f"  {label}: n={n:,}, groups={len(uniq)}, "
        f"{time.time()-t0:.0f}s; slope-tilt terms:")
    for r in rows:
        if r["term"].startswith("px_"):
            log(f"    {r['term'][3:]:12s} c={r['beta']:+.4f} (t={r['t']:+.2f})")
    return pd.DataFrame(rows)


# ---------------- main ----------------

def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

    log("loading mature trades joined to dimensions ...")
    df = con.execute(f"""
        SELECT t.price, t.won, t.usdc, t.trade_day, t.proxyWallet, t.market_id,
               coalesce(d.topic, 'UNKNOWN') AS topic,
               d.cluster_k200,
               d.life_d, d.usd_full, d.sim_k25_x, d.novelty_vint_decile,
               d.anchor_class, d.prior_instances, d.in_series,
               coalesce(d.rules_len, 0) AS rules_len,
               coalesce(d.neg_risk, FALSE) AS neg_risk,
               d.vintage_year, d.series_slug
        FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
        JOIN read_parquet('{DIMS}') d ON t.market_id = d.condition_id
        WHERE t.pos BETWEEN 0.25 AND 0.80
          AND d.sim_k25_x IS NOT NULL AND d.cluster_k200 IS NOT NULL
          AND d.life_d IS NOT NULL AND d.vintage_year IS NOT NULL
    """).fetchdf()
    log(f"estimation sample: {len(df):,} trades, "
        f"{df['market_id'].nunique():,} markets")

    # market-level standardization (each market weighted once)
    mk = (df.groupby("market_id", observed=True)
            .agg(life_d=("life_d", "first"), usd_full=("usd_full", "first"),
                 sim=("sim_k25_x", "first"), dec=("novelty_vint_decile", "first"),
                 anch=("anchor_class", "first"), prior=("prior_instances", "first"),
                 rules=("rules_len", "first"), neg=("neg_risk", "first"),
                 vint=("vintage_year", "first")))
    raw = pd.DataFrame({
        "ln_life": np.log(np.maximum(mk["life_d"], 1e-5)),
        "ln_usd": np.log1p(mk["usd_full"]),
        "nov_tail": (mk["dec"] == 1).astype(float),
        "z_nov_raw": -mk["sim"],
        "anchored": mk["anch"].isin(["data_feed", "official_scorer"]).astype(float),
        "ln_prior": np.log1p(mk["prior"]),
        "ln_rules": np.log1p(mk["rules"]),
        "neg_risk": mk["neg"].astype(float),
        "vintage": mk["vint"].astype(float),
    }, index=mk.index)
    zmap = {"z_ln_life": "ln_life", "z_ln_usd": "ln_usd", "z_ln_prior": "ln_prior",
            "z_ln_rules": "ln_rules", "z_vintage": "vintage", "z_nov": "z_nov_raw"}
    mdims = pd.DataFrame(index=raw.index)
    for z, rcol in zmap.items():
        v = raw[rcol]
        mdims[z] = (v - v.mean()) / v.std()
    for c in ["nov_tail", "anchored", "neg_risk"]:
        mdims[c] = raw[c]

    # correlation + VIF (market level)
    allc = DIM_COLS + ["z_nov"]
    corr = mdims[allc].corr()
    Ci = np.linalg.inv(mdims[DIM_COLS].corr().to_numpy())
    vif = pd.DataFrame({"dim": DIM_COLS, "vif": np.diag(Ci)})
    corr.to_parquet(OUT_DIR / "horse_race_v1_corr.parquet")
    vif.to_parquet(OUT_DIR / "horse_race_v1_vif.parquet")
    log("VIF:\n" + vif.to_string())

    # within-FE identification share (market level)
    mk_cl = df.groupby("market_id", observed=True)["cluster_k200"].first()
    mk_tp = df.groupby("market_id", observed=True)["topic"].first()
    wv = []
    for fe_name, fe in [("cluster_k200", mk_cl), ("topic", mk_tp)]:
        for d in DIM_COLS:
            tot = mdims[d].var()
            within = mdims[d].groupby(fe).transform(lambda s: s - s.mean()).var()
            wv.append({"fe": fe_name, "dim": d,
                       "within_share": within / tot if tot > 0 else np.nan})
    pd.DataFrame(wv).to_parquet(OUT_DIR / "horse_race_v1_withinvar.parquet")

    # attach dims to trades
    for c in DIM_COLS + ["z_nov"]:
        df[c] = mdims[c].reindex(df["market_id"]).to_numpy()

    # ---- joint models ----
    log("=== joint models ===")
    df["_all"] = 0
    coef_frames = []
    for fe_col, fe_label in [("_all", "noFE"), ("topic", "topicFE"),
                             ("cluster_k200", "clusterFE")]:
        for wcol, wlabel in [(None, "count"), ("usdc", "dollar")]:
            coef_frames.append(joint_model(df, fe_col, wcol, DIM_COLS,
                                           f"{fe_label}|{wlabel}"))
    # robustness: continuous novelty in place of the tail flag
    rob_dims = [d if d != "nov_tail" else "z_nov" for d in DIM_COLS]
    coef_frames.append(joint_model(df, "cluster_k200", None, rob_dims,
                                   "clusterFE|count|z_nov"))
    pd.concat(coef_frames, ignore_index=True).to_parquet(
        OUT_DIR / "horse_race_v1_coefs.parquet")

    # ---- univariate gradients (quintiles / binaries) ----
    log("=== univariate gradients ===")
    uni_rows = []
    for d in DIM_COLS:
        if d in ("nov_tail", "anchored", "neg_risk"):
            mbin = mdims[d]
            bins = mbin.reindex(df["market_id"]).to_numpy()
            labels_ = {0.0: f"{d}=0", 1.0: f"{d}=1"}
            binkeys = [0.0, 1.0]
        else:
            q = mdims[d].rank(pct=True)
            mbin = np.ceil(q * 5).clip(1, 5)
            bins = mbin.reindex(df["market_id"]).to_numpy()
            labels_ = {float(i): f"{d}_q{i}" for i in range(1, 6)}
            binkeys = [float(i) for i in range(1, 6)]
        for bk in binkeys:
            sub = df[bins == bk]
            if len(sub) < 5000:
                continue
            b, se = threeway_cluster_slope(
                sub["won"].to_numpy(float) - sub["price"].to_numpy(float),
                sub["price"], None, sub["trade_day"], sub["proxyWallet"],
                sub["market_id"])
            bd, sed = threeway_cluster_slope(
                sub["won"].to_numpy(float) - sub["price"].to_numpy(float),
                sub["price"], sub["usdc"], sub["trade_day"], sub["proxyWallet"],
                sub["market_id"])
            uni_rows.append({"dim": d, "bin": labels_[bk], "n": len(sub),
                             "usd": float(sub["usdc"].sum()),
                             "slope_dev": b, "se": se,
                             "t": b / se if se > 0 else np.nan,
                             "slope_dev_dol": bd, "se_dol": sed,
                             "t_dol": bd / sed if sed > 0 else np.nan})
            log(f"  {labels_[bk]:16s} n={len(sub):>10,} dev={b:+.4f} "
                f"(t={b/se if se>0 else float('nan'):+.2f})")
    pd.DataFrame(uni_rows).to_parquet(OUT_DIR / "horse_race_v1_univariate.parquet")

    # ---- within-series test ----
    log("=== within-series test (series FE) ===")
    ins = df[df["series_slug"].notna()].copy()
    counts = ins.groupby("series_slug", observed=True)["market_id"].nunique()
    keep = counts[counts >= 5].index
    ins = ins[ins["series_slug"].isin(keep)]
    ins["ln_prior_raw"] = np.log1p(
        ins.groupby("market_id", observed=True)["prior_instances"]
           .transform("first"))
    ws_frames = []
    for wcol, wlabel in [(None, "count"), ("usdc", "dollar")]:
        r = joint_model(ins.assign(_fe=ins["series_slug"]), "_fe", wcol,
                        ["ln_prior_raw"], f"seriesFE|{wlabel}")
        ws_frames.append(r)
    log(f"  within-series sample: {len(ins):,} trades, "
        f"{ins['series_slug'].nunique():,} series, "
        f"{ins['market_id'].nunique():,} markets")
    # secondary: ordinal thirds within series (>=9 traded instances)
    keep9 = counts[counts >= 9].index
    ins9 = ins[ins["series_slug"].isin(keep9)].copy()
    ordinal = (ins9.groupby(["series_slug", "market_id"], observed=True)
                   ["prior_instances"].first().rank(method="first")
                   .groupby("series_slug", observed=True).rank(pct=True))
    third = np.ceil(ordinal * 3).clip(1, 3)
    third_of_market = third.droplevel(0)
    ins9["third"] = third_of_market.reindex(ins9["market_id"]).to_numpy()
    for th, nm in [(1, "early"), (2, "middle"), (3, "late")]:
        sub = ins9[ins9["third"] == th]
        if len(sub) < 5000:
            continue
        b, se = threeway_cluster_slope(
            sub["won"].to_numpy(float) - sub["price"].to_numpy(float),
            sub["price"], None, sub["trade_day"], sub["proxyWallet"],
            sub["market_id"])
        ws_frames.append(pd.DataFrame([{"spec": "series_thirds", "term": nm,
                                        "beta": b, "se": se,
                                        "t": b / se if se > 0 else np.nan}]))
        log(f"  third {nm}: n={len(sub):,} dev={b:+.4f} (t={b/se:+.2f})")
    pd.concat(ws_frames, ignore_index=True).to_parquet(
        OUT_DIR / "within_series_v1.parquet")

    # ---- economics: dollar sizing ----
    log("=== economics ===")
    eco_rows = []
    def eco(mask, name):
        sub = df[mask]
        if not len(sub):
            return
        usd = sub["usdc"].sum()
        ret = sub["won"].to_numpy(float) - sub["price"].to_numpy(float)
        wret = float((sub["usdc"].to_numpy() * ret).sum())
        eco_rows.append({"group": name, "n_trades": len(sub),
                         "n_markets": sub["market_id"].nunique(),
                         "usd": float(usd),
                         "dollar_wt_ret": wret / usd if usd > 0 else np.nan,
                         "net_buyer_pnl_usd": wret})
    eco(df["nov_tail"] == 1, "novelty tail (d1)")
    eco(df["nov_tail"] == 0, "not novelty tail")
    eco(df["anchored"] == 1, "anchored")
    eco(df["anchored"] == 0, "judgment/other")
    liq_q = mdims["z_ln_usd"].rank(pct=True).reindex(df["market_id"]).to_numpy()
    for i in range(5):
        eco((liq_q > i / 5) & (liq_q <= (i + 1) / 5), f"liquidity q{i+1}")
    life_q = mdims["z_ln_life"].rank(pct=True).reindex(df["market_id"]).to_numpy()
    for i in range(5):
        eco((life_q > i / 5) & (life_q <= (i + 1) / 5), f"lifetime q{i+1}")
    pd.DataFrame(eco_rows).to_parquet(OUT_DIR / "horse_race_v1_economics.parquet")

    summary = {
        "run_finished_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
        "n_trades": int(len(df)), "n_markets": int(df["market_id"].nunique()),
        "window": "mature 25-80%", "dims": DIM_COLS,
        "fe_variants": ["noFE", "topicFE", "clusterFE(k200)"],
        "weights": ["count", "dollar"],
        "se": "CGM 3-way (day x wallet x market)",
        "within_series": {"min_instances": 5, "thirds_min_instances": 9,
                          "n_trades": int(len(ins)),
                          "n_series": int(ins["series_slug"].nunique())},
        "elapsed_min": round((time.time() - t0) / 60, 1),
    }
    (OUT_DIR / "horse_race_v1_summary.json").write_text(
        json.dumps(summary, indent=2))
    log(f"DONE in {summary['elapsed_min']} min")


if __name__ == "__main__":
    main()
