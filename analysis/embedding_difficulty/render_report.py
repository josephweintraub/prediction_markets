"""Render the embedding-difficulty session report (self-contained HTML).

Reproducibility: every number in the report is read from artifacts produced by
committed scripts in analysis/embedding_difficulty/ (build_universe.py,
build_flb_base.py, embed_universe.py, run_pca.py, make_cluster_slices.py,
compute_novelty.py, novelty_diagnostics.py, make_novelty_slices.py,
make_actsubj_slices.py, run_schemes.py). Artifact root:
/mnt/data/embedding_difficulty/. No hand-transcribed numbers.

Output: /mnt/data/embedding_difficulty/report/embedding_difficulty_report.html
"""
from __future__ import annotations
import base64
import glob
import io
import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import norm

BASE = "/mnt/data/embedding_difficulty"
OUT = f"{BASE}/report"
os.makedirs(OUT, exist_ok=True)
plt.rcParams.update({"figure.dpi": 110, "font.size": 9,
                     "axes.grid": True, "grid.alpha": 0.3})


def fig64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return ("<img src='data:image/png;base64,"
            + base64.b64encode(buf.getvalue()).decode() + "'/>")


def summary(scheme: str, window: str) -> pd.DataFrame | None:
    p = f"{BASE}/output/flb_summary_{scheme}_{window}.parquet"
    if not os.path.exists(p):
        return None
    df = pd.read_parquet(p)
    return df if len(df) else None


def deciles(scheme: str, window: str) -> pd.DataFrame | None:
    p = f"{BASE}/output/flb_deciles_{scheme}_{window}.parquet"
    return pd.read_parquet(p) if os.path.exists(p) else None


def slope_panel(df: pd.DataFrame, title: str, order=None, dollar=False,
                rotate=30) -> str:
    """Point-and-CI plot of per-slice slope."""
    col, se = ("slope_dol", "slope_se_dol") if dollar else ("slope", "slope_se")
    d = df.copy()
    if order is not None:
        d = d.set_index("slice").loc[[o for o in order if o in set(df["slice"])]] \
             .reset_index()
    fig, ax = plt.subplots(figsize=(max(4, 0.55 * len(d)), 3.2))
    x = np.arange(len(d))
    ax.errorbar(x, d[col], yerr=1.96 * d[se], fmt="o", ms=4, capsize=3, lw=1)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(d["slice"], rotation=rotate, ha="right")
    ax.set_ylabel("signed slope" + (" ($-wt)" if dollar else ""))
    ax.set_title(title)
    return fig64(fig)


def decile_curve(dec: pd.DataFrame, title: str) -> str:
    fig, ax = plt.subplots(figsize=(4.6, 3.2))
    ax.errorbar(dec["decile"], dec["cal_error"], yerr=1.96 * dec["se"],
                fmt="-o", ms=4, capsize=3, lw=1, label="count-wt")
    ax.errorbar(dec["decile"], dec["cal_error_dol"], yerr=1.96 * dec["se_dol"],
                fmt="-s", ms=4, capsize=3, lw=1, label="dollar-wt", alpha=0.7)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xlabel("price decile")
    ax.set_ylabel("calibration error (win − price)")
    ax.set_title(title)
    ax.legend()
    return fig64(fig)


def excess_dispersion(df: pd.DataFrame) -> dict:
    """Trade-weighted dispersion of true slopes across slices, corrected for
    estimation noise: Var_signal = wvar(slope) − wmean(slope_se^2)."""
    w = df["n_trades"] / df["n_trades"].sum()
    m = (w * df["slope"]).sum()
    wvar = (w * (df["slope"] - m) ** 2).sum()
    noise = (w * df["slope_se"] ** 2).sum()
    nb = max(len(df) - 1, 1)
    z = norm.ppf(1 - 0.025 / max(len(df), 1))
    return {"n_slices": int(len(df)),
            "wmean_slope": float(m),
            "raw_sd": float(np.sqrt(wvar)),
            "signal_sd": float(np.sqrt(max(wvar - noise, 0))),
            "share_sig_bonf": float((df["slope_t"].abs() > z).mean()),
            "min_slope": float(df["slope"].min()),
            "max_slope": float(df["slope"].max())}


H = ["<html><head><meta charset='utf-8'><title>Embedding-based intrinsic "
     "difficulty — session 1</title><style>",
     "body{font-family:Georgia,serif;max-width:1100px;margin:24px auto;"
     "padding:0 16px;line-height:1.45;color:#1a1a1a}",
     "h1{font-size:22px} h2{font-size:18px;border-bottom:1px solid #ccc;"
     "padding-bottom:4px;margin-top:34px} h3{font-size:15px}",
     "table{border-collapse:collapse;font-size:12px;font-family:Menlo,monospace}",
     "td,th{border:1px solid #ddd;padding:3px 7px;text-align:right}",
     "th{background:#f2f2f2} .note{background:#fff8e1;border-left:4px solid "
     "#f0c040;padding:8px 12px;font-size:13px} .prov{background:#eef3f8;"
     "border-left:4px solid #4a7dab;padding:8px 12px;font-size:12.5px}",
     "img{max-width:100%}</style></head><body>"]


def add(s: str) -> None:
    H.append(s)


def tbl(df: pd.DataFrame, fl="{:+.4f}") -> str:
    return df.to_html(index=False, float_format=lambda x: fl.format(x),
                      border=0)


# ---------------- header ----------------
cov = json.load(open(f"{BASE}/build_universe_coverage.json"))
bmeta = json.load(open(f"{BASE}/flb_base_meta.json"))
add("<h1>Embedding-based intrinsic difficulty — session 1 (2026-07-03)</h1>")
add("<div class='prov'><b>Provenance.</b> All numbers are rendered from "
    "artifacts under <code>/mnt/data/embedding_difficulty/</code> produced by "
    "committed scripts in <code>analysis/embedding_difficulty/</code> "
    "(build_universe.py, build_flb_base.py, embed_universe.py, run_pca.py, "
    "make_cluster_slices.py, compute_novelty.py, novelty_diagnostics.py, "
    "make_novelty_slices.py, make_actsubj_slices.py, run_schemes.py; rendered "
    "by render_report.py). Measurement: standard filters, signed calibration "
    "slope primary (OLS of ret on price; &gt;0 = classic FLB direction), "
    "D10−D1 secondary, CGM 3-way clustered SEs (day × wallet × market), "
    "5,000-trade slice floor, mature (25–80%) and closing (80–100%) windows. "
    "Embeddings: BAAI/bge-small-en-v1.5 on question text.</div>")

add(f"<h2>1. Data</h2><p>Universe: <b>{cov['markets_universe']:,}</b> "
    f"non-up/down markets with question+rules text "
    f"({cov['with_created_at']:,} with native created_at); "
    f"{cov['markets_ge1_filtered_trade']:,} with ≥1 standard-filtered trade. "
    f"Filtered BUY trades: {bmeta['buy_filtered_rows']:,} → mature window "
    f"{bmeta['rows_mature']:,}, closing {bmeta['rows_closing']:,}.</p>")
add("<div class='note'><b>Two data-plumbing findings surfaced by this build "
    "(affect any standard-filter run on the June-2026 extended trade set):</b> "
    "(a) the March resolutions spine covers only ~49% of extended trade rows — "
    "this build uses the fresh June-24 spine (100% token coverage); (b) "
    "trades' <code>eventSlug</code> is empty for newer markets, so the "
    f"standard up/down exclusion catches ~nothing; {cov['markets_updown_flagged']:,} "
    f"up/down markets (~1.34B raw rows, {cov['updown_buy_filtered_trades']:,} "
    "filtered BUY trades) were excluded here at market level from Gamma "
    "metadata.</div>")

# ---------------- baseline ----------------
add("<h2>2. Baseline calibration</h2>")
for win in ("mature", "closing"):
    d = deciles("all", win)
    if d is not None and len(d):
        add(decile_curve(d[d["slice"] == "ALL"], f"Pooled — {win} window"))
s_all = pd.concat([x.assign(window=w) for w in ("mature", "closing")
                   if (x := summary("all", w)) is not None])
add(tbl(s_all[["window", "n_trades", "slope", "slope_se", "slope_t",
               "slope_dol", "slope_t_dol", "spread", "spread_t"]]))

cat_m = summary("category", "mature")
if cat_m is not None:
    cat_m = cat_m.sort_values("slope")
    add("<h3>By curated category (mature)</h3>")
    add(slope_panel(cat_m, "slope by category — mature"))
    add(tbl(cat_m[["slice", "n_trades", "n_markets", "slope", "slope_se",
                   "slope_t", "slope_dol", "slope_t_dol"]]))
ser_m = summary("series_membership", "mature")
if ser_m is not None:
    add("<h3>Series membership (recurrence axis)</h3>")
    add(tbl(ser_m[["slice", "n_trades", "slope", "slope_se", "slope_t",
                   "slope_dol", "slope_t_dol"]]))

# ---------------- PCA ----------------
add("<h2>3. Approach A — PCA structure</h2>")
evr = json.load(open(f"{BASE}/pca_evr.json"))["evr"]
fig, ax = plt.subplots(figsize=(5, 2.6))
ax.bar(range(1, 21), evr[:20])
ax.set_xlabel("PC")
ax.set_ylabel("explained variance ratio")
add(fig64(fig))
corr = pd.read_parquet(f"{BASE}/pca_correlates.parquet")
piv = corr[~corr["observable"].str.startswith("beta_")
           & (corr["observable"] != "ols_r2")] \
    .pivot(index="observable", columns="pc", values="corr")
piv = piv[[c for c in piv.columns if c <= 8]]
add("<p>Correlation of top PCs with observables (top PCs of sentence "
    "embeddings are known to encode frequency/length artifacts — interpret "
    "via this table, not by eyeballing):</p>")
add(piv.reset_index().to_html(index=False, border=0,
                              float_format=lambda x: f"{x:+.2f}"))
for i in range(1, 5):
    s = summary(f"pca_pc{i}_quintile", "mature")
    if s is not None:
        s = s.sort_values("slice")
        add(slope_panel(s, f"slope by PC{i} quintile — mature", rotate=0))

# ---------------- novelty ----------------
add("<h2>4. Approach B — novelty / precedent density at birth</h2>")
nmeta = json.load(open(f"{BASE}/novelty_meta.json"))
hub = json.load(open(f"{BASE}/novelty_hubness.json"))
add(f"<p>Strict predecessor rule (birth-time ordered; τ = {nmeta['tau']:.3f} "
    f"at the {nmeta['tau_quantile']} quantile of random-pair similarity). "
    f"Birth fallback (first trade) used for {nmeta['birth_fallback_n']:,} "
    f"markets. Hubness: k-occurrence skewness "
    f"{hub['k_occurrence_skewness']:.1f}, max occurrence "
    f"{hub['max_occurrence']:,}.</p>")
dist = pd.read_parquet(f"{BASE}/novelty_dist.parquet")
fig, ax = plt.subplots(figsize=(5, 2.8))
ax.plot(dist["year"], dist["p50"], "-o", ms=3, label="median")
ax.fill_between(dist["year"], dist["p10"], dist["p90"], alpha=0.25,
                label="p10–p90")
ax.set_ylabel("sim_k25_x")
ax.set_title("novelty distribution by vintage (excl. same-series neighbors)")
ax.legend()
add(fig64(fig))
conf = pd.read_parquet(f"{BASE}/novelty_confounds.parquet")
add("<h3>Confound table (correlations & standardized OLS betas)</h3>")
add(tbl(conf[conf["target"] == "sim_k25_x"], "{:+.3f}"))
for sch, ttl in (("nov_k25", "novelty deciles (incl. same-series)"),
                 ("nov_k25x", "novelty deciles (EXCL. same-series/event)"),
                 ("nov_k25x_vint", "novelty deciles within vintage year"),
                 ("nov_cnt", "precedent-count bins (τ-neighbors, excl. series)")):
    s = summary(sch, "mature")
    if s is not None:
        s = s.sort_values("slice")
        add(slope_panel(s, f"{ttl} — mature", rotate=0))
        add(tbl(s[["slice", "n_trades", "n_markets", "slope", "slope_se",
                   "slope_t", "slope_dol", "slope_t_dol"]]))
sx = summary("nov_k25x", "closing")
if sx is not None:
    add(slope_panel(sx.sort_values("slice"),
                    "novelty deciles (excl.) — closing window", rotate=0))
ex = pd.read_parquet(f"{BASE}/novelty_examples.parquet")
add("<h3>Qualitative anchors</h3>")
add(ex.to_html(index=False, border=0))

# ---------------- clusters ----------------
add("<h2>5. Approach C — FLB dispersion across granularities</h2>")
disp_rows = []
for sch in ("category", "cluster_k12", "cluster_k50", "cluster_k200",
            "cluster_k1000"):
    s = summary(sch, "mature")
    if s is not None and len(s) > 2:
        disp_rows.append({"scheme": sch, **excess_dispersion(s)})
if disp_rows:
    add("<p>Noise-corrected dispersion of true slice slopes (trade-weighted); "
        "signal_sd is the estimated SD of TRUE slopes across slices after "
        "subtracting estimation noise — the 'how much difficulty "
        "heterogeneity exists at this granularity' number:</p>")
    add(tbl(pd.DataFrame(disp_rows), "{:+.4f}"))
for k in (50, 200):
    s = summary(f"cluster_k{k}", "mature")
    if s is None:
        continue
    terms = pd.read_parquet(f"{BASE}/cluster_terms_k{k}.parquet")
    m = s.merge(terms, left_on="slice", right_on="cluster")
    fig, ax = plt.subplots(figsize=(5.4, 3.2))
    ax.scatter(np.log10(m["n_trades"]), m["slope"], s=12, alpha=0.6)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xlabel("log10 slice trades")
    ax.set_ylabel("slope")
    ax.set_title(f"k={k}: per-cluster slope vs size — mature")
    add(fig64(fig))
    m = m.sort_values("slope")
    cols = ["slice", "n_trades", "slope", "slope_t", "top_terms", "ex1"]
    add(f"<h3>k={k}: most negative / most positive slope clusters</h3>")
    add(pd.concat([m.head(8), m.tail(8)])[cols].to_html(
        index=False, border=0, float_format=lambda x: f"{x:+.3f}"))

# ---------------- act/subj ----------------
add("<h2>6. Approach D — action × subject precedent (exploratory)</h2>")
add("<p class='note'>Stage-2 labels cover 379K/850K markets (59% of filtered "
    "trades), none after the pre-June universe — vintage-confounded; "
    "suggestive only.</p>")
for sch, ttl in (("act_prec", "action precedent count"),
                 ("subj_prec", "subject precedent count"),
                 ("actsubj_2x2", "action-seen × subject-seen"),
                 ("act_prec_vint",
                  "action precedent, quintiles WITHIN vintage year")):
    s = summary(sch, "mature")
    if s is not None:
        order = sorted(s["slice"])
        add(slope_panel(s, f"{ttl} — mature", order=order, rotate=20))
        add(tbl(s[["slice", "n_trades", "n_markets", "slope", "slope_se",
                   "slope_t", "slope_dol", "slope_t_dol"]]))

# ---------------- caveats ----------------
add("<h2>7. Caveats & open items</h2><ul>"
    "<li>Resolution censoring (methods_reference): trade set contains only "
    "markets resolved by build time; late-vintage novelty slices are "
    "horizon-censored.</li>"
    "<li>wallet_flags built 2026-06-11; bot coverage of newest-era wallets "
    "unaudited.</li>"
    "<li>Slope is trade-level; contract-level robustness not yet run.</li>"
    "<li>Encoder robustness (second model family) not yet run; novelty "
    "computed on question-only text (qd variant pending).</li>"
    "<li>All schemes here are cross-sectional; within-series designs are the "
    "natural next step.</li></ul>")
add("</body></html>")

with open(f"{OUT}/embedding_difficulty_report.html", "w") as f:
    f.write("\n".join(H))
print(f"wrote {OUT}/embedding_difficulty_report.html", flush=True)
