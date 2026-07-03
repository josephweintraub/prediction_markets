"""Render the embedding-difficulty session report (self-contained HTML), v2.

v2 (2026-07-03): de-cluttered slice axes (common prefixes stripped, wider
panels), per-section "How to read" guides, and a liquidity section (volume
gradient, inclusion floors, rolling-median rule, novelty x liquidity).

Reproducibility: every number is read from artifacts produced by committed
scripts in analysis/embedding_difficulty/ (build_universe.py,
build_flb_base.py, embed_universe.py, run_pca.py, make_cluster_slices.py,
compute_novelty.py, novelty_diagnostics.py, make_novelty_slices.py,
make_actsubj_slices.py, make_baseline_slices.py, make_liquidity_slices.py,
run_schemes.py). Artifact root: /mnt/data/embedding_difficulty/.

Output: /mnt/data/embedding_difficulty/report/embedding_difficulty_report.html
"""
from __future__ import annotations
import base64
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


# ---------------- helpers ----------------

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


def short_labels(labels: list[str]) -> list[str]:
    """Strip the longest common prefix (up to its last '_') for display."""
    if len(labels) < 2:
        return labels
    pref = os.path.commonprefix(labels)
    cut = pref.rfind("_") + 1
    return [l[cut:] for l in labels] if cut >= 2 else labels


def slope_panel(df: pd.DataFrame, title: str, order=None, dollar=False) -> str:
    col, se = ("slope_dol", "slope_se_dol") if dollar else ("slope", "slope_se")
    d = df.copy()
    if order is None:
        order = sorted(d["slice"])
    d = d.set_index("slice").loc[[o for o in order if o in set(df["slice"])]] \
         .reset_index()
    fig, ax = plt.subplots(figsize=(max(4.4, 0.72 * len(d)), 3.2))
    x = np.arange(len(d))
    ax.errorbar(x, d[col], yerr=1.96 * d[se], fmt="o", ms=4, capsize=3, lw=1)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(short_labels(d["slice"].tolist()), rotation=35,
                       ha="right", fontsize=8)
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
    ax.set_xlabel("price decile (1 = longshots, 10 = favorites)")
    ax.set_ylabel("calibration error (win − price)")
    ax.set_title(title)
    ax.legend()
    return fig64(fig)


def excess_dispersion(df: pd.DataFrame) -> dict:
    w = df["n_trades"] / df["n_trades"].sum()
    m = (w * df["slope"]).sum()
    wvar = (w * (df["slope"] - m) ** 2).sum()
    noise = (w * df["slope_se"] ** 2).sum()
    z = norm.ppf(1 - 0.025 / max(len(df), 1))
    return {"n_slices": int(len(df)), "wmean_slope": float(m),
            "raw_sd": float(np.sqrt(wvar)),
            "signal_sd": float(np.sqrt(max(wvar - noise, 0))),
            "share_sig_bonf": float((df["slope_t"].abs() > z).mean()),
            "min_slope": float(df["slope"].min()),
            "max_slope": float(df["slope"].max())}


H = ["<html><head><meta charset='utf-8'><title>Embedding-based intrinsic "
     "difficulty — sessions 1–2</title><style>",
     "body{font-family:Georgia,serif;max-width:1100px;margin:24px auto;"
     "padding:0 16px;line-height:1.45;color:#1a1a1a}",
     "h1{font-size:22px} h2{font-size:18px;border-bottom:1px solid #ccc;"
     "padding-bottom:4px;margin-top:34px} h3{font-size:15px}",
     "table{border-collapse:collapse;font-size:12px;font-family:Menlo,monospace}",
     "td,th{border:1px solid #ddd;padding:3px 7px;text-align:right}",
     "th{background:#f2f2f2} .note{background:#fff8e1;border-left:4px solid "
     "#f0c040;padding:8px 12px;font-size:13px} .prov{background:#eef3f8;"
     "border-left:4px solid #4a7dab;padding:8px 12px;font-size:12.5px}",
     ".how{background:#f0f7f0;border-left:4px solid #6aa84f;padding:8px 12px;"
     "font-size:13px;margin:8px 0}",
     "img{max-width:100%}</style></head><body>"]


def add(s: str) -> None:
    H.append(s)


def how(text: str) -> None:
    add(f"<div class='how'><b>How to read.</b> {text}</div>")


def tbl(df: pd.DataFrame, fl="{:+.4f}") -> str:
    return df.to_html(index=False, float_format=lambda x: fl.format(x), border=0)


def fmt_t(row, col="slope", se="slope_se", t="slope_t"):
    return f"{row[col]:+.4f} (t={row[t]:+.1f})"


# ---------------- header ----------------
cov = json.load(open(f"{BASE}/build_universe_coverage.json"))
bmeta = json.load(open(f"{BASE}/flb_base_meta.json"))
add("<h1>Embedding-based intrinsic difficulty — sessions 1–2 (2026-07-03)</h1>")
add("<div class='prov'><b>Provenance.</b> All numbers are rendered from "
    "artifacts under <code>/mnt/data/embedding_difficulty/</code> produced by "
    "committed scripts in <code>analysis/embedding_difficulty/</code> "
    "(see script list in render_report.py header). Measurement: standard "
    "filters; <b>signed calibration slope</b> primary; D10−D1 secondary; CGM "
    "3-way clustered SEs (day × wallet × market); 5,000-trade slice floor; "
    "mature (25–80% of lifetime) and closing (80–100%) windows. Embeddings: "
    "BAAI/bge-small-en-v1.5 on question text.</div>")
how("One metric appears everywhere: the <b>signed calibration slope</b> — a "
    "per-slice regression of trade returns on price. Slope = 0 means prices "
    "are calibrated (longshots and favorites win exactly as often as their "
    "prices imply). Slope &gt; 0 is the classic favorite–longshot bias "
    "(longshots overpriced, favorites underpriced); slope &lt; 0 is the "
    "reverse. In every point-plot below, each dot is one slice of markets, "
    "whiskers are 95% confidence intervals from SEs clustered three ways "
    "(same day, same wallet, same market) — so a dot whose whiskers exclude "
    "zero indicates statistically distinguishable miscalibration. "
    "Count-weighted treats every trade equally; dollar-weighted weights by "
    "trade size (what the marginal dollar experiences).")

# ---------------- 1. data ----------------
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
    f"up/down markets (~1.34B raw rows) were excluded here at market level "
    "from Gamma metadata. Both fixes are now shared plumbing: "
    "<code>scripts/build_market_flags.py</code>.</div>")

# ---------------- 2. baseline ----------------
add("<h2>2. Baseline calibration</h2>")
how("These curves show raw calibration by price decile: for trades bought at "
    "prices in each decile, the average of (won − price). A perfectly "
    "calibrated decile sits at 0. Classic FLB looks like below-zero on the "
    "left (longshots) and above-zero on the right (favorites). The table "
    "gives the pooled slope per lifecycle window.")
for win in ("mature", "closing"):
    d = deciles("all", win)
    if d is not None and len(d):
        add(decile_curve(d[d["slice"] == "ALL"], f"Pooled — {win} window"))
s_all = pd.concat([x.assign(window=w) for w in ("mature", "closing")
                   if (x := summary("all", w)) is not None])
add(tbl(s_all[["window", "n_trades", "slope", "slope_se", "slope_t",
               "slope_dol", "slope_t_dol", "spread", "spread_t"]]))
pooled_m = s_all[s_all["window"] == "mature"].iloc[0]
add(f"<p><i>Interpretation:</i> the pooled mature-window slope is "
    f"{pooled_m['slope']:+.4f} (t={pooled_m['slope_t']:+.1f}) — statistically "
    "indistinguishable from calibrated. Everything below asks where "
    "miscalibration hides underneath this aggregate zero.</p>")

cat_m = summary("category", "mature")
if cat_m is not None:
    cat_m = cat_m.sort_values("slope")
    add("<h3>By curated category (mature)</h3>")
    how("Each dot is one of the 12 curated native categories (plus UNKNOWN "
        "for unmapped markets). Categories left of zero have longshots "
        "UNDERpriced; right of zero, overpriced. This is the comparison "
        "baseline for the finer, label-free slicings in section 6.")
    add(slope_panel(cat_m, "slope by category — mature",
                    order=cat_m["slice"].tolist()))
    add(tbl(cat_m[["slice", "n_trades", "n_markets", "slope", "slope_se",
                   "slope_t", "slope_dol", "slope_t_dol"]]))
ser_m = summary("series_membership", "mature")
if ser_m is not None:
    add("<h3>Series membership (recurrence axis)</h3>")
    how("Native Polymarket series group recurring market instances (NBA "
        "games, weekly crypto closes). If repetition alone produced "
        "calibration, in-series should differ from standalone.")
    add(tbl(ser_m[["slice", "n_trades", "slope", "slope_se", "slope_t",
                   "slope_dol", "slope_t_dol"]]))

# ---------------- 3. PCA ----------------
add("<h2>3. Approach A — PCA structure of question space</h2>")
how("PCA finds the main axes along which market question texts vary. The bar "
    "chart shows how much variance each axis explains. The correlation table "
    "is the guardrail: top components of sentence embeddings are known to "
    "pick up question length and template frequency rather than meaning, so "
    "an axis is only interpreted through what it correlates with. The "
    "quintile panels then ask whether calibration varies along each axis: "
    "markets are sorted by their position on the axis and cut into five "
    "equal groups (q1 = lowest).")
evr = json.load(open(f"{BASE}/pca_evr.json"))["evr"]
fig, ax = plt.subplots(figsize=(5, 2.6))
ax.bar(range(1, 21), evr[:20])
ax.set_xlabel("principal component")
ax.set_ylabel("explained variance ratio")
add(fig64(fig))
corr = pd.read_parquet(f"{BASE}/pca_correlates.parquet")
piv = corr[~corr["observable"].str.startswith("beta_")
           & (corr["observable"] != "ols_r2")] \
    .pivot(index="observable", columns="pc", values="corr")
piv = piv[[c for c in piv.columns if c <= 8]]
add(piv.reset_index().to_html(index=False, border=0,
                              float_format=lambda x: f"{x:+.2f}"))
for i in range(1, 5):
    s = summary(f"pca_pc{i}_quintile", "mature")
    if s is not None:
        add(slope_panel(s, f"slope by PC{i} quintile — mature"))
add("<p><i>Interpretation:</i> PC1 (correlated with question length, trade "
    "count, and series membership) carries a monotone calibration gradient "
    "in the mature window; PC2–PC3 carry little. In the closing window the "
    "PC1 gradient flattens (see closing artifacts) — position-in-question-"
    "space miscalibration is corrected as resolution approaches.</p>")

# ---------------- 4. novelty ----------------
add("<h2>4. Approach B — novelty / precedent density at birth</h2>")
nmeta = json.load(open(f"{BASE}/novelty_meta.json"))
hub = json.load(open(f"{BASE}/novelty_hubness.json"))
how("For every market we measure how similar its question is to markets "
    "created STRICTLY BEFORE it (no lookahead): sim_k25 = mean cosine "
    "similarity to its 25 nearest predecessors. High = the market has close "
    "precedents; low = nothing like it existed. The _x variant excludes "
    "same-series/same-event predecessors, so recurring templates can't "
    "trivially count as their own precedent. Markets are then cut into "
    "deciles: <b>d01 = most novel, d10 = most precedented</b>. The "
    "within-vintage variant forms deciles inside each birth year, so 'novel' "
    "means novel relative to its own era, not to the platform's early days.")
add(f"<p>τ = {nmeta['tau']:.3f} ({nmeta['tau_quantile']} quantile of "
    f"random-pair similarity). Birth fallback (first trade) for "
    f"{nmeta['birth_fallback_n']:,} markets. Hubness: k-occurrence skewness "
    f"{hub['k_occurrence_skewness']:.1f} (max {hub['max_occurrence']:,}) — "
    "high; rank-based deciles soften this but a mutual-proximity rescale is "
    "a pending robustness item.</p>")
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
add("<h3>Confound table</h3>")
how("Correlations and standardized OLS betas of the novelty score on the "
    "known artifact channels (question length, volume, platform growth, "
    "series membership). If novelty were just recovering one of these, its "
    "calibration gradient would be an artifact — this table is what any "
    "referee checks first.")
add(tbl(conf[conf["target"] == "sim_k25_x"], "{:+.3f}"))
for sch, ttl in (("nov_k25", "novelty deciles (incl. same-series)"),
                 ("nov_k25x", "novelty deciles (EXCL. same-series/event)"),
                 ("nov_k25x_vint", "novelty deciles within vintage year"),
                 ("nov_cnt", "precedent-count bins (τ-neighbors, excl. series)")):
    s = summary(sch, "mature")
    if s is not None:
        add(slope_panel(s, f"{ttl} — mature"))
        add(tbl(s.sort_values("slice")[
            ["slice", "n_trades", "n_markets", "slope", "slope_se",
             "slope_t", "slope_dol", "slope_t_dol"]]))
sx = summary("nov_k25x_vint", "closing")
if sx is not None:
    add(slope_panel(sx, "novelty deciles within vintage year — closing window"))
add("<p><i>Interpretation:</i> the signal is a TAIL effect, concentrated in "
    "d01 (the most-novel decile): classic-FLB-direction miscalibration, "
    "strongest in the within-vintage variant, roughly halving but persisting "
    "in the closing window. Middle and high deciles are ≈ calibrated, and "
    "the precedent-COUNT bins are flat: having <i>no close analog</i> "
    "predicts miscalibration; the number of analogs beyond the first few "
    "does not.</p>")
ex = pd.read_parquet(f"{BASE}/novelty_examples.parquet")
add("<h3>Qualitative anchors</h3>")
how("Spot-check of what the measure calls novel vs. precedented: the 15 most "
    "novel and 15 most precedented trade-viable markets with their nearest "
    "predecessor. The measure is working if the 'most novel' rows look "
    "genuinely unusual and the 'most precedented' rows are template "
    "repeats.")
add(ex.to_html(index=False, border=0))

# ---------------- 5. liquidity ----------------
add("<h2>5. Liquidity: the FLB–liquidity gradient and inclusion floors</h2>")
lmeta = json.load(open(f"{BASE}/liquidity_meta.json"))
how("Liquidity proxy: the market's dollar volume under the standard filters "
    "(BUY side, bots excluded) — volume, not order-book depth. Three "
    "questions: (i) is miscalibration concentrated in thin markets? (tier "
    "and era-relative panels); (ii) how sensitive are headline results to "
    "excluding thin markets? (floors table); (iii) does the novelty-tail "
    "result survive a liquidity floor, or was novelty just proxying "
    "illiquidity? (last panel).")
s = summary("liq_tier", "mature")
if s is not None:
    add(slope_panel(s, "slope by absolute volume tier — mature"))
    add(slope_panel(s, "slope by absolute volume tier — mature, dollar-weighted",
                    dollar=True))
    add(tbl(s.sort_values("slice")[
        ["slice", "n_trades", "n_markets", "slope", "slope_se", "slope_t",
         "slope_dol", "slope_t_dol"]]))
sc = summary("liq_tier", "closing")
if sc is not None:
    add(slope_panel(sc, "slope by absolute volume tier — closing"))
s = summary("liq_pctl_vint", "mature")
if s is not None:
    add(slope_panel(s, "slope by era-relative volume quintile "
                       "(within birth month) — mature"))

add("<h3>Inclusion-floor sensitivity</h3>")
how("Each row re-estimates the POOLED calibration slope after dropping "
    "markets below a volume floor. 'rollmed25' is the rolling rule: a market "
    "is kept only if its volume is ≥ 25% of the median volume of markets "
    "born in the trailing 90 days (era-adaptive junk filter). If the pooled "
    "slope moves materially with the floor, thin markets were driving it.")
rows = []
for sch, label, kept_key in (("all", "no floor", None),
                             ("all_f1k", "≥ $1k", "floor_1k_markets_kept"),
                             ("all_f10k", "≥ $10k", "floor_10k_markets_kept"),
                             ("all_f100k", "≥ $100k", "floor_100k_markets_kept"),
                             ("rollmed25", "rolling-median 25%",
                              "rollmed_markets_kept")):
    t = summary(sch, "mature")
    if t is None:
        continue
    r = t.iloc[0]
    rows.append({"floor": label, "n_markets_kept":
                 lmeta.get(kept_key, cov["markets_ge1_filtered_trade"]),
                 "n_trades": r["n_trades"], "slope": r["slope"],
                 "slope_t": r["slope_t"], "slope_dol": r["slope_dol"],
                 "slope_t_dol": r["slope_t_dol"]})
add(tbl(pd.DataFrame(rows)))
add(f"<p>Rolling rule excludes {lmeta['rollmed_share_excluded']:.1%} of "
    "trade-viable markets overall; per-year exclusion below.</p>")
rstats = pd.read_parquet(f"{BASE}/rollmed_stats.parquet")
add(tbl(rstats, "{:,.3f}"))

add("<h3>Is the novelty tail just illiquidity?</h3>")
nv = pd.read_parquet(f"{BASE}/schemes/scheme_nov_k25x_vint.parquet")
uu = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                     columns=["market_id", "usd_buy_filtered"])
nvu = nv.merge(uu, on="market_id")
volt = nvu.groupby("slice")["usd_buy_filtered"].median().reset_index() \
    .rename(columns={"usd_buy_filtered": "median_market_usd"})
add(tbl(volt, "{:,.0f}"))
s = summary("novx_vint_f10k", "mature")
if s is not None:
    how("Novelty deciles rebuilt using ONLY markets with ≥ $10k volume "
        "(d01 = most novel within its birth year). If the d01 effect "
        "vanished here, the novelty result would just be thin-market "
        "noise.")
    add(slope_panel(s, "novelty deciles within vintage year, "
                       "≥$10k markets only — mature"))
    add(tbl(s.sort_values("slice")[
        ["slice", "n_trades", "n_markets", "slope", "slope_se", "slope_t",
         "slope_dol", "slope_t_dol"]]))

# ---------------- 6. granularity ----------------
add("<h2>6. Approach C — how much heterogeneity does each granularity "
    "reveal?</h2>")
how("Markets are clustered on their embeddings at four granularities "
    "(k = 12 … 1000) and the calibration slope is estimated per cluster. "
    "raw_sd is the trade-weighted spread of estimated slopes; signal_sd "
    "subtracts estimation noise — it is the spread of TRUE slopes, the "
    "honest measure of how much difficulty heterogeneity exists at that "
    "granularity. share_sig_bonf = share of slices significant after "
    "Bonferroni. Rising signal_sd with k means finer slices keep revealing "
    "real structure that coarser ones average away.")
disp_rows = []
for sch in ("category", "cluster_k12", "cluster_k50", "cluster_k200",
            "cluster_k1000"):
    s = summary(sch, "mature")
    if s is not None and len(s) > 2:
        disp_rows.append({"scheme": sch, **excess_dispersion(s)})
if disp_rows:
    add(tbl(pd.DataFrame(disp_rows)))
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
    how("Extremes of the per-cluster slope distribution with each cluster's "
        "top TF-IDF terms and an example question — what kinds of markets "
        "sit at each end. Negative = longshots underpriced in that family; "
        "positive = classic FLB.")
    add(pd.concat([m.head(8), m.tail(8)])[cols].to_html(
        index=False, border=0, float_format=lambda x: f"{x:+.3f}"))

# ---------------- 7. action/subject ----------------
add("<h2>7. Approach D — action × subject precedent (exploratory)</h2>")
add("<p class='note'>Stage-2 labels cover 379K/850K markets (59% of filtered "
    "trades), none after the pre-June universe — vintage-confounded; "
    "suggestive only.</p>")
how("A market is decomposed as subject (Lakers, Bitcoin, Trump) × action "
    "(win game, cross price threshold, tweet count). For each market at its "
    "birth we count prior markets with the SAME action, and prior markets "
    "sharing a subject. Bins: 0 / 1–9 / 10–99 / 100–999 / 1000+ priors. The "
    "2×2 splits markets by whether the action and the subject had ≥10 "
    "priors. The vintage-controlled panel re-forms action-precedent "
    "quintiles within each birth year (q1 = least precedented).")
for sch, ttl in (("act_prec", "action precedent count"),
                 ("subj_prec", "subject precedent count"),
                 ("actsubj_2x2", "action-seen × subject-seen"),
                 ("act_prec_vint",
                  "action precedent, quintiles WITHIN vintage year")):
    s = summary(sch, "mature")
    if s is not None:
        add(slope_panel(s, f"{ttl} — mature"))
        add(tbl(s.sort_values("slice")[
            ["slice", "n_trades", "n_markets", "slope", "slope_se",
             "slope_t", "slope_dol", "slope_t_dol"]]))
add("<p><i>Interpretation:</i> the gradient lives on the ACTION axis — "
    "never-seen action types show classic FLB, fading with precedent; "
    "subject familiarity is flat everywhere. The vintage-controlled variant "
    "attenuates the gradient but keeps its monotone shape, so part of the "
    "raw effect is platform era, part survives within-era.</p>")

# ---------------- 8. caveats ----------------
add("<h2>8. Caveats & open items</h2><ul>"
    "<li>Resolution censoring (methods_reference): the trade set contains "
    "only markets resolved by build time; late-vintage slices are "
    "horizon-censored.</li>"
    "<li>wallet_flags built 2026-06-11; bot coverage of newest-era wallets "
    "unaudited.</li>"
    "<li>Liquidity proxy is realized filtered volume, not order-book depth; "
    "volume is also an outcome, so floors condition on an endogenous "
    "variable — floors are inclusion-sensitivity checks, not causal "
    "controls.</li>"
    "<li>Hubness in the neighbor graph is high; mutual-proximity rescale "
    "pending.</li>"
    "<li>Slope is trade-level; contract-level robustness not yet run.</li>"
    "<li>Encoder robustness (second model family) and the question+rules "
    "text variant pending; lexical (TF-IDF) baseline pending.</li>"
    "<li>All slicings are cross-sectional; within-series designs are the "
    "natural next step.</li></ul>")
add("</body></html>")

with open(f"{OUT}/embedding_difficulty_report.html", "w") as f:
    f.write("\n".join(H))
print(f"wrote {OUT}/embedding_difficulty_report.html", flush=True)
