"""Render the embedding-difficulty session report (self-contained HTML), v5.

v5 (2026-08-24): DECILE-FIRST diagnostic. Per project decision (JW + KV), the
primary calibration object is the full 10-price-decile table — per-decile
calibration error (win − price) with CGM 3-way clustered SEs — presented as
tail-error panels (D1 = longshot error, D10 = favorite error), slice × decile
heatmaps, and D10−D1 spreads. The signed slope remains in the artifacts and
appears as an auxiliary column only. Thin-tail guard: tail-decile trade
counts are shown next to every D1/D10 number; sign claims require the full
decile profile, never the spread alone.

Reproducibility: every number is read from artifacts produced by committed
scripts in analysis/embedding_difficulty/ (build_universe.py,
build_flb_base.py, embed_universe.py, embed_fields.py, run_pca.py,
make_cluster_slices.py, compute_novelty.py, novelty_diagnostics.py,
make_novelty_slices.py, make_actsubj_slices.py, make_baseline_slices.py,
make_liquidity_slices.py, make_field_variants.py, make_field_novelty_slices.py,
make_horizon_slices.py, run_schemes.py). Artifact root:
/mnt/data/embedding_difficulty/.

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

C_D1 = "#D55E00"   # longshot-decile error (vermillion)
C_D10 = "#0072B2"  # favorite-decile error (blue)
CMAP = "RdBu_r"    # diverging, neutral midpoint at 0


# ---------------- data access ----------------

def summary(scheme: str, window: str) -> pd.DataFrame | None:
    p = f"{BASE}/output/flb_summary_{scheme}_{window}.parquet"
    if not os.path.exists(p):
        return None
    df = pd.read_parquet(p)
    return df if len(df) else None


def deciles(scheme: str, window: str) -> pd.DataFrame | None:
    p = f"{BASE}/output/flb_deciles_{scheme}_{window}.parquet"
    if not os.path.exists(p):
        return None
    df = pd.read_parquet(p)
    return df if len(df) else None


def dtable(scheme: str, window: str) -> pd.DataFrame | None:
    """Per-slice decile-first table: D1/D10 errors (+n, t), spread, aux slope."""
    dec = deciles(scheme, window)
    s = summary(scheme, window)
    if dec is None or s is None:
        return None
    d1 = dec[dec["decile"] == 1].set_index("slice")
    d10 = dec[dec["decile"] == 10].set_index("slice")
    t = s.set_index("slice")
    out = pd.DataFrame(index=t.index)
    out["n_trades"] = t["n_trades"]
    out["d1_n"] = d1["n"]
    out["d1_err"] = d1["cal_error"]
    out["d1_t"] = d1["cal_error"] / d1["se"]
    out["d10_n"] = d10["n"]
    out["d10_err"] = d10["cal_error"]
    out["d10_t"] = d10["cal_error"] / d10["se"]
    out["spread"] = t["spread"]
    out["spread_t"] = t["spread_t"]
    out["spread_dol"] = t["spread_dol"]
    out["spread_t_dol"] = t["spread_t_dol"]
    out["slope_aux"] = t["slope"]
    return out.reset_index().sort_values("slice")


# ---------------- figure helpers ----------------

def fig64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return ("<img src='data:image/png;base64,"
            + base64.b64encode(buf.getvalue()).decode() + "'/>")


def short_labels(labels: list[str]) -> list[str]:
    if len(labels) < 2:
        return labels
    pref = os.path.commonprefix(labels)
    cut = pref.rfind("_") + 1
    return [l[cut:] for l in labels] if cut >= 2 else labels


def tail_panel(scheme: str, window: str, title: str, order=None,
               dollar: bool = False) -> str:
    """D1 (longshot) and D10 (favorite) calibration error across slices."""
    dec = deciles(scheme, window)
    if dec is None:
        return ""
    err, se = ("cal_error_dol", "se_dol") if dollar else ("cal_error", "se")
    d1 = dec[dec["decile"] == 1].set_index("slice")
    d10 = dec[dec["decile"] == 10].set_index("slice")
    labs = order if order is not None else sorted(d1.index)
    labs = [l for l in labs if l in d1.index]
    x = np.arange(len(labs))
    fig, ax = plt.subplots(figsize=(max(4.6, 0.78 * len(labs)), 3.3))
    ax.errorbar(x - 0.08, d1.loc[labs, err], yerr=1.96 * d1.loc[labs, se],
                fmt="o", ms=5, capsize=3, lw=1, color=C_D1,
                label="D1 error (longshots)")
    ax.errorbar(x + 0.08, d10.loc[labs, err], yerr=1.96 * d10.loc[labs, se],
                fmt="s", ms=5, capsize=3, lw=1, color=C_D10,
                label="D10 error (favorites)")
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(short_labels(labs), rotation=35, ha="right", fontsize=8)
    ax.set_ylabel("calibration error (win − price)"
                  + (" [$-wt]" if dollar else ""))
    ax.set_title(title)
    ax.legend(fontsize=8)
    return fig64(fig)


def decile_heatmap(scheme: str, window: str, title: str, order=None,
                   dollar: bool = False, vmax: float | None = None) -> str:
    """slices × 10 price-deciles, cell = calibration error, dot = |t|>1.96."""
    dec = deciles(scheme, window)
    if dec is None:
        return ""
    err, se = ("cal_error_dol", "se_dol") if dollar else ("cal_error", "se")
    piv = dec.pivot(index="slice", columns="decile", values=err)
    pse = dec.pivot(index="slice", columns="decile", values=se)
    labs = order if order is not None else sorted(piv.index)
    labs = [l for l in labs if l in piv.index]
    M = piv.loc[labs].to_numpy(float)
    T = M / pse.loc[labs].to_numpy(float)
    if vmax is None:
        vmax = float(np.nanquantile(np.abs(M), 0.98))
        vmax = max(vmax, 0.02)
    fig, ax = plt.subplots(figsize=(6.4, 0.34 * len(labs) + 1.4))
    im = ax.imshow(M, aspect="auto", cmap=CMAP, vmin=-vmax, vmax=vmax)
    yy, xx = np.where(np.abs(T) > 1.96)
    ax.scatter(xx, yy, s=6, c="black", marker=".")
    ax.set_xticks(range(10))
    ax.set_xticklabels([f"D{i}" for i in range(1, 11)], fontsize=8)
    ax.set_yticks(range(len(labs)))
    ax.set_yticklabels(short_labels(labs), fontsize=8)
    ax.grid(False)
    cb = fig.colorbar(im, ax=ax, shrink=0.85)
    cb.set_label("calibration error" + (" [$-wt]" if dollar else ""),
                 fontsize=8)
    ax.set_title(title + "  (· = |t| > 1.96)")
    return fig64(fig)


def decile_curve(dec: pd.DataFrame, title: str) -> str:
    fig, ax = plt.subplots(figsize=(4.6, 3.2))
    ax.errorbar(dec["decile"], dec["cal_error"], yerr=1.96 * dec["se"],
                fmt="-o", ms=4, capsize=3, lw=1, color=C_D10,
                label="count-wt")
    ax.errorbar(dec["decile"], dec["cal_error_dol"], yerr=1.96 * dec["se_dol"],
                fmt="-s", ms=4, capsize=3, lw=1, color=C_D1, alpha=0.75,
                label="dollar-wt")
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xlabel("price decile (1 = longshots, 10 = favorites)")
    ax.set_ylabel("calibration error (win − price)")
    ax.set_title(title)
    ax.legend()
    return fig64(fig)


def spread_dispersion(df: pd.DataFrame) -> dict:
    """Noise-corrected dispersion of TRUE D10−D1 spreads across slices."""
    d = df.dropna(subset=["spread", "spread_se"])
    w = d["n_trades"] / d["n_trades"].sum()
    m = (w * d["spread"]).sum()
    wvar = (w * (d["spread"] - m) ** 2).sum()
    noise = (w * d["spread_se"] ** 2).sum()
    z = norm.ppf(1 - 0.025 / max(len(d), 1))
    return {"n_slices": int(len(d)), "wmean_spread": float(m),
            "raw_sd": float(np.sqrt(wvar)),
            "signal_sd": float(np.sqrt(max(wvar - noise, 0))),
            "share_sig_bonf": float((d["spread_t"].abs() > z).mean())}


# ---------------- html scaffolding ----------------

H = ["<html><head><meta charset='utf-8'><title>Embedding-based intrinsic "
     "difficulty — decile-first report</title><style>",
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
    def fmt(x):
        if isinstance(x, (int, np.integer)):
            return f"{x:,}"
        return fl.format(x)
    return df.to_html(index=False, float_format=lambda x: fl.format(x),
                      border=0)


def add_dtable(scheme: str, window: str, order=None) -> None:
    t = dtable(scheme, window)
    if t is None:
        return
    if order is not None:
        t = t.set_index("slice").reindex([o for o in order
                                          if o in set(t["slice"])]).reset_index()
    cols = ["slice", "n_trades", "d1_n", "d1_err", "d1_t", "d10_n",
            "d10_err", "d10_t", "spread", "spread_t", "spread_dol",
            "spread_t_dol", "slope_aux"]
    add(tbl(t[cols]))


# ---------------- header ----------------
cov = json.load(open(f"{BASE}/build_universe_coverage.json"))
bmeta = json.load(open(f"{BASE}/flb_base_meta.json"))
add("<h1>Embedding-based intrinsic difficulty — decile-first report "
    "(v5, 2026-08-24)</h1>")
add("<div class='prov'><b>Provenance.</b> All numbers are rendered from "
    "artifacts under <code>/mnt/data/embedding_difficulty/</code> produced by "
    "committed scripts in <code>analysis/embedding_difficulty/</code> (script "
    "list in render_report.py header). Standard trade filters; CGM 3-way "
    "clustered SEs (day × wallet × market); 5,000-trade slice floor; mature "
    "(25–80% of lifetime) and closing (80–100%) windows; count- and "
    "dollar-weighted. Embeddings: BAAI/bge-small-en-v1.5. <b>v5 spec change "
    "(project decision 2026-08-24): the primary calibration diagnostic is the "
    "full price-decile profile; the signed slope is retained in artifacts as "
    "an auxiliary summary only.</b></div>")
how("Every market slice gets a 10-bin calibration profile: trades are grouped "
    "by price decile (D1 = cheapest longshots, D10 = priciest favorites) and "
    "each bin shows the mean of (win − price) — the <b>calibration error</b>. "
    "A perfectly priced bin sits at 0. <b>Classic favorite–longshot bias = "
    "D1 below zero (longshots overpriced) together with D10 above zero "
    "(favorites underpriced)</b>; the reverse pattern is longshots "
    "underpriced. Three views recur: (i) <b>tail panels</b> — D1 (red "
    "circles) and D10 (blue squares) errors with 95% CIs across slices; "
    "(ii) <b>heatmaps</b> — every slice's full 10-decile profile, red = "
    "positive error (bin wins more than its price), blue = negative, black "
    "dot = |t| &gt; 1.96; (iii) <b>D10−D1 spread</b> as a one-number "
    "summary, always read alongside the full profile, never alone. Tail-"
    "decile trade counts (d1_n, d10_n) appear in every table — thin tails "
    "make noisy tail errors, so treat small-count cells with suspicion.")

# ---------------- 1. data ----------------
add(f"<h2>1. Data</h2><p>Universe: <b>{cov['markets_universe']:,}</b> "
    f"non-up/down markets with question+rules text "
    f"({cov['with_created_at']:,} with native created_at); "
    f"{cov['markets_ge1_filtered_trade']:,} with ≥1 standard-filtered trade. "
    f"Filtered BUY trades: {bmeta['buy_filtered_rows']:,} → mature window "
    f"{bmeta['rows_mature']:,}, closing {bmeta['rows_closing']:,}.</p>")
add("<div class='note'><b>Two data-plumbing findings from this build (affect "
    "any standard-filter run on the June-2026 extended trade set):</b> (a) "
    "the March resolutions spine covers only ~49% of extended trade rows — "
    "this build uses the fresh June-24 spine (100% token coverage); (b) "
    "trades' <code>eventSlug</code> is empty for newer markets, so the "
    f"standard up/down exclusion catches ~nothing; {cov['markets_updown_flagged']:,} "
    "up/down markets (~1.34B raw rows) were excluded at market level from "
    "Gamma metadata. Shared fix: <code>scripts/build_market_flags.py</code>."
    "</div>")

# ---------------- 2. baseline ----------------
add("<h2>2. Baseline calibration</h2>")
how("The pooled decile curves are the reference: if the whole sample were "
    "classically FLB-biased the curve would slope up left-to-right through "
    "zero. The category panels then show each curated category's tails and "
    "full profile.")
for win in ("mature", "closing"):
    d = deciles("all", win)
    if d is not None and len(d):
        add(decile_curve(d[d["slice"] == "ALL"], f"Pooled — {win} window"))
add_dtable("all", "mature")
add_dtable("all", "closing")

cat_t = dtable("category", "mature")
if cat_t is not None:
    order = cat_t.sort_values("spread")["slice"].tolist()
    add("<h3>By curated category (mature)</h3>")
    add(tail_panel("category", "mature", "tail errors by category — mature",
                   order=order))
    add(decile_heatmap("category", "mature",
                       "decile profiles by category — mature", order=order))
    add_dtable("category", "mature", order=order)
ser = dtable("series_membership", "mature")
if ser is not None:
    add("<h3>Series membership (recurrence axis)</h3>")
    add_dtable("series_membership", "mature")

# ---------------- 3. PCA ----------------
add("<h2>3. Approach A — PCA structure of question space</h2>")
how("Markets are sorted along each principal component of the question-"
    "embedding space and cut into quintiles (q1 = lowest). The heatmaps show "
    "each quintile's full decile profile; the correlation table is the "
    "guardrail against interpreting components that merely encode question "
    "length or attention.")
evr = json.load(open(f"{BASE}/pca_evr.json"))["evr"]
fig, ax = plt.subplots(figsize=(5, 2.6))
ax.bar(range(1, 21), evr[:20], color=C_D10)
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
    sch = f"pca_pc{i}_quintile"
    if deciles(sch, "mature") is not None:
        o = sorted(deciles(sch, "mature")["slice"].unique())
        add(decile_heatmap(sch, "mature",
                           f"decile profiles by PC{i} quintile — mature",
                           order=o))
add_dtable("pca_pc1_quintile", "mature")

# ---------------- 4. novelty ----------------
add("<h2>4. Approach B — novelty / precedent density at birth</h2>")
nmeta = json.load(open(f"{BASE}/novelty_meta.json"))
hub = json.load(open(f"{BASE}/novelty_hubness.json"))
how("For every market: similarity of its question to markets created "
    "STRICTLY before it (no lookahead); the _x variant excludes same-series/"
    "same-event predecessors. Deciles within birth year: <b>d01 = most novel "
    "of its era, d10 = most precedented</b>. In the heatmaps the rows are "
    "novelty deciles and the columns are PRICE deciles — the question is "
    "whether the top row (most novel markets) shows the classic tail "
    "pattern (blue D1 cell, red D10 cell) while lower rows sit near white.")
add(f"<p>τ = {nmeta['tau']:.3f} ({nmeta['tau_quantile']} quantile of "
    f"random-pair similarity). Birth fallback for "
    f"{nmeta['birth_fallback_n']:,} markets. Hubness: k-occurrence skewness "
    f"{hub['k_occurrence_skewness']:.1f} (max {hub['max_occurrence']:,}) — "
    "high; rank-based deciles soften it; mutual-proximity rescale pending.</p>")
dist = pd.read_parquet(f"{BASE}/novelty_dist.parquet")
fig, ax = plt.subplots(figsize=(5, 2.8))
ax.plot(dist["year"], dist["p50"], "-o", ms=3, color=C_D10, label="median")
ax.fill_between(dist["year"], dist["p10"], dist["p90"], alpha=0.25,
                color=C_D10, label="p10–p90")
ax.set_ylabel("sim_k25_x")
ax.set_title("novelty distribution by vintage (excl. same-series neighbors)")
ax.legend()
add(fig64(fig))
conf = pd.read_parquet(f"{BASE}/novelty_confounds.parquet")
add("<h3>Confound table</h3>")
add(tbl(conf[conf["target"] == "sim_k25_x"], "{:+.3f}"))
for sch, ttl in (("nov_k25x_vint", "novelty deciles within vintage year"),
                 ("nov_k25x", "novelty deciles (excl. same-series/event)"),
                 ("nov_cnt", "precedent-count bins")):
    if deciles(sch, "mature") is not None:
        o = sorted(deciles(sch, "mature")["slice"].unique())
        add(decile_heatmap(sch, "mature", f"{ttl} — mature", order=o))
add("<h3>Within-vintage novelty deciles — table (mature)</h3>")
add_dtable("nov_k25x_vint", "mature")
if deciles("nov_k25x_vint", "closing") is not None:
    o = sorted(deciles("nov_k25x_vint", "closing")["slice"].unique())
    add(decile_heatmap("nov_k25x_vint", "closing",
                       "novelty deciles within vintage year — closing",
                       order=o))
ex = pd.read_parquet(f"{BASE}/novelty_examples.parquet")
add("<h3>Qualitative anchors</h3>")
add(ex.to_html(index=False, border=0))

# ---- 4b. field variants ----
if os.path.exists(f"{BASE}/field_compare.parquet"):
    add("<h2>4b. Multi-field text variants: question vs. rules vs. context</h2>")
    how("The novelty pipeline re-run on embeddings of the RULES text and the "
        "event-level CONTEXT, plus pre-registered combined weightings. The "
        "table shows each variant's most-novel slice: its D1/D10 tail errors "
        "and spread — which text field carries the difficulty signal.")
    fc = pd.read_parquet(f"{BASE}/field_compare.parquet")
    add(tbl(fc, "{:+.3f}"))
    if os.path.exists(f"{BASE}/novelty_port_check.json"):
        pc = json.load(open(f"{BASE}/novelty_port_check.json"))
        add(f"<p><i>Engine check:</i> torch novelty engine reproduces the "
            f"numpy scores (corr = {pc['corr']:.6f}).</p>")
    rows = []
    variants = [("q (question)", "nov_k25x_vint", "novx_vint_f10k")]
    for v in ("rules", "context", "comb_eq", "comb_qc"):
        variants.append((v, f"nv_{v}", f"nv_{v}_f10k"))
    for label, sch, schf in variants:
        for sub, name in ((sch, "all viable"), (schf, ">=$10k")):
            t = dtable(sub, "mature")
            if t is None:
                continue
            r = t.sort_values("slice").iloc[0]
            rows.append({"variant": label, "sample": name,
                         "d1_err": r["d1_err"], "d1_t": r["d1_t"],
                         "d1_n": int(r["d1_n"]),
                         "d10_err": r["d10_err"], "d10_t": r["d10_t"],
                         "spread": r["spread"], "spread_t": r["spread_t"]})
    if rows:
        add("<h3>Most-novel slice (d01) tails by variant — mature</h3>")
        add(tbl(pd.DataFrame(rows)))

# ---------------- 5. liquidity ----------------
add("<h2>5. Liquidity: the FLB–liquidity gradient and inclusion floors</h2>")
lmeta = json.load(open(f"{BASE}/liquidity_meta.json"))
how("Liquidity proxy = the market's dollar volume under standard filters. "
    "The heatmap rows run from thinnest (&lt;$1k) to deepest (≥$1M) "
    "markets; classic FLB in a row = blue left cell, red right cell.")
o = sorted(deciles("liq_tier", "mature")["slice"].unique()) \
    if deciles("liq_tier", "mature") is not None else None
if o:
    add(decile_heatmap("liq_tier", "mature",
                       "decile profiles by volume tier — mature", order=o))
    add(decile_heatmap("liq_tier", "mature",
                       "volume tiers — mature, dollar-weighted", order=o,
                       dollar=True))
    add(tail_panel("liq_tier", "mature",
                   "tail errors by volume tier — mature", order=o))
    add_dtable("liq_tier", "mature", order=o)
if deciles("liq_tier", "closing") is not None:
    add(decile_heatmap("liq_tier", "closing",
                       "volume tiers — closing", order=o))
if deciles("liq_pctl_vint", "mature") is not None:
    o2 = sorted(deciles("liq_pctl_vint", "mature")["slice"].unique())
    add(decile_heatmap("liq_pctl_vint", "mature",
                       "era-relative volume quintiles (within birth month) "
                       "— mature", order=o2))

add("<h3>Inclusion-floor sensitivity (pooled tails under floors)</h3>")
how("Each row re-estimates the POOLED profile after dropping markets below "
    "a volume floor; 'rollmed25' keeps markets ≥25% of the trailing-90-day "
    "median volume. If tails moved materially with the floor, thin markets "
    "were driving them.")
rows = []
for sch, label, kept in (("all", "no floor", None),
                         ("all_f1k", ">= $1k", "floor_1k_markets_kept"),
                         ("all_f10k", ">= $10k", "floor_10k_markets_kept"),
                         ("all_f100k", ">= $100k", "floor_100k_markets_kept"),
                         ("rollmed25", "rolling-median 25%",
                          "rollmed_markets_kept")):
    t = dtable(sch, "mature")
    if t is None:
        continue
    r = t.iloc[0]
    rows.append({"floor": label,
                 "markets_kept": lmeta.get(kept,
                                           cov["markets_ge1_filtered_trade"]),
                 "n_trades": int(r["n_trades"]),
                 "d1_err": r["d1_err"], "d1_t": r["d1_t"],
                 "d10_err": r["d10_err"], "d10_t": r["d10_t"],
                 "spread": r["spread"], "spread_t": r["spread_t"]})
add(tbl(pd.DataFrame(rows)))
add(f"<p>Rolling rule excludes {lmeta['rollmed_share_excluded']:.1%} of "
    "trade-viable markets (~0.7% of trades).</p>")
if deciles("novx_vint_f10k", "mature") is not None:
    o3 = sorted(deciles("novx_vint_f10k", "mature")["slice"].unique())
    add("<h3>Is the novelty tail just illiquidity?</h3>")
    add(decile_heatmap("novx_vint_f10k", "mature",
                       "novelty deciles within vintage, ≥$10k markets only "
                       "— mature", order=o3))

# ---------------- 5b. horizon ----------------
if deciles("horizon", "mature") is not None:
    add("<h2>5b. Horizon (contract lifetime)</h2>")
    how("Horizon = market creation → close. Rows run from &lt;1 day to ≥90 "
        "days. Watch how the tail pattern rotates: which tail carries the "
        "short-horizon error, and does the long-horizon row show the classic "
        "blue-left/red-right shape? The within-category panels separate "
        "composition (families living at short horizons) from a genuine "
        "horizon gradient.")
    ho = ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d", "h5_ge90d"]
    for win in ("mature", "closing"):
        if deciles("horizon", win) is not None:
            add(decile_heatmap("horizon", win,
                               f"decile profiles by horizon — {win}",
                               order=ho))
    add(tail_panel("horizon", "mature", "tail errors by horizon — mature",
                   order=ho))
    add_dtable("horizon", "mature", order=ho)
    add_dtable("horizon", "closing", order=ho)
    hv = pd.read_parquet(f"{BASE}/horizon_volume.parquet")
    add("<h3>Where do trades vs. dollars sit across horizons?</h3>")
    add(tbl(hv, "{:,.3f}"))
    dec_hc = deciles("horizon_cat", "mature")
    if dec_hc is not None:
        d1 = dec_hc[dec_hc["decile"] == 1].copy()
        d10 = dec_hc[dec_hc["decile"] == 10].copy()
        for dd, ttl, col in ((d1, "D1 (longshot) error", C_D1),
                             (d10, "D10 (favorite) error", C_D10)):
            parts = dd["slice"].str.split("|", expand=True)
            dd["cat"], dd["hbin"] = parts[0], parts[1]
            big = dd.groupby("cat")["n"].sum().nlargest(8).index
            fig, ax = plt.subplots(figsize=(6.4, 3.4))
            for c in big:
                sub = dd[dd["cat"] == c].set_index("hbin").reindex(ho)
                ax.plot(range(len(ho)), sub["cal_error"], "-o", ms=3,
                        label=c)
            ax.axhline(0, color="k", lw=0.8)
            ax.set_xticks(range(len(ho)))
            ax.set_xticklabels([h.split("_", 1)[1] for h in ho])
            ax.set_ylabel("calibration error")
            ax.set_title(f"{ttl} by horizon WITHIN category — mature")
            ax.legend(fontsize=7, ncol=2)
            add(fig64(fig))
        add("<h3>Category × horizon cells (mature)</h3>")
        add_dtable("horizon_cat", "mature")
    add("<h3>Recurrence and anchorability (native fields)</h3>")
    for sch in ("recurrence", "anchor"):
        if deciles(sch, "mature") is not None:
            o4 = sorted(deciles(sch, "mature")["slice"].unique())
            add(decile_heatmap(sch, "mature",
                               f"decile profiles by {sch} — mature",
                               order=o4))
            add_dtable(sch, "mature", order=o4)
    if dtable("novtail_cat", "mature") is not None:
        add("<h3>Novelty tail within each category (mature)</h3>")
        how("Per category: the most-novel within-vintage decile ('tail') vs "
            "everything else ('rest'). Compare each pair's D1/D10 errors — "
            "does novelty push the category's tails toward the classic "
            "pattern?")
        add_dtable("novtail_cat", "mature")

# ---------------- 6. granularity ----------------
add("<h2>6. Approach C — how much heterogeneity does each granularity "
    "reveal?</h2>")
how("Markets clustered on embeddings at four granularities; per-cluster "
    "decile profiles summarized by the D10−D1 spread. signal_sd = the "
    "noise-corrected dispersion of TRUE spreads across slices — how much "
    "real calibration heterogeneity exists at that granularity. Rising "
    "signal_sd with k means finer slices keep revealing structure that "
    "coarser ones average away. (Slope-based version in prior report "
    "versions; ordering of granularities is unchanged.)")
disp_rows = []
for sch in ("category", "cluster_k12", "cluster_k50", "cluster_k200",
            "cluster_k1000"):
    s = summary(sch, "mature")
    if s is not None and len(s) > 2:
        disp_rows.append({"scheme": sch, **spread_dispersion(s)})
if disp_rows:
    add(tbl(pd.DataFrame(disp_rows)))
for k in (50, 200):
    s = summary(f"cluster_k{k}", "mature")
    if s is None:
        continue
    terms = pd.read_parquet(f"{BASE}/cluster_terms_k{k}.parquet")
    m = s.merge(terms, left_on="slice", right_on="cluster")
    fig, ax = plt.subplots(figsize=(5.4, 3.2))
    ax.scatter(np.log10(m["n_trades"]), m["spread"], s=12, alpha=0.6,
               color=C_D10)
    ax.axhline(0, color="k", lw=0.8)
    ax.set_xlabel("log10 slice trades")
    ax.set_ylabel("D10−D1 spread")
    ax.set_title(f"k={k}: per-cluster spread vs size — mature")
    add(fig64(fig))
    m = m.sort_values("spread")
    cols = ["slice", "n_trades", "spread", "spread_t", "top_terms", "ex1"]
    add(f"<h3>k={k}: most negative / most positive spread clusters</h3>")
    add(pd.concat([m.head(8), m.tail(8)])[cols].to_html(
        index=False, border=0, float_format=lambda x: f"{x:+.3f}"))

# ---------------- 7. action/subject ----------------
add("<h2>7. Approach D — action × subject precedent (exploratory)</h2>")
add("<p class='note'>Stage-2 labels cover 379K/850K markets (59% of filtered "
    "trades), none after the pre-June universe — vintage-confounded; "
    "suggestive only.</p>")
how("Markets decomposed as subject × action; bins by how many prior markets "
    "shared the action / a subject. The heatmaps show each bin's full "
    "profile — the question is whether low-precedent bins show the classic "
    "tail pattern and high-precedent bins sit near white.")
for sch, ttl in (("act_prec", "action precedent count"),
                 ("subj_prec", "subject precedent count"),
                 ("actsubj_2x2", "action-seen × subject-seen"),
                 ("act_prec_vint",
                  "action precedent, quintiles WITHIN vintage year")):
    if deciles(sch, "mature") is not None:
        o5 = sorted(deciles(sch, "mature")["slice"].unique())
        add(decile_heatmap(sch, "mature", f"{ttl} — mature", order=o5))
        add_dtable(sch, "mature", order=o5)

# ---------------- 8. caveats ----------------
add("<h2>8. Caveats & open items</h2><ul>"
    "<li><b>Thin-tail guard:</b> D1/D10 errors from cells with small d1_n / "
    "d10_n are noisy and composition-sensitive; sign claims require the "
    "full decile profile and adequate tail counts — never the spread "
    "alone.</li>"
    "<li>Resolution censoring: the trade set contains only markets resolved "
    "by build time; long-horizon and late-vintage cells are "
    "horizon-censored.</li>"
    "<li>wallet_flags built 2026-06-11; bot coverage of newest-era wallets "
    "unaudited.</li>"
    "<li>Liquidity proxy is realized volume (an outcome); floors are "
    "inclusion-sensitivity checks, not causal controls.</li>"
    "<li>Hubness in the neighbor graph is high; mutual-proximity rescale "
    "pending. Encoder robustness and lexical baseline pending.</li>"
    "<li>All slicings cross-sectional; within-series designs are the "
    "natural next step.</li></ul>")
add("</body></html>")

with open(f"{OUT}/embedding_difficulty_report.html", "w") as f:
    f.write("\n".join(H))
print(f"wrote {OUT}/embedding_difficulty_report.html", flush=True)
