"""Standalone brief: liquidity x maturity disentangling (sessions 6/6b).

Self-contained HTML with ONLY the 2026-08-27 disentangling results:
de-confounding stats, volume-rate quartiles, horizon x liquidity crosses,
clean-maturity samples (standalone binaries / final-two), fixed-window
(first-1d) liquidity, dropout-contamination stats. Decile-first per the
2026-08-24 spec (D1/D10 tail errors + spread; slope auxiliary).

Artifacts: flb_{summary,deciles}_{liqrate, liqrate_vint, hor_x_liqrate,
horizon_binary, horizon_final2, liq1d, hor_x_liq1d}_{mature,closing}.parquet
+ liq_horizon_meta.json, produced by make_liq_horizon_slices.py,
make_timeliq_slices.py, run_schemes.py.

Runs on EC2 (artifact root /mnt/data/embedding_difficulty) or locally against
the pulled mirror (analysis/embedding_difficulty/output_session1) — the root
is auto-detected.
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

EC2_BASE = "/mnt/data/embedding_difficulty"
LOCAL_MIRROR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "output_session1")
if os.path.isdir(EC2_BASE):
    ART, META_DIR, OUT_DIR = (f"{EC2_BASE}/output", EC2_BASE,
                              f"{EC2_BASE}/report")
else:
    ART = META_DIR = OUT_DIR = LOCAL_MIRROR
os.makedirs(OUT_DIR, exist_ok=True)
plt.rcParams.update({"figure.dpi": 110, "font.size": 9,
                     "axes.grid": True, "grid.alpha": 0.3})
C_D1, C_D10, CMAP = "#D55E00", "#0072B2", "RdBu_r"
HO = ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d", "h5_ge90d"]
WIN = "full"  # primary window for these analyses (JW decision 2026-08-27);
# falls back to mature if full-window artifacts are absent



def deciles(scheme, win):
    p = f"{ART}/flb_deciles_{scheme}_{win}.parquet"
    return pd.read_parquet(p) if os.path.exists(p) else None


def summary(scheme, win):
    p = f"{ART}/flb_summary_{scheme}_{win}.parquet"
    return pd.read_parquet(p) if os.path.exists(p) else None


def fig64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return ("<img src='data:image/png;base64,"
            + base64.b64encode(buf.getvalue()).decode() + "'/>")


def short_labels(labels):
    if len(labels) < 2:
        return labels
    pref = os.path.commonprefix(labels)
    cut = pref.rfind("_") + 1
    return [l[cut:] for l in labels] if cut >= 2 else labels


def heatmap(scheme, win, title, order):
    dec = deciles(scheme, win)
    if dec is None:
        return ""
    piv = dec.pivot(index="slice", columns="decile", values="cal_error")
    pse = dec.pivot(index="slice", columns="decile", values="se")
    labs = [l for l in order if l in piv.index]
    M = piv.loc[labs].to_numpy(float)
    T = M / pse.loc[labs].to_numpy(float)
    vmax = max(float(np.nanquantile(np.abs(M), 0.98)), 0.02)
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
    cb.set_label("calibration error", fontsize=8)
    ax.set_title(title + "  (· = |t| > 1.96)")
    return fig64(fig)


def dtable(scheme, win, order=None):
    dec, s = deciles(scheme, win), summary(scheme, win)
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
    out = out.reset_index()
    if order:
        out = out.set_index("slice").reindex(
            [o for o in order if o in set(out["slice"])]).reset_index()
    return out


if not os.path.exists(f"{ART}/flb_summary_liqrate_{WIN}.parquet"):
    WIN = "mature"


def cell(scheme, sl, col):
    t = dtable(scheme, WIN)
    r = t.set_index("slice").loc[sl]
    return float(r[col])


H = ["<html><head><meta charset='utf-8'><title>Liquidity × maturity — "
     "disentangling brief</title><style>",
     "body{font-family:Georgia,serif;max-width:1000px;margin:24px auto;"
     "padding:0 16px;line-height:1.45;color:#1a1a1a}",
     "h1{font-size:21px} h2{font-size:17px;border-bottom:1px solid #ccc;"
     "padding-bottom:4px;margin-top:30px}",
     "table{border-collapse:collapse;font-size:12px;font-family:Menlo,monospace}",
     "td,th{border:1px solid #ddd;padding:3px 7px;text-align:right}",
     "th{background:#f2f2f2} .prov{background:#eef3f8;border-left:4px solid "
     "#4a7dab;padding:8px 12px;font-size:12.5px} .how{background:#f0f7f0;"
     "border-left:4px solid #6aa84f;padding:8px 12px;font-size:13px;"
     "margin:8px 0} .key{background:#fff8e1;border-left:4px solid #f0c040;"
     "padding:8px 12px;font-size:13px}",
     "img{max-width:100%}</style></head><body>"]
add = H.append


def tbl(df, fl="{:+.4f}"):
    return df.to_html(index=False, float_format=lambda x: fl.format(x),
                      border=0)


meta = json.load(open(f"{META_DIR}/liq_horizon_meta.json"))

add("<h1>Liquidity × maturity: disentangling brief (2026-08-27)</h1>")
add("<div class='prov'><b>Provenance.</b> Rendered from committed-script "
    "artifacts (make_liq_horizon_slices.py, make_timeliq_slices.py, "
    "run_schemes.py; renderer render_liq_maturity_brief.py). Standard "
    "filters; window: '" + WIN + "' lifecycle (full = 0–100% of contract "
    "lifetime — JW decision 2026-08-27 for these analyses); CGM 3-way "
    "clustered SEs; decile-first spec (2026-08-24). "
    "D1 = calibration error of the cheapest price decile (longshots), "
    "D10 = priciest (favorites); classic FLB = D1 &lt; 0 with D10 &gt; 0.</div>")

add("<h2>1. The confound, quantified</h2>")
add(f"<p>corr(log horizon, log TOTAL volume) = "
    f"<b>{meta['corr_loghorizon_logtotalusd']:+.3f}</b> — total volume "
    "accumulates mechanically with time-open. Re-measured as a RATE "
    "(standard-filtered dollars per day of market life): corr(log horizon, "
    f"log volume rate) = <b>{meta['corr_loghorizon_logvolrate']:+.3f}</b> — "
    "per day, short-horizon markets are the MORE liquid ones. The two axes "
    "are separable once liquidity is a rate.</p>")

add("<h2>2. Volume-rate quartiles (liquidity de-confounded from age)</h2>")
add(f"<div class='key'>Classic two-tailed FLB across the bottom THREE rate "
    f"quartiles — e.g. rq2: D1 {cell('liqrate','rq2','d1_err'):+.4f} "
    f"(t={cell('liqrate','rq2','d1_t'):+.1f}) / D10 "
    f"{cell('liqrate','rq2','d10_err'):+.4f} "
    f"(t={cell('liqrate','rq2','d10_t'):+.1f}); rq3: "
    f"{cell('liqrate','rq3','d1_err'):+.4f} / "
    f"{cell('liqrate','rq3','d10_err'):+.4f} — not just a thin-market "
    f"sliver. The top per-day quartile differs: D1 "
    f"{cell('liqrate','rq4','d1_err'):+.4f} "
    f"(t={cell('liqrate','rq4','d1_t'):+.1f}).</div>")
add(heatmap("liqrate", WIN,
            "volume-rate quartiles (rq1 thinnest … rq4 deepest) — " + WIN + "",
            ["rq1", "rq2", "rq3", "rq4"]))
add(tbl(dtable("liqrate", WIN, ["rq1", "rq2", "rq3", "rq4"])))
if deciles("liqrate_vint", WIN) is not None:
    add("<p>Within-birth-month version (era-controlled):</p>")
    add(heatmap("liqrate_vint", WIN,
                "volume-rate quartiles WITHIN birth month — " + WIN + "",
                ["rqv1", "rqv2", "rqv3", "rqv4"]))
    add(tbl(dtable("liqrate_vint", WIN,
                   ["rqv1", "rqv2", "rqv3", "rqv4"])))

add("<h2>3. The cross: horizon × within-bin volume-rate quartile</h2>")
add("<div class='how'>Quartiles are formed WITHIN each horizon bin, so every "
    "horizon stratum has balanced liquidity groups. Read across a horizon's "
    "four rows for the liquidity effect net of horizon; compare same-quartile "
    "rows across horizons for the horizon effect net of liquidity.</div>")
add(f"<div class='key'>Liquidity mostly wins: the classic pattern holds in "
    f"rq1–rq3 at every horizon. What remains of horizon: (a) the top "
    f"quartile's longshot-underpricing is a SHORT-horizon phenomenon "
    f"(h1|rq4: D1 {cell('hor_x_liqrate','h1_lt1d|rq4','d1_err'):+.4f}, "
    f"t={cell('hor_x_liqrate','h1_lt1d|rq4','d1_t'):+.1f} — fast recurring "
    f"match-type markets); (b) at ≥90 days even the deepest quartile is "
    f"classic-shaped (h5|rq4: D1 "
    f"{cell('hor_x_liqrate','h5_ge90d|rq4','d1_err'):+.4f} / D10 "
    f"{cell('hor_x_liqrate','h5_ge90d|rq4','d10_err'):+.4f}).</div>")
OC = [f"{h}|rq{q}" for h in HO for q in (1, 2, 3, 4)]
add(heatmap("hor_x_liqrate", WIN,
            "horizon × within-bin volume-rate quartile — " + WIN + "", OC))
add(tbl(dtable("hor_x_liqrate", WIN, OC)))

add("<h2>4. Maturity measured cleanly (dropout fix)</h2>")
add(f"<p>Of {meta['multi_markets_with_end']:,} multi-event markets, "
    f"{meta['dropout_markets']:,} ({meta['dropout_share_of_multi']:.1%}) "
    "resolved &gt;2 days before their event's end (dropout-style early "
    f"resolution). Clean samples: {meta['horizon_binary_markets']:,} "
    f"standalone-binary markets; {meta['horizon_final2_markets']:,} "
    "final-two markets.</p>")
add(f"<div class='key'>Standalone always-binary markets (no dropouts "
    f"possible): <b>classic FLB at ≥90 days — D1 "
    f"{cell('horizon_binary','h5_ge90d','d1_err'):+.4f} "
    f"(t={cell('horizon_binary','h5_ge90d','d1_t'):+.1f}), D10 "
    f"{cell('horizon_binary','h5_ge90d','d10_err'):+.4f} "
    f"(t={cell('horizon_binary','h5_ge90d','d10_t'):+.1f})</b>; shorter "
    f"horizons per table below. Long-dated true binaries are where the "
    f"classic bias lives.</div>")
add(heatmap("horizon_binary", WIN,
            "horizon — standalone binaries only — " + WIN + "", HO))
add(tbl(dtable("horizon_binary", WIN, HO)))
add("<div class='how'>Caveat on the final-two sample: multi-market events "
    "include sports GAME events (moneyline/spread/totals share one event), "
    "so this sample is sports-heavy and shows the sports families' reverse "
    "tails at short horizons — the dropping rule needs a winner-take-all "
    "(negRisk) or Politics restriction to isolate elections as intended. "
    "Next refinement.</div>")
add(heatmap("horizon_final2", WIN,
            "horizon — binaries + final-two of multi events — " + WIN + "", HO))
add(tbl(dtable("horizon_final2", WIN, HO)))

add("<h2>5. Fixed-window ('cross-sectional time') liquidity: first 1 day</h2>")
add("<div class='how'>Volume in a FIXED window after the market's first "
    "filtered trade equalizes the accumulation footprint. Pooled first-1d "
    "quartiles wash out because volume TIMING differs by market type "
    "(sports trade near close; long markets trade early and slowly), so the "
    "within-horizon cross is the informative view. First-7d/30d versions "
    "behave similarly (full report, section 5c).</div>")
add(heatmap("liq1d", WIN,
            "first-1-day volume quartiles (pooled) — " + WIN + "",
            ["w1q1", "w1q2", "w1q3", "w1q4"]))
OC1 = [f"{h}|w1q{q}" for h in HO for q in (1, 2, 3, 4)]
add(heatmap("hor_x_liq1d", WIN,
            "horizon × within-bin first-1d volume quartile — " + WIN + "", OC1))
add(tbl(dtable("hor_x_liq1d", WIN, OC1)))

add("<h2>6. Caveats</h2><ul>"
    "<li>Thin-tail guard: read D1/D10 with their bin counts (d1_n, d10_n); "
    "no sign claims from the spread alone.</li>"
    "<li>Volume (rate or windowed) is an equilibrium outcome — these are "
    "conditioning/sensitivity views, not causal controls.</li>"
    "<li>Long-horizon cells are resolution-censored (only resolved markets "
    "are in the trade set).</li>"
    "<li>Dropout rule = resolving &gt;2 days before the event's last "
    "resolution; final-two = latest-surviving two markets per multi-market "
    "event (ties by volume).</li></ul>")
add("</body></html>")

out = f"{OUT_DIR}/liq_maturity_brief.html"
with open(out, "w") as f:
    f.write("\n".join(H))
print(f"wrote {out}")
