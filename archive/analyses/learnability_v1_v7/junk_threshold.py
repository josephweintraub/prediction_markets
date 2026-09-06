"""Choosing the junk-market floor x on rolling relative volume.

Relative volume r_m = (std-filtered $ volume of market m) / (median volume of
markets whose first trade falls in the 90 days before m's first trade;
expanding window until 30 such markets exist). Count variant analogous.

A market is 'junk' where its price stops being information. Two estimators:
  1. SKILL CROSSING: per ln-r bin, Brier skill of the mature Yes-price vs the
     market's own family precedent base rate (>=10 precedents). x* = lowest r
     above which bin skill is positive with t >= 2 for all higher bins.
  2. THRESHOLD REGRESSION: trade-level two-regime model won = a_s + b_s p,
     regimes r < x vs r >= x, over a grid of candidate x (quantiles of r);
     x* = argmin SSR. The b_low/b_high profile shows how sharp the break is.
Diagnostics per bin: raw informativeness slope (won on p; 1 = calibrated,
0 = uninformative), skill, n, markets, $.
Outputs: junk_threshold_bins.parquet, junk_threshold_grid.parquet,
junk_threshold_summary.json, junk_rel_volume.parquet (per-market r).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
BR = "/mnt/data/learnability/output/baserate_markets.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


t0 = time.time()
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

# per-market volume + birth (first trade day), rolling median benchmark
mk = con.execute(f"""
    SELECT market_id, min(trade_day) AS birth, count(*) AS n_tr,
           sum(usdc) AS vol
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1)
    GROUP BY 1
""").fetchdf()
mk["birth"] = pd.to_datetime(mk["birth"])
mk = mk.sort_values("birth").reset_index(drop=True)
births = mk["birth"].to_numpy(dtype="datetime64[ns]")
vols = mk["vol"].to_numpy()
ntrs = mk["n_tr"].to_numpy()
lo = np.searchsorted(births, births - np.timedelta64(90, "D"), side="left")
rollmed = np.empty(len(mk))
rollmed_n = np.empty(len(mk))
for i in range(len(mk)):
    a = lo[i]
    if i - a < 30:
        a = max(0, i - 30)
    w = vols[a:i] if i > a else vols[:1]
    rollmed[i] = np.median(w) if len(w) else np.nan
    wn = ntrs[a:i] if i > a else ntrs[:1]
    rollmed_n[i] = np.median(wn) if len(wn) else np.nan
mk["r_vol"] = mk["vol"] / np.maximum(rollmed, 1e-9)
mk["r_cnt"] = mk["n_tr"] / np.maximum(rollmed_n, 1e-9)
mk[["market_id", "birth", "vol", "n_tr", "r_vol", "r_cnt"]].to_parquet(
    OUT / "junk_rel_volume.parquet")
log(f"markets: {len(mk):,}; median r_vol={np.nanmedian(mk.r_vol):.3f}")

# trade-level with r
df = con.execute(f"""
    SELECT t.price, t.won, t.usdc, t.market_id
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    WHERE t.pos BETWEEN 0.25 AND 0.80
""").fetchdf()
rmap = mk.set_index("market_id")["r_vol"]
df["r"] = rmap.reindex(df["market_id"]).to_numpy()
df = df[np.isfinite(df["r"])]
lnr = np.log(np.maximum(df["r"].to_numpy(), 1e-9))

# --- bins: informativeness slope + skill ---
edges = np.quantile(lnr, np.linspace(0, 1, 21))
edges[0] -= 1e-9
bin_id = np.clip(np.searchsorted(edges, lnr, side="right") - 1, 0, 19)
brm = pd.read_parquet(BR)[["market_id", "skill"]].dropna().set_index("market_id")
mk2 = mk.set_index("market_id").join(brm)
rows = []
p = df["price"].to_numpy(float)
y = df["won"].to_numpy(float)
for b in range(20):
    m = bin_id == b
    pp, yy = p[m], y[m]
    pv = pp - pp.mean()
    slope = (pv * yy).sum() / (pv * pv).sum()
    rlo, rhi = np.exp(edges[b]), np.exp(edges[b + 1])
    mkb = mk2[(mk2.r_vol >= rlo) & (mk2.r_vol < rhi)]
    sk = mkb["skill"].dropna()
    rows.append({"bin": b, "r_lo": rlo, "r_hi": rhi, "n_trades": int(m.sum()),
                 "n_markets": int(len(mkb)), "usd": float(df["usdc"][m].sum()),
                 "info_slope": slope,
                 "skill_mean": sk.mean() if len(sk) else np.nan,
                 "skill_t": (sk.mean() / (sk.std() / np.sqrt(len(sk)))
                             if len(sk) > 5 else np.nan),
                 "n_skill": int(len(sk))})
    log(f"  bin {b:2d} r=[{rlo:8.3f},{rhi:8.3f}) info_slope={slope:+.3f} "
        f"skill={rows[-1]['skill_mean'] if len(sk) else float('nan'):+.4f} "
        f"(t={rows[-1]['skill_t'] if len(sk)>5 else float('nan'):+.1f}) "
        f"mkts={len(mkb):,}")
bins = pd.DataFrame(rows)
bins.to_parquet(OUT / "junk_threshold_bins.parquet")

# skill crossing: lowest bin b* such that all bins >= b* have skill t >= 2
ok = (bins.skill_t >= 2).to_numpy()
bstar = None
for b in range(20):
    if ok[b:].all():
        bstar = b
        break
x_skill = float(bins.r_lo.iloc[bstar]) if bstar is not None else np.nan
log(f"skill-crossing x* = {x_skill:.3f} (bin {bstar})")

# threshold regression on the trade level
grid_q = np.linspace(0.02, 0.80, 40)
grid_x = np.quantile(df["r"].to_numpy(), grid_q)
grows = []
for x in grid_x:
    m = df["r"].to_numpy() < x
    ssr = 0.0
    seg = {}
    for s, mm in [("lo", m), ("hi", ~m)]:
        pp, yy = p[mm], y[mm]
        if len(pp) < 1000:
            ssr = np.nan
            break
        pv = pp - pp.mean()
        b1 = (pv * yy).sum() / (pv * pv).sum()
        a1 = yy.mean() - b1 * pp.mean()
        e = yy - a1 - b1 * pp
        ssr += (e * e).sum()
        seg[s] = b1
    grows.append({"x": x, "ssr": ssr, "b_lo": seg.get("lo", np.nan),
                  "b_hi": seg.get("hi", np.nan)})
grid = pd.DataFrame(grows)
grid.to_parquet(OUT / "junk_threshold_grid.parquet")
x_thresh = float(grid.loc[grid.ssr.idxmin(), "x"])
gmin = grid.loc[grid.ssr.idxmin()]
log(f"threshold-regression x* = {x_thresh:.3f} "
    f"(b_lo={gmin.b_lo:.3f}, b_hi={gmin.b_hi:.3f})")

# exclusion shares at candidate floors
sh = {}
for x, nm in [(0.25, "x=0.25"), (x_skill, "x=skill"), (x_thresh, "x=thresh"),
              (1.0, "x=1.00")]:
    if not np.isfinite(x):
        continue
    keep = mk["r_vol"] >= x
    sh[nm] = {"x": float(x),
              "mkts_excl": float(1 - keep.mean()),
              "trades_excl": float(1 - mk.n_tr[keep].sum() / mk.n_tr.sum()),
              "usd_excl": float(1 - mk.vol[keep].sum() / mk.vol.sum())}
    log(f"  {nm}: excl {sh[nm]['mkts_excl']:.1%} markets, "
        f"{sh[nm]['trades_excl']:.1%} trades, {sh[nm]['usd_excl']:.2%} $")
json.dump({"x_skill": x_skill, "x_thresh": x_thresh, "shares": sh,
           "note": "r = market vol / trailing-90d median vol at birth"},
          open(OUT / "junk_threshold_summary.json", "w"), indent=2)
log(f"DONE junk_threshold in {(time.time()-t0)/60:.1f} min")
