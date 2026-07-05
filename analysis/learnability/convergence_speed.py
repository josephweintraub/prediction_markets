"""Convergence speed: process-based difficulty, and the within-series
learning test on speed rather than terminal calibration.

Per resolved market (from the full-window BUY intermediate), price error
e(t) = |p - won| averaged in lifecycle bands:
  e_early  pos in [0.05, 0.35]
  e_late   pos in [0.65, 0.95]
  surprise pos in [0.90, 1.00]   (terminal error)
speed = e_early - e_late (how much of the pricing error resolves mid-life).

Tests (market-level, floors: >=50 trades total, >=10 per band):
  1. Within-series: speed_m and surprise_m on ln(1+prior instances) with
     series FE (demeaned OLS), SEs clustered by series. If crowds learn to
     price templates FASTER, speed drifts up / surprise drifts down across
     instances even though terminal calibration (earlier null) does not move.
  2. Cross-section: speed and surprise on the dimension set, cluster-robust
     by text family.
Outputs: convergence_markets.parquet, convergence_tests.parquet
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def cluster_ols(y, X, cl):
    """OLS with one-way cluster-robust SEs. X without constant (added)."""
    X = np.column_stack([np.ones(len(y)), X])
    A = X.T @ X
    b = np.linalg.solve(A, X.T @ y)
    e = y - X @ b
    U = X * e[:, None]
    codes, _ = pd.factorize(cl, sort=False)
    G = codes.max() + 1
    S = np.zeros((G, X.shape[1]))
    for k in range(X.shape[1]):
        S[:, k] = np.bincount(codes, weights=U[:, k], minlength=G)
    V = np.linalg.inv(A) @ (S.T @ S) @ np.linalg.inv(A)
    se = np.sqrt(np.diag(V))
    return b, se


t0 = time.time()
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")
mk = con.execute(f"""
    SELECT t.market_id,
       count(*) n,
       avg(abs(t.price - t.won)) FILTER (WHERE t.pos BETWEEN .05 AND .35) e_early,
       count(*)                  FILTER (WHERE t.pos BETWEEN .05 AND .35) n_early,
       avg(abs(t.price - t.won)) FILTER (WHERE t.pos BETWEEN .65 AND .95) e_late,
       count(*)                  FILTER (WHERE t.pos BETWEEN .65 AND .95) n_late,
       avg(abs(t.price - t.won)) FILTER (WHERE t.pos >= .90) surprise,
       count(*)                  FILTER (WHERE t.pos >= .90) n_surp
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1) t
    GROUP BY 1 HAVING count(*) >= 50
""").fetchdf()
d = con.execute(f"""
    SELECT condition_id, series_slug, prior_instances, cluster_k200,
           life_d, usd_full, (novelty_vint_decile = 1) AS nov_tail,
           vintage_year, sim_k25_x
    FROM read_parquet('{DIMS}')
""").fetchdf().set_index("condition_id")
mk = mk.join(d, on="market_id")
mk = mk[(mk.n_early >= 10) & (mk.n_late >= 10) & mk.cluster_k200.notna()
        & mk.sim_k25_x.notna() & mk.life_d.notna() & mk.vintage_year.notna()]
mk["speed"] = mk["e_early"] - mk["e_late"]
mk["ln_prior"] = np.log1p(mk["prior_instances"])
log(f"markets with convergence metrics: {len(mk):,} "
    f"(mean e_early={mk.e_early.mean():.3f}, e_late={mk.e_late.mean():.3f}, "
    f"surprise={mk.surprise.mean():.3f})")
mk.to_parquet(OUT / "convergence_markets.parquet")

rows = []
# 1. within-series
ins = mk[mk.series_slug.notna()].copy()
cnt = ins.groupby("series_slug")["market_id"].transform("nunique")
ins = ins[cnt >= 5]
for outc in ["speed", "surprise", "e_late"]:
    sub = ins[ins[outc].notna()]
    ydm = sub[outc] - sub.groupby("series_slug")[outc].transform("mean")
    xdm = sub["ln_prior"] - sub.groupby("series_slug")["ln_prior"].transform("mean")
    b, se = cluster_ols(ydm.to_numpy(), xdm.to_numpy()[:, None],
                        sub["series_slug"])
    rows.append({"test": f"within-series: {outc} ~ ln(1+prior)",
                 "n": len(sub), "coef": b[1], "se": se[1], "t": b[1] / se[1]})
    log(f"  {rows[-1]['test']:44s} b={b[1]:+.5f} (t={b[1]/se[1]:+.2f}, "
        f"n={len(sub):,})")

# 2. cross-section on dims
zl = np.log(np.maximum(mk["life_d"], 1e-5)); zl = (zl - zl.mean()) / zl.std()
zu = np.log1p(mk["usd_full"]); zu = (zu - zu.mean()) / zu.std()
zv = mk["vintage_year"].astype(float); zv = (zv - zv.mean()) / zv.std()
X = np.column_stack([zl, zu, mk["nov_tail"].astype(float), zv])
names = ["z_ln_life", "z_ln_usd", "nov_tail", "z_vintage"]
for outc in ["speed", "surprise"]:
    m2 = mk[mk[outc].notna()]
    Xs = X[mk[outc].notna().to_numpy()]
    b, se = cluster_ols(m2[outc].to_numpy(), Xs, m2["cluster_k200"])
    for i, nm in enumerate(names, start=1):
        rows.append({"test": f"cross-section: {outc} ~ {nm}",
                     "n": len(m2), "coef": b[i], "se": se[i],
                     "t": b[i] / se[i]})
        log(f"  {outc} ~ {nm:12s} b={b[i]:+.5f} (t={b[i]/se[i]:+.2f})")
pd.DataFrame(rows).to_parquet(OUT / "convergence_tests.parquet")
log(f"DONE convergence_speed in {(time.time()-t0)/60:.1f} min")
