"""Variance-ratio difficulty: a process measure that needs no resolution.

An efficient probability price is a martingale; VR(q) = Var(q-period return)
/ (q x Var(1-period return)) equals 1 under efficiency, >1 under drift or
momentum, <1 under mean reversion / overreaction correction. Computed on
DAILY last-price grids (interior prices 0.05-0.95, gaps <= 3 days), q = 5
(weekly vs daily), overlapping windows, markets with >=30 daily returns.

Part A (resolved markets, validation): VR per market from the BUY
intermediate; correlate with dimensions and with terminal surprise.
Part B (OPEN markets, the censoring-immune application): fills of tokens
absent from market_flags (unresolved, hence excluded from every calibration
analysis) reconstructed from raw_events + block timestamps, top tokens by
fill count. Outputs: vr_resolved.parquet, vr_open.parquet, vr_tests.parquet
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
RAW = "/mnt/data/pipeline_data/raw_events.parquet"
BLOCKTS = "/mnt/data/pipeline_data/block_timestamps.parquet"
FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
TOK2EVT = "/mnt/data/pipeline_data/token_to_event.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
OUT = Path("/mnt/data/learnability/output")
Q = 5  # weekly vs daily


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def vr_from_grid(g: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """g: id_col, dayg (int), px. Returns VR per id."""
    rows = []
    for mid, sub in g.groupby(id_col, observed=True):
        sub = sub.sort_values("dayg")
        h = sub["dayg"].to_numpy()
        p = sub["px"].to_numpy()
        keep = np.diff(h) <= 3  # gap <= 3 days
        r = np.diff(p)[keep]
        if len(r) < 30 or r.std() == 0:
            continue
        var1 = r.var()
        csum = np.cumsum(np.insert(r, 0, 0.0))
        rq = csum[Q:] - csum[:-Q]
        vr = rq.var() / (Q * var1)
        rows.append({id_col: mid, "vr": vr, "n_ret": len(r),
                     "sd1": r.std()})
    return pd.DataFrame(rows)


t0 = time.time()
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

# --- Part A: resolved ---
log("Part A: resolved-market daily grids ...")
grid = con.execute(f"""
    SELECT market_id,
           (epoch(trade_day) / 86400)::BIGINT AS dayg,
           arg_max(price, pos) AS px
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1)
    WHERE price BETWEEN 0.05 AND 0.95
    GROUP BY 1, 2
""").fetchdf()
log(f"grid rows: {len(grid):,}")
vrA = vr_from_grid(grid.rename(columns={"market_id": "id"}), "id")
vrA = vrA.rename(columns={"id": "market_id"})
d = con.execute(f"""
    SELECT condition_id, cluster_k200, life_d, usd_full,
           (novelty_vint_decile=1) AS nov_tail, vintage_year
    FROM read_parquet('{DIMS}')""").fetchdf().set_index("condition_id")
cm = pd.read_parquet(OUT / "convergence_markets.parquet")[
    ["market_id", "surprise", "speed"]].set_index("market_id")
vrA = vrA.join(d, on="market_id").join(cm, on="market_id")
vrA.to_parquet(OUT / "vr_resolved.parquet")
log(f"resolved markets with VR: {len(vrA):,}; "
    f"median VR={vrA.vr.median():.3f}")

rows = []
va = vrA[vrA.life_d.notna() & vrA.cluster_k200.notna()]
zl = np.log(np.maximum(va.life_d, 1e-5)); zl = (zl - zl.mean()) / zl.std()
zu = np.log1p(va.usd_full); zu = (zu - zu.mean()) / zu.std()
for nm, x in [("z_ln_life", zl), ("z_ln_usd", zu),
              ("nov_tail", va.nov_tail.astype(float)),
              ("surprise", va.surprise), ("speed", va.speed)]:
    m = x.notna() & va.vr.notna()
    c = np.corrcoef(va.vr[m], x[m])[0, 1]
    rows.append({"test": f"corr(VR, {nm})", "n": int(m.sum()), "coef": c,
                 "se": np.nan, "t": np.nan})
    log(f"  corr(VR, {nm}) = {c:+.3f} (n={int(m.sum()):,})")

# --- Part B: open markets from raw_events ---
log("Part B: open-market fills from raw_events (unresolved tokens) ...")
try:
    gridB = con.execute(f"""
        WITH resolved AS (SELECT token_id FROM read_parquet('{FLAGS}')),
        fills AS (
            SELECT CASE WHEN maker_asset_id = '0' THEN taker_asset_id
                        ELSE maker_asset_id END AS token,
                   CASE WHEN maker_asset_id = '0'
                        THEN maker_amount_filled::DOUBLE / nullif(taker_amount_filled,0)
                        ELSE taker_amount_filled::DOUBLE / nullif(maker_amount_filled,0)
                   END AS price,
                   block_number
            FROM read_parquet('{RAW}')
            WHERE (maker_asset_id = '0') != (taker_asset_id = '0')
        ),
        open_tok AS (
            SELECT token, count(*) nf FROM fills
            WHERE token NOT IN (SELECT token_id FROM resolved)
            GROUP BY 1 HAVING count(*) >= 2000
        )
        SELECT f.token AS id,
               (b.timestamp / 86400)::BIGINT AS dayg,
               arg_max(f.price, b.timestamp) AS px
        FROM fills f
        JOIN open_tok o ON f.token = o.token
        JOIN read_parquet('{BLOCKTS}') b ON f.block_number = b.block_number
        WHERE f.price BETWEEN 0.05 AND 0.95
        GROUP BY 1, 2
    """).fetchdf()
    log(f"open-token grid rows: {len(gridB):,}, "
        f"tokens: {gridB['id'].nunique():,}")
    vrB = vr_from_grid(gridB, "id")
    ev = con.execute(f"SELECT * FROM read_parquet('{TOK2EVT}') LIMIT 1").fetchdf()
    key = [c for c in ev.columns if "token" in c.lower()]
    slug = [c for c in ev.columns if "slug" in c.lower() or "event" in c.lower()]
    if key and slug:
        t2e = con.execute(f"""
            SELECT {key[0]} AS id, any_value({slug[0]}) AS event_slug
            FROM read_parquet('{TOK2EVT}') GROUP BY 1""").fetchdf()
        vrB = vrB.merge(t2e, on="id", how="left")
    vrB.to_parquet(OUT / "vr_open.parquet")
    log(f"open tokens with VR: {len(vrB):,}; median VR={vrB.vr.median():.3f} "
        f"(resolved median {vrA.vr.median():.3f})")
    rows.append({"test": "open median VR", "n": len(vrB),
                 "coef": float(vrB.vr.median()), "se": np.nan, "t": np.nan})
    rows.append({"test": "resolved median VR", "n": len(vrA),
                 "coef": float(vrA.vr.median()), "se": np.nan, "t": np.nan})
except Exception as e:  # noqa: BLE001
    log(f"Part B failed (non-fatal): {e}")

pd.DataFrame(rows).to_parquet(OUT / "vr_tests.parquet")
log(f"DONE variance_ratio in {(time.time()-t0)/60:.1f} min")
