"""Family base-rate skill: learnability as predictability from precedent.

Restricted to Yes/No binary markets (consistent outcome orientation).
For market m in text family f (k=200 cluster), the family base rate br_m is
the share of Yes outcomes among family members whose LAST TRADE predates m's
FIRST trade (strict, no lookahead), requiring >=10 such precedents.
Market forecast p_m = dollar-weighted mean Yes-token price in the mature
window. Skill_m = (br_m - y_m)^2 - (p_m - y_m)^2  (positive: the market beats
its own family's precedent base rate).

Reports: distribution of Brier scores for market vs base rate; skill by
precedent depth; accuracy of markets WITHOUT a usable base rate (novelty-tail
adjacent) vs with. Outputs: baserate_markets.parquet, baserate_summary.parquet
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

TRADES = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
WFLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
DIMS = "/mnt/data/learnability/output/market_dimensions_v1.parquet"
TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
OUT = Path("/mnt/data/learnability/output")


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


t0 = time.time()
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

# Yes/No markets and their Yes-side mature vwap. The horizon intermediate has
# no outcome column, so pull the Yes-token rows in one targeted scan.
con.execute(f"""
    CREATE TEMP TABLE yes_mkts AS
    SELECT market_id, max(winning_outcome = 'Yes') AS yes_won
    FROM read_parquet('{FLAGS}')
    WHERE winning_outcome IS NOT NULL AND NOT is_updown
    GROUP BY market_id
    HAVING bool_or(winning_outcome IN ('Yes','No'))
""")
con.execute(f"""
    CREATE TEMP TABLE life AS
    SELECT market_id, min(trade_day) AS first_day, max(trade_day) AS last_day
    FROM read_parquet('{TRADES_INT}', hive_partitioning=1)
    GROUP BY market_id
""")
log("scanning Yes-token mature vwap (one pass over trades_clean) ...")
con.execute(f"""
    CREATE TEMP TABLE pvwap AS
    WITH tok_life AS (
        SELECT conditionId, MIN(timestamp) ts0,
               GREATEST(MAX(timestamp) - MIN(timestamp), 1) dur
        FROM read_parquet('{TRADES}', hive_partitioning=1)
        WHERE side = 'BUY'
          AND proxyWallet NOT IN (SELECT proxyWallet
                                  FROM read_parquet('{WFLAGS}') WHERE is_nonhuman)
        GROUP BY conditionId
    )
    SELECT f.market_id,
           sum(t.price * t.usdcSize) / sum(t.usdcSize) AS p_yes,
           count(*) AS n_yes_trades
    FROM read_parquet('{TRADES}', hive_partitioning=1) t
    JOIN read_parquet('{FLAGS}') f ON t.conditionId = f.token_id
    JOIN tok_life l ON t.conditionId = l.conditionId
    JOIN yes_mkts y ON f.market_id = y.market_id
    WHERE t.side = 'BUY' AND t.outcome = 'Yes'
      AND t.price > 0.01 AND t.price < 0.99
      AND t.proxyWallet NOT IN (SELECT proxyWallet
                                FROM read_parquet('{WFLAGS}') WHERE is_nonhuman)
      AND (t.timestamp - l.ts0)::FLOAT / l.dur BETWEEN 0.25 AND 0.80
    GROUP BY f.market_id HAVING count(*) >= 20
""")
log(f"markets with Yes vwap: {con.sql('SELECT count(*) FROM pvwap').fetchone()[0]:,}")

mk = con.execute(f"""
    SELECT p.market_id, p.p_yes, p.n_yes_trades,
           y.yes_won::INT AS yes_won,
           l.first_day, l.last_day,
           d.cluster_k200, d.series_slug, (d.novelty_vint_decile=1) AS nov_tail,
           d.life_d, d.usd_full
    FROM pvwap p
    JOIN yes_mkts y USING (market_id)
    JOIN life l ON p.market_id = l.market_id
    JOIN read_parquet('{DIMS}') d ON p.market_id = d.condition_id
    WHERE d.cluster_k200 IS NOT NULL
""").fetchdf()
log(f"analysis markets: {len(mk):,}")

# family base rate: strict precedents by trade-time ordering within cluster
mk = mk.sort_values(["cluster_k200", "first_day"]).reset_index(drop=True)
brs, nprior = np.full(len(mk), np.nan), np.zeros(len(mk), int)
for _, idx in mk.groupby("cluster_k200", observed=True).indices.items():
    sub = mk.iloc[idx]
    last = sub["last_day"].to_numpy()
    first = sub["first_day"].to_numpy()
    won = sub["yes_won"].to_numpy(float)
    order = np.argsort(last)
    for j, i in enumerate(idx):
        m = last[order] < first[j]
        k = m.sum()
        nprior[i] = k
        if k >= 10:
            brs[i] = won[order][m].mean()
mk["br"] = brs
mk["n_prior_family"] = nprior
mk["bs_market"] = (mk["p_yes"] - mk["yes_won"]) ** 2
mk["bs_base"] = (mk["br"] - mk["yes_won"]) ** 2
mk["skill"] = mk["bs_base"] - mk["bs_market"]
mk.to_parquet(OUT / "baserate_markets.parquet")

rows = []
def add(name, sub, col="bs_market"):
    rows.append({"group": name, "n": len(sub),
                 "bs_market": float(sub["bs_market"].mean()),
                 "bs_base": float(sub["bs_base"].mean()) if sub["bs_base"].notna().any() else np.nan,
                 "skill": float(sub["skill"].mean()) if sub["skill"].notna().any() else np.nan})
    log(f"  {name:38s} n={len(sub):>7,} BSmkt={rows[-1]['bs_market']:.4f} "
        f"BSbase={rows[-1]['bs_base']:.4f} skill={rows[-1]['skill']:+.4f}")

has = mk[mk.br.notna()]
add("all with base rate (>=10 precedents)", has)
for lo, hi, nm in [(10, 49, "10-49 precedents"), (50, 499, "50-499"),
                   (500, 10**9, "500+")]:
    add(nm, has[(has.n_prior_family >= lo) & (has.n_prior_family <= hi)])
add("novelty tail, with base rate", has[has.nov_tail == True])   # noqa: E712
no = mk[mk.br.isna()]
add("no usable base rate (<10 precedents)", no)
add("novelty tail, no base rate", no[no.nov_tail == True])       # noqa: E712
add("non-tail, no base rate", no[no.nov_tail == False])          # noqa: E712
pd.DataFrame(rows).to_parquet(OUT / "baserate_summary.parquet")
log(f"DONE family_baserate in {(time.time()-t0)/60:.1f} min")
