"""Fixed-window ("cross-sectional time") liquidity: volume in the market's
first 1 day / 7 days / 30 days.

Motivation (JW/KV, 2026-08-27): total volume accumulates mechanically with
time-open. A fixed window anchored at the market's start gives every market
the same accumulation footprint. Anchor = first standard-filtered BUY trade
(consistent with the lifecycle convention); volume = standard-filtered BUY
dollars in (t0, t0 + window]. For markets shorter than the window the measure
equals total volume (unavoidable; the 1-day window is therefore the only one
fully comparable across ALL horizon bins and is the one used in the
horizon cross).

Outputs:
  timeliq.parquet                      market_id, usd_1d, usd_7d, usd_30d
  schemes/scheme_liq1d.parquet         quartiles of usd_1d (w1q1..w1q4)
  schemes/scheme_liq7d.parquet         quartiles of usd_7d (w7q1..w7q4)
  schemes/scheme_liq30d.parquet        quartiles of usd_30d (w30q1..w30q4)
  schemes/scheme_hor_x_liq1d.parquet   horizon bin x WITHIN-bin usd_1d
                                       quartile ("h1_lt1d|w1q1", ... 20 cells)
"""
from __future__ import annotations
import os

import duckdb
import numpy as np
import pandas as pd

BASE = "/mnt/data/embedding_difficulty"
TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
WALLET_FLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"
START_TS = 1590969600
os.makedirs(f"{BASE}/schemes", exist_ok=True)

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count()}")

print("scanning trades for fixed-window volumes", flush=True)
con.execute(f"""
CREATE TEMP TABLE win AS
WITH tok AS (SELECT token_id, market_id
             FROM read_parquet('{BASE}/universe_tokens.parquet')),
t AS (
  SELECT tk.market_id, tr.timestamp, tr.usdcSize
  FROM read_parquet('{TRADES_GLOB}') tr
  JOIN tok tk ON tr.conditionId = tk.token_id
  WHERE tr.side = 'BUY' AND tr.price > 0.01 AND tr.price < 0.99
    AND tr.timestamp >= {START_TS}
    AND tr.proxyWallet NOT IN (
        SELECT proxyWallet FROM read_parquet('{WALLET_FLAGS}')
        WHERE is_nonhuman)
),
m0 AS (SELECT market_id, MIN(timestamp) AS t0 FROM t GROUP BY market_id)
SELECT t.market_id,
  SUM(t.usdcSize) FILTER (WHERE t.timestamp <= m0.t0 + 86400)    AS usd_1d,
  SUM(t.usdcSize) FILTER (WHERE t.timestamp <= m0.t0 + 604800)   AS usd_7d,
  SUM(t.usdcSize) FILTER (WHERE t.timestamp <= m0.t0 + 2592000)  AS usd_30d
FROM t JOIN m0 USING (market_id)
GROUP BY t.market_id
""")
con.execute(f"COPY win TO '{BASE}/timeliq.parquet' (FORMAT PARQUET)")
w = con.execute("SELECT * FROM win").fetchdf()
print(f"{len(w):,} markets with fixed-window volumes", flush=True)

uni = pd.read_parquet(f"{BASE}/universe_markets.parquet",
                      columns=["market_id", "created_at", "first_trade_at",
                               "last_trade_at", "n_buy_filtered"])
nat = con.execute(f"""
    SELECT condition_id AS market_id,
           TRY_CAST(closed_time AS TIMESTAMP) AS closed_time
    FROM read_parquet('{NATIVE_META}')
""").fetchdf()
df = uni.merge(nat, on="market_id", how="left").merge(w, on="market_id",
                                                      how="inner")


def naive(s):
    s = pd.to_datetime(s)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s


start = naive(df["created_at"]).fillna(naive(df["first_trade_at"]))
end = naive(df["closed_time"]).fillna(naive(df["last_trade_at"]))
hd = ((end - start).dt.total_seconds() / 86400).where(lambda s: s > 0)
HBINS = ["h1_lt1d", "h2_1_7d", "h3_7_30d", "h4_30_90d", "h5_ge90d"]
df["hbin"] = pd.Series(np.select(
    [hd < 1, hd < 7, hd < 30, hd < 90], HBINS[:4], default=HBINS[4]),
    index=df.index).where(hd.notna())
df = df[df["n_buy_filtered"].fillna(0) > 0]

for col, tag in (("usd_1d", "w1"), ("usd_7d", "w7"), ("usd_30d", "w30")):
    v = df[col].fillna(0)
    q = pd.qcut(v.rank(method="first"), 4, labels=False)
    pd.DataFrame({"market_id": df["market_id"],
                  "slice": [f"{tag}q{int(x)+1}" for x in q]}) \
        .to_parquet(f"{BASE}/schemes/scheme_liq{tag[1:]}d.parquet",
                    index=False)
    print(f"scheme_liq{tag[1:]}d written", flush=True)

m = df["hbin"].notna()
qh = df[m].groupby("hbin")["usd_1d"].transform(
    lambda s: pd.qcut(s.fillna(0).rank(method="first"), 4, labels=False,
                      duplicates="drop"))
ok = qh.notna()
sub = df[m]
pd.DataFrame({"market_id": sub.loc[ok, "market_id"],
              "slice": sub.loc[ok, "hbin"] + "|w1q"
              + (qh[ok].astype(int) + 1).astype(str)}) \
    .to_parquet(f"{BASE}/schemes/scheme_hor_x_liq1d.parquet", index=False)
print("scheme_hor_x_liq1d written", flush=True)
