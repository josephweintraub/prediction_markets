"""Materialize compact standard-filtered trade-level base tables for FLB runs.

One row per qualifying BUY trade, per lifecycle window. All slicing schemes in
this workstream join their market_id -> slice map against these tables, so the
2B-row trades parquet is scanned once per window, not once per scheme.

Token spine: universe_tokens.parquet from build_universe.py (fresh June-2026
resolutions + token_map; up/down markets already excluded at MARKET level —
the trade-level eventSlug filter is broken on the extended set because
eventSlug is empty-string for newer markets; see build_universe.py header).

Standard filters applied here (project spec, docs/methods_reference.md):
  side = BUY; 0.01 < price < 0.99; timestamp >= 2020-06-01; bot wallets
  excluded (wallet_flags.is_nonhuman); up/down excluded (via spine); token
  resolved (winning_outcome known, true for the whole spine).
Lifecycle window: fraction of per-token trade-time span (min..max over the
BUY/bot/start-filtered set, before the price filter — mirrors the existing
engine), mature = 25-80%, closing = 80-100%.

Outputs (/mnt/data/embedding_difficulty/):
  flb_base_mature.parquet / flb_base_closing.parquet
      market_code int32, token_code int32, wallet_code int32, day int32
      (epoch days), price float32, ret float32, won int8, usdc float32
  code_maps.parquet  (kind, code, value) for market/token/wallet decode
  flb_base_meta.json row counts per stage
"""
from __future__ import annotations
import json
import os
import time

import duckdb

TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
UNIVERSE_TOKENS = "/mnt/data/embedding_difficulty/universe_tokens.parquet"
WALLET_FLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
OUT_DIR = "/mnt/data/embedding_difficulty"
POLYMARKET_START_TIMESTAMP = 1590969600  # 2020-06-01 UTC

WINDOWS = {"mature": (0.25, 0.80), "closing": (0.80, 1.00)}

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count()}")
con.execute("SET preserve_insertion_order=false")
con.execute(f"SET temp_directory='{OUT_DIR}/duckdb_tmp'")
t0 = time.time()
meta: dict[str, int] = {}


def log(msg: str) -> None:
    print(f"[{time.time()-t0:7.1f}s] {msg}", flush=True)


log("registering views")
con.execute(f"CREATE VIEW trades_raw AS SELECT * FROM read_parquet('{TRADES_GLOB}')")
con.execute(f"""
CREATE TEMP TABLE bots AS
SELECT proxyWallet FROM read_parquet('{WALLET_FLAGS}') WHERE is_nonhuman
""")
con.execute(f"""
CREATE TEMP TABLE tokens AS
SELECT token_id, market_id, winning_outcome
FROM read_parquet('{UNIVERSE_TOKENS}')
""")
meta["spine_tokens"] = con.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]

log("pass 1: filtered BUY trades (no price filter yet) -> temp base")
con.execute(f"""
CREATE TEMP TABLE base AS
SELECT t.proxyWallet, t.conditionId AS token_id, t.timestamp, t.price,
       t.usdcSize, t.outcome
FROM trades_raw t
WHERE t.side = 'BUY'
  AND t.timestamp >= {POLYMARKET_START_TIMESTAMP}
  AND t.proxyWallet NOT IN (SELECT proxyWallet FROM bots)
  AND t.conditionId IN (SELECT token_id FROM tokens)
""")
meta["buy_filtered_rows"] = con.execute("SELECT COUNT(*) FROM base").fetchone()[0]
log(f"  {meta['buy_filtered_rows']:,} rows")

log("token lifecycle spans")
con.execute("""
CREATE TEMP TABLE tok_life AS
SELECT token_id, MIN(timestamp) AS t0,
       GREATEST(MAX(timestamp) - MIN(timestamp), 1) AS dur
FROM base GROUP BY token_id
""")

log("code maps")
con.execute("""
CREATE TEMP TABLE mkt_dim AS
SELECT market_id, (ROW_NUMBER() OVER (ORDER BY market_id))::INT AS market_code
FROM (SELECT DISTINCT market_id FROM tokens)
""")
con.execute("""
CREATE TEMP TABLE tok_dim AS
SELECT t.token_id, (ROW_NUMBER() OVER (ORDER BY t.token_id))::INT AS token_code,
       m.market_code, t.winning_outcome
FROM tokens t JOIN mkt_dim m USING (market_id)
""")
con.execute("""
CREATE TEMP TABLE wal_dim AS
SELECT proxyWallet, (ROW_NUMBER() OVER (ORDER BY proxyWallet))::INT AS wallet_code
FROM (SELECT DISTINCT proxyWallet FROM base)
""")
con.execute(f"""
COPY (
  SELECT 'market' AS kind, market_code AS code, market_id AS value FROM mkt_dim
  UNION ALL
  SELECT 'token', token_code, token_id FROM tok_dim
  UNION ALL
  SELECT 'wallet', wallet_code, proxyWallet FROM wal_dim
) TO '{OUT_DIR}/code_maps.parquet' (FORMAT PARQUET)
""")

for name, (lo, hi) in WINDOWS.items():
    log(f"pass 2 ({name}): window {lo}-{hi}, price bounds, encode, write")
    con.execute(f"""
    COPY (
      SELECT
        d.market_code,
        d.token_code,
        w.wallet_code,
        (b.timestamp // 86400)::INT AS day,
        b.price::FLOAT AS price,
        (CASE WHEN b.outcome = d.winning_outcome THEN 1.0 - b.price
              ELSE -b.price END)::FLOAT AS ret,
        (b.outcome = d.winning_outcome)::TINYINT AS won,
        b.usdcSize::FLOAT AS usdc
      FROM base b
      JOIN tok_life l USING (token_id)
      JOIN tok_dim d USING (token_id)
      JOIN wal_dim w USING (proxyWallet)
      WHERE b.price > 0.01 AND b.price < 0.99
        AND (b.timestamp - l.t0)::FLOAT / l.dur BETWEEN {lo} AND {hi}
    ) TO '{OUT_DIR}/flb_base_{name}.parquet' (FORMAT PARQUET)
    """)
    meta[f"rows_{name}"] = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{OUT_DIR}/flb_base_{name}.parquet')"
    ).fetchone()[0]
    log(f"  {meta[f'rows_{name}']:,} rows")

with open(f"{OUT_DIR}/flb_base_meta.json", "w") as f:
    json.dump(meta, f, indent=2)
log(json.dumps(meta))
log("done")
