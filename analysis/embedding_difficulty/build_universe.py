"""Build the market-level universe for the embedding-difficulty workstream.

Grain: one row per MARKET (0x condition id).

Spine (June-2026 refresh artifacts — full coverage of trades_clean tokens):
  - /home/ubuntu/pipeline/output/market_resolutions.parquet
      token_id (as conditionId) -> winning_outcome, 2,373,197 rows = exactly
      the distinct tokens in trades_clean
  - /mnt/data/pipeline_data/token_map.parquet
      token_id -> condition_id (0x market), outcome, question, event_slug
  NOTE: /home/ubuntu/pipeline/output/market_resolutions_enriched.parquet is
  STALE relative to the June-2026 extended trade set (covers only 947K tokens,
  ~49% of trade rows) — deliberately not used as spine here.

Text/metadata sources, in preference order:
  1. /mnt/data/learnability/native/native_market_meta.parquet (question,
     description, created_at, category, tags, series, recurrence)
  2. /mnt/data/pipeline_data/gamma_markets.parquet (question, description, tags)
  3. token_map.question

Universe inclusion: every market with >= 1 traded token. Up/down markets
EXCLUDED (standard filter; they never enter any calibration run).

IMPORTANT (2026-07-03): in the June-2026 rebuilt trades_clean the eventSlug
column is the EMPTY STRING for the newer markets (event-slug map gap), so the
standard trade-level filter `eventSlug NOT LIKE '%updown%'` no longer catches
the up/down series (1.34B rows, ~416K markets). Up/down is therefore flagged
here at MARKET level from Gamma metadata (event_slug / series_slug / question
patterns) and excluded from both outputs; downstream FLB builds inherit the
exclusion via universe_tokens.parquet.

Outputs (/mnt/data/embedding_difficulty/):
  universe_markets.parquet      one row per market
  universe_tokens.parquet       token_id -> market_id, winning_outcome
  build_universe_coverage.json  coverage/fallback stats (for the log)
"""
from __future__ import annotations
import json
import os
import time

import duckdb

TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
TOKEN_MAP = "/mnt/data/pipeline_data/token_map.parquet"
GAMMA_MKTS = "/mnt/data/pipeline_data/gamma_markets.parquet"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"
WALLET_FLAGS = "/mnt/data/learnability/cache/wallet_flags.parquet"
OUT_DIR = "/mnt/data/embedding_difficulty"
POLYMARKET_START_TIMESTAMP = 1590969600  # 2020-06-01 UTC (project standard)

os.makedirs(OUT_DIR, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count()}")
t0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time()-t0:7.1f}s] {msg}", flush=True)


log("registering views")
con.execute(f"CREATE VIEW trades_raw AS SELECT * FROM read_parquet('{TRADES_GLOB}')")
con.execute(f"CREATE VIEW native_meta AS SELECT * FROM read_parquet('{NATIVE_META}')")
con.execute(f"CREATE VIEW gamma_mkts AS SELECT * FROM read_parquet('{GAMMA_MKTS}')")
con.execute(f"CREATE VIEW wallet_flags AS SELECT * FROM read_parquet('{WALLET_FLAGS}')")

# ---- per-token trade aggregates (raw + standard-filtered) ------------------
log("aggregating trades per token (single pass over trades_clean)")
con.execute(f"""
CREATE TEMP TABLE tok_agg AS
SELECT
    conditionId AS token_id,
    COUNT(*) AS n_trades_raw,
    MIN(timestamp) AS first_trade_ts,
    MAX(timestamp) AS last_trade_ts,
    COUNT(*) FILTER (
        WHERE side = 'BUY'
          AND price > 0.01 AND price < 0.99
          AND timestamp >= {POLYMARKET_START_TIMESTAMP}
          AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman)
    ) AS n_buy_filtered,
    SUM(usdcSize) FILTER (
        WHERE side = 'BUY'
          AND price > 0.01 AND price < 0.99
          AND timestamp >= {POLYMARKET_START_TIMESTAMP}
          AND proxyWallet NOT IN (SELECT proxyWallet FROM wallet_flags WHERE is_nonhuman)
    ) AS usd_buy_filtered,
    BOOL_OR(eventSlug LIKE '%updown%' OR eventSlug LIKE '%up-or-down%') AS is_updown
FROM trades_raw
GROUP BY conditionId
""")
n_tok = con.execute("SELECT COUNT(*) FROM tok_agg").fetchone()[0]
log(f"  {n_tok:,} distinct tokens in trades_clean")

# ---- token -> market map + outcome -----------------------------------------
log("building token->market map (fresh spine)")
con.execute(f"""
CREATE TEMP TABLE tokens AS
SELECT
    r.conditionId AS token_id,
    tm.condition_id AS market_id,
    r.winning_outcome,
    tm.question AS q_tm,
    tm.event_slug
FROM read_parquet('{RESOLUTIONS}') r
LEFT JOIN read_parquet('{TOKEN_MAP}') tm ON r.conditionId = tm.token_id
""")

cov = {}
cov["tokens_in_trades"] = n_tok
cov["tokens_in_spine"] = con.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
cov["traded_tokens_matched"] = con.execute(
    "SELECT COUNT(*) FROM tok_agg a JOIN tokens t USING (token_id)").fetchone()[0]
cov["traded_tokens_no_market_id"] = con.execute(
    "SELECT COUNT(*) FROM tok_agg a JOIN tokens t USING (token_id) WHERE t.market_id IS NULL"
).fetchone()[0]

# ---- market-level assembly ---------------------------------------------------
log("assembling market-level universe")
con.execute("""
CREATE TEMP TABLE mkt AS
SELECT
    t.market_id,
    ANY_VALUE(t.q_tm) AS q_tm,
    ANY_VALUE(t.event_slug) AS event_slug_tm,
    COUNT(DISTINCT t.token_id) AS n_tokens,
    SUM(a.n_trades_raw) AS n_trades_raw,
    SUM(a.n_buy_filtered) AS n_buy_filtered,
    SUM(a.usd_buy_filtered) AS usd_buy_filtered,
    MIN(a.first_trade_ts) AS first_trade_ts,
    MAX(a.last_trade_ts) AS last_trade_ts,
    BOOL_OR(a.is_updown) AS is_updown
FROM tokens t
JOIN tok_agg a USING (token_id)
WHERE t.market_id IS NOT NULL
GROUP BY t.market_id
""")
cov["markets_traded"] = con.execute("SELECT COUNT(*) FROM mkt").fetchone()[0]
cov["markets_updown"] = con.execute("SELECT COUNT(*) FROM mkt WHERE is_updown").fetchone()[0]

log("joining metadata (native preferred, gamma fallback, token_map last)")
con.execute("""
CREATE TEMP TABLE universe AS
SELECT
    m.market_id,
    COALESCE(NULLIF(TRIM(n.question), ''), NULLIF(TRIM(g.question), ''),
             NULLIF(TRIM(m.q_tm), '')) AS question,
    COALESCE(NULLIF(TRIM(n.description), ''), NULLIF(TRIM(g.description), '')) AS description,
    CASE WHEN NULLIF(TRIM(n.question), '') IS NOT NULL THEN 'native'
         WHEN NULLIF(TRIM(g.question), '') IS NOT NULL THEN 'gamma'
         WHEN NULLIF(TRIM(m.q_tm), '') IS NOT NULL THEN 'token_map'
         ELSE NULL END AS text_source,
    TRY_CAST(n.created_at AS TIMESTAMP) AS created_at,
    to_timestamp(m.first_trade_ts) AS first_trade_at,
    to_timestamp(m.last_trade_ts) AS last_trade_at,
    n.category,
    n.tags,
    COALESCE(n.event_slug, m.event_slug_tm) AS event_slug,
    n.series_slug,
    n.recurrence,
    n.group_item_title,
    n.volume_num,
    m.n_tokens,
    m.n_trades_raw,
    m.n_buy_filtered,
    m.usd_buy_filtered,
    (COALESCE(n.event_slug, m.event_slug_tm, '') ILIKE '%updown%'
     OR COALESCE(n.event_slug, m.event_slug_tm, '') ILIKE '%up-or-down%'
     OR COALESCE(n.series_slug, '') ILIKE '%updown%'
     OR COALESCE(n.series_slug, '') ILIKE '%up-or-down%'
     OR COALESCE(NULLIF(TRIM(n.question), ''), NULLIF(TRIM(g.question), ''),
                 NULLIF(TRIM(m.q_tm), ''), '') ILIKE '%up or down%'
     OR m.is_updown) AS is_updown
FROM mkt m
LEFT JOIN native_meta n ON m.market_id = n.condition_id
LEFT JOIN gamma_mkts g ON m.market_id = g.condition_id
""")

cov["markets_incl_updown"] = con.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
cov["markets_updown_flagged"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE is_updown").fetchone()[0]
cov["updown_buy_filtered_trades"] = int(con.execute(
    "SELECT COALESCE(SUM(n_buy_filtered),0) FROM universe WHERE is_updown").fetchone()[0])
cov["nonupdown_buy_filtered_trades"] = int(con.execute(
    "SELECT COALESCE(SUM(n_buy_filtered),0) FROM universe WHERE NOT is_updown").fetchone()[0])
for src in ("native", "gamma", "token_map"):
    cov[f"text_source_{src}"] = con.execute(
        f"SELECT COUNT(*) FROM universe WHERE text_source = '{src}' AND NOT is_updown"
    ).fetchone()[0]
cov["markets_universe"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE NOT is_updown").fetchone()[0]
cov["with_question"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE question IS NOT NULL AND NOT is_updown").fetchone()[0]
cov["with_description"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE description IS NOT NULL AND NOT is_updown").fetchone()[0]
cov["with_created_at"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE created_at IS NOT NULL AND NOT is_updown").fetchone()[0]
cov["markets_ge1_filtered_trade"] = con.execute(
    "SELECT COUNT(*) FROM universe WHERE n_buy_filtered > 0 AND NOT is_updown").fetchone()[0]

log("writing outputs")
con.execute(f"""
COPY (SELECT * EXCLUDE (is_updown) FROM universe
      WHERE question IS NOT NULL AND NOT is_updown ORDER BY market_id)
TO '{OUT_DIR}/universe_markets.parquet' (FORMAT PARQUET)
""")
con.execute(f"""
COPY (
  SELECT t.token_id, t.market_id, t.winning_outcome
  FROM tokens t
  JOIN tok_agg a USING (token_id)
  JOIN universe u ON t.market_id = u.market_id
  WHERE NOT u.is_updown
) TO '{OUT_DIR}/universe_tokens.parquet' (FORMAT PARQUET)
""")
cov["markets_written"] = cov["with_question"]

with open(f"{OUT_DIR}/build_universe_coverage.json", "w") as f:
    json.dump(cov, f, indent=2)
log(json.dumps(cov, indent=2))
log("done")
