"""Build the canonical token->market flag table for the extended trade set.

Motivation (2026-07-03, see CHANGELOG): on the June-2026 extended trades_clean,
(a) the old market_resolutions_enriched.parquet spine covers only ~49% of trade
rows, and (b) trades' eventSlug is the EMPTY STRING for newer markets, so the
standard `eventSlug NOT LIKE '%updown%'` exclusion catches ~nothing. This table
fixes both: full-coverage token spine + a market-level up/down flag derived
from Gamma METADATA (native event_slug / series_slug / tags / question), never
from trades' eventSlug.

Output: /mnt/data/pipeline_output/market_flags.parquet, one row per token:
  token_id          77-digit outcome token id (trades_clean's conditionId)
  market_id         0x per-market condition id
  winning_outcome   resolved winner ('Yes'/'No'/team name/...)
  is_updown         market-level up/down-series flag
  question          market question text (convenience)

Coverage check printed at the end must show 100% of the distinct tokens in
trades_clean; the script fails loudly if any token is unmatched.

Run on EC2: /home/ubuntu/venv/bin/python scripts/build_market_flags.py
"""
from __future__ import annotations
import os
import sys

import duckdb

RESOLUTIONS = "/home/ubuntu/pipeline/output/market_resolutions.parquet"
TOKEN_MAP = "/mnt/data/pipeline_data/token_map.parquet"
NATIVE_META = "/mnt/data/learnability/native/native_market_meta.parquet"
GAMMA_MKTS = "/mnt/data/pipeline_data/gamma_markets.parquet"
TRADES_GLOB = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
OUT = "/mnt/data/pipeline_output/market_flags.parquet"

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count()}")

con.execute(f"""
COPY (
  SELECT
    r.conditionId AS token_id,
    tm.condition_id AS market_id,
    r.winning_outcome,
    (COALESCE(n.event_slug, tm.event_slug, '') ILIKE '%updown%'
     OR COALESCE(n.event_slug, tm.event_slug, '') ILIKE '%up-or-down%'
     OR COALESCE(n.series_slug, '') ILIKE '%updown%'
     OR COALESCE(n.series_slug, '') ILIKE '%up-or-down%'
     OR list_contains(COALESCE(n.tags, []), 'Up or Down')
     OR COALESCE(NULLIF(TRIM(n.question), ''), NULLIF(TRIM(g.question), ''),
                 NULLIF(TRIM(tm.question), ''), '') ILIKE '%up or down%'
    ) AS is_updown,
    COALESCE(NULLIF(TRIM(n.question), ''), NULLIF(TRIM(g.question), ''),
             NULLIF(TRIM(tm.question), '')) AS question
  FROM read_parquet('{RESOLUTIONS}') r
  LEFT JOIN read_parquet('{TOKEN_MAP}') tm ON r.conditionId = tm.token_id
  LEFT JOIN read_parquet('{NATIVE_META}') n ON tm.condition_id = n.condition_id
  LEFT JOIN read_parquet('{GAMMA_MKTS}') g ON tm.condition_id = g.condition_id
) TO '{OUT}' (FORMAT PARQUET)
""")

stats = con.execute(f"""
SELECT COUNT(*) AS tokens,
       COUNT(*) FILTER (WHERE market_id IS NULL) AS no_market,
       COUNT(*) FILTER (WHERE winning_outcome IS NULL) AS no_outcome,
       COUNT(*) FILTER (WHERE is_updown) AS updown_tokens
FROM read_parquet('{OUT}')
""").fetchdf()
print(stats.to_string(index=False))

missing = con.execute(f"""
SELECT COUNT(DISTINCT t.conditionId)
FROM read_parquet('{TRADES_GLOB}') t
WHERE t.conditionId NOT IN (SELECT token_id FROM read_parquet('{OUT}'))
""").fetchone()[0]
print(f"trades_clean tokens missing from flags: {missing}")
if missing:
    sys.exit(f"FAIL: {missing} traded tokens unmatched")
print(f"OK -> {OUT}")
