#!/usr/bin/env python
"""Scope the systematic Telonex quotes pull.

Builds the candidate set for the bulk bid/ask download:
market-level USD volume since 2025-10-11 (quote-coverage era) from trades_clean,
excluding up/down series markets, keeping one token per market (binary books
mirror), with per-token quote-coverage day counts from the Telonex crosswalk.

Writes /mnt/data/telonex/pull_candidates.parquet and prints a floor table
(markets / files / est. size at various volume floors).
"""

import duckdb

TRADES = "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
XWALK = "/mnt/data/telonex/telonex_coverage_by_token.parquet"
OUT = "/mnt/data/telonex/pull_candidates.parquet"
QUOTES_ERA_START = 1760140800  # 2025-10-11 00:00 UTC

con = duckdb.connect()
con.execute("PRAGMA threads=16; SET temp_directory='/mnt/data/duckdb_tmp';")

con.execute(f"""
    CREATE VIEW vol AS
    SELECT conditionId AS token_id, sum(usdcSize) AS usd_vol, count(*) AS n_trades
    FROM read_parquet('{TRADES}')
    WHERE timestamp >= {QUOTES_ERA_START}
    GROUP BY 1;
""")

con.execute(f"""
    COPY (
        WITH tok AS (
            SELECT x.token_id, x.our_market_id, x.tlx_slug, x.tlx_question,
                   x.tlx_outcome_id, x.quotes_from, x.quotes_to,
                   date_diff('day', strptime(x.quotes_from, '%Y-%m-%d'),
                             strptime(x.quotes_to, '%Y-%m-%d')) + 1 AS n_days,
                   coalesce(v.usd_vol, 0) AS token_usd_vol,
                   coalesce(v.n_trades, 0) AS token_n_trades,
                   f.is_updown
            FROM read_parquet('{XWALK}') x
            JOIN read_parquet('{FLAGS}') f ON x.token_id = CAST(f.token_id AS VARCHAR)
            LEFT JOIN vol v ON x.token_id = v.token_id
            WHERE x.quotes_from IS NOT NULL AND x.quotes_from <> ''
              AND NOT f.is_updown
        ),
        mkt AS (
            SELECT *,
                   sum(token_usd_vol) OVER (PARTITION BY our_market_id) AS market_usd_vol,
                   row_number() OVER (
                       PARTITION BY our_market_id
                       ORDER BY tlx_outcome_id NULLS LAST, token_usd_vol DESC
                   ) AS rn
            FROM tok
        )
        SELECT token_id, our_market_id, tlx_slug, tlx_question, tlx_outcome_id,
               quotes_from, quotes_to, n_days, market_usd_vol, token_n_trades
        FROM mkt WHERE rn = 1
    ) TO '{OUT}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

print("candidates written:", OUT)
print(con.execute(f"SELECT count(*) AS markets, sum(n_days) AS files FROM read_parquet('{OUT}')").df().to_string())
print("\nFloor table:")
print(con.execute(f"""
    SELECT floor_usd,
           count(*) AS markets,
           sum(n_days) AS files,
           round(sum(n_days) * 0.2 / 1024, 1) AS est_gb_at_200kb
    FROM read_parquet('{OUT}'),
         (SELECT unnest([0, 1000, 5000, 10000, 50000, 100000, 500000]) AS floor_usd)
    WHERE market_usd_vol >= floor_usd
    GROUP BY floor_usd ORDER BY floor_usd
""").df().to_string())
