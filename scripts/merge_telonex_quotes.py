#!/usr/bin/env python
"""Merge raw Telonex per-contract-day quote files into consolidated datasets.

Input:  /mnt/data/telonex/quotes_raw/token_id=<id>/<date>.parquet
        (built by pull_telonex_quotes.py; one token per market, $500k floor)

Outputs (in /mnt/data/telonex/):
  quotes_ticks/month=YYYY-MM/*.parquet
      Tick-level top-of-book: token_id, timestamp_us, bid_price, bid_size,
      ask_price, ask_size, mid. Redundant per-token strings (slug, market_id,
      outcome) are dropped — join market metadata via
      telonex_coverage_by_token.parquet on token_id.
  quotes_daily.parquet
      Per token × UTC day convenience layer: quote counts, time-weighted
      mean mid, open/close/min/max mid, mean spread, share of two-sided time.

Prices arrive as strings with NULLs for one-sided books; cast to DOUBLE.
mid is NULL unless both sides are present.
"""

import duckdb

RAW = "/mnt/data/telonex/quotes_raw/*/*.parquet"
TICKS_DIR = "/mnt/data/telonex/quotes_ticks"
DAILY = "/mnt/data/telonex/quotes_daily.parquet"

con = duckdb.connect()
con.execute("PRAGMA threads=16; SET temp_directory='/mnt/data/duckdb_tmp';")
con.execute("SET partitioned_write_max_open_files=100;")

con.execute(f"""
    CREATE VIEW ticks AS
    SELECT CAST(token_id AS VARCHAR) AS token_id,
           timestamp_us,
           CAST(bid_price AS DOUBLE) AS bid_price,
           CAST(bid_size  AS DOUBLE) AS bid_size,
           CAST(ask_price AS DOUBLE) AS ask_price,
           CAST(ask_size  AS DOUBLE) AS ask_size,
           CASE WHEN bid_price IS NOT NULL AND ask_price IS NOT NULL
                THEN (CAST(bid_price AS DOUBLE) + CAST(ask_price AS DOUBLE)) / 2
           END AS mid,
           strftime(to_timestamp(timestamp_us / 1000000), '%Y-%m') AS month
    FROM read_parquet('{RAW}', hive_partitioning=1, union_by_name=0);
""")

print("writing consolidated ticks...", flush=True)
con.execute(f"""
    COPY (SELECT * FROM ticks)
    TO '{TICKS_DIR}' (FORMAT PARQUET, PARTITION_BY (month),
                      COMPRESSION ZSTD, OVERWRITE_OR_IGNORE);
""")
print("ticks done", flush=True)

print("writing daily summary...", flush=True)
con.execute(f"""
    COPY (
        SELECT token_id,
               strftime(to_timestamp(timestamp_us / 1000000), '%Y-%m-%d') AS date,
               count(*) AS n_updates,
               count(mid) AS n_two_sided,
               avg(mid) AS mean_mid,
               arg_min(mid, timestamp_us) AS open_mid,
               arg_max(mid, timestamp_us) AS close_mid,
               min(mid) AS min_mid,
               max(mid) AS max_mid,
               avg(ask_price - bid_price) AS mean_spread
        FROM ticks
        GROUP BY 1, 2
    ) TO '{DAILY}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")
print("daily done", flush=True)

n_ticks = con.execute(f"SELECT count(*) FROM read_parquet('{TICKS_DIR}/*/*.parquet')").fetchone()[0]
n_daily = con.execute(f"SELECT count(*) FROM read_parquet('{DAILY}')").fetchone()[0]
n_tok = con.execute(f"SELECT count(DISTINCT token_id) FROM read_parquet('{DAILY}')").fetchone()[0]
print(f"MERGE COMPLETE: {n_ticks:,} tick rows, {n_daily:,} token-days, {n_tok:,} tokens", flush=True)
