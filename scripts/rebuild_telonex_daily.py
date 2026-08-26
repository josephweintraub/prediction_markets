#!/usr/bin/env python
"""Recompute quotes_daily.parquet from the full consolidated quotes_ticks tree."""

import duckdb

TICKS = "/mnt/data/telonex/quotes_ticks/*/*.parquet"
DAILY = "/mnt/data/telonex/quotes_daily.parquet"

con = duckdb.connect()
con.execute("PRAGMA threads=16; SET temp_directory='/mnt/data/duckdb_tmp';")
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
        FROM read_parquet('{TICKS}', hive_partitioning=1)
        GROUP BY 1, 2
    ) TO '{DAILY}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")
n, t = con.execute(f"SELECT count(*), count(DISTINCT token_id) FROM read_parquet('{DAILY}')").fetchone()
print(f"DAILY REBUILT: {n:,} token-days, {t:,} tokens")
