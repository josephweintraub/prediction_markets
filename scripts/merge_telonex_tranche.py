#!/usr/bin/env python
"""Merge the raw quote files currently in quotes_raw/ into quotes_ticks/.

Tranche step of the full Telonex pull (see run_full_telonex_pull.sh):
consolidates whatever per-contract-day files are in
/mnt/data/telonex/quotes_raw/ (one pulled tranche) into the month-partitioned
/mnt/data/telonex/quotes_ticks/ tree, naming the new part files
<name>_data_N.parquet so successive tranches never collide. The driver
uploads and deletes quotes_raw afterwards.
"""

import argparse
import glob
import os
import shutil

import duckdb

RAW = "/mnt/data/telonex/quotes_raw/*/*.parquet"
STAGE = "/mnt/data/telonex/quotes_ticks_stage"
FINAL = "/mnt/data/telonex/quotes_ticks"

ap = argparse.ArgumentParser()
ap.add_argument("--name", required=True, help="tranche name, used as filename prefix")
args = ap.parse_args()

if not glob.glob(RAW):
    print("no raw files present; nothing to merge")
    raise SystemExit(0)

if os.path.exists(STAGE):
    shutil.rmtree(STAGE)

con = duckdb.connect()
con.execute("PRAGMA threads=16; SET temp_directory='/mnt/data/duckdb_tmp';")
con.execute("SET partitioned_write_max_open_files=100;")
con.execute(f"""
    COPY (
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
        FROM read_parquet('{RAW}', hive_partitioning=1, union_by_name=0)
    ) TO '{STAGE}' (FORMAT PARQUET, PARTITION_BY (month),
                    COMPRESSION ZSTD, OVERWRITE_OR_IGNORE);
""")

moved = 0
for month_dir in sorted(glob.glob(os.path.join(STAGE, "month=*"))):
    dest_dir = os.path.join(FINAL, os.path.basename(month_dir))
    os.makedirs(dest_dir, exist_ok=True)
    for f in sorted(glob.glob(os.path.join(month_dir, "*.parquet"))):
        dest = os.path.join(dest_dir, f"{args.name}_{os.path.basename(f)}")
        os.replace(f, dest)
        moved += 1
shutil.rmtree(STAGE)

n = con.execute(f"""
    SELECT count(*) FROM read_parquet('{FINAL}/*/{args.name}_*.parquet')
""").fetchone()[0]
print(f"TRANCHE {args.name} MERGED: {n:,} tick rows in {moved} part files")
