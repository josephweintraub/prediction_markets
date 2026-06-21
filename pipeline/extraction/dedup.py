"""
Stage 2: Deduplicate OrderFilled events.

The Paradigm matching engine emits TWO OrderFilled events per matched trade —
one for each side. We keep only the maker-side event by filtering out events
where the taker address is one of the exchange contracts (the Paradigm
double-counted event).

Expected: ~394.6M raw → ~233M deduplicated.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

import duckdb

from config import (
    DUCKDB_PATH,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    RAW_EVENTS_PATH,
    DEDUPED_EVENTS_PATH,
    ALL_EXCHANGE_ADDRESSES,
)

log = logging.getLogger(__name__)


def run_stage2(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """
    Deduplicate raw events.

    Strategy: remove events where the taker is an exchange contract address
    (the Paradigm mirror event). For any remaining duplicates sharing the same
    (transaction_hash, order_hash), keep the row with the lowest log_index.

    Returns the deduplicated row count.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    own_con = False
    if con is None:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("SET memory_limit = '200GB'")
        con.execute("SET threads = 16")
        con.execute("SET temp_directory = '/mnt/data/tmp'")
        con.execute("SET max_temp_directory_size = '250GB'")
        con.execute("SET preserve_insertion_order = false")
        own_con = True

    # Work directly on parquet — no need to load into a DuckDB table
    log.info("Reading raw events from %s", RAW_EVENTS_PATH)
    con.execute(f"CREATE OR REPLACE VIEW raw_events AS SELECT * FROM read_parquet('{RAW_EVENTS_PATH}')")

    raw_count = con.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    log.info("Raw events: %d", raw_count)

    # Build the exchange address filter
    exchange_addrs = ", ".join(f"'{a.lower()}'" for a in ALL_EXCHANGE_ADDRESSES)

    # Step 1: Remove events where taker is an exchange contract (Paradigm mirror)
    # Step 2: For any remaining duplicates on (transaction_hash, order_hash),
    #         keep the one with the lowest log_index (maker-side).
    log.info("Deduplicating and writing directly to parquet...")
    con.execute(f"""
        COPY (
            WITH filtered AS (
                SELECT *
                FROM raw_events
                WHERE taker NOT IN ({exchange_addrs})
            ),
            ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY transaction_hash, order_hash
                           ORDER BY log_index ASC
                       ) AS rn
                FROM filtered
            )
            SELECT * EXCLUDE (rn)
            FROM ranked
            WHERE rn = 1
        )
        TO '{DEDUPED_EVENTS_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    deduped_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{DEDUPED_EVENTS_PATH}')").fetchone()[0]
    reduction = (1 - deduped_count / max(raw_count, 1)) * 100
    log.info("Deduped events: %d  (%.1f%% reduction)", deduped_count, reduction)

    log.info("Stage 2 complete.")
    if own_con:
        con.close()

    return deduped_count


if __name__ == "__main__":
    run_stage2()
