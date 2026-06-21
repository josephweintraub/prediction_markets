"""
Stage 6: Transform into target schema — produce trades.parquet and
market_resolutions.parquet that are drop-in replacements for the existing
dataset used by the analysis notebooks.

Output schema:

trades.parquet (partitioned by year_month):
  proxyWallet  VARCHAR   — wallet address
  timestamp    BIGINT    — unix epoch seconds
  conditionId  VARCHAR   — per-outcome token ID (one per outcome leg)
  usdcSize     DOUBLE    — dollar value of trade
  price        DOUBLE    — price per share, 0–1
  side         VARCHAR   — 'BUY' or 'SELL'
  outcome      VARCHAR   — outcome label (e.g. "Yes", "No")
  eventSlug    VARCHAR   — parent event slug

market_resolutions.parquet:
  conditionId       VARCHAR   — matches conditionId in trades
  winning_outcome   VARCHAR   — outcome name that won
"""

import logging

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    DUCKDB_PATH,
    DUCKDB_MEMORY_LIMIT,
    DUCKDB_THREADS,
    USDC_DECIMALS,
    CTF_TOKEN_DECIMALS,
    TRADES_OUTPUT_DIR,
    RESOLUTIONS_OUTPUT_PATH,
    RESOLVED_MARKETS_PATH,
    DEDUPED_EVENTS_PATH,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Block timestamp fetching
# ---------------------------------------------------------------------------

def ensure_block_timestamps(con: duckdb.DuckDBPyConnection):
    """
    Ensure we have block timestamps available.

    Strategy: use the block_number from events and fetch timestamps via RPC,
    or approximate from Polygon's ~2-second block time:
        timestamp ≈ 1590969600 + (block_number - 21_000_000) * 2

    For production accuracy, you should batch-fetch real block timestamps.
    This function provides the approximation as a fallback.
    """
    # Check if we already have a non-empty block_timestamps table
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "block_timestamps" in tables:
        count = con.execute("SELECT COUNT(*) FROM block_timestamps").fetchone()[0]
        if count > 0:
            return
        con.execute("DROP TABLE block_timestamps")

    log.info("Creating approximate block timestamps (Polygon ~2s block time)...")
    # Polygon mainnet: block 0 at ~May 30, 2020.
    # More accurate reference: block 21_000_000 ≈ Nov 2022.
    # Polygon block time is ~2 seconds.
    # Reference: block 21,000,000 ≈ timestamp 1667260800 (Nov 1, 2022)
    con.execute("""
        CREATE TABLE block_timestamps AS
        SELECT DISTINCT block_number,
               -- Approximate: reference block 21M ≈ 2022-11-01T00:00:00 UTC
               (1667260800 + (block_number - 21000000) * 2)::BIGINT AS timestamp
        FROM resolved_trades
    """)
    log.info("Block timestamps table created (approximate).")


def fetch_real_block_timestamps(con: duckdb.DuckDBPyConnection, rpc_url: str):
    """
    Fetch real block timestamps from RPC for all unique blocks in our dataset.
    This is more accurate but requires many RPC calls.

    Call this INSTEAD of the approximate method if you need exact timestamps.
    """
    import requests
    import time
    from config import RPC_REQUESTS_PER_SECOND, BLOCK_TIMESTAMPS_PATH

    if BLOCK_TIMESTAMPS_PATH.exists():
        log.info("Loading cached block timestamps from %s", BLOCK_TIMESTAMPS_PATH)
        con.execute(f"""
            CREATE OR REPLACE TABLE block_timestamps AS
            SELECT * FROM read_parquet('{BLOCK_TIMESTAMPS_PATH}')
        """)
        return

    # Get unique block numbers
    blocks = con.execute("""
        SELECT DISTINCT block_number FROM resolved_trades ORDER BY block_number
    """).fetchdf()["block_number"].tolist()

    log.info("Fetching timestamps for %d unique blocks...", len(blocks))

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    min_interval = 1.0 / RPC_REQUESTS_PER_SECOND
    results = []
    batch_size = 50  # batch RPC calls

    for i in range(0, len(blocks), batch_size):
        batch = blocks[i:i + batch_size]
        payloads = [
            {
                "jsonrpc": "2.0",
                "id": j,
                "method": "eth_getBlockByNumber",
                "params": [hex(bn), False],
            }
            for j, bn in enumerate(batch)
        ]

        for attempt in range(5):
            try:
                t0 = time.time()
                resp = session.post(rpc_url, json=payloads, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                for item, bn in zip(sorted(data, key=lambda x: x["id"]), batch):
                    ts = int(item["result"]["timestamp"], 16)
                    results.append({"block_number": bn, "timestamp": ts})
                elapsed = time.time() - t0
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning("Block timestamp fetch error: %s — retry in %ds", e, wait)
                time.sleep(wait)

        if (i // batch_size) % 100 == 0:
            log.info("  Fetched timestamps for %d / %d blocks", len(results), len(blocks))

    import pandas as pd
    ts_df = pd.DataFrame(results)
    ts_df.to_parquet(str(BLOCK_TIMESTAMPS_PATH), index=False)

    con.execute("DROP TABLE IF EXISTS block_timestamps")
    con.execute("CREATE TABLE block_timestamps AS SELECT * FROM ts_df")
    log.info("Block timestamps fetched and cached: %d blocks", len(results))


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------

def run_stage6(con: duckdb.DuckDBPyConnection | None = None, use_real_timestamps: bool = False) -> dict:
    """
    Transform pipeline data into the target schema.
    Produces two parquet outputs:
      - trades.parquet (partitioned by year_month)
      - market_resolutions.parquet

    Args:
        con: DuckDB connection (created if None)
        use_real_timestamps: If True, fetch real block timestamps from RPC
                            (slower but accurate). Otherwise use approximation.

    Returns dict with row counts.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    own_con = False
    if con is None:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads = {DUCKDB_THREADS}")
        own_con = True

    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "resolved_trades" not in tables:
        log.error("Table 'resolved_trades' not found. Run Stages 1-4 first.")
        if own_con:
            con.close()
        return {}

    # --- Block timestamps ---
    if use_real_timestamps:
        from config import RPC_URL
        fetch_real_block_timestamps(con, RPC_URL)
    else:
        ensure_block_timestamps(con)

    usdc_scale = 10 ** USDC_DECIMALS
    ctf_scale = 10 ** CTF_TOKEN_DECIMALS

    # -----------------------------------------------------------------------
    # 6a: Build trades table (two rows per matched trade: maker + taker)
    # -----------------------------------------------------------------------
    log.info("6a: Building trades table (maker + taker expansion)...")
    con.execute("DROP TABLE IF EXISTS final_trades")
    con.execute(f"""
        CREATE TABLE final_trades AS

        -- MAKER row
        SELECT
            rt.maker AS proxyWallet,
            bt.timestamp,
            -- conditionId = the outcome token's asset ID (stringified)
            CASE
                WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                ELSE rt.taker_asset_id
            END AS conditionId,
            -- usdcSize
            CASE
                WHEN rt.outcome_token_side = 'maker'
                    THEN rt.taker_amount_filled / {usdc_scale}.0
                ELSE rt.maker_amount_filled / {usdc_scale}.0
            END AS usdcSize,
            -- price = usdc / tokens
            CASE
                WHEN rt.outcome_token_side = 'maker'
                    THEN (rt.taker_amount_filled / {usdc_scale}.0)
                         / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)
                ELSE (rt.maker_amount_filled / {usdc_scale}.0)
                     / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0)
            END AS price,
            -- side: maker whose asset is outcome token → SELL; else BUY
            CASE
                WHEN rt.outcome_token_side = 'maker' THEN 'SELL'
                ELSE 'BUY'
            END AS side,
            rt.outcome,
            rt.event_slug AS eventSlug,
            TRUE AS is_maker,
            rt.taker AS counterparty

        FROM resolved_trades rt
        JOIN block_timestamps bt ON rt.block_number = bt.block_number
        WHERE rt.condition_id IS NOT NULL

        UNION ALL

        -- TAKER row
        SELECT
            rt.taker AS proxyWallet,
            bt.timestamp,
            CASE
                WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                ELSE rt.taker_asset_id
            END AS conditionId,
            CASE
                WHEN rt.outcome_token_side = 'maker'
                    THEN rt.taker_amount_filled / {usdc_scale}.0
                ELSE rt.maker_amount_filled / {usdc_scale}.0
            END AS usdcSize,
            CASE
                WHEN rt.outcome_token_side = 'maker'
                    THEN (rt.taker_amount_filled / {usdc_scale}.0)
                         / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)
                ELSE (rt.maker_amount_filled / {usdc_scale}.0)
                     / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0)
            END AS price,
            -- Taker direction is opposite of maker
            CASE
                WHEN rt.outcome_token_side = 'maker' THEN 'BUY'
                ELSE 'SELL'
            END AS side,
            rt.outcome,
            rt.event_slug AS eventSlug,
            FALSE AS is_maker,
            rt.maker AS counterparty

        FROM resolved_trades rt
        JOIN block_timestamps bt ON rt.block_number = bt.block_number
        WHERE rt.condition_id IS NOT NULL
    """)

    # Clamp price to [0, 1] and filter nulls
    con.execute("""
        DELETE FROM final_trades WHERE price IS NULL OR price <= 0 OR price > 1
    """)

    trade_count = con.execute("SELECT COUNT(*) FROM final_trades").fetchone()[0]
    log.info("Final trades (maker+taker): %d", trade_count)

    # -----------------------------------------------------------------------
    # 6b: Build market_resolutions table
    # -----------------------------------------------------------------------
    log.info("6b: Building market_resolutions table...")
    con.execute("DROP TABLE IF EXISTS final_resolutions")
    con.execute("""
        CREATE TABLE final_resolutions AS
        SELECT DISTINCT
            conditionId,
            winning_outcome
        FROM (
            SELECT
                CASE
                    WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                    ELSE rt.taker_asset_id
                END AS conditionId,
                rt.winning_outcome
            FROM resolved_trades rt
            WHERE rt.condition_id IS NOT NULL
        )
    """)

    resolution_count = con.execute("SELECT COUNT(*) FROM final_resolutions").fetchone()[0]
    log.info("Resolution entries: %d", resolution_count)

    # -----------------------------------------------------------------------
    # 6c: Write to parquet
    # -----------------------------------------------------------------------
    log.info("6c: Writing trades.parquet (partitioned by year_month)...")

    # Add year_month partition column
    con.execute("""
        ALTER TABLE final_trades ADD COLUMN year_month VARCHAR;
    """)
    con.execute("""
        UPDATE final_trades
        SET year_month = strftime(to_timestamp(timestamp), '%Y-%m')
    """)

    # Write partitioned parquet using DuckDB's COPY with PARTITION_BY
    import shutil
    if TRADES_OUTPUT_DIR.exists():
        shutil.rmtree(TRADES_OUTPUT_DIR)
    TRADES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    con.execute(f"""
        COPY (
            SELECT proxyWallet, timestamp, conditionId, usdcSize, price,
                   side, outcome, eventSlug, year_month
            FROM final_trades
        )
        TO '{TRADES_OUTPUT_DIR}' (
            FORMAT PARQUET,
            PARTITION_BY (year_month),
            COMPRESSION ZSTD,
            OVERWRITE_OR_IGNORE
        )
    """)
    log.info("Trades written to %s", TRADES_OUTPUT_DIR)

    # Write market_resolutions
    log.info("Writing market_resolutions.parquet...")
    con.execute(f"""
        COPY (
            SELECT conditionId, winning_outcome
            FROM final_resolutions
        )
        TO '{RESOLUTIONS_OUTPUT_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    log.info("Resolutions written to %s", RESOLUTIONS_OUTPUT_PATH)

    # -----------------------------------------------------------------------
    # 6d: Validate schema
    # -----------------------------------------------------------------------
    log.info("6d: Running schema validation...")
    _validate(con)

    result = {
        "trade_rows": trade_count,
        "resolution_rows": resolution_count,
        "trades_path": str(TRADES_OUTPUT_DIR),
        "resolutions_path": str(RESOLUTIONS_OUTPUT_PATH),
    }

    if own_con:
        con.close()

    log.info("Stage 6 complete.")
    return result


def _validate(con: duckdb.DuckDBPyConnection):
    """Run the validation checks from the plan's §6d."""
    import glob as globmod

    trades_glob = f"{TRADES_OUTPUT_DIR}/**/*.parquet"

    # Check if any parquet files exist
    if not globmod.glob(str(TRADES_OUTPUT_DIR / "**" / "*.parquet"), recursive=True):
        log.warning("  No parquet files found — skipping validation (0 trades produced)")
        return

    # Type check
    types = con.execute(f"""
        SELECT typeof(proxyWallet), typeof(timestamp), typeof(conditionId),
               typeof(usdcSize), typeof(price), typeof(side),
               typeof(outcome), typeof(eventSlug)
        FROM '{trades_glob}'
        LIMIT 1
    """).fetchone()
    expected = ("VARCHAR", "BIGINT", "VARCHAR", "DOUBLE", "DOUBLE", "VARCHAR", "VARCHAR", "VARCHAR")
    if types != expected:
        log.warning("Type mismatch! Got %s, expected %s", types, expected)
    else:
        log.info("  Types: OK")

    # Timestamp range
    ts_range = con.execute(f"""
        SELECT MIN(timestamp), MAX(timestamp),
               to_timestamp(MIN(timestamp)), to_timestamp(MAX(timestamp))
        FROM '{trades_glob}'
    """).fetchone()
    log.info("  Timestamp range: %d – %d  (%s – %s)", *ts_range)

    # Side values
    sides = con.execute(f"""
        SELECT DISTINCT side FROM '{trades_glob}'
    """).fetchdf()["side"].tolist()
    if set(sides) == {"BUY", "SELL"}:
        log.info("  Side values: OK (BUY, SELL)")
    else:
        log.warning("  Unexpected side values: %s", sides)

    # conditionId uniqueness per outcome
    multi = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT conditionId, COUNT(DISTINCT outcome) as n
            FROM '{trades_glob}'
            GROUP BY conditionId
            HAVING n > 1
        )
    """).fetchone()[0]
    if multi == 0:
        log.info("  conditionId per-outcome: OK (each maps to one outcome)")
    else:
        log.warning("  %d conditionIds map to multiple outcomes!", multi)

    # Price range
    price_stats = con.execute(f"""
        SELECT MIN(price), MAX(price), AVG(price)
        FROM '{trades_glob}'
    """).fetchone()
    log.info("  Price: min=%.4f, max=%.4f, avg=%.4f", *price_stats)

    # Resolution join
    join_count = con.execute(f"""
        SELECT COUNT(*) FROM '{trades_glob}' t
        JOIN '{RESOLUTIONS_OUTPUT_PATH}' r ON t.conditionId = r.conditionId
    """).fetchone()[0]
    log.info("  Resolution join: %d rows matched", join_count)

    # Event slug sample
    slugs = con.execute(f"""
        SELECT eventSlug, COUNT(*) as n
        FROM '{trades_glob}'
        WHERE eventSlug IS NOT NULL AND eventSlug != ''
        GROUP BY eventSlug
        ORDER BY n DESC
        LIMIT 10
    """).fetchdf()
    log.info("  Top event slugs:\n%s", slugs.to_string(index=False))


if __name__ == "__main__":
    run_stage6()
