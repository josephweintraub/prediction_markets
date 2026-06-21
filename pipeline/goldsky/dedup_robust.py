"""
Robust dedup orchestrator. Three strategies, tried in order:

  1. Single GROUP BY on all per-dir parquets (fastest, most memory-hungry)
  2. Partition-by-id-prefix dedup (16 partitions, each ~32M unique IDs)
  3. Trim-and-union by timestamp ranges (no dedup; bounded by parquet sizes)

Each strategy logs progress, checks disk, and falls through on failure.
Final output: /mnt/data/goldsky/orderfilled_all.parquet
"""

import shutil
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path('/mnt/data/goldsky')
PER_DIR = ROOT / '_per_dir_parquets'
TMP_DIR = ROOT / 'tmp'
FINAL = ROOT / 'orderfilled_all.parquet'
LOG = Path('/home/ubuntu/pipeline/dedup_robust.log')


def log(msg):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, 'a') as f:
        f.write(line + '\n')


def free_gb():
    return shutil.disk_usage('/mnt/data').free / 1024 ** 3


def reset_state():
    if FINAL.exists():
        FINAL.unlink()
    if TMP_DIR.exists():
        for p in TMP_DIR.glob('*'):
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
    TMP_DIR.mkdir(exist_ok=True)


def get_con(memory='180GB', threads=16):
    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory}'")
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET temp_directory = '{TMP_DIR}'")
    con.execute("SET preserve_insertion_order = false")
    return con


GLOB = str(PER_DIR / '*.parquet')


def strategy_1():
    """Single GROUP BY hash aggregate."""
    log(f"STRATEGY 1: single GROUP BY  (free={free_gb():.0f}GB)")
    reset_state()
    con = get_con(memory='150GB')
    con.execute(f"""
        COPY (
            SELECT id,
                   ANY_VALUE(transactionHash)   AS transactionHash,
                   ANY_VALUE(timestamp)         AS timestamp,
                   ANY_VALUE(orderHash)         AS orderHash,
                   ANY_VALUE(maker)             AS maker,
                   ANY_VALUE(taker)             AS taker,
                   ANY_VALUE(makerAssetId)      AS makerAssetId,
                   ANY_VALUE(takerAssetId)      AS takerAssetId,
                   ANY_VALUE(makerAmountFilled) AS makerAmountFilled,
                   ANY_VALUE(takerAmountFilled) AS takerAmountFilled,
                   ANY_VALUE(fee)               AS fee
            FROM read_parquet('{GLOB}')
            GROUP BY id
        ) TO '{FINAL}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
    """)


def strategy_2(n_partitions=16):
    """Partition by first hex char of id, dedup each partition independently, union."""
    log(f"STRATEGY 2: partition-by-prefix ({n_partitions} parts)  (free={free_gb():.0f}GB)")
    reset_state()
    parts_dir = TMP_DIR / 'partitions'
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir()

    con = get_con(memory='100GB')
    HEX = '0123456789abcdef'
    chars = list(HEX)[:n_partitions] if n_partitions <= 16 else list(HEX)

    for i, c in enumerate(chars):
        out = parts_dir / f'part_{c}.parquet'
        log(f"  part {i+1}/{len(chars)} (prefix '{c}', free={free_gb():.0f}GB)")
        con.execute(f"""
            COPY (
                SELECT id,
                       ANY_VALUE(transactionHash)   AS transactionHash,
                       ANY_VALUE(timestamp)         AS timestamp,
                       ANY_VALUE(orderHash)         AS orderHash,
                       ANY_VALUE(maker)             AS maker,
                       ANY_VALUE(taker)             AS taker,
                       ANY_VALUE(makerAssetId)      AS makerAssetId,
                       ANY_VALUE(takerAssetId)      AS takerAssetId,
                       ANY_VALUE(makerAmountFilled) AS makerAmountFilled,
                       ANY_VALUE(takerAmountFilled) AS takerAmountFilled,
                       ANY_VALUE(fee)               AS fee
                FROM read_parquet('{GLOB}')
                WHERE substr(id, 1, 1) = '{c}'
                GROUP BY id
            ) TO '{out}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        log(f"    rows={n:,}  size={out.stat().st_size/1024**3:.2f}GB")

    log(f"  union {len(chars)} parts → {FINAL}")
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{parts_dir}/part_*.parquet')
        ) TO '{FINAL}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
    """)
    shutil.rmtree(parts_dir)


def strategy_3():
    """Trim-and-union by timestamp ranges (NO dedup needed by construction).

    Boundaries determined by each worker's START_TS:
      chunks      : ts < 1763640753  (Nov 20 2025)
      chunks_w2   : 1763640753 ≤ ts < 1766989737  (Dec 30 2025)
      chunks_w3f* : 1766989737 ≤ ts < 1770493605  (Feb 7 2026)
      chunks_w4   : 1770493605 ≤ ts < 1771537718  (Feb 19 2026)
      chunks_w4f* : 1771537718 ≤ ts                (Mar 19 2026)
    """
    log(f"STRATEGY 3: trim-and-union  (free={free_gb():.0f}GB)")
    reset_state()
    con = get_con(memory='100GB')

    parts = [
        (str(PER_DIR / 'chunks.parquet'),                "timestamp < 1763640753"),
        (str(PER_DIR / 'chunks_w2.parquet'),             "timestamp >= 1763640753 AND timestamp < 1766989737"),
        (str(PER_DIR / 'chunks_w3f*.parquet'),           "timestamp >= 1766989737 AND timestamp < 1770493605"),
        (str(PER_DIR / 'chunks_w4.parquet'),             "timestamp >= 1770493605 AND timestamp < 1771537718"),
        (str(PER_DIR / 'chunks_w4f*.parquet'),           "timestamp >= 1771537718"),
    ]
    selects = '\nUNION ALL\n'.join(
        f"SELECT * FROM read_parquet('{glob}') WHERE {where}"
        for glob, where in parts
    )
    con.execute(f"""
        COPY ({selects})
        TO '{FINAL}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
    """)


def verify():
    if not FINAL.exists() or FINAL.stat().st_size == 0:
        return False
    con = get_con(memory='30GB')
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{FINAL}')").fetchone()[0]
    n_unique = con.execute(f"SELECT COUNT(DISTINCT id) FROM read_parquet('{FINAL}')").fetchone()[0]
    n_raw = con.execute(f"SELECT COUNT(*) FROM read_parquet('{GLOB}')").fetchone()[0]
    sz = FINAL.stat().st_size / 1024 ** 3
    log(f"VERIFY: final rows={n:,}  unique_ids={n_unique:,}  raw_input={n_raw:,}")
    log(f"        size={sz:.2f}GB  duplicates_removed={n_raw - n:,} ({100*(n_raw-n)/n_raw:.1f}%)")
    return n == n_unique and n > 0


def main():
    if not LOG.exists() or LOG.stat().st_size == 0:
        LOG.write_text('')

    strategies = [strategy_1, strategy_2, strategy_3]
    for i, s in enumerate(strategies, 1):
        try:
            t0 = time.time()
            s()
            elapsed = (time.time() - t0) / 60
            log(f"  strategy {i} done in {elapsed:.1f} min")
            if verify():
                log(f"SUCCESS: {FINAL}")
                return 0
            log(f"  strategy {i} produced output but verify failed")
        except Exception as e:
            log(f"  strategy {i} FAILED: {type(e).__name__}: {e}")
            # Clean up before next attempt
            try:
                reset_state()
            except Exception:
                pass

    log("ALL STRATEGIES FAILED")
    return 1


if __name__ == '__main__':
    sys.exit(main())
