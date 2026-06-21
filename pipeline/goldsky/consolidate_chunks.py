"""
Consolidate Goldsky chunks into a single deduped parquet — 2-pass version.

Pass 1: each chunk dir → per-dir parquet (in /mnt/data/goldsky/_per_dir_parquets/)
Pass 2: UNION ALL across per-dir parquets, GROUP BY id (hash aggregate, bounded memory),
         write /mnt/data/goldsky/orderfilled_all.parquet
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path('/mnt/data/goldsky')
PER_DIR_OUT = ROOT / '_per_dir_parquets'
FINAL_OUT = ROOT / 'orderfilled_all.parquet'


def list_chunk_dirs():
    dirs = []
    for d in sorted(ROOT.iterdir()):
        if not d.is_dir() or d == PER_DIR_OUT:
            continue
        for candidate in [d, d / 'chunks']:
            if candidate.is_dir() and any(candidate.glob('chunk_*.json.gz')):
                dirs.append((d.name, candidate))
                break
    return dirs


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--memory', default='200GB')
    p.add_argument('--threads', type=int, default=16)
    args = p.parse_args()

    dirs = list_chunk_dirs()
    if not dirs:
        sys.exit('No chunk dirs')
    print(f'Found {len(dirs)} chunk dirs')

    PER_DIR_OUT.mkdir(parents=True, exist_ok=True)
    (ROOT / 'tmp').mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory}'")
    con.execute(f"SET threads = {args.threads}")
    con.execute(f"SET temp_directory = '{ROOT / 'tmp'}'")
    con.execute("SET preserve_insertion_order = false")

    # === Pass 1: per-dir parquet ===
    pass1_t0 = time.time()
    for name, chunk_dir in dirs:
        out = PER_DIR_OUT / f'{name}.parquet'
        if out.exists():
            print(f'  [skip] {name}')
            continue
        glob = str(chunk_dir / 'chunk_*.json.gz')
        t0 = time.time()
        con.execute(f"""
            COPY (
                SELECT
                    id,
                    transactionHash,
                    CAST(timestamp AS BIGINT) AS timestamp,
                    orderHash,
                    maker,
                    taker,
                    makerAssetId,
                    takerAssetId,
                    CAST(makerAmountFilled AS HUGEINT) AS makerAmountFilled,
                    CAST(takerAmountFilled AS HUGEINT) AS takerAmountFilled,
                    CAST(fee AS HUGEINT) AS fee
                FROM read_json_auto('{glob}', format='array', maximum_object_size=100000000)
            )
            TO '{out}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
        """)
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        sz = out.stat().st_size / 1024 ** 3
        print(f'  [done] {name:<10} rows={n:>11,}  size={sz:5.2f}GB  ({time.time() - t0:.0f}s)')

    print(f'Pass 1 total: {(time.time() - pass1_t0)/60:.1f} min')

    # === Pass 2: union + dedup ===
    print('\nPass 2: union all per-dir parquets + GROUP BY id (hash aggregate)…')
    pass2_t0 = time.time()
    glob = str(PER_DIR_OUT / '*.parquet')

    con.execute(f"""
        COPY (
            SELECT
                id,
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
            FROM read_parquet('{glob}')
            GROUP BY id
        )
        TO '{FINAL_OUT}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD')
    """)

    n_unique = con.execute(f"SELECT COUNT(*) FROM read_parquet('{FINAL_OUT}')").fetchone()[0]
    n_raw = con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob}')").fetchone()[0]
    sz = FINAL_OUT.stat().st_size / 1024 ** 3
    elapsed = time.time() - pass2_t0
    print(f'Pass 2 done in {elapsed/60:.1f} min')
    print(f'Output: {FINAL_OUT}  ({sz:.2f} GB)')
    print(f'Unique events: {n_unique:,}')
    print(f'Raw rows ingested: {n_raw:,}  (duplicates removed: {n_raw - n_unique:,}, '
          f'{100*(n_raw - n_unique)/n_raw:.1f}%)')


if __name__ == '__main__':
    main()
