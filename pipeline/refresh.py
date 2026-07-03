#!/usr/bin/env python3
"""
Incremental refresh of the Polymarket trades dataset.

Pipeline (each step has a --skip-* flag for resuming partial runs):
  1. Stage 1 extraction        — pull new OrderFilled events since last endpoint
  2. Concat                    — streaming pyarrow merge of archive + increment
  3. Stage 2 dedup             — drop the Paradigm mirror event per fill
  4. Stage 3 token mapping     — fetch CLOB /markets (~1.1M markets), join events
  5. Stage 4 resolutions       — INNER JOIN to resolved markets (picks up newly-closed)
  6. fetch_block_timestamps    — real per-block unix timestamps, iterative retry
  7. Stage 6 final transform   — maker+taker expand, real ts, partition by year_month
  8. Event-slug backfill       — fetch gamma /events with monthly end-date windows,
                                  build token→event map, partition-by-partition rewrite

Usage:
    python refresh.py                          # full incremental
    python refresh.py --skip-extract           # use existing raw_events.parquet
    python refresh.py --skip-extract --skip-dedup --skip-stage3
                                               # resume from Stage 4
    python refresh.py --skip-eventslug         # don't backfill eventSlug
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import requests

PIPELINE_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PIPELINE_ROOT / "data"
OUTPUT_DIR = PIPELINE_ROOT / "output"
EXTRACTION_DIR = PIPELINE_ROOT / "extraction"
TRANSFORM_DIR = PIPELINE_ROOT / "transform"

RAW_EVENTS = DATA_DIR / "raw_events.parquet"
DEDUPED_EVENTS = DATA_DIR / "deduped_events.parquet"
MAPPED_EVENTS = DATA_DIR / "mapped_events.parquet"
RESOLVED_TRADES = DATA_DIR / "resolved_trades.parquet"
TOKEN_MAP = DATA_DIR / "token_map.parquet"
GAMMA_MARKETS = DATA_DIR / "gamma_markets.parquet"
RESOLVED_MARKETS = DATA_DIR / "resolved_markets.parquet"
BLOCK_TIMESTAMPS = DATA_DIR / "block_timestamps.parquet"
EVENT_SLUG_MAP = DATA_DIR / "event_slug_map.parquet"
TOKEN_TO_EVENT = DATA_DIR / "token_to_event.parquet"
TRADES_OUT = OUTPUT_DIR / "trades.parquet"
RESOLUTIONS_OUT = OUTPUT_DIR / "market_resolutions.parquet"

# build_trades.py uses its own DATA_DIR/OUTPUT_DIR; if transform/output/trades.parquet
# is produced we move it to the canonical OUTPUT_DIR (handled at end of stage 6).
TRANSFORM_OUTPUT = TRANSFORM_DIR / "output"

PYTHON = "/home/ubuntu/venv/bin/python"
RPC_URL = os.environ.get(
    "POLYGON_RPC_URL",
    "https://polygon-mainnet.g.alchemy.com/v2/NSDVafyFOpXs26a2uN3Z_",
)
GAMMA_BASE = "https://gamma-api.polymarket.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] refresh: %(message)s",
)
log = logging.getLogger(__name__)


def get_polygon_head() -> int:
    r = requests.post(
        RPC_URL,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        timeout=15,
    )
    r.raise_for_status()
    return int(r.json()["result"], 16)


def get_current_endpoint() -> int:
    if not RAW_EVENTS.exists():
        return 21_000_000  # full history start
    con = duckdb.connect()
    return con.execute(
        f"SELECT MAX(block_number) FROM read_parquet('{RAW_EVENTS}')"
    ).fetchone()[0]


def archive_canonical_files(last_block: int) -> Path | None:
    raw_archive = None
    if RAW_EVENTS.exists():
        raw_archive = DATA_DIR / f"raw_events_through_{last_block}.parquet"
        if raw_archive.exists():
            log.info("Archive already exists, leaving in place: %s", raw_archive)
        else:
            log.info("Archiving raw_events.parquet → %s", raw_archive.name)
            RAW_EVENTS.rename(raw_archive)
    if TRADES_OUT.exists():
        ts = time.strftime("%Y-%m-%d")
        trades_archive = OUTPUT_DIR / f"trades_through_{ts}.parquet"
        if trades_archive.exists():
            log.warning("Trades archive already exists: %s — leaving in place", trades_archive)
        else:
            log.info("Archiving trades.parquet → %s", trades_archive.name)
            TRADES_OUT.rename(trades_archive)
    return raw_archive


def run_stage1(start_block: int, end_block: int, workers: int) -> None:
    log.info("Stage 1: extracting blocks %d → %d with %d workers", start_block, end_block, workers)
    cmd = [
        PYTHON,
        str(EXTRACTION_DIR / "extract_orderfilled.py"),
        "--start", str(start_block),
        "--end", str(end_block),
        "--workers", str(workers),
    ]
    t0 = time.time()
    subprocess.run(cmd, check=True, cwd=PIPELINE_ROOT)
    log.info("Stage 1 done in %.1f min", (time.time() - t0) / 60)


def concat_raw_events(archive: Path) -> int:
    """Stream-concat archive + new increment using pyarrow (constant memory, no temp dir)."""
    import pyarrow.parquet as pq

    new_inc = RAW_EVENTS
    if not archive.exists():
        log.info("No archive — new raw_events stands alone")
        return duckdb.connect().execute(
            f"SELECT COUNT(*) FROM read_parquet('{new_inc}')"
        ).fetchone()[0]
    if not new_inc.exists():
        log.info("No new raw_events — restoring archive in place")
        archive.rename(RAW_EVENTS)
        return duckdb.connect().execute(
            f"SELECT COUNT(*) FROM read_parquet('{RAW_EVENTS}')"
        ).fetchone()[0]

    log.info("Streaming concat: %s + %s", archive.name, new_inc.name)
    out = DATA_DIR / "raw_events_merged.parquet"
    writer = None
    total = 0
    t0 = time.time()
    for src in (archive, new_inc):
        pf = pq.ParquetFile(src)
        for i in range(pf.metadata.num_row_groups):
            rg = pf.read_row_group(i)
            if writer is None:
                writer = pq.ParquetWriter(out, rg.schema, compression="zstd")
            writer.write_table(rg)
            total += rg.num_rows
    writer.close()
    new_inc.unlink()
    out.rename(RAW_EVENTS)
    log.info("Concat done: %d rows in %.1f min", total, (time.time() - t0) / 60)
    return total


def force_refresh_caches() -> None:
    stage3_ckpt = DATA_DIR / "checkpoints" / "stage3_markets.json"
    for p in (TOKEN_MAP, GAMMA_MARKETS, RESOLVED_MARKETS, stage3_ckpt):
        if p.exists():
            log.info("Removing cached %s", p.name)
            p.unlink()


def run_build_trades(stages: list[int]) -> None:
    log.info("Running build_trades stages %s", stages)
    cmd = [PYTHON, str(TRANSFORM_DIR / "build_trades.py"), "--stages", *map(str, stages)]
    t0 = time.time()
    subprocess.run(cmd, check=True, cwd=PIPELINE_ROOT)
    log.info("build_trades done in %.1f min", (time.time() - t0) / 60)


def _coverage_stats() -> tuple[int, int]:
    con = duckdb.connect()
    needed = con.execute(
        f"SELECT COUNT(DISTINCT block_number) FROM read_parquet('{RESOLVED_TRADES}')"
    ).fetchone()[0]
    have = 0
    if BLOCK_TIMESTAMPS.exists():
        have = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{BLOCK_TIMESTAMPS}')"
        ).fetchone()[0]
    con.close()
    return needed, have


def run_fetch_timestamps(workers: int, max_passes: int = 5) -> None:
    log.info("Fetching real block timestamps (%d workers, up to %d passes)", workers, max_passes)
    last_have = -1
    for p in range(1, max_passes + 1):
        needed, have = _coverage_stats()
        log.info("Pass %d/%d: %d / %d covered (%.2f%%)",
                 p, max_passes, have, needed, 100 * have / max(needed, 1))
        if have >= needed:
            log.info("Full timestamp coverage reached.")
            return
        if have == last_have and p > 1:
            log.warning("No progress between passes — stopping at %d / %d covered", have, needed)
            return
        last_have = have

        cmd = [PYTHON, str(EXTRACTION_DIR / "fetch_block_timestamps.py"),
               "--workers", str(workers), "--max-iters", "4"]
        t0 = time.time()
        subprocess.run(cmd, check=True, cwd=PIPELINE_ROOT)
        log.info("Pass %d done in %.1f min", p, (time.time() - t0) / 60)


def run_dedup() -> None:
    log.info("Stage 2: dedup")
    cmd = [PYTHON, str(EXTRACTION_DIR / "dedup.py")]
    t0 = time.time()
    subprocess.run(cmd, check=True, cwd=PIPELINE_ROOT)
    log.info("Stage 2 done in %.1f min", (time.time() - t0) / 60)


def relocate_trades_output() -> None:
    """Move Stage 6 output from transform/output/ to canonical OUTPUT_DIR/."""
    src = TRANSFORM_OUTPUT / "trades.parquet"
    res_src = TRANSFORM_OUTPUT / "market_resolutions.parquet"
    if src.exists():
        if TRADES_OUT.exists():
            log.warning("%s already exists — leaving Stage 6 output in transform/output/", TRADES_OUT)
        else:
            log.info("Moving %s → %s", src, TRADES_OUT)
            shutil.move(str(src), str(TRADES_OUT))
    if res_src.exists():
        if RESOLUTIONS_OUT.exists():
            log.info("Removing stale %s before swap", RESOLUTIONS_OUT)
            RESOLUTIONS_OUT.unlink()
        log.info("Moving %s → %s", res_src, RESOLUTIONS_OUT)
        shutil.move(str(res_src), str(RESOLUTIONS_OUT))


# ---------------------------------------------------------------------------
# Event-slug backfill (CLOB markets schema doesn't include event_slug,
# so we hit gamma /events with monthly end-date windows to recover the mapping)
# ---------------------------------------------------------------------------

def _fetch_events_window(session, params, max_offset=100_000):
    out = []
    offset = 0
    while offset < max_offset:
        p = {**params, "limit": 500, "offset": offset}
        for attempt in range(6):
            try:
                r = session.get(f"{GAMMA_BASE}/events", params=p, timeout=30)
                if r.status_code in (400, 422):
                    return out
                r.raise_for_status()
                page = r.json()
                break
            except Exception:
                time.sleep(1.5 ** attempt)
        else:
            return out
        if not page:
            break
        out.extend(page)
        offset += len(page)
        if len(page) < 500:
            break
    return out


def fetch_events_to_map() -> int:
    """Build conditionId → event_slug map from gamma /events via monthly windows.

    Gamma /events caps at offset 100K per filter. Monthly end_date_min/max
    windows partition the data so each window stays under the cap.
    """
    log.info("Fetching events via monthly end_date windows...")
    windows = []
    for y in range(2022, 2027):
        for m in range(1, 13):
            if (y, m) < (2022, 11) or (y, m) > (2026, 12):
                continue
            s = date(y, m, 1).isoformat()
            e = date(y + (m // 12), (m % 12) + 1, 1).isoformat()
            windows.append((s, e))
    queries = [{"end_date_min": s, "end_date_max": e, "closed": "true"} for s, e in windows]
    queries.append({"closed": "false"})  # open events catch-all

    session = requests.Session()
    all_events = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=16) as ex:
        for evs in ex.map(lambda q: _fetch_events_window(session, q), queries):
            for ev in evs:
                eid = ev.get("id") or ev.get("slug")
                if eid and eid not in all_events:
                    all_events[eid] = ev
    log.info("Fetched %d unique events in %.1f s", len(all_events), time.time() - t0)

    rows = []
    for ev in all_events.values():
        slug = ev.get("slug")
        if not slug:
            continue
        for m in ev.get("markets", []):
            cid = m.get("conditionId") or m.get("condition_id")
            if cid:
                rows.append({"conditionId": cid, "eventSlug": slug})
    df = pd.DataFrame(rows).drop_duplicates(subset="conditionId", keep="first")
    df.to_parquet(EVENT_SLUG_MAP, index=False)
    log.info("event_slug_map: %d unique conditionIds → %s", len(df), EVENT_SLUG_MAP)
    return len(df)


def build_token_to_event() -> None:
    """token_id → eventSlug (joined via condition_id) so we can update trades.parquet directly."""
    if not RESOLVED_TRADES.exists() or not EVENT_SLUG_MAP.exists():
        log.error("Need both resolved_trades and event_slug_map to build token_to_event")
        return
    con = duckdb.connect()
    con.execute("SET memory_limit='32GB'")
    con.execute("SET threads=16")
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"""
        COPY (
            SELECT DISTINCT
                CASE WHEN outcome_token_side = 'maker' THEN maker_asset_id ELSE taker_asset_id END AS token_id,
                condition_id,
                es.eventSlug
            FROM read_parquet('{RESOLVED_TRADES}') rt
            LEFT JOIN read_parquet('{EVENT_SLUG_MAP}') es ON rt.condition_id = es.conditionId
            WHERE rt.condition_id IS NOT NULL
        ) TO '{TOKEN_TO_EVENT}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n, matched = con.execute(f"""
        SELECT COUNT(*), SUM(CASE WHEN eventSlug IS NOT NULL THEN 1 ELSE 0 END)
        FROM read_parquet('{TOKEN_TO_EVENT}')
    """).fetchone()
    log.info("token_to_event: %d tokens, %d matched (%.2f%%)",
             n, matched or 0, 100 * (matched or 0) / max(n, 1))


def backfill_event_slug() -> None:
    """Rewrite trades.parquet partition-by-partition with eventSlug filled from token_to_event."""
    if not TRADES_OUT.exists():
        log.error("trades.parquet not found at %s — run Stage 6 first", TRADES_OUT)
        return
    if not TOKEN_TO_EVENT.exists():
        log.error("token_to_event.parquet not found — run build_token_to_event first")
        return

    con = duckdb.connect()
    con.execute("SET memory_limit='150GB'")
    con.execute("SET threads=16")
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"""
        CREATE TABLE te AS
        SELECT token_id, eventSlug FROM read_parquet('{TOKEN_TO_EVENT}')
    """)
    con.execute("CREATE INDEX te_idx ON te(token_id)")

    # Write to a sibling dir on /mnt/data (avoids root-disk pressure), then swap.
    staging = DATA_DIR / "trades_with_events.parquet"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    partitions = sorted(p for p in TRADES_OUT.iterdir() if p.is_dir() and p.name.startswith("year_month="))
    log.info("Backfilling eventSlug across %d partitions", len(partitions))
    t0 = time.time()
    grand_total = grand_matched = 0
    for i, part in enumerate(partitions, 1):
        out_part = staging / part.name
        out_part.mkdir(parents=True, exist_ok=True)
        out_file = out_part / "data.parquet"
        con.execute(f"""
            COPY (
                SELECT
                    t.proxyWallet, t.timestamp, t.conditionId, t.usdcSize, t.price,
                    t.side, t.outcome,
                    COALESCE(te.eventSlug, '') AS eventSlug,
                    t.is_maker, t.counterparty
                FROM read_parquet('{part}/*.parquet') t
                LEFT JOIN te ON t.conditionId = te.token_id
            ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        n, matched = con.execute(f"""
            SELECT COUNT(*), SUM(CASE WHEN eventSlug != '' THEN 1 ELSE 0 END)
            FROM read_parquet('{out_file}')
        """).fetchone()
        grand_total += n
        grand_matched += matched or 0
        log.info("  [%d/%d] %s: %d rows, %d with eventSlug (%.1f%%)",
                 i, len(partitions), part.name.split("=")[1], n, matched or 0,
                 100 * (matched or 0) / max(n, 1))

    log.info("backfill done in %.1f min: %d / %d with eventSlug (%.2f%%)",
             (time.time() - t0) / 60, grand_matched, grand_total,
             100 * grand_matched / max(grand_total, 1))

    # Atomic swap: rename old, move new in
    backup = OUTPUT_DIR / "trades_no_event_slug.parquet"
    if backup.exists():
        shutil.rmtree(backup)
    TRADES_OUT.rename(backup)
    shutil.move(str(staging), str(TRADES_OUT))
    log.info("Swapped in updated trades.parquet (old → %s)", backup)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-extract", action="store_true",
                        help="Skip Stage 1 (use existing raw_events.parquet)")
    parser.add_argument("--skip-dedup", action="store_true",
                        help="Skip Stage 2 dedup (reuse existing deduped_events.parquet)")
    parser.add_argument("--skip-stage3", action="store_true",
                        help="Skip Stage 3 token mapping (reuse cached gamma + mapped_events)")
    parser.add_argument("--skip-stage4", action="store_true",
                        help="Skip Stage 4 (reuse existing resolved_trades.parquet)")
    parser.add_argument("--skip-timestamps", action="store_true",
                        help="Skip block-timestamp fetch (use approximated timestamps)")
    parser.add_argument("--skip-stage6", action="store_true",
                        help="Skip Stage 6 rebuild (keep existing trades.parquet)")
    parser.add_argument("--skip-eventslug", action="store_true",
                        help="Skip the eventSlug backfill step")
    parser.add_argument("--target-block", type=int, default=None,
                        help="End block for Stage 1 (default: polygon head − 200)")
    parser.add_argument("--workers", type=int, default=50,
                        help="Workers for Stage 1 (50 fine). Timestamp fetch uses 16 internally.")
    parser.add_argument("--keep-cached-gamma", action="store_true",
                        help="Reuse cached gamma_markets/token_map/resolutions")
    args = parser.parse_args()

    last = get_current_endpoint()
    log.info("Current raw_events endpoint: block %d", last)

    if not args.skip_extract:
        target = args.target_block or (get_polygon_head() - 200)
        log.info("Polygon head: target block %d", target)
        if target <= last:
            log.info("Already up to date — skipping Stage 1")
            args.skip_extract = True
        else:
            archive = archive_canonical_files(last)
            run_stage1(last + 1, target, args.workers)
            if archive:
                concat_raw_events(archive)
    else:
        log.info("--skip-extract: skipping Stage 1 and merge")

    if not args.keep_cached_gamma and not args.skip_stage3:
        force_refresh_caches()
    elif args.skip_stage3:
        log.info("--skip-stage3: keeping cached gamma_markets/token_map (Stage 4 needs them)")

    if not args.skip_dedup:
        run_dedup()
    else:
        log.info("--skip-dedup: reusing existing deduped_events.parquet")

    stages_3_4 = [s for s, skip in ((3, args.skip_stage3), (4, args.skip_stage4)) if not skip]
    if stages_3_4:
        run_build_trades(stages_3_4)

    if not args.skip_timestamps:
        run_fetch_timestamps(workers=16)  # multi-process inside fetch_block_timestamps.py

    if not args.skip_stage6:
        run_build_trades([6])
        relocate_trades_output()
    else:
        log.info("--skip-stage6: keeping existing trades.parquet")

    if not args.skip_eventslug:
        fetch_events_to_map()
        build_token_to_event()
        backfill_event_slug()
    else:
        log.info("--skip-eventslug: eventSlug column will be empty for any rebuilt rows")

    log.info("Refresh complete.")


if __name__ == "__main__":
    main()
