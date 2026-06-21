#!/usr/bin/env python3
"""
EC2-optimized Stages 3, 4, 6: Token mapping, resolution, and final transform.

Designed for 500M+ row datasets. Uses DuckDB views and COPY (streaming)
instead of CREATE TABLE to avoid loading everything into RAM.

Usage:
    python3 ec2_stages_3_4_6.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import logging
import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("stages_3_4_6.log"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEDUPED_PATH = DATA_DIR / "deduped_events.parquet"
TOKEN_MAP_PATH = DATA_DIR / "token_map.parquet"
GAMMA_MARKETS_PATH = DATA_DIR / "gamma_markets.parquet"
RESOLVED_MARKETS_PATH = DATA_DIR / "resolved_markets.parquet"
MAPPED_PATH = DATA_DIR / "mapped_events.parquet"
RESOLVED_TRADES_PATH = DATA_DIR / "resolved_trades.parquet"
TRADES_OUTPUT_DIR = OUTPUT_DIR / "trades.parquet"
RESOLUTIONS_OUTPUT_PATH = OUTPUT_DIR / "market_resolutions.parquet"
BLOCK_TIMESTAMPS_PATH = DATA_DIR / "block_timestamps.parquet"

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
GAMMA_PAGE_LIMIT = 500  # Gamma API caps at 500 per page
GAMMA_RATE_LIMIT = 5

EXCHANGE_V1 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
EXCHANGE_V2 = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

USDC_DECIMALS = 6
CTF_TOKEN_DECIMALS = 6


# ---------------------------------------------------------------------------
# DuckDB connection
# ---------------------------------------------------------------------------
def get_con():
    con = duckdb.connect()
    con.execute("SET memory_limit='48GB'")
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='200GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=8")
    return con


# ---------------------------------------------------------------------------
# Stage 3: Token mapping via Gamma API
# ---------------------------------------------------------------------------

def _fetch_one_filter(session, filters: dict, label: str, min_interval: float) -> list[dict]:
    """Paginate /markets/keyset with a given filter set until pages stop yielding new ids.

    The legacy /markets endpoint was deprecated 2026-05-01; offset > ~100K returns
    422. We use the cursor-based /markets/keyset endpoint instead.
    """
    out: list[dict] = []
    seen_ids: set[str] = set()
    cursor: str | None = None
    pages = 0
    stagnant = 0
    while True:
        t0 = time.time()
        params = {"limit": GAMMA_PAGE_LIMIT, **filters}
        if cursor:
            params["cursor"] = cursor
        for attempt in range(5):
            try:
                resp = session.get(f"{GAMMA_API_BASE}/markets/keyset", params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning("[%s] Gamma error (attempt %d): %s — retry in %ds", label, attempt+1, e, wait)
                time.sleep(wait)
        else:
            log.error("[%s] Gamma failed after 5 retries on cursor=%s", label, cursor)
            break

        body = resp.json()
        page = body.get("markets") or []
        next_cursor = body.get("next_cursor") or body.get("nextCursor")

        if not page:
            log.info("[%s] empty page received — stopping (cursor=%s)", label, cursor)
            break

        new = 0
        for m in page:
            mid = m.get("id") or m.get("conditionId") or m.get("slug")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                out.append(m)
                new += 1
        pages += 1

        if pages % 20 == 0:
            log.info("[%s] page=%d, kept=%d (page added %d new)", label, pages, len(out), new)

        elapsed = time.time() - t0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        if new == 0:
            stagnant += 1
            if stagnant >= 3:
                log.info("[%s] %d consecutive pages with no new ids — stopping at page %d",
                         label, stagnant, pages)
                break
        else:
            stagnant = 0

        if not next_cursor or next_cursor == cursor:
            log.info("[%s] no next_cursor — terminal page", label)
            break
        cursor = next_cursor

    log.info("[%s] done: %d unique markets across %d pages", label, len(out), pages)
    return out


CLOB_API_BASE = "https://clob.polymarket.com"


def fetch_all_markets() -> list[dict]:
    """Fetch every market via the CLOB /markets endpoint with cursor pagination.

    The Gamma /markets endpoint was deprecated 2026-05-01 and caps offset at
    ~100K. Gamma /markets/keyset advertises cursors but silently ignores them.
    The CLOB endpoint (clob.polymarket.com/markets) supports proper keyset
    pagination via `next_cursor` and terminates with `LTE=`. It returns ~1M+
    markets with a richer schema: tokens[].token_id, tokens[].outcome,
    tokens[].winner (bool), condition_id, market_slug, closed, etc.
    """
    session = requests.Session()

    ckpt_path = DATA_DIR / "checkpoints" / "stage3_markets.json"
    ckpt_path.parent.mkdir(parents=True, exist_ok=True)
    if ckpt_path.exists():
        log.info("Loading cached markets from checkpoint (%s)", ckpt_path)
        all_markets = json.loads(ckpt_path.read_text())
        log.info("Loaded %d cached markets — skipping refetch", len(all_markets))
        return all_markets

    all_markets: list[dict] = []
    cursor = ""
    page_n = 0
    t0 = time.time()
    while True:
        params = {"next_cursor": cursor} if cursor else {}
        for attempt in range(5):
            try:
                resp = session.get(f"{CLOB_API_BASE}/markets", params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning("CLOB API error (attempt %d): %s — retry in %ds", attempt + 1, e, wait)
                time.sleep(wait)
        else:
            log.error("CLOB API failed after 5 retries at cursor=%s", cursor[:30])
            break

        body = resp.json()
        data = body.get("data") or []
        nc = body.get("next_cursor")
        all_markets.extend(data)
        page_n += 1

        if page_n % 50 == 0:
            log.info("CLOB: page=%d total=%d elapsed=%.0fs", page_n, len(all_markets), time.time() - t0)
            ckpt_path.write_text(json.dumps(all_markets))

        if not nc or nc == cursor or nc == "LTE=":
            log.info("CLOB terminal sentinel: %r", nc)
            break
        cursor = nc

    log.info("Total markets fetched: %d in %d pages, %.1fs", len(all_markets), page_n, time.time() - t0)
    if ckpt_path.exists():
        ckpt_path.unlink()
    return all_markets


def _extract_event_slug(mkt: dict) -> str:
    es = mkt.get("eventSlug", mkt.get("event_slug", ""))
    if es:
        return es
    events = mkt.get("events", [])
    if events:
        try:
            if isinstance(events, str):
                events = json.loads(events)
            if hasattr(events, '__len__') and len(events) > 0:
                first = events[0]
                if isinstance(first, dict):
                    es = first.get("slug", "")
                    if es:
                        return es
        except (json.JSONDecodeError, TypeError, IndexError):
            pass
    return mkt.get("slug", "")


def build_token_map(markets: list[dict]) -> pd.DataFrame:
    rows = []
    for mkt in markets:
        event_slug = _extract_event_slug(mkt)
        tokens = mkt.get("tokens", [])
        if not tokens:
            clob_ids = mkt.get("clobTokenIds", "")
            outcomes = mkt.get("outcomes", "")
            if clob_ids and outcomes:
                try:
                    token_ids = json.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
                    outcome_labels = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
                except (json.JSONDecodeError, TypeError):
                    continue
                for tid, outcome in zip(token_ids, outcome_labels):
                    rows.append({
                        "token_id": str(tid),
                        "condition_id": mkt.get("conditionId", mkt.get("condition_id", "")),
                        "outcome": outcome,
                        "market_slug": mkt.get("slug", "") or mkt.get("market_slug", ""),
                        "event_slug": event_slug,
                        "question": mkt.get("question", ""),
                    })
            continue
        for token in tokens:
            rows.append({
                "token_id": str(token.get("token_id", "")),
                "condition_id": mkt.get("conditionId", mkt.get("condition_id", "")),
                "outcome": token.get("outcome", ""),
                "market_slug": mkt.get("slug", ""),
                "event_slug": event_slug,
                "question": mkt.get("question", ""),
            })
    df = pd.DataFrame(rows)
    log.info("Token map: %d entries, %d unique condition_ids",
             len(df), df["condition_id"].nunique() if len(df) > 0 else 0)
    return df


def run_stage3():
    """Fetch markets, build token map, join to deduped events → mapped_events.parquet"""
    log.info("=" * 60)
    log.info("STAGE 3: Token mapping")
    log.info("=" * 60)

    # Fetch or load token map
    if TOKEN_MAP_PATH.exists():
        log.info("Loading cached token map from %s", TOKEN_MAP_PATH)
        token_map_df = pd.read_parquet(TOKEN_MAP_PATH)
    else:
        log.info("Fetching ALL markets from Gamma API...")
        raw_markets = fetch_all_markets()
        if raw_markets:
            markets_df = pd.DataFrame(raw_markets)
            markets_df.to_parquet(GAMMA_MARKETS_PATH, index=False)
            log.info("Saved %d markets to %s", len(markets_df), GAMMA_MARKETS_PATH)
        token_map_df = build_token_map(raw_markets)
        if len(token_map_df) > 0:
            token_map_df.to_parquet(TOKEN_MAP_PATH, index=False)

    log.info("Token map: %d entries", len(token_map_df))

    # Join using DuckDB — stream directly to parquet
    log.info("Joining deduped events to token map (streaming to parquet)...")
    con = get_con()

    # Register token map as a small table (fits in RAM easily)
    con.execute("CREATE TABLE token_map AS SELECT * FROM token_map_df")

    con.execute(f"""
        COPY (
            SELECT
                e.*,
                COALESCE(tm1.condition_id, tm2.condition_id) AS condition_id,
                COALESCE(tm1.outcome, tm2.outcome) AS outcome,
                COALESCE(tm1.market_slug, tm2.market_slug) AS market_slug,
                COALESCE(tm1.event_slug, tm2.event_slug) AS event_slug,
                COALESCE(tm1.question, tm2.question) AS question,
                CASE
                    WHEN tm1.token_id IS NOT NULL THEN 'maker'
                    WHEN tm2.token_id IS NOT NULL THEN 'taker'
                    ELSE NULL
                END AS outcome_token_side
            FROM read_parquet('{DEDUPED_PATH}') e
            LEFT JOIN token_map tm1 ON e.maker_asset_id = tm1.token_id
            LEFT JOIN token_map tm2 ON e.taker_asset_id = tm2.token_id
        )
        TO '{MAPPED_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{MAPPED_PATH}')").fetchone()[0]
    mapped = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{MAPPED_PATH}')
        WHERE condition_id IS NOT NULL
    """).fetchone()[0]
    log.info("Total: %d, Mapped: %d (%.1f%%)", total, mapped, mapped / max(total, 1) * 100)
    con.close()
    return mapped


# ---------------------------------------------------------------------------
# Stage 4: Market resolution
# ---------------------------------------------------------------------------

def parse_resolutions(markets) -> pd.DataFrame:
    """Parse resolution info from CLOB market objects.

    CLOB returns each market with `tokens=[{token_id, outcome, price, winner}, ...]`
    and a `closed` flag. We treat a market as resolved iff it's `closed` AND has
    exactly one token with `winner=true`. The legacy Gamma path that parsed
    outcomePrices >= 0.99 is also kept as a fallback for older cached data.
    """
    if isinstance(markets, pd.DataFrame):
        markets = markets.to_dict(orient="records")

    rows = []
    for mkt in markets:
        condition_id = mkt.get("conditionId") or mkt.get("condition_id", "")
        tokens = mkt.get("tokens", [])
        # Normalize: parquet round-trip can yield numpy arrays of dict structs
        if hasattr(tokens, "tolist"):
            tokens = tokens.tolist()
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except (json.JSONDecodeError, TypeError):
                tokens = []
        if not isinstance(tokens, list):
            tokens = []

        # Path A: CLOB-style — winner flag on tokens
        if len(tokens) > 0 and any(isinstance(t, dict) and "winner" in t for t in tokens):
            if not mkt.get("closed"):
                continue
            winner_tokens = [t for t in tokens if isinstance(t, dict) and t.get("winner")]
            if len(winner_tokens) != 1:
                continue
            winning_outcome = winner_tokens[0].get("outcome", "")
            for t in tokens:
                if isinstance(t, dict):
                    rows.append({
                        "token_id": str(t.get("token_id", "")),
                        "winning_outcome": winning_outcome,
                        "condition_id": condition_id,
                    })
            continue

        # Path B: Gamma-style — outcomePrices array, look for >= 0.99
        outcome_prices_raw = mkt.get("outcomePrices", mkt.get("outcome_prices", ""))
        outcomes_raw = mkt.get("outcomes", "")
        if not outcome_prices_raw or not outcomes_raw:
            continue

        try:
            if isinstance(outcome_prices_raw, str):
                outcome_prices_raw = outcome_prices_raw.strip()
                if outcome_prices_raw.startswith("["):
                    prices = json.loads(outcome_prices_raw)
                else:
                    prices = [float(x.strip()) for x in outcome_prices_raw.split(",")]
            elif isinstance(outcome_prices_raw, list):
                prices = [float(x) for x in outcome_prices_raw]
            else:
                continue
        except (ValueError, json.JSONDecodeError):
            continue

        try:
            if isinstance(outcomes_raw, str):
                outcomes_raw = outcomes_raw.strip()
                if outcomes_raw.startswith("["):
                    outcomes = json.loads(outcomes_raw)
                else:
                    outcomes = [x.strip().strip('"') for x in outcomes_raw.split(",")]
            elif isinstance(outcomes_raw, list):
                outcomes = outcomes_raw
            else:
                continue
        except (ValueError, json.JSONDecodeError):
            continue

        if len(prices) != len(outcomes):
            continue

        winning_outcome = None
        for price, outcome in zip(prices, outcomes):
            if float(price) >= 0.99:
                winning_outcome = outcome
                break
        if winning_outcome is None:
            continue

        clob_ids = mkt.get("clobTokenIds", "")
        if tokens:
            for token in tokens:
                if isinstance(token, dict):
                    rows.append({
                        "token_id": str(token.get("token_id", "")),
                        "winning_outcome": winning_outcome,
                        "condition_id": condition_id,
                    })
        elif clob_ids and isinstance(clob_ids, (str, list)):
            try:
                token_ids = json.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
                if not isinstance(token_ids, list):
                    continue
            except (json.JSONDecodeError, TypeError):
                continue
            for tid in token_ids:
                rows.append({
                    "token_id": str(tid),
                    "winning_outcome": winning_outcome,
                    "condition_id": condition_id,
                })

    df = pd.DataFrame(rows)
    if len(df) > 0:
        log.info("Parsed resolutions: %d token entries, %d unique markets",
                 len(df), df["condition_id"].nunique())
    return df


def run_stage4():
    """Parse resolutions, join to mapped events → resolved_trades.parquet"""
    log.info("=" * 60)
    log.info("STAGE 4: Market resolution")
    log.info("=" * 60)

    # Load or build resolutions
    if RESOLVED_MARKETS_PATH.exists():
        log.info("Loading cached resolutions from %s", RESOLVED_MARKETS_PATH)
        resolutions_df = pd.read_parquet(RESOLVED_MARKETS_PATH)
    else:
        if GAMMA_MARKETS_PATH.exists():
            log.info("Parsing resolutions from cached Gamma markets (chunked)...")
            # Read in chunks to avoid OOM when converting to dicts
            markets_df = pd.read_parquet(GAMMA_MARKETS_PATH)
            chunk_size = 50_000
            all_res = []
            for i in range(0, len(markets_df), chunk_size):
                chunk = markets_df.iloc[i:i+chunk_size]
                res = parse_resolutions(chunk.to_dict(orient="records"))
                if len(res) > 0:
                    all_res.append(res)
                log.info("  Parsed chunk %d-%d, found %d resolutions so far",
                         i, min(i+chunk_size, len(markets_df)),
                         sum(len(r) for r in all_res))
                del chunk
            resolutions_df = pd.concat(all_res, ignore_index=True) if all_res else pd.DataFrame()
            del markets_df, all_res
        else:
            log.info("Fetching resolved markets from Gamma API...")
            session = requests.Session()
            all_resolved = []
            offset = 0
            while True:
                t0 = time.time()
                params = {"limit": GAMMA_PAGE_LIMIT, "offset": offset, "closed": "true"}
                for attempt in range(5):
                    try:
                        resp = session.get(f"{GAMMA_API_BASE}/markets", params=params, timeout=30)
                        resp.raise_for_status()
                        break
                    except Exception as e:
                        time.sleep(2 ** attempt)
                else:
                    break
                page = resp.json()
                if not page:
                    break
                all_resolved.extend(page)
                offset += len(page)
                if offset % 10_000 == 0:
                    log.info("Fetched %d resolved markets...", offset)
                elapsed = time.time() - t0
                if elapsed < 1.0 / GAMMA_RATE_LIMIT:
                    time.sleep(1.0 / GAMMA_RATE_LIMIT - elapsed)
                if len(page) < GAMMA_PAGE_LIMIT:
                    break
            resolutions_df = parse_resolutions(all_resolved)

        if len(resolutions_df) > 0:
            resolutions_df.to_parquet(RESOLVED_MARKETS_PATH, index=False)
            log.info("Saved resolutions to %s", RESOLVED_MARKETS_PATH)

    log.info("Resolution entries: %d", len(resolutions_df))

    # Join mapped events to resolutions — streaming to parquet
    log.info("Joining mapped events to resolutions...")
    con = get_con()
    con.execute("CREATE TABLE resolutions AS SELECT * FROM resolutions_df")

    con.execute(f"""
        COPY (
            SELECT
                me.*,
                r.winning_outcome
            FROM read_parquet('{MAPPED_PATH}') me
            INNER JOIN resolutions r
                ON COALESCE(
                    CASE WHEN me.outcome_token_side = 'maker' THEN me.maker_asset_id END,
                    CASE WHEN me.outcome_token_side = 'taker' THEN me.taker_asset_id END
                ) = r.token_id
            WHERE me.condition_id IS NOT NULL
        )
        TO '{RESOLVED_TRADES_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    resolved = con.execute(f"SELECT COUNT(*) FROM read_parquet('{RESOLVED_TRADES_PATH}')").fetchone()[0]
    mapped = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{MAPPED_PATH}') WHERE condition_id IS NOT NULL
    """).fetchone()[0]
    log.info("Resolved: %d / %d mapped (%.1f%% coverage)", resolved, mapped, resolved / max(mapped, 1) * 100)
    con.close()
    return resolved


# ---------------------------------------------------------------------------
# Stage 6: Final transform
# ---------------------------------------------------------------------------

def run_stage6():
    """Maker+taker expansion, timestamps, final schema → trades.parquet + market_resolutions.parquet"""
    log.info("=" * 60)
    log.info("STAGE 6: Final transform")
    log.info("=" * 60)

    con = get_con()
    usdc_scale = 10 ** USDC_DECIMALS
    ctf_scale = 10 ** CTF_TOKEN_DECIMALS

    import shutil
    if TRADES_OUTPUT_DIR.exists():
        shutil.rmtree(TRADES_OUTPUT_DIR)
    TRADES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load real block timestamps if available, otherwise use approximation
    use_real_ts = BLOCK_TIMESTAMPS_PATH.exists()
    approx_expr = "(1667260800 + (rt.block_number - 21000000) * 1.676312)::BIGINT"
    if use_real_ts:
        log.info("6a: Using REAL block timestamps from %s (with approx fallback for missing blocks)",
                 BLOCK_TIMESTAMPS_PATH)
        con.execute(f"CREATE TABLE block_ts AS SELECT * FROM read_parquet('{BLOCK_TIMESTAMPS_PATH}')")
        # LEFT JOIN so trades on un-fetched blocks survive with approximated ts.
        ts_expr = f"COALESCE(bt.timestamp, {approx_expr})"
        ts_join = "LEFT JOIN block_ts bt ON rt.block_number = bt.block_number"
    else:
        log.info("6a: Using approximate timestamps (1.676s/block)")
        ts_expr = approx_expr
        ts_join = ""

    log.info("6a: Building trades (maker+taker expansion) → partitioned parquet...")
    con.execute(f"""
        COPY (
            WITH expanded AS (
                -- MAKER row
                SELECT
                    rt.maker AS proxyWallet,
                    {ts_expr} AS timestamp,
                    CASE WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                         ELSE rt.taker_asset_id END AS conditionId,
                    CASE WHEN rt.outcome_token_side = 'maker'
                         THEN rt.taker_amount_filled / {usdc_scale}.0
                         ELSE rt.maker_amount_filled / {usdc_scale}.0 END AS usdcSize,
                    CASE WHEN rt.outcome_token_side = 'maker'
                         THEN (rt.taker_amount_filled / {usdc_scale}.0)
                              / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)
                         ELSE (rt.maker_amount_filled / {usdc_scale}.0)
                              / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0) END AS price,
                    CASE WHEN rt.outcome_token_side = 'maker' THEN 'SELL' ELSE 'BUY' END AS side,
                    rt.outcome,
                    rt.event_slug AS eventSlug,
                    TRUE AS is_maker,
                    rt.taker AS counterparty
                FROM read_parquet('{RESOLVED_TRADES_PATH}') rt
                {ts_join}

                UNION ALL

                -- TAKER row
                SELECT
                    rt.taker AS proxyWallet,
                    {ts_expr} AS timestamp,
                    CASE WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                         ELSE rt.taker_asset_id END AS conditionId,
                    CASE WHEN rt.outcome_token_side = 'maker'
                         THEN rt.taker_amount_filled / {usdc_scale}.0
                         ELSE rt.maker_amount_filled / {usdc_scale}.0 END AS usdcSize,
                    CASE WHEN rt.outcome_token_side = 'maker'
                         THEN (rt.taker_amount_filled / {usdc_scale}.0)
                              / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)
                         ELSE (rt.maker_amount_filled / {usdc_scale}.0)
                              / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0) END AS price,
                    CASE WHEN rt.outcome_token_side = 'maker' THEN 'BUY' ELSE 'SELL' END AS side,
                    rt.outcome,
                    rt.event_slug AS eventSlug,
                    FALSE AS is_maker,
                    rt.maker AS counterparty
                FROM read_parquet('{RESOLVED_TRADES_PATH}') rt
                {ts_join}
            )
            SELECT
                proxyWallet, timestamp, conditionId, usdcSize, price,
                side, outcome, eventSlug, is_maker, counterparty,
                strftime(to_timestamp(timestamp), '%Y-%m') AS year_month
            FROM expanded
            WHERE price IS NOT NULL AND price > 0 AND price <= 1
        )
        TO '{TRADES_OUTPUT_DIR}' (
            FORMAT PARQUET,
            PARTITION_BY (year_month),
            COMPRESSION ZSTD,
            OVERWRITE_OR_IGNORE
        )
    """)

    trade_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{TRADES_OUTPUT_DIR}/**/*.parquet')
    """).fetchone()[0]
    log.info("Final trades written: %d", trade_count)

    # 6b: Market resolutions
    log.info("6b: Building market_resolutions...")
    con.execute(f"""
        COPY (
            SELECT DISTINCT
                CASE WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id
                     ELSE rt.taker_asset_id END AS conditionId,
                rt.winning_outcome
            FROM read_parquet('{RESOLVED_TRADES_PATH}') rt
            WHERE rt.condition_id IS NOT NULL
        )
        TO '{RESOLUTIONS_OUTPUT_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    resolution_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{RESOLUTIONS_OUTPUT_PATH}')
    """).fetchone()[0]
    log.info("Resolution entries: %d", resolution_count)

    # 6c: Validate
    log.info("6c: Validating...")
    trades_glob = f"{TRADES_OUTPUT_DIR}/**/*.parquet"

    types = con.execute(f"""
        SELECT typeof(proxyWallet), typeof(timestamp), typeof(conditionId),
               typeof(usdcSize), typeof(price), typeof(side),
               typeof(outcome), typeof(eventSlug)
        FROM '{trades_glob}' LIMIT 1
    """).fetchone()
    expected = ("VARCHAR", "BIGINT", "VARCHAR", "DOUBLE", "DOUBLE", "VARCHAR", "VARCHAR", "VARCHAR")
    log.info("  Types: %s", "OK" if types == expected else f"MISMATCH {types}")

    ts_range = con.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM '{trades_glob}'").fetchone()
    log.info("  Timestamp range: %d – %d", ts_range[0], ts_range[1])

    sides = con.execute(f"SELECT DISTINCT side FROM '{trades_glob}'").fetchdf()["side"].tolist()
    log.info("  Sides: %s", sides)

    price_stats = con.execute(f"SELECT MIN(price), MAX(price), AVG(price) FROM '{trades_glob}'").fetchone()
    log.info("  Price: min=%.4f, max=%.4f, avg=%.4f", *price_stats)

    slugs = con.execute(f"""
        SELECT eventSlug, COUNT(*) as n FROM '{trades_glob}'
        WHERE eventSlug IS NOT NULL AND eventSlug != ''
        GROUP BY eventSlug ORDER BY n DESC LIMIT 10
    """).fetchdf()
    log.info("  Top event slugs:\n%s", slugs.to_string(index=False))

    con.close()

    log.info("=" * 60)
    log.info("ALL STAGES COMPLETE")
    log.info("  Trades: %d → %s", trade_count, TRADES_OUTPUT_DIR)
    log.info("  Resolutions: %d → %s", resolution_count, RESOLUTIONS_OUTPUT_PATH)
    log.info("=" * 60)

    return {"trade_rows": trade_count, "resolution_rows": resolution_count}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--stages", nargs="+", type=int, default=[3, 4, 6])
    args = parser.parse_args()

    t0 = time.time()
    if 3 in args.stages:
        run_stage3()
    if 4 in args.stages:
        run_stage4()
    if 6 in args.stages:
        run_stage6()
    log.info("Total time: %.1f minutes", (time.time() - t0) / 60)
