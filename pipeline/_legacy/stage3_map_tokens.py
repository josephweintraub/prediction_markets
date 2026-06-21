"""
Stage 3: Map CTF token IDs to Polymarket markets using the Gamma API.

Downloads all market metadata from the Gamma API, builds a token_id → market
lookup table, and joins it to the deduplicated trade events.

Two modes:
  - Full: paginate through ALL markets (500K+). Used for production runs.
  - Targeted: look up only the token IDs found in our events. Used for test runs
    and is much faster.

Expected output: each trade gets market_id, outcome label, eventSlug, etc.
"""

import json
import time
import logging
from pathlib import Path

import duckdb
import pandas as pd
import requests

import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gamma API helpers
# ---------------------------------------------------------------------------

def fetch_all_markets() -> list[dict]:
    """
    Paginate through the Gamma API to fetch ALL market metadata.
    Returns a list of raw market dicts.
    """
    session = requests.Session()
    all_markets = []
    offset = 0
    min_interval = 1.0 / config.GAMMA_RATE_LIMIT

    ckpt_path = config.CHECKPOINT_DIR / "stage3_markets.json"
    if ckpt_path.exists():
        log.info("Resuming from checkpoint...")
        all_markets = json.loads(ckpt_path.read_text())
        offset = len(all_markets)
        log.info("Loaded %d markets from checkpoint, resuming at offset %d",
                 len(all_markets), offset)

    while True:
        t0 = time.time()
        url = f"{config.GAMMA_API_BASE}/markets"
        params = {"limit": config.GAMMA_PAGE_LIMIT, "offset": offset}

        for attempt in range(5):
            try:
                resp = session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning("Gamma API error (attempt %d): %s — retrying in %ds",
                            attempt + 1, e, wait)
                time.sleep(wait)
        else:
            log.error("Gamma API failed after 5 retries at offset %d", offset)
            break

        page = resp.json()
        if not page:
            break

        all_markets.extend(page)
        offset += len(page)

        if offset % 10_000 == 0:
            log.info("Fetched %d markets so far...", offset)
            ckpt_path.write_text(json.dumps(all_markets))

        elapsed = time.time() - t0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        if len(page) < config.GAMMA_PAGE_LIMIT:
            break

    log.info("Total markets fetched: %d", len(all_markets))
    if ckpt_path.exists():
        ckpt_path.unlink()

    return all_markets


def fetch_markets_for_tokens(token_ids: list[str]) -> list[dict]:
    """
    Fetch market metadata for specific token IDs using the Gamma API
    clob_token_ids query parameter. Much faster than full pagination
    when you only need a subset.
    """
    session = requests.Session()
    all_markets = []
    seen_ids = set()
    min_interval = 1.0 / config.GAMMA_RATE_LIMIT

    # Query one token at a time (API may not support comma-separated lists reliably)
    for i, tid in enumerate(token_ids):
        if tid in seen_ids or tid == "0":
            continue

        t0 = time.time()
        url = f"{config.GAMMA_API_BASE}/markets"
        params = {"clob_token_ids": tid}

        for attempt in range(3):
            try:
                resp = session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                break
            except Exception as e:
                wait = 2 ** attempt
                log.warning("Gamma lookup error for token %s: %s", tid[:20], e)
                time.sleep(wait)
        else:
            continue

        results = resp.json()
        for mkt in results:
            mkt_id = mkt.get("conditionId", mkt.get("id", ""))
            if mkt_id not in seen_ids:
                all_markets.append(mkt)
                seen_ids.add(mkt_id)

        if (i + 1) % 100 == 0:
            log.info("  Looked up %d / %d tokens, found %d markets",
                     i + 1, len(token_ids), len(all_markets))

        elapsed = time.time() - t0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    log.info("Targeted lookup: %d tokens → %d unique markets", len(token_ids), len(all_markets))
    return all_markets


def _extract_event_slug(mkt: dict) -> str:
    """
    Extract the parent event slug from a Gamma API market object.

    The API nests event info inside an 'events' array. Each element has a 'slug'.
    Falls back to the top-level 'eventSlug' field (rare), then the market 'slug'.
    """
    # Try top-level eventSlug first (some API versions include it)
    es = mkt.get("eventSlug", mkt.get("event_slug", ""))
    if es:
        return es

    # Try nested events[0].slug
    events = mkt.get("events", [])
    if events:
        try:
            # Could be a list, numpy array, or JSON string
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

    # Fallback to market slug
    return mkt.get("slug", "")


def build_token_map(markets: list[dict]) -> pd.DataFrame:
    """
    Build a mapping from CTF token_id → (conditionId, outcome, market_slug, event_slug).
    """
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
                        "market_slug": mkt.get("slug", ""),
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
    if len(df) > 0:
        log.info("Token map: %d token→market mappings across %d unique condition_ids",
                 len(df), df["condition_id"].nunique())
    else:
        log.warning("Token map is empty!")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_stage3(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """
    Run Stage 3: fetch Gamma markets, build token map, join to trades.
    Returns the number of mapped trades.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    own_con = False
    if con is None:
        con = duckdb.connect(config.DUCKDB_PATH)
        con.execute(f"SET memory_limit = '{config.DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads = {config.DUCKDB_THREADS}")
        own_con = True

    # Ensure deduped_events is loaded
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "deduped_events" not in tables:
        log.info("Loading deduped events from %s", config.DEDUPED_EVENTS_PATH)
        con.execute(f"""
            CREATE TABLE deduped_events AS
            SELECT * FROM read_parquet('{config.DEDUPED_EVENTS_PATH}')
        """)

    # --- Build or load token map ---
    if config.TOKEN_MAP_PATH.exists():
        log.info("Loading cached token map from %s", config.TOKEN_MAP_PATH)
        token_map_df = pd.read_parquet(config.TOKEN_MAP_PATH)
    else:
        if config.TEST_MODE:
            # Targeted lookup: get unique token IDs from events, query Gamma for just those
            log.info("Test mode: targeted token lookup from events...")
            asset_ids = con.execute("""
                SELECT DISTINCT maker_asset_id AS tid FROM deduped_events
                WHERE maker_asset_id != '0'
                UNION
                SELECT DISTINCT taker_asset_id FROM deduped_events
                WHERE taker_asset_id != '0'
            """).fetchdf()["tid"].tolist()
            log.info("Found %d unique non-zero asset IDs in events", len(asset_ids))
            raw_markets = fetch_markets_for_tokens(asset_ids)
        else:
            # Full pagination
            log.info("Fetching ALL markets from Gamma API (this may take a while)...")
            raw_markets = fetch_all_markets()

        # Save raw markets
        if raw_markets:
            markets_df = pd.DataFrame(raw_markets)
            markets_df.to_parquet(config.GAMMA_MARKETS_PATH, index=False)
            log.info("Saved %d markets to %s", len(markets_df), config.GAMMA_MARKETS_PATH)

        token_map_df = build_token_map(raw_markets)
        if len(token_map_df) > 0:
            token_map_df.to_parquet(config.TOKEN_MAP_PATH, index=False)
            log.info("Saved token map to %s", config.TOKEN_MAP_PATH)

    log.info("Token map: %d entries", len(token_map_df))

    # --- Register and join ---
    con.execute("DROP TABLE IF EXISTS token_map")
    con.execute("CREATE TABLE token_map AS SELECT * FROM token_map_df")

    log.info("Joining trades to token map...")
    con.execute("DROP TABLE IF EXISTS mapped_events")
    con.execute("""
        CREATE TABLE mapped_events AS
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
        FROM deduped_events e
        LEFT JOIN token_map tm1 ON e.maker_asset_id = tm1.token_id
        LEFT JOIN token_map tm2 ON e.taker_asset_id = tm2.token_id
    """)

    total = con.execute("SELECT COUNT(*) FROM mapped_events").fetchone()[0]
    mapped = con.execute(
        "SELECT COUNT(*) FROM mapped_events WHERE condition_id IS NOT NULL"
    ).fetchone()[0]
    unmapped = total - mapped
    coverage = mapped / max(total, 1) * 100

    log.info("Mapped: %d / %d (%.1f%%). Unmapped: %d", mapped, total, coverage, unmapped)

    if own_con:
        con.close()

    return mapped


if __name__ == "__main__":
    run_stage3()
