"""
Stage 4: Resolve markets — determine which outcome won for each market.

Fetches resolution data from the Gamma API and filters trades to only
resolved markets. Expected: ~95.4% coverage (222M of 233M trades).

In test mode, reuses the market data already fetched in Stage 3 (which
contains outcomePrices for resolved markets) rather than re-paginating.
"""

import json
import time
import logging

import duckdb
import pandas as pd
import requests

import config

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gamma API: fetch resolved markets
# ---------------------------------------------------------------------------

def fetch_resolved_markets() -> list[dict]:
    """
    Fetch all closed/resolved markets from the Gamma API.
    Paginates through all pages.
    """
    session = requests.Session()
    all_resolved = []
    offset = 0
    min_interval = 1.0 / config.GAMMA_RATE_LIMIT

    while True:
        t0 = time.time()
        url = f"{config.GAMMA_API_BASE}/markets"
        params = {
            "limit": config.GAMMA_PAGE_LIMIT,
            "offset": offset,
            "closed": "true",
        }

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

        all_resolved.extend(page)
        offset += len(page)

        if offset % 10_000 == 0:
            log.info("Fetched %d resolved markets...", offset)

        elapsed = time.time() - t0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        if len(page) < config.GAMMA_PAGE_LIMIT:
            break

    log.info("Total resolved markets fetched: %d", len(all_resolved))
    return all_resolved


def fetch_resolutions_for_condition_ids(condition_ids: list[str]) -> list[dict]:
    """
    Fetch resolution status for specific markets by condition_id.
    Uses the Gamma API condition_id parameter for targeted lookup.
    """
    session = requests.Session()
    all_markets = []
    seen = set()
    min_interval = 1.0 / config.GAMMA_RATE_LIMIT

    for i, cid in enumerate(condition_ids):
        if cid in seen or not cid:
            continue

        t0 = time.time()
        url = f"{config.GAMMA_API_BASE}/markets"
        params = {"condition_id": cid}

        for attempt in range(3):
            try:
                resp = session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                break
            except Exception:
                time.sleep(2 ** attempt)
        else:
            continue

        results = resp.json()
        for mkt in results:
            mid = mkt.get("conditionId", "")
            if mid not in seen:
                all_markets.append(mkt)
                seen.add(mid)

        if (i + 1) % 200 == 0:
            log.info("  Checked resolution for %d / %d condition_ids, found %d markets",
                     i + 1, len(condition_ids), len(all_markets))

        elapsed = time.time() - t0
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    log.info("Targeted resolution lookup: %d condition_ids → %d markets",
             len(condition_ids), len(all_markets))
    return all_markets


def parse_resolutions(markets: list[dict]) -> pd.DataFrame:
    """
    Parse resolution data from Gamma API market objects.
    Returns DataFrame with: token_id, winning_outcome, condition_id.
    """
    rows = []

    for mkt in markets:
        outcome_prices_raw = mkt.get("outcomePrices", mkt.get("outcome_prices", ""))
        outcomes_raw = mkt.get("outcomes", "")

        if not outcome_prices_raw or not outcomes_raw:
            continue

        # Parse outcome prices
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

        # Parse outcomes
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

        # Determine the winning outcome (price == 1)
        winning_outcome = None
        for price, outcome in zip(prices, outcomes):
            if float(price) >= 0.99:
                winning_outcome = outcome
                break

        if winning_outcome is None:
            continue

        # Get token IDs
        tokens = mkt.get("tokens", [])
        clob_ids = mkt.get("clobTokenIds", "")

        if tokens:
            for token in tokens:
                rows.append({
                    "token_id": str(token.get("token_id", "")),
                    "winning_outcome": winning_outcome,
                    "condition_id": mkt.get("conditionId", mkt.get("condition_id", "")),
                })
        elif clob_ids:
            try:
                token_ids = json.loads(clob_ids) if isinstance(clob_ids, str) else clob_ids
            except (json.JSONDecodeError, TypeError):
                continue
            for tid in token_ids:
                rows.append({
                    "token_id": str(tid),
                    "winning_outcome": winning_outcome,
                    "condition_id": mkt.get("conditionId", mkt.get("condition_id", "")),
                })

    df = pd.DataFrame(rows)
    if len(df) > 0:
        log.info("Parsed resolutions: %d token entries, %d unique markets",
                 len(df), df["condition_id"].nunique())
    else:
        log.warning("No resolved markets found in this batch")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_stage4(con: duckdb.DuckDBPyConnection | None = None) -> int:
    """
    Run Stage 4: fetch resolved markets, parse outcomes, filter trades.
    Returns the number of trades in resolved markets.
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

    # Check if mapped_events exists
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "mapped_events" not in tables:
        log.error("Table 'mapped_events' not found. Run Stage 3 first.")
        if own_con:
            con.close()
        return 0

    # --- Fetch or load resolution data ---
    if config.RESOLVED_MARKETS_PATH.exists():
        log.info("Loading cached resolutions from %s", config.RESOLVED_MARKETS_PATH)
        resolutions_df = pd.read_parquet(config.RESOLVED_MARKETS_PATH)
    else:
        if config.TEST_MODE and config.GAMMA_MARKETS_PATH.exists():
            # In test mode, Stage 3 already fetched market metadata for our tokens.
            # Parse resolution data directly from that cached data — no extra API calls.
            log.info("Test mode: parsing resolutions from Stage 3 market cache...")
            cached_markets = pd.read_parquet(config.GAMMA_MARKETS_PATH)
            # Convert parquet rows back to dicts for parse_resolutions
            resolved_markets = cached_markets.to_dict(orient="records")
            log.info("Loaded %d cached markets from Stage 3", len(resolved_markets))
        else:
            # Full pagination of all closed markets
            resolved_markets = fetch_resolved_markets()

        resolutions_df = parse_resolutions(resolved_markets)
        if len(resolutions_df) > 0:
            resolutions_df.to_parquet(config.RESOLVED_MARKETS_PATH, index=False)
            log.info("Saved resolutions to %s", config.RESOLVED_MARKETS_PATH)

    log.info("Resolution entries: %d", len(resolutions_df))

    # --- Register in DuckDB and join ---
    con.execute("DROP TABLE IF EXISTS resolutions")
    if len(resolutions_df) > 0:
        con.execute("CREATE TABLE resolutions AS SELECT * FROM resolutions_df")
    else:
        con.execute("""
            CREATE TABLE resolutions (
                token_id VARCHAR,
                winning_outcome VARCHAR,
                condition_id VARCHAR
            )
        """)

    # Filter to resolved markets
    log.info("Filtering to resolved markets...")
    con.execute("DROP TABLE IF EXISTS resolved_trades")
    con.execute("""
        CREATE TABLE resolved_trades AS
        SELECT
            me.*,
            r.winning_outcome
        FROM mapped_events me
        INNER JOIN resolutions r
            ON COALESCE(
                CASE WHEN me.outcome_token_side = 'maker' THEN me.maker_asset_id END,
                CASE WHEN me.outcome_token_side = 'taker' THEN me.taker_asset_id END
            ) = r.token_id
        WHERE me.condition_id IS NOT NULL
    """)

    total_mapped = con.execute(
        "SELECT COUNT(*) FROM mapped_events WHERE condition_id IS NOT NULL"
    ).fetchone()[0]
    resolved_count = con.execute("SELECT COUNT(*) FROM resolved_trades").fetchone()[0]
    coverage = resolved_count / max(total_mapped, 1) * 100

    log.info("Resolved trades: %d / %d mapped (%.1f%% coverage)",
             resolved_count, total_mapped, coverage)

    if own_con:
        con.close()

    return resolved_count


if __name__ == "__main__":
    run_stage4()
