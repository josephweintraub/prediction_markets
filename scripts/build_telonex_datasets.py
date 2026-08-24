#!/usr/bin/env python
"""Build Telonex Polymarket datasets + token-grain coverage crosswalk.

Downloads Telonex's free Polymarket datasets (no API key required):
  - markets catalog (per-market metadata + per-channel data-coverage dates)
  - tags dataset
then aligns the catalog with our canonical token spine
(/mnt/data/pipeline_output/market_flags.parquet, 100% of trades_clean tokens)
into a token-grain coverage file, so Telonex coverage joins directly onto
anything keyed by token_id.

Outputs (all in /mnt/data/telonex/):
  telonex_polymarket_markets.parquet   catalog as delivered, market grain
  telonex_polymarket_tags.parquet      tag definitions
  telonex_coverage_by_token.parquet    one row per market_flags token_id
  telonex_build_summary.json           match rates + coverage counts

Deliverable copy: Dropbox "Polymarket Data and Code/telonex/" (via rclone).

Re-running skips downloads if the catalog/tags files already exist; delete
them (or pass --force) to re-pull.
"""

import argparse
import datetime as dt
import json
import os
import shutil
import urllib.request

import duckdb

OUT_DIR = "/mnt/data/telonex"
MARKET_FLAGS = "/mnt/data/pipeline_output/market_flags.parquet"
DATASETS = {
    "telonex_polymarket_markets.parquet": "https://api.telonex.io/v1/datasets/polymarket/markets",
    "telonex_polymarket_tags.parquet": "https://api.telonex.io/v1/datasets/polymarket/tags",
}

COVERAGE_COLS = [
    "quotes_from", "quotes_to",
    "trades_from", "trades_to",
    "book_snapshot_5_from", "book_snapshot_5_to",
    "book_snapshot_25_from", "book_snapshot_25_to",
    "book_snapshot_full_from", "book_snapshot_full_to",
    "onchain_fills_from", "onchain_fills_to",
]


def download(url: str, path: str, force: bool) -> None:
    if os.path.exists(path) and not force:
        print(f"exists, skipping download: {path}")
        return
    print(f"downloading {url} -> {path}")
    tmp = path + ".tmp"
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as f:
        shutil.copyfileobj(resp, f, length=1 << 20)
    os.replace(tmp, path)
    print(f"  done ({os.path.getsize(path) / 1e6:.0f} MB)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="re-download datasets")
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)
    for fname, url in DATASETS.items():
        download(url, os.path.join(OUT_DIR, fname), args.force)

    catalog = os.path.join(OUT_DIR, "telonex_polymarket_markets.parquet")
    crosswalk = os.path.join(OUT_DIR, "telonex_coverage_by_token.parquet")

    con = duckdb.connect()
    cov = ", ".join(f"t.{c}" for c in COVERAGE_COLS)
    # Unpivot the market-grain catalog to token grain (one row per outcome
    # token), then LEFT JOIN from our spine so the output covers exactly the
    # tokens in trades_clean. Token ids are 77-digit decimal strings on both
    # sides; cast to VARCHAR defensively.
    con.execute(f"""
        CREATE VIEW catalog AS SELECT * FROM read_parquet('{catalog}');
        CREATE VIEW tlx_tokens AS
        WITH unpivoted AS (
            SELECT CAST(asset_id_0 AS VARCHAR) AS token_id,
                   market_id AS tlx_market_id, slug AS tlx_slug,
                   event_slug AS tlx_event_slug, question AS tlx_question,
                   status AS tlx_status, outcome_0 AS tlx_outcome,
                   0 AS tlx_outcome_id, {', '.join(COVERAGE_COLS)}
            FROM catalog WHERE asset_id_0 IS NOT NULL AND asset_id_0 <> ''
            UNION ALL
            SELECT CAST(asset_id_1 AS VARCHAR),
                   market_id, slug, event_slug, question, status, outcome_1,
                   1, {', '.join(COVERAGE_COLS)}
            FROM catalog WHERE asset_id_1 IS NOT NULL AND asset_id_1 <> ''
        )
        SELECT * FROM unpivoted
        QUALIFY row_number() OVER (PARTITION BY token_id ORDER BY tlx_market_id) = 1;
    """)
    con.execute(f"""
        COPY (
            SELECT CAST(mf.token_id AS VARCHAR) AS token_id,
                   mf.market_id AS our_market_id,
                   t.token_id IS NOT NULL AS in_telonex,
                   t.tlx_market_id, t.tlx_slug, t.tlx_event_slug,
                   t.tlx_question, t.tlx_status, t.tlx_outcome, t.tlx_outcome_id,
                   {cov}
            FROM read_parquet('{MARKET_FLAGS}') mf
            LEFT JOIN tlx_tokens t ON CAST(mf.token_id AS VARCHAR) = t.token_id
        ) TO '{crosswalk}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    def one(sql: str):
        return con.execute(sql).fetchone()[0]

    summary = {
        "built_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "script": "scripts/build_telonex_datasets.py",
        "catalog_markets": one("SELECT count(*) FROM catalog"),
        "catalog_markets_with_quotes": one(
            "SELECT count(*) FROM catalog WHERE quotes_from IS NOT NULL AND quotes_from <> ''"),
        "our_tokens": one(f"SELECT count(*) FROM read_parquet('{crosswalk}')"),
        "our_tokens_in_telonex": one(
            f"SELECT count(*) FROM read_parquet('{crosswalk}') WHERE in_telonex"),
        "our_tokens_with_quotes": one(
            f"SELECT count(*) FROM read_parquet('{crosswalk}') "
            "WHERE quotes_from IS NOT NULL AND quotes_from <> ''"),
        "our_tokens_with_onchain_fills": one(
            f"SELECT count(*) FROM read_parquet('{crosswalk}') "
            "WHERE onchain_fills_from IS NOT NULL AND onchain_fills_from <> ''"),
        "quotes_from_min": one(
            f"SELECT min(quotes_from) FROM read_parquet('{crosswalk}') WHERE quotes_from <> ''"),
        "quotes_to_max": one(
            f"SELECT max(quotes_to) FROM read_parquet('{crosswalk}') WHERE quotes_to <> ''"),
    }
    with open(os.path.join(OUT_DIR, "telonex_build_summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
