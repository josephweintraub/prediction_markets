"""Build exact bought-contract closes and literal phase fills for added sports."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    require_columns,
    write_json,
)


IDENTITY = ("transaction_hash", "log_index", "exchange_address")


def _stats(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, Any]:
    row = con.execute(
        f"""SELECT count(*) AS row_count,count(DISTINCT market_id) AS market_count,
                   count(DISTINCT event_slug) AS event_count,
                   coalesce(sum(usdc),0)::DOUBLE AS dollars
            FROM {relation}"""
    ).fetchone()
    return {"rows": int(row[0]), "markets": int(row[1]), "events": int(row[2]),
            "dollars": float(row[3])}


def build_trade_dataset(
    raw_trades_path: str | Path,
    timestamp_cache_path: str | Path,
    wallet_flags_path: str | Path,
    candidate_run_dir: str | Path,
    timing_run_dir: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    raw = Path(raw_trades_path).expanduser().resolve()
    timestamps = Path(timestamp_cache_path).expanduser().resolve()
    wallet_flags = Path(wallet_flags_path).expanduser().resolve()
    candidates = Path(candidate_run_dir).expanduser().resolve()
    timing = Path(timing_run_dir).expanduser().resolve()
    candidate_markets = candidates / "candidate_markets.parquet"
    candidate_tokens = candidates / "candidate_tokens.parquet"
    event_timing = timing / "event_timing.parquet"
    phase_boundaries = timing / "phase_boundaries.parquet"
    inputs = (raw, timestamps, wallet_flags, candidate_markets, candidate_tokens,
              event_timing, phase_boundaries)
    if any(not path.is_file() for path in inputs):
        missing = [str(path) for path in inputs if not path.is_file()]
        raise FileNotFoundError(f"Missing trade-build inputs: {missing}")

    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target, inputs) as staging:
        con = duckdb.connect()
        try:
            con.execute(f"CREATE VIEW raw_input AS SELECT * FROM read_parquet('{quoted(raw)}')")
            con.execute(f"CREATE VIEW block_cache AS SELECT * FROM read_parquet('{quoted(timestamps)}')")
            con.execute(f"CREATE VIEW flags_input AS SELECT * FROM read_parquet('{quoted(wallet_flags)}')")
            con.execute(f"CREATE VIEW markets AS SELECT * FROM read_parquet('{quoted(candidate_markets)}')")
            con.execute(f"CREATE VIEW tokens AS SELECT * FROM read_parquet('{quoted(candidate_tokens)}')")
            con.execute(f"CREATE VIEW timing AS SELECT * FROM read_parquet('{quoted(event_timing)}')")
            con.execute(f"CREATE VIEW boundaries AS SELECT * FROM read_parquet('{quoted(phase_boundaries)}')")
            require_columns(con, "raw_input", (
                "maker", "taker", "maker_asset_id", "taker_asset_id",
                "maker_amount_filled", "taker_amount_filled", "block_number",
                "transaction_hash", "log_index", "exchange_address", "condition_id",
                "outcome_token_side",
            ), "Resolved fills")
            require_columns(con, "block_cache", ("block_number", "timestamp"), "Exact block cache")
            require_columns(con, "flags_input", ("proxyWallet", "is_nonhuman"), "Wallet flags")
            require_columns(con, "markets", ("sport", "event_slug", "market_id", "market_date"), "Candidates")
            require_columns(con, "tokens", ("market_id", "token_id", "outcome", "won"), "Candidate tokens")
            require_columns(con, "timing", (
                "sport", "event_slug", "game_id", "market_date", "actual_start_utc", "actual_end_utc",
            ), "Event timing")
            require_columns(con, "boundaries", (
                "sport", "event_slug", "phase", "phase_order", "start_utc", "end_utc",
            ), "Phase boundaries")

            con.execute("""
                CREATE TABLE eligible_markets AS
                SELECT m.sport,m.event_slug,m.market_id,m.market_date,t.game_id,
                       t.actual_start_utc,t.actual_end_utc
                FROM markets m JOIN timing t USING(sport,event_slug,market_date)
            """)
            if con.execute("SELECT count(*) FROM eligible_markets").fetchone()[0] == 0:
                raise ValueError("No candidate markets have validated event timing")
            if con.execute("""SELECT count(*) FROM (
                    SELECT market_id,count(*) n FROM eligible_markets GROUP BY 1 HAVING n<>1)""").fetchone()[0]:
                raise ValueError("Eligible market timing must be unique")

            con.execute("""
                CREATE TABLE scoped_source AS
                SELECT r.maker,r.taker,r.maker_asset_id,r.taker_asset_id,
                       r.maker_amount_filled,r.taker_amount_filled,r.block_number,
                       r.transaction_hash,r.log_index,r.exchange_address,r.condition_id,
                       r.outcome_token_side
                FROM raw_input r JOIN eligible_markets e ON r.condition_id=e.market_id
            """)
            raw_scoped = int(con.execute("SELECT count(*) FROM scoped_source").fetchone()[0])
            if raw_scoped == 0:
                raise ValueError("No raw fills matched validated sport markets")
            con.execute("CREATE TABLE fills AS SELECT DISTINCT * FROM scoped_source")
            identity_conflicts = int(con.execute("""
                SELECT count(*) FROM (
                  SELECT transaction_hash,log_index,exchange_address,count(*) n
                  FROM fills GROUP BY 1,2,3 HAVING n<>1)
            """).fetchone()[0])
            if identity_conflicts:
                raise ValueError(f"Contradictory EVM identities: {identity_conflicts}")
            missing_blocks = int(con.execute("""
                SELECT count(*) FROM fills f ANTI JOIN block_cache b USING(block_number)
            """).fetchone()[0])
            duplicate_blocks = int(con.execute("""
                SELECT count(*) FROM (
                  SELECT block_number,count(*) n FROM block_cache GROUP BY 1 HAVING n<>1)
            """).fetchone()[0])
            if missing_blocks or duplicate_blocks:
                raise ValueError(
                    f"Exact timestamp gate failed: missing={missing_blocks}, duplicate={duplicate_blocks}"
                )
            con.execute("""
                CREATE TABLE flags AS
                SELECT proxyWallet,max(is_nonhuman::INTEGER)::BOOLEAN is_nonhuman
                FROM flags_input GROUP BY proxyWallet
            """)
            con.execute("""
                CREATE TABLE exact_buys AS
                SELECT e.sport,e.event_slug,e.market_id,e.market_date,e.game_id,
                       e.actual_start_utc,e.actual_end_utc,
                       CASE WHEN f.outcome_token_side='maker' THEN f.maker_asset_id
                            ELSE f.taker_asset_id END::VARCHAR token_id,
                       tok.outcome,tok.won,
                       f.block_number::BIGINT block_number,b.timestamp::BIGINT AS "timestamp",
                       f.transaction_hash::VARCHAR transaction_hash,
                       f.log_index::INTEGER log_index,f.exchange_address::VARCHAR exchange_address,
                       CASE WHEN f.outcome_token_side='maker' THEN f.taker ELSE f.maker END::VARCHAR proxyWallet,
                       CASE WHEN f.outcome_token_side='maker' THEN f.maker ELSE f.taker END::VARCHAR counterparty,
                       coalesce(w.is_nonhuman,false)::BOOLEAN buyer_is_flagged_nonhuman,
                       CASE WHEN f.outcome_token_side='maker'
                         THEN (f.taker_amount_filled/1000000.0)/(f.maker_amount_filled/1000000.0)
                         ELSE (f.maker_amount_filled/1000000.0)/(f.taker_amount_filled/1000000.0)
                       END::DOUBLE price,
                       CASE WHEN f.outcome_token_side='maker' THEN f.taker_amount_filled/1000000.0
                            ELSE f.maker_amount_filled/1000000.0 END::DOUBLE usdc
                FROM fills f
                JOIN eligible_markets e ON f.condition_id=e.market_id
                JOIN block_cache b USING(block_number)
                JOIN tokens tok ON tok.market_id=e.market_id AND tok.token_id=
                  CASE WHEN f.outcome_token_side='maker' THEN f.maker_asset_id ELSE f.taker_asset_id END
                LEFT JOIN flags w ON w.proxyWallet=
                  CASE WHEN f.outcome_token_side='maker' THEN f.taker ELSE f.maker END
                WHERE f.outcome_token_side IN ('maker','taker')
                  AND f.maker_amount_filled>0 AND f.taker_amount_filled>0
            """)
            invalid_buys = int(con.execute("""
                SELECT count(*) FROM exact_buys
                WHERE token_id IS NULL OR proxyWallet IS NULL OR price IS NULL OR usdc IS NULL
                   OR NOT isfinite(price) OR NOT isfinite(usdc) OR usdc<=0
            """).fetchone()[0])
            if invalid_buys:
                raise ValueError(f"Invalid expanded BUY rows: {invalid_buys}")
            exact_rows = int(con.execute("SELECT count(*) FROM exact_buys").fetchone()[0])
            distinct_fills = int(con.execute("SELECT count(*) FROM fills").fetchone()[0])
            if exact_rows != distinct_fills:
                raise ValueError(
                    f"BUY expansion did not reconcile: fills={distinct_fills}, buys={exact_rows}"
                )

            con.execute("""
                CREATE TABLE phase_trades AS
                WITH live AS (
                  SELECT b.*,p.phase,p.phase_order
                  FROM exact_buys b JOIN boundaries p USING(sport,event_slug)
                  WHERE to_timestamp(b."timestamp")>=p.start_utc
                    AND (to_timestamp(b."timestamp")<p.end_utc OR
                         (p.phase_order=(SELECT max(q.phase_order) FROM boundaries q
                                         WHERE q.sport=p.sport AND q.event_slug=p.event_slug)
                          AND to_timestamp(b."timestamp")<=p.end_utc))
                ), pregame AS (
                  SELECT b.*,'pregame'::VARCHAR phase,1::INTEGER phase_order
                  FROM exact_buys b WHERE to_timestamp(b."timestamp")<b.actual_start_utc
                )
                SELECT sport,event_slug,market_id,market_date,game_id,token_id,outcome,won,
                       block_number,"timestamp",transaction_hash,log_index,exchange_address,
                       proxyWallet,counterparty,buyer_is_flagged_nonhuman,price,usdc,
                       date(to_timestamp("timestamp")) trade_day,phase,phase_order,
                       (won::DOUBLE-price)::DOUBLE calibration_error,
                       actual_start_utc,actual_end_utc
                FROM (SELECT * FROM pregame UNION ALL SELECT * FROM live)
                WHERE NOT buyer_is_flagged_nonhuman AND price>0.01 AND price<0.99
            """)
            duplicate_phase_rows = int(con.execute("""
                SELECT count(*) FROM (
                  SELECT transaction_hash,log_index,exchange_address,count(*) n
                  FROM phase_trades GROUP BY 1,2,3 HAVING n<>1)
            """).fetchone()[0])
            if duplicate_phase_rows:
                raise ValueError(f"Fills assigned to multiple phases: {duplicate_phase_rows}")
            con.execute("""
                CREATE TABLE closing_lines AS
                WITH definitions AS (
                  SELECT *,'all_trades'::VARCHAR close_sample
                  FROM exact_buys
                  WHERE price>0 AND price<1 AND to_timestamp("timestamp")<actual_start_utc
                  UNION ALL
                  SELECT *,'filtered_trades'::VARCHAR close_sample
                  FROM exact_buys
                  WHERE price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman
                    AND to_timestamp("timestamp")<actual_start_utc
                ), ranked AS (
                  SELECT *,row_number() OVER (
                    PARTITION BY sport,market_id,close_sample
                    ORDER BY "timestamp" DESC,block_number DESC,log_index DESC,
                             transaction_hash DESC,exchange_address DESC) rank
                  FROM definitions
                )
                SELECT sport,event_slug,market_id,market_date,game_id,close_sample,
                       token_id,outcome,won,price,usdc,"timestamp",block_number,
                       transaction_hash,log_index,exchange_address,proxyWallet,
                       buyer_is_flagged_nonhuman,
                       (epoch(actual_start_utc)-"timestamp")::DOUBLE close_age_seconds,
                       (won::DOUBLE-price)::DOUBLE calibration_error
                FROM ranked WHERE rank=1
            """)
            if con.execute("SELECT count(*) FROM closing_lines WHERE close_age_seconds<=0").fetchone()[0]:
                raise ValueError("Closing fills must be strictly pregame")
            phase_stats = _stats(con, "phase_trades")
            close_stats = _stats(con, "closing_lines")
            phase_by_sport = {
                row[0]: int(row[1]) for row in con.execute(
                    "SELECT sport,count(*) FROM phase_trades GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            close_by_sport = {
                row[0]: int(row[1]) for row in con.execute(
                    "SELECT sport,count(*) FROM closing_lines GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            near_event_position = {
                sport: {
                    "pregame_6h": int(pregame),
                    "live": int(live),
                    "post_final_6h": int(post_final),
                }
                for sport, pregame, live, post_final in con.execute("""
                    SELECT sport,
                           count(*) FILTER (WHERE to_timestamp("timestamp")<actual_start_utc),
                           count(*) FILTER (WHERE to_timestamp("timestamp")>=actual_start_utc
                                             AND to_timestamp("timestamp")<=actual_end_utc),
                           count(*) FILTER (WHERE to_timestamp("timestamp")>actual_end_utc)
                    FROM exact_buys
                    WHERE to_timestamp("timestamp")>=actual_start_utc-INTERVAL 6 HOUR
                      AND to_timestamp("timestamp")<=actual_end_utc+INTERVAL 6 HOUR
                    GROUP BY 1 ORDER BY 1
                """).fetchall()
            }
            close_age_by_sport = {
                (sport, sample): {
                    "n": int(n),
                    "median_seconds": float(median),
                    "p90_seconds": float(p90),
                }
                for sport, sample, n, median, p90 in con.execute("""
                    SELECT sport,close_sample,count(*),
                           median(close_age_seconds),quantile_cont(close_age_seconds,0.9)
                    FROM closing_lines GROUP BY 1,2 ORDER BY 1,2
                """).fetchall()
            }
            con.execute(f"COPY exact_buys TO '{quoted(staging/'exact_buys.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            con.execute(f"COPY phase_trades TO '{quoted(staging/'phase_trades.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            con.execute(f"COPY closing_lines TO '{quoted(staging/'closing_lines.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
        finally:
            con.close()

        manifest = {
            "schema_version": 1,
            "stage": "multisport_exact_bought_contract_trade_dataset_v1",
            "filters": {
                "phase": "literal boundaries; 0.01 < price < 0.99; flagged outcome-token buyers excluded",
                "all_trades_close": "last 0 < price < 1 BUY fill strictly before actual start",
                "filtered_trades_close": "last 0.01 < price < 0.99 nonflagged-buyer BUY fill strictly before actual start",
                "post_final": "audit in exact_buys only; excluded from phase_trades",
            },
            "counts": {
                "raw_scoped_rows": raw_scoped,
                "distinct_fills": distinct_fills,
                "replay_duplicates": raw_scoped - distinct_fills,
                "exact_buy_rows": exact_rows,
                "missing_exact_blocks": missing_blocks,
                "phase": phase_stats,
                "closing": close_stats,
                "phase_rows_by_sport": phase_by_sport,
                "closing_rows_by_sport": close_by_sport,
                "near_event_position_by_sport": near_event_position,
                "close_age_by_sport_sample": {
                    f"{sport}:{sample}": values
                    for (sport, sample), values in close_age_by_sport.items()
                },
            },
            "inputs": {path.name: fingerprint(path) for path in inputs},
            "outputs": {
                name: artifact_fingerprint(staging / name)
                for name in ("exact_buys.parquet", "phase_trades.parquet", "closing_lines.parquet")
            },
        }
        write_json(staging / "trade_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-trades", required=True)
    parser.add_argument("--timestamp-cache", required=True)
    parser.add_argument("--wallet-flags", required=True)
    parser.add_argument("--candidate-run", required=True)
    parser.add_argument("--timing-run", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(json.dumps(build_trade_dataset(
        args.raw_trades, args.timestamp_cache, args.wallet_flags,
        args.candidate_run, args.timing_run, args.run_dir,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
