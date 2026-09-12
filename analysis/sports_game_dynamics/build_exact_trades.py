"""Build immutable sport-scoped BUY fills with exact Polygon timestamps."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import duckdb

from .artifacts import (
    ArtifactError, INTEGER_TYPES, artifact_fingerprint, fingerprint, fresh_run, quoted, require_columns,
    require_exact_schema, require_sport, resolved, write_json,
)
from .schemas import ELIGIBLE_SCHEMA, EXACT_TRADE_SCHEMA
from .timestamp_provenance import load_and_verify_declaration


RAW_COLUMNS = (
    "maker", "taker", "maker_asset_id", "taker_asset_id",
    "maker_amount_filled", "taker_amount_filled", "block_number",
    "transaction_hash", "log_index", "exchange_address", "condition_id",
    "outcome_token_side",
)
IDENTITY = ("transaction_hash", "log_index", "exchange_address")
USDC_SCALE = 1_000_000.0
TOKEN_SCALE = 1_000_000.0


def build_exact_trades(
    sport: str,
    raw_trades_path: str | Path,
    eligible_path: str | Path,
    cache_path: str | Path,
    timestamp_declaration_path: str | Path,
    adapter_provenance_path: str | Path,
    phase_contract_path: str | Path,
    wallet_flags_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    raw, eligible, cache, declaration, adapter_provenance, phase_contract, flags = map(
        resolved,
        (raw_trades_path, eligible_path, cache_path, timestamp_declaration_path,
         adapter_provenance_path, phase_contract_path, wallet_flags_path),
    )
    load_and_verify_declaration(
        declaration, sport, raw, eligible, cache, adapter_provenance, phase_contract
    )
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW raw_input AS SELECT * FROM read_parquet('{quoted(raw)}')")
        con.execute(f"CREATE VIEW eligible_input AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        con.execute(f"CREATE VIEW cache_input AS SELECT * FROM read_parquet('{quoted(cache)}')")
        con.execute(f"CREATE VIEW flag_input AS SELECT * FROM read_parquet('{quoted(flags)}')")
        raw_schema = require_columns(con, "raw_input", RAW_COLUMNS, "Resolved EVM fills")
        require_exact_schema(con, "eligible_input", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff")
        flag_schema = require_columns(
            con, "flag_input", {"proxyWallet", "is_nonhuman"}, "Wallet flags"
        )
        for field in ("maker_amount_filled", "taker_amount_filled", "block_number", "log_index"):
            if raw_schema[field] not in INTEGER_TYPES:
                raise ArtifactError(f"Resolved EVM {field} must be integer typed")
        if flag_schema["is_nonhuman"] != "BOOLEAN":
            raise ArtifactError("Wallet is_nonhuman must be BOOLEAN")
        eligible_stats = con.execute(
            """SELECT count(*),count(DISTINCT market_id),count(DISTINCT game_id),
                      count(DISTINCT sport),min(sport),
                      count(*) FILTER (WHERE market_id IS NULL OR trim(market_id)=''
                        OR game_id IS NULL OR trim(game_id)='')
               FROM eligible_input"""
        ).fetchone()
        if eligible_stats[0] == 0 or eligible_stats[0] != eligible_stats[1] or eligible_stats[0] != eligible_stats[2]:
            raise ArtifactError("Eligible handoff must be nonempty and one-to-one by market/game")
        if eligible_stats[3] != 1 or eligible_stats[4] != sport or eligible_stats[5]:
            raise ArtifactError("Eligible handoff sport/identity values are invalid")

        columns = ",".join(f"raw.{name}" for name in RAW_COLUMNS)
        con.execute(
            f"""CREATE TABLE scoped_source AS SELECT {columns}
                FROM raw_input raw JOIN eligible_input e ON raw.condition_id=e.market_id"""
        )
        missing_market = con.execute(
            """SELECT count(*) FROM eligible_input e ANTI JOIN
                 (SELECT DISTINCT condition_id market_id FROM scoped_source) r USING(market_id)"""
        ).fetchone()[0]
        nulls = con.execute(
            "SELECT count(*) FROM scoped_source WHERE "
            + " OR ".join(f"{name} IS NULL" for name in RAW_COLUMNS)
        ).fetchone()[0]
        bad_economics = con.execute(
            """SELECT count(*) FROM scoped_source
               WHERE outcome_token_side NOT IN ('maker','taker')
                  OR maker_amount_filled<=0 OR taker_amount_filled<=0
                  OR trim(transaction_hash)='' OR trim(exchange_address)=''
                  OR trim(condition_id)=''"""
        ).fetchone()[0]
        if missing_market or nulls or bad_economics:
            raise ArtifactError(
                f"Invalid scoped fills: markets_without_rows={missing_market}, nulls={nulls}, bad={bad_economics}"
            )
        con.execute(f"CREATE TABLE unique_payloads AS SELECT DISTINCT {','.join(RAW_COLUMNS)} FROM scoped_source")
        contradictions = con.execute(
            f"""SELECT {','.join(IDENTITY)},count(*) FROM unique_payloads
                GROUP BY {','.join(IDENTITY)} HAVING count(*)<>1 LIMIT 10"""
        ).fetchall()
        if contradictions:
            raise ArtifactError(f"Contradictory immutable EVM identities: {contradictions}")
        con.execute("CREATE TABLE fills AS SELECT * FROM unique_payloads")
        cache_missing = con.execute(
            "SELECT count(*) FROM fills ANTI JOIN cache_input USING(block_number)"
        ).fetchone()[0]
        cache_mismatch = con.execute(
            """SELECT count(*) FROM (SELECT block_number,count(*) n FROM cache_input GROUP BY 1)
               WHERE n<>1"""
        ).fetchone()[0]
        if cache_missing or cache_mismatch:
            raise ArtifactError("Exact block cache has missing or duplicate fill coverage")
        flag_conflicts = con.execute(
            """SELECT proxyWallet FROM flag_input WHERE proxyWallet IS NULL OR trim(proxyWallet)=''
                  OR is_nonhuman IS NULL
               UNION ALL
               SELECT proxyWallet FROM flag_input GROUP BY proxyWallet
               HAVING count(DISTINCT is_nonhuman)<>1 LIMIT 10"""
        ).fetchall()
        if flag_conflicts:
            raise ArtifactError(f"Wallet flag mapping is invalid: {flag_conflicts}")
        con.execute(
            """CREATE TABLE wallet_flags AS
               SELECT proxyWallet,max(is_nonhuman::INTEGER)::BOOLEAN is_nonhuman
               FROM flag_input GROUP BY proxyWallet"""
        )
        con.execute(
            f"""CREATE TABLE output_rows AS
               SELECT '{sport}'::VARCHAR sport,condition_id::VARCHAR market_id,
                 CASE WHEN outcome_token_side='maker' THEN maker_asset_id ELSE taker_asset_id END::VARCHAR token_id,
                 block_number::BIGINT block_number,cache.timestamp::BIGINT AS "timestamp",
                 transaction_hash::VARCHAR transaction_hash,log_index::INTEGER log_index,
                 exchange_address::VARCHAR exchange_address,
                 CASE WHEN outcome_token_side='maker' THEN taker ELSE maker END::VARCHAR proxyWallet,
                 CASE WHEN outcome_token_side='maker' THEN maker ELSE taker END::VARCHAR counterparty,
                 (outcome_token_side='taker')::BOOLEAN is_maker,
                 coalesce(flag.is_nonhuman,false)::BOOLEAN buyer_is_flagged_nonhuman,
                 CASE WHEN outcome_token_side='maker'
                   THEN (taker_amount_filled/{USDC_SCALE})/(maker_amount_filled/{TOKEN_SCALE})
                   ELSE (maker_amount_filled/{USDC_SCALE})/(taker_amount_filled/{TOKEN_SCALE}) END::DOUBLE price,
                 CASE WHEN outcome_token_side='maker' THEN taker_amount_filled/{USDC_SCALE}
                   ELSE maker_amount_filled/{USDC_SCALE} END::DOUBLE usdc
               FROM fills JOIN cache_input cache USING(block_number)
               LEFT JOIN wallet_flags flag ON flag.proxyWallet=
                 CASE WHEN outcome_token_side='maker' THEN taker ELSE maker END"""
        )
        invalid = con.execute(
            """SELECT count(*) FROM output_rows WHERE token_id IS NULL OR trim(token_id)=''
               OR proxyWallet IS NULL OR trim(proxyWallet)='' OR NOT isfinite(price)
               OR NOT isfinite(usdc) OR usdc<=0"""
        ).fetchone()[0]
        if invalid:
            raise ArtifactError(f"Expanded BUY rows contain {invalid} invalid rows")
        raw_rows = int(con.execute("SELECT count(*) FROM scoped_source").fetchone()[0])
        fill_rows = int(con.execute("SELECT count(*) FROM fills").fetchone()[0])
        output_rows = int(con.execute("SELECT count(*) FROM output_rows").fetchone()[0])
        if output_rows != fill_rows:
            raise ArtifactError("One-BUY-row-per-distinct-fill reconciliation failed")
        rows = con.execute(
            "SELECT * FROM output_rows ORDER BY timestamp,block_number,log_index,transaction_hash,exchange_address"
        ).fetchall()
    finally:
        con.close()

    inputs = (raw, eligible, cache, declaration, adapter_provenance, phase_contract, flags)
    with fresh_run(run_dir, inputs) as staging:
        from .artifacts import write_parquet
        write_parquet(
            staging / "exact_trades.parquet", EXACT_TRADE_SCHEMA, rows,
            ("timestamp", "block_number", "log_index", "transaction_hash", "exchange_address"),
        )
        summary = {
            "schema_version": 1, "sport": sport,
            "method": "evm_identity_replay_dedup_exact_buy_expansion_v1",
            "counts": {"raw_scoped_rows": raw_rows, "distinct_fills": fill_rows,
                       "duplicate_replays": raw_rows-fill_rows, "output_buy_rows": output_rows},
            "bot_semantics": "flag applies only to outcome-token buyer proxyWallet",
            "inputs": {"raw_trades": fingerprint(raw), "eligible_moneylines": fingerprint(eligible),
                       "exact_cache": fingerprint(cache), "timestamp_declaration": fingerprint(declaration),
                       "adapter_provenance": fingerprint(adapter_provenance),
                       "phase_contract": fingerprint(phase_contract),
                       "wallet_flags": fingerprint(flags)},
            "outputs": {"exact_trades": artifact_fingerprint(staging / "exact_trades.parquet")},
        }
        write_json(staging / "build_audit.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "raw_trades", "eligible", "cache", "timestamp_declaration",
                 "adapter_provenance", "phase_contract", "wallet_flags", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(build_exact_trades(args.sport, args.raw_trades, args.eligible, args.cache,
                             args.timestamp_declaration, args.adapter_provenance,
                             args.phase_contract, args.wallet_flags, args.run_dir))


if __name__ == "__main__":
    main()
