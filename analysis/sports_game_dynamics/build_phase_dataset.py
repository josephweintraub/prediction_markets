"""Assign exact non-bot BUY fills to frozen quarter phases."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, fresh_run, matches_artifact_fingerprint,
    quoted, require_exact_schema, require_sport, resolved, write_json, write_parquet,
)
from .phase_contract import classify_timestamp, load_phase_contract, phase_contract_fingerprint
from .schemas import ELIGIBLE_SCHEMA, EXACT_TRADE_SCHEMA, PHASE_TRADE_SCHEMA


BOUNDARIES = (
    "actual_start_utc", "period_2_start_utc", "period_3_start_utc",
    "period_4_start_utc", "actual_end_utc",
)


def _rows(con: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM {relation}")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def build_phase_dataset(
    sport: str,
    eligible_path: str | Path,
    exact_trades_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    eligible, trades, contract_path = map(
        resolved, (eligible_path, exact_trades_path, phase_contract_path)
    )
    contract = load_phase_contract(contract_path)
    if contract.sport != sport:
        raise ArtifactError(f"Phase contract sport {contract.sport!r} does not match {sport!r}")
    exact_audit_path = trades.parent / "build_audit.json"
    exact_audit = json.loads(exact_audit_path.read_text(encoding="utf-8"))
    if (exact_audit.get("sport") != sport
            or not matches_artifact_fingerprint(exact_audit.get("outputs", {}).get("exact_trades"), trades)
            or exact_audit.get("inputs", {}).get("eligible_moneylines") != fingerprint(eligible)
            or exact_audit.get("inputs", {}).get("phase_contract") != fingerprint(contract_path)):
        raise ArtifactError("Exact-trade audit does not match phase inputs")
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW eligible_input AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        con.execute(f"CREATE VIEW trade_input AS SELECT * FROM read_parquet('{quoted(trades)}')")
        require_exact_schema(con, "eligible_input", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff")
        require_exact_schema(con, "trade_input", EXACT_TRADE_SCHEMA, "Exact BUY fills")
        dimensions = _rows(con, "eligible_input")
        source_trades = _rows(con, "trade_input")
    finally:
        con.close()
    if not dimensions or not source_trades:
        raise ArtifactError("Phase build inputs must be nonempty")
    by_market: dict[str, dict[str, Any]] = {}
    games: set[str] = set()
    for row in dimensions:
        if row["sport"] != sport or row["market_id"] in by_market or row["game_id"] in games:
            raise ArtifactError("Eligible dimensions must match sport and be one-to-one")
        if (
            not row["away_token_id"] or not row["home_token_id"]
            or row["away_token_id"] == row["home_token_id"]
            or row["winning_token_id"] not in (row["away_token_id"], row["home_token_id"])
        ):
            raise ArtifactError(f"Invalid eligible token dimension: {row['market_id']}")
        winner_expected = (
            row["home_team_id"] if row["winning_token_id"] == row["home_token_id"]
            else row["away_team_id"]
        )
        if row["winning_team_id"] != winner_expected:
            raise ArtifactError(f"Winning team/token mismatch: {row['market_id']}")
        boundary_values = [row[name] for name in BOUNDARIES]
        if any(value is None for value in boundary_values) or not all(
            left < right for left, right in zip(boundary_values, boundary_values[1:])
        ):
            raise ArtifactError(f"Invalid timing boundaries: {row['market_id']}")
        by_market[row["market_id"]] = row
        games.add(row["game_id"])

    output: list[tuple[Any, ...]] = []
    seen: set[tuple[str, int, str]] = set()
    excluded_bots = excluded_prices = post_final = 0
    for trade in source_trades:
        if trade["sport"] != sport or trade["market_id"] not in by_market:
            raise ArtifactError("Exact trade has wrong sport or an unknown market")
        identity = (trade["transaction_hash"], trade["log_index"], trade["exchange_address"])
        if identity in seen:
            raise ArtifactError(f"Duplicate exact EVM identity: {identity}")
        seen.add(identity)
        if trade["buyer_is_flagged_nonhuman"]:
            excluded_bots += 1
            continue
        if not (0.01 < trade["price"] < 0.99):
            excluded_prices += 1
            continue
        dimension = by_market[trade["market_id"]]
        if trade["token_id"] not in (dimension["away_token_id"], dimension["home_token_id"]):
            raise ArtifactError(f"Trade token is outside eligible game: {identity}")
        timestamp = datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc)
        boundaries = {name: dimension[name] for name in BOUNDARIES}
        phase = classify_timestamp(contract, boundaries, timestamp)
        analysis_eligible = phase != "post_final"
        if not analysis_eligible:
            post_final += 1
        near = any(abs((timestamp-boundary).total_seconds()) <= 30 for boundary in boundaries.values())
        home_won = dimension["winning_token_id"] == dimension["home_token_id"]
        home_probability = (
            trade["price"] if trade["token_id"] == dimension["home_token_id"]
            else 1.0-trade["price"]
        )
        extra = (
            dimension["game_id"], dimension["official_date"], dimension["winning_token_id"],
            dimension["home_token_id"], home_won, home_probability,
            float(home_won)-home_probability, timestamp.date(), phase, analysis_eligible, near,
            *(dimension[name] for name in BOUNDARIES),
        )
        output.append(tuple(trade[name] for name, _ in EXACT_TRADE_SCHEMA) + extra)

    if not output:
        raise ArtifactError("No exact fills survived phase price/buyer gates")
    inputs = (eligible, trades, contract_path)
    with fresh_run(run_dir, inputs) as staging:
        write_parquet(
            staging / "phase_trades.parquet", PHASE_TRADE_SCHEMA, output,
            ("timestamp", "block_number", "log_index", "transaction_hash", "exchange_address"),
        )
        summary = {
            "schema_version": 1, "sport": sport, "method": "exact_quarter_phase_assignment_v1",
            "filters": {"price": "0.01 < price < 0.99",
                        "bots": "exclude flagged outcome-token buyer only",
                        "boundary_sensitivity": "exclude abs(t-boundary) <= 30 seconds; inclusive"},
            "counts": {"exact_input_rows": len(source_trades), "phase_rows": len(output),
                       "buyer_bot_exclusions": excluded_bots, "price_exclusions": excluded_prices,
                       "post_final_audit_rows": post_final},
            "inputs": {"eligible_moneylines": fingerprint(eligible),
                       "exact_trades": fingerprint(trades),
                       "phase_contract": phase_contract_fingerprint(contract_path),
                       "timestamp_declaration": exact_audit["inputs"]["timestamp_declaration"],
                       "adapter_provenance": exact_audit["inputs"]["adapter_provenance"]},
            "outputs": {"phase_trades": artifact_fingerprint(staging / "phase_trades.parquet")},
        }
        write_json(staging / "phase_summary.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "eligible", "exact_trades", "phase_contract", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(build_phase_dataset(args.sport, args.eligible, args.exact_trades,
                              args.phase_contract, args.run_dir))


if __name__ == "__main__":
    main()
