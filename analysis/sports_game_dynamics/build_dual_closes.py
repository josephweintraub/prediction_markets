"""Build one primary A and buyer-filtered sensitivity C close per eligible game."""
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
from .schemas import ELIGIBLE_SCHEMA, EXACT_TRADE_SCHEMA


CLOSE_SCHEMA = (
    ("sport", "VARCHAR"), ("market_id", "VARCHAR"), ("game_id", "VARCHAR"),
    ("official_date", "DATE"), ("actual_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("home_won", "BOOLEAN"),
    ("primary_has_close", "BOOLEAN"), ("primary_missing_reason", "VARCHAR"),
    ("primary_close_timestamp", "BIGINT"), ("primary_block_number", "BIGINT"),
    ("primary_transaction_hash", "VARCHAR"), ("primary_log_index", "INTEGER"),
    ("primary_exchange_address", "VARCHAR"), ("primary_home_probability", "DOUBLE"),
    ("primary_usdc", "DOUBLE"), ("primary_buyer_is_flagged_nonhuman", "BOOLEAN"),
    ("primary_inside_001_099", "BOOLEAN"),
    ("sensitivity_has_close", "BOOLEAN"), ("sensitivity_missing_reason", "VARCHAR"),
    ("sensitivity_close_timestamp", "BIGINT"), ("sensitivity_block_number", "BIGINT"),
    ("sensitivity_transaction_hash", "VARCHAR"), ("sensitivity_log_index", "INTEGER"),
    ("sensitivity_exchange_address", "VARCHAR"), ("sensitivity_home_probability", "DOUBLE"),
    ("sensitivity_usdc", "DOUBLE"),
)


def _dict_rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _close_values(prefix: str, trade: dict[str, Any] | None) -> dict[str, Any]:
    if trade is None:
        return {f"{prefix}_has_close": False, f"{prefix}_missing_reason": "no_eligible_pregame_fill",
                f"{prefix}_close_timestamp": None, f"{prefix}_block_number": None,
                f"{prefix}_transaction_hash": None, f"{prefix}_log_index": None,
                f"{prefix}_exchange_address": None, f"{prefix}_home_probability": None,
                f"{prefix}_usdc": None,
                **({f"{prefix}_buyer_is_flagged_nonhuman": None,
                    f"{prefix}_inside_001_099": None} if prefix == "primary" else {})}
    values = {f"{prefix}_has_close": True, f"{prefix}_missing_reason": None,
              f"{prefix}_close_timestamp": trade["timestamp"],
              f"{prefix}_block_number": trade["block_number"],
              f"{prefix}_transaction_hash": trade["transaction_hash"],
              f"{prefix}_log_index": trade["log_index"],
              f"{prefix}_exchange_address": trade["exchange_address"],
              f"{prefix}_home_probability": trade["home_probability"],
              f"{prefix}_usdc": trade["usdc"]}
    if prefix == "primary":
        values[f"{prefix}_buyer_is_flagged_nonhuman"] = trade["buyer_is_flagged_nonhuman"]
        values[f"{prefix}_inside_001_099"] = 0.01 < trade["price"] < 0.99
    return values


def build_dual_closes(
    sport: str,
    eligible_path: str | Path,
    exact_trades_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    eligible, trades = map(resolved, (eligible_path, exact_trades_path))
    exact_audit = json.loads((trades.parent/"build_audit.json").read_text(encoding="utf-8"))
    if (exact_audit.get("sport") != sport
            or not matches_artifact_fingerprint(exact_audit.get("outputs", {}).get("exact_trades"), trades)
            or exact_audit.get("inputs", {}).get("eligible_moneylines") != fingerprint(eligible)):
        raise ArtifactError("Exact-trade audit does not match close inputs")
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW eligible_input AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        con.execute(f"CREATE VIEW trade_input AS SELECT * FROM read_parquet('{quoted(trades)}')")
        require_exact_schema(con, "eligible_input", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff")
        require_exact_schema(con, "trade_input", EXACT_TRADE_SCHEMA, "Exact BUY fills")
        dimensions = _dict_rows(con, "SELECT * FROM eligible_input ORDER BY market_id")
        source = _dict_rows(
            con,
            """SELECT * FROM trade_input ORDER BY timestamp,block_number,log_index,
                      transaction_hash,exchange_address""",
        )
    finally:
        con.close()
    if not dimensions:
        raise ArtifactError("Eligible-moneyline handoff is empty")
    by_market: dict[str, list[dict[str, Any]]] = {row["market_id"]: [] for row in dimensions}
    if len(by_market) != len(dimensions):
        raise ArtifactError("Eligible closes contain duplicate market IDs")
    game_ids: set[str] = set()
    for row in dimensions:
        if row["sport"] != sport or row["game_id"] in game_ids:
            raise ArtifactError("Eligible closes must be one-to-one for the requested sport")
        if (row["away_token_id"] == row["home_token_id"]
                or row["winning_token_id"] not in (row["away_token_id"], row["home_token_id"])):
            raise ArtifactError("Eligible close token dimension is invalid")
        expected_winner = (row["home_team_id"] if row["winning_token_id"] == row["home_token_id"]
                           else row["away_team_id"])
        if row["winning_team_id"] != expected_winner:
            raise ArtifactError("Eligible close winner team/token mapping is invalid")
        game_ids.add(row["game_id"])
    seen: set[tuple[Any, ...]] = set()
    for trade in source:
        identity = tuple(trade[name] for name in ("transaction_hash", "log_index", "exchange_address"))
        if identity in seen:
            raise ArtifactError(f"Duplicate exact EVM identity: {identity}")
        seen.add(identity)
        if trade["sport"] != sport or trade["market_id"] not in by_market:
            raise ArtifactError("Exact close source has wrong sport or unknown market")
        if (not isinstance(trade["timestamp"], int) or isinstance(trade["timestamp"], bool)
                or not (float("-inf") < trade["price"] < float("inf"))
                or not (0 < trade["usdc"] < float("inf"))):
            raise ArtifactError("Exact close source has invalid timestamp/price/dollars")
        by_market[trade["market_id"]].append(trade)

    output: list[tuple[Any, ...]] = []
    primary_count = sensitivity_count = 0
    for dimension in dimensions:
        start = dimension["actual_start_utc"]
        candidates: list[dict[str, Any]] = []
        for trade in by_market[dimension["market_id"]]:
            if trade["token_id"] not in (dimension["away_token_id"], dimension["home_token_id"]):
                raise ArtifactError("Close source token does not belong to eligible game")
            if datetime.fromtimestamp(trade["timestamp"], tz=timezone.utc) >= start:
                continue
            home_probability = trade["price"] if trade["token_id"] == dimension["home_token_id"] else 1-trade["price"]
            candidates.append({**trade, "home_probability": home_probability})
        primary_rows = [row for row in candidates if 0 < row["price"] < 1]
        sensitivity_rows = [row for row in candidates
                            if not row["buyer_is_flagged_nonhuman"] and 0.01 < row["price"] < 0.99]
        primary = primary_rows[-1] if primary_rows else None
        sensitivity = sensitivity_rows[-1] if sensitivity_rows else None
        primary_count += primary is not None
        sensitivity_count += sensitivity is not None
        values = {
            "sport": sport, "market_id": dimension["market_id"], "game_id": dimension["game_id"],
            "official_date": dimension["official_date"], "actual_start_utc": dimension["actual_start_utc"],
            "home_won": dimension["winning_token_id"] == dimension["home_token_id"],
            **_close_values("primary", primary), **_close_values("sensitivity", sensitivity),
        }
        output.append(tuple(values[name] for name, _ in CLOSE_SCHEMA))
    inputs = (eligible, trades)
    with fresh_run(run_dir, inputs) as staging:
        write_parquet(staging / "game_closes.parquet", CLOSE_SCHEMA, output, ("market_id",))
        summary = {
            "schema_version": 1, "sport": sport, "method": "dual_pregame_closes_v1",
            "definitions": {
                "primary": "A: last exact pregame BUY fill with 0 < price < 1; flagged buyers included",
                "sensitivity": "C: last exact pregame BUY fill with 0.01 < price < 0.99; flagged outcome-token buyers excluded",
                "ordering": "timestamp, block_number, log_index, transaction_hash, exchange_address",
            },
            "counts": {"eligible_games": len(dimensions), "primary_closes": primary_count,
                       "sensitivity_closes": sensitivity_count},
            "inputs": {"eligible_moneylines": fingerprint(eligible), "exact_trades": fingerprint(trades)},
            "outputs": {"game_closes": artifact_fingerprint(staging / "game_closes.parquet")},
        }
        summary["inputs"]["timestamp_declaration"] = exact_audit["inputs"]["timestamp_declaration"]
        summary["inputs"]["adapter_provenance"] = exact_audit["inputs"]["adapter_provenance"]
        summary["inputs"]["phase_contract"] = exact_audit["inputs"]["phase_contract"]
        write_json(staging / "close_summary.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "eligible", "exact_trades", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    print(build_dual_closes(args.sport, args.eligible, args.exact_trades, args.run_dir))
