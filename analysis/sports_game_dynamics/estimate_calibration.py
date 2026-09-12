"""Estimate frozen equal-game closing and equal-fill phase calibration profiles."""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb

from .artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, fresh_run, matches_artifact_fingerprint,
    quoted, require_exact_schema, require_sport, resolved, write_json, write_parquet,
)
from .build_dual_closes import CLOSE_SCHEMA
from .fixed_bins import MIN_CELL_N, fixed_bin_grid, price_bin_label, price_decile
from .phase_contract import classify_timestamp, load_phase_contract, phase_contract_fingerprint
from .schemas import PHASE_TRADE_SCHEMA


CLOSING_SCHEMA = (
    ("close_definition", "VARCHAR"), ("profile_scope", "VARCHAR"),
    ("price_decile", "INTEGER"), ("price_bin", "VARCHAR"), ("game_count", "BIGINT"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
    ("mean_probability", "DOUBLE"), ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"), ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"), ("calibration_ci95_high", "DOUBLE"),
    ("brier_score", "DOUBLE"),
)
PAIRED_SCHEMA = (
    ("profile_scope", "VARCHAR"), ("primary_price_decile", "INTEGER"),
    ("game_count", "BIGINT"), ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
    ("mean_primary_probability", "DOUBLE"), ("mean_sensitivity_probability", "DOUBLE"),
    ("mean_probability_difference_a_minus_c", "DOUBLE"),
    ("mean_calibration_difference_a_minus_c", "DOUBLE"),
    ("mean_brier_difference_a_minus_c", "DOUBLE"),
    ("probability_difference_se", "DOUBLE"),
    ("probability_difference_ci95_low", "DOUBLE"),
    ("probability_difference_ci95_high", "DOUBLE"),
)
PHASE_PROFILE_SCHEMA = (
    ("boundary_sample", "VARCHAR"), ("phase", "VARCHAR"), ("phase_order", "INTEGER"),
    ("price_decile", "INTEGER"), ("price_bin", "VARCHAR"),
    ("trade_count", "BIGINT"), ("game_count", "BIGINT"), ("dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
    ("mean_price", "DOUBLE"), ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"), ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"), ("calibration_ci95_high", "DOUBLE"),
)


def _read(con: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM {relation}")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values)


def _oneway_se(rows: list[dict[str, Any]], value: Callable[[dict[str, Any]], float], cluster: str) -> float:
    center = _mean(value(row) for row in rows)
    scores: dict[Any, float] = defaultdict(float)
    for row in rows:
        scores[row[cluster]] += value(row)-center
    return math.sqrt(sum(score*score for score in scores.values()))/len(rows)


def _cgm_se(rows: list[dict[str, Any]], value: Callable[[dict[str, Any]], float]) -> float:
    center = _mean(value(row) for row in rows)
    dimensions = (("trade_day",), ("proxyWallet",), ("game_id",),
                  ("trade_day", "proxyWallet"), ("trade_day", "game_id"),
                  ("proxyWallet", "game_id"), ("trade_day", "proxyWallet", "game_id"))
    signs = (1, 1, 1, -1, -1, -1, 1)
    variance = 0.0
    for fields, sign in zip(dimensions, signs):
        scores: dict[Any, float] = defaultdict(float)
        for row in rows:
            scores[tuple(row[field] for field in fields)] += value(row)-center
        variance += sign*sum(score*score for score in scores.values())
    return math.sqrt(max(variance, 0.0))/len(rows)


def _reported(n: int, values: dict[str, float | None]) -> tuple[bool, str, dict[str, float | None]]:
    suppressed = n < MIN_CELL_N
    return suppressed, ("suppressed_n_lt_50" if suppressed else "reported"), {
        key: None if suppressed else value for key, value in values.items()
    }


def estimate_calibration(
    sport: str,
    game_closes_path: str | Path,
    phase_trades_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    closes, phases, contract_path = map(
        resolved, (game_closes_path, phase_trades_path, phase_contract_path)
    )
    contract = load_phase_contract(contract_path)
    if contract.sport != sport:
        raise ArtifactError("Calibration sport does not match phase contract")
    close_summary = json.loads((closes.parent/"close_summary.json").read_text(encoding="utf-8"))
    phase_summary = json.loads((phases.parent/"phase_summary.json").read_text(encoding="utf-8"))
    lineage_keys = ("timestamp_declaration", "adapter_provenance", "phase_contract")
    if (close_summary.get("sport") != sport
            or phase_summary.get("sport") != sport
            or not matches_artifact_fingerprint(close_summary.get("outputs", {}).get("game_closes"), closes)
            or not matches_artifact_fingerprint(phase_summary.get("outputs", {}).get("phase_trades"), phases)
            or phase_summary.get("inputs", {}).get("phase_contract") != phase_contract_fingerprint(contract_path)
            or any(close_summary.get("inputs", {}).get(key) != phase_summary.get("inputs", {}).get(key)
                   for key in lineage_keys)):
        raise ArtifactError("Stage-06/07 lineage does not match calibration inputs")
    grid = fixed_bin_grid(contract)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW closes AS SELECT * FROM read_parquet('{quoted(closes)}')")
        con.execute(f"CREATE VIEW phases AS SELECT * FROM read_parquet('{quoted(phases)}')")
        require_exact_schema(con, "closes", CLOSE_SCHEMA, "Dual closes")
        require_exact_schema(con, "phases", PHASE_TRADE_SCHEMA, "Phase fills")
        close_rows, phase_rows = _read(con, "closes"), _read(con, "phases")
    finally:
        con.close()
    if not close_rows or not phase_rows:
        raise ArtifactError("Calibration inputs must be nonempty")
    if any(row["sport"] != sport for row in close_rows+phase_rows):
        raise ArtifactError("Calibration inputs contain the wrong sport")
    if len({row["market_id"] for row in close_rows}) != len(close_rows) or len({row["game_id"] for row in close_rows}) != len(close_rows):
        raise ArtifactError("Dual closes must be one-to-one by market/game")
    close_by_dimension = {(row["market_id"], row["game_id"]): row for row in close_rows}
    for row in close_rows:
        start = row["actual_start_utc"]
        for definition in ("primary", "sensitivity"):
            present = row[f"{definition}_has_close"]
            fields = [row[f"{definition}_{name}"] for name in (
                "close_timestamp", "block_number", "transaction_hash", "log_index",
                "exchange_address", "home_probability", "usdc")]
            if present != all(value is not None for value in fields):
                raise ArtifactError("Close presence flag does not match its payload")
            if present:
                probability = row[f"{definition}_home_probability"]
                if (datetime.fromtimestamp(row[f"{definition}_close_timestamp"], tz=timezone.utc) >= start
                        or not math.isfinite(probability) or not 0 < probability < 1
                        or not 0 < row[f"{definition}_usdc"] < math.inf
                        or row[f"{definition}_missing_reason"] is not None):
                    raise ArtifactError("Invalid pregame close payload")
            elif row[f"{definition}_missing_reason"] != "no_eligible_pregame_fill":
                raise ArtifactError("Missing close requires the frozen missing reason")
        if row["sensitivity_has_close"]:
            if (not row["primary_has_close"]
                    or not 0.01 < row["sensitivity_home_probability"] < 0.99
                    or row["sensitivity_close_timestamp"] > row["primary_close_timestamp"]):
                raise ArtifactError("Sensitivity close must be a valid subset of primary A")
    phase_games = {(row["market_id"], row["game_id"]) for row in phase_rows}
    close_games = {(row["market_id"], row["game_id"]) for row in close_rows}
    if not phase_games <= close_games:
        raise ArtifactError("Phase games must be a subset of dual-close dimensions")
    identities = [(row["transaction_hash"], row["log_index"], row["exchange_address"]) for row in phase_rows]
    if len(identities) != len(set(identities)):
        raise ArtifactError("Phase input contains duplicate EVM identities")
    boundary_names = ("actual_start_utc", "period_2_start_utc", "period_3_start_utc",
                      "period_4_start_utc", "actual_end_utc")
    for row in phase_rows:
        close = close_by_dimension[(row["market_id"], row["game_id"])]
        if (row["official_date"] != close["official_date"]
                or row["home_won"] != close["home_won"]
                or row["actual_start_utc"] != close["actual_start_utc"]
                or row["buyer_is_flagged_nonhuman"]
                or not 0.01 < row["price"] < 0.99):
            raise ArtifactError("Phase/close dimensions or frozen phase filters disagree")
        expected_probability = row["price"] if row["token_id"] == row["home_token_id"] else 1-row["price"]
        if (row["token_id"] not in (row["home_token_id"],)
                and abs(expected_probability-row["home_probability"]) > 1e-12):
            raise ArtifactError("Phase home normalization is inconsistent")
        if row["token_id"] == row["home_token_id"] and abs(row["price"]-row["home_probability"]) > 1e-12:
            raise ArtifactError("Phase home normalization is inconsistent")
        if abs(float(row["home_won"])-row["home_probability"]-row["calibration_error"]) > 1e-12:
            raise ArtifactError("Phase calibration error is inconsistent")
        timestamp = datetime.fromtimestamp(row["timestamp"], tz=timezone.utc)
        boundaries = {name: row[name] for name in boundary_names}
        expected_phase = classify_timestamp(contract, boundaries, timestamp)
        expected_near = any(abs((timestamp-boundary).total_seconds()) <= 30 for boundary in boundaries.values())
        if (row["phase"] != expected_phase or row["analysis_eligible"] != (expected_phase != "post_final")
                or row["exclude_within_30s"] != expected_near):
            raise ArtifactError("Serialized phase/boundary assignment is inconsistent")

    close_obs: list[dict[str, Any]] = []
    for row in close_rows:
        for definition in ("primary", "sensitivity"):
            if not row[f"{definition}_has_close"]:
                continue
            probability = row[f"{definition}_home_probability"]
            won = float(row["home_won"])
            close_obs.append({"definition": definition, "market_id": row["market_id"],
                              "game_id": row["game_id"], "official_date": row["official_date"],
                              "probability": probability, "won": won,
                              "error": won-probability, "decile": price_decile(probability)})
    closing_output: list[tuple[Any, ...]] = []
    for definition, scope, decile in grid.closing_profile:
        rows = [row for row in close_obs if row["definition"] == definition
                and (scope == "overall" or row["decile"] == decile)]
        n = len(rows)
        values: dict[str, float | None] = {key: None for key in (
            "mean_probability", "win_rate", "mean_calibration", "calibration_se",
            "calibration_ci95_low", "calibration_ci95_high", "brier_score")}
        if rows:
            mean_error = _mean(row["error"] for row in rows)
            se = _oneway_se(rows, lambda row: row["error"], "official_date")
            values = {"mean_probability": _mean(row["probability"] for row in rows),
                      "win_rate": _mean(row["won"] for row in rows),
                      "mean_calibration": mean_error, "calibration_se": se,
                      "calibration_ci95_low": mean_error-1.96*se,
                      "calibration_ci95_high": mean_error+1.96*se,
                      "brier_score": _mean(row["error"]**2 for row in rows)}
        suppressed, status, values = _reported(n, values)
        closing_output.append((definition, scope, decile,
                               "overall" if decile is None else price_bin_label(decile),
                               n, suppressed, status, *values.values()))

    paired_output: list[tuple[Any, ...]] = []
    paired_rows: list[dict[str, Any]] = []
    by_market_definition = {(row["market_id"], row["definition"]): row for row in close_obs}
    for row in close_rows:
        a = by_market_definition.get((row["market_id"], "primary"))
        c = by_market_definition.get((row["market_id"], "sensitivity"))
        if a and c:
            paired_rows.append({"official_date": row["official_date"], "primary_decile": a["decile"],
                                "a": a["probability"], "c": c["probability"],
                                "prob_diff": a["probability"]-c["probability"],
                                "cal_diff": a["error"]-c["error"],
                                "brier_diff": a["error"]**2-c["error"]**2})
    for scope, decile in grid.paired_profile:
        rows = [row for row in paired_rows if scope == "overall" or row["primary_decile"] == decile]
        n = len(rows)
        values: dict[str, float | None] = {key: None for key in (
            "a", "c", "prob_diff", "cal_diff", "brier_diff", "se", "low", "high")}
        if rows:
            diff = _mean(row["prob_diff"] for row in rows)
            se = _oneway_se(rows, lambda row: row["prob_diff"], "official_date")
            values = {"a": _mean(row["a"] for row in rows), "c": _mean(row["c"] for row in rows),
                      "prob_diff": diff, "cal_diff": _mean(row["cal_diff"] for row in rows),
                      "brier_diff": _mean(row["brier_diff"] for row in rows),
                      "se": se, "low": diff-1.96*se, "high": diff+1.96*se}
        suppressed, status, values = _reported(n, values)
        paired_output.append((scope, decile, n, suppressed, status, *values.values()))

    phase_obs: list[dict[str, Any]] = []
    for row in phase_rows:
        if not row["analysis_eligible"]:
            continue
        for sample in ("literal", "exclude_within_30s"):
            if sample == "exclude_within_30s" and row["exclude_within_30s"]:
                continue
            phase_obs.append({**row, "sample": sample,
                              "decile": price_decile(row["home_probability"])})
    phase_output: list[tuple[Any, ...]] = []
    phase_order = {phase.key: index+1 for index, phase in enumerate(contract.analysis_phases)}
    for sample, phase, decile in grid.trade_phase_profile:
        rows = [row for row in phase_obs if row["sample"] == sample
                and row["phase"] == phase and row["decile"] == decile]
        n = len(rows)
        values: dict[str, float | None] = {key: None for key in (
            "mean_price", "win_rate", "mean_calibration", "calibration_se", "low", "high")}
        if rows:
            mean_error = _mean(row["calibration_error"] for row in rows)
            se = _cgm_se(rows, lambda row: row["calibration_error"])
            values = {"mean_price": _mean(row["home_probability"] for row in rows),
                      "win_rate": _mean(float(row["home_won"]) for row in rows),
                      "mean_calibration": mean_error, "calibration_se": se,
                      "low": mean_error-1.96*se, "high": mean_error+1.96*se}
        suppressed, status, values = _reported(n, values)
        phase_output.append((sample, phase, phase_order[phase], decile, price_bin_label(decile),
                             n, len({row["game_id"] for row in rows}),
                             sum(row["usdc"] for row in rows), suppressed, status, *values.values()))

    inputs = (closes, phases, contract_path)
    with fresh_run(run_dir, inputs) as staging:
        write_parquet(staging/"closing_calibration.parquet", CLOSING_SCHEMA, closing_output,
                      ("close_definition", "profile_scope", "price_decile"))
        write_parquet(staging/"closing_paired_sensitivity.parquet", PAIRED_SCHEMA, paired_output,
                      ("profile_scope", "primary_price_decile"))
        write_parquet(staging/"trade_phase_calibration.parquet", PHASE_PROFILE_SCHEMA, phase_output,
                      ("boundary_sample", "phase_order", "price_decile"))
        summary = {
            "schema_version": 1, "sport": sport, "method": "fixed_decile_calibration_v1",
            "interpretation_status": "exploratory_descriptive",
            "definitions": {"calibration_error": "eventual home outcome - home probability",
                            "closing_weighting": "equal game",
                            "phase_weighting": "equal BUY fill; dollars descriptive only",
                            "closing_uncertainty": "official-date clustered normal SE",
                            "phase_uncertainty": "CGM day x buyer wallet x game clustered normal SE",
                            "suppression": "withhold estimates when n < 50"},
            "counts": grid.row_counts,
            "inputs": {"game_closes": fingerprint(closes), "phase_trades": fingerprint(phases),
                       "phase_contract": phase_contract_fingerprint(contract_path),
                       "timestamp_declaration": phase_summary["inputs"]["timestamp_declaration"],
                       "adapter_provenance": phase_summary["inputs"]["adapter_provenance"]},
            "outputs": {
                "closing_calibration": artifact_fingerprint(staging/"closing_calibration.parquet"),
                "closing_paired_sensitivity": artifact_fingerprint(staging/"closing_paired_sensitivity.parquet"),
                "trade_phase_calibration": artifact_fingerprint(staging/"trade_phase_calibration.parquet"),
            },
        }
        write_json(staging/"estimator_summary.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "game_closes", "phase_trades", "phase_contract", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    print(estimate_calibration(args.sport, args.game_closes, args.phase_trades,
                               args.phase_contract, args.run_dir))
