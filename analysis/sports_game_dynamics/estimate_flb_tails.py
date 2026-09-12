"""Summarize fixed D1/D10 FLB tails with joint clustered uncertainty."""
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from .artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, fresh_run, matches_artifact_fingerprint,
    quoted, require_exact_schema, require_sport, resolved, write_json, write_parquet,
)
from .build_dual_closes import CLOSE_SCHEMA
from .estimate_calibration import CLOSING_SCHEMA, PHASE_PROFILE_SCHEMA, _cgm_se, _oneway_se
from .fixed_bins import (
    MIN_CELL_N, fixed_bin_grid, price_bin_label, price_decile, require_complete_unique_grid,
)
from .phase_contract import load_phase_contract, phase_contract_fingerprint
from .schemas import PHASE_TRADE_SCHEMA


TAIL_SCHEMA = (
    ("analysis_scope", "VARCHAR"), ("close_definition", "VARCHAR"),
    ("boundary_sample", "VARCHAR"), ("phase", "VARCHAR"),
    ("d1_n", "BIGINT"), ("d1_games", "BIGINT"), ("d1_dollars", "DOUBLE"),
    ("d10_n", "BIGINT"), ("d10_games", "BIGINT"), ("d10_dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"), ("point_pattern", "VARCHAR"),
    ("d1_mean_probability", "DOUBLE"), ("d1_win_rate", "DOUBLE"),
    ("d1_mean_calibration", "DOUBLE"), ("d1_calibration_se", "DOUBLE"),
    ("d1_calibration_ci95_low", "DOUBLE"), ("d1_calibration_ci95_high", "DOUBLE"),
    ("d10_mean_probability", "DOUBLE"), ("d10_win_rate", "DOUBLE"),
    ("d10_mean_calibration", "DOUBLE"), ("d10_calibration_se", "DOUBLE"),
    ("d10_calibration_ci95_low", "DOUBLE"), ("d10_calibration_ci95_high", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"), ("spread_se", "DOUBLE"),
    ("spread_ci95_low", "DOUBLE"), ("spread_ci95_high", "DOUBLE"),
)


def _read(con: duckdb.DuckDBPyConnection, relation: str) -> list[dict[str, Any]]:
    cursor = con.execute(f"SELECT * FROM {relation}")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _joint_se(rows1: list[dict[str, Any]], rows10: list[dict[str, Any]], cluster_fields: tuple[str, ...],
              three_way: bool = False) -> float:
    mean1 = sum(row["error"] for row in rows1)/len(rows1)
    mean10 = sum(row["error"] for row in rows10)/len(rows10)
    observations = [(row, -(row["error"]-mean1)/len(rows1)) for row in rows1]
    observations += [(row, (row["error"]-mean10)/len(rows10)) for row in rows10]
    if not three_way:
        scores: dict[Any, float] = defaultdict(float)
        for row, score in observations:
            scores[tuple(row[field] for field in cluster_fields)] += score
        return math.sqrt(sum(score*score for score in scores.values()))
    dims = (("trade_day",), ("proxyWallet",), ("game_id",),
            ("trade_day", "proxyWallet"), ("trade_day", "game_id"),
            ("proxyWallet", "game_id"), ("trade_day", "proxyWallet", "game_id"))
    signs = (1, 1, 1, -1, -1, -1, 1)
    variance = 0.0
    for fields, sign in zip(dims, signs):
        scores: dict[Any, float] = defaultdict(float)
        for row, score in observations:
            scores[tuple(row[field] for field in fields)] += score
        variance += sign*sum(score*score for score in scores.values())
    return math.sqrt(max(variance, 0.0))


def _pattern(d1: float, d10: float) -> str:
    if d1 < 0 < d10:
        return "classic_flb_signs"
    if d10 < 0 < d1:
        return "reverse_flb_signs"
    if d1 > 0 and d10 > 0:
        return "both_positive"
    if d1 < 0 and d10 < 0:
        return "both_negative"
    return "mixed_or_zero"


def _same(observed: Any, expected: Any) -> bool:
    if observed is None or expected is None:
        return observed is expected
    if isinstance(expected, float):
        return isinstance(observed, (int, float)) and math.isclose(
            observed, expected, rel_tol=1e-12, abs_tol=1e-12
        )
    return observed == expected


def _assert_profile_row(row: dict[str, Any], expected: dict[str, Any], label: str) -> None:
    mismatches = {key: (row.get(key), value) for key, value in expected.items()
                  if not _same(row.get(key), value)}
    if mismatches:
        raise ArtifactError(f"{label} does not recompute from Stage-07 inputs: {mismatches}")


def _recompute_all_profiles(
    grid: Any,
    phase_order: dict[str, int],
    closing_by_key: dict[tuple[Any, ...], dict[str, Any]],
    profile_by_key: dict[tuple[Any, ...], dict[str, Any]],
    close_source: list[dict[str, Any]],
    phase_source: list[dict[str, Any]],
    sport: str,
) -> None:
    close_obs: list[dict[str, Any]] = []
    for row in close_source:
        if row["sport"] != sport:
            raise ArtifactError("Closing source contains the wrong sport")
        for definition in ("primary", "sensitivity"):
            if row[f"{definition}_has_close"]:
                probability = row[f"{definition}_home_probability"]
                won = float(row["home_won"])
                close_obs.append({"definition": definition, "official_date": row["official_date"],
                                  "probability": probability, "won": won,
                                  "error": won-probability, "decile": price_decile(probability)})
    for definition, scope, decile in grid.closing_profile:
        rows = [row for row in close_obs if row["definition"] == definition
                and (scope == "overall" or row["decile"] == decile)]
        n, suppressed = len(rows), len(rows) < MIN_CELL_N
        expected: dict[str, Any] = {
            "price_bin": "overall" if decile is None else price_bin_label(decile),
            "game_count": n, "suppressed": suppressed,
            "status": "suppressed_n_lt_50" if suppressed else "reported",
        }
        if suppressed:
            expected.update({key: None for key in (
                "mean_probability", "win_rate", "mean_calibration", "calibration_se",
                "calibration_ci95_low", "calibration_ci95_high", "brier_score")})
        else:
            error = sum(row["error"] for row in rows)/n
            se = _oneway_se(rows, lambda row: row["error"], "official_date")
            expected.update({"mean_probability": sum(row["probability"] for row in rows)/n,
                             "win_rate": sum(row["won"] for row in rows)/n,
                             "mean_calibration": error, "calibration_se": se,
                             "calibration_ci95_low": error-1.96*se,
                             "calibration_ci95_high": error+1.96*se,
                             "brier_score": sum(row["error"]**2 for row in rows)/n})
        _assert_profile_row(closing_by_key[(definition, scope, decile)], expected,
                            f"Closing profile {(definition, scope, decile)}")

    phase_obs: list[dict[str, Any]] = []
    for row in phase_source:
        if row["sport"] != sport:
            raise ArtifactError("Phase source contains the wrong sport")
        if not row["analysis_eligible"]:
            continue
        for sample in ("literal", "exclude_within_30s"):
            if sample == "exclude_within_30s" and row["exclude_within_30s"]:
                continue
            phase_obs.append({**row, "sample": sample,
                              "decile": price_decile(row["home_probability"])})
    for sample, phase, decile in grid.trade_phase_profile:
        rows = [row for row in phase_obs if row["sample"] == sample
                and row["phase"] == phase and row["decile"] == decile]
        n, suppressed = len(rows), len(rows) < MIN_CELL_N
        expected = {"phase_order": phase_order[phase], "price_bin": price_bin_label(decile),
                    "trade_count": n, "game_count": len({row["game_id"] for row in rows}),
                    "dollars": sum(row["usdc"] for row in rows), "suppressed": suppressed,
                    "status": "suppressed_n_lt_50" if suppressed else "reported"}
        if suppressed:
            expected.update({key: None for key in (
                "mean_price", "win_rate", "mean_calibration", "calibration_se",
                "calibration_ci95_low", "calibration_ci95_high")})
        else:
            error = sum(row["calibration_error"] for row in rows)/n
            se = _cgm_se(rows, lambda row: row["calibration_error"])
            expected.update({"mean_price": sum(row["home_probability"] for row in rows)/n,
                             "win_rate": sum(float(row["home_won"]) for row in rows)/n,
                             "mean_calibration": error, "calibration_se": se,
                             "calibration_ci95_low": error-1.96*se,
                             "calibration_ci95_high": error+1.96*se})
        _assert_profile_row(profile_by_key[(sample, phase, decile)], expected,
                            f"Phase profile {(sample, phase, decile)}")


def _tail_tuple(
    scope: str, definition: str | None, sample: str | None, phase: str,
    row1: dict[str, Any], row10: dict[str, Any], obs1: list[dict[str, Any]],
    obs10: list[dict[str, Any]], spread_se: float,
) -> tuple[Any, ...]:
    n1, n10 = len(obs1), len(obs10)
    suppressed = n1 < MIN_CELL_N or n10 < MIN_CELL_N
    support = (n1, len({row["game_id"] for row in obs1}), sum(row["dollars"] for row in obs1),
               n10, len({row["game_id"] for row in obs10}), sum(row["dollars"] for row in obs10))
    if suppressed:
        return (scope, definition, sample, phase, *support, True, "suppressed_tail_n_lt_50",
                "suppressed", *([None]*16))
    d1, d10 = row1["mean_calibration"], row10["mean_calibration"]
    spread = d10-d1
    probability_field = "mean_probability" if "mean_probability" in row1 else "mean_price"
    estimates = (
        row1[probability_field], row1["win_rate"], d1, row1["calibration_se"],
        row1["calibration_ci95_low"], row1["calibration_ci95_high"],
        row10[probability_field], row10["win_rate"], d10, row10["calibration_se"],
        row10["calibration_ci95_low"], row10["calibration_ci95_high"],
        spread, spread_se, spread-1.96*spread_se, spread+1.96*spread_se,
    )
    return (scope, definition, sample, phase, *support, False, "reported", _pattern(d1, d10), *estimates)


def estimate_flb_tails(
    sport: str,
    calibration_run_dir: str | Path,
    game_closes_path: str | Path,
    phase_trades_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    calibration = resolved(calibration_run_dir)
    closes, phases, contract_path = map(resolved, (game_closes_path, phase_trades_path, phase_contract_path))
    contract = load_phase_contract(contract_path)
    if contract.sport != sport:
        raise ArtifactError("Tail sport does not match phase contract")
    closing_path = calibration/"closing_calibration.parquet"
    profile_path = calibration/"trade_phase_calibration.parquet"
    estimator_summary_path = calibration/"estimator_summary.json"
    estimator_summary = json.loads(estimator_summary_path.read_text(encoding="utf-8"))
    expected_core_inputs = {
        "game_closes": fingerprint(closes), "phase_trades": fingerprint(phases),
        "phase_contract": phase_contract_fingerprint(contract_path),
    }
    declared_inputs = estimator_summary.get("inputs", {})
    if (estimator_summary.get("sport") != sport
            or estimator_summary.get("method") != "fixed_decile_calibration_v1"
            or any(declared_inputs.get(key) != value for key, value in expected_core_inputs.items())
            or not isinstance(declared_inputs.get("timestamp_declaration"), dict)
            or not isinstance(declared_inputs.get("adapter_provenance"), dict)
            or not matches_artifact_fingerprint(
                estimator_summary.get("outputs", {}).get("closing_calibration"), closing_path)
            or not matches_artifact_fingerprint(
                estimator_summary.get("outputs", {}).get("trade_phase_calibration"), profile_path)):
        raise ArtifactError("Stage-08 summary does not match tail inputs")
    con = duckdb.connect()
    try:
        for relation, path in (("closing", closing_path), ("profiles", profile_path),
                               ("closes", closes), ("phases", phases)):
            con.execute(f"CREATE VIEW {relation} AS SELECT * FROM read_parquet('{quoted(path)}')")
        require_exact_schema(con, "closing", CLOSING_SCHEMA, "Closing calibration")
        require_exact_schema(con, "profiles", PHASE_PROFILE_SCHEMA, "Phase calibration")
        require_exact_schema(con, "closes", CLOSE_SCHEMA, "Dual closes")
        require_exact_schema(con, "phases", PHASE_TRADE_SCHEMA, "Phase fills")
        closing_rows, profile_rows = _read(con, "closing"), _read(con, "profiles")
        close_source, phase_source = _read(con, "closes"), _read(con, "phases")
    finally:
        con.close()
    grid = fixed_bin_grid(contract)
    require_complete_unique_grid(closing_rows, ("close_definition", "profile_scope", "price_decile"),
                                 grid.closing_profile, "closing profile")
    require_complete_unique_grid(profile_rows, ("boundary_sample", "phase", "price_decile"),
                                 grid.trade_phase_profile, "phase profile")
    closing_by_key = {(row["close_definition"], row["profile_scope"], row["price_decile"]): row
                      for row in closing_rows}
    profile_by_key = {(row["boundary_sample"], row["phase"], row["price_decile"]): row
                      for row in profile_rows}
    phase_order = {phase.key: index+1 for index, phase in enumerate(contract.analysis_phases)}
    _recompute_all_profiles(
        grid, phase_order, closing_by_key, profile_by_key,
        close_source, phase_source, sport,
    )
    output: list[tuple[Any, ...]] = []
    for definition in ("primary", "sensitivity"):
        observations = []
        for row in close_source:
            if row["sport"] != sport or not row[f"{definition}_has_close"]:
                continue
            probability = row[f"{definition}_home_probability"]
            error = float(row["home_won"])-probability
            observations.append({"game_id": row["game_id"], "official_date": row["official_date"],
                                 "probability": probability, "error": error,
                                 "dollars": row[f"{definition}_usdc"], "decile": price_decile(probability)})
        d1 = [row for row in observations if row["decile"] == 1]
        d10 = [row for row in observations if row["decile"] == 10]
        for decile, rows in ((1, d1), (10, d10)):
            profile = closing_by_key[(definition, "price_decile", decile)]
            if profile["game_count"] != len(rows):
                raise ArtifactError("Closing tail support does not match Stage-08 profile")
        se = _joint_se(d1, d10, ("official_date",)) if d1 and d10 else 0.0
        output.append(_tail_tuple(
            "closing", definition, None, "pregame_close",
            closing_by_key[(definition, "price_decile", 1)],
            closing_by_key[(definition, "price_decile", 10)], d1, d10, se,
        ))
    for sample in ("literal", "exclude_within_30s"):
        for phase in (item.key for item in contract.analysis_phases):
            observations = []
            for row in phase_source:
                if row["sport"] != sport or not row["analysis_eligible"] or row["phase"] != phase:
                    continue
                if sample == "exclude_within_30s" and row["exclude_within_30s"]:
                    continue
                observations.append({"game_id": row["game_id"], "trade_day": row["trade_day"],
                                     "proxyWallet": row["proxyWallet"],
                                     "probability": row["home_probability"],
                                     "error": row["calibration_error"], "dollars": row["usdc"],
                                     "decile": price_decile(row["home_probability"])})
            d1 = [row for row in observations if row["decile"] == 1]
            d10 = [row for row in observations if row["decile"] == 10]
            for decile, rows in ((1, d1), (10, d10)):
                profile = profile_by_key[(sample, phase, decile)]
                if (profile["trade_count"] != len(rows)
                        or profile["game_count"] != len({row["game_id"] for row in rows})
                        or not math.isclose(profile["dollars"], sum(row["dollars"] for row in rows),
                                            rel_tol=1e-12, abs_tol=1e-9)):
                    raise ArtifactError("Phase tail support does not match Stage-08 profile")
            se = _joint_se(d1, d10, (), True) if d1 and d10 else 0.0
            output.append(_tail_tuple(
                "trade_phase", None, sample, phase,
                profile_by_key[(sample, phase, 1)], profile_by_key[(sample, phase, 10)],
                d1, d10, se,
            ))
    expected = grid.tail_summary
    require_complete_unique_grid(
        [dict(zip((name for name, _ in TAIL_SCHEMA), row)) for row in output],
        ("analysis_scope", "close_definition", "boundary_sample", "phase"),
        expected, "FLB tail summary",
    )
    inputs = (calibration, closes, phases, contract_path)
    with fresh_run(run_dir, inputs) as staging:
        write_parquet(staging/"flb_tail_summary.parquet", TAIL_SCHEMA, output,
                      ("analysis_scope", "close_definition", "boundary_sample", "phase"))
        summary = {
            "schema_version": 1, "sport": sport, "method": "fixed_d1_d10_tail_summary_v1",
            "definitions": {"d1": "[0.0,0.1)", "d10": "[0.9,1.0]",
                            "spread": "D10 mean calibration minus D1 mean calibration",
                            "suppression": "withhold when either tail n < 50",
                            "uncertainty": "joint clustered normal CI using the source estimator clusters"},
            "counts": {"tail_rows": len(output), "reported": sum(not row[10] for row in output),
                       "suppressed": sum(row[10] for row in output)},
            "inputs": {"closing_calibration": fingerprint(closing_path),
                       "trade_phase_calibration": fingerprint(profile_path),
                       "estimator_summary": fingerprint(estimator_summary_path),
                       "game_closes": fingerprint(closes), "phase_trades": fingerprint(phases),
                       "phase_contract": phase_contract_fingerprint(contract_path)},
            "outputs": {"flb_tail_summary": artifact_fingerprint(staging/"flb_tail_summary.parquet")},
        }
        summary["inputs"]["timestamp_declaration"] = declared_inputs["timestamp_declaration"]
        summary["inputs"]["adapter_provenance"] = declared_inputs["adapter_provenance"]
        write_json(staging/"flb_summary.json", summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sport", "calibration_run_dir", "game_closes", "phase_trades", "phase_contract", "run_dir"):
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    print(estimate_flb_tails(args.sport, args.calibration_run_dir, args.game_closes,
                             args.phase_trades, args.phase_contract, args.run_dir))
