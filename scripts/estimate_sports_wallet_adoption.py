"""Bounded serial adoption estimates and saved-output comparisons for nine sports."""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import duckdb
from production_guard import require_production_host
from analysis.multisport_game_dynamics import estimate_flb_decay as engine

CASES = ("historic_filtered", "restored_historical", "restored_legacy_recomputed",
         "restored_repaired", "restored_all")
INPUT_NAMES = ("new_phase", "mlb_phase", "nfl_phase", "nba_phase", "new_exact",
               "mlb_exact", "nfl_exact", "nba_exact", "wallet_flags")
CONTRASTS = (("mlb_coverage", CASES[0], CASES[1]),
             ("historical_flag_gap", CASES[1], CASES[2]),
             ("wallet_identity_repair", CASES[2], CASES[3]),
             ("total_adoption", CASES[0], CASES[3]))
CAPS = {"memory_limit": "96GB", "threads": 8, "spill_bytes": 4_000_000_000,
        "output_bytes": 4_000_000_000, "free_reserve_bytes": 12_000_000_000,
        "summary_file_bytes": 16_000_000}
TOLERANCE = {"relative": 1e-8, "absolute": 1e-10}
CLASSIFIER_SHA256 = "52ee90215b81346852fa775cc1f9df6d72b7b7d5dcbc6ef357c1b7e379fb42fe"
REPAIR_SHA256 = "f142313238e9be25541f97dac4ba24de7ad6b49484812e81e7c6640eefd50daa"
REPAIR_QA_SHA256 = "06d0fd376c7c6aede78016e50bc34cd63220cf8a15d481598303679d7de2cbb3"
SOURCE_PATHS = (
    "scripts/estimate_sports_wallet_adoption.py", "scripts/render_sports_wallet_adoption.py",
    "tests/test_sports_wallet_adoption_estimates.py", "tests/test_render_sports_wallet_adoption.py",
    "analysis/multisport_game_dynamics/estimate_flb_decay.py",
    "analysis/multisport_game_dynamics/render_flb_decay.py",
    "analysis/bot_filter.py", "scripts/rebuild_polymarket_wallet_flags.py",
    "scripts/rebuild_sports_wallet_samples.py",
    "analysis/sports_game_dynamics/artifacts.py", "production_guard.py",
    "docs/analysis_specs/sports_wallet_adoption_v1.md",
    "docs/analysis_specs/flb_time_regressions_v1.md",
    "docs/analysis_specs/flb_time_regressions_v2.md",
    "docs/analysis_specs/flb_time_regressions_v3.md",
)
OUTPUT_SCHEMAS = {
    "coefficients.parquet": engine.COEFFICIENT_SCHEMA,
    "model_summary.parquet": engine.MODEL_SCHEMA,
    "estimands.parquet": engine.ESTIMAND_SCHEMA,
    "support.parquet": engine.SUPPORT_SCHEMA,
    "duration_reference.parquet": engine.DURATION_SCHEMA,
    "time_bin_spreads.parquet": engine.TIME_BIN_SCHEMA,
    "kernel_time_spreads.parquet": engine.KERNEL_SCHEMA,
    "pregame_time_distribution.parquet": engine.PREGAME_TIME_SCHEMA,
}
UNIQUE_KEYS = {
    "coefficients.parquet": ("model_id", "term_order"),
    "model_summary.parquet": ("model_id",), "estimands.parquet": ("estimand_id",),
    "support.parquet": ("sample", "time_normalization", "sport", "segment", "tail"),
    "duration_reference.parquet": ("sport",),
    "time_bin_spreads.parquet": ("scope", "sport", "weighting", "time_bin"),
    "kernel_time_spreads.parquet": ("scope", "sport", "weighting", "phase", "grid_index"),
    "pregame_time_distribution.parquet": ("sport", "tail"),
}


class AdoptionBlocked(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AdoptionBlocked(message)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stat_identity(path: Path) -> dict[str, int]:
    info = path.stat()
    require(path.is_file(), f"Input is not a regular file: {path}")
    return {"bytes": info.st_size, "device": info.st_dev, "inode": info.st_ino,
            "mtime_ns": info.st_mtime_ns}


def fingerprint(path: Path) -> dict[str, Any]:
    before = stat_identity(path)
    digest = sha256(path)
    require(before == stat_identity(path), f"Input changed while hashing: {path}")
    return {"path": str(path.resolve()), "bytes": before["bytes"], "sha256": digest}


def read_json(path: Path) -> dict[str, Any]:
    require(stat_identity(path)["bytes"] <= CAPS["summary_file_bytes"], "JSON exceeds summary cap")
    value = json.loads(path.read_text())
    require(isinstance(value, dict), "Expected JSON object")
    return value


def atomic_publish(staging: Path, target: Path) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        result = library.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = library.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise AdoptionBlocked("Atomic no-replace publication unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def close(left: float, right: float) -> bool:
    return math.isclose(float(left), float(right), rel_tol=TOLERANCE["relative"],
                        abs_tol=TOLERANCE["absolute"])


def integer(value: Any, label: str) -> int:
    require(type(value) is int and value >= 0, f"Invalid nonnegative count: {label}")
    return value


def keyed(rows: list[dict], keys: tuple[str, ...], label: str) -> dict[tuple, dict]:
    result = {tuple(row[key] for key in keys): row for row in rows}
    require(len(result) == len(rows), f"Duplicate keys: {label}")
    require(all(all(part is not None for part in key) for key in result), f"Null key: {label}")
    return result


def validate_interval(row: dict, *, suppressed: bool, spread: bool = False) -> None:
    fields = (("d1_mean_calibration", "d10_mean_calibration", "spread_d10_minus_d1",
               "spread_standard_error", "spread_ci95_low", "spread_ci95_high") if spread else
              ("estimate", "standard_error", "ci95_low", "ci95_high"))
    if suppressed:
        require(all(row[field] is None for field in fields), "Suppressed numeric values must be null")
        return
    require(all(row[field] is not None and math.isfinite(float(row[field])) for field in fields),
            "Supported estimate/interval must be finite")
    estimate, se, low, high = (row[field] for field in fields[-4:])
    require(se >= 0 and close(low, estimate-1.96*se) and close(high, estimate+1.96*se),
            "Interval does not reconcile to saved clustered uncertainty")
    if spread:
        require(close(estimate, row[fields[1]]-row[fields[0]]), "Tail spread does not reconcile")


def model_grid() -> dict[str, dict[str, Any]]:
    """Expected frozen model IDs; feature expressions come from the existing engine."""
    result = {}
    variants = (("all_pregame_live", "realized_duration", False),
                ("live_only", "realized_duration", False),
                ("bounded_pregame", "realized_duration", False),
                ("all_pregame_live", "sport_median_duration", False),
                ("all_pregame_live", "realized_duration", True))
    for sport in engine.SPORTS:
        for sample, normalization, piecewise in variants:
            family = "tail_piecewise" if piecewise else "tail_linear"
            result[f"sport_{sport}_{family}_{sample}_{normalization}"] = dict(
                scope="sport", sport=sport, family=family, sample=sample,
                time_normalization=normalization, weighting="equal_fill", adjustment="none")
        result[f"sport_{sport}_continuous_all_pregame_live_realized_duration"] = dict(
            scope="sport", sport=sport, family="continuous_price", sample="all_pregame_live",
            time_normalization="realized_duration", weighting="equal_fill", adjustment="none")
    variants = (("all_pregame_live", "realized_duration", "none", "equal_fill"),
                ("all_pregame_live", "realized_duration", "sport_intercepts", "equal_fill"),
                ("all_pregame_live", "realized_duration", "sport_composition", "equal_fill"),
                ("all_pregame_live", "realized_duration", "sport_composition", "equal_sport"),
                ("all_pregame_live", "realized_duration", "sport_composition", "dollar"),
                ("live_only", "realized_duration", "sport_composition", "equal_fill"),
                ("live_only", "realized_duration", "sport_composition", "equal_sport"),
                ("bounded_pregame", "realized_duration", "sport_composition", "equal_fill"),
                ("bounded_pregame", "realized_duration", "sport_composition", "equal_sport"),
                ("all_pregame_live", "sport_median_duration", "sport_composition", "equal_fill"),
                ("all_pregame_live", "sport_median_duration", "sport_composition", "equal_sport"))
    for sample, normalization, adjustment, weighting in variants:
        result[f"pooled_tail_{sample}_{normalization}_{adjustment}_{weighting}"] = dict(
            scope="pooled", sport="all", family="tail_linear", sample=sample,
            time_normalization=normalization, weighting=weighting, adjustment=adjustment)
    for sample, normalization in (("all_pregame_live", "realized_duration"),
                                  ("live_only", "realized_duration"),
                                  ("bounded_pregame", "realized_duration"),
                                  ("all_pregame_live", "sport_median_duration")):
        for adjustment, weighting in (("sport_composition", "equal_fill"),
                                       ("sport_composition", "equal_sport"),
                                       ("fully_interacted", "equal_fill")):
            result[f"pooled_supported_tail_{sample}_{normalization}_{adjustment}_{weighting}"] = dict(
                scope="pooled_supported", family="tail_linear", sample=sample,
                time_normalization=normalization, weighting=weighting, adjustment=adjustment)
    for weighting in ("equal_fill", "equal_sport"):
        result[f"pooled_supported_tail_piecewise_all_pregame_live_realized_duration_sport_composition_{weighting}"] = dict(
            scope="pooled_supported", family="tail_piecewise", sample="all_pregame_live",
            time_normalization="realized_duration", weighting=weighting, adjustment="sport_composition")
    for adjustment, weighting in (("none", "equal_fill"), ("sport_composition", "equal_fill"),
                                   ("sport_composition", "equal_sport")):
        result[f"pooled_continuous_all_pregame_live_realized_duration_{adjustment}_{weighting}"] = dict(
            scope="pooled", sport="all", family="continuous_price", sample="all_pregame_live",
            time_normalization="realized_duration", weighting=weighting, adjustment=adjustment)
    return result


def features_for(model: dict) -> list:
    time_column = "realized_time" if model["time_normalization"] == "realized_duration" else "fixed_time"
    sports = engine.SPORTS if model["sport"] == "all" else tuple(model["sport"].split("+"))
    if model["scope"] == "sport":
        builder = {"tail_linear": engine._tail_features, "tail_piecewise": engine._piecewise_features,
                   "continuous_price": engine._continuous_features}[model["family"]]
        return builder(time_column)
    if model["family"] == "tail_linear":
        return engine._pooled_tail_features(time_column, model["adjustment"], sports)
    if model["family"] == "tail_piecewise":
        return engine._pooled_piecewise_features(time_column, sports)
    return engine._pooled_continuous_features(time_column, model["adjustment"])


def validate_tables(tables: dict[str, list[dict]], manifest: dict) -> None:
    maps = {name: keyed(rows, UNIQUE_KEYS[name], name) for name, rows in tables.items()}
    models = {key[0]: row for key, row in maps["model_summary.parquet"].items()}
    expected = model_grid()
    require(set(models) == set(expected), "Incomplete frozen model grid")
    support = maps["support.parquet"]
    expected_support = {(sample, norm, sport, segment, tail)
        for sample, norm in (("all_pregame_live", "realized_duration"),
                             ("live_only", "realized_duration"),
                             ("bounded_pregame", "realized_duration"),
                             ("all_pregame_live", "sport_median_duration"))
        for sport in engine.SPORTS
        for segment in (("live",) if sample == "live_only" else ("pregame", "live"))
        for tail in ("D1", "D10")}
    require(set(support) == expected_support, "Incomplete frozen support grid")
    for row in support.values():
        require(integer(row["n_events"], "support events") <= integer(row["n_obs"], "support fills"),
                "Support events exceed fills")
    estimands = maps["estimands.parquet"]
    expected_estimands = set()
    expected_coefficients = set()
    for model_id, spec in expected.items():
        model = models[model_id]
        require(all(model[name] == value for name, value in spec.items()), "Model contract changed")
        active = engine.SPORTS if model["sport"] == "all" else tuple(model["sport"].split("+"))
        require(set(active) <= set(engine.SPORTS) and len(active) == len(set(active)), "Invalid supported sports")
        if model["scope"] == "pooled_supported":
            require(len(active) >= 2, "Balanced pool has fewer than two supported sports")
        features = features_for(model)
        _, low, high = engine._sample_clause(model["sample"], "realized_time")
        require(model["window_low"] == low and model["window_high"] == high, "Model time window changed")
        require(model["n_parameters"] == len(features), "Wrong model parameter count")
        require(type(model["suppressed"]) is bool, "Invalid suppression flag")
        if model["family"] == "continuous_price":
            expected_n = sum(manifest["observation_counts"][sport] for sport in active)
            require(not model["suppressed"], "Continuous model unexpectedly suppressed")
        else:
            segments = ("live",) if model["sample"] == "live_only" else ("pregame", "live")
            cells = [support[(model["sample"], model["time_normalization"], sport, segment, tail)]
                     for sport in active for segment in segments for tail in ("D1", "D10")]
            expected_n = sum(row["n_obs"] for row in cells)
            if model["scope"] == "sport":
                require(model["suppressed"] == any(row["n_obs"] < engine.MIN_N for row in cells),
                        "Sport suppression disagrees with support")
            if model["scope"] == "pooled_supported":
                supported = tuple(sport for sport in engine.SPORTS if all(
                    support[(model["sample"], model["time_normalization"], sport, segment, tail)]["n_obs"] >= engine.MIN_N
                    for segment in segments for tail in ("D1", "D10")))
                require(active == supported, "Balanced pool membership disagrees with support")
        require(model["n_obs"] == (0 if model["suppressed"] else expected_n), "Model fill count disagrees")
        require(model["rank"] == (0 if model["suppressed"] else len(features)), "Model rank disagrees")
        if not model["suppressed"]:
            require(model["status"] == "reported", "Supported model status changed")
            require(math.isfinite(model["condition_number"]) and model["condition_number"] > 0,
                    "Invalid design condition number")
            for name in ("n_events", "n_days", "n_wallets", "n_event_clusters"):
                require(0 < integer(model[name], name) <= model["n_obs"], "Invalid model cluster count")
        else:
            require(model["status"].startswith("withheld_support:"), "Missing suppression reason")
        for order, (term, _) in enumerate(features, 1):
            key = (model_id, order)
            expected_coefficients.add(key)
            require(key in maps["coefficients.parquet"], "Missing coefficient row")
            row = maps["coefficients.parquet"][key]
            require(row["term"] == term, "Coefficient term grid changed")
            require(all(row[name] == model[name] for name in (*spec, "sport")), "Coefficient metadata changed")
            validate_interval(row, suppressed=model["suppressed"])
        names = (("pregame_tail_spread_change", "live_tail_spread_change")
                 if model["family"] == "tail_piecewise" else
                 ("price_gradient_time_slope",) if model["family"] == "continuous_price" else
                 ("equal_weight_mean_sport_tail_slope",) if model["adjustment"] == "fully_interacted" else
                 ("tail_spread_time_slope",))
        for name in names:
            key = (f"{model_id}:{name}",)
            expected_estimands.add(key)
            require(key in estimands, "Missing estimand row")
            row = estimands[key]
            require(row["source_model_id"] == model_id and row["estimand"] == name, "Estimand linkage changed")
            require(all(row[field] == model[field] for field in (*spec, "sport")) and
                    row["suppressed"] == model["suppressed"] and row["status"] == model["status"] and
                    row["n_obs"] == model["n_obs"] and row["n_events"] == model["n_events"],
                    "Estimand/model reconciliation failed")
            validate_interval(row, suppressed=model["suppressed"])
            if not model["suppressed"] and model["adjustment"] != "fully_interacted":
                term = {"tail_spread_time_slope": "D10 x time", "price_gradient_time_slope": "Price x time",
                        "pregame_tail_spread_change": "D10 x pregame time", "live_tail_spread_change": "D10 x live time"}[name]
                coefficient = next(item for item in tables["coefficients.parquet"] if item["model_id"] == model_id and item["term"] == term)
                require(all(close(row[field], coefficient[field]) for field in
                            ("estimate", "standard_error", "ci95_low", "ci95_high")), "Estimand/coefficient contrast differs")
    require(set(maps["coefficients.parquet"]) == expected_coefficients, "Extra coefficient rows")
    require(set(estimands) == expected_estimands, "Extra estimand rows")
    require(set(maps["duration_reference.parquet"]) == {(sport,) for sport in engine.SPORTS}, "Duration grid changed")
    for row in tables["duration_reference.parquet"]:
        require(row["event_count"] > 0 and math.isfinite(row["median_duration_seconds"]) and
                row["median_duration_seconds"] > 0 and
                close(row["median_duration_minutes"], row["median_duration_seconds"]/60), "Invalid duration reference")
    expected_bins = {(scope, sport, weighting, index)
        for scope, sport, weighting in (("pooled", "all", "equal_fill"), ("pooled", "all", "equal_sport"),
                                        *(("sport", sport, "equal_fill") for sport in engine.SPORTS))
        for index in range(1, 11)}
    require(set(maps["time_bin_spreads.parquet"]) == expected_bins, "Incomplete live-bin grid")
    for sport in engine.SPORTS:
        rows = [row for row in tables["time_bin_spreads.parquet"] if row["scope"] == "sport" and row["sport"] == sport]
        for tail, field in (("D1", "d1_n"), ("D10", "d10_n")):
            require(sum(row[field] for row in rows) == support[("live_only", "realized_duration", sport, "live", tail)]["n_obs"], "Live-bin support reconciliation failed")
    for weighting in ("equal_fill", "equal_sport"):
        for index in range(1, 11):
            pooled = maps["time_bin_spreads.parquet"][("pooled", "all", weighting, index)]
            for field in ("d1_n", "d10_n"):
                require(pooled[field] == sum(maps["time_bin_spreads.parquet"][("sport", sport, "equal_fill", index)][field] for sport in engine.SPORTS), "Pooled live-bin support differs")
    kernel_groups = {}
    for name in ("time_bin_spreads.parquet", "kernel_time_spreads.parquet"):
        for row in tables[name]:
            for field in ("d1_n", "d10_n", "d1_events", "d10_events"):
                integer(row[field], field)
            require(row["d1_events"] <= row["d1_n"] and row["d10_events"] <= row["d10_n"], "Tail events exceed fills")
            suppressed = row["d1_n"] < engine.MIN_N or row["d10_n"] < engine.MIN_N
            require(row["suppressed"] is suppressed, "Tail suppression disagrees with support")
            require(row["status"] == (f"withheld_tail_n_lt_{engine.MIN_N}" if suppressed else "reported"), "Tail status changed")
            validate_interval(row, suppressed=suppressed, spread=True)
            if name == "time_bin_spreads.parquet":
                require(close(row["time_low"], (row["time_bin"]-1)/10) and close(row["time_high"], row["time_bin"]/10), "Live-bin boundaries changed")
            else:
                require(row["phase"] in engine.KERNEL_BANDWIDTHS and row["kernel"] == "epanechnikov" and
                        row["bandwidth"] == engine.KERNEL_BANDWIDTHS[row["phase"]], "Kernel definition changed")
                key = (row["scope"], row["sport"], row["weighting"], row["phase"])
                kernel_groups.setdefault(key, []).append(row)
    expected_groups = {(scope, sport, weighting, phase) for scope, sport, weighting in
        (("pooled", "all", "equal_fill"), ("pooled", "all", "equal_sport"),
         *(("sport", sport, "equal_fill") for sport in engine.SPORTS)) for phase in ("pregame", "live")}
    require(set(kernel_groups) == expected_groups, "Incomplete kernel panel grid")
    for key, rows in kernel_groups.items():
        rows.sort(key=lambda row: row["grid_index"])
        values = [row["time_value"] for row in rows]
        require([row["grid_index"] for row in rows] == list(range(len(rows))) and
                all(math.isfinite(value) for value in values) and values == sorted(set(values)), "Invalid kernel grid")
        if key[-1] == "live":
            require(len(rows) == 101 and all(close(value, index/100) for index, value in enumerate(values)), "Live kernel grid changed")
        else:
            require(1 <= len(rows) <= 102 and values[-1] == 0 and all(value <= 0 for value in values), "Pregame kernel grid changed")
    pregame = maps["pregame_time_distribution.parquet"]
    require(set(pregame) == {(sport, tail) for sport in engine.SPORTS for tail in ("D1", "D10")}, "Pregame grid changed")
    for (sport, tail), row in pregame.items():
        require(row["n_obs"] == support[("all_pregame_live", "realized_duration", sport, "pregame", tail)]["n_obs"], "Pregame count differs")


def load_estimates(manifest_path: Path, *, expected_counts: dict | None = None,
                   expected_inputs: dict | None = None, require_resources: bool = False) -> dict:
    manifest = read_json(manifest_path)
    require(manifest.get("schema_version") == 3 and manifest.get("stage") == "multisport_flb_time_regressions_v3" and
            manifest.get("sports") == list(engine.SPORTS) and manifest.get("support_floor") == 500,
            "Estimator manifest contract changed")
    require(manifest.get("trade_sample") in ("filtered_trades", "all_trades"), "Unknown trade sample")
    require(set(manifest["observation_counts"]) == set(engine.SPORTS) and
            all(integer(value, "observation count") > 0 for value in manifest["observation_counts"].values()), "Invalid observation counts")
    if expected_counts is not None:
        require(manifest["observation_counts"] == expected_counts, "Estimator/sample counts differ")
    if expected_inputs is not None:
        require(manifest["inputs"] == {f"input_{index:02d}": expected_inputs[name]
                for index, name in enumerate(INPUT_NAMES, 1)}, "Estimator/sample input fingerprints differ")
    if require_resources:
        resources = manifest.get("execution_resources", {})
        require(resources.get("memory_limit") == "96GB" and resources.get("threads") == 8 and
                resources.get("max_temp_directory_size") == "4000000000B" and resources.get("timezone") == "UTC",
                "Fixed estimator resource settings missing")
    require(set(manifest["outputs"]) == set(OUTPUT_SCHEMAS), "Estimator output inventory changed")
    tables = {}
    con = duckdb.connect(config={"memory_limit": "128MB", "threads": 1, "max_temp_directory_size": "0B"})
    try:
        for name, schema in OUTPUT_SCHEMAS.items():
            path = manifest_path.parent/name
            require(stat_identity(path)["bytes"] <= CAPS["summary_file_bytes"], "Estimator summary exceeds bound")
            observed = fingerprint(path)
            require(manifest["outputs"][name] == {**observed, "path": name}, "Saved estimator output hash differs")
            expression = str(path.resolve()).replace("'", "''")
            actual = tuple((row[0], row[1]) for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{expression}')").fetchall())
            require(actual == schema, f"Saved estimator schema differs: {name}")
            cursor = con.execute(f"SELECT * FROM read_parquet('{expression}')")
            rows = cursor.fetchmany(10_001)
            require(len(rows) <= 10_000, "Summary row cap exceeded")
            tables[name] = [dict(zip((field for field, _ in schema), row)) for row in rows]
    finally:
        con.close()
    validate_tables(tables, manifest)
    return {"manifest": manifest, "tables": tables, "manifest_fingerprint": fingerprint(manifest_path)}


def compare_estimands(before: list[dict], after: list[dict], comparison: str) -> list[dict]:
    left = keyed(before, ("estimand_id",), "before estimands")
    right = keyed(after, ("estimand_id",), "after estimands")
    require(set(left) == set(right), "Comparison estimand grids differ")
    rows = []
    fields = ("family", "scope", "sport", "sample", "time_normalization", "weighting", "adjustment", "estimand")
    for key in sorted(left):
        old, new = left[key], right[key]
        same = all(old[field] == new[field] for field in fields)
        status = ("supported" if same and not old["suppressed"] and not new["suppressed"] else
                  "supported_population_changed" if not same else "suppression_transition" if
                  old["suppressed"] != new["suppressed"] else "both_withheld")
        delta = new["estimate"]-old["estimate"] if status == "supported" else None
        rows.append({"comparison": comparison, "estimand_id": key[0],
                     **{field: new[field] for field in fields}, "before": project_estimand(old), "after": project_estimand(new),
                     "change": delta, "status": status,
                     "change_uncertainty": "not estimated; cross-run covariance unavailable"})
    return rows


def project_estimand(row: dict) -> dict:
    """Keep needed, finite evidence; diagnostic t/p values stay in source Parquet."""
    return {key: value for key, value in row.items() if key not in {"t_statistic", "p_value"}}


def verify_historical_reproduction(archived: dict, reproduced: dict) -> dict:
    """Execution-only changes must reproduce all bounded archived summary evidence."""
    require(archived["manifest"]["trade_sample"] == reproduced["manifest"]["trade_sample"] == "filtered_trades" and
            archived["manifest"]["observation_counts"] == reproduced["manifest"]["observation_counts"],
            "Historical sample reproduction differs")
    comparisons = {}
    for name, schema in OUTPUT_SCHEMAS.items():
        before = keyed(archived["tables"][name], UNIQUE_KEYS[name], "archived "+name)
        after = keyed(reproduced["tables"][name], UNIQUE_KEYS[name], "reproduced "+name)
        require(set(before) == set(after), "Historical output key grid differs: "+name)
        numeric_fields = {field for field, kind in schema if kind in {"DOUBLE", "FLOAT"}}
        numeric_comparisons, exact_comparisons, maximum_absolute_difference = 0, 0, 0.
        for key in before:
            for field, _ in schema:
                left, right = before[key][field], after[key][field]
                if field in numeric_fields and left is not None and right is not None:
                    same_nonfinite = (math.isnan(left) and math.isnan(right)) or (math.isinf(left) and left == right)
                    require(same_nonfinite or (math.isfinite(left) and math.isfinite(right) and close(left, right)),
                            f"Historical numeric reproduction differs: {name} {key} {field}")
                    numeric_comparisons += 1
                    if not same_nonfinite:
                        maximum_absolute_difference = max(maximum_absolute_difference, abs(left-right))
                else:
                    require(left == right, f"Historical definition/count/status reproduction differs: {name} {key} {field}")
                    exact_comparisons += 1
        comparisons[name] = {"rows": len(before), "numeric_comparisons": numeric_comparisons,
            "exact_comparisons": exact_comparisons, "maximum_absolute_difference": maximum_absolute_difference}
    return {"verified": True, "numerical_tolerance": TOLERANCE,
            "archived_manifest": archived["manifest_fingerprint"], "tables": comparisons}


def build_comparison(runs: dict[str, dict], flag_manifest: dict, samples: dict) -> dict:
    require(set(runs) == set(CASES), "Incomplete adoption cases")
    changes = []
    for name, before, after in CONTRASTS:
        changes.extend(compare_estimands(runs[before]["tables"]["estimands.parquet"],
                                         runs[after]["tables"]["estimands.parquet"], name))
    by_key = {(row["comparison"], row["estimand_id"]): row for row in changes}
    decomposition = []
    for row in changes:
        if row["comparison"] != "total_adoption":
            continue
        pieces = [by_key[(name, row["estimand_id"])] for name, _, _ in CONTRASTS[:3]]
        supported = row["status"] == "supported" and all(part["status"] == "supported" for part in pieces)
        if supported:
            require(close(row["change"], sum(part["change"] for part in pieces)), "Adoption difference composition failed")
        decomposition.append({"estimand_id": row["estimand_id"], "supported": supported,
            **{part["comparison"]: part["change"] if supported else None for part in pieces},
            "total_adoption": row["change"] if supported else None,
            "status": "reconciled" if supported else "withheld_noncomparable_components"})
    return {"schema_version": "sports_wallet_adoption_comparison_v1", "status": "comparison_complete",
            "data_certified": False, "downstream_adoption": "current_nine_sport_results",
            "numerical_tolerance": TOLERANCE,
            "cases": {name: {"trade_sample": run["manifest"]["trade_sample"],
                "observation_counts": run["manifest"]["observation_counts"],
                "estimands": [project_estimand(row) for row in run["tables"]["estimands.parquet"]],
                "manifest": run["manifest_fingerprint"]} for name, run in runs.items()},
            "changes": changes, "decomposition": decomposition,
            "flags": {"builds": flag_manifest["builds"], "comparisons": flag_manifest["comparisons"]},
            "sample_scenarios": samples.get("scenarios", {}),
            "gates": {"all_trade_regime_invariant": True, "historical_observation_reproduction": True,
                      "historical_estimate_reproduction": samples.get("historical_estimate_reproduction") is True,
                      "saved_output_schema_grid_support_rank_interval_checks": True,
                      "supported_difference_composition": True},
            "interpretation": "Observed result changes under fixed definitions. Identity repair does not certify inferred counterparty action, holdings, completeness or causal profit taking."}


def committed_source(expected_head: str) -> dict:
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head) is not None, "Expected committed HEAD must be full SHA")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(head == expected_head, "Source HEAD differs")
    result = {}
    for name in SOURCE_PATHS:
        subprocess.run(["git", "ls-files", "--error-unmatch", "--", name], cwd=ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=ROOT, check=True)
        result[name] = fingerprint(ROOT/name)
    return {"head": head, "files": result}


def sample_contract(samples: dict, flags: dict) -> None:
    require(samples.get("schema_version") == 1 and samples.get("status") == "sports_wallet_samples_complete" and
            samples.get("data_certified") is False and set(samples.get("cases", {})) == set(CASES), "Incomplete sample manifest")
    require(samples.get("gates", {}).get("all_trade_regime_invariant") is True and
            samples.get("gates", {}).get("historical_observation_reproduction") is True, "Sample invariant/reproduction gates missing")
    for name in ("historical_H_labels_reproduced", "all_nonflag_payloads_bit_exact", "native_ids_unique",
                 "saved_flags_rejoined", "normalizer_oracle_equal", "filtered_subset_of_all", "disjoint_exclusions",
                 "all_support_invariant", "input_and_source_freeze"):
        require(samples.get("reconciliation", {}).get(name) is True, f"Sample reconciliation missing: {name}")
    require(flags.get("schema_version") == "polymarket_wallet_flags_rebuild_v1" and
            flags.get("status") == "wallet_flags_rebuild_complete" and flags.get("data_certified") is False and
            flags.get("downstream_adoption") == "pending",
            "Incomplete paired flag manifest")
    require(flags.get("reconciliation", {}).get("exact_source_wallet_keys_and_counts") is True,
            "Paired flags/source wallet reconciliation missing")
    require(flags["contract"]["timestamp_lower_inclusive"] == 1590969600 and
            flags["contract"]["sides"] == "all published sides" and flags["contract"]["timezone"] == "UTC" and
            flags["contract"]["classifier_sha256"] == CLASSIFIER_SHA256, "Classifier population changed")
    require(flags["binding"]["repair_manifest"]["sha256"] == REPAIR_SHA256 and
            flags["binding"]["repair_qa"]["sha256"] == REPAIR_QA_SHA256, "Repair evidence binding changed")
    outputs = {Path(value["path"]).name: value for value in flags["outputs"]}
    historical = flags["inputs"]["historical_flags"]["pipeline_data"]
    expected_flags = {
        "historic_filtered": (historical["stat"]["bytes"], historical["expected_content_sha256"]),
        "restored_historical": (historical["stat"]["bytes"], historical["expected_content_sha256"]),
        "restored_legacy_recomputed": (outputs["legacy_recomputed_flags.parquet"]["bytes"], outputs["legacy_recomputed_flags.parquet"]["sha256"]),
        "restored_repaired": (outputs["wallet_flags.parquet"]["bytes"], outputs["wallet_flags.parquet"]["sha256"]),
        "restored_all": (outputs["wallet_flags.parquet"]["bytes"], outputs["wallet_flags.parquet"]["sha256"]),
    }
    for name, case in samples["cases"].items():
        require(set(case["engine_inputs"]) == set(INPUT_NAMES), "Engine input inventory differs")
        require(case["trade_sample"] == ("all_trades" if name == "restored_all" else "filtered_trades"), "Case trade sample differs")
        require(set(case["observation_counts"]) == set(engine.SPORTS) and
                all(integer(value, "case observation count") > 0 for value in case["observation_counts"].values()), "Case counts incomplete")
        for value in case["engine_inputs"].values():
            require(set(value) == {"path", "bytes", "sha256"} and
                    re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is not None, "Invalid engine input fingerprint")
        chosen = case["engine_inputs"]["wallet_flags"]
        require((chosen["bytes"], chosen["sha256"]) == expected_flags[name], "Case/paired flag regime differs")
    require(set(samples.get("scenarios", {})) == {"old_H", "restored_H", "restored_F0", "restored_F1"}, "Incomplete source/flag scenarios")
    scenario_names = dict(zip(CASES, ("old_H", "restored_H", "restored_F0", "restored_F1", "restored_F1")))
    for name, case in samples["cases"].items():
        rows = samples["scenarios"][scenario_names[name]]["counts"]
        counts = keyed(rows, ("sport",), "scenario counts")
        require(set(counts) == {(sport,) for sport in engine.SPORTS}, "Scenario sport grid differs")
        for (sport,), row in counts.items():
            for field in ("raw", "outside_cohort", "post_end", "invalid_price", "all", "extreme", "flagged_interior", "filtered", "missing_flag_all", "null_flag_all", "pregame_all", "end_equal_all"):
                integer(row[field], field)
            require(row["raw"] == row["outside_cohort"]+row["post_end"]+row["invalid_price"]+row["all"] and
                    row["all"] == row["extreme"]+row["flagged_interior"]+row["filtered"], "Scenario exclusion accounting differs")
            require(case["observation_counts"][sport] == row["all" if name == "restored_all" else "filtered"], "Case/scenario counts differ")
    require(samples["cases"]["restored_all"]["engine_inputs"] == samples["cases"]["restored_repaired"]["engine_inputs"], "Final all/filtered source generation differs")
    for name in CASES[1:]:
        for role in INPUT_NAMES[:4]:
            require(samples["cases"][name]["engine_inputs"][role] == samples["cases"]["historic_filtered"]["engine_inputs"][role], "Frozen cohort/clock input changed")
    require(all(samples["cases"][name]["engine_inputs"]["mlb_exact"] == samples["cases"]["restored_historical"]["engine_inputs"]["mlb_exact"] for name in CASES[2:]), "Restored MLB input vintage differs")


def free_bytes(path: Path) -> int:
    while not path.exists():
        path = path.parent
    values = os.statvfs(path)
    return values.f_bavail*values.f_frsize


def preflight(sample_path: Path, flag_path: Path, historical_path: Path,
              target: Path, expected_head: str) -> dict:
    require(not target.exists(), "Immutable target exists")
    samples, flags, historic = read_json(sample_path), read_json(flag_path), read_json(historical_path)
    sample_contract(samples, flags)
    source = committed_source(expected_head)
    require(samples.get("source", {}).get("head") == expected_head and
            samples.get("source", {}).get("expected_head") == expected_head and
            flags.get("source", {}).get("head") == expected_head, "Sample/flag/source generation differs")
    estimator = "analysis/multisport_game_dynamics/estimate_flb_decay.py"
    require(samples.get("source", {}).get("files", {}).get(estimator) == source["files"][estimator],
            "Sample/committed scientific estimator binding differs")
    require(samples.get("flag_manifest") == fingerprint(flag_path), "Sample/paired flag manifest binding differs")
    historical_evidence = samples.get("inputs", {}).get("sep20_filtered", {})
    require(fingerprint(historical_path) == {"path": historical_evidence.get("path"),
            "bytes": historical_evidence.get("stat_before", {}).get("bytes"), "sha256": historical_evidence.get("sha256")},
            "Archived September manifest/frozen sample evidence binding differs")
    require(historic["observation_counts"] == samples["cases"]["historic_filtered"]["observation_counts"], "Historical observation reproduction differs")
    require(historic["inputs"]["input_09"]["sha256"] == samples["cases"]["historic_filtered"]["engine_inputs"]["wallet_flags"]["sha256"], "Historical shared flag binding differs")
    files = {str(path.resolve()): fingerprint(path) for path in (sample_path, flag_path, historical_path)}
    for case in samples["cases"].values():
        for expected in case["engine_inputs"].values():
            path = Path(expected["path"]).resolve()
            require(target != path and target not in path.parents and path not in target.parents, "Output overlaps input")
            info = stat_identity(path)
            require(info["bytes"] == expected["bytes"], "Sample input bytes differ")
            entry = {"expected": expected, "stat": info}
            require(str(path) not in files or files[str(path)] == entry, "Conflicting shared input declarations")
            files[str(path)] = entry
    available = free_bytes(target.parent)
    require(available >= CAPS["free_reserve_bytes"]+CAPS["spill_bytes"]+CAPS["output_bytes"], "Insufficient disk reserve")
    return {"schema_version": "sports_wallet_adoption_estimates_v1", "status": "preflight_complete",
            "data_certified": False, "source": source, "caps": CAPS, "target": str(target.resolve()),
            "numerical_tolerance": TOLERANCE, "inputs": files,
            "controls": {"samples": fingerprint(sample_path), "flags": fingerprint(flag_path),
                         "historical_estimates": fingerprint(historical_path)},
            "available_free_bytes": available,
            "limits": "Metadata/source-only admission; complete case input hashes are verified before estimation and publication. No scientific estimates in preflight."}


def verify_case_inputs(preflight_info: dict, *, content: bool) -> None:
    for name, value in preflight_info["inputs"].items():
        path = Path(name)
        if "expected" in value:
            require(stat_identity(path) == value["stat"], "Case input stat changed")
            if content:
                require(fingerprint(path) == value["expected"], "Case input hash changed")
        else:
            require(fingerprint(path) == value, "Control input changed")


def body(sample_path: Path, flag_path: Path, historical_path: Path, target: Path,
         expected_head: str, reviewed: dict, command: list[str] | None = None) -> dict:
    current = preflight(sample_path, flag_path, historical_path, target, expected_head)
    for name in ("schema_version", "status", "source", "caps", "target", "numerical_tolerance", "inputs", "controls"):
        require(current[name] == reviewed[name], f"Reviewed preflight differs: {name}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    started = time.monotonic()
    try:
        verify_case_inputs(current, content=True)
        samples, flags = read_json(sample_path), read_json(flag_path)
        require(samples.get("flag_manifest") == fingerprint(flag_path), "Sample/paired flag generation differs")
        archived = load_estimates(historical_path,
                 expected_counts=samples["cases"]["historic_filtered"]["observation_counts"])
        require(archived["manifest"]["trade_sample"] == "filtered_trades", "Historical baseline is not filtered")
        runs, historical_reproduction = {}, None
        for name in CASES:
            verify_case_inputs(current, content=False)
            require(free_bytes(staging) >= CAPS["free_reserve_bytes"]+CAPS["spill_bytes"]+CAPS["output_bytes"], "Disk reserve failed before case")
            case = samples["cases"][name]
            arguments = {key: value["path"] for key, value in case["engine_inputs"].items()}
            engine.estimate_flb_decay(**arguments, trade_sample=case["trade_sample"], run_dir=staging/name)
            runs[name] = load_estimates(staging/name/"manifest.json", expected_counts=case["observation_counts"],
                                       expected_inputs=case["engine_inputs"], require_resources=True)
            if name == "historic_filtered":
                historical_reproduction = verify_historical_reproduction(archived, runs[name])
                write_json(staging/"historical_reproduction.json", historical_reproduction)
            output_bytes = sum(path.stat().st_size for path in staging.rglob("*") if path.is_file())
            require(output_bytes <= CAPS["output_bytes"] and free_bytes(staging) >= CAPS["free_reserve_bytes"], "Output/disk cap exceeded")
            print(json.dumps({"completed_case": name, "output_bytes": output_bytes}), flush=True)
        require(historical_reproduction is not None and historical_reproduction["verified"] is True,
                "Historical estimate reproduction not verified")
        comparison = build_comparison(runs, flags, {**samples, "historical_estimate_reproduction": True})
        comparison["archived_baseline"] = archived["manifest_fingerprint"]
        comparison["historical_reproduction"] = historical_reproduction
        for name in CASES:
            comparison["cases"][name]["manifest"]["path"] = f"{name}/manifest.json"
        write_json(staging/"comparison.json", comparison)
        verify_case_inputs(current, content=True)
        require(committed_source(expected_head) == current["source"], "Source changed during estimation")
        result = {"schema_version": "sports_wallet_adoption_estimates_v1", "status": "estimates_complete",
            "data_certified": False, "source": current["source"], "inputs": current["controls"],
            "caps": CAPS, "numerical_tolerance": TOLERANCE,
            "command": command or [], "wall_seconds": time.monotonic()-started,
            "environment": {"python": sys.version, "duckdb": duckdb.__version__},
            "outputs": {name: {**fingerprint(staging/name), "path": name}
                        for name in ("comparison.json", "historical_reproduction.json")},
            "case_manifests": {name: {**fingerprint(staging/name/"manifest.json"), "path": f"{name}/manifest.json"} for name in CASES},
            "historical_baseline": fingerprint(historical_path), "gates": comparison["gates"]}
        write_json(staging/"manifest.json", result)
        require(sum(path.stat().st_size for path in staging.rglob("*") if path.is_file()) <= CAPS["output_bytes"] and
                free_bytes(staging) >= CAPS["free_reserve_bytes"], "Final output/disk cap exceeded")
        require(read_json(staging/"comparison.json") == comparison and read_json(staging/"manifest.json") == result, "Saved JSON reopen differs")
        atomic_publish(staging, target)
        return result
    except BaseException as error:
        write_json(staging/"failure.json", {"status": "estimation_failed", "error": str(error),
                   "source": current["source"], "command": command or [], "wall_seconds": time.monotonic()-started})
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("sample-manifest", "flag-manifest", "historical-estimates", "run-dir", "expected-head"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--preflight-dir")
    parser.add_argument("--reviewed-preflight")
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args()
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "Production requires canonical venv")
    paths = [Path(value).resolve() for value in (args.sample_manifest, args.flag_manifest, args.historical_estimates, args.run_dir)]
    if args.preflight_only:
        require(args.preflight_dir and not args.reviewed_preflight and not args.reviewed_preflight_sha256,
                "Metadata-only mode requires separate --preflight-dir and no body flags")
        value = preflight(*paths, args.expected_head)
        destination = Path(args.preflight_dir).resolve()
        require(not destination.exists(), "Immutable preflight target exists")
        require(destination != paths[-1] and destination not in paths[-1].parents and paths[-1] not in destination.parents,
                "Preflight/body targets overlap")
        for path in paths[:-1]:
            require(destination != path and destination not in path.parents and path not in destination.parents,
                    "Preflight target overlaps control input")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
        write_json(staging/"summary.json", value)
        atomic_publish(staging, destination)
    else:
        require(not args.preflight_dir, "Body does not accept --preflight-dir")
        require(args.reviewed_preflight and args.reviewed_preflight_sha256, "Body requires reviewed preflight binding")
        reviewed_path = Path(args.reviewed_preflight).resolve()
        require(sha256(reviewed_path) == args.reviewed_preflight_sha256, "Reviewed preflight hash differs")
        value = body(*paths, args.expected_head, read_json(reviewed_path), sys.argv)
    print(json.dumps({"status": value["status"], "run_dir": str(paths[-1])}))


if __name__ == "__main__":
    main()
