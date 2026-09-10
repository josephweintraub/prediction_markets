#!/usr/bin/env python3
"""Render the audited MLB fixed-bin FLB artifacts as standalone HTML.

This module is deliberately presentation-only.  It validates and displays
published Parquet/JSON results; it never estimates calibration or FLB effects.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Iterable

import duckdb


MIN_CELL_N = 50

GAME_CLOSE_PROVENANCE_TYPES = {
    "market_id": "VARCHAR",
    "game_pk": "BIGINT",
    "primary_has_close": "BOOLEAN",
    "sensitivity_has_close": "BOOLEAN",
    "sensitivity_counterparty_is_flagged_bot": "BOOLEAN",
}

CLOSING_SCHEMA = (
    ("close_definition", "VARCHAR"),
    ("profile_scope", "VARCHAR"),
    ("price_decile", "BIGINT"),
    ("price_bin", "VARCHAR"),
    ("game_count", "BIGINT"),
    ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
    ("mean_probability", "DOUBLE"),
    ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"),
    ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"),
    ("calibration_ci95_high", "DOUBLE"),
    ("brier_score", "DOUBLE"),
)

PAIRED_SCHEMA = (
    ("profile_scope", "VARCHAR"),
    ("primary_price_decile", "BIGINT"),
    ("primary_price_bin", "VARCHAR"),
    ("common_games", "BIGINT"),
    ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
    ("same_close_event_games", "BIGINT"),
    ("different_close_event_games", "BIGINT"),
    ("same_close_timestamp_games", "BIGINT"),
    ("different_close_timestamp_games", "BIGINT"),
    ("primary_mean_probability", "DOUBLE"),
    ("sensitivity_mean_probability", "DOUBLE"),
    ("mean_probability_difference", "DOUBLE"),
    ("mean_absolute_probability_difference", "DOUBLE"),
    ("primary_mean_calibration", "DOUBLE"),
    ("sensitivity_mean_calibration", "DOUBLE"),
    ("mean_calibration_difference", "DOUBLE"),
    ("primary_brier_score", "DOUBLE"),
    ("sensitivity_brier_score", "DOUBLE"),
    ("mean_brier_difference", "DOUBLE"),
)

TRADE_SCHEMA = (
    ("boundary_sample", "VARCHAR"),
    ("phase", "VARCHAR"),
    ("phase_order", "INTEGER"),
    ("price_decile", "BIGINT"),
    ("price_bin", "VARCHAR"),
    ("trade_count", "BIGINT"),
    ("game_count", "BIGINT"),
    ("dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
    ("mean_price", "DOUBLE"),
    ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"),
    ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"),
    ("calibration_ci95_high", "DOUBLE"),
)

TAIL_SCHEMA = (
    ("analysis_scope", "VARCHAR"),
    ("close_definition", "VARCHAR"),
    ("boundary_sample", "VARCHAR"),
    ("phase", "VARCHAR"),
    ("d1_n", "BIGINT"),
    ("d1_games", "BIGINT"),
    ("d1_dollars", "DOUBLE"),
    ("d10_n", "BIGINT"),
    ("d10_games", "BIGINT"),
    ("d10_dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"),
    ("status", "VARCHAR"),
    ("point_pattern", "VARCHAR"),
    ("d1_mean_probability", "DOUBLE"),
    ("d1_win_rate", "DOUBLE"),
    ("d1_mean_calibration", "DOUBLE"),
    ("d1_calibration_se", "DOUBLE"),
    ("d1_calibration_ci95_low", "DOUBLE"),
    ("d1_calibration_ci95_high", "DOUBLE"),
    ("d10_mean_probability", "DOUBLE"),
    ("d10_win_rate", "DOUBLE"),
    ("d10_mean_calibration", "DOUBLE"),
    ("d10_calibration_se", "DOUBLE"),
    ("d10_calibration_ci95_low", "DOUBLE"),
    ("d10_calibration_ci95_high", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"),
    ("spread_se", "DOUBLE"),
    ("spread_ci95_low", "DOUBLE"),
    ("spread_ci95_high", "DOUBLE"),
)

PHASES = (
    ("pregame", 1, "Pregame"),
    ("innings_1_3", 2, "Innings 1–3"),
    ("innings_4_6", 3, "Innings 4–6"),
    ("innings_7_plus", 4, "Innings 7+"),
)
BOUNDARY_SAMPLES = ("literal", "exclude_within_30s")
CLOSE_DEFINITIONS = ("primary", "sensitivity")

STAGE09_TOP_LEVEL = {
    "schema_version",
    "method",
    "inputs",
    "source_estimator",
    "definitions",
    "counts",
    "reconciliation",
    "outputs",
}
STAGE09_INPUT_KEYS = {
    "closing_calibration",
    "trade_phase_calibration",
    "estimator_summary",
    "game_closes",
    "phase_trades",
}
STAGE09_DEFINITION_KEYS = {
    "calibration_error",
    "fixed_bins",
    "d1",
    "d10",
    "spread",
    "classic_flb",
    "closing_weighting",
    "phase_weighting",
    "closing_uncertainty",
    "phase_uncertainty",
    "boundary_samples",
    "close_definitions",
    "suppression",
    "point_pattern",
}
STAGE09_COUNT_KEYS = {
    "closing_profile_rows",
    "trade_phase_profile_rows",
    "tail_summary_rows",
    "closing_tail_rows",
    "trade_phase_tail_rows",
    "reported_tail_rows",
    "suppressed_tail_rows",
}
STAGE09_RECONCILIATION_KEYS = {
    "source_fingerprints_match_stage08",
    "closing_profile_grid_complete",
    "trade_phase_profile_grid_complete",
    "closing_profile_cells_recomputed",
    "trade_phase_profile_cells_recomputed",
    "tail_support_matches_profiles",
    "tail_rows_partition_expected_grid",
    "suppression_is_fail_closed",
    "serialized_output_verified",
}


class FlbReportError(RuntimeError):
    """Raised when report inputs do not satisfy the frozen contracts."""


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FlbReportError(f"Missing input: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FlbReportError(f"Invalid JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FlbReportError(f"JSON input must be an object: {path}")
    return value


def _require_keys(
    value: dict[str, Any], required: set[str], label: str, *, exact: bool = False
) -> None:
    actual = set(value)
    missing = sorted(required - actual)
    extra = sorted(actual - required) if exact else []
    if missing or extra:
        raise FlbReportError(f"{label} keys mismatch; missing={missing}, extra={extra}")


def _read_rows(con: duckdb.DuckDBPyConnection, path: Path) -> list[dict[str, Any]]:
    escaped = str(path).replace("'", "''")
    cursor = con.execute(f"SELECT * FROM read_parquet('{escaped}')")
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _describe(con: duckdb.DuckDBPyConnection, path: Path) -> tuple[tuple[str, str], ...]:
    escaped = str(path).replace("'", "''")
    return tuple(
        (row[0], row[1])
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{escaped}')").fetchall()
    )


def _require_exact_schema(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    expected: tuple[tuple[str, str], ...],
    label: str,
) -> None:
    if not path.is_file():
        raise FlbReportError(f"Missing input: {path}")
    actual = _describe(con, path)
    if actual != expected:
        raise FlbReportError(f"{label} schema mismatch; expected={expected}, actual={actual}")


def _require_provenance_schema(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    if not path.is_file():
        raise FlbReportError(f"Missing input: {path}")
    actual = dict(_describe(con, path))
    missing = sorted(set(GAME_CLOSE_PROVENANCE_TYPES) - set(actual))
    mismatches = {
        name: {"expected": expected, "actual": actual.get(name)}
        for name, expected in GAME_CLOSE_PROVENANCE_TYPES.items()
        if name in actual and actual[name] != expected
    }
    if missing or mismatches:
        raise FlbReportError(
            f"game_closes provenance schema mismatch; missing={missing}, "
            f"type_mismatches={mismatches}"
        )


def _same_declared_file(path: Path, declaration: Any, label: str) -> None:
    if not isinstance(declaration, dict):
        raise FlbReportError(f"{label} fingerprint must be an object")
    _require_keys(declaration, {"path", "bytes", "sha256"}, f"{label} fingerprint")
    actual = _fingerprint(path)
    if actual["bytes"] != declaration["bytes"] or actual["sha256"] != declaration["sha256"]:
        raise FlbReportError(
            f"{label} fingerprint mismatch; expected bytes/hash "
            f"{declaration.get('bytes')}/{declaration.get('sha256')}, "
            f"actual {actual['bytes']}/{actual['sha256']}"
        )


def _same_fingerprint(a: Any, b: Any, label: str) -> None:
    for value in (a, b):
        if not isinstance(value, dict):
            raise FlbReportError(f"{label} fingerprints must be objects")
        _require_keys(value, {"path", "bytes", "sha256"}, f"{label} fingerprint")
    if a["bytes"] != b["bytes"] or a["sha256"] != b["sha256"]:
        raise FlbReportError(f"{label} fingerprints disagree")


def _require_unique(rows: list[dict[str, Any]], columns: tuple[str, ...], label: str) -> None:
    keys = [tuple(row[column] for column in columns) for row in rows]
    if len(keys) != len(set(keys)):
        raise FlbReportError(f"{label} does not have unique grain {columns}")


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _near(a: Any, b: Any, tolerance: float = 1e-11) -> bool:
    return _finite(a) and _finite(b) and math.isclose(float(a), float(b), rel_tol=tolerance, abs_tol=tolerance)


def _bin_label(decile: int) -> str:
    closing = "]" if decile == 10 else ")"
    return f"[{(decile - 1) / 10:.1f},{decile / 10:.1f}{closing}"


def _validate_estimate_row(
    row: dict[str, Any],
    count_column: str,
    metric_columns: tuple[str, ...],
    label: str,
) -> None:
    count = row[count_column]
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise FlbReportError(f"{label} has invalid {count_column}: {count!r}")
    suppressed = count < MIN_CELL_N
    expected_status = "suppressed_n_lt_50" if suppressed else "reported"
    if row["suppressed"] is not suppressed or row["status"] != expected_status:
        raise FlbReportError(f"{label} has inconsistent suppression/status")
    values = [row[column] for column in metric_columns]
    if suppressed and any(value is not None for value in values):
        raise FlbReportError(f"{label} leaks estimate fields from a suppressed cell")
    if not suppressed and any(not _finite(value) for value in values):
        raise FlbReportError(f"{label} has missing or nonfinite reported estimates")


def _validate_closing(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    if len(rows) != 22:
        raise FlbReportError(f"closing_calibration must contain 22 rows, found {len(rows)}")
    _require_unique(rows, ("close_definition", "profile_scope", "price_decile"), "closing_calibration")
    expected = {
        (definition, "overall", None) for definition in CLOSE_DEFINITIONS
    } | {
        (definition, "price_decile", decile)
        for definition in CLOSE_DEFINITIONS
        for decile in range(1, 11)
    }
    actual = {(row["close_definition"], row["profile_scope"], row["price_decile"]) for row in rows}
    if actual != expected:
        raise FlbReportError("closing_calibration fixed grid is incomplete")
    metrics = (
        "mean_probability",
        "win_rate",
        "mean_calibration",
        "calibration_se",
        "calibration_ci95_low",
        "calibration_ci95_high",
        "brier_score",
    )
    coverage = summary["counts"]["closing_coverage"]
    for row in rows:
        label = f"closing {row['close_definition']}/{row['price_bin']}"
        _validate_estimate_row(row, "game_count", metrics, label)
        decile = row["price_decile"]
        if decile is None:
            if row["price_bin"] != "overall":
                raise FlbReportError(f"{label} has invalid overall label")
        elif row["price_bin"] != _bin_label(int(decile)):
            raise FlbReportError(f"{label} has invalid bin label")
        if not row["suppressed"]:
            if not (0 <= row["mean_probability"] <= 1 and 0 <= row["win_rate"] <= 1):
                raise FlbReportError(f"{label} has probability outside [0,1]")
            if row["calibration_se"] < 0 or not (0 <= row["brier_score"] <= 1):
                raise FlbReportError(f"{label} has invalid uncertainty/Brier value")
            if not _near(row["mean_calibration"], row["win_rate"] - row["mean_probability"]):
                raise FlbReportError(f"{label} calibration identity fails")
            if not _near(row["calibration_ci95_low"], row["mean_calibration"] - 1.96 * row["calibration_se"]):
                raise FlbReportError(f"{label} lower interval identity fails")
            if not _near(row["calibration_ci95_high"], row["mean_calibration"] + 1.96 * row["calibration_se"]):
                raise FlbReportError(f"{label} upper interval identity fails")
    for definition in CLOSE_DEFINITIONS:
        overall = next(
            row for row in rows if row["close_definition"] == definition and row["profile_scope"] == "overall"
        )
        binned = [
            row for row in rows if row["close_definition"] == definition and row["profile_scope"] == "price_decile"
        ]
        if sum(row["game_count"] for row in binned) != overall["game_count"]:
            raise FlbReportError(f"{definition} closing bin counts do not reconcile")
        expected_count = coverage[f"{definition}_coverage"]
        if overall["game_count"] != expected_count:
            raise FlbReportError(f"{definition} overall count disagrees with estimator summary")


def _validate_paired(rows: list[dict[str, Any]], summary: dict[str, Any], stage07: dict[str, Any]) -> None:
    if len(rows) != 11:
        raise FlbReportError(f"closing_paired_sensitivity must contain 11 rows, found {len(rows)}")
    _require_unique(rows, ("profile_scope", "primary_price_decile"), "closing_paired_sensitivity")
    expected = {("overall", None)} | {("primary_price_decile", decile) for decile in range(1, 11)}
    actual = {(row["profile_scope"], row["primary_price_decile"]) for row in rows}
    if actual != expected:
        raise FlbReportError("closing_paired_sensitivity fixed grid is incomplete")
    count_columns = (
        "same_close_event_games",
        "different_close_event_games",
        "same_close_timestamp_games",
        "different_close_timestamp_games",
    )
    metrics = (
        "primary_mean_probability",
        "sensitivity_mean_probability",
        "mean_probability_difference",
        "mean_absolute_probability_difference",
        "primary_mean_calibration",
        "sensitivity_mean_calibration",
        "mean_calibration_difference",
        "primary_brier_score",
        "sensitivity_brier_score",
        "mean_brier_difference",
    )
    for row in rows:
        label = f"paired {row['primary_price_bin']}"
        _validate_estimate_row(row, "common_games", metrics, label)
        if any(not isinstance(row[column], int) or row[column] < 0 for column in count_columns):
            raise FlbReportError(f"{label} has invalid identity counts")
        if row["same_close_event_games"] + row["different_close_event_games"] != row["common_games"]:
            raise FlbReportError(f"{label} event counts do not partition common games")
        if row["same_close_timestamp_games"] + row["different_close_timestamp_games"] != row["common_games"]:
            raise FlbReportError(f"{label} timestamp counts do not partition common games")
        decile = row["primary_price_decile"]
        if decile is None:
            if row["primary_price_bin"] != "overall":
                raise FlbReportError(f"{label} has invalid overall label")
        elif row["primary_price_bin"] != _bin_label(int(decile)):
            raise FlbReportError(f"{label} has invalid bin label")
        if not row["suppressed"]:
            if not _near(row["mean_calibration_difference"], -row["mean_probability_difference"]):
                raise FlbReportError(f"{label} A-minus-C calibration sign is inconsistent")
            if not _near(
                row["mean_brier_difference"],
                row["primary_brier_score"] - row["sensitivity_brier_score"],
            ):
                raise FlbReportError(f"{label} A-minus-C Brier identity fails")
            if row["mean_absolute_probability_difference"] + 1e-12 < abs(row["mean_probability_difference"]):
                raise FlbReportError(f"{label} absolute probability difference is invalid")
    overall = next(row for row in rows if row["profile_scope"] == "overall")
    binned = [row for row in rows if row["profile_scope"] == "primary_price_decile"]
    if sum(row["common_games"] for row in binned) != overall["common_games"]:
        raise FlbReportError("Paired bin counts do not reconcile")
    if overall["common_games"] != stage07["counts"]["both_closes"]:
        raise FlbReportError("Paired common-game count disagrees with stage-07 reconciliation")
    if overall["same_close_event_games"] != stage07["counts"]["same_close_identity"]:
        raise FlbReportError("Paired same-event count disagrees with stage-07 reconciliation")
    if overall["different_close_event_games"] != stage07["counts"]["different_close_identity"]:
        raise FlbReportError("Paired different-event count disagrees with stage-07 reconciliation")


def _validate_trade(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    if len(rows) != 80:
        raise FlbReportError(f"trade_phase_calibration must contain 80 rows, found {len(rows)}")
    _require_unique(rows, ("boundary_sample", "phase", "price_decile"), "trade_phase_calibration")
    expected = {
        (sample, phase, decile)
        for sample in BOUNDARY_SAMPLES
        for phase, _, _ in PHASES
        for decile in range(1, 11)
    }
    actual = {(row["boundary_sample"], row["phase"], row["price_decile"]) for row in rows}
    if actual != expected:
        raise FlbReportError("trade_phase_calibration fixed grid is incomplete")
    phase_orders = {phase: order for phase, order, _ in PHASES}
    metrics = (
        "mean_price",
        "win_rate",
        "mean_calibration",
        "calibration_se",
        "calibration_ci95_low",
        "calibration_ci95_high",
    )
    by_key = {}
    for row in rows:
        label = f"trade {row['boundary_sample']}/{row['phase']}/{row['price_bin']}"
        _validate_estimate_row(row, "trade_count", metrics, label)
        if row["phase_order"] != phase_orders[row["phase"]]:
            raise FlbReportError(f"{label} has invalid phase order")
        if row["price_bin"] != _bin_label(int(row["price_decile"])):
            raise FlbReportError(f"{label} has invalid bin label")
        if row["game_count"] < 0 or row["game_count"] > row["trade_count"]:
            raise FlbReportError(f"{label} has invalid game count")
        if not _finite(row["dollars"]) or row["dollars"] < 0:
            raise FlbReportError(f"{label} has invalid dollars")
        if not row["suppressed"]:
            if not (0 <= row["mean_price"] <= 1 and 0 <= row["win_rate"] <= 1):
                raise FlbReportError(f"{label} has probability outside [0,1]")
            if row["calibration_se"] < 0:
                raise FlbReportError(f"{label} has negative standard error")
            if not _near(row["mean_calibration"], row["win_rate"] - row["mean_price"]):
                raise FlbReportError(f"{label} calibration identity fails")
            if not _near(row["calibration_ci95_low"], row["mean_calibration"] - 1.96 * row["calibration_se"]):
                raise FlbReportError(f"{label} lower interval identity fails")
            if not _near(row["calibration_ci95_high"], row["mean_calibration"] + 1.96 * row["calibration_se"]):
                raise FlbReportError(f"{label} upper interval identity fails")
        by_key[(row["boundary_sample"], row["phase"], row["price_decile"])] = row
    for phase, _, _ in PHASES:
        for decile in range(1, 11):
            literal = by_key[("literal", phase, decile)]
            excluded = by_key[("exclude_within_30s", phase, decile)]
            if excluded["trade_count"] > literal["trade_count"]:
                raise FlbReportError("30-second sample is not a trade-count subset")
            if excluded["game_count"] > literal["game_count"]:
                raise FlbReportError("30-second sample is not a game-count subset")
            if excluded["dollars"] > literal["dollars"] + 1e-7:
                raise FlbReportError("30-second sample is not a dollar subset")
    recorded = {
        (item["boundary_sample"], item["phase"]): item
        for item in summary["counts"]["trade_samples"]
    }
    if set(recorded) != {(sample, phase) for sample in BOUNDARY_SAMPLES for phase, _, _ in PHASES}:
        raise FlbReportError("Estimator summary trade-sample grid is incomplete")
    for sample in BOUNDARY_SAMPLES:
        for phase, _, _ in PHASES:
            cells = [row for row in rows if row["boundary_sample"] == sample and row["phase"] == phase]
            audit = recorded[(sample, phase)]
            if sum(row["trade_count"] for row in cells) != audit["trade_rows"]:
                raise FlbReportError("Trade cell counts disagree with estimator summary")
            if not math.isclose(
                sum(float(row["dollars"]) for row in cells),
                float(audit["dollars"]),
                rel_tol=1e-11,
                abs_tol=1e-3,
            ):
                raise FlbReportError("Trade cell dollars disagree with estimator summary")


def _tail_pattern(d1: float, d10: float) -> str:
    if d1 < 0 and d10 > 0:
        return "classic_flb_signs"
    if d1 > 0 and d10 < 0:
        return "reverse_flb_signs"
    if d1 > 0 and d10 > 0:
        return "both_positive"
    if d1 < 0 and d10 < 0:
        return "both_negative"
    return "mixed_or_zero"


def _validate_tail(
    rows: list[dict[str, Any]],
    closing: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    stage09: dict[str, Any],
) -> None:
    if len(rows) != 10:
        raise FlbReportError(f"flb_tail_summary must contain 10 rows, found {len(rows)}")
    key = ("analysis_scope", "close_definition", "boundary_sample", "phase")
    _require_unique(rows, key, "flb_tail_summary")
    expected = {
        ("closing", definition, None, "pregame_close")
        for definition in CLOSE_DEFINITIONS
    } | {
        ("trade_phase", None, sample, phase)
        for sample in BOUNDARY_SAMPLES
        for phase, _, _ in PHASES
    }
    actual = {tuple(row[column] for column in key) for row in rows}
    if actual != expected:
        raise FlbReportError("flb_tail_summary expected grid is incomplete")
    estimate_columns = tuple(name for name, _ in TAIL_SCHEMA[13:])
    for row in rows:
        label = "/".join(str(row[column]) for column in key)
        for column in ("d1_n", "d1_games", "d10_n", "d10_games"):
            if not isinstance(row[column], int) or row[column] < 0:
                raise FlbReportError(f"Tail row {label} has invalid {column}")
        for column in ("d1_dollars", "d10_dollars"):
            if not _finite(row[column]) or row[column] < 0:
                raise FlbReportError(f"Tail row {label} has invalid {column}")
        suppressed = row["d1_n"] < MIN_CELL_N or row["d10_n"] < MIN_CELL_N
        if suppressed:
            if row["suppressed"] is not True or row["status"] != "suppressed_tail_n_lt_50":
                raise FlbReportError(f"Tail row {label} has inconsistent suppression")
            if row["point_pattern"] != "suppressed":
                raise FlbReportError(f"Tail row {label} must suppress point pattern")
            if any(row[column] is not None for column in estimate_columns):
                raise FlbReportError(f"Tail row {label} leaks suppressed estimates")
        else:
            if row["suppressed"] is not False or row["status"] != "reported":
                raise FlbReportError(f"Tail row {label} has inconsistent reported status")
            if any(not _finite(row[column]) for column in estimate_columns):
                raise FlbReportError(f"Tail row {label} has missing/nonfinite estimates")
            if row["point_pattern"] != _tail_pattern(
                row["d1_mean_calibration"], row["d10_mean_calibration"]
            ):
                raise FlbReportError(f"Tail row {label} has invalid point pattern")
            if not _near(
                row["spread_d10_minus_d1"],
                row["d10_mean_calibration"] - row["d1_mean_calibration"],
            ):
                raise FlbReportError(f"Tail row {label} has invalid D10-minus-D1 spread")
            if row["spread_se"] < 0:
                raise FlbReportError(f"Tail row {label} has negative spread SE")
            if not _near(row["spread_ci95_low"], row["spread_d10_minus_d1"] - 1.96 * row["spread_se"]):
                raise FlbReportError(f"Tail row {label} has invalid lower spread interval")
            if not _near(row["spread_ci95_high"], row["spread_d10_minus_d1"] + 1.96 * row["spread_se"]):
                raise FlbReportError(f"Tail row {label} has invalid upper spread interval")
        if row["analysis_scope"] == "closing":
            source = {
                decile: next(
                    item
                    for item in closing
                    if item["close_definition"] == row["close_definition"]
                    and item["profile_scope"] == "price_decile"
                    and item["price_decile"] == decile
                )
                for decile in (1, 10)
            }
            for prefix, decile in (("d1", 1), ("d10", 10)):
                if row[f"{prefix}_n"] != source[decile]["game_count"]:
                    raise FlbReportError(f"Tail row {label} support disagrees with closing profile")
                if row[f"{prefix}_games"] != source[decile]["game_count"]:
                    raise FlbReportError(f"Tail row {label} game support disagrees with closing profile")
        else:
            source = {
                decile: next(
                    item
                    for item in trades
                    if item["boundary_sample"] == row["boundary_sample"]
                    and item["phase"] == row["phase"]
                    and item["price_decile"] == decile
                )
                for decile in (1, 10)
            }
            for prefix, decile in (("d1", 1), ("d10", 10)):
                if row[f"{prefix}_n"] != source[decile]["trade_count"]:
                    raise FlbReportError(f"Tail row {label} support disagrees with trade profile")
                if row[f"{prefix}_games"] != source[decile]["game_count"]:
                    raise FlbReportError(f"Tail row {label} game support disagrees with trade profile")
                if not _near(row[f"{prefix}_dollars"], source[decile]["dollars"], 1e-10):
                    raise FlbReportError(f"Tail row {label} dollars disagree with trade profile")
        if not suppressed:
            for prefix, decile in (("d1", 1), ("d10", 10)):
                source_row = source[decile]
                source_probability = (
                    source_row["mean_probability"]
                    if row["analysis_scope"] == "closing"
                    else source_row["mean_price"]
                )
                comparisons = {
                    f"{prefix}_mean_probability": source_probability,
                    f"{prefix}_win_rate": source_row["win_rate"],
                    f"{prefix}_mean_calibration": source_row["mean_calibration"],
                    f"{prefix}_calibration_se": source_row["calibration_se"],
                    f"{prefix}_calibration_ci95_low": source_row["calibration_ci95_low"],
                    f"{prefix}_calibration_ci95_high": source_row["calibration_ci95_high"],
                }
                if any(not _near(row[column], value) for column, value in comparisons.items()):
                    raise FlbReportError(f"Tail row {label} estimates disagree with source profiles")
    counts = stage09["counts"]
    reported = sum(not row["suppressed"] for row in rows)
    if counts != {
        "closing_profile_rows": 22,
        "trade_phase_profile_rows": 80,
        "tail_summary_rows": 10,
        "closing_tail_rows": 2,
        "trade_phase_tail_rows": 8,
        "reported_tail_rows": reported,
        "suppressed_tail_rows": 10 - reported,
    }:
        raise FlbReportError("Stage-09 summary counts disagree with tail artifact")


def _validate_stage07(
    con: duckdb.DuckDBPyConnection,
    game_closes: Path,
    reconciliation: dict[str, Any],
) -> None:
    _require_keys(reconciliation, {"schema_version", "method", "counts", "definitions", "inputs", "outputs", "reconciliation", "missing_reasons"}, "stage-07 reconciliation")
    if reconciliation["schema_version"] != 1 or reconciliation["method"] != "exact_polygon_dual_pregame_close":
        raise FlbReportError("Unexpected stage-07 schema version or method")
    required_counts = {
        "eligible_games",
        "primary_closes",
        "primary_missing",
        "sensitivity_closes",
        "sensitivity_missing",
        "both_closes",
        "primary_only",
        "sensitivity_only",
        "same_close_identity",
        "different_close_identity",
        "sensitivity_closes_with_flagged_bot_counterparty",
        "raw_candidate_rows",
        "distinct_fills",
        "duplicate_ingestion_replays",
        "source_distinct_blocks",
    }
    _require_keys(reconciliation["counts"], required_counts, "stage-07 counts")
    if reconciliation["definitions"].get("timestamp_fallback_rows") != 0:
        raise FlbReportError("Stage-07 reconciliation reports timestamp fallback rows")
    expected_bot_semantics = (
        "only the outcome-token buyer is filtered; a flagged counterparty does not exclude a fill"
    )
    if reconciliation["definitions"].get("bot_semantics") != expected_bot_semantics:
        raise FlbReportError("Stage-07 bot semantics do not match the frozen buyer-only rule")
    gates = reconciliation["reconciliation"]
    if not gates or any(value is not True for value in gates.values()):
        raise FlbReportError("A stage-07 reconciliation gate is not true")
    provenance = reconciliation.get("inputs", {}).get("timestamp_provenance_validation", {})
    metadata = provenance.get("build_metadata", {})
    if (
        provenance.get("status") != "passed"
        or provenance.get("scope") != "cache_declaration_only"
        or metadata.get("used_exact_cache") is not True
        or metadata.get("fallback_rows") != 0
        or metadata.get("missing_blocks") != 0
    ):
        raise FlbReportError("Stage-07 exact timestamp provenance is not audit-safe")
    if Path(reconciliation.get("outputs", {}).get("game_closes", "")).name != "game_closes.parquet":
        raise FlbReportError("Stage-07 game-closes output name is inconsistent")
    escaped = str(game_closes).replace("'", "''")
    counts = con.execute(
        f"""
        SELECT count(*)::BIGINT,
               count(DISTINCT market_id)::BIGINT,
               count(DISTINCT game_pk)::BIGINT,
               count(*) FILTER (WHERE primary_has_close)::BIGINT,
               count(*) FILTER (WHERE sensitivity_has_close)::BIGINT,
               count(*) FILTER (
                 WHERE sensitivity_has_close
                   AND sensitivity_counterparty_is_flagged_bot
               )::BIGINT
        FROM read_parquet('{escaped}')
        """
    ).fetchone()
    expected = reconciliation["counts"]
    if not (
        counts[0] == counts[1] == counts[2] == expected["eligible_games"]
        and counts[3] == expected["primary_closes"]
        and counts[4] == expected["sensitivity_closes"]
        and counts[5] == expected["sensitivity_closes_with_flagged_bot_counterparty"]
    ):
        raise FlbReportError("game_closes grain/coverage disagrees with stage-07 reconciliation")
    if expected["primary_closes"] + expected["primary_missing"] != expected["eligible_games"]:
        raise FlbReportError("Stage-07 primary close partition does not reconcile")
    if expected["sensitivity_closes"] + expected["sensitivity_missing"] != expected["eligible_games"]:
        raise FlbReportError("Stage-07 sensitivity close partition does not reconcile")
    if expected["both_closes"] + expected["primary_only"] != expected["primary_closes"]:
        raise FlbReportError("Stage-07 primary availability partition does not reconcile")
    if expected["both_closes"] + expected["sensitivity_only"] != expected["sensitivity_closes"]:
        raise FlbReportError("Stage-07 sensitivity availability partition does not reconcile")


def _validate_summaries(
    stage07: dict[str, Any],
    stage08: dict[str, Any],
    stage09: dict[str, Any],
    paths: dict[str, Path],
) -> None:
    _require_keys(stage08, {"schema_version", "method", "inputs", "definitions", "counts", "outputs", "interpretation_status"}, "estimator summary")
    if (
        stage08["schema_version"] != 1
        or stage08["method"] != "descriptive_fixed_width_calibration_v1"
        or stage08["interpretation_status"] != "exploratory_descriptive"
    ):
        raise FlbReportError("Unexpected stage-08 estimator identity/status")
    expected_outputs = {
        "closing_calibration": "closing_calibration.parquet",
        "closing_paired_sensitivity": "closing_paired_sensitivity.parquet",
        "trade_phase_calibration": "trade_phase_calibration.parquet",
        "summary": "estimator_summary.json",
    }
    if stage08["outputs"] != expected_outputs:
        raise FlbReportError("Stage-08 output names do not match the frozen contract")
    expected_definitions = {
        "calibration_error": "won - price",
        "price_decile": "least(floor(price * 10), 9) + 1",
        "close_timestamp_unit": "Unix seconds",
        "primary_close": "last raw pregame BUY fill with 0 < price < 1; no wallet filter",
        "close_sensitivity": (
            "last pregame BUY fill with 0.01 < price < 0.99 after buyer-bot exclusion"
        ),
        "paired_probability_difference": (
            "primary probability - sensitivity probability (A - C)"
        ),
        "paired_calibration_difference": (
            "primary calibration - sensitivity calibration (A - C)"
        ),
        "paired_brier_difference": (
            "primary Brier score - sensitivity Brier score (A - C)"
        ),
        "close_weighting": "one equal-weight observation per game",
        "trade_weighting": "one equal-weight observation per eligible BUY fill",
        "boundary_primary": "literal audited half-open phase boundaries",
        "boundary_sensitivity": (
            "exclude abs(trade timestamp - any boundary) <= 30 seconds"
        ),
        "suppression": "estimate fields null when n < 50; counts remain",
        "closing_se": (
            "one-way official-date clustered normal SE; 95% CI = estimate +/- 1.96 SE"
        ),
        "trade_se": (
            "existing CGM day x wallet x game clustered normal SE; "
            "95% CI = estimate +/- 1.96 SE"
        ),
    }
    if stage08["definitions"] != expected_definitions:
        raise FlbReportError("Stage-08 definitions do not match the frozen estimator contract")
    if stage08["counts"].get("output_rows") != {
        "closing_calibration": 22,
        "closing_paired_sensitivity": 11,
        "trade_phase_calibration": 80,
    }:
        raise FlbReportError("Stage-08 fixed output counts are inconsistent")
    _same_declared_file(paths["game_closes"], stage08["inputs"]["dual_closes"], "stage-08 dual closes")

    _require_keys(stage09, STAGE09_TOP_LEVEL, "stage-09 summary", exact=True)
    if stage09["schema_version"] != 1 or stage09["method"] != "mlb_fixed_bin_flb_tail_summary_v1":
        raise FlbReportError("Unexpected stage-09 schema version or method")
    _require_keys(stage09["inputs"], STAGE09_INPUT_KEYS, "stage-09 inputs", exact=True)
    _require_keys(stage09["definitions"], STAGE09_DEFINITION_KEYS, "stage-09 definitions", exact=True)
    _require_keys(stage09["counts"], STAGE09_COUNT_KEYS, "stage-09 counts", exact=True)
    _require_keys(
        stage09["reconciliation"],
        STAGE09_RECONCILIATION_KEYS,
        "stage-09 reconciliation",
        exact=True,
    )
    if any(value is not True for value in stage09["reconciliation"].values()):
        raise FlbReportError("A stage-09 reconciliation gate is not true")
    if stage09["outputs"] != {
        "flb_tail_summary": "flb_tail_summary.parquet",
        "summary": "flb_summary.json",
    }:
        raise FlbReportError("Stage-09 output names do not match the frozen contract")
    if stage09["source_estimator"] != {
        "schema_version": stage08["schema_version"],
        "method": stage08["method"],
        "interpretation_status": stage08["interpretation_status"],
    }:
        raise FlbReportError("Stage-09 source estimator does not match stage 08")
    for name in ("closing_calibration", "trade_phase_calibration", "estimator_summary", "game_closes"):
        _same_declared_file(paths[name], stage09["inputs"][name], f"stage-09 {name}")
    _same_fingerprint(
        stage09["inputs"]["phase_trades"],
        stage08["inputs"]["phase_trades"],
        "stage-08/stage-09 phase trades",
    )
    _same_fingerprint(
        stage09["inputs"]["game_closes"],
        stage08["inputs"]["dual_closes"],
        "stage-08/stage-09 game closes",
    )
    coverage = stage08["counts"].get("closing_coverage", {})
    counts = stage07["counts"]
    expected_coverage = {
        "games": counts["eligible_games"],
        "primary_coverage": counts["primary_closes"],
        "primary_missing": counts["primary_missing"],
        "sensitivity_coverage": counts["sensitivity_closes"],
        "sensitivity_missing": counts["sensitivity_missing"],
    }
    if any(coverage.get(name) != value for name, value in expected_coverage.items()):
        raise FlbReportError("Stage-08 close coverage disagrees with stage 07")


def _validate_paths(destination: Path, inputs: Iterable[Path]) -> None:
    destination = destination.resolve()
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if destination in dangerous:
        raise FlbReportError(f"Refusing dangerous report directory: {destination}")
    for path in inputs:
        resolved = path.resolve()
        if destination == resolved or destination in resolved.parents or resolved in destination.parents:
            raise FlbReportError("Report directory collides with an input path")
    if destination.exists():
        raise FileExistsError(f"Immutable report directory already exists: {destination}")


def _e(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _fmt_number(value: Any, digits: int = 6, *, signed: bool = False) -> str:
    if value is None:
        return "withheld"
    prefix = "+" if signed else ""
    return format(float(value), f"{prefix}.{digits}f")


def _fmt_count(value: Any) -> str:
    return f"{int(value):,}"


def _fmt_dollars(value: Any) -> str:
    return f"${float(value):,.2f}"


def _fmt_ci(low: Any, high: Any) -> str:
    if low is None or high is None:
        return "withheld"
    return f"[{float(low):+.6f}, {float(high):+.6f}]"


def _table(
    caption: str,
    headers: tuple[str, ...],
    rows: Iterable[tuple[Any, ...]],
) -> str:
    pieces = [
        '<div class="table-scroll" tabindex="0">',
        "<table>",
        f"<caption>{_e(caption)}</caption>",
        "<thead><tr>",
    ]
    pieces.extend(f'<th scope="col">{_e(header)}</th>' for header in headers)
    pieces.append("</tr></thead><tbody>")
    for row in rows:
        pieces.append("<tr>")
        for index, value in enumerate(row):
            tag = 'th scope="row"' if index == 0 else "td"
            pieces.append(f"<{tag}>{_e(value)}</{tag.split()[0]}>")
        pieces.append("</tr>")
    pieces.extend(("</tbody></table>", "</div>"))
    return "".join(pieces)


def _nice_limit(rows: list[dict[str, Any]]) -> float:
    values = [
        abs(float(row[column]))
        for row in rows
        if not row["suppressed"]
        for column in ("calibration_ci95_low", "calibration_ci95_high")
        if row[column] is not None
    ]
    maximum = max(values, default=0.05) * 1.08
    if maximum <= 0:
        return 0.05
    power = 10 ** math.floor(math.log10(maximum))
    scaled = maximum / power
    step = 1 if scaled <= 1 else 2 if scaled <= 2 else 5 if scaled <= 5 else 10
    return step * power


def _profile_svg(
    chart_id: str,
    title: str,
    description: str,
    series: tuple[tuple[str, str, str, list[dict[str, Any]]], ...],
) -> str:
    all_rows = [row for _, _, _, rows in series for row in rows]
    limit = _nice_limit(all_rows)
    width, height = 760, 350
    left, right, top, bottom = 72.0, 738.0, 48.0, 276.0
    plot_height = bottom - top

    def x(decile: int, offset: float) -> float:
        return left + (decile - 0.5) * (right - left) / 10 + offset

    def y(value: float) -> float:
        return top + (limit - value) * plot_height / (2 * limit)

    items = [
        f'<figure class="chart-card" tabindex="0"><svg role="img" aria-labelledby="{_e(chart_id)}-title {_e(chart_id)}-desc" viewBox="0 0 {width} {height}">',
        f'<title id="{_e(chart_id)}-title">{_e(title)}</title>',
        f'<desc id="{_e(chart_id)}-desc">{_e(description)}</desc>',
    ]
    for tick in (-limit, -limit / 2, 0.0, limit / 2, limit):
        yy = y(tick)
        css = "zero" if tick == 0 else "grid"
        items.append(f'<line class="{css}" x1="{left}" y1="{yy:.2f}" x2="{right}" y2="{yy:.2f}"/>')
        items.append(f'<text class="tick" x="{left - 9}" y="{yy + 4:.2f}" text-anchor="end">{tick:+.3f}</text>')
    for decile in range(1, 11):
        xx = x(decile, 0)
        items.append(f'<text class="tick" x="{xx:.2f}" y="{bottom + 22}" text-anchor="middle">D{decile}</text>')
    items.append(
        f'<text class="axis-label" x="{(left + right) / 2:.2f}" y="{bottom + 48}" text-anchor="middle">Fixed probability bin</text>'
    )
    items.append(
        f'<text class="axis-label" transform="translate(18 {(top + bottom) / 2:.2f}) rotate(-90)" text-anchor="middle">Calibration error (y − p)</text>'
    )
    offsets = (-6.0, 6.0) if len(series) == 2 else (0.0,)
    legend_x = left
    for index, (name, color, shape, rows) in enumerate(series):
        offset = offsets[index]
        for row in rows:
            if row["suppressed"]:
                continue
            xx = x(int(row["price_decile"]), offset)
            low = y(float(row["calibration_ci95_low"]))
            high = y(float(row["calibration_ci95_high"]))
            point = y(float(row["mean_calibration"]))
            items.append(
                f'<g data-status="reported"><line class="whisker" style="stroke:{color}" x1="{xx:.2f}" y1="{low:.2f}" x2="{xx:.2f}" y2="{high:.2f}"/>'
                f'<line class="whisker" style="stroke:{color}" x1="{xx - 4:.2f}" y1="{low:.2f}" x2="{xx + 4:.2f}" y2="{low:.2f}"/>'
                f'<line class="whisker" style="stroke:{color}" x1="{xx - 4:.2f}" y1="{high:.2f}" x2="{xx + 4:.2f}" y2="{high:.2f}"/>'
            )
            if shape == "circle":
                items.append(f'<circle cx="{xx:.2f}" cy="{point:.2f}" r="4.5" style="fill:{color}"/>')
            else:
                points = " ".join(
                    f"{px:.2f},{py:.2f}"
                    for px, py in ((xx, point - 5), (xx + 5, point), (xx, point + 5), (xx - 5, point))
                )
                items.append(f'<polygon points="{points}" style="fill:{color}"/>')
            items.append("</g>")
        lx = legend_x + index * 235
        items.append(f'<line x1="{lx}" y1="20" x2="{lx + 24}" y2="20" style="stroke:{color};stroke-width:2"/>')
        items.append(f'<text class="legend" x="{lx + 31}" y="24">{_e(name)}</text>')
    items.append("</svg>")
    items.append(
        "<figcaption>Points are published cell means; whiskers are nominal 95% intervals. "
        "Suppressed cells are omitted from the chart and retained in the table.</figcaption></figure>"
    )
    return "".join(items)


def _phase_table(phase_label: str, rows: list[dict[str, Any]]) -> str:
    ordered = sorted(
        rows,
        key=lambda row: (BOUNDARY_SAMPLES.index(row["boundary_sample"]), row["price_decile"]),
    )
    return _table(
        f"{phase_label}: complete fixed-bin profile",
        (
            "Boundary sample",
            "Bin",
            "Trades",
            "Games",
            "Dollars",
            "Mean price",
            "Win rate",
            "Mean y − p",
            "Nominal 95% interval",
            "Status",
        ),
        (
            (
                row["boundary_sample"],
                row["price_bin"],
                _fmt_count(row["trade_count"]),
                _fmt_count(row["game_count"]),
                _fmt_dollars(row["dollars"]),
                _fmt_number(row["mean_price"]),
                _fmt_number(row["win_rate"]),
                _fmt_number(row["mean_calibration"], signed=True),
                _fmt_ci(row["calibration_ci95_low"], row["calibration_ci95_high"]),
                row["status"],
            )
            for row in ordered
        ),
    )


def _phase_tail_table(
    phase_label: str, phase: str, rows: list[dict[str, Any]]
) -> str:
    selected = sorted(
        (
            row
            for row in rows
            if row["analysis_scope"] == "trade_phase" and row["phase"] == phase
        ),
        key=lambda row: BOUNDARY_SAMPLES.index(row["boundary_sample"]),
    )

    def status(row: dict[str, Any]) -> str:
        if not row["suppressed"]:
            return row["status"]
        thin_tails = []
        if row["d1_n"] < MIN_CELL_N:
            thin_tails.append(f"D1 n={row['d1_n']} (<{MIN_CELL_N})")
        if row["d10_n"] < MIN_CELL_N:
            thin_tails.append(f"D10 n={row['d10_n']} (<{MIN_CELL_N})")
        return f"{row['status']} because {' and '.join(thin_tails)}"

    sample_labels = {
        "literal": "Literal boundaries",
        "exclude_within_30s": "Exclude within ±30s",
    }
    return _table(
        f"{phase_label}: adjacent D1/D10 tail contrasts",
        (
            "Timing sample",
            "D1 n / games / dollars",
            "D1 y − p",
            "D10 n / games / dollars",
            "D10 y − p",
            "D10 − D1 / joint nominal 95% interval",
            "Point signs",
            "Status",
        ),
        (
            (
                sample_labels[row["boundary_sample"]],
                f"{_fmt_count(row['d1_n'])} / {_fmt_count(row['d1_games'])} / {_fmt_dollars(row['d1_dollars'])}",
                _fmt_number(row["d1_mean_calibration"], signed=True),
                f"{_fmt_count(row['d10_n'])} / {_fmt_count(row['d10_games'])} / {_fmt_dollars(row['d10_dollars'])}",
                _fmt_number(row["d10_mean_calibration"], signed=True),
                (
                    f"{_fmt_number(row['spread_d10_minus_d1'], signed=True)} "
                    f"{_fmt_ci(row['spread_ci95_low'], row['spread_ci95_high'])}"
                    if not row["suppressed"]
                    else "withheld"
                ),
                row["point_pattern"],
                status(row),
            )
            for row in selected
        ),
    )


def _closing_table(rows: list[dict[str, Any]], *, overall: bool) -> str:
    chosen = [row for row in rows if (row["profile_scope"] == "overall") is overall]
    chosen.sort(
        key=lambda row: (
            CLOSE_DEFINITIONS.index(row["close_definition"]),
            -1 if row["price_decile"] is None else row["price_decile"],
        )
    )
    caption = "Closing calibration: overall rows" if overall else "Closing calibration: complete fixed-bin profiles"
    return _table(
        caption,
        (
            "Close definition",
            "Bin",
            "Games",
            "Mean home probability",
            "Home win rate",
            "Mean y − p",
            "Nominal 95% interval",
            "Brier score",
            "Status",
        ),
        (
            (
                row["close_definition"],
                row["price_bin"],
                _fmt_count(row["game_count"]),
                _fmt_number(row["mean_probability"]),
                _fmt_number(row["win_rate"]),
                _fmt_number(row["mean_calibration"], signed=True),
                _fmt_ci(row["calibration_ci95_low"], row["calibration_ci95_high"]),
                _fmt_number(row["brier_score"]),
                row["status"],
            )
            for row in chosen
        ),
    )


def _paired_table(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: -1 if row["primary_price_decile"] is None else row["primary_price_decile"])
    return _table(
        "Paired closing sensitivity: primary A minus sensitivity C",
        (
            "Primary-A bin",
            "Common games",
            "Same EVM close",
            "Different EVM close",
            "A mean probability",
            "C mean probability",
            "A − C probability",
            "A − C calibration",
            "A − C Brier",
            "Status",
        ),
        (
            (
                row["primary_price_bin"],
                _fmt_count(row["common_games"]),
                _fmt_count(row["same_close_event_games"]),
                _fmt_count(row["different_close_event_games"]),
                _fmt_number(row["primary_mean_probability"]),
                _fmt_number(row["sensitivity_mean_probability"]),
                _fmt_number(row["mean_probability_difference"], signed=True),
                _fmt_number(row["mean_calibration_difference"], signed=True),
                _fmt_number(row["mean_brier_difference"], signed=True),
                row["status"],
            )
            for row in ordered
        ),
    )


def _tail_table(rows: list[dict[str, Any]]) -> str:
    phase_order = {phase: order for phase, order, _ in PHASES}
    ordered = sorted(
        rows,
        key=lambda row: (
            0 if row["analysis_scope"] == "closing" else 1,
            row["close_definition"] or "",
            BOUNDARY_SAMPLES.index(row["boundary_sample"]) if row["boundary_sample"] else -1,
            phase_order.get(row["phase"], 0),
        ),
    )
    return _table(
        "D1/D10 tail summary; point pattern describes signs only",
        (
            "Scope / definition",
            "Phase",
            "D1 n / games / dollars",
            "D1 y − p",
            "D10 n / games / dollars",
            "D10 y − p",
            "D10 − D1",
            "Nominal 95% interval",
            "Point-sign pattern",
            "Status",
        ),
        (
            (
                row["close_definition"]
                if row["analysis_scope"] == "closing"
                else row["boundary_sample"],
                row["phase"],
                f"{_fmt_count(row['d1_n'])} / {_fmt_count(row['d1_games'])} / {_fmt_dollars(row['d1_dollars'])}",
                _fmt_number(row["d1_mean_calibration"], signed=True),
                f"{_fmt_count(row['d10_n'])} / {_fmt_count(row['d10_games'])} / {_fmt_dollars(row['d10_dollars'])}",
                _fmt_number(row["d10_mean_calibration"], signed=True),
                _fmt_number(row["spread_d10_minus_d1"], signed=True),
                _fmt_ci(row["spread_ci95_low"], row["spread_ci95_high"]),
                row["point_pattern"],
                row["status"],
            )
            for row in ordered
        ),
    )


def _boundary_table(summary: dict[str, Any]) -> str:
    sample_rows = {
        (row["boundary_sample"], row["phase"]): row
        for row in summary["counts"]["trade_samples"]
    }
    output = []
    for phase, _, label in PHASES:
        literal = sample_rows[("literal", phase)]
        excluded = sample_rows[("exclude_within_30s", phase)]
        output.append(
            (
                label,
                _fmt_count(literal["trade_rows"]),
                _fmt_count(excluded["trade_rows"]),
                _fmt_count(literal["trade_rows"] - excluded["trade_rows"]),
                _fmt_dollars(literal["dollars"]),
                _fmt_dollars(excluded["dollars"]),
                _fmt_dollars(literal["dollars"] - excluded["dollars"]),
            )
        )
    return _table(
        "Literal and inclusive ±30-second boundary samples",
        (
            "Phase",
            "Literal trades",
            "Exclusion trades",
            "Rows removed",
            "Literal dollars",
            "Exclusion dollars",
            "Dollars removed",
        ),
        output,
    )


def _provenance_table(fingerprints: dict[str, dict[str, Any]]) -> str:
    return _table(
        "Report input fingerprints",
        ("Artifact", "Bytes", "SHA-256", "Path"),
        (
            (name, _fmt_count(value["bytes"]), value["sha256"], value["path"])
            for name, value in sorted(fingerprints.items())
        ),
    )


def _build_html(bundle: dict[str, Any]) -> str:
    closing = bundle["closing"]
    paired = bundle["paired"]
    trades = bundle["trades"]
    tails = bundle["tails"]
    stage07 = bundle["stage07"]
    stage08 = bundle["stage08"]
    stage09 = bundle["stage09"]
    fingerprints = bundle["fingerprints"]
    counts07 = stage07["counts"]
    phase_counts = stage08["counts"]["phase_input"]
    close_counts = stage08["counts"]["closing_coverage"]
    primary_overall = next(
        row for row in closing if row["close_definition"] == "primary" and row["profile_scope"] == "overall"
    )
    sensitivity_overall = next(
        row for row in closing if row["close_definition"] == "sensitivity" and row["profile_scope"] == "overall"
    )
    closing_bins = [row for row in closing if row["profile_scope"] == "price_decile"]
    closing_svg = _profile_svg(
        "closing-profile",
        "Closing calibration by fixed home-win probability bin",
        "Primary A and buyer-filtered sensitivity C. The complete numeric values and suppression statuses follow in a table.",
        (
            (
                "Primary A",
                "#006d77",
                "circle",
                [row for row in closing_bins if row["close_definition"] == "primary"],
            ),
            (
                "Sensitivity C",
                "#d97706",
                "diamond",
                [row for row in closing_bins if row["close_definition"] == "sensitivity"],
            ),
        ),
    )
    phase_sections = []
    for phase, _, label in PHASES:
        selected = [row for row in trades if row["phase"] == phase]
        phase_chart = _profile_svg(
                f"phase-{phase.replace('_', '-')}",
                f"{label} calibration by purchased-outcome probability bin",
                "Literal official boundaries and the sample excluding timestamps within 30 seconds inclusive of any boundary. Values are descriptive fixed-bin estimates.",
                (
                    (
                        "Literal boundaries",
                        "#006d77",
                        "circle",
                        [row for row in selected if row["boundary_sample"] == "literal"],
                    ),
                    (
                        "Exclude within ±30s",
                        "#d97706",
                        "diamond",
                        [row for row in selected if row["boundary_sample"] == "exclude_within_30s"],
                    ),
                ),
            )
        phase_sections.append(
            f'<article class="phase-card"><h3>{_e(label)}</h3>'
            f"{phase_chart}{_phase_table(label, selected)}"
            f"{_phase_tail_table(label, phase, tails)}</article>"
        )
    provenance_payload = {
        "report_method": "mlb_flb_self_contained_html_v1",
        "inputs": fingerprints,
        "stage07_reconciliation": stage07,
        "stage08_estimator_summary": stage08,
        "stage09_flb_summary": stage09,
    }
    provenance_json = html.escape(
        json.dumps(provenance_payload, indent=2, sort_keys=True), quote=False
    )
    close_only_sample = close_counts.get("close_only_game_sample", [])
    close_only_text = ", ".join(
        f"{item['market_id']} / MLB {item['game_pk']}" for item in close_only_sample
    ) or "none"
    body = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src 'unsafe-inline'; img-src data:">
<title>MLB moneyline game dynamics — exploratory fixed-bin FLB report</title>
<style>
:root{{--ink:#17212b;--muted:#52606d;--paper:#fff;--wash:#f5f7f9;--line:#cbd5df;--blue:#006d77;--orange:#d97706;--warning:#8a4b08}}
*{{box-sizing:border-box}} html{{scroll-behavior:auto}} body{{margin:0;background:var(--wash);color:var(--ink);font:16px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:1180px;margin:0 auto;background:var(--paper);padding:clamp(1rem,3vw,2.5rem)}}
h1{{font-size:clamp(1.65rem,4vw,2.4rem);line-height:1.15;margin:.25rem 0}} h2{{margin-top:2.5rem;border-bottom:2px solid var(--line);padding-bottom:.35rem}} h3{{margin-top:2rem}}
a{{color:#005b96}} a:focus-visible,.table-scroll:focus-visible{{outline:3px solid var(--orange);outline-offset:3px}}
.skip{{position:absolute;left:-9999px}} .skip:focus{{left:1rem;top:1rem;background:#fff;padding:.5rem;z-index:3}}
.status{{display:inline-block;background:#fff4df;color:#653400;border:1px solid #d97706;border-radius:999px;padding:.25rem .65rem;font-weight:700}}
.lede{{font-size:1.08rem;max-width:78ch}} .callout{{border-left:5px solid var(--blue);background:#eef8f8;padding:1rem;margin:1rem 0}} .caution{{border-left-color:var(--orange);background:#fff8eb}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,14rem),1fr));gap:.75rem;margin:1rem 0}} .card{{border:1px solid var(--line);border-radius:.5rem;padding:.85rem;background:#fff}} .card strong{{display:block;font-size:1.35rem}}
.chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,34rem),1fr));gap:1rem}} .phase-card{{min-width:0}} .chart-card{{margin:1rem 0;border:1px solid var(--line);border-radius:.5rem;padding:.65rem;background:#fff;overflow-x:auto}} svg{{display:block;width:100%;min-width:34rem;height:auto;overflow:visible}} svg .grid{{stroke:#d9e1e8;stroke-width:1}} svg .zero{{stroke:#17212b;stroke-width:1.5}} svg .whisker{{stroke-width:1.7}} svg .tick{{font-size:12px;fill:#394957}} svg .axis-label{{font-size:13px;font-weight:600;fill:#253642}} svg .legend{{font-size:12px;fill:#253642}} figcaption{{font-size:.9rem;color:var(--muted);margin:.4rem .3rem}}
.table-scroll{{max-width:100%;overflow-x:auto;margin:1rem 0;border:1px solid var(--line);border-radius:.4rem}} table{{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;font-size:.88rem}} caption{{text-align:left;font-weight:700;font-size:1rem;padding:.7rem;background:#edf2f6}} th,td{{padding:.5rem .6rem;border-bottom:1px solid #dde4ea;text-align:right;white-space:nowrap}} th:first-child,td:first-child{{text-align:left}} thead th{{background:#f7f9fb}} tbody tr:nth-child(even){{background:#fbfcfd}}
code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}} code{{display:inline-block;max-width:100%;font-size:.9em;overflow-wrap:anywhere;word-break:break-all;vertical-align:bottom}} pre{{white-space:pre-wrap;word-break:break-word;font-size:.76rem}} details{{margin:1rem 0}} .muted{{color:var(--muted)}}
@media(max-width:700px){{main{{padding:1rem}} .chart-card{{padding:.25rem}} th,td{{padding:.42rem;font-size:.8rem}} .lede{{font-size:1rem}}}}
@media print{{body{{background:#fff}} main{{max-width:none;padding:0}} nav,.skip{{display:none}} svg{{min-width:0}} .chart-card,.table-scroll{{break-inside:avoid}} .table-scroll{{overflow:visible}} table{{table-layout:fixed}} th,td{{white-space:normal;overflow-wrap:anywhere;word-break:break-word}} details{{display:block}} details>summary{{display:none}} details>pre{{display:block}}}}
</style>
</head>
<body>
<a class="skip" href="#main-content">Skip to report</a>
<main id="main-content">
<p class="status">Exploratory descriptive — not confirmatory</p>
<h1>MLB moneyline game dynamics</h1>
<p class="lede">A fixed-bin view of calibration before first play and during early, middle, and late MLB innings, with separately defined market-close calibration.</p>
<nav aria-label="Report sections"><ol><li><a href="#reading">How to read</a></li><li><a href="#coverage">Coverage</a></li><li><a href="#phase">Phase profiles</a></li><li><a href="#closing">Closing calibration</a></li><li><a href="#tails">FLB tails</a></li><li><a href="#provenance">Provenance</a></li></ol></nav>

<section id="reading"><h2>1. How to read this report</h2>
<div class="callout"><strong>Calibration is y − p.</strong> Positive values mean the purchased outcome won more often than its trade price; negative values mean it won less often. A classic favorite–longshot sign pattern requires negative D1 and positive D10 point estimates. The full ten-bin profile remains primary.</div>
<div class="callout caution">Whiskers are nominal 95% intervals and are not adjusted for the number of cells inspected. Fixed bins can be noisy or composition-sensitive. This report does not establish a smooth FLB curve, a difference between phases, a complexity relationship, predictability mechanism, or causal effect.</div>
<p>Phase charts use the purchased outcome's price. Closing charts instead normalize each close to the official home team's win probability. A market close is an observed pregame fill, not an eventual outcome or latent “closing probability,” and this report estimates no quantity named CLV.</p></section>

<section id="coverage"><h2>2. Audited coverage</h2>
<div class="cards">
<div class="card"><span>Eligible moneyline games</span><strong>{_fmt_count(counts07['eligible_games'])}</strong></div>
<div class="card"><span>Phase-input games</span><strong>{_fmt_count(phase_counts['games'])}</strong></div>
<div class="card"><span>Phase-input rows</span><strong>{_fmt_count(phase_counts['rows'])}</strong></div>
<div class="card"><span>Primary A closes</span><strong>{_fmt_count(counts07['primary_closes'])}</strong></div>
<div class="card"><span>Sensitivity C closes</span><strong>{_fmt_count(counts07['sensitivity_closes'])}</strong></div>
<div class="card"><span>C closes unavailable</span><strong>{_fmt_count(counts07['sensitivity_missing'])}</strong></div>
</div>
<p>The dual-close spine contains {_fmt_count(close_counts['close_only_games'])} close-only game(s); deterministic audit sample: <code>{_e(close_only_text)}</code>. Closing estimates retain the full close spine, while phase estimates use only filtered phase rows.</p>
{_boundary_table(stage08)}
</section>

<section id="phase"><h2>3. Calibration by game phase</h2>
<p>Literal phases use official observed first-play, top-fourth, top-seventh, and final-play boundaries. The fixed sensitivity removes, without reassigning, trades within 30 seconds inclusive of any boundary. Phase observations require <code>0.01 &lt; price &lt; 0.99</code> and exclude flagged outcome-token buyers; this is a buyer-centered sample, not a bot-free or human-to-human series. Each BUY fill has equal estimator weight; dollars are descriptive.</p>
<p>Each phase panel contains the complete D1–D10 profile for both timing samples. Its adjacent D10 − D1 tail contrast is reported only when both D1 and D10 have <em>n</em> ≥ {MIN_CELL_N}; thin-tail support and the suppression reason remain visible.</p>
<div class="chart-grid">{''.join(phase_sections)}</div>
</section>

<section id="closing"><h2>4. Pregame closing calibration</h2>
<p>Primary A is the last valid exact-timestamp fill strictly before observed first play with <code>0 &lt; price &lt; 1</code> and bot participants included. Sensitivity C requires <code>0.01 &lt; price &lt; 0.99</code> and excludes only a flagged outcome-token buyer. A flagged seller/counterparty does not remove a fill; {_fmt_count(counts07['sensitivity_closes_with_flagged_bot_counterparty'])} retained C closes have a flagged counterparty.</p>
<div class="cards"><div class="card"><span>Primary A overall y − p</span><strong>{_fmt_number(primary_overall['mean_calibration'], signed=True)}</strong><span>{_e(_fmt_ci(primary_overall['calibration_ci95_low'], primary_overall['calibration_ci95_high']))}</span></div><div class="card"><span>Sensitivity C overall y − p</span><strong>{_fmt_number(sensitivity_overall['mean_calibration'], signed=True)}</strong><span>{_e(_fmt_ci(sensitivity_overall['calibration_ci95_low'], sensitivity_overall['calibration_ci95_high']))}</span></div></div>
{closing_svg}
{_closing_table(closing, overall=True)}
{_closing_table(closing, overall=False)}
<h3>Paired filter sensitivity</h3>
<p>Every displayed difference is primary A minus sensitivity C on common games. It attributes the effect of the C filter and is not CLV. The paired artifact contains no paired uncertainty columns, so none are invented here.</p>
{_paired_table(paired)}
</section>

<section id="tails"><h2>5. D1/D10 descriptive tail summary</h2>
<p>The stage-09 artifact reports D1, D10, and D10 − D1 only when both tail cells contain at least {MIN_CELL_N} observations. Its point-pattern label is determined solely from the two point-estimate signs; it is not a discovery label. Suppressed rows retain support but withhold every tail estimate and interval.</p>
{_tail_table(tails)}
</section>

<section id="limitations"><h2>6. Limits on interpretation</h2><ul>
<li>Phase estimates weight eligible trades equally and use Cameron–Gelbach–Miller clustering by UTC day, buyer wallet, and MLB game. Dollars do not weight estimates.</li>
<li>Closing estimates weight games equally and use official-date clustered uncertainty.</li>
<li>The closing sensitivity is buyer-centered and is neither bot-free nor a human-to-human series.</li>
<li>The standard-timing MLB subset is a controlled exploration, not evidence that baseball represents the Polymarket universe.</li>
<li>No regression, slope, complexity proxy, price-path variance, multiplicity-adjusted inference, or causal model appears in this report.</li>
</ul></section>

<section id="provenance"><h2>7. Provenance and reproducibility</h2>
<div class="callout"><strong>Exact time only.</strong> The report traces to the Polygon block-timestamp cache with zero fallback rows. The generic declaration validator is cache-scoped; the dual-close builder separately passed its row-level exact-cache coverage and recorded every close timestamp as cache-derived.</div>
<p>Rendered only from immutable stage-07, stage-08, and stage-09 artifacts. The renderer validates their schemas, complete fixed grids, suppression, source fingerprints, availability partitions, phase-sample nesting, A-minus-C identities, and all recorded reconciliation gates before publication.</p>
{_provenance_table(fingerprints)}
<details><summary>Embedded machine-readable provenance</summary><pre>{provenance_json}</pre></details>
</section>
</main></body></html>
"""
    return body


def _validate_html(document: str) -> None:
    lowered = document.lower()
    required = (
        '<html lang="en">',
        'name="viewport"',
        "content-security-policy",
        "@media(max-width:700px)",
        "@media print",
        'role="img"',
        "<title id=",
        "<desc id=",
        "<caption>",
    )
    missing = [item for item in required if item not in lowered]
    if missing:
        raise FlbReportError(f"Rendered HTML is missing accessibility/responsive elements: {missing}")
    forbidden = ("http://", "https://", "<script src=", "<link rel=", "significant")
    present = [item for item in forbidden if item in lowered]
    if present:
        raise FlbReportError(f"Rendered HTML contains forbidden external/interpretive content: {present}")
    if lowered.count("<svg ") != lowered.count('role="img"'):
        raise FlbReportError("Every SVG must be exposed as one labelled image")
    if 'data-status="suppressed"' in lowered:
        raise FlbReportError("Suppressed estimates must not be plotted")


def render_flb_report(
    stage07_dir: str | Path,
    stage08_dir: str | Path,
    stage09_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    stage07 = Path(stage07_dir).resolve()
    stage08 = Path(stage08_dir).resolve()
    stage09 = Path(stage09_dir).resolve()
    destination = Path(output_dir).resolve()
    paths = {
        "game_closes": stage07 / "game_closes.parquet",
        "stage07_reconciliation": stage07 / "reconciliation.json",
        "closing_calibration": stage08 / "closing_calibration.parquet",
        "closing_paired_sensitivity": stage08 / "closing_paired_sensitivity.parquet",
        "trade_phase_calibration": stage08 / "trade_phase_calibration.parquet",
        "estimator_summary": stage08 / "estimator_summary.json",
        "flb_tail_summary": stage09 / "flb_tail_summary.parquet",
        "flb_summary": stage09 / "flb_summary.json",
    }
    _validate_paths(destination, paths.values())
    for path in paths.values():
        if not path.is_file():
            raise FlbReportError(f"Missing input: {path}")
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        _require_provenance_schema(con, paths["game_closes"])
        _require_exact_schema(con, paths["closing_calibration"], CLOSING_SCHEMA, "closing_calibration")
        _require_exact_schema(con, paths["closing_paired_sensitivity"], PAIRED_SCHEMA, "closing_paired_sensitivity")
        _require_exact_schema(con, paths["trade_phase_calibration"], TRADE_SCHEMA, "trade_phase_calibration")
        _require_exact_schema(con, paths["flb_tail_summary"], TAIL_SCHEMA, "flb_tail_summary")
        stage07_summary = _load_json(paths["stage07_reconciliation"])
        stage08_summary = _load_json(paths["estimator_summary"])
        stage09_summary = _load_json(paths["flb_summary"])
        _validate_stage07(con, paths["game_closes"], stage07_summary)
        _validate_summaries(stage07_summary, stage08_summary, stage09_summary, paths)
        closing = _read_rows(con, paths["closing_calibration"])
        paired = _read_rows(con, paths["closing_paired_sensitivity"])
        trades = _read_rows(con, paths["trade_phase_calibration"])
        tails = _read_rows(con, paths["flb_tail_summary"])
        _validate_closing(closing, stage08_summary)
        _validate_paired(paired, stage08_summary, stage07_summary)
        _validate_trade(trades, stage08_summary)
        _validate_tail(tails, closing, trades, stage09_summary)
    finally:
        con.close()
    fingerprints = {name: _fingerprint(path) for name, path in paths.items()}
    bundle = {
        "closing": closing,
        "paired": paired,
        "trades": trades,
        "tails": tails,
        "stage07": stage07_summary,
        "stage08": stage08_summary,
        "stage09": stage09_summary,
        "fingerprints": fingerprints,
    }
    document = _build_html(bundle)
    if document != _build_html(bundle):
        raise FlbReportError("Renderer is not deterministic for identical inputs")
    _validate_html(document)
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
    try:
        html_path = staging / "mlb_flb_report.html"
        html_path.write_text(document, encoding="utf-8")
        manifest = {
            "schema_version": 1,
            "method": "mlb_flb_self_contained_html_v1",
            "analysis_state": "exploratory_descriptive",
            "inputs": fingerprints,
            "validations": {
                "source_fingerprints_reconciled": True,
                "schemas_and_fixed_grains_validated": True,
                "suppression_fail_closed": True,
                "no_headline_estimation_in_renderer": True,
                "deterministic_second_render_matches": True,
                "standalone_no_external_resources": True,
                "semantic_tables_and_labelled_svgs": True,
                "responsive_viewports": [320, 375, 768, 1440],
                "atomic_fresh_publication": True,
            },
            "outputs": {
                "html": {
                    "name": "mlb_flb_report.html",
                    "bytes": html_path.stat().st_size,
                    "sha256": hashlib.sha256(html_path.read_bytes()).hexdigest(),
                },
                "manifest": "report_manifest.json",
            },
        }
        manifest_path = staging / "report_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if html_path.read_text(encoding="utf-8") != document:
            raise FlbReportError("Serialized HTML does not match deterministic render")
        json.loads(manifest_path.read_text(encoding="utf-8"))
        os.rename(staging, destination)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage07-dir", required=True, type=Path)
    parser.add_argument("--stage08-dir", required=True, type=Path)
    parser.add_argument("--stage09-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = render_flb_report(
        args.stage07_dir, args.stage08_dir, args.stage09_dir, args.output_dir
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
