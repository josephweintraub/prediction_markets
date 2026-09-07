#!/usr/bin/env python3
"""Compose a validated MLB moneyline/game/timing universe without estimating results.

Moneyline validity and suitability for the conservative standard-timing core
are recorded separately.  Every preliminary candidate remains in the validation
audit; only candidates passing both gates enter ``eligible_moneylines.parquet``.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from build_game_timing import MATCH_SCHEMA, SCHEDULE_SCHEMA, TIMING_SCHEMA
from build_market_universe import MLB_SLUG_PATTERN
from match_games import (
    GameMatchAudit,
    MLB_TEAMS,
    MlbTeam,
    assert_one_to_one_matches,
    match_market_candidates,
)
from mlb_api import ScheduleGame
from validate_moneylines import load_canonical_token_rows, validate_moneylines


SCHEDULE_INPUT = "schedule_audit.parquet"
MATCH_INPUT = "match_audit.parquet"
TIMING_INPUT = "game_timing.parquet"
AUDIT_OUTPUT = "candidate_validation_audit.parquet"
ELIGIBLE_OUTPUT = "eligible_moneylines.parquet"
SUMMARY_OUTPUT = "summary.json"

CANDIDATE_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("date", "DATE"),
    ("away", "VARCHAR"),
    ("home", "VARCHAR"),
    ("question", "VARCHAR"),
)

AUDIT_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("market_date", "DATE"),
    ("away_slug", "VARCHAR"),
    ("home_slug", "VARCHAR"),
    ("slug_orientation", "VARCHAR"),
    ("question", "VARCHAR"),
    ("matched_game_pk", "BIGINT"),
    ("match_exclusion_reason", "VARCHAR"),
    ("timing_status", "VARCHAR"),
    ("timing_exclusion_reason", "VARCHAR"),
    ("timing_error_type", "VARCHAR"),
    ("timing_error_message", "VARCHAR"),
    ("moneyline_valid", "BOOLEAN"),
    ("moneyline_exclusion_reason", "VARCHAR"),
    ("unique_token_count", "INTEGER"),
    ("distinct_outcome_count", "INTEGER"),
    ("polymarket_winning_outcome", "VARCHAR"),
    ("mlb_winning_team_id", "INTEGER"),
    ("core_timing_eligible", "BOOLEAN"),
    ("core_timing_exclusion_reason", "VARCHAR"),
    ("eligible_for_core_analysis", "BOOLEAN"),
    ("official_date", "DATE"),
    ("game_type", "VARCHAR"),
    ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"),
    ("away_token_id", "VARCHAR"),
    ("home_token_id", "VARCHAR"),
    ("winning_team_id", "INTEGER"),
    ("winning_token_id", "VARCHAR"),
    ("winning_outcome", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("actual_start_utc", "TIMESTAMPTZ"),
    ("inning_4_start_utc", "TIMESTAMPTZ"),
    ("inning_7_start_utc", "TIMESTAMPTZ"),
    ("actual_end_utc", "TIMESTAMPTZ"),
    ("final_inning", "INTEGER"),
    ("play_count", "INTEGER"),
    ("doubleheader", "VARCHAR"),
    ("game_number", "INTEGER"),
    ("series_game_number", "INTEGER"),
    ("reschedule_date_utc", "TIMESTAMPTZ"),
    ("rescheduled_from_date", "DATE"),
    ("resume_date_utc", "TIMESTAMPTZ"),
    ("resumed_from_date", "DATE"),
    ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"),
    ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"),
)

ELIGIBLE_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("market_date", "DATE"),
    ("away_slug", "VARCHAR"),
    ("home_slug", "VARCHAR"),
    ("slug_orientation", "VARCHAR"),
    ("question", "VARCHAR"),
    ("game_pk", "BIGINT"),
    ("official_date", "DATE"),
    ("game_type", "VARCHAR"),
    ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"),
    ("away_token_id", "VARCHAR"),
    ("home_token_id", "VARCHAR"),
    ("winning_team_id", "INTEGER"),
    ("winning_token_id", "VARCHAR"),
    ("winning_outcome", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("actual_start_utc", "TIMESTAMPTZ"),
    ("inning_4_start_utc", "TIMESTAMPTZ"),
    ("inning_7_start_utc", "TIMESTAMPTZ"),
    ("actual_end_utc", "TIMESTAMPTZ"),
    ("final_inning", "INTEGER"),
    ("play_count", "INTEGER"),
    ("doubleheader", "VARCHAR"),
    ("game_number", "INTEGER"),
    ("series_game_number", "INTEGER"),
    ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"),
    ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"),
)


class ValidatedUniverseBuildError(ValueError):
    """Raised when input artifacts cannot be reconciled exactly."""


def _path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _quoted(value: str | Path) -> str:
    return str(_path(value)).replace("'", "''")


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def _validate_destination(run_dir: Path, inputs: Iterable[Path]) -> None:
    dangerous = {Path("/").resolve(), Path.home().resolve(), Path.cwd().resolve()}
    if run_dir in dangerous:
        raise ValidatedUniverseBuildError(f"Refusing dangerous run directory: {run_dir}")
    for input_path in inputs:
        if _paths_overlap(run_dir, input_path):
            raise ValidatedUniverseBuildError(
                f"Output run directory must not overlap input path: {input_path}"
            )
    if run_dir.exists():
        raise FileExistsError(f"Immutable validated-universe run already exists: {run_dir}")


def _normalized_type(value: str) -> str:
    upper = value.upper()
    return "TIMESTAMP WITH TIME ZONE" if upper == "TIMESTAMPTZ" else upper


def _read_typed_rows(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    expected_schema: tuple[tuple[str, str], ...],
    label: str,
    order_by: str,
) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"{label} does not exist: {path}")
    relation = f"read_parquet('{_quoted(path)}')"
    observed = {
        row[0]: _normalized_type(row[1])
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    missing = [name for name, _ in expected_schema if name not in observed]
    if missing:
        raise ValidatedUniverseBuildError(f"{label} is missing required columns: {missing}")
    wrong_types = {
        name: {"expected": _normalized_type(kind), "observed": observed[name]}
        for name, kind in expected_schema
        if observed[name] != _normalized_type(kind)
    }
    if wrong_types:
        raise ValidatedUniverseBuildError(f"{label} has invalid column types: {wrong_types}")
    columns = [name for name, _ in expected_schema]
    selected = ", ".join(f'"{name}"' for name in columns)
    values = con.execute(
        f"SELECT {selected} FROM {relation} ORDER BY {order_by}"
    ).fetchall()
    return [dict(zip(columns, row, strict=True)) for row in values]


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidatedUniverseBuildError(f"{label} must be a non-empty string")
    return value.strip()


def _validate_candidates(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not rows:
        raise ValidatedUniverseBuildError("Candidate-market input must be nonempty")
    candidates: dict[str, dict[str, Any]] = {}
    event_slugs: set[str] = set()
    slug_pattern = re.compile(MLB_SLUG_PATTERN)
    for row in rows:
        market_id = _require_string(row["market_id"], "candidate market_id")
        event_slug = _require_string(row["event_slug"], f"candidate {market_id} event_slug")
        away = _require_string(row["away"], f"candidate {market_id} away")
        home = _require_string(row["home"], f"candidate {market_id} home")
        _require_string(row["question"], f"candidate {market_id} question")
        market_date = row["date"]
        if not isinstance(market_date, date):
            raise ValidatedUniverseBuildError(f"candidate {market_id} has invalid date")
        parsed = slug_pattern.fullmatch(event_slug)
        if parsed is None or (parsed.group(1), parsed.group(2), parsed.group(3)) != (
            away,
            home,
            market_date.isoformat(),
        ):
            raise ValidatedUniverseBuildError(
                f"candidate {market_id} slug fields do not reconcile"
            )
        if market_id in candidates:
            raise ValidatedUniverseBuildError(f"Duplicate candidate market_id: {market_id}")
        if event_slug in event_slugs:
            raise ValidatedUniverseBuildError(f"Duplicate candidate event_slug: {event_slug}")
        candidates[market_id] = row
        event_slugs.add(event_slug)
    return candidates


def _schedule_games(rows: list[dict[str, Any]]) -> dict[int, ScheduleGame]:
    games: dict[int, ScheduleGame] = {}
    for row in rows:
        game_pk = row["game_pk"]
        if not isinstance(game_pk, int) or isinstance(game_pk, bool):
            raise ValidatedUniverseBuildError("Schedule audit has a null or invalid game_pk")
        if game_pk in games:
            raise ValidatedUniverseBuildError(f"Duplicate schedule game_pk: {game_pk}")
        games[game_pk] = ScheduleGame(**row)
    return games


def _same(label: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise ValidatedUniverseBuildError(
            f"Cross-artifact mismatch for {label}: {observed!r} != {expected!r}"
        )


def _schedule_ids(row: Mapping[str, Any]) -> tuple[int, ...]:
    raw = row["schedule_match_game_pks_json"]
    if not isinstance(raw, str):
        raise ValidatedUniverseBuildError("schedule_match_game_pks_json must be a string")
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValidatedUniverseBuildError(
            f"Invalid schedule_match_game_pks_json for {row['market_id']}"
        ) from exc
    if not isinstance(decoded, list) or any(
        not isinstance(value, int) or isinstance(value, bool) for value in decoded
    ):
        raise ValidatedUniverseBuildError(
            f"schedule_match_game_pks_json must be an integer list for {row['market_id']}"
        )
    if len(decoded) != len(set(decoded)):
        raise ValidatedUniverseBuildError(
            f"Duplicate schedule match game IDs for {row['market_id']}"
        )
    _same(
        f"{row['market_id']} schedule_match_count",
        row["schedule_match_count"],
        len(decoded),
    )
    return tuple(decoded)


MATCH_SCHEDULE_FIELDS = {
    "scheduled_start_utc": "scheduled_start_utc",
    "schedule_status_abstract": "status_abstract",
    "schedule_status_detailed": "status_detailed",
    "schedule_status_code": "status_code",
    "schedule_is_completed": "is_completed",
    "schedule_doubleheader": "doubleheader",
    "schedule_game_number": "game_number",
    "schedule_series_game_number": "series_game_number",
    "schedule_reschedule_date_utc": "reschedule_date_utc",
    "schedule_rescheduled_from_date": "rescheduled_from_date",
    "schedule_resume_date_utc": "resume_date_utc",
    "schedule_resumed_from_date": "resumed_from_date",
    "schedule_away_final_score": "away_final_score",
    "schedule_home_final_score": "home_final_score",
    "schedule_away_is_winner": "away_is_winner",
    "schedule_home_is_winner": "home_is_winner",
}

TIMING_SCHEDULE_FIELDS = {
    "official_date": "official_date",
    "away_team_id": "away_team_id",
    "away_team_name": "away_team_name",
    "home_team_id": "home_team_id",
    "home_team_name": "home_team_name",
    "scheduled_start_utc": "scheduled_start_utc",
    "status_abstract": "status_abstract",
    "status_detailed": "status_detailed",
    "status_code": "status_code",
    "doubleheader": "doubleheader",
    "game_number": "game_number",
    "series_game_number": "series_game_number",
    "reschedule_date_utc": "reschedule_date_utc",
    "rescheduled_from_date": "rescheduled_from_date",
    "resume_date_utc": "resume_date_utc",
    "resumed_from_date": "resumed_from_date",
    "schedule_away_final_score": "away_final_score",
    "schedule_home_final_score": "home_final_score",
    "schedule_away_is_winner": "away_is_winner",
    "schedule_home_is_winner": "home_is_winner",
}


def _reconstruct_matches(
    candidates: Mapping[str, dict[str, Any]],
    match_rows: list[dict[str, Any]],
    games: Mapping[int, ScheduleGame],
) -> tuple[tuple[GameMatchAudit, ...], dict[str, dict[str, Any]]]:
    rows_by_market: dict[str, dict[str, Any]] = {}
    for row in match_rows:
        market_id = _require_string(row["market_id"], "match audit market_id")
        if market_id in rows_by_market:
            raise ValidatedUniverseBuildError(f"Duplicate match-audit market_id: {market_id}")
        rows_by_market[market_id] = row
    if set(rows_by_market) != set(candidates):
        missing = sorted(set(candidates) - set(rows_by_market))
        extra = sorted(set(rows_by_market) - set(candidates))
        raise ValidatedUniverseBuildError(
            f"Candidate/match audit market IDs do not reconcile; missing={missing}, extra={extra}"
        )

    expected_matches = match_market_candidates(candidates.values(), games.values())
    try:
        assert_one_to_one_matches(expected_matches)
    except ValueError as exc:
        raise ValidatedUniverseBuildError(str(exc)) from exc
    expected_by_market = {row.market_id: row for row in expected_matches}

    audits: list[GameMatchAudit] = []
    for market_id, candidate in candidates.items():
        row = rows_by_market[market_id]
        expected_match = expected_by_market[market_id]
        _same(f"{market_id} market_date", row["market_date"], candidate["date"])
        _same(f"{market_id} away_slug", row["away_slug"], candidate["away"])
        _same(f"{market_id} home_slug", row["home_slug"], candidate["home"])
        first_slug_team = MLB_TEAMS.get(candidate["away"])
        second_slug_team = MLB_TEAMS.get(candidate["home"])

        match_ids = _schedule_ids(row)
        _same(
            f"{market_id} selected schedule matches",
            match_ids,
            tuple(game.game_pk for game in expected_match.schedule_matches),
        )
        absent = [game_pk for game_pk in match_ids if game_pk not in games]
        if absent:
            raise ValidatedUniverseBuildError(
                f"Match audit {market_id} references absent schedule games: {absent}"
            )
        schedule_matches = tuple(games[game_pk] for game_pk in match_ids)
        orientations: set[str] = set()
        for game in schedule_matches:
            _same(f"{market_id}/game {game.game_pk} date", game.official_date, candidate["date"])
            if first_slug_team is None or second_slug_team is None:
                raise ValidatedUniverseBuildError(
                    f"Match audit {market_id} has schedule games for an unknown team slug"
                )
            observed_team_ids = {
                first_slug_team.team_id,
                second_slug_team.team_id,
            }
            official_team_ids = {game.away_team_id, game.home_team_id}
            _same(
                f"{market_id}/game {game.game_pk} unordered team identity",
                official_team_ids,
                observed_team_ids,
            )
            if (
                first_slug_team.team_id,
                second_slug_team.team_id,
            ) == (game.away_team_id, game.home_team_id):
                orientations.add("official")
            elif (
                first_slug_team.team_id,
                second_slug_team.team_id,
            ) == (game.home_team_id, game.away_team_id):
                orientations.add("reversed")
            else:  # The unordered-identity gate above makes this unreachable.
                raise ValidatedUniverseBuildError(
                    f"Cannot orient candidate slug teams for {market_id}"
                )

        expected_orientation = next(iter(orientations)) if len(orientations) == 1 else None
        _same(
            f"{market_id} slug_orientation",
            row["slug_orientation"],
            expected_orientation,
        )
        canonical_pairs = {
            (
                game.away_team_id,
                game.away_team_name,
                game.home_team_id,
                game.home_team_name,
            )
            for game in schedule_matches
        }
        canonical_pair = next(iter(canonical_pairs)) if len(canonical_pairs) == 1 else None
        away_team = (
            MlbTeam(canonical_pair[0], canonical_pair[1]) if canonical_pair else None
        )
        home_team = (
            MlbTeam(canonical_pair[2], canonical_pair[3]) if canonical_pair else None
        )
        _same(
            f"{market_id} away_team_id",
            row["away_team_id"],
            away_team.team_id if away_team else None,
        )
        _same(
            f"{market_id} away_team_name",
            row["away_team_name"],
            away_team.name if away_team else None,
        )
        _same(
            f"{market_id} home_team_id",
            row["home_team_id"],
            home_team.team_id if home_team else None,
        )
        _same(
            f"{market_id} home_team_name",
            row["home_team_name"],
            home_team.name if home_team else None,
        )

        unique_game = schedule_matches[0] if len(schedule_matches) == 1 else None
        for match_field, game_field in MATCH_SCHEDULE_FIELDS.items():
            expected = getattr(unique_game, game_field) if unique_game else None
            _same(f"{market_id} {match_field}", row[match_field], expected)

        reason = row["match_exclusion_reason"]
        matched_game_pk = row["matched_game_pk"]
        _same(
            f"{market_id} match_exclusion_reason",
            reason,
            expected_match.exclusion_reason,
        )
        _same(
            f"{market_id} matched_game_pk",
            matched_game_pk,
            expected_match.matched_game_pk,
        )
        if matched_game_pk is not None:
            if reason is not None or len(match_ids) != 1 or matched_game_pk != match_ids[0]:
                raise ValidatedUniverseBuildError(
                    f"Matched-game identity is inconsistent for {market_id}"
                )
        elif reason is None:
            raise ValidatedUniverseBuildError(
                f"Unmatched audit row lacks an exclusion reason for {market_id}"
            )

        audits.append(
            GameMatchAudit(
                market_id=market_id,
                market_date=row["market_date"],
                away_slug=row["away_slug"],
                home_slug=row["home_slug"],
                slug_orientation=row["slug_orientation"],
                away_team=away_team,
                home_team=home_team,
                matched_game_pk=matched_game_pk,
                exclusion_reason=reason,
                schedule_matches=schedule_matches,
            )
        )
    try:
        assert_one_to_one_matches(audits)
    except ValueError as exc:
        raise ValidatedUniverseBuildError(str(exc)) from exc
    return tuple(audits), rows_by_market


def _validate_timing_rows(
    rows: list[dict[str, Any]],
    matches: Iterable[GameMatchAudit],
    match_rows: Mapping[str, dict[str, Any]],
    games: Mapping[int, ScheduleGame],
) -> dict[str, dict[str, Any]]:
    matches_by_market = {row.market_id: row for row in matches}
    by_market: dict[str, dict[str, Any]] = {}
    game_ids: set[int] = set()
    for row in rows:
        market_id = _require_string(row["market_id"], "game timing market_id")
        game_pk = row["game_pk"]
        if not isinstance(game_pk, int) or isinstance(game_pk, bool):
            raise ValidatedUniverseBuildError("Game timing has a null or invalid game_pk")
        if market_id in by_market:
            raise ValidatedUniverseBuildError(f"Duplicate timing market_id: {market_id}")
        if game_pk in game_ids:
            raise ValidatedUniverseBuildError(f"Duplicate timing game_pk: {game_pk}")
        if market_id not in matches_by_market:
            raise ValidatedUniverseBuildError(f"Orphan timing market_id: {market_id}")
        match = matches_by_market[market_id]
        if not match.is_matched or match.matched_game_pk != game_pk:
            raise ValidatedUniverseBuildError(
                f"Timing row does not match the exact schedule assignment for {market_id}"
            )
        if match_rows[market_id]["timing_status"] != "passed":
            raise ValidatedUniverseBuildError(
                f"Timing row exists without passed upstream timing status for {market_id}"
            )
        if game_pk not in games:
            raise ValidatedUniverseBuildError(f"Timing row references absent game_pk: {game_pk}")
        game = games[game_pk]
        for timing_field, game_field in TIMING_SCHEDULE_FIELDS.items():
            _same(
                f"{market_id} timing {timing_field}",
                row[timing_field],
                getattr(game, game_field),
            )
        by_market[market_id] = row
        game_ids.add(game_pk)

    for market_id, match in matches_by_market.items():
        row = match_rows[market_id]
        status = row["timing_status"]
        reason = row["timing_exclusion_reason"]
        if match.is_matched:
            if status not in {"passed", "failed"}:
                raise ValidatedUniverseBuildError(
                    f"Completed timing audit has invalid status {status!r} for {market_id}"
                )
            if status == "failed" and (not reason or market_id in by_market):
                raise ValidatedUniverseBuildError(
                    f"Failed timing status is inconsistent for {market_id}"
                )
            if status == "passed" and reason is not None:
                raise ValidatedUniverseBuildError(
                    f"Passed timing status has an exclusion reason for {market_id}"
                )
        elif status != "not_eligible" or not reason or market_id in by_market:
            raise ValidatedUniverseBuildError(
                f"Unmatched candidate timing status is inconsistent for {market_id}"
            )
    return by_market


def _core_timing_decision(
    match: GameMatchAudit,
    match_row: Mapping[str, Any],
    timing: Mapping[str, Any] | None,
) -> tuple[bool, str | None]:
    if not match.is_matched:
        reason = match.exclusion_reason or "unmatched_game"
        return False, f"upstream_match_{reason}"
    if match_row["timing_status"] != "passed":
        reason = match_row["timing_exclusion_reason"] or str(match_row["timing_status"])
        return False, f"upstream_timing_{reason}"
    if timing is None:
        return False, "missing_timing_row"

    game = match.schedule_matches[0]
    if game.reschedule_date_utc is not None or game.rescheduled_from_date is not None:
        return False, "irregular_rescheduled_game"
    if game.resume_date_utc is not None or game.resumed_from_date is not None:
        return False, "irregular_resumed_game"
    status_text = " ".join(
        str(value) for value in (game.status_abstract, game.status_detailed, game.status_code)
    ).casefold()
    if "suspend" in status_text:
        return False, "irregular_suspended_game"
    doubleheader = (
        game.doubleheader.strip().casefold()
        if isinstance(game.doubleheader, str)
        else None
    )
    if doubleheader not in {None, "n"}:
        return False, "irregular_doubleheader"
    if timing["final_inning"] is None:
        return False, "missing_final_inning"
    if timing["final_inning"] < 9:
        return False, "irregular_shortened_game"
    boundaries = (
        timing["actual_start_utc"],
        timing["inning_4_start_utc"],
        timing["inning_7_start_utc"],
        timing["actual_end_utc"],
    )
    if any(value is None for value in boundaries):
        return False, "missing_core_phase_boundary"
    if not boundaries[0] < boundaries[1] < boundaries[2] <= boundaries[3]:
        return False, "invalid_core_phase_boundary_order"
    return True, None


def _timing_values(
    match: GameMatchAudit, timing: Mapping[str, Any] | None
) -> dict[str, Any]:
    game = match.schedule_matches[0] if len(match.schedule_matches) == 1 else None
    return {
        "official_date": game.official_date if game else None,
        "game_type": game.game_type if game else None,
        "away_team_id": game.away_team_id if game else None,
        "away_team_name": game.away_team_name if game else None,
        "home_team_id": game.home_team_id if game else None,
        "home_team_name": game.home_team_name if game else None,
        "scheduled_start_utc": game.scheduled_start_utc if game else None,
        "actual_start_utc": timing["actual_start_utc"] if timing else None,
        "inning_4_start_utc": timing["inning_4_start_utc"] if timing else None,
        "inning_7_start_utc": timing["inning_7_start_utc"] if timing else None,
        "actual_end_utc": timing["actual_end_utc"] if timing else None,
        "final_inning": timing["final_inning"] if timing else None,
        "play_count": timing["play_count"] if timing else None,
        "doubleheader": game.doubleheader if game else None,
        "game_number": game.game_number if game else None,
        "series_game_number": game.series_game_number if game else None,
        "reschedule_date_utc": game.reschedule_date_utc if game else None,
        "rescheduled_from_date": game.rescheduled_from_date if game else None,
        "resume_date_utc": game.resume_date_utc if game else None,
        "resumed_from_date": game.resumed_from_date if game else None,
        "away_final_score": game.away_final_score if game else None,
        "home_final_score": game.home_final_score if game else None,
        "away_is_winner": game.away_is_winner if game else None,
        "home_is_winner": game.home_is_winner if game else None,
    }


def _write_rows(
    rows: Iterable[Mapping[str, Any]],
    schema: tuple[tuple[str, str], ...],
    path: Path,
    order_by: str,
) -> None:
    columns = [name for name, _ in schema]
    declarations = ", ".join(f'"{name}" {kind}' for name, kind in schema)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE rows_to_write ({declarations})")
        materialized = list(rows)
        if materialized:
            placeholders = ", ".join("?" for _ in columns)
            con.executemany(
                f"INSERT INTO rows_to_write VALUES ({placeholders})",
                [[row.get(column) for column in columns] for row in materialized],
            )
        con.execute(
            f"COPY (SELECT * FROM rows_to_write ORDER BY {order_by}) "
            f"TO '{_quoted(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def _verify_staging(staging: Path, candidate_ids: set[str], eligible_count: int) -> None:
    con = duckdb.connect()
    try:
        audit_relation = f"read_parquet('{_quoted(staging / AUDIT_OUTPUT)}')"
        rows, distinct_rows = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT market_id) FROM {audit_relation}"
        ).fetchone()
        written_ids = {
            row[0] for row in con.execute(f"SELECT market_id FROM {audit_relation}").fetchall()
        }
        eligible_rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{_quoted(staging / ELIGIBLE_OUTPUT)}')"
        ).fetchone()[0]
    finally:
        con.close()
    if rows != len(candidate_ids) or distinct_rows != rows or written_ids != candidate_ids:
        raise ValidatedUniverseBuildError("Written candidate audit did not reconcile one-to-one")
    if eligible_rows != eligible_count:
        raise ValidatedUniverseBuildError("Written eligible-moneyline count did not reconcile")


def build_validated_universe(
    candidate_path: str | Path,
    timing_run_dir: str | Path,
    universe_tokens_path: str | Path,
    token_map_path: str | Path,
    output_run_dir: str | Path,
) -> dict[str, Any]:
    """Validate all inputs and atomically publish the conservative MLB core."""

    candidates_path = _path(candidate_path)
    timing_run = _path(timing_run_dir)
    universe_tokens = _path(universe_tokens_path)
    token_map = _path(token_map_path)
    output = _path(output_run_dir)
    schedule_path = timing_run / SCHEDULE_INPUT
    match_path = timing_run / MATCH_INPUT
    timing_path = timing_run / TIMING_INPUT
    inputs = [
        candidates_path,
        timing_run,
        schedule_path,
        match_path,
        timing_path,
        universe_tokens,
        token_map,
    ]
    _validate_destination(output, inputs)
    if not timing_run.is_dir():
        raise FileNotFoundError(f"Completed MLB timing-audit directory is missing: {timing_run}")

    con = duckdb.connect()
    try:
        candidate_rows = _read_typed_rows(
            con, candidates_path, CANDIDATE_SCHEMA, "Candidate markets", "date, market_id"
        )
        schedule_rows = _read_typed_rows(
            con, schedule_path, SCHEDULE_SCHEMA, "Schedule audit", "official_date, game_pk"
        )
        match_rows = _read_typed_rows(
            con, match_path, MATCH_SCHEMA, "Match audit", "market_date, market_id"
        )
        timing_rows = _read_typed_rows(
            con, timing_path, TIMING_SCHEMA, "Game timing", "official_date, game_pk"
        )
        candidates = _validate_candidates(candidate_rows)
        games = _schedule_games(schedule_rows)
        matches, match_rows_by_market = _reconstruct_matches(candidates, match_rows, games)
        timing_by_market = _validate_timing_rows(
            timing_rows, matches, match_rows_by_market, games
        )
        token_rows = load_canonical_token_rows(
            con, universe_tokens, token_map, candidates.keys()
        )
        validation = validate_moneylines(candidates.values(), matches, token_rows)
    finally:
        con.close()

    if len(validation.audits) != len(candidates):
        raise ValidatedUniverseBuildError(
            "Moneyline validator did not return one audit per candidate"
        )
    moneyline_audits = {row.market_id: row for row in validation.audits}
    if set(moneyline_audits) != set(candidates):
        raise ValidatedUniverseBuildError("Moneyline audit candidate IDs did not reconcile")
    dimensions = {row.market_id: row for row in validation.eligible_markets}
    matches_by_market = {row.market_id: row for row in matches}

    audit_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, Any]] = []
    core_reasons: Counter[str] = Counter()
    moneyline_reasons: Counter[str] = Counter()
    upstream_match_reasons: Counter[str] = Counter(
        row["match_exclusion_reason"]
        for row in match_rows_by_market.values()
        if row["match_exclusion_reason"] is not None
    )
    upstream_timing_reasons: Counter[str] = Counter(
        row["timing_exclusion_reason"]
        for row in match_rows_by_market.values()
        if row["timing_exclusion_reason"] is not None
    )
    for market_id, candidate in candidates.items():
        match = matches_by_market[market_id]
        match_row = match_rows_by_market[market_id]
        timing = timing_by_market.get(market_id)
        core_eligible, core_reason = _core_timing_decision(match, match_row, timing)
        moneyline_audit = moneyline_audits[market_id]
        dimension = dimensions.get(market_id)
        final_eligible = moneyline_audit.is_eligible and core_eligible
        if moneyline_audit.exclusion_reason is not None:
            moneyline_reasons[moneyline_audit.exclusion_reason] += 1
        if core_reason is not None:
            core_reasons[core_reason] += 1
        timing_values = _timing_values(match, timing)
        audit_rows.append(
            {
                "market_id": market_id,
                "event_slug": candidate["event_slug"],
                "market_date": candidate["date"],
                "away_slug": candidate["away"],
                "home_slug": candidate["home"],
                "slug_orientation": match.slug_orientation,
                "question": candidate["question"],
                "matched_game_pk": match.matched_game_pk,
                "match_exclusion_reason": match.exclusion_reason,
                "timing_status": match_row["timing_status"],
                "timing_exclusion_reason": match_row["timing_exclusion_reason"],
                "timing_error_type": match_row["timing_error_type"],
                "timing_error_message": match_row["timing_error_message"],
                "moneyline_valid": moneyline_audit.is_eligible,
                "moneyline_exclusion_reason": moneyline_audit.exclusion_reason,
                "unique_token_count": moneyline_audit.unique_token_count,
                "distinct_outcome_count": moneyline_audit.distinct_outcome_count,
                "polymarket_winning_outcome": moneyline_audit.polymarket_winning_outcome,
                "mlb_winning_team_id": moneyline_audit.mlb_winning_team_id,
                "core_timing_eligible": core_eligible,
                "core_timing_exclusion_reason": core_reason,
                "eligible_for_core_analysis": final_eligible,
                **({
                    "away_token_id": dimension.away_token_id,
                    "home_token_id": dimension.home_token_id,
                    "winning_team_id": dimension.winning_team_id,
                    "winning_token_id": dimension.winning_token_id,
                    "winning_outcome": dimension.winning_outcome,
                } if dimension else {}),
                **timing_values,
            }
        )
        if final_eligible:
            if dimension is None or timing is None:
                raise ValidatedUniverseBuildError(
                    f"Eligible candidate lacks dimension or timing row: {market_id}"
                )
            eligible_rows.append(
                {
                    "market_id": market_id,
                    "event_slug": candidate["event_slug"],
                    "market_date": candidate["date"],
                    "away_slug": candidate["away"],
                    "home_slug": candidate["home"],
                    "slug_orientation": match.slug_orientation,
                    "question": candidate["question"],
                    "game_pk": dimension.game_pk,
                    "official_date": dimension.official_date,
                    "game_type": match.schedule_matches[0].game_type,
                    "away_team_id": dimension.away_team_id,
                    "away_team_name": dimension.away_team_name,
                    "home_team_id": dimension.home_team_id,
                    "home_team_name": dimension.home_team_name,
                    "away_token_id": dimension.away_token_id,
                    "home_token_id": dimension.home_token_id,
                    "winning_team_id": dimension.winning_team_id,
                    "winning_token_id": dimension.winning_token_id,
                    "winning_outcome": dimension.winning_outcome,
                    "scheduled_start_utc": timing["scheduled_start_utc"],
                    "actual_start_utc": timing["actual_start_utc"],
                    "inning_4_start_utc": timing["inning_4_start_utc"],
                    "inning_7_start_utc": timing["inning_7_start_utc"],
                    "actual_end_utc": timing["actual_end_utc"],
                    "final_inning": timing["final_inning"],
                    "play_count": timing["play_count"],
                    "doubleheader": timing["doubleheader"],
                    "game_number": timing["game_number"],
                    "series_game_number": timing["series_game_number"],
                    "away_final_score": timing["schedule_away_final_score"],
                    "home_final_score": timing["schedule_home_final_score"],
                    "away_is_winner": timing["schedule_away_is_winner"],
                    "home_is_winner": timing["schedule_home_is_winner"],
                }
            )

    summary: dict[str, Any] = {
        "schema_version": 1,
        "definition": "validated_mlb_moneyline_standard_timing_core_v1",
        "counts": {
            "candidate_markets": len(candidates),
            "moneyline_valid_markets": sum(row.is_eligible for row in validation.audits),
            "core_timing_eligible_markets": sum(
                row["core_timing_eligible"] for row in audit_rows
            ),
            "eligible_moneylines": len(eligible_rows),
        },
        "exclusion_counts": {
            "upstream_match": dict(sorted(upstream_match_reasons.items())),
            "upstream_timing": dict(sorted(upstream_timing_reasons.items())),
            "moneyline_validation": dict(sorted(moneyline_reasons.items())),
            "core_timing": dict(sorted(core_reasons.items())),
        },
        "outputs": {
            "candidate_validation_audit": AUDIT_OUTPUT,
            "eligible_moneylines": ELIGIBLE_OUTPUT,
        },
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    _validate_destination(output, inputs)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent)
    ).resolve()
    try:
        _write_rows(
            audit_rows,
            AUDIT_SCHEMA,
            staging / AUDIT_OUTPUT,
            "market_date, away_slug, home_slug, market_id",
        )
        _write_rows(
            eligible_rows,
            ELIGIBLE_SCHEMA,
            staging / ELIGIBLE_OUTPUT,
            "official_date, game_pk, market_id",
        )
        (staging / SUMMARY_OUTPUT).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _verify_staging(staging, set(candidates), len(eligible_rows))
        if json.loads((staging / SUMMARY_OUTPUT).read_text(encoding="utf-8")) != summary:
            raise ValidatedUniverseBuildError("Written summary did not round-trip exactly")
        if output.exists():
            raise FileExistsError(
                f"Immutable validated-universe run appeared during build: {output}"
            )
        os.rename(staging, output)
        return summary
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--timing-run-dir", required=True)
    parser.add_argument("--universe-tokens", required=True)
    parser.add_argument("--token-map", required=True)
    parser.add_argument("--output-run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = build_validated_universe(
        args.candidates,
        args.timing_run_dir,
        args.universe_tokens,
        args.token_map,
        args.output_run_dir,
    )
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
