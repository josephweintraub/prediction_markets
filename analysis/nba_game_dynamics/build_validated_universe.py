"""Compose strict NBA moneyline and official timing audits without estimation.

Moneyline validity is evaluated from the unique final schedule match even when
LiveData timing failed. Timing suitability is a separate gate. Publication is
a fresh atomic run directory with fixed schemas, including empty outputs.
"""
from __future__ import annotations

import argparse
import hashlib
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
import pandas as pd

try:  # Package import for tests; direct import for CLI execution.
    from .artifact_manifest import (
        ArtifactManifestError,
        file_fingerprint,
        require_parquet_schema,
        verify_fingerprint,
    )
    from .build_game_timing import (
        DEFAULT_PHASE_CONTRACT,
        NBA_ANALYSIS_PHASES,
        MATCH_OUTPUT,
        MATCH_SCHEMA,
        PROVENANCE_OUTPUT,
        SCHEDULE_OUTPUT,
        SCHEDULE_SCHEMA,
        SUMMARY_OUTPUT as TIMING_SUMMARY_OUTPUT,
        TIMING_OUTPUT,
        TIMING_SCHEMA,
        verify_game_timing_run,
    )
    from .build_market_universe import CANDIDATE_SCHEMA, NBA_SLUG_PATTERN
    from .match_games import NBA_TEAMS, GameMatchAudit, match_market_candidates
    from .nba_api import (
        ACTUAL_END_EVENT,
        ACTUAL_START_EVENT,
        PERIOD_BOUNDARY_EVENT,
        ScheduleGame,
        winning_team_id,
    )
    from analysis.sports_game_dynamics.phase_contract import (
        classify_timestamp as classify_contract_timestamp,
        load_phase_contract,
        phase_contract_fingerprint,
    )
except ImportError:  # pragma: no cover
    from artifact_manifest import (
        ArtifactManifestError,
        file_fingerprint,
        require_parquet_schema,
        verify_fingerprint,
    )
    from build_game_timing import (
        DEFAULT_PHASE_CONTRACT,
        NBA_ANALYSIS_PHASES,
        MATCH_OUTPUT,
        MATCH_SCHEMA,
        PROVENANCE_OUTPUT,
        SCHEDULE_OUTPUT,
        SCHEDULE_SCHEMA,
        SUMMARY_OUTPUT as TIMING_SUMMARY_OUTPUT,
        TIMING_OUTPUT,
        TIMING_SCHEMA,
        verify_game_timing_run,
    )
    from build_market_universe import CANDIDATE_SCHEMA, NBA_SLUG_PATTERN
    from match_games import NBA_TEAMS, GameMatchAudit, match_market_candidates
    from nba_api import (
        ACTUAL_END_EVENT,
        ACTUAL_START_EVENT,
        PERIOD_BOUNDARY_EVENT,
        ScheduleGame,
        winning_team_id,
    )
    from analysis.sports_game_dynamics.phase_contract import (
        classify_timestamp as classify_contract_timestamp,
        load_phase_contract,
        phase_contract_fingerprint,
    )


AUDIT_OUTPUT = "candidate_validation_audit.parquet"
ELIGIBLE_OUTPUT = "eligible_moneylines.parquet"
SUMMARY_OUTPUT = "summary.json"
MANIFEST_OUTPUT = "validated_manifest.json"
TIMESTAMP_SOURCE = "official_nba_livedata_timeActual"

AUDIT_SCHEMA = (
    ("market_id", "VARCHAR"),
    ("event_slug", "VARCHAR"),
    ("market_date", "DATE"),
    ("observed_first_slug", "VARCHAR"),
    ("observed_second_slug", "VARCHAR"),
    ("question", "VARCHAR"),
    ("slug_orientation", "VARCHAR"),
    ("game_id", "VARCHAR"),
    ("match_exclusion_reason", "VARCHAR"),
    ("timing_status", "VARCHAR"),
    ("timing_exclusion_reason", "VARCHAR"),
    ("timing_error_type", "VARCHAR"),
    ("timing_error_message", "VARCHAR"),
    ("moneyline_valid", "BOOLEAN"),
    ("moneyline_exclusion_reason", "VARCHAR"),
    ("timing_valid", "BOOLEAN"),
    ("standard_timing_eligible", "BOOLEAN"),
    ("standard_timing_exclusion_reason", "VARCHAR"),
    ("eligible_for_analysis", "BOOLEAN"),
    ("official_date", "DATE"),
    ("season_start", "INTEGER"),
    ("game_type_code", "VARCHAR"),
    ("status_text", "VARCHAR"),
    ("expected_final_period", "INTEGER"),
    ("away_team_id", "BIGINT"),
    ("away_team_name", "VARCHAR"),
    ("away_team_tricode", "VARCHAR"),
    ("home_team_id", "BIGINT"),
    ("home_team_name", "VARCHAR"),
    ("home_team_tricode", "VARCHAR"),
    ("away_token_id", "VARCHAR"),
    ("home_token_id", "VARCHAR"),
    ("winning_token_id", "VARCHAR"),
    ("winning_outcome", "VARCHAR"),
    ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"),
    ("winner_team_id", "BIGINT"),
    ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"),
    ("home_won", "BOOLEAN"),
    ("postponement_status", "VARCHAR"),
    ("postponement_reason", "VARCHAR"),
    ("irregular_exclusion_reason", "VARCHAR"),
    ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("actual_start_utc", "TIMESTAMPTZ"),
    ("period_2_start_utc", "TIMESTAMPTZ"),
    ("period_3_start_utc", "TIMESTAMPTZ"),
    ("period_4_start_utc", "TIMESTAMPTZ"),
    ("actual_end_utc", "TIMESTAMPTZ"),
    ("actual_start_action_number", "INTEGER"),
    ("actual_start_order_number", "BIGINT"),
    ("period_2_start_action_number", "INTEGER"),
    ("period_2_start_order_number", "BIGINT"),
    ("period_3_start_action_number", "INTEGER"),
    ("period_3_start_order_number", "BIGINT"),
    ("period_4_start_action_number", "INTEGER"),
    ("period_4_start_order_number", "BIGINT"),
    ("actual_end_action_number", "INTEGER"),
    ("actual_end_order_number", "BIGINT"),
    ("actual_start_event", "VARCHAR"),
    ("actual_end_event", "VARCHAR"),
    ("period_boundary_event", "VARCHAR"),
    ("final_period", "INTEGER"),
    ("action_count", "INTEGER"),
    ("timestamp_source", "VARCHAR"),
    ("provider_provenance_sha256", "VARCHAR"),
    ("phase_contract_sha256", "VARCHAR"),
)

ELIGIBLE_SCHEMA = tuple(
    field for field in AUDIT_SCHEMA
    if field[0] not in {
        "match_exclusion_reason", "timing_status", "timing_exclusion_reason",
        "timing_error_type", "timing_error_message", "moneyline_valid",
        "moneyline_exclusion_reason", "timing_valid", "standard_timing_eligible",
        "standard_timing_exclusion_reason", "eligible_for_analysis",
        "irregular_exclusion_reason",
    }
)


class ValidatedUniverseBuildError(ValueError):
    """Raised when independently stored NBA artifacts do not reconcile."""


def _path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _quote(value: str | Path) -> str:
    return str(_path(value)).replace("'", "''")


def _normalized_type(value: str) -> str:
    upper = value.upper()
    return "TIMESTAMP WITH TIME ZONE" if upper == "TIMESTAMPTZ" else upper


def _read_typed_rows(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    schema: tuple[tuple[str, str], ...],
    label: str,
    order_by: str,
) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(f"{label} does not exist: {path}")
    relation = f"read_parquet('{_quote(path)}')"
    observed = {
        row[0]: _normalized_type(row[1])
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    expected_names = [name for name, _ in schema]
    missing = [name for name in expected_names if name not in observed]
    extra = sorted(set(observed) - set(expected_names))
    if missing or extra or list(observed) != expected_names:
        raise ValidatedUniverseBuildError(
            f"{label} columns mismatch; missing={missing}, extra={extra}, "
            f"expected_order={expected_names}, observed_order={list(observed)}"
        )
    wrong = {
        name: (_normalized_type(kind), observed[name])
        for name, kind in schema
        if observed[name] != _normalized_type(kind)
    }
    if wrong:
        raise ValidatedUniverseBuildError(f"{label} has invalid column types: {wrong}")
    names = [name for name, _ in schema]
    selected = ", ".join(f'"{name}"' for name in names)
    values = con.execute(f"SELECT {selected} FROM {relation} ORDER BY {order_by}").fetchall()
    return [dict(zip(names, row, strict=True)) for row in values]


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidatedUniverseBuildError(f"{label} must be a non-empty string")
    return value.strip()


def _same(label: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise ValidatedUniverseBuildError(
            f"Cross-artifact mismatch for {label}: {observed!r} != {expected!r}"
        )


def _unique(
    rows: Iterable[dict[str, Any]], field: str, label: str
) -> dict[Any, dict[str, Any]]:
    result: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row[field]
        if key in (None, ""):
            raise ValidatedUniverseBuildError(f"{label} has missing {field}")
        if key in result:
            raise ValidatedUniverseBuildError(f"{label} has duplicate {field}: {key}")
        result[key] = row
    return result


def _validate_candidates(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not rows:
        raise ValidatedUniverseBuildError("NBA candidate input must be nonempty")
    pattern = re.compile(NBA_SLUG_PATTERN)
    result: dict[str, dict[str, Any]] = {}
    slugs: set[str] = set()
    for row in rows:
        market_id = _require_string(row["market_id"], "candidate market_id")
        slug = _require_string(row["event_slug"], f"candidate {market_id} event_slug")
        first = _require_string(row["away"], f"candidate {market_id} away")
        second = _require_string(row["home"], f"candidate {market_id} home")
        _require_string(row["question"], f"candidate {market_id} question")
        parsed = pattern.fullmatch(slug)
        if not isinstance(row["date"], date) or parsed is None:
            raise ValidatedUniverseBuildError(f"candidate {market_id} has invalid slug/date")
        if parsed.groups() != (first, second, row["date"].isoformat()):
            raise ValidatedUniverseBuildError(f"candidate {market_id} slug fields do not reconcile")
        if market_id in result:
            raise ValidatedUniverseBuildError(f"Duplicate candidate market_id: {market_id}")
        if slug in slugs:
            raise ValidatedUniverseBuildError(f"Duplicate candidate event_slug: {slug}")
        result[market_id] = row
        slugs.add(slug)
    return result


def _validate_schedule(rows: list[dict[str, Any]]) -> dict[str, ScheduleGame]:
    games: dict[str, ScheduleGame] = {}
    for row in rows:
        game = ScheduleGame(**row)
        if game.game_id in games:
            raise ValidatedUniverseBuildError(f"Duplicate schedule game_id: {game.game_id}")
        if game.is_completed:
            try:
                winning_team_id(game)
            except ValueError as exc:
                raise ValidatedUniverseBuildError(str(exc)) from exc
            if (
                game.expected_final_period is None
                and game.status_text.strip().casefold() != "final"
            ):
                raise ValidatedUniverseBuildError(
                    f"Final schedule game {game.game_id} lacks a valid final status"
                )
        elif any(
            value is not None
            for value in (
                game.expected_final_period, game.away_final_score, game.home_final_score,
                game.winner_team_id, game.away_is_winner, game.home_is_winner,
            )
        ):
            raise ValidatedUniverseBuildError(
                f"Nonfinal schedule game {game.game_id} carries final-result fields"
            )
        games[game.game_id] = game
    return games


def _schedule_ids(row: Mapping[str, Any]) -> tuple[str, ...]:
    raw = row["schedule_match_game_ids_json"]
    if not isinstance(raw, str):
        raise ValidatedUniverseBuildError("schedule_match_game_ids_json must be a string")
    try:
        values = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValidatedUniverseBuildError("Invalid schedule_match_game_ids_json") from exc
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise ValidatedUniverseBuildError("schedule match game IDs must be a string list")
    if len(values) != len(set(values)):
        raise ValidatedUniverseBuildError("schedule match game IDs contain duplicates")
    _same(f"{row['market_id']} schedule_match_count", row["schedule_match_count"], len(values))
    return tuple(values)


MATCH_SCHEDULE_FIELDS = {
    "schedule_official_date": "official_date",
    "schedule_scheduled_start_utc": "scheduled_start_utc",
    "schedule_season_start": "season_start",
    "schedule_game_type_code": "game_type_code",
    "schedule_status_text": "status_text",
    "schedule_is_completed": "is_completed",
    "schedule_expected_final_period": "expected_final_period",
    "schedule_postponement_status": "postponement_status",
    "schedule_postponement_reason": "postponement_reason",
    "schedule_away_final_score": "away_final_score",
    "schedule_home_final_score": "home_final_score",
    "schedule_winner_team_id": "winner_team_id",
    "schedule_away_is_winner": "away_is_winner",
    "schedule_home_is_winner": "home_is_winner",
}


def _team_value(team: Any, suffix: str) -> Any:
    return getattr(team, {"team_id": "team_id", "team_name": "name", "team_tricode": "tricode"}[suffix])


def _validate_matches(
    candidates: Mapping[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    games: Mapping[str, ScheduleGame],
) -> tuple[dict[str, GameMatchAudit], dict[str, dict[str, Any]]]:
    stored = _unique(rows, "market_id", "match audit")
    if set(stored) != set(candidates):
        raise ValidatedUniverseBuildError("NBA match audit does not cover exactly the candidates")
    expected_rows = match_market_candidates(candidates.values(), games.values())
    expected = {row.market_id: row for row in expected_rows}
    if len(expected) != len(candidates):
        raise ValidatedUniverseBuildError("NBA matcher did not return one row per candidate")

    for market_id, candidate in candidates.items():
        row = stored[market_id]
        audit = expected[market_id]
        _same(f"{market_id} market_date", row["market_date"], candidate["date"])
        _same(f"{market_id} observed_first_slug", row["observed_first_slug"], candidate["away"])
        _same(f"{market_id} observed_second_slug", row["observed_second_slug"], candidate["home"])
        _same(f"{market_id} slug_orientation", row["slug_orientation"], audit.slug_orientation)
        _same(f"{market_id} matched_game_id", row["matched_game_id"], audit.matched_game_id)
        _same(f"{market_id} match reason", row["match_exclusion_reason"], audit.exclusion_reason)
        ids = _schedule_ids(row)
        _same(
            f"{market_id} schedule matches",
            ids,
            tuple(game.game_id for game in audit.schedule_matches),
        )
        if any(game_id not in games for game_id in ids):
            raise ValidatedUniverseBuildError(f"Match audit {market_id} references absent schedule game")
        unique_game = games[ids[0]] if len(ids) == 1 else None
        for stored_field, game_field in MATCH_SCHEDULE_FIELDS.items():
            _same(
                f"{market_id} {stored_field}", row[stored_field],
                getattr(unique_game, game_field) if unique_game else None,
            )
        for prefix, team in (("away", audit.away_team), ("home", audit.home_team)):
            for suffix in ("team_id", "team_name", "team_tricode"):
                _same(
                    f"{market_id} {prefix}_{suffix}", row[f"{prefix}_{suffix}"],
                    _team_value(team, suffix) if team else None,
                )
    return expected, stored


TIMING_SCHEDULE_FIELDS = {
    "official_date": "official_date", "season_start": "season_start",
    "game_type_code": "game_type_code", "away_team_id": "away_team_id",
    "away_team_name": "away_team_name", "away_team_tricode": "away_team_tricode",
    "home_team_id": "home_team_id", "home_team_name": "home_team_name",
    "home_team_tricode": "home_team_tricode",
    "scheduled_start_utc": "scheduled_start_utc",
    "expected_final_period": "expected_final_period", "status_text": "status_text",
    "postponement_status": "postponement_status",
    "postponement_reason": "postponement_reason",
    "away_final_score": "away_final_score", "home_final_score": "home_final_score",
    "winner_team_id": "winner_team_id", "away_is_winner": "away_is_winner",
    "home_is_winner": "home_is_winner",
}


def _validate_timing_rows(
    rows: list[dict[str, Any]],
    matches: Mapping[str, GameMatchAudit],
    match_rows: Mapping[str, dict[str, Any]],
    games: Mapping[str, ScheduleGame],
    provenance_sha: str,
    phase_contract_sha: str,
) -> dict[str, dict[str, Any]]:
    by_market = _unique(rows, "market_id", "timing audit")
    if not set(by_market) <= set(matches):
        raise ValidatedUniverseBuildError("Timing audit contains non-candidate markets")
    if len({row["game_id"] for row in rows}) != len(rows):
        raise ValidatedUniverseBuildError("Timing audit contains duplicate game_id")
    for market_id, timing in by_market.items():
        match = matches[market_id]
        row = match_rows[market_id]
        if not match.is_matched or match.matched_game_id != timing["game_id"]:
            raise ValidatedUniverseBuildError(f"Timing row has wrong match identity: {market_id}")
        if row["timing_status"] != "passed":
            raise ValidatedUniverseBuildError(f"Timing row lacks passed status: {market_id}")
        game = games[timing["game_id"]]
        for timing_field, game_field in TIMING_SCHEDULE_FIELDS.items():
            _same(
                f"{market_id} timing {timing_field}", timing[timing_field],
                getattr(game, game_field),
            )
        if game.expected_final_period is not None:
            _same(
                f"{market_id} final period",
                timing["final_period"], game.expected_final_period,
            )
        elif game.status_text.strip().casefold() != "final" or timing["final_period"] < 4:
            raise ValidatedUniverseBuildError(
                f"NBA game {game.game_id} lacks compatible observed final-period evidence"
            )
        _same(
            f"{market_id} PBP away final score",
            timing["pbp_away_final_score"], game.away_final_score,
        )
        _same(
            f"{market_id} PBP home final score",
            timing["pbp_home_final_score"], game.home_final_score,
        )
        _same(f"{market_id} timestamp source", timing["timestamp_source"], TIMESTAMP_SOURCE)
        _same(f"{market_id} provenance", timing["provider_provenance_sha256"], provenance_sha)
        _same(f"{market_id} phase contract", timing["phase_contract_sha256"], phase_contract_sha)
        _same(f"{market_id} actual start event", timing["actual_start_event"], ACTUAL_START_EVENT)
        _same(f"{market_id} actual end event", timing["actual_end_event"], ACTUAL_END_EVENT)
        _same(f"{market_id} boundary event", timing["period_boundary_event"], PERIOD_BOUNDARY_EVENT)
        expected_irregular = (
            "postponed_or_rescheduled"
            if game.postponement_reason not in (None, "") else None
        )
        _same(f"{market_id} irregular lineage", timing["irregular_exclusion_reason"], expected_irregular)
        boundaries = tuple(
            timing[field] for field in (
                "actual_start_utc", "period_2_start_utc", "period_3_start_utc",
                "period_4_start_utc", "actual_end_utc",
            )
        )
        if any(value is None for value in boundaries) or not all(
            left < right for left, right in zip(boundaries, boundaries[1:])
        ):
            raise ValidatedUniverseBuildError(f"NBA phase boundaries are invalid: {market_id}")
        actions = tuple(
            timing[field] for field in (
                "actual_start_action_number", "period_2_start_action_number",
                "period_3_start_action_number", "period_4_start_action_number",
                "actual_end_action_number",
            )
        )
        orders = tuple(
            timing[field] for field in (
                "actual_start_order_number", "period_2_start_order_number",
                "period_3_start_order_number", "period_4_start_order_number",
                "actual_end_order_number",
            )
        )
        if any(not isinstance(value, int) or value <= 0 for value in actions + orders):
            raise ValidatedUniverseBuildError(f"NBA boundary action identity is invalid: {market_id}")
        if len(set(actions)) != len(actions) or not all(
            left < right for left, right in zip(orders, orders[1:])
        ):
            raise ValidatedUniverseBuildError(f"NBA boundary action lineage is invalid: {market_id}")
        if not isinstance(timing["action_count"], int) or timing["action_count"] < len(actions):
            raise ValidatedUniverseBuildError(f"NBA timing action count is invalid: {market_id}")

    for market_id, match in matches.items():
        row = match_rows[market_id]
        status = row["timing_status"]
        reason = row["timing_exclusion_reason"]
        errors = (row["timing_error_type"], row["timing_error_message"])
        if match.is_matched and status == "passed":
            if market_id not in by_market or reason is not None or any(value is not None for value in errors):
                raise ValidatedUniverseBuildError(f"Passed timing state is inconsistent: {market_id}")
        elif match.is_matched and status == "failed":
            if market_id in by_market or not reason or any(value in (None, "") for value in errors):
                raise ValidatedUniverseBuildError(f"Failed timing state is inconsistent: {market_id}")
        elif not match.is_matched and status == "not_eligible":
            if market_id in by_market or reason != "not_exact_final_match" or any(value is not None for value in errors):
                raise ValidatedUniverseBuildError(f"Excluded timing state is inconsistent: {market_id}")
        else:
            raise ValidatedUniverseBuildError(f"Unknown timing state for {market_id}: {status!r}")
    return by_market


def _read_and_validate_provenance(
    timing_run: Path,
    candidate_count: int,
    schedule_rows: int,
    matches: Mapping[str, GameMatchAudit],
    match_rows: Mapping[str, dict[str, Any]],
    timing_rows: int,
) -> str:
    summary_path = timing_run / TIMING_SUMMARY_OUTPUT
    manifest_path = timing_run / PROVENANCE_OUTPUT
    if not summary_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError("NBA timing run lacks summary or provider provenance")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidatedUniverseBuildError("NBA timing metadata is invalid JSON") from exc
    sha = hashlib.sha256(manifest_bytes).hexdigest()
    exact_manifest_keys = {
        "schema_version", "schedule_provider", "timing_provider",
        "actual_start_event", "actual_end_event", "period_boundary_event", "resources",
    }
    if not isinstance(manifest, dict) or set(manifest) != exact_manifest_keys:
        raise ValidatedUniverseBuildError("NBA provider provenance schema is invalid")
    expected_manifest = {
        "schema_version": 1,
        "schedule_provider": "official_nba_data_nba_com_historical_schedule",
        "timing_provider": "official_nba_livedata_s3_origin",
        "actual_start_event": ACTUAL_START_EVENT,
        "actual_end_event": ACTUAL_END_EVENT,
        "period_boundary_event": PERIOD_BOUNDARY_EVENT,
    }
    for field, expected in expected_manifest.items():
        _same(f"provider provenance {field}", manifest[field], expected)
    resources = manifest["resources"]
    if not isinstance(resources, list) or not resources:
        raise ValidatedUniverseBuildError("NBA provider provenance lacks resources")
    urls: set[str] = set()
    for resource in resources:
        if not isinstance(resource, dict) or set(resource) != {
            "url", "cache_path", "bytes", "sha256", "source"
        }:
            raise ValidatedUniverseBuildError("NBA provider resource schema is invalid")
        if not isinstance(resource["url"], str) or not resource["url"] or resource["url"] in urls:
            raise ValidatedUniverseBuildError("NBA provider resource URL is invalid or duplicated")
        urls.add(resource["url"])
        if not isinstance(resource["bytes"], int) or resource["bytes"] <= 0:
            raise ValidatedUniverseBuildError("NBA provider resource byte count is invalid")
        if not isinstance(resource["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", resource["sha256"]) is None:
            raise ValidatedUniverseBuildError("NBA provider resource SHA-256 is invalid")
        if resource["source"] not in {"cache", "network", "network_verified_identical"}:
            raise ValidatedUniverseBuildError("NBA provider resource source is invalid")
        try:
            observed = file_fingerprint(resource["cache_path"])
        except FileNotFoundError as exc:
            raise ValidatedUniverseBuildError(
                f"NBA provider cache resource is missing: {resource.get('cache_path')}"
            ) from exc
        if (
            observed["bytes"] != resource["bytes"]
            or observed["sha256"] != resource["sha256"]
        ):
            raise ValidatedUniverseBuildError(
                f"NBA provider cache fingerprint mismatch: {resource['cache_path']}"
            )
    required_summary = {
        "schema_version": 1,
        "schedule_source": "official_nba_historical_schedule",
        "timestamp_source": TIMESTAMP_SOURCE,
        "espn_fallback_used": False,
        "actual_start_event": ACTUAL_START_EVENT,
        "actual_end_event": ACTUAL_END_EVENT,
        "period_boundary_event": PERIOD_BOUNDARY_EVENT,
        "provider_provenance_sha256": sha,
        "candidate_markets": candidate_count,
        "schedule_records": schedule_rows,
        "exact_final_matches": sum(row.is_matched for row in matches.values()),
        "timing_games_written": timing_rows,
        "match_exclusions": dict(sorted(Counter(
            row.exclusion_reason for row in matches.values() if row.exclusion_reason
        ).items())),
        "timing_exclusions": dict(sorted(Counter(
            row["timing_exclusion_reason"] for row in match_rows.values()
            if row["timing_exclusion_reason"]
        ).items())),
    }
    if not isinstance(summary, dict):
        raise ValidatedUniverseBuildError("NBA timing summary must be an object")
    for field, expected in required_summary.items():
        _same(f"timing summary {field}", summary.get(field), expected)
    return sha


def _nonnull(value: Any) -> Any | None:
    if value is None:
        return None
    try:
        return None if bool(pd.isna(value)) else value
    except (TypeError, ValueError):
        return value


_NICKNAMES = {
    "atl": ("Hawks",), "bos": ("Celtics",), "cle": ("Cavaliers",),
    "nop": ("Pelicans",), "chi": ("Bulls",), "dal": ("Mavericks",),
    "den": ("Nuggets",), "gsw": ("Warriors",), "hou": ("Rockets",),
    "lac": ("Clippers",), "lal": ("Lakers",), "mia": ("Heat",),
    "mil": ("Bucks",), "min": ("Timberwolves", "Twolves", "T-Wolves"),
    "bkn": ("Nets",), "nyk": ("Knicks",), "orl": ("Magic",),
    "ind": ("Pacers",), "phi": ("76ers",), "phx": ("Suns", "PHO"),
    "por": ("Trail Blazers",), "sac": ("Kings",), "sas": ("Spurs",),
    "okc": ("Thunder",), "tor": ("Raptors",), "uta": ("Jazz",),
    "mem": ("Grizzlies",), "was": ("Wizards",), "det": ("Pistons",),
    "cha": ("Hornets",),
}


def _label(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return " ".join(value.casefold().split())


def _accepted_labels() -> dict[str, int]:
    labels: dict[str, int] = {}
    for slug, nicknames in _NICKNAMES.items():
        team = NBA_TEAMS[slug]
        for value in (team.name, team.tricode, *nicknames):
            normalized = _label(value)
            assert normalized is not None
            if normalized in labels and labels[normalized] != team.team_id:
                raise AssertionError(f"NBA label collision: {value}")
            labels[normalized] = team.team_id
    return labels


ACCEPTED_LABELS = _accepted_labels()


def _load_token_rows(
    con: duckdb.DuckDBPyConnection,
    candidates: Mapping[str, dict[str, Any]],
    universe_tokens: Path,
    token_map: Path,
) -> dict[str, list[dict[str, Any]]]:
    for path, required in (
        (universe_tokens, {"token_id", "market_id", "winning_outcome"}),
        (token_map, {"token_id", "condition_id", "outcome", "event_slug", "question"}),
    ):
        if not path.is_file():
            raise FileNotFoundError(path)
        columns = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{_quote(path)}')"
            ).fetchall()
        }
        missing = sorted(required - columns)
        if missing:
            raise ValidatedUniverseBuildError(f"{path.name} is missing columns: {missing}")
    ids = pd.DataFrame({"market_id": sorted(candidates)})
    con.register("candidate_ids", ids)
    rows = con.execute(
        f"""
        WITH u AS (
            SELECT u.token_id, u.market_id, u.winning_outcome
            FROM read_parquet('{_quote(universe_tokens)}') u
            JOIN candidate_ids c USING (market_id)
        )
        SELECT u.market_id, u.token_id, u.winning_outcome,
               m.condition_id, m.outcome, m.event_slug, m.question
        FROM u LEFT JOIN read_parquet('{_quote(token_map)}') m USING (token_id)
        ORDER BY u.market_id, u.token_id
        """
    ).fetchdf().to_dict("records")
    grouped = {market_id: [] for market_id in candidates}
    all_tokens: set[str] = set()
    for row in rows:
        market_id = row["market_id"]
        grouped[market_id].append(row)
        token_id = _require_string(row["token_id"], f"{market_id} token_id")
        if token_id in all_tokens:
            raise ValidatedUniverseBuildError(f"NBA candidate token ID is reused: {token_id}")
        all_tokens.add(token_id)
    for market_id, market_rows in grouped.items():
        if len(market_rows) != 2:
            raise ValidatedUniverseBuildError(
                f"NBA candidate {market_id} must have exactly two canonical token rows"
            )
        if any(_nonnull(row["condition_id"]) is None for row in market_rows):
            raise ValidatedUniverseBuildError(f"NBA candidate {market_id} lacks token-map rows")
    return grouped


def _validate_tokens(
    candidate: Mapping[str, Any],
    match: GameMatchAudit,
    game: ScheduleGame | None,
    token_rows: list[dict[str, Any]],
) -> tuple[bool, str | None, dict[str, Any]]:
    market_id = candidate["market_id"]
    for row in token_rows:
        if _nonnull(row["condition_id"]) != market_id:
            raise ValidatedUniverseBuildError(f"NBA token condition_id mismatch: {market_id}")
        if _nonnull(row["question"]) != candidate["question"]:
            raise ValidatedUniverseBuildError(f"NBA token-map identity mismatch: {market_id}")
    token_slugs = [_nonnull(row["event_slug"]) for row in token_rows]
    if not (
        token_slugs == [candidate["event_slug"], candidate["event_slug"]]
        or token_slugs == ["", ""]
    ):
        raise ValidatedUniverseBuildError(
            f"NBA token-map event_slug convention mismatch: {market_id}"
        )
    if not match.is_matched or game is None:
        return False, "upstream_not_exact_final_match", {}
    token_by_team: dict[int, str] = {}
    rows_by_team: dict[int, dict[str, Any]] = {}
    for row in token_rows:
        team_id = ACCEPTED_LABELS.get(_label(row["outcome"]) or "")
        if team_id not in {game.away_team_id, game.home_team_id}:
            return False, "token_outcome_not_matched_team", {}
        if team_id in token_by_team:
            return False, "duplicate_team_token", {}
        token_by_team[team_id] = str(row["token_id"])
        rows_by_team[team_id] = row
    if set(token_by_team) != {game.away_team_id, game.home_team_id}:
        return False, "missing_team_token", {}
    winning_labels = {_label(row["winning_outcome"]) for row in token_rows}
    if None in winning_labels or len(winning_labels) != 1:
        return False, "invalid_winning_outcome", {}
    winning_label = next(iter(winning_labels))
    market_winner = ACCEPTED_LABELS.get(winning_label or "")
    if market_winner not in {game.away_team_id, game.home_team_id}:
        return False, "winner_not_matched_team", {}
    result = {
        "away_token_id": token_by_team[game.away_team_id],
        "home_token_id": token_by_team[game.home_team_id],
        "winning_token_id": token_by_team[market_winner],
        "winning_outcome": rows_by_team[market_winner]["outcome"],
        "home_won": market_winner == game.home_team_id,
    }
    if winning_team_id(game) != market_winner:
        return False, "polymarket_nba_winner_disagreement", result
    return True, None, result


def _standard_timing_decision(
    match: GameMatchAudit,
    match_row: Mapping[str, Any],
    timing: Mapping[str, Any] | None,
) -> tuple[bool, str | None]:
    if not match.is_matched:
        return False, f"upstream_match_{match.exclusion_reason or 'unmatched'}"
    if match_row["timing_status"] != "passed":
        return False, f"upstream_timing_{match_row['timing_exclusion_reason'] or 'failed'}"
    if timing is None:
        return False, "missing_timing_row"
    if timing["irregular_exclusion_reason"]:
        return False, str(timing["irregular_exclusion_reason"])
    return True, None


def _canonical_values(
    game: ScheduleGame | None, timing: Mapping[str, Any] | None
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for field in (
        "official_date", "season_start", "game_type_code", "status_text",
        "expected_final_period", "away_team_id", "away_team_name", "away_team_tricode",
        "home_team_id", "home_team_name", "home_team_tricode", "away_final_score",
        "home_final_score", "winner_team_id", "away_is_winner", "home_is_winner",
        "postponement_status", "postponement_reason", "scheduled_start_utc",
    ):
        values[field] = getattr(game, field) if game else None
    for field in (
        "irregular_exclusion_reason", "actual_start_utc", "period_2_start_utc",
        "period_3_start_utc", "period_4_start_utc", "actual_end_utc",
        "actual_start_action_number", "actual_start_order_number",
        "period_2_start_action_number", "period_2_start_order_number",
        "period_3_start_action_number", "period_3_start_order_number",
        "period_4_start_action_number", "period_4_start_order_number",
        "actual_end_action_number", "actual_end_order_number",
        "actual_start_event", "actual_end_event", "period_boundary_event",
        "final_period", "action_count", "timestamp_source", "provider_provenance_sha256",
        "phase_contract_sha256",
    ):
        values[field] = timing[field] if timing else None
    values["home_won"] = game.home_is_winner if game else None
    return values


def _write_rows(
    rows: Iterable[Mapping[str, Any]],
    schema: tuple[tuple[str, str], ...],
    path: Path,
    order_by: str,
) -> None:
    names = [name for name, _ in schema]
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE output (" + ",".join(f'\"{n}\" {t}' for n, t in schema) + ")")
        materialized = list(rows)
        if materialized:
            con.executemany(
                "INSERT INTO output VALUES (" + ",".join("?" for _ in names) + ")",
                [[row.get(name) for name in names] for row in materialized],
            )
        con.execute(
            f"COPY (SELECT * FROM output ORDER BY {order_by}) TO '{_quote(path)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def _verify_outputs(staging: Path, candidate_ids: set[str], eligible_count: int) -> None:
    require_parquet_schema(staging / AUDIT_OUTPUT, AUDIT_SCHEMA, AUDIT_OUTPUT)
    require_parquet_schema(staging / ELIGIBLE_OUTPUT, ELIGIBLE_SCHEMA, ELIGIBLE_OUTPUT)
    con = duckdb.connect()
    try:
        audit = f"read_parquet('{_quote(staging / AUDIT_OUTPUT)}')"
        rows, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT market_id) FROM {audit}"
        ).fetchone()
        written_ids = {row[0] for row in con.execute(f"SELECT market_id FROM {audit}").fetchall()}
        eligible = f"read_parquet('{_quote(staging / ELIGIBLE_OUTPUT)}')"
        eligible_rows, games, tokens = con.execute(
            f"SELECT count(*), count(DISTINCT game_id), "
            f"count(DISTINCT away_token_id) + count(DISTINCT home_token_id) FROM {eligible}"
        ).fetchone()
    finally:
        con.close()
    if rows != len(candidate_ids) or distinct != rows or written_ids != candidate_ids:
        raise ValidatedUniverseBuildError("Written NBA audit is not one row per candidate")
    if eligible_rows != eligible_count or games != eligible_rows or tokens != 2 * eligible_rows:
        raise ValidatedUniverseBuildError("Written NBA eligible dimensions are not unique")


def build_validated_universe(
    candidate_path: str | Path,
    timing_run_dir: str | Path,
    universe_tokens_path: str | Path,
    token_map_path: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    candidate_path = _path(candidate_path)
    timing_run = _path(timing_run_dir)
    universe_tokens = _path(universe_tokens_path)
    token_map = _path(token_map_path)
    output = _path(output_dir)
    inputs = (candidate_path, timing_run, universe_tokens, token_map)
    if output.exists():
        raise FileExistsError(f"NBA validated output already exists: {output}")
    for source in inputs:
        if output == source or output.is_relative_to(source) or source.is_relative_to(output):
            raise ValidatedUniverseBuildError("NBA validated output must not overlap inputs")
    if not timing_run.is_dir():
        raise FileNotFoundError(f"NBA timing run does not exist: {timing_run}")
    timing_summary = verify_game_timing_run(timing_run)
    timing_manifest = json.loads(
        (timing_run / "timing_manifest.json").read_text(encoding="utf-8")
    )
    timing_candidate = verify_fingerprint(timing_manifest["inputs"]["candidates"])
    if timing_candidate != candidate_path:
        raise ValidatedUniverseBuildError(
            "NBA validated candidate input is not the fingerprinted timing input"
        )
    contract_record = timing_manifest["inputs"]["phase_contract"]
    contract_path = verify_fingerprint(contract_record)
    contract = load_phase_contract(contract_path)
    phase_contract_sha = str(contract_record["sha256"])
    if (
        contract.sport != "nba"
        or contract.contract_version != 2
        or contract.regulation_period_minutes != 12
        or tuple(phase.key for phase in contract.analysis_phases) != NBA_ANALYSIS_PHASES
        or contract.actual_start_event != ACTUAL_START_EVENT
        or contract.actual_end_event != ACTUAL_END_EVENT
        or contract.period_boundary_event != PERIOD_BOUNDARY_EVENT
    ):
        raise ValidatedUniverseBuildError("NBA phase contract semantics do not reconcile")
    if tuple(timing_summary.get("analysis_phases", ())) != NBA_ANALYSIS_PHASES:
        raise ValidatedUniverseBuildError("NBA timing run does not declare five eligible phases")

    con = duckdb.connect()
    try:
        candidate_rows = _read_typed_rows(
            con, candidate_path, CANDIDATE_SCHEMA, "NBA candidates", "date, market_id"
        )
        schedule_rows = _read_typed_rows(
            con, timing_run / SCHEDULE_OUTPUT, SCHEDULE_SCHEMA, "NBA schedule audit",
            "official_date, game_id",
        )
        match_rows = _read_typed_rows(
            con, timing_run / MATCH_OUTPUT, MATCH_SCHEMA, "NBA match audit", "market_id"
        )
        timing_rows = _read_typed_rows(
            con, timing_run / TIMING_OUTPUT, TIMING_SCHEMA, "NBA timing audit",
            "official_date, game_id",
        )
        candidates = _validate_candidates(candidate_rows)
        games = _validate_schedule(schedule_rows)
        matches, matches_stored = _validate_matches(candidates, match_rows, games)
        provenance_sha = _read_and_validate_provenance(
            timing_run, len(candidates), len(games), matches, matches_stored, len(timing_rows)
        )
        timings = _validate_timing_rows(
            timing_rows, matches, matches_stored, games, provenance_sha,
            phase_contract_sha,
        )
        tokens = _load_token_rows(con, candidates, universe_tokens, token_map)
    finally:
        con.close()

    audit_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, Any]] = []
    moneyline_reasons: Counter[str] = Counter()
    timing_reasons: Counter[str] = Counter()
    for market_id, candidate in candidates.items():
        match = matches[market_id]
        match_row = matches_stored[market_id]
        game = games[match.matched_game_id] if match.is_matched else None
        timing = timings.get(market_id)
        moneyline_valid, moneyline_reason, token_values = _validate_tokens(
            candidate, match, game, tokens[market_id]
        )
        timing_eligible, timing_reason = _standard_timing_decision(match, match_row, timing)
        is_eligible = moneyline_valid and timing_eligible
        if moneyline_reason:
            moneyline_reasons[moneyline_reason] += 1
        if timing_reason:
            timing_reasons[timing_reason] += 1
        audit = {
            "market_id": market_id,
            "event_slug": candidate["event_slug"],
            "market_date": candidate["date"],
            "observed_first_slug": candidate["away"],
            "observed_second_slug": candidate["home"],
            "question": candidate["question"],
            "slug_orientation": match.slug_orientation,
            "game_id": match.matched_game_id,
            "match_exclusion_reason": match.exclusion_reason,
            "timing_status": match_row["timing_status"],
            "timing_exclusion_reason": match_row["timing_exclusion_reason"],
            "timing_error_type": match_row["timing_error_type"],
            "timing_error_message": match_row["timing_error_message"],
            "moneyline_valid": moneyline_valid,
            "moneyline_exclusion_reason": moneyline_reason,
            "timing_valid": timing is not None and match_row["timing_status"] == "passed",
            "standard_timing_eligible": timing_eligible,
            "standard_timing_exclusion_reason": timing_reason,
            "eligible_for_analysis": is_eligible,
            **_canonical_values(game, timing),
            **token_values,
        }
        audit_rows.append(audit)
        if is_eligible:
            eligible = {name: audit[name] for name, _ in ELIGIBLE_SCHEMA}
            boundaries = {
                name: eligible[name] for name in contract.required_boundaries
            }
            expected_at_boundaries = {
                "actual_start_utc": "quarter_1",
                "period_2_start_utc": "quarter_2",
                "period_3_start_utc": "quarter_3",
                "period_4_start_utc": "quarter_4_plus",
                "actual_end_utc": "quarter_4_plus",
            }
            for boundary, expected_phase in expected_at_boundaries.items():
                observed_phase = classify_contract_timestamp(
                    contract, boundaries, eligible[boundary]
                )
                if observed_phase != expected_phase:
                    raise ValidatedUniverseBuildError(
                        f"NBA eligible-row contract handoff failed at {boundary}"
                    )
            eligible_rows.append(eligible)

    summary = {
        "schema_version": 1,
        "definition": "validated_nba_moneyline_standard_timing_v1",
        "counts": {
            "candidate_markets": len(candidates),
            "moneyline_valid": sum(row["moneyline_valid"] for row in audit_rows),
            "timing_valid": sum(row["timing_valid"] for row in audit_rows),
            "standard_timing_eligible": sum(
                row["standard_timing_eligible"] for row in audit_rows
            ),
            "eligible_moneylines": len(eligible_rows),
        },
        "exclusion_counts": {
            "moneyline": dict(sorted(moneyline_reasons.items())),
            "standard_timing": dict(sorted(timing_reasons.items())),
        },
        "timestamp_semantics": {
            "actual_start_event": ACTUAL_START_EVENT,
            "actual_end_event": ACTUAL_END_EVENT,
            "period_boundary_event": PERIOD_BOUNDARY_EVENT,
        },
        "provider_provenance_sha256": provenance_sha,
        "phase_contract_sha256": phase_contract_sha,
        "outputs": {
            "candidate_validation_audit": AUDIT_OUTPUT,
            "eligible_moneylines": ELIGIBLE_OUTPUT,
        },
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:
        _write_rows(audit_rows, AUDIT_SCHEMA, staging / AUDIT_OUTPUT, "market_date, market_id")
        _write_rows(eligible_rows, ELIGIBLE_SCHEMA, staging / ELIGIBLE_OUTPUT, "official_date, game_id")
        (staging / SUMMARY_OUTPUT).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        _verify_outputs(staging, set(candidates), len(eligible_rows))
        schemas = {AUDIT_OUTPUT: AUDIT_SCHEMA, ELIGIBLE_OUTPUT: ELIGIBLE_SCHEMA}
        manifest = {
            "schema_version": 1,
            "stage": "nba_validated_universe",
            "inputs": {
                "candidates": file_fingerprint(candidate_path),
                "timing_manifest": file_fingerprint(timing_run / "timing_manifest.json"),
                "universe_tokens": file_fingerprint(universe_tokens),
                "token_map": file_fingerprint(token_map),
                "phase_contract": phase_contract_fingerprint(contract_path),
            },
            "schemas": {
                name: [list(row) for row in schema] for name, schema in schemas.items()
            },
            "outputs": {
                name: file_fingerprint(staging / name, relative_to=staging)
                for name in (*schemas, SUMMARY_OUTPUT)
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if output.exists():
            raise FileExistsError(f"NBA validated output appeared during build: {output}")
        os.rename(staging, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True)
    parser.add_argument(
        "--timing-run-dir", "--timing-run", dest="timing_run", required=True
    )
    parser.add_argument("--universe-tokens", required=True)
    parser.add_argument("--token-map", required=True)
    parser.add_argument("--run-dir", "--output-dir", dest="output_dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = build_validated_universe(
        args.candidates, args.timing_run, args.universe_tokens,
        args.token_map, args.output_dir,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
