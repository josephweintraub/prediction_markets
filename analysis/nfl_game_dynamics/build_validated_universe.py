"""Compose the validated NFL moneyline and complete-quarter timing universe."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .artifact_manifest import (
    ArtifactManifestError,
    file_fingerprint,
    require_parquet_schema,
    verify_fingerprint,
)
from .build_game_timing import _write_parquet, verify_timing_run
from .build_market_universe import CANDIDATE_SCHEMA, NFL_SLUG_PATTERN
from .match_games import assert_one_to_one_matches, match_market_candidates
from .nfl_api import NFL_ANALYSIS_PHASES, NFL_PHASE_CONTRACT_SHA256, ScheduleGame
from .validate_moneylines import (
    assert_unique_eligible_assignments,
    load_canonical_token_rows,
    validate_moneylines,
)


AUDIT_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"),
    ("market_date", "DATE"), ("team_1_slug", "VARCHAR"),
    ("team_2_slug", "VARCHAR"), ("slug_orientation", "VARCHAR"),
    ("matched_game_id", "VARCHAR"), ("match_exclusion_reason", "VARCHAR"),
    ("moneyline_valid", "BOOLEAN"), ("moneyline_exclusion_reason", "VARCHAR"),
    ("core_timing_eligible", "BOOLEAN"), ("timing_exclusion_reason", "VARCHAR"),
    ("eligible_for_core_analysis", "BOOLEAN"), ("season", "INTEGER"),
    ("season_type", "INTEGER"), ("week", "INTEGER"),
    ("official_date", "DATE"), ("neutral_site", "BOOLEAN"),
    ("went_to_overtime", "BOOLEAN"), ("phase_contract_sha256", "VARCHAR"),
)
ELIGIBLE_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"),
    ("market_date", "DATE"), ("team_1_slug", "VARCHAR"),
    ("team_2_slug", "VARCHAR"), ("slug_orientation", "VARCHAR"),
    ("game_id", "VARCHAR"), ("official_date", "DATE"),
    ("season", "INTEGER"), ("season_type", "INTEGER"), ("week", "INTEGER"),
    ("neutral_site", "BOOLEAN"), ("away_team_id", "INTEGER"),
    ("away_team_name", "VARCHAR"), ("home_team_id", "INTEGER"),
    ("home_team_name", "VARCHAR"), ("away_token_id", "VARCHAR"),
    ("home_token_id", "VARCHAR"), ("winning_team_id", "INTEGER"),
    ("winning_token_id", "VARCHAR"), ("winning_outcome", "VARCHAR"),
    ("away_final_score", "INTEGER"), ("home_final_score", "INTEGER"),
    ("home_won", "BOOLEAN"),
    ("scheduled_start_utc", "TIMESTAMPTZ"), ("actual_start_utc", "TIMESTAMPTZ"),
    ("period_2_start_utc", "TIMESTAMPTZ"), ("period_3_start_utc", "TIMESTAMPTZ"),
    ("period_4_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ"),
    ("competitive_play_count", "INTEGER"), ("final_period", "INTEGER"),
    ("went_to_overtime", "BOOLEAN"), ("phase_contract_sha256", "VARCHAR"),
)
AUDIT_OUTPUT = "candidate_validation_audit.parquet"
ELIGIBLE_OUTPUT = "eligible_moneylines.parquet"
SUMMARY_OUTPUT = "summary.json"
MANIFEST_OUTPUT = "validated_manifest.json"


class ValidatedUniverseBuildError(ValueError):
    pass


def _read(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    con = duckdb.connect()
    try:
        quoted = str(path).replace("'", "''")
        return con.execute(f"SELECT * FROM read_parquet('{quoted}')").fetchdf().to_dict("records")
    finally:
        con.close()


def _none(value: Any) -> Any:
    # Pandas represents nullable artifact values as NaN/NaT.
    return None if pd.isna(value) else value


def _schedule(row: dict[str, Any]) -> ScheduleGame:
    fields = ScheduleGame.__dataclass_fields__
    values = {name: _none(row.get(name)) for name in fields}
    official_date = values["official_date"]
    if hasattr(official_date, "date"):
        values["official_date"] = official_date.date()
    return ScheduleGame(**values)


def _validate_schedule_game(game: ScheduleGame) -> None:
    if not game.is_completed:
        if any(value is not None for value in (
            game.expected_final_period, game.away_final_score,
            game.home_final_score, game.away_is_winner, game.home_is_winner,
        )):
            raise ValidatedUniverseBuildError(
                f"Nonfinal schedule game carries final fields: {game.game_id}"
            )
        return
    detail_match = (
        re.fullmatch(r"Final(?:/(?:([2-9]\d*)?OT))?", game.status_detail)
        if isinstance(game.status_detail, str)
        else None
    )
    expected_period = (
        4 if game.status_detail == "Final" else 4 + int(detail_match.group(1) or 1)
    ) if detail_match is not None else None
    if (
        game.status_state.casefold() != "post"
        or expected_period is None
        or game.expected_final_period != expected_period
    ):
        raise ValidatedUniverseBuildError(f"Invalid final schedule status: {game.game_id}")
    values = (game.away_final_score, game.home_final_score)
    if any(not isinstance(value, int) or value < 0 for value in values):
        raise ValidatedUniverseBuildError(f"Invalid final schedule scores: {game.game_id}")
    if game.away_final_score == game.home_final_score:
        if (game.away_is_winner, game.home_is_winner) != (False, False):
            raise ValidatedUniverseBuildError(
                f"Tied final schedule winner flags are invalid: {game.game_id}"
            )
        return
    if (game.away_is_winner, game.home_is_winner) not in {(True, False), (False, True)}:
        raise ValidatedUniverseBuildError(f"Invalid final schedule winner flags: {game.game_id}")
    if game.away_is_winner is not (game.away_final_score > game.home_final_score):
        raise ValidatedUniverseBuildError(f"Schedule score/winner mismatch: {game.game_id}")


def _validate_candidate_rows(rows: list[dict[str, Any]]) -> None:
    import re
    if not rows:
        raise ValidatedUniverseBuildError("Candidate input must be nonempty")
    ids: set[str] = set()
    slugs: set[str] = set()
    pattern = re.compile(NFL_SLUG_PATTERN)
    for row in rows:
        market_id = row.get("market_id")
        slug = row.get("event_slug")
        parsed = pattern.fullmatch(str(slug))
        market_date = row.get("date")
        if hasattr(market_date, "date"):
            market_date = market_date.date()
        expected = (
            str(row.get("team_1_slug")),
            str(row.get("team_2_slug")),
            market_date.isoformat() if hasattr(market_date, "isoformat") else str(market_date),
        )
        if not isinstance(market_id, str) or not market_id or parsed is None or parsed.groups() != expected:
            raise ValidatedUniverseBuildError(f"Invalid/reconstructed candidate row: {market_id}")
        if market_id in ids or slug in slugs:
            raise ValidatedUniverseBuildError("Candidate market/event IDs must be unique")
        ids.add(market_id); slugs.add(slug)


def _check_match_artifact(recomputed: tuple[Any, ...], serialized: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if len(recomputed) != len(serialized):
        raise ValidatedUniverseBuildError("Match artifact must have one row per candidate")
    by_market: dict[str, dict[str, Any]] = {}
    for row in serialized:
        market_id = row.get("market_id")
        if market_id in by_market:
            raise ValidatedUniverseBuildError("Duplicate match-artifact market_id")
        by_market[market_id] = row
    for match in recomputed:
        row = by_market.get(match.market_id)
        if row is None:
            raise ValidatedUniverseBuildError(f"Missing match artifact row: {match.market_id}")
        expected = {
            "matched_game_id": match.matched_game_id,
            "match_exclusion_reason": match.exclusion_reason,
            "slug_orientation": match.slug_orientation,
            "away_team_id": match.away_team.team_id if match.away_team else None,
            "away_team_name": match.away_team.name if match.away_team else None,
            "home_team_id": match.home_team.team_id if match.home_team else None,
            "home_team_name": match.home_team.name if match.home_team else None,
            "schedule_match_count": len(match.schedule_matches),
        }
        for key, value in expected.items():
            if _none(row.get(key)) != value:
                raise ValidatedUniverseBuildError(f"Cross-artifact match mismatch {match.market_id} {key}")
        decoded = json.loads(row["schedule_match_game_ids_json"])
        if decoded != [game.game_id for game in match.schedule_matches]:
            raise ValidatedUniverseBuildError(f"Cross-artifact schedule matches mismatch {match.market_id}")
    return by_market


def verify_validated_run(run_dir: str | Path) -> dict[str, Any]:
    run = Path(run_dir).expanduser().resolve()
    schemas = {AUDIT_OUTPUT: AUDIT_SCHEMA, ELIGIBLE_OUTPUT: ELIGIBLE_SCHEMA}
    expected_files = {*schemas, SUMMARY_OUTPUT, MANIFEST_OUTPUT}
    if not run.is_dir() or {path.name for path in run.iterdir()} != expected_files:
        raise ArtifactManifestError("NFL validated run file set is incomplete or unexpected")
    manifest = json.loads((run / MANIFEST_OUTPUT).read_text(encoding="utf-8"))
    expected_keys = {
        "schema_version", "stage", "inputs", "schemas", "outputs",
        "counts", "uniqueness",
    }
    if not isinstance(manifest, dict) or set(manifest) != expected_keys:
        raise ArtifactManifestError("NFL validated manifest keys mismatch")
    if manifest["schema_version"] != 1 or manifest["stage"] != "nfl_validated_universe":
        raise ArtifactManifestError("NFL validated manifest identity mismatch")
    input_keys = {"candidates", "timing_manifest", "universe_tokens", "token_map"}
    if set(manifest["inputs"]) != input_keys:
        raise ArtifactManifestError("NFL validated input manifest mismatch")
    input_paths = {
        name: verify_fingerprint(record)
        for name, record in manifest["inputs"].items()
    }
    timing_manifest_path = input_paths["timing_manifest"]
    if timing_manifest_path.name != "timing_manifest.json":
        raise ArtifactManifestError("NFL validated timing-manifest path mismatch")
    verify_timing_run(timing_manifest_path.parent)
    timing_manifest = json.loads(timing_manifest_path.read_text(encoding="utf-8"))
    pinned_candidate = timing_manifest["inputs"]["candidates"]
    validated_candidate = manifest["inputs"]["candidates"]
    if any(
        pinned_candidate.get(key) != validated_candidate.get(key)
        for key in ("path", "bytes", "sha256")
    ):
        raise ArtifactManifestError(
            "NFL validated candidate fingerprint does not match the timing manifest"
        )
    require_parquet_schema(input_paths["candidates"], CANDIDATE_SCHEMA, "candidate input")
    declared_schemas = {name: [list(row) for row in schema] for name, schema in schemas.items()}
    if manifest["schemas"] != declared_schemas:
        raise ArtifactManifestError("NFL validated declared schemas mismatch")
    if set(manifest["outputs"]) != {*schemas, SUMMARY_OUTPUT}:
        raise ArtifactManifestError("NFL validated outputs manifest mismatch")
    for name, schema in schemas.items():
        require_parquet_schema(run / name, schema, name)
    for name, record in manifest["outputs"].items():
        if verify_fingerprint(record, base_dir=run) != run / name:
            raise ArtifactManifestError("NFL validated output path mismatch")
    summary = json.loads((run / SUMMARY_OUTPUT).read_text(encoding="utf-8"))
    if summary.get("phase_contract_sha256") != NFL_PHASE_CONTRACT_SHA256:
        raise ArtifactManifestError("NFL validated phase contract mismatch")
    if summary.get("analysis_phases") != list(NFL_ANALYSIS_PHASES):
        raise ArtifactManifestError(
            "NFL validated analysis phases do not match the frozen contract"
        )
    con = duckdb.connect()
    try:
        audit = str(run / AUDIT_OUTPUT).replace("'", "''")
        eligible = str(run / ELIGIBLE_OUTPUT).replace("'", "''")
        audit_count, audit_markets, audit_moneyline, audit_timing, audit_eligible = con.execute(
            f"SELECT count(*),count(DISTINCT market_id),"
            f"count(*) FILTER (WHERE moneyline_valid),"
            f"count(*) FILTER (WHERE core_timing_eligible),"
            f"count(*) FILTER (WHERE eligible_for_core_analysis) "
            f"FROM read_parquet('{audit}')"
        ).fetchone()
        candidate = str(input_paths["candidates"]).replace("'", "''")
        candidate_ids = {
            row[0] for row in con.execute(
                f"SELECT market_id FROM read_parquet('{candidate}')"
            ).fetchall()
        }
        audit_ids = {
            row[0] for row in con.execute(
                f"SELECT market_id FROM read_parquet('{audit}')"
            ).fetchall()
        }
        eligible_count, eligible_markets, eligible_games = con.execute(
            f"SELECT count(*),count(DISTINCT market_id),count(DISTINCT game_id) "
            f"FROM read_parquet('{eligible}')"
        ).fetchone()
        token_count = con.execute(
            f"SELECT count(DISTINCT token_id) FROM ("
            f"SELECT away_token_id AS token_id FROM read_parquet('{eligible}') UNION ALL "
            f"SELECT home_token_id AS token_id FROM read_parquet('{eligible}'))"
        ).fetchone()[0]
        invalid_contract_rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{eligible}') "
            f"WHERE phase_contract_sha256 IS DISTINCT FROM ?", [NFL_PHASE_CONTRACT_SHA256]
        ).fetchone()[0]
    finally:
        con.close()
    if invalid_contract_rows:
        raise ArtifactManifestError(
            "Every NFL eligible row must carry the exact frozen phase contract SHA"
        )
    observed_counts = {
        "candidate_markets": audit_count,
        "moneyline_valid": audit_moneyline,
        "core_timing_eligible": audit_timing,
        "eligible_moneylines": eligible_count,
    }
    if manifest["counts"] != observed_counts:
        raise ArtifactManifestError("NFL validated counts do not reconcile")
    uniqueness = {
        "audit_market_id_unique": audit_markets == audit_count,
        "eligible_market_id_unique": eligible_markets == eligible_count,
        "eligible_game_id_unique": eligible_games == eligible_count,
        "eligible_token_id_unique": token_count == 2 * eligible_count,
    }
    if not all(uniqueness.values()) or manifest["uniqueness"] != uniqueness:
        raise ArtifactManifestError("NFL validated uniqueness does not reconcile")
    if candidate_ids != audit_ids:
        raise ArtifactManifestError("NFL validated audit does not cover the candidate input")
    if audit_eligible != eligible_count or summary.get("candidate_markets") != audit_count:
        raise ArtifactManifestError("NFL validated summary/audit counts do not reconcile")
    if any(summary.get(key) != value for key, value in observed_counts.items()):
        raise ArtifactManifestError("NFL validated summary eligible count does not reconcile")
    return summary


def build_validated_universe(
    candidates_path: str | Path,
    timing_run_dir: str | Path,
    universe_tokens_path: str | Path,
    token_map_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    candidates_path = Path(candidates_path).expanduser().resolve()
    timing_run = Path(timing_run_dir).expanduser().resolve()
    universe_tokens_path = Path(universe_tokens_path).expanduser().resolve()
    token_map_path = Path(token_map_path).expanduser().resolve()
    target = Path(run_dir).expanduser().resolve()
    inputs = (candidates_path, timing_run, universe_tokens_path, token_map_path)
    if any(target == item or target in item.parents or item in target.parents for item in inputs):
        raise ValidatedUniverseBuildError("Output run directory overlaps an input")
    if target.exists():
        raise FileExistsError(f"Immutable NFL validated run exists: {target}")
    verify_timing_run(timing_run)
    timing_manifest = json.loads((timing_run / "timing_manifest.json").read_text())
    pinned_candidate = timing_manifest["inputs"]["candidates"]
    observed_candidate = file_fingerprint(candidates_path)
    if any(pinned_candidate[key] != observed_candidate[key] for key in ("bytes", "sha256")):
        raise ValidatedUniverseBuildError(
            "Validated candidate input does not match the timing-manifest candidate SHA"
        )
    require_parquet_schema(candidates_path, CANDIDATE_SCHEMA, "candidate input")
    candidates = _read(candidates_path)
    _validate_candidate_rows(candidates)
    schedules = tuple(_schedule(row) for row in _read(timing_run / "schedule_audit.parquet"))
    if len({game.game_id for game in schedules}) != len(schedules):
        raise ValidatedUniverseBuildError("Schedule game IDs are duplicated")
    for game in schedules:
        _validate_schedule_game(game)
    schedules_by_id = {row.game_id: row for row in schedules}
    matches = match_market_candidates(candidates, schedules)
    assert_one_to_one_matches(matches)
    match_artifact = _check_match_artifact(matches, _read(timing_run / "match_audit.parquet"))
    timing_rows = _read(timing_run / "game_timing.parquet")
    timing_by_game: dict[str, dict[str, Any]] = {}
    for row in timing_rows:
        game_id = row.get("game_id")
        if game_id in timing_by_game:
            raise ValidatedUniverseBuildError("Duplicate timing game_id")
        boundaries = [_none(row.get(name)) for name in (
            "actual_start_utc", "period_2_start_utc", "period_3_start_utc",
            "period_4_start_utc", "actual_end_utc")]
        if any(value is None for value in boundaries) or not all(a < b for a, b in zip(boundaries, boundaries[1:])):
            raise ValidatedUniverseBuildError(f"Invalid serialized timing boundaries: {game_id}")
        final_period = _none(row.get("final_period"))
        overtime = _none(row.get("went_to_overtime"))
        play_count = _none(row.get("competitive_play_count"))
        if (
            not isinstance(final_period, int)
            or final_period < 4
            or overtime is not (final_period > 4)
            or not isinstance(play_count, int)
            or play_count <= 0
            or row.get("source_provider") != "ESPN site API (third-party undocumented endpoint)"
            or row.get("timestamp_semantics")
            != (
                "competitive-play wallclock; final boundary is the final competitive-play "
                "timestamp with subsequent terminal End of Game evidence"
            )
            or row.get("phase_contract_sha256") != NFL_PHASE_CONTRACT_SHA256
        ):
            raise ValidatedUniverseBuildError(f"Invalid serialized timing metadata: {game_id}")
        game = schedules_by_id.get(game_id)
        if game is None:
            raise ValidatedUniverseBuildError(f"Timing artifact game is absent from schedule: {game_id}")
        exact_fields = {
            "away_team_id": game.away_team_id,
            "home_team_id": game.home_team_id,
            "away_final_score": game.away_final_score,
            "home_final_score": game.home_final_score,
            "away_is_winner": game.away_is_winner,
            "home_is_winner": game.home_is_winner,
            "status_detail": game.status_detail,
            "final_period": game.expected_final_period,
        }
        for field, expected in exact_fields.items():
            if _none(row.get(field)) != expected:
                raise ValidatedUniverseBuildError(
                    f"Cross-artifact timing mismatch {game_id} {field}"
                )
        timing_by_game[game_id] = row
    matched_game_ids = {match.matched_game_id for match in matches if match.is_matched}
    extra_timing = sorted(set(timing_by_game) - matched_game_ids)
    if extra_timing:
        raise ValidatedUniverseBuildError(f"Timing artifact contains unmatched games: {extra_timing}")
    expected_timing: set[str] = set()
    for match in matches:
        serialized = match_artifact[match.market_id]
        status = serialized.get("timing_status")
        reason = _none(serialized.get("timing_exclusion_reason"))
        error_type = _none(serialized.get("timing_error_type"))
        error_message = _none(serialized.get("timing_error_message"))
        if match.is_matched and status == "parsed" and reason is None:
            if error_type is not None or error_message is not None:
                raise ValidatedUniverseBuildError(
                    f"Parsed timing row carries an error: {match.market_id}"
                )
            expected_timing.add(match.matched_game_id)
        elif match.is_matched and not (
            status == "excluded" and reason == "timing_fetch_or_parse_failure"
            and isinstance(error_type, str) and error_type
            and isinstance(error_message, str) and error_message
        ):
            raise ValidatedUniverseBuildError(
                f"Matched timing status is inconsistent: {match.market_id}"
            )
        elif not match.is_matched and not (
            status == "not_attempted" and reason == "not_exact_final_match"
            and error_type is None and error_message is None
        ):
            raise ValidatedUniverseBuildError(
                f"Unmatched timing status is inconsistent: {match.market_id}"
            )
    if set(timing_by_game) != expected_timing:
        raise ValidatedUniverseBuildError(
            "Timing rows do not reconcile with parsed match-audit statuses"
        )
    con = duckdb.connect()
    try:
        tokens = load_canonical_token_rows(con, universe_tokens_path, token_map_path,
                                           [row["market_id"] for row in candidates])
    finally:
        con.close()
    validation = validate_moneylines(candidates, matches, tokens)
    assert_unique_eligible_assignments(validation.eligible_markets)
    moneyline_by_market = {row.market_id: row for row in validation.audits}
    eligible_dims = {row.market_id: row for row in validation.eligible_markets}

    audit_rows: list[tuple[Any, ...]] = []
    eligible_rows: list[tuple[Any, ...]] = []
    for candidate, match in zip(candidates, matches, strict=True):
        moneyline = moneyline_by_market[match.market_id]
        serialized_match = match_artifact[match.market_id]
        timing = timing_by_game.get(match.matched_game_id)
        timing_ok = (
            match.is_matched and timing is not None
            and serialized_match.get("timing_status") == "parsed"
            and _none(serialized_match.get("timing_exclusion_reason")) is None
        )
        eligible = moneyline.is_valid and timing_ok
        game = schedules_by_id.get(match.matched_game_id)
        audit_values = {
            "market_id": match.market_id, "event_slug": candidate["event_slug"],
            "market_date": match.market_date, "team_1_slug": match.team_1_slug,
            "team_2_slug": match.team_2_slug, "slug_orientation": match.slug_orientation,
            "matched_game_id": match.matched_game_id,
            "match_exclusion_reason": match.exclusion_reason,
            "moneyline_valid": moneyline.is_valid,
            "moneyline_exclusion_reason": moneyline.exclusion_reason,
            "core_timing_eligible": timing_ok,
            "timing_exclusion_reason": None if timing_ok else _none(serialized_match.get("timing_exclusion_reason")),
            "eligible_for_core_analysis": eligible,
            "season": game.season if game else None, "season_type": game.season_type if game else None,
            "week": game.week if game else None, "official_date": game.official_date if game else None,
            "neutral_site": game.neutral_site if game else None,
            "went_to_overtime": _none(timing.get("went_to_overtime")) if timing else None,
            "phase_contract_sha256": (
                _none(timing.get("phase_contract_sha256")) if timing else NFL_PHASE_CONTRACT_SHA256
            ),
        }
        audit_rows.append(tuple(audit_values[name] for name, _ in AUDIT_SCHEMA))
        if eligible:
            dimension = eligible_dims[match.market_id]
            values = {
                **audit_values,
                "game_id": game.game_id,
                "away_team_id": game.away_team_id, "away_team_name": game.away_team_name,
                "home_team_id": game.home_team_id, "home_team_name": game.home_team_name,
                "away_token_id": dimension.away_token_id, "home_token_id": dimension.home_token_id,
                "winning_team_id": dimension.winning_team_id,
                "winning_token_id": dimension.winning_token_id,
                "winning_outcome": dimension.winning_outcome,
                "away_final_score": game.away_final_score, "home_final_score": game.home_final_score,
                "home_won": game.home_is_winner,
                "scheduled_start_utc": game.scheduled_start_utc,
                **{name: _none(timing[name]) for name in (
                    "actual_start_utc", "period_2_start_utc", "period_3_start_utc",
                    "period_4_start_utc", "actual_end_utc", "competitive_play_count",
                    "final_period", "went_to_overtime")},
            }
            eligible_rows.append(tuple(values[name] for name, _ in ELIGIBLE_SCHEMA))

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        _write_parquet(staging / AUDIT_OUTPUT, AUDIT_SCHEMA, audit_rows)
        _write_parquet(staging / ELIGIBLE_OUTPUT, ELIGIBLE_SCHEMA, eligible_rows)
        summary = {
            "schema_version": 1,
            "definition": "validated_nfl_moneyline_quarter_timing_v1",
            "candidate_markets": len(candidates),
            "moneyline_valid": sum(row.is_valid for row in validation.audits),
            "core_timing_eligible": sum(bool(row[10]) for row in audit_rows),
            "eligible_moneylines": len(eligible_rows),
            "moneyline_exclusions": dict(Counter(row.exclusion_reason for row in validation.audits if row.exclusion_reason)),
            "timing_source": "ESPN site API (third-party undocumented)",
            "phase_contract_sha256": NFL_PHASE_CONTRACT_SHA256,
            "analysis_phases": list(NFL_ANALYSIS_PHASES),
            "outputs": {
                "candidate_validation_audit": AUDIT_OUTPUT,
                "eligible_moneylines": ELIGIBLE_OUTPUT,
            },
        }
        (staging / SUMMARY_OUTPUT).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        schemas = {AUDIT_OUTPUT: AUDIT_SCHEMA, ELIGIBLE_OUTPUT: ELIGIBLE_SCHEMA}
        manifest = {
            "schema_version": 1,
            "stage": "nfl_validated_universe",
            "inputs": {
                "candidates": file_fingerprint(candidates_path),
                "timing_manifest": file_fingerprint(timing_run / "timing_manifest.json"),
                "universe_tokens": file_fingerprint(universe_tokens_path),
                "token_map": file_fingerprint(token_map_path),
            },
            "schemas": {
                name: [list(row) for row in schema] for name, schema in schemas.items()
            },
            "outputs": {
                name: file_fingerprint(staging / name, relative_to=staging)
                for name in (*schemas, SUMMARY_OUTPUT)
            },
            "counts": {
                "candidate_markets": len(candidates),
                "moneyline_valid": sum(row.is_valid for row in validation.audits),
                "core_timing_eligible": sum(bool(row[10]) for row in audit_rows),
                "eligible_moneylines": len(eligible_rows),
            },
            "uniqueness": {
                "audit_market_id_unique": True,
                "eligible_market_id_unique": True,
                "eligible_game_id_unique": True,
                "eligible_token_id_unique": True,
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        verify_validated_run(staging)
        if target.exists():
            raise FileExistsError(f"Immutable NFL validated run appeared during build: {target}")
        staging.rename(target)
        verify_validated_run(target)
        return summary
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--timing-run-dir", required=True)
    parser.add_argument("--universe-tokens", required=True)
    parser.add_argument("--token-map", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(build_validated_universe(args.candidates, args.timing_run_dir,
                                   args.universe_tokens, args.token_map, args.run_dir))


if __name__ == "__main__":
    main()
