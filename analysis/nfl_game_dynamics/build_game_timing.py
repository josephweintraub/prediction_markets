"""Build immutable NFL schedule-match and quarter-boundary audit artifacts."""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections import Counter
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from .artifact_manifest import (
    ArtifactManifestError,
    file_fingerprint,
    require_parquet_schema,
    verify_fingerprint,
)
from .build_market_universe import CANDIDATE_SCHEMA
from .match_games import GameMatchAudit, assert_one_to_one_matches, match_market_candidates
from .nfl_api import (
    ADMINISTRATIVE_PLAY_TYPES,
    AUDITED_ESPN_GAME_IDS,
    COMPETITIVE_PLAY_TYPES,
    NFL_PHASE_CONTRACT_PATH,
    NFL_PHASE_CONTRACT_SHA256,
    NFL_TAXONOMY_AUDIT_PATH,
    NFL_TAXONOMY_AUDIT_SHA256,
    POINT_AFTER_TYPES,
    EspnNflClient,
    GameTiming,
    NFL_ANALYSIS_PHASES,
    ScheduleGame,
    parse_game_timing,
    parse_scoreboard,
    validate_schedule_timing,
)


SCHEDULE_SCHEMA = (
    ("game_id", "VARCHAR"), ("official_date", "DATE"),
    ("scheduled_start_utc", "TIMESTAMPTZ"), ("season", "INTEGER"),
    ("season_type", "INTEGER"), ("week", "INTEGER"),
    ("away_team_id", "INTEGER"), ("away_abbreviation", "VARCHAR"),
    ("away_team_name", "VARCHAR"), ("home_team_id", "INTEGER"),
    ("home_abbreviation", "VARCHAR"), ("home_team_name", "VARCHAR"),
    ("away_final_score", "INTEGER"), ("home_final_score", "INTEGER"),
    ("away_is_winner", "BOOLEAN"), ("home_is_winner", "BOOLEAN"),
    ("status_state", "VARCHAR"), ("status_detail", "VARCHAR"),
    ("is_completed", "BOOLEAN"), ("expected_final_period", "INTEGER"),
    ("neutral_site", "BOOLEAN"),
)
MATCH_SCHEMA = (
    ("market_id", "VARCHAR"), ("market_date", "DATE"),
    ("team_1_slug", "VARCHAR"), ("team_2_slug", "VARCHAR"),
    ("slug_orientation", "VARCHAR"), ("matched_game_id", "VARCHAR"),
    ("match_exclusion_reason", "VARCHAR"), ("schedule_match_count", "INTEGER"),
    ("schedule_match_game_ids_json", "VARCHAR"),
    ("away_team_id", "INTEGER"), ("away_team_name", "VARCHAR"),
    ("home_team_id", "INTEGER"), ("home_team_name", "VARCHAR"),
    ("timing_status", "VARCHAR"), ("timing_exclusion_reason", "VARCHAR"),
    ("timing_error_type", "VARCHAR"), ("timing_error_message", "VARCHAR"),
)
TIMING_SCHEMA = (
    ("game_id", "VARCHAR"), ("away_team_id", "INTEGER"),
    ("home_team_id", "INTEGER"), ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"), ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"), ("status_detail", "VARCHAR"),
    ("actual_start_utc", "TIMESTAMPTZ"),
    ("period_2_start_utc", "TIMESTAMPTZ"),
    ("period_3_start_utc", "TIMESTAMPTZ"),
    ("period_4_start_utc", "TIMESTAMPTZ"), ("actual_end_utc", "TIMESTAMPTZ"),
    ("competitive_play_count", "INTEGER"), ("final_period", "INTEGER"),
    ("went_to_overtime", "BOOLEAN"),
    ("source_provider", "VARCHAR"), ("timestamp_semantics", "VARCHAR"),
    ("phase_contract_sha256", "VARCHAR"),
)
MANIFEST_OUTPUT = "timing_manifest.json"


class TimingBuildError(ValueError):
    pass


def _write_parquet(path: Path, schema: tuple[tuple[str, str], ...], rows: list[tuple[Any, ...]]) -> None:
    con = duckdb.connect()
    try:
        definition = ",".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE artifact({definition})")
        if rows:
            con.executemany(f"INSERT INTO artifact VALUES ({','.join('?' for _ in schema)})", rows)
        quoted = str(path).replace("'", "''")
        order = '"market_id"' if any(name == "market_id" for name, _ in schema) else '"game_id"'
        con.execute(f"COPY (SELECT * FROM artifact ORDER BY {order}) TO '{quoted}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()


def _schedule_row(game: ScheduleGame) -> tuple[Any, ...]:
    values = asdict(game)
    return tuple(values[name] for name, _ in SCHEDULE_SCHEMA)


def _timing_row(timing: GameTiming) -> tuple[Any, ...]:
    values = asdict(timing)
    values["source_provider"] = "ESPN site API (third-party undocumented endpoint)"
    values["timestamp_semantics"] = (
        "competitive-play wallclock; final boundary is the final competitive-play "
        "timestamp with subsequent terminal End of Game evidence"
    )
    values["phase_contract_sha256"] = NFL_PHASE_CONTRACT_SHA256
    return tuple(values[name] for name, _ in TIMING_SCHEMA)


def _support_fingerprints(
    phase_contract_source: str | Path,
    taxonomy_audit_source: str | Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    phase = file_fingerprint(phase_contract_source)
    if phase["sha256"] != NFL_PHASE_CONTRACT_SHA256:
        raise TimingBuildError("NFL phase contract does not match its frozen SHA-256")
    taxonomy = file_fingerprint(taxonomy_audit_source)
    if taxonomy["sha256"] != NFL_TAXONOMY_AUDIT_SHA256:
        raise TimingBuildError("NFL taxonomy audit does not match its frozen SHA-256")
    value = json.loads(Path(taxonomy_audit_source).read_text(encoding="utf-8"))
    expected = {
        "schema_version": 1,
        "source": "ESPN site API (third-party undocumented endpoint)",
        "audited_game_ids": list(AUDITED_ESPN_GAME_IDS),
        "administrative_play_types": ADMINISTRATIVE_PLAY_TYPES,
        "competitive_play_types": COMPETITIVE_PLAY_TYPES,
        "point_after_types": POINT_AFTER_TYPES,
    }
    if value != expected:
        raise TimingBuildError("NFL taxonomy audit content does not match the parser union")
    return phase, taxonomy


def _match_row(row: GameMatchAudit, error: Exception | None, has_timing: bool) -> tuple[Any, ...]:
    if row.is_matched and has_timing:
        timing_status, timing_reason = "parsed", None
    elif row.is_matched:
        timing_status, timing_reason = "excluded", "timing_fetch_or_parse_failure"
    else:
        timing_status, timing_reason = "not_attempted", "not_exact_final_match"
    values = {
        "market_id": row.market_id, "market_date": row.market_date,
        "team_1_slug": row.team_1_slug, "team_2_slug": row.team_2_slug,
        "slug_orientation": row.slug_orientation,
        "matched_game_id": row.matched_game_id,
        "match_exclusion_reason": row.exclusion_reason,
        "schedule_match_count": len(row.schedule_matches),
        "schedule_match_game_ids_json": json.dumps([game.game_id for game in row.schedule_matches]),
        "away_team_id": row.away_team.team_id if row.away_team else None,
        "away_team_name": row.away_team.name if row.away_team else None,
        "home_team_id": row.home_team.team_id if row.home_team else None,
        "home_team_name": row.home_team.name if row.home_team else None,
        "timing_status": timing_status, "timing_exclusion_reason": timing_reason,
        "timing_error_type": type(error).__name__ if error else None,
        "timing_error_message": str(error) if error else None,
    }
    return tuple(values[name] for name, _ in MATCH_SCHEMA)


def build_from_payloads(
    candidates: Iterable[Mapping[str, Any]],
    scoreboard_payloads: Iterable[Mapping[str, Any]],
    summary_payloads: Mapping[str, Mapping[str, Any]],
    run_dir: str | Path,
    *,
    candidate_source: str | Path,
    scoreboard_sources: Iterable[str | Path],
    summary_sources: Mapping[str, str | Path],
    phase_contract_source: str | Path = NFL_PHASE_CONTRACT_PATH,
    taxonomy_audit_source: str | Path = NFL_TAXONOMY_AUDIT_PATH,
) -> dict[str, Any]:
    """Pure-input artifact build used by the network CLI and offline tests."""

    candidates = tuple(candidates)
    scoreboard_payloads = tuple(scoreboard_payloads)
    scoreboard_sources = tuple(Path(path).expanduser().resolve() for path in scoreboard_sources)
    summary_sources = {str(key): Path(path).expanduser().resolve() for key, path in summary_sources.items()}
    if not candidates:
        raise TimingBuildError("NFL candidate input must be nonempty")
    candidate_source = Path(candidate_source).expanduser().resolve()
    phase_contract_source = Path(phase_contract_source).expanduser().resolve()
    taxonomy_audit_source = Path(taxonomy_audit_source).expanduser().resolve()
    target = Path(run_dir).expanduser().resolve()
    if target.exists():
        raise FileExistsError(f"Immutable NFL timing run exists: {target}")
    inputs = (
        candidate_source, phase_contract_source, taxonomy_audit_source,
        *scoreboard_sources, *summary_sources.values(),
    )
    if any(target == source or target in source.parents or source in target.parents for source in inputs):
        raise TimingBuildError("NFL timing run must not overlap inputs or raw cache files")
    phase_fingerprint, taxonomy_fingerprint = _support_fingerprints(
        phase_contract_source, taxonomy_audit_source
    )
    require_parquet_schema(candidate_source, CANDIDATE_SCHEMA, "candidate input")
    serialized_candidates = tuple(_load_candidates(candidate_source))
    def candidate_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
        raw_date = row.get("date")
        if not isinstance(raw_date, str) and hasattr(raw_date, "date"):
            raw_date = raw_date.date()
        normalized_date = raw_date.isoformat() if isinstance(raw_date, date) else str(raw_date)
        return (
            str(row.get("market_id")), normalized_date,
            str(row.get("team_1_slug")), str(row.get("team_2_slug")),
        )
    if sorted(map(candidate_key, candidates)) != sorted(map(candidate_key, serialized_candidates)):
        raise TimingBuildError("Candidate rows do not match the fingerprinted candidate source")
    candidates = serialized_candidates
    if len(scoreboard_payloads) != len(scoreboard_sources):
        raise TimingBuildError("Scoreboard payload/source counts do not match")
    if set(summary_payloads) != set(summary_sources):
        raise TimingBuildError("Summary payload/source game IDs do not match")
    for payload, path in zip(scoreboard_payloads, scoreboard_sources, strict=True):
        if json.loads(path.read_text(encoding="utf-8")) != payload:
            raise TimingBuildError(f"Scoreboard cache content does not match payload: {path}")
    for game_id, payload in summary_payloads.items():
        path = summary_sources[game_id]
        if json.loads(path.read_text(encoding="utf-8")) != payload:
            raise TimingBuildError(f"Summary cache content does not match payload: {path}")
    schedules_by_id: dict[str, ScheduleGame] = {}
    for payload in scoreboard_payloads:
        for game in parse_scoreboard(payload):
            prior = schedules_by_id.get(game.game_id)
            if prior is not None and prior != game:
                raise TimingBuildError(f"Conflicting scoreboard records for game {game.game_id}")
            schedules_by_id[game.game_id] = game
    schedules = tuple(schedules_by_id.values())
    matches = match_market_candidates(candidates, schedules)
    assert_one_to_one_matches(matches)
    expected_summary_ids = {
        row.matched_game_id for row in matches if row.is_matched
    }
    extra_summary_ids = sorted(set(summary_payloads) - expected_summary_ids)
    if extra_summary_ids:
        raise TimingBuildError(
            f"Summary inputs contain games outside the exact match spine: {extra_summary_ids}"
        )
    timing_by_game: dict[str, GameTiming] = {}
    errors: dict[str, Exception] = {}
    for row in matches:
        if not row.is_matched:
            continue
        try:
            if row.matched_game_id not in summary_payloads:
                raise KeyError(f"No summary payload for matched game {row.matched_game_id}")
            timing = parse_game_timing(
                summary_payloads[row.matched_game_id], row.matched_game_id
            )
            validate_schedule_timing(row.schedule_matches[0], timing)
            timing_by_game[row.matched_game_id] = timing
        except Exception as exc:  # exclusions are serialized, not silently dropped
            errors[row.matched_game_id] = exc

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        schedule_path = staging / "schedule_audit.parquet"
        timing_path = staging / "game_timing.parquet"
        match_path = staging / "match_audit.parquet"
        summary_path = staging / "summary.json"
        _write_parquet(schedule_path, SCHEDULE_SCHEMA,
                       [_schedule_row(game) for game in schedules])
        _write_parquet(timing_path, TIMING_SCHEMA,
                       [_timing_row(timing) for timing in timing_by_game.values()])
        _write_parquet(match_path, MATCH_SCHEMA, [
            _match_row(row, errors.get(row.matched_game_id), row.matched_game_id in timing_by_game)
            for row in matches
        ])
        summary = {
            "schema_version": 1,
            "source": "ESPN site API",
            "source_status": "third_party_undocumented",
            "provider_limitations": [
                "Administrative timeout/end markers can carry stale placeholder wallclocks and are excluded by audited type IDs.",
                "actual_end_utc is the timestamp of the final competitive play, not a separately reported game-over timestamp.",
                "Completed reschedule history is not exposed reliably by this endpoint.",
            ],
            "analysis_phases": list(NFL_ANALYSIS_PHASES),
            "boundary_sensitivity": "exclude abs(t-boundary) <= 30 seconds; inclusive; no reassignment",
            "phase_contract_sha256": NFL_PHASE_CONTRACT_SHA256,
            "candidate_markets": len(candidates),
            "schedule_games": len(schedules),
            "exact_final_matches": sum(row.is_matched for row in matches),
            "timing_games": len(timing_by_game),
            "match_exclusions": dict(Counter(row.exclusion_reason for row in matches if row.exclusion_reason)),
            "timing_exclusions": len(errors),
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        schemas = {
            "schedule_audit.parquet": SCHEDULE_SCHEMA,
            "match_audit.parquet": MATCH_SCHEMA,
            "game_timing.parquet": TIMING_SCHEMA,
        }
        for name, schema in schemas.items():
            require_parquet_schema(staging / name, schema, name)
        output_names = (*schemas, "summary.json")
        manifest = {
            "schema_version": 1,
            "stage": "nfl_game_timing",
            "inputs": {
                "candidates": file_fingerprint(candidate_source),
                "phase_contract": phase_fingerprint,
                "taxonomy_audit": taxonomy_fingerprint,
            },
            "cache_inputs": {
                "scoreboards": [file_fingerprint(path) for path in scoreboard_sources],
                "summaries": {
                    game_id: file_fingerprint(path)
                    for game_id, path in sorted(summary_sources.items())
                },
            },
            "schemas": {name: [list(row) for row in schema] for name, schema in schemas.items()},
            "outputs": {
                name: file_fingerprint(staging / name, relative_to=staging)
                for name in output_names
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        verify_timing_run(staging)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError(f"Immutable NFL timing run appeared during build: {target}")
        staging.rename(target)
        verify_timing_run(target)
        return summary
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_timing_run(run_dir: str | Path) -> dict[str, Any]:
    run = Path(run_dir).expanduser().resolve()
    output_schemas = {
        "schedule_audit.parquet": SCHEDULE_SCHEMA,
        "match_audit.parquet": MATCH_SCHEMA,
        "game_timing.parquet": TIMING_SCHEMA,
    }
    expected_files = {*output_schemas, "summary.json", MANIFEST_OUTPUT}
    if not run.is_dir() or {path.name for path in run.iterdir()} != expected_files:
        raise ArtifactManifestError("NFL timing run file set is incomplete or unexpected")
    manifest = json.loads((run / MANIFEST_OUTPUT).read_text(encoding="utf-8"))
    if set(manifest) != {"schema_version", "stage", "inputs", "cache_inputs", "schemas", "outputs"}:
        raise ArtifactManifestError("NFL timing manifest keys mismatch")
    if manifest["schema_version"] != 1 or manifest["stage"] != "nfl_game_timing":
        raise ArtifactManifestError("NFL timing manifest identity mismatch")
    if set(manifest["inputs"]) != {"candidates", "phase_contract", "taxonomy_audit"}:
        raise ArtifactManifestError("NFL timing input manifest mismatch")
    verify_fingerprint(manifest["inputs"]["candidates"])
    phase_path = verify_fingerprint(manifest["inputs"]["phase_contract"])
    taxonomy_path = verify_fingerprint(manifest["inputs"]["taxonomy_audit"])
    _support_fingerprints(phase_path, taxonomy_path)
    cache = manifest["cache_inputs"]
    if not isinstance(cache, dict) or set(cache) != {"scoreboards", "summaries"}:
        raise ArtifactManifestError("NFL timing cache-input manifest mismatch")
    if not isinstance(cache["scoreboards"], list) or not cache["scoreboards"]:
        raise ArtifactManifestError("NFL timing scoreboard fingerprints must be nonempty")
    if not isinstance(cache["summaries"], dict):
        raise ArtifactManifestError("NFL timing summary fingerprints must be an object")
    for record in cache["scoreboards"]:
        verify_fingerprint(record)
    for game_id, record in cache["summaries"].items():
        if not isinstance(game_id, str) or not game_id.isdigit():
            raise ArtifactManifestError("NFL timing summary fingerprint game ID is invalid")
        verify_fingerprint(record)
    expected_declared = {name: [list(row) for row in schema] for name, schema in output_schemas.items()}
    if manifest["schemas"] != expected_declared:
        raise ArtifactManifestError("NFL timing declared schemas mismatch")
    expected_outputs = {*output_schemas, "summary.json"}
    if set(manifest["outputs"]) != expected_outputs:
        raise ArtifactManifestError("NFL timing outputs manifest mismatch")
    for name in expected_outputs:
        if verify_fingerprint(manifest["outputs"][name], base_dir=run) != run / name:
            raise ArtifactManifestError("NFL timing output path mismatch")
    for name, schema in output_schemas.items():
        require_parquet_schema(run / name, schema, name)
    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    con = duckdb.connect()
    try:
        timing_path = str(run / "game_timing.parquet").replace("'", "''")
        counts = {
            "schedule_games": int(con.execute(
                f"SELECT count(*) FROM read_parquet('{str(run / 'schedule_audit.parquet').replace("'", "''")}')"
            ).fetchone()[0]),
            "candidate_markets": int(con.execute(
                f"SELECT count(*) FROM read_parquet('{str(run / 'match_audit.parquet').replace("'", "''")}')"
            ).fetchone()[0]),
            "timing_games": int(con.execute(
                f"SELECT count(*) FROM read_parquet('{str(run / 'game_timing.parquet').replace("'", "''")}')"
            ).fetchone()[0]),
        }
        invalid_contract_rows = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{timing_path}') "
            f"WHERE phase_contract_sha256 IS DISTINCT FROM ?",
            [NFL_PHASE_CONTRACT_SHA256],
        ).fetchone()[0])
    finally:
        con.close()
    if invalid_contract_rows:
        raise ArtifactManifestError(
            "Every NFL timing row must carry the exact frozen phase contract SHA"
        )
    if any(summary.get(key) != value for key, value in counts.items()):
        raise ArtifactManifestError("NFL timing summary counts do not reconcile")
    if summary.get("analysis_phases") != list(NFL_ANALYSIS_PHASES):
        raise ArtifactManifestError("NFL timing analysis phases do not match the frozen contract")
    if summary.get("phase_contract_sha256") != NFL_PHASE_CONTRACT_SHA256:
        raise ArtifactManifestError("NFL timing summary phase contract mismatch")
    return summary


def _load_candidates(path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        quoted = str(path).replace("'", "''")
        relation = f"read_parquet('{quoted}')"
        required = {"market_id", "date", "team_1_slug", "team_2_slug"}
        columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
        if not required.issubset(columns):
            raise TimingBuildError(f"Candidate columns missing: {sorted(required-columns)}")
        frame = con.execute(f"SELECT * FROM {relation} ORDER BY market_id").fetchdf()
        return frame.to_dict("records")
    finally:
        con.close()


def matched_summary_game_ids(
    candidates: Iterable[Mapping[str, Any]],
    scoreboard_payloads: Iterable[Mapping[str, Any]],
) -> tuple[str, ...]:
    """Return only exact, unique, completed candidate matches for summary fetches."""

    schedules = tuple(
        game for payload in scoreboard_payloads for game in parse_scoreboard(payload)
    )
    return tuple(sorted({
        row.matched_game_id
        for row in match_market_candidates(candidates, schedules)
        if row.is_matched
    }))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    candidate_path = Path(args.candidates).expanduser().resolve()
    cache_dir = Path(args.cache_dir).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    if run_dir == cache_dir or run_dir in cache_dir.parents or cache_dir in run_dir.parents:
        raise TimingBuildError("NFL raw API cache and immutable timing run must be separate")
    if run_dir == candidate_path or run_dir in candidate_path.parents or candidate_path in run_dir.parents:
        raise TimingBuildError("NFL timing run must not overlap its candidate input")
    candidates = _load_candidates(candidate_path)
    client = EspnNflClient(cache_dir)
    dates = sorted({row["date"] if isinstance(row["date"], date) else date.fromisoformat(str(row["date"])) for row in candidates})
    scoreboards = [client.scoreboard(value, refresh=args.refresh) for value in dates]
    summaries = {
        game_id: client.summary(game_id, refresh=args.refresh)
        for game_id in matched_summary_game_ids(candidates, scoreboards)
    }
    print(build_from_payloads(
        candidates,
        scoreboards,
        summaries,
        run_dir,
        candidate_source=candidate_path,
        scoreboard_sources=[client.scoreboard_cache_path(value) for value in dates],
        summary_sources={
            game_id: client.summary_cache_path(game_id) for game_id in summaries
        },
    ))


if __name__ == "__main__":
    main()
