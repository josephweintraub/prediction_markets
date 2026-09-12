"""Build official NBA schedule, match, and exact wall-clock timing audits."""
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
from typing import Any, Iterable

import duckdb

try:  # Package import for tests; direct import for CLI execution.
    from .artifact_manifest import (
        ArtifactManifestError,
        file_fingerprint,
        require_parquet_schema,
        verify_fingerprint,
    )
    from .build_market_universe import CANDIDATE_SCHEMA
    from .match_games import GameMatchAudit, assert_one_to_one_matches, match_market_candidates
    from .nba_api import (
        ACTUAL_END_EVENT,
        ACTUAL_START_EVENT,
        PERIOD_BOUNDARY_EVENT,
        GameTiming,
        NbaApiClient,
        ScheduleGame,
        validate_schedule_timing,
    )
    from analysis.sports_game_dynamics.phase_contract import (
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
    from build_market_universe import CANDIDATE_SCHEMA
    from match_games import GameMatchAudit, assert_one_to_one_matches, match_market_candidates
    from nba_api import (
        ACTUAL_END_EVENT,
        ACTUAL_START_EVENT,
        PERIOD_BOUNDARY_EVENT,
        GameTiming,
        NbaApiClient,
        ScheduleGame,
        validate_schedule_timing,
    )
    from analysis.sports_game_dynamics.phase_contract import (
        load_phase_contract,
        phase_contract_fingerprint,
    )


SCHEDULE_OUTPUT = "schedule_audit.parquet"
MATCH_OUTPUT = "match_audit.parquet"
TIMING_OUTPUT = "game_timing.parquet"
SUMMARY_OUTPUT = "summary.json"
PROVENANCE_OUTPUT = "provider_provenance.json"
MANIFEST_OUTPUT = "timing_manifest.json"
DEFAULT_PHASE_CONTRACT = (
    Path(__file__).resolve().parents[2]
    / "configs/game_dynamics/nba_phase_contract_v1.json"
)
NBA_ANALYSIS_PHASES = (
    "pregame",
    "quarter_1",
    "quarter_2",
    "quarter_3",
    "quarter_4_plus",
)

SCHEDULE_SCHEMA = (
    ("game_id", "VARCHAR"), ("official_date", "DATE"),
    ("scheduled_start_utc", "TIMESTAMPTZ"), ("season_start", "INTEGER"),
    ("game_type_code", "VARCHAR"), ("away_team_id", "BIGINT"),
    ("away_team_name", "VARCHAR"), ("away_team_tricode", "VARCHAR"),
    ("home_team_id", "BIGINT"), ("home_team_name", "VARCHAR"),
    ("home_team_tricode", "VARCHAR"), ("status_text", "VARCHAR"),
    ("is_completed", "BOOLEAN"), ("expected_final_period", "INTEGER"),
    ("postponement_status", "VARCHAR"),
    ("postponement_reason", "VARCHAR"), ("away_final_score", "INTEGER"),
    ("home_final_score", "INTEGER"), ("winner_team_id", "BIGINT"),
    ("away_is_winner", "BOOLEAN"), ("home_is_winner", "BOOLEAN"),
)
MATCH_SCHEMA = (
    ("market_id", "VARCHAR"), ("market_date", "DATE"),
    ("observed_first_slug", "VARCHAR"), ("observed_second_slug", "VARCHAR"),
    ("slug_orientation", "VARCHAR"), ("away_team_id", "BIGINT"),
    ("away_team_name", "VARCHAR"), ("away_team_tricode", "VARCHAR"),
    ("home_team_id", "BIGINT"), ("home_team_name", "VARCHAR"),
    ("home_team_tricode", "VARCHAR"), ("matched_game_id", "VARCHAR"),
    ("match_exclusion_reason", "VARCHAR"), ("schedule_match_count", "INTEGER"),
    ("schedule_match_game_ids_json", "VARCHAR"),
    ("schedule_official_date", "DATE"),
    ("schedule_scheduled_start_utc", "TIMESTAMPTZ"),
    ("schedule_season_start", "INTEGER"), ("schedule_game_type_code", "VARCHAR"),
    ("schedule_status_text", "VARCHAR"), ("schedule_is_completed", "BOOLEAN"),
    ("schedule_expected_final_period", "INTEGER"),
    ("schedule_postponement_status", "VARCHAR"),
    ("schedule_postponement_reason", "VARCHAR"),
    ("schedule_away_final_score", "INTEGER"),
    ("schedule_home_final_score", "INTEGER"),
    ("schedule_winner_team_id", "BIGINT"),
    ("schedule_away_is_winner", "BOOLEAN"),
    ("schedule_home_is_winner", "BOOLEAN"),
    ("timing_status", "VARCHAR"), ("timing_exclusion_reason", "VARCHAR"),
    ("timing_error_type", "VARCHAR"), ("timing_error_message", "VARCHAR"),
)
TIMING_SCHEMA = (
    ("market_id", "VARCHAR"), ("game_id", "VARCHAR"),
    ("official_date", "DATE"), ("season_start", "INTEGER"),
    ("game_type_code", "VARCHAR"), ("away_team_id", "BIGINT"),
    ("away_team_name", "VARCHAR"), ("away_team_tricode", "VARCHAR"),
    ("home_team_id", "BIGINT"), ("home_team_name", "VARCHAR"),
    ("home_team_tricode", "VARCHAR"), ("scheduled_start_utc", "TIMESTAMPTZ"),
    ("actual_start_utc", "TIMESTAMPTZ"), ("period_2_start_utc", "TIMESTAMPTZ"),
    ("period_3_start_utc", "TIMESTAMPTZ"), ("period_4_start_utc", "TIMESTAMPTZ"),
    ("actual_end_utc", "TIMESTAMPTZ"), ("final_period", "INTEGER"),
    ("expected_final_period", "INTEGER"),
    ("actual_start_action_number", "INTEGER"),
    ("actual_start_order_number", "BIGINT"),
    ("actual_start_event", "VARCHAR"),
    ("period_2_start_action_number", "INTEGER"),
    ("period_2_start_order_number", "BIGINT"),
    ("period_3_start_action_number", "INTEGER"),
    ("period_3_start_order_number", "BIGINT"),
    ("period_4_start_action_number", "INTEGER"),
    ("period_4_start_order_number", "BIGINT"),
    ("actual_end_action_number", "INTEGER"),
    ("actual_end_order_number", "BIGINT"),
    ("actual_end_event", "VARCHAR"),
    ("period_boundary_event", "VARCHAR"),
    ("action_count", "INTEGER"), ("status_text", "VARCHAR"),
    ("postponement_status", "VARCHAR"), ("postponement_reason", "VARCHAR"),
    ("away_final_score", "INTEGER"), ("home_final_score", "INTEGER"),
    ("winner_team_id", "BIGINT"), ("away_is_winner", "BOOLEAN"),
    ("home_is_winner", "BOOLEAN"), ("irregular_exclusion_reason", "VARCHAR"),
    ("timestamp_source", "VARCHAR"), ("provider_provenance_sha256", "VARCHAR"),
    ("phase_contract_sha256", "VARCHAR"),
)


def _quote(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve()).replace("'", "''")


def _read_candidates(path: str | Path) -> tuple[list[dict[str, Any]], date, date]:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"NBA candidate Parquet does not exist: {source}")
    con = duckdb.connect()
    try:
        require_parquet_schema(source, CANDIDATE_SCHEMA, "NBA candidate input")
        relation = f"read_parquet('{_quote(source)}')"
        frame = con.execute(
            f"SELECT * FROM {relation} ORDER BY TRY_CAST(date AS DATE), market_id"
        ).fetchdf()
        if frame.empty:
            raise ValueError("NBA candidate Parquet contains no rows")
        start, end = con.execute(
            f"SELECT min(TRY_CAST(date AS DATE)), max(TRY_CAST(date AS DATE)) FROM {relation}"
        ).fetchone()
    finally:
        con.close()
    if start is None or end is None:
        raise ValueError("NBA candidate Parquet contains invalid dates")
    return frame.to_dict(orient="records"), start, end


def _write_parquet(
    rows: Iterable[dict[str, Any]], schema: tuple[tuple[str, str], ...],
    path: Path, order_by: str
) -> None:
    materialized = list(rows)
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE output (" + ",".join(f'\"{n}\" {t}' for n, t in schema) + ")")
        if materialized:
            names = [name for name, _ in schema]
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


def _schedule_row(row: ScheduleGame) -> dict[str, Any]:
    return {name: getattr(row, name) for name, _ in SCHEDULE_SCHEMA}


def _match_row(row: GameMatchAudit) -> dict[str, Any]:
    schedule = row.schedule_matches[0] if len(row.schedule_matches) == 1 else None
    return {
        "market_id": row.market_id,
        "market_date": row.market_date,
        "observed_first_slug": row.observed_first_slug,
        "observed_second_slug": row.observed_second_slug,
        "slug_orientation": row.slug_orientation,
        "away_team_id": row.away_team.team_id if row.away_team else None,
        "away_team_name": row.away_team.name if row.away_team else None,
        "away_team_tricode": row.away_team.tricode if row.away_team else None,
        "home_team_id": row.home_team.team_id if row.home_team else None,
        "home_team_name": row.home_team.name if row.home_team else None,
        "home_team_tricode": row.home_team.tricode if row.home_team else None,
        "matched_game_id": row.matched_game_id,
        "match_exclusion_reason": row.exclusion_reason,
        "schedule_match_count": len(row.schedule_matches),
        "schedule_match_game_ids_json": json.dumps(
            [game.game_id for game in row.schedule_matches], separators=(",", ":")
        ),
        "schedule_official_date": schedule.official_date if schedule else None,
        "schedule_scheduled_start_utc": schedule.scheduled_start_utc if schedule else None,
        "schedule_season_start": schedule.season_start if schedule else None,
        "schedule_game_type_code": schedule.game_type_code if schedule else None,
        "schedule_status_text": schedule.status_text if schedule else None,
        "schedule_is_completed": schedule.is_completed if schedule else None,
        "schedule_expected_final_period": schedule.expected_final_period if schedule else None,
        "schedule_postponement_status": schedule.postponement_status if schedule else None,
        "schedule_postponement_reason": schedule.postponement_reason if schedule else None,
        "schedule_away_final_score": schedule.away_final_score if schedule else None,
        "schedule_home_final_score": schedule.home_final_score if schedule else None,
        "schedule_winner_team_id": schedule.winner_team_id if schedule else None,
        "schedule_away_is_winner": schedule.away_is_winner if schedule else None,
        "schedule_home_is_winner": schedule.home_is_winner if schedule else None,
        "timing_status": "not_eligible" if row.exclusion_reason else "pending",
        "timing_exclusion_reason": "not_exact_final_match" if row.exclusion_reason else None,
        "timing_error_type": None,
        "timing_error_message": None,
    }


def _timing_row(
    match: GameMatchAudit, timing: GameTiming, phase_contract_sha256: str
) -> dict[str, Any]:
    schedule = match.schedule_matches[0]
    starts = {period.period: period.start_utc for period in timing.periods}
    return {
        "market_id": match.market_id,
        "game_id": timing.game_id,
        "official_date": schedule.official_date,
        "season_start": schedule.season_start,
        "game_type_code": schedule.game_type_code,
        "away_team_id": schedule.away_team_id,
        "away_team_name": schedule.away_team_name,
        "away_team_tricode": schedule.away_team_tricode,
        "home_team_id": schedule.home_team_id,
        "home_team_name": schedule.home_team_name,
        "home_team_tricode": schedule.home_team_tricode,
        "scheduled_start_utc": schedule.scheduled_start_utc,
        "actual_start_utc": timing.actual_start_utc,
        "period_2_start_utc": starts[2],
        "period_3_start_utc": starts[3],
        "period_4_start_utc": starts[4],
        "actual_end_utc": timing.actual_end_utc,
        "final_period": timing.final_period,
        "expected_final_period": schedule.expected_final_period,
        "actual_start_action_number": timing.actual_start_action_number,
        "actual_start_order_number": timing.actual_start_order_number,
        "actual_start_event": timing.actual_start_event,
        "period_2_start_action_number": timing.periods[1].start_action_number,
        "period_2_start_order_number": timing.periods[1].start_order_number,
        "period_3_start_action_number": timing.periods[2].start_action_number,
        "period_3_start_order_number": timing.periods[2].start_order_number,
        "period_4_start_action_number": timing.periods[3].start_action_number,
        "period_4_start_order_number": timing.periods[3].start_order_number,
        "actual_end_action_number": timing.actual_end_action_number,
        "actual_end_order_number": timing.actual_end_order_number,
        "actual_end_event": timing.actual_end_event,
        "period_boundary_event": timing.period_boundary_event,
        "action_count": timing.action_count,
        "status_text": schedule.status_text,
        "postponement_status": schedule.postponement_status,
        "postponement_reason": schedule.postponement_reason,
        "away_final_score": schedule.away_final_score,
        "home_final_score": schedule.home_final_score,
        "winner_team_id": schedule.winner_team_id,
        "away_is_winner": schedule.away_is_winner,
        "home_is_winner": schedule.home_is_winner,
        "irregular_exclusion_reason": (
            "postponed_or_rescheduled"
            if schedule.postponement_reason not in (None, "")
            else None
        ),
        "timestamp_source": "official_nba_livedata_timeActual",
        "provider_provenance_sha256": None,
        "phase_contract_sha256": phase_contract_sha256,
    }


def _validated_provenance(api: Any) -> tuple[dict[str, Any], bytes, str]:
    method = getattr(api, "provenance_manifest", None)
    if not callable(method):
        raise ValueError("NBA API client must expose provider provenance")
    manifest = method()
    expected_keys = {
        "schema_version", "schedule_provider", "timing_provider",
        "actual_start_event", "actual_end_event", "period_boundary_event",
        "resources",
    }
    if not isinstance(manifest, dict) or set(manifest) != expected_keys:
        raise ValueError("NBA provider provenance has invalid top-level schema")
    if manifest["schema_version"] != 1:
        raise ValueError("NBA provider provenance has unsupported schema version")
    expected_semantics = {
        "actual_start_event": ACTUAL_START_EVENT,
        "actual_end_event": ACTUAL_END_EVENT,
        "period_boundary_event": PERIOD_BOUNDARY_EVENT,
    }
    for field, expected in expected_semantics.items():
        if manifest[field] != expected:
            raise ValueError(f"NBA provider provenance changes {field}")
    resources = manifest["resources"]
    if not isinstance(resources, list) or not resources:
        raise ValueError("NBA provider provenance must fingerprint fetched resources")
    seen_urls: set[str] = set()
    for resource in resources:
        if not isinstance(resource, dict) or set(resource) != {
            "url", "cache_path", "bytes", "sha256", "source"
        }:
            raise ValueError("NBA provider resource has invalid schema")
        if not isinstance(resource["url"], str) or not resource["url"]:
            raise ValueError("NBA provider resource URL is missing")
        if resource["url"] in seen_urls:
            raise ValueError("NBA provider provenance contains duplicate URLs")
        seen_urls.add(resource["url"])
        if not isinstance(resource["bytes"], int) or resource["bytes"] <= 0:
            raise ValueError("NBA provider resource byte count is invalid")
        if (
            not isinstance(resource["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", resource["sha256"]) is None
        ):
            raise ValueError("NBA provider resource SHA-256 is invalid")
        if resource["source"] not in {
            "cache", "network", "network_verified_identical"
        }:
            raise ValueError("NBA provider resource source is invalid")
        if not isinstance(resource["cache_path"], str) or not resource["cache_path"]:
            raise ValueError("NBA provider resource cache path is invalid")
        try:
            observed = file_fingerprint(resource["cache_path"])
        except FileNotFoundError as exc:
            raise ValueError(
                f"NBA provider cache resource is missing: {resource['cache_path']}"
            ) from exc
        if (
            observed["bytes"] != resource["bytes"]
            or observed["sha256"] != resource["sha256"]
        ):
            raise ValueError(
                f"NBA provider cache resource changed: {resource['cache_path']}"
            )
    payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return manifest, payload, hashlib.sha256(payload).hexdigest()


def build_game_timing_audit(
    candidate_path: str | Path,
    cache_dir: str | Path,
    output_dir: str | Path,
    *, refresh: bool = False,
    client: NbaApiClient | None = None,
    phase_contract_path: str | Path = DEFAULT_PHASE_CONTRACT,
) -> dict[str, Any]:
    output = Path(output_dir).expanduser().resolve()
    cache = Path(cache_dir).expanduser().resolve()
    candidate = Path(candidate_path).expanduser().resolve()
    contract_path = Path(phase_contract_path).expanduser().resolve()
    contract = load_phase_contract(contract_path)
    if contract.sport != "nba":
        raise ValueError(f"NBA timing requires an NBA phase contract, got {contract.sport!r}")
    if contract.regulation_period_minutes != 12:
        raise ValueError("NBA phase contract must use 12-minute regulation periods")
    if tuple(phase.key for phase in contract.analysis_phases) != NBA_ANALYSIS_PHASES:
        raise ValueError("NBA phase contract must expose exactly five eligible phases")
    expected_contract_semantics = (
        ACTUAL_START_EVENT, ACTUAL_END_EVENT, PERIOD_BOUNDARY_EVENT
    )
    if (
        contract.actual_start_event,
        contract.actual_end_event,
        contract.period_boundary_event,
    ) != expected_contract_semantics:
        raise ValueError("NBA phase contract changes the frozen LiveData event semantics")
    contract_record = phase_contract_fingerprint(contract_path)
    contract_sha = str(contract_record["sha256"])
    if output.exists():
        raise FileExistsError(f"NBA timing output directory already exists: {output}")
    for source in (cache, candidate, contract_path):
        if source == output or source.is_relative_to(output) or output.is_relative_to(source):
            raise ValueError("NBA timing output must not overlap inputs or API cache")
    candidates, start, end = _read_candidates(candidate)
    api = client or NbaApiClient(cache)
    schedules = api.schedule_games(start, end, refresh=refresh)
    schedule_ids = [row.game_id for row in schedules]
    if len(schedule_ids) != len(set(schedule_ids)):
        raise ValueError("Official NBA schedule contains duplicate game IDs")
    audits = match_market_candidates(candidates, schedules)
    if len(audits) != len(candidates):
        raise ValueError("NBA matcher did not return one row per candidate")
    assert_one_to_one_matches(audits)
    match_rows = {row.market_id: _match_row(row) for row in audits}
    timing_rows: list[dict[str, Any]] = []
    for audit in audits:
        if not audit.is_matched:
            continue
        assert audit.matched_game_id is not None
        try:
            timing = api.game_timing(audit.matched_game_id, refresh=refresh)
            validate_schedule_timing(audit.schedule_matches[0], timing)
            timing_rows.append(_timing_row(audit, timing, contract_sha))
        except Exception as exc:
            row = match_rows[audit.market_id]
            row.update(
                timing_status="failed",
                timing_exclusion_reason="timing_fetch_or_parse_failure",
                timing_error_type=type(exc).__name__,
                timing_error_message=str(exc),
            )
        else:
            match_rows[audit.market_id]["timing_status"] = "passed"

    _, provenance_payload, provenance_sha256 = _validated_provenance(api)
    for row in timing_rows:
        row["provider_provenance_sha256"] = provenance_sha256
    summary = {
        "schema_version": 1,
        "schedule_source": "official_nba_historical_schedule",
        "timestamp_source": "official_nba_livedata_timeActual",
        "espn_fallback_used": False,
        "actual_start_event": ACTUAL_START_EVENT,
        "actual_end_event": ACTUAL_END_EVENT,
        "period_boundary_event": PERIOD_BOUNDARY_EVENT,
        "provider_provenance_sha256": provenance_sha256,
        "phase_contract_sha256": contract_sha,
        "analysis_phases": [phase.key for phase in contract.analysis_phases],
        "candidate_date_min": start.isoformat(),
        "candidate_date_max": end.isoformat(),
        "candidate_markets": len(candidates),
        "schedule_records": len(schedules),
        "exact_final_matches": sum(row.is_matched for row in audits),
        "timing_games_written": len(timing_rows),
        "match_exclusions": dict(sorted(Counter(
            row.exclusion_reason for row in audits if row.exclusion_reason
        ).items())),
        "timing_exclusions": dict(sorted(Counter(
            row["timing_exclusion_reason"] for row in match_rows.values()
            if row["timing_exclusion_reason"]
        ).items())),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:
        _write_parquet((_schedule_row(row) for row in schedules), SCHEDULE_SCHEMA,
                       staging / SCHEDULE_OUTPUT, "official_date, game_id")
        _write_parquet(match_rows.values(), MATCH_SCHEMA, staging / MATCH_OUTPUT, "market_id")
        _write_parquet(timing_rows, TIMING_SCHEMA, staging / TIMING_OUTPUT, "official_date, game_id")
        (staging / SUMMARY_OUTPUT).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (staging / PROVENANCE_OUTPUT).write_bytes(provenance_payload)
        schemas = {
            SCHEDULE_OUTPUT: SCHEDULE_SCHEMA,
            MATCH_OUTPUT: MATCH_SCHEMA,
            TIMING_OUTPUT: TIMING_SCHEMA,
        }
        output_names = (*schemas, SUMMARY_OUTPUT, PROVENANCE_OUTPUT)
        manifest = {
            "schema_version": 1,
            "stage": "nba_game_timing",
            "inputs": {
                "candidates": file_fingerprint(candidate),
                "phase_contract": contract_record,
            },
            "provider_cache_inputs": [
                file_fingerprint(resource["cache_path"])
                for resource in json.loads(provenance_payload)["resources"]
            ],
            "schemas": {
                name: [list(row) for row in schema] for name, schema in schemas.items()
            },
            "outputs": {
                name: file_fingerprint(staging / name, relative_to=staging)
                for name in output_names
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        verify_game_timing_run(staging)
        if output.exists():
            raise FileExistsError(f"NBA timing output appeared during build: {output}")
        os.replace(staging, output)
        verify_game_timing_run(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return summary


def verify_game_timing_run(run_dir: str | Path) -> dict[str, Any]:
    """Re-open and reconcile the complete immutable timing publication."""

    run = Path(run_dir).expanduser().resolve()
    schemas = {
        SCHEDULE_OUTPUT: SCHEDULE_SCHEMA,
        MATCH_OUTPUT: MATCH_SCHEMA,
        TIMING_OUTPUT: TIMING_SCHEMA,
    }
    expected_files = {*schemas, SUMMARY_OUTPUT, PROVENANCE_OUTPUT, MANIFEST_OUTPUT}
    if not run.is_dir() or {path.name for path in run.iterdir()} != expected_files:
        raise ArtifactManifestError("NBA timing run file set is incomplete or unexpected")
    try:
        manifest = json.loads((run / MANIFEST_OUTPUT).read_text(encoding="utf-8"))
        summary = json.loads((run / SUMMARY_OUTPUT).read_text(encoding="utf-8"))
        provenance_bytes = (run / PROVENANCE_OUTPUT).read_bytes()
        provenance = json.loads(provenance_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactManifestError("NBA timing metadata is invalid JSON") from exc
    if set(manifest) != {
        "schema_version", "stage", "inputs", "provider_cache_inputs",
        "schemas", "outputs",
    }:
        raise ArtifactManifestError("NBA timing manifest keys mismatch")
    if manifest["schema_version"] != 1 or manifest["stage"] != "nba_game_timing":
        raise ArtifactManifestError("NBA timing manifest identity mismatch")
    if set(manifest["inputs"]) != {"candidates", "phase_contract"}:
        raise ArtifactManifestError("NBA timing input declaration mismatch")
    candidate_path = verify_fingerprint(manifest["inputs"]["candidates"])
    contract_path = verify_fingerprint(manifest["inputs"]["phase_contract"])
    require_parquet_schema(candidate_path, CANDIDATE_SCHEMA, "NBA timing candidate input")
    contract = load_phase_contract(contract_path)
    if contract.sport != "nba":
        raise ArtifactManifestError("NBA timing phase contract has wrong sport")
    if contract.regulation_period_minutes != 12:
        raise ArtifactManifestError("NBA timing phase contract must use 12-minute periods")
    if tuple(phase.key for phase in contract.analysis_phases) != NBA_ANALYSIS_PHASES:
        raise ArtifactManifestError("NBA timing phase contract eligible phases mismatch")
    cache_records = manifest["provider_cache_inputs"]
    if not isinstance(cache_records, list) or not cache_records:
        raise ArtifactManifestError("NBA timing provider-cache declaration is empty")
    for record in cache_records:
        verify_fingerprint(record)
    declared = {name: [list(row) for row in schema] for name, schema in schemas.items()}
    if manifest["schemas"] != declared:
        raise ArtifactManifestError("NBA timing declared schemas mismatch")
    expected_outputs = {*schemas, SUMMARY_OUTPUT, PROVENANCE_OUTPUT}
    if set(manifest["outputs"]) != expected_outputs:
        raise ArtifactManifestError("NBA timing output declaration mismatch")
    for name in expected_outputs:
        if verify_fingerprint(manifest["outputs"][name], base_dir=run) != run / name:
            raise ArtifactManifestError("NBA timing output path mismatch")
    for name, schema in schemas.items():
        require_parquet_schema(run / name, schema, name)
    provenance_sha = hashlib.sha256(provenance_bytes).hexdigest()
    if summary.get("provider_provenance_sha256") != provenance_sha:
        raise ArtifactManifestError("NBA timing provenance hash does not reconcile")
    if summary.get("phase_contract_sha256") != manifest["inputs"]["phase_contract"]["sha256"]:
        raise ArtifactManifestError("NBA timing phase-contract hash does not reconcile")
    if tuple(summary.get("analysis_phases", ())) != NBA_ANALYSIS_PHASES:
        raise ArtifactManifestError("NBA timing summary eligible phases mismatch")
    resource_records = provenance.get("resources") if isinstance(provenance, dict) else None
    if not isinstance(resource_records, list) or len(resource_records) != len(cache_records):
        raise ArtifactManifestError("NBA provider resources do not reconcile with manifest")
    for resource, cache_record in zip(resource_records, cache_records, strict=True):
        if (
            resource.get("cache_path") != cache_record.get("path")
            or resource.get("bytes") != cache_record.get("bytes")
            or resource.get("sha256") != cache_record.get("sha256")
        ):
            raise ArtifactManifestError("NBA provider cache fingerprint lineage mismatch")
    con = duckdb.connect()
    try:
        q = lambda name: str(run / name).replace("'", "''")
        candidate_count = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{str(candidate_path).replace("'", "''")}')"
        ).fetchone()[0])
        candidate_ids = {
            row[0] for row in con.execute(
                f"SELECT market_id FROM read_parquet('{str(candidate_path).replace("'", "''")}')"
            ).fetchall()
        }
        schedule_count, schedule_unique = con.execute(
            f"SELECT count(*), count(DISTINCT game_id) FROM read_parquet('{q(SCHEDULE_OUTPUT)}')"
        ).fetchone()
        match_count, match_unique = con.execute(
            f"SELECT count(*), count(DISTINCT market_id) FROM read_parquet('{q(MATCH_OUTPUT)}')"
        ).fetchone()
        timing_count, timing_games, timing_markets = con.execute(
            f"SELECT count(*), count(DISTINCT game_id), count(DISTINCT market_id) "
            f"FROM read_parquet('{q(TIMING_OUTPUT)}')"
        ).fetchone()
        bad_timing = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{q(TIMING_OUTPUT)}') WHERE "
            "NOT (actual_start_utc < period_2_start_utc "
            "AND period_2_start_utc < period_3_start_utc "
            "AND period_3_start_utc < period_4_start_utc "
            "AND period_4_start_utc < actual_end_utc) "
            f"OR provider_provenance_sha256 <> '{provenance_sha}' "
            f"OR phase_contract_sha256 <> '{manifest['inputs']['phase_contract']['sha256']}'"
        ).fetchone()[0])
        passed = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{q(MATCH_OUTPUT)}') WHERE timing_status='passed'"
        ).fetchone()[0])
        match_ids = {
            row[0] for row in con.execute(
                f"SELECT market_id FROM read_parquet('{q(MATCH_OUTPUT)}')"
            ).fetchall()
        }
        passed_pairs = {
            (row[0], row[1]) for row in con.execute(
                f"SELECT market_id, matched_game_id FROM read_parquet('{q(MATCH_OUTPUT)}') "
                "WHERE timing_status='passed'"
            ).fetchall()
        }
        timing_pairs = {
            (row[0], row[1]) for row in con.execute(
                f"SELECT market_id, game_id FROM read_parquet('{q(TIMING_OUTPUT)}')"
            ).fetchall()
        }
    finally:
        con.close()
    if schedule_count != schedule_unique:
        raise ArtifactManifestError("NBA serialized schedule game IDs are not unique")
    if match_count != candidate_count or match_count != match_unique:
        raise ArtifactManifestError("NBA serialized match audit is not one row per candidate")
    if match_ids != candidate_ids:
        raise ArtifactManifestError("NBA serialized match audit does not cover candidate identities")
    if timing_count != timing_games or timing_count != timing_markets or timing_count != passed:
        raise ArtifactManifestError("NBA serialized timing rows do not reconcile with passed matches")
    if timing_pairs != passed_pairs:
        raise ArtifactManifestError("NBA serialized timing identities do not match passed matches")
    if bad_timing:
        raise ArtifactManifestError("NBA serialized timing boundaries or lineage are invalid")
    expected_counts = {
        "candidate_markets": candidate_count,
        "schedule_records": schedule_count,
        "timing_games_written": timing_count,
    }
    if any(summary.get(key) != value for key, value in expected_counts.items()):
        raise ArtifactManifestError("NBA timing summary counts do not reconcile")
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--run-dir", "--output-dir", dest="output_dir", required=True)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--phase-contract", default=str(DEFAULT_PHASE_CONTRACT))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    summary = build_game_timing_audit(
        args.candidates, args.cache_dir, args.output_dir, refresh=args.refresh,
        phase_contract_path=args.phase_contract,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
