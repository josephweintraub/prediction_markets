"""Collect bounded AO provider-observed clocks for frozen accepted ATP matches.

The start is the official ``actual_start_time`` field at minute precision in
Australia/Melbourne. The end is the recorded terminal competitive point, not
start plus duration. Neither field certifies a physical second-exact first
serve or zero provider latency. Source JSON and all exclusions are retained.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import shutil
import sys
import urllib.request
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

import duckdb

from analysis.multisport_game_dynamics.provider_extractors import (
    match_name_pair,
    names_match,
    normalize_name,
)
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    require_columns,
    require_exact_schema,
    write_json,
    write_parquet,
)


BASE_URL = "https://prod-scores-api.ausopen.com"
TIMEZONE_EVIDENCE_URL = (
    "https://ausopen.com/themes/custom/ausopen_official/"
    "common.61a74cadc67f18291423.js"
)
SOURCE_TIMEZONE = "Australia/Melbourne"
CLOCK_BASIS = "ao_provider_actual_start_and_terminal_point"
CLOCK_STATUS = "verified_provider_observation"
QUALIFICATION = (
    "minute_precision_provider_start;final_competitive_point_record;"
    "provider_latency_unquantified;not_second_exact_first_serve"
)
COMPETITIVE_TYPES = {"point", "game", "set", "match"}
SCHEMA = (
    ("sport", "VARCHAR"), ("event_slug", "VARCHAR"), ("market_date", "DATE"),
    ("ao_match_id", "VARCHAR"), ("official_match_date", "DATE"),
    ("participant_1", "VARCHAR"), ("participant_2", "VARCHAR"),
    ("result_label", "VARCHAR"),
    ("actual_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("actual_end_utc", "TIMESTAMP WITH TIME ZONE"),
    ("clock_basis", "VARCHAR"), ("start_source_field", "VARCHAR"),
    ("end_source_field", "VARCHAR"), ("source_timezone", "VARCHAR"),
    ("start_precision_seconds", "INTEGER"), ("end_precision_seconds", "INTEGER"),
    ("clock_status", "VARCHAR"), ("qualification", "VARCHAR"),
    ("first_completed_point_utc", "TIMESTAMP WITH TIME ZONE"),
    ("first_point_start_delta_seconds", "DOUBLE"),
    ("first_point_near_start", "BOOLEAN"), ("actual_start_literal", "VARCHAR"),
    ("terminal_point_id", "VARCHAR"), ("terminal_point_timestamp", "BIGINT"),
    ("results_source_url", "VARCHAR"), ("match_source_url", "VARCHAR"),
    ("evidence_results_cache", "VARCHAR"), ("evidence_match_cache", "VARCHAR"),
    ("exclusion_reason", "VARCHAR"),
    ("frozen_provider_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("official_minus_scheduled_date_days", "INTEGER"),
    ("official_minus_market_date_days", "INTEGER"),
    ("official_full_participants", "VARCHAR"), ("official_winner", "VARCHAR"),
    ("competitive_chronology_valid", "BOOLEAN"), ("competitive_point_count", "INTEGER"),
    ("competitive_timestamp_reversal_count", "INTEGER"),
    ("competitive_duplicate_id_count", "INTEGER"),
    ("competitive_conflicting_duplicate_id_count", "INTEGER"),
    ("terminal_is_last_logical_point", "BOOLEAN"),
    ("competitive_max_reversal_seconds", "DOUBLE"),
    ("competitive_missing_timestamp_count", "INTEGER"),
)


class TimingError(ValueError):
    """A source record fails the literal clock or identity contract."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise TimingError(reason)


def _date(value: Any) -> date:
    _require(isinstance(value, str), "invalid_official_date")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise TimingError("invalid_official_date") from exc
    _require(result.isoformat() == value, "invalid_official_date")
    return result


def _timestamp(value: Any) -> datetime:
    _require(type(value) is int and value > 0, "invalid_competitive_timestamp")
    try:
        return datetime.fromtimestamp(value, timezone.utc)
    except (ValueError, OverflowError, OSError) as exc:
        raise TimingError("invalid_competitive_timestamp") from exc


def _exact_pair(left: tuple[str, str], right: tuple[str, str]) -> bool:
    a = tuple(normalize_name(name) for name in left)
    b = tuple(normalize_name(name) for name in right)
    return len(set(a)) == 2 and len(set(b)) == 2 and set(a) == set(b)


def reference_identity(frozen: Mapping[str, Any]) -> tuple[tuple[str, str], str]:
    """Use frozen provider full names when market labels contain surnames."""
    raw = str(frozen.get("provider_participants") or "")
    parts = tuple(part.strip() for part in raw.split(" | "))
    _require(len(parts) == 2 and all(parts), "missing_frozen_full_participants")
    pair = (parts[0], parts[1])
    _require(match_name_pair((frozen["participant_1"], frozen["participant_2"]), pair)
             is not None, "frozen_market_provider_identity_conflict")
    winners = [name for name in pair if names_match(str(frozen["result_label"]), name)]
    _require(len(winners) == 1, "ambiguous_frozen_winner")
    return pair, winners[0]


def _score(teams: list[Mapping[str, Any]]) -> tuple[tuple[Any, ...], ...]:
    """Require an ordinary completed men's best-of-five result."""
    scores = []
    for team in teams:
        raw = team.get("score")
        _require(isinstance(raw, list) and 3 <= len(raw) <= 5, "irregular_final_score")
        values = []
        for index, entry in enumerate(raw, 1):
            _require(isinstance(entry, Mapping) and entry.get("set") == index,
                     "irregular_final_score")
            game = entry.get("game")
            _require(isinstance(game, str) and re.fullmatch(r"[0-7]", game) is not None
                     and type(entry.get("winner")) is bool, "irregular_final_score")
            tie_break = entry.get("tie_break")
            _require(tie_break is None or (type(tie_break) is int and tie_break >= 0),
                     "irregular_final_score")
            values.append((index, int(game), entry["winner"], tie_break))
        scores.append(tuple(values))
    _require(len(scores[0]) == len(scores[1]), "irregular_final_score")
    for a, b in zip(*scores):
        _require(a[2] != b[2], "irregular_final_score")
        winning, losing = (a[1], b[1]) if a[2] else (b[1], a[1])
        _require((winning == 6 and losing <= 4) or (winning == 7 and losing in (5, 6)),
                 "irregular_final_score")
    winner_indices = [index for index, team in enumerate(teams) if team.get("status") == "Winner"]
    _require(len(winner_indices) == 1, "missing_or_ambiguous_official_winner")
    winner_index = winner_indices[0]
    _require(sum(value[2] for value in scores[winner_index]) == 3
             and scores[winner_index][-1][2], "irregular_final_score")
    return tuple(scores)


def result_records(payload: Mapping[str, Any], year: int, cache: str, url: str) -> list[dict[str, Any]]:
    """Resolve result-row team/player references without trusting current branding."""
    _require(str(payload.get("year", {}).get("year")) == str(year), "results_year_conflict")
    teams = {str(team["uuid"]): team for team in payload.get("teams", [])}
    players = {str(player["uuid"]): player for player in payload.get("players", [])}
    events = {str(event["uuid"]): event for event in payload.get("events", [])}
    records = []
    for raw in payload.get("matches", []):
        event = events.get(str(raw.get("event_uuid")), {})
        if event.get("name") != "Men's Singles" or not re.fullmatch(r"MS[0-9]{3}", str(raw.get("match_id"))):
            continue
        names = []
        for side in raw.get("teams", []):
            team = teams.get(str(side.get("team_id")), {})
            ids = team.get("players", [])
            if len(ids) != 1 or str(ids[0]) not in players:
                names = []
                break
            player = players[str(ids[0])]
            if player.get("gender") != "M" or not player.get("full_name"):
                names = []
                break
            names.append(str(player["full_name"]))
        if len(names) != 2:
            continue
        record = dict(raw)
        record.update(full_names=tuple(names), results_cache=cache, results_url=url)
        records.append(record)
    return records


def select_result(frozen: Mapping[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    pair, winner = reference_identity(frozen)
    matches = [row for row in records if _exact_pair(pair, row["full_names"])]
    _require(len(matches) == 1, "no_official_pair_match" if not matches else "ambiguous_official_pair_match")
    result = matches[0]
    _require(result.get("match_state") == "Complete" and
             result.get("match_status", {}).get("code") == "C", "noncomplete_or_irregular_official_result")
    _require(not result.get("team_substituted"), "substituted_official_team")
    _score(result["teams"])
    winner_index = next(i for i, team in enumerate(result["teams"]) if team.get("status") == "Winner")
    _require(normalize_name(result["full_names"][winner_index]) == normalize_name(winner),
             "official_frozen_winner_conflict")
    return result


def chronology_summary(detail: Mapping[str, Any]) -> dict[str, Any]:
    """Check recorded timestamps against literal numeric competitive point IDs."""
    commentary = detail.get("commentary")
    _require(isinstance(commentary, list), "missing_competitive_commentary")
    match_id = str(detail.get("match_id"))
    competitive = [point for point in commentary if point.get("type") in COMPETITIVE_TYPES]
    by_id: dict[str, list[Mapping[str, Any]]] = {}
    pattern = re.compile(re.escape(match_id) + r"-([0-9]{3})-([0-9]{3})-([0-9]{3})")
    ordered = []
    for point in competitive:
        point_id = str(point.get("id"))
        parsed = pattern.fullmatch(point_id)
        _require(parsed is not None, "competitive_point_identity_conflict")
        by_id.setdefault(point_id, []).append(point)
        if point.get("timestamp") is not None:
            _timestamp(point["timestamp"])
            ordered.append((tuple(map(int, parsed.groups())), point))
    ordered.sort(key=lambda value: value[0])
    points = [value[1] for value in ordered]
    reversals = [a["timestamp"] - b["timestamp"] for a, b in zip(points, points[1:])
                 if a["timestamp"] > b["timestamp"]]
    duplicate_count = sum(len(values) > 1 for values in by_id.values())
    conflicting_count = sum(len({point.get("timestamp") for point in values}) > 1
                            for values in by_id.values())
    terminals = [point for point in competitive if point.get("type") == "match"]
    last = bool(points) and len(terminals) == 1 and terminals[0]["id"] == points[-1]["id"]
    return {
        "competitive_chronology_valid": bool(points) and not reversals and duplicate_count == 0 and last,
        "competitive_point_count": len(competitive),
        "competitive_timestamp_reversal_count": len(reversals),
        "competitive_duplicate_id_count": duplicate_count,
        "competitive_conflicting_duplicate_id_count": conflicting_count,
        "terminal_is_last_logical_point": last,
        "competitive_max_reversal_seconds": float(max(reversals, default=0)),
        "competitive_missing_timestamp_count": len(competitive) - len(points),
    }


def verify_clock(frozen: Mapping[str, Any], result: Mapping[str, Any], detail: Mapping[str, Any],
                 year: int, timezone_verified: bool) -> dict[str, Any]:
    """Validate literal provider boundaries; elapsed duration is never a clock."""
    _require(timezone_verified, "missing_verified_source_timezone")
    pair, winner = reference_identity(frozen)
    match_id = str(result["match_id"])
    _require(detail.get("match_id") == match_id, "detail_match_id_conflict")
    official_date = _date(result.get("date"))
    _require(official_date.year == year and detail.get("date") == result.get("date"),
             "official_year_or_date_conflict")
    event = detail.get("event", {})
    _require(event.get("event_name") == "Men's Singles"
             and event.get("title") == f"{year} Men's Singles"
             and event.get("tournament_period") == "Main Draw", "detail_tournament_identity_conflict")
    _require(detail.get("match_state") == "Complete"
             and detail.get("match_status", {}).get("code") == "C"
             and not detail.get("team_substituted"), "noncomplete_or_irregular_detail")
    teams = detail.get("teams")
    _require(isinstance(teams, list) and len(teams) == 2, "detail_player_identity_conflict")
    names = []
    for side in teams:
        players = side.get("players", [])
        _require(len(players) == 1 and isinstance(players[0], Mapping)
                 and players[0].get("gender") == "M" and bool(players[0].get("full_name")),
                 "detail_player_identity_conflict")
        names.append(str(players[0]["full_name"]))
    _require(_exact_pair(pair, (names[0], names[1])) and tuple(names) == result["full_names"],
             "detail_player_identity_conflict")
    _require(tuple(str(side.get("team_id")) for side in teams)
             == tuple(str(side.get("team_id")) for side in result["teams"]), "detail_team_id_conflict")
    score = _score(teams)
    _require(score == _score(result["teams"]), "detail_result_score_conflict")
    winner_index = next(i for i, team in enumerate(teams) if team.get("status") == "Winner")
    _require(normalize_name(names[winner_index]) == normalize_name(winner), "detail_winner_conflict")
    literal = detail.get("actual_start_time")
    _require(isinstance(literal, str) and re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", literal) is not None,
             "missing_or_invalid_actual_start")
    _require(literal == result.get("actual_start_time"), "results_detail_actual_start_conflict")
    start = datetime.combine(official_date, datetime.strptime(literal, "%H:%M").time(),
                             ZoneInfo(SOURCE_TIMEZONE)).astimezone(timezone.utc)
    commentary = detail.get("commentary")
    _require(isinstance(commentary, list), "missing_competitive_commentary")
    terminals = [point for point in commentary if point.get("type") == "match"]
    _require(len(terminals) == 1, "missing_or_ambiguous_terminal_point")
    terminal = terminals[0]
    _require(terminal.get("score") == "Game" and terminal.get("is_game_complete") is True,
             "unknown_terminal_score_type")
    _require(terminal.get("set") == len(score[0]) and terminal.get("winner") == winner_index + 1,
             "terminal_result_conflict")
    _require(terminal.get("games_score") == f"{score[0][-1][1]} - {score[1][-1][1]}",
             "terminal_final_score_conflict")
    point_id = str(terminal.get("id"))
    _require(re.fullmatch(re.escape(match_id) + r"-[0-9]{3}-[0-9]{3}-[0-9]{3}", point_id) is not None,
             "terminal_point_identity_conflict")
    end = _timestamp(terminal.get("timestamp"))
    competitive = [point for point in commentary if point.get("type") in COMPETITIVE_TYPES]
    observed = [point for point in competitive if point.get("timestamp") is not None]
    for point in observed:
        _timestamp(point["timestamp"])
        _require(str(point.get("id", "")).startswith(match_id + "-"), "competitive_point_identity_conflict")
    _require(observed and terminal["timestamp"] == max(point["timestamp"] for point in observed),
             "terminal_not_last_competitive_timestamp")
    first = [point for point in competitive if point.get("id") == f"{match_id}-001-001-001"]
    _require(len(first) == 1 and first[0].get("timestamp") is not None, "missing_first_completed_point")
    first_time = _timestamp(first[0]["timestamp"])
    _require(first[0]["timestamp"] == min(point["timestamp"] for point in observed),
             "first_point_timestamp_conflict")
    _require(start <= first_time < end and end.year == year, "actual_clock_order_or_year_conflict")
    chronology = chronology_summary(detail)
    _require(chronology["competitive_duplicate_id_count"] == 0, "duplicate_competitive_point_id")
    _require(chronology["terminal_is_last_logical_point"], "terminal_not_last_logical_point")
    _require(chronology["competitive_timestamp_reversal_count"] == 0,
             "internal_competitive_timestamp_reversal")
    delta = (first_time - start).total_seconds()
    scheduled = frozen.get("provider_start_utc")
    scheduled_day = scheduled.astimezone(ZoneInfo(SOURCE_TIMEZONE)).date() if scheduled else None
    return {
        "ao_match_id": match_id, "official_match_date": official_date,
        "actual_start_utc": start, "actual_end_utc": end, "clock_basis": CLOCK_BASIS,
        "start_source_field": "actual_start_time", "end_source_field": "commentary[type=match].timestamp",
        "source_timezone": SOURCE_TIMEZONE, "start_precision_seconds": 60, "end_precision_seconds": 1,
        "clock_status": CLOCK_STATUS, "qualification": QUALIFICATION,
        "first_completed_point_utc": first_time, "first_point_start_delta_seconds": delta,
        "first_point_near_start": delta <= 300, "actual_start_literal": literal,
        "terminal_point_id": point_id, "terminal_point_timestamp": terminal["timestamp"],
        "official_minus_scheduled_date_days": (official_date - scheduled_day).days if scheduled_day else None,
        "official_minus_market_date_days": (official_date - frozen["market_date"]).days,
        "official_full_participants": " | ".join(names), "official_winner": names[winner_index],
        **chronology,
    }


def _fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "prediction-market-research/1.0"})
    with urllib.request.urlopen(request, timeout=40) as response:
        return response.read()


def build_collection(match_audit: str | Path, run_dir: str | Path, *, year: int = 2026,
                     cache_dir: str | Path | None = None,
                     fetch: Callable[[str], bytes] = _fetch) -> dict[str, Any]:
    """Download only 15 result days and match details for accepted AO events."""
    _require(year == 2026, "unsupported_source_year")
    source = Path(match_audit).resolve()
    replay = Path(cache_dir).resolve() if cache_dir else None
    inputs = (source,) if replay is None else (source, replay)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW frozen AS SELECT * FROM read_parquet('{quoted(source)}')")
        require_columns(con, "frozen", ("sport", "event_slug", "market_date", "participant_1",
                        "participant_2", "result_label", "eligible", "provider_event_name",
                        "provider_participants", "provider_start_utc"), "Frozen match audit")
        _require(con.execute("SELECT count(*)=count(DISTINCT event_slug) FROM frozen").fetchone()[0],
                 "duplicate_frozen_event_slug")
        cursor = con.execute("SELECT * FROM frozen WHERE sport='atp' AND eligible "
                             "ORDER BY event_slug")
        names = [column[0] for column in cursor.description]
        candidates = [dict(zip(names, row)) for row in cursor.fetchall()]
        candidates = [row for row in candidates if normalize_name(str(row["provider_event_name"])) == "australian open"]
    finally:
        con.close()
    with fresh_run(run_dir, inputs) as staging:
        inventory, errors, records = [], [], []

        def collect(relative: str, url: str) -> bytes | None:
            path = staging / "source_cache" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                if replay is None:
                    payload = fetch(url)
                    path.write_bytes(payload)
                else:
                    shutil.copyfile(replay / relative, path)
                    payload = path.read_bytes()
                inventory.append({"path": path.relative_to(staging).as_posix(), "url": url,
                                  "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()})
                return payload
            except (OSError, TimeoutError) as exc:
                errors.append({"path": f"source_cache/{relative}", "url": url,
                               "error": type(exc).__name__})
                return None

        client = collect("source/ao_common.js", TIMEZONE_EVIDENCE_URL)
        timezone_verified = client is not None and b'"Australia/Melbourne"' in client and b"utc(1e3*" in client
        results_valid = True
        for day in range(1, 16):
            relative = f"results/day{day:02d}.json"
            url = f"{BASE_URL}/year/{year}/period/MD/day/{day}/results"
            raw = collect(relative, url)
            if raw is None:
                results_valid = False
                continue
            try:
                records.extend(result_records(json.loads(raw), year, f"source_cache/{relative}", url))
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                results_valid = False
                errors.append({"path": f"source_cache/{relative}", "url": url,
                               "error": str(exc) if isinstance(exc, TimingError) else type(exc).__name__})
        # Identical records repeated by a daily API do not create duplicate matches.
        by_id: dict[str, dict[str, Any]] = {}
        conflicting = set()
        for row in records:
            match_id = row["match_id"]
            comparable = {key: value for key, value in row.items() if key not in ("results_cache", "results_url")}
            if match_id in by_id:
                old = {key: value for key, value in by_id[match_id].items() if key not in ("results_cache", "results_url")}
                if old != comparable:
                    conflicting.add(match_id)
            else:
                by_id[match_id] = row
        audits, detail_cache = [], {}
        for frozen in candidates:
            audit = {name: None for name, _ in SCHEMA}
            audit.update({key: frozen[key] for key in ("sport", "event_slug", "market_date", "participant_1",
                                                     "participant_2", "result_label")})
            audit["frozen_provider_start_utc"] = frozen.get("provider_start_utc")
            try:
                _require(results_valid, "incomplete_or_invalid_results_collection")
                result = select_result(frozen, list(by_id.values()))
                match_id = result["match_id"]
                audit.update(ao_match_id=match_id, official_match_date=_date(result.get("date")),
                             results_source_url=result["results_url"], evidence_results_cache=result["results_cache"])
                _require(match_id not in conflicting, "conflicting_official_result_records")
                relative = f"match_centre/{match_id}.json"
                url = f"{BASE_URL}/match-centre/{match_id}"
                audit.update(match_source_url=url, evidence_match_cache=f"source_cache/{relative}")
                if match_id not in detail_cache:
                    detail_cache[match_id] = collect(relative, url)
                raw = detail_cache[match_id]
                _require(raw is not None, "match_detail_collection_failed")
                detail = json.loads(raw)
                audit.update(chronology_summary(detail))
                audit.update(verify_clock(frozen, result, detail, year, timezone_verified))
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                audit["exclusion_reason"] = str(exc) if isinstance(exc, TimingError) else f"malformed_source_{type(exc).__name__}"
            audits.append(audit)
        counts = Counter(row["ao_match_id"] for row in audits if row["exclusion_reason"] is None)
        for row in audits:
            if row["exclusion_reason"] is None and counts[row["ao_match_id"]] > 1:
                row["exclusion_reason"] = "duplicate_frozen_event_to_official_match"
                row["clock_status"] = None
        accepted = [row for row in audits if row["exclusion_reason"] is None]
        for name, rows in (("actual_timing.parquet", accepted), ("actual_timing_audit.parquet", audits)):
            write_parquet(staging/name, SCHEMA, (tuple(row[column] for column, _ in SCHEMA) for row in rows), ("event_slug",))
        verification = duckdb.connect()
        try:
            for name, expected in (("actual_timing.parquet", len(accepted)), ("actual_timing_audit.parquet", len(audits))):
                verification.execute(f"CREATE OR REPLACE VIEW observed AS SELECT * FROM read_parquet('{quoted(staging/name)}')")
                require_exact_schema(verification, "observed", SCHEMA, name)
                observed_count, unique_count = verification.execute(
                    "SELECT count(*),count(DISTINCT event_slug) FROM observed").fetchone()
                _require(observed_count == unique_count == expected, "published_event_grain_conflict")
        finally:
            verification.close()
        index = {"schema_version": 1, "files": sorted(inventory, key=lambda row: row["path"])}
        write_json(staging/"source_index.json", index)
        manifest = {
            "schema_version": 2, "stage": "ao_provider_actual_timing_v2",
            "status": "complete", "completed": True, "collection_complete": results_valid and not errors,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "command": [sys.executable, "-m", "analysis.diagnostics.collect_ao_actual_timing",
                        "--match-audit", str(source), "--run-dir", str(Path(run_dir).resolve()),
                        "--year", str(year)] + (["--cache-dir", str(replay)] if replay else []),
            "contract": {"year": year, "tournament": "Australian Open", "period": "Main Draw",
                         "results_days": list(range(1, 16)), "clock_basis": CLOCK_BASIS,
                         "source_timezone": SOURCE_TIMEZONE, "timezone_verified": timezone_verified,
                         "timezone_evidence_url": TIMEZONE_EVIDENCE_URL,
                         "start_source_field": "actual_start_time", "end_source_field": "commentary[type=match].timestamp",
                         "start_precision_seconds": 60, "end_precision_seconds": 1,
                         "clock_status": CLOCK_STATUS, "qualification": QUALIFICATION,
                         "identity": "unique normalized full provider-name pair and frozen winner; official results/detail year/date/score agreement",
                         "first_point_near_start": "delta<=300 seconds diagnostic only; never an acceptance filter",
                         "old_schedule": "date differences are diagnostic only; never an acceptance filter",
                         "competitive_chronology": "unique literal competitive IDs in numeric set/game/point order; recorded timestamps nondecreasing; terminal last logical point",
                         "duration": "retained in raw evidence; never used to infer start or end"},
            "counts": {"accepted_frozen_ao_events": len(candidates), "official_mens_matches": len(by_id),
                       "actual_timing_events": len(accepted), "excluded_events": len(audits)-len(accepted),
                       "passed_boundary_gates_events": len(accepted) + sum(row["exclusion_reason"] in
                            {"duplicate_competitive_point_id", "terminal_not_last_logical_point",
                             "internal_competitive_timestamp_reversal"} for row in audits),
                       "exclusion_reasons": dict(sorted(Counter(row["exclusion_reason"] for row in audits if row["exclusion_reason"]).items())),
                       "first_point_not_near_start": sum(row["first_point_near_start"] is False for row in accepted),
                       "first_point_delta_min_seconds": min((row["first_point_start_delta_seconds"] for row in accepted), default=None),
                       "first_point_delta_max_seconds": max((row["first_point_start_delta_seconds"] for row in accepted), default=None)},
            "inputs": {"match_audit": fingerprint(source)}, "source_cache": index,
            "code": {"collector": fingerprint(Path(__file__))},
            "environment": {"python": platform.python_version(), "python_executable": sys.executable,
                            "duckdb": duckdb.__version__, "platform": platform.platform()},
            "collection_errors": errors,
            "outputs": {name: artifact_fingerprint(staging/name) for name in
                        ("actual_timing.parquet", "actual_timing_audit.parquet", "source_index.json")},
        }
        write_json(staging/"actual_timing_manifest.json", manifest)
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--match-audit", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--cache-dir", help="Replay a previous run's source_cache without network access")
    args = parser.parse_args(argv)
    print(json.dumps(build_collection(args.match_audit, args.run_dir, year=args.year,
                                     cache_dir=args.cache_dir), sort_keys=True))


if __name__ == "__main__":
    main()
