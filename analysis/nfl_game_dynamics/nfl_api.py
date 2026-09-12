"""Minimal ESPN NFL schedule client and strict wall-clock timing parser.

ESPN is used because its historical summary response supplies a timezone-aware
``wallclock`` for each play.  It is a third-party, undocumented endpoint, so
raw responses are cached and the parser fails closed on schema drift, missing
quarters, invalid timestamps, or chronological disagreement.  Administrative
timeout/end markers are not game plays and are excluded by their observed ESPN
type IDs; sampled historical feeds showed that those markers can carry stale
placeholder timestamps.
"""
from __future__ import annotations

import json
import math
import os
import random
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import requests


ESPN_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
)
ESPN_SUMMARY_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"
)
NFL_CALENDAR_ZONE = ZoneInfo("America/New_York")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
NFL_PHASE_CONTRACT_PATH = PROJECT_ROOT / "configs/game_dynamics/nfl_phase_contract_v1.json"
NFL_TAXONOMY_AUDIT_PATH = PROJECT_ROOT / "configs/game_dynamics/nfl_espn_taxonomy_audit_v2.json"
NFL_PHASE_CONTRACT_SHA256 = "62826a4e7e647db78dfcd7862e16d73396c7942b9cdbeb7dc50b9c8b38f21cfd"
NFL_TAXONOMY_AUDIT_SHA256 = "2d2f4b3a10d73090da64f54898503d75a095df7262e74d7f598ce725a78ddfee"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _analysis_phase_keys(path: Path) -> tuple[str, ...]:
    """Return the ordered analysis-eligible projection of the frozen contract."""

    value = json.loads(path.read_text(encoding="utf-8"))
    phases = value.get("phases") if isinstance(value, dict) else None
    if not isinstance(phases, list):
        raise ValueError("NFL phase contract lacks a phase list")
    keys = tuple(
        row.get("key")
        for row in phases
        if isinstance(row, dict) and row.get("analysis_eligible") is True
    )
    if any(not isinstance(key, str) or not key for key in keys) or len(keys) != len(set(keys)):
        raise ValueError("NFL phase contract has invalid analysis phases")
    return keys


NFL_ANALYSIS_PHASES = _analysis_phase_keys(NFL_PHASE_CONTRACT_PATH)
ACTUAL_START_EVENT = "start of the opening kickoff play"
ACTUAL_END_EVENT = "timestamp of the last competitive play, with subsequent End Game evidence"
PERIOD_BOUNDARY_EVENT = "start of the first complete official play in the period"

# These are feed events rather than competitive plays.  In the audited sample,
# their wallclock values were sometimes placeholders copied from early Q1.
ADMINISTRATIVE_PLAY_TYPES: dict[str, str] = {
    "2": "End Period",
    "21": "Timeout",
    "65": "End of Half",
    "66": "End of Game",
    "74": "Official Timeout",
    "75": "Two-minute warning",
    "79": "End of Regulation",
}

# Competitive type IDs observed across the frozen regular, postseason,
# preseason, overtime, neutral-site, delayed-start, and Super Bowl audit.  This
# is intentionally an allowlist: a new ESPN taxonomy value causes an auditable
# exclusion until reviewed, rather than silently becoming a phase boundary.
COMPETITIVE_PLAY_TYPES: dict[str, str] = {
    "3": "Pass Incompletion",
    "5": "Rush",
    "7": "Sack",
    "8": "Penalty",
    "9": "Fumble Recovery (Own)",
    "12": "Kickoff Return (Offense)",
    "17": "Blocked Punt",
    "18": "Blocked Field Goal",
    "20": "Safety",
    "24": "Pass Reception",
    "26": "Pass Interception Return",
    "29": "Fumble Recovery (Opponent)",
    "30": "Muffed Punt Recovery (Opponent)",
    "32": "Kickoff Return Touchdown",
    "34": "Punt Return Touchdown",
    "36": "Interception Return Touchdown",
    "37": "Blocked Punt Touchdown",
    "38": "Blocked Field Goal Touchdown",
    "39": "Fumble Return Touchdown",
    "40": "Missed Field Goal Return",
    "51": "Pass",
    "52": "Punt",
    "53": "Kickoff",
    "59": "Field Goal Good",
    "60": "Field Goal Missed",
    "67": "Passing Touchdown",
    "68": "Rushing Touchdown",
    "80": "Sack Opp Fumble Recovery",
}

# ``pointAfterAttempt`` is nested inside touchdown plays rather than emitted as
# a top-level drive play in the audited responses.
POINT_AFTER_TYPES: dict[str, str] = {
    "0": "Not Available",
    "10": "Extra Point",
    "15": "Two Point Pass",
    "16": "Two Point Rush",
    "43": "Blocked PAT",
    "61": "Extra Point Good",
    "62": "Extra Point Missed",
}
REPRESENTATIVE_ESPN_GAME_IDS = (
    "401671718",  # regular
    "401671610",  # shortened preseason
    "401671626",  # overtime
    "401671802",  # neutral site
    "401671784",  # delayed start
    "401671886",  # postseason
    "401671889",  # Super Bowl
    "401671645",  # Blocked Punt
    "401772631",  # Muffed Punt Recovery (Opponent)
    "401671685",  # Punt Return Touchdown
    "401772845",  # Blocked Punt Touchdown
    "401671714",  # Blocked Field Goal Touchdown
    "401671494",  # Fumble Return Touchdown
    "401772630",  # Sack Opp Fumble Recovery
    "401671491",  # Missed Field Goal Return
    "401671637",  # Missed Field Goal Return
    "401673561",  # Missed Field Goal Return
    "401671651",  # top-level Two Point Pass exclusion
    "401671844",  # nested point-after taxonomy
    "401773016",  # suspended terminal
)
TAXONOMY_AUDIT_SCOPE: dict[str, Any] = {
    "abnormal_terminal_resources": 1,
    "cache_inventory_name": "2026-09-11_nfl_espn_v1",
    "cache_inventory_sha256": "f432485a8868b85046f106f711d7e2a89044715f86409a833d833c480271a807",
    "cache_inventory_sha256_method": (
        "SHA-256 of sorted UTF-8 <relative_path>\\t<byte_count>\\t<file_sha256>\\n records"
    ),
    "candidate_artifact_sha256": "d3ece3ff78040ec15751fb536ff731965fc7abdad7fb93ff07e0b0cb57af10df",
    "candidate_date_from": "2024-08-08",
    "candidate_date_to": "2026-02-08",
    "candidate_market_rows": 661,
    "normal_end_game_text_resources": 658,
    "overtime_nonzero_terminal_clock_resources": 22,
    "overtime_zero_terminal_clock_resources": 9,
    "regulation_zero_terminal_clock_resources": 627,
    "representative_game_ids_are_exhaustive": False,
    "scoreboard_resources": 150,
    "summary_resources": 659,
}


@dataclass(frozen=True)
class ScheduleGame:
    game_id: str
    official_date: date
    scheduled_start_utc: datetime
    season: int | None
    season_type: int | None
    week: int | None
    away_team_id: int
    away_abbreviation: str
    away_team_name: str
    home_team_id: int
    home_abbreviation: str
    home_team_name: str
    away_final_score: int | None
    home_final_score: int | None
    away_is_winner: bool | None
    home_is_winner: bool | None
    status_state: str
    status_detail: str
    is_completed: bool
    expected_final_period: int | None
    neutral_site: bool


@dataclass(frozen=True)
class GameTiming:
    game_id: str
    away_team_id: int
    home_team_id: int
    away_final_score: int
    home_final_score: int
    away_is_winner: bool
    home_is_winner: bool
    status_detail: str
    actual_start_utc: datetime
    period_2_start_utc: datetime
    period_3_start_utc: datetime
    period_4_start_utc: datetime
    actual_end_utc: datetime
    competitive_play_count: int
    final_period: int
    went_to_overtime: bool


@dataclass(frozen=True)
class PhaseAssignment:
    phase: str
    analysis_eligible: bool
    exclude_within_30s: bool


def _parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing or invalid {field}")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp {field} lacks a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _strict_int(value: Any, field: str) -> int:
    """Parse an integral ESPN value without bool or fractional coercion."""

    if isinstance(value, bool):
        raise ValueError(f"Missing or non-integral {field}")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(f"Missing or non-integral {field}")


def _sequence_int(value: Any, field: str) -> int:
    """Parse a non-negative integral sequence without truncating decimals."""

    parsed = _strict_int(value, field)
    if parsed < 0:
        raise ValueError(f"Missing or non-integral {field}")
    return parsed


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return _strict_int(value, "optional integer")
    except ValueError:
        return None


def _final_period(detail: Any) -> int:
    if not isinstance(detail, str):
        raise ValueError("Completed NFL status lacks an explicit final detail")
    match = re.fullmatch(r"Final(?:/(?:([2-9]\d*)?OT))?", detail)
    if match is None:
        raise ValueError(f"Unsupported completed NFL status detail: {detail!r}")
    return 4 if detail == "Final" else 4 + int(match.group(1) or 1)


def _validate_final_result(
    away_score: Any,
    home_score: Any,
    away_winner: Any,
    home_winner: Any,
    *,
    source: str,
    allow_tie: bool = False,
) -> tuple[int, int, bool, bool]:
    away = _strict_int(away_score, f"{source} away score")
    home = _strict_int(home_score, f"{source} home score")
    if away < 0 or home < 0:
        raise ValueError(f"{source} final scores must be nonnegative")
    if not isinstance(away_winner, bool) or not isinstance(home_winner, bool):
        raise ValueError(f"{source} requires two boolean winner flags")
    if away == home:
        if not allow_tie:
            raise ValueError(f"{source} tied final is unsupported for a binary game winner")
        if away_winner or home_winner:
            raise ValueError(f"{source} tied final requires both winner flags false")
        return away, home, away_winner, home_winner
    if away_winner is home_winner:
        raise ValueError(f"{source} requires exactly one true winner flag")
    score_away_won = away > home
    if away_winner is not score_away_won:
        raise ValueError(f"{source} score and winner flags disagree")
    return away, home, away_winner, home_winner


def _competition(event: Mapping[str, Any]) -> Mapping[str, Any]:
    competitions = event.get("competitions")
    if not isinstance(competitions, list) or len(competitions) != 1:
        raise ValueError("NFL event must contain exactly one competition")
    competition = competitions[0]
    if not isinstance(competition, Mapping):
        raise ValueError("NFL competition must be an object")
    return competition


def _team(competition: Mapping[str, Any], side: str) -> tuple[int, str, str, int | None, bool | None]:
    competitors = competition.get("competitors")
    if not isinstance(competitors, list):
        raise ValueError("NFL competition competitors must be a list")
    matches = [row for row in competitors if isinstance(row, Mapping) and row.get("homeAway") == side]
    if len(matches) != 1:
        raise ValueError(f"NFL competition must contain exactly one {side} team")
    row = matches[0]
    team = row.get("team")
    if not isinstance(team, Mapping):
        raise ValueError(f"NFL {side} team must be an object")
    abbreviation = team.get("abbreviation")
    name = team.get("displayName") or team.get("name")
    if not isinstance(abbreviation, str) or not abbreviation.strip():
        raise ValueError(f"NFL {side} abbreviation is missing")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"NFL {side} team name is missing")
    score = _optional_int(row.get("score"))
    winner = row.get("winner")
    if not isinstance(winner, bool):
        winner = None
    return _strict_int(team.get("id"), f"{side} team id"), abbreviation.strip().upper(), name.strip(), score, winner


def parse_scoreboard(payload: Mapping[str, Any]) -> tuple[ScheduleGame, ...]:
    """Parse an ESPN scoreboard response into stable schedule records."""

    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("Scoreboard field 'events' must be a list")
    games: list[ScheduleGame] = []
    for event in events:
        if not isinstance(event, Mapping):
            raise ValueError("Scoreboard event must be an object")
        game_id = event.get("id")
        if not isinstance(game_id, str) or not game_id.strip():
            raise ValueError("Scoreboard event id is missing")
        competition = _competition(event)
        competition_id = competition.get("id")
        if not isinstance(competition_id, str) or competition_id.strip() != game_id.strip():
            raise ValueError("Scoreboard event and competition game IDs disagree")
        scheduled = _parse_utc(competition.get("date") or event.get("date"), "competition date")
        away = _team(competition, "away")
        home = _team(competition, "home")
        status = competition.get("status") or event.get("status")
        if not isinstance(status, Mapping) or not isinstance(status.get("type"), Mapping):
            raise ValueError("NFL competition status is missing")
        status_type = status["type"]
        state = str(status_type.get("state") or "")
        detail = str(status_type.get("detail") or status_type.get("description") or "")
        completed = status_type.get("completed") is True and state.casefold() == "post"
        expected_final_period = None
        if completed:
            if (
                str(status_type.get("id")) != "3"
                or status_type.get("name") != "STATUS_FINAL"
                or status_type.get("description") != "Final"
            ):
                raise ValueError(f"Completed scoreboard game {game_id} lacks exact Final status")
            expected_final_period = _final_period(detail)
            final_result = _validate_final_result(
                away[3], home[3], away[4], home[4], source="Scoreboard",
                allow_tie=True,
            )
        else:
            final_result = (None, None, None, None)
        season = event.get("season")
        season_year = _optional_int(season.get("year")) if isinstance(season, Mapping) else None
        season_type = _optional_int(season.get("type")) if isinstance(season, Mapping) else None
        week = event.get("week")
        week_number = _optional_int(week.get("number")) if isinstance(week, Mapping) else None
        games.append(
            ScheduleGame(
                game_id=game_id.strip(),
                official_date=scheduled.astimezone(NFL_CALENDAR_ZONE).date(),
                scheduled_start_utc=scheduled,
                season=season_year,
                season_type=season_type,
                week=week_number,
                away_team_id=away[0],
                away_abbreviation=away[1],
                away_team_name=away[2],
                home_team_id=home[0],
                home_abbreviation=home[1],
                home_team_name=home[2],
                away_final_score=final_result[0],
                home_final_score=final_result[1],
                away_is_winner=final_result[2],
                home_is_winner=final_result[3],
                status_state=state,
                status_detail=detail,
                is_completed=completed,
                expected_final_period=expected_final_period,
                neutral_site=competition.get("neutralSite") is True,
            )
        )
    ids = [game.game_id for game in games]
    if len(ids) != len(set(ids)):
        raise ValueError("Scoreboard contains duplicate NFL game ids")
    return tuple(sorted(games, key=lambda game: (game.official_date, game.game_id)))


def _summary_competition(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
    header = payload.get("header")
    if not isinstance(header, Mapping):
        raise ValueError("Summary header is missing")
    game_id = header.get("id")
    competitions = header.get("competitions")
    if not isinstance(competitions, list) or len(competitions) != 1 or not isinstance(competitions[0], Mapping):
        raise ValueError("Summary header must contain exactly one competition")
    competition = competitions[0]
    competition_id = competition.get("id")
    if not isinstance(game_id, str) or not game_id.strip():
        raise ValueError("Summary header game id is missing")
    if (
        not isinstance(competition_id, str)
        or not competition_id.strip()
        or competition_id.strip() != game_id.strip()
    ):
        raise ValueError("Summary header and competition game IDs disagree")
    return game_id.strip(), competition


def _summary_team(
    competition: Mapping[str, Any], side: str
) -> tuple[int, int, bool]:
    competitors = competition.get("competitors")
    if not isinstance(competitors, list):
        raise ValueError("Completed NFL summary competitors must be a list")
    matches = [
        row for row in competitors
        if isinstance(row, Mapping) and row.get("homeAway") == side
    ]
    if len(matches) != 1:
        raise ValueError(f"Completed NFL summary requires exactly one {side} competitor")
    row = matches[0]
    team = row.get("team")
    if not isinstance(team, Mapping):
        raise ValueError(f"Completed NFL summary {side} team is missing")
    winner = row.get("winner")
    if not isinstance(winner, bool):
        raise ValueError("Completed NFL summary requires two boolean winner flags")
    return (
        _strict_int(team.get("id"), f"summary {side} team id"),
        _strict_int(row.get("score"), f"summary {side} score"),
        winner,
    )


def parse_game_timing(payload: Mapping[str, Any], expected_game_id: str | None = None) -> GameTiming:
    """Derive Q1/Q2/Q3/Q4+ boundaries from competitive-play wall clocks.

    Sequence order is ESPN's numeric ``sequenceNumber``.  Every competitive
    play must have an absolute timezone-aware timestamp, periods must progress
    monotonically, and periods 1--4 must all be represented.  Overtime periods
    remain inside Q4+.
    """

    game_id, competition = _summary_competition(payload)
    if expected_game_id is not None and game_id != str(expected_game_id):
        raise ValueError(f"Summary game id {game_id!r} does not match expected {expected_game_id!r}")
    status = competition.get("status")
    if not isinstance(status, Mapping) or not isinstance(status.get("type"), Mapping):
        raise ValueError("Summary competition status is missing")
    status_type = status["type"]
    if (
        str(status_type.get("id")) != "3"
        or status_type.get("name") != "STATUS_FINAL"
        or status_type.get("completed") is not True
        or str(status_type.get("state") or "").casefold() != "post"
        or status_type.get("description") != "Final"
    ):
        raise ValueError("NFL timing requires a completed game")
    detail = status_type.get("detail")
    if not isinstance(detail, str):
        raise ValueError("Completed NFL status lacks an explicit final detail")
    expected_final_period = _final_period(detail)
    reported_period = status.get("period")
    if reported_period is not None and _strict_int(reported_period, "status.period") != expected_final_period:
        raise ValueError("NFL final status period conflicts with final detail")
    competitors = competition.get("competitors")
    if not isinstance(competitors, list) or len(competitors) != 2:
        raise ValueError("Completed NFL summary must contain exactly two competitors")
    away_team = _summary_team(competition, "away")
    home_team = _summary_team(competition, "home")
    away_score, home_score, away_winner, home_winner = _validate_final_result(
        away_team[1], home_team[1], away_team[2], home_team[2], source="Summary"
    )

    drives = payload.get("drives")
    if not isinstance(drives, Mapping) or not isinstance(drives.get("previous"), list):
        raise ValueError("Summary drives.previous must be a list")
    if drives.get("current") not in (None, {}):
        raise ValueError("Completed NFL summary unexpectedly has a current drive")

    by_id: dict[str, tuple[int, datetime, int, str, str]] = {}
    sequence_owner: dict[int, str] = {}
    terminal_markers: list[tuple[int, int]] = []
    for drive in drives["previous"]:
        if not isinstance(drive, Mapping) or not isinstance(drive.get("plays"), list):
            raise ValueError("Each NFL drive must contain a play list")
        for play in drive["plays"]:
            if not isinstance(play, Mapping):
                raise ValueError("NFL play must be an object")
            type_obj = play.get("type")
            if not isinstance(type_obj, Mapping):
                raise ValueError("NFL play type is missing")
            type_id = str(type_obj.get("id") or "")
            type_text = str(type_obj.get("text") or "")
            if type_id in ADMINISTRATIVE_PLAY_TYPES:
                if type_text != ADMINISTRATIVE_PLAY_TYPES[type_id]:
                    raise ValueError(f"Administrative play type {type_id} changed meaning to {type_text!r}")
                if type_id == "66":
                    marker_id = play.get("id")
                    period_obj = play.get("period")
                    if not isinstance(marker_id, str) or not marker_id.strip() or not isinstance(period_obj, Mapping):
                        raise ValueError("NFL terminal marker lacks identity or period")
                    terminal_period = _strict_int(period_obj.get("number"), f"play {marker_id} period")
                    terminal_clock_obj = play.get("clock")
                    terminal_clock = (
                        terminal_clock_obj.get("displayValue")
                        if isinstance(terminal_clock_obj, Mapping) else None
                    )
                    if play.get("text") != "END GAME":
                        raise ValueError(
                            "NFL terminal indicates a suspended or shortened game: "
                            "expected exact END GAME text"
                        )
                    if not isinstance(terminal_clock, str) or re.fullmatch(
                        r"(?:[0-9]|1[0-5]):[0-5][0-9]", terminal_clock
                    ) is None:
                        raise ValueError(
                            "NFL terminal indicates a suspended or shortened game: "
                            "missing or invalid game clock"
                        )
                    if terminal_period == 4 and terminal_clock != "0:00":
                        raise ValueError(
                            "NFL terminal indicates a suspended or shortened regulation game: "
                            "expected a zero game clock"
                        )
                    terminal_markers.append((
                        _sequence_int(play.get("sequenceNumber"), f"play {marker_id} sequenceNumber"),
                        terminal_period,
                    ))
                continue
            if COMPETITIVE_PLAY_TYPES.get(type_id) != type_text:
                raise ValueError(
                    f"Unreviewed competitive play taxonomy: id={type_id!r}, "
                    f"text={type_text!r}"
                )
            play_id = play.get("id")
            if not isinstance(play_id, str) or not play_id.strip():
                raise ValueError("Competitive NFL play id is missing")
            sequence = _sequence_int(play.get("sequenceNumber"), f"play {play_id} sequenceNumber")
            period_obj = play.get("period")
            if not isinstance(period_obj, Mapping):
                raise ValueError(f"Competitive NFL play {play_id} period is missing")
            period = _strict_int(period_obj.get("number"), f"play {play_id} period")
            if period < 1:
                raise ValueError(f"Competitive NFL play {play_id} has invalid period {period}")
            timestamp = _parse_utc(play.get("wallclock"), f"play {play_id} wallclock")
            clock_obj = play.get("clock")
            clock = clock_obj.get("displayValue") if isinstance(clock_obj, Mapping) else None
            if not isinstance(clock, str) or not clock:
                raise ValueError(f"Competitive NFL play {play_id} clock is missing")
            point_after = play.get("pointAfterAttempt")
            if point_after is not None:
                if not isinstance(point_after, Mapping):
                    raise ValueError(f"Competitive NFL play {play_id} has invalid pointAfterAttempt")
                point_id = str(point_after.get("id"))
                point_text = point_after.get("text")
                if POINT_AFTER_TYPES.get(point_id) != point_text:
                    raise ValueError(
                        f"Unreviewed point-after taxonomy: id={point_id!r}, text={point_text!r}"
                    )
            payload_value = (sequence, timestamp, period, type_id, clock)
            prior = by_id.get(play_id)
            if prior is not None and prior != payload_value:
                raise ValueError(f"Duplicate NFL play id {play_id} has contradictory payloads")
            by_id[play_id] = payload_value
            owner = sequence_owner.get(sequence)
            if owner is not None and owner != play_id:
                raise ValueError(f"NFL sequence {sequence} belongs to multiple play ids")
            sequence_owner[sequence] = play_id

    if not by_id:
        raise ValueError("NFL summary has no competitive plays")
    ordered = sorted(
        (sequence, timestamp, period, play_id, type_id, clock)
        for play_id, (sequence, timestamp, period, type_id, clock) in by_id.items()
    )
    periods = [row[2] for row in ordered]
    if any(later < earlier for earlier, later in zip(periods, periods[1:])):
        raise ValueError("Competitive NFL play periods are not chronological")
    timestamps = [row[1] for row in ordered]
    if any(later < earlier for earlier, later in zip(timestamps, timestamps[1:])):
        raise ValueError("Competitive NFL play timestamps are not chronological")
    present = set(periods)
    max_period = max(present)
    if present != set(range(1, max_period + 1)) or not {1, 2, 3, 4}.issubset(present):
        raise ValueError(f"NFL game lacks complete Q1-Q4 competitive-play boundaries: periods={sorted(present)}")
    if max_period != expected_final_period:
        raise ValueError(
            f"NFL final status implies period {expected_final_period}, but competitive plays end in period {max_period}"
        )
    first = ordered[0]
    if first[2] != 1 or first[4] not in {"12", "53"} or first[5] != "15:00":
        raise ValueError("NFL feed lacks a defensible opening kickoff record")
    last_sequence = ordered[-1][0]
    if not any(sequence > last_sequence and period == expected_final_period for sequence, period in terminal_markers):
        raise ValueError("NFL feed lacks terminal End of Game evidence after the final competitive play")
    first_by_period = {period: next(row[1] for row in ordered if row[2] == period) for period in present}
    boundaries = [first_by_period[index] for index in (1, 2, 3, 4)]
    actual_end = ordered[-1][1]
    if not (boundaries[0] < boundaries[1] < boundaries[2] < boundaries[3]):
        raise ValueError("NFL quarter boundaries are not strictly ordered")
    if not boundaries[3] < actual_end:
        raise ValueError(
            "NFL timing indicates a suspended or shortened game: "
            "Quarter 4 has no positive competitive-play span"
        )
    return GameTiming(
        game_id=game_id,
        away_team_id=away_team[0],
        home_team_id=home_team[0],
        away_final_score=away_score,
        home_final_score=home_score,
        away_is_winner=away_winner,
        home_is_winner=home_winner,
        status_detail=detail,
        actual_start_utc=boundaries[0],
        period_2_start_utc=boundaries[1],
        period_3_start_utc=boundaries[2],
        period_4_start_utc=boundaries[3],
        actual_end_utc=actual_end,
        competitive_play_count=len(ordered),
        final_period=max_period,
        went_to_overtime=max_period > 4,
    )


def validate_schedule_timing(game: ScheduleGame, timing: GameTiming) -> None:
    """Require the scoreboard and summary to describe the identical final game."""

    if not game.is_completed or game.expected_final_period is None:
        raise ValueError(f"Scoreboard game {game.game_id} is not exactly final")
    expected = {
        "game_id": game.game_id,
        "away_team_id": game.away_team_id,
        "home_team_id": game.home_team_id,
        "away_final_score": game.away_final_score,
        "home_final_score": game.home_final_score,
        "away_is_winner": game.away_is_winner,
        "home_is_winner": game.home_is_winner,
        "status_detail": game.status_detail,
        "final_period": game.expected_final_period,
    }
    for field, value in expected.items():
        observed = getattr(timing, field)
        if observed != value:
            raise ValueError(
                f"Scoreboard/summary mismatch for {game.game_id} {field}: "
                f"{value!r} != {observed!r}"
            )


def assign_phase(timestamp: datetime, timing: GameTiming) -> PhaseAssignment:
    """Apply literal half-open phases and the inclusive 30-second sensitivity."""

    if timestamp.tzinfo is None:
        raise ValueError("Trade timestamp must be timezone-aware")
    t = timestamp.astimezone(timezone.utc)
    if t < timing.actual_start_utc:
        phase = "pregame"
    elif t < timing.period_2_start_utc:
        phase = "quarter_1"
    elif t < timing.period_3_start_utc:
        phase = "quarter_2"
    elif t < timing.period_4_start_utc:
        phase = "quarter_3"
    elif t <= timing.actual_end_utc:
        phase = "quarter_4_plus"
    else:
        phase = "post_final"
    near = any(
        abs((t - boundary).total_seconds()) <= 30
        for boundary in (
            timing.actual_start_utc,
            timing.period_2_start_utc,
            timing.period_3_start_utc,
            timing.period_4_start_utc,
            timing.actual_end_utc,
        )
    )
    return PhaseAssignment(
        phase=phase,
        analysis_eligible=phase != "post_final",
        exclude_within_30s=near,
    )


class EspnNflClient:
    """Read-through JSON cache for ESPN scoreboard and summary responses."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        timeout: float = 30.0,
        session: requests.Session | None = None,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        success_pause_seconds: float = 0.25,
        jitter_seconds: float = 0.1,
    ) -> None:
        if (
            timeout <= 0
            or max_attempts < 1
            or backoff_seconds < 0
            or success_pause_seconds < 0
            or jitter_seconds < 0
        ):
            raise ValueError("Invalid NFL client retry configuration")
        self.cache_dir = Path(cache_dir).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.session = session or requests.Session()
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.success_pause_seconds = success_pause_seconds
        self.jitter_seconds = jitter_seconds

    def _get(self, path: Path, url: str, params: Mapping[str, Any], refresh: bool) -> Mapping[str, Any]:
        if path.is_file() and not refresh:
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            response = None
            last_error: requests.RequestException | None = None
            for attempt in range(self.max_attempts):
                retry_after = 0.0
                try:
                    response = self.session.get(url, params=dict(params), timeout=self.timeout)
                    status_code = getattr(response, "status_code", 200)
                    if status_code in RETRYABLE_STATUS_CODES:
                        raw_retry_after = getattr(response, "headers", {}).get("Retry-After")
                        try:
                            parsed_retry_after = float(raw_retry_after)
                        except (TypeError, ValueError):
                            parsed_retry_after = 0.0
                        if math.isfinite(parsed_retry_after) and parsed_retry_after >= 0:
                            retry_after = parsed_retry_after
                        raise requests.HTTPError(
                            f"Retryable ESPN response {status_code}", response=response
                        )
                    response.raise_for_status()
                    last_error = None
                    break
                except requests.RequestException as exc:
                    last_error = exc
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    if status is not None and status not in RETRYABLE_STATUS_CODES:
                        raise
                    if attempt + 1 < self.max_attempts:
                        delay = max(self.backoff_seconds * (2**attempt), retry_after)
                        delay += random.uniform(0, self.jitter_seconds)
                        if delay:
                            time.sleep(delay)
            if response is None or (last_error is not None and attempt + 1 == self.max_attempts):
                assert last_error is not None
                raise last_error
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise ValueError("ESPN response must be a JSON object")
            payload_bytes = (
                json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            if path.exists():
                if not path.is_file() or path.read_bytes() != payload_bytes:
                    raise ValueError(
                        f"Immutable ESPN cache collision for {path}; use a fresh cache directory"
                    )
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                fd, temporary = tempfile.mkstemp(
                    prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
                )
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(payload_bytes)
                    try:
                        os.link(temporary, path)
                    except FileExistsError:
                        if not path.is_file() or path.read_bytes() != payload_bytes:
                            raise ValueError(
                                f"Immutable ESPN cache collision for {path}; use a fresh cache directory"
                            )
                finally:
                    try:
                        os.unlink(temporary)
                    except FileNotFoundError:
                        pass
            pause = self.success_pause_seconds + random.uniform(0, self.jitter_seconds)
            if pause:
                time.sleep(pause)
        if not isinstance(payload, Mapping):
            raise ValueError(f"Cached ESPN response is not an object: {path}")
        return payload

    def scoreboard(self, game_date: date, *, refresh: bool = False) -> Mapping[str, Any]:
        key = game_date.strftime("%Y%m%d")
        return self._get(
            self.cache_dir / f"scoreboard_{key}.json",
            ESPN_SCOREBOARD_URL,
            {"dates": key, "limit": 100},
            refresh,
        )

    def scoreboard_cache_path(self, game_date: date) -> Path:
        return self.cache_dir / f"scoreboard_{game_date.strftime('%Y%m%d')}.json"

    def summary(self, game_id: str, *, refresh: bool = False) -> Mapping[str, Any]:
        if not game_id.isdigit():
            raise ValueError(f"Unsafe ESPN NFL game id: {game_id!r}")
        return self._get(
            self.cache_dir / f"game_{game_id}.json",
            ESPN_SUMMARY_URL,
            {"event": game_id},
            refresh,
        )

    def summary_cache_path(self, game_id: str) -> Path:
        if not game_id.isdigit():
            raise ValueError(f"Unsafe ESPN NFL game id: {game_id!r}")
        return self.cache_dir / f"game_{game_id}.json"
