"""Official NBA schedule/LiveData client and strict wall-clock parsers.

Historical schedules come from ``data.nba.com``.  Play boundaries come from
the NBA LiveData S3 origin because the public CDN returns HTTP 403 from the
project EC2 environment.  The parser requires ``timeActual`` on every action;
it never converts game clock to wall clock or substitutes scheduled tip time.
"""
from __future__ import annotations

import json
import hashlib
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

import requests


NBA_SCHEDULE_URL = (
    "https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/"
    "{season}/league/00_full_schedule.json"
)
NBA_LIVE_DATA_ORIGIN = (
    "https://nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData"
)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
GAME_ID_PATTERN = re.compile(r"^[0-9]{10}$")
PHASES = ("quarter_1", "quarter_2", "quarter_3", "quarter_4_plus")
ACTUAL_START_EVENT = (
    "first audited opening-tip action immediately after Q1 period/start and exact "
    "0-0 12:00 delay-of-game violations"
)
ACTUAL_END_EVENT = "unique official final-period period/end action at 00:00"
PERIOD_BOUNDARY_EVENT = "unique official period/start action at regulation clock"
PROVENANCE_SCHEMA_VERSION = 1
OPENING_SIGNATURES = {
    ("jumpball", "recovered", "startperiod"),
    ("jumpball", "recovered", "outofbounds"),
    ("jumpball", "recovered", "heldball"),
    ("jumpball", "recovered", "unclearpass"),
    ("violation", "jumpball", ""),
}


@dataclass(frozen=True)
class ScheduleGame:
    """Stable official game identity and final result from an NBA schedule."""

    game_id: str
    official_date: date
    scheduled_start_utc: datetime
    season_start: int
    game_type_code: str
    away_team_id: int
    away_team_name: str
    away_team_tricode: str
    home_team_id: int
    home_team_name: str
    home_team_tricode: str
    status_text: str
    is_completed: bool
    expected_final_period: int | None
    postponement_status: str | None
    postponement_reason: str | None
    away_final_score: int | None
    home_final_score: int | None
    winner_team_id: int | None = None
    away_is_winner: bool | None = None
    home_is_winner: bool | None = None


@dataclass(frozen=True)
class PeriodBoundary:
    """Observed NBA LiveData start/end timestamps for one period."""

    period: int
    start_utc: datetime
    end_utc: datetime
    start_action_number: int
    end_action_number: int
    start_order_number: int
    end_order_number: int


@dataclass(frozen=True)
class PhaseWindow:
    phase: str
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class GameTiming:
    """Observed four-quarter-plus timing; overtime is folded into Q4+."""

    game_id: str
    actual_start_utc: datetime
    actual_end_utc: datetime
    actual_start_action_number: int
    actual_start_order_number: int
    actual_start_event: str
    actual_end_action_number: int
    actual_end_order_number: int
    actual_end_event: str
    period_boundary_event: str
    final_period: int
    pbp_away_final_score: int
    pbp_home_final_score: int
    action_count: int
    periods: tuple[PeriodBoundary, ...]
    phase_windows: tuple[PhaseWindow, ...]


@dataclass(frozen=True)
class _ObservedAction:
    action_number: int
    order_number: int
    period: int
    timestamp: datetime
    clock: str
    action_type: str
    subtype: str
    descriptor: str
    description: str
    raw_score_away: Any
    raw_score_home: Any


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"Missing or invalid {field}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc


def _parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing or invalid {field}")
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp {field} lacks a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _required_int(value: Any, field: str) -> int:
    if value is None or isinstance(value, bool):
        raise ValueError(f"Missing or invalid {field}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Missing or invalid {field}") from exc


def _required_score(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Missing or invalid {field}")
    if isinstance(value, int):
        score = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        score = int(value.strip())
    else:
        raise ValueError(f"Missing or invalid {field}")
    if score < 0:
        raise ValueError(f"Missing or invalid {field}")
    return score


def _optional_score(value: Any, field: str) -> int | None:
    if value in (None, ""):
        return None
    return _required_score(value, field)


def nba_season_start_for_date(value: date) -> int:
    """Return the exact season directory for an NBA local game date.

    NBA historical schedule directories turn over on July 1.  Iterating the
    required first/last values avoids silently fetching adjacent seasons.
    """

    return value.year if value.month >= 7 else value.year - 1


def _final_status(status: str) -> tuple[bool, int | None]:
    normalized = status.strip().casefold()
    if not normalized.startswith("final"):
        return False, None
    match = re.fullmatch(r"final(?:/(\d*)ot)?", normalized)
    if match is None:
        raise ValueError(f"Unsupported final NBA schedule status: {status!r}")
    overtime = match.group(1)
    if "/" not in normalized:
        return True, None
    overtime_count = int(overtime) if overtime else 1
    if overtime_count < 1:
        raise ValueError(f"Invalid overtime count in NBA schedule status: {status!r}")
    return True, 4 + overtime_count


def _legacy_start_utc(game: Mapping[str, Any]) -> datetime:
    millis = game.get("utcMillis")
    if millis not in (None, ""):
        try:
            number = int(millis)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid schedule utcMillis: {millis!r}") from exc
        return datetime.fromtimestamp(number / 1000, tz=timezone.utc)
    day = game.get("gdtutc")
    clock = game.get("utctm")
    if not isinstance(day, str) or not isinstance(clock, str):
        raise ValueError("Schedule game lacks utcMillis or gdtutc/utctm")
    return _parse_utc(f"{day}T{clock}:00Z", "schedule UTC start")


def _team(game: Mapping[str, Any], key: str) -> tuple[int, str, str, int | None]:
    value = game.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"Schedule game field {key!r} must be an object")
    team_id = _required_int(value.get("tid"), f"{key}.tid")
    tricode = value.get("ta")
    if not isinstance(tricode, str) or not tricode.strip():
        raise ValueError(f"Missing or invalid {key}.ta")
    city = value.get("tc")
    nickname = value.get("tn")
    parts = [part.strip() for part in (city, nickname) if isinstance(part, str) and part.strip()]
    if not parts:
        raise ValueError(f"Missing or invalid {key} team name")
    return team_id, " ".join(parts), tricode.strip().upper(), _optional_score(
        value.get("s"), f"{key}.s"
    )


def parse_legacy_schedule(
    payload: Mapping[str, Any], season_start: int
) -> tuple[ScheduleGame, ...]:
    """Parse the NBA's historical mobile season schedule without guessing."""

    months = payload.get("lscd")
    if not isinstance(months, list):
        raise ValueError("NBA historical schedule field 'lscd' must be a list")
    games: list[ScheduleGame] = []
    for month in months:
        if not isinstance(month, Mapping):
            raise ValueError("NBA schedule month entries must be objects")
        container = month.get("mscd")
        if not isinstance(container, Mapping) or not isinstance(container.get("g"), list):
            raise ValueError("NBA schedule month must contain mscd.g list")
        for raw in container["g"]:
            if not isinstance(raw, Mapping):
                raise ValueError("NBA schedule game entries must be objects")
            game_id = str(raw.get("gid", ""))
            if not GAME_ID_PATTERN.fullmatch(game_id):
                raise ValueError(f"Invalid NBA game id: {game_id!r}")
            away_id, away_name, away_code, away_score = _team(raw, "v")
            home_id, home_name, home_code, home_score = _team(raw, "h")
            status = str(raw.get("stt") or raw.get("st") or "").strip()
            is_completed, expected_final_period = _final_status(status)
            if is_completed:
                if away_score is None or home_score is None:
                    raise ValueError(f"Final NBA game {game_id} lacks final scores")
                if away_score == home_score:
                    raise ValueError(f"Final NBA game {game_id} has tied final scores")
                winner_id = away_id if away_score > home_score else home_id
            else:
                winner_id = None
            games.append(
                ScheduleGame(
                    game_id=game_id,
                    official_date=_parse_date(raw.get("gdte"), "gdte"),
                    scheduled_start_utc=_legacy_start_utc(raw),
                    season_start=int(season_start),
                    game_type_code=game_id[:3],
                    away_team_id=away_id,
                    away_team_name=away_name,
                    away_team_tricode=away_code,
                    home_team_id=home_id,
                    home_team_name=home_name,
                    home_team_tricode=home_code,
                    status_text=status,
                    is_completed=is_completed,
                    expected_final_period=expected_final_period,
                    postponement_status=(
                        str(raw["ppdst"]).strip() if raw.get("ppdst") not in (None, "") else None
                    ),
                    postponement_reason=(
                        str(raw["ppd"]).strip() if raw.get("ppd") not in (None, "") else None
                    ),
                    away_final_score=away_score if is_completed else None,
                    home_final_score=home_score if is_completed else None,
                    winner_team_id=winner_id,
                    away_is_winner=(winner_id == away_id) if winner_id else None,
                    home_is_winner=(winner_id == home_id) if winner_id else None,
                )
            )
    result = tuple(sorted(games, key=lambda row: (row.official_date, row.game_id)))
    seen: set[str] = set()
    for game in result:
        if nba_season_start_for_date(game.official_date) != season_start:
            raise ValueError(
                f"NBA schedule {season_start} contains out-of-season game "
                f"{game.game_id} on {game.official_date}"
            )
        if game.game_id in seen:
            raise ValueError(f"NBA schedule contains duplicate game id: {game.game_id}")
        seen.add(game.game_id)
    return result


def winning_team_id(game: ScheduleGame) -> int:
    if not game.is_completed:
        raise ValueError(f"NBA game {game.game_id} is not final")
    if game.away_final_score is None or game.home_final_score is None:
        raise ValueError(f"NBA game {game.game_id} lacks final scores")
    away_score = _required_score(game.away_final_score, "away final score")
    home_score = _required_score(game.home_final_score, "home final score")
    if away_score == home_score:
        raise ValueError(f"NBA game {game.game_id} has tied final scores")
    score_winner = (
        game.away_team_id
        if away_score > home_score
        else game.home_team_id
    )
    if game.winner_team_id != score_winner:
        raise ValueError(f"NBA game {game.game_id} has inconsistent canonical winner")
    if (game.away_is_winner, game.home_is_winner) not in {
        (True, False),
        (False, True),
    }:
        raise ValueError(f"NBA game {game.game_id} lacks exactly one winner flag")
    flag_winner = game.away_team_id if game.away_is_winner else game.home_team_id
    if flag_winner != score_winner:
        raise ValueError(f"NBA game {game.game_id} has inconsistent winner flags")
    return score_winner


def _clock(value: Any, expected: str, label: str) -> None:
    if value != expected:
        raise ValueError(f"{label} must have clock {expected}, got {value!r}")


def _clock_seconds(value: str, label: str) -> float:
    match = re.fullmatch(r"PT([0-9]+)M([0-9]+(?:\.[0-9]+)?)S", value)
    if match is None:
        raise ValueError(f"{label} has invalid clock {value!r}")
    seconds = float(match.group(2))
    if seconds >= 60:
        raise ValueError(f"{label} has invalid clock {value!r}")
    return 60 * int(match.group(1)) + seconds


def _scores(action: _ObservedAction, label: str) -> tuple[int, int]:
    return (
        _required_score(action.raw_score_away, f"{label}.scoreAway"),
        _required_score(action.raw_score_home, f"{label}.scoreHome"),
    )


def _is_opening_admin(action: _ObservedAction) -> bool:
    return (
        action.period == 1
        and action.clock == "PT12M00.00S"
        and action.action_type == "violation"
        and action.subtype == "delay-of-game"
        and action.descriptor == ""
        and action.description == "TEAM delay-of-game VIOLATION"
        and _scores(action, "opening administration") == (0, 0)
    )


def _has_opening_description(action: _ObservedAction) -> bool:
    signature = (action.action_type, action.subtype, action.descriptor)
    if signature == ("violation", "jumpball", ""):
        return bool(re.fullmatch(r".+ jumpball VIOLATION", action.description))
    return action.description.startswith("Jump Ball ")


def parse_live_data_play_by_play(
    payload: Mapping[str, Any], *, expected_game_id: str | None = None
) -> GameTiming:
    """Parse exact phase boundaries from official NBA LiveData actions.

    Every action must have a timezone-aware ``timeActual``. ``actual_start``
    is the first audited opening-tip representation immediately after the Q1
    period start and any exact delay-of-game administration. ``actual_end`` is
    the unique official ``period/end`` action in the final observed period.
    Each period must have exactly one start and end action. Four regulation
    periods are mandatory; later periods remain inside Q4+.
    """

    game = payload.get("game")
    if not isinstance(game, Mapping):
        raise ValueError("NBA LiveData field 'game' must be an object")
    game_id = str(game.get("gameId", ""))
    if not GAME_ID_PATTERN.fullmatch(game_id):
        raise ValueError(f"Invalid NBA LiveData game id: {game_id!r}")
    if expected_game_id is not None and game_id != expected_game_id:
        raise ValueError(
            f"Requested NBA game {expected_game_id}, but LiveData returned {game_id}"
        )
    actions = game.get("actions")
    if not isinstance(actions, list) or not actions:
        raise ValueError(f"NBA game {game_id} has no LiveData actions")

    observed: list[_ObservedAction] = []
    for action in actions:
        if not isinstance(action, Mapping):
            raise ValueError("NBA LiveData actions must be objects")
        action_number = _required_int(action.get("actionNumber"), "actionNumber")
        order_number = _required_int(action.get("orderNumber"), "orderNumber")
        period = _required_int(action.get("period"), "period")
        if period < 1:
            raise ValueError(f"NBA action has invalid period {period}")
        timestamp = _parse_utc(action.get("timeActual"), "action.timeActual")
        clock = action.get("clock")
        action_type = action.get("actionType")
        subtype = action.get("subType", "")
        descriptor = action.get("descriptor", "")
        description = action.get("description", "")
        if not isinstance(clock, str) or not isinstance(action_type, str) or not isinstance(subtype, str):
            raise ValueError("NBA LiveData clock/actionType/subType fields must be strings")
        if descriptor is None:
            descriptor = ""
        if not isinstance(descriptor, str):
            raise ValueError("NBA LiveData descriptor must be a string or null")
        if not isinstance(description, str):
            description = ""
        observed.append(
            _ObservedAction(
                action_number=action_number,
                order_number=order_number,
                period=period,
                timestamp=timestamp,
                clock=clock.strip().upper(),
                action_type=action_type.strip().casefold(),
                subtype=subtype.strip().casefold(),
                descriptor=descriptor.strip().casefold(),
                description=description.strip(),
                raw_score_away=action.get("scoreAway"),
                raw_score_home=action.get("scoreHome"),
            )
        )

    order_numbers = [row.order_number for row in observed]
    if len(set(order_numbers)) != len(order_numbers) or order_numbers != sorted(order_numbers):
        raise ValueError("NBA LiveData orderNumber values must be unique and ordered")
    action_numbers = [row.action_number for row in observed]
    if len(set(action_numbers)) != len(action_numbers):
        raise ValueError("NBA LiveData actionNumber values must be unique")
    periods = sorted({row.period for row in observed})
    if periods != list(range(1, max(periods) + 1)) or max(periods) < 4:
        raise ValueError(
            f"NBA game {game_id} must contain contiguous periods 1 through at least 4"
        )

    boundaries: list[PeriodBoundary] = []
    period_starts: dict[int, _ObservedAction] = {}
    period_ends: dict[int, _ObservedAction] = {}
    for period in periods:
        starts = [
            row for row in observed
            if row.period == period
            and row.action_type == "period"
            and row.subtype == "start"
        ]
        ends = [
            row for row in observed
            if row.period == period
            and row.action_type == "period"
            and row.subtype == "end"
        ]
        if len(starts) != 1 or len(ends) != 1:
            raise ValueError(
                f"NBA game {game_id} period {period} requires exactly one start and end"
            )
        start, end = starts[0], ends[0]
        period_starts[period] = start
        period_ends[period] = end
        _clock(start.clock, "PT12M00.00S" if period <= 4 else "PT05M00.00S", f"period {period} start")
        _clock(end.clock, "PT00M00.00S", f"period {period} end")
        if end.timestamp <= start.timestamp:
            raise ValueError(f"NBA game {game_id} period {period} ends before it starts")
        boundaries.append(
            PeriodBoundary(
                period,
                start.timestamp,
                end.timestamp,
                start.action_number,
                end.action_number,
                start.order_number,
                end.order_number,
            )
        )

    if any(right.start_utc <= left.end_utc for left, right in zip(boundaries, boundaries[1:])):
        raise ValueError(f"NBA game {game_id} period boundaries overlap or reorder")
    by_period = {row.period: row for row in boundaries}
    start_index = observed.index(period_starts[1])
    opening_index = start_index + 1
    while opening_index < len(observed) and _is_opening_admin(observed[opening_index]):
        opening_index += 1
    if opening_index >= len(observed):
        raise ValueError(
            f"NBA game {game_id} lacks an action after its Q1 period start"
        )
    opening_tip = observed[opening_index]
    signature = (
        opening_tip.action_type, opening_tip.subtype, opening_tip.descriptor
    )
    opening_seconds = _clock_seconds(opening_tip.clock, "opening-tip action")
    if (
        opening_tip.period != 1
        or not 11 * 60 + 45 <= opening_seconds <= 12 * 60
        or _scores(opening_tip, "opening-tip action") != (0, 0)
        or signature not in OPENING_SIGNATURES
        or not _has_opening_description(opening_tip)
        or not (
            by_period[1].start_order_number < opening_tip.order_number
            < by_period[1].end_order_number
        )
    ):
        raise ValueError(
            f"NBA game {game_id} first post-start action is not an audited opening-tip"
        )
    if opening_tip.timestamp < by_period[1].start_utc or opening_tip.timestamp >= by_period[1].end_utc:
        raise ValueError(f"NBA game {game_id} opening-tip timestamp is outside period 1")
    final_period = periods[-1]
    terminal = period_ends[final_period]
    game_ends = [
        row for row in observed
        if row.action_type == "game" and row.subtype == "end"
    ]
    if len(game_ends) != 1:
        raise ValueError(f"NBA game {game_id} requires exactly one game/end action")
    game_end = game_ends[0]
    _clock(game_end.clock, "PT00M00.00S", "game/end action")
    if game_end.period != final_period:
        raise ValueError(f"NBA game {game_id} game/end is not in its final period")
    if game_end.order_number <= terminal.order_number or game_end.timestamp < terminal.timestamp:
        raise ValueError(f"NBA game {game_id} game/end precedes its final period/end")
    final_scores = _scores(terminal, "final period/end")
    if _scores(game_end, "game/end action") != final_scores:
        raise ValueError(f"NBA game {game_id} terminal scores disagree")
    for period in range(4, final_period):
        away_score, home_score = _scores(period_ends[period], f"period {period} end")
        if away_score != home_score:
            raise ValueError(
                f"NBA game {game_id} has a decisive period {period} before later play"
            )
    if final_scores[0] == final_scores[1]:
        raise ValueError(f"NBA game {game_id} has tied final PBP scores")
    actual_start = opening_tip.timestamp
    actual_end = terminal.timestamp
    starts = [actual_start, *(by_period[index].start_utc for index in range(2, 5))]
    windows = tuple(
        PhaseWindow(
            PHASES[index],
            starts[index],
            starts[index + 1] if index < 3 else actual_end,
        )
        for index in range(4)
    )
    return GameTiming(
        game_id=game_id,
        actual_start_utc=actual_start,
        actual_end_utc=actual_end,
        actual_start_action_number=opening_tip.action_number,
        actual_start_order_number=opening_tip.order_number,
        actual_start_event=ACTUAL_START_EVENT,
        actual_end_action_number=terminal.action_number,
        actual_end_order_number=terminal.order_number,
        actual_end_event=ACTUAL_END_EVENT,
        period_boundary_event=PERIOD_BOUNDARY_EVENT,
        final_period=final_period,
        pbp_away_final_score=final_scores[0],
        pbp_home_final_score=final_scores[1],
        action_count=len(actions),
        periods=tuple(boundaries),
        phase_windows=windows,
    )


def validate_schedule_timing(game: ScheduleGame, timing: GameTiming) -> None:
    """Fail unless schedule result and observed PBP terminal evidence agree."""

    if game.game_id != timing.game_id:
        raise ValueError(
            f"NBA schedule game {game.game_id} does not match timing {timing.game_id}"
        )
    schedule_winner = winning_team_id(game)
    if game.expected_final_period is None:
        if game.status_text.strip().casefold() != "final":
            raise ValueError(f"NBA game {game.game_id} lacks a valid final status")
    elif timing.final_period != game.expected_final_period:
        raise ValueError(
            f"NBA game {game.game_id} schedule expects final period "
            f"{game.expected_final_period}, but PBP ends in period {timing.final_period}"
        )
    schedule_scores = (game.away_final_score, game.home_final_score)
    pbp_scores = (timing.pbp_away_final_score, timing.pbp_home_final_score)
    if pbp_scores != schedule_scores:
        raise ValueError(
            f"NBA game {game.game_id} schedule/PBP final scores disagree: "
            f"schedule={schedule_scores}, PBP={pbp_scores}"
        )
    pbp_winner = (
        game.away_team_id
        if timing.pbp_away_final_score > timing.pbp_home_final_score
        else game.home_team_id
    )
    if pbp_winner != schedule_winner:
        raise ValueError(f"NBA game {game.game_id} schedule/PBP winners disagree")
    if timing.actual_start_event != ACTUAL_START_EVENT:
        raise ValueError(f"NBA game {game.game_id} has wrong actual-start semantics")
    if timing.actual_end_event != ACTUAL_END_EVENT:
        raise ValueError(f"NBA game {game.game_id} has wrong actual-end semantics")
    if timing.period_boundary_event != PERIOD_BOUNDARY_EVENT:
        raise ValueError(f"NBA game {game.game_id} has wrong period-boundary semantics")


def classify_timestamp(timing: GameTiming, value: datetime) -> str:
    """Classify an exact trade timestamp using half-open literal phases."""

    if value.tzinfo is None:
        raise ValueError("Trade timestamp must include a timezone")
    timestamp = value.astimezone(timezone.utc)
    if timestamp < timing.actual_start_utc:
        return "pregame"
    if timestamp > timing.actual_end_utc:
        return "post_final"
    for window in timing.phase_windows:
        if window.start_utc <= timestamp < window.end_utc:
            return window.phase
    if timestamp == timing.actual_end_utc:
        return "quarter_4_plus"
    raise ValueError("Timestamp falls inside a game but outside phase windows")


def is_boundary_sensitive(
    timing: GameTiming, value: datetime, *, seconds: int = 30
) -> bool:
    """Return whether a timestamp is inclusively within a study boundary."""

    if seconds < 0:
        raise ValueError("Boundary sensitivity seconds cannot be negative")
    if value.tzinfo is None:
        raise ValueError("Trade timestamp must include a timezone")
    timestamp = value.astimezone(timezone.utc)
    boundaries = (
        timing.actual_start_utc,
        *(window.start_utc for window in timing.phase_windows[1:]),
        timing.actual_end_utc,
    )
    return any(abs((timestamp - boundary).total_seconds()) <= seconds for boundary in boundaries)


class NbaApiClient:
    """Read-through cache for official historical schedule and LiveData PBP."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 30.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
        success_pause_seconds: float = 0.25,
        jitter_seconds: float = 0.1,
    ) -> None:
        if (
            timeout_seconds <= 0
            or max_attempts < 1
            or backoff_seconds < 0
            or success_pause_seconds < 0
            or jitter_seconds < 0
        ):
            raise ValueError("Invalid NBA client retry configuration")
        self.cache_dir = Path(cache_dir)
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.success_pause_seconds = success_pause_seconds
        self.jitter_seconds = jitter_seconds
        self._resources: dict[str, dict[str, Any]] = {}

    def _decode_and_record(
        self, payload_bytes: bytes, cache_path: Path, url: str, source: str
    ) -> Mapping[str, Any]:
        self._resources[url] = {
            "url": url,
            "cache_path": str(cache_path.expanduser().resolve()),
            "bytes": len(payload_bytes),
            "sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "source": source,
        }
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Invalid NBA JSON response for {url}: {exc}") from exc
        if not isinstance(payload, Mapping):
            raise ValueError("NBA response must be a JSON object")
        return payload

    def _read_or_fetch(self, cache_path: Path, url: str, refresh: bool) -> Mapping[str, Any]:
        if cache_path.exists() and not refresh:
            return self._decode_and_record(cache_path.read_bytes(), cache_path, url, "cache")
        last_error: Exception | None = None
        headers = {"User-Agent": "prediction-markets-research/1.0", "Accept": "application/json"}
        for attempt in range(self.max_attempts):
            retry_after = 0.0
            try:
                response = self.session.get(url, timeout=self.timeout_seconds, headers=headers)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raw_retry_after = getattr(response, "headers", {}).get("Retry-After")
                    try:
                        parsed_retry_after = float(raw_retry_after)
                    except (TypeError, ValueError):
                        parsed_retry_after = 0.0
                    if math.isfinite(parsed_retry_after) and parsed_retry_after >= 0:
                        retry_after = parsed_retry_after
                    raise requests.HTTPError(
                        f"Retryable NBA response {response.status_code}", response=response
                    )
                response.raise_for_status()
                payload_bytes = bytes(response.content)
                payload = self._decode_and_record(
                    payload_bytes,
                    cache_path,
                    url,
                    "network_verified_identical" if cache_path.exists() else "network",
                )
                if cache_path.exists():
                    if cache_path.read_bytes() != payload_bytes:
                        raise ValueError(
                            f"Immutable NBA cache collision for {cache_path}; "
                            "use a fresh cache directory"
                        )
                else:
                    self._write_cache(cache_path, payload_bytes)
                pause = self.success_pause_seconds + random.uniform(0, self.jitter_seconds)
                if pause:
                    time.sleep(pause)
                return payload
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
        assert last_error is not None
        raise last_error

    @staticmethod
    def _write_cache(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def schedule_games(
        self, start: date, end: date, *, refresh: bool = False
    ) -> tuple[ScheduleGame, ...]:
        if end < start:
            raise ValueError("NBA schedule end date precedes start date")
        first_season = nba_season_start_for_date(start)
        last_season = nba_season_start_for_date(end)
        seasons = range(first_season, last_season + 1)
        rows: list[ScheduleGame] = []
        for season in seasons:
            payload = self._read_or_fetch(
                self.cache_dir / f"schedule_{season}.json",
                NBA_SCHEDULE_URL.format(season=season),
                refresh,
            )
            rows.extend(parse_legacy_schedule(payload, season))
        by_id: dict[str, ScheduleGame] = {}
        for row in rows:
            if not (start <= row.official_date <= end):
                continue
            previous = by_id.get(row.game_id)
            if previous is not None and previous != row:
                raise ValueError(f"Conflicting official schedule records for {row.game_id}")
            by_id[row.game_id] = row
        return tuple(sorted(by_id.values(), key=lambda row: (row.official_date, row.game_id)))

    def provenance_manifest(self) -> dict[str, Any]:
        """Return immutable content fingerprints and frozen event semantics."""

        return {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "schedule_provider": "official_nba_data_nba_com_historical_schedule",
            "timing_provider": "official_nba_livedata_s3_origin",
            "actual_start_event": ACTUAL_START_EVENT,
            "actual_end_event": ACTUAL_END_EVENT,
            "period_boundary_event": PERIOD_BOUNDARY_EVENT,
            "resources": [self._resources[key] for key in sorted(self._resources)],
        }

    def game_timing(self, game_id: str, *, refresh: bool = False) -> GameTiming:
        if not GAME_ID_PATTERN.fullmatch(game_id):
            raise ValueError(f"Invalid NBA game id: {game_id!r}")
        url = f"{NBA_LIVE_DATA_ORIGIN}/playbyplay/playbyplay_{game_id}.json"
        payload = self._read_or_fetch(
            self.cache_dir / "playbyplay" / f"{game_id}.json", url, refresh
        )
        return parse_live_data_play_by_play(payload, expected_game_id=game_id)
