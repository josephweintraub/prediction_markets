"""Small MLB Stats API client and pure schedule/live-feed parsers.

The client returns schedule-final games by default, which is not itself an
analysis-eligibility decision.  Callers may request non-final entries when
building a match audit.  Postponement, suspension, resumption, doubleheader,
and shortened-game metadata remain visible so irregular games can be excluded
or audited under a separately frozen rule.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests


MLB_STATS_API_BASE = "https://statsapi.mlb.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
VALID_HALVES = {"top", "bottom"}
SCHEDULE_MAX_DAYS_PER_REQUEST = 180
ZERO_PITCH_RUNNER_OUT_EVENTS = {
    "caught_stealing_2b",
    "pickoff_1b",
    "pickoff_caught_stealing_2b",
}


@dataclass(frozen=True)
class ScheduleGame:
    """Stable game identity plus schedule-state metadata used for auditing.

    ``is_completed`` describes only the schedule state.  A completed game with
    suspension, resumption, rescheduling, doubleheader, or shortened-game
    metadata is not automatically analysis-eligible; callers must retain it in
    the match audit until an irregular-game rule is frozen.
    """

    game_pk: int
    official_date: date
    scheduled_start_utc: datetime
    game_type: str | None
    season: int | None
    away_team_id: int
    away_team_name: str
    home_team_id: int
    home_team_name: str
    status_abstract: str
    status_detailed: str
    status_code: str | None
    is_completed: bool
    doubleheader: str | None
    game_number: int | None
    series_game_number: int | None
    reschedule_date_utc: datetime | None
    rescheduled_from_date: date | None
    resume_date_utc: datetime | None
    resumed_from_date: date | None
    away_final_score: int | None = None
    home_final_score: int | None = None
    away_is_winner: bool | None = None
    home_is_winner: bool | None = None


@dataclass(frozen=True)
class InningHalfBoundary:
    """Observed start/end of one inning half from completed plate appearances."""

    inning: int
    half: str
    phase: str
    start_utc: datetime
    end_utc: datetime
    first_at_bat_index: int
    last_at_bat_index: int


@dataclass(frozen=True)
class PhaseWindow:
    """Continuous clock interval for an inning group."""

    phase: str
    start_utc: datetime
    end_utc: datetime


@dataclass(frozen=True)
class GameTiming:
    """Actual game-time boundaries derived only from completed live-feed plays."""

    game_pk: int
    actual_start_utc: datetime
    actual_end_utc: datetime
    play_count: int
    inning_halves: tuple[InningHalfBoundary, ...]
    phase_windows: tuple[PhaseWindow, ...]


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"Missing or invalid {field}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc


def _parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"Missing or invalid {field}")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp {field} lacks a timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _optional_date(value: Any, field: str) -> date | None:
    return None if value in (None, "") else _parse_date(value, field)


def _optional_utc(value: Any, field: str) -> datetime | None:
    return None if value in (None, "") else _parse_utc(value, field)


def _required_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Missing or invalid {field}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Missing or invalid {field}") from exc


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _team(
    game: Mapping[str, Any], side: str
) -> tuple[int, str, int | None, bool | None]:
    team = game.get("teams", {}).get(side, {}).get("team", {})
    team_id = _required_int(team.get("id"), f"teams.{side}.team.id")
    name = team.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Missing or invalid teams.{side}.team.name")
    side_record = game.get("teams", {}).get(side, {})
    score = _optional_int(side_record.get("score"))
    winner = side_record.get("isWinner")
    if not isinstance(winner, bool):
        winner = None
    return team_id, name.strip(), score, winner


def parse_schedule(payload: Mapping[str, Any]) -> tuple[ScheduleGame, ...]:
    """Parse every schedule entry, retaining non-final records for audit.

    A game's ``is_completed`` flag is deliberately strict: the MLB API must
    label its abstract state ``Final`` and must not label its detailed state
    Postponed, Suspended, or Cancelled.  MLB sometimes uses abstract ``Final``
    for those schedule-history records.
    """

    parsed: list[ScheduleGame] = []
    dates = payload.get("dates", [])
    if not isinstance(dates, list):
        raise ValueError("Schedule payload field 'dates' must be a list")

    for day in dates:
        if not isinstance(day, Mapping):
            raise ValueError("Schedule date entries must be objects")
        games = day.get("games", [])
        if not isinstance(games, list):
            raise ValueError("Schedule date field 'games' must be a list")
        for game in games:
            if not isinstance(game, Mapping):
                raise ValueError("Schedule game entries must be objects")
            status = game.get("status", {})
            if not isinstance(status, Mapping):
                raise ValueError("Schedule game field 'status' must be an object")
            away_id, away_name, away_score, away_winner = _team(game, "away")
            home_id, home_name, home_score, home_winner = _team(game, "home")
            abstract = str(status.get("abstractGameState", ""))
            detailed = str(status.get("detailedState", ""))
            # MLB can label the abstract state "Final" for records whose
            # detailed state is still Postponed or Cancelled.  Those history
            # rows are audit records, not completed games.
            is_completed = abstract.casefold() == "final" and detailed.casefold() not in {
                "postponed",
                "suspended",
                "cancelled",
            }
            parsed.append(
                ScheduleGame(
                    game_pk=_required_int(game.get("gamePk"), "gamePk"),
                    official_date=_parse_date(
                        game.get("officialDate") or day.get("date"), "officialDate"
                    ),
                    scheduled_start_utc=_parse_utc(game.get("gameDate"), "gameDate"),
                    game_type=(str(game["gameType"]) if game.get("gameType") else None),
                    season=_optional_int(game.get("season")),
                    away_team_id=away_id,
                    away_team_name=away_name,
                    home_team_id=home_id,
                    home_team_name=home_name,
                    status_abstract=abstract,
                    status_detailed=detailed,
                    status_code=(
                        str(status["statusCode"]) if status.get("statusCode") else None
                    ),
                    is_completed=is_completed,
                    doubleheader=(
                        str(game["doubleHeader"]) if game.get("doubleHeader") else None
                    ),
                    game_number=_optional_int(game.get("gameNumber")),
                    series_game_number=_optional_int(game.get("seriesGameNumber")),
                    reschedule_date_utc=_optional_utc(
                        game.get("rescheduleDate"), "rescheduleDate"
                    ),
                    rescheduled_from_date=_optional_date(
                        game.get("rescheduledFromDate"), "rescheduledFromDate"
                    ),
                    resume_date_utc=_optional_utc(game.get("resumeDate"), "resumeDate"),
                    resumed_from_date=_optional_date(
                        game.get("resumedFromDate"), "resumedFromDate"
                    ),
                    away_final_score=away_score if is_completed else None,
                    home_final_score=home_score if is_completed else None,
                    away_is_winner=away_winner if is_completed else None,
                    home_is_winner=home_winner if is_completed else None,
                )
            )

    return tuple(sorted(parsed, key=lambda game: (game.official_date, game.game_pk)))


def _unique_present_value(
    games: tuple[ScheduleGame, ...], field: str
) -> Any | None:
    values: list[Any] = []
    for game in games:
        value = getattr(game, field)
        if value is not None and value not in values:
            values.append(value)
    if len(values) > 1:
        raise ValueError(
            f"MLB schedule history for game {games[0].game_pk} has conflicting {field}"
        )
    return values[0] if values else None


def _reconcile_schedule_history(
    game_pk: int, records: tuple[ScheduleGame, ...]
) -> ScheduleGame:
    """Collapse one linked MLB reschedule/resume history without guessing.

    Long schedule responses can contain the same ``gamePk`` under both its
    original date and its makeup/resumption date.  MLB links those records in
    both directions: the source has ``rescheduleDate``/``resumeDate`` and the
    destination has ``rescheduledFromDate``/``resumedFromDate``.  The linked
    destination is the current played-game record; complementary history
    fields are retained so downstream irregular-game gates still exclude or
    audit it.  Any conflict that is not exactly this shape fails closed.
    """

    unique: list[ScheduleGame] = []
    for record in records:
        if record not in unique:
            unique.append(record)
    if len(unique) == 1:
        return unique[0]
    if len(unique) != 2:
        raise ValueError(
            f"MLB schedule returned {len(unique)} distinct records for game "
            f"{game_pk}; history cannot be reconciled"
        )

    pair = tuple(unique)
    identity_fields = (
        "game_pk",
        "official_date",
        "game_type",
        "season",
        "away_team_id",
        "away_team_name",
        "home_team_id",
        "home_team_name",
    )
    disagreements = [
        field for field in identity_fields if getattr(pair[0], field) != getattr(pair[1], field)
    ]
    if disagreements:
        raise ValueError(
            f"MLB schedule history for game {game_pk} changes stable identity fields: "
            f"{', '.join(disagreements)}"
        )

    relationships: list[tuple[str, ScheduleGame, ScheduleGame]] = []
    for kind, source_field, destination_field in (
        ("reschedule", "reschedule_date_utc", "rescheduled_from_date"),
        ("resume", "resume_date_utc", "resumed_from_date"),
    ):
        sources = [game for game in pair if getattr(game, source_field) is not None]
        destinations = [
            game for game in pair if getattr(game, destination_field) is not None
        ]
        if len(sources) == 1 and len(destinations) == 1:
            source = sources[0]
            destination = destinations[0]
            if source is not destination:
                relationships.append((kind, source, destination))

    if len(relationships) != 1:
        raise ValueError(
            f"MLB schedule returned conflicting records for game {game_pk} without "
            "one unambiguous reschedule/resume link"
        )
    kind, source, destination = relationships[0]
    other_kind_fields = (
        ("resume_date_utc", "resumed_from_date")
        if kind == "reschedule"
        else ("reschedule_date_utc", "rescheduled_from_date")
    )
    if any(getattr(game, field) is not None for game in pair for field in other_kind_fields):
        raise ValueError(
            f"MLB schedule history for game {game_pk} mixes reschedule and resume links"
        )

    target_field = "reschedule_date_utc" if kind == "reschedule" else "resume_date_utc"
    if getattr(source, target_field) != destination.scheduled_start_utc:
        raise ValueError(
            f"MLB schedule {kind} target does not match the destination start for "
            f"game {game_pk}"
        )

    result_fields = (
        "away_final_score",
        "home_final_score",
        "away_is_winner",
        "home_is_winner",
    )
    merged_results = {
        field: _unique_present_value(pair, field) for field in result_fields
    }
    history_fields = (
        "reschedule_date_utc",
        "rescheduled_from_date",
        "resume_date_utc",
        "resumed_from_date",
    )
    merged_history = {
        field: _unique_present_value(pair, field) for field in history_fields
    }
    return replace(destination, **merged_results, **merged_history)


def reconcile_schedule_games(
    games: Iterable[ScheduleGame],
) -> tuple[ScheduleGame, ...]:
    """Return one deterministic record per game, merging only linked history."""

    grouped: dict[int, list[ScheduleGame]] = {}
    for game in games:
        grouped.setdefault(game.game_pk, []).append(game)
    reconciled = (
        _reconcile_schedule_history(game_pk, tuple(records))
        for game_pk, records in grouped.items()
    )
    return tuple(sorted(reconciled, key=lambda game: (game.official_date, game.game_pk)))


def completed_schedule_games(games: Iterable[ScheduleGame]) -> tuple[ScheduleGame, ...]:
    """Return only games the schedule explicitly marks final."""

    return tuple(game for game in games if game.is_completed)


def winning_mlb_team_id(game: ScheduleGame) -> int:
    """Return the official winner only when scores and MLB flags agree consistently."""

    if not game.is_completed:
        raise ValueError(f"MLB game {game.game_pk} is not final")
    if game.away_final_score is None or game.home_final_score is None:
        raise ValueError(f"MLB game {game.game_pk} lacks final scores")
    if game.away_final_score == game.home_final_score:
        raise ValueError(f"MLB game {game.game_pk} has tied final scores")
    flags = (game.away_is_winner, game.home_is_winner)
    if any(flag is None for flag in flags) or sum(flag is True for flag in flags) != 1:
        raise ValueError(f"MLB game {game.game_pk} lacks exactly one official winner flag")

    score_winner = (
        game.away_team_id
        if game.away_final_score > game.home_final_score
        else game.home_team_id
    )
    flag_winner = game.away_team_id if game.away_is_winner else game.home_team_id
    if score_winner != flag_winner:
        raise ValueError(f"MLB game {game.game_pk} has inconsistent score and winner flags")
    return score_winner


def inning_phase(inning: int) -> str:
    """Map an inning to the predeclared early/middle/late study phase."""

    if inning < 1:
        raise ValueError(f"Inning must be positive, got {inning}")
    if inning <= 3:
        return "innings_1_3"
    if inning <= 6:
        return "innings_4_6"
    return "innings_7_plus"


def _zero_pitch_event_envelope(
    play: Mapping[str, Any],
) -> tuple[datetime, datetime] | None:
    """Return an exact child-event envelope for one impossible no-pitch window.

    MLB occasionally puts a synthetic intentional-walk or baserunner-out
    play's ``about.startTime`` after every one of its child events has ended.
    The child events are usable only when they are explicitly zero-pitch,
    complete timestamp pairs with ordered indices and start times.  Any other
    shape keeps the ordinary fail-closed timestamp checks.
    """

    about = play.get("about", {})
    if not isinstance(about, Mapping) or about.get("startTime") in (None, ""):
        return None
    try:
        about_start = _parse_utc(about["startTime"], "play.startTime")
    except (TypeError, ValueError):
        return None

    result = play.get("result", {})
    if not isinstance(result, Mapping):
        return None
    event_type = result.get("eventType")
    if event_type == "intent_walk":
        expected_child_type = "no_pitch"
    elif isinstance(event_type, str) and event_type in ZERO_PITCH_RUNNER_OUT_EVENTS:
        expected_child_type = "pickoff"
    else:
        return None

    events = play.get("playEvents", [])
    if not isinstance(events, list) or not events:
        return None
    observed: list[tuple[int, datetime, datetime]] = []
    for event in events:
        if (
            not isinstance(event, Mapping)
            or event.get("isPitch") is not False
            or event.get("type") != expected_child_type
        ):
            return None
        index = event.get("index")
        if not isinstance(index, int) or isinstance(index, bool):
            return None
        if event.get("startTime") in (None, "") or event.get("endTime") in (None, ""):
            return None
        try:
            start = _parse_utc(event["startTime"], "playEvent.startTime")
            end = _parse_utc(event["endTime"], "playEvent.endTime")
        except (TypeError, ValueError):
            return None
        if end < start:
            return None
        observed.append((index, start, end))

    if [row[0] for row in observed] != list(range(len(observed))):
        return None
    if any(right[1] < left[1] for left, right in zip(observed, observed[1:])):
        return None
    event_end = max(row[2] for row in observed)
    if about_start <= event_end:
        return None
    return observed[0][1], event_end


def _play_timestamp(play: Mapping[str, Any], end: bool) -> datetime:
    event_envelope = _zero_pitch_event_envelope(play)
    if event_envelope is not None:
        return event_envelope[1] if end else event_envelope[0]

    about = play.get("about", {})
    if not isinstance(about, Mapping):
        raise ValueError("Live-feed play field 'about' must be an object")
    field = "endTime" if end else "startTime"
    value = about.get(field)

    # A small number of feeds omit about.endTime but retain event end times.
    # This is still an observed MLB timestamp, not an inferred clock value.
    if value in (None, ""):
        events = play.get("playEvents", [])
        if isinstance(events, list):
            ordered = reversed(events) if end else events
            for event in ordered:
                if isinstance(event, Mapping) and event.get(field):
                    value = event[field]
                    break
    return _parse_utc(value, f"play.{field}")


def _build_phase_windows(
    halves: tuple[InningHalfBoundary, ...], actual_end: datetime
) -> tuple[PhaseWindow, ...]:
    by_key = {(boundary.inning, boundary.half): boundary for boundary in halves}
    final_inning = max(boundary.inning for boundary in halves)
    required_starts = [(1, "innings_1_3")]
    if final_inning >= 4:
        required_starts.append((4, "innings_4_6"))
    if final_inning >= 7:
        required_starts.append((7, "innings_7_plus"))
    first_start_by_phase = {
        phase: by_key[(inning, "top")].start_utc for inning, phase in required_starts
    }

    ordered = [
        phase
        for phase in ("innings_1_3", "innings_4_6", "innings_7_plus")
        if phase in first_start_by_phase
    ]
    windows: list[PhaseWindow] = []
    for index, phase in enumerate(ordered):
        next_start = (
            first_start_by_phase[ordered[index + 1]]
            if index + 1 < len(ordered)
            else actual_end
        )
        windows.append(PhaseWindow(phase, first_start_by_phase[phase], next_start))
    return tuple(windows)


def classify_timestamp(timing: GameTiming, value: datetime) -> str:
    """Classify one exact UTC timestamp using the study's fixed intervals.

    Phase starts are inclusive and phase ends are exclusive, except that the
    recorded final-play timestamp remains in the game's final live phase.
    """

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
    if timestamp == timing.actual_end_utc and timing.phase_windows:
        return timing.phase_windows[-1].phase
    raise ValueError("Timestamp falls inside a game but outside validated phase windows")


def parse_live_feed(payload: Mapping[str, Any]) -> GameTiming:
    """Parse actual play boundaries from a completed game's live feed.

    The start is the first plate appearance's observed start timestamp and the
    end is the final plate appearance's observed end timestamp.  Scheduled
    first pitch is never substituted for either value.
    """

    game_data = payload.get("gameData", {})
    if not isinstance(game_data, Mapping):
        raise ValueError("Live-feed field 'gameData' must be an object")
    status = game_data.get("status", {})
    if (
        not isinstance(status, Mapping)
        or str(status.get("abstractGameState", "")).casefold() != "final"
    ):
        raise ValueError("Live feed is not for a completed game")
    game_pk = _required_int(game_data.get("game", {}).get("pk"), "gameData.game.pk")

    live_data = payload.get("liveData", {})
    if not isinstance(live_data, Mapping):
        raise ValueError("Live-feed field 'liveData' must be an object")
    linescore = live_data.get("linescore", {})
    if not isinstance(linescore, Mapping):
        raise ValueError("Live-feed field 'liveData.linescore' must be an object")
    final_inning = _required_int(
        linescore.get("currentInning"), "liveData.linescore.currentInning"
    )
    plays_container = live_data.get("plays", {})
    if not isinstance(plays_container, Mapping):
        raise ValueError("Live-feed field 'liveData.plays' must be an object")
    plays = plays_container.get("allPlays", [])
    if not isinstance(plays, list) or not plays:
        raise ValueError(f"Completed game {game_pk} has no live-feed plays")

    observed: list[tuple[int, str, int, datetime, datetime]] = []
    for play in plays:
        if not isinstance(play, Mapping):
            raise ValueError("Live-feed plays must be objects")
        about = play.get("about", {})
        if not isinstance(about, Mapping):
            raise ValueError("Live-feed play field 'about' must be an object")
        inning = _required_int(about.get("inning"), "play.about.inning")
        half = str(about.get("halfInning", "")).casefold()
        if half not in VALID_HALVES:
            raise ValueError(f"Invalid halfInning for game {game_pk}: {half!r}")
        index = _required_int(about.get("atBatIndex"), "play.about.atBatIndex")
        if about.get("isComplete") is False:
            raise ValueError(f"Play {index} is incomplete in final feed for game {game_pk}")
        start = _play_timestamp(play, end=False)
        end = _play_timestamp(play, end=True)
        if end < start:
            raise ValueError(f"Play {index} ends before it starts for game {game_pk}")
        observed.append((inning, half, index, start, end))

    indices = [row[2] for row in observed]
    if indices != list(range(len(observed))):
        raise ValueError(
            f"Play indices must be unique, ordered, and contiguous from zero for game {game_pk}"
        )
    ordered_plays = observed
    first = ordered_plays[0]
    if (first[0], first[1]) != (1, "top"):
        raise ValueError(f"Final feed for game {game_pk} does not begin at top of inning 1")
    # MLB may record the next plate appearance's start before the previous
    # plate appearance's end (the scorer's PA intervals can overlap).  Starts
    # must still be chronological; each PA's end-before-start check above
    # remains strict.
    if any(
        right[3] < left[3]
        for left, right in zip(ordered_plays, ordered_plays[1:])
    ):
        raise ValueError(
            f"Plate-appearance start timestamps are not chronological for game {game_pk}"
        )
    half_order = {"top": 0, "bottom": 1}
    ordinals = [(row[0], half_order[row[1]]) for row in ordered_plays]
    if any(right < left for left, right in zip(ordinals, ordinals[1:])):
        raise ValueError(f"Inning halves are not chronological for game {game_pk}")
    observed_final_inning = max(row[0] for row in ordered_plays)
    if observed_final_inning != final_inning:
        raise ValueError(
            f"Final linescore ends in inning {final_inning}, but plays end in inning "
            f"{observed_final_inning} for game {game_pk}"
        )
    groups: dict[tuple[int, str], list[tuple[int, str, int, datetime, datetime]]] = {}
    group_order: list[tuple[int, str]] = []
    for row in ordered_plays:
        key = (row[0], row[1])
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append(row)

    required_boundary_innings = [1]
    if final_inning >= 4:
        required_boundary_innings.append(4)
    if final_inning >= 7:
        required_boundary_innings.append(7)
    missing_top_boundaries = [
        inning for inning in required_boundary_innings if (inning, "top") not in groups
    ]
    if missing_top_boundaries:
        raise ValueError(
            f"Final feed for game {game_pk} is missing top-of-inning boundaries: "
            f"{missing_top_boundaries}"
        )

    halves = tuple(
        InningHalfBoundary(
            inning=inning,
            half=half,
            phase=inning_phase(inning),
            start_utc=min(row[3] for row in groups[(inning, half)]),
            end_utc=max(row[4] for row in groups[(inning, half)]),
            first_at_bat_index=min(row[2] for row in groups[(inning, half)]),
            last_at_bat_index=max(row[2] for row in groups[(inning, half)]),
        )
        for inning, half in group_order
    )
    actual_start = ordered_plays[0][3]
    actual_end = ordered_plays[-1][4]
    return GameTiming(
        game_pk=game_pk,
        actual_start_utc=actual_start,
        actual_end_utc=actual_end,
        play_count=len(ordered_plays),
        inning_halves=halves,
        phase_windows=_build_phase_windows(halves, actual_end),
    )


class MlbApiClient:
    """Read-through cached client for the two MLB endpoints used by the study."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
        max_attempts: int = 3,
        backoff_seconds: float = 0.5,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")
        self.cache_dir = Path(cache_dir)
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    def _read_or_fetch(
        self,
        cache_path: Path,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        *,
        refresh: bool = False,
    ) -> Mapping[str, Any]:
        if cache_path.exists() and not refresh:
            with cache_path.open(encoding="utf-8") as handle:
                cached = json.load(handle)
            if not isinstance(cached, Mapping):
                raise ValueError(f"Cached MLB response must be an object: {cache_path}")
            return cached

        url = f"{MLB_STATS_API_BASE}{endpoint}"
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise requests.HTTPError(
                        f"Retryable MLB Stats API response {response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, Mapping):
                    raise ValueError("MLB Stats API response must be a JSON object")
                self._write_cache(cache_path, payload)
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                retryable = not isinstance(exc, requests.HTTPError) or (
                    exc.response is not None
                    and exc.response.status_code in RETRYABLE_STATUS_CODES
                )
                if not retryable or attempt + 1 == self.max_attempts:
                    raise
                time.sleep(self.backoff_seconds * (2**attempt))
        raise RuntimeError("MLB request failed") from last_error

    @staticmethod
    def _write_cache(path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def schedule_games(
        self,
        start_date: date,
        end_date: date,
        *,
        include_nonfinal_for_audit: bool = False,
        refresh: bool = False,
    ) -> tuple[ScheduleGame, ...]:
        """Fetch an inclusive date range and return final games by default."""

        if end_date < start_date:
            raise ValueError("end_date cannot precede start_date")
        schedule_records: list[ScheduleGame] = []
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(
                chunk_start + timedelta(days=SCHEDULE_MAX_DAYS_PER_REQUEST - 1),
                end_date,
            )
            cache_path = self.cache_dir / "schedule" / (
                f"{chunk_start.isoformat()}_{chunk_end.isoformat()}_linescore.json"
            )
            payload = self._read_or_fetch(
                cache_path,
                "/api/v1/schedule",
                {
                    "sportId": 1,
                    "startDate": chunk_start.isoformat(),
                    "endDate": chunk_end.isoformat(),
                    "hydrate": "linescore",
                },
                refresh=refresh,
            )
            schedule_records.extend(parse_schedule(payload))
            chunk_start = chunk_end + timedelta(days=1)

        games = reconcile_schedule_games(schedule_records)
        return games if include_nonfinal_for_audit else completed_schedule_games(games)

    def game_timing(self, game_pk: int, *, refresh: bool = False) -> GameTiming:
        """Fetch and parse observed timing for one completed game."""

        normalized_pk = _required_int(game_pk, "game_pk")
        cache_path = self.cache_dir / "live" / f"game_{normalized_pk}.json"
        payload = self._read_or_fetch(
            cache_path,
            f"/api/v1.1/game/{normalized_pk}/feed/live",
            refresh=refresh,
        )
        timing = parse_live_feed(payload)
        if timing.game_pk != normalized_pk:
            raise ValueError(
                f"Requested game {normalized_pk}, but MLB live feed returned game "
                f"{timing.game_pk}"
            )
        return timing
