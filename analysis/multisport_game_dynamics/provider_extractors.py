"""Pure, fail-closed extractors for cached multisport provider payloads.

The helpers in this module deliberately perform no network access.  They turn
already-cached ESPN payloads into small immutable records, provide conservative
participant matching, and extract only timezone-aware wall-clock timestamps.
Game-clock values and scheduled starts are never converted into live phase
boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import itertools
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping, Sequence
import unicodedata


class ProviderDataError(ValueError):
    """Raised when a cached provider payload cannot be interpreted safely."""


@dataclass(frozen=True, slots=True)
class CompetitorRecord:
    """Stable participant identity from one ESPN competition."""

    competitor_id: str
    name: str
    home_away: str | None
    winner: bool | None


@dataclass(frozen=True, slots=True)
class CompetitionRecord:
    """A single two-participant contest flattened from an ESPN scoreboard."""

    event_id: str
    competition_id: str
    event_name: str
    scheduled_start_utc: datetime
    competitors: tuple[CompetitorRecord, CompetitorRecord]
    status_state: str | None
    status_detail: str | None
    grouping_id: str | None = None
    grouping_name: str | None = None


@dataclass(frozen=True, slots=True)
class PairMatch:
    """Unique mapping from each left-side participant to a right-side index."""

    right_indices: tuple[int, int]
    scores: tuple[int, int]


@dataclass(frozen=True, slots=True)
class WallclockBoundaries:
    """Observed period starts and the final observed competitive/terminal time."""

    period_starts: Mapping[int, datetime]
    actual_end_utc: datetime


_MATCHUP_SEPARATOR = re.compile(r"\s+(?:vs?\.?|versus|at|@)\s+", re.IGNORECASE)
_DRAW_SUFFIX = re.compile(
    r"\s+(?:(?:end|finish|result)\s+in\s+(?:a\s+)?draw|be\s+(?:a\s+)?draw)\??$",
    re.IGNORECASE,
)
_SPACE = re.compile(r"\s+")
_TERMINAL_TEXT = (
    "end game",
    "end of game",
    "end regular time",
    "end of regulation",
    "end period",
    "final whistle",
    "match ends",
    "game ends",
    "bout ends",
)
_ADMINISTRATIVE_TEXT = (
    "timeout",
    "time out",
    "two minute warning",
    "two-minute warning",
    "injury delay",
    "official review",
)
_TIMESTAMP_FIELDS = ("wallclock", "wallClock", "timeActual", "date")


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderDataError(f"missing or invalid {field}")
    return value.strip()


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field)


def _parse_utc(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ProviderDataError(f"invalid {field}: {value!r}") from exc
    else:
        raise ProviderDataError(f"missing or invalid {field}")
    if parsed.tzinfo is None:
        raise ProviderDataError(f"{field} lacks a timezone")
    return parsed.astimezone(timezone.utc)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ProviderDataError(f"missing or invalid {field}")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        result = int(value.strip())
    else:
        raise ProviderDataError(f"missing or invalid {field}")
    if result < 1:
        raise ProviderDataError(f"{field} must be positive")
    return result


def _first_present(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def normalize_name(value: str) -> str:
    """Return a deterministic comparison form without inventing aliases.

    Unicode accents are removed, punctuation becomes a token boundary, and
    whitespace/case are normalized.  Initials are not expanded, suffixes are
    not removed, and tokens are never reordered.
    """

    text = _required_text(value, "participant name")
    decomposed = unicodedata.normalize("NFKD", text)
    without_marks = "".join(char for char in decomposed if not unicodedata.combining(char))
    tokenized = "".join(
        char.casefold() if char.isalnum() else " " for char in without_marks
    )
    normalized = _SPACE.sub(" ", tokenized).strip()
    if not normalized:
        raise ProviderDataError("participant name has no alphanumeric content")
    return normalized


def _tokens_contained(shorter: tuple[str, ...], longer: tuple[str, ...]) -> bool:
    width = len(shorter)
    return any(longer[index : index + width] == shorter for index in range(len(longer) - width + 1))


def name_match_score(left: str, right: str) -> int | None:
    """Score only exact or token-boundary containment matches.

    ``3`` is normalized equality, ``2`` is a full token suffix, and ``1`` is
    other contiguous token containment.  There is intentionally no edit
    distance, phonetic matching, or free-form confidence score.
    """

    left_tokens = tuple(normalize_name(left).split())
    right_tokens = tuple(normalize_name(right).split())
    if left_tokens == right_tokens:
        return 3
    shorter, longer = sorted((left_tokens, right_tokens), key=len)
    if not _tokens_contained(shorter, longer):
        return None
    if longer[-len(shorter) :] == shorter:
        return 2
    return 1


def names_match(left: str, right: str) -> bool:
    """Return whether two names meet the conservative token matching rule."""

    return name_match_score(left, right) is not None


def match_name_pair(
    left: Sequence[str], right: Sequence[str]
) -> PairMatch | None:
    """Return the unique best two-name mapping, or ``None`` when unsafe.

    Exact and token-contained edges are considered.  All complete mappings are
    enumerated, and a result is accepted only when one mapping has a unique
    highest deterministic score.  Tied mappings and duplicate normalized names
    are rejected rather than resolved by input order.
    """

    if len(left) != 2 or len(right) != 2:
        raise ProviderDataError("pair matching requires exactly two names on each side")
    left_normalized = tuple(normalize_name(value) for value in left)
    right_normalized = tuple(normalize_name(value) for value in right)
    if len(set(left_normalized)) != 2 or len(set(right_normalized)) != 2:
        return None

    candidates: list[tuple[int, tuple[int, int], tuple[int, int]]] = []
    for indices in itertools.permutations(range(2)):
        scores = tuple(name_match_score(left[index], right[indices[index]]) for index in range(2))
        if all(score is not None for score in scores):
            concrete = (int(scores[0]), int(scores[1]))  # narrowed by the preceding check
            candidates.append((sum(concrete), (indices[0], indices[1]), concrete))
    if not candidates:
        return None
    best_total = max(item[0] for item in candidates)
    best = [item for item in candidates if item[0] == best_total]
    if len(best) != 1:
        return None
    _, indices, scores = best[0]
    return PairMatch(right_indices=indices, scores=scores)


def pair_matches(left: Sequence[str], right: Sequence[str]) -> bool:
    """Return whether two participant pairs have one safe mapping."""

    return match_name_pair(left, right) is not None


def split_matchup(value: str) -> tuple[str, str] | None:
    """Split a plainly delimited matchup without fuzzy language parsing."""

    text = _required_text(value, "matchup")
    text = re.sub(r"^will\s+", "", text, flags=re.IGNORECASE)
    parts = _MATCHUP_SEPARATOR.split(text, maxsplit=1)
    if len(parts) != 2:
        return None
    left = parts[0].strip(" \t\r\n?:")
    right = _DRAW_SUFFIX.sub("", parts[1]).strip(" \t\r\n?:")
    if not left or not right or normalize_name(left) == normalize_name(right):
        return None
    return left, right


def _status(
    competition: Mapping[str, Any], event: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    status = competition.get("status", event.get("status"))
    if status is None:
        return None, None
    if not isinstance(status, Mapping):
        raise ProviderDataError("competition status must be an object")
    kind = status.get("type")
    if kind is None:
        return None, None
    if not isinstance(kind, Mapping):
        raise ProviderDataError("competition status.type must be an object")
    state = _optional_text(kind.get("state"), "status.type.state")
    detail_value = _first_present(kind.get("detail"), kind.get("description"))
    detail = _optional_text(detail_value, "status.type.detail")
    return state, detail


def _competitor(row: Mapping[str, Any], field: str) -> CompetitorRecord:
    entity: Mapping[str, Any] | None = None
    for key in ("team", "athlete"):
        candidate = row.get(key)
        if candidate is not None:
            if not isinstance(candidate, Mapping):
                raise ProviderDataError(f"{field}.{key} must be an object")
            entity = candidate
            break
    if entity is None:
        entity = row
    raw_id = _first_present(entity.get("id"), row.get("id"))
    competitor_id = _required_text(str(raw_id) if raw_id is not None else None, f"{field}.id")
    name = None
    for key in ("displayName", "fullName", "name", "shortName"):
        candidate = entity.get(key)
        if isinstance(candidate, str) and candidate.strip():
            name = candidate.strip()
            break
    if name is None:
        raise ProviderDataError(f"missing or invalid {field}.name")
    home_away = _optional_text(row.get("homeAway"), f"{field}.homeAway")
    winner = row.get("winner")
    if winner is not None and not isinstance(winner, bool):
        raise ProviderDataError(f"{field}.winner must be boolean or null")
    return CompetitorRecord(
        competitor_id=competitor_id,
        name=name,
        home_away=home_away,
        winner=winner,
    )


def _competition_record(
    event: Mapping[str, Any],
    competition: Mapping[str, Any],
    *,
    grouping: Mapping[str, Any] | None = None,
) -> CompetitionRecord:
    event_id = _required_text(event.get("id"), "event.id")
    competition_id = _required_text(competition.get("id"), "competition.id")
    event_name = _required_text(
        _first_present(event.get("name"), event.get("shortName"), competition.get("name")),
        "event.name",
    )
    start = _first_present(
        competition.get("startDate"), competition.get("date"), event.get("date")
    )
    scheduled_start = _parse_utc(start, "competition scheduled start")
    rows = competition.get("competitors")
    if (
        not isinstance(rows, list)
        or len(rows) != 2
        or not all(isinstance(row, Mapping) for row in rows)
    ):
        raise ProviderDataError("competition must contain exactly two competitor objects")
    competitors = tuple(
        _competitor(row, f"competition.competitors[{index}]")
        for index, row in enumerate(rows)
    )
    if len({row.competitor_id for row in competitors}) != 2:
        raise ProviderDataError("competition competitor ids must be distinct")
    if len({normalize_name(row.name) for row in competitors}) != 2:
        raise ProviderDataError("competition competitor names must be distinct")
    state, detail = _status(competition, event)
    grouping_id = None
    grouping_name = None
    if grouping is not None:
        grouping_id = _optional_text(grouping.get("id"), "grouping.id")
        grouping_name = _optional_text(
            _first_present(grouping.get("name"), grouping.get("displayName")),
            "grouping.name",
        )
    return CompetitionRecord(
        event_id=event_id,
        competition_id=competition_id,
        event_name=event_name,
        scheduled_start_utc=scheduled_start,
        competitors=(competitors[0], competitors[1]),
        status_state=state,
        status_detail=detail,
        grouping_id=grouping_id,
        grouping_name=grouping_name,
    )


def _events(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = payload.get("events")
    if not isinstance(rows, list):
        raise ProviderDataError("scoreboard events must be a list")
    if not all(isinstance(row, Mapping) for row in rows):
        raise ProviderDataError("every scoreboard event must be an object")
    return rows


def _require_unique_competitions(records: list[CompetitionRecord]) -> tuple[CompetitionRecord, ...]:
    ids = [row.competition_id for row in records]
    if len(ids) != len(set(ids)):
        raise ProviderDataError("flattened competition ids must be unique")
    return tuple(records)


def flatten_espn_standard(payload: Mapping[str, Any]) -> tuple[CompetitionRecord, ...]:
    """Flatten the ordinary ``events[].competitions[]`` scoreboard shape."""

    records: list[CompetitionRecord] = []
    for event in _events(payload):
        competitions = event.get("competitions")
        if not isinstance(competitions, list) or not competitions:
            raise ProviderDataError("standard ESPN event competitions must be a nonempty list")
        if not all(isinstance(row, Mapping) for row in competitions):
            raise ProviderDataError("every ESPN competition must be an object")
        records.extend(_competition_record(event, row) for row in competitions)
    return _require_unique_competitions(records)


def flatten_espn_tennis(payload: Mapping[str, Any]) -> tuple[CompetitionRecord, ...]:
    """Flatten tennis ``events[].groupings[].competitions[]`` payloads."""

    records: list[CompetitionRecord] = []
    for event in _events(payload):
        groupings = event.get("groupings")
        if not isinstance(groupings, list) or not groupings:
            raise ProviderDataError("tennis ESPN event groupings must be a nonempty list")
        for grouping in groupings:
            if not isinstance(grouping, Mapping):
                raise ProviderDataError("every tennis grouping must be an object")
            grouping_metadata = grouping.get("grouping", grouping)
            if not isinstance(grouping_metadata, Mapping):
                raise ProviderDataError("tennis grouping metadata must be an object")
            competitions = grouping.get("competitions")
            if not isinstance(competitions, list) or not competitions:
                raise ProviderDataError("tennis grouping competitions must be a nonempty list")
            if not all(isinstance(row, Mapping) for row in competitions):
                raise ProviderDataError("every tennis competition must be an object")
            for row in competitions:
                try:
                    records.append(_competition_record(event, row, grouping=grouping_metadata))
                except ProviderDataError:
                    # Tennis scoreboards retain future, cancelled, and TBD draw
                    # slots with incomplete competitors. Exclude only that slot;
                    # completed, fully identified matches on the date remain safe.
                    continue
    return _require_unique_competitions(records)


def flatten_espn_ufc(payload: Mapping[str, Any]) -> tuple[CompetitionRecord, ...]:
    """Flatten all bout competitions within each UFC card event."""

    records = flatten_espn_standard(payload)
    if records:
        by_event: dict[str, int] = {}
        for record in records:
            by_event[record.event_id] = by_event.get(record.event_id, 0) + 1
        if any(count < 1 for count in by_event.values()):  # defensive, documents card grain
            raise ProviderDataError("UFC event must contain at least one bout competition")
    return records


def _summary_events(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("plays", "commentary"):
        value = payload.get(key)
        if value is not None:
            if not isinstance(value, list) or not value:
                raise ProviderDataError(f"summary {key} must be a nonempty list")
            if not all(isinstance(row, Mapping) for row in value):
                raise ProviderDataError(f"every summary {key} row must be an object")
            if key == "commentary":
                nested = [
                    row["play"] if isinstance(row.get("play"), Mapping) else row
                    for row in value
                    if isinstance(row.get("play"), Mapping)
                    or (row.get("period") is not None and row.get("wallclock") is not None)
                ]
                if not nested:
                    raise ProviderDataError("summary commentary contains no literal plays")
                return nested
            return value
    drives = payload.get("drives")
    if not isinstance(drives, Mapping):
        raise ProviderDataError("summary lacks plays, commentary, and drives")
    previous = drives.get("previous")
    if not isinstance(previous, list) or not previous:
        raise ProviderDataError("summary drives.previous must be a nonempty list")
    if drives.get("current") not in (None, {}):
        raise ProviderDataError("completed summary unexpectedly has a current drive")
    flattened: list[Mapping[str, Any]] = []
    for drive in previous:
        if not isinstance(drive, Mapping):
            raise ProviderDataError("every summary drive must be an object")
        plays = drive.get("plays")
        if not isinstance(plays, list) or not all(isinstance(row, Mapping) for row in plays):
            raise ProviderDataError("every summary drive must contain a play list")
        flattened.extend(plays)
    if not flattened:
        raise ProviderDataError("summary drives contain no plays")
    return flattened


def _event_period(row: Mapping[str, Any], index: int) -> int | None:
    period = row.get("period")
    if isinstance(period, Mapping):
        period = period.get("number", period.get("value"))
    try:
        return _positive_int(period, f"event[{index}].period")
    except ProviderDataError:
        return None


def _event_timestamp(row: Mapping[str, Any], index: int) -> datetime | None:
    observed: list[tuple[str, datetime]] = []
    for key in _TIMESTAMP_FIELDS:
        if row.get(key) is not None:
            try:
                parsed = _parse_utc(row[key], f"event[{index}].{key}")
            except ProviderDataError:
                continue
            observed.append((key, parsed))
    if not observed:
        return None
    values = {value for _, value in observed}
    if len(values) != 1:
        fields = ", ".join(key for key, _ in observed)
        raise ProviderDataError(f"event[{index}] has contradictory timestamps in {fields}")
    return observed[0][1]


def _event_text(row: Mapping[str, Any]) -> str:
    values: list[str] = []
    for key in ("text", "shortText", "description"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    kind = row.get("type")
    if isinstance(kind, Mapping):
        for key in ("text", "name", "description"):
            value = kind.get(key)
            if isinstance(value, str) and value.strip():
                values.append(value.strip())
    return " ".join(values).casefold()


def _can_end_event(row: Mapping[str, Any]) -> bool:
    text = _event_text(row)
    if any(marker in text for marker in _TERMINAL_TEXT):
        return True
    return not any(marker in text for marker in _ADMINISTRATIVE_TEXT)


def _boundaries_from_events(events: Iterable[Mapping[str, Any]]) -> WallclockBoundaries:
    observed: list[tuple[int, datetime, Mapping[str, Any]]] = []
    for index, row in enumerate(events):
        if not isinstance(row, Mapping):
            raise ProviderDataError(f"event[{index}] must be an object")
        period = _event_period(row, index)
        if period is None:
            continue
        timestamp = _event_timestamp(row, index)
        if timestamp is None:
            continue
        observed.append((period, timestamp, row))
    if not observed:
        raise ProviderDataError("no usable literal timestamped period rows")
    present = tuple(sorted({period for period, _, _ in observed}))
    if present != tuple(range(1, max(present) + 1)):
        raise ProviderDataError(f"observed periods are not contiguous from one: {present}")
    starts = {
        period: min(timestamp for value, timestamp, _ in observed if value == period)
        for period in present
    }
    ordered_starts = [starts[period] for period in present]
    if any(later <= earlier for earlier, later in zip(ordered_starts, ordered_starts[1:])):
        raise ProviderDataError("observed period starts are not strictly increasing")
    eligible_end = [timestamp for _, timestamp, row in observed if _can_end_event(row)]
    if not eligible_end:
        raise ProviderDataError("no observed competitive or terminal event")
    actual_end = max(eligible_end)
    if actual_end <= ordered_starts[-1]:
        raise ProviderDataError("final observed period has no positive wall-clock span")
    return WallclockBoundaries(
        period_starts=MappingProxyType(starts),
        actual_end_utc=actual_end,
    )


def extract_standard_wallclock_boundaries(
    payload: Mapping[str, Any],
) -> WallclockBoundaries:
    """Extract ordered starts/end from standard plays, commentary, or CFB drives."""

    return _boundaries_from_events(_summary_events(payload))


def extract_ufc_core_wallclock_boundaries(
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
) -> WallclockBoundaries:
    """Extract round starts/end from already-hydrated ESPN Core play objects.

    A Core collection containing only ``$ref`` objects is rejected.  The caller
    must cache/hydrate those resources before invoking this network-free helper.
    """

    if isinstance(payload, Mapping):
        rows = payload.get("items", payload.get("plays"))
        if not isinstance(rows, list):
            raise ProviderDataError("UFC Core payload must contain an items or plays list")
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
        rows = list(payload)
    else:
        raise ProviderDataError("UFC Core payload must be an object or play sequence")
    if not rows or not all(isinstance(row, Mapping) for row in rows):
        raise ProviderDataError("UFC Core plays must be a nonempty object list")
    if any(set(row) <= {"$ref"} for row in rows):
        raise ProviderDataError("UFC Core play references must be hydrated before extraction")
    live_rows: list[Mapping[str, Any]] = []
    observed_period = 0
    for row in rows:
        period = row.get("period")
        if isinstance(period, Mapping):
            period = period.get("number", period.get("value"))
        if isinstance(period, int) and not isinstance(period, bool) and period >= 1:
            observed_period = max(observed_period, period)
            live_rows.append(row)
        elif observed_period and any(
            marker in _event_text(row) for marker in ("fight over", "bout ends")
        ):
            live_rows.append({**row, "period": {"number": observed_period}})
    if not live_rows:
        raise ProviderDataError("UFC Core payload has no live-round plays")
    return _boundaries_from_events(live_rows)


def derive_tennis_elapsed_thirds(
    actual_start_utc: datetime | str,
    duration_seconds: int,
    *,
    observed_end_utc: datetime | str | None = None,
) -> WallclockBoundaries:
    """Derive three equal elapsed-time phases from a validated match duration.

    These are elapsed thirds, not inferred set boundaries.  Duration must be an
    integral number of seconds in ``(0, 24h]``.  When an observed end is
    supplied it must agree exactly with start plus duration.
    """

    start = _parse_utc(actual_start_utc, "tennis actual start")
    if isinstance(duration_seconds, bool) or not isinstance(duration_seconds, int):
        raise ProviderDataError("tennis duration_seconds must be an integer")
    if not 0 < duration_seconds <= 24 * 60 * 60:
        raise ProviderDataError("tennis duration_seconds must be in (0, 86400]")
    duration = timedelta(seconds=duration_seconds)
    end = start + duration
    if observed_end_utc is not None:
        observed_end = _parse_utc(observed_end_utc, "tennis observed end")
        if observed_end != end:
            raise ProviderDataError("tennis start, duration, and observed end disagree")
    return WallclockBoundaries(
        period_starts=MappingProxyType(
            {
                1: start,
                2: start + duration / 3,
                3: start + duration * 2 / 3,
            }
        ),
        actual_end_utc=end,
    )
