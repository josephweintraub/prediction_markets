"""Deterministic NBA candidate-to-schedule matching.

Match exact local date and observed slug order first.  Only when that finds no
game is the reversed orientation tried.  Ambiguities and duplicate market to
game assignments are retained and rejected; no time/volume/confidence ranking
or fuzzy team matching is permitted.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Iterable, Mapping

try:  # Package import for tests; direct import for script-directory execution.
    from .nba_api import ScheduleGame
except ImportError:  # pragma: no cover
    from nba_api import ScheduleGame


@dataclass(frozen=True)
class NbaTeam:
    team_id: int
    name: str
    tricode: str


_CANONICAL = {
    "atl": NbaTeam(1610612737, "Atlanta Hawks", "ATL"),
    "bos": NbaTeam(1610612738, "Boston Celtics", "BOS"),
    "cle": NbaTeam(1610612739, "Cleveland Cavaliers", "CLE"),
    "nop": NbaTeam(1610612740, "New Orleans Pelicans", "NOP"),
    "chi": NbaTeam(1610612741, "Chicago Bulls", "CHI"),
    "dal": NbaTeam(1610612742, "Dallas Mavericks", "DAL"),
    "den": NbaTeam(1610612743, "Denver Nuggets", "DEN"),
    "gsw": NbaTeam(1610612744, "Golden State Warriors", "GSW"),
    "hou": NbaTeam(1610612745, "Houston Rockets", "HOU"),
    "lac": NbaTeam(1610612746, "LA Clippers", "LAC"),
    "lal": NbaTeam(1610612747, "Los Angeles Lakers", "LAL"),
    "mia": NbaTeam(1610612748, "Miami Heat", "MIA"),
    "mil": NbaTeam(1610612749, "Milwaukee Bucks", "MIL"),
    "min": NbaTeam(1610612750, "Minnesota Timberwolves", "MIN"),
    "bkn": NbaTeam(1610612751, "Brooklyn Nets", "BKN"),
    "nyk": NbaTeam(1610612752, "New York Knicks", "NYK"),
    "orl": NbaTeam(1610612753, "Orlando Magic", "ORL"),
    "ind": NbaTeam(1610612754, "Indiana Pacers", "IND"),
    "phi": NbaTeam(1610612755, "Philadelphia 76ers", "PHI"),
    "phx": NbaTeam(1610612756, "Phoenix Suns", "PHX"),
    "por": NbaTeam(1610612757, "Portland Trail Blazers", "POR"),
    "sac": NbaTeam(1610612758, "Sacramento Kings", "SAC"),
    "sas": NbaTeam(1610612759, "San Antonio Spurs", "SAS"),
    "okc": NbaTeam(1610612760, "Oklahoma City Thunder", "OKC"),
    "tor": NbaTeam(1610612761, "Toronto Raptors", "TOR"),
    "uta": NbaTeam(1610612762, "Utah Jazz", "UTA"),
    "mem": NbaTeam(1610612763, "Memphis Grizzlies", "MEM"),
    "was": NbaTeam(1610612764, "Washington Wizards", "WAS"),
    "det": NbaTeam(1610612765, "Detroit Pistons", "DET"),
    "cha": NbaTeam(1610612766, "Charlotte Hornets", "CHA"),
}
CANONICAL_TEAM_SLUGS = frozenset(_CANONICAL)
NBA_TEAMS = {**_CANONICAL, "pho": _CANONICAL["phx"], "wsh": _CANONICAL["was"]}
SPECIAL_EVENT_SLUGS = frozenset(
    {"kys", "cgs", "sog", "crs", "stars", "stripes", "world"}
)


@dataclass(frozen=True)
class GameMatchAudit:
    market_id: str
    market_date: date | None
    observed_first_slug: str
    observed_second_slug: str
    slug_orientation: str | None
    away_team: NbaTeam | None
    home_team: NbaTeam | None
    matched_game_id: str | None
    exclusion_reason: str | None
    schedule_matches: tuple[ScheduleGame, ...]

    @property
    def is_matched(self) -> bool:
        return self.matched_game_id is not None and self.exclusion_reason is None


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _string(value: Any) -> str:
    return value.strip().casefold() if isinstance(value, str) else ""


def _match_one(
    candidate: Mapping[str, Any], schedules: tuple[ScheduleGame, ...]
) -> GameMatchAudit:
    market_id = candidate.get("market_id")
    if not isinstance(market_id, str) or not market_id.strip():
        raise ValueError("Each NBA candidate must have a non-empty market_id")
    market_id = market_id.strip()
    market_date = _date(candidate.get("date"))
    first_slug = _string(candidate.get("away"))
    second_slug = _string(candidate.get("home"))
    first = NBA_TEAMS.get(first_slug)
    second = NBA_TEAMS.get(second_slug)
    common = {
        "market_id": market_id,
        "market_date": market_date,
        "observed_first_slug": first_slug,
        "observed_second_slug": second_slug,
        "slug_orientation": None,
        "away_team": None,
        "home_team": None,
        "matched_game_id": None,
        "schedule_matches": (),
    }
    if first_slug in SPECIAL_EVENT_SLUGS or second_slug in SPECIAL_EVENT_SLUGS:
        return GameMatchAudit(**common, exclusion_reason="special_event_team")
    if market_date is None:
        return GameMatchAudit(**common, exclusion_reason="invalid_candidate_date")
    if first is None or second is None:
        return GameMatchAudit(**common, exclusion_reason="unknown_team_slug")

    matches = tuple(
        row for row in schedules
        if row.official_date == market_date
        and row.away_team_id == first.team_id
        and row.home_team_id == second.team_id
    )
    orientation = "official"
    if not matches:
        matches = tuple(
            row for row in schedules
            if row.official_date == market_date
            and row.away_team_id == second.team_id
            and row.home_team_id == first.team_id
        )
        orientation = "reversed"
    common["schedule_matches"] = matches
    if not matches:
        return GameMatchAudit(**common, exclusion_reason="no_schedule_match")
    common["slug_orientation"] = orientation
    canonical = {
        (row.away_team_id, row.away_team_name, row.away_team_tricode,
         row.home_team_id, row.home_team_name, row.home_team_tricode)
        for row in matches
    }
    if len(canonical) == 1:
        away_id, away_name, away_code, home_id, home_name, home_code = canonical.pop()
        common["away_team"] = NbaTeam(away_id, away_name, away_code)
        common["home_team"] = NbaTeam(home_id, home_name, home_code)
    if len(matches) > 1:
        return GameMatchAudit(**common, exclusion_reason="multiple_schedule_matches")
    game = matches[0]
    if not game.is_completed:
        return GameMatchAudit(**common, exclusion_reason="nonfinal_game")
    common["matched_game_id"] = game.game_id
    return GameMatchAudit(**common, exclusion_reason=None)


def match_market_candidates(
    candidates: Iterable[Mapping[str, Any]], schedules: Iterable[ScheduleGame]
) -> tuple[GameMatchAudit, ...]:
    schedule_rows = tuple(sorted(schedules, key=lambda row: (row.official_date, row.game_id)))
    audits = [_match_one(candidate, schedule_rows) for candidate in candidates]
    positions: dict[str, list[int]] = {}
    for index, row in enumerate(audits):
        if row.matched_game_id is not None:
            positions.setdefault(row.matched_game_id, []).append(index)
    for indexes in positions.values():
        if len(indexes) > 1:
            for index in indexes:
                audits[index] = replace(
                    audits[index], matched_game_id=None,
                    exclusion_reason="duplicate_market_to_game_mapping"
                )
    return tuple(audits)


def assert_one_to_one_matches(audits: Iterable[GameMatchAudit]) -> None:
    rows = tuple(audits)
    market_ids = [row.market_id for row in rows]
    duplicates = sorted({value for value in market_ids if market_ids.count(value) > 1})
    if duplicates:
        raise ValueError(f"Duplicate market IDs in NBA match audit: {duplicates}")
    game_to_markets: dict[str, list[str]] = {}
    for row in rows:
        if row.is_matched:
            assert row.matched_game_id is not None
            game_to_markets.setdefault(row.matched_game_id, []).append(row.market_id)
        elif row.exclusion_reason == "duplicate_market_to_game_mapping":
            if len(row.schedule_matches) != 1:
                raise ValueError("Duplicate game mapping must retain one schedule record")
            game_to_markets.setdefault(row.schedule_matches[0].game_id, []).append(row.market_id)
    conflicts = {key: value for key, value in game_to_markets.items() if len(value) > 1}
    if conflicts:
        raise ValueError(f"NBA market-to-game mapping is not one-to-one: {conflicts}")
