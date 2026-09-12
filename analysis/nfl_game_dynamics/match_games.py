"""Exact NFL candidate-to-schedule matching with explicit team aliases."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from .nfl_api import ScheduleGame


@dataclass(frozen=True)
class NflTeam:
    team_id: int
    abbreviation: str
    name: str


_CANONICAL = {
    "ari": (22, "ARI", "Arizona Cardinals"), "atl": (1, "ATL", "Atlanta Falcons"),
    "bal": (33, "BAL", "Baltimore Ravens"), "buf": (2, "BUF", "Buffalo Bills"),
    "car": (29, "CAR", "Carolina Panthers"), "chi": (3, "CHI", "Chicago Bears"),
    "cin": (4, "CIN", "Cincinnati Bengals"), "cle": (5, "CLE", "Cleveland Browns"),
    "dal": (6, "DAL", "Dallas Cowboys"), "den": (7, "DEN", "Denver Broncos"),
    "det": (8, "DET", "Detroit Lions"), "gb": (9, "GB", "Green Bay Packers"),
    "hou": (34, "HOU", "Houston Texans"), "ind": (11, "IND", "Indianapolis Colts"),
    "jax": (30, "JAX", "Jacksonville Jaguars"), "kc": (12, "KC", "Kansas City Chiefs"),
    "lar": (14, "LAR", "Los Angeles Rams"), "lac": (24, "LAC", "Los Angeles Chargers"),
    "lv": (13, "LV", "Las Vegas Raiders"), "mia": (15, "MIA", "Miami Dolphins"),
    "min": (16, "MIN", "Minnesota Vikings"), "ne": (17, "NE", "New England Patriots"),
    "no": (18, "NO", "New Orleans Saints"), "nyg": (19, "NYG", "New York Giants"),
    "nyj": (20, "NYJ", "New York Jets"), "phi": (21, "PHI", "Philadelphia Eagles"),
    "pit": (23, "PIT", "Pittsburgh Steelers"), "sf": (25, "SF", "San Francisco 49ers"),
    "sea": (26, "SEA", "Seattle Seahawks"), "tb": (27, "TB", "Tampa Bay Buccaneers"),
    "ten": (10, "TEN", "Tennessee Titans"), "wsh": (28, "WSH", "Washington Commanders"),
}
NFL_TEAMS = {slug: NflTeam(*values) for slug, values in _CANONICAL.items()}

# Exact aliases observed in the 2024--2026 Polymarket NFL slug inventory.
NFL_TEAM_ALIASES: dict[str, str] = {
    **{slug: slug for slug in NFL_TEAMS},
    "la": "lar",
    "las": "lv",
    "was": "wsh",
}


@dataclass(frozen=True)
class GameMatchAudit:
    market_id: str
    market_date: date | None
    team_1_slug: str
    team_2_slug: str
    slug_orientation: str | None
    away_team: NflTeam | None
    home_team: NflTeam | None
    matched_game_id: str | None
    exclusion_reason: str | None
    schedule_matches: tuple[ScheduleGame, ...]

    @property
    def is_matched(self) -> bool:
        return self.matched_game_id is not None and self.exclusion_reason is None


def _as_date(value: Any) -> date | None:
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


def _candidate_value(candidate: Mapping[str, Any], key: str) -> str:
    value = candidate.get(key)
    return value.strip().casefold() if isinstance(value, str) else ""


def _candidate_id(candidate: Mapping[str, Any]) -> str:
    value = candidate.get("market_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each NFL candidate must have a non-empty market_id")
    return value.strip()


def _match_one(candidate: Mapping[str, Any], schedules: tuple[ScheduleGame, ...]) -> GameMatchAudit:
    market_id = _candidate_id(candidate)
    market_date = _as_date(candidate.get("date"))
    first_slug = _candidate_value(candidate, "team_1_slug")
    second_slug = _candidate_value(candidate, "team_2_slug")
    first_key = NFL_TEAM_ALIASES.get(first_slug)
    second_key = NFL_TEAM_ALIASES.get(second_slug)
    common: dict[str, Any] = {
        "market_id": market_id,
        "market_date": market_date,
        "team_1_slug": first_slug,
        "team_2_slug": second_slug,
        "slug_orientation": None,
        "away_team": None,
        "home_team": None,
        "matched_game_id": None,
        "schedule_matches": (),
    }
    if market_date is None:
        return GameMatchAudit(**common, exclusion_reason="invalid_candidate_date")
    if first_key is None or second_key is None:
        return GameMatchAudit(**common, exclusion_reason="unknown_team_slug")
    first = NFL_TEAMS[first_key]
    second = NFL_TEAMS[second_key]
    if first.team_id == second.team_id:
        return GameMatchAudit(**common, exclusion_reason="duplicate_team_identity")
    exact = tuple(
        game for game in schedules
        if game.official_date == market_date
        and game.away_team_id == first.team_id
        and game.home_team_id == second.team_id
    )
    if exact:
        common["slug_orientation"] = "official"
    else:
        exact = tuple(
            game for game in schedules
            if game.official_date == market_date
            and game.away_team_id == second.team_id
            and game.home_team_id == first.team_id
        )
        if exact:
            common["slug_orientation"] = "reversed"
    common["schedule_matches"] = exact
    if not exact:
        return GameMatchAudit(**common, exclusion_reason="no_schedule_match")
    pairs = {(g.away_team_id, g.away_abbreviation, g.away_team_name,
              g.home_team_id, g.home_abbreviation, g.home_team_name) for g in exact}
    if len(pairs) == 1:
        away_id, away_abbr, away_name, home_id, home_abbr, home_name = pairs.pop()
        common["away_team"] = NflTeam(away_id, away_abbr, away_name)
        common["home_team"] = NflTeam(home_id, home_abbr, home_name)
    if len(exact) != 1:
        return GameMatchAudit(**common, exclusion_reason="multiple_schedule_matches")
    if not exact[0].is_completed:
        return GameMatchAudit(**common, exclusion_reason="nonfinal_game")
    common["matched_game_id"] = exact[0].game_id
    return GameMatchAudit(**common, exclusion_reason=None)


def match_market_candidates(
    candidates: Iterable[Mapping[str, Any]], schedules: Iterable[ScheduleGame]
) -> tuple[GameMatchAudit, ...]:
    schedule_rows = tuple(sorted(schedules, key=lambda game: (game.official_date, game.game_id)))
    rows = [_match_one(candidate, schedule_rows) for candidate in candidates]
    positions: dict[str, list[int]] = {}
    for index, row in enumerate(rows):
        if row.matched_game_id is not None:
            positions.setdefault(row.matched_game_id, []).append(index)
    for indexes in positions.values():
        if len(indexes) > 1:
            for index in indexes:
                rows[index] = replace(rows[index], matched_game_id=None,
                                      exclusion_reason="duplicate_market_to_game_mapping")
    return tuple(rows)


def assert_one_to_one_matches(rows: Iterable[GameMatchAudit]) -> None:
    rows = tuple(rows)
    markets = [row.market_id for row in rows]
    if len(markets) != len(set(markets)):
        raise ValueError("Duplicate market IDs in NFL match audit")
    assigned: dict[str, list[str]] = {}
    for row in rows:
        if row.is_matched:
            assigned.setdefault(row.matched_game_id, []).append(row.market_id)
        elif row.exclusion_reason == "duplicate_market_to_game_mapping" and len(row.schedule_matches) == 1:
            assigned.setdefault(row.schedule_matches[0].game_id, []).append(row.market_id)
    conflicts = {game_id: ids for game_id, ids in assigned.items() if len(ids) > 1}
    if conflicts:
        raise ValueError(f"NFL market-to-game mapping is not one-to-one: {conflicts}")
