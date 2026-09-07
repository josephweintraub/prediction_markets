"""Pure, inspectable matching of MLB moneyline candidates to schedule games.

Candidate discovery belongs to ``build_market_universe.py``.  This module does
not decide whether a Polymarket market is a moneyline; it only links an already
selected candidate to an official game using date and exact two-team identity.
Observed slug order is tried first as official away/home order, then reversed
only when no official-order game exists.  The orientation is audited, and
canonical away/home identity comes only from MLB's schedule.  No fuzzy names,
start-time proximity, or doubleheader ranking is used.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from mlb_api import ScheduleGame


@dataclass(frozen=True)
class MlbTeam:
    team_id: int
    name: str


# Polymarket's observed 2025-2026 team slugs.  Names are included for audit
# readability; matching uses MLB's stable team IDs.
MLB_TEAMS: dict[str, MlbTeam] = {
    "ari": MlbTeam(109, "Arizona Diamondbacks"),
    "atl": MlbTeam(144, "Atlanta Braves"),
    "bal": MlbTeam(110, "Baltimore Orioles"),
    "bos": MlbTeam(111, "Boston Red Sox"),
    "chc": MlbTeam(112, "Chicago Cubs"),
    "cin": MlbTeam(113, "Cincinnati Reds"),
    "cle": MlbTeam(114, "Cleveland Guardians"),
    "col": MlbTeam(115, "Colorado Rockies"),
    "cws": MlbTeam(145, "Chicago White Sox"),
    "det": MlbTeam(116, "Detroit Tigers"),
    "hou": MlbTeam(117, "Houston Astros"),
    "kc": MlbTeam(118, "Kansas City Royals"),
    "laa": MlbTeam(108, "Los Angeles Angels"),
    "lad": MlbTeam(119, "Los Angeles Dodgers"),
    "mia": MlbTeam(146, "Miami Marlins"),
    "mil": MlbTeam(158, "Milwaukee Brewers"),
    "min": MlbTeam(142, "Minnesota Twins"),
    "nym": MlbTeam(121, "New York Mets"),
    "nyy": MlbTeam(147, "New York Yankees"),
    "oak": MlbTeam(133, "Athletics"),
    "phi": MlbTeam(143, "Philadelphia Phillies"),
    "pit": MlbTeam(134, "Pittsburgh Pirates"),
    "sd": MlbTeam(135, "San Diego Padres"),
    "sea": MlbTeam(136, "Seattle Mariners"),
    "sf": MlbTeam(137, "San Francisco Giants"),
    "stl": MlbTeam(138, "St. Louis Cardinals"),
    "tb": MlbTeam(139, "Tampa Bay Rays"),
    "tex": MlbTeam(140, "Texas Rangers"),
    "tor": MlbTeam(141, "Toronto Blue Jays"),
    "wsh": MlbTeam(120, "Washington Nationals"),
}

NON_TEAM_ALL_STAR_SLUGS = frozenset({"al", "nl"})


@dataclass(frozen=True)
class GameMatchAudit:
    """One deterministic linkage result for one preliminary candidate.

    ``schedule_matches`` preserves every exact schedule record and its status,
    rescheduling, and doubleheader metadata.  It is particularly important for
    explaining ambiguous and non-final exclusions.
    """

    market_id: str
    market_date: date | None
    # Legacy column names retained for artifact compatibility.  These are the
    # observed first and second team positions in the Polymarket event slug;
    # only ``slug_orientation`` says whether they are official away/home order.
    away_slug: str
    home_slug: str
    slug_orientation: str | None
    # Canonical away/home identities, populated only from same-date MLB
    # schedule rows.  They are never inferred from slug position.
    away_team: MlbTeam | None
    home_team: MlbTeam | None
    matched_game_pk: int | None
    exclusion_reason: str | None
    schedule_matches: tuple[ScheduleGame, ...]

    @property
    def is_matched(self) -> bool:
        return self.matched_game_pk is not None and self.exclusion_reason is None


def _candidate_date(value: Any) -> date | None:
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


def _candidate_slug(value: Any) -> str:
    return value.strip().casefold() if isinstance(value, str) else ""


def _market_id(candidate: Mapping[str, Any]) -> str:
    value = candidate.get("market_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Each MLB candidate must have a non-empty string market_id")
    return value.strip()


def _match_one(
    candidate: Mapping[str, Any], schedules: tuple[ScheduleGame, ...]
) -> GameMatchAudit:
    market_id = _market_id(candidate)
    market_date = _candidate_date(candidate.get("date"))
    away_slug = _candidate_slug(candidate.get("away"))
    home_slug = _candidate_slug(candidate.get("home"))
    first_team = MLB_TEAMS.get(away_slug)
    second_team = MLB_TEAMS.get(home_slug)

    common = {
        "market_id": market_id,
        "market_date": market_date,
        "away_slug": away_slug,
        "home_slug": home_slug,
        "slug_orientation": None,
        "away_team": None,
        "home_team": None,
        "matched_game_pk": None,
        "schedule_matches": (),
    }
    if away_slug in NON_TEAM_ALL_STAR_SLUGS or home_slug in NON_TEAM_ALL_STAR_SLUGS:
        return GameMatchAudit(**common, exclusion_reason="non_team_all_star")
    if market_date is None:
        return GameMatchAudit(**common, exclusion_reason="invalid_candidate_date")
    if first_team is None or second_team is None:
        return GameMatchAudit(**common, exclusion_reason="unknown_team_slug")

    exact = tuple(
        game
        for game in schedules
        if game.official_date == market_date
        and game.away_team_id == first_team.team_id
        and game.home_team_id == second_team.team_id
    )
    if exact:
        common["slug_orientation"] = "official"
    else:
        exact = tuple(
            game
            for game in schedules
            if game.official_date == market_date
            and game.away_team_id == second_team.team_id
            and game.home_team_id == first_team.team_id
        )
        if exact:
            common["slug_orientation"] = "reversed"
    common["schedule_matches"] = exact
    if not exact:
        return GameMatchAudit(**common, exclusion_reason="no_schedule_match")

    canonical_pairs = {
        (
            game.away_team_id,
            game.away_team_name,
            game.home_team_id,
            game.home_team_name,
        )
        for game in exact
    }
    if len(canonical_pairs) == 1:
        away_id, away_name, home_id, home_name = canonical_pairs.pop()
        common["away_team"] = MlbTeam(away_id, away_name)
        common["home_team"] = MlbTeam(home_id, home_name)
    if len(exact) > 1:
        return GameMatchAudit(**common, exclusion_reason="multiple_schedule_matches")

    game = exact[0]
    if not game.is_completed:
        return GameMatchAudit(**common, exclusion_reason="nonfinal_game")
    common["matched_game_pk"] = game.game_pk
    return GameMatchAudit(**common, exclusion_reason=None)


def match_market_candidates(
    candidates: Iterable[Mapping[str, Any]], schedules: Iterable[ScheduleGame]
) -> tuple[GameMatchAudit, ...]:
    """Match candidates, then mark every duplicated game assignment ambiguous."""

    schedule_rows = tuple(
        sorted(schedules, key=lambda game: (game.official_date, game.game_pk))
    )
    audits = [_match_one(candidate, schedule_rows) for candidate in candidates]

    positions_by_game: dict[int, list[int]] = {}
    for position, row in enumerate(audits):
        if row.matched_game_pk is not None:
            positions_by_game.setdefault(row.matched_game_pk, []).append(position)
    for positions in positions_by_game.values():
        if len(positions) > 1:
            for position in positions:
                audits[position] = replace(
                    audits[position],
                    matched_game_pk=None,
                    exclusion_reason="duplicate_market_to_game_mapping",
                )
    return tuple(audits)


def assert_one_to_one_matches(audits: Iterable[GameMatchAudit]) -> None:
    """Fail if market IDs or underlying eligible game assignments are duplicated.

    Legitimate exclusions such as unknown slugs and non-final games do not fail
    this gate.  Duplicate candidate-to-game assignments do, even though the
    matcher has already marked them as exclusions in the audit table.
    """

    rows = tuple(audits)
    positions_by_market: dict[str, list[int]] = {}
    for position, row in enumerate(rows):
        positions_by_market.setdefault(row.market_id, []).append(position)
    duplicate_markets = {
        market_id: positions
        for market_id, positions in positions_by_market.items()
        if len(positions) > 1
    }
    if duplicate_markets:
        details = "; ".join(
            f"{market_id}: rows {positions}"
            for market_id, positions in sorted(duplicate_markets.items())
        )
        raise ValueError(f"Duplicate market IDs in MLB match audit: {details}")

    game_to_markets: dict[int, list[str]] = {}
    for row in rows:
        if row.is_matched:
            game_to_markets.setdefault(row.matched_game_pk, []).append(row.market_id)
        elif row.exclusion_reason == "duplicate_market_to_game_mapping":
            if len(row.schedule_matches) != 1:
                raise ValueError(
                    "Duplicate game-mapping audit row must retain one schedule match: "
                    f"{row.market_id}"
                )
            game_to_markets.setdefault(row.schedule_matches[0].game_pk, []).append(
                row.market_id
            )
    duplicate_games = {
        game_pk: market_ids
        for game_pk, market_ids in game_to_markets.items()
        if len(market_ids) > 1
    }
    if duplicate_games:
        details = "; ".join(
            f"game {game_pk}: {sorted(market_ids)}"
            for game_pk, market_ids in sorted(duplicate_games.items())
        )
        raise ValueError(f"MLB market-to-game mapping is not one-to-one: {details}")
