from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest


from analysis.nba_game_dynamics.match_games import (
    CANONICAL_TEAM_SLUGS,
    NBA_TEAMS,
    assert_one_to_one_matches,
    match_market_candidates,
)
from analysis.nba_game_dynamics.nba_api import ScheduleGame


def _game(game_id: str = "0022400953", away: str = "nyk", home: str = "por", final: bool = True) -> ScheduleGame:
    a, h = NBA_TEAMS[away], NBA_TEAMS[home]
    return ScheduleGame(
        game_id=game_id, official_date=date(2025, 3, 12),
        scheduled_start_utc=datetime(2025, 3, 13, 2, tzinfo=timezone.utc),
        season_start=2024, game_type_code=game_id[:3],
        away_team_id=a.team_id, away_team_name=a.name, away_team_tricode=a.tricode,
        home_team_id=h.team_id, home_team_name=h.name, home_team_tricode=h.tricode,
        status_text="Final/OT" if final else "Postponed", is_completed=final,
        expected_final_period=5 if final else None,
        postponement_status=None, postponement_reason=None,
        away_final_score=114 if final else None, home_final_score=113 if final else None,
        winner_team_id=a.team_id if final else None,
        away_is_winner=True if final else None,
        home_is_winner=False if final else None,
    )


def _candidate(market_id: str, away: str = "nyk", home: str = "por") -> dict:
    return {"market_id": market_id, "date": "2025-03-12", "away": away, "home": home}


def test_team_map_has_30_canonical_teams_and_explicit_observed_aliases() -> None:
    assert len(CANONICAL_TEAM_SLUGS) == 30
    assert len({NBA_TEAMS[key].team_id for key in CANONICAL_TEAM_SLUGS}) == 30
    assert NBA_TEAMS["pho"] == NBA_TEAMS["phx"]
    assert NBA_TEAMS["wsh"] == NBA_TEAMS["was"]


def test_ordered_first_then_unique_reverse_and_alias() -> None:
    official = match_market_candidates([_candidate("a")], [_game()])[0]
    reversed_row = match_market_candidates([_candidate("b", "por", "nyk")], [_game()])[0]
    alias = match_market_candidates(
        [_candidate("c", "pho", "lal")], [_game("0022400954", "phx", "lal")]
    )[0]
    assert official.matched_game_id == "0022400953"
    assert official.slug_orientation == "official"
    assert reversed_row.matched_game_id == "0022400953"
    assert reversed_row.slug_orientation == "reversed"
    assert alias.is_matched and alias.away_team == NBA_TEAMS["phx"]


def test_same_date_ambiguity_and_duplicate_assignment_never_rank() -> None:
    ambiguous = match_market_candidates(
        [_candidate("a")], [_game(), replace(_game(), game_id="0022400955")]
    )[0]
    assert ambiguous.exclusion_reason == "multiple_schedule_matches"
    assert len(ambiguous.schedule_matches) == 2

    duplicate = match_market_candidates([_candidate("a"), _candidate("b")], [_game()])
    assert {row.exclusion_reason for row in duplicate} == {"duplicate_market_to_game_mapping"}
    with pytest.raises(ValueError, match="not one-to-one"):
        assert_one_to_one_matches(duplicate)


def test_special_event_unknown_nonfinal_and_date_mismatch_are_explicit() -> None:
    rows = match_market_candidates(
        [
            _candidate("special", "world", "stars"),
            _candidate("unknown", "xyz", "por"),
            _candidate("nonfinal", "bos", "lal"),
            {**_candidate("wrong-date"), "date": "2025-03-13"},
        ],
        [_game(), _game("0022400954", "bos", "lal", final=False)],
    )
    assert [row.exclusion_reason for row in rows] == [
        "special_event_team", "unknown_team_slug", "nonfinal_game", "no_schedule_match"
    ]
