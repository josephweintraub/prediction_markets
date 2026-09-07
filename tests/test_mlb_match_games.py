from __future__ import annotations

import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

from match_games import (  # noqa: E402
    MLB_TEAMS,
    assert_one_to_one_matches,
    match_market_candidates,
)
from mlb_api import ScheduleGame  # noqa: E402


def _candidate(
    market_id: str,
    away: str = "nyy",
    home: str = "bos",
    game_date: str = "2025-07-04",
) -> dict[str, object]:
    return {
        "market_id": market_id,
        "date": game_date,
        "away": away,
        "home": home,
        "question": "Yankees vs. Red Sox",
    }


def _game(
    game_pk: int,
    away: str = "nyy",
    home: str = "bos",
    *,
    game_date: date = date(2025, 7, 4),
    final: bool = True,
    doubleheader: str = "N",
    game_number: int = 1,
) -> ScheduleGame:
    return ScheduleGame(
        game_pk=game_pk,
        official_date=game_date,
        scheduled_start_utc=datetime(2025, 7, 4, 17, 5, tzinfo=timezone.utc),
        game_type="R",
        season=2025,
        away_team_id=MLB_TEAMS[away].team_id,
        away_team_name=MLB_TEAMS[away].name,
        home_team_id=MLB_TEAMS[home].team_id,
        home_team_name=MLB_TEAMS[home].name,
        status_abstract="Final" if final else "Preview",
        status_detailed="Final" if final else "Postponed",
        status_code="F" if final else "DR",
        is_completed=final,
        doubleheader=doubleheader,
        game_number=game_number,
        series_game_number=game_number,
        reschedule_date_utc=None,
        rescheduled_from_date=None,
        resume_date_utc=None,
        resumed_from_date=None,
    )


def test_team_map_covers_the_30_observed_team_slugs() -> None:
    assert set(MLB_TEAMS) == {
        "ari", "atl", "bal", "bos", "chc", "cin", "cle", "col", "cws", "det",
        "hou", "kc", "laa", "lad", "mia", "mil", "min", "nym", "nyy", "oak",
        "phi", "pit", "sd", "sea", "sf", "stl", "tb", "tex", "tor", "wsh",
    }
    assert len({team.team_id for team in MLB_TEAMS.values()}) == 30


def test_exact_date_and_ordered_teams_match_and_preserve_metadata() -> None:
    game = _game(2001, doubleheader="N")
    row = match_market_candidates([_candidate("market-a")], [game])[0]

    assert row.is_matched is True
    assert row.matched_game_pk == 2001
    assert row.exclusion_reason is None
    assert row.slug_orientation == "official"
    assert row.away_team == MLB_TEAMS["nyy"]
    assert row.home_team == MLB_TEAMS["bos"]
    assert row.schedule_matches == (game,)
    assert row.schedule_matches[0].status_detailed == "Final"
    assert row.schedule_matches[0].doubleheader == "N"
    assert_one_to_one_matches([row])


def test_reversed_slug_order_matches_and_uses_official_schedule_identity() -> None:
    row = match_market_candidates(
        [_candidate("market-a", away="bos", home="nyy")], [_game(2001)]
    )[0]

    assert row.is_matched
    assert row.matched_game_pk == 2001
    assert row.exclusion_reason is None
    assert row.slug_orientation == "reversed"
    assert (row.away_slug, row.home_slug) == ("bos", "nyy")
    assert row.away_team == MLB_TEAMS["nyy"]
    assert row.home_team == MLB_TEAMS["bos"]
    assert row.schedule_matches == (_game(2001),)


def test_production_shaped_106_unique_reverse_order_games_are_recovered() -> None:
    first_day = date(2025, 4, 2)
    candidates = []
    games = []
    for index in range(106):
        game_date = first_day + timedelta(days=index)
        candidates.append(
            _candidate(
                f"reversed-{index:03d}",
                away="bos",
                home="nyy",
                game_date=game_date.isoformat(),
            )
        )
        games.append(
            _game(
                10_000 + index,
                away="nyy",
                home="bos",
                game_date=game_date,
            )
        )

    rows = match_market_candidates(candidates, games)

    assert len(rows) == 106
    assert all(row.is_matched for row in rows)
    assert {row.slug_orientation for row in rows} == {"reversed"}
    assert {row.away_team for row in rows} == {MLB_TEAMS["nyy"]}
    assert {row.home_team for row in rows} == {MLB_TEAMS["bos"]}
    assert_one_to_one_matches(rows)


def test_different_official_date_does_not_match() -> None:
    row = match_market_candidates(
        [_candidate("market-a", game_date="2025-07-05")], [_game(2001)]
    )[0]

    assert row.matched_game_pk is None
    assert row.exclusion_reason == "no_schedule_match"


def test_rescheduled_from_date_is_not_used_as_a_match_key() -> None:
    moved = replace(
        _game(2001, game_date=date(2025, 7, 5)),
        rescheduled_from_date=date(2025, 7, 4),
    )
    row = match_market_candidates([_candidate("market-a")], [moved])[0]

    assert row.matched_game_pk is None
    assert row.exclusion_reason == "no_schedule_match"
    assert row.slug_orientation is None
    assert row.away_team is None
    assert row.home_team is None


def test_unknown_and_all_star_slugs_have_explicit_reasons() -> None:
    unknown, all_star = match_market_candidates(
        [
            _candidate("unknown", away="xyz"),
            _candidate("all-star", away="al", home="nl"),
        ],
        [_game(2001)],
    )

    assert unknown.exclusion_reason == "unknown_team_slug"
    assert all_star.exclusion_reason == "non_team_all_star"


def test_unique_nonfinal_schedule_record_is_audited_but_not_matched() -> None:
    postponed = _game(2001, final=False, doubleheader="S")
    row = match_market_candidates([_candidate("market-a")], [postponed])[0]

    assert row.matched_game_pk is None
    assert row.exclusion_reason == "nonfinal_game"
    assert row.schedule_matches == (postponed,)
    assert row.schedule_matches[0].status_code == "DR"
    assert row.schedule_matches[0].doubleheader == "S"


def test_same_date_matchup_doubleheader_is_never_guessed() -> None:
    games = [
        _game(2001, doubleheader="Y", game_number=1),
        _game(2002, doubleheader="Y", game_number=2),
    ]
    row = match_market_candidates([_candidate("market-a")], games)[0]

    assert row.matched_game_pk is None
    assert row.exclusion_reason == "multiple_schedule_matches"
    assert [game.game_pk for game in row.schedule_matches] == [2001, 2002]
    assert [game.game_number for game in row.schedule_matches] == [1, 2]


def test_reverse_order_split_doubleheader_is_still_ambiguous() -> None:
    games = [
        _game(2001, away="stl", home="bos", doubleheader="S", game_number=1),
        _game(2002, away="stl", home="bos", doubleheader="S", game_number=2),
    ]
    row = match_market_candidates(
        [_candidate("bos-stl", away="bos", home="stl")], games
    )[0]

    assert row.matched_game_pk is None
    assert row.exclusion_reason == "multiple_schedule_matches"
    assert row.slug_orientation == "reversed"
    assert row.away_team == MLB_TEAMS["stl"]
    assert row.home_team == MLB_TEAMS["bos"]
    assert [game.game_pk for game in row.schedule_matches] == [2001, 2002]


def test_ordered_first_keeps_opposite_home_games_distinct() -> None:
    candidates = [
        _candidate("cin-sf", away="cin", home="sf"),
        _candidate("sf-cin", away="sf", home="cin"),
    ]
    games = [
        _game(2001, away="cin", home="sf"),
        _game(2002, away="sf", home="cin"),
    ]

    rows = match_market_candidates(candidates, games)

    assert [row.matched_game_pk for row in rows] == [2001, 2002]
    assert all(row.is_matched for row in rows)
    assert [row.slug_orientation for row in rows] == ["official", "official"]
    assert [tuple(game.game_pk for game in row.schedule_matches) for row in rows] == [
        (2001,),
        (2002,),
    ]
    assert_one_to_one_matches(rows)


def test_duplicate_market_to_game_assignment_is_audited_and_fails_gate() -> None:
    rows = match_market_candidates(
        [_candidate("market-a"), _candidate("market-b")], [_game(2001)]
    )

    assert [row.exclusion_reason for row in rows] == [
        "duplicate_market_to_game_mapping",
        "duplicate_market_to_game_mapping",
    ]
    assert all(row.matched_game_pk is None for row in rows)
    assert all(row.schedule_matches[0].game_pk == 2001 for row in rows)
    with pytest.raises(ValueError, match="not one-to-one.*2001.*market-a.*market-b"):
        assert_one_to_one_matches(rows)


def test_one_to_one_gate_allows_distinct_matches_and_ordinary_exclusions() -> None:
    candidates = [
        _candidate("market-a"),
        _candidate("market-b", away="lad", home="sf"),
        _candidate("unmatched", away="mia", home="atl"),
    ]
    games = [_game(2001), _game(2002, away="lad", home="sf")]
    rows = match_market_candidates(candidates, games)

    assert [row.matched_game_pk for row in rows] == [2001, 2002, None]
    assert rows[2].exclusion_reason == "no_schedule_match"
    assert_one_to_one_matches(rows)
