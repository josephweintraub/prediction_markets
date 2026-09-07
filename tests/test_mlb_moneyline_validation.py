from __future__ import annotations

import sys
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

from match_games import MLB_TEAMS, match_market_candidates  # noqa: E402
from mlb_api import ScheduleGame  # noqa: E402
from validate_moneylines import (  # noqa: E402
    ACCEPTED_MLB_LABEL_TO_TEAM_ID,
    MLB_OUTCOME_LABELS_BY_SLUG,
    MoneylineTokenCompositionError,
    assert_unique_eligible_assignments,
    load_canonical_token_rows,
    validate_moneylines,
)


def _candidate(
    market_id: str,
    away: str = "nyy",
    home: str = "bos",
    game_date: str = "2025-07-04",
) -> dict[str, str]:
    return {"market_id": market_id, "away": away, "home": home, "date": game_date}


def _game(
    game_pk: int,
    away: str = "nyy",
    home: str = "bos",
    *,
    game_date: date = date(2025, 7, 4),
    winner: str = "home",
) -> ScheduleGame:
    away_won = winner == "away"
    return ScheduleGame(
        game_pk=game_pk,
        official_date=game_date,
        scheduled_start_utc=datetime.combine(
            game_date, datetime.min.time(), tzinfo=timezone.utc
        ),
        game_type="R",
        season=2025,
        away_team_id=MLB_TEAMS[away].team_id,
        away_team_name=MLB_TEAMS[away].name,
        home_team_id=MLB_TEAMS[home].team_id,
        home_team_name=MLB_TEAMS[home].name,
        status_abstract="Final",
        status_detailed="Final",
        status_code="F",
        is_completed=True,
        doubleheader="N",
        game_number=1,
        series_game_number=1,
        reschedule_date_utc=None,
        rescheduled_from_date=None,
        resume_date_utc=None,
        resumed_from_date=None,
        away_final_score=5 if away_won else 2,
        home_final_score=2 if away_won else 5,
        away_is_winner=away_won,
        home_is_winner=not away_won,
    )


def _tokens(
    market_id: str,
    away: str,
    home: str,
    winner: str,
) -> list[dict[str, str | None]]:
    winning_outcome = away if winner == "away" else home
    return [
        {
            "token_id": f"{market_id}-away",
            "market_id": market_id,
            "outcome": away,
            "winning_outcome": winning_outcome,
        },
        {
            "token_id": f"{market_id}-home",
            "market_id": market_id,
            "outcome": home,
            "winning_outcome": winning_outcome,
        },
    ]


def _validate_one(
    candidate: dict[str, str], game: ScheduleGame, tokens: list[dict[str, str | None]]
):
    matches = match_market_candidates([candidate], [game])
    return validate_moneylines([candidate], matches, tokens)


def test_valid_home_and_away_winners_produce_complete_dimensions() -> None:
    home_candidate = _candidate("home-win")
    away_candidate = _candidate(
        "away-win", away="lad", home="sf", game_date="2026-07-05"
    )
    games = [
        _game(2001),
        _game(
            2002,
            away="lad",
            home="sf",
            game_date=date(2026, 7, 5),
            winner="away",
        ),
    ]
    tokens = [
        *_tokens("home-win", "  YANKEES ", "Red   Sox", "home"),
        *_tokens("away-win", "Los Angeles Dodgers", "San Francisco Giants", "away"),
    ]

    result = validate_moneylines(
        [home_candidate, away_candidate],
        match_market_candidates([home_candidate, away_candidate], games),
        tokens,
    )

    assert [audit.is_eligible for audit in result.audits] == [True, True]
    home, away = result.eligible_markets
    assert (home.away_token_id, home.home_token_id) == (
        "home-win-away",
        "home-win-home",
    )
    assert (home.winning_team_id, home.winning_token_id, home.winning_outcome) == (
        MLB_TEAMS["bos"].team_id,
        "home-win-home",
        "Red   Sox",
    )
    assert away.winning_team_id == MLB_TEAMS["lad"].team_id
    assert away.winning_token_id == "away-win-away"
    assert_unique_eligible_assignments(result.eligible_markets)


def test_reversed_slug_order_uses_official_schedule_sides_for_tokens() -> None:
    candidate = _candidate("reversed", away="bos", home="nyy")
    game = _game(2001, away="nyy", home="bos", winner="away")
    result = _validate_one(
        candidate,
        game,
        _tokens("reversed", "Red Sox", "Yankees", "home"),
    )

    assert result.audits[0].is_eligible
    market = result.eligible_markets[0]
    assert market.away_team_id == MLB_TEAMS["nyy"].team_id
    assert market.home_team_id == MLB_TEAMS["bos"].team_id
    assert market.away_token_id == "reversed-home"
    assert market.home_token_id == "reversed-away"
    assert market.winning_token_id == "reversed-home"


def test_explicit_label_map_covers_short_and_full_names_for_all_teams() -> None:
    assert set(MLB_OUTCOME_LABELS_BY_SLUG) == set(MLB_TEAMS)
    for slug, labels in MLB_OUTCOME_LABELS_BY_SLUG.items():
        team_id = MLB_TEAMS[slug].team_id
        for label in labels:
            assert ACCEPTED_MLB_LABEL_TO_TEAM_ID[" ".join(label.split()).casefold()] == team_id


def test_known_one_token_pattern_is_excluded() -> None:
    candidate = _candidate("one-token")
    game = _game(2001)
    result = _validate_one(
        candidate,
        game,
        _tokens("one-token", "New York Yankees", "Boston Red Sox", "home")[:1],
    )

    assert result.eligible_markets == ()
    assert result.audits[0].exclusion_reason == "not_exactly_two_unique_tokens"


def test_duplicate_token_rows_are_excluded() -> None:
    candidate = _candidate("duplicate-token")
    game = _game(2001)
    tokens = _tokens(
        "duplicate-token", "New York Yankees", "Boston Red Sox", "home"
    )
    tokens.append(dict(tokens[0]))

    result = _validate_one(candidate, game, tokens)

    assert result.audits[0].exclusion_reason == "duplicate_or_invalid_token_rows"


def test_outcome_label_must_equal_official_team_name_without_aliasing() -> None:
    candidate = _candidate("short-label")
    result = _validate_one(
        candidate,
        _game(2001),
        _tokens("short-label", "NY Yankees", "Boston", "home"),
    )

    assert result.audits[0].exclusion_reason == "unrecognized_outcome_label"


def test_accepted_cross_team_labels_are_rejected() -> None:
    candidate = _candidate("cross-team")
    result = _validate_one(
        candidate,
        _game(2001),
        _tokens("cross-team", "Yankees", "Mets", "home"),
    )

    assert result.audits[0].exclusion_reason == "outcome_teams_do_not_match_game"


@pytest.mark.parametrize(
    ("winning_values", "reason"),
    [
        ([None, "Boston Red Sox"], "missing_winning_outcome"),
        (["New York Yankees", "Boston Red Sox"], "contradictory_winning_outcome"),
    ],
)
def test_missing_or_contradictory_resolution_is_excluded(
    winning_values: list[str | None], reason: str
) -> None:
    candidate = _candidate("bad-resolution")
    tokens = _tokens(
        "bad-resolution", "New York Yankees", "Boston Red Sox", "home"
    )
    for token, winner in zip(tokens, winning_values, strict=True):
        token["winning_outcome"] = winner

    result = _validate_one(candidate, _game(2001), tokens)

    assert result.audits[0].exclusion_reason == reason


def test_polymarket_and_official_winner_disagreement_is_excluded() -> None:
    candidate = _candidate("winner-mismatch")
    result = _validate_one(
        candidate,
        _game(2001, winner="home"),
        _tokens(
            "winner-mismatch", "New York Yankees", "Boston Red Sox", "away"
        ),
    )

    assert result.audits[0].exclusion_reason == "polymarket_mlb_winner_disagreement"
    assert result.audits[0].mlb_winning_team_id == MLB_TEAMS["bos"].team_id


def test_upstream_unmatched_reason_is_retained() -> None:
    candidate = _candidate("unmatched")
    matches = match_market_candidates([candidate], [])
    result = validate_moneylines(
        [candidate],
        matches,
        _tokens("unmatched", "New York Yankees", "Boston Red Sox", "home"),
    )

    assert result.audits[0].exclusion_reason == "no_schedule_match"
    assert result.eligible_markets == ()


def test_duplicate_candidate_ids_fail_before_auditing() -> None:
    candidate = _candidate("duplicate")
    with pytest.raises(ValueError, match="Duplicate candidate market IDs"):
        validate_moneylines([candidate, dict(candidate)], [], [])


def test_public_validator_invokes_eligible_uniqueness_gate() -> None:
    game = _game(2001)
    first = _candidate("first")
    second = _candidate("second")
    audits = (
        match_market_candidates([first], [game])[0],
        match_market_candidates([second], [game])[0],
    )
    tokens = [
        *_tokens("first", "Yankees", "Red Sox", "home"),
        *_tokens("second", "Yankees", "Red Sox", "home"),
    ]

    with pytest.raises(ValueError, match="game assignments are not unique"):
        validate_moneylines([first, second], audits, tokens)


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_parquet(path, index=False)


def _canonical_token_inputs() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    universe = [
        {
            "token_id": "token-away",
            "market_id": "candidate",
            "winning_outcome": "Red Sox",
        },
        {
            "token_id": "token-home",
            "market_id": "candidate",
            "winning_outcome": "Red Sox",
        },
        {
            "token_id": "irrelevant-token",
            "market_id": "outside",
            "winning_outcome": "Dodgers",
        },
    ]
    token_map = [
        {
            "token_id": "token-away",
            "condition_id": "candidate",
            "outcome": "Yankees",
        },
        {
            "token_id": "token-home",
            "condition_id": "candidate",
            "outcome": "Red Sox",
        },
        {
            "token_id": "irrelevant-token",
            "condition_id": "outside",
            "outcome": "Dodgers",
        },
    ]
    return universe, token_map


def _load_tokens(
    tmp_path: Path,
    universe: list[dict[str, object]],
    token_map: list[dict[str, object]],
    candidate_ids: list[str] | None = None,
):
    universe_path = tmp_path / "universe_tokens.parquet"
    token_map_path = tmp_path / "token_map.parquet"
    _write_parquet(universe_path, universe)
    _write_parquet(token_map_path, token_map)
    con = duckdb.connect()
    try:
        return load_canonical_token_rows(
            con,
            universe_path,
            token_map_path,
            candidate_ids or ["candidate"],
        )
    finally:
        con.close()


def test_canonical_token_loader_joins_and_limits_to_candidates(tmp_path: Path) -> None:
    universe, token_map = _canonical_token_inputs()

    rows = _load_tokens(tmp_path, universe, token_map)

    assert rows == (
        {
            "token_id": "token-away",
            "market_id": "candidate",
            "outcome": "Yankees",
            "winning_outcome": "Red Sox",
        },
        {
            "token_id": "token-home",
            "market_id": "candidate",
            "outcome": "Red Sox",
            "winning_outcome": "Red Sox",
        },
    )


def test_canonical_token_loader_requires_schema(tmp_path: Path) -> None:
    universe, token_map = _canonical_token_inputs()
    for row in universe:
        row.pop("winning_outcome")

    with pytest.raises(MoneylineTokenCompositionError, match="missing required columns"):
        _load_tokens(tmp_path, universe, token_map)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("null", "null or empty universe_tokens.winning_outcome"),
        ("duplicate_universe", "universe_tokens contains duplicate token IDs"),
        ("duplicate_map", "token_map contains duplicate token IDs"),
        ("missing_map", "token_map is missing relevant universe token IDs"),
        ("mismatched_market", "does not match universe market_id"),
        ("null_map", "null or empty token_map.outcome"),
    ],
)
def test_canonical_token_loader_fails_on_composition_errors(
    tmp_path: Path, mutation: str, message: str
) -> None:
    universe, token_map = _canonical_token_inputs()
    if mutation == "null":
        universe[0]["winning_outcome"] = None
    elif mutation == "duplicate_universe":
        universe.append(dict(universe[0]))
    elif mutation == "duplicate_map":
        token_map.append(dict(token_map[0]))
    elif mutation == "missing_map":
        token_map.pop(1)
    elif mutation == "mismatched_market":
        token_map[0]["condition_id"] = "different-market"
    else:
        token_map[0]["outcome"] = None

    with pytest.raises(MoneylineTokenCompositionError, match=message):
        _load_tokens(tmp_path, universe, token_map)


def test_canonical_token_loader_rejects_empty_relevant_set(tmp_path: Path) -> None:
    universe, token_map = _canonical_token_inputs()
    with pytest.raises(MoneylineTokenCompositionError, match="No universe_tokens rows"):
        _load_tokens(tmp_path, universe, token_map, ["absent-candidate"])


def test_canonical_token_loader_rejects_partial_candidate_coverage(tmp_path: Path) -> None:
    universe, token_map = _canonical_token_inputs()
    with pytest.raises(
        MoneylineTokenCompositionError,
        match="missing candidate market IDs.*absent-candidate",
    ):
        _load_tokens(
            tmp_path,
            universe,
            token_map,
            ["candidate", "absent-candidate"],
        )


@pytest.mark.parametrize("duplicate", ["market", "game", "token"])
def test_eligible_assignment_uniqueness_gate(duplicate: str) -> None:
    first = _validate_one(
        _candidate("first"),
        _game(2001),
        _tokens("first", "New York Yankees", "Boston Red Sox", "home"),
    ).eligible_markets[0]
    second = _validate_one(
        _candidate("second", away="lad", home="sf", game_date="2025-07-05"),
        _game(2002, away="lad", home="sf", game_date=date(2025, 7, 5)),
        _tokens("second", "Los Angeles Dodgers", "San Francisco Giants", "home"),
    ).eligible_markets[0]
    if duplicate == "market":
        second = replace(second, market_id=first.market_id)
    elif duplicate == "game":
        second = replace(second, game_pk=first.game_pk)
    else:
        second = replace(second, away_token_id=first.away_token_id)

    with pytest.raises(ValueError, match=f"{duplicate} assignments are not unique"):
        assert_unique_eligible_assignments([first, second])
