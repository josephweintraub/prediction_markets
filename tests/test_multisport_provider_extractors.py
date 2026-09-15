from __future__ import annotations

from datetime import datetime, timezone

import pytest

from analysis.multisport_game_dynamics.provider_extractors import (
    ProviderDataError,
    derive_tennis_elapsed_thirds,
    extract_standard_wallclock_boundaries,
    extract_ufc_core_wallclock_boundaries,
    flatten_espn_standard,
    flatten_espn_tennis,
    flatten_espn_ufc,
    match_name_pair,
    name_match_score,
    normalize_name,
    pair_matches,
    split_matchup,
)


UTC = timezone.utc


def _competitor(identifier: str, name: str, side: str | None = None) -> dict[str, object]:
    value: dict[str, object] = {
        "id": identifier,
        "athlete": {"id": identifier, "displayName": name},
        "winner": identifier == "1",
    }
    if side is not None:
        value["homeAway"] = side
    return value


def _competition(identifier: str, names: tuple[str, str]) -> dict[str, object]:
    return {
        "id": identifier,
        "date": "2026-09-14T18:00:00Z",
        "competitors": [_competitor("1", names[0], "home"), _competitor("2", names[1], "away")],
        "status": {"type": {"state": "post", "detail": "Final"}},
    }


def test_name_normalization_and_scoring_are_literal_and_deterministic() -> None:
    assert normalize_name("  João-Félix O’Neill  ") == "joao felix o neill"
    assert name_match_score("Jannik Sinner", "JANNIK SINNER") == 3
    assert name_match_score("Sinner", "Jannik Sinner") == 2
    assert name_match_score("Felix Auger", "Mr Felix Auger Aliassime") == 1
    assert name_match_score("Sinner", "Simmer") is None


def test_pair_matching_accepts_unique_orientation_and_rejects_ambiguity() -> None:
    match = match_name_pair(
        ("J. Sinner", "Carlos Alcaraz"),
        ("Carlos Alcaraz", "J. Sinner"),
    )
    assert match is not None
    assert match.right_indices == (1, 0)
    assert match.scores == (3, 3)
    assert pair_matches(("Sinner", "Alcaraz"), ("Carlos Alcaraz", "Jannik Sinner"))
    assert pair_matches(("Williams", "Venus Williams"), ("Serena Williams", "Venus Williams"))
    assert not pair_matches(
        ("Alpha", "Beta"),
        ("X Alpha Y Beta", "Y Alpha X Beta"),
    )
    assert match_name_pair(("Same", "Same"), ("Same A", "Same B")) is None


def test_matchup_split_is_plain_and_handles_draw_question_suffix() -> None:
    assert split_matchup("Arsenal vs. Bournemouth") == ("Arsenal", "Bournemouth")
    assert split_matchup("Will Arsenal vs Bournemouth end in a draw?") == (
        "Arsenal",
        "Bournemouth",
    )
    assert split_matchup("Arsenal to win") is None


def test_flatten_standard_scoreboard_preserves_competitor_identity() -> None:
    payload = {
        "events": [
            {
                "id": "event-1",
                "name": "Arsenal at Bournemouth",
                "date": "2026-09-14T18:00:00Z",
                "competitions": [_competition("competition-1", ("Arsenal", "Bournemouth"))],
            }
        ]
    }
    rows = flatten_espn_standard(payload)
    assert len(rows) == 1
    assert rows[0].event_id == "event-1"
    assert rows[0].competition_id == "competition-1"
    assert rows[0].scheduled_start_utc == datetime(2026, 9, 14, 18, tzinfo=UTC)
    assert tuple(item.name for item in rows[0].competitors) == ("Arsenal", "Bournemouth")
    assert rows[0].status_state == "post"


def test_flatten_tennis_groupings_and_ufc_card_competitions() -> None:
    tennis = {
        "events": [
            {
                "id": "tournament-1",
                "name": "US Open",
                "date": "2026-09-14T18:00:00Z",
                "groupings": [
                    {
                        "grouping": {
                            "id": "mens-singles",
                            "displayName": "Men's Singles",
                        },
                        "competitions": [
                            _competition("match-1", ("Jannik Sinner", "Carlos Alcaraz"))
                        ],
                    }
                ],
            }
        ]
    }
    tennis_rows = flatten_espn_tennis(tennis)
    assert len(tennis_rows) == 1
    assert tennis_rows[0].grouping_id == "mens-singles"
    assert tennis_rows[0].grouping_name == "Men's Singles"

    ufc = {
        "events": [
            {
                "id": "card-1",
                "name": "UFC 999",
                "date": "2026-09-14T18:00:00Z",
                "competitions": [
                    _competition("bout-1", ("Fighter One", "Fighter Two")),
                    {
                        **_competition("bout-2", ("Fighter Three", "Fighter Four")),
                        "competitors": [
                            _competitor("3", "Fighter Three"),
                            _competitor("4", "Fighter Four"),
                        ],
                    },
                ],
            }
        ]
    }
    ufc_rows = flatten_espn_ufc(ufc)
    assert [row.competition_id for row in ufc_rows] == ["bout-1", "bout-2"]
    assert {row.event_id for row in ufc_rows} == {"card-1"}


def test_flatten_tennis_skips_only_incomplete_draw_slots() -> None:
    incomplete = _competition("match-tbd", ("Player One", "Player Two"))
    incomplete["competitors"][0].pop("athlete")
    payload = {
        "events": [{
            "id": "tournament-1",
            "name": "US Open",
            "date": "2026-09-14T18:00:00Z",
            "groupings": [{
                "grouping": {"id": "2", "displayName": "Women's Singles"},
                "competitions": [
                    incomplete,
                    _competition("match-valid", ("Iga Swiatek", "Coco Gauff")),
                ],
            }],
        }],
    }

    rows = flatten_espn_tennis(payload)
    assert [row.competition_id for row in rows] == ["match-valid"]


def test_flatteners_fail_closed_on_duplicate_ids_or_missing_groupings() -> None:
    duplicate = {
        "events": [
            {
                "id": "card-1",
                "name": "UFC 999",
                "competitions": [
                    _competition("same", ("A", "B")),
                    _competition("same", ("C", "D")),
                ],
            }
        ]
    }
    with pytest.raises(ProviderDataError, match="unique"):
        flatten_espn_ufc(duplicate)
    with pytest.raises(ProviderDataError, match="groupings"):
        flatten_espn_tennis({"events": [{"id": "x", "name": "Event"}]})


def _play(period: int, timestamp: str, text: str = "Shot made") -> dict[str, object]:
    return {"period": {"number": period}, "wallclock": timestamp, "text": text}


@pytest.mark.parametrize(
    "payload",
    (
        {
            "plays": [
                _play(1, "2026-09-14T18:00:00Z"),
                _play(1, "2026-09-14T18:20:00Z"),
                _play(2, "2026-09-14T18:30:00Z"),
                _play(2, "2026-09-14T19:00:00Z", "End Period"),
            ]
        },
        {
            "commentary": [
                _play(1, "2026-09-14T18:00:00Z"),
                _play(1, "2026-09-14T18:20:00Z"),
                _play(2, "2026-09-14T18:30:00Z"),
                _play(2, "2026-09-14T19:00:00Z", "Final whistle"),
            ]
        },
        {
            "drives": {
                "previous": [
                    {"plays": [_play(1, "2026-09-14T18:00:00Z"), _play(1, "2026-09-14T18:20:00Z")]},
                    {
                        "plays": [
                            _play(2, "2026-09-14T18:30:00Z"),
                            _play(2, "2026-09-14T19:00:00Z", "END GAME"),
                        ]
                    },
                ],
                "current": None,
            }
        },
    ),
)
def test_standard_boundaries_support_plays_commentary_and_cfb_drives(
    payload: dict[str, object],
) -> None:
    result = extract_standard_wallclock_boundaries(payload)
    assert dict(result.period_starts) == {
        1: datetime(2026, 9, 14, 18, tzinfo=UTC),
        2: datetime(2026, 9, 14, 18, 30, tzinfo=UTC),
    }
    assert result.actual_end_utc == datetime(2026, 9, 14, 19, tzinfo=UTC)


def test_standard_boundaries_skip_trailing_timeout_for_actual_end() -> None:
    result = extract_standard_wallclock_boundaries(
        {
            "plays": [
                _play(1, "2026-09-14T18:00:00Z"),
                _play(1, "2026-09-14T18:50:00Z", "Goal"),
                _play(1, "2026-09-14T18:51:00Z", "Official Timeout"),
            ]
        }
    )
    assert result.actual_end_utc == datetime(2026, 9, 14, 18, 50, tzinfo=UTC)


def test_standard_boundaries_are_order_agnostic_within_period() -> None:
    result = extract_standard_wallclock_boundaries(
        {
            "plays": [
                _play(1, "2026-09-14T18:20:00Z"),
                _play(1, "2026-09-14T18:00:00Z"),
                _play(2, "2026-09-14T19:00:00Z", "End Period"),
                _play(2, "2026-09-14T18:30:00Z"),
            ]
        }
    )
    assert dict(result.period_starts) == {
        1: datetime(2026, 9, 14, 18, tzinfo=UTC),
        2: datetime(2026, 9, 14, 18, 30, tzinfo=UTC),
    }
    assert result.actual_end_utc == datetime(2026, 9, 14, 19, tzinfo=UTC)


def test_standard_boundaries_ignore_unusable_administrative_metadata_rows() -> None:
    result = extract_standard_wallclock_boundaries(
        {
            "plays": [
                {"text": "Venue metadata"},
                {"period": {"number": 1}, "text": "Administrative note"},
                {"wallclock": "2026-09-14T17:59:00Z", "text": "Broadcast metadata"},
                _play(1, "2026-09-14T18:00:00Z"),
                _play(1, "2026-09-14T18:50:00Z", "Final whistle"),
            ]
        }
    )
    assert dict(result.period_starts) == {1: datetime(2026, 9, 14, 18, tzinfo=UTC)}
    assert result.actual_end_utc == datetime(2026, 9, 14, 18, 50, tzinfo=UTC)


@pytest.mark.parametrize(
    ("plays", "message"),
    (
        (
            [
                {
                    "period": {"number": 1},
                    "wallclock": "2026-09-14T18:00:00Z",
                    "date": "2026-09-14T18:01:00Z",
                    "text": "Play",
                },
                _play(1, "2026-09-14T18:30:00Z"),
            ],
            "contradictory timestamps",
        ),
        (
            [
                _play(1, "2026-09-14T18:10:00Z"),
                _play(1, "2026-09-14T18:20:00Z"),
                _play(2, "2026-09-14T18:00:00Z"),
                _play(2, "2026-09-14T18:30:00Z"),
            ],
            "starts are not strictly increasing",
        ),
        (
            [_play(1, "2026-09-14T18:00:00Z"), _play(3, "2026-09-14T18:10:00Z")],
            "not contiguous",
        ),
        (
            [
                {"period": {"number": 1}, "text": "Administrative note"},
                {"wallclock": "2026-09-14T18:00:00Z", "text": "Metadata"},
            ],
            "no usable literal timestamped period rows",
        ),
    ),
)
def test_standard_boundaries_fail_closed(
    plays: list[dict[str, object]], message: str
) -> None:
    with pytest.raises(ProviderDataError, match=message):
        extract_standard_wallclock_boundaries({"plays": plays})


def test_ufc_core_boundaries_require_hydrated_literal_plays() -> None:
    result = extract_ufc_core_wallclock_boundaries(
        {
            "items": [
                _play(1, "2026-09-14T18:00:00Z", "Round 1 starts"),
                _play(1, "2026-09-14T18:05:00Z", "End Period"),
                _play(2, "2026-09-14T18:06:00Z", "Round 2 starts"),
                _play(2, "2026-09-14T18:08:30Z", "Bout ends"),
            ]
        }
    )
    assert tuple(result.period_starts) == (1, 2)
    assert result.actual_end_utc == datetime(2026, 9, 14, 18, 8, 30, tzinfo=UTC)
    with pytest.raises(ProviderDataError, match="hydrated"):
        extract_ufc_core_wallclock_boundaries({"items": [{"$ref": "https://example.test/play/1"}]})


def test_ufc_core_ignores_walkouts_and_uses_period_zero_fight_over() -> None:
    result = extract_ufc_core_wallclock_boundaries(
        {
            "items": [
                {"period": {"number": 0}, "wallclock": "2026-09-14T17:55:00Z", "text": "Walkout"},
                _play(1, "2026-09-14T18:00:00Z", "Round Start"),
                _play(1, "2026-09-14T18:04:00Z", "Strike"),
                {"period": {"number": 0}, "wallclock": "2026-09-14T18:04:05Z", "text": "Fight Over"},
                {"period": {"number": 0}, "wallclock": "2026-09-14T18:06:00Z", "text": "Results"},
            ]
        }
    )
    assert tuple(result.period_starts) == (1,)
    assert result.actual_end_utc == datetime(2026, 9, 14, 18, 4, 5, tzinfo=UTC)


def test_tennis_elapsed_thirds_validate_duration_and_optional_end() -> None:
    result = derive_tennis_elapsed_thirds(
        "2026-09-14T18:00:00Z",
        90 * 60,
        observed_end_utc="2026-09-14T19:30:00Z",
    )
    assert dict(result.period_starts) == {
        1: datetime(2026, 9, 14, 18, tzinfo=UTC),
        2: datetime(2026, 9, 14, 18, 30, tzinfo=UTC),
        3: datetime(2026, 9, 14, 19, tzinfo=UTC),
    }
    assert result.actual_end_utc == datetime(2026, 9, 14, 19, 30, tzinfo=UTC)
    with pytest.raises(ProviderDataError, match="duration_seconds"):
        derive_tennis_elapsed_thirds("2026-09-14T18:00:00Z", 1.5)  # type: ignore[arg-type]
    with pytest.raises(ProviderDataError, match="disagree"):
        derive_tennis_elapsed_thirds(
            "2026-09-14T18:00:00Z",
            60,
            observed_end_utc="2026-09-14T18:02:00Z",
        )
