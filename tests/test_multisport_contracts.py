from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from analysis.multisport_game_dynamics.contracts import (
    ESPN_TIMING_SOURCE,
    ESPN_TIMING_SOURCE_STATUS,
    SPORT_CONFIGS,
    Phase,
    SportConfigError,
    get_sport_config,
    validate_registry,
    validate_sport_config,
)


EXPECTED = {
    "nhl": (
        "hockey/nhl",
        ("Pregame", "Period 1", "Period 2", "Period 3 and overtime/shootout"),
        3,
        "period_3_plus",
    ),
    "cbb": (
        "basketball/mens-college-basketball",
        ("Pregame", "First half", "Second half and overtime"),
        2,
        "half_2_plus",
    ),
    "cfb": (
        "football/college-football",
        ("Pregame", "Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4 and overtime"),
        4,
        "quarter_4_plus",
    ),
    "wnba": (
        "basketball/wnba",
        ("Pregame", "Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4 and overtime"),
        4,
        "quarter_4_plus",
    ),
    "epl": (
        "soccer/eng.1",
        ("Pregame", "First half", "Second half and stoppage time"),
        2,
        "half_2_plus",
    ),
    "atp": (
        "tennis/atp",
        ("Pregame", "First elapsed third", "Middle elapsed third", "Final elapsed third"),
        3,
        "elapsed_3",
    ),
    "wta": (
        "tennis/wta",
        ("Pregame", "First elapsed third", "Middle elapsed third", "Final elapsed third"),
        3,
        "elapsed_3",
    ),
    "ufc": (
        "mma/ufc",
        ("Pregame", "Round 1", "Round 2", "Round 3 and later rounds"),
        3,
        "round_3_plus",
    ),
}


def test_registry_has_only_requested_sports_and_explicit_source_status() -> None:
    assert set(SPORT_CONFIGS) == set(EXPECTED)
    for sport, config in SPORT_CONFIGS.items():
        assert config.sport == sport
        assert config.timing_source == ESPN_TIMING_SOURCE
        assert config.timing_source_status == ESPN_TIMING_SOURCE_STATUS
        if sport in {"atp", "wta"}:
            assert config.duration_source_status == "third_party_public_archive"
        else:
            assert config.duration_source is None


@pytest.mark.parametrize("sport", tuple(EXPECTED))
def test_espn_paths_phase_order_and_period_folding(sport: str) -> None:
    league_path, labels, fold_from, fold_into = EXPECTED[sport]
    config = get_sport_config(sport)

    base = f"/apis/site/v2/sports/{league_path}"
    assert config.espn_scoreboard_path == f"{base}/scoreboard"
    assert config.espn_summary_path == f"{base}/summary"
    assert config.phase_labels == labels
    assert tuple(phase.order for phase in config.phases) == tuple(
        range(1, len(labels) + 1)
    )
    assert config.fold_from_period == fold_from
    assert config.fold_into_phase == fold_into
    assert config.phase_for_period(fold_from).key == fold_into
    assert config.phase_for_period(fold_from + 4).key == fold_into
    if fold_from > 1:
        assert config.phase_for_period(fold_from - 1) == config.phases[fold_from - 1]


def test_registry_and_nested_values_are_immutable() -> None:
    with pytest.raises(TypeError):
        SPORT_CONFIGS["new"] = SPORT_CONFIGS["nhl"]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        SPORT_CONFIGS["nhl"].display_name = "Changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        SPORT_CONFIGS["nhl"].phases[0].label = "Changed"  # type: ignore[misc]


@pytest.mark.parametrize("period", (0, -1, 1.5, True))
def test_phase_for_period_requires_a_positive_integer(period: object) -> None:
    with pytest.raises(SportConfigError, match="positive integer"):
        SPORT_CONFIGS["nhl"].phase_for_period(period)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ({"timing_source_status": "official"}, "third_party_undocumented"),
        ({"espn_summary_path": "/apis/site/v2/sports/hockey/nfl/summary"}, "sibling"),
        ({"fold_from_period": 2}, "one live phase per period"),
        ({"fold_into_phase": "period_2"}, "final live phase"),
        (
            {
                "phases": (
                    Phase("pregame", "Pregame", 1),
                    Phase("period_1", "Period 1", 3),
                    Phase("period_2", "Period 2", 2),
                    Phase("period_3_plus", "Period 3 and overtime/shootout", 4),
                )
            },
            "contiguous",
        ),
    ),
)
def test_validation_fails_closed_on_inconsistent_declarations(
    mutation: dict[str, object], message: str
) -> None:
    invalid = replace(SPORT_CONFIGS["nhl"], **mutation)
    with pytest.raises(SportConfigError, match=message):
        validate_sport_config(invalid)


def test_registry_identity_and_lookup_fail_closed() -> None:
    with pytest.raises(SportConfigError, match="does not match"):
        validate_registry({"wrong": SPORT_CONFIGS["nhl"]})
    with pytest.raises(SportConfigError, match="canonical lowercase"):
        get_sport_config("NHL")
    with pytest.raises(SportConfigError, match="unsupported"):
        get_sport_config("mlb")
