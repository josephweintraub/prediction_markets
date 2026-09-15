"""Immutable source and phase declarations for additional sports.

These declarations identify candidate ESPN resources and natural game phases.
They do not certify ESPN's undocumented schemas or play taxonomies; each sport
still requires a reviewed, fingerprinted provider cache before estimation.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Mapping


ESPN_TIMING_SOURCE = "ESPN site API"
ESPN_TIMING_SOURCE_STATUS = "third_party_undocumented"
_ESPN_PATH_PREFIX = "/apis/site/v2/sports/"


class SportConfigError(ValueError):
    """Raised when a multisport declaration is internally inconsistent."""


@dataclass(frozen=True, slots=True)
class Phase:
    """One canonical display phase in analysis order."""

    key: str
    label: str
    order: int


@dataclass(frozen=True, slots=True)
class SportConfig:
    """Frozen provider and phase metadata for one sport.

    ``fold_from_period`` is the first provider period assigned to the final
    live phase. All higher provider periods are folded into that same phase.
    For example, NHL period 3, overtime, and a shootout all map to
    ``period_3_plus``.
    """

    sport: str
    display_name: str
    espn_scoreboard_path: str
    espn_summary_path: str
    timing_source: str
    timing_source_status: str
    period_unit: str
    phases: tuple[Phase, ...]
    fold_from_period: int
    fold_into_phase: str
    duration_source: str | None = None
    duration_source_status: str | None = None

    def phase_for_period(self, period: int) -> Phase:
        """Return the live analysis phase for a positive provider period."""

        if not isinstance(period, int) or isinstance(period, bool) or period < 1:
            raise SportConfigError("provider period must be a positive integer")
        phase_index = min(period, self.fold_from_period)
        return self.phases[phase_index]

    @property
    def phase_labels(self) -> tuple[str, ...]:
        """Return canonical labels in analysis order."""

        return tuple(phase.label for phase in self.phases)


def validate_sport_config(config: SportConfig) -> SportConfig:
    """Validate one declaration and return it unchanged."""

    if not isinstance(config, SportConfig):
        raise SportConfigError("config must be a SportConfig")
    if re.fullmatch(r"[a-z][a-z0-9_]*", config.sport) is None:
        raise SportConfigError(f"invalid sport key: {config.sport!r}")
    if not config.display_name or config.display_name != config.display_name.strip():
        raise SportConfigError("display_name must be a nonempty trimmed string")
    if not config.period_unit or config.period_unit != config.period_unit.strip():
        raise SportConfigError("period_unit must be a nonempty trimmed string")
    if config.timing_source != ESPN_TIMING_SOURCE:
        raise SportConfigError("ESPN paths require the explicit ESPN timing source")
    if config.timing_source_status != ESPN_TIMING_SOURCE_STATUS:
        raise SportConfigError(
            "ESPN timing must be labeled third_party_undocumented"
        )
    if (config.duration_source is None) != (config.duration_source_status is None):
        raise SportConfigError("duration source and status must be declared together")
    if config.duration_source_status not in (None, "third_party_public_archive"):
        raise SportConfigError("unsupported duration source status")

    scoreboard_suffix = "/scoreboard"
    summary_suffix = "/summary"
    scoreboard = config.espn_scoreboard_path
    summary = config.espn_summary_path
    if (
        not scoreboard.startswith(_ESPN_PATH_PREFIX)
        or not scoreboard.endswith(scoreboard_suffix)
        or not summary.startswith(_ESPN_PATH_PREFIX)
        or not summary.endswith(summary_suffix)
        or scoreboard[: -len(scoreboard_suffix)] != summary[: -len(summary_suffix)]
    ):
        raise SportConfigError(
            "scoreboard and summary must be sibling ESPN site API paths"
        )

    phases = config.phases
    if not isinstance(phases, tuple) or len(phases) < 2:
        raise SportConfigError("phases must be an immutable tuple with pregame and live phases")
    if any(not isinstance(phase, Phase) for phase in phases):
        raise SportConfigError("every phase must be a Phase")
    if tuple(phase.order for phase in phases) != tuple(range(1, len(phases) + 1)):
        raise SportConfigError("phase order must be contiguous and one-based")
    if phases[0].key != "pregame" or phases[0].label != "Pregame":
        raise SportConfigError("the first phase must be canonical Pregame")
    if any(
        re.fullmatch(r"[a-z][a-z0-9_]*", phase.key) is None
        or not phase.label
        or phase.label != phase.label.strip()
        for phase in phases
    ):
        raise SportConfigError("phase keys and labels must be nonempty and canonical")
    if len({phase.key for phase in phases}) != len(phases):
        raise SportConfigError("phase keys must be unique")
    if len({phase.label for phase in phases}) != len(phases):
        raise SportConfigError("phase labels must be unique")

    if (
        not isinstance(config.fold_from_period, int)
        or isinstance(config.fold_from_period, bool)
        or config.fold_from_period < 1
    ):
        raise SportConfigError("fold_from_period must be a positive integer")
    if len(phases) != config.fold_from_period + 1:
        raise SportConfigError(
            "pregame plus one live phase per period through fold_from_period is required"
        )
    if config.fold_into_phase != phases[-1].key:
        raise SportConfigError("later periods must fold into the final live phase")
    return config


def _espn_config(
    *,
    sport: str,
    display_name: str,
    league_path: str,
    period_unit: str,
    phase_specs: tuple[tuple[str, str], ...],
    fold_from_period: int,
    duration_source: str | None = None,
    duration_source_status: str | None = None,
) -> SportConfig:
    phases = tuple(
        Phase(key=key, label=label, order=index)
        for index, (key, label) in enumerate(phase_specs, start=1)
    )
    base = f"{_ESPN_PATH_PREFIX}{league_path}"
    return validate_sport_config(
        SportConfig(
            sport=sport,
            display_name=display_name,
            espn_scoreboard_path=f"{base}/scoreboard",
            espn_summary_path=f"{base}/summary",
            timing_source=ESPN_TIMING_SOURCE,
            timing_source_status=ESPN_TIMING_SOURCE_STATUS,
            period_unit=period_unit,
            phases=phases,
            fold_from_period=fold_from_period,
            fold_into_phase=phases[-1].key,
            duration_source=duration_source,
            duration_source_status=duration_source_status,
        )
    )


_SPORT_CONFIGS = {
    "nhl": _espn_config(
        sport="nhl",
        display_name="NHL",
        league_path="hockey/nhl",
        period_unit="period",
        phase_specs=(
            ("pregame", "Pregame"),
            ("period_1", "Period 1"),
            ("period_2", "Period 2"),
            ("period_3_plus", "Period 3 and overtime/shootout"),
        ),
        fold_from_period=3,
    ),
    "cbb": _espn_config(
        sport="cbb",
        display_name="CBB",
        league_path="basketball/mens-college-basketball",
        period_unit="half",
        phase_specs=(
            ("pregame", "Pregame"),
            ("half_1", "First half"),
            ("half_2_plus", "Second half and overtime"),
        ),
        fold_from_period=2,
    ),
    "cfb": _espn_config(
        sport="cfb",
        display_name="CFB",
        league_path="football/college-football",
        period_unit="quarter",
        phase_specs=(
            ("pregame", "Pregame"),
            ("quarter_1", "Quarter 1"),
            ("quarter_2", "Quarter 2"),
            ("quarter_3", "Quarter 3"),
            ("quarter_4_plus", "Quarter 4 and overtime"),
        ),
        fold_from_period=4,
    ),
    "wnba": _espn_config(
        sport="wnba",
        display_name="WNBA",
        league_path="basketball/wnba",
        period_unit="quarter",
        phase_specs=(
            ("pregame", "Pregame"),
            ("quarter_1", "Quarter 1"),
            ("quarter_2", "Quarter 2"),
            ("quarter_3", "Quarter 3"),
            ("quarter_4_plus", "Quarter 4 and overtime"),
        ),
        fold_from_period=4,
    ),
    "epl": _espn_config(
        sport="epl",
        display_name="EPL",
        league_path="soccer/eng.1",
        period_unit="half",
        phase_specs=(
            ("pregame", "Pregame"),
            ("half_1", "First half"),
            ("half_2_plus", "Second half and stoppage time"),
        ),
        fold_from_period=2,
    ),
    "atp": _espn_config(
        sport="atp",
        display_name="ATP",
        league_path="tennis/atp",
        period_unit="elapsed third",
        phase_specs=(
            ("pregame", "Pregame"),
            ("elapsed_1", "First elapsed third"),
            ("elapsed_2", "Middle elapsed third"),
            ("elapsed_3", "Final elapsed third"),
        ),
        fold_from_period=3,
        duration_source="Tennis Abstract Jeff Sackmann archive mirror",
        duration_source_status="third_party_public_archive",
    ),
    "wta": _espn_config(
        sport="wta",
        display_name="WTA",
        league_path="tennis/wta",
        period_unit="elapsed third",
        phase_specs=(
            ("pregame", "Pregame"),
            ("elapsed_1", "First elapsed third"),
            ("elapsed_2", "Middle elapsed third"),
            ("elapsed_3", "Final elapsed third"),
        ),
        fold_from_period=3,
        duration_source="Tennis Abstract Jeff Sackmann archive mirror",
        duration_source_status="third_party_public_archive",
    ),
    "ufc": _espn_config(
        sport="ufc",
        display_name="UFC",
        league_path="mma/ufc",
        period_unit="round",
        phase_specs=(
            ("pregame", "Pregame"),
            ("round_1", "Round 1"),
            ("round_2", "Round 2"),
            ("round_3_plus", "Round 3 and later rounds"),
        ),
        fold_from_period=3,
    ),
}

SPORT_CONFIGS: Mapping[str, SportConfig] = MappingProxyType(_SPORT_CONFIGS)


def get_sport_config(sport: str) -> SportConfig:
    """Return the immutable declaration for one canonical lowercase sport key."""

    if not isinstance(sport, str) or sport != sport.strip().lower():
        raise SportConfigError("sport must be a canonical lowercase key")
    try:
        return SPORT_CONFIGS[sport]
    except KeyError as exc:
        raise SportConfigError(f"unsupported sport: {sport!r}") from exc


def validate_registry(configs: Mapping[str, SportConfig] = SPORT_CONFIGS) -> None:
    """Fail closed if registry keys and embedded sport identities disagree."""

    if not configs:
        raise SportConfigError("sport registry cannot be empty")
    for key, config in configs.items():
        validate_sport_config(config)
        if key != config.sport:
            raise SportConfigError(
                f"registry key {key!r} does not match config sport {config.sport!r}"
            )


validate_registry()
