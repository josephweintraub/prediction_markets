"""Shared fixed-bin grids for descriptive sports calibration artifacts.

This module declares row grains and labels only.  It does not estimate means,
uncertainty, or favorite-longshot effects.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable, Mapping

from .phase_contract import PhaseContract


PRICE_DECILES = tuple(range(1, 11))
CLOSE_DEFINITIONS = ("primary", "sensitivity")
BOUNDARY_SAMPLES = ("literal", "exclude_within_30s")
MIN_CELL_N = 50


class FixedBinContractError(ValueError):
    """Raised when a fixed-bin artifact does not have its frozen grain."""


@dataclass(frozen=True)
class FixedBinGrid:
    closing_profile: tuple[tuple[str, str, int | None], ...]
    paired_profile: tuple[tuple[str, int | None], ...]
    trade_phase_profile: tuple[tuple[str, str, int], ...]
    tail_summary: tuple[tuple[str, str | None, str | None, str], ...]

    @property
    def row_counts(self) -> dict[str, int]:
        return {
            "closing_calibration": len(self.closing_profile),
            "closing_paired_sensitivity": len(self.paired_profile),
            "trade_phase_calibration": len(self.trade_phase_profile),
            "flb_tail_summary": len(self.tail_summary),
        }


def price_decile(probability: float) -> int:
    if (
        isinstance(probability, bool)
        or not isinstance(probability, (int, float))
        or not math.isfinite(float(probability))
        or probability < 0
        or probability > 1
    ):
        raise FixedBinContractError(
            f"Probability must be finite and in [0, 1], found {probability!r}"
        )
    return min(int(math.floor(float(probability) * 10)) + 1, 10)


def price_bin_label(decile: int) -> str:
    if (
        not isinstance(decile, int)
        or isinstance(decile, bool)
        or decile not in PRICE_DECILES
    ):
        raise FixedBinContractError(
            f"Price decile must be an integer from 1 through 10, found {decile!r}"
        )
    closing = "]" if decile == 10 else ")"
    return f"[{(decile - 1) / 10:.1f},{decile / 10:.1f}{closing}"


def fixed_bin_grid(contract: PhaseContract) -> FixedBinGrid:
    phase_keys = tuple(phase.key for phase in contract.analysis_phases)
    sample_keys = tuple(sample.key for sample in contract.boundary_samples)
    if sample_keys != BOUNDARY_SAMPLES:
        raise FixedBinContractError(
            f"Unsupported boundary-sample order: {sample_keys}"
        )
    closing = tuple(
        (definition, "overall", None) for definition in CLOSE_DEFINITIONS
    ) + tuple(
        (definition, "price_decile", decile)
        for definition in CLOSE_DEFINITIONS
        for decile in PRICE_DECILES
    )
    paired = (("overall", None),) + tuple(
        ("primary_price_decile", decile) for decile in PRICE_DECILES
    )
    trade = tuple(
        (sample, phase, decile)
        for sample in sample_keys
        for phase in phase_keys
        for decile in PRICE_DECILES
    )
    tails = tuple(
        ("closing", definition, None, "pregame_close")
        for definition in CLOSE_DEFINITIONS
    ) + tuple(
        ("trade_phase", None, sample, phase)
        for sample in sample_keys
        for phase in phase_keys
    )
    return FixedBinGrid(
        closing_profile=closing,
        paired_profile=paired,
        trade_phase_profile=trade,
        tail_summary=tails,
    )


def require_complete_unique_grid(
    rows: Iterable[Mapping[str, Any]],
    key_fields: tuple[str, ...],
    expected_keys: Iterable[tuple[Any, ...]],
    label: str,
) -> None:
    materialized = list(rows)
    keys: list[tuple[Any, ...]] = []
    typed_keys: list[tuple[tuple[type[Any], Any], ...]] = []
    for index, row in enumerate(materialized):
        missing = sorted(set(key_fields) - set(row))
        if missing:
            raise FixedBinContractError(f"{label} row {index} is missing keys: {missing}")
        key = tuple(row[field] for field in key_fields)
        keys.append(key)
        typed_keys.append(_type_sensitive_key(key, label, f"row {index}"))
    if len(typed_keys) != len(set(typed_keys)):
        raise FixedBinContractError(f"{label} has duplicate grain {key_fields}")

    expected_values = list(expected_keys)
    expected_typed = [
        _type_sensitive_key(key, label, f"expected key {index}")
        for index, key in enumerate(expected_values)
    ]
    if len(expected_typed) != len(set(expected_typed)):
        raise FixedBinContractError(f"{label} expected grid has duplicate grain")

    expected_by_typed = dict(zip(expected_typed, expected_values))
    actual_by_typed = dict(zip(typed_keys, keys))
    missing = sorted(
        (expected_by_typed[key] for key in set(expected_typed) - set(typed_keys)),
        key=repr,
    )
    extra = sorted(
        (actual_by_typed[key] for key in set(typed_keys) - set(expected_typed)),
        key=repr,
    )
    if missing or extra:
        raise FixedBinContractError(
            f"{label} grid mismatch; missing={missing}, extra={extra}"
        )


def _type_sensitive_key(
    key: tuple[Any, ...], label: str, origin: str
) -> tuple[tuple[type[Any], Any], ...]:
    typed: list[tuple[type[Any], Any]] = []
    for position, value in enumerate(key):
        try:
            hash(value)
        except TypeError as exc:
            raise FixedBinContractError(
                f"{label} {origin} has unhashable grain value at position "
                f"{position}: {value!r}"
            ) from exc
        typed.append((type(value), value))
    return tuple(typed)
