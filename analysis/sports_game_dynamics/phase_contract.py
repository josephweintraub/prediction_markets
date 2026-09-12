"""Strict phase contracts shared by quarter-based game-dynamics studies.

Schedule and play-by-play adapters remain sport-specific.  This module starts
only after an adapter has produced exact UTC boundary timestamps; it validates
the frozen phase declaration and applies its interval semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping


SCHEMA_VERSION = 1
SUPPORTED_CONTRACT_VERSIONS = {1, 2}
EXPECTED_PHASE_KEYS = (
    "pregame",
    "quarter_1",
    "quarter_2",
    "quarter_3",
    "quarter_4_plus",
    "post_final",
)
EXPECTED_BOUNDARY_KEYS = (
    "actual_start_utc",
    "period_2_start_utc",
    "period_3_start_utc",
    "period_4_start_utc",
    "actual_end_utc",
)
EXPECTED_SAMPLE_KEYS = ("literal", "exclude_within_30s")

TOP_LEVEL_KEYS = {
    "schema_version",
    "contract_version",
    "sport",
    "display_name",
    "regulation_periods",
    "regulation_period_minutes",
    "actual_start_event",
    "actual_end_event",
    "period_boundary_event",
    "overtime_policy",
    "tie_policy",
    "nonstandard_game_policy",
    "boundary_samples",
    "phases",
}
V2_AUDIT_SCOPE_KEYS = {
    "schema_version",
    "cache_inventory_name",
    "cache_inventory_sha256",
    "cache_inventory_sha256_method",
    "candidate_artifact_sha256",
    "candidate_date_from",
    "candidate_date_to",
    "candidate_market_rows",
    "matched_completed_date_from",
    "matched_completed_date_to",
    "schedule_resources",
    "play_by_play_resources",
    "matched_completed_games",
    "opening_rule_passes",
    "opening_rule_failures",
    "opening_failure_game_id",
    "schedule_score_mismatches",
    "schedule_score_mismatch_game_id",
    "reconciled_timing_games",
    "overtime_games",
    "single_overtime_games",
    "double_overtime_games",
    "spurious_period_5_games",
}
SAMPLE_KEYS = {
    "key",
    "label",
    "exclude_within_seconds",
    "inclusive",
}
PHASE_KEYS = {
    "key",
    "order",
    "label",
    "start_boundary",
    "start_inclusive",
    "end_boundary",
    "end_inclusive",
    "analysis_eligible",
}


class PhaseContractError(ValueError):
    """Raised when a phase contract or its timestamp inputs fail closed."""


@dataclass(frozen=True)
class BoundarySample:
    key: str
    label: str
    exclude_within_seconds: int | None
    inclusive: bool | None


@dataclass(frozen=True)
class Phase:
    key: str
    order: int
    label: str
    start_boundary: str | None
    start_inclusive: bool | None
    end_boundary: str | None
    end_inclusive: bool | None
    analysis_eligible: bool


@dataclass(frozen=True)
class PhaseContract:
    schema_version: int
    contract_version: int
    sport: str
    display_name: str
    regulation_periods: int
    regulation_period_minutes: int
    actual_start_event: str
    actual_end_event: str
    period_boundary_event: str
    overtime_policy: str
    tie_policy: str
    nonstandard_game_policy: str
    boundary_samples: tuple[BoundarySample, ...]
    phases: tuple[Phase, ...]
    audit_scope: Mapping[str, Any] | None = None

    @property
    def analysis_phases(self) -> tuple[Phase, ...]:
        return tuple(phase for phase in self.phases if phase.analysis_eligible)

    @property
    def required_boundaries(self) -> tuple[str, ...]:
        return EXPECTED_BOUNDARY_KEYS

    def phase_label(self, key: str) -> str:
        for phase in self.phases:
            if phase.key == key:
                return phase.label
        raise PhaseContractError(f"Unknown phase key: {key!r}")

    def sample(self, key: str) -> BoundarySample:
        for sample in self.boundary_samples:
            if sample.key == key:
                return sample
        raise PhaseContractError(f"Unknown boundary-sample key: {key!r}")


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PhaseContractError(f"{label} must be an object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise PhaseContractError(
            f"{label} keys mismatch; missing={missing}, extra={extra}"
        )


def _integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise PhaseContractError(f"{label} must be an integer")
    return value


def _nonempty(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise PhaseContractError(f"{label} must be a nonempty trimmed string")
    return value


def _nullable_bool(value: Any, label: str) -> bool | None:
    if value is not None and not isinstance(value, bool):
        raise PhaseContractError(f"{label} must be boolean or null")
    return value


def _nullable_boundary(value: Any, label: str) -> str | None:
    if value is None:
        return None
    boundary = _nonempty(value, label)
    if boundary not in EXPECTED_BOUNDARY_KEYS:
        raise PhaseContractError(f"{label} has unknown boundary {boundary!r}")
    return boundary


def _parse_sample(value: Any, index: int) -> BoundarySample:
    row = _object(value, f"boundary_samples[{index}]")
    _exact_keys(row, SAMPLE_KEYS, f"boundary_samples[{index}]")
    seconds = row["exclude_within_seconds"]
    if seconds is not None:
        seconds = _integer(seconds, f"boundary_samples[{index}].exclude_within_seconds")
        if seconds <= 0:
            raise PhaseContractError("Boundary exclusion seconds must be positive")
    return BoundarySample(
        key=_nonempty(row["key"], f"boundary_samples[{index}].key"),
        label=_nonempty(row["label"], f"boundary_samples[{index}].label"),
        exclude_within_seconds=seconds,
        inclusive=_nullable_bool(row["inclusive"], f"boundary_samples[{index}].inclusive"),
    )


def _parse_phase(value: Any, index: int) -> Phase:
    row = _object(value, f"phases[{index}]")
    _exact_keys(row, PHASE_KEYS, f"phases[{index}]")
    if not isinstance(row["analysis_eligible"], bool):
        raise PhaseContractError(f"phases[{index}].analysis_eligible must be boolean")
    return Phase(
        key=_nonempty(row["key"], f"phases[{index}].key"),
        order=_integer(row["order"], f"phases[{index}].order"),
        label=_nonempty(row["label"], f"phases[{index}].label"),
        start_boundary=_nullable_boundary(
            row["start_boundary"], f"phases[{index}].start_boundary"
        ),
        start_inclusive=_nullable_bool(
            row["start_inclusive"], f"phases[{index}].start_inclusive"
        ),
        end_boundary=_nullable_boundary(
            row["end_boundary"], f"phases[{index}].end_boundary"
        ),
        end_inclusive=_nullable_bool(
            row["end_inclusive"], f"phases[{index}].end_inclusive"
        ),
        analysis_eligible=row["analysis_eligible"],
    )


def _sha256(value: Any, label: str) -> str:
    digest = _nonempty(value, label)
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise PhaseContractError(f"{label} must be a lowercase SHA-256")
    return digest


def _iso_date(value: Any, label: str) -> date:
    raw = _nonempty(value, label)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise PhaseContractError(f"{label} must be an ISO date") from exc


def _parse_v2_audit_scope(value: Any) -> dict[str, Any]:
    scope = _object(value, "audit_scope")
    _exact_keys(scope, V2_AUDIT_SCOPE_KEYS, "audit_scope")
    if _integer(scope["schema_version"], "audit_scope.schema_version") != 1:
        raise PhaseContractError("audit_scope.schema_version must equal 1")
    _nonempty(scope["cache_inventory_name"], "audit_scope.cache_inventory_name")
    _sha256(scope["cache_inventory_sha256"], "audit_scope.cache_inventory_sha256")
    expected_method = (
        "SHA-256 of sorted UTF-8 <relative_path>\\t<byte_count>\\t"
        "<file_sha256>\\n records"
    )
    if scope["cache_inventory_sha256_method"] != expected_method:
        raise PhaseContractError("audit_scope cache-inventory method is unsupported")
    _sha256(scope["candidate_artifact_sha256"], "audit_scope.candidate_artifact_sha256")
    candidate_from = _iso_date(scope["candidate_date_from"], "audit_scope.candidate_date_from")
    candidate_to = _iso_date(scope["candidate_date_to"], "audit_scope.candidate_date_to")
    matched_from = _iso_date(
        scope["matched_completed_date_from"], "audit_scope.matched_completed_date_from"
    )
    matched_to = _iso_date(
        scope["matched_completed_date_to"], "audit_scope.matched_completed_date_to"
    )
    if candidate_to < candidate_from or matched_to < matched_from:
        raise PhaseContractError("audit_scope date ranges must be ordered")
    if matched_from < candidate_from or matched_to > candidate_to:
        raise PhaseContractError("audit_scope matched dates must lie inside candidate dates")
    count_keys = V2_AUDIT_SCOPE_KEYS - {
        "cache_inventory_name", "cache_inventory_sha256",
        "cache_inventory_sha256_method", "candidate_artifact_sha256",
        "candidate_date_from", "candidate_date_to",
        "matched_completed_date_from", "matched_completed_date_to",
        "opening_failure_game_id", "schedule_score_mismatch_game_id",
    }
    counts = {key: _integer(scope[key], f"audit_scope.{key}") for key in count_keys}
    if any(value < 0 for value in counts.values()):
        raise PhaseContractError("audit_scope counts must be nonnegative")
    if counts["candidate_market_rows"] < counts["matched_completed_games"]:
        raise PhaseContractError("audit_scope matched games exceed candidate rows")
    if counts["play_by_play_resources"] != counts["matched_completed_games"]:
        raise PhaseContractError("audit_scope PBP resources must equal matched games")
    if (
        counts["opening_rule_passes"] + counts["opening_rule_failures"]
        != counts["matched_completed_games"]
    ):
        raise PhaseContractError("audit_scope opening counts do not reconcile")
    if (
        counts["reconciled_timing_games"]
        != counts["opening_rule_passes"] - counts["schedule_score_mismatches"]
    ):
        raise PhaseContractError("audit_scope reconciled timing count does not reconcile")
    if (
        counts["single_overtime_games"] + counts["double_overtime_games"]
        != counts["overtime_games"]
    ):
        raise PhaseContractError("audit_scope overtime counts do not reconcile")
    for key in ("opening_failure_game_id", "schedule_score_mismatch_game_id"):
        game_id = _nonempty(scope[key], f"audit_scope.{key}")
        if re.fullmatch(r"[0-9]{10}", game_id) is None:
            raise PhaseContractError(f"audit_scope.{key} must be a ten-digit game id")
    return dict(scope)


def _validate_fixed_semantics(contract: PhaseContract) -> None:
    if contract.schema_version != SCHEMA_VERSION:
        raise PhaseContractError(
            f"Unsupported phase-contract schema_version: {contract.schema_version}"
        )
    if contract.contract_version not in SUPPORTED_CONTRACT_VERSIONS:
        raise PhaseContractError(
            f"Unsupported phase contract_version: {contract.contract_version}"
        )
    if contract.contract_version == 1 and contract.audit_scope is not None:
        raise PhaseContractError("Phase contract v1 cannot carry audit_scope")
    if contract.contract_version == 2:
        if contract.sport != "nba" or contract.audit_scope is None:
            raise PhaseContractError(
                "Phase contract v2 is supported only for NBA with audit_scope"
            )
    if not re.fullmatch(r"[a-z][a-z0-9_]*", contract.sport):
        raise PhaseContractError(f"Invalid sport key: {contract.sport!r}")
    if contract.regulation_periods != 4:
        raise PhaseContractError("Quarter-based contracts require four regulation periods")
    if contract.regulation_period_minutes <= 0:
        raise PhaseContractError("regulation_period_minutes must be positive")
    if contract.overtime_policy != "fold_into_final_phase":
        raise PhaseContractError("Overtime must fold into the final phase")
    if contract.tie_policy != "exclude_without_unique_winner":
        raise PhaseContractError("Ties must fail closed without a unique winner")
    if contract.nonstandard_game_policy != "retain_in_audit_exclude_from_core":
        raise PhaseContractError("Nonstandard games must remain audited and leave the core")

    samples = contract.boundary_samples
    if tuple(sample.key for sample in samples) != EXPECTED_SAMPLE_KEYS:
        raise PhaseContractError(
            f"Boundary samples must be ordered as {EXPECTED_SAMPLE_KEYS}"
        )
    literal, buffered = samples
    if literal.exclude_within_seconds is not None or literal.inclusive is not None:
        raise PhaseContractError("Literal boundary sample cannot exclude a time window")
    if buffered.exclude_within_seconds != 30 or buffered.inclusive is not True:
        raise PhaseContractError("Buffered boundary sample must exclude inclusive ±30 seconds")

    if tuple(phase.key for phase in contract.phases) != EXPECTED_PHASE_KEYS:
        raise PhaseContractError(f"Phase keys must be ordered as {EXPECTED_PHASE_KEYS}")
    if tuple(phase.order for phase in contract.phases) != tuple(range(1, 7)):
        raise PhaseContractError("Phase order must be contiguous from 1 through 6")
    expected_intervals = (
        (None, None, "actual_start_utc", False, True),
        ("actual_start_utc", True, "period_2_start_utc", False, True),
        ("period_2_start_utc", True, "period_3_start_utc", False, True),
        ("period_3_start_utc", True, "period_4_start_utc", False, True),
        ("period_4_start_utc", True, "actual_end_utc", True, True),
        ("actual_end_utc", False, None, None, False),
    )
    actual_intervals = tuple(
        (
            phase.start_boundary,
            phase.start_inclusive,
            phase.end_boundary,
            phase.end_inclusive,
            phase.analysis_eligible,
        )
        for phase in contract.phases
    )
    if actual_intervals != expected_intervals:
        raise PhaseContractError(
            "Phase intervals must be half-open, with final-play equality in "
            "quarter_4_plus and post-final strictly afterward"
        )


def load_phase_contract(path: str | Path) -> PhaseContract:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Phase contract does not exist: {source}")
    try:
        root = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PhaseContractError(f"Invalid phase-contract JSON: {exc}") from exc
    value = _object(root, "phase contract")
    contract_version = _integer(value.get("contract_version"), "contract_version")
    if contract_version not in SUPPORTED_CONTRACT_VERSIONS:
        raise PhaseContractError(f"Unsupported phase contract_version: {contract_version}")
    expected_keys = TOP_LEVEL_KEYS | ({"audit_scope"} if contract_version == 2 else set())
    _exact_keys(value, expected_keys, "phase contract")
    raw_samples = value["boundary_samples"]
    raw_phases = value["phases"]
    if not isinstance(raw_samples, list) or not isinstance(raw_phases, list):
        raise PhaseContractError("boundary_samples and phases must be arrays")
    contract = PhaseContract(
        schema_version=_integer(value["schema_version"], "schema_version"),
        contract_version=contract_version,
        sport=_nonempty(value["sport"], "sport"),
        display_name=_nonempty(value["display_name"], "display_name"),
        regulation_periods=_integer(value["regulation_periods"], "regulation_periods"),
        regulation_period_minutes=_integer(
            value["regulation_period_minutes"], "regulation_period_minutes"
        ),
        actual_start_event=_nonempty(value["actual_start_event"], "actual_start_event"),
        actual_end_event=_nonempty(value["actual_end_event"], "actual_end_event"),
        period_boundary_event=_nonempty(
            value["period_boundary_event"], "period_boundary_event"
        ),
        overtime_policy=_nonempty(value["overtime_policy"], "overtime_policy"),
        tie_policy=_nonempty(value["tie_policy"], "tie_policy"),
        nonstandard_game_policy=_nonempty(
            value["nonstandard_game_policy"], "nonstandard_game_policy"
        ),
        boundary_samples=tuple(
            _parse_sample(item, index) for index, item in enumerate(raw_samples)
        ),
        phases=tuple(_parse_phase(item, index) for index, item in enumerate(raw_phases)),
        audit_scope=(
            _parse_v2_audit_scope(value["audit_scope"])
            if contract_version == 2 else None
        ),
    )
    _validate_fixed_semantics(contract)
    return contract


def phase_contract_fingerprint(path: str | Path) -> dict[str, str | int]:
    source = Path(path).expanduser().resolve()
    payload = source.read_bytes()
    return {
        "path": str(source),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _validated_boundaries(
    contract: PhaseContract, values: Mapping[str, datetime]
) -> tuple[datetime, ...]:
    missing = sorted(set(contract.required_boundaries) - set(values))
    extra = sorted(set(values) - set(contract.required_boundaries))
    if missing or extra:
        raise PhaseContractError(
            f"Boundary keys mismatch; missing={missing}, extra={extra}"
        )
    ordered = tuple(values[key] for key in contract.required_boundaries)
    for key, value in zip(contract.required_boundaries, ordered):
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise PhaseContractError(f"{key} must be a timezone-aware datetime")
        if not math.isfinite(value.timestamp()):
            raise PhaseContractError(f"{key} is not a finite timestamp")
    if any(left >= right for left, right in zip(ordered, ordered[1:])):
        raise PhaseContractError("Game boundaries must be strictly increasing")
    return ordered


def classify_timestamp(
    contract: PhaseContract, boundaries: Mapping[str, datetime], value: datetime
) -> str:
    ordered = _validated_boundaries(contract, boundaries)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise PhaseContractError("Trade timestamp must be timezone-aware")
    start, period_2, period_3, period_4, end = ordered
    if value < start:
        return "pregame"
    if value < period_2:
        return "quarter_1"
    if value < period_3:
        return "quarter_2"
    if value < period_4:
        return "quarter_3"
    if value <= end:
        return "quarter_4_plus"
    return "post_final"


def included_in_boundary_sample(
    contract: PhaseContract,
    boundaries: Mapping[str, datetime],
    value: datetime,
    sample_key: str,
) -> bool:
    sample = contract.sample(sample_key)
    ordered = _validated_boundaries(contract, boundaries)
    phase = classify_timestamp(contract, boundaries, value)
    if phase == "post_final":
        return False
    if sample.exclude_within_seconds is None:
        return True
    distance = min(abs((value - boundary).total_seconds()) for boundary in ordered)
    if sample.inclusive:
        return distance > sample.exclude_within_seconds
    return distance >= sample.exclude_within_seconds
