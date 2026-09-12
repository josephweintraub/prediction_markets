from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from analysis.sports_game_dynamics.fixed_bins import (
    FixedBinContractError,
    fixed_bin_grid,
    price_bin_label,
    price_decile,
    require_complete_unique_grid,
)
from analysis.sports_game_dynamics.phase_contract import (
    PhaseContractError,
    classify_timestamp,
    included_in_boundary_sample,
    load_phase_contract,
    phase_contract_fingerprint,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "configs" / "game_dynamics"


def _contract_path(sport: str) -> Path:
    version = 2 if sport == "nba" else 1
    return CONTRACT_DIR / f"{sport}_phase_contract_v{version}.json"


def _boundaries() -> dict[str, datetime]:
    start = datetime(2026, 9, 10, 20, 0, tzinfo=timezone.utc)
    return {
        "actual_start_utc": start,
        "period_2_start_utc": start + timedelta(minutes=35),
        "period_3_start_utc": start + timedelta(minutes=75),
        "period_4_start_utc": start + timedelta(minutes=110),
        "actual_end_utc": start + timedelta(minutes=150),
    }


@pytest.mark.parametrize(
    ("sport", "minutes", "start_event", "period_event", "end_event"),
    (
        ("nfl", 15, "start of the opening kickoff play",
         "start of the first complete official play in the period",
         "timestamp of the last competitive play, with subsequent End Game evidence"),
        ("nba", 12,
         "first audited opening-tip action immediately after Q1 period/start and exact "
         "0-0 12:00 delay-of-game violations",
         "unique official period/start action at regulation clock",
         "unique official final-period period/end action at 00:00"),
    ),
)
def test_versioned_contracts_freeze_natural_quarters(
    sport: str, minutes: int, start_event: str, period_event: str, end_event: str
) -> None:
    path = _contract_path(sport)
    contract = load_phase_contract(path)

    assert contract.sport == sport
    assert contract.regulation_periods == 4
    assert contract.regulation_period_minutes == minutes
    assert contract.actual_start_event == start_event
    assert contract.period_boundary_event == period_event
    assert contract.actual_end_event == end_event
    assert contract.overtime_policy == "fold_into_final_phase"
    assert contract.tie_policy == "exclude_without_unique_winner"
    assert contract.nonstandard_game_policy == "retain_in_audit_exclude_from_core"
    assert [phase.key for phase in contract.analysis_phases] == [
        "pregame",
        "quarter_1",
        "quarter_2",
        "quarter_3",
        "quarter_4_plus",
    ]
    assert [sample.key for sample in contract.boundary_samples] == [
        "literal",
        "exclude_within_30s",
    ]
    fingerprint = phase_contract_fingerprint(path)
    assert fingerprint["bytes"] == path.stat().st_size
    assert len(fingerprint["sha256"]) == 64


def test_nba_v1_remains_loadable_but_v2_binds_the_audited_cache() -> None:
    v1 = load_phase_contract(CONTRACT_DIR / "nba_phase_contract_v1.json")
    v2 = load_phase_contract(CONTRACT_DIR / "nba_phase_contract_v2.json")

    assert v1.contract_version == 1
    assert v1.audit_scope is None
    assert v2.contract_version == 2
    assert v2.audit_scope == {
        "cache_inventory_name": "2026-09-12_nba_official_v1",
        "cache_inventory_sha256": "7adac545cb08071305d05385eb07514e25d6712a0a170ecea36f2d3dc490be31",
        "cache_inventory_sha256_method": (
            "SHA-256 of sorted UTF-8 <relative_path>\\t<byte_count>\\t"
            "<file_sha256>\\n records"
        ),
        "candidate_artifact_sha256": "ac4eb0236becbccb3a38bfb7401176a06eb529408c11ff5c1dd93329fe9f948f",
        "candidate_date_from": "2024-10-22",
        "candidate_date_to": "2026-06-13",
        "candidate_market_rows": 2796,
        "double_overtime_games": 4,
        "matched_completed_date_from": "2024-10-22",
        "matched_completed_date_to": "2025-10-14",
        "matched_completed_games": 1365,
        "opening_failure_game_id": "0022400887",
        "opening_rule_failures": 1,
        "opening_rule_passes": 1364,
        "overtime_games": 70,
        "play_by_play_resources": 1365,
        "reconciled_timing_games": 1363,
        "schedule_resources": 2,
        "schedule_score_mismatch_game_id": "0022400072",
        "schedule_score_mismatches": 1,
        "schema_version": 1,
        "single_overtime_games": 66,
        "spurious_period_5_games": 0,
    }


@pytest.mark.parametrize("mutation", ("missing", "extra", "bad_sha", "bad_count"))
def test_nba_v2_audit_scope_fails_closed(
    mutation: str, tmp_path: Path
) -> None:
    source = CONTRACT_DIR / "nba_phase_contract_v2.json"
    payload = json.loads(source.read_text(encoding="utf-8"))
    if mutation == "missing":
        payload["audit_scope"].pop("play_by_play_resources")
    elif mutation == "extra":
        payload["audit_scope"]["unexpected"] = 1
    elif mutation == "bad_sha":
        payload["audit_scope"]["cache_inventory_sha256"] = "bad"
    else:
        payload["audit_scope"]["opening_rule_passes"] -= 1
    target = tmp_path / f"bad-{mutation}.json"
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    with pytest.raises(PhaseContractError, match="audit_scope"):
        load_phase_contract(target)


@pytest.mark.parametrize("sport", ("nfl", "nba"))
def test_exact_phase_boundaries_and_final_equality(sport: str) -> None:
    contract = load_phase_contract(_contract_path(sport))
    boundaries = _boundaries()
    start = boundaries["actual_start_utc"]

    cases = (
        (start - timedelta(microseconds=1), "pregame"),
        (start, "quarter_1"),
        (boundaries["period_2_start_utc"], "quarter_2"),
        (boundaries["period_3_start_utc"], "quarter_3"),
        (boundaries["period_4_start_utc"], "quarter_4_plus"),
        (boundaries["actual_end_utc"], "quarter_4_plus"),
        (boundaries["actual_end_utc"] + timedelta(microseconds=1), "post_final"),
    )
    assert [classify_timestamp(contract, boundaries, value) for value, _ in cases] == [
        expected for _, expected in cases
    ]


def test_boundary_sensitivity_is_inclusive_and_does_not_reassign() -> None:
    contract = load_phase_contract(_contract_path("nba"))
    boundaries = _boundaries()
    boundary = boundaries["period_3_start_utc"]

    assert included_in_boundary_sample(
        contract, boundaries, boundary, "literal"
    )
    assert not included_in_boundary_sample(
        contract,
        boundaries,
        boundary - timedelta(seconds=30),
        "exclude_within_30s",
    )
    assert not included_in_boundary_sample(
        contract,
        boundaries,
        boundary + timedelta(seconds=30),
        "exclude_within_30s",
    )
    assert included_in_boundary_sample(
        contract,
        boundaries,
        boundary + timedelta(seconds=30, microseconds=1),
        "exclude_within_30s",
    )
    assert classify_timestamp(
        contract, boundaries, boundary - timedelta(seconds=30)
    ) == "quarter_2"


def test_boundaries_fail_closed_on_missing_or_nonchronological_values() -> None:
    contract = load_phase_contract(_contract_path("nfl"))
    missing = _boundaries()
    del missing["period_3_start_utc"]
    with pytest.raises(PhaseContractError, match="Boundary keys mismatch"):
        classify_timestamp(contract, missing, datetime.now(timezone.utc))

    reversed_values = _boundaries()
    reversed_values["period_3_start_utc"] = reversed_values["period_2_start_utc"]
    with pytest.raises(PhaseContractError, match="strictly increasing"):
        classify_timestamp(contract, reversed_values, datetime.now(timezone.utc))

    naive = _boundaries()
    naive["actual_end_utc"] = naive["actual_end_utc"].replace(tzinfo=None)
    with pytest.raises(PhaseContractError, match="timezone-aware"):
        classify_timestamp(contract, naive, datetime.now(timezone.utc))


def test_fixed_bin_grid_is_complete_for_five_analysis_phases() -> None:
    contract = load_phase_contract(_contract_path("nfl"))
    grid = fixed_bin_grid(contract)

    assert grid.row_counts == {
        "closing_calibration": 22,
        "closing_paired_sensitivity": 11,
        "trade_phase_calibration": 100,
        "flb_tail_summary": 12,
    }
    assert grid.trade_phase_profile[0] == ("literal", "pregame", 1)
    assert grid.trade_phase_profile[-1] == (
        "exclude_within_30s",
        "quarter_4_plus",
        10,
    )
    assert grid.tail_summary[-1] == (
        "trade_phase",
        None,
        "exclude_within_30s",
        "quarter_4_plus",
    )
    assert price_decile(0) == 1
    assert price_decile(0.1) == 2
    assert price_decile(0.999) == 10
    assert price_decile(1) == 10
    assert price_bin_label(1) == "[0.0,0.1)"
    assert price_bin_label(10) == "[0.9,1.0]"


@pytest.mark.parametrize("value", (-0.01, 1.01, float("nan"), True, "0.5"))
def test_fixed_bin_assignment_rejects_invalid_probabilities(value: object) -> None:
    with pytest.raises(FixedBinContractError, match=r"in \[0, 1\]"):
        price_decile(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", (True, 1.0))
def test_price_bin_label_requires_exact_non_bool_integer(value: object) -> None:
    with pytest.raises(FixedBinContractError, match="must be an integer"):
        price_bin_label(value)  # type: ignore[arg-type]


def test_complete_grid_gate_rejects_duplicate_and_unknown_rows() -> None:
    contract = load_phase_contract(_contract_path("nba"))
    grid = fixed_bin_grid(contract)
    rows = [
        {"sample": sample, "phase": phase, "decile": decile}
        for sample, phase, decile in grid.trade_phase_profile
    ]
    require_complete_unique_grid(
        rows,
        ("sample", "phase", "decile"),
        grid.trade_phase_profile,
        "phase profile",
    )

    duplicate = rows[:-1] + [rows[0]]
    with pytest.raises(FixedBinContractError, match="duplicate grain"):
        require_complete_unique_grid(
            duplicate,
            ("sample", "phase", "decile"),
            grid.trade_phase_profile,
            "phase profile",
        )

    unknown = [dict(row) for row in rows]
    unknown[-1]["phase"] = "overtime"
    with pytest.raises(FixedBinContractError, match="grid mismatch"):
        require_complete_unique_grid(
            unknown,
            ("sample", "phase", "decile"),
            grid.trade_phase_profile,
            "phase profile",
        )


@pytest.mark.parametrize("value", (True, 1.0))
def test_complete_grid_gate_compares_grain_types_exactly(value: object) -> None:
    rows = [{"sample": "literal", "phase": "pregame", "decile": value}]
    with pytest.raises(FixedBinContractError, match="grid mismatch"):
        require_complete_unique_grid(
            rows,
            ("sample", "phase", "decile"),
            [("literal", "pregame", 1)],
            "phase profile",
        )


def test_complete_grid_gate_fails_cleanly_on_unhashable_grain() -> None:
    rows = [{"sample": "literal", "phase": "pregame", "decile": [1]}]
    with pytest.raises(FixedBinContractError, match="unhashable grain value"):
        require_complete_unique_grid(
            rows,
            ("sample", "phase", "decile"),
            [("literal", "pregame", 1)],
            "phase profile",
        )


def test_unknown_sample_fails_even_for_post_final_timestamp() -> None:
    contract = load_phase_contract(_contract_path("nfl"))
    boundaries = _boundaries()
    post_final = boundaries["actual_end_utc"] + timedelta(seconds=1)

    with pytest.raises(PhaseContractError, match="Unknown boundary-sample key"):
        included_in_boundary_sample(
            contract,
            boundaries,
            post_final,
            "unknown_sample",
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (("boundary_samples", 1, "exclude_within_seconds", 29), "inclusive ±30"),
        (("phases", 2, "key", "quarter_two"), "Phase keys"),
        (("phases", 4, "end_inclusive", False), "Phase intervals"),
        ((None, None, "regulation_periods", 3), "four regulation periods"),
    ),
)
def test_contract_loader_rejects_semantic_drift(
    tmp_path: Path, mutation: tuple[object, ...], message: str
) -> None:
    value = json.loads(_contract_path("nfl").read_text(encoding="utf-8"))
    group, index, key, replacement = mutation
    if group is None:
        value[key] = replacement
    else:
        value[group][index][key] = replacement
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(PhaseContractError, match=message):
        load_phase_contract(path)


def test_contract_loader_rejects_extra_fields(tmp_path: Path) -> None:
    value = json.loads(_contract_path("nba").read_text(encoding="utf-8"))
    value["provider"] = "not-frozen-here"
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(PhaseContractError, match=r"extra=\['provider'\]"):
        load_phase_contract(path)
