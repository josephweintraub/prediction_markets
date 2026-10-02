import argparse
import json

import duckdb
import matplotlib.axes
import pytest

from analysis.diagnostics import render_tennis_wallet_investigation as report
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint, quoted


def tennis_data():
    coverage = [dict(cohort=c, sample=s, accepted_events=1, n_fills=600,
                     n_pregame=0, n_live=600, n_post_end=0)
                for c in report.COHORTS for s in report.SAMPLES]
    curves = [dict(cohort=c, sample=s, evaluation_time=i / 50, weighting="equal_fill", bandwidth=.10,
                   d1_n=500, d10_n=501, d1_events=1, d10_events=1,
                   d1_error=-.02, d10_error=.03, spread_d10_minus_d1=.05,
                   suppressed=False, uncertainty_status="point_estimate_only")
              for c in report.COHORTS for s in report.SAMPLES for i in range(51)]
    tails = [dict(cohort=c, sample=s, weighting="equal_fill", time_bin=10, d1_n=500, d10_n=501,
                  d1_error=-.02, d10_error=.03, spread_d10_minus_d1=.05, suppressed=False)
             for c in report.COHORTS for s in report.SAMPLES]
    tails += [dict(cohort=c, sample=s, weighting="dollar", time_bin=10, d1_n=500, d10_n=501,
                   d1_error=.04, d10_error=-.02, spread_d10_minus_d1=-.06, suppressed=False)
              for c in report.COHORTS for s in report.SAMPLES]
    phases = [dict(sample=s, scheduled_phase=a, provider_phase=b, n_fills=600 if a == b == "live" else 0)
              for s in report.SAMPLES for a in ("pregame", "live", "post") for b in ("pregame", "live", "post")]
    profiles = [dict(cohort=c, sample=s, weighting="equal_fill", time_bin=10, price_bin=b,
                     n_fills=500, n_events=1, suppressed=False, mean_calibration=.01)
                for c in report.COHORTS for s in report.SAMPLES for b in range(1, 11)]
    offsets = {f"{stat}_{key}_offset_seconds": 60. for stat in ("min", "median", "mean", "max") for key in ("start", "end")}
    offsets.update({f"p90_absolute_{key}_offset_seconds": 60. for key in ("start", "end")})
    return dict(cohort_coverage=coverage, kernel_tail_curves=curves, tail_contrasts=tails,
                calibration_profile=profiles,
                event_cohort=[dict(event_slug="ao", grand_slam_name="Australian Open", is_grand_slam=True)],
                clock_phase_assignments=phases, clock_offset_summary=[offsets],
                legacy_duration_mismatches=[dict(event_slug="audit", archive_duration_mismatch=True)],
                clock_comparison_support=[dict(sample=s, same_scoped_fill_membership=True, n_phase_changed=0) for s in report.SAMPLES])


def wallet_data():
    profiles = [dict(sample=s, window_id="t99_100", sport=sport, buy_group=g, price_bin=b,
                     n_fills=500, n_events=1, suppressed=False, calibration_equal_fill=.01)
                for s in report.SAMPLES for sport in report.SPORTS for g in report.GROUPS for b in range(1, 11)]
    tails = [dict(sample=s, window_id="t99_100", sport=sport, buy_group=g, d1_n=500,
                  d10_n=500, spread_equal_fill=.02, spread_dollar=-.03, suppressed=False)
             for s in report.SAMPLES for sport in report.SPORTS for g in report.GROUPS]
    shares = [dict(sample=s, window_id="t99_100", sport=sport, measure=m, numerator_fills=10,
                   denominator_fills=100, fill_share=.1, support_status="descriptive_counts_no_minimum")
              for s in report.SAMPLES for sport in report.SPORTS for m in report.MEASURES]
    comparisons = [dict(sample=s, sport=sport, measure="winner_sell_prevalence", early_window_id="t80_90", late_window_id="t99_100",
                        early_numerator_fills=10, early_denominator_fills=100, late_numerator_fills=20, late_denominator_fills=100,
                        early_fill_share=.1, late_fill_share=.2, change_in_fill_share=.1)
                   for s in report.SAMPLES for sport in report.SPORTS]
    return dict(linked_buy_profile=profiles, linked_buy_tails=tails), dict(conditional_shares=shares, phase_comparisons=comparisons)


def publish_fixture(directory, data, counts=None, manifest_name="manifest.json"):
    directory.mkdir()
    outputs = {}
    con = duckdb.connect()
    for name, rows in data.items():
        path = directory / (name + ".parquet")
        json_path = directory / (name + ".json")
        json_path.write_text(json.dumps(rows))
        con.execute(f"COPY (SELECT * FROM read_json_auto('{quoted(json_path)}')) TO '{quoted(path)}' (FORMAT PARQUET)")
        outputs[path.name] = artifact_fingerprint(path)
    con.close()
    (directory / manifest_name).write_text(json.dumps(dict(status="complete", outputs=outputs, counts=counts or {},
                                                          schema_version=4, stage="atp_timing_cohort_audit_v4")))


def test_formatting_never_turns_missing_into_zero():
    assert report.number(None) == "withheld"
    assert report.number(-.032, scale=100, signed=True) == "-3.20"
    assert report.count(123456) == "123,456"
    assert report.tex("A&B_1%") == r"A\&B\_1\%"
    with pytest.raises(ValueError, match="Nonfinite"):
        report.number(float("nan"))


def test_duplicate_summary_key_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        report.indexed([dict(x=1), dict(x=1)], ("x",))


def test_source_fingerprint_and_complete_gate(tmp_path):
    directory = tmp_path / "source"
    publish_fixture(directory, {"small": [dict(x=1)]})
    _, data, consumed = report.load_stage(directory, ("small.parquet",))
    assert data["small"] == [dict(x=1)]
    assert "manifest.json" in consumed
    with (directory / "small.parquet").open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(ValueError, match="fingerprint mismatch"):
        report.load_stage(directory, ("small.parquet",))
    (directory / "manifest.json").write_text('{"status":"running"}')
    with pytest.raises(ValueError, match="Incomplete"):
        report.load_stage(directory, ("small.parquet",))


def test_tennis_saved_contract():
    data = tennis_data()
    report.validate_tennis(data)
    data["kernel_tail_curves"].pop()
    with pytest.raises(ValueError, match="grid"):
        report.validate_tennis(data)


def test_slam_coverage_is_derived_from_saved_membership():
    rows = [dict(event_slug="ao1", grand_slam_name="Australian Open", is_grand_slam=True),
            dict(event_slug="ao2", grand_slam_name="Australian Open", is_grand_slam=True),
            dict(event_slug="rg1", grand_slam_name="Roland-Garros", is_grand_slam=True),
            dict(event_slug="excluded", grand_slam_name="Roland-Garros", is_grand_slam=False)]
    assert report.grand_slam_coverage(rows) == {"Australian Open": 2, "Roland-Garros": 1}
    data = tennis_data()
    data["event_cohort"] = rows
    with pytest.raises(ValueError, match="metadata count"):
        report.validate_tennis(data)
    rows[0]["grand_slam_name"] = "Other tournament"
    with pytest.raises(ValueError, match="Unexpected"):
        report.grand_slam_coverage(rows)


@pytest.mark.parametrize("mutation, message", [
    (lambda d: d["cohort_coverage"][0].update(n_live=1), "reconcile"),
    (lambda d: d["clock_comparison_support"][0].update(same_scoped_fill_membership=False), "membership"),
    (lambda d: d["kernel_tail_curves"][0].update(d1_n=499), "suppression"),
    (lambda d: d["kernel_tail_curves"][0].update(d1_error=None), "finite"),
    (lambda d: d["kernel_tail_curves"][0].update(bandwidth=.2), "estimator"),
])
def test_tennis_invalid_evidence_fails_closed(mutation, message):
    data = tennis_data()
    mutation(data)
    with pytest.raises(ValueError, match=message):
        report.validate_tennis(data)


def test_wallet_suppression_and_conditional_denominator():
    wallet, reader = wallet_data()
    report.validate_wallet(wallet, reader)
    reader["conditional_shares"][0]["fill_share"] = .2
    with pytest.raises(ValueError, match="ratio"):
        report.validate_wallet(wallet, reader)


def test_saved_phase_change_must_reconcile():
    wallet, reader = wallet_data()
    reader["phase_comparisons"][0]["change_in_fill_share"] = .5
    with pytest.raises(ValueError, match="phase change"):
        report.validate_wallet(wallet, reader)


def test_dollar_spread_is_saved_and_uses_same_support_gate():
    wallet, reader = wallet_data()
    _, table = report.wallet_tables(wallet, reader)
    assert "dollar-weighted before/after spread" in table
    assert "-3.00" in table and "+2.00" in table
    assert "retrospectively selects focal losing-side BUYs" in table
    row = wallet["linked_buy_tails"][0]
    row.update(d1_n=499, suppressed=True, spread_equal_fill=None, spread_dollar=None)
    report.validate_wallet(wallet, reader)
    row["spread_dollar"] = 0
    with pytest.raises(ValueError, match="must be null"):
        report.validate_wallet(wallet, reader)
    tennis = tennis_data()
    table = report.tennis_tail_table(tennis["tail_contrasts"])
    assert r"\$ spread" in table and "-6.00" in table
    dollar = next(r for r in tennis["tail_contrasts"] if r["weighting"] == "dollar")
    dollar["d1_n"] = 501
    with pytest.raises(ValueError, match="changed fill support"):
        report.validate_tennis(tennis)
    wallet, reader = wallet_data()
    row = wallet["linked_buy_profile"][0]
    row.update(n_fills=499, suppressed=True, calibration_equal_fill=None)
    report.validate_wallet(wallet, reader)
    row["calibration_equal_fill"] = 0
    with pytest.raises(ValueError, match="must be null"):
        report.validate_wallet(wallet, reader)


def test_fixed_bins_have_no_connecting_paths_and_pdf_deterministic(tmp_path, monkeypatch):
    wallet, _ = wallet_data()
    rows = wallet["linked_buy_profile"]
    rows[0].update(n_fills=499, suppressed=True, calibration_equal_fill=None)
    calls = []
    original = matplotlib.axes.Axes.plot
    monkeypatch.setattr(matplotlib.axes.Axes, "plot", lambda self, *a, **kw: calls.append((a, kw)) or original(self, *a, **kw))
    first, second = tmp_path / "first.pdf", tmp_path / "second.pdf"
    evidence = report.wallet_figure(rows, first)
    report.wallet_figure(rows, second)
    assert not calls
    assert first.read_bytes() == second.read_bytes()
    assert len(evidence["wallet_bin_profile"]) == 360
    assert evidence["wallet_bin_profile"][0]["suppressed"]
    calls.clear()
    tennis = tennis_data()
    tennis["calibration_profile"][0].update(n_fills=499, suppressed=True, mean_calibration=None)
    evidence = report.tennis_profile_figure(tennis["calibration_profile"], tmp_path / "tennis.pdf")
    assert not calls
    assert len(evidence["tennis_bin_profile"]) == 80
    assert evidence["tennis_bin_profile"][0]["suppressed"]


def test_end_to_end_portable_atomic_report(tmp_path):
    tennis = tennis_data()
    wallet, reader = wallet_data()
    ao_counts = dict(accepted_frozen_ao_events=1, passed_boundary_gates_events=1, actual_timing_events=1,
                     first_point_delta_min_seconds=17, first_point_delta_max_seconds=94)
    publish_fixture(tmp_path / "tennis", tennis, dict(accepted_atp_events=1, grand_slam_events=1, provider_actual_events=1,
                                                     legacy_archive_duration_mismatches=1))
    publish_fixture(tmp_path / "source", {"actual_timing": [dict(competitive_chronology_valid=True)]}, ao_counts, "actual_timing_manifest.json")
    publish_fixture(tmp_path / "wallet", wallet)
    publish_fixture(tmp_path / "reader", reader)
    args = argparse.Namespace(tennis_dir=tmp_path / "tennis", source_dir=tmp_path / "source", wallet_dir=tmp_path / "wallet",
                              reader_dir=tmp_path / "reader", run_dir=tmp_path / "report")
    manifest = report.render(args)
    output = tmp_path / "report"
    source = (output / "tennis_wallet_investigation.tex").read_text()
    assert r"\begin{tabular}" in source and r"\toprule" in source
    assert r"{wallet_price_bins.pdf}" in source
    assert r"{tennis_price_bins.pdf}" in source
    assert "http" not in source and "fingerprint" not in source
    assert "No event has a certified second-exact first serve" in source
    assert "Tennis retains the frozen exposure-normalized fills and inferred direction" in source
    assert r"80--90\% versus final 1\%" in source
    assert r"T=(\text{trade UTC}-\text{start UTC})/(\text{end UTC}-\text{start UTC})" in source
    assert r"live fills satisfy $0\leq T\leq1$" in source
    assert "Grand Slam membership: 1 Australian Open and 0 Roland-Garros matches" in source
    for name, expected in manifest["outputs"].items():
        assert artifact_fingerprint(output / name) == expected
    with pytest.raises(FileExistsError):
        report.render(args)
