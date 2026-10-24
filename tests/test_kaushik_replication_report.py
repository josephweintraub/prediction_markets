"""Synthetic saved-estimate reporting checks; no real data or production work."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from analysis.kaushik_polymarket_replication import render_report as report


def estimate(value=1.25, *, n=1000, g=40, sports=False, cells=1):
    interval = {"estimate": value, "standard_error": .5,
                "ci95_low": value-.979981992270027, "ci95_high": value+.979981992270027}
    return {"suppressed": False, "suppression_reasons": [], "CR0": interval,
            "cluster_count_adjusted": dict(interval), "influence": {"flags": [], "effective_clusters": 40,
            "maximum_cluster_variance_share": .05}, "n_observations": n*cells, "n_clusters": g,
            "contributing_cluster_minimum": g,
            "cell_support": [{"cell": str(index), "n_observations": n, "n_clusters": g} for index in range(cells)],
            "paper_support": g>=30 if sports else None, "project_support": n>=500 if sports else None}


def model(column, clocks, effects, *, label="model"):
    grouped = {"group_summaries": {tail: {"r_squared_including_fixed_effects_by_target": {
        target: .1234 if tail == "D1" else .2345 for target in report.OUTCOMES}} for tail in ("D1", "D10")},
        "stacked_weighted_tss_r_squared_by_target": {target: .3456 for target in report.OUTCOMES}}
    return {"model_id": label+str(column), "spec": {"column": column, "label": label,
            "clocks": clocks, "report_clocks": clocks, "effects": effects}, "suppressed": False,
            "n_observations": 18000, "n_clusters": 360,
            "suppression_reasons": [], "joint": {"metadata": {"n": 18000, "cluster_count": 360,
            "grouped_r_squared": grouped, "r_squared_including_fixed_effects_by_target": {
                target: .3456 for target in report.OUTCOMES}}},
            "slopes": [{"outcome": target, "clock": clock, **estimate()} for target in report.OUTCOMES for clock in clocks]}


def synthetic_estimates():
    profiles = [{"scope": "all_markets", "phase": None, "bin": bin_, "outcome": target,
                 "mean_price": (bin_-.5)/10, "win_rate": .5,
                 **estimate(n=9000, g=360)} for target in report.OUTCOMES for bin_ in range(1, 11)]
    table2 = [model(1, ["xL"], []), model(2, ["xL"], ["cat_code"]),
              model(3, ["xR"], []), model(4, ["xR"], ["cat_code"]),
              model(5, ["xL", "xR"], ["cat_code", "price_code", "month_code"])]
    table3 = {}
    for panel, clock in (("L_gt1", "xL"), ("R_gt1", "xR")):
        table3[panel] = [model(1, [clock], [], label=panel), model(2, [clock], ["cat_code"], label=panel),
                         model(3, ["xL", "xR"], ["cat_code", "price_code", "month_code"], label=panel)]
        table3[panel][2]["spec"]["report_clocks"] = [clock]
        table3[panel][2]["slopes"] = [row for row in table3[panel][2]["slopes"] if row["clock"] == clock]
    a1 = model(1, ["xL", "xR"], ["claim_fe_code"], label="a1")
    a1["claim_support"] = {"claims": 150, "observations": 18000, "both_tail_claims": 20,
                           "both_tail_claim_observations": 2500}
    a2 = []
    for convention in report.CONVENTIONS:
        n = 4500 if convention in ("maker_buy", "taker_buy") else 9000
        a2.append({"convention": convention, "counts": {"rows": n*10, "clusters": 360, "tail_rows": n*2},
                   "profile_rows": [{**deepcopy(row), "n_observations": n, "scope": convention} for row in profiles],
                   "gap_rows": [{"outcome": target, **estimate(n=n, g=360, cells=2)} for target in report.OUTCOMES]})
    sports = {"coverage_qualification": "Available audited provider cache; historical date coverage is incomplete.",
              "observation_cache": {"sport_clock_columns": {"elapsed_seconds": "sport_elapsed_seconds",
                  "remaining_seconds": "sport_remaining_seconds"},
                  "reconciliation": {"sport_clock_mismatch_rows": 0},
                  "schema": "sport: string\ngame_key: string\ntimestamp: double\nsport_elapsed_seconds: double\nsport_remaining_seconds: double"},
              "coverage": [], "exclusions": [], "phase_counts": [], "scope_counts": [], "phase_rows": [], "profile_rows": [], "window_rows": []}
    for sport in report.SPORTS:
        sports["coverage"].append({"sport": sport, "games": 40, "markets": 40, "first_game_utc": "2025-01-01T00:00:00+00:00",
                                   "last_game_utc": "2026-03-24T00:00:00+00:00", "timing_quality": "audited provider clocks"})
        sports["exclusions"].append({"sport": sport, "joined_rows": 10010, "after_end_rows": 10,
                                     "admitted_rows": 10000, "games_with_archive_rows": 40})
    for scope in report.SCOPES:
        n, g = (4500, 360) if scope == "pooled" else (500, 40)
        sports["phase_counts"] += [{"scope": scope, "phase": phase, "n_observations": n*10, "n_games": g} for phase in report.PHASES]
        sports["scope_counts"].append({"scope": scope, "n_observations": n*20, "n_games": g})
        sports["profile_rows"] += [{"scope": scope, "phase": phase, "bin": bin_, "outcome": target,
                                     **estimate(n=n, g=g, sports=True)}
                                    for phase in report.PHASES for bin_ in range(1, 11) for target in report.OUTCOMES]
        sports["phase_rows"] += [{"scope": scope, "phase": phase, "outcome": target,
                                  **estimate(n=n, g=g, sports=True, cells=4 if phase == "in_play_minus_pregame" else 2)}
                                 for phase in (*report.PHASES, "in_play_minus_pregame") for target in report.OUTCOMES]
        sports["window_rows"] += [{"scope": scope, "window": window, "outcome": target, "panel": panel,
                                   "label": label, "window_order": order, **estimate(n=n, g=g, sports=True, cells=2)}
                                  for order, (panel, window, label) in enumerate(report.WINDOWS) for target in report.OUTCOMES]
    return {"schema_version": "kaushik_replication_estimates_v1", "status": "estimates_complete",
            "definitions": {"cutoff_utc": "2026-03-25T00:00:00Z", "uncertainty": {
                "primary": "event_cluster_CR0", "full_N_minus_k_CR1_used": False}},
            "sample": {"rows": 90000, "conditions": 400, "normalized_claims": 800, "clusters": 360,
                "unique_event_clusters": 360, "market_fallback_clusters": 0, "tail_rows": 18000,
                "duration_tail_rows": 18000, "duration_tail_clusters": 360, "future_ending_rows": 10,
                "first_execution_utc": "2022-11-21T12:00:00+00:00", "last_execution_utc": "2026-03-24T23:59:59+00:00",
                "source_row_counts": {"source": 180010, "baseline_all_roles": 180000, "excluded": 10, "primary_taker": 90000},
                "archive_exclusions": {"2025-02": [{"reason": "eligible", "side": "BUY", "is_maker": False, "rows": 90000, "precut_rows": 90000},
                    {"reason": "eligible", "side": "BUY", "is_maker": True, "rows": 90000, "precut_rows": 90000},
                    {"reason": "invalid_size", "side": "BUY", "is_maker": False, "rows": 10, "precut_rows": 10}]}},
            "categories": [{"category": category, "n_observations": 90000 if category == "Sports" else 0,
                            "share": 1.0 if category == "Sports" else 0.0} for category in report.CATEGORIES],
            "table1": {"profile_rows": profiles, "gap_rows": [{"outcome": target, **estimate(n=9000, g=360, cells=2)} for target in report.OUTCOMES]},
            "table2": table2, "table3": table3, "appendix_a1": a1, "appendix_a2": a2, "sports": sports}


def suppress(row, reason="observation_support_below_floor"):
    row.update(suppressed=True, suppression_reasons=[reason], CR0=None, cluster_count_adjusted=None, influence=None)


def synthetic_full_date_estimates():
    data = synthetic_estimates()
    data["sample_scope"] = "all_available_dates"
    data["definitions"].update(sample_scope="all_available_dates", cutoff_utc=None, cutoff_seconds=None)
    data["sample"].update(future_ending_rows=None, last_execution_utc="2026-06-23T23:59:59+00:00")
    for monthly in data["sample"]["archive_exclusions"].values():
        for row in monthly:
            row["scope_rows"] = row.pop("precut_rows")
    return data


def fixture_binding(directory, *, audit=False):
    first = "audit" if audit else "manifest"
    return {"directory": directory, first+"_path": directory+"/"+first+".json",
            first+"_sha256": "1"*64, "acceptance_path": directory+"/acceptance.json",
            "acceptance_sha256": "2"*64}


def fixture_wallet_filter():
    return {"wallet_policy": deepcopy(report.filters.POLICY), "input_representation": "parent_base_plus_mask",
            "parent_binding": fixture_binding("/mnt/data/runs/fixture_parent"),
            "mask_audit_binding": fixture_binding("/mnt/data/runs/fixture_mask", audit=True)}


def binding_identities(binding):
    return {name: {"path": binding[name+"_path"], "sha256": binding[name+"_sha256"]}
            for name in ("manifest", "acceptance")}


def synthetic_filtered_estimates():
    data = synthetic_full_date_estimates()
    data.update(schema_version="kaushik_replication_estimates_v2", wallet_filter=fixture_wallet_filter(),
                filtered_input_binding=fixture_binding("/mnt/data/runs/fixture_filtered"))
    data["definitions"]["wallet_filter"] = deepcopy(data["wallet_filter"])
    data["sample"]["rows"] -= 100
    data["sample"]["source_row_counts"].update(baseline_all_roles=179800, excluded=210, primary_taker=89900)
    monthly = data["sample"]["archive_exclusions"]["2025-02"]
    for row in monthly[:2]:
        row["rows"] -= 100
        row["scope_rows"] -= 100
        monthly.append({"reason": "exchange_wallet", "side": "BUY", "is_maker": row["is_maker"],
                        "rows": 100, "scope_rows": 100})
    next(row for row in data["categories"] if row["category"] == "Sports")["n_observations"] -= 100
    for row in data["table1"]["profile_rows"]:
        if row["bin"] == 5:
            row["n_observations"] -= 100
    for convention in data["appendix_a2"]:
        reduction = 50 if convention["convention"] in ("maker_buy", "taker_buy") else 100
        convention["counts"]["rows"] -= reduction
        for row in convention["profile_rows"]:
            if row["bin"] == 5:
                row["n_observations"] -= reduction
    return data


def save_stage(folder, data, *, base_binding=None, sports_binding=None):
    folder.mkdir()
    def save(path, value):
        raw = (json.dumps(value, sort_keys=True, allow_nan=False)+"\n").encode()
        path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()
    digest = save(folder/"estimates.json", data)
    manifest = {"schema_version": "kaushik_replication_estimate_stage_v1", "status": "estimates_complete",
                "source": {"head": "a"*40}, "estimates_json": {"sha256": digest}, "reconciliation": {
                key: True for key in ("all_inputs_reopened", "all_outputs_reopened", "common_duration_population",
                                     "buy_role_partition", "expected_grids_serialized")}}
    if data.get("sample_scope") == "all_available_dates":
        manifest.update(sample_scope="all_available_dates", preflight={"base_manifest": {
            "sample_scope": "all_available_dates", "definitions": {key: data["definitions"][key]
                for key in ("sample_scope", "cutoff_utc", "cutoff_seconds")},
            "input_preflight": {"contract": {"sample_scope": "all_available_dates"}}}})
    filtered = data.get("schema_version") == "kaushik_replication_estimates_v2"
    if filtered:
        base_binding = binding_identities(data["filtered_input_binding"])
        manifest.update(schema_version="kaushik_replication_estimate_stage_v2",
                        wallet_filter=deepcopy(data["wallet_filter"]),
                        filtered_input_binding=deepcopy(data["filtered_input_binding"]), base_binding=base_binding)
        manifest["preflight"].update(base_binding=base_binding)
        manifest["preflight"]["base_manifest"].update(schema_version=report.filters.FILTERED_INPUT_SCHEMA,
                                                     wallet_filter=deepcopy(data["wallet_filter"]))
    elif base_binding is not None:
        manifest["base_binding"] = base_binding
        manifest["preflight"]["base_binding"] = base_binding
    if sports_binding is not None:
        manifest["preflight"]["sports_binding"] = sports_binding
    manifest_digest = save(folder/"manifest.json", manifest)
    acceptance = {"schema_version": "kaushik_replication_estimate_acceptance_v1",
         "status": "estimates_reopened_accepted", "manifest_sha256": manifest_digest, "estimates_sha256": digest,
         "source_head": "a"*40, "all_outputs_reopened": True}
    if data.get("sample_scope") == "all_available_dates":
        acceptance["sample_scope"] = "all_available_dates"
    if filtered:
        acceptance.update(schema_version="kaushik_replication_estimate_acceptance_v2",
                          wallet_filter=deepcopy(data["wallet_filter"]),
                          filtered_input_binding=deepcopy(data["filtered_input_binding"]))
    save(folder/"acceptance.json", acceptance)


def fixture_review_report(report_dir):
    """Persist a clearly marked tiny-driver layout fixture, never real estimates."""
    from collections import Counter
    import duckdb
    from analysis.kaushik_polymarket_replication import run_estimates as driver
    from tests.test_kaushik_replication_driver import fixture
    report_dir = Path(report_dir)
    work = report_dir.with_name(report_dir.name + "_inputs")
    work.mkdir()
    con = duckdb.connect()
    try:
        records, manifest, binding = fixture(con, work)
        tally = Counter((row["is_maker"], row["side"]) for row in records)
        manifest["exclusions"] = {"2025-02": [{"reason": "eligible", "side": side, "is_maker": maker,
            "rows": n, "precut_rows": n, "duration_tail_rows": n} for (maker, side), n in tally.items()]}
        saved = driver.estimate_all(con, work/"score_stage", manifest, binding)
        saved["fixture_only"] = True
        save_stage(work/"accepted_fixture", saved)
        return report.render(work/"accepted_fixture", report_dir)
    finally:
        con.close()


class ReportTests(unittest.TestCase):
    def test_json_read_allows_only_access_time_metadata_to_change(self):
        raw = b'{"fixture": true}\n'
        before = SimpleNamespace(st_dev=1, st_ino=2, st_mode=0o100600, st_nlink=1,
            st_uid=1000, st_gid=1000, st_size=len(raw), st_mtime_ns=10, st_ctime_ns=20,
            st_atime=0.0, st_atime_ns=0)
        after = SimpleNamespace(**vars(before))
        after.st_atime = 1.0
        after.st_atime_ns = 1_000_000_000
        path = Mock(spec=Path)
        path.is_file.return_value = True
        path.is_symlink.return_value = False
        path.stat.side_effect = [before, after]
        path.read_bytes.return_value = raw

        value, digest = report._read_json(path)
        self.assertEqual(value, {"fixture": True})
        self.assertEqual(digest, hashlib.sha256(raw).hexdigest())

    def test_json_read_rejects_identity_or_content_change_metadata(self):
        raw = b'{"fixture": true}\n'
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_gid",
                  "st_size", "st_mtime_ns", "st_ctime_ns")
        before = SimpleNamespace(**dict.fromkeys(fields, 1))
        before.st_size = len(raw)
        for field in fields:
            with self.subTest(field=field):
                after = SimpleNamespace(**vars(before))
                setattr(after, field, getattr(after, field) + 1)
                path = Mock(spec=Path)
                path.is_file.return_value = True
                path.is_symlink.return_value = False
                path.stat.side_effect = [before, after]
                path.read_bytes.return_value = raw
                with self.assertRaisesRegex(report.ReportBlocked, "JSON changed during read"):
                    report._read_json(path)

    def test_same_size_estimate_content_drift_still_fails_receipt_hash_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "accepted"
            save_stage(stage, synthetic_estimates())
            path = stage / "estimates.json"
            raw = path.read_bytes()
            changed = raw.replace(b'"2026-03-25T00:00:00Z"', b'"2025-03-25T00:00:00Z"', 1)
            self.assertEqual(len(changed), len(raw))
            self.assertNotEqual(changed, raw)
            path.write_bytes(changed)
            with self.assertRaisesRegex(report.ReportBlocked, "fingerprint"):
                report.load_accepted_estimates(stage)

    def test_filtered_v2_source_retains_grid_and_truthful_effective_waterfall(self):
        data = synthetic_filtered_estimates()
        source = report.render_source(data)
        for label in ("exchange-wallet exclusion", "source-record exclusion", "four reviewed exchange addresses",
                      "Original eligible all-role records & 180,000", "Exchange-wallet records removed & 200",
                      "Retained eligible all-role records & 179,800", "Retained primary taker records & 89,900",
                      "Originally ineligible records & 10", "All-date eligibility and exchange-wallet waterfall",
                      "exchange wallet & Taker & BUY & 100"):
            self.assertIn(label, source)
        self.assertEqual(source.count(r"\includegraphics"), 21)
        for label in ("Table 1.", "Table 2A.", "Table 2B.", "Table 3.", "Table 4.", "Table 5.",
                      "Appendix A1.", "Appendix A2."):
            self.assertIn(label, source)
        self.assertNotIn("March 25", source)
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)/"filtered"
            save_stage(stage, data)
            _, binding = report.load_accepted_estimates(stage)
            self.assertEqual(binding["wallet_filter"], data["wallet_filter"])

    def test_filter_and_version_mutations_block_at_every_accepted_layer(self):
        with tempfile.TemporaryDirectory() as temporary:
            stage = Path(temporary)/"filtered"
            save_stage(stage, synthetic_filtered_estimates())
            data = json.loads((stage/"estimates.json").read_text())
            manifest = json.loads((stage/"manifest.json").read_text())
            acceptance = json.loads((stage/"acceptance.json").read_text())
            report.validate_estimate_filter_binding(data, manifest, acceptance)
            for layer in ("summary", "definitions", "stage", "receipt", "base"):
                for damage in ("missing", "policy", "representation", "parent", "mask", "bool_type"):
                    values = deepcopy((data, manifest, acceptance))
                    d, m, a = values
                    target = {"summary": d, "definitions": d["definitions"], "stage": m,
                              "receipt": a, "base": m["preflight"]["base_manifest"]}[layer]
                    if damage == "missing": target.pop("wallet_filter")
                    elif damage == "policy": target["wallet_filter"]["wallet_policy"]["addresses"].pop()
                    elif damage == "representation": target["wallet_filter"]["input_representation"] = "physical_copy"
                    elif damage == "parent": target["wallet_filter"]["parent_binding"]["manifest_sha256"] = "f"*64
                    elif damage == "mask": target["wallet_filter"]["mask_audit_binding"]["audit_path"] = "relative.json"
                    else: target["wallet_filter"]["wallet_policy"]["null_blank_match"] = 0
                    with self.subTest(layer=layer, damage=damage), self.assertRaises(ValueError):
                        report.validate_estimate_filter_binding(*values)
            for layer in range(3):
                values = deepcopy((data, manifest, acceptance))
                values[layer]["schema_version"] = values[layer]["schema_version"].replace("v2", "v1")
                with self.subTest(schema=layer), self.assertRaises(ValueError):
                    report.validate_estimate_filter_binding(*values)
            for layer in range(3):
                values = deepcopy((data, manifest, acceptance))
                values[layer]["filtered_input_binding"]["acceptance_sha256"] = "f"*64
                with self.subTest(binding=layer), self.assertRaises(ValueError):
                    report.validate_estimate_filter_binding(*values)

    def test_legacy_summary_cannot_be_relabelled_as_filtered(self):
        for layer in ("summary", "definitions"):
            for field, value in (("wallet_policy", {}), ("wallet_filter", fixture_wallet_filter()),
                                 ("filtered_input_binding", fixture_binding("/mnt/data/runs/fixture_filtered")),
                                 ("input_representation", "parent_base_plus_mask")):
                data = synthetic_full_date_estimates()
                (data if layer == "summary" else data["definitions"])[field] = value
                with self.subTest(layer=layer, field=field), self.assertRaises(report.ReportBlocked):
                    report.render_source(data)
        data = synthetic_full_date_estimates()
        data["sample"]["archive_exclusions"]["2025-02"][2]["reason"] = "exchange_wallet"
        with self.assertRaisesRegex(report.ReportBlocked, "legacy estimates"):
            report.render_source(data)
        for layer in ("summary", "definitions"):
            for field in ("wallet_policy", "input_representation"):
                data = synthetic_filtered_estimates()
                (data if layer == "summary" else data["definitions"])[field] = "conflicting alias"
                with self.subTest(filtered_layer=layer, field=field), self.assertRaises(report.ReportBlocked):
                    report.render_source(data)

    def test_descriptive_comparison_uses_saved_levels_and_no_difference_inference(self):
        original, filtered = synthetic_full_date_estimates(), synthetic_filtered_estimates()
        filtered["table1"]["profile_rows"][0]["CR0"].update(estimate=1.5)
        source = report.render_source(filtered, original)
        self.assertIn("Comparison. Original and exchange-filtered support", source)
        self.assertIn("Primary taker records & 90,000 & 89,900", source)
        self.assertIn("Payoff: D1 & +1.25 & +1.50 & +0.25", source)
        self.assertIn("no standard error or hypothesis test is calculated for that difference", source)
        self.assertEqual(source.count(r"\includegraphics"), 21)

    def test_filtered_report_assets_and_comparison_are_bound_to_original_parent_and_providers(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            original, filtered = synthetic_full_date_estimates(), synthetic_filtered_estimates()
            sports_binding = {"sports": list(report.SPORTS), "inputs": [
                {"role": "fixture_provider", "path": "/mnt/data/fixture_provider.parquet", "sha256": "3"*64}]}
            save_stage(folder/"original", original,
                       base_binding=binding_identities(filtered["wallet_filter"]["parent_binding"]),
                       sports_binding=sports_binding)
            save_stage(folder/"filtered", filtered, sports_binding=sports_binding)
            manifest = report.render(folder/"filtered", folder/"report", folder/"original")
            self.assertEqual(manifest["schema_version"], "kaushik_replication_report_v2")
            self.assertEqual(manifest["wallet_filter"], filtered["wallet_filter"])
            self.assertEqual(len(manifest["outputs"]), 22)
            self.assertIn("comparison_inputs", manifest)
            self.assertEqual((folder/"report"/"source.tex").read_text().count(r"\includegraphics"), 21)
            for name in report.figure_names():
                self.assertTrue((folder/"report"/"figures"/name).read_bytes().startswith(b"%PDF-"))
            for damage in ("failure", "parent", "providers", "definitions", "models"):
                with self.subTest(damage=damage):
                    candidate = folder/("original_"+damage)
                    bad_data = deepcopy(original)
                    parent = binding_identities(filtered["wallet_filter"]["parent_binding"])
                    providers = deepcopy(sports_binding)
                    if damage == "parent": parent["manifest"]["sha256"] = "f"*64
                    elif damage == "providers": providers["inputs"][0]["sha256"] = "f"*64
                    elif damage == "definitions": bad_data["definitions"]["record_weights"] = "different"
                    elif damage == "models": bad_data["table2"][0]["spec"]["label"] = "different"
                    save_stage(candidate, bad_data, base_binding=parent, sports_binding=providers)
                    if damage == "failure": (candidate/"failure.json").write_text("{}")
                    with patch.object(report, "plot_profile") as plot:
                        with self.assertRaises(report.ReportBlocked):
                            report.render(folder/"filtered", folder/("blocked_"+damage), candidate)
                        plot.assert_not_called()
                    self.assertFalse((folder/("blocked_"+damage)).exists())

    def test_filtered_admission_mutation_precedes_plotting_and_failure_stage_is_blocked(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            save_stage(folder/"filtered", synthetic_filtered_estimates())
            receipt_path = folder/"filtered"/"acceptance.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["wallet_filter"]["input_representation"] = "physical_copy"
            receipt_path.write_text(json.dumps(receipt))
            with patch.object(report, "plot_profile") as plot:
                with self.assertRaises(report.ReportBlocked):
                    report.render(folder/"filtered", folder/"blocked")
                plot.assert_not_called()
            self.assertFalse((folder/"blocked").exists())
            (folder/"filtered"/"failure.json").write_text("{}")
            with self.assertRaisesRegex(report.ReportBlocked, "failed estimate stage"):
                report.load_accepted_estimates(folder/"filtered")

    def test_full_date_source_labels_all_dates_and_preserves_all_outputs(self):
        data = synthetic_full_date_estimates()
        source = report.render_source(data)
        self.assertIn("full available-date sample", source)
        self.assertIn("no execution-date cutoff", source)
        self.assertIn("All-date eligibility waterfall", source)
        self.assertIn("Excluded by eligibility", source)
        self.assertIn("2026-06-23", source)
        self.assertEqual(source.count(r"\includegraphics"), 21)
        for misleading in ("March 25", "before the cutoff", "Pre-cutoff", "future-ending claims", "Same archive, cutoff"):
            self.assertNotIn(misleading, source)

    def test_full_date_definitions_and_waterfall_cannot_silently_retain_cutoff(self):
        mutations = [lambda data: data["definitions"].update(cutoff_utc="2026-03-25T00:00:00Z"),
                     lambda data: data["definitions"].update(cutoff_seconds=1774396800),
                     lambda data: data["sample"].update(future_ending_rows=0),
                     lambda data: data.update(sample_scope="paper_cutoff"),
                     lambda data: data["sample"]["archive_exclusions"]["2025-02"][0].update(scope_rows=89999),
                     lambda data: data["sample"]["archive_exclusions"]["2025-02"][0].update(precut_rows=90000),
                     lambda data: data["sample"]["archive_exclusions"]["2025-02"][0].update(reason="at_or_after_cutoff")]
        for index, mutation in enumerate(mutations):
            data = synthetic_full_date_estimates()
            mutation(data)
            with self.subTest(index=index), self.assertRaises(report.ReportBlocked):
                report.validate_estimates(data)

    def test_full_date_scope_must_match_estimate_stage_receipt_and_input_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory) / "full"
            save_stage(stage, synthetic_full_date_estimates())
            data, binding = report.load_accepted_estimates(stage)
            self.assertEqual(binding["sample_scope"], "all_available_dates")
            manifest = json.loads((stage / "manifest.json").read_text())
            receipt = json.loads((stage / "acceptance.json").read_text())
            mutations = [lambda m, a: m.pop("sample_scope"), lambda m, a: a.pop("sample_scope"),
                         lambda m, a: m.pop("preflight"),
                         lambda m, a: m["preflight"]["base_manifest"].update(sample_scope="paper_cutoff"),
                         lambda m, a: m["preflight"]["base_manifest"]["definitions"].update(sample_scope="paper_cutoff"),
                         lambda m, a: m["preflight"]["base_manifest"]["input_preflight"]["contract"].clear()]
            for index, mutation in enumerate(mutations):
                invalid_manifest, invalid_receipt = deepcopy(manifest), deepcopy(receipt)
                mutation(invalid_manifest, invalid_receipt)
                with self.subTest(index=index), self.assertRaises(report.ReportBlocked):
                    report.validate_sample_scope_binding(data, invalid_manifest, invalid_receipt)
            legacy = synthetic_estimates()
            self.assertEqual(report.validate_sample_scope_binding(legacy, {}, {}), "paper_cutoff")

    def test_complete_source_has_all_paper_outputs_and_material_definitions(self):
        source = report.render_source(synthetic_estimates())
        for text in ("Table 1.", "Table 2A.", "Table 2B.", "Table 3.", "Table 4.", "Table 5.",
                     "Appendix A1.", "Appendix A2.", "Source accounting", "eligibility waterfall",
                     "provider-covered", "approximate", "Unclassified", "binary64", "CR0", "$G/(G-1)$",
                     "no bot filter", "not independently certified", "500-record guard", "long-horizon",
                     r"\log_2(1+\mathrm{days})", "0.1234", "0.2345", "0.3456", "90,000",
                     "pp means percentage points", "FE means fixed effects", "$H=1$ for D10", "comparisons", "Observed games",
                     "P is normalized claim price", "Y is its eventual binary payout", "$t<s$", r"$s\leq t\leq e$"):
            self.assertIn(text, source)
        self.assertEqual(source.count(r"\includegraphics"), 21)
        for name in report.figure_names():
            self.assertIn("figures/"+name, source)
        for forbidden in ("https://", "fingerprint", r"\section{Conclusion", r"\section{Caveats", "Lines connect", "economic magnitude"):
            self.assertNotIn(forbidden, source)
        self.assertTrue(source.rstrip().endswith(r"\end{document}"))

    def test_nulls_and_project_support_are_withheld_with_reason_not_zero(self):
        value = synthetic_estimates()
        for row in value["sports"]["profile_rows"]:
            if row["scope"] == "mlb" and row["phase"] == "pregame" and row["bin"] == 1:
                row.update(n_observations=499, project_support=False)
                row["cell_support"][0]["n_observations"] = 499
                suppress(row)
        suppress(value["table2"][4]["slopes"][0], "unidentified_clock")
        source = report.render_source(value)
        self.assertIn(r"Pre & D1 & 499 & 40 & withheld & withheld & withheld: <500 records", source)
        self.assertNotIn("Pre & D1 & 499 & 40 & +0.00", source)
        self.assertIn("(5) Original: unidentified clock", source)

    def test_duration_notes_define_clock_orientation_without_game_progress_or_causal_claim(self):
        data = synthetic_estimates()
        for target in report.OUTCOMES:
            source = report._model_table(data, target)
            self.assertIn("Original duration is claim lifespan (constant within claim), not elapsed time or game progress", source)
            self.assertIn("Larger remaining time means farther from the endpoint proxy", source)
            self.assertIn("a positive remaining-time gap slope means the fitted gap decreases as remaining time runs down, conditional on controls", source)
        source = report.render_source(data)
        self.assertIn("endpoint-proxy final-day records retained", source)
        self.assertIn("endpoint-proxy final-day records excluded", source)
        self.assertIn(r"$\widetilde{Y-P}=-\widetilde P$", source)
        self.assertIn("payoff coefficients diagnose price paths, not independent calibration or causal FLB", source)

    def test_fixture_mark_is_not_research_findings(self):
        value = synthetic_estimates()
        value["fixture_only"] = True
        source = report.render_source(value)
        self.assertIn("SYNTHETIC FIXTURE --- NOT RESEARCH FINDINGS", source)
        self.assertNotIn("SYNTHETIC FIXTURE", report.render_source(synthetic_estimates()))

    def test_rank_withheld_regression_retains_original_sample_support_without_scores(self):
        import contextlib
        import io
        import duckdb
        import pyarrow as pa
        from analysis.kaushik_polymarket_replication import run_estimates as driver
        records = []
        for event in range(12):
            for tail in (0, 1):
                for category, clock in enumerate((.3, .7)):
                    price = ((.025, .06) if not tail else (.91, .96))[category]
                    for index in range(3):
                        payout = (event+index) % 2
                        records.append({"event_cluster": f"event:{event}", "tail": tail,
                            "cat_code": category+2*tail, "price_code": category+2*tail,
                            "month_code": tail, "xL": clock, "xR": .2+.1*index,
                            "payoff": 100*(payout-price), "roi": 100*(payout/price-1)})
        with tempfile.TemporaryDirectory() as directory:
            con = duckdb.connect()
            try:
                folder = Path(directory)
                con.register("absorbed_clock_fixture", pa.Table.from_pylist(records))
                cache = driver.group_cache(con, "absorbed_clock_fixture", folder/"absorbed_groups.parquet",
                    ("event_cluster", "tail", "cat_code", "price_code", "month_code"),
                    ("xL", "xR", "payoff", "roi"))
                spec = {**driver.model_specs()[1], "report_clocks": ["xL"]}
                with contextlib.redirect_stdout(io.StringIO()):
                    saved = driver.regression_model(con, cache, "absorbed_groups", spec,
                        "report_absorbed_clock", folder, {}, driver.ReadLedger())
                self.assertEqual((saved["n_observations"], saved["n_clusters"]), (144, 12))
                self.assertEqual(saved["joint"]["metadata"]["cluster_count"], 0)
                self.assertTrue(all(value is None for row in saved["joint"]["covariance_CR0"] for value in row))
                self.assertTrue(all(row["suppressed"] and row["CR0"] is None for row in saved["slopes"]))
                data = synthetic_estimates()
                data["table2"][1] = saved
                source = report.render_source(data)
                self.assertIn("Records & 18,000 & 144 & 18,000", source)
                self.assertIn("Event/market clusters & 360 & 12 & 360", source)
                self.assertIn("Original duration & +1.25 & withheld", source)
                self.assertIn("rank deficient residualized design", source)
                self.assertEqual(report._model_count(saved, "cluster_count"), "12")
                self.assertEqual(saved["joint"]["metadata"]["cluster_count"], 0)
            finally:
                con.close()

    def test_model_support_is_required_and_only_explicit_empty_sample_displays_zero(self):
        saved = model(1, ["xL"], [])
        saved["n_observations"] = saved["n_clusters"] = 0
        saved.update(joint=None, suppressed=True, suppression_reasons=["empty_estimation_sample"])
        self.assertEqual(report._model_count(saved, "n"), "0")
        self.assertEqual(report._model_count(saved, "cluster_count"), "0")
        del saved["n_clusters"]
        with self.assertRaisesRegex(report.ReportBlocked, "model sample G"):
            report._model_count(saved, "cluster_count")

    def test_observed_games_do_not_use_provider_population(self):
        value = synthetic_estimates()
        value["sports"]["coverage"][0]["games"] = 50
        source = report.render_source(value)
        self.assertIn("MLB & 50 & 40 & 10,010", source)
        self.assertIn("MLB & 40 & +1.25", source)
        self.assertIn("Provider games", source)
        self.assertNotIn("Archive observation-convention sensitivity", source)

    def test_failed_summary_missing_grid_duplicate_or_inconsistent_support_rejected(self):
        for mutation in (lambda value: value.update(status="estimates_incomplete"),
                         lambda value: value["sports"]["window_rows"].pop(),
                         lambda value: value["table1"]["profile_rows"].append(deepcopy(value["table1"]["profile_rows"][0])),
                         lambda value: value["sports"]["profile_rows"][0].update(paper_support=False),
                         lambda value: value["appendix_a2"][1]["counts"].update(rows=1)):
            value = synthetic_estimates()
            mutation(value)
            with self.assertRaises(report.ReportBlocked):
                report.render_source(value)

    def test_corrected_sports_clock_proof_is_required_and_old_cache_is_rejected(self):
        mutations = (
            lambda cache: cache.pop("sport_clock_columns"),
            lambda cache: cache["sport_clock_columns"].update(remaining_seconds="R"),
            lambda cache: cache["sport_clock_columns"].update(extra="timestamp"),
            lambda cache: cache.pop("reconciliation"),
            lambda cache: cache["reconciliation"].pop("sport_clock_mismatch_rows"),
            lambda cache: cache["reconciliation"].update(sport_clock_mismatch_rows=1),
            lambda cache: cache["reconciliation"].update(sport_clock_mismatch_rows=False),
            lambda cache: cache.pop("schema"),
            lambda cache: cache.update(schema=cache["schema"].replace("sport_remaining_seconds:", "R:")),
            lambda cache: cache.update(schema=cache["schema"]+"\nu: double"),
            lambda cache: cache.update(schema=cache["schema"]+"\nr: double"),
            lambda cache: cache.update(schema=cache["schema"]+"\nR: double"),
            lambda cache: cache.update(schema=cache["schema"]+"\nSPORT_REMAINING_SECONDS: double"),
            lambda cache: cache.update(schema=cache["schema"].replace("sport_elapsed_seconds: double", "sport_elapsed_seconds: int64")),
            lambda cache: cache.update(schema=cache["schema"].replace("sport_remaining_seconds: double", "sport_remaining_seconds: float")),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=index):
                data = synthetic_estimates()
                mutation(data["sports"]["observation_cache"])
                with self.assertRaises(report.ReportBlocked):
                    report.validate_estimates(data)
        data = synthetic_estimates()
        del data["sports"]["observation_cache"]
        with self.assertRaisesRegex(report.ReportBlocked, "corrected sports clock"):
            report.validate_estimates(data)

    def test_accepted_receipt_cannot_admit_legacy_sports_clock_or_create_report(self):
        data = synthetic_estimates()
        data["sports"]["observation_cache"] = {
            "schema": "u: double\nr: double", "reconciliation": {"joined_rows": 100}}
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            save_stage(folder/"old_accepted", data)
            with patch.object(report, "plot_profile") as plot:
                with self.assertRaisesRegex(report.ReportBlocked, "corrected sports clock"):
                    report.render(folder/"old_accepted", folder/"report")
                plot.assert_not_called()
            self.assertFalse((folder/"report").exists())

    def test_nonfinite_and_overflow_are_rejected_without_silent_zero(self):
        value = synthetic_estimates()
        value["table1"]["profile_rows"][0]["CR0"]["estimate"] = float("inf")
        with self.assertRaisesRegex(report.ReportBlocked, "nonfinite"):
            report.render_source(value)
        self.assertEqual(report.number(1.23e308), r"$1.23\times 10^{308}$")
        self.assertEqual(report.number(None), "withheld")
        row = {"outcome": "payoff_cents", "suppressed": False, "CR0": {"ci95_low": -1e308, "ci95_high": 1e308}}
        with self.assertRaisesRegex(report.ReportBlocked, "range overflow"):
            report._limits([row], "payoff_cents")

    def test_tex_escaping_and_multipage_source_counts(self):
        self.assertEqual(report.tex("a_%&{x}\\"), r"a\_\%\&\{x\}\textbackslash{}")
        source = report.table("Waterfall", ["Reason", "N"], [["invalid", "10"]]*30, multipage=True)
        self.assertIn(r"\begin{longtable}", source)
        self.assertIn(r"\endfirsthead", source)
        self.assertIn(r"\endhead", source)

    def test_actual_fixture_estimator_output_renders_monthly_schema_and_null_grids(self):
        from collections import Counter
        import duckdb
        from analysis.kaushik_polymarket_replication import run_estimates as driver
        from tests.test_kaushik_replication_driver import fixture
        with tempfile.TemporaryDirectory() as directory:
            con = duckdb.connect()
            try:
                records, manifest, binding = fixture(con, directory)
                tally = Counter((row["is_maker"], row["side"]) for row in records)
                manifest["exclusions"] = {"2025-02": [{"reason": "eligible", "side": side, "is_maker": maker,
                    "rows": n, "precut_rows": n, "duration_tail_rows": n} for (maker, side), n in tally.items()]}
                saved = driver.estimate_all(con, Path(directory)/"out", manifest, binding)
                source = report.render_source(saved)
                self.assertIn("Taker & SELL", source)
                self.assertIn("All price-band records", source)
                self.assertIn("withheld: <30 games; <500 records", source)
                self.assertEqual(source.count(r"\includegraphics"), 21)
            finally:
                con.close()

    def test_isolated_markers_and_caps_skip_suppressed_cells(self):
        plt = report._pyplot()
        fig, axis = plt.subplots()
        rows = [estimate(2), estimate(3), estimate(4)]
        suppress(rows[1])
        try:
            artist = report.draw_discrete(axis, rows, [1, 2, 3], label="saved", color="#0072B2", marker="o")
            self.assertEqual(list(artist.lines[0].get_xdata()), [1, 3])
            self.assertEqual(list(artist.lines[0].get_ydata()), [2, 4])
            self.assertEqual(artist.lines[0].get_linestyle(), "None")
            self.assertTrue(artist.has_yerr)
            self.assertEqual(len(artist.lines[1]), 2)
            self.assertEqual(report.draw_discrete(axis, [rows[1]], [2], label="none", color="k", marker="s"), None)
        finally:
            plt.close(fig)

    def test_receipt_failure_hash_mismatch_and_oversize_block_before_render(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage = root/"accepted"
            save_stage(stage, synthetic_estimates())
            data, _ = report.load_accepted_estimates(stage)
            self.assertEqual(data["status"], "estimates_complete")
            acceptance = json.loads((stage/"acceptance.json").read_text())
            acceptance["status"] = "estimates_failed"
            (stage/"acceptance.json").write_text(json.dumps(acceptance))
            with self.assertRaisesRegex(report.ReportBlocked, "accepted estimate receipt"):
                report.render(stage, root/"blocked")
            self.assertFalse((root/"blocked").exists())
            acceptance["status"] = "estimates_reopened_accepted"
            (stage/"acceptance.json").write_text(json.dumps(acceptance))
            (stage/"estimates.json").write_text("{}")
            with self.assertRaisesRegex(report.ReportBlocked, "fingerprint"):
                report.load_accepted_estimates(stage)
            with (stage/"estimates.json").open("wb") as stream:
                stream.truncate(report.MAX_JSON_BYTES+1)
            with patch.object(Path, "read_bytes", side_effect=AssertionError("read must not run")):
                with self.assertRaisesRegex(report.ReportBlocked, "size cap"):
                    report._read_json(stage/"estimates.json")

    def test_full_fixture_vector_assets_no_overwrite_and_deterministic_pdf(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = synthetic_estimates()
            save_stage(root/"accepted", data)
            manifest = report.render(root/"accepted", root/"report")
            self.assertEqual(manifest["primary_deliverable"], "source.tex")
            self.assertEqual(len(manifest["outputs"]), 22)
            self.assertEqual(manifest["compilation_status"], "pending")
            self.assertEqual(manifest["visual_qa_status"], "pending")
            for name in report.figure_names():
                self.assertTrue((root/"report"/"figures"/name).read_bytes().startswith(b"%PDF-"))
            with self.assertRaisesRegex(report.ReportBlocked, "target exists"):
                report.render(root/"accepted", root/"report")
            report.plot_profile(data, "pooled", root/"second.pdf")
            self.assertEqual((root/"second.pdf").read_bytes(), (root/"report"/"figures"/"figure1_pooled.pdf").read_bytes())


if __name__ == "__main__":
    unittest.main()
