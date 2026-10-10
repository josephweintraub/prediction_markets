"""Synthetic saved-estimate reporting checks; no real data or production work."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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


def save_stage(folder, data):
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
    manifest_digest = save(folder/"manifest.json", manifest)
    save(folder/"acceptance.json", {"schema_version": "kaushik_replication_estimate_acceptance_v1",
         "status": "estimates_reopened_accepted", "manifest_sha256": manifest_digest, "estimates_sha256": digest,
         "source_head": "a"*40, "all_outputs_reopened": True})


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
