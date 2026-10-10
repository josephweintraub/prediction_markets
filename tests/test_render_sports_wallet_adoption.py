from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import estimate_sports_wallet_adoption as adoption
from scripts import render_sports_wallet_adoption as renderer
from tests.test_sports_wallet_adoption_estimates import synthetic_comparison, synthetic_run, save_run


class AdoptionReportTests(unittest.TestCase):
    def test_oversized_comparison_and_case_metadata_rejected_before_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            comparison_path = folder/"comparison.json"
            with comparison_path.open("wb") as stream:
                stream.truncate(adoption.CAPS["summary_file_bytes"]+1)
            adoption.write_json(folder/"manifest.json", {"status": "estimates_complete", "data_certified": False,
                "outputs": {"comparison.json": {}}})
            with patch.object(renderer, "fingerprint") as fingerprint:
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "before hashing"):
                    renderer.render(folder/"manifest.json", folder/"report")
                fingerprint.assert_not_called()
            case_folder = folder/"historic_filtered"
            case_folder.mkdir()
            with (case_folder/"manifest.json").open("wb") as stream:
                stream.truncate(adoption.CAPS["summary_file_bytes"]+1)
            manifest = {"case_manifests": {"historic_filtered": {"path": "historic_filtered/manifest.json"}}}
            with patch.object(renderer, "fingerprint") as fingerprint:
                with self.assertRaisesRegex(adoption.AdoptionBlocked, "before hashing"):
                    renderer.final_kernel_inputs(folder/"manifest.json", manifest, synthetic_comparison())
                fingerprint.assert_not_called()

    def test_singleton_marks_saved_interval_without_bridging_support_or_time_gaps(self):
        from analysis.multisport_game_dynamics import render_flb_decay as plots
        rows = [dict(grid_index=index, time_value=time, bandwidth=.1, suppressed=suppressed,
                     spread_d10_minus_d1=.02, spread_ci95_low=.01, spread_ci95_high=.03)
                for index, time, suppressed in ((0, .1, False), (1, .2, True), (2, .3, False),
                                                (3, .31, False), (4, .7, False))]
        fig, axis = plots.plt.subplots()
        try:
            self.assertTrue(renderer.draw_kernel(axis, rows, "#1f4e79", plots, "Series"))
            self.assertEqual(len(axis.containers), 2)
            self.assertEqual([float(container.lines[0].get_xdata()[0]) for container in axis.containers], [.1, .7])
            for container in axis.containers:
                self.assertTrue(container.has_yerr)
                self.assertEqual(float(container.lines[0].get_ydata()[0]), 2.)
                self.assertEqual([float(cap.get_ydata()[0]) for cap in container.lines[1]], [1., 3.])
                self.assertEqual(container.lines[0].get_linestyle(), "None")
            curves = [line for line in axis.lines if len(line.get_xdata()) > 1]
            self.assertEqual(len(curves), 1)
            self.assertEqual(list(curves[0].get_xdata()), [.3, .31])
            self.assertEqual(axis.get_legend_handles_labels()[1], ["Series"])
        finally:
            plots.plt.close(fig)

    def test_pooled_and_sport_panels_display_supported_singletons(self):
        from analysis.multisport_game_dynamics import render_flb_decay as plots
        supported = dict(grid_index=0, time_value=.5, bandwidth=.1, suppressed=False,
                         spread_d10_minus_d1=.02, spread_ci95_low=.01, spread_ci95_high=.03,
                         phase="live", weighting="equal_fill", d1_n=700, d10_n=800)
        rows = [{**supported, "scope": "pooled", "sport": "all"},
                {**supported, "scope": "sport", "sport": "mlb"}]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(renderer, "draw_kernel", wraps=renderer.draw_kernel) as draw:
            folder = Path(directory)
            renderer.plot_pooled_kernel(rows, folder/"pooled.pdf", plots)
            renderer.plot_sport_phase(rows, "live", folder/"sports.pdf", plots)
            self.assertEqual(draw.call_count, 11)
            pooled_axis, sport_axis = draw.call_args_list[0].args[0], draw.call_args_list[2].args[0]
            self.assertEqual(len(pooled_axis.containers), 1)
            self.assertEqual(len(sport_axis.containers), 1)
            self.assertFalse(any("No locally supported" in text.get_text() for text in sport_axis.texts))
            self.assertTrue((folder/"pooled.pdf").read_bytes().startswith(b"%PDF-"))
            self.assertTrue((folder/"sports.pdf").read_bytes().startswith(b"%PDF-"))

    def test_definitions_uncertainty_counts_and_three_distinct_changes(self):
        source = renderer.render_source(synthetic_comparison())
        for text in ("MLB coverage", "Saved-flag gap", "Identity repair", "Original CLEAN", "Corrected CLEAN",
                     "cross-run covariance", "legacy inferred", "UTC", "[0,.1)", "[.9,1]",
                     "40,500", "College football", "WNBA", "95\\%", "Epanechnikov", "$h=.50$"):
            self.assertIn(text, source)
        self.assertEqual(source.count("\\begin{table}"), 7)
        self.assertEqual(source.count("\\end{table}"), 7)
        self.assertEqual(source.count("\\includegraphics"), 6)
        self.assertEqual(source.count("\\begin{figure}"), 5)
        for name in renderer.FIGURES:
            self.assertIn("figures/"+name, source)
        self.assertNotIn("https://", source)
        self.assertNotIn("\\section{Conclusion", source)
        self.assertTrue(source.rstrip().endswith("\\end{document}"))

    def test_withheld_estimate_not_rendered_as_zero(self):
        value = synthetic_comparison()
        key = renderer.headline_ids()[0][0]
        for name in adoption.CASES:
            row = next(row for row in value["cases"][name]["estimands"] if row["estimand_id"] == key)
            row.update(suppressed=True, estimate=None, standard_error=None, ci95_low=None, ci95_high=None)
        value["changes"] = [row for name, before, after in adoption.CONTRASTS for row in adoption.compare_estimands(
            value["cases"][before]["estimands"], value["cases"][after]["estimands"], name)]
        source = renderer.render_source(value)
        self.assertIn("MLB & withheld & withheld & withheld & withheld", source)

    def test_missing_gate_or_duplicate_estimand_is_rejected(self):
        value = synthetic_comparison()
        value["gates"]["all_trade_regime_invariant"] = False
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "gates"):
            renderer.render_source(value)
        value = synthetic_comparison()
        rows = value["cases"]["restored_repaired"]["estimands"]
        rows.append(deepcopy(rows[0]))
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "Duplicate"):
            renderer.render_source(value)
        value = synthetic_comparison()
        value["changes"][0]["before"]["estimate"] += .01
        with self.assertRaisesRegex(adoption.AdoptionBlocked, "embedded case estimates"):
            renderer.render_source(value)

    def test_saved_comparison_hash_no_overwrite_and_pending_visual_qa(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            comparison = synthetic_comparison()
            cases = {}
            for index, name in enumerate(adoption.CASES):
                value = synthetic_run(index*.001, "all_trades" if name == "restored_all" else "filtered_trades")
                cases[name] = {**save_run(folder/name, value), "path": f"{name}/manifest.json"}
                comparison["cases"][name]["manifest"] = cases[name]
            adoption.write_json(folder/"comparison.json", comparison)
            manifest = {"status": "estimates_complete", "data_certified": False,
                        "outputs": {"comparison.json": {**adoption.fingerprint(folder/"comparison.json"), "path": "comparison.json"}},
                        "case_manifests": cases}
            adoption.write_json(folder/"manifest.json", manifest)
            result = renderer.render(folder/"manifest.json", folder/"report")
            self.assertEqual(result["compilation_status"], "pending")
            self.assertEqual(result["visual_qa_status"], "pending")
            self.assertTrue((folder/"report"/"wallet_adoption_comparison.tex").is_file())
            self.assertEqual(len(result["outputs"]), 7)
            for name in renderer.FIGURES:
                self.assertTrue((folder/"report"/"figures"/name).read_bytes().startswith(b"%PDF-"))
            with self.assertRaisesRegex(adoption.AdoptionBlocked, "target exists"):
                renderer.render(folder/"manifest.json", folder/"report")
            (folder/"comparison.json").write_text("{}")
            with self.assertRaisesRegex(adoption.AdoptionBlocked, "fingerprint"):
                renderer.render(folder/"manifest.json", folder/"another")

    def test_final_kernel_manifest_binding_is_required(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            comparison = synthetic_comparison()
            value = synthetic_run()
            expected = {**save_run(folder/"historic_filtered", value), "path": "historic_filtered/manifest.json"}
            comparison["cases"]["historic_filtered"]["manifest"] = expected
            manifest = {"case_manifests": {"historic_filtered": {**expected, "sha256": "0"*64}}}
            with self.assertRaisesRegex(adoption.AdoptionBlocked, "manifest fingerprint"):
                renderer.final_kernel_inputs(folder/"manifest.json", manifest, comparison)


if __name__ == "__main__":
    unittest.main()
