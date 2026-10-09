"""Synthetic compact summaries only; no production data or TeX compiler."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/render_mlb_unfiltered_rebuild.py"
SPEC = importlib.util.spec_from_file_location("mlb_count_renderer", SCRIPT)
renderer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(renderer)


def fixture():
    counts = {"old_accepted_all_rows": 50, "old_accepted_filtered_rows": 40,
              "new_accepted_all_rows": 80, "new_accepted_filtered_rows": 55,
              "restored_all_rows": 30, "restored_filtered_rows": 15,
              "distinct_candidate_fills": 100, "prefilter_exact_rows": 100, "old_exact_rows": 60,
              "outside_accepted_market_rows": 10, "post_end_rows": 5, "invalid_sample_price_rows": 5,
              "accepted_valid_price_rows": 80, "filtered_extreme_price_exclusions": 15,
              "filtered_flagged_interior_exclusions": 10,
              "accepted_missing_flag_rows": 2, "accepted_null_flag_rows": 1,
              "raw_candidate_rows": 103, "duplicate_payload_rows": 3,
              "source_blocks": 10, "matched_blocks": 10, "missing_blocks": 0,
              "candidate_markets": 6, "accepted_metadata_markets": 5, "accepted_metadata_events": 3,
              "new_all_markets": 5, "new_all_events": 3, "new_filtered_markets": 4, "new_filtered_events": 3,
              "old_all_markets": 4, "old_all_events": 3, "old_filtered_markets": 3, "old_filtered_events": 2}
    files = {name: {"path": "/home/ubuntu/prediction_markets/" + name, "bytes": 100,
                    "sha256": "b" * 64} for name in (renderer.PRODUCER, renderer.SPEC)}
    outputs = {name: {"path": name, "bytes": 100, "sha256": "c" * 64, "rows": rows,
                      "schema": "synthetic fixture schema"}
               for name, rows in (("exact_trades.parquet", 100), ("all_trades.parquet", 80),
                                  ("filtered_trades.parquet", 55))}
    return {"schema_version": 1, "status": "mlb_unfiltered_samples_complete", "data_certified": False,
            "scientific_estimators_rerun": False, "counts": counts,
            "reconciliation": {key: True for key, _ in renderer.QA},
            "definitions": copy.deepcopy(renderer.DEFINITIONS),
            "source": {"expected_head": "a" * 40, "head_before": "a" * 40, "head_after": "a" * 40, "files": files},
            "inputs": {name: {"path": "/synthetic/" + name, "bytes": 100, "sha256": "d" * 64}
                       for name in renderer.INPUTS}, "outputs": outputs,
            "summary_artifact": {"path": "summary.json", "bytes": 100, "sha256": "e" * 64}}


def identity(summary):
    raw = json.dumps(summary, sort_keys=True).encode()
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def qa_fixture(summary):
    return {"schema_version": 1, "status": "mlb_unfiltered_saved_artifact_qa_complete",
            "data_certified": False, "scientific_estimators_rerun": False,
            "manifest": {"path": "/synthetic/ec2/manifest.json", **identity(summary)},
            "expected_head": summary["source"]["expected_head"], "reviewer_source_sha256": "f" * 64,
            "counts": {key: summary["counts"][key] for key in renderer.INDEPENDENT_COUNTS |
                       {"source_blocks", "matched_blocks", "missing_blocks"}},
            "gates": {key: True for key in renderer.INDEPENDENT_GATES}}


class MLBCountReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.path = self.base / "manifest.json"
        self.qa_path = self.base / "receipt.json"
        self.summary = fixture()
        self.save()

    def tearDown(self):
        self.temp.cleanup()

    def save(self):
        self.path.write_text(json.dumps(self.summary, sort_keys=True), encoding="utf-8")
        self.qa = qa_fixture(self.summary)
        self.qa_path.write_text(json.dumps(self.qa, sort_keys=True), encoding="utf-8")

    def render(self, summary=None):
        summary = self.summary if summary is None else summary
        return renderer.render_tex(summary, qa_fixture(summary), identity(summary))

    def publish(self, destination=None):
        return renderer.publish(self.path, self.qa_path, destination or self.base / "report")

    def test_count_comparison_and_accounting_use_saved_fixture(self):
        tex = self.render()
        self.assertIn(r"All & \num{50} & \num{80} & \num{30} & 60.00\%", tex)
        self.assertIn(r"Filtered & \num{40} & \num{55} & \num{15} & 37.50\%", tex)
        self.assertIn(r"Invalid sample price & \num{5}", tex)
        self.assertIn(r"Current flagged interior-price rows excluded & \num{10}", tex)
        self.assertEqual(tex.count(" & Pass "), 10)
        self.assertNotIn("2502803", tex)
        self.assertNotIn("2412918", tex)

    def test_short_portable_table_first_report_has_paragraph_boundaries(self):
        tex = self.render()
        self.assertEqual(tex.count(r"\begin{threeparttable}"), 3)
        self.assertEqual(tex.count(r"\par\begin{threeparttable}"), 3)
        self.assertEqual(tex.count(r"\end{threeparttable}\par"), 3)
        self.assertNotIn(r"\includegraphics", tex)
        self.assertNotIn("https://", tex)
        self.assertNotIn("Conclusion", tex)
        prose = [line for line in tex.splitlines() if line.startswith(r"\item") or line.startswith("Inferred BUY rows")]
        self.assertLessEqual(sum(len(line.split()) for line in prose), 150)
        self.assertIn("no FLB, calibration or regression estimates were rerun", tex)
        self.assertIn("Broader canon remains unrepaired", tex)
        self.assertIn("inferred BUYs do not certify native own actions", tex)
        self.assertIn("All pregame history", tex)
        self.assertIn("its `all trades' branch was already filtered", tex)
        self.assertIn("collection completeness", tex)
        self.assertTrue(tex.endswith("\\end{tablenotes}\\end{threeparttable}\\par\n\\end{document}\n"))

    def test_each_required_boolean_gate_is_fail_closed(self):
        for key, _ in renderer.QA:
            changed = copy.deepcopy(self.summary)
            changed["reconciliation"][key] = False
            with self.subTest(key=key), self.assertRaises(renderer.ReportBlocked):
                self.render(changed)

    def test_incomplete_or_scientific_rerun_refuses_before_publication(self):
        for key, value in (("status", "blocked"), ("data_certified", True), ("scientific_estimators_rerun", True)):
            self.summary = fixture()
            self.summary[key] = value
            self.save()
            with self.subTest(key=key), self.assertRaises(renderer.ReportBlocked):
                self.publish()
            self.assertFalse((self.base / "report").exists())

    def test_count_laws_and_noninteger_counts_refuse(self):
        for key, value in (("post_end_rows", 6), ("restored_all_rows", 31),
                           ("filtered_extreme_price_exclusions", 16), ("prefilter_exact_rows", 101),
                           ("old_accepted_filtered_rows", 0), ("old_accepted_all_rows", 50.0),
                           ("new_all_markets", -1), ("candidate_markets", True), ("raw_candidate_rows", 104)):
            changed = copy.deepcopy(self.summary)
            changed["counts"][key] = value
            with self.subTest(key=key), self.assertRaises(renderer.ReportBlocked):
                self.render(changed)

    def test_policy_source_and_output_bindings_are_required(self):
        for mutate in (lambda value: value["definitions"].update(all_trades="drops flagged buyers"),
                       lambda value: value["source"].update(head_after="e" * 40),
                       lambda value: value["source"]["files"].pop(renderer.PRODUCER),
                       lambda value: value["inputs"].pop("phase"),
                       lambda value: value.pop("summary_artifact"),
                       lambda value: value["outputs"]["all_trades.parquet"].update(rows=79),
                       lambda value: value["outputs"]["all_trades.parquet"].update(path="../wrong.parquet")):
            changed = copy.deepcopy(self.summary)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                self.render(changed)

    def test_immutable_publication_roundtrips_and_is_deterministic(self):
        first = self.publish()
        second = self.publish(self.base / "second")
        self.assertEqual(first["output"], second["output"])
        self.assertEqual(json.loads((self.base / "report/manifest.json").read_text()), first)
        self.assertEqual(first["counts"], self.summary["counts"])
        self.assertFalse(first["layout"]["compiled_or_visually_verified"])
        self.assertEqual(first["build_manifest"]["sha256"], identity(self.summary)["sha256"])
        self.assertEqual(first["qa_receipt"]["sha256"], hashlib.sha256(self.qa_path.read_bytes()).hexdigest())
        self.assertEqual(first["independent_qa"]["gates"], self.qa["gates"])
        with self.assertRaises(renderer.ReportBlocked):
            self.publish()

    def test_duplicate_nonfinite_and_oversize_json_are_rejected(self):
        for raw in (b'{"status":"x","status":"y"}', b'{"value":NaN}', b" " * (renderer.MAX_BYTES + 1)):
            self.path.write_bytes(raw)
            with self.assertRaises(renderer.ReportBlocked):
                self.publish()
            self.assertFalse((self.base / "report").exists())

    def test_receipt_bindings_and_independent_counts_refuse_tampering(self):
        for mutate in (lambda value: value["manifest"].update(sha256="1" * 64),
                       lambda value: value["manifest"].update(bytes=value["manifest"]["bytes"] + 1),
                       lambda value: value.update(expected_head="b" * 40),
                       lambda value: value.update(reviewer_source_sha256="invalid"),
                       lambda value: value["counts"].update(new_accepted_all_rows=81),
                       lambda value: value["counts"].update(new_all_events=2),
                       lambda value: value["counts"].pop("old_all_markets"),
                       lambda value: value["counts"].update(accepted_missing_flag_rows=2.0),
                       lambda value: value.update(status="blocked"),
                       lambda value: value.update(data_certified=True),
                       lambda value: value.update(scientific_estimators_rerun=True)):
            changed = qa_fixture(self.summary)
            mutate(changed)
            self.qa_path.write_text(json.dumps(changed), encoding="utf-8")
            with self.assertRaises(renderer.ReportBlocked):
                self.publish()
            self.assertFalse((self.base / "report").exists())

    def test_each_independent_gate_requires_exact_true_and_no_extra_gates(self):
        for key in renderer.INDEPENDENT_GATES:
            for value in (False, 1, None, "missing"):
                changed = qa_fixture(self.summary)
                if value == "missing":
                    changed["gates"].pop(key)
                else:
                    changed["gates"][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(renderer.ReportBlocked):
                    renderer.render_tex(self.summary, changed, identity(self.summary))
        changed = qa_fixture(self.summary)
        changed["gates"]["extra"] = True
        with self.assertRaises(renderer.ReportBlocked):
            renderer.render_tex(self.summary, changed, identity(self.summary))

    def test_receipt_duplicate_nonfinite_and_oversize_fail_before_publication(self):
        for raw in (b'{"status":"x","status":"y"}', b'{"value":NaN}', b" " * (renderer.MAX_BYTES + 1)):
            self.qa_path.write_bytes(raw)
            with self.assertRaises(renderer.ReportBlocked):
                self.publish()
            self.assertFalse((self.base / "report").exists())

    def test_receipt_is_bound_to_exact_manifest_bytes_not_only_counts(self):
        self.path.write_bytes(self.path.read_bytes() + b"\n")
        with self.assertRaises(renderer.ReportBlocked):
            self.publish()
        self.assertFalse((self.base / "report").exists())

    def test_all_additional_shared_receipt_counts_are_compared(self):
        self.qa["counts"]["matched_blocks"] = 9
        self.qa_path.write_text(json.dumps(self.qa), encoding="utf-8")
        with self.assertRaises(renderer.ReportBlocked):
            self.publish()
        self.assertFalse((self.base / "report").exists())

    def test_cli_requires_independent_receipt(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--summary", str(self.path),
                                 "--run-dir", str(self.base / "report")], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--qa-receipt", result.stderr)
        self.assertFalse((self.base / "report").exists())


if __name__ == "__main__":
    unittest.main()
