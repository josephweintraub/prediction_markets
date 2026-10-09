"""Small saved-metadata fixtures; no DuckDB, Parquet, network or production data."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "scripts/audit_polymarket_wallet_census_manifest.py"
spec = importlib.util.spec_from_file_location("wallet_census_manifest", SOURCE)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def metrics(rows=4, payload=None):
    assert rows % 2 == 0
    half = rows // 2
    value = {"row_count": rows, "maker_rows": half, "nonmaker_rows": half,
             "missing_role_rows": 0, "invalid_rows": 0, "self_wallet_rows": 0,
             "null_event_label_rows": 0, "blank_event_label_rows": 0,
             "gross_recorded_cash": float(rows), "logical_payload_bytes": 83 * rows if payload is None else payload}
    categories = {name: {"common_classes": 0, "maker_rows": 0, "nonmaker_rows": 0,
                         "singleton_role_classes": 0} for name in audit.CATEGORIES}
    categories["copied_only"] = dict(common_classes=half, maker_rows=half, nonmaker_rows=half,
                                     singleton_role_classes=half)
    pairs = {"maker_rows": half, "nonmaker_rows": half, "common_classes": half,
             "singleton_role_classes": half, "matched_correct": 0, "matched_copied": half,
             "excess_observed_correct": half, "missing_observed_correct": half,
             "excess_observed_copied": 0, "missing_observed_copied": 0,
             "shared_candidate_capacity": 0, "observed_shared_candidate_capacity": 0,
             "compatibility": categories}
    return {"support": {name: copy.deepcopy(value) for name in audit.RELATIONS},
            "cleaning": {"root_distinct_rows": rows, "root_value_row_surplus": 0,
                         "clean_value_row_surplus": 0, "root_distinct_gross_recorded_cash": float(rows),
                         "expected_clean_only_rows": 0, "clean_only_rows": 0,
                         "clean_only_value_classes": 0, "failed_full11_reconciliation_leaves": 0},
            **{name: {mode: copy.deepcopy(pairs) for mode in audit.MODES} for name in audit.RELATIONS}}


def scan(empty=False):
    return {"parquet_scan_operators": 0 if empty else 1,
            "footer_proved_empty_result": empty, "physical_plan_sha256": "1" * 64}


def node(month, lower, upper, record, *, empty=False):
    return {"month": month, "lower_inclusive": lower, "upper_exclusive": upper,
            "metrics": record,
            "source_scan_plans": {name: {stage: scan(empty) for stage in ("admission", "grouping")}
                                  for name in audit.RELATIONS}}


def fixture():
    inventories = {name: [] for name in audit.RELATIONS}
    plan, leaves, monthly = [], [], {}
    for month in audit.months():
        lower, upper = audit.bounds(month)
        for name in audit.RELATIONS:
            inventories[name].append({"partition_month": month, "path": f"/fixture/{name}/{month}.parquet",
                "rows": 4, "created_by": "DuckDB version v1.5.0 (build 3a3967aa81)",
                "row_groups": [{"index": 0, "rows": 4, "compressed_bytes": 100,
                                "stats": {"timestamp": {"min": lower, "max": lower + 1}}}]})
        plan.append({"month": month, "lower_inclusive": lower, "upper_exclusive": upper,
                     "initial_unit": "month"})
        monthly[month] = metrics()
        leaves.append(node(month, lower, upper, metrics()))
    preflight = {"schema_version": 1, "status": "preflight_complete", "data_certified": False,
                 "expected_head": audit.HEAD, "source_sha256": copy.deepcopy(audit.SOURCES),
                 "contract_sha256": audit.SOURCES["docs/analysis_specs/polymarket_wallet_pair_census_v1.json"],
                 "contract": {"scope": "synthetic metadata fixture, not data certification"},
                 "caps": copy.deepcopy(audit.CAPS),
                 "inputs": {"root": "/mnt/data/pipeline_root_output/trades.parquet",
                            "clean": "/mnt/data/pipeline_output/trades_clean.parquet"},
                 "inventories": inventories, "initial_plan": plan,
                 "published_month_proofs": {}, "published_directory_layouts": {},
                 "footer_rows": {name: 176 for name in audit.RELATIONS},
                 "environment": {"platform": "linux", "duckdb": "1.5.0", "runtime_settings": {}},
                 "io_plan": {"planned_initial_two_pass_compressed_bytes": 17600}}
    body = copy.deepcopy(preflight)
    body["status"] = audit.COMPLETE
    body["census"] = {"status": audit.COMPLETE, "data_certified": False, "leaves": leaves, "splits": [],
                      "months": monthly, "completed_months": audit.months(),
                      "final_input_identity_reopened": True, "root_distinct_to_clean_reconciled": True,
                      "admission_compressed_footprint_bytes": 8800,
                      "grouping_compressed_footprint_bytes": 8800,
                      "planned_original_read_footprint_bytes": 17600, "admission_nodes": 44,
                      "global": audit.sum_records(list(monthly.values()))}
    return preflight, body


class SavedManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.preflight, self.body = fixture()

    def tearDown(self):
        self.temp.cleanup()

    def prepare(self, profile_transform=lambda text: text):
        raw = encoded(self.preflight)
        digest = hashlib.sha256(raw).hexdigest()
        self.body["reviewed_preflight_path"] = "/fixture/preflight/manifest.json"
        self.body["reviewed_preflight_sha256"] = digest
        self.body["command"] = ["scripts/audit_polymarket_wallet_pairs.py", "--expected-head", audit.HEAD,
            "--reviewed-preflight", self.body["reviewed_preflight_path"],
            "--reviewed-preflight-sha256", digest, "--run-dir", "/fixture/census_durable_v2"]
        final_raw = encoded(self.body)
        summary = {field: self.body[field] for field in audit.SUMMARY_FIELDS}
        summary["census"] = {field: value for field, value in self.body["census"].items() if field not in {"leaves", "splits"}}
        summary.update(manifest_bytes=len(final_raw), manifest_sha256=hashlib.sha256(final_raw).hexdigest())
        profile = '\tCommand being timed: "/home/ubuntu/venv/bin/python -u ' + ' '.join(self.body["command"]) + '"\n'
        profile += "\tUser time (seconds): 1.00\n\tExit status: 0\n"
        for filename, content in (("preflight.json", raw), ("manifest.json", final_raw),
                                  ("summary.json", encoded(summary)), ("profile.txt", profile_transform(profile).encode())):
            (self.base / filename).write_bytes(content)
        return digest

    def build(self, profile_transform=lambda text: text, *, output_name="review"):
        digest = self.prepare(profile_transform)
        return audit.build_review(self.base / "preflight.json", digest, self.base / "manifest.json",
                                  self.base / "summary.json", self.base / "profile.txt", self.base / output_name)

    def test_complete_metadata_fixture_is_bounded_immutable_and_not_certified(self):
        result = self.build()
        self.assertEqual(result["completed_months"], audit.months())
        self.assertEqual((result["leaf_count"], result["split_count"], result["admission_nodes"]), (44, 0, 44))
        self.assertEqual(result["planned_original_read_footprint_bytes"], 17600)
        self.assertFalse(result["data_certified"])
        self.assertIn("not native identity", result["limits"])
        self.assertLess((self.base / "review/receipt.json").stat().st_size, audit.MAX_RECEIPT)
        for name, identity in result["inputs"].items():
            path = Path(identity["path"])
            self.assertEqual(identity["bytes"], path.stat().st_size)
            self.assertEqual(identity["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
        with self.assertRaises(audit.ReviewBlocked):
            self.build()

    def test_exit_missing_duplicate_nonzero_and_command_changes_refuse(self):
        for transform in (lambda text: text.replace("\tExit status: 0\n", ""),
                          lambda text: text + "\tExit status: 0\n",
                          lambda text: text.replace("Exit status: 0", "Exit status: 120"),
                          lambda text: text.replace(audit.HEAD, "a" * 40),
                          lambda text: text.replace("/fixture/census_durable_v2", "/fixture/other_run"),
                          lambda text: text.replace("python -u ", "python ")):
            with self.subTest(transform=transform), self.assertRaises(audit.ReviewBlocked):
                self.build(transform)
            self.assertFalse((self.base / "review").exists())

    def test_frozen_binding_certification_partial_and_count_changes_refuse(self):
        original = copy.deepcopy(self.body)
        mutations = [lambda body: body.update(data_certified=True),
                     lambda body: body["census"].update(data_certified=True),
                     lambda body: body["census"].update(active_leaf={}),
                     lambda body: body["census"].update(blocked_original_query={}),
                     lambda body: body["census"].update(final_input_identity_reopened=False),
                     lambda body: body["census"]["completed_months"].pop(),
                     lambda body: body["caps"].update(threads=8),
                     lambda body: body["source_sha256"].update({"scripts/audit_polymarket_wallet_pairs.py": "0" * 64}),
                     lambda body: body["census"]["global"]["support"]["root"].update(row_count=177),
                     lambda body: body["census"].update(admission_compressed_footprint_bytes=8801),
                     lambda body: body["census"].update(admission_nodes=45)]
        for mutate in mutations:
            self.body = copy.deepcopy(original); mutate(self.body)
            with self.subTest(mutate=mutate), self.assertRaises(audit.ReviewBlocked):
                self.build()
        self.assertFalse((self.base / "review").exists())

    def test_gap_duplicate_orphan_and_fractional_second_refuse(self):
        original = copy.deepcopy(self.body)
        mutations = [lambda body: body["census"]["leaves"].pop(),
                     lambda body: body["census"]["leaves"].append(copy.deepcopy(body["census"]["leaves"][0])),
                     lambda body: body["census"]["leaves"][0].update(lower_inclusive=body["census"]["leaves"][0]["lower_inclusive"] + 1),
                     lambda body: body["census"]["leaves"][0].update(lower_inclusive=float(body["census"]["leaves"][0]["lower_inclusive"]))]
        for mutate in mutations:
            self.body = copy.deepcopy(original); mutate(self.body)
            with self.subTest(mutate=mutate), self.assertRaises(audit.ReviewBlocked):
                self.build()

    def test_saved_scan_and_footer_empty_proofs_are_checked_not_reexecuted(self):
        for scans, proved in ((0, True), (2, False), (1, True)):
            self.body["census"]["leaves"][0]["source_scan_plans"]["root"]["admission"].update(
                parquet_scan_operators=scans, footer_proved_empty_result=proved)
            with self.subTest(scans=scans, proved=proved), self.assertRaises(audit.ReviewBlocked):
                self.build()

    def test_payload_split_uses_all_complete_utc_days_and_empty_proofs(self):
        month = audit.months()[0]
        lower, upper = audit.bounds(month)
        large = 17 * 1024**3 // 2
        support = metrics(payload=large)["support"]
        parent = self.body["census"]["leaves"].pop(0)
        parent.pop("metrics")
        parent.update(support=support, split_kind="complete_utc_days")
        parent["source_scan_plans"] = {name: {"admission": scan()} for name in audit.RELATIONS}
        self.body["census"]["splits"] = [parent]
        day_nodes = []
        for index, start in enumerate(range(lower, upper, 86400)):
            day_nodes.append(node(month, start, min(start + 86400, upper),
                                  metrics(2, large // 2) if index < 2 else metrics(0), empty=index >= 2))
        self.body["census"]["leaves"] = day_nodes + self.body["census"]["leaves"]
        self.body["census"]["months"][month] = audit.sum_records([item["metrics"] for item in day_nodes])
        self.body["census"]["global"] = audit.sum_records(list(self.body["census"]["months"].values()))
        self.body["census"].update(admission_nodes=74, admission_compressed_footprint_bytes=9200,
                                  grouping_compressed_footprint_bytes=9000, planned_original_read_footprint_bytes=18200)
        for name in audit.RELATIONS:
            self.preflight["inventories"][name][0]["row_groups"][0]["stats"]["timestamp"]["max"] = lower + 86400 + 1
        self.body["inventories"] = copy.deepcopy(self.preflight["inventories"])
        result = self.build()
        self.assertEqual((result["split_count"], result["coverage"][month]["leaf_count"]), (1, 30))
        self.assertTrue(any(plan["source_scan_plans"]["root"]["admission"]["footer_proved_empty_result"]
                            for plan in result["scan_plan_receipts"]))

    def test_duplicate_nonfinite_profile_size_summary_projection_and_digest_gates(self):
        digest = self.prepare()
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}'):
            (self.base / "summary.json").write_bytes(raw)
            with self.assertRaises(audit.ReviewBlocked):
                audit.build_review(self.base / "preflight.json", digest, self.base / "manifest.json",
                                   self.base / "summary.json", self.base / "profile.txt", self.base / "review")
        self.prepare()
        (self.base / "profile.txt").write_bytes(b" " * (audit.MAX_PROFILE + 1))
        with self.assertRaises(audit.ReviewBlocked):
            audit.build_review(self.base / "preflight.json", digest, self.base / "manifest.json",
                               self.base / "summary.json", self.base / "profile.txt", self.base / "review")
        self.prepare()
        with self.assertRaises(audit.ReviewBlocked):
            audit.build_review(self.base / "preflight.json", "0" * 64, self.base / "manifest.json",
                               self.base / "summary.json", self.base / "profile.txt", self.base / "review")
        summary = json.loads((self.base / "summary.json").read_text()); summary["manifest_bytes"] += 1
        (self.base / "summary.json").write_bytes(encoded(summary))
        with self.assertRaises(audit.ReviewBlocked):
            audit.build_review(self.base / "preflight.json", digest, self.base / "manifest.json",
                               self.base / "summary.json", self.base / "profile.txt", self.base / "review")

    def test_receipt_cap_refuses_without_output_and_exact_large_integers_survive(self):
        digest = self.prepare()
        with mock.patch.object(audit, "review", return_value={"data_certified": False, "large": "x" * audit.MAX_RECEIPT}):
            with self.assertRaises(audit.ReviewBlocked):
                audit.build_review(self.base / "preflight.json", digest, self.base / "manifest.json",
                                   self.base / "summary.json", self.base / "profile.txt", self.base / "review")
        self.assertFalse((self.base / "review").exists())
        self.assertEqual(audit.sum_records([{"n": 2**60}, {"n": 7}])["n"], 2**60 + 7)
        with self.assertRaises(audit.ReviewBlocked):
            audit.sum_records([{"n": True}])

    def test_complete_second_bisection_is_verified_from_saved_nodes(self):
        month = audit.months()[0]
        lower, upper = audit.bounds(month)
        large = 17 * 1024**3 // 2
        parent = self.body["census"]["leaves"].pop(0)
        parent.pop("metrics")
        parent.update(support=metrics(payload=large)["support"], split_kind="complete_utc_days")
        parent["source_scan_plans"] = {name: {"admission": scan()} for name in audit.RELATIONS}
        first_day = {"month": month, "lower_inclusive": lower, "upper_exclusive": lower + 86400,
                     "support": metrics(payload=large)["support"], "split_kind": "integer_second_bisection",
                     "source_scan_plans": {name: {"admission": scan()} for name in audit.RELATIONS}}
        halves = [node(month, lower, lower + 43200, metrics(2, large // 2)),
                  node(month, lower + 43200, lower + 86400, metrics(2, large // 2))]
        empty_days = [node(month, start, min(start + 86400, upper), metrics(0), empty=True)
                      for start in range(lower + 86400, upper, 86400)]
        monthly_nodes = halves + empty_days
        self.body["census"]["splits"] = [parent, first_day]
        self.body["census"]["leaves"] = monthly_nodes + self.body["census"]["leaves"]
        self.body["census"]["months"][month] = audit.sum_records([item["metrics"] for item in monthly_nodes])
        self.body["census"]["global"] = audit.sum_records(list(self.body["census"]["months"].values()))
        self.body["census"].update(admission_nodes=76, admission_compressed_footprint_bytes=9400,
                                  grouping_compressed_footprint_bytes=9000, planned_original_read_footprint_bytes=18400)
        for name in audit.RELATIONS:
            self.preflight["inventories"][name][0]["row_groups"][0]["stats"]["timestamp"]["max"] = lower + 43200 + 1
        self.body["inventories"] = copy.deepcopy(self.preflight["inventories"])
        self.assertEqual(self.build()["split_count"], 2)
        first_day["split_kind"] = "complete_utc_days"
        with self.assertRaises(audit.ReviewBlocked):
            self.build(output_name="invalid_bisection")
        self.assertFalse((self.base / "invalid_bisection").exists())

    def test_exact_read_budget_boundary_and_one_over_fail_closed(self):
        limit = audit.CAPS["maximum_planned_read_footprint_bytes"]
        for name in audit.RELATIONS:
            for info in self.preflight["inventories"][name]:
                info["row_groups"][0]["compressed_bytes"] = 1
        self.preflight["inventories"]["root"][0]["row_groups"][0]["compressed_bytes"] = limit // 2 - 87
        self.body["inventories"] = copy.deepcopy(self.preflight["inventories"])
        self.body["census"].update(admission_compressed_footprint_bytes=limit // 2,
                                  grouping_compressed_footprint_bytes=limit // 2,
                                  planned_original_read_footprint_bytes=limit)
        self.assertEqual(self.build()["planned_original_read_footprint_bytes"], limit)
        self.preflight["inventories"]["root"][0]["row_groups"][0]["compressed_bytes"] += 1
        self.body["inventories"] = copy.deepcopy(self.preflight["inventories"])
        self.body["census"].update(admission_compressed_footprint_bytes=limit // 2 + 1,
                                  grouping_compressed_footprint_bytes=limit // 2 + 1,
                                  planned_original_read_footprint_bytes=limit + 2)
        with self.assertRaises(audit.ReviewBlocked):
            self.build(output_name="over_budget")
        self.assertFalse((self.base / "over_budget").exists())

    def test_consistently_fractional_capacities_and_negative_cash_refuse(self):
        original = copy.deepcopy(self.body)
        for field in ("shared_candidate_capacity", "observed_shared_candidate_capacity"):
            self.body = copy.deepcopy(original)
            for index, item in enumerate(self.body["census"]["leaves"]):
                item["metrics"]["root"]["full_label"][field] = 0.5 if index == 0 else 0.0
                self.body["census"]["months"][item["month"]] = copy.deepcopy(item["metrics"])
            # Construct the consistently typed adverse summary without invoking
            # the now-strict production reviewer sum helper.
            global_record = copy.deepcopy(original["census"]["global"])
            global_record["root"]["full_label"][field] = 0.5
            self.body["census"]["global"] = global_record
            with self.subTest(field=field), self.assertRaises(audit.ReviewBlocked):
                self.build()
        self.body = copy.deepcopy(original)
        first = self.body["census"]["leaves"][0]
        first["metrics"]["cleaning"]["root_distinct_gross_recorded_cash"] = -1.0
        self.body["census"]["months"][first["month"]] = copy.deepcopy(first["metrics"])
        self.body["census"]["global"]["cleaning"]["root_distinct_gross_recorded_cash"] = 171.0
        with self.assertRaises(audit.ReviewBlocked):
            self.build()
        self.assertFalse((self.base / "review").exists())


if __name__ == "__main__":
    unittest.main()
