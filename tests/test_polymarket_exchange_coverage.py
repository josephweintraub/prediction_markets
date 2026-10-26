"""Tiny synthetic raw coverage audits, never real data or network."""
from __future__ import annotations

from contextlib import redirect_stderr
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_exchange_coverage as coverage
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_protocol_activity as activity
from tests.test_polymarket_protocol_activity import code, header

INVENTORY_HEAD, PLAN_HEAD, AUDIT_HEAD = (character * 40 for character in "abc")


class ExchangeCoverageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        self.inventory_path = self.root / "inventory.json"
        self.plan_path = self.root / "plan.json"
        self.probe_path = self.root / "probe.json"
        self.populate([(coverage.EXCHANGES[0][1], 1), (activity.ADDRESS, 500)])

    def populate(self, rows, *, omit=None):
        for path in self.paths.values():
            pq.write_table(pa.table({"block_number": [1, 500]}), path)
        fields = {"exchange_address": pa.array([row[0] for row in rows], type=pa.string()),
                  "block_number": pa.array([row[1] for row in rows], type=pa.int64()),
                  "private_wallet_body": pa.array(["DO-NOT-EXPORT"] * len(rows))}
        if omit:
            del fields[omit]
        pq.write_table(pa.table(fields), self.paths["raw_events"], row_group_size=2)
        with patch.object(inventory, "code_state", return_value=code(INVENTORY_HEAD)):
            self.source_inventory = inventory.build_inventory(self.paths, disk_path=self.root)
        self.inventory_path.write_text(json.dumps(self.source_inventory))
        inventory_sha = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        _, binding = planner.read_inventory(self.inventory_path, inventory_sha, INVENTORY_HEAD)
        self.plan = {"schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": code(PLAN_HEAD), "inventory_binding": binding,
            "final_input_identity_unchanged": True, "cutoff_headers_reopened_unchanged": True,
            "trade_logs_requested": False, "parquet_body_reads": 0,
            "collection_approved": False, "exchange_abi_admitted": False,
            "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(1000), "first_excluded_header": header(1001),
            "raw_footer_tail_header": header(500),
            "provisional_append_start": {"start_block": 501, "raw_footer_declared_maximum_block": 500},
            "cutoff_proof": {key: True for key in ("adjacent_block_numbers", "parent_hash_matches",
                "last_time_strictly_before_cutoff", "first_time_at_or_after_cutoff")}}
        self.save_bindings()

    def save_bindings(self):
        self.plan_path.write_text(json.dumps(self.plan))
        self.plan_sha = hashlib.sha256(self.plan_path.read_bytes()).hexdigest()
        binding = {"path": str(self.plan_path), "sha256": self.plan_sha, "source_head": PLAN_HEAD,
                   "identity": inventory.path_identity(self.plan_path)}
        self.probe = {"schema_version": "polymarket_protocol_activity_probe_v1",
            "status": "bounded_activity_probe_complete", "source_plan_binding": binding,
            "address": activity.ADDRESS, "topic0": activity.TOPIC0, "final_input_identity_unchanged": True,
            "collection_approved": False, "exchange_abi_admitted": False,
            "windows": [{"label": "historical_tail", "start_block": 401, "end_block": 500,
                         "matching_log_count": 1, "presence_observed": True}]}
        self.probe_path.write_text(json.dumps(self.probe))
        self.probe_sha = hashlib.sha256(self.probe_path.read_bytes()).hexdigest()

    def execute(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=code(AUDIT_HEAD)), \
                patch.object(activity, "http_rpc", side_effect=AssertionError("no RPC")):
            return coverage.audit(self.plan_path, self.plan_sha, PLAN_HEAD, self.probe_path,
                                  self.probe_sha, AUDIT_HEAD, **kwargs)

    def test_one_two_column_scan_six_buckets_and_no_body_values(self):
        result = self.execute()
        self.assertEqual(result["status"], "exchange_coverage_audit_complete")
        self.assertTrue(result["audit_checks_passed"])
        self.assertEqual(result["large_body_projection_queries"], 1)
        self.assertEqual(result["rpc_calls_attempted"], 0)
        self.assertEqual(result["counts"]["total_rows"], 2)
        self.assertEqual(len(result["counts"]["buckets"]), 6)
        self.assertEqual(result["projection_plan"]["projected_columns"], ["exchange_address", "block_number"])
        self.assertTrue(result["counts"]["known_positive_probe"]["count_matches_probe"])
        self.assertNotIn("DO-NOT-EXPORT", json.dumps(result))
        self.assertFalse(result["raw_final_admission"])
        self.assertEqual(result["manual_footer_budget"]["files"], 2)

    def test_v3_absence_proves_known_missing_activity_not_entire_history(self):
        self.populate([(coverage.EXCHANGES[0][1], 1), (coverage.EXCHANGES[1][1], 500)])
        result = self.execute()
        self.assertEqual(result["status"], "exchange_coverage_audit_complete")
        self.assertFalse(result["audit_checks_passed"])
        self.assertTrue(result["counts"]["known_positive_probe"]["known_activity_absent_from_archive"])
        self.assertIn("known observations are missing", result["counts"]["known_positive_probe"]["interpretation"])

    def test_v3_outside_known_window_does_not_satisfy_known_presence(self):
        self.populate([(activity.ADDRESS, 1), (coverage.EXCHANGES[0][1], 500)])
        result = self.execute()
        self.assertFalse(result["audit_checks_passed"])
        self.assertEqual(result["counts"]["known_positive_probe"]["archive_address_window_rows"], 0)

    def test_uppercase_valid_addresses_and_every_known_bucket(self):
        self.populate([(address.upper(), 500 if i == 4 else i + 1)
                       for i, (_, address) in enumerate(coverage.EXCHANGES)])
        result = self.execute()
        self.assertTrue(result["audit_checks_passed"])
        self.assertEqual([item["rows"] for item in result["counts"]["buckets"]], [1, 1, 1, 1, 1, 0])

    def test_null_invalid_and_unsupported_rows_are_preserved_and_counted(self):
        self.populate([(activity.ADDRESS, 500), (None, None), ("", 0),
                       ("0x" + "a" * 40, -1), (coverage.EXCHANGES[0][1], None)])
        result = self.execute()
        self.assertEqual(result["status"], "exchange_coverage_audit_complete")
        self.assertEqual(result["counts"]["total_rows"], 5)
        self.assertEqual(result["counts"]["invalidity_counts"],
                         {"null_address_rows": 1, "invalid_address_rows": 1,
                          "null_block_rows": 2, "invalid_block_rows": 2})
        self.assertEqual(result["counts"]["unsupported_address_rows"], 3)
        self.assertFalse(result["audit_checks_passed"])

    def test_projected_plan_limit_stops_before_body_scan(self):
        with patch.object(coverage, "execute_scan", side_effect=AssertionError("must not scan")) as scan:
            result = self.execute(maximum_projected_bytes=1)
        self.assertEqual(result["error_code"], "projected_compressed_byte_limit")
        self.assertEqual(result["large_body_projection_queries"], 0)
        self.assertGreater(result["projection_plan"]["projected_compressed_bytes"], 1)
        scan.assert_not_called()

    def test_missing_projection_column_blocks_before_scan(self):
        self.populate([(activity.ADDRESS, 500)], omit="exchange_address")
        result = self.execute()
        self.assertEqual(result["error_code"], "raw_projected_columns_absent_or_duplicate")
        self.assertEqual(result["large_body_projection_queries"], 0)

    def test_count_conservation_and_declared_footer_count(self):
        records = [{"bucket": "v3_candidate", "rows": 1, "null_address_rows": 0,
            "invalid_address_rows": 0, "null_block_rows": 0, "invalid_block_rows": 0,
            "minimum_nonnull_block": 500, "maximum_nonnull_block": 500, "known_probe_window_rows": 1}]
        with patch.object(coverage, "execute_scan", return_value=records):
            result = self.execute()
        self.assertEqual(result["error_code"], "scan_rows_do_not_match_footer")
        modified = copy.deepcopy(self.source_inventory["inputs"]["raw_events"]["files"][0])
        modified["rows_declared"] += 1
        with self.assertRaisesRegex(coverage.CoverageBlocked, "raw_footer_rows_differ_from_inventory"):
            coverage.inspect_projection(modified, inventory.Budget())

    def test_raw_drift_during_scan_is_not_accepted(self):
        original = coverage.execute_scan
        def mutate(plan, window):
            result = original(plan, window)
            path = self.paths["raw_events"]
            value = path.stat()
            os.utime(path, ns=(value.st_atime_ns, value.st_mtime_ns + 1))
            return result
        with patch.object(coverage, "execute_scan", side_effect=mutate):
            result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["error_code"], "raw_identity_differs_from_inventory")

    def test_bound_inventory_json_drift_during_scan_blocks(self):
        original = coverage.execute_scan
        def mutate(plan, window):
            result = original(plan, window)
            self.inventory_path.write_text(self.inventory_path.read_text() + " ")
            return result
        with patch.object(coverage, "execute_scan", side_effect=mutate):
            result = self.execute()
        self.assertEqual(result["error_code"], "inventory_changed_during_audit")

    def test_probe_digest_binding_and_positive_window_required(self):
        self.probe_sha = "0" * 64
        self.assertEqual(self.execute()["error_code"], "activity_probe_digest_differs")
        self.save_bindings()
        self.probe["windows"][0]["matching_log_count"] = 0
        self.probe_path.write_text(json.dumps(self.probe))
        self.probe_sha = hashlib.sha256(self.probe_path.read_bytes()).hexdigest()
        self.assertEqual(self.execute()["error_code"], "historical_positive_probe_window_differs")

    def test_partial_known_count_shortfall_also_fails_checks(self):
        self.probe["windows"][0]["matching_log_count"] = 2
        self.probe_path.write_text(json.dumps(self.probe))
        self.probe_sha = hashlib.sha256(self.probe_path.read_bytes()).hexdigest()
        result = self.execute()
        self.assertEqual(result["status"], "exchange_coverage_audit_complete")
        self.assertFalse(result["audit_checks_passed"])
        self.assertTrue(result["counts"]["known_positive_probe"]["archive_count_below_known_probe"])

    def test_exclusive_destination_and_local_guard(self):
        result = self.execute(output=self.plan_path)
        self.assertEqual(result["error_code"], "output_already_exists")
        errors = io.StringIO()
        output = self.root / "audit.json"
        with patch("production_guard.require_production_host", side_effect=RuntimeError("refusal")), \
                patch.object(coverage, "audit", side_effect=AssertionError("must not scan")), redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                coverage.main(["--source-plan", str(self.plan_path), "--source-plan-sha256", self.plan_sha,
                    "--source-plan-source-head", PLAN_HEAD, "--activity-probe", str(self.probe_path),
                    "--activity-probe-sha256", self.probe_sha, "--expected-head", AUDIT_HEAD, "--output", str(output)])
        self.assertEqual(stopped.exception.code, 2)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
