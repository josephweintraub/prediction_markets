"""Small local fixtures for required native block coverage; no real data."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_timestamp_coverage as audit


def write(path: Path, schema: tuple[tuple[str, str], ...], rows: list[dict]) -> None:
    kinds = {"string": pa.string(), "int64": pa.int64()}
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field(name, kinds[kind]) for name, kind in schema])), path, row_group_size=2)


def native(block: int | None) -> dict:
    return {"block_number": block, "maker": "private-maker-value",
            "taker": "private-taker-value", "transaction_hash": "private-transaction-value"}


class TimestampCoverageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.resolved = self.root / "native.parquet"
        self.cache = self.root / "timestamps.parquet"
        self.run = self.root / "run"
        self.native_rows = [native(10), native(10), native(20)]
        self.cache_rows = [{"block_number": 20, "timestamp": 200},
                           {"block_number": 10, "timestamp": 100}]

    def publish(self) -> None:
        write(self.resolved, audit.RESOLVED_SCHEMA, self.native_rows)
        write(self.cache, audit.CACHE_SCHEMA, self.cache_rows)

    def execute(self, **updates) -> dict:
        return audit.run_audit(self.resolved, self.cache, self.run,
                              expected_resolved_rows=len(self.native_rows),
                              expected_cache_rows=len(self.cache_rows), **updates)

    def checks(self, name: str) -> dict:
        return json.loads((self.run / (name + "_checks.json")).read_text())

    def test_every_native_row_counts_and_scope_is_narrow(self) -> None:
        self.publish()
        result = self.execute()
        self.assertEqual(result["status"], "required_block_cache_coverage_complete")
        self.assertEqual(result["certification_status"], "cache_coverage_and_internal_clock_consistency_only")
        self.assertEqual(result["required_rows"], 3)
        self.assertEqual(result["matched_required_rows"], 3)
        self.assertEqual(result["missing_required_rows"], 0)
        self.assertTrue(result["final_input_identity_verified"])
        required = self.checks("required")
        self.assertTrue(required["join_preserves_rows"])
        self.assertTrue(required["row_coverage_conserved"])
        inventory = json.loads((self.run / "inventory.json").read_text())
        self.assertEqual(inventory["planned_parquet_body_passes"],
                         {"resolved_block_number": 1, "cache_block_number_timestamp": 1})
        payload = "".join(path.read_text() for path in self.run.glob("*.json"))
        for private in ("private-maker-value", "private-taker-value", "private-transaction-value"):
            self.assertNotIn(private, payload)
        self.assertIn("duckdb", result["environment"])
        self.assertGreater(result["resource_profile"]["peak_rss_bytes"], 0)
        self.assertEqual(result["engine_settings"]["threads"], 4)
        self.assertEqual(result["engine_settings"]["temp_directory"], "")

    def test_equal_cache_count_decoy_cannot_mask_missing_required_block(self) -> None:
        self.native_rows = [native(10), native(20)]
        self.cache_rows = [{"block_number": 10, "timestamp": 100},
                           {"block_number": 30, "timestamp": 300}]
        self.publish()
        result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["missing_required_rows"], 1)
        self.assertEqual(result["missing_required_blocks"], 1)
        self.assertEqual(result["certification_status"], "not_certified")

    def test_missing_block_counts_are_distinct_but_rows_are_not(self) -> None:
        self.native_rows = [native(10), native(20), native(20), native(30)]
        self.cache_rows = [{"block_number": 10, "timestamp": 100}]
        self.publish()
        result = self.execute()
        self.assertEqual(result["missing_required_rows"], 3)
        self.assertEqual(result["missing_required_blocks"], 2)
        self.assertEqual(result["required_rows"], 4)

    def test_duplicate_cache_keys_identical_or_conflicting_fail_before_join(self) -> None:
        for timestamp in (100, 101):
            with self.subTest(timestamp=timestamp):
                self.cache_rows = [{"block_number": 10, "timestamp": 100},
                                   {"block_number": 10, "timestamp": timestamp}]
                self.publish()
                self.run = self.root / ("duplicates-" + str(timestamp))
                with patch.object(audit, "scan_required_rows", side_effect=AssertionError("must not join")):
                    result = self.execute()
                self.assertEqual(result["status"], "blocked_integrity")
                self.assertFalse(result["required_rows_scan_completed"])
                self.assertEqual(self.checks("cache")["cache_duplicate_block_keys"], 1)
                self.assertEqual(self.checks("cache")["cache_duplicate_excess_rows"], 1)
                self.assertFalse(self.checks("cache")["clock_order_checked"])

    def test_invalid_cache_key_and_clock_values_fail_before_join(self) -> None:
        for column, value, counter in (
            ("block_number", None, "cache_null_block_rows"),
            ("block_number", 0, "cache_nonpositive_block_rows"),
            ("block_number", -1, "cache_nonpositive_block_rows"),
            ("timestamp", None, "cache_null_timestamp_rows"),
            ("timestamp", 0, "cache_nonpositive_timestamp_rows"),
            ("timestamp", -1, "cache_nonpositive_timestamp_rows"),
        ):
            with self.subTest(column=column, value=value):
                self.cache_rows = [{"block_number": 10, "timestamp": 100, column: value}]
                self.publish()
                self.run = self.root / (column + "-" + str(value))
                with patch.object(audit, "scan_required_rows", side_effect=AssertionError("must not join")):
                    result = self.execute()
                self.assertEqual(result["status"], "blocked_integrity")
                self.assertEqual(self.checks("cache")[counter], 1)
                self.assertFalse(result["required_rows_scan_completed"])

    def test_invalid_native_blocks_are_counted_not_silently_dropped(self) -> None:
        self.native_rows = [native(10), native(None), native(0), native(-1)]
        self.publish()
        result = self.execute()
        required = self.checks("required")
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["required_rows"], 4)
        self.assertEqual(required["required_null_block_rows"], 1)
        self.assertEqual(required["required_nonpositive_block_rows"], 2)
        self.assertEqual(required["missing_required_rows"], 3)
        self.assertEqual(required["missing_required_blocks"], 2)
        self.assertEqual(required["missing_required_null_block_rows"], 1)

    def test_global_clock_order_uses_blocks_not_cache_file_order(self) -> None:
        self.cache_rows = [{"block_number": 10, "timestamp": 100},
                           {"block_number": 30, "timestamp": 105},
                           {"block_number": 20, "timestamp": 110}]
        self.publish()
        with patch.object(audit, "scan_required_rows", side_effect=AssertionError("must not join")):
            result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(self.checks("cache")["cache_clock_reversals"], 1)

    def test_equal_timestamp_steps_and_unneeded_cached_blocks_are_valid(self) -> None:
        self.cache_rows = [{"block_number": 20, "timestamp": 100},
                           {"block_number": 30, "timestamp": 101},
                           {"block_number": 10, "timestamp": 100}]
        self.publish()
        self.assertEqual(self.execute()["status"], "required_block_cache_coverage_complete")
        self.assertEqual(self.checks("cache")["cache_equal_timestamp_steps"], 1)
        self.assertEqual(self.checks("cache")["cache_clock_reversals"], 0)

    def test_schema_and_frozen_counts_fail_before_any_body_query(self) -> None:
        self.publish()
        for target, schema, rows in (
            (self.resolved, audit.RESOLVED_SCHEMA[:-1], self.native_rows),
            (self.cache, (("block_number", "int64"),), self.cache_rows),
        ):
            with self.subTest(target=target.name):
                self.publish()
                write(target, schema, rows)
                self.run = self.root / ("schema-" + target.name)
                with patch.object(audit, "prepare_cache", side_effect=AssertionError("must not scan")):
                    result = self.execute()
                self.assertEqual(result["status"], "blocked_integrity")
                self.assertEqual(result["failure_stage"], "footer_inventory")
        for resolved_count, cache_count in ((999, len(self.cache_rows)), (len(self.native_rows), 999)):
            with self.subTest(resolved_count=resolved_count, cache_count=cache_count):
                self.publish()
                self.run = self.root / ("count-" + str(resolved_count) + "-" + str(cache_count))
                with patch.object(audit, "prepare_cache", side_effect=AssertionError("must not scan")):
                    result = audit.run_audit(self.resolved, self.cache, self.run,
                                            expected_resolved_rows=resolved_count, expected_cache_rows=cache_count)
                self.assertEqual(result["status"], "blocked_integrity")

    def test_changed_source_after_scan_cannot_be_certified(self) -> None:
        self.publish()
        original = audit.scan_required_rows
        def changed(con, resolved, expected_rows):
            result = original(con, resolved, expected_rows)
            current = self.cache.stat()
            os.utime(self.cache, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000))
            return result
        with patch.object(audit, "scan_required_rows", side_effect=changed):
            result = self.execute()
        self.assertEqual(result["status"], "blocked_integrity")
        self.assertEqual(result["failure_stage"], "final_inventory_reopen")
        self.assertFalse(result["final_input_identity_verified"])
        self.assertEqual(result["certification_status"], "not_certified")

    def test_incomplete_execution_preserves_completed_cache_proof(self) -> None:
        self.publish()
        with patch.object(audit, "scan_required_rows", side_effect=RuntimeError("private-maker-value")):
            result = self.execute()
        self.assertEqual(result["status"], "incomplete_execution_error")
        self.assertEqual(result["failure_stage"], "required_native_rows")
        self.assertTrue(result["cache_preflight_completed"])
        self.assertFalse(result["required_rows_scan_completed"])
        self.assertTrue((self.run / "cache_checks.json").exists())
        self.assertFalse((self.run / "required_checks.json").exists())
        self.assertNotIn("private-maker-value", (self.run / "summary.json").read_text())
        self.assertEqual(result["certification_status"], "not_certified")

    def test_interruption_is_visible_not_successful(self) -> None:
        self.publish()
        with patch.object(audit, "scan_required_rows", side_effect=KeyboardInterrupt()):
            result = self.execute()
        self.assertEqual(result["status"], "interrupted")
        self.assertFalse(result["required_rows_scan_completed"])
        self.assertEqual(result["certification_status"], "not_certified")

    def test_existing_run_is_never_overwritten(self) -> None:
        self.publish()
        self.execute()
        original = (self.run / "summary.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.execute()
        self.assertEqual((self.run / "summary.json").read_bytes(), original)

    def test_output_input_overlap_and_input_symlink_are_refused(self) -> None:
        self.publish()
        with self.assertRaises(audit.AuditBlocked):
            audit.run_audit(self.resolved, self.cache, self.root)
        link = self.root / "native-link.parquet"
        link.symlink_to(self.resolved)
        with self.assertRaises(audit.AuditBlocked):
            audit.run_audit(link, self.cache, self.run)
        self.assertFalse(self.run.exists())

    def test_local_cli_production_guard_refuses_before_run_creation(self) -> None:
        command = [sys.executable, "-S", str(Path(audit.__file__).resolve()),
                   "--expected-head", "a" * 40, "--run-dir", str(self.run)]
        result = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.run.exists())
        self.assertNotIn("duckdb", result.stderr.lower())
        self.assertNotIn("ModuleNotFoundError", result.stderr)
        help_result = subprocess.run([sys.executable, "-S", str(Path(audit.__file__).resolve()), "--help"],
                                     text=True, capture_output=True)
        self.assertEqual(help_result.returncode, 0, help_result.stderr)

    def test_native_join_query_plan_has_one_projected_parquet_scan(self) -> None:
        self.publish()
        original = audit._record
        plans = []
        def explain(con, sql):
            if "FROM required_native n" in sql:
                plans.append(json.loads(con.execute("EXPLAIN (FORMAT JSON) " + sql).fetchone()[1]))
            return original(con, sql)
        with patch.object(audit, "_record", side_effect=explain):
            self.assertEqual(self.execute()["status"], "required_block_cache_coverage_complete")
        self.assertEqual(len(plans), 1)
        def nodes(items):
            for item in items:
                yield item
                yield from nodes(item["children"])
        scans = [node for node in nodes(plans[0]) if node["name"].strip() == "READ_PARQUET"]
        self.assertEqual(len(scans), 1)
        self.assertEqual(scans[0]["extra_info"]["Projections"], "block_number")


if __name__ == "__main__":
    unittest.main()
