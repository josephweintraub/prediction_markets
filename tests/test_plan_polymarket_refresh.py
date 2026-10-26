"""Synthetic source planning: no network, private configuration, or real data."""
from __future__ import annotations

from contextlib import redirect_stderr
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

from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner


SOURCE_HEAD = "a" * 40
PLANNER_HEAD = "b" * 40


def code(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def block_hash(number):
    return "0x" + f"{number + 1:064x}"


class FakeRPC:
    def __init__(self, *, first_excluded=11, latest=20):
        self.first_excluded = first_excluded
        self.latest = latest
        self.calls = []
        self.overrides = {}
        self.timestamps = {}
        self.header_counts = {}

    def timestamp(self, number):
        if number == 0:
            return 0
        return self.timestamps.get(number, planner.utc_cutoff(planner.END_EXCLUSIVE)
                                   + (number - self.first_excluded) * 10)

    def __call__(self, endpoint, payload):
        self.calls.append(payload)
        method = payload["method"]
        if method == "eth_chainId":
            result = "0x89"
        elif method == "eth_blockNumber":
            result = hex(self.latest)
        elif method == "eth_getBlockByNumber":
            number = int(payload["params"][0], 16)
            self.header_counts[number] = self.header_counts.get(number, 0) + 1
            result = {"number": hex(number), "hash": block_hash(number),
                      "parentHash": block_hash(number - 1), "timestamp": hex(self.timestamp(number))}
        elif method == "eth_getCode":
            result = "0x6000" if payload["params"][0] == planner.EXCHANGES[0][1] else "0x"
        else:
            raise AssertionError("forbidden method")
        result = self.overrides.get(method, lambda value, request: value)(result, payload)
        return {"jsonrpc": "2.0", "id": payload["id"], "result": result}


class RefreshPlanTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for path in self.paths.values():
            pq.write_table(pa.table({"block_number": [2, 5], "timestamp": [20, 50]}), path)
        self.inventory_path = self.root / "inventory.json"
        with patch.object(inventory, "code_state", return_value=code(SOURCE_HEAD)):
            self.value = inventory.build_inventory(self.paths, disk_path=self.root)
        self.save_inventory()
        self.rpc = FakeRPC()

    def save_inventory(self):
        self.inventory_path.write_text(json.dumps(self.value, sort_keys=True))
        self.digest = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()

    def execute(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=code(PLANNER_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")):
            return planner.plan_refresh(self.inventory_path, self.digest, SOURCE_HEAD,
                                        PLANNER_HEAD, transport=kwargs.pop("transport", self.rpc), **kwargs)

    def test_exact_boundary_append_range_code_presence_and_no_logs(self):
        result = self.execute()
        self.assertEqual(result["status"], "source_plan_ready_for_review")
        self.assertEqual(result["last_included_header"]["number"], 10)
        self.assertEqual(result["first_excluded_header"]["number"], 11)
        self.assertEqual(result["first_excluded_header"]["timestamp"], planner.utc_cutoff(planner.END_EXCLUSIVE))
        self.assertEqual(result["provisional_append_start"]["start_block"], 6)
        self.assertEqual(result["provisional_append_blocks"], 5)
        self.assertTrue(result["cutoff_headers_reopened_unchanged"])
        self.assertLessEqual(result["rpc_calls_attempted"], 100)
        self.assertEqual(len(result["exchange_code_presence"]), 5)
        self.assertEqual([item["code_present"] for item in result["exchange_code_presence"]],
                         [True, False, False, False, False])
        self.assertTrue(all(item["params"][1] == "0xa" for item in self.rpc.calls
                            if item["method"] == "eth_getCode"))
        encoded = json.dumps(result)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("0x6000", encoded)
        self.assertFalse(result["trade_logs_requested"])
        self.assertFalse(result["collection_approved"])

    def test_equal_timestamp_blocks_exclude_every_block_at_cutoff(self):
        cutoff = planner.utc_cutoff(planner.END_EXCLUSIVE)
        self.rpc.timestamps = {11: cutoff, 12: cutoff, 13: cutoff}
        result = self.execute()
        self.assertEqual(result["first_excluded_header"]["number"], 11)

    def test_inventory_digest_and_source_head_must_match(self):
        with patch.object(inventory, "load_rpc_endpoint", side_effect=AssertionError("no RPC")):
            result = planner.plan_refresh(self.inventory_path, "0" * 64, SOURCE_HEAD, PLANNER_HEAD)
        self.assertEqual(result["error_code"], "inventory_digest_differs")
        self.value["code_before"]["head"] = "c" * 40
        self.save_inventory()
        self.assertEqual(self.execute()["error_code"], "inventory_source_head_or_clean_state_differs")

    def test_inventory_strict_keys_duplicates_and_changed_identity_block(self):
        self.value["extra"] = "not admitted"
        self.save_inventory()
        self.assertEqual(self.execute()["error_code"], "inventory_schema_keys_differ")
        del self.value["extra"]
        self.save_inventory()
        source = self.paths["raw_events"]
        value = source.stat()
        os.utime(source, ns=(value.st_atime_ns, value.st_mtime_ns + 1))
        self.assertEqual(self.execute()["error_code"], "source_topology_changed_since_inventory")
        self.inventory_path.write_text('{"schema_version":"a","schema_version":"b"}')
        self.digest = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        self.assertEqual(self.execute()["error_code"], "inventory_json_invalid")

    def test_missing_deduped_inventory_is_explicitly_supported(self):
        self.paths["deduped_events"].unlink()
        with patch.object(inventory, "code_state", return_value=code(SOURCE_HEAD)):
            self.value = inventory.build_inventory(self.paths, disk_path=self.root)
        self.save_inventory()
        self.assertEqual(self.value["status"], "readiness_inventory_incomplete")
        self.assertEqual(self.execute()["status"], "source_plan_ready_for_review")

    def test_unknown_raw_maximum_requires_explicit_start(self):
        stats = self.value["inputs"]["raw_events"]["files"][0]["extrema"]["block_number"]
        stats.update(all_nonempty_nonnull_groups_have_minmax=False, declared_global_maximum=None)
        self.save_inventory()
        self.assertEqual(self.execute()["error_code"], "raw_footer_maximum_unknown_explicit_start_required")
        result = self.execute(start_block=7)
        self.assertEqual(result["status"], "source_plan_ready_for_review")
        self.assertEqual(result["provisional_append_start"]["start_block"], 7)
        self.assertIsNone(result["provisional_append_start"]["raw_footer_declared_maximum_block"])

    def test_known_raw_maximum_forbids_override(self):
        self.assertEqual(self.execute(start_block=6)["error_code"],
                         "explicit_start_forbidden_with_known_raw_footer_maximum")

    def test_fixed_cutoff_rejects_non_utc_malformed_and_other_dates(self):
        for value in ("2026-10-02", "2026-10-02T00:00:00+00:00", "2026-10-02T00:00:00.0Z",
                      "2026-02-30T00:00:00Z", "2026-10-01T00:00:00Z"):
            with self.subTest(value=value):
                result = self.execute(end_exclusive=value)
                self.assertEqual(result["status"], "blocked")
                self.assertEqual(result["rpc_calls_attempted"], 0)

    def test_wrong_chain_and_missing_or_boolean_response_ids_block(self):
        self.rpc.overrides["eth_chainId"] = lambda *_: "0x1"
        self.assertEqual(self.execute()["error_code"], "rpc_wrong_chain")
        for identifier in (None, True, 2):
            def transport(endpoint, payload):
                reply = {"jsonrpc": "2.0", "result": "0x89"}
                if identifier is not None:
                    reply["id"] = identifier
                return reply
            with self.subTest(identifier=identifier):
                self.assertEqual(self.execute(transport=transport)["status"], "blocked")

    def test_wrong_returned_block_missing_hash_or_missing_header_blocks(self):
        for update, code_name in (({"number": "0x99"}, "rpc_header_number_mismatch"),
                                  ({"hash": "bad"}, "rpc_invalid_header_hash"),
                                  ({"parentHash": None}, "rpc_invalid_header_hash")):
            with self.subTest(update=update):
                self.rpc = FakeRPC()
                self.rpc.overrides["eth_getBlockByNumber"] = lambda result, request: {**result, **update}
                self.assertEqual(self.execute()["error_code"], code_name)
        self.rpc.overrides["eth_getBlockByNumber"] = lambda *_: None
        self.assertEqual(self.execute()["error_code"], "rpc_header_not_object")

    def test_observed_nonmonotone_headers_fail_closed(self):
        cutoff = planner.utc_cutoff(planner.END_EXCLUSIVE)
        self.rpc.timestamps[15] = cutoff - 100
        result = self.execute()
        self.assertEqual(result["error_code"], "rpc_observed_nonmonotone_header_time")

    def test_broken_boundary_parent_link_is_rejected(self):
        self.rpc.overrides["eth_getBlockByNumber"] = lambda result, request: (
            {**result, "parentHash": "0x" + "f" * 64} if result["number"] == "0xb" else result)
        self.assertEqual(self.execute()["error_code"], "cutoff_parent_hash_proof_failed")

    def test_reopened_boundary_drift_is_rejected(self):
        def mutate(result, request):
            number = int(result["number"], 16)
            return ({**result, "hash": "0x" + "f" * 64}
                    if number == 10 and self.rpc.header_counts[number] > 1 else result)
        self.rpc.overrides["eth_getBlockByNumber"] = mutate
        self.assertEqual(self.execute()["error_code"], "rpc_reopened_header_changed")

    def test_source_drift_during_rpc_is_rejected(self):
        original = self.rpc
        changed = False
        def mutate(endpoint, payload):
            nonlocal changed
            if not changed:
                changed = True
                path = self.paths["raw_events"]
                value = path.stat()
                os.utime(path, ns=(value.st_atime_ns, value.st_mtime_ns + 1))
            return original(endpoint, payload)
        self.assertEqual(self.execute(transport=mutate)["error_code"], "source_topology_changed_since_inventory")

    def test_rpc_call_ceiling_and_sanitized_failure(self):
        with patch.object(planner, "MAX_RPC_CALLS", 2):
            result = self.execute()
        self.assertEqual(result["error_code"], "rpc_call_limit")
        self.assertEqual(result["rpc_calls_attempted"], 2)
        def fail(*_):
            raise RuntimeError("https://rpc.invalid/SECRET")
        result = self.execute(transport=fail)
        self.assertEqual(result["status"], "blocked")
        self.assertNotIn("SECRET", json.dumps(result))

    def test_invalid_code_data_is_not_presence(self):
        self.rpc.overrides["eth_getCode"] = lambda *_: "0x0"
        self.assertEqual(self.execute()["error_code"], "rpc_invalid_code_result")

    def test_guard_refusal_is_host_independent_and_before_any_io(self):
        errors = io.StringIO()
        output = self.root / "blocked.json"
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture refusal")), \
                patch.object(planner, "plan_refresh", side_effect=AssertionError("must not plan")) as plan, \
                redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                planner.main(["--inventory", str(self.inventory_path), "--inventory-sha256", self.digest,
                              "--inventory-source-head", SOURCE_HEAD, "--expected-head", PLANNER_HEAD,
                              "--output", str(output)])
        self.assertEqual(stopped.exception.code, 2)
        plan.assert_not_called()
        self.assertFalse(output.exists())

    def test_output_overlap_is_rejected_before_rpc_or_writes(self):
        source_directory = self.root / "source-directory"
        source_directory.mkdir()
        self.value["inputs"]["raw_events"]["path"] = str(source_directory)
        for destination in (source_directory / "plan.json", self.inventory_path, self.root / "rpc-private"):
            with self.subTest(destination=destination):
                with self.assertRaisesRegex(planner.PlanBlocked, "output_(overlaps_bound_input|already_exists)"):
                    planner.validate_output(destination, self.value, self.inventory_path, self.root / "rpc-private")
        planner.validate_output(self.root / "new-plan.json", self.value, self.inventory_path, self.root / "rpc-private")

    def test_cli_overlap_admission_does_not_call_planner(self):
        source_directory = self.root / "source-directory"
        source_directory.mkdir()
        self.value["inputs"]["raw_events"]["path"] = str(source_directory)
        self.save_inventory()
        output = source_directory / "plan.json"
        with patch("production_guard.require_production_host"), \
                patch.object(planner, "plan_refresh", side_effect=AssertionError("must not call RPC")) as plan, \
                redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                planner.main(["--inventory", str(self.inventory_path), "--inventory-sha256", self.digest,
                              "--inventory-source-head", SOURCE_HEAD, "--expected-head", PLANNER_HEAD,
                              "--output", str(output)])
        self.assertEqual(stopped.exception.code, 2)
        plan.assert_not_called()
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
