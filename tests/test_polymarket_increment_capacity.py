"""Small synthetic raw-log pilot fixtures; no real RPC, data, or host use."""
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
from scripts import probe_polymarket_increment_capacity as pilot

INVENTORY_HEAD, PLAN_HEAD, PILOT_HEAD = "a" * 40, "b" * 40, "c" * 40


def code(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def block_hash(number):
    return "0x" + hashlib.sha256(str(number).encode()).hexdigest()


def header(number):
    return {"number": number, "hash": block_hash(number), "parentHash": block_hash(number - 1),
            "timestamp": planner.utc_cutoff(planner.END_EXCLUSIVE) - 3000 + number}


def native_log(address, block, index=0):
    topic = pilot.EXPECTED_TOPICS[address]
    return {"address": address, "blockNumber": hex(block), "blockHash": block_hash(block),
            "transactionHash": "0x" + hashlib.sha256(f"tx-{block}-{index}".encode()).hexdigest(),
            "transactionIndex": hex(index), "logIndex": hex(index), "removed": False,
            "topics": [topic, "0x" + "a" * 64, "0x" + "0" * 24 + "b" * 40,
                       "0x" + "0" * 24 + "c" * 40],
            "data": "0x" + "01" * (160 if topic == pilot.LEGACY_TOPIC else 224)}


class FakeRPC:
    def __init__(self, *, empty=False):
        self.calls = []
        self.empty = empty
        self.mutate = None

    def __call__(self, endpoint, payload):
        self.calls.append(payload)
        method = payload["method"]
        if method == "eth_chainId":
            result = "0x89"
        elif method == "eth_getBlockByNumber":
            number = int(payload["params"][0], 16)
            value = header(number)
            result = {**value, "number": hex(number), "timestamp": hex(value["timestamp"])}
        elif method == "eth_getLogs":
            query = payload["params"][0]
            first = int(query["fromBlock"], 16)
            result = [] if self.empty else [native_log(address, first + index, index)
                                           for index, address in enumerate(pilot.ADDRESSES)]
        else:
            raise AssertionError("forbidden method")
        reply = {"jsonrpc": "2.0", "id": payload["id"], "result": result}
        if self.mutate is not None:
            reply = self.mutate(payload, reply)
        return json.dumps(reply).encode()


class IncrementCapacityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for path in paths.values():
            pq.write_table(pa.table({"block_number": [998, 999]}), path)
        with patch.object(inventory, "code_state", return_value=code(INVENTORY_HEAD)):
            value = inventory.build_inventory(paths, disk_path=self.root)
        self.inventory_path = self.root / "inventory.json"
        self.inventory_path.write_text(json.dumps(value))
        inventory_digest = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        self.plan = {
            "schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": code(PLAN_HEAD), "final_input_identity_unchanged": True,
            "cutoff_headers_reopened_unchanged": True, "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(2999), "first_excluded_header": header(3000),
            "provisional_append_start": planner.raw_append_start(value, None), "provisional_append_blocks": 2000,
            "raw_footer_tail_header": header(999),
            "inventory_binding": {"path": str(self.inventory_path), "sha256": inventory_digest,
                                  "source_head": INVENTORY_HEAD,
                                  "identity": inventory.path_identity(self.inventory_path)},
        }
        self.plan_path = self.root / "plan.json"
        self.save_plan()
        self.output = self.root / "pilot-v1"
        self.rpc = FakeRPC()

    def save_plan(self):
        self.plan_path.write_text(json.dumps(self.plan))
        self.digest = hashlib.sha256(self.plan_path.read_bytes()).hexdigest()

    def execute(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=code(PILOT_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")):
            return pilot.run_pilot(self.plan_path, self.digest, PLAN_HEAD, PILOT_HEAD, self.output,
                                   transport=kwargs.pop("transport", self.rpc), **kwargs)

    def test_fixed_windows_are_disjoint_exact_and_not_data_selected(self):
        windows = pilot.fixed_windows(1000, 2999)
        self.assertEqual([(item["first_block"], item["last_block"]) for item in windows],
                         [(1450, 1549), (1950, 2049), (2450, 2549)])
        for bad in ((0, 1000), (10, 309), (1000, 100), (True, 1000)):
            with self.subTest(bad=bad), self.assertRaises(pilot.PilotBlocked):
                pilot.fixed_windows(*bad)

    def test_complete_pilot_roundtrip_registry_counts_and_marker(self):
        result = self.execute()
        self.assertEqual(result["status"], "bounded_pilot_complete")
        self.assertEqual(result["rpc_calls_attempted"], 15)
        self.assertEqual(result["sampled_rows"], 15)
        self.assertEqual(len(result["windows"]), 3)
        self.assertFalse(result["collection_complete"])
        self.assertFalse(result["analysis_admitted"])
        self.assertFalse(result["economic_deduplication"])
        marker = json.loads((self.output / "COMPLETE.json").read_text())
        self.assertEqual(marker["manifest_sha256"], hashlib.sha256((self.output / "manifest.json").read_bytes()).hexdigest())
        self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), pilot.MAX_OUTPUT_BYTES)
        for window in result["windows"]:
            rows = pq.read_table(self.output / window["parquet_path"]).to_pylist()
            self.assertEqual(len(rows), 5)
            self.assertEqual(rows[0]["data"], native_log(pilot.ADDRESSES[0], window["first_block"])["data"])
            self.assertEqual(sum(item["rows"] for item in window["counts"]), 5)
            self.assertFalse(window["interior_block_headers_verified"])
            self.assertFalse(window["interior_exact_timestamps_collected"])
        log_calls = [call for call in self.rpc.calls if call["method"] == "eth_getLogs"]
        self.assertEqual(len(log_calls), 3)
        self.assertTrue(all(call["params"][0]["address"] == list(pilot.ADDRESSES) for call in log_calls))
        self.assertTrue(all(call["params"][0]["topics"] == [[pilot.LEGACY_TOPIC, pilot.MODERN_TOPIC]] for call in log_calls))
        self.assertNotIn("SECRET", (self.output / "manifest.json").read_text())

    def test_all_zero_windows_are_saved_not_a_zero_population_certificate(self):
        self.rpc.empty = True
        result = self.execute()
        self.assertEqual(result["status"], "bounded_pilot_complete")
        self.assertTrue(all(item["zero_log_window"] for item in result["windows"]))
        self.assertEqual(result["sampled_rows"], 0)
        self.assertEqual(result["density_scenarios"]["status"], "not_computed_no_observed_logs")
        self.assertEqual(result["density_scenarios"]["scenarios"], [])
        for window in result["windows"]:
            self.assertEqual(pq.read_table(self.output / window["parquet_path"]).num_rows, 0)

    def test_optional_provider_provenance_presence_absence_and_header_independence(self):
        raw_value = "not a verified block clock"
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs":
                reply["result"][0]["blockTimestamp"] = raw_value
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["status"], "bounded_pilot_complete")
        self.assertFalse(result["optional_provider_provenance"]["blockTimestamp"]["used_for_clock_identity_or_eligibility"])
        for window in result["windows"]:
            rows = pq.read_table(self.output / window["parquet_path"]).to_pylist()
            self.assertEqual(rows[0]["blockTimestamp"], raw_value)
            self.assertTrue(rows[0]["provider_block_timestamp_present"])
            self.assertIsNone(rows[1]["blockTimestamp"])
            self.assertFalse(rows[1]["provider_block_timestamp_present"])
            self.assertEqual(window["provider_block_timestamp_rows"], 1)
            self.assertFalse(window["provider_block_timestamp_used_as_clock"])
            self.assertFalse(window["interior_exact_timestamps_collected"])
            self.assertEqual(window["first_header"]["timestamp"], header(window["first_block"])["timestamp"])
            self.assertNotIn("timestamp", rows[0])

    def test_provider_provenance_has_explicit_bounded_string_gate(self):
        window = pilot.fixed_windows(1000, 2999)[0]
        original = native_log(pilot.ADDRESSES[0], window["first_block"])
        for bad in (None, 1, True, b"bytes", "", "a" * 129, "clock\n", "\t", "\x00", "\x7f", "é"):
            with self.subTest(bad=bad), self.assertRaisesRegex(pilot.PilotBlocked, "invalid_provider_block_timestamp_provenance"):
                pilot.validate_logs([{**original, "blockTimestamp": bad}], window, {}, 1)
        with self.assertRaisesRegex(pilot.PilotBlocked, "native_log_schema_differs"):
            pilot.validate_logs([{**original, "unreviewedProviderField": "anything"}], window, {}, 1)

    def test_existing_output_is_not_overwritten(self):
        self.output.mkdir()
        keep = self.output / "keep"
        keep.write_text("original")
        with self.assertRaises(planner.PlanBlocked):
            self.execute()
        self.assertEqual(keep.read_text(), "original")
        self.assertEqual(self.rpc.calls, [])

    def test_duplicate_and_conflicting_native_keys_fail_without_deduplication(self):
        window = pilot.fixed_windows(1000, 2999)[0]
        log = native_log(pilot.ADDRESSES[0], window["first_block"])
        for second, code_name in ((dict(log), "duplicate_native_log_key"),
                                  ({**log, "data": "0x" + "02" * 160}, "conflicting_native_log_key")):
            with self.subTest(code=code_name), self.assertRaisesRegex(pilot.PilotBlocked, code_name):
                pilot.validate_logs([log, second], window, {}, 1)

    def test_conflicting_native_key_cannot_reappear_in_a_later_window(self):
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs":
                reply["result"][0]["transactionHash"] = "0x" + "f" * 64
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "conflicting_native_log_key_across_windows")
        self.assertEqual(result["status"], "partial_pilot")
        self.assertEqual(len(result["windows"]), 1)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_transaction_cannot_reappear_with_different_log_index_in_later_block(self):
        count = 0
        def mutate(payload, reply):
            nonlocal count
            if payload["method"] == "eth_getLogs":
                count += 1
                reply["result"][0]["transactionHash"] = "0x" + "f" * 64
                reply["result"][0]["logIndex"] = hex(100 + count)
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "inconsistent_transaction_position_across_windows")
        self.assertEqual(result["status"], "partial_pilot")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_window_address_topics_removed_and_native_uint_fields(self):
        window = pilot.fixed_windows(1000, 2999)[0]
        original = native_log(pilot.ADDRESSES[0], window["first_block"])
        cases = [({"blockNumber": hex(window["last_block"] + 1)}, "log_outside_requested_window"),
                 ({"address": "0x" + "f" * 40}, "unexpected_log_address"),
                 ({"topics": [pilot.MODERN_TOPIC, *original["topics"][1:]]}, "unexpected_address_topic_pair"),
                 ({"topics": original["topics"][:3]}, "invalid_log_topics"),
                 ({"removed": True}, "removed_or_unconfirmed_log"),
                 ({"removed": 0}, "removed_or_unconfirmed_log"),
                 ({"blockHash": "0x12"}, "invalid_native_log_hash"),
                 ({"transactionHash": None}, "invalid_native_log_hash"),
                 ({"data": "0x01"}, "raw_event_data_width_invalid")]
        for update, code_name in cases:
            with self.subTest(update=update), self.assertRaisesRegex(pilot.PilotBlocked, code_name):
                pilot.validate_logs([{**original, **update}], window, {}, 1)
        for field in ("blockNumber", "logIndex", "transactionIndex"):
            for bad in (None, True, -1, "0x00", "0x-1", hex(2**64)):
                with self.subTest(field=field, bad=bad), self.assertRaises((pilot.PilotBlocked, inventory.ReadinessError)):
                    pilot.validate_logs([{**original, field: bad}], window, {}, 1)

    def test_same_block_hash_and_endpoint_header_consistency(self):
        window = pilot.fixed_windows(1000, 2999)[0]
        first = native_log(pilot.ADDRESSES[0], window["first_block"], 0)
        second = native_log(pilot.ADDRESSES[1], window["first_block"], 1)
        second["blockHash"] = "0x" + "f" * 64
        with self.assertRaisesRegex(pilot.PilotBlocked, "inconsistent_same_block_hash"):
            pilot.validate_logs([first, second], window, {}, 1)
        with self.assertRaisesRegex(pilot.PilotBlocked, "log_endpoint_header_hash_mismatch"):
            pilot.validate_logs([first], window, {window["first_block"]: {"hash": "0x" + "f" * 64}}, 1)

    def test_transaction_index_and_global_block_log_position_are_consistent(self):
        window = pilot.fixed_windows(1000, 2999)[0]
        first = native_log(pilot.ADDRESSES[0], window["first_block"], 0)
        second = native_log(pilot.ADDRESSES[1], window["first_block"], 1)
        second["transactionIndex"] = "0x0"
        with self.assertRaisesRegex(pilot.PilotBlocked, "conflicting_block_transaction_index"):
            pilot.validate_logs([first, second], window, {}, 1)
        second = native_log(pilot.ADDRESSES[1], window["first_block"], 1)
        second["logIndex"] = "0x0"
        with self.assertRaisesRegex(pilot.PilotBlocked, "conflicting_block_log_position"):
            pilot.validate_logs([first, second], window, {}, 1)

    def test_wrong_rpc_id_chain_and_raw_response_limit(self):
        for mutate, code_name in ((lambda payload, reply: {**reply, "id": True}, "rpc_reply_id_mismatch"),
                                  (lambda payload, reply: {key: value for key, value in reply.items() if key != "id"},
                                   "rpc_invalid_reply_shape"),
                                  (lambda payload, reply: {**reply, "result": "0x1"}, "rpc_wrong_chain")):
            with self.subTest(code=code_name):
                self.output = self.root / code_name
                self.rpc = FakeRPC()
                self.rpc.mutate = mutate
                result = self.execute()
                self.assertEqual(result["error_code"], code_name)
                self.assertFalse((self.output / "COMPLETE.json").exists())
        self.output = self.root / "oversized"
        with patch.object(pilot, "MAX_RESPONSE_BYTES", 20):
            result = self.execute(transport=lambda *_: b"x" * 21)
        self.assertEqual(result["error_code"], "rpc_response_byte_limit")
        self.assertEqual(result["rpc_calls_attempted"], 1)

    def test_per_window_row_and_total_output_limits_fail_closed(self):
        with patch.object(pilot, "MAX_WINDOW_ROWS", 1):
            result = self.execute()
        self.assertEqual(result["error_code"], "window_log_row_limit_or_shape")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.output = self.root / "output-cap"
        with patch.object(pilot, "MAX_OUTPUT_BYTES", pilot.MANIFEST_RESERVE + 512), \
                patch.object(pilot, "parquet_payload", return_value=b"x" * 1024):
            result = self.execute()
        self.assertEqual(result["error_code"], "pilot_output_byte_limit")
        self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), pilot.MANIFEST_RESERVE + 512)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_partial_run_retains_evidence_without_complete_marker_and_secrets(self):
        original = self.rpc
        log_calls = 0
        def transport(endpoint, payload):
            nonlocal log_calls
            if payload["method"] == "eth_getLogs":
                log_calls += 1
                if log_calls == 2:
                    raise RuntimeError("https://rpc.invalid/SECRET")
            return original(endpoint, payload)
        result = self.execute(transport=transport)
        self.assertEqual(result["status"], "partial_pilot")
        self.assertEqual(len(result["windows"]), 1)
        self.assertTrue((self.output / "q25.parquet").exists())
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertNotIn("SECRET", (self.output / "manifest.json").read_text())

    def test_source_plan_hash_source_head_and_drift_are_not_accepted(self):
        bad_digest = "0" * 64
        with self.assertRaisesRegex(pilot.PilotBlocked, "plan_hash_or_identity_differs"):
            pilot.run_pilot(self.plan_path, bad_digest, PLAN_HEAD, PILOT_HEAD, self.output)
        self.plan["planner_source"]["head"] = "d" * 40
        self.save_plan()
        with self.assertRaisesRegex(pilot.PilotBlocked, "plan_source_or_identity_not_admitted"):
            self.execute()
        self.plan["planner_source"] = code(PLAN_HEAD)
        self.save_plan()
        original = self.rpc
        changed = False
        def transport(endpoint, payload):
            nonlocal changed
            if not changed:
                changed = True
                value = self.plan_path.stat()
                os.utime(self.plan_path, ns=(value.st_atime_ns, value.st_mtime_ns + 1))
            return original(endpoint, payload)
        result = self.execute(transport=transport)
        self.assertEqual(result["error_code"], "source_plan_identity_changed")
        self.assertEqual(result["status"], "partial_pilot")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_append_start_raw_maximum_and_tail_header_must_match_bound_inventory(self):
        original_start = dict(self.plan["provisional_append_start"])
        cases = (
            ({**original_start, "start_block": 1001}, 1999, header(999), "plan_append_start_inventory_binding_differs"),
            ({**original_start, "raw_footer_declared_maximum_block": 998}, 2000, header(999),
             "plan_append_start_inventory_binding_differs"),
            (original_start, 2000, header(998), "plan_raw_tail_header_inventory_binding_differs"),
            (original_start, 2000, None, "plan_raw_tail_header_inventory_binding_differs"),
        )
        for start, count, tail, code_name in cases:
            with self.subTest(start=start, tail=tail):
                self.plan.update(provisional_append_start=start, provisional_append_blocks=count,
                                 raw_footer_tail_header=tail)
                self.save_plan()
                with self.assertRaisesRegex(pilot.PilotBlocked, code_name):
                    self.execute()
                self.assertEqual(self.rpc.calls, [])
                self.assertFalse(self.output.exists())

    def test_bound_inventory_json_drift_after_rpc_prevents_complete(self):
        original = self.rpc
        changed = False
        def transport(endpoint, payload):
            nonlocal changed
            if not changed:
                changed = True
                value = json.loads(self.inventory_path.read_text())
                value["resources"]["fixture_changed"] = True
                self.inventory_path.write_text(json.dumps(value))
            return original(endpoint, payload)
        result = self.execute(transport=transport)
        self.assertEqual(result["error_code"], "bound_inventory_identity_changed")
        self.assertEqual(len(result["windows"]), 3)
        self.assertEqual(result["status"], "partial_pilot")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_bound_inventory_digest_is_reopened_even_if_identity_check_is_deceived(self):
        actual_identity = inventory.path_identity
        fixed_identity = actual_identity(self.inventory_path)
        original = self.rpc
        changed = False
        def transport(endpoint, payload):
            nonlocal changed
            if not changed:
                changed = True
                value = json.loads(self.inventory_path.read_text())
                value["resources"]["fixture_changed"] = True
                self.inventory_path.write_text(json.dumps(value))
            return original(endpoint, payload)
        def identity(path):
            return fixed_identity if Path(path) == self.inventory_path else actual_identity(path)
        with patch.object(inventory, "path_identity", side_effect=identity):
            result = self.execute(transport=transport)
        self.assertEqual(result["error_code"], "inventory_digest_differs")
        self.assertEqual(result["status"], "partial_pilot")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_endpoint_reopen_or_cutoff_anchor_drift_blocks_completion(self):
        counts = {}
        def mutate(payload, reply):
            if payload["method"] == "eth_getBlockByNumber":
                block = int(payload["params"][0], 16)
                counts[block] = counts.get(block, 0) + 1
                if block == 1549 and counts[block] > 1:
                    reply["result"]["hash"] = "0x" + "f" * 64
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "rpc_reopened_header_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_empty_parquet_roundtrip_preserves_raw_schema(self):
        payload = pilot.parquet_payload([])
        table = pq.read_table(pa.BufferReader(payload))
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(str(table.schema.field("block_number").type), "uint64")
        self.assertEqual(str(table.schema.field("topics").type), "list<element: string>")

    def test_guard_refusal_on_every_host_precedes_default_data_or_rpc_use(self):
        errors = io.StringIO()
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture refusal")), \
                patch.object(pilot, "run_pilot", side_effect=AssertionError("must not probe")) as run, \
                redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                pilot.main(["--source-plan", str(self.plan_path), "--source-plan-sha256", self.digest,
                            "--plan-source-head", PLAN_HEAD, "--expected-head", PILOT_HEAD,
                            "--output-dir", str(self.output)])
        self.assertEqual(stopped.exception.code, 2)
        run.assert_not_called()
        self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
