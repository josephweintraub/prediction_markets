"""Synthetic bounded probes: no network, real trade bodies or private endpoint."""
from __future__ import annotations

from contextlib import redirect_stderr
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_protocol_activity as activity

INVENTORY_HEAD, PLAN_HEAD, PROBE_HEAD = (character * 40 for character in "abc")


def code(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def block_hash(number):
    return "0x" + f"{number + 1:064x}"


def header(number, *, rpc=False):
    result = {"number": number, "hash": block_hash(number), "parentHash": block_hash(number - 1),
              "timestamp": planner.utc_cutoff(planner.END_EXCLUSIVE) + (number - 1001) * 2}
    if rpc:
        result.update(number=hex(number), timestamp=hex(result["timestamp"]))
    return result


def log(number, index=0):
    return {"address": activity.ADDRESS, "blockNumber": hex(number), "blockHash": block_hash(number),
            "transactionHash": "0x" + f"{number + 5000:064x}", "logIndex": hex(index),
            "transactionIndex": "0x0", "removed": False,
            "topics": [activity.TOPIC0, "0x" + "1" * 64, "0x" + "2" * 64, "0x" + "3" * 64],
            "data": "0x" + "de" * 224}


class FakeRPC:
    def __init__(self):
        self.calls = []
        self.override = {}

    def __call__(self, endpoint, payload):
        self.calls.append(payload)
        method, params = payload["method"], payload["params"]
        if method == "eth_chainId":
            result = "0x89"
        elif method == "eth_getBlockByNumber":
            result = header(int(params[0], 16), rpc=True)
        elif method == "eth_getLogs":
            result = [log(int(params[0]["toBlock"], 16))]
        elif method == "eth_getStorageAt":
            result = "0x" + "00" * 12 + "ab" * 20
        else:
            raise AssertionError("forbidden method")
        if method in self.override:
            result = self.override[method](result, payload)
        return json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}).encode()


class ProtocolActivityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for path in paths.values():
            pq.write_table(pa.table({"block_number": [1, 500], "timestamp": [1, 500]}), path)
        with patch.object(inventory, "code_state", return_value=code(INVENTORY_HEAD)):
            source_inventory = inventory.build_inventory(paths, disk_path=self.root)
        self.inventory_path = self.root / "inventory.json"
        self.inventory_path.write_text(json.dumps(source_inventory))
        inventory_sha = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        _, binding = planner.read_inventory(self.inventory_path, inventory_sha, INVENTORY_HEAD)
        self.value = {
            "schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
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
        self.plan_path = self.root / "plan.json"
        self.save_plan()
        self.rpc = FakeRPC()

    def save_plan(self):
        self.plan_path.write_text(json.dumps(self.value))
        self.digest = hashlib.sha256(self.plan_path.read_bytes()).hexdigest()

    def execute(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=code(PROBE_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")):
            return activity.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD,
                                  transport=kwargs.pop("transport", self.rpc), **kwargs)

    def test_default_windows_optional_slots_and_public_evidence_only(self):
        result = self.execute(check_implementation=True)
        self.assertEqual(result["status"], "bounded_activity_probe_complete")
        self.assertEqual(result["rpc_calls_attempted"], 7)
        self.assertEqual([(item["start_block"], item["end_block"]) for item in result["windows"]],
                         [(401, 500), (901, 1000)])
        self.assertTrue(all(item["matching_log_count"] == 1 for item in result["windows"]))
        self.assertEqual(result["implementation_slots"][0]["implementation_address"], "0x" + "ab" * 20)
        self.assertEqual(result["windows"][0]["first_native_identity"]["block_number"], 500)
        encoded = json.dumps(result)
        self.assertNotIn("SECRET", encoded)
        self.assertNotIn("de" * 224, encoded)
        self.assertNotIn("1" * 64, encoded)
        self.assertNotIn("eth_getCode", encoded)
        self.assertFalse(result["collection_approved"])
        self.assertFalse(result["exchange_abi_admitted"])
        self.assertEqual(result["windows"][0]["observed_schema"]["data_byte_lengths"], [224])

    def test_zero_logs_are_window_specific_not_period_absence(self):
        self.rpc.override["eth_getLogs"] = lambda *_: []
        result = self.execute()
        self.assertEqual(result["status"], "bounded_activity_probe_complete")
        self.assertFalse(result["windows"][0]["presence_observed"])
        self.assertIsNone(result["windows"][0]["first_native_identity"])
        self.assertIn("no whole-period absence", result["windows"][0]["absence_scope"])

    def test_three_maximum_literal_windows_fit_ten_call_ceiling(self):
        result = self.execute(windows=[(0, 299), (300, 599), (600, 1000)], check_implementation=True)
        self.assertEqual(result["status"], "bounded_activity_probe_complete")
        self.assertEqual(result["rpc_calls_attempted"], 9)
        for windows, expected in (([(0, 1000)], "window_block_limit"),
                                  ([(1, 10)] * 4, "window_count_limit"),
                                  ([(1, 10), (10, 20)], "overlapping_probe_windows"),
                                  ([(1, 1001)], "invalid_window_bounds")):
            with self.subTest(expected=expected):
                result = self.execute(windows=windows)
                self.assertEqual(result["error_code"], expected)
                self.assertEqual(result["rpc_calls_attempted"], 0)

    def test_digest_source_head_and_false_proof_rejected_before_rpc(self):
        self.digest = "0" * 64
        self.assertEqual(self.execute()["error_code"], "source_plan_digest_differs")
        self.save_plan()
        for field, value, expected in (("planner_source", code("f" * 40), "source_plan_head_or_clean_state_differs"),
                ("cutoff_headers_reopened_unchanged", False, "source_plan_scope_or_identity_differs"),
                ("last_included_header", header(999), "source_plan_cutoff_proof_differs")):
            original = self.value[field]
            self.value[field] = value
            self.save_plan()
            result = self.execute()
            self.assertEqual(result["error_code"], expected)
            self.assertEqual(result["rpc_calls_attempted"], 0)
            self.value[field] = original

    def test_wrong_chain_id_shape_and_oversized_response(self):
        self.rpc.override["eth_chainId"] = lambda *_: "0x1"
        self.assertEqual(self.execute()["error_code"], "rpc_wrong_chain")
        for identifier in (True, 2, None):
            def bad_id(endpoint, payload):
                return json.dumps({"jsonrpc": "2.0", "id": identifier, "result": "0x89"}).encode()
            self.assertEqual(self.execute(transport=bad_id)["error_code"], "rpc_reply_id_mismatch")
        oversized = lambda *_: b"x" * (activity.MAX_RESPONSE_BYTES + 1)
        self.assertEqual(self.execute(transport=oversized)["error_code"], "rpc_response_byte_limit_or_type")

    def test_header_number_hash_source_binding_and_monotonicity(self):
        for update, expected in (({"number": "0x1"}, "rpc_header_number_mismatch"),
                                  ({"hash": "bad"}, "invalid_header_hash"),
                                  ({"hash": "0x" + "f" * 64}, "source_or_reopened_header_changed")):
            self.rpc.override["eth_getBlockByNumber"] = lambda value, payload: {**value, **update}
            self.assertEqual(self.execute()["error_code"], expected)

    def test_wrong_topic_address_range_removed_and_data_fail_closed(self):
        cases = [({"topics": ["0x" + "f" * 64] * 4}, "log_topic_or_schema_mismatch"),
                 ({"address": "0x" + "a" * 40}, "log_address_mismatch"),
                 ({"blockNumber": "0x1"}, "log_outside_requested_window"),
                 ({"removed": True}, "log_removed_or_flag_absent"),
                 ({"data": "0x1"}, "invalid_log_data_encoding"),
                 ({"blockHash": "0x" + "f" * 64}, "observed_block_hash_conflict")]
        for update, expected in cases:
            with self.subTest(expected=expected):
                self.rpc.override["eth_getLogs"] = lambda value, payload: [{**value[0], **update}]
                self.assertEqual(self.execute()["error_code"], expected)

    def test_duplicate_and_conflicting_native_payloads(self):
        self.rpc.override["eth_getLogs"] = lambda value, payload: value + copy.deepcopy(value)
        self.assertEqual(self.execute()["error_code"], "duplicate_native_identity")
        self.rpc.override["eth_getLogs"] = lambda value, payload: value + [{**value[0], "data": "0x00"}]
        self.assertEqual(self.execute()["error_code"], "conflicting_native_identity")

    def test_two_hashes_for_same_non_end_block_rejected(self):
        def logs(value, payload):
            number = int(payload["params"][0]["toBlock"], 16) - 1
            first = log(number)
            return [first, {**log(number, 1), "blockHash": "0x" + "f" * 64}]
        self.rpc.override["eth_getLogs"] = logs
        self.assertEqual(self.execute()["error_code"], "observed_block_hash_conflict")

    def test_storage_zero_malformed_and_non_address_words(self):
        self.rpc.override["eth_getStorageAt"] = lambda *_: "0x" + "0" * 64
        result = self.execute(check_implementation=True)
        self.assertIsNone(result["implementation_slots"][0]["implementation_address"])
        self.rpc.override["eth_getStorageAt"] = lambda *_: "0x0"
        self.assertEqual(self.execute(check_implementation=True)["error_code"], "invalid_implementation_storage_word")
        self.rpc.override["eth_getStorageAt"] = lambda *_: "0x" + "ab" * 32
        self.assertEqual(self.execute(check_implementation=True)["error_code"], "implementation_slot_not_address_word")

    def test_call_limit_and_transport_errors_do_not_disclose_endpoint(self):
        with patch.object(activity, "MAX_RPC_CALLS", 2):
            result = self.execute()
        self.assertEqual(result["error_code"], "rpc_call_limit")
        self.assertEqual(result["rpc_calls_attempted"], 2)
        def fail(*_):
            raise RuntimeError("https://rpc.invalid/SECRET")
        result = self.execute(transport=fail)
        self.assertEqual(result["error_code"], "probe_input_rpc_or_configuration_failure")
        self.assertNotIn("SECRET", json.dumps(result))

    def test_duplicate_json_keys_and_error_envelopes_are_sanitized(self):
        replies = [b'{"jsonrpc":"2.0","id":1,"id":2,"result":"0x89"}',
                   b'{"jsonrpc":"2.0","id":1,"error":"https://rpc.invalid/SECRET"}',
                   b'not JSON https://rpc.invalid/SECRET']
        for raw in replies:
            with self.subTest(raw=raw[:20]):
                result = self.execute(transport=lambda *_: raw)
                self.assertEqual(result["status"], "blocked")
                self.assertNotIn("SECRET", json.dumps(result))

    def test_http_reader_requests_only_cap_plus_one_bytes(self):
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self, amount):
                self.amount = amount
                return b"x" * amount
        response = Response()
        with patch.object(activity.urllib.request, "urlopen", return_value=response):
            with self.assertRaisesRegex(activity.ProbeBlocked, "rpc_response_byte_limit"):
                activity.http_rpc("https://rpc.invalid/SECRET", {"id": 1})
        self.assertEqual(response.amount, activity.MAX_RESPONSE_BYTES + 1)

    def test_source_inventory_and_raw_endpoint_bridge_are_required(self):
        self.value["raw_footer_tail_header"] = header(499)
        self.value["provisional_append_start"] = {"raw_footer_declared_maximum_block": 499, "start_block": 500}
        self.save_plan()
        result = self.execute()
        self.assertEqual(result["error_code"], "source_plan_raw_endpoint_inventory_mismatch")
        self.assertEqual(result["rpc_calls_attempted"], 0)

    def test_cli_overlap_refuses_even_blocked_result_write(self):
        with patch("production_guard.require_production_host"), \
                patch.object(activity, "probe", side_effect=AssertionError("must not probe")) as probe:
            with self.assertRaisesRegex(activity.ProbeBlocked, "output_already_exists"):
                activity.main(["--source-plan", str(self.plan_path), "--source-plan-sha256", self.digest,
                    "--source-plan-source-head", PLAN_HEAD, "--expected-head", PROBE_HEAD,
                    "--output", str(self.plan_path)])
        probe.assert_not_called()

    def test_input_mutation_and_output_overlap_block(self):
        output = self.root / "raw_events.parquet" / "nested.json"
        result = self.execute(output=output)
        self.assertEqual(result["error_code"], "output_overlaps_input_or_private_configuration")
        self.assertEqual(result["rpc_calls_attempted"], 0)
        def mutate(endpoint, payload):
            self.plan_path.write_text(self.plan_path.read_text() + " ")
            return self.rpc(endpoint, payload)
        self.assertEqual(self.execute(transport=mutate)["error_code"], "source_plan_changed_during_probe")

    def test_cli_guard_and_exclusive_writer(self):
        output = self.root / "probe.json"
        errors = io.StringIO()
        with patch("production_guard.require_production_host", side_effect=RuntimeError("refusal")), \
                patch.object(activity, "probe", side_effect=AssertionError("must not probe")), redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                activity.main(["--source-plan", str(self.plan_path), "--source-plan-sha256", self.digest,
                    "--source-plan-source-head", PLAN_HEAD, "--expected-head", PROBE_HEAD, "--output", str(output)])
        self.assertEqual(stopped.exception.code, 2)
        self.assertFalse(output.exists())
        inventory.write_exclusive(output, {"a": 1})
        with self.assertRaises(FileExistsError):
            inventory.write_exclusive(output, {"a": 2})


if __name__ == "__main__":
    unittest.main()
