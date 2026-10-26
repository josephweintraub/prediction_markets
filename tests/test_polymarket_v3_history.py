"""Synthetic native-history evidence only: no real RPC, wallet or trade input."""
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
from scripts import probe_polymarket_v3_history as history

INVENTORY_HEAD, PLAN_HEAD, PROBE_HEAD = (character * 40 for character in "abc")


def code(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def block_hash(number):
    return "0x" + f"{number + 1:064x}"


def header(number, *, rpc=False):
    result = {"number": number, "hash": block_hash(number), "parentHash": block_hash(number - 1),
              "timestamp": planner.utc_cutoff(planner.END_EXCLUSIVE) + number - history.CUTOFF_BLOCK - 1}
    if rpc:
        result.update(number=hex(number), timestamp=hex(result["timestamp"]))
    return result


def native_log(index=history.UPGRADE_LOG_INDEX):
    return {"address": history.ADDRESS, "blockNumber": hex(history.UPGRADE_BLOCK),
            "blockHash": block_hash(history.UPGRADE_BLOCK), "transactionHash": history.UPGRADE_TRANSACTION,
            "transactionIndex": "0x3", "logIndex": hex(index), "removed": False,
            "topics": [history.UPGRADED_TOPIC, "0x" + "00" * 12 + history.NEW_IMPLEMENTATION[2:]],
            "data": "0x"}


def receipt():
    return {"transactionHash": history.UPGRADE_TRANSACTION, "blockNumber": hex(history.UPGRADE_BLOCK),
            "blockHash": block_hash(history.UPGRADE_BLOCK), "transactionIndex": "0x3", "status": "0x1",
            "from": "0x" + "fa" * 20, "logs": [native_log()]}


class FakeRPC:
    def __init__(self):
        self.calls = []
        self.override = {}

    def __call__(self, endpoint, payload):
        self.calls.append(copy.deepcopy(payload))
        method, params = payload["method"], payload["params"]
        if method == "eth_chainId":
            result = "0x89"
        elif method == "eth_getBlockByNumber":
            result = header(int(params[0], 16), rpc=True)
        elif method == "eth_getStorageAt":
            block = int(params[2], 16)
            implementation = history.OLD_IMPLEMENTATION if block < history.UPGRADE_BLOCK else history.NEW_IMPLEMENTATION
            result = "0x" + "00" * 12 + implementation[2:]
        elif method == "eth_getCode":
            result = {history.ADDRESS: "0x6000", history.OLD_IMPLEMENTATION: "0x6001",
                      history.NEW_IMPLEMENTATION: "0x6002"}[params[0]]
        elif method == "eth_getTransactionReceipt":
            result = receipt()
        else:
            raise AssertionError("forbidden method")
        if method in self.override:
            result = self.override[method](result, payload)
        return json.dumps({"jsonrpc": "2.0", "id": payload["id"], "result": result}).encode()


class V3HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for path in paths.values():
            pq.write_table(pa.table({"block_number": [1, history.RAW_TAIL_BLOCK], "timestamp": [1, 2]}), path)
        with patch.object(inventory, "code_state", return_value=code(INVENTORY_HEAD)):
            source_inventory = inventory.build_inventory(paths, disk_path=self.root)
        self.inventory_path = self.root / "inventory.json"
        self.inventory_path.write_text(json.dumps(source_inventory))
        digest = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        _, binding = planner.read_inventory(self.inventory_path, digest, INVENTORY_HEAD)
        self.value = {
            "schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": code(PLAN_HEAD), "inventory_binding": binding,
            "final_input_identity_unchanged": True, "cutoff_headers_reopened_unchanged": True,
            "trade_logs_requested": False, "parquet_body_reads": 0,
            "collection_approved": False, "exchange_abi_admitted": False,
            "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(history.CUTOFF_BLOCK),
            "first_excluded_header": header(history.CUTOFF_BLOCK + 1),
            "raw_footer_tail_header": header(history.RAW_TAIL_BLOCK),
            "provisional_append_start": {"start_block": history.RAW_TAIL_BLOCK + 1,
                                         "raw_footer_declared_maximum_block": history.RAW_TAIL_BLOCK},
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
            return history.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD,
                                 transport=kwargs.pop("transport", self.rpc), **kwargs)

    def assert_blocked(self, expected, **kwargs):
        result = self.execute(**kwargs)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["error_code"], expected)
        self.assertFalse(result["collection_approved"])
        self.assertFalse(result["exchange_abi_admitted"])
        return result

    def test_bounded_native_evidence_no_trade_or_first_deployment_claim(self):
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_history_probe_complete")
        self.assertEqual(result["rpc_calls_attempted"], 25)
        self.assertLessEqual(result["response_bytes_received"], history.MAX_TOTAL_RESPONSE_BYTES)
        self.assertEqual(len(result["headers"]), 8)
        self.assertEqual(len(result["implementation_slots"]), 4)
        self.assertEqual(result["known_upgrade_receipt"]["native_identity"]["log_index"], 752)
        self.assertEqual(result["earliest_queried_proxy_code_present_block"], history.OLD_REPORTED_CREATION_BLOCK - 1)
        self.assertTrue(result["reopened_headers_unchanged"])
        for key in ("collection_approved", "exchange_abi_admitted", "trade_logs_requested",
                    "whole_history_scan_performed", "gap_free_upgrade_history_certified",
                    "first_deployment_certified", "backfill_start_admitted"):
            self.assertFalse(result[key])
        self.assertIsNone(result["safe_backfill_start_block"])
        encoded = json.dumps(result)
        for forbidden in ("SECRET", "0x6001", "0x" + "fa" * 20, "eth_getLogs", "eth_call"):
            self.assertNotIn(forbidden, encoded)
        old = result["implementation_code_observations"][0]
        self.assertEqual(old["runtime_sha256"], hashlib.sha256(bytes.fromhex("6001")).hexdigest())
        self.assertEqual(old["runtime_bytes"], 2)
        self.assertIn("not an Ethereum Keccak", old["hash_semantics"])

    def test_proxy_lower_bound_does_not_become_creation_or_backfill_certificate(self):
        def absent_before(value, payload):
            return "0x" if payload["params"] == [history.ADDRESS, hex(history.OLD_REPORTED_CREATION_BLOCK - 1)] else value
        self.rpc.override["eth_getCode"] = absent_before
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_history_probe_complete")
        self.assertEqual(result["earliest_queried_proxy_code_present_block"], history.OLD_REPORTED_CREATION_BLOCK)
        self.assertFalse(result["first_deployment_certified"])
        self.assertIsNone(result["safe_backfill_start_block"])

    def test_source_digest_head_scope_and_cutoff_rejected_before_rpc(self):
        self.digest = "0" * 64
        self.assertEqual(self.assert_blocked("source_plan_digest_differs")["rpc_calls_attempted"], 0)
        self.save_plan()
        cases = [("planner_source", code("f" * 40), "source_plan_head_or_clean_state_differs"),
                 ("cutoff_headers_reopened_unchanged", False, "source_plan_scope_or_identity_differs"),
                 ("end_exclusive_utc", "2026-10-01T00:00:00Z", "end_exclusive_differs_from_user_contract")]
        for key, value, expected in cases:
            with self.subTest(key=key):
                original = self.value[key]
                self.value[key] = value
                self.save_plan()
                self.assertEqual(self.assert_blocked(expected)["rpc_calls_attempted"], 0)
                self.value[key] = original
        self.save_plan()

    def test_frozen_endpoint_and_runtime_source_clean_state_required(self):
        changed = copy.deepcopy(self.value)
        changed["last_included_header"] = header(history.CUTOFF_BLOCK - 1)
        with self.assertRaisesRegex(history.HistoryBlocked, "source_plan_frozen_endpoints_differ"):
            history.validate_plan_blocks(changed)
        with patch.object(inventory, "code_state", return_value={**code(PROBE_HEAD), "tracked_worktree_clean": False}):
            result = history.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD, transport=self.rpc)
        self.assertEqual(result["error_code"], "probe_source_head_or_clean_state_differs")
        self.assertEqual(result["rpc_calls_attempted"], 0)

    def test_wrong_chain_and_rpc_id_shape_error_payloads_never_leak(self):
        self.rpc.override["eth_chainId"] = lambda *_: "0x1"
        self.assert_blocked("rpc_wrong_chain")
        replies = [b'{"jsonrpc":"2.0","id":true,"result":"0x89"}',
                   b'{"jsonrpc":"2.0","id":1,"error":"https://rpc.invalid/SECRET"}',
                   b'{"jsonrpc":"2.0","id":1,"id":2,"result":"0x89"}',
                   b'not JSON https://rpc.invalid/SECRET']
        for raw in replies:
            with self.subTest(raw=raw[:30]):
                result = self.execute(transport=lambda *_: raw)
                self.assertEqual(result["status"], "blocked")
                self.assertNotIn("SECRET", json.dumps(result))

    def test_call_response_and_total_byte_caps_stop_bounded_execution(self):
        with patch.object(history, "MAX_RPC_CALLS", 2):
            result = self.assert_blocked("rpc_call_limit")
            self.assertEqual(result["rpc_calls_attempted"], 2)
        with patch.object(history, "MAX_RESPONSE_BYTES", 80):
            result = self.assert_blocked("rpc_response_byte_limit")
            self.assertEqual(result["rpc_calls_attempted"], 2)
        with patch.object(history, "MAX_TOTAL_RESPONSE_BYTES", 80):
            result = self.assert_blocked("rpc_total_response_byte_limit")
            self.assertEqual(result["rpc_calls_attempted"], 2)
        self.assert_blocked("rpc_response_not_bytes", transport=lambda *_: {"result": "0x89"})

    def test_http_reader_obeys_remaining_budget_plus_one_sentinel(self):
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def read(self, amount):
                self.amount = amount
                return b"x" * amount
        response = Response()
        with patch.object(history.urllib.request, "urlopen", return_value=response):
            self.assertEqual(len(history.http_rpc("https://rpc.invalid/SECRET", {"id": 1}, byte_limit=23)), 24)
        self.assertEqual(response.amount, 24)

    def test_header_number_hash_and_adjacent_parent_proof(self):
        cases = [({"number": "0x1"}, "rpc_header_number_mismatch"),
                 ({"hash": "bad"}, "invalid_header_hash"),
                 ({"parentHash": "0x" + "f" * 64}, "adjacent_probe_headers_not_linked")]
        for update, expected in cases:
            with self.subTest(expected=expected):
                self.rpc.override["eth_getBlockByNumber"] = lambda value, payload: (
                    {**value, **update} if expected != "adjacent_probe_headers_not_linked"
                    or int(payload["params"][0], 16) == history.OLD_REPORTED_CREATION_BLOCK else value)
                self.assert_blocked(expected)

    def test_changed_source_bound_and_reopened_headers_rejected(self):
        self.rpc.override["eth_getBlockByNumber"] = lambda value, payload: (
            {**value, "hash": "0x" + "f" * 64} if int(payload["params"][0], 16) == history.RAW_TAIL_BLOCK else value)
        self.assert_blocked("source_or_reopened_header_changed")
        seen = {}
        def change_reopened(value, payload):
            number = int(payload["params"][0], 16)
            seen[number] = seen.get(number, 0) + 1
            return {**value, "hash": "0x" + "f" * 64} if seen[number] > 1 else value
        self.rpc.override["eth_getBlockByNumber"] = change_reopened
        self.assert_blocked("source_or_reopened_header_changed")

    def test_missing_proxy_malformed_and_changed_implementation_code_fail(self):
        cases = [(lambda value, payload: "0x" if payload["params"][0] == history.ADDRESS else value,
                  "proxy_code_absent_at_cutoff"),
                 (lambda *_: "0x1", "invalid_runtime_bytecode_encoding"),
                 (lambda value, payload: "0x" if payload["params"][0] == history.OLD_IMPLEMENTATION else value,
                  "implementation_code_absent_at_queried_block"),
                 (lambda value, payload: "0x6003" if payload["params"] == [history.OLD_IMPLEMENTATION, hex(history.CUTOFF_BLOCK)] else value,
                  "implementation_runtime_changed_between_observations")]
        for override, expected in cases:
            with self.subTest(expected=expected):
                self.rpc.override["eth_getCode"] = override
                self.assert_blocked(expected)

    def test_storage_address_word_and_expected_era(self):
        for value, expected in (("0x0", "invalid_implementation_storage_word"),
                                ("0x" + "ab" * 32, "implementation_slot_not_address_word"),
                                ("0x" + "00" * 32, "implementation_slot_differs_from_expected_era")):
            with self.subTest(expected=expected):
                self.rpc.override["eth_getStorageAt"] = lambda *_: value
                self.assert_blocked(expected)

    def test_upgrade_receipt_identity_block_hash_status_and_count_fail_closed(self):
        cases = [(None, "upgrade_receipt_absent_or_not_object"),
                 ({"transactionHash": "0x" + "f" * 64}, "upgrade_receipt_transaction_mismatch"),
                 ({"blockNumber": hex(history.UPGRADE_BLOCK - 1)}, "upgrade_receipt_block_mismatch"),
                 ({"blockHash": "0x" + "f" * 64}, "upgrade_receipt_block_hash_mismatch"),
                 ({"status": "0x0"}, "upgrade_receipt_not_successful"),
                 ({"logs": []}, "upgrade_receipt_log_count_invalid")]
        for update, expected in cases:
            with self.subTest(expected=expected):
                self.rpc.override["eth_getTransactionReceipt"] = lambda value, payload: (None if update is None else {**value, **update})
                self.assert_blocked(expected)
        with patch.object(history, "MAX_RECEIPT_LOGS", 0):
            self.rpc.override.clear()
            self.assert_blocked("upgrade_receipt_log_count_invalid")

    def test_upgrade_log_index_address_topics_payload_and_native_identity(self):
        cases = [({"logIndex": "0x0"}, "expected_upgrade_log_absent"),
                 ({"address": "0x" + "ab" * 20}, "upgrade_log_address_mismatch"),
                 ({"topics": [history.UPGRADED_TOPIC]}, "upgrade_log_topic_or_arity_mismatch"),
                 ({"topics": ["0x" + "f" * 64, native_log()["topics"][1]]}, "upgrade_log_topic_or_arity_mismatch"),
                 ({"topics": [history.UPGRADED_TOPIC, "0x" + "00" * 32]}, "upgrade_log_implementation_mismatch"),
                 ({"data": "0x00"}, "upgrade_log_unexpected_data"),
                 ({"transactionHash": "0x" + "f" * 64}, "receipt_log_identity_mismatch"),
                 ({"blockHash": "0x" + "f" * 64}, "receipt_log_identity_mismatch"),
                 ({"transactionIndex": "0x4"}, "receipt_log_position_mismatch"),
                 ({"removed": True}, "receipt_log_removed_or_flag_absent")]
        for update, expected in cases:
            with self.subTest(expected=expected):
                self.rpc.override["eth_getTransactionReceipt"] = lambda value, payload: {**value, "logs": [{**native_log(), **update}]}
                self.assert_blocked(expected)

    def test_receipt_duplicates_and_multiple_upgrade_events_rejected(self):
        self.rpc.override["eth_getTransactionReceipt"] = lambda value, payload: {**value, "logs": [native_log(), native_log()]}
        self.assert_blocked("duplicate_receipt_log_index")
        self.rpc.override["eth_getTransactionReceipt"] = lambda value, payload: {**value, "logs": [native_log(), native_log(753)]}
        self.assert_blocked("ambiguous_proxy_upgrades_in_receipt")

    def test_irrelevant_receipt_log_is_not_saved(self):
        unrelated = {**native_log(751), "address": "0x" + "aa" * 20,
                     "topics": ["0x" + "bb" * 32], "data": "0x" + "ce" * 100}
        self.rpc.override["eth_getTransactionReceipt"] = lambda value, payload: {**value, "logs": [unrelated, native_log()]}
        result = self.execute()
        self.assertEqual(result["status"], "bounded_v3_history_probe_complete")
        self.assertEqual(result["known_upgrade_receipt"]["receipt_log_count"], 2)
        self.assertNotIn("ce" * 100, json.dumps(result))

    def test_input_mutation_and_output_overlap_block(self):
        result = self.assert_blocked("output_overlaps_input_or_private_configuration",
                                    output=self.root / "raw_events.parquet" / "nested.json")
        self.assertEqual(result["rpc_calls_attempted"], 0)
        def mutate(endpoint, payload):
            self.plan_path.write_text(self.plan_path.read_text() + " ")
            return self.rpc(endpoint, payload)
        self.assert_blocked("source_plan_changed_during_probe", transport=mutate)

    def test_publication_is_no_overwrite_reopened_and_size_limited(self):
        path = self.root / "result.json"
        result = self.execute()
        history.publish(path, result)
        self.assertEqual(json.loads(path.read_bytes()), result)
        original = path.read_bytes()
        with self.assertRaises(FileExistsError):
            history.publish(path, {"status": "blocked"})
        self.assertEqual(path.read_bytes(), original)
        with patch.object(history, "MAX_OUTPUT_BYTES", 1):
            with self.assertRaisesRegex(history.HistoryBlocked, "output_byte_limit"):
                history.publish(self.root / "large.json", result)
        self.assertFalse((self.root / "large.json").exists())

    def test_cli_guard_and_preexisting_output_prevent_execution(self):
        args = ["--source-plan", str(self.plan_path), "--source-plan-sha256", self.digest,
                "--source-plan-source-head", PLAN_HEAD, "--expected-head", PROBE_HEAD,
                "--output", str(self.plan_path)]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("refusal")), \
                patch.object(history, "probe", side_effect=AssertionError("must not probe")), \
                redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as caught:
                history.main(args)
        self.assertEqual(caught.exception.code, 2)
        with patch("production_guard.require_production_host"), \
                patch.object(history, "probe", side_effect=AssertionError("must not probe")) as probe:
            with self.assertRaisesRegex(history.activity.ProbeBlocked, "output_already_exists"):
                history.main(args)
        probe.assert_not_called()

    def test_blocked_probe_is_never_published_as_completed(self):
        self.rpc.override["eth_chainId"] = lambda *_: "0x1"
        result = self.execute()
        path = self.root / "blocked.json"
        history.publish(path, result)
        self.assertEqual(json.loads(path.read_bytes())["status"], "blocked")
        self.assertFalse(json.loads(path.read_bytes())["exchange_abi_admitted"])


if __name__ == "__main__":
    unittest.main()
