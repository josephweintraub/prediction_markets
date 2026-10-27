"""Synthetic fixtures only. No production host, private endpoint or real RPC."""
from __future__ import annotations

from collections import namedtuple
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_v3_history_v2 as history

INVENTORY_HEAD, PLAN_HEAD, PROBE_HEAD = (character * 40 for character in "abc")
DISK = namedtuple("Disk", "total used free")(10**12, 1, 10**12 - 1)


def state(head):
    return {"status": "inspected", "head": head, "tracked_worktree_clean": True,
            "tracked_dirty_entries": 0, "untracked_files_checked": False}


def block_hash(number):
    return "0x" + f"{number + 1:064x}"


def header(number, *, rpc=False):
    value = {"number": number, "hash": block_hash(number), "parentHash": block_hash(number - 1),
             "timestamp": planner.utc_cutoff(planner.END_EXCLUSIVE) + number - history.CUTOFF_BLOCK - 1}
    if rpc:
        value.update(number=hex(number), timestamp=hex(value["timestamp"]))
    return value


def upgrade(*, index=history.prior.UPGRADE_LOG_INDEX, implementation=history.prior.NEW_IMPLEMENTATION,
            block=history.prior.UPGRADE_BLOCK, tx=history.prior.UPGRADE_TRANSACTION, tx_index=62):
    return {"address": history.ADDRESS, "blockNumber": hex(block), "blockHash": block_hash(block),
            "transactionHash": tx, "transactionIndex": hex(tx_index), "logIndex": hex(index), "removed": False,
            "topics": [history.UPGRADED_TOPIC, "0x" + "00" * 12 + implementation[2:]], "data": "0x"}


class FakeRPC:
    def __init__(self):
        self.calls, self.logs, self.overrides = [], [upgrade()], {}
        self.reply_override = None

    def __call__(self, endpoint, payload, *, byte_limit, timeout):
        assert 0 < timeout <= 30 and 1 < byte_limit <= history.MAX_RESPONSE_BYTES
        self.calls.append(copy.deepcopy(payload))
        method, params = payload["method"], payload["params"]
        if method == "eth_chainId":
            value = "0x89"
        elif method == "eth_getBlockByNumber":
            value = header(int(params[0], 16), rpc=True)
        elif method == "eth_getStorageAt":
            implementation = (history.prior.OLD_IMPLEMENTATION if int(params[2], 16) < history.prior.UPGRADE_BLOCK
                              else history.prior.NEW_IMPLEMENTATION)
            value = "0x" + "00" * 12 + implementation[2:]
        elif method == "eth_getLogs":
            predicate = params[0]
            if "topics" in predicate:
                value = [copy.deepcopy(log) for log in self.logs if
                         int(predicate["fromBlock"], 16) <= int(log["blockNumber"], 16) <= int(predicate["toBlock"], 16)]
            else:
                value = []
        elif method == "eth_getTransactionReceipt":
            logs = [copy.deepcopy(log) for log in self.logs if log["transactionHash"] == params[0]]
            first = logs[0]
            value = {"transactionHash": params[0], "blockNumber": first["blockNumber"],
                     "blockHash": first["blockHash"], "transactionIndex": first["transactionIndex"],
                     "status": "0x1", "logs": logs, "from": "0x" + "fa" * 20}
        else:
            raise AssertionError("unexpected method")
        if method in self.overrides:
            value = self.overrides[method](value, payload)
        reply = {"jsonrpc": "2.0", "id": payload["id"], "result": value}
        status = 200
        if self.reply_override:
            replacement = self.reply_override(reply, payload, byte_limit)
            if replacement is not None:
                reply, status = replacement
        raw = reply if type(reply) is bytes else json.dumps(reply).encode()
        return history.HTTPResponse(raw[:byte_limit], status)


@contextmanager
def fixture_limits():
    yield {"started_monotonic": time.monotonic(), "address_space_soft_bytes": history.MAX_RAM_BYTES,
           "wall_alarm_seconds": history.MAX_RUNTIME_SECONDS}


class SparseHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for path in paths.values():
            pq.write_table(pa.table({"block_number": [1, history.prior.RAW_TAIL_BLOCK], "timestamp": [1, 2]}), path)
        with patch.object(inventory, "code_state", return_value=state(INVENTORY_HEAD)):
            original = inventory.build_inventory(paths, disk_path=self.root)
        self.inventory_path = self.root / "inventory.json"
        self.inventory_path.write_text(json.dumps(original))
        digest = hashlib.sha256(self.inventory_path.read_bytes()).hexdigest()
        _, binding = planner.read_inventory(self.inventory_path, digest, INVENTORY_HEAD)
        self.plan = {"schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": state(PLAN_HEAD), "inventory_binding": binding,
            "final_input_identity_unchanged": True, "cutoff_headers_reopened_unchanged": True,
            "trade_logs_requested": False, "parquet_body_reads": 0,
            "collection_approved": False, "exchange_abi_admitted": False,
            "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(history.CUTOFF_BLOCK),
            "first_excluded_header": header(history.CUTOFF_BLOCK + 1),
            "raw_footer_tail_header": header(history.prior.RAW_TAIL_BLOCK),
            "provisional_append_start": {"start_block": history.prior.RAW_TAIL_BLOCK + 1,
                                         "raw_footer_declared_maximum_block": history.prior.RAW_TAIL_BLOCK},
            "cutoff_proof": {key: True for key in ("adjacent_block_numbers", "parent_hash_matches",
                "last_time_strictly_before_cutoff", "first_time_at_or_after_cutoff")}}
        self.plan_path = self.root / "plan.json"
        self.save_plan()
        self.rpc, self.run_number = FakeRPC(), 0

    def save_plan(self):
        self.plan_path.write_text(json.dumps(self.plan))
        self.digest = hashlib.sha256(self.plan_path.read_bytes()).hexdigest()

    def execute(self, *, transport=None, snapshot=None, resources=None):
        self.run_number += 1
        self.run_dir = self.root / f"run-{self.run_number}"
        with patch.object(history, "require_production_host"), patch.object(history, "runtime_limits", fixture_limits), \
                patch.object(history, "RUN_ROOT", self.root), \
                patch.object(history, "source_snapshot", side_effect=snapshot or (lambda _: {"head": PROBE_HEAD})), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(inventory, "resource_state", return_value=resources or {
                    "ram_bytes": {"MemAvailable": 10**12}, "disk_bytes": {"free": 10**12}}), \
                patch.object(history.shutil, "disk_usage", return_value=DISK):
            return history.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD,
                                 run_dir=self.run_dir, transport=transport or self.rpc)

    def blocked(self, code, **kwargs):
        value = self.execute(**kwargs)
        self.assertEqual(value["status"], "blocked", value)
        self.assertEqual(value["error_code"], code)
        self.assertFalse((self.run_dir / "COMPLETE.json").exists())
        self.assertNotIn("SECRET", json.dumps(value))
        self.assertFalse(value["analysis_admitted"])
        return value

    def test_complete_literal_coverage_without_deployment_or_semantic_claim(self):
        value = self.execute()
        self.assertEqual(value["status"], "bounded_v3_sparse_history_complete", value)
        self.assertEqual(len(value["upgrade_ranges"]), 95)
        self.assertEqual(len(value["zero_prefix_ranges"]), 88)
        history.verify_coverage(value["upgrade_ranges"], history.CUTOFF_BLOCK, "upgraded")
        history.verify_coverage(value["zero_prefix_ranges"], history.PREFIX_END, "zero_prefix")
        self.assertTrue(value["zero_prefix_verified"])
        self.assertEqual(value["safe_event_collection_start_block"], 87_479_447)
        for key in ("first_deployment_certified", "gap_free_upgrade_history_certified", "implementation_eras_admitted",
                    "collection_approved", "exchange_abi_admitted", "economic_decoding_admitted", "analysis_admitted"):
            self.assertFalse(value[key])
        self.assertLess(value["rpc_calls_attempted"], history.MAX_RPC_CALLS)
        self.assertLessEqual(value["response_bytes_charged"], history.MAX_TOTAL_RESPONSE_BYTES)
        self.assertEqual(value["response_bytes_received"], value["response_bytes_charged"])
        marker = json.loads((self.run_dir / "COMPLETE.json").read_text())
        self.assertEqual(marker["result_sha256"], hashlib.sha256((self.run_dir / "RESULT.json").read_bytes()).hexdigest())
        journal = (self.run_dir / "REQUESTS.jsonl").read_bytes()
        self.assertEqual(value["request_journal_sha256"], hashlib.sha256(journal).hexdigest())
        self.assertEqual(value["request_journal_records"], len(journal.splitlines()))
        self.assertLessEqual(sum(path.stat().st_size for path in self.run_dir.iterdir()), history.MAX_OUTPUT_BYTES)
        for forbidden in ("SECRET", "0x" + "fa" * 20):
            self.assertNotIn(forbidden, json.dumps(value))
        self.assertFalse(value["upgrade_block_proofs"][0]["whole_block_implementation_assignment_admitted"])

    def test_production_guard_precedes_inputs_rpc_and_new_files(self):
        with patch.object(history, "require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(history.activity, "read_plan") as read, patch.object(history, "runtime_limits") as runtime:
            with self.assertRaises(RuntimeError):
                history.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD, run_dir=self.root / "never")
        read.assert_not_called()
        runtime.assert_not_called()
        self.assertFalse((self.root / "never").exists())

    def test_source_plan_digest_head_scope_and_original_identity(self):
        self.digest = "0" * 64
        self.assertEqual(self.blocked("source_plan_digest_differs")["rpc_calls_attempted"], 0)
        self.save_plan()
        self.plan["planner_source"]["head"] = "d" * 40
        self.save_plan()
        self.assertEqual(self.blocked("source_plan_head_or_clean_state_differs")["rpc_calls_attempted"], 0)

    def test_source_reopen_change_and_uncommitted_dependency_block(self):
        count = 0
        def changed(_head):
            nonlocal count
            count += 1
            return {"head": PROBE_HEAD, "files": {"fixture": str(count)}}
        self.blocked("probe_source_changed_during_probe", snapshot=changed)
        def untracked(_head):
            raise history.HistoryBlocked("source_dependency_uncommitted_or_changed")
        self.assertEqual(self.blocked("source_dependency_uncommitted_or_changed", snapshot=untracked)["rpc_calls_attempted"], 0)

    def test_source_snapshot_binds_all_helpers_and_rejects_missing_blob(self):
        with patch.object(history, "REPO", self.root), patch.object(history, "SOURCE_FILES", ("test.py",)), \
                patch.object(inventory, "code_state", return_value=state(PROBE_HEAD)):
            (self.root / "test.py").write_bytes(b"small source")
            with patch.object(history.subprocess, "run", return_value=type("R", (), {"returncode": 0, "stdout": b"small source"})()):
                value = history.source_snapshot(PROBE_HEAD)
                self.assertEqual(value["files"]["test.py"]["sha256"], hashlib.sha256(b"small source").hexdigest())
            for status, raw in ((128, b""), (0, b"different")):
                with patch.object(history.subprocess, "run", return_value=type("R", (), {"returncode": status, "stdout": raw})()):
                    with self.assertRaisesRegex(history.HistoryBlocked, "source_dependency_uncommitted_or_changed"):
                        history.source_snapshot(PROBE_HEAD)
        self.assertEqual(len(history.SOURCE_FILES), 6)

    def test_same_length_owner_or_journal_mutation_never_completes(self):
        for name in ("OWNER.json", "REQUESTS.jsonl"):
            changed = False
            original = FakeRPC()
            def corrupt(endpoint, payload, **kwargs):
                nonlocal changed
                answer = original(endpoint, payload, **kwargs)
                if not changed:
                    path = self.run_dir / name
                    raw = path.read_bytes()
                    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
                    changed = True
                return answer
            with self.subTest(name=name):
                self.blocked("owned_control_content_changed", transport=corrupt)

    def test_nonzero_prefix_and_known_missing_upgrade_fail_closed(self):
        self.rpc.overrides["eth_getLogs"] = lambda value, payload: [{}] if "topics" not in payload["params"][0] else value
        self.blocked("prefix_contains_emitted_logs")
        self.rpc.overrides.clear()
        self.rpc.logs = []
        self.blocked("known_upgrade_absent_or_differs")

    def test_upgrade_native_fields_removed_topics_data_bounds_and_duplicates(self):
        cases = [("removed", True, "native_log_removed_or_unconfirmed"),
                 ("address", "0x" + "11" * 20, "upgrade_address_or_genesis_invalid"),
                 ("topics", [history.UPGRADED_TOPIC], "upgrade_topic_arity_or_data_invalid"),
                 ("data", "0x00", "upgrade_topic_arity_or_data_invalid"),
                 ("logIndex", True, "rpc_invalid_hex_quantity"),
                 ("transactionHash", "bad", "native_log_hash_invalid"),
                 ("blockNumber", "0x1", "known_upgrade_absent_or_differs")]
        for field, replacement, code in cases:
            with self.subTest(field=field):
                self.rpc = FakeRPC()
                self.rpc.logs[0][field] = replacement
                self.blocked(code)
        self.rpc = FakeRPC()
        self.rpc.logs *= 2
        self.blocked("duplicate_or_conflicting_native_identity")

    def test_zero_implementation_word_and_event_count_cap(self):
        self.rpc.logs[0]["topics"][1] = "0x" + "00" * 32
        self.blocked("upgrade_implementation_word_invalid")
        self.rpc = FakeRPC()
        with patch.object(history, "MAX_UPGRADES", 0):
            self.blocked("upgrade_event_limit")

    def test_order_strict_even_across_distinct_transactions_in_one_block(self):
        self.rpc.logs.append(upgrade(index=751, tx="0x" + "44" * 32, tx_index=63))
        self.blocked("in_block_log_order_not_strict")

    def test_same_block_multiple_upgrades_use_receipt_order_and_endpoint_slots(self):
        intermediate = "0x" + "12" * 20
        self.rpc.logs.insert(0, upgrade(index=751, implementation=intermediate))
        value = self.execute()
        self.assertEqual(value["status"], "bounded_v3_sparse_history_complete", value)
        self.assertEqual(len(value["upgrades"]), 2)
        self.assertEqual(value["receipt_proofs"][0]["upgrade_log_indices"], [751, 752])
        proof = value["upgrade_block_proofs"][0]
        self.assertEqual(len(proof["in_block_upgrades"]), 2)
        self.assertFalse(proof["whole_block_implementation_assignment_admitted"])
        self.assertEqual(proof["before_block_slot"]["implementation_address"], history.prior.OLD_IMPLEMENTATION)

    def test_receipt_missing_failed_or_inconsistent_native_evidence(self):
        cases = [(lambda _: None, "upgrade_receipt_missing"),
                 (lambda value: {**value, "status": "0x0"}, "upgrade_receipt_unsuccessful"),
                 (lambda value: {**value, "transactionHash": "0x" + "aa" * 32}, "upgrade_receipt_transaction_mismatch"),
                 (lambda value: {**value, "blockHash": "0x" + "aa" * 32}, "upgrade_receipt_block_hash_mismatch"),
                 (lambda value: {**value, "logs": []}, "upgrade_receipt_logs_invalid"),
                 (lambda value: {**value, "logs": value["logs"] * 2}, "receipt_log_order_or_duplicate"),
                 (lambda value: {**value, "logs": [upgrade(index=753)]}, "receipt_upgrade_missing_or_differs_from_ledger")]
        for alter, code in cases:
            with self.subTest(code=code):
                self.rpc = FakeRPC()
                self.rpc.overrides["eth_getTransactionReceipt"] = lambda value, payload: alter(value)
                self.blocked(code)

    def test_slot_words_and_exact_header_reopening(self):
        self.rpc.overrides["eth_getStorageAt"] = lambda *_: "0x0"
        self.blocked("invalid_implementation_storage_word")
        self.rpc = FakeRPC()
        self.rpc.overrides["eth_getStorageAt"] = lambda *_: "0x" + "00" * 32
        self.blocked("post_upgrade_slot_differs")
        self.rpc = FakeRPC()
        counts = {}
        def changed(value, payload):
            number = int(payload["params"][0], 16)
            counts[number] = counts.get(number, 0) + 1
            return {**value, "hash": "0x" + "ab" * 32} if counts[number] > 1 else value
        self.rpc.overrides["eth_getBlockByNumber"] = changed
        self.blocked("source_or_reopened_header_changed")

    def test_rpc_ids_malformed_duplicate_json_and_untrusted_error_do_not_split(self):
        bad = [b'{"jsonrpc":"2.0","id":true,"result":"0x89"}',
               b'{"jsonrpc":"2.0","id":1,"id":1,"result":"0x89"}',
               b'{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"quota SECRET exceeded"}}',
               b'not JSON SECRET']
        for raw in bad:
            with self.subTest(raw=raw[:40]):
                self.rpc = FakeRPC()
                self.rpc.reply_override = lambda *_: (raw, 200)
                value = self.execute()
                self.assertEqual(value["status"], "blocked")
                self.assertEqual(value["rpc_calls_attempted"], 1)
                self.assertNotIn("SECRET", json.dumps(value))

    def test_only_demonstrable_rpc_range_refusal_bisects_same_predicate(self):
        failed = False
        def refuse(reply, payload, _limit):
            nonlocal failed
            if payload["method"] == "eth_getLogs" and not failed:
                failed = True
                return {"jsonrpc": "2.0", "id": payload["id"], "error": {
                    "code": -32005, "message": "block range exceeds maximum SECRET"}}, 200
        self.rpc.reply_override = refuse
        value = self.execute()
        self.assertEqual(value["status"], "bounded_v3_sparse_history_complete", value)
        self.assertEqual(len(value["upgrade_ranges"]), 96)
        self.assertEqual([(row["first_block"], row["last_block"]) for row in value["upgrade_ranges"][:2]],
                         [(0, 499_999), (500_000, 999_999)])
        self.assertEqual(sum(row["status"] == "range_refused" for row in value["request_ledger"]), 1)
        self.assertNotIn("SECRET", json.dumps(value))

    def test_http_413_and_boundary_sized_bodies_split_without_accepting_truncation(self):
        for kind in ("413", "size"):
            self.rpc = FakeRPC()
            refused = False
            def refuse(reply, payload, limit):
                nonlocal refused
                if payload["method"] == "eth_getLogs" and not refused:
                    refused = True
                    return (b"too large", 413) if kind == "413" else (b"x" * limit, 200)
            self.rpc.reply_override = refuse
            with self.subTest(kind=kind):
                value = self.execute()
                self.assertEqual(value["status"], "bounded_v3_sparse_history_complete", value)
                self.assertLessEqual(value["response_bytes_charged"], history.MAX_TOTAL_RESPONSE_BYTES)

    def test_rate_limit_or_wrong_id_range_error_never_causes_adaptive_retry(self):
        for wrong_id in (False, True):
            self.rpc = FakeRPC()
            def refuse(reply, payload, _limit):
                if payload["method"] == "eth_getLogs":
                    return {"jsonrpc": "2.0", "id": payload["id"] + int(wrong_id), "error": {
                        "code": -32005, "message": "block range too wide" if wrong_id else "rate limit SECRET exceeded"}}, 200
            self.rpc.reply_override = refuse
            code = "rpc_reply_id_mismatch" if wrong_id else "rpc_error_not_range_refusal"
            value = self.blocked(code)
            self.assertEqual(value["rpc_calls_attempted"], 5)

    def test_attempt_cap_and_unobserved_transport_charge_are_durable(self):
        with patch.object(history, "MAX_RPC_CALLS", 5):
            value = self.blocked("rpc_attempt_limit")
            self.assertEqual(value["rpc_calls_attempted"], 5)
        def unavailable(*args, **kwargs):
            raise OSError("https://rpc.invalid/SECRET")
        value = self.blocked("bounded_input_transport_or_runtime_failure", transport=unavailable)
        self.assertEqual(value["response_bytes_charged"], history.MAX_RESPONSE_BYTES)
        self.assertEqual(value["response_bytes_received"], 0)
        journal = [json.loads(line) for line in (self.run_dir / "REQUESTS.jsonl").read_text().splitlines()]
        self.assertEqual([item["event"] for item in journal], ["start", "finish"])
        self.assertTrue(journal[1]["unobserved_body_charged_at_full_reservation"])

    def test_total_response_cap_never_exceeded_even_when_body_remains_unknown(self):
        self.rpc.reply_override = lambda reply, payload, limit: (
            (b"x" * limit, 200) if payload["method"] == "eth_getLogs" else None)
        value = self.blocked("total_response_byte_limit")
        self.assertEqual(value["response_bytes_charged"], history.MAX_TOTAL_RESPONSE_BYTES)
        self.assertLessEqual(value["response_bytes_received"], history.MAX_TOTAL_RESPONSE_BYTES)

    def test_ram_and_disk_admission_fail_before_inputs_or_rpc(self):
        for resources, code in (({"ram_bytes": {"MemAvailable": 1}, "disk_bytes": {"free": 10**12}},
                                 "available_ram_below_admission"),
                                ({"ram_bytes": {"MemAvailable": 10**12}, "disk_bytes": {"free": 1}},
                                 "available_disk_below_admission")):
            value = self.blocked(code, resources=resources)
            self.assertEqual(value["rpc_calls_attempted"], 0)
            self.assertFalse(self.run_dir.exists())

    def test_entire_output_ceiling_and_changing_disk_floor_block_publication(self):
        with patch.object(history, "MAX_OUTPUT_BYTES", history.FINAL_RESERVE_BYTES + 50):
            value = self.blocked("all_control_output_byte_limit")
            self.assertEqual(value["rpc_calls_attempted"], 0)
        with patch.object(history, "RUN_ROOT", self.root), \
                patch.object(history.shutil, "disk_usage", return_value=DISK):
            controls = history.Controls(self.root / "control-test", {"owner": True})
            with patch.object(history.shutil, "disk_usage", return_value=DISK._replace(free=history.FREE_FLOOR_BYTES)):
                with self.assertRaisesRegex(history.HistoryBlocked, "free_disk_floor_would_be_crossed"):
                    controls.journal({"another": "record"})

    def test_runtime_limit_is_checked_before_rpc_and_alarm_covers_publication(self):
        events = []
        @contextmanager
        def active_limits():
            events.append("enter")
            yield {"started_monotonic": time.monotonic(), "address_space_soft_bytes": history.MAX_RAM_BYTES,
                   "wall_alarm_seconds": history.MAX_RUNTIME_SECONDS}
            events.append("exit")
        original_write = history.Controls.write
        def write(controls, name, value, **kwargs):
            self.assertEqual(events, ["enter"])
            return original_write(controls, name, value, **kwargs)
        with patch.object(history, "require_production_host"), patch.object(history, "runtime_limits", active_limits), \
                patch.object(history, "_probe_with_limits", side_effect=lambda *args, **kwargs: events.append("body")):
            history.probe(self.plan_path, self.digest, PLAN_HEAD, PROBE_HEAD, run_dir=self.root / "unused")
        self.assertEqual(events, ["enter", "body", "exit"])
        with patch.object(history, "RUN_ROOT", self.root), patch.object(history.shutil, "disk_usage", return_value=DISK):
            controls = history.Controls(self.root / "deadline", {"owner": True})
            rpc = history.HistoryRPC("SECRET", self.rpc, [], controls, 0, clock=lambda: history.MAX_RUNTIME_SECONDS)
            with self.assertRaisesRegex(history.HistoryBlocked, "runtime_deadline_exceeded"):
                rpc.call("eth_chainId", [])
            self.assertEqual(len(rpc.ledger), 0)

    def test_real_runtime_limit_context_installs_and_restores_alarm_and_address_space(self):
        calls = []
        previous = (history.resource.RLIM_INFINITY, history.resource.RLIM_INFINITY)
        def getlimit(_kind):
            return (history.MAX_RAM_BYTES, history.resource.RLIM_INFINITY) if calls else previous
        with patch.object(history.resource, "getrlimit", side_effect=getlimit), \
                patch.object(history.resource, "setrlimit", side_effect=lambda *args: calls.append(args)), \
                patch.object(history.signal, "getsignal", return_value="old"), \
                patch.object(history.signal, "getitimer", return_value=(0.0, 0.0)), \
                patch.object(history.signal, "signal") as handler, patch.object(history.signal, "setitimer") as alarm:
            with history.runtime_limits() as enforcement:
                self.assertEqual(enforcement["address_space_soft_bytes"], history.MAX_RAM_BYTES)
            self.assertEqual(calls[-1][1], previous)
            self.assertEqual(alarm.call_args_list[0].args, (history.signal.ITIMER_REAL, history.MAX_RUNTIME_SECONDS))
            self.assertEqual(alarm.call_args_list[-1].args, (history.signal.ITIMER_REAL, 0))
            self.assertEqual(handler.call_args_list[-1].args, (history.signal.SIGALRM, "old"))

    def test_receipt_upgrade_missing_and_unreported_extra_upgrade_are_blockers(self):
        other_topic = "0x" + "11" * 32
        for missing in (True, False):
            self.rpc = FakeRPC()
            def change(value, _payload):
                if missing:
                    value["logs"][0]["topics"] = [other_topic]
                else:
                    value["logs"].append(upgrade(index=753))
                return value
            self.rpc.overrides["eth_getTransactionReceipt"] = change
            with self.subTest(missing=missing):
                self.blocked("ledger_upgrade_missing_from_receipt" if missing else
                             "receipt_upgrade_missing_or_differs_from_ledger")

    def test_native_transaction_hash_and_index_conflicts_are_blocked(self):
        for same_tx in (True, False):
            self.rpc = FakeRPC()
            self.rpc.logs.append(upgrade(index=753, tx_index=63 if same_tx else 62,
                                         tx=history.prior.UPGRADE_TRANSACTION if same_tx else "0x" + "33" * 32))
            with self.subTest(same_tx=same_tx):
                self.blocked("transaction_locator_conflict")

    def test_fixed_endpoint_plan_and_input_mutation_are_blocked(self):
        self.plan["raw_footer_tail_header"]["number"] -= 1
        self.save_plan()
        self.blocked("source_plan_raw_endpoint_differs")
        self.plan["raw_footer_tail_header"]["number"] += 1
        self.save_plan()
        original = self.rpc
        changed = False
        def mutate(endpoint, payload, **kwargs):
            nonlocal changed
            value = original(endpoint, payload, **kwargs)
            if not changed:
                self.plan_path.write_bytes(self.plan_path.read_bytes() + b" ")
                changed = True
            return value
        self.blocked("source_plan_changed_during_probe", transport=mutate)

    def test_forbidden_methods_and_unchanged_predicates(self):
        with patch.object(history, "RUN_ROOT", self.root), patch.object(history.shutil, "disk_usage", return_value=DISK):
            controls = history.Controls(self.root / "method", {"owner": True})
            rpc = history.HistoryRPC("SECRET", self.rpc, [], controls, time.monotonic())
            with self.assertRaisesRegex(history.HistoryBlocked, "rpc_method_not_permitted"):
                rpc.call("eth_sendTransaction", [])
            self.assertEqual(rpc.ledger, [])
        value = self.execute()
        self.assertEqual(value["status"], "bounded_v3_sparse_history_complete")
        for item in self.rpc.calls:
            if item["method"] == "eth_getLogs":
                predicate = item["params"][0]
                self.assertEqual(predicate["address"], history.ADDRESS)
                if "topics" in predicate:
                    self.assertEqual(predicate["topics"], [history.UPGRADED_TOPIC])
                else:
                    self.assertLessEqual(int(predicate["toBlock"], 16), history.PREFIX_END)

    def test_output_freshness_symlink_and_foreign_artifact_checks(self):
        with patch.object(history, "RUN_ROOT", self.root), patch.object(history.shutil, "disk_usage", return_value=DISK):
            path = self.root / "existing"
            path.mkdir()
            with self.assertRaisesRegex(history.HistoryBlocked, "run_directory_already_exists"):
                history.Controls(path, {"owner": True})
            link = self.root / "link"
            link.symlink_to(path)
            with self.assertRaisesRegex(history.HistoryBlocked, "run_directory_already_exists"):
                history.Controls(link, {"owner": True})
            controls = history.Controls(self.root / "foreign", {"owner": True})
            (controls.root / "unexpected").write_bytes(b"foreign")
            with self.assertRaisesRegex(history.HistoryBlocked, "unexpected_control_artifact"):
                controls.verify()

    def test_http_reader_has_no_unaccounted_sentinel_byte(self):
        class Response:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *_):
                return False
            def read(self, size):
                self.size = size
                return b"x" * size
        response = Response()
        with patch.object(history.urllib.request, "urlopen", return_value=response) as request:
            value = history.http_response("https://rpc.invalid/SECRET", {"id": 1}, byte_limit=23, timeout=5)
        self.assertEqual(response.size, 23)
        self.assertEqual(len(value.body), 23)
        self.assertEqual(request.call_args.kwargs["timeout"], 5)

    def test_completion_file_fsync_failure_leaves_no_visible_marker(self):
        original = history.Controls.write
        def fail_marker(controls, name, value, **kwargs):
            if name == "COMPLETE.json":
                with patch.object(history.os, "fsync", side_effect=OSError("fixture marker fsync failure")):
                    return original(controls, name, value, **kwargs)
            return original(controls, name, value, **kwargs)
        with patch.object(history.Controls, "write", fail_marker):
            with self.assertRaises(OSError):
                self.execute()
        self.assertFalse((self.run_dir / "COMPLETE.json").exists())
        self.assertTrue((self.run_dir / ".COMPLETE.pending").exists())
        self.assertLessEqual(sum(path.stat().st_size for path in self.run_dir.iterdir()), history.MAX_OUTPUT_BYTES)

    def test_completion_directory_fsync_precedes_publication_and_failure_is_not_success(self):
        original_write, original_fsync = history.Controls.write, history.os.fsync
        def fail_marker(controls, name, value, **kwargs):
            def fsync(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise OSError("fixture directory fsync failure")
                return original_fsync(fd)
            if name == "COMPLETE.json":
                with patch.object(history.os, "fsync", side_effect=fsync):
                    return original_write(controls, name, value, **kwargs)
            return original_write(controls, name, value, **kwargs)
        with patch.object(history.Controls, "write", fail_marker):
            with self.assertRaises(OSError):
                self.execute()
        self.assertFalse((self.run_dir / "COMPLETE.json").exists())
        self.assertTrue((self.run_dir / ".COMPLETE.pending").exists())

    def test_completion_flush_close_and_reopen_failures_never_publish(self):
        original_write, original_fdopen, original_open = history.Controls.write, history.os.fdopen, Path.open
        for failure in ("flush", "close", "reopen"):
            def fail_marker(controls, name, value, **kwargs):
                if name != "COMPLETE.json":
                    return original_write(controls, name, value, **kwargs)
                class Stream:
                    def __init__(self, *args, **options):
                        self.stream = original_fdopen(*args, **options)
                    def __enter__(self):
                        self.stream.__enter__()
                        return self
                    def __exit__(self, *args):
                        self.stream.__exit__(*args)
                        if failure == "close":
                            raise OSError("fixture close failure")
                    def write(self, raw):
                        return self.stream.write(raw)
                    def flush(self):
                        if failure == "flush":
                            raise OSError("fixture flush failure")
                        return self.stream.flush()
                    def fileno(self):
                        return self.stream.fileno()
                def read(path, *args, **options):
                    if path.name == ".COMPLETE.pending" and failure == "reopen":
                        raise OSError("fixture reopen failure")
                    return original_open(path, *args, **options)
                with patch.object(history.os, "fdopen", Stream), patch.object(Path, "open", read):
                    return original_write(controls, name, value, **kwargs)
            with self.subTest(failure=failure), patch.object(history.Controls, "write", fail_marker):
                with self.assertRaises(OSError):
                    self.execute()
                self.assertFalse((self.run_dir / "COMPLETE.json").exists())

    def test_completion_rename_is_final_artifact_io_and_does_not_duplicate_temporary_bytes(self):
        original_write, original_fsync, original_commit = history.Controls.write, history.os.fsync, history.atomic_completion
        events = []
        def record(controls, name, value, **kwargs):
            if name != "COMPLETE.json":
                return original_write(controls, name, value, **kwargs)
            before = controls.bytes
            def fsync(fd):
                events.append("directory_fsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file_fsync")
                return original_fsync(fd)
            def commit(temporary, destination):
                self.assertTrue(temporary.exists())
                self.assertFalse(destination.exists())
                self.assertEqual(controls.bytes, before + temporary.stat().st_size)
                self.assertEqual(sum(path.stat().st_size for path in controls.root.iterdir()), controls.bytes)
                events.append("commit")
                return original_commit(temporary, destination)
            with patch.object(history.os, "fsync", side_effect=fsync), patch.object(history, "atomic_completion", side_effect=commit):
                return original_write(controls, name, value, **kwargs)
        with patch.object(history.Controls, "write", record):
            value = self.execute()
        self.assertEqual(value["status"], "bounded_v3_sparse_history_complete")
        self.assertEqual(events, ["file_fsync", "directory_fsync", "commit"])
        self.assertTrue((self.run_dir / "COMPLETE.json").exists())
        self.assertFalse((self.run_dir / ".COMPLETE.pending").exists())

    def test_atomic_completion_refuses_competing_destination_without_overwrite(self):
        with patch.object(history, "RUN_ROOT", self.root), patch.object(history.shutil, "disk_usage", return_value=DISK):
            controls = history.Controls(self.root / "atomic-race", {"owner": True})
            original = history.atomic_completion
            def competitor(temporary, destination):
                destination.write_bytes(b"competing owner")
                return original(temporary, destination)
            with patch.object(history, "atomic_completion", side_effect=competitor):
                with self.assertRaisesRegex(history.HistoryBlocked, "atomic_completion_refused"):
                    controls.write("COMPLETE.json", {"status": "would otherwise succeed"}, final=True)
            self.assertEqual((controls.root / "COMPLETE.json").read_bytes(), b"competing owner")
            self.assertTrue((controls.root / ".COMPLETE.pending").exists())

    def test_completion_pending_file_is_inside_aggregate_cap_after_failed_stage(self):
        original_write, original_fsync = history.Controls.write, history.os.fsync
        owners = []
        def fail_marker(controls, name, value, **kwargs):
            if name == "COMPLETE.json":
                owners.append(controls)
                def fsync(fd):
                    if stat.S_ISDIR(os.fstat(fd).st_mode):
                        raise OSError("fixture directory failure")
                    return original_fsync(fd)
                with patch.object(history.os, "fsync", side_effect=fsync):
                    return original_write(controls, name, value, **kwargs)
            return original_write(controls, name, value, **kwargs)
        with patch.object(history.Controls, "write", fail_marker):
            with self.assertRaises(OSError):
                self.execute()
        controls = owners[0]
        self.assertTrue(controls.pending_marker)
        self.assertIn(".COMPLETE.pending", controls.expected)
        self.assertEqual(sum(path.stat().st_size for path in controls.root.iterdir()), controls.bytes)
        self.assertLessEqual(controls.bytes, history.MAX_OUTPUT_BYTES)


if __name__ == "__main__":
    unittest.main()
