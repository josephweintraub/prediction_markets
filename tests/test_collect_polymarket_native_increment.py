"""Synthetic bounded collector fixtures: no real RPC, production data or EC2."""
from contextlib import redirect_stderr, redirect_stdout
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_increment_capacity as pilot
from scripts import collect_polymarket_native_increment as collector

INVENTORY_HEAD, PLAN_HEAD, COLLECTOR_HEAD = "a" * 40, "b" * 40, "c" * 40


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
    def __init__(self):
        self.calls, self.empty, self.mutate = [], False, None

    def __call__(self, endpoint, payload):
        self.calls.append(payload)
        if payload["method"] == "eth_chainId":
            result = "0x89"
        elif payload["method"] == "eth_getBlockByNumber":
            number = int(payload["params"][0], 16)
            value = header(number)
            result = {**value, "number": hex(number), "timestamp": hex(value["timestamp"])}
        else:
            query = payload["params"][0]
            result = [] if self.empty else [native_log(address, int(query["fromBlock"], 16), index)
                                           for index, address in enumerate(query["address"])]
        reply = {"jsonrpc": "2.0", "id": payload["id"], "result": result}
        if self.mutate:
            reply = self.mutate(payload, reply)
        return json.dumps(reply).encode()


class CollectorTests(unittest.TestCase):
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
        self.plan = {
            "schema_version": "polymarket_refresh_source_plan_v1", "status": "source_plan_ready_for_review",
            "planner_source": code(PLAN_HEAD), "final_input_identity_unchanged": True,
            "cutoff_headers_reopened_unchanged": True, "end_exclusive_utc": planner.END_EXCLUSIVE,
            "end_exclusive_unix_seconds": planner.utc_cutoff(planner.END_EXCLUSIVE),
            "last_included_header": header(2999), "first_excluded_header": header(3000),
            "provisional_append_start": planner.raw_append_start(value, None), "provisional_append_blocks": 2000,
            "raw_footer_tail_header": header(999),
            "inventory_binding": {"path": str(self.inventory_path), "sha256": collector.digest(self.inventory_path.read_bytes()),
                                  "source_head": INVENTORY_HEAD,
                                  "identity": inventory.path_identity(self.inventory_path)}}
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_text(json.dumps(self.plan))
        self.plan_digest = collector.digest(self.plan_path.read_bytes())
        self.output, self.rpc = self.root / "collection", FakeRPC()

    def execute(self, **changes):
        args = dict(mode="append", first_block=1000, last_block=1299, max_rpc_calls=100,
                    max_output_bytes=32 * 1024**2, max_runtime_seconds=120, max_rows=1000,
                    max_ram_bytes=512 * 1024**2, transport=self.rpc)
        args["max_total_response_bytes"] = 256 * 1024**2
        args.update(changes)
        with patch.object(inventory, "code_state", return_value=code(COLLECTOR_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)):
            return collector.run_collection(self.plan_path, self.plan_digest, PLAN_HEAD, COLLECTOR_HEAD,
                                            self.output, **args)

    def test_gap_free_complete_shards_native_roundtrip_and_contract_zero_receipts(self):
        result = self.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        self.assertEqual(result["validated_windows"], 3)
        self.assertEqual(result["rows"], 15)
        self.assertFalse(result["analysis_admitted"])
        manifest = json.loads((self.output / "manifest.json").read_text())
        marker = json.loads((self.output / "COMPLETE.json").read_text())
        self.assertEqual(marker["manifest_sha256"], collector.digest((self.output / "manifest.json").read_bytes()))
        self.assertEqual(manifest["distinct_native_keys"], 15)
        self.assertEqual(len(manifest["windows"]), 3)
        for record in manifest["windows"]:
            rows = pq.read_table(self.output / record["payload"]["path"]).to_pylist()
            self.assertEqual(len(rows), 5)
            self.assertEqual(len(record["contract_intervals"]), 5)
            self.assertTrue(all(item["status"] == "success_logs" for item in record["contract_intervals"]))
        for path in self.output.rglob("*.json"):
            self.assertNotIn("SECRET", path.read_text())

    def test_successful_zero_log_intervals_are_complete_not_missing(self):
        self.rpc.empty = True
        result = self.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        manifest = json.loads((self.output / "manifest.json").read_text())
        self.assertEqual(result["rows"], 0)
        self.assertTrue(all(item["status"] == "success_zero_logs" for window in manifest["windows"]
                            for item in window["contract_intervals"]))

    def test_fixed_modes_preserve_exact_address_roster_and_boundaries(self):
        for mode, first, last, allowed in (("append", 1000, 2999, pilot.ADDRESSES),
                                         ("v3-historical", 1, 999, (pilot.ADDRESSES[-1],)),
                                         ("endpoint-overlap", 900, 999, pilot.ADDRESSES)):
            with self.subTest(mode=mode):
                self.assertEqual(collector.requested_addresses(mode, first, last, self.plan), allowed)
        for mode, first, last in (("append", 999, 1000), ("append", 1000, 3000),
                                 ("v3-historical", 999, 1000), ("endpoint-overlap", 901, 999),
                                 ("arbitrary", 1, 2)):
            with self.subTest(mode=mode), self.assertRaises(pilot.PilotBlocked):
                collector.requested_addresses(mode, first, last, self.plan)
        result = self.execute(mode="v3-historical", first_block=900, last_block=999)
        self.assertEqual(result["rows"], 1)
        calls = [item for item in self.rpc.calls if item["method"] == "eth_getLogs"]
        self.assertEqual(calls[0]["params"][0]["address"], [pilot.ADDRESSES[-1]])

    def test_source_plan_head_and_clean_source_are_required_before_network(self):
        with patch.object(inventory, "code_state", return_value=code("f" * 40)):
            with self.assertRaisesRegex(pilot.PilotBlocked, "source_head_or_clean"):
                collector.run_collection(self.plan_path, self.plan_digest, PLAN_HEAD, COLLECTOR_HEAD, self.output,
                    mode="append", first_block=1000, last_block=1299, max_rpc_calls=100,
                    max_output_bytes=32 * 1024**2, max_runtime_seconds=120, max_rows=1000,
                    max_ram_bytes=512 * 1024**2, max_total_response_bytes=256 * 1024**2, transport=self.rpc)
        self.assertFalse(self.output.exists())
        self.assertEqual(self.rpc.calls, [])

    def test_production_guard_precedes_any_plan_or_private_endpoint_access(self):
        argv = ["--source-plan", "/never/read", "--source-plan-sha256", "x", "--plan-source-head", "x",
                "--expected-head", "x", "--output-dir", "/never/write", "--mode", "append",
                "--first-block", "1", "--last-block", "100", "--max-rpc-calls", "20",
                "--max-output-bytes", "32000000", "--max-runtime-seconds", "20", "--max-rows", "10",
                "--max-ram-bytes", "536870912", "--max-total-response-bytes", "268435456"]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(pilot, "read_plan") as read, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                collector.main(argv)
        read.assert_not_called()

    def test_main_sets_single_thread_environment_and_address_space_before_collection(self):
        argv = ["--source-plan", "/never/read", "--source-plan-sha256", "x", "--plan-source-head", "x",
                "--expected-head", "x", "--output-dir", "/mnt/data/runs/fixture", "--mode", "append",
                "--first-block", "1", "--last-block", "100", "--max-rpc-calls", "20",
                "--max-output-bytes", "32000000", "--max-runtime-seconds", "20", "--max-rows", "10",
                "--max-ram-bytes", "8589934592", "--max-total-response-bytes", "268435456"]
        def collect(*args, **kwargs):
            for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
                self.assertEqual(os.environ[variable], "1")
            self.assertEqual(kwargs["max_ram_bytes"], 8589934592)
            return {"status": "raw_collection_complete"}
        with patch("production_guard.require_production_host"), \
                patch.object(inventory, "resource_state", return_value={"ram_bytes": {"MemAvailable": 16 * 1024**3}}), \
                patch.object(collector.resource, "setrlimit") as address_space, \
                patch.object(collector, "run_collection", side_effect=collect), \
                patch.dict(os.environ, {"OPENBLAS_NUM_THREADS": "32", "OMP_NUM_THREADS": "32", "NUMEXPR_NUM_THREADS": "32"}), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(collector.main(argv), 0)
        address_space.assert_called_once_with(collector.resource.RLIMIT_AS, (8589934592, 8589934592))

    def test_duplicate_and_conflicting_payloads_fail_no_silent_deduplication(self):
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs":
                reply["result"].append(dict(reply["result"][0]))
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "duplicate_native_log_key")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertEqual(len(list((self.output / "attempts").iterdir())), 1)

    def test_cross_window_native_key_conflict_is_blocked(self):
        original = native_log(pilot.ADDRESSES[0], 1000)
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs" and int(payload["params"][0]["fromBlock"], 16) > 1000:
                reply["result"][0]["transactionHash"] = original["transactionHash"]
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "conflicting_native_log_key_across_windows")
        self.assertEqual(result["validated_windows"], 1)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def interrupt_second_window(self):
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs" and int(payload["params"][0]["fromBlock"], 16) == 1100:
                raise KeyboardInterrupt("fixture interruption")
            return reply
        self.rpc.mutate = mutate
        with self.assertRaises(KeyboardInterrupt):
            self.execute()
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.rpc.mutate = None

    def test_interrupted_resume_revalidates_success_and_consumes_attempt_and_calls(self):
        self.interrupt_second_window()
        first_calls = len(self.rpc.calls)
        result = self.execute(resume=True)
        self.assertEqual(result["status"], "raw_collection_complete")
        self.assertEqual(result["rpc_calls_charged"], len(self.rpc.calls))
        subsequent_getlogs = [item for item in self.rpc.calls[first_calls:] if item["method"] == "eth_getLogs"]
        self.assertEqual(len(subsequent_getlogs), 2)
        self.assertTrue((self.output / "attempts" / "b000000001100-000000001199.a1" / "START.json").exists())
        self.assertTrue((self.output / "attempts" / "b000000001100-000000001199.a2" / "SUCCESS.json").exists())

    def test_resume_hash_conflict_blocks_before_new_network(self):
        self.interrupt_second_window()
        payload = next((self.output / "attempts").glob("*/payload.parquet"))
        with payload.open("ab") as stream:
            stream.write(b"corruption")
        calls = len(self.rpc.calls)
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "resume_payload_size_or_type_differs")
        self.assertEqual(len(self.rpc.calls), calls)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_resume_changed_caps_or_request_predicate_is_rejected(self):
        self.interrupt_second_window()
        calls = len(self.rpc.calls)
        with self.assertRaisesRegex(pilot.PilotBlocked, "resume_contract_differs"):
            self.execute(resume=True, max_rows=999)
        self.assertEqual(len(self.rpc.calls), calls)

    def test_existing_directory_and_completed_run_never_overwritten(self):
        self.output.mkdir()
        with self.assertRaises(FileExistsError):
            self.execute()
        self.assertEqual(self.rpc.calls, [])

    def test_successful_run_cannot_be_resumed_or_overwritten(self):
        self.execute()
        original = (self.output / "COMPLETE.json").read_bytes()
        calls = len(self.rpc.calls)
        with self.assertRaisesRegex(pilot.PilotBlocked, "run_already_complete"):
            self.execute(resume=True)
        self.assertEqual((self.output / "COMPLETE.json").read_bytes(), original)
        self.assertEqual(len(self.rpc.calls), calls)

    def test_kernel_run_lock_prevents_concurrent_resume(self):
        self.interrupt_second_window()
        fd = os.open(self.output / "LOCK", os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(pilot.PilotBlocked, "another_collector_holds_run_lock"):
                self.execute(resume=True)
        finally:
            os.close(fd)

    def test_interrupted_unadmitted_attempt_directory_is_consumed_without_adopting_files(self):
        self.interrupt_second_window()
        empty = self.output / "attempts" / "b000000001200-000000001299.a1"
        empty.mkdir()
        result = self.execute(resume=True)
        self.assertEqual(result["status"], "raw_collection_complete")
        self.assertTrue((self.output / "attempts" / "b000000001200-000000001299.a2" / "SUCCESS.json").exists())

    def test_checkpoint_failure_never_publishes_complete_or_silently_retries(self):
        real_publish = collector.publish
        def fail_success(path, raw):
            if path.name == "SUCCESS.json":
                raise OSError("fixture write failure")
            return real_publish(path, raw)
        with patch.object(collector, "publish", side_effect=fail_success):
            result = self.execute()
        self.assertEqual(result["status"], "failed_raw_collection")
        self.assertEqual(len(list((self.output / "attempts").iterdir())), 1)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_response_byte_cap_is_charged_before_another_call(self):
        result = self.execute(max_total_response_bytes=pilot.MAX_RESPONSE_BYTES + 1)
        self.assertEqual(result["error_code"], "stage_response_byte_limit")
        self.assertEqual(len(self.rpc.calls), 1)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_pending_failed_rpc_is_charged_conservatively_when_resumed(self):
        self.interrupt_second_window()
        result = self.execute(resume=True)
        finishes = list((self.output / "sessions").glob("*/rpc/*.FINISH.json"))
        known_bytes = sum(json.loads(path.read_text())["response_bytes"] for path in finishes)
        self.assertEqual(result["response_bytes_charged"], known_bytes + pilot.MAX_RESPONSE_BYTES + 1)

    def test_cumulative_elapsed_runtime_cannot_reset_on_resume(self):
        self.interrupt_second_window()
        result_file = next((self.output / "sessions").glob("*/RESULT.json"))
        result = json.loads(result_file.read_text())
        result["session_elapsed_seconds"] = 120
        result_file.write_text(json.dumps(result))
        calls = len(self.rpc.calls)
        with self.assertRaisesRegex(pilot.PilotBlocked, "stage_runtime_limit"):
            self.execute(resume=True)
        self.assertEqual(len(self.rpc.calls), calls)

    def test_successful_ledger_corruption_blocks_resume_before_network(self):
        self.interrupt_second_window()
        ledger = next((self.output / "attempts").glob("*/REQUESTS.json"))
        with ledger.open("ab") as stream:
            stream.write(b" ")
        calls = len(self.rpc.calls)
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "resume_request_ledger_hash_differs")
        self.assertEqual(len(self.rpc.calls), calls)

    def test_finite_call_row_runtime_and_ram_caps_fail_without_complete(self):
        for options, expected in (({"max_rpc_calls": 4}, "stage_rpc_call_limit"),
                                  ({"max_rows": 4}, "stage_row_limit"),
                                  ({"max_ram_bytes": collector.TRANSIENT_RESERVE}, "identity_state_ram_limit")):
            with self.subTest(options=options):
                self.output = self.root / str(len(list(self.root.iterdir())))
                result = self.execute(**options)
                self.assertEqual(result["error_code"], expected)
                self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_provider_error_or_oversize_fails_closed_without_adaptive_split(self):
        def mutate(payload, reply):
            if payload["method"] == "eth_getLogs":
                return {"jsonrpc": "2.0", "id": payload["id"], "error": {"code": -32005, "message": "too large"}}
            return reply
        self.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "rpc_invalid_reply_shape")
        self.assertEqual(len([item for item in self.rpc.calls if item["method"] == "eth_getLogs"]), 1)

    def test_transport_retries_at_most_three_and_never_emits_secret_exception(self):
        def transport(endpoint, payload):
            if payload["method"] == "eth_getLogs":
                self.rpc.calls.append(payload)
                raise OSError("https://rpc.invalid/SECRET")
            return self.rpc(endpoint, payload)
        result = self.execute(transport=transport)
        self.assertEqual(result["status"], "failed_raw_collection")
        self.assertEqual(len([item for item in self.rpc.calls if item["method"] == "eth_getLogs"]), 3)
        self.assertEqual(len(list((self.output / "attempts").iterdir())), 3)
        for path in self.output.rglob("*.json"):
            self.assertNotIn("SECRET", path.read_text())

    def test_live_unknown_transport_bytes_cannot_escape_response_budget(self):
        failures = 0
        def transport(endpoint, payload):
            nonlocal failures
            if payload["method"] == "eth_getLogs" and failures < 2:
                failures += 1
                self.rpc.calls.append(payload)
                raise OSError("unknown response before connection loss")
            return self.rpc(endpoint, payload)
        result = self.execute(transport=transport, max_total_response_bytes=pilot.MAX_RESPONSE_BYTES + 100000)
        self.assertEqual(result["status"], "failed_raw_collection")
        self.assertEqual(result["error_code"], "stage_response_byte_limit")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertEqual(failures, 1)
        self.assertGreaterEqual(result["response_bytes_charged"], pilot.MAX_RESPONSE_BYTES + 1)

    def test_observed_header_drift_on_retry_is_blocked(self):
        failed, first_headers = False, 0
        def transport(endpoint, payload):
            nonlocal failed, first_headers
            if payload["method"] == "eth_getLogs" and not failed:
                failed = True
                self.rpc.calls.append(payload)
                raise OSError("fixture")
            raw = self.rpc(endpoint, payload)
            if payload["method"] == "eth_getBlockByNumber" and payload["params"][0] == hex(1000):
                first_headers += 1
                if first_headers > 1:
                    value = json.loads(raw)
                    value["result"]["hash"] = "0x" + "f" * 64
                    return json.dumps(value).encode()
            return raw
        result = self.execute(transport=transport)
        self.assertEqual(result["error_code"], "rpc_reopened_header_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_interval_gaps_overlaps_and_missing_contract_zero_receipts_block(self):
        result = self.execute()
        manifest = json.loads((self.output / "manifest.json").read_text())
        records = manifest["windows"]
        planned = collector.windows(1000, 1299, 100)
        with self.assertRaisesRegex(pilot.PilotBlocked, "incomplete_interval_ledger"):
            collector.verify_coverage(records[:-1], planned, pilot.ADDRESSES)
        records[1]["contract_intervals"].pop()
        with self.assertRaisesRegex(pilot.PilotBlocked, "per_contract_interval_ledger_differs"):
            collector.verify_coverage(records, planned, pilot.ADDRESSES)

    def test_uint64_request_id_and_provider_timestamp_not_a_clock(self):
        window = collector.windows(1000, 1099, 100)[0]
        log = {**native_log(pilot.ADDRESSES[0], 1001), "blockTimestamp": "not a clock"}
        rows, evidence = pilot.validate_logs([log], window, {}, 12345)
        raw = collector.parquet_payload(rows)
        reopened = pq.read_table(pa.BufferReader(raw)).to_pylist()
        self.assertEqual(reopened[0]["source_request_id"], 12345)
        self.assertEqual(reopened[0]["blockTimestamp"], "not a clock")
        self.assertFalse(evidence["provider_block_timestamp_used_as_clock"])


if __name__ == "__main__":
    unittest.main()
