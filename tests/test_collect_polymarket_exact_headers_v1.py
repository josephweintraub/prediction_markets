"""Synthetic, network-free adversarial fixtures for the literal-header pilot."""
from __future__ import annotations

from collections import namedtuple
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

from scripts import collect_polymarket_exact_headers_v1 as pilot

EXPECTED_HEAD = "a" * 40
SOURCE = {"head": EXPECTED_HEAD, "tracked_worktree_clean": True, "files": {"fixture": "b" * 64}}
Disk = namedtuple("Disk", "total used free")


def block_hash(number):
    return "0x" + f"{number:064x}"


def header(number):
    return {"number": hex(number), "timestamp": hex(1_000 + number),
            "hash": block_hash(number), "parentHash": block_hash(number - 1)}


class FakeRPC:
    def __init__(self, change=None):
        self.calls = []
        self.change = change

    def __call__(self, endpoint, payload, *, byte_limit, timeout):
        self.calls.append(copy.deepcopy(payload))
        requests = payload if type(payload) is list else [payload]
        values = [{"jsonrpc": "2.0", "id": item["id"],
                   "result": "0x89" if item["method"] == "eth_chainId"
                   else header(int(item["params"][0], 16))} for item in requests]
        result = list(reversed(values)) if type(payload) is list else values[0]
        if self.change is not None:
            changed = self.change(result, payload, len(self.calls))
            if changed is not None:
                result = changed
        if isinstance(result, tuple):
            return result
        return json.dumps(result).encode(), 200


class ExactHeaderPilotTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.request_file = self.root / "keys.json"
        self.private = self.root / "private_config"
        self.private.write_text("private-endpoint-value")
        self.output = self.root / "pilot"
        self.source_patch = patch.object(pilot, "source_snapshot", return_value=copy.deepcopy(SOURCE))
        self.snapshot = self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.endpoint_patch = patch.object(pilot.inventory, "load_rpc_endpoint",
                                           return_value=("https://private.invalid/secret", "private_file"))
        self.endpoint = self.endpoint_patch.start()
        self.addCleanup(self.endpoint_patch.stop)
        self.disk_patch = patch.object(pilot.shutil, "disk_usage",
                                       return_value=Disk(100_000_000_000, 0, 100_000_000_000))
        self.disk = self.disk_patch.start()
        self.addCleanup(self.disk_patch.stop)
        self.write_keys([{"block_number": number} for number in (10, 11, 20)])

    def write_keys(self, keys):
        self.request_file.write_bytes(json.dumps(keys).encode())
        self.request_sha = hashlib.sha256(self.request_file.read_bytes()).hexdigest()

    def execute(self, transport=None, **kwargs):
        self.rpc = transport or FakeRPC()
        return pilot.run_pilot(self.request_file, self.request_sha, EXPECTED_HEAD, self.output,
                               rpc_private_file=self.private, transport=self.rpc, **kwargs)

    def assert_blocked(self, result, code=None):
        self.assertEqual(result["status"], "blocked")
        self.assertFalse((self.output / "RESULT.json").exists())
        if code:
            self.assertEqual(result["error_code"], code)
        self.assertTrue((self.output / "FAILED.json").is_file())

    def test_shuffled_replies_exact_keys_parquet_chunks_and_narrow_scope(self):
        self.write_keys([{"block_number": number, "block_hash": block_hash(number).upper().replace("0X", "0x")}
                         for number in reversed(range(10, 23))])
        result = self.execute()
        self.assertEqual(result["status"], "literal_header_pilot_complete")
        self.assertEqual(result["accepted_keys"], 13)
        self.assertFalse(result["final_sample_admitted"])
        self.assertFalse(result["history_completeness_certified"])
        self.assertEqual(result["fallback_rows"], 0)
        self.assertEqual(len(result["chunk_bindings"]), 2)
        self.assertEqual(result["usage"]["rpc_batch_attempts"], 5)
        self.assertTrue(result["request_file_reopened_unchanged"])
        self.assertTrue(result["source_reopened_unchanged"])
        saved = json.loads((self.output / "RESULT.json").read_text())
        self.assertEqual(saved, result)
        chunks = [pq.read_table(self.output / item["payload"]["path"]).to_pylist()
                  for item in result["chunk_bindings"]]
        self.assertEqual([row["block_number"] for chunk in chunks for row in chunk], list(range(10, 23)))
        self.assertEqual([len(chunk) for chunk in chunks], [10, 3])
        visible = "".join(path.read_text() for path in self.output.glob("*.json"))
        for secret in ("private-endpoint-value", "https://private.invalid/secret"):
            self.assertNotIn(secret, visible)
        self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), pilot.MAX_OUTPUT_BYTES)

    def test_singleton_reopen_and_equal_timestamps_are_valid(self):
        self.write_keys([{"block_number": 20}])
        result = self.execute()
        self.assertEqual(result["accepted_keys"], 1)
        self.assertEqual(len(self.rpc.calls[-1]), 1)
        self.assertEqual(result["status"], "literal_header_pilot_complete")

    def test_equal_timestamp_steps_and_noncontiguous_hashes_are_allowed(self):
        def change(result, payload, call):
            if type(result) is list:
                for item in result:
                    item["result"]["timestamp"] = "0x100"
        self.assertEqual(self.execute(FakeRPC(change))["status"], "literal_header_pilot_complete")

    def test_parquet_input_has_same_literal_key_contract(self):
        self.request_file = self.root / "keys.parquet"
        pq.write_table(pa.table({"block_number": pa.array([10, 11], type=pa.int64()),
                                "block_hash": [block_hash(10), None]}), self.request_file)
        self.request_sha = hashlib.sha256(self.request_file.read_bytes()).hexdigest()
        self.assertEqual(self.execute()["accepted_keys"], 2)

    def test_invalid_request_schema_key_types_duplicates_and_count_fail_before_rpc(self):
        bad = [[], [{"block_number": 0}], [{"block_number": -1}], [{"block_number": True}],
               [{"block_number": "10"}], [{"block_number": 2**63}],
               [{"block_number": 10, "block_hash": "0x1"}],
               [{"block_number": 10, "unknown": "private"}],
               [{"block_number": 10}, {"block_number": 10}],
               [{"block_number": number} for number in range(1, 1002)]]
        for keys in bad:
            with self.subTest(keys=str(keys)[:100]):
                self.write_keys(keys)
                rpc = FakeRPC()
                with self.assertRaises(pilot.PilotBlocked):
                    self.execute(rpc)
                self.assertFalse(self.output.exists())
                self.assertEqual(rpc.calls, [])

    def test_duplicate_json_keys_nonfinite_and_digest_changes_fail_before_output(self):
        for raw in (b'[{"block_number":10,"block_number":20}]', b'[NaN]', b'{broken'):
            with self.subTest(raw=raw):
                self.request_file.write_bytes(raw)
                self.request_sha = hashlib.sha256(raw).hexdigest()
                with self.assertRaises(pilot.PilotBlocked):
                    self.execute()
                self.assertFalse(self.output.exists())
        self.write_keys([{"block_number": 10}])
        self.request_sha = "0" * 64
        with self.assertRaisesRegex(pilot.PilotBlocked, "request_file_sha256_differs"):
            self.execute()

    def test_request_size_symlink_and_bad_parquet_fail_before_network(self):
        with patch.object(pilot, "MAX_REQUEST_FILE_BYTES", 1):
            with self.assertRaisesRegex(pilot.PilotBlocked, "request_file_byte_limit"):
                self.execute()
        target = self.root / "real.json"
        self.request_file.rename(target)
        self.request_file.symlink_to(target)
        with self.assertRaisesRegex(pilot.PilotBlocked, "input_not_regular_file"):
            self.execute()

    def test_parquet_row_count_types_and_expansion_are_bounded(self):
        for table in (pa.table({"block_number": [1.5]}), pa.table({"block_number": [1], "extra": [1]}),
                      pa.table({"block_number": pa.array([], type=pa.int64())}),
                      pa.table({"block_number": [1], "block_hash": pa.array([1], type=pa.int64())})):
            with self.subTest(schema=str(table.schema)):
                self.request_file = self.root / "keys.parquet"
                pq.write_table(table, self.request_file)
                self.request_sha = hashlib.sha256(self.request_file.read_bytes()).hexdigest()
                with self.assertRaises(pilot.PilotBlocked):
                    self.execute()
                self.assertFalse(self.output.exists())

    def test_wrong_chain_rpc_ids_envelopes_and_missing_replies_fail_closed(self):
        for mutation in (lambda value: value.update(result="0x1"),
                         lambda value: value.update(id=True),
                         lambda value: value.update(id=999),
                         lambda value: value.update(jsonrpc="1.0"),
                         lambda value: value.update(error={"message": "private-secret"})):
            with self.subTest(mutation=mutation):
                def change(result, payload, call):
                    if call == 1:
                        mutation(result)
                result = self.execute(FakeRPC(change))
                self.assert_blocked(result)
                self.assertNotIn("private-secret", (self.output / "FAILED.json").read_text())
                self.output = self.root / uuid_name()

    def test_shuffled_batch_missing_duplicate_and_unexpected_id_fail(self):
        for mutation in (lambda values: values.pop(),
                         lambda values: values[0].update(id=values[1]["id"]),
                         lambda values: values[0].update(id=999),
                         lambda values: values[0].update(id=False)):
            with self.subTest(mutation=mutation):
                def change(result, payload, call):
                    if call == 2:
                        mutation(result)
                self.assert_blocked(self.execute(FakeRPC(change)))
                self.output = self.root / uuid_name()

    def test_header_number_hash_parent_timestamp_and_native_hash_must_match(self):
        bad = [("number", "0x99"), ("number", "0x00"), ("timestamp", "0x0"),
               ("timestamp", "0x01"), ("timestamp", hex(253402300800)),
               ("timestamp", 100), ("hash", "bad"), ("parentHash", None)]
        for field, value in bad:
            with self.subTest(field=field, value=value):
                def change(result, payload, call):
                    if call == 2:
                        result[0]["result"][field] = value
                self.assert_blocked(self.execute(FakeRPC(change)))
                self.output = self.root / uuid_name()
        self.write_keys([{"block_number": 10, "block_hash": block_hash(999)}])
        self.assert_blocked(self.execute(), "native_block_hash_differs")

    def test_null_header_and_rpc_error_never_become_missing_row_success(self):
        def change(result, payload, call):
            if call == 2:
                result[0]["result"] = None
        self.assert_blocked(self.execute(FakeRPC(change)), "rpc_header_missing_or_not_object")

    def test_nonmonotonic_and_adjacent_parent_hash_fail_even_across_chunks(self):
        self.write_keys([{"block_number": number} for number in range(10, 22)])
        for field, value in (("timestamp", "0x1"), ("parentHash", block_hash(999))):
            with self.subTest(field=field):
                def change(result, payload, call):
                    if call == 3:
                        for item in result:
                            if item["result"]["number"] == hex(20):
                                item["result"][field] = value
                self.assert_blocked(self.execute(FakeRPC(change)))
                self.output = self.root / uuid_name()

    def test_final_reopened_header_change_blocks_completion(self):
        def change(result, payload, call):
            if call == 4:
                result[0]["result"]["timestamp"] = hex(999_999)
        self.assert_blocked(self.execute(FakeRPC(change)), "reopened_header_changed")

    def test_retryable_http_attempts_are_charged_and_do_not_write_duplicate_headers(self):
        def change(result, payload, call):
            if call == 2:
                return b'{"private":"hidden"}', 503
        result = self.execute(FakeRPC(change))
        self.assertEqual(result["status"], "literal_header_pilot_complete")
        self.assertEqual(result["usage"]["rpc_batch_attempts"], 5)
        self.assertEqual(result["accepted_keys"], 3)
        finish = json.loads((self.output / "rpc_0002.FINISH.json").read_text())
        self.assertEqual(finish["error_code"], "rpc_retryable_http_failure")
        self.assertEqual(finish["charged_response_bytes"], len(b'{"private":"hidden"}'))

    def test_exhausted_transport_failure_preserves_conservative_charges(self):
        calls = []
        def fail(endpoint, payload, *, byte_limit, timeout):
            calls.append(payload)
            raise OSError("https://private.invalid/secret")
        result = self.execute(fail)
        self.assert_blocked(result, "rpc_transport_failure")
        self.assertEqual(len(calls), 3)
        self.assertEqual(result["usage"]["rpc_batch_attempts"], 3)
        self.assertEqual(result["usage"]["charged_response_bytes"], 3 * (pilot.MAX_RESPONSE_BYTES + 1))
        self.assertNotIn("https://private.invalid", (self.output / "FAILED.json").read_text())

    def test_exhausted_http_retries_and_nonretryable_403_fail(self):
        for status, count in ((429, 3), (503, 3), (403, 1)):
            with self.subTest(status=status):
                rpc = FakeRPC(lambda *_: (b'{}', status))
                result = self.execute(rpc)
                self.assert_blocked(result)
                self.assertEqual(len(rpc.calls), count)
                self.output = self.root / uuid_name()

    def test_malformed_rpc_payload_is_not_retried(self):
        rpc = FakeRPC(lambda *_: (b'not JSON', 200))
        self.assert_blocked(self.execute(rpc), "malformed_json")
        self.assertEqual(len(rpc.calls), 1)

    def test_individual_response_and_cumulative_bytes_fail_without_completion(self):
        def oversized(endpoint, payload, *, byte_limit, timeout):
            return b"x" * (byte_limit + 1), 200
        with patch.object(pilot, "MAX_RESPONSE_BYTES", 128):
            result = self.execute(oversized)
            self.assert_blocked(result, "individual_response_byte_limit")
            self.assertEqual(result["usage"]["charged_response_bytes"], 129)
        self.output = self.root / uuid_name()
        with patch.object(pilot, "MAX_TOTAL_RESPONSE_BYTES", 10):
            result = self.execute(oversized)
            self.assert_blocked(result, "individual_response_byte_limit")
            self.assertEqual(result["usage"]["charged_response_bytes"], 10)

    def test_attempt_ceiling_and_runtime_deadline_cannot_publish_success(self):
        with patch.object(pilot, "MAX_RPC_ATTEMPTS", 1):
            result = self.execute()
            self.assert_blocked(result, "rpc_attempt_limit")
            self.assertEqual(result["usage"]["rpc_batch_attempts"], 1)
        self.output = self.root / uuid_name()
        now = [0.0]
        def change(result, payload, call):
            now[0] = 1201.0
        self.assert_blocked(self.execute(FakeRPC(change), clock=lambda: now[0]), "runtime_deadline_exceeded")

    def test_deadline_signal_inside_transport_is_not_swallowed_as_retryable(self):
        calls = []
        def fail(endpoint, payload, **kwargs):
            calls.append(payload)
            raise pilot.PilotBlocked("runtime_deadline_exceeded")
        self.assert_blocked(self.execute(fail), "runtime_deadline_exceeded")
        self.assertEqual(len(calls), 1)

    def test_completed_or_partial_output_cannot_be_reused_or_overwritten(self):
        self.assertEqual(self.execute()["status"], "literal_header_pilot_complete")
        before = {item.name: item.read_bytes() for item in self.output.iterdir()}
        with self.assertRaisesRegex(pilot.PilotBlocked, "output_must_be_new_absolute_directory"):
            self.execute()
        self.assertEqual(before, {item.name: item.read_bytes() for item in self.output.iterdir()})
        self.output = self.root / uuid_name()
        self.output.mkdir()
        (self.output / "partial").write_text("keep")
        with self.assertRaises(pilot.PilotBlocked):
            self.execute()
        self.assertEqual((self.output / "partial").read_text(), "keep")

    def test_changed_request_and_source_at_finalization_fail(self):
        def change(result, payload, call):
            if call == 3:
                self.request_file.write_bytes(self.request_file.read_bytes() + b' ')
        self.assert_blocked(self.execute(FakeRPC(change)), "request_file_sha256_differs")
        self.output = self.root / uuid_name()
        self.write_keys([{"block_number": 10}])
        self.snapshot.side_effect = [copy.deepcopy(SOURCE), {**SOURCE, "head": "c" * 40}]
        self.assert_blocked(self.execute(), "source_changed_during_run")

    def test_private_configuration_identity_is_reopened_without_saving_credential(self):
        def change(result, payload, call):
            if call == 3:
                self.private.write_text("changed-private-endpoint-value")
        result = self.execute(FakeRPC(change))
        self.assert_blocked(result, "rpc_private_file_changed_during_run")
        self.assertNotIn("changed-private-endpoint-value", (self.output / "FAILED.json").read_text())

    def test_changed_payload_receipt_or_rpc_evidence_cannot_be_finalized(self):
        for name in ("headers_0001.parquet", "headers_0001.json", "rpc_0001.START.json",
                     "rpc_0001.FINISH.json", "CONTRACT.json"):
            with self.subTest(name=name):
                def change(result, payload, call):
                    if call == 3:
                        target = self.output / name
                        target.write_bytes(target.read_bytes() + b' ')
                self.assert_blocked(self.execute(FakeRPC(change)))
                self.output = self.root / uuid_name()

    def test_request_file_mutation_with_same_content_identity_change_is_rejected(self):
        def change(result, payload, call):
            if call == 3:
                old = self.request_file.read_bytes()
                replacement = self.root / "replacement"
                replacement.write_bytes(old)
                replacement.replace(self.request_file)
        self.assert_blocked(self.execute(FakeRPC(change)), "request_file_changed_during_run")

    def test_low_disk_admission_happens_before_output_or_rpc(self):
        self.disk.return_value = Disk(100, 1, pilot.FREE_FLOOR_BYTES)
        rpc = FakeRPC()
        with self.assertRaisesRegex(pilot.PilotBlocked, "free_space_floor"):
            self.execute(rpc)
        self.assertFalse(self.output.exists())
        self.assertEqual(rpc.calls, [])

    def test_new_low_disk_and_total_output_cap_preserve_partial_without_result(self):
        def change(result, payload, call):
            if call == 2:
                self.disk.return_value = Disk(100, 1, pilot.FREE_FLOOR_BYTES + 100)
        result = self.execute(FakeRPC(change))
        self.assertEqual(result["status"], "blocked")
        self.assertFalse((self.output / "RESULT.json").exists())
        self.output = self.root / uuid_name()
        self.disk.return_value = Disk(100_000_000_000, 0, 100_000_000_000)
        with patch.object(pilot, "MAX_OUTPUT_BYTES", pilot.TERMINAL_RESERVE + 100):
            self.assert_blocked(self.execute(), "output_byte_limit")

    def test_all_retained_and_pending_bytes_are_counted(self):
        self.output.mkdir()
        budget = pilot.Budget(self.output)
        (self.output / ".old.pending").write_bytes(b"x" * 100)
        (self.output / "failed.json").write_bytes(b"y" * 50)
        self.assertEqual(budget.retained_bytes(), 150)
        with patch.object(pilot, "MAX_OUTPUT_BYTES", pilot.TERMINAL_RESERVE + 200):
            with self.assertRaisesRegex(pilot.PilotBlocked, "output_byte_limit"):
                budget.publish("next.json", b"z" * 26)
        self.assertFalse((self.output / "next.json").exists())

    def test_output_symlinks_refused_and_exclusive_file_publication(self):
        self.output.mkdir()
        budget = pilot.Budget(self.output)
        budget.publish("first.json", b"{}")
        with self.assertRaisesRegex(pilot.PilotBlocked, "output_already_exists_or_nested"):
            budget.publish("first.json", b"[]")
        (self.output / "link").symlink_to(self.request_file)
        with self.assertRaisesRegex(pilot.PilotBlocked, "unexpected_output_symlink"):
            budget.check_space()

    def test_http_reader_uses_bounded_cap_plus_one(self):
        class Response:
            status = 200
            def __enter__(self):
                return self
            def __exit__(self, *_):
                pass
            def read(self, size):
                self.size = size
                return b"{}"
        response = Response()
        with patch.object(pilot.urllib.request, "urlopen", return_value=response):
            raw, status = pilot.http_response("https://example.invalid", {}, byte_limit=100, timeout=1)
        self.assertEqual((raw, status, response.size), (b"{}", 200, 101))

    def test_guard_precedes_request_reads_endpoint_load_or_output(self):
        argv = ["--request-file", str(self.request_file), "--request-sha256", self.request_sha,
                "--expected-head", EXPECTED_HEAD, "--output-dir", str(self.output)]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("not production")), \
                patch.object(pilot, "read_requests") as reads, redirect_stderr(io.StringIO()), \
                self.assertRaises(SystemExit) as stopped:
            pilot.main(argv)
        self.assertEqual(stopped.exception.code, 2)
        reads.assert_not_called()
        self.endpoint.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_source_verifier_rejects_invalid_head_dirty_tree_and_uncommitted_dependencies(self):
        self.source_patch.stop()
        for state in ({"status": "unknown"}, {"status": "inspected", "head": EXPECTED_HEAD,
                                              "tracked_worktree_clean": False},
                      {"status": "inspected", "head": "b" * 40, "tracked_worktree_clean": True}):
            with self.subTest(state=state), patch.object(pilot.inventory, "code_state", return_value=state):
                with self.assertRaisesRegex(pilot.PilotBlocked, "source_head_or_clean_state_differs"):
                    pilot.source_snapshot(EXPECTED_HEAD)
        with self.assertRaisesRegex(pilot.PilotBlocked, "invalid_expected_head"):
            pilot.source_snapshot("not a head")
        with patch.object(pilot.inventory, "code_state", return_value={"status": "inspected", "head": EXPECTED_HEAD,
                                                                    "tracked_worktree_clean": True}), \
                patch.object(pilot.subprocess, "run") as command:
            command.return_value.returncode = 1
            command.return_value.stdout = b""
            with self.assertRaisesRegex(pilot.PilotBlocked, "source_dependency_uncommitted_or_changed"):
                pilot.source_snapshot(EXPECTED_HEAD)


def uuid_name():
    import uuid
    return "pilot_" + uuid.uuid4().hex


if __name__ == "__main__":
    unittest.main()
