"""Synthetic compact scale-pilot tests. No network, real data or EC2 access."""
from __future__ import annotations

import copy
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import collect_polymarket_exact_header_campaign_v2 as scale

HEAD = "a" * 40
SOURCE = {"head": HEAD, "files": {name: "b" * 64 for name in scale.SOURCE_FILES}}
OLD_SOURCE = {"head": scale.ORIGINAL_HEAD, "tracked_worktree_clean": True,
              "files": {name: "c" * 64 for name in scale.OLD_SOURCE_FILES}}
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def block_hash(number):
    return "0x" + f"{number:064x}"


def header(number):
    return {"number": hex(number), "hash": block_hash(number), "parentHash": block_hash(number - 1),
            "timestamp": hex(1000 + number), "transactions": ["NOT_RETAINED_TRANSACTION_HASH"]}


def reply(raw, status=200, **changes):
    return {"raw": raw, "status": status, "error_code": None, "charged_bytes": len(raw),
            "complete": True, "proven_oversize": False, **changes}


class FakeRPC:
    def __init__(self, change=None):
        self.calls, self.change = [], change

    def __call__(self, endpoint, payload, *, byte_limit, timeout):
        self.calls.append(copy.deepcopy(payload))
        requests = payload if type(payload) is list else [payload]
        values = [{"jsonrpc": "2.0", "id": item["id"], "result": "0x89" if item["method"] == "eth_chainId"
                   else header(int(item["params"][0], 16))} for item in requests]
        values = list(reversed(values)) if type(payload) is list else values[0]
        if self.change:
            changed = self.change(values, payload, len(self.calls), byte_limit)
            if changed is not None:
                if isinstance(changed, BaseException):
                    raise changed
                return changed
        return reply(json.dumps(values).encode())


class Response:
    def __init__(self, raw, *, status=200, headers=None):
        self.raw, self.status, self.closed = raw, status, False
        self.headers = {"Content-Length": str(len(raw))}
        self.headers.update(headers or {})
        self.stream, self.read_bytes = io.BytesIO(raw), 0

    def geturl(self):
        return "https://private.invalid/PRIVATE_SECRET"

    def read(self, size):
        raw = self.stream.read(size); self.read_bytes += len(raw)
        return raw

    def close(self):
        self.closed = True


class ScalePilotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.keys = self.root / "original.json"
        values = [{"block_number": number, "block_hash": block_hash(number)} for number in range(1, 1000)]
        values.append({"block_number": 2000, "block_hash": block_hash(2000)})
        self.keys.write_bytes(json.dumps(values).encode())
        self.key_sha = scale.digest(self.keys.read_bytes())
        self.private = self.root / "private"; self.private.write_text("PRIVATE_SECRET"); self.private.chmod(0o600)
        self.old = self.root / "old"
        self.output = self.root / "scale"
        self.ledger_path = self.root / "ledger.json"
        self.source = patch.object(scale, "source_snapshot", return_value=copy.deepcopy(SOURCE)).start()
        self.addCleanup(patch.stopall)
        patch.object(scale, "committed_prior_source").start()
        patch.object(scale.shutil, "disk_usage", return_value=FREE).start()
        patch.object(scale.inventory, "load_rpc_endpoint",
                     return_value=("https://private.invalid/PRIVATE_SECRET", "private_file")).start()
        # Build the old format with its real publisher/reconciliation, but only synthetic headers.
        old_rpc = FakeRPC()
        def old_transport(endpoint, payload, *, byte_limit, timeout):
            value = old_rpc(endpoint, payload, byte_limit=byte_limit, timeout=timeout)
            return value["raw"], value["status"]
        with patch.object(scale.pilot, "source_snapshot", return_value=OLD_SOURCE), \
                patch.object(scale.pilot.os, "fsync", return_value=None):
            old_result = scale.pilot.run_pilot(self.keys, self.key_sha, scale.ORIGINAL_HEAD, self.old,
                                             rpc_private_file=self.private, transport=old_transport, clock=lambda: 0)
        self.assertEqual(old_result["status"], "literal_header_pilot_complete")
        self.prior = self.old / "RESULT.json"
        self.prior_sha = scale.digest(self.prior.read_bytes())
        self.old_result = old_result
        self.write_ledger()

    def entry(self, path):
        return {"path": str(path), "bytes": path.stat().st_size, "sha256": scale.digest(path.read_bytes()),
                "identity": scale.file_identity(path)}

    def write_ledger(self, change=None):
        usage = self.old_result["usage"]
        self.ledger = {"schema_version": "polymarket_header_global_ledger_v2", "category_ceiling_bytes": 8_000_000_000,
                       "complete_header_category_roster": True, "baseline_cache_excluded": True,
                       "entries": [self.entry(path) for path in sorted(self.old.iterdir())] + [self.entry(self.keys)],
                       "inherited_rpc": {"result_path": str(self.prior), "result_sha256": self.prior_sha,
                           "http_attempts": usage["rpc_batch_attempts"], "method_items": usage["rpc_method_items"],
                           "response_bytes": usage["charged_response_bytes"], "elapsed_seconds": 8.829112935}}
        if change:
            change(self.ledger)
        self.ledger_path.write_bytes(scale.json_bytes(self.ledger))
        self.ledger_sha = scale.digest(self.ledger_path.read_bytes())

    def execute(self, transport=None, **kwargs):
        self.rpc = transport or FakeRPC()
        return scale.run_scale(self.keys, self.key_sha, self.prior, self.prior_sha, self.ledger_path,
                               self.ledger_sha, HEAD, self.output, rpc_private_file=self.private,
                               transport=self.rpc, **kwargs)

    def read(self, name):
        return json.loads((self.output / name).read_bytes())

    def records(self):
        return [json.loads(raw) for raw in (self.output / "rpc.jsonl").read_bytes().splitlines()]

    def blocked(self, value, code=None):
        self.assertEqual(value["status"], "blocked")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertTrue((self.output / "FAILED.json").exists())
        if code:
            self.assertEqual(value["error_code"], code)

    def fresh(self, suffix):
        self.output = self.root / ("scale_" + str(suffix))

    def test_compact_batch100_sorted_shuffled_replies_prior_match_and_narrow_scope(self):
        result = self.execute()
        self.assertEqual(result["status"], "literal_compact_scale_pilot_complete")
        self.assertEqual(result["accepted_keys"], 1000)
        self.assertEqual(result["usage"]["http_attempts"], 116)
        self.assertEqual(result["usage"]["method_items"], 2008)
        self.assertEqual(result["usage"]["inherited_rpc"], self.ledger["inherited_rpc"])
        self.assertEqual(result["usage"]["external_operational_control_reservation_bytes"], 2 * 1024**2)
        self.assertGreaterEqual(result["usage"]["elapsed_seconds"], 8.829112935)
        self.assertFalse(result["full_campaign_admitted"])
        self.assertFalse(result["final_sample_admitted"])
        self.assertFalse(result["full_campaign_feasibility_demonstrated"])
        table = pq.read_table(self.output / "headers.parquet")
        self.assertEqual(table.schema, pa.schema([("block_number", pa.int64()), ("block_hash", pa.binary(32)),
                                                ("parent_hash", pa.binary(32)), ("timestamp", pa.int64())]))
        self.assertEqual(table.column("block_number").to_pylist(), list(range(1, 1000)) + [2000])
        self.assertTrue(all(len(value) == 32 for value in table.column("block_hash").to_pylist()))
        self.assertEqual([len(value) for value in self.rpc.calls if type(value) is list], [100] * 10 + [2])
        for payload in self.rpc.calls:
            for request in payload if type(payload) is list else [payload]:
                if request["method"] == "eth_getBlockByNumber":
                    self.assertIs(request["params"][1], False)
        saved = self.read("manifest.json")
        self.assertEqual(saved, result)
        self.assertEqual(self.read("COMPLETE.json")["manifest"]["sha256"], scale.digest((self.output / "manifest.json").read_bytes()))
        self.assertEqual(len(self.records()), 26)
        visible = b"".join(path.read_bytes() for path in self.output.iterdir())
        self.assertNotIn(b"PRIVATE_SECRET", visible)
        self.assertNotIn(b"NOT_RETAINED_TRANSACTION_HASH", visible)
        self.assertLess(sum(path.stat().st_size for path in self.output.iterdir()), self.old_result["usage"]["retained_bytes_before_terminal"])
        self.assertEqual(result["headers"]["bytes"], (self.output / "headers.parquet").stat().st_size)
        self.assertLess(result["measured_parquet_bytes_per_key"], 80)

    def test_start_is_durable_before_dispatch_and_every_finish_has_full_and_projection_hash(self):
        underlying = FakeRPC()
        def transport(endpoint, payload, **kwargs):
            records = self.records()
            self.assertEqual(records[-1]["event"], "START")
            return underlying(endpoint, payload, **kwargs)
        self.execute(transport)
        for record in self.records():
            if record["event"] == "FINISH":
                self.assertTrue(record["response_complete"])
                self.assertEqual(record["full_response_sha256"], record["observed_body_sha256"])
                self.assertRegex(record["full_response_sha256"], r"^[0-9a-f]{64}$")
                if "projected_headers" in record:
                    self.assertRegex(record["projection_sha256"], r"^[0-9a-f]{64}$")

    def test_proven_http413_reduces_batch_size_without_raising_again(self):
        def change(values, payload, call, allowance):
            if type(payload) is list and len(payload) > 50:
                return reply(b'{"error":"too large"}', 413)
        result = self.execute(FakeRPC(change))
        self.assertEqual(result["status"], "literal_compact_scale_pilot_complete")
        sizes = [len(value) for value in self.rpc.calls if type(value) is list]
        self.assertEqual(sizes, [100] + [50] * 20 + [2])
        split = [record for record in self.records() if record.get("outcome") == "split_required"]
        self.assertEqual(len(split), 1)
        self.assertEqual(split[0]["charged_response_bytes"], len(b'{"error":"too large"}'))

    def test_explicit_batch_refusal_and_declared_oversize_allow_downward_split(self):
        for index, kind in enumerate(("jsonrpc", "declared")):
            self.fresh(index)
            def change(values, payload, call, allowance):
                if type(payload) is list and len(payload) > 50:
                    if kind == "jsonrpc":
                        return reply(b'{"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"batch size limit exceeded"}}')
                    return reply(b"", error_code="response_declared_oversize", charged_bytes=allowance,
                                 complete=False, proven_oversize=True, declared_content_length=allowance + 1)
            self.assertEqual(self.execute(FakeRPC(change))["status"], "literal_compact_scale_pilot_complete")

    def test_arbitrary_rpc_error_malformed_or_incomplete_reply_never_triggers_split(self):
        cases = [reply(b'{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}'),
                 reply(b'{broken'), reply(b'[]'), reply(b'{}', complete=False),
                 reply(b'{}', error_code="response_close_failure", status=413, complete=False)]
        for index, returned in enumerate(cases):
            self.fresh(index)
            def change(values, payload, call, allowance):
                return returned if call == 2 else None
            self.blocked(self.execute(FakeRPC(change)))
            self.assertEqual(len(self.rpc.calls), 2)
            self.assertFalse(any(row.get("outcome") == "split_required" for row in self.records()))

    def test_integer_parser_resource_refusal_still_saves_finish_and_response_charge(self):
        def change(values, payload, call, allowance):
            if call == 2: return reply(b"[" + b"9" * 5000 + b"]")
        self.blocked(self.execute(FakeRPC(change)), "invalid_json")
        self.assertEqual(self.records()[-1]["event"], "FINISH")
        self.assertEqual(self.records()[-1]["charged_response_bytes"], 5002)

    def test_wrong_chain_id_missing_duplicate_unexpected_and_boolean_reply_ids_fail(self):
        mutations = [lambda values: values.pop(), lambda values: values[0].update(id=values[1]["id"]),
                     lambda values: values[0].update(id=999999), lambda values: values[0].update(id=True)]
        for index, mutate in enumerate(mutations):
            self.fresh(index)
            def change(values, payload, call, allowance):
                if call == 2: mutate(values)
            self.blocked(self.execute(FakeRPC(change)))
        self.fresh("wrongchain")
        def wrong_chain(values, payload, call, allowance):
            if call == 1: values["result"] = "0x1"
        self.blocked(self.execute(FakeRPC(wrong_chain)), "rpc_wrong_chain")

    def test_native_hash_number_parent_timestamp_and_null_header_fail(self):
        cases = [("number", "0x0"), ("number", "0x01"), ("hash", block_hash(9999)),
                 ("parentHash", "bad"), ("timestamp", "0x0"), ("timestamp", hex(253402300800))]
        for index, (field, value) in enumerate(cases):
            self.fresh(index)
            def change(values, payload, call, allowance):
                if call == 2: values[0]["result"][field] = value
            self.blocked(self.execute(FakeRPC(change)))
        self.fresh("null")
        def null(values, payload, call, allowance):
            if call == 2: values[0]["result"] = None
        self.blocked(self.execute(FakeRPC(null)), "rpc_header_missing_or_not_object")

    def test_clock_and_adjacent_links_are_checked_across_batch_boundaries(self):
        for index, (field, value, code) in enumerate((("timestamp", "0x1", "nonmonotone_header_clock"),
                       ("parentHash", block_hash(99999), "adjacent_header_parent_hash_differs"))):
            self.fresh(index)
            def change(values, payload, call, allowance):
                if call == 3:
                    for item in values:
                        if item["result"]["number"] == "0x65": item["result"][field] = value
            self.blocked(self.execute(FakeRPC(change)), code)

    def test_final_boundary_reopen_mutation_fails_before_compact_output(self):
        def change(values, payload, call, allowance):
            if call == 13: values[0]["result"]["timestamp"] = hex(999999)
        self.blocked(self.execute(FakeRPC(change)), "final_header_reopen_differs")
        self.assertFalse((self.output / "headers.parquet").exists())

    def test_retryable_status_and_transport_attempts_remain_cumulatively_charged(self):
        def change(values, payload, call, allowance):
            if call == 2: return reply(b'busy', 503)
            if call == 3: return OSError("PRIVATE_SECRET")
        result = self.execute(FakeRPC(change))
        self.assertEqual(result["status"], "literal_compact_scale_pilot_complete")
        self.assertEqual(result["usage"]["http_attempts"], 118)
        failed = [row for row in self.records() if row.get("outcome") == "failed"]
        self.assertEqual(len(failed), 2)
        self.assertEqual(failed[1]["charged_response_bytes"], scale.CAPS["individual_response_bytes"])
        self.assertIsNone(failed[1]["full_response_sha256"])
        self.assertNotIn(b"PRIVATE_SECRET", (self.output / "rpc.jsonl").read_bytes())

    def test_three_opaque_failures_stop_without_free_retries(self):
        result = self.execute(lambda *args, **kwargs: (_ for _ in ()).throw(OSError("PRIVATE_SECRET")))
        self.blocked(result, "transport_failure")
        self.assertEqual(result["usage"]["http_attempts"], 106)
        self.assertEqual(result["usage"]["method_items"], 1007)
        self.assertEqual(result["usage"]["response_bytes"], self.ledger["inherited_rpc"]["response_bytes"] + 3 * scale.CAPS["individual_response_bytes"])

    def test_prior_payload_and_control_same_size_mutations_during_rpc_block_complete(self):
        for index, name in enumerate(("headers_0001.parquet", "rpc_0001.START.json", "rpc_0001.FINISH.json", "CONTRACT.json")):
            self.fresh(index)
            path = self.old / name; original = path.read_bytes()
            def change(values, payload, call, allowance):
                if call == 2:
                    raw = bytearray(original); raw[-1] ^= 1; path.write_bytes(raw)
            self.blocked(self.execute(FakeRPC(change)), "ledger_entry_identity_or_bytes_differs")
            path.write_bytes(original)
            self.write_ledger()

    def test_ledger_understated_bytes_counter_or_missing_known_artifact_refused_before_rpc(self):
        changes = [lambda value: value["entries"][0].update(bytes=value["entries"][0]["bytes"] - 1),
                   lambda value: value["inherited_rpc"].update(response_bytes=value["inherited_rpc"]["response_bytes"] - 1),
                   lambda value: value["inherited_rpc"].update(method_items=value["inherited_rpc"]["method_items"] - 1),
                   lambda value: value["inherited_rpc"].update(http_attempts=value["inherited_rpc"]["http_attempts"] - 1),
                   lambda value: value["entries"].pop(0)]
        for index, change in enumerate(changes):
            self.fresh(index); self.write_ledger(change)
            rpc = FakeRPC()
            with self.assertRaises(scale.Blocked): self.execute(rpc)
            self.assertEqual(rpc.calls, [])
            self.assertFalse(self.output.exists())

    def test_duplicate_ledger_entry_and_same_inode_alias_are_not_double_counted(self):
        self.write_ledger(lambda value: value["entries"].append(copy.deepcopy(value["entries"][0])))
        with self.assertRaisesRegex(scale.Blocked, "duplicate_ledger_entry"): self.execute()

    def test_prior_durable_counter_mismatch_is_not_accepted_from_result_alone(self):
        value = copy.deepcopy(self.old_result)
        value["usage"]["rpc_method_items"] -= 1
        self.prior.write_bytes(scale.pilot.json_bytes(value)); self.prior_sha = scale.digest(self.prior.read_bytes())
        self.old_result = value; self.write_ledger()
        with self.assertRaisesRegex(scale.Blocked, "prior_durable_counters_differs"): self.execute()

    def mutate_during_rpc(self, path):
        original = path.read_bytes()
        def change(values, payload, call, allowance):
            if call == 2:
                raw = bytearray(original); raw[-1] ^= 1; path.write_bytes(raw)
        return self.execute(FakeRPC(change))

    def test_request_identity_mutation_during_rpc_blocks_complete(self):
        self.blocked(self.mutate_during_rpc(self.keys))

    def test_ledger_mutation_during_rpc_blocks_complete(self):
        self.blocked(self.mutate_during_rpc(self.ledger_path), "input_sha256_differs")

    def test_private_file_mutation_during_rpc_blocks_complete(self):
        self.blocked(self.mutate_during_rpc(self.private), "rpc_private_file_changed")

    def test_new_source_changes_block_publication(self):
        def source(_):
            return SOURCE | {"head": "d" * 40} if (self.output / "headers.parquet").exists() else SOURCE
        self.source.side_effect = source
        self.blocked(self.execute(), "source_changed")

    def test_existing_output_and_private_authority_change_never_overwrite(self):
        self.output.mkdir(); keep = self.output / "keep"; keep.write_bytes(b"original")
        with self.assertRaisesRegex(scale.Blocked, "destination_not_new_literal_directory"): self.execute()
        self.assertEqual(keep.read_bytes(), b"original")
        self.fresh("authority")
        with patch.object(scale.inventory, "load_rpc_endpoint", return_value=("https://other.invalid/secret", "environment")):
            with self.assertRaisesRegex(scale.Blocked, "rpc_authority_differs_from_prior"): self.execute()

    def test_cumulative_http_method_response_and_time_caps_include_inherited_charges(self):
        inherited = self.ledger["inherited_rpc"]
        for index, (key, value, code) in enumerate((("http_attempts", inherited["http_attempts"] + 3, "http_attempt_limit"),
                   ("method_items", inherited["method_items"] + 3, "method_item_limit"),
                   ("response_bytes", inherited["response_bytes"] + 1, "transport_result_contract"))):
            self.fresh(index)
            with patch.dict(scale.CAPS, {key: value}):
                self.blocked(self.execute(), code)
        self.fresh("time")
        calls = iter([0., 1200.])
        self.blocked(self.execute(clock=lambda: next(calls, 1200.)), "runtime_limit")

    def test_global_artifact_reservation_and_disk_floor_are_checked_before_output(self):
        with patch.dict(scale.CAPS, {"new_artifact_bytes": scale.CAPS["global_header_bytes"]}):
            with self.assertRaisesRegex(scale.Blocked, "global_header_reservation_limit"): self.execute()
        with patch.object(scale.shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 1)):
            with self.assertRaisesRegex(scale.Blocked, "disk_floor"): self.execute()
        self.assertFalse(self.output.exists())

    def test_owned_partial_staging_is_counted_and_unknown_files_refused(self):
        self.output.mkdir()
        budget = scale.Budget(self.output, 0, self.ledger["inherited_rpc"])
        pending = self.output / ".manifest.json.pending"; pending.write_bytes(b"partial")
        budget.files[pending.name] = {"bytes": 100, "sha256": "0" * 64}
        self.assertEqual(budget.retained(), 7)
        unknown = self.output / "unknown"; unknown.write_bytes(b"x")
        with self.assertRaisesRegex(scale.Blocked, "unexpected_output_artifact"): budget.retained()

    def test_journal_same_size_mutation_fails_independent_record_reopen(self):
        def change(values, payload, call, allowance):
            if call == 3:
                path = self.output / "rpc.jsonl"; raw = path.read_bytes()
                path.write_bytes(raw.replace(b'"event":"START"', b'"event":"STORT"', 1))
        self.blocked(self.execute(FakeRPC(change)), "journal_records_changed")

    def test_complete_fsync_directory_and_atomic_rename_faults_leave_no_marker(self):
        original_fsync, original_publish = scale.os.fsync, scale.atomic_publish
        for phase in ("file", "directory", "rename"):
            self.fresh(phase)
            injected = False
            def fsync(fd):
                nonlocal injected
                pending = self.output / ".COMPLETE.json.pending"
                if not injected and pending.exists() and ((phase == "file" and os.fstat(fd).st_ino == pending.stat().st_ino)
                                        or (phase == "directory" and stat.S_ISDIR(os.fstat(fd).st_mode))):
                    injected = True
                    raise OSError("injected fsync failure")
                return original_fsync(fd)
            def publish(pending, target):
                if phase == "rename" and target.name == "COMPLETE.json": raise scale.Blocked("atomic_publication_refused")
                return original_publish(pending, target)
            with patch.object(scale.os, "fsync", side_effect=fsync), patch.object(scale, "atomic_publish", side_effect=publish):
                self.blocked(self.execute())
            self.assertTrue((self.output / ".COMPLETE.json.pending").exists())
            self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), scale.CAPS["new_artifact_bytes"])

    def test_pending_marker_mutation_during_directory_fsync_is_not_published(self):
        original = scale.fsync_directory
        def fsync(path):
            pending = path / ".COMPLETE.json.pending"
            if pending.exists():
                raw = bytearray(pending.read_bytes()); raw[-1] ^= 1; pending.write_bytes(raw)
            return original(path)
        with patch.object(scale, "fsync_directory", side_effect=fsync):
            self.blocked(self.execute(), "artifact_reopen_differs")

    def test_compact_payload_same_size_mutation_during_final_source_check_blocks_complete(self):
        changed = False
        def source(_):
            nonlocal changed
            if not changed and (self.output / ".COMPLETE.json.pending").exists():
                path = self.output / "headers.parquet"
                raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
                changed = True
            return SOURCE
        self.source.side_effect = source
        self.blocked(self.execute(), "artifact_reopen_differs")
        self.assertTrue(changed)

    def test_complete_partial_write_flush_and_close_faults_are_never_published(self):
        original = Path.open
        for phase in ("partial", "flush", "close"):
            self.fresh(phase)
            class FaultyWrite:
                def __init__(self, stream): self.stream = stream
                def __enter__(self): self.stream.__enter__(); return self
                def __exit__(self, *args):
                    result = self.stream.__exit__(*args)
                    if phase == "close": raise OSError("injected close")
                    return result
                def write(self, raw): return self.stream.write(raw[:len(raw)//2] if phase == "partial" else raw)
                def flush(self):
                    if phase == "flush": raise OSError("injected flush")
                    return self.stream.flush()
                def __getattr__(self, name): return getattr(self.stream, name)
            def open_path(path, *args, **kwargs):
                stream = original(path, *args, **kwargs)
                return FaultyWrite(stream) if path.name == ".COMPLETE.json.pending" and args == ("xb",) else stream
            with patch.object(Path, "open", open_path): self.blocked(self.execute())
            self.assertGreater((self.output / ".COMPLETE.json.pending").stat().st_size, 0)
            self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()) + scale.EXTERNAL_CONTROL_RESERVE,
                                 scale.CAPS["new_artifact_bytes"])

    def test_completion_atomic_commit_is_last_artifact_io(self):
        original_open, original_publish = Path.open, scale.atomic_publish
        committed = False
        def publish(pending, target):
            nonlocal committed
            result = original_publish(pending, target)
            if target.name == "COMPLETE.json": committed = True
            return result
        def open_path(path, *args, **kwargs):
            if committed: raise AssertionError("artifact I/O after COMPLETE")
            return original_open(path, *args, **kwargs)
        with patch.object(scale, "atomic_publish", side_effect=publish), patch.object(Path, "open", open_path):
            result = self.execute()
        self.assertTrue(committed)
        self.assertEqual(result["status"], "literal_compact_scale_pilot_complete")

    def test_atomic_publication_cannot_replace_competing_marker(self):
        pending, target = self.root / "pending", self.root / "marker"
        pending.write_bytes(b"new"); target.write_bytes(b"old")
        with self.assertRaisesRegex(scale.Blocked, "atomic_publication_refused"): scale.atomic_publish(pending, target)
        self.assertEqual(target.read_bytes(), b"old")
        self.assertEqual(pending.read_bytes(), b"new")

    def test_guard_precedes_every_input_source_limit_and_transport_read(self):
        argv = ["--request-file", "/input", "--prior-result", "/prior", "--ledger", "/ledger",
                "--ledger-sha256", "b" * 64, "--expected-head", HEAD, "--output-dir", "/mnt/data/runs/new"]
        with patch.object(scale, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(scale, "read_ledger") as ledger, patch.object(scale, "source_snapshot") as source, \
                patch.object(scale, "process_limits") as limits, patch.object(scale, "http_response") as transport:
            with self.assertRaisesRegex(RuntimeError, "guard"): scale.main(argv)
        ledger.assert_not_called(); source.assert_not_called(); limits.assert_not_called(); transport.assert_not_called()

    def test_cli_exposes_no_full_campaign_sample_or_cap_overrides(self):
        result = subprocess.run([sys.executable, scale.__file__, "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        for option in ("--full", "--inventory", "--max", "--request-sha256", "--prior-result-sha256", "--resume", "--endpoint"):
            self.assertNotIn(option, result.stdout)


class TransportAndSourceTests(unittest.TestCase):
    def test_prior_source_requires_the_original_epoch_and_exact_git_blobs(self):
        def blob(head, name):
            self.assertEqual(head, scale.ORIGINAL_HEAD)
            return (name + " fixture\n").encode()
        source = {"head": scale.ORIGINAL_HEAD, "tracked_worktree_clean": True,
                  "files": {name: scale.digest(blob(scale.ORIGINAL_HEAD, name)) for name in scale.OLD_SOURCE_FILES}}
        with patch.object(scale, "git_blob", side_effect=blob):
            scale.committed_prior_source(source)
            with self.assertRaisesRegex(scale.Blocked, "prior_source_schema"):
                scale.committed_prior_source(source | {"head": HEAD})
            source["files"][scale.OLD_SOURCE_FILES[0]] = "0" * 64
            with self.assertRaisesRegex(scale.Blocked, "prior_source_blob_differs"):
                scale.committed_prior_source(source)

    def test_bounded_transport_retains_exposed_partial_and_conservatively_charges(self):
        for phase in ("partial", "opaque", "close", "declared"):
            class Fault(Response):
                def read(self, size):
                    if phase == "partial": raise http.client.IncompleteRead(b"abc")
                    if phase == "opaque": raise OSError("PRIVATE_SECRET")
                    return super().read(size)
                def close(self):
                    if phase == "close": raise OSError("PRIVATE_SECRET")
                    return super().close()
            response = Fault(b"abc", headers={"Content-Length": "65"} if phase == "declared" else {})
            with patch.object(scale.request, "build_opener") as build:
                build.return_value.open.return_value = response
                value = scale.http_response(response.geturl(), [], byte_limit=64, timeout=1)
            self.assertEqual(value["charged_bytes"], 64)
            self.assertFalse(value["complete"])
            self.assertNotIn("PRIVATE_SECRET", json.dumps({key: val for key, val in value.items() if key != "raw"}))
            if phase in ("partial", "close"): self.assertEqual(value["raw"], b"abc")
            if phase == "declared":
                self.assertTrue(value["proven_oversize"])
                self.assertEqual(response.read_bytes, 0)

    def test_transport_never_overreads_and_disables_proxy_redirect_and_auth(self):
        response = Response(b"x" * 65); del response.headers["Content-Length"]
        with patch.object(scale.request, "build_opener") as build:
            build.return_value.open.return_value = response
            value = scale.http_response(response.geturl(), [], byte_limit=64, timeout=1)
        self.assertEqual(response.read_bytes, 64)
        self.assertFalse(value["complete"])
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertIsInstance(build.call_args.args[1], scale.NoRedirect)
        req = build.return_value.open.call_args.args[0]
        self.assertNotIn("Authorization", req.headers)
        self.assertNotIn("Cookie", req.headers)

    def test_source_blob_comparison_detects_assume_unchanged_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for name in scale.SOURCE_FILES:
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text("fixture\n")
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", *scale.SOURCE_FILES], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                            "commit", "--quiet", "-m", "Source fixture"], check=True)
            head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            with patch.object(scale, "REPO", root):
                self.assertEqual(scale.source_snapshot(head)["head"], head)
                subprocess.run(["git", "-C", str(root), "update-index", "--assume-unchanged", "production_guard.py"], check=True)
                (root / "production_guard.py").write_text("changed\n")
                with self.assertRaisesRegex(scale.Blocked, "source_bytes_differ_from_commit"): scale.source_snapshot(head)

    def test_address_space_and_alarm_are_fixed_cumulative_and_restored(self):
        with patch.object(scale.resource, "getrlimit", side_effect=[(-1, -1), (scale.CAPS["address_space_bytes"], -1)]), \
                patch.object(scale.resource, "setrlimit") as limits, \
                patch.object(scale.signal, "getsignal", return_value=scale.signal.SIG_DFL), \
                patch.object(scale.signal, "signal") as handlers, patch.object(scale.signal, "setitimer") as timer:
            with scale.process_limits(): pass
        self.assertEqual(limits.call_args_list[0].args[1], (8 * 1024**3, -1))
        self.assertEqual(limits.call_args_list[-1].args[1], (-1, -1))
        self.assertEqual(timer.call_args_list[0].args, (scale.signal.ITIMER_REAL, 1200 - 8.829112935))
        self.assertEqual(timer.call_args_list[-1].args, (scale.signal.ITIMER_REAL, 0))
        self.assertEqual(handlers.call_args_list[-1].args[1], scale.signal.SIG_DFL)


if __name__ == "__main__":
    unittest.main()
