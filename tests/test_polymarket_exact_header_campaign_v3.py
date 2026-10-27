"""Synthetic bounded clocks, admission, durability and continuation fixtures."""
from contextlib import nullcontext
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from scripts import collect_polymarket_exact_header_campaign_v3 as full

HEAD, PRODUCER = "a" * 40, "b" * 40
BOOT = "12345678-1234-4234-8234-123456789abc"
RESOURCES = {"ram_bytes": {"MemAvailable": 10**12}, "disk_bytes": {"free": 10**12}}


def block_hash(number): return "0x" + f"{number:064x}"


def header(number):
    return {"block_number": number, "block_hash": block_hash(number),
            "parent_hash": block_hash(number - 1), "timestamp": 1_700_000_000 + number}


def rpc_header(number):
    item = header(number)
    return {"number": hex(number), "hash": item["block_hash"], "parentHash": item["parent_hash"], "timestamp": hex(item["timestamp"]),
            "transactions": [block_hash(number)] * 20}


class FullCampaignTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.output = self.root / "full"
        self.inv, self.scale = self.root / "inventory", self.root / "scale"
        self.inv.mkdir(); self.scale.mkdir()
        self.private = self.root / "private"; self.private.write_bytes(b"https://rpc.invalid/SECRET_KEY"); self.private.chmod(0o600)
        self.admission_path = self.root / "admission.json"
        self.original = self.root / "original_pilot_keys.json"
        original_bytes = full.encode([{"block_number": n} for n in range(1, 1001)])
        self.original.write_bytes(original_bytes + b" " * (38091 - len(original_bytes)))
        self.original_ref = self.ref(self.original)
        self.tick, self.calls, self.effects = 0.0, [], None
        self.owner_status = "dead"
        self.blobs = {name: (full.REPO / name).read_bytes() for name in full.SOURCE_FILES}
        self.source = {"head": HEAD, "files": {name: {"bytes": len(raw), "sha256": full.digest(raw)} for name, raw in self.blobs.items()}}
        self.K = 250
        self.inputs = self.make_inputs()
        self.admission = {"schema_version": full.SCHEMA, "status": "root_exact_header_campaign_admitted", "caps": full.CAPS,
            "consumer_head": HEAD, "run_dir": str(self.output), "raw_clocks_only": True, "final_sample_admitted": False,
            "inputs": self.inputs, "private_file": self.ref(self.private), "actual_keys": self.K,
            "conservative_new_runtime_seconds": 1000,
            "prior": {"complete_roster": True, "baseline_cache_excluded": True, "roots": [str(self.inv), str(self.scale)],
                "standalone_control_files": [], "original_pilot_input": self.original_ref, "files": [], "rpc": {"http_attempts": 116, "method_items": 2008,
                    "response_bytes": 20_000_000, "elapsed_seconds": 5000}, "campaigns": []}}
        self.refresh_ledger()

    def ref(self, path):
        raw = path.read_bytes(); return {"path": str(path), "bytes": len(raw), "sha256": full.digest(raw)}

    def save(self, path, value): path.write_bytes(full.encode(value)); return self.ref(path)

    def make_inputs(self):
        keys = [{"block_number": n, "expected_block_hash": block_hash(n) if n % 2 else None} for n in range(1, self.K + 1)]
        pq.write_table(pa.Table.from_pylist(keys, schema=pa.schema([("block_number", pa.int64()), ("expected_block_hash", pa.string())])),
                       self.inv / "required_keys.parquet")
        roster = self.save(self.inv / "roster.json", {"schema_version": "polymarket_exact_header_key_roster_v1", "files": []})
        names = ("production_guard.py", "scripts/inventory_polymarket_refresh.py", "scripts/inventory_polymarket_exact_header_keys_v1.py",
                 "tests/test_inventory_polymarket_exact_header_keys_v1.py")
        manifest = {"schema_version": "polymarket_exact_header_key_inventory_v1", "status": "literal_header_key_inventory_complete",
            "source": {"head": PRODUCER, "files": {name: self.source["files"][name] for name in names}},
            "literal_roster_only": True, "final_universe_admitted": False, "required_keys": self.K,
            "failure_counts": {"null_block_rows": 0}, "global_conflicting_hash_blocks": 0, "usage": {"elapsed_seconds": 2},
            "required_keys_parquet": self.ref(self.inv / "required_keys.parquet") | {"path": "required_keys.parquet"},
            "roster": roster}
        im = self.save(self.inv / "manifest.json", manifest)
        ic = self.save(self.inv / "COMPLETE.json", {"schema_version": "polymarket_exact_header_key_inventory_completion_v1",
            "status": manifest["status"], "manifest": im | {"path": "manifest.json"}})
        rows = [header(n) for n in range(1, 1001)]
        (self.scale / "headers.parquet").write_bytes(full.scale.compact_parquet(rows))
        (self.scale / "rpc.jsonl").write_bytes(b"synthetic accepted upstream journal bound by producer\n")
        ct = self.save(self.scale / "CONTRACT.json", {"schema_version": "polymarket_exact_header_scale_contract_v2", "caps": full.scale.CAPS,
            "source": {"head": PRODUCER, "files": {name: self.source["files"][name]["sha256"] for name in full.scale.SOURCE_FILES}},
            "request": full.pilot.read_requests(self.original, self.original_ref["sha256"])[1],
            "rpc_authority_private_file_identity": full.pilot.identity(self.private)})
        sm = self.save(self.scale / "manifest.json", {"schema_version": "polymarket_exact_header_scale_pilot_v2",
            "status": "literal_compact_scale_pilot_complete", "requested_keys": 1000, "accepted_keys": 1000,
            "matches_accepted_prior": True, "missing_keys": 0, "fallback_rows": 0, "exact_key_coverage": True,
            "headers": self.ref(self.scale / "headers.parquet") | {"path": "headers.parquet"},
            "journal": self.ref(self.scale / "rpc.jsonl") | {"path": "rpc.jsonl"}, "contract": ct | {"path": "CONTRACT.json"},
            "header_projection_sha256": full.digest(full.scale.projection_bytes(rows)),
            "usage": {"http_attempts": 116, "method_items": 2008, "response_bytes": 20_000_000, "elapsed_seconds": 10,
                      "inherited_rpc": {"http_attempts": 103, "method_items": 1004, "response_bytes": 11_922_825, "elapsed_seconds": 8.829112935}}})
        sc = self.save(self.scale / "COMPLETE.json", {"schema_version": "polymarket_exact_header_scale_completion_v2",
            "status": "literal_compact_scale_pilot_complete", "manifest": sm | {"path": "manifest.json"}})
        return {"inventory_complete": ic, "inventory_manifest": im, "keys": self.ref(self.inv / "required_keys.parquet"), "roster": roster,
                "scale_complete": sc, "scale_manifest": sm, "scale_contract": ct, "scale_headers": self.ref(self.scale / "headers.parquet"),
                "scale_journal": self.ref(self.scale / "rpc.jsonl")}

    def refresh_ledger(self):
        files = []
        for root in self.admission["prior"]["roots"]:
            for path in sorted(Path(root).rglob("*")):
                if path.is_file(): files.append(self.ref(path) | {"category": "baseline", "chunk": None})
        files.append(self.original_ref | {"category": "baseline", "chunk": None})
        self.admission["prior"]["files"] = files

    def transport(self, endpoint, payload, *, byte_limit, timeout):
        self.calls.append(payload); self.tick += .01
        if self.effects: return self.effects(payload, byte_limit, timeout)
        requests = payload if isinstance(payload, list) else [payload]
        values = [{"jsonrpc": "2.0", "id": item["id"], "result": "0x89" if item["method"] == "eth_chainId"
                   else rpc_header(int(item["params"][0], 16))} for item in requests]
        raw = full.encode(values if isinstance(payload, list) else values[0])
        return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}

    def execute(self, source_effect=None):
        admission = self.save(self.admission_path, self.admission)
        def blob(head, name):
            self.assertIn(head, (HEAD, PRODUCER)); return self.blobs[name]
        with patch.object(full, "RUN_ROOT", self.root), patch.object(full, "ORIGINAL_PILOT_INPUT", self.original_ref), \
                patch.object(full, "SCALE_STDOUT_ECHO", getattr(self, "scale_stdout_echo_ref", full.SCALE_STDOUT_ECHO)), \
                patch.object(full, "ROSTER_STDOUT_ECHO", getattr(self, "roster_stdout_echo_ref", full.ROSTER_STDOUT_ECHO)), \
                patch.object(full, "source_snapshot", side_effect=source_effect, return_value=self.source), \
                patch.object(full.scale, "git_blob", side_effect=blob), patch.object(full.inventory, "resource_state", return_value=RESOURCES), \
                patch.object(full.inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET_KEY", "private_file")), \
                patch.object(full, "owner_identity", return_value={"hostname": "fixture", "boot_id": BOOT, "pid": 123, "birth_ticks": 456}), \
                patch.object(full, "owner_state", side_effect=lambda _: self.owner_status), \
                patch.object(full.time, "monotonic", side_effect=lambda: self.tick), \
                patch.object(full, "request_deadline", return_value=nullcontext()):
            return full.run_campaign(self.admission_path, admission["sha256"], HEAD, self.output, self.private, transport=self.transport)

    def read(self, path): return json.loads((self.output / path).read_bytes())

    def assert_blocked(self, result, code):
        self.assertEqual(result["status"], "blocked"); self.assertEqual(result["error_code"], code)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def prepare_continuation(self, result):
        old_root, old_admission = self.output, self.admission_path
        owner = self.ref(old_root / "OWNER.json")
        times = {}
        for path in old_root.rglob("rpc.jsonl"):
            if path.parent == old_root: continue
            usage, starts, finishes = full.journal_usage(path)
            chunk = str(int(path.relative_to(old_root).parts[1][1:]))
            values = [*starts.values(), *finishes.values()]
            times[chunk] = (max(v["t"] for v in values) - min(v["t"] for v in values)) / 1000 + .001
        duration = max(1, sum(times.values()))
        drain = self.save(self.root / "old_drain.json", {"schema_version": "polymarket_exact_header_drain_v3",
            "run_dir": str(old_root), "owner_sha256": owner["sha256"], "owner_identity_verified_dead": True,
            "no_owned_workers_or_transfers": True, "runtime_charge_seconds": duration, "chunk_runtime_seconds": times})
        prior = self.admission["prior"]
        prior["roots"].append(str(old_root)); prior["campaigns"].append({"run_dir": str(old_root), "drain": drain})
        prior["standalone_control_files"] += [str(old_admission), drain["path"]]
        for path in sorted(old_root.rglob("*")):
            if not path.is_file(): continue
            parts = path.relative_to(old_root).parts
            chunk = int(parts[1][1:]) if parts[0] == "chunks" else None
            category = "accepted" if path.name == "headers.parquet" and (path.parent / "COMPLETE.json").exists() else \
                       "failed" if path.name == "headers.parquet" else "control"
            prior["files"].append(self.ref(path) | {"category": category, "chunk": chunk})
        prior["files"] += [self.ref(old_admission) | {"category": "control", "chunk": None},
                           drain | {"category": "control", "chunk": None}]
        prior["rpc"] = {key: result["usage"][key] for key in ("http_attempts", "method_items", "response_bytes", "elapsed_seconds")}
        prior["rpc"]["elapsed_seconds"] = result["usage"]["prior_rpc"]["elapsed_seconds"] + duration
        self.output = self.root / "continuation"; self.admission["run_dir"] = str(self.output)
        self.admission_path = self.root / "continuation_admission.json"
        self.calls, self.effects = [], None
        return old_root

    def test_full_literal_keys_compact_projection_and_small_durable_journals(self):
        value = self.execute()
        self.assertEqual(value["status"], full.SUCCESS); self.assertEqual(value["accepted_keys"], self.K)
        manifest = self.read("manifest.json")
        self.assertFalse(manifest["final_sample_admitted"]); self.assertFalse(manifest["cryptographic_header_verification"])
        self.assertEqual(manifest["missing_keys"], 0)
        self.assertEqual(len(self.calls), 5)
        self.assertEqual([len(item) for item in self.calls if isinstance(item, list)], [100, 100, 50])
        for path in self.output.rglob("rpc.jsonl"):
            for line in path.read_bytes().splitlines(keepends=True): self.assertLessEqual(len(line), 384)
        self.assertEqual(pq.read_schema(self.output / "chunks/c000000/a01/headers.parquet").field("block_hash").type, pa.binary(32))
        for path in self.output.rglob("*"):
            if path.is_file():
                self.assertNotIn(b"SECRET_KEY", path.read_bytes())
                self.assertNotIn(b'"transactions"', path.read_bytes())

    def test_guard_before_inputs_resource_changes_or_network(self):
        with patch.object(full, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(full, "run_campaign") as run, patch.object(full, "process_limits") as limits, \
                patch.object(Path, "read_bytes") as read:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                full.main(["--admission", "/none", "--admission-sha256", "0" * 64, "--expected-head", HEAD,
                           "--run-dir", "/mnt/data/runs/none", "--rpc-private-file", "/none"])
        run.assert_not_called(); limits.assert_not_called(); read.assert_not_called()

    def test_no_cap_source_concurrency_or_resume_cli_overrides(self):
        result = subprocess.run([sys.executable, full.__file__, "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        for flag in ("--endpoint", "--workers", "--resume", "--max-", "--batch", "--keys"):
            self.assertNotIn(flag, result.stdout)

    def test_root_admission_is_exact_and_output_is_fresh(self):
        self.admission["raw_clocks_only"] = False
        with self.assertRaisesRegex(full.Blocked, "root_admission_not_exact"): self.execute()
        self.assertFalse(self.calls); self.assertFalse(self.output.exists())
        self.admission["raw_clocks_only"] = True
        self.execute()
        with self.assertRaisesRegex(full.Blocked, "destination_not_fresh"): self.execute()

    def test_actual_key_count_and_runtime_feasibility_are_launch_gates(self):
        self.admission["actual_keys"] += 1
        with self.assertRaisesRegex(full.Blocked, "inventory_contract_invalid"): self.execute()
        self.admission["actual_keys"] -= 1; self.admission["conservative_new_runtime_seconds"] = 0
        with self.assertRaisesRegex(full.Blocked, "runtime_feasibility_not_admitted"): self.execute()
        self.assertFalse(self.calls)

    def test_omitted_prior_file_or_inputs_block_before_output(self):
        self.admission["prior"]["files"].pop()
        with self.assertRaisesRegex(full.Blocked, "prior_roster_incomplete"): self.execute()
        self.assertFalse(self.calls)

    def test_wrong_chain_and_native_hash_mismatch_fail_without_chunk_seal(self):
        def wrong(payload, allowance, timeout):
            raw = full.encode({"jsonrpc": "2.0", "id": payload["id"], "result": "0x1"})
            return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}
        self.effects = wrong
        self.assert_blocked(self.execute(), "wrong_chain")

    def test_wrong_hash_and_clock_and_parent_checks(self):
        for index, mode in enumerate(("hash", "number", "timestamp", "parent")):
            self.output = self.root / f"bad_{index}"; self.admission["run_dir"] = str(self.output); self.calls = []
            def effect(payload, allowance, timeout):
                requests = payload if isinstance(payload, list) else [payload]
                values = [{"jsonrpc": "2.0", "id": item["id"], "result": "0x89" if item["method"] == "eth_chainId"
                           else rpc_header(int(item["params"][0], 16))} for item in requests]
                if isinstance(payload, list):
                    field = {"hash": "hash", "number": "number", "timestamp": "timestamp", "parent": "parentHash"}[mode]
                    values[1 if mode == "parent" else 0]["result"][field] = "0x" + "f" * 64 if mode in {"hash", "parent"} else "0x0"
                raw = full.encode(values if isinstance(payload, list) else values[0])
                return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}
            self.effects = effect
            value = self.execute()
            self.assertEqual(value["status"], "blocked"); self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_only_proven_size_refusal_downsplits_without_dropping_keys(self):
        def effect(payload, allowance, timeout):
            if isinstance(payload, list) and len(payload) > 50:
                raw = full.encode({"jsonrpc": "2.0", "id": None, "error": {"code": -32005, "message": "batch size limit exceeded"}})
            else:
                requests = payload if isinstance(payload, list) else [payload]
                rows = [{"jsonrpc": "2.0", "id": item["id"], "result": "0x89" if item["method"] == "eth_chainId"
                         else rpc_header(int(item["params"][0], 16))} for item in requests]
                raw = full.encode(rows if isinstance(payload, list) else rows[0])
            return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}
        self.effects = effect
        value = self.execute(); self.assertEqual(value["status"], full.SUCCESS)
        self.assertEqual([len(item) for item in self.calls if isinstance(item, list)], [100, 50, 50, 50, 50, 50])

    def test_unproven_size_or_malformed_reply_never_downsplits(self):
        def effect(payload, allowance, timeout):
            raw = full.encode({"message": "maybe too large"})
            return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}
        # Fail the initial chain response: it must not be retried or guessed.
        self.effects = effect
        self.assert_blocked(self.execute(), "rpc_reply_shape")
        self.assertEqual(len(self.calls), 1)

    def test_unknown_transport_charges_full_allowance_and_three_attempts_only(self):
        def effect(*args): raise OSError("SECRET_KEY")
        self.effects = effect
        value = self.execute(); self.assert_blocked(value, "transport_failure")
        self.assertEqual(len(self.calls), 3)
        usage, starts, finishes = full.journal_usage(self.output / "rpc.jsonl")
        self.assertEqual(usage["response_bytes"], 3 * 16 * 1024**2)
        self.assertEqual(len(starts), len(finishes))
        self.assertNotIn(b"SECRET_KEY", (self.output / "FAILED.json").read_bytes())

    def test_malformed_transport_keeps_closed_full_charge_receipt(self):
        self.effects = lambda *args: {"raw": b"", "charged_bytes": -1}
        value = self.execute(); self.assert_blocked(value, "transport_contract")
        usage, starts, finishes = full.journal_usage(self.output / "rpc.jsonl")
        self.assertEqual(usage["response_bytes"], 16 * 1024**2); self.assertEqual(len(finishes), 1)

    def test_prior_http_method_and_response_budgets_never_reset(self):
        for index, (key, ceiling, code) in enumerate((("http_attempts", full.CAPS["http_attempts"], "http_attempts_limit"),
                ("method_items", full.CAPS["method_items"], "method_items_limit"),
                ("response_bytes", full.CAPS["response_bytes"], "response_bytes_limit"))):
            self.output = self.root / f"budget_{index}"; self.admission["run_dir"] = str(self.output)
            old = self.admission["prior"]["rpc"][key]; self.admission["prior"]["rpc"][key] = ceiling
            self.assert_blocked(self.execute(), code); self.admission["prior"]["rpc"][key] = old
        self.assertFalse(self.calls)

    def test_same_size_source_admission_private_and_key_mutations_block(self):
        for index, target in enumerate(("admission", "private", "keys")):
            if index: self.setUp()
            self.output = self.root / f"mutated_{index}"; self.admission["run_dir"] = str(self.output); self.calls = []
            path = self.admission_path if target == "admission" else self.private if target == "private" else self.inv / "required_keys.parquet"
            def effect(payload, allowance, timeout):
                self.effects = None; reply = self.transport("fixture", payload, byte_limit=allowance, timeout=timeout); self.effects = effect
                if isinstance(payload, list):
                    raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
                return reply
            self.effects = effect
            value = self.execute(); self.assertEqual(value["status"], "blocked")
            self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_late_prior_inclusive_deadline_blocks_final_marker(self):
        def source(_):
            if (self.output / ".COMPLETE.json.pending").exists(): self.tick = full.CAPS["runtime_seconds"]
            return self.source
        self.assert_blocked(self.execute(source_effect=source), "runtime_limit")

    def test_pending_marker_short_write_and_fsync_failure_do_not_publish(self):
        original = full.os.fsync
        def fsync(fd):
            path = self.output / ".COMPLETE.json.pending"
            if path.exists() and os.fstat(fd).st_ino == path.stat().st_ino: raise OSError("fault")
            return original(fd)
        with patch.object(full.os, "fsync", side_effect=fsync):
            self.assert_blocked(self.execute(), "header_campaign_runtime_failure")

    def test_final_publication_is_last_artifact_io(self):
        published = False; original, publish = Path.open, full.scale.atomic_publish
        def atomic(pending, destination):
            nonlocal published
            value = publish(pending, destination)
            if destination == self.output / "COMPLETE.json": published = True
            return value
        def open_path(path, *args, **kwargs):
            if published: raise AssertionError("post-publication I/O")
            return original(path, *args, **kwargs)
        with patch.object(full.scale, "atomic_publish", side_effect=atomic), patch.object(Path, "open", open_path):
            value = self.execute()
        self.assertTrue(published); self.assertEqual(value["status"], full.SUCCESS)

    def test_unclosed_start_is_full_charged_and_interruption_is_not_completion(self):
        path = self.root / "unclosed.jsonl"
        path.write_bytes(full.encode({"e": "s", "i": 1, "n": 100, "b": 16 * 1024**2, "k": 0, "j": 1, "t": 0, "h": "a" * 64}))
        usage, starts, finishes = full.journal_usage(path)
        self.assertEqual(usage, {"http_attempts": 1, "method_items": 100, "response_bytes": 16 * 1024**2})
        self.assertEqual(finishes, {})
        self.effects = lambda *args: (_ for _ in ()).throw(KeyboardInterrupt())
        with self.assertRaises(KeyboardInterrupt): self.execute()
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertEqual(full.journal_usage(self.output / "rpc.jsonl")[0]["response_bytes"], 16 * 1024**2)

    def test_fresh_continuation_reuses_only_verified_sealed_chunk(self):
        first = self.execute(); self.assertEqual(first["status"], full.SUCCESS)
        old_root = self.prepare_continuation(first)
        hashes = {str(path): self.ref(path)["sha256"] for path in old_root.rglob("*") if path.is_file()}
        second = self.execute(); self.assertEqual(second["status"], full.SUCCESS)
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(all(type(call) is dict and call["method"] == "eth_chainId" for call in self.calls))
        self.assertEqual(hashes, {str(path): self.ref(path)["sha256"] for path in old_root.rglob("*") if path.is_file()})
        self.assertFalse((self.output / "chunks").exists())

    def test_failed_chunk_is_refetched_in_fresh_attempt_not_adopted(self):
        seen = 0
        def effect(payload, allowance, timeout):
            nonlocal seen
            if isinstance(payload, list): seen += 1
            if seen == 2:
                raw = b'{}'
                return {"raw": raw, "charged_bytes": len(raw), "status": 200, "complete": True, "error_code": None, "proven_oversize": False}
            self.effects = None
            reply = self.transport("fixture", payload, byte_limit=allowance, timeout=timeout)
            self.effects = effect
            return reply
        self.effects = effect
        first = self.execute(); self.assert_blocked(first, "rpc_batch_shape")
        old_root = self.prepare_continuation(first)
        self.assertFalse((old_root / "chunks/c000000/a01/COMPLETE.json").exists())
        second = self.execute(); self.assertEqual(second["status"], full.SUCCESS)
        self.assertTrue((self.output / "chunks/c000000/a02/COMPLETE.json").exists())
        self.assertEqual(sum(len(call) for call in self.calls if type(call) is list), self.K)

    def test_live_unknown_or_missing_drain_blocks_before_any_continuation_request(self):
        first = self.execute(); self.prepare_continuation(first)
        for state in ("live", "unknown"):
            self.owner_status = state
            with self.assertRaisesRegex(full.Blocked, "prior_owner_live_or_unknown"): self.execute()
            self.assertFalse(self.calls)
        self.owner_status = "dead"; self.admission["prior"]["campaigns"] = []
        with self.assertRaisesRegex(full.Blocked, "prior_campaign_drain_missing"): self.execute()
        self.assertFalse(self.calls)

    def test_prior_unclosed_start_full_charge_cannot_be_omitted(self):
        first = self.execute(); old_root = self.prepare_continuation(first)
        path = old_root / "rpc.jsonl"
        raw = path.read_bytes() + full.encode({"e": "s", "i": full.CAPS["http_attempts"], "k": -1,
                  "n": 0, "j": 9999, "b": 16 * 1024**2, "t": 100,
                  "h": full.digest(full.encode({"jsonrpc": "2.0", "id": 9999, "method": "eth_chainId", "params": []}))})
        path.write_bytes(raw)
        for index, item in enumerate(self.admission["prior"]["files"]):
            if item["path"] == str(path): self.admission["prior"]["files"][index] = self.ref(path) | {"category": "control", "chunk": None}
        with self.assertRaisesRegex(full.Blocked, "prior_rpc_below_durable_minima"): self.execute()
        self.assertFalse(self.calls)

    def test_minimal_finish_or_unknown_body_refund_cannot_erase_reserved_charge(self):
        path = self.root / "forged.jsonl"
        start = {"e": "s", "i": 1, "n": 0, "b": 16 * 1024**2, "k": -1, "j": 1, "t": 0,
                 "h": full.digest(full.encode({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}))}
        for finish, code in (({"e": "f", "i": 1, "b": 0}, "journal_finish_schema"),
                             ({"e": "f", "i": 1, "b": 0, "z": 0, "h": None, "x": full.digest(b""),
                               "p": None, "r": "failed", "s": None, "t": 1}, "journal_unknown_body_charge_refund")):
            path.write_bytes(full.encode(start) + full.encode(finish))
            with self.assertRaisesRegex(full.Blocked, code): full.journal_usage(path)

    def test_incomplete_adapter_reply_is_conservatively_charged_before_receipt(self):
        prefix = b"observed-prefix"
        self.effects = lambda *args: {"raw": prefix, "charged_bytes": len(prefix), "status": 200,
                                     "complete": False, "error_code": "response_incomplete", "proven_oversize": False}
        value = self.execute(); self.assert_blocked(value, "response_incomplete")
        usage, starts, finishes = full.journal_usage(self.output / "rpc.jsonl")
        self.assertEqual(usage["response_bytes"], 3 * 16 * 1024**2)
        self.assertEqual(next(iter(finishes.values()))["z"], len(prefix))

    def test_legacy_crashed_attempt_root_without_summary_blocks(self):
        root = self.root / "legacy_crashed"; root.mkdir()
        self.save(root / "rpc_0001.START.json", {"call": 1, "requests": [{"jsonrpc": "2.0", "id": 1,
            "method": "eth_getBlockByNumber", "params": ["0x1", False]}], "response_byte_limit": 16 * 1024**2})
        self.admission["prior"]["roots"].append(str(root)); self.refresh_ledger()
        with self.assertRaisesRegex(full.Blocked, "prior_rpc_summary_missing"): self.execute()
        self.assertFalse(self.calls)

    def test_chunk_deadline_after_independent_verify_is_not_cleared_or_promoted(self):
        original = full.verify_chunk
        def verify(root, keys, offset):
            value = original(root, keys, offset)
            if self.output in root.parents: self.tick = 3601
            return value
        with patch.object(full, "verify_chunk", side_effect=verify):
            self.assert_blocked(self.execute(), "chunk_runtime_limit")

    def test_prior_owner_must_bind_same_inventory_scale_and_caps(self):
        first = self.execute(); old = self.prepare_continuation(first)
        path = old / "OWNER.json"; value = json.loads(path.read_bytes())
        value["inputs"]["keys"]["sha256"] = "f" * 64
        updated = self.save(path, value)
        for index, item in enumerate(self.admission["prior"]["files"]):
            if item["path"] == str(path): self.admission["prior"]["files"][index] = updated | {"category": "control", "chunk": None}
        with self.assertRaisesRegex(full.Blocked, "prior_owner_schema_or_inputs"): self.execute()
        self.assertFalse(self.calls)

    def test_prior_chunk_drain_must_cover_manifest_validation_time_not_only_http_span(self):
        first = self.execute(); old = self.prepare_continuation(first)
        path = old / "chunks/c000000/a01/manifest.json"; value = json.loads(path.read_bytes())
        value["active_seconds_charged"] = 10
        updated = self.save(path, value)
        for index, item in enumerate(self.admission["prior"]["files"]):
            if item["path"] == str(path): self.admission["prior"]["files"][index] = updated | {"category": "control", "chunk": 0}
        with self.assertRaisesRegex(full.Blocked, "prior_chunk_drain_time_understated"): self.execute()
        self.assertFalse(self.calls)

    def test_prior_root_cannot_grow_unrostered_files_during_campaign(self):
        def source(_):
            if (self.output / ".COMPLETE.json.pending").exists():
                (self.inv / "late_unrostered.json").write_bytes(b'{}')
            return self.source
        self.assert_blocked(self.execute(source_effect=source), "prior_roster_incomplete")

    def test_partial_response_interruption_records_observed_body_digest_and_full_charge(self):
        prefix = b"observed-prefix"
        self.effects = lambda *args: {"raw": prefix, "charged_bytes": 16 * 1024**2, "status": 200,
                                     "complete": False, "error_code": "interrupted", "proven_oversize": False}
        with self.assertRaises(KeyboardInterrupt): self.execute()
        finish = json.loads((self.output / "rpc.jsonl").read_bytes().splitlines()[-1])
        self.assertEqual(finish["x"], full.digest(prefix)); self.assertEqual(finish["z"], len(prefix))
        self.assertIsNone(finish["h"]); self.assertEqual(finish["b"], 16 * 1024**2)

    def test_suballocations_and_external_reserve_cannot_borrow_from_each_other(self):
        self.output.mkdir()
        prior = {"bytes": dict.fromkeys(("baseline", "accepted", "control", "failed"), 0), "files": {},
                 "rpc": {"http_attempts": 0, "method_items": 0, "response_bytes": 0, "elapsed_seconds": 0}}
        budget = full.Budget(self.output, prior, 0, 0)
        with patch.object(full.inventory, "resource_state", return_value=RESOURCES), patch.object(full.time, "monotonic", return_value=0):
            for category in ("baseline", "accepted", "control", "failed"):
                prior["bytes"][category] = full.CAPS[category + "_bytes"]
                with self.assertRaisesRegex(full.Blocked, "header_disk_allocation"): budget.check(1, category)
                prior["bytes"][category] = 0
            prior["bytes"]["control"] = full.CAPS["control_bytes"] - full.CAPS["external_control_reservation_bytes"]
            with self.assertRaisesRegex(full.Blocked, "header_disk_allocation"): budget.check(1, failure=True)

    def test_all_logical_chunk_resource_caps_apply_before_dispatch(self):
        self.output.mkdir()
        prior = {"bytes": dict.fromkeys(("baseline", "accepted", "control", "failed"), 0), "files": {},
                 "rpc": {"http_attempts": 0, "method_items": 0, "response_bytes": 0, "elapsed_seconds": 0}}
        for key, cap, code in (("http_attempts", "chunk_http_attempts", "chunk_http_attempts_limit"),
                               ("method_items", "chunk_method_items", "chunk_method_items_limit"),
                               ("response_bytes", "chunk_response_bytes", "response_bytes_limit"),
                               ("elapsed_seconds", "chunk_runtime_seconds", "chunk_runtime_limit")):
            budget = full.Budget(self.output, prior, 0, 0)
            budget.chunk, budget.chunk_started = 0, 0
            budget.chunk_base = dict.fromkeys(prior["rpc"], 0); budget.chunk_base[key] = full.CAPS[cap]
            budget.chunk_start_rpc = dict(budget.rpc)
            with patch.object(full.inventory, "resource_state", return_value=RESOURCES), patch.object(full.time, "monotonic", return_value=0):
                with self.assertRaisesRegex(full.Blocked, code):
                    full.RPC("private", budget, self.transport, "chunks/c000000/a01/rpc.jsonl").call([{ "block_number": 1, "block_hash": None}])
        self.assertFalse(self.calls)

    def test_missing_chunk_time_drain_proof_is_not_a_zero_runtime_resume(self):
        first = self.execute(); self.prepare_continuation(first)
        campaign = self.admission["prior"]["campaigns"][0]
        path = Path(campaign["drain"]["path"]); value = json.loads(path.read_bytes())
        value["chunk_runtime_seconds"] = {}
        new = self.save(path, value); campaign["drain"] = new
        for index, item in enumerate(self.admission["prior"]["files"]):
            if item["path"] == str(path): self.admission["prior"]["files"][index] = new | {"category": "control", "chunk": None}
        with self.assertRaisesRegex(full.Blocked, "prior_chunk_drain_time_understated"): self.execute()
        self.assertFalse(self.calls)

    def test_prior_full_campaign_headers_cannot_be_reclassified_into_baseline(self):
        first = self.execute(); self.prepare_continuation(first)
        item = next(item for item in self.admission["prior"]["files"] if item["category"] == "accepted")
        item["category"] = "baseline"
        with self.assertRaisesRegex(full.Blocked, "prior_chunk_path_or_category"): self.execute()
        self.assertFalse(self.calls)

    def test_schema_or_helper_drift_is_not_accepted_upstream_evidence(self):
        path = self.scale / "CONTRACT.json"; value = json.loads(path.read_bytes())
        value["source"]["files"]["production_guard.py"] = "f" * 64
        updated = self.save(path, value)
        self.inputs["scale_contract"] = updated
        sm_path = self.scale / "manifest.json"; sm = json.loads(sm_path.read_bytes()); sm["contract"] = updated | {"path": "CONTRACT.json"}
        smb = self.save(sm_path, sm); self.inputs["scale_manifest"] = smb
        sc = self.save(self.scale / "COMPLETE.json", {"schema_version": "polymarket_exact_header_scale_completion_v2",
            "status": sm["status"], "manifest": smb | {"path": "manifest.json"}})
        self.inputs["scale_complete"] = sc; self.refresh_ledger()
        with self.assertRaisesRegex(full.Blocked, "producer_consumer_source_drift"): self.execute()
        self.assertFalse(self.calls)

    def test_late_chunk_and_control_mutations_cannot_seal_campaign(self):
        original = full.Budget.seal
        for index, target in enumerate(("headers", "journal", "manifest")):
            if index: self.setUp()
            def seal(budget, name, value, external):
                if name == "COMPLETE.json":
                    path = budget.output / ("chunks/c000000/a01/headers.parquet" if target == "headers" else
                                             "chunks/c000000/a01/rpc.jsonl" if target == "journal" else "manifest.json")
                    raw = bytearray(path.read_bytes()); raw[-1] ^= 1; path.write_bytes(raw)
                return original(budget, name, value, external)
            with patch.object(full.Budget, "seal", seal):
                value = self.execute()
            self.assertEqual(value["status"], "blocked"); self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_marker_short_flush_close_and_directory_fsync_faults_never_publish(self):
        original_open, original_fsync = Path.open, full.os.fsync
        for index, mode in enumerate(("short", "flush", "close", "directory")):
            if index: self.setUp()
            class Marker:
                def __init__(self, stream): self.stream = stream
                def __enter__(self): self.stream.__enter__(); return self
                def __exit__(self, *args):
                    result = self.stream.__exit__(*args)
                    if mode == "close": raise OSError("close fault")
                    return result
                def write(self, raw): return self.stream.write(raw[:len(raw)//2] if mode == "short" else raw)
                def flush(self):
                    if mode == "flush": raise OSError("flush fault")
                    return self.stream.flush()
                def __getattr__(self, name): return getattr(self.stream, name)
            def open_path(path, *args, **kwargs):
                stream = original_open(path, *args, **kwargs)
                return Marker(stream) if path == self.output / ".COMPLETE.json.pending" and args == ("xb",) else stream
            def fsync(fd):
                if mode == "directory" and (self.output / ".COMPLETE.json.pending").exists() and stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise OSError("directory fault")
                return original_fsync(fd)
            with patch.object(Path, "open", open_path), patch.object(full.os, "fsync", side_effect=fsync):
                value = self.execute()
            self.assertEqual(value["status"], "blocked"); self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_request_deadline_sets_and_restores_remaining_outer_timer(self):
        with patch.object(full.signal, "getsignal", return_value=None), patch.object(full.signal, "signal"), \
                patch.object(full.signal, "getitimer", return_value=(100, 0)), patch.object(full.signal, "setitimer") as timer, \
                patch.object(full.time, "monotonic", side_effect=[10, 15]):
            with full.request_deadline(30): pass
        self.assertEqual(timer.call_args_list[0].args, (signal.ITIMER_REAL, 30))
        self.assertEqual(timer.call_args_list[-1].args, (signal.ITIMER_REAL, 95, 0))

    def test_real_committed_source_reopen_checks_own_and_helper_bytes(self):
        repo = self.root / "source"; repo.mkdir()
        for name in full.SOURCE_FILES:
            path = repo / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b"fixture\n")
        subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "add", *full.SOURCE_FILES], check=True)
        subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--quiet", "-m", "Fixture"], check=True)
        head = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
        with patch.object(full, "REPO", repo), patch.object(full.scale, "REPO", repo):
            self.assertEqual(set(full.source_snapshot(head)["files"]), set(full.SOURCE_FILES))
            name = "scripts/collect_polymarket_exact_header_campaign_v3.py"
            subprocess.run(["git", "-C", str(repo), "update-index", "--assume-unchanged", name], check=True)
            (repo / name).write_bytes(b"changed\n")
            with self.assertRaisesRegex(full.Blocked, "source_blob_differs"): full.source_snapshot(head)

    def test_owner_identity_uses_birth_and_boot_not_pid_alone(self):
        owner = {"hostname": "fixture", "boot_id": BOOT, "pid": 123, "birth_ticks": 456}
        def read(path, *args, **kwargs):
            if str(path).endswith("boot_id"): return BOOT + "\n"
            return "123 (fixture name) " + " ".join(["S", *(["0"] * 18), "456"])
        with patch.object(full.socket, "gethostname", return_value="fixture"), patch.object(Path, "read_text", read):
            self.assertEqual(full.owner_state(owner), "live")
            self.assertEqual(full.owner_state(owner | {"birth_ticks": 789}), "dead")
            self.assertEqual(full.owner_state(owner | {"boot_id": "22345678-1234-4234-8234-123456789abc"}), "dead")
            self.assertEqual(full.owner_state(owner | {"hostname": "another-host"}), "unknown")
        with patch.object(full.socket, "gethostname", return_value="fixture"), patch.object(Path, "read_text", side_effect=PermissionError()):
            self.assertEqual(full.owner_state(owner), "unknown")

    def test_malformed_saved_owner_is_unknown_before_any_boot_or_pid_decision(self):
        owner = {"hostname": "fixture", "boot_id": BOOT, "pid": 123, "birth_ticks": 456}
        malformed = [owner | {"boot_id": "not-a-valid-boot-id", "pid": False, "birth_ticks": None},
                     owner | {"pid": False}, owner | {"pid": 0}, owner | {"birth_ticks": True},
                     owner | {"birth_ticks": 0}, owner | {"birth_ticks": None}, owner | {"hostname": ""},
                     owner | {"hostname": " "}, owner | {"extra": 1}, None,
                     owner | {"boot_id": BOOT.upper()}, owner | {"boot_id": "00000000-0000-0000-0000-000000000000"}]
        with patch.object(full.socket, "gethostname", return_value="fixture"), patch.object(Path, "read_text") as read:
            for value in malformed: self.assertEqual(full.owner_state(value), "unknown")
        read.assert_not_called()

    def test_whitelisted_transient_read_failures_retry_exact_batch_then_recover(self):
        for index, code in enumerate(("response_incomplete", "content_length_differs", "response_close_failure")):
            if index: self.setUp()
            failed, retried = False, None
            def effect(payload, allowance, timeout):
                nonlocal failed, retried
                if isinstance(payload, list) and not failed:
                    failed, retried = True, payload
                    return {"raw": b"observed-prefix", "charged_bytes": allowance, "complete": False,
                            "status": 200, "error_code": code, "proven_oversize": False}
                self.effects = None
                result = self.transport("fixture", payload, byte_limit=allowance, timeout=timeout)
                self.effects = effect
                return result
            self.effects = effect
            value = self.execute(); self.assertEqual(value["status"], full.SUCCESS)
            requests = [call for call in self.calls if type(call) is list]
            self.assertEqual([r["params"] for r in requests[0]], [r["params"] for r in requests[1]])
            self.assertEqual(len(requests[0]), 100)
            usage, starts, finishes = full.journal_usage(self.output / "chunks/c000000/a01/rpc.jsonl")
            first = next(iter(finishes.values()))
            self.assertEqual(first["b"], 16 * 1024**2); self.assertEqual(first["x"], full.digest(b"observed-prefix"))
            self.assertIsNone(first["h"]); self.assertEqual(first["r"], "failed")

    def test_whitelisted_transient_read_failures_exhaust_three_same_batch_attempts(self):
        prefix = b"observed-prefix"
        self.effects = lambda *args: {"raw": prefix, "charged_bytes": len(prefix), "complete": False,
            "status": 200, "error_code": "response_close_failure", "proven_oversize": False}
        self.assert_blocked(self.execute(), "response_close_failure")
        self.assertEqual(len(self.calls), 3)
        usage, starts, finishes = full.journal_usage(self.output / "rpc.jsonl")
        self.assertEqual(usage["http_attempts"], 3); self.assertEqual(usage["method_items"], 3)
        self.assertEqual(usage["response_bytes"], 3 * 16 * 1024**2)
        self.assertEqual(len({item["k"] for item in starts.values()}), 1)
        self.assertEqual(len(finishes), 3)

    def test_standalone_controls_cannot_bypass_live_owner_and_campaign_charges(self):
        first = self.execute(); old = self.prepare_continuation(first)
        prior = self.admission["prior"]
        prior["roots"].remove(str(old)); prior["campaigns"] = []
        for item in prior["files"]:
            if old in Path(item["path"]).parents:
                prior["standalone_control_files"].append(item["path"])
                item.update(category="control", chunk=None)
        self.owner_status = "live"
        with self.assertRaisesRegex(full.Blocked, "standalone_protocol_artifact_forbidden"): self.execute()
        self.assertFalse(self.calls)

    def test_known_sealed_roster_profile_is_bound_zero_rpc_baseline_not_a_bypass(self):
        root = self.root / "roster_builder"; root.mkdir()
        source = {"head": PRODUCER, "files": {name: self.source["files"][name]["sha256"] for name in full.roster_profile.SOURCE_FILES}}
        start = {"schema_version": "polymarket_exact_header_roster_build_v1", "source": source, "caps": full.roster_profile.CAPS,
                 "status": "blocked", "literal_source_generation_only": True, "final_universe_admitted": False, "analysis_admitted": False}
        self.save(root / "START.json", start)
        roster = self.save(root / "roster.json", {"schema_version": "polymarket_exact_header_key_roster_v1", "files": [{"source_role": "legacy_raw"}]})
        manifest = start | {"status": "sealed_source_roster_complete_not_inventory_admission", "roster": roster | {"path": "roster.json"}, "source_files": 1}
        mb = self.save(root / "manifest.json", manifest)
        self.save(root / "COMPLETE.json", {"status": manifest["status"], "manifest": mb | {"path": "manifest.json"},
                                          "final_universe_admitted": False, "analysis_admitted": False})
        self.admission["prior"]["roots"].append(str(root)); self.refresh_ledger()
        self.assertEqual(self.execute()["status"], full.SUCCESS)
        self.output = self.root / "roster_budget"; self.admission["run_dir"] = str(self.output); self.calls = []
        self.admission["prior"]["rpc"]["elapsed_seconds"] = 4200
        with self.assertRaisesRegex(full.Blocked, "prior_rpc_below_durable_minima"): self.execute()
        self.assertFalse(self.calls)

    def test_unsealed_or_extra_file_roster_profile_is_not_assumed_zero_rpc(self):
        root = self.root / "unsealed_roster"; root.mkdir()
        self.save(root / "manifest.json", {"schema_version": "polymarket_exact_header_roster_build_v1", "status": "blocked"})
        self.admission["prior"]["roots"].append(str(root)); self.refresh_ledger()
        with self.assertRaisesRegex(full.Blocked, "roster_profile_not_sealed"): self.execute()
        self.assertFalse(self.calls)

    def test_renamed_collection_evidence_is_not_an_operational_control(self):
        bodies = [full.encode({"schema_version": "polymarket_exact_header_owner_v3"}),
                  full.encode({"e": "s", "i": 1, "n": 100}) + full.encode({"e": "f", "i": 1}),
                  b"PAR1not-a-control", full.encode([{"block_number": 1, "block_hash": None}])]
        for index, raw in enumerate(bodies):
            path = self.root / f"renamed_{index}.log"; path.write_bytes(raw)
            self.admission["prior"]["standalone_control_files"].append(str(path))
            self.admission["prior"]["files"].append(self.ref(path) | {"category": "control", "chunk": None})
            with self.assertRaisesRegex(full.Blocked, "standalone_protocol_artifact_forbidden"): self.execute()
            self.admission["prior"]["standalone_control_files"].pop(); self.admission["prior"]["files"].pop()
        self.assertFalse(self.calls)

    def test_original_loose_pilot_input_has_one_exact_baseline_only_allowance(self):
        self.admission["prior"]["original_pilot_input"] = self.original_ref | {"bytes": 38090}
        with self.assertRaisesRegex(full.Blocked, "original_pilot_input_not_pinned"): self.execute()
        self.admission["prior"]["original_pilot_input"] = self.original_ref
        item = next(item for item in self.admission["prior"]["files"] if item["path"] == str(self.original))
        item["category"] = "control"
        with self.assertRaisesRegex(full.Blocked, "original_pilot_input_misclassified"): self.execute()
        self.assertFalse(self.calls)

    def test_original_loose_input_must_match_saved_scale_request_identity(self):
        self.original.write_bytes(self.original.read_bytes())
        # Identical bytes still cannot silently replace the producer input inode/stat.
        with self.assertRaisesRegex(full.Blocked, "original_pilot_scale_binding_differs"): self.execute()
        self.assertFalse(self.calls)

    def scale_stdout_echo(self, raw=None):
        mixed = self.root / "mixed"; mixed.mkdir()
        path = mixed / "header_scale_v2.log"
        path.write_bytes((self.scale / "manifest.json").read_bytes() if raw is None else raw)
        self.scale_stdout_echo_ref = self.ref(path)
        self.admission["prior"]["standalone_control_files"].append(str(path))
        self.admission["prior"]["files"].append(self.ref(path) | {"category": "control", "chunk": None})
        return path

    def test_scale_stdout_echo_has_only_one_literal_production_pin(self):
        self.assertEqual(full.SCALE_STDOUT_ECHO, {
            "path": "/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_scale_v2.log",
            "bytes": 1723, "sha256": "1fa7aadb16d893677a10ed00948806d1bef87c6698d63fc826b80ed620e37e3b"})

    def test_exact_scale_stdout_echo_is_control_but_does_not_roster_mixed_root(self):
        path = self.scale_stdout_echo()
        (path.parent / "unrelated_growing.log").write_bytes(b"not header evidence")
        self.assertEqual(self.execute()["status"], full.SUCCESS)
        item = next(item for item in self.admission["prior"]["files"] if item["path"] == str(path))
        self.assertEqual((item["category"], item["chunk"]), ("control", None))

    def test_scale_stdout_echo_changed_bytes_or_renamed_path_still_block(self):
        for index, renamed in enumerate((False, True)):
            if index: self.setUp()
            path = self.scale_stdout_echo()
            if renamed:
                changed = path.with_name("renamed_scale.log"); path.rename(changed)
                self.admission["prior"]["standalone_control_files"] = [str(changed)]
                self.admission["prior"]["files"][-1] = self.ref(changed) | {"category": "control", "chunk": None}
                code = "standalone_protocol_artifact_forbidden"
            else:
                path.write_bytes(path.read_bytes() + b" ")
                self.admission["prior"]["files"][-1] = self.ref(path) | {"category": "control", "chunk": None}
                code = "reference_changed"
            with self.assertRaisesRegex(full.Blocked, code): self.execute()
            self.assertFalse(self.calls)

    def test_scale_stdout_echo_must_equal_accepted_manifest_and_include_whole_scale_root(self):
        changed = full.read_json(self.inputs["scale_manifest"])[0] | {"accepted_keys": 999}
        self.scale_stdout_echo(full.encode(changed))
        with self.assertRaisesRegex(full.Blocked, "fixed_scale_stdout_manifest_differs"): self.execute()
        self.assertFalse(self.calls)
        self.setUp(); self.scale_stdout_echo()
        self.admission["prior"]["roots"].remove(str(self.scale))
        with self.assertRaisesRegex(full.Blocked, "fixed_scale_stdout_scale_root_missing"): self.execute()
        self.assertFalse(self.calls)

    def test_scale_stdout_echo_cannot_be_chunk_data_or_survive_late_mutation(self):
        path = self.scale_stdout_echo()
        self.admission["prior"]["files"][-1]["chunk"] = 0
        with self.assertRaisesRegex(full.Blocked, "fixed_scale_stdout_category_invalid"): self.execute()
        self.assertFalse(self.calls)
        self.admission["prior"]["files"][-1]["chunk"] = None
        def source(*_):
            if (self.output / ".COMPLETE.json.pending").exists():
                path.write_bytes(path.read_bytes().replace(b"1000", b"1001", 1))
            return self.source
        self.assert_blocked(self.execute(source_effect=source), "input_identity_changed")

    def roster_stdout_echo(self, *, changed_echo=False):
        root = self.root / "roster_builder"; root.mkdir()
        source = {"head": PRODUCER, "files": {name: self.source["files"][name]["sha256"] for name in full.roster_profile.SOURCE_FILES}}
        start = {"schema_version": "polymarket_exact_header_roster_build_v1", "source": source, "caps": full.roster_profile.CAPS,
                 "status": "blocked", "literal_source_generation_only": True, "final_universe_admitted": False, "analysis_admitted": False}
        self.save(root / "START.json", start)
        roster = self.save(root / "roster.json", {"schema_version": "polymarket_exact_header_key_roster_v1", "files": [{"source_role": "legacy_raw"}]})
        manifest = start | {"status": "sealed_source_roster_complete_not_inventory_admission", "roster": roster | {"path": "roster.json"}, "source_files": 1}
        mb = self.save(root / "manifest.json", manifest)
        self.save(root / "COMPLETE.json", {"status": manifest["status"], "manifest": mb | {"path": "manifest.json"},
                                          "final_universe_admitted": False, "analysis_admitted": False})
        self.inputs["roster"] = roster
        im = full.read_json(self.inputs["inventory_manifest"])[0]; im["roster"] = roster
        self.inputs["inventory_manifest"] = self.save(self.inv / "manifest.json", im)
        ic = full.read_json(self.inputs["inventory_complete"])[0]
        ic["manifest"] = self.inputs["inventory_manifest"] | {"path": "manifest.json"}
        self.inputs["inventory_complete"] = self.save(self.inv / "COMPLETE.json", ic)
        self.admission["prior"]["roots"].append(str(root)); self.refresh_ledger()
        mixed = self.root / "mixed"; mixed.mkdir(exist_ok=True)
        path = mixed / "header_roster_v1.log"
        self.save(path, manifest | {"source_files": 2} if changed_echo else manifest)
        self.roster_stdout_echo_ref = self.ref(path)
        self.admission["prior"]["standalone_control_files"].append(str(path))
        self.admission["prior"]["files"].append(self.ref(path) | {"category": "control", "chunk": None})
        return path, root

    def test_roster_stdout_echo_has_one_literal_production_pin_and_exact_accepted_root(self):
        self.assertEqual(full.ROSTER_STDOUT_ECHO, {
            "path": "/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_roster_v1.log",
            "bytes": 3084, "sha256": "4bd368bc90ac33203989b43174425a504da55730314fea833ff8d87d8f6a8aaf"})
        path, _ = self.roster_stdout_echo()
        (path.parent / "unrelated_growing.log").write_bytes(b"not header evidence")
        self.assertEqual(self.execute()["status"], full.SUCCESS)

    def test_roster_stdout_echo_changed_bytes_or_renamed_path_still_block(self):
        for index, renamed in enumerate((False, True)):
            if index: self.setUp()
            path, _ = self.roster_stdout_echo()
            if renamed:
                changed = path.with_name("renamed_roster.log"); path.rename(changed)
                self.admission["prior"]["standalone_control_files"] = [str(changed)]
                self.admission["prior"]["files"][-1] = self.ref(changed) | {"category": "control", "chunk": None}
                code = "standalone_protocol_artifact_forbidden"
            else:
                path.write_bytes(path.read_bytes() + b" ")
                self.admission["prior"]["files"][-1] = self.ref(path) | {"category": "control", "chunk": None}
                code = "reference_changed"
            with self.assertRaisesRegex(full.Blocked, code): self.execute()
            self.assertFalse(self.calls)

    def test_roster_stdout_requires_equal_manifest_whole_root_and_normal_profile_checks(self):
        self.roster_stdout_echo(changed_echo=True)
        with self.assertRaisesRegex(full.Blocked, "fixed_roster_stdout_manifest_differs"): self.execute()
        self.assertFalse(self.calls)
        self.setUp(); _, root = self.roster_stdout_echo()
        self.admission["prior"]["roots"].remove(str(root))
        with self.assertRaisesRegex(full.Blocked, "fixed_roster_stdout_roster_root_missing"): self.execute()
        self.assertFalse(self.calls)
        self.setUp(); _, root = self.roster_stdout_echo()
        marker = full.read_json(self.ref(root / "COMPLETE.json"))[0]; marker["status"] = "blocked"
        self.save(root / "COMPLETE.json", marker)
        item = next(item for item in self.admission["prior"]["files"] if item["path"] == str(root / "COMPLETE.json"))
        item.update(self.ref(root / "COMPLETE.json"))
        with self.assertRaisesRegex(full.Blocked, "roster_profile_marker_or_start_differs"): self.execute()
        self.assertFalse(self.calls)

    def test_roster_stdout_is_control_chunk_none_and_cannot_survive_late_mutation(self):
        path, _ = self.roster_stdout_echo()
        self.admission["prior"]["files"][-1]["chunk"] = 0
        with self.assertRaisesRegex(full.Blocked, "fixed_roster_stdout_category_invalid"): self.execute()
        self.assertFalse(self.calls)
        self.admission["prior"]["files"][-1]["chunk"] = None
        def source(*_):
            if (self.output / ".COMPLETE.json.pending").exists():
                raw = path.read_bytes(); path.write_bytes(raw[:-1] + b" ")
            return self.source
        self.assert_blocked(self.execute(source_effect=source), "input_identity_changed")


if __name__ == "__main__": unittest.main()
