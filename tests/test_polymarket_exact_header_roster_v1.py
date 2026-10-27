"""Synthetic sealed-lineage/Parquet-footer fixtures, never production data."""
from __future__ import annotations

import copy
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import io
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import build_polymarket_exact_header_roster_v1 as builder

HEAD = "a" * 40
OLD_HEAD = "b" * 40
HISTORY_HEAD = "c" * 40
SOURCE = {"head": HEAD, "files": {"fixture": "d" * 64}}
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)
RUN_ID = "12345678-1234-1234-1234-123456789abc"


def reference(path):
    raw = path.read_bytes()
    return {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def write(path, value, *, raw=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value if raw else builder.producer_encode(value))
    return reference(path)


def parquet(path, rows, *, legacy=False, kind=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = {"block_number": pa.array(list(range(1, rows + 1)), type=kind or (pa.int64() if legacy else pa.uint64())),
               "private_unprojected": pa.array(["never read this body"] * rows, type=pa.string())}
    if not legacy:
        columns["blockHash"] = pa.array(["0x" + f"{number:064x}" for number in range(1, rows + 1)], type=pa.string())
    pq.write_table(pa.table(columns), path, compression="zstd")


class SourceRosterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.output, self.descriptor = self.root / "out", self.root / "descriptor.json"
        self.legacy = self.root / "legacy.parquet"
        patches = {"FIRST": 100, "TAIL": 299, "PROBE_FIRST": 300, "CAMPAIGN_FIRST": 400,
                   "END": 599, "CHUNK_WIDTH": 100, "CHUNKS": 2, "REUSED_CHUNKS": 1,
                   "REUSED_IDS": {"c000001"}, "LEGACY_PATH": str(self.legacy),
                   "HISTORY_HEAD": HISTORY_HEAD, "APPEND_HEAD": HEAD, "REUSED_HEAD": OLD_HEAD}
        for key, value in patches.items():
            bound = patch.object(builder, key, value)
            bound.start(); self.addCleanup(bound.stop)
        self.source_patch = patch.object(builder, "source_snapshot", return_value=copy.deepcopy(SOURCE))
        self.source = self.source_patch.start(); self.addCleanup(self.source_patch.stop)
        disk = patch.object(builder.shutil, "disk_usage", return_value=FREE)
        self.disk = disk.start(); self.addCleanup(disk.stop)
        parquet(self.legacy, 3, legacy=True)
        raw = self.legacy.read_bytes(); footer_len = int.from_bytes(raw[-8:-4], "little")
        self.frozen_legacy = {"path": str(self.legacy), "bytes": len(raw), "rows_declared": 3,
            "identity": {"target": {**builder.identity(self.legacy), "bytes": len(raw)}},
            "footer_sha256": builder.digest(raw[-8-footer_len:]), "footer_bytes": footer_len}
        self.old = self.raw_fixture(self.root / "old/chunks/c000001/a01/raw", 400, 499, OLD_HEAD)
        self.fresh = self.raw_fixture(self.root / "campaign/chunks/c000002/a01/raw", 500, 599, HEAD, adaptive=True)
        self.probe = self.raw_fixture(self.root / "probe", 300, 399, HEAD)
        self.history = self.raw_fixture(self.root / "history/raw", 100, 299, HISTORY_HEAD, historical=True)
        self.history_owner = {"schema_version": "polymarket_v3_historical_acceptance_owner_v1", "profile": "full",
            "run_id": RUN_ID, "source": {"code_state": self.code(HISTORY_HEAD)}, "source_plan_binding": {"fixture": True},
            "economic_decoding_admitted": False, "analysis_admitted": False}
        self.history_result = {"schema_version": "polymarket_v3_historical_acceptance_result_v1",
            "status": "accepted_v3_historical_raw_not_analysis_admission", "profile": "full", "run_id": RUN_ID,
            "source": self.history_owner["source"], "source_plan_binding": self.history_owner["source_plan_binding"],
            "economic_decoding_admitted": False, "analysis_admitted": False, "children_still_running": False,
            "diagnostic_overlap_not_authoritative_source": False, "child_tree": {"sha256": "e" * 64}}
        self.exit = {"exit_code": 0, "owned_group_drained": True}
        self.campaign_owner = {"run_id": RUN_ID, "plan": {"source": self.code(HEAD), "chunks": [
            {"chunk": "c000001", "first_block": 400, "last_block": 499},
            {"chunk": "c000002", "first_block": 500, "last_block": 599}]}}
        self.campaign_manifest = {"schema_version": "polymarket_native_campaign_manifest_v2", "run_id": RUN_ID,
            "status": "raw_campaign_complete", "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True}
        self.build_inputs()

    @staticmethod
    def code(head):
        return {"status": "inspected", "head": head, "tracked_worktree_clean": True}

    def raw_fixture(self, root, first, last, head, *, adaptive=False, historical=False):
        addresses = builder.ADDRESSES[-1:] if historical else builder.ADDRESSES
        contract = {"schema_version": "polymarket_native_collection_contract_v" + ("2" if adaptive else "1"),
            "source": self.code(head), "first_block": first, "last_block": last, "addresses": list(addresses),
            "topics": sorted({builder.NATIVE_TOPIC if address not in builder.ADDRESSES[:2] else builder.LEGACY_TOPIC for address in addresses}),
            "mode": "v3-historical" if historical else "append", "end_exclusive_utc": "2026-10-02T00:00:00Z",
            "analysis_admitted": False, "raw_payload_only": True}
        if adaptive:
            contract["adaptive_policy"] = {"base_blocks": 50, "minimum_blocks": 1, "split": "binary_on_proven_getlogs_byte_limit_only"}
        else:
            contract["window_blocks"] = 100 if historical else 50
        roots = builder.grid(first, last, 100 if historical else 50)
        splits, leaves = [], roots
        if adaptive:
            node = roots[0]; middle = (node["first_block"] + node["last_block"]) // 2
            children = [builder.window(node["first_block"], middle), builder.window(middle+1, node["last_block"])]
            splits = [{"window": node, "children": children, "reason": "proven_getlogs_response_byte_limit"}]
            leaves = children + roots[1:]
        records = []
        for index, node in enumerate(leaves):
            nrows = 0 if index == 0 else 1
            relative = f"nodes/{node['window']}/a1/payload.parquet" if adaptive else f"attempts/{node['window']}.a1/payload.parquet"
            payload = root / relative; parquet(payload, nrows)
            counts = [{"address": address, "topic": builder.LEGACY_TOPIC if address in builder.ADDRESSES[:2] else builder.NATIVE_TOPIC,
                       "rows": nrows if address == addresses[-1] else 0} for address in builder.ADDRESSES]
            intervals = [{"address": item["address"], "topic": item["topic"], "rows": item["rows"],
                "first_block": node["first_block"], "last_block": node["last_block"],
                "status": "success_logs" if item["rows"] else "success_zero_logs"} for item in counts if item["address"] in addresses]
            records.append({"status": "raw_shard_validated", "attempt": 1, "window": node,
                "payload": {**reference(payload), "path": relative}, "evidence": {"rows": nrows, "distinct_native_keys": nrows, "counts": counts},
                "contract_intervals": intervals})
        owner = {"contract": contract, "run_id": RUN_ID}
        manifest = {"schema_version": "polymarket_native_collection_manifest_v" + ("2" if adaptive else "1"),
            "status": "raw_collection_complete", "collection_complete": True, "analysis_admitted": False,
            "raw_payload_only": True, "run_id": RUN_ID, "windows": records}
        if adaptive:
            manifest.update(roots=roots, splits=splits)
        return {"root": root, "owner": owner, "manifest": manifest}

    def seal_raw(self, item):
        root, owner, manifest = item["root"], item["owner"], item["manifest"]
        sha = builder.digest(builder.producer_encode(owner["contract"])); owner["contract_sha256"] = sha
        manifest.update(contract=owner["contract"], contract_sha256=sha,
                        rows=sum(row["evidence"]["rows"] for row in manifest["windows"]))
        manifest["distinct_native_keys"] = manifest["rows"]
        for row in manifest["windows"]: row["contract_sha256"] = sha
        for split in manifest.get("splits", []): split["contract_sha256"] = sha
        owner_ref = write(root / "OWNER.json", owner)
        manifest_ref = write(root / "manifest.json", manifest)
        marker_ref = write(root / "COMPLETE.json", {"status": "raw_collection_complete_not_analysis_admission",
            "run_id": RUN_ID, "contract_sha256": sha, "manifest_sha256": manifest_ref["sha256"]})
        item.update(owner_ref=owner_ref, manifest_ref=manifest_ref, marker_ref=marker_ref)

    def refs(self, item, group):
        return [{"path": str(item["root"] / row["payload"]["path"]), "bytes": row["payload"]["bytes"],
            "payload_sha256": row["payload"]["sha256"], "rows_declared": row["evidence"]["rows"], "group": group}
            for row in item["manifest"]["windows"]]

    def build_inputs(self):
        plan_path, inventory_path = self.root / "source_plan.json", self.root / "inventory.json"
        inventory_ref = write(inventory_path, {"inputs": {"raw_events": {"files": [self.frozen_legacy]}}})
        plan_ref = write(plan_path, {"inventory_binding": inventory_ref})
        plan_binding = {"path": plan_ref["path"], "sha256": plan_ref["sha256"], "source_head": HEAD}
        self.history_owner["source_plan_binding"] = plan_binding
        self.history_result["source_plan_binding"] = plan_binding
        self.history["owner"]["contract"]["source_plan"] = plan_binding
        self.campaign_owner["plan"]["source_plan"] = plan_binding
        self.fresh["owner"]["contract"]["source_plan"] = plan_binding
        for raw in (self.old, self.fresh, self.probe, self.history): self.seal_raw(raw)
        history_root = self.history["root"].parent
        self.history_owner["expected_child_contract"] = self.history["owner"]["contract"]
        howner = write(history_root / "controls/OWNER.json", self.history_owner)
        process = write(history_root / "controls/PROCESS.json", {"fixture": True})
        exited = write(history_root / "controls/CHILD_EXIT.json", self.exit)
        journal = write(history_root / "controls/REQUESTS.jsonl", b'{"event":"accepted"}\n', raw=True)
        self.history_result.update(accepted_child={"manifest_sha256": self.history["manifest_ref"]["sha256"],
            "owner_sha256": self.history["owner_ref"]["sha256"], "run_id": RUN_ID,
            "rows": self.history["manifest"]["rows"], "windows": len(self.history["manifest"]["windows"])},
            control_bindings={"hashes": {Path(ref["path"]).name: ref["sha256"] for ref in (howner, process, exited, journal)}, "journal_records": 1})
        hresult = write(history_root / "controls/RESULT.json", self.history_result)
        hmarker = write(history_root / "controls/COMPLETE.json", {"status": self.history_result["status"], "run_id": RUN_ID,
            "result_sha256": hresult["sha256"], "analysis_admitted": False, "child_tree_sha256": "e" * 64})
        self.campaign_owner["plan_sha256"] = builder.digest(builder.producer_encode(self.campaign_owner["plan"]))
        self.campaign_manifest.update(plan_sha256=self.campaign_owner["plan_sha256"],
            accepted_chunks=[{"chunk": chunk, "status": "accepted_raw_child",
                "accepted": {"manifest_sha256": raw["manifest_ref"]["sha256"], "rows": raw["manifest"]["rows"]},
                **({"external_attempt": str(raw["root"].parent), "producer_head": OLD_HEAD} if index == 0 else {})}
                for index, (chunk, raw) in enumerate(zip(self.campaign_owner["plan"]["chunks"], (self.old, self.fresh)))],
            rows=self.old["manifest"]["rows"] + self.fresh["manifest"]["rows"])
        camp_root = self.fresh["root"].parents[3]
        campaign_plan = write(camp_root / "PLAN.json", self.campaign_owner)
        campaign_manifest = write(camp_root / "manifests/frozen.json", self.campaign_manifest)
        campaign_marker = write(camp_root / "COMPLETE.json", {"status": "raw_campaign_complete_not_analysis_admission", "run_id": RUN_ID,
            "plan_sha256": self.campaign_owner["plan_sha256"], "manifest_path": "manifests/frozen.json", "manifest_sha256": campaign_manifest["sha256"]})
        current = self.refs(self.old, "reused_v1") + self.refs(self.fresh, "fresh_v2") + self.refs(self.probe, "initial_probe_v1")
        counts = {"native_payload_files": len(current), "native_payload_bytes_from_references": sum(ref["bytes"] for ref in current),
            "native_rows_declared": sum(ref["rows_declared"] for ref in current), "legacy_rows_declared": self.frozen_legacy["rows_declared"],
            "legacy_payload_bytes": self.frozen_legacy["bytes"],
            "native_payload_reference_projection_sha256": builder.digest(b"".join(builder.encode(ref) for ref in current))}
        preflight = write(self.root / "preflight.json", {"schema_version": "polymarket_header_source_roster_preflight_v1",
            "status": "sealed_roster_scale_preflight_complete_not_inventory_admission", "final_sample_admitted": False,
            "read_accounting": {"control_json_bytes_including_prior_inspection_reads": builder.PRIOR_JSON,
                "parquet_footer_bytes_read": builder.PRIOR_FOOTER, "trade_payload_body_bytes_read": 0, "payload_sha256_rehash_bytes": 0},
            "exact_declared_counts": counts})
        ledger = {"schema_version": "polymarket_header_global_ledger_v2", "category_ceiling_bytes": 8_000_000_000,
            "complete_header_category_roster": True, "baseline_cache_excluded": True,
            "entries": [{**preflight, "identity": builder.identity(Path(preflight["path"]))}],
            "inherited_rpc": {"result_path": preflight["path"], "result_sha256": preflight["sha256"],
                              "http_attempts": 103, "method_items": 1004, "response_bytes": 11922825, "elapsed_seconds": 8.829112935}}
        ledger_ref = write(self.root / "ledger.json", ledger)
        self.desc = {"schema_version": "polymarket_exact_header_roster_descriptor_v1", "preflight": preflight,
            "source_plan": plan_ref, "legacy_inventory": inventory_ref, "legacy_raw_path": str(self.legacy),
            "initial_native": {"root": str(self.probe["root"]), "owner": self.probe["owner_ref"], "manifest": self.probe["manifest_ref"],
                "complete": self.probe["marker_ref"], "producer_head": HEAD},
            "append_campaign": {"root": str(camp_root), "plan": campaign_plan, "complete": campaign_marker,
                "manifest": campaign_manifest, "producer_head": HEAD},
            "historical_full": {"root": str(history_root), "owner": howner, "complete": hmarker, "result": hresult,
                "raw_owner": self.history["owner_ref"], "raw_complete": self.history["marker_ref"],
                "raw_manifest": self.history["manifest_ref"], "producer_head": HISTORY_HEAD}, "header_ledger": ledger_ref}
        for name, sha in (("PREFLIGHT_SHA", preflight["sha256"]), ("PLAN_SHA", plan_ref["sha256"]),
                          ("PROBE_SHA", self.probe["manifest_ref"]["sha256"]), ("CAMPAIGN_SHA", campaign_manifest["sha256"])):
            patcher = patch.object(builder, name, sha); patcher.start(); self.addCleanup(patcher.stop)
        self.bind_descriptor()

    def bind_descriptor(self):
        self.descriptor_ref = write(self.descriptor, self.desc)

    def execute(self, **kwargs):
        return builder.run_builder(self.descriptor, self.descriptor_ref["sha256"], HEAD, self.output, **kwargs)

    def blocked(self, code=None):
        try:
            result = self.execute()
        except builder.Blocked as exc:
            self.assertFalse((self.output / "COMPLETE.json").exists())
            if code: self.assertEqual(str(exc), code)
            return
        self.assertEqual(result["status"], "blocked", result)
        self.assertFalse((self.output / "COMPLETE.json").exists())
        if code: self.assertEqual(result["error_code"], code)

    def test_literal_lineage_preserves_zero_rows_compact_inventory_schema(self):
        result = self.execute()
        self.assertEqual(result["status"], "sealed_source_roster_complete_not_inventory_admission", result)
        roster = json.loads((self.output / "roster.json").read_bytes())
        self.assertEqual(roster["schema_version"], "polymarket_exact_header_key_roster_v1")
        self.assertEqual(len(roster["files"]), 10)
        self.assertEqual(result["counts"]["historical_full"]["zero_row_files"], 1)
        self.assertEqual(result["source_footer_passes_in_this_builder"], 1)
        self.assertTrue(result["independent_inventory_native_rehash_and_footer_reopen_required"])
        self.assertFalse(result["final_universe_admitted"])
        self.assertNotIn("files", result)
        self.assertEqual(result["usage"]["trade_body_bytes_read"], 0)
        self.assertEqual(result["usage"]["native_payload_sha_rehash_bytes"], 0)
        for item in roster["files"]:
            self.assertEqual(set(item), {"path", "source_role", "bytes", "identity", "footer_sha256"}
                | ({"payload_sha256"} if item["source_role"] == "native_raw" else set()))
        marker = json.loads((self.output / "COMPLETE.json").read_bytes())
        self.assertEqual(marker["manifest"]["sha256"], reference(self.output / "manifest.json")["sha256"])
        self.assertLess(len((self.output / "roster.json").read_bytes()), builder.CAPS["roster_bytes"])
        self.assertGreater(marker["final_cumulative_json_bytes"], result["usage"]["cumulative_json_bytes_at_manifest_construction"])
        self.assertLessEqual(marker["final_cumulative_json_bytes"], builder.CAPS["cumulative_json_bytes"])

    def test_footer_only_reads_and_no_header_entry_body_hash(self):
        real_open = Path.open
        sources = {self.legacy, *(Path(ref["path"]) for item, group in ((self.old, "old"), (self.fresh, "fresh"),
                    (self.probe, "probe"), (self.history, "history")) for ref in self.refs(item, group))}
        reads = {path: [] for path in sources}
        footer_lengths = {}
        for path in sources:
            with real_open(path, "rb") as stream:
                stream.seek(-8, os.SEEK_END)
                footer_lengths[path] = int.from_bytes(stream.read(4), "little")
        class Restricted:
            def __init__(self, stream, path): self.stream, self.path = stream, path
            def __enter__(self): self.stream.__enter__(); return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def seek(self, *args): return self.stream.seek(*args)
            def read(self, size=-1):
                position = self.stream.tell(); total = self.path.stat().st_size
                raw = self.stream.read(size)
                footer_length = footer_lengths[self.path]
                if not (position == 0 and size == 4 or position >= total - footer_length - 8):
                    raise AssertionError("trade body read")
                reads[self.path].append((position, len(raw)))
                return raw
        def opened(path, *args, **kwargs):
            stream = real_open(path, *args, **kwargs)
            return Restricted(stream, path) if path in sources else stream
        with patch.object(Path, "open", opened): result = self.execute()
        self.assertEqual(result["status"], "sealed_source_roster_complete_not_inventory_admission", result)
        self.assertTrue(all(len(value) == 3 for value in reads.values()))
        self.assertFalse(result["header_accounting_entry_body_hashes_rechecked"])

    def test_historical_probe_and_unsealed_parent_rejected_before_footer(self):
        for key, value in (("profile", "probe"), ("status", "blocked"), ("children_still_running", True)):
            with self.subTest(key=key):
                previous = self.history_result[key]; self.history_result[key] = value
                self.build_inputs()
                with patch.object(builder, "footer_entry", side_effect=AssertionError("footer before full admission")):
                    self.blocked("historical_full_parent_not_accepted")
                self.history_result[key] = previous

    def test_historical_failed_exit_not_adopted(self):
        self.exit["exit_code"] = 1; self.build_inputs(); self.blocked("historical_child_not_drained")

    def test_historical_child_only_and_wrong_parent_source_rejected(self):
        self.history_owner["source"] = {"code_state": self.code(HEAD)}
        self.history_result["source"] = self.history_owner["source"]
        self.build_inputs(); self.blocked("historical_full_parent_not_accepted")

    def test_literal_grid_gap_rejected(self):
        self.probe["manifest"]["windows"].pop(); self.build_inputs()
        self.blocked("literal_window_grid_differs")

    def test_adaptive_noncanonical_split_and_unreachable_leaf_rejected(self):
        self.fresh["manifest"]["splits"][0]["reason"] = "timeout"
        self.build_inputs(); self.blocked("noncanonical_adaptive_split")

    def test_contract_topic_and_source_generation_mismatch(self):
        self.probe["owner"]["contract"]["topics"] = [builder.NATIVE_TOPIC]
        self.build_inputs(); self.blocked("raw_contract_generation_differs")

    def test_source_blob_equality_not_just_clean_status(self):
        self.source_patch.stop()
        state = self.code(HEAD)
        def command(argv, **kwargs):
            name = argv[-1].split(":", 1)[1]
            raw = (builder.REPO / name).read_bytes()
            return subprocess.CompletedProcess(argv, 0, stdout=raw, stderr=b"")
        with patch.object(builder.shared, "code_state", return_value=state), patch.object(builder.subprocess, "run", side_effect=command):
            actual = builder.source_snapshot(HEAD)
            self.assertEqual(set(actual["files"]), set(builder.SOURCE_FILES))
        def stale(argv, **kwargs):
            result = command(argv, **kwargs)
            result.stdout = result.stdout[:-1] + b" "
            return result
        with patch.object(builder.shared, "code_state", return_value=state), patch.object(builder.subprocess, "run", side_effect=stale):
            with self.assertRaisesRegex(builder.Blocked, "source_blob_or_identity_differs"):
                builder.source_snapshot(HEAD)
        self.source_patch.start()

    def test_unrequested_historical_rows_rejected(self):
        self.history["manifest"]["windows"][0]["evidence"]["counts"][0]["rows"] = 1
        self.build_inputs(); self.blocked("unrequested_address_rows")

    def test_payload_path_traversal_and_byte_type_fail_closed(self):
        self.probe["manifest"]["windows"][0]["payload"]["path"] = "../payload.parquet"
        self.build_inputs(); self.blocked("payload_reference_schema")

    def test_native_rowcount_footer_mismatch_retained_as_failure(self):
        ref = self.refs(self.probe, "probe")[0]
        path = Path(ref["path"]); parquet(path, 2)
        row = self.probe["manifest"]["windows"][0]; row["payload"] = {**reference(path), "path": row["payload"]["path"]}
        self.build_inputs(); self.blocked("source_footer_schema_or_rows")

    def test_legacy_identity_is_frozen_not_rebound(self):
        os.utime(self.legacy, ns=(1, 1)); self.blocked("legacy_frozen_identity_differs")

    def test_legacy_footer_sha_and_native_schema_fail_closed(self):
        self.frozen_legacy["footer_sha256"] = "0" * 64
        self.build_inputs(); self.blocked("legacy_frozen_footer_differs")

    def test_symlink_source_rejected(self):
        path = Path(self.refs(self.probe, "probe")[0]["path"])
        original = path.with_name("original.parquet"); path.rename(original); path.symlink_to(original)
        self.blocked("path_not_literal_absolute")

    def test_payload_duplicate_physical_file_rejected(self):
        first, second = self.refs(self.probe, "probe")
        path = Path(second["path"]); path.unlink(); os.link(first["path"], path)
        self.blocked("source_not_regular_single_link")

    def test_control_hash_and_same_size_mutation_rejected(self):
        path = Path(self.desc["historical_full"]["result"]["path"])
        raw = path.read_bytes(); path.write_bytes(raw[:-1] + b" ")
        self.blocked("control_hash_or_identity_differs")

    def test_json_and_footer_limits_include_inherited_reads(self):
        with patch.dict(builder.CAPS, cumulative_json_bytes=builder.PRIOR_JSON):
            self.blocked("cumulative_json_byte_limit")
        with patch.dict(builder.CAPS, cumulative_footer_bytes=builder.PRIOR_FOOTER + 12):
            self.blocked("footer_byte_limit")

    def test_footer_limit_does_not_read_footer_body_after_limit(self):
        with patch.dict(builder.CAPS, footer_bytes_per_file=12): self.blocked("footer_byte_limit")

    def test_file_count_roster_output_and_free_floor_limits(self):
        with patch.dict(builder.CAPS, files=2): self.blocked("source_file_count_limit")
        with patch.dict(builder.CAPS, roster_bytes=100): self.blocked("output_single_file_byte_limit")

    def test_global_ledger_size_stat_types_duplicates_and_preflight_are_bound(self):
        ledger_path = Path(self.desc["header_ledger"]["path"])
        original = json.loads(ledger_path.read_bytes())
        for variant in ("bool_identity", "understated", "duplicate", "missing_preflight"):
            with self.subTest(variant=variant):
                ledger = copy.deepcopy(original)
                if variant == "bool_identity": ledger["entries"][0]["identity"]["uid"] = True
                if variant == "understated": ledger["entries"][0]["bytes"] -= 1
                if variant == "duplicate": ledger["entries"].append(copy.deepcopy(ledger["entries"][0]))
                if variant == "missing_preflight": ledger["entries"][0]["sha256"] = "0"*64
                self.desc["header_ledger"] = write(ledger_path, ledger); self.bind_descriptor()
                self.blocked()

    def test_control_json_exact_duplicate_nonfinite_and_reference_types(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e9999}'):
            with self.subTest(raw=raw), self.assertRaises(builder.Blocked): builder.decode(raw)
        self.desc["preflight"]["bytes"] = True; self.bind_descriptor(); self.blocked("control_reference_size_or_sha")

    def test_observed_child_controls_are_bounded_before_reading(self):
        reader = builder.Reader(0, lambda: 0)
        oversized = self.root / "oversized.json"; oversized.write_bytes(b" " * 1025)
        with self.assertRaisesRegex(builder.Blocked, "observed_control_byte_limit"):
            reader.observed(oversized, limit=1024)
        self.assertEqual(reader.json_bytes, builder.PRIOR_JSON)

    def test_planned_final_json_reopens_reserved_before_footers(self):
        baseline = self.execute()
        cap = baseline["usage"]["cumulative_json_bytes_at_manifest_construction"]
        shutil.rmtree(self.output)
        with patch.dict(builder.CAPS, cumulative_json_bytes=cap):
            with patch.object(builder, "footer_entry", side_effect=AssertionError("footer before reopen reservation")):
                self.blocked("planned_final_json_reopen_byte_limit")

    def test_owned_output_reopens_are_charged_and_exact_counter_matches_marker(self):
        real = builder.Reader.owned_json
        output_reads, readers = [], []
        def charged(reader, path, **kwargs):
            before = reader.json_bytes
            raw = real(reader, path, **kwargs)
            self.assertEqual(reader.json_bytes - before, len(raw))
            output_reads.append((path.name, len(raw))); readers.append(reader)
            return raw
        with patch.object(builder.Reader, "owned_json", charged): result = self.execute()
        self.assertEqual(result["status"], "sealed_source_roster_complete_not_inventory_admission", result)
        roster_reads = [item for item in output_reads if item[0] in {".roster.json.pending", "roster.json"}]
        self.assertEqual(len(roster_reads), 3)
        self.assertGreaterEqual(len(output_reads), 9)
        marker = json.loads((self.output / "COMPLETE.json").read_bytes())
        self.assertEqual(marker["final_cumulative_json_bytes"], readers[-1].json_bytes)
        self.assertEqual(result["usage"]["reserved_output_json_reads_before_first_footer"],
                         3 * builder.CAPS["roster_bytes"] + 7 * 1024**2)

    def test_low_free_space_rejected_before_output(self):
        self.disk.return_value = shutil._ntuple_diskusage(1, 0, 1)
        self.blocked("resource_reservation_not_admitted")
        self.assertFalse(self.output.exists())

    def test_existing_output_never_overwritten(self):
        self.output.mkdir(); (self.output / "user.txt").write_text("keep")
        self.blocked("output_not_new_literal_directory")
        self.assertEqual((self.output / "user.txt").read_text(), "keep")

    def test_manifest_contains_no_payload_duplicate_list(self):
        result = self.execute()
        self.assertNotIn("payload.parquet", (self.output / "manifest.json").read_text())
        self.assertEqual(result["counts"]["legacy_raw"]["rows_declared"], 3)

    def test_late_input_output_and_source_mutation_withhold_marker(self):
        for phase in ("manifest", "pending"):
            for target in ("descriptor", "native", "source", "roster", "marker"):
                with self.subTest(phase=phase, target=target):
                    if self.output.exists(): shutil.rmtree(self.output)
                    self.build_inputs(); self.source.return_value = copy.deepcopy(SOURCE)
                    original_publish, original_fsync = builder.Writer.publish, builder.os.fsync
                    mutated = [False]
                    def mutate():
                        if mutated[0]: return
                        mutated[0] = True
                        if target == "source": self.source.return_value = {"head": "f" * 40}
                        else:
                            path = {"descriptor": self.descriptor, "native": Path(self.refs(self.probe, "probe")[-1]["path"]),
                                "roster": self.output / "roster.json", "marker": self.output / ".COMPLETE.json.pending"}[target]
                            if not path.exists(): return
                            raw = path.read_bytes()
                            if target == "native": path.write_bytes(raw[:4] + bytes([raw[4] ^ 1]) + raw[5:])
                            else: path.write_bytes(raw[:-1] + (b" " if raw[-1:] != b" " else b"\n"))
                    def published(writer, name, raw, **kwargs):
                        bound = original_publish(writer, name, raw, **kwargs)
                        if phase == "manifest" and name == "manifest.json" and target != "marker": mutate()
                        return bound
                    def fsync(fd):
                        result = original_fsync(fd)
                        if (phase == "pending" or target == "marker") and (self.output / ".COMPLETE.json.pending").exists(): mutate()
                        return result
                    with patch.object(builder.Writer, "publish", published), patch.object(builder.os, "fsync", fsync):
                        self.blocked()
                    self.assertTrue(mutated[0])
                    self.assertTrue((self.output / ".COMPLETE.json.pending").exists())
                    if target == "native": parquet(Path(self.refs(self.probe, "probe")[-1]["path"]), 1)
                    self.source.return_value = copy.deepcopy(SOURCE)

    def test_complete_file_directory_fsync_and_atomic_rename_faults(self):
        for operation in ("file", "directory", "rename"):
            with self.subTest(operation=operation):
                if self.output.exists(): shutil.rmtree(self.output)
                original_fsync, original_commit = builder.os.fsync, builder.atomic_commit
                def fsync(fd):
                    if (self.output / ".COMPLETE.json.pending").exists():
                        is_directory = __import__("stat").S_ISDIR(os.fstat(fd).st_mode)
                        if operation in {"file", "directory"} and (operation == "directory") == is_directory: raise OSError("fault")
                    return original_fsync(fd)
                def commit(pending, target):
                    if operation == "rename" and target.name == "COMPLETE.json": raise OSError("fault")
                    return original_commit(pending, target)
                with patch.object(builder.os, "fsync", fsync), patch.object(builder, "atomic_commit", commit): self.blocked()
                self.assertTrue((self.output / ".COMPLETE.json.pending").exists())

    def test_marker_is_last_fallible_artifact_operation(self):
        original = builder.atomic_commit
        committed = [False]
        original_open, original_identity = Path.open, builder.identity
        def commit(pending, destination):
            result = original(pending, destination)
            if destination.name == "COMPLETE.json": committed[0] = True
            return result
        def opened(path, *args, **kwargs):
            if committed[0]: raise AssertionError("artifact read after COMPLETE")
            return original_open(path, *args, **kwargs)
        def identity(path):
            if committed[0]: raise AssertionError("artifact stat after COMPLETE")
            return original_identity(path)
        with patch.object(builder, "atomic_commit", commit), patch.object(Path, "open", opened), patch.object(builder, "identity", identity):
            result = self.execute()
        self.assertEqual(result["status"], "sealed_source_roster_complete_not_inventory_admission", result)

    def test_deadline_at_staged_marker_withholds_complete(self):
        def clock(): return 1801 if (self.output / ".COMPLETE.json.pending").exists() else 0
        result = self.execute(clock=clock)
        self.assertEqual(result["error_code"], "runtime_limit")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_cli_guard_precedes_all_input_reads_and_rejects_overrides(self):
        with (patch.object(builder, "require_production_host", side_effect=RuntimeError("guard")) as guard,
              patch.object(builder, "run_builder", side_effect=AssertionError("input opened"))):
            with self.assertRaisesRegex(RuntimeError, "guard"):
                builder.main(["--descriptor", "/nonexistent", "--descriptor-sha256", "f"*64,
                              "--expected-head", HEAD, "--output-dir", "/nonexistent-output"])
            guard.assert_called_once()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            builder.main(["--max-files", "999999"])


if __name__ == "__main__":
    unittest.main()
