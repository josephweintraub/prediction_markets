#!/usr/bin/env python3
"""Build the fixed Oct-1 source roster from sealed raw lineage and footers only.

No trade body or native payload SHA is read. The subsequent inventory must
independently rehash native payloads and reopen every admitted source footer.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import inventory_polymarket_refresh as shared

PREFLIGHT_SHA = "acb848a8866959dd6fcaf5a8c5d62800c14b1b29cbed250be8fa2994bd71561e"
PLAN_SHA = "b07dfa71169029913a2281a2cb9df3d567d620be89aaa14b84eca5e674e0f688"
PROBE_SHA = "92af7ab19e0de9deab0f4237b09343cbc393eb7a6ff545afa175a562aaf9477c"
CAMPAIGN_SHA = "c5611226d9beb7c81dad4b2a5191edb2dbf3cfd987fbfb9a32b4d5c18a0e952e"
HISTORY_HEAD = "2ae681f0df162fa96c2988860a43ba312b208b8f"
APPEND_HEAD = "c39ddce3d9a010fe65faac652359fd401561e877"
REUSED_HEAD = "19202a820bef569f449a34fc731107f16470e155"
LEGACY_PATH = "/mnt/data/pipeline_data/raw_events.parquet"
FIRST, TAIL, END = 87_479_447, 88_978_537, 94_796_039
PROBE_FIRST, CAMPAIGN_FIRST, CHUNK_WIDTH = TAIL + 1, TAIL + 1001, 10_000
CHUNKS, REUSED_CHUNKS = 582, 19
REUSED_IDS = {f"c{index:06d}" for index in (*range(1, 18), 19, 20)}
PRIOR_JSON, PRIOR_FOOTER = 522_862_870, 29_878_542
CAPS = {"files": 150_000, "roster_bytes": 64 * 1024**2, "descriptor_bytes": 64 * 1024**2,
        "preflight_bytes": 1024**2, "lineage_json_bytes_per_file": 128 * 1024**2,
        "cumulative_json_bytes": 2_000_000_000, "cumulative_footer_bytes": 1_000_000_000,
        "footer_bytes_per_file": 32 * 1024**2, "all_new_artifact_bytes": 128_000_000,
        "global_header_bytes": 8_000_000_000, "address_space_bytes": 8 * 1024**3,
        "runtime_seconds": 1800, "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0}
EXTERNAL_RESERVE, TERMINAL_RESERVE = 2 * 1024**2, 1024**2
SHA = re.compile(r"[0-9a-f]{64}")
HEAD = re.compile(r"[0-9a-f]{40}")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
STAT_FIELDS = {"device", "inode", "mode", "uid", "gid", "mtime_ns", "ctime_ns"}
SOURCE_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                "scripts/build_polymarket_exact_header_roster_v1.py", "tests/test_polymarket_exact_header_roster_v1.py")
ADDRESSES = ("0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e", "0xc5d563a36ae78145c45a50134d48a1215220f80a",
             "0xe111180000d2663c0091e4f400237545b87b996b", "0xe2222d279d744050d28e00520010520000310f59",
             "0xe3333700ca9d93003f00f0f71f8515005f6c00aa")
LEGACY_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
NATIVE_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


class Blocked(ValueError):
    pass


def require(ok, code):
    if not ok:
        raise Blocked(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def producer_encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def finite(value):
        result = float(value)
        require(math.isfinite(result), "nonfinite_json")
        return result
    try:
        return json.loads(raw.decode(), object_pairs_hook=unique, parse_float=finite,
                          parse_constant=lambda _: (_ for _ in ()).throw(Blocked("nonfinite_json")))
    except Blocked:
        raise
    except (ValueError, UnicodeDecodeError, RecursionError, OverflowError):
        raise Blocked("invalid_json") from None


def identity(path):
    require(path.is_absolute() and path.resolve() == path, "path_not_literal_absolute")
    value = path.lstat()
    require(stat.S_ISREG(value.st_mode) and value.st_nlink == 1, "source_not_regular_single_link")
    return {"device": value.st_dev, "inode": value.st_ino, "mode": value.st_mode,
            "uid": value.st_uid, "gid": value.st_gid, "mtime_ns": value.st_mtime_ns,
            "ctime_ns": value.st_ctime_ns}


def source_snapshot(head):
    require(type(head) is str and HEAD.fullmatch(head), "invalid_source_head")
    state = shared.code_state()
    require(state.get("status") == "inspected" and state.get("head") == head
            and state.get("tracked_worktree_clean") is True, "source_not_clean_bound_head")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = identity(path)
        require(path.stat().st_size <= 1024**2, "source_file_size")
        raw = path.read_bytes()
        saved = subprocess.run(["git", "show", head + ":" + name], cwd=REPO, capture_output=True, timeout=10)
        require(saved.returncode == 0 and saved.stdout == raw and identity(path) == before,
                "source_blob_or_identity_differs")
        files[name] = digest(raw)
    require(state == shared.code_state(), "source_changed_during_snapshot")
    return {"head": head, "files": files}


class Reader:
    def __init__(self, started, clock):
        self.started, self.clock, self.controls = started, clock, {}
        self.json_bytes, self.footer_bytes = PRIOR_JSON, PRIOR_FOOTER

    def time(self):
        require(self.clock() - self.started < CAPS["runtime_seconds"], "runtime_limit")

    def owned_json(self, path, *, failure=False):
        """Charge all owned JSON reads too, including staging and failed bytes."""
        if not failure: self.time()
        before, size = identity(path), path.stat().st_size
        require(0 < size <= max(CAPS["roster_bytes"], 1024**2)
                and self.json_bytes + size <= CAPS["cumulative_json_bytes"], "owned_json_byte_limit")
        self.json_bytes += size
        with path.open("rb") as stream:
            raw = stream.read(size + 1)
        require(len(raw) == size and identity(path) == before, "owned_json_identity_differs")
        return raw

    def raw(self, reference, *, limit=None):
        self.time()
        require(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}, "control_reference_schema")
        path = Path(reference["path"])
        require(type(reference["bytes"]) is int and 0 < reference["bytes"] <= (limit or CAPS["lineage_json_bytes_per_file"])
                and type(reference["sha256"]) is str and SHA.fullmatch(reference["sha256"]), "control_reference_size_or_sha")
        before = identity(path)
        require(path.stat().st_size == reference["bytes"], "control_byte_count_differs")
        require(self.json_bytes + reference["bytes"] <= CAPS["cumulative_json_bytes"], "cumulative_json_byte_limit")
        self.json_bytes += reference["bytes"]
        with path.open("rb") as stream:
            raw = stream.read(reference["bytes"] + 1)
        require(len(raw) == reference["bytes"] and digest(raw) == reference["sha256"]
                and identity(path) == before, "control_hash_or_identity_differs")
        bound = {**reference, "identity": before}
        previous = self.controls.setdefault(str(path), bound)
        require(previous == bound, "conflicting_control_binding")
        return raw

    def json(self, reference, *, limit=None):
        return decode(self.raw(reference, limit=limit))

    def observed(self, path, *, limit=1024**2):
        """Read a fixed derived control once; its sealed manifest binds semantics."""
        self.time()
        before, size = identity(path), path.stat().st_size
        require(0 < size <= limit and self.json_bytes + size <= CAPS["cumulative_json_bytes"], "observed_control_byte_limit")
        self.json_bytes += size
        with path.open("rb") as stream:
            raw = stream.read(size + 1)
        require(len(raw) == size and identity(path) == before, "observed_control_identity_differs")
        reference = {"path": str(path), "bytes": size, "sha256": digest(raw)}
        bound = {**reference, "identity": before}
        require(self.controls.setdefault(str(path), bound) == bound, "conflicting_control_binding")
        return reference

    def derived(self, path, sha256):
        return self.json({"path": str(path), "bytes": path.stat().st_size, "sha256": sha256})

    def reopen(self):
        for bound in list(self.controls.values()):
            self.raw({key: bound[key] for key in ("path", "bytes", "sha256")})
            require(identity(Path(bound["path"])) == bound["identity"], "control_identity_changed")


def source_head(value):
    require(type(value) is dict, "producer_source_schema")
    state = value.get("code_state", value)
    require(type(state) is dict and state.get("tracked_worktree_clean") is True
            and type(state.get("head")) is str and HEAD.fullmatch(state["head"]), "producer_source_not_bound")
    return state["head"]


def window(first, last):
    return {"window": f"b{first:012d}-{last:012d}", "first_block": first, "last_block": last, "blocks": last-first+1}


def grid(first, last, width):
    return [window(start, min(last, start + width - 1)) for start in range(first, last + 1, width)]


def check_intervals(record, addresses):
    evidence = record["evidence"]
    counts = evidence["counts"]
    # The known native producer emits all five counters even for historical
    # V3-only requests. Unrequested addresses must be explicit zeros.
    require(type(counts) is list and len(counts) == len(ADDRESSES), "interval_count_roster")
    expected = []
    for address, item in zip(ADDRESSES, counts):
        topic = LEGACY_TOPIC if address in ADDRESSES[:2] else NATIVE_TOPIC
        require(item.get("address") == address and item.get("topic") == topic
                and type(item.get("rows")) is int and item["rows"] >= 0, "interval_count_schema")
        if address not in addresses:
            require(item["rows"] == 0, "unrequested_address_rows")
            continue
        expected.append({"address": address, "topic": topic, "first_block": record["window"]["first_block"],
                         "last_block": record["window"]["last_block"], "rows": item["rows"],
                         "status": "success_logs" if item["rows"] else "success_zero_logs"})
    require(record.get("contract_intervals") == expected and type(evidence.get("rows")) is int
            and evidence["rows"] == sum(item["rows"] for item in counts)
            and type(evidence.get("distinct_native_keys")) is int
            and evidence.get("distinct_native_keys") == evidence["rows"], "interval_rows_or_contracts_differ")


def check_grid(manifest, first, last, adaptive, addresses):
    records = manifest["windows"]
    require(type(records) is list and records, "empty_window_ledger")
    roots = grid(first, last, 50 if len(addresses) > 1 else 100)
    if adaptive:
        require(manifest.get("roots") == roots and type(manifest.get("splits")) is list, "adaptive_root_grid")
        splits, leaves, reached = {}, {}, set()
        for split in manifest["splits"]:
            key = split["window"]["window"]
            require(key not in splits, "duplicate_split_node"); splits[key] = split
        for record in records:
            key = record["window"]["window"]
            require(key not in leaves and key not in splits, "duplicate_leaf_node"); leaves[key] = record
        def visit(node):
            key = node["window"]
            require(key not in reached, "duplicate_grid_node"); reached.add(key)
            if key in splits:
                middle = (node["first_block"] + node["last_block"]) // 2
                require(node["blocks"] > 1, "singleton_split")
                children = [window(node["first_block"], middle), window(middle + 1, node["last_block"])]
                split = splits[key]
                require(split["window"] == node and split.get("children") == children
                        and split.get("reason") == "proven_getlogs_response_byte_limit", "noncanonical_adaptive_split")
                for child in children: visit(child)
            else:
                require(key in leaves and leaves[key]["window"] == node, "missing_grid_leaf")
        for root in roots: visit(root)
        require(reached == set(splits) | set(leaves), "unreachable_grid_node")
    else:
        require([item["window"] for item in records] == roots, "literal_window_grid_differs")
    ordered = sorted(records, key=lambda item: item["window"]["first_block"])
    require(ordered == records and ordered[0]["window"]["first_block"] == first
            and ordered[-1]["window"]["last_block"] == last and all(left["window"]["last_block"] + 1
                == right["window"]["first_block"] for left, right in zip(ordered, ordered[1:])), "leaf_gap_or_overlap")
    for record in records:
        require(record.get("status") == "raw_shard_validated", "unaccepted_raw_window")
        check_intervals(record, addresses)


def collect_raw(reader, root, owner_ref, marker_ref, manifest_ref, producer_head, first, last, adaptive, addresses, group):
    require(owner_ref["path"] == str(root / "OWNER.json") and marker_ref["path"] == str(root / "COMPLETE.json")
            and manifest_ref["path"] == str(root / "manifest.json"), "raw_control_path_differs")
    owner, marker, manifest = reader.json(owner_ref), reader.json(marker_ref), reader.json(manifest_ref)
    contract = owner.get("contract")
    require(type(contract) is dict and owner.get("contract_sha256") == digest(producer_encode(contract)), "raw_contract_sha_differs")
    contract_sha = owner["contract_sha256"]
    require(marker.get("status") == "raw_collection_complete_not_analysis_admission"
            and marker.get("manifest_sha256") == manifest_ref["sha256"] and marker.get("run_id") == owner.get("run_id")
            == manifest.get("run_id") and type(owner.get("run_id")) is str and UUID.fullmatch(owner["run_id"])
            and marker.get("contract_sha256") == manifest.get("contract_sha256") == contract_sha
            and manifest.get("contract") == contract, "raw_marker_owner_manifest_differ")
    require(manifest.get("schema_version") == "polymarket_native_collection_manifest_v" + ("2" if adaptive else "1")
            and manifest.get("status") == "raw_collection_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True,
            "raw_manifest_not_sealed")
    require(contract.get("first_block") == first and contract.get("last_block") == last
            and contract.get("addresses") == list(addresses) and source_head(contract.get("source")) == producer_head
            and contract.get("topics") == sorted({LEGACY_TOPIC if address in ADDRESSES[:2] else NATIVE_TOPIC for address in addresses})
            and contract.get("mode") == ("v3-historical" if len(addresses) == 1 else "append")
            and contract.get("end_exclusive_utc") == "2026-10-02T00:00:00Z"
            and contract.get("analysis_admitted") is False and contract.get("raw_payload_only") is True,
            "raw_contract_generation_differs")
    require(contract.get("schema_version") == "polymarket_native_collection_contract_v" + ("2" if adaptive else "1"), "raw_contract_version")
    if not adaptive:
        require(contract.get("window_blocks") == (100 if len(addresses) == 1 else 50), "raw_window_width")
    else:
        require(contract.get("adaptive_policy") == {"base_blocks": 50, "minimum_blocks": 1,
                "split": "binary_on_proven_getlogs_byte_limit_only"}, "adaptive_policy_differs")
    check_grid(manifest, first, last, adaptive, addresses)
    rows = sum(item["evidence"]["rows"] for item in manifest["windows"])
    require(type(manifest.get("rows")) is int and type(manifest.get("distinct_native_keys")) is int
            and rows == manifest["rows"] == manifest.get("distinct_native_keys"), "raw_row_reconciliation")
    refs = []
    for item in manifest["windows"]:
        require(item.get("contract_sha256") == contract_sha, "window_contract_sha_differs")
        ref = item["payload"]
        require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"} and type(ref["path"]) is str
                and not Path(ref["path"]).is_absolute() and ".." not in Path(ref["path"]).parts
                and type(ref["bytes"]) is int and ref["bytes"] > 0 and type(ref["sha256"]) is str
                and SHA.fullmatch(ref["sha256"]), "payload_reference_schema")
        attempt = item.get("attempt")
        require(type(attempt) is int and 1 <= attempt <= 3, "payload_attempt_invalid")
        node = item["window"]["window"]
        expected = f"nodes/{node}/a{attempt}/payload.parquet" if adaptive else f"attempts/{node}.a{attempt}/payload.parquet"
        require(ref["path"] == expected, "payload_path_not_producer_literal")
        refs.append({"path": str(root / expected), "bytes": ref["bytes"], "payload_sha256": ref["sha256"],
                     "rows_declared": item["evidence"]["rows"], "group": group})
    return refs, owner, manifest


def child_references(reader, root, sha256):
    # These paths are fixed producer outputs, never directory discovery.
    paths = [root / name for name in ("OWNER.json", "COMPLETE.json", "manifest.json")]
    owner = reader.observed(paths[0])
    marker = reader.observed(paths[1])
    return owner, marker, {"path": str(paths[2]), "bytes": paths[2].stat().st_size, "sha256": sha256}


def historical(reader, descriptor):
    root = Path(descriptor["root"])
    owner, marker, result = (reader.json(descriptor[key]) for key in ("owner", "complete", "result"))
    require(descriptor["owner"]["path"] == str(root / "controls/OWNER.json")
            and descriptor["complete"]["path"] == str(root / "controls/COMPLETE.json")
            and descriptor["result"]["path"] == str(root / "controls/RESULT.json"), "historical_parent_control_path")
    status = "accepted_v3_historical_raw_not_analysis_admission"
    require(owner.get("schema_version") == "polymarket_v3_historical_acceptance_owner_v1"
            and result.get("schema_version") == "polymarket_v3_historical_acceptance_result_v1"
            and marker.get("status") == result.get("status") == status and owner.get("profile") == result.get("profile") == "full"
            and marker.get("result_sha256") == descriptor["result"]["sha256"] and marker.get("run_id") == result.get("run_id")
            == owner.get("run_id") and result.get("children_still_running") is False
            and result.get("diagnostic_overlap_not_authoritative_source") is False and result.get("analysis_admitted") is False
            and result.get("economic_decoding_admitted") is False and result.get("source") == owner.get("source")
            and source_head(owner.get("source")) == descriptor["producer_head"] == HISTORY_HEAD, "historical_full_parent_not_accepted")
    require(owner.get("source_plan_binding") == result.get("source_plan_binding")
            and owner.get("analysis_admitted") is False and owner.get("economic_decoding_admitted") is False
            and marker.get("analysis_admitted") is False, "historical_parent_contract_differs")
    controls = result.get("control_bindings", {}).get("hashes", {})
    require(type(controls) is dict and set(controls) == {"OWNER.json", "PROCESS.json", "CHILD_EXIT.json", "REQUESTS.jsonl"}
            and controls["OWNER.json"] == descriptor["owner"]["sha256"], "historical_parent_control_bindings")
    for name, sha in controls.items():
        path = root / "controls" / name
        raw = reader.raw({"path": str(path), "bytes": path.stat().st_size, "sha256": sha}, limit=1024**2)
        if name == "CHILD_EXIT.json":
            exited = decode(raw)
            require(exited.get("exit_code") == 0 and exited.get("owned_group_drained") is True, "historical_child_not_drained")
        elif name == "REQUESTS.jsonl":
            records = [decode(line) for line in raw.splitlines()]
            require(len(records) == result["control_bindings"].get("journal_records"), "historical_journal_count_differs")
        else:
            require(type(decode(raw)) is dict, "historical_parent_control_schema")
    proof = result.get("accepted_child")
    require(type(proof) is dict and proof.get("manifest_sha256") == descriptor["raw_manifest"]["sha256"]
            and proof.get("owner_sha256") == descriptor["raw_owner"]["sha256"]
            and type(marker.get("child_tree_sha256")) is str and SHA.fullmatch(marker["child_tree_sha256"])
            and result.get("child_tree", {}).get("sha256") == marker.get("child_tree_sha256"), "historical_parent_child_binding")
    refs, raw_owner, manifest = collect_raw(reader, root / "raw", descriptor["raw_owner"], descriptor["raw_complete"],
            descriptor["raw_manifest"], descriptor["producer_head"], FIRST, TAIL, False, ADDRESSES[-1:], "historical_full")
    require(owner.get("expected_child_contract") == raw_owner.get("contract") and proof.get("run_id") == raw_owner.get("run_id")
            and raw_owner["contract"].get("source_plan") == owner.get("source_plan_binding")
            and proof.get("rows") == manifest["rows"] and proof.get("windows") == len(refs), "historical_accepted_child_differ")
    return refs, owner


def lineage(reader, desc, preflight):
    # A child COMPLETE alone or a probe profile never authorizes this builder.
    historical_refs, owner = historical(reader, desc["historical_full"])
    historical_plan = owner.get("source_plan_binding", {})
    require(historical_plan.get("path") == desc["source_plan"]["path"]
            and historical_plan.get("sha256") == desc["source_plan"]["sha256"], "historical_frozen_plan_binding_differs")
    plan = reader.json(desc["source_plan"])
    frozen = reader.json(desc["legacy_inventory"])
    require(desc["source_plan"]["sha256"] == PLAN_SHA and plan.get("inventory_binding", {}).get("sha256")
            == desc["legacy_inventory"]["sha256"] and plan["inventory_binding"]["path"] == desc["legacy_inventory"]["path"], "frozen_plan_inventory_binding")
    legacy = frozen["inputs"]["raw_events"]["files"]
    require(type(legacy) is list and len(legacy) == 1 and legacy[0]["path"] == desc["legacy_raw_path"] == LEGACY_PATH,
            "legacy_source_not_single_frozen_file")
    app = desc["append_campaign"]; root = Path(app["root"])
    owner, marker, manifest = (reader.json(app[key]) for key in ("plan", "complete", "manifest"))
    require(app["plan"]["path"] == str(root / "PLAN.json") and app["complete"]["path"] == str(root / "COMPLETE.json")
            and app["manifest"]["path"] == str(root / marker.get("manifest_path", ""))
            and app["manifest"]["sha256"] == CAMPAIGN_SHA == marker.get("manifest_sha256"), "campaign_top_path_or_sha")
    require(manifest.get("schema_version") == "polymarket_native_campaign_manifest_v2"
            and marker.get("status") == "raw_campaign_complete_not_analysis_admission"
            and manifest.get("status") == "raw_campaign_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True
            and owner.get("run_id") == marker.get("run_id") == manifest.get("run_id")
            and owner.get("plan_sha256") == marker.get("plan_sha256") == manifest.get("plan_sha256")
            == digest(producer_encode(owner["plan"])) and source_head(owner["plan"]["source"]) == app["producer_head"] == APPEND_HEAD,
            "campaign_not_sealed_bound_plan")
    campaign_plan = owner["plan"].get("source_plan", {})
    require(campaign_plan.get("path") == desc["source_plan"]["path"]
            and campaign_plan.get("sha256") == desc["source_plan"]["sha256"], "campaign_frozen_plan_binding_differs")
    chunks = [{"chunk": f"c{index:06d}", "first_block": start, "last_block": min(END, start + CHUNK_WIDTH - 1)}
              for index, start in enumerate(range(CAMPAIGN_FIRST, END + 1, CHUNK_WIDTH), 1)]
    accepted = manifest.get("accepted_chunks")
    require(len(chunks) == CHUNKS and type(accepted) is list and [item["chunk"] for item in accepted] == chunks
            and owner["plan"].get("chunks") == chunks, "campaign_literal_chunk_grid")
    current, reused = [], 0
    for item in accepted:
        chunk = item["chunk"]
        require(item.get("status") == "accepted_raw_child", "campaign_unaccepted_child")
        external = "external_attempt" in item
        if external:
            attempt = Path(item["external_attempt"])
            require(attempt.name == "a01" and attempt.parent.name == chunk["chunk"]
                    and attempt.parent.parent.name == "chunks", "external_child_path")
            producer = item["producer_head"]; reused += 1
            require(producer == REUSED_HEAD and reused <= REUSED_CHUNKS, "reused_producer_generation_differs")
        else:
            attempt = root / "chunks" / chunk["chunk"] / "a01"; producer = app["producer_head"]
        raw = attempt / "raw"
        require(external == (chunk["chunk"] in REUSED_IDS), "reused_chunk_roster_order")
        refs, child_owner, child = collect_raw(reader, raw, *child_references(reader, raw, item["accepted"]["manifest_sha256"]),
                producer, chunk["first_block"], chunk["last_block"], not external, ADDRESSES,
                "reused_v1" if external else "fresh_v2")
        require(child["rows"] == item["accepted"]["rows"], "accepted_child_rows_differ")
        if not external:
            require(child_owner["contract"].get("source_plan") == campaign_plan, "fresh_child_source_plan_binding_differs")
        current.extend(refs)
    require(reused == REUSED_CHUNKS and sum(item["rows_declared"] for item in current) == manifest["rows"], "campaign_reuse_or_rows_differ")
    probe = desc["initial_native"]
    require(probe["manifest"]["sha256"] == PROBE_SHA, "initial_probe_sha_differs")
    refs, _, _ = collect_raw(reader, Path(probe["root"]), probe["owner"], probe["complete"], probe["manifest"],
            probe["producer_head"], PROBE_FIRST, CAMPAIGN_FIRST - 1, False, ADDRESSES, "initial_probe_v1")
    current.extend(refs)
    expected = preflight["exact_declared_counts"]
    require(len(current) == expected["native_payload_files"] and sum(item["bytes"] for item in current)
            == expected["native_payload_bytes_from_references"] and sum(item["rows_declared"] for item in current)
            == expected["native_rows_declared"] and legacy[0]["rows_declared"] == expected["legacy_rows_declared"]
            and legacy[0]["bytes"] == expected["legacy_payload_bytes"], "current_source_declarations_differ_from_preflight")
    rolling = hashlib.sha256()
    for item in current: rolling.update(encode(item))
    require(rolling.hexdigest() == expected["native_payload_reference_projection_sha256"], "current_payload_references_differ_from_preflight")
    return legacy[0], current + historical_refs


def header_accounting(reader, reference, descriptor_path):
    ledger = reader.json(reference, limit=CAPS["descriptor_bytes"])
    require(type(ledger) is dict and set(ledger) == {"schema_version", "category_ceiling_bytes", "complete_header_category_roster",
            "baseline_cache_excluded", "entries", "inherited_rpc"}
            and ledger.get("schema_version") == "polymarket_header_global_ledger_v2"
            and ledger.get("category_ceiling_bytes") == CAPS["global_header_bytes"]
            and ledger.get("complete_header_category_roster") is True and ledger.get("baseline_cache_excluded") is True
            and type(ledger.get("inherited_rpc")) is dict and type(ledger.get("entries")) is list
            and 0 < len(ledger["entries"]) <= CAPS["files"], "header_ledger_schema")
    inherited = ledger["inherited_rpc"]
    require(set(inherited) == {"result_path", "result_sha256", "http_attempts", "method_items", "response_bytes", "elapsed_seconds"}
            and type(inherited["result_path"]) is str and Path(inherited["result_path"]).is_absolute()
            and type(inherited["result_sha256"]) is str and SHA.fullmatch(inherited["result_sha256"])
            and all(type(inherited[key]) is int and inherited[key] >= 0 for key in ("http_attempts", "method_items", "response_bytes"))
            and type(inherited["elapsed_seconds"]) in (int, float) and math.isfinite(inherited["elapsed_seconds"])
            and inherited["elapsed_seconds"] >= 0, "header_ledger_inherited_schema")
    paths, physical, total = set(), set(), reference["bytes"]
    for entry in ledger["entries"]:
        require(type(entry) is dict and set(entry) == {"path", "bytes", "sha256", "identity"}
                and type(entry["bytes"]) is int and entry["bytes"] >= 0 and type(entry["sha256"]) is str
                and SHA.fullmatch(entry["sha256"]) and type(entry["identity"]) is dict
                and set(entry["identity"]) == STAT_FIELDS and all(type(value) is int for value in entry["identity"].values()),
                "header_ledger_entry_schema")
        path = Path(entry["path"])
        actual = identity(path); key = (actual["device"], actual["inode"])
        require(str(path) not in paths and key not in physical and path != descriptor_path
                and path != Path(reference["path"]), "header_ledger_duplicate_or_recursive")
        require(actual == entry["identity"] and path.stat().st_size == entry["bytes"], "header_ledger_entry_changed")
        paths.add(str(path)); physical.add(key); total += entry["bytes"]
    require(total < CAPS["global_header_bytes"], "header_global_byte_limit")
    return ledger, total, paths, physical


def footer_entry(reader, ref, legacy=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = Path(ref["path"]); before = identity(path)
    require(path.stat().st_size == ref["bytes"], "source_payload_size_differs")
    if legacy:
        require(before == {key: ref["identity"]["target"][key] for key in STAT_FIELDS}, "legacy_frozen_identity_differs")
    with path.open("rb", buffering=0) as stream:
        reader.time()
        require(reader.footer_bytes + 12 <= CAPS["cumulative_footer_bytes"], "footer_byte_limit")
        reader.footer_bytes += 12
        require(stream.read(4) == b"PAR1", "parquet_magic")
        stream.seek(-8, os.SEEK_END); trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "parquet_trailer")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= ref["bytes"] - 12 and length + 12 <= CAPS["footer_bytes_per_file"]
                and reader.footer_bytes + length <= CAPS["cumulative_footer_bytes"], "footer_byte_limit")
        reader.footer_bytes += length
        stream.seek(-8 - length, os.SEEK_END); raw = stream.read(length)
        require(len(raw) == length, "footer_short_read")
    footer_sha = digest(raw + trailer)
    require(not legacy or footer_sha == ref["footer_sha256"], "legacy_frozen_footer_differs")
    meta = pq.read_metadata(pa.BufferReader(b"PAR1" + raw + trailer)); schema = meta.schema.to_arrow_schema()
    require(len(schema.names) == len(set(schema.names)) and schema.field("block_number").type
            == (pa.int64() if legacy else pa.uint64()) and meta.num_rows == ref["rows_declared"], "source_footer_schema_or_rows")
    if not legacy: require(schema.field("blockHash").type == pa.string(), "native_footer_hash_schema")
    require(identity(path) == before, "source_identity_changed_during_footer")
    entry = {"path": str(path), "bytes": ref["bytes"], "identity": before, "footer_sha256": footer_sha,
             "source_role": "legacy_raw" if legacy else "native_raw"}
    if not legacy: entry["payload_sha256"] = ref["payload_sha256"]
    return entry


def atomic_commit(pending, destination):
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        result = library.renameat2(-100, os.fsencode(pending), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        result = library.renamex_np(os.fsencode(pending), os.fsencode(destination), 4)
    else: raise Blocked("atomic_publication_unavailable")
    require(result == 0, "atomic_publication_refused")


class Writer:
    def __init__(self, root, reader, existing):
        self.root, self.reader, self.existing, self.files = root, reader, existing, {}

    def check(self, extra=0, failure=False, final=False):
        if not failure: self.reader.time()
        total = 0
        for path in self.root.iterdir():
            require(path.name in self.files and stat.S_ISREG(path.lstat().st_mode) and path.lstat().st_nlink == 1,
                    "unexpected_output_artifact")
            total += path.stat().st_size
        reserve = EXTERNAL_RESERVE + (0 if final or failure else TERMINAL_RESERVE)
        require(total + extra + reserve <= CAPS["all_new_artifact_bytes"]
                and self.existing + total + extra + reserve <= CAPS["global_header_bytes"], "artifact_byte_limit")
        require(shutil.disk_usage(self.root).free >= extra + reserve + CAPS["minimum_free_bytes"], "free_space_floor")
        return total

    def reopen(self):
        self.check()
        for name, bound in self.files.items():
            raw = self.reader.owned_json(self.root / name)
            require(len(raw) == bound["bytes"] and digest(raw) == bound["sha256"], "output_reopen_differs")

    def publish(self, name, raw, *, failure=False, final_check=None, final_json_bytes=None):
        limit = CAPS["roster_bytes"] if name == "roster.json" else 1024**2
        require(len(raw) <= limit and Path(name).name == name, "output_single_file_byte_limit")
        self.check(len(raw), failure, final_check is not None)
        pending, target = self.root / ("." + name + ".pending"), self.root / name
        require(not pending.exists() and not pending.is_symlink() and not target.exists() and not target.is_symlink(), "output_exists")
        bound = {"path": name, "bytes": len(raw), "sha256": digest(raw)}
        self.files[pending.name] = bound
        with pending.open("xb") as stream:
            require(stream.write(raw) == len(raw), "output_short_write"); stream.flush(); os.fsync(stream.fileno())
        require(self.reader.owned_json(pending, failure=failure) == raw, "output_reopen_differs")
        fd = os.open(self.root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(fd)
        finally: os.close(fd)
        if final_check is not None:
            final_check(); self.reopen()
            require(final_json_bytes == self.reader.json_bytes, "final_json_accounting_differs")
        self.check(0, failure, final_check is not None)
        atomic_commit(pending, target)
        self.files[name] = self.files.pop(pending.name)
        return bound


def run_builder(descriptor_path, descriptor_sha, expected_head, output, *, clock=time.monotonic):
    descriptor_path, output = Path(descriptor_path), Path(output)
    reader = Reader(clock(), clock)
    require(output.is_absolute() and output.parent.is_dir() and output.parent.resolve() == output.parent
            and not output.exists() and not output.is_symlink(), "output_not_new_literal_directory")
    source = source_snapshot(expected_head)
    descriptor_ref = {"path": str(descriptor_path), "bytes": descriptor_path.stat().st_size, "sha256": descriptor_sha}
    desc = reader.json(descriptor_ref, limit=CAPS["descriptor_bytes"])
    require(type(desc) is dict and set(desc) == {"schema_version", "preflight", "source_plan", "legacy_inventory",
            "legacy_raw_path", "initial_native", "append_campaign", "historical_full", "header_ledger"}
            and desc["schema_version"] == "polymarket_exact_header_roster_descriptor_v1", "descriptor_schema")
    for key, fields in (("initial_native", {"root", "owner", "complete", "manifest", "producer_head"}),
                        ("append_campaign", {"root", "plan", "complete", "manifest", "producer_head"}),
                        ("historical_full", {"root", "owner", "complete", "result", "raw_owner", "raw_complete", "raw_manifest", "producer_head"})):
        require(type(desc[key]) is dict and set(desc[key]) == fields and type(desc[key]["root"]) is str
                and Path(desc[key]["root"]).is_absolute() and type(desc[key]["producer_head"]) is str
                and HEAD.fullmatch(desc[key]["producer_head"]), "descriptor_source_schema")
    preflight = reader.json(desc["preflight"], limit=CAPS["preflight_bytes"])
    require(desc["preflight"]["sha256"] == PREFLIGHT_SHA and preflight.get("schema_version") == "polymarket_header_source_roster_preflight_v1"
            and preflight.get("status") == "sealed_roster_scale_preflight_complete_not_inventory_admission"
            and preflight.get("read_accounting", {}).get("control_json_bytes_including_prior_inspection_reads") == PRIOR_JSON
            and preflight["read_accounting"].get("parquet_footer_bytes_read") == PRIOR_FOOTER
            and preflight["read_accounting"].get("trade_payload_body_bytes_read") == 0
            and preflight["read_accounting"].get("payload_sha256_rehash_bytes") == 0
            and preflight.get("final_sample_admitted") is False, "preflight_binding_or_prior_charges_differ")
    legacy, native = lineage(reader, desc, preflight)
    require(1 + len(native) <= CAPS["files"], "source_file_count_limit")
    ledger, existing, ledger_paths, ledger_physical = header_accounting(reader, desc["header_ledger"], descriptor_path)
    existing += descriptor_ref["bytes"]
    require(any(entry["path"] == desc["preflight"]["path"] and entry["sha256"] == PREFLIGHT_SHA
                for entry in ledger["entries"]), "header_ledger_omits_preflight")
    # Three complete roster reads (staging, schema/count reopening, final
    # marker gate), plus the small controls and one possible failed receipt.
    output_json_allowance = 3 * CAPS["roster_bytes"] + 7 * 1024**2
    require(reader.json_bytes + sum(item["bytes"] for item in reader.controls.values()) + output_json_allowance
            <= CAPS["cumulative_json_bytes"], "planned_final_json_reopen_byte_limit")
    require(shutil.disk_usage(output.parent).free >= CAPS["all_new_artifact_bytes"] + CAPS["minimum_free_bytes"]
            and existing + CAPS["all_new_artifact_bytes"] <= CAPS["global_header_bytes"], "resource_reservation_not_admitted")
    for path in [descriptor_path, *[Path(item) for item in reader.controls], Path(legacy["path"]),
                 *[Path(item["path"]) for item in native]]:
        require(output != path and output not in path.parents and path not in output.parents, "output_overlaps_input")
    output.mkdir(mode=0o700); writer = Writer(output, reader, existing)
    result = {"schema_version": "polymarket_exact_header_roster_build_v1", "status": "blocked",
              "literal_source_generation_only": True, "final_universe_admitted": False, "analysis_admitted": False,
              "source": source, "descriptor": descriptor_ref, "caps": CAPS}
    try:
        writer.publish("START.json", encode(result))
        files, paths, physical, counts = [], set(), set(), {}
        for ref, is_legacy in [(legacy, True), *[(item, False) for item in native]]:
            entry = footer_entry(reader, ref, is_legacy)
            key = (entry["identity"]["device"], entry["identity"]["inode"])
            require(entry["path"] not in paths and key not in physical, "duplicate_source_path_or_physical_file")
            require(entry["path"] not in ledger_paths and key not in ledger_physical, "trade_source_charged_as_header_artifact")
            paths.add(entry["path"]); physical.add(key); files.append(entry)
            group = "legacy_raw" if is_legacy else ref["group"]
            target = counts.setdefault(group, {"files": 0, "bytes": 0, "rows_declared": 0, "zero_row_files": 0})
            target["files"] += 1; target["bytes"] += entry["bytes"]
            target["rows_declared"] += ref["rows_declared"]; target["zero_row_files"] += int(ref["rows_declared"] == 0)
        roster = {"schema_version": "polymarket_exact_header_key_roster_v1", "files": files}
        roster_raw = encode(roster)
        bound = writer.publish("roster.json", roster_raw)
        reopened = decode(reader.owned_json(output / "roster.json"))
        require(reopened == roster and len(reopened["files"]) == len(paths) == len(physical), "roster_schema_or_count_reopen_differs")
        # Hash all consumed lineage/control JSON exactly once more, immediately
        # inside the staged marker gate. Source bodies and footers are not reread.
        def final_inputs():
            require(source_snapshot(expected_head) == source, "source_changed")
            reader.reopen()
            for entry in files:
                require(identity(Path(entry["path"])) == entry["identity"] and Path(entry["path"]).stat().st_size == entry["bytes"], "source_identity_changed_before_commit")
            for entry in ledger["entries"]:
                require(identity(Path(entry["path"])) == entry["identity"] and Path(entry["path"]).stat().st_size == entry["bytes"], "header_ledger_entry_changed")
        control_projection = hashlib.sha256()
        for path in sorted(reader.controls): control_projection.update(encode(reader.controls[path]))
        result.update(status="sealed_source_roster_complete_not_inventory_admission", roster=bound,
                      source_files=len(files), counts=counts, lineage_control_files=len(reader.controls),
                      lineage_control_binding_projection_sha256=control_projection.hexdigest(),
                      usage={"cumulative_json_bytes_at_manifest_construction": reader.json_bytes,
                             "reserved_final_json_reopen_bytes": sum(item["bytes"] for item in reader.controls.values()),
                             "reserved_output_json_reads_before_first_footer": output_json_allowance,
                             "reserved_remaining_output_json_reads_at_manifest": bound["bytes"] + writer.files["START.json"]["bytes"] + 5 * 1024**2,
                             "cumulative_footer_bytes": reader.footer_bytes, "prior_json_bytes": PRIOR_JSON,
                             "prior_footer_bytes": PRIOR_FOOTER, "trade_body_bytes_read": 0, "native_payload_sha_rehash_bytes": 0},
                      existing_header_bytes_including_descriptor_ledger=existing,
                      external_control_reservation_bytes=EXTERNAL_RESERVE,
                      source_footer_passes_in_this_builder=1, source_final_stat_identity_reopened=True,
                      native_payload_hashes_are_manifest_claims_not_reverified=True,
                      header_accounting_entry_body_hashes_rechecked=False,
                      independent_inventory_native_rehash_and_footer_reopen_required=True,
                      completed_utc=datetime.now(timezone.utc).isoformat())
        manifest = writer.publish("manifest.json", encode(result))
        completion = {"status": result["status"], "manifest": manifest,
                      "final_universe_admitted": False, "analysis_admitted": False, "final_cumulative_json_bytes": 0}
        base = reader.json_bytes + sum(item["bytes"] for item in reader.controls.values()) + sum(item["bytes"] for item in writer.files.values())
        for _ in range(8):
            raw = encode(completion)
            exact = base + 2 * len(raw)  # staging read and final staged-marker read
            if completion["final_cumulative_json_bytes"] == exact: break
            completion["final_cumulative_json_bytes"] = exact
        else: raise Blocked("completion_json_accounting_not_stable")
        writer.publish("COMPLETE.json", raw, final_check=final_inputs, final_json_bytes=exact)
        return result
    except BaseException as exc:
        result.update(status="blocked", error_code=str(exc) if isinstance(exc, Blocked) else "roster_io_or_resource_failure",
                      usage={"cumulative_json_bytes": reader.json_bytes, "cumulative_footer_bytes": reader.footer_bytes,
                             "trade_body_bytes_read": 0, "native_payload_sha_rehash_bytes": 0})
        try: writer.publish("FAILED.json", encode(result), failure=True)
        except BaseException: result["failure_receipt_published"] = False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result


@contextmanager
def process_limits():
    old = resource.getrlimit(resource.RLIMIT_AS); handler = signal.getsignal(signal.SIGALRM)
    value = min(old[0], CAPS["address_space_bytes"]) if old[0] >= 0 else CAPS["address_space_bytes"]
    try:
        resource.setrlimit(resource.RLIMIT_AS, (value, old[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "memory_limit_not_effective")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Blocked("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"])
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, handler)
        resource.setrlimit(resource.RLIMIT_AS, old)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--descriptor-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    require_production_host()
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python")
                and args.output_dir.parent == Path("/mnt/data/runs"), "production_interpreter_or_output_differs")
        require(shared.resource_state()["ram_bytes"]["MemAvailable"] >= CAPS["address_space_bytes"], "available_ram_below_floor")
        with process_limits(): result = run_builder(args.descriptor, args.descriptor_sha256, args.expected_head, args.output_dir)
    except (Blocked, OSError, KeyError):
        result = {"status": "blocked", "error_code": "roster_admission_failed"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "sealed_source_roster_complete_not_inventory_admission" else 2


if __name__ == "__main__":
    raise SystemExit(main())
