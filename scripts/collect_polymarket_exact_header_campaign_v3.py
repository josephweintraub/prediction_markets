#!/usr/bin/env python3
"""Root-admitted, serialized exact RPC clocks; immutable versioned continuation.

No final-sample, history-completeness or cryptographic-header admission. Full RPC
bodies are hashed in memory then discarded. Only four validated fields persist.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import math
import os
from pathlib import Path
import resource
import signal
import socket
import stat
import subprocess
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v2 as scale
from scripts import collect_polymarket_exact_headers_v1 as pilot
from scripts import inventory_polymarket_refresh as inventory
from scripts import build_polymarket_exact_header_roster_v1 as roster_profile

CAPS = {"keys": 40_000_000, "http_attempts": 1_300_000, "method_items": 130_000_000,
        "response_bytes": 2_000_000_000_000, "runtime_seconds": 72 * 3600,
        "individual_response_bytes": 16 * 1024**2, "request_seconds": 30,
        "address_space_bytes": 8 * 1024**3, "minimum_available_ram_bytes": 8 * 1024**3,
        "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0, "active_http": 1,
        "keys_per_chunk": 100_000, "initial_batch_size": 100, "attempts_per_batch": 3,
        "attempts_per_chunk": 3, "chunk_http_attempts": 10_010, "chunk_method_items": 500_000,
        "chunk_response_bytes": 8 * 1024**3, "chunk_runtime_seconds": 3600,
        "chunk_artifact_bytes": 32_000_000, "journal_record_bytes": 384,
        "global_header_bytes": 8_000_000_000, "baseline_bytes": 1_600_000_000,
        "accepted_bytes": 4_800_000_000, "control_bytes": 1_100_000_000,
        "failed_bytes": 500_000_000, "external_control_reservation_bytes": 2 * 1024**2}
SOURCE_FILES = tuple(dict.fromkeys((*scale.SOURCE_FILES, *roster_profile.SOURCE_FILES,
    "scripts/inventory_polymarket_exact_header_keys_v1.py", "tests/test_inventory_polymarket_exact_header_keys_v1.py",
    "scripts/collect_polymarket_exact_header_campaign_v3.py", "tests/test_polymarket_exact_header_campaign_v3.py")))
RUN_ROOT = Path("/mnt/data/runs")
MAX_JSON, MAX_LEDGER, MAX_FILES = 1024**2, 64 * 1024**2, 150_000
ORIGINAL_PILOT_INPUT = {
    "path": "/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_pilot_keys_v1.json",
    "bytes": 38091, "sha256": scale.ORIGINAL_REQUEST_SHA256}
SCALE_STDOUT_ECHO = {
    "path": "/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/header_scale_v2.log",
    "bytes": 1723, "sha256": "1fa7aadb16d893677a10ed00948806d1bef87c6698d63fc826b80ed620e37e3b"}
FINAL_RESERVE = 2 * 1024**2
SUCCESS = "literal_exact_header_campaign_complete_raw_clocks_only"
SCHEMA = "polymarket_exact_header_campaign_admission_v3"
Blocked, require, digest, encode, decode = scale.Blocked, scale.require, scale.digest, scale.json_bytes, scale.decode


def contract():
    return {"schema_version": SCHEMA, "status": "root_exact_header_campaign_admitted", "caps": CAPS,
        "consumer_head": "clean committed HEAD", "run_dir": "fresh literal /mnt/data/runs child",
        "raw_clocks_only": True, "final_sample_admitted": False,
        "inputs": {"inventory_complete": "absolute path/bytes/sha256", "inventory_manifest": "same",
                   "keys": "same", "roster": "same", "scale_complete": "same", "scale_manifest": "same",
                   "scale_contract": "same", "scale_headers": "same", "scale_journal": "same"},
        "private_file": "absolute path/bytes/sha256; content never copied",
        "actual_keys": "integer actual K", "conservative_new_runtime_seconds": "root-admitted projection",
        "prior": {"complete_roster": True, "baseline_cache_excluded": True,
            "original_pilot_input": ORIGINAL_PILOT_INPUT,
            "roots": "fully inventoried immutable roots", "standalone_control_files": "literal controls in mixed roots",
            "files": "path/bytes/sha256/category/chunk entries; categories baseline/accepted/control/failed",
            "rpc": "http_attempts/method_items/response_bytes/elapsed_seconds",
            "campaigns": "run_dir/drain path-byte-SHA entries; every prior campaign root exactly once"}}


def identity(path):
    path = Path(path)
    info = path.lstat()
    require(path.is_absolute() and path.resolve() == path and stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1, "input_not_literal_regular")
    return {"device": info.st_dev, "inode": info.st_ino, "bytes": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns, "mode": info.st_mode}


def bound(path, maximum=CAPS["global_header_bytes"]):
    path = Path(path); before = identity(path)
    require(before["bytes"] <= maximum, "input_byte_limit")
    hasher, count = hashlib.sha256(), 0
    with path.open("rb") as stream:
        while raw := stream.read(min(1024**2, maximum - count + 1)):
            count += len(raw); require(count <= maximum, "input_byte_limit"); hasher.update(raw)
    require(before == identity(path), "input_changed_during_read")
    return {"path": str(path), "bytes": count, "sha256": hasher.hexdigest(), "identity": before}


def reference(value, maximum=MAX_JSON):
    require(type(value) is dict and set(value) == {"path", "bytes", "sha256"}, "reference_schema")
    require(type(value["path"]) is str and type(value["bytes"]) is int and 0 <= value["bytes"] <= maximum
            and type(value["sha256"]) is str and pilot.SHA.fullmatch(value["sha256"]), "reference_invalid")
    actual = bound(Path(value["path"]), maximum)
    require(all(actual[key] == value[key] for key in value), "reference_changed")
    return actual


def read_json(value, maximum=MAX_JSON):
    actual = reference(value, maximum)
    with Path(actual["path"]).open("rb") as stream: raw = stream.read(maximum + 1)
    require(len(raw) == actual["bytes"] and digest(raw) == actual["sha256"]
            and identity(Path(actual["path"])) == actual["identity"], "input_reopen_changed")
    return decode(raw), actual


def ref(value):
    return {key: value[key] for key in ("path", "bytes", "sha256")}


def source_snapshot(head):
    require(type(head) is str and pilot.HEAD.fullmatch(head), "source_head_invalid")
    try:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, stderr=subprocess.DEVNULL, timeout=10).decode().strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=REPO,
                                        stderr=subprocess.DEVNULL, timeout=10).strip()
    except (OSError, subprocess.SubprocessError): raise Blocked("source_unavailable") from None
    require(actual == head and not dirty, "source_not_expected_clean_head")
    files = {}
    for name in SOURCE_FILES:
        value = bound(REPO / name, MAX_JSON)
        raw = scale.git_blob(head, name)
        require(len(raw) == value["bytes"] and digest(raw) == value["sha256"], "source_blob_differs")
        files[name] = {key: value[key] for key in ("bytes", "sha256")}
    return {"head": head, "files": files}


def producer_source(value, source, names, *, hashes_only=False):
    require(type(value) is dict and type(value.get("head")) is str and pilot.HEAD.fullmatch(value["head"]),
            "producer_source_invalid")
    expected = {name: source["files"][name]["sha256"] if hashes_only else source["files"][name] for name in names}
    require(value.get("files") == expected, "producer_consumer_source_drift")
    for name in names:
        raw = scale.git_blob(value["head"], name)
        require(len(raw) == source["files"][name]["bytes"] and digest(raw) == source["files"][name]["sha256"],
                "producer_committed_blob_differs")


def integer(value, maximum, code="counter_invalid"):
    require(type(value) is int and 0 <= value <= maximum, code)
    return value


def elapsed(value, maximum=CAPS["runtime_seconds"]):
    require(type(value) in (float, int) and math.isfinite(value) and 0 <= value <= maximum, "runtime_counter_invalid")
    return value


def key_chunks(path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    parquet = pq.ParquetFile(path)
    require(parquet.schema_arrow == pa.schema([("block_number", pa.int64()), ("expected_block_hash", pa.string())]),
            "required_keys_schema")
    require(0 < parquet.metadata.num_rows <= CAPS["keys"], "required_keys_count")
    previous, chunk = 0, []
    for batch in parquet.iter_batches(batch_size=CAPS["keys_per_chunk"]):
        for item in batch.to_pylist():
            number, expected = item["block_number"], item["expected_block_hash"]
            require(type(number) is int and previous < number <= 2**63 - 1, "required_keys_not_sorted_unique")
            require(expected is None or type(expected) is str and pilot.HASH.fullmatch(expected), "expected_hash_invalid")
            previous = number
            chunk.append({"block_number": number, "block_hash": expected.lower() if expected else None})
            if len(chunk) == CAPS["keys_per_chunk"]:
                yield chunk; chunk = []
    if chunk: yield chunk


def validate_inputs(value, source, actual_keys):
    names = {"inventory_complete", "inventory_manifest", "keys", "roster", "scale_complete", "scale_manifest",
             "scale_contract", "scale_headers", "scale_journal"}
    require(type(value) is dict and set(value) == names, "input_roster_invalid")
    bindings = {name: reference(item, CAPS["baseline_bytes"] if name == "keys" else
                 MAX_LEDGER if name == "roster" else 128_000_000 if name in {"scale_headers", "scale_journal"} else MAX_JSON)
                for name, item in value.items()}
    def load(name): return read_json(value[name], MAX_LEDGER if name == "roster" else MAX_JSON)[0]
    marker, manifest = load("inventory_complete"), load("inventory_manifest")
    root = Path(bindings["inventory_manifest"]["path"]).parent
    require(Path(bindings["inventory_complete"]["path"]) == root / "COMPLETE.json" and
            marker.get("schema_version") == "polymarket_exact_header_key_inventory_completion_v1" and
            marker.get("status") == manifest.get("status") == "literal_header_key_inventory_complete" and
            marker.get("manifest") == {"path": "manifest.json", **{key: bindings["inventory_manifest"][key] for key in ("bytes", "sha256")}},
            "inventory_not_complete")
    require(manifest.get("schema_version") == "polymarket_exact_header_key_inventory_v1" and
            manifest.get("literal_roster_only") is True and manifest.get("final_universe_admitted") is False and
            manifest.get("required_keys") == actual_keys and all(count == 0 for count in manifest.get("failure_counts", {}).values())
            and manifest.get("global_conflicting_hash_blocks") == 0, "inventory_contract_invalid")
    require(manifest.get("required_keys_parquet") == {"path": "required_keys.parquet",
                **{key: bindings["keys"][key] for key in ("bytes", "sha256")}}
            and Path(bindings["keys"]["path"]) == root / "required_keys.parquet"
            and manifest.get("roster", {}).get("path") == bindings["roster"]["path"]
            and manifest.get("roster", {}).get("sha256") == bindings["roster"]["sha256"], "inventory_input_binding_differs")
    producer_source(manifest["source"], source, ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
        "scripts/inventory_polymarket_exact_header_keys_v1.py", "tests/test_inventory_polymarket_exact_header_keys_v1.py"))
    count = sum(len(keys) for keys in key_chunks(bindings["keys"]["path"]))
    require(count == actual_keys, "required_keys_count_differs")
    sm, sc, ct = load("scale_manifest"), load("scale_complete"), load("scale_contract")
    scale_root = Path(bindings["scale_manifest"]["path"]).parent
    require(sc.get("schema_version") == "polymarket_exact_header_scale_completion_v2" and
            sc.get("status") == sm.get("status") == "literal_compact_scale_pilot_complete" and
            sc.get("manifest") == {"path": "manifest.json", **{key: bindings["scale_manifest"][key] for key in ("bytes", "sha256")}}
            and Path(bindings["scale_complete"]["path"]) == scale_root / "COMPLETE.json", "scale_not_accepted")
    require(sm.get("schema_version") == "polymarket_exact_header_scale_pilot_v2" and
            sm.get("requested_keys") == sm.get("accepted_keys") == 1000 and sm.get("matches_accepted_prior") is True
            and sm.get("missing_keys") == sm.get("fallback_rows") == 0 and sm.get("exact_key_coverage") is True
            and ct.get("schema_version") == "polymarket_exact_header_scale_contract_v2" and ct.get("caps") == scale.CAPS
            and ct.get("request", {}).get("sha256") == ORIGINAL_PILOT_INPUT["sha256"],
            "scale_contract_invalid")
    producer_source(ct["source"], source, scale.SOURCE_FILES, hashes_only=True)
    for name, field in (("scale_headers", "headers"), ("scale_journal", "journal"), ("scale_contract", "contract")):
        require(sm.get(field) == {"path": Path(bindings[name]["path"]).name,
                **{key: bindings[name][key] for key in ("bytes", "sha256")}}
                and Path(bindings[name]["path"]).parent == scale_root, "scale_artifact_binding_differs")
    import pyarrow.parquet as pq
    rows = pq.read_table(bindings["scale_headers"]["path"]).to_pylist()
    require(len(rows) == 1000 and all(type(row["block_hash"]) is bytes and len(row["block_hash"]) == 32
            and type(row["parent_hash"]) is bytes and len(row["parent_hash"]) == 32 for row in rows), "scale_header_schema")
    projected = [{**row, "block_hash": "0x" + row["block_hash"].hex(), "parent_hash": "0x" + row["parent_hash"].hex()}
                 for row in rows]
    pilot.validate_clock(projected)
    require(digest(scale.projection_bytes(projected)) == sm.get("header_projection_sha256"), "scale_projection_differs")
    usage = sm.get("usage", {})
    delta = elapsed(usage.get("elapsed_seconds"), 1200) - elapsed(usage.get("inherited_rpc", {}).get("elapsed_seconds"), 1200)
    require(delta > 0 and usage.get("http_attempts", 0) > usage.get("inherited_rpc", {}).get("http_attempts", 0), "scale_usage_missing")
    return bindings, {"seconds_per_key": delta / 1000, "bytes_per_key": bindings["scale_headers"]["bytes"] / 1000}


def owner_identity():
    raw = Path("/proc/self/stat").read_text()
    return {"hostname": socket.gethostname(), "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "pid": os.getpid(), "birth_ticks": int(raw[raw.rfind(")") + 2:].split()[19])}


def owner_state(owner):
    try:
        require(type(owner) is dict and set(owner) == {"hostname", "boot_id", "pid", "birth_ticks"}
                and type(owner["hostname"]) is str and 0 < len(owner["hostname"]) <= 255
                and owner["hostname"].strip() == owner["hostname"] and "\x00" not in owner["hostname"]
                and type(owner["boot_id"]) is str and str(uuid.UUID(owner["boot_id"])) == owner["boot_id"]
                and uuid.UUID(owner["boot_id"]).int > 0 and type(owner["pid"]) is int and 0 < owner["pid"] <= 2**31 - 1
                and type(owner["birth_ticks"]) is int and 0 < owner["birth_ticks"] <= 2**64 - 1,
                "owner_process_schema_invalid")
        if owner["hostname"] != socket.gethostname(): return "unknown"
        current_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        require(str(uuid.UUID(current_boot)) == current_boot and uuid.UUID(current_boot).int > 0, "current_boot_identity_invalid")
        if owner["boot_id"] != current_boot: return "dead"
        path = Path(f"/proc/{integer(owner['pid'], 2**31 - 1)}/stat")
        try: raw = path.read_text()
        except FileNotFoundError: return "dead"
        return "live" if int(raw[raw.rfind(")") + 2:].split()[19]) == owner["birth_ticks"] else "dead"
    except (OSError, KeyError, ValueError, TypeError, IndexError, Blocked): return "unknown"


def journal_usage(path, key_context=None):
    starts, finishes, last, last_time = {}, {}, 0, 0
    with Path(path).open("rb") as stream:
        for raw in stream:
            require(len(raw) <= CAPS["journal_record_bytes"] and raw.endswith(b"\n"), "journal_record_invalid")
            item = decode(raw); call = item.get("i")
            require(type(call) is int and 0 < call <= CAPS["http_attempts"], "journal_call_invalid")
            if item.get("e") == "s":
                require(set(item) == {"e", "i", "k", "n", "j", "b", "t", "h"}, "journal_start_schema")
                require(call not in starts and call > last and item.get("n", -1) >= 0, "journal_start_invalid")
                require(not starts or last in finishes, "journal_overlapping_serial_requests")
                integer(item.get("b"), CAPS["individual_response_bytes"])
                integer(item.get("n"), CAPS["initial_batch_size"])
                require(item["b"] > 0 and type(item["t"]) is int and item["t"] >= last_time
                        and type(item["j"]) is int and 0 < item["j"] <= CAPS["method_items"]
                        and type(item["h"]) is str and pilot.SHA.fullmatch(item["h"]), "journal_start_invalid")
                if item["n"] == 0:
                    require(item["k"] == -1 and item["h"] == digest(encode({"jsonrpc": "2.0", "id": item["j"],
                        "method": "eth_chainId", "params": []})), "journal_chain_request_differs")
                else:
                    require(type(item["k"]) is int and 0 <= item["k"] < CAPS["keys"] and
                            item["k"] + item["n"] <= CAPS["keys"] and item["j"] + item["n"] - 1 <= CAPS["method_items"],
                            "journal_key_locator_invalid")
                    if key_context is not None:
                        offset, keys = key_context
                        require(offset <= item["k"] and item["k"] + item["n"] <= offset + len(keys), "journal_key_locator_invalid")
                        requests = [{"jsonrpc": "2.0", "id": item["j"] + index, "method": "eth_getBlockByNumber",
                                     "params": [hex(key["block_number"]), False]}
                                    for index, key in enumerate(keys[item["k"] - offset:item["k"] - offset + item["n"]])]
                        require(item["h"] == digest(encode(requests)), "journal_request_digest_differs")
                starts[call], last = item, call
            else:
                require(item.get("e") == "f" and call in starts and call not in finishes, "journal_finish_invalid")
                require(set(item) == {"e", "i", "b", "z", "h", "x", "p", "r", "s", "t"}, "journal_finish_schema")
                integer(item.get("b"), starts[call]["b"])
                integer(item.get("z"), item["b"])
                require(item["r"] in {"validated", "failed", "split"} and type(item["t"]) is int
                        and item["t"] >= starts[call]["t"] and (item["s"] is None or
                        type(item["s"]) is int and 100 <= item["s"] <= 599)
                        and type(item["x"]) is str and pilot.SHA.fullmatch(item["x"]), "journal_finish_invalid")
                if item["h"] is None:
                    require(item["b"] == starts[call]["b"] and item["r"] != "validated", "journal_unknown_body_charge_refund")
                else:
                    require(type(item["h"]) is str and pilot.SHA.fullmatch(item["h"]) and
                            item["h"] == item["x"] and item["b"] == item["z"], "journal_complete_body_proof_invalid")
                if item["r"] == "validated":
                    require(item["s"] == 200 and (item["p"] is None if starts[call]["n"] == 0 else
                        type(item["p"]) is str and pilot.SHA.fullmatch(item["p"])), "journal_validated_projection_invalid")
                else:
                    require(item["p"] is None and (item["r"] != "split" or starts[call]["n"] > 1), "journal_failed_projection_invalid")
                finishes[call] = item
            last_time = item["t"]
    return {"http_attempts": len(starts), "method_items": sum(max(1, item["n"]) for item in starts.values()),
            "response_bytes": sum(finishes[call]["b"] if call in finishes else item["b"] for call, item in starts.items())}, starts, finishes


def key_chunk_at(path, chunk):
    """Read only the row groups intersecting one already validated key chunk."""
    import pyarrow.parquet as pq
    file = pq.ParquetFile(path)
    first, end, position, values = chunk * CAPS["keys_per_chunk"], (chunk + 1) * CAPS["keys_per_chunk"], 0, []
    for index in range(file.metadata.num_row_groups):
        count = file.metadata.row_group(index).num_rows
        if position < end and position + count > first:
            table = file.read_row_group(index)
            start = max(first - position, 0)
            values.extend(table.slice(start, min(count - start, end - max(position, first))).to_pylist())
        position += count
        if position >= end: break
    require(values, "prior_chunk_outside_key_sample")
    return [{"block_number": row["block_number"], "block_hash": row["expected_block_hash"].lower()
             if row["expected_block_hash"] else None} for row in values]


def standalone_operational_control(path):
    """Do not let renamed collection evidence escape whole-root admission."""
    require(identity(path)["bytes"] <= MAX_LEDGER, "standalone_control_byte_limit")
    with path.open("rb") as stream: raw = stream.read(MAX_LEDGER + 1)
    require(len(raw) <= MAX_LEDGER and not raw.startswith(b"PAR1"), "standalone_protocol_artifact_forbidden")
    candidates = []
    try: candidates.append(decode(raw))
    except Blocked:
        # Operational stdout/stderr may be ordinary text. Still recognize a
        # renamed JSONL request journal one bounded line at a time.
        for line in raw.splitlines():
            if len(line) > MAX_JSON: continue
            try: candidates.append(decode(line))
            except Blocked: continue
    protocol_prefixes = ("polymarket_exact_header_owner_", "polymarket_exact_header_campaign_v",
        "polymarket_exact_header_chunk_", "polymarket_exact_header_pilot_result_", "polymarket_exact_header_scale_pilot_",
        "polymarket_exact_header_key_inventory_", "polymarket_exact_header_roster_build_", "polymarket_exact_header_key_roster_")
    for item in candidates:
        if type(item) is list:
            require(not any(type(row) is dict and ("block_number" in row or "jsonrpc" in row) for row in item),
                    "standalone_protocol_artifact_forbidden")
        if type(item) is not dict: continue
        schema = item.get("schema_version", "")
        require(not (type(schema) is str and schema.startswith(protocol_prefixes))
                and not (type(item.get("e")) is str and item["e"] in {"s", "f"} and "i" in item)
                and not (type(item.get("event")) is str and item["event"] in {"START", "FINISH", "start", "finish"} and "call" in item)
                and not ("requests" in item and type(item["requests"]) is list
                         and any(type(row) is dict and "jsonrpc" in row for row in item["requests"])),
                "standalone_protocol_artifact_forbidden")


def validate_prior(value, output, source, inputs):
    require(type(value) is dict and set(value) == {"complete_roster", "baseline_cache_excluded", "roots",
            "standalone_control_files", "original_pilot_input", "files", "rpc", "campaigns"} and value["complete_roster"] is True
            and value["baseline_cache_excluded"] is True, "prior_ledger_schema")
    require(value["original_pilot_input"] == ORIGINAL_PILOT_INPUT, "original_pilot_input_not_pinned")
    original = reference(value["original_pilot_input"], MAX_JSON)
    original_keys, original_binding = pilot.read_requests(Path(original["path"]), original["sha256"])
    ct = read_json(ref(inputs["scale_contract"]))[0]
    require(len(original_keys) == 1000 and ct.get("request") == original_binding, "original_pilot_scale_binding_differs")
    read_headers(Path(inputs["scale_headers"]["path"]), original_keys)
    roots, standalone, entries = value["roots"], value["standalone_control_files"], value["files"]
    require(type(roots) is list and len(roots) <= 512 and type(standalone) is list and
            type(entries) is list and 0 < len(entries) <= MAX_FILES, "prior_roster_size")
    actual, directories = set(), []
    for name in roots:
        path = Path(name)
        require(path.is_absolute() and path.resolve() == path and path.is_dir() and RUN_ROOT in path.parents
                and not path.is_symlink() and output not in path.parents and path not in output.parents and output != path
                and all(path != old and path not in old.parents and old not in path.parents for old in directories), "prior_root_invalid")
        directories.append(path)
        for item in path.rglob("*"):
            require(not item.is_symlink() and (item.is_dir() or item.is_file()), "prior_tree_invalid")
            if item.is_file(): actual.add(str(item))
            require(len(actual) <= MAX_FILES, "prior_roster_size")
    for name in standalone:
        path = Path(name)
        require(str(path) not in actual and path.is_absolute() and path.resolve() == path and RUN_ROOT in path.parents
                and output not in path.parents and path not in output.parents and path != output, "standalone_control_invalid")
        require(path.name not in {"OWNER.json", "manifest.json", "RESULT.json", "FAILED.json", "FAILURE.json",
                "COMPLETE.json", "CONTRACT.json", "artifact_inventory.json", "rpc.jsonl", "REQUESTS.jsonl", "roster.json"}
                and not path.name.endswith((".START.json", ".FINISH.json", ".parquet", ".raw", ".body", ".body.gz"))
                and not {"chunks", "responses"}.intersection(path.parts), "standalone_protocol_artifact_forbidden")
        if str(path) == SCALE_STDOUT_ECHO["path"]:
            # One immutable, known CLI manifest echo is control evidence, not
            # another collection root. Its accepted producer root must still
            # be fully inventoried and charged through the ordinary gates.
            echoed, _ = read_json(SCALE_STDOUT_ECHO)
            require(Path(inputs["scale_manifest"]["path"]).parent in directories,
                    "fixed_scale_stdout_scale_root_missing")
            require(echoed == read_json(ref(inputs["scale_manifest"]))[0],
                    "fixed_scale_stdout_manifest_differs")
        else:
            standalone_operational_control(path)
        actual.add(str(path))
    require(original["path"] not in standalone and RUN_ROOT in Path(original["path"]).parents,
            "original_pilot_input_misclassified_or_outside_run_root")
    actual.add(original["path"])
    files, totals = {}, dict.fromkeys(("baseline", "accepted", "control", "failed"), 0)
    for item in entries:
        require(type(item) is dict and set(item) == {"path", "bytes", "sha256", "category", "chunk"}
                and item["category"] in totals and (item["chunk"] is None or type(item["chunk"]) is int and 0 <= item["chunk"] < 400),
                "prior_file_schema")
        require(item["path"] in actual and item["path"] not in files, "prior_file_duplicate_or_missing")
        require(item["path"] not in standalone or item["category"] == "control", "standalone_control_category")
        require(item["path"] != SCALE_STDOUT_ECHO["path"] or
                item["category"] == "control" and item["chunk"] is None, "fixed_scale_stdout_category_invalid")
        require(item["path"] != original["path"] or item["category"] == "baseline" and item["chunk"] is None,
                "original_pilot_input_misclassified_or_outside_run_root")
        capture = reference(ref(item), CAPS["global_header_bytes"])
        files[item["path"]] = capture | {"category": item["category"], "chunk": item["chunk"]}
        totals[item["category"]] += item["bytes"]
    require(set(files) == actual and len(standalone) == len(set(standalone)), "prior_roster_incomplete")
    require(all(totals[key] <= CAPS[key + "_bytes"] for key in totals)
            and sum(totals.values()) <= CAPS["global_header_bytes"], "prior_disk_allocation")
    rpc = value["rpc"]
    require(type(rpc) is dict and set(rpc) == {"http_attempts", "method_items", "response_bytes", "elapsed_seconds"}, "prior_rpc_schema")
    for key in ("http_attempts", "method_items", "response_bytes"): integer(rpc[key], CAPS[key])
    elapsed(rpc["elapsed_seconds"])
    minima = dict.fromkeys(rpc, 0)
    chunk_usage, sealed, attempts = {}, {}, {}
    campaigns = value["campaigns"]
    require(type(campaigns) is list and len(campaigns) <= 1200, "prior_campaign_count")
    campaign_roots = set()
    for campaign in campaigns:
        require(type(campaign) is dict and set(campaign) == {"run_dir", "drain"}, "prior_campaign_schema")
        root = Path(campaign["run_dir"])
        require(root in directories and str(root) not in campaign_roots, "prior_campaign_not_rostered")
        campaign_roots.add(str(root))
        owner_path = str(root / "OWNER.json")
        require(owner_path in files, "prior_owner_missing")
        owner = read_json(ref(files[owner_path]))[0]
        require(owner.get("schema_version") == "polymarket_exact_header_owner_v3" and owner.get("run_dir") == str(root)
                and owner.get("caps") == CAPS and owner.get("inputs") == inputs and owner.get("raw_clocks_only") is True
                and owner.get("final_sample_admitted") is False, "prior_owner_schema_or_inputs")
        previous_admission = owner.get("admission", {})
        require(type(previous_admission) is dict and previous_admission.get("path") in files and
                ref(previous_admission) == ref(files[previous_admission["path"]]), "prior_root_admission_missing")
        require(owner_state(owner.get("process", {})) == "dead", "prior_owner_live_or_unknown")
        require(owner.get("source", {}).get("files") == source["files"], "prior_campaign_source_drift")
        producer_source(owner["source"], source, SOURCE_FILES)
        drain, drainage = read_json(campaign["drain"])
        require(drainage["path"] in files and drain.get("schema_version") == "polymarket_exact_header_drain_v3"
                and drain.get("run_dir") == str(root) and drain.get("owner_sha256") == files[owner_path]["sha256"]
                and drain.get("owner_identity_verified_dead") is True and drain.get("no_owned_workers_or_transfers") is True,
                "prior_drain_not_accepted")
        minima["elapsed_seconds"] += elapsed(drain.get("runtime_charge_seconds"))
        times = drain.get("chunk_runtime_seconds")
        require(type(times) is dict and all(type(key) is str and key.isdecimal() for key in times), "prior_chunk_drain_times_missing")
        root_chunks = set()
        for path, file in files.items():
            if root not in Path(path).parents: continue
            if Path(path).name == "rpc.jsonl":
                chunk = file["chunk"]
                keys = key_chunk_at(inputs["keys"]["path"], chunk) if chunk is not None else None
                usage, starts, finishes = journal_usage(path, (chunk * CAPS["keys_per_chunk"], keys) if keys is not None else None)
                for key, count in usage.items(): minima[key] += count
                if chunk is not None:
                    root_chunks.add(chunk)
                    aggregate = chunk_usage.setdefault(chunk, dict.fromkeys(("http_attempts", "method_items", "response_bytes", "elapsed_seconds"), 0))
                    for key, count in usage.items(): aggregate[key] += count
                    minimum_time = max(0, max(item["t"] for item in [*starts.values(), *finishes.values()]) - min(item["t"] for item in starts.values())) / 1000 if starts else 0
                    require(str(chunk) in times and elapsed(times[str(chunk)], CAPS["chunk_runtime_seconds"]) >= minimum_time,
                            "prior_chunk_drain_time_understated")
                    aggregate["elapsed_seconds"] += times[str(chunk)]
                    attempts.setdefault(chunk, set()).add(str(Path(path).parent))
                    marker = Path(path).parent / "COMPLETE.json"
                    if str(marker) in files:
                        require(chunk not in sealed and len(starts) == len(finishes), "sealed_chunk_duplicate_or_open_call")
                        manifest_file = str(marker.parent / "manifest.json")
                        require(manifest_file in files, "prior_chunk_manifest_missing")
                        chunk_manifest = read_json(ref(files[manifest_file]))[0]
                        require(aggregate["elapsed_seconds"] >= elapsed(chunk_manifest.get("active_seconds_charged"),
                                CAPS["chunk_runtime_seconds"]), "prior_chunk_drain_time_understated")
                        sealed[chunk] = {"root": str(Path(path).parent), "journal": path, "source_root": str(root)}
        require(set(times) == {str(chunk) for chunk in root_chunks} and
                sum(times.values()) <= drain["runtime_charge_seconds"], "prior_chunk_drain_time_roster")
    for root in directories:
        owner = str(root / "OWNER.json")
        if owner in files:
            value_owner = read_json(ref(files[owner]))[0]
            if value_owner.get("schema_version") == "polymarket_exact_header_owner_v3":
                require(str(root) in campaign_roots, "prior_campaign_drain_missing")
        if str(root) in campaign_roots: continue
        summaries = [files[str(root / name)] for name in ("RESULT.json", "manifest.json", "FAILED.json") if str(root / name) in files]
        recognized = False
        root_minima = dict.fromkeys(rpc, 0)
        for item in summaries:
            summary = read_json(ref(item))[0]
            schema = summary.get("schema_version")
            if schema == "polymarket_exact_header_pilot_result_v1":
                u = summary.get("usage", {}); values = {"http_attempts": u.get("rpc_batch_attempts"),
                    "method_items": u.get("rpc_method_items"), "response_bytes": u.get("charged_response_bytes"), "elapsed_seconds": u.get("elapsed_seconds")}
            elif schema == "polymarket_exact_header_scale_pilot_v2":
                u = summary.get("usage", {}); inherited = u.get("inherited_rpc", {})
                values = {key: u.get(key, -1) - inherited.get(key, 0) for key in rpc}
            elif schema == "polymarket_exact_header_key_inventory_v1":
                producer_source(summary.get("source"), source, ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                    "scripts/inventory_polymarket_exact_header_keys_v1.py", "tests/test_inventory_polymarket_exact_header_keys_v1.py"))
                require(all(file["category"] == "baseline" and file["chunk"] is None
                    for path, file in files.items() if root in Path(path).parents), "inventory_profile_category_invalid")
                root_minima["elapsed_seconds"] = max(root_minima["elapsed_seconds"], 2400,
                    elapsed(summary.get("usage", {}).get("elapsed_seconds", 0)))
                continue  # Projection inventory performs no RPC, but costs runtime.
            elif schema == "polymarket_exact_header_roster_build_v1":
                require(summary.get("status") == "sealed_source_roster_complete_not_inventory_admission"
                        and summary.get("caps") == roster_profile.CAPS and summary.get("literal_source_generation_only") is True
                        and summary.get("final_universe_admitted") is False and summary.get("analysis_admitted") is False,
                        "roster_profile_not_sealed")
                producer_source(summary.get("source"), source, roster_profile.SOURCE_FILES, hashes_only=True)
                expected = {str(root / name) for name in ("START.json", "roster.json", "manifest.json", "COMPLETE.json")}
                require({path for path in files if root in Path(path).parents} == expected and
                        all(files[path]["category"] == "baseline" and files[path]["chunk"] is None for path in expected),
                        "roster_profile_artifact_roster_or_category")
                marker = read_json(ref(files[str(root / "COMPLETE.json")]))[0]
                start = read_json(ref(files[str(root / "START.json")]))[0]
                require(marker.get("status") == summary["status"] and marker.get("manifest") ==
                    {"path": "manifest.json", **{key: item[key] for key in ("bytes", "sha256")}}
                    and marker.get("final_universe_admitted") is False and marker.get("analysis_admitted") is False
                    and start.get("schema_version") == schema and start.get("source") == summary["source"]
                    and start.get("caps") == roster_profile.CAPS, "roster_profile_marker_or_start_differs")
                roster_item = files[str(root / "roster.json")]
                require(summary.get("roster") == {"path": "roster.json", **{key: roster_item[key] for key in ("bytes", "sha256")}},
                        "roster_profile_output_hash_differs")
                roster_value = read_json(ref(roster_item), MAX_LEDGER)[0]
                require(type(roster_value) is dict and set(roster_value) == {"schema_version", "files"}
                        and roster_value["schema_version"] == "polymarket_exact_header_key_roster_v1"
                        and type(roster_value["files"]) is list and 0 < len(roster_value["files"]) <= MAX_FILES
                        and summary.get("source_files") == len(roster_value["files"]), "roster_profile_output_schema")
                root_minima["elapsed_seconds"] = max(root_minima["elapsed_seconds"], 1800)
                continue  # Exact reviewed profile has no HTTP/RPC; no payload is adopted.
            else:
                raise Blocked("prior_summary_schema_unknown")
            recognized = True
            for key, count in values.items():
                root_minima[key] = max(root_minima[key], elapsed(count) if key == "elapsed_seconds" else integer(count, CAPS[key]))
        for key in minima: minima[key] += root_minima[key]
        if any((Path(path).name in {"rpc.jsonl", "REQUESTS.jsonl"} or
                Path(path).name.endswith((".START.json", ".FINISH.json"))) and root in Path(path).parents for path in files):
            require(recognized, "prior_rpc_summary_missing")
    require(all(rpc[key] >= minima[key] for key in rpc), "prior_rpc_below_durable_minima")
    accepted_paths = {str(Path(item["root"]) / "headers.parquet"): chunk for chunk, item in sealed.items()}
    for path, item in files.items():
        if item["category"] == "accepted":
            require(path in accepted_paths and item["chunk"] == accepted_paths[path], "accepted_header_not_sealed")
        for root in campaign_roots:
            if Path(root) not in Path(path).parents: continue
            parts = Path(path).relative_to(root).parts
            if parts[0] == "chunks":
                require(len(parts) == 4 and len(parts[1]) == 7 and parts[1][0] == "c" and parts[1][1:].isdecimal()
                        and len(parts[2]) == 3 and parts[2][0] == "a" and parts[2][1:].isdecimal()
                        and item["chunk"] == int(parts[1][1:]), "prior_chunk_path_or_category")
                require(item["category"] == ("accepted" if path in accepted_paths else "failed")
                        if parts[-1] == "headers.parquet" else item["category"] == "control", "prior_chunk_path_or_category")
            else:
                require(item["chunk"] is None and item["category"] == "control", "prior_campaign_control_category")
    for chunk, usage in chunk_usage.items():
        require(len(attempts[chunk]) <= CAPS["attempts_per_chunk"] and all(usage[key] <= CAPS["chunk_" + key]
                for key in ("http_attempts", "method_items", "response_bytes")) and usage["elapsed_seconds"] <= CAPS["chunk_runtime_seconds"]
                and sum(item["bytes"] for item in files.values() if item["chunk"] == chunk) <= CAPS["chunk_artifact_bytes"], "prior_chunk_cap")
    return {"files": files, "bytes": totals, "rpc": rpc, "sealed": sealed, "attempts": attempts, "chunk_usage": chunk_usage}


class Budget:
    def __init__(self, output, prior, admission_bytes, started):
        self.output, self.prior, self.started = output, prior, started
        self.used = dict.fromkeys(prior["bytes"], 0); self.used["control"] = admission_bytes
        self.rpc = dict(prior["rpc"]); self.files, self.hashers = {}, {}
        self.chunk = None; self.chunk_started = None; self.chunk_base = None

    def remaining(self): return CAPS["runtime_seconds"] - self.prior["rpc"]["elapsed_seconds"] - (time.monotonic() - self.started)

    def check(self, extra=0, category="control", *, failure=False):
        if not failure:
            require(self.remaining() > 0, "runtime_limit")
            if self.chunk is not None: require(self.chunk_elapsed() < CAPS["chunk_runtime_seconds"], "chunk_runtime_limit")
        totals = {key: self.prior["bytes"][key] + self.used[key] for key in self.used}; totals[category] += extra
        totals["control"] += CAPS["external_control_reservation_bytes"] + (0 if failure else FINAL_RESERVE)
        require(all(count <= CAPS[key + "_bytes"] for key, count in totals.items()) and sum(totals.values()) <= CAPS["global_header_bytes"], "header_disk_allocation")
        require(inventory.resource_state(self.output)["disk_bytes"]["free"] >= CAPS["minimum_free_bytes"] + extra
                + CAPS["external_control_reservation_bytes"] + (0 if failure else FINAL_RESERVE), "disk_floor")
        if self.chunk is not None:
            amount = sum(item["bytes"] for item in [*self.prior["files"].values(), *self.files.values()] if item["chunk"] == self.chunk)
            require(amount + extra <= CAPS["chunk_artifact_bytes"], "chunk_artifact_limit")

    def chunk_elapsed(self):
        return self.chunk_base["elapsed_seconds"] + time.monotonic() - self.chunk_started

    def write(self, relative, raw, category="control", *, failure=False):
        self.check(len(raw), category, failure=failure)
        path = self.output / relative
        require(relative not in self.files and not path.exists() and not path.is_symlink(), "artifact_exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.used[category] += len(raw)
        file_chunk = self.chunk if Path(relative).parts[0] == "chunks" else None
        self.files[relative] = {"bytes": len(raw), "sha256": digest(raw), "category": category, "chunk": file_chunk}
        with path.open("xb") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write"); stream.flush(); os.fsync(stream.fileno())
        require(path.read_bytes() == raw, "artifact_reopen_changed")
        return {"path": relative, "bytes": len(raw), "sha256": digest(raw)}

    def append(self, relative, item, *, failure=False):
        raw = encode(item); require(len(raw) <= CAPS["journal_record_bytes"], "journal_record_byte_limit")
        self.check(len(raw), failure=failure)
        path = self.output / relative
        if relative not in self.files:
            require(not path.exists() and not path.is_symlink(), "journal_exists")
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb"): pass
            self.files[relative] = {"bytes": 0, "sha256": digest(b""), "category": "control", "chunk": self.chunk}
            self.hashers[relative] = hashlib.sha256()
        require(identity(path)["bytes"] == self.files[relative]["bytes"], "journal_size_changed")
        self.used["control"] += len(raw); self.hashers[relative].update(raw)
        self.files[relative].update(bytes=self.files[relative]["bytes"] + len(raw), sha256=self.hashers[relative].hexdigest())
        with path.open("ab") as stream:
            require(stream.write(raw) == len(raw), "journal_short_write"); stream.flush(); os.fsync(stream.fileno())

    def reopen(self):
        paths = {str(path.relative_to(self.output)) for path in self.output.rglob("*") if path.is_file()}
        require(paths == set(self.files), "artifact_roster_changed")
        for relative, expected in self.files.items():
            actual = bound(self.output / relative)
            require(all(actual[key] == expected[key] for key in ("bytes", "sha256")), "artifact_changed")

    def seal(self, name, value, external):
        pending = str(Path(name).parent / ("." + Path(name).name + ".pending"))
        self.write(pending, encode(value)); self.reopen()
        for directory in sorted({self.output, (self.output / pending).parent}, key=str): scale.fsync_directory(directory)
        external(); self.reopen(); self.check()
        scale.atomic_publish(self.output / pending, self.output / name)
        self.files[name] = self.files.pop(pending)

    def usage(self):
        return {**self.rpc, "elapsed_seconds": self.prior["rpc"]["elapsed_seconds"] + time.monotonic() - self.started,
                "prior_rpc": self.prior["rpc"], "new_retained_bytes": self.used,
                "external_control_reservation_bytes": CAPS["external_control_reservation_bytes"],
                "final_control_reservation_bytes": FINAL_RESERVE}


@contextmanager
def request_deadline(seconds):
    previous = signal.getsignal(signal.SIGALRM)
    timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()
    try:
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Blocked("request_deadline")))
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, previous)
        if timer[0] > 0: signal.setitimer(signal.ITIMER_REAL, max(.000001, timer[0] - (time.monotonic() - started)), timer[1])


class RPC:
    def __init__(self, endpoint, budget, transport, journal, key_offset=0):
        self.endpoint, self.budget, self.transport, self.journal, self.key_offset = endpoint, budget, transport, journal, key_offset
        self.batch_size = CAPS["initial_batch_size"]

    def call(self, keys=None, offset=0, *, terminal=False):
        b = self.budget; n = 0 if keys is None else len(keys)
        for attempt in range(CAPS["attempts_per_batch"]):
            b.check()
            if hasattr(b, "before_request"): b.before_request()
            counts = {"http_attempts": 1, "method_items": max(1, n)}
            for key, extra in counts.items():
                require(b.rpc[key] + extra + (0 if terminal else 1) <= CAPS[key], key + "_limit")
                if b.chunk is not None:
                    require(b.chunk_base[key] + b.rpc[key] - b.chunk_start_rpc[key] + extra <= CAPS["chunk_" + key], "chunk_" + key + "_limit")
            allowance = min(CAPS["individual_response_bytes"], CAPS["response_bytes"] - b.rpc["response_bytes"])
            if b.chunk is not None:
                allowance = min(allowance, CAPS["chunk_response_bytes"] - b.chunk_base["response_bytes"]
                                - b.rpc["response_bytes"] + b.chunk_start_rpc["response_bytes"])
            require(allowance > 0, "response_bytes_limit")
            call, first_id = b.rpc["http_attempts"] + 1, b.rpc["method_items"] + 1
            requests = [{"jsonrpc": "2.0", "id": first_id + index,
                "method": "eth_chainId" if keys is None else "eth_getBlockByNumber",
                "params": [] if keys is None else [hex(key["block_number"]), False]}
                for index, key in enumerate([None] if keys is None else keys)]
            payload = requests[0] if keys is None else requests
            start = {"e": "s", "i": call, "k": -1 if keys is None else self.key_offset + offset,
                     "n": n, "j": first_id, "b": allowance, "t": int(time.monotonic() * 1000), "h": digest(encode(payload))}
            b.append(self.journal, start)
            b.rpc["http_attempts"] += 1; b.rpc["method_items"] += max(1, n)
            try:
                with request_deadline(min(CAPS["request_seconds"], b.remaining(),
                    CAPS["chunk_runtime_seconds"] - b.chunk_elapsed() if b.chunk is not None else b.remaining())):
                    reply = self.transport(self.endpoint, payload, byte_limit=allowance,
                                           timeout=min(CAPS["request_seconds"], b.remaining()))
            except BaseException as exc:
                reply = {"raw": b"", "charged_bytes": allowance, "complete": False, "status": None,
                         "error_code": "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "transport_failure", "proven_oversize": False}
            if not (type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance
                    and type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance
                    and type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool):
                observed = reply.get("raw", b"") if type(reply) is dict else b""
                observed = observed if type(observed) is bytes and len(observed) <= allowance else b""
                reply = {"raw": observed, "charged_bytes": allowance, "complete": False, "status": None,
                         "error_code": "transport_contract", "proven_oversize": False}
            if reply["complete"] is False:
                reply["charged_bytes"] = allowance
            elif reply["charged_bytes"] != len(reply["raw"]):
                reply.update(charged_bytes=allowance, complete=False, error_code="transport_contract", proven_oversize=False)
            b.rpc["response_bytes"] += reply["charged_bytes"]
            finish = {"e": "f", "i": call, "b": reply["charged_bytes"], "z": len(reply["raw"]),
                      "h": digest(reply["raw"]) if reply.get("complete") else None,
                      "x": digest(reply["raw"]), "p": None, "r": "failed", "s": reply.get("status"), "t": int(time.monotonic() * 1000)}
            code, split, retry, rows = reply.get("error_code"), False, False, None
            try:
                split = (reply.get("proven_oversize") is True and code == "response_declared_oversize") or (reply.get("status") == 413 and code is None)
                require(not split, "proven_batch_size_refusal")
                require(code is None, code or "transport_failure")
                if reply.get("status") == 429 or type(reply.get("status")) is int and 500 <= reply["status"] <= 599:
                    retry = True; raise Blocked("retryable_http_failure")
                require(reply.get("status") == 200 and reply.get("complete") is True, "http_or_body_incomplete")
                values = decode(reply["raw"])
                if keys is not None and type(values) is dict and set(values) == {"jsonrpc", "id", "error"}:
                    err = values["error"]
                    split = (values["jsonrpc"] == "2.0" and values["id"] is None and type(err) is dict and
                        err.get("code") in (-32600, -32005) and type(err.get("code")) is int and
                        type(err.get("message")) is str and err["message"].lower() in
                        {"batch requests are not supported", "batch size limit exceeded", "batch request limit exceeded"})
                    require(not split, "proven_batch_size_refusal")
                values = [values] if keys is None else values
                require(type(values) is list and len(values) == len(requests), "rpc_batch_shape")
                results = {}
                for item in values:
                    require(type(item) is dict and set(item) == {"jsonrpc", "id", "result"} and item["jsonrpc"] == "2.0"
                            and type(item["id"]) is int and item["id"] not in results, "rpc_reply_shape")
                    results[item["id"]] = item["result"]
                require(set(results) == {item["id"] for item in requests}, "rpc_reply_ids")
                if keys is None: require(pilot.quantity(results[first_id]) == 137, "wrong_chain")
                else:
                    rows = [pilot.validate_header(results[item["id"]], key) for item, key in zip(requests, keys)]
                    pilot.validate_clock(rows); finish["p"] = digest(scale.projection_bytes(rows))
                finish["r"] = "validated"
            except (Blocked, pilot.PilotBlocked) as exc:
                code = str(exc); retry = retry or code in {"transport_failure", "request_deadline",
                    "response_incomplete", "content_length_differs", "response_close_failure"}
                finish["r"] = "split" if split else "failed"
            b.append(self.journal, finish, failure=finish["r"] != "validated")
            if code == "interrupted": raise KeyboardInterrupt()
            if finish["r"] == "validated": return rows
            if split:
                require(keys is not None and len(keys) > 1, "singleton_size_refusal")
                self.batch_size = min(self.batch_size, max(1, len(keys) // 2)); return None
            require(retry and attempt + 1 < CAPS["attempts_per_batch"], code or "rpc_failure")
        raise Blocked("batch_attempts_exhausted")

    def fetch(self, keys):
        rows, offset = [], 0
        while offset < len(keys):
            batch = self.call(keys[offset:offset + self.batch_size], offset)
            if batch is not None:
                if rows: pilot.validate_clock([rows[-1], batch[0]])
                rows.extend(batch); offset += len(batch)
        return rows


def read_headers(path, keys):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pq.read_table(path)
    schema = pa.schema([("block_number", pa.int64()), ("block_hash", pa.binary(32)), ("parent_hash", pa.binary(32)), ("timestamp", pa.int64())])
    require(table.schema == schema and table.num_rows == len(keys), "header_parquet_schema_or_count")
    rows = [{**row, "block_hash": "0x" + row["block_hash"].hex(), "parent_hash": "0x" + row["parent_hash"].hex()}
            for row in table.to_pylist()]
    for row, key in zip(rows, keys):
        require(pilot.validate_header({"number": hex(row["block_number"]), "hash": row["block_hash"],
            "parentHash": row["parent_hash"], "timestamp": hex(row["timestamp"])}, key) == row, "header_projection_invalid")
    pilot.validate_clock(rows)
    return rows


def verify_chunk(root, keys, offset):
    marker_path, manifest_path = root / "COMPLETE.json", root / "manifest.json"
    marker = read_json(ref(bound(marker_path, MAX_JSON)))[0]
    manifest_binding = bound(manifest_path, MAX_JSON); manifest = read_json(ref(manifest_binding))[0]
    require(marker.get("manifest") == {"path": "manifest.json", **{key: manifest_binding[key] for key in ("bytes", "sha256")}}
            and marker.get("status") == manifest.get("status") == "exact_header_chunk_complete_raw_clocks_only"
            and manifest.get("schema_version") == "polymarket_exact_header_chunk_v3"
            and manifest.get("raw_clocks_only") is True and manifest.get("final_sample_admitted") is False
            and manifest.get("chunk") == offset // CAPS["keys_per_chunk"]
            and manifest.get("offset") == offset and manifest.get("keys") == len(keys), "chunk_not_sealed_exactly")
    require(manifest.get("keys_sha256") == digest(encode(keys)), "chunk_keys_differ")
    header = root / "headers.parquet"; actual = bound(header, CAPS["chunk_artifact_bytes"])
    require(manifest.get("headers") == {"path": "headers.parquet", **{key: actual[key] for key in ("bytes", "sha256")}}, "chunk_header_changed")
    rows = read_headers(header, keys)
    usage, starts, finishes = journal_usage(root / "rpc.jsonl", (offset, keys))
    require(len(starts) == len(finishes) and usage == manifest.get("rpc_usage"), "chunk_journal_unclosed_or_changed")
    cursor = offset
    for call, item in starts.items():
        finish = finishes[call]
        if finish["r"] != "validated": continue
        requests = [{"jsonrpc": "2.0", "id": item["j"] + index, "method": "eth_getBlockByNumber",
                     "params": [hex(key["block_number"]), False]}
                    for index, key in enumerate(keys[cursor - offset:cursor - offset + item["n"]])]
        require(item["k"] == cursor and item["n"] > 0 and finish["p"] ==
                digest(scale.projection_bytes(rows[cursor - offset:cursor - offset + item["n"]]))
                and item["h"] == digest(encode(requests)) and finish["s"] == 200 and finish["h"] == finish["x"]
                and type(finish["h"]) is str and pilot.SHA.fullmatch(finish["h"]) and finish["b"] == finish["z"],
                "chunk_request_projection_differs")
        cursor += item["n"]
    require(cursor == offset + len(keys), "chunk_journal_key_coverage")
    return rows, actual


def run_campaign(admission_path, admission_sha, head, output, private, *, transport=scale.http_response, arm_prior=None):
    started = time.monotonic(); output, private = Path(output), Path(private)
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.resolve() == output.parent
            and output.parent.is_dir() and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    admission, admission_binding = read_json({"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}, MAX_LEDGER)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "caps", "consumer_head", "run_dir",
        "raw_clocks_only", "final_sample_admitted", "inputs", "private_file", "actual_keys", "conservative_new_runtime_seconds", "prior"}
        and admission["schema_version"] == SCHEMA and admission["status"] == "root_exact_header_campaign_admitted"
        and admission["caps"] == CAPS and admission["consumer_head"] == head and admission["run_dir"] == str(output)
        and admission["raw_clocks_only"] is True and admission["final_sample_admitted"] is False, "root_admission_not_exact")
    source = source_snapshot(head)
    # Tighten the outer alarm before large SHA/Parquet admission reads. The
    # subsequent ledger validation independently checks this declared charge.
    declared_runtime = elapsed(admission["prior"]["rpc"]["elapsed_seconds"])
    require(declared_runtime + time.monotonic() - started < CAPS["runtime_seconds"], "runtime_limit")
    if arm_prior: arm_prior(declared_runtime, started)
    K = integer(admission["actual_keys"], CAPS["keys"]); require(K > 0, "empty_key_sample")
    inputs, measured = validate_inputs(admission["inputs"], source, K)
    prior = validate_prior(admission["prior"], output, source, inputs)
    for value in inputs.values():
        require(value["path"] in prior["files"] and ref(prior["files"][value["path"]]) == ref(value), "input_absent_from_global_ledger")
    projection = elapsed(admission["conservative_new_runtime_seconds"])
    require(projection >= 2 * measured["seconds_per_key"] * K and projection + prior["rpc"]["elapsed_seconds"] < CAPS["runtime_seconds"], "runtime_feasibility_not_admitted")
    require(max(80, 1.25 * measured["bytes_per_key"]) * K <= CAPS["accepted_bytes"], "header_disk_projection_not_fit")
    private_binding = reference(admission["private_file"])
    require(private_binding["path"] == str(private) and private.stat().st_size <= 16 * 1024, "private_configuration_binding")
    endpoint, configuration = inventory.load_rpc_endpoint(private)
    require(configuration == "private_file", "private_rpc_authority_required")
    scale_contract = read_json(admission["inputs"]["scale_contract"])[0]
    require(scale_contract.get("rpc_authority_private_file_identity") == pilot.identity(private), "rpc_authority_differs_from_scale")
    for path in [admission_binding["path"], private_binding["path"], *prior["files"]]:
        require(Path(path) != output and output not in Path(path).parents and Path(path) not in output.parents, "input_output_overlap")
    resources = inventory.resource_state(output.parent)
    require(resources["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "available_ram_floor")
    require(resources["disk_bytes"]["free"] >= CAPS["minimum_free_bytes"] + CAPS["chunk_artifact_bytes"] + FINAL_RESERVE,
            "disk_floor")
    require(prior["rpc"]["elapsed_seconds"] == declared_runtime, "prior_runtime_changed")
    output.mkdir(mode=0o700)
    b = Budget(output, prior, admission_binding["bytes"], started)
    source_identities = {name: identity(REPO / name) for name in SOURCE_FILES}
    def before_request():
        for value in (admission_binding, private_binding, inputs["keys"]):
            require(identity(Path(value["path"])) == value["identity"], "request_input_changed")
        require(all(identity(REPO / name) == value for name, value in source_identities.items()), "request_source_changed")
    b.before_request = before_request
    completed, last, accepted = {}, None, 0
    def external(full=False):
        require(source_snapshot(head) == source, "source_changed")
        for value in [admission_binding, private_binding, *inputs.values(), *prior["files"].values()]:
            require(identity(Path(value["path"])) == value["identity"], "input_identity_changed")
        require(inventory.load_rpc_endpoint(private) == (endpoint, configuration), "private_authority_changed")
        if full:
            require(read_json(ref(admission_binding), MAX_LEDGER)[1] == admission_binding, "admission_changed")
            require(reference(admission["private_file"]) == private_binding, "private_authority_changed")
            for value in inputs.values(): require(bound(Path(value["path"]), CAPS["baseline_bytes"]) == value, "input_changed")
            require(validate_prior(admission["prior"], output, source, inputs) == prior, "prior_artifacts_or_roster_changed")
        b.check()
    try:
        b.write("OWNER.json", encode({"schema_version": "polymarket_exact_header_owner_v3", "run_dir": str(output),
            "source": source, "process": owner_identity(), "inputs": inputs, "caps": CAPS, "admission": admission_binding,
            "raw_clocks_only": True, "final_sample_admitted": False}))
        external()
        RPC(endpoint, b, transport, "rpc.jsonl").call()
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            offset = chunk * CAPS["keys_per_chunk"]
            if chunk in prior["sealed"]:
                root = Path(prior["sealed"][chunk]["root"])
                rows, header_binding = verify_chunk(root, keys, offset)
            else:
                attempt = len(prior["attempts"].get(chunk, set())) + 1
                require(attempt <= CAPS["attempts_per_chunk"], "chunk_attempts_exhausted")
                b.chunk, b.chunk_started = chunk, time.monotonic()
                b.chunk_base = prior["chunk_usage"].get(chunk, dict.fromkeys(("http_attempts", "method_items", "response_bytes", "elapsed_seconds"), 0))
                b.chunk_start_rpc = dict(b.rpc)
                base = f"chunks/c{chunk:06d}/a{attempt:02d}"
                rows = RPC(endpoint, b, transport, base + "/rpc.jsonl", offset).fetch(keys)
                payload = scale.compact_parquet(rows)
                require(len(payload) <= CAPS["chunk_artifact_bytes"], "chunk_parquet_byte_limit")
                b.check(len(payload), "accepted")  # Accepted allocation must fit before staging.
                headers = b.write(base + "/headers.parquet", payload, "failed")
                require(read_headers(output / headers["path"], keys) == rows, "chunk_header_reopen_differs")
                usage, starts, finishes = journal_usage(output / (base + "/rpc.jsonl"))
                require(len(starts) == len(finishes), "chunk_open_request")
                manifest = {"schema_version": "polymarket_exact_header_chunk_v3", "status": "exact_header_chunk_complete_raw_clocks_only",
                    "chunk": chunk, "attempt": attempt, "offset": offset, "keys": len(keys), "keys_sha256": digest(encode(keys)),
                    "headers": {**headers, "path": "headers.parquet"}, "rpc_usage": usage,
                    "active_seconds_charged": b.chunk_elapsed(), "raw_clocks_only": True, "final_sample_admitted": False}
                mb = b.write(base + "/manifest.json", encode(manifest))
                b.seal(base + "/COMPLETE.json", {"status": manifest["status"], "manifest": {**mb, "path": "manifest.json"}}, external)
                b.files[headers["path"]]["category"] = "accepted"
                b.used["failed"] -= len(payload); b.used["accepted"] += len(payload)
                root = output / base
                rows, header_binding = verify_chunk(root, keys, offset)
                b.check()
                b.chunk = None
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; accepted += len(rows)
            completed[chunk] = {"root": str(root), "headers": header_binding, "keys": len(keys)}
        require(accepted == K and len(completed) == math.ceil(K / CAPS["keys_per_chunk"]), "exact_key_coverage_failed")
        RPC(endpoint, b, transport, "rpc.jsonl").call(terminal=True)
        external(full=True); b.reopen()
        observed = dict.fromkeys(("http_attempts", "method_items", "response_bytes"), 0)
        for name in b.files:
            if Path(name).name != "rpc.jsonl": continue
            usage, starts, finishes = journal_usage(output / name)
            require(len(starts) == len(finishes), "campaign_unclosed_request")
            for key in observed: observed[key] += usage[key]
        require(all(b.rpc[key] - prior["rpc"][key] == observed[key] for key in observed), "campaign_charge_reconciliation")
        # Independent ordered final pass catches late mutations of every sealed
        # chunk, with native expected hashes and adjacent cross-chunk parents.
        last, reopened_count = None, 0
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            rows, header_binding = verify_chunk(Path(completed[chunk]["root"]), keys, chunk * CAPS["keys_per_chunk"])
            require(header_binding == completed[chunk]["headers"], "final_chunk_changed")
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; reopened_count += len(rows)
        require(reopened_count == K, "final_key_coverage_failed")
        inventory_binding = b.write("artifact_inventory.json", encode(b.files))
        manifest = {"schema_version": "polymarket_exact_header_campaign_v3", "status": SUCCESS, "source": source,
            "inputs": inputs, "admission": admission_binding, "caps": CAPS, "requested_keys": K, "accepted_keys": K,
            "missing_keys": 0, "duplicate_keys": 0, "fallback_rows": 0, "exact_key_coverage": True,
            "chunks": completed, "artifact_inventory": inventory_binding, "usage": b.usage(),
            "raw_clocks_only": True, "final_sample_admitted": False, "history_completeness_certified": False,
            "cryptographic_header_verification": False, "timestamp_semantics": "RPC-reported integer-second block time shared by fills in the same block"}
        mb = b.write("manifest.json", encode(manifest))
        b.seal("COMPLETE.json", {"schema_version": "polymarket_exact_header_campaign_completion_v3", "status": SUCCESS,
            "manifest": mb, "raw_clocks_only": True, "final_sample_admitted": False}, lambda: external(full=True))
        return {"status": SUCCESS, "run_dir": str(output), "accepted_keys": K, "usage": manifest["usage"], "final_sample_admitted": False}
    except BaseException as exc:
        code = str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "header_campaign_runtime_failure"
        result = {"schema_version": "polymarket_exact_header_campaign_v3", "status": "blocked", "error_code": code,
                  "usage": b.usage(), "accepted_keys": accepted, "raw_clocks_only": True, "final_sample_admitted": False}
        try: b.write("FAILED.json", encode(result), failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result


@contextmanager
def process_limits():
    old, handler = resource.getrlimit(resource.RLIMIT_AS), signal.getsignal(signal.SIGALRM)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (min(old[0], CAPS["address_space_bytes"]) if old[0] >= 0 else CAPS["address_space_bytes"], old[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "address_space_not_enforced")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Blocked("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"])
        def debit(prior, started):
            left = CAPS["runtime_seconds"] - prior - (time.monotonic() - started)
            require(left > 0, "runtime_limit"); signal.setitimer(signal.ITIMER_REAL, left)
        yield debit
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, handler); resource.setrlimit(resource.RLIMIT_AS, old)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission", type=Path, required=True); parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True); parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--rpc-private-file", type=Path, required=True)
    args = parser.parse_args(argv)
    require_production_host()
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_required")
        with process_limits() as debit:
            result = run_campaign(args.admission, args.admission_sha256, args.expected_head, args.run_dir,
                                  args.rpc_private_file, arm_prior=debit)
    except (Blocked, pilot.PilotBlocked) as exc: result = {"status": "blocked", "error_code": str(exc)}
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError): result = {"status": "blocked", "error_code": "admission_or_environment_failure"}
    print(encode(result).decode().strip())
    return 0 if result["status"] == SUCCESS else 1


if __name__ == "__main__": raise SystemExit(main())
