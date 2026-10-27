#!/usr/bin/env python3
"""Compact SCALE PILOT only. No full-history campaign entrypoint is implemented.

The production CLI is pinned to the original 1,000-key pilot and its accepted
RESULT. All inherited RPC charges and explicitly rostered header-category bytes
remain charged. Completed and failed earlier runs are never resumed or changed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import hashlib
import http.client
import io
import json
import math
import os
from pathlib import Path
import resource
import shutil
import signal
import stat
import struct
import subprocess
import sys
import time
from urllib import error, request

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_headers_v1 as pilot
from scripts import inventory_polymarket_refresh as inventory

ORIGINAL_HEAD = "04f958661612e5b250f1ec0eff7b44e9ffb357a2"
ORIGINAL_REQUEST_SHA256 = "af83e45810ab3f3e156c54420fca5b1017ac402067c941fff5ef9be4a865a150"
ORIGINAL_RESULT_SHA256 = "6afa31e23be7a9abd61c91bf90ab39ade7eef55ffe8da635a69d2c036828ada4"
ORIGINAL_CHARGES = {"http_attempts": 103, "method_items": 1004,
                    "response_bytes": 11_922_825, "elapsed_seconds": 8.829112935}
CAPS = {"keys": 1000, "http_attempts": 350, "method_items": 35_000,
        "individual_response_bytes": 16 * 1024**2, "response_bytes": 256 * 1024**2,
        "new_artifact_bytes": 128_000_000, "global_header_bytes": 8_000_000_000,
        "runtime_seconds": 1200, "address_space_bytes": 8 * 1024**3,
        "minimum_available_ram_bytes": 8 * 1024**3, "minimum_free_bytes": 20_000_000_000,
        "spill_bytes": 0, "initial_batch_size": 100, "attempts_per_batch": 3}
MAX_CONTROL = 1024**2
MAX_LEDGER = 64 * 1024**2
MAX_LEDGER_ENTRIES = 150_000
CONTROL_RESERVE = 2 * MAX_CONTROL
EXTERNAL_CONTROL_RESERVE = 2 * MAX_CONTROL
STAT_FIELDS = {"device", "inode", "mode", "uid", "gid", "mtime_ns", "ctime_ns"}
SOURCE_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                "scripts/collect_polymarket_exact_headers_v1.py",
                "scripts/collect_polymarket_exact_header_campaign_v2.py",
                "tests/test_polymarket_exact_header_campaign_v2.py")
OLD_SOURCE_FILES = ("scripts/collect_polymarket_exact_headers_v1.py",
                    "scripts/inventory_polymarket_refresh.py", "production_guard.py")
SCOPE = "Original literal pilot keys only; full campaign and final sample are not admitted."


class Blocked(ValueError):
    """Only fixed nonsecret codes may be saved."""


def require(ok, code):
    if not ok:
        raise Blocked(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def json_bytes(value):
    return (json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False) + "\n").encode()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def finite(value):
        number = float(value)
        require(math.isfinite(number), "nonfinite_json")
        return number
    try:
        return json.loads(raw.decode(), object_pairs_hook=unique, parse_float=finite,
                          parse_constant=lambda _: (_ for _ in ()).throw(Blocked("nonfinite_json")))
    except Blocked:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError, OverflowError):
        raise Blocked("invalid_json") from None


def file_identity(path):
    info = path.lstat()
    require(path.is_absolute() and path.resolve() == path and stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1, "input_not_literal_regular_file")
    return {"device": info.st_dev, "inode": info.st_ino, "mode": info.st_mode,
            "uid": info.st_uid, "gid": info.st_gid, "mtime_ns": info.st_mtime_ns,
            "ctime_ns": info.st_ctime_ns}


def read_small(path, sha256, limit=MAX_CONTROL):
    require(type(sha256) is str and pilot.SHA.fullmatch(sha256), "invalid_input_sha256")
    before = file_identity(path)
    size = path.stat().st_size
    require(0 < size <= limit, "input_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    require(len(raw) == size and before == file_identity(path), "input_changed_during_read")
    require(digest(raw) == sha256, "input_sha256_differs")
    return raw, {"path": str(path), "bytes": size, "sha256": sha256, "identity": before}


def hash_entry(entry):
    path = Path(entry["path"])
    require(file_identity(path) == entry["identity"] and path.stat().st_size == entry["bytes"],
            "ledger_entry_identity_or_bytes_differs")
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for raw in iter(lambda: stream.read(1024**2), b""):
            hasher.update(raw)
    require(hasher.hexdigest() == entry["sha256"] and file_identity(path) == entry["identity"],
            "ledger_entry_sha256_or_identity_differs")


def read_ledger(path, sha256):
    raw, binding = read_small(path, sha256, MAX_LEDGER)
    ledger = decode(raw)
    require(type(ledger) is dict and set(ledger) == {"schema_version", "category_ceiling_bytes",
            "complete_header_category_roster", "baseline_cache_excluded", "entries", "inherited_rpc"}
            and ledger["schema_version"] == "polymarket_header_global_ledger_v2"
            and ledger["category_ceiling_bytes"] == CAPS["global_header_bytes"]
            and ledger["complete_header_category_roster"] is True
            and ledger["baseline_cache_excluded"] is True, "ledger_schema")
    entries = ledger["entries"]
    require(type(entries) is list and 0 < len(entries) <= MAX_LEDGER_ENTRIES, "ledger_entry_count")
    paths, inodes, total = set(), set(), len(raw)
    for entry in entries:
        require(type(entry) is dict and set(entry) == {"path", "bytes", "sha256", "identity"}, "ledger_entry_schema")
        require(type(entry["path"]) is str and Path(entry["path"]).is_absolute()
                and str(Path(entry["path"])) == entry["path"] and entry["path"] != str(path), "ledger_entry_path")
        require(type(entry["bytes"]) is int and entry["bytes"] >= 0 and type(entry["sha256"]) is str
                and pilot.SHA.fullmatch(entry["sha256"]), "ledger_entry_size_or_hash")
        require(type(entry["identity"]) is dict and set(entry["identity"]) == STAT_FIELDS
                and all(type(value) is int for value in entry["identity"].values()), "ledger_entry_identity_schema")
        inode = (entry["identity"]["device"], entry["identity"]["inode"])
        require(entry["path"] not in paths and inode not in inodes, "duplicate_ledger_entry")
        paths.add(entry["path"]); inodes.add(inode)
        total += entry["bytes"]
    require(total < CAPS["global_header_bytes"], "global_header_byte_limit")
    inherited = ledger["inherited_rpc"]
    require(type(inherited) is dict and set(inherited) == {"result_path", "result_sha256", "http_attempts",
            "method_items", "response_bytes", "elapsed_seconds"}, "inherited_rpc_schema")
    require(type(inherited["result_path"]) is str and type(inherited["result_sha256"]) is str
            and pilot.SHA.fullmatch(inherited["result_sha256"]), "inherited_result_binding")
    for key in ("http_attempts", "method_items", "response_bytes"):
        require(type(inherited[key]) is int and 0 <= inherited[key] < CAPS[key], "inherited_rpc_charge")
    require(type(inherited["elapsed_seconds"]) in (int, float) and math.isfinite(inherited["elapsed_seconds"])
            and 0 <= inherited["elapsed_seconds"] < CAPS["runtime_seconds"], "inherited_runtime_charge")
    for entry in entries:
        hash_entry(entry)
    return ledger, binding, total


def git_blob(head, name):
    try:
        size = subprocess.check_output(["git", "cat-file", "-s", f"{head}:{name}"], cwd=REPO,
                                       stderr=subprocess.DEVNULL, text=True).strip()
        require(size.isdecimal() and int(size) <= MAX_CONTROL, "committed_source_size")
        raw = subprocess.check_output(["git", "cat-file", "blob", f"{head}:{name}"], cwd=REPO,
                                      stderr=subprocess.DEVNULL)
        require(len(raw) == int(size), "committed_source_size_differs")
        return raw
    except (OSError, subprocess.CalledProcessError):
        raise Blocked("committed_source_unavailable") from None


def source_snapshot(head):
    require(type(head) is str and pilot.HEAD.fullmatch(head), "invalid_expected_head")
    try:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"],
                                        cwd=REPO, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        raise Blocked("source_git_unavailable") from None
    require(actual == head and not dirty, "source_head_or_clean_state_differs")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = file_identity(path)
        require(path.stat().st_size <= MAX_CONTROL, "source_file_size")
        raw = path.read_bytes()
        require(file_identity(path) == before and raw == git_blob(head, name), "source_bytes_differ_from_commit")
        files[name] = digest(raw)
    return {"head": head, "files": files}


def committed_prior_source(value):
    require(type(value) is dict and value.get("head") == ORIGINAL_HEAD
            and value.get("tracked_worktree_clean") is True and type(value.get("files")) is dict
            and set(value["files"]) == set(OLD_SOURCE_FILES), "prior_source_schema")
    require(value["files"] == {name: digest(git_blob(ORIGINAL_HEAD, name)) for name in OLD_SOURCE_FILES},
            "prior_source_blob_differs")


def read_prior(path, sha256, keys, request_binding, ledger):
    import pyarrow as pa
    import pyarrow.parquet as pq
    raw, binding = read_small(path, sha256)
    result = decode(raw)
    require(type(result) is dict and result.get("schema_version") == "polymarket_exact_header_pilot_result_v1"
            and result.get("status") == "literal_header_pilot_complete"
            and result.get("accepted_keys") == result.get("requested_keys") == len(keys)
            and result.get("missing_keys") == result.get("duplicate_keys") == result.get("fallback_rows") == 0
            and result.get("exact_key_coverage") is True and result.get("final_sample_admitted") is False
            and result.get("history_completeness_certified") is False, "prior_result_not_accepted_literal_pilot")
    contract = result.get("contract")
    require(type(contract) is dict and contract.get("schema_version") == "polymarket_exact_header_pilot_contract_v1"
            and contract.get("request_file") == request_binding and contract.get("caps") == pilot.CAPS,
            "prior_contract_or_request_differs")
    committed_prior_source(contract.get("source"))
    entries = {entry["path"]: entry for entry in ledger["entries"]}
    required_paths = {str(path), request_binding["identity"]["path"]}
    def read_reference(reference):
        require(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}
                and type(reference["path"]) is str and Path(reference["path"]).name == reference["path"],
                "prior_reference_schema")
        candidate = path.parent / reference["path"]
        entry = entries.get(str(candidate))
        require(entry is not None and entry["bytes"] == reference["bytes"]
                and entry["sha256"] == reference["sha256"], "prior_reference_missing_or_understated_in_ledger")
        require(entry["bytes"] <= MAX_CONTROL, "prior_artifact_expansion_limit")
        required_paths.add(str(candidate))
        payload = candidate.read_bytes()
        require(len(payload) == entry["bytes"] and digest(payload) == entry["sha256"], "prior_reference_changed")
        return payload
    require(decode(read_reference(result["contract_binding"])) == contract, "prior_contract_reopen_differs")
    rows = []
    chunks = result.get("chunk_bindings")
    require(type(chunks) is list and 0 < len(chunks) <= CAPS["keys"], "prior_chunk_count")
    for chunk_index, chunk in enumerate(chunks, 1):
        require(type(chunk) is dict and set(chunk) == {"payload", "receipt"}, "prior_chunk_schema")
        payload = read_reference(chunk["payload"])
        parquet = pq.ParquetFile(pa.BufferReader(payload))
        require(parquet.metadata.num_rows <= 10 and parquet.schema_arrow == pa.schema([
                ("block_number", pa.int64()), ("block_hash", pa.string()),
                ("parent_hash", pa.string()), ("timestamp", pa.int64())])
                and sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups))
                <= 100_000, "prior_chunk_schema_or_expansion")
        values = parquet.read().to_pylist()
        receipt = decode(read_reference(chunk["receipt"]))
        require(receipt.get("headers") == values and receipt.get("payload") == chunk["payload"]
                and receipt.get("contract_sha256") == result["contract_binding"]["sha256"]
                and receipt.get("chunk") == chunk_index
                and receipt.get("requested_keys") == keys[len(rows):len(rows) + len(values)],
                "prior_chunk_receipt_differs")
        rows.extend(values)
    require([row["block_number"] for row in rows] == [key["block_number"] for key in keys], "prior_key_coverage_differs")
    try:
        for key, row in zip(keys, rows):
            require(pilot.validate_header({"number": hex(row["block_number"]), "hash": row["block_hash"],
                    "parentHash": row["parent_hash"], "timestamp": hex(row["timestamp"])}, key) == row,
                    "prior_header_projection_differs")
        pilot.validate_clock(rows)
    except (pilot.PilotBlocked, KeyError, TypeError):
        raise Blocked("prior_header_invalid") from None
    finishes = result.get("rpc_finish_receipts")
    require(type(finishes) is list and 0 < len(finishes) < CAPS["http_attempts"], "prior_rpc_receipt_count")
    method_items = response_bytes = 0
    ids = set()
    for call, reference in enumerate(finishes, 1):
        finish = decode(read_reference(reference))
        require(reference["path"] == f"rpc_{call:04d}.FINISH.json" and finish.get("call") == call
                and finish.get("status") == "validated" and finish.get("response_size_is_upper_bound") is False
                and finish.get("contract_sha256") == result["contract_binding"]["sha256"]
                and type(finish.get("response_sha256")) is str
                and pilot.SHA.fullmatch(finish["response_sha256"]), "prior_rpc_finish_invalid")
        charge = finish.get("charged_response_bytes")
        require(type(charge) is int and 0 <= charge <= CAPS["individual_response_bytes"], "prior_response_charge_invalid")
        response_bytes += charge
        start_path = path.parent / f"rpc_{call:04d}.START.json"
        entry = entries.get(str(start_path))
        require(entry is not None and entry["bytes"] <= MAX_CONTROL, "prior_start_missing_in_ledger")
        required_paths.add(str(start_path))
        start = decode(start_path.read_bytes())
        require(start.get("call") == call and start.get("contract_sha256") == result["contract_binding"]["sha256"],
                "prior_rpc_start_invalid")
        requests = start.get("requests")
        require(type(requests) is list and 0 < len(requests) <= 10, "prior_rpc_requests_invalid")
        for item in requests:
            require(type(item) is dict and set(item) == {"jsonrpc", "id", "method", "params"}
                    and item["jsonrpc"] == "2.0" and type(item["id"]) is int and item["id"] > 0
                    and item["id"] not in ids, "prior_rpc_request_identity_invalid")
            ids.add(item["id"])
            require((item["method"] == "eth_chainId" and item["params"] == []) or
                    (item["method"] == "eth_getBlockByNumber" and type(item["params"]) is list
                     and len(item["params"]) == 2 and type(item["params"][0]) is str
                     and pilot.QUANTITY.fullmatch(item["params"][0]) and item["params"][1] is False),
                    "prior_rpc_method_or_params_invalid")
        method_items += len(requests)
    usage, inherited = result.get("usage"), ledger["inherited_rpc"]
    require(type(usage) is dict and usage.get("rpc_batch_attempts") == len(finishes)
            and usage.get("rpc_method_items") == method_items and usage.get("charged_response_bytes") == response_bytes,
            "prior_durable_counters_differs")
    require(inherited["result_path"] == str(path) and inherited["result_sha256"] == sha256
            and inherited["http_attempts"] == len(finishes) and inherited["method_items"] == method_items
            and inherited["response_bytes"] == response_bytes and type(usage.get("elapsed_seconds")) in (int, float)
            and math.isfinite(usage["elapsed_seconds"]) and inherited["elapsed_seconds"] >= usage["elapsed_seconds"],
            "inherited_rpc_understated_or_differs")
    require(required_paths <= set(entries), "prior_artifact_missing_in_ledger")
    require(entries[str(path)]["sha256"] == sha256 and entries[str(path)]["bytes"] == len(raw), "prior_result_ledger_differs")
    require(entries[request_binding["identity"]["path"]]["sha256"] == request_binding["sha256"], "request_ledger_differs")
    require(type(usage.get("retained_bytes_before_terminal")) is int and
            usage["retained_bytes_before_terminal"] + len(raw) == sum(
                entry["bytes"] for entry in entries.values() if Path(entry["path"]).parent == path.parent),
            "prior_retained_bytes_ledger_differs")
    return rows, result, binding


def atomic_publish(pending, destination):
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        value = library.renameat2(-100, os.fsencode(pending), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        value = library.renamex_np(os.fsencode(pending), os.fsencode(destination), 4)
    else:
        raise Blocked("atomic_publication_unavailable")
    require(value == 0, "atomic_publication_refused")


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Budget:
    def __init__(self, output, ledger_bytes, inherited, clock=time.monotonic, started=None):
        self.output, self.ledger_bytes, self.inherited, self.clock = output, ledger_bytes, inherited.copy(), clock
        self.started, self.files = clock() if started is None else started, {}
        self.http, self.methods, self.responses = (inherited[key] for key in ("http_attempts", "method_items", "response_bytes"))

    def elapsed(self):
        return self.inherited["elapsed_seconds"] + max(0, self.clock() - self.started)

    def retained(self):
        total = 0
        for path in self.output.iterdir():
            info = path.lstat()
            require(path.name in self.files and stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                    "unexpected_output_artifact")
            total += info.st_size
        return total

    def check(self, extra=0, *, failure=False, terminal=False):
        if not failure:
            require(self.elapsed() < CAPS["runtime_seconds"], "runtime_limit")
        total = self.retained()
        reserve = 0 if terminal else CONTROL_RESERVE
        require(total + extra + reserve + EXTERNAL_CONTROL_RESERVE <= CAPS["new_artifact_bytes"], "new_artifact_byte_limit")
        require(self.ledger_bytes + total + extra + reserve + EXTERNAL_CONTROL_RESERVE
                <= CAPS["global_header_bytes"], "global_header_byte_limit")
        require(shutil.disk_usage(self.output).free >= extra + reserve + EXTERNAL_CONTROL_RESERVE
                + CAPS["minimum_free_bytes"], "disk_floor")
        return total

    def publish(self, name, raw, *, failure=False, final_check=None):
        require(Path(name).name == name and len(raw) <= CAPS["new_artifact_bytes"], "output_name_or_file_size")
        self.check(len(raw), failure=failure, terminal=final_check is not None)
        pending, target = self.output / ("." + name + ".pending"), self.output / name
        require(not pending.exists() and not pending.is_symlink() and not target.exists()
                and not target.is_symlink(), "output_exists")
        bound = {"path": name, "bytes": len(raw), "sha256": digest(raw)}
        self.files[pending.name] = {"bytes": len(raw), "sha256": bound["sha256"]}
        with pending.open("xb") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write")
            stream.flush(); os.fsync(stream.fileno())
        require(pending.read_bytes() == raw, "artifact_reopen_differs")
        fsync_directory(self.output)
        if final_check is not None:
            final_check()
            self.reopen()
        self.check(0, failure=failure, terminal=final_check is not None)
        atomic_publish(pending, target)
        self.files[name] = self.files.pop(pending.name)
        return bound

    def append(self, value, *, failure=False):
        raw = json_bytes(value)
        require(len(raw) <= MAX_CONTROL, "journal_record_byte_limit")
        self.check(len(raw), failure=failure)
        path = self.output / "rpc.jsonl"
        require(not path.is_symlink(), "journal_symlink")
        if path.name not in self.files:
            require(not path.exists(), "journal_exists")
            self.files[path.name] = None
        with path.open("ab") as stream:
            require(stream.write(raw) == len(raw), "journal_short_write")
            stream.flush(); os.fsync(stream.fileno())
        fsync_directory(self.output)
        # Final journal bytes and records are independently reopened before commit.

    def seal_journal(self):
        raw = (self.output / "rpc.jsonl").read_bytes()
        self.files["rpc.jsonl"] = {"bytes": len(raw), "sha256": digest(raw)}
        return {"path": "rpc.jsonl", **self.files["rpc.jsonl"]}

    def reopen(self):
        self.check()
        for name, expected in self.files.items():
            require(expected is not None, "unsealed_journal")
            raw = (self.output / name).read_bytes()
            require({"bytes": len(raw), "sha256": digest(raw)} == expected, "artifact_reopen_differs")

    def usage(self):
        return {"http_attempts": self.http, "method_items": self.methods, "response_bytes": self.responses,
                "elapsed_seconds": self.elapsed(), "inherited_rpc": self.inherited,
                "external_operational_control_reservation_bytes": EXTERNAL_CONTROL_RESERVE,
                "new_retained_bytes_before_next_control": self.retained(),
                "new_charged_bytes_before_next_control": self.retained() + EXTERNAL_CONTROL_RESERVE,
                "global_charged_bytes_before_next_control": self.ledger_bytes + self.retained() + EXTERNAL_CONTROL_RESERVE}


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_response(endpoint, payload, *, byte_limit, timeout):
    """Return bounded bytes and conservative charges, including failed body reads."""
    response, raw, status, code, length, complete = None, bytearray(), None, None, None, False
    charged, oversize = byte_limit, False
    try:
        opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
        req = request.Request(endpoint, data=json_bytes(payload), method="POST",
                              headers={"Content-Type": "application/json", "Accept-Encoding": "identity"})
        try:
            response = opener.open(req, timeout=timeout)
        except error.HTTPError as returned:
            response = returned
        status = response.status
        require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
        require(response.geturl() == endpoint and response.headers.get("Content-Encoding", "identity").lower()
                == "identity", "redirect_or_encoding_changed")
        header = response.headers.get("Content-Length")
        if header is not None:
            require(type(header) is str and header.isdecimal() and str(int(header)) == header, "invalid_content_length")
            length = int(header)
        if length is not None and length > byte_limit:
            code, oversize = "response_declared_oversize", True
        else:
            while len(raw) < byte_limit:
                part = response.read(min(65536, byte_limit - len(raw)))
                require(type(part) is bytes and len(part) <= min(65536, byte_limit - len(raw)), "transport_read_contract")
                if not part:
                    complete = True
                    break
                raw.extend(part)
            else:
                complete = length == len(raw)
            require(complete, "response_at_limit_without_terminal_proof")
            require(length is None or len(raw) == length, "content_length_differs")
            charged = len(raw)
    except http.client.IncompleteRead as exc:
        if type(exc.partial) is bytes and len(exc.partial) <= byte_limit - len(raw):
            raw.extend(exc.partial)
        code = "response_incomplete"
    except Blocked as exc:
        code = str(exc)
    except BaseException as exc:
        code = "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "transport_failure"
    finally:
        if response is not None:
            try:
                response.close()
            except BaseException as exc:
                charged, complete = byte_limit, False
                code = "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "response_close_failure"
    return {"raw": bytes(raw), "status": status, "error_code": code, "charged_bytes": charged,
            "complete": complete and code is None, "proven_oversize": oversize,
            "declared_content_length": length}


def projection_bytes(rows):
    return b"".join(struct.pack(">q32s32sq", row["block_number"], bytes.fromhex(row["block_hash"][2:]),
                                bytes.fromhex(row["parent_hash"][2:]), row["timestamp"]) for row in rows)


class RPC:
    def __init__(self, endpoint, budget, contract_sha256, transport):
        self.endpoint, self.budget, self.contract, self.transport = endpoint, budget, contract_sha256, transport
        self.next_id, self.batch_size, self.records = budget.methods + 1, CAPS["initial_batch_size"], []

    def journal(self, value, *, failure=False):
        self.budget.append(value, failure=failure)
        self.records.append(value)

    def call(self, keys=None, *, terminal=False):
        for attempt in range(1, CAPS["attempts_per_batch"] + 1):
            self.budget.check()
            count = 1 if keys is None else len(keys)
            require(0 < count <= CAPS["initial_batch_size"], "invalid_batch_size")
            require(self.budget.http + 1 + (0 if terminal else 3) <= CAPS["http_attempts"], "http_attempt_limit")
            require(self.budget.methods + count + (0 if terminal else 3) <= CAPS["method_items"], "method_item_limit")
            remaining = CAPS["response_bytes"] - self.budget.responses
            require(remaining > 0, "response_byte_limit")
            allowance = min(CAPS["individual_response_bytes"], remaining)
            requests = []
            for key in [None] if keys is None else keys:
                requests.append({"jsonrpc": "2.0", "id": self.next_id,
                                 "method": "eth_chainId" if key is None else "eth_getBlockByNumber",
                                 "params": [] if key is None else [hex(key["block_number"]), False]})
                self.next_id += 1
            call = self.budget.http + 1
            self.journal({"event": "START", "call": call, "attempt": attempt, "contract_sha256": self.contract,
                          "requests": requests, "response_byte_reservation": allowance})
            self.budget.http += 1; self.budget.methods += count
            payload = requests[0] if keys is None else requests
            try:
                reply = self.transport(self.endpoint, payload, byte_limit=allowance,
                                       timeout=max(.01, min(30., CAPS["runtime_seconds"] - self.budget.elapsed())))
            except BaseException as exc:
                reply = {"raw": b"", "status": None, "error_code": "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit))
                         else "transport_failure", "charged_bytes": allowance, "complete": False, "proven_oversize": False}
            if not (type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance
                    and type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance
                    and type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool):
                reply = {"raw": b"", "status": None, "error_code": "transport_result_contract", "charged_bytes": allowance,
                         "complete": False, "proven_oversize": False}
            self.budget.responses += reply["charged_bytes"]
            finish = {"event": "FINISH", "call": call, "contract_sha256": self.contract,
                      "charged_response_bytes": reply["charged_bytes"], "observed_response_bytes": len(reply["raw"]),
                      "observed_body_sha256": digest(reply["raw"]), "response_complete": reply["complete"],
                      "full_response_sha256": digest(reply["raw"]) if reply["complete"] else None,
                      "http_status": reply.get("status"), "declared_content_length": reply.get("declared_content_length")}
            code, split, retry, result = reply.get("error_code"), False, False, None
            try:
                require(self.budget.responses <= CAPS["response_bytes"], "response_byte_limit")
                if ((reply["proven_oversize"] and code == "response_declared_oversize")
                        or (reply.get("status") == 413 and code is None)):
                    split, code = True, "proven_batch_size_refusal"
                    raise Blocked(code)
                require(code is None, code or "transport_failure")
                status = reply.get("status")
                require(type(status) is int, "invalid_http_status")
                if status == 429 or 500 <= status <= 599:
                    retry = True
                    raise Blocked("retryable_http_failure")
                require(200 <= status < 300 and reply["complete"], "http_or_body_incomplete")
                self.budget.check()
                values = decode(reply["raw"])
                if keys is not None and type(values) is dict and set(values) == {"jsonrpc", "id", "error"}:
                    err = values["error"]
                    if (values["jsonrpc"] == "2.0" and values["id"] is None and type(err) is dict
                            and type(err.get("code")) is int and err["code"] in (-32600, -32005)
                            and type(err.get("message")) is str and err["message"].lower() in
                            {"batch requests are not supported", "batch size limit exceeded", "batch request limit exceeded"}):
                        split = True
                        raise Blocked("proven_batch_size_refusal")
                values = [values] if keys is None else values
                require(type(values) is list and len(values) == len(requests), "rpc_batch_shape")
                replies = {}
                for item in values:
                    require(type(item) is dict and set(item) == {"jsonrpc", "id", "result"}
                            and item["jsonrpc"] == "2.0" and type(item["id"]) is int, "rpc_reply_shape")
                    require(item["id"] not in replies, "duplicate_rpc_reply_id")
                    replies[item["id"]] = item["result"]
                require(set(replies) == {item["id"] for item in requests}, "rpc_reply_id_set_differs")
                if keys is None:
                    require(pilot.quantity(replies[requests[0]["id"]]) == 137, "rpc_wrong_chain")
                    result = 137
                else:
                    result = [pilot.validate_header(replies[item["id"]], key) for item, key in zip(requests, keys)]
                    pilot.validate_clock(result)
                    finish["projection_sha256"] = digest(projection_bytes(result))
                    finish["projected_headers"] = len(result)
                finish["outcome"] = "validated"
            except (Blocked, pilot.PilotBlocked) as exc:
                code = str(exc)
                retry = retry or code == "transport_failure"
                finish.update(outcome="split_required" if split else "failed", error_code=code)
            self.journal(finish, failure=finish["outcome"] != "validated")
            if finish["outcome"] == "validated":
                return result
            if split:
                require(keys is not None and len(keys) > 1, "singleton_or_chain_size_refusal")
                self.batch_size = min(self.batch_size, max(1, len(keys) // 2))
                return None
            if not retry or attempt == CAPS["attempts_per_batch"]:
                raise Blocked(code)
        raise Blocked("rpc_attempts_exhausted")

    def fetch(self, keys, *, terminal=False):
        rows, offset = [], 0
        while offset < len(keys):
            batch = self.call(keys[offset:offset + self.batch_size], terminal=terminal)
            if batch is not None:
                try:
                    pilot.validate_clock([*rows, *batch])
                except pilot.PilotBlocked as exc:
                    raise Blocked(str(exc)) from None
                rows.extend(batch); offset += len(batch)
        return rows

    def reopen_journal(self):
        raw = (self.budget.output / "rpc.jsonl").read_bytes()
        require([decode(line) for line in raw.splitlines()] == self.records, "journal_records_changed")
        starts = [row for row in self.records if row["event"] == "START"]
        finishes = [row for row in self.records if row["event"] == "FINISH"]
        require(len(starts) == len(finishes) == self.budget.http - self.budget.inherited["http_attempts"]
                and [row["call"] for row in starts] == [row["call"] for row in finishes]
                and sum(len(row["requests"]) for row in starts) == self.budget.methods - self.budget.inherited["method_items"]
                and sum(row["charged_response_bytes"] for row in finishes)
                == self.budget.responses - self.budget.inherited["response_bytes"], "journal_charge_reconciliation")


def compact_parquet(rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([("block_number", pa.int64()), ("block_hash", pa.binary(32)),
                        ("parent_hash", pa.binary(32)), ("timestamp", pa.int64())])
    values = [{**row, "block_hash": bytes.fromhex(row["block_hash"][2:]),
               "parent_hash": bytes.fromhex(row["parent_hash"][2:])} for row in rows]
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(values, schema=schema), sink, compression="zstd", row_group_size=10_000)
    raw = sink.getvalue().to_pybytes()
    reopened = pq.read_table(pa.BufferReader(raw))
    require(reopened.schema == schema and reopened.to_pylist() == values, "compact_parquet_roundtrip_differs")
    return raw


def run_scale(request_file, request_sha256, prior_result, prior_sha256, ledger_path, ledger_sha256,
              expected_head, output, *, rpc_private_file, transport=http_response, clock=time.monotonic):
    started = clock()
    request_file, prior_result, ledger_path, output, private = map(Path, (request_file, prior_result, ledger_path, output, rpc_private_file))
    require(output.is_absolute() and output.parent.is_dir() and output.parent.resolve() == output.parent
            and not output.exists() and not output.is_symlink(), "destination_not_new_literal_directory")
    source = source_snapshot(expected_head)
    keys, key_binding = pilot.read_requests(request_file, request_sha256)
    ledger, ledger_binding, ledger_bytes = read_ledger(ledger_path, ledger_sha256)
    old_rows, old_result, old_binding = read_prior(prior_result, prior_sha256, keys, key_binding, ledger)
    for path in [request_file, prior_result, ledger_path, private, *[Path(item["path"]) for item in ledger["entries"]]]:
        require(output != path and output not in path.parents and path not in output.parents, "destination_overlaps_input")
    private_identity = pilot.identity(private)
    endpoint, configuration = inventory.load_rpc_endpoint(private)
    require(configuration == "private_file" and old_result["contract"].get("rpc_configuration_source") == configuration
            and old_result["contract"].get("rpc_private_file_identity") == private_identity, "rpc_authority_differs_from_prior")
    require(shutil.disk_usage(output.parent).free >= CAPS["new_artifact_bytes"] + CAPS["minimum_free_bytes"], "disk_floor")
    require(ledger_bytes + CAPS["new_artifact_bytes"] <= CAPS["global_header_bytes"], "global_header_reservation_limit")
    output.mkdir(mode=0o700)
    budget = Budget(output, ledger_bytes, ledger["inherited_rpc"], clock, started)
    result = {"schema_version": "polymarket_exact_header_scale_pilot_v2", "status": "blocked", "scope": SCOPE,
              "literal_keys_only": True, "full_campaign_admitted": False, "final_sample_admitted": False,
              "requested_keys": len(keys), "accepted_keys": 0}
    try:
        contract = {"schema_version": "polymarket_exact_header_scale_contract_v2", "source": source, "caps": CAPS,
                    "request": key_binding, "prior_result": old_binding, "header_ledger": ledger_binding,
                    "inherited_rpc": ledger["inherited_rpc"], "preexisting_header_bytes_including_ledger": ledger_bytes,
                    "external_operational_control_reservation_bytes": EXTERNAL_CONTROL_RESERVE,
                    "scope": SCOPE, "cache_reuse": False, "rpc_authority_private_file_identity": private_identity}
        contract_bound = budget.publish("CONTRACT.json", json_bytes(contract))
        rpc = RPC(endpoint, budget, contract_bound["sha256"], transport)
        rpc.call()
        rows = rpc.fetch(keys)
        require(rows == old_rows, "new_headers_differ_from_accepted_prior")
        rpc.call(terminal=True)
        boundary = rows[:1] if len(rows) == 1 else [rows[0], rows[-1]]
        reopened = rpc.fetch([{"block_number": row["block_number"], "block_hash": row["block_hash"]}
                              for row in boundary], terminal=True)
        require(reopened == boundary, "final_header_reopen_differs")
        payload = compact_parquet(rows)
        parquet_bound = budget.publish("headers.parquet", payload)
        require((output / "headers.parquet").read_bytes() == payload, "published_parquet_differs")
        rpc.reopen_journal()
        journal_bound = budget.seal_journal()
        def final_inputs():
            require(source_snapshot(expected_head) == source, "source_changed")
            new_keys, new_binding = pilot.read_requests(request_file, request_sha256)
            require(new_keys == keys and new_binding == key_binding, "request_changed")
            new_ledger, new_ledger_binding, new_bytes = read_ledger(ledger_path, ledger_sha256)
            require(new_ledger == ledger and new_ledger_binding == ledger_binding and new_bytes == ledger_bytes, "ledger_changed")
            require(read_small(prior_result, prior_sha256)[1] == old_binding, "prior_result_changed")
            require(pilot.identity(private) == private_identity, "rpc_private_file_changed")
            current_endpoint, current_configuration = inventory.load_rpc_endpoint(private)
            require(current_endpoint == endpoint and current_configuration == configuration, "rpc_authority_changed")
            rpc.reopen_journal()
        final_inputs(); budget.reopen()
        result.update(status="literal_compact_scale_pilot_complete", accepted_keys=len(rows), missing_keys=0,
                      fallback_rows=0, exact_key_coverage=True, matches_accepted_prior=True,
                      headers=parquet_bound, journal=journal_bound, contract=contract_bound,
                      header_projection_sha256=digest(projection_bytes(rows)), usage=budget.usage(),
                      measured_parquet_bytes_per_key=len(payload) / len(rows),
                      full_campaign_feasibility_demonstrated=False,
                      completed_utc=datetime.now(timezone.utc).isoformat())
        manifest = budget.publish("manifest.json", json_bytes(result))
        budget.publish("COMPLETE.json", json_bytes({"schema_version": "polymarket_exact_header_scale_completion_v2",
                       "status": result["status"], "manifest": manifest, "literal_keys_only": True,
                       "full_campaign_admitted": False, "final_sample_admitted": False}), final_check=final_inputs)
        return result
    except BaseException as exc:
        code = str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "scale_io_or_resource_failure"
        result.update(status="blocked", error_code=code, usage=budget.usage())
        try:
            budget.publish("FAILED.json", json_bytes(result), failure=True)
        except BaseException:
            result["failure_receipt_published"] = False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return result


@contextmanager
def process_limits():
    previous_limit = resource.getrlimit(resource.RLIMIT_AS)
    previous_handler = signal.getsignal(signal.SIGALRM)
    value = min(previous_limit[0], CAPS["address_space_bytes"]) if previous_limit[0] >= 0 else CAPS["address_space_bytes"]
    try:
        resource.setrlimit(resource.RLIMIT_AS, (value, previous_limit[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "address_space_limit_not_effective")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Blocked("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"] - ORIGINAL_CHARGES["elapsed_seconds"])
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, previous_handler)
        resource.setrlimit(resource.RLIMIT_AS, previous_limit)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("request-file", "prior-result", "ledger", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--ledger-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    require_production_host()  # Before every production source/config/input read.
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        require(args.output_dir.is_absolute() and args.output_dir.parent == Path("/mnt/data/runs"), "production_output_invalid")
        require(inventory.resource_state()["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
        # No flags can select another sample or a full campaign.
        with process_limits():
            ledger, _, _ = read_ledger(args.ledger, args.ledger_sha256)
            require({key: ledger["inherited_rpc"][key] for key in ORIGINAL_CHARGES} == ORIGINAL_CHARGES,
                    "production_inherited_charges_differ")
            require(ledger["inherited_rpc"]["result_path"] == str(args.prior_result)
                    and ledger["inherited_rpc"]["result_sha256"] == ORIGINAL_RESULT_SHA256,
                    "production_prior_result_differs")
            result = run_scale(args.request_file, ORIGINAL_REQUEST_SHA256, args.prior_result, ORIGINAL_RESULT_SHA256,
                               args.ledger, args.ledger_sha256, args.expected_head, args.output_dir,
                               rpc_private_file=args.rpc_private_file)
    except (Blocked, pilot.PilotBlocked, OSError, KeyError):
        result = {"status": "blocked", "error_code": "scale_admission_failed", "final_sample_admitted": False}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "literal_compact_scale_pilot_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
