"""Acquire one fixed, authenticated native compiler, without executing it.

This toolchain stage requires a root-bound terminal metadata ledger, not
successful pagination. Failed metadata remains unaccepted and immutable.
It never collects markets/trades, follows URLs, changes executable permissions,
or admits dependencies, economic semantics or a scientific sample.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import hashlib
import http.client as http_client
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
import threading
import time
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import inventory_polymarket_refresh as shared

URL = "https://binaries.soliditylang.org/linux-amd64/solc-linux-amd64-v0.8.34+commit.80d5c536"
COMPILER_SHA = "d40adc6f9fdbb22a97d32a02fa05688bf2ee7886affc48c9851b0afd4a726b39"
VERSION = "0.8.34+commit.80d5c536"
RUN_ROOT = Path("/mnt/data/runs")
SOURCE_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
    "scripts/collect_polymarket_solc_binary_v2.py", "tests/test_polymarket_solc_binary_v2.py")
CAPS = {"http_attempts": 2, "body_bytes_per_attempt": 32*1024**2,
    "response_bytes_per_attempt_including_detector": 32*1024**2+1,
    "total_response_bytes_charged": 64*1024**2+2, "all_new_artifact_bytes": 66*1024**2,
    "external_root_control_reservation_bytes": 2*1024**2, "terminal_control_reserve_bytes": 1024**2,
    "owned_control_artifact_bytes": 1024**2, "owned_control_reopen_reservation_bytes": 3*1024**2,
    "aggregate_metadata_bytes": 8_000_000_000, "raw_metadata_bytes": 4_000_000_000,
    "metadata_control_bytes": 500_000_000, "other_metadata_bytes": 3_500_000_000,
    "small_control_bytes_per_file": 16*1024**2, "all_small_control_read_bytes": 64*1024**2,
    "prior_files": 50_000, "directory_entries": 100_000, "prior_roots": 256,
    "address_space_bytes": 256*1024**2, "runtime_seconds": 300, "http_timeout_seconds": 30,
    "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0, "rpc_calls": 0, "compiler_executions": 0}
SCHEMA = "polymarket_solc_binary_acquisition_v2"
META_CEILINGS = {"http_attempts":40000,"response_bytes_charged":32*1024**3,"runtime_seconds":86400}
META_MINIMA = {"http_attempts":4859,"response_bytes_charged":11970589082,"runtime_seconds":7800}
PRODUCER_HEAD = "1bbeac146d22b2ca0ec290180f6dc7bf89318b6c"
OP_ROOT = RUN_ROOT/"2026-10-27_polymarket_oct1_finalization_v1"
FAILED_ROOT = RUN_ROOT/"2026-10-27_polymarket_market_metadata_full_v3"
TERMINAL_PINS = {
 "start":{"path":str(FAILED_ROOT/"START.json"),"bytes":3973,"sha256":"846447007f74519af09aa62ae2224bbf09c3f469be868c533a09e58867fb2dd5"},
 "failure":{"path":str(FAILED_ROOT/"FAILURE.json"),"bytes":1235,"sha256":"e8c215008efe90a593fa848003581458a6d2909bf7befe13e7bcf1736fea70ad"},
 "admission":{"path":str(OP_ROOT/"full_metadata_admission_v2.json"),"bytes":20604,"sha256":"c310cc43f4ee68bb7713808b34fdc245f55efaccf707e98f1679a872628bff56"},
 "launch":{"path":str(OP_ROOT/"full_metadata_v1.LAUNCH.json"),"bytes":741,"sha256":"f79a4812657e2d51c5aea3870189b89c266bb796cfaa216f734c1b1558a71a66"},
 "exit":{"path":str(OP_ROOT/"full_metadata_v1.EXIT.json"),"bytes":867,"sha256":"9607851c2e1d6b7444c4f2423ba2c70854318f6209876a891dec9bbc5f8d77b3"},
 "drain":{"path":str(OP_ROOT/"finalization_shutdown_drain_v1.json"),"bytes":232044,"sha256":"188c8153f91eada372fe52b678d985976a1216bae4978e5d8c2342e13c13b795"}}
HEAD = re.compile(r"[0-9a-f]{40}")
SHA = re.compile(r"[0-9a-f]{64}")
STAT_FIELDS = {"device", "inode", "mode", "uid", "gid", "bytes", "mtime_ns", "ctime_ns"}
TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
HEADERS = {"Accept": "application/octet-stream", "Accept-Encoding": "identity",
           "User-Agent": "prediction-markets-fixed-solc-acquisition/1"}


class Blocked(ValueError): pass


def require(ok, code):
    if not ok: raise Blocked(code)


def digest(raw): return hashlib.sha256(raw).hexdigest()


def encode(value): return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)+"\n").encode()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate_json_key"); result[key] = value
        return result
    def finite(value):
        parsed = float(value); require(math.isfinite(parsed), "nonfinite_json"); return parsed
    try:
        return json.loads(raw.decode(), object_pairs_hook=unique, parse_float=finite,
            parse_constant=lambda _: (_ for _ in ()).throw(Blocked("nonfinite_json")))
    except Blocked: raise
    except (ValueError, UnicodeDecodeError, RecursionError, OverflowError): raise Blocked("invalid_json") from None


def identity(path):
    require(path.is_absolute() and path.resolve() == path, "path_not_literal_absolute")
    value = path.lstat()
    require(stat.S_ISREG(value.st_mode) and value.st_nlink == 1, "not_single_regular_file")
    return {"device": value.st_dev, "inode": value.st_ino, "mode": value.st_mode, "uid": value.st_uid,
            "gid": value.st_gid, "bytes": value.st_size, "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def source_snapshot(head):
    state = shared.code_state()
    require(type(head) is str and HEAD.fullmatch(head) and state.get("status") == "inspected"
            and state.get("head") == head and state.get("tracked_worktree_clean") is True, "source_not_clean_bound_head")
    hashes = {}
    for name in SOURCE_FILES:
        path = REPO/name; before = identity(path)
        require(0 < before["bytes"] <= 1024**2, "source_size_limit")
        count = subprocess.check_output(["git", "cat-file", "-s", head+":"+name], cwd=REPO, stderr=subprocess.DEVNULL, timeout=10).strip()
        require(count.isdigit() and int(count) == before["bytes"], "source_blob_size_differs")
        raw = subprocess.check_output(["git", "cat-file", "blob", head+":"+name], cwd=REPO, stderr=subprocess.DEVNULL, timeout=10)
        require(raw == path.read_bytes() and identity(path) == before, "source_blob_differs")
        hashes[name] = digest(raw)
    require(shared.code_state() == state, "source_changed_during_snapshot")
    return {"head": head, "files": hashes}


class Controls:
    def __init__(self, deadline):
        self.deadline, self.read_bytes, self.bindings, self.cache = deadline, 0, {}, {}

    def read(self, ref, *, force=False):
        self.deadline()
        require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"} and type(ref["path"]) is str
                and type(ref["bytes"]) is int and 0 < ref["bytes"] <= CAPS["small_control_bytes_per_file"]
                and type(ref["sha256"]) is str and SHA.fullmatch(ref["sha256"]), "control_reference_schema")
        path = Path(ref["path"]); before = identity(path)
        require(before["bytes"] == ref["bytes"], "control_size_differs")
        bound = {**ref, "identity": before}
        require(str(path) not in self.bindings or self.bindings[str(path)] == bound, "control_identity_changed")
        if not force and str(path) in self.cache: return self.cache[str(path)]
        require(self.read_bytes + ref["bytes"] <= CAPS["all_small_control_read_bytes"], "small_control_read_limit")
        self.read_bytes += ref["bytes"]
        with path.open("rb") as stream: raw = stream.read(ref["bytes"]+1)
        require(len(raw) == ref["bytes"] and digest(raw) == ref["sha256"] and identity(path) == before, "control_hash_or_identity_differs")
        self.bindings[str(path)] = bound; self.cache[str(path)] = raw
        self.deadline(); return raw

    def reopen(self):
        for bound in list(self.bindings.values()):
            self.read({key: bound[key] for key in ("path", "bytes", "sha256")}, force=True)

    def charge_owned(self, amount):
        require(self.read_bytes+amount<=CAPS["all_small_control_read_bytes"],"small_control_read_limit")
        self.read_bytes+=amount


def short_ref(value): return {key: value[key] for key in ("path", "bytes", "sha256")}


def tree_paths(roots, output, deadline):
    actual, traversed = set(), 0
    for root in roots:
        require(root.is_absolute() and RUN_ROOT in root.parents and root.resolve() == root and root.is_dir() and not root.is_symlink()
                and root != output and root not in output.parents and output not in root.parents, "prior_root_invalid")
        stack = [root]
        while stack:
            deadline()
            directory = stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    traversed += 1; require(traversed <= CAPS["directory_entries"], "directory_entry_limit")
                    require(not entry.is_symlink(), "prior_tree_symlink")
                    if entry.is_dir(follow_symlinks=False): stack.append(Path(entry.path))
                    else:
                        require(entry.is_file(follow_symlinks=False), "prior_tree_file_type")
                        actual.add(entry.path); require(len(actual) <= CAPS["prior_files"], "prior_file_count_limit")
    return actual


def nonnegative(value, ceiling, *, integer=True):
    require(type(value) is int if integer else type(value) in (int,float),
            "usage_number_schema")
    require(math.isfinite(value) and 0 <= value <= ceiling, "usage_number_range")
    return value


def failed_journal(controls, binding, failure, parent_prior, entries, root):
    raw = controls.read(short_ref(binding)); records = raw.splitlines()
    require(records and len(records) <= 2*META_CEILINGS["http_attempts"] and len(records)%2 == 0,
            "failed_journal_count")
    count = len(records)//2; observed = charged = 0; bodies = {}
    for index in range(count):
        controls.deadline()
        start, finish = decode(records[2*index]), decode(records[2*index+1])
        call = index+1
        require(start.get("event") == "START" and finish.get("event") == "FINISH"
                and type(start.get("call")) is int and start["call"] == call
                and type(finish.get("call")) is int and finish["call"] == call,
                "failed_journal_order_or_pair")
        require(start.get("stream") == finish.get("stream") == "clob"
                and type(start.get("page_index")) is int and start["page_index"] == index
                and finish.get("page_index") == index
                and type(start.get("exact_page_attempt")) is int and start["exact_page_attempt"] == 1,
                "failed_journal_locator")
        reservation = nonnegative(start.get("response_byte_reservation"),4*1024**2)
        require(reservation > 0 and type(start.get("request")) is dict
                and start.get("request_sha256") == digest(encode(start["request"]))
                and finish.get("request_sha256") == start["request_sha256"],
                "failed_journal_request")
        n = nonnegative(finish.get("response_bytes_charged"),reservation)
        seen = nonnegative(finish.get("response_bytes_observed"),reservation)
        require(n >= seen and type(finish.get("body_complete")) is bool
                and finish.get("transport_dispatched") is True and finish.get("interrupted") is False,
                "failed_journal_response_charge")
        accepted = index < count-1
        require(finish.get("accepted") is accepted
                and (finish.get("error_code") is None if accepted else finish.get("error_code") == "response_byte_limit")
                and finish["body_complete"] is accepted
                and (n == seen if accepted else n == reservation == seen),
                "failed_journal_terminal")
        body = finish.get("body")
        relative = f"responses/a{call:05d}.body.gz"
        require(type(body) is dict and body.get("path") == relative and body.get("category") == "raw"
                and type(body.get("bytes")) is int and body["bytes"] >= 0
                and type(body.get("sha256")) is str and SHA.fullmatch(body["sha256"])
                and body.get("uncompressed_bytes") == seen
                and type(body.get("uncompressed_sha256")) is str and SHA.fullmatch(body["uncompressed_sha256"])
                and body.get("complete_received_body") is accepted, "failed_journal_body")
        path = str(root/relative)
        require(path in entries and all(entries[path][key] == body[key] for key in ("bytes","sha256","category")),
                "failed_journal_body_ledger")
        bodies[path] = {key:entries[path][key] for key in ("bytes","sha256","category")}
        observed += seen; charged += n
    usage = failure["usage"]
    require(type(usage.get("this_run_http_attempts")) is int and usage["this_run_http_attempts"] == count
            and usage.get("this_run_response_bytes_charged") == charged
            and usage.get("response_bytes_observed_this_run") == observed
            and usage.get("http_attempts_charged") == parent_prior["http_attempts"]+count
            and usage.get("response_bytes_charged") == parent_prior["response_bytes_charged"]+charged,
            "failed_journal_usage_differs")
    waterfall = failure.get("waterfall",{})
    require(waterfall.get("http_attempts_this_run") == count and waterfall.get("accepted_pages") == count-1
            and waterfall.get("failed_attempts") == 1, "failed_waterfall_differs")
    return bodies


def admit(controls, ref, head, output):
    require(RUN_ROOT in Path(ref["path"]).parents,"admission_outside_run_roots")
    value = decode(controls.read(ref))
    require(type(value) is dict and set(value) == {"schema_version","status","consumer_head","run_dir","caps",
            "metadata_terminal","prior_ledger","prior_usage"}
            and value["schema_version"] == "polymarket_solc_acquisition_admission_v2"
            and value["status"] == "root_terminal_metadata_solc_admitted"
            and value["consumer_head"] == head and value["run_dir"] == str(output) and value["caps"] == CAPS,
            "root_admission_schema")
    terminal = value["metadata_terminal"]
    require(type(terminal) is dict and set(terminal) == {"root",*TERMINAL_PINS}
            and terminal["root"] == str(FAILED_ROOT)
            and all(terminal[key] == pin for key,pin in TERMINAL_PINS.items()),"metadata_terminal_reference_schema")
    root = Path(terminal["root"])
    saved = {key:decode(controls.read(terminal[key])) for key in TERMINAL_PINS}
    start,failure,parent,launch,exit_receipt,drain = (saved[k] for k in ("start","failure","admission","launch","exit","drain"))
    require(start.get("schema_version") == "polymarket_market_metadata_campaign_v3"
            and start.get("source",{}).get("head") == PRODUCER_HEAD
            and all(start.get("admission",{}).get(k) == terminal["admission"][k] for k in ("path","bytes","sha256"))
            and parent.get("schema_version") == "polymarket_market_metadata_campaign_admission_v3"
            and parent.get("status") == "root_metadata_campaign_admitted"
            and parent.get("run_dir") == str(root) and parent.get("consumer_head") == PRODUCER_HEAD,
            "metadata_parent_or_start_binding")
    require(failure.get("status") == "failed_unaccepted" and failure.get("error_code") == "response_byte_limit"
            and failure.get("universe_complete") is False and failure.get("analysis_admitted") is False
            and start.get("universe_complete") is False and start.get("analysis_admitted") is False
            and not (root/"COMPLETE.json").exists() and not (root/"manifest.json").exists(),
            "metadata_terminal_not_expected_failure")
    command = ["/home/ubuntu/venv/bin/python","scripts/collect_polymarket_market_metadata_campaign_v3.py",
        "--admission",terminal["admission"]["path"],"--admission-sha256",terminal["admission"]["sha256"],
        "--expected-head",PRODUCER_HEAD,"--run-dir",str(root)]
    require(launch.get("command") == command and launch.get("source_head") == PRODUCER_HEAD
            and type(launch.get("pid")) is int and launch["pid"] > 0
            and type(launch.get("linux_birth_ticks")) is int and launch["linux_birth_ticks"] > 0
            and type(launch.get("boot_id")) is str and launch["boot_id"]
            and exit_receipt.get("launch") == launch and type(exit_receipt.get("exit_code")) is int
            and exit_receipt["exit_code"] == 1, "metadata_launch_exit_binding")
    receipts = drain.get("process_receipts")
    require(drain.get("schema_version") == "polymarket_oct1_terminal_shutdown_drain_v1"
            and drain.get("source_head") == PRODUCER_HEAD and drain.get("source_clean") is True
            and drain.get("metadata_failure") == terminal["failure"] and drain.get("boot_id") == launch["boot_id"]
            and drain.get("agents_and_transfers_released") is True and drain.get("shutdown_ready") is True
            and all(drain.get(k) == [] for k in ("live_receipt_births","unknown_process_receipts",
                "owned_live_groups_or_sessions","relevant_compute_notebook_transfer_consumers"))
            and type(receipts) is list and any(r.get("binding") == terminal["launch"]
                and r.get("pid") == launch["pid"] and r.get("birth_ticks") == launch["linux_birth_ticks"]
                and r.get("receipt_boot_id") == launch["boot_id"] and r.get("matching_live_birth") is False
                for r in receipts), "metadata_terminal_drain_binding")
    # This proves only the saved terminal boundary, never present host idleness.
    for key,number in (("aggregate_metadata_bytes",8_000_000_000),("raw_body_bytes",4_000_000_000),
                       ("control_bytes",500_000_000),("other_metadata_bytes",3_500_000_000),
                       ("http_attempts",40000),("total_response_bytes",32*1024**3),("runtime_seconds",86400)):
        require(parent.get("caps",{}).get(key) == start.get("caps",{}).get(key) == number,
                "metadata_partition_contract")
    inherited = parent.get("prior_ledger",{})
    require(inherited.get("all_prior_metadata_roots_included") is True and type(inherited.get("roots")) is list
            and len(inherited["roots"]) == 4 and len(set(inherited["roots"])) == 4
            and type(inherited.get("files")) is list, "four_prior_metadata_roots_missing")
    expected = {}
    def include(item):
        require(type(item) is dict and type(item.get("path")) is str and type(item.get("bytes")) is int
                and item["bytes"] >= 0 and type(item.get("sha256")) is str and SHA.fullmatch(item["sha256"])
                and item.get("category") in {"raw","control","other"}, "producer_inventory_entry")
        projected = {k:item[k] for k in ("path","bytes","sha256","category")}
        require(item["path"] not in expected or expected[item["path"]] == projected,"producer_inventory_conflict")
        expected[item["path"]] = projected
    for item in inherited["files"]:include(item)
    for pin in terminal.values():
        if type(pin) is dict:include({**pin,"category":"control"})
    ledger = value["prior_ledger"]
    require(type(ledger) is dict and set(ledger) == {"all_prior_metadata_roots_included","roots",
            "standalone_control_files","files","retained_bytes_charged"}
            and ledger["all_prior_metadata_roots_included"] is True and type(ledger["roots"]) is list
            and 5 <= len(ledger["roots"]) <= CAPS["prior_roots"] and type(ledger["files"]) is list
            and 1 <= len(ledger["files"]) <= CAPS["prior_files"] and type(ledger["standalone_control_files"]) is list,
            "terminal_prior_ledger_schema")
    roots = [Path(name) for name in ledger["roots"]]
    require(len(set(roots)) == len(roots) and set(inherited["roots"])|{str(root)} <= set(ledger["roots"])
            and all(a == b or a not in b.parents and b not in a.parents for a in roots for b in roots),
            "prior_root_hierarchy")
    actual = tree_paths(roots,output,controls.deadline)
    standalone = ledger["standalone_control_files"]
    require(len(standalone) <= CAPS["prior_files"] and len(set(standalone)) == len(standalone),"standalone_roster")
    for name in standalone:
        path=Path(name)
        require(RUN_ROOT in path.parents and name not in actual and path != Path(ref["path"])
                and output not in path.parents and not any(r == path or r in path.parents for r in roots),
                "standalone_control_overlap")
        identity(path);actual.add(name)
    entries,by_path,physical = [],{},set();observed={"raw":0,"control":0,"other":0}
    for item in ledger["files"]:
        require(type(item) is dict and set(item) == {"path","bytes","sha256","category","identity"}
                and item.get("category") in observed and type(item["path"]) is str
                and type(item["bytes"]) is int and item["bytes"] >= 0 and type(item["sha256"]) is str
                and SHA.fullmatch(item["sha256"]) and type(item["identity"]) is dict
                and set(item["identity"]) == STAT_FIELDS
                and all(type(n) is int and n >= 0 for n in item["identity"].values()),"prior_entry_schema")
        path=Path(item["path"]);state=identity(path);inode=(state["device"],state["inode"])
        require(item["path"] in actual and item["path"] not in by_path and inode not in physical
                and state == item["identity"] and state["bytes"] == item["bytes"],"prior_entry_identity_or_uniqueness")
        require(item["path"] not in expected or all(item[k] == expected[item["path"]][k]
                for k in ("bytes","sha256","category")),"accepted_prior_binding_differs")
        require(item["path"] not in standalone or item["category"] == "control","standalone_not_control")
        if path.name.endswith((".body",".body.gz",".raw")) or "responses" in path.parts:
            require(item["category"] == "raw","prior_raw_body_misclassified")
        if item["category"] == "control" and item["bytes"]:controls.read(short_ref(item))
        elif item["category"] == "control":require(item["sha256"] == digest(b""),"empty_control_hash_differs")
        entries.append(item);by_path[item["path"]]=item;physical.add(inode);observed[item["category"]]+=item["bytes"]
    require(set(by_path) == actual and set(expected) <= actual,"prior_ledger_coverage")
    journal_path = str(root/"attempts.jsonl")
    require(journal_path in by_path and by_path[journal_path]["category"] == "control","failed_journal_missing")
    expected.update({path:{"path":path,**body} for path,body in
        failed_journal(controls,by_path[journal_path],failure,inherited,by_path,root).items()})
    literal_failed_paths = {str(root/name) for name in ("START.json","FAILURE.json","attempts.jsonl","pages.jsonl")}
    literal_failed_paths |= {p for p in expected if root in Path(p).parents}
    require({p for p in actual if root in Path(p).parents} == literal_failed_paths,"failed_root_lineage_coverage")
    require({p for p,item in by_path.items() if item["category"] == "raw"}
            == {p for p,item in expected.items() if item["category"] == "raw"},"raw_lineage_coverage")
    usage = value["prior_usage"]
    require(type(usage) is dict and set(usage) == set(META_MINIMA),"prior_usage_schema")
    for key,ceiling in META_CEILINGS.items():
        n=nonnegative(usage[key],ceiling,integer=key!="runtime_seconds")
        failure_key="runtime_seconds_charged" if key=="runtime_seconds" else "http_attempts_charged" if key=="http_attempts" else key
        require(n >= META_MINIMA[key] and n >= nonnegative(failure["usage"].get(failure_key),ceiling,
                integer=key!="runtime_seconds"),"prior_usage_below_terminal_minimum")
    require(usage["runtime_seconds"] >= nonnegative(drain.get("metadata_conservative_runtime_reservation_seconds"),86400,
            integer=False),"prior_runtime_below_root_drain")
    require(usage["http_attempts"]+CAPS["http_attempts"] <= META_CEILINGS["http_attempts"]
            and usage["response_bytes_charged"]+CAPS["total_response_bytes_charged"] <= META_CEILINGS["response_bytes_charged"]
            and usage["runtime_seconds"]+CAPS["runtime_seconds"] <= META_CEILINGS["runtime_seconds"],
            "metadata_cumulative_reservation")
    totals=ledger["retained_bytes_charged"]
    require(type(totals) is dict and set(totals) == set(observed),"prior_retained_charge_schema")
    retained=failure["usage"].get("retained_bytes_charged",{})
    for key in observed:
        require(nonnegative(totals[key],CAPS["aggregate_metadata_bytes"]) >= observed[key]
                and totals[key] >= nonnegative(retained.get(key),CAPS["aggregate_metadata_bytes"]),
                "prior_retained_charge_understated")
    require(totals["raw"] <= CAPS["raw_metadata_bytes"]
            and totals["control"]+ref["bytes"]+CAPS["external_root_control_reservation_bytes"]+CAPS["terminal_control_reserve_bytes"]
                <= CAPS["metadata_control_bytes"]
            and totals["other"]+CAPS["all_new_artifact_bytes"] <= CAPS["other_metadata_bytes"]
            and sum(totals.values())+CAPS["all_new_artifact_bytes"] <= CAPS["aggregate_metadata_bytes"],
            "metadata_partition_reservation")
    require(controls.read_bytes+sum(item["bytes"] for item in controls.bindings.values())
            +CAPS["owned_control_reopen_reservation_bytes"] <= CAPS["all_small_control_read_bytes"],
            "final_control_reopen_reservation")
    return value,entries,roots,standalone,actual,totals


def atomic_commit(pending, target):
    library=ctypes.CDLL(None,use_errno=True)
    if hasattr(library,"renameat2"): result=library.renameat2(-100,os.fsencode(pending),-100,os.fsencode(target),1)
    elif hasattr(library,"renamex_np"): result=library.renamex_np(os.fsencode(pending),os.fsencode(target),4)
    else: raise Blocked("atomic_publication_unavailable")
    require(result==0,"atomic_publication_refused")


class Artifacts:
    def __init__(self, root, deadline, prior, admission_bytes, controls):
        self.root,self.deadline,self.prior,self.admission_bytes,self.controls,self.files=root,deadline,prior,admission_bytes,controls,{}

    def reserve(self, extra=0, *, failure=False, terminal=False):
        if not failure:self.deadline()
        total=self.admission_bytes
        for path in self.root.iterdir():
            require(path.name in self.files,"unexpected_artifact");total+=identity(path)["bytes"]
        reserve=CAPS["external_root_control_reservation_bytes"]+(0 if terminal else CAPS["terminal_control_reserve_bytes"])
        require(total+extra+reserve<=CAPS["all_new_artifact_bytes"],"artifact_byte_limit")
        require(self.prior+total+extra+reserve<=CAPS["aggregate_metadata_bytes"],"metadata_aggregate_limit")
        require(shutil.disk_usage(self.root).free>=CAPS["minimum_free_bytes"]+extra+reserve,"free_space_floor")

    def create(self,name):
        require(Path(name).name==name and name not in self.files and not (self.root/name).exists(),"artifact_exists")
        self.files[name]=None
        return (self.root/name).open("xb")

    def seal(self,name):
        path=self.root/name;before=identity(path);require(before["bytes"]<=CAPS["all_new_artifact_bytes"],"artifact_byte_limit")
        if ".json" in name:self.controls.charge_owned(before["bytes"])
        hasher=hashlib.sha256();remaining=before["bytes"]
        with path.open("rb") as stream:
            while remaining:
                raw=stream.read(min(remaining,64*1024));require(raw,"artifact_short_read");hasher.update(raw);remaining-=len(raw)
            require(stream.read(1)==b"","artifact_grew")
        require(identity(path)==before,"artifact_identity_changed")
        bound={"path":name,"bytes":before["bytes"],"sha256":hasher.hexdigest()}
        self.files[name]=bound;return bound

    def json(self,name,value,*,failure=False,final_check=None):
        raw=encode(value);require(len(raw)<=1024**2,"control_output_limit")
        prior_controls=sum((self.root/key).stat().st_size for key in self.files if ".json" in key)
        require(prior_controls+len(raw)<=CAPS["owned_control_artifact_bytes"],"owned_control_artifact_limit")
        self.reserve(len(raw),failure=failure,terminal=final_check is not None)
        pending="."+name+".pending"
        with self.create(pending) as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
        bound=self.seal(pending);require(bound["sha256"]==digest(raw),"control_reopen_differs")
        fd=os.open(self.root,os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
        try:os.fsync(fd)
        finally:os.close(fd)
        if final_check is not None:
            final_check();require(self.seal(pending)==bound,"pending_marker_changed")
        self.reserve(failure=failure,terminal=final_check is not None)
        atomic_commit(self.root/pending,self.root/name)
        self.files.pop(pending);self.files[name]={**bound,"path":name};return self.files[name]

    def reopen(self):
        for name,bound in list(self.files.items()):require(bound is not None and self.seal(name)==bound,"owned_artifact_changed")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def transport(timeout):
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    request=urllib.request.Request(URL,headers=HEADERS,method="GET")
    try:return opener.open(request,timeout=timeout)
    except urllib.error.HTTPError as response:return response


@contextmanager
def attempt_alarm(writer, attempt_ends):
    """Interrupt the whole normal attempt without refunding the outer timer."""
    require(threading.current_thread() is threading.main_thread(),"main_thread_attempt_timer_required")
    began=time.monotonic()
    prior_handler=signal.getsignal(signal.SIGALRM)
    prior_delay,prior_interval=signal.getitimer(signal.ITIMER_REAL)
    require(prior_interval==0,"unsupported_existing_interval_timer")
    prior_ends=began+prior_delay if prior_delay else None
    local=attempt_ends-writer.now();whole=writer.remaining()
    delay=min(local,whole,prior_delay if prior_delay else float("inf"))
    require(delay>0,"http_attempt_timeout" if local<=whole else "runtime_limit")
    reason="http_attempt_timeout" if local<whole and (not prior_delay or local<prior_delay) else "runtime_limit"
    alarm_ends=began+delay
    def expired(*_):raise Blocked(reason)
    def rearm():
        # Never grant cleanup a new transport interval after a consumed alarm.
        remaining=alarm_ends-time.monotonic();require(remaining>0,reason)
        signal.setitimer(signal.ITIMER_REAL,remaining)
    try:
        signal.signal(signal.SIGALRM,expired);rearm();yield rearm
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        signal.signal(signal.SIGALRM,prior_handler)
        if prior_ends is not None:
            signal.setitimer(signal.ITIMER_REAL,max(0.000001,prior_ends-time.monotonic()))


def attempt(writer, number, allowance, http):
    attempt_ends=writer.now()+CAPS["http_timeout_seconds"]
    def attempt_check():
        writer.deadline()
        require(writer.now()<attempt_ends,"http_attempt_timeout")
    response=None;observed=retained=0;status=None;code=None;transient=False;opaque=False;interrupted=None;length=None;inflight=False
    bound=None;response_close_attempted=False;response_close_completed=False
    name=f"attempt{number:02d}.part";stream=None
    def retain(raw):
        nonlocal observed,retained
        require(type(raw) is bytes,"response_body_type")
        amount=min(len(raw),max(0,CAPS["body_bytes_per_attempt"]-retained))
        observed+=len(raw)
        if amount:
            # Returned bytes have already been consumed. Preserve the bounded
            # prefix before checking elapsed time, without relaxing disk/byte caps.
            writer.reserve(amount,failure=True);require(stream.write(raw[:amount])==amount,"body_short_write");stream.flush();retained+=amount
    def failed(exc):
        nonlocal code,opaque,transient,interrupted
        code=str(exc) if isinstance(exc,Blocked) else "interrupted" if isinstance(exc,(KeyboardInterrupt,SystemExit)) else "response_transport_or_read_failure"
        opaque=True;transient=inflight and isinstance(exc,(urllib.error.URLError,TimeoutError,OSError))
        if isinstance(exc,(KeyboardInterrupt,SystemExit)):transient=False;interrupted=exc
    try:
        with attempt_alarm(writer,attempt_ends) as rearm:
            stream=writer.create(name)
            try:
                attempt_check();inflight=True;response=http(min(attempt_ends-writer.now(),writer.remaining()));inflight=False
                attempt_check();status=response.status
                require(type(status) is int and 100<=status<=599,"http_status_schema")
                require(response.geturl()==URL,"response_url_changed")
                encoding=response.headers.get("Content-Encoding")
                require(encoding in (None,"identity"),"response_encoding_not_identity")
                length=response.headers.get("Content-Length")
                if length is not None:
                    require(type(length) is str and length.isdecimal(),"content_length_schema")
                    length=int(length);require(length<=CAPS["body_bytes_per_attempt"],"response_byte_limit")
                while observed<allowance:
                    attempt_check()
                    try:
                        inflight=True;raw=response.read(min(64*1024,allowance-observed));inflight=False
                    except http_client.IncompleteRead as exc:
                        inflight=False;retain(exc.partial);attempt_check();opaque=True;code="incomplete_response";transient=True;break
                    retain(raw);attempt_check()
                    if not raw:break
                if observed>=allowance:code="response_byte_limit"
                if code is None and length is not None and observed!=length:
                    code="incomplete_response";opaque=True;transient=True
                if code is None and status!=200:
                    code="http_status_not_200";transient=status in TRANSIENT_HTTP
            except BaseException as exc:failed(exc)
            finally:
                if response is not None:
                    try:rearm();response_close_attempted=True;response.close();response_close_completed=True
                    except BaseException as exc:
                        code=str(exc) if isinstance(exc,Blocked) else "response_close_failed";opaque=True;transient=False
                        if isinstance(exc,(KeyboardInterrupt,SystemExit)):interrupted=exc;code="interrupted"
                try:attempt_check()
                except Blocked as exc:code=str(exc);opaque=True;transient=False
            if code is None:
                # Normal success, including durability and body authentication,
                # remains inside the same hard whole-attempt timer.
                attempt_check();stream.flush();os.fsync(stream.fileno());stream.close()
                bound={**writer.seal(name),"identity":identity(writer.root/name)}
                if bound["sha256"]!=COMPILER_SHA:code="compiler_sha_mismatch";transient=False
                attempt_check()
    except BaseException as exc:failed(exc)
    finally:
        # Only failed-attempt evidence may finish after its expired timer. The
        # original outer stage timer has been restored with elapsed time debited.
        if stream is not None and not stream.closed:
            try:stream.flush();os.fsync(stream.fileno())
            finally:stream.close()
    if bound is None:bound={**writer.seal(name),"identity":identity(writer.root/name)}
    charged=allowance if opaque or code=="response_byte_limit" else observed
    return {"http_status":status,"error_code":code,"transient":transient,"response_bytes_observed":observed,
        "response_bytes_charged":charged,"body":bound,"interrupted":interrupted,
        "response_close_attempted":response_close_attempted,"response_close_completed":response_close_completed}


def run_collection(admission,admission_sha,head,output,*,http=transport,clock=time.monotonic):
    output,admission=Path(output),Path(admission);started=clock()
    def deadline():require(clock()-started<CAPS["runtime_seconds"],"runtime_limit")
    require(output.is_absolute() and output.parent.is_dir() and output.parent.resolve()==output.parent
            and not output.exists() and not output.is_symlink(),"output_not_new_literal_directory")
    source=source_snapshot(head);controls=Controls(deadline)
    reference={"path":str(admission),"bytes":identity(admission)["bytes"],"sha256":admission_sha}
    contract,entries,roots,standalone,paths,totals=admit(controls,reference,head,output)
    prior=sum(totals.values())
    require(shutil.disk_usage(output.parent).free>=CAPS["minimum_free_bytes"]+CAPS["all_new_artifact_bytes"],"free_space_floor")
    output.mkdir(mode=0o700);writer=Artifacts(output,deadline,prior,reference["bytes"],controls)
    writer.now=clock
    writer.remaining=lambda:max(0,CAPS["runtime_seconds"]-(clock()-started))
    result={"schema_version":SCHEMA,"status":"blocked","source":source,"admission":reference,
        "url":URL,"compiler_version":VERSION,"compiler_sha256":COMPILER_SHA,"caps":CAPS,"prior_metadata_bytes":totals,
        "evidence_class":"public_toolchain_binary","prior_raw_body_sha_rehashed":False,"prior_other_body_sha_rehashed":False,
        "download_is_immutable_nonexecutable":True,"executable_copy_requires_separate_root_admission":True,
        "compiler_executed":False,"executable_permission_granted":False,"sample_admitted":False,"http_attempts":0,"market_api_calls":0,
        "response_bytes_charged":0,"attempts":[],"runtime_seconds_charged":CAPS["runtime_seconds"],
        "prior_metadata_usage":contract["prior_usage"],"metadata_pagination_succeeded":False,
        "terminal_drain_is_historical_not_current_host_idle":True}
    def update_cumulative():
        result["cumulative_metadata_usage"]={
            "http_attempts":contract["prior_usage"]["http_attempts"]+result["http_attempts"],
            "response_bytes_charged":contract["prior_usage"]["response_bytes_charged"]+result["response_bytes_charged"],
            "runtime_seconds":contract["prior_usage"]["runtime_seconds"]+CAPS["runtime_seconds"]}
        require(all(result["cumulative_metadata_usage"][k]<=META_CEILINGS[k] for k in META_CEILINGS),
                "metadata_cumulative_usage_limit")
    try:
        update_cumulative()
        writer.json("START.json",result)
        for number in range(1,CAPS["http_attempts"]+1):
            allowance=CAPS["response_bytes_per_attempt_including_detector"]
            require(result["response_bytes_charged"]+allowance<=CAPS["total_response_bytes_charged"],"response_charge_limit")
            writer.reserve(CAPS["body_bytes_per_attempt"]+65536)
            writer.json(f"attempt{number:02d}.START.json",{"attempt":number,"url":URL,"allowance":allowance})
            result["http_attempts"]+=1
            try:
                observed=attempt(writer,number,allowance,http)
            except BaseException:
                # A dispatched attempt with a failed staging/hash receipt is
                # never represented as zero network use or retried blindly.
                result["response_bytes_charged"]+=allowance
                failed={"error_code":"attempt_failed_before_sealed_receipt","response_bytes_charged":allowance,
                        "partial_artifact":f"attempt{number:02d}.part","body_integrity_verified":False,"transient":False}
                result["attempts"].append(failed)
                try:writer.json(f"attempt{number:02d}.FINISH.json",failed,failure=True)
                except BaseException:pass
                raise
            interrupted=observed.pop("interrupted")
            result["response_bytes_charged"]+=observed["response_bytes_charged"]
            result["attempts"].append(observed)
            writer.json(f"attempt{number:02d}.FINISH.json",observed,failure=observed["error_code"] is not None)
            if interrupted is not None:raise interrupted
            if observed["error_code"] is None:
                result["binary"]=observed["body"];break
            require(observed["transient"] and number<CAPS["http_attempts"],observed["error_code"])
        require("binary" in result,"authenticated_binary_missing")
        binary=result["binary"]
        require(not identity(output/binary["path"])["mode"]&0o111,"download_became_executable")
        result.update(status="authenticated_solc_binary_not_executed",small_control_read_bytes_at_result=controls.read_bytes,
            reserved_final_control_reopen_bytes=sum(x["bytes"] for x in controls.bindings.values()),
            reserved_owned_control_reopens=CAPS["owned_control_reopen_reservation_bytes"])
        update_cumulative();saved=writer.json("RESULT.json",result)
        def final_gate():
            writer.reopen();controls.reopen()
            require(tree_paths(roots,output,deadline)|set(standalone)==paths,"prior_hierarchy_changed")
            for item in entries:require(identity(Path(item["path"]))==item["identity"],"prior_identity_changed")
            require(source_snapshot(head)==source,"source_changed")
            require(writer.seal(binary["path"])==short_ref(binary) and identity(output/binary["path"])==binary["identity"]
                    and not identity(output/binary["path"])["mode"]&0o111,"binary_changed")
            deadline()
        writer.json("COMPLETE.json",{"status":result["status"],"result":saved,"binary":binary,
            "compiler_executed":False,"sample_admitted":False},final_check=final_gate)
    except BaseException as exc:
        result.update(status="blocked",error_code=str(exc) if isinstance(exc,Blocked) else "interrupted" if isinstance(exc,(KeyboardInterrupt,SystemExit)) else "acquisition_io_or_resource_failure")
        update_cumulative()
        try:writer.json("FAILED.json",result,failure=True)
        except BaseException:result["failure_receipt_published"]=False
        if isinstance(exc,(KeyboardInterrupt,SystemExit)):raise
    return result


@contextmanager
def process_limits():
    old=resource.getrlimit(resource.RLIMIT_AS);handler=signal.getsignal(signal.SIGALRM)
    try:
        value=min(old[0],CAPS["address_space_bytes"]) if old[0]>=0 else CAPS["address_space_bytes"]
        resource.setrlimit(resource.RLIMIT_AS,(value,old[1]))
        signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(Blocked("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL,CAPS["runtime_seconds"]);yield
    finally:
        signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,handler);resource.setrlimit(resource.RLIMIT_AS,old)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission",type=Path,required=True);parser.add_argument("--admission-sha256",required=True)
    parser.add_argument("--expected-head",required=True);parser.add_argument("--output-dir",type=Path,required=True)
    args=parser.parse_args(argv);require_production_host()
    try:
        with process_limits():
            require(Path(sys.executable)==Path("/home/ubuntu/venv/bin/python") and args.output_dir.parent==Path("/mnt/data/runs"),"production_paths")
            require(shared.resource_state()["ram_bytes"]["MemAvailable"]>=CAPS["address_space_bytes"],"ram_floor")
            result=run_collection(args.admission,args.admission_sha256,args.expected_head,args.output_dir)
    except (Blocked,OSError,KeyError):result={"status":"blocked","error_code":"acquisition_admission_failed"}
    print(json.dumps(result,sort_keys=True));return 0 if result["status"]=="authenticated_solc_binary_not_executed" else 2


if __name__=="__main__":raise SystemExit(main())
