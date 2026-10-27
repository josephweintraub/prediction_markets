"""One fresh legacy V3 RAW child, independently checked and parent-sealed.

The child COMPLETE is never admission. No resume, parent RPC, economic decoding,
first-deployment claim, exact interior clock, or final-sample certification.
Root supplies a SHA-bound accounting receipt covering ALL earlier campaigns and
diagnostics. Fixed profile reservations are inclusive, not capacity forecasts.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_native_increment as child
from scripts import probe_polymarket_v3_history_v2 as shared

inventory, pilot, planner = child.inventory, child.pilot, child.planner
require, encode, digest = shared.require, shared.encode, shared.digest
ADDRESS = shared.ADDRESS
PLAN_SHA = "b07dfa71169029913a2281a2cb9df3d567d620be89aaa14b84eca5e674e0f688"
PLAN_HEAD = "c39ddce3d9a010fe65faac652359fd401561e877"
SPARSE_SHA = "faebc665f82de3a66ae73bdb1ea96a089ee39fca8be8a20cdbb27046eb87e10b"
FIRST, TAIL, CUTOFF = 87479447, 88978537, 94796039
RAM, PARENT_LIVE_RAM = 8 * 1024**3, 256 * 1024**2
FREE_FLOOR, CONTROL_CAP = 20_000_000_000, 1_000_000
GLOBAL_CAPS = {"retained_bytes": 128_000_000_000, "rpc_calls": 900_000,
               "response_bytes": 3_000_000_000_000, "runtime_seconds": 172_800}
KNOWN_MINIMA = {"retained_bytes": 39_161_003_314, "rpc_calls": 497_667,
                "response_bytes": 571_146_511_990}
PROFILES = {
    "probe": {"first_block": 88977538, "last_block": TAIL, "rpc_calls": 100,
              "retained_bytes": 128_000_000, "runtime_seconds": 1200, "rows": 200_000,
              "response_bytes": 256 * 1024**2, "child_output": 127_000_000, "child_seconds": 1000},
    "full": {"first_block": FIRST, "last_block": TAIL, "rpc_calls": 120_000,
             "retained_bytes": 8_000_000_000, "runtime_seconds": 7200, "rows": 5_000_000,
             "response_bytes": 16_000_000_000, "child_output": 7_900_000_000, "child_seconds": 6600},
}
SOURCE_FILES = tuple(dict.fromkeys((*shared.SOURCE_FILES,
    "scripts/probe_polymarket_increment_capacity.py", "scripts/collect_polymarket_native_increment.py",
    "scripts/run_polymarket_v3_historical_collection_v1.py")))
SUCCESS = "accepted_v3_historical_raw_not_analysis_admission"
UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def number(value, *, integer=False):
    require(type(value) is int if integer else type(value) in (int, float), "numeric_counter_invalid")
    require(math.isfinite(value) and value >= 0, "numeric_counter_invalid")
    return value


def read_bound(path: Path, sha: str, maximum=64 * 1024**2, *, json_lines=False):
    require(type(sha) is str and re.fullmatch(r"[0-9a-f]{64}", sha), "input_sha_invalid")
    before = inventory.path_identity(path)
    require(before.get("status") == "present" and not before.get("is_symlink")
            and path.is_file() and 0 < path.stat().st_size <= maximum, "input_not_bounded_regular")
    raw = path.read_bytes()
    require(before == inventory.path_identity(path) and digest(raw) == sha, "input_hash_or_identity_differs")
    def decode(data):
        value = json.loads(data, object_pairs_hook=inventory.unique_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(shared.HistoryBlocked("nonfinite_input_json")))
        require(type(value) is dict, "input_not_object")
        return value
    value = [decode(line) for line in raw.splitlines()] if json_lines else decode(raw)
    return value, {"path": str(path), "sha256": sha, "identity": before}


def read_ref(reference: dict, maximum=64 * 1024**2):
    require(type(reference) is dict and set(reference) == {"path", "sha256"}, "reference_shape_invalid")
    path = Path(reference["path"])
    require(path.is_absolute(), "reference_not_absolute")
    return read_bound(path, reference["sha256"], maximum)


def source_snapshot(head: str):
    state = inventory.code_state()
    require(type(head) is str and re.fullmatch(r"[0-9a-f]{40}", head)
            and state.get("status") == "inspected" and state.get("head") == head
            and state.get("tracked_worktree_clean") is True, "source_head_not_clean")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = inventory.path_identity(path)
        require(before.get("status") == "present" and not before.get("is_symlink")
                and path.is_file() and 0 < path.stat().st_size <= 1024**2, "source_file_invalid")
        saved = subprocess.run(["git", "show", f"{head}:{name}"], cwd=REPO,
                               capture_output=True, timeout=10)
        require(saved.returncode == 0 and saved.stdout == path.read_bytes()
                and before == inventory.path_identity(path), "source_blob_uncommitted_or_changed")
        files[name] = {"sha256": digest(saved.stdout), "identity": before}
    require(state == inventory.code_state(), "source_changed_during_snapshot")
    return {"head": head, "files": files, "code_state": state}


def validate_sparse(value: dict, binding: dict, plan: dict):
    require(value.get("schema_version") == "polymarket_v3_sparse_history_probe_v2"
            and value.get("status") == "bounded_v3_sparse_history_complete"
            and value.get("address") == ADDRESS and value.get("chain_id") == 137
            and all(value.get(key) is True for key in ("zero_prefix_verified", "conservative_event_start_supported",
                "requested_upgraded_event_ledger_complete", "final_input_identity_unchanged", "reopened_headers_unchanged"))
            and value.get("safe_event_collection_start_block") == FIRST
            and value.get("source_plan_binding") == binding, "sparse_result_not_admitted")
    calls = value.get("request_ledger")
    require(type(calls) is list and len(calls) == value.get("rpc_calls_attempted")
            and [item.get("id") for item in calls] == list(range(1, len(calls) + 1)), "sparse_request_ids_invalid")
    for name, end, topic in (("zero_prefix_ranges", FIRST - 1, None),
                             ("upgrade_ranges", CUTOFF, shared.UPGRADED_TOPIC)):
        ranges = value.get(name)
        require(type(ranges) is list and ranges, "sparse_ranges_absent")
        previous, count = -1, 0
        for item in ranges:
            first, last = item.get("first_block"), item.get("last_block")
            require(type(first) is int and type(last) is int and first == previous + 1 and first <= last <= end,
                    "sparse_literal_range_gap_overlap")
            identifier = item.get("request_id")
            require(type(identifier) is int and 1 <= identifier <= len(calls), "sparse_request_reference_invalid")
            request = calls[identifier - 1]
            query = {"address": ADDRESS, "fromBlock": hex(first), "toBlock": hex(last)}
            if topic:
                query["topics"] = [topic]
            amount = number(item.get("log_count"), integer=True)
            require(request.get("method") == "eth_getLogs" and request.get("params") == [query]
                    and request.get("status") == "range_validated" and request.get("returned_log_count") == amount
                    and request.get("response_sha256") == item.get("response_sha256"), "sparse_predicate_or_count_differs")
            require(topic is not None or amount == 0, "sparse_prefix_nonzero")
            previous, count = last, count + amount
        require(previous == end, "sparse_literal_range_incomplete")
        if topic:
            require(type(value.get("upgrades")) is list and count == len(value["upgrades"]), "sparse_upgrade_count_differs")
    headers = value.get("headers")
    require(type(headers) is list and len({item.get("number") for item in headers}) == len(headers), "sparse_headers_invalid")
    indexed = {item["number"]: item for item in headers}
    for name in ("raw_footer_tail_header", "last_included_header", "first_excluded_header"):
        require(indexed.get(plan[name]["number"]) == plan[name], "sparse_plan_header_differs")
    require(FIRST in indexed and FIRST - 1 in indexed
            and indexed[FIRST]["parentHash"] == indexed[FIRST - 1]["hash"], "sparse_first_header_not_bound")
    upgrades = value.get("upgrades")
    require(type(upgrades) is list and len(upgrades) == 1
            and upgrades[0].get("block_number") == shared.prior.UPGRADE_BLOCK
            and upgrades[0].get("transaction_hash") == shared.prior.UPGRADE_TRANSACTION
            and upgrades[0].get("transaction_index") == 62
            and upgrades[0].get("log_index") == shared.prior.UPGRADE_LOG_INDEX
            and upgrades[0].get("new_implementation") == shared.prior.NEW_IMPLEMENTATION,
            "sparse_known_upgrade_differs")
    proofs, receipts = value.get("upgrade_block_proofs"), value.get("receipt_proofs")
    require(type(proofs) is list and len(proofs) == 1 and type(receipts) is list and len(receipts) == 1
            and proofs[0].get("in_block_upgrades") == upgrades
            and proofs[0].get("before_block_slot", {}).get("implementation_address") == shared.prior.OLD_IMPLEMENTATION
            and proofs[0].get("after_block_slot", {}).get("implementation_address") == shared.prior.NEW_IMPLEMENTATION
            and proofs[0].get("whole_block_implementation_assignment_admitted") is False
            and receipts[0].get("receipt_status") == 1
            and receipts[0].get("transaction_hash") == upgrades[0]["transaction_hash"]
            and receipts[0].get("upgrade_log_indices") == [shared.prior.UPGRADE_LOG_INDEX], "sparse_upgrade_proofs_missing")
    return indexed[FIRST]


def global_admission(value: dict, profile: dict):
    require(value.get("schema_version") == "polymarket_v3_historical_global_admission_v1"
            and value.get("status") == "root_global_budget_admitted"
            and value.get("all_prior_roots_and_diagnostics_included") is True, "root_global_admission_missing")
    manifest, mb = read_ref(value.get("v2_manifest"))
    marker, cb = read_ref(value.get("v2_complete"))
    owner, pb = read_ref(value.get("v2_plan"))
    semantic = digest(child.json_bytes(owner.get("plan")))
    campaign = Path(cb["path"]).resolve().parent
    relative = Path(marker.get("manifest_path", ""))
    require(not relative.is_absolute() and ".." not in relative.parts
            and (campaign / relative).resolve() == Path(mb["path"]).resolve()
            and Path(pb["path"]).resolve() == campaign / "PLAN.json", "v2_reference_path_differs")
    require(marker.get("status") == "raw_campaign_complete_not_analysis_admission"
            and manifest.get("schema_version") == "polymarket_native_campaign_manifest_v2"
            and manifest.get("status") == "raw_campaign_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True
            and marker.get("manifest_sha256") == mb["sha256"]
            and owner.get("plan_sha256") == marker.get("plan_sha256") == manifest.get("plan_sha256") == semantic
            and owner.get("run_id") == marker.get("run_id") == manifest.get("run_id")
            and owner["plan"].get("source_plan", {}).get("sha256") == PLAN_SHA, "v2_completion_binding_differs")
    refs = value.get("v2_session_results")
    require(type(refs) is list and 0 < len(refs) <= 256, "v2_session_references_invalid")
    sessions = campaign / "sessions"
    require(sessions.is_dir() and not sessions.is_symlink(), "v2_sessions_not_regular")
    expected = set()
    for directory in sessions.iterdir():
        require(directory.is_dir() and not directory.is_symlink(), "v2_session_not_regular")
        expected.add(str((directory / "RESULT.json").resolve()))
    observed, runtime, retained, session_calls, session_responses = set(), 0.0, 0, 0, 0
    bindings = [mb, cb, pb]
    for reference in refs:
        ended, eb = read_ref(reference)
        path = str(Path(eb["path"]).resolve())
        require(path not in observed and path in expected
                and ended.get("run_id") == owner["run_id"] and ended.get("plan_sha256") == semantic
                and ended.get("children_still_running") is False
                and ended.get("analysis_admitted") is False
                and ended.get("status") in {"raw_campaign_complete", "failed_raw_campaign"}
                and ended.get("collection_complete") is (ended.get("status") == "raw_campaign_complete"),
                "v2_session_binding_differs")
        observed.add(path)
        runtime += number(ended.get("session_elapsed_seconds"))
        retained = max(retained, number(ended.get("aggregate_bytes"), integer=True))
        session_calls = max(session_calls, number(ended.get("rpc_calls_charged"), integer=True))
        session_responses = max(session_responses, number(ended.get("response_bytes_charged"), integer=True))
        bindings.append(eb)
    require(observed == expected and any(read_ref(ref)[0].get("status") == "raw_campaign_complete" for ref in refs),
            "v2_sessions_incomplete")
    runtime += number(owner["plan"].get("reuse", {}).get("runtime_seconds"))
    runtime += number(owner["plan"].get("reboot_admission", {}).get("prep_charge", {}).get("runtime_seconds"))
    minima = {"retained_bytes": max(KNOWN_MINIMA["retained_bytes"], retained),
              "rpc_calls": max(KNOWN_MINIMA["rpc_calls"], session_calls, number(manifest.get("rpc_calls_charged"), integer=True)),
              "response_bytes": max(KNOWN_MINIMA["response_bytes"], session_responses, number(manifest.get("response_bytes_charged"), integer=True)),
              "runtime_seconds": runtime}
    prior = {}
    for key, cap in GLOBAL_CAPS.items():
        used = number(value.get("prior_" + key), integer=key != "runtime_seconds")
        require(used >= minima[key] and used + profile[key] <= cap, "global_prior_or_reservation_not_admitted")
        prior[key] = used
    return {"prior": prior, "minima": minima, "bound_inputs": bindings}


def expected_contract(plan, binding, source, profile):
    return {"schema_version": "polymarket_native_collection_contract_v1", "source_plan": binding,
            "source": source["code_state"], "mode": "v3-historical", "first_block": profile["first_block"],
            "last_block": profile["last_block"], "window_blocks": 100, "addresses": [ADDRESS],
            "topics": [pilot.EXPECTED_TOPICS[ADDRESS]],
            "limits": child.limits(profile["rpc_calls"], profile["child_output"], profile["child_seconds"],
                profile["rows"], RAM - PARENT_LIVE_RAM, profile["response_bytes"]),
            "end_exclusive_utc": plan["end_exclusive_utc"], "raw_payload_only": True,
            "interior_exact_timestamps_collected": False, "analysis_admitted": False,
            "provider_block_timestamp_used_as_clock": False}


def tree_digest(root: Path, cap: int, check=lambda: None):
    require(root.is_dir() and not root.is_symlink(), "child_tree_not_regular")
    total, files, rolling = 0, 0, hashlib.sha256()
    for directory, names, filenames in os.walk(root, followlinks=False):
        names.sort()
        require(all(not (Path(directory) / name).is_symlink() for name in names), "child_tree_symlink")
        for name in sorted(filenames):
            check()
            path = Path(directory) / name
            before = inventory.path_identity(path)
            require(before.get("status") == "present" and not before.get("is_symlink") and path.is_file(), "child_tree_file_invalid")
            size = before["target"]["bytes"]
            total, files = total + size, files + 1
            require(total <= cap and files <= 1_000_000, "child_tree_output_or_file_cap")
            hashed = hashlib.sha256()
            with path.open("rb") as stream:
                for data in iter(lambda: stream.read(1024**2), b""):
                    check()
                    hashed.update(data)
            require(before == inventory.path_identity(path), "child_tree_changed_during_hash")
            rolling.update(encode({"path": str(path.relative_to(root)), "bytes": size, "sha256": hashed.hexdigest()}))
    return {"bytes": total, "files": files, "sha256": rolling.hexdigest()}


def validate_child(raw: Path, expected: dict, plan: dict, first_header: dict, *, exit_code: int,
                   stdout_result: dict, check=lambda: None):
    require(type(exit_code) is int and exit_code == 0, "child_exit_not_zero")
    caps, contract_sha = expected["limits"], digest(child.json_bytes(expected))
    owner = child.load_json(raw / "OWNER.json")
    require(type(owner.get("run_id")) is str and UUID.fullmatch(owner["run_id"])
            and owner.get("contract") == expected and owner.get("contract_sha256") == contract_sha,
            "child_owner_contract_differs")
    marker = child.load_json(raw / "COMPLETE.json")
    manifest_path = raw / "manifest.json"
    manifest = child.load_json(manifest_path)
    manifest_raw = manifest_path.read_bytes()
    require(marker == {"status": "raw_collection_complete_not_analysis_admission", "run_id": owner["run_id"],
            "contract_sha256": contract_sha, "manifest_sha256": digest(manifest_raw)}, "child_complete_differs")
    require(manifest.get("schema_version") == "polymarket_native_collection_manifest_v1"
            and manifest.get("status") == "raw_collection_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True
            and manifest.get("run_id") == owner["run_id"] and manifest.get("contract") == expected
            and manifest.get("contract_sha256") == contract_sha, "child_manifest_contract_differs")
    allowed_root = {"OWNER.json", "LOCK", "attempts", "sessions", "manifest.json", "COMPLETE.json"}
    require({path.name for path in raw.iterdir()} == allowed_root, "child_root_entries_differs")
    sessions = list((raw / "sessions").iterdir())
    require(len(sessions) == 1 and sessions[0].is_dir() and not sessions[0].is_symlink(), "child_not_one_fresh_session")
    session = sessions[0]
    require({path.name for path in session.iterdir()} == {"START.json", "RESULT.json", "rpc"}, "child_session_entries_differs")
    start, ended = child.load_json(session / "START.json"), child.load_json(session / "RESULT.json")
    require(start.get("contract_sha256") == contract_sha and start.get("resume") is False
            and ended == stdout_result and ended.get("schema_version") == "polymarket_native_collection_session_v1"
            and ended.get("status") == "raw_collection_complete" and ended.get("collection_complete") is True
            and ended.get("analysis_admitted") is False and ended.get("endpoint_disclosed") is False
            and ended.get("run_id") == owner["run_id"]
            and ended.get("contract_sha256") == contract_sha and "error_code" not in ended,
            "child_session_result_contradiction")
    planned = child.windows(expected["first_block"], expected["last_block"], 100)
    records = manifest.get("windows")
    require(type(records) is list, "child_window_records_absent")
    child.verify_coverage(records, planned, (ADDRESS,))
    previous_header = None
    for record, window in zip(records, planned):
        for name, block in (("first_header", window["first_block"]), ("last_header", window["last_block"])):
            header = record.get(name)
            require(type(header) is dict and set(header) == {"number", "timestamp", "hash", "parentHash"}
                    and type(header["number"]) is int and header["number"] == block
                    and type(header["timestamp"]) is int and 0 < header["timestamp"] <= 253402300799
                    and type(header["hash"]) is str and inventory.HEX_HASH.fullmatch(header["hash"])
                    and type(header["parentHash"]) is str and inventory.HEX_HASH.fullmatch(header["parentHash"]),
                    "child_header_shape_or_block_differs")
        require(record["first_header"]["timestamp"] <= record["last_header"]["timestamp"], "child_header_time_decreases")
        if previous_header is not None:
            require(record["first_header"]["parentHash"] == previous_header["hash"]
                    and record["first_header"]["timestamp"] >= previous_header["timestamp"], "child_adjacent_boundary_headers_differ")
        previous_header = record["last_header"]
    require(records[-1]["last_header"] == plan["raw_footer_tail_header"], "child_tail_header_differs_from_plan")
    if expected["first_block"] == FIRST:
        require(records[0]["first_header"] == first_header, "child_first_header_differs_from_sparse")
    calls, responses, requests, finishes = {}, 0, {}, {}
    rpcdir = session / "rpc"
    require(rpcdir.is_dir() and not rpcdir.is_symlink(), "child_rpc_not_regular")
    for path in sorted(rpcdir.iterdir()):
        check()
        match = re.fullmatch(r"r([0-9]{9})\.(START|FINISH)\.json", path.name)
        require(match is not None, "child_rpc_extra_or_partial_artifact")
        value = child.load_json(path)
        identifier = int(match[1])
        require(value.get("id") == identifier and type(value.get("id")) is int, "child_rpc_id_differs")
        selected = requests if match[2] == "START" else finishes
        require(identifier not in selected, "child_rpc_duplicate_id")
        selected[identifier] = {"value": value, "path": path}
    require(set(requests) == set(finishes) == set(range(1, len(requests) + 1)), "child_rpc_missing_or_gap")
    for identifier, item in requests.items():
        start_rpc, finish_rpc = item["value"], finishes[identifier]["value"]
        require(start_rpc.get("method") in {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}
                and start_rpc.get("method") == finish_rpc.get("method")
                and start_rpc.get("params") == finish_rpc.get("params"), "child_rpc_start_finish_differs")
        size = number(finish_rpc.get("response_bytes"), integer=True)
        require(size <= pilot.MAX_RESPONSE_BYTES + 1
                and finish_rpc.get("status") in {"response_shape_validated", "failed"}
                and (finish_rpc["status"] != "response_shape_validated" or
                     (size <= pilot.MAX_RESPONSE_BYTES and re.fullmatch(r"[0-9a-f]{64}", finish_rpc.get("response_sha256", "")))),
                "child_rpc_finish_invalid")
        responses += size
        calls[identifier] = start_rpc
    require(calls.get(1, {}).get("method") == "eth_chainId" and calls[1].get("params") == [], "child_initial_chain_missing")
    for identifier, name in ((2, "last_included_header"), (3, "first_excluded_header")):
        require(calls.get(identifier, {}).get("method") == "eth_getBlockByNumber"
                and calls[identifier].get("params") == [hex(plan[name]["number"]), False], "child_cutoff_request_differs")
    require(3 + 4 * len(planned) <= len(calls) <= caps["rpc_calls"]
            and responses <= caps["total_response_bytes"]
            and ended.get("rpc_calls_charged") == ended.get("session_rpc_calls") == len(calls)
            and ended.get("response_bytes_charged") == responses
            and number(ended.get("session_elapsed_seconds")) <= caps["runtime_seconds"], "child_resource_reconciliation_differs")
    actual_attempts = {path.name: path for path in (raw / "attempts").iterdir()}
    require(manifest.get("attempts") == sorted(actual_attempts), "child_attempt_ledger_differs")
    expected_success, permitted_ids = set(), {1, 2, 3}
    keys, transactions, item_bytes, total = {}, {}, 0, 0
    for record, window in zip(records, planned):
        check()
        attempt = record.get("attempt")
        require(type(attempt) is int and 1 <= attempt <= 3 and record.get("contract_sha256") == contract_sha,
                "child_attempt_contract_differs")
        directory = raw / "attempts" / f"{window['window']}.a{attempt}"
        expected_success.add(directory.name)
        require(directory.is_dir() and not directory.is_symlink()
                and child.load_json(directory / "SUCCESS.json") == record, "child_success_differs")
        require(child.load_json(directory / "START.json") == {"contract_sha256": contract_sha, "window": window,
                "attempt": attempt}, "child_attempt_start_differs")
        require({path.name for path in directory.iterdir()} == {"START.json", "REQUESTS.json", "payload.parquet", "SUCCESS.json"},
                "child_success_extra_or_partial_artifact")
        for name, filename in (("payload", "payload.parquet"), ("request_ledger", "REQUESTS.json")):
            reference = record.get(name)
            require(type(reference) is dict and reference.get("path") == str((directory / filename).relative_to(raw)),
                    "child_payload_or_ledger_path_differs")
        child.verify_success_ledger(raw, directory, record, window, (ADDRESS,), expected["topics"], requests)
        ledger = child.load_json(directory / "REQUESTS.json")["calls"]
        require(ledger[0].get("header") == record["first_header"]
                and ledger[1].get("header") == ledger[3].get("header") == record["last_header"],
                "child_header_receipt_ledger_differs")
        for call in ledger:
            require(call["id"] not in permitted_ids, "child_request_used_twice")
            permitted_ids.add(call["id"])
        local_keys, local_transactions = {}, {}
        rows, evidence = child.native_rows(directory / "payload.parquet", window, record,
                                           local_keys, local_transactions, (ADDRESS,))
        require(evidence == record["evidence"] and record["contract_intervals"] == child.interval_receipts(window, evidence, (ADDRESS,)),
                "child_native_evidence_differs")
        item_bytes = child.merge_identity_state(keys, transactions, local_keys, local_transactions, item_bytes, caps, commit=True)
        total += len(rows)
    for name, directory in actual_attempts.items():
        check()
        require(directory.is_dir() and not directory.is_symlink(), "child_attempt_not_regular")
        if name in expected_success:
            continue
        match = re.fullmatch(r"(b[0-9]{12}-[0-9]{12})\.a([123])", name)
        matching = [window for window in planned if match and window["window"] == match[1]]
        require(len(matching) == 1 and int(match[2]) < next(record["attempt"] for record in records
                if record["window"] == matching[0]), "child_unexpected_attempt")
        require({path.name for path in directory.iterdir()} == {"START.json", "REQUESTS.json", "FAILURE.json"},
                "child_failed_attempt_partial_or_outputs")
        require(child.load_json(directory / "START.json") == {"contract_sha256": contract_sha,
                "window": matching[0], "attempt": int(match[2])}, "child_failed_attempt_start_differs")
        require(child.load_json(directory / "FAILURE.json") == {"status": "failed_attempt", "error_code": "transport_payload_or_checkpoint_failure"},
                "child_failed_attempt_not_transport")
        failed_calls = child.load_json(directory / "REQUESTS.json").get("calls")
        require(type(failed_calls) is list and failed_calls and failed_calls[-1].get("status") == "failed",
                "child_failed_attempt_ledger_invalid")
        window = matching[0]
        methods = ["eth_getBlockByNumber", "eth_getBlockByNumber", "eth_getLogs", "eth_getBlockByNumber"]
        params = [[hex(window["first_block"]), False], [hex(window["last_block"]), False],
                  [{"fromBlock": hex(window["first_block"]), "toBlock": hex(window["last_block"]),
                    "address": [ADDRESS], "topics": [expected["topics"]]}], [hex(window["last_block"]), False]]
        require(1 <= len(failed_calls) <= 4 and [item.get("method") for item in failed_calls] == methods[:len(failed_calls)]
                and [item.get("params") for item in failed_calls] == params[:len(failed_calls)]
                and all(item.get("status") == "validated" for item in failed_calls[:-1]), "child_failed_attempt_predicate_differs")
        for call in failed_calls:
            identifier = call.get("id")
            require(identifier not in permitted_ids and identifier in requests
                    and call.get("method") == calls[identifier].get("method")
                    and call.get("params") == calls[identifier].get("params")
                    and all(call.get(key) == finishes[identifier]["value"].get(key)
                            for key in ("response_bytes", "response_sha256", "response_size_is_conservative_bound")),
                    "child_failed_request_differs")
            permitted_ids.add(identifier)
        last_finish = finishes[failed_calls[-1]["id"]]["value"]
        require(last_finish.get("status") == "failed" and last_finish.get("response_size_is_conservative_bound") is True
                and last_finish.get("response_bytes") == pilot.MAX_RESPONSE_BYTES + 1, "child_failed_transport_charge_differs")
    require(permitted_ids == set(calls), "child_unattributed_rpc_calls")
    require(total == len(keys) == manifest.get("rows") == manifest.get("distinct_native_keys") == ended.get("rows")
            == ended.get("identity_key_count") and ended.get("transaction_anchor_count") == len(transactions)
            and ended.get("conservatively_counted_identity_item_bytes") == item_bytes
            and ended.get("validated_windows") == len(planned) and total <= caps["rows"], "child_rows_or_identity_differs")
    return {"rows": total, "windows": len(planned), "manifest_sha256": digest(manifest_raw),
            "owner_sha256": digest((raw / "OWNER.json").read_bytes()), "run_id": owner["run_id"],
            "usage": {"rpc_calls": len(calls), "response_bytes": responses,
                      "runtime_seconds": ended["session_elapsed_seconds"]}}


class ParentControls(shared.Controls):
    def check(self, additional: int, *, final: bool):
        require(self.root.is_dir() and not self.root.is_symlink()
                and (self.root.stat().st_dev, self.root.stat().st_ino) == self.identity, "parent_controls_changed")
        allowed = {"OWNER.json", "REQUESTS.jsonl", "PROCESS.json", "CHILD_EXIT.json", "RESULT.json", "COMPLETE.json"}
        if self.pending_marker:
            allowed.add(".COMPLETE.pending")
        entries = list(self.root.iterdir())
        require(all(path.name in allowed and path.is_file() and not path.is_symlink() for path in entries), "parent_control_artifacts_invalid")
        require(sum(path.stat().st_size for path in entries) == self.bytes, "parent_control_bytes_changed")
        require(self.bytes + additional <= CONTROL_CAP - (0 if final else 128_000), "parent_control_byte_cap")
        self.free(additional)


def process_identity(pid: int):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {"pid": pid, "start_ticks": int(fields[19]), "state": fields[0],
                "pgid": int(fields[2]), "session": int(fields[3])}
    except FileNotFoundError:
        return None


class OwnedChild:
    """Only the new session created here can be signalled or drained."""
    def __init__(self, command):
        # Never lose a spawned handle by raising before _run assigns `active`.
        # An unproven birth is NOT a proven absence of a process group.
        self.process = self.pid = self.boot = self.birth = None
        self.startup_error, self.ownership_proven, self.drained = None, False, False
        self.command_sha256 = digest(encode(command))
        self.stdout, self.stderr_hash, self.stderr_bytes = bytearray(), hashlib.sha256(), 0
        self.overflow = False
        self.streams = {}
        try:
            self.process = subprocess.Popen(command, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                            start_new_session=True, bufsize=0)
            self.pid = self.process.pid
            require(type(self.pid) is int and self.pid > 0, "child_pid_invalid")
            self.streams = {"stdout": self.process.stdout, "stderr": self.process.stderr}
            self.boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            self.birth = process_identity(self.pid)
            require(UUID.fullmatch(self.boot) and self.birth is not None
                    and self.birth["pid"] == self.pid and type(self.birth["start_ticks"]) is int
                    and self.birth["start_ticks"] > 0
                    and self.birth["pgid"] == self.pid == self.birth["session"], "child_birth_not_bound")
            self.ownership_proven = True
            for stream in self.streams.values():
                os.set_blocking(stream.fileno(), False)
        except BaseException as exc:
            self.startup_error = str(exc) if isinstance(exc, shared.HistoryBlocked) else "child_startup_setup_failed"
            # A partly configured pipe may still be blocking. Closing our own
            # descriptors is safe; signalling an unbound PID/PGID is not.
            for stream in self.streams.values():
                try:
                    if stream is not None:
                        stream.close()
                except Exception:
                    pass
            self.streams = {}

    def proof(self):
        return {"pid": self.pid, "boot_id": self.boot, "birth": self.birth,
                "command_sha256": self.command_sha256, "ownership_proven": self.ownership_proven,
                "startup_error": self.startup_error,
                "liveness": "proven_drained" if self.drained else "not_yet_drained" if self.ownership_proven else "unknown"}

    def pump(self):
        for name, stream in list(self.streams.items()):
            for _ in range(16):
                try:
                    data = os.read(stream.fileno(), 8192)
                except BlockingIOError:
                    break
                if not data:
                    stream.close()
                    del self.streams[name]
                    break
                if name == "stdout":
                    self.stdout.extend(data)
                    amount = len(self.stdout)
                else:
                    self.stderr_hash.update(data)
                    self.stderr_bytes += len(data)
                    amount = self.stderr_bytes
                if amount > 64 * 1024:
                    self.overflow = True
                    stream.close()
                    del self.streams[name]
                    break

    def members(self):
        require(self.process is not None and self.ownership_proven and self.birth is not None,
                "owned_child_liveness_unknown")
        require(Path("/proc/sys/kernel/random/boot_id").read_text().strip() == self.boot, "child_boot_changed")
        leader = process_identity(self.pid)
        require(leader is None or leader["start_ticks"] == self.birth["start_ticks"], "child_pid_reused")
        members = []
        entries = list(Path("/proc").iterdir())
        require(len(entries) <= 65_536, "process_inventory_cap")
        for path in entries:
            if path.name.isdigit():
                value = process_identity(int(path.name))
                if value and value["pgid"] == self.pid and value["state"] != "Z":
                    require(value["session"] == self.pid, "child_group_session_differs")
                    members.append(value)
        return members

    def drain(self):
        if self.members():
            os.killpg(self.pid, signal.SIGTERM)
        until = time.monotonic() + 15
        while self.process.poll() is None or self.members():
            self.pump()
            if time.monotonic() >= until:
                if self.members():
                    os.killpg(self.pid, signal.SIGKILL)
                self.process.wait(timeout=5)
                break
            time.sleep(0.05)
        self.process.wait(timeout=5)
        self.pump()
        require(not self.members() and not self.streams, "owned_child_not_drained")
        self.drained = True

    def poll(self):
        require(self.process is not None, "owned_child_liveness_unknown")
        return self.process.poll()


@contextmanager
def runtime_limits(seconds):
    previous = resource.getrlimit(resource.RLIMIT_AS)
    handler, timer = signal.getsignal(signal.SIGALRM), signal.getitimer(signal.ITIMER_REAL)
    require(timer == (0.0, 0.0), "preexisting_alarm")
    require(previous[1] == resource.RLIM_INFINITY or previous[1] >= RAM, "ram_hard_limit_below_profile")
    try:
        resource.setrlimit(resource.RLIMIT_AS, (PARENT_LIVE_RAM, previous[1]))
        def expired(_signum, _frame):
            raise shared.HistoryBlocked("parent_runtime_deadline")
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)
        resource.setrlimit(resource.RLIMIT_AS, previous)


def command_for(plan_path, expected_head, raw, profile, private):
    command = [sys.executable, str(REPO / "scripts/collect_polymarket_native_increment.py"),
        "--source-plan", str(plan_path), "--source-plan-sha256", PLAN_SHA, "--plan-source-head", PLAN_HEAD,
        "--expected-head", expected_head, "--output-dir", str(raw), "--mode", "v3-historical",
        "--first-block", str(profile["first_block"]), "--last-block", str(profile["last_block"]),
        "--window-blocks", "100", "--rpc-private-file", str(private)]
    for name, value in (("max-rpc-calls", profile["rpc_calls"]), ("max-output-bytes", profile["child_output"]),
                        ("max-runtime-seconds", profile["child_seconds"]), ("max-rows", profile["rows"]),
                        ("max-ram-bytes", RAM - PARENT_LIVE_RAM), ("max-total-response-bytes", profile["response_bytes"])):
        command += ["--" + name, str(value)]
    return command


def accepted_probe(path, marker_sha, plan, binding, source, first_header, check):
    marker, marker_binding = read_bound(path / "controls/COMPLETE.json", marker_sha, CONTROL_CAP)
    result, result_binding = read_bound(path / "controls/RESULT.json", marker.get("result_sha256"), CONTROL_CAP)
    require(marker.get("status") == result.get("status") == SUCCESS and result.get("profile") == "probe"
            and result.get("source") == source and result.get("source_plan_binding") == binding
            and result.get("analysis_admitted") is False and result.get("children_still_running") is False,
            "accepted_positive_probe_missing")
    for name, sha in result.get("control_bindings", {}).get("hashes", {}).items():
        require(name in {"OWNER.json", "PROCESS.json", "CHILD_EXIT.json", "REQUESTS.jsonl"}, "probe_control_reference_invalid")
        value, _ = read_bound(path / "controls" / name, sha, CONTROL_CAP, json_lines=name == "REQUESTS.jsonl")
        if name == "REQUESTS.jsonl":
            require(len(value) == result["control_bindings"].get("journal_records"), "probe_journal_count_differs")
    require(set(result.get("control_bindings", {}).get("hashes", {})) ==
            {"OWNER.json", "PROCESS.json", "CHILD_EXIT.json", "REQUESTS.jsonl"}, "probe_control_bindings_missing")
    owner = child.load_json(path / "controls/OWNER.json")
    exit_receipt = child.load_json(path / "controls/CHILD_EXIT.json")
    proof = result.get("accepted_child")
    require(owner.get("profile") == "probe" and owner.get("run_id") == result.get("run_id") == marker.get("run_id")
            and owner.get("expected_child_contract") == expected_contract(plan, binding, source, PROFILES["probe"])
            and exit_receipt.get("exit_code") == 0 and exit_receipt.get("owned_group_drained") is True
            and type(proof) is dict and type(proof.get("rows")) is int and proof["rows"] > 0,
            "accepted_probe_contract_or_exit_differs")
    raw = path / "raw"
    require(tree_digest(raw, PROFILES["probe"]["child_output"], check) == result.get("child_tree"),
            "accepted_probe_evidence_changed")
    return {"marker": marker_binding, "result": result_binding, "rows": proof["rows"],
            "prior_global_usage": result.get("prior_global_usage"), "usage_charged": result.get("usage_charged"),
            "diagnostic_overlap_not_authoritative_source": True}


def run(*, profile_name: str, plan_path: Path, plan_sha: str, plan_head: str, expected_head: str,
        sparse_path: Path, sparse_sha: str, admission_path: Path, admission_sha: str, output: Path,
        accepted_probe_dir: Path | None = None, accepted_probe_sha: str | None = None,
        rpc_private_file: Path | None = None):
    require_production_host()  # Before all input reads, output creation and child execution.
    require(profile_name in PROFILES, "profile_invalid")
    profile = PROFILES[profile_name]
    with runtime_limits(profile["runtime_seconds"]):
        return _run(profile_name=profile_name, plan_path=plan_path, plan_sha=plan_sha, plan_head=plan_head,
            expected_head=expected_head, sparse_path=sparse_path, sparse_sha=sparse_sha,
            admission_path=admission_path, admission_sha=admission_sha, output=output,
            accepted_probe_dir=accepted_probe_dir, accepted_probe_sha=accepted_probe_sha,
            rpc_private_file=rpc_private_file)


def _run(*, profile_name, plan_path, plan_sha, plan_head, expected_head, sparse_path, sparse_sha,
         admission_path, admission_sha, output, accepted_probe_dir=None, accepted_probe_sha=None, rpc_private_file=None):
    profile, began = PROFILES[profile_name], time.monotonic()
    def deadline():
        require(time.monotonic() - began < profile["runtime_seconds"], "parent_runtime_deadline")
    require(plan_sha == PLAN_SHA and plan_head == PLAN_HEAD and sparse_sha == SPARSE_SHA, "frozen_input_binding_differs")
    require(output.is_absolute() and shared.RUN_ROOT.resolve() in output.resolve().parents
            and not output.exists() and not output.is_symlink() and output.parent.is_dir(), "output_not_fresh_run")
    plan, binding, original = pilot.read_plan(plan_path, plan_sha, plan_head)
    shared.prior.validate_plan_blocks(plan)
    sparse, sparse_binding = read_bound(sparse_path, sparse_sha, 1024**2)
    first_header = validate_sparse(sparse, binding, plan)
    admission, admission_binding = read_bound(admission_path, admission_sha, 1024**2)
    accounting = global_admission(admission, profile)
    source = source_snapshot(expected_head)
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    child.check_destination(output, original, plan_path, plan, private)
    inputs = [sparse_binding, admission_binding, *accounting["bound_inputs"]]
    for item in inputs:
        path = Path(item["path"]).resolve()
        require(path != output.resolve() and path not in output.resolve().parents
                and output.resolve() not in path.parents, "output_overlaps_evidence")
    probe_binding = None
    if profile_name == "full":
        require(accepted_probe_dir is not None and accepted_probe_sha is not None, "positive_probe_required")
        require(output.resolve() != accepted_probe_dir.resolve() and output.resolve() not in accepted_probe_dir.resolve().parents
                and accepted_probe_dir.resolve() not in output.resolve().parents, "output_overlaps_probe")
        probe_binding = accepted_probe(accepted_probe_dir, accepted_probe_sha, plan, binding, source, first_header, deadline)
        require(all(accounting["prior"][key] >= number(probe_binding["prior_global_usage"].get(key))
                    + number(probe_binding["usage_charged"].get(key)) for key in GLOBAL_CAPS),
                "global_admission_omits_accepted_probe_charges")
    else:
        require(accepted_probe_dir is None and accepted_probe_sha is None, "probe_must_not_adopt_prior_child")
    resources = inventory.resource_state(output.parent)
    require(resources.get("ram_bytes", {}).get("MemAvailable", 0) >= RAM
            and resources.get("disk_bytes", {}).get("free", 0) >= FREE_FLOOR + profile["retained_bytes"], "profile_resources_not_available")
    expected = expected_contract(plan, binding, source, profile)
    owner = {"schema_version": "polymarket_v3_historical_acceptance_owner_v1", "run_id": str(uuid.uuid4()),
             "created_utc": shared.now(), "profile": profile_name, "limits": profile, "source": source,
             "source_plan_binding": binding, "sparse_result": sparse_binding, "global_admission": admission_binding,
             "expected_child_contract": expected, "accepted_probe": probe_binding,
             "ram_bytes": RAM, "parent_live_ram_reservation_bytes": PARENT_LIVE_RAM,
             "free_floor_bytes": FREE_FLOOR, "spill_bytes": 0, "parent_control_cap_bytes": CONTROL_CAP,
             "resume_permitted": False, "economic_decoding_admitted": False, "analysis_admitted": False}
    output.mkdir(mode=0o700)
    controls = ParentControls(output / "controls", owner)
    active = None
    result = {"schema_version": "polymarket_v3_historical_acceptance_result_v1", "status": "blocked",
              "profile": profile_name, "run_id": owner["run_id"], "source": source,
              "source_plan_binding": binding, "analysis_admitted": False, "economic_decoding_admitted": False,
              "interior_exact_timestamps_collected": False, "children_still_running": True,
              "prior_global_usage": accounting["prior"], "profile_reservation": profile,
              "accepted_probe": probe_binding, "endpoint_disclosed": False}
    try:
        deadline()
        command = command_for(plan_path, expected_head, output / "raw", profile, private)
        active = OwnedChild(command)
        active.command_sha256 = digest(encode(command))
        controls.write("PROCESS.json", active.proof(), final=False)
        controls.journal({"event": "one_fresh_child_launched", "process": active.proof()})
        require(getattr(active, "startup_error", None) is None, "child_startup_failed")
        child_started = time.monotonic()
        while active.poll() is None:
            active.pump()
            deadline()
            require(not active.overflow, "child_stream_cap")
            require(time.monotonic() - child_started < profile["child_seconds"], "child_runtime_deadline")
            time.sleep(0.1)
        active.drain()
        controls.write("CHILD_EXIT.json", {"exit_code": active.poll(), "process": active.proof(),
            "stdout_bytes": len(active.stdout), "stdout_sha256": digest(bytes(active.stdout)),
            "stderr_bytes": active.stderr_bytes, "stderr_sha256": active.stderr_hash.hexdigest(),
            "owned_group_drained": True}, final=False)
        require(active.poll() == 0 and not active.overflow, "child_exit_or_stream_not_admitted")
        resource.setrlimit(resource.RLIMIT_AS, (RAM, resource.getrlimit(resource.RLIMIT_AS)[1]))
        stdout = json.loads(bytes(active.stdout), object_pairs_hook=inventory.unique_object,
                            parse_constant=lambda _: (_ for _ in ()).throw(shared.HistoryBlocked("child_stdout_nonfinite")))
        before = tree_digest(output / "raw", profile["child_output"], deadline)
        proof = validate_child(output / "raw", expected, plan, first_header, exit_code=active.poll(), stdout_result=stdout, check=deadline)
        require(profile_name != "probe" or proof["rows"] > 0, "probe_has_no_positive_native_rows")
        require({path.name for path in output.iterdir()} == {"controls", "raw"}, "parent_run_entries_changed")
        require((output / "raw/OWNER.json").read_bytes() and proof["owner_sha256"] ==
                digest((output / "raw/OWNER.json").read_bytes()), "child_owner_changed")
        require(pilot.read_plan(plan_path, plan_sha, plan_head) == (plan, binding, original)
                and source_snapshot(expected_head) == source, "final_source_or_plan_changed")
        for item in inputs:
            _, rebound = read_bound(Path(item["path"]), item["sha256"])
            require(rebound == item, "final_evidence_identity_changed")
        if profile_name == "full":
            require(accepted_probe(accepted_probe_dir, accepted_probe_sha, plan, binding, source, first_header, deadline)
                    == probe_binding, "accepted_probe_changed")
        require(tree_digest(output / "raw", profile["child_output"], deadline) == before, "child_artifact_tree_changed")
        deadline()
        elapsed = time.monotonic() - began
        controls.journal({"event": "child_independently_accepted", "child_tree": before, "rows": proof["rows"]})
        own = controls.verify()
        require(before["bytes"] + own["bytes"] + 128_000 <= profile["retained_bytes"], "inclusive_output_cap")
        usage = {"retained_bytes": before["bytes"] + own["bytes"] + 128_000,
                 "rpc_calls": proof["usage"]["rpc_calls"], "response_bytes": proof["usage"]["response_bytes"],
                 "runtime_seconds": profile["runtime_seconds"]}
        require(all(accounting["prior"][key] + usage[key] <= GLOBAL_CAPS[key] for key in GLOBAL_CAPS), "final_global_budget_exceeded")
        result.update(status=SUCCESS, accepted_child=proof, child_tree=before, children_still_running=False,
                      control_bindings=own, usage_charged=usage,
                      usage_output_reserves_final_128000_bytes=True,
                      usage_runtime_charged_at_full_profile_reservation=True,
                      diagnostic_overlap_not_authoritative_source=profile_name == "probe")
    except BaseException as exc:
        result["error_code"] = str(exc) if isinstance(exc, shared.HistoryBlocked) else "child_acceptance_or_interruption_failed"
        if active is not None:
            active.drain()  # If this cannot prove drainage, do not publish a terminal receipt.
            if not (controls.root / "CHILD_EXIT.json").exists():
                controls.write("CHILD_EXIT.json", {"exit_code": active.poll(), "process": active.proof(),
                    "stdout_bytes": len(active.stdout), "stdout_sha256": digest(bytes(active.stdout)),
                    "stderr_bytes": active.stderr_bytes, "stderr_sha256": active.stderr_hash.hexdigest(),
                    "owned_group_drained": True, "accepted": False}, final=False)
        result["children_still_running"] = False
        result["usage_charged"] = {key: profile[key] for key in GLOBAL_CAPS}
        result["failed_stage_charged_at_full_reservation"] = True
    result["elapsed_seconds"] = max(0.0, time.monotonic() - began)
    controls.verify()
    controls.write("RESULT.json", result, final=True)
    if result["status"] == SUCCESS:
        deadline()
        controls.verify()
        controls.write("COMPLETE.json", {"status": SUCCESS, "result_sha256": digest(encode(result)),
            "run_id": owner["run_id"], "analysis_admitted": False,
            "child_tree_sha256": result["child_tree"]["sha256"]}, final=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILES), required=True)
    for name in ("source-plan", "sparse-result", "global-admission", "output-dir", "accepted-probe-dir", "rpc-private-file"):
        parser.add_argument("--" + name, type=Path, required=name in {"source-plan", "sparse-result", "global-admission", "output-dir"})
    for name in ("source-plan-sha256", "plan-source-head", "expected-head", "sparse-result-sha256", "global-admission-sha256", "accepted-probe-complete-sha256"):
        parser.add_argument("--" + name, required=name != "accepted-probe-complete-sha256")
    args = parser.parse_args(argv)
    try:
        for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[key] = "1"
        result = run(profile_name=args.profile, plan_path=args.source_plan, plan_sha=args.source_plan_sha256,
            plan_head=args.plan_source_head, expected_head=args.expected_head, sparse_path=args.sparse_result,
            sparse_sha=args.sparse_result_sha256, admission_path=args.global_admission,
            admission_sha=args.global_admission_sha256, output=args.output_dir,
            accepted_probe_dir=args.accepted_probe_dir, accepted_probe_sha=args.accepted_probe_complete_sha256,
            rpc_private_file=args.rpc_private_file)
    except BaseException:
        parser.error("V3 historical stage not admitted; preserve all receipts and inspect the bound run.")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == SUCCESS else 2


if __name__ == "__main__":
    raise SystemExit(main())
