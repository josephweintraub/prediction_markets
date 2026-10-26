"""Bounded serial-chunk/parallel-process controller for reviewed RAW collection.

Only append ranges are supported. This controller does not decode trades or admit
an analysis sample. A failed child stops new launches; already-running children
are drained with their own deadlines. Resume never overwrites a returned child.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Callable
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import collect_polymarket_native_increment as collector

inventory, pilot = collector.inventory, collector.pilot
require = collector.require
COLLECTOR = REPO / "scripts/collect_polymarket_native_increment.py"
COLLECTOR_SHA256 = "0f1f0c1dd0f8f6a7f7dd3daee832ebf93f03210df7fe8f7b8ad7d736bba2d14f"
CHUNK_BLOCKS = 10_000
WINDOW_BLOCKS = 50
CHILD_BYTES = 2_000_000_000
CHILD_CONTROL = 4 * 1024**2
CHILD_CAPS = {"max_rpc_calls": 1200, "max_output_bytes": CHILD_BYTES - CHILD_CONTROL,
              "max_runtime_seconds": 3600, "max_rows": 3_000_000,
              "max_ram_bytes": 8 * 1024**3, "max_total_response_bytes": 4 * 1024**3}
GLOBAL_BYTES = 128_000_000_000
PARENT_RESERVE = 64 * 1024**2
MAX_FILES = 10_000
MAX_LOG_BYTES = 64 * 1024
MAX_ATTEMPTS = 3
MAX_RUNTIME = 7 * 24 * 3600
json_bytes, publish, load_json, digest = collector.json_bytes, collector.publish, collector.load_json, collector.digest


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def elapsed_since(value: str) -> float:
    parsed = datetime.fromisoformat(value)
    require(parsed.tzinfo is not None, "receipt_timezone_missing")
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def file_digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for raw in iter(lambda: stream.read(1024**2), b""):
            value.update(raw)
    return value.hexdigest()


def tree_inventory(root: Path, *, cap: int) -> dict:
    require(root.is_dir() and not root.is_symlink(), "inventory_directory_missing_or_symlink")
    records, total = [], 0
    for directory, names, filenames in os.walk(root, followlinks=False):
        require(all(not (Path(directory) / name).is_symlink() for name in names), "inventory_symlink_directory")
        for name in sorted(filenames):
            path = Path(directory) / name
            before = inventory.path_identity(path)
            require(before["status"] == "present" and not before["is_symlink"] and path.is_file(), "inventory_nonregular_file")
            total += before["target"]["bytes"]
            require(total <= cap and len(records) < MAX_FILES, "inventory_size_or_file_limit")
            sha = file_digest(path)
            require(before == inventory.path_identity(path), "inventory_file_changed")
            records.append({"path": str(path.relative_to(root)), "bytes": before["target"]["bytes"], "sha256": sha})
    return {"files": sorted(records, key=lambda item: item["path"]), "bytes": total}


def tree_bytes(root: Path) -> int:
    total = 0
    for directory, names, filenames in os.walk(root, followlinks=False):
        require(all(not (Path(directory) / name).is_symlink() for name in names), "output_symlink_directory")
        for name in filenames:
            path = Path(directory) / name
            require(not path.is_symlink() and path.is_file(), "output_nonregular_file")
            total += path.stat().st_size
    return total


def child_usage(raw: Path) -> dict:
    """Read only durable request/session receipts, including failed/pending calls."""
    calls, responses, elapsed = {}, 0, 0.0
    sessions = raw / "sessions"
    if not sessions.exists():
        return {"rpc_calls": 0, "response_bytes": 0, "runtime_seconds": 0.0}
    require(sessions.is_dir() and not sessions.is_symlink(), "child_sessions_invalid")
    for session in sessions.iterdir():
        require(session.is_dir() and not session.is_symlink(), "child_session_not_directory")
        start = load_json(session / "START.json")
        if (session / "RESULT.json").exists():
            duration = load_json(session / "RESULT.json").get("session_elapsed_seconds")
            require(type(duration) in (int, float) and duration >= 0, "child_runtime_invalid")
            elapsed += duration
        else:
            elapsed += elapsed_since(start["started_utc"])
        request_dir = session / "rpc"
        require(request_dir.is_dir() and not request_dir.is_symlink(), "child_rpc_directory_invalid")
        for path in request_dir.glob("*.START.json"):
            value = load_json(path)
            identifier = value.get("id")
            require(type(identifier) is int and identifier > 0 and identifier not in calls
                    and path.name == f"r{identifier:09d}.START.json" and value.get("method") in
                    {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}, "child_rpc_identity_invalid")
            calls[identifier] = value
            finish = path.with_name(path.name.replace(".START.json", ".FINISH.json"))
            if finish.exists():
                ended = load_json(finish)
                size = ended.get("response_bytes")
                require(ended.get("id") == identifier and type(size) is int and 0 <= size <= pilot.MAX_RESPONSE_BYTES + 1,
                        "child_rpc_response_receipt_invalid")
                responses += size
            else:
                responses += pilot.MAX_RESPONSE_BYTES + 1
    require(set(calls) == set(range(1, len(calls) + 1)), "child_rpc_identity_gap")
    return {"rpc_calls": len(calls), "response_bytes": responses, "runtime_seconds": elapsed}


def chunks(first: int, last: int) -> list[dict]:
    require(type(first) is int and type(last) is int and 0 < first <= last, "invalid_campaign_range")
    require((last - first) // CHUNK_BLOCKS + 1 <= 2000, "campaign_chunk_limit")
    return [{"chunk": f"c{index:06d}", "first_block": start, "last_block": min(last, start + CHUNK_BLOCKS - 1)}
            for index, start in enumerate(range(first, last + 1, CHUNK_BLOCKS), 1)]


def expected_child_contract(plan: dict, binding: dict, source: dict, chunk: dict) -> dict:
    return {"schema_version": "polymarket_native_collection_contract_v1", "source_plan": binding,
            "source": source, "mode": "append", "first_block": chunk["first_block"], "last_block": chunk["last_block"],
            "window_blocks": WINDOW_BLOCKS, "addresses": list(pilot.ADDRESSES),
            "topics": sorted(set(pilot.EXPECTED_TOPICS.values())),
            "limits": collector.limits(CHILD_CAPS["max_rpc_calls"], CHILD_CAPS["max_output_bytes"],
                                       CHILD_CAPS["max_runtime_seconds"], CHILD_CAPS["max_rows"],
                                       CHILD_CAPS["max_ram_bytes"], CHILD_CAPS["max_total_response_bytes"]),
            "end_exclusive_utc": plan["end_exclusive_utc"], "raw_payload_only": True,
            "interior_exact_timestamps_collected": False, "analysis_admitted": False,
            "provider_block_timestamp_used_as_clock": False}


def validate_child(raw: Path, expected: dict) -> dict:
    """Reopen the reviewed child's completion, literals, receipts and all hashes."""
    owner = load_json(raw / "OWNER.json")
    contract_sha = digest(json_bytes(expected))
    require(owner.get("contract") == expected and owner.get("contract_sha256") == contract_sha, "child_contract_differs")
    marker = load_json(raw / "COMPLETE.json")
    manifest_path = raw / "manifest.json"
    require(marker.get("status") == "raw_collection_complete_not_analysis_admission"
            and marker.get("run_id") == owner["run_id"] and marker.get("contract_sha256") == contract_sha
            and marker.get("manifest_sha256") == file_digest(manifest_path), "child_completion_binding_differs")
    manifest = load_json(manifest_path)
    require(manifest.get("schema_version") == "polymarket_native_collection_manifest_v1"
            and manifest.get("status") == "raw_collection_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True
            and manifest.get("contract") == expected and manifest.get("contract_sha256") == contract_sha
            and manifest.get("run_id") == owner["run_id"], "child_manifest_contract_differs")
    planned = collector.windows(expected["first_block"], expected["last_block"], WINDOW_BLOCKS)
    records = manifest.get("windows")
    require(type(records) is list, "child_windows_missing")
    collector.verify_coverage(records, planned, pilot.ADDRESSES)
    rpc_starts = {}
    for path in (raw / "sessions").glob("*/rpc/*.START.json"):
        value = load_json(path)
        require(value["id"] not in rpc_starts, "child_rpc_identity_conflict")
        rpc_starts[value["id"]] = {"value": value, "path": path}
    total = 0
    for record in records:
        require(record.get("contract_sha256") == contract_sha, "child_shard_contract_differs")
        total += record["evidence"]["rows"]
        for kind in ("payload", "request_ledger"):
            reference = record[kind]
            relative = Path(reference["path"])
            require(not relative.is_absolute() and ".." not in relative.parts, "child_reference_escapes_run")
            path = raw / relative
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == reference["bytes"]
                    and file_digest(path) == reference["sha256"], "child_shard_hash_differs")
        receipt = (raw / record["payload"]["path"]).parent / "SUCCESS.json"
        require(load_json(receipt) == record, "child_success_receipt_differs")
        collector.verify_success_ledger(raw, receipt.parent, record, record["window"], pilot.ADDRESSES,
                                        expected["topics"], rpc_starts)
    require(type(total) is int and total == manifest.get("rows") == manifest.get("distinct_native_keys")
            and total <= CHILD_CAPS["max_rows"], "child_row_reconciliation")
    usage = child_usage(raw)
    require(3 + 4 * len(planned) <= usage["rpc_calls"] <= CHILD_CAPS["max_rpc_calls"]
            and usage["response_bytes"] <= CHILD_CAPS["max_total_response_bytes"]
            and usage["runtime_seconds"] <= CHILD_CAPS["max_runtime_seconds"] + 30,
            "child_resource_caps_exceeded")
    return {"rows": total, "manifest_sha256": file_digest(manifest_path), "usage": usage,
            "first_block": expected["first_block"], "last_block": expected["last_block"]}


def finish_child(running: dict, plan_sha: str, *, force_failure: str | None = None) -> dict:
    child, raw, directory = running["child"], running["raw"], running["directory"]
    require(child.poll() is not None, "cannot_checkpoint_running_child")
    child.pump()
    usage = child_usage(raw) if raw.exists() else {"rpc_calls": 0, "response_bytes": 0, "runtime_seconds": 0.0}
    status, checked, error = "failed_raw_child", None, force_failure
    if error is None:
        try:
            require(child.poll() == 0 and running["terminating"] is None and not child.overflow,
                    "child_nonzero_timeout_or_log_limit")
            checked = validate_child(raw, running["expected"])
            status = "accepted_raw_child"
        except Exception as exc:
            error = str(exc) if isinstance(exc, pilot.PilotBlocked) else "child_acceptance_failed"
    files = tree_inventory(raw, cap=CHILD_CAPS["max_output_bytes"]) if raw.exists() else {"files": [], "bytes": 0}
    publish(directory / "INVENTORY.json", json_bytes(files))
    ended = {"plan_sha256": plan_sha, "status": status, "chunk": running["chunk"],
             "exit_code": child.poll(), "error_code": error, "accepted": checked, "usage": usage,
             "raw_inventory_sha256": digest(json_bytes(files)), "streams": child.receipt()}
    publish(directory / "EXIT.json", json_bytes(ended))
    require(tree_bytes(directory) <= CHILD_BYTES, "child_total_reserved_bytes_exceeded")
    return ended


def process_identity(pid: int) -> dict | None:
    """Linux process birth proof; no signals are sent to a recovered PID."""
    path = Path(f"/proc/{pid}/stat")
    try:
        fields = path.read_text().rsplit(")", 1)[1].split()
        return {"pid": pid, "start_ticks": int(fields[19]), "state": fields[0]}
    except FileNotFoundError:
        return None
    except Exception:
        raise pilot.PilotBlocked("prior_process_identity_unreadable") from None


class SubprocessChild:
    def __init__(self, argv: list[str]):
        self.process = subprocess.Popen(argv, cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        start_new_session=True, bufsize=0)
        self.pid = self.process.pid
        self.streams = {}
        for name, stream in (("stdout", self.process.stdout), ("stderr", self.process.stderr)):
            try:
                os.set_blocking(stream.fileno(), False)
            except Exception:
                self.process.terminate()
                try:
                    self.process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
                raise
            self.streams[name] = {"stream": stream, "bytes": 0, "hash": hashlib.sha256(), "closed": False}
        self.overflow = False

    def pump(self) -> None:
        for value in self.streams.values():
            if value["closed"]:
                continue
            while True:
                try:
                    raw = os.read(value["stream"].fileno(), 8192)
                except BlockingIOError:
                    break
                if not raw:
                    value["closed"] = True
                    value["stream"].close()
                    break
                value["bytes"] += len(raw)
                value["hash"].update(raw)
                if value["bytes"] > MAX_LOG_BYTES:
                    self.overflow = True
                    break

    def poll(self):
        return self.process.poll()

    def terminate(self):
        self.process.terminate()

    def kill(self):
        self.process.kill()

    def receipt(self):
        return {name: {"bytes": value["bytes"], "sha256": value["hash"].hexdigest()}
                for name, value in self.streams.items()}


def reservations(*, retained: int, running: int, calls_used: int, responses_used: int,
                 launch: bool, caps: dict, disk_free: int) -> None:
    count = running + int(launch)
    outstanding = count * CHILD_BYTES
    require(retained + outstanding + PARENT_RESERVE <= caps["output_bytes"], "campaign_output_reservation_limit")
    require(disk_free >= collector.FREE_FLOOR + outstanding + PARENT_RESERVE, "campaign_free_space_reservation_limit")
    require(calls_used + count * CHILD_CAPS["max_rpc_calls"] <= caps["rpc_calls"], "campaign_call_reservation_limit")
    require(responses_used + count * CHILD_CAPS["max_total_response_bytes"] <= caps["response_bytes"],
            "campaign_response_reservation_limit")


def verify_parent_coverage(accepted: list[dict], planned: list[dict]) -> int:
    require(len(accepted) == len(planned), "campaign_incomplete_coverage")
    total = 0
    for record, chunk in zip(accepted, planned):
        require(record["chunk"] == chunk and record["status"] == "accepted_raw_child"
                and record["accepted"]["first_block"] == chunk["first_block"]
                and record["accepted"]["last_block"] == chunk["last_block"], "campaign_range_gap_or_overlap")
        total += record["accepted"]["rows"]
    require(all(left["last_block"] + 1 == right["first_block"] for left, right in zip(planned, planned[1:])),
            "campaign_range_gap_or_overlap")
    return total


def child_command(plan_path: Path, plan_sha: str, plan_head: str, head: str, raw: Path,
                  chunk: dict, private: Path) -> list[str]:
    command = [sys.executable, str(COLLECTOR), "--source-plan", str(plan_path), "--source-plan-sha256", plan_sha,
               "--plan-source-head", plan_head, "--expected-head", head, "--output-dir", str(raw),
               "--mode", "append", "--first-block", str(chunk["first_block"]), "--last-block", str(chunk["last_block"]),
               "--window-blocks", str(WINDOW_BLOCKS), "--rpc-private-file", str(private)]
    for key, value in CHILD_CAPS.items():
        command += ["--" + key.replace("_", "-"), str(value)]
    return command


def run_campaign(plan_path: Path, plan_sha: str, plan_head: str, expected_head: str, output: Path, *,
                 first_block: int, last_block: int, workers: int, prior_runs: list[Path],
                 max_total_rpc_calls: int, max_total_response_bytes: int, max_runtime_seconds: int,
                 max_output_bytes: int, resume: bool = False, rpc_private_file: Path | None = None,
                 diagnostic_runs: list[Path] | None = None, diagnostic_rpc_call_cap: int | None = None,
                 diagnostic_response_byte_cap: int | None = None, diagnostic_runtime_second_cap: int | None = None,
                 launcher: Callable = SubprocessChild, clock: Callable = time.monotonic,
                 sleep: Callable = time.sleep, identity: Callable = process_identity) -> dict:
    start_clock = clock()  # Includes source admission, restored-state and hashing time.
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    require(type(workers) is int and 1 <= workers <= 4 and type(max_total_rpc_calls) is int
            and 0 < max_total_rpc_calls <= 2_000_000 and type(max_total_response_bytes) is int
            and 0 < max_total_response_bytes <= 4_000_000_000_000 and type(max_runtime_seconds) is int
            and 0 < max_runtime_seconds <= MAX_RUNTIME and type(max_output_bytes) is int
            and CHILD_BYTES + PARENT_RESERVE <= max_output_bytes <= GLOBAL_BYTES, "campaign_caps_invalid")
    require(prior_runs and len(prior_runs) == len(set(path.resolve() for path in prior_runs)) <= 8,
            "prior_raw_runs_required_unique_and_bounded")
    diagnostic_runs = diagnostic_runs or []
    all_prior_paths = prior_runs + diagnostic_runs
    require(len(all_prior_paths) == len(set(path.resolve() for path in all_prior_paths)) <= 8,
            "prior_diagnostic_runs_must_be_unique_and_bounded")
    diagnostic_usage = None
    if diagnostic_runs:
        require(type(diagnostic_rpc_call_cap) is int and 0 < diagnostic_rpc_call_cap <= 2_000_000
                and type(diagnostic_response_byte_cap) is int and 0 < diagnostic_response_byte_cap <= 4_000_000_000_000
                and type(diagnostic_runtime_second_cap) is int and 0 < diagnostic_runtime_second_cap <= MAX_RUNTIME,
                "explicit_diagnostic_conservative_caps_required")
        diagnostic_usage = {"rpc_calls": diagnostic_rpc_call_cap, "response_bytes": diagnostic_response_byte_cap,
                            "runtime_seconds": diagnostic_runtime_second_cap}
    else:
        require(all(value is None for value in (diagnostic_rpc_call_cap, diagnostic_response_byte_cap,
                                                diagnostic_runtime_second_cap)), "diagnostic_caps_without_runs")
    plan, binding, original = pilot.read_plan(plan_path, plan_sha, plan_head)
    collector.requested_addresses("append", first_block, last_block, plan)
    collector.check_destination(output, original, plan_path, plan, private)
    source = inventory.code_state()
    require(source.get("status") == "inspected" and source.get("head") == expected_head
            and bool(re.fullmatch(r"[0-9a-f]{40}", expected_head or "")) and source.get("tracked_worktree_clean") is True,
            "campaign_source_head_or_clean_state_differs")
    require(file_digest(COLLECTOR) == COLLECTOR_SHA256, "reviewed_collector_hash_differs")
    planned = chunks(first_block, last_block)
    priors = []
    for path in all_prior_paths:
        resolved = path.resolve()
        require(not path.is_symlink() and output.resolve() != resolved and resolved not in output.resolve().parents
                and output.resolve() not in resolved.parents, "prior_output_overlap")
        files = tree_inventory(path, cap=GLOBAL_BYTES)
        if path in diagnostic_runs:
            usage = dict(diagnostic_usage)
            policy = "explicit_conservative_diagnostic_charge_not_observed_use"
        else:
            require((path / "sessions").is_dir() and not (path / "sessions").is_symlink(),
                    "native_prior_sessions_missing_use_explicit_diagnostic_policy")
            owner = load_json(path / "OWNER.json")
            prior_contract = owner.get("contract", {})
            prior_sha = digest(json_bytes(prior_contract))
            require(prior_contract.get("schema_version") == "polymarket_native_collection_contract_v1"
                    and prior_contract.get("raw_payload_only") is True and prior_contract.get("analysis_admitted") is False
                    and owner.get("contract_sha256") == prior_sha, "native_prior_owner_contract_invalid")
            sessions = list((path / "sessions").iterdir())
            require(sessions and all(session.is_dir() and not session.is_symlink()
                    and load_json(session / "START.json").get("contract_sha256") == prior_sha for session in sessions),
                    "native_prior_sessions_empty_or_contract_mismatch")
            usage = child_usage(path)
            policy = "native_collector_durable_request_and_session_receipts"
        priors.append({"path": str(path), "inventory": files, "usage": usage, "usage_policy": policy})
    caps = {"rpc_calls": max_total_rpc_calls, "response_bytes": max_total_response_bytes,
            "runtime_seconds": max_runtime_seconds, "output_bytes": max_output_bytes}
    frozen = {"schema_version": "polymarket_native_campaign_plan_v1", "source_plan": binding,
              "source": source, "collector_sha256": COLLECTOR_SHA256, "chunks": planned, "workers": workers,
              "child_caps": CHILD_CAPS, "child_total_reserved_bytes": CHILD_BYTES, "window_blocks": WINDOW_BLOCKS,
              "max_attempts_per_chunk": MAX_ATTEMPTS, "limits": caps, "prior_raw_runs": priors,
              "raw_payload_only": True, "analysis_admitted": False, "rpc_endpoint_disclosed": False}
    frozen_sha = digest(json_bytes(frozen))
    require(len(json_bytes(frozen)) < PARENT_RESERVE // 2, "campaign_plan_control_limit")
    reservations(retained=sum(item["inventory"]["bytes"] for item in priors), running=0,
                 calls_used=sum(item["usage"]["rpc_calls"] for item in priors),
                 responses_used=sum(item["usage"]["response_bytes"] for item in priors), launch=False,
                 caps=caps, disk_free=shutil.disk_usage(output.parent).free)
    if resume:
        require(output.is_dir() and not output.is_symlink(), "campaign_resume_directory_missing")
    else:
        output.mkdir(mode=0o700)
    with collector.run_lock(output):
        if resume:
            owner = load_json(output / "PLAN.json")
            require(owner.get("plan") == frozen and owner.get("plan_sha256") == frozen_sha,
                    "campaign_resume_plan_differs")
            require(not (output / "COMPLETE.json").exists(), "campaign_already_complete")
        else:
            owner = {"run_id": str(uuid.uuid4()), "created_utc": now(), "plan_sha256": frozen_sha, "plan": frozen,
                     "admission_resources": inventory.resource_state(output.parent)}
            publish(output / "PLAN.json", json_bytes(owner))
            for name in ("chunks", "sessions", "manifests"):
                (output / name).mkdir(mode=0o700)
        require(all(path.name in {"PLAN.json", "LOCK", "chunks", "sessions", "manifests", "COMPLETE.json"}
                    or (path.name.startswith(".") and path.name.endswith(".pending")) for path in output.iterdir())
                and all((output / name).is_dir() and not (output / name).is_symlink()
                        for name in ("chunks", "sessions", "manifests")), "campaign_root_entries_invalid")
        prior_bytes = sum(item["inventory"]["bytes"] for item in priors)
        calls_used = sum(item["usage"]["rpc_calls"] for item in priors)
        responses_used = sum(item["usage"]["response_bytes"] for item in priors)
        elapsed = sum(item["usage"]["runtime_seconds"] for item in priors)
        for session in (output / "sessions").iterdir():
            started = load_json(session / "START.json")
            require(started.get("plan_sha256") == frozen_sha, "campaign_session_plan_differs")
            if (session / "RESULT.json").exists():
                duration = load_json(session / "RESULT.json").get("session_elapsed_seconds")
                require(type(duration) in (int, float) and duration >= 0, "campaign_session_runtime_invalid")
                elapsed += duration
            else:
                elapsed += elapsed_since(started["started_utc"])

        def check_runtime():
            require(elapsed + clock() - start_clock < caps["runtime_seconds"], "campaign_runtime_limit")

        # Mandatory even when every chunk was accepted before an interrupted
        # publication: that resume has no pending/active loop to supply a gate.
        check_runtime()
        session = output / "sessions" / uuid.uuid4().hex
        session.mkdir(mode=0o700)
        publish(session / "START.json", json_bytes({"plan_sha256": frozen_sha, "started_utc": now()}))
        active, accepted, finished_bytes, attempted = {}, {}, 0, {}
        result = {"status": "failed_raw_campaign", "run_id": owner["run_id"], "plan_sha256": frozen_sha,
                  "collection_complete": False, "analysis_admitted": False, "error_code": None}
        stopping = False
        try:
            # Reopen prior attempt inventories, and refuse a live recovered PID.
            expected_chunk_names = {chunk["chunk"] for chunk in planned}
            require({path.name for path in (output / "chunks").iterdir()} <= expected_chunk_names,
                    "unexpected_campaign_chunk")
            for chunk in planned:
                directory = output / "chunks" / chunk["chunk"]
                attempts = []
                if directory.exists():
                    require(directory.is_dir() and not directory.is_symlink(), "chunk_directory_invalid")
                    require({path.name for path in directory.iterdir()} <= {f"a{index:02d}" for index in range(1, MAX_ATTEMPTS + 1)},
                            "unexpected_chunk_attempt")
                    for attempt_dir in sorted(directory.iterdir()):
                        receipt = load_json(attempt_dir / "START.json")
                        require(receipt.get("plan_sha256") == frozen_sha and receipt.get("chunk") == chunk,
                                "chunk_attempt_binding_differs")
                        if (attempt_dir / "PROCESS.json").exists():
                            previous = load_json(attempt_dir / "PROCESS.json")
                            actual = identity(previous["pid"])
                            require(actual is None or actual.get("start_ticks") != previous["start_ticks"]
                                    or actual.get("state") == "Z", "prior_child_still_running")
                        else:
                            raise pilot.PilotBlocked("orphan_child_identity_missing")
                        raw = attempt_dir / "raw"
                        if (attempt_dir / "EXIT.json").exists():
                            ended = load_json(attempt_dir / "EXIT.json")
                            require(ended.get("plan_sha256") == frozen_sha and ended.get("chunk") == chunk
                                    and ended.get("status") in {"accepted_raw_child", "failed_raw_child"},
                                    "returned_child_receipt_binding_differs")
                            files = tree_inventory(raw, cap=CHILD_CAPS["max_output_bytes"]) if raw.exists() else {"files": [], "bytes": 0}
                            require(digest(json_bytes(files)) == ended["raw_inventory_sha256"]
                                    and load_json(attempt_dir / "INVENTORY.json") == files, "returned_child_inventory_changed")
                            usage = child_usage(raw) if raw.exists() else {"rpc_calls": 0, "response_bytes": 0, "runtime_seconds": 0.0}
                            require(usage == ended.get("usage"), "returned_child_usage_differs_from_durable_journals")
                            if ended["status"] == "accepted_raw_child":
                                checked = validate_child(raw, expected_child_contract(plan, binding, source, chunk))
                                require(checked == ended["accepted"], "returned_child_acceptance_changed")
                                require(chunk["chunk"] not in accepted, "duplicate_accepted_chunk")
                                accepted[chunk["chunk"]] = ended
                        else:
                            # No process is alive and no result was committed. Do
                            # not rewrite it; charge durable requests, retain all files.
                            usage = child_usage(raw) if raw.exists() else {"rpc_calls": 0, "response_bytes": 0, "runtime_seconds": 0.0}
                        calls_used += usage["rpc_calls"]
                        responses_used += usage["response_bytes"]
                        finished_bytes += tree_bytes(attempt_dir)
                        attempts.append(attempt_dir)
                attempted[chunk["chunk"]] = len(attempts)
            check_runtime()  # Restored-child inventory/hash validation counts too.
            pending = [chunk for chunk in planned if chunk["chunk"] not in accepted]
            while pending or active:
                if elapsed + clock() - start_clock >= caps["runtime_seconds"]:
                    stopping, result["error_code"] = True, "campaign_runtime_limit"
                if inventory.code_state() != source or file_digest(COLLECTOR) != COLLECTOR_SHA256:
                    stopping, result["error_code"] = True, "campaign_source_changed"
                while pending and len(active) < workers and not stopping:
                    check_runtime()
                    chunk = pending[0]
                    count = attempted[chunk["chunk"]] + 1
                    require(count <= MAX_ATTEMPTS, "chunk_attempt_limit")
                    control_bytes = tree_bytes(output / "sessions") + tree_bytes(output / "manifests")
                    control_bytes += (output / "PLAN.json").stat().st_size
                    retained = prior_bytes + finished_bytes + control_bytes
                    reservations(retained=retained, running=len(active), calls_used=calls_used,
                                 responses_used=responses_used, launch=True, caps=caps,
                                 disk_free=shutil.disk_usage(output).free)
                    available = inventory.resource_state().get("ram_bytes", {}).get("MemAvailable")
                    require(type(available) is int and available >= (len(active) + 1) * CHILD_CAPS["max_ram_bytes"],
                            "campaign_available_ram_reservation_limit")
                    directory = output / "chunks" / chunk["chunk"]
                    directory.mkdir(mode=0o700, exist_ok=True)
                    attempt_dir = directory / f"a{count:02d}"
                    attempt_dir.mkdir(mode=0o700)
                    expected = expected_child_contract(plan, binding, source, chunk)
                    publish(attempt_dir / "START.json", json_bytes({"plan_sha256": frozen_sha, "chunk": chunk,
                            "attempt": count, "started_utc": now(), "expected_child_contract_sha256": digest(json_bytes(expected))}))
                    raw = attempt_dir / "raw"
                    child = launcher(child_command(plan_path, plan_sha, plan_head, expected_head, raw, chunk, private))
                    active[chunk["chunk"]] = {"child": child, "chunk": chunk, "directory": attempt_dir, "raw": raw,
                                              "expected": expected, "launched": clock(), "terminating": None}
                    proof = identity(child.pid)
                    require(proof is not None and type(proof.get("start_ticks")) is int, "new_child_identity_unavailable")
                    publish(attempt_dir / "PROCESS.json", json_bytes(proof))
                    attempted[chunk["chunk"]] = count
                    pending.pop(0)
                for key, running in list(active.items()):
                    child = running["child"]
                    child.pump()
                    timeout = clock() - running["launched"] >= CHILD_CAPS["max_runtime_seconds"]
                    parent_timeout = elapsed + clock() - start_clock >= caps["runtime_seconds"]
                    if (timeout or parent_timeout or child.overflow) and child.poll() is None:
                        stopping = True
                        result["error_code"] = "child_timeout_or_log_limit"
                        if running["terminating"] is None:
                            child.terminate()  # Only this owned Popen handle, never PID globbing.
                            running["terminating"] = clock()
                        elif clock() - running["terminating"] >= 15:
                            child.kill()
                    exit_code = child.poll()
                    if exit_code is None:
                        continue
                    child.pump()
                    ended = finish_child(running, frozen_sha)
                    calls_used += ended["usage"]["rpc_calls"]
                    responses_used += ended["usage"]["response_bytes"]
                    finished_bytes += tree_bytes(running["directory"])
                    if ended["status"] == "accepted_raw_child":
                        accepted[key] = ended
                    else:
                        stopping, result["error_code"] = True, ended["error_code"]
                    del active[key]
                if stopping and not active:
                    break
                if active:
                    sleep(0.2)
            require(not stopping, result["error_code"] or "campaign_child_failure")
            check_runtime()
            ordered = [accepted[chunk["chunk"]] for chunk in planned if chunk["chunk"] in accepted]
            rows = verify_parent_coverage(ordered, planned)
            rebound_plan, rebound_binding, rebound_original = pilot.read_plan(plan_path, plan_sha, plan_head)
            require(rebound_plan == plan and rebound_binding == binding and rebound_original == original
                    and inventory.code_state() == source, "campaign_final_source_binding_changed")
            check_runtime()
            for prior in priors:
                require(tree_inventory(Path(prior["path"]), cap=GLOBAL_BYTES) == prior["inventory"], "prior_raw_inventory_changed")
                check_runtime()
            actual_bytes = prior_bytes + tree_bytes(output)
            require(actual_bytes + PARENT_RESERVE <= caps["output_bytes"] and calls_used <= caps["rpc_calls"]
                    and responses_used <= caps["response_bytes"], "campaign_final_budget_exceeded")
            check_runtime()
            manifest = {"schema_version": "polymarket_native_campaign_manifest_v1", "status": "raw_campaign_complete",
                        "run_id": owner["run_id"], "plan_sha256": frozen_sha, "accepted_chunks": ordered,
                        "rows": rows, "rpc_calls_charged": calls_used, "response_bytes_charged": responses_used,
                        "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True,
                        "limitations": ["Only the literal append interval, five known exchanges and frozen topics are covered.",
                                        "Cross-stage native-key reconciliation and exact interior timestamps remain separate gates."]}
            name = uuid.uuid4().hex + ".json"
            manifest_raw = json_bytes(manifest)
            publish(output / "manifests" / name, manifest_raw)
            check_runtime()  # Publication/fsync time must not evade the final gate.
            publish(output / "COMPLETE.json", json_bytes({"status": "raw_campaign_complete_not_analysis_admission",
                    "run_id": owner["run_id"], "plan_sha256": frozen_sha, "manifest_path": "manifests/" + name,
                    "manifest_sha256": digest(manifest_raw)}))
            result.update(status="raw_campaign_complete", collection_complete=True, rows=rows)
        except BaseException as exc:
            result["error_code"] = str(exc) if isinstance(exc, pilot.PilotBlocked) else "campaign_interrupted_or_control_failure"
            # Drain only this invocation's known children before returning. Their
            # individual timeout and a short termination grace remain bounded.
            for running in active.values():
                child = running["child"]
                if child.poll() is None:
                    child.terminate()
            deadline = clock() + 15
            while any(item["child"].poll() is None for item in active.values()):
                for running in active.values():
                    running["child"].pump()
                    if clock() >= deadline and running["child"].poll() is None:
                        running["child"].kill()
                sleep(0.1)
            for running in active.values():
                if not (running["directory"] / "EXIT.json").exists():
                    try:
                        ended = finish_child(running, frozen_sha, force_failure="campaign_stopped_before_acceptance")
                        calls_used += ended["usage"]["rpc_calls"]
                        responses_used += ended["usage"]["response_bytes"]
                    except Exception:
                        # Remaining reservation is not silently released when a
                        # checkpoint cannot be written or inspected.
                        calls_used += CHILD_CAPS["max_rpc_calls"]
                        responses_used += CHILD_CAPS["max_total_response_bytes"]
        finally:
            result.update(accepted_chunks=len(accepted), rpc_calls_charged=calls_used,
                          response_bytes_charged=responses_used, session_elapsed_seconds=max(0, clock() - start_clock),
                          aggregate_bytes=prior_bytes + tree_bytes(output), children_still_running=False)
            publish(session / "RESULT.json", json_bytes(result))
            if result["status"] != "raw_campaign_complete":
                publish(session / "FAILED.json", json_bytes({"status": "failed_raw_campaign", "error_code": result["error_code"]}))
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-plan", "output-dir", "rpc-private-file"):
        parser.add_argument("--" + name, type=Path, required=name != "rpc-private-file")
    for name in ("source-plan-sha256", "plan-source-head", "expected-head"):
        parser.add_argument("--" + name, required=True)
    for name in ("first-block", "last-block", "workers", "max-total-rpc-calls", "max-total-response-bytes",
                 "max-runtime-seconds", "max-output-bytes"):
        parser.add_argument("--" + name, type=int, required=True)
    parser.add_argument("--prior-raw-run", type=Path, action="append", required=True)
    parser.add_argument("--prior-diagnostic-run", type=Path, action="append")
    parser.add_argument("--diagnostic-rpc-call-cap", type=int)
    parser.add_argument("--diagnostic-response-byte-cap", type=int)
    parser.add_argument("--diagnostic-runtime-second-cap", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Campaign requires the canonical mounted EC2 checkout.")
    if not args.output_dir.is_absolute() or Path("/mnt/data/runs") not in args.output_dir.resolve().parents:
        parser.error("Campaign output must be an explicit child of /mnt/data/runs.")
    if any(not path.is_absolute() or Path("/mnt/data/runs") not in path.resolve().parents
           for path in args.prior_raw_run + (args.prior_diagnostic_run or [])):
        parser.error("Prior raw runs must be explicit children of /mnt/data/runs.")
    try:
        for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[variable] = "1"
        result = run_campaign(args.source_plan, args.source_plan_sha256, args.plan_source_head, args.expected_head,
                              args.output_dir, first_block=args.first_block, last_block=args.last_block,
                              workers=args.workers, prior_runs=args.prior_raw_run, max_total_rpc_calls=args.max_total_rpc_calls,
                              max_total_response_bytes=args.max_total_response_bytes,
                              max_runtime_seconds=args.max_runtime_seconds, max_output_bytes=args.max_output_bytes,
                              diagnostic_runs=args.prior_diagnostic_run, diagnostic_rpc_call_cap=args.diagnostic_rpc_call_cap,
                              diagnostic_response_byte_cap=args.diagnostic_response_byte_cap,
                              diagnostic_runtime_second_cap=args.diagnostic_runtime_second_cap,
                              resume=args.resume, rpc_private_file=args.rpc_private_file)
    except Exception:
        parser.error("Campaign source/destination admission failed; no completion was published.")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "raw_campaign_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
