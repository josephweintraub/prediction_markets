"""Adaptive append campaign with exact, externally referenced v1 chunk reuse.

No old run is resumed or modified. The sealed v1 campaign and its six prior
roots form one once-charged baseline. An explicit reboot-equivalence certificate
links sealed old bindings to the fresh strict writer binding and is charged once.
First fresh chunk primes adaptive behavior before parallel launches. Raw
completion is not analysis admission.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import collect_polymarket_native_increment_v2 as adaptive
from scripts import run_polymarket_native_collection_campaign as legacy

v1, pilot, inventory = adaptive.v1, adaptive.pilot, adaptive.inventory
require, json_bytes, load_json, digest = v1.require, v1.json_bytes, v1.load_json, v1.digest
OLD_CONTROLLER_SHA = "dcdce74740009f62f6fbd3a65cef33351fc7bbe4ab6f2fb92b5e3e7e96c1a7a4"
OLD_HEAD = "19202a820bef569f449a34fc731107f16470e155"
OLD_PLAN_FILE_SHA = "c4fba53915536077f3619ac3fae352edbd509722d6d223f54b4c2f4973917526"
OLD_PLAN_SHA = "364a81171cb31f098e1876832ad9a703c6ead963862fc56cd2084a44a1ac6682"
OLD_RESULT_SHA = "96fb0dd73491fc78d72810bfb2fc0cd43b976c5503244c86d47f0e6cce4193e4"
COLLECTOR = REPO / "scripts/collect_polymarket_native_increment_v2.py"
COLLECTOR_SHA = "fc75da08eec45a9fd339ea030261c76a7ad2f62768c3fa0f1a4aad1393a7c637"
FIRST, LAST = 88_979_538, 94_796_039
REUSED = {f"c{index:06d}" for index in (*range(1, 18), 19, 20)}
CAPS = {"rpc_calls": 900000, "response_bytes": 3000000000000, "runtime_seconds": 172800, "output_bytes": 128000000000}
PREP_CHARGE = {"rpc_calls": 100, "response_bytes": 268435456, "runtime_seconds": 1200}


def validate_reboot_bridge(path, sha256, fresh_plan_path, fresh_plan_sha256, fresh_plan_head):
    from scripts import rebind_polymarket_refresh_after_reboot as admission
    return admission.validate_bridge(path, sha256, fresh_plan_path, fresh_plan_sha256, fresh_plan_head)


@contextmanager
def readonly_old_lock(root: Path):
    fd = os.open(root / "LOCK", os.O_RDONLY | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise pilot.PilotBlocked("reuse_campaign_parent_still_running") from None
        yield
    finally:
        os.close(fd)


def root_controls(root: Path) -> dict:
    """Bounded root controls, deliberately excluding separately inventoried chunks."""
    files, total = [], 0
    for directory, names, filenames in os.walk(root, followlinks=False):
        require(all(not (Path(directory) / name).is_symlink() for name in names), "reuse_control_symlink")
        if Path(directory) == root:
            names[:] = [name for name in names if name != "chunks"]
        for name in filenames:
            path = Path(directory) / name
            require(path.is_file() and not path.is_symlink() and len(files) < 1024, "reuse_control_file_limit")
            size = path.stat().st_size
            total += size
            require(total <= legacy.PARENT_RESERVE, "reuse_control_byte_limit")
            files.append({"path": str(path.relative_to(root)), "bytes": size, "sha256": legacy.file_digest(path)})
    return {"files": sorted(files, key=lambda item: item["path"]), "bytes": total}


def compact_inventory(path: Path, value: dict) -> dict:
    return {"path": str(path), "inventory_sha256": digest(json_bytes(value)), "bytes": value["bytes"],
            "files": len(value["files"])}


def sealed_parent_result(session: Path, run_id: str) -> dict:
    require(legacy.file_digest(session / "RESULT.json") == OLD_RESULT_SHA,
            "reuse_sealed_parent_result_hash_differs")
    start, ended = load_json(session / "START.json"), load_json(session / "RESULT.json")
    require(start.get("plan_sha256") == OLD_PLAN_SHA and ended.get("plan_sha256") == OLD_PLAN_SHA
            and ended.get("run_id") == run_id and ended.get("status") == "failed_raw_campaign"
            and ended.get("children_still_running") is False
            and type(ended.get("session_elapsed_seconds")) in (int, float)
            and math.isfinite(ended["session_elapsed_seconds"]) and ended["session_elapsed_seconds"] >= 0,
            "reuse_parent_not_sealed")
    return ended


def load_reuse(root: Path, plan_file_sha: str, old_head: str, source_plan: dict, binding: dict,
               identity=legacy.process_identity) -> dict:
    require(plan_file_sha == OLD_PLAN_FILE_SHA and old_head == OLD_HEAD, "reuse_authorized_plan_or_source_differs")
    require(legacy.file_digest(Path(legacy.__file__)) == OLD_CONTROLLER_SHA
            and legacy.file_digest(Path(v1.__file__)) == adaptive.V1_SHA256, "reuse_v1_helpers_changed")
    require(not root.is_symlink() and root.is_dir() and not (root / "PLAN.json").is_symlink()
            and (root / "PLAN.json").is_file() and legacy.file_digest(root / "PLAN.json") == plan_file_sha,
            "reuse_plan_file_hash_differs")
    require(all((root / name).is_dir() and not (root / name).is_symlink() for name in ("chunks", "sessions", "manifests")),
            "reuse_root_directory_topology_invalid")
    with readonly_old_lock(root):
        owner = load_json(root / "PLAN.json")
        frozen = owner["plan"]
        require(owner.get("plan_sha256") == OLD_PLAN_SHA == digest(json_bytes(frozen))
                and frozen["source"]["head"] == old_head and frozen["source"].get("tracked_worktree_clean") is True
                and frozen.get("collector_sha256") == adaptive.V1_SHA256 and frozen.get("source_plan") == binding
                and frozen.get("limits") == CAPS and frozen.get("chunks") == legacy.chunks(FIRST, LAST),
                "reuse_frozen_contract_differs")
        require(not (root / "COMPLETE.json").exists(), "reuse_expected_failed_campaign")
        prior = frozen.get("prior_raw_runs")
        require(type(prior) is list and len(prior) == 6 and len({Path(item["path"]).resolve() for item in prior}) == 6,
                "reuse_six_unique_prior_roots_required")
        prior_paths = [Path(item["path"]).resolve() for item in prior]
        require(all(left not in right.parents and right not in left.parents for index, left in enumerate(prior_paths)
                    for right in prior_paths[index + 1:]), "reuse_prior_roots_not_disjoint")
        baseline_calls = baseline_responses = baseline_bytes = 0
        baseline_runtime = 0.0
        inventories = []
        native_count = diagnostic_count = 0
        for item in prior:
            path = Path(item["path"])
            require(root.resolve() != path.resolve() and root.resolve() not in path.resolve().parents
                    and path.resolve() not in root.resolve().parents, "reuse_prior_roots_overlap")
            actual = legacy.tree_inventory(path, cap=legacy.GLOBAL_BYTES)
            require(actual == item["inventory"], "reuse_prior_inventory_changed")
            if item["usage_policy"] == "native_collector_durable_request_and_session_receipts":
                usage = legacy.child_usage(path)
                native_count += 1
                require(usage == item["usage"], "reuse_native_prior_usage_changed")
            else:
                require(item["usage_policy"] == "explicit_conservative_diagnostic_charge_not_observed_use"
                        and item["usage"] == {"rpc_calls": 10000, "response_bytes": 17179869184, "runtime_seconds": 21600},
                        "reuse_diagnostic_policy_changed")
                usage = item["usage"]
                diagnostic_count += 1
            baseline_calls += usage["rpc_calls"]
            baseline_responses += usage["response_bytes"]
            baseline_runtime += usage["runtime_seconds"]
            baseline_bytes += actual["bytes"]
            inventories.append(compact_inventory(path, actual))
        require(native_count == diagnostic_count == 3, "reuse_prior_policy_counts_differ")
        controls = root_controls(root)
        baseline_bytes += controls["bytes"]
        session_results = []
        for session in (root / "sessions").iterdir():
            require(session.is_dir() and not session.is_symlink(), "reuse_parent_session_directory_invalid")
            ended = sealed_parent_result(session, owner["run_id"])
            baseline_runtime += ended["session_elapsed_seconds"]
            session_results.append(ended)
        require(len(session_results) == 1, "reuse_exactly_one_sealed_parent_session_required")
        accepted, attempts = {}, []
        expected_dirs = {f"c{index:06d}" for index in range(1, 21)}
        require({path.name for path in (root / "chunks").iterdir()} == expected_dirs, "reuse_old_chunk_topology_differs")
        for chunk in legacy.chunks(FIRST, LAST)[:20]:
            directory = root / "chunks" / chunk["chunk"]
            require(directory.is_dir() and not directory.is_symlink()
                    and {path.name for path in directory.iterdir()} == {"a01"}, "reuse_old_attempt_topology_differs")
            attempt, raw = directory / "a01", directory / "a01/raw"
            require(attempt.is_dir() and not attempt.is_symlink(), "reuse_old_attempt_symlink")
            start, process, ended = (load_json(attempt / name) for name in ("START.json", "PROCESS.json", "EXIT.json"))
            require(start.get("plan_sha256") == ended.get("plan_sha256") == OLD_PLAN_SHA
                    and start.get("chunk") == ended.get("chunk") == chunk, "reuse_attempt_binding_differs")
            active = identity(process["pid"])
            require(active is None or active.get("start_ticks") != process["start_ticks"] or active.get("state") == "Z",
                    "reuse_old_child_still_running")
            raw_inventory = legacy.tree_inventory(raw, cap=legacy.CHILD_CAPS["max_output_bytes"])
            require(load_json(attempt / "INVENTORY.json") == raw_inventory
                    and (attempt / "INVENTORY.json").read_bytes() == json_bytes(raw_inventory)
                    and digest(json_bytes(raw_inventory)) == ended.get("raw_inventory_sha256"), "reuse_old_raw_inventory_changed")
            usage = legacy.child_usage(raw)
            require(usage == ended.get("usage"), "reuse_old_usage_differs_from_journals")
            baseline_calls += usage["rpc_calls"]
            baseline_responses += usage["response_bytes"]
            expected = legacy.expected_child_contract(source_plan, binding, frozen["source"], chunk)
            if chunk["chunk"] in REUSED:
                checked = legacy.validate_child(raw, expected)
                require(ended.get("status") == "accepted_raw_child" and ended.get("accepted") == checked,
                        "reuse_old_acceptance_differs")
                accepted[chunk["chunk"]] = {"chunk": chunk, "status": "accepted_raw_child", "accepted": checked,
                                            "external_attempt": str(attempt), "producer_head": old_head}
            else:
                old_owner = load_json(raw / "OWNER.json")
                require(chunk["chunk"] == "c000018" and ended.get("status") == "failed_raw_child"
                        and not (raw / "COMPLETE.json").exists() and old_owner.get("contract") == expected
                        and old_owner.get("contract_sha256") == digest(json_bytes(expected)), "reuse_failed_chunk_not_audit_only")
            # Hierarchical: retain each bounded attempt inventory separately; do
            # not feed >40k old files into the legacy 10k-file flat limit.
            actual = legacy.tree_inventory(attempt, cap=legacy.CHILD_BYTES)
            baseline_bytes += actual["bytes"]
            attempts.append(compact_inventory(attempt, actual))
        terminal = session_results[0]
        require(set(accepted) == REUSED and terminal.get("accepted_chunks") == 19
                and baseline_calls == terminal.get("rpc_calls_charged") == 45663
                and baseline_responses == terminal.get("response_bytes_charged") == 76324722328,
                "reuse_once_only_resource_reconciliation_failed")
        return {"campaign": str(root), "plan_file_sha256": plan_file_sha, "plan_sha256": OLD_PLAN_SHA,
                "source_head": old_head, "accepted": accepted, "prior_inventories": inventories,
                "root_controls": compact_inventory(root, controls), "attempt_inventories": attempts, "bytes": baseline_bytes,
                "rpc_calls": baseline_calls, "response_bytes": baseline_responses, "runtime_seconds": baseline_runtime}


def expected_contract(plan, binding, source, chunk):
    return {"schema_version": "polymarket_native_collection_contract_v2", "source_plan": binding, "source": source,
            "mode": "append", "first_block": chunk["first_block"], "last_block": chunk["last_block"],
            "addresses": list(pilot.ADDRESSES), "topics": sorted(set(pilot.EXPECTED_TOPICS.values())),
            "limits": v1.limits(*[legacy.CHILD_CAPS[key] for key in
                    ("max_rpc_calls", "max_output_bytes", "max_runtime_seconds", "max_rows", "max_ram_bytes", "max_total_response_bytes")]),
            "adaptive_policy": {"base_blocks": 50, "minimum_blocks": 1, "split": "binary_on_proven_getlogs_byte_limit_only"},
            "v1_helper_sha256": adaptive.V1_SHA256, "end_exclusive_utc": plan["end_exclusive_utc"],
            "raw_payload_only": True, "analysis_admitted": False, "interior_exact_timestamps_collected": False,
            "provider_block_timestamp_used_as_clock": False}


def validate_child(raw, expected):
    owner, marker, manifest = (load_json(raw / name) for name in ("OWNER.json", "COMPLETE.json", "manifest.json"))
    sha = digest(json_bytes(expected))
    require(owner.get("contract") == expected and owner.get("contract_sha256") == sha
            and marker.get("contract_sha256") == sha and marker.get("run_id") == owner["run_id"]
            and marker.get("status") == "raw_collection_complete_not_analysis_admission"
            and marker.get("manifest_sha256") == legacy.file_digest(raw / "manifest.json")
            and manifest.get("contract") == expected and manifest.get("contract_sha256") == sha
            and manifest.get("schema_version") == "polymarket_native_collection_manifest_v2"
            and manifest.get("status") == "raw_collection_complete" and manifest.get("run_id") == owner["run_id"]
            and manifest.get("raw_payload_only") is True
            and manifest.get("collection_complete") is True and manifest.get("analysis_admitted") is False,
            "adaptive_child_completion_binding_differs")
    roots = v1.windows(expected["first_block"], expected["last_block"], 50)
    require(manifest.get("roots") == roots, "adaptive_child_root_range_differs")
    adaptive.validate_artifacts(raw, expected, roots, manifest["splits"], manifest["windows"])
    rows = sum(record["evidence"]["rows"] for record in manifest["windows"])
    require(type(rows) is int and type(manifest.get("rows")) is int and type(manifest.get("distinct_native_keys")) is int
            and rows == manifest.get("rows") == manifest.get("distinct_native_keys") <= legacy.CHILD_CAPS["max_rows"],
            "adaptive_child_row_reconciliation")
    usage = legacy.child_usage(raw)
    require(usage["rpc_calls"] <= 1200 and usage["response_bytes"] <= 4 * 1024**3
            and usage["runtime_seconds"] <= 3630, "adaptive_child_resource_cap_exceeded")
    return {"first_block": expected["first_block"], "last_block": expected["last_block"], "rows": rows,
            "manifest_sha256": legacy.file_digest(raw / "manifest.json"), "usage": usage, "split_nodes": len(manifest["splits"])}


def command(plan_path, plan_sha, plan_head, head, raw, chunk, private):
    argv = [sys.executable, str(COLLECTOR), "--source-plan", str(plan_path), "--source-plan-sha256", plan_sha,
            "--plan-source-head", plan_head, "--expected-head", head, "--output-dir", str(raw),
            "--first-block", str(chunk["first_block"]), "--last-block", str(chunk["last_block"]), "--rpc-private-file", str(private)]
    for key, value in legacy.CHILD_CAPS.items():
        argv += ["--" + key.replace("_", "-"), str(value)]
    return argv


def finish(running, plan_sha, force_error=None):
    child, raw, directory = running["child"], running["raw"], running["directory"]
    require(child.poll() is not None, "adaptive_campaign_child_still_running")
    child.pump()
    usage = legacy.child_usage(raw) if raw.exists() else {"rpc_calls": 0, "response_bytes": 0, "runtime_seconds": 0.0}
    error, checked = force_error, None
    try:
        require(error is None and child.poll() == 0 and not child.overflow and running["terminating"] is None,
                error or "adaptive_campaign_child_nonzero_or_timeout")
        checked = validate_child(raw, running["expected"])
    except Exception as exc:
        error = str(exc) if isinstance(exc, pilot.PilotBlocked) else "adaptive_campaign_child_acceptance_failure"
    files = legacy.tree_inventory(raw, cap=legacy.CHILD_CAPS["max_output_bytes"]) if raw.exists() else {"files": [], "bytes": 0}
    v1.publish(directory / "INVENTORY.json", json_bytes(files))
    ended = {"plan_sha256": plan_sha, "chunk": running["chunk"], "status": "accepted_raw_child" if checked else "failed_raw_child",
             "accepted": checked, "usage": usage, "error_code": error, "raw_inventory_sha256": digest(json_bytes(files)),
             "streams": child.receipt(), "exit_code": child.poll()}
    v1.publish(directory / "EXIT.json", json_bytes(ended))
    require(legacy.tree_bytes(directory) <= legacy.CHILD_BYTES, "adaptive_campaign_child_bytes_exceeded")
    return ended


def run_campaign(plan_path, plan_sha, plan_head, head, output, *, reuse_campaign, reuse_plan_file_sha256,
                 reuse_source_head, reboot_bridge, reboot_bridge_sha256, workers=4, resume=False, rpc_private_file=None,
                 launcher=legacy.SubprocessChild, clock=time.monotonic, sleep=time.sleep,
                 identity=legacy.process_identity):
    started = clock()
    require(type(workers) is int and 1 <= workers <= 4, "adaptive_campaign_workers_invalid")
    require(legacy.file_digest(COLLECTOR) == COLLECTOR_SHA, "adaptive_collector_source_hash_differs")
    plan, binding, original = pilot.read_plan(plan_path, plan_sha, plan_head)
    proof = validate_reboot_bridge(reboot_bridge, reboot_bridge_sha256, plan_path, plan_sha, plan_head)
    require(proof.get("fresh_plan") == plan and proof.get("fresh_binding") == binding,
            "adaptive_reboot_fresh_plan_binding_differs")
    prep = proof.get("prep_charge", {})
    require(all(type(prep.get(key)) is int and prep[key] == value for key, value in PREP_CHARGE.items())
            and type(prep.get("output_bytes")) is int and prep["output_bytes"] >= 0
            and proof.get("admission_tree_inventory", {}).get("bytes") == prep["output_bytes"],
            "adaptive_reboot_preparation_charge_differs")
    v1.requested_addresses("append", FIRST, LAST, plan)
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    v1.check_destination(output, original, plan_path, plan, private)
    require(output.resolve() != reuse_campaign.resolve() and reuse_campaign.resolve() not in output.resolve().parents,
            "adaptive_campaign_output_overlaps_reuse")
    source = inventory.code_state()
    require(source.get("head") == head and source.get("status") == "inspected"
            and source.get("tracked_worktree_clean") is True, "adaptive_campaign_source_not_clean")
    baseline = load_reuse(reuse_campaign, reuse_plan_file_sha256, reuse_source_head,
                          proof["old_plan"], proof["old_binding"], identity)
    admission_root = Path(proof["admission_tree_inventory"]["path"]).resolve()
    protected = [reuse_campaign, output, *(Path(item["path"]) for item in baseline["prior_inventories"])]
    require(all(admission_root != path.resolve() and admission_root not in path.resolve().parents
                and path.resolve() not in admission_root.parents for path in protected),
            "adaptive_reboot_admission_tree_overlaps_retained_roots")
    require(admission_root in reboot_bridge.resolve().parents and admission_root in plan_path.resolve().parents,
            "adaptive_reboot_admission_controls_outside_charged_tree")
    retained_base_bytes = baseline["bytes"] + prep["output_bytes"]
    planned = legacy.chunks(FIRST, LAST)
    frozen = {"schema_version": "polymarket_native_campaign_plan_v2", "source_plan": binding, "source": source,
              "collector_sha256": COLLECTOR_SHA, "v1_controller_sha256": OLD_CONTROLLER_SHA, "reuse": baseline,
              "reboot_admission": proof,
              "chunks": planned, "limits": CAPS, "child_caps": legacy.CHILD_CAPS, "workers": workers,
              "priming": "first_fresh_pending_chunk_must_be_accepted_before_parallel_launches",
              "raw_payload_only": True, "analysis_admitted": False}
    sha = digest(json_bytes(frozen))
    require(len(json_bytes(frozen)) < legacy.PARENT_RESERVE // 2
            and retained_base_bytes + legacy.PARENT_RESERVE <= CAPS["output_bytes"], "adaptive_campaign_initial_control_budget")
    require(legacy.shutil.disk_usage(output.parent).free >= v1.FREE_FLOOR + legacy.PARENT_RESERVE,
            "adaptive_campaign_initial_free_floor")
    if resume:
        require(output.is_dir() and not output.is_symlink(), "adaptive_campaign_resume_missing")
    else:
        output.mkdir(mode=0o700)
    with v1.run_lock(output):
        if resume:
            owner = load_json(output / "PLAN.json")
            require(owner.get("plan") == frozen and owner.get("plan_sha256") == sha, "adaptive_campaign_resume_plan_differs")
            require(not (output / "COMPLETE.json").exists(), "adaptive_campaign_already_complete")
        else:
            owner = {"run_id": str(uuid.uuid4()), "created_utc": legacy.now(), "plan_sha256": sha, "plan": frozen}
            v1.publish(output / "PLAN.json", json_bytes(owner))
            for name in ("chunks", "sessions", "manifests"):
                (output / name).mkdir(mode=0o700)
        require(all(path.name in {"PLAN.json", "LOCK", "chunks", "sessions", "manifests", "PRIMED.json", "COMPLETE.json"}
                    or (path.name.startswith(".") and path.name.endswith(".pending")) for path in output.iterdir())
                and all((output / name).is_dir() and not (output / name).is_symlink()
                        for name in ("chunks", "sessions", "manifests")), "adaptive_campaign_root_topology_invalid")
        elapsed = baseline["runtime_seconds"] + prep["runtime_seconds"]
        for prior in (output / "sessions").iterdir():
            require(prior.is_dir() and not prior.is_symlink(), "adaptive_campaign_session_topology_invalid")
            start = load_json(prior / "START.json")
            require(start.get("plan_sha256") == sha, "adaptive_campaign_session_binding_differs")
            if (prior / "RESULT.json").exists():
                ended = load_json(prior / "RESULT.json")
                duration = ended.get("session_elapsed_seconds")
                require(type(duration) in (int, float) and math.isfinite(duration) and duration >= 0
                        and ended.get("plan_sha256") == sha and ended.get("run_id") == owner["run_id"]
                        and ended.get("status") in {"failed_raw_campaign", "raw_campaign_complete"}
                        and ended.get("collection_complete") is (ended.get("status") == "raw_campaign_complete")
                        and ended.get("analysis_admitted") is False, "adaptive_campaign_session_result_invalid")
                elapsed += duration
            else:
                elapsed += legacy.elapsed_since(start["started_utc"])
        def deadline():
            require(elapsed + clock() - started < CAPS["runtime_seconds"], "adaptive_campaign_runtime_limit")
        deadline()
        session = output / "sessions" / uuid.uuid4().hex
        session.mkdir(mode=0o700)
        v1.publish(session / "START.json", json_bytes({"plan_sha256": sha, "started_utc": legacy.now()}))
        accepted = dict(baseline["accepted"])
        counts = {"rpc_calls": baseline["rpc_calls"] + prep["rpc_calls"],
                  "response_bytes": baseline["response_bytes"] + prep["response_bytes"]}
        closed_bytes, attempts, active = 0, {}, {}
        result = {"status": "failed_raw_campaign", "run_id": owner["run_id"], "plan_sha256": sha,
                  "collection_complete": False, "analysis_admitted": False}
        stopping = False
        try:
            require({path.name for path in (output / "chunks").iterdir()} <= {chunk["chunk"] for chunk in planned},
                    "adaptive_campaign_unknown_chunk_directory")
            for chunk in planned:
                directory = output / "chunks" / chunk["chunk"]
                require(not directory.is_symlink(), "adaptive_campaign_chunk_symlink")
                entries = sorted(directory.iterdir()) if directory.exists() else []
                require(all(path.name in {"a01", "a02", "a03"} and path.is_dir() and not path.is_symlink() for path in entries),
                        "adaptive_campaign_attempt_topology_invalid")
                attempts[chunk["chunk"]] = len(entries)
                for attempt in entries:
                    attempt_start = load_json(attempt / "START.json")
                    require(attempt_start.get("plan_sha256") == sha and attempt_start.get("chunk") == chunk,
                            "adaptive_campaign_returned_start_binding_differs")
                    process = load_json(attempt / "PROCESS.json")
                    alive = identity(process["pid"])
                    require(alive is None or alive.get("start_ticks") != process["start_ticks"] or alive.get("state") == "Z",
                            "adaptive_campaign_prior_child_running")
                    ended = load_json(attempt / "EXIT.json")
                    require(ended.get("plan_sha256") == sha and ended.get("chunk") == chunk
                            and ended.get("status") in {"accepted_raw_child", "failed_raw_child"},
                            "adaptive_campaign_returned_binding_differs")
                    raw = attempt / "raw"
                    files = legacy.tree_inventory(raw, cap=legacy.CHILD_CAPS["max_output_bytes"])
                    require(load_json(attempt / "INVENTORY.json") == files
                            and digest(json_bytes(files)) == ended["raw_inventory_sha256"], "adaptive_campaign_returned_inventory_changed")
                    usage = legacy.child_usage(raw)
                    require(usage == ended["usage"], "adaptive_campaign_returned_usage_changed")
                    counts["rpc_calls"] += usage["rpc_calls"]
                    counts["response_bytes"] += usage["response_bytes"]
                    closed_bytes += legacy.tree_bytes(attempt)
                    if ended["status"] == "accepted_raw_child":
                        checked = validate_child(raw, expected_contract(plan, binding, source, chunk))
                        require(checked == ended["accepted"] and chunk["chunk"] not in accepted, "adaptive_campaign_duplicate_or_changed_acceptance")
                        accepted[chunk["chunk"]] = ended
            pending = [chunk for chunk in planned if chunk["chunk"] not in accepted]
            first_fresh = next(chunk["chunk"] for chunk in planned if chunk["chunk"] not in REUSED)
            primed = first_fresh in accepted
            deadline()
            while pending or active:
                deadline()
                require(inventory.code_state() == source and legacy.file_digest(COLLECTOR) == COLLECTOR_SHA,
                        "adaptive_campaign_source_changed")
                while pending and not stopping and len(active) < (workers if primed else 1):
                    chunk = pending[0]
                    if not primed and active:
                        break
                    require(attempts[chunk["chunk"]] < 3, "adaptive_campaign_attempt_limit")
                    retained = retained_base_bytes + closed_bytes + legacy.tree_bytes(output / "sessions")
                    retained += legacy.tree_bytes(output / "manifests") + (output / "PLAN.json").stat().st_size
                    legacy.reservations(retained=retained, running=len(active), calls_used=counts["rpc_calls"],
                                        responses_used=counts["response_bytes"], launch=True, caps=CAPS,
                                        disk_free=legacy.shutil.disk_usage(output).free)
                    require(inventory.resource_state().get("ram_bytes", {}).get("MemAvailable", 0)
                            >= (len(active) + 1) * legacy.CHILD_CAPS["max_ram_bytes"], "adaptive_campaign_available_ram_limit")
                    deadline()
                    directory = output / "chunks" / chunk["chunk"]
                    directory.mkdir(mode=0o700, exist_ok=True)
                    attempt = directory / f"a{attempts[chunk['chunk']] + 1:02d}"
                    attempt.mkdir(mode=0o700)
                    v1.publish(attempt / "START.json", json_bytes({"plan_sha256": sha, "chunk": chunk, "started_utc": legacy.now()}))
                    raw = attempt / "raw"
                    deadline()
                    child = launcher(command(plan_path, plan_sha, plan_head, head, raw, chunk, private))
                    active[chunk["chunk"]] = {"child": child, "chunk": chunk, "directory": attempt, "raw": raw,
                                              "expected": expected_contract(plan, binding, source, chunk),
                                              "launched": clock(), "terminating": None}
                    process_proof = identity(child.pid)
                    require(process_proof is not None, "adaptive_campaign_child_birth_missing")
                    v1.publish(attempt / "PROCESS.json", json_bytes(process_proof))
                    attempts[chunk["chunk"]] += 1
                    pending.pop(0)
                for key, running in list(active.items()):
                    child = running["child"]
                    child.pump()
                    if child.poll() is None and (clock() - running["launched"] >= 3600 or child.overflow):
                        stopping = True
                        if running["terminating"] is None:
                            child.terminate()
                            running["terminating"] = clock()
                        elif clock() - running["terminating"] >= 15:
                            child.kill()
                    if child.poll() is None:
                        continue
                    ended = finish(running, sha)
                    counts["rpc_calls"] += ended["usage"]["rpc_calls"]
                    counts["response_bytes"] += ended["usage"]["response_bytes"]
                    closed_bytes += legacy.tree_bytes(running["directory"])
                    if ended["status"] == "accepted_raw_child":
                        accepted[key] = ended
                        if key == first_fresh:
                            primed = True
                            v1.publish(output / "PRIMED.json", json_bytes({"plan_sha256": sha, "chunk": first_fresh,
                                       "manifest_sha256": ended["accepted"]["manifest_sha256"]}))
                    else:
                        stopping = True
                        result["error_code"] = ended["error_code"]
                    del active[key]
                if stopping and not active:
                    break
                if active:
                    sleep(0.2)
            require(not stopping, result.get("error_code", "adaptive_campaign_child_failure"))
            deadline()
            ordered = [accepted[chunk["chunk"]] for chunk in planned if chunk["chunk"] in accepted]
            rows = legacy.verify_parent_coverage(ordered, planned)
            for chunk in planned:
                if chunk["chunk"] in REUSED:
                    continue  # The sealed external hierarchy is reopened below.
                matches = []
                for attempt in (output / "chunks" / chunk["chunk"]).iterdir():
                    ended = load_json(attempt / "EXIT.json")
                    if ended.get("status") == "accepted_raw_child":
                        matches.append((attempt, ended))
                require(len(matches) == 1 and matches[0][1] == accepted[chunk["chunk"]],
                        "adaptive_campaign_final_acceptance_changed")
                attempt, ended = matches[0]
                raw = attempt / "raw"
                files = legacy.tree_inventory(raw, cap=legacy.CHILD_CAPS["max_output_bytes"])
                require(load_json(attempt / "INVENTORY.json") == files
                        and digest(json_bytes(files)) == ended["raw_inventory_sha256"]
                        and legacy.child_usage(raw) == ended["usage"]
                        and validate_child(raw, expected_contract(plan, binding, source, chunk)) == ended["accepted"],
                        "adaptive_campaign_final_child_artifact_changed")
                deadline()
            require(load_reuse(reuse_campaign, reuse_plan_file_sha256, reuse_source_head,
                               proof["old_plan"], proof["old_binding"], identity) == baseline,
                    "adaptive_campaign_reuse_changed")
            deadline()
            require(validate_reboot_bridge(reboot_bridge, reboot_bridge_sha256, plan_path, plan_sha, plan_head) == proof,
                    "adaptive_reboot_bridge_or_admission_tree_changed")
            deadline()
            require(pilot.read_plan(plan_path, plan_sha, plan_head) == (plan, binding, original)
                    and inventory.code_state() == source, "adaptive_campaign_final_source_changed")
            require(retained_base_bytes + legacy.tree_bytes(output) + legacy.PARENT_RESERVE <= CAPS["output_bytes"]
                    and counts["rpc_calls"] <= CAPS["rpc_calls"] and counts["response_bytes"] <= CAPS["response_bytes"],
                    "adaptive_campaign_final_budget_exceeded")
            manifest = {"schema_version": "polymarket_native_campaign_manifest_v2", "status": "raw_campaign_complete",
                        "run_id": owner["run_id"], "plan_sha256": sha, "accepted_chunks": ordered, "rows": rows,
                        "rpc_calls_charged": counts["rpc_calls"], "response_bytes_charged": counts["response_bytes"],
                        "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True}
            name = uuid.uuid4().hex + ".json"
            raw = json_bytes(manifest)
            v1.publish(output / "manifests" / name, raw)
            deadline()
            v1.publish(output / "COMPLETE.json", json_bytes({"status": "raw_campaign_complete_not_analysis_admission",
                       "run_id": owner["run_id"], "plan_sha256": sha, "manifest_path": "manifests/" + name,
                       "manifest_sha256": digest(raw)}))
            result.update(status="raw_campaign_complete", collection_complete=True, rows=rows)
        except BaseException as exc:
            result["error_code"] = str(exc) if isinstance(exc, pilot.PilotBlocked) else "adaptive_campaign_control_or_interruption_failure"
            for running in active.values():
                if running["child"].poll() is None:
                    running["child"].terminate()
            grace = clock() + 15
            while any(running["child"].poll() is None for running in active.values()):
                for running in active.values():
                    running["child"].pump()
                    if clock() >= grace and running["child"].poll() is None:
                        running["child"].kill()
                sleep(0.1)
            for running in active.values():
                if not (running["directory"] / "EXIT.json").exists():
                    try:
                        ended = finish(running, sha, "adaptive_campaign_stopped_before_acceptance")
                        counts["rpc_calls"] += ended["usage"]["rpc_calls"]
                        counts["response_bytes"] += ended["usage"]["response_bytes"]
                    except Exception:
                        counts["rpc_calls"] += 1200
                        counts["response_bytes"] += 4 * 1024**3
        finally:
            result.update(accepted_chunks=len(accepted), rpc_calls_charged=counts["rpc_calls"],
                          response_bytes_charged=counts["response_bytes"], session_elapsed_seconds=max(0, clock() - started),
                          aggregate_bytes=retained_base_bytes + legacy.tree_bytes(output), children_still_running=False)
            v1.publish(session / "RESULT.json", json_bytes(result))
            if result["status"] != "raw_campaign_complete":
                v1.publish(session / "FAILED.json", json_bytes({"error_code": result.get("error_code")}))
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-plan", "output-dir", "reuse-campaign", "reboot-bridge", "rpc-private-file"):
        parser.add_argument("--" + name, required=name != "rpc-private-file", type=Path)
    for name in ("source-plan-sha256", "plan-source-head", "expected-head", "reuse-plan-file-sha256", "reuse-source-head",
                 "reboot-bridge-sha256"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
        require(args.output_dir.is_absolute() and Path("/mnt/data/runs") in args.output_dir.resolve().parents,
                "adaptive_campaign_output_outside_run_root")
        for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[key] = "1"
        result = run_campaign(args.source_plan, args.source_plan_sha256, args.plan_source_head, args.expected_head, args.output_dir,
                              reuse_campaign=args.reuse_campaign, reuse_plan_file_sha256=args.reuse_plan_file_sha256,
                              reuse_source_head=args.reuse_source_head, workers=args.workers, resume=args.resume,
                              reboot_bridge=args.reboot_bridge, reboot_bridge_sha256=args.reboot_bridge_sha256,
                              rpc_private_file=args.rpc_private_file)
    except Exception:
        parser.error("Adaptive campaign source/production admission failed; no completion published.")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "raw_campaign_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
