"""Append-only adaptive RAW collection. V1 remains immutable and authoritative.

Only a durably proved 16 MiB getLogs overflow permits binary subdivision of a
50-block root. Singleton overflow, malformed payloads and every other admission
failure remain fatal. Only successful leaves cover blocks; failed parents never
contribute observations. No ABI, economic or exact-interior-clock claim is made.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import resource
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import collect_polymarket_native_increment as v1

pilot, inventory = v1.pilot, v1.inventory
require, json_bytes, digest, load_json = v1.require, v1.json_bytes, v1.digest, v1.load_json
V1_SHA256 = "0f1f0c1dd0f8f6a7f7dd3daee832ebf93f03210df7fe8f7b8ad7d736bba2d14f"
BASE_BLOCKS = 50


def window(first: int, last: int) -> dict:
    require(type(first) is int and type(last) is int and 0 < first <= last, "invalid_adaptive_interval")
    return {"window": f"b{first:012d}-{last:012d}", "first_block": first, "last_block": last,
            "blocks": last - first + 1}


def children(parent: dict) -> list[dict]:
    require(parent == window(parent["first_block"], parent["last_block"]) and parent["blocks"] > 1,
            "singleton_or_invalid_split")
    middle = (parent["first_block"] + parent["last_block"]) // 2
    return [window(parent["first_block"], middle), window(middle + 1, parent["last_block"])]


def reference(output: Path, path: Path) -> dict:
    raw = path.read_bytes()
    return {"path": str(path.relative_to(output)), "bytes": len(raw), "sha256": digest(raw)}


def verify_ref(output: Path, ref: dict, expected_path: Path) -> dict:
    require(ref.get("path") == str(expected_path.relative_to(output)) and expected_path.is_file()
            and not expected_path.is_symlink() and expected_path.stat().st_size == ref.get("bytes") <= v1.MAX_CONTROL,
            "adaptive_control_reference_invalid")
    require(digest(expected_path.read_bytes()) == ref.get("sha256"), "adaptive_control_hash_differs")
    return load_json(expected_path)


def prove_overflow(output: Path, parent: dict, attempt: int, contract_sha: str, addresses: tuple,
                   topics: list, rpc_starts: dict) -> dict:
    directory = output / "nodes" / parent["window"] / f"a{attempt}"
    require(load_json(directory / "START.json") == {"contract_sha256": contract_sha, "window": parent, "attempt": attempt},
            "split_attempt_binding_differs")
    require(load_json(directory / "FAILURE.json").get("error_code") == "rpc_response_byte_limit",
            "split_without_proven_response_overflow")
    requests = load_json(directory / "REQUESTS.json").get("calls")
    require(type(requests) is list and len(requests) == 3
            and [item.get("method") for item in requests] ==
            ["eth_getBlockByNumber", "eth_getBlockByNumber", "eth_getLogs"], "split_request_ledger_invalid")
    expected = [[hex(parent["first_block"]), False], [hex(parent["last_block"]), False],
                [{"fromBlock": hex(parent["first_block"]), "toBlock": hex(parent["last_block"]),
                  "address": list(addresses), "topics": [topics]}]]
    require([item.get("params") for item in requests] == expected
            and [item.get("status") for item in requests] == ["validated", "validated", "failed"],
            "split_request_predicate_or_status_differs")
    for call in requests:
        start = rpc_starts.get(call["id"])
        require(start is not None and start["value"].get("method") == call["method"]
                and start["value"].get("params") == call["params"], "split_rpc_start_binding_differs")
        finish = load_json(start["path"].with_name(start["path"].name.replace(".START.json", ".FINISH.json")))
        require(finish.get("id") == call["id"] and finish.get("params") == call["params"]
                and finish.get("method") == call["method"] and finish.get("response_bytes") == call.get("response_bytes")
                and finish.get("response_sha256") == call.get("response_sha256"), "split_rpc_finish_binding_differs")
        if call["method"] == "eth_getLogs":
            require(finish.get("status") == "failed" and finish.get("response_bytes") == pilot.MAX_RESPONSE_BYTES + 1
                    and finish.get("response_size_is_conservative_bound") is not True
                    and type(finish.get("response_sha256")) is str and len(finish["response_sha256"]) == 64,
                    "split_overflow_not_observed_at_bounded_read_limit")
    return {"schema_version": "polymarket_native_split_v2", "contract_sha256": contract_sha,
            "window": parent, "attempt": attempt, "request_ledger": reference(output, directory / "REQUESTS.json"),
            "children": children(parent), "reason": "proven_getlogs_response_byte_limit"}


def verify_tree(roots: list[dict], splits: list[dict], leaves: list[dict]) -> None:
    split_map, leaf_map = {}, {}
    for record in splits:
        key = record["window"]["window"]
        require(key not in split_map, "duplicate_split_node")
        split_map[key] = record
    for record in leaves:
        key = record["window"]["window"]
        require(key not in leaf_map and key not in split_map and record.get("status") == "raw_shard_validated",
                "duplicate_or_split_success_node")
        leaf_map[key] = record
    reached = set()
    def visit(node):
        key = node["window"]
        require(key not in reached, "adaptive_duplicate_reachable_node")
        reached.add(key)
        if key in split_map:
            require(split_map[key]["window"] == node and split_map[key]["children"] == children(node),
                    "adaptive_children_not_canonical_partition")
            for child in children(node):
                visit(child)
        else:
            require(key in leaf_map and leaf_map[key]["window"] == node, "adaptive_missing_leaf")
    for root in roots:
        visit(root)
    require(reached == set(split_map) | set(leaf_map), "adaptive_unreachable_nodes")
    ordered = sorted(leaves, key=lambda item: item["window"]["first_block"])
    require(ordered and ordered[0]["window"]["first_block"] == roots[0]["first_block"]
            and ordered[-1]["window"]["last_block"] == roots[-1]["last_block"]
            and all(left["window"]["last_block"] + 1 == right["window"]["first_block"]
                    for left, right in zip(ordered, ordered[1:])), "adaptive_leaf_gap_or_overlap")
    for leaf in leaves:
        require(leaf["contract_intervals"] == v1.interval_receipts(leaf["window"], leaf["evidence"], pilot.ADDRESSES),
                "adaptive_contract_zero_or_count_receipt_differs")


def restore_sessions(output: Path, contract_sha: str) -> tuple[float, dict, int]:
    elapsed, starts, responses = 0.0, {}, 0
    for session in (output / "sessions").iterdir():
        require(session.is_dir() and not session.is_symlink(), "adaptive_session_directory_invalid")
        started = load_json(session / "START.json")
        require(started.get("contract_sha256") == contract_sha, "adaptive_session_contract_differs")
        if (session / "RESULT.json").exists():
            duration = load_json(session / "RESULT.json").get("session_elapsed_seconds")
            require(type(duration) in (int, float) and duration >= 0, "adaptive_session_duration_invalid")
            elapsed += duration
        else:
            elapsed += max(0, (datetime.now(timezone.utc) - datetime.fromisoformat(started["started_utc"])).total_seconds())
        require((session / "rpc").is_dir() and not (session / "rpc").is_symlink(), "adaptive_rpc_directory_invalid")
        require(all(re.fullmatch(r"r[0-9]{9}\.(START|FINISH)\.json", path.name)
                    or (path.name.startswith(".") and path.name.endswith(".pending"))
                    for path in (session / "rpc").iterdir()), "adaptive_rpc_filenames_invalid")
        for path in (session / "rpc").glob("*.START.json"):
            value = load_json(path)
            identifier = value.get("id")
            require(type(identifier) is int and identifier > 0 and identifier not in starts
                    and path.name == f"r{identifier:09d}.START.json" and value.get("method") in
                    {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}, "adaptive_rpc_identity_invalid")
            starts[identifier] = {"value": value, "path": path}
            finish = path.with_name(path.name.replace(".START.json", ".FINISH.json"))
            if finish.exists():
                ended = load_json(finish)
                size = ended.get("response_bytes")
                require(ended.get("id") == identifier and ended.get("method") == value["method"]
                        and ended.get("params") == value.get("params") and type(size) is int
                        and 0 <= size <= pilot.MAX_RESPONSE_BYTES + 1,
                        "adaptive_rpc_response_receipt_invalid")
                responses += size
            else:
                responses += pilot.MAX_RESPONSE_BYTES + 1
    require(set(starts) == set(range(1, len(starts) + 1)), "adaptive_rpc_identity_gap")
    return elapsed, starts, responses


def validate_artifacts(output: Path, contract: dict, roots: list, splits: list, leaves: list) -> None:
    """Terminal reopening is independent of the controller's later acceptance."""
    sha = digest(json_bytes(contract))
    _, starts, _ = restore_sessions(output, sha)
    verify_tree(roots, splits, leaves)
    for leaf in leaves:
        node, attempt = leaf["window"], leaf["attempt"]
        target = output / "nodes" / node["window"] / f"a{attempt}"
        require(load_json(target / "SUCCESS.json") == leaf and leaf.get("contract_sha256") == sha,
                "adaptive_final_success_differs")
        v1.verify_success_ledger(output, target, leaf, node, tuple(contract["addresses"]), contract["topics"], starts)
        require(not (target / "payload.parquet").is_symlink()
                and reference(output, target / "payload.parquet") == leaf["payload"], "adaptive_final_payload_hash_differs")
    for split in splits:
        proved = prove_overflow(output, split["window"], split["attempt"], sha,
                               tuple(contract["addresses"]), contract["topics"], starts)
        path = output / "nodes" / split["window"]["window"] / "SPLIT.json"
        require(load_json(path) == split == proved, "adaptive_final_split_proof_differs")


def run_collection(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, output: Path, *,
                   first_block: int, last_block: int, max_rpc_calls: int, max_output_bytes: int,
                   max_runtime_seconds: int, max_rows: int, max_ram_bytes: int, max_total_response_bytes: int,
                   resume: bool = False, rpc_private_file: Path | None = None, transport=pilot.http_response,
                   clock=time.monotonic) -> dict:
    require(digest(Path(v1.__file__).read_bytes()) == V1_SHA256, "adaptive_v1_helper_hash_differs")
    caps = v1.limits(max_rpc_calls, max_output_bytes, max_runtime_seconds, max_rows, max_ram_bytes, max_total_response_bytes)
    plan, binding, original = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
    addresses = v1.requested_addresses("append", first_block, last_block, plan)
    require(last_block - first_block + 1 <= 10_000, "adaptive_chunk_exceeds_10000_blocks")
    roots = v1.windows(first_block, last_block, BASE_BLOCKS)
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    v1.check_destination(output, original, plan_path, plan, private)
    source = inventory.code_state()
    require(source.get("head") == expected_head and source.get("status") == "inspected"
            and source.get("tracked_worktree_clean") is True, "adaptive_source_head_or_clean_state_differs")
    contract = {"schema_version": "polymarket_native_collection_contract_v2", "source_plan": binding, "source": source,
                "mode": "append", "first_block": first_block, "last_block": last_block, "addresses": list(addresses),
                "topics": sorted(set(pilot.EXPECTED_TOPICS.values())), "limits": caps,
                "adaptive_policy": {"base_blocks": 50, "minimum_blocks": 1, "split": "binary_on_proven_getlogs_byte_limit_only"},
                "v1_helper_sha256": V1_SHA256, "end_exclusive_utc": plan["end_exclusive_utc"],
                "raw_payload_only": True, "analysis_admitted": False, "interior_exact_timestamps_collected": False,
                "provider_block_timestamp_used_as_clock": False}
    sha = digest(json_bytes(contract))
    require(v1.shutil.disk_usage(output.parent).free >= v1.FREE_FLOOR + caps["output_bytes"],
            "adaptive_insufficient_free_space_for_stage_budget")
    if resume:
        require(output.is_dir() and not output.is_symlink(), "adaptive_resume_directory_missing")
    else:
        output.mkdir(mode=0o700)
    with v1.run_lock(output):
        if resume:
            owner = load_json(output / "OWNER.json")
            require(owner.get("contract") == contract and owner.get("contract_sha256") == sha, "adaptive_resume_contract_differs")
            require(not (output / "COMPLETE.json").exists(), "adaptive_run_already_complete")
        else:
            owner = {"run_id": str(uuid.uuid4()), "created_utc": datetime.now(timezone.utc).isoformat(),
                     "contract": contract, "contract_sha256": sha}
            v1.publish(output / "OWNER.json", json_bytes(owner))
            (output / "nodes").mkdir(mode=0o700)
            (output / "sessions").mkdir(mode=0o700)
        require((output / "nodes").is_dir() and not (output / "nodes").is_symlink()
                and (output / "sessions").is_dir() and not (output / "sessions").is_symlink(), "adaptive_topology_invalid")
        require(all(path.name in {"OWNER.json", "LOCK", "nodes", "sessions", "manifest.json"}
                    or (path.name.startswith(".") and path.name.endswith(".pending")) for path in output.iterdir()),
                "adaptive_unexpected_root_entries")
        elapsed, starts, responses = restore_sessions(output, sha)
        budget = v1.Budget(output, caps, v1.account_existing(output, caps["output_bytes"]), clock, elapsed)
        budget.calls, budget.response_bytes = len(starts), responses
        session = output / "sessions" / uuid.uuid4().hex
        session.mkdir(mode=0o700)
        (session / "rpc").mkdir(mode=0o700)
        budget.write(session / "START.json", json_bytes({"contract_sha256": sha, "started_utc": datetime.now(timezone.utc).isoformat()}))
        result = {"schema_version": "polymarket_native_collection_session_v2", "status": "failed_raw_collection",
                  "run_id": owner["run_id"], "contract_sha256": sha, "collection_complete": False, "analysis_admitted": False}
        states, splits, leaves, keys, transactions = {}, [], [], {}, {}
        state_bytes = 0
        rpc = None
        def inspect(node):
            nonlocal state_bytes
            name = node["window"]
            directory = output / "nodes" / name
            states[name] = {"node": node, "saved": None, "split": None, "overflow": None, "attempts": 0}
            if not directory.exists():
                return
            require(directory.is_dir() and not directory.is_symlink()
                    and all(path.name in {"a1", "a2", "a3", "SPLIT.json"}
                            or (path.name.startswith(".") and path.name.endswith(".pending")) for path in directory.iterdir()),
                    "adaptive_node_topology_invalid")
            successes, oversizes = [], []
            for attempt in range(1, 4):
                target = directory / f"a{attempt}"
                if not target.exists():
                    continue
                states[name]["attempts"] = attempt
                require(target.is_dir() and not target.is_symlink(), "adaptive_attempt_not_directory")
                require(all(path.name in {"START.json", "REQUESTS.json", "FAILURE.json", "payload.parquet", "SUCCESS.json"}
                            or (path.name.startswith(".") and path.name.endswith(".pending")) for path in target.iterdir()),
                        "adaptive_attempt_files_invalid")
                if not (target / "START.json").exists():
                    require(all(path.name.startswith(".") and path.name.endswith(".pending") for path in target.iterdir()),
                            "adaptive_unbound_attempt_outputs")
                    continue
                require(load_json(target / "START.json") == {"contract_sha256": sha, "window": node, "attempt": attempt},
                        "adaptive_attempt_binding_differs")
                if (target / "SUCCESS.json").exists():
                    receipt = load_json(target / "SUCCESS.json")
                    require(receipt.get("window") == node and receipt.get("contract_sha256") == sha
                            and receipt.get("attempt") == attempt
                            and receipt.get("payload", {}).get("path") == str((target / "payload.parquet").relative_to(output)),
                            "adaptive_saved_leaf_binding_differs")
                    v1.verify_success_ledger(output, target, receipt, node, addresses, contract["topics"], starts)
                    local_keys, local_tx = {}, {}
                    rows, evidence = v1.native_rows(target / "payload.parquet", node, receipt, local_keys, local_tx, addresses)
                    require(evidence == receipt["evidence"], "adaptive_saved_leaf_evidence_differs")
                    state_bytes = v1.merge_identity_state(keys, transactions, local_keys, local_tx, state_bytes, caps, commit=True)
                    budget.rows += len(rows)
                    successes.append(receipt)
                if (target / "FAILURE.json").exists() and load_json(target / "FAILURE.json").get("error_code") == "rpc_response_byte_limit":
                    oversizes.append(prove_overflow(output, node, attempt, sha, addresses, contract["topics"], starts))
            require(len(successes) <= 1 and len(oversizes) <= 1 and not (successes and oversizes), "adaptive_conflicting_node_terminals")
            if successes:
                require(not (directory / "SPLIT.json").exists(), "adaptive_node_both_split_and_success")
                states[name]["saved"] = successes[0]
            if oversizes:
                states[name]["overflow"] = oversizes[0]
            if (directory / "SPLIT.json").exists():
                split = load_json(directory / "SPLIT.json")
                require(oversizes and split == oversizes[0], "adaptive_split_proof_differs")
                states[name]["split"] = split
                for child in children(node):
                    inspect(child)
        try:
            for root in roots:
                inspect(root)
            require({path.name for path in (output / "nodes").iterdir()} <= set(states), "adaptive_unreachable_node_directory")
            budget.check()
            rpc = v1.CollectionRPC(private, transport, budget, session)
            rpc.chain()
            for item in ("last_included_header", "first_excluded_header"):
                require(rpc.header(plan[item]["number"]) == plan[item], "adaptive_cutoff_header_drift")
            def process(node):
                nonlocal state_bytes
                name = node["window"]
                if name not in states:
                    inspect(node)
                state = states[name]
                directory = output / "nodes" / name
                directory.mkdir(mode=0o700, exist_ok=True)
                if state["saved"] is not None:
                    for field in ("first_header", "last_header"):
                        header = state["saved"][field]
                        require(rpc.header(header["number"]) == header, "adaptive_saved_header_drift")
                    leaves.append(state["saved"])
                    return
                if state["overflow"] is not None:
                    if state["split"] is None:
                        budget.write(directory / "SPLIT.json", json_bytes(state["overflow"]))
                    splits.append(state["overflow"])
                    for child in children(node):
                        process(child)
                    return
                for attempt in range(state["attempts"] + 1, 4):
                    target = directory / f"a{attempt}"
                    target.mkdir(mode=0o700)
                    budget.write(target / "START.json", json_bytes({"contract_sha256": sha, "window": node, "attempt": attempt}))
                    ledger_start = len(rpc.ledger)
                    try:
                        first, last = (rpc.header(node[key]) for key in ("first_block", "last_block"))
                        query = {"fromBlock": hex(node["first_block"]), "toBlock": hex(node["last_block"]),
                                 "address": list(addresses), "topics": [contract["topics"]]}
                        logs, request = rpc.call("eth_getLogs", [query])
                        local_keys, local_tx = {}, {}
                        rows, evidence = pilot.validate_logs(logs, node, {first["number"]: first, last["number"]: last}, request["id"],
                                                             shared_keys=local_keys, shared_transactions=local_tx)
                        require(all(row["exchange_address"] in addresses for row in rows)
                                and budget.rows + len(rows) <= caps["rows"], "adaptive_roster_or_row_limit")
                        next_bytes = v1.merge_identity_state(keys, transactions, local_keys, local_tx, state_bytes, caps, commit=False)
                        rpc.header(last["number"], reopen=True)
                        payload_ref = budget.write(target / "payload.parquet", v1.parquet_payload(rows))
                        request.update(status="validated", rows=len(rows), payload_sha256=payload_ref["sha256"])
                        ledger_ref = budget.write(target / "REQUESTS.json", json_bytes({"calls": rpc.ledger[ledger_start:]}))
                        receipt = {"status": "raw_shard_validated", "contract_sha256": sha, "window": node, "attempt": attempt,
                                   "first_header": first, "last_header": last, "log_request_id": request["id"],
                                   "payload": payload_ref, "request_ledger": ledger_ref, "evidence": evidence,
                                   "contract_intervals": v1.interval_receipts(node, evidence, addresses)}
                        budget.write(target / "SUCCESS.json", json_bytes(receipt))
                        v1.merge_identity_state(keys, transactions, local_keys, local_tx, state_bytes, caps, commit=True)
                        state_bytes = next_bytes
                        budget.rows += len(rows)
                        leaves.append(receipt)
                        return
                    except Exception as exc:
                        code = str(exc) if isinstance(exc, pilot.PilotBlocked) else "adaptive_transport_or_checkpoint_failure"
                        if not (target / "REQUESTS.json").exists():
                            budget.write(target / "REQUESTS.json", json_bytes({"calls": rpc.ledger[ledger_start:]}), reserve=False)
                        budget.write(target / "FAILURE.json", json_bytes({"error_code": code}), reserve=False)
                        for entry in rpc.ledger[ledger_start:]:
                            starts[entry["id"]] = {"value": load_json(session / "rpc" / f"r{entry['id']:09d}.START.json"),
                                                   "path": session / "rpc" / f"r{entry['id']:09d}.START.json"}
                        if code == "rpc_response_byte_limit":
                            split = prove_overflow(output, node, attempt, sha, addresses, contract["topics"], starts)
                            budget.write(directory / "SPLIT.json", json_bytes(split))
                            splits.append(split)
                            for child in children(node):
                                process(child)
                            return
                        if not isinstance(exc, v1.TransportRetry):
                            raise
                raise pilot.PilotBlocked("adaptive_attempt_limit")
            for root in roots:
                process(root)
            validate_artifacts(output, contract, roots, splits, leaves)
            reopened = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
            require(reopened == (plan, binding, original) and inventory.code_state() == source
                    and digest(Path(v1.__file__).read_bytes()) == V1_SHA256, "adaptive_final_source_binding_changed")
            budget.check()
            manifest = {"schema_version": "polymarket_native_collection_manifest_v2", "status": "raw_collection_complete",
                        "run_id": owner["run_id"], "contract": contract, "contract_sha256": sha, "roots": roots,
                        "splits": splits, "windows": sorted(leaves, key=lambda item: item["window"]["first_block"]),
                        "rows": budget.rows, "distinct_native_keys": len(keys), "collection_complete": True,
                        "raw_payload_only": True, "analysis_admitted": False}
            raw = json_bytes(manifest)
            budget.write(output / "manifest.json", raw)
            budget.check()
            budget.write(output / "COMPLETE.json", json_bytes({"status": "raw_collection_complete_not_analysis_admission",
                         "run_id": owner["run_id"], "contract_sha256": sha, "manifest_sha256": digest(raw)}))
            result.update(status="raw_collection_complete", collection_complete=True)
        except Exception as exc:
            result["error_code"] = str(exc) if isinstance(exc, pilot.PilotBlocked) else "adaptive_transport_or_checkpoint_failure"
        finally:
            result.update(rows=budget.rows, validated_windows=len(leaves), split_nodes=len(splits), rpc_calls_charged=budget.calls,
                          response_bytes_charged=budget.response_bytes, session_elapsed_seconds=max(0, clock() - budget.start))
            raw = json_bytes(result)
            if budget.used + len(raw) <= caps["output_bytes"]:
                v1.publish(session / "RESULT.json", raw)
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-plan", "output-dir", "rpc-private-file"):
        parser.add_argument("--" + name, type=Path, required=name != "rpc-private-file")
    for name in ("source-plan-sha256", "plan-source-head", "expected-head"):
        parser.add_argument("--" + name, required=True)
    for name in ("first-block", "last-block", "max-rpc-calls", "max-output-bytes", "max-runtime-seconds", "max-rows",
                 "max-ram-bytes", "max-total-response-bytes"):
        parser.add_argument("--" + name, required=True, type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
        require(args.output_dir.is_absolute() and Path("/mnt/data/runs") in args.output_dir.resolve().parents,
                "adaptive_output_outside_run_root")
        for key in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[key] = "1"
        require(inventory.resource_state()["ram_bytes"]["MemAvailable"] >= args.max_ram_bytes, "adaptive_insufficient_available_ram")
        resource.setrlimit(resource.RLIMIT_AS, (args.max_ram_bytes, args.max_ram_bytes))
        result = run_collection(args.source_plan, args.source_plan_sha256, args.plan_source_head, args.expected_head,
                                args.output_dir, first_block=args.first_block, last_block=args.last_block,
                                max_rpc_calls=args.max_rpc_calls, max_output_bytes=args.max_output_bytes,
                                max_runtime_seconds=args.max_runtime_seconds, max_rows=args.max_rows,
                                max_ram_bytes=args.max_ram_bytes, max_total_response_bytes=args.max_total_response_bytes,
                                resume=args.resume, rpc_private_file=args.rpc_private_file)
    except Exception:
        parser.error("Adaptive source/destination/production admission failed; no completion published.")
    import json
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "raw_collection_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
