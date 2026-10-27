"""Fixed, observation-only V3 runtimes and dependency getters on Polygon.

No trade logs, off-chain HTTP, economic decoding, token/outcome mapping, CBOR
decoding or source-equivalence admission. Getter reverts/empty returns are
explicit unknowns. Controls and atomic publication reuse reviewed history v2.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
import re
import resource
import signal
import subprocess
import sys
import time
from typing import Callable
import json

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import probe_polymarket_v3_history_v2 as shared

inventory, planner, activity, prior = shared.inventory, shared.planner, shared.activity, shared.prior
HistoryBlocked, require, encode, digest, now = shared.HistoryBlocked, shared.require, shared.encode, shared.digest, shared.now
Controls, HTTPResponse, http_response = shared.Controls, shared.HTTPResponse, shared.http_response
ADDRESS = shared.ADDRESS
MAX_RPC_CALLS, MAX_RESPONSE_BYTES, MAX_TOTAL_RESPONSE_BYTES = 48, 256 * 1024, 2 * 1024**2
MAX_OUTPUT_BYTES, MAX_RUNTIME_SECONDS = 1024**2, 480
MAX_RAM_BYTES, FREE_FLOOR_BYTES = 8 * 1024**3, 20_000_000_000
BLOCKS = (prior.RAW_TAIL_BLOCK, prior.UPGRADE_BLOCK - 1, prior.UPGRADE_BLOCK, prior.CUTOFF_BLOCK)
FROZEN_UPGRADE_HEADERS = {
    prior.UPGRADE_BLOCK - 1: {"hash": "0x89455597b0a7f93107b594a7b1738a24cf8986ba8fd1185e54349f574b3df404", "timestamp": 1788883530},
    prior.UPGRADE_BLOCK: {"hash": "0x1aa733aadf9f051793fc4ab5245fbfd1bc5adba4ba50593afd2ba6b1911844d1", "timestamp": 1788883531},
}
HEADER_REOPEN_RESERVE = len(BLOCKS)
GETTER_GAS = 200_000
PRIOR_EVIDENCE_SHA256 = "3a22182f99d70a9c0d05f3b915f77aed2fc33aa5491035fa921c9ca3e22e08b8"
OBSERVED_RUNTIME = {
    prior.OLD_IMPLEMENTATION: {"bytes": 22_204, "sha256": "dda5fcc0be82f96ae766bf269b5c8f87871cf458fac18423b265ac57123901fb"},
    prior.NEW_IMPLEMENTATION: {"bytes": 22_256, "sha256": "2dfc1d2e867a7fa851cb195cc7887b614442f4f2de04d3081155a99bd502c6d6"},
}
# Ethereum Keccak-256(signature)[:4], computed and independently fixture-checked.
# These are not hashlib.sha3_256. Signatures come from the pinned first-party
# Exchange.sol, PositionManager.sol and CollateralToken.sol sources.
SELECTORS = {
    "POSITION_MANAGER()": "0x1bea83fe", "COLLATERAL_TOKEN()": "0xf5f1f1a7",
    "COMBINATORIAL_MODULE()": "0x5d85fba8", "FEE_RECEIVER()": "0xd3e78e4d",
    "MAX_FEE_RATE()": "0x92f6576e", "decimals()": "0x313ce567", "moduleById(uint256)": "0x4d51cdf1",
}
EXCHANGE_GETTERS = tuple(list(SELECTORS)[:5])
FIRST_PARTY_SOURCE = {
    "repository": "https://github.com/Polymarket/polymarket-v2-external",
    "commit": "741f8bbe88c3f29a1c55d1248a3dd411f622f111",
    "paths": ["src/exchange/Exchange.sol", "src/positionManager/PositionManager.sol", "src/collateral/CollateralToken.sol"],
    "association_only_not_runtime_equivalence": True,
}


class GetterReverted(HistoryBlocked):
    """An explicitly recognized execution revert, not any generic RPC error."""


def limits() -> dict:
    return {"rpc_attempts": MAX_RPC_CALLS, "wire_response_bytes_per_attempt_including_detector": MAX_RESPONSE_BYTES,
            "total_charged_response_bytes": MAX_TOTAL_RESPONSE_BYTES, "all_controls_staging_output_bytes": MAX_OUTPUT_BYTES,
            "runtime_seconds": MAX_RUNTIME_SECONDS, "address_space_bytes": MAX_RAM_BYTES,
            "minimum_available_ram_bytes": MAX_RAM_BYTES, "untouched_free_disk_bytes": FREE_FLOOR_BYTES,
            "workers": 1, "spill_bytes": 0, "eth_call_gas_ceiling": GETTER_GAS,
            "reserved_final_header_requests": HEADER_REOPEN_RESERVE}


def source_snapshot(expected_head: str) -> dict:
    snapshot = shared.source_snapshot(expected_head)
    name = "scripts/probe_polymarket_v3_runtime_v1.py"
    path = REPO / name
    before = inventory.path_identity(path)
    require(before.get("status") == "present" and not before.get("is_symlink")
            and path.is_file() and 0 < path.stat().st_size <= 1024**2, "runtime_source_not_bounded_regular_file")
    saved = subprocess.run(["git", "show", f"{expected_head}:{name}"], cwd=REPO, capture_output=True, timeout=10)
    require(saved.returncode == 0 and saved.stdout == path.read_bytes(), "runtime_source_uncommitted_or_changed")
    require(before == inventory.path_identity(path), "runtime_source_changed_during_read")
    snapshot["files"][name] = {"sha256": digest(saved.stdout), "identity": before}
    state = inventory.code_state()
    require(state.get("status") == "inspected" and state.get("head") == expected_head
            and state.get("tracked_worktree_clean") is True, "runtime_source_changed_during_snapshot")
    return snapshot


@contextmanager
def runtime_limits():
    previous_as = resource.getrlimit(resource.RLIMIT_AS)
    previous_handler, previous_timer = signal.getsignal(signal.SIGALRM), signal.getitimer(signal.ITIMER_REAL)
    require(previous_timer == (0.0, 0.0), "preexisting_alarm_not_admitted")
    soft = min(MAX_RAM_BYTES, previous_as[1]) if previous_as[1] != resource.RLIM_INFINITY else MAX_RAM_BYTES
    require(soft > 0, "address_space_limit_unavailable")
    started, changed = time.monotonic(), False
    try:
        resource.setrlimit(resource.RLIMIT_AS, (soft, previous_as[1]))
        changed = True
        effective = resource.getrlimit(resource.RLIMIT_AS)
        require(0 < effective[0] <= MAX_RAM_BYTES, "address_space_limit_not_effective")
        def expired(_signum, _frame):
            raise HistoryBlocked("runtime_deadline_exceeded")
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, MAX_RUNTIME_SECONDS)
        yield {"started_monotonic": started, "address_space_soft_bytes": effective[0],
               "wall_alarm_seconds": MAX_RUNTIME_SECONDS}
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if changed:
            resource.setrlimit(resource.RLIMIT_AS, previous_as)


def is_execution_revert(error: dict) -> bool:
    message = error.get("message")
    return (type(error.get("code")) is int and error["code"] in {3, -32000}
            and type(message) is str and len(message) <= 8192
            and re.match(r"\Aexecution reverted(?:\Z|:)", message.lower()) is not None)


class RuntimeRPC(shared.HistoryRPC):
    def remaining_time(self) -> float:
        remaining = MAX_RUNTIME_SECONDS - (self.clock() - self.started)
        require(remaining > 0, "runtime_deadline_exceeded")
        return remaining

    def call(self, method: str, params: list, *, getter_revert_unknown: bool = False):
        require(method in {"eth_chainId", "eth_getBlockByNumber", "eth_getStorageAt", "eth_getCode", "eth_call"},
                "rpc_method_not_permitted")
        require(not getter_revert_unknown or method == "eth_call", "getter_revert_mode_not_call")
        remaining_time = self.remaining_time()
        require(len(self.ledger) < MAX_RPC_CALLS, "rpc_attempt_limit")
        available = MAX_TOTAL_RESPONSE_BYTES - self.response_bytes_charged
        require(available >= 2, "total_response_byte_limit")
        reserved = min(MAX_RESPONSE_BYTES, available)
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params,
                 "status": "requested", "wire_byte_reservation": reserved,
                 "maximum_accepted_payload_bytes": reserved - 1, "started_utc": now()}
        self.controls.journal({"event": "start", **entry})
        self.ledger.append(entry)
        self.response_bytes_charged += reserved
        try:
            response = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"],
                "method": method, "params": params}, byte_limit=reserved, timeout=min(30.0, remaining_time))
            require(type(response) is HTTPResponse and type(response.body) is bytes
                    and type(response.status) is int, "transport_response_shape_invalid")
            raw = response.body
            self.response_bytes_received += len(raw)
            require(len(raw) <= reserved, "transport_wire_byte_cap_violated")
            self.response_bytes_charged -= reserved - len(raw)
            entry.update(response_bytes=len(raw), charged_response_bytes=len(raw),
                         response_sha256=digest(raw), http_status=response.status)
            self.remaining_time()
            require(len(raw) < reserved, "rpc_response_size_refused")
            require(response.status == 200, "rpc_http_status_not_admitted")
            value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                parse_constant=lambda _: (_ for _ in ()).throw(HistoryBlocked("rpc_nonfinite_json")))
            require(type(value) is dict and set(value) in ({"jsonrpc", "id", "result"},
                    {"jsonrpc", "id", "error"}), "rpc_reply_shape_invalid")
            require(value["jsonrpc"] == "2.0" and type(value["id"]) is int
                    and value["id"] == entry["id"], "rpc_reply_id_mismatch")
            if "error" in value:
                error = value["error"]
                require(type(error) is dict, "rpc_error_shape_invalid")
                if getter_revert_unknown and is_execution_revert(error):
                    raise GetterReverted("getter_execution_reverted_unknown")
                raise HistoryBlocked("rpc_error_not_admitted")
            entry["status"] = "reply_shape_validated"
            return value["result"], entry
        except GetterReverted as exc:
            entry.update(status="getter_reverted_unknown", error_code=str(exc))
            raise
        except Exception as exc:
            entry.update(status="failed", error_code=shared.safe_error(exc))
            raise
        finally:
            if "charged_response_bytes" not in entry:
                entry["charged_response_bytes"] = reserved
                entry["unobserved_body_charged_at_full_reservation"] = True
            self.controls.journal({"event": "finish", **entry})

    def runtime(self, address: str, block: int, blobs: dict) -> dict:
        require(address in OBSERVED_RUNTIME, "runtime_address_not_in_frozen_roster")
        value, entry = self.call("eth_getCode", [address, hex(block)])
        require(type(value) is str and activity.HEX_BYTES.fullmatch(value), "runtime_hex_invalid")
        raw = bytes.fromhex(value[2:])
        observed = {"bytes": len(raw), "sha256": digest(raw)}
        require(observed == OBSERVED_RUNTIME[address], "runtime_differs_from_prior_observation")
        require(observed["sha256"] not in blobs or blobs[observed["sha256"]] == value.lower(), "runtime_blob_conflict")
        blobs[observed["sha256"]] = value.lower()
        entry["status"] = "runtime_observation_validated"
        return {"implementation_address": address, "block_number": block, "block_hash": self.headers[block]["hash"],
                "runtime": observed, "runtime_blob_key": observed["sha256"],
                "suffix_evidence": suffix_evidence(raw), "source_equivalence_admitted": False,
                "response_sha256": entry["response_sha256"]}

    def getter(self, target: str, block: int, signature: str, *, argument: int | None = None) -> dict:
        require(type(target) is str and shared.ADDRESS_RE.fullmatch(target), "getter_target_address_invalid")
        require(signature in SELECTORS, "getter_signature_not_in_frozen_roster")
        if signature == "moduleById(uint256)":
            require(type(argument) is int and argument in {1, 2, 3}, "getter_argument_not_in_frozen_roster")
        else:
            require(argument is None, "getter_argument_not_admitted")
        data = SELECTORS[signature] + (f"{argument:064x}" if argument is not None else "")
        observation = {"target": target.lower(), "block_number": block, "signature": signature,
                       "selector": SELECTORS[signature], "argument": argument,
                       "status": "unknown", "value": None, "abi_compatibility_admitted": False}
        try:
            value, entry = self.call("eth_call", [{"to": target, "data": data, "gas": hex(GETTER_GAS)}, hex(block)],
                                     getter_revert_unknown=True)
        except GetterReverted:
            observation.update(status="unknown_execution_reverted", request_id=self.ledger[-1]["id"])
            return observation
        observation.update(request_id=entry["id"], response_sha256=entry["response_sha256"])
        require(type(value) is str and activity.HEX_BYTES.fullmatch(value), "getter_result_hex_invalid")
        raw = bytes.fromhex(value[2:])
        if not raw:
            observation["status"] = "unknown_empty_return"
        elif len(raw) != 32:
            observation["status"] = "unknown_abi_return_shape"
        elif signature not in {"MAX_FEE_RATE()", "decimals()"}:
            if raw[:12] != bytes(12):
                observation["status"] = "unknown_noncanonical_address_word"
            else:
                observation.update(status="observed", value="0x" + raw[12:].hex(), abi_type="address")
        elif signature == "decimals()" and int.from_bytes(raw, "big") > 255:
            observation["status"] = "unknown_noncanonical_uint8_word"
        else:
            observation.update(status="observed", value=int.from_bytes(raw, "big"), abi_type="uint256" if
                               signature == "MAX_FEE_RATE()" else "uint8")
        entry["status"] = "getter_observation_recorded"
        return observation


def suffix_evidence(raw: bytes) -> dict:
    length_candidate = int.from_bytes(raw[-2:], "big") if len(raw) >= 2 else None
    within = length_candidate is not None and 0 < length_candidate <= min(512, len(raw) - 2)
    return {"status": "unparsed_runtime_suffix_only", "suffix_hex": "0x" + raw[-514:].hex(),
            "trailing_two_byte_length_candidate": length_candidate,
            "candidate_fits_bounded_suffix": within,
            "candidate_trailer_hex": "0x" + raw[-2 - length_candidate:-2].hex() if within else None,
            "cbor_decoded": False, "metadata_presence_certified": False, "metadata_content_verified": False,
            "cid": None, "cid_content_verified": False,
            "note": "A length-like trailer is not a CBOR/metadata certificate. No decoder or CID-content verification runs."}


def usable_address(item: dict | None) -> str | None:
    if item and item["status"] == "observed" and type(item["value"]) is str and int(item["value"], 16) != 0:
        return item["value"]
    return None


def diagnose(rpc: RuntimeRPC, result: dict) -> None:
    chain, entry = rpc.call("eth_chainId", [])
    require(inventory._quantity(chain) == 137, "rpc_wrong_chain")
    entry["status"] = "chain_validated"
    for block in BLOCKS:
        rpc.header(block)
        if block in FROZEN_UPGRADE_HEADERS:
            require(all(rpc.headers[block][key] == value for key, value in FROZEN_UPGRADE_HEADERS[block].items()),
                    "header_differs_from_prior_upgrade_observation")
    require(rpc.headers[prior.UPGRADE_BLOCK]["parentHash"] == rpc.headers[prior.UPGRADE_BLOCK - 1]["hash"],
            "upgrade_adjacent_header_link_differs")
    for block in BLOCKS:
        observed = rpc.implementation(block)
        expected = prior.OLD_IMPLEMENTATION if block < prior.UPGRADE_BLOCK else prior.NEW_IMPLEMENTATION
        require(observed["implementation_address"] == expected, "frozen_proxy_implementation_slot_differs")
        result["proxy_implementation_slots"].append(observed)
    for implementation, blocks in ((prior.OLD_IMPLEMENTATION, BLOCKS[:2]), (prior.NEW_IMPLEMENTATION, BLOCKS[2:])):
        for block in blocks:
            result["runtime_observations"].append(rpc.runtime(implementation, block, result["runtime_blobs"]))
    eras = (("old", prior.OLD_IMPLEMENTATION, prior.RAW_TAIL_BLOCK),
            ("new", prior.NEW_IMPLEMENTATION, prior.CUTOFF_BLOCK))
    configs = {}
    for era, implementation, block in eras:
        configurations = {}
        for target_kind, target in (("implementation", implementation), ("proxy", ADDRESS)):
            values = {}
            for signature in EXCHANGE_GETTERS:
                observed = rpc.getter(target, block, signature)
                observed.update(era=era, target_kind=target_kind)
                result["getters"].append(observed)
                values[signature] = observed
            configurations[target_kind] = values
        for signature in EXCHANGE_GETTERS:
            direct, proxy = (configurations[kind][signature] for kind in ("implementation", "proxy"))
            both = direct["status"] == proxy["status"] == "observed"
            require(not both or direct["value"] == proxy["value"], "proxy_and_implementation_getter_differs")
            result["configuration_checks"].append({"era": era, "signature": signature,
                "status": "observed_equal_at_snapshot_only" if both else "unknown_missing_getter_observation",
                "abi_or_economic_equivalence_admitted": False})
        configs[era] = configurations["proxy"]
    # Every optional attempt retains room for all four mandatory header reopens.
    def optional(label: str, operation: Callable) -> dict | None:
        if MAX_RPC_CALLS - len(rpc.ledger) <= HEADER_REOPEN_RESERVE:
            result["dependency_checks"].append({"check": label, "status": "not_attempted_call_budget_reserved"})
            return None
        observed = operation()
        result["dependency_checks"].append({"check": label, **observed})
        return observed
    for era, _implementation, block in eras:
        pm = usable_address(configs[era]["POSITION_MANAGER()"])
        collateral = usable_address(configs[era]["COLLATERAL_TOKEN()"])
        if pm:
            pm_collateral = optional(f"{era}_position_manager_collateral", lambda: rpc.getter(pm, block, "COLLATERAL_TOKEN()"))
            if pm_collateral and pm_collateral["status"] == "observed" and collateral:
                require(pm_collateral["value"] == collateral, "position_manager_collateral_differs")
        else:
            result["dependency_checks"].append({"check": f"{era}_position_manager_collateral", "status": "unknown_parent_getter"})
        if collateral:
            optional(f"{era}_collateral_decimals", lambda: rpc.getter(collateral, block, "decimals()"))
        else:
            result["dependency_checks"].append({"check": f"{era}_collateral_decimals", "status": "unknown_parent_getter"})
        for kind, target in (("position_manager", pm), ("collateral", collateral)):
            if target:
                def dependency_slot(target=target, block=block):
                    raw, request = rpc.call("eth_getStorageAt", [target, activity.IMPLEMENTATION_SLOT, hex(block)])
                    require(type(raw) is str and activity.HASH.fullmatch(raw), "dependency_slot_word_invalid")
                    word = bytes.fromhex(raw[2:])
                    require(word[:12] == bytes(12), "dependency_slot_not_address_word")
                    return {"target": target, "block_number": block,
                            "status": "observed_nonzero_slot" if any(word) else "observed_zero_slot_no_proxy_assertion",
                            "implementation_address": "0x" + word[12:].hex() if any(word) else None,
                            "response_sha256": request["response_sha256"], "proxy_or_source_equivalence_admitted": False}
                optional(f"{era}_{kind}_implementation_slot", dependency_slot)
            else:
                result["dependency_checks"].append({"check": f"{era}_{kind}_implementation_slot", "status": "unknown_parent_getter"})
    pm = usable_address(configs["new"]["POSITION_MANAGER()"])
    for module_id in (1, 2, 3):
        if pm:
            optional(f"new_module_{module_id}", lambda module_id=module_id: rpc.getter(pm, prior.CUTOFF_BLOCK,
                     "moduleById(uint256)", argument=module_id))
        else:
            result["dependency_checks"].append({"check": f"new_module_{module_id}", "status": "unknown_parent_getter"})
    for block in BLOCKS:
        rpc.header(block)
    result["headers"] = [rpc.headers[block] for block in BLOCKS]
    for sha, runtime_hex in result["runtime_blobs"].items():
        require(digest(bytes.fromhex(runtime_hex[2:])) == sha, "retained_runtime_blob_differs")


def probe(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, *, run_dir: Path,
          rpc_private_file: Path | None = None, transport: Callable = http_response) -> dict:
    require_production_host()
    with runtime_limits() as enforcement:
        return _probe(plan_path, plan_sha256, plan_source_head, expected_head, run_dir=run_dir,
                      rpc_private_file=rpc_private_file, transport=transport, enforcement=enforcement)


def _probe(plan_path, plan_sha256, plan_source_head, expected_head, *, run_dir, rpc_private_file, transport, enforcement):
    result = {"schema_version": "polymarket_v3_runtime_probe_v1", "status": "blocked", "created_utc": now(),
        "limits": limits(), "chain_id": 137, "address": ADDRESS, "prior_evidence_sha256": PRIOR_EVIDENCE_SHA256,
        "prior_runtime_observations": OBSERVED_RUNTIME, "getter_source_association": FIRST_PARTY_SOURCE,
        "selectors": SELECTORS, "selector_hash_function": "Ethereum Keccak-256, first four bytes",
        "collection_approved": False, "analysis_admitted": False, "source_equivalence_admitted": False,
        "economic_decoding_admitted": False, "token_mapping_admitted": False, "outcome_mapping_admitted": False,
        "exchange_abi_admitted": False, "first_deployment_certified": False,
        "cbor_decoding_performed": False, "offchain_source_http_requested": False,
        "only_blockchain_rpc_transport": True, "cid_content_verified": False,
        "trade_logs_requested": False, "parquet_body_reads": 0, "spill_bytes": 0, "endpoint_disclosed": False,
        "runtime_observations": [], "runtime_blobs": {}, "proxy_implementation_slots": [], "headers": [],
        "getters": [], "configuration_checks": [], "dependency_checks": []}
    controls = rpc = None
    try:
        result["runtime_enforcement"] = {key: value for key, value in enforcement.items() if key != "started_monotonic"}
        resources = inventory.resource_state(run_dir.parent)
        require(resources.get("ram_bytes", {}).get("MemAvailable", 0) >= MAX_RAM_BYTES, "available_ram_below_admission")
        require(resources.get("disk_bytes", {}).get("free", 0) >= FREE_FLOOR_BYTES + MAX_OUTPUT_BYTES, "available_disk_below_admission")
        plan, binding, original = activity.read_plan(plan_path, plan_sha256, plan_source_head)
        prior.validate_plan_blocks(plan)
        private = rpc_private_file or Path.home() / ".polygon_rpc_url"
        activity.validate_output(run_dir, plan_path, private, original, plan)
        source = source_snapshot(expected_head)
        result.update(source_plan_binding=binding, probe_source=source, resource_admission=resources)
        controls = Controls(run_dir, {"schema_version": "polymarket_v3_runtime_owner_v1", "created_utc": now(),
            "source_plan_binding": binding, "probe_source": source, "limits": limits(), "blocks": list(BLOCKS),
            "address": ADDRESS, "runtime_roster": OBSERVED_RUNTIME, "selectors": SELECTORS,
            "resume_permitted": False, "source_equivalence_admitted": False})
        endpoint, configuration_source = inventory.load_rpc_endpoint(private)
        result["configuration_source"] = configuration_source
        rpc = RuntimeRPC(endpoint, transport, [plan[name] for name in
            ("raw_footer_tail_header", "last_included_header", "first_excluded_header")], controls,
            enforcement["started_monotonic"])
        diagnose(rpc, result)
        planner.reopen_inputs(original)
        require(inventory.path_identity(Path(plan["inventory_binding"]["path"])) == plan["inventory_binding"]["identity"],
                "source_inventory_changed_during_probe")
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_changed_during_probe")
        require(source == source_snapshot(expected_head), "probe_source_changed_during_probe")
        result["reopened_control_bindings"] = controls.verify()
        rpc.remaining_time()
        result.update(status="bounded_v3_runtime_observations_complete", final_input_identity_unchanged=True,
                      reopened_headers_unchanged=True,
                      unknown_getter_observations=sum(item["status"] != "observed" for item in result["getters"]))
    except Exception as exc:
        result["error_code"] = shared.safe_error(exc)
    finally:
        result.update(rpc_calls_attempted=len(rpc.ledger) if rpc else 0,
            response_bytes_received=rpc.response_bytes_received if rpc else 0,
            response_bytes_charged=rpc.response_bytes_charged if rpc else 0,
            request_ledger=rpc.ledger if rpc else [], finished_utc=now())
        if rpc:
            result["elapsed_seconds"] = max(0.0, time.monotonic() - rpc.started)
        if controls:
            try:
                result["final_control_bindings"] = controls.verify()
            except HistoryBlocked as exc:
                result.update(status="blocked", error_code=str(exc))
            result.update(request_journal_sha256=controls.journal_hash.hexdigest(),
                          request_journal_records=controls.journal_records,
                          expected_owner_sha256=controls.expected["OWNER.json"], control_bytes_before_result=controls.bytes)
            controls.write("RESULT.json", result, final=True)
            if result["status"] == "bounded_v3_runtime_observations_complete":
                rpc.remaining_time()
                controls.verify()
                controls.write("COMPLETE.json", {"status": result["status"], "analysis_admitted": False,
                    "source_equivalence_admitted": False, "collection_approved": False,
                    "result_sha256": digest(encode(result)), "request_journal_sha256": result["request_journal_sha256"]}, final=True)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", type=Path, required=True)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--source-plan-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    try:
        result = probe(args.source_plan, args.source_plan_sha256, args.source_plan_source_head, args.expected_head,
                       run_dir=args.run_dir, rpc_private_file=args.rpc_private_file)
    except Exception:
        print(json.dumps({"status": "blocked", "error_code": "production_guard_or_control_publication_failure"}))
        return 2
    print(json.dumps({"status": result["status"], "rpc_calls_attempted": result["rpc_calls_attempted"],
                      "analysis_admitted": False, "source_equivalence_admitted": False}))
    return 0 if result["status"] == "bounded_v3_runtime_observations_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
