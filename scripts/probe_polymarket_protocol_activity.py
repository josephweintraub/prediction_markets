"""Bounded V3-topic activity evidence; not a collector or protocol admission.

Only explicit small windows are queried. Saved evidence contains counts,
response hashes and one public native-log identity per window, never wallet,
order, event-data, endpoint or raw bytecode values.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any, Callable
import urllib.request

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner

ADDRESS = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
TOPIC0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
MAX_RPC_CALLS = 10
MAX_RESPONSE_BYTES = 16 * 1024**2
MAX_WINDOWS = 3
MAX_WINDOW_BLOCKS = 1000
DEFAULT_WINDOW_BLOCKS = 100
HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
HEX_BYTES = re.compile(r"0x(?:[0-9a-fA-F]{2})*\Z")


class ProbeBlocked(ValueError):
    """Only fixed non-sensitive evidence codes cross the output boundary."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise ProbeBlocked(code)


def http_rpc(endpoint: str, payload: dict) -> bytes:
    request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    require(len(raw) <= MAX_RESPONSE_BYTES, "rpc_response_byte_limit")
    return raw


def header_fields(value: Any) -> dict:
    require(type(value) is dict, "header_not_object")
    number = inventory._quantity(value.get("number")) if type(value.get("number")) is str else value.get("number")
    timestamp = (inventory._quantity(value.get("timestamp")) if type(value.get("timestamp")) is str
                 else value.get("timestamp"))
    require(type(number) is int and number >= 0, "invalid_header_number")
    require(type(timestamp) is int and 0 <= timestamp <= 253402300799
            and (number == 0 or timestamp > 0), "invalid_header_timestamp")
    require(type(value.get("hash")) is str and HASH.fullmatch(value["hash"])
            and type(value.get("parentHash")) is str and HASH.fullmatch(value["parentHash"]),
            "invalid_header_hash")
    return {"number": number, "timestamp": timestamp,
            "hash": value["hash"].lower(), "parentHash": value["parentHash"].lower()}


def read_plan(path: Path, expected_sha256: str, expected_head: str) -> tuple[dict, dict, dict]:
    require(type(expected_sha256) is str and re.fullmatch(r"[0-9a-f]{64}", expected_sha256),
            "invalid_source_plan_digest")
    before = inventory.path_identity(path)
    require(before["status"] == "present" and not before["is_symlink"], "source_plan_not_regular")
    require(0 < before["target"]["bytes"] <= MAX_RESPONSE_BYTES, "source_plan_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_RESPONSE_BYTES + 1)
    require(len(raw) <= MAX_RESPONSE_BYTES and hashlib.sha256(raw).hexdigest() == expected_sha256,
            "source_plan_digest_differs")
    require(before == inventory.path_identity(path), "source_plan_changed_during_read")
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ProbeBlocked("source_plan_nonfinite_json")))
    require(type(value) is dict and value.get("schema_version") == "polymarket_refresh_source_plan_v1"
            and value.get("status") == "source_plan_ready_for_review", "source_plan_not_ready")
    require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head),
            "invalid_source_plan_head")
    source = value.get("planner_source", {})
    require(type(source) is dict and source.get("status") == "inspected"
            and source.get("head") == expected_head and source.get("tracked_worktree_clean") is True,
            "source_plan_head_or_clean_state_differs")
    require(value.get("final_input_identity_unchanged") is True
            and value.get("cutoff_headers_reopened_unchanged") is True
            and value.get("trade_logs_requested") is False
            and type(value.get("parquet_body_reads")) is int and value["parquet_body_reads"] == 0
            and value.get("collection_approved") is False and value.get("exchange_abi_admitted") is False,
            "source_plan_scope_or_identity_differs")
    cutoff = planner.utc_cutoff(value.get("end_exclusive_utc"))
    require(type(value.get("end_exclusive_unix_seconds")) is int
            and value["end_exclusive_unix_seconds"] == cutoff, "source_plan_cutoff_differs")
    proof = value.get("cutoff_proof")
    require(type(proof) is dict and set(proof) == {"adjacent_block_numbers", "parent_hash_matches",
            "last_time_strictly_before_cutoff", "first_time_at_or_after_cutoff"}
            and all(item is True for item in proof.values()), "source_plan_cutoff_proof_incomplete")
    previous, excluded = (header_fields(value.get(name)) for name in
                          ("last_included_header", "first_excluded_header"))
    require(previous["number"] + 1 == excluded["number"]
            and previous["timestamp"] < cutoff <= excluded["timestamp"]
            and excluded["parentHash"] == previous["hash"], "source_plan_cutoff_proof_differs")
    raw_header = header_fields(value.get("raw_footer_tail_header"))
    start = value.get("provisional_append_start", {})
    require(type(start) is dict and type(start.get("raw_footer_declared_maximum_block")) is int
            and start["raw_footer_declared_maximum_block"] == raw_header["number"]
            and 0 < raw_header["number"] <= previous["number"], "source_plan_raw_endpoint_differs")
    binding = value.get("inventory_binding")
    require(type(binding) is dict, "source_inventory_binding_absent")
    source_inventory, identity = planner.read_inventory(Path(binding["path"]), binding["sha256"], binding["source_head"])
    require(identity == binding, "source_inventory_binding_differs")
    planner.reopen_inputs(source_inventory)
    declared_start = planner.raw_append_start(source_inventory, None)
    require(declared_start["raw_footer_declared_maximum_block"] == raw_header["number"]
            and type(start.get("start_block")) is int and start["start_block"] == declared_start["start_block"],
            "source_plan_raw_endpoint_inventory_mismatch")
    return value, {"path": str(path), "sha256": expected_sha256, "source_head": expected_head,
                   "identity": before}, source_inventory


def windows_for_plan(plan: dict, windows: list[tuple[int, int]] | None) -> list[dict]:
    end = plan["last_included_header"]["number"]
    raw = plan["raw_footer_tail_header"]["number"]
    selected = ([("historical_tail", max(0, raw - DEFAULT_WINDOW_BLOCKS + 1), raw),
                 ("cutoff_tail", max(0, end - DEFAULT_WINDOW_BLOCKS + 1), end)]
                if windows is None else [(f"explicit_{i}", pair[0], pair[1]) for i, pair in enumerate(windows, 1)])
    require(0 < len(selected) <= MAX_WINDOWS, "window_count_limit")
    result = []
    for label, lower, upper in selected:
        require(type(lower) is int and type(upper) is int and 0 <= lower <= upper <= end,
                "invalid_window_bounds")
        require(upper - lower + 1 <= MAX_WINDOW_BLOCKS, "window_block_limit")
        require(all(upper < item["start_block"] or lower > item["end_block"] for item in result),
                "overlapping_probe_windows")
        result.append({"label": label, "start_block": lower, "end_block": upper,
                       "block_count": upper - lower + 1})
    return result


class ProbeRPC:
    def __init__(self, endpoint: str, transport: Callable, seeds: list[dict]):
        self.endpoint, self.transport = endpoint, transport
        self.ledger: list[dict] = []
        self.headers: dict[int, dict] = {}
        self.native: dict[tuple, str] = {}
        self.block_hashes: dict[int, str] = {}
        for value in seeds:
            self.admit_header(header_fields(value))

    def admit_header(self, value: dict) -> dict:
        number, timestamp = value["number"], value["timestamp"]
        if number in self.headers:
            require(self.headers[number] == value, "source_or_reopened_header_changed")
        for other_number, other in self.headers.items():
            require(not (other_number < number and other["timestamp"] > timestamp)
                    and not (other_number > number and other["timestamp"] < timestamp),
                    "observed_nonmonotone_header_time")
        require(number not in self.block_hashes or self.block_hashes[number] == value["hash"],
                "observed_block_hash_conflict")
        self.headers[number] = value
        self.block_hashes[number] = value["hash"]
        return value

    def call(self, method: str, params: list) -> tuple[Any, dict]:
        require(method in {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs", "eth_getStorageAt"},
                "rpc_method_not_permitted")
        require(len(self.ledger) < MAX_RPC_CALLS, "rpc_call_limit")
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params, "status": "requested"}
        self.ledger.append(entry)
        try:
            raw = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"],
                                                "method": method, "params": params})
            require(type(raw) is bytes and len(raw) <= MAX_RESPONSE_BYTES, "rpc_response_byte_limit_or_type")
            entry.update(response_bytes=len(raw), response_sha256=hashlib.sha256(raw).hexdigest())
            value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(ProbeBlocked("rpc_nonfinite_json")))
            require(type(value) is dict and set(value) == {"jsonrpc", "id", "result"}, "rpc_invalid_reply_shape")
            require(value["jsonrpc"] == "2.0" and type(value["id"]) is int and value["id"] == entry["id"],
                    "rpc_reply_id_mismatch")
            entry["status"] = "response_shape_validated"
            return value["result"], entry
        except Exception:
            entry["status"] = "failed"
            raise

    def header(self, number: int) -> dict:
        value, entry = self.call("eth_getBlockByNumber", [hex(number), False])
        require(type(value) is dict and type(value.get("number")) is str
                and type(value.get("timestamp")) is str, "rpc_header_quantity_type")
        result = header_fields(value)
        require(result["number"] == number, "rpc_header_number_mismatch")
        self.admit_header(result)
        entry["status"] = "validated"
        return result

    def probe_window(self, window: dict) -> dict:
        header = self.header(window["end_block"])
        logs, entry = self.call("eth_getLogs", [{"fromBlock": hex(window["start_block"]),
            "toBlock": hex(window["end_block"]), "address": ADDRESS, "topics": [TOPIC0]}])
        require(type(logs) is list, "rpc_logs_not_list")
        first = None
        data_lengths, topic_counts = set(), set()
        for item in logs:
            require(type(item) is dict and type(item.get("address")) is str
                    and item["address"].lower() == ADDRESS, "log_address_mismatch")
            topics = item.get("topics")
            require(type(topics) is list and len(topics) == 4
                    and all(type(topic) is str and HASH.fullmatch(topic) for topic in topics)
                    and topics[0].lower() == TOPIC0, "log_topic_or_schema_mismatch")
            require(item.get("removed") is False, "log_removed_or_flag_absent")
            number = inventory._quantity(item.get("blockNumber"))
            index = inventory._quantity(item.get("logIndex"))
            transaction_index = inventory._quantity(item.get("transactionIndex"))
            require(window["start_block"] <= number <= window["end_block"], "log_outside_requested_window")
            require(type(item.get("blockHash")) is str and HASH.fullmatch(item["blockHash"])
                    and type(item.get("transactionHash")) is str and HASH.fullmatch(item["transactionHash"]),
                    "invalid_log_hash")
            block_hash, transaction_hash = item["blockHash"].lower(), item["transactionHash"].lower()
            require(number not in self.block_hashes or self.block_hashes[number] == block_hash,
                    "observed_block_hash_conflict")
            self.block_hashes[number] = block_hash
            data = item.get("data")
            require(type(data) is str and HEX_BYTES.fullmatch(data), "invalid_log_data_encoding")
            native = (ADDRESS, transaction_hash, index)
            normalized = [number, block_hash, transaction_hash, transaction_index, index,
                          ADDRESS, [topic.lower() for topic in topics], data.lower()]
            digest = hashlib.sha256(json.dumps(normalized, separators=(",", ":")).encode()).hexdigest()
            if native in self.native:
                require(self.native[native] == digest, "conflicting_native_identity")
                raise ProbeBlocked("duplicate_native_identity")
            self.native[native] = digest
            if first is None:
                first = {"exchange_address": ADDRESS, "transaction_hash": transaction_hash,
                         "log_index": index, "block_number": number, "block_hash": block_hash}
            topic_counts.add(len(topics))
            data_lengths.add((len(data) - 2) // 2)
        entry["status"] = "validated"
        return {**window, "end_header": header, "matching_log_count": len(logs),
                "response_bytes": entry["response_bytes"], "response_sha256": entry["response_sha256"],
                "first_native_identity": first,
                "observed_schema": {"topic_counts": sorted(topic_counts), "data_byte_lengths": sorted(data_lengths),
                    "validated_fields": ["address", "topics", "blockNumber", "blockHash", "transactionHash",
                                         "transactionIndex", "logIndex", "removed", "data"]},
                "presence_observed": bool(logs),
                "absence_scope": "Only this literal window and topic; no whole-period absence claim."}

    def implementation(self, block: int) -> dict:
        value, entry = self.call("eth_getStorageAt", [ADDRESS, IMPLEMENTATION_SLOT, hex(block)])
        require(type(value) is str and HASH.fullmatch(value), "invalid_implementation_storage_word")
        raw = bytes.fromhex(value[2:])
        require(raw[:12] == bytes(12), "implementation_slot_not_address_word")
        entry["status"] = "validated"
        return {"block": block, "implementation_address": "0x" + raw[12:].hex() if any(raw) else None,
                "storage_value_sha256": hashlib.sha256(raw).hexdigest(),
                "response_sha256": entry["response_sha256"],
                "semantics": "EIP-1967 slot observation only; zero is not proof of no proxy, and no ABI is admitted."}


def validate_output(output: Path, plan_path: Path, private_file: Path, source_inventory: dict,
                    plan: dict) -> None:
    require(not output.exists() and not output.is_symlink(), "output_already_exists")
    forbidden = [plan_path, private_file, Path(plan["inventory_binding"]["path"])]
    forbidden.extend(Path(item["path"]) for item in source_inventory["inputs"].values())
    if source_inventory["repair_manifest"] is not None:
        forbidden.append(Path(source_inventory["repair_manifest"]["path"]))
    target = output.resolve()
    require(all(target != path.resolve() and path.resolve() not in target.parents for path in forbidden),
            "output_overlaps_input_or_private_configuration")


def probe(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, *,
          windows: list[tuple[int, int]] | None = None, check_implementation: bool = False,
          rpc_private_file: Path | None = None, transport: Callable = http_rpc,
          output: Path | None = None) -> dict:
    result = {"schema_version": "polymarket_protocol_activity_probe_v1", "status": "blocked",
              "created_utc": datetime.now(timezone.utc).isoformat(), "address": ADDRESS,
              "topic_name": "OrderFilled candidate, topic-only diagnostic", "topic0": TOPIC0,
              "maximum_rpc_calls": MAX_RPC_CALLS, "maximum_response_bytes": MAX_RESPONSE_BYTES,
              "collection_approved": False, "exchange_abi_admitted": False, "parquet_body_reads": 0,
              "trade_logs_requested": True, "whole_history_scan_performed": False,
              "endpoint_disclosed": False, "sensitive_log_values_saved": False,
              "windows": [], "implementation_slots": [], "request_ledger": [],
              "limitations": ["Positive activity proves only the observed native log identity and topic in a literal window.",
                  "Zero matching logs is not whole-history or whole-period absence evidence.",
                  "Response hashes are not saved raw responses; replay requires the bound RPC predicates.",
                  "Window-end headers are verified; other log blocks are checked for internal hash consistency, not all independently fetched.",
                  "No decoding, ABI, proxy architecture, economic direction or final-sample admission is established."]}
    rpc = None
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    try:
        value, binding, source_inventory = read_plan(plan_path, plan_sha256, plan_source_head)
        if output is not None:
            validate_output(output, plan_path, private, source_inventory, value)
        result["source_plan_binding"] = binding
        source = inventory.code_state()
        require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head)
                and source.get("status") == "inspected" and source.get("head") == expected_head
                and source.get("tracked_worktree_clean") is True, "probe_source_head_or_clean_state_differs")
        result["probe_source"] = source
        selected = windows_for_plan(value, windows)
        endpoint, configuration_source = inventory.load_rpc_endpoint(private)
        rpc = ProbeRPC(endpoint, transport, [value[name] for name in
                       ("raw_footer_tail_header", "last_included_header", "first_excluded_header")])
        chain, entry = rpc.call("eth_chainId", [])
        require(inventory._quantity(chain) == 137, "rpc_wrong_chain")
        entry["status"] = "validated"
        for window in selected:
            result["windows"].append(rpc.probe_window(window))
        if check_implementation:
            for block in (value["raw_footer_tail_header"]["number"], value["last_included_header"]["number"]):
                result["implementation_slots"].append(rpc.implementation(block))
        planner.reopen_inputs(source_inventory)
        inventory_binding = value["inventory_binding"]
        require(inventory.path_identity(Path(inventory_binding["path"])) == inventory_binding["identity"],
                "source_inventory_changed_during_probe")
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_changed_during_probe")
        require(source == inventory.code_state(), "probe_source_changed_during_probe")
        result.update(status="bounded_activity_probe_complete", final_input_identity_unchanged=True,
                      configuration_source=configuration_source)
    except (ProbeBlocked, planner.PlanBlocked, inventory.ReadinessError) as exc:
        result["error_code"] = str(exc)
    except Exception:
        result["error_code"] = "probe_input_rpc_or_configuration_failure"
    finally:
        if rpc is not None:
            result["request_ledger"] = rpc.ledger
        result["rpc_calls_attempted"] = len(result["request_ledger"])
    return result


def parse_window(value: str) -> tuple[int, int]:
    if not re.fullmatch(r"[0-9]+:[0-9]+", value):
        raise argparse.ArgumentTypeError("Window must be literal START:END integer blocks.")
    start, end = (int(item) for item in value.split(":"))
    return start, end


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", type=Path, required=True)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--source-plan-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--window", type=parse_window, action="append")
    parser.add_argument("--check-implementation", action="store_true")
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production probing requires the canonical mounted EC2 checkout.")
    # Refuse even a blocked-result write if its destination overlaps an input.
    value, _, source_inventory = read_plan(args.source_plan, args.source_plan_sha256, args.source_plan_source_head)
    validate_output(args.output, args.source_plan, args.rpc_private_file, source_inventory, value)
    result = probe(args.source_plan, args.source_plan_sha256, args.source_plan_source_head, args.expected_head,
                   windows=args.window, check_implementation=args.check_implementation,
                   rpc_private_file=args.rpc_private_file, output=args.output)
    inventory.write_exclusive(args.output, result)
    print(json.dumps({"status": result["status"], "rpc_calls_attempted": result["rpc_calls_attempted"],
                      "collection_approved": False, "exchange_abi_admitted": False}, sort_keys=True))
    return 0 if result["status"] == "bounded_activity_probe_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
