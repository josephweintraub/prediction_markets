"""Bounded EC2 raw-log capacity pilot, not collection or analysis admission.

Three fixed 100-block windows, five known addresses, two OrderFilled topics,
at most 15 RPC calls, 16 MiB per response, 50,000 logs per window, and 64 MiB
total new output. No ABI/economic decoding, pandas, DuckDB, or disk spill.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Callable
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner

# Verified against the canonical handoff's extraction/extract_orderfilled*.py.
LEGACY_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
MODERN_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
ADDRESSES = tuple(address for _, address in planner.EXCHANGES)
EXPECTED_TOPICS = {address: LEGACY_TOPIC if index < 2 else MODERN_TOPIC
                   for index, address in enumerate(ADDRESSES)}
MAX_CALLS = 15
MAX_RESPONSE_BYTES = 16 * 1024**2
MAX_WINDOW_ROWS = 50_000
MAX_OUTPUT_BYTES = 64 * 1024**2
MANIFEST_RESERVE = 1024**2
LOG_FIELDS = {"address", "blockHash", "blockNumber", "data", "logIndex", "removed",
              "topics", "transactionHash", "transactionIndex"}


class PilotBlocked(ValueError):
    """Fixed non-sensitive failure code."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise PilotBlocked(code)


def read_plan(path: Path, digest: str, source_head: str) -> tuple[dict, dict, dict]:
    require(bool(re.fullmatch(r"[0-9a-f]{64}", digest or "")), "invalid_plan_digest")
    before = inventory.path_identity(path)
    require(before["status"] == "present" and not before["is_symlink"]
            and 0 < before["target"]["bytes"] <= inventory.LIMITS["maximum_output_bytes"], "plan_file_not_admitted")
    with path.open("rb") as stream:
        raw = stream.read(inventory.LIMITS["maximum_output_bytes"] + 1)
    require(before == inventory.path_identity(path) and hashlib.sha256(raw).hexdigest() == digest,
            "plan_hash_or_identity_differs")
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(PilotBlocked("nonfinite_plan_json")))
    require(type(value) is dict and value.get("schema_version") == "polymarket_refresh_source_plan_v1"
            and value.get("status") == "source_plan_ready_for_review", "source_plan_not_ready")
    require(bool(re.fullmatch(r"[0-9a-f]{40}", source_head or ""))
            and value.get("planner_source", {}).get("head") == source_head
            and value["planner_source"].get("tracked_worktree_clean") is True
            and value.get("final_input_identity_unchanged") is True
            and value.get("cutoff_headers_reopened_unchanged") is True, "plan_source_or_identity_not_admitted")
    cutoff = planner.utc_cutoff(value.get("end_exclusive_utc"))
    previous, excluded = value.get("last_included_header", {}), value.get("first_excluded_header", {})
    for header in (previous, excluded):
        require(type(header) is dict and type(header.get("number")) is int and header["number"] > 0
                and type(header.get("timestamp")) is int
                and bool(inventory.HEX_HASH.fullmatch(header.get("hash", "")))
                and bool(inventory.HEX_HASH.fullmatch(header.get("parentHash", ""))), "plan_cutoff_header_invalid")
    require(value.get("end_exclusive_unix_seconds") == cutoff and previous["number"] + 1 == excluded["number"]
            and previous["timestamp"] < cutoff <= excluded["timestamp"]
            and excluded["parentHash"].lower() == previous["hash"].lower(), "plan_cutoff_proof_invalid")
    start = value.get("provisional_append_start", {}).get("start_block")
    require(type(start) is int and 0 < start <= previous["number"]
            and type(value.get("provisional_append_blocks")) is int
            and value["provisional_append_blocks"] == previous["number"] - start + 1,
            "plan_append_range_invalid")
    binding = value.get("inventory_binding", {})
    original, _ = planner.read_inventory(Path(binding["path"]), binding["sha256"], binding["source_head"])
    require(inventory.path_identity(Path(binding["path"])) == binding["identity"], "bound_inventory_identity_changed")
    planner.reopen_inputs(original)
    expected_start = planner.raw_append_start(original, None)
    require(value["provisional_append_start"] == expected_start, "plan_append_start_inventory_binding_differs")
    tail = value.get("raw_footer_tail_header", {})
    require(type(tail) is dict and type(tail.get("number")) is int
            and tail["number"] == expected_start["raw_footer_declared_maximum_block"]
            and type(tail.get("timestamp")) is int and 0 < tail["timestamp"] < cutoff
            and bool(inventory.HEX_HASH.fullmatch(tail.get("hash", "")))
            and bool(inventory.HEX_HASH.fullmatch(tail.get("parentHash", ""))),
            "plan_raw_tail_header_inventory_binding_differs")
    return value, {"path": str(path), "sha256": digest, "source_head": source_head, "identity": before}, original


def fixed_windows(start: int, end: int) -> list[dict]:
    require(type(start) is int and type(end) is int and 0 < start <= end and end - start + 1 >= 400,
            "append_range_too_small_for_fixed_windows")
    span = end - start
    result = [{"window": f"q{quarter * 25}", "first_block": start + span * quarter // 4 - 49,
               "last_block": start + span * quarter // 4 + 50, "blocks": 100}
              for quarter in (1, 2, 3)]
    require(all(start <= item["first_block"] <= item["last_block"] <= end for item in result)
            and all(left["last_block"] < right["first_block"] for left, right in zip(result, result[1:])),
            "fixed_window_bounds_or_overlap")
    return result


def http_response(endpoint: str, payload: dict) -> bytes:
    request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read(MAX_RESPONSE_BYTES + 1)


class PilotRPC:
    def __init__(self, private_file: Path, transport: Callable):
        self.endpoint, self.configuration_source = inventory.load_rpc_endpoint(private_file)
        self.transport = transport
        self.ledger: list[dict] = []
        self.headers: dict[int, dict] = {}

    def call(self, method: str, params: list) -> tuple[Any, dict]:
        require(method in {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}, "rpc_method_not_permitted")
        require(len(self.ledger) < MAX_CALLS, "rpc_call_limit")
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params, "status": "requested"}
        self.ledger.append(entry)
        try:
            raw = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"], "method": method, "params": params})
            require(type(raw) is bytes, "rpc_response_not_bytes")
            entry["response_bytes"] = len(raw)
            require(len(raw) <= MAX_RESPONSE_BYTES, "rpc_response_byte_limit")
            reply = json.loads(raw, object_pairs_hook=inventory.unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(PilotBlocked("rpc_nonfinite_json")))
            require(type(reply) is dict and set(reply) == {"jsonrpc", "id", "result"}, "rpc_invalid_reply_shape")
            require(reply["jsonrpc"] == "2.0" and type(reply["id"]) is int and reply["id"] == entry["id"],
                    "rpc_reply_id_mismatch")
            return reply["result"], entry
        except Exception:
            entry["status"] = "failed"
            raise

    def chain(self) -> None:
        result, entry = self.call("eth_chainId", [])
        require(inventory._quantity(result) == 137, "rpc_wrong_chain")
        entry.update(status="validated", chain_id=137)

    def header(self, number: int, *, reopen: bool = False) -> dict:
        result, entry = self.call("eth_getBlockByNumber", [hex(number), False])
        require(type(result) is dict, "rpc_header_not_object")
        require(inventory._quantity(result.get("number")) == number, "rpc_header_number_mismatch")
        timestamp = inventory._quantity(result.get("timestamp"))
        require(0 < timestamp <= 253402300799
                and bool(inventory.HEX_HASH.fullmatch(result.get("hash", "")))
                and bool(inventory.HEX_HASH.fullmatch(result.get("parentHash", ""))), "rpc_header_clock_or_hash_invalid")
        header = {"number": number, "hash": result["hash"], "parentHash": result["parentHash"], "timestamp": timestamp}
        if reopen:
            require(self.headers.get(number) == header, "rpc_reopened_header_changed")
        for other_number, other in self.headers.items():
            require(not (other_number < number and other["timestamp"] > timestamp)
                    and not (other_number > number and other["timestamp"] < timestamp), "rpc_header_time_nonmonotone")
        self.headers[number] = header
        entry.update(status="validated", header=header)
        return header


def _uint64(value: Any) -> int:
    number = inventory._quantity(value)
    require(0 <= number < 2**64, "native_uint64_out_of_range")
    return number


def validate_logs(logs: Any, window: dict, headers: dict[int, dict], request_id: int, *,
                  shared_keys: dict | None = None, shared_transactions: dict | None = None) -> tuple[list[dict], dict]:
    require(type(logs) is list and len(logs) <= MAX_WINDOW_ROWS, "window_log_row_limit_or_shape")
    rows = []
    identities, block_positions, block_hashes, transaction_positions, transaction_indices = {}, {}, {}, {}, {}
    shared_keys = {} if shared_keys is None else shared_keys
    shared_transactions = {} if shared_transactions is None else shared_transactions
    counts = Counter()
    for log in logs:
        require(type(log) is dict and set(log) == LOG_FIELDS, "native_log_schema_differs")
        require(log["removed"] is False, "removed_or_unconfirmed_log")
        block, index, tx_index = (_uint64(log[key]) for key in ("blockNumber", "logIndex", "transactionIndex"))
        require(window["first_block"] <= block <= window["last_block"], "log_outside_requested_window")
        require(type(log["address"]) is str and bool(re.fullmatch(r"0x[0-9a-fA-F]{40}", log["address"])),
                "invalid_log_address")
        address = log["address"].lower()
        require(address in EXPECTED_TOPICS, "unexpected_log_address")
        topics = log["topics"]
        require(type(topics) is list and len(topics) == 4
                and all(type(topic) is str and bool(inventory.HEX_HASH.fullmatch(topic)) for topic in topics),
                "invalid_log_topics")
        topic = topics[0].lower()
        require(topic == EXPECTED_TOPICS[address], "unexpected_address_topic_pair")
        require(all(item[2:26] == "0" * 24 for item in topics[2:]), "noncanonical_indexed_address")
        expected_words = 5 if topic == LEGACY_TOPIC else 7
        require(type(log["data"]) is str and bool(re.fullmatch(r"0x[0-9a-fA-F]*", log["data"]))
                and len(log["data"]) == 2 + expected_words * 64, "raw_event_data_width_invalid")
        require(all(type(log[key]) is str and bool(inventory.HEX_HASH.fullmatch(log[key]))
                    for key in ("blockHash", "transactionHash")), "invalid_native_log_hash")
        block_hash, transaction = log["blockHash"].lower(), log["transactionHash"].lower()
        require(block not in block_hashes or block_hashes[block] == block_hash, "inconsistent_same_block_hash")
        block_hashes[block] = block_hash
        if block in headers:
            require(headers[block]["hash"].lower() == block_hash, "log_endpoint_header_hash_mismatch")
        tx_position = (block, block_hash, tx_index)
        require(transaction not in transaction_positions or transaction_positions[transaction] == tx_position,
                "inconsistent_transaction_position")
        transaction_positions[transaction] = tx_position
        require((block, tx_index) not in transaction_indices or transaction_indices[(block, tx_index)] == transaction,
                "conflicting_block_transaction_index")
        transaction_indices[(block, tx_index)] = transaction
        key = (address, transaction, index)
        if key in identities:
            require(identities[key] != log, "duplicate_native_log_key")
            raise PilotBlocked("conflicting_native_log_key")
        identities[key] = log
        payload_digest = hashlib.sha256(json.dumps(log, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if key in shared_keys:
            require(shared_keys[key] != payload_digest, "duplicate_native_log_key_across_windows")
            raise PilotBlocked("conflicting_native_log_key_across_windows")
        require(transaction not in shared_transactions or shared_transactions[transaction] == tx_position,
                "inconsistent_transaction_position_across_windows")
        shared_keys[key] = payload_digest
        shared_transactions[transaction] = tx_position
        position = (block, index)
        require(position not in block_positions, "conflicting_block_log_position")
        block_positions[position] = key
        rows.append({**log, "block_number": block, "log_index": index, "transaction_index": tx_index,
                     "exchange_address": address, "window": window["window"], "source_request_id": request_id})
        counts[(address, topic)] += 1
    rows.sort(key=lambda row: (row["block_number"], row["transaction_index"], row["log_index"]))
    return rows, {"rows": len(rows), "distinct_native_keys": len(identities),
                  "occupied_blocks": len(block_hashes), "zero_log_window": not bool(rows),
                  "counts": [{"address": address, "topic": EXPECTED_TOPICS[address],
                              "rows": counts[(address, EXPECTED_TOPICS[address])]} for address in ADDRESSES],
                  "interior_block_headers_verified": False, "interior_exact_timestamps_collected": False}


def parquet_payload(rows: list[dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([(key, pa.bool_() if key == "removed" else pa.list_(pa.string()) if key == "topics"
                         else pa.string()) for key in sorted(LOG_FIELDS)] +
                       [("block_number", pa.uint64()), ("log_index", pa.uint64()), ("transaction_index", pa.uint64()),
                        ("exchange_address", pa.string()), ("window", pa.string()), ("source_request_id", pa.uint8())])
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression="zstd", row_group_size=10_000)
    payload = sink.getvalue().to_pybytes()
    reopened = pq.read_table(pa.BufferReader(payload))
    require(reopened.schema == schema and reopened.to_pylist() == rows, "parquet_roundtrip_differs")
    return payload


def density_scenarios(windows: list[dict], full_range_blocks: int) -> dict:
    require(len(windows) == 3 and all(item["status"] == "validated" for item in windows), "incomplete_pilot_for_scenarios")
    total_rows = sum(item["rows"] for item in windows)
    if total_rows == 0:
        return {"status": "not_computed_no_observed_logs", "scenarios": [], "sampled_blocks": 300}
    mean_rows = total_rows / 300
    mean_bytes = sum(item["parquet_bytes"] for item in windows) / 300
    maximum_rows = max(item["rows"] / 100 for item in windows)
    maximum_bytes = max(item["parquet_bytes"] / 100 for item in windows)
    return {"status": "illustrative_composition_sensitive_not_capacity_admission", "sampled_blocks": 300,
            "scenarios": [{"label": label, "rows_if_density_persisted": rate * full_range_blocks,
                           "compressed_bytes_if_density_persisted": size * full_range_blocks}
                          for label, rate, size in (("sample_mean", mean_rows, mean_bytes),
                                                   ("densest_observed_window", maximum_rows, maximum_bytes),
                                                   ("twice_observed_maxima", 2 * maximum_rows, 2 * maximum_bytes))],
            "limitations": "Selected windows can miss activity/composition peaks; 2x is not an upper bound. Parquet schema/row-group overhead and compression change with shard size; these are neither confidence bounds nor accepted storage reservations."}


def _json_bytes(value: dict) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def run_pilot(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str,
              output_dir: Path, *, rpc_private_file: Path | None = None,
              transport: Callable = http_response) -> dict:
    private_file = rpc_private_file or Path.home() / ".polygon_rpc_url"
    plan, binding, original = read_plan(plan_path, plan_sha256, plan_source_head)
    planner.validate_output(output_dir, original, plan_path, private_file)
    require(output_dir.resolve() != Path(plan["inventory_binding"]["path"]).resolve(), "output_overlaps_inventory")
    code_before = inventory.code_state()
    require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_head or "")) and code_before.get("status") == "inspected"
            and code_before.get("head") == expected_head and code_before.get("tracked_worktree_clean") is True,
            "pilot_source_head_or_clean_state_differs")
    windows = fixed_windows(plan["provisional_append_start"]["start_block"], plan["last_included_header"]["number"])
    output_dir.mkdir(mode=0o700)  # Exclusive ownership; failures are never resumed in place.
    run_id = str(uuid.uuid4())
    outputs, charged_bytes = [], 0

    def write(name: str, raw: bytes, *, reserve_manifest: bool = True) -> None:
        nonlocal charged_bytes
        reserve = MANIFEST_RESERVE if reserve_manifest else 0
        require(charged_bytes + len(raw) + reserve <= MAX_OUTPUT_BYTES, "pilot_output_byte_limit")
        with (output_dir / name).open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        charged_bytes += len(raw)
        outputs.append({"path": name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})

    write("OWNER.json", _json_bytes({"run_id": run_id, "source_plan_sha256": plan_sha256}))
    result: dict[str, Any] = {
        "schema_version": "polymarket_increment_capacity_pilot_v1", "status": "failed_pilot",
        "created_utc": datetime.now(timezone.utc).isoformat(), "run_id": run_id, "source_plan": binding,
        "source": code_before, "end_exclusive_utc": planner.END_EXCLUSIVE,
        "limits": {"rpc_calls": MAX_CALLS, "response_bytes": MAX_RESPONSE_BYTES,
                   "rows_per_window": MAX_WINDOW_ROWS, "total_output_bytes": MAX_OUTPUT_BYTES, "spill_bytes": 0},
        "known_address_topic_policy": EXPECTED_TOPICS, "planned_windows": windows, "windows": [], "request_ledger": [],
        "native_payload_only": True, "economic_deduplication": False, "resolution_join": False,
        "collection_complete": False, "analysis_admitted": False, "endpoint_disclosed": False,
        "limitations": ["Three fixed windows are not full-range collection, complete activity detection or capacity certification.",
                        "Only window endpoint headers are independently fetched; interior hashes have format/within-window consistency checks only.",
                        "No exact interior timestamps, historical ABI implementation, payout or Combo admission is established.",
                        "No fallback, retry, response truncation or silent duplicate removal is permitted."],
    }
    rpc = None
    # At most 3 * MAX_WINDOW_ROWS entries; retain digests, not prior raw bodies.
    shared_keys, shared_transactions = {}, {}
    try:
        rpc = PilotRPC(private_file, transport)
        rpc.chain()
        for key in ("last_included_header", "first_excluded_header"):
            require(rpc.header(plan[key]["number"]) == plan[key], "source_plan_cutoff_header_drift")
        for window in windows:
            first, last = (rpc.header(window[key]) for key in ("first_block", "last_block"))
            query = {"fromBlock": hex(window["first_block"]), "toBlock": hex(window["last_block"]),
                     "address": list(ADDRESSES), "topics": [[LEGACY_TOPIC, MODERN_TOPIC]]}
            logs, request = rpc.call("eth_getLogs", [query])
            rows, evidence = validate_logs(logs, window, {first["number"]: first, last["number"]: last}, request["id"],
                                           shared_keys=shared_keys, shared_transactions=shared_transactions)
            payload = parquet_payload(rows)
            filename = window["window"] + ".parquet"
            write(filename, payload)
            require((output_dir / filename).read_bytes() == payload, "published_parquet_bytes_differ")
            rpc.header(last["number"], reopen=True)
            request.update(status="validated", log_rows=len(rows), raw_payload_path=filename)
            result["windows"].append({**window, **evidence, "status": "validated", "first_header": first,
                                      "last_header": last, "parquet_bytes": len(payload), "parquet_path": filename,
                                      "response_bytes": request["response_bytes"]})
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_identity_changed")
        with plan_path.open("rb") as stream:
            plan_bytes = stream.read(inventory.LIMITS["maximum_output_bytes"] + 1)
        require(len(plan_bytes) <= inventory.LIMITS["maximum_output_bytes"]
                and hashlib.sha256(plan_bytes).hexdigest() == plan_sha256, "source_plan_hash_changed")
        bound_inventory = plan["inventory_binding"]
        inventory_path = Path(bound_inventory["path"])
        require(inventory.path_identity(inventory_path) == bound_inventory["identity"],
                "bound_inventory_identity_changed")
        reopened_inventory, _ = planner.read_inventory(inventory_path, bound_inventory["sha256"],
                                                       bound_inventory["source_head"])
        require(reopened_inventory == original, "bound_inventory_content_changed")
        planner.reopen_inputs(original)
        require(inventory.code_state() == code_before, "pilot_source_changed")
        result.update(status="bounded_pilot_complete", final_input_identity_unchanged=True,
                      validated_windows=3, sampled_blocks=300, sampled_rows=sum(item["rows"] for item in result["windows"]),
                      sampled_distinct_native_keys=len(shared_keys),
                      sampled_log_response_bytes=sum(item["response_bytes"] for item in result["windows"]),
                      sampled_parquet_bytes=sum(item["parquet_bytes"] for item in result["windows"]),
                      density_scenarios=density_scenarios(result["windows"], plan["provisional_append_blocks"]))
    except (PilotBlocked, planner.PlanBlocked, inventory.ReadinessError) as exc:
        result["error_code"] = str(exc)
        if rpc is not None and rpc.ledger and rpc.ledger[-1]["status"] == "requested":
            rpc.ledger[-1]["status"] = "validation_failed"
    except Exception:
        result["error_code"] = "pilot_transport_payload_or_input_failure"
    finally:
        if rpc is not None:
            result["request_ledger"] = rpc.ledger
        result["rpc_calls_attempted"] = len(result["request_ledger"])
        if result["status"] != "bounded_pilot_complete" and result["windows"]:
            result["status"] = "partial_pilot"
        result["outputs"] = list(outputs)
        result["output_bytes_before_manifest"] = charged_bytes
        manifest = _json_bytes(result)
        require(len(manifest) + 4096 <= MANIFEST_RESERVE, "manifest_reserve_exceeded")
        write("manifest.json", manifest, reserve_manifest=False)
        require((output_dir / "manifest.json").read_bytes() == manifest, "published_manifest_bytes_differ")
        if result["status"] == "bounded_pilot_complete":
            for item in result["outputs"]:
                require(hashlib.sha256((output_dir / item["path"]).read_bytes()).hexdigest() == item["sha256"],
                        "pilot_output_reopen_differs")
            marker = {"schema_version": "polymarket_increment_capacity_pilot_complete_v1", "run_id": run_id,
                      "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
                      "source_plan_sha256": plan_sha256, "status": "bounded_pilot_complete_not_collection_complete"}
            write("COMPLETE.json", _json_bytes(marker), reserve_manifest=False)
        directory_fd = os.open(output_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", required=True, type=Path)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--plan-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production pilot requires the canonical mounted EC2 checkout.")
    try:
        result = run_pilot(args.source_plan, args.source_plan_sha256, args.plan_source_head,
                           args.expected_head, args.output_dir, rpc_private_file=args.rpc_private_file)
    except Exception:
        parser.error("Source/destination admission failed; no complete pilot was published.")
    print(json.dumps({"status": result["status"], "rpc_calls_attempted": result["rpc_calls_attempted"],
                      "validated_windows": len(result["windows"]), "collection_complete": False}, sort_keys=True))
    return 0 if result["status"] == "bounded_pilot_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
