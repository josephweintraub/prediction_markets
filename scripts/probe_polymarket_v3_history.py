"""Bounded native checks of one known V3 upgrade, not historical ABI admission.

No trade logs, receipts other than the explicitly named upgrade transaction,
wallets, endpoint values or runtime bytecode are saved. The diagnostic does not
discover the first proxy deployment or certify a gap-free upgrade history.
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
from scripts import probe_polymarket_protocol_activity as activity

ADDRESS = activity.ADDRESS
OLD_IMPLEMENTATION = "0x7345c6842b244926125ed4054905cac49620b5dc"
NEW_IMPLEMENTATION = "0x641b40ec414a076b9e79e703fc7bf4ebec248bb7"
OLD_REPORTED_CREATION_BLOCK = 87_479_446
NEW_REPORTED_CREATION_BLOCK = 92_765_657
UPGRADE_BLOCK = 93_452_332
UPGRADE_LOG_INDEX = 752
UPGRADE_TRANSACTION = "0x2b55c1ba874ab18e9f4258c99db25bebd0a34bdcf3834eea1ea77e4e7629d364"
# keccak256("Upgraded(address)"), the ERC-1967 indexed-address event.
UPGRADED_TOPIC = "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"
RAW_TAIL_BLOCK = 88_978_537
CUTOFF_BLOCK = 94_796_039
MAX_RPC_CALLS = 32
MAX_RESPONSE_BYTES = 4 * 1024**2
MAX_TOTAL_RESPONSE_BYTES = 16 * 1024**2
MAX_RECEIPT_LOGS = 4096
MAX_OUTPUT_BYTES = 1024**2


class HistoryBlocked(ValueError):
    """Only fixed non-sensitive evidence codes cross the output boundary."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise HistoryBlocked(code)


def http_rpc(endpoint: str, payload: dict, *, byte_limit: int = MAX_RESPONSE_BYTES) -> bytes:
    request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(byte_limit + 1)
    # The caller records/counts the possible sentinel byte before rejecting it.
    return raw


class HistoryRPC(activity.ProbeRPC):
    """Reuse existing exact-header and EIP-1967 validation, with a total budget."""

    def __init__(self, endpoint: str, transport: Callable, seeds: list[dict]):
        super().__init__(endpoint, transport, seeds)
        self.response_bytes = 0

    def call(self, method: str, params: list) -> tuple[Any, dict]:
        require(method in {"eth_chainId", "eth_getBlockByNumber", "eth_getStorageAt",
                           "eth_getCode", "eth_getTransactionReceipt"}, "rpc_method_not_permitted")
        require(len(self.ledger) < MAX_RPC_CALLS, "rpc_call_limit")
        remaining = MAX_TOTAL_RESPONSE_BYTES - self.response_bytes
        require(remaining > 0, "rpc_total_response_byte_limit")
        limit = min(MAX_RESPONSE_BYTES, remaining)
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params,
                 "status": "requested", "response_byte_limit": limit}
        self.ledger.append(entry)
        try:
            payload = {"jsonrpc": "2.0", "id": entry["id"], "method": method, "params": params}
            raw = (http_rpc(self.endpoint, payload, byte_limit=limit) if self.transport is http_rpc
                   else self.transport(self.endpoint, payload))
            require(type(raw) is bytes, "rpc_response_not_bytes")
            entry.update(response_bytes=len(raw), response_sha256=hashlib.sha256(raw).hexdigest())
            self.response_bytes += len(raw)
            require(self.response_bytes <= MAX_TOTAL_RESPONSE_BYTES, "rpc_total_response_byte_limit")
            require(len(raw) <= MAX_RESPONSE_BYTES, "rpc_response_byte_limit")
            value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(HistoryBlocked("rpc_nonfinite_json")))
            require(type(value) is dict and set(value) == {"jsonrpc", "id", "result"}, "rpc_invalid_reply_shape")
            require(value["jsonrpc"] == "2.0" and type(value["id"]) is int and value["id"] == entry["id"],
                    "rpc_reply_id_mismatch")
            entry["status"] = "response_shape_validated"
            return value["result"], entry
        except Exception:
            entry["status"] = "failed"
            raise

    def code_hash(self, address: str, block: int) -> dict:
        value, entry = self.call("eth_getCode", [address, hex(block)])
        require(type(value) is str and activity.HEX_BYTES.fullmatch(value), "invalid_runtime_bytecode_encoding")
        raw = bytes.fromhex(value[2:])
        entry["status"] = "validated"
        return {"address": address, "block": block, "code_present": bool(raw),
                "runtime_bytes": len(raw), "runtime_sha256": hashlib.sha256(raw).hexdigest(),
                "response_sha256": entry["response_sha256"],
                "hash_semantics": "SHA-256 of returned runtime bytes, not an Ethereum Keccak codeHash or source-equivalence proof."}

    def upgrade_receipt(self) -> dict:
        value, entry = self.call("eth_getTransactionReceipt", [UPGRADE_TRANSACTION])
        require(type(value) is dict, "upgrade_receipt_absent_or_not_object")
        require(type(value.get("transactionHash")) is str
                and value["transactionHash"].lower() == UPGRADE_TRANSACTION, "upgrade_receipt_transaction_mismatch")
        number = inventory._quantity(value.get("blockNumber"))
        require(number == UPGRADE_BLOCK, "upgrade_receipt_block_mismatch")
        block_hash = self.headers[UPGRADE_BLOCK]["hash"]
        require(type(value.get("blockHash")) is str and value["blockHash"].lower() == block_hash,
                "upgrade_receipt_block_hash_mismatch")
        require(inventory._quantity(value.get("status")) == 1, "upgrade_receipt_not_successful")
        transaction_index = inventory._quantity(value.get("transactionIndex"))
        logs = value.get("logs")
        require(type(logs) is list and 0 < len(logs) <= MAX_RECEIPT_LOGS, "upgrade_receipt_log_count_invalid")
        seen: set[int] = set()
        selected = None
        matching = 0
        for item in logs:
            require(type(item) is dict, "upgrade_receipt_log_not_object")
            index = inventory._quantity(item.get("logIndex"))
            require(index not in seen, "duplicate_receipt_log_index")
            seen.add(index)
            require(inventory._quantity(item.get("blockNumber")) == number
                    and inventory._quantity(item.get("transactionIndex")) == transaction_index,
                    "receipt_log_position_mismatch")
            require(type(item.get("blockHash")) is str and item["blockHash"].lower() == block_hash
                    and type(item.get("transactionHash")) is str
                    and item["transactionHash"].lower() == UPGRADE_TRANSACTION, "receipt_log_identity_mismatch")
            require(item.get("removed") is False, "receipt_log_removed_or_flag_absent")
            require(type(item.get("address")) is str and re.fullmatch(r"0x[0-9a-fA-F]{40}", item["address"]),
                    "receipt_log_address_invalid")
            topics = item.get("topics")
            require(type(topics) is list and len(topics) <= 4
                    and all(type(topic) is str and activity.HASH.fullmatch(topic) for topic in topics),
                    "receipt_log_topics_invalid")
            if item["address"].lower() == ADDRESS and topics and topics[0].lower() == UPGRADED_TOPIC:
                matching += 1
            if index != UPGRADE_LOG_INDEX:
                continue
            require(item["address"].lower() == ADDRESS, "upgrade_log_address_mismatch")
            require(len(topics) == 2 and topics[0].lower() == UPGRADED_TOPIC, "upgrade_log_topic_or_arity_mismatch")
            require(topics[1].lower() == "0x" + "00" * 12 + NEW_IMPLEMENTATION[2:],
                    "upgrade_log_implementation_mismatch")
            require(item.get("data") == "0x", "upgrade_log_unexpected_data")
            selected = {"exchange_address": ADDRESS, "transaction_hash": UPGRADE_TRANSACTION,
                        "block_number": number, "block_hash": block_hash,
                        "transaction_index": transaction_index, "log_index": index,
                        "event_topic": UPGRADED_TOPIC, "new_implementation": NEW_IMPLEMENTATION}
        require(selected is not None, "expected_upgrade_log_absent")
        require(matching == 1, "ambiguous_proxy_upgrades_in_receipt")
        entry["status"] = "validated"
        return {"native_identity": selected, "receipt_status": 1, "receipt_log_count": len(logs),
                "response_sha256": entry["response_sha256"],
                "scope": "One named successful transaction and one exact proxy Upgraded log, not a complete upgrade ledger."}


def validate_plan_blocks(plan: dict) -> None:
    require(plan["raw_footer_tail_header"]["number"] == RAW_TAIL_BLOCK
            and plan["last_included_header"]["number"] == CUTOFF_BLOCK,
            "source_plan_frozen_endpoints_differ")
    require(0 < OLD_REPORTED_CREATION_BLOCK < RAW_TAIL_BLOCK < NEW_REPORTED_CREATION_BLOCK
            < UPGRADE_BLOCK < CUTOFF_BLOCK, "historical_probe_block_order_invalid")


def probe(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, *,
          rpc_private_file: Path | None = None, transport: Callable = http_rpc,
          output: Path | None = None) -> dict:
    result = {"schema_version": "polymarket_v3_history_probe_v1", "status": "blocked",
              "created_utc": datetime.now(timezone.utc).isoformat(), "address": ADDRESS,
              "maximum_rpc_calls": MAX_RPC_CALLS, "maximum_response_bytes": MAX_RESPONSE_BYTES,
              "maximum_total_response_bytes": MAX_TOTAL_RESPONSE_BYTES,
              "collection_approved": False, "exchange_abi_admitted": False,
              "trade_logs_requested": False, "parquet_body_reads": 0,
              "whole_history_scan_performed": False, "gap_free_upgrade_history_certified": False,
              "first_deployment_certified": False, "backfill_start_admitted": False,
              "safe_backfill_start_block": None, "endpoint_disclosed": False,
              "sensitive_values_saved": False, "headers": [], "proxy_code_observations": [],
              "implementation_code_observations": [], "implementation_slots": [], "request_ledger": [],
              "limitations": [
                  "Creation blocks are externally reported candidates checked for code presence, not verified creation transactions.",
                  "Code already present at the earliest checked block gives only a latest bound on deployment, never the first deployment.",
                  "One known upgrade plus four storage-slot observations does not exclude intermediate or earlier implementations.",
                  "Runtime SHA-256 hashes are observations, not compiled-source equivalence or ABI/economic admission.",
                  "No trade data, collateral getter, decimals or settlement computation is requested or certified.",
                  "Per-call reads use at most the remaining response budget plus one oversize-detection byte; oversized replies fail closed."],
              "still_required_before_historical_admission": [
                  "Prove the proxy creation transaction and conservative historical collection start.",
                  "Collect a gap-free bounded Upgraded-event ledger from deployment through the cutoff, retaining successful zero intervals.",
                  "Bind each implementation era to independently verified source/runtime and its own BUY/SELL amount and fee semantics.",
                  "Verify historical collateral, decimals, token-family mappings and exact timestamps before admitting decoded fills."]}
    rpc = None
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    try:
        value, binding, source_inventory = activity.read_plan(plan_path, plan_sha256, plan_source_head)
        if output is not None:
            activity.validate_output(output, plan_path, private, source_inventory, value)
        validate_plan_blocks(value)
        result["source_plan_binding"] = binding
        source = inventory.code_state()
        require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head)
                and source.get("status") == "inspected" and source.get("head") == expected_head
                and source.get("tracked_worktree_clean") is True, "probe_source_head_or_clean_state_differs")
        result["probe_source"] = source
        endpoint, configuration_source = inventory.load_rpc_endpoint(private)
        rpc = HistoryRPC(endpoint, transport, [value[name] for name in
                         ("raw_footer_tail_header", "last_included_header", "first_excluded_header")])
        chain, entry = rpc.call("eth_chainId", [])
        require(inventory._quantity(chain) == 137, "rpc_wrong_chain")
        entry["status"] = "validated"
        blocks = sorted({OLD_REPORTED_CREATION_BLOCK - 1, OLD_REPORTED_CREATION_BLOCK,
                         NEW_REPORTED_CREATION_BLOCK, UPGRADE_BLOCK - 1, UPGRADE_BLOCK,
                         RAW_TAIL_BLOCK, CUTOFF_BLOCK, CUTOFF_BLOCK + 1})
        for block in blocks:
            result["headers"].append(rpc.header(block))
        for later in (OLD_REPORTED_CREATION_BLOCK, UPGRADE_BLOCK, CUTOFF_BLOCK + 1):
            require(rpc.headers[later]["parentHash"] == rpc.headers[later - 1]["hash"],
                    "adjacent_probe_headers_not_linked")
        for block in (OLD_REPORTED_CREATION_BLOCK - 1, OLD_REPORTED_CREATION_BLOCK, CUTOFF_BLOCK):
            result["proxy_code_observations"].append(rpc.code_hash(ADDRESS, block))
        require(result["proxy_code_observations"][-1]["code_present"], "proxy_code_absent_at_cutoff")
        present = [item["block"] for item in result["proxy_code_observations"] if item["code_present"]]
        result["earliest_queried_proxy_code_present_block"] = min(present)
        for address, creation in ((OLD_IMPLEMENTATION, OLD_REPORTED_CREATION_BLOCK),
                                  (NEW_IMPLEMENTATION, NEW_REPORTED_CREATION_BLOCK)):
            pair = [rpc.code_hash(address, block) for block in (creation, CUTOFF_BLOCK)]
            require(all(item["code_present"] for item in pair), "implementation_code_absent_at_queried_block")
            require(pair[0]["runtime_sha256"] == pair[1]["runtime_sha256"], "implementation_runtime_changed_between_observations")
            result["implementation_code_observations"].extend(pair)
        for block, expected in ((UPGRADE_BLOCK - 1, OLD_IMPLEMENTATION), (UPGRADE_BLOCK, NEW_IMPLEMENTATION),
                                (RAW_TAIL_BLOCK, OLD_IMPLEMENTATION), (CUTOFF_BLOCK, NEW_IMPLEMENTATION)):
            observation = rpc.implementation(block)
            require(observation["implementation_address"] == expected, "implementation_slot_differs_from_expected_era")
            result["implementation_slots"].append(observation)
        result["known_upgrade_receipt"] = rpc.upgrade_receipt()
        for block in (UPGRADE_BLOCK - 1, UPGRADE_BLOCK, CUTOFF_BLOCK, CUTOFF_BLOCK + 1):
            rpc.header(block)
        planner.reopen_inputs(source_inventory)
        inventory_binding = value["inventory_binding"]
        require(inventory.path_identity(Path(inventory_binding["path"])) == inventory_binding["identity"],
                "source_inventory_changed_during_probe")
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_changed_during_probe")
        require(source == inventory.code_state(), "probe_source_changed_during_probe")
        result.update(status="bounded_v3_history_probe_complete", final_input_identity_unchanged=True,
                      configuration_source=configuration_source, reopened_headers_unchanged=True)
    except (HistoryBlocked, activity.ProbeBlocked, planner.PlanBlocked, inventory.ReadinessError) as exc:
        result["error_code"] = str(exc)
    except Exception:
        result["error_code"] = "probe_input_rpc_or_configuration_failure"
    finally:
        if rpc is not None:
            result["request_ledger"] = rpc.ledger
        result["rpc_calls_attempted"] = len(result["request_ledger"])
        result["response_bytes_received"] = rpc.response_bytes if rpc is not None else 0
    return result


def publish(output: Path, result: dict) -> None:
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= MAX_OUTPUT_BYTES, "output_byte_limit")
    inventory.write_exclusive(output, result)
    with output.open("rb") as stream:
        reopened = stream.read(MAX_OUTPUT_BYTES + 1)
    require(len(reopened) <= MAX_OUTPUT_BYTES and json.loads(reopened) == result, "published_output_reopen_differs")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", type=Path, required=True)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--source-plan-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production probing requires the canonical mounted EC2 checkout.")
    # Refuse existing/overlapping outputs before any RPC or blocked-result write.
    value, _, source_inventory = activity.read_plan(args.source_plan, args.source_plan_sha256,
                                                    args.source_plan_source_head)
    activity.validate_output(args.output, args.source_plan, args.rpc_private_file, source_inventory, value)
    result = probe(args.source_plan, args.source_plan_sha256, args.source_plan_source_head, args.expected_head,
                   rpc_private_file=args.rpc_private_file, output=args.output)
    publish(args.output, result)
    print(json.dumps({"status": result["status"], "rpc_calls_attempted": result["rpc_calls_attempted"],
                      "collection_approved": False, "exchange_abi_admitted": False}, sort_keys=True))
    return 0 if result["status"] == "bounded_v3_history_probe_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
