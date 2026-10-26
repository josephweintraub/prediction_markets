"""Read-only exact-block plan for the approved Polymarket sample extension.

Reads a hash-bound readiness inventory, reopens file identities, and makes at
most 100 header/code RPC calls. It never requests logs or approves collection,
source completeness, exchange ABIs, or an estimated final research sample.
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

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory

END_EXCLUSIVE = "2026-10-02T00:00:00Z"
MAX_RPC_CALLS = 100
EXCHANGES = (
    ("legacy", "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"),
    ("legacy_negative_risk", "0xc5d563a36ae78145c45a50134d48a1215220f80a"),
    ("v2", "0xe111180000d2663c0091e4f400237545b87b996b"),
    ("v2_negative_risk", "0xe2222d279d744050d28e00520010520000310f59"),
    ("v3_candidate", "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"),
)
INVENTORY_FIELDS = {
    "schema_version", "status", "created_utc", "scope", "scientific_sample_decisions_made",
    "parquet_body_reads", "limits", "consumed", "inputs", "repair_manifest", "rpc_readiness",
    "code_before", "code_after", "code_state_unchanged", "final_input_identity_unchanged",
    "resources", "limitations",
}


class PlanBlocked(ValueError):
    """Fixed, non-sensitive evidence code."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise PlanBlocked(code)


def utc_cutoff(value: str) -> int:
    require(type(value) is str and bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value)),
            "invalid_end_exclusive_utc")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise PlanBlocked("invalid_end_exclusive_utc") from None
    require(value == END_EXCLUSIVE, "end_exclusive_differs_from_user_contract")
    return int(parsed.timestamp())


def validate_inventory(value: Any, expected_source_head: str) -> None:
    require(type(value) is dict and set(value) == INVENTORY_FIELDS, "inventory_schema_keys_differ")
    require(value["schema_version"] == "polymarket_refresh_readiness_inventory_v1", "inventory_version_differs")
    require(value["status"] in ("readiness_inventory_complete", "readiness_inventory_incomplete"),
            "inventory_status_not_admitted")
    require(value["scientific_sample_decisions_made"] is False and type(value["parquet_body_reads"]) is int
            and value["parquet_body_reads"] == 0, "inventory_scope_differs")
    require(value["code_state_unchanged"] is True and value["final_input_identity_unchanged"] is True,
            "inventory_identity_not_accepted")
    require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_source_head or "")), "invalid_inventory_source_head")
    for key in ("code_before", "code_after"):
        code = value[key]
        require(type(code) is dict and code.get("status") == "inspected"
                and code.get("head") == expected_source_head and code.get("tracked_worktree_clean") is True,
                "inventory_source_head_or_clean_state_differs")
    require(value["code_before"] == value["code_after"], "inventory_code_binding_differs")
    inputs = value["inputs"]
    require(type(inputs) is dict and set(inputs) == set(inventory.DEFAULT_PATHS), "inventory_input_keys_differ")
    total_files = 0
    for entry in inputs.values():
        require(type(entry) is dict and type(entry.get("path")) is str
                and entry.get("status") in ("inspected", "missing")
                and entry.get("final_identity_unchanged") is True, "inventory_input_not_admitted")
        if entry["status"] == "missing":
            require(type(entry.get("identity")) is dict
                    and entry["identity"].get("status") in ("missing", "dangling_symlink"),
                    "inventory_missing_identity_absent")
            continue
        files = entry.get("files")
        require(type(files) is list and 0 < len(files) <= inventory.LIMITS["maximum_files"]
                and type(entry.get("file_count")) is int and entry["file_count"] == len(files),
                "inventory_file_count_invalid")
        total_files += len(files)
        require(total_files <= inventory.LIMITS["maximum_files"], "inventory_total_file_limit")
        require(type(entry.get("topology")) is dict and "input" in entry["topology"],
                "inventory_topology_absent")
        for item in files:
            require(type(item) is dict and type(item.get("path")) is str and type(item.get("identity")) is dict
                    and bool(re.fullmatch(r"[0-9a-f]{64}", item.get("footer_sha256", "")))
                    and type(item.get("extrema")) is dict, "inventory_file_binding_invalid")


def read_inventory(path: Path, expected_sha256: str, expected_source_head: str) -> tuple[dict, dict]:
    require(bool(re.fullmatch(r"[0-9a-f]{64}", expected_sha256 or "")), "invalid_inventory_digest")
    before = inventory.path_identity(path)
    require(before["status"] == "present" and not before["is_symlink"], "inventory_not_regular")
    require(0 < before["target"]["bytes"] <= inventory.LIMITS["maximum_output_bytes"], "inventory_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(inventory.LIMITS["maximum_output_bytes"] + 1)
    require(before == inventory.path_identity(path), "inventory_changed_during_read")
    require(len(raw) <= inventory.LIMITS["maximum_output_bytes"]
            and hashlib.sha256(raw).hexdigest() == expected_sha256, "inventory_digest_differs")
    try:
        value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(PlanBlocked("inventory_nonfinite_json")))
    except PlanBlocked:
        raise
    except Exception:
        raise PlanBlocked("inventory_json_invalid") from None
    validate_inventory(value, expected_source_head)
    return value, {"path": str(path), "sha256": expected_sha256,
                   "source_head": expected_source_head, "identity": before}


def reopen_inputs(value: dict) -> None:
    for entry in value["inputs"].values():
        path = Path(entry["path"])
        if entry["status"] == "missing":
            require(inventory.path_identity(path) == entry["identity"], "missing_source_identity_changed")
            continue
        files, topology = inventory.collect_parquet_files(path)
        require(topology == entry["topology"] and [str(item) for item in files] == [item["path"] for item in entry["files"]],
                "source_topology_changed_since_inventory")
        require(all(inventory.path_identity(Path(item["path"])) == item["identity"] for item in entry["files"]),
                "source_identity_changed_since_inventory")
    manifest = value["repair_manifest"]
    if manifest is not None:
        require(type(manifest) is dict and manifest.get("status") == "inspected"
                and manifest.get("final_identity_unchanged") is True
                and inventory.path_identity(Path(manifest["path"])) == manifest["identity"],
                "repair_manifest_identity_changed")


def validate_output(path: Path, value: dict, inventory_path: Path, private_file: Path) -> None:
    require(not path.exists() and not path.is_symlink(), "output_already_exists")
    output = path.resolve()
    sources = [inventory_path, private_file, *(Path(entry["path"]) for entry in value["inputs"].values())]
    if value["repair_manifest"] is not None:
        sources.append(Path(value["repair_manifest"]["path"]))
    for source in sources:
        resolved = source.resolve()
        require(output != resolved and resolved not in output.parents, "output_overlaps_bound_input")


def raw_append_start(value: dict, explicit_start: int | None) -> dict:
    raw = value["inputs"]["raw_events"]
    maxima = []
    known = raw["status"] == "inspected"
    if known:
        for item in raw["files"]:
            available = [item["extrema"][name] for name in ("block_number", "blockNumber") if name in item["extrema"]]
            if len(available) != 1:
                known = False
                break
            clock = available[0]
            maximum = clock.get("declared_global_maximum")
            if (clock.get("all_nonempty_nonnull_groups_have_minmax") is not True
                    or type(maximum) is not int or maximum <= 0):
                known = False
                break
            maxima.append(maximum)
    if known and maxima:
        require(explicit_start is None, "explicit_start_forbidden_with_known_raw_footer_maximum")
        maximum = max(maxima)
        return {"start_block": maximum + 1, "raw_footer_declared_maximum_block": maximum,
                "start_authority": "Complete available raw footer declarations plus one; provisional, not native completeness."}
    require(type(explicit_start) is int and explicit_start > 0, "raw_footer_maximum_unknown_explicit_start_required")
    return {"start_block": explicit_start, "raw_footer_declared_maximum_block": None,
            "start_authority": "Explicit start supplied because raw footer maximum is unknown; historical completeness unverified."}


class HeaderRPC:
    def __init__(self, private_file: Path, transport: Callable = inventory._http_rpc):
        self.endpoint, self.configuration_source = inventory.load_rpc_endpoint(private_file)
        self.transport = transport
        self.ledger: list[dict] = []
        self.headers: dict[int, dict] = {}

    def call(self, method: str, params: list[Any]) -> tuple[Any, dict]:
        require(method in {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_getCode"},
                "rpc_method_not_permitted")
        require(len(self.ledger) < MAX_RPC_CALLS, "rpc_call_limit")
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params, "status": "requested"}
        self.ledger.append(entry)
        try:
            reply = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"], "method": method, "params": params})
            require(type(reply) is dict and set(reply) == {"jsonrpc", "id", "result"}, "rpc_invalid_reply_shape")
            require(reply["jsonrpc"] == "2.0" and type(reply["id"]) is int and reply["id"] == entry["id"],
                    "rpc_reply_id_mismatch")
            return reply["result"], entry
        except Exception:
            entry["status"] = "failed"
            raise

    def quantity(self, method: str) -> int:
        result, entry = self.call(method, [])
        number = inventory._quantity(result)
        entry.update(status="validated", result_integer=number)
        return number

    def header(self, number: int, *, reopen: bool = False) -> dict:
        if number in self.headers and not reopen:
            return self.headers[number]
        result, entry = self.call("eth_getBlockByNumber", [hex(number), False])
        require(type(result) is dict, "rpc_header_not_object")
        returned = inventory._quantity(result.get("number"))
        timestamp = inventory._quantity(result.get("timestamp"))
        require(returned == number, "rpc_header_number_mismatch")
        require(0 <= timestamp <= 253402300799 and (number == 0 or timestamp > 0), "rpc_invalid_header_timestamp")
        require(type(result.get("hash")) is str and bool(inventory.HEX_HASH.fullmatch(result["hash"]))
                and type(result.get("parentHash")) is str and bool(inventory.HEX_HASH.fullmatch(result["parentHash"])),
                "rpc_invalid_header_hash")
        header = {"number": number, "hash": result["hash"], "parentHash": result["parentHash"],
                  "timestamp": timestamp}
        if number in self.headers:
            require(header == self.headers[number], "rpc_reopened_header_changed")
        for other_number, other in self.headers.items():
            require(not (other_number < number and other["timestamp"] > timestamp)
                    and not (other_number > number and other["timestamp"] < timestamp),
                    "rpc_observed_nonmonotone_header_time")
        self.headers[number] = header
        entry.update(status="validated", header=header)
        return header

    def code_presence(self, address: str, block: int) -> bool:
        result, entry = self.call("eth_getCode", [address, hex(block)])
        require(type(result) is str and bool(re.fullmatch(r"0x(?:[0-9a-fA-F]{2})*", result)),
                "rpc_invalid_code_result")
        presence = result != "0x"
        entry.update(status="validated", code_present=presence)
        return presence


def cutoff_boundary(rpc: HeaderRPC, cutoff_seconds: int) -> tuple[dict, dict, int]:
    require(rpc.quantity("eth_chainId") == 137, "rpc_wrong_chain")
    latest = rpc.quantity("eth_blockNumber")
    require(latest > 0, "rpc_nonpositive_latest_block")
    first = rpc.header(0)
    tip = rpc.header(latest)
    require(first["timestamp"] < cutoff_seconds <= tip["timestamp"], "cutoff_not_bracketed_by_chain")
    lower, upper = 0, latest
    while lower + 1 < upper:
        middle = (lower + upper) // 2
        if rpc.header(middle)["timestamp"] < cutoff_seconds:
            lower = middle
        else:
            upper = middle
    previous = rpc.header(lower)
    cutoff = rpc.header(upper)
    require(upper == lower + 1 and previous["timestamp"] < cutoff_seconds <= cutoff["timestamp"],
            "cutoff_adjacency_or_time_proof_failed")
    require(cutoff["parentHash"] == previous["hash"], "cutoff_parent_hash_proof_failed")
    return previous, cutoff, latest


def plan_refresh(inventory_path: Path, inventory_sha256: str, inventory_source_head: str,
                 expected_head: str, *, end_exclusive: str = END_EXCLUSIVE,
                 start_block: int | None = None, rpc_private_file: Path | None = None,
                 transport: Callable = inventory._http_rpc) -> dict:
    result: dict[str, Any] = {
        "schema_version": "polymarket_refresh_source_plan_v1", "status": "blocked",
        "created_utc": datetime.now(timezone.utc).isoformat(), "maximum_rpc_calls": MAX_RPC_CALLS,
        "trade_logs_requested": False, "parquet_body_reads": 0, "endpoint_disclosed": False,
        "collection_approved": False, "exchange_abi_admitted": False,
        "request_ledger": [], "limitations": [
            "A source plan is not collection approval, historical coverage or final-sample certification.",
            "Raw maxima are footer declarations; a provisional append range does not prove earlier native-log completeness.",
            "Code presence at one block does not establish ABI, deployment history or exchange-address completeness.",
            "Binary search assumes nondecreasing canonical block timestamps; every queried header and the adjacent cutoff proof are checked.",
            "Only selected public header fields and code-presence indicators are saved; no endpoint or bytecode is published."],
    }
    rpc = None
    try:
        cutoff_seconds = utc_cutoff(end_exclusive)
        result.update(end_exclusive_utc=end_exclusive, end_exclusive_unix_seconds=cutoff_seconds)
        value, binding = read_inventory(inventory_path, inventory_sha256, inventory_source_head)
        result["inventory_binding"] = binding
        reopen_inputs(value)
        source_before = inventory.code_state()
        require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_head or "")) and source_before.get("status") == "inspected"
                and source_before.get("head") == expected_head and source_before.get("tracked_worktree_clean") is True,
                "planner_source_head_or_clean_state_differs")
        result["planner_source"] = source_before
        start = raw_append_start(value, start_block)
        result["provisional_append_start"] = start
        rpc = HeaderRPC(rpc_private_file or Path.home() / ".polygon_rpc_url", transport)
        previous, cutoff, latest = cutoff_boundary(rpc, cutoff_seconds)
        require(start["start_block"] <= latest + 1, "append_start_exceeds_latest_plus_one")
        raw_maximum = start["raw_footer_declared_maximum_block"]
        if raw_maximum is not None:
            result["raw_footer_tail_header"] = rpc.header(raw_maximum)
        result.update(latest_block_observed=latest, last_included_header=previous,
                      first_excluded_header=cutoff,
                      cutoff_proof={"adjacent_block_numbers": True, "parent_hash_matches": True,
                                    "last_time_strictly_before_cutoff": True,
                                    "first_time_at_or_after_cutoff": True})
        result["exchange_code_presence"] = [
            {"label": label, "address": address, "block": previous["number"],
             "code_present": rpc.code_presence(address, previous["number"])}
            for label, address in EXCHANGES]
        rpc.header(previous["number"], reopen=True)
        rpc.header(cutoff["number"], reopen=True)
        reopen_inputs(value)
        require(inventory.path_identity(inventory_path) == binding["identity"], "inventory_changed_during_plan")
        require(source_before == inventory.code_state(), "planner_source_changed_during_plan")
        result.update(status="source_plan_ready_for_review", final_input_identity_unchanged=True,
                      cutoff_headers_reopened_unchanged=True,
                      configuration_source=rpc.configuration_source,
                      provisional_append_blocks=max(0, previous["number"] - start["start_block"] + 1),
                      provisional_append_range_empty=start["start_block"] > previous["number"])
    except (PlanBlocked, inventory.ReadinessError) as exc:
        result["error_code"] = str(exc)
    except Exception:
        result["error_code"] = "source_plan_input_rpc_or_configuration_failure"
    finally:
        if rpc is not None:
            result["request_ledger"] = rpc.ledger
            result["rpc_calls_attempted"] = len(rpc.ledger)
            if result["status"] == "blocked" and rpc.ledger and rpc.ledger[-1]["status"] != "failed":
                rpc.ledger[-1]["validation_outcome"] = "plan_blocked_after_this_response"
        else:
            result["rpc_calls_attempted"] = 0
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, type=Path)
    parser.add_argument("--inventory-sha256", required=True)
    parser.add_argument("--inventory-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--end-exclusive", default=END_EXCLUSIVE)
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production planning requires the canonical mounted EC2 checkout.")
    # Admit the destination before any RPC. Do not write even a failure receipt
    # into a source directory, inventory file, or private configuration file.
    try:
        value, _ = read_inventory(args.inventory, args.inventory_sha256, args.inventory_source_head)
        validate_output(args.output, value, args.inventory, args.rpc_private_file)
    except (PlanBlocked, inventory.ReadinessError):
        parser.error("Input/output admission failed; no RPC requests or output writes were made.")
    result = plan_refresh(args.inventory, args.inventory_sha256, args.inventory_source_head, args.expected_head,
                          end_exclusive=args.end_exclusive, start_block=args.start_block,
                          rpc_private_file=args.rpc_private_file)
    inventory.write_exclusive(args.output, result)
    print(json.dumps({"status": result["status"], "rpc_calls_attempted": result["rpc_calls_attempted"],
                      "trade_logs_requested": False}, sort_keys=True))
    return 0 if result["status"] == "source_plan_ready_for_review" else 2


if __name__ == "__main__":
    raise SystemExit(main())
