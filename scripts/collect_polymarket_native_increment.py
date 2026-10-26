"""Serial, bounded, resumable native-log collection; RAW ONLY, not sample admission.

Reuse the reviewed source-plan and native-payload validators. A successful shard
is committed by its immutable receipt, including explicit zero counts for every
requested exchange. Missing/failed requests never become COMPLETE. No ABI,
economic direction, payout, deduplication, or exact interior clock is inferred.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import stat
import sys
import time
from typing import Callable
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_increment_capacity as pilot

require = pilot.require
FREE_FLOOR = 20_000_000_000
MAX_ROWS = 5_000_000
MAX_CALLS = 300_000
MAX_OUTPUT = 32_000_000_000
MAX_RESPONSES = 512_000_000_000
MAX_RUNTIME = 86_400
MAX_RAM = 16 * 1024**3
TRANSIENT_RESERVE = 256 * 1024**2
MAX_WINDOWS = 50_000
CONTROL_RESERVE = 4 * 1024**2
MAX_CONTROL = 64 * 1024**2
MODES = ("append", "v3-historical", "endpoint-overlap")


def json_bytes(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def load_json(path: Path, maximum: int = MAX_CONTROL) -> dict:
    require(not path.is_symlink() and path.is_file() and 0 < path.stat().st_size <= maximum,
            "checkpoint_control_not_regular_or_bounded")
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(pilot.PilotBlocked("checkpoint_nonfinite_json")))
    require(type(value) is dict, "checkpoint_control_not_object")
    return value


def publish(path: Path, raw: bytes) -> None:
    """Atomic no-replace publication; a killed write leaves only a .pending file."""
    require(not path.exists() and not path.is_symlink(), "checkpoint_already_exists")
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".pending")
    with temporary.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(temporary, path)  # Atomic and fails if another writer published first.
    temporary.unlink()
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def windows(first: int, last: int, width: int) -> list[dict]:
    require(type(first) is int and type(last) is int and 0 < first <= last
            and type(width) is int and 1 <= width <= 100, "requested_range_or_window_invalid")
    require((last - first) // width + 1 <= MAX_WINDOWS, "window_count_limit")
    return [{"window": f"b{start:012d}-{min(start + width - 1, last):012d}",
             "first_block": start, "last_block": min(start + width - 1, last),
             "blocks": min(width, last - start + 1)} for start in range(first, last + 1, width)]


def requested_addresses(mode: str, first: int, last: int, plan: dict) -> tuple[str, ...]:
    require(mode in MODES, "unsupported_collection_mode")
    require(type(first) is int and type(last) is int and 0 < first <= last, "requested_range_invalid")
    raw_tail = plan["provisional_append_start"]["raw_footer_declared_maximum_block"]
    if mode == "v3-historical":
        require(last <= raw_tail, "historical_range_exceeds_raw_tail")
        return (pilot.ADDRESSES[-1],)
    if mode == "endpoint-overlap":
        require(first == raw_tail - 99 and last == raw_tail, "endpoint_overlap_must_be_fixed_100_blocks")
    else:
        require(first >= plan["provisional_append_start"]["start_block"]
                and last <= plan["last_included_header"]["number"], "append_range_not_within_missing_interval")
    return pilot.ADDRESSES


def limits(calls: int, output: int, runtime: int, rows: int, ram: int, responses: int) -> dict:
    for value in (calls, output, runtime, rows, ram, responses):
        require(type(value) is int and value > 0, "positive_integer_caps_required")
    require(calls <= MAX_CALLS and CONTROL_RESERVE * 2 <= output <= MAX_OUTPUT
            and runtime <= MAX_RUNTIME and rows <= MAX_ROWS
            and TRANSIENT_RESERVE <= ram <= MAX_RAM
            and pilot.MAX_RESPONSE_BYTES <= responses <= MAX_RESPONSES, "stage_cap_outside_hard_bound")
    return {"rpc_calls": calls, "output_bytes": output, "runtime_seconds": runtime, "rows": rows,
            "window_rows": pilot.MAX_WINDOW_ROWS, "response_bytes": pilot.MAX_RESPONSE_BYTES,
            "attempts_per_window": 3, "workers": 1, "free_floor_bytes": FREE_FLOOR, "spill_bytes": 0,
            "ram_state_and_transient_budget_bytes": ram, "transient_reserve_bytes": TRANSIENT_RESERVE,
            "total_response_bytes": responses, "production_main_address_space_limit_bytes": ram,
            "production_main_minimum_available_ram_bytes": ram}


def check_destination(output: Path, original: dict, plan_path: Path, plan: dict, private: Path) -> None:
    # Unlike the pilot, a matching owned directory may be resumed. Every input
    # exclusion remains identical, including children of a bound input tree.
    sources = [plan_path, Path(plan["inventory_binding"]["path"]), private,
               *(Path(entry["path"]) for entry in original["inputs"].values())]
    if original["repair_manifest"] is not None:
        sources.append(Path(original["repair_manifest"]["path"]))
    resolved = output.resolve()
    require(not output.is_symlink(), "output_is_symlink")
    for source in sources:
        bound = source.resolve()
        require(resolved != bound and bound not in resolved.parents and resolved not in bound.parents,
                "output_overlaps_bound_input")
    require(output.parent.is_dir() and not output.parent.is_symlink(), "output_parent_not_regular_directory")


@contextmanager
def run_lock(output: Path):
    """Kernel lock is released even on interruption; no unsafe stale-lock removal."""
    fd = os.open(output / "LOCK", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), "lock_not_regular")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise pilot.PilotBlocked("another_collector_holds_run_lock") from None
        yield
    finally:
        os.close(fd)


def account_existing(output: Path, cap: int) -> int:
    total = 0
    for root, directories, filenames in os.walk(output, followlinks=False):
        require(all(not (Path(root) / name).is_symlink() for name in directories), "checkpoint_symlink_directory")
        for name in filenames:
            path = Path(root) / name
            require(not path.is_symlink() and path.is_file(), "checkpoint_nonregular_file")
            total += path.stat().st_size
            require(total + CONTROL_RESERVE <= cap, "existing_checkpoint_exceeds_byte_budget")
    return total


class Budget:
    def __init__(self, output: Path, caps: dict, existing: int, clock: Callable, elapsed: float = 0):
        self.output, self.caps, self.used, self.clock = output, caps, existing, clock
        self.start = clock()
        self.previous_elapsed = elapsed
        self.calls = self.rows = self.response_bytes = 0

    def check(self, extra: int = 0, *, reserve: bool = True) -> None:
        require(self.previous_elapsed + self.clock() - self.start < self.caps["runtime_seconds"], "stage_runtime_limit")
        require(self.used + extra + (CONTROL_RESERVE if reserve else 0) <= self.caps["output_bytes"],
                "stage_output_byte_limit")
        require(shutil.disk_usage(self.output).free - extra >= FREE_FLOOR, "stage_free_space_floor")
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = int(rss if sys.platform == "darwin" else rss * 1024)
        require(rss_bytes <= self.caps["ram_state_and_transient_budget_bytes"], "process_peak_ram_limit")

    def write(self, path: Path, raw: bytes, *, reserve: bool = True) -> dict:
        self.check(len(raw), reserve=reserve)
        publish(path, raw)
        self.used += len(raw)
        require(path.read_bytes() == raw, "published_checkpoint_bytes_differ")
        return {"path": str(path.relative_to(self.output)), "bytes": len(raw), "sha256": digest(raw)}


class TransportRetry(Exception):
    """A transport attempt failed; never retains an endpoint-bearing exception."""


class CollectionRPC(pilot.PilotRPC):
    """Reviewed header validation, but stage-wide finite calls and no secret errors."""
    def __init__(self, private: Path, transport: Callable, budget: Budget, session: Path):
        super().__init__(private, transport)
        self.budget = budget
        self.session = session

    def header(self, number: int, *, reopen: bool = False) -> dict:
        return super().header(number, reopen=reopen or number in self.headers)

    def call(self, method: str, params: list):
        require(method in {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}, "rpc_method_not_permitted")
        self.budget.check()
        require(self.budget.calls < self.budget.caps["rpc_calls"], "stage_rpc_call_limit")
        require(self.budget.response_bytes + pilot.MAX_RESPONSE_BYTES + 1 <= self.budget.caps["total_response_bytes"],
                "stage_response_byte_limit")
        identifier = self.budget.calls + 1
        entry = {"id": identifier, "method": method, "params": params, "status": "requested"}
        self.budget.write(self.session / "rpc" / f"r{identifier:09d}.START.json", json_bytes(entry))
        self.budget.calls += 1
        self.ledger.append(entry)
        try:
            try:
                raw = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"], "method": method, "params": params})
            except Exception:
                raise TransportRetry("rpc_transport_failure") from None
            self.budget.check()
            require(type(raw) is bytes, "rpc_response_not_bytes")
            self.budget.response_bytes += len(raw)
            entry.update(response_bytes=len(raw), response_sha256=digest(raw))
            require(len(raw) <= pilot.MAX_RESPONSE_BYTES, "rpc_response_byte_limit")
            reply = json.loads(raw, object_pairs_hook=inventory.unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(pilot.PilotBlocked("rpc_nonfinite_json")))
            require(type(reply) is dict and set(reply) == {"jsonrpc", "id", "result"}, "rpc_invalid_reply_shape")
            require(reply["jsonrpc"] == "2.0" and type(reply["id"]) is int and reply["id"] == entry["id"],
                    "rpc_reply_id_mismatch")
            self.budget.write(self.session / "rpc" / f"r{identifier:09d}.FINISH.json",
                              json_bytes({**entry, "status": "response_shape_validated"}))
            return reply["result"], entry
        except Exception:
            entry["status"] = "failed"
            if "response_bytes" not in entry:
                # A timeout can occur after a full response was received by the
                # transport. The live retry and later resume charge identically.
                entry.update(response_bytes=pilot.MAX_RESPONSE_BYTES + 1,
                             response_size_is_conservative_bound=True)
                self.budget.response_bytes += pilot.MAX_RESPONSE_BYTES + 1
            finish = self.session / "rpc" / f"r{identifier:09d}.FINISH.json"
            if not finish.exists():
                self.budget.write(finish, json_bytes(entry), reserve=False)
            raise


def parquet_payload(rows: list[dict]) -> bytes:
    """Same reviewed raw schema, with uint64 request IDs for multi-window stages."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([(key, pa.bool_() if key == "removed" else pa.list_(pa.string()) if key == "topics"
                         else pa.string()) for key in sorted(pilot.LOG_FIELDS | pilot.PROVENANCE_FIELDS)] +
                       [("block_number", pa.uint64()), ("log_index", pa.uint64()), ("transaction_index", pa.uint64()),
                        ("exchange_address", pa.string()), ("window", pa.string()), ("source_request_id", pa.uint64()),
                        ("provider_block_timestamp_present", pa.bool_())])
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression="zstd", row_group_size=10_000)
    payload = sink.getvalue().to_pybytes()
    reopened = pq.read_table(pa.BufferReader(payload))
    require(reopened.schema == schema and reopened.to_pylist() == rows, "parquet_roundtrip_differs")
    return payload


def native_rows(path: Path, window: dict, receipt: dict, shared_keys: dict, transactions: dict,
                addresses: tuple[str, ...]) -> tuple[list[dict], dict]:
    import pyarrow.parquet as pq
    require(not path.is_symlink() and path.is_file() and path.stat().st_size == receipt["payload"]["bytes"],
            "resume_payload_size_or_type_differs")
    raw = path.read_bytes()
    require(digest(raw) == receipt["payload"]["sha256"], "resume_payload_hash_differs")
    table = pq.read_table(path)
    require(table.num_rows <= pilot.MAX_WINDOW_ROWS, "resume_window_row_limit")
    rows = table.to_pylist()
    raw_logs = []
    for row in rows:
        require(row.get("window") == window["window"] and row.get("source_request_id") == receipt["log_request_id"],
                "resume_row_request_binding_differs")
        require(type(row.get("provider_block_timestamp_present")) is bool, "resume_provenance_presence_invalid")
        log = {key: row[key] for key in pilot.LOG_FIELDS}
        if row["provider_block_timestamp_present"]:
            log["blockTimestamp"] = row["blockTimestamp"]
        else:
            require(row.get("blockTimestamp") is None, "resume_absent_provenance_not_null")
        raw_logs.append(log)
    headers = {item["number"]: item for item in (receipt["first_header"], receipt["last_header"])}
    validated, evidence = pilot.validate_logs(raw_logs, window, headers, receipt["log_request_id"],
                                             shared_keys=shared_keys, shared_transactions=transactions)
    require(validated == rows and all(row["exchange_address"] in addresses for row in rows), "resume_row_schema_or_roster_differs")
    return rows, evidence


def verify_success_ledger(output: Path, directory: Path, receipt: dict, window: dict,
                          addresses: tuple[str, ...], topics: list[str], rpc_starts: dict) -> None:
    reference = receipt["request_ledger"]
    path = directory / "REQUESTS.json"
    require(reference["path"] == str(path.relative_to(output)) and not path.is_symlink() and path.is_file()
            and path.stat().st_size == reference["bytes"] <= MAX_CONTROL
            and digest(path.read_bytes()) == reference["sha256"], "resume_request_ledger_hash_differs")
    calls = load_json(path).get("calls")
    require(type(calls) is list and len(calls) == 4
            and [item.get("method") for item in calls] ==
            ["eth_getBlockByNumber", "eth_getBlockByNumber", "eth_getLogs", "eth_getBlockByNumber"]
            and all(item.get("status") == "validated" for item in calls), "resume_terminal_request_ledger_invalid")
    expected_params = [[hex(window["first_block"]), False], [hex(window["last_block"]), False],
                       [{"fromBlock": hex(window["first_block"]), "toBlock": hex(window["last_block"]),
                         "address": list(addresses), "topics": [topics]}], [hex(window["last_block"]), False]]
    require([item["params"] for item in calls] == expected_params
            and calls[2]["id"] == receipt["log_request_id"], "resume_request_predicate_differs")
    for call in calls:
        start = rpc_starts.get(call["id"])
        require(start is not None and start["value"]["method"] == call["method"]
                and start["value"]["params"] == call["params"], "resume_rpc_start_binding_differs")
        finish_path = start["path"].with_name(start["path"].name.replace(".START.json", ".FINISH.json"))
        finished = load_json(finish_path)
        require(finished.get("id") == call["id"] and finished.get("method") == call["method"]
                and finished.get("params") == call["params"] and finished.get("status") == "response_shape_validated"
                and finished.get("response_sha256") == call["response_sha256"]
                and finished.get("response_bytes") == call["response_bytes"], "resume_rpc_finish_binding_differs")


def interval_receipts(window: dict, evidence: dict, addresses: tuple[str, ...]) -> list[dict]:
    counts = {item["address"]: item["rows"] for item in evidence["counts"]}
    return [{"address": address, "topic": pilot.EXPECTED_TOPICS[address],
             "first_block": window["first_block"], "last_block": window["last_block"],
             "rows": counts[address], "status": "success_zero_logs" if counts[address] == 0 else "success_logs"}
            for address in addresses]


def sized(value) -> int:
    """Conservative owned-map accounting, counting shared strings more than once."""
    return sys.getsizeof(value) + (sum(sized(item) for item in value) if type(value) is tuple else 0)


def merge_identity_state(keys: dict, transactions: dict, new_keys: dict, new_transactions: dict,
                         item_bytes: int, caps: dict, *, commit: bool) -> int:
    for key, value in new_keys.items():
        if key in keys:
            require(keys[key] != value, "duplicate_native_log_key_across_windows")
            raise pilot.PilotBlocked("conflicting_native_log_key_across_windows")
    for key, value in new_transactions.items():
        require(key not in transactions or transactions[key] == value,
                "inconsistent_transaction_position_across_windows")
    extra = sum(sized(key) + sized(value) for key, value in new_keys.items())
    extra += sum(sized(key) + sized(value) for key, value in new_transactions.items() if key not in transactions)
    # Python dictionary allocation can grow at insertion. Reserve 128 bytes per
    # combined entry in addition to conservatively counted key/value objects.
    entries = len(keys) + len(new_keys) + len(transactions) + len(new_transactions)
    require(len(keys) + len(new_keys) <= caps["rows"] and entries <= 2 * caps["rows"], "identity_state_row_limit")
    require(item_bytes + extra + entries * 128 + TRANSIENT_RESERVE <= caps["ram_state_and_transient_budget_bytes"],
            "identity_state_ram_limit")
    if commit:
        keys.update(new_keys)
        transactions.update(new_transactions)
    return item_bytes + extra


def verify_coverage(records: list[dict], planned: list[dict], addresses: tuple[str, ...]) -> None:
    require(len(records) == len(planned), "incomplete_interval_ledger")
    for position, (record, window) in enumerate(zip(records, planned)):
        require(record["window"] == window and record["status"] == "raw_shard_validated",
                "interval_order_gap_or_overlap")
        if position:
            require(records[position - 1]["window"]["last_block"] + 1 == window["first_block"],
                    "interval_order_gap_or_overlap")
        expected = interval_receipts(window, record["evidence"], addresses)
        require(record["contract_intervals"] == expected, "per_contract_interval_ledger_differs")
        require(sum(item["rows"] for item in expected) == record["evidence"]["rows"], "interval_count_reconciliation")


def run_collection(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, output: Path, *,
                   mode: str, first_block: int, last_block: int, max_rpc_calls: int, max_output_bytes: int,
                   max_runtime_seconds: int, max_rows: int, window_blocks: int = 100,
                   max_ram_bytes: int,
                   max_total_response_bytes: int,
                   resume: bool = False, rpc_private_file: Path | None = None,
                   transport: Callable = pilot.http_response, clock: Callable = time.monotonic) -> dict:
    # main invokes the production guard before this callable fixture seam.
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    caps = limits(max_rpc_calls, max_output_bytes, max_runtime_seconds, max_rows, max_ram_bytes,
                  max_total_response_bytes)
    plan, binding, original = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
    addresses = requested_addresses(mode, first_block, last_block, plan)
    planned = windows(first_block, last_block, window_blocks)
    check_destination(output, original, plan_path, plan, private)
    source = inventory.code_state()
    require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_head or "")) and source.get("status") == "inspected"
            and source.get("head") == expected_head and source.get("tracked_worktree_clean") is True,
            "collector_source_head_or_clean_state_differs")
    contract = {"schema_version": "polymarket_native_collection_contract_v1", "source_plan": binding,
                "source": source, "mode": mode, "first_block": first_block, "last_block": last_block,
                "window_blocks": window_blocks, "addresses": list(addresses),
                "topics": sorted({pilot.EXPECTED_TOPICS[address] for address in addresses}), "limits": caps,
                "end_exclusive_utc": plan["end_exclusive_utc"], "raw_payload_only": True,
                "interior_exact_timestamps_collected": False, "analysis_admitted": False,
                "provider_block_timestamp_used_as_clock": False}
    contract_sha256 = digest(json_bytes(contract))
    require(shutil.disk_usage(output.parent).free >= FREE_FLOOR + caps["output_bytes"],
            "insufficient_free_space_for_stage_budget")
    if resume:
        require(output.is_dir() and not output.is_symlink(), "resume_directory_absent")
    else:
        output.mkdir(mode=0o700)  # Never adopt an existing directory implicitly.
    with run_lock(output):
        if resume:
            owner = load_json(output / "OWNER.json")
            require(owner.get("contract") == contract and owner.get("contract_sha256") == contract_sha256,
                    "resume_contract_differs")
            require(not (output / "COMPLETE.json").exists(), "run_already_complete")
        else:
            owner = {"run_id": str(uuid.uuid4()), "created_utc": datetime.now(timezone.utc).isoformat(),
                     "contract": contract, "contract_sha256": contract_sha256}
            publish(output / "OWNER.json", json_bytes(owner))
            (output / "attempts").mkdir(mode=0o700)
            (output / "sessions").mkdir(mode=0o700)
        require(all(path.name in {"OWNER.json", "LOCK", "attempts", "sessions", "manifest.json"}
                    or (path.name.startswith(".") and path.name.endswith(".pending")) for path in output.iterdir())
                and all((output / name).is_dir() and not (output / name).is_symlink() for name in ("attempts", "sessions")),
                "unexpected_run_entries")
        elapsed, rpc_starts, previous_response_bytes = 0.0, {}, 0
        for previous in (output / "sessions").iterdir():
            require(previous.is_dir() and not previous.is_symlink(), "session_directory_invalid")
            started = load_json(previous / "START.json")
            require(started.get("contract_sha256") == contract_sha256, "session_contract_differs")
            if (previous / "RESULT.json").exists():
                ended = load_json(previous / "RESULT.json")
                duration = ended.get("session_elapsed_seconds")
                require(type(duration) in (int, float) and duration >= 0, "session_duration_invalid")
                elapsed += duration
            else:
                start_utc = datetime.fromisoformat(started["started_utc"])
                require(start_utc.tzinfo is not None, "session_start_timezone_invalid")
                elapsed += max(0, (datetime.now(timezone.utc) - start_utc).total_seconds())
            request_dir = previous / "rpc"
            require(request_dir.is_dir() and not request_dir.is_symlink(), "session_rpc_directory_invalid")
            require(all(re.fullmatch(r"r[0-9]{9}\.(START|FINISH)\.json", path.name)
                        or (path.name.startswith(".") and path.name.endswith(".pending"))
                        for path in request_dir.iterdir()), "unexpected_session_rpc_files")
            for request_path in request_dir.glob("*.START.json"):
                request = load_json(request_path)
                require(type(request.get("id")) is int and request.get("method") in
                        {"eth_chainId", "eth_getBlockByNumber", "eth_getLogs"}, "session_rpc_start_invalid")
                require(request_path.name == f"r{request['id']:09d}.START.json" and request["id"] not in rpc_starts,
                        "session_rpc_id_conflict")
                rpc_starts[request["id"]] = {"value": request, "path": request_path}
                finish_path = request_path.with_name(request_path.name.replace(".START.json", ".FINISH.json"))
                if finish_path.exists():
                    finished = load_json(finish_path)
                    size = finished.get("response_bytes")
                    require(type(size) is int and 0 <= size <= pilot.MAX_RESPONSE_BYTES + 1
                            and (size <= pilot.MAX_RESPONSE_BYTES
                                 or finished.get("response_size_is_conservative_bound") is True
                                 or finished.get("status") == "failed")
                            and finished.get("id") == request["id"], "session_response_size_invalid")
                    previous_response_bytes += size
                else:
                    previous_response_bytes += pilot.MAX_RESPONSE_BYTES + 1
        require(set(rpc_starts) == set(range(1, len(rpc_starts) + 1)), "session_rpc_id_gap")
        budget = Budget(output, caps, account_existing(output, caps["output_bytes"]), clock, elapsed)
        budget.calls = len(rpc_starts)
        budget.response_bytes = previous_response_bytes
        session = output / "sessions" / uuid.uuid4().hex
        session.mkdir(mode=0o700)
        (session / "rpc").mkdir(mode=0o700)
        budget.write(session / "START.json", json_bytes({"contract_sha256": contract_sha256,
                     "started_utc": datetime.now(timezone.utc).isoformat(), "resume": resume}))
        result = {"schema_version": "polymarket_native_collection_session_v1", "status": "failed_raw_collection",
                  "run_id": owner["run_id"], "contract_sha256": contract_sha256, "collection_complete": False,
                  "analysis_admitted": False, "endpoint_disclosed": False, "validated_windows": 0}
        rpc = None
        records, shared_keys, transactions = [], {}, {}
        item_bytes = 0
        try:
            # Reopen every saved success before any network access. Partial attempts
            # remain consumed attempts and are never silently adopted or removed.
            expected_names = {f"{window['window']}.a{attempt}" for window in planned for attempt in range(1, 4)}
            actual_names = {path.name for path in (output / "attempts").iterdir()}
            require(actual_names <= expected_names and all(path.is_dir() and not path.is_symlink()
                    for path in (output / "attempts").iterdir()), "unexpected_attempt_directory")
            saved = {}
            for window in planned:
                successes = []
                for attempt in range(1, 4):
                    directory = output / "attempts" / f"{window['window']}.a{attempt}"
                    if not directory.exists():
                        continue
                    if not (directory / "START.json").exists():
                        require(all(path.name.startswith(".") and path.name.endswith(".pending")
                                    for path in directory.iterdir()), "unbound_incomplete_attempt_has_outputs")
                        continue  # Killed before request admission; consume this attempt.
                    start = load_json(directory / "START.json")
                    require(start == {"contract_sha256": contract_sha256, "window": window, "attempt": attempt},
                            "attempt_request_binding_differs")
                    require(all(path.name in {"START.json", "REQUESTS.json", "FAILURE.json", "payload.parquet", "SUCCESS.json"}
                                or (path.name.startswith(".") and path.name.endswith(".pending"))
                                for path in directory.iterdir()), "unexpected_attempt_files")
                    if (directory / "SUCCESS.json").exists():
                        receipt = load_json(directory / "SUCCESS.json")
                        require(receipt.get("contract_sha256") == contract_sha256 and receipt.get("window") == window
                                and receipt.get("attempt") == attempt and receipt.get("status") == "raw_shard_validated"
                                and receipt["payload"]["path"] == str((directory / "payload.parquet").relative_to(output)),
                                "success_receipt_binding_differs")
                        verify_success_ledger(output, directory, receipt, window, addresses, contract["topics"], rpc_starts)
                        local_keys, local_transactions = {}, {}
                        rows, evidence = native_rows(directory / "payload.parquet", window, receipt,
                                                     local_keys, local_transactions, addresses)
                        item_bytes = merge_identity_state(shared_keys, transactions, local_keys, local_transactions,
                                                          item_bytes, caps, commit=True)
                        require(evidence == receipt["evidence"]
                                and receipt["contract_intervals"] == interval_receipts(window, evidence, addresses),
                                "resume_evidence_differs")
                        budget.rows += len(rows)
                        successes.append(receipt)
                require(len(successes) <= 1, "duplicate_successful_window")
                if successes:
                    saved[window["window"]] = successes[0]
            require(budget.rows <= caps["rows"] and budget.calls < caps["rpc_calls"], "resumed_stage_cap_exhausted")
            budget.check()
            rpc = CollectionRPC(private, transport, budget, session)
            rpc.chain()
            for key in ("last_included_header", "first_excluded_header"):
                require(rpc.header(plan[key]["number"]) == plan[key], "source_plan_cutoff_header_drift")
            for window in planned:
                if window["window"] in saved:
                    receipt = saved[window["window"]]
                    for key in ("first_header", "last_header"):
                        require(rpc.header(receipt[key]["number"]) == receipt[key], "resumed_shard_header_drift")
                    records.append(receipt)
                    continue
                last_error = "window_attempts_exhausted"
                for attempt in range(1, 4):
                    directory = output / "attempts" / f"{window['window']}.a{attempt}"
                    if directory.exists():
                        continue
                    budget.check()
                    directory.mkdir(mode=0o700)
                    budget.write(directory / "START.json", json_bytes({"contract_sha256": contract_sha256,
                                 "window": window, "attempt": attempt}))
                    ledger_start = len(rpc.ledger)
                    try:
                        first, last = (rpc.header(window[key]) for key in ("first_block", "last_block"))
                        query = {"fromBlock": hex(window["first_block"]), "toBlock": hex(window["last_block"]),
                                 "address": list(addresses), "topics": [contract["topics"]]}
                        logs, request = rpc.call("eth_getLogs", [query])
                        # Mutate global identity maps only after all window checks;
                        # retries must not turn a failed attempt into a fake replay.
                        local_keys, local_transactions = {}, {}
                        rows, evidence = pilot.validate_logs(logs, window, {first["number"]: first, last["number"]: last},
                                                             request["id"], shared_keys=local_keys,
                                                             shared_transactions=local_transactions)
                        require(all(row["exchange_address"] in addresses for row in rows), "log_outside_requested_roster")
                        require(budget.rows + len(rows) <= caps["rows"], "stage_row_limit")
                        next_item_bytes = merge_identity_state(shared_keys, transactions, local_keys, local_transactions,
                                                               item_bytes, caps, commit=False)
                        rpc.header(last["number"], reopen=True)
                        payload = parquet_payload(rows)
                        payload_ref = budget.write(directory / "payload.parquet", payload)
                        request.update(status="validated", rows=len(rows), payload_sha256=payload_ref["sha256"])
                        ledger_ref = budget.write(directory / "REQUESTS.json", json_bytes({"calls": rpc.ledger[ledger_start:]}))
                        receipt = {"status": "raw_shard_validated", "contract_sha256": contract_sha256,
                                   "window": window, "attempt": attempt, "first_header": first, "last_header": last,
                                   "log_request_id": request["id"], "payload": payload_ref, "request_ledger": ledger_ref,
                                   "evidence": evidence, "contract_intervals": interval_receipts(window, evidence, addresses)}
                        budget.write(directory / "SUCCESS.json", json_bytes(receipt))
                        merge_identity_state(shared_keys, transactions, local_keys, local_transactions,
                                             item_bytes, caps, commit=True)
                        item_bytes = next_item_bytes
                        budget.rows += len(rows)
                        records.append(receipt)
                        break
                    except Exception as exc:
                        last_error = str(exc) if isinstance(exc, (pilot.PilotBlocked, planner.PlanBlocked, inventory.ReadinessError)) else "transport_payload_or_checkpoint_failure"
                        if not (directory / "REQUESTS.json").exists():
                            budget.write(directory / "REQUESTS.json", json_bytes({"calls": rpc.ledger[ledger_start:]}), reserve=False)
                        budget.write(directory / "FAILURE.json", json_bytes({"status": "failed_attempt", "error_code": last_error}), reserve=False)
                        # Bad payloads, range/oversize provider errors and resource
                        # limits require review. Only transport failures are retried.
                        if not isinstance(exc, TransportRetry):
                            raise
                else:
                    raise pilot.PilotBlocked(last_error)
                require(records and records[-1]["window"] == window, "window_attempts_exhausted")
            verify_coverage(records, planned, addresses)
            reopened, rebound, reopened_original = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
            require(reopened == plan and rebound == binding and reopened_original == original
                    and inventory.code_state() == source, "final_source_or_input_binding_differs")
            manifest = {"schema_version": "polymarket_native_collection_manifest_v1", "status": "raw_collection_complete",
                        "run_id": owner["run_id"], "contract_sha256": contract_sha256, "contract": contract,
                        "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True,
                        "distinct_native_keys": len(shared_keys), "rows": budget.rows,
                        "windows": records, "attempts": sorted(actual_names | {path.name for path in (output / "attempts").iterdir()}),
                        "limitations": ["Coverage is only the exact requested ranges, addresses and OrderFilled topics.",
                                        "Only endpoint headers are fetched; exact interior timestamps require a separate admitted cache.",
                                        "Provider blockTimestamp is uninterpreted provenance and never used as a clock.",
                                        "No ABI/economic decoding, retention rule, metadata join, or final-sample admission is established."]}
            manifest_raw = json_bytes(manifest)
            require(len(manifest_raw) <= MAX_CONTROL, "manifest_control_byte_limit")
            if (output / "manifest.json").exists():
                require((output / "manifest.json").read_bytes() == manifest_raw, "existing_uncommitted_manifest_differs")
            else:
                budget.write(output / "manifest.json", manifest_raw)
            # Reopen all receipt/payload hashes once more before terminal publication.
            for receipt in records:
                for reference in (receipt["payload"], receipt["request_ledger"]):
                    path = output / reference["path"]
                    require(not path.is_symlink() and path.stat().st_size == reference["bytes"]
                            and digest(path.read_bytes()) == reference["sha256"], "final_checkpoint_hash_differs")
            budget.write(output / "COMPLETE.json", json_bytes({"status": "raw_collection_complete_not_analysis_admission",
                         "run_id": owner["run_id"], "contract_sha256": contract_sha256,
                         "manifest_sha256": digest(manifest_raw)}))
            result.update(status="raw_collection_complete", collection_complete=True)
        except Exception as exc:
            result["error_code"] = str(exc) if isinstance(exc, (pilot.PilotBlocked, planner.PlanBlocked, inventory.ReadinessError)) else "transport_payload_or_checkpoint_failure"
        finally:
            result.update(validated_windows=len(records), rows=budget.rows, rpc_calls_charged=budget.calls,
                          response_bytes_charged=budget.response_bytes,
                          session_rpc_calls=len(rpc.ledger) if rpc is not None else 0,
                          identity_key_count=len(shared_keys), transaction_anchor_count=len(transactions),
                          conservatively_counted_identity_item_bytes=item_bytes,
                          session_elapsed_seconds=max(0, clock() - budget.start),
                          output_bytes_before_session_result=budget.used)
            # A resource/time failure must still leave a small non-sensitive result.
            # The reserved bytes were withheld from every ordinary write.
            raw = json_bytes(result)
            if budget.used + len(raw) <= caps["output_bytes"]:
                publish(session / "RESULT.json", raw)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-plan", type=Path, required=True)
    parser.add_argument("--source-plan-sha256", required=True)
    parser.add_argument("--plan-source-head", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--first-block", type=int, required=True)
    parser.add_argument("--last-block", type=int, required=True)
    parser.add_argument("--max-rpc-calls", type=int, required=True)
    parser.add_argument("--max-output-bytes", type=int, required=True)
    parser.add_argument("--max-runtime-seconds", type=int, required=True)
    parser.add_argument("--max-rows", type=int, required=True)
    parser.add_argument("--max-ram-bytes", type=int, required=True)
    parser.add_argument("--max-total-response-bytes", type=int, required=True)
    parser.add_argument("--window-blocks", type=int, default=100)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production collection requires the canonical mounted EC2 checkout.")
    if not args.output_dir.is_absolute() or Path("/mnt/data/runs") not in args.output_dir.resolve().parents:
        parser.error("Production outputs must be an explicit child of /mnt/data/runs.")
    try:
        for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[variable] = "1"  # Before lazy Arrow/NumPy imports and the AS cap.
        limits(args.max_rpc_calls, args.max_output_bytes, args.max_runtime_seconds, args.max_rows,
               args.max_ram_bytes, args.max_total_response_bytes)
        available = inventory.resource_state()["ram_bytes"]["MemAvailable"]
        require(available >= args.max_ram_bytes, "insufficient_available_ram_before_collection")
        resource.setrlimit(resource.RLIMIT_AS, (args.max_ram_bytes, args.max_ram_bytes))
        result = run_collection(args.source_plan, args.source_plan_sha256, args.plan_source_head, args.expected_head,
                                args.output_dir, mode=args.mode, first_block=args.first_block, last_block=args.last_block,
                                max_rpc_calls=args.max_rpc_calls, max_output_bytes=args.max_output_bytes,
                                max_runtime_seconds=args.max_runtime_seconds, max_rows=args.max_rows,
                                max_ram_bytes=args.max_ram_bytes,
                                max_total_response_bytes=args.max_total_response_bytes,
                                window_blocks=args.window_blocks, resume=args.resume, rpc_private_file=args.rpc_private_file)
    except Exception:
        parser.error("Source/destination admission failed; no complete collection was published.")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "raw_collection_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
