"""Bounded sparse V3 event-history diagnostic, not deployment/economic admission.

Fixed Polygon proxy; Upgraded-only [0, 94_796_039] and all-topic zero-prefix
[0, 87_479_446]. No monotone-code assumption, raw trade staging or resumption.
Every attempt is durably reserved before dispatch in a fresh exclusive run.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time
from typing import Any, Callable
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_protocol_activity as activity
from scripts import probe_polymarket_v3_history as prior

ADDRESS, UPGRADED_TOPIC = prior.ADDRESS, prior.UPGRADED_TOPIC
CUTOFF_BLOCK, PREFIX_END = prior.CUTOFF_BLOCK, prior.OLD_REPORTED_CREATION_BLOCK
INITIAL_WINDOW_BLOCKS = 1_000_000
MAX_RPC_CALLS, MAX_UPGRADES = 256, 8
MAX_RESPONSE_BYTES, MAX_TOTAL_RESPONSE_BYTES = 4 * 1024**2, 16 * 1024**2
MAX_OUTPUT_BYTES, FINAL_RESERVE_BYTES = 1024**2, 384 * 1024
MAX_RUNTIME_SECONDS, MAX_RAM_BYTES = 1200, 8 * 1024**3
FREE_FLOOR_BYTES, MAX_RECEIPT_LOGS = 20_000_000_000, 4096
RUN_ROOT = Path("/mnt/data/runs")
ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
SAFE_CODE = re.compile(r"[a-z][a-z0-9_]{0,99}\Z")
SOURCE_FILES = ("scripts/probe_polymarket_v3_history_v2.py", "scripts/inventory_polymarket_refresh.py",
                "scripts/plan_polymarket_refresh.py", "scripts/probe_polymarket_protocol_activity.py",
                "scripts/probe_polymarket_v3_history.py", "production_guard.py")


class HistoryBlocked(ValueError):
    """Only fixed machine codes, never transport text or endpoints."""


class RangeRefused(HistoryBlocked):
    """A proven size/range refusal may split only the identical log predicate."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise HistoryBlocked(code)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def encode(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def limits() -> dict:
    return {"rpc_attempts": MAX_RPC_CALLS, "response_bytes_per_attempt_including_detector": MAX_RESPONSE_BYTES,
            "total_charged_response_bytes": MAX_TOTAL_RESPONSE_BYTES, "all_control_and_output_bytes": MAX_OUTPUT_BYTES,
            "upgrade_events": MAX_UPGRADES, "runtime_seconds": MAX_RUNTIME_SECONDS,
            "address_space_bytes": MAX_RAM_BYTES, "minimum_available_ram_bytes": MAX_RAM_BYTES,
            "untouched_free_disk_bytes": FREE_FLOOR_BYTES, "spill_bytes": 0,
            "workers": 1, "initial_sparse_window_blocks": INITIAL_WINDOW_BLOCKS}


def source_snapshot(expected_head: str) -> dict:
    require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head), "invalid_expected_head")
    source = inventory.code_state()
    require(source.get("status") == "inspected" and source.get("head") == expected_head
            and source.get("tracked_worktree_clean") is True, "probe_source_head_or_clean_state_differs")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = inventory.path_identity(path)
        require(before.get("status") == "present" and not before.get("is_symlink")
                and path.is_file() and 0 < path.stat().st_size <= 1024**2, "source_dependency_not_bounded_regular_file")
        saved = subprocess.run(["git", "show", f"{expected_head}:{name}"], cwd=REPO,
                               capture_output=True, timeout=10)
        require(saved.returncode == 0 and saved.stdout == path.read_bytes(),
                "source_dependency_uncommitted_or_changed")
        require(before == inventory.path_identity(path), "source_dependency_changed_during_read")
        files[name] = {"sha256": digest(saved.stdout), "identity": before}
    require(source == inventory.code_state(), "probe_source_changed_during_snapshot")
    return {"head": expected_head, "tracked_worktree_clean": True, "files": files}


@contextmanager
def runtime_limits():
    """CLI and public probe both use real address-space and wall-clock limits."""
    previous_as = resource.getrlimit(resource.RLIMIT_AS)
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    require(previous_timer == (0.0, 0.0), "preexisting_alarm_not_admitted")
    soft = min(MAX_RAM_BYTES, previous_as[1]) if previous_as[1] != resource.RLIM_INFINITY else MAX_RAM_BYTES
    require(soft > 0, "address_space_limit_unavailable")
    started = time.monotonic()
    changed = False
    try:
        resource.setrlimit(resource.RLIMIT_AS, (soft, previous_as[1]))
        changed = True
        effective = resource.getrlimit(resource.RLIMIT_AS)
        require(0 < effective[0] <= MAX_RAM_BYTES, "address_space_limit_not_effective")
        def expired(_signum, _frame):
            raise HistoryBlocked("runtime_deadline_exceeded")
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, MAX_RUNTIME_SECONDS)
        yield {"address_space_soft_bytes": effective[0], "wall_alarm_seconds": MAX_RUNTIME_SECONDS,
               "started_monotonic": started}
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if changed:
            resource.setrlimit(resource.RLIMIT_AS, previous_as)


class Controls:
    """One owned directory. Reserve final evidence before growing the journal."""

    def __init__(self, root: Path, owner: dict):
        require(not root.exists() and not root.is_symlink(), "run_directory_already_exists")
        require(root.parent.is_dir() and not root.parent.is_symlink(), "run_parent_not_regular")
        require(RUN_ROOT.resolve() in root.resolve().parents, "run_outside_production_run_root")
        self.root, self.bytes = root, 0
        self.expected: dict[str, str] = {}
        self.journal_hash, self.journal_records = hashlib.sha256(), 0
        self.pending_marker = False
        self.free(MAX_OUTPUT_BYTES)
        root.mkdir(mode=0o700)
        self.identity = (root.stat().st_dev, root.stat().st_ino)
        self.write("OWNER.json", owner, final=False)
        self.write("REQUESTS.jsonl", b"", final=False)

    def free(self, additional: int) -> None:
        require(shutil.disk_usage(self.root if self.root.exists() else self.root.parent).free
                - additional >= FREE_FLOOR_BYTES, "free_disk_floor_would_be_crossed")

    def check(self, additional: int, *, final: bool) -> None:
        require(not self.root.is_symlink() and self.root.is_dir()
                and (self.root.stat().st_dev, self.root.stat().st_ino) == self.identity,
                "owned_directory_changed")
        names = list(self.root.iterdir())
        allowed = {"OWNER.json", "REQUESTS.jsonl", "RESULT.json", "COMPLETE.json"}
        if self.pending_marker:
            allowed.add(".COMPLETE.pending")
        require(all(item.name in allowed
                    and item.is_file() and not item.is_symlink() for item in names), "unexpected_control_artifact")
        actual = sum(item.stat().st_size for item in names)
        require(actual == self.bytes, "owned_control_bytes_changed")
        cap = MAX_OUTPUT_BYTES if final else MAX_OUTPUT_BYTES - FINAL_RESERVE_BYTES
        require(actual + additional <= cap, "all_control_output_byte_limit")
        self.free(additional)

    def write(self, name: str, value: Any, *, final: bool) -> None:
        raw = value if type(value) is bytes else encode(value)
        if name == "COMPLETE.json":
            self._complete(raw, final=final)
            return
        self.check(len(raw), final=final)
        fd = os.open(self.root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self.bytes += len(raw)
        self.expected[name] = digest(raw)
        with (self.root / name).open("rb") as stream:
            require(stream.read(len(raw) + 1) == raw, "control_reopen_differs")
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _complete(self, raw: bytes, *, final: bool) -> None:
        """The no-replace rename is the last fallible artifact operation.

        Failed write/flush/fsync/close/reopen/directory-fsync leaves only the
        fully charged pending file. A crash after rename may lose the final
        directory rename, leaving an incomplete run, never a partial marker.
        No post-publication directory-fsync is attempted and then reported as
        an ordinary failed run with a reader-visible valid certificate.
        """
        require(final and not self.pending_marker, "completion_not_final_or_already_pending")
        self.verify()
        self.check(len(raw), final=True)
        temporary, destination = self.root / ".COMPLETE.pending", self.root / "COMPLETE.json"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.pending_marker = True
        # Charge the full reservation before touching the staged marker. Even
        # a partially written retained attempt has no quota exemption.
        self.bytes += len(raw)
        self.expected[temporary.name] = digest(raw)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self.verify()
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self.verify()
        # Set only in-memory accounting before the commit syscall; no later
        # artifact read/write/fsync can turn committed success into failure.
        self.expected.pop(temporary.name)
        self.expected[destination.name] = digest(raw)
        self.pending_marker = False
        atomic_completion(temporary, destination)

    def journal(self, value: dict) -> None:
        raw = encode(value)
        self.check(len(raw), final=False)
        fd = os.open(self.root / "REQUESTS.jsonl", os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(fd, "ab") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self.bytes += len(raw)
        self.journal_hash.update(raw)
        self.journal_records += 1
        self.expected["REQUESTS.jsonl"] = self.journal_hash.hexdigest()

    def verify(self) -> dict:
        self.check(0, final=True)
        hashes = {}
        for name, expected in self.expected.items():
            path = self.root / name
            require(path.stat().st_size <= MAX_OUTPUT_BYTES, "control_reopen_size_limit")
            raw = path.read_bytes()
            require(digest(raw) == expected, "owned_control_content_changed")
            if name == "REQUESTS.jsonl":
                require(len(raw.splitlines()) == self.journal_records, "request_journal_record_count_changed")
            hashes[name] = expected
        return {"hashes": hashes, "journal_records": self.journal_records, "bytes": self.bytes}


def atomic_completion(temporary: Path, destination: Path) -> None:
    """Atomic rename without replacement; do not emulate with an overwrite."""
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        outcome = library.renameat2(-100, os.fsencode(temporary), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        outcome = library.renamex_np(os.fsencode(temporary), os.fsencode(destination), 4)
    else:
        raise HistoryBlocked("atomic_completion_unavailable")
    require(outcome == 0, "atomic_completion_refused")


@dataclass(frozen=True)
class HTTPResponse:
    body: bytes
    status: int = 200


def http_response(endpoint: str, payload: dict, *, byte_limit: int, timeout: float) -> HTTPResponse:
    request = urllib.request.Request(endpoint, data=encode(payload), method="POST",
                                     headers={"Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        # One detector byte is INSIDE the wire cap. A boundary-sized read is
        # refused, never parsed as a potentially truncated successful result.
        return HTTPResponse(response.read(byte_limit), response.status)


def demonstrable_range_error(value: dict) -> bool:
    if type(value.get("code")) is not int or value["code"] not in {-32005, -32602, -32000}:
        return False
    message = value.get("message")
    if type(message) is not str or len(message) > 8192:
        return False
    message = message.lower()
    patterns = (r"block range.{0,80}(?:exceed|too (?:large|wide)|maximum|limit)",
                r"(?:exceed|maximum|limited to).{0,80}block range",
                r"query returned more than \d+ (?:results|logs)",
                r"(?:log )?response size.{0,40}(?:exceed|too large)",
                r"(?:query|result).{0,30}(?:exceed|too many).{0,30}(?:results|logs)")
    return any(re.search(pattern, message) for pattern in patterns)


class HistoryRPC(activity.ProbeRPC):
    def __init__(self, endpoint: str, transport: Callable, seeds: list[dict], controls: Controls,
                 started: float, clock: Callable = time.monotonic):
        super().__init__(endpoint, transport, seeds)
        self.controls, self.started, self.clock = controls, started, clock
        self.response_bytes_received = self.response_bytes_charged = 0
        self.queried_headers: set[int] = set()

    def remaining_time(self) -> float:
        remaining = MAX_RUNTIME_SECONDS - (self.clock() - self.started)
        require(remaining > 0, "runtime_deadline_exceeded")
        return remaining

    def call(self, method: str, params: list) -> tuple[Any, dict]:
        require(method in {"eth_chainId", "eth_getLogs", "eth_getBlockByNumber",
                           "eth_getTransactionReceipt", "eth_getStorageAt"}, "rpc_method_not_permitted")
        remaining_time = self.remaining_time()
        require(len(self.ledger) < MAX_RPC_CALLS, "rpc_attempt_limit")
        remaining = MAX_TOTAL_RESPONSE_BYTES - self.response_bytes_charged
        require(remaining >= 2, "total_response_byte_limit")
        reserved = min(MAX_RESPONSE_BYTES, remaining)
        entry = {"id": len(self.ledger) + 1, "method": method, "params": params,
                 "status": "requested", "wire_byte_reservation": reserved,
                 "maximum_accepted_payload_bytes": reserved - 1, "started_utc": now()}
        self.controls.journal({"event": "start", **entry})
        self.ledger.append(entry)
        self.response_bytes_charged += reserved
        try:
            reply = self.transport(self.endpoint, {"jsonrpc": "2.0", "id": entry["id"],
                                                  "method": method, "params": params},
                                   byte_limit=reserved, timeout=min(30.0, remaining_time))
            require(type(reply) is HTTPResponse and type(reply.body) is bytes
                    and type(reply.status) is int, "transport_response_shape_invalid")
            raw = reply.body
            self.response_bytes_received += len(raw)
            require(len(raw) <= reserved, "transport_wire_byte_cap_violated")
            self.response_bytes_charged -= reserved - len(raw)
            entry.update(response_bytes=len(raw), charged_response_bytes=len(raw),
                         response_sha256=digest(raw), http_status=reply.status)
            self.remaining_time()
            if method == "eth_getLogs" and (len(raw) == reserved or reply.status == 413):
                raise RangeRefused("log_response_size_refused")
            require(len(raw) < reserved, "rpc_response_size_refused")
            require(reply.status == 200, "rpc_http_status_not_admitted")
            value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(HistoryBlocked("rpc_nonfinite_json")))
            require(type(value) is dict and set(value) in ({"jsonrpc", "id", "result"},
                                                          {"jsonrpc", "id", "error"}), "rpc_reply_shape_invalid")
            require(value["jsonrpc"] == "2.0" and type(value["id"]) is int
                    and value["id"] == entry["id"], "rpc_reply_id_mismatch")
            if "error" in value:
                error = value["error"]
                require(type(error) is dict, "rpc_error_shape_invalid")
                if method == "eth_getLogs" and demonstrable_range_error(error):
                    raise RangeRefused("log_range_refused")
                raise HistoryBlocked("rpc_error_not_range_refusal")
            entry["status"] = "reply_shape_validated"
            return value["result"], entry
        except RangeRefused as exc:
            entry.update(status="range_refused", error_code=str(exc))
            raise
        except Exception as exc:
            entry.update(status="failed", error_code=safe_error(exc))
            raise
        finally:
            if "charged_response_bytes" not in entry:
                entry["charged_response_bytes"] = reserved
                entry["unobserved_body_charged_at_full_reservation"] = True
            self.controls.journal({"event": "finish", **entry})

    def header(self, number: int) -> dict:
        result = super().header(number)
        self.queried_headers.add(number)
        return result


def safe_error(exc: Exception) -> str:
    if isinstance(exc, (HistoryBlocked, activity.ProbeBlocked, planner.PlanBlocked, inventory.ReadinessError)):
        code = str(exc)
        if SAFE_CODE.fullmatch(code):
            return code
    return "bounded_input_transport_or_runtime_failure"


def normalize_log(value: Any, *, lower: int, upper: int, upgrade: bool) -> dict:
    require(type(value) is dict, "native_log_not_object")
    required = {"address", "topics", "data", "removed", "blockNumber", "blockHash",
                "transactionHash", "transactionIndex", "logIndex"}
    require(required <= set(value), "native_log_fields_missing")
    require(type(value["address"]) is str and ADDRESS_RE.fullmatch(value["address"]), "native_log_address_invalid")
    require(value["removed"] is False, "native_log_removed_or_unconfirmed")
    number, tx_index, log_index = (inventory._quantity(value[key])
                                  for key in ("blockNumber", "transactionIndex", "logIndex"))
    require(lower <= number <= upper and max(number, tx_index, log_index) < 2**64, "native_log_bounds_invalid")
    for key in ("blockHash", "transactionHash"):
        require(type(value[key]) is str and activity.HASH.fullmatch(value[key]), "native_log_hash_invalid")
    topics, data = value["topics"], value["data"]
    require(type(topics) is list and len(topics) <= 4
            and all(type(item) is str and activity.HASH.fullmatch(item) for item in topics), "native_log_topics_invalid")
    require(type(data) is str and activity.HEX_BYTES.fullmatch(data), "native_log_data_invalid")
    result = {"exchange_address": value["address"].lower(), "block_number": number,
              "block_hash": value["blockHash"].lower(), "transaction_hash": value["transactionHash"].lower(),
              "transaction_index": tx_index, "log_index": log_index}
    if upgrade:
        require(number > 0 and result["exchange_address"] == ADDRESS, "upgrade_address_or_genesis_invalid")
        require(len(topics) == 2 and topics[0].lower() == UPGRADED_TOPIC and data == "0x",
                "upgrade_topic_arity_or_data_invalid")
        require(topics[1][2:26] == "0" * 24 and int(topics[1], 16) != 0,
                "upgrade_implementation_word_invalid")
        result["new_implementation"] = "0x" + topics[1][-40:].lower()
    return result


def verify_coverage(records: list[dict], end: int, kind: str) -> None:
    next_block = 0
    for item in records:
        require(item["kind"] == kind and item["first_block"] == next_block
                and item["first_block"] <= item["last_block"] <= end, "range_coverage_gap_overlap_or_kind")
        next_block = item["last_block"] + 1
    require(next_block == end + 1, "range_coverage_incomplete")


def scan(rpc: HistoryRPC, *, kind: str, end: int, upgrades: list[dict]) -> list[dict]:
    records: list[dict] = []
    identities, positions, transactions, tx_positions = set(), set(), {}, {}
    previous_position = None
    for start in range(0, end + 1, INITIAL_WINDOW_BLOCKS):
        stack = [(start, min(end, start + INITIAL_WINDOW_BLOCKS - 1))]
        while stack:
            lower, upper = stack.pop()
            predicate = {"address": ADDRESS, "fromBlock": hex(lower), "toBlock": hex(upper)}
            if kind == "upgraded":
                predicate["topics"] = [UPGRADED_TOPIC]
            try:
                logs, request = rpc.call("eth_getLogs", [predicate])
            except RangeRefused:
                require(lower < upper, "one_block_range_refusal")
                middle = (lower + upper) // 2
                stack.extend([(middle + 1, upper), (lower, middle)])
                continue
            require(type(logs) is list, "rpc_logs_not_list")
            request["returned_log_count"] = len(logs)
            if kind == "zero_prefix":
                require(not logs, "prefix_contains_emitted_logs")
            else:
                require(len(upgrades) + len(logs) <= MAX_UPGRADES, "upgrade_event_limit")
                for value in logs:
                    item = normalize_log(value, lower=lower, upper=upper, upgrade=True)
                    native = (item["exchange_address"], item["transaction_hash"], item["log_index"])
                    position = (item["block_number"], item["log_index"])
                    ordered = (item["block_number"], item["transaction_index"], item["log_index"])
                    require(native not in identities and position not in positions, "duplicate_or_conflicting_native_identity")
                    require(previous_position is None or previous_position < ordered, "upgrade_order_not_strict")
                    require(previous_position is None or previous_position[0] != ordered[0]
                            or (ordered[1] >= previous_position[1] and ordered[2] > previous_position[2]),
                            "in_block_log_order_not_strict")
                    tx = item["transaction_hash"]
                    tx_position = (item["block_number"], item["transaction_index"])
                    require(transactions.get(tx, tx_position) == tx_position
                            and tx_positions.get(tx_position, tx) == tx, "transaction_locator_conflict")
                    require(item["block_number"] not in rpc.block_hashes
                            or rpc.block_hashes[item["block_number"]] == item["block_hash"], "observed_block_hash_conflict")
                    identities.add(native)
                    positions.add(position)
                    transactions[tx], tx_positions[tx_position] = tx_position, tx
                    rpc.block_hashes[item["block_number"]] = item["block_hash"]
                    upgrades.append(item)
                    previous_position = ordered
            request["status"] = "range_validated"
            records.append({"kind": kind, "first_block": lower, "last_block": upper,
                            "request_id": request["id"], "log_count": len(logs),
                            "response_sha256": request["response_sha256"]})
            rpc.controls.journal({"event": "range_validated", **records[-1]})
    verify_coverage(records, end, kind)
    return records


def verify_receipts(rpc: HistoryRPC, upgrades: list[dict]) -> list[dict]:
    expected = {(item["transaction_hash"], item["log_index"]): item for item in upgrades}
    proofs = []
    for tx in dict.fromkeys(item["transaction_hash"] for item in upgrades):
        own = [item for item in upgrades if item["transaction_hash"] == tx]
        first = own[0]
        value, entry = rpc.call("eth_getTransactionReceipt", [tx])
        require(type(value) is dict, "upgrade_receipt_missing")
        require(type(value.get("transactionHash")) is str and value["transactionHash"].lower() == tx,
                "upgrade_receipt_transaction_mismatch")
        require(inventory._quantity(value.get("status")) == 1, "upgrade_receipt_unsuccessful")
        require(inventory._quantity(value.get("blockNumber")) == first["block_number"]
                and inventory._quantity(value.get("transactionIndex")) == first["transaction_index"],
                "upgrade_receipt_locator_mismatch")
        require(type(value.get("blockHash")) is str and value["blockHash"].lower() == first["block_hash"],
                "upgrade_receipt_block_hash_mismatch")
        logs = value.get("logs")
        require(type(logs) is list and 0 < len(logs) <= MAX_RECEIPT_LOGS, "upgrade_receipt_logs_invalid")
        seen, selected, previous = set(), [], -1
        for log in logs:
            basic = normalize_log(log, lower=first["block_number"], upper=first["block_number"], upgrade=False)
            require(basic["transaction_hash"] == tx and basic["transaction_index"] == first["transaction_index"]
                    and basic["block_hash"] == first["block_hash"], "receipt_log_locator_mismatch")
            index = basic["log_index"]
            require(index not in seen and index > previous, "receipt_log_order_or_duplicate")
            seen.add(index)
            previous = index
            if basic["exchange_address"] == ADDRESS and log["topics"] and log["topics"][0].lower() == UPGRADED_TOPIC:
                item = normalize_log(log, lower=first["block_number"], upper=first["block_number"], upgrade=True)
                require(expected.get((tx, index)) == item, "receipt_upgrade_missing_or_differs_from_ledger")
                selected.append(item)
        require(selected == own, "ledger_upgrade_missing_from_receipt")
        entry["status"] = "receipt_validated"
        proofs.append({"transaction_hash": tx, "block_number": first["block_number"],
                       "transaction_index": first["transaction_index"], "receipt_status": 1,
                       "receipt_log_count": len(logs), "upgrade_log_indices": [item["log_index"] for item in own],
                       "response_sha256": entry["response_sha256"]})
    return proofs


def verify_upgrade_blocks(rpc: HistoryRPC, upgrades: list[dict]) -> tuple[list[dict], list[dict]]:
    proofs, previous_implementation = [], None
    for number in dict.fromkeys(item["block_number"] for item in upgrades):
        own = [item for item in upgrades if item["block_number"] == number]
        before, after = rpc.header(number - 1), rpc.header(number)
        require(after["parentHash"] == before["hash"], "upgrade_adjacent_header_link_differs")
        require(all(item["block_hash"] == after["hash"] for item in own), "upgrade_exact_header_hash_differs")
        pre, post = rpc.implementation(number - 1), rpc.implementation(number)
        if previous_implementation is not None:
            require(pre["implementation_address"] == previous_implementation, "implementation_between_upgrade_blocks_differs")
        require(post["implementation_address"] == own[-1]["new_implementation"], "post_upgrade_slot_differs")
        if number == prior.UPGRADE_BLOCK:
            require(pre["implementation_address"] == prior.OLD_IMPLEMENTATION, "known_upgrade_pre_slot_differs")
        previous_implementation = own[-1]["new_implementation"]
        proofs.append({"block_number": number, "before_header": before, "after_header": after,
                       "before_block_slot": pre, "after_block_slot": post,
                       "in_block_upgrades": own,
                       "whole_block_implementation_assignment_admitted": False,
                       "boundary_note": "Slots are end-of-block observations. Keep transaction/log ordering; "
                                        "fills in an upgrade block need execution-order/source evidence before era assignment."})
    cutoff = rpc.implementation(CUTOFF_BLOCK)
    require(cutoff["implementation_address"] == prior.NEW_IMPLEMENTATION
            and upgrades[-1]["new_implementation"] == cutoff["implementation_address"], "cutoff_implementation_differs")
    tail = rpc.implementation(prior.RAW_TAIL_BLOCK)
    require(tail["implementation_address"] == prior.OLD_IMPLEMENTATION, "historical_tail_implementation_differs")
    earlier = [item for item in upgrades if item["block_number"] <= prior.RAW_TAIL_BLOCK]
    require(not earlier or earlier[-1]["new_implementation"] == tail["implementation_address"],
            "historical_tail_upgrade_ledger_differs")
    return proofs, [tail, cutoff]


def diagnostic(plan: dict, rpc: HistoryRPC, result: dict) -> None:
    chain, entry = rpc.call("eth_chainId", [])
    require(inventory._quantity(chain) == 137, "rpc_wrong_chain")
    entry["status"] = "chain_validated"
    for block in (prior.RAW_TAIL_BLOCK, CUTOFF_BLOCK, CUTOFF_BLOCK + 1):
        rpc.header(block)
    result["upgrade_ranges"] = scan(rpc, kind="upgraded", end=CUTOFF_BLOCK, upgrades=result["upgrades"])
    known = [item for item in result["upgrades"] if item["transaction_hash"] == prior.UPGRADE_TRANSACTION
             and item["log_index"] == prior.UPGRADE_LOG_INDEX]
    require(len(known) == 1 and known[0]["block_number"] == prior.UPGRADE_BLOCK
            and known[0]["transaction_index"] == 62
            and known[0]["new_implementation"] == prior.NEW_IMPLEMENTATION, "known_upgrade_absent_or_differs")
    result["zero_prefix_ranges"] = scan(rpc, kind="zero_prefix", end=PREFIX_END, upgrades=[])
    for block in (PREFIX_END, PREFIX_END + 1):
        rpc.header(block)
    require(rpc.headers[PREFIX_END + 1]["parentHash"] == rpc.headers[PREFIX_END]["hash"], "prefix_adjacent_header_link_differs")
    result["upgrade_block_proofs"], result["endpoint_slots"] = verify_upgrade_blocks(rpc, result["upgrades"])
    result["receipt_proofs"] = verify_receipts(rpc, result["upgrades"])
    for number in sorted(rpc.queried_headers):
        rpc.header(number)
    result["headers"] = [rpc.headers[number] for number in sorted(rpc.queried_headers)]
    require(plan["last_included_header"] == rpc.headers[CUTOFF_BLOCK], "final_cutoff_header_differs")


def probe(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, *, run_dir: Path,
          rpc_private_file: Path | None = None, transport: Callable = http_response) -> dict:
    # This cannot be bypassed by invoking the public Python API instead of CLI.
    require_production_host()
    with runtime_limits() as enforcement:
        return _probe_with_limits(plan_path, plan_sha256, plan_source_head, expected_head,
                                  run_dir=run_dir, rpc_private_file=rpc_private_file,
                                  transport=transport, enforcement=enforcement)


def _probe_with_limits(plan_path: Path, plan_sha256: str, plan_source_head: str, expected_head: str, *,
                       run_dir: Path, rpc_private_file: Path | None, transport: Callable, enforcement: dict) -> dict:
    result = {"schema_version": "polymarket_v3_sparse_history_probe_v2", "status": "blocked",
              "created_utc": now(), "address": ADDRESS, "chain_id": 137, "limits": limits(),
              "collection_approved": False, "exchange_abi_admitted": False, "economic_decoding_admitted": False,
              "analysis_admitted": False, "first_deployment_certified": False,
              "gap_free_upgrade_history_certified": False, "implementation_eras_admitted": False,
              "requested_upgraded_event_ledger_complete": False, "zero_prefix_verified": False,
              "conservative_event_start_supported": False, "safe_event_collection_start_block": None,
              "parquet_body_reads": 0, "spill_bytes": 0, "endpoint_disclosed": False,
              "upgrade_ranges": [], "zero_prefix_ranges": [], "upgrades": [], "headers": [],
              "upgrade_block_proofs": [], "receipt_proofs": [], "endpoint_slots": [],
              "scope": "Literal RPC event-range evidence, not proof that every historical implementation "
                       "emitted every storage change, first deployment, ABI semantics, or final sample admission."}
    rpc = controls = None
    try:
        result["runtime_enforcement"] = {key: value for key, value in enforcement.items() if key != "started_monotonic"}
        resources = inventory.resource_state(run_dir.parent)
        require(resources.get("ram_bytes", {}).get("MemAvailable", 0) >= MAX_RAM_BYTES, "available_ram_below_admission")
        require(resources.get("disk_bytes", {}).get("free", 0) >= FREE_FLOOR_BYTES + MAX_OUTPUT_BYTES,
                "available_disk_below_admission")
        plan, binding, original = activity.read_plan(plan_path, plan_sha256, plan_source_head)
        prior.validate_plan_blocks(plan)
        private = rpc_private_file or Path.home() / ".polygon_rpc_url"
        activity.validate_output(run_dir, plan_path, private, original, plan)
        source = source_snapshot(expected_head)
        result.update(source_plan_binding=binding, probe_source=source, resource_admission=resources)
        controls = Controls(run_dir, {"schema_version": "polymarket_v3_sparse_history_owner_v2",
            "created_utc": now(), "source_plan_binding": binding, "probe_source": source,
            "limits": limits(), "address": ADDRESS, "chain_id": 137,
            "upgraded_range": [0, CUTOFF_BLOCK], "zero_prefix_range": [0, PREFIX_END],
            "resume_permitted": False, "economic_decoding_admitted": False})
        endpoint, configuration_source = inventory.load_rpc_endpoint(private)
        rpc = HistoryRPC(endpoint, transport, [plan[name] for name in
                         ("raw_footer_tail_header", "last_included_header", "first_excluded_header")],
                         controls, enforcement["started_monotonic"])
        result["configuration_source"] = configuration_source
        diagnostic(plan, rpc, result)
        planner.reopen_inputs(original)
        require(inventory.path_identity(Path(plan["inventory_binding"]["path"])) == plan["inventory_binding"]["identity"],
                "source_inventory_changed_during_probe")
        require(inventory.path_identity(plan_path) == binding["identity"], "source_plan_changed_during_probe")
        require(source == source_snapshot(expected_head), "probe_source_changed_during_probe")
        result["reopened_control_bindings"] = controls.verify()
        rpc.remaining_time()
        result.update(status="bounded_v3_sparse_history_complete", requested_upgraded_event_ledger_complete=True,
                      zero_prefix_verified=True, conservative_event_start_supported=True,
                      safe_event_collection_start_block=PREFIX_END + 1,
                      final_input_identity_unchanged=True, reopened_headers_unchanged=True)
    except Exception as exc:
        result["error_code"] = safe_error(exc)
    finally:
        result["rpc_calls_attempted"] = len(rpc.ledger) if rpc else 0
        result["response_bytes_received"] = rpc.response_bytes_received if rpc else 0
        result["response_bytes_charged"] = rpc.response_bytes_charged if rpc else 0
        result["request_ledger"] = rpc.ledger if rpc else []
        result["finished_utc"] = now()
        if rpc:
            result["elapsed_seconds"] = max(0.0, time.monotonic() - rpc.started)
        if controls:
            # The outer alarm remains effective during final publication.
            # An interrupted/partial result never receives COMPLETE.
            # Expected hashes come from bytes written by this process, never
            # from adopting newly reopened content as the original binding.
            try:
                result["final_control_bindings"] = controls.verify()
            except HistoryBlocked as exc:
                result.update(status="blocked", error_code=str(exc), requested_upgraded_event_ledger_complete=False,
                              zero_prefix_verified=False, conservative_event_start_supported=False,
                              safe_event_collection_start_block=None)
            result["request_journal_sha256"] = controls.journal_hash.hexdigest()
            result["request_journal_records"] = controls.journal_records
            result["expected_owner_sha256"] = controls.expected["OWNER.json"]
            result["control_bytes_before_result"] = controls.bytes
            controls.write("RESULT.json", result, final=True)
            if result["status"] == "bounded_v3_sparse_history_complete":
                rpc.remaining_time()
                controls.verify()
                controls.write("COMPLETE.json", {"status": result["status"], "analysis_admitted": False,
                    "collection_approved": False, "result_sha256": digest(encode(result)),
                    "request_journal_sha256": result["request_journal_sha256"]}, final=True)
    return result


def main(argv: list[str] | None = None) -> int:
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
                      "collection_approved": False, "analysis_admitted": False}))
    return 0 if result["status"] == "bounded_v3_sparse_history_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
