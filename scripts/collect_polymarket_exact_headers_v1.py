"""Bounded, immutable exact-header PILOT for at most 1,000 literal block keys.

This is not source discovery, a full-history campaign, or final-sample admission.
JSON input is a list of {block_number, optional block_hash} objects; Parquet uses
those same columns. Existing timestamp caches are not trusted or modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time
from typing import Callable
import urllib.error
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory

MAX_KEYS = 1_000
MAX_RPC_ATTEMPTS = 350  # HTTP batch attempts; individual method items also saved.
MAX_RESPONSE_BYTES = 16 * 1024**2
MAX_TOTAL_RESPONSE_BYTES = 256 * 1024**2
MAX_OUTPUT_BYTES = 128_000_000
MAX_RUNTIME_SECONDS = 1_200
MAX_RAM_BYTES = 8 * 1024**3
FREE_FLOOR_BYTES = 20_000_000_000
MAX_REQUEST_FILE_BYTES = 1024**2
BATCH_SIZE = 10
MAX_ATTEMPTS_PER_BATCH = 3
TERMINAL_RESERVE = 1024**2
HASH = re.compile(r"0x[0-9a-fA-F]{64}")
SHA = re.compile(r"[0-9a-f]{64}")
HEAD = re.compile(r"[0-9a-f]{40}")
QUANTITY = re.compile(r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]*)")
CAPS = {
    "maximum_keys": MAX_KEYS, "maximum_rpc_batch_attempts": MAX_RPC_ATTEMPTS,
    "maximum_response_bytes": MAX_RESPONSE_BYTES,
    "maximum_total_response_bytes": MAX_TOTAL_RESPONSE_BYTES,
    "maximum_all_output_bytes": MAX_OUTPUT_BYTES, "maximum_runtime_seconds": MAX_RUNTIME_SECONDS,
    "maximum_address_space_bytes": MAX_RAM_BYTES, "spill_bytes": 0,
    "untouched_free_floor_bytes": FREE_FLOOR_BYTES, "batch_size": BATCH_SIZE,
    "maximum_attempts_per_batch": MAX_ATTEMPTS_PER_BATCH,
}
SCOPE = "Literal requested keys only; no history completeness, economic-fill or final-sample admission."


class PilotBlocked(ValueError):
    """Only fixed non-sensitive codes are saved or displayed."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise PilotBlocked(code)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_object_key")
        result[key] = value
    return result


def decode_json(raw: bytes):
    try:
        return json.loads(raw, object_pairs_hook=unique_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(PilotBlocked("nonfinite_json")))
    except PilotBlocked:
        raise
    except Exception:
        raise PilotBlocked("malformed_json") from None


def identity(path: Path) -> dict:
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and not path.is_symlink(), "input_not_regular_file")
    return {"path": str(path.absolute()), "device": info.st_dev, "inode": info.st_ino,
            "bytes": info.st_size, "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
            "mode": info.st_mode, "uid": info.st_uid, "gid": info.st_gid}


def read_requests(path: Path, expected_sha256: str) -> tuple[list[dict], dict]:
    require(type(expected_sha256) is str and SHA.fullmatch(expected_sha256), "invalid_request_sha256")
    before = identity(path)
    require(0 < before["bytes"] <= MAX_REQUEST_FILE_BYTES, "request_file_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_REQUEST_FILE_BYTES + 1)
    require(len(raw) <= MAX_REQUEST_FILE_BYTES and before == identity(path), "request_file_changed")
    require(digest(raw) == expected_sha256, "request_file_sha256_differs")
    if path.suffix.lower() == ".json":
        values = decode_json(raw)
    elif path.suffix.lower() == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq
        parquet = pq.ParquetFile(pa.BufferReader(raw))
        names = parquet.schema_arrow.names
        require(len(names) == len(set(names)) and set(names) in
                ({"block_number"}, {"block_number", "block_hash"}), "request_parquet_columns_differ")
        require(0 < parquet.metadata.num_rows <= MAX_KEYS, "request_key_count_limit")
        require(sum(parquet.metadata.row_group(index).total_byte_size
                    for index in range(parquet.metadata.num_row_groups)) <= 4 * MAX_REQUEST_FILE_BYTES,
                "request_parquet_expansion_limit")
        require(pa.types.is_integer(parquet.schema_arrow.field("block_number").type),
                "request_block_number_type")
        if "block_hash" in names:
            require(pa.types.is_string(parquet.schema_arrow.field("block_hash").type), "request_block_hash_type")
        values = parquet.read().to_pylist()
    else:
        raise PilotBlocked("request_file_extension")
    require(type(values) is list and 0 < len(values) <= MAX_KEYS, "request_key_count_limit")
    keys, seen = [], set()
    for row in values:
        require(type(row) is dict and set(row) in
                ({"block_number"}, {"block_number", "block_hash"}), "request_row_schema")
        number = row["block_number"]
        require(type(number) is int and 0 < number <= 2**63 - 1, "request_block_number_invalid")
        require(number not in seen, "duplicate_request_block_key")
        seen.add(number)
        block_hash = row.get("block_hash")
        require(block_hash is None or (type(block_hash) is str and HASH.fullmatch(block_hash)),
                "request_block_hash_invalid")
        keys.append({"block_number": number, "block_hash": block_hash.lower() if block_hash else None})
    keys.sort(key=lambda item: item["block_number"])
    return keys, {"identity": before, "sha256": expected_sha256, "keys": len(keys),
                  "normalized_keys_sha256": digest(json_bytes(keys))}


def source_snapshot(expected_head: str) -> dict:
    require(type(expected_head) is str and HEAD.fullmatch(expected_head), "invalid_expected_head")
    state = inventory.code_state()
    require(state.get("status") == "inspected" and state.get("head") == expected_head
            and state.get("tracked_worktree_clean") is True, "source_head_or_clean_state_differs")
    files = {}
    for name in ("scripts/collect_polymarket_exact_headers_v1.py",
                 "scripts/inventory_polymarket_refresh.py", "production_guard.py"):
        saved = subprocess.run(["git", "show", f"HEAD:{name}"], cwd=REPO, capture_output=True, timeout=10)
        require(saved.returncode == 0 and saved.stdout == (REPO / name).read_bytes(),
                "source_dependency_uncommitted_or_changed")
        files[name] = digest(saved.stdout)
    return {"head": expected_head, "tracked_worktree_clean": True, "files": files}


def validate_destination(output: Path, request_file: Path, private_file: Path) -> None:
    require(output.is_absolute() and output.parent.is_dir() and not output.exists()
            and not output.is_symlink(), "output_must_be_new_absolute_directory")
    require(output.parent.resolve() == output.parent, "output_parent_symlink")
    for item in (request_file, private_file):
        resolved = item.resolve()
        require(resolved != output and output not in resolved.parents and resolved not in output.parents,
                "output_overlaps_input")


class Budget:
    def __init__(self, output: Path, clock: Callable = time.monotonic):
        self.output, self.clock, self.started = output, clock, clock()
        self.rpc_attempts = self.rpc_method_items = self.response_bytes = 0

    def elapsed(self) -> float:
        return max(0.0, self.clock() - self.started)

    def check_time(self) -> None:
        require(self.elapsed() < MAX_RUNTIME_SECONDS, "runtime_deadline_exceeded")

    def retained_bytes(self) -> int:
        total, count = 0, 0
        for folder, directories, names in os.walk(self.output, followlinks=False):
            for name in [*directories, *names]:
                item = Path(folder) / name
                info = item.lstat()
                require(not stat.S_ISLNK(info.st_mode), "unexpected_output_symlink")
                if stat.S_ISREG(info.st_mode):
                    total += info.st_size
                    count += 1
                else:
                    require(stat.S_ISDIR(info.st_mode), "unexpected_output_file_type")
                require(count <= 2_000, "output_file_count_limit")
        return total

    def check_space(self, reservation: int = 0, *, terminal: bool = False) -> None:
        retained = self.retained_bytes()
        ceiling = MAX_OUTPUT_BYTES if terminal else MAX_OUTPUT_BYTES - TERMINAL_RESERVE
        require(retained + reservation <= ceiling, "output_byte_limit")
        require(shutil.disk_usage(self.output).free - reservation >= FREE_FLOOR_BYTES, "free_space_floor")

    def publish(self, relative: str, raw: bytes, *, terminal: bool = False) -> dict:
        if not terminal:
            self.check_time()
        self.check_space(2 * len(raw), terminal=terminal)  # Charge even the transient link twice.
        target = self.output / relative
        require(target.parent == self.output and not target.exists() and not target.is_symlink(),
                "output_already_exists_or_nested")
        pending = self.output / ("." + target.name + "." + uuid.uuid4().hex + ".pending")
        with pending.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        require(pending.read_bytes() == raw, "staged_output_reopen_differs")
        if not terminal:
            self.check_time()
        self.check_space(len(raw), terminal=terminal)
        os.link(pending, target)
        pending.unlink()
        fd = os.open(self.output, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return {"path": relative, "bytes": len(raw), "sha256": digest(raw)}

    def usage(self) -> dict:
        return {"rpc_batch_attempts": self.rpc_attempts, "rpc_method_items": self.rpc_method_items,
                "charged_response_bytes": self.response_bytes, "retained_bytes_before_terminal": self.retained_bytes(),
                "elapsed_seconds": self.elapsed()}


def http_response(endpoint: str, payload: object, *, byte_limit: int, timeout: float) -> tuple[bytes, int]:
    request = urllib.request.Request(endpoint, data=json_bytes(payload),
                                     headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read(byte_limit + 1), response.status
    except urllib.error.HTTPError as response:
        with response:
            return response.read(byte_limit + 1), response.code


def quantity(value: object) -> int:
    require(type(value) is str and 0 < len(value) <= 66 and QUANTITY.fullmatch(value), "rpc_noncanonical_quantity")
    return int(value, 16)


def validate_header(value: object, key: dict) -> dict:
    require(type(value) is dict, "rpc_header_missing_or_not_object")
    number, timestamp = quantity(value.get("number")), quantity(value.get("timestamp"))
    require(number == key["block_number"], "rpc_returned_block_number_differs")
    require(0 < timestamp <= 253402300799, "rpc_header_timestamp_invalid")
    for field in ("hash", "parentHash"):
        require(type(value.get(field)) is str and HASH.fullmatch(value[field]), "rpc_header_hash_invalid")
    block_hash = value["hash"].lower()
    require(key["block_hash"] is None or key["block_hash"] == block_hash, "native_block_hash_differs")
    return {"block_number": number, "block_hash": block_hash,
            "parent_hash": value["parentHash"].lower(), "timestamp": timestamp}


def validate_clock(headers: list[dict]) -> None:
    ordered = sorted(headers, key=lambda item: item["block_number"])
    require(len({item["block_number"] for item in ordered}) == len(ordered), "duplicate_collected_block_key")
    for previous, current in zip(ordered, ordered[1:]):
        require(previous["timestamp"] <= current["timestamp"], "nonmonotone_header_clock")
        if previous["block_number"] + 1 == current["block_number"]:
            require(current["parent_hash"] == previous["block_hash"], "adjacent_header_parent_hash_differs")


class RPC:
    def __init__(self, endpoint: str, budget: Budget, contract_sha: str, transport: Callable):
        self.endpoint, self.budget, self.contract_sha, self.transport = endpoint, budget, contract_sha, transport
        self.next_id = 1
        self.receipts = []
        self.evidence = []

    def reopen_evidence(self) -> None:
        for reference, expected in self.evidence:
            path = self.budget.output / reference["path"]
            require(not path.is_symlink() and path.is_file()
                    and path.stat().st_size == reference["bytes"], "rpc_evidence_identity_differs")
            raw = path.read_bytes()
            require(digest(raw) == reference["sha256"] and decode_json(raw) == expected,
                    "rpc_evidence_changed")

    def call(self, keys: list[dict] | None = None) -> list[dict] | int:
        require(keys is None or 0 < len(keys) <= BATCH_SIZE, "invalid_rpc_batch_size")
        for attempt in range(1, MAX_ATTEMPTS_PER_BATCH + 1):
            self.budget.check_time()
            require(self.budget.rpc_attempts < MAX_RPC_ATTEMPTS, "rpc_attempt_limit")
            remaining = MAX_TOTAL_RESPONSE_BYTES - self.budget.response_bytes
            require(remaining > 1, "total_response_byte_limit")
            # Reserve the possible oversize sentinel inside the cumulative ceiling.
            byte_limit = min(MAX_RESPONSE_BYTES, remaining - 1)
            numbers = [None] if keys is None else keys
            requests = []
            for key in numbers:
                requests.append({"jsonrpc": "2.0", "id": self.next_id,
                                 "method": "eth_chainId" if key is None else "eth_getBlockByNumber",
                                 "params": [] if key is None else [hex(key["block_number"]), False]})
                self.next_id += 1
            payload = requests[0] if keys is None else requests
            call = self.budget.rpc_attempts + 1
            prefix = f"rpc_{call:04d}"
            start = {"contract_sha256": self.contract_sha, "call": call, "batch_attempt": attempt,
                     "requests": requests, "response_byte_limit": byte_limit,
                     "failure_response_reservation_bytes": byte_limit + 1}
            start_ref = self.budget.publish(prefix + ".START.json", json_bytes(start))
            self.evidence.append((start_ref, start))
            self.budget.rpc_attempts += 1
            self.budget.rpc_method_items += len(requests)
            finish = {"call": call, "contract_sha256": self.contract_sha, "batch_attempt": attempt}
            retry = False
            try:
                try:
                    raw, status = self.transport(self.endpoint, payload, byte_limit=byte_limit,
                                                 timeout=max(0.01, min(30.0, MAX_RUNTIME_SECONDS - self.budget.elapsed())))
                except Exception as exc:
                    # A transport exception may follow a partial read. Charge its entire
                    # bounded-read reservation, never assume the failed attempt was free.
                    self.budget.response_bytes += byte_limit + 1
                    finish.update(status="transport_failed", charged_response_bytes=byte_limit + 1,
                                  response_size_is_upper_bound=True)
                    retry = not isinstance(exc, PilotBlocked)
                    if isinstance(exc, PilotBlocked):
                        raise
                    raise PilotBlocked("rpc_transport_failure") from None
                require(type(raw) is bytes and type(status) is int, "transport_result_shape")
                self.budget.response_bytes += len(raw)
                finish.update(charged_response_bytes=len(raw), response_sha256=digest(raw),
                              response_size_is_upper_bound=False, http_status=status)
                require(self.budget.response_bytes <= MAX_TOTAL_RESPONSE_BYTES, "total_response_byte_limit")
                require(len(raw) <= byte_limit, "individual_response_byte_limit")
                self.budget.check_time()
                if status == 429 or 500 <= status < 600:
                    retry = True
                    raise PilotBlocked("rpc_retryable_http_failure")
                require(200 <= status < 300, "rpc_http_failure")
                values = decode_json(raw)
                if keys is None:
                    require(type(values) is dict, "rpc_reply_batch_shape")
                    values = [values]
                else:
                    require(type(values) is list and len(values) == len(requests), "rpc_reply_batch_shape")
                by_id = {}
                for item in values:
                    require(type(item) is dict and set(item) == {"jsonrpc", "id", "result"}
                            and item["jsonrpc"] == "2.0" and type(item["id"]) is int, "rpc_reply_shape")
                    require(item["id"] not in by_id, "rpc_duplicate_reply_id")
                    by_id[item["id"]] = item["result"]
                require(set(by_id) == {item["id"] for item in requests}, "rpc_reply_id_set_differs")
                if keys is None:
                    result = quantity(by_id[requests[0]["id"]])
                    require(result == 137, "rpc_wrong_chain")
                else:
                    result = [validate_header(by_id[request["id"]], key)
                              for request, key in zip(requests, keys)]
                    validate_clock(result)
                finish["status"] = "validated"
                ref = self.budget.publish(prefix + ".FINISH.json", json_bytes(finish))
                self.receipts.append(ref)
                self.evidence.append((ref, finish))
                return result
            except PilotBlocked as exc:
                finish.setdefault("status", "failed")
                finish["error_code"] = str(exc)
                ref = self.budget.publish(prefix + ".FINISH.json", json_bytes(finish), terminal=True)
                self.receipts.append(ref)
                self.evidence.append((ref, finish))
                if not retry or attempt == MAX_ATTEMPTS_PER_BATCH:
                    raise
        raise PilotBlocked("rpc_attempts_exhausted")


def parquet_bytes(headers: list[dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([("block_number", pa.int64()), ("block_hash", pa.string()),
                        ("parent_hash", pa.string()), ("timestamp", pa.int64())])
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(headers, schema=schema), sink, compression="zstd")
    raw = sink.getvalue().to_pybytes()
    reopened = pq.read_table(pa.BufferReader(raw))
    require(reopened.schema == schema and reopened.to_pylist() == headers, "header_parquet_roundtrip_differs")
    return raw


def reopen_chunks(output: Path, chunks: list[dict], required: list[dict]) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    headers = []
    for chunk in chunks:
        for binding in (chunk["payload"], chunk["receipt"]):
            path = output / binding["path"]
            require(path.parent == output and not path.is_symlink() and path.is_file(), "chunk_not_regular")
            require(path.stat().st_size == binding["bytes"], "chunk_bytes_differ")
            raw = path.read_bytes()
            require(digest(raw) == binding["sha256"], "chunk_hash_differs")
            if binding is chunk["payload"]:
                rows = pq.read_table(pa.BufferReader(raw)).to_pylist()
                require(rows == chunk["headers"], "chunk_rows_differ")
                headers.extend(rows)
            else:
                require(decode_json(raw) == chunk["receipt_value"], "chunk_receipt_differs")
    require([row["block_number"] for row in headers] == [key["block_number"] for key in required],
            "literal_required_key_coverage_differs")
    validate_clock(headers)
    return headers


def run_pilot(request_file: Path, request_sha256: str, expected_head: str, output: Path, *,
              rpc_private_file: Path | None = None, transport: Callable = http_response,
              clock: Callable = time.monotonic) -> dict:
    private = rpc_private_file or Path.home() / ".polygon_rpc_url"
    validate_destination(output, request_file, private)
    keys, binding = read_requests(request_file, request_sha256)
    source = source_snapshot(expected_head)
    private_before = identity(private)
    endpoint, configuration_source = inventory.load_rpc_endpoint(private)
    require(configuration_source != "private_file" or identity(private) == private_before,
            "rpc_private_file_changed_during_admission")
    require(shutil.disk_usage(output.parent).free >= FREE_FLOOR_BYTES + TERMINAL_RESERVE,
            "free_space_floor")
    output.mkdir(mode=0o700)
    budget = Budget(output, clock)
    contract = {"schema_version": "polymarket_exact_header_pilot_contract_v1", "source": source,
                "request_file": binding, "caps": CAPS, "scope": SCOPE,
                "cache_reuse": False, "final_sample_admitted": False,
                "timestamp_semantics": "Authoritative chain integer-second block time, shared by all fills in that block.",
                "rpc_configuration_source": configuration_source,
                "rpc_private_file_identity": private_before if configuration_source == "private_file" else None}
    chunks, headers = [], []
    result = {"schema_version": "polymarket_exact_header_pilot_result_v1", "status": "blocked",
              "created_utc": datetime.now(timezone.utc).isoformat(), "scope": SCOPE,
              "final_sample_admitted": False, "history_completeness_certified": False,
              "requested_keys": len(keys), "accepted_keys": 0, "contract": contract}
    try:
        contract_ref = budget.publish("CONTRACT.json", json_bytes(contract))
        result["contract_binding"] = contract_ref
        rpc = RPC(endpoint, budget, contract_ref["sha256"], transport)
        rpc.call()
        for offset in range(0, len(keys), BATCH_SIZE):
            selected = keys[offset:offset + BATCH_SIZE]
            before = len(rpc.receipts)
            batch = rpc.call(selected)
            validate_clock([*headers, *batch])
            index = offset // BATCH_SIZE + 1
            payload = budget.publish(f"headers_{index:04d}.parquet", parquet_bytes(batch))
            receipt_value = {"contract_sha256": contract_ref["sha256"], "chunk": index,
                             "requested_keys": selected, "headers": batch, "payload": payload,
                             "rpc_finish_receipts": rpc.receipts[before:]}
            receipt = budget.publish(f"headers_{index:04d}.json", json_bytes(receipt_value))
            chunks.append({"payload": payload, "receipt": receipt,
                           "headers": batch, "receipt_value": receipt_value})
            headers.extend(batch)
        rpc.call()  # The same source must still identify Polygon at finalization.
        reopen_keys = [{"block_number": row["block_number"], "block_hash": row["block_hash"]}
                       for row in ([headers[0]] if len(headers) == 1 else [headers[0], headers[-1]])]
        repeated = rpc.call(reopen_keys)
        require(repeated == ([headers[0]] if len(headers) == 1 else [headers[0], headers[-1]]),
                "reopened_header_changed")
        reopened = reopen_chunks(output, chunks, keys)
        require(reopened == headers, "all_headers_reopen_differs")
        rpc.reopen_evidence()
        require((output / "CONTRACT.json").read_bytes() == json_bytes(contract), "contract_changed_during_run")
        final_keys, final_binding = read_requests(request_file, request_sha256)
        require(final_keys == keys and final_binding == binding, "request_file_changed_during_run")
        require(configuration_source != "private_file" or identity(private) == private_before,
                "rpc_private_file_changed_during_run")
        require(source_snapshot(expected_head) == source, "source_changed_during_run")
        budget.check_time()
        budget.check_space()
        result.update(status="literal_header_pilot_complete", accepted_keys=len(headers), missing_keys=0,
                      request_file_reopened_unchanged=True, source_reopened_unchanged=True,
                      exact_key_coverage=True, duplicate_keys=0, fallback_rows=0,
                      chunk_bindings=[{"payload": item["payload"], "receipt": item["receipt"]} for item in chunks],
                      rpc_finish_receipts=rpc.receipts, rpc_configuration_source=configuration_source,
                      usage=budget.usage())
        # RESULT is the sole completion commit; partial artifacts never establish success.
        budget.publish("RESULT.json", json_bytes(result))
    except BaseException as exc:
        code = str(exc) if isinstance(exc, PilotBlocked) else "interrupted" if isinstance(exc, KeyboardInterrupt) else "pilot_input_transport_or_resource_failure"
        result.update(status="blocked", error_code=code, accepted_keys=len(headers), usage=budget.usage())
        try:
            budget.publish("FAILED.json", json_bytes(result), terminal=True)
        except Exception:
            result["failure_receipt_published"] = False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request-file", required=True, type=Path)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()  # Before any production request/configuration input read.
        require(args.output_dir.is_absolute() and Path("/mnt/data/runs") in args.output_dir.resolve().parents,
                "output_outside_production_run_root")
        require(inventory.resource_state()["ram_bytes"]["MemAvailable"] >= MAX_RAM_BYTES,
                "insufficient_available_ram")
        for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[name] = "1"
        resource.setrlimit(resource.RLIMIT_AS, (MAX_RAM_BYTES, MAX_RAM_BYTES))
        previous = signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(PilotBlocked("runtime_deadline_exceeded")))
        signal.setitimer(signal.ITIMER_REAL, MAX_RUNTIME_SECONDS)
        try:
            result = run_pilot(args.request_file, args.request_sha256, args.expected_head, args.output_dir,
                               rpc_private_file=args.rpc_private_file)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
    except Exception:
        parser.error("Exact-header pilot admission failed; no completion published.")
    print(json.dumps({"status": result["status"], "accepted_keys": result["accepted_keys"],
                      "final_sample_admitted": False}, sort_keys=True))
    return 0 if result["status"] == "literal_header_pilot_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
