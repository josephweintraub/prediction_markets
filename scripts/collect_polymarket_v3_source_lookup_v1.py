"""Acquire two bounded Sourcify v2 records. RAW records, never source proof.

Four attempts total, two attempts/address, no redirects/proxies/auth, no RPC,
POST, compiler, downloads execution, imports, URL following, or sample admission.
Only exact public response bodies and bounded descriptive receipts are retained.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import probe_polymarket_v3_history_v2 as shared

require, encode, digest = shared.require, shared.encode, shared.digest
inventory = shared.inventory
RUNTIME_SHA = "761eb05565361d5303e14a4b0fceb21dbc5b18b6181879ce3aea3f53cf1f156b"
PLAN_SHA = "b07dfa71169029913a2281a2cb9df3d567d620be89aaa14b84eca5e674e0f688"
PLAN_HEAD = "c39ddce3d9a010fe65faac652359fd401561e877"
IMPLEMENTATIONS = (shared.prior.OLD_IMPLEMENTATION, shared.prior.NEW_IMPLEMENTATION)
RUNTIMES = {
    IMPLEMENTATIONS[0]: (22204, "dda5fcc0be82f96ae766bf269b5c8f87871cf458fac18423b265ac57123901fb"),
    IMPLEMENTATIONS[1]: (22256, "2dfc1d2e867a7fa851cb195cc7887b614442f4f2de04d3081155a99bd502c6d6"),
}
FIELDS = ("compilation", "metadata", "stdJsonInput", "runtimeBytecode", "creationBytecode", "deployment", "abi")
URLS = tuple("https://sourcify.dev/server/v2/contract/137/" + address + "?" +
             urllib.parse.urlencode({"fields": ",".join(FIELDS)}) for address in IMPLEMENTATIONS)
USER_AGENT = "prediction-markets-v3-source-lookup/1 (+https://github.com/josephweintraub/prediction_markets)"
MAX_ATTEMPTS, MAX_PER_ADDRESS = 4, 2
MAX_RESPONSE, MAX_TOTAL_RESPONSE = 8 * 1024**2, 32 * 1024**2
MAX_OUTPUT, MAX_RUNTIME, RAM, FREE_FLOOR = 64 * 1024**2, 1200, 8 * 1024**3, 20_000_000_000
CONTROL_RESERVE = shared.MAX_OUTPUT_BYTES  # Unmodified reviewed Controls: 1 MiB.
COMPLETE = "bounded_sourcify_raw_lookup_complete_not_source_equivalence"
SOURCE_FILES = (*shared.SOURCE_FILES, "scripts/collect_polymarket_v3_source_lookup_v1.py",
                "tests/test_polymarket_v3_source_lookup_v1.py")


def limits():
    return {"http_attempts": MAX_ATTEMPTS, "attempts_per_fixed_address": MAX_PER_ADDRESS,
            "wire_body_bytes_per_attempt_including_detector": MAX_RESPONSE,
            "total_charged_wire_body_bytes": MAX_TOTAL_RESPONSE,
            "all_retained_staging_control_bytes": MAX_OUTPUT, "control_reserve_bytes": CONTROL_RESERVE,
            "runtime_seconds": MAX_RUNTIME, "address_space_bytes": RAM, "free_disk_floor_bytes": FREE_FLOOR,
            "workers": 1, "spill_bytes": 0, "rpc_calls": 0, "redirects": 0, "proxies": False,
            "authentication": False, "compiler_execution": False}


def source_snapshot(expected_head):
    state = inventory.code_state()
    require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head)
            and state.get("status") == "inspected" and state.get("head") == expected_head
            and state.get("tracked_worktree_clean") is True, "lookup_source_head_not_clean")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = inventory.path_identity(path)
        require(before.get("status") == "present" and not before.get("is_symlink")
                and path.is_file() and 0 < path.stat().st_size <= 1024**2, "lookup_source_file_invalid")
        saved = subprocess.run(["git", "show", f"{expected_head}:{name}"], cwd=REPO,
                               capture_output=True, timeout=10)
        require(saved.returncode == 0 and saved.stdout == path.read_bytes()
                and before == inventory.path_identity(path), "lookup_source_blob_uncommitted_or_changed")
        files[name] = {"sha256": digest(saved.stdout), "identity": before}
    require(state == inventory.code_state(), "lookup_source_changed_during_snapshot")
    return {"head": expected_head, "tracked_worktree_clean": True, "files": files}


def read_runtime(path: Path, sha: str):
    require(sha == RUNTIME_SHA, "runtime_input_not_frozen_receipt")
    before = inventory.path_identity(path)
    require(before.get("status") == "present" and not before.get("is_symlink")
            and path.is_file() and 0 < path.stat().st_size <= 1024**2, "runtime_input_not_bounded_regular")
    raw = path.read_bytes()
    require(digest(raw) == sha and before == inventory.path_identity(path), "runtime_input_hash_or_identity_differs")
    value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(shared.HistoryBlocked("runtime_input_nonfinite")))
    require(type(value) is dict and value.get("schema_version") == "polymarket_v3_runtime_probe_v1"
            and value.get("status") == "bounded_v3_runtime_observations_complete"
            and value.get("chain_id") == 137 and value.get("address") == shared.ADDRESS
            and value.get("source_equivalence_admitted") is False and value.get("analysis_admitted") is False
            and value.get("source_plan_binding", {}).get("sha256") == PLAN_SHA
            and value.get("source_plan_binding", {}).get("source_head") == PLAN_HEAD,
            "runtime_input_schema_or_scope_differs")
    blobs = value.get("runtime_blobs")
    require(type(blobs) is dict and set(blobs) == {item[1] for item in RUNTIMES.values()}, "runtime_blob_roster_differs")
    for size, sha256 in RUNTIMES.values():
        blob = blobs[sha256]
        require(type(blob) is str and re.fullmatch(r"0x[0-9a-fA-F]+", blob)
                and len(blob) == 2 + 2 * size and digest(bytes.fromhex(blob[2:])) == sha256, "runtime_blob_hash_differs")
    observations = value.get("runtime_observations")
    require(type(observations) is list and len(observations) == 4, "runtime_observations_incomplete")
    expected = {(IMPLEMENTATIONS[0], 88978537), (IMPLEMENTATIONS[0], 93452331),
                (IMPLEMENTATIONS[1], 93452332), (IMPLEMENTATIONS[1], 94796039)}
    seen = set()
    for item in observations:
        key = (item.get("implementation_address"), item.get("block_number"))
        require(key in expected and key not in seen, "runtime_observation_locator_differs")
        size, sha256 = RUNTIMES[key[0]]
        require(item.get("runtime") == {"bytes": size, "sha256": sha256}
                and item.get("runtime_blob_key") == sha256, "runtime_observation_binding_differs")
        seen.add(key)
    return value, {"path": str(path), "sha256": sha, "identity": before}


@dataclass(frozen=True)
class Response:
    body: bytes
    status: int
    url: str
    content_type: str | None = None
    retry_after: str | None = None
    content_length: str | None = None
    content_encoding: str | None = None
    complete: bool = True
    error_code: str | None = None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get_response(url: str, *, byte_limit: int, timeout: float):
    require(url in URLS and type(byte_limit) is int and 0 < byte_limit <= MAX_RESPONSE
            and 0 < timeout <= 30, "get_transport_request_not_permitted")
    request = urllib.request.Request(url, method="GET", headers={"Accept": "application/json",
        "Accept-Encoding": "identity", "User-Agent": USER_AGENT})
    # Local opener only. Do not install a global opener, inherit proxy env,
    # enable auth/cookies, or follow an API-supplied Location/source URL.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        response = exc  # 404/redirect/error body is evidence, not an exception to erase.
    body, complete, error = bytearray(), False, None
    try:
        status, received_url = response.status, response.geturl()
        headers = {}
        for key in ("Content-Type", "Retry-After", "Content-Length", "Content-Encoding"):
            values = response.headers.get_all(key, [])
            require(len(values) <= 1 and all(type(item) is str and len(item) <= 8192 for item in values),
                    "response_headers_not_bounded_single_values")
            headers[key] = values[0] if values else None
        try:
            while len(body) < byte_limit:
                amount = min(64 * 1024, byte_limit - len(body))
                part = response.read(amount)
                require(type(part) is bytes and len(part) <= amount, "get_stream_shape_or_cap_invalid")
                if not part:
                    complete = True
                    break
                body.extend(part)
        except http.client.IncompleteRead as exc:
            part = exc.partial
            require(type(part) is bytes and len(part) <= byte_limit - len(body), "get_partial_body_exceeds_cap")
            body.extend(part)
            error = "incomplete_http_body"
        except BaseException as exc:
            error = "interrupted_http_body" if isinstance(exc, (KeyboardInterrupt, SystemExit, shared.HistoryBlocked)) else "http_body_read_failed"
        if len(body) == byte_limit:
            complete, error = False, "response_body_at_detector_limit"
        declared = headers["Content-Length"]
        if declared is not None and (not re.fullmatch(r"[0-9]{1,20}", declared) or int(declared) != len(body)):
            complete, error = False, "declared_http_length_differs"
    finally:
        # A context manager's failing __exit__/close would discard an already
        # constructed return value and its observed prefix. Close explicitly,
        # retaining that prefix and marking the body unproven instead.
        try:
            response.close()
        except BaseException:
            complete, error = False, "http_response_close_failed"
    return Response(bytes(body), status, received_url, headers["Content-Type"], headers["Retry-After"],
                    declared, headers["Content-Encoding"], complete, error)


def describe(body: bytes, response: Response):
    """Describe untrusted fields without resolving URLs or granting any match."""
    result = {"source_equivalence_admitted": False, "embedded_urls_followed": 0,
              "schema_status": "uninterpreted_body", "reported_verification_trusted": False}
    if response.content_encoding not in (None, "identity"):
        result["schema_status"] = "encoded_body_not_decompressed"
        return result
    if response.content_type is None or not re.match(r"\Aapplication/(?:json|[^; ]+\+json)(?:;|\Z)", response.content_type.lower()):
        result["schema_status"] = "content_type_not_json"
        return result
    try:
        value = json.loads(body, object_pairs_hook=inventory.unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")))
    except Exception:
        result["schema_status"] = "invalid_or_unsupported_json"
        return result
    if type(value) is not dict:
        result["schema_status"] = "json_not_object"
        return result
    result.update(schema_status="json_object_described_only", selected_field_types={
        key: type(value[key]).__name__ if key in value else "absent" for key in FIELDS})
    standard = value.get("stdJsonInput")
    sources = standard.get("sources") if type(standard) is dict else None
    if type(sources) is dict:
        result["reported_source_files"] = len(sources)
        result["source_entries_with_literal_content"] = sum(type(item) is dict and type(item.get("content")) is str for item in sources.values())
        result["source_entries_with_urls_not_followed"] = sum(type(item) is dict and "urls" in item for item in sources.values())
    return result


class Bodies:
    def __init__(self, root):
        self.root, self.expected = root, {}
        root.mkdir(mode=0o700)
        self.identity = (root.stat().st_dev, root.stat().st_ino)

    def verify(self):
        require(self.root.is_dir() and not self.root.is_symlink()
                and (self.root.stat().st_dev, self.root.stat().st_ino) == self.identity, "response_directory_changed")
        require({path.name for path in self.root.iterdir()} == set(self.expected), "response_artifact_roster_changed")
        total = 0
        for name, expected in self.expected.items():
            path = self.root / name
            require(path.is_file() and not path.is_symlink() and path.stat().st_size == expected["bytes"], "response_artifact_size_or_type_changed")
            require(digest(path.read_bytes()) == expected["sha256"], "response_artifact_hash_changed")
            total += expected["bytes"]
        require(total + CONTROL_RESERVE <= MAX_OUTPUT, "all_retained_output_cap")
        return {"bytes": total, "files": self.expected.copy()}

    def write(self, identifier, body):
        before = self.verify()
        require(type(body) is bytes and len(body) <= MAX_RESPONSE
                and before["bytes"] + len(body) + CONTROL_RESERVE <= MAX_OUTPUT, "response_retention_cap")
        require(shutil.disk_usage(self.root).free - len(body) >= FREE_FLOOR, "free_disk_floor_would_be_crossed")
        name, pending = f"response_{identifier:03d}.raw", f".response_{identifier:03d}.pending"
        require(name not in self.expected and pending not in self.expected, "response_file_already_owned")
        record = {"bytes": len(body), "sha256": digest(body)}
        self.expected[pending] = record  # Reserve even a partial failed retained write.
        temporary, destination = self.root / pending, self.root / name
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        self.verify()
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self.verify()
        shared.atomic_completion(temporary, destination)
        self.expected[name] = self.expected.pop(pending)
        return {"path": "responses/" + name, **record}


class BoundControls(shared.Controls):
    """Reopen external bindings at the reviewed marker kernel's checkpoints."""

    def __init__(self, root, owner, validate_external):
        self.validate_external = validate_external
        super().__init__(root, owner)

    def verify(self):
        result = super().verify()
        # The inherited completion kernel invokes this after pending-marker
        # fsync/reopen and again after directory fsync, before atomic commit.
        # Blocked diagnostics still need to preserve their failure receipt.
        if self.pending_marker:
            self.validate_external()
        return result


def retry_delay(raw: str | None):
    if raw is None:
        return 2.0
    if re.fullmatch(r"[0-9]+", raw):
        # An arbitrarily long valid delta must not become a two-second retry.
        # The bounded sentinel blocks the next request at the stage deadline.
        return float(MAX_RUNTIME + 1) if len(raw) > 10 else max(2.0, float(raw))
    try:
        parsed = parsedate_to_datetime(raw)
        require(parsed.tzinfo is not None, "retry_after_timezone_absent")
        return max(2.0, (parsed - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return 2.0


def safe_failure(exc):
    if isinstance(exc, shared.HistoryBlocked) and shared.SAFE_CODE.fullmatch(str(exc)):
        return str(exc)
    return "lookup_transport_or_control_failed"


def collect(runtime_path: Path, runtime_sha: str, expected_head: str, *, run_dir: Path,
            transport=get_response, clock=time.monotonic, sleep=time.sleep):
    require_production_host()
    with shared.runtime_limits() as enforcement:
        return _collect(runtime_path, runtime_sha, expected_head, run_dir=run_dir, transport=transport,
                        clock=clock, sleep=sleep, started=enforcement["started_monotonic"],
                        enforcement={key: value for key, value in enforcement.items() if key != "started_monotonic"})


def _collect(runtime_path, runtime_sha, expected_head, *, run_dir, transport=get_response,
             clock=time.monotonic, sleep=time.sleep, started=None, enforcement=None):
    started = clock() if started is None else started
    def remaining():
        left = MAX_RUNTIME - (clock() - started)
        require(left > 0, "lookup_runtime_deadline")
        return left
    runtime, binding = read_runtime(runtime_path, runtime_sha)
    source = source_snapshot(expected_head)
    require(run_dir.is_absolute() and shared.RUN_ROOT.resolve() in run_dir.resolve().parents
            and not run_dir.exists() and not run_dir.is_symlink() and run_dir.parent.is_dir()
            and not run_dir.parent.is_symlink(), "lookup_output_not_fresh")
    require(run_dir.resolve() not in runtime_path.resolve().parents
            and runtime_path.resolve() not in run_dir.resolve().parents, "lookup_output_overlaps_input")
    resources = inventory.resource_state(run_dir.parent)
    require(resources.get("ram_bytes", {}).get("MemAvailable", 0) >= RAM
            and resources.get("disk_bytes", {}).get("free", 0) >= FREE_FLOOR + MAX_OUTPUT, "lookup_resources_not_admitted")
    remaining()
    run_dir.mkdir(mode=0o700)
    run_identity = (run_dir.stat().st_dev, run_dir.stat().st_ino)
    bodies = Bodies(run_dir / "responses")
    def validate_external():
        require(source_snapshot(expected_head) == source and read_runtime(runtime_path, runtime_sha) == (runtime, binding),
                "lookup_source_or_input_changed")
        remaining()
    controls = BoundControls(run_dir / "controls", {"schema_version": "polymarket_v3_source_lookup_owner_v1",
        "created_utc": shared.now(), "runtime_observation": binding, "source": source, "limits": limits(),
        "urls": list(URLS), "fields": list(FIELDS), "resume_permitted": False,
        "source_equivalence_admitted": False, "economic_decoding_admitted": False, "analysis_admitted": False},
        validate_external)
    result = {"schema_version": "polymarket_v3_source_lookup_result_v1", "status": "blocked",
              "runtime_observation": binding, "source": source, "limits": limits(), "attempts": [], "lookups": [],
              "source_equivalence_admitted": False, "economic_decoding_admitted": False,
              "analysis_admitted": False, "cid_content_verified": False, "compiler_executed": False,
              "embedded_urls_followed": 0, "rpc_calls": 0, "endpoint_credentials_used": False}
    result["runtime_enforcement"] = enforcement or {"fixture_seam_only": True}
    charged, next_allowed = 0, clock()
    try:
        for address, url in zip(IMPLEMENTATIONS, URLS):
            terminal = None
            for own_attempt in range(1, MAX_PER_ADDRESS + 1):
                while clock() < next_allowed:
                    left = remaining()
                    require(next_allowed - clock() < left, "retry_after_exceeds_remaining_deadline")
                    sleep(min(1.0, next_allowed - clock()))
                left = remaining()
                require(len(result["attempts"]) < MAX_ATTEMPTS and charged + MAX_RESPONSE <= MAX_TOTAL_RESPONSE,
                        "lookup_cumulative_attempt_or_wire_cap")
                entry = {"id": len(result["attempts"]) + 1, "address": address, "chain_id": 137,
                    "source_url": url, "method": "GET", "address_attempt": own_attempt,
                    "wire_body_reservation_bytes": MAX_RESPONSE, "charged_response_bytes": MAX_RESPONSE,
                    "status": None, "content_type": None, "body": None, "body_complete": False}
                controls.journal({"event": "start", **entry})
                result["attempts"].append(entry)
                charged += MAX_RESPONSE
                retry = False
                try:
                    response = transport(url, byte_limit=MAX_RESPONSE, timeout=min(30.0, left))
                    require(type(response) is Response and type(response.body) is bytes
                            and len(response.body) <= MAX_RESPONSE and type(response.status) is int
                            and 100 <= response.status <= 599 and type(response.complete) is bool,
                            "lookup_response_shape_invalid")
                    entry.update(status=response.status,
                        content_type=response.content_type, content_length=response.content_length,
                        content_encoding=response.content_encoding, body_complete=response.complete,
                        retry_after=response.retry_after, response_bytes_received=len(response.body),
                        received_body_sha256=digest(response.body), response_error_code=response.error_code,
                        response_url_matches_requested=response.url == url)
                    try:
                        entry["body"] = bodies.write(entry["id"], response.body)
                    except BaseException:
                        entry["retention_state"] = "pending_or_failed_publication_not_adopted"
                        raise shared.HistoryBlocked("response_retention_failed") from None
                    require(response.url == url and response.url in URLS, "lookup_response_url_changed")
                    for value in (response.content_type, response.retry_after, response.content_length, response.content_encoding):
                        require(value is None or type(value) is str and len(value) <= 8192, "lookup_response_header_invalid")
                    require(response.error_code is None or response.error_code in {
                        "incomplete_http_body", "interrupted_http_body", "http_body_read_failed",
                        "response_body_at_detector_limit", "declared_http_length_differs",
                        "http_response_close_failed"}, "lookup_response_error_code_invalid")
                    if response.complete:
                        require(response.error_code is None and len(response.body) < MAX_RESPONSE,
                                "lookup_complete_body_at_detector_or_error")
                        charged -= MAX_RESPONSE - len(response.body)
                        entry["charged_response_bytes"] = len(response.body)
                    entry["description"] = describe(response.body, response)
                    if response.complete and response.status == 200:
                        entry["availability"] = "http_200_record_retained_unverified"
                    elif response.complete and response.status == 404:
                        entry["availability"] = "unavailable_http_404"
                    else:
                        entry["availability"] = "unavailable_or_incomplete_response_retained"
                    retry = response.status == 429 or response.status >= 500 or (
                        not response.complete and response.error_code not in {"response_body_at_detector_limit", "interrupted_http_body"})
                    if retry:
                        entry["minimum_backoff_seconds"] = retry_delay(response.retry_after)
                        next_allowed = clock() + entry["minimum_backoff_seconds"]
                    require(response.error_code != "interrupted_http_body", "lookup_response_read_interrupted")
                except BaseException as exc:
                    entry["error_code"] = safe_failure(exc)
                    entry.setdefault("availability", "unavailable_transport_or_invalid_response")
                    # Transport failure has no trustworthy observed byte total.
                    # Keep its entire reservation; never expose exception text.
                    if isinstance(exc, (KeyboardInterrupt, SystemExit, shared.HistoryBlocked)):
                        raise
                    retry, next_allowed = True, clock() + 2.0
                finally:
                    controls.journal({"event": "finish", **entry})
                remaining()
                terminal = entry
                if not retry:
                    break
            result["lookups"].append({"address": address, "source_url": url, "terminal_attempt_id": terminal["id"],
                "availability": terminal["availability"], "status": terminal["status"], "source_equivalence_admitted": False})
        require([item["address"] for item in result["lookups"]] == list(IMPLEMENTATIONS), "fixed_lookup_roster_incomplete")
        require(source_snapshot(expected_head) == source and read_runtime(runtime_path, runtime_sha) == (runtime, binding),
                "lookup_source_or_input_changed")
        require(run_dir.is_dir() and not run_dir.is_symlink()
                and (run_dir.stat().st_dev, run_dir.stat().st_ino) == run_identity
                and {path.name for path in run_dir.iterdir()} == {"responses", "controls"}, "lookup_run_topology_changed")
        result["response_artifacts"] = bodies.verify()
        result["control_bindings"] = controls.verify()
        remaining()
        result["status"] = COMPLETE
    except BaseException as exc:
        result["error_code"] = safe_failure(exc)
    result.update(http_attempts=len(result["attempts"]), response_bytes_charged=charged,
                  elapsed_seconds=max(0.0, clock() - started), request_journal_sha256=controls.journal_hash.hexdigest(),
                  request_journal_records=controls.journal_records, expected_owner_sha256=controls.expected["OWNER.json"])
    # Controls retains its reviewed independent expected hashes and staged marker
    # kernel. Its separate 1 MiB reserve is inside the 64 MiB aggregate ceiling.
    controls.verify()
    controls.write("RESULT.json", result, final=True)
    if result["status"] == COMPLETE:
        remaining()
        require(bodies.verify() == result["response_artifacts"], "lookup_response_artifacts_changed_before_seal")
        controls.verify()
        controls.write("COMPLETE.json", {"status": COMPLETE, "result_sha256": digest(encode(result)),
            "source_equivalence_admitted": False, "analysis_admitted": False,
            "request_journal_sha256": result["request_journal_sha256"]}, final=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-observation", type=Path, required=True)
    parser.add_argument("--runtime-observation-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = collect(args.runtime_observation, args.runtime_observation_sha256, args.expected_head,
                         run_dir=args.output_dir)
    except BaseException:
        parser.error("Raw source lookup incomplete; preserve and inspect the bound immutable run.")
    print(json.dumps({key: result[key] for key in ("status", "http_attempts", "response_bytes_charged", "source_equivalence_admitted")}, sort_keys=True))
    return 0 if result["status"] == COMPLETE else 2


if __name__ == "__main__":
    raise SystemExit(main())
