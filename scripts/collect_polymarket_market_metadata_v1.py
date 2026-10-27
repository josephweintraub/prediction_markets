#!/usr/bin/env python3
"""Bounded public CLOB pagination pilot, never a final metadata/sample builder.

Primary-source contract checked 2026-10-27 (no endpoint request was made):
https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/client.py
https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/endpoints.py
https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/constants.py
https://github.com/Polymarket/clob-client/blob/main/src/types.ts (PaginationPayload)

GET /markets takes next_cursor; MA== starts pagination and LTE= terminates it.
The pilot retains literal response bytes and opaque market objects. It neither
normalizes tokens nor infers final payouts from winner flags or trading prices.
Only the CLI is a production entrypoint; run_pilot is a synthetic-testable core.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from urllib import error, parse, request

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host

ENDPOINT = "https://clob.polymarket.com/markets"
INITIAL_CURSOR, END_CURSOR = "MA==", "LTE="
CAPS = {"http_attempts": 5, "successful_pages": 2,
        "individual_response_bytes": 4 * 1024**2, "total_response_bytes": 16 * 1024**2,
        "all_artifact_bytes": 32_000_000, "runtime_seconds": 300,
        "address_space_bytes": 8 * 1024**3, "minimum_available_ram_bytes": 8 * 1024**3,
        "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0}
CONTROL_RESERVE = 1_000_000
MAX_CONTROL = 128_000
HEADERS = {"Accept": "application/json", "Accept-Encoding": "identity",
           "User-Agent": "prediction-markets-metadata-pilot/1"}
SOURCE_FILES = ("production_guard.py", "scripts/collect_polymarket_market_metadata_v1.py",
                "tests/test_collect_polymarket_market_metadata_v1.py")
REFERENCES = (
    "https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/client.py",
    "https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/endpoints.py",
    "https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/constants.py",
    "https://github.com/Polymarket/clob-client/blob/main/src/types.ts",
)
RETRYABLE = {429, 500, 502, 503, 504}


class PilotError(ValueError):
    """Only constant, nonsecret error codes may reach receipts or CLI output."""


def require(ok, code):
    if not ok:
        raise PilotError(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or "") is not None, "invalid_expected_head")
    def git(*args):
        try:
            return subprocess.check_output(["git", *args], cwd=REPO, text=True,
                                           stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            raise PilotError("source_git_unavailable") from None
    require(git("rev-parse", "HEAD") == expected_head, "source_head_differs")
    require(not git("status", "--porcelain", "--untracked-files=normal"), "source_not_clean")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        before = path.lstat()
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= 1_000_000,
                "source_file_invalid")
        raw = path.read_bytes()
        after = path.lstat()
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "source_changed_during_read")
        # Git's stat/index cache is not a content proof. Compare the reopened
        # bytes to the immutable blob at the explicitly admitted commit too.
        object_name = expected_head + ":" + name
        size = git("cat-file", "-s", object_name)
        require(size.isdecimal() and int(size) <= 1_000_000, "committed_source_file_invalid")
        try:
            committed = subprocess.check_output(["git", "cat-file", "blob", object_name],
                                                cwd=REPO, stderr=subprocess.DEVNULL)
        except (OSError, subprocess.CalledProcessError):
            raise PilotError("committed_source_unavailable") from None
        require(len(committed) == int(size) and committed == raw, "source_bytes_differ_from_commit")
        files[name] = {"bytes": len(raw), "sha256": digest(raw)}
    return {"head": expected_head, "files": files}


def available_ram():
    with Path("/proc/meminfo").open() as stream:
        for line in stream:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    raise PilotError("available_ram_unknown")


def strict_json(raw):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out, "duplicate_json_key")
            out[key] = value
        return out
    def number(value):
        result = float(value)
        require(math.isfinite(result), "nonfinite_json")
        return result
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_float=number,
                          parse_constant=lambda _: (_ for _ in ()).throw(PilotError("nonfinite_json")))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise PilotError("invalid_json") from None


def validate_cursor(value):
    require(isinstance(value, str) and 0 < len(value) <= 1024, "invalid_cursor")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise PilotError("invalid_cursor") from None
    require(raw and base64.b64encode(raw).decode("ascii") == value, "invalid_cursor")
    return value


def validate_page(raw, requested_cursor, seen_cursors, seen_conditions):
    body = strict_json(raw)
    require(type(body) is dict and set(body) == {"limit", "count", "next_cursor", "data"},
            "page_envelope_schema")
    require(type(body["limit"]) is int and body["limit"] > 0 and type(body["count"]) is int
            and type(body["data"]) is list and 0 <= body["count"] == len(body["data"]) <= body["limit"],
            "page_count_or_data_schema")
    cursor = validate_cursor(body["next_cursor"])
    require(cursor == END_CURSOR or (cursor != requested_cursor and cursor not in seen_cursors),
            "repeated_cursor")
    require(body["data"] or cursor == END_CURSOR, "empty_nonterminal_page")
    conditions = []
    for market in body["data"]:
        require(type(market) is dict and isinstance(market.get("condition_id"), str)
                and re.fullmatch(r"0x[0-9a-fA-F]{64}", market["condition_id"]) is not None,
                "market_object_schema")
        condition = market["condition_id"].lower()
        require(condition not in seen_conditions and condition not in conditions, "duplicate_market_condition")
        conditions.append(condition)
        if "tokens" in market:
            require(type(market["tokens"]) is list and all(type(token) is dict for token in market["tokens"]),
                    "market_tokens_schema")
            require(all(type(token["winner"]) is bool for token in market["tokens"] if "winner" in token),
                    "literal_winner_schema")
    return {"next_cursor": cursor, "count": body["count"], "conditions": conditions,
            "terminal_cursor_observed": cursor == END_CURSOR}


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def public_transport(spec, timeout):
    """No environment proxies, cookies, authentication or automatic redirects."""
    opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
    req = request.Request(spec["url"], headers=HEADERS, method="GET")
    try:
        return opener.open(req, timeout=timeout)
    except error.HTTPError as response:
        return response


def atomic_completion(temporary, destination):
    """Publish a fully reopened marker with the OS no-replace rename primitive."""
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        result = library.renameat2(-100, os.fsencode(temporary), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        result = library.renamex_np(os.fsencode(temporary), os.fsencode(destination), 4)
    else:
        raise PilotError("atomic_completion_unavailable")
    require(result == 0, "atomic_completion_refused")


class ArtifactWriter:
    def __init__(self, output, started):
        self.output, self.started, self.files = output, started, {}

    def check(self, extra=0, reserve=CONTROL_RESERVE):
        require(time.monotonic() - self.started < CAPS["runtime_seconds"], "runtime_limit")
        total = 0
        for path in self.output.rglob("*"):
            info = path.lstat()
            require(not path.is_symlink() and (stat.S_ISDIR(info.st_mode) or
                    (stat.S_ISREG(info.st_mode) and info.st_nlink == 1)), "artifact_type_invalid")
            if stat.S_ISREG(info.st_mode):
                require(path.relative_to(self.output).as_posix() in self.files, "unexpected_artifact")
                total += info.st_size
        require(total + extra + reserve <= CAPS["all_artifact_bytes"], "artifact_byte_limit")
        require(shutil.disk_usage(self.output).free >= extra + reserve + CAPS["minimum_free_bytes"],
                "disk_floor")
        return total

    def write(self, relative, raw, *, control=False, failure=False):
        require(len(raw) <= (MAX_CONTROL if control else CAPS["individual_response_bytes"]),
                "artifact_single_file_limit")
        # A small pre-reserved terminal failure receipt remains writable on an
        # elapsed-runtime failure; byte/floor checks still apply below.
        if failure:
            total = sum(path.lstat().st_size for path in self.output.rglob("*") if path.is_file())
            require(total + len(raw) <= CAPS["all_artifact_bytes"] and
                    shutil.disk_usage(self.output).free >= len(raw) + CAPS["minimum_free_bytes"],
                    "failure_receipt_resource_limit")
        else:
            self.check(len(raw), 0 if relative == "COMPLETE.json" else CONTROL_RESERVE)
        path = self.output / relative
        require(relative not in self.files and not path.exists(), "artifact_exists")
        path.parent.mkdir(exist_ok=True)
        write_path = self.output / ".COMPLETE.pending" if relative == "COMPLETE.json" else path
        with write_path.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        require(write_path.read_bytes() == raw, "artifact_reopen_differs")
        if relative == "COMPLETE.json":
            # This is the final I/O operation on success. A failed or partial
            # marker write can never leave a reader-visible COMPLETE.json.
            atomic_completion(write_path, path)
        self.files[relative] = {"bytes": len(raw), "sha256": digest(raw)}
        return {"path": relative, **self.files[relative]}

    def json(self, relative, value, *, failure=False):
        return self.write(relative, json_bytes(value), control=True, failure=failure)

    def reopen(self):
        self.check()
        for relative, bound in self.files.items():
            raw = (self.output / relative).read_bytes()
            require({"bytes": len(raw), "sha256": digest(raw)} == bound, "artifact_reopen_differs")


def read_response(response, allowance, writer):
    """Return (retained bytes, error code, charged bytes).

    At an exact cap without a matching Content-Length, reject conservatively
    instead of reading an additional byte merely to discover whether EOF follows.
    """
    raw = bytearray()
    code = None
    header = response.headers.get("Content-Length")
    try:
        length = None if header is None else int(header)
        require(header is None or (str(length) == header and length >= 0), "invalid_content_length")
        if length is not None and length > allowance:
            return b"", "response_byte_limit", allowance
        while len(raw) < allowance:
            writer.check()
            part = response.read(min(65536, allowance - len(raw)))
            require(isinstance(part, bytes) and len(part) <= min(65536, allowance - len(raw)),
                    "transport_read_contract")
            if not part:
                if length is not None and len(raw) != length:
                    code = "truncated_response"
                break
            raw.extend(part)
        else:
            if length != len(raw):
                code = "response_byte_limit"
        if length is not None and len(raw) > length:
            code = "content_length_differs"
    except http.client.IncompleteRead as exc:
        # The standard HTTP reader exposes any bytes received by its failed
        # bounded read here, rather than returning them normally.
        partial = exc.partial
        if isinstance(partial, bytes) and len(partial) <= allowance - len(raw):
            raw.extend(partial)
        return bytes(raw), "incomplete_response", allowance
    except PilotError as exc:
        return bytes(raw), str(exc), allowance
    except Exception:
        # A failing read can consume bytes before raising without exposing
        # them. Reserve its entire bounded allowance; do not claim zero use.
        return bytes(raw), "response_transport_error", allowance
    return bytes(raw), code, len(raw)


def run_pilot(output, expected_head, *, transport=None):
    """Fixed-size pilot core; injection exists solely for small synthetic tests."""
    source = source_snapshot(expected_head)
    output = Path(output)
    require(output.name not in ("", ".", "..") and output.parent.is_dir(), "invalid_destination")
    require(not output.exists() and not output.is_symlink(), "destination_exists")
    require(all(not parent.is_symlink() for parent in (output.parent, *output.parent.parents)),
            "destination_parent_symlink")
    require(available_ram() >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
    require(shutil.disk_usage(output.parent).free >= CAPS["all_artifact_bytes"] + CAPS["minimum_free_bytes"],
            "disk_floor")
    started = time.monotonic()
    output.mkdir(exist_ok=False)
    writer = ArtifactWriter(output, started)
    transport = transport or public_transport
    attempts, response_bytes, observed_bytes, pages, cursor = 0, 0, 0, [], INITIAL_CURSOR
    seen_cursors, seen_conditions = {INITIAL_CURSOR}, set()
    try:
        writer.json("START.json", {"schema_version": "polymarket_market_metadata_pilot_v1",
                    "created_utc": utc_now(), "source": source, "caps": CAPS,
                    "endpoint": ENDPOINT, "initial_cursor": INITIAL_CURSOR,
                    "terminal_cursor": END_CURSOR, "primary_source_references": REFERENCES,
                    "mode": "public_raw_pagination_pilot_no_payout_or_token_normalization"})
        while len(pages) < CAPS["successful_pages"]:
            require(attempts < CAPS["http_attempts"], "http_attempt_limit")
            require(response_bytes < CAPS["total_response_bytes"], "total_response_byte_limit")
            require(source_snapshot(expected_head) == source, "source_changed")
            writer.check()
            attempts += 1
            spec = {"method": "GET", "url": ENDPOINT + "?" + parse.urlencode({"next_cursor": cursor}),
                    "headers": HEADERS, "next_cursor": cursor}
            attempt = {"attempt": attempts, "page_index": len(pages), "requested_utc": utc_now(),
                       "request": spec, "request_sha256": digest(json_bytes(spec)),
                       "response_byte_reservation": min(CAPS["individual_response_bytes"],
                                                        CAPS["total_response_bytes"] - response_bytes)}
            writer.json(f"attempts/a{attempts:02d}.START.json", attempt)
            raw, code, status, content_type = b"", None, None, None
            read_attempted = False
            dispatched, read_returned = False, False
            charged, pending_interrupt = 0, False
            allowance = attempt["response_byte_reservation"]
            response = None
            try:
                remaining = CAPS["runtime_seconds"] - (time.monotonic() - started)
                require(remaining > 0, "runtime_limit")
                dispatched = True
                response = transport(spec, min(30.0, remaining))
                status = response.status
                require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
                require(response.geturl() == spec["url"], "redirect_or_response_url_changed")
                require(response.headers.get("Content-Encoding", "identity").lower() == "identity",
                        "unexpected_content_encoding")
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                read_attempted = True
                raw, code, charged = read_response(response, allowance, writer)
                read_returned = True
                if code is None:
                    require(status == 200, "http_status_not_200")
                    require(content_type == "application/json", "unexpected_content_type")
            except PilotError as exc:
                code = str(exc)
                if dispatched and not read_returned:
                    charged = allowance
            except (OSError, error.URLError, socket.timeout):
                code = "request_transport_error"
                charged = allowance
            except BaseException as exc:
                pending_interrupt = isinstance(exc, (KeyboardInterrupt, SystemExit))
                code = "interrupted" if pending_interrupt else "request_runtime_error"
                charged = allowance
            finally:
                # Charge before close: response.close itself may fail. An
                # opaque close failure gets the full reservation as well.
                response_bytes += charged
                observed_bytes += len(raw)
                if response is not None:
                    try:
                        response.close()
                    except BaseException as exc:
                        response_bytes += allowance - charged
                        charged = allowance
                        pending_interrupt = pending_interrupt or isinstance(exc, (KeyboardInterrupt, SystemExit))
                        code = "interrupted" if pending_interrupt else "response_close_error"
            require(response_bytes <= CAPS["total_response_bytes"], "total_response_byte_limit")
            body = None
            if raw:
                body = writer.write(f"responses/a{attempts:02d}.body", raw)
            accepted = None
            if code is None:
                try:
                    accepted = validate_page(raw, cursor, seen_cursors, seen_conditions)
                except PilotError as exc:
                    code = str(exc)
            page_proof = ({key: value for key, value in accepted.items() if key != "conditions"}
                          | {"condition_ids_sha256": digest(json_bytes(accepted["conditions"]))}) if accepted else None
            finish = {"attempt": attempts, "retrieved_utc": utc_now(),
                      "request_sha256": attempt["request_sha256"], "status_code": status,
                      "transport_dispatched": dispatched,
                      "response_bytes": len(raw), "response_sha256": digest(raw), "body": body,
                      "response_bytes_charged": charged, "charge_is_conservative": charged != len(raw),
                      "body_complete": read_attempted and code not in {
                          "truncated_response", "response_byte_limit", "response_transport_error",
                          "incomplete_response", "interrupted", "content_length_differs",
                          "invalid_content_length", "transport_read_contract", "runtime_limit"},
                      "result": "page_validated" if accepted else "failed", "error_code": code,
                      "page": page_proof}
            writer.json(f"attempts/a{attempts:02d}.FINISH.json", finish)
            if pending_interrupt:
                raise KeyboardInterrupt() from None
            require(source_snapshot(expected_head) == source, "source_changed")
            if code:
                if (code == "request_transport_error" or (code == "http_status_not_200" and status in RETRYABLE)):
                    continue
                raise PilotError(code)
            pages.append({"attempt": attempts, "request_sha256": attempt["request_sha256"],
                          "body": body, **page_proof})
            seen_conditions.update(accepted["conditions"])
            cursor = accepted["next_cursor"]
            if cursor == END_CURSOR:
                break
            seen_cursors.add(cursor)
        terminal = cursor == END_CURSOR
        writer.reopen()
        require(source_snapshot(expected_head) == source, "source_changed")
        manifest = {"schema_version": "polymarket_market_metadata_pilot_v1",
                    "status": "pilot_complete_terminal_cursor_observed" if terminal else "pilot_complete_universe_incomplete",
                    "completed_utc": utc_now(), "source": source, "caps": CAPS,
                    "http_attempts": attempts, "response_bytes": response_bytes,
                    "response_bytes_charged": response_bytes, "response_bytes_observed": observed_bytes,
                    "successful_pages": len(pages), "markets_in_validated_pages": len(seen_conditions),
                    "pages": pages, "terminal_cursor_observed": terminal,
                    "requested_pagination_complete": terminal, "next_cursor": cursor,
                    "universe_complete": False, "snapshot_consistency_certified": False,
                    "analysis_admitted": False, "normalized_labels_emitted": False,
                    "artifacts": dict(writer.files), "elapsed_seconds": time.monotonic() - started}
        bound = writer.json("manifest.json", manifest)
        writer.reopen()
        require(source_snapshot(expected_head) == source, "source_changed")
        writer.json("COMPLETE.json", {"schema_version": "polymarket_market_metadata_pilot_completion_v1",
                    "status": manifest["status"], "manifest": bound,
                    "universe_complete": False, "analysis_admitted": False})
        return {"status": manifest["status"], "run_dir": str(output), "http_attempts": attempts,
                "successful_pages": len(pages), "terminal_cursor_observed": terminal,
                "universe_complete": False, "analysis_admitted": False}
    except BaseException as exc:
        code = str(exc) if isinstance(exc, PilotError) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "internal_runtime_error"
        failed = {"status": "failed_unaccepted", "error_code": code,
                  "http_attempts": attempts, "response_bytes": response_bytes,
                  "response_bytes_charged": response_bytes, "response_bytes_observed": observed_bytes,
                  "successful_pages": len(pages), "universe_complete": False, "analysis_admitted": False}
        try:
            writer.json("FAILURE.json", failed, failure=True)
        except (OSError, PilotError):
            pass  # Absence of COMPLETE remains decisive if failure storage is unavailable.
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return failed


@contextmanager
def process_limits():
    old = resource.getrlimit(resource.RLIMIT_AS)
    limit = min(CAPS["address_space_bytes"], old[0]) if old[0] >= 0 else CAPS["address_space_bytes"]
    previous = signal.getsignal(signal.SIGALRM)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit, old[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "memory_limit_not_effective")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(PilotError("runtime_limit")))
        signal.alarm(CAPS["runtime_seconds"])
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
        resource.setrlimit(resource.RLIMIT_AS, old)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args(argv)
    require_production_host()  # Before source, destination, runtime, or network access.
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        require(args.run_dir.is_absolute() and args.run_dir.parent == Path("/mnt/data/runs"), "production_destination_invalid")
        with process_limits():
            result = run_pilot(args.run_dir, args.expected_head)
    except PilotError as exc:
        result = {"status": "failed_unaccepted", "error_code": str(exc), "analysis_admitted": False}
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] == "failed_unaccepted" else 0


if __name__ == "__main__":
    raise SystemExit(main())
