#!/usr/bin/env python3
"""Fixed public raw-metadata probe; not normalization or full-universe admission.

Five immutable streams, at most two pages each. CLOB uses MA==/LTE= and
next_cursor. Gamma uses after_cursor, explicit v1/v2 and closed filters, and
an omitted cursor on a short/empty terminal page. Every raw row, including
scalar rows and blank identifiers, remains in the exact compressed response.
Only the CLI is a production entrypoint; run_probe supports synthetic fixtures.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import gzip
import hashlib
import http.client
import io
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

INITIAL_CURSOR, END_CURSOR = "MA==", "LTE="
GAMMA_LIMIT = 10
STREAMS = (
    {"id": "clob", "provider": "clob", "endpoint": "https://clob.polymarket.com/markets"},
    *({"id": f"gamma_{version}_{'closed' if closed else 'open'}", "provider": "gamma",
       "endpoint": "https://gamma-api.polymarket.com/markets/keyset",
       "version": version, "closed": closed}
      for version in ("v1", "v2") for closed in (False, True)),
)
CAPS = {"http_attempts": 20, "successful_pages": 10, "pages_per_stream": 2,
        "individual_response_bytes": 4 * 1024**2, "total_response_bytes": 32 * 1024**2,
        "all_artifact_bytes": 128_000_000, "runtime_seconds": 1200,
        "address_space_bytes": 8 * 1024**3, "minimum_available_ram_bytes": 8 * 1024**3,
        "minimum_free_bytes": 20_000_000_000, "spill_bytes": 0}
CONTROL_RESERVE, MAX_CONTROL = 4_000_000, 1_000_000
MAX_ROW_INDEX = 16 * 1024**2
GZIP_OVERHEAD_RESERVE = 65536
GZIP_LEVEL = 6
HEADERS = {"Accept": "application/json", "Accept-Encoding": "identity",
           "User-Agent": "prediction-markets-raw-metadata-probe/2"}
SOURCE_FILES = ("production_guard.py", "scripts/collect_polymarket_raw_metadata_probe_v2.py",
                "tests/test_polymarket_raw_metadata_probe_v2.py")
REFERENCES = (
    "https://raw.githubusercontent.com/Polymarket/py-clob-client-v2/main/py_clob_client_v2/client.py",
    "https://raw.githubusercontent.com/Polymarket/py-clob-client-v2/main/py_clob_client_v2/constants.py",
    "https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination",
    "https://docs.polymarket.com/market-data/discover-markets",
    "https://raw.githubusercontent.com/Polymarket/py-sdk/main/src/polymarket/_internal/actions/gamma.py",
)
RETRYABLE = {429, 500, 502, 503, 504}


class ProbeError(ValueError):
    """Only constant, nonsecret codes are written to receipts."""


def require(ok, code):
    if not ok:
        raise ProbeError(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or "") is not None, "invalid_expected_head")
    def git(*args):
        try:
            return subprocess.check_output(["git", *args], cwd=REPO, text=True,
                                           stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            raise ProbeError("source_git_unavailable") from None
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
        obj = expected_head + ":" + name
        size = git("cat-file", "-s", obj)
        require(size.isdecimal() and int(size) <= 1_000_000, "committed_source_file_invalid")
        try:
            committed = subprocess.check_output(["git", "cat-file", "blob", obj], cwd=REPO,
                                                stderr=subprocess.DEVNULL)
        except (OSError, subprocess.CalledProcessError):
            raise ProbeError("committed_source_unavailable") from None
        require(len(committed) == int(size) and committed == raw, "source_bytes_differ_from_commit")
        files[name] = {"bytes": len(raw), "sha256": digest(raw)}
    return {"head": expected_head, "files": files}


def available_ram():
    with Path("/proc/meminfo").open() as stream:
        for line in stream:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    raise ProbeError("available_ram_unknown")


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
                          parse_constant=lambda _: (_ for _ in ()).throw(ProbeError("nonfinite_json")))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise ProbeError("invalid_json") from None


def cursor_value(value, provider):
    require(type(value) is str and 0 < len(value) <= 4096 and
            not any(ord(char) < 32 or ord(char) == 127 for char in value), "invalid_cursor")
    if provider == "clob":
        try:
            decoded = base64.b64decode(value, validate=True)
        except (ValueError, TypeError):
            raise ProbeError("invalid_cursor") from None
        require(decoded and base64.b64encode(decoded).decode("ascii") == value, "invalid_cursor")
    return value


def request_spec(stream, cursor):
    if stream["provider"] == "clob":
        params = {"next_cursor": cursor}
    else:
        params = {"version": stream["version"], "closed": str(stream["closed"]).lower(),
                  "limit": str(GAMMA_LIMIT), "include_tag": "true", "order": "id", "ascending": "true"}
        if cursor is not None:
            params["after_cursor"] = cursor
    return {"method": "GET", "url": stream["endpoint"] + "?" + parse.urlencode(params),
            "headers": HEADERS, "stream": stream["id"], "parameters": params}


def page_envelope(raw, stream, requested_cursor, seen_cursors, seen_rows):
    body = strict_json(raw)
    if stream["provider"] == "clob":
        require(type(body) is dict and set(body) == {"limit", "count", "next_cursor", "data"},
                "page_envelope_schema")
        require(type(body["limit"]) is int and body["limit"] > 0 and type(body["count"]) is int
                and type(body["data"]) is list and 0 <= body["count"] == len(body["data"]) <= body["limit"],
                "page_count_or_data_schema")
        rows, cursor = body["data"], cursor_value(body["next_cursor"], "clob")
        terminal = cursor == END_CURSOR
        require(rows or terminal, "empty_nonterminal_page")
    else:
        require(type(body) is dict and set(body) in ({"markets"}, {"markets", "next_cursor"}),
                "page_envelope_schema")
        require(type(body["markets"]) is list and len(body["markets"]) <= GAMMA_LIMIT,
                "page_count_or_data_schema")
        rows = body["markets"]
        terminal = "next_cursor" not in body
        if terminal:
            require(len(rows) < GAMMA_LIMIT, "gamma_full_page_missing_cursor")
            cursor = None
        else:
            cursor = cursor_value(body["next_cursor"], "gamma")
            require(len(rows) == GAMMA_LIMIT, "gamma_short_page_has_cursor")
    require(terminal or (cursor != requested_cursor and cursor not in seen_cursors), "repeated_cursor")
    rows_hash = digest(json_bytes(rows))
    require(not rows or rows_hash not in seen_rows, "repeated_page_rows")
    return {"rows": rows, "count": len(rows), "next_cursor": cursor,
            "pagination_terminal_observed": terminal, "rows_sha256": rows_hash}


def inspect_rows(rows, stream, page_index):
    """Account for every raw row; identity issues never become implicit drops."""
    from collections import Counter
    issues, types, versions, winner_types = Counter(), Counter(), Counter(), Counter()
    records, scope_conflicts = [], Counter()
    verified_rows = 0
    for ordinal, row in enumerate(rows):
        row_issues = set()
        def issue(code):
            issues[code] += 1
            row_issues.add(code)
        types[type(row).__name__] += 1
        scope_verified = None if stream["provider"] == "clob" else False
        if type(row) is not dict:
            issue("row_not_object")
            if stream["provider"] == "gamma":
                issue("scope_unverifiable")
        else:
            condition_key = "condition_id" if stream["provider"] == "clob" else "conditionId"
            condition = row.get(condition_key)
            if condition_key not in row:
                issue("condition_id_missing")
            elif condition is None:
                issue("condition_id_null")
            elif condition == "":
                issue("condition_id_empty")
            elif type(condition) is not str or re.fullmatch(r"0x[0-9a-fA-F]{64}", condition) is None:
                issue("condition_id_unsupported_shape")
            question_key = "question_id" if stream["provider"] == "clob" else "questionID"
            if question_key not in row:
                issue("question_id_missing")
            elif row[question_key] is None:
                issue("question_id_null")
            elif row[question_key] == "":
                issue("question_id_empty")
            elif type(row[question_key]) is not str:
                issue("question_id_unsupported_shape")
            version = row.get("version")
            versions[version if type(version) is str and len(version) <= 32 else
                     "missing" if "version" not in row else "null" if version is None else "unsupported"] += 1
            if stream["provider"] == "gamma":
                known_version = version is not None and "version" in row
                known_closed = row.get("closed") is not None and "closed" in row
                if known_version and (type(version) is not str or version != stream["version"]):
                    scope_conflicts["version"] += 1
                    issue("scope_version_conflict")
                if known_closed and (type(row["closed"]) is not bool or row["closed"] != stream["closed"]):
                    scope_conflicts["closed"] += 1
                    issue("scope_closed_conflict")
                scope_verified = (known_version and known_closed and
                                  "scope_version_conflict" not in row_issues and
                                  "scope_closed_conflict" not in row_issues)
                if not known_version or not known_closed:
                    issue("scope_unverifiable")
                if scope_verified:
                    verified_rows += 1
                # These are uninterpreted protocol-specific arrays, never a
                # guessed common token family. Missing arrays remain evidence.
                asset_fields = ("clobTokenIds", "positionIds")
                for field in asset_fields:
                    if field not in row or row[field] is None:
                        issue(field + "_absent")
                        continue
                    values = row[field]
                    if type(values) is str:
                        try:
                            values = strict_json(values.encode())
                        except ProbeError:
                            issue(field + "_unsupported_shape")
                            continue
                    if type(values) is not list:
                        issue(field + "_unsupported_shape")
                        continue
                    for value in values:
                        if value == "":
                            issue(field + "_empty_id")
                        elif type(value) is not str or re.fullmatch(r"[0-9]+", value) is None:
                            issue(field + "_unsupported_id_shape")
            else:
                tokens = row.get("tokens")
                if type(tokens) is not list:
                    issue("tokens_missing_or_unsupported")
                else:
                    for token in tokens:
                        if type(token) is not dict:
                            issue("token_not_object")
                            continue
                        if "token_id" not in token:
                            issue("token_id_missing")
                        elif token["token_id"] == "":
                            issue("token_id_empty")
                        elif type(token["token_id"]) is not str or re.fullmatch(r"[0-9]+", token["token_id"]) is None:
                            issue("token_id_unsupported_shape")
                        if "winner" not in token:
                            winner_types["missing"] += 1
                            issue("winner_missing")
                        elif type(token["winner"]) is bool:
                            winner_types["literal_true" if token["winner"] else "literal_false"] += 1
                        else:
                            winner_types["unsupported"] += 1
                            issue("winner_unsupported_shape")
        records.append({"locator": f"{stream['id']}/p{page_index:02d}/r{ordinal:06d}",
                        "row_type": type(row).__name__, "identity_issues": sorted(row_issues),
                        "returned_scope_fields_verified": scope_verified})
    require(len(records) == len(rows) == sum(types.values()), "raw_row_conservation_failed")
    return {"raw_row_count": len(rows), "row_types": dict(types), "identity_issue_counts": dict(issues),
            "literal_version_counts": dict(versions), "literal_winner_counts": dict(winner_types),
            "scope_conflict_counts": dict(scope_conflicts),
            "returned_scope_fields_verified": None if stream["provider"] == "clob" else verified_rows == len(rows),
            "scope_verified_rows": verified_rows, "records": records}


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def public_transport(spec, timeout):
    opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
    req = request.Request(spec["url"], headers=HEADERS, method="GET")
    try:
        return opener.open(req, timeout=timeout)
    except error.HTTPError as response:
        return response


def atomic_completion(temporary, destination):
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        result = library.renameat2(-100, os.fsencode(temporary), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        result = library.renamex_np(os.fsencode(temporary), os.fsencode(destination), 4)
    else:
        raise ProbeError("atomic_completion_unavailable")
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
        require(shutil.disk_usage(self.output).free >= extra + reserve + CAPS["minimum_free_bytes"], "disk_floor")
        return total

    def write(self, relative, raw, *, limit=MAX_CONTROL, failure=False):
        require(len(raw) <= limit, "artifact_single_file_limit")
        if failure:
            total = sum(path.lstat().st_size for path in self.output.rglob("*") if path.is_file())
            require(total + len(raw) <= CAPS["all_artifact_bytes"] and
                    shutil.disk_usage(self.output).free >= len(raw) + CAPS["minimum_free_bytes"],
                    "failure_receipt_resource_limit")
        else:
            self.check(len(raw))
        path = self.output / relative
        require(relative not in self.files and not path.exists() and not path.is_symlink(), "artifact_exists")
        path.parent.mkdir(exist_ok=True)
        # Register the intended identity before writing: partial bytes remain
        # chargeable artifacts and can never be adopted as a new original.
        self.files[relative] = {"bytes": len(raw), "sha256": digest(raw)}
        with path.open("xb") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write")
            stream.flush()
            os.fsync(stream.fileno())
        require(path.read_bytes() == raw, "artifact_reopen_differs")
        return {"path": relative, **self.files[relative]}

    def json(self, relative, value, *, limit=MAX_CONTROL, failure=False):
        return self.write(relative, json_bytes(value), limit=limit, failure=failure)

    def reopen(self):
        self.check()
        for relative, bound in self.files.items():
            raw = (self.output / relative).read_bytes()
            require({"bytes": len(raw), "sha256": digest(raw)} == bound, "artifact_reopen_differs")

    def complete(self, value, final_source_check):
        raw = json_bytes(value)
        require(len(raw) <= MAX_CONTROL, "artifact_single_file_limit")
        self.check(len(raw), 0)
        pending, destination = self.output / ".COMPLETE.pending", self.output / "COMPLETE.json"
        require(not pending.exists() and not pending.is_symlink() and not destination.exists()
                and not destination.is_symlink(), "artifact_exists")
        self.files[".COMPLETE.pending"] = {"bytes": len(raw), "sha256": digest(raw)}
        with pending.open("xb") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write")
            stream.flush()
            os.fsync(stream.fileno())
        require(pending.read_bytes() == raw, "artifact_reopen_differs")
        self.reopen()
        for directory in [self.output, *sorted(path for path in self.output.rglob("*") if path.is_dir())]:
            fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        final_source_check()
        self.reopen()
        # The no-replace rename is the last artifact I/O on success. Every
        # preceding failure leaves no reader-visible completion marker.
        atomic_completion(pending, destination)


def gzip_bytes(raw):
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buffer, compresslevel=GZIP_LEVEL, mtime=0) as stream:
        stream.write(raw)
    return buffer.getvalue()


def verify_body(output, proof):
    compressed = (output / proof["path"]).read_bytes()
    require(len(compressed) == proof["bytes"] and digest(compressed) == proof["sha256"],
            "compressed_body_reopen_differs")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
            raw = stream.read(proof["uncompressed_bytes"] + 1)
    except (OSError, EOFError):
        raise ProbeError("compressed_body_invalid") from None
    require(len(raw) == proof["uncompressed_bytes"] and digest(raw) == proof["uncompressed_sha256"],
            "decompressed_body_reopen_differs")


def read_response(response, allowance, writer):
    raw, code = bytearray(), None
    header = response.headers.get("Content-Length")
    try:
        length = None if header is None else int(header)
        require(header is None or (str(length) == header and length >= 0), "invalid_content_length")
        if length is not None and length > allowance:
            return b"", "response_byte_limit", allowance
        while len(raw) < allowance:
            writer.check()
            size = min(65536, allowance - len(raw))
            part = response.read(size)
            require(isinstance(part, bytes) and len(part) <= size, "transport_read_contract")
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
        if isinstance(exc.partial, bytes) and len(exc.partial) <= allowance - len(raw):
            raw.extend(exc.partial)
        return bytes(raw), "incomplete_response", allowance
    except ProbeError as exc:
        return bytes(raw), str(exc), allowance
    except Exception:
        return bytes(raw), "response_transport_error", allowance
    return bytes(raw), code, len(raw)


def run_probe(output, expected_head, *, transport=None):
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
    writer, transport = ArtifactWriter(output, started), transport or public_transport
    attempts = response_bytes = observed_bytes = successful_pages = raw_rows = 0
    streams, bodies = [], []
    try:
        writer.json("START.json", {"schema_version": "polymarket_raw_metadata_probe_v2", "source": source,
                    "created_utc": utc_now(), "caps": CAPS, "streams": STREAMS,
                    "gamma_limit": GAMMA_LIMIT, "gzip": {"level": GZIP_LEVEL, "mtime": 0, "filename": "",
                    "python": sys.version.split()[0], "zlib": gzip.zlib.ZLIB_VERSION},
                    "primary_source_references": REFERENCES, "universe_complete": False,
                    "analysis_admitted": False, "mode": "raw_rows_only_no_claims_categories_or_payout_inference"})
        for stream in STREAMS:
            cursor = INITIAL_CURSOR if stream["provider"] == "clob" else None
            seen_cursors, seen_rows, pages = {cursor}, set(), []
            terminal, scope_verified = False, True if stream["provider"] == "gamma" else None
            stream_receipt = {"id": stream["id"], "provider": stream["provider"], "pages": pages}
            streams.append(stream_receipt)
            while len(pages) < CAPS["pages_per_stream"]:
                require(attempts < CAPS["http_attempts"], "http_attempt_limit")
                require(response_bytes < CAPS["total_response_bytes"], "total_response_byte_limit")
                require(source_snapshot(expected_head) == source, "source_changed")
                writer.check(CAPS["individual_response_bytes"] + GZIP_OVERHEAD_RESERVE + MAX_CONTROL)
                attempts += 1
                spec = request_spec(stream, cursor)
                allowance = min(CAPS["individual_response_bytes"], CAPS["total_response_bytes"] - response_bytes)
                attempt = {"attempt": attempts, "stream": stream["id"], "page_index": len(pages),
                           "requested_utc": utc_now(), "request": spec,
                           "request_sha256": digest(json_bytes(spec)), "response_byte_reservation": allowance}
                writer.json(f"attempts/a{attempts:02d}.START.json", attempt)
                raw, code, status, content_type = b"", None, None, None
                dispatched = read_returned = read_complete = pending_interrupt = False
                charged, response = 0, None
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
                    raw, code, charged = read_response(response, allowance, writer)
                    read_returned, read_complete = True, code is None
                    if code is None:
                        require(status == 200, "http_status_not_200")
                        require(content_type == "application/json", "unexpected_content_type")
                except ProbeError as exc:
                    code = str(exc)
                    if dispatched and not read_returned:
                        charged = allowance
                except (OSError, error.URLError, socket.timeout):
                    code, charged = "request_transport_error", allowance
                except BaseException as exc:
                    pending_interrupt = isinstance(exc, (KeyboardInterrupt, SystemExit))
                    code, charged = "interrupted" if pending_interrupt else "request_runtime_error", allowance
                finally:
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
                compressed = gzip_bytes(raw)
                body = writer.write(f"responses/a{attempts:02d}.body.gz", compressed,
                                    limit=CAPS["individual_response_bytes"] + GZIP_OVERHEAD_RESERVE,
                                    failure=code is not None)
                body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw),
                         "complete_received_body": read_complete}
                bodies.append(body)
                verify_body(output, body)
                accepted, summary, row_index = None, None, None
                if code is None:
                    try:
                        accepted = page_envelope(raw, stream, cursor, seen_cursors, seen_rows)
                        summary = inspect_rows(accepted["rows"], stream, len(pages))
                        row_index = writer.json(f"rows/a{attempts:02d}.json", summary, limit=MAX_ROW_INDEX)
                        if summary["scope_conflict_counts"]:
                            code = "gamma_returned_scope_conflict"
                    except ProbeError as exc:
                        code = str(exc)
                page_proof = None
                if accepted is not None:
                    page_proof = {key: value for key, value in accepted.items() if key != "rows"}
                    page_proof |= {"row_index": row_index, "row_summary":
                                   {key: value for key, value in summary.items() if key != "records"}}
                finish = {"attempt": attempts, "stream": stream["id"], "retrieved_utc": utc_now(),
                          "request_sha256": attempt["request_sha256"], "status_code": status,
                          "transport_dispatched": dispatched, "response_bytes_observed": len(raw),
                          "response_bytes_charged": charged, "charge_is_conservative": charged != len(raw),
                          "body": body, "body_complete": read_complete, "page": page_proof,
                          "result": "page_validated" if code is None else "failed", "error_code": code}
                writer.json(f"attempts/a{attempts:02d}.FINISH.json", finish, failure=code is not None)
                if pending_interrupt:
                    raise KeyboardInterrupt() from None
                require(source_snapshot(expected_head) == source, "source_changed")
                if code:
                    if code == "request_transport_error" or (code == "http_status_not_200" and status in RETRYABLE):
                        continue
                    raise ProbeError(code)
                pages.append({"attempt": attempts, "request_sha256": attempt["request_sha256"],
                              "body": body, **page_proof})
                successful_pages += 1
                raw_rows += accepted["count"]
                require(successful_pages <= CAPS["successful_pages"], "successful_page_limit")
                if stream["provider"] == "gamma":
                    scope_verified = scope_verified and summary["returned_scope_fields_verified"]
                seen_rows.add(accepted["rows_sha256"])
                cursor, terminal = accepted["next_cursor"], accepted["pagination_terminal_observed"]
                if terminal:
                    break
                seen_cursors.add(cursor)
            stream_receipt |= {"successful_pages": len(pages), "pagination_terminal_observed": terminal,
                               "returned_scope_fields_verified": scope_verified, "next_cursor": cursor,
                               "stopped_at_page_cap": not terminal, "scope_or_claims_admitted": False}
        require(len(streams) == len(STREAMS), "stream_conservation_failed")
        require(successful_pages == sum(len(stream["pages"]) for stream in streams), "page_conservation_failed")
        writer.reopen()
        for body in bodies:
            verify_body(output, body)
        require(source_snapshot(expected_head) == source, "source_changed")
        all_terminal = all(stream["pagination_terminal_observed"] for stream in streams)
        manifest = {"schema_version": "polymarket_raw_metadata_probe_v2",
                    "status": "probe_complete_terminal_cursors_observed" if all_terminal else "probe_complete_universe_incomplete",
                    "completed_utc": utc_now(), "source": source, "caps": CAPS, "streams": streams,
                    "http_attempts": attempts, "successful_pages": successful_pages,
                    "response_bytes_charged": response_bytes, "response_bytes_observed": observed_bytes,
                    "raw_rows_in_validated_pages": raw_rows, "all_streams_terminal_observed": all_terminal,
                    "universe_complete": False, "snapshot_consistency_certified": False,
                    "analysis_admitted": False, "normalized_labels_emitted": False,
                    "family_or_payout_admitted": False, "artifacts": dict(writer.files),
                    "elapsed_seconds": time.monotonic() - started}
        bound = writer.json("manifest.json", manifest)
        writer.reopen()
        require(source_snapshot(expected_head) == source, "source_changed")
        writer.complete({"schema_version": "polymarket_raw_metadata_probe_completion_v2",
                         "status": manifest["status"], "manifest": bound, "universe_complete": False,
                         "analysis_admitted": False, "family_or_payout_admitted": False},
                        lambda: require(source_snapshot(expected_head) == source, "source_changed"))
        return {"status": manifest["status"], "run_dir": str(output), "http_attempts": attempts,
                "successful_pages": successful_pages, "raw_rows_in_validated_pages": raw_rows,
                "all_streams_terminal_observed": all_terminal, "universe_complete": False,
                "analysis_admitted": False}
    except BaseException as exc:
        code = str(exc) if isinstance(exc, ProbeError) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "internal_runtime_error"
        failure = {"status": "failed_unaccepted", "error_code": code, "http_attempts": attempts,
                   "response_bytes_charged": response_bytes, "response_bytes_observed": observed_bytes,
                   "successful_pages": successful_pages, "universe_complete": False, "analysis_admitted": False}
        try:
            writer.json("FAILURE.json", failure, failure=True)
        except (OSError, ProbeError):
            pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return failure


@contextmanager
def process_limits():
    old = resource.getrlimit(resource.RLIMIT_AS)
    limit = min(CAPS["address_space_bytes"], old[0]) if old[0] >= 0 else CAPS["address_space_bytes"]
    previous = signal.getsignal(signal.SIGALRM)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit, old[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "memory_limit_not_effective")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(ProbeError("runtime_limit")))
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
    require_production_host()  # Before source, destination, memory or network access.
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        require(args.run_dir.is_absolute() and args.run_dir.parent == Path("/mnt/data/runs"), "production_destination_invalid")
        with process_limits():
            result = run_probe(args.run_dir, args.expected_head)
    except ProbeError as exc:
        result = {"status": "failed_unaccepted", "error_code": str(exc), "analysis_admitted": False}
    print(json.dumps(result, sort_keys=True))
    return 1 if result["status"] == "failed_unaccepted" else 0


if __name__ == "__main__":
    raise SystemExit(main())
