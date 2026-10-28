#!/usr/bin/env python3
"""Bounded, root-admitted recovery of the one failed metadata-v4 walk.

Replays every accepted v4 page before dispatch, then continues only its unfinished
Gamma stream and the two unreached Gamma streams. Old files remain immutable and
are referenced, never copied or relabelled complete. Pagination across retrieval
vintages is not an atomic snapshot or a scientific/universe admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import gzip
import http.client
import math
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_market_metadata_campaign_v4 as previous

legacy, probe = previous.legacy, previous.probe
Error, require, encode, digest = previous.Error, previous.require, previous.encode, previous.digest
identity, hash_file, read_bound, number = previous.identity, previous.hash_file, previous.read_bound, previous.number
CAPS = dict(previous.CAPS)
STREAMS, HEADERS, HTTP_SECONDS = previous.STREAMS, previous.HEADERS, previous.HTTP_SECONDS
MAX_CONTROL, CONTROL_RESERVE = previous.MAX_CONTROL, previous.CONTROL_RESERVE
RUN_ROOT = Path("/mnt/data/runs")
OPERATIONS = RUN_ROOT / "2026-10-27_polymarket_oct1_finalization_v1"
SCHEMA = "polymarket_market_metadata_campaign_admission_v5"
START_SCHEMA = "polymarket_market_metadata_campaign_v5"
STATUS = "root_compact_metadata_continuation_admitted"
SUCCESS = "requested_metadata_continuation_complete_not_universe_admission"
SOURCE_FILES = (*previous.SOURCE_FILES, "scripts/collect_polymarket_market_metadata_campaign_v5.py",
                "tests/test_polymarket_market_metadata_campaign_v5.py")
SCOPE = {**previous.SCOPE, "resume_permitted": True,
    "continuation": "only_the_pinned_failed_v4_accepted_prefix", "old_prefix_imported_by_reference": True,
    "replay_before_any_http": True, "atomic_snapshot_certified": False,
    "old_full_v3_prefix_remains_supplemental_only": True}
PRIOR_MINIMUM = {"http_attempts": 16319, "response_bytes_charged": 18843920531, "runtime_seconds": 22800}
RETAINED_MINIMUM = {"raw": 2222462636, "control": 107482141, "other": 16865544}
PIN = {"root": str(RUN_ROOT / "2026-10-27_polymarket_market_metadata_full_v4"),
    "producer_head": "d20f23a0b8ab71a4107bd3535d37b6afaae6e0e1",
    "admission": (12457, "3ebbbef6b9a54abd4c499c42b313077cd17a6ac2507674031d505294b7919f6a"),
    "start": (8416, "b40a217908a949f1b23cc1287de6d33078eea83308496450bb935501e1201858"),
    "failure": (2063, "970bb128a11caba232fa1a7f2bab04ae658f7df256c62a140161e9b41d5825f7"),
    "launch": (2336, "d623fe53b067de99c3f28c6961b99ebeef4365f44d4e1219e24dc815ae98b3d8"),
    "exit": (2778, "f077a983f90f0bf55080f9226f3fe6a938fc1d6bac4561e60c87ec79ec1492cd"),
    "drain": (1759, "f43c577f339fed2891b5de9a18dc6bb80656f27833e14af26a0d9b563f6967ab"),
    "attempts": (20256182, "e28d8281178a1c30d27197071990124d91e2af207a252e1a90c5655f57edb913"),
    "pages_journal": (58162014, "1eba888c3bcef7d0140c258e96895383a0c2ee0fe49706fca00aef351310919b"),
    "pages": 11456, "raw_rows": 5763939,
    "streams": ((5132, 5131572, True), (2740, 273967, True), (3584, 358400, False))}
# Bounds on reading the separately authenticated journals, not new retained-byte
# or API allowances. Each original control record keeps its original 16 MiB cap.
JOURNAL_LIMITS = {"attempts": 64 * 1024**2, "pages": 128 * 1024**2}
SAFE_CODES = frozenset({"runtime_limit", "http_attempt_timeout", "invalid_http_status", "redirect_or_response_url_changed",
    "unexpected_content_encoding", "http_status_not_200", "unexpected_content_type", "request_transport_error",
    "response_close_error", "attempt_runtime_or_durability_error", "invalid_content_length", "response_byte_limit",
    "incomplete_response", "transport_read_contract", "truncated_response", "content_length_differs", "response_transport_error",
    "interrupted", "body_expansion_limit", "attempt_charge_invalid", "gamma_returned_scope_conflict", "page_envelope_schema",
    "page_count_or_data_schema", "gamma_schema_uri_differs", "gamma_full_page_missing_cursor", "gamma_short_page_has_cursor",
    "invalid_cursor", "repeated_cursor", "repeated_page_rows", "invalid_json", "duplicate_json_key", "nonfinite_json",
    "source_changed", "source_changed_before_dispatch", "source_or_admission_changed", "original_prior_changed", "input_changed",
    "input_identity_changed", "metadata_aggregate_or_subcap", "disk_floor", "http_attempt_limit", "total_response_byte_limit",
    "artifact_short_write", "artifact_reopen_differs", "artifact_exists", "control_size_changed", "control_record_limit",
    "artifact_single_file_limit", "source_git_unavailable", "source_not_clean", "source_head_differs", "source_bytes_differ_from_commit",
    "count_conservation_failed", "pagination_incomplete", "input_changed_during_read", "reference_bytes_or_hash_differs"})


def safe_code(exc, fallback):
    # Only program-owned enumerated codes may be published. Even an externally
    # raised ProbeError must not echo arbitrary exception text.
    if isinstance(exc, Error):
        value = str(exc)
        return value if value in SAFE_CODES else "contract_validation_error"
    return "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else fallback


def admission_contract():
    return {"schema_version": SCHEMA, "status": STATUS, "mode": "full",
        "consumer_head": "clean committed consumer HEAD", "run_dir": "fresh direct child of /mnt/data/runs",
        "caps": CAPS, "scope_sha256": digest(encode(SCOPE)), "root_checked_prior_producers_drained": True,
        "v4_prefix": {"root": PIN["root"], "producer_head": PIN["producer_head"],
            **{key: "literal absolute path/bytes/sha256; exact fixed control pin" for key in
               ("admission", "start", "failure", "launch", "exit", "drain")},
            "attempts": "SHA-bound attempts.jsonl, streamed <=64MiB",
            "pages": "SHA-bound pages.jsonl, streamed <=128MiB; also the ordered body index"},
        "prior_ledger": {**{key: f"cumulative >= {value}, never reset" for key, value in PRIOR_MINIMUM.items()},
            "retained_bytes_charged": "raw/control/other; no old reservation refunds",
            "root_controls": "explicit <=256 immutable operational control refs, <=2MiB total; no collection payloads"}}


def source_snapshot(head):
    base = previous.source_snapshot(head)
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        bound = hash_file(REPO / name, previous.MAX_INPUT)
        try:
            size = subprocess.check_output(["git", "cat-file", "-s", head + ":" + name], cwd=REPO,
                                           stderr=subprocess.DEVNULL, timeout=10).strip()
            require(size.isdigit() and int(size) == bound["bytes"], "own_source_size_differs")
            raw = subprocess.check_output(["git", "cat-file", "blob", head + ":" + name], cwd=REPO,
                                          stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            raise Error("own_source_git_unavailable") from None
        require(len(raw) == bound["bytes"] and digest(raw) == bound["sha256"], "own_source_blob_differs")
        base["files"][name] = {key: bound[key] for key in ("bytes", "sha256")}
    # The final parent check must not leave the two new bytes unchecked afterward.
    require(previous.source_snapshot(head) == {"head": head,
        "files": {name: base["files"][name] for name in previous.SOURCE_FILES}}, "source_changed")
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        bound = hash_file(REPO / name, previous.MAX_INPUT)
        require({key: bound[key] for key in ("bytes", "sha256")} == base["files"][name], "own_source_changed")
    return base


def wait_until(when, ledger, sleeper):
    while True:
        remaining = when - time.monotonic()  # exactly one clock read per decision
        if remaining <= 0:
            return
        require(ledger.remaining_time() > remaining, "runtime_limit")
        sleeper(min(.25, remaining))


@contextmanager
def attempt_alarm(ledger, deadline=None):
    require(threading.current_thread() is threading.main_thread(), "main_thread_attempt_timer_required")
    began = time.monotonic(); handler = signal.getsignal(signal.SIGALRM)
    old_delay, interval = signal.getitimer(signal.ITIMER_REAL)
    require(interval == 0, "unsupported_existing_interval_timer")
    old_end = began + old_delay if old_delay else None
    available = ledger.remaining_time(); local = HTTP_SECONDS if deadline is None else deadline - began
    delay = min(local, available, old_delay if old_delay else math.inf)
    reason = "http_attempt_timeout" if local < available and (not old_delay or local < old_delay) else "runtime_limit"
    require(delay > 0, reason); ends = began + delay
    def expired(*_): raise Error(reason)
    def check():
        ledger.check(); require(time.monotonic() < ends, reason)
    def rearm():
        ledger.check()
        remaining = ends - time.monotonic()
        require(remaining > 0, reason)
        signal.setitimer(signal.ITIMER_REAL, remaining)
    try:
        signal.signal(signal.SIGALRM, expired); rearm()
        yield check, rearm
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, handler)
        if old_end is not None:
            signal.setitimer(signal.ITIMER_REAL, max(.000001, old_end - time.monotonic()))


def received_attempt(ledger, call, spec, allowance, transport, deadline):
    """Frozen v4 transport/body contract with only the local safe alarm seam."""
    raw, code, charged, complete = b"", None, 0, False
    response, status, retry_header = None, None, None
    dispatched = interrupted = close_attempted = close_completed = False
    body = None; relative = f"responses/a{call:05d}.body.gz"
    try:
        with attempt_alarm(ledger, deadline) as (check, rearm):
            try:
                check(); dispatched = True
                response = transport(spec, min(HTTP_SECONDS, ledger.remaining_time())); check()
                status = response.status
                require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
                require(response.geturl() == spec["url"], "redirect_or_response_url_changed")
                require(response.headers.get("Content-Encoding", "identity").lower() == "identity", "unexpected_content_encoding")
                retry_header = response.headers.get("Retry-After")
                raw, code, charged, complete, interrupted = previous.bounded_read(response, allowance, check)
                if code is None:
                    require(status == 200, "http_status_not_200")
                    require(response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/json", "unexpected_content_type")
            except BaseException as exc:
                code = safe_code(exc, "request_transport_error")
                interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
                if dispatched and not complete: charged = allowance
            finally:
                if response is not None:
                    try:
                        rearm(); close_attempted = True; response.close(); close_completed = True
                    except BaseException as exc:
                        code = safe_code(exc, "response_close_error")
                        charged, complete = allowance, False
                        interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
                try: check()
                except Error as exc: code, charged, complete = safe_code(exc, "runtime_or_transport_error"), allowance, False
            if code is None:
                compressed = probe.gzip_bytes(raw); check()
                body = ledger.write(relative, compressed, "raw")
                body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw), "complete_received_body": True}
                probe.verify_body(ledger.output, body); check()
    except BaseException as exc:
        code = safe_code(exc, "attempt_runtime_or_durability_error")
        charged, complete = allowance, False
        interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
    if body is None or code is not None:
        if relative in ledger.files or (ledger.output / relative).exists(): relative = f"responses/a{call:05d}.failed.body.gz"
        body = ledger.write(relative, probe.gzip_bytes(raw), "raw", failure=True)
        body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw), "complete_received_body": complete}
        probe.verify_body(ledger.output, body)
    return {"status_code": status, "error_code": code, "body": body,
        "response_bytes_observed": len(raw), "response_bytes_charged": charged, "body_complete": complete,
        "transport_dispatched": dispatched, "interrupted": interrupted,
        "response_close_attempted": close_attempted, "response_close_completed": close_completed,
        "retry_after_header": retry_header if type(retry_header) is str and len(retry_header) <= 128 else None}


class Inputs:
    """Content bindings plus fresh run-local physical identities, not old st_dev."""
    def __init__(self, check): self.saved = {}; self.check = check

    def bind(self, ref, maximum):
        self.check(); raw, bound = read_bound(ref, maximum)
        old = self.saved.get(bound["path"])
        require(old is None or old == bound, "input_identity_changed")
        self.saved[bound["path"]] = bound
        return raw

    def json(self, ref, maximum=previous.MAX_INPUT): return probe.strict_json(self.bind(ref, maximum))

    def reopen(self):
        for bound in list(self.saved.values()):
            self.check(); fresh = hash_file(Path(bound["path"]), max(bound["bytes"], 1))
            require(fresh == bound, "input_changed")

    def lines(self, ref, maximum):
        require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"}, "journal_reference_schema")
        path = Path(ref["path"]); before = identity(path)
        require(path.is_absolute() and path.resolve() == path and type(ref["bytes"]) is int
            and 0 < ref["bytes"] == before["bytes"] <= maximum
            and type(ref["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]), "journal_reference_invalid")
        import hashlib
        hasher = hashlib.sha256(); total = 0
        with path.open("rb") as handle:
            while True:
                self.check(); line = handle.readline(MAX_CONTROL + 1)
                if not line: break
                require(line.endswith(b"\n") and len(line) <= MAX_CONTROL, "journal_line_limit_or_truncated")
                total += len(line); require(total <= ref["bytes"], "journal_size_changed")
                hasher.update(line)
                yield probe.strict_json(line)
        require(total == ref["bytes"] and hasher.hexdigest() == ref["sha256"] and identity(path) == before, "journal_binding_changed")
        bound = {"path": str(path), "bytes": total, "sha256": hasher.hexdigest(), "identity": before}
        require(str(path) not in self.saved or self.saved[str(path)] == bound, "input_identity_changed")
        self.saved[str(path)] = bound


def _ref(bound): return {key: bound[key] for key in ("path", "bytes", "sha256")}


def _birth(value):
    require(type(value) is dict and set(value) == {"hostname", "boot_id", "pid", "birth_ticks", "process_group", "session"}
        and type(value["hostname"]) is str and bool(value["hostname"].strip()) and type(value["boot_id"]) is str,
        "prefix_birth_invalid")
    try: valid = str(uuid.UUID(value["boot_id"])) == value["boot_id"]
    except (ValueError, AttributeError): valid = False
    require(valid and all(type(value[key]) is int and value[key] > 0 for key in ("pid", "birth_ticks", "process_group", "session"))
        and value["pid"] == value["process_group"] == value["session"], "prefix_birth_invalid")


def validate_parent(value, inputs, source, output):
    require(type(value) is dict and set(value) == {"root", "producer_head", "admission", "start", "failure", "launch", "exit", "drain", "attempts", "pages"}
        and value["root"] == PIN["root"] and value["producer_head"] == PIN["producer_head"], "prefix_contract_differs")
    root = Path(value["root"])
    require(root.parent == RUN_ROOT and root.resolve() == root and root != output, "prefix_root_invalid")
    controls = {}
    for key in ("attempts", "pages"):
        ref = value[key]; expected = PIN["pages_journal" if key == "pages" else key]
        require(type(ref) is dict and (ref.get("bytes"), ref.get("sha256")) == expected, "prefix_journal_pin_differs")
    for key in ("admission", "start", "failure", "launch", "exit", "drain"):
        ref = value[key]
        require(type(ref) is dict and (ref.get("bytes"), ref.get("sha256")) == PIN[key], "prefix_control_pin_differs")
        controls[key] = inputs.json(ref)
    require(Path(value["start"]["path"]) == root / "START.json" and Path(value["failure"]["path"]) == root / "FAILURE.json"
        and Path(value["admission"]["path"]) == OPERATIONS / "full_metadata_v4_admission_v3.json"
        and Path(value["launch"]["path"]) == OPERATIONS / "full_metadata_v4.LAUNCH.json"
        and Path(value["exit"]["path"]) == OPERATIONS / "full_metadata_v4.EXIT.json"
        and Path(value["drain"]["path"]) == OPERATIONS / "full_metadata_v4_root_drain_v1.json"
        and Path(value["attempts"]["path"]) == root / "attempts.jsonl"
        and Path(value["pages"]["path"]) == root / "pages.jsonl", "prefix_paths_differ")
    start, failure, launch, exited, drain, parent = (controls[name] for name in ("start", "failure", "launch", "exit", "drain", "admission"))
    old_source = {"head": PIN["producer_head"], "files": {name: source["files"][name] for name in previous.SOURCE_FILES}}
    previous.verify_producer(old_source, old_source["files"])
    require(start.get("schema_version") == previous.START_SCHEMA and start.get("mode") == "full" and start.get("source") == old_source
        and start.get("caps") == CAPS and start.get("pilot_caps") == previous.PILOT and start.get("scope") == probe.strict_json(encode(previous.SCOPE))
        and _ref(start.get("admission", {})) == value["admission"] and start.get("universe_complete") is False
        and start.get("analysis_admitted") is False, "prefix_start_differs")
    require(parent.get("schema_version") == previous.SCHEMA and parent.get("status") == "root_compact_metadata_campaign_admitted"
        and parent.get("mode") == "full" and parent.get("consumer_head") == PIN["producer_head"] and parent.get("run_dir") == str(root)
        and parent.get("caps") == CAPS and parent.get("pilot_caps") == previous.PILOT
        and parent.get("scope_sha256") == digest(encode(previous.SCOPE)) and parent.get("root_checked_prior_producers_drained") is True,
        "prefix_parent_admission_differs")
    # Unmodified old parser retains its 10k-row /16MiB limits. It never sees the
    # separately streamed new prefix tree. Historical st_dev is content evidence,
    # not compared with a fresh post-reboot identity.
    old = previous.inventory_prior(parent["prior_ledger"], Path(value["admission"]["path"]), root)
    previous.verify_failed(parent["failed_campaign"], old)
    legacy.verify_probe(parent["legacy_gamma_probe"], old_source, old)
    previous.verify_compiler_producers(old, old_source)
    previous.verify_pilot(parent["pilot_acceptance"], old_source, old)
    require(failure.get("schema_version") == previous.START_SCHEMA and failure.get("mode") == "full"
        and failure.get("status") == "failed_unaccepted" and failure.get("error_code") == "internal_runtime_error"
        and failure.get("universe_complete") is False and failure.get("analysis_admitted") is False, "prefix_failure_differs")
    require(launch.get("schema_version") == "polymarket_oct1_stage_launcher_v1" and launch.get("stage") == "metadata"
        and launch.get("status") == "producer_dispatched_owned_session" and launch.get("output_dir") == str(root)
        and launch.get("source", {}).get("head") == PIN["producer_head"] and launch.get("admission") == start["admission"]
        and launch.get("ownership_unknown") is False and launch.get("retry_or_resume") is False, "prefix_launch_differs")
    require(exited.get("schema_version") == launch["schema_version"] and exited.get("stage") == "metadata"
        and exited.get("status") == "producer_exited" and exited.get("output_dir") == str(root)
        and exited.get("source") == launch["source"] and exited.get("admission") == launch["admission"]
        and exited.get("command") == launch.get("command") and exited.get("error_code") is None
        and exited.get("producer_exit_code") == 1 and type(exited.get("producer_exit_code")) is int
        and exited.get("ownership_unknown") is False and exited.get("owned_group_drained") is True
        and exited.get("children_still_running") is False and exited.get("stdout_eof_observed") is True
        and exited.get("source_and_admission_unchanged") is True, "prefix_exit_differs")
    require(type(drain) is dict and set(drain) == {"schema_version", "status", "run_dir", "source_head", "launch", "producer_start",
        "caller_exit", "terminal_summary", "complete", "child", "caller", "producer_pid_absent", "caller_pid_absent",
        "owned_group_and_session_empty", "transfers_drained", "host_idle_claimed", "universe_admitted"}
        and drain["schema_version"] == "polymarket_metadata_full_v4_stage_drain_v1"
        and drain["status"] == "root_verified_bound_metadata_stage_drained" and drain["run_dir"] == str(root)
        and drain["source_head"] == PIN["producer_head"] and drain["launch"] == value["launch"]
        and drain["producer_start"] == value["start"] and drain["caller_exit"] == value["exit"]
        and drain["terminal_summary"] == value["failure"] and drain["complete"] is None
        and all(drain[key] is True for key in ("producer_pid_absent", "caller_pid_absent", "owned_group_and_session_empty", "transfers_drained"))
        and drain["host_idle_claimed"] is False and drain["universe_admitted"] is False, "prefix_drain_differs")
    for role in ("child", "caller"):
        _birth(drain[role]); require(drain[role] == launch.get(role) == exited.get(role), "prefix_birth_differs")
    require(drain["child"]["boot_id"] == drain["caller"]["boot_id"] and drain["child"]["hostname"] == drain["caller"]["hostname"]
        and drain["child"]["pid"] != drain["caller"]["pid"], "prefix_birth_differs")
    return old, failure


def replay(value, inputs, old, failure):
    root = Path(value["root"]); attempts = inputs.lines(value["attempts"], JOURNAL_LIMITS["attempts"])
    states, seen, files = [], {}, {"START.json", "FAILURE.json", "attempts.jsonl", "pages.jsonl"}
    charge = observed = rows = count = raw_bytes = 0; stream_index = -1
    for page in inputs.lines(value["pages"], JOURNAL_LIMITS["pages"]):
        count += 1; require(count <= PIN["pages"] and type(page) is dict and page.get("event") == "PAGE"
            and type(page.get("call")) is int and page["call"] == count and page.get("accepted") is True
            and page.get("coverage_role") == "full_walk", "prefix_page_sequence_differs")
        start, finish = next(attempts, None), next(attempts, None)
        require(type(start) is dict and type(finish) is dict and start.get("event") == "START" and finish.get("event") == "FINISH"
            and type(start.get("call")) is int and type(finish.get("call")) is int and start["call"] == finish["call"] == count,
            "prefix_attempt_pair_differs")
        ids = [stream["id"] for stream in STREAMS]; index = ids.index(page.get("stream")) if page.get("stream") in ids else -1
        require(0 <= index < len(PIN["streams"]), "prefix_stream_differs")
        if index != stream_index:
            require(index == stream_index + 1 and (stream_index < 0 or states[-1]["pagination_terminal_observed"]), "prefix_stream_order_differs")
            stream_index = index; cursor = "MA==" if index == 0 else None
            seen[index] = ({cursor}, set())
            states.append({"id": ids[index], "provider": STREAMS[index]["provider"], "accepted_pages": 0, "raw_rows": 0,
                "identity_issue_counts": {}, "pagination_terminal_observed": False,
                "returned_scope_fields_verified": None if index == 0 else True})
        state = states[index]; page_index = state["accepted_pages"]
        requested = state.get("next_cursor", "MA==" if index == 0 else None)
        spec = previous.request_spec(STREAMS[index], requested)
        require(not state["pagination_terminal_observed"] and type(page.get("page_index")) is int and page["page_index"] == page_index
            and page.get("requested_cursor") == requested, "prefix_cursor_or_index_differs")
        require(start.get("stream") == finish.get("stream") == page["stream"]
            and type(start.get("page_index")) is int and type(finish.get("page_index")) is int
            and start["page_index"] == finish["page_index"] == page_index
            and start.get("coverage_role") == finish.get("coverage_role") == "full_walk"
            and type(start.get("exact_page_attempt")) is int and start["exact_page_attempt"] == 1
            and start.get("request") == spec and start.get("request_sha256") == finish.get("request_sha256") == digest(encode(spec))
            and type(start.get("response_byte_reservation")) is int and 0 < start["response_byte_reservation"] <= CAPS["individual_response_bytes"]
            and finish.get("accepted") is True and finish.get("error_code") is None and finish.get("status_code") == 200
            and finish.get("body_complete") is True and finish.get("transport_dispatched") is True
            and finish.get("response_close_completed") is True and finish.get("interrupted") is False
            and finish.get("retry_delay_seconds") is None and finish.get("body") == page.get("body"), "prefix_attempt_binding_differs")
        body = page["body"]; name = f"responses/a{count:05d}.body.gz"
        require(type(body) is dict and body.get("path") == name and body.get("category") == "raw"
            and body.get("complete_received_body") is True and type(body.get("uncompressed_bytes")) is int
            and 0 <= body["uncompressed_bytes"] <= CAPS["individual_response_bytes"], "prefix_body_proof_differs")
        compressed = inputs.bind({"path": str(root / name), "bytes": body["bytes"], "sha256": body["sha256"]}, CAPS["individual_response_bytes"] + previous.GZIP_RESERVE)
        raw_bytes += len(compressed)
        with gzip.GzipFile(fileobj=__import__("io").BytesIO(compressed)) as handle:
            raw = handle.read(CAPS["individual_response_bytes"] + 1)
        inputs.check()
        require(len(raw) == body["uncompressed_bytes"] and digest(raw) == body.get("uncompressed_sha256"), "prefix_expanded_body_differs")
        parsed = legacy.page_envelope(raw, STREAMS[index], requested, *seen[index])
        summary = probe.inspect_rows(parsed["rows"], STREAMS[index], page_index)
        require(not summary["scope_conflict_counts"] and page.get("row_summary") == legacy.compact_summary(summary)
            and all(page.get(key) == parsed[key] for key in ("count", "next_cursor", "rows_sha256", "pagination_terminal_observed")),
            "prefix_envelope_summary_differs")
        require(type(finish.get("response_bytes_observed")) is int and finish["response_bytes_observed"] == len(raw)
            and type(finish.get("response_bytes_charged")) is int
            and len(raw) <= finish["response_bytes_charged"] <= start["response_byte_reservation"], "prefix_charge_differs")
        observed += len(raw); charge += finish["response_bytes_charged"]; rows += parsed["count"]
        state["accepted_pages"] += 1; state["raw_rows"] += parsed["count"]
        codes = Counter(state["identity_issue_counts"]); codes.update(summary["identity_issue_counts"])
        state["identity_issue_counts"] = dict(codes); state["pagination_terminal_observed"] = parsed["pagination_terminal_observed"]
        if index: state["returned_scope_fields_verified"] = state["returned_scope_fields_verified"] and summary["returned_scope_fields_verified"]
        state["next_cursor"] = parsed["next_cursor"]; seen[index][0].add(parsed["next_cursor"]); seen[index][1].add(parsed["rows_sha256"])
        files.add(name)
    require(next(attempts, None) is None and count == PIN["pages"] and rows == PIN["raw_rows"]
        and len(states) == len(PIN["streams"]), "prefix_counts_or_orphan_attempts")
    require(states == failure.get("streams") and all((state["accepted_pages"], state["raw_rows"], state["pagination_terminal_observed"]) == expected
        for state, expected in zip(states, PIN["streams"])), "prefix_failure_state_differs")
    usage = failure.get("usage", {}); fall = failure.get("waterfall", {})
    require(usage.get("this_run_http_attempts") == count and usage.get("this_run_response_bytes_charged") == charge
        and usage.get("response_bytes_observed_this_run") == observed and usage.get("http_attempts_charged") == old["http_attempts"] + count
        and usage.get("response_bytes_charged") == old["response_bytes_charged"] + charge
        and fall.get("accepted_pages") == fall.get("http_attempts_this_run") == count
        and fall.get("raw_rows_in_accepted_pages") == rows and not fall.get("failed_attempts", 0), "prefix_usage_differs")
    actual = set()
    for path in root.rglob("*"):
        inputs.check(); require(not path.is_symlink() and (path.is_file() or path.is_dir()), "prefix_tree_type")
        if path.is_file(): actual.add(str(path.relative_to(root)))
        else: require(path == root / "responses", "prefix_tree_directory_differs")
        require(len(actual) <= PIN["pages"] + 4, "prefix_tree_roster_differs")
    require(actual == files, "prefix_tree_roster_differs")
    expected_bytes = {"raw": old["bytes"]["raw"] + raw_bytes, "other": old["bytes"]["other"],
        "control": old["bytes"]["control"] + old["external_inventory"]["bytes"] + value["admission"]["bytes"]
            + sum(value[key]["bytes"] for key in ("start", "attempts", "pages")) + CAPS["external_root_controls_reservation_bytes"]}
    require(usage.get("retained_bytes_charged") == expected_bytes, "prefix_retained_totals_differ")
    return states, seen, {"accepted_pages": count, "raw_rows": rows, "response_bytes_charged": charge,
        "body_files": count, "fixed_prefix_adopted_by_reference": True}


def reopen_prefix_roster(inputs):
    root = Path(PIN["root"])
    expected = {"START.json", "FAILURE.json", "pages.jsonl", "attempts.jsonl"} | {
        f"responses/a{call:05d}.body.gz" for call in range(1, PIN["pages"] + 1)}
    actual = set()
    for path in root.rglob("*"):
        inputs.check(); require(not path.is_symlink() and (path.is_file() or path.is_dir()), "prefix_tree_type")
        if path.is_file(): actual.add(str(path.relative_to(root)))
        else: require(path == root / "responses", "prefix_tree_directory_differs")
        require(len(actual) <= len(expected), "prefix_tree_roster_differs")
    require(actual == expected, "prefix_tree_roster_differs")


def validate_usage(value, prefix, old, failure, inputs, admission_path, output):
    require(type(value) is dict and set(value) == {"http_attempts", "response_bytes_charged", "runtime_seconds", "retained_bytes_charged", "root_controls"}, "prior_ledger_schema_v5")
    for key, cap in (("http_attempts", CAPS["http_attempts"]), ("response_bytes_charged", CAPS["total_response_bytes"]), ("runtime_seconds", CAPS["runtime_seconds"])):
        number(value[key], cap, integer=key != "runtime_seconds")
        reported = failure["usage"]["runtime_seconds_charged" if key == "runtime_seconds" else "http_attempts_charged" if key == "http_attempts" else key]
        require(value[key] >= max(PRIOR_MINIMUM[key], reported), "prior_usage_below_floor")
    amounts = value["retained_bytes_charged"]
    require(type(amounts) is dict and set(amounts) == set(RETAINED_MINIMUM), "prior_retained_schema")
    controls = value["root_controls"]
    require(type(controls) is list and len(controls) <= 256, "root_control_roster_invalid")
    old_paths = {item["path"] for item in old["files"]} | {old["external_inventory"]["path"], prefix["admission"]["path"]}
    required = {prefix[key]["path"] for key in ("launch", "exit", "drain")}
    paths = set(); control_bytes = 0
    for ref in controls:
        path = Path(ref["path"])
        require(path.parent == OPERATIONS and str(path) not in paths and str(path) not in old_paths
            and path != admission_path and path != output and not path.name.endswith((".part", ".body", ".body.gz"))
            and re.fullmatch(r"(?:full_metadata_v[45](?:[._][A-Za-z0-9_.-]+)?|full_acquisition_(?:root_admission_checks_v3|dispatch_v3\.(?:START|COMPLETE))|finalization_shutdown_drain_v2)\.(?:json|log)", path.name)
            and path.name not in {"attempts.jsonl", "pages.jsonl", "START.json", "FAILURE.json", "manifest.json", "COMPLETE.json"},
            "root_control_invalid_or_duplicate")
        inputs.bind(ref, MAX_CONTROL); paths.add(str(path)); control_bytes += ref["bytes"]
    require(required <= paths and control_bytes <= CAPS["external_root_controls_reservation_bytes"], "root_controls_missing_or_limit")
    for key, cap in (("raw", CAPS["raw_body_bytes"]), ("control", CAPS["control_bytes"]), ("other", CAPS["other_metadata_bytes"])):
        number(amounts[key], cap)
        minimum = max(RETAINED_MINIMUM[key], failure["usage"]["retained_bytes_charged"][key])
        if key == "control": minimum += prefix["failure"]["bytes"] + control_bytes
        require(amounts[key] >= minimum, "prior_retained_floor_refunded")
    require(sum(amounts.values()) <= CAPS["aggregate_metadata_bytes"], "prior_byte_cap")
    # The independently validated original prior retains its own resource
    # floors. Prefix bytes are already in failure's cumulative floors, not added
    # a second time. Every extra OP control is conservatively additive.
    return {"roots": old["roots"], "files": old["files"], "bytes": dict(amounts),
        "http_attempts": value["http_attempts"], "response_bytes_charged": value["response_bytes_charged"],
        "runtime_seconds": value["runtime_seconds"], "external_inventory": {"bytes": 0},
        "root_control_bytes_additively_charged": control_bytes}


class Ledger(previous.Ledger):
    def __init__(self, output, prior, admission_bytes, began): super().__init__(output, prior, admission_bytes, began, "full")


PHASES = {"admission", "parent_validation", "prefix_replay", "source_check", "request_prepare", "spacing_wait", "attempt_start",
          "http_attempt", "page_processing", "retry_wait", "state_update", "final_reopen", "publication"}


def diagnostic(exc, phase):
    cls = type(exc)
    allowed_classes = {Error, OSError, FileNotFoundError, PermissionError, TimeoutError, ValueError, TypeError,
        KeyError, IndexError, MemoryError, RuntimeError, OverflowError, UnicodeError, UnicodeDecodeError,
        UnicodeEncodeError, EOFError, KeyboardInterrupt, SystemExit, subprocess.CalledProcessError, subprocess.TimeoutExpired}
    result = {"phase": phase if phase in PHASES else "admission", "exception_module": cls.__module__ if cls in allowed_classes else "unlisted",
        "exception_class": cls.__name__ if cls in allowed_classes else "unlisted", "errno": None, "code_frames": []}
    if isinstance(exc, OSError) and type(exc.errno) is int: result["errno"] = exc.errno
    allowed = {str(REPO / name): name for name in SOURCE_FILES}
    frame = exc.__traceback__
    while frame:
        code = frame.tb_frame.f_code
        if code.co_filename in allowed and re.fullmatch(r"[A-Za-z0-9_<>]{1,96}", code.co_name):
            result["code_frames"].append({"file": allowed[code.co_filename], "function": code.co_name, "line": frame.tb_lineno})
            result["code_frames"] = result["code_frames"][-8:]
        frame = frame.tb_next
    return result


def run_campaign(output, expected_head, admission_path, admission_sha, mode="full", *, transport=None, sleeper=time.sleep, debit_prior_runtime=None):
    began = time.monotonic(); phase = "admission"; ledger = None
    output, admission_path = Path(output), Path(admission_path)
    require(mode == "full" and CAPS == previous.CAPS, "full_only_or_caps_changed")
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.resolve() == output.parent
        and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    source = source_snapshot(expected_head)
    admission_ref = {"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}
    raw, admission_bound = read_bound(admission_ref)
    admission = probe.strict_json(raw)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "mode", "consumer_head", "run_dir", "caps", "scope_sha256",
        "v4_prefix", "prior_ledger", "root_checked_prior_producers_drained"}
        and admission["schema_version"] == SCHEMA and admission["status"] == STATUS and admission["mode"] == "full"
        and admission["consumer_head"] == expected_head and admission["run_dir"] == str(output) and admission["caps"] == CAPS
        and admission["scope_sha256"] == digest(encode(SCOPE)) and admission["root_checked_prior_producers_drained"] is True,
        "root_admission_not_exact")
    prior_time = number(admission["prior_ledger"].get("runtime_seconds"), CAPS["runtime_seconds"], integer=False)
    require(prior_time >= PRIOR_MINIMUM["runtime_seconds"], "prior_runtime_below_floor")
    def check(): require(prior_time + time.monotonic() - began < CAPS["runtime_seconds"], "runtime_limit")
    check()
    if debit_prior_runtime is not None: debit_prior_runtime(prior_time, began)
    inputs = Inputs(check); inputs.bind(admission_ref, previous.MAX_INPUT)
    phase = "parent_validation"
    old, failure = validate_parent(admission["v4_prefix"], inputs, source, output)
    prior = validate_usage(admission["prior_ledger"], admission["v4_prefix"], old, failure, inputs, admission_path, output)
    phase = "prefix_replay"
    states, seen, replay_summary = replay(admission["v4_prefix"], inputs, old, failure)
    require(source_snapshot(expected_head) == source, "source_changed_before_dispatch")
    inputs.reopen(); reopen_prefix_roster(inputs); check()
    require(probe.available_ram() >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CONTROL_RESERVE + CAPS["external_root_controls_reservation_bytes"], "disk_floor")
    output.mkdir(mode=0o700); ledger = Ledger(output, prior, len(raw), began)
    waterfalls = Counter(); last_dispatch = -math.inf; transport = transport or previous.public_transport
    try:
        ledger.json("START.json", {"schema_version": START_SCHEMA, "mode": "full", "source": source, "admission": admission_bound,
            "caps": CAPS, "scope": SCOPE, "adopted_prefix": admission["v4_prefix"], "replay": replay_summary,
            "root_controls_additively_charged": prior["root_control_bytes_additively_charged"],
            "started_utc": probe.utc_now(), "universe_complete": False, "analysis_admitted": False})
        for index, stream in enumerate(STREAMS):
            if index < len(states): state = states[index]
            else:
                state = {"id": stream["id"], "provider": stream["provider"], "accepted_pages": 0, "raw_rows": 0,
                    "identity_issue_counts": {}, "pagination_terminal_observed": False, "returned_scope_fields_verified": True}
                states.append(state); seen[index] = ({None}, set())
            if state["pagination_terminal_observed"]: continue
            require(index >= 2 and stream["provider"] == "gamma", "unexpected_continuation_scope")
            cursor = state.get("next_cursor"); page_index = state["accepted_pages"]
            while not state["pagination_terminal_observed"]:
                phase = "request_prepare"; spec = previous.request_spec(stream, cursor)
                parsed = summary = None
                for exact_attempt in range(1, CAPS["attempts_per_exact_page"] + 1):
                    require(ledger.http_attempts < CAPS["http_attempts"], "http_attempt_limit")
                    require(ledger.responses < CAPS["total_response_bytes"], "total_response_byte_limit")
                    phase = "source_check"; require(source_snapshot(expected_head) == source, "source_changed")
                    allowance = min(CAPS["individual_response_bytes"], CAPS["total_response_bytes"] - ledger.responses)
                    ledger.reserve("raw", allowance + previous.GZIP_RESERVE); ledger.reserve("control", MAX_CONTROL)
                    phase = "spacing_wait"; wait_until(last_dispatch + .5, ledger, sleeper)
                    ledger.http_attempts += 1; call = ledger.http_attempts - prior["http_attempts"]
                    phase = "attempt_start"
                    start = {"event": "START", "call": call, "cumulative_http_attempt": ledger.http_attempts,
                        "stream": stream["id"], "page_index": page_index, "coverage_role": "v4_continuation",
                        "exact_page_attempt": exact_attempt, "requested_utc": probe.utc_now(), "request": spec,
                        "request_sha256": digest(encode(spec)), "response_byte_reservation": allowance}
                    # A durable START without a FINISH must already carry its
                    # full conservative response debit, even at a signal/I/O
                    # boundary before transport is actually dispatched.
                    before = ledger.responses; ledger.responses += allowance
                    ledger.append("attempts.jsonl", start)
                    last_dispatch = time.monotonic(); deadline = last_dispatch + HTTP_SECONDS
                    observed = None; finish_written = False
                    try:
                        phase = "http_attempt"; observed = received_attempt(ledger, call, spec, allowance, transport, deadline)
                        require(observed["response_bytes_observed"] <= observed["response_bytes_charged"] <= allowance, "attempt_charge_invalid")
                        if not observed["body_complete"]: observed["response_bytes_charged"] = allowance
                        ledger.responses = before + observed["response_bytes_charged"]; ledger.observed += observed["response_bytes_observed"]
                        ledger.bodies.append(observed["body"]); code = observed["error_code"]
                        if code is not None: raise Error(code)
                        phase = "page_processing"
                        with attempt_alarm(ledger, deadline) as (check_attempt, _):
                            with gzip.open(output / observed["body"]["path"], "rb") as handle: body_raw = handle.read(CAPS["individual_response_bytes"] + 1)
                            require(len(body_raw) <= CAPS["individual_response_bytes"], "body_expansion_limit")
                            parsed = legacy.page_envelope(body_raw, stream, cursor, *seen[index])
                            summary = probe.inspect_rows(parsed["rows"], stream, page_index)
                            require(not summary["scope_conflict_counts"], "gamma_returned_scope_conflict")
                            page = {"event": "PAGE", "call": call, "stream": stream["id"], "page_index": page_index,
                                "coverage_role": "v4_continuation", "requested_cursor": cursor, "accepted": True, "body": observed["body"],
                                **{key: val for key, val in parsed.items() if key != "rows"}, "row_summary": legacy.compact_summary(summary)}
                            ledger.append("pages.jsonl", page); check_attempt()
                            ledger.append("attempts.jsonl", {"event": "FINISH", "call": call, "stream": stream["id"], "page_index": page_index,
                                "coverage_role": "v4_continuation", "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"],
                                **observed, "accepted": True, "retry_delay_seconds": None})
                            finish_written = True; check_attempt()
                    except BaseException as exc:
                        code = safe_code(exc, "attempt_processing_error")
                        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit)) or observed is not None and observed["interrupted"]
                        if observed is None or code in {"runtime_limit", "http_attempt_timeout", "interrupted", "attempt_processing_error"}: ledger.responses = before + allowance
                        if observed is None:
                            observed = {"status_code": None, "body": None, "response_bytes_observed": 0, "body_complete": False,
                                "transport_dispatched": True, "interrupted": interrupted, "response_close_attempted": False,
                                "response_close_completed": False, "retry_after_header": None}
                        observed.update(error_code=code, response_bytes_charged=ledger.responses - before)
                        retryable = code == "request_transport_error" or code == "http_status_not_200" and observed["status_code"] in probe.RETRYABLE
                        delay = None
                        if retryable:
                            try: delay = legacy.retry_delay(observed["retry_after_header"])
                            except Error as invalid: code, retryable = safe_code(invalid, "invalid_retry_after"), False; observed["error_code"] = code
                        ledger.append("attempts.jsonl", {"event": "INVALIDATE" if finish_written else "FINISH", "call": call,
                            "stream": stream["id"], "page_index": page_index, "coverage_role": "v4_continuation",
                            "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"], **observed, "accepted": False,
                            "retry_delay_seconds": delay, "diagnostic": diagnostic(exc, phase)}, failure=True)
                        waterfalls["http_attempts_this_run"] += 1; waterfalls["failed_attempts"] += 1
                        if interrupted: raise KeyboardInterrupt()
                        require(retryable and exact_attempt < CAPS["attempts_per_exact_page"], code)
                        phase = "retry_wait"; wait_until(time.monotonic() + delay, ledger, sleeper)
                        continue
                    waterfalls["http_attempts_this_run"] += 1; break
                phase = "state_update"; waterfalls["accepted_pages"] += 1; waterfalls["raw_rows_in_accepted_pages"] += parsed["count"]
                state["accepted_pages"] += 1; state["raw_rows"] += parsed["count"]
                codes = Counter(state["identity_issue_counts"]); codes.update(summary["identity_issue_counts"])
                state["identity_issue_counts"] = dict(codes); state["pagination_terminal_observed"] = parsed["pagination_terminal_observed"]
                state["returned_scope_fields_verified"] = state["returned_scope_fields_verified"] and summary["returned_scope_fields_verified"]
                cursor = parsed["next_cursor"]; state["next_cursor"] = cursor
                seen[index][0].add(cursor); seen[index][1].add(parsed["rows_sha256"]); page_index += 1
        require(len(states) == 5 and all(row["pagination_terminal_observed"] for row in states), "pagination_incomplete")
        require(sum(row["raw_rows"] for row in states) == PIN["raw_rows"] + waterfalls["raw_rows_in_accepted_pages"], "count_conservation_failed")
        def recheck():
            require(source_snapshot(expected_head) == source and read_bound(admission_ref)[1] == admission_bound, "source_or_admission_changed")
            require(previous.inventory_prior(probe.strict_json(inputs.bind(admission["v4_prefix"]["admission"], previous.MAX_INPUT))["prior_ledger"],
                Path(admission["v4_prefix"]["admission"]["path"]), Path(PIN["root"])) == old, "original_prior_changed")
            inputs.reopen(); reopen_prefix_roster(inputs); ledger.check()
        phase = "final_reopen"; ledger.reopen(); recheck()
        inventory = b"".join(encode({"path": name, **bound}) for name, bound in sorted(ledger.files.items()))
        inventory_bound = ledger.write("artifact_inventory.jsonl", inventory)
        manifest = {"schema_version": START_SCHEMA, "mode": "full", "status": SUCCESS, "source": source,
            "admission": admission_bound, "caps": CAPS, "scope": SCOPE, "adopted_prefix": admission["v4_prefix"], "replay": replay_summary,
            "streams": states, "waterfall_new_run": dict(waterfalls), "usage": ledger.usage(final_reservation=CONTROL_RESERVE),
            "artifact_inventory": inventory_bound, "universe_complete": False, "analysis_admitted": False,
            "family_or_payout_admitted": False, "snapshot_consistency_certified": False, "normalized_labels_emitted": False}
        phase = "publication"; manifest_bound = ledger.json("manifest.json", manifest)
        ledger.complete({"schema_version": "polymarket_market_metadata_campaign_completion_v5", "mode": "full", "status": SUCCESS,
            "manifest": manifest_bound, "universe_complete": False, "analysis_admitted": False}, recheck)
        return {"status": SUCCESS, "mode": "full", "usage": manifest["usage"], "streams": states,
            "universe_complete": False, "analysis_admitted": False}
    except BaseException as exc:
        code = safe_code(exc, "internal_runtime_error")
        result = {"schema_version": START_SCHEMA, "mode": "full", "status": "failed_unaccepted", "error_code": code,
            "diagnostic": diagnostic(exc, phase), "usage": ledger.usage(), "streams": states,
            "waterfall_new_run": dict(waterfalls), "universe_complete": False, "analysis_admitted": False}
        try: ledger.json("FAILURE.json", result, failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("full",), required=True)
    parser.add_argument("--admission", type=Path, required=True); parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True); parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv); require_production_host()
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        with legacy.process_limits() as debit:
            result = run_campaign(args.run_dir, args.expected_head, args.admission, args.admission_sha256,
                                  args.mode, debit_prior_runtime=debit)
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        result = {"status": "failed_unaccepted", "error_code": safe_code(exc, "admission_or_environment_error"),
            "diagnostic": diagnostic(exc, "admission"), "analysis_admitted": False}
    print(__import__("json").dumps(result, sort_keys=True))
    return 0 if result["status"] == SUCCESS else 1


if __name__ == "__main__":
    raise SystemExit(main())
