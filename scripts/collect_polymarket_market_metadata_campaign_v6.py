#!/usr/bin/env python3
"""Root-admitted raw metadata continuation under the human-approved v6 budget.

The two immutable failed walks are replayed in full before any new HTTP call.
The final partial response remains rejected, and its exact-page attempt is not
reset. This is a multi-vintage raw walk, not universe or sample admission.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import io
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_market_metadata_campaign_v5 as previous

v4, legacy, probe = previous.previous, previous.legacy, previous.probe
Error, require, encode, digest = previous.Error, previous.require, previous.encode, previous.digest
identity, hash_file, read_bound, number = previous.identity, previous.hash_file, previous.read_bound, previous.number
Inputs, _ref = previous.Inputs, previous._ref
STREAMS, HEADERS, HTTP_SECONDS = previous.STREAMS, previous.HEADERS, previous.HTTP_SECONDS
MAX_CONTROL, CONTROL_RESERVE = previous.MAX_CONTROL, previous.CONTROL_RESERVE
wait_until, attempt_alarm, received_attempt = previous.wait_until, previous.attempt_alarm, previous.received_attempt
safe_code = previous.safe_code
CAPS = {**previous.CAPS, "http_attempts": 100_000, "total_response_bytes": 96 * 1024**3,
    "aggregate_metadata_bytes": 20_000_000_000, "raw_body_bytes": 12_000_000_000,
    "control_bytes": 1_000_000_000}
RUN_ROOT = Path("/mnt/data/runs")
OPERATIONS = RUN_ROOT / "2026-10-27_polymarket_oct1_finalization_v1"
OUTPUT_NAME = "2026-10-28_polymarket_market_metadata_full_v6"
SCHEMA = "polymarket_market_metadata_campaign_admission_v6"
START_SCHEMA = "polymarket_market_metadata_campaign_v6"
STATUS = "root_compact_metadata_budget_continuation_admitted"
SUCCESS = "requested_metadata_continuation_complete_not_universe_admission"
SOURCE_FILES = (*previous.SOURCE_FILES, "scripts/collect_polymarket_market_metadata_campaign_v6.py",
                "tests/test_polymarket_market_metadata_campaign_v6.py")
SCOPE = {**previous.SCOPE, "continuation": "only_pinned_v4_then_v5_raw_walk_prefixes",
    "human_approved_budget_version": 6, "failed_partial_page_adopted": False,
    "inherited_exact_page_attempts_preserved": True, "atomic_snapshot_certified": False}
PRIOR_MINIMUM = {"http_attempts": 38565, "response_bytes_charged": 34359738368, "runtime_seconds": 40200}
RETAINED_MINIMUM = {"raw": 3401462298, "control": 199303529, "other": 16865544}
LATE_V5_CONTROLS = 69075  # fixed terminal union: six receipts + audit code + drain
APPROVAL_PIN = (1558, "e80d1ccd0fec9d1118835cdf915c1be16df5dee76560bb81b3a14d756ea243fc")
PIN = {"root": str(RUN_ROOT / "2026-10-27_polymarket_market_metadata_full_v5"),
    "producer_head": "42d7c5e6c21c03100dd7e22a88a391ca2e278f66",
    "admission": (6301, "89be28c7d19976ad9ab7863ade642940a83bfa294066d91ef557e2ce9d24ef00"),
    "start": (6027, "078f1c4b78717d7acfde215c47d35dfe8499fa39222d790078cc255e0e889bbe"),
    "failure": (2440, "762db1f2f5a52cf19f6c0a933d1a1669601e1b616059952d4d77af5828cff227"),
    "launch": (3473, "1ab1d8da85b925614f72557578232379476062fd69b84777c4aa3882835928b0"),
    "exit": (3914, "4b49be1978296e9136c3777d79b6bba7b7ae54f329eccd41c80932be02c0d34a"),
    "drain": (18522, "de9f38ccd916a726b0a6d580f4384ba09c5271df4068e21a4a3cd81813e60f01"),
    "pages": 22245, "calls": 22246, "raw_rows": 2224500,
    "streams": ((5132, 5131572, True), (2740, 273967, True), (25829, 2582900, False))}
JOURNAL_LIMIT = 128 * 1024**2
INVENTORY_SEGMENT_BYTES = 1024**2
INVENTORY_SEGMENTS = 128
PREFIX_KEYS = {"root", "producer_head", "admission", "start", "failure", "launch", "exit", "drain", "attempts", "pages"}


def admission_contract():
    return {"schema_version": SCHEMA, "status": STATUS, "mode": "full", "consumer_head": "clean committed HEAD",
        "run_dir": str(RUN_ROOT / OUTPUT_NAME), "caps": CAPS, "scope_sha256": digest(encode(SCOPE)),
        "v4_prefix": previous.admission_contract()["v4_prefix"],
        "v5_prefix": {"root": PIN["root"], "producer_head": PIN["producer_head"],
            **{key: "strict path/bytes/sha256; fixed historical pin" for key in
               ("admission", "start", "failure", "launch", "exit", "drain")},
            "attempts": "strict ref to attempts.jsonl, streamed <=128MiB", "pages": "strict ref to pages.jsonl, streamed <=128MiB"},
        "approval": {"path": str(OPERATIONS / "full_metadata_v6_human_budget_approval_v1.json"),
                     "bytes": APPROVAL_PIN[0], "sha256": APPROVAL_PIN[1]},
        "prior_ledger": {**PRIOR_MINIMUM, "runtime_seconds": "40200 + ceil(preparation elapsed since approval start UTC)",
            "retained_bytes_charged": dict(RETAINED_MINIMUM),
            "root_controls": "<=256 literal OP JSON/log refs; combined external closure <=2MiB; additive late charges"},
        "root_checked_prior_producers_drained": True}


def source_snapshot(head):
    base = previous.source_snapshot(head)
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        bound = hash_file(REPO / name, v4.MAX_INPUT)
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
    require(previous.source_snapshot(head) == {"head": head,
        "files": {name: base["files"][name] for name in previous.SOURCE_FILES}}, "source_changed")
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        bound = hash_file(REPO / name, v4.MAX_INPUT)
        require({key: bound[key] for key in ("bytes", "sha256")} == base["files"][name], "own_source_changed")
    return base


def validate_admission(value, expected_head, output):
    require(type(value) is dict and set(value) == {"schema_version", "status", "mode", "consumer_head", "run_dir", "caps", "scope_sha256",
        "v4_prefix", "v5_prefix", "approval", "prior_ledger", "root_checked_prior_producers_drained"}
        and value["schema_version"] == SCHEMA and value["status"] == STATUS and value["mode"] == "full"
        and value["consumer_head"] == expected_head and value["run_dir"] == str(output) and value["caps"] == CAPS
        and value["scope_sha256"] == digest(encode(SCOPE)) and value["root_checked_prior_producers_drained"] is True,
        "root_admission_not_exact")
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head) is not None, "consumer_head_invalid")


def validate_approval(ref, inputs, started_utc):
    require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"}
        and ref["path"] == str(OPERATIONS / "full_metadata_v6_human_budget_approval_v1.json")
        and (ref["bytes"], ref["sha256"]) == APPROVAL_PIN, "human_approval_pin_differs")
    value = inputs.json(ref)
    require(value.get("schema") == "polymarket_metadata_v6_human_budget_approval_v1"
        and value.get("status") == "human_authorized_budget_change_not_production_admission"
        and value.get("human_reply") == "yes sure" and value.get("production_launch_admitted") is False
        and value.get("fresh_preparation_must_be_added_to_prior_runtime") is True, "human_approval_differs")
    began = datetime.fromisoformat(value["fresh_preparation_conservative_start_utc"].replace("Z", "+00:00"))
    require(began.tzinfo is not None and started_utc.tzinfo is not None and started_utc >= began, "preparation_clock_invalid")
    return PRIOR_MINIMUM["runtime_seconds"] + math.ceil((started_utc - began).total_seconds())


def validate_v5_parent(value, v4_prefix, inputs, source, output):
    require(type(value) is dict and set(value) == PREFIX_KEYS and value["root"] == PIN["root"]
        and value["producer_head"] == PIN["producer_head"], "v5_prefix_contract_differs")
    root = Path(value["root"])
    require(root.parent == RUN_ROOT and root.resolve() == root and root != output, "v5_prefix_root_invalid")
    names = {"admission": OPERATIONS / "full_metadata_v5_admission_v1.json", "start": root / "START.json",
        "failure": root / "FAILURE.json", "launch": OPERATIONS / "full_metadata_v5.LAUNCH.json",
        "exit": OPERATIONS / "full_metadata_v5.EXIT.json", "drain": OPERATIONS / "full_metadata_v5_root_terminal_drain_v1.json",
        "attempts": root / "attempts.jsonl", "pages": root / "pages.jsonl"}
    controls = {}
    for key, path in names.items():
        ref = value[key]
        require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"} and ref["path"] == str(path), "v5_prefix_path_differs")
        if key in {"attempts", "pages"}:
            require(type(ref["bytes"]) is int and 0 < ref["bytes"] <= JOURNAL_LIMIT
                and type(ref["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]), "v5_journal_ref_invalid")
        else:
            require((ref["bytes"], ref["sha256"]) == PIN[key], "v5_prefix_control_pin_differs")
            controls[key] = inputs.json(ref)
    start, failure, parent, launch, exited, drain = (controls[key] for key in ("start", "failure", "admission", "launch", "exit", "drain"))
    old_source = {"head": PIN["producer_head"], "files": {name: source["files"][name] for name in previous.SOURCE_FILES}}
    v4.verify_producer(old_source, old_source["files"])
    require(start.get("schema_version") == previous.START_SCHEMA and start.get("mode") == "full"
        and start.get("source") == old_source and start.get("caps") == previous.CAPS
        and start.get("scope") == probe.strict_json(encode(previous.SCOPE))
        and _ref(start.get("admission", {})) == value["admission"] and start.get("adopted_prefix") == v4_prefix
        and start.get("universe_complete") is False and start.get("analysis_admitted") is False, "v5_prefix_start_differs")
    require(parent.get("schema_version") == previous.SCHEMA and parent.get("status") == previous.STATUS
        and parent.get("mode") == "full" and parent.get("consumer_head") == PIN["producer_head"]
        and parent.get("run_dir") == str(root) and parent.get("caps") == previous.CAPS
        and parent.get("scope_sha256") == digest(encode(previous.SCOPE)) and parent.get("v4_prefix") == v4_prefix
        and parent.get("root_checked_prior_producers_drained") is True, "v5_prefix_admission_differs")
    require(failure.get("schema_version") == previous.START_SCHEMA and failure.get("mode") == "full"
        and failure.get("status") == "failed_unaccepted" and failure.get("error_code") == "response_byte_limit"
        and failure.get("universe_complete") is False and failure.get("analysis_admitted") is False
        and not (root / "COMPLETE.json").exists() and not (root / "manifest.json").exists(), "v5_prefix_failure_differs")
    require(launch.get("schema_version") == "polymarket_oct1_stage_launcher_v3" and launch.get("stage") == "metadata"
        and launch.get("status") == "producer_dispatched_owned_session" and launch.get("output_dir") == str(root)
        and launch.get("source", {}).get("head") == PIN["producer_head"] and launch.get("admission") == start["admission"]
        and launch.get("ownership_unknown") is False and launch.get("retry_or_resume") is False, "v5_prefix_launch_differs")
    require(exited.get("schema_version") == launch["schema_version"] and exited.get("stage") == "metadata"
        and exited.get("status") == "producer_exited" and exited.get("output_dir") == str(root)
        and exited.get("source") == launch["source"] and exited.get("admission") == launch["admission"]
        and exited.get("command") == launch.get("command") and exited.get("error_code") is None
        and type(exited.get("producer_exit_code")) is int and exited["producer_exit_code"] == 1
        and all(exited.get(key) is True for key in ("owned_group_drained", "stdout_eof_observed", "source_and_admission_unchanged"))
        and exited.get("ownership_unknown") is False and exited.get("children_still_running") is False, "v5_prefix_exit_differs")
    require(drain.get("schema_version") == "polymarket_metadata_v5_root_terminal_drain_v1"
        and drain.get("status") == "root_verified_terminal_failed_metadata_stage_drained"
        and drain.get("run_dir") == str(root) and drain.get("source_head") == PIN["producer_head"]
        and drain.get("admission") == value["admission"]
        and all(drain.get(key) is True for key in ("producer_pid_absent", "caller_pid_absent", "owned_group_and_session_empty", "transfers_drained"))
        and type(drain.get("relevant_consumers_remaining")) is int and drain["relevant_consumers_remaining"] == 0
        and drain.get("universe_admitted") is False and drain.get("final_sample_admitted") is False
        and drain.get("metadata_runtime_seconds_floor_for_next_stage") == 40200, "v5_prefix_drain_differs")
    for key, refkey in (("producer_start", "start"), ("failure", "failure"), ("launch", "launch"), ("exit", "exit")):
        require(drain.get("bindings", {}).get(key) == value[refkey], "v5_drain_binding_differs")
    for role in ("child", "caller"):
        previous._birth(drain.get(role)); require(drain[role] == launch.get(role) == exited.get(role), "v5_birth_differs")
    require(drain["child"]["boot_id"] == drain["caller"]["boot_id"] and drain["child"]["hostname"] == drain["caller"]["hostname"]
        and drain["child"]["pid"] != drain["caller"]["pid"], "v5_birth_differs")
    return controls


def body_bytes(root, body, call, inputs, *, complete):
    require(type(body) is dict and body.get("path") == f"responses/a{call:05d}.body.gz" and body.get("category") == "raw"
        and body.get("complete_received_body") is complete and type(body.get("uncompressed_bytes")) is int
        and 0 <= body["uncompressed_bytes"] <= CAPS["individual_response_bytes"], "v5_body_proof_differs")
    compressed = inputs.bind({"path": str(root / body["path"]), "bytes": body["bytes"], "sha256": body["sha256"]},
        CAPS["individual_response_bytes"] + v4.GZIP_RESERVE)
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as handle:
        raw = handle.read(CAPS["individual_response_bytes"] + 1)
    inputs.check()
    require(len(raw) == body["uncompressed_bytes"] and digest(raw) == body.get("uncompressed_sha256"), "v5_expanded_body_differs")
    return raw, len(compressed)


def check_v5_tree(value, inputs):
    root = Path(value["root"])
    expected = {"START.json", "FAILURE.json", "attempts.jsonl", "pages.jsonl"} | {
        f"responses/a{call:05d}.body.gz" for call in range(1, PIN["calls"] + 1)}
    actual = set()
    for path in root.rglob("*"):
        inputs.check(); require(not path.is_symlink() and (path.is_file() or path.is_dir()), "v5_tree_type")
        if path.is_file(): actual.add(str(path.relative_to(root)))
        else: require(path == root / "responses", "v5_tree_directory_differs")
        require(len(actual) <= len(expected), "v5_tree_roster_differs")
    require(actual == expected, "v5_tree_roster_differs")


def replay_v5(value, inputs, controls, old_prior, states, seen, replay_v4):
    root = Path(value["root"]); failure = controls["failure"]
    require(controls["start"].get("replay") == replay_v4, "v5_adopted_replay_differs")
    attempts = inputs.lines(value["attempts"], JOURNAL_LIMIT)
    charge = observed = rows = count = raw_bytes = 0
    index = 2; stream = STREAMS[index]; state = states[index]
    for page in inputs.lines(value["pages"], JOURNAL_LIMIT):
        count += 1; require(count <= PIN["pages"] and type(page) is dict and page.get("event") == "PAGE"
            and type(page.get("call")) is int and page["call"] == count and page.get("accepted") is True
            and page.get("coverage_role") == "v4_continuation" and page.get("stream") == stream["id"], "v5_page_sequence_differs")
        start, finish = next(attempts, None), next(attempts, None)
        spec = v4.request_spec(stream, state["next_cursor"]); page_index = state["accepted_pages"]
        validate_pair(start, finish, count, page_index, spec, accepted=True)
        require(start["cumulative_http_attempt"] == old_prior["http_attempts"] + count, "v5_cumulative_call_differs")
        remaining = previous.CAPS["total_response_bytes"] - old_prior["response_bytes_charged"] - charge
        require(remaining > 0 and start["response_byte_reservation"] == min(previous.CAPS["individual_response_bytes"], remaining),
            "v5_historical_reservation_differs")
        require(not state["pagination_terminal_observed"] and type(page.get("page_index")) is int and page["page_index"] == page_index
            and page.get("requested_cursor") == state["next_cursor"] and finish.get("body") == page.get("body"), "v5_page_binding_differs")
        raw, compressed_bytes = body_bytes(root, page["body"], count, inputs, complete=True); raw_bytes += compressed_bytes
        require(finish["response_bytes_observed"] == finish["response_bytes_charged"] == len(raw), "v5_complete_charge_differs")
        parsed = legacy.page_envelope(raw, stream, state["next_cursor"], *seen[index])
        summary = probe.inspect_rows(parsed["rows"], stream, page_index)
        require(not summary["scope_conflict_counts"] and page.get("row_summary") == legacy.compact_summary(summary)
            and all(page.get(key) == parsed[key] for key in ("count", "next_cursor", "rows_sha256", "pagination_terminal_observed")), "v5_envelope_summary_differs")
        charge += finish["response_bytes_charged"]; observed += len(raw); rows += parsed["count"]
        state["accepted_pages"] += 1; state["raw_rows"] += parsed["count"]
        codes = Counter(state["identity_issue_counts"]); codes.update(summary["identity_issue_counts"])
        state["identity_issue_counts"] = dict(codes); state["pagination_terminal_observed"] = parsed["pagination_terminal_observed"]
        state["returned_scope_fields_verified"] = state["returned_scope_fields_verified"] and summary["returned_scope_fields_verified"]
        state["next_cursor"] = parsed["next_cursor"]; seen[index][0].add(parsed["next_cursor"]); seen[index][1].add(parsed["rows_sha256"])
    # Exactly one final rejected partial response is retained and billed. It is
    # deliberately never fed to the envelope/row parser or the accepted state.
    call = count + 1; start, finish = next(attempts, None), next(attempts, None)
    spec = v4.request_spec(stream, state["next_cursor"])
    validate_pair(start, finish, call, state["accepted_pages"], spec, accepted=False)
    require(start["cumulative_http_attempt"] == old_prior["http_attempts"] + call, "v5_cumulative_call_differs")
    remaining = previous.CAPS["total_response_bytes"] - old_prior["response_bytes_charged"] - charge
    require(remaining > 0 and start["response_byte_reservation"] == min(previous.CAPS["individual_response_bytes"], remaining),
        "v5_historical_reservation_differs")
    raw, compressed_bytes = body_bytes(root, finish["body"], call, inputs, complete=False); raw_bytes += compressed_bytes
    require(finish["response_bytes_observed"] == len(raw) and finish["response_bytes_charged"] == start["response_byte_reservation"], "v5_failed_charge_differs")
    charge += finish["response_bytes_charged"]; observed += len(raw)
    require(next(attempts, None) is None and count == PIN["pages"] and call == PIN["calls"] and rows == PIN["raw_rows"], "v5_counts_or_orphan_attempts")
    require(states == failure.get("streams") and all((s["accepted_pages"], s["raw_rows"], s["pagination_terminal_observed"]) == expected
        for s, expected in zip(states, PIN["streams"])) and len(states) == len(PIN["streams"]), "v5_failure_state_differs")
    usage, fall = failure.get("usage", {}), failure.get("waterfall_new_run", {})
    require(usage.get("this_run_http_attempts") == call and usage.get("this_run_response_bytes_charged") == charge
        and usage.get("response_bytes_observed_this_run") == observed
        and usage.get("http_attempts_charged") == old_prior["http_attempts"] + call
        and usage.get("response_bytes_charged") == old_prior["response_bytes_charged"] + charge == previous.CAPS["total_response_bytes"]
        and fall == {"accepted_pages": count, "failed_attempts": 1, "http_attempts_this_run": call, "raw_rows_in_accepted_pages": rows}, "v5_usage_differs")
    expected_bytes = {"raw": old_prior["bytes"]["raw"] + raw_bytes, "other": old_prior["bytes"]["other"],
        "control": old_prior["bytes"]["control"] + value["admission"]["bytes"]
            + sum(value[key]["bytes"] for key in ("start", "attempts", "pages")) + CAPS["external_root_controls_reservation_bytes"]}
    require(usage.get("retained_bytes_charged") == expected_bytes, "v5_retained_totals_differ")
    rec = controls["drain"].get("reconciliation", {})
    require(rec.get("new_accepted_pages") == count and rec.get("new_http_attempts") == call and rec.get("new_raw_rows") == rows
        and rec.get("new_response_bytes_charged") == charge and rec.get("failed_page_index") == state["accepted_pages"]
        and rec.get("next_cursor") == state["next_cursor"] and rec.get("final_partial_body") == {
            "path": str(root / finish["body"]["path"]), "bytes": finish["body"]["bytes"], "sha256": finish["body"]["sha256"]}, "v5_drain_reconciliation_differs")
    check_v5_tree(value, inputs)
    return {"accepted_pages": count, "raw_rows": rows, "response_bytes_charged": charge, "body_files": call,
        "rejected_partial_bodies": 1, "inherited_exact_page_attempts": start["exact_page_attempt"],
        "fixed_prefix_adopted_by_reference": True}


def validate_pair(start, finish, call, page_index, spec, *, accepted):
    require(type(start) is dict and type(finish) is dict and start.get("event") == "START" and finish.get("event") == "FINISH"
        and all(type(item.get("call")) is int and item["call"] == call and item.get("stream") == STREAMS[2]["id"]
            and type(item.get("page_index")) is int and item["page_index"] == page_index
            and item.get("coverage_role") == "v4_continuation" for item in (start, finish))
        and type(start.get("exact_page_attempt")) is int and start["exact_page_attempt"] == 1
        and start.get("request") == spec and start.get("request_sha256") == finish.get("request_sha256") == digest(encode(spec))
        and type(start.get("response_byte_reservation")) is int and 0 < start["response_byte_reservation"] <= CAPS["individual_response_bytes"]
        and type(start.get("cumulative_http_attempt")) is int
        and all(type(finish.get(key)) is int and 0 <= finish[key] <= start["response_byte_reservation"] for key in ("response_bytes_observed", "response_bytes_charged"))
        and finish.get("accepted") is accepted and finish.get("transport_dispatched") is True
        and finish.get("interrupted") is False and finish.get("retry_delay_seconds") is None, "v5_attempt_pair_differs")
    if accepted:
        require(finish.get("error_code") is None and type(finish.get("status_code")) is int and finish["status_code"] == 200
            and finish.get("body_complete") is True and finish.get("response_close_completed") is True, "v5_accepted_attempt_differs")
    else:
        require(finish.get("error_code") == "response_byte_limit" and finish.get("body_complete") is False
            and type(finish.get("status_code")) is int and finish["status_code"] == 200
            and finish.get("response_close_completed") is True
            and finish.get("response_bytes_charged") == start["response_byte_reservation"], "v5_failed_attempt_differs")


def validate_usage(value, failure, inputs, admission_path, prefix_controls, minimum_runtime=None):
    require(type(value) is dict and set(value) == {"http_attempts", "response_bytes_charged", "runtime_seconds", "retained_bytes_charged", "root_controls"}, "prior_ledger_schema_v6")
    for key, cap in (("http_attempts", CAPS["http_attempts"]), ("response_bytes_charged", CAPS["total_response_bytes"]), ("runtime_seconds", CAPS["runtime_seconds"])):
        number(value[key], cap, integer=key != "runtime_seconds")
        reported = failure["usage"]["runtime_seconds_charged" if key == "runtime_seconds" else "http_attempts_charged" if key == "http_attempts" else key]
        require(value[key] >= max(PRIOR_MINIMUM[key], reported, minimum_runtime or 0 if key == "runtime_seconds" else 0), "prior_usage_below_floor")
    controls = value["root_controls"]
    require(type(controls) is list and len(controls) <= 256, "root_control_roster_invalid")
    paths = set(); control_bytes = 0
    for ref in controls:
        require(type(ref) is dict and set(ref) == {"path", "bytes", "sha256"}, "root_control_ref_schema")
        path = Path(ref["path"])
        require(path.parent == OPERATIONS and str(path) not in paths and path != admission_path
            and re.fullmatch(r"(?:full_metadata_v[456](?:[._][A-Za-z0-9_.-]+)?|full_acquisition_(?:root_admission_checks_v3|dispatch_v3\.(?:START|COMPLETE))|finalization_shutdown_drain_v2)\.(?:json|log)", path.name)
            and path.name not in {"START.json", "FAILURE.json", "manifest.json", "COMPLETE.json"}, "root_control_invalid_or_duplicate")
        inputs.bind(ref, MAX_CONTROL); paths.add(str(path)); control_bytes += ref["bytes"]
    require(control_bytes <= CAPS["external_root_controls_reservation_bytes"], "root_controls_missing_or_limit")
    amounts = value["retained_bytes_charged"]
    require(type(amounts) is dict and set(amounts) == set(RETAINED_MINIMUM), "prior_retained_schema")
    for key, cap in (("raw", CAPS["raw_body_bytes"]), ("control", CAPS["control_bytes"]), ("other", CAPS["other_metadata_bytes"])):
        number(amounts[key], cap)
        floor = max(RETAINED_MINIMUM[key], failure["usage"]["retained_bytes_charged"][key])
        if key == "control": floor += prefix_controls["failure"]["bytes"] + LATE_V5_CONTROLS + control_bytes
        require(amounts[key] >= floor, "prior_retained_floor_refunded")
    require(sum(amounts.values()) <= CAPS["aggregate_metadata_bytes"], "prior_byte_cap")
    return {"bytes": dict(amounts), "http_attempts": value["http_attempts"], "response_bytes_charged": value["response_bytes_charged"],
        "runtime_seconds": value["runtime_seconds"], "external_inventory": {"bytes": 0}, "root_control_bytes_additively_charged": control_bytes}


def diagnostic(exc, phase):
    result = previous.diagnostic(exc, phase)
    frame = exc.__traceback__; allowed = {str(REPO / name): name for name in SOURCE_FILES}
    result["code_frames"] = []
    while frame:
        code = frame.tb_frame.f_code
        if code.co_filename in allowed and re.fullmatch(r"[A-Za-z0-9_<>]{1,96}", code.co_name):
            result["code_frames"].append({"file": allowed[code.co_filename], "function": code.co_name, "line": frame.tb_lineno})
            result["code_frames"] = result["code_frames"][-8:]
        frame = frame.tb_next
    return result

class Ledger:
    def __init__(self, output, prior, admission_bytes, started):
        self.output, self.prior, self.started = output, prior, started
        self.used = {"raw": 0, "control": admission_bytes, "other": 0}
        self.files, self.hashers, self.bodies = {}, {}, []
        self.http_attempts = prior["http_attempts"]
        self.responses = prior["response_bytes_charged"]
        self.observed = 0

    def remaining_time(self):
        return CAPS["runtime_seconds"] - self.prior["runtime_seconds"] - (time.monotonic() - self.started)

    def check(self):
        # Compatible pure read_response helper calls this between bounded reads.
        self.reserve("control", 0)

    def reserve(self, category, amount, *, failure=False):
        require(type(amount) is int and amount >= 0, "write_reservation_invalid")
        if not failure:
            require(self.remaining_time() > 0, "runtime_limit")
        totals = {key: self.used[key] + self.prior["bytes"][key] for key in self.used}
        totals[category] += amount
        totals["control"] += CAPS["external_root_controls_reservation_bytes"]
        require(totals["raw"] <= CAPS["raw_body_bytes"] and
                totals["control"] + (0 if failure else CONTROL_RESERVE) <= CAPS["control_bytes"] and
                totals["other"] <= CAPS["other_metadata_bytes"] and
                sum(totals.values()) + (0 if failure else CONTROL_RESERVE) <= CAPS["aggregate_metadata_bytes"],
                "metadata_aggregate_or_subcap")
        require(shutil.disk_usage(self.output).free >= amount + CAPS["minimum_free_bytes"] +
                CAPS["external_root_controls_reservation_bytes"] + (0 if failure else CONTROL_RESERVE), "disk_floor")

    def write(self, relative, raw, category="control", *, failure=False):
        require(len(raw) <= (CAPS["individual_response_bytes"] + v4.GZIP_RESERVE if category == "raw" else MAX_CONTROL),
                "artifact_single_file_limit")
        self.reserve(category, len(raw), failure=failure)
        path = self.output / relative
        require(relative not in self.files and not path.exists() and not path.is_symlink(), "artifact_exists")
        path.parent.mkdir(exist_ok=True)
        self.used[category] += len(raw)
        bound = {"bytes": len(raw), "sha256": digest(raw), "category": category}
        self.files[relative] = bound
        with path.open("xb") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write")
            stream.flush(); os.fsync(stream.fileno())
        require(path.read_bytes() == raw, "artifact_reopen_differs")
        return {"path": relative, **bound}

    def json(self, relative, value, *, failure=False):
        return self.write(relative, encode(value), failure=failure)

    def append(self, relative, value, *, failure=False):
        raw = encode(value)
        require(len(raw) <= MAX_CONTROL, "control_record_limit")
        self.reserve("control", len(raw), failure=failure)
        path = self.output / relative
        if relative not in self.files:
            require(not path.exists() and not path.is_symlink(), "artifact_exists")
            self.files[relative] = {"bytes": 0, "sha256": digest(b""), "category": "control"}
            self.hashers[relative] = hashlib.sha256()
            with path.open("xb"):
                pass
        before = identity(path)
        require(before["bytes"] == self.files[relative]["bytes"], "control_size_changed")
        offset = before["bytes"]
        self.used["control"] += len(raw)
        self.hashers[relative].update(raw)
        self.files[relative] = {"bytes": offset + len(raw), "sha256": self.hashers[relative].hexdigest(), "category": "control"}
        with path.open("ab") as stream:
            require(stream.write(raw) == len(raw), "artifact_short_write")
            stream.flush(); os.fsync(stream.fileno())
        return {"path": relative, "offset": offset, "bytes": len(raw), "sha256": digest(raw)}

    def reopen(self, *, verify_bodies=True):
        actual = {str(path.relative_to(self.output)) for path in self.output.rglob("*") if path.is_file()}
        require(actual == set(self.files), "artifact_set_changed")
        for path in self.output.rglob("*"):
            require(not path.is_symlink() and (path.is_file() or path.is_dir()), "artifact_type_invalid")
        for relative, expected in self.files.items():
            bound = hash_file(self.output / relative, CAPS["aggregate_metadata_bytes"])
            require(all(bound[key] == expected[key] for key in ("bytes", "sha256")), "artifact_reopen_differs")
        if verify_bodies:
            for body in self.bodies:
                probe.verify_body(self.output, body)

    def usage(self, final_reservation=0):
        return {"http_attempts_charged": self.http_attempts, "response_bytes_charged": self.responses,
                "this_run_http_attempts": self.http_attempts - self.prior["http_attempts"],
                "this_run_response_bytes_charged": self.responses - self.prior["response_bytes_charged"],
                "this_run_runtime_seconds": time.monotonic() - self.started,
                "response_bytes_observed_this_run": self.observed,
                "retained_bytes_observed_before_final_controls":
                    {key: self.prior["bytes"][key] + self.used[key] for key in self.used},
                "retained_bytes_charged": {key: self.prior["bytes"][key] + self.used[key] +
                                           (final_reservation + CAPS["external_root_controls_reservation_bytes"]
                                            if key == "control" else 0) for key in self.used},
                "final_control_reservation_not_observed_bytes": final_reservation,
                "external_control_reservation_not_observed_bytes": CAPS["external_root_controls_reservation_bytes"],
                "runtime_seconds_charged": self.prior["runtime_seconds"] + time.monotonic() - self.started}

    def complete(self, value, source_check, input_check):
        self.json(".COMPLETE.pending", value)
        self.reopen()
        for directory in [self.output, self.output / "responses", self.output / "inventory"]:
            if directory.is_dir():
                fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try: os.fsync(fd)
                finally: os.close(fd)
        source_check()
        self.reopen()
        # A body/source verifier must not be the final observer of other bound
        # bytes. Finish with content-only reopens, never another body callback.
        input_check()
        self.reopen(verify_bodies=False)
        # Final external reopens can consume the remaining prior-inclusive
        # budget. No completion marker may cross that resource boundary.
        self.check()
        probe.atomic_completion(self.output / ".COMPLETE.pending", self.output / "COMPLETE.json")



def inventory(ledger):
    """Bounded deterministic segments; no single control limit is enlarged."""
    entries = sorted(ledger.files.items()); segments = []; pending = bytearray(); count = 0
    def seal():
        nonlocal pending, count
        require(len(segments) < INVENTORY_SEGMENTS, "inventory_segment_limit")
        ref = ledger.write(f"inventory/part{len(segments):05d}.jsonl", bytes(pending))
        segments.append({**ref, "record_count": count}); pending = bytearray(); count = 0
    for name, bound in entries:
        ledger.check(); raw = encode({"path": name, **bound})
        require(len(raw) <= 512, "inventory_record_limit")
        if pending and len(pending) + len(raw) > INVENTORY_SEGMENT_BYTES: seal()
        pending.extend(raw); count += 1
    if pending: seal()
    return ledger.json("artifact_inventory.json", {"schema_version": "polymarket_metadata_segmented_inventory_v6",
        "record_count": len(entries), "segment_bytes_limit": INVENTORY_SEGMENT_BYTES,
        "scope": "all_owned_artifacts_before_inventory_manifest_and_marker", "segments": segments})


def run_campaign(output, expected_head, admission_path, admission_sha, mode="full", *, transport=None, sleeper=time.sleep, debit_prior_runtime=None):
    began = time.monotonic(); started_utc = datetime.now(timezone.utc); phase = "admission"
    output, admission_path = Path(output), Path(admission_path)
    require(mode == "full", "full_only")
    require(output.is_absolute() and output.parent == RUN_ROOT and output.name == OUTPUT_NAME
        and output.parent.resolve() == output.parent and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    source = source_snapshot(expected_head)
    admission_ref = {"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}
    raw, admission_bound = read_bound(admission_ref); admission = probe.strict_json(raw)
    validate_admission(admission, expected_head, output)
    prior_time = number(admission["prior_ledger"].get("runtime_seconds"), CAPS["runtime_seconds"], integer=False)
    require(prior_time >= PRIOR_MINIMUM["runtime_seconds"], "prior_runtime_below_floor")
    def check(): require(prior_time + time.monotonic() - began < CAPS["runtime_seconds"], "runtime_limit")
    check()
    if debit_prior_runtime is not None: debit_prior_runtime(prior_time, began)
    inputs = Inputs(check); inputs.bind(admission_ref, v4.MAX_INPUT)
    # Fresh source identities join the final content reopens. Historical st_dev
    # is never compared with a current post-reboot physical identity.
    for name in SOURCE_FILES:
        inputs.bind(_ref(hash_file(REPO / name, v4.MAX_INPUT)), v4.MAX_INPUT)
    minimum_runtime = validate_approval(admission["approval"], inputs, started_utc)
    require(prior_time >= minimum_runtime, "preparation_runtime_floor_not_charged")
    phase = "parent_validation"
    old, failure_v4 = previous.validate_parent(admission["v4_prefix"], inputs, source, output)
    controls = validate_v5_parent(admission["v5_prefix"], admission["v4_prefix"], inputs, source, output)
    old_prior = previous.validate_usage(controls["admission"]["prior_ledger"], admission["v4_prefix"], old, failure_v4,
        inputs, Path(admission["v5_prefix"]["admission"]["path"]), Path(admission["v5_prefix"]["root"]))
    prior = validate_usage(admission["prior_ledger"], controls["failure"], inputs, admission_path, admission["v5_prefix"], minimum_runtime)
    phase = "prefix_replay"
    states, seen, replay_v4 = previous.replay(admission["v4_prefix"], inputs, old, failure_v4)
    replay_summary_v5 = replay_v5(admission["v5_prefix"], inputs, controls, old_prior, states, seen, replay_v4)
    replay_summary = {"v4": replay_v4, "v5": replay_summary_v5,
        "accepted_pages": replay_v4["accepted_pages"] + replay_summary_v5["accepted_pages"],
        "raw_rows": replay_v4["raw_rows"] + replay_summary_v5["raw_rows"],
        "all_accepted_bodies_replayed_before_http": True, "failed_partial_accepted": False}
    require(source_snapshot(expected_head) == source, "source_changed_before_dispatch")
    inputs.reopen(); previous.reopen_prefix_roster(inputs); check_v5_tree(admission["v5_prefix"], inputs); check()
    require(probe.available_ram() >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CONTROL_RESERVE + CAPS["external_root_controls_reservation_bytes"], "disk_floor")
    output.mkdir(mode=0o700); ledger = Ledger(output, prior, len(raw), began)
    waterfalls = Counter(); last_dispatch = -math.inf; transport = transport or v4.public_transport
    try:
        ledger.json("START.json", {"schema_version": START_SCHEMA, "mode": "full", "source": source, "admission": admission_bound,
            "caps": CAPS, "scope": SCOPE, "adopted_prefixes": {key: admission[key] for key in ("v4_prefix", "v5_prefix")}, "replay": replay_summary,
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
                phase = "request_prepare"; spec = v4.request_spec(stream, cursor)
                parsed = summary = None
                first_attempt = replay_summary_v5["inherited_exact_page_attempts"] + 1 if index == 2 and page_index == controls["failure"]["streams"][2]["accepted_pages"] else 1
                for exact_attempt in range(first_attempt, CAPS["attempts_per_exact_page"] + 1):
                    require(ledger.http_attempts < CAPS["http_attempts"], "http_attempt_limit")
                    require(ledger.responses < CAPS["total_response_bytes"], "total_response_byte_limit")
                    phase = "source_check"; require(source_snapshot(expected_head) == source, "source_changed")
                    allowance = min(CAPS["individual_response_bytes"], CAPS["total_response_bytes"] - ledger.responses)
                    ledger.reserve("raw", allowance + v4.GZIP_RESERVE); ledger.reserve("control", MAX_CONTROL)
                    phase = "spacing_wait"; wait_until(last_dispatch + .5, ledger, sleeper)
                    ledger.http_attempts += 1; call = ledger.http_attempts - prior["http_attempts"]
                    phase = "attempt_start"
                    start = {"event": "START", "call": call, "cumulative_http_attempt": ledger.http_attempts,
                        "stream": stream["id"], "page_index": page_index, "coverage_role": "v6_budget_continuation",
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
                                "coverage_role": "v6_budget_continuation", "requested_cursor": cursor, "accepted": True, "body": observed["body"],
                                **{key: val for key, val in parsed.items() if key != "rows"}, "row_summary": legacy.compact_summary(summary)}
                            ledger.append("pages.jsonl", page); check_attempt()
                            ledger.append("attempts.jsonl", {"event": "FINISH", "call": call, "stream": stream["id"], "page_index": page_index,
                                "coverage_role": "v6_budget_continuation", "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"],
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
                            "stream": stream["id"], "page_index": page_index, "coverage_role": "v6_budget_continuation",
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
        require(sum(row["raw_rows"] for row in states) == replay_summary["raw_rows"] + waterfalls["raw_rows_in_accepted_pages"], "count_conservation_failed")
        def recheck():
            require(source_snapshot(expected_head) == source and read_bound(admission_ref)[1] == admission_bound, "source_or_admission_changed")
            require(v4.inventory_prior(probe.strict_json(inputs.bind(admission["v4_prefix"]["admission"], v4.MAX_INPUT))["prior_ledger"],
                Path(admission["v4_prefix"]["admission"]["path"]), Path(previous.PIN["root"])) == old, "original_prior_changed")
            inputs.reopen(); previous.reopen_prefix_roster(inputs); check_v5_tree(admission["v5_prefix"], inputs); ledger.check()
        def input_check():
            inputs.reopen(); previous.reopen_prefix_roster(inputs); check_v5_tree(admission["v5_prefix"], inputs); ledger.check()
        phase = "final_reopen"; ledger.reopen(); recheck()
        inventory_bound = inventory(ledger)
        manifest = {"schema_version": START_SCHEMA, "mode": "full", "status": SUCCESS, "source": source,
            "admission": admission_bound, "caps": CAPS, "scope": SCOPE, "adopted_prefixes": {key: admission[key] for key in ("v4_prefix", "v5_prefix")}, "replay": replay_summary,
            "streams": states, "waterfall_new_run": dict(waterfalls), "usage": ledger.usage(final_reservation=CONTROL_RESERVE),
            "artifact_inventory": inventory_bound, "universe_complete": False, "analysis_admitted": False,
            "family_or_payout_admitted": False, "snapshot_consistency_certified": False, "normalized_labels_emitted": False}
        phase = "publication"; manifest_bound = ledger.json("manifest.json", manifest)
        ledger.complete({"schema_version": "polymarket_market_metadata_campaign_completion_v6", "mode": "full", "status": SUCCESS,
            "manifest": manifest_bound, "universe_complete": False, "analysis_admitted": False}, recheck, input_check)
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
