#!/usr/bin/env python3
"""Fresh, root-admitted public RAW metadata campaign. No normalization/admission.

CLI: --admission JSON --admission-sha256 SHA --expected-head HEAD --run-dir DIR.
Admission schema is returned by admission_contract(); no endpoint/cap/resume
overrides exist. Producer probe HEAD and consumer HEAD are deliberately distinct.
All preceding metadata artifacts and resource charges must be explicitly bound.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
from scripts import collect_polymarket_raw_metadata_probe_v2 as probe

CAPS = {"http_attempts": 40_000, "attempts_per_exact_page": 3,
        "individual_response_bytes": 4 * 1024**2, "total_response_bytes": 32 * 1024**3,
        "aggregate_metadata_bytes": 8_000_000_000, "raw_body_bytes": 4_000_000_000,
        "control_bytes": 500_000_000, "other_metadata_bytes": 3_500_000_000,
        "runtime_seconds": 86_400, "address_space_bytes": 8 * 1024**3,
        "minimum_available_ram_bytes": 8 * 1024**3, "minimum_free_bytes": 20_000_000_000,
        "spill_bytes": 0, "requests_per_second": 2, "maximum_retry_after_seconds": 60,
        "external_root_controls_reservation_bytes": 2 * 1024**2}
GAMMA_LIMIT = 100
GAMMA_SCHEMA_URI = "https://gamma-api.polymarket.com/schemas/MarketsKeysetListResponse.json"
STREAMS = tuple(dict(stream) for stream in probe.STREAMS)
SCOPE = {"streams": STREAMS, "gamma_limit": GAMMA_LIMIT, "gamma_include_tag": True,
         "gamma_order": "id", "gamma_ascending": True, "gamma_cursor_parameter": "after_cursor",
         "gamma_optional_schema_uri": GAMMA_SCHEMA_URI,
         "clob_initial_cursor": "MA==", "clob_terminal_cursor": "LTE=",
         "raw_rows_only": True, "resume_permitted": False}
MAX_INPUT = 1024**2
MAX_CONTROL = 16 * 1024**2
CONTROL_RESERVE = 2 * 1024**2
GZIP_RESERVE = 65536
SOURCE_FILES = tuple(dict.fromkeys((*probe.SOURCE_FILES,
    "scripts/collect_polymarket_market_metadata_campaign_v3.py",
    "tests/test_polymarket_market_metadata_campaign_v3.py")))
HEADERS = {"Accept": "application/json", "Accept-Encoding": "identity",
           "User-Agent": "prediction-markets-raw-metadata-campaign/3"}
SCHEMA, SUCCESS = "polymarket_market_metadata_campaign_admission_v3", "requested_metadata_pagination_complete_not_universe_admission"
RUN_ROOT = Path("/mnt/data/runs")
Error = probe.ProbeError
require, digest, encode = probe.require, probe.digest, probe.json_bytes


def admission_contract():
    """Machine-readable fixed launch contract, not permission to collect."""
    return {"schema_version": SCHEMA, "status": "root_metadata_campaign_admitted",
            "run_dir": "literal fresh absolute /mnt/data/runs child",
            "consumer_head": "40 lowercase hex", "caps": CAPS, "scope_sha256": digest(encode(SCOPE)),
            "approved_probe": {"schema_continuation_accepted": True, "producer_head": "40 lowercase hex",
                "producer_blobs": "exact probe.SOURCE_FILES map of bytes/sha256",
                "complete": "absolute path/bytes/sha256", "manifest": "absolute path/bytes/sha256"},
            "prior_ledger": {"all_prior_metadata_roots_included": True,
                "roots": "literal nonoverlapping immutable run directories",
                "standalone_control_files": "literal immutable controls from otherwise mixed/growing roots",
                "files": "absolute path/bytes/sha256/category (raw/control/other) entries",
                "http_attempts": "all preceding metadata HTTP attempts",
                "response_bytes_charged": "all preceding conservative response charges",
                "runtime_seconds": "all preceding charged metadata runtime"}}


def identity(path):
    info = Path(path).lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and not Path(path).is_symlink(),
            "input_not_single_link_regular")
    return {"device": info.st_dev, "inode": info.st_ino, "bytes": info.st_size,
            "mode": info.st_mode, "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def hash_file(path, maximum):
    before = identity(path)
    require(before["bytes"] <= maximum, "input_file_byte_limit")
    value = hashlib.sha256()
    observed = 0
    with Path(path).open("rb") as stream:
        while raw := stream.read(min(1024**2, maximum - observed + 1)):
            observed += len(raw)
            require(observed <= maximum, "input_file_byte_limit")
            value.update(raw)
    require(identity(path) == before, "input_changed_during_read")
    return {"path": str(path), "bytes": before["bytes"], "sha256": value.hexdigest(), "identity": before}


def read_bound(reference, maximum=MAX_INPUT):
    require(type(reference) is dict and set(reference) == {"path", "bytes", "sha256"}, "reference_schema")
    path = Path(reference["path"])
    require(path.is_absolute() and path.resolve() == path and type(reference["bytes"]) is int
            and 0 <= reference["bytes"] <= maximum and
            re.fullmatch(r"[0-9a-f]{64}", reference["sha256"] or ""), "reference_invalid")
    bound = hash_file(path, maximum)
    require(all(bound[key] == reference[key] for key in reference), "reference_bytes_or_hash_differs")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "input_file_byte_limit")
    require(identity(path) == bound["identity"] and digest(raw) == bound["sha256"], "input_reopen_differs")
    return raw, bound


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "invalid_expected_head")
    def git(*args, binary=False):
        try:
            raw = subprocess.check_output(["git", *args], cwd=REPO, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            raise Error("source_git_unavailable") from None
        return raw if binary else raw.decode().strip()
    require(git("rev-parse", "HEAD") == expected_head, "source_head_differs")
    require(not git("status", "--porcelain", "--untracked-files=normal"), "source_not_clean")
    files = {}
    for name in SOURCE_FILES:
        path = REPO / name
        bound = hash_file(path, MAX_INPUT)
        size = git("cat-file", "-s", expected_head + ":" + name)
        require(size.isdecimal() and int(size) == bound["bytes"], "committed_source_size_differs")
        committed = git("cat-file", "blob", expected_head + ":" + name, binary=True)
        require(len(committed) <= MAX_INPUT and len(committed) == bound["bytes"] and
                digest(committed) == bound["sha256"], "source_bytes_differ_from_commit")
        files[name] = {key: bound[key] for key in ("bytes", "sha256")}
    return {"head": expected_head, "files": files}


def number(value, maximum, *, integer=True):
    require((type(value) is int if integer else type(value) in (int, float)) and
            math.isfinite(value) and 0 <= value <= maximum, "prior_counter_invalid")
    return value


def inventory_prior(value, admission_path, output):
    require(type(value) is dict and set(value) == {"all_prior_metadata_roots_included", "roots", "standalone_control_files", "files",
            "http_attempts", "response_bytes_charged", "runtime_seconds"} and
            value["all_prior_metadata_roots_included"] is True, "prior_ledger_schema")
    roots, files, standalone = value["roots"], value["files"], value["standalone_control_files"]
    require(type(roots) is list and 1 <= len(roots) <= 256 and type(files) is list and
            1 <= len(files) <= 10_000 and type(standalone) is list and len(standalone) <= 10_000, "prior_roster_size")
    directories, actual_paths = [], set()
    for name in roots:
        require(type(name) is str, "prior_root_invalid")
        root = Path(name)
        require(root.is_absolute() and root.resolve() == root and root.is_dir() and not root.is_symlink()
                and RUN_ROOT in root.parents and root != output and root not in output.parents
                and output not in root.parents, "prior_root_invalid")
        require(all(root != prior and root not in prior.parents and prior not in root.parents
                    for prior in directories), "overlapping_prior_roots")
        directories.append(root)
        for path in root.rglob("*"):
            require(not path.is_symlink(), "prior_tree_symlink")
            if path.is_file():
                actual_paths.add(str(path))
                require(len(actual_paths) <= 10_000, "prior_roster_size")
            else:
                require(path.is_dir(), "prior_tree_file_type")
    standalone_paths = set()
    for name in standalone:
        require(type(name) is str, "standalone_control_invalid")
        path = Path(name)
        require(path.is_absolute() and path.resolve() == path and RUN_ROOT in path.parents
                and name not in actual_paths and name not in standalone_paths and path != admission_path
                and path != output and output not in path.parents and path not in output.parents
                and not path.name.endswith((".body", ".body.gz")) and "responses" not in path.parts
                and all(root not in path.parents and path != root for root in directories),
                "standalone_control_overlap_or_invalid")
        identity(path)
        standalone_paths.add(name)
    actual_paths |= standalone_paths
    observed, bindings = set(), []
    totals = {"raw": 0, "control": 0, "other": 0}
    for item in files:
        require(type(item) is dict and set(item) == {"path", "bytes", "sha256", "category"}
                and item["category"] in totals, "prior_file_schema")
        require(item["path"] not in observed and item["path"] in actual_paths
                and Path(item["path"]) != admission_path, "prior_file_missing_duplicate_or_self")
        require(item["path"] not in standalone_paths or item["category"] == "control",
                "standalone_control_misclassified")
        observed.add(item["path"])
        bound = hash_file(Path(item["path"]), CAPS["aggregate_metadata_bytes"])
        require(all(item[key] == bound[key] for key in ("path", "bytes", "sha256")), "prior_file_binding_differs")
        # A response body cannot be reclassified to the reserved 'other' pool.
        item_path = Path(item["path"])
        in_response_tree = any(root in item_path.parents and "responses" in item_path.relative_to(root).parts[:-1]
                               for root in directories)
        if item_path.name.endswith((".body", ".body.gz")) or in_response_tree:
            require(item["category"] == "raw", "prior_raw_body_misclassified")
        totals[item["category"]] += bound["bytes"]
        bindings.append(bound | {"category": item["category"]})
    require(observed == actual_paths, "prior_roster_not_complete")
    require(totals["raw"] <= CAPS["raw_body_bytes"] and totals["control"] <= CAPS["control_bytes"]
            and totals["other"] <= CAPS["other_metadata_bytes"] and
            sum(totals.values()) <= CAPS["aggregate_metadata_bytes"], "prior_byte_cap")
    prior = {"roots": roots, "standalone_control_files": standalone, "files": bindings, "bytes": totals,
            "http_attempts": number(value["http_attempts"], CAPS["http_attempts"]),
            "response_bytes_charged": number(value["response_bytes_charged"], CAPS["total_response_bytes"]),
            "runtime_seconds": number(value["runtime_seconds"], CAPS["runtime_seconds"], integer=False)}
    minima = {"http_attempts": 0, "response_bytes_charged": 0, "runtime_seconds": 0.0}
    by_path = {item["path"]: item for item in bindings}
    paths = set(by_path)
    for root in directories:
        local = dict.fromkeys(minima, 0)
        recognized = False
        start_path = root / "START.json"
        start_schema = None
        if str(start_path) in paths:
            start = probe.strict_json(read_bound({key: by_path[str(start_path)][key]
                for key in ("path", "bytes", "sha256")})[0])
            require(type(start) is dict, "prior_start_schema")
            start_schema = start.get("schema_version")
        known_collectors = {"polymarket_market_metadata_pilot_v1", "polymarket_raw_metadata_probe_v2",
                            "polymarket_market_metadata_campaign_v3"}
        for name in ("manifest.json", "FAILURE.json", "controls/RESULT.json"):
            path = root / name
            if str(path) not in paths:
                continue
            summary = probe.strict_json(read_bound({key: by_path[str(path)][key]
                for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
            require(type(summary) is dict, "prior_summary_schema")
            schema = summary.get("schema_version")
            if name == "controls/RESULT.json":
                require(schema == "polymarket_v3_source_lookup_result_v1" and
                        summary.get("status") in {"blocked", "bounded_sourcify_raw_lookup_complete_not_source_equivalence"},
                        "prior_summary_format_unrecognized")
                attempts = summary.get("attempts")
                require(type(attempts) is list and len(attempts) <= 4, "prior_lookup_attempts_invalid")
                charges = 0
                for index, attempt in enumerate(attempts):
                    require(type(attempt) is dict and attempt.get("id") == index + 1,
                            "prior_lookup_attempts_invalid")
                    charges += number(attempt.get("charged_response_bytes"), 8 * 1024**2)
                require(number(summary.get("http_attempts"), 4) >= len(attempts) and
                        number(summary.get("response_bytes_charged"), 32 * 1024**2) >= charges,
                        "prior_lookup_summary_below_attempts")
            elif name == "manifest.json":
                require(schema in known_collectors and (start_schema is None or start_schema == schema),
                        "prior_summary_format_unrecognized")
            else:
                require(summary.get("status") == "failed_unaccepted" and start_schema in known_collectors,
                        "prior_summary_format_unrecognized")
            recognized = True
            usage = summary.get("usage", {})
            if type(usage) is dict and "this_run_http_attempts" in usage:
                candidates = {"http_attempts": usage["this_run_http_attempts"],
                              "response_bytes_charged": usage.get("this_run_response_bytes_charged"),
                              "runtime_seconds": usage.get("this_run_runtime_seconds", 0)}
            elif "http_attempts" in summary:
                candidates = {"http_attempts": summary["http_attempts"],
                              "response_bytes_charged": summary.get("response_bytes_charged"),
                              "runtime_seconds": summary.get("elapsed_seconds", 0)}
            else:
                raise Error("prior_summary_usage_missing")
            for key, candidate in candidates.items():
                ceiling = CAPS["http_attempts" if key == "http_attempts" else
                               "total_response_bytes" if key == "response_bytes_charged" else "runtime_seconds"]
                local[key] = max(local[key], number(candidate, ceiling, integer=key != "runtime_seconds"))
        attempt_evidence = any(Path(path).relative_to(root).parts[0] == "attempts" or
            Path(path).name in {"attempts.jsonl", "REQUESTS.jsonl"} or
            Path(path).name.endswith((".START.json", ".FINISH.json"))
            for path in paths if root in Path(path).parents)
        require(recognized or (not attempt_evidence and start_schema not in known_collectors),
                "prior_collection_summary_missing")
        for key in minima:
            minima[key] += local[key]
    require(all(prior[key] >= minima[key] for key in minima), "prior_usage_below_durable_minima")
    prior["durable_usage_minima"] = minima
    return prior


def verify_probe(value, source, prior):
    require(type(value) is dict and set(value) == {"schema_continuation_accepted", "producer_head",
            "producer_blobs", "complete", "manifest"} and value["schema_continuation_accepted"] is True,
            "approved_probe_schema")
    require(re.fullmatch(r"[0-9a-f]{40}", value["producer_head"] or ""), "probe_producer_head_invalid")
    expected_blobs = {name: source["files"][name] for name in probe.SOURCE_FILES}
    require(value["producer_blobs"] == expected_blobs, "probe_consumer_helper_bytes_differ")
    for name, bound in expected_blobs.items():
        try:
            size = subprocess.check_output(["git", "cat-file", "-s", value["producer_head"] + ":" + name],
                                           cwd=REPO, stderr=subprocess.DEVNULL, timeout=10).decode().strip()
            require(size.isdecimal() and int(size) == bound["bytes"], "probe_producer_blob_differs")
            raw = subprocess.check_output(["git", "cat-file", "blob", value["producer_head"] + ":" + name],
                                          cwd=REPO, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            raise Error("probe_producer_blob_unavailable") from None
        require(len(raw) == bound["bytes"] and digest(raw) == bound["sha256"], "probe_producer_blob_differs")
    marker_raw, marker_bound = read_bound(value["complete"])
    manifest_raw, manifest_bound = read_bound(value["manifest"])
    marker, manifest = probe.strict_json(marker_raw), probe.strict_json(manifest_raw)
    root = Path(marker_bound["path"]).parent
    require(root in [Path(path) for path in prior["roots"]] and
            Path(manifest_bound["path"]) == root / "manifest.json" and
            Path(marker_bound["path"]) == root / "COMPLETE.json", "probe_paths_not_prior_root")
    require(marker.get("schema_version") == "polymarket_raw_metadata_probe_completion_v2"
            and marker.get("manifest") == {"path": "manifest.json", "bytes": len(manifest_raw), "sha256": digest(manifest_raw)}
            and marker.get("status") == manifest.get("status")
            and marker.get("universe_complete") is False and marker.get("analysis_admitted") is False,
            "probe_completion_differs")
    require(manifest.get("schema_version") == "polymarket_raw_metadata_probe_v2"
            and manifest.get("status") in {"probe_complete_terminal_cursors_observed", "probe_complete_universe_incomplete"}
            and manifest.get("source") == {"head": value["producer_head"], "files": expected_blobs}
            and manifest.get("caps") == probe.CAPS and manifest.get("universe_complete") is False
            and manifest.get("analysis_admitted") is False and manifest.get("family_or_payout_admitted") is False,
            "probe_contract_differs")
    artifacts = manifest.get("artifacts")
    require(type(artifacts) is dict and len(artifacts) <= 128, "probe_artifact_inventory_invalid")
    prior_by_path = {item["path"]: item for item in prior["files"]}
    require({str(path) for path in root.rglob("*") if path.is_file()} ==
            {str(root / name) for name in artifacts} | {str(root / "manifest.json"), str(root / "COMPLETE.json")},
            "probe_artifact_set_differs")
    for name, bound in artifacts.items():
        relative = Path(name)
        require(not relative.is_absolute() and ".." not in relative.parts and type(bound) is dict
                and set(bound) == {"bytes", "sha256"}, "probe_artifact_reference_invalid")
        full = str(root / name)
        require(full in prior_by_path and all(prior_by_path[full][key] == bound[key] for key in bound),
                "probe_artifact_binding_differs")
    require(sum(bound["bytes"] for bound in artifacts.values()) + len(marker_raw) + len(manifest_raw)
            <= probe.CAPS["all_artifact_bytes"], "probe_artifact_byte_limit")
    def probe_json(name, maximum=probe.MAX_CONTROL):
        require(name in artifacts, "probe_artifact_reference_not_admitted")
        return probe.strict_json(read_bound({"path": str(root / name), **artifacts[name]}, maximum)[0])
    start = probe_json("START.json")
    require(start.get("source") == manifest["source"] and start.get("caps") == probe.CAPS
            and start.get("streams") == list(probe.STREAMS) and start.get("gamma_limit") == 10,
            "probe_start_contract_differs")
    streams = manifest.get("streams")
    require(type(streams) is list and len(streams) == 5 and
            [item.get("id") for item in streams] == [item["id"] for item in STREAMS], "probe_stream_inventory_differs")
    accepted_attempts, rows = set(), 0
    for stream, saved in zip(STREAMS, streams):
        require(saved.get("provider") == stream["provider"] and type(saved.get("pages")) is list
                and 1 <= len(saved["pages"]) <= 2, "probe_stream_pages_invalid")
        cursor, seen_cursors, seen_rows, verified = ("MA==" if stream["provider"] == "clob" else None), set(), set(), True
        for index, page in enumerate(saved["pages"]):
            attempt = page["attempt"]
            require(type(attempt) is int and attempt not in accepted_attempts, "probe_attempt_reused")
            accepted_attempts.add(attempt)
            attempt_start = probe_json(f"attempts/a{attempt:02d}.START.json")
            finish = probe_json(f"attempts/a{attempt:02d}.FINISH.json")
            spec = probe.request_spec(stream, cursor)
            require(attempt_start.get("request") == spec and attempt_start.get("page_index") == index
                    and attempt_start.get("request_sha256") == digest(encode(spec))
                    and finish.get("request_sha256") == digest(encode(spec))
                    and finish.get("result") == "page_validated" and finish.get("error_code") is None
                    and finish.get("status_code") == 200 and finish.get("body_complete") is True
                    and finish.get("transport_dispatched") is True and page.get("request_sha256") == digest(encode(spec)),
                    "probe_request_or_finish_differs")
            body = page["body"]
            require(type(body) is dict and body.get("path") in artifacts and
                    all(body.get(key) == artifacts[body["path"]][key] for key in ("bytes", "sha256"))
                    and type(body.get("uncompressed_bytes")) is int
                    and 0 <= body["uncompressed_bytes"] <= CAPS["individual_response_bytes"]
                    and finish.get("body") == body,
                    "probe_body_reference_not_admitted")
            probe.verify_body(root, body)
            import gzip
            with gzip.open(root / body["path"], "rb") as handle:
                raw = handle.read(CAPS["individual_response_bytes"] + 1)
            require(len(raw) <= CAPS["individual_response_bytes"], "probe_body_expansion_limit")
            parsed = probe.page_envelope(raw, stream, cursor, seen_cursors, seen_rows)
            summary = probe.inspect_rows(parsed["rows"], stream, index)
            require(not summary["scope_conflict_counts"] and page["row_summary"] ==
                    {key: val for key, val in summary.items() if key != "records"}, "probe_page_summary_differs")
            require(all(page[key] == parsed[key] for key in parsed if key != "rows"), "probe_cursor_or_count_differs")
            require(finish.get("page") == {key: val for key, val in page.items()
                    if key not in {"attempt", "request_sha256", "body"}}, "probe_finish_page_differs")
            row_bound = page["row_index"]
            require(type(row_bound) is dict and row_bound.get("path") in artifacts and
                    all(row_bound.get(key) == artifacts[row_bound["path"]][key] for key in ("bytes", "sha256")),
                    "probe_row_reference_not_admitted")
            require(probe_json(row_bound["path"], probe.MAX_ROW_INDEX) == summary, "probe_row_index_differs")
            rows += parsed["count"]
            if stream["provider"] == "gamma":
                verified = verified and summary["returned_scope_fields_verified"]
            cursor = parsed["next_cursor"]
            seen_cursors.add(cursor)
            seen_rows.add(parsed["rows_sha256"])
            require(not parsed["pagination_terminal_observed"] or index == len(saved["pages"]) - 1,
                    "probe_pages_after_terminal")
        require(saved.get("pagination_terminal_observed") == parsed["pagination_terminal_observed"]
                and saved.get("next_cursor") == cursor and
                (parsed["pagination_terminal_observed"] or len(saved["pages"]) == 2), "probe_continuation_not_proven")
        if stream["provider"] == "gamma":
            require(saved.get("returned_scope_fields_verified") == verified, "probe_scope_proof_differs")
    count = manifest.get("http_attempts")
    require(type(count) is int and 1 <= count <= probe.CAPS["http_attempts"]
            and manifest.get("successful_pages") == len(accepted_attempts)
            and manifest.get("raw_rows_in_validated_pages") == rows, "probe_count_conservation_differs")
    for attempt in range(1, count + 1):
        require(f"attempts/a{attempt:02d}.START.json" in artifacts and
                f"attempts/a{attempt:02d}.FINISH.json" in artifacts, "probe_attempt_receipt_missing")
    require(prior["http_attempts"] >= count and prior["response_bytes_charged"] >=
            number(manifest.get("response_bytes_charged"), probe.CAPS["total_response_bytes"])
            and prior["runtime_seconds"] >= number(manifest.get("elapsed_seconds"),
                                                   probe.CAPS["runtime_seconds"], integer=False),
            "prior_ledger_omits_accepted_probe_usage")
    return {"complete": marker_bound, "manifest": manifest_bound,
            "producer_head": value["producer_head"], "producer_blobs": expected_blobs}


def request_spec(stream, cursor):
    parameters = {"next_cursor": cursor} if stream["provider"] == "clob" else {
        "version": stream["version"], "closed": str(stream["closed"]).lower(), "limit": "100",
        "include_tag": "true", "order": "id", "ascending": "true"}
    if stream["provider"] == "gamma" and cursor is not None:
        parameters["after_cursor"] = cursor
    return {"method": "GET", "url": stream["endpoint"] + "?" + parse.urlencode(parameters),
            "headers": HEADERS, "stream": stream["id"], "parameters": parameters}


def page_envelope(raw, stream, requested_cursor, seen_cursors, seen_rows):
    body = probe.strict_json(raw)
    if stream["provider"] == "clob":
        require(type(body) is dict and set(body) == {"limit", "count", "next_cursor", "data"}, "page_envelope_schema")
        require(type(body["limit"]) is int and body["limit"] > 0 and type(body["count"]) is int
                and type(body["data"]) is list and 0 <= body["count"] == len(body["data"]) <= body["limit"],
                "page_count_or_data_schema")
        rows, cursor = body["data"], probe.cursor_value(body["next_cursor"], "clob")
        terminal = cursor == "LTE="
        require(rows or terminal, "empty_nonterminal_page")
    else:
        require(type(body) is dict and set(body) - {"$schema"} in ({"markets"}, {"markets", "next_cursor"}),
                "page_envelope_schema")
        require("$schema" not in body or body["$schema"] == GAMMA_SCHEMA_URI, "gamma_schema_uri_differs")
        require(type(body["markets"]) is list and len(body["markets"]) <= GAMMA_LIMIT, "page_count_or_data_schema")
        rows, terminal = body["markets"], "next_cursor" not in body
        if terminal:
            require(len(rows) < GAMMA_LIMIT, "gamma_full_page_missing_cursor")
            cursor = None
        else:
            cursor = probe.cursor_value(body["next_cursor"], "gamma")
            require(len(rows) == GAMMA_LIMIT, "gamma_short_page_has_cursor")
    require(terminal or (cursor != requested_cursor and cursor not in seen_cursors), "repeated_cursor")
    rows_sha = digest(encode(rows))
    require(not rows or rows_sha not in seen_rows, "repeated_page_rows")
    return {"rows": rows, "count": len(rows), "next_cursor": cursor,
            "rows_sha256": rows_sha, "pagination_terminal_observed": terminal}


def compact_summary(summary):
    codes = sorted(summary["identity_issue_counts"])
    require(len(codes) <= 63, "identity_issue_code_limit")
    bits = {code: 1 << index for index, code in enumerate(codes)}
    indexed = [[index, sum(bits[code] for code in set(row["identity_issues"]))]
               for index, row in enumerate(summary["records"]) if row["identity_issues"]]
    return {**{key: value for key, value in summary.items() if key != "records"},
            "identity_issue_bit_order": codes, "issue_rows": indexed,
            "all_row_locators": "{stream}/p{page_index:02d}/r{ordinal:06d}; every ordinal in [0,raw_row_count)"}


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
        require(len(raw) <= (CAPS["individual_response_bytes"] + GZIP_RESERVE if category == "raw" else MAX_CONTROL),
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

    def reopen(self):
        actual = {str(path.relative_to(self.output)) for path in self.output.rglob("*") if path.is_file()}
        require(actual == set(self.files), "artifact_set_changed")
        for path in self.output.rglob("*"):
            require(not path.is_symlink() and (path.is_file() or path.is_dir()), "artifact_type_invalid")
        for relative, expected in self.files.items():
            bound = hash_file(self.output / relative, CAPS["aggregate_metadata_bytes"])
            require(all(bound[key] == expected[key] for key in ("bytes", "sha256")), "artifact_reopen_differs")
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

    def complete(self, value, source_check):
        self.json(".COMPLETE.pending", value)
        self.reopen()
        for directory in [self.output, self.output / "responses"]:
            if directory.is_dir():
                fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
                try: os.fsync(fd)
                finally: os.close(fd)
        source_check()
        self.reopen()
        # Final external reopens can consume the remaining prior-inclusive
        # budget. No completion marker may cross that resource boundary.
        self.check()
        probe.atomic_completion(self.output / ".COMPLETE.pending", self.output / "COMPLETE.json")


def public_transport(spec, timeout):
    opener = request.build_opener(request.ProxyHandler({}), probe.NoRedirect())
    try:
        return opener.open(request.Request(spec["url"], headers=HEADERS, method="GET"), timeout=timeout)
    except error.HTTPError as response:
        return response


def retry_delay(value, now=None):
    if value is None:
        return 1.0
    require(type(value) is str and 0 < len(value) <= 128, "retry_after_invalid")
    if re.fullmatch(r"[0-9]+", value):
        seconds = int(value)
    else:
        try:
            date = parsedate_to_datetime(value)
            require(date.tzinfo is not None, "retry_after_invalid")
            seconds = max(0.0, date.timestamp() - (time.time() if now is None else now))
        except (TypeError, ValueError, OverflowError):
            raise Error("retry_after_invalid") from None
    require(seconds <= CAPS["maximum_retry_after_seconds"], "retry_after_exceeds_bound")
    return float(seconds)


def wait_until(when, ledger, sleeper):
    while time.monotonic() < when:
        remaining = when - time.monotonic()
        require(ledger.remaining_time() > remaining, "runtime_limit")
        sleeper(min(.25, remaining))


def read_response(response, allowance, ledger):
    """Bounded read retaining every returned prefix, including on interruption."""
    raw = bytearray()
    try:
        header = response.headers.get("Content-Length")
        length = None if header is None else int(header)
        require(header is None or (str(length) == header and length >= 0), "invalid_content_length")
        if length is not None and length > allowance:
            return b"", "response_byte_limit", allowance, False
        code = None
        while len(raw) < allowance:
            ledger.check()
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
        return bytes(raw), "incomplete_response", allowance, False
    except Error as exc:
        return bytes(raw), str(exc), allowance, False
    except BaseException as exc:
        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit))
        return bytes(raw), "interrupted" if interrupted else "response_transport_error", allowance, interrupted
    return bytes(raw), code, len(raw), False


def run_campaign(output, expected_head, admission_path, admission_sha, *, transport=None, sleeper=time.sleep,
                 debit_prior_runtime=None):
    started = time.monotonic()
    output, admission_path = Path(output), Path(admission_path)
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.is_dir() and
            output.parent.resolve() == output.parent and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    reference = {"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}
    admission_raw, admission_bound = read_bound(reference)
    admission = probe.strict_json(admission_raw)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "run_dir", "consumer_head",
            "caps", "scope_sha256", "approved_probe", "prior_ledger"} and admission["schema_version"] == SCHEMA
            and admission["status"] == "root_metadata_campaign_admitted" and admission["run_dir"] == str(output)
            and admission["consumer_head"] == expected_head and admission["caps"] == CAPS
            and admission["scope_sha256"] == digest(encode(SCOPE)), "root_admission_not_exact")
    source = source_snapshot(expected_head)
    prior = inventory_prior(admission["prior_ledger"], admission_path, output)
    require(prior["runtime_seconds"] + time.monotonic() - started < CAPS["runtime_seconds"], "runtime_limit")
    if debit_prior_runtime is not None:
        debit_prior_runtime(prior["runtime_seconds"], started)
    approved = verify_probe(admission["approved_probe"], source, prior)
    require(probe.available_ram() >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CONTROL_RESERVE +
            CAPS["external_root_controls_reservation_bytes"] + CAPS["individual_response_bytes"] + GZIP_RESERVE, "disk_floor")
    output.mkdir(mode=0o700)
    ledger = Ledger(output, prior, len(admission_raw), started)
    transport = transport or public_transport
    states, waterfalls = [], Counter()
    last_dispatch = -math.inf
    try:
        ledger.json("START.json", {"schema_version": "polymarket_market_metadata_campaign_v3",
                    "source": source, "admission": admission_bound, "approved_probe": approved,
                    "caps": CAPS, "scope": SCOPE, "started_utc": probe.utc_now(),
                    "prior_bytes": prior["bytes"], "all_admission_inputs_reopened": True,
                    "universe_complete": False, "analysis_admitted": False})
        for stream in STREAMS:
            cursor = "MA==" if stream["provider"] == "clob" else None
            page_index, seen_cursors, seen_rows, scope_verified = 0, {cursor}, set(), True
            state = {"id": stream["id"], "provider": stream["provider"], "accepted_pages": 0,
                     "raw_rows": 0, "identity_issue_counts": {}, "pagination_terminal_observed": False,
                     "returned_scope_fields_verified": None if stream["provider"] == "clob" else True}
            states.append(state)
            while not state["pagination_terminal_observed"]:
                spec = request_spec(stream, cursor)
                for exact_attempt in range(1, CAPS["attempts_per_exact_page"] + 1):
                    require(ledger.http_attempts < CAPS["http_attempts"], "http_attempt_limit")
                    require(ledger.responses < CAPS["total_response_bytes"], "total_response_byte_limit")
                    require(source_snapshot(expected_head) == source, "source_changed")
                    allowance = min(CAPS["individual_response_bytes"], CAPS["total_response_bytes"] - ledger.responses)
                    ledger.reserve("raw", allowance + GZIP_RESERVE)
                    ledger.reserve("control", MAX_CONTROL)
                    wait_until(last_dispatch + .5, ledger, sleeper)
                    ledger.http_attempts += 1
                    call = ledger.http_attempts - prior["http_attempts"]
                    start = {"event": "START", "call": call, "stream": stream["id"], "page_index": page_index,
                             "exact_page_attempt": exact_attempt, "requested_utc": probe.utc_now(), "request": spec,
                             "request_sha256": digest(encode(spec)), "response_byte_reservation": allowance}
                    ledger.append("attempts.jsonl", start)
                    raw, code, response, status, retry_header = b"", None, None, None, None
                    charged = 0
                    dispatched = returned = complete_body = interrupted = False
                    last_dispatch = time.monotonic()
                    try:
                        require(ledger.remaining_time() > 0, "runtime_limit")
                        dispatched = True
                        response = transport(spec, min(30.0, ledger.remaining_time()))
                        status = response.status
                        require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
                        require(response.geturl() == spec["url"], "redirect_or_response_url_changed")
                        require(response.headers.get("Content-Encoding", "identity").lower() == "identity", "unexpected_content_encoding")
                        retry_header = response.headers.get("Retry-After")
                        raw, code, charged, interrupted = read_response(response, allowance, ledger)
                        returned, complete_body = True, code is None
                        if code is None:
                            require(status == 200, "http_status_not_200")
                            require(response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() ==
                                    "application/json", "unexpected_content_type")
                    except Error as exc:
                        code = str(exc)
                        if dispatched and not returned: charged = allowance
                    except (OSError, error.URLError, socket.timeout):
                        code, charged = "request_transport_error", allowance
                    except BaseException as exc:
                        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit))
                        code, charged = "interrupted" if interrupted else "request_runtime_error", allowance
                    finally:
                        ledger.responses += charged
                        ledger.observed += len(raw)
                        if response is not None:
                            try: response.close()
                            except BaseException as exc:
                                ledger.responses += allowance - charged
                                charged = allowance
                                interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
                                code = "interrupted" if interrupted else "response_close_error"
                    require(ledger.responses <= CAPS["total_response_bytes"], "total_response_byte_limit")
                    compressed = probe.gzip_bytes(raw)
                    body = ledger.write(f"responses/a{call:05d}.body.gz", compressed, "raw", failure=code is not None)
                    body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw),
                             "complete_received_body": complete_body}
                    ledger.bodies.append(body)
                    probe.verify_body(output, body)
                    parsed = summary = None
                    if code is None:
                        try:
                            parsed = page_envelope(raw, stream, cursor, seen_cursors, seen_rows)
                            summary = probe.inspect_rows(parsed["rows"], stream, page_index)
                            waterfalls["raw_rows_in_structurally_valid_pages_seen"] += summary["raw_row_count"]
                            waterfalls["identity_issue_occurrences_seen"] += sum(summary["identity_issue_counts"].values())
                            waterfalls["explicit_scope_conflict_occurrences_seen"] += sum(summary["scope_conflict_counts"].values())
                            if summary["scope_conflict_counts"]: code = "gamma_returned_scope_conflict"
                            page = {"event": "PAGE", "call": call, "stream": stream["id"], "page_index": page_index,
                                    "accepted": code is None, "body": body,
                                    **{key: val for key, val in parsed.items() if key != "rows"},
                                    "row_summary": compact_summary(summary)}
                            ledger.append("pages.jsonl", page)
                        except Error as exc:
                            code = str(exc)
                    retryable = code == "request_transport_error" or (code == "http_status_not_200" and status in probe.RETRYABLE)
                    delay = None
                    if retryable:
                        try: delay = retry_delay(retry_header)
                        except Error as exc: code, retryable = str(exc), False
                    finish = {"event": "FINISH", "call": call, "stream": stream["id"], "page_index": page_index,
                              "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"],
                              "status_code": status, "transport_dispatched": dispatched, "body": body,
                              "response_bytes_observed": len(raw), "response_bytes_charged": charged,
                              "charge_is_conservative": charged != len(raw), "body_complete": complete_body,
                              "interrupted": interrupted,
                              "retry_after_header": retry_header if type(retry_header) is str and len(retry_header) <= 128 else None,
                              "retry_delay_seconds": delay,
                              "error_code": code, "accepted": code is None}
                    ledger.append("attempts.jsonl", finish, failure=code is not None)
                    waterfalls["http_attempts_this_run"] += 1
                    if interrupted: raise KeyboardInterrupt()
                    if code:
                        waterfalls["failed_attempts"] += 1
                        require(retryable and exact_attempt < CAPS["attempts_per_exact_page"], code)
                        wait_until(time.monotonic() + delay, ledger, sleeper)
                        continue
                    break
                state["accepted_pages"] += 1
                state["raw_rows"] += parsed["count"]
                waterfalls["accepted_pages"] += 1
                waterfalls["raw_rows_in_accepted_pages"] += parsed["count"]
                issue_counts = Counter(state["identity_issue_counts"])
                issue_counts.update(summary["identity_issue_counts"])
                state["identity_issue_counts"] = dict(issue_counts)
                if stream["provider"] == "gamma":
                    scope_verified = scope_verified and summary["returned_scope_fields_verified"]
                    state["returned_scope_fields_verified"] = scope_verified
                state["pagination_terminal_observed"] = parsed["pagination_terminal_observed"]
                cursor = parsed["next_cursor"]
                state["next_cursor"] = cursor
                seen_cursors.add(cursor); seen_rows.add(parsed["rows_sha256"])
                page_index += 1
        require(len(states) == 5 and all(item["pagination_terminal_observed"] for item in states), "pagination_incomplete")
        require(waterfalls["accepted_pages"] == sum(item["accepted_pages"] for item in states)
                and waterfalls["raw_rows_in_accepted_pages"] == sum(item["raw_rows"] for item in states), "count_conservation_failed")
        ledger.reopen()
        final_prior = inventory_prior(admission["prior_ledger"], admission_path, output)
        require(final_prior == prior and read_bound(reference)[1] == admission_bound, "admission_or_prior_inputs_changed")
        require(verify_probe(admission["approved_probe"], source, prior) == approved, "approved_probe_changed")
        require(source_snapshot(expected_head) == source, "source_changed")
        inventory = b"".join(encode({"path": path, **bound}) for path, bound in sorted(ledger.files.items()))
        inventory_bound = ledger.write("artifact_inventory.jsonl", inventory)
        manifest = {"schema_version": "polymarket_market_metadata_campaign_v3", "status": SUCCESS,
                    "source": source, "admission": admission_bound, "approved_probe": approved, "scope": SCOPE,
                    "caps": CAPS, "streams": states, "waterfall": dict(waterfalls),
                    "usage": ledger.usage(final_reservation=CONTROL_RESERVE),
                    "artifact_inventory": inventory_bound, "completed_utc": probe.utc_now(),
                    "universe_complete": False, "snapshot_consistency_certified": False,
                    "analysis_admitted": False, "family_or_payout_admitted": False,
                    "normalized_labels_emitted": False, "schema_provider_or_record_universe_certified": False}
        manifest_bound = ledger.json("manifest.json", manifest)
        def final_check():
            require(source_snapshot(expected_head) == source, "source_changed")
            require(read_bound(reference)[1] == admission_bound, "admission_changed")
            require(inventory_prior(admission["prior_ledger"], admission_path, output) == prior,
                    "prior_inputs_changed_at_commit")
            require(verify_probe(admission["approved_probe"], source, prior) == approved, "probe_changed_at_commit")
        ledger.complete({"schema_version": "polymarket_market_metadata_campaign_completion_v3", "status": SUCCESS,
                         "manifest": manifest_bound, "universe_complete": False, "analysis_admitted": False}, final_check)
        return {"status": SUCCESS, "run_dir": str(output), "usage": manifest["usage"],
                "streams": states, "universe_complete": False, "analysis_admitted": False}
    except BaseException as exc:
        code = str(exc) if isinstance(exc, Error) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "internal_runtime_error"
        failure = {"status": "failed_unaccepted", "error_code": code, "usage": ledger.usage(), "streams": states,
                   "waterfall": dict(waterfalls), "universe_complete": False, "analysis_admitted": False}
        try: ledger.json("FAILURE.json", failure, failure=True)
        except (OSError, Error): pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return failure


@contextmanager
def process_limits():
    previous = resource.getrlimit(resource.RLIMIT_AS)
    handler = signal.getsignal(signal.SIGALRM)
    maximum = CAPS["address_space_bytes"] if previous[0] < 0 else min(previous[0], CAPS["address_space_bytes"])
    try:
        resource.setrlimit(resource.RLIMIT_AS, (maximum, previous[1]))
        require(0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= CAPS["address_space_bytes"], "memory_limit_not_effective")
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Error("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"])
        def debit_prior(prior_seconds, started):
            remaining = CAPS["runtime_seconds"] - prior_seconds - (time.monotonic() - started)
            require(remaining > 0, "runtime_limit")
            signal.setitimer(signal.ITIMER_REAL, remaining)
        yield debit_prior
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, handler)
        resource.setrlimit(resource.RLIMIT_AS, previous)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission", type=Path, required=True)
    parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    require_production_host()
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        with process_limits() as debit_prior:
            result = run_campaign(args.run_dir, args.expected_head, args.admission, args.admission_sha256,
                                  debit_prior_runtime=debit_prior)
    except Error as exc:
        result = {"status": "failed_unaccepted", "error_code": str(exc), "analysis_admitted": False}
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        # Input and environment failures must never print raw exceptions or paths
        # supplied by a failed request. No output or network is adopted on failure.
        result = {"status": "failed_unaccepted", "error_code": "admission_or_environment_error",
                  "analysis_admitted": False}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == SUCCESS else 1


if __name__ == "__main__":
    raise SystemExit(main())
