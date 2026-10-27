#!/usr/bin/env python3
"""Root-admitted compact RAW metadata pilot/full acquisition; no sample admission.

The fixed simplified CLOB walk starts afresh at MA==. A pilot observes its first
two pages and one separately labelled old-cursor diagnostic, never a stitched
coverage certificate. All old full-market bodies remain immutable supplemental
evidence, including the failed prefix. Gamma requests remain unchanged.

CLI --mode pilot|full --admission JSON --admission-sha256 SHA --expected-head HEAD
--run-dir DIR. No endpoint, cursor, cap, source, or ledger overrides are available.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
import gzip
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
import threading
import time
from urllib import error, parse, request

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_market_metadata_campaign_v3 as legacy

probe = legacy.probe
Error, require, digest, encode = legacy.Error, legacy.require, legacy.digest, legacy.encode
identity, hash_file, read_bound, number = legacy.identity, legacy.hash_file, legacy.read_bound, legacy.number
CAPS = dict(legacy.CAPS)
PILOT = {"http_attempts": 5, "page_observations": 3, "runtime_seconds": 300,
         "all_new_artifact_bytes": 128_000_000}
HTTP_SECONDS = 30.0
RUN_ROOT = Path("/mnt/data/runs")
MAX_INPUT, MAX_CONTROL = legacy.MAX_INPUT, legacy.MAX_CONTROL
CONTROL_RESERVE, GZIP_RESERVE = legacy.CONTROL_RESERVE, legacy.GZIP_RESERVE
GAMMA_LIMIT, GAMMA_SCHEMA_URI = legacy.GAMMA_LIMIT, legacy.GAMMA_SCHEMA_URI
STREAMS = ({"id": "clob_simplified", "provider": "clob",
            "endpoint": "https://clob.polymarket.com/simplified-markets"},
           *(dict(value) for value in legacy.STREAMS[1:]))
HEADERS = {"Accept": "application/json", "Accept-Encoding": "identity",
           "User-Agent": "prediction-markets-raw-metadata-campaign/4"}
SCOPE = {"streams": STREAMS, "gamma_limit": 100, "gamma_include_tag": True,
         "gamma_order": "id", "gamma_ascending": True, "gamma_cursor_parameter": "after_cursor",
         "gamma_optional_schema_uri": GAMMA_SCHEMA_URI, "clob_initial_cursor": "MA==",
         "clob_terminal_cursor": "LTE=", "clob_representation": "simplified",
         "old_full_pages": "immutable_supplemental_only", "cursor_splicing_permitted": False,
         "raw_rows_only": True, "resume_permitted": False, "hard_http_attempt_seconds": HTTP_SECONDS}
SCHEMA = "polymarket_market_metadata_campaign_admission_v4"
START_SCHEMA = "polymarket_market_metadata_campaign_v4"
FULL_SUCCESS = legacy.SUCCESS
PILOT_SUCCESS = "compact_metadata_pilot_complete_universe_incomplete"
PRIOR_MINIMUM = {"http_attempts": 4859, "response_bytes_charged": 11970589082,
                 "runtime_seconds": 7800}
RETAINED_MINIMUM = {"raw": 1223425859, "control": 14293757, "other": 0}
COMPILER_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                  "scripts/collect_polymarket_solc_binary_v2.py", "tests/test_polymarket_solc_binary_v2.py")
FAILED_PIN = {
    "root": "/mnt/data/runs/2026-10-27_polymarket_market_metadata_full_v3",
    "failure_bytes": 1235, "failure_sha256": "e8c215008efe90a593fa848003581458a6d2909bf7befe13e7bcf1736fea70ad",
    "exit_bytes": 867, "exit_sha256": "9607851c2e1d6b7444c4f2423ba2c70854318f6209876a891dec9bbc5f8d77b3",
    "drain_bytes": 232044, "drain_sha256": "188c8153f91eada372fe52b678d985976a1216bae4978e5d8c2342e13c13b795",
    "producer_head": "1bbeac146d22b2ca0ec290180f6dc7bf89318b6c",
    "pid": 37980, "birth_ticks": 1771282, "boot_id": "b9008fdc-0b94-4daf-b498-13403e96c475",
    "accepted_pages": 4809, "raw_rows": 4809000, "this_run_http_attempts": 4810,
    "cursor": "aWQ6NjAwMzE5NQ=="}
SOURCE_FILES = tuple(dict.fromkeys((*legacy.SOURCE_FILES, *COMPILER_FILES,
    "scripts/collect_polymarket_market_metadata_campaign_v4.py",
    "tests/test_polymarket_market_metadata_campaign_v4.py")))


def admission_contract():
    return {"schema_version": SCHEMA, "status": "root_compact_metadata_campaign_admitted",
        "mode": "pilot|full, exact CLI match", "consumer_head": "clean committed HEAD",
        "root_checked_prior_producers_drained": True,
        "run_dir": "fresh literal direct child of /mnt/data/runs", "caps": CAPS,
        "pilot_caps": PILOT, "scope_sha256": digest(encode(SCOPE)),
        "prior_ledger": {"all_prior_metadata_roots_included": True,
            "roots": "complete literal immutable roots",
            "standalone_control_files": "literal controls outside those roots",
            "inventory": "SHA-bound <=16MiB JSON: schema_version + files[]; counted once as control",
            "http_attempts": "all inherited debits, >=4859",
            "response_bytes_charged": "all inherited debits, >=11970589082",
            "runtime_seconds": "all inherited debits, >=7800",
            "retained_bytes_charged": "raw/control/other, no reservation refunds"},
        "failed_campaign": {"root": FAILED_PIN["root"], "failure": "fixed ref",
                            "exit": "fixed ref", "drain": "fixed ref"},
        "legacy_gamma_probe": "existing v3 approved_probe binding, unchanged helper bytes",
        "pilot_acceptance": "null for pilot; root-accepted source-bound v4 pilot for full"}


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
        bound = hash_file(REPO / name, MAX_INPUT)
        size = git("cat-file", "-s", expected_head + ":" + name)
        require(size.isdecimal() and int(size) == bound["bytes"], "committed_source_size_differs")
        raw = git("cat-file", "blob", expected_head + ":" + name, binary=True)
        require(len(raw) == bound["bytes"] and digest(raw) == bound["sha256"],
                "source_bytes_differ_from_commit")
        files[name] = {key: bound[key] for key in ("bytes", "sha256")}
    return {"head": expected_head, "files": files}


def inventory_files(value, admission_path, output):
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
    chain_minima = dict(minima)
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
                            "polymarket_market_metadata_campaign_v3", START_SCHEMA}
        for name in ("manifest.json", "FAILURE.json", "controls/RESULT.json", "RESULT.json", "FAILED.json"):
            path = root / name
            if str(path) not in paths:
                continue
            summary = probe.strict_json(read_bound({key: by_path[str(path)][key]
                for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
            require(type(summary) is dict, "prior_summary_schema")
            schema = summary.get("schema_version")
            if name in {"RESULT.json", "FAILED.json"}:
                cumulative = compiler_usage(summary, paths, by_path, root)
                for key in chain_minima:
                    chain_minima[key] = max(chain_minima[key], cumulative[key])
            elif name == "controls/RESULT.json":
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
            if name in {"RESULT.json", "FAILED.json"}:
                candidates["runtime_seconds"] = 300
            for key, candidate in candidates.items():
                ceiling = CAPS["http_attempts" if key == "http_attempts" else
                               "total_response_bytes" if key == "response_bytes_charged" else "runtime_seconds"]
                local[key] = max(local[key], number(candidate, ceiling, integer=key != "runtime_seconds"))
        attempt_evidence = any(Path(path).relative_to(root).parts[0] == "attempts" or
            Path(path).name in {"attempts.jsonl", "REQUESTS.jsonl"} or
            Path(path).name.endswith((".START.json", ".FINISH.json"))
            for path in paths if root in Path(path).parents)
        require(recognized, "prior_collection_summary_missing")
        for key in minima:
            minima[key] += local[key]
    require(all(prior[key] >= max(minima[key], chain_minima[key]) for key in minima), "prior_usage_below_durable_minima")
    prior["durable_usage_minima"] = minima
    return prior


def compiler_usage(summary, paths, by_path, root):
    """Only the fixed acquisition-v2 profile; its inherited totals are not added twice."""
    require(summary.get("schema_version") == "polymarket_solc_binary_acquisition_v2"
            and summary.get("status") in {"authenticated_solc_binary_not_executed", "blocked"}
            and summary.get("evidence_class") == "public_toolchain_binary"
            and summary.get("compiler_executed") is False and summary.get("sample_admitted") is False,
            "prior_compiler_profile_unrecognized")
    caps = summary.get("caps", {})
    require(type(caps) is dict and caps.get("http_attempts") == 2
            and caps.get("total_response_bytes_charged") == 64 * 1024**2 + 2
            and caps.get("runtime_seconds") == 300, "prior_compiler_caps_differ")
    attempts = summary.get("attempts")
    require(type(attempts) is list and len(attempts) <= 2, "prior_compiler_attempts_invalid")
    calls = number(summary.get("http_attempts"), 2)
    charge = number(summary.get("response_bytes_charged"), 64 * 1024**2 + 2)
    require(calls >= len(attempts), "prior_compiler_attempt_count_differs")
    billed = 0
    starts = [path for path in paths if Path(path).parent == root and re.fullmatch(r"attempt[0-9]+\.START\.json", Path(path).name)]
    require(len(starts) == calls, "prior_compiler_attempt_count_differs")
    for index in range(1, calls+1):
        start_path = str(root / f"attempt{index:02d}.START.json")
        require(start_path in paths, "prior_compiler_attempt_receipt_missing")
        start = probe.strict_json(read_bound({key: by_path[start_path][key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
        allowance = 32 * 1024**2 + 1
        require(type(start) is dict and set(start) == {"attempt", "url", "allowance"}
                and type(start["attempt"]) is int and start["attempt"] == index and type(start["allowance"]) is int
                and start["allowance"] == allowance and start["url"] ==
                "https://binaries.soliditylang.org/linux-amd64/solc-linux-amd64-v0.8.34+commit.80d5c536",
                "prior_compiler_request_differs")
        finish_path = str(root / f"attempt{index:02d}.FINISH.json")
        if finish_path not in paths:
            require(summary["status"] == "blocked", "prior_compiler_unclosed_success")
            billed += allowance
            continue
        finish = probe.strict_json(read_bound({key: by_path[finish_path][key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
        require(type(finish) is dict and index <= len(attempts) and finish == attempts[index-1], "prior_compiler_finish_differs")
        charge_this = number(finish.get("response_bytes_charged"), allowance)
        if finish.get("error_code") == "attempt_failed_before_sealed_receipt":
            require(charge_this == allowance and finish.get("body_integrity_verified") is False,
                    "prior_compiler_unknown_body_refund")
        else:
            observed = number(finish.get("response_bytes_observed"), allowance)
            body = finish.get("body", {})
            path = root/body.get("path", "")
            require(path.parent == root and path.name == f"attempt{index:02d}.part" and str(path) in by_path
                    and by_path[str(path)]["category"] == "other"
                    and all(body.get(key) == by_path[str(path)][key] for key in ("bytes", "sha256")),
                    "prior_compiler_body_binding_differs")
            require(charge_this >= observed, "prior_compiler_usage_below_attempts")
            if charge_this < allowance:
                require(finish.get("error_code") in {None, "compiler_sha_mismatch", "http_status_not_200"}
                        and finish.get("response_close_completed") is True and observed == body["bytes"],
                        "prior_compiler_unknown_body_refund")
        billed += charge_this
    require(charge >= billed, "prior_compiler_usage_below_attempts")
    # The caller's inherited/cumulative profiles remain evidence, never fresh use.
    for key in ("prior_metadata_usage", "cumulative_metadata_usage"):
        require(type(summary.get(key)) is dict, "prior_compiler_global_usage_missing")
    inherited, cumulative = summary["prior_metadata_usage"], summary["cumulative_metadata_usage"]
    require(set(inherited) == set(cumulative) == set(PRIOR_MINIMUM)
            and summary.get("runtime_seconds_charged") == 300, "prior_compiler_global_usage_invalid")
    own = {"http_attempts": calls, "response_bytes_charged": charge, "runtime_seconds": 300}
    for key in own:
        ceiling = CAPS["http_attempts" if key == "http_attempts" else "total_response_bytes" if key == "response_bytes_charged" else "runtime_seconds"]
        before = number(inherited[key], ceiling, integer=key != "runtime_seconds")
        total = number(cumulative[key], ceiling, integer=key != "runtime_seconds")
        require(before >= PRIOR_MINIMUM[key] and total == before + own[key], "prior_compiler_global_usage_invalid")
    return cumulative


def inventory_prior(value, admission_path, output):
    require(type(value) is dict and set(value) == {"all_prior_metadata_roots_included", "roots",
            "standalone_control_files", "inventory", "http_attempts", "response_bytes_charged", "runtime_seconds", "retained_bytes_charged"},
            "prior_ledger_schema")
    raw, inventory = read_bound(value["inventory"], MAX_CONTROL)
    saved = probe.strict_json(raw)
    require(type(saved) is dict and set(saved) == {"schema_version", "files"}
            and saved["schema_version"] == "polymarket_metadata_prior_inventory_v4", "prior_inventory_schema")
    path = Path(inventory["path"])
    require(path != admission_path and path != output and output not in path.parents
            and RUN_ROOT in path.parents and all(Path(root) not in path.parents for root in value["roots"])
            and str(path) not in value["standalone_control_files"], "prior_inventory_overlap")
    forbidden = {"START.json", "RESULT.json", "FAILED.json", "FAILURE.json", "manifest.json", "COMPLETE.json",
                 "attempts.jsonl", "pages.jsonl", "artifact_inventory.jsonl"}
    for name in value["standalone_control_files"]:
        local = Path(name)
        require(local.name not in forbidden and not local.name.endswith((".START.json", ".FINISH.json", ".part")),
                "standalone_collection_evidence_forbidden")
    expanded = {key: val for key, val in value.items() if key not in {"inventory", "retained_bytes_charged"}}
    expanded["files"] = saved["files"]
    prior = inventory_files(expanded, admission_path, output)
    require(all(prior[key] >= minimum for key, minimum in PRIOR_MINIMUM.items()), "prior_below_frozen_minimum")
    require(all(item["path"] != inventory["path"] for item in prior["files"]), "prior_inventory_double_counted")
    charged = value["retained_bytes_charged"]
    require(type(charged) is dict and set(charged) == set(RETAINED_MINIMUM), "prior_retained_charge_schema")
    for key in charged:
        number(charged[key], CAPS["aggregate_metadata_bytes"])
        require(charged[key] >= max(prior["bytes"][key], RETAINED_MINIMUM[key]), "prior_retained_charge_understated")
    require(charged["raw"] <= CAPS["raw_body_bytes"] and charged["control"] <= CAPS["control_bytes"]
            and charged["other"] <= CAPS["other_metadata_bytes"] and sum(charged.values()) <= CAPS["aggregate_metadata_bytes"],
            "prior_byte_cap")
    prior["observed_bytes"], prior["bytes"] = prior["bytes"], dict(charged)
    for root in prior["roots"]:
        for filename in ("manifest.json", "FAILURE.json"):
            ref = next((item for item in prior["files"] if item["path"] == str(Path(root)/filename)), None)
            if ref is not None:
                summary = probe.strict_json(read_bound({key: ref[key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
                previous = summary.get("usage", {}).get("retained_bytes_charged")
                if previous is not None:
                    require(type(previous) is dict and set(previous) == set(charged)
                            and all(charged[key] >= number(previous[key], CAPS["aggregate_metadata_bytes"]) for key in charged),
                            "prior_published_retained_charge_understated")
    prior["external_inventory"] = inventory
    return prior


def verify_compiler_producers(prior, source):
    """Carry the fixed v2 producer, admission and newly retained profile exactly."""
    by_path = {item["path"]: item for item in prior["files"]}
    for name in prior["roots"]:
        root = Path(name)
        refs = [by_path[str(root / key)] for key in ("RESULT.json", "FAILED.json") if str(root / key) in by_path]
        if not refs:
            continue
        summary = probe.strict_json(read_bound({key: refs[-1][key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
        require(summary.get("schema_version") == "polymarket_solc_binary_acquisition_v2", "prior_compiler_profile_unrecognized")
        producer = summary.get("source", {})
        expected = {key: source["files"][key] for key in COMPILER_FILES}
        require(type(producer) is dict and set(producer) == {"head", "files"}
                and producer["files"] == {key: value["sha256"] for key, value in expected.items()},
                "compiler_producer_binding_differs")
        verify_producer({"head": producer["head"], "files": expected}, expected)
        raw, admission = prior_reference(summary.get("admission"), prior)
        parent = probe.strict_json(raw)
        require(parent.get("schema_version") == "polymarket_solc_acquisition_admission_v2"
                and parent.get("status") == "root_terminal_metadata_solc_admitted"
                and parent.get("consumer_head") == producer["head"] and parent.get("run_dir") == str(root),
                "prior_compiler_admission_differs")
        inherited = parent.get("prior_ledger", {})
        entries = inherited.get("files")
        require(type(entries) is list and type(inherited.get("retained_bytes_charged")) is dict
                and inherited["retained_bytes_charged"] == summary.get("prior_metadata_bytes"),
                "prior_compiler_retained_parent_differs")
        old = set()
        for item in entries:
            require(type(item) is dict and item.get("path") in by_path
                    and all(item.get(key) == by_path[item["path"]][key] for key in ("bytes", "sha256", "category")),
                    "prior_compiler_parent_file_missing")
            require(item["path"] not in old, "prior_compiler_parent_file_duplicate")
            old.add(item["path"])
        new = {key: 0 for key in RETAINED_MINIMUM}
        for path, item in by_path.items():
            if path not in old:
                new[item["category"]] += item["bytes"]
        for item in prior["files"]:
            if root in Path(item["path"]).parents:
                require(item["category"] == ("other" if Path(item["path"]).suffix == ".part" else "control"),
                        "prior_compiler_artifact_misclassified")
        new["control"] = max(new["control"], admission["bytes"] + 3 * 1024**2)
        # Later metadata bodies are not part of the compiler's 66MiB profile.
        own_other = sum(item["bytes"] for item in prior["files"] if root in Path(item["path"]).parents and item["category"] == "other")
        own_control = sum(item["bytes"] for item in prior["files"] if root in Path(item["path"]).parents and item["category"] == "control")
        require(own_other + max(own_control + admission["bytes"], admission["bytes"] + 3 * 1024**2) <= 66 * 1024**2,
                "prior_compiler_profile_byte_limit")
        require(all(prior["bytes"][key] >= number(summary["prior_metadata_bytes"].get(key), CAPS["aggregate_metadata_bytes"]) + new[key]
                    for key in new), "prior_compiler_retained_charge_understated")


def prior_reference(reference, prior, *, maximum=MAX_CONTROL):
    raw, bound = read_bound(reference, maximum)
    matches = [item for item in prior["files"] if item["path"] == bound["path"]]
    require(len(matches) == 1 and all(matches[0][key] == bound[key] for key in ("bytes", "sha256")),
            "input_not_in_prior_ledger")
    return raw, bound


def verify_failed(value, prior):
    require(type(value) is dict and set(value) == {"root", "failure", "exit", "drain"}
            and value["root"] == FAILED_PIN["root"] and value["root"] in prior["roots"], "failed_root_differs")
    controls = {}
    for key in ("failure", "exit", "drain"):
        require(value[key].get("bytes") == FAILED_PIN[key + "_bytes"]
                and value[key].get("sha256") == FAILED_PIN[key + "_sha256"], "failed_control_pin_differs")
        raw, controls[key] = prior_reference(value[key], prior)
        if key != "drain":
            controls[key]["value"] = probe.strict_json(raw)
    root = Path(value["root"])
    prior_by_path = {item["path"]: item for item in prior["files"]}
    start_ref = prior_by_path.get(str(root / "START.json"))
    require(start_ref is not None, "failed_start_missing")
    start = probe.strict_json(read_bound({key: start_ref[key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0])
    require(start.get("schema_version") == "polymarket_market_metadata_campaign_v3"
            and start.get("source", {}).get("head") == FAILED_PIN["producer_head"]
            and start.get("caps") == CAPS and start.get("scope") == json.loads(encode(legacy.SCOPE)),
            "failed_start_contract_differs")
    parent_raw, _ = prior_reference({key: start.get("admission", {}).get(key) for key in ("path", "bytes", "sha256")}, prior)
    parent = probe.strict_json(parent_raw)
    require(parent.get("schema_version") == legacy.SCHEMA and parent.get("consumer_head") == FAILED_PIN["producer_head"]
            and parent.get("caps") == CAPS and parent.get("scope_sha256") == digest(encode(legacy.SCOPE)),
            "failed_parent_contract_differs")
    inherited = parent.get("prior_ledger", {})
    require(inherited.get("all_prior_metadata_roots_included") is True and type(inherited.get("roots")) is list
            and len(inherited["roots"]) == 4 and set(inherited["roots"]) <= set(prior["roots"])
            and type(inherited.get("files")) is list, "failed_parent_roots_missing")
    for item in inherited["files"]:
        require(type(item) is dict and item.get("path") in prior_by_path
                and all(item.get(key) == prior_by_path[item["path"]][key] for key in ("bytes", "sha256", "category")),
                "failed_parent_file_missing")
    require(Path(value["failure"]["path"]) == root / "FAILURE.json", "failed_failure_path_differs")
    failure = controls["failure"]["value"]
    require(failure.get("status") == "failed_unaccepted" and failure.get("error_code") == "response_byte_limit"
            and failure.get("universe_complete") is False and failure.get("analysis_admitted") is False,
            "failed_status_differs")
    streams = failure.get("streams")
    require(type(streams) is list and len(streams) == 1 and streams[0].get("id") == "clob"
            and streams[0].get("accepted_pages") == FAILED_PIN["accepted_pages"]
            and streams[0].get("raw_rows") == FAILED_PIN["raw_rows"]
            and streams[0].get("next_cursor") == FAILED_PIN["cursor"]
            and streams[0].get("pagination_terminal_observed") is False, "failed_prefix_scope_differs")
    usage = failure.get("usage", {})
    require(usage.get("this_run_http_attempts") == FAILED_PIN["this_run_http_attempts"], "failed_usage_differs")
    exited = controls["exit"]["value"]
    launch = exited.get("launch", {})
    require(exited.get("exit_code") == 1 and launch.get("pid") == FAILED_PIN["pid"]
            and launch.get("linux_birth_ticks") == FAILED_PIN["birth_ticks"]
            and launch.get("boot_id") == FAILED_PIN["boot_id"]
            and launch.get("source_head") == FAILED_PIN["producer_head"], "failed_exit_identity_differs")
    journal = next((item for item in prior["files"] if item["path"] == str(root / "attempts.jsonl")), None)
    require(journal is not None, "failed_attempt_journal_missing")
    raw = read_bound({key: journal[key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0]
    records = [probe.strict_json(line) for line in raw.splitlines()]
    count = FAILED_PIN["this_run_http_attempts"]
    require(len(records) == 2 * count, "failed_attempt_journal_incomplete")
    charge, accepted = 0, 0
    by_path = prior_by_path
    for index in range(count):
        start, finish = records[2*index:2*index+2]
        require(type(start) is dict and type(finish) is dict and start.get("event") == "START"
                and finish.get("event") == "FINISH" and type(start.get("call")) is int
                and start["call"] == finish.get("call") == index+1
                and start.get("stream") == finish.get("stream") == "clob"
                and start.get("page_index") == finish.get("page_index") == index,
                "failed_attempt_identity_differs")
        spec = start.get("request", {})
        cursor = spec.get("parameters", {}).get("next_cursor")
        require(spec == legacy.request_spec(legacy.STREAMS[0], cursor)
                and start.get("request_sha256") == finish.get("request_sha256") == digest(encode(spec))
                and start.get("response_byte_reservation") == CAPS["individual_response_bytes"],
                "failed_attempt_request_differs")
        body = finish.get("body", {})
        body_path = root / body.get("path", "")
        require(body_path.parent == root / "responses" and str(body_path) in by_path
                and all(body.get(key) == by_path[str(body_path)][key] for key in ("bytes", "sha256")),
                "failed_body_not_in_ledger")
        observed = number(finish.get("response_bytes_observed"), CAPS["individual_response_bytes"])
        billed = number(finish.get("response_bytes_charged"), CAPS["individual_response_bytes"])
        require(billed >= observed and observed == body.get("uncompressed_bytes")
                and re.fullmatch(r"[0-9a-f]{64}", body.get("uncompressed_sha256", "")), "failed_attempt_charge_invalid")
        charge += billed
        if index < count-1:
            require(finish.get("accepted") is True and finish.get("error_code") is None
                    and finish.get("body_complete") is True and body.get("complete_received_body") is True,
                    "failed_prefix_receipt_not_accepted")
            accepted += 1
        else:
            require(cursor == FAILED_PIN["cursor"] and finish.get("accepted") is False
                    and finish.get("error_code") == "response_byte_limit" and finish.get("body_complete") is False
                    and body.get("complete_received_body") is False, "failed_final_receipt_differs")
            probe.verify_body(root, body)
    require(accepted == FAILED_PIN["accepted_pages"] and charge == usage.get("this_run_response_bytes_charged"),
            "failed_prefix_counters_differ")
    return {"root": str(root), "controls": {key: {k: v for k, v in bound.items() if k != "value"}
            for key, bound in controls.items()}, "reported_accepted_pages": accepted,
            "reported_raw_rows": FAILED_PIN["raw_rows"], "diagnostic_cursor": FAILED_PIN["cursor"],
            "old_pages_supplemental_only": True, "old_page_rows_renormalized": False,
            "old_prefix_is_new_stream_coverage": False, "failed_prefix_admitted": False}


def request_spec(stream, cursor):
    params = {"next_cursor": cursor} if stream["provider"] == "clob" else {
        "version": stream["version"], "closed": str(stream["closed"]).lower(), "limit": "100",
        "include_tag": "true", "order": "id", "ascending": "true"}
    if stream["provider"] == "gamma" and cursor is not None:
        params["after_cursor"] = cursor
    return {"method": "GET", "url": stream["endpoint"] + "?" + parse.urlencode(params),
            "headers": HEADERS, "stream": stream["id"], "parameters": params}


def public_transport(spec, timeout):
    opener = request.build_opener(request.ProxyHandler({}), probe.NoRedirect())
    require(spec.get("method") == "GET" and spec.get("headers") == HEADERS, "transport_request_differs")
    outgoing = request.Request(spec["url"], headers=HEADERS, method="GET")
    try:
        return opener.open(outgoing, timeout=timeout)
    except error.HTTPError as response:
        return response


class Ledger(legacy.Ledger):
    def __init__(self, output, prior, admission_bytes, started, mode):
        super().__init__(output, prior, admission_bytes + prior["external_inventory"]["bytes"], started)
        self.mode = mode

    def remaining_time(self):
        remaining = super().remaining_time()
        return min(remaining, PILOT["runtime_seconds"] - (time.monotonic() - self.started)) if self.mode == "pilot" else remaining

    def reserve(self, category, amount, *, failure=False):
        super().reserve(category, amount, failure=failure)
        if self.mode == "pilot":
            require(sum(self.used.values()) + amount + CONTROL_RESERVE + CAPS["external_root_controls_reservation_bytes"]
                    <= PILOT["all_new_artifact_bytes"], "pilot_artifact_limit")

    def usage(self, final_reservation=0):
        return super().usage(final_reservation) | {
            "external_prior_inventory_control_bytes": self.prior["external_inventory"]["bytes"]}


@contextmanager
def attempt_alarm(ledger, deadline=None):
    """Hard open/read/close/normal-seal deadline, restoring the elapsed outer timer."""
    require(threading.current_thread() is threading.main_thread(), "main_thread_attempt_timer_required")
    began = time.monotonic()
    prior_handler = signal.getsignal(signal.SIGALRM)
    prior_delay, interval = signal.getitimer(signal.ITIMER_REAL)
    require(interval == 0, "unsupported_existing_interval_timer")
    prior_ends = began + prior_delay if prior_delay else None
    available = ledger.remaining_time()
    local = HTTP_SECONDS if deadline is None else deadline - began
    delay = min(local, available, prior_delay if prior_delay else math.inf)
    reason = "http_attempt_timeout" if local < available and (not prior_delay or local < prior_delay) else "runtime_limit"
    require(delay > 0, reason)
    ends = began + delay
    def expired(*_):
        raise Error(reason)
    def check():
        ledger.check()
        require(time.monotonic() < ends, reason)
    def rearm():
        check()
        signal.setitimer(signal.ITIMER_REAL, ends - time.monotonic())
    try:
        signal.signal(signal.SIGALRM, expired)
        rearm()
        yield check, rearm
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior_handler)
        if prior_ends is not None:
            signal.setitimer(signal.ITIMER_REAL, max(.000001, prior_ends - time.monotonic()))


def bounded_read(response, allowance, check):
    raw = bytearray()
    try:
        header = response.headers.get("Content-Length")
        length = None if header is None else int(header)
        require(header is None or (type(header) is str and str(length) == header and length >= 0), "invalid_content_length")
        if length is not None and length > allowance:
            return b"", "response_byte_limit", allowance, False, False
        code = None
        while len(raw) < allowance:
            check()
            size = min(65536, allowance - len(raw))
            try:
                part = response.read(size)
            except http.client.IncompleteRead as exc:
                if type(exc.partial) is bytes:
                    raw.extend(exc.partial[:allowance-len(raw)])
                return bytes(raw), "incomplete_response", allowance, False, False
            require(type(part) is bytes and len(part) <= size, "transport_read_contract")
            raw.extend(part)
            check()
            if not part:
                if length is not None and len(raw) != length:
                    code = "truncated_response"
                break
        else:
            if length != len(raw):
                code = "response_byte_limit"
        if length is not None and len(raw) > length:
            code = "content_length_differs"
        return bytes(raw), code, allowance if code else len(raw), code is None, False
    except BaseException as exc:
        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit))
        code = str(exc) if isinstance(exc, Error) else "interrupted" if interrupted else "response_transport_error"
        return bytes(raw), code, allowance, False, interrupted


def received_attempt(ledger, call, spec, allowance, transport, deadline):
    raw, code, charged, complete = b"", None, 0, False
    response, status, retry_header = None, None, None
    dispatched = interrupted = close_attempted = close_completed = False
    body = None
    relative = f"responses/a{call:05d}.body.gz"
    try:
        with attempt_alarm(ledger, deadline) as (check, rearm):
            try:
                check()
                dispatched = True
                response = transport(spec, min(HTTP_SECONDS, ledger.remaining_time()))
                check()
                status = response.status
                require(type(status) is int and 100 <= status <= 599, "invalid_http_status")
                require(response.geturl() == spec["url"], "redirect_or_response_url_changed")
                require(response.headers.get("Content-Encoding", "identity").lower() == "identity", "unexpected_content_encoding")
                retry_header = response.headers.get("Retry-After")
                raw, code, charged, complete, interrupted = bounded_read(response, allowance, check)
                if code is None:
                    require(status == 200, "http_status_not_200")
                    require(response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower() == "application/json",
                            "unexpected_content_type")
            except BaseException as exc:
                code = str(exc) if isinstance(exc, Error) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "request_transport_error"
                interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
                if dispatched and not complete:
                    charged = allowance
            finally:
                if response is not None:
                    try:
                        rearm(); close_attempted = True
                        response.close(); close_completed = True
                    except BaseException as exc:
                        code = str(exc) if isinstance(exc, Error) else "response_close_error"
                        charged, complete = allowance, False
                        interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
                try:
                    check()
                except Error as exc:
                    code, charged, complete = str(exc), allowance, False
            if code is None:
                compressed = probe.gzip_bytes(raw)
                check()
                body = ledger.write(relative, compressed, "raw")
                body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw), "complete_received_body": True}
                probe.verify_body(ledger.output, body)
                check()
    except BaseException as exc:
        code = str(exc) if isinstance(exc, Error) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "attempt_runtime_or_durability_error"
        charged, complete = allowance, False
        interrupted = interrupted or isinstance(exc, (KeyboardInterrupt, SystemExit))
    if body is None or code is not None:
        # Failure evidence may finish under the unchanged outer stage deadline.
        # Never replace a partially published normal body after an I/O timeout.
        if relative in ledger.files or (ledger.output / relative).exists():
            relative = f"responses/a{call:05d}.failed.body.gz"
        body = ledger.write(relative, probe.gzip_bytes(raw), "raw", failure=True)
        body |= {"uncompressed_bytes": len(raw), "uncompressed_sha256": digest(raw), "complete_received_body": complete}
        probe.verify_body(ledger.output, body)
    return {"status_code": status, "error_code": code, "body": body,
        "response_bytes_observed": len(raw), "response_bytes_charged": charged,
        "body_complete": complete, "transport_dispatched": dispatched, "interrupted": interrupted,
        "response_close_attempted": close_attempted, "response_close_completed": close_completed,
        "retry_after_header": retry_header if type(retry_header) is str and len(retry_header) <= 128 else None}


def verify_producer(producer, expected):
    require(type(producer) is dict and set(producer) == {"head", "files"}
            and re.fullmatch(r"[0-9a-f]{40}", producer.get("head", ""))
            and producer["files"] == expected, "producer_binding_differs")
    for name, bound in expected.items():
        try:
            size = subprocess.check_output(["git", "cat-file", "-s", producer["head"] + ":" + name], cwd=REPO,
                                           stderr=subprocess.DEVNULL, timeout=10).decode().strip()
            require(size.isdecimal() and int(size) == bound["bytes"], "producer_blob_differs")
            raw = subprocess.check_output(["git", "cat-file", "blob", producer["head"] + ":" + name], cwd=REPO,
                                          stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            raise Error("producer_blob_unavailable") from None
        require(len(raw) == bound["bytes"] and digest(raw) == bound["sha256"], "producer_blob_differs")


def load_records(root, name, prior):
    item = next((row for row in prior["files"] if row["path"] == str(root/name)), None)
    require(item is not None, "pilot_journal_missing")
    raw = read_bound({key: item[key] for key in ("path", "bytes", "sha256")}, MAX_CONTROL)[0]
    return [probe.strict_json(line) for line in raw.splitlines()]


def verify_pilot(value, source, prior):
    require(type(value) is dict and set(value) == {"accepted", "producer_head", "producer_blobs", "complete", "manifest"}
            and value["accepted"] is True, "compact_pilot_acceptance_missing")
    expected = source["files"]
    verify_producer({"head": value["producer_head"], "files": value["producer_blobs"]}, expected)
    marker_raw, marker_bound = prior_reference(value["complete"], prior)
    manifest_raw, manifest_bound = prior_reference(value["manifest"], prior)
    marker, manifest = probe.strict_json(marker_raw), probe.strict_json(manifest_raw)
    root = Path(marker_bound["path"]).parent
    require(str(root) in prior["roots"] and Path(marker_bound["path"]) == root/"COMPLETE.json"
            and Path(manifest_bound["path"]) == root/"manifest.json", "compact_pilot_root_differs")
    require(marker.get("schema_version") == "polymarket_market_metadata_campaign_completion_v4"
            and marker.get("mode") == "pilot" and marker.get("status") == PILOT_SUCCESS
            and marker.get("manifest") == {"path": "manifest.json", "bytes": len(manifest_raw),
                "sha256": digest(manifest_raw), "category": "control"}
            and marker.get("universe_complete") is False and marker.get("analysis_admitted") is False,
            "compact_pilot_marker_differs")
    require(manifest.get("schema_version") == START_SCHEMA and manifest.get("mode") == "pilot"
            and manifest.get("status") == PILOT_SUCCESS and manifest.get("caps") == CAPS
            and manifest.get("pilot_caps") == PILOT and manifest.get("scope") == json.loads(encode(SCOPE))
            and manifest.get("source") == {"head": value["producer_head"], "files": expected}
            and manifest.get("universe_complete") is False and manifest.get("analysis_admitted") is False
            and manifest.get("old_prefix_is_new_stream_coverage") is False,
            "compact_pilot_contract_differs")
    inventory = manifest.get("artifact_inventory", {})
    require(type(inventory) is dict and inventory.get("path") == "artifact_inventory.jsonl", "compact_pilot_inventory_invalid")
    raw, _ = prior_reference({"path": str(root/inventory["path"]), **{key: inventory.get(key) for key in ("bytes", "sha256")}}, prior)
    entries = [probe.strict_json(line) for line in raw.splitlines()]
    by_path = {item["path"]: item for item in prior["files"]}
    names = set()
    for item in entries:
        require(type(item) is dict and set(item) == {"path", "bytes", "sha256", "category"}, "compact_pilot_inventory_invalid")
        relative = Path(item["path"])
        require(not relative.is_absolute() and ".." not in relative.parts and item["path"] not in names,
                "compact_pilot_inventory_invalid")
        names.add(item["path"])
        full = str(root/relative)
        require(full in by_path and all(item[key] == by_path[full][key] for key in ("bytes", "sha256", "category")),
                "compact_pilot_inventory_binding_differs")
    actual = {str(Path(path).relative_to(root)) for path in by_path if root in Path(path).parents}
    require(actual == names | {"artifact_inventory.jsonl", "manifest.json", "COMPLETE.json"}, "compact_pilot_inventory_incomplete")
    parent_raw, _ = prior_reference({key: manifest.get("admission", {}).get(key) for key in ("path", "bytes", "sha256")}, prior)
    parent = probe.strict_json(parent_raw)
    require(parent.get("schema_version") == SCHEMA and parent.get("mode") == "pilot"
            and parent.get("consumer_head") == value["producer_head"] and parent.get("run_dir") == str(root)
            and parent.get("scope_sha256") == digest(encode(SCOPE)), "compact_pilot_admission_differs")
    prior_reference(parent.get("prior_ledger", {}).get("inventory"), prior)
    records = load_records(root, "attempts.jsonl", prior)
    require(len(records) % 2 == 0 and 1 <= len(records)//2 <= PILOT["http_attempts"], "compact_pilot_attempts_invalid")
    calls = {}
    for index in range(len(records)//2):
        start, finish = records[2*index:2*index+2]
        require(type(start) is dict and type(finish) is dict and start.get("event") == "START"
                and finish.get("event") == "FINISH" and type(start.get("call")) is int
                and start["call"] == finish.get("call") == index+1
                and start.get("request_sha256") == finish.get("request_sha256") == digest(encode(start.get("request"))),
                "compact_pilot_attempt_binding_differs")
        observed = number(finish.get("response_bytes_observed"), CAPS["individual_response_bytes"])
        billed = number(finish.get("response_bytes_charged"), CAPS["individual_response_bytes"])
        require(billed >= observed and (finish.get("body_complete") is True or billed == start.get("response_byte_reservation")),
                "compact_pilot_attempt_charge_invalid")
        calls[index+1] = (start, finish)
    observations = manifest.get("observations")
    require(type(observations) is list and len(observations) == PILOT["page_observations"], "compact_pilot_observation_count_differs")
    cursor, seen_cursors, seen_rows, accepted = "MA==", {"MA=="}, set(), set()
    raw_count = 0
    for index, observation in enumerate(observations):
        role = "fresh_prefix" if index < 2 else "old_cursor_diagnostic"
        requested = cursor if index < 2 else FAILED_PIN["cursor"]
        require(type(observation) is dict and observation.get("coverage_role") == role
                and observation.get("call") in calls and observation["call"] not in accepted,
                "compact_pilot_observation_binding_differs")
        start, finish = calls[observation["call"]]
        accepted.add(observation["call"])
        spec = request_spec(STREAMS[0], requested)
        require(start.get("request") == spec and start.get("coverage_role") == role
                and observation.get("requested_cursor") == requested and finish.get("accepted") is True
                and finish.get("error_code") is None and finish.get("status_code") == 200
                and finish.get("transport_dispatched") is True and finish.get("body_complete") is True
                and finish.get("response_close_completed") is True and finish.get("body") == observation.get("body"),
                "compact_pilot_request_or_finish_differs")
        body = observation["body"]
        prior_reference({"path": str(root/body["path"]), **{key: body[key] for key in ("bytes", "sha256")}}, prior,
                        maximum=CAPS["individual_response_bytes"]+GZIP_RESERVE)
        probe.verify_body(root, body)
        with gzip.open(root/body["path"], "rb") as stream:
            raw = stream.read(CAPS["individual_response_bytes"]+1)
        require(len(raw) <= CAPS["individual_response_bytes"], "compact_pilot_body_expansion_limit")
        parsed = legacy.page_envelope(raw, STREAMS[0], requested, seen_cursors if index < 2 else {requested},
                                      seen_rows if index < 2 else set())
        location = dict(STREAMS[0], id=STREAMS[0]["id"] if index < 2 else "clob_simplified_diagnostic")
        summary = probe.inspect_rows(parsed["rows"], location, index if index < 2 else 0)
        require(observation.get("row_summary") == legacy.compact_summary(summary)
                and all(observation.get(key) == value for key, value in parsed.items() if key != "rows"),
                "compact_pilot_page_differs")
        require(index != 0 or not parsed["pagination_terminal_observed"], "compact_pilot_first_page_terminal")
        raw_count += parsed["count"]
        if index < 2:
            cursor = parsed["next_cursor"]; seen_cursors.add(cursor); seen_rows.add(parsed["rows_sha256"])
    require({call for call, (_, finish) in calls.items() if finish.get("accepted") is True} == accepted,
            "compact_pilot_accepted_call_count_differs")
    usage = manifest.get("usage", {})
    require(usage.get("this_run_http_attempts") == len(calls)
            and manifest.get("waterfall", {}).get("raw_rows_in_accepted_pages") == raw_count
            and usage.get("this_run_response_bytes_charged") == sum(item[1]["response_bytes_charged"] for item in calls.values()),
            "compact_pilot_usage_differs")
    return {"complete": marker_bound, "manifest": manifest_bound, "producer_head": value["producer_head"],
            "producer_blobs": expected, "old_cursor_diagnostic_is_coverage": False}


def run_campaign(output, expected_head, admission_path, admission_sha, mode, *, transport=None,
                 sleeper=time.sleep, debit_prior_runtime=None):
    started = time.monotonic()
    require(mode in {"pilot", "full"} and CAPS == legacy.CAPS, "mode_or_helper_caps_differs")
    output, admission_path = Path(output), Path(admission_path)
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.is_dir()
            and output.parent.resolve() == output.parent and not output.exists() and not output.is_symlink(),
            "destination_not_fresh")
    source = source_snapshot(expected_head)
    reference = {"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}
    raw, admission_bound = read_bound(reference)
    admission = probe.strict_json(raw)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "run_dir", "consumer_head", "mode",
            "caps", "pilot_caps", "scope_sha256", "prior_ledger", "failed_campaign", "legacy_gamma_probe", "pilot_acceptance",
            "root_checked_prior_producers_drained"}
            and admission["schema_version"] == SCHEMA and admission["status"] == "root_compact_metadata_campaign_admitted"
            and admission["run_dir"] == str(output) and admission["consumer_head"] == expected_head
            and admission["mode"] == mode and admission["caps"] == CAPS and admission["pilot_caps"] == PILOT
            and admission["root_checked_prior_producers_drained"] is True
            and admission["scope_sha256"] == digest(encode(SCOPE)), "root_admission_not_exact")
    prior = inventory_prior(admission["prior_ledger"], admission_path, output)
    require(prior["runtime_seconds"] + time.monotonic() - started < CAPS["runtime_seconds"], "runtime_limit")
    if debit_prior_runtime is not None:
        debit_prior_runtime(prior["runtime_seconds"], started)
    if mode == "pilot":
        require(time.monotonic() - started < PILOT["runtime_seconds"], "runtime_limit")
    failed = verify_failed(admission["failed_campaign"], prior)
    gamma = legacy.verify_probe(admission["legacy_gamma_probe"], source, prior)
    verify_compiler_producers(prior, source)
    if mode == "full":
        accepted_pilot = verify_pilot(admission["pilot_acceptance"], source, prior)
    else:
        require(admission["pilot_acceptance"] is None, "pilot_must_not_adopt_pilot")
        accepted_pilot = None
    require(probe.available_ram() >= CAPS["minimum_available_ram_bytes"], "available_ram_below_floor")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CONTROL_RESERVE
            + CAPS["external_root_controls_reservation_bytes"] + CAPS["individual_response_bytes"] + GZIP_RESERVE,
            "disk_floor")
    output.mkdir(mode=0o700)
    ledger = Ledger(output, prior, len(raw), started, mode)
    states, observations, waterfalls = [], [], Counter()
    last_dispatch = -math.inf
    transport = transport or public_transport
    try:
        ledger.json("START.json", {"schema_version": START_SCHEMA, "mode": mode, "source": source,
            "admission": admission_bound, "caps": CAPS, "pilot_caps": PILOT, "scope": SCOPE,
            "legacy_gamma_probe": gamma, "approved_compact_pilot": accepted_pilot,
            "failed_campaign_supplement": failed, "started_utc": probe.utc_now(),
            "universe_complete": False, "analysis_admitted": False})
        for stream in STREAMS[:1] if mode == "pilot" else STREAMS:
            cursor = "MA==" if stream["provider"] == "clob" else None
            page_index, seen_cursors, seen_rows, scope_verified = 0, {cursor}, set(), True
            state = {"id": stream["id"], "provider": stream["provider"], "accepted_pages": 0, "raw_rows": 0,
                     "identity_issue_counts": {}, "pagination_terminal_observed": False,
                     "returned_scope_fields_verified": None if stream["provider"] == "clob" else True}
            states.append(state)
            while not state["pagination_terminal_observed"] or (mode == "pilot" and page_index == 2):
                diagnostic = mode == "pilot" and page_index == 2
                role = "old_cursor_diagnostic" if diagnostic else "fresh_prefix" if mode == "pilot" else "full_walk"
                requested = FAILED_PIN["cursor"] if diagnostic else cursor
                spec = request_spec(stream, requested)
                parsed = summary = None
                for exact_attempt in range(1, CAPS["attempts_per_exact_page"]+1):
                    require(ledger.http_attempts < CAPS["http_attempts"], "http_attempt_limit")
                    if mode == "pilot":
                        require(ledger.http_attempts - prior["http_attempts"] < PILOT["http_attempts"], "pilot_http_attempt_limit")
                    require(ledger.responses < CAPS["total_response_bytes"], "total_response_byte_limit")
                    require(source_snapshot(expected_head) == source, "source_changed")
                    allowance = min(CAPS["individual_response_bytes"], CAPS["total_response_bytes"] - ledger.responses)
                    ledger.reserve("raw", allowance+GZIP_RESERVE)
                    ledger.reserve("control", MAX_CONTROL)
                    legacy.wait_until(last_dispatch + .5, ledger, sleeper)
                    ledger.http_attempts += 1
                    call = ledger.http_attempts-prior["http_attempts"]
                    start = {"event": "START", "call": call, "stream": stream["id"], "page_index": page_index,
                        "coverage_role": role, "exact_page_attempt": exact_attempt, "requested_utc": probe.utc_now(),
                        "request": spec, "request_sha256": digest(encode(spec)), "response_byte_reservation": allowance}
                    ledger.append("attempts.jsonl", start)
                    # Reserve before dispatch. An exception before a sealed receipt never refunds it.
                    before_responses = ledger.responses
                    ledger.responses += allowance
                    last_dispatch = time.monotonic()
                    deadline = last_dispatch + HTTP_SECONDS
                    observed = None
                    finish_written = False
                    try:
                        observed = received_attempt(ledger, call, spec, allowance, transport, deadline)
                        require(observed["response_bytes_charged"] <= allowance
                                and observed["response_bytes_charged"] >= observed["response_bytes_observed"],
                                "attempt_charge_invalid")
                        if not observed["body_complete"]:
                            observed["response_bytes_charged"] = allowance
                        ledger.responses = before_responses + observed["response_bytes_charged"]
                        ledger.observed += observed["response_bytes_observed"]
                        ledger.bodies.append(observed["body"])
                        code = observed["error_code"]
                        if code is None:
                            # Continue the ORIGINAL hard attempt deadline through parsing and normal receipts.
                            with attempt_alarm(ledger, deadline) as (check, _):
                                with gzip.open(output/observed["body"]["path"], "rb") as handle:
                                    body_raw = handle.read(CAPS["individual_response_bytes"]+1)
                                require(len(body_raw) <= CAPS["individual_response_bytes"], "body_expansion_limit")
                                parsed = legacy.page_envelope(body_raw, stream, requested,
                                    {requested} if diagnostic else seen_cursors, set() if diagnostic else seen_rows)
                                row_stream = dict(stream, id="clob_simplified_diagnostic") if diagnostic else stream
                                summary = probe.inspect_rows(parsed["rows"], row_stream, 0 if diagnostic else page_index)
                                require(not summary["scope_conflict_counts"], "gamma_returned_scope_conflict")
                                page = {"event": "PAGE", "call": call, "stream": stream["id"], "page_index": page_index,
                                    "coverage_role": role, "requested_cursor": requested, "accepted": True,
                                    "body": observed["body"], **{key: val for key, val in parsed.items() if key != "rows"},
                                    "row_summary": legacy.compact_summary(summary)}
                                ledger.append("pages.jsonl", page)
                                check()
                                finish = {"event": "FINISH", "call": call, "stream": stream["id"], "page_index": page_index,
                                    "coverage_role": role, "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"],
                                    **observed, "accepted": True, "retry_delay_seconds": None}
                                ledger.append("attempts.jsonl", finish)
                                finish_written = True
                                check()
                        else:
                            raise Error(code)
                    except BaseException as exc:
                        code = str(exc) if isinstance(exc, Error) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "attempt_processing_error"
                        interrupted = isinstance(exc, (KeyboardInterrupt, SystemExit)) or (observed is not None and observed["interrupted"])
                        if observed is None or code in {"runtime_limit", "http_attempt_timeout", "interrupted", "attempt_processing_error"}:
                            ledger.responses = before_responses + allowance
                        if observed is None:
                            observed = {"status_code": None, "body": None, "response_bytes_observed": 0,
                                "response_bytes_charged": allowance, "body_complete": False, "transport_dispatched": True,
                                "interrupted": interrupted, "response_close_attempted": False, "response_close_completed": False,
                                "retry_after_header": None}
                        observed.update(error_code=code, response_bytes_charged=ledger.responses-before_responses)
                        retryable = code == "request_transport_error" or (code == "http_status_not_200" and observed["status_code"] in probe.RETRYABLE)
                        delay = None
                        if retryable:
                            try:
                                delay = legacy.retry_delay(observed["retry_after_header"])
                            except Error as bad_retry:
                                code, retryable = str(bad_retry), False
                                observed["error_code"] = code
                        finish = {"event": "INVALIDATE" if finish_written else "FINISH", "call": call,
                            "stream": stream["id"], "page_index": page_index, "coverage_role": role,
                            "retrieved_utc": probe.utc_now(), "request_sha256": start["request_sha256"], **observed,
                            "accepted": False, "retry_delay_seconds": delay}
                        ledger.append("attempts.jsonl", finish, failure=True)
                        waterfalls["http_attempts_this_run"] += 1
                        waterfalls["failed_attempts"] += 1
                        if interrupted:
                            raise KeyboardInterrupt()
                        require(retryable and exact_attempt < CAPS["attempts_per_exact_page"], code)
                        legacy.wait_until(time.monotonic()+delay, ledger, sleeper)
                        continue
                    waterfalls["http_attempts_this_run"] += 1
                    break
                waterfalls["accepted_pages"] += 1
                waterfalls["raw_rows_in_accepted_pages"] += parsed["count"]
                waterfalls["identity_issue_occurrences_seen"] += sum(summary["identity_issue_counts"].values())
                if mode == "pilot":
                    observations.append({key: val for key, val in page.items() if key not in {"event", "stream", "accepted"}})
                if not diagnostic:
                    state["accepted_pages"] += 1; state["raw_rows"] += parsed["count"]
                    counts = Counter(state["identity_issue_counts"]); counts.update(summary["identity_issue_counts"])
                    state["identity_issue_counts"] = dict(counts)
                    state["pagination_terminal_observed"] = parsed["pagination_terminal_observed"]
                    if stream["provider"] == "gamma":
                        scope_verified = scope_verified and summary["returned_scope_fields_verified"]
                        state["returned_scope_fields_verified"] = scope_verified
                    cursor = parsed["next_cursor"]; state["next_cursor"] = cursor
                    seen_cursors.add(cursor); seen_rows.add(parsed["rows_sha256"])
                else:
                    waterfalls["diagnostic_pages"] += 1
                    waterfalls["diagnostic_raw_rows"] += parsed["count"]
                page_index += 1
                if mode == "pilot":
                    require(page_index != 1 or not state["pagination_terminal_observed"], "pilot_first_page_terminal")
                    if page_index == 3:
                        break
            if mode == "pilot":
                require(len(observations) == 3, "pilot_observations_incomplete")
        if mode == "full":
            require(len(states) == 5 and all(row["pagination_terminal_observed"] for row in states), "pagination_incomplete")
        require(sum(row["raw_rows"] for row in states) + waterfalls["diagnostic_raw_rows"] == waterfalls["raw_rows_in_accepted_pages"],
                "count_conservation_failed")
        def recheck():
            require(source_snapshot(expected_head) == source and read_bound(reference)[1] == admission_bound, "source_or_admission_changed")
            require(inventory_prior(admission["prior_ledger"], admission_path, output) == prior, "prior_inputs_changed")
            require(verify_failed(admission["failed_campaign"], prior) == failed, "failed_inputs_changed")
            require(legacy.verify_probe(admission["legacy_gamma_probe"], source, prior) == gamma, "gamma_probe_changed")
            verify_compiler_producers(prior, source)
            if mode == "full":
                require(verify_pilot(admission["pilot_acceptance"], source, prior) == accepted_pilot, "compact_pilot_changed")
        ledger.reopen(); recheck()
        inventory = b"".join(encode({"path": name, **bound}) for name, bound in sorted(ledger.files.items()))
        inventory_bound = ledger.write("artifact_inventory.jsonl", inventory)
        status = PILOT_SUCCESS if mode == "pilot" else FULL_SUCCESS
        manifest = {"schema_version": START_SCHEMA, "status": status, "mode": mode, "source": source,
            "admission": admission_bound, "caps": CAPS, "pilot_caps": PILOT, "scope": SCOPE,
            "streams": states, "observations": observations, "waterfall": dict(waterfalls),
            "usage": ledger.usage(final_reservation=CONTROL_RESERVE), "artifact_inventory": inventory_bound,
            "failed_campaign_supplement": failed, "legacy_gamma_probe": gamma, "approved_compact_pilot": accepted_pilot,
            "completed_utc": probe.utc_now(), "old_prefix_is_new_stream_coverage": False,
            "universe_complete": False, "analysis_admitted": False, "family_or_payout_admitted": False,
            "normalized_labels_emitted": False, "snapshot_consistency_certified": False}
        manifest_bound = ledger.json("manifest.json", manifest)
        ledger.complete({"schema_version": "polymarket_market_metadata_campaign_completion_v4", "mode": mode,
            "status": status, "manifest": manifest_bound, "universe_complete": False, "analysis_admitted": False}, recheck)
        return {"status": status, "mode": mode, "usage": manifest["usage"], "streams": states,
                "universe_complete": False, "analysis_admitted": False}
    except BaseException as exc:
        code = str(exc) if isinstance(exc, Error) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "internal_runtime_error"
        failure = {"schema_version": START_SCHEMA, "status": "failed_unaccepted", "mode": mode, "error_code": code,
            "usage": ledger.usage(), "streams": states, "waterfall": dict(waterfalls),
            "universe_complete": False, "analysis_admitted": False}
        try:
            ledger.json("FAILURE.json", failure, failure=True)
        except BaseException:
            pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        return failure


@contextmanager
def process_limits(mode):
    with legacy.process_limits() as debit:
        if mode == "pilot":
            delay, _ = signal.getitimer(signal.ITIMER_REAL)
            signal.setitimer(signal.ITIMER_REAL, min(delay, PILOT["runtime_seconds"]))
        def debit_profile(prior_seconds, started):
            debit(prior_seconds, started)
            if mode == "pilot":
                delay, _ = signal.getitimer(signal.ITIMER_REAL)
                remaining = PILOT["runtime_seconds"] - (time.monotonic()-started)
                require(remaining > 0, "runtime_limit")
                signal.setitimer(signal.ITIMER_REAL, min(delay, remaining))
        yield debit_profile


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("pilot", "full"))
    parser.add_argument("--admission", type=Path, required=True)
    parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    require_production_host()
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        with process_limits(args.mode) as debit:
            result = run_campaign(args.run_dir, args.expected_head, args.admission, args.admission_sha256, args.mode,
                                  debit_prior_runtime=debit)
    except Error as exc:
        result = {"status": "failed_unaccepted", "error_code": str(exc), "analysis_admitted": False}
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        result = {"status": "failed_unaccepted", "error_code": "admission_or_environment_error", "analysis_admitted": False}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {PILOT_SUCCESS, FULL_SUCCESS} else 1


if __name__ == "__main__":
    raise SystemExit(main())



