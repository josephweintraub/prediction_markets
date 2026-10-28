"""One fresh, root-admitted V3 build, with the executable copy inside 64 MiB.

No downloader, network client, resumable controller or sample publisher. The
unchanged v1 verifier supplies compiler invocation and exact build comparisons.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import verify_polymarket_v3_source_build_v1 as verifier

SCHEMA = "polymarket_v3_source_build_execution_admission_v2"
STATUS = "root_zero_api_v3_source_build_execution_admitted"
RESULT_SCHEMA = "polymarket_v3_source_build_execution_v2"
SUCCESS = "root_verified_v3_exchange_build_stage_only"
RUN_ROOT = Path("/mnt/data/runs")
OPERATIONS = RUN_ROOT / "2026-10-27_polymarket_oct1_finalization_v1"
OUTPUT = RUN_ROOT / "2026-10-28_polymarket_v3_source_build_v2"
METADATA_RUN = RUN_ROOT / "2026-10-28_polymarket_market_metadata_full_v6"
OLD_QA = RUN_ROOT / "2026-10-27_polymarket_metadata_v5_caller_linux_qa_v1"
METADATA_HEAD = "d28bca69b1282252ea8b76bba7073a2ad7b4ba9e"
ANCHOR = "2026-10-28T17:45:44Z"
BINARY_BYTES = 16865544
ACQUISITION_ROOT = RUN_ROOT / "2026-10-27_polymarket_solc_binary_v2"
ACQUISITION_ACCEPTANCE_PIN = (1716, "dd43c90be3cde023db8621e67ff92e51173e2c1b7a21ab804d592f4bf0c30146")
SOURCE_FILES = (*verifier.SOURCE_FILES, "scripts/prepare_polymarket_v3_source_build_v2.py",
    "tests/test_prepare_polymarket_v3_source_build_v2.py",
    "docs/analysis_specs/polymarket_v3_source_build_execution_v2.md")
CAPS = {**verifier.CAPS, "control_bytes": 1024**2,
    "shared_external_control_bytes": 2 * 1024**2, "shared_external_control_refs": 256,
    "metadata_http_attempts": 100_000, "metadata_response_bytes": 96 * 1024**3,
    "metadata_runtime_seconds": 86400, "metadata_artifact_bytes": 20_000_000_000,
    "metadata_raw_bytes": 12_000_000_000, "metadata_control_bytes": 1_000_000_000,
    "metadata_other_bytes": 3_500_000_000}
MINIMUM = {"http_attempts": 60715, "response_bytes_charged": 50614490077,
    "runtime_seconds": 59400, "retained_bytes_charged":
        {"raw": 4481019912, "control": 288982658, "other": 16865544}}
NEW_RESERVATION = {"raw": 0, "control": CAPS["control_bytes"],
    "other": CAPS["all_artifact_bytes"] - CAPS["control_bytes"]}
UUID = "d0cd087b-94c4-428c-bae9-ae28929059f6"
SERIAL = "vol0acbba4565a64f792"
SHA = re.compile(r"[0-9a-f]{64}")
HEAD = re.compile(r"[0-9a-f]{40}")
Error, require, encode, decode, digest = verifier.Blocked, verifier.require, verifier.encode, verifier.decode, verifier.digest


def ref_schema(value):
    require(type(value) is dict and set(value) == {"path", "bytes", "sha256"}
        and type(value["path"]) is str and type(value["bytes"]) is int and value["bytes"] >= 0
        and type(value["sha256"]) is str and SHA.fullmatch(value["sha256"]), "reference_schema")
    return value


class Inputs:
    def __init__(self, check):
        self.check, self.saved = check, {}

    def read(self, ref, maximum=1024**2):
        self.check(); ref_schema(ref); path = Path(ref["path"])
        raw, bound = verifier.bound_read(path, ref["sha256"], maximum=maximum, size=ref["bytes"])
        require(str(path) not in self.saved or self.saved[str(path)] == bound, "bound_input_changed")
        self.saved[str(path)] = bound
        self.check(); return raw

    def json(self, ref):
        return decode(self.read(ref))

    def reopen(self):
        for path, expected in list(self.saved.items()):
            self.check()
            _, bound = verifier.bound_read(Path(path), expected["sha256"],
                maximum=max(expected["identity"]["bytes"], 1), size=expected["identity"]["bytes"])
            require(bound == expected, "final_bound_input_changed")


def source_snapshot(head, inputs):
    base = verifier.source_snapshot(head)
    result = {"head": head, "files": {}}
    for name in SOURCE_FILES:
        path = REPO / name
        before = verifier.identity(path)
        require(0 < before["bytes"] <= 1024**2, "source_size_limit")
        raw = subprocess.check_output(["git", "cat-file", "blob", head + ":" + name],
            cwd=REPO, stderr=subprocess.DEVNULL, timeout=10)
        ref = {"path": str(path), "bytes": len(raw), "sha256": digest(raw)}
        require(inputs.read(ref) == raw, "source_blob_differs")
        if name in base["files"]:
            require(base["files"][name] == ref["sha256"], "verifier_source_differs")
        result["files"][name] = {"bytes": ref["bytes"], "sha256": ref["sha256"]}
    require(verifier.source_snapshot(head) == base, "source_changed")
    inputs.reopen()
    return result


def usage(value):
    require(type(value) is dict and set(value) == set(MINIMUM), "usage_schema")
    for key, ceiling in (("http_attempts", CAPS["metadata_http_attempts"]),
                         ("response_bytes_charged", CAPS["metadata_response_bytes"]),
                         ("runtime_seconds", CAPS["metadata_runtime_seconds"])):
        item = value[key]
        require(type(item) in (int, float) and math.isfinite(item) and MINIMUM[key] <= item <= ceiling
            and (key == "runtime_seconds" or type(item) is int), "usage_below_floor_or_limit")
    amounts = value["retained_bytes_charged"]
    require(type(amounts) is dict and set(amounts) == {"raw", "control", "other"}, "retained_schema")
    for key, ceiling in (("raw", CAPS["metadata_raw_bytes"]), ("control", CAPS["metadata_control_bytes"]),
                         ("other", CAPS["metadata_other_bytes"])):
        require(type(amounts[key]) is int and MINIMUM["retained_bytes_charged"][key] <= amounts[key] <= ceiling,
            "retained_floor_or_limit")
    require(sum(amounts.values()) <= CAPS["metadata_artifact_bytes"], "metadata_artifact_limit")
    return value


def usage_asof(value):
    require(type(value) is str and len(value) <= 64, "formal_usage_asof_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0, "formal_usage_asof_invalid")
        seconds = parsed.timestamp()
    except (ValueError, OverflowError):
        raise Error("formal_usage_asof_invalid") from None
    require(math.isfinite(seconds), "formal_usage_asof_invalid")
    return seconds


def terminal(value, inputs):
    require(type(value) is dict and set(value) == {"result", "complete"}, "terminal_reference_schema")
    result, marker = inputs.json(value["result"]), inputs.json(value["complete"])
    require(result.get("schema_version") == "polymarket_metadata_v6_terminal_acceptance_v1"
        and result.get("status") == "root_reopened_completed_raw_pagination_only"
        and result.get("metadata_run_dir") == str(METADATA_RUN) and result.get("producer_head") == METADATA_HEAD
        and result.get("owned_producers_drained") is True and result.get("metadata_pagination_complete") is True
        and all(result.get(key) is False for key in ("metadata_universe_admitted", "outcomes_admitted",
            "categories_admitted", "analysis_admitted", "final_sample_admitted")), "terminal_scope_differs")
    expected = {"path": "RESULT.json", "bytes": value["result"]["bytes"], "sha256": value["result"]["sha256"], "category": "control"}
    require(Path(value["result"]["path"]).name == "RESULT.json"
        and Path(value["complete"]["path"]) == Path(value["result"]["path"]).with_name("COMPLETE.json")
        and marker.get("schema_version") == "polymarket_metadata_v6_terminal_acceptance_completion_v1"
        and marker.get("status") == result["status"] and marker.get("result") == expected
        and all(marker.get(key) is False for key in ("metadata_universe_admitted", "analysis_admitted", "final_sample_admitted")),
        "terminal_marker_differs")
    bindings = result.get("bindings")
    require(type(bindings) is dict and set(bindings) == {"manifest", "complete", "producer_start", "launch",
        "caller_exit", "admission", "root_drain"}, "terminal_bindings_schema")
    for ref in bindings.values():
        inputs.read(ref)
    require(marker.get("root_drain") == {"path": "root_drain.json", "bytes": bindings["root_drain"]["bytes"],
        "sha256": bindings["root_drain"]["sha256"], "category": "control"}, "terminal_drain_marker_differs")
    source = result.get("source")
    require(type(source) is dict and set(source) == {"head", "files"} and HEAD.fullmatch(source["head"])
        and type(source["files"]) is dict and 0 < len(source["files"]) <= 64, "terminal_producer_source")
    for name, item in source["files"].items():
        require(type(name) is str and not Path(name).is_absolute() and ".." not in Path(name).parts
            and type(item) is dict and set(item) == {"bytes", "sha256"}
            and type(item["bytes"]) is int and 0 < item["bytes"] <= 1024**2
            and type(item["sha256"]) is str and SHA.fullmatch(item["sha256"]), "terminal_source_binding")
        size = subprocess.check_output(["git", "cat-file", "-s", source["head"] + ":" + name],
            cwd=REPO, stderr=subprocess.DEVNULL, timeout=10).strip()
        require(size.isdigit() and int(size) == item["bytes"], "terminal_producer_size_differs")
        raw = subprocess.check_output(["git", "cat-file", "blob", source["head"] + ":" + name],
            cwd=REPO, stderr=subprocess.DEVNULL, timeout=10)
        require(len(raw) == item["bytes"] and digest(raw) == item["sha256"], "terminal_producer_blob_differs")
    shared = result.get("shared_external_controls")
    require(type(shared) is dict and set(shared) == {"references", "actual_bytes", "actual_refs", "scope", "own_output_control_reservation_bytes"}
        and shared["scope"] == "bound_external_controls_excludes_this_acceptor_output_directory"
        and type(shared["references"]) is list and len(shared["references"]) <= 256, "terminal_external_closure_schema")
    require(type(shared["own_output_control_reservation_bytes"]) is int
        and 0 <= shared["own_output_control_reservation_bytes"] <= 262144, "terminal_output_reservation")
    required = [ref_schema(ref) for ref in shared["references"]]
    require(len({ref["path"] for ref in required}) == len(required)
        and shared["actual_bytes"] == sum(ref["bytes"] for ref in required)
        and shared["actual_refs"] == len(required), "terminal_external_closure_totals")
    acceptor = Path(value["result"]["path"]).parent
    own = [value["result"], value["complete"], bindings["root_drain"]]
    require(Path(bindings["root_drain"]["path"]) == acceptor / "root_drain.json"
        and {p.name for p in acceptor.iterdir()} == {"RESULT.json", "COMPLETE.json", "root_drain.json"}
        and all(p.is_file() and not p.is_symlink() for p in acceptor.iterdir())
        and sum(ref["bytes"] for ref in own) <= 262144, "terminal_output_closure_differs")
    return usage(result["prior_usage"]), required + own, usage_asof(result.get("prior_usage_as_of_utc"))


def external_controls(value, inputs, required):
    ref_schema(value); ledger = inputs.json(value)
    require(type(ledger) is dict and set(ledger) == {"schema_version", "references", "future_reserved_bytes", "future_reserved_refs"}
        and ledger["schema_version"] == "polymarket_source_build_shared_external_controls_v2"
        and type(ledger["references"]) is list and type(ledger["future_reserved_bytes"]) is int
        and type(ledger["future_reserved_refs"]) is int and ledger["future_reserved_bytes"] >= CAPS["control_bytes"]
        and ledger["future_reserved_refs"] >= 16, "external_ledger_schema")
    paths = set(); total = value["bytes"]; refs = {}
    for ref in ledger["references"]:
        ref_schema(ref); require(ref["path"] not in paths and ref["path"] != value["path"], "external_duplicate")
        require(Path(ref["path"]).is_absolute(), "external_path")
        inputs.read(ref, maximum=CAPS["shared_external_control_bytes"]); paths.add(ref["path"]); refs[ref["path"]] = ref; total += ref["bytes"]
    require(all(refs.get(ref["path"]) == ref for ref in required), "external_required_closure_missing")
    require(OLD_QA.is_dir() and OLD_QA.resolve() == OLD_QA, "old_qa_root_invalid")
    expected = {path for path in refs if OLD_QA in Path(path).parents}; observed = set()
    entries = 0
    for path in OLD_QA.rglob("*"):
        entries += 1; require(entries <= 4096, "old_qa_entry_limit")
        inputs.check(); require(not path.is_symlink() and (path.is_file() or path.is_dir()), "old_qa_tree_type")
        if path.is_file(): observed.add(str(path))
        require(len(observed) <= CAPS["shared_external_control_refs"], "old_qa_tree_limit")
    require(observed == expected, "old_qa_set_changed")
    require(total + ledger["future_reserved_bytes"] <= CAPS["shared_external_control_bytes"]
        and len(paths) + 1 + ledger["future_reserved_refs"] <= CAPS["shared_external_control_refs"], "shared_external_limit")
    return {"actual_bytes": total, "actual_refs": len(paths) + 1, "future_reserved_bytes": ledger["future_reserved_bytes"],
        "future_reserved_refs": ledger["future_reserved_refs"]}


def host_proof(value, head, now, inputs):
    host = inputs.json(value)
    require(host.get("schema_version") == "polymarket_v3_source_build_fresh_host_v2"
        and host.get("status") == "root_verified_current_host_drained" and host.get("source_head") == head
        and host.get("source_clean") is True and host.get("data_uuid") == UUID and host.get("data_serial") == SERIAL
        and host.get("data_mount") == "/mnt/data" and host.get("agents_need_EC2") is False
        and host.get("live_owned_births") == 0 and type(host.get("live_owned_births")) is int
        and host.get("unknown_owned_births") == 0 and type(host.get("unknown_owned_births")) is int
        and host.get("relevant_compute_notebook_transfer_consumers") == [], "fresh_host_proof_differs")
    checked = datetime.fromisoformat(host["checked_utc"].replace("Z", "+00:00"))
    require(checked.tzinfo is not None and 0 <= now - checked.timestamp() <= 300, "host_proof_stale")
    require(host.get("boot_id") == Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        and type(host.get("data_st_dev")) is int and host["data_st_dev"] == Path("/mnt/data").stat().st_dev, "host_physical_identity_differs")
    return host


def admit(value, head, output, inputs, now):
    require(type(value) is dict and set(value) == {"schema_version", "status", "consumer_head", "run_dir", "caps",
        "terminal_acceptance", "acquisition", "source_packages", "runtime_observation", "source_commit", "host_proof",
        "external_controls", "prior_usage", "fresh_preparation_anchor_utc"}
        and value["schema_version"] == SCHEMA and value["status"] == STATUS and value["consumer_head"] == head
        and value["run_dir"] == str(output) and value["caps"] == CAPS and value["fresh_preparation_anchor_utc"] == ANCHOR,
        "root_admission_differs")
    inherited, required_external, inherited_asof = terminal(value["terminal_acceptance"], inputs)
    prior = usage(value["prior_usage"])
    for key in ("http_attempts", "response_bytes_charged", "runtime_seconds"):
        require(prior[key] >= inherited[key], "prior_usage_refunded")
    require(all(prior["retained_bytes_charged"][key] >= inherited["retained_bytes_charged"][key]
        for key in NEW_RESERVATION), "prior_retained_refunded")
    amounts = prior["retained_bytes_charged"]
    require(amounts["control"] + NEW_RESERVATION["control"] <= CAPS["metadata_control_bytes"]
        and amounts["other"] + NEW_RESERVATION["other"] <= CAPS["metadata_other_bytes"]
        and sum(amounts.values()) + sum(NEW_RESERVATION.values()) <= CAPS["metadata_artifact_bytes"], "shared_metadata_reservation_unavailable")
    anchor = datetime.fromisoformat(ANCHOR.replace("Z", "+00:00")).timestamp()
    require(anchor <= inherited_asof <= now, "formal_usage_asof_outside_preparation")
    require(prior["runtime_seconds"] >= inherited["runtime_seconds"] + math.ceil(now-inherited_asof),
        "prior_runtime_nonoverlap_missing")
    require(now >= anchor and prior["runtime_seconds"] >= MINIMUM["runtime_seconds"] + math.ceil(now-anchor)
        and prior["runtime_seconds"] + CAPS["runtime_seconds"] <= CAPS["metadata_runtime_seconds"], "cumulative_runtime_limit")
    source = source_snapshot(head, inputs)
    commit = inputs.json(value["source_commit"])
    require(commit.get("schema_version") == "polymarket_v3_source_build_commit_v2"
        and commit.get("status") == "clean_committed_and_pushed" and commit.get("source_head") == head
        and commit.get("git_status_empty") is True and commit.get("origin_commit_matches") is True
        and commit.get("source_files") == source["files"], "source_commit_proof_differs")
    host = host_proof(value["host_proof"], head, now, inputs)
    require(type(value["source_packages"]) is dict and set(value["source_packages"]) == {"old", "new"}, "source_packages_schema")
    packages, augmented = {}, {}
    for era in ("old", "new"):
        ref = value["source_packages"][era]; pin = verifier.PINS[era]
        require((ref_schema(ref)["bytes"], ref["sha256"]) == (pin["bytes"], pin["sha256"]), "source_package_pin")
        packages[era] = decode(inputs.read(ref)); augmented[era] = verifier.package_input(packages[era], era)
    ref = value["runtime_observation"]
    require((ref_schema(ref)["bytes"], ref["sha256"]) == (143415, verifier.RUNTIME_SHA), "runtime_observation_pin")
    runtime = decode(inputs.read(ref))
    require(runtime.get("chain_id") == 137 and runtime.get("schema_version") == "polymarket_v3_runtime_probe_v1"
        and runtime.get("source_equivalence_admitted") is False, "runtime_observation_scope")
    acquisition = value["acquisition"]
    require(type(acquisition) is dict and set(acquisition) == {"result", "complete", "root_acceptance", "binary"}, "acquisition_schema")
    result, marker, acceptance = (inputs.json(acquisition[key]) for key in ("result", "complete", "root_acceptance"))
    require((acquisition["root_acceptance"]["bytes"], acquisition["root_acceptance"]["sha256"]) == ACQUISITION_ACCEPTANCE_PIN,
        "acquisition_root_acceptance_pin")
    require(result.get("status") == "authenticated_solc_binary_not_executed" and result.get("compiler_sha256") == verifier.COMPILER_SHA
        and result.get("compiler_executed") is False and marker.get("status") == result["status"]
        and marker.get("result") == {"path": "RESULT.json", "bytes": acquisition["result"]["bytes"], "sha256": acquisition["result"]["sha256"]}
        and acceptance.get("schema_version") == "polymarket_solc_v2_root_acceptance_v1"
        and acceptance.get("owned_pid_absent") is True and acceptance.get("owned_group_and_session_empty") is True
        and acceptance.get("compiler_executed") is False and acceptance.get("sample_admitted") is False
        and all(acceptance.get(key) == acquisition[key] for key in ("result", "complete", "binary")), "acquisition_acceptance_differs")
    for key in ("launch", "exit"):
        inputs.json(acceptance[key])
    binary_ref = ref_schema(acquisition["binary"])
    require(Path(binary_ref["path"]) == ACQUISITION_ROOT / "attempt01.part"
        and binary_ref["bytes"] == BINARY_BYTES and binary_ref["sha256"] == verifier.COMPILER_SHA, "compiler_pin")
    reported = result.get("binary", {})
    require(reported == marker.get("binary") and reported.get("path") == "attempt01.part"
        and reported.get("bytes") == binary_ref["bytes"] and reported.get("sha256") == binary_ref["sha256"], "acquisition_binary_differs")
    binary = inputs.read(binary_ref, 64 * 1024**2)
    require(not verifier.identity(Path(binary_ref["path"]))["mode"] & 0o111, "original_compiler_must_remain_nonexecutable")
    # Acquisition controls are read above; all of them, including the original
    # root acceptance and its launch/exit, belong to this same shared pool.
    acquisition_controls = [acquisition[key] for key in ("result", "complete", "root_acceptance")]
    acquisition_controls.extend(acceptance[key] for key in ("launch", "exit"))
    closure = external_controls(value["external_controls"], inputs,
        required_external + acquisition_controls + [value["source_commit"], value["host_proof"]])
    return prior, source, packages, augmented, runtime, binary, closure, host


def category(name):
    name = name.removeprefix(".").removesuffix(".pending")
    return "other" if name == "solc" or name.endswith((".stdout", ".stderr", ".standard-input.json", ".proof.json")) else "control"


class Artifacts(verifier.Artifacts):
    """The unchanged verifier's writer, with one inclusive copy-aware budget."""
    def __init__(self, root, clock, check, prior, admission_bytes):
        super().__init__(root, clock); self.outer_check, self.prior = check, prior
        self.extra_category = "other"; self.admission_bytes = admission_bytes

    def check(self, extra=0, *, terminal=False, failure=False):
        if not failure: self.outer_check()
        super().check(extra, terminal=terminal, failure=failure)
        amounts = {"control": self.admission_bytes, "other": 0}
        for path in self.root.iterdir(): amounts[category(path.name)] += verifier.identity(path)["bytes"]
        amounts[self.extra_category] += extra
        require(amounts["control"] <= CAPS["control_bytes"], "stage_control_limit")
        require(amounts["other"] <= NEW_RESERVATION["other"], "stage_other_limit_including_copy")
        require(sum(amounts.values()) <= CAPS["all_artifact_bytes"], "stage_copy_inclusive_artifact_limit")
        prior = self.prior["retained_bytes_charged"]
        require(prior["control"] + NEW_RESERVATION["control"] <= CAPS["metadata_control_bytes"]
            and prior["other"] + NEW_RESERVATION["other"] <= CAPS["metadata_other_bytes"]
            and sum(prior.values()) + sum(NEW_RESERVATION.values()) <= CAPS["metadata_artifact_bytes"], "shared_metadata_limit")

    def write(self, name, raw, **kwargs):
        old = self.extra_category; self.extra_category = category(name)
        try: return super().write(name, raw, **kwargs)
        finally: self.extra_category = old


def drained(output):
    proofs = []
    for era in ("version", "old", "new"):
        birth = decode((output / (era + ".PROCESS.json")).read_bytes())
        exited = decode((output / (era + ".EXIT.json")).read_bytes())
        require(exited.get("birth") == birth and exited.get("exit_code") == 0
            and exited.get("owned_group_drained") is True and verifier.group_absent(birth["pid"]), "compiler_group_not_drained")
        proofs.append({"label": era, "birth": birth, "owned_group_absent": True})
    return proofs


def run(admission_path, admission_sha, head, output, *, check_only=False,
        executor=verifier.real_executor, clock=time.monotonic, utc=time.time):
    started = clock(); prior = None
    def check():
        require(clock() - started < CAPS["runtime_seconds"], "whole_runtime_limit")
        if prior is not None:
            require(prior["runtime_seconds"] + clock() - started < CAPS["metadata_runtime_seconds"], "cumulative_runtime_limit")
    inputs = Inputs(check); path, output = Path(admission_path), Path(output)
    require(HEAD.fullmatch(head or "") and SHA.fullmatch(admission_sha or ""), "cli_pins")
    require(output == OUTPUT and output.parent.resolve() == output.parent and not output.exists() and not output.is_symlink(), "fresh_output_required")
    ref = {"path": str(path), "bytes": verifier.identity(path)["bytes"], "sha256": admission_sha}
    value = inputs.json(ref)
    prior, source, packages, augmented, runtime, binary, closure, host = admit(value, head, output, inputs, utc())
    require(verifier.shared.resource_state()["ram_bytes"]["MemAvailable"] >= CAPS["address_space_bytes"], "available_ram_floor")
    require(verifier.shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CAPS["all_artifact_bytes"], "free_space_floor")
    inputs.reopen(); host_proof(value["host_proof"], head, utc(), inputs); check()
    if check_only:
        return {"status": "source_build_readiness_only_not_executed", "api_calls": 0, "compiler_executed": False,
            "output_written": False, "prior_usage": prior, "new_reservation": NEW_RESERVATION}
    output.mkdir(mode=0o700); writer = Artifacts(output, clock, check, prior, ref["bytes"])
    result = {"schema_version": RESULT_SCHEMA, "status": "blocked", "source": source, "admission": ref,
        "caps": CAPS, "prior_usage": prior, "new_usage": {"http_attempts": 0, "response_bytes_charged": 0,
            "runtime_seconds_reserved": CAPS["runtime_seconds"], "retained_bytes_charged": NEW_RESERVATION},
        "external_control_closure": closure, "compiler_executed": False, "api_calls": 0,
        "dependencies_admitted": False, "token_mapping_admitted": False, "outcomes_admitted": False,
        "analysis_admitted": False, "final_sample_admitted": False, "ownership_unknown": True}
    result.update(source_content_keccak_independently_verified_here=False,
        source_binding_basis="Complete-response SHA256 pins, literal sources and authenticated compiler/runtime comparison")
    result["cumulative_usage"] = {key: prior[key] for key in ("http_attempts", "response_bytes_charged")}
    result["cumulative_usage"].update(runtime_seconds=prior["runtime_seconds"] + CAPS["runtime_seconds"],
        retained_bytes_charged={key: prior["retained_bytes_charged"][key] + NEW_RESERVATION[key] for key in NEW_RESERVATION})
    try:
        writer.write("START.json", encode(result))
        writer.check(len(binary)); copy = writer.write("solc", binary); del binary
        executable = output / "solc"; executable.chmod(0o500)
        compiler = verifier.compiler_binding(executable, check)
        require(copy["bytes"] == compiler["identity"]["bytes"] == BINARY_BYTES and compiler["sha256"] == verifier.COMPILER_SHA, "executable_copy_differs")
        inputs.reopen(); writer.reopen(); check()
        out, err = verifier.invoke("version", [str(executable), "--version"], None, writer,
            CAPS["version_seconds"], executor, compiler, check)
        require((output / out["path"]).read_bytes() == verifier.VERSION_STDOUT and err["bytes"] == 0, "compiler_version_differs")
        builds = []
        for era in ("old", "new"):
            standard, sources = augmented[era]
            stdin = writer.write(era + ".standard-input.json", encode(standard))
            stdout, stderr = verifier.invoke(era, [str(executable), "--standard-json", "--no-import-callback"],
                output / stdin["path"], writer, CAPS["child_seconds"], executor, compiler, check)
            result["compiler_executed"] = True
            compiled = decode((output / stdout["path"]).read_bytes())
            proof = verifier.verify_build(compiled, packages[era], runtime, era); del compiled
            proof.update(literal_source_hashes=sources, standard_input=stdin, compiler_stdout=stdout, compiler_stderr=stderr)
            writer.write(era + ".proof.json", encode(proof)); builds.append(proof)
        child_proofs = drained(output)
        result.update(status=SUCCESS, builds=builds, executable_copy={**copy, "binding": compiler},
            owned_children=child_proofs, owned_children_drained=True, ownership_unknown=False,
            runtime_source_build_verified=True, onchain_creation_authenticated=False,
            observed_runtime_seconds=clock()-started,
            cumulative_usage_as_of_utc=datetime.fromtimestamp(utc(), timezone.utc).isoformat())
        saved = writer.write("RESULT.json", encode(result))
        def final():
            require(source_snapshot(head, inputs) == source, "final_source_changed")
            require(verifier.compiler_binding(executable, check) == compiler, "final_executable_changed")
            require(drained(output) == child_proofs, "final_child_proofs_changed")
            terminal(value["terminal_acceptance"], inputs)
            require(external_controls(value["external_controls"], inputs, []) == closure, "final_external_closure_changed")
            inputs.reopen(); writer.reopen(); check()
        writer.write("COMPLETE.json", encode({"schema_version": RESULT_SCHEMA, "status": SUCCESS, "result": saved,
            "runtime_source_build_verified": True, "analysis_admitted": False, "final_sample_admitted": False}), final_check=final)
        return result
    except BaseException as exc:
        receipt = getattr(exc, "compiler_receipt", {})
        result.update(status="blocked", error_code=str(exc) if isinstance(exc, Error) else "source_build_failure_or_interruption",
            ownership_unknown=receipt.get("owned_group_drained") is not True,
            owned_children_drained=receipt.get("owned_group_drained") is True,
            failed_compiler_receipt=receipt, observed_elapsed_seconds=clock()-started,
            cumulative_usage_as_of_utc=datetime.fromtimestamp(utc(), timezone.utc).isoformat())
        try: writer.write("FAILED.json", encode(result), failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission", type=Path, required=True); parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True); parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--check-only", action="store_true"); args = parser.parse_args(argv)
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
    try:
        with verifier.process_limits():
            result = run(args.admission, args.admission_sha256, args.expected_head, args.run_dir, check_only=args.check_only)
    except (Error, OSError, KeyError, ValueError):
        result = {"status": "blocked", "error_code": "root_admission_or_environment_blocked", "api_calls": 0,
            "compiler_executed": None, "ownership_unknown": True}
    print(json.dumps({key: result.get(key) for key in ("status", "api_calls", "compiler_executed", "ownership_unknown")}, sort_keys=True))
    return 0 if result["status"] in {SUCCESS, "source_build_readiness_only_not_executed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
