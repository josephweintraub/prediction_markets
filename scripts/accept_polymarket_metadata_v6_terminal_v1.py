"""Zero-request, root-executed acceptance of the completed raw metadata walk.

Compressed retained artifacts are authenticated, not decoded as new market or
payout evidence. The native universe, token spine, categories and sample remain
unadmitted. This stage makes its own fresh process/transfer-drain observation.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import math
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
import uuid

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_market_metadata_campaign_v6 as acquisition

SCHEMA = "polymarket_metadata_v6_terminal_acceptance_v1"
ADMISSION = "polymarket_metadata_v6_terminal_acceptance_admission_v1"
STATUS = "root_reopened_completed_raw_pagination_only"
RUN_ROOT = Path("/mnt/data/runs")
OPERATIONS = RUN_ROOT / "2026-10-27_polymarket_oct1_finalization_v1"
RUN = RUN_ROOT / "2026-10-28_polymarket_market_metadata_full_v6"
HEAD = "d28bca69b1282252ea8b76bba7073a2ad7b4ba9e"
PINS = {"manifest": (10801, "017e0462f334c2e77211ec3359f0be7c295fec1ca7767f571d1e7711031a1d5e"),
    "complete": (359, "7e71b3109c009a4e0d21fb6a24a9b170a91ddca990e945e0bfcddac68176abab"),
    "caller_exit": (4492, "e119a7cfd2f0986c4114608fd49eaa1a93203312394f4614c8765079b20fd91d")}
BASE = {"http_attempts": 60715, "response_bytes_charged": 50614490077, "runtime_seconds": 59400,
    "retained_bytes_charged": {"raw": 4481019912, "control": 288982658, "other": 16865544}}
ANCHOR = datetime(2026, 10, 28, 17, 45, 44, tzinfo=timezone.utc)
CAPS = {"runtime_seconds": 600, "address_space_bytes": 8 * 1024**3,
    "input_read_bytes": 16 * 1024**3, "all_new_artifact_bytes": 256 * 1024,
    "single_new_artifact_bytes": 64 * 1024, "minimum_free_bytes": 20_000_000_000,
    "spill_bytes": 0, "http_attempts": 0, "processes": 20_000,
    "receipt_walk_entries": 100_000, "external_references": 256, "external_bytes": 2 * 1024**2}
SOURCE_FILES = (*acquisition.SOURCE_FILES, "scripts/accept_polymarket_metadata_v6_terminal_v1.py",
    "tests/test_accept_polymarket_metadata_v6_terminal_v1.py", "docs/analysis_specs/polymarket_metadata_spine_pilot_v2.md")
FALSE_SCOPE = {"metadata_universe_admitted": False, "outcomes_admitted": False,
    "categories_admitted": False, "analysis_admitted": False, "final_sample_admitted": False}
encode, digest, require = acquisition.encode, acquisition.digest, acquisition.require
Error = acquisition.Error
COUNTS = {"v4_files": 11460, "v5_files": 22250, "prefix_pages": 33701, "prefix_rows": 7988439,
    "new_pages": 22150, "new_rows": 2214843, "inventory_records": 22153, "old_process_receipts": 603,
    "old_external_refs": 36, "old_external_bytes": 577036, "external_refs": 52, "external_actual_bytes": 658630}
OLD_QA = RUN_ROOT / "2026-10-27_polymarket_metadata_v5_caller_linux_qa_v1"
OLD_ROSTER_PIN = (245096, "8db35c9fd233a087a7a4a8ccbd760f2e6c4a15969f7cdb0e707a13a23f26063f")
DISPATCH_PIN = (2996, "b03214498492769a801d9d9a737e9753a617bc14a561c082240b5b475110692c")
OS_SERVICES = {('/usr/lib/systemd/systemd-journald',0),('/sbin/multipathd',0),('/usr/lib/systemd/systemd-udevd',0),
    ('/usr/lib/systemd/systemd-resolved',991),('/usr/lib/systemd/systemd-networkd',998),('/usr/sbin/acpid',0),
    ('/usr/sbin/cron',0),('@dbus-daemon',101),('/usr/sbin/irqbalance',0),('/usr/lib/polkit-1/polkitd',989),
    ('/snap/amazon-ssm-agent/13349/amazon-ssm-agent',0),('/snap/snapd/current/usr/lib/snapd/snapd',0),
    ('/usr/lib/systemd/systemd-logind',0),('/usr/libexec/udisks2/udisksd',0),('/sbin/agetty',0),
    ('/usr/sbin/chronyd',110),('/usr/sbin/rsyslogd',102),('/usr/sbin/ModemManager',0),('/usr/libexec/fwupd/fwupd',0)}
OS_PROGRAMS = {"/usr/bin/networkd-dispatcher", "/usr/share/unattended-upgrades/unattended-upgrade-shutdown", "/usr/bin/unattended-upgrade"}
PROCESS_ROOTS = (OPERATIONS, RUN, RUN_ROOT / "2026-10-27_polymarket_market_metadata_full_v4",
    RUN_ROOT / "2026-10-27_polymarket_market_metadata_full_v5", RUN_ROOT / "2026-10-27_polymarket_exact_header_campaign_v4", OLD_QA)


def ref(value): return {key: value[key] for key in ("path", "bytes", "sha256")}


class Reader:
    def __init__(self, check): self.check = check; self.bytes = 0; self.saved = {}

    def charge(self, size):
        self.check(); require(type(size) is int and size >= 0 and self.bytes + size <= CAPS["input_read_bytes"], "input_read_limit")
        self.bytes += size

    def bounded(self, operation, maximum):
        """Reserve before I/O; refund only returned, known-length local bytes."""
        self.check(); allowance = min(maximum, CAPS["input_read_bytes"] - self.bytes)
        require(allowance > 0, "input_read_limit")
        self.bytes += allowance
        raw = operation(allowance)
        require(type(raw) is bytes and len(raw) <= allowance, "bounded_read_differs")
        self.bytes -= allowance - max(1, len(raw))
        self.check(); return raw

    def read(self, binding, maximum=16 * 1024**2, *, retain=True):
        require(type(binding) is dict and set(binding) == {"path", "bytes", "sha256"}
            and type(binding["path"]) is str and type(binding["bytes"]) is int and 0 <= binding["bytes"] <= maximum
            and type(binding["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", binding["sha256"]), "reference_schema")
        path = Path(binding["path"]); before = acquisition.identity(path)
        require(path.is_absolute() and path.resolve() == path, "input_path_not_literal")
        require(before["bytes"] == binding["bytes"], "input_size_changed")
        previous = self.saved.get(str(path)); require(previous is None or previous == (binding, before), "input_identity_changed")
        hasher = hashlib.sha256(); count = 0; parts = []
        with path.open("rb") as stream:
            while True:
                allowance = min(65536, binding["bytes"] - count + 1)
                self.charge(allowance); chunk = stream.read(allowance)
                count += len(chunk); require(count <= binding["bytes"], "input_grew")
                hasher.update(chunk)
                if retain: parts.append(chunk)
                if not chunk: break
        require(count == binding["bytes"] and hasher.hexdigest() == binding["sha256"] and acquisition.identity(path) == before, "input_hash_or_identity_changed")
        self.saved[str(path)] = (dict(binding), before)
        return b"".join(parts) if retain else None

    def json(self, binding, maximum=16 * 1024**2): return acquisition.probe.strict_json(self.read(binding, maximum))

    def observe(self, path, maximum=262144):
        path = Path(path); before = acquisition.identity(path)
        require(path.is_absolute() and path.resolve() == path, "input_path_not_literal")
        require(before["bytes"] <= maximum, "observed_input_limit")
        self.charge(before["bytes"] + 1)
        with path.open("rb") as stream: raw = stream.read(before["bytes"] + 1)
        require(len(raw) == before["bytes"] and acquisition.identity(path) == before, "observed_input_changed")
        binding = {"path": str(path), "bytes": len(raw), "sha256": digest(raw)}
        old = self.saved.get(str(path)); require(old is None or old == (binding, before), "input_identity_changed")
        self.saved[str(path)] = (binding, before)
        return binding, raw

    def lines(self, binding, maximum=256 * 1024**2):
        # One physical streaming pass; only one bounded record is materialized.
        require(type(binding) is dict and set(binding) == {"path", "bytes", "sha256"}
            and type(binding["bytes"]) is int and 0 < binding["bytes"] <= maximum, "journal_reference_schema")
        path = Path(binding["path"]); before = acquisition.identity(path); hasher = hashlib.sha256(); total = 0
        require(path.is_absolute() and path.resolve() == path, "input_path_not_literal")
        require(before["bytes"] == binding["bytes"], "journal_size_changed")
        with path.open("rb") as stream:
            while True:
                line = self.bounded(stream.readline, min(16 * 1024**2 + 1, binding["bytes"] - total + 1))
                if not line: break
                require(line.endswith(b"\n") and len(line) <= 16 * 1024**2, "journal_record_limit")
                total += len(line); require(total <= binding["bytes"], "journal_grew"); hasher.update(line)
                yield acquisition.probe.strict_json(line)
        require(total == binding["bytes"] and hasher.hexdigest() == binding["sha256"] and acquisition.identity(path) == before, "journal_binding_changed")
        prior = self.saved.get(str(path)); require(prior is None or prior == (binding, before), "input_identity_changed")
        self.saved[str(path)] = (dict(binding), before)

    def reopen(self):
        for binding, before in list(self.saved.values()):
            require(acquisition.identity(Path(binding["path"])) == before, "input_identity_changed")
            self.read(binding, max(1, binding["bytes"]), retain=False)

    def identities(self):
        for binding, before in list(self.saved.values()):
            self.check(); require(acquisition.identity(Path(binding["path"])) == before, "input_identity_changed")


def git_bytes(reader, args, maximum=1024**2):
    def receive(allowance):
        process = subprocess.Popen(["git", *args], cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            raw = process.stdout.read(allowance)
            require(len(raw) < allowance, "git_output_limit")
            reader.check(); require(process.wait(timeout=10) == 0, "git_command_failed")
            return raw
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.kill()
                reader.check(); process.wait(timeout=1)
    raw = reader.bounded(receive, maximum + 1)
    require(len(raw) <= maximum, "git_output_limit"); return raw


def source_snapshot(head, reader):
    def git(*args): return git_bytes(reader, args)
    require(type(head) is str and re.fullmatch(r"[0-9a-f]{40}", head), "source_head_schema")
    require(git("rev-parse", "HEAD").decode().strip() == head and not git("status", "--porcelain", "--untracked-files=normal").strip(), "source_not_clean")
    source = {"head": head, "files": {}}
    for name in SOURCE_FILES:
        path = REPO / name
        size = git("cat-file", "-s", head + ":" + name).strip()
        require(size.isdigit() and int(size) <= 1024**2, "source_size_limit")
        raw = git("cat-file", "blob", head + ":" + name); require(len(raw) == int(size), "source_blob_size_differs")
        source["files"][name] = {"bytes": len(raw), "sha256": digest(raw)}
        reader.read({"path": str(path), **source["files"][name]}, 1024**2, retain=False)
    require(git("rev-parse", "HEAD").decode().strip() == head and not git("status", "--porcelain", "--untracked-files=normal").strip(), "source_changed")
    return source


def producer(source, expected, reader):
    require(type(source) is dict and set(source) == {"head", "files"} and source["head"] == HEAD and source["files"] == expected, "producer_source_differs")
    for name, binding in expected.items():
        size = git_bytes(reader, ["cat-file", "-s", HEAD + ":" + name], 32).strip()
        raw = git_bytes(reader, ["cat-file", "blob", HEAD + ":" + name], binding["bytes"])
        require(size.isdigit() and int(size) == len(raw) == binding["bytes"] and digest(raw) == binding["sha256"], "producer_blob_differs")


def artifact_ref(root, value):
    name = Path(value["path"])
    require(not name.is_absolute() and name.parts and ".." not in name.parts and str(name) == value["path"], "artifact_path_invalid")
    return {"path": str(root / name), "bytes": value["bytes"], "sha256": value["sha256"]}


def tree(root, expected, reader):
    root = Path(root); require(root.is_absolute() and root.resolve() == root and root.is_dir(), "root_not_literal")
    actual = set()
    for path in root.rglob("*"):
        reader.check(); require(not path.is_symlink() and (path.is_file() or path.is_dir()), "tree_type_invalid")
        if path.is_file(): actual.add(str(path))
        require(len(actual) <= len(expected), "tree_roster_changed")
    require(actual == set(expected), "tree_roster_changed")


def add_file(files, binding, category):
    require(category in {"raw", "control", "other"}, "artifact_category_invalid")
    key = binding["path"]; value = {**binding, "category": category}
    require(key not in files or files[key] == value, "artifact_binding_conflict")
    files[key] = value


def retained_closure(admission, manifest, reader):
    """Use existing page/index inventories; never duplicate old body payloads."""
    files, roots = {}, {}
    v4_admission = reader.json(admission["v4_prefix"]["admission"])
    old = v4_admission["prior_ledger"]; prior_inventory = reader.json(old["inventory"])
    require(prior_inventory.get("schema_version") == "polymarket_metadata_prior_inventory_v4", "old_inventory_schema")
    require(type(prior_inventory.get("files")) is list and len(prior_inventory["files"]) <= 10_000, "old_inventory_limit")
    for item in prior_inventory["files"]: add_file(files, ref(item), item["category"])
    add_file(files, old["inventory"], "control")
    for name in old["roots"]:
        root = Path(name); require(root.parent == RUN_ROOT, "old_root_scope")
        roots[name] = {p for p in files if root in Path(p).parents}
        require(roots[name], "empty_old_root")
    for key in ("v4_prefix", "v5_prefix"):
        prefix = admission[key]; root = Path(prefix["root"]); names = set()
        for field in ("start", "failure", "attempts", "pages"):
            item = prefix[field]; add_file(files, item, "control"); names.add(item["path"])
        for item in reader.lines(prefix["attempts"]):
            body = item.get("body")
            if type(body) is dict:
                binding = artifact_ref(root, body); add_file(files, binding, "raw"); names.add(binding["path"])
        expected = COUNTS["v4_files"] if key == "v4_prefix" else COUNTS["v5_files"]
        require(len(names) == expected, "historical_prefix_file_count")
        roots[str(root)] = names
    # The v6 segmented inventory records every pre-inventory owned artifact.
    index_ref = artifact_ref(RUN, manifest["artifact_inventory"]); index = reader.json(index_ref)
    require(index.get("schema_version") == "polymarket_metadata_segmented_inventory_v6"
        and index.get("record_count") == COUNTS["inventory_records"] and type(index.get("segments")) is list and 1 <= len(index["segments"]) <= 128,
        "v6_inventory_schema")
    current = set(); total = 0
    for segment in index["segments"]:
        binding = artifact_ref(RUN, segment); count = 0
        require(binding["bytes"] <= 1024**2, "inventory_segment_limit")
        for entry in reader.lines(binding, 1024**2):
            item = artifact_ref(RUN, entry); require(item["path"] not in current, "inventory_duplicate")
            add_file(files, item, entry["category"]); current.add(item["path"]); count += 1
        require(count == segment["record_count"], "inventory_segment_count")
        total += count; add_file(files, binding, "control"); current.add(binding["path"])
    require(total == index["record_count"], "inventory_count_differs")
    for binding in (index_ref, manifest["_manifest_ref"], manifest["_complete_ref"]):
        add_file(files, binding, "control"); current.add(binding["path"])
    roots[str(RUN)] = current
    for item in files.values(): reader.read(ref(item), max(1, item["bytes"]), retain=False)
    for root, names in roots.items(): tree(root, names, reader)
    return files, roots


def validate_terminal(bindings, reader, current):
    fields = {"manifest", "complete", "producer_start", "launch", "caller_exit", "admission"}
    require(type(bindings) is dict and set(bindings) == fields, "terminal_bindings_schema")
    values = {key: reader.json(value) for key, value in bindings.items()}
    for key, pin in PINS.items(): require((bindings[key]["bytes"], bindings[key]["sha256"]) == pin, "terminal_pin_differs")
    require(bindings["manifest"]["path"] == str(RUN / "manifest.json") and bindings["complete"]["path"] == str(RUN / "COMPLETE.json")
        and bindings["producer_start"]["path"] == str(RUN / "START.json")
        and bindings["launch"]["path"] == str(OPERATIONS / "full_metadata_v6.LAUNCH.json")
        and bindings["caller_exit"]["path"] == str(OPERATIONS / "full_metadata_v6.EXIT.json"), "terminal_path_differs")
    manifest, marker, start, launch, exited, admission = (values[key] for key in ("manifest", "complete", "producer_start", "launch", "caller_exit", "admission"))
    expected_source = {"head": HEAD, "files": {name: current["files"][name] for name in acquisition.SOURCE_FILES}}
    producer(expected_source, expected_source["files"], reader)
    require(manifest.get("source") == start.get("source") == expected_source
        and manifest.get("schema_version") == start.get("schema_version") == acquisition.START_SCHEMA
        and manifest.get("status") == acquisition.SUCCESS and manifest.get("mode") == start.get("mode") == "full"
        and manifest.get("caps") == start.get("caps") == acquisition.CAPS
        and manifest.get("scope") == start.get("scope") == acquisition.probe.strict_json(encode(acquisition.SCOPE))
        and manifest.get("universe_complete") is False and manifest.get("analysis_admitted") is False, "producer_terminal_differs")
    require(marker.get("schema_version") == "polymarket_market_metadata_campaign_completion_v6"
        and marker.get("status") == acquisition.SUCCESS and marker.get("mode") == "full"
        and marker.get("manifest") == {"path": "manifest.json", "bytes": bindings["manifest"]["bytes"], "sha256": bindings["manifest"]["sha256"], "category": "control"}
        and marker.get("universe_complete") is False and marker.get("analysis_admitted") is False, "completion_link_differs")
    require(ref(start["admission"]) == ref(manifest["admission"]) == bindings["admission"], "producer_admission_differs")
    acquisition.validate_admission(admission, HEAD, RUN)
    require(start.get("adopted_prefixes") == manifest.get("adopted_prefixes") == {key: admission[key] for key in ("v4_prefix", "v5_prefix")}
        and start.get("replay") == manifest.get("replay")
        and start["replay"].get("accepted_pages") == COUNTS["prefix_pages"] and start["replay"].get("raw_rows") == COUNTS["prefix_rows"]
        and start["replay"].get("all_accepted_bodies_replayed_before_http") is True
        and start["replay"].get("failed_partial_accepted") is False, "inherited_prefix_replay_binding_differs")
    require(launch.get("schema_version") == "polymarket_oct1_stage_launcher_v4" and launch.get("stage") == "metadata"
        and launch.get("status") == "producer_dispatched_owned_session" and launch.get("output_dir") == str(RUN)
        and launch.get("source", {}).get("head") == HEAD and launch.get("admission") == start["admission"]
        and launch.get("ownership_unknown") is False and launch.get("retry_or_resume") is False, "launch_differs")
    require(exited.get("schema_version") == launch["schema_version"] and exited.get("stage") == "metadata"
        and exited.get("status") == "producer_exited" and exited.get("output_dir") == str(RUN)
        and exited.get("source") == launch["source"] and exited.get("admission") == launch["admission"]
        and exited.get("command") == launch.get("command") and type(exited.get("producer_exit_code")) is int and exited["producer_exit_code"] == 0
        and exited.get("error_code") is None and exited.get("ownership_unknown") is False
        and all(exited.get(key) is True for key in ("owned_group_drained", "stdout_eof_observed", "source_and_admission_unchanged"))
        and exited.get("children_still_running") is False, "caller_exit_differs")
    for role in ("caller", "child"):
        acquisition.previous._birth(exited.get(role)); require(exited[role] == launch.get(role), "caller_birth_differs")
    return values


def reconcile(values, files, reader):
    """Reconcile new control evidence without claiming expanded-body validation."""
    admission, manifest = values["admission"], values["manifest"]
    previous_failure = reader.json(admission["v5_prefix"]["failure"])
    states = acquisition.probe.strict_json(encode(previous_failure["streams"]))
    ids = [s["id"] for s in acquisition.STREAMS]; starts = reader.lines(ref(files[str(RUN / "attempts.jsonl")]))
    charge = observed = rows = count = 0; prior = admission["prior_ledger"]
    seen_cursors = {i: set() for i in range(5)}; seen_pages = {i: set() for i in range(5)}
    for key in ("v4_prefix", "v5_prefix"):
        for page in reader.lines(admission[key]["pages"]):
            i = ids.index(page["stream"]); seen_cursors[i].update((page["requested_cursor"], page["next_cursor"])); seen_pages[i].add(page["rows_sha256"])
    for page in reader.lines(ref(files[str(RUN / "pages.jsonl")])):
        count += 1; start, finish = next(starts, None), next(starts, None)
        require(type(start) is dict and type(finish) is dict and start.get("event") == "START" and finish.get("event") == "FINISH", "attempt_pair_missing")
        require(type(page) is dict and page.get("event") == "PAGE" and page.get("accepted") is True and page.get("stream") in ids, "page_schema")
        i = ids.index(page["stream"]); require(i >= 2, "completed_stream_recollected")
        if i == len(states):
            require(states[-1]["pagination_terminal_observed"], "stream_order_differs")
            states.append({"id": ids[i], "provider": "gamma", "accepted_pages": 0, "raw_rows": 0,
                "identity_issue_counts": {}, "pagination_terminal_observed": False, "returned_scope_fields_verified": True})
        require(i == len(states)-1 and not states[i]["pagination_terminal_observed"], "stream_order_differs")
        state = states[i]; cursor = state.get("next_cursor"); index = state["accepted_pages"]
        request = acquisition.v4.request_spec(acquisition.STREAMS[i], cursor)
        allowance = min(acquisition.CAPS["individual_response_bytes"], acquisition.CAPS["total_response_bytes"] - prior["response_bytes_charged"] - charge)
        expected_attempt = 2 if count == 1 else 1
        require(all(type(item.get("call")) is int and item["call"] == count and item.get("stream") == ids[i]
            and type(item.get("page_index")) is int and item["page_index"] == index
            and item.get("coverage_role") == "v6_budget_continuation" for item in (start, finish, page)), "page_attempt_join_differs")
        require(start.get("request") == request and start.get("request_sha256") == finish.get("request_sha256") == digest(encode(request))
            and type(start.get("exact_page_attempt")) is int and start["exact_page_attempt"] == expected_attempt
            and start.get("cumulative_http_attempt") == prior["http_attempts"] + count
            and start.get("response_byte_reservation") == allowance and page.get("requested_cursor") == cursor, "request_lineage_differs")
        body = page.get("body"); require(type(body) is dict and finish.get("body") == body, "page_body_differs")
        binding = artifact_ref(RUN, body)
        require(str(RUN / f"responses/a{count:05d}.body.gz") == binding["path"] and ref(files[binding["path"]]) == binding
            and body.get("complete_received_body") is True and finish.get("body_complete") is True
            and finish.get("accepted") is True and finish.get("error_code") is None and finish.get("status_code") == 200
            and finish.get("transport_dispatched") is True and finish.get("response_close_completed") is True
            and finish.get("interrupted") is False and finish.get("retry_delay_seconds") is None, "accepted_body_evidence_differs")
        length = body.get("uncompressed_bytes")
        require(type(length) is int and 0 <= length <= allowance and finish.get("response_bytes_observed") == finish.get("response_bytes_charged") == length
            and type(body.get("uncompressed_sha256")) is str and re.fullmatch(r"[0-9a-f]{64}", body["uncompressed_sha256"]), "response_charge_differs")
        require(type(page.get("count")) is int and 0 <= page["count"] <= 100 and type(page.get("rows_sha256")) is str
            and re.fullmatch(r"[0-9a-f]{64}", page["rows_sha256"]) and page["rows_sha256"] not in seen_pages[i]
            and type(page.get("pagination_terminal_observed")) is bool, "page_count_or_replay_differs")
        terminal = page["pagination_terminal_observed"]; next_cursor = page.get("next_cursor")
        require((terminal and page["count"] < 100 and next_cursor is None) or
            (not terminal and page["count"] == 100 and type(next_cursor) is str and next_cursor and next_cursor not in seen_cursors[i]), "cursor_terminal_differs")
        summary = page.get("row_summary"); require(type(summary) is dict and not summary.get("scope_conflict_counts"), "row_summary_conflict")
        state["accepted_pages"] += 1; state["raw_rows"] += page["count"]
        codes = Counter(state["identity_issue_counts"]); codes.update(summary["identity_issue_counts"]); state["identity_issue_counts"] = dict(codes)
        state["returned_scope_fields_verified"] &= summary["returned_scope_fields_verified"]
        state["pagination_terminal_observed"] = terminal; state["next_cursor"] = next_cursor
        seen_pages[i].add(page["rows_sha256"]); seen_cursors[i].add(next_cursor)
        charge += length; observed += length; rows += page["count"]
    require(next(starts, None) is None and count == COUNTS["new_pages"] and rows == COUNTS["new_rows"]
        and len(states) == 5 and all(s["pagination_terminal_observed"] for s in states) and states == manifest["streams"], "terminal_counts_differ")
    require(manifest["waterfall_new_run"] == {"accepted_pages": count, "http_attempts_this_run": count, "raw_rows_in_accepted_pages": rows}, "waterfall_differs")
    usage = manifest["usage"]
    require(usage["this_run_http_attempts"] == count and usage["this_run_response_bytes_charged"] == charge
        and usage["response_bytes_observed_this_run"] == observed and usage["http_attempts_charged"] == prior["http_attempts"] + count == BASE["http_attempts"]
        and usage["response_bytes_charged"] == prior["response_bytes_charged"] + charge == BASE["response_bytes_charged"], "cumulative_usage_differs")
    return {"new_http_attempts": count, "new_accepted_pages": count, "new_raw_rows": rows,
        "new_response_bytes_charged": charge, "historical_semantic_replay_repeated": False, "expanded_bodies_decoded": False}


def births(value):
    if type(value) is dict:
        names = ("birth_ticks", "start_ticks", "linux_birth_ticks")
        if "pid" in value and any(name in value for name in names):
            ticks = [value[name] for name in names if name in value]
            require(type(value["pid"]) is int and value["pid"] > 0 and ticks
                and all(type(tick) is int and tick > 0 for tick in ticks) and len(set(ticks)) == 1
                and type(value.get("boot_id")) is str, "saved_birth_schema")
            try: valid = str(uuid.UUID(value["boot_id"])) == value["boot_id"]
            except (ValueError, AttributeError): valid = False
            require(valid, "saved_birth_schema"); yield {**value, "birth_ticks": ticks[0]}
        for child in value.values(): yield from births(child)
    elif type(value) is list:
        for child in value: yield from births(child)


def census(reader):
    result = {}; proc = Path("/proc")
    def small(path, maximum=16384):
        with path.open("rb") as handle: raw = reader.bounded(handle.read, maximum + 1)
        require(len(raw) <= maximum, "process_record_limit"); return raw
    for path in proc.iterdir():
        if not path.name.isdigit(): continue
        require(len(result) < CAPS["processes"], "process_count_limit")
        try:
            raw = small(path / "stat").decode(); fields = raw[raw.rfind(")") + 2:].split(); argv = small(path / "cmdline").split(b"\0")
            require(len(fields) > 19, "process_stat_invalid")
            try: executable = os.readlink(path / "exe")
            except PermissionError: executable = argv[0].decode(errors="replace") if argv[0] else None
            except FileNotFoundError: executable = None
            result[int(path.name)] = {"pid": int(path.name), "parent_pid": int(fields[1]), "birth_ticks": int(fields[19]),
                "group": int(fields[2]), "session": int(fields[3]), "state": fields[0], "kernel_thread_flag": bool(int(fields[6]) & 0x00200000),
                "cmdline_empty": not any(argv), "full_exe": executable, "comm": raw[raw.find("(")+1:raw.rfind(")")],
                "program": next((a.decode(errors="replace") for a in argv[1:] if a.startswith(b"/")), None), "uid": path.stat().st_uid,
                "user_service": "systemd_user" if argv[:2] == [b"/usr/lib/systemd/systemd", b"--user"] else "sd_pam" if argv[0] == b"(sd-pam)" else None}
        except (FileNotFoundError, ProcessLookupError): continue
    return result


def own_ancestry(processes, self_pid):
    chain = {}; pid = self_pid
    while pid in processes and pid not in chain:
        item = processes[pid]
        if pid != self_pid: require(item["comm"] in {"sshd", "sshd-session", "sshd-auth", "systemd", "init", "bash", "dash", "sh"}, "unknown_ancestry")
        chain[pid] = (item["birth_ticks"], item["parent_pid"], item["full_exe"]); pid = item["parent_pid"]
    require(self_pid in chain, "acceptor_birth_missing")
    return chain


def classify(processes, bound_ancestry):
    records = []; kernel = 0
    for pid, value in bound_ancestry.items():
        item = processes.get(pid); require(item is not None and (item["birth_ticks"], item["parent_pid"], item["full_exe"]) == value, "acceptor_ancestry_changed")
    for p in processes.values():
        if p["pid"] in bound_ancestry: kind = "bound_acceptor_ancestry"
        elif p["full_exe"] is None:
            require(p["state"] != "Z" and p["kernel_thread_flag"] and p["cmdline_empty"] and p["uid"] == 0, "unclassified_executable_missing")
            kernel += 1; continue
        elif (p["full_exe"], p["uid"]) in OS_SERVICES: kind = "exact_os_service"
        elif p["uid"] == 0 and p["program"] in OS_PROGRAMS: kind = "exact_os_program"
        elif p["uid"] == 1000 and p["user_service"] == "systemd_user" and p["parent_pid"] == 1: kind = "systemd_user"
        elif p["uid"] == 1000 and p["user_service"] == "sd_pam" and processes.get(p["parent_pid"], {}).get("user_service") == "systemd_user": kind = "sd_pam"
        else: raise Error("unclassified_compute_notebook_transfer_consumer")
        records.append({key: p[key] for key in ("pid", "parent_pid", "birth_ticks", "group", "session", "state", "full_exe", "comm", "uid")} | {"classification": kind})
    return sorted(records, key=lambda row: row["pid"]), kernel


def absent(saved, processes, boot):
    # Validate before using a boot mismatch as evidence of absence.
    parsed = list(births(saved)); require(parsed and parsed[0]["pid"] == saved["pid"], "saved_birth_schema")
    item = processes.get(saved["pid"])
    require(not (saved["boot_id"] == boot and item is not None and item["birth_ticks"] == parsed[0]["birth_ticks"]), "saved_birth_live")


def process_gate(reader, old_roster, process_roots, boot, ancestry, terminal, *, snapshot=census):
    processes = snapshot(reader); records, kernel = classify(processes, ancestry)
    old = reader.json(old_roster); require(type(old.get("process_receipts")) is list
        and len(old["process_receipts"]) == COUNTS["old_process_receipts"], "old_process_roster_count")
    paths = set(); checks = extra = entries = 0
    for item in old["process_receipts"]:
        binding = item["binding"]; value = reader.json(binding); saved = list(births(value))
        require(saved and binding["path"] not in paths, "old_process_roster_duplicate_or_empty")
        for birth in saved: absent(birth, processes, boot)
        paths.add(binding["path"]); checks += 1
    for root in process_roots:
        root = Path(root); require(root.is_dir() and root.resolve() == root, "process_receipt_root_invalid")
        for base, dirs, names in os.walk(root, followlinks=False):
            entries += len(dirs) + len(names); require(entries <= CAPS["receipt_walk_entries"], "receipt_walk_limit")
            require(all(not Path(base, name).is_symlink() for name in dirs), "receipt_tree_symlink")
            dirs[:] = [name for name in dirs if name not in {"responses", "payloads", "raw", "headers", "__pycache__", ".git"}]
            for name in names:
                if not name.endswith(".json") or not any(part in name.lower() for part in ("process", "launch", "caller", "start", "exit", "owner")): continue
                path = Path(base, name)
                if str(path) in paths: continue
                _, raw = reader.observe(path, 262144); value = acquisition.probe.strict_json(raw); saved = list(births(value))
                require(not name.endswith("PROCESS.json") or saved, "empty_process_receipt")
                for birth in saved: absent(birth, processes, boot); extra += 1
                paths.add(str(path))
    for role in ("caller", "child"):
        saved = terminal["caller_exit"][role]; absent(saved, processes, boot)
        if saved["boot_id"] == boot:
            require(all(item["group"] != saved["process_group"] and item["session"] != saved["session"] for item in processes.values()), "owned_group_or_session_live")
    return {"old_process_receipts_checked": checks, "new_birth_checks": extra, "receipt_walk_entries": entries,
        "nonkernel_process_census": records, "kernel_processes_classified": kernel,
        "owned_producers_drained": True, "transfers_drained": True, "host_idle_claimed": False}


def prepare_candidate(terminal_bindings, dispatch_binding, reader, current):
    """Derive the exact historic closure, never execute an old builder/run."""
    values = validate_terminal(terminal_bindings, reader, current)
    require(dispatch_binding["path"] == str(OPERATIONS / "full_metadata_v6_root_dispatch_review_v1.json")
        and (dispatch_binding["bytes"], dispatch_binding["sha256"]) == DISPATCH_PIN, "dispatch_pin_differs")
    dispatch = reader.json(dispatch_binding)
    require(dispatch.get("schema_version") == "polymarket_metadata_v6_root_dispatch_review_v1"
        and dispatch.get("source_head") == HEAD and dispatch.get("admission") == terminal_bindings["admission"]
        and dispatch.get("launch") == terminal_bindings["launch"], "dispatch_bindings_differ")
    ledger_ref = dispatch["external_ledger"]; ledger = reader.json(ledger_ref)
    require(ledger.get("schema_version") == "polymarket_metadata_v6_preparation_external_controls_v1"
        and ledger.get("consumer_head") == HEAD and ledger.get("shared_external_control_ceiling_bytes") == CAPS["external_bytes"]
        and ledger.get("no_second_reservation") is True, "external_ledger_schema")
    require(ledger["prior_terminal_drain"] == values["admission"]["v5_prefix"]["drain"]
        and values["admission"]["prior_ledger"]["root_controls"] == ledger["new_direct_controls"] + [ledger_ref],
        "external_ledger_admission_differs")
    old_terminal_ref = ledger["prior_terminal_drain"]; old_terminal = reader.json(old_terminal_ref)
    require((old_terminal_ref["bytes"], old_terminal_ref["sha256"]) == acquisition.PIN["drain"], "old_terminal_pin_differs")
    old_closure = old_terminal["external_control_closure"]; old_ledger_ref = old_closure["external_closure_ref"]
    old_ledger = reader.json(old_ledger_ref)
    require(old_ledger.get("schema_version") == "polymarket_metadata_v5_preparation_external_controls_v1"
        and old_ledger.get("isolated_qa_root") == str(OLD_QA), "old_external_ledger_schema")
    old_refs = old_ledger["direct_controls"] + old_ledger["nested_files"] + [old_ledger_ref]
    old_refs += old_closure["future_actual_controls"] + [old_closure["audit_source"], old_terminal_ref]
    require(len(old_refs) == COUNTS["old_external_refs"] and sum(item["bytes"] for item in old_refs) == COUNTS["old_external_bytes"], "old_external_union_differs")
    log_ref, _ = reader.observe(OPERATIONS / "full_metadata_v6.log", 1024**2)
    exited = values["caller_exit"]
    require(exited.get("stdout_truncated") is False and log_ref["bytes"] == exited.get("stdout_bytes_observed") == exited.get("stdout_prefix_bytes_retained")
        and log_ref["sha256"] == exited.get("stdout_sha256"), "caller_log_binding_differs")
    future = [dispatch["admission"], dispatch["builder_check"], dispatch["caller_start"], dispatch["launch"], terminal_bindings["caller_exit"], log_ref, dispatch_binding]
    controls = old_refs + ledger["new_direct_controls"] + ledger["new_nested_files"] + [ledger_ref] + future
    require(len(controls) == COUNTS["external_refs"] and len({item["path"] for item in controls}) == COUNTS["external_refs"]
        and sum(item["bytes"] for item in controls) == COUNTS["external_actual_bytes"], "external_union_differs")
    for item in controls: reader.read(item, CAPS["external_bytes"], retain=False)
    qa_refs = [item for item in old_ledger["nested_files"] if OLD_QA in Path(item["path"]).parents]
    tree(OLD_QA, {item["path"] for item in qa_refs}, reader)
    roster = old_terminal["bindings"]["old_roster"]
    require((roster["bytes"], roster["sha256"]) == OLD_ROSTER_PIN and roster["path"] == str(OPERATIONS / "finalization_shutdown_drain_v2.json"), "old_process_roster_pin_differs")
    return {"values": values, "historical_external_controls": controls, "historical_qa_files": qa_refs, "old_process_roster": roster}


class Output:
    def __init__(self, root, input_bytes, stdout_reserve, check):
        self.root, self.used, self.stdout, self.check = root, input_bytes, stdout_reserve, check
        self.files = {}

    def write(self, name, value):
        self.check(); raw = encode(value)
        require(len(raw) <= CAPS["single_new_artifact_bytes"] and self.used + self.stdout + len(raw) <= CAPS["all_new_artifact_bytes"], "new_stage_artifact_limit")
        require(shutil.disk_usage(self.root).free >= CAPS["minimum_free_bytes"] + len(raw), "disk_floor")
        path = self.root / name; require(name not in self.files and not path.exists() and not path.is_symlink(), "output_exists")
        self.used += len(raw); binding = {"path": name, "bytes": len(raw), "sha256": digest(raw), "category": "control"}
        self.files[name] = binding
        with path.open("xb") as handle:
            require(handle.write(raw) == len(raw), "short_write"); handle.flush(); os.fsync(handle.fileno())
        require(path.read_bytes() == raw, "output_reopen_differs"); return binding

    def reopen(self):
        require({p.name for p in self.root.iterdir()} == set(self.files)
            and all(p.is_file() and not p.is_symlink() for p in self.root.iterdir()), "output_set_changed")
        for name, expected in self.files.items():
            self.check(); raw = (self.root / name).read_bytes()
            require(len(raw) == expected["bytes"] and digest(raw) == expected["sha256"], "output_changed")


def validate_prior(value, now):
    require(type(value) is dict and set(value) == set(BASE), "prior_usage_schema")
    for key, cap in (("http_attempts", acquisition.CAPS["http_attempts"]), ("response_bytes_charged", acquisition.CAPS["total_response_bytes"]), ("runtime_seconds", acquisition.CAPS["runtime_seconds"])):
        acquisition.number(value[key], cap, integer=key != "runtime_seconds")
        minimum = BASE[key]
        if key == "runtime_seconds":
            require(now >= ANCHOR, "preparation_clock_invalid")
            minimum += math.ceil((now - ANCHOR).total_seconds())
        require(value[key] >= minimum, "prior_usage_below_floor")
    require(value["runtime_seconds"] + CAPS["runtime_seconds"] < acquisition.CAPS["runtime_seconds"], "shared_runtime_exhausted")
    amounts = value["retained_bytes_charged"]
    require(type(amounts) is dict and set(amounts) == {"raw", "control", "other"}, "prior_retained_schema")
    for key, cap in (("raw", "raw_body_bytes"), ("control", "control_bytes"), ("other", "other_metadata_bytes")):
        acquisition.number(amounts[key], acquisition.CAPS[cap])
        require(amounts[key] >= BASE["retained_bytes_charged"][key], "prior_retained_floor_refunded")
    require(amounts["control"] + CAPS["all_new_artifact_bytes"] <= acquisition.CAPS["control_bytes"]
        and sum(amounts.values()) + CAPS["all_new_artifact_bytes"] <= acquisition.CAPS["aggregate_metadata_bytes"], "shared_metadata_byte_limit")


def admission_contract():
    """Only this fixed terminal profile; placeholders are not admission."""
    return {"schema_version": ADMISSION, "status": "root_metadata6_terminal_reopen_admitted",
        "consumer_head": "clean committed acceptor HEAD", "run_dir": "fresh literal /mnt/data/runs stage directory", "caps": CAPS,
        "terminal": {name: "strict absolute path/bytes/sha256 ref" for name in
            ("manifest", "complete", "producer_start", "launch", "caller_exit", "admission")},
        "dispatch": "fixed full_metadata_v6_root_dispatch_review_v1.json ref",
        "prior_usage": {**BASE, "runtime_seconds": "59400 + ceil(fresh preparation since 2026-10-28T17:45:44Z), or higher already charged usage"},
        "prior_read_bytes": "exact preceding candidate Reader.bytes, already incurred inside the same16GiB input ceiling",
        "new_artifacts": "own source/test/contract plus fresh OP metadata_v6_terminal_acceptance*.json/log refs; <=32",
        "stdout_reserved_bytes": "1024..65536, inside256KiB stage and existing2MiB pool",
        "boot_id": "fresh canonical Linux boot UUID", "data_st_dev": "fresh mounted data device, independently verified UUID/serial"}


def run_acceptance(admission_path, admission_sha, head, output, *, snapshot=census, boot_reader=None, utc_now=None):
    began = time.monotonic(); output = Path(output)
    def check(): require(time.monotonic() - began < CAPS["runtime_seconds"], "runtime_limit")
    reader = Reader(check); now = (utc_now or (lambda: datetime.now(timezone.utc)))()
    admission_path = Path(admission_path); before = acquisition.identity(admission_path)
    binding = {"path": str(admission_path), "bytes": before["bytes"], "sha256": admission_sha}
    value = reader.json(binding, CAPS["single_new_artifact_bytes"])
    require(type(value) is dict and set(value) == {"schema_version", "status", "consumer_head", "run_dir", "caps", "terminal", "dispatch", "prior_usage",
        "new_artifacts", "stdout_reserved_bytes", "boot_id", "data_st_dev", "prior_read_bytes"}
        and value["schema_version"] == ADMISSION and value["status"] == "root_metadata6_terminal_reopen_admitted"
        and value["consumer_head"] == head and value["run_dir"] == str(output) and value["caps"] == CAPS, "root_admission_differs")
    reader.charge(value["prior_read_bytes"])
    require(output.is_absolute() and output.parent == RUN_ROOT and output.parent.resolve() == output.parent
        and not output.exists() and not output.is_symlink() and output != RUN, "output_not_fresh")
    validate_prior(value["prior_usage"], now)
    check_boot = boot_reader or (lambda: Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    boot = check_boot()
    try: valid_boot = str(uuid.UUID(boot)) == boot
    except (ValueError, AttributeError): valid_boot = False
    require(valid_boot and value["boot_id"] == boot and type(value["data_st_dev"]) is int
        and value["data_st_dev"] == os.stat(RUN_ROOT).st_dev, "fresh_host_identity_differs")
    current = source_snapshot(head, reader)
    initial_processes = snapshot(reader); ancestry = own_ancestry(initial_processes, os.getpid()); classify(initial_processes, ancestry)
    candidate = prepare_candidate(value["terminal"], value["dispatch"], reader, current)
    values = candidate["values"]; manifest = dict(values["manifest"])
    manifest.update(_manifest_ref=value["terminal"]["manifest"], _complete_ref=value["terminal"]["complete"])
    files, roots = retained_closure(values["admission"], manifest, reader)
    reconciliation = reconcile(values, files, reader)
    require(type(value["new_artifacts"]) is list and len(value["new_artifacts"]) <= 32, "new_artifact_roster")
    new = []; paths = set()
    for item in value["new_artifacts"]:
        require(item["path"] not in paths and item["path"] != str(admission_path), "new_artifact_duplicate")
        path = Path(item["path"])
        require(path in {REPO / name for name in SOURCE_FILES[len(acquisition.SOURCE_FILES):]}
            or path.parent == OPERATIONS and re.fullmatch(r"metadata_v6_terminal_acceptance[A-Za-z0-9_.-]*\.(?:json|log)", path.name), "new_artifact_scope")
        reader.read(item, CAPS["single_new_artifact_bytes"], retain=False); new.append(item); paths.add(item["path"])
    require({str(REPO / name) for name in SOURCE_FILES[len(acquisition.SOURCE_FILES):]} <= paths, "new_source_artifacts_missing")
    new.append(binding); new_bytes = sum(item["bytes"] for item in new)
    require(type(value["stdout_reserved_bytes"]) is int and 1024 <= value["stdout_reserved_bytes"] <= CAPS["single_new_artifact_bytes"]
        and new_bytes + value["stdout_reserved_bytes"] < CAPS["all_new_artifact_bytes"], "new_stage_artifact_limit")
    controls = candidate["historical_external_controls"] + new
    require(len({item["path"] for item in controls}) == len(controls) and len(controls) + 3 <= CAPS["external_references"]
        and sum(item["bytes"] for item in controls) + CAPS["all_new_artifact_bytes"] - new_bytes <= CAPS["external_bytes"], "shared_external_limit")
    require(shutil.disk_usage(output.parent).free >= CAPS["minimum_free_bytes"] + CAPS["all_new_artifact_bytes"], "disk_floor")
    process_proof = process_gate(reader, candidate["old_process_roster"], PROCESS_ROOTS, boot, ancestry, values, snapshot=snapshot)
    source_before = current
    def final_inputs():
        require(source_snapshot(head, reader) == source_before and check_boot() == boot and os.stat(RUN_ROOT).st_dev == value["data_st_dev"], "source_or_host_changed")
        for root, expected in roots.items(): tree(root, expected, reader)
        tree(OLD_QA, {item["path"] for item in candidate["historical_qa_files"]}, reader)
        reader.reopen(); check()
    # One initial authentication plus one final content reopen for large bodies.
    output.mkdir(mode=0o700); writer = Output(output, new_bytes, value["stdout_reserved_bytes"], check)
    try:
        drain = {"schema_version": "polymarket_metadata_v6_root_terminal_drain_v1", "status": "root_verified_bound_metadata_stage_drained",
            "metadata_run_dir": str(RUN), "producer_head": HEAD, "consumer_source": current,
            "bindings": value["terminal"], "boot_id": boot, "data_st_dev": value["data_st_dev"],
            "caller": values["caller_exit"]["caller"], "child": values["caller_exit"]["child"],
            "acceptor_ancestry": {str(pid): list(identity) for pid, identity in ancestry.items()},
            **process_proof, **FALSE_SCOPE}
        drain_bound = writer.write("root_drain.json", drain)
        cumulative = acquisition.probe.strict_json(encode(value["prior_usage"]))
        cumulative["runtime_seconds"] += CAPS["runtime_seconds"]
        cumulative["retained_bytes_charged"]["control"] += CAPS["all_new_artifact_bytes"]
        as_of = (utc_now or (lambda: datetime.now(timezone.utc)))()
        require(as_of.tzinfo is not None and as_of >= now, "preparation_clock_invalid")
        result = {"schema_version": SCHEMA, "status": STATUS, "metadata_run_dir": str(RUN), "producer_head": HEAD,
            "source": current, "bindings": {**value["terminal"], "root_drain": artifact_ref(output, drain_bound)},
            "prior_usage": cumulative, "prior_usage_as_of_utc": as_of.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "stage_prior_usage": value["prior_usage"], "caps": CAPS,
            "new_usage": {"http_attempts": 0, "response_bytes_charged": 0, "runtime_seconds_reserved": CAPS["runtime_seconds"],
                "retained_bytes_charged": {"raw": 0, "control": CAPS["all_new_artifact_bytes"], "other": 0}},
            "reconciliation": reconciliation, "retained_files_rehashed": len(files), "retained_bytes_by_category": dict(Counter({key: sum(item["bytes"] for item in files.values() if item["category"] == key) for key in ("raw", "control", "other")})),
            "compressed_artifacts_authenticated": True, "historical_semantic_replay_repeated": False,
            "shared_external_controls": {"references": controls, "actual_bytes": sum(item["bytes"] for item in controls), "actual_refs": len(controls),
                "scope": "bound_external_controls_excludes_this_acceptor_output_directory", "own_output_control_reservation_bytes": CAPS["all_new_artifact_bytes"] - new_bytes},
            "owned_producers_drained": True, "metadata_pagination_complete": True,
            "charged_input_bytes_before_final_reopens": reader.bytes, **FALSE_SCOPE}
        result_bound = writer.write("RESULT.json", result)
        marker = {"schema_version": "polymarket_metadata_v6_terminal_acceptance_completion_v1", "status": STATUS,
            "result": result_bound, "root_drain": drain_bound, **FALSE_SCOPE}
        writer.write(".COMPLETE.pending", marker)
        fd = os.open(output, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try: os.fsync(fd)
        finally: os.close(fd)
        final_inputs()
        process_gate(reader, candidate["old_process_roster"], PROCESS_ROOTS, boot, ancestry, values, snapshot=snapshot)
        for root, expected in roots.items(): tree(root, expected, reader)
        tree(OLD_QA, {item["path"] for item in candidate["historical_qa_files"]}, reader)
        require(check_boot() == boot and os.stat(RUN_ROOT).st_dev == value["data_st_dev"], "fresh_host_identity_differs")
        writer.reopen(); reader.identities(); check()
        acquisition.probe.atomic_completion(output / ".COMPLETE.pending", output / "COMPLETE.json")
        return result
    except BaseException:
        # No unknown exception text, process command lines or credentials escape.
        try: writer.write("FAILURE.json", {"schema_version": SCHEMA, "status": "failed_unaccepted", "error_code": "acceptance_failed", **FALSE_SCOPE})
        except BaseException: pass
        raise


@contextmanager
def limits():
    old = resource.getrlimit(resource.RLIMIT_AS); handler = signal.getsignal(signal.SIGALRM)
    require(signal.getitimer(signal.ITIMER_REAL) == (0., 0.), "unexpected_existing_timer")
    try:
        resource.setrlimit(resource.RLIMIT_AS, (min(old[0], CAPS["address_space_bytes"]) if old[0] >= 0 else CAPS["address_space_bytes"], old[1]))
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(Error("runtime_limit")))
        signal.setitimer(signal.ITIMER_REAL, CAPS["runtime_seconds"]); yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, handler); resource.setrlimit(resource.RLIMIT_AS, old)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission", type=Path, required=True); parser.add_argument("--admission-sha256", required=True)
    parser.add_argument("--expected-head", required=True); parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args(argv); require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
    with limits(): result = run_acceptance(args.admission, args.admission_sha256, args.expected_head, args.run_dir)
    print(encode({"status": result["status"], "analysis_admitted": False}).decode().rstrip()); return 0


if __name__ == "__main__": raise SystemExit(main())
