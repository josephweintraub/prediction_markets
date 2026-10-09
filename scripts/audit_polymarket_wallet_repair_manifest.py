#!/usr/bin/env python3
"""Reconcile saved wallet-repair evidence only; never open a trade/Parquet file."""
from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import tempfile

CENSUS_SHA256 = "91f14b86c452496879e49181713ff6a4654b52c1cf4f46967d9ff3772ea7ba4c"
QA_SHA256 = "8d155da22c591e69ea77825dac793b866c7a28c9651b692f4bb27351bd4c1faa"
SOURCES = {
    "scripts/repair_polymarket_wallet_attribution.py": "ad51cd9d954a577093b3264739b36d57ae45e77150fbe0329e5c6188e0fbcb61",
    "tests/test_polymarket_wallet_repair.py": "665bfaed5629f9810acc61bd931aba0933c75806c2e086336539d4b7bc72d219",
    "scripts/audit_polymarket_lineage.py": "f6b093a57fcef36391649a657e0ec8565b672fbf9206d65812c797dd14809a25",
    "production_guard.py": "24865e6c0759c4b4c1080e4f47c07beecfebf674f6f02bef7d06351c9b180d58",
}
CAPS = {"memory_limit": "64GB", "threads": 4, "spill_bytes": 0,
        "maximum_leaf_rows": 25_000_000, "maximum_leaf_payload_bytes": 16 * 1024**3,
        "maximum_read_bytes": 16 * 1024**4, "minimum_free_bytes": 5 * 1024**3,
        "output_expansion_factor": 2, "metadata_allowance_per_file": 1024**2,
        "maximum_manifest_bytes": 4 * 1024**2, "maximum_census_bytes": 128 * 1024**2}
TOTALS = {"root": 2_114_623_452, "clean": 2_036_128_538}
INPUT_BASES = {"root": "/mnt/data/pipeline_root_output/trades.parquet",
               "clean": "/mnt/data/pipeline_output/trades_clean.parquet"}
RELATIONS = {"root": "root_transformed", "clean": "clean"}
PAIR_LEAVES = 616
MAX_PROFILE = 16 * 1024
MAX_RECEIPT = 1024**2
LEAF_MEMORY = "Scalar-admit original/output pair <=16GiB, materialize only that pair, exact comparisons in memory, drop before next leaf; 64GB DuckDB/0B spill."
WRITE_CONTRACT = "COPY RLIMIT_FSIZE ceiling is twice original file bytes; per-file metadata allowance and 5GiB reserve are separate. Run as the sole production stage."
TRANSFORM = "Swap proxyWallet/counterparty only when is_maker=false; all other fields unchanged"
ACTION = "Original inferred opposite action remains unverified"
SUPPORT_KEYS = {"row_count", "maker_rows", "nonmaker_rows", "invalid_rows", "logical_payload_bytes"}
TOP_KEYS = {"schema_version", "status", "downstream_adoption", "data_certified", "binding", "source", "caps",
            "leaf_memory_contract", "write_contract", "command", "inputs", "outputs", "rows", "reconciliation",
            "environment", "resource_profile", "read_charges", "limits", "downstream_gate"}
MONTH_KEYS = {"schema_version", "status", "relation", "month", "data_certified", "input", "output",
              "maximum_copy_file_bytes", "separately_reserved_metadata_bytes", "reconciliation", "grain",
              "timestamp_pruning", "transform", "counterparty_action"}
OUTPUT_KEYS = {"relation", "month", "path", "input", "output", "manifest_sha256", "validation_leaves"}
LEAF_KEYS = {"lower_inclusive", "upper_exclusive", "counts", "materialized_pair_payload_bytes",
             "full11_expected_to_output", "unchanged_other9_fields"}


class ReviewBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ReviewBlocked(message)


def count(value):
    require(type(value) is int and value >= 0, "nonnegative exact integer required")
    return value


def digest(value):
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "SHA256 digest required")
    return value


def same(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right))
    return left == right


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def finite(token):
    value = float(token)
    require(math.isfinite(value), "nonfinite JSON number")
    return value


def read_saved(path, limit, *, text=False):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular non-symlink metadata required")
    before = path.stat()
    require(0 < before.st_size <= limit, "metadata size bound exceeded or empty")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    after = path.stat()
    signature = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    require(signature(before) == signature(after) and len(raw) == before.st_size,
            "metadata changed during read")
    decoded = raw.decode("utf-8")
    value = decoded if text else json.loads(decoded, object_pairs_hook=unique_object,
        parse_float=finite, parse_constant=lambda token: (_ for _ in ()).throw(ReviewBlocked(token)))
    if not text:
        require(type(value) is dict, "metadata JSON object required")
    return value, {"path": str(path.resolve()), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def months():
    return [f"{year:04d}-{month:02d}" for year in range(2022, 2027) for month in range(1, 13)
            if "2022-11" <= f"{year:04d}-{month:02d}" <= "2026-06"]


def bounds(month):
    year, number = map(int, month.split("-"))
    begin = datetime(year, number, 1, tzinfo=timezone.utc)
    end = datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=timezone.utc)
    return int(begin.timestamp()), int(end.timestamp())


def interval(record, month):
    lower, upper = count(record["lower_inclusive"]), count(record["upper_exclusive"])
    begin, end = bounds(month)
    require(begin <= lower < upper <= end, "invalid complete-second leaf interval")
    return lower, upper


def coverage(records, month):
    require(type(records) is list and records, "nonempty complete leaf list required")
    spans = [interval(record, month) for record in records]
    begin, end = bounds(month)
    require(spans == sorted(spans) and spans[0][0] == begin and spans[-1][1] == end and
            all(left[1] == right[0] for left, right in zip(spans, spans[1:])), "leaf gap, overlap or incomplete month")
    return spans


def support(record):
    require(type(record) is dict and set(record) == SUPPORT_KEYS, "leaf support schema differs")
    for value in record.values():
        count(value)
    require(record["invalid_rows"] == 0 and record["maker_rows"] == record["nonmaker_rows"] and
            record["maker_rows"] + record["nonmaker_rows"] == record["row_count"] <= CAPS["maximum_leaf_rows"] and
            2 * record["logical_payload_bytes"] <= CAPS["maximum_leaf_payload_bytes"], "leaf integrity/payload/role cap failed")


def footprint(info, lower, upper):
    total = 0
    for group in info["row_groups"]:
        stat = group["stats"]["timestamp"]
        require(type(stat) is dict and count(stat["null_count"]) == 0, "missing/null timestamp footer evidence")
        minimum, maximum = count(stat["min"]), count(stat["max"])
        begin, end = bounds(info["partition_month"])
        require(begin <= minimum <= maximum < end, "original footer month confinement failed")
        size = count(group["compressed_bytes"])
        if minimum < upper and maximum >= lower:
            total += size
    return total


def command_binding(profile, receipt, manifest, identities, expected_head):
    command = manifest["command"]
    require(type(command) is list and all(type(value) is str and value for value in command) and
            command[0] in ("scripts/repair_polymarket_wallet_attribution.py",
                "/home/ubuntu/prediction_markets/scripts/repair_polymarket_wallet_attribution.py") and
            len(command) == 13, "unexpected repair command")
    flags = {}
    expected_flags = {"--census-manifest", "--census-qa", "--expected-head", "--run-dir",
                      "--reviewed-preflight", "--approved-preflight-sha256"}
    for offset in range(1, len(command), 2):
        flag, value = command[offset:offset + 2]
        require(flag in expected_flags and flag not in flags, "unexpected/duplicate repair flag")
        flags[flag] = value
    require(set(flags) == expected_flags and flags["--expected-head"] == expected_head,
            "repair CLI source binding differs")
    run = str(Path(identities["manifest"]["path"]).parent)
    require(flags["--run-dir"] == run and flags["--census-manifest"] == identities["census"]["path"] and
            flags["--census-qa"] == identities["census_qa"]["path"], "repair CLI metadata/run membership differs")
    digest(flags["--approved-preflight-sha256"])
    require(Path(flags["--reviewed-preflight"]).is_absolute(), "absolute reviewed preflight path required")
    require(set(receipt) == {"status", "run_dir", "rows", "downstream_adoption", "reviewed_preflight"} and
            receipt["status"] == "repair_complete" and receipt["downstream_adoption"] == "pending" and
            receipt["run_dir"] == run and same(receipt["rows"], TOTALS), "durable completion receipt differs")
    reviewed = receipt["reviewed_preflight"]
    require(set(reviewed) == {"path", "bytes", "sha256"} and count(reviewed["bytes"]) > 0 and
            reviewed["path"] == flags["--reviewed-preflight"] and
            reviewed["sha256"] == flags["--approved-preflight-sha256"], "durable reviewed-preflight binding differs")
    lines = profile.splitlines()
    require(not any("Command terminated" in line or "Command exited with non-zero status" in line for line in lines),
            "timed execution contains failure evidence")
    timed = [line for line in lines if line.lstrip().startswith("Command being timed:")]
    exits = [line for line in lines if line.lstrip().startswith("Exit status:")]
    require(len(exits) == 1 and exits[0] == "\tExit status: 0", "missing/duplicate/nonzero timed exit")
    require(len(timed) == 1, "missing/duplicate timed command")
    match = re.fullmatch(r'\tCommand being timed: "(.*)"', timed[0])
    require(match is not None, "malformed timed command")
    argv = shlex.split(match.group(1))
    require(argv and argv[0] == "/home/ubuntu/venv/bin/python", "unexpected timed executable")
    tail = argv[2:] if argv[1:2] == ["-u"] else argv[1:]
    require(tail == command, "timed command differs from saved repair argv")
    return {"exit_status": 0, "timed_command": argv, "reviewed_preflight": reviewed, "run_dir": run}


def review(manifest, summary, census, qa, monthly, profile, receipt, identities, expected_digest, expected_head):
    digest(expected_digest)
    require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head), "full expected source HEAD required")
    require(identities["manifest"]["sha256"] == expected_digest and
            identities["census"]["sha256"] == CENSUS_SHA256 and identities["census_qa"]["sha256"] == QA_SHA256,
            "expected manifest/frozen census/QA digest differs")
    require(set(manifest) == TOP_KEYS and set(manifest["environment"]) == {"python", "duckdb", "platform"} and
            set(manifest["resource_profile"]) == {"read_bytes_charged", "wall_seconds", "peak_rss_bytes", "memory_note"},
            "unexpected producer claim/record fields")
    require(same(summary, {**manifest, "manifest_sha256": expected_digest}), "summary is not the exact manifest projection")
    require(manifest["schema_version"] == "polymarket_wallet_repair_v1" and manifest["status"] == "repair_complete" and
            manifest["downstream_adoption"] == "pending" and manifest["data_certified"] is False,
            "repair completion/scientific adoption gate failed")
    require(same(manifest["caps"], CAPS) and same(manifest["source"], {"head": expected_head, "sha256": SOURCES}) and
            manifest["leaf_memory_contract"] == LEAF_MEMORY and manifest["write_contract"] == WRITE_CONTRACT,
            "vetted source/caps/memory/write contract differs")
    require(manifest["limits"] == "Wallet-only published representation repair; no native identity/action certification, cleaning changes, flag/base rebuild or scientific rerun." and
            manifest["downstream_gate"] == "Previously saved wallet flags and analytic bases require independent lineage/revalidation before adoption of this repaired vintage.",
            "scientific limitations/gate missing")
    required_reconciliation = {"exact_full11_multisets": True, "unchanged_other9_fields": True,
        "original_file_counts_types_and_multiplicities": True,
        "root_distinct_to_clean_preserved_by_bijective_transform": True, "original_inputs_reopened": True}
    require(same(manifest["reconciliation"], required_reconciliation), "global recorded reconciliation differs")
    require(census["status"] == census["census"]["status"] == "published_pair_census_complete" and
            census["data_certified"] is False and census["census"]["data_certified"] is False and
            census["census"]["completed_months"] == months() and census["census"]["final_input_identity_reopened"] is True and
            census["census"]["root_distinct_to_clean_reconciled"] is True and
            count(census["census"]["global"]["cleaning"]["failed_full11_reconciliation_leaves"]) == 0,
            "frozen original census gate failed")
    require(qa["status"] == "complete_saved_wallet_census_manifest_qa" and qa["data_certified"] is False and
            count(qa["exit_profile"]["exit_status"]) == 0 and qa["completed_months"] == months() and
            qa["final_input_identity_reopened_recorded"] is True and
            qa["inputs"]["manifest"]["sha256"] == CENSUS_SHA256 and
            count(qa["inputs"]["manifest"]["bytes"]) == identities["census"]["bytes"], "original QA binding differs")
    require(count(qa["leaf_count"]) * 2 == PAIR_LEAVES and len(census["census"]["leaves"]) * 2 == PAIR_LEAVES,
            "frozen 308-leaf census/QA coverage differs")
    require(same(manifest["binding"], {"census": identities["census"], "census_qa": identities["census_qa"]}),
            "repair binding differs from exact original metadata inputs")
    require(same(census["inputs"], INPUT_BASES) and same(census["footer_rows"], TOTALS) and
            same(manifest["rows"], TOTALS), "frozen global row totals/input bases differ")
    execution = command_binding(profile, receipt, manifest, identities, expected_head)
    expected_pairs = [(relation, month) for relation in RELATIONS for month in months()]
    require(type(manifest["inputs"]) is list and type(manifest["outputs"]) is list and
            len(manifest["inputs"]) == len(manifest["outputs"]) == len(monthly) == 88, "88-file coverage differs")
    require([(value["relation"], value["month"]) for value in manifest["inputs"]] == expected_pairs and
            [(value["relation"], value["month"]) for value in manifest["outputs"]] == expected_pairs and
            set(monthly) == set(expected_pairs), "duplicate/missing/out-of-order relation/month membership")
    charges = manifest["read_charges"]
    require(type(charges) is list, "read charge list required")
    cursor, computed_read, leaf_total, metadata_bytes, output_bytes = 0, 0, 0, 0, 0
    totals = {relation: 0 for relation in RELATIONS}
    coverage_receipt = []

    def charge(stage, file, leaf, expected=None):
        nonlocal cursor, computed_read
        require(cursor < len(charges), "missing read charge")
        value = charges[cursor]; cursor += 1
        require(set(value) == {"stage", "bytes", "file", "leaf"} and value["stage"] == stage and
                same(value["file"], file) and same(value["leaf"], leaf), "read charge stage/file/leaf membership differs")
        amount = count(value["bytes"])
        require(expected is None or amount == expected, "read charge footprint law failed")
        computed_read += amount
        require(computed_read <= CAPS["maximum_read_bytes"], "read cap exceeded")
        return amount

    for original, output, pair in zip(manifest["inputs"], manifest["outputs"], expected_pairs):
        require(type(output) is dict and set(output) == OUTPUT_KEYS, "unexpected output claim/record fields")
        relation, month = pair
        info_list = census["inventories"][relation]
        candidates = [info for info in info_list if info["partition_month"] == month]
        require(len(info_list) == 44 and len(candidates) == 1, "original inventory membership differs")
        frozen = candidates[0]
        require(frozen["relation"] == RELATIONS[relation] and type(frozen["schema"]) is str and
                frozen["path"] == str(Path(INPUT_BASES[relation]) / ("year_month=" + month) / Path(frozen["path"]).name),
                "original physical path/schema membership differs")
        require(set(original) == {"relation", "month", "path", "stat", "footer_sha256", "rows", "schema"} and
                original["path"] == frozen["path"] and original["schema"] == frozen["schema"] and
                original["footer_sha256"] == digest(frozen["footer_sha256"]) and
                count(original["rows"]) == count(frozen["rows"]), "original input schema/footer/count differs")
        stat = original["stat"]
        require(set(stat) == {"device", "inode", "bytes", "mtime_ns", "ctime_ns"}, "input stat schema differs")
        for number in stat.values():
            count(number)
        require(stat["bytes"] == count(frozen["bytes"]) and stat["mtime_ns"] == count(frozen["mtime_ns"]) and
                sum(count(group["rows"]) for group in frozen["row_groups"]) == original["rows"], "original stat/footer law failed")
        doc, identity = monthly[pair]
        require(type(doc) is dict and set(doc) == MONTH_KEYS, "unexpected monthly claim/record fields")
        metadata_bytes += identity["bytes"]
        relative = relation + "/year_month=" + month
        require(output["path"] == relative and output["manifest_sha256"] == identity["sha256"] and
                doc["schema_version"] == "polymarket_wallet_repair_file_v1" and doc["status"] == "repair_file_complete" and
                doc["relation"] == relation and doc["month"] == month and doc["data_certified"] is False,
                "monthly identity/atomic completion differs")
        require(same(doc["input"], output["input"]) and same({key: value for key, value in doc["input"].items() if key != "sha256"}, original),
                "monthly input identity differs from top/frozen original")
        digest(doc["input"]["sha256"])
        require(same(doc["output"], output["output"]) and set(doc["output"]) == {"path", "bytes", "sha256", "rows", "schema"} and
                doc["output"]["path"] == "data.parquet" and doc["output"]["schema"] == original["schema"] and
                count(doc["output"]["rows"]) == original["rows"], "monthly physical output schema/count/membership differs")
        digest(doc["output"]["sha256"])
        size = count(doc["output"]["bytes"])
        require(size > 0 and size <= 2 * stat["bytes"] == count(doc["maximum_copy_file_bytes"]) and
                count(doc["separately_reserved_metadata_bytes"]) == CAPS["metadata_allowance_per_file"] and
                size + identity["bytes"] <= 2 * stat["bytes"] + CAPS["metadata_allowance_per_file"], "output write/metadata ceiling failed")
        require(doc["transform"] == TRANSFORM and doc["counterparty_action"] == ACTION and
                doc["grain"] == "Original published wallet-row multiplicity", "wallet-only/native-action contract differs")
        checks = doc["reconciliation"]
        spans = coverage(checks, month)
        frozen_leaves = sorted((leaf for leaf in census["census"]["leaves"] if leaf["month"] == month),
                               key=lambda leaf: leaf["lower_inclusive"])
        require(spans == coverage(frozen_leaves, month) and count(output["validation_leaves"]) == len(checks),
                "repair leaves differ from frozen complete-second coverage")
        file_key = {"relation": relation, "month": month}
        charge("input_sha256_before", file_key, None, stat["bytes"])
        charge("wallet_only_copy", file_key, None, stat["bytes"])
        rows, source_overlap, output_overlap = 0, 0, 0
        for check, old, (lower, upper) in zip(checks, frozen_leaves, spans):
            require(type(check) is dict and set(check) == LEAF_KEYS, "unexpected leaf claim/record fields")
            values = check["counts"]; support(values)
            old_support = old["metrics"]["support"][relation]
            require(same(values, {key: old_support[key] for key in SUPPORT_KEYS}) and
                    count(check["materialized_pair_payload_bytes"]) == 2 * values["logical_payload_bytes"],
                    "original/output leaf support or bounded pair payload differs")
            for field in ("full11_expected_to_output", "unchanged_other9_fields"):
                require(set(check[field]) == {"left_only_rows", "right_only_rows"} and
                        all(count(value) == 0 for value in check[field].values()), "exact multiset difference is nonzero")
            leaf_key = {"lower_inclusive": lower, "upper_exclusive": upper}
            source = footprint(frozen, lower, upper)
            charge("leaf_counts:original_leaf", file_key, leaf_key, source)
            emitted = charge("leaf_counts:output_leaf", file_key, leaf_key)
            require(emitted <= size and (emitted > 0 or values["row_count"] == 0),
                    "output overlap exceeds file size or cannot support nonempty leaf")
            charge("leaf_materialize:original", file_key, leaf_key, source)
            charge("leaf_materialize:output", file_key, leaf_key, emitted)
            for stage in ("exact:expected_values:except_all:output_values", "exact:output_values:except_all:expected_values",
                          "exact:original_values:except_all:output_values", "exact:output_values:except_all:original_values"):
                charge(stage, file_key, leaf_key, 0)
            rows += values["row_count"]; source_overlap += source; output_overlap += emitted
        require(rows == original["rows"], "leaf/file row reconciliation failed")
        pruning = doc["timestamp_pruning"]
        require(same(pruning, {"source_overlap_bytes": source_overlap, "output_overlap_bytes": output_overlap,
                    "preserve_insertion_order": True, "global_sort_performed": False}) and
                output_overlap <= 2 * source_overlap + len(checks) * CAPS["metadata_allowance_per_file"],
                "timestamp pruning/read resource law failed")
        charge("output_sha256", file_key, None, size)
        charge("input_sha256_after", file_key, None, stat["bytes"])
        totals[relation] += rows; output_bytes += size; leaf_total += len(checks)
        coverage_receipt.append({"relation": relation, "month": month, "rows": rows, "leaves": len(checks),
                                 "monthly_manifest_sha256": identity["sha256"]})
    require(cursor == len(charges) and leaf_total == PAIR_LEAVES and same(totals, TOTALS),
            "global charged-read/616-leaf/row coverage differs")
    resource = manifest["resource_profile"]
    require(count(resource["read_bytes_charged"]) == computed_read and count(resource["peak_rss_bytes"]) > 0 and
            type(resource["wall_seconds"]) in (int, float) and not isinstance(resource["wall_seconds"], bool) and
            math.isfinite(resource["wall_seconds"]) and resource["wall_seconds"] >= 0 and
            resource["memory_note"] == "64GB bounds DuckDB-managed memory, not process RSS; admitted leaf-pair tables are dropped before next leaf, spill is forbidden.",
            "recorded execution resource profile differs")
    input_bytes = sum(value["stat"]["bytes"] for value in manifest["inputs"])
    require(output_bytes + metadata_bytes + identities["manifest"]["bytes"] + identities["summary"]["bytes"] <=
            2 * input_bytes + 88 * CAPS["metadata_allowance_per_file"], "total saved output cap exceeded")
    require(type(manifest["environment"]) is dict and manifest["environment"]["platform"] == "linux" and
            manifest["environment"]["duckdb"] in {"1.4.4", "1.5.0"} and
            all(type(manifest["environment"][field]) is str and manifest["environment"][field]
                for field in ("python", "duckdb")), "producer environment evidence missing")
    return {"schema_version": 1, "status": "complete_saved_wallet_repair_manifest_qa", "data_certified": False,
            "downstream_adoption": "pending", "inputs": identities, "expected_source_head": expected_head,
            "producer_source_sha256": SOURCES, "exit_profile": execution, "file_count": 88,
            "relation_leaf_count": leaf_total, "rows": totals, "charged_read_bytes": computed_read,
            "declared_output_bytes": output_bytes, "coverage": coverage_receipt,
            "reconciliation": {"frozen_original_metadata_bound": True, "leaf_file_global_rows_reconciled": True,
                "all_recorded_full11_and_other9_differences_zero": True, "complete_second_coverage": True},
            "limits": "Saved evidence consistency only: no trade rows, Parquet decoding, output content hashes, filesystem data identities, or query plans were independently re-read or executed. Atomic publication, unchanged inputs, native/action limits and scientific gates remain recorded producer evidence. This is not independent trade-data certification or scientific adoption."}


def atomic_publish(staging, target):
    library = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        result = library.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = library.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise ReviewBlocked("atomic no-replace publication unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def build_review(run, census_path, qa_path, profile_path, receipt_path, destination, expected_digest, expected_head):
    run, destination = Path(run), Path(destination)
    require(not destination.exists() and not destination.is_symlink(), "immutable QA destination already exists")
    require(not run.is_symlink() and run.is_dir(), "published metadata directory required")
    documents, identities = {}, {}
    paths = {"manifest": run / "manifest.json", "summary": run / "summary.json", "census": Path(census_path),
             "census_qa": Path(qa_path), "exit_profile": Path(profile_path), "stdout_receipt": Path(receipt_path)}
    for key, path in paths.items():
        require(destination.resolve() not in path.resolve().parents and destination.resolve() != path.resolve(),
                "QA output overlaps metadata input")
        limit = CAPS["maximum_census_bytes"] if key == "census" else MAX_PROFILE if key == "exit_profile" else CAPS["maximum_manifest_bytes"]
        documents[key], identities[key] = read_saved(path, limit, text=key == "exit_profile")
    require(run.resolve() not in destination.resolve().parents, "QA output cannot alter published repair run")
    for base in INPUT_BASES.values():
        original = Path(base)
        require(destination.resolve() != original and original not in destination.resolve().parents and
                destination.resolve() not in original.parents, "QA output overlaps original trade storage")
    monthly = {}
    for relation in RELATIONS:
        for month in months():
            parent = run / relation / ("year_month=" + month)
            require(not (run / relation).is_symlink() and not parent.is_symlink(), "symlinked monthly metadata refused")
            monthly[relation, month] = read_saved(parent / "manifest.json", CAPS["maximum_manifest_bytes"])
    result = review(documents["manifest"], documents["summary"], documents["census"], documents["census_qa"],
        monthly, documents["exit_profile"], documents["stdout_receipt"], identities, expected_digest, expected_head)
    result["reviewer_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    encoded = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(encoded) <= MAX_RECEIPT, "complete QA receipt exceeds cap; no truncation")
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="." + destination.name + ".staging-", dir=destination.parent))
    with (staging / "receipt.json").open("xb") as stream:
        stream.write(encoded); stream.flush(); os.fsync(stream.fileno())
    atomic_publish(staging, destination)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repair-run", type=Path, required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--census-manifest", type=Path, required=True)
    parser.add_argument("--census-qa", type=Path, required=True)
    parser.add_argument("--exit-profile", type=Path, required=True)
    parser.add_argument("--stdout-receipt", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_review(args.repair_run, args.census_manifest, args.census_qa, args.exit_profile,
        args.stdout_receipt, args.run_dir, args.expected_manifest_sha256, args.expected_head)
    print(json.dumps({"status": result["status"], "data_certified": False, "downstream_adoption": "pending",
                      "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
