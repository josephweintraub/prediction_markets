#!/usr/bin/env python3
"""Review saved wallet-census metadata only; never query or reopen trade files."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex

MAX_MANIFEST = 256 * 1024**2
MAX_RECEIPT = 1024**2
MAX_PROFILE = 8 * 1024
COMPLETE = "published_pair_census_complete"
HEAD = "6f03a31257b7735d2392ed447f5eeaff38211375"
SOURCES = {
    "scripts/audit_polymarket_wallet_pairs.py": "d24d8489b1412e02e20ad76075ac1a18d38c9be34b625477a31b8fae5b9932c4",
    "tests/test_polymarket_wallet_pairs.py": "0a15f026c940d4286a892183ace09aa588c370a4e6b3397c2056f49c000f90ac",
    "scripts/audit_polymarket_lineage.py": "f6b093a57fcef36391649a657e0ec8565b672fbf9206d65812c797dd14809a25",
    "docs/analysis_specs/polymarket_wallet_pair_census_v1.json": "6bde5efe9ea71641eb8200e21c0eb60334a9e4f1302eb0b1d0b5dc3e211b53db",
}
CAPS = {"duckdb_memory_limit": "64GB", "threads": 4, "disk_spill": "0B",
        "maximum_rows_per_relation_leaf": 25000000,
        "maximum_combined_logical_payload_bytes_per_leaf": 16 * 1024**3,
        "maximum_compact_output_bytes": MAX_MANIFEST,
        "maximum_planned_read_footprint_bytes": 8 * 1024**4,
        "maximum_admission_nodes": 4096, "no_arrow_body_materialization": True,
        "disabled_optimizers": "common_subplan"}
FROZEN_FIELDS = ("schema_version", "data_certified", "contract", "contract_sha256", "expected_head",
                 "source_sha256", "inputs", "caps", "inventories", "published_month_proofs",
                 "published_directory_layouts", "footer_rows", "initial_plan", "environment", "io_plan")
SUMMARY_FIELDS = ("schema_version", "status", "data_certified", "expected_head", "source_sha256",
                  "contract_sha256", "caps", "inputs", "footer_rows", "environment", "io_plan")
RELATIONS = ("root", "clean")
INPUTS = {"root": "/mnt/data/pipeline_root_output/trades.parquet",
          "clean": "/mnt/data/pipeline_output/trades_clean.parquet"}
MODES = ("full_label", "label_omitted_diagnostic")
CATEGORIES = ("correct_only", "copied_only", "both_compatible", "neither")


class ReviewBlocked(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReviewBlocked(message)


def count(value) -> int:
    require(type(value) is int and value >= 0, "nonnegative exact integer required")
    return value


def strict_equal(left, right) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(strict_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(strict_equal(a, b) for a, b in zip(left, right))
    return left == right


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key")
        result[key] = value
    return result


def finite_float(token: str) -> float:
    value = float(token)
    require(math.isfinite(value), "nonfinite JSON number")
    return value


def read_saved(path: Path, limit: int, *, json_object: bool = True):
    before = path.stat()
    require(0 < before.st_size <= limit, "saved input exceeds size bound or is empty")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    after = path.stat()
    signature = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    require(signature(before) == signature(after) and len(raw) == before.st_size,
            "saved input changed during review")
    text = raw.decode("utf-8")
    value = json.loads(text, object_pairs_hook=unique_object, parse_float=finite_float,
                       parse_constant=lambda token: (_ for _ in ()).throw(ReviewBlocked(token))) if json_object else text
    if json_object:
        require(isinstance(value, dict), "saved JSON object required")
    return value, {"path": str(path.resolve()), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def months() -> list[str]:
    return [f"{year:04d}-{month:02d}" for year in range(2022, 2027) for month in range(1, 13)
            if "2022-11" <= f"{year:04d}-{month:02d}" <= "2026-06"]


def bounds(month: str) -> tuple[int, int]:
    year, number = map(int, month.split("-"))
    start = datetime(year, number, 1, tzinfo=timezone.utc)
    end = datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp()), int(end.timestamp())


def key(item: dict) -> tuple[str, int, int]:
    month, lower, upper = item["month"], item["lower_inclusive"], item["upper_exclusive"]
    require(month in months(), "interval outside frozen months")
    count(lower); count(upper)
    start, end = bounds(month)
    require(start <= lower < upper <= end, "invalid complete-second interval")
    return month, lower, upper


def sum_records(records: list[dict]) -> dict:
    require(bool(records) and all(record.keys() == records[0].keys() for record in records),
            "additive record schemas differ")
    result = {}
    for field in records[0]:
        values = [record[field] for record in records]
        if isinstance(values[0], dict):
            require(all(isinstance(value, dict) for value in values), "additive record type differs")
            result[field] = sum_records(values)
        elif type(values[0]) is float:
            require(field in {"gross_recorded_cash", "root_distinct_gross_recorded_cash"} and
                    all(type(value) is float and math.isfinite(value) and value >= 0 for value in values),
                    "only named finite nonnegative recorded cash may be float")
            result[field] = math.fsum(values)
        else:
            result[field] = sum(count(value) for value in values)
    return result


def support_valid(support: dict) -> None:
    require(set(support) == set(RELATIONS), "support relations differ")
    for value in support.values():
        for field, number in value.items():
            if field == "gross_recorded_cash":
                require(type(number) in (int, float) and math.isfinite(number) and number >= 0,
                        "finite nonnegative recorded cash required")
            else:
                count(number)
        require(value["maker_rows"] + value["nonmaker_rows"] == value["row_count"] and
                value["missing_role_rows"] == value["invalid_rows"] == 0, "support integrity failed")


def admitted(support: dict) -> bool:
    return (all(value["row_count"] <= CAPS["maximum_rows_per_relation_leaf"] for value in support.values()) and
            sum(value["logical_payload_bytes"] for value in support.values()) <=
            CAPS["maximum_combined_logical_payload_bytes_per_leaf"])


def arithmetic(record: dict) -> None:
    support_valid(record["support"])
    for relation in RELATIONS:
        support = record["support"][relation]
        for mode in MODES:
            metrics = record[relation][mode]
            for field, value in metrics.items():
                if field != "compatibility":
                    count(value)
            require(metrics["maker_rows"] == support["maker_rows"] and
                    metrics["nonmaker_rows"] == support["nonmaker_rows"], "pair support differs")
            for hypothesis in ("correct", "copied"):
                matched = count(metrics["matched_" + hypothesis])
                require(matched + count(metrics["missing_observed_" + hypothesis]) == support["maker_rows"] and
                        matched + count(metrics["excess_observed_" + hypothesis]) == support["nonmaker_rows"],
                        "pair capacity law failed")
            require(set(metrics["compatibility"]) == set(CATEGORIES), "compatibility categories differ")
            for field in ("common_classes", "maker_rows", "nonmaker_rows", "singleton_role_classes"):
                require(sum(count(metrics["compatibility"][category][field]) for category in CATEGORIES) ==
                        count(metrics[field]), "category denominator law failed")
    cleaning = record["cleaning"]
    cash = cleaning["root_distinct_gross_recorded_cash"]
    require(type(cash) in (int, float) and math.isfinite(cash) and cash >= 0,
            "finite nonnegative distinct recorded cash required")
    for field, value in cleaning.items():
        if field != "root_distinct_gross_recorded_cash":
            count(value)
    root, clean = (record["support"][relation]["row_count"] for relation in RELATIONS)
    require(count(cleaning["root_distinct_rows"]) + count(cleaning["root_value_row_surplus"]) == root and
            cleaning["root_distinct_rows"] - count(cleaning["expected_clean_only_rows"]) +
            count(cleaning["clean_only_rows"]) == clean and
            cleaning["clean_only_rows"] == count(cleaning["clean_value_row_surplus"]) +
            count(cleaning["clean_only_value_classes"]), "root DISTINCT/clean law failed")


def exit_binding(text: str, manifest: dict, approved_sha256: str) -> dict:
    lines = text.splitlines()
    exits = [line for line in lines if line.lstrip().startswith("Exit status:")]
    require(len(exits) == 1 and re.fullmatch(r"\tExit status: 0", exits[0]) is not None,
            "exit profile missing, duplicated, malformed or nonzero")
    match = re.fullmatch(r'\tCommand being timed: "(.*)"', lines[0])
    require(match is not None and sum(line.lstrip().startswith("Command being timed:") for line in lines) == 1,
            "timed command missing or duplicated")
    timed = shlex.split(match.group(1))
    require(len(timed) >= 3 and timed[:2] == ["/home/ubuntu/venv/bin/python", "-u"] and
            timed[2:] == manifest["command"], "profile command differs from saved census command")
    command = manifest["command"]
    require(command[0] in ("scripts/audit_polymarket_wallet_pairs.py",
                          "/home/ubuntu/prediction_markets/scripts/audit_polymarket_wallet_pairs.py") and
            (len(command) - 1) % 2 == 0, "unexpected census command")
    flags = {}
    for index in range(1, len(command), 2):
        flag, value = command[index:index + 2]
        require(flag not in flags and flag in {"--expected-head", "--reviewed-preflight",
                "--reviewed-preflight-sha256", "--run-dir", "--root", "--clean"}, "unexpected/duplicate CLI flag")
        flags[flag] = value
    require(flags.get("--expected-head") == HEAD and
            flags.get("--reviewed-preflight-sha256") == approved_sha256 and
            flags.get("--reviewed-preflight") == manifest["reviewed_preflight_path"] and
            manifest["reviewed_preflight_sha256"] == approved_sha256 and
            bool(flags.get("--run-dir")) and Path(flags["--run-dir"]).is_absolute(), "frozen CLI binding failed")
    for relation in RELATIONS:
        require(flags.get("--" + relation, manifest["inputs"][relation]) == manifest["inputs"][relation],
                "CLI relation differs from frozen input")
    return {"exit_status": 0, "timed_command": timed, "body_run_dir": flags["--run-dir"],
            "reviewed_preflight_path": flags["--reviewed-preflight"]}


def review(preflight: dict, manifest: dict, summary: dict, profile: str,
           identities: dict, approved_sha256: str) -> dict:
    require(re.fullmatch(r"[0-9a-f]{64}", approved_sha256) is not None and
            identities["preflight"]["sha256"] == approved_sha256, "approved preflight digest mismatch")
    require(preflight["status"] == "preflight_complete" and manifest["status"] == COMPLETE and
            manifest["census"]["status"] == COMPLETE, "saved census not complete")
    require(preflight["expected_head"] == HEAD and strict_equal(preflight["source_sha256"], SOURCES) and
            preflight["contract_sha256"] == SOURCES["docs/analysis_specs/polymarket_wallet_pair_census_v1.json"] and
            strict_equal(preflight["caps"], CAPS) and strict_equal(preflight["inputs"], INPUTS),
            "approved frozen source/contract/caps/inputs differ")
    for field in FROZEN_FIELDS:
        require(strict_equal(preflight[field], manifest[field]), "frozen preflight binding differs: " + field)
    census = manifest["census"]
    require(manifest["data_certified"] is False and census["data_certified"] is False and
            census["final_input_identity_reopened"] is True, "false certification or missing final reopen")
    require(not ({"active_leaf", "active_admission", "error_class", "failure_reason", "blocked_original_query"} & census.keys()),
            "complete result retains partial/error state")
    require(census["completed_months"] == months() and sorted(census["months"]) == months(), "44-month completion failed")
    expected_summary = {field: manifest[field] for field in SUMMARY_FIELDS}
    expected_summary["census"] = {field: value for field, value in census.items() if field not in {"leaves", "splits"}}
    expected_summary.update(manifest_bytes=identities["manifest"]["bytes"], manifest_sha256=identities["manifest"]["sha256"])
    require(strict_equal(summary, expected_summary), "compact summary is not the exact manifest projection")
    require(identities["manifest"]["bytes"] + identities["summary"]["bytes"] <= MAX_MANIFEST,
            "saved census publication exceeds combined output cap")
    profile_receipt = exit_binding(profile, manifest, approved_sha256)

    inventories = {}
    for relation in RELATIONS:
        infos = preflight["inventories"][relation]
        require(len(infos) == 44 and {info["partition_month"] for info in infos} == set(months()), "inventory month coverage failed")
        inventories[relation] = {info["partition_month"]: info for info in infos}
        require(sum(count(info["rows"]) for info in infos) == count(preflight["footer_rows"][relation]), "footer total differs")
        for month, info in inventories[relation].items():
            lower, upper = bounds(month)
            require(sum(count(group["rows"]) for group in info["row_groups"]) == info["rows"], "row-group/footer law failed")
            for group in info["row_groups"]:
                stat = group["stats"]["timestamp"]
                require(type(stat["min"]) is int and type(stat["max"]) is int and
                        lower <= stat["min"] <= stat["max"] < upper, "footer timestamp not month-confined")
                count(group["compressed_bytes"])

    nodes = {}
    for kind, records in (("leaf", census["leaves"]), ("split", census["splits"])):
        for item in records:
            interval = key(item)
            require(interval not in nodes, "duplicate admission interval")
            nodes[interval] = (kind, item)
    require(len(nodes) == count(census["admission_nodes"]) <= CAPS["maximum_admission_nodes"], "admission-node law/cap failed")
    roots = [key(item) for item in preflight["initial_plan"]]
    require(len(roots) == len(set(roots)), "duplicate initial interval")
    seen, plans, leaf_records = set(), [], {month: [] for month in months()}
    charges = {"admission": 0, "grouping": 0}
    for month in months():
        intervals = sorted((lower, upper) for current, lower, upper in roots if current == month)
        require(bool(intervals) and intervals[0][0] == bounds(month)[0] and intervals[-1][1] == bounds(month)[1] and
                all(left[1] == right[0] for left, right in zip(intervals, intervals[1:])), "initial month partition incomplete")
    pending = list(roots)
    while pending:
        interval = pending.pop()
        require(interval in nodes and interval not in seen, "missing or multiply reached admission node")
        seen.add(interval)
        kind, item = nodes[interval]
        month, lower, upper = interval
        support = item["metrics"]["support"] if kind == "leaf" else item["support"]
        support_valid(support)
        require(admitted(support) is (kind == "leaf"), "leaf admission/split resource status differs")
        saved_plans = item["source_scan_plans"]
        require(set(saved_plans) == set(RELATIONS), "scan receipt relations differ")
        for relation in RELATIONS:
            groups = [group for group in inventories[relation][month]["row_groups"]
                      if group["stats"]["timestamp"]["min"] < upper and group["stats"]["timestamp"]["max"] >= lower]
            empty = sum(group["rows"] for group in groups) == 0
            footprint = sum(group["compressed_bytes"] for group in groups)
            stages = {"admission", "grouping"} if kind == "leaf" else {"admission"}
            require(set(saved_plans[relation]) == stages, "scan receipt stages differ")
            for stage, plan in saved_plans[relation].items():
                scans = count(plan["parquet_scan_operators"])
                require((scans == 1 and plan["footer_proved_empty_result"] is False) or
                        (scans == 0 and empty and plan["footer_proved_empty_result"] is True), "saved one-scan/empty proof failed")
                require(re.fullmatch(r"[0-9a-f]{64}", plan["physical_plan_sha256"]) is not None, "invalid saved plan digest")
                charges[stage] += footprint
        plans.append({"kind": kind, "month": month, "lower_inclusive": lower, "upper_exclusive": upper,
                      "source_scan_plans": saved_plans})
        if kind == "leaf":
            arithmetic(item["metrics"])
            leaf_records[month].append(item["metrics"])
        else:
            if (lower, upper) == bounds(month):
                children = [(month, value, min(value + 86400, upper)) for value in range(lower, upper, 86400)]
                require(item["split_kind"] == "complete_utc_days", "whole-month split is not complete days")
            else:
                require(upper - lower > 1 and item["split_kind"] == "integer_second_bisection", "invalid second bisection")
                middle = (lower + upper) // 2
                children = [(month, lower, middle), (month, middle, upper)]
            pending.extend(children)
    require(seen == set(nodes), "unreachable admission interval")
    for item in census["splits"]:
        month, lower, upper = key(item)
        descendants = [leaf["metrics"]["support"] for leaf in census["leaves"]
                       if leaf["month"] == month and lower <= leaf["lower_inclusive"] < leaf["upper_exclusive"] <= upper]
        summed = sum_records(descendants)
        for relation in RELATIONS:
            for field, value in item["support"][relation].items():
                if type(value) is int:
                    require(value == summed[relation][field], "split/descendant support integer law failed")
    coverage = {}
    for month in months():
        computed = sum_records(leaf_records[month])
        require(strict_equal(computed, census["months"][month]), "leaf/month arithmetic differs")
        arithmetic(computed)
        for relation in RELATIONS:
            require(computed["support"][relation]["row_count"] == inventories[relation][month]["rows"], "month/footer rows differ")
        coverage[month] = {"lower_inclusive": bounds(month)[0], "upper_exclusive": bounds(month)[1],
                           "leaf_count": len(leaf_records[month]),
                           "footer_rows": {relation: inventories[relation][month]["rows"] for relation in RELATIONS}}
    require(strict_equal(sum_records([census["months"][month] for month in months()]), census["global"]), "month/global arithmetic differs")
    arithmetic(census["global"])
    for relation in RELATIONS:
        require(census["global"]["support"][relation]["row_count"] == preflight["footer_rows"][relation], "global/footer rows differ")
    cleaning = census["global"]["cleaning"]
    require(census["root_distinct_to_clean_reconciled"] is (cleaning["failed_full11_reconciliation_leaves"] == 0) and
            bool(cleaning["failed_full11_reconciliation_leaves"]) == bool(cleaning["expected_clean_only_rows"] or cleaning["clean_only_rows"]),
            "cleaning status/counts differ")
    require(charges["admission"] == count(census["admission_compressed_footprint_bytes"]) and
            charges["grouping"] == count(census["grouping_compressed_footprint_bytes"]) and
            sum(charges.values()) == count(census["planned_original_read_footprint_bytes"]) <=
            CAPS["maximum_planned_read_footprint_bytes"], "read footprint law/cap failed")
    return {"schema_version": 1, "status": "complete_saved_wallet_census_manifest_qa", "data_certified": False,
            "inputs": identities, "approved_preflight_sha256": approved_sha256,
            "frozen_bindings": {field: preflight[field] for field in
                                ("expected_head", "source_sha256", "contract_sha256", "inputs", "caps", "environment")},
            "exit_profile": profile_receipt, "completed_months": months(), "coverage": coverage,
            "leaf_count": len(census["leaves"]), "split_count": len(census["splits"]),
            "admission_nodes": len(nodes), "read_footprint_bytes": charges,
            "planned_original_read_footprint_bytes": sum(charges.values()), "scan_plan_receipts": plans,
            "final_input_identity_reopened_recorded": True,
            "limits": "Saved metadata consistency only. Trade files and query plans were not executed or independently reopened. Plan operator counts/physical digests and final identity reopening remain recorded producer evidence. This is not native identity, economic-action, historical-writer, particular clean-removal attribution or data certification; footprints are planned, not measured I/O."}


def build_review(preflight_path: Path, approved_sha256: str, manifest_path: Path,
                 summary_path: Path, profile_path: Path, destination: Path) -> dict:
    require(not destination.exists(), "immutable QA directory already exists")
    documents, identities = {}, {}
    for name, path, limit, is_json in (("preflight", preflight_path, MAX_MANIFEST, True),
            ("manifest", manifest_path, MAX_MANIFEST, True), ("summary", summary_path, MAX_RECEIPT, True),
            ("exit_profile", profile_path, MAX_PROFILE, False)):
        require(destination.resolve() != path.resolve() and destination.resolve() not in path.resolve().parents,
                "QA output overlaps an input")
        documents[name], identities[name] = read_saved(path, limit, json_object=is_json)
    result = review(documents["preflight"], documents["manifest"], documents["summary"],
                    documents["exit_profile"], identities, approved_sha256)
    result["reviewer_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    encoded = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")
    require(len(encoded) <= MAX_RECEIPT, "complete QA receipt exceeds 1MiB; no truncation")
    destination.mkdir(parents=True, exist_ok=False)
    partial = destination / "receipt.json.partial"
    with partial.open("xb") as stream:
        stream.write(encoded); stream.flush(); os.fsync(stream.fileno())
    os.replace(partial, destination / "receipt.json")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-manifest", type=Path, required=True)
    parser.add_argument("--approved-preflight-sha256", required=True)
    parser.add_argument("--census-manifest", type=Path, required=True)
    parser.add_argument("--census-summary", type=Path, required=True)
    parser.add_argument("--exit-profile", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_review(args.preflight_manifest, args.approved_preflight_sha256, args.census_manifest,
                          args.census_summary, args.exit_profile, args.run_dir)
    print(json.dumps({"status": result["status"], "data_certified": False, "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
