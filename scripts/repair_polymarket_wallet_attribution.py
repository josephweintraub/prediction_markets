#!/usr/bin/env python3
"""Publish immutable wallet-only copies of the frozen ROOT and CLEAN vintage.

Only synthetic non-maker proxyWallet/counterparty fields are reversed. Native
identity, economic-action inference, cleaning, flags and scientific estimates
are not reconstructed. A separately reviewed preflight is required for writing.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import audit_polymarket_lineage as lineage

CENSUS_SHA256 = "91f14b86c452496879e49181713ff6a4654b52c1cf4f46967d9ff3772ea7ba4c"
QA_SHA256 = "8d155da22c591e69ea77825dac793b866c7a28c9651b692f4bb27351bd4c1faa"
COMPLETE = "published_pair_census_complete"
FIELDS = lineage.VALUE_FIELDS
OTHER_FIELDS = tuple(field for field in FIELDS if field not in {"proxyWallet", "counterparty"})
RELATIONS = {"root": "root_transformed", "clean": "clean"}
CAPS = {"memory_limit": "64GB", "threads": 4, "spill_bytes": 0,
        "maximum_leaf_rows": 25_000_000, "maximum_leaf_payload_bytes": 16 * 1024**3,
        "maximum_read_bytes": 16 * 1024**4, "minimum_free_bytes": 5 * 1024**3,
        "output_expansion_factor": 2, "metadata_allowance_per_file": 1024**2,
        "maximum_manifest_bytes": 4 * 1024**2, "maximum_census_bytes": 128 * 1024**2}
SOURCE_PATHS = ("scripts/repair_polymarket_wallet_attribution.py", "tests/test_polymarket_wallet_repair.py",
                "scripts/audit_polymarket_lineage.py", "production_guard.py")


class RepairBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise RepairBlocked(message)


def stat_identity(path: Path) -> dict:
    value = path.stat()
    require(path.is_file() and not path.is_symlink(), "regular non-symlink file required: " + str(path))
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def sha256(path: Path) -> str:
    before = stat_identity(path)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    require(stat_identity(path) == before, "file changed while hashing: " + str(path))
    return digest.hexdigest()


def read_json(path: Path, expected_sha256: str, limit: int) -> tuple[dict, dict]:
    before = stat_identity(path)
    require(0 < before["bytes"] <= limit, "JSON input is empty or exceeds bound")
    raw = path.read_bytes()
    require(stat_identity(path) == before and len(raw) == before["bytes"], "JSON input changed during read")
    digest = hashlib.sha256(raw).hexdigest()
    require(re.fullmatch(r"[0-9a-f]{64}", expected_sha256) and digest == expected_sha256,
            "JSON input differs from required SHA256")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    value = json.loads(raw, object_pairs_hook=unique_object,
                       parse_constant=lambda token: (_ for _ in ()).throw(RepairBlocked(token)))
    require(isinstance(value, dict), "JSON object required")
    # Refuse overflowed finite-looking JSON numbers as well as NaN constants.
    json.dumps(value, allow_nan=False)
    return value, {"path": str(path.resolve()), "bytes": len(raw), "sha256": digest}


def write_json(path: Path, value: dict):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= CAPS["maximum_manifest_bytes"], "complete manifest exceeds bound; no truncation")
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def atomic_publish(staging: Path, target: Path):
    """Never replace an existing target, including a concurrent publication."""
    library = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        result = library.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = library.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise RepairBlocked("atomic no-replace publication is unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def admit_census(manifest: dict, qa: dict, *, months=None) -> list[dict]:
    """Fixture callers may supply one month; the production CLI always uses all 44."""
    months = list(lineage.canonical_months() if months is None else months)
    census = manifest["census"]
    require(manifest["status"] == census["status"] == COMPLETE and
            manifest["data_certified"] is False and census["data_certified"] is False,
            "completed original census with explicit certification limits required")
    require(qa["status"] == "complete_saved_wallet_census_manifest_qa" and qa["data_certified"] is False and
            qa["exit_profile"]["exit_status"] == 0 and qa["final_input_identity_reopened_recorded"] is True,
            "completed census QA and successful durable exit required")
    require(census["final_input_identity_reopened"] is True and census["completed_months"] == months and
            qa["completed_months"] == months and sorted(census["months"]) == months,
            "complete frozen month coverage required")
    require(census["root_distinct_to_clean_reconciled"] is True and
            census["global"]["cleaning"]["failed_full11_reconciliation_leaves"] == 0,
            "original full-eleven-field cleaning reconciliation required")
    require(set(manifest["inventories"]) == set(RELATIONS), "ROOT/CLEAN input inventory required")
    plan = []
    for relation, canonical_relation in RELATIONS.items():
        infos = manifest["inventories"][relation]
        require(len(infos) == len(months) and {info["partition_month"] for info in infos} == set(months),
                "one original file per relation/month required")
        by_month = {info["partition_month"]: info for info in infos}
        total = 0
        for month in months:
            info = by_month[month]
            require(info["relation"] == canonical_relation and type(info["rows"]) is int and info["rows"] > 0,
                    "original relation or footer count differs")
            require(Path(info["path"]).parent.name == "year_month=" + month and
                    Path(info["path"]).parent.parent == Path(manifest["inputs"][relation]),
                    "file path differs from the frozen relation/month")
            expected_fields = set(FIELDS) - ({"year_month"} if relation == "root" else set())
            require(set(info["fields"]) == expected_fields, "unexpected or missing original physical fields")
            leaves = sorted((leaf for leaf in census["leaves"] if leaf["month"] == month),
                            key=lambda leaf: leaf["lower_inclusive"])
            lower, upper = lineage.month_bounds(month)
            position, counted, admitted_leaves = lower, 0, []
            for leaf in leaves:
                start, end = leaf["lower_inclusive"], leaf["upper_exclusive"]
                require(type(start) is int and type(end) is int and start == position < end <= upper,
                        "leaf gap, overlap or non-integer timestamp boundary")
                support = leaf["metrics"]["support"][relation]
                pairs = leaf["metrics"][relation]["full_label"]
                require(type(support["row_count"]) is int and 0 <= support["row_count"] <= CAPS["maximum_leaf_rows"] and
                        type(support["logical_payload_bytes"]) is int and 0 <=
                        2 * support["logical_payload_bytes"] <= CAPS["maximum_leaf_payload_bytes"] and
                        support["invalid_rows"] == support["missing_role_rows"] == 0 and
                        support["maker_rows"] + support["nonmaker_rows"] == support["row_count"],
                        "original leaf integrity/resource admission failed")
                require(pairs["excess_observed_copied"] == pairs["missing_observed_copied"] == 0 and
                        pairs["maker_rows"] == pairs["nonmaker_rows"] == support["maker_rows"] and
                        pairs["compatibility"]["correct_only"]["nonmaker_rows"] == 0 and
                        pairs["compatibility"]["neither"]["nonmaker_rows"] == 0,
                        "original copied-wallet construction is not proved; corrected/mixed input refused")
                admitted_leaves.append({"lower_inclusive": start, "upper_exclusive": end,
                                        "support": support})
                position, counted = end, counted + support["row_count"]
            require(position == upper and counted == info["rows"] ==
                    census["months"][month]["support"][relation]["row_count"],
                    "leaf/month/footer reconciliation failed")
            total += counted
            plan.append({"relation": relation, "month": month, "input": info, "leaves": admitted_leaves})
        require(total == manifest["footer_rows"][relation] == census["global"]["support"][relation]["row_count"],
                "global/footer row reconciliation failed")
    return plan


def load_census(manifest_path: Path, qa_path: Path):
    manifest, manifest_id = read_json(manifest_path, CENSUS_SHA256, CAPS["maximum_census_bytes"])
    qa, qa_id = read_json(qa_path, QA_SHA256, CAPS["maximum_manifest_bytes"])
    require(qa["inputs"]["manifest"]["sha256"] == manifest_id["sha256"] and
            qa["inputs"]["manifest"]["bytes"] == manifest_id["bytes"], "QA receipt is not bound to this census")
    return admit_census(manifest, qa), {"census": manifest_id, "census_qa": qa_id}


def verify_original(info: dict) -> dict:
    frozen_stat = stat_identity(Path(info["path"]))
    actual = lineage.footer_info(Path(info["path"]), info["relation"])
    for field in ("bytes", "mtime_ns", "rows", "schema", "footer_sha256", "row_groups", "fields"):
        require(actual[field] == info[field], "original input identity mismatch: " + info["path"] + "/" + field)
    require(stat_identity(Path(info["path"])) == frozen_stat, "original changed during identity check")
    return frozen_stat


def overlap_bytes(info: dict, lower: int, upper: int) -> int:
    total = 0
    for group in info["row_groups"]:
        stat = group["stats"]["timestamp"]
        require(stat and stat["null_count"] == 0, "timestamp footer statistics missing or null")
        if stat["min"] < upper and stat["max"] >= lower:
            total += group["compressed_bytes"]
    return total


def source_snapshot(expected_head: str) -> dict:
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head), "full expected source HEAD required")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    require(head == expected_head, "canonical source HEAD differs")
    hashes = {}
    for name in SOURCE_PATHS:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=REPO)
        require(tracked.returncode == unchanged.returncode == 0, "repair source must be committed and unchanged: " + name)
        hashes[name] = sha256(REPO / name)
    return {"head": head, "sha256": hashes}


def existing_parent(path: Path) -> Path:
    while not path.exists():
        path = path.parent
    return path


def preflight(plan: list[dict], target: Path, binding: dict, source: dict) -> dict:
    require(not target.exists() and not target.is_symlink(), "immutable repair destination already exists")
    target = target.resolve()
    require(not target.exists() and not target.is_symlink(), "immutable repair destination already exists")
    lineage.verify_snapshots([item["input"] for item in plan])
    inputs, input_bytes, read_bytes = [], 0, 0
    for item in plan:
        info = item["input"]
        path = Path(info["path"]).resolve()
        require(target != path and target not in path.parents and path.parent.parent not in target.parents,
                "repair destination overlaps original data")
        identity = verify_original(info)
        inputs.append({"relation": item["relation"], "month": item["month"], "path": info["path"],
                       "stat": identity, "footer_sha256": info["footer_sha256"], "rows": info["rows"],
                       "schema": info["schema"]})
        input_bytes += info["bytes"]
        # Source COPY + two input hashes + one capped output hash. Each bounded
        # leaf has one scalar admission scan and one materialization scan per
        # relation; the four exact multiset comparisons then use only tables.
        read_bytes += 5 * info["bytes"] + 2 * ((1 + CAPS["output_expansion_factor"]) * sum(
            overlap_bytes(info, leaf["lower_inclusive"], leaf["upper_exclusive"]) for leaf in item["leaves"]) +
            len(item["leaves"]) * CAPS["metadata_allowance_per_file"])
    output_cap = CAPS["output_expansion_factor"] * input_bytes + CAPS["metadata_allowance_per_file"] * len(plan)
    require(read_bytes <= CAPS["maximum_read_bytes"], "planned repair read footprint exceeds bound")
    required = output_cap + CAPS["minimum_free_bytes"]
    free = shutil.disk_usage(existing_parent(target.parent)).free
    require(free >= required, "insufficient capacity for complete immutable copies and free-space reserve")
    return {"schema_version": "polymarket_wallet_repair_v1", "status": "preflight_complete",
            "data_certified": False, "target": str(target), "binding": binding, "source": source,
            "caps": dict(CAPS), "inputs": inputs, "input_bytes": input_bytes,
            "maximum_output_bytes": output_cap, "required_free_bytes": required,
            "observed_free_bytes": free, "planned_read_bytes": read_bytes,
            "read_note": "Compressed-overlap planning estimate; output row groups are unknown until written. Actual charged reads remain capped.",
            "leaf_memory_contract": "Scalar-admit original/output pair <=16GiB, materialize only that pair, exact comparisons in memory, drop before next leaf; 64GB DuckDB/0B spill.",
            "write_contract": "COPY RLIMIT_FSIZE ceiling is twice original file bytes; per-file metadata allowance and 5GiB reserve are separate. Run as the sole production stage.",
            "leaf_count_per_relation": sum(len(item["leaves"]) for item in plan)}


def reserve_read(budget: dict, amount: int, stage: str):
    require(type(amount) is int and amount >= 0 and
            budget["read_bytes"] + amount <= CAPS["maximum_read_bytes"], "repair read budget exhausted before query")
    budget["read_bytes"] += amount
    budget.setdefault("read_charges", []).append({"stage": stage, "bytes": amount,
        "file": budget.get("active_file"), "leaf": budget.get("active_leaf")})


def connection(staging: Path):
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{CAPS['memory_limit']}'")
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute("SET max_temp_directory_size='0B'")
    con.execute("SET temp_directory=?", [str(staging / "spill")])
    # Keep the existing physical timestamp locality without a global sort.
    con.execute("SET preserve_insertion_order=true")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET enable_object_cache=false")
    if con.execute("SELECT 1 FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone():
        con.execute("SET disabled_optimizers='common_subplan'")
    return con


@contextmanager
def copy_size_limit(maximum_bytes: int):
    """Bound the sole COPY's file writes; preserve stricter inherited limits."""
    require(type(maximum_bytes) is int and maximum_bytes > 0, "positive COPY write ceiling required")
    previous = resource.getrlimit(resource.RLIMIT_FSIZE)
    previous_signal = signal.getsignal(signal.SIGXFSZ)
    finite = [value for value in previous if value != resource.RLIM_INFINITY]
    ceiling = min([maximum_bytes, *finite])
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    try:
        resource.setrlimit(resource.RLIMIT_FSIZE, (ceiling, previous[1]))
        yield
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, previous)
        signal.signal(signal.SIGXFSZ, previous_signal)


def projected_fields(fields, *, repaired: bool) -> str:
    result = []
    for field in fields:
        if repaired and field in {"proxyWallet", "counterparty"}:
            other = "counterparty" if field == "proxyWallet" else "proxyWallet"
            result.append(f"CASE WHEN is_maker THEN {lineage.qname(field)} ELSE {lineage.qname(other)} END AS {lineage.qname(field)}")
        else:
            result.append(lineage.qname(field))
    return ",".join(result)


def leaf_view(con, name: str, path: Path, fields, month: str, lower: int, upper: int, *, repaired=False):
    selected = projected_fields(fields, repaired=repaired)
    if "year_month" not in fields:
        selected += "," + lineage.literal(month) + ' AS "year_month"'
    con.execute(f"DROP VIEW IF EXISTS {name}")
    con.execute(f"CREATE TEMP VIEW {name} AS SELECT {selected} FROM read_parquet({lineage.literal(str(path))},hive_partitioning=false) "
                f"WHERE timestamp>={lower} AND timestamp<{upper}")


def full_difference(con, left: str, right: str, fields, budget: dict, footprint: int) -> dict:
    selected = ",".join(lineage.qname(field) for field in fields)
    result = {}
    for key, first, second in (("left_only_rows", left, right), ("right_only_rows", right, left)):
        reserve_read(budget, footprint, f"exact:{first}:except_all:{second}")
        result[key] = con.execute(f"SELECT count(*) FROM (SELECT {selected} FROM {first} EXCEPT ALL SELECT {selected} FROM {second})").fetchone()[0]
    return result


def check_leaf(con, item: dict, output: Path, output_info: dict, leaf: dict, budget: dict) -> dict:
    source = Path(item["input"]["path"])
    fields = pq.ParquetFile(source).schema_arrow.names
    lower, upper = leaf["lower_inclusive"], leaf["upper_exclusive"]
    budget["active_leaf"] = {"lower_inclusive": lower, "upper_exclusive": upper}
    leaf_view(con, "original_leaf", source, fields, item["month"], lower, upper)
    leaf_view(con, "output_leaf", output, fields, item["month"], lower, upper)
    source_footprint = overlap_bytes(item["input"], lower, upper)
    output_footprint = overlap_bytes(output_info, lower, upper)
    counts = {}
    strings = [field for field in FIELDS if field not in {"timestamp", "usdcSize", "price", "is_maker"}]
    lengths = "+".join(f"coalesce(octet_length(encode({lineage.qname(field)})),0)" for field in strings)
    for relation, footprint in (("original_leaf", source_footprint), ("output_leaf", output_footprint)):
        reserve_read(budget, footprint, "leaf_counts:" + relation)
        counts[relation] = lineage.scalar(con, f"""SELECT count(*) row_count,
          coalesce(sum(({lengths})::HUGEINT),0)+83*count(*) logical_payload_bytes,
          count(*) FILTER(WHERE is_maker) maker_rows,count(*) FILTER(WHERE NOT is_maker) nonmaker_rows,
          count(*) FILTER(WHERE is_maker IS NULL OR proxyWallet IS NULL OR trim(proxyWallet)='' OR
            counterparty IS NULL OR trim(counterparty)='' OR conditionId IS NULL OR trim(conditionId)='' OR
            year_month IS DISTINCT FROM {lineage.literal(item['month'])}) invalid_rows FROM {relation}""")
        expected = {key: leaf["support"][key] for key in ("row_count", "maker_rows", "nonmaker_rows", "invalid_rows", "logical_payload_bytes")}
        require(counts[relation] == expected, "leaf role/key/count reconciliation failed")
    require(sum(value["logical_payload_bytes"] for value in counts.values()) <= CAPS["maximum_leaf_payload_bytes"],
            "original/output logical payload exceeds the admitted leaf resource bound")
    try:
        for relation, footprint in (("original", source_footprint), ("output", output_footprint)):
            reserve_read(budget, footprint, "leaf_materialize:" + relation)
            con.execute(f"CREATE TEMP TABLE {relation}_values AS SELECT * FROM {relation}_leaf")
        con.execute(f"CREATE TEMP VIEW expected_values AS SELECT {projected_fields(FIELDS, repaired=True)} FROM original_values")
        full = full_difference(con, "expected_values", "output_values", FIELDS, budget, 0)
        other = full_difference(con, "original_values", "output_values", OTHER_FIELDS, budget, 0)
        require(not any(full.values()) and not any(other.values()), "exact wallet-only multiset reconciliation failed")
        return {"lower_inclusive": lower, "upper_exclusive": upper, "counts": counts["output_leaf"],
                "materialized_pair_payload_bytes": sum(value["logical_payload_bytes"] for value in counts.values()),
                "full11_expected_to_output": full, "unchanged_other9_fields": other}
    finally:
        con.execute("DROP VIEW IF EXISTS expected_values")
        con.execute("DROP TABLE IF EXISTS output_values")
        con.execute("DROP TABLE IF EXISTS original_values")


def repair_file(item: dict, run_staging: Path, reviewed_input: dict, budget: dict) -> dict:
    info = item["input"]
    source = Path(info["path"])
    budget["active_file"] = {"relation": item["relation"], "month": item["month"]}
    budget.pop("active_leaf", None)
    require(reviewed_input["path"] == info["path"] and reviewed_input["relation"] == item["relation"] and
            reviewed_input["month"] == item["month"], "file plan differs from reviewed input")
    require(verify_original(info) == reviewed_input["stat"], "input stat differs from reviewed preflight")
    reserve_read(budget, info["bytes"], "input_sha256_before")
    before_hash = sha256(source)
    parent = run_staging / item["relation"]
    parent.mkdir(exist_ok=True)
    target = parent / ("year_month=" + item["month"])
    require(not target.exists(), "completed per-file stage already exists")
    staging = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=parent))
    file_cap = CAPS["output_expansion_factor"] * info["bytes"] + CAPS["metadata_allowance_per_file"]
    require(shutil.disk_usage(staging).free >= file_cap + CAPS["minimum_free_bytes"], "insufficient capacity before file COPY")
    output = staging / "data.parquet"
    con = None
    try:
        con = connection(staging)
        schema = pq.ParquetFile(source).schema_arrow
        reserve_read(budget, info["bytes"], "wallet_only_copy")
        copy_cap = CAPS["output_expansion_factor"] * info["bytes"]
        with copy_size_limit(copy_cap):
            con.execute(f"COPY (SELECT {projected_fields(schema.names, repaired=True)} FROM "
                        f"read_parquet({lineage.literal(str(source))},hive_partitioning=false)) TO "
                        f"{lineage.literal(str(output))} (FORMAT PARQUET,COMPRESSION ZSTD)")
        require(output.stat().st_size <= copy_cap and
                shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "file output cap/free-space reserve exceeded")
        require(pq.ParquetFile(output).schema_arrow.equals(schema, check_metadata=True), "physical schema changed during wallet repair")
        output_info = lineage.footer_info(output, info["relation"])
        require(output_info["rows"] == info["rows"], "file row count changed")
        lower, upper = lineage.month_bounds(item["month"])
        require(all(group["stats"]["timestamp"] and group["stats"]["timestamp"]["null_count"] == 0 and
                    lower <= group["stats"]["timestamp"]["min"] <= group["stats"]["timestamp"]["max"] < upper
                    for group in output_info["row_groups"]), "output timestamp footer escapes the admitted UTC month")
        source_overlap = sum(overlap_bytes(info, leaf["lower_inclusive"], leaf["upper_exclusive"]) for leaf in item["leaves"])
        output_overlap = sum(overlap_bytes(output_info, leaf["lower_inclusive"], leaf["upper_exclusive"]) for leaf in item["leaves"])
        require(output_overlap <= CAPS["output_expansion_factor"] * source_overlap +
                len(item["leaves"]) * CAPS["metadata_allowance_per_file"],
                "output timestamp pruning worsened beyond the reviewed footprint allowance")
        checks = [check_leaf(con, item, output, output_info, leaf, budget) for leaf in item["leaves"]]
        require(sum(check["counts"]["row_count"] for check in checks) == info["rows"], "complete output leaves do not cover file rows")
        con.close()
        con = None
        budget.pop("active_leaf", None)
        reserve_read(budget, output.stat().st_size, "output_sha256")
        output_hash = sha256(output)
        reserve_read(budget, info["bytes"], "input_sha256_after")
        require(sha256(source) == before_hash and verify_original(info) == reviewed_input["stat"],
                "original input changed during repair")
        manifest = {"schema_version": "polymarket_wallet_repair_file_v1", "status": "repair_file_complete",
                    "relation": item["relation"], "month": item["month"], "data_certified": False,
                    "input": {**reviewed_input, "sha256": before_hash},
                    "output": {"path": "data.parquet", "bytes": output.stat().st_size,
                               "sha256": output_hash, "rows": output_info["rows"], "schema": str(schema)},
                    "maximum_copy_file_bytes": copy_cap,
                    "separately_reserved_metadata_bytes": CAPS["metadata_allowance_per_file"],
                    "reconciliation": checks, "grain": "Original published wallet-row multiplicity",
                    "timestamp_pruning": {"source_overlap_bytes": source_overlap, "output_overlap_bytes": output_overlap,
                                          "preserve_insertion_order": True, "global_sort_performed": False},
                    "transform": "Swap proxyWallet/counterparty only when is_maker=false; all other fields unchanged",
                    "counterparty_action": "Original inferred opposite action remains unverified"}
        write_json(staging / "manifest.json", manifest)
        require(sum(path.stat().st_size for path in staging.rglob("*") if path.is_file()) <= file_cap,
                "complete file stage exceeds output allowance")
        atomic_publish(staging, target)
        return {"relation": item["relation"], "month": item["month"],
                "path": str(target.relative_to(run_staging)), "input": manifest["input"],
                "output": manifest["output"], "manifest_sha256": sha256(target / "manifest.json"),
                "validation_leaves": len(checks)}
    finally:
        if con is not None:
            con.close()


def build_run(plan: list[dict], target: Path, reviewed: dict, fresh: dict, *, command=None) -> dict:
    for field in ("schema_version", "target", "binding", "source", "caps", "inputs", "input_bytes",
                  "maximum_output_bytes", "required_free_bytes", "planned_read_bytes", "leaf_count_per_relation",
                  "leaf_memory_contract", "write_contract"):
        require(reviewed[field] == fresh[field], "fresh preflight differs from reviewed contract: " + field)
    require(fresh["status"] == reviewed["status"] == "preflight_complete", "reviewed preflight is incomplete")
    target = Path(target).resolve()
    require(str(target) == fresh["target"] and not target.exists(), "immutable run destination differs or exists")
    require(len(plan) == len(reviewed["inputs"]), "file plan coverage differs before writing")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    started = time.monotonic()
    budget = {"read_bytes": 0, "read_charges": []}
    outputs = []
    try:
        for item, reviewed_input in zip(plan, reviewed["inputs"]):
            output = repair_file(item, staging, reviewed_input, budget)
            outputs.append(output)
            require(sum(value["output"]["bytes"] for value in outputs) <= fresh["maximum_output_bytes"],
                    "complete repair output cap exceeded")
            print(json.dumps({"stage": "file_complete", "relation": item["relation"], "month": item["month"],
                              "rows": output["output"]["rows"], "read_bytes": budget["read_bytes"]}),
                  file=sys.stderr, flush=True)
        require(len(outputs) == len(plan) == len(reviewed["inputs"]), "file plan coverage incomplete")
        for item, frozen in zip(plan, reviewed["inputs"]):
            require(verify_original(item["input"]) == frozen["stat"], "original changed before final publication")
        lineage.verify_snapshots([item["input"] for item in plan])
        summary = {"schema_version": "polymarket_wallet_repair_v1", "status": "repair_complete",
                   "downstream_adoption": "pending", "data_certified": False,
                   "binding": fresh["binding"], "source": fresh["source"], "caps": fresh["caps"],
                   "leaf_memory_contract": fresh["leaf_memory_contract"], "write_contract": fresh["write_contract"],
                   "command": command or [], "inputs": reviewed["inputs"], "outputs": outputs,
                   "rows": {relation: sum(value["output"]["rows"] for value in outputs if value["relation"] == relation)
                            for relation in RELATIONS},
                   "reconciliation": {"exact_full11_multisets": True, "unchanged_other9_fields": True,
                                      "original_file_counts_types_and_multiplicities": True,
                                      "root_distinct_to_clean_preserved_by_bijective_transform": True,
                                      "original_inputs_reopened": True},
                   "environment": {"python": sys.version, "duckdb": duckdb.__version__, "platform": sys.platform},
                   "resource_profile": {"read_bytes_charged": budget["read_bytes"], "wall_seconds": time.monotonic()-started,
                                        "peak_rss_bytes": lineage.peak_rss_bytes(),
                                        "memory_note": "64GB bounds DuckDB-managed memory, not process RSS; admitted leaf-pair tables are dropped before next leaf, spill is forbidden."},
                   "read_charges": budget["read_charges"],
                   "limits": "Wallet-only published representation repair; no native identity/action certification, cleaning changes, flag/base rebuild or scientific rerun.",
                   "downstream_gate": "Previously saved wallet flags and analytic bases require independent lineage/revalidation before adoption of this repaired vintage."}
        write_json(staging / "manifest.json", summary)
        write_json(staging / "summary.json", {**summary, "manifest_sha256": sha256(staging / "manifest.json")})
        require(sum(path.stat().st_size for path in staging.rglob("*") if path.is_file()) <= fresh["maximum_output_bytes"] and
                shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "final output/free-space reserve gate failed")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "source changed before publication")
        atomic_publish(staging, target)
        return summary
    except BaseException as error:
        if staging.exists():
            failure = staging / "failure.json"
            if not failure.exists():
                write_json(failure, {"status": "repair_incomplete", "error_type": type(error).__name__,
                                    "reason": str(error), "completed_files": len(outputs), "read_bytes": budget["read_bytes"]})
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census-manifest", type=Path, required=True)
    parser.add_argument("--census-qa", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--preflight", type=Path, help="publish metadata-only preflight into this new directory")
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--approved-preflight-sha256")
    args = parser.parse_args()
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "use /home/ubuntu/venv/bin/python for production")
    source = source_snapshot(args.expected_head)
    plan, binding = load_census(args.census_manifest, args.census_qa)
    fresh = preflight(plan, args.run_dir, binding, source)
    if args.preflight:
        require(not args.reviewed_preflight and not args.approved_preflight_sha256, "preflight and body modes are exclusive")
        preflight_target = args.preflight.resolve()
        for item in plan:
            original = Path(item["input"]["path"]).resolve()
            require(preflight_target not in original.parents and original.parent.parent not in preflight_target.parents,
                    "preflight destination overlaps original data")
        args.preflight.mkdir(parents=True, exist_ok=False)
        write_json(args.preflight / "manifest.json", fresh)
        print(json.dumps({"status": fresh["status"], "manifest": str(args.preflight / "manifest.json"),
                          "maximum_output_bytes": fresh["maximum_output_bytes"], "required_free_bytes": fresh["required_free_bytes"]}))
        return 0
    require(args.reviewed_preflight and args.approved_preflight_sha256, "body requires a separately reviewed preflight digest")
    reviewed, reviewed_id = read_json(args.reviewed_preflight, args.approved_preflight_sha256, CAPS["maximum_manifest_bytes"])
    summary = build_run(plan, args.run_dir, reviewed, fresh, command=sys.argv)
    require(source_snapshot(args.expected_head) == source, "source changed during repair")
    print(json.dumps({"status": summary["status"], "run_dir": str(args.run_dir), "rows": summary["rows"],
                      "downstream_adoption": summary["downstream_adoption"], "reviewed_preflight": reviewed_id}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
