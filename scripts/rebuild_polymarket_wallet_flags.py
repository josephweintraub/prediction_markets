#!/usr/bin/env python3
"""Recompute immutable F0/F1 wallet flags from frozen legacy/repaired CLEAN.

F0 and F1 use the unchanged classifier, all published sides and the legacy
timestamp cutoff. Only F1 versus F0 isolates the saved wallet repair. Historical
flag comparisons also contain unresolved producer/cohort/vintage differences.
The CLI first publishes a metadata-only preflight; its reviewed SHA256 is
required for the serial body. This does not adopt flags or certify native data.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from analysis.bot_filter import build_wallet_flags
from scripts.repair_polymarket_wallet_attribution import (
    atomic_publish, copy_size_limit, read_json, sha256, stat_identity,
)

CLASSIFIER_SHA256 = "52ee90215b81346852fa775cc1f9df6d72b7b7d5dcbc6ef357c1b7e379fb42fe"
REPAIR_SHA256 = "f142313238e9be25541f97dac4ba24de7ad6b49484812e81e7c6640eefd50daa"
REPAIR_QA_SHA256 = "06d0fd376c7c6aede78016e50bc34cd63220cf8a15d481598303679d7de2cbb3"
START_TIMESTAMP = 1590969600
MONTHS = tuple(f"{year}-{month:02d}" for year in range(2022, 2027)
               for month in range(1, 13) if "2022-11" <= f"{year}-{month:02d}" <= "2026-06")
TRADE_TYPES = {"proxyWallet": "string", "timestamp": "int64", "conditionId": "string",
               "usdcSize": "double", "price": "double", "side": "string", "outcome": "string",
               "eventSlug": "string", "is_maker": "bool", "counterparty": "string", "year_month": "string"}
FLAGS = ("flag_a_definite", "flag_a_likely", "flag_b_definite", "flag_b_likely", "flag_c", "flag_e", "is_nonhuman")
FLAG_TYPES = {"proxyWallet": "string", "n_trades": "int64", "trades_per_active_day": "double",
              "active_days": "int64", "median_iti": "double", **{name: "bool" for name in FLAGS}}
SOURCE_PATHS = ("scripts/rebuild_polymarket_wallet_flags.py", "tests/test_rebuild_polymarket_wallet_flags.py",
                "analysis/bot_filter.py", "scripts/repair_polymarket_wallet_attribution.py",
                "scripts/audit_polymarket_lineage.py", "production_guard.py")
CAPS = {"memory_limit": "160GB", "threads": 8, "spill_bytes": 20 * 1024**3,
        "maximum_output_bytes": 2 * 1024**3, "minimum_free_bytes": 20 * 1024**3,
        "maximum_read_bytes": 1024**4, "maximum_metadata_bytes": 16 * 1024**2,
        "maximum_footer_bytes": 8 * 1024**2, "maximum_files": 256, "maximum_row_groups": 100_000}
CONTRACT = {"timestamp_lower_inclusive": START_TIMESTAMP, "sides": "all published sides",
            "wallet_grouping": "raw proxyWallet; normalized keys are validation/comparison only",
            "normalized_wallet_key": "lower(proxyWallet); padded source and flag keys fail closed",
            "classifier": "analysis.bot_filter.build_wallet_flags", "classifier_sha256": CLASSIFIER_SHA256,
            "candidate_gate": "mean span/(n-1) strictly below 120 seconds; no exact median otherwise",
            "criteria": "A: median<1 or 1<=median<10; B: trades/day>500 or >200; C: HHI<0.06 and n>500; E: CV<0.05 and n>50",
            "composite": "unchanged existing classifier", "timezone": "UTC",
            "repair_only_pair": "corrected_vs_legacy_recomputed",
            "historical_comparison_limit": "Historical producer/cohort/vintage differences remain unresolved, especially pipeline_data flags."}


class RebuildBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise RebuildBlocked(message)


def digest(value: str) -> str:
    require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), "full lower-case SHA256 required")
    return value


def encoded(value) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def write_json(path: Path, value):
    raw = encoded(value)
    require(len(raw) <= CAPS["maximum_metadata_bytes"], "complete metadata exceeds bound; no truncation")
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def paths_sql(files: list[dict]) -> str:
    return "[" + ",".join(literal(info["path"]) for info in files) + "]"


def directory_identity(path: Path) -> dict:
    require(path.is_dir() and not path.is_symlink() and path == path.resolve(), "non-symlink canonical input directory required")
    value = path.stat()
    return {"path": str(path), "device": value.st_dev, "inode": value.st_ino,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def footer(path: Path, expected_types: dict | None = None) -> dict:
    """Bind the complete footer; retain bounded row counts/bytes/time statistics."""
    require(path.is_absolute() and path == path.resolve(), "absolute canonical Parquet path required")
    identity = stat_identity(path)
    require(identity["bytes"] >= 12, "invalid short Parquet file")
    with path.open("rb") as stream:
        stream.seek(-8, 2)
        trailer = stream.read(8)
        length, magic = struct.unpack("<I4s", trailer)
        require(magic == b"PAR1" and 0 < length <= CAPS["maximum_footer_bytes"] and
                length + 8 <= identity["bytes"], "invalid or oversized Parquet footer")
        stream.seek(-length-8, 2)
        footer_hash = hashlib.sha256(stream.read(length) + trailer).hexdigest()
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    fields = {field.name: str(field.type) for field in schema}
    require(len(fields) == len(schema.names), "duplicate physical field names")
    if expected_types is not None:
        require(fields == expected_types, "Parquet schema differs from the exact classifier contract")
    require(parquet.metadata.num_row_groups <= CAPS["maximum_row_groups"], "row-group metadata exceeds bound")
    groups = []
    for index in range(parquet.metadata.num_row_groups):
        group = parquet.metadata.row_group(index)
        timestamp = None
        for col_index in range(group.num_columns):
            column = group.column(col_index)
            if column.path_in_schema == "timestamp":
                stat = column.statistics
                timestamp = None if stat is None else {"null_count": stat.null_count,
                    "min": stat.min if stat.has_min_max else None, "max": stat.max if stat.has_min_max else None}
        groups.append({"index": index, "rows": group.num_rows, "timestamp": timestamp,
                       "compressed_bytes": sum(group.column(i).total_compressed_size for i in range(group.num_columns)),
                       "uncompressed_bytes": group.total_byte_size})
    require(sum(group["rows"] for group in groups) == parquet.metadata.num_rows,
            "footer/row-group count law failed")
    result = {"path": str(path), "stat": identity, "rows": parquet.metadata.num_rows,
              "schema": str(schema), "fields": fields, "footer_sha256": footer_hash,
              "row_groups": groups}
    require(stat_identity(path) == identity, "input identity changed during footer read")
    encoded(result)  # Reject non-finite or unrepresentable metadata.
    return result


def repair_plan(manifest: dict, qa: dict, legacy_root: Path, corrected_root: Path, *, months=MONTHS) -> list[dict]:
    """Only fixture callers may supply fewer months; the production CLI cannot."""
    require(manifest.get("status") == "repair_complete" and qa.get("status") == "complete_saved_wallet_repair_manifest_qa",
            "completed repair and saved-evidence QA required")
    for value in (manifest, qa):
        require(value.get("data_certified") is False and value.get("downstream_adoption") == "pending",
                "repair certification/adoption limits must remain explicit")
    require(qa["inputs"]["manifest"]["sha256"] == REPAIR_SHA256 and
            qa["inputs"]["manifest"]["bytes"] == 1_582_547 and qa["exit_profile"]["exit_status"] == 0,
            "QA must bind the actual completed repair and successful producer exit")
    require(all(value is True for value in manifest["reconciliation"].values()) and
            all(value is True for value in qa["reconciliation"].values()) and
            qa["rows"] == manifest["rows"], "saved repair reconciliation failed")
    original = [info for info in manifest["inputs"] if info["relation"] == "clean"]
    outputs = [info for info in manifest["outputs"] if info["relation"] == "clean"]
    coverage = [info for info in qa["coverage"] if info["relation"] == "clean"]
    for values in (original, outputs, coverage):
        require(len(values) == len(months) and {info["month"] for info in values} == set(months),
                "complete unique frozen CLEAN month coverage required")
    require(legacy_root.is_absolute() and corrected_root.is_absolute() and legacy_root != corrected_root,
            "explicit distinct absolute CLEAN roots required")
    plan = []
    for month in months:
        old = next(info for info in original if info["month"] == month)
        out = next(info for info in outputs if info["month"] == month)
        covered = next(info for info in coverage if info["month"] == month)
        require(old["path"] == str(legacy_root / ("year_month=" + month) / "data.parquet") and
                out["path"] == "clean/year_month=" + month and out["output"]["path"] == "data.parquet" and
                covered["monthly_manifest_sha256"] == out["manifest_sha256"] and
                covered["rows"] == out["output"]["rows"] == old["rows"] and
                out["input"] == {**old, "sha256": out["input"]["sha256"]} and
                old["schema"] == out["output"]["schema"], "saved CLEAN file/count/schema binding differs")
        require(type(old["rows"]) is int and old["rows"] > 0, "positive frozen CLEAN footer count required")
        plan.append({"month": month, "legacy": {"path": old["path"], "expected": out["input"]},
                     "corrected": {"path": str(corrected_root / ("year_month=" + month) / "data.parquet"),
                                   "expected": out["output"]},
                     "monthly_manifest": {"path": str(corrected_root / ("year_month=" + month) / "manifest.json"),
                                          "sha256": digest(out["manifest_sha256"])}})
    require(sum(info["legacy"]["expected"]["rows"] for info in plan) == manifest["rows"]["clean"],
            "global CLEAN/footer count law failed")
    return plan


def load_repair(manifest_path: Path, qa_path: Path, legacy_root: Path, corrected_root: Path):
    manifest, manifest_id = read_json(manifest_path, REPAIR_SHA256, CAPS["maximum_metadata_bytes"])
    qa, qa_id = read_json(qa_path, REPAIR_QA_SHA256, CAPS["maximum_metadata_bytes"])
    require(corrected_root == manifest_path.parent / "clean", "corrected CLEAN must be inside the actual repair run")
    return repair_plan(manifest, qa, legacy_root, corrected_root), {"repair_manifest": manifest_id, "repair_qa": qa_id}


def dataset_snapshot(root: Path, plan: list[dict], vintage: str) -> dict:
    directories = [directory_identity(root)]
    expected_names = {"year_month=" + item["month"] for item in plan}
    require({path.name for path in root.iterdir()} == expected_names, "CLEAN root layout differs from frozen coverage")
    files, metadata = [], []
    for item in plan:
        partition = root / ("year_month=" + item["month"])
        directories.append(directory_identity(partition))
        require({path.name for path in partition.iterdir()} ==
                ({"data.parquet", "manifest.json"} if vintage == "corrected" else {"data.parquet"}),
                "CLEAN partition layout differs from frozen input")
        record = item[vintage]
        info = footer(Path(record["path"]), TRADE_TYPES)
        expected = record["expected"]
        require(info["stat"]["bytes"] == (expected["stat"]["bytes"] if vintage == "legacy" else expected["bytes"]) and
                info["rows"] == expected["rows"] and info["schema"] == expected["schema"],
                "CLEAN input differs from saved repair file metadata")
        if vintage == "legacy":
            require(info["stat"] == expected["stat"] and info["footer_sha256"] == expected["footer_sha256"],
                    "legacy CLEAN differs from original repair identity")
        for group in info["row_groups"]:
            stat = group["timestamp"]
            require(stat is not None and stat["null_count"] == 0 and type(stat["min"]) is int and
                    type(stat["max"]) is int and stat["min"] <= stat["max"], "timestamp footer has missing/null bounds")
        files.append({**info, "expected_content_sha256": digest(expected["sha256"])})
        if vintage == "corrected":
            path = Path(item["monthly_manifest"]["path"])
            identity = stat_identity(path)
            require(0 < identity["bytes"] <= CAPS["maximum_metadata_bytes"], "monthly metadata exceeds bound")
            hashed = sha256(path)
            require(hashed == item["monthly_manifest"]["sha256"], "monthly repair metadata hash differs")
            metadata.append({"path": str(path), "stat": identity, "sha256": hashed})
    snapshot = {"root": str(root), "directories": directories, "files": files, "metadata": metadata}
    snapshot["layout_sha256"] = hashlib.sha256(encoded(snapshot)).hexdigest()
    return snapshot


def flags_snapshot(path: Path, expected_sha256: str) -> dict:
    require(path.is_absolute(), "explicit absolute historical flag path required")
    result = footer(path, FLAG_TYPES)
    require(result["stat"]["bytes"] <= CAPS["maximum_output_bytes"], "historical flags exceed bounded wallet-level size")
    return {**result, "expected_content_sha256": digest(expected_sha256)}


def source_snapshot(expected_head: str, expected_hashes: dict) -> dict:
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head), "full expected committed HEAD required")
    require(set(expected_hashes) == set(SOURCE_PATHS) and expected_hashes["analysis/bot_filter.py"] == CLASSIFIER_SHA256,
            "complete expected source hashes and unchanged classifier required")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO, text=True)
    require(head == expected_head and not status.strip(), "canonical HEAD differs or checkout is not clean")
    actual = {}
    for name in SOURCE_PATHS:
        committed = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        require(committed.returncode == 0, "all rebuild sources/tests must be committed")
        actual[name] = sha256(REPO / name)
        require(actual[name] == digest(expected_hashes[name]), "source differs from required SHA256: " + name)
    return {"head": head, "sha256": actual}


def existing_parent(path: Path) -> Path:
    while not path.exists():
        path = path.parent
    return path


def disjoint_output(target: Path, roots: list[Path], files: list[Path]):
    require(target.is_absolute() and target == target.resolve() and not target.exists() and not target.is_symlink(),
            "fresh absolute canonical immutable destination required")
    for root in roots:
        require(target != root and root not in target.parents and target not in root.parents, "output overlaps a CLEAN input root")
    for path in files:
        require(target != path and target not in path.parents and path not in target.parents, "output overlaps an input file")


def preflight(plan: list[dict], legacy_root: Path, corrected_root: Path, historical: dict,
              target: Path, binding: dict, source: dict) -> dict:
    roots = [legacy_root, corrected_root]
    control_paths = [Path(value["path"]) for value in binding.values()]
    disjoint_output(target, roots, control_paths + [value[0] for value in historical.values()])
    datasets = {name: dataset_snapshot(root, plan, name) for name, root in zip(("legacy", "corrected"), roots)}
    flags = {name: flags_snapshot(path, hashed) for name, (path, hashed) in historical.items()}
    require(set(flags) == {"learnability", "pipeline_data"} and flags["learnability"]["path"] != flags["pipeline_data"]["path"],
            "both distinct historical flag artifacts required")
    files = [info for value in datasets.values() for info in value["files"]] + list(flags.values())
    require(len(files) <= CAPS["maximum_files"], "input file count exceeds bound")
    trade_bytes = sum(info["stat"]["bytes"] for value in datasets.values() for info in value["files"])
    historical_bytes = sum(info["stat"]["bytes"] for info in flags.values())
    # Each CLEAN: two content hashes, one integrity/count scan, one independent
    # source wallet-count scan, and the unchanged classifier's four corpus scans
    # (base, candidate ITI, hour HHI, size CV).
    # Wallet-level Parquet reads and output reopens/hashes share a conservative
    # 16x-total-output reserve. This is charged scan accounting, not measured I/O.
    planned = 8 * trade_bytes + 3 * historical_bytes + 16 * CAPS["maximum_output_bytes"]
    require(planned <= CAPS["maximum_read_bytes"], "planned charged reads exceed 1TiB cap")
    required_free = CAPS["spill_bytes"] + CAPS["maximum_output_bytes"] + CAPS["minimum_free_bytes"]
    free = shutil.disk_usage(existing_parent(target.parent)).free
    require(free >= required_free, "insufficient output/spill capacity and 20GiB remaining reserve")
    result = {"schema_version": "polymarket_wallet_flags_rebuild_v1", "status": "preflight_complete",
              "data_certified": False, "downstream_adoption": "pending", "target": str(target),
              "binding": binding, "source": source, "caps": dict(CAPS), "contract": dict(CONTRACT),
              "datasets": datasets, "historical_flags": flags, "trade_input_bytes": trade_bytes,
              "planned_read_bytes": planned, "required_free_bytes": required_free, "observed_free_bytes": free,
              "read_contract": "8 full-file charges per CLEAN plus historical/output reserves; each scan is charged before execution; 1TiB maximum.",
              "resource_contract": "Serial classifier connections; 160GB DuckDB managed memory, 8 threads, 20GiB spill per connection; no full-corpus pandas. RSS can exceed managed memory.",
              "output_contract": "At most 2GiB total persistent outputs, enforced COPY file ceilings and final size gate; immutable no-replace publication, inputs reopened before publication."}
    require(len(encoded(result)) <= CAPS["maximum_metadata_bytes"], "complete preflight metadata exceeds bound")
    return result


def reviewed_contract(reviewed: dict, fresh: dict):
    require(reviewed.get("status") == fresh.get("status") == "preflight_complete", "completed reviewed preflight required")
    for field in set(fresh) - {"observed_free_bytes"}:
        require(reviewed.get(field) == fresh[field], "fresh preflight differs from reviewed contract: " + field)
    require(set(reviewed) == set(fresh), "unexpected reviewed preflight fields")


def charge(budget: dict, amount: int, stage: str):
    require(type(amount) is int and amount >= 0 and budget["read_bytes"] + amount <= CAPS["maximum_read_bytes"],
            "charged read budget exhausted before query")
    budget["read_bytes"] += amount
    budget["charges"].append({"stage": stage, "bytes": amount})


def verify_content(files: list[dict], budget: dict, stage: str):
    for info in files:
        path = Path(info["path"])
        require(stat_identity(path) == info["stat"], "input stat changed before content verification")
        charge(budget, info["stat"]["bytes"], stage + ":" + path.name)
        require(sha256(path) == info["expected_content_sha256"] and stat_identity(path) == info["stat"],
                "input content hash/identity differs from frozen evidence")


def connection(staging: Path, stage: str):
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET memory_limit=" + literal(CAPS["memory_limit"]))
        con.execute("SET threads=" + str(CAPS["threads"]))
        con.execute("SET temp_directory=" + literal(str(staging / ("spill_" + stage))))
        con.execute("SET max_temp_directory_size=" + literal(str(CAPS["spill_bytes"]) + "B"))
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET TimeZone='UTC'")
        con.execute("SET enable_object_cache=false")
        # DuckDB 1.4.4 has no common_subplan optimizer. Where present, the
        # known wallet-projection safeguard disables it before any trade query.
        if con.execute("SELECT count(*) FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone()[0]:
            con.execute("SET disabled_optimizers='common_subplan'")
        return con
    except BaseException:
        con.close()
        raise


def scalar(con, sql: str) -> dict:
    cursor = con.execute(sql)
    return dict(zip((field[0] for field in cursor.description), cursor.fetchone()))


def validate_flags(con, relation: str, *, expected_trades: int | None = None,
                   expected_wallet_counts: str | None = None, allow_unknown=False) -> dict:
    description = con.execute("DESCRIBE SELECT * FROM " + relation).fetchall()
    wanted = {"proxyWallet": "VARCHAR", "n_trades": "BIGINT", "trades_per_active_day": "DOUBLE",
              "active_days": "BIGINT", "median_iti": "DOUBLE", **{flag: "BOOLEAN" for flag in FLAGS}}
    require({row[0]: row[1] for row in description} == wanted, "flag schema/types differ from unchanged classifier")
    invalid = "false" if allow_unknown else " OR ".join(flag + " IS NULL" for flag in FLAGS)
    null_counts = ",".join(f"count(*) FILTER(WHERE {flag} IS NULL)::BIGINT null_{flag}" for flag in FLAGS)
    result = scalar(con, f"""SELECT count(*) wallets, coalesce(sum(n_trades),0)::BIGINT trades,
        count(DISTINCT lower(proxyWallet)) normalized_wallets,
        count(*) FILTER(WHERE proxyWallet IS NULL OR trim(proxyWallet)='' OR proxyWallet<>trim(proxyWallet)) invalid_keys,
        count(*) FILTER(WHERE n_trades IS NULL OR n_trades<=0 OR active_days IS NULL OR active_days<=0
            OR trades_per_active_day IS NULL OR NOT isfinite(trades_per_active_day)
            OR trades_per_active_day<=0 OR (median_iti IS NOT NULL AND (NOT isfinite(median_iti) OR median_iti<0))
            OR {invalid}) invalid_payload,
        coalesce(sum(is_nonhuman::BIGINT),0)::BIGINT nonhuman_wallets,
        coalesce(sum(CASE WHEN is_nonhuman THEN n_trades ELSE 0 END),0)::BIGINT nonhuman_trades,{null_counts}
        FROM {relation}""")
    require(result["invalid_keys"] == result["invalid_payload"] == 0 and result["wallets"] == result["normalized_wallets"],
            "null/blank/padded/duplicate normalized wallet keys or invalid flag payload")
    if expected_trades is not None:
        require(result["trades"] == expected_trades, "wallet n_trades does not conserve timestamp-admitted rows")
    if expected_wallet_counts is not None:
        for left, right in ((relation, expected_wallet_counts), (expected_wallet_counts, relation)):
            require(con.execute(f"SELECT count(*) FROM (SELECT proxyWallet,n_trades FROM {left} "
                                f"EXCEPT ALL SELECT proxyWallet,n_trades FROM {right})").fetchone()[0] == 0,
                    "flags differ from exact source raw-wallet coverage/per-wallet trade counts")
    result["flag_null_counts"] = {flag: result.pop("null_" + flag) for flag in FLAGS}
    return result


def output_bytes(staging: Path) -> int:
    return sum(path.stat().st_size for path in staging.rglob("*") if path.is_file() and
               not any(part.startswith("spill_") for part in path.relative_to(staging).parts))


def copy_output(con, query: str, name: str, staging: Path, budget: dict) -> dict:
    target, partial = staging / name, staging / (name + ".partial")
    require(not target.exists() and not partial.exists(), "immutable output already exists")
    remaining = CAPS["maximum_output_bytes"] - output_bytes(staging) - CAPS["maximum_metadata_bytes"]
    require(remaining > 0 and shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "output/reserve gate failed before COPY")
    with copy_size_limit(remaining):
        con.execute(f"COPY ({query}) TO {literal(str(partial))} (FORMAT PARQUET)")
    info = footer(partial)
    charge(budget, info["stat"]["bytes"], "output_content_hash:" + name)
    info["sha256"] = sha256(partial)
    atomic_publish(partial, target)
    return {"path": name, "bytes": info["stat"]["bytes"], "rows": info["rows"], "schema": info["schema"],
            "footer_sha256": info["footer_sha256"], "sha256": info["sha256"]}


def exact_reopen(con, relation: str, path: Path, budget: dict):
    require(path.is_file(), "output reopen requires saved Parquet")
    charge(budget, 2 * path.stat().st_size, "output_exact_reopen:" + path.name)
    saved = f"read_parquet({literal(str(path))}, hive_partitioning=false)"
    for left, right in ((relation, saved), (saved, relation)):
        require(con.execute(f"SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})").fetchone()[0] == 0,
                "saved output differs from in-memory result")


def rebuild_one(snapshot: dict, vintage: str, staging: Path, budget: dict) -> tuple[dict, dict]:
    files = snapshot["files"]
    con = connection(staging, vintage)
    try:
        source = f"read_parquet({paths_sql(files)},union_by_name=true,hive_partitioning=false)"
        con.execute("CREATE TEMP VIEW trades_raw AS SELECT * FROM " + source)
        con.execute(f"CREATE TEMP VIEW trades AS SELECT * FROM trades_raw WHERE timestamp>={START_TIMESTAMP}")
        charge(budget, sum(info["stat"]["bytes"] for info in files), "trade_integrity:" + vintage)
        counts = scalar(con, f"""SELECT count(*)::BIGINT total_rows,
            count(*) FILTER(WHERE timestamp>={START_TIMESTAMP})::BIGINT admitted_rows,
            count(*) FILTER(WHERE timestamp<{START_TIMESTAMP})::BIGINT excluded_before_start_rows,
            count(*) FILTER(WHERE timestamp IS NULL)::BIGINT null_timestamp_rows,
            count(*) FILTER(WHERE timestamp>={START_TIMESTAMP} AND (proxyWallet IS NULL OR trim(proxyWallet)=''
                OR proxyWallet<>trim(proxyWallet)
                OR usdcSize IS NULL OR NOT isfinite(usdcSize)))::BIGINT invalid_admitted_rows
            FROM trades_raw""")
        require(counts["total_rows"] == sum(info["rows"] for info in files) and counts["null_timestamp_rows"] ==
                counts["invalid_admitted_rows"] == 0 and counts["admitted_rows"] + counts["excluded_before_start_rows"] ==
                counts["total_rows"], "trade integrity/footer/cutoff count law failed")
        charge(budget, sum(info["stat"]["bytes"] for info in files), "source_wallet_counts:" + vintage)
        con.execute("CREATE TEMP TABLE source_wallet_counts AS SELECT proxyWallet,count(*)::BIGINT n_trades "
                    "FROM trades GROUP BY proxyWallet")
        charge(budget, 4 * sum(info["stat"]["bytes"] for info in files), "classifier_four_corpus_scans:" + vintage)
        stats = build_wallet_flags(con, verbose=False)
        validated = validate_flags(con, "wallet_flags", expected_trades=counts["admitted_rows"],
                                   expected_wallet_counts="source_wallet_counts")
        require(stats["total_wallets"] == validated["wallets"] and (stats["total_trades"] or 0) == validated["trades"] and
                (stats["nonhuman_wallets"] or 0) == validated["nonhuman_wallets"] and
                (stats["nonhuman_trades"] or 0) == validated["nonhuman_trades"], "classifier summary differs from reopened count checks")
        name = "legacy_recomputed_flags.parquet" if vintage == "legacy" else "wallet_flags.parquet"
        output = copy_output(con, "SELECT * FROM wallet_flags ORDER BY proxyWallet", name, staging, budget)
        charge(budget, 3 * output["bytes"], "output_flag_validation_and_source_counts:" + vintage)
        saved = "read_parquet(" + literal(str(staging / name)) + ",hive_partitioning=false)"
        require(validate_flags(con, saved, expected_trades=counts["admitted_rows"],
                               expected_wallet_counts="source_wallet_counts") == validated,
                "saved flag count/schema/coverage validation differs")
        exact_reopen(con, "wallet_flags", staging / name, budget)
        settings = scalar(con, """SELECT current_setting('TimeZone') timezone,
            current_setting('disabled_optimizers') disabled_optimizers,
            current_setting('memory_limit') memory_limit, current_setting('threads') threads,
            current_setting('max_temp_directory_size') max_temp_directory_size""")
        settings["common_subplan_available"] = bool(con.execute(
            "SELECT count(*) FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone()[0])
        require(settings["timezone"] == "UTC" and (not settings["common_subplan_available"] or
                "common_subplan" in settings["disabled_optimizers"].split(",")), "actual optimizer/timezone settings drifted")
        return output, {"rows": counts, "flags": validated, "execution_settings": settings}
    finally:
        con.close()


def compare_flags(con, left: str, right: str, name: str, *, repair_only=False) -> dict:
    """Absent-wallet flags stay NULL; coverage and common-wallet flips are distinct."""
    select = ["coalesce(l.wallet_key,r.wallet_key) wallet_key", "l.proxyWallet left_raw_wallet", "r.proxyWallet right_raw_wallet",
              "l.wallet_key IS NOT NULL left_present", "r.wallet_key IS NOT NULL right_present"]
    for column in FLAG_TYPES:
        if column != "proxyWallet":
            select.extend((f"l.{column} left_{column}", f"r.{column} right_{column}"))
    con.execute("CREATE OR REPLACE TEMP TABLE flag_pair AS SELECT " + ",".join(select) +
                f" FROM {left} l FULL OUTER JOIN {right} r USING(wallet_key)")
    summary = scalar(con, """SELECT count(*)::BIGINT union_wallets,
        count(*) FILTER(WHERE left_present AND right_present)::BIGINT common_wallets,
        count(*) FILTER(WHERE left_present AND NOT right_present)::BIGINT left_only_wallets,
        count(*) FILTER(WHERE right_present AND NOT left_present)::BIGINT right_only_wallets,
        coalesce(sum(left_n_trades),0)::BIGINT left_trades, coalesce(sum(right_n_trades),0)::BIGINT right_trades,
        coalesce(sum(left_n_trades) FILTER(WHERE NOT right_present),0)::BIGINT left_only_trades,
        coalesce(sum(right_n_trades) FILTER(WHERE NOT left_present),0)::BIGINT right_only_trades
        FROM flag_pair""")
    require(summary["union_wallets"] == summary["common_wallets"] + summary["left_only_wallets"] + summary["right_only_wallets"],
            "comparison coverage count law failed")
    criteria = []
    for flag in FLAGS:
        values = scalar(con, f"""SELECT
            count(*) FILTER(WHERE left_present AND right_present AND NOT left_{flag} AND right_{flag})::BIGINT common_enter,
            count(*) FILTER(WHERE left_present AND right_present AND left_{flag} AND NOT right_{flag})::BIGINT common_exit,
            count(*) FILTER(WHERE left_present AND right_present AND left_{flag}=right_{flag})::BIGINT common_unchanged,
            count(*) FILTER(WHERE left_present AND right_present AND left_{flag} IS NULL AND right_{flag} IS FALSE)::BIGINT common_left_unknown_right_false,
            count(*) FILTER(WHERE left_present AND right_present AND left_{flag} IS NULL AND right_{flag} IS TRUE)::BIGINT common_left_unknown_right_true,
            count(*) FILTER(WHERE left_present AND right_present AND right_{flag} IS NULL AND left_{flag} IS FALSE)::BIGINT common_right_unknown_left_false,
            count(*) FILTER(WHERE left_present AND right_present AND right_{flag} IS NULL AND left_{flag} IS TRUE)::BIGINT common_right_unknown_left_true,
            count(*) FILTER(WHERE left_present AND right_present AND left_{flag} IS NULL AND right_{flag} IS NULL)::BIGINT common_both_unknown,
            count(*) FILTER(WHERE left_present AND left_{flag} IS NULL)::BIGINT left_unknown,
            count(*) FILTER(WHERE right_present AND right_{flag} IS NULL)::BIGINT right_unknown,
            count(*) FILTER(WHERE left_present AND right_present AND NOT coalesce(left_{flag},false) AND coalesce(right_{flag},false))::BIGINT selection_common_enter,
            count(*) FILTER(WHERE left_present AND right_present AND coalesce(left_{flag},false) AND NOT coalesce(right_{flag},false))::BIGINT selection_common_exit,
            count(*) FILTER(WHERE NOT left_present AND right_{flag})::BIGINT added_wallet_flagged,
            count(*) FILTER(WHERE NOT right_present AND left_{flag})::BIGINT removed_wallet_flagged,
            count(*) FILTER(WHERE NOT left_present AND right_present AND right_{flag} IS NULL)::BIGINT added_wallet_unknown,
            count(*) FILTER(WHERE NOT right_present AND left_present AND left_{flag} IS NULL)::BIGINT removed_wallet_unknown,
            coalesce(sum(left_{flag}::BIGINT),0)::BIGINT left_flagged,
            coalesce(sum(right_{flag}::BIGINT),0)::BIGINT right_flagged,
            coalesce(sum(left_n_trades) FILTER(WHERE left_{flag}),0)::BIGINT left_flagged_trades,
            coalesce(sum(right_n_trades) FILTER(WHERE right_{flag}),0)::BIGINT right_flagged_trades
            FROM flag_pair""")
        common_states = ("common_enter", "common_exit", "common_unchanged", "common_left_unknown_right_false",
                         "common_left_unknown_right_true", "common_right_unknown_left_false", "common_right_unknown_left_true", "common_both_unknown")
        require(sum(values[key] for key in common_states) == summary["common_wallets"] and
                values["right_flagged"] - values["left_flagged"] == values["selection_common_enter"] - values["selection_common_exit"] +
                values["added_wallet_flagged"] - values["removed_wallet_flagged"], "criterion transition count law failed")
        criteria.append({"comparison": name, "criterion": flag, **values})
    metrics = {}
    for column in FLAG_TYPES:
        if column != "proxyWallet":
            metrics[column] = con.execute(f"SELECT count(*) FROM flag_pair WHERE left_present AND right_present AND "
                                          f"left_{column} IS DISTINCT FROM right_{column}").fetchone()[0]
    return {"comparison": name, "repair_only": repair_only, **summary,
            "changed_common_payload": metrics, "criteria": criteria}


def comparison_outputs(staging: Path, historical: dict, budget: dict) -> tuple[list[dict], list[dict]]:
    con = connection(staging, "comparisons")
    outputs, comparisons = [], []
    try:
        relations = {"legacy_recomputed": staging / "legacy_recomputed_flags.parquet", "corrected": staging / "wallet_flags.parquet",
                     **{name: Path(info["path"]) for name, info in historical.items()}}
        statistics = {}
        for name, path in relations.items():
            charge(budget, path.stat().st_size, "wallet_level_load:" + name)
            con.execute(f"CREATE TEMP TABLE {name} AS SELECT lower(proxyWallet) wallet_key,* FROM "
                        f"read_parquet({literal(str(path))},hive_partitioning=false)")
            con.execute(f"CREATE TEMP VIEW validate_{name} AS SELECT * EXCLUDE(wallet_key) FROM {name}")
            statistics[name] = validate_flags(con, "validate_" + name, allow_unknown=name in historical)
        pairs = (("legacy_recomputed", "corrected"), ("learnability", "legacy_recomputed"),
                 ("pipeline_data", "legacy_recomputed"), ("learnability", "corrected"), ("pipeline_data", "corrected"))
        for left, right in pairs:
            name = right + "_vs_" + left
            summary = compare_flags(con, left, right, name, repair_only=(left, right) == pairs[0])
            require(summary["common_wallets"] + summary["left_only_wallets"] == statistics[left]["wallets"] and
                    summary["common_wallets"] + summary["right_only_wallets"] == statistics[right]["wallets"] and
                    summary["left_trades"] == statistics[left]["trades"] and summary["right_trades"] == statistics[right]["trades"],
                    "comparison input counts/coverage differ")
            comparisons.append(summary)
            if (left, right) == pairs[0]:
                name = "repair_only_flag_pairs.parquet"
                outputs.append(copy_output(con, "SELECT * FROM flag_pair ORDER BY wallet_key", name, staging, budget))
                exact_reopen(con, "flag_pair", staging / name, budget)
        return outputs, comparisons
    finally:
        con.close()


def reopen_inputs(plan: list[dict], fresh: dict, budget: dict):
    for vintage in ("legacy", "corrected"):
        require(dataset_snapshot(Path(fresh["datasets"][vintage]["root"]), plan, vintage) == fresh["datasets"][vintage],
                "complete CLEAN input layout/metadata identity changed before publication")
    files = [info for snapshot in fresh["datasets"].values() for info in snapshot["files"]] + list(fresh["historical_flags"].values())
    for info in fresh["historical_flags"].values():
        require(flags_snapshot(Path(info["path"]), info["expected_content_sha256"]) == info,
                "historical flag footer/identity changed before publication")
    verify_content(files, budget, "final_input_content_hash")
    for info in fresh["binding"].values():
        _, identity = read_json(Path(info["path"]), info["sha256"], CAPS["maximum_metadata_bytes"])
        require(identity == info, "repair metadata identity changed before publication")


def build_run(plan: list[dict], target: Path, reviewed: dict, fresh: dict, *, command=None, reviewed_identity=None) -> dict:
    reviewed_contract(reviewed, fresh)
    require(str(target) == fresh["target"] and not target.exists(), "fresh target differs or exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    budget = {"read_bytes": 0, "charges": []}
    started, outputs, builds = time.monotonic(), [], {}
    try:
        files = [info for value in fresh["datasets"].values() for info in value["files"]] + list(fresh["historical_flags"].values())
        verify_content(files, budget, "initial_input_content_hash")
        for vintage in ("legacy", "corrected"):
            output, stats = rebuild_one(fresh["datasets"][vintage], vintage, staging, budget)
            outputs.append(output)
            builds[vintage] = stats
            write_json(staging / (vintage + ".receipt.json"), {"status": "flags_recomputed", "output": output, **stats})
            print(json.dumps({"stage": vintage + "_complete", "rows": stats["rows"], "flags": stats["flags"]}), file=sys.stderr, flush=True)
        require(builds["legacy"]["rows"] == builds["corrected"]["rows"], "repair vintage timestamp/cohort counts differ")
        compared_outputs, comparisons = comparison_outputs(staging, fresh["historical_flags"], budget)
        outputs.extend(compared_outputs)
        write_json(staging / "comparisons.json", {"status": "comparisons_complete", "comparisons": comparisons,
                   "interpretation": CONTRACT["historical_comparison_limit"]})
        reopen_inputs(plan, fresh, budget)
        require(source_snapshot(fresh["source"]["head"], fresh["source"]["sha256"]) == fresh["source"],
                "source identity changed before publication")
        for output in outputs:
            path = staging / output["path"]
            info = footer(path)
            charge(budget, output["bytes"], "final_output_hash:" + output["path"])
            require(info["rows"] == output["rows"] and info["schema"] == output["schema"] and
                    info["footer_sha256"] == output["footer_sha256"] and sha256(path) == output["sha256"],
                    "saved output changed after exact reopen")
        summary = {"schema_version": fresh["schema_version"], "status": "wallet_flags_rebuild_complete",
                   "data_certified": False, "downstream_adoption": "pending", "binding": fresh["binding"],
                   "reviewed_preflight": reviewed_identity, "source": fresh["source"], "caps": fresh["caps"],
                   "contract": fresh["contract"], "command": command or [], "inputs": {"datasets": fresh["datasets"],
                       "historical_flags": fresh["historical_flags"]}, "outputs": outputs, "builds": builds,
                   "comparisons": comparisons, "reconciliation": {"admitted_trade_counts_conserved": True,
                       "exact_source_wallet_keys_and_counts": True,
                       "unique_normalized_nonblank_wallets": True, "non_null_boolean_flags": True,
                       "flag_outputs_exactly_reopened": True, "all_inputs_layout_metadata_and_content_reopened": True},
                   "environment": {"python": sys.version, "duckdb": duckdb.__version__, "platform": sys.platform},
                   "resource_profile": {"charged_read_bytes": budget["read_bytes"], "charges": budget["charges"],
                                        "elapsed_seconds": time.monotonic() - started},
                   "downstream_gate": "New flags require coherent consumer adoption. Current sports consumers use frozen native exact artifacts; older encoded analysis bases and wallet code maps remain legacy and unadopted."}
        write_json(staging / "manifest.json", summary)
        require(not any(path.is_file() and any(part.startswith("spill_") for part in path.relative_to(staging).parts)
                        for path in staging.rglob("*")), "closed connections retained spill files; publication blocked")
        require(output_bytes(staging) <= CAPS["maximum_output_bytes"] and
                shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "final total output/free-space reserve gate failed")
        atomic_publish(staging, target)
        return summary
    except BaseException as error:
        if staging.exists() and not (staging / "failure.json").exists():
            write_json(staging / "failure.json", {"status": "wallet_flags_rebuild_incomplete", "data_certified": False,
                       "downstream_adoption": "pending", "error_type": type(error).__name__, "reason": str(error),
                       "completed_vintages": list(builds), "charged_read_bytes": budget["read_bytes"]})
        raise


def parse_hashes(values: list[str]) -> dict:
    result = {}
    for value in values:
        require("=" in value, "expected source SHA256 must be path=hash")
        name, hashed = value.split("=", 1)
        require(name not in result, "duplicate expected source path")
        result[name] = digest(hashed)
    return result


def publish_preflight(target: Path, fresh: dict) -> dict:
    """Publish complete metadata atomically and never replace a concurrent run."""
    require(not target.exists() and not target.is_symlink(), "fresh preflight destination required")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    write_json(staging / "manifest.json", fresh)
    hashed = sha256(staging / "manifest.json")
    atomic_publish(staging, target)
    return {"path": str(target / "manifest.json"), "sha256": hashed}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repair-manifest", "repair-qa", "legacy-clean", "corrected-clean", "learnability-flags", "pipeline-data-flags", "run-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--learnability-flags-sha256", required=True)
    parser.add_argument("--pipeline-data-flags-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--expected-source-sha256", required=True, action="append", help="repeat path=SHA256 for every frozen source")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--preflight", type=Path, help="new metadata-only preflight directory")
    modes.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--approved-preflight-sha256")
    args = parser.parse_args(argv)
    require(bool(args.reviewed_preflight) == bool(args.approved_preflight_sha256), "body requires separately reviewed preflight and its SHA256")
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production requires /home/ubuntu/venv/bin/python")
    source = source_snapshot(args.expected_head, parse_hashes(args.expected_source_sha256))
    plan, binding = load_repair(args.repair_manifest, args.repair_qa, args.legacy_clean, args.corrected_clean)
    fresh = preflight(plan, args.legacy_clean, args.corrected_clean, {
        "learnability": (args.learnability_flags, args.learnability_flags_sha256),
        "pipeline_data": (args.pipeline_data_flags, args.pipeline_data_flags_sha256)}, args.run_dir, binding, source)
    if args.preflight:
        disjoint_output(args.preflight, [args.legacy_clean, args.corrected_clean],
                        [Path(info["path"]) for info in binding.values()] + [args.learnability_flags, args.pipeline_data_flags])
        require(args.preflight != args.run_dir and args.run_dir not in args.preflight.parents and args.preflight not in args.run_dir.parents,
                "preflight destination must be separate from body destination")
        require(source_snapshot(args.expected_head, source["sha256"]) == source, "source changed during metadata preflight")
        receipt = publish_preflight(args.preflight, fresh)
        print(json.dumps({"status": fresh["status"], "manifest": receipt["path"],
                          "sha256": receipt["sha256"], "planned_read_bytes": fresh["planned_read_bytes"]}))
        return 0
    reviewed, identity = read_json(args.reviewed_preflight, args.approved_preflight_sha256, CAPS["maximum_metadata_bytes"])
    summary = build_run(plan, args.run_dir, reviewed, fresh, command=sys.argv if argv is None else argv, reviewed_identity=identity)
    print(json.dumps({"status": summary["status"], "run_dir": str(args.run_dir), "data_certified": False, "downstream_adoption": "pending"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
