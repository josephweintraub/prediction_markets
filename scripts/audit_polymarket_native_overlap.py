"""Reconcile one fixed 100-block native overlap, not whole-history admission.

Read only footer-selected old raw row groups, preserving physical multiplicity.
Decode the four legacy/V2 emitters into the reviewed old 12-column extraction
representation. V3 remains uninterpreted raw source, never a CTF token mapping.
No RPC, pandas, full-history scan, economic deduplication or sample is produced.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts import inventory_polymarket_refresh as inventory
from scripts import plan_polymarket_refresh as planner
from scripts import probe_polymarket_increment_capacity as pilot

RAW_TAIL = 88_978_537
FIRST_BLOCK = RAW_TAIL - 99
OLD_ADDRESSES = pilot.ADDRESSES[:4]
V3_ADDRESS = pilot.ADDRESSES[-1]
COLLECTOR_SHA256 = "0f1f0c1dd0f8f6a7f7dd3daee832ebf93f03210df7fe8f7b8ad7d736bba2d14f"
FIELDS = ("order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
          "maker_amount_filled", "taker_amount_filled", "fee", "block_number",
          "transaction_hash", "log_index", "exchange_address")
STRINGS = {"order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
           "transaction_hash", "exchange_address"}
MAX_PROJECTED_BYTES = 256 * 1024**2
MAX_SELECTED_ROWS = 2_000_000
MAX_NEW_ROWS = 200_000
MAX_NEW_BYTES = 128_000_000
MAX_CONTROL_BYTES = 8 * 1024**2
MAX_FOOTER_BYTES = 128 * 1024**2
MAX_OUTPUT_BYTES = 1024**2
BATCH_ROWS = 8192
RAM_BYTES = 8 * 1024**3
RUNTIME_SECONDS = 1200
ADDRESS_RE = re.compile(r"0x[0-9a-fA-F]{40}\Z")
DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]{0,77})\Z")


class OverlapBlocked(ValueError):
    """Fixed non-sensitive failure codes only."""


def require(condition, code):
    if not condition:
        raise OverlapBlocked(code)


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class Audit:
    def __init__(self, clock=time.monotonic):
        self.clock, self.started = clock, clock()
        self.files = {}
        self.new_bytes = 0

    def check(self):
        require(self.clock() - self.started < RUNTIME_SECONDS, "audit_runtime_limit")

    def read(self, path, maximum, expected=None, *, charge_new=False):
        self.check()
        before = inventory.path_identity(path)
        require(before["status"] == "present" and not before["is_symlink"]
                and path.is_file() and 0 < before["target"]["bytes"] <= maximum,
                "bound_file_not_regular_or_bounded")
        size = before["target"]["bytes"]
        if charge_new:
            self.new_bytes += size
            require(self.new_bytes <= MAX_NEW_BYTES, "new_collection_read_byte_limit")
        with path.open("rb") as stream:
            raw = stream.read(maximum + 1)
        require(len(raw) == size and inventory.path_identity(path) == before,
                "bound_file_changed_during_read")
        digest = sha(raw)
        require(expected is None or digest == expected, "bound_file_hash_differs")
        self.files[str(path)] = {"identity": before, "sha256": digest, "bytes": size}
        return raw

    def json(self, path, expected=None):
        raw = self.read(path, MAX_CONTROL_BYTES, expected, charge_new=True)
        value = json.loads(raw, object_pairs_hook=inventory.unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(OverlapBlocked("nonfinite_json")))
        require(type(value) is dict, "control_not_object")
        return value

    def reopen(self):
        for name, binding in list(self.files.items()):
            self.check()
            path = Path(name)
            require(inventory.path_identity(path) == binding["identity"], "bound_file_changed_during_audit")
            # Only the admitted small collector controls/payloads are hashed.
            with path.open("rb") as stream:
                raw = stream.read(binding["bytes"] + 1)
            require(len(raw) == binding["bytes"] and sha(raw) == binding["sha256"]
                    and inventory.path_identity(path) == binding["identity"], "bound_file_changed_during_audit")


def child(root, relative):
    require(type(relative) is str and relative and not Path(relative).is_absolute()
            and all(part not in {"", ".", ".."} for part in relative.split("/")), "unsafe_collection_reference")
    path = root
    for part in relative.split("/"):
        path = path / part
        require(not path.is_symlink(), "collection_reference_symlink")
    require(root.resolve() in path.resolve().parents, "collection_reference_escapes_run")
    return path


def load_collection(audit, directory, manifest_sha, complete_sha, collector_head, plan_binding):
    require(directory.is_dir() and not directory.is_symlink(), "collection_directory_not_regular")
    complete = audit.json(child(directory, "COMPLETE.json"), complete_sha)
    manifest = audit.json(child(directory, "manifest.json"), manifest_sha)
    require(complete.get("status") == "raw_collection_complete_not_analysis_admission"
            and complete.get("manifest_sha256") == manifest_sha, "collection_complete_binding_differs")
    require(manifest.get("schema_version") == "polymarket_native_collection_manifest_v1"
            and manifest.get("status") == "raw_collection_complete" and manifest.get("collection_complete") is True
            and manifest.get("analysis_admitted") is False and manifest.get("raw_payload_only") is True
            and type(manifest.get("rows")) is int and type(manifest.get("distinct_native_keys")) is int
            and type(manifest.get("run_id")) is str and bool(re.fullmatch(r"[0-9a-f-]{36}", manifest["run_id"])),
            "collection_not_raw_complete")
    contract = manifest.get("contract")
    require(type(contract) is dict and contract.get("schema_version") == "polymarket_native_collection_contract_v1"
            and sha(encoded(contract)) == manifest.get("contract_sha256") == complete.get("contract_sha256")
            and manifest.get("run_id") == complete.get("run_id"), "collection_contract_binding_differs")
    require(contract.get("source_plan") == plan_binding, "collection_source_plan_differs")
    require(bool(re.fullmatch(r"[0-9a-f]{40}", collector_head or ""))
            and contract.get("source", {}).get("head") == collector_head
            and contract["source"].get("status") == "inspected"
            and contract["source"].get("tracked_worktree_clean") is True, "collector_source_head_differs")
    require(contract.get("mode") == "endpoint-overlap" and contract.get("first_block") == FIRST_BLOCK
            and contract.get("last_block") == RAW_TAIL and contract.get("window_blocks") == 100
            and contract.get("addresses") == list(pilot.ADDRESSES)
            and contract.get("topics") == sorted({pilot.LEGACY_TOPIC, pilot.MODERN_TOPIC})
            and contract.get("end_exclusive_utc") == planner.END_EXCLUSIVE
            and contract.get("raw_payload_only") is True and contract.get("analysis_admitted") is False
            and contract.get("interior_exact_timestamps_collected") is False
            and contract.get("provider_block_timestamp_used_as_clock") is False, "collection_predicates_or_scope_differ")
    caps = contract.get("limits", {})
    for field, maximum in (("rows", MAX_NEW_ROWS), ("output_bytes", MAX_NEW_BYTES), ("rpc_calls", 100),
                           ("runtime_seconds", 1200), ("response_bytes", 16 * 1024**2),
                           ("total_response_bytes", 256 * 1024**2)):
        require(type(caps.get(field)) is int and 0 < caps[field] <= maximum, "collection_cap_not_admitted")
    require(type(caps.get("spill_bytes")) is int and caps["spill_bytes"] == 0
            and type(caps.get("workers")) is int and caps["workers"] == 1, "collection_resources_differ")
    records = manifest.get("windows")
    require(type(records) is list and len(records) == 1, "fixed_overlap_requires_one_shard")
    record = records[0]
    window = {"window": f"b{FIRST_BLOCK:012d}-{RAW_TAIL:012d}", "first_block": FIRST_BLOCK,
              "last_block": RAW_TAIL, "blocks": 100}
    require(record.get("window") == window and record.get("status") == "raw_shard_validated"
            and record.get("contract_sha256") == manifest["contract_sha256"]
            and type(record.get("attempt")) is int and 1 <= record["attempt"] <= 3,
            "overlap_receipt_window_or_contract_differs")
    prefix = f"attempts/{window['window']}.a{record['attempt']}"
    success = audit.json(child(directory, prefix + "/SUCCESS.json"))
    require(success == record, "saved_success_receipt_differs")
    start = audit.json(child(directory, prefix + "/START.json"))
    require(start == {"contract_sha256": manifest["contract_sha256"], "window": window,
                      "attempt": record["attempt"]}, "saved_attempt_binding_differs")
    for role, filename in (("payload", "payload.parquet"), ("request_ledger", "REQUESTS.json")):
        reference = record.get(role, {})
        require(reference.get("path") == prefix + "/" + filename
                and type(reference.get("bytes")) is int and 0 < reference["bytes"] <= MAX_NEW_BYTES
                and bool(re.fullmatch(r"[0-9a-f]{64}", reference.get("sha256", ""))), "shard_reference_differs")
    ledger = audit.json(child(directory, record["request_ledger"]["path"]), record["request_ledger"]["sha256"])
    require(audit.files[str(child(directory, record["request_ledger"]["path"]))]["bytes"]
            == record["request_ledger"]["bytes"], "request_ledger_size_differs")
    calls = ledger.get("calls")
    predicates = [[hex(FIRST_BLOCK), False], [hex(RAW_TAIL), False],
                  [{"fromBlock": hex(FIRST_BLOCK), "toBlock": hex(RAW_TAIL),
                    "address": list(pilot.ADDRESSES), "topics": [contract["topics"]]}], [hex(RAW_TAIL), False]]
    require(type(calls) is list and len(calls) == 4
            and [item.get("method") for item in calls] ==
            ["eth_getBlockByNumber", "eth_getBlockByNumber", "eth_getLogs", "eth_getBlockByNumber"]
            and [item.get("params") for item in calls] == predicates
            and all(item.get("status") == "validated" for item in calls)
            and calls[2].get("id") == record.get("log_request_id"), "terminal_request_predicate_differs")
    require(len({item["id"] for item in calls}) == 4 and all(type(item["id"]) is int and item["id"] > 0
            for item in calls), "terminal_request_ids_invalid")
    first, last = record.get("first_header"), record.get("last_header")
    require(type(first) is dict and type(last) is dict and first.get("number") == FIRST_BLOCK
            and last.get("number") == RAW_TAIL, "overlap_header_endpoints_differ")
    for header in (first, last):
        require(type(header.get("timestamp")) is int and 0 < header["timestamp"] <= 253402300799
                and all(type(header.get(field)) is str and bool(inventory.HEX_HASH.fullmatch(header[field]))
                        for field in ("hash", "parentHash")), "overlap_header_invalid")
    require(first["timestamp"] <= last["timestamp"] and calls[0].get("header") == first
            and calls[1].get("header") == last == calls[3].get("header")
            and calls[2].get("payload_sha256") == record["payload"]["sha256"]
            and calls[2].get("rows") == record.get("evidence", {}).get("rows"), "request_receipt_header_or_payload_differs")
    raw = audit.read(child(directory, record["payload"]["path"]), MAX_NEW_BYTES,
                     record["payload"]["sha256"], charge_new=True)
    require(len(raw) == record["payload"]["bytes"], "payload_size_differs")
    return manifest, record, raw


def old_footer(path, source_inventory):
    """Return footer-only metadata; never open a body before selecting groups."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    entry = source_inventory["inputs"]["raw_events"]
    require(entry["status"] == "inspected" and entry["file_count"] == 1
            and Path(entry["path"]).resolve() == path.resolve() and len(entry["files"]) == 1,
            "old_raw_not_single_bound_file")
    bound = entry["files"][0]
    before = inventory.path_identity(path)
    require(before == bound["identity"] and not before["is_symlink"] and path.is_file(), "old_raw_identity_differs")
    size = before["target"]["bytes"]
    with path.open("rb") as stream:
        require(stream.read(4) == b"PAR1", "old_raw_magic_invalid")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "old_raw_trailer_invalid")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= MAX_FOOTER_BYTES and length <= size - 12, "old_raw_footer_byte_limit")
        stream.seek(-8 - length, os.SEEK_END)
        body = stream.read(length)
    require(len(body) == length and sha(body + trailer) == bound["footer_sha256"]
            and inventory.path_identity(path) == before, "old_raw_footer_or_identity_differs")
    metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + body + trailer))
    require(metadata.num_rows == bound["rows_declared"] and metadata.num_row_groups == bound["row_groups"],
            "old_raw_footer_count_differs")
    return metadata, {"path": str(path), "identity": before, "footer_sha256": bound["footer_sha256"],
                      "footer_bytes_read": length + 12, "rows_declared": metadata.num_rows}


def select_old_row_groups(metadata):
    import pyarrow as pa
    schema = metadata.schema.to_arrow_schema()
    require(len(schema.names) == 12 and set(schema.names) == set(FIELDS)
            and metadata.num_columns == 12, "old_raw_schema_differs")
    for name in FIELDS:
        require(schema.field(name).type == (pa.string() if name in STRINGS else pa.int64()), "old_raw_field_type_differs")
    positions = {metadata.schema.column(i).path: i for i in range(metadata.num_columns)}
    require(set(positions) == set(FIELDS), "old_raw_columns_not_flat_unique")
    groups, compressed, rows = [], 0, 0
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        if group.num_rows == 0:
            continue
        stats = group.column(positions["block_number"]).statistics
        require(stats is not None and stats.has_min_max and stats.has_null_count and stats.null_count == 0
                and type(stats.min) is int and type(stats.max) is int and 0 < stats.min <= stats.max,
                "old_block_statistics_required")
        if stats.max < FIRST_BLOCK or stats.min > RAW_TAIL:
            continue
        sizes = [group.column(positions[name]).total_compressed_size for name in FIELDS]
        require(all(type(size) is int and size >= 0 for size in sizes), "projected_column_size_invalid")
        compressed += sum(sizes)
        rows += group.num_rows
        require(compressed <= MAX_PROJECTED_BYTES and rows <= MAX_SELECTED_ROWS, "old_projection_budget_exceeded")
        groups.append(index)
    return {"row_groups": groups, "projected_compressed_bytes": compressed, "selected_rows_declared": rows,
            "projected_fields": list(FIELDS), "batch_rows": BATCH_ROWS,
            "scope": "Producer-declared footer bounds select groups; other body rows are not audited."}


def canonical_old(row):
    require(set(row) == set(FIELDS), "old_row_field_set_differs")
    normalized = dict(row)
    for name in ("order_hash", "transaction_hash"):
        require(type(row[name]) is str and bool(inventory.HEX_HASH.fullmatch(row[name])), "old_hash_invalid")
        normalized[name] = row[name].lower()
    for name in ("maker", "taker", "exchange_address"):
        require(type(row[name]) is str and bool(ADDRESS_RE.fullmatch(row[name])), "old_address_invalid")
        normalized[name] = row[name].lower()
    for name in ("maker_asset_id", "taker_asset_id"):
        require(type(row[name]) is str and bool(DECIMAL_RE.fullmatch(row[name]))
                and int(row[name]) < 2**256, "old_asset_id_invalid")
    for name in set(FIELDS) - STRINGS:
        require(type(row[name]) is int and row[name] >= 0, "old_integer_invalid")
    require(FIRST_BLOCK <= row["block_number"] <= RAW_TAIL, "old_row_outside_overlap")
    return tuple(normalized[name] for name in FIELDS)


def native_key(payload):
    return payload[FIELDS.index("exchange_address")], payload[FIELDS.index("transaction_hash")], payload[FIELDS.index("log_index")]


def decode_old_representation(row):
    """Pure ABI-word decoding of reviewed legacy and V2 extractor rules only."""
    address = row["exchange_address"]
    require(address in OLD_ADDRESSES, "v3_or_unknown_economic_decode_prohibited")
    words = [int(row["data"][offset:offset + 64], 16) for offset in range(2, len(row["data"]), 64)]
    if address in OLD_ADDRESSES[:2]:
        require(len(words) == 5, "legacy_word_count_differs")
        maker_asset, taker_asset, maker_amount, taker_amount, fee = words
    else:
        require(len(words) == 7 and words[0] in (0, 1), "v2_side_or_word_count_not_admitted")
        side, token, maker_amount, taker_amount, fee, _, _ = words
        maker_asset, taker_asset = (0, token) if side == 0 else (token, 0)
    return (row["topics"][1].lower(), "0x" + row["topics"][2][-40:].lower(),
            "0x" + row["topics"][3][-40:].lower(), str(maker_asset), str(taker_asset),
            maker_amount, taker_amount, fee, row["block_number"], row["transactionHash"].lower(),
            row["log_index"], address)


def read_new_rows(raw, manifest, record):
    import pyarrow as pa
    import pyarrow.parquet as pq
    parquet = pq.ParquetFile(pa.BufferReader(raw))
    require(parquet.metadata.num_rows <= MAX_NEW_ROWS, "new_payload_row_limit")
    rows = []
    for batch in parquet.iter_batches(batch_size=BATCH_ROWS, use_threads=False):
        rows.extend(batch.to_pylist())
    logs = []
    for row in rows:
        require(row.get("window") == record["window"]["window"]
                and row.get("source_request_id") == record["log_request_id"]
                and type(row.get("provider_block_timestamp_present")) is bool, "new_row_request_binding_differs")
        log = {key: row[key] for key in pilot.LOG_FIELDS}
        if row["provider_block_timestamp_present"]:
            log["blockTimestamp"] = row["blockTimestamp"]
        else:
            require(row.get("blockTimestamp") is None, "absent_provider_provenance_not_null")
        logs.append(log)
    validated, evidence = pilot.validate_logs(logs, record["window"],
        {item["number"]: item for item in (record["first_header"], record["last_header"])}, record["log_request_id"])
    require(validated == rows and evidence == record["evidence"]
            and manifest.get("rows") == len(rows) == manifest.get("distinct_native_keys"), "new_row_or_evidence_reconciliation_differs")
    counts = Counter(row["exchange_address"] for row in rows)
    intervals = [{"address": address, "topic": pilot.EXPECTED_TOPICS[address], "first_block": FIRST_BLOCK,
                  "last_block": RAW_TAIL, "rows": counts[address],
                  "status": "success_logs" if counts[address] else "success_zero_logs"} for address in pilot.ADDRESSES]
    require(record.get("contract_intervals") == intervals, "new_per_contract_counts_differ")
    return rows


def compare(old_rows, new_rows):
    old, physical, new = {}, Counter(), {}
    old_counts, new_counts = Counter(), Counter(row["exchange_address"] for row in new_rows)
    for row in old_rows:
        payload = canonical_old(row)
        key = native_key(payload)
        item = old.setdefault(key, {"payloads": set(), "raw_digests": set(), "rows": 0})
        item["payloads"].add(payload)
        item["raw_digests"].add(sha(encoded(row)))
        item["rows"] += 1
        old_counts[key[0]] += 1
        physical[key] += 1
    for row in new_rows:
        if row["exchange_address"] == V3_ADDRESS:
            continue
        payload = decode_old_representation(row)
        key = native_key(payload)
        require(key not in new, "new_duplicate_or_conflicting_native_key")
        new[key] = payload
    old_supported = {key for key in old if key[0] in OLD_ADDRESSES}
    missing, extra = old_supported - new.keys(), new.keys() - old_supported
    conflicts = {key for key, item in old.items() if len(item["payloads"]) > 1}
    mismatch = {key for key in old_supported & new.keys() if old[key]["payloads"] != {new[key]}}
    mismatched_fields = Counter()
    for key in mismatch:
        for field, index in zip(FIELDS, range(len(FIELDS))):
            if any(payload[index] != new[key][index] for payload in old[key]["payloads"]):
                mismatched_fields[field] += 1
    buckets = []
    for address in list(pilot.ADDRESSES) + sorted({key[0] for key in old} - set(pilot.ADDRESSES)):
        keys = {key for key in old if key[0] == address}
        buckets.append({"address": address, "old_physical_rows": old_counts[address], "old_distinct_native_keys": len(keys),
            "old_identical_physical_duplicate_rows": sum(old[key]["rows"] - len(old[key]["raw_digests"]) for key in keys),
            "old_canonical_duplicate_rows": sum(old[key]["rows"] - len(old[key]["payloads"]) for key in keys),
            "old_conflicting_native_keys": len(keys & conflicts), "new_raw_rows": new_counts[address],
            "missing_old_keys": sum(key[0] == address for key in missing),
            "extra_new_old_format_keys": sum(key[0] == address for key in extra),
            "mismatched_payload_keys": sum(key[0] == address for key in mismatch)})
    unexpected_old = sum(item["rows"] for key, item in old.items() if key[0] not in OLD_ADDRESSES)
    checks = {"no_old_supported_keys_missing": not missing, "no_extra_new_old_format_keys": not extra,
              "no_old_payload_conflicts": not conflicts, "all_shared_payloads_agree": not mismatch,
              "no_unsupported_old_emitters": unexpected_old == 0}
    def examples(keys):
        return [{"exchange_address": key[0], "transaction_hash": key[1], "log_index": key[2]}
                for key in sorted(keys)[:20]]
    return {"checks": checks, "all_overlap_checks_passed": all(checks.values()), "by_address": buckets,
            "old_physical_rows": sum(physical.values()), "old_distinct_native_keys": len(old),
            "unsupported_old_physical_rows": unexpected_old,
            "new_raw_rows": len(new_rows), "new_old_format_distinct_keys": len(new),
            "new_v3_raw_rows_not_economically_decoded": new_counts[V3_ADDRESS],
            "v3_positive_is_expected_recovered_source_not_old_format_mismatch": True,
            "mismatched_fields": dict(sorted(mismatched_fields.items())),
            "examples": {name: examples(keys) for name, keys in
                         (("missing_old_keys", missing), ("extra_new_old_format_keys", extra),
                          ("old_payload_conflicts", conflicts), ("mismatched_payload_keys", mismatch))}}


def audit_overlap(plan_path, plan_sha256, plan_source_head, collection_dir, manifest_sha256,
                  complete_sha256, collector_source_head, expected_head, output, *, clock=time.monotonic):
    require(not output.exists() and not output.is_symlink(), "output_already_exists")
    audit = Audit(clock)
    result = {"schema_version": "polymarket_native_overlap_audit_v1", "status": "blocked",
              "created_utc": datetime.now(timezone.utc).isoformat(), "first_block": FIRST_BLOCK, "last_block": RAW_TAIL,
              "whole_history_admitted": False, "analysis_admitted": False, "v3_economic_decoding": False,
              "rpc_calls": 0, "spill_bytes": 0, "physical_old_rows_removed": 0,
              "extractor_rule_references": {
                  "legacy": {"sealed_bundle_path": "pipeline/extraction/extract_orderfilled.py",
                             "sha256": "bbaa3e7eed8e7997c32a89806f9365c80e0497d0bcca0a105526f4d5ab297590"},
                  "v2": {"sealed_bundle_path": "pipeline/extraction/extract_orderfilled_v2.py",
                         "sha256": "98db7e8141694e28da04132a35b46b39190b444824d62c2448ca7f8c08012eac"}},
              "limits": {"projected_compressed_bytes": MAX_PROJECTED_BYTES, "selected_old_rows": MAX_SELECTED_ROWS,
                         "batch_rows": BATCH_ROWS, "new_collection_read_bytes": MAX_NEW_BYTES,
                         "output_bytes": MAX_OUTPUT_BYTES, "runtime_seconds": RUNTIME_SECONDS, "address_space_bytes": RAM_BYTES},
              "limitations": ["Only this literal 100-block overlap is reconciled; no whole-history completeness claim.",
                  "Old raw has no block hash: block number and execution payload are compared, not historical block-hash lineage.",
                  "Footer min/max bounds and compressed column sizes are producer declarations, not an independent scan of skipped groups.",
                  "Canonical hex case is normalized only for comparison; physical old rows are preserved and counted separately.",
                  "Legacy 5-word and V2 7-word rules reproduce the reviewed extraction representation; ignored V2 builder/metadata words have no old-column counterpart.",
                  "V3 raw observations are reported separately without token, payout, ABI-era or economic admission.",
                  "Interior exact timestamps, archive value-retention rules and final sample membership are not certified."]}
    try:
        plan, binding, original = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
        require(plan["raw_footer_tail_header"]["number"] == RAW_TAIL, "frozen_raw_endpoint_differs")
        source = inventory.code_state()
        require(bool(re.fullmatch(r"[0-9a-f]{40}", expected_head or "")) and source.get("status") == "inspected"
                and source.get("head") == expected_head and source.get("tracked_worktree_clean") is True,
                "auditor_source_head_or_clean_state_differs")
        with (REPO / "scripts/collect_polymarket_native_increment.py").open("rb") as stream:
            require(sha(stream.read(1024**2)) == COLLECTOR_SHA256, "reviewed_collector_source_differs")
        forbidden = [plan_path, Path(plan["inventory_binding"]["path"]), collection_dir]
        forbidden.extend(Path(entry["path"]) for entry in original["inputs"].values())
        require(all(output.resolve() != path.resolve() and path.resolve() not in output.resolve().parents
                    and output.resolve() not in path.resolve().parents for path in forbidden), "output_overlaps_input")
        manifest, record, raw = load_collection(audit, collection_dir, manifest_sha256,
                                               complete_sha256, collector_source_head, binding)
        require(record["last_header"] == plan["raw_footer_tail_header"], "overlap_tail_header_plan_differs")
        new_rows = read_new_rows(raw, manifest, record)
        old_path = Path(original["inputs"]["raw_events"]["files"][0]["path"])
        metadata, old_binding = old_footer(old_path, original)
        projection = select_old_row_groups(metadata)
        result.update(source=source, source_plan_binding=binding, old_raw_binding=old_binding,
                      collection_manifest_sha256=manifest_sha256, collection_complete_sha256=complete_sha256,
                      collector_source_head=collector_source_head, reviewed_collector_sha256=COLLECTOR_SHA256,
                      old_projection=projection)
        import pyarrow.parquet as pq
        counts = Counter()
        def in_window_rows():
            # Metadata is supplied explicitly; no second footer or unrelated columns need reading.
            parquet = pq.ParquetFile(old_path, metadata=metadata, memory_map=False, pre_buffer=False)
            for batch in parquet.iter_batches(batch_size=BATCH_ROWS, row_groups=projection["row_groups"],
                                               columns=list(FIELDS), use_threads=False):
                audit.check()
                for row in batch.to_pylist():
                    counts["selected_body_rows"] += 1
                    require(counts["selected_body_rows"] <= projection["selected_rows_declared"], "old_selected_row_count_exceeded")
                    block = row["block_number"]
                    require(type(block) is int and block > 0, "old_body_block_invalid")
                    if block < FIRST_BLOCK:
                        counts["below_window"] += 1
                    elif block > RAW_TAIL:
                        counts["above_window"] += 1
                    else:
                        counts["inside_window"] += 1
                        yield row
        comparison = compare(in_window_rows(), new_rows)
        require(counts["selected_body_rows"] == projection["selected_rows_declared"]
                == counts["below_window"] + counts["inside_window"] + counts["above_window"]
                and counts["inside_window"] == comparison["old_physical_rows"], "old_filter_lineage_does_not_reconcile")
        audit.reopen()
        reopened, rebound, reoriginal = pilot.read_plan(plan_path, plan_sha256, plan_source_head)
        require(reopened == plan and rebound == binding and reoriginal == original
                and inventory.path_identity(old_path) == old_binding["identity"]
                and inventory.code_state() == source, "final_source_or_input_binding_differs")
        result.update(status="overlap_reconciled" if comparison["all_overlap_checks_passed"] else "overlap_reconciled_with_failures",
                      comparison=comparison, filter_lineage={key: counts[key] for key in
                          ("selected_body_rows", "below_window", "inside_window", "above_window")},
                      final_input_identity_unchanged=True, bound_collection_files=audit.files)
    except (OverlapBlocked, pilot.PilotBlocked, planner.PlanBlocked, inventory.ReadinessError) as exc:
        if str(exc) == "output_overlaps_input":
            raise  # Do not publish even a blocked result inside an immutable input.
        result["error_code"] = str(exc)
    except Exception:
        result["error_code"] = "overlap_input_metadata_payload_or_resource_failure"
    return result


def publish(output, result):
    raw = encoded(result)
    require(len(raw) <= MAX_OUTPUT_BYTES and not output.exists() and not output.is_symlink(), "output_exists_or_byte_limit")
    pending = output.with_name("." + output.name + "." + uuid.uuid4().hex + ".pending")
    with pending.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, output)
    pending.unlink()
    require(output.read_bytes() == raw, "published_output_reopen_differs")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source-plan", "collection-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("source-plan-sha256", "plan-source-head", "collection-manifest-sha256",
                 "collection-complete-sha256", "collector-source-head", "expected-head"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production overlap audit requires the canonical mounted EC2 checkout.")
    require(args.output.is_absolute() and Path("/mnt/data/runs") in args.output.resolve().parents,
            "production_output_outside_runs")
    for name in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[name] = "1"
    require(inventory.resource_state()["ram_bytes"]["MemAvailable"] >= RAM_BYTES, "minimum_available_ram_required")
    resource.setrlimit(resource.RLIMIT_AS, (RAM_BYTES, RAM_BYTES))
    previous = signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(OverlapBlocked("audit_runtime_limit")))
    signal.alarm(RUNTIME_SECONDS)
    try:
        result = audit_overlap(args.source_plan, args.source_plan_sha256, args.plan_source_head,
            args.collection_dir, args.collection_manifest_sha256, args.collection_complete_sha256,
            args.collector_source_head, args.expected_head, args.output)
        publish(args.output, result)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
    print(json.dumps({"status": result["status"], "analysis_admitted": False, "rpc_calls": 0}, sort_keys=True))
    return 0 if result["status"] == "overlap_reconciled" else 2


if __name__ == "__main__":
    raise SystemExit(main())
