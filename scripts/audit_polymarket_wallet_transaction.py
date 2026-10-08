"""Read-only trace of the first implicated frozen-source March transaction.

Published values supply exhaustive diagnostic candidates, never native identity.
Production body reads require a separately reviewed immutable preflight.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import audit_polymarket_lineage as lineage

SOURCE_BASELINE = lineage.BASELINE_COMMIT
SOURCE_PATHS = lineage.FROZEN_SOURCES + ("pipeline/refresh.py",)
REQUIRED_SOURCE_BLOBS = {
    "pipeline/transform/build_trades.py": "19bacf3c87494b782ca1c87213f0e8d72191ecb5",
    "pipeline/refresh.py": "79e99631b1e919d1aa171db31217e76fc361cfe8",
}
SCRIPT_PATHS = ("scripts/audit_polymarket_wallet_transaction.py",
                "scripts/audit_polymarket_lineage.py")
WINDOWS = (lineage.WINDOWS[0],)
SIGNATURE_FIELDS = tuple(field for field in lineage.VALUE_FIELDS
                         if field not in {"proxyWallet", "counterparty", "eventSlug"})
NATIVE_ROLE_FIELDS = lineage.VALUE_FIELDS + ("exchange_address", "transaction_hash", "log_index")
MAX_OUTPUT_BYTES = 64 * 1024**2
MAX_TRACE_BYTES = 8 * 1024**2
MAX_SUMMARY_BYTES = 1024**2
CONTRACT = {
    "window": list(WINDOWS[0]),
    "selection_order": ["block_number ASC", "transaction_hash ASC (binary string)"],
    "selection_predicate": "transaction produces an exact eight-field class whose whole-minute wallet/counterparty multiset differs from root",
    "candidate_fields": list(SIGNATURE_FIELDS),
    "ambiguity_policy": "Keep the earliest implicated transaction, including collisions or absent published candidates; never substitute another transaction.",
    "completeness": "All preserved raw OrderFilled rows in the selected complete block for the selected transaction, across emitters; no receipt, transfer or chain-collection completeness claim.",
    "identity_policy": "Published values have no native IDs. Exact signatures establish diagnostic candidacy only, even with one candidate.",
    "economic_direction": "Asset-XOR supports the maker's own side only. Legacy opposite counterparty side is synthetic inference.",
    "label_policy": "Preserve eventSlug differences separately; historical refresh execution linkage is unproved.",
    "data_certified": False,
}


def columns(fields) -> str:
    return ",".join(lineage.qname(field) for field in fields)


def exact_join(left: str, right: str, fields) -> str:
    return " AND ".join(f"{left}.{lineage.qname(field)} IS NOT DISTINCT FROM {right}.{lineage.qname(field)}"
                        for field in fields)


def query_rows(con, sql: str) -> list[dict]:
    cursor = con.execute(sql)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def grouped_rows(con, relation: str, fields) -> list[dict]:
    selected = columns(fields)
    return query_rows(con, f"SELECT {selected},count(*) multiplicity FROM {relation} "
                      f"GROUP BY {selected} ORDER BY {selected}")


def exact_json_bytes(value) -> bytes:
    """Refuse nonfinite numbers and verify exact Python JSON round trips."""
    encoded = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True,
                         allow_nan=False).encode("utf-8")
    reopened = json.loads(encoded)

    def equal(original, decoded):
        if type(original) is not type(decoded):
            return False
        if isinstance(original, float):
            return original.hex() == decoded.hex()
        if isinstance(original, dict):
            return original.keys() == decoded.keys() and all(equal(item, decoded[key]) for key, item in original.items())
        if isinstance(original, list):
            return len(original) == len(decoded) and all(equal(a, b) for a, b in zip(original, decoded))
        return original == decoded

    if not equal(value, reopened):
        raise lineage.AuditBlocked("JSON evidence did not round-trip exactly")
    return encoded


def require_expected_head(actual: str, expected: str) -> None:
    if len(expected) != 40 or any(character not in "0123456789abcdef" for character in expected):
        raise lineage.AuditBlocked("expected head must be one full lowercase Git commit hash")
    if actual != expected:
        raise lineage.AuditBlocked("canonical HEAD differs from the root-approved expected head")


def read_reviewed_manifest(path: Path, expected_sha256: str) -> tuple[dict, str]:
    if len(expected_sha256) != 64 or any(character not in "0123456789abcdef" for character in expected_sha256):
        raise lineage.AuditBlocked("reviewed preflight SHA256 must be 64 lowercase hexadecimal characters")
    encoded = path.read_bytes()
    digest = hashlib.sha256(encoded).hexdigest()
    if digest != expected_sha256:
        raise lineage.AuditBlocked("reviewed preflight differs from the root-approved SHA256")
    parsed = json.loads(encoded)
    if not isinstance(parsed, dict):
        raise lineage.AuditBlocked("reviewed preflight must be a JSON object")
    return parsed, digest


def source_snapshot() -> dict:
    """Canonical-only source verification; never read credentials or data bodies."""
    changed = subprocess.run(["git", "diff", "--quiet", SOURCE_BASELINE, "--", *SOURCE_PATHS], cwd=ROOT)
    if changed.returncode:
        raise lineage.AuditBlocked("canonical builder/refresh source differs from frozen baseline")
    blobs = {}
    for path in SOURCE_PATHS:
        blob = subprocess.run(["git", "rev-parse", f"{SOURCE_BASELINE}:{path}"], cwd=ROOT,
                              check=True, text=True, capture_output=True).stdout.strip()
        if not (ROOT / path).is_file():
            raise lineage.AuditBlocked(f"frozen source missing: {path}")
        working_blob = subprocess.run(["git", "hash-object", "--", path], cwd=ROOT,
                                      check=True, text=True, capture_output=True).stdout.strip()
        if working_blob != blob:
            raise lineage.AuditBlocked(f"frozen source blob mismatch: {path}")
        if path in REQUIRED_SOURCE_BLOBS and blob != REQUIRED_SOURCE_BLOBS[path]:
            raise lineage.AuditBlocked(f"predeclared production source blob mismatch: {path}")
        blobs[path] = blob
    return {"baseline_commit": SOURCE_BASELINE, "source_blobs": blobs,
            "script_sha256": {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
                              for path in SCRIPT_PATHS}}


def rank_tie_summary(con, relation: str = "raw_window") -> dict:
    old = ",".join(lineage.literal(item) for item in lineage.OLD_EXCHANGES)
    return lineage.scalar(con, f"""WITH eligible AS (
        SELECT * FROM {relation} WHERE taker NOT IN ({old})), minimum AS (
        SELECT transaction_hash,order_hash,min(log_index) log_index FROM eligible GROUP BY 1,2),
        tied AS (SELECT e.transaction_hash,e.order_hash,count(*) source_rows,
          count(DISTINCT ({','.join('e.' + lineage.qname(f) for f in lineage.RAW_FIELDS)})) payloads
          FROM eligible e JOIN minimum m USING(transaction_hash,order_hash,log_index) GROUP BY 1,2)
        SELECT count(*) FILTER(WHERE payloads>1) conflicting_lowest_log_groups,
          coalesce(sum(source_rows-1) FILTER(WHERE payloads=1),0) identical_lowest_log_replay_surplus
          FROM tied""")


def prepare_reconstruction(con) -> dict:
    native = lineage.native_summary(con, "raw_window")
    defects = {**native["integrity"], **native["roles"], **native["identity"]}
    for field in ("invalid_native_keys", "invalid_payload", "invalid_asset_xor",
                  "unknown_exchange_rows", "conflicting_native_keys"):
        if defects[field]:
            raise lineage.AuditBlocked(f"native integrity gate failed: {field}")
    ties = rank_tie_summary(con)
    if ties["conflicting_lowest_log_groups"]:
        raise lineage.AuditBlocked("ambiguous canonical lowest-log retention")
    con.execute("CREATE TEMP VIEW touched_tokens AS SELECT maker_asset_id token_id FROM canonical_retained UNION SELECT taker_asset_id FROM canonical_retained")
    for relation in ("token_map", "resolutions"):
        duplicate = lineage.scalar(con, f"SELECT count(*) n FROM (SELECT m.token_id FROM {relation} m JOIN touched_tokens t USING(token_id) GROUP BY 1 HAVING count(*)>1)")
        if duplicate["n"]:
            raise lineage.AuditBlocked(f"{relation}: duplicate touched keys")
    metadata = lineage.touched_metadata_summary(con)
    if any(metadata.values()):
        raise lineage.AuditBlocked("touched metadata integrity failed")
    lineage.create_expected_resolved(con)
    if lineage.scalar(con, "SELECT (SELECT count(*) FROM expected_mapped)<>(SELECT count(*) FROM canonical_retained) OR (SELECT count(*) FROM expected_resolved)>(SELECT count(*) FROM expected_mapped) bad")["bad"]:
        raise lineage.AuditBlocked("mapping/resolution join cardinality failed")
    native_difference = lineage.multiplicity_difference(con, "expected_resolved", "resolved_window", lineage.RESOLVED_FIELDS)
    if any(native_difference.values()):
        raise lineage.AuditBlocked("native-to-resolved exact payload failed")
    lineage.create_expansion(con, "resolved_window", "expanded")
    return {"native": native, "canonical_rank_ties": ties, "metadata_integrity": metadata,
            "native_to_resolved_full_payload": native_difference,
            "stage6_price_exclusions": lineage.stage6_price_summary(con, "resolved_window", "expanded")}


def select_transaction(con) -> dict:
    """Compare whole-minute class multisets before stable native selection."""
    wallet_fields = SIGNATURE_FIELDS + ("proxyWallet", "counterparty")
    selected = columns(wallet_fields)
    signature = columns(SIGNATURE_FIELDS)
    con.execute(f"""CREATE TEMP VIEW implicated_classes AS SELECT DISTINCT {signature} FROM (
        (SELECT {selected} FROM expanded EXCEPT ALL SELECT {selected} FROM root_transformed)
        UNION ALL
        (SELECT {selected} FROM root_transformed EXCEPT ALL SELECT {selected} FROM expanded))""")
    candidates = query_rows(con, f"""SELECT DISTINCT r.block_number,e.transaction_hash
        FROM expanded e JOIN resolved_window r
          ON {exact_join('e', 'r', ('exchange_address', 'transaction_hash', 'log_index'))}
        WHERE EXISTS(SELECT 1 FROM implicated_classes s WHERE {exact_join('e', 's', SIGNATURE_FIELDS)})
        ORDER BY r.block_number,e.transaction_hash""")
    if not candidates:
        raise lineage.AuditBlocked("no transaction implicated by the frozen wallet-pair predicate")
    return candidates[0]


def build_trace(con) -> dict:
    reconciliation = prepare_reconstruction(con)
    chosen = select_transaction(con)
    block, tx = chosen["block_number"], lineage.literal(chosen["transaction_hash"])
    # The frozen raw fence relation was read with complete block predicates.
    closure = lineage.scalar(con, f"SELECT count(*) source_rows,count(DISTINCT block_number) blocks,min(block_number) block_number FROM raw WHERE transaction_hash={tx}")
    if closure["blocks"] != 1 or closure["block_number"] != block:
        raise lineage.AuditBlocked("selected transaction has contradictory frozen block membership")
    con.execute(f"CREATE TEMP VIEW transaction_raw AS SELECT * FROM raw WHERE block_number={block} AND transaction_hash={tx}")
    con.execute(f"CREATE TEMP VIEW transaction_resolved AS SELECT * FROM resolved_window WHERE block_number={block} AND transaction_hash={tx}")
    con.execute(f"CREATE TEMP VIEW transaction_mapped AS SELECT * FROM expected_mapped WHERE block_number={block} AND transaction_hash={tx}")
    con.execute(f"CREATE TEMP VIEW transaction_expanded AS SELECT * FROM expanded WHERE transaction_hash={tx}")
    signature = columns(SIGNATURE_FIELDS)
    con.execute(f"CREATE TEMP VIEW transaction_signatures AS SELECT DISTINCT {signature} FROM transaction_expanded")
    for relation, output in (("expanded", "native_candidates"), ("root_transformed", "root_candidates"), ("clean", "clean_candidates")):
        con.execute(f"CREATE TEMP VIEW {output} AS SELECT e.* FROM {relation} e WHERE EXISTS(SELECT 1 FROM transaction_signatures s WHERE {exact_join('e', 's', SIGNATURE_FIELDS)})")
    native_rows = grouped_rows(con, "native_candidates", NATIVE_ROLE_FIELDS)
    root_rows = grouped_rows(con, "root_candidates", lineage.VALUE_FIELDS)
    clean_rows = grouped_rows(con, "clean_candidates", lineage.VALUE_FIELDS)
    selected_rows = grouped_rows(con, "transaction_expanded", NATIVE_ROLE_FIELDS)
    key = lambda row: tuple(row[field] for field in SIGNATURE_FIELDS)
    classes = []
    for value in query_rows(con, f"SELECT {signature} FROM transaction_signatures ORDER BY {signature}"):
        value_key = key(value)
        members = [row for row in native_rows if key(row) == value_key]
        txs = sorted({row["transaction_hash"] for row in members})
        roles = {(row["exchange_address"].lower(), row["transaction_hash"], row["log_index"], row["is_maker"]) for row in members}
        counts = {name: sum(row["multiplicity"] for row in rows if key(row) == value_key)
                  for name, rows in (("expected_whole_minute", native_rows), ("selected_transaction", selected_rows), ("root", root_rows), ("clean", clean_rows))}
        classes.append({**value, "multiplicities": counts, "candidate_transaction_hashes": txs,
                        "candidate_native_roles": len(roles), "cross_transaction_collision": len(txs)>1,
                        "multiple_native_role_candidates": len(roles)>1,
                        "published_native_identity": "absent; association remains unproved"})
    mapped_rows = query_rows(con, f"SELECT {columns(lineage.RAW_FIELDS)},condition_id,outcome,market_slug,event_slug,question,outcome_token_side FROM transaction_mapped ORDER BY log_index,exchange_address")
    retained = Counter(tuple(row[field] for field in lineage.RAW_FIELDS) for row in query_rows(con, f"SELECT {columns(lineage.RAW_FIELDS)} FROM canonical_retained WHERE block_number={block} AND transaction_hash={tx}"))
    resolved_rows = grouped_rows(con, "transaction_resolved", lineage.RESOLVED_FIELDS)
    resolved_counts = Counter()
    for row in resolved_rows:
        resolved_counts[tuple(row[field] for field in lineage.RAW_FIELDS)] += row["multiplicity"]
    mapped = {tuple(row[field] for field in lineage.RAW_FIELDS): row for row in mapped_rows}
    ledger = []
    for row in grouped_rows(con, "transaction_raw", lineage.RAW_FIELDS):
        row_key = tuple(row[field] for field in lineage.RAW_FIELDS)
        if row["taker"] in lineage.OLD_EXCHANGES:
            disposition = "old_exchange_aggregate_exclusion"
        elif not retained[row_key]:
            disposition = "canonical_transaction_order_rank_removal"
        elif mapped[row_key]["condition_id"] is None:
            disposition = "missing_mapping"
        elif not resolved_counts[row_key]:
            disposition = "mapped_without_cached_resolution"
        elif not any(candidate["exchange_address"] == row["exchange_address"] and candidate["log_index"] == row["log_index"] for candidate in selected_rows):
            disposition = "canonical_price_exclusion"
        else:
            disposition = "admitted_legacy_expansion"
        maker_side = "BUY" if row["maker_asset_id"] == "0" else "SELL"
        aggregate_excluded = row["multiplicity"] if row["taker"] in lineage.OLD_EXCHANGES else 0
        rank_removed = row["multiplicity"]-aggregate_excluded-retained[row_key]
        retained_stages = {name: retained[row_key] if disposition == name else 0 for name in (
            "missing_mapping", "mapped_without_cached_resolution", "canonical_price_exclusion", "admitted_legacy_expansion")}
        if rank_removed < 0 or aggregate_excluded+rank_removed+sum(retained_stages.values()) != row["multiplicity"]:
            raise lineage.AuditBlocked("transaction source-occurrence ledger does not reconcile")
        ledger.append({**row, "canonical_retained_occurrences": retained[row_key],
                       "old_aggregate_excluded_occurrences": aggregate_excluded,
                       "canonical_rank_removed_occurrences": rank_removed,
                       "identical_source_replay_surplus": row["multiplicity"]-1,
                       "retained_stage_occurrences": retained_stages,
                       "resolved_native_occurrences": resolved_counts[row_key], "disposition": disposition,
                       "maker_own_side": maker_side, "maker_side_basis": "exact collateral/outcome asset XOR",
                       "legacy_counterparty_side": "SELL" if maker_side == "BUY" else "BUY",
                       "counterparty_side_basis": "synthetic opposite-side inference; economic action unverified"})
    tokens = sorted({row[field] for row in ledger for field in ("maker_asset_id", "taker_asset_id")})
    token_values = ",".join(lineage.literal(token) for token in tokens)
    timestamp_rows = query_rows(con, f"SELECT block_number,timestamp FROM timestamp_slice WHERE block_number={block}")
    if len(timestamp_rows) != 1:
        raise lineage.AuditBlocked("selected transaction lacks one exact cached timestamp")
    con.execute(f"CREATE TEMP VIEW expected_clean_candidates AS SELECT DISTINCT {columns(lineage.VALUE_FIELDS)} FROM root_candidates")
    result = {"status": "complete_diagnostic_existing_certification_failure_preserved", "data_certified": False,
              "selected_transaction": {**chosen, "exact_cached_timestamp": timestamp_rows[0]["timestamp"],
                  "frozen_transaction_source_rows": closure["source_rows"],
                  "complete_frozen_block_source_rows": lineage.scalar(con, f"SELECT count(*) n FROM raw WHERE block_number={block}")["n"]},
              "contract": CONTRACT, "reconciliation": reconciliation,
              "full11_window_difference": lineage.multiplicity_difference(con, "expanded", "root_transformed", lineage.VALUE_FIELDS),
              "full11_selected_values_to_root_candidates": lineage.multiplicity_difference(con, "transaction_expanded", "root_candidates", lineage.VALUE_FIELDS),
              "distinct_root_to_clean_candidate_difference": lineage.multiplicity_difference(con, "expected_clean_candidates", "clean_candidates", lineage.VALUE_FIELDS),
              "native_transaction_ledger": ledger, "resolved_transaction_rows": resolved_rows,
              "mapped_transaction_rows": mapped_rows, "timestamp_cache_rows": timestamp_rows,
              "token_map_rows": query_rows(con, f"SELECT * FROM token_map WHERE token_id IN ({token_values}) ORDER BY token_id"),
              "resolution_cache_rows": query_rows(con, f"SELECT * FROM resolutions WHERE token_id IN ({token_values}) ORDER BY token_id"),
              "reconstructed_transaction_rows": selected_rows, "signature_classes": classes,
              "all_native_role_candidates": native_rows, "all_root_value_candidates": root_rows,
              "all_clean_value_candidates": clean_rows,
              "clean_removal_native_attribution_valid": False,
              "event_label_source_history_note": "The frozen refresh.py source can replace eventSlug with COALESCE(te.eventSlug,'') while preserving wallet/counterparty/role fields. Its historical invocation and cache linkage to these artifacts remain unproved; this diagnostic does not replay it."}
    if len(exact_json_bytes(result)) > MAX_TRACE_BYTES:
        raise lineage.AuditBlocked("complete transaction evidence exceeds 8MiB; no truncation")
    return result


def execute_trace(inputs: dict, window: dict, frozen: list[dict]) -> dict:
    if window["status"] != "preflight_complete":
        raise lineage.AuditBlocked("blocked preflight cannot authorize body reads")
    con = lineage.connection("4GB")
    started = time.monotonic()
    usage_before = resource.getrusage(resource.RUSAGE_SELF)
    try:
        lineage.verify_snapshots(frozen)
        relations = (("raw", lineage.RAW_FIELDS), ("resolved", lineage.RESOLVED_FIELDS),
                     ("root_transformed", lineage.VALUE_FIELDS), ("clean", lineage.VALUE_FIELDS))
        queries = {name: lineage.selected_query(window["selections"][name], fields, name, window["start_utc"][:7]) for name, fields in relations}
        footprints = {name: lineage.streamed_footprint(con, queries[name], name) for name, _ in relations}
        estimate = lineage.enforce_stream_footprints(footprints)
        lineage.verify_snapshots(frozen)
        for name, _ in relations:
            table = lineage.fetch_streamed(con, queries[name], footprints[name])
            footprints[name].update(fetched_rows=table.num_rows, actual_arrow_buffer_bytes=table.nbytes)
            con.register(name, table)
        lineage.verify_snapshots(frozen)
        fences = window["fences"]
        con.execute(f"CREATE VIEW timestamp_slice AS SELECT * FROM read_parquet({lineage.literal(inputs['timestamps'])}) WHERE block_number BETWEEN {fences['lower_fence_block']} AND {fences['upper_fence_block']}")
        for name in ("raw", "resolved"):
            missing = lineage.scalar(con, f"SELECT count(*) n FROM {name} r LEFT JOIN timestamp_slice t USING(block_number) WHERE t.block_number IS NULL")["n"]
            if missing:
                raise lineage.AuditBlocked("missing exact required cached timestamp")
            con.execute(f"CREATE VIEW {name}_window AS SELECT r.* FROM {name} r JOIN timestamp_slice t USING(block_number) WHERE t.timestamp>={fences['start_timestamp']} AND t.timestamp<{fences['end_timestamp_exclusive']}")
        for name in ("token_map", "resolutions"):
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({lineage.literal(inputs[name])})")
        trace = build_trace(con)
        lineage.verify_snapshots(frozen)
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        return {"status": trace["status"], "filtered_footprints": footprints,
                "combined_arrow_payload_estimate_bytes": estimate, "trace": trace,
                "trace_bytes": len(exact_json_bytes(trace)), "peak_rss_bytes": lineage.peak_rss_bytes(),
                "resource_profile": {"wall_seconds": time.monotonic()-started,
                    "user_cpu_seconds": usage_after.ru_utime-usage_before.ru_utime,
                    "system_cpu_seconds": usage_after.ru_stime-usage_before.ru_stime,
                    "rss_note": "Process high-water mark, including earlier stages; not a hard process memory cap."},
                "completed_stages": ["scalar_count_and_byte_gates", "complete_count_matched_fetch",
                    "complete_transaction_and_candidate_trace", "final_input_identity_reopen"],
                "final_input_identity_reopened": True}
    finally:
        con.close()


def compact_summary(result: dict, manifest_bytes: bytes) -> dict:
    """Small exact input-identity proof for root review before body authority."""
    summary = {field: result[field] for field in (
        "schema_version", "status", "data_certified", "contract", "source_snapshot",
        "actual_audit_commit", "expected_head", "environment", "caps", "inputs", "resource_scope",
        "separate_metadata_cache_work", "reviewed_preflight_path", "reviewed_preflight_sha256",
        "reason", "peak_rss_bytes") if field in result}
    identity_fields = ("path", "relation", "bytes", "mtime_ns", "rows", "footer_sha256",
                       "schema", "fields", "serialized_footer_bytes")
    summary["frozen_inputs"] = {name: [{field: info[field] for field in identity_fields}
                                      for info in infos] for name, infos in result.get("inventories", {}).items()}
    summary["windows"] = []
    for window in result.get("windows", []):
        summary["windows"].append({**{field: window[field] for field in (
            "name", "status", "start_utc", "end_utc_exclusive", "fences", "resource_scope",
            "unique_overlapping_compressed_footprint_bytes", "planned_two_pass_compressed_upper_bound_bytes") if field in window},
            "selection_estimates": {name: {field: selection[field] for field in (
                "column", "lower_inclusive", "upper_exclusive", "selected_rows",
                "selected_compressed_bytes", "selected_uncompressed_bytes")}
                for name, selection in window.get("selections", {}).items()}})
    if "diagnostic" in result:
        diagnostic = result["diagnostic"]
        summary["diagnostic"] = {field: diagnostic[field] for field in (
            "status", "filtered_footprints", "combined_arrow_payload_estimate_bytes",
            "trace_bytes", "peak_rss_bytes", "resource_profile", "completed_stages",
            "final_input_identity_reopened")}
        trace = diagnostic["trace"]
        summary["diagnostic"].update(selected_transaction=trace["selected_transaction"],
            full11_window_difference=trace["full11_window_difference"],
            signature_classes=len(trace["signature_classes"]),
            cross_transaction_collision_classes=sum(row["cross_transaction_collision"] for row in trace["signature_classes"]))
    summary.update(manifest_bytes=len(manifest_bytes), manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest())
    return summary


def write_immutable(destination: Path, result: dict) -> None:
    manifest = exact_json_bytes(result)
    summary = exact_json_bytes(compact_summary(result, manifest))
    if len(summary) > MAX_SUMMARY_BYTES:
        raise lineage.AuditBlocked("complete review summary exceeds 1MiB; no truncation")
    outputs = {"summary.json": summary}
    if "diagnostic" in result:
        evidence = exact_json_bytes(result["diagnostic"]["trace"])
        if len(evidence) > MAX_TRACE_BYTES:
            raise lineage.AuditBlocked("complete transaction evidence exceeds 8MiB; no truncation")
        outputs["trace.json"] = evidence
    # Publish the manifest last, so interrupted publication lacks a final manifest.
    outputs["manifest.json"] = manifest
    if sum(len(value) for value in outputs.values()) > MAX_OUTPUT_BYTES:
        raise lineage.AuditBlocked("complete output exceeds 64MiB; no truncation")
    destination.mkdir(parents=True, exist_ok=False)
    for name, encoded in outputs.items():
        partial = destination / (name + ".partial")
        with partial.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, destination / name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--reviewed-preflight")
    parser.add_argument("--reviewed-preflight-sha256")
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", required=True)
    for name, path in lineage.DEFAULT_INPUTS.items():
        parser.add_argument("--" + name.replace("_", "-"), default=path)
    args = parser.parse_args()
    from production_guard import require_production_host
    require_production_host()
    actual_commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                                   text=True, capture_output=True).stdout.strip()
    require_expected_head(actual_commit, args.expected_head)
    for path in SCRIPT_PATHS:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", path], cwd=ROOT, capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", path], cwd=ROOT)
        if tracked.returncode or unchanged.returncode:
            raise lineage.AuditBlocked("both diagnostic scripts must be committed and unchanged")
    reviewed = None
    reviewed_digest = None
    if not args.preflight:
        if not args.reviewed_preflight or not args.reviewed_preflight_sha256:
            raise lineage.AuditBlocked("body reads require a separately reviewed fresh preflight and its root-approved SHA256")
        reviewed, reviewed_digest = read_reviewed_manifest(Path(args.reviewed_preflight), args.reviewed_preflight_sha256)
    destination = Path(args.run_dir)
    if destination.exists():
        raise lineage.AuditBlocked("immutable output directory already exists")
    sources = source_snapshot()
    inputs = {name: getattr(args, name) for name in lineage.DEFAULT_INPUTS}
    con = lineage.connection("4GB")
    try:
        result = lineage.preflight(inputs, con, stream_filtered=True, windows=WINDOWS)
    finally:
        con.close()
    result["caps"].update(max_output_bytes=MAX_OUTPUT_BYTES, max_trace_evidence_bytes=MAX_TRACE_BYTES,
                         max_review_summary_bytes=MAX_SUMMARY_BYTES, preflight_duckdb_memory_limit="4GB")
    result.update(contract=CONTRACT, source_snapshot=sources, command=sys.argv,
                  actual_audit_commit=actual_commit, expected_head=args.expected_head, data_certified=False)
    if not args.preflight:
        if reviewed.get("status") != "preflight_complete" or result["status"] != "preflight_complete":
            raise lineage.AuditBlocked("blocked or incomplete fresh preflight")
        for field in ("inputs", "caps", "inventories", "windows", "baseline_commit", "environment",
                      "resource_scope", "published_month_proofs", "published_directory_layouts", "contract", "source_snapshot", "actual_audit_commit", "expected_head"):
            if reviewed.get(field) != result[field]:
                raise lineage.AuditBlocked(f"current {field} differs from reviewed preflight")
        result["reviewed_preflight_path"] = str(Path(args.reviewed_preflight))
        result["reviewed_preflight_sha256"] = reviewed_digest
        frozen = [info for infos in result["inventories"].values() for info in infos]
        try:
            result["diagnostic"] = execute_trace(inputs, result["windows"][0], frozen)
            result["status"] = result["diagnostic"]["status"]
        except (lineage.AuditBlocked, lineage.duckdb.Error, MemoryError, ValueError) as error:
            result.update(status="blocked_transaction_diagnostic", reason=str(error), peak_rss_bytes=lineage.peak_rss_bytes())
    write_immutable(destination, result)
    print(json.dumps({"status": result["status"], "run_dir": str(destination), "data_certified": False}, allow_nan=False))
    return 0 if result["status"] in {"preflight_complete", "complete_diagnostic_existing_certification_failure_preserved"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
