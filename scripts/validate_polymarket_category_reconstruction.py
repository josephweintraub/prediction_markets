"""EC2-only, metadata-only shadow validation of the frozen category candidate.

No label dataset is published. Immutable JSON records the independent candidate
comparison and a virtual merge that preserves every saved label exactly.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from production_guard import require_production_host
from scripts import reconstruct_polymarket_categories as candidate

RULE_SHA256 = "e1ec7de651dc23f7ca64f5e480ef83ddfe6640004c30fd2413b6b15ffbb0459e"
CANDIDATE_SOURCE_SHA256 = "977de5a4f741c73786956212abf64c31af396ee86a17727212138a18bc4b2be8"
SOURCE_FILES = ("production_guard.py", "scripts/reconstruct_polymarket_categories.py",
                "scripts/validate_polymarket_category_reconstruction.py",
                "tests/test_reconstruct_polymarket_categories.py",
                "tests/test_validate_polymarket_category_reconstruction.py",
                "docs/analysis_specs/polymarket_category_reconstruction_v1.md")
CAPS = {"rows_per_input": 2_000_000, "single_input_bytes": 512 * 1024**2,
        "single_footer_bytes": 1024**2, "row_groups_per_input": 1000,
        "projected_scan_passes": 2, "projected_compressed_read_bytes": 512 * 1024**2,
        "projected_uncompressed_bytes": 1024**3, "total_read_reservation_bytes": 2 * 1024**3,
        "process_address_bytes": 4 * 1024**3, "duckdb_memory_bytes": 3 * 1024**3,
        "spill_bytes": 0, "threads": 2, "batch_rows": 2048,
        "json_bytes": 16 * 1024**2, "output_reservation_bytes": 48 * 1024**2,
        "minimum_free_bytes": 1024**3, "maximum_seconds": 900,
        "tags_per_market": 256, "text_bytes": 512,
        "unknown_tag_types": 16_384, "evidence_strata": 4096, "examples": 25}


class ValidationError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise ValidationError(code)


def identity(path):
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), "input_not_regular_unlinked_file")
    value = path.stat()
    return {"path": str(path.resolve()), "bytes": value.st_size, "device": value.st_dev,
            "inode": value.st_ino, "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def sha256_file(path, maximum_bytes):
    before = identity(path)
    require(before["bytes"] <= maximum_bytes, "hash_input_byte_limit")
    digest = hashlib.sha256()
    consumed = 0
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024**2):
            consumed += len(chunk)
            require(consumed <= maximum_bytes, "hash_input_byte_limit")
            digest.update(chunk)
    require(consumed == before["bytes"] and identity(path) == before, "input_changed_during_hash")
    return digest.hexdigest()


def bind_source(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "expected_head_not_full_sha")
    git = lambda *args: subprocess.check_output(["git", "-C", str(REPO), *args], text=True).strip()
    require(git("rev-parse", "HEAD") == expected_head, "source_head_differs")
    require(not git("status", "--porcelain", "--untracked-files=normal"), "source_checkout_not_clean")
    files = {}
    for name in SOURCE_FILES:
        git("ls-files", "--error-unmatch", "--", name)
        files[name] = sha256_file(REPO / name, 1024**2)
    require(files["scripts/reconstruct_polymarket_categories.py"] == CANDIDATE_SOURCE_SHA256,
            "frozen_candidate_source_changed")
    require(candidate.rule_contract()["rule_sha256"] == RULE_SHA256, "frozen_rule_changed")
    return {"head": expected_head, "clean_checkout": True, "files_sha256": files,
            **candidate.rule_contract()}


def inspect_footer(path, role, expected_rows):
    """Decode a capped footer buffer; never give the footer reader a data file."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    before = identity(path)
    require(path.suffix == ".parquet", "input_not_parquet")
    require(12 <= before["bytes"] <= CAPS["single_input_bytes"], "parquet_input_byte_limit")
    with path.open("rb") as stream:
        require(stream.read(4) == b"PAR1", "invalid_parquet_header")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "invalid_parquet_trailer")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= min(CAPS["single_footer_bytes"], before["bytes"] - 12),
                "footer_byte_limit")
        stream.seek(-8 - length, os.SEEK_END)
        footer = stream.read(length)
        require(len(footer) == length, "short_footer")
    metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + footer + trailer))
    schema = metadata.schema.to_arrow_schema()
    require(len(schema.names) == len(set(schema.names)), "duplicate_schema_field")
    require(metadata.num_rows == expected_rows and 0 <= expected_rows <= CAPS["rows_per_input"],
            "footer_row_count_differs_or_exceeds_cap")
    require(metadata.num_row_groups <= CAPS["row_groups_per_input"], "row_group_limit")
    if role == "metadata":
        required = ("condition_id", "tags")
        require(set(required) <= set(schema.names), "metadata_projection_missing")
        require(pa.types.is_large_string(schema.field("condition_id").type)
                and pa.types.is_list(schema.field("tags").type)
                and pa.types.is_string(schema.field("tags").type.value_type),
                "historical_metadata_schema_differs")
    else:
        require(role == "accepted_labels", "unknown_input_role")
        required = ("mkt", "prim")
        require(set(required) <= set(schema.names)
                and all(pa.types.is_string(schema.field(name).type) for name in required),
                "accepted_label_schema_differs")
    leaves = [i for i in range(metadata.num_columns)
              if metadata.schema.column(i).path.split(".")[0] in required]
    require(len(leaves) == 2, "projection_leaf_count_differs")
    compressed = uncompressed = 0
    for group in range(metadata.num_row_groups):
        for index in leaves:
            column = metadata.row_group(group).column(index)
            require(column.total_compressed_size >= 0 and column.total_uncompressed_size >= 0,
                    "missing_projected_chunk_sizes")
            compressed += column.total_compressed_size
            uncompressed += column.total_uncompressed_size
    require(identity(path) == before, "input_changed_during_footer")
    return {"role": role, "identity": before, "rows": metadata.num_rows,
            "row_groups": metadata.num_row_groups, "projection": list(required),
            "schema": {name: str(schema.field(name).type) for name in required},
            "projected_leaf_paths": [metadata.schema.column(i).path for i in leaves],
            "projected_compressed_bytes": compressed,
            "projected_uncompressed_bytes": uncompressed, "footer_bytes": length,
            "footer_sha256": hashlib.sha256(footer + trailer).hexdigest(),
            "producer": metadata.created_by, "body_rows_read": 0}


def admit_resources(footers, output_parent):
    compressed = sum(item["projected_compressed_bytes"] for item in footers)
    uncompressed = sum(item["projected_uncompressed_bytes"] for item in footers)
    planned = CAPS["projected_scan_passes"] * compressed
    reservation = 2 * sum(item["identity"]["bytes"] for item in footers) + planned + 8 * 1024**2
    require(planned <= CAPS["projected_compressed_read_bytes"], "projected_scan_byte_limit")
    require(uncompressed <= CAPS["projected_uncompressed_bytes"], "projected_uncompressed_byte_limit")
    require(reservation <= CAPS["total_read_reservation_bytes"], "total_read_reservation_limit")
    require((os.cpu_count() or 0) >= CAPS["threads"], "insufficient_cpu")
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.M)
    require(available and int(available.group(1)) * 1024 >= CAPS["process_address_bytes"],
            "insufficient_available_memory")
    free = shutil.disk_usage(output_parent).free
    require(free >= CAPS["output_reservation_bytes"] + CAPS["minimum_free_bytes"],
            "insufficient_output_capacity")
    return {"caps": dict(CAPS), "projected_bytes_per_pass": compressed,
            "projected_scan_read_reservation_bytes": planned,
            "total_read_reservation_bytes": reservation, "observed_free_bytes": free,
            "observed_available_memory_bytes": int(available.group(1)) * 1024,
            "spill_allowed": False}


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def vintage_record(value):
    """Retain a documented date without manufacturing a retrieval instant."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""):
        try:
            date.fromisoformat(value)
        except ValueError as error:
            raise ValidationError("metadata_vintage_invalid") from error
        return {"metadata_vintage_declared": value, "metadata_vintage_precision": "date_only",
                "metadata_vintage_scope": "documented source generation date; exact retrieval instant unknown"}
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise ValidationError("metadata_vintage_invalid") from error
    require(instant.utcoffset() is not None, "metadata_vintage_requires_timezone")
    return {"metadata_vintage_declared": value, "metadata_vintage_precision": "timestamp_with_offset",
            "metadata_vintage_scope": "declared source retrieval instant"}


def configure(con):
    con.execute("SET memory_limit=" + literal(str(CAPS["duckdb_memory_bytes"]) + "B"))
    con.execute("SET threads=" + str(CAPS["threads"]))
    con.execute("SET max_temp_directory_size='0B'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_object_cache=false")


def prepare_views(con, metadata, accepted):
    con.execute("CREATE TEMP VIEW metadata_projection AS SELECT condition_id,tags,1 metadata_present "
                "FROM read_parquet(" + literal(metadata) + ",hive_partitioning=false)")
    con.execute("CREATE TEMP VIEW accepted_projection AS SELECT mkt,prim,1 accepted_present "
                "FROM read_parquet(" + literal(accepted) + ",hive_partitioning=false)")
    health = {}
    for view, key in (("metadata_projection", "condition_id"), ("accepted_projection", "mkt")):
        extra = (",count(*) FILTER(WHERE prim IS NOT NULL AND prim NOT IN (" +
                 ",".join(literal(c) for c in candidate.LABELS) + ")),count(*) FILTER(WHERE prim IS NULL)"
                 if view == "accepted_projection" else "")
        row = con.execute(f"SELECT count(*),count(DISTINCT {key}),count(*) FILTER(WHERE {key} IS NULL "
                          f"OR trim({key})='' OR octet_length(encode({key}))>{CAPS['text_bytes']})"
                          + extra + f" FROM {view}").fetchone()
        require(row[0] == row[1] and row[2] == 0, "duplicate_or_invalid_" + key)
        require(row[0] <= CAPS["rows_per_input"], "observed_row_cap")
        if extra:
            require(row[3] == 0, "saved_label_outside_frozen_categories")
        health[view] = {"rows": row[0], "unique_keys": row[1], "invalid_keys": row[2],
                        **({"raw_null_labels": row[4]} if extra else {})}
    return health


def stream_comparison(con, tag_map, health):
    """Stream one full-outer-joined projected row; hold only capped counters."""
    confusion = {a: {b: 0 for b in candidate.LABELS} for a in candidate.LABELS}
    reasons = Counter()
    all_reasons = Counter()
    unknown = Counter()
    strata = {}
    examples = []
    counts = Counter()
    digest = hashlib.sha256()
    cursor = con.execute("SELECT coalesce(m.condition_id,a.mkt),m.tags,m.metadata_present,"
                         "a.prim,a.accepted_present FROM metadata_projection m FULL OUTER JOIN "
                         "accepted_projection a ON m.condition_id=a.mkt ORDER BY 1")
    previous = None
    while rows := cursor.fetchmany(CAPS["batch_rows"]):
        for market, tags, metadata_present, saved, accepted_present in rows:
            require(previous is None or previous != market, "join_fanout")
            previous = market
            require(len(market.encode()) <= CAPS["text_bytes"], "market_text_byte_limit")
            if isinstance(tags, list):
                require(len(tags) <= CAPS["tags_per_market"], "tag_count_limit")
                require(all(not isinstance(t, str) or len(t.encode()) <= CAPS["text_bytes"] for t in tags),
                        "tag_text_byte_limit")
            decision = candidate.candidate_from_tags(tags, tag_map)
            if metadata_present is None:
                decision["reason"] = "metadata_missing"
            final = saved if accepted_present is not None else decision["category"]
            counts["union"] += 1
            counts["metadata"] += metadata_present is not None
            counts["accepted_labels"] += accepted_present is not None
            all_reasons[decision["reason"]] += 1
            if accepted_present is not None:
                counts["preserved_labels_checked"] += 1
                counts["changed_saved_labels"] += final != saved
                counts["preserved_raw_null_labels"] += saved is None and final is None
            digest.update((json.dumps([market, final, decision], sort_keys=True, separators=(",", ":")) + "\n").encode())
            if metadata_present is None:
                counts["saved_label_without_metadata"] += 1
                continue
            if accepted_present is None:
                counts["metadata_without_saved_label"] += 1
                continue
            counts["overlap"] += 1
            prediction = decision["category"]
            reference = saved if saved is not None else "Unclassified"
            matched = prediction == reference
            assigned = prediction != "Unclassified"
            counts["candidate_assigned"] += assigned
            counts["agreement"] += matched
            counts["assigned_agreement"] += assigned and matched
            confusion[reference][prediction] += 1
            reasons[decision["reason"]] += 1
            unknown.update(decision["unknown_tags"])
            require(len(unknown) <= CAPS["unknown_tag_types"], "unknown_tag_state_limit")
            key = (len(decision["voting_tags"]), len(decision["excluded_tags"]),
                   len(decision["unknown_tags"]), len(decision["votes"]),
                   tuple(decision["overrides"]), decision["reason"])
            if key not in strata:
                require(len(strata) < CAPS["evidence_strata"], "evidence_state_limit")
                strata[key] = {"voting_tag_count": key[0], "excluded_tag_count": key[1],
                    "unknown_tag_count": key[2], "voting_category_count": key[3],
                    "overrides": list(key[4]), "reason": key[5], "markets": 0,
                    "assigned": 0, "agreement": 0}
            strata[key]["markets"] += 1
            strata[key]["assigned"] += assigned
            strata[key]["agreement"] += matched
            if not matched and len(examples) < CAPS["examples"]:
                examples.append({"mkt": market, "saved_prim": saved, "candidate": decision})
    counts["candidate_unclassified"] = counts["overlap"] - counts["candidate_assigned"]
    require(counts["metadata"] == health["metadata_projection"]["rows"], "metadata_row_conservation")
    require(counts["accepted_labels"] == health["accepted_projection"]["rows"], "saved_row_conservation")
    require(counts["union"] == counts["metadata"] + counts["accepted_labels"] - counts["overlap"],
            "union_row_conservation")
    require(counts["changed_saved_labels"] == 0
            and counts["preserved_labels_checked"] == counts["accepted_labels"]
            and counts["preserved_raw_null_labels"] == health["accepted_projection"]["raw_null_labels"],
            "saved_label_preservation_failed")
    require(sum(sum(row.values()) for row in confusion.values()) == counts["overlap"]
            and sum(reasons.values()) == counts["overlap"]
            and sum(item["markets"] for item in strata.values()) == counts["overlap"],
            "shadow_count_reconciliation")
    return {"counts": {key: counts[key] for key in ("union", "metadata", "accepted_labels", "overlap",
                "metadata_without_saved_label", "saved_label_without_metadata", "preserved_labels_checked",
                "changed_saved_labels", "preserved_raw_null_labels", "candidate_assigned",
                "candidate_unclassified", "agreement", "assigned_agreement")},
            "confusion": confusion, "reason_counts_overlap": dict(sorted(reasons.items())),
            "reason_counts_union": dict(sorted(all_reasons.items())),
            "saved_category_counts_overlap": {c: sum(confusion[c].values()) for c in candidate.LABELS},
            "candidate_category_counts_overlap": {c: sum(confusion[s][c] for s in candidate.LABELS)
                                                   for c in candidate.LABELS},
            "unknown_tag_market_counts_overlap": dict(sorted(unknown.items())),
            "unknown_tag_summary_complete": True, "tag_evidence_overlap": [strata[k] for k in sorted(strata)],
            "disagreement_examples": examples, "example_selection": "first_25_by_exact_market_key",
            "virtual_merge_sha256": digest.hexdigest(), "weighting": "equal_Gamma_market",
            "labels_published": False, "candidate_parquet_written": False,
            "interpretation": "Independent shadow comparison; no parity threshold and no original-scorer reproduction claim."}


def write_json(path, value):
    encoded = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(encoded) <= CAPS["json_bytes"], "json_output_byte_limit")
    partial = path.with_name(path.name + ".partial")
    with partial.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(partial, path)  # atomic publication that cannot replace an existing file
    partial.unlink()
    reopened = path.read_bytes()
    require(reopened == encoded, "json_reopen_differs")
    return {"path": path.name, "bytes": len(encoded), "sha256": hashlib.sha256(encoded).hexdigest()}


def execute(args):
    require_production_host()  # precedes source/input inventory, hashing, or body reads
    require(args.expected_rule_sha256 == RULE_SHA256, "expected_rule_hash_differs")
    require(args.expected_map_sha256 == candidate.FROZEN_TAG_MAP_SHA256, "expected_map_hash_differs")
    source = bind_source(args.expected_head)
    for value in (args.expected_metadata_sha256, args.expected_labels_sha256):
        require(re.fullmatch(r"[0-9a-f]{64}", value or ""), "expected_input_hash_invalid")
    vintage = vintage_record(args.metadata_vintage)
    output = Path(args.output)
    require(output.is_absolute() and output.resolve().is_relative_to(Path("/mnt/data/runs"))
            and not output.exists() and output.parent.is_dir(), "exclusive_run_destination_required")
    paths = {"metadata": Path(args.metadata), "accepted_labels": Path(args.accepted_labels),
             "tag_map": Path(args.tag_map)}
    require(len({p.resolve() for p in paths.values()}) == 3, "input_paths_overlap")
    require(all(output.resolve() != p.resolve() and output.resolve() not in p.resolve().parents
                for p in paths.values()), "output_overlaps_input")
    footers = [inspect_footer(paths["metadata"], "metadata", args.expected_metadata_rows),
               inspect_footer(paths["accepted_labels"], "accepted_labels", args.expected_labels_rows)]
    resources = admit_resources(footers, output.parent)
    tag_identity = identity(paths["tag_map"])
    tag_map = candidate.load_frozen_tag_map(paths["tag_map"])
    require(identity(paths["tag_map"]) == tag_identity, "tag_map_changed_during_admission")
    preflight = {"schema_version": "polymarket_category_parity_preflight_v1", "status": "preflight_complete",
                 "source": source, "footers": footers, "resources": resources,
                 "tag_map": {"identity": tag_identity, "sha256": args.expected_map_sha256},
                 **vintage,
                 "input_hashes_expected": {"metadata": args.expected_metadata_sha256,
                                            "accepted_labels": args.expected_labels_sha256},
                 "body_rows_read": 0, "labels_published": False}
    output.mkdir(exist_ok=False)
    preflight_record = write_json(output / "preflight.json", preflight)
    started = time.monotonic()
    try:
        import duckdb
        for item in footers:
            role = item["role"]
            require(sha256_file(paths[role], CAPS["single_input_bytes"])
                    == preflight["input_hashes_expected"][role], "input_hash_differs_" + role)
            require(identity(paths[role]) == item["identity"], "input_identity_changed_before_scan")
        with duckdb.connect() as con:
            configure(con)
            health = prepare_views(con, paths["metadata"], paths["accepted_labels"])
            require(health["metadata_projection"]["rows"] == footers[0]["rows"]
                    and health["accepted_projection"]["rows"] == footers[1]["rows"],
                    "observed_rows_differ_from_admitted_footers")
            summary = stream_comparison(con, tag_map, health)
        for item in footers:
            role = item["role"]
            require(identity(paths[role]) == item["identity"]
                    and sha256_file(paths[role], CAPS["single_input_bytes"])
                    == preflight["input_hashes_expected"][role], "input_changed_after_scan")
        require(identity(paths["tag_map"]) == tag_identity
                and sha256_file(paths["tag_map"], 1024**2) == args.expected_map_sha256,
                "tag_map_changed_after_scan")
        require(bind_source(args.expected_head) == source, "source_changed_during_validation")
        validation = {"schema_version": "polymarket_category_parity_evidence_v1", "status": "validation_complete",
                      "rule_sha256": RULE_SHA256, "health": health, **summary}
        result = write_json(output / "validation.json", validation)
        manifest = {"schema_version": "polymarket_category_parity_manifest_v1",
                    "status": "metadata_parity_complete_no_label_publication", "source": source,
                    "preflight": preflight_record, "validation": result,
                    "elapsed_seconds": time.monotonic() - started,
                    "input_identity_and_hashes_rechecked": True, "labels_published": False,
                    "runtime": {"python": sys.version.split()[0], "duckdb": duckdb.__version__},
                    "real_data_parity_not_original_scorer_reproduction": True}
        write_json(output / "manifest.json", manifest)
        return manifest
    except Exception as error:
        write_json(output / "failure.json", {"status": "failed_unaccepted",
                   "error_type": type(error).__name__,
                   "error_code": str(error) if isinstance(error, ValidationError) else "runtime_failure",
                   "elapsed_seconds": time.monotonic() - started, "labels_published": False})
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("metadata", "accepted-labels", "tag-map", "output", "expected-head",
                 "expected-rule-sha256", "expected-map-sha256", "expected-metadata-sha256",
                 "expected-labels-sha256", "metadata-vintage"):
        parser.add_argument("--" + name, required=True)
    for name in ("expected-metadata-rows", "expected-labels-rows"):
        parser.add_argument("--" + name, required=True, type=int)
    args = parser.parse_args(argv)
    # Production refusal happens before changing resource limits or opening data.
    require_production_host()
    # These libraries may otherwise reserve large thread-stack address spaces
    # while importing Arrow/DuckDB. The process does not perform BLAS work.
    for variable in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ[variable] = "1"
    old_limit = resource.getrlimit(resource.RLIMIT_AS)
    memory_limit = min(CAPS["process_address_bytes"], old_limit[0]) if old_limit[0] >= 0 else CAPS["process_address_bytes"]
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit, old_limit[1]))
    old_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(ValidationError("runtime_limit")))
    signal.alarm(CAPS["maximum_seconds"])
    try:
        manifest = execute(args)
        print(json.dumps({"status": manifest["status"], "output": args.output}))
        return 0
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)
        resource.setrlimit(resource.RLIMIT_AS, old_limit)


if __name__ == "__main__":
    raise SystemExit(main())
