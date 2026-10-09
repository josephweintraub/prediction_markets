#!/usr/bin/env python3
"""Independently audit saved MLB sample artifacts, without reading resolved trades."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb

ROOT = Path(__file__).resolve().parents[1]
EXACT = ("market_id", "token_id", "block_number", "timestamp", "transaction_hash",
         "log_index", "exchange_address", "proxyWallet", "counterparty", "is_maker",
         "outcome", "winning_outcome", "price", "usdcSize")
IDENTITY = ("transaction_hash", "log_index", "exchange_address")
META = ("market_id", "game_pk", "official_date", "winning_outcome", "actual_start_utc", "actual_end_utc")
ENRICHED = EXACT + ("game_pk", "official_date", "actual_start_utc", "actual_end_utc",
                    "buyer_is_flagged_nonhuman", "won", "calibration_error", "trade_day", "realized_time")
INTEGER_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT",
                 "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"}
PARQUETS = ("exact", "all", "filtered", "old", "phase", "flags", "cache", "candidates")
STATUS = "mlb_unfiltered_samples_complete"
EXPECTED = {"prefilter_exact_rows": 6_912_624, "old_exact_rows": 2_555_139,
            "old_accepted_all_rows": 2_502_803, "old_accepted_filtered_rows": 2_412_918}
CAPS = {"memory_limit": "96GB", "threads": 8, "maximum_spill_bytes": 4_000_000_000,
        "maximum_sample_output_bytes": 4_000_000_000, "minimum_free_bytes": 12_000_000_000,
        "maximum_receipt_bytes": 1024**2}
RECONCILIATION = ("old_exact_full_payload_subset", "old_loader_counts_reproduced",
                 "output_full_payload_reopened", "exact_timestamps_verified", "source_fill_ids_one_to_one",
                 "metadata_unique", "lowercase_flag_keys_unique", "inputs_hashes_unchanged", "filtered_subset_of_all")


class AuditBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise AuditBlocked(message)


def sql_path(path: Path) -> str:
    return "'" + str(path.resolve()).replace("'", "''") + "'"


def names(fields, alias="") -> str:
    return ",".join((alias + "." if alias else "") + '"' + field + '"' for field in fields)


def fingerprint(path: Path) -> dict:
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    after = path.stat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), "artifact changed during fingerprint")
    return {"path": str(path.resolve()), "bytes": before.st_size, "sha256": digest.hexdigest()}


def read_head() -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                          capture_output=True, text=True).stdout.strip()


def read_json(path: Path) -> dict:
    require(0 < path.stat().st_size <= CAPS["maximum_receipt_bytes"], "JSON exceeds 1MiB or is empty")
    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "duplicate JSON key")
            value[key] = item
        return value
    def finite(token):
        value = float(token)
        require(math.isfinite(value), "nonfinite JSON number")
        return value
    value = json.loads(path.read_text(), object_pairs_hook=unique, parse_float=finite,
                       parse_constant=lambda token: (_ for _ in ()).throw(AuditBlocked(token)))
    require(isinstance(value, dict), "JSON object required")
    return value


def audit_relations(con, paths: dict[str, Path]) -> dict:
    """Only small scalar results leave DuckDB; callers use synthetic or guarded inputs."""
    require(set(paths) == set(PARQUETS), "eight declared artifact paths required")
    query_count = 0
    artifact_schemas = {}
    def run(query):
        nonlocal query_count
        query_count += 1
        return con.execute(query)
    def number(query):
        value = run(query).fetchone()[0]
        require(type(value) is int and value >= 0, "scalar count must be an exact nonnegative integer")
        return value
    def zero(query, message):
        require(number(query) == 0, message)
    def schema(table):
        return dict((row[0], row[1]) for row in run(f"DESCRIBE SELECT * FROM {table}").fetchall())
    def required(table, fields):
        found = schema(table)
        require(set(fields) <= set(found), "missing artifact columns: " + table)
        return found
    def unique_id(table):
        zero(f"SELECT count(*)-count(DISTINCT({names(IDENTITY)})) FROM {table}", "duplicate native identity: " + table)
    def payload_subset(left, right):
        predicate = " OR ".join(f"l.\"{field}\" IS DISTINCT FROM r.\"{field}\"" for field in EXACT)
        zero(f"SELECT count(*) FROM {left} l LEFT JOIN {right} r USING({names(IDENTITY)}) "
             f"WHERE r.transaction_hash IS NULL OR {predicate}", "missing/altered full payload: " + left)

    run("SET TimeZone='UTC'")
    for name, path in paths.items():
        run(f"CREATE TEMP VIEW qa_{name} AS SELECT * FROM read_parquet({sql_path(path)})")
    required("qa_candidates", ("market_id",))
    zero("SELECT count(*) FROM qa_candidates WHERE market_id IS NULL OR trim(market_id)=''", "null/blank candidate market")
    zero("SELECT count(*)-count(DISTINCT market_id) FROM qa_candidates", "duplicate candidate market")
    for name in ("exact", "old", "all", "filtered"):
        table = "qa_" + name
        fields = ENRICHED if name in {"all", "filtered"} else EXACT
        found = required(table, fields)
        artifact_schemas[name] = [list(item) for item in found.items()]
        require(tuple(found) == fields, "artifact column order/schema differs: " + table)
        require(all(found[field] in INTEGER_TYPES for field in ("block_number", "timestamp", "log_index")),
                "native identity/timestamp must be integer")
        require(found["is_maker"] == "BOOLEAN", "is_maker must be boolean")
        require(all(found[field] == "VARCHAR" for field in EXACT if field not in
                    {"block_number", "timestamp", "log_index", "is_maker", "price", "usdcSize"}) and
                found["price"] == found["usdcSize"] == "DOUBLE", "exact payload types differ")
        if name in {"all", "filtered"}:
            require(found["game_pk"] in INTEGER_TYPES and found["official_date"] == found["trade_day"] == "DATE" and
                    found["actual_start_utc"] == found["actual_end_utc"] == "TIMESTAMP WITH TIME ZONE" and
                    found["buyer_is_flagged_nonhuman"] == found["won"] == "BOOLEAN" and
                    found["calibration_error"] == found["realized_time"] == "DOUBLE", "sample enrichment types differ")
        nulls = " OR ".join(f'"{field}" IS NULL' for field in EXACT)
        blank = " OR ".join(f'trim("{field}")=\'\'' for field in EXACT if field not in
                            {"block_number", "timestamp", "log_index", "is_maker", "price", "usdcSize"})
        zero(f"SELECT count(*) FROM {table} WHERE {nulls} OR {blank} OR block_number<=0 "
             "OR timestamp<=0 OR log_index<0 OR price<=0 OR NOT isfinite(price) "
             "OR usdcSize<=0 OR NOT isfinite(usdcSize)", "invalid exact payload: " + table)
        unique_id(table)
    cache_schema = required("qa_cache", ("block_number", "timestamp"))
    require(all(cache_schema[field] in INTEGER_TYPES for field in cache_schema if field in {"block_number", "timestamp"}),
            "cache keys must be integers")
    zero("SELECT count(*) FROM qa_cache WHERE block_number IS NULL OR timestamp IS NULL OR block_number<=0 OR timestamp<=0",
         "invalid exact-cache key/timestamp")
    zero("SELECT count(*)-count(DISTINCT block_number) FROM qa_cache", "duplicate cached block")
    cache_rows = number("SELECT count(*) FROM qa_cache")
    for table in ("qa_exact", "qa_old"):
        zero(f"SELECT count(*) FROM {table} e LEFT JOIN qa_cache c USING(block_number) "
             "WHERE c.block_number IS NULL OR e.timestamp IS DISTINCT FROM c.timestamp", "missing/nonexact cached timestamp")
    metadata_schema = required("qa_phase", META)
    require(metadata_schema["game_pk"] in INTEGER_TYPES and metadata_schema["official_date"] == "DATE" and
            metadata_schema["actual_start_utc"] in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"} and
            metadata_schema["actual_end_utc"] in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}, "metadata types differ")
    run(f"CREATE TEMP TABLE qa_metadata AS SELECT DISTINCT {names(META)} FROM qa_phase")
    zero("SELECT count(*)-count(DISTINCT market_id) FROM qa_metadata", "conflicting accepted market metadata")
    null_meta = " OR ".join(f'"{field}" IS NULL' for field in META)
    zero(f"SELECT count(*) FROM qa_metadata WHERE {null_meta} OR trim(market_id)='' OR trim(winning_outcome)='' "
         "OR game_pk<=0 OR epoch(actual_start_utc)<=0 OR NOT isfinite(epoch(actual_start_utc)) "
         "OR epoch(actual_start_utc) IS NULL OR epoch(actual_end_utc) IS NULL "
         "OR NOT isfinite(epoch(actual_end_utc)) OR actual_end_utc<=actual_start_utc", "null/invalid accepted market metadata")
    zero("SELECT count(*) FROM qa_metadata ANTI JOIN qa_candidates USING(market_id)", "noncandidate accepted market")
    flag_schema = required("qa_flags", ("proxyWallet", "is_nonhuman"))
    require(flag_schema["is_nonhuman"] == "BOOLEAN", "wallet flag must be boolean")
    zero("SELECT count(*) FROM qa_flags WHERE proxyWallet IS NULL OR trim(proxyWallet)=''", "null/blank wallet flag key")
    zero("SELECT count(*)-count(DISTINCT lower(proxyWallet)) FROM qa_flags", "duplicate/conflicting lowercase wallet flags")
    zero("SELECT count(*) FROM qa_exact e JOIN qa_metadata m USING(market_id) "
         "WHERE e.winning_outcome IS DISTINCT FROM m.winning_outcome", "source/phase winner disagreement")
    zero("SELECT count(*) FROM qa_exact ANTI JOIN qa_candidates USING(market_id)", "noncandidate prefilter market")
    zero("SELECT count(*) FROM qa_candidates ANTI JOIN qa_exact USING(market_id)", "candidate missing from prefilter")
    for fields, value in ((("market_id", "token_id"), "outcome"), (("market_id", "outcome"), "token_id")):
        zero(f"SELECT count(*) FROM (SELECT {names(fields)} FROM qa_exact SEMI JOIN qa_metadata USING(market_id) GROUP BY {names(fields)} "
             f"HAVING count(DISTINCT {value})<>1)", "non-one-to-one market token/outcome mapping")
    zero("SELECT count(*) FROM (SELECT market_id FROM qa_exact SEMI JOIN qa_metadata USING(market_id) "
         "GROUP BY market_id HAVING count(DISTINCT token_id)>2)", "nonbinary accepted token support")
    payload_subset("qa_old", "qa_exact")
    for table in ("qa_all", "qa_filtered"):
        payload_subset(table, "qa_exact")
    payload_subset("qa_filtered", "qa_all")
    run("CREATE TEMP VIEW qa_accepted AS SELECT e.*,m.game_pk,m.official_date,m.actual_start_utc,m.actual_end_utc,"
        "f.proxyWallet IS NULL AS missing_flag,f.is_nonhuman IS NULL AND f.proxyWallet IS NOT NULL AS null_flag,"
        "coalesce(f.is_nonhuman,false) AS flagged "
        "FROM qa_exact e JOIN qa_metadata m USING(market_id) LEFT JOIN qa_flags f ON lower(e.proxyWallet)=lower(f.proxyWallet) "
        "WHERE e.timestamp<=epoch(m.actual_end_utc)")
    all_rule = "price>0 AND price<1"
    filtered_rule = "price>0.01 AND price<0.99 AND NOT flagged"
    for name, rule in (("all", all_rule), ("filtered", filtered_rule)):
        zero(f"SELECT count(*) FROM (SELECT {names(IDENTITY)} FROM qa_accepted WHERE {rule}) e "
             f"ANTI JOIN qa_{name} s USING({names(IDENTITY)})", "missing eligible sample identity: " + name)
        zero(f"SELECT count(*) FROM qa_{name} s ANTI JOIN "
             f"(SELECT {names(IDENTITY)} FROM qa_accepted WHERE {rule}) e USING({names(IDENTITY)})",
             "ineligible sample identity: " + name)
        comparisons = [f"s.{field} IS DISTINCT FROM e.{field}" for field in
                       ("game_pk", "official_date", "actual_start_utc", "actual_end_utc")]
        comparisons += ["s.buyer_is_flagged_nonhuman IS DISTINCT FROM e.flagged",
                        "s.won IS DISTINCT FROM (e.outcome=e.winning_outcome)",
                        "s.calibration_error IS DISTINCT FROM ((e.outcome=e.winning_outcome)::DOUBLE-e.price)",
                        "s.trade_day IS DISTINCT FROM timezone('UTC',to_timestamp(e.timestamp))::DATE",
                        "s.realized_time IS DISTINCT FROM ((e.timestamp-epoch(e.actual_start_utc))/"
                        "(epoch(e.actual_end_utc)-epoch(e.actual_start_utc)))"]
        zero(f"SELECT count(*) FROM qa_{name} s JOIN qa_accepted e USING({names(IDENTITY)}) WHERE " +
             " OR ".join(comparisons), "altered/null sample enrichment: " + name)
    counts = {
        "prefilter_exact_rows": number("SELECT count(*) FROM qa_exact"),
        "old_exact_rows": number("SELECT count(*) FROM qa_old"),
        "new_accepted_all_rows": number("SELECT count(*) FROM qa_all"),
        "new_accepted_filtered_rows": number("SELECT count(*) FROM qa_filtered"),
        "old_accepted_all_rows": number("SELECT count(*) FROM qa_old e JOIN qa_metadata m USING(market_id) "
                                       "WHERE e.timestamp<=epoch(m.actual_end_utc) AND e.price>0 AND e.price<1"),
        "old_accepted_filtered_rows": number("SELECT count(*) FROM qa_old e JOIN qa_metadata m USING(market_id) "
            "LEFT JOIN qa_flags f ON lower(e.proxyWallet)=lower(f.proxyWallet) WHERE e.timestamp<=epoch(m.actual_end_utc) "
            "AND e.price>0.01 AND e.price<0.99 AND NOT coalesce(f.is_nonhuman,false)"),
        "outside_accepted_market_rows": number("SELECT count(*) FROM qa_exact ANTI JOIN qa_metadata USING(market_id)"),
        "post_end_rows": number("SELECT count(*) FROM qa_exact e JOIN qa_metadata m USING(market_id) WHERE e.timestamp>epoch(m.actual_end_utc)"),
        "invalid_sample_price_rows": number("SELECT count(*) FROM qa_accepted WHERE NOT (price>0 AND price<1)"),
        "filtered_extreme_price_exclusions": number("SELECT count(*) FROM qa_accepted WHERE price>0 AND price<1 AND NOT (price>0.01 AND price<0.99)"),
        "filtered_flagged_interior_exclusions": number("SELECT count(*) FROM qa_accepted WHERE price>0.01 AND price<0.99 AND flagged"),
        "accepted_missing_flag_rows": number("SELECT count(*) FROM qa_accepted WHERE price>0 AND price<1 AND missing_flag"),
        "accepted_null_flag_rows": number("SELECT count(*) FROM qa_accepted WHERE price>0 AND price<1 AND null_flag"),
    }
    counts["accepted_valid_price_rows"] = counts["new_accepted_all_rows"]
    counts["source_blocks"] = number("SELECT count(DISTINCT block_number) FROM qa_exact")
    counts["matched_blocks"] = counts["source_blocks"]
    counts["missing_blocks"] = 0
    for sample in ("all", "filtered"):
        counts["restored_" + sample + "_rows"] = number(f"SELECT count(*) FROM qa_{sample} ANTI JOIN qa_old USING({names(IDENTITY)})")
        require(counts["old_accepted_" + sample + "_rows"] + counts["restored_" + sample + "_rows"] ==
                counts["new_accepted_" + sample + "_rows"], "legacy/restored sample partition failed")
    require(counts["prefilter_exact_rows"] == counts["outside_accepted_market_rows"] + counts["post_end_rows"] +
            counts["invalid_sample_price_rows"] + counts["new_accepted_all_rows"], "prefilter sample attrition failed")
    require(counts["new_accepted_all_rows"] == counts["new_accepted_filtered_rows"] +
            counts["filtered_extreme_price_exclusions"] + counts["filtered_flagged_interior_exclusions"], "filtered attrition failed")
    cash = {name: run(f"SELECT coalesce(fsum(usdcSize),0.0) FROM qa_{name}").fetchone()[0]
            for name in ("exact", "all", "filtered", "old")}
    support = {"candidate_markets": number("SELECT count(DISTINCT market_id) FROM qa_candidates"),
               "accepted_metadata_markets": number("SELECT count(DISTINCT market_id) FROM qa_metadata"),
               "accepted_metadata_events": number("SELECT count(DISTINCT game_pk) FROM qa_metadata")}
    for label, table in (("prefilter_exact", "qa_exact"), ("old_exact", "qa_old"),
                         ("new_all", "qa_all"), ("new_filtered", "qa_filtered")):
        support[label] = {"markets": number(f"SELECT count(DISTINCT market_id) FROM {table}"),
                          "accepted_events": number(f"SELECT count(DISTINCT m.game_pk) FROM {table} e JOIN qa_metadata m USING(market_id)")}
    for sample, rule in (("all", "e.price>0 AND e.price<1"),
                         ("filtered", "e.price>0.01 AND e.price<0.99 AND NOT coalesce(f.is_nonhuman,false)")):
        base = "FROM qa_old e JOIN qa_metadata m USING(market_id) LEFT JOIN qa_flags f ON lower(e.proxyWallet)=lower(f.proxyWallet) "
        predicate = "WHERE e.timestamp<=epoch(m.actual_end_utc) AND " + rule
        support["old_" + sample] = {"markets": number("SELECT count(DISTINCT e.market_id) " + base + predicate),
                                  "accepted_events": number("SELECT count(DISTINCT m.game_pk) " + base + predicate)}
    for field in ("candidate_markets", "accepted_metadata_markets", "accepted_metadata_events"):
        counts[field] = support[field]
    for sample in ("new_all", "new_filtered", "old_all", "old_filtered"):
        counts[sample + "_markets"] = support[sample]["markets"]
        counts[sample + "_events"] = support[sample]["accepted_events"]
    return {"counts": counts, "support": support, "timestamp_cache_rows": cache_rows, "artifact_schemas": artifact_schemas,
            "gross_recorded_cash": cash, "query_count": query_count,
            "gates": {name: True for name in ("native_id_uniqueness", "full_payload_membership", "exact_cache",
                       "metadata_and_flag_uniqueness", "winner_token_mapping", "sample_membership", "sample_enrichment", "attrition")}}


def audit_saved(manifest_path: Path, destination: Path, expected_head: str) -> dict:
    require(not destination.exists(), "immutable QA directory already exists")
    require(read_head() == expected_head, "current HEAD differs before saved-artifact QA")
    manifest_identity = fingerprint(manifest_path)
    manifest = read_json(manifest_path)
    require(manifest["status"] == STATUS and manifest["data_certified"] is False and
            manifest["scientific_estimators_rerun"] is False, "incomplete or overstated rebuild")
    source = manifest["source"]
    require(source["expected_head"] == source["head_before"] == source["head_after"] == expected_head,
            "rebuild source HEAD differs")
    require(all(manifest["reconciliation"].get(name) is True for name in RECONCILIATION), "missing/failed producer gate")
    require(not ({"active_leaf", "failure_reason", "error_class", "blocked_original_query"} & manifest.keys()), "partial/blocked residue")
    require(manifest["schema_version"] == 1 and type(manifest["schema_version"]) is int, "unknown manifest schema")
    producer_caps = manifest["caps"]
    for key, value in (("memory_limit", "96GB"), ("threads", 8), ("spill_bytes", 4_000_000_000),
                       ("output_bytes", 4_000_000_000), ("minimum_free_bytes", 12_000_000_000),
                       ("disabled_optimizers", "common_subplan")):
        require(type(producer_caps[key]) is type(value) and producer_caps[key] == value, "producer resource cap differs: " + key)
    counts = manifest["counts"]
    require(all(type(value) is int and value >= 0 for value in counts.values()), "manifest counts must be exact integers")
    require(counts["raw_candidate_rows"] == counts["distinct_candidate_fills"] + counts["duplicate_payload_rows"] and
            counts["distinct_candidate_fills"] == counts["prefilter_exact_rows"], "recorded source dedup law failed")
    require(all(counts[name] == value for name, value in EXPECTED.items()), "frozen legacy/prefilter row gate failed")
    require(sum(manifest["outputs"][name]["bytes"] for name in
                ("exact_trades.parquet", "all_trades.parquet", "filtered_trades.parquet")) <=
            CAPS["maximum_sample_output_bytes"], "written sample output cap exceeded")
    base = manifest_path.resolve().parent
    summary_artifact = manifest["summary_artifact"]
    require(summary_artifact["path"] == "summary.json", "summary completion filename differs")
    summary_path = base / "summary.json"
    summary_identity = fingerprint(summary_path)
    require(type(summary_artifact["bytes"]) is int and
            all(summary_identity[key] == summary_artifact[key] for key in ("bytes", "sha256")), "summary fingerprint differs")
    require(read_json(summary_path) == {key: value for key, value in manifest.items() if key != "summary_artifact"},
            "summary differs from completed manifest")
    paths, recorded = {}, {}
    for name, filename in (("exact", "exact_trades.parquet"), ("all", "all_trades.parquet"), ("filtered", "filtered_trades.parquet")):
        item = manifest["outputs"][filename]
        path = Path(item["path"])
        require(not path.is_absolute() and len(path.parts) == 1 and path.name == filename, "output must be a run-relative filename")
        paths[name], recorded[name] = base / path, item
    for name, label in (("old", "old_exact"), ("phase", "phase"), ("flags", "wallet_flags"), ("cache", "cache"),
                        ("candidates", "candidates")):
        item = manifest["inputs"][label]
        paths[name], recorded[name] = Path(item["path"]), item
        require(paths[name].is_absolute(), "frozen input path must be absolute")
    require(sum(paths[name].stat().st_size for name in ("exact", "all", "filtered")) +
            manifest_path.stat().st_size + summary_path.stat().st_size <= CAPS["maximum_sample_output_bytes"],
            "actual completed sample stage exceeds output cap")
    import pyarrow.parquet as pq
    for name in ("exact", "all", "filtered"):
        require(type(recorded[name]["rows"]) is int and 0 <= recorded[name]["rows"] <= counts["prefilter_exact_rows"] and
                pq.ParquetFile(paths[name]).metadata.num_rows == recorded[name]["rows"], "saved/physical output footer rows differ")
    declaration_path = Path(manifest["inputs"]["timestamp_provenance"]["path"])
    require(declaration_path.is_absolute(), "timestamp declaration path must be absolute")
    declaration = read_json(declaration_path)
    require(declaration["schema_version"] == 1 and type(declaration["schema_version"]) is int and
            declaration["method"] == "polygon_rpc_block_timestamp" and declaration["timestamp_unit"] == "unix_seconds",
            "timestamp declaration method differs")
    build = declaration["build_metadata"]
    require(build["used_exact_cache"] is True and all(type(build[key]) is int and build[key] >= 0 for key in
            ("source_distinct_blocks", "cache_distinct_blocks", "missing_blocks", "fallback_rows")) and
            build["missing_blocks"] == build["fallback_rows"] == 0 and
            build["source_distinct_blocks"] == build["cache_distinct_blocks"] > 0, "invalid exact-cache coverage declaration")
    declared_cache = Path(declaration["cache"]["path"])
    if not declared_cache.is_absolute():
        declared_cache = declaration_path.parent / declared_cache
    require(declared_cache.resolve() == paths["cache"].resolve() and declaration["cache"]["format"] == "parquet",
            "timestamp declaration cache path differs")
    for path in (manifest_path, summary_path, declaration_path, *paths.values()):
        resolved = path.resolve()
        require(destination.resolve() != resolved and destination.resolve() not in resolved.parents and
                resolved not in destination.resolve().parents, "QA destination overlaps an input")
    require(shutil.disk_usage(destination.parent).free >= CAPS["minimum_free_bytes"] + CAPS["maximum_spill_bytes"] +
            CAPS["maximum_receipt_bytes"], "insufficient free storage for bounded QA")
    snapshots = {name: fingerprint(path) for name, path in paths.items()}
    require(declaration["cache"]["sha256"].lower() == snapshots["cache"]["sha256"], "declared cache fingerprint differs")
    for name, item in snapshots.items():
        require(type(recorded[name]["bytes"]) is int and recorded[name]["bytes"] > 0,
                "artifact byte size must be an exact positive integer")
        require(all(item[field] == recorded[name][field] for field in ("bytes", "sha256")), "artifact fingerprint differs: " + name)
    declaration_identity = fingerprint(declaration_path)
    require(all(declaration_identity[field] == manifest["inputs"]["timestamp_provenance"][field] for field in ("bytes", "sha256")),
            "timestamp declaration fingerprint differs")
    source_identities = {name: fingerprint(Path(item["path"])) for name, item in source["files"].items()}
    require(bool(source_identities) and all(all(identity[field] == source["files"][name][field]
            for field in ("bytes", "sha256")) for name, identity in source_identities.items()), "producer source fingerprint differs")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=".mlb-artifact-qa-", dir=destination.parent) as scratch:
        con = duckdb.connect()
        try:
            con.execute("SET memory_limit='96GB'")
            con.execute("SET threads=8")
            con.execute("SET max_temp_directory_size='4GB'")
            con.execute("SET temp_directory=" + sql_path(Path(scratch)))
            if con.execute("SELECT count(*) FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone()[0]:
                con.execute("SET disabled_optimizers='common_subplan'")
            settings = dict(zip(("memory_limit", "threads", "max_temp_directory_size", "disabled_optimizers"),
                            con.execute("SELECT current_setting('memory_limit'),current_setting('threads'),"
                            "current_setting('max_temp_directory_size'),current_setting('disabled_optimizers')").fetchone()))
            result = audit_relations(con, paths)
        finally:
            con.close()
    require(all(result["counts"][name] == counts[name] for name in result["counts"]), "independent/saved count mismatch")
    for name, field in (("exact", "prefilter_exact_rows"), ("all", "new_accepted_all_rows"), ("filtered", "new_accepted_filtered_rows")):
        require(type(recorded[name]["rows"]) is int and recorded[name]["rows"] == result["counts"][field] and
                recorded[name]["schema"] == result["artifact_schemas"][name], "saved output rows/schema differs: " + name)
    require(result["timestamp_cache_rows"] == build["cache_distinct_blocks"], "observed/declared cache row count differs")
    if "rows" in declaration["cache"]:
        require(type(declaration["cache"]["rows"]) is int and declaration["cache"]["rows"] == result["timestamp_cache_rows"],
                "declared cache rows differ")
    if "bytes" in declaration["cache"]:
        require(type(declaration["cache"]["bytes"]) is int and declaration["cache"]["bytes"] == snapshots["cache"]["bytes"],
                "declared cache bytes differ")
    require(snapshots == {name: fingerprint(path) for name, path in paths.items()}, "artifact changed during QA")
    require(shutil.disk_usage(destination.parent).free >= CAPS["minimum_free_bytes"], "storage floor violated during QA")
    require(manifest_identity == fingerprint(manifest_path), "manifest changed during QA")
    require(summary_identity == fingerprint(summary_path) and declaration_identity == fingerprint(declaration_path) and
            source_identities == {name: fingerprint(Path(item["path"])) for name, item in source["files"].items()},
            "summary/declaration/source changed during QA")
    require(read_head() == expected_head, "current HEAD changed during saved-artifact QA")
    result.update(schema_version=1, status="mlb_unfiltered_saved_artifact_qa_complete", data_certified=False,
                  scientific_estimators_rerun=False, manifest=manifest_identity, inputs=snapshots,
                  summary=summary_identity, timestamp_declaration=declaration_identity, producer_sources=source_identities,
                  reviewer_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), caps=CAPS,
                  expected_head=expected_head, command=sys.argv,
                  environment={"python": sys.version, "duckdb": duckdb.__version__, "runtime_settings": settings},
                  wall_seconds=time.monotonic()-started,
                  peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
                  limits="Saved artifact checks only: resolved trades and native filled amounts were not rescanned; producer dedup counts remain recorded evidence. Legacy BUY normalization is retained, not independently certified economic action. Fingerprint reads and query count are recorded, not measured physical I/O. DuckDB memory limit is not a hard process RSS cap.")
    encoded = (json.dumps(result, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(encoded) <= CAPS["maximum_receipt_bytes"], "QA receipt exceeds 1MiB")
    require(shutil.disk_usage(destination.parent).free >= CAPS["minimum_free_bytes"] + len(encoded),
            "receipt publication would breach storage floor")
    destination.mkdir(parents=True, exist_ok=False)
    partial = destination / "receipt.json.partial"
    with partial.open("xb") as stream:
        stream.write(encoded); stream.flush(); os.fsync(stream.fileno())
    os.replace(partial, destination / "receipt.json")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args()
    sys.path.insert(0, str(ROOT))
    from production_guard import require_production_host
    require_production_host()
    actual = read_head()
    require(actual == args.expected_head, "canonical HEAD differs from root-approved HEAD")
    manifest = read_json(args.manifest)
    files = (*manifest["source"]["files"], "scripts/audit_mlb_unfiltered_samples.py", "tests/test_audit_mlb_unfiltered_samples.py")
    for name in files:
        if name in manifest["source"]["files"]:
            require(Path(manifest["source"]["files"][name]["path"]).resolve() == (ROOT / name).resolve(),
                    "producer source path differs from committed repository source")
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=ROOT, capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=ROOT)
        require(tracked.returncode == unchanged.returncode == 0, "producer/reviewer sources must be committed and unchanged")
    result = audit_saved(args.manifest, args.run_dir, args.expected_head)
    print(json.dumps({"status": result["status"], "counts": result["counts"], "data_certified": False}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
