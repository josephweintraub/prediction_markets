#!/usr/bin/env python3
"""Rebuild frozen MLB inferred-BUY inputs without upstream price/bot exclusions."""
from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
HELPER_SHA = "6802cfe15e21b6f4375fc8655f88b33a7d9de0e21d09a716d688f78dea125f44"
TIMESTAMP_HELPER_SHA = "12602e6d6965fb613da31788ea4488f192e6d542cc10301634979c0f6ed9497f"
FROZEN_HASHES = {
    "raw": "346a3d26ca99d980480b9d39f2a90b8e0b6baafe146a6080e3029ebe30c0c520",
    "cache": "fec60fed5c1a36664f164457447b120cddae057aebb6ca43592e48d68ae28002",
    "old_exact": "ef85a4326100f0e4e73e82c3f2a6a0a81e2a675782b6ebb291a1b595f771a0a1",
    "wallet_flags": "e1bfb6163db0e0112c2bf912de7d62353cc75378547e5cfde27a008979836c0a",
}
FROZEN_COUNTS = {"distinct_candidate_fills": 6912624, "old_exact_rows": 2555139,
                 "old_accepted_all_rows": 2502803, "old_accepted_filtered_rows": 2412918}
CAPS = {"memory_limit": "96GB", "memory_bytes": 96000000000, "threads": 8,
        "spill_bytes": 4000000000, "output_bytes": 4000000000,
        "minimum_free_bytes": 12000000000, "maximum_candidate_rows": 8000000,
        "maximum_candidate_markets": 5000, "maximum_cache_rows": 40000000,
        "maximum_planned_read_bytes": 1024**4, "maximum_json_bytes": 1024**2,
        "disabled_optimizers": "common_subplan"}
INPUT_NAMES = ("raw", "candidates", "timestamp_provenance", "cache", "phase", "wallet_flags", "old_exact")
EXACT_COLUMNS = ("market_id", "token_id", "block_number", "timestamp", "transaction_hash",
                 "log_index", "exchange_address", "proxyWallet", "counterparty", "is_maker",
                 "outcome", "winning_outcome", "price", "usdcSize")
IDENTITY = ("transaction_hash", "log_index", "exchange_address")
META_COLUMNS = ("market_id", "game_pk", "official_date", "winning_outcome", "actual_start_utc", "actual_end_utc")
DEFINITIONS = {
    "prefilter": "all distinct candidate fills; no price-range or bot exclusion; invalid payload/amount/timestamp blocks publication",
    "all_trades": "accepted frozen phase market cohort; timestamp <= actual_end_utc; 0 < price < 1; flagged buyers retained",
    "filtered_trades": "same cohort/time; 0.01 < price < 0.99; NOT coalesce(current shared flag.is_nonhuman,false)",
    "time": "exact cached UTC Unix seconds; all pregame history retained without lower cutoff; recorded event end included",
    "observation": "legacy inferred outcome-token BUY per resolved fill, not certified counterparty own action",
    "exclusion_order": ["outside_accepted_market_rows", "post_end_rows", "invalid_sample_price_rows", "accepted_valid_price_rows"],
    "filtered_exclusion_order": ["filtered_extreme_price_exclusions", "filtered_flagged_interior_exclusions", "new_accepted_filtered_rows"],
    "flag_support": "missing/null support counted in all_trades; current null/missing flags retain COALESCEfalse",
    "fixed_time": "not exported; unchanged downstream loader recomputes sport median duration among unique events represented in each sample",
}


class RebuildBlocked(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RebuildBlocked(message)


def quote(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def stat_identity(path: Path) -> dict:
    value = path.stat()
    require(path.is_file(), "input must be a regular file: " + str(path))
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def sha256(path: Path) -> str:
    before = stat_identity(path)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    require(before == stat_identity(path), "file changed while hashing: " + str(path))
    return digest.hexdigest()


def read_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def process_command() -> list[str]:
    if platform.system() == "Linux":
        return [os.fsdecode(value) for value in Path("/proc/self/cmdline").read_bytes().split(b"\0") if value]
    return [sys.executable, *sys.argv]


def progress(stage: str, **values) -> None:
    print(json.dumps({"stage": stage, **values}, sort_keys=True), file=sys.stderr, flush=True)


def load_frozen_helper(builder: Path, timestamps: Path):
    """Import immutable original helpers; alternate paths are used only by fixtures."""
    require(sha256(builder) == HELPER_SHA and sha256(timestamps) == TIMESTAMP_HELPER_SHA,
            "frozen MLB helper source differs")
    dep_spec = importlib.util.spec_from_file_location("timestamp_provenance", timestamps)
    dep = importlib.util.module_from_spec(dep_spec)
    old_dep = sys.modules.get("timestamp_provenance")
    sys.modules["timestamp_provenance"] = dep
    try:
        dep_spec.loader.exec_module(dep)
        spec = importlib.util.spec_from_file_location("_mlb_rebuild_frozen_helper", builder)
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
    finally:
        if old_dep is None:
            sys.modules.pop("timestamp_provenance", None)
        else:
            sys.modules["timestamp_provenance"] = old_dep
    require(tuple(helper.OUTPUT_COLUMNS) == EXACT_COLUMNS, "frozen exact schema differs")
    return helper


def load_helper():
    folder = REPO / "analysis/mlb_game_dynamics"
    return load_frozen_helper(folder / "build_exact_trades.py", folder / "timestamp_provenance.py")


def source_files(helper) -> dict:
    files = {
        "scripts/rebuild_mlb_unfiltered_samples.py": Path(__file__).resolve(),
        "docs/analysis_specs/mlb_unfiltered_rebuild_v1.md": REPO / "docs/analysis_specs/mlb_unfiltered_rebuild_v1.md",
        "analysis/mlb_game_dynamics/build_exact_trades.py": Path(helper.__file__).resolve(),
        "analysis/mlb_game_dynamics/timestamp_provenance.py": Path(helper.validate_timestamp_provenance.__globals__["__file__"]).resolve(),
    }
    return {name: {"path": str(path), "bytes": path.stat().st_size, "sha256": sha256(path)}
            for name, path in files.items()}


def require_committed_sources(helper) -> None:
    for name in (*source_files(helper), "tests/test_rebuild_mlb_unfiltered_samples.py"):
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=REPO)
        require(tracked.returncode == unchanged.returncode == 0, "rebuild sources must be committed and unchanged")


def available_memory() -> int:
    require(platform.system() == "Linux", "resource preflight requires Linux")
    text = Path("/proc/meminfo").read_text()
    match = re.search(r"^MemAvailable:\s+(\d+) kB$", text, re.MULTILINE)
    require(match is not None, "available RAM unavailable")
    return int(match.group(1)) * 1024


def existing_parent(path: Path) -> Path:
    while not path.exists():
        path = path.parent
    return path


def preflight(paths: dict[str, Path], target: Path, expected_head: str, helper) -> dict:
    require(set(paths) == set(INPUT_NAMES), "input inventory differs")
    helper._validate_run_destination(target, list(paths.values()))
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head) is not None and read_head() == expected_head,
            "source HEAD differs")
    inventory = {}
    for name, path in paths.items():
        entry = {"path": str(path), "stat": stat_identity(path)}
        if name != "timestamp_provenance":
            footer = pq.ParquetFile(path)
            entry.update(rows=footer.metadata.num_rows,
                         row_groups=footer.metadata.num_row_groups,
                         schema=str(footer.schema_arrow))
        entry["expected_sha256"] = FROZEN_HASHES.get(name)
        inventory[name] = entry
    require(0 < inventory["candidates"]["rows"] <= CAPS["maximum_candidate_markets"], "candidate footer exceeds cap")
    require(0 < inventory["cache"]["rows"] <= CAPS["maximum_cache_rows"], "cache footer exceeds cap")
    planned = 3 * inventory["raw"]["stat"]["bytes"] + 48 * sum(
        value["stat"]["bytes"] for name, value in inventory.items() if name != "raw") + 4 * CAPS["output_bytes"]
    require(planned <= CAPS["maximum_planned_read_bytes"], "planned read footprint exceeds cap")
    working = (6 * 1024 * FROZEN_COUNTS["distinct_candidate_fills"] +
               64 * inventory["cache"]["rows"] + 256 * inventory["phase"]["rows"] +
               256 * inventory["wallet_flags"]["rows"] + CAPS["memory_bytes"] // 25)
    require(working <= CAPS["memory_bytes"], "conservative working estimate exceeds configured memory")
    free = shutil.disk_usage(existing_parent(target.parent)).free
    require(free >= CAPS["minimum_free_bytes"] + CAPS["spill_bytes"] + CAPS["output_bytes"],
            "insufficient disk reserve for spill/output/free floor")
    memory = available_memory()
    require(memory >= CAPS["memory_bytes"] + 12000000000, "insufficient available RAM")
    require((os.cpu_count() or 0) >= CAPS["threads"], "insufficient CPU count")
    require(all(value["stat"] == stat_identity(paths[name]) for name, value in inventory.items()),
            "input changed during footer preflight")
    return {"schema_version": 1, "status": "mlb_rebuild_preflight_complete", "data_certified": False,
            "source": {"expected_head": expected_head, "files": source_files(helper)},
            "caps": dict(CAPS), "inputs": inventory, "full_input_hashes_verified": False,
            "planned_read_bytes": planned, "working_memory_estimate_bytes": working,
            "available_memory_bytes": memory, "free_disk_bytes": free,
            "limits": "Footer/stat/resource admission only, no trade queries. Full input SHA-256 verification precedes body queries and repeats before publication. Read footprint is conservative planning, not measured I/O."}


def freeze_inputs(paths: dict[str, Path], reviewed: dict, expected_hashes: dict) -> dict:
    frozen = {}
    for name, path in paths.items():
        before = stat_identity(path)
        require(reviewed["inputs"][name]["path"] == str(path) and reviewed["inputs"][name]["stat"] == before,
                "reviewed input stat/path differs: " + name)
        progress("hash_input_before", input=name, bytes=before["bytes"])
        digest = sha256(path)
        require(name not in expected_hashes or digest == expected_hashes[name], "frozen input SHA-256 differs: " + name)
        frozen[name] = {"path": str(path), "bytes": before["bytes"], "sha256": digest,
                        "stat_before": before}
    return frozen


def configure(con, spill: Path) -> dict:
    optimizers = {row[0] for row in con.execute("SELECT name FROM duckdb_optimizers()").fetchall()}
    if "common_subplan" in optimizers:
        con.execute("SET disabled_optimizers='common_subplan'")
    con.execute(f"SET memory_limit='{CAPS['memory_limit']}'")
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute(f"SET max_temp_directory_size='{CAPS['spill_bytes']}B'")
    con.execute(f"SET temp_directory='{quote(spill)}'")
    con.execute("SET TimeZone='UTC'")
    settings = dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
        "('disabled_optimizers','memory_limit','threads','max_temp_directory_size','TimeZone')").fetchall())
    require(settings["TimeZone"] == "UTC" and int(settings["threads"]) == CAPS["threads"], "runtime settings differ")
    require("common_subplan" not in optimizers or settings["disabled_optimizers"] == "common_subplan", "optimizer disable failed")
    return {"duckdb": duckdb.__version__, "python": sys.version, "executable": sys.executable, "platform": platform.platform(),
            "runtime_settings": settings, "common_subplan_available": "common_subplan" in optimizers}


def number(con, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def unique_ids(con, relation: str) -> None:
    invalid = " OR ".join(f"{field} IS NULL" for field in IDENTITY)
    require(number(con, f"SELECT count(*) FROM {relation} WHERE {invalid}") == 0, "null fill identity")
    require(number(con, f"SELECT count(*)-count(DISTINCT ({','.join(IDENTITY)})) FROM {relation}") == 0,
            "duplicate fill identity: " + relation)


def full_equal(con, left: str, right: str, columns: tuple, *, subset: bool = False) -> None:
    values = ",".join(f'"{name}"' for name in columns)
    missing = number(con, f"SELECT count(*) FROM (SELECT {values} FROM {left} EXCEPT ALL SELECT {values} FROM {right})")
    require(missing == 0, "full payload missing or changed: " + left)
    if not subset:
        require(number(con, f"SELECT count(*) FROM (SELECT {values} FROM {right} EXCEPT ALL SELECT {values} FROM {left})") == 0,
                "full payload excess: " + right)


def derive(con, source: str, destination: str) -> None:
    con.execute(f"""CREATE TEMP TABLE {destination} AS
        SELECT e.*,m.game_pk::BIGINT game_pk,m.official_date::DATE official_date,
               m.actual_start_utc::TIMESTAMPTZ actual_start_utc,m.actual_end_utc::TIMESTAMPTZ actual_end_utc,
               coalesce(w.is_nonhuman,false)::BOOLEAN buyer_is_flagged_nonhuman,
               (e.outcome=m.winning_outcome)::BOOLEAN won,
               ((e.outcome=m.winning_outcome)::DOUBLE-e.price)::DOUBLE calibration_error,
               timezone('UTC',to_timestamp(e.timestamp))::DATE trade_day,
               ((e.timestamp-epoch(m.actual_start_utc))/(epoch(m.actual_end_utc)-epoch(m.actual_start_utc)))::DOUBLE realized_time
        FROM {source} e JOIN metadata m USING(market_id)
        LEFT JOIN current_flags w ON e.proxyWallet=w.proxyWallet
        WHERE e.timestamp<=epoch(m.actual_end_utc) AND e.price>0 AND e.price<1""")


def create_relations(con, paths: dict, helper, expected_counts: dict) -> tuple[dict, dict]:
    helper._create_input_views(con, paths["raw"], paths["candidates"], paths["cache"], paths["wallet_flags"])
    helper._validate_inputs(con)
    raw_schema = helper._schema(con, "raw_input")
    string_fields = ("order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
                     "transaction_hash", "exchange_address", "condition_id", "outcome", "winning_outcome", "outcome_token_side")
    require(all(raw_schema[name] == "VARCHAR" for name in string_fields), "raw string payload field type differs")
    helper._assert_valid_candidates(con)
    declaration = helper.load_timestamp_provenance(paths["timestamp_provenance"])
    timestamp_report = helper.validate_timestamp_provenance(declaration, con)
    progress("exact_cache_validated")
    require(Path(timestamp_report["cache"]["path"]).resolve() == paths["cache"], "declared cache path differs")
    require(number(con, "SELECT count(*) FROM exact_cache WHERE block_number<=0 OR timestamp<=0") == 0,
            "nonpositive cached block/timestamp")
    con.execute("CREATE TEMP TABLE candidate_markets AS SELECT market_id::VARCHAR market_id FROM candidate_input")
    con.execute(f"""CREATE TEMP TABLE candidate_source AS
        SELECT raw.order_hash::VARCHAR order_hash,lower(raw.maker)::VARCHAR maker,
               lower(raw.taker)::VARCHAR taker,raw.maker_asset_id::VARCHAR maker_asset_id,
               raw.taker_asset_id::VARCHAR taker_asset_id,raw.maker_amount_filled,raw.taker_amount_filled,
               raw.fee,raw.block_number,lower(raw.transaction_hash)::VARCHAR transaction_hash,
               raw.log_index,lower(raw.exchange_address)::VARCHAR exchange_address,
               raw.condition_id::VARCHAR condition_id,raw.outcome::VARCHAR outcome,
               raw.winning_outcome::VARCHAR winning_outcome,raw.outcome_token_side::VARCHAR outcome_token_side
        FROM raw_input raw SEMI JOIN candidate_markets candidate ON raw.condition_id=candidate.market_id
        LIMIT {CAPS['maximum_candidate_rows'] + 1}""")
    require(number(con, "SELECT count(*) FROM candidate_source") <= CAPS["maximum_candidate_rows"], "candidate body exceeds row cap")
    helper._assert_valid_candidate_source(con)
    require(number(con, "SELECT count(*) FROM candidate_source WHERE maker_amount_filled<=0 OR taker_amount_filled<=0 "
        "OR fee<0 OR block_number<=0 OR log_index<0 OR trim(order_hash)='' OR trim(maker_asset_id)='' "
        "OR trim(taker_asset_id)='' OR trim(maker)='' OR trim(taker)='' OR trim(transaction_hash)='' "
        "OR trim(exchange_address)='' OR trim(outcome)='' OR trim(winning_outcome)=''") == 0, "invalid candidate payload/amount")
    raw_rows, fills, replays = helper._deduplicate_fills(con)
    progress("candidate_fills_reconciled", raw_rows=raw_rows, distinct_fills=fills, exact_payload_replays=replays)
    require(fills == expected_counts["distinct_candidate_fills"], "distinct candidate count differs from frozen vintage")
    blocks, matched, missing = helper._assert_block_coverage(con)
    # Reuse the exact frozen CASE expression. Its transient filtered tables are not published or used.
    con.execute("CREATE TEMP TABLE bot_wallets(proxyWallet VARCHAR)")
    helper._create_buy_relations(con)
    con.execute("DROP TABLE output_rows"); con.execute("DROP TABLE price_eligible")
    require(number(con, "SELECT count(*) FROM buy_all") == fills, "source/BUY one-to-one failed")
    unique_ids(con, "buy_all")
    full_equal(con, "fills", "buy_all", IDENTITY)
    require(number(con, "SELECT count(*) FROM buy_all WHERE price IS NULL OR usdcSize IS NULL "
        "OR NOT isfinite(price) OR NOT isfinite(usdcSize) OR price<=0 OR usdcSize<=0 "
        "OR token_id IS NULL OR trim(token_id)='' OR is_maker IS NULL") == 0, "invalid prefilter exact payload")
    helper.verify_output_timestamps(con, "buy_all", "exact_cache")
    con.execute("DROP TABLE candidate_source"); con.execute("DROP TABLE unique_payloads"); con.execute("DROP TABLE fills")
    con.execute(f"CREATE TEMP VIEW phase_input AS SELECT * FROM read_parquet('{quote(paths['phase'])}')")
    meta_schema = helper._require_columns(con, "phase_input", set(META_COLUMNS), "Frozen phase metadata")
    require(meta_schema["game_pk"] in helper.INTEGER_TYPES and meta_schema["official_date"] == "DATE" and
            all(meta_schema[name] in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"} for name in
                ("actual_start_utc", "actual_end_utc")), "metadata field types differ")
    null_meta = " OR ".join(f"{name} IS NULL" for name in META_COLUMNS)
    require(number(con, f"SELECT count(*) FROM phase_input WHERE {null_meta} OR trim(market_id)='' "
        "OR trim(winning_outcome)='' OR game_pk<=0 OR epoch(actual_start_utc)<=0 "
        "OR epoch(actual_start_utc) IS NULL OR epoch(actual_end_utc) IS NULL "
        "OR NOT isfinite(epoch(actual_start_utc)) OR NOT isfinite(epoch(actual_end_utc)) "
        "OR actual_end_utc<=actual_start_utc") == 0, "invalid timing/outcome metadata")
    con.execute(f"CREATE TEMP TABLE metadata AS SELECT DISTINCT {','.join(META_COLUMNS)} FROM phase_input")
    require(number(con, "SELECT count(*) FROM metadata") > 0, "empty accepted metadata")
    require(number(con, "SELECT count(*) FROM (SELECT market_id FROM metadata GROUP BY 1 HAVING count(*)<>1)") == 0,
            "nonunique market metadata")
    require(number(con, "SELECT count(*) FROM metadata ANTI JOIN candidate_markets USING(market_id)") == 0,
            "accepted market absent from candidate universe")
    require(number(con, "SELECT count(*) FROM buy_all e JOIN metadata m USING(market_id) "
        "WHERE e.winning_outcome IS DISTINCT FROM m.winning_outcome") == 0, "accepted winning outcome contradiction")
    require(number(con, "SELECT count(*) FROM (SELECT market_id,token_id FROM buy_all SEMI JOIN metadata USING(market_id) "
        "GROUP BY 1,2 HAVING count(DISTINCT outcome)<>1)") == 0, "ambiguous token/outcome mapping")
    require(number(con, "SELECT count(*) FROM (SELECT market_id,outcome FROM buy_all SEMI JOIN metadata USING(market_id) "
        "GROUP BY 1,2 HAVING count(DISTINCT token_id)<>1)") == 0, "ambiguous outcome/token mapping")
    require(number(con, "SELECT count(*) FROM (SELECT market_id FROM buy_all SEMI JOIN metadata USING(market_id) "
        "GROUP BY 1 HAVING count(DISTINCT token_id)>2)") == 0, "nonbinary accepted token support")
    require(number(con, "SELECT count(*) FROM wallet_flag_input WHERE proxyWallet IS NULL OR trim(proxyWallet)=''") == 0,
            "null/blank flag wallet key")
    require(number(con, "SELECT count(*) FROM (SELECT lower(proxyWallet) FROM wallet_flag_input GROUP BY 1 HAVING count(*)<>1)") == 0,
            "nonunique lowercased flag keys")
    con.execute("CREATE TEMP TABLE current_flags AS SELECT lower(proxyWallet)::VARCHAR proxyWallet,is_nonhuman FROM wallet_flag_input")
    con.execute(f"CREATE TEMP TABLE old_rows AS SELECT * FROM read_parquet('{quote(paths['old_exact'])}')")
    require(tuple(helper._schema(con, "old_rows")) == EXACT_COLUMNS, "old exact column contract differs")
    unique_ids(con, "old_rows")
    require(number(con, "SELECT count(*) FROM old_rows") == expected_counts["old_exact_rows"], "old exact count differs")
    full_equal(con, "old_rows", "buy_all", EXACT_COLUMNS, subset=True)
    progress("old_exact_full_payload_subset_verified")
    derive(con, "buy_all", "all_rows")
    con.execute("CREATE TEMP VIEW filtered_rows AS SELECT * FROM all_rows WHERE price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman")
    derive(con, "old_rows", "old_all_rows")
    con.execute("CREATE TEMP VIEW old_filtered_rows AS SELECT * FROM old_all_rows WHERE price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman")
    all_columns = tuple(helper._schema(con, "all_rows"))
    full_equal(con, "old_all_rows", "all_rows", all_columns, subset=True)
    full_equal(con, "old_filtered_rows", "filtered_rows", all_columns, subset=True)
    full_equal(con, "filtered_rows", "all_rows", all_columns, subset=True)
    counts = {"raw_candidate_rows": raw_rows, "distinct_candidate_fills": fills,
        "duplicate_payload_rows": replays, "prefilter_exact_rows": fills,
        "source_blocks": blocks, "matched_blocks": matched, "missing_blocks": missing,
        "old_exact_rows": number(con, "SELECT count(*) FROM old_rows"),
        "old_accepted_all_rows": number(con, "SELECT count(*) FROM old_all_rows"),
        "old_accepted_filtered_rows": number(con, "SELECT count(*) FROM old_filtered_rows"),
        "new_accepted_all_rows": number(con, "SELECT count(*) FROM all_rows"),
        "new_accepted_filtered_rows": number(con, "SELECT count(*) FROM filtered_rows"),
        "outside_accepted_market_rows": number(con, "SELECT count(*) FROM buy_all ANTI JOIN metadata USING(market_id)"),
        "post_end_rows": number(con, "SELECT count(*) FROM buy_all e JOIN metadata m USING(market_id) WHERE e.timestamp>epoch(m.actual_end_utc)"),
        "invalid_sample_price_rows": number(con, "SELECT count(*) FROM buy_all e JOIN metadata m USING(market_id) WHERE e.timestamp<=epoch(m.actual_end_utc) AND NOT(e.price>0 AND e.price<1)"),
        "accepted_valid_price_rows": number(con, "SELECT count(*) FROM all_rows"),
        "filtered_extreme_price_exclusions": number(con, "SELECT count(*) FROM all_rows WHERE NOT(price>0.01 AND price<0.99)"),
        "filtered_flagged_interior_exclusions": number(con, "SELECT count(*) FROM all_rows WHERE price>0.01 AND price<0.99 AND buyer_is_flagged_nonhuman"),
        "accepted_missing_flag_rows": number(con, "SELECT count(*) FROM all_rows ANTI JOIN current_flags USING(proxyWallet)"),
        "accepted_null_flag_rows": number(con, "SELECT count(*) FROM all_rows JOIN current_flags USING(proxyWallet) WHERE is_nonhuman IS NULL"),
    }
    for name, expected in expected_counts.items():
        require(counts[name] == expected, "old-loader/frozen count differs: " + name)
    for sample in ("all", "filtered"):
        counts[f"restored_{sample}_rows"] = counts[f"new_accepted_{sample}_rows"] - counts[f"old_accepted_{sample}_rows"]
        require(counts[f"restored_{sample}_rows"] >= 0, "restored count negative")
    counts["candidate_markets"] = number(con, "SELECT count(*) FROM candidate_markets")
    counts["accepted_metadata_markets"] = number(con, "SELECT count(*) FROM metadata")
    counts["accepted_metadata_events"] = number(con, "SELECT count(DISTINCT game_pk) FROM metadata")
    for name, relation in (("new_all", "all_rows"), ("new_filtered", "filtered_rows"),
                           ("old_all", "old_all_rows"), ("old_filtered", "old_filtered_rows")):
        counts[name + "_markets"] = number(con, f"SELECT count(DISTINCT market_id) FROM {relation}")
        counts[name + "_events"] = number(con, f"SELECT count(DISTINCT game_pk) FROM {relation}")
    require(raw_rows == fills + replays and fills == sum(counts[name] for name in DEFINITIONS["exclusion_order"]), "prefilter exclusion law failed")
    require(counts["new_accepted_all_rows"] == sum(counts[name] for name in DEFINITIONS["filtered_exclusion_order"]), "filtered exclusion law failed")
    for relation in ("all_rows", "filtered_rows"):
        unique_ids(con, relation)
        require(number(con, f"SELECT count(*) FROM {relation} WHERE won IS NULL OR calibration_error IS NULL "
            "OR NOT isfinite(calibration_error) OR trade_day IS NULL OR realized_time IS NULL "
            "OR NOT isfinite(realized_time) OR realized_time>1 OR buyer_is_flagged_nonhuman IS NULL") == 0,
            "invalid enriched observations")
    progress("sample_counts_reconciled", old_all=counts["old_accepted_all_rows"], old_filtered=counts["old_accepted_filtered_rows"],
             new_all=counts["new_accepted_all_rows"], new_filtered=counts["new_accepted_filtered_rows"])
    return counts, timestamp_report


def write_json(path: Path, value: dict) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= CAPS["maximum_json_bytes"], "JSON artifact exceeds bound")
    with path.open("xb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())


def atomic_publish(staging: Path, target: Path) -> None:
    """Atomic no-replace directory publication on the production/test platforms."""
    library = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        result = library.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = library.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise RebuildBlocked("atomic no-replace rename unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def build_run(paths: dict[str, Path], target: Path, expected_head: str, reviewed: dict,
              *, expected_hashes: dict = FROZEN_HASHES, expected_counts: dict = FROZEN_COUNTS,
              command: list[str] | None = None) -> dict:
    """Small fixtures pass explicit hash/count dictionaries; CLI has no such overrides."""
    paths = {name: Path(path).resolve() for name, path in paths.items()}
    target = target.resolve()
    helper = load_helper()
    current = preflight(paths, target, expected_head, helper)
    for field in ("source", "caps", "inputs", "planned_read_bytes", "working_memory_estimate_bytes"):
        require(current[field] == reviewed[field], "reviewed preflight differs: " + field)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    started = time.monotonic()
    frozen = {}
    con = None
    try:
        frozen = freeze_inputs(paths, reviewed, expected_hashes)
        spill = staging / "spill"; spill.mkdir()
        con = duckdb.connect()
        environment = configure(con, spill)
        counts, timestamp_report = create_relations(con, paths, helper, expected_counts)
        outputs = {}
        for filename, relation in (("exact_trades.parquet", "buy_all"), ("all_trades.parquet", "all_rows"),
                                   ("filtered_trades.parquet", "filtered_rows")):
            require(shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "free disk floor breached")
            output = staging / filename
            con.execute(f"COPY (SELECT * FROM {relation}) TO '{quote(output)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            require(sum(path.stat().st_size for path in staging.glob("*.parquet")) <= CAPS["output_bytes"], "output cap exceeded")
            require(shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "free disk floor breached after output")
            con.execute(f"CREATE TEMP VIEW reopened_output AS SELECT * FROM read_parquet('{quote(output)}')")
            columns = tuple(helper._schema(con, relation))
            full_equal(con, relation, "reopened_output", columns)
            unique_ids(con, "reopened_output")
            timestamp_verification = helper.verify_output_timestamps(con, "reopened_output", "exact_cache")
            outputs[filename] = {"path": filename, "bytes": output.stat().st_size, "sha256": sha256(output),
                                 "rows": number(con, "SELECT count(*) FROM reopened_output"),
                                 "schema": [list(item) for item in helper._schema(con, "reopened_output").items()],
                                 "timestamp_verification": timestamp_verification}
            con.execute("DROP VIEW reopened_output")
            progress("output_reopened", artifact=filename, rows=outputs[filename]["rows"], bytes=outputs[filename]["bytes"])
        con.close(); con = None
        spill.rmdir()
        for name, path in paths.items():
            progress("hash_input_after", input=name, bytes=frozen[name]["bytes"])
            require(stat_identity(path) == frozen[name]["stat_before"] and sha256(path) == frozen[name]["sha256"],
                    "input changed before publication: " + name)
            frozen[name]["stat_after"] = stat_identity(path)
        require(read_head() == expected_head and source_files(helper) == reviewed["source"]["files"], "source changed during rebuild")
        summary = {"schema_version": 1, "status": "mlb_unfiltered_samples_complete", "data_certified": False,
            "scientific_estimators_rerun": False, "definitions": DEFINITIONS, "caps": dict(CAPS),
            "source": {**reviewed["source"], "head_before": expected_head, "head_after": read_head()},
            "command": command or [], "environment": environment, "preflight": current,
            "inputs": frozen, "outputs": outputs, "counts": counts,
            "timestamp_provenance_validation": timestamp_report,
            "reconciliation": {name: True for name in ("old_exact_full_payload_subset", "old_loader_counts_reproduced",
                "output_full_payload_reopened", "exact_timestamps_verified", "source_fill_ids_one_to_one",
                "metadata_unique", "lowercase_flag_keys_unique", "inputs_hashes_unchanged", "filtered_subset_of_all")},
            "resource_profile": {"wall_seconds": time.monotonic()-started},
            "limits": "Frozen-source analytic input repair only. Inferred opposite-side BUY semantics, frozen flag-label uncertainty, and resolution censoring remain. No native/action/wallet-flag repair or new FLB estimates."}
        write_json(staging / "summary.json", summary)
        manifest = {**summary, "summary_artifact": {"path": "summary.json", "bytes": (staging/"summary.json").stat().st_size,
                                                   "sha256": sha256(staging/"summary.json")}}
        write_json(staging / "manifest.json", manifest)
        require(sum(path.stat().st_size for path in staging.iterdir()) <= CAPS["output_bytes"], "complete stage output cap exceeded")
        require(shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "free disk floor breached before publication")
        atomic_publish(staging, target)
        return summary
    except Exception as error:
        if con is not None:
            con.close()
        if staging.exists():
            write_json(staging / "failure.json", {"schema_version": 1, "status": "blocked_mlb_rebuild",
                "data_certified": False, "error_class": type(error).__name__, "error": str(error)[:4096],
                "input_snapshot": frozen, "final_run_directory": str(target), "staging_directory": str(staging)})
            print(json.dumps({"status": "blocked_mlb_rebuild", "failure_evidence": str(staging/"failure.json")}), file=sys.stderr)
        raise


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in INPUT_NAMES:
        parser.add_argument("--" + name.replace("_", "-"), required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--reviewed-preflight")
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    if not args.preflight_only and not (args.reviewed_preflight and args.reviewed_preflight_sha256):
        parser.error("body run requires reviewed preflight path and SHA-256")
    return args


def main(argv=None):
    from production_guard import require_production_host
    require_production_host()
    args = parse_args(argv)
    helper = load_helper()
    require_committed_sources(helper)
    paths = {name: Path(getattr(args, name)).resolve() for name in INPUT_NAMES}
    target = Path(args.run_dir).resolve()
    if args.preflight_only:
        result = preflight(paths, target, args.expected_head, helper)
        result["command"] = process_command()
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
        write_json(staging / "summary.json", result)
        atomic_publish(staging, target)
    else:
        reviewed_path = Path(args.reviewed_preflight).resolve()
        require(reviewed_path.stat().st_size <= CAPS["maximum_json_bytes"], "reviewed preflight exceeds JSON bound")
        require(sha256(reviewed_path) == args.reviewed_preflight_sha256, "reviewed preflight SHA-256 differs")
        reviewed = json.loads(reviewed_path.read_text())
        require(reviewed["status"] == "mlb_rebuild_preflight_complete" and reviewed["data_certified"] is False,
                "reviewed preflight incomplete")
        result = build_run(paths, target, args.expected_head, reviewed, command=process_command())
    print(json.dumps({"status": result["status"], "run_dir": str(target), "data_certified": False}, sort_keys=True))


if __name__ == "__main__":
    main()
