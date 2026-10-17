#!/usr/bin/env python3
"""Guarded serial Kalshi input builds; candidate outputs need independent audit."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
import production_guard

RUN_ROOT = Path("/mnt/data/runs")
DEFAULT_ADMISSION_ROOT = RUN_ROOT / "2026-10-16_kalshi_source_admission_v2"
REMOTE_ROOT = "dropbox:Polymarket Data and Code/kalshi"
CAPS = {"memory_limit": "192GB", "threads": 4, "minimum_available_ram_bytes": 240_000_000_000,
        "output_bytes": 12_000_000_000, "transient_bytes": 4_000_000_000,
        "spill_bytes": 4_000_000_000, "minimum_free_bytes": 20_000_000_000,
        "json_bytes": 1_048_576, "stage_processing_reserve_bytes": 16_000_000_000}
EXPECTED_METADATA_FILES = 80
EXPECTED_METADATA_ROWS = 166_961_153
EXPECTED_METADATA_BYTES = 18_527_222_916
PINNED = {
    "trade_manifest": {"bytes": 228872, "sha256": "37aae4a94b1125cea81eb2222caba0daaa089eb9c241a0e6dd90bb0144fdf0ee"},
    "metadata_manifest": {"bytes": 11046, "sha256": "0ad2a1c6167f5171a5323e3bee021a27ae58516ee68ca23aab64d10ca5683ecf"},
    "events": {"bytes": 43666531, "sha256": "f535371e2b2c50946125e5ef0004aa418c1ad0fe1dea186c90768717a0480c47"},
    "series": {"bytes": 2277714, "sha256": "270c6de4d1a9d7d05820af126e631694ccb4f64e7dcb5d4e25ce976a5614960d"},
    "collections": {"bytes": 262933, "sha256": "69fbcd18676aad55394235da8408044c61cd70ec0926c63af7f1185a82b04ac5"},
}
SPORTS_WINNER_SERIES = {"KXMLBGAME": "mlb", "KXNFLGAME": "nfl", "KXNBAGAME": "nba", "KXNHLGAME": "nhl",
                        "KXNCAAMBGAME": "cbb", "KXATPMATCH": "atp", "KXEPLGAME": "epl",
                        "KXNCAAFGAME": "cfb", "KXWNBAGAME": "wnba"}
COMPOUND_PREFIXES = ("KXMVE", "KXOSCARWINNERS", "KXCITIESWEATHER")
POLY_SOURCE = ("analysis/kaushik_polymarket_replication/estimators.py",
               "analysis/kaushik_polymarket_replication/run_estimates.py",
               "analysis/kaushik_polymarket_replication/build_inputs.py",
               "analysis/kaushik_polymarket_replication/exchange_filter_contract.py",
               "production_guard.py", "docs/analysis_specs/kaushik_kalshi_replication_v1.md")


class PipelineBlocked(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise PipelineBlocked(reason)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def _identity(path):
    value = Path(path).lstat()
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(path, expected_sha256=None):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular JSON source required")
    before = _identity(path)
    require(0 < before["bytes"] <= CAPS["json_bytes"], "bounded JSON source required")
    raw = path.read_bytes()
    require(_identity(path) == before, "JSON source changed while reading")
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha256 is not None:
        require(re.fullmatch(r"[0-9a-f]{64}", expected_sha256 or "") and digest == expected_sha256,
                "JSON source hash differs")
    value = json.loads(raw, object_pairs_hook=_unique,
                       parse_constant=lambda value: (_ for _ in ()).throw(PipelineBlocked("nonfinite JSON value")))
    require(type(value) is dict, "JSON object required")
    return value, {"path": str(path), "bytes": len(raw), "sha256": digest}


def _relative(root, value):
    require(isinstance(value, str) and value and "\\" not in value and not Path(value).is_absolute()
            and all(part not in ("", ".", "..") for part in value.split("/")), "normalized relative artifact path required")
    path = root / value
    require(path.resolve(strict=True) == path and path.is_relative_to(root), "artifact path escapes root or contains a symlink")
    return path


def bind_file(root, record, *, pinned=None):
    require(type(record) is dict and type(record.get("bytes")) is int and record["bytes"] > 0
            and re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", "")), "explicit source size/hash required")
    if pinned is not None:
        require(all(record.get(key) == value for key, value in pinned.items()), "pinned source record differs")
    path = _relative(Path(root), record.get("path"))
    before = _identity(path)
    require(path.is_file() and not path.is_symlink() and before["bytes"] == record["bytes"], "source artifact size/type differs")
    require(sha256(path) == record["sha256"] and _identity(path) == before, "source artifact hash/drift gate failed")
    return {**record, "absolute_path": str(path)}


def write_json(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= CAPS["json_bytes"], "output JSON exceeds admitted bound")
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"path": Path(path).name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "explicit committed source HEAD required")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    require(head == expected_head, "production source HEAD differs")
    paths = set(POLY_SOURCE)
    for pattern in ("analysis/kaushik_kalshi_replication/*.py", "tests/test_kaushik_kalshi_*.py", "scripts/*kaushik_kalshi*.py"):
        paths.update(str(path.relative_to(REPO)) for path in REPO.glob(pattern))
    paths = sorted(paths)
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", *paths], cwd=REPO, capture_output=True)
    clean = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", *paths], cwd=REPO)
    require(tracked.returncode == clean.returncode == 0, "production sources/tests must be tracked, committed and unchanged")
    require(all((REPO / path).is_file() and not (REPO / path).is_symlink() for path in paths), "regular committed source files required")
    return {"head": head, "files": {path: sha256(REPO / path) for path in paths}}


def available_ram_bytes():
    match = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    require(match is not None, "Linux MemAvailable unavailable")
    return int(match.group(1)) * 1024


def preflight(output_dir):
    target = Path(output_dir)
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "canonical production Python required")
    require(target.is_absolute() and target.resolve() == target and target.parent == RUN_ROOT,
            "new top-level canonical run directory required")
    require(not os.path.lexists(target), "immutable output directory already exists")
    reservation = sum(CAPS[name] for name in ("output_bytes", "transient_bytes", "spill_bytes", "minimum_free_bytes"))
    free = shutil.disk_usage(target.parent).free
    require(free >= reservation, "40GB output/transient/spill/free-floor reservation failed")
    require((os.cpu_count() or 0) >= CAPS["threads"], "four production threads unavailable")
    ram = available_ram_bytes()
    require(ram >= CAPS["minimum_available_ram_bytes"], "240GB available RAM required")
    require(all(value == resource.RLIM_INFINITY or value >= CAPS["transient_bytes"]
                for value in resource.getrlimit(resource.RLIMIT_FSIZE)), "inherited file ceiling below transient bound")
    return {"free_bytes": free, "available_ram_bytes": ram, "reservation_bytes": reservation, **CAPS}


def load_admission_plan(admission_root, plan_path=None):
    from analysis.kaushik_kalshi_replication import source_contract
    root = Path(admission_root)
    require(root.is_absolute() and root.resolve(strict=True) == root and root.is_dir(), "canonical existing admission root required")
    require(not (root / "failure.json").exists(), "source admission has a failure marker")
    plan_path = Path(plan_path) if plan_path is not None else root / "admission_plan.json"
    require(plan_path.is_relative_to(root) and plan_path.resolve(strict=True) == plan_path, "admission plan must remain inside its root")
    plan, plan_binding = read_json(plan_path)
    require(plan.get("schema_version") == "kaushik_kalshi_admission_plan_v1", "source admission plan version differs")
    require(plan.get("sports_winner_series") == SPORTS_WINNER_SERIES, "explicit nine-sport winner-series map differs")
    records = {key: bind_file(root, plan.get(key), pinned=PINNED[key]) for key in ("trade_manifest", "metadata_manifest")}
    references = plan.get("references", {})
    require(type(references) is dict and set(references) == {"events", "series", "collections"}, "exact admitted native reference inventory required")
    records["references"] = {key: bind_file(root, value, pinned=PINNED[key]) for key, value in references.items()}
    trades = source_contract.parse_trade_manifest(Path(records["trade_manifest"]["absolute_path"]).read_bytes(), expected_sha256=PINNED["trade_manifest"]["sha256"])
    trade_inventory = source_contract.validate_trade_days(trades)
    metadata_entries, metadata_counts = source_contract.parse_metadata_manifest(Path(records["metadata_manifest"]["absolute_path"]).read_bytes(), expected_sha256=PINNED["metadata_manifest"]["sha256"])
    require(len(metadata_entries) == EXPECTED_METADATA_FILES and sum(metadata_counts.values()) == EXPECTED_METADATA_ROWS
            and sum(entry.size_bytes for entry in metadata_entries) == EXPECTED_METADATA_BYTES, "full pinned metadata inventory differs")
    require(max(entry.size_bytes for entry in (*trades, *metadata_entries)) + 1 + 2_000_000_000 <= CAPS["transient_bytes"],
            "staged shard plus output artifact exceeds combined transient ceiling")
    return {"root": str(root), "plan": plan_binding, "records": records,
            "trade_entries": trades, "trade_inventory": trade_inventory,
            "metadata_entries": metadata_entries, "metadata_counts": metadata_counts,
            "sports_winner_series": dict(SPORTS_WINNER_SERIES)}


def _connection(target):
    import duckdb
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='192GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=true")
    con.execute("SET enable_object_cache=false")
    con.execute("SET temp_directory='" + str(target / "spill").replace("'", "''") + "'")
    con.execute("SET max_temp_directory_size='4000000000B'")
    con.execute("SET disabled_optimizers='join_order,build_side_probe_side'")
    return con


def _parquet_view(con, name, paths):
    from analysis.kaushik_polymarket_replication import build_inputs as io
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), "simple relation name required")
    require(bool(paths), "nonempty explicit Parquet path inventory required")
    values = ",".join(io.literal(path) for path in paths)
    con.execute(f"CREATE TEMP VIEW {io.ident(name)} AS SELECT * FROM read_parquet([{values}],hive_partitioning=false)")


def _limits():
    from analysis.kaushik_kalshi_replication.build import BuildLimits
    return BuildLimits(output_bytes=CAPS["output_bytes"], transient_bytes=CAPS["transient_bytes"],
                       spill_bytes=CAPS["spill_bytes"], minimum_free_bytes=CAPS["minimum_free_bytes"])


def _rebased(record, prefix):
    return {**record, "path": str(Path(prefix) / record["path"])}


def _json_record(target, path, value):
    record = write_json(path, value)
    return {**record, "path": str(Path(path).relative_to(target))}


def _inventory_record(target, admission, kind):
    entries = admission[kind + "_entries"]
    value = {"schema_version": "kaushik_kalshi_source_inventory_v1", "kind": kind,
             "source_admission_root": admission["root"], "admission_plan": admission["plan"],
             "source_records": admission["records"], "entries": [asdict(entry) for entry in entries],
             "trade_inventory": admission["trade_inventory"], "sports_winner_series": admission["sports_winner_series"]}
    return _json_record(target, target / "input_inventory.json", value)


def _receipt_index(target, paths, name):
    records = [{"path": str(path.relative_to(target)), "bytes": path.stat().st_size, "sha256": sha256(path)} for path in paths]
    return _json_record(target, target / name, {"schema_version": "kaushik_kalshi_receipt_inventory_v1", "receipts": records})


def _source_policies():
    from analysis.kaushik_kalshi_replication.metadata import SourceVersionPolicy
    return {source: SourceVersionPolicy(payout_rule="standard_binary_venue_contract", notional_value_unit="dollars",
        payout_evidence_rule="https://docs.kalshi.com/getting_started/market_settlement; pinned metadata README and inspected source-version evidence",
        allow_missing_or_zero_notional_under_venue_rule=True, settlement_ts_encoding="ISO8601_UTC",
        settlement_ts_evidence_rule="pinned markets_meta_all_v2 source-version ISO8601_UTC encoding inspection",
        settlement_value_unit=None) for source in ("bulk_v2", "forward", "event_v1")}


def build_metadata(con, target, admission, source):
    import pyarrow.parquet as pq
    from analysis.kaushik_kalshi_replication import build, metadata, staging
    from analysis.kaushik_polymarket_replication import build_inputs as io
    artifacts_dir = target / "artifacts"
    artifacts_dir.mkdir()
    for child in ("metadata", "sports", "exclusions", "health"):
        (artifacts_dir / child).mkdir()
    for key, item in admission["records"]["references"].items():
        _parquet_view(con, "native_" + key, [item["absolute_path"]])
    refs = metadata.NativeReferences("native_events", "https://docs.kalshi.com/api-reference/events/get-events; pinned native event membership",
            "native_series", "pinned native series audit only", "native_collections", "pinned native collections audit only")
    con.execute("CREATE TEMP TABLE metadata_all_keys(ticker VARCHAR,source_version VARCHAR,status VARCHAR,result VARCHAR,source_shard VARCHAR,known_compound_family VARCHAR)")
    con.execute("CREATE TEMP TABLE winner_series(series_ticker VARCHAR,sport VARCHAR)")
    con.executemany("INSERT INTO winner_series VALUES (?,?)", list(SPORTS_WINNER_SERIES.items()))
    policies = _source_policies()
    writer = build._Writer(artifacts_dir, _limits())
    artifacts, health_records, source_rows = {}, [], 0

    def process(path, entry):
        nonlocal source_rows
        shard = Path(entry.path).stem
        expected_rows = admission["metadata_counts"][entry.path]
        require(pq.ParquetFile(path).metadata.num_rows == expected_rows, "metadata footer/source-manifest row count differs")
        _parquet_view(con, "metadata_source", [path])
        inspection = metadata.inspect_metadata_shard(con, "metadata_source")
        result = metadata.project_metadata_shard(con, "metadata_source", source_shard=entry.path,
            source_version_policy=policies, native_references=refs, output_prefix="projected")
        con.execute("INSERT INTO metadata_all_keys SELECT * FROM " + io.ident(result["raw_keys_relation"]))
        source_rows += result["key_health"]["rows"]
        info = {"source_entry": asdict(entry), "footer_rows": expected_rows, "inspection": inspection,
                "key_health": result["key_health"], "reference_health": result["reference_health"]}
        text_fields = [f"m.{io.ident(name)} {io.ident(name)}" if name in inspection["source_schema"]
                       else f"NULL::VARCHAR {io.ident(name)}"
                       for name in ("title", "question", "yes_sub_title", "no_sub_title", "subtitle", "contract_subtitle")]
        for label, query in {
            "metadata": "SELECT * FROM " + io.ident(result["projection_relation"]),
            "sports": "SELECT p.*,w.sport," + ",".join(text_fields) + " FROM " + io.ident(result["projection_relation"]) +
                      " p JOIN winner_series w USING(series_ticker) JOIN metadata_source m ON p.ticker=m.ticker WHERE p.combo_state='single'",
            "exclusions": "SELECT * FROM " + io.ident(result["exclusions_relation"]),
        }.items():
            item = writer.parquet(con, artifacts_dir / label / (shard + ".parquet"), query)
            artifacts[label + "_" + shard] = _rebased(item, "artifacts")
        health_path = artifacts_dir / "health" / (shard + ".json")
        writer.json(health_path, info)
        health_records.append({"path": str(health_path.relative_to(target)), "bytes": health_path.stat().st_size, "sha256": sha256(health_path)})
        metadata.drop_projection_views(con, result)
        con.execute("DROP VIEW metadata_source")
        return {"metadata_source_rows": expected_rows, "projected_artifacts": [key for key in artifacts if key.endswith("_" + shard)],
                "health": health_records[-1], "global_uniqueness": "pending all pinned shards"}

    receipts = staging.stage_and_process(admission["metadata_entries"], remote_root=REMOTE_ROOT, run_dir=target,
        output_spill_reserve_bytes=CAPS["stage_processing_reserve_bytes"], process=process)
    proof = metadata.validate_global_metadata_keys(con, "metadata_all_keys", expected_source_shards=[entry.path for entry in admission["metadata_entries"]])
    require(source_rows == proof["rows"] == EXPECTED_METADATA_ROWS, "all-shard metadata row conservation differs")
    proof_record = _json_record(target, target / "global_metadata_proof.json", {
        "schema_version": "kaushik_kalshi_global_metadata_proof_v1", "status": "structural_global_metadata_keys_passed", "health": proof,
        "source_manifest_sha256": admission["records"]["metadata_manifest"]["sha256"], "source": source})
    con.execute("DROP TABLE metadata_all_keys")
    return {"schema_version": "kaushik_kalshi_metadata_stage_v1", "status": "metadata_complete_pending_independent_audit",
            "source": source, "counts": {"source_rows": source_rows, "source_shards": len(receipts)},
            "input_inventory": _inventory_record(target, admission, "metadata"), "global_metadata_proof": proof_record,
            "staging_receipts": _receipt_index(target, receipts, "staging_receipts.json"), "artifacts": artifacts,
            "health_records": health_records, "source_version_policy": {key: asdict(value) for key, value in policies.items()},
            "sports": {"status": "unbound_native_winner_candidates", "winner_series": SPORTS_WINNER_SERIES,
                       "independent_provider_game_result_start_end_binding": "pending"}, "caps": CAPS}


def admit_metadata_stage(metadata_dir, manifest_sha256, admission):
    import pyarrow.parquet as pq
    root = Path(metadata_dir)
    require(root.is_absolute() and root.resolve(strict=True) == root and root.is_dir(), "existing canonical metadata stage required")
    require(not (root / "failure.json").exists(), "metadata stage contains failure marker")
    manifest, binding = read_json(root / "candidate_manifest.json", manifest_sha256)
    require(manifest.get("schema_version") == "kaushik_kalshi_metadata_stage_v1"
            and manifest.get("status") == "metadata_complete_pending_independent_audit", "candidate metadata stage contract differs")
    proof_record = bind_file(root, manifest.get("global_metadata_proof"))
    proof, _ = read_json(proof_record["absolute_path"], proof_record["sha256"])
    health = proof.get("health", {})
    expected_shards = {entry.path for entry in admission["metadata_entries"]}
    require(proof.get("status") == "structural_global_metadata_keys_passed"
            and proof.get("source_manifest_sha256") == admission["records"]["metadata_manifest"]["sha256"]
            and health.get("rows") == EXPECTED_METADATA_ROWS and health.get("duplicate_ticker_keys") == 0
            and health.get("invalid_ticker_rows") == 0 and health.get("ticker_groups") == health.get("nonnull_ticker_groups") == EXPECTED_METADATA_ROWS
            and health.get("conflicting_result_keys") == health.get("conflicting_status_keys") == 0
            and set(health.get("observed_source_shards", [])) == set(health.get("expected_source_shards", [])) == expected_shards
            and manifest.get("counts") == {"source_rows": EXPECTED_METADATA_ROWS, "source_shards": EXPECTED_METADATA_FILES}
            and proof.get("source") == manifest.get("source"),
            "metadata structural/global proof differs")
    expected_roles = {label + "_" + Path(entry.path).stem: str(Path("artifacts") / label / Path(entry.path).name)
                      for label in ("metadata", "sports", "exclusions") for entry in admission["metadata_entries"]}
    require(type(manifest.get("artifacts")) is dict and set(manifest["artifacts"]) == set(expected_roles),
            "exact metadata artifact-role inventory differs")
    admitted = {}
    for key, record in manifest.get("artifacts", {}).items():
        require(record.get("path") == expected_roles[key], "metadata artifact role/path differs")
        item = bind_file(root, record)
        parquet = pq.ParquetFile(item["absolute_path"])
        require(parquet.metadata.num_rows == item.get("rows") and str(parquet.schema_arrow) == item.get("schema"), "metadata artifact footer drift")
        admitted[key] = item
    for label in ("metadata", "sports", "exclusions"):
        require(len([key for key in admitted if key.startswith(label + "_")]) == EXPECTED_METADATA_FILES, "metadata artifact-role inventory differs")
    return manifest, {"manifest": binding, "proof": proof_record}, admitted


def build_trades(con, target, admission, source, *, metadata_dir, metadata_manifest_sha256):
    import pyarrow.parquet as pq
    from analysis.kaushik_kalshi_replication import build, staging
    metadata_manifest, metadata_binding, admitted = admit_metadata_stage(metadata_dir, metadata_manifest_sha256, admission)
    _parquet_view(con, "prepared_metadata_source", [item["absolute_path"] for key, item in admitted.items() if key.startswith("metadata_")])
    _parquet_view(con, "sports_metadata", [item["absolute_path"] for key, item in admitted.items() if key.startswith("sports_")])
    con.execute("CREATE TEMP TABLE prepared_metadata AS SELECT ticker,event_cluster,category,market_reason,opening_seconds,endpoint_seconds,result FROM prepared_metadata_source")
    con.execute("DROP VIEW prepared_metadata_source")
    con.execute("CREATE TEMP TABLE sports_winner_candidates AS SELECT ticker,sport FROM sports_metadata")
    con.execute("DROP VIEW sports_metadata")
    prepared_rows, unique_tickers = con.execute("SELECT count(*),count(DISTINCT ticker) FROM prepared_metadata").fetchone()
    expected_prepared = sum(item["rows"] for key, item in admitted.items() if key.startswith("metadata_"))
    require(prepared_rows == unique_tickers == expected_prepared, "compact metadata cache uniqueness/row conservation failed")
    sports_rows, unique_sports = con.execute("SELECT count(*),count(DISTINCT ticker) FROM sports_winner_candidates").fetchone()
    require(sports_rows == unique_sports, "winner candidate cache ticker uniqueness failed")
    memory, spill = con.execute("SELECT coalesce(sum(memory_usage_bytes),0),coalesce(sum(temporary_storage_bytes),0) FROM duckdb_memory()").fetchone()
    require(memory <= 192_000_000_000 and spill <= CAPS["spill_bytes"], "prepared input cache exceeds shared memory/spill caps")
    cache = {"prepared_metadata_rows": prepared_rows, "sports_winner_candidate_rows": sports_rows,
             "prepared_metadata_columns": ["ticker", "event_cluster", "category", "market_reason", "opening_seconds", "endpoint_seconds", "result"],
             "duckdb_memory_bytes_before_native_ids": memory, "duckdb_spill_bytes_before_native_ids": spill,
             "available_ram_bytes_before_native_ids": available_ram_bytes(),
             "policy": "one compact DuckDB TEMP TABLE cache; same 192GB shared cap, not an extra allocation allowance"}
    cells = target / "cells"
    cells.mkdir()
    build.initialize_ids(con)
    day_receipts, receipt_paths = [], []

    def process(path, entry):
        day = entry.path.split("dt=", 1)[1].split("/", 1)[0]
        rows = pq.ParquetFile(path).metadata.num_rows
        receipt = build.process_day(con, path, day, cells, expected_rows=rows, metadata_relation="prepared_metadata",
            sports_candidates_relation="sports_winner_candidates", known_compound_prefixes=COMPOUND_PREFIXES, limits=_limits())
        day_receipts.append(receipt)
        receipt_path = cells / ("day=" + day) / "receipt.json"
        receipt_paths.append(receipt_path)
        return {"day_receipt": {"path": str(receipt_path.relative_to(target)), "bytes": receipt_path.stat().st_size,
                                "sha256": sha256(receipt_path)}, "source_rows": rows,
                "global_native_id_uniqueness": "pending complete archive"}

    staging_receipts = staging.stage_and_process(admission["trade_entries"], remote_root=REMOTE_ROOT, run_dir=target,
        output_spill_reserve_bytes=CAPS["stage_processing_reserve_bytes"], process=process)
    source_rows = sum(item["source_rows"] for item in day_receipts)
    con.execute("DROP TABLE prepared_metadata")
    con.execute("DROP TABLE sports_winner_candidates")
    cache["released_before_global_id_proof"] = True
    final = build.finalize_cells(con, cells, day_receipts, expected_source_rows=source_rows, limits=_limits())
    con.execute("DROP TABLE native_ids")
    require(admit_metadata_stage(metadata_dir, metadata_manifest_sha256, admission)[1] == metadata_binding,
            "metadata input binding drift before candidate publication")
    counts = {"source_rows": source_rows, "baseline_rows": final["baseline_rows"], "duration_tail_rows": final["duration_rows"],
              "excluded_rows": final["excluded_rows"], "source_days": len(day_receipts)}
    minima = [item["execution_seconds_min"] for item in day_receipts if item["execution_seconds_min"] is not None]
    maxima = [item["execution_seconds_max"] for item in day_receipts if item["execution_seconds_max"] is not None]
    return {"schema_version": "kaushik_kalshi_inputs_v1", "status": "inputs_complete_pending_independent_audit",
        "source": source, "scientific_contract": {"sample_scope": "all_available_dates", "combos": "excluded",
            "duration_endpoint": "settlement_ts", "equal_print_weight": True, "cutoff_utc": None, "cutoff_seconds": None,
            "source_first_day": admission["trade_inventory"]["first_day"], "source_last_day": admission["trade_inventory"]["last_day"],
            "known_compound_prefixes": list(COMPOUND_PREFIXES)}, "counts": counts,
        "sample_metadata": {"baseline_rows": counts["baseline_rows"], "duration_tail_rows": counts["duration_tail_rows"],
            "source_rows": source_rows, "execution_seconds_min": min(minima) if minima else None,
            "execution_seconds_max": max(maxima) if maxima else None},
        "identity": final["identity"], "input_cache": cache, "input_inventory": _inventory_record(target, admission, "trade"),
        "metadata_binding": metadata_binding, "metadata_independent_acceptance": "pending separate audit",
        "daily_receipts": _receipt_index(target, receipt_paths, "daily_receipts.json"),
        "staging_receipts": _receipt_index(target, staging_receipts, "staging_receipts.json"),
        "artifacts": {key: _rebased(record, "cells") for key, record in final["artifacts"].items()},
        "sports": {"status": "unbound_native_winner_trade_candidates", "candidate_rows": final["sports_candidate_rows"],
                   "independent_provider_game_result_start_end_binding": "pending"}, "caps": CAPS}


def _persistent_output_bytes(target):
    total = 0
    for path in target.rglob("*"):
        require(not path.is_symlink(), "owned output contains unexpected symlink")
        if path.is_file() and path.relative_to(target).parts[0] != "spill":
            total += path.stat().st_size
    require(total <= CAPS["output_bytes"], "combined persistent output ceiling exceeded")
    require(shutil.disk_usage(target).free >= CAPS["minimum_free_bytes"], "final free-space floor failed")
    return total


def run_production(args):
    production_guard.require_production_host()
    resources = preflight(args.output_dir)
    source = source_snapshot(args.expected_head)
    admission = load_admission_plan(args.admission_root, args.admission_plan)
    target = Path(args.output_dir)
    target.mkdir(mode=0o700)
    con = None
    try:
        con = _connection(target)
        if args.command == "metadata":
            result = build_metadata(con, target, admission, source)
        else:
            result = build_trades(con, target, admission, source, metadata_dir=args.metadata_dir,
                                  metadata_manifest_sha256=args.metadata_manifest_sha256)
        require(source_snapshot(args.expected_head) == source, "source drift before candidate publication")
        reopened_admission = load_admission_plan(args.admission_root, args.admission_plan)
        require(reopened_admission["records"] == admission["records"] and reopened_admission["plan"] == admission["plan"],
                "source admission drift before candidate publication")
        result["resources"] = resources
        result["independent_acceptance"] = "pending; this command does not produce acceptance.json"
        result["persistent_output_bytes_before_manifest"] = _persistent_output_bytes(target)
        write_json(target / "candidate_manifest.json", result)
        require(read_json(target / "candidate_manifest.json")[0] == result, "candidate manifest failed reopen")
        _persistent_output_bytes(target)
        return result
    except BaseException as error:
        if not (target / "failure.json").exists():
            try:
                write_json(target / "failure.json", {"status": "input_build_incomplete", "command": args.command,
                    "error_type": type(error).__name__, "reason": str(error)[:4096]})
            except OSError as marker_error:
                print("Failure marker could not be written: " + str(marker_error), file=sys.stderr)
        raise
    finally:
        if con is not None:
            con.close()


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for command in ("metadata", "trades"):
        sub = commands.add_parser(command)
        sub.add_argument("--expected-head", required=True)
        sub.add_argument("--output-dir", required=True, type=Path)
        sub.add_argument("--admission-root", type=Path, default=DEFAULT_ADMISSION_ROOT)
        sub.add_argument("--admission-plan", type=Path)
        if command == "trades":
            sub.add_argument("--metadata-dir", type=Path, required=True)
            sub.add_argument("--metadata-manifest-sha256", required=True)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    run_production(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
