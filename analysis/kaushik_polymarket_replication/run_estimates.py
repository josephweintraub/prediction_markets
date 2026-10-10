"""Guarded immutable estimator stages; numerical fixtures never open real data.

Production consumes the independently accepted compact archive base.  Duration
estimation caches lossless event/nuisance-cell sufficient statistics: iteration
operates on bounded group-mean batches while within-cell cross-products restore
the exact record-weighted moments and event scores.  No full trade pandas frame,
rounded price, new API collection, flags, or additional model grid is used.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Mapping

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from . import build_inputs as inputs
from . import estimators as engine

REPO = Path(__file__).resolve().parents[2]
SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
TARGETS = ("payoff_cents", "roi_percent")
CAPS = {"memory_limit": "64GB", "threads": 4, "spill_bytes": 16_000_000_000,
        "numpy_memory_bytes": 32_000_000_000, "total_memory_bytes": 96_000_000_000,
        "minimum_available_ram_bytes": 110_000_000_000,
        "minimum_free_bytes": 20_000_000_000, "maximum_output_bytes": 8_000_000_000,
        "maximum_cache_bytes": 3_000_000_000, "maximum_read_bytes": 8_000_000_000_000,
        "maximum_transient_file_bytes": 16_000_000_000,
        "maximum_json_bytes": 16_000_000, "batch_rows": 65_536,
        "maximum_projection_iterations": 100, "projection_tolerance": 1e-10}
SOURCE_FILES = ("analysis/kaushik_polymarket_replication/run_estimates.py",
                "analysis/kaushik_polymarket_replication/estimators.py",
                "analysis/kaushik_polymarket_replication/build_inputs.py",
                "scripts/estimate_kaushik_polymarket_replication.py",
                "tests/test_kaushik_replication_driver.py",
                "docs/analysis_specs/kaushik_polymarket_replication_v1.md")
WINDOWS = (
    ("pregame", "pre_lt24h", "<−24h", "u < -86400"),
    ("pregame", "pre_24to6h", "−24 to −6h", "u >= -86400 AND u < -21600"),
    ("pregame", "pre_6to1h", "−6 to −1h", "u >= -21600 AND u < -3600"),
    ("pregame", "pre_60to15m", "−60 to −15m", "u >= -3600 AND u < -900"),
    ("pregame", "pre_15to0m", "−15 to 0m", "u >= -900 AND u < 0"),
    ("since_start", "live_0to15m", "0–15m", "u >= 0 AND u < 900"),
    ("since_start", "live_15to30m", "15–30m", "u >= 900 AND u < 1800"),
    ("since_start", "live_30to60m", "30–60m", "u >= 1800 AND u < 3600"),
    ("since_start", "live_1to2h", "1–2h", "u >= 3600 AND u < 7200"),
    ("since_start", "live_2hplus", "2h+", "u >= 7200"),
    ("final_hour", "final_60to30m", "60–30m", "u >= 0 AND r > 1800 AND r <= 3600"),
    ("final_hour", "final_30to15m", "30–15m", "u >= 0 AND r > 900 AND r <= 1800"),
    ("final_hour", "final_15to5m", "15–5m", "u >= 0 AND r > 300 AND r <= 900"),
    ("final_hour", "final_5to0m", "5–0m", "u >= 0 AND r >= 0 AND r <= 300"),
)


def require(condition, message):
    if not condition:
        raise inputs.InputBlocked(message)


def rows(con, query):
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def utc(seconds):
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat() if seconds is not None else None


@dataclass
class ReadLedger:
    ceiling: int = CAPS["maximum_read_bytes"]
    charged_bytes: int = 0
    scans: dict[str, dict[str, int]] = field(default_factory=dict)

    def charge(self, size, label):
        require(isinstance(size, int) and size >= 0, "invalid charged read size")
        require(self.charged_bytes + size <= self.ceiling, "bounded read-pass budget exceeded before " + label)
        self.charged_bytes += size
        value = self.scans.setdefault(label, {"passes": 0, "charged_bytes": 0})
        value["passes"] += 1
        value["charged_bytes"] += size

    def to_dict(self):
        return {"maximum_read_bytes": self.ceiling, "charged_bytes": self.charged_bytes,
                "method": "conservative full-file bytes charged for every declared scan/hash",
                "scans": self.scans}


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "committed 40-character estimator HEAD required")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    require(head == expected_head, "estimator source HEAD differs")
    result = {}
    for name in SOURCE_FILES:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        clean = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=REPO)
        require(tracked.returncode == clean.returncode == 0, "estimator source uncommitted or changed: " + name)
        result[name] = inputs.sha256(REPO / name)
    return {"head": head, "files": result}


def parquet_footer(path):
    before = inputs.stat_identity(path)
    parquet = pq.ParquetFile(path)
    with Path(path).open("rb") as stream:
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        require(trailer[4:] == b"PAR1", "invalid Parquet trailer")
        size = int.from_bytes(trailer[:4], "little")
        stream.seek(-8 - size, os.SEEK_END)
        digest = hashlib.sha256(stream.read(size) + trailer).hexdigest()
    require(before == inputs.stat_identity(path), "Parquet footer stat drift")
    return {"path": str(Path(path).resolve()), "stat": before, "rows": parquet.metadata.num_rows,
            "schema": str(parquet.schema_arrow), "footer_sha256": digest}


def load_base(stage, manifest_sha256, acceptance_sha256):
    stage = Path(stage).resolve()
    manifest, identity = inputs.read_json(stage / "manifest.json", manifest_sha256)
    acceptance, receipt = inputs.read_json(stage / "acceptance.json", acceptance_sha256)
    require(manifest.get("schema_version") == "kaushik_replication_inputs_v1" and
            manifest.get("status") == "inputs_complete", "accepted input manifest required")
    require(acceptance.get("status") == "inputs_reopened_accepted" and
            acceptance.get("manifest_sha256") == identity["sha256"] and
            acceptance.get("output_schema_hash_count_support_reopened") is True,
            "independent reopened-input acceptance receipt required")
    require(acceptance.get("source_head") == manifest["source"]["head"] and
            acceptance.get("rows") == manifest["rows"], "acceptance row/source binding differs")
    require(all(manifest["reconciliation"].get(key) is True for key in (
        "all_records_accounted", "archive_multiplicity_preserved", "no_join_fanout",
        "unique_reciprocal_binary_complements", "outputs_reopened")), "input reconciliation incomplete")
    require(manifest["definitions"]["cutoff_utc"] == inputs.CUTOFF_ISO, "cutoff contract differs")
    inventory = []
    for relative, expected in sorted(manifest["outputs"].items()):
        path = (stage / relative).resolve()
        require(stage in path.parents and not Path(relative).is_absolute(), "input output escapes accepted stage")
        item = parquet_footer(path)
        require(item["stat"]["bytes"] == expected["bytes"] and item["rows"] == expected["rows"] and
                item["schema"] == expected["schema"], "accepted output footer/schema/count drift")
        inventory.append({**item, "expected_sha256": expected["sha256"], "relative_path": relative})
    monthly = [item["path"] for item in inventory if re.fullmatch(r"year_month=\d{4}-\d{2}/base\.parquet", item["relative_path"])]
    require(monthly and set(monthly) == {str(path.resolve()) for path in stage.glob("year_month=*/base.parquet")},
            "monthly Parquet body inventory differs from accepted manifest")
    return manifest, {"manifest": identity, "acceptance": receipt}, inventory, monthly


def sports_input_inventory(binding):
    """A flat explicitly bound inventory, including retained provider proofs."""
    require(binding.get("schema_version") == "kaushik_replication_sports_binding_v1" and
            binding.get("sports") == list(SPORTS), "nine-sport binding roster/version differs")
    require(binding.get("provider_cohort_admitted") is True and
            isinstance(binding.get("coverage_qualification"), str) and binding["coverage_qualification"],
            "provider-covered cohort adaptation/proof not admitted")
    inventory = []
    for item in binding.get("inputs", []):
        require(re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")), "sports digest absent")
        path = Path(item["path"])
        if path.suffix == ".parquet":
            footer = parquet_footer(path)
        else:
            _, identity = inputs.read_json(path, item["sha256"])
            footer = {"path": identity["path"], "stat": identity["stat"], "sha256": identity["sha256"]}
        inventory.append({**footer, "role": item["role"], "expected_sha256": item["sha256"]})
    roles = [item["role"] for item in inventory]
    require(len(roles) == len(set(roles)), "duplicate sports input role")
    require("sports_market_map" in roles or {"six_candidates", "six_timing", "six_proof", "six_tokens", "nfl_moneylines", "nba_moneylines", "mlb_phase"} <= set(roles),
            "normalized map or all original sports metadata roles required")
    require(binding.get("retained_identity_result_timing_proof") is True, "provider identity/result/timing proof not admitted")
    return inventory


def reviewed_sports_metadata(binding, base_binding):
    review = binding.get("reviewed_metadata_stage", {})
    require(review.get("reviewed") is True, "separately reviewed sports metadata stage required before estimator trade scan")
    manifest, identity = inputs.read_json(review["manifest_path"], review["manifest_sha256"])
    receipt, receipt_id = inputs.read_json(review["acceptance_path"], review["acceptance_sha256"])
    require(manifest.get("schema_version") == "kaushik_replication_sports_metadata_v1" and
            manifest.get("status") == "sports_metadata_complete" and
            receipt.get("schema_version") == "kaushik_replication_sports_metadata_acceptance_v1" and
            receipt.get("status") == "sports_metadata_reopened_accepted"
            and receipt.get("manifest_sha256") == identity["sha256"] and receipt.get("all_outputs_reopened") is True,
            "sports metadata stage has not reopened accepted outputs")
    require(manifest.get("trade_bodies_read") is False and manifest.get("native_pair_and_provider_result_proof") is True
            and receipt.get("source_head") == manifest["source"]["head"], "sports metadata proof/source receipt incomplete")
    require(manifest.get("base_binding") == base_binding and
            manifest.get("provider_inputs") == binding["inputs"] and
            manifest.get("coverage_qualification") == binding["coverage_qualification"], "reviewed sports metadata input/cohort drift")
    parent = Path(identity["path"]).parent
    require(set(manifest["outputs"]) == {"sports_market_map.parquet", "sports_metadata_exclusions.parquet"},
            "reviewed sports metadata output inventory differs")
    files = []
    for relative, output in manifest["outputs"].items():
        item = parquet_footer(parent / relative)
        require(item["rows"] == output["rows"] and item["schema"] == output["schema"] and item["stat"]["bytes"] == output["bytes"], "reviewed sports map footer drift")
        files.append({**item, "role": "reviewed_" + Path(relative).stem, "expected_sha256": output["sha256"]})
    return {"manifest": identity, "acceptance": receipt_id, "source": manifest["source"]}, files


def preflight(base_dir, base_manifest_sha256, base_acceptance_sha256,
              sports_binding_path, sports_binding_sha256, target, expected_head,
              *, sports_metadata_only=False):
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    source = source_snapshot(expected_head)
    manifest, base_binding, base_files, monthly = load_base(base_dir, base_manifest_sha256, base_acceptance_sha256)
    sports, sports_id = inputs.read_json(sports_binding_path, sports_binding_sha256)
    sports_files = sports_input_inventory(sports)
    require({"six_candidates", "six_timing", "six_proof", "six_tokens", "nfl_moneylines", "nba_moneylines", "mlb_phase"} <= {item["role"] for item in sports_files},
            "production requires frozen original sports metadata/proof roles")
    sports_review = None
    if not sports_metadata_only:
        sports_review, reviewed_files = reviewed_sports_metadata(sports, base_binding)
        require(sports_review["source"]["files"] == source["files"], "sports metadata source files differ from estimator source")
        sports_files += reviewed_files
    inputs.validate_destination(target, [base_dir, sports_binding_path, *[item["path"] for item in sports_files]])
    free = shutil.disk_usage(Path(target).parent).free
    reserve = (CAPS["maximum_output_bytes"] + CAPS["maximum_transient_file_bytes"]
               + CAPS["spill_bytes"] + CAPS["minimum_free_bytes"])
    require(free >= reserve, "estimator output/spill/free-floor capacity insufficient after input build")
    require(all(value == resource.RLIM_INFINITY or value >= CAPS["maximum_transient_file_bytes"]
                for value in resource.getrlimit(resource.RLIMIT_FSIZE)), "inherited process file limit below admitted transient bound")
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    require(available and int(available.group(1)) * 1024 >= CAPS["minimum_available_ram_bytes"],
            "available RAM insufficient for separated DuckDB/array budgets plus headroom")
    require((os.cpu_count() or 0) >= CAPS["threads"], "four estimator threads unavailable")
    body_bytes = sum(item["stat"]["bytes"] for item in base_files)
    planned = (32 * body_bytes + CAPS["maximum_cache_bytes"] *
               (3 * (6 * CAPS["maximum_projection_iterations"] + 12) + 4 * 10 + 4 * 4 + 10 + 196)
               + 4 * CAPS["maximum_output_bytes"] + 6 * sum(item["stat"]["bytes"] for item in sports_files))
    require(planned <= CAPS["maximum_read_bytes"], "declared worst-case serial cache/read plan exceeds ceiling")
    return {"schema_version": "kaushik_replication_estimate_preflight_v1", "status": "preflight_complete",
            "target": str(Path(target).resolve()), "source": source, "base_dir": str(Path(base_dir).resolve()),
            "base_binding": base_binding, "base_manifest": manifest, "base_files": base_files,
            "monthly_paths": monthly, "sports_binding": sports, "sports_binding_identity": sports_id,
            "mode": "sports_metadata_only" if sports_metadata_only else "estimates", "sports_metadata_review": sports_review,
            "sports_files": sports_files, "caps": CAPS, "required_free_bytes": reserve,
            "write_limit_policy": {"process_per_file_transient_bytes": CAPS["maximum_transient_file_bytes"],
                "accepted_cache_bytes": CAPS["maximum_cache_bytes"], "accepted_total_bytes": CAPS["maximum_output_bytes"],
                "enforcement": "RLIMIT_FSIZE bounds transient COPY/score writes including spill; actual artifact size and total accepted size checked immediately afterward before reuse/publication",
                "disk_reservation": "accepted8GB + transient16GB + spill16GB + free_floor20GB =60GB; existing occupied bytes deducted at each write"},
            "observed_free_bytes": free, "planned_maximum_read_bytes": planned,
            "maximum_full_effect_passes_per_model": 6 * CAPS["maximum_projection_iterations"] + 12,
            "maximum_single_effect_iterations": 2,
            "numerical_allocation_preflight": "per-model code cardinality and event-score dimensions admitted before allocation",
            "limits": "metadata/footer preflight only; no trade bodies read; accepted artifacts checked post-write; transient process file writes and read-pass ledger bounded separately"}


def configure(con, spill):
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit=" + inputs.literal(CAPS["memory_limit"]))
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute("SET max_temp_directory_size=" + inputs.literal(str(CAPS["spill_bytes"]) + "B"))
    con.execute("SET temp_directory=" + inputs.literal(spill))
    con.execute("SET preserve_insertion_order=true")
    con.execute("SET enable_object_cache=false")


def reserve_output(stage, ceiling):
    stage = Path(stage)
    used, occupied_spill = 0, 0
    for path in stage.rglob("*"):
        if not path.is_file():
            continue
        stat = path.stat()
        if path.relative_to(stage).parts[0] == "spill":
            # Count only physically allocated bytes, conservatively capped by
            # logical size: sparse/preallocated spill cannot reduce reservation.
            occupied_spill += min(stat.st_size, stat.st_blocks * 512)
        else:
            used += stat.st_size
    require(used + ceiling <= CAPS["maximum_output_bytes"], "remaining estimator output capacity below hard ceiling")
    # disk_usage.free already excludes existing files. Reserve only remaining
    # accepted-output/spill capacities, plus a whole new transient file and the
    # untouched free floor: (8GB-used) +16GB +(16GB-occupied_spill) +20GB.
    # The extra transient file can exceed its accepted limit only in staging;
    # post-write size checks reject it before any reuse/publication.
    needed = (CAPS["maximum_output_bytes"] - used + CAPS["maximum_transient_file_bytes"]
              + max(0, CAPS["spill_bytes"] - occupied_spill) + CAPS["minimum_free_bytes"])
    require(shutil.disk_usage(stage).free >= needed,
            "estimator free-space floor fails before output")


def artifact_info(path, ledger=None):
    if ledger:
        ledger.charge(Path(path).stat().st_size, "artifact_hash:" + Path(path).name)
    return inputs._output_info(path)


def copy_parquet(con, query, path, ceiling=CAPS["maximum_cache_bytes"], *, ledger=None):
    path = Path(path)
    require(not path.exists(), "immutable artifact already exists")
    reserve_output(path.parent, ceiling)
    # RLIMIT_FSIZE is process-wide, so an accepted cache limit would also cap
    # DuckDB's spill files. Permit the separately admitted transient bound, then
    # reject oversized accepted artifacts before hashing or downstream use.
    with inputs.copy_ceiling(CAPS["maximum_transient_file_bytes"]):
        copied = con.execute(f"COPY ({query}) TO {inputs.literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 65536)").fetchone()[0]
    require(path.stat().st_size <= ceiling, "written artifact exceeds accepted per-file ceiling: " + path.name)
    reserve_output(path.parent, 0)
    info = artifact_info(path, ledger)
    require(info["rows"] == copied, "COPY/reopened Parquet row count differs")
    return info


def array_batches(con, query):
    for batch in con.execute(query).fetch_record_batch(CAPS["batch_rows"]):
        yield {name: batch.column(index).to_numpy(zero_copy_only=False) for index, name in enumerate(batch.schema.names)}


def compact_joint(result, stage, key, artifacts, ledger=None):
    """Persist every cluster's joint projected score; keep reader JSON bounded."""
    ids = result.metadata["cluster_levels"]
    dimension = len(result.names)
    scores = result.coefficient_cluster_influence
    require(scores.nbytes <= CAPS["numpy_memory_bytes"], "score array cap exceeded")
    path = Path(stage) / (key + "_scores.parquet")
    require(not path.exists(), "immutable score artifact already exists")
    values = pa.FixedSizeListArray.from_arrays(pa.array(scores.reshape(-1)), dimension)
    table = pa.table({"cluster_key": pa.array([str(item) for item in ids], type=pa.string()), "projected_score": values})
    reserve_output(stage, 1_000_000_000)
    with inputs.copy_ceiling(CAPS["maximum_transient_file_bytes"]):
        pq.write_table(table, path, compression="zstd")
    require(path.stat().st_size <= 1_000_000_000, "written score artifact exceeds accepted per-file ceiling")
    reserve_output(stage, 0)
    info = artifact_info(path, ledger)
    require(info["rows"] == len(ids), "saved score cluster count differs")
    if ledger:
        ledger.charge(info["bytes"], "score_reopen:" + key)
    reopened = pq.read_table(path)
    require(reopened.schema == table.schema and reopened.equals(table), "saved joint scores failed reopen")
    artifact = {"path": path.name, **info, "coefficient_order": list(result.names)}
    artifact["score_definition"] = "projected joint coefficient influence; CR0=score.T@score"
    artifacts[key] = artifact
    output = result.to_dict()
    output["metadata"].pop("cluster_levels", None)
    output["metadata"].pop("cluster_counts", None)
    output["metadata"]["score_artifact"] = artifact
    return output


def group_mean_result(con, relation, cells, cell_expression, *, targets=TARGETS,
                      sports=False, where="TRUE", ledger=None, scan_bytes=0):
    if ledger:
        ledger.charge(scan_bytes, "mean:" + relation)
    expressions = {"price": "P", "win_rate": "Y", "payoff_cents": "payoff", "roi_percent": "roi"}
    sums = ",".join(f"sum({expressions[target]}) s{index}" for index, target in enumerate(targets))
    query = f'SELECT event_cluster AS "cluster",{cell_expression} cell,count(*) n,{sums} FROM {inputs.ident(relation)} WHERE {where} GROUP BY 1,2 ORDER BY 1,2'
    def records():
        for batch in array_batches(con, query):
            for index in range(len(batch["n"])):
                yield {"cluster": batch["cluster"][index], "cell": batch["cell"][index],
                       "count": int(batch["n"][index]), "weight_sum": float(batch["n"][index]),
                       "weighted_sums": [float(batch[f"s{i}"][index]) for i in range(len(targets))]}
    return engine.joint_clustered_means_from_totals(records(), cells=cells, target_names=targets,
        min_observations=500 if sports else 1, min_clusters=30 if sports else 2,
        memory_limit_bytes=CAPS["numpy_memory_bytes"] // 4)


def support_record(result, cells, *, sports):
    ns = result.metadata["n_by_cell"]
    gs = result.metadata["cluster_count_by_cell"]
    return {"n_observations": sum(ns[cell] for cell in cells),
            "n_clusters": min(gs[cell] for cell in cells),
            "contributing_cluster_minimum": min(gs[cell] for cell in cells),
            "cell_support": [{"cell": cell, "n_observations": ns[cell], "n_clusters": gs[cell]} for cell in cells],
            "paper_support": all(gs[cell] >= 30 for cell in cells) if sports else None,
            "project_support": all(ns[cell] >= 500 for cell in cells) if sports else None}


def gap_rows(result, *, scope, phases=None, windows=None, sports=False):
    output = []
    names = result.names
    for target in TARGETS:
        labels = phases or windows or [""]
        vectors = {}
        for label in labels:
            low, high = (f"D1:{label}", f"D10:{label}") if label else ("D1", "D10")
            vector = engine.contrast_vector(names, {f"{target}|{high}": 1, f"{target}|{low}": -1})
            vectors[label] = vector
            estimate = result.contrast(vector, name="D10_minus_D1")
            output.append({"scope": scope, "outcome": target, "phase": label if phases else None,
                "window": label if windows else None, "estimand": "D10_minus_D1",
                **support_record(result, [low, high], sports=sports), **estimate})
        if phases:
            estimate = result.contrast(vectors["in_play"] - vectors["pregame"], name="in_play_minus_pregame_tail_gap")
            output.append({"scope": scope, "outcome": target, "phase": "in_play_minus_pregame",
                "window": None, "estimand": "in_play_minus_pregame_tail_gap",
                **support_record(result, ["D1:pregame", "D10:pregame", "D1:in_play", "D10:in_play"], sports=sports), **estimate})
    return output


def profile_rows(result, *, scope, phases=None, sports=False):
    output = []
    for phase in phases or [""]:
        for bin_number in range(1, 11):
            cell = f"D{bin_number}:{phase}" if phase else f"D{bin_number}"
            for target in TARGETS:
                vector = engine.contrast_vector(result.names, {f"{target}|{cell}": 1})
                row = {"scope": scope, "phase": phase or None, "bin": bin_number, "outcome": target,
                       **support_record(result, [cell], sports=sports), **result.contrast(vector, name=cell)}
                if "price" in result.metadata["target_names"]:
                    for auxiliary in ("price", "win_rate"):
                        index = result.names.index(f"{auxiliary}|{cell}")
                        row["mean_price" if auxiliary == "price" else "win_rate"] = float(result.values[index]) if np.isfinite(result.values[index]) else None
                output.append(row)
    return output


def make_duration_cache(con, stage, ledger, base_bytes):
    """One lossless group scan, with exact price levels and common row membership."""
    con.execute("CREATE TEMP VIEW duration_source AS SELECT *,CASE WHEN bin=10 THEN 1 ELSE 0 END tail FROM analysis_base WHERE NOT is_maker AND duration_eligible AND bin IN (1,10)")
    for family, columns in (("category", "tail,category"), ("price", "tail,P"), ("month", "tail,trade_month")):
        ledger.charge(base_bytes, "duration_dictionary:" + family)
        con.execute(f"CREATE TEMP TABLE {family}_dictionary AS SELECT {columns},dense_rank() OVER(ORDER BY {columns})-1 code FROM (SELECT DISTINCT {columns} FROM duration_source)")
        copy_parquet(con, f"SELECT * FROM {family}_dictionary ORDER BY code", Path(stage) / (family + "_dictionary.parquet"), 500_000_000, ledger=ledger)
    con.execute("""CREATE TEMP VIEW duration_coded AS SELECT d.*,c.code cat_code,p.code price_code,m.code month_code
        FROM duration_source d JOIN category_dictionary c USING(tail,category)
        JOIN price_dictionary p USING(tail,P) JOIN month_dictionary m USING(tail,trade_month)""")
    ledger.charge(base_bytes, "duration_group_cache")
    return group_cache(con, "duration_coded", Path(stage) / "duration_groups.parquet",
        ("event_cluster", "tail", "cat_code", "price_code", "month_code"),
        ("xL", "xR", "payoff", "roi"), extra_keys=("(L>1) lifespan_gt1", "(R>1) remaining_gt1"), ledger=ledger)


def group_cache(con, relation, path, keys, feature_expressions, extra_keys=(), *, ledger=None):
    derived = ",".join(f"{expression} f{index}" for index, expression in enumerate(feature_expressions))
    key_sql = ",".join([*keys, *extra_keys])
    key_names = [*keys, *[item.split()[-1] for item in extra_keys]]
    sums = [f"sum(f{i}) a{i}" for i in range(len(feature_expressions))]
    sums += [f"count(*)*covar_pop(f{i},f{j}) c{i}_{j}" for i in range(len(feature_expressions)) for j in range(i, len(feature_expressions))]
    group_by = ",".join(str(i + 1) for i in range(len(key_names)))
    query = f"WITH source AS (SELECT *,{derived} FROM {inputs.ident(relation)}) SELECT {key_sql},count(*)::BIGINT n_rows,{','.join(sums)} FROM source GROUP BY {group_by} ORDER BY {','.join(key_names)}"
    info = copy_parquet(con, query, path, ledger=ledger)
    con.execute(f"CREATE TEMP VIEW {inputs.ident(path.stem)} AS SELECT * FROM read_parquet({inputs.literal(path)})")
    return {"path": str(path), **info, "feature_count": len(feature_expressions), "feature_expressions": list(feature_expressions),
            "within_moment_definition": "c_i_j=count(*)*covar_pop(f_i,f_j), centered online covariance; a_i=sum(f_i)"}


def model_specs():
    return [{"column": 1, "label": "Original raw", "clocks": ["xL"], "effects": []},
            {"column": 2, "label": "Original category", "clocks": ["xL"], "effects": ["cat_code"]},
            {"column": 3, "label": "Remaining raw", "clocks": ["xR"], "effects": []},
            {"column": 4, "label": "Remaining category", "clocks": ["xR"], "effects": ["cat_code"]},
            {"column": 5, "label": "Both full", "clocks": ["xL", "xR"], "effects": ["cat_code", "price_code", "month_code"]}]


def remap_effect_codes(con, cache_view, effects, prefix):
    joins, selected, levels = [], [], {}
    for effect in effects:
        name = prefix + "_" + effect
        con.execute(f"CREATE TEMP TABLE {inputs.ident(name)} AS SELECT old_code,dense_rank() OVER(ORDER BY old_code)-1 code FROM (SELECT DISTINCT {inputs.ident(effect)} old_code FROM {inputs.ident(cache_view)})")
        levels[effect] = con.execute(f"SELECT count(*) FROM {inputs.ident(name)}").fetchone()[0]
        joins.append(f"JOIN {inputs.ident(name)} {inputs.ident(effect+'map')} ON b.{inputs.ident(effect)}={inputs.ident(effect+'map')}.old_code")
        selected.append(f"{inputs.ident(effect+'map')}.code AS {inputs.ident(effect+'_active')}")
    query = f"SELECT b.*{',' if selected else ''}{','.join(selected)} FROM {inputs.ident(cache_view)} b {' '.join(joins)}"
    return query, levels


def regression_model(con, cache, cache_view, spec, model_id, stage, artifacts, ledger,
                     *, claim_fe=False):
    """Exact record OLS from group moments, including within-cell covariance."""
    clocks, effects = spec["clocks"], spec["effects"]
    raw_width = cache["feature_count"]
    q = len(TARGETS)
    if claim_fe:
        term_names = ("H", "H:xL", "xR", "H:xR")
    else:
        base_names = (("level", *clocks) if not effects else tuple(clocks))
        term_names = tuple(f"{tail}:{name}" for tail in ("D1", "D10") for name in base_names)
    p = len(term_names)
    for effect in effects:
        ledger.charge(cache["bytes"], "active_code_dictionary:" + model_id + ":" + effect)
    ledger.charge(cache["bytes"], "group_population:" + model_id)
    query, levels = remap_effect_codes(con, cache_view, effects, model_id)
    levels_by_tail = None
    if effects and not claim_fe:
        ledger.charge(cache["bytes"], "active_effect_cardinality_by_tail:" + model_id)
        counts_sql = ",".join(f"count(DISTINCT {inputs.ident(effect)}) AS {inputs.ident(effect)}" for effect in effects)
        levels_by_tail = {"D10" if row["tail"] else "D1": {effect: row[effect] for effect in effects}
            for row in rows(con, f"SELECT tail,{counts_sql} FROM {inputs.ident(cache_view)} GROUP BY 1 ORDER BY 1")}
    group_count, observation_count, cluster_count = con.execute(f"SELECT count(*),coalesce(sum(n_rows),0),count(DISTINCT event_cluster) FROM {inputs.ident(cache_view)}").fetchone()
    persistent = 8 * (sum(levels.values()) * (p + q) + max(levels.values(), default=0) * (p + q + 1) + cluster_count * p * q * 6)
    require(persistent + CAPS["batch_rows"] * (raw_width ** 2 + p ** 2 + p * q + 30) * 8 <= CAPS["numpy_memory_bytes"], "model arrays exceed admitted 32GB budget")
    print(json.dumps({"stage": "model_start", "model_id": model_id, "observations": int(observation_count),
                      "groups": int(group_count), "clusters": int(cluster_count), "effect_levels": levels}), flush=True)
    if not observation_count:
        return {"model_id": model_id, "spec": spec, "n_observations": 0, "n_clusters": 0, "suppressed": True,
                "suppression_reasons": ["empty_estimation_sample"], "joint": None,
                "slopes": [{"outcome": target, "clock": clock, "suppressed": True,
                            "suppression_reasons": ["empty_estimation_sample"], "CR0": None,
                            "cluster_count_adjusted": None, "influence": None}
                           for target in TARGETS for clock in spec["report_clocks"]]}

    def replay():
        ledger.charge(cache["bytes"], "projection_or_moments:" + model_id)
        for raw in array_batches(con, query):
            n = raw["n_rows"].astype(np.int64)
            mean = np.column_stack([raw[f"a{i}"] / n for i in range(raw_width)])
            within = np.zeros((len(n), raw_width, raw_width))
            for i in range(raw_width):
                for j in range(i, raw_width):
                    within[:, i, j] = raw[f"c{i}_{j}"]
                    within[:, j, i] = within[:, i, j]
            if claim_fe:
                x, y = mean[:, :4], mean[:, 4:6]
                within_xx, within_xy, within_yy = within[:, :4, :4], within[:, :4, 4:6], within[:, 4:6, 4:6]
            else:
                h = raw["tail"]
                controls = mean[:, [0 if clock == "xL" else 1 for clock in clocks]]
                x, _ = engine.tail_varying_design(controls, clocks, h, intercept=not effects)
                y = mean[:, 2:4]
                transform = np.zeros((len(n), p, 2))
                block = p // 2
                offset = 1 if not effects else 0
                for tail in (0, 1):
                    for index, clock in enumerate(clocks):
                        transform[:, tail * block + offset + index, 0 if clock == "xL" else 1] = h == tail
                within_xx = np.einsum("npi,nij,nqj->npq", transform, within[:, :2, :2], transform)
                within_xy = np.einsum("npi,niq->npq", transform, within[:, :2, 2:4])
                within_yy = within[:, 2:4, 2:4]
            yield {"x": x, "y": y, "weights": n.astype(float), "observation_counts": n,
                "codes": {effect: raw[effect + "_active"].astype(np.int64) for effect in effects},
                "clusters": raw["event_cluster"], "tail": raw.get("tail"),
                "within_xx": within_xx, "within_xy": within_xy, "within_yy": within_yy}

    def projection_progress(value):
        print(json.dumps({"stage": "projection_iteration", "model_id": model_id, **value}), flush=True)
    absorber = (engine.absorb_categorical_effects(replay, term_names=term_names, target_names=TARGETS,
        level_counts=levels, tolerance=CAPS["projection_tolerance"], max_iterations=2 if len(effects)==1 else CAPS["maximum_projection_iterations"],
        memory_limit_bytes=CAPS["numpy_memory_bytes"] // 2, progress=projection_progress) if effects else None)
    moments = engine.RegressionMoments(term_names, TARGETS)
    original_within_y = np.zeros((q, q))
    original_within_x = np.zeros(p)
    for batch in replay():
        x, y, w = absorber.transform(batch) if absorber else (batch["x"], batch["y"], batch["weights"])
        moments.add(x, y, w, batch["observation_counts"])
        moments.xtx += batch["within_xx"].sum(axis=0)
        moments.xty += batch["within_xy"].sum(axis=0)
        moments.yty += batch["within_yy"].sum(axis=0)
        original_within_y += batch["within_yy"].sum(axis=0)
        original_within_x += np.diagonal(batch["within_xx"], axis1=1, axis2=2).sum(axis=0)
    if absorber:
        absorber.original_yty += original_within_y
        absorber.original_x_squared_norms += original_within_x
    fit = engine.fit_ols_moments(moments, absorption=absorber)
    ranks_by_tail = None
    if not claim_fe:
        block = p // 2
        ranks_by_tail = {}
        for index, label in enumerate(("D1", "D10")):
            selected = slice(index*block,(index+1)*block)
            original_norm = absorber.original_x_squared_norms[selected] if absorber else None
            ranks_by_tail[label] = engine.continuous_design_diagnostics(moments.xtx[selected,selected],
                original_squared_norms=original_norm)[2]
    scores = engine.ClusterScoreMoments(p * q, memory_limit_bytes=CAPS["numpy_memory_bytes"] // 4)
    fit_stats = {}
    if fit.beta is not None:
        for batch in replay():
            x, y, w = absorber.transform(batch) if absorber else (batch["x"], batch["y"], batch["weights"])
            correction = batch["within_xy"] - batch["within_xx"] @ fit.beta
            row_score = fit.score_batch(x, y, w) + correction.transpose(0, 2, 1).reshape(len(x), -1)
            scores.add(batch["clusters"], row_score, batch["observation_counts"])
            residual = y - x @ fit.beta
            residual_within = (batch["within_yy"] - fit.beta.T @ batch["within_xy"]
                - batch["within_xy"].transpose(0, 2, 1) @ fit.beta
                + fit.beta.T @ batch["within_xx"] @ fit.beta)
            if not claim_fe:
                for tail, label in ((0, "D1"), (1, "D10")):
                    selected = batch["tail"] == tail
                    value = fit_stats.setdefault(label, {"weight_sum": 0., "y_sum": np.zeros(q),
                        "y_squared_sum": np.zeros(q), "residual_squared_sum": np.zeros(q)})
                    value["weight_sum"] += float(w[selected].sum())
                    value["y_sum"] += (batch["y"][selected] * w[selected, None]).sum(axis=0)
                    value["y_squared_sum"] += (batch["y"][selected] ** 2 * w[selected, None]).sum(axis=0) + np.diagonal(batch["within_yy"][selected], axis1=1, axis2=2).sum(axis=0)
                    value["residual_squared_sum"] += (residual[selected] ** 2 * w[selected, None]).sum(axis=0) + np.diagonal(residual_within[selected], axis1=1, axis2=2).sum(axis=0)
    result = engine.finalize_clustered_regression(fit, scores, fit_statistics_by_group=fit_stats or None)
    joint = compact_joint(result, stage, model_id, artifacts, ledger)
    joint["metadata"].update({"group_count": group_count, "admitted_allocation_bytes": persistent,
                              "grouped_within_moment_definition": cache["within_moment_definition"],
                              "moment_restoration": "group mean contribution n*mean_i*mean_j + centered c_i_j; never subtract raw second moments",
                              "event_score_restoration": "n*Xmean_residual*(Ymean_residual-Xmean_residual*beta) + withinXY-withinXX*beta",
                              "exact_price_effect": "normalized binary64 P, no rounding; separate tails",
                              "effect_cardinality": levels, "effect_cardinality_by_tail": levels_by_tail,
                              "residualized_continuous_design_by_tail": ranks_by_tail,
                              "combined_absorbed_fixed_effect_rank": "unknown; not inferred from cardinalities"})
    slopes = []
    for target in TARGETS:
        for clock in spec["report_clocks"]:
            terms = ({f"{target}|H:{clock}": 1} if claim_fe else
                     {f"{target}|D10:{clock}": 1, f"{target}|D1:{clock}": -1})
            vector = engine.contrast_vector(result.names, terms)
            slopes.append({"outcome": target, "clock": clock,
                           **result.contrast(vector, name="D10_minus_D1_clock_slope" if not claim_fe else "claim_FE_interaction")})
    print(json.dumps({"stage": "model_complete", "model_id": model_id, "suppressed": bool(fit.suppressed_reasons),
                      "reasons": list(fit.suppressed_reasons), "charged_read_bytes": ledger.charged_bytes}), flush=True)
    return {"model_id": model_id, "spec": spec, "n_observations": int(observation_count), "n_clusters": int(cluster_count), "suppressed": bool(fit.suppressed_reasons),
            "suppression_reasons": list(fit.suppressed_reasons), "joint": joint, "slopes": slopes}


def prepare_sports_map(con, binding, stage=None, *, ledger=None):
    """Bound provider metadata only; old phase observations never enter estimation."""
    paths = {item["role"]: item["path"] for item in binding["inputs"]}
    for role, path in paths.items():
        if Path(path).suffix == ".parquet":
            con.execute(f"CREATE TEMP VIEW {inputs.ident('provider_'+role)} AS SELECT * FROM read_parquet({inputs.literal(path)})")
    six = ("nhl", "cbb", "atp", "epl", "cfb", "wnba")
    six_sql = ",".join(inputs.literal(item) for item in six)
    if binding.get("reviewed_metadata_stage", {}).get("reviewed") is True:
        review = binding["reviewed_metadata_stage"]
        metadata, identity = inputs.read_json(review["manifest_path"], review["manifest_sha256"])
        require(metadata.get("status") == "sports_metadata_complete", "incomplete reviewed sports metadata")
        parent = Path(identity["path"]).parent
        con.execute(f"CREATE TEMP TABLE sports_market_map AS SELECT * FROM read_parquet({inputs.literal(parent/'sports_market_map.parquet')})")
        con.execute(f"CREATE TEMP TABLE sports_metadata_exclusions AS SELECT * FROM read_parquet({inputs.literal(parent/'sports_metadata_exclusions.parquet')})")
    elif "sports_market_map" in paths:
        con.execute("CREATE TEMP TABLE sports_market_map AS SELECT * FROM provider_sports_market_map")
        con.execute("CREATE TEMP TABLE sports_metadata_exclusions(market_id VARCHAR,sport VARCHAR,reason VARCHAR)")
    else:
        require({"six_candidates", "six_timing", "six_proof", "six_tokens", "nfl_moneylines", "nba_moneylines", "mlb_phase"} <= set(paths), "complete original sports metadata/proof roles required")
        for relation, keys in (("provider_six_candidates", "market_id"),
                               ("provider_six_timing", "sport,event_slug"),
                               ("provider_six_proof", "sport,event_slug"),
                               ("provider_nfl_moneylines", "market_id"),
                               ("provider_nba_moneylines", "market_id")):
            require(con.execute(f"SELECT count(*) FROM (SELECT {keys} FROM {relation} GROUP BY {keys} HAVING count(*)<>1)").fetchone()[0] == 0,
                    "sports metadata key ambiguity: " + relation)
        require(con.execute(f"""SELECT count(*) FROM provider_six_timing t LEFT JOIN provider_six_proof p USING(sport,event_slug)
            WHERE t.sport IN ({six_sql}) AND (p.eligible IS DISTINCT FROM TRUE OR p.match_exclusion_reason IS NOT NULL
              OR p.timing_exclusion_reason IS NOT NULL)""").fetchone()[0] == 0, "six-sport timing lacks accepted identity/result proof")
        con.execute(f"""CREATE TEMP TABLE six_bound AS SELECT m.market_id,m.sport,m.event_slug,
            m.sport||':'||t.game_id game_key,epoch(t.actual_start_utc) actual_start_seconds,
            epoch(t.actual_end_utc) actual_end_seconds,t.timing_quality,
            'accepted candidate + provider match audit + event timing' provenance
            FROM provider_six_candidates m JOIN provider_six_timing t USING(sport,event_slug)
            WHERE m.sport IN ({six_sql})""")
        con.execute("""CREATE TEMP TABLE claim_pair_health AS SELECT market_id,count(*) native_claims,sum(Y) wins,
            max(token_id) FILTER(WHERE Y=1) winning_token_id FROM claims GROUP BY 1""")
        con.execute("""CREATE TEMP TABLE six_native_proof AS SELECT m.market_id,m.sport,
            CASE WHEN h.native_claims IS DISTINCT FROM 2 OR h.wins IS DISTINCT FROM 1 THEN 'no_admitted_native_binary_claim_pair'
              WHEN count(p.token_id)<>2 OR count(DISTINCT p.token_id)<>2 OR sum(CAST(p.won AS INTEGER))<>1 THEN 'provider_binary_pair_proof_incomplete'
              WHEN count(*) FILTER(WHERE c.token_id IS NULL OR c.market_id<>p.market_id OR c.Y IS DISTINCT FROM CAST(p.won AS DOUBLE)
                OR c.outcome_label IS DISTINCT FROM p.outcome)>0 THEN 'provider_native_resolution_disagreement' END reason
            FROM six_bound m LEFT JOIN claim_pair_health h USING(market_id) LEFT JOIN provider_six_tokens p USING(market_id)
            LEFT JOIN claims c ON CAST(p.token_id AS VARCHAR)=c.token_id GROUP BY m.market_id,m.sport,h.native_claims,h.wins""")
        con.execute("""CREATE TEMP TABLE legacy_bound AS
            SELECT DISTINCT market_id,'mlb' sport,'mlb:'||CAST(game_pk AS VARCHAR) game_key,
              epoch(actual_start_utc) actual_start_seconds,epoch(actual_end_utc) actual_end_seconds,
              'accepted first/last provider play clocks from retained phase metadata' timing_quality,
              'DISTINCT retained MLB market/game clocks; old phase observations excluded' provenance,
              CAST(winning_token_id AS VARCHAR) winning_token_id FROM provider_mlb_phase
            UNION ALL
            SELECT market_id,'nfl','nfl:'||CAST(game_id AS VARCHAR),epoch(actual_start_utc),epoch(actual_end_utc),
              'accepted first/last ESPN play clocks','retained eligible moneyline provider proof',CAST(winning_token_id AS VARCHAR)
              FROM provider_nfl_moneylines
            UNION ALL
            SELECT market_id,'nba','nba:'||CAST(game_id AS VARCHAR),epoch(actual_start_utc),epoch(actual_end_utc),
              'accepted first/last ESPN play clocks','retained eligible moneyline provider proof',CAST(winning_token_id AS VARCHAR)
              FROM provider_nba_moneylines""")
        con.execute("""CREATE TEMP TABLE legacy_native_proof AS SELECT p.market_id,p.sport,
            CASE WHEN h.native_claims IS DISTINCT FROM 2 OR h.wins IS DISTINCT FROM 1 THEN 'no_admitted_native_binary_claim_pair'
              WHEN h.winning_token_id IS DISTINCT FROM p.winning_token_id THEN 'provider_native_resolution_disagreement' END reason
            FROM legacy_bound p LEFT JOIN claim_pair_health h USING(market_id)""")
        con.execute("""CREATE TEMP TABLE sports_metadata_exclusions AS SELECT * FROM six_native_proof WHERE reason IS NOT NULL
            UNION ALL SELECT * FROM legacy_native_proof WHERE reason IS NOT NULL""")
        con.execute("""CREATE TEMP TABLE sports_market_map AS SELECT m.market_id,m.sport,game_key,actual_start_seconds,
            actual_end_seconds,timing_quality,provenance FROM six_bound m JOIN six_native_proof p USING(market_id,sport) WHERE p.reason IS NULL
            UNION ALL SELECT m.market_id,m.sport,game_key,actual_start_seconds,actual_end_seconds,timing_quality,provenance
            FROM legacy_bound m JOIN legacy_native_proof p USING(market_id,sport) WHERE p.reason IS NULL""")
    allowed = ",".join(inputs.literal(item) for item in SPORTS)
    require(con.execute(f"""SELECT count(*) FROM sports_market_map WHERE market_id IS NULL OR market_id='' OR sport IS NULL OR
        sport NOT IN ({allowed}) OR game_key IS NULL OR game_key='' OR actual_start_seconds IS NULL OR actual_end_seconds IS NULL
        OR NOT isfinite(actual_start_seconds) OR NOT isfinite(actual_end_seconds) OR actual_end_seconds<=actual_start_seconds
        OR timing_quality IS NULL OR provenance IS NULL""").fetchone()[0] == 0,
        "invalid admitted sports identity/clock/quality record")
    require(con.execute("SELECT count(*) FROM (SELECT market_id FROM sports_market_map GROUP BY 1 HAVING count(*)<>1)").fetchone()[0] == 0,
            "sports market map is not unique")
    require(con.execute("""SELECT count(*) FROM (SELECT game_key FROM sports_market_map GROUP BY 1 HAVING
        count(DISTINCT sport)<>1 OR count(DISTINCT actual_start_seconds)<>1 OR count(DISTINCT actual_end_seconds)<>1)""").fetchone()[0] == 0,
        "game mapping carries inconsistent clocks/sports")
    coverage = rows(con, """SELECT sport,count(*) markets,count(DISTINCT game_key) games,
        min(actual_start_seconds) first_game_seconds,max(actual_start_seconds) last_game_seconds,
        string_agg(DISTINCT timing_quality,'; ' ORDER BY timing_quality) timing_quality FROM sports_market_map GROUP BY 1 ORDER BY 1""")
    for row in coverage:
        row["first_game_utc"], row["last_game_utc"] = utc(row.pop("first_game_seconds")), utc(row.pop("last_game_seconds"))
    if stage:
        copy_parquet(con, "SELECT * FROM sports_market_map ORDER BY sport,game_key,market_id", Path(stage) / "sports_market_map.parquet", 500_000_000, ledger=ledger)
        copy_parquet(con, "SELECT * FROM sports_metadata_exclusions ORDER BY sport,market_id,reason", Path(stage) / "sports_metadata_exclusions.parquet", 500_000_000, ledger=ledger)
    return coverage


def create_sports_view(con, stage=None, ledger=None, base_bytes=0):
    con.execute("""CREATE TEMP VIEW sports_joined AS SELECT a.*,m.sport,m.game_key,m.actual_start_seconds,m.actual_end_seconds,
        m.timing_quality,m.provenance,a.timestamp-m.actual_start_seconds u,m.actual_end_seconds-a.timestamp r
        FROM analysis_base a JOIN sports_market_map m USING(market_id) WHERE NOT a.is_maker""")
    con.execute("""CREATE TEMP VIEW sports_base AS SELECT * EXCLUDE(event_cluster),game_key event_cluster,
        CASE WHEN u<0 THEN 'pregame' ELSE 'in_play' END phase FROM sports_joined WHERE timestamp<=actual_end_seconds""")
    cache_info = None
    if stage is not None:
        path = Path(stage) / "sports_observations.parquet"
        columns = {row[0] for row in con.execute("DESCRIBE sports_joined").fetchall()}
        locators = [name for name in ("source_month", "source_ordinal") if name in columns]
        locator_sql = "," + ",".join(locators) if locators else ""
        if ledger:
            ledger.charge(base_bytes, "sports_raw_join_count_and_locator_proof")
        unique_sql = (",count(DISTINCT (source_month,source_ordinal)) unique_locators,"
                      "count(*) FILTER(WHERE source_month IS NULL OR source_ordinal IS NULL) missing_locators"
                      if len(locators) == 2 else "")
        raw_count = rows(con, f"SELECT count(*) joined_rows{unique_sql} FROM sports_joined")[0]
        if len(locators) == 2:
            require(raw_count["missing_locators"] == 0 and raw_count["joined_rows"] == raw_count["unique_locators"],
                    "sports raw source locator missing or not unique; join fanout or duplicated locator")
        if ledger:
            ledger.charge(base_bytes, "sports_observation_cache")
        cache_info = copy_parquet(con, f"""SELECT sport,game_key,event_cluster,market_id,P,Y,bin,payoff,roi,timestamp,
            CASE WHEN u<0 THEN 'pregame' ELSE 'in_play' END phase,u,r,actual_start_seconds,actual_end_seconds{locator_sql}
            FROM sports_joined ORDER BY sport,game_key,timestamp,P,Y{locator_sql}""", path, ledger=ledger)
        require(raw_count["joined_rows"] == cache_info["rows"], "sports raw source/COPY count differs")
        cache_info["raw_source_count"] = raw_count["joined_rows"]
        cache_info["source_locator_retained"] = len(locators) == 2
        cache_info["source_locator_unique"] = True if len(locators) == 2 else None
        con.execute("DROP VIEW sports_base")
        con.execute("DROP VIEW sports_joined")
        con.execute(f"CREATE TEMP VIEW sports_joined AS SELECT * FROM read_parquet({inputs.literal(path)})")
        con.execute("""CREATE TEMP VIEW sports_base AS SELECT * EXCLUDE(event_cluster),game_key event_cluster
            FROM sports_joined WHERE timestamp<=actual_end_seconds""")
        if ledger:
            ledger.charge(cache_info["bytes"], "sports_cache_reconciliation")
        cached_count = rows(con, f"SELECT count(*) joined_rows{unique_sql} FROM sports_joined")[0]
        require(cached_count == raw_count, "sports cache retained locator/count proof differs")
        if ledger:
            ledger.charge(cache_info["bytes"], "sports_cache_phase_counts")
        counts = rows(con, """SELECT count(*) joined_rows,count(*) FILTER(WHERE timestamp<=actual_end_seconds) admitted_rows,
            count(*) FILTER(WHERE timestamp>actual_end_seconds) after_end_rows,count(*) FILTER(WHERE timestamp<=actual_end_seconds AND u<0) pregame_rows,
            count(*) FILTER(WHERE timestamp<=actual_end_seconds AND u>=0) in_play_rows FROM sports_joined""")[0]
        require(counts["joined_rows"] == cache_info["rows"] and counts["admitted_rows"] == counts["pregame_rows"]+counts["in_play_rows"] and
                counts["joined_rows"] == counts["admitted_rows"]+counts["after_end_rows"], "sports cache phase/endpoint partition differs")
        cache_info["reconciliation"] = counts
    unions = [f"SELECT *,{inputs.literal(key)} clock_window,{inputs.literal(panel)} clock_panel FROM sports_base WHERE {predicate}"
              for panel, key, _, predicate in WINDOWS]
    con.execute("CREATE TEMP VIEW sports_windows AS " + " UNION ALL ".join(unions))
    return cache_info


def describe_sample(con, base_manifest, ledger, base_bytes):
    ledger.charge(base_bytes, "sample_counts")
    sample = rows(con, f"""SELECT count(*) AS "rows",count(DISTINCT market_id) conditions,count(DISTINCT claim_id) normalized_claims,
        count(DISTINCT event_cluster) clusters,count(DISTINCT event_cluster) FILTER(WHERE cluster_source='native_event') unique_event_clusters,
        count(DISTINCT event_cluster) FILTER(WHERE cluster_source<>'native_event') market_fallback_clusters,
        min(timestamp) first_timestamp,max(timestamp) last_timestamp,count(*) FILTER(WHERE bin IN (1,10)) tail_rows,
        count(*) FILTER(WHERE duration_eligible AND bin IN (1,10)) duration_tail_rows,
        count(DISTINCT event_cluster) FILTER(WHERE duration_eligible AND bin IN (1,10)) duration_tail_clusters,
        count(*) FILTER(WHERE endpoint_seconds>{inputs.CUTOFF_SECONDS}) future_ending_rows
        FROM analysis_base WHERE NOT is_maker""")[0]
    sample["first_execution_utc"] = utc(sample.pop("first_timestamp"))
    sample["last_execution_utc"] = utc(sample.pop("last_timestamp"))
    sample["archive_exclusions"] = base_manifest["exclusions"]
    sample["source_row_counts"] = base_manifest["rows"]
    sample["metadata_health"] = base_manifest.get("metadata_health")
    sample["resolution_censoring"] = "eventual resolved-source vintage; long-horizon unresolved claims can be omitted"
    require(sample["rows"] == base_manifest["rows"]["primary_taker"], "primary row count differs from accepted base")
    require(sample["clusters"] == sample["unique_event_clusters"]+sample["market_fallback_clusters"], "native/fallback cluster partition differs")
    if "support" in base_manifest:
        expected_duration = sum(row["duration_tail_rows"] for row in base_manifest["support"] if row["is_maker"] is False)
        require(sample["duration_tail_rows"] == expected_duration, "duration row count differs from independently accepted input support")
        sample["accepted_input_duration_tail_rows"] = expected_duration
    ledger.charge(base_bytes, "category_partition")
    found = {row["category"]: row["n"] for row in rows(con, "SELECT category,count(*) n FROM analysis_base WHERE NOT is_maker GROUP BY 1")}
    require(set(found) <= set(inputs.CATEGORY_LABELS), "category partition outside frozen taxonomy")
    categories = [{"category": category, "n_observations": found.get(category, 0),
                   "share": found.get(category, 0) / sample["rows"] if sample["rows"] else None}
                  for category in inputs.CATEGORY_LABELS]
    require(sum(row["n_observations"] for row in categories) == sample["rows"], "category partition does not exhaust primary")
    return sample, categories


def estimate_all(con, stage, base_manifest, sports_binding, *, ledger=None, base_bytes=0):
    """Fixture-accessible estimator; production admission lives in run_stage."""
    stage = Path(stage)
    stage.mkdir(exist_ok=True)
    ledger = ledger or ReadLedger()
    artifacts = {}
    if sports_binding.get("reviewed_metadata_stage", {}).get("reviewed") is True:
        review = sports_binding["reviewed_metadata_stage"]
        reviewed_manifest, _ = inputs.read_json(review["manifest_path"], review["manifest_sha256"])
        ledger.charge(sum(item["bytes"] for item in reviewed_manifest["outputs"].values()), "reviewed_sports_metadata_load")
    coverage = prepare_sports_map(con, sports_binding, stage, ledger=ledger)
    sports_cache = create_sports_view(con, stage, ledger, base_bytes)
    sports_bytes = sports_cache["bytes"]
    ledger.charge(base_bytes, "analytic_admission_gate")
    anomalies = con.execute("SELECT count(*) FROM analysis_base WHERE NOT isfinite(payoff) OR NOT isfinite(roi) OR timestamp>=? OR NOT(P>0 AND P<1)", [inputs.CUTOFF_SECONDS]).fetchone()[0]
    require(anomalies == 0, "accepted convention contains nonfinite outcomes or cutoff/price contract violation")
    sample, categories = describe_sample(con, base_manifest, ledger, base_bytes)
    con.execute("CREATE TEMP VIEW primary_taker AS SELECT * FROM analysis_base WHERE NOT is_maker")
    bins = tuple(f"D{i}" for i in range(1, 11))
    table1_result = group_mean_result(con, "primary_taker", bins, "'D'||bin", targets=("price", "win_rate", *TARGETS), ledger=ledger, scan_bytes=base_bytes)
    table1 = {"joint": compact_joint(table1_result, stage, "table1", artifacts, ledger),
              "profile_rows": profile_rows(table1_result, scope="all_markets")}
    table1["gap_rows"] = []
    for target in TARGETS:
        vector = engine.contrast_vector(table1_result.names, {f"{target}|D10": 1, f"{target}|D1": -1})
        table1["gap_rows"].append({"outcome": target, **support_record(table1_result, ["D1", "D10"], sports=False),
                                  **table1_result.contrast(vector, name="D10_minus_D1")})
    cache = make_duration_cache(con, stage, ledger, base_bytes)
    ledger.charge(base_bytes, "duration_source_panel_counts")
    source_counts = rows(con, """SELECT count(*) duration_rows,count(*) FILTER(WHERE L>1) lifespan_gt1_rows,
        count(*) FILTER(WHERE R>1) remaining_gt1_rows FROM duration_source""")[0]
    ledger.charge(cache["bytes"], "duration_cache_panel_counts")
    cache_counts = rows(con, """SELECT coalesce(sum(n_rows),0) duration_rows,
        coalesce(sum(n_rows) FILTER(WHERE lifespan_gt1),0) lifespan_gt1_rows,
        coalesce(sum(n_rows) FILTER(WHERE remaining_gt1),0) remaining_gt1_rows FROM duration_groups""")[0]
    require(source_counts == cache_counts and source_counts["duration_rows"] == sample["duration_tail_rows"],
            "duration source/cache/sample row counts differ")
    sample["duration_reconciliation"] = {"source": source_counts, "group_cache": cache_counts,
        "matches_accepted_input_support": "accepted_input_duration_tail_rows" in sample}
    table2 = []
    for raw_spec in model_specs():
        spec = {**raw_spec, "report_clocks": raw_spec["clocks"]}
        table2.append(regression_model(con, cache, "duration_groups", spec, f"table2_c{spec['column']}", stage, artifacts, ledger))
    table3 = {}
    for panel, predicate, clock in (("L_gt1", "lifespan_gt1", "xL"), ("R_gt1", "remaining_gt1", "xR")):
        con.execute(f"CREATE TEMP VIEW {inputs.ident(panel)} AS SELECT * FROM duration_groups WHERE {predicate}")
        table3[panel] = []
        for column, effects, clocks in ((1, [], [clock]), (2, ["cat_code"], [clock]), (3, ["cat_code", "price_code", "month_code"], ["xL", "xR"])):
            spec = {"column": column, "label": panel, "clocks": clocks, "report_clocks": [clock], "effects": effects}
            table3[panel].append(regression_model(con, cache, panel, spec, f"table3_{panel}_c{column}", stage, artifacts, ledger))
    ledger.charge(base_bytes, "claim_diagnostic_dictionary")
    con.execute("CREATE TEMP TABLE claim_dictionary AS SELECT claim_code old_claim,dense_rank() OVER(ORDER BY claim_code)-1 claim_fe_code FROM (SELECT DISTINCT claim_code FROM duration_source)")
    con.execute("CREATE TEMP VIEW claim_source AS SELECT d.*,c.claim_fe_code,(d.bin=10)::DOUBLE H FROM duration_source d JOIN claim_dictionary c ON d.claim_code=c.old_claim")
    ledger.charge(base_bytes, "claim_diagnostic_group_cache")
    a1_cache = group_cache(con, "claim_source", stage / "claim_groups.parquet", ("event_cluster", "claim_fe_code"),
                           ("H", "H*xL", "xR", "H*xR", "payoff", "roi"), ledger=ledger)
    ledger.charge(a1_cache["bytes"], "claim_cache_population_reconciliation")
    a1_cached_rows = con.execute("SELECT coalesce(sum(n_rows),0) FROM claim_groups").fetchone()[0]
    require(a1_cached_rows == sample["duration_tail_rows"], "A1 grouped cache drops common duration observations")
    a1_spec = {"column": 1, "label": "Claim FE diagnostic", "clocks": ["xL", "xR"], "report_clocks": ["xL", "xR"], "effects": ["claim_fe_code"]}
    a1 = regression_model(con, a1_cache, "claim_groups", a1_spec, "appendix_a1", stage, artifacts, ledger, claim_fe=True)
    ledger.charge(base_bytes, "claim_support")
    a1["claim_support"] = rows(con, """SELECT count(*) claims,coalesce(sum(n),0) observations,count(*) FILTER(WHERE tails=2) both_tail_claims,
        coalesce(sum(n) FILTER(WHERE tails=2),0) both_tail_claim_observations FROM
        (SELECT claim_code,count(*) n,count(DISTINCT bin) tails,count(DISTINCT Y) payouts FROM duration_source GROUP BY 1)""")[0]
    ledger.charge(base_bytes, "claim_outcome_constancy")
    require(con.execute("SELECT count(*) FROM (SELECT claim_code FROM duration_source GROUP BY 1 HAVING count(DISTINCT Y)<>1)").fetchone()[0] == 0,
            "A1 resolved outcome varies within normalized claim")
    require(a1["n_observations"] == a1["claim_support"]["observations"] == a1_cached_rows,
            "A1 model/support/common duration population differs")
    a2 = []
    convention_counts = {}
    for convention, predicate in (("taker_direction", "NOT is_maker"), ("all_buy", "side='BUY'"),
                                  ("maker_buy", "side='BUY' AND is_maker"), ("taker_buy", "side='BUY' AND NOT is_maker")):
        con.execute(f"CREATE TEMP VIEW {inputs.ident(convention)} AS SELECT * FROM analysis_base WHERE {predicate}")
        ledger.charge(base_bytes, "convention_counts:" + convention)
        counts = rows(con, f'SELECT count(*) AS "rows",count(DISTINCT event_cluster) clusters,count(*) FILTER(WHERE bin IN (1,10)) tail_rows FROM {inputs.ident(convention)}')[0]
        convention_counts[convention] = counts["rows"]
        fitted = group_mean_result(con, convention, bins, "'D'||bin", ledger=ledger, scan_bytes=base_bytes)
        gaps = []
        for target in TARGETS:
            vector = engine.contrast_vector(fitted.names, {f"{target}|D10": 1, f"{target}|D1": -1})
            gaps.append({"outcome": target, **support_record(fitted, ["D1", "D10"], sports=False), **fitted.contrast(vector, name="D10_minus_D1")})
        a2.append({"convention": convention, "counts": counts, "joint": compact_joint(fitted, stage, "a2_" + convention, artifacts, ledger),
                   "profile_rows": profile_rows(fitted, scope=convention), "gap_rows": gaps})
    require(convention_counts["all_buy"] == convention_counts["maker_buy"] + convention_counts["taker_buy"], "A2 BUY role partition differs")
    sports = {"coverage": coverage, "coverage_qualification": sports_binding["coverage_qualification"],
              "phase_rows": [], "profile_rows": [], "window_rows": [], "joint_artifacts": {}}
    sports["metadata_exclusions"] = rows(con, "SELECT sport,reason,count(*) markets FROM sports_metadata_exclusions GROUP BY 1,2 ORDER BY 1,2")
    sports["observation_cache"] = sports_cache
    ledger.charge(sports_bytes, "sports_all_band_phase_support")
    found = {(row["scope"],row["phase"]):row for row in rows(con, """SELECT coalesce(sport,'pooled') AS "scope",phase,
        count(*) n_observations,count(DISTINCT event_cluster) n_games FROM sports_base GROUP BY GROUPING SETS((sport,phase),(phase))""")}
    sports["phase_counts"] = [found.get((scope,phase), {"scope":scope,"phase":phase,"n_observations":0,"n_games":0})
        for scope in ("pooled",*SPORTS) for phase in ("pregame","in_play")]
    ledger.charge(sports_bytes, "sports_all_band_scope_support")
    found_scope = {row["scope"]:row for row in rows(con, """SELECT coalesce(sport,'pooled') AS "scope",
        count(*) n_observations,count(DISTINCT event_cluster) n_games FROM sports_base GROUP BY GROUPING SETS((sport),())""")}
    sports["scope_counts"] = [found_scope.get(scope, {"scope":scope,"n_observations":0,"n_games":0}) for scope in ("pooled",*SPORTS)]
    ledger.charge(sports_bytes * 14, "sports_window_partition_support")
    window_counts = {row["window"]:row for row in rows(con, """SELECT clock_window AS "window",clock_panel panel,count(*) n_observations,
        count(DISTINCT event_cluster) n_games FROM sports_windows GROUP BY 1,2""")}
    sports["window_counts"] = [window_counts.get(key,{"window":key,"panel":panel,"n_observations":0,"n_games":0}) for panel,key,_,_ in WINDOWS]
    pre_rows = sum(row["n_observations"] for row in sports["window_counts"] if row["panel"]=="pregame")
    live_rows = sum(row["n_observations"] for row in sports["window_counts"] if row["panel"]=="since_start")
    pooled_phases = {row["phase"]:row["n_observations"] for row in sports["phase_counts"] if row["scope"]=="pooled"}
    require(pre_rows == pooled_phases["pregame"] and live_rows == pooled_phases["in_play"], "sports calendar window partition differs")
    sports["window_reconciliation"] = {"pregame_window_rows":pre_rows,"in_play_since_start_window_rows":live_rows,
        "final_hour_overlaps_since_start":True,"exact_start_and_endpoint_retained":True}
    ledger.charge(sports_bytes, "sports_exclusion_and_window_reconciliation")
    sports["exclusions"] = rows(con, """SELECT sport,count(*) joined_rows,count(*) FILTER(WHERE timestamp>actual_end_seconds) after_end_rows,
        count(*) FILTER(WHERE timestamp<=actual_end_seconds) admitted_rows,count(DISTINCT game_key) games_with_archive_rows
        FROM sports_joined GROUP BY 1 ORDER BY 1""")
    for scope in ("pooled", *SPORTS):
        predicate = "TRUE" if scope == "pooled" else "sport=" + inputs.literal(scope)
        name = "sport_" + scope
        con.execute(f"CREATE TEMP VIEW {inputs.ident(name)} AS SELECT * FROM sports_base WHERE {predicate}")
        con.execute(f"CREATE TEMP VIEW {inputs.ident(name+'_windows')} AS SELECT * FROM sports_windows WHERE {predicate}")
        phase_cells = tuple(f"{tail}:{phase}" for phase in ("pregame", "in_play") for tail in ("D1", "D10"))
        phase_result = group_mean_result(con, name, phase_cells, "'D'||bin||':'||phase", where="bin IN (1,10)", sports=True, ledger=ledger, scan_bytes=sports_bytes)
        sports["phase_rows"] += gap_rows(phase_result, scope=scope, phases=["pregame", "in_play"], sports=True)
        sports["joint_artifacts"][name+"_phases"] = compact_joint(phase_result, stage, name+"_phases", artifacts, ledger)
        profile_cells = tuple(f"D{bin_number}:{phase}" for phase in ("pregame", "in_play") for bin_number in range(1, 11))
        profile_result = group_mean_result(con, name, profile_cells, "'D'||bin||':'||phase", sports=True, ledger=ledger, scan_bytes=sports_bytes)
        sports["profile_rows"] += profile_rows(profile_result, scope=scope, phases=["pregame", "in_play"], sports=True)
        sports["joint_artifacts"][name+"_profiles"] = compact_joint(profile_result, stage, name+"_profiles", artifacts, ledger)
        window_keys = [item[1] for item in WINDOWS]
        window_cells = tuple(f"{tail}:{window}" for window in window_keys for tail in ("D1", "D10"))
        window_result = group_mean_result(con, name+"_windows", window_cells, "'D'||bin||':'||clock_window", where="bin IN (1,10)", sports=True, ledger=ledger, scan_bytes=sports_bytes * 14)
        window_rows = gap_rows(window_result, scope=scope, windows=window_keys, sports=True)
        labels = {key: (panel, label, order) for order, (panel, key, label, _) in enumerate(WINDOWS)}
        for row in window_rows:
            row["panel"], row["label"], row["window_order"] = labels[row["window"]]
        sports["window_rows"] += window_rows
        sports["joint_artifacts"][name+"_windows"] = compact_joint(window_result, stage, name+"_windows", artifacts, ledger)
    require(len(sports["phase_rows"]) == 60 and len(sports["profile_rows"]) == 400 and len(sports["window_rows"]) == 280,
            "expected sports grids incomplete")
    require(all(model["n_observations"] == sample["duration_tail_rows"] for model in table2) and
            all(model["n_observations"] == source_counts["lifespan_gt1_rows" if panel=="L_gt1" else "remaining_gt1_rows"]
                for panel, models in table3.items() for model in models), "duration column/source populations differ")
    return {"schema_version": "kaushik_replication_estimates_v1", "status": "estimates_complete", "definitions": {
        "observation": "archive-recorded row preserving multiplicity; primary taker BUY plus complemented taker SELL",
        "weight": "one per admitted archive record", "cutoff_utc": inputs.CUTOFF_ISO,
        "price": "exact normalized binary64; fixed ten-cent bins; 0<P<1",
        "outcomes": {"payoff_cents": "100*(Y-P)", "roi_percent": "100*(Y/P-1), before averaging"},
        "duration": "created_at/end_date proxy; xL=log2(1+L),xR=log2(1+R); raw=no categorical effects",
        "categories": "existing native 12 categories + explicit Unclassified; user-selected adaptation",
        "uncertainty": engine.VARIANCE_NOTE, "sports": "available audited provider-covered cohort; game clustering; 30 games +500 records per required cell",
        "sports_timestamp_quality": "archive approximate execution timestamps; provider start/end clocks retain their recorded source quality",
        "a1": "claim-FE diagnostic; payoff within claim mechanically reduces to -P; no independent calibration claim"},
        "sample": sample, "categories": categories, "table1": table1, "table2": table2, "table3": table3,
        "appendix_a1": a1, "appendix_a2": a2, "sports": sports, "score_artifacts": artifacts}


def run_stage(reviewed, fresh, command, reviewed_identity=None):
    from production_guard import require_production_host
    require_production_host()
    require(isinstance(reviewed_identity, dict), "reviewed preflight file identity required")
    reopened_review, reopened_id = inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])
    require(reopened_review == reviewed and reopened_id == reviewed_identity, "reviewed preflight content/stat drift")
    for field in ("schema_version", "status", "target", "source", "base_dir", "base_binding", "base_manifest",
                  "base_files", "monthly_paths", "sports_binding", "sports_binding_identity", "sports_files", "caps", "required_free_bytes", "write_limit_policy", "mode", "sports_metadata_review"):
        require(reviewed[field] == fresh[field], "reviewed estimator preflight drift: " + field)
    require(fresh["mode"] == "estimates" and fresh["sports_metadata_review"] is not None, "estimate stage lacks independently reviewed sports metadata")
    target = Path(fresh["target"])
    inputs.validate_destination(target, [fresh["base_dir"], *[item["path"] for item in fresh["sports_files"]]])
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    ledger, con = ReadLedger(), None
    published = False
    started = time.monotonic()
    frozen = [*fresh["base_files"], *fresh["sports_files"]]
    json_bindings = [fresh["base_binding"]["manifest"], fresh["base_binding"]["acceptance"],
                     fresh["sports_binding_identity"], fresh["sports_metadata_review"]["manifest"],
                     fresh["sports_metadata_review"]["acceptance"], reviewed_identity]
    try:
        for item in json_bindings:
            ledger.charge(item["stat"]["bytes"], "binding_reopen_before:" + item["path"])
            require(inputs.read_json(item["path"], item["sha256"])[1] == item, "estimator JSON input binding/stat drift")
        for item in frozen:
            require(inputs.stat_identity(item["path"]) == item["stat"], "estimator input stat changed before body")
            ledger.charge(item["stat"]["bytes"], "input_hash_before:" + item["path"])
            require(inputs.sha256(item["path"]) == item["expected_sha256"], "estimator input digest differs")
        con = duckdb.connect()
        configure(con, stage / "spill")
        inputs.create_analysis_view(con, fresh["base_dir"], explicit_month_paths=fresh["monthly_paths"])
        con.execute(f"CREATE TEMP VIEW claims AS SELECT * FROM read_parquet({inputs.literal(Path(fresh['base_dir'])/'claims.parquet')})")
        body_bytes = sum(item["stat"]["bytes"] for item in fresh["base_files"])
        output = estimate_all(con, stage, fresh["base_manifest"], fresh["sports_binding"], ledger=ledger, base_bytes=body_bytes)
        require(output["sports"]["observation_cache"]["source_locator_retained"] is True and
                output["sports"]["observation_cache"]["source_locator_unique"] is True,
                "production sports cache lacks unique archive record locators")
        require(len(json.dumps(output, sort_keys=True, allow_nan=False).encode()) <= CAPS["maximum_json_bytes"], "estimates JSON exceeds admitted cap")
        reserve_output(stage, CAPS["maximum_json_bytes"])
        inputs.write_json(stage / "estimates.json", output)
        reserve_output(stage, 0)
        outputs = {str(path.relative_to(stage)): artifact_info(path, ledger) for path in sorted(stage.glob("*.parquet"))}
        result, result_id = inputs.read_json(stage / "estimates.json")
        require(result == output, "serialized estimates failed reopen")
        for item in frozen:
            ledger.charge(item["stat"]["bytes"], "input_hash_after:" + item["path"])
            require(inputs.stat_identity(item["path"]) == item["stat"] and inputs.sha256(item["path"]) == item["expected_sha256"], "estimator input drift before publication")
        for item in json_bindings:
            ledger.charge(item["stat"]["bytes"], "binding_reopen_after:" + item["path"])
            require(inputs.read_json(item["path"], item["sha256"])[1] == item, "estimator JSON input binding/stat drift before publication")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "estimator source drift before publication")
        for relative, expected in outputs.items():
            ledger.charge(expected["bytes"], "published_artifact_reopen_hash:" + relative)
        manifest = {"schema_version": "kaushik_replication_estimate_stage_v1", "status": "estimates_complete",
            "source": fresh["source"], "preflight": reviewed, "command": command, "caps": CAPS,
            "base_binding": fresh["base_binding"], "sports_binding": fresh["sports_binding_identity"],
            "outputs": outputs, "estimates_json": {"sha256": result_id["sha256"], "bytes": result_id["stat"]["bytes"]},
            "read_ledger": ledger.to_dict(), "wall_seconds": time.monotonic() - started,
            "reconciliation": {"all_inputs_reopened": True, "all_outputs_reopened": True,
                "common_duration_population": True, "buy_role_partition": True, "expected_grids_serialized": True},
            "environment": {"python": sys.version, "duckdb": duckdb.__version__, "numpy": np.__version__}}
        reserve_output(stage, CAPS["maximum_json_bytes"])
        inputs.write_json(stage / "manifest.json", manifest)
        reserve_output(stage, CAPS["maximum_json_bytes"])
        require(shutil.disk_usage(stage).free >= CAPS["minimum_free_bytes"], "final estimator disk floor failed")
        con.close()
        con = None
        inputs.atomic_publish(stage, target)
        published = True
        saved, saved_identity = inputs.read_json(target / "manifest.json")
        require(saved == manifest, "published estimator manifest differs")
        saved_estimates, _ = inputs.read_json(target / "estimates.json", result_id["sha256"])
        require(saved_estimates == output, "published estimates differ")
        for relative, expected in outputs.items():
            require(inputs._output_info(target / relative) == expected, "published estimator artifact hash/schema/count drift")
        inputs.write_json(target / "acceptance.json", {"schema_version": "kaushik_replication_estimate_acceptance_v1",
            "status": "estimates_reopened_accepted", "manifest_sha256": saved_identity["sha256"],
            "estimates_sha256": result_id["sha256"],
            "source_head": fresh["source"]["head"], "all_outputs_reopened": True})
        reserve_output(target, 0)
        return manifest
    except BaseException as error:
        failure_root = target if published else stage
        if failure_root.exists() and not (failure_root / "failure.json").exists():
            inputs.write_json(failure_root / "failure.json", {"status": "estimates_incomplete", "error_type": type(error).__name__,
                "reason": str(error), "read_ledger": ledger.to_dict()})
        raise
    finally:
        if con is not None:
            con.close()


def run_sports_metadata_stage(reviewed, fresh, command, reviewed_identity=None):
    """Admit/prove provider metadata against claims without any trade body scan."""
    from production_guard import require_production_host
    require_production_host()
    require(isinstance(reviewed_identity, dict), "reviewed preflight file identity required")
    reopened_review, reopened_id = inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])
    require(reopened_review == reviewed and reopened_id == reviewed_identity, "reviewed sports metadata preflight content/stat drift")
    for name in ("schema_version", "status", "target", "source", "base_dir", "base_binding", "base_manifest",
                 "base_files", "monthly_paths", "sports_binding", "sports_binding_identity", "sports_files", "caps", "required_free_bytes", "write_limit_policy", "mode", "sports_metadata_review"):
        require(reviewed[name] == fresh[name], "reviewed sports metadata preflight drift: " + name)
    require(fresh["mode"] == "sports_metadata_only" and fresh["sports_metadata_review"] is None and
            not fresh["sports_binding"].get("reviewed_metadata_stage"), "sports metadata-only mode must build original provider proofs")
    target = Path(fresh["target"])
    inputs.validate_destination(target, [fresh["base_dir"], *[item["path"] for item in fresh["sports_files"]]])
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    selected = [item for item in fresh["base_files"] if item["relative_path"] == "claims.parquet"] + fresh["sports_files"]
    ledger, con, published = ReadLedger(), None, False
    json_bindings = [fresh["base_binding"]["manifest"], fresh["base_binding"]["acceptance"],
                     fresh["sports_binding_identity"], reviewed_identity]
    try:
        for item in json_bindings:
            ledger.charge(item["stat"]["bytes"], "sports_metadata_binding_before:" + item["path"])
            require(inputs.read_json(item["path"], item["sha256"])[1] == item, "sports metadata JSON binding/stat drift")
        for item in selected:
            ledger.charge(item["stat"]["bytes"], "sports_metadata_input_hash_before:" + item["path"])
            require(inputs.stat_identity(item["path"]) == item["stat"] and inputs.sha256(item["path"]) == item["expected_sha256"], "sports metadata input drift")
        con = duckdb.connect()
        configure(con, stage / "spill")
        con.execute(f"CREATE TEMP VIEW claims AS SELECT * FROM read_parquet({inputs.literal(Path(fresh['base_dir'])/'claims.parquet')})")
        ledger.charge(4 * sum(item["stat"]["bytes"] for item in selected), "sports_metadata_proof_and_coverage_queries")
        coverage = prepare_sports_map(con, fresh["sports_binding"], stage, ledger=ledger)
        exclusions = rows(con, "SELECT sport,reason,count(*) markets FROM sports_metadata_exclusions GROUP BY 1,2 ORDER BY 1,2")
        candidate_counts = rows(con, "SELECT sport,count(*) markets,count(DISTINCT event_slug) events FROM provider_six_candidates GROUP BY 1 ORDER BY 1")
        audit_counts = rows(con, "SELECT sport,eligible,match_exclusion_reason,timing_exclusion_reason,count(*) events FROM provider_six_proof GROUP BY ALL ORDER BY 1,2,3,4")
        outputs = {path.name: artifact_info(path, ledger) for path in sorted(stage.glob("*.parquet"))}
        for item in selected:
            ledger.charge(item["stat"]["bytes"], "sports_metadata_input_hash_after:" + item["path"])
            require(inputs.stat_identity(item["path"]) == item["stat"] and inputs.sha256(item["path"]) == item["expected_sha256"], "sports metadata input changed before publication")
        for item in json_bindings:
            ledger.charge(item["stat"]["bytes"], "sports_metadata_binding_after:" + item["path"])
            require(inputs.read_json(item["path"], item["sha256"])[1] == item, "sports metadata JSON binding/stat drift before publication")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "sports metadata source drift")
        for relative, output in outputs.items():
            ledger.charge(output["bytes"], "published_sports_metadata_reopen_hash:" + relative)
        manifest = {"schema_version": "kaushik_replication_sports_metadata_v1", "status": "sports_metadata_complete",
            "source": fresh["source"], "base_binding": fresh["base_binding"], "provider_inputs": fresh["sports_binding"]["inputs"],
            "coverage_qualification": fresh["sports_binding"]["coverage_qualification"], "coverage": coverage,
            "metadata_exclusions": exclusions, "six_candidate_coverage": candidate_counts, "six_match_timing_audit": audit_counts,
            "outputs": outputs, "preflight": reviewed, "command": command, "read_ledger": ledger.to_dict(),
            "trade_bodies_read": False, "native_pair_and_provider_result_proof": True}
        reserve_output(stage, CAPS["maximum_json_bytes"])
        inputs.write_json(stage / "manifest.json", manifest)
        reserve_output(stage, CAPS["maximum_json_bytes"])
        con.close()
        con = None
        inputs.atomic_publish(stage, target)
        published = True
        saved, saved_id = inputs.read_json(target / "manifest.json")
        require(saved == manifest, "published sports metadata manifest differs")
        for relative, output in outputs.items():
            require(inputs._output_info(target / relative) == output, "published sports metadata outputs differ")
        inputs.write_json(target / "acceptance.json", {"schema_version": "kaushik_replication_sports_metadata_acceptance_v1",
            "status": "sports_metadata_reopened_accepted", "manifest_sha256": saved_id["sha256"],
            "source_head": fresh["source"]["head"], "all_outputs_reopened": True})
        reserve_output(target, 0)
        return manifest
    except BaseException as error:
        failure_root = target if published else stage
        if failure_root.exists():
            inputs.write_json(failure_root / "failure.json", {"status": "sports_metadata_incomplete", "error_type": type(error).__name__, "reason": str(error)})
        raise
    finally:
        if con is not None:
            con.close()
