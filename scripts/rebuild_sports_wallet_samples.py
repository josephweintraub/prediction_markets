#!/usr/bin/env python3
"""Admit immutable nine-sport exact sources under H/F0/F1 wallet flags.

This changes only embedded buyer flags. It preserves inferred native buyers,
economic payloads, cohorts and clocks, and runs no scientific estimators.
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from analysis.multisport_game_dynamics.estimate_flb_decay import _create_exact_observations
from scripts.repair_polymarket_wallet_attribution import copy_size_limit

SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "atp", "epl", "cfb", "wnba")
REGIMES = ("H", "F0", "F1")
EXACT_ROLES = ("new_exact", "nfl_exact", "nba_exact", "old_mlb_exact", "restored_mlb_exact")
PHASE_ROLES = ("new_phase", "mlb_phase", "nfl_phase", "nba_phase")
FLAG_ROLES = ("historic_flags", "f0_flags", "f1_flags")
DATA_ROLES = EXACT_ROLES + PHASE_ROLES + FLAG_ROLES
EVIDENCE_ROLES = ("sep20_filtered", "sep20_all", "mlb_manifest", "mlb_summary", "mlb_qa", "paired_flags_manifest")
INPUT_ROLES = DATA_ROLES + EVIDENCE_ROLES
IDENTITY = ("transaction_hash", "log_index", "exchange_address")
FLAG_COLUMN = "buyer_is_flagged_nonhuman"
SCENARIOS = (("old_H", "H", "old_mlb_exact"), ("restored_H", "H", "restored_mlb_exact"),
             ("restored_F0", "F0", "restored_mlb_exact"), ("restored_F1", "F1", "restored_mlb_exact"))
CAPS = {"memory_limit": "96GB", "memory_bytes": 96_000_000_000, "threads": 8,
        "spill_bytes": 4_000_000_000, "output_bytes": 8_000_000_000,
        "minimum_free_bytes": 12_000_000_000, "maximum_planned_read_bytes": 1024**4,
        "metadata_reserve_bytes": 3 * 16 * 1024**2,
        "maximum_json_bytes": 16 * 1024**2, "maximum_footer_bytes": 8 * 1024**2,
        "maximum_row_groups": 100_000, "maximum_exact_rows": 60_000_000,
        "maximum_flag_rows": 20_000_000}
FROZEN_HASHES = {
    "new_phase": "6d3b90d69945e8a47fcabe2cddb105706cb08a67e97e7e7f412a841229e50b43",
    "mlb_phase": "0ac506e548112949317e94c611d5c856abc4c5dfe6bc98b561e2af3bc5198bdd",
    "nfl_phase": "2f42333eba1d2c5c0da6675a86fe235e18209b16f77f81baf459258af20b6c73",
    "nba_phase": "a9ae6f9d181c48101fae403e37c12d165e61be34c3ba6d3acfbaafb5fdd26c8f",
    "new_exact": "f20f49955811016a5b89ce50de689e961233bf0e7321d700419c996071f405c6",
    "old_mlb_exact": "ef85a4326100f0e4e73e82c3f2a6a0a81e2a675782b6ebb291a1b595f771a0a1",
    "nfl_exact": "fe86dbbeac3c0f3065181a9e148da2e2c565938e4f7263eb9dacf524a3cd114d",
    "nba_exact": "4010212774d5c6b9236c34f22897b7e6d13f67c1035a7c45371252efe8376490",
    "historic_flags": "e1bfb6163db0e0112c2bf912de7d62353cc75378547e5cfde27a008979836c0a",
    "restored_mlb_exact": "9a5cc486c7127ec21e0f9c3a867d797d6d0d8b7a518c57f7ecd32ff92e29ebd9",
    "sep20_filtered": "78d27b16f35dbf698779e7abf47fc10c54ee7132aba7c8a430ea8f2076f4316d",
    "sep20_all": "fd6b2c959f26896afa89ec0828b451b68eaca7f527417c6891cf8a1956b1b733",
    "mlb_manifest": "d26b516b6e2a63f16b6a0256ec694f9bdb632a251d37e385452cc32bb820142b",
    "mlb_summary": "a66c392ef8e8a61afa2c37aa654f4933324014669b2e679eead9856772267a8b",
    "mlb_qa": "bcc2955b78c17fcc68350693b9a699765a5df6a9046bd63c09a8b8aac293a933",
}
ESTIMATOR_SHA = "1b8bcf9e6a1554be504c833e43fa03cacd8c47fbc77596b37f43d4180241ca37"
CLASSIFIER_SHA = "52ee90215b81346852fa775cc1f9df6d72b7b7d5dcbc6ef357c1b7e379fb42fe"
REPAIR_SHA = "f142313238e9be25541f97dac4ba24de7ad6b49484812e81e7c6640eefd50daa"
REPAIR_QA_SHA = "06d0fd376c7c6aede78016e50bc34cd63220cf8a15d481598303679d7de2cbb3"
DEFINITIONS = {
    "all": "frozen accepted cohort; exact timestamp <= recorded end; 0 < price < 1; all pregame retained",
    "filtered": "same cohort/time; .01 < price < .99; NOT coalesce(is_nonhuman,false)",
    "flags": "join lower(proxyWallet) to unique lower(flag.proxyWallet); missing and matched null retained as false",
    "unit": "legacy inferred outcome-token BUY per resolved fill; not certified own counterparty action",
    "contrasts": "old_H to restored_H: MLB coverage; restored_H to restored_F0: historical policy/vintage; restored_F0 to restored_F1: identity repair",
    "new_cohort": "six-sport cohort/clocks/outcomes embedded in frozen exact input; phase overlap must agree; phase support is not a new inclusion filter",
}


class AdmissionBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise AdmissionBlocked(message)


def quote(path):
    return str(Path(path).resolve()).replace("'", "''")


def ident(name):
    return '"' + name.replace('"', '""') + '"'


def stat_identity(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular non-symlink input required: " + str(path))
    value = path.stat()
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def sha256(path):
    before = stat_identity(path)
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    require(before == stat_identity(path), "input changed while hashing: " + str(path))
    return digest.hexdigest()


def read_json(path):
    require(0 < Path(path).stat().st_size <= CAPS["maximum_json_bytes"], "JSON size outside cap")
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out, "duplicate JSON key")
            out[key] = value
        return out
    value = json.loads(Path(path).read_text(), object_pairs_hook=unique,
                       parse_constant=lambda value: (_ for _ in ()).throw(AdmissionBlocked(value)))
    require(isinstance(value, dict), "JSON object required")
    return value


def footer_snapshot(path):
    before = stat_identity(path)
    require(before["bytes"] >= 12, "short Parquet input")
    with Path(path).open("rb") as stream:
        stream.seek(-8, 2)
        trailer = stream.read(8)
        length, magic = struct.unpack("<I4s", trailer)
        require(magic == b"PAR1" and 0 < length <= CAPS["maximum_footer_bytes"] and
                length+8 <= before["bytes"], "invalid/oversized Parquet footer")
        stream.seek(-length-8, 2)
        digest = hashlib.sha256(stream.read(length)+trailer).hexdigest()
    footer = pq.ParquetFile(path)
    require(footer.metadata.num_row_groups <= CAPS["maximum_row_groups"], "row-group metadata cap exceeded")
    require(len(set(footer.schema_arrow.names)) == len(footer.schema_arrow.names), "duplicate physical field names")
    require(before == stat_identity(path), "input changed during footer read")
    return {"rows": footer.metadata.num_rows, "row_groups": footer.metadata.num_row_groups,
            "schema": str(footer.schema_arrow), "footer_sha256": digest}


def write_json(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= CAPS["maximum_json_bytes"], "JSON output cap exceeded")
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def read_head():
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()


def source_files():
    names = ("scripts/rebuild_sports_wallet_samples.py", "tests/test_rebuild_sports_wallet_samples.py",
             "analysis/multisport_game_dynamics/estimate_flb_decay.py",
             "analysis/sports_game_dynamics/artifacts.py", "production_guard.py",
             "scripts/repair_polymarket_wallet_attribution.py", "scripts/audit_polymarket_lineage.py",
             "docs/analysis_specs/sports_wallet_adoption_v1.md")
    return {name: {"path": str(REPO/name), "bytes": (REPO/name).stat().st_size,
                   "sha256": sha256(REPO/name)} for name in names}


def require_committed_sources():
    for name in source_files():
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        unchanged = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=REPO)
        require(tracked.returncode == unchanged.returncode == 0, "source must be committed/unchanged: " + name)
    require(sha256(REPO/"analysis/multisport_game_dynamics/estimate_flb_decay.py") == ESTIMATOR_SHA,
            "frozen scientific estimator source differs")


def available_memory():
    require(platform.system() == "Linux", "resource admission requires Linux")
    match = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    require(match is not None, "available RAM unavailable")
    return int(match.group(1)) * 1024


def destination(target, paths):
    require(not target.exists(), "immutable destination exists")
    for path in paths.values():
        require(target != path and target not in path.parents and path not in target.parents,
                "output overlaps input")


def atomic_publish(staging, target):
    lib = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        result = lib.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = lib.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise AdmissionBlocked("atomic no-replace publication unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def production_contract(paths):
    """Resolve released evidence; production has no alternate population/count overrides."""
    require(set(paths) == set(INPUT_ROLES), "production input roles differ")
    for name in EVIDENCE_ROLES:
        require(0 < stat_identity(paths[name])["bytes"] <= CAPS["maximum_json_bytes"], "metadata input size outside cap: "+name)
    for name in EVIDENCE_ROLES[:-1]:
        require(sha256(paths[name]) == FROZEN_HASHES[name], "frozen metadata hash differs: " + name)
    filtered, all_manifest = (read_json(paths[name]) for name in ("sep20_filtered", "sep20_all"))
    sep_order = ("new_phase", "mlb_phase", "nfl_phase", "nba_phase", "new_exact", "old_mlb_exact",
                 "nfl_exact", "nba_exact", "historic_flags")
    specs = {}
    for index, name in enumerate(sep_order, 1):
        entry = filtered["inputs"][f"input_{index:02d}"]
        require(entry == all_manifest["inputs"][f"input_{index:02d}"] and entry["sha256"] == FROZEN_HASHES[name],
                "September input manifests differ: " + name)
        require(str(paths[name]) == entry["path"], "frozen production path differs: " + name)
        specs[name] = dict(entry)
    mlb_manifest, summary, qa = (read_json(paths[name]) for name in ("mlb_manifest", "mlb_summary", "mlb_qa"))
    require(mlb_manifest["summary_artifact"]["sha256"] == FROZEN_HASHES["mlb_summary"] and
            {key: value for key, value in mlb_manifest.items() if key != "summary_artifact"} == summary,
            "MLB manifest/summary differ")
    require(summary["status"] == "mlb_unfiltered_samples_complete" and all(summary["reconciliation"].values()),
            "MLB producer incomplete")
    require(qa["status"] == "mlb_unfiltered_saved_artifact_qa_complete" and all(qa["gates"].values()) and
            qa["summary"]["sha256"] == FROZEN_HASHES["mlb_summary"], "MLB saved QA incomplete/unbound")
    restored = summary["outputs"]["exact_trades.parquet"]
    require(restored["sha256"] == FROZEN_HASHES["restored_mlb_exact"] and
            qa["inputs"]["exact"]["sha256"] == restored["sha256"] and
            str(paths["restored_mlb_exact"]) == qa["inputs"]["exact"]["path"], "restored MLB evidence differs")
    specs["restored_mlb_exact"] = {**qa["inputs"]["exact"], "rows": restored["rows"]}
    # This released parent contract is checked below; its exact receipt is bound by preflight.
    paired = read_json(paths["paired_flags_manifest"])
    specs.update(paired_flag_specs(paired, paths))
    for name in EVIDENCE_ROLES:
        specs[name] = {"path": str(paths[name]), "bytes": paths[name].stat().st_size, "sha256": sha256(paths[name])}
    expected = {"old_H": {"all": all_manifest["observation_counts"], "filtered": filtered["observation_counts"]},
                "restored_H": {"all": {**all_manifest["observation_counts"], "mlb": summary["counts"]["new_accepted_all_rows"]},
                               "filtered": {**filtered["observation_counts"], "mlb": summary["counts"]["new_accepted_filtered_rows"]}}}
    return {"schema_version": 1, "inputs": specs, "expected_counts": expected,
            "expected_estimator_sha256": ESTIMATOR_SHA, "paired_flags_source_head": paired["source"]["head"],
            "production_contract": True}


def paired_flag_specs(paired, paths):
    """Only complete, paired unchanged-classifier output receipts are admissible."""
    require(paired.get("schema_version") == "polymarket_wallet_flags_rebuild_v1" and
            paired.get("status") == "wallet_flags_rebuild_complete", "paired flags incomplete")
    require(paired.get("data_certified") is False and paired.get("downstream_adoption") == "pending",
            "paired flags certification/adoption differs")
    definition = paired.get("contract", {})
    require(definition.get("timestamp_lower_inclusive") == 1590969600 and
            definition.get("sides") == "all published sides" and definition.get("timezone") == "UTC",
            "paired classifier population differs")
    require(definition.get("classifier") == "analysis.bot_filter.build_wallet_flags" and
            definition.get("classifier_sha256") == CLASSIFIER_SHA and
            definition.get("repair_only_pair") == "corrected_vs_legacy_recomputed" and
            paired["source"]["sha256"]["analysis/bot_filter.py"] == CLASSIFIER_SHA,
            "unchanged classifier binding differs")
    require(paired["binding"]["repair_manifest"]["sha256"] == REPAIR_SHA and
            paired["binding"]["repair_qa"]["sha256"] == REPAIR_QA_SHA, "paired repair receipt binding differs")
    gates = ("admitted_trade_counts_conserved", "unique_normalized_nonblank_wallets", "non_null_boolean_flags",
             "flag_outputs_exactly_reopened", "all_inputs_layout_metadata_and_content_reopened", "exact_source_wallet_keys_and_counts")
    require(all(paired.get("reconciliation", {}).get(name) is True for name in gates), "paired reconciliation incomplete")
    require(paired["inputs"]["historical_flags"]["pipeline_data"]["expected_content_sha256"] == FROZEN_HASHES["historic_flags"],
            "paired historical shared flags differ")
    outputs = {item["path"]: item for item in paired["outputs"]}
    require(len(outputs) == len(paired["outputs"]), "duplicate paired output paths")
    out = {}
    for build, role, filename in (("legacy", "f0_flags", "legacy_recomputed_flags.parquet"),
                                 ("corrected", "f1_flags", "wallet_flags.parquet")):
        artifact = outputs[filename]
        path = paths["paired_flags_manifest"].parent/artifact["path"]
        require(path.resolve() == paths[role] and re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]),
                "paired flag output path/hash differs")
        out[role] = {**artifact, "path": str(paths[role])}
        rows, flags = paired["builds"][build]["rows"], paired["builds"][build]["flags"]
        require(rows["total_rows"] == rows["admitted_rows"]+rows["excluded_before_start_rows"]+rows["null_timestamp_rows"] and
                rows["null_timestamp_rows"] == rows["invalid_admitted_rows"] == 0 and flags["trades"] == rows["admitted_rows"] and
                flags["invalid_keys"] == flags["invalid_payload"] == 0 and
                flags["wallets"] == flags["normalized_wallets"] == artifact["rows"] and
                all(value == 0 for value in flags["flag_null_counts"].values()), "paired build counts/gates differ")
    require(paired["builds"]["legacy"]["rows"] == paired["builds"]["corrected"]["rows"], "paired timestamp population differs")
    return out


def preflight(paths, target, expected_head, contract):
    paths = {name: Path(path).resolve() for name, path in paths.items()}
    target = Path(target).resolve()
    require(set(paths) == set(contract["inputs"]) and set(DATA_ROLES) <= set(paths), "contract input inventory differs")
    destination(target, paths)
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head) and read_head() == expected_head, "source HEAD differs")
    if contract.get("production_contract"):
        require(contract["paired_flags_source_head"] == expected_head, "paired/sample source generation differs")
    inventory = {}
    for name, path in paths.items():
        spec = contract["inputs"][name]
        require(spec["path"] == str(path) and re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]), "explicit input binding absent")
        item = {"path": str(path), "stat": stat_identity(path), "expected_sha256": spec["sha256"]}
        require(item["stat"]["bytes"] == spec["bytes"], "input byte contract differs: " + name)
        if name in DATA_ROLES:
            item.update(footer_snapshot(path))
            limit = CAPS["maximum_flag_rows"] if name in FLAG_ROLES else CAPS["maximum_exact_rows"]
            require(0 < item["rows"] <= limit, "footer row cap exceeded: " + name)
            require("rows" not in spec or item["rows"] == spec["rows"], "footer row contract differs: " + name)
            for field in ("schema", "footer_sha256"):
                require(field not in spec or item[field] == spec[field], "footer/schema contract differs: "+name)
        inventory[name] = item
    planned = 64 * sum(item["stat"]["bytes"] for item in inventory.values()) + 12 * CAPS["output_bytes"]
    require(planned <= CAPS["maximum_planned_read_bytes"], "planned read cap exceeded")
    working = 2048 * sum(inventory[name]["rows"] for name in EXACT_ROLES) + 128 * sum(inventory[name]["rows"] for name in FLAG_ROLES)
    require(working <= CAPS["memory_bytes"], "working memory estimate exceeds cap")
    parent = target.parent
    while not parent.exists():
        parent = parent.parent
    free = shutil.disk_usage(parent).free
    require(free >= sum(CAPS[name] for name in ("minimum_free_bytes", "spill_bytes", "output_bytes")), "insufficient disk reserve")
    memory = available_memory()
    require(memory >= CAPS["memory_bytes"] + 12_000_000_000, "insufficient available RAM")
    require((os.cpu_count() or 0) >= CAPS["threads"], "insufficient CPUs")
    require(all(item["stat"] == stat_identity(paths[name]) for name, item in inventory.items()), "layout changed in preflight")
    return {"schema_version": 1, "status": "sports_wallet_samples_preflight_complete", "data_certified": False,
            "target": str(target),
            "source": {"expected_head": expected_head, "files": source_files()}, "caps": dict(CAPS),
            "inputs": inventory, "contract": contract, "planned_read_bytes": planned,
            "working_memory_estimate_bytes": working, "free_disk_bytes": free, "available_memory_bytes": memory,
            "full_input_hashes_verified": False, "limits": "metadata/footer/resource admission only; no trade queries"}


def configure(con, spill):
    con.execute(f"SET memory_limit='{CAPS['memory_limit']}'")
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute(f"SET max_temp_directory_size='{CAPS['spill_bytes']}B'")
    con.execute(f"SET temp_directory='{quote(spill)}'")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET preserve_insertion_order=false")
    optimizers = {row[0] for row in con.execute("SELECT name FROM duckdb_optimizers()").fetchall()}
    if "common_subplan" in optimizers:
        con.execute("SET disabled_optimizers='common_subplan'")


def number(con, sql):
    return int(con.execute(sql).fetchone()[0])


def schema(con, relation):
    return tuple((row[0], row[1]) for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall())


def unique_ids(con, relation):
    require(number(con, f"SELECT count(*) FROM {relation} WHERE transaction_hash IS NULL OR trim(transaction_hash)='' "
                   "OR log_index IS NULL OR log_index<0 OR exchange_address IS NULL OR trim(exchange_address)='' ") == 0,
            "null/blank native identity: " + relation)
    require(number(con, f"SELECT count(*)-count(DISTINCT(lower(transaction_hash),log_index,lower(exchange_address))) FROM {relation}") == 0,
            "duplicate native identity: " + relation)


def payload_equal(con, left, right, columns, subset=False):
    """EXCEPT ALL preserves multiplicity; float payload equality is additionally bit exact."""
    types = dict(schema(con, left))
    expressions = [f"float_bits({ident(name)})" if types[name] in ("FLOAT", "DOUBLE") else ident(name) for name in columns]
    select = ",".join(expressions)
    require(number(con, f"SELECT count(*) FROM (SELECT {select} FROM {left} EXCEPT ALL SELECT {select} FROM {right})") == 0,
            "nonflag/full payload missing or changed: " + left)
    if not subset:
        require(number(con, f"SELECT count(*) FROM (SELECT {select} FROM {right} EXCEPT ALL SELECT {select} FROM {left})") == 0,
                "nonflag/full payload multiplicity changed: " + right)


def register_bits(con):
    import numpy as np
    def bits(values):
        array = values.combine_chunks()
        payload = np.asarray(array.to_numpy(zero_copy_only=False), dtype=np.float64).view(np.uint64)
        return pa.array(payload, mask=np.asarray(array.is_null()), type=pa.uint64())
    con.create_function("float_bits", bits, ["DOUBLE"], "UBIGINT", type="arrow", null_handling="special")


def flags_relation(con, path, name):
    con.execute(f"CREATE TEMP VIEW {name}_raw AS SELECT * FROM read_parquet('{quote(path)}')")
    observed = dict(schema(con, name+"_raw"))
    require(observed.get("proxyWallet") == "VARCHAR" and observed.get("is_nonhuman") == "BOOLEAN", "flag schema differs")
    require(number(con, f"SELECT count(*) FROM {name}_raw WHERE proxyWallet IS NULL OR trim(proxyWallet)='' OR trim(proxyWallet)<>proxyWallet") == 0,
            "null/blank/whitespace flag keys")
    require(number(con, f"SELECT count(*) FROM (SELECT lower(proxyWallet) FROM {name}_raw GROUP BY 1 HAVING count(*)<>1)") == 0,
            "nonunique normalized flag keys")
    con.execute(f"CREATE TEMP TABLE {name} AS SELECT lower(proxyWallet) wallet,is_nonhuman FROM {name}_raw")


def validate_exact(con, relation, role):
    observed = dict(schema(con, relation))
    required = (*IDENTITY, "proxyWallet", "counterparty", "market_id", "token_id", "timestamp", "price")
    require(set(required) <= set(observed), "exact columns missing: " + role)
    require(observed["price"] == "DOUBLE" and observed["proxyWallet"] == "VARCHAR", "exact price/actor type differs")
    require(observed["timestamp"] == "BIGINT" and observed["log_index"] in ("INTEGER", "BIGINT") and
            all(observed[name] == "VARCHAR" for name in ("transaction_hash", "exchange_address", "market_id", "token_id")),
            "native identity/key/timestamp types differ")
    require(number(con, f"SELECT count(*) FROM {relation} WHERE proxyWallet IS NULL OR trim(proxyWallet)='' OR trim(proxyWallet)<>proxyWallet "
                   "OR market_id IS NULL OR trim(market_id)='' OR token_id IS NULL OR trim(token_id)='' "
                   "OR timestamp IS NULL OR timestamp<=0 OR price IS NULL OR NOT isfinite(price)") == 0,
            "invalid exact actor/timestamp/price: " + role)
    unique_ids(con, relation)
    if role in ("new_exact", "nfl_exact", "nba_exact"):
        require(observed.get(FLAG_COLUMN) == "BOOLEAN", "embedded flag type differs")
        require(number(con, f"SELECT count(*) FROM {relation} WHERE {FLAG_COLUMN} IS NULL") == 0, "null embedded historical flags")


def refresh_exact(con, paths, staging):
    register_bits(con)
    for regime, role in zip(REGIMES, FLAG_ROLES):
        flags_relation(con, paths[role], "flags_"+regime)
        if regime != "H":
            require(number(con, f"SELECT count(*) FROM flags_{regime} WHERE is_nonhuman IS NULL") == 0,
                    "new flag regime has null classification")
    outputs, refreshed = {}, {}
    for role in EXACT_ROLES:
        con.execute(f"CREATE TEMP VIEW {role} AS SELECT * FROM read_parquet('{quote(paths[role])}')")
        validate_exact(con, role, role)
    mlb_columns = tuple(name for name, _ in schema(con, "old_mlb_exact"))
    require(schema(con, "old_mlb_exact") == schema(con, "restored_mlb_exact"), "old/restored MLB schema differs")
    payload_equal(con, "old_mlb_exact", "restored_mlb_exact", mlb_columns, subset=True)
    # Exact inputs supply the six-sport clocks; phase rows validate overlap only.
    retained = ",".join("'"+sport+"'" for sport in SPORTS[3:])
    metadata = "sport,event_slug,market_id,market_date,actual_start_utc,actual_end_utc"
    con.execute(f"CREATE TEMP TABLE new_clock_metadata AS SELECT DISTINCT {metadata} FROM new_exact WHERE sport IN ({retained})")
    require(number(con, "SELECT count(*) FROM (SELECT market_id FROM new_clock_metadata GROUP BY 1 HAVING count(*)<>1)") == 0,
            "six-sport exact clocks are not unique by market")
    con.execute(f"CREATE TEMP TABLE phase_clock_metadata AS SELECT DISTINCT {metadata} FROM read_parquet('{quote(paths['new_phase'])}') WHERE sport IN ({retained})")
    require(number(con, "SELECT count(*) FROM (SELECT market_id FROM phase_clock_metadata GROUP BY 1 HAVING count(*)<>1)") == 0,
            "six-sport phase clocks are not unique by market")
    payload_equal(con, "phase_clock_metadata", "new_clock_metadata", tuple(metadata.split(",")), subset=True)
    for role in ("new_exact", "nfl_exact", "nba_exact"):
        require(number(con, f"SELECT count(*) FROM {role} e LEFT JOIN flags_H f ON lower(e.proxyWallet)=f.wallet "
                       f"WHERE e.{FLAG_COLUMN} IS DISTINCT FROM coalesce(f.is_nonhuman,false)") == 0,
                "historical embedded flags differ from H: " + role)
        columns = tuple(name for name, _ in schema(con, role))
        nonflag = tuple(name for name in columns if name != FLAG_COLUMN)
        for regime in REGIMES:
            folder = staging/regime
            folder.mkdir(exist_ok=True)
            output = folder/("exact_buys.parquet" if role == "new_exact" else role+".parquet")
            select = ",".join("coalesce(f.is_nonhuman,false)::BOOLEAN AS "+ident(name) if name == FLAG_COLUMN else "e."+ident(name)
                              for name in columns)
            con.execute(f"CREATE TEMP VIEW refreshed AS SELECT {select} FROM {role} e LEFT JOIN flags_{regime} f ON lower(e.proxyWallet)=f.wallet")
            used = sum(file.stat().st_size for file in staging.rglob("*") if file.is_file())
            allowance = min(CAPS["output_bytes"]-used-CAPS["metadata_reserve_bytes"],
                shutil.disk_usage(staging).free-CAPS["minimum_free_bytes"]-CAPS["spill_bytes"]-CAPS["metadata_reserve_bytes"])
            require(allowance > 0, "no admitted COPY allowance after metadata/free-space reserve")
            with copy_size_limit(allowance):
                con.execute(f"COPY refreshed TO '{quote(output)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            require(sum(file.stat().st_size for file in staging.rglob("*.parquet")) <= CAPS["output_bytes"], "output cap exceeded")
            con.execute(f"CREATE TEMP VIEW reopened AS SELECT * FROM read_parquet('{quote(output)}')")
            require(schema(con, role) == schema(con, "reopened"), "refreshed exact schema changed")
            unique_ids(con, "reopened")
            payload_equal(con, role, "reopened", nonflag)
            payload_equal(con, "refreshed", "reopened", columns)
            require(number(con, f"SELECT count(*) FROM reopened e LEFT JOIN flags_{regime} f ON lower(e.proxyWallet)=f.wallet "
                           f"WHERE e.{FLAG_COLUMN} IS DISTINCT FROM coalesce(f.is_nonhuman,false)") == 0, "saved flags differ after rejoin")
            outputs[str(output.relative_to(staging))] = {"path": str(output.relative_to(staging)), "bytes": output.stat().st_size,
                                                        "sha256": sha256(output), "rows": number(con, "SELECT count(*) FROM reopened"),
                                                        "schema": [list(value) for value in schema(con, "reopened")],
                                                        "footer": footer_snapshot(output)}
            refreshed[(regime, role)] = output
            con.execute("DROP VIEW reopened")
            con.execute("DROP VIEW refreshed")
    return outputs, refreshed


def eligible_rows(con, paths, exact_paths, flags_path, mlb_role):
    """Independent row admission before calling the unchanged scientific normalizer."""
    flags_relation(con, flags_path, "current_flags")
    pieces = []
    for role in ("new_exact", "nfl_exact", "nba_exact", mlb_role):
        path = exact_paths.get(role, paths[role])
        con.execute(f"CREATE TEMP VIEW audit_{role} AS SELECT * FROM read_parquet('{quote(path)}')")
        if role == "new_exact":
            meta = "e.sport,e.event_slug::VARCHAR event_id,e.market_id,e.market_date,e.timestamp trade_timestamp,e.price,e.won::DOUBLE won,e.usdc,e.proxyWallet,e.actual_start_utc,e.actual_end_utc"
            retained = ",".join("'"+sport+"'" for sport in SPORTS[3:])
            joined = f"(SELECT * FROM audit_{role} WHERE sport IN ({retained})) e LEFT JOIN current_flags f ON lower(e.proxyWallet)=f.wallet"
            outside = "false"
        else:
            sport = "mlb" if "mlb" in role else role.split("_")[0]
            event = "game_pk" if sport == "mlb" else "game_id"
            outcome = "(e.outcome=m.winning_outcome)" if sport == "mlb" else "(e.token_id=m.winning_token_id)"
            dollars = "usdcSize" if sport == "mlb" else "usdc"
            fields = f"market_id,{event},official_date,"+("winning_outcome" if sport == "mlb" else "winning_token_id")+",actual_start_utc,actual_end_utc"
            con.execute(f"CREATE TEMP TABLE audit_{sport}_metadata AS SELECT DISTINCT {fields} FROM read_parquet('{quote(paths[sport+'_phase'])}')")
            require(number(con, f"SELECT count(*) FROM (SELECT market_id FROM audit_{sport}_metadata GROUP BY 1 HAVING count(*)<>1)") == 0,
                    "nonunique frozen market metadata: "+sport)
            meta = f"'{sport}' sport,m.{event}::VARCHAR event_id,e.market_id,m.official_date market_date,e.timestamp trade_timestamp,e.price,{outcome}::DOUBLE won,e.{dollars} usdc,e.proxyWallet,m.actual_start_utc,m.actual_end_utc"
            joined = f"audit_{role} e LEFT JOIN audit_{sport}_metadata m USING(market_id) LEFT JOIN current_flags f ON lower(e.proxyWallet)=f.wallet"
            outside = "m.market_id IS NULL"
        pieces.append(f"SELECT {meta},{outside} outside_cohort,(f.wallet IS NULL) missing_flag,(f.wallet IS NOT NULL AND f.is_nonhuman IS NULL) null_flag,coalesce(f.is_nonhuman,false) flagged FROM {joined}")
    con.execute("CREATE TEMP TABLE audit_rows AS "+" UNION ALL ".join(pieces))
    require(number(con, "SELECT count(*) FROM audit_rows WHERE NOT outside_cohort AND (event_id IS NULL OR trim(event_id)='' "
                   "OR market_date IS NULL OR actual_start_utc IS NULL OR actual_end_utc IS NULL OR actual_end_utc<=actual_start_utc "
                   "OR NOT isfinite(actual_start_utc) OR NOT isfinite(actual_end_utc) OR won IS NULL OR won NOT IN (0,1) OR usdc IS NULL OR usdc<=0 OR NOT isfinite(usdc))") == 0,
            "invalid frozen outcomes/clocks/economics")
    con.execute("CREATE TEMP VIEW audit_all AS SELECT * FROM audit_rows WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>0 AND price<1")
    con.execute("CREATE TEMP VIEW audit_filtered AS SELECT * FROM audit_all WHERE price>.01 AND price<.99 AND NOT flagged")
    return [dict(zip(("sport", "raw", "outside_cohort", "post_end", "invalid_price", "all", "extreme", "flagged_interior", "filtered", "missing_flag_all", "null_flag_all", "pregame_all", "end_equal_all", "missing_flag_raw", "null_flag_raw", "missing_flag_filtered", "null_flag_filtered"), row))
            for row in con.execute("""SELECT sport,count(*),count(*) FILTER(WHERE outside_cohort),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp>epoch(actual_end_utc)),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND NOT(price>0 AND price<1)),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>0 AND price<1),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>0 AND price<1 AND NOT(price>.01 AND price<.99)),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>.01 AND price<.99 AND flagged),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>.01 AND price<.99 AND NOT flagged),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>0 AND price<1 AND missing_flag),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>0 AND price<1 AND null_flag),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<epoch(actual_start_utc) AND price>0 AND price<1),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp=epoch(actual_end_utc) AND price>0 AND price<1),
              count(*) FILTER(WHERE missing_flag),count(*) FILTER(WHERE null_flag),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>.01 AND price<.99 AND NOT flagged AND missing_flag),
              count(*) FILTER(WHERE NOT outside_cohort AND trade_timestamp<=epoch(actual_end_utc) AND price>.01 AND price<.99 AND NOT flagged AND null_flag)
              FROM audit_rows GROUP BY sport ORDER BY sport""").fetchall()]


def audit_scenario(paths, refreshed, staging, scenario, regime, mlb_role, contract):
    con = duckdb.connect()
    try:
        configure(con, staging/"spill")
        register_bits(con)
        exact_paths = {role: refreshed[(regime, role)] for role in ("new_exact", "nfl_exact", "nba_exact")}
        flags_path = paths[FLAG_ROLES[REGIMES.index(regime)]]
        counts = eligible_rows(con, paths, exact_paths, flags_path, mlb_role)
        require({row["sport"] for row in counts} == set(SPORTS), "nine-sport audit domain differs")
        for row in counts:
            require(row["raw"] == row["outside_cohort"]+row["post_end"]+row["invalid_price"]+row["all"], "cohort exclusion law failed")
            require(row["all"] == row["extreme"]+row["flagged_interior"]+row["filtered"], "filtered exclusion law failed")
        for sample, relation in (("all", "audit_all"), ("filtered", "audit_filtered")):
            for name in ("observations", "mlb_metadata", "nfl_metadata", "nba_metadata", "duration_reference"):
                con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute("DROP VIEW IF EXISTS weighted_observations")
            _create_exact_observations(con, exact_paths["new_exact"], paths[mlb_role], paths["mlb_phase"],
                exact_paths["nfl_exact"], paths["nfl_phase"], exact_paths["nba_exact"], paths["nba_phase"], flags_path,
                "all_trades" if sample == "all" else "filtered_trades")
            actual = dict(con.execute("SELECT sport,count(*) FROM observations GROUP BY sport").fetchall())
            require(actual == {row["sport"]: row[sample] for row in counts}, "unchanged normalizer counts differ")
            expected = contract.get("expected_counts", {}).get(scenario, {}).get(sample)
            require(expected is None or actual == expected, "frozen baseline counts differ: "+scenario+"/"+sample)
            con.execute(f"CREATE TEMP VIEW oracle AS SELECT sport,event_id,market_id,market_date,trade_timestamp,price,won,"
                f"(won-price)::DOUBLE calibration_error,usdc,proxyWallet,timezone('UTC',to_timestamp(trade_timestamp))::DATE trade_day,"
                f"actual_start_utc,actual_end_utc FROM {relation}")
            columns = tuple(name for name, _ in schema(con, "oracle"))
            payload_equal(con, "oracle", "observations", columns)
            if sample == "all":
                con.execute("CREATE TEMP TABLE normalized_all AS SELECT * FROM observations")
            else:
                payload_equal(con, "observations", "normalized_all", tuple(name for name, _ in schema(con, "observations") if name != "fixed_time"), subset=True)
            con.execute("DROP VIEW oracle")
        return counts
    finally:
        con.close()


def build_run(paths, target, expected_head, reviewed, contract, command=None, reviewed_identity=None):
    """Fixtures supply small explicit contracts; CLI always constructs the released contract."""
    paths = {name: Path(path).resolve() for name, path in paths.items()}
    target = Path(target).resolve()
    current = preflight(paths, target, expected_head, contract)
    for key in ("target", "source", "caps", "inputs", "contract", "planned_read_bytes", "working_memory_estimate_bytes"):
        require(current[key] == reviewed[key], "reviewed preflight differs: "+key)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    (staging/"spill").mkdir()
    frozen = {}
    started = time.monotonic()
    con = None
    try:
        for name, path in paths.items():
            require(stat_identity(path) == reviewed["inputs"][name]["stat"], "reviewed input layout differs")
            digest = sha256(path)
            require(digest == contract["inputs"][name]["sha256"], "frozen input hash differs: "+name)
            frozen[name] = {"path": str(path), "sha256": digest, "stat_before": stat_identity(path)}
        con = duckdb.connect()
        configure(con, staging/"spill")
        outputs, refreshed = refresh_exact(con, paths, staging)
        con.close()
        con = None
        scenarios = {}
        for name, regime, mlb_role in SCENARIOS:
            scenarios[name] = {"flag_regime": regime, "mlb_source": mlb_role,
                               "counts": audit_scenario(paths, refreshed, staging, name, regime, mlb_role, contract),
                               "estimator_sources": {**{role: str(refreshed[(regime, role)].relative_to(staging)) for role in ("new_exact", "nfl_exact", "nba_exact")},
                                                     "mlb_exact": str(paths[mlb_role]), "wallet_flags": str(paths[FLAG_ROLES[REGIMES.index(regime)]])}}
        invariant_fields = ("sport", "raw", "outside_cohort", "post_end", "invalid_price", "all", "extreme", "pregame_all", "end_equal_all")
        reference = [{key: row[key] for key in invariant_fields}
                     for row in scenarios["restored_H"]["counts"]]
        for name in ("restored_F0", "restored_F1"):
            comparison = [{key: row[key] for key in invariant_fields}
                          for row in scenarios[name]["counts"]]
            require(comparison == reference, "all-trades support changed under reflagging")
        cases = {}
        case_specs = (("historic_filtered", "old_H", "filtered"),
                      ("restored_historical", "restored_H", "filtered"),
                      ("restored_legacy_recomputed", "restored_F0", "filtered"),
                      ("restored_repaired", "restored_F1", "filtered"),
                      ("restored_all", "restored_F1", "all"))
        for case, scenario, sample in case_specs:
            regime = scenarios[scenario]["flag_regime"]
            mlb_role = scenarios[scenario]["mlb_source"]
            engine_inputs = {}
            for role in PHASE_ROLES:
                engine_inputs[role] = {"path": str(paths[role]), "bytes": frozen[role]["stat_before"]["bytes"], "sha256": frozen[role]["sha256"]}
            for role in ("new_exact", "nfl_exact", "nba_exact"):
                filename = str(refreshed[(regime, role)].relative_to(staging))
                engine_inputs[role] = {"path": str(target/filename), "bytes": outputs[filename]["bytes"], "sha256": outputs[filename]["sha256"]}
            for role, source in (("mlb_exact", mlb_role), ("wallet_flags", FLAG_ROLES[REGIMES.index(regime)])):
                engine_inputs[role] = {"path": str(paths[source]), "bytes": frozen[source]["stat_before"]["bytes"], "sha256": frozen[source]["sha256"]}
            cases[case] = {"engine_inputs": engine_inputs, "trade_sample": "all_trades" if sample == "all" else "filtered_trades",
                           "observation_counts": {row["sport"]: row[sample] for row in scenarios[scenario]["counts"]}}
        for name, path in paths.items():
            require(stat_identity(path) == frozen[name]["stat_before"] and sha256(path) == frozen[name]["sha256"], "input drift before publication: "+name)
            frozen[name]["stat_after"] = stat_identity(path)
            if name in DATA_ROLES:
                require(footer_snapshot(path) == {key: reviewed["inputs"][name][key] for key in
                        ("rows", "row_groups", "schema", "footer_sha256")}, "input footer changed before publication: "+name)
        require(read_head() == expected_head and source_files() == reviewed["source"]["files"], "source drift before publication")
        for name, output in outputs.items():
            path = staging/name
            require(path.stat().st_size == output["bytes"] and sha256(path) == output["sha256"] and
                    footer_snapshot(path) == output["footer"], "output layout/content changed before publication: "+name)
        require(not any((staging/"spill").iterdir()), "spill still held after connections closed")
        (staging/"spill").rmdir()
        result = {"schema_version": 1, "status": "sports_wallet_samples_complete", "data_certified": False,
                  "scientific_estimators_rerun": False, "definitions": DEFINITIONS, "caps": dict(CAPS),
                  "source": {**reviewed["source"], "head": expected_head}, "inputs": frozen, "outputs": outputs,
                  "scenarios": scenarios, "cases": cases,
                  "flag_manifest": ({"path": str(paths["paired_flags_manifest"]), "bytes": frozen["paired_flags_manifest"]["stat_before"]["bytes"],
                                     "sha256": frozen["paired_flags_manifest"]["sha256"]} if "paired_flags_manifest" in paths else None),
                  "gates": {"all_trade_regime_invariant": True, "historical_observation_reproduction": True},
                  "command": command or [], "preflight": current, "reviewed_preflight": reviewed_identity,
                  "reconciliation": {name: True for name in ("historical_H_labels_reproduced", "all_nonflag_payloads_bit_exact",
                    "native_ids_unique", "saved_flags_rejoined", "normalizer_oracle_equal", "filtered_subset_of_all",
                    "disjoint_exclusions", "all_support_invariant", "input_and_source_freeze")},
                  "wall_seconds": time.monotonic()-started,
                  "environment": {"python": sys.version, "duckdb": duckdb.__version__, "pyarrow": pa.__version__, "platform": sys.platform},
                  "exit_profile": {"exit_status": 0},
                  "limits": "Analytic input/flag adoption only. Frozen inferred buyers, censoring and ATP clocks remain; no native action or whole-data certification."}
        write_json(staging/"summary.json", result)
        write_json(staging/"manifest.json", {**result, "summary_artifact": {"path": "summary.json", "sha256": sha256(staging/"summary.json"), "bytes": (staging/"summary.json").stat().st_size}})
        require(sum(path.stat().st_size for path in staging.rglob("*") if path.is_file()) <= CAPS["output_bytes"], "complete output cap exceeded")
        require(shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "free disk floor breached before publication")
        atomic_publish(staging, target)
        return result
    except BaseException as error:
        if con is not None:
            con.close()
        write_json(staging/"failure.json", {"status": "blocked_sports_wallet_samples", "data_certified": False,
                    "error_class": type(error).__name__, "error": str(error)[:4096], "inputs": frozen,
                    "staging_directory": str(staging), "final_directory": str(target), "exit_profile": {"exit_status": 1}})
        raise


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in INPUT_ROLES:
        parser.add_argument("--"+name.replace("_", "-"), required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--preflight-dir")
    parser.add_argument("--reviewed-preflight")
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    if args.preflight_only != bool(args.preflight_dir):
        parser.error("metadata-only mode requires separate --preflight-dir")
    if not args.preflight_only and not (args.reviewed_preflight and args.reviewed_preflight_sha256):
        parser.error("body requires reviewed preflight path/hash")
    return args


def main(argv=None):
    from production_guard import require_production_host
    args = parse_args(argv)
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production requires canonical venv")
    require_committed_sources()
    paths = {name: Path(getattr(args, name)).resolve() for name in INPUT_ROLES}
    target = Path(args.run_dir).resolve()
    contract = production_contract(paths)
    if args.preflight_only:
        result = preflight(paths, target, args.expected_head, contract)
        preflight_target = Path(args.preflight_dir).resolve()
        destination(preflight_target, {**paths, "body_target": target})
        preflight_target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{preflight_target.name}.staging-", dir=preflight_target.parent))
        write_json(staging/"summary.json", result)
        atomic_publish(staging, preflight_target)
    else:
        reviewed_path = Path(args.reviewed_preflight).resolve()
        require(0 < stat_identity(reviewed_path)["bytes"] <= CAPS["maximum_json_bytes"], "reviewed preflight size outside cap")
        reviewed_hash = sha256(reviewed_path)
        require(reviewed_hash == args.reviewed_preflight_sha256, "reviewed preflight hash differs")
        reviewed = read_json(reviewed_path)
        require(reviewed.get("status") == "sports_wallet_samples_preflight_complete", "reviewed preflight incomplete")
        reviewed_identity = {"path": str(reviewed_path), "bytes": reviewed_path.stat().st_size, "sha256": reviewed_hash}
        result = build_run(paths, target, args.expected_head, reviewed, contract,
                           [sys.executable, *(sys.argv if argv is None else argv)], reviewed_identity)
    print(json.dumps({"status": result["status"], "run_dir": str(target), "data_certified": False}, sort_keys=True))


if __name__ == "__main__":
    main()
