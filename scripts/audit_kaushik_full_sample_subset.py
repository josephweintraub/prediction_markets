#!/usr/bin/env python3
"""Guarded serial equality audit of two accepted saved-input stages, never raw trades."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import duckdb
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from analysis.kaushik_polymarket_replication import build_inputs as inputs

CAPS = {"memory_limit": "64GB", "threads": 4, "spill_bytes": 16_000_000_000,
        "minimum_free_bytes": 20_000_000_000, "maximum_read_bytes": 1_000_000_000_000,
        "maximum_stage_bytes": 32_000_000_000, "maximum_month_file_bytes": 4_000_000_000,
        "maximum_claims_file_bytes": 1_000_000_000, "maximum_output_bytes": 1_000_000}
SOURCE_FILES = ("scripts/audit_kaushik_full_sample_subset.py",
                "tests/test_kaushik_full_sample_subset_audit.py",
                "analysis/kaushik_polymarket_replication/build_inputs.py",
                "production_guard.py", "docs/analysis_specs/kaushik_polymarket_full_sample_v1.md")
FIELDS = ("source_ordinal", "source_month", "claim_code", "recorded_token_code", "timestamp",
          "recorded_price", "P", "Y", "usdcSize", "side", "is_maker", "duration_eligible")
PARTITIONS = ("all_roles", "primary_taker", "all_buy", "maker_buy", "taker_buy")
CUTOFF = 1774396800
SOURCE_MONTHS = ("2022-11", "2022-12", *(f"{year}-{month:02d}" for year in (2023,2024,2025)
                  for month in range(1,13)), *(f"2026-{month:02d}" for month in range(1,7)))


def require(condition, message):
    if not condition:
        raise ValueError("full-sample subset audit blocked: " + message)


class Reads:
    def __init__(self):
        self.bytes = 0

    def charge(self, size):
        require(type(size) is int and size >= 0 and self.bytes+size <= CAPS["maximum_read_bytes"],
                "read ceiling before next pass")
        self.bytes += size


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "committed expected HEAD required")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()
    require(git("rev-parse", "HEAD") == expected_head, "audit HEAD differs")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    require(not git("status", "--porcelain", "--", *SOURCE_FILES), "audit source/dependencies uncommitted")
    require(all(inputs.stat_identity(REPO/name)["bytes"] <= 1_000_000 for name in SOURCE_FILES),
            "audit source dependency size cap")
    return {"head": expected_head, "files": {name: inputs.sha256(REPO/name) for name in SOURCE_FILES}}


def read_bound(path, digest, reads):
    reads.charge(inputs.stat_identity(path)["bytes"])
    return inputs.read_json(path, digest)


def load_stage(folder, manifest_hash, acceptance_hash, policy, reads):
    folder = Path(folder)
    require(folder.is_dir() and not folder.is_symlink(), "regular accepted stage directory required")
    folder = folder.resolve()
    manifest, mid = read_bound(folder/"manifest.json", manifest_hash, reads)
    acceptance, aid = read_bound(folder/"acceptance.json", acceptance_hash, reads)
    require(manifest.get("schema_version") == "kaushik_replication_inputs_v1" and
            manifest.get("status") == "inputs_complete" and not (folder/"failure.json").exists(),
            "complete input manifest without failure required")
    require(acceptance.get("schema_version") == "kaushik_replication_input_acceptance_v1" and
            acceptance.get("status") == "inputs_reopened_accepted" and
            acceptance.get("manifest_sha256") == manifest_hash and
            acceptance.get("output_schema_hash_count_support_reopened") is True and
            acceptance.get("source_head") == manifest["source"]["head"] and
            acceptance.get("rows") == manifest["rows"], "accepted input receipt/source/count binding")
    require(all(manifest["reconciliation"].get(k) is True for k in (
        "all_records_accounted", "archive_multiplicity_preserved", "no_join_fanout",
        "unique_reciprocal_binary_complements", "outputs_reopened")), "input reconciliation gates")
    require(inputs.validate_sample_definitions(manifest.get("definitions", {})) == policy and
            inputs.sample_scope(manifest) == inputs.sample_scope(acceptance) == policy,
            "declared input/receipt sample policy differs")
    contract = manifest.get("input_preflight", {}).get("contract")
    require(isinstance(contract, dict) and inputs.sample_scope(contract) == policy,
            "accepted input contract provenance differs")
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["source"]["head"] or ""), "input source HEAD absent")
    rows = manifest["rows"]
    require(all(type(rows[k]) is int and rows[k] >= 0 for k in
                ("source", "baseline_all_roles", "excluded", "primary_taker")) and
            rows["source"] == rows["baseline_all_roles"]+rows["excluded"] and
            0 <= rows["primary_taker"] <= rows["baseline_all_roles"], "source count partition")
    files, months = {}, []
    for name, info in sorted(manifest["outputs"].items()):
        match = re.fullmatch(r"year_month=(\d{4}-\d{2})/(base|exclusions)\.parquet", name)
        require(name == "claims.parquet" or match is not None, "unexpected saved-input output")
        path = folder/name
        require(not Path(name).is_absolute() and folder in path.resolve().parents,
                "saved-input path escapes stage")
        stat = inputs.stat_identity(path)
        maximum = CAPS["maximum_claims_file_bytes"] if name == "claims.parquet" else CAPS["maximum_month_file_bytes"]
        require(stat["bytes"] == info["bytes"] <= maximum and
                re.fullmatch(r"[0-9a-f]{64}", info["sha256"] or ""), "saved-input bytes/digest cap")
        reads.charge(stat["bytes"])  # Conservative file bytes for footer admission.
        parquet = pq.ParquetFile(path)
        require(parquet.metadata.num_rows == info["rows"] and
                parquet.metadata.num_row_groups == info["row_groups"] and
                str(parquet.schema_arrow) == info["schema"], "saved-input footer/schema/count drift")
        if match and match[2] == "base":
            require(parquet.schema_arrow.names == list(FIELDS), "base analytical/locator field inventory differs")
            months.append(match[1])
        require(inputs.stat_identity(path) == stat, "input changed during footer admission")
        files[name] = {"path": str(path), "stat": stat, "info": info}
    require("claims.parquet" in files and 0 < len(months) <= 44 and
            set(files) == {"claims.parquet", *(f"year_month={m}/{kind}.parquet" for m in months for kind in ("base", "exclusions"))},
            "complete paired monthly input inventory required")
    require(set(manifest["exclusions"]) == set(months) and
            sum(f["info"]["bytes"] for f in files.values()) <= CAPS["maximum_stage_bytes"], "stage inventory/count cap")
    require(all(type(row["rows"]) is int and row["rows"] >= 0 for group in manifest["exclusions"].values() for row in group) and
            sum(row["rows"] for group in manifest["exclusions"].values() for row in group) == rows["source"] and
            sum(row["rows"] for group in manifest["exclusions"].values() for row in group
                if row["reason"] == "eligible") == rows["baseline_all_roles"], "saved exclusion waterfall does not exhaust source/base")
    require(all(type(row["rows"]) is int and row["rows"] >= 0 and type(row["is_maker"]) is bool and
                row["side"] in ("BUY", "SELL") for row in manifest["support"]) and
            sum(row["rows"] for row in manifest["support"]) == rows["baseline_all_roles"] and
            sum(row["rows"] for row in manifest["support"] if not row["is_maker"]) == rows["primary_taker"],
            "saved support does not exhaust accepted role populations")
    for month in months:
        group = manifest["exclusions"][month]
        eligible = sum(row["rows"] for row in group if row["reason"] == "eligible")
        require(eligible == files[f"year_month={month}/base.parquet"]["info"]["rows"] and
                sum(row["rows"] for row in group if row["reason"] != "eligible") ==
                files[f"year_month={month}/exclusions.parquet"]["info"]["rows"], "monthly waterfall/footer count differs")
    if policy == "all_available_dates":
        require(all(row.get("scope_rows") == row["rows"] and "precut_rows" not in row and
                    row["reason"] != "at_or_after_cutoff" for group in manifest["exclusions"].values() for row in group),
                "full-source exclusion waterfall must cover all rows without cutoff masking")
    return {"directory": str(folder), "manifest": manifest, "acceptance": acceptance,
            "bindings": {"manifest": mid, "acceptance": aid}, "files": files, "months": months}


def admit_pair(previous, full):
    require(previous["directory"] != full["directory"] and previous["months"] == full["months"],
            "distinct stages with identical source months required")
    left, right = previous["manifest"], full["manifest"]
    require(left["inputs"] == right["inputs"] and left["rows"]["source"] == right["rows"]["source"],
            "canonical source inventories/hashes differ")
    for key in ("definitions",):
        clean = lambda value: {k:v for k,v in value.items() if k not in ("sample_scope", "cutoff_utc", "cutoff_seconds")}
        require(clean(left[key]) == clean(right[key]), "non-date eligibility/analytical definitions changed")
    contracts = [value["input_preflight"]["contract"] for value in (left,right)]
    caps = [value.get("caps") for value in contracts]
    require(all(type(value) is dict and set(value) == set(inputs.CAPS) for value in caps),
            "complete input resource caps required")
    require(caps[0]["memory_limit"] == "64GB" and caps[1]["memory_limit"] in ("64GB","192GB"),
            "only approved input memory 64GB to 192GB or unchanged 64GB permitted")
    require({k:v for k,v in caps[0].items() if k != "memory_limit"} ==
            {k:v for k,v in caps[1].items() if k != "memory_limit"}, "non-memory input caps differ")
    require({k:v for k,v in contracts[0].items() if k not in ("sample_scope","caps")} ==
            {k:v for k,v in contracts[1].items() if k not in ("sample_scope","caps")},
            "non-date input contracts differ")
    require(set(previous["files"]) == set(full["files"]) and all(
        previous["files"][name]["info"]["schema"] == full["files"][name]["info"]["schema"] for name in previous["files"]),
        "old/full saved-input schemas differ")
    return ([] if caps[1]["memory_limit"] == "64GB" else [{"path":"caps.memory_limit",
        "previous":"64GB","full":"192GB","classification":"approved_resource_only",
        "reason":"input support DISTINCT memory-only retry; scientific contract unchanged"}])


def preflight(previous_dir, previous_manifest, previous_acceptance, full_dir, full_manifest, full_acceptance,
              target, source):
    reads = Reads()
    previous = load_stage(previous_dir, previous_manifest, previous_acceptance, "paper_cutoff", reads)
    full = load_stage(full_dir, full_manifest, full_acceptance, "all_available_dates", reads)
    differences = admit_pair(previous, full)
    target = Path(target).resolve()
    require(not target.exists() and all(target != Path(v["directory"]) and
            target not in Path(v["directory"]).parents and Path(v["directory"]) not in target.parents
            for v in (previous, full)), "new nonoverlapping audit target required")
    planned = 12*sum(f["info"]["bytes"] for v in (previous, full) for f in v["files"].values()) + 16_000_000*10
    require(planned <= CAPS["maximum_read_bytes"], "planned saved-input scan budget exceeded")
    required = CAPS["minimum_free_bytes"]+CAPS["spill_bytes"]+CAPS["maximum_output_bytes"]
    require(shutil.disk_usage(target.parent).free >= required, "spill/output/free-floor admission")
    return {"schema_version": "kaushik_full_sample_subset_preflight_v1", "status": "preflight_complete",
            "source": source, "target": str(target), "previous": previous, "full": full, "caps": CAPS,
            "input_contracts":{k:value["manifest"]["input_preflight"]["contract"]
                               for k,value in (("previous",previous),("full",full))},
            "resource_only_differences":differences,
            "planned_read_bytes": planned, "metadata_admission_charged_bytes": reads.bytes,
            "required_free_bytes": required, "cutoff_seconds": CUTOFF}


def partition_counts(con, relation, predicate="true"):
    conditions = ("true", "NOT is_maker", "side='BUY'", "is_maker AND side='BUY'", "NOT is_maker AND side='BUY'")
    query = ",".join(f"count(*) FILTER(WHERE {predicate} AND {p})" for p in conditions)
    return dict(zip(PARTITIONS, con.execute("SELECT "+query+" FROM "+relation).fetchone()))


def reserve_disk(stage):
    occupied = 0
    for path in (Path(stage)/"spill").rglob("*"):
        if path.is_file() and not path.is_symlink():
            stat = path.stat()
            occupied += min(stat.st_size, getattr(stat, "st_blocks", 0)*512)
    remaining = max(0, CAPS["spill_bytes"]-occupied)
    require(shutil.disk_usage(stage).free >= CAPS["minimum_free_bytes"]+remaining+CAPS["maximum_output_bytes"],
            "remaining spill/output/free-floor reservation")


def audit_subset(reviewed, fresh, reviewed_identity=None):
    """Tiny-fixture accessible worker; production main guards and binds its preflight."""
    require(reviewed == fresh, "reviewed subset preflight/source/input/stat drift")
    require(fresh["caps"] == CAPS and fresh["resource_only_differences"] == admit_pair(fresh["previous"],fresh["full"]) and
            fresh["input_contracts"] == {k:fresh[k]["manifest"]["input_preflight"]["contract"]
                                        for k in ("previous","full")}, "input resource exception/provenance drift")
    if reviewed_identity is not None:
        reopened, identity = inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])
        require(reopened == reviewed and identity == reviewed_identity,
                "reviewed preflight JSON drift")
    stage = Path(tempfile.mkdtemp(prefix="."+Path(fresh["target"]).name+".staging-", dir=Path(fresh["target"]).parent))
    reads, con = Reads(), None
    reads.charge(fresh["metadata_admission_charged_bytes"])
    if reviewed_identity is not None:
        # Main reads the reviewed binding, then this worker reopens it. Charge both
        # conservatively even in tiny tests invoking the worker alone.
        reads.charge(2*reviewed_identity["stat"]["bytes"])
    try:
        for value in (fresh["previous"], fresh["full"]):
            for item in value["files"].values():
                require(inputs.stat_identity(item["path"]) == item["stat"], "accepted input stat drift before hash")
                reads.charge(item["info"]["bytes"])
                require(inputs.sha256(item["path"]) == item["info"]["sha256"], "accepted input fingerprint differs")
        con = duckdb.connect()
        con.execute("SET memory_limit="+inputs.literal(CAPS["memory_limit"]))
        con.execute("SET threads="+str(CAPS["threads"]))
        con.execute("SET max_temp_directory_size="+inputs.literal(str(CAPS["spill_bytes"])+"B"))
        con.execute("SET temp_directory="+inputs.literal(stage/"spill"))
        con.execute("SET preserve_insertion_order=true")
        reserve_disk(stage)
        # Claims are date-independent; prove exact analytical mapping equality.
        claims = [v["files"]["claims.parquet"] for v in (fresh["previous"], fresh["full"])]
        for direction in (claims, claims[::-1]):
            for item in direction: reads.charge(item["info"]["bytes"])
            a,b = [inputs.literal(v["path"]) for v in direction]
            difference = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet({a}) EXCEPT ALL SELECT * FROM read_parquet({b}))").fetchone()[0]
            require(difference == 0, "claim/complement/category/clock map changed")
        months, totals = [], {k:{p:0 for p in PARTITIONS} for k in ("previous", "retained", "added", "full")}
        fields = ",".join(inputs.ident(k) for k in FIELDS)
        for month in fresh["previous"]["months"]:
            reserve_disk(stage)
            name = f"year_month={month}/base.parquet"
            pair = [v["files"][name] for v in (fresh["previous"], fresh["full"])]
            for label,item in zip(("old", "new"), pair):
                con.execute(f"CREATE OR REPLACE TEMP VIEW {label} AS SELECT * FROM read_parquet({inputs.literal(item['path'])},hive_partitioning=false)")
                reads.charge(item["info"]["bytes"])
                n,locators,bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),count(*) FILTER(WHERE
                    source_ordinal IS NULL OR source_ordinal<0 OR source_month IS NULL OR source_month<>{inputs.literal(month)}
                    OR timestamp IS NULL OR timestamp<0 OR is_maker IS NULL OR side IS NULL OR side NOT IN ('BUY','SELL')
                    OR recorded_price IS NULL OR NOT isfinite(recorded_price) OR NOT(recorded_price>0 AND recorded_price<1)
                    OR P IS NULL OR NOT isfinite(P) OR NOT(P>0 AND P<1) OR Y IS NULL OR Y NOT IN (0,1)
                    OR usdcSize IS NULL OR NOT isfinite(usdcSize) OR usdcSize<=0 OR claim_code IS NULL OR recorded_token_code IS NULL
                    OR duration_eligible IS NULL OR P<>CASE WHEN side='SELL' THEN 1-recorded_price ELSE recorded_price END)
                    FROM {label}""").fetchone()
                require(n == locators == item["info"]["rows"] and bad == 0, "base locator/analytical eligibility contract violation")
            reads.charge(pair[0]["info"]["bytes"])
            old = partition_counts(con, "old")
            reads.charge(pair[1]["info"]["bytes"])
            new = partition_counts(con, "new")
            reads.charge(pair[1]["info"]["bytes"])
            retained = partition_counts(con, "new", f"timestamp<{CUTOFF}")
            reads.charge(pair[1]["info"]["bytes"])
            added = partition_counts(con, "new", f"timestamp>={CUTOFF}")
            reads.charge(pair[1]["info"]["bytes"])
            boundary = con.execute(f"SELECT count(*) FROM new WHERE timestamp={CUTOFF}").fetchone()[0]
            reads.charge(pair[0]["info"]["bytes"])
            old_after = con.execute(f"SELECT count(*) FROM old WHERE timestamp>={CUTOFF}").fetchone()[0]
            require(old_after == 0 and all(old[p] == retained[p] and new[p] == retained[p]+added[p] for p in PARTITIONS),
                    "retained/new role projection population identity")
            differences = []
            for a,b in ((f"SELECT {fields} FROM old", f"SELECT {fields} FROM new WHERE timestamp<{CUTOFF}"),
                        (f"SELECT {fields} FROM new WHERE timestamp<{CUTOFF}", f"SELECT {fields} FROM old")):
                reads.charge(sum(v["info"]["bytes"] for v in pair))
                differences.append(con.execute(f"SELECT count(*) FROM ({a} EXCEPT ALL {b})").fetchone()[0])
            require(differences == [0,0], "pre-cutoff source-locator/analytic-fields multiset differs")
            for key,counts in (("previous",old),("retained",retained),("added",added),("full",new)):
                for p in PARTITIONS: totals[key][p] += counts[p]
            months.append({"month":month,"previous":old,"retained":retained,"added":added,"full":new,
                           "exact_cutoff_rows":boundary,"EXCEPT_ALL_old_minus_retained":differences[0],"EXCEPT_ALL_retained_minus_old":differences[1]})
            print(json.dumps({"stage":"subset_month_complete","month":month,"retained_rows":retained["all_roles"],
                              "new_rows":added["all_roles"],"charged_read_bytes":reads.bytes}), flush=True)
        for key,value in (("previous",fresh["previous"]),("full",fresh["full"])):
            require(totals[key]["all_roles"] == value["manifest"]["rows"]["baseline_all_roles"] and
                    totals[key]["primary_taker"] == value["manifest"]["rows"]["primary_taker"], "stage role totals differ from acceptance")
            require(totals[key]["all_buy"] == totals[key]["maker_buy"]+totals[key]["taker_buy"], "BUY role projection partition")
            support = value["manifest"]["support"]
            conditions = (lambda row: True, lambda row: not row["is_maker"], lambda row: row["side"] == "BUY",
                          lambda row: row["is_maker"] and row["side"] == "BUY",
                          lambda row: not row["is_maker"] and row["side"] == "BUY")
            require(all(totals[key][name] == sum(row["rows"] for row in support if predicate(row))
                        for name,predicate in zip(PARTITIONS,conditions)), "accepted support/BUY projection totals differ")
        con.close(); con = None
        for value in (fresh["previous"],fresh["full"]):
            for item in value["files"].values():
                reads.charge(item["info"]["bytes"])
                require(inputs.stat_identity(item["path"]) == item["stat"] and
                        inputs.sha256(item["path"]) == item["info"]["sha256"], "input drift after comparison")
            for kind,binding in value["bindings"].items():
                reopened, identity = read_bound(binding["path"],binding["sha256"],reads)
                require(identity == binding and reopened == value[kind], "input manifest/receipt drift after comparison")
        spill = stage/"spill"
        if spill.exists():
            require(not any(spill.iterdir()), "unexpected residual spill files after close")
            spill.rmdir()
        result = {"schema_version":"kaushik_full_sample_subset_audit_v1","status":"accepted_cutoff_subset_equal",
            "source":fresh["source"],"input_bindings":{k:fresh[k]["bindings"] for k in ("previous","full")},
            "input_contracts":fresh["input_contracts"],"resource_only_differences":fresh["resource_only_differences"],
            "caps":CAPS,"cutoff_utc":"2026-03-25T00:00:00Z","cutoff_seconds":CUTOFF,
            "comparison_fields":list(FIELDS),"months":months,"totals":totals,"read_bytes":reads.bytes,
            "planned_read_bytes":fresh["planned_read_bytes"],
            "read_accounting":"Conservative complete-file bytes charged before every saved-input hash/footer/query pass and bound JSON read, including reviewed preflight rereads; not measured physical I/O. Source-code hash reads and audit output reopen are separately bounded and excluded.",
            "checks":{"same_canonical_sources_and_non_date_contract":True,"claims_map_EXCEPT_ALL_both_directions":True,
                "precut_input_multiset_EXCEPT_ALL_both_directions":True,"source_locator_uniqueness":True,
                "retained_plus_new_role_projection_counts":True,"BUY_partition":True,"accepted_support_counts":True,
                "inputs_reopened_unchanged":True},
            "limitations":"Only accepted saved-input Parquets and bound metadata were read. No raw archive, native APIs or new outcome/timing certification. "
                "New rows are admitted builder outputs, not all old date-excluded rows. Exclusion labels remain builder evidence; raw eligibility is not independently reconstructed."}
        require(reads.bytes <= fresh["planned_read_bytes"], "actual reads exceed admitted plan")
        return result, stage
    finally:
        if con is not None: con.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for kind in ("previous", "full"):
        parser.add_argument("--"+kind+"-dir",type=Path,required=True)
        parser.add_argument("--"+kind+"-manifest-sha256",required=True)
        parser.add_argument("--"+kind+"-acceptance-sha256",required=True)
    parser.add_argument("--expected-head",required=True)
    parser.add_argument("--run-dir",type=Path,required=True)
    parser.add_argument("--preflight-dir",type=Path)
    parser.add_argument("--reviewed-preflight",type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    source = source_snapshot(args.expected_head)
    inputs.validate_destination(args.run_dir,[args.previous_dir,args.full_dir])
    fresh = preflight(args.previous_dir,args.previous_manifest_sha256,args.previous_acceptance_sha256,
        args.full_dir,args.full_manifest_sha256,args.full_acceptance_sha256,args.run_dir,source)
    require(fresh["previous"]["months"] == list(SOURCE_MONTHS), "production requires same admitted 44 source months")
    if args.preflight_dir:
        require(not args.reviewed_preflight and not args.reviewed_preflight_sha256,"preflight/body modes exclusive")
        inputs.validate_destination(args.preflight_dir,[args.previous_dir,args.full_dir,args.run_dir])
        args.preflight_dir.mkdir(exist_ok=False)
        inputs.write_json(args.preflight_dir/"manifest.json",fresh)
        print(json.dumps({"status":"preflight_complete","planned_read_bytes":fresh["planned_read_bytes"],"required_free_bytes":fresh["required_free_bytes"]}))
        return 0
    require(args.reviewed_preflight and args.reviewed_preflight_sha256,"body requires reviewed preflight binding")
    reviewed,identity = inputs.read_json(args.reviewed_preflight,args.reviewed_preflight_sha256)
    result,stage = audit_subset(reviewed,fresh,identity)
    require(source_snapshot(args.expected_head) == source,"audit source drift")
    result["command"] = [sys.executable,str(Path(__file__).resolve()),*(sys.argv[1:] if argv is None else argv)]
    result["reviewed_preflight"] = identity
    raw=(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+"\n").encode()
    require(len(raw)<=CAPS["maximum_output_bytes"] and shutil.disk_usage(stage).free>=CAPS["minimum_free_bytes"],"audit output/free-floor cap")
    inputs.write_json(stage/"audit.json",result)
    inputs.atomic_publish(stage,args.run_dir)
    require((args.run_dir/"audit.json").read_bytes()==raw,"published subset audit differs")
    print(json.dumps({"status":result["status"],"run_dir":str(args.run_dir),"read_bytes":result["read_bytes"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
