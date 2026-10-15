"""Immutable parent-base-plus-accepted-mask inputs; never copy trade bodies."""
from __future__ import annotations

from datetime import datetime, timezone
import copy
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow.parquet as pq

from . import build_inputs as inputs
from . import exchange_filter_contract as contract
from scripts import audit_kaushik_exchange_wallets as audit
from scripts import audit_kaushik_full_sample_subset as subset

REPO = Path(__file__).resolve().parents[2]
CAPS = {"memory_limit": "192GB", "threads": 4, "spill_bytes": 16_000_000_000,
        "minimum_available_ram_bytes": 240_000_000_000, "minimum_free_bytes": 20_000_000_000,
        "maximum_output_bytes": 16_000_000, "maximum_read_bytes": 1_000_000_000_000}
SOURCE_FILES = (
    "analysis/kaushik_polymarket_replication/filtered_inputs.py",
    "analysis/kaushik_polymarket_replication/exchange_filter_contract.py",
    "analysis/kaushik_polymarket_replication/build_inputs.py",
    "analysis/kaushik_polymarket_replication/run_estimates.py",
    "analysis/kaushik_polymarket_replication/estimators.py",
    "scripts/build_kaushik_exchange_filtered_inputs.py",
    "scripts/audit_kaushik_exchange_wallets.py", "scripts/audit_kaushik_full_sample_subset.py",
    "tests/test_kaushik_exchange_filtered_inputs.py",
    "tests/test_kaushik_exchange_filtered_independent.py",
    "tests/test_kaushik_exchange_wallets_independent_qa.py", "production_guard.py",
    "tests/test_kaushik_replication_driver.py",
    "docs/analysis_specs/kaushik_exchange_wallet_exclusion_v1.md", "docs/methods_reference.md")
EXPECTED_WALLET_FILTER = {
    "wallet_policy": contract.POLICY, "input_representation": "parent_base_plus_mask",
    "parent_binding": {
        "directory": "/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2",
        "manifest_path": "/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2/manifest.json",
        "manifest_sha256": "72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c",
        "acceptance_path": "/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2/acceptance.json",
        "acceptance_sha256": "e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519"},
    "mask_audit_binding": {
        "directory": "/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4",
        "audit_path": "/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4/audit.json",
        "audit_sha256": "3001812c6534bcf68cbe2597f487a86bd1be1546ce72cd31898f7f11c31ee1fb",
        "acceptance_path": "/mnt/data/runs/2026-10-14_kaushik_exchange_wallet_audit_v4/acceptance.json",
        "acceptance_sha256": "e9a793a8bd5a90087c90aa51cb4e1a88ab905da99736bdc3550f13954eeec183"}}
AUDIT_HEAD = "8e8c1ffb735035df5db391df53aa923b34901c95"
GATES = ("parent_and_masks_reopened_unchanged", "source_locators_validated",
         "filtered_population_reconciled", "support_reopened", "all_source_records_accounted")


def require(condition, message):
    inputs.require(condition, "filtered inputs: " + message)


class ReadLedger:
    def __init__(self):
        self.bytes, self.scans = 0, {}

    def charge(self, size, label="metadata"):
        require(type(size) is int and size >= 0 and self.bytes + size <= CAPS["maximum_read_bytes"],
                "read ceiling before " + label)
        self.bytes += size
        self.scans[label] = self.scans.get(label, 0) + size


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "committed expected HEAD required")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()
    require(git("rev-parse", "HEAD") == expected_head, "source HEAD differs")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    require(not git("status", "--porcelain", "--", *SOURCE_FILES), "source/dependencies uncommitted")
    return {"head": expected_head, "files": {name: inputs.sha256(REPO / name) for name in SOURCE_FILES}}


def make_wallet_filter(parent_dir, parent_manifest_sha, parent_acceptance_sha,
                       audit_dir, audit_sha, audit_acceptance_sha):
    parent, masks = Path(parent_dir).resolve(), Path(audit_dir).resolve()
    value = {"wallet_policy": copy.deepcopy(contract.POLICY), "input_representation": "parent_base_plus_mask",
             "parent_binding": {"directory": str(parent), "manifest_path": str(parent / "manifest.json"),
                 "manifest_sha256": parent_manifest_sha, "acceptance_path": str(parent / "acceptance.json"),
                 "acceptance_sha256": parent_acceptance_sha},
             "mask_audit_binding": {"directory": str(masks), "audit_path": str(masks / "audit.json"),
                 "audit_sha256": audit_sha, "acceptance_path": str(masks / "acceptance.json"),
                 "acceptance_sha256": audit_acceptance_sha}}
    return contract.validate_wallet_filter(value)


def load_references(wallet_filter, ledger=None):
    """Footer/JSON admission only, also usable with tiny accepted fixture stages."""
    ledger = ledger or ReadLedger()
    contract.validate_wallet_filter(wallet_filter)
    p, a = wallet_filter["parent_binding"], wallet_filter["mask_audit_binding"]
    parent = subset.load_stage(p["directory"], p["manifest_sha256"], p["acceptance_sha256"],
                               "all_available_dates", ledger)
    contract.reject_filter_fields(parent["manifest"])
    parent_files = {str(path.relative_to(parent["directory"]))
                    for path in Path(parent["directory"]).rglob("*.parquet")}
    require(parent_files == set(parent["files"]), "parent Parquet inventory differs")
    audit_dir = Path(a["directory"])
    require(audit_dir.is_dir() and not audit_dir.is_symlink() and
            not (audit_dir / "failure.json").exists(), "audit directory invalid or has a failure marker")
    recorded, audit_id = inputs.read_json(a["audit_path"], a["audit_sha256"])
    receipt, receipt_id = inputs.read_json(a["acceptance_path"], a["acceptance_sha256"])
    ledger.charge(audit_id["stat"]["bytes"] + receipt_id["stat"]["bytes"], "audit_JSON")
    require(recorded.get("schema_version") == "kaushik_exchange_wallet_audit_v1" and
            recorded.get("status") == "exchange_wallet_audit_complete" and
            recorded.get("scientific_admission") is True and
            receipt.get("schema_version") == "kaushik_exchange_wallet_acceptance_v1" and
            receipt.get("status") == "exchange_wallet_masks_reopened_accepted" and
            receipt.get("audit_sha256") == audit_id["sha256"] and
            receipt.get("all_masks_reopened") is True and receipt.get("scientific_admission") is True,
            "accepted audit/receipt required")
    # Validate policy types through the shared exact contract, not bool/int equality.
    for value in (recorded, receipt):
        contract.validate_wallet_filter({**wallet_filter, "wallet_policy": value.get("policy")})
    require(receipt.get("source_head") == recorded["source"]["head"] and
            receipt.get("totals") == recorded.get("totals") and
            recorded.get("reference_binding") == parent["bindings"], "audit source/parent/receipt binding differs")
    require(all(recorded.get("checks", {}).get(k) is True for k in
                ("source_mask_locator_and_fields_exact", "reference_multiset_partition", "BUY_role_partition",
                 "source_inputs_reopened_unchanged", "unsupported_eligible_matches_absent")), "audit gates incomplete")
    months = parent["months"]
    require([m.get("month") for m in recorded.get("months", [])] == months, "audit month inventory differs")
    expected_names = {f"year_month={m}/wallet_mask.parquet" for m in months}
    require(set(recorded.get("outputs", {})) == expected_names, "mask inventory differs")
    actual_names = {str(path.relative_to(a["directory"])) for path in Path(a["directory"]).rglob("*.parquet")}
    require(actual_names == expected_names, "unmanifested or missing mask Parquet")
    masks = []
    for month in months:
        name = f"year_month={month}/wallet_mask.parquet"
        path, expected = Path(a["directory"]) / name, recorded["outputs"][name]
        require(audit_dir in path.resolve().parents, "mask path escapes accepted audit")
        stat, parquet = inputs.stat_identity(path), pq.ParquetFile(path)
        ledger.charge(stat["bytes"], "mask_footer")
        require(stat["bytes"] == expected["bytes"] <= audit.CAPS["maximum_mask_file_bytes"] and
                parquet.metadata.num_rows == expected["rows"] and
                parquet.metadata.num_row_groups == expected["row_groups"] and
                parquet.schema_arrow.equals(audit.MASK_SCHEMA) and str(parquet.schema_arrow) == expected["schema"] and
                re.fullmatch(r"[0-9a-f]{64}", expected["sha256"] or ""), "mask footer/schema/count differs")
        require(inputs.stat_identity(path) == stat, "mask changed during footer admission")
        masks.append({"path": str(path), "stat": stat, "expected_sha256": expected["sha256"],
                      "rows": expected["rows"], "schema": expected["schema"],
                      "relative_path": "mask_audit/" + name, "month": month})
    files = [{"path": item["path"], "stat": item["stat"], "expected_sha256": item["info"]["sha256"],
              "rows": item["info"]["rows"], "schema": item["info"]["schema"], "relative_path": name}
             for name, item in sorted(parent["files"].items())]
    bindings = {"parent_manifest": parent["bindings"]["manifest"], "parent_acceptance": parent["bindings"]["acceptance"],
                "mask_audit": audit_id, "mask_acceptance": receipt_id}
    require(recorded.get("exact_no_change", {}).get("cutoff") is True and
            all(v["removed"] == 0 for v in recorded["totals"]["cutoff"].values()), "cutoff identity gate differs")
    return {"parent": parent, "audit": recorded, "reference_bindings": bindings, "files": files + masks,
            "monthly_paths": [parent["files"][f"year_month={m}/base.parquet"]["path"] for m in months],
            "mask_paths": [m["path"] for m in masks]}


def effective_accounting(parent, recorded):
    """Append the sole new exclusion after the parent's original first failure."""
    exclusions = copy.deepcopy(parent["exclusions"])
    for month in recorded["months"]:
        groups = month["eligible_groups"]
        expected_keys = {(r["is_maker"], r["side"]) for r in exclusions[month["month"]] if r["reason"] == "eligible"}
        require({(g["is_maker"], g["side"]) for g in groups} <= expected_keys, "audit introduces a role/action cell")
        additions = []
        for row in exclusions[month["month"]]:
            if row["reason"] != "eligible":
                continue
            matching = [g for g in groups if g["is_maker"] is row["is_maker"] and g["side"] == row["side"]]
            require(sum(g["full_baseline"] for g in matching) == row["rows"], "parent eligible/audit cell differs")
            removed = sum(g["full_baseline"] for g in matching if g["removed"])
            duration = sum(g["full_common_duration"] for g in matching if g["removed"])
            require(type(removed) is int and 0 <= removed <= row["rows"] and
                    type(duration) is int and 0 <= duration <= row["duration_tail_rows"], "negative or excessive removal")
            if removed:
                additions.append({**row, "reason": "exchange_wallet", "rows": removed,
                                  "scope_rows": removed, "duration_tail_rows": duration})
                row["rows"] -= removed
                row["scope_rows"] -= removed
                row["duration_tail_rows"] -= duration
        exclusions[month["month"]] += additions
    retained = recorded["role_totals"]["full"]["baseline"]["retained"]
    rows = {"source": parent["rows"]["source"], "baseline_all_roles": retained["all_roles"],
            "primary_taker": retained["primary_taker"],
            "excluded": parent["rows"]["source"] - retained["all_roles"]}
    require(sum(r["rows"] for group in exclusions.values() for r in group) == rows["source"] and
            sum(r["rows"] for group in exclusions.values() for r in group if r["reason"] == "eligible") == rows["baseline_all_roles"] and
            sum(r["rows"] for group in exclusions.values() for r in group if r["reason"] == "eligible" and r["is_maker"] is False) == rows["primary_taker"],
            "effective source waterfall does not reconcile")
    return rows, exclusions


def create_analysis_view(con, parent_dir, monthly_paths, mask_paths, name="analysis_base"):
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name or ""), "invalid view name")
    require(monthly_paths and len(monthly_paths) == len(mask_paths) and len(set(mask_paths)) == len(mask_paths),
            "complete unique mask paths required")
    for base, mask in zip(monthly_paths, mask_paths):
        require(Path(base).parent.name == Path(mask).parent.name, "base/mask month pairing differs")
    parent_name = name + "_unfiltered_parent"
    inputs.create_analysis_view(con, parent_dir, name=parent_name, explicit_month_paths=monthly_paths)
    paths = "[" + ",".join(inputs.literal(p) for p in mask_paths) + "]"
    con.execute(f"""CREATE OR REPLACE TEMP VIEW {inputs.ident(name)} AS SELECT p.*
        FROM {inputs.ident(parent_name)} p ANTI JOIN read_parquet({paths},hive_partitioning=false) m
        ON p.source_month=m.source_month AND p.source_ordinal=m.source_ordinal AND m.known_exchange_match""")


def validate_locators(con, references, ledger):
    parent = references["parent"]
    sources = {item["month"]: item for item in parent["manifest"]["input_preflight"]["trades"]}
    for month, base, mask in zip(parent["months"], references["monthly_paths"], references["mask_paths"]):
        maximum = sources[month]["rows"]
        ledger.charge(Path(base).stat().st_size + Path(mask).stat().st_size, "locator_and_mask_validity")
        audit.validate_mask(con, mask, month, maximum)
        count, unique, bad = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),count(*) FILTER(WHERE
            source_month IS DISTINCT FROM {inputs.literal(month)} OR source_ordinal IS NULL OR source_ordinal<0
            OR source_ordinal>={maximum}) FROM read_parquet({inputs.literal(base)},hive_partitioning=false)""").fetchone()
        require(count == unique == parent["files"][f"year_month={month}/base.parquet"]["info"]["rows"] and bad == 0,
                "parent locator uniqueness/range differs")


def reconcile_population(con, recorded, ledger, body_bytes):
    projections = []
    tail = "bin IN (1,10)"
    metrics = {"baseline": "true", "tails": tail, "common_duration": "duration_eligible AND " + tail,
               "lifespan_gt1": "duration_eligible AND " + tail + " AND L>1",
               "remaining_gt1": "duration_eligible AND " + tail + " AND R>1"}
    for scope, date in (("full", "true"), ("cutoff", f"timestamp<{inputs.CUTOFF_SECONDS}")):
        for metric, predicate in metrics.items():
            for role, role_pred in (("all_roles", "true"), ("primary_taker", "NOT is_maker"),
                ("all_buy", "side='BUY'"), ("maker_buy", "is_maker AND side='BUY'"),
                ("taker_buy", "NOT is_maker AND side='BUY'")):
                projections.append((scope, metric, role,
                    f"count(*) FILTER(WHERE ({date}) AND ({predicate}) AND ({role_pred}))"))
    ledger.charge(body_bytes, "filtered_population_all_roles_metrics_and_cutoff")
    observed = con.execute("SELECT " + ",".join(p[3] for p in projections) + " FROM analysis_base").fetchone()
    require(all(value == recorded["role_totals"][scope][metric]["retained"][role]
                for (scope, metric, role, _), value in zip(projections, observed)), "filtered population differs from accepted audit")


def reopen_inputs(references, ledger, label):
    for identity in references["reference_bindings"].values():
        ledger.charge(identity["stat"]["bytes"], label + "_JSON")
        require(inputs.read_json(identity["path"], identity["sha256"])[1] == identity, "JSON binding/stat drift")
    for item in references["files"]:
        ledger.charge(item["stat"]["bytes"], label + "_Parquet")
        require(inputs.stat_identity(item["path"]) == item["stat"] and
                inputs.sha256(item["path"]) == item["expected_sha256"], "original/mask content drift")


def preflight(wallet_filter, target, source):
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    require(wallet_filter == EXPECTED_WALLET_FILTER, "frozen accepted generation differs")
    ledger = ReadLedger()
    refs = load_references(wallet_filter, ledger)
    require(refs["parent"]["months"] == list(subset.SOURCE_MONTHS) and refs["audit"]["source"]["head"] == AUDIT_HEAD,
            "frozen 44-month audit source differs")
    require(refs["audit"]["role_totals"]["full"]["baseline"]["retained"]["primary_taker"] == 890_449_688,
            "frozen retained primary count differs")
    inputs.validate_destination(target, [i["path"] for i in refs["files"]] +
                                [i["path"] for i in refs["reference_bindings"].values()])
    reserve = CAPS["maximum_output_bytes"] + CAPS["spill_bytes"] + CAPS["minimum_free_bytes"]
    free = shutil.disk_usage(Path(target).parent).free
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    ram = int(available.group(1)) * 1024 if available else 0
    require(free >= reserve and ram >= CAPS["minimum_available_ram_bytes"] and (os.cpu_count() or 0) >= CAPS["threads"],
            "output/spill/free-floor or RAM/thread admission fails")
    planned = ledger.bytes + 16 * sum(i["stat"]["bytes"] for i in refs["files"]) + 16 * CAPS["maximum_output_bytes"]
    require(planned <= CAPS["maximum_read_bytes"], "read plan exceeds ceiling")
    with duckdb.connect() as con:
        physical = audit.configure(con, Path(target) / "spill")
    return {"schema_version": "kaushik_replication_filtered_preflight_v1", "status": "preflight_complete",
            "target": str(Path(target).resolve()), "source": source, "wallet_filter": wallet_filter,
            "references": refs, "caps": CAPS, "required_free_bytes": reserve, "planned_read_bytes": planned,
            "metadata_read_bytes": ledger.bytes,
            "join_planning_policy": physical, "observed_free_bytes": free, "observed_available_ram_bytes": ram,
            "limits": "JSON/footer admission only; body publishes only logical manifest/receipt; no base/claims copy."}


def build_stage(reviewed, fresh, command, reviewed_identity=None):
    from production_guard import require_production_host
    require_production_host()
    ledger = ReadLedger()
    ledger.charge(fresh.get("metadata_read_bytes", 0), "fresh_metadata_admission")
    # Three current review reopenings, source reopening, final manifest/receipt
    # reads and final admission are reserved before their corresponding reads.
    ledger.charge(8 * inputs.CAPS["maximum_manifest_bytes"], "control_JSON_and_source_reserved")
    require(isinstance(reviewed_identity, dict) and
            inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])[1] == reviewed_identity,
            "reviewed preflight identity required")
    require(inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])[0] == reviewed, "reviewed preflight differs")
    for key in ("schema_version", "status", "target", "source", "wallet_filter", "references", "caps",
                "required_free_bytes", "planned_read_bytes", "join_planning_policy"):
        require(reviewed[key] == fresh[key], "reviewed preflight drift: " + key)
    target, refs = Path(fresh["target"]), fresh["references"]
    inputs.validate_destination(target, [i["path"] for i in refs["files"]] +
                                [i["path"] for i in refs["reference_bindings"].values()])
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    con, published, started = None, False, time.monotonic()
    try:
        reopen_inputs(refs, ledger, "before")
        con = duckdb.connect()
        require(audit.configure(con, stage / "spill") == fresh["join_planning_policy"], "physical policy drift")
        validate_locators(con, refs, ledger)
        create_analysis_view(con, refs["parent"]["directory"], refs["monthly_paths"], refs["mask_paths"])
        body_bytes = sum(i["stat"]["bytes"] for i in refs["files"])
        reconcile_population(con, refs["audit"], ledger, body_bytes)
        rows, exclusions = effective_accounting(refs["parent"]["manifest"], refs["audit"])
        ledger.charge(body_bytes, "filtered_support")
        support = inputs.support_counts(con)
        require(sum(r["rows"] for r in support) == rows["baseline_all_roles"] and
                sum(r["rows"] for r in support if not r["is_maker"]) == rows["primary_taker"] and
                all(r["nonfinite_analytic_outcomes"] == 0 for r in support), "support/analytic gate differs")
        ledger.charge(body_bytes, "support_reopen")
        require(inputs.support_counts(con) == support, "support changed on reopen")
        reopen_inputs(refs, ledger, "after")
        # Reserve conservative complete-file charges for the final footer/JSON
        # admission after publication, which intentionally performs no body scan.
        ledger.charge(body_bytes + 4 * CAPS["maximum_output_bytes"], "published_admission_reserved")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "source drift")
        require(inputs.read_json(reviewed_identity["path"], reviewed_identity["sha256"])[1] == reviewed_identity,
                "reviewed preflight drift before publication")
        parent = refs["parent"]["manifest"]
        manifest = {"schema_version": contract.FILTERED_INPUT_SCHEMA, "status": "filtered_inputs_complete",
            "sample_scope": "all_available_dates", "source": fresh["source"], "wallet_filter": fresh["wallet_filter"],
            "reference_bindings": refs["reference_bindings"], "reference_files": refs["files"], "outputs": {},
            "monthly_paths": refs["monthly_paths"], "mask_paths": refs["mask_paths"],
            "rows": rows, "support": support, "exclusions": exclusions, "original_rows": parent["rows"],
            "original_exclusions": parent["exclusions"], "definitions": parent["definitions"],
            "input_preflight": parent["input_preflight"], "metadata_health": parent.get("metadata_health"),
            "original_inputs": parent["inputs"], "binding": parent["binding"],
            "reviewed_preflight": reviewed_identity, "command": command, "caps": CAPS,
            "join_planning_policy": fresh["join_planning_policy"], "reconciliation": dict.fromkeys(GATES, True),
            "read_ledger": {"charged_bytes": ledger.bytes, "scans": ledger.scans,
                "ceiling_bytes": CAPS["maximum_read_bytes"],
                "method": "Conservative complete-file charges and reserved control/publication admission reads."},
            "wall_seconds": time.monotonic() - started,
            "environment": {"python": sys.version, "duckdb": duckdb.__version__, "platform": sys.platform}}
        require(ledger.bytes <= fresh["planned_read_bytes"], "charged reads exceed reviewed plan")
        inputs.write_json(stage / "manifest.json", manifest)
        require(inputs._directory_bytes(stage) <= CAPS["maximum_output_bytes"] and
                shutil.disk_usage(stage).free >= CAPS["minimum_free_bytes"], "output/free-floor gate")
        require(audit.planning_policy(con) == fresh["join_planning_policy"], "physical policy changed")
        con.close(); con = None
        inputs.atomic_publish(stage, target); published = True
        saved, identity = inputs.read_json(target / "manifest.json")
        require(saved == manifest, "published manifest differs")
        inputs.write_json(target / "acceptance.json", {
            "schema_version": contract.FILTERED_INPUT_ACCEPTANCE_SCHEMA, "status": "filtered_inputs_reopened_accepted",
            "manifest_sha256": identity["sha256"], "source_head": fresh["source"]["head"],
            "sample_scope": "all_available_dates", "wallet_filter": fresh["wallet_filter"], "rows": rows,
            "reconciliation": dict.fromkeys(GATES, True), "accepted_at_utc": datetime.now(timezone.utc).isoformat()})
        require(inputs._directory_bytes(target) <= CAPS["maximum_output_bytes"], "logical stage total output cap")
        load_stage(target, identity["sha256"], inputs.sha256(target / "acceptance.json"))
        return manifest
    except BaseException as error:
        root = target if published else stage
        if not (root / "failure.json").exists():
            inputs.write_json(root / "failure.json", {"status": "filtered_inputs_incomplete", "reason": str(error),
                              "error_type": type(error).__name__, "charged_read_bytes": ledger.bytes})
        raise
    finally:
        if con is not None:
            con.close()


def load_stage(stage, manifest_sha, acceptance_sha):
    """Resolve an accepted logical stage back to its immutable physical inputs."""
    stage = Path(stage)
    require(stage.is_dir() and not stage.is_symlink() and not (stage / "failure.json").exists(), "regular accepted stage required")
    stage = stage.resolve()
    manifest, mid = inputs.read_json(stage / "manifest.json", manifest_sha)
    receipt, rid = inputs.read_json(stage / "acceptance.json", acceptance_sha)
    require(manifest.get("schema_version") == contract.FILTERED_INPUT_SCHEMA and
            manifest.get("status") == "filtered_inputs_complete" and manifest.get("outputs") == {} and
            not list(stage.rglob("*.parquet")), "logical input stage must contain no physical Parquet outputs")
    require(not any(k in value for value in (manifest, receipt) for k in
                    ("wallet_policy", "input_representation", "filtered_input_binding")), "conflicting filtered input aliases")
    require(re.fullmatch(r"[0-9a-f]{40}", manifest.get("source", {}).get("head", "")), "committed filtered source HEAD absent")
    require(receipt.get("schema_version") == contract.FILTERED_INPUT_ACCEPTANCE_SCHEMA and
            receipt.get("status") == "filtered_inputs_reopened_accepted" and receipt.get("manifest_sha256") == manifest_sha and
            receipt.get("source_head") == manifest["source"]["head"] and receipt.get("rows") == manifest["rows"],
            "filtered input acceptance binding differs")
    wallet_filter = contract.validate_wallet_filter(manifest.get("wallet_filter"))
    require(contract.validate_wallet_filter(receipt.get("wallet_filter")) == wallet_filter and
            manifest.get("sample_scope") == receipt.get("sample_scope") == "all_available_dates" and
            all(v.get(k) is True for v in (manifest["reconciliation"], receipt["reconciliation"]) for k in GATES),
            "filtered acceptance policy/gates differ")
    refs = load_references(wallet_filter)
    parent = refs["parent"]["manifest"]
    expected_rows, expected_exclusions = effective_accounting(parent, refs["audit"])
    require(manifest["reference_bindings"] == refs["reference_bindings"] and manifest["reference_files"] == refs["files"] and
            manifest["monthly_paths"] == refs["monthly_paths"] and manifest["mask_paths"] == refs["mask_paths"],
            "filtered physical inventory/binding drift")
    require(manifest["rows"] == expected_rows and manifest["exclusions"] == expected_exclusions and
            manifest["original_rows"] == parent["rows"] and manifest["original_exclusions"] == parent["exclusions"] and
            all(manifest[k] == parent[k] for k in ("definitions", "input_preflight", "binding")) and
            manifest["original_inputs"] == parent["inputs"] and manifest["metadata_health"] == parent.get("metadata_health"),
            "effective population or original definitions drift")
    support = manifest["support"]
    require(isinstance(support, list) and all(type(r.get("is_maker")) is bool and
            r.get("side") in ("BUY", "SELL") and r.get("category") in inputs.CATEGORY_LABELS and
            type(r.get("bin")) is int and 1 <= r["bin"] <= 10 and
            all(type(r.get(k)) is int and r[k] >= 0 for k in ("rows", "clusters", "duration_tail_rows",
                "numerical_amount_anomalies", "nonfinite_analytic_outcomes")) and
            r["clusters"] <= r["rows"] and r["duration_tail_rows"] <= r["rows"] and
            r["nonfinite_analytic_outcomes"] == 0 for r in support), "effective support schema/analytic gate differs")
    require(len({(r["is_maker"], r["side"], r["category"], r["bin"]) for r in support}) == len(support),
            "duplicate effective support cell")
    require(sum(r["rows"] for r in support) == expected_rows["baseline_all_roles"] and
            sum(r["rows"] for r in manifest["support"] if r["is_maker"] is False) == expected_rows["primary_taker"] and
            sum(r["duration_tail_rows"] for r in manifest["support"] if r["is_maker"] is False) ==
            refs["audit"]["role_totals"]["full"]["common_duration"]["retained"]["primary_taker"], "effective support differs")
    return manifest, {"manifest": mid, "acceptance": rid}, refs["files"], refs["monthly_paths"]
