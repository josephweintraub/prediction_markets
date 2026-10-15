#!/usr/bin/env python3
"""Independent bounded raw-mean release audit; no coefficient refit or sports scan."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

import duckdb

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from analysis.kaushik_polymarket_replication import build_inputs as inputs
from analysis.kaushik_polymarket_replication import exchange_filter_contract as contract
from analysis.kaushik_polymarket_replication import filtered_inputs as filtered
from analysis.kaushik_polymarket_replication import render_report as report
from scripts import audit_kaushik_exchange_wallets as masks
from scripts import audit_kaushik_replication_saved_scores as saved

CAPS = {"memory_limit": "192GB", "threads": 4, "spill_bytes": 16_000_000_000,
        "minimum_available_ram_bytes": 240_000_000_000, "minimum_free_bytes": 20_000_000_000,
        "maximum_read_bytes": 1_000_000_000_000, "maximum_output_bytes": 1_000_000}
RTOL, ATOL = 2e-8, 1e-10
SOURCE_FILES = ("scripts/audit_kaushik_exchange_filtered_release.py",
    "tests/test_kaushik_exchange_filtered_independent.py",
    "tests/test_kaushik_exchange_wallets_independent_qa.py",
    "analysis/kaushik_polymarket_replication/filtered_inputs.py",
    "analysis/kaushik_polymarket_replication/exchange_filter_contract.py",
    "analysis/kaushik_polymarket_replication/build_inputs.py",
    "analysis/kaushik_polymarket_replication/render_report.py",
    "scripts/audit_kaushik_replication_saved_scores.py",
    "scripts/audit_kaushik_exchange_wallets.py", "scripts/audit_kaushik_full_sample_subset.py",
    "production_guard.py", "docs/methods_reference.md",
    "docs/analysis_specs/kaushik_exchange_wallet_exclusion_v1.md")
SCORE_GATES = ("CR0_score_outer_products", "named_contrasts_SE_CI",
    "supplementary_G_over_G_minus_1_not_CR1", "influence_diagnostics",
    "saved_support_and_complete_grids", "common_duration_and_BUY_partition",
    "saved_sports_seconds_clock_contract")


def require(condition, message):
    inputs.require(condition, "filtered release audit: " + message)


class Reads:
    def __init__(self):
        self.bytes, self.scans = 0, {}

    def charge(self, size, label="metadata"):
        require(type(size) is int and size >= 0 and self.bytes + size <= CAPS["maximum_read_bytes"],
                "read ceiling before " + label)
        self.bytes += size
        self.scans[label] = self.scans.get(label, 0) + size


def read_json(path, digest, reads, label):
    require(re.fullmatch(r"[0-9a-f]{64}", digest or ""), "explicit JSON SHA256 required")
    reads.charge(inputs.stat_identity(path)["bytes"], label)
    return inputs.read_json(path, digest)


def source_snapshot(head):
    require(re.fullmatch(r"[0-9a-f]{40}", head or ""), "committed audit HEAD required")
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=REPO, text=True).strip()
    require(git("rev-parse", "HEAD") == head, "audit source HEAD differs")
    git("ls-files", "--error-unmatch", *SOURCE_FILES)
    require(not git("status", "--porcelain", "--", *SOURCE_FILES), "audit source/dependencies uncommitted")
    return {"head": head, "files": {name: inputs.sha256(REPO / name) for name in SOURCE_FILES}}


def close(actual, expected, label):
    require(type(actual) in (int, float) and math.isfinite(actual) and math.isfinite(expected) and
            math.isclose(actual, expected, rel_tol=RTOL, abs_tol=ATOL), "raw mean mismatch: " + label)


def numerical_checks(data):
    """Check saved numerical gates and their withholding, without a model fit."""
    models = [*data["table2"], *data["table3"]["L_gt1"], *data["table3"]["R_gt1"], data["appendix_a1"]]
    require(len(models) == 12 and len({m["model_id"] for m in models}) == 12, "twelve distinct duration models required")
    checked = []
    for model in models:
        joint = model.get("joint")
        if joint is None:
            require(model["n_observations"] == 0 and model["suppressed"] and
                    "empty_estimation_sample" in model["suppression_reasons"], "unexplained empty model")
            require(all(row["suppressed"] and row.get("CR0") is None and
                        "empty_estimation_sample" in row["suppression_reasons"] for row in model["slopes"]),
                    "empty model has a reported slope")
            checked.append({"model_id": model["model_id"], "status": "empty_withheld"})
            continue
        m = joint["metadata"]
        rank, columns = m["residualized_design_rank"], m["structural_column_count"]
        require(type(rank) is int and type(columns) is int and 0 <= rank <= columns and columns > 0,
                "invalid residualized model rank")
        tails = m.get("residualized_continuous_design_by_tail")
        if tails is not None:
            require(set(tails) == {"D1", "D10"} and
                    all(type(t["rank"]) is int and type(t["continuous_columns"]) is int and
                        0 <= t["rank"] <= t["continuous_columns"] for t in tails.values()) and
                    sum(t["rank"] for t in tails.values()) == rank and
                    sum(t["continuous_columns"] for t in tails.values()) == columns,
                    "tail/stacked residualized rank differs")
        failed = []
        if rank != columns:
            failed.append("rank_deficient_residualized_design")
        else:
            condition = m["condition_number_scaled_gram"]
            require(type(condition) in (int, float) and math.isfinite(condition) and condition > 0,
                    "full-rank condition diagnostic absent")
        absorption = m.get("absorption")
        if model["spec"]["effects"]:
            require(isinstance(absorption, dict) and type(absorption.get("converged")) is bool and
                    type(absorption.get("iterations")) is int and 1 <= absorption["iterations"] <= 100 and
                    absorption.get("tolerance") == 1e-10 and
                    type(absorption.get("maximum_scaled_group_mean")) in (int, float) and
                    math.isfinite(absorption["maximum_scaled_group_mean"]) and
                    absorption["maximum_scaled_group_mean"] >= 0, "projection diagnostic absent or changed")
            if absorption["converged"]:
                require(absorption["maximum_scaled_group_mean"] <= absorption["tolerance"],
                        "false projection convergence")
            else:
                failed.append("categorical_projection_not_converged")
        else:
            require(absorption is None, "unexpected absorption in uncontrolled model")
        if failed:
            require(model["suppressed"] and set(failed) <= set(model["suppression_reasons"]),
                    "failed model rank/projection not withheld")
            require(all(row["suppressed"] and set(failed) <= set(row["suppression_reasons"])
                        for row in model["slopes"]), "failed numerical gate has a reported slope")
        error, tolerance = m["cluster_score_normal_equation_error"], m["cluster_score_normal_equation_tolerance"]
        require(type(tolerance) in (int, float) and math.isfinite(tolerance) and tolerance > 0,
                "normal-equation tolerance absent")
        if error is not None:
            require(type(error) in (int, float) and math.isfinite(error) and error >= 0,
                    "invalid score normal-equation diagnostic")
            if error > tolerance:
                require(all(row["suppressed"] and "cluster_score_normal_equations_failed" in row["suppression_reasons"]
                            for row in model["slopes"]), "failed normal equations have a reported slope")
        else:
            require(bool(failed) or all(row["suppressed"] for row in model["slopes"]),
                    "reported model lacks normal-equation diagnostic")
        checked.append({"model_id": model["model_id"], "rank": rank, "columns": columns,
                        "absorption": absorption, "withheld": model["suppressed"],
                        "normal_equation_error": error, "normal_equation_tolerance": tolerance})
    return checked


def raw_checks(con, data, base, reads, body_bytes):
    """One grouped pass, with C and individual Q computed directly from P and Y."""
    reads.charge(body_bytes, "raw_role_action_bin_means_and_duration_counts")
    values = con.execute("""WITH direct AS (
        SELECT is_maker,side,P,Y,duration_eligible,L,R,
        CASE WHEN P<.1 THEN 1 WHEN P<.2 THEN 2 WHEN P<.3 THEN 3 WHEN P<.4 THEN 4
             WHEN P<.5 THEN 5 WHEN P<.6 THEN 6 WHEN P<.7 THEN 7 WHEN P<.8 THEN 8 WHEN P<.9 THEN 9 ELSE 10 END bin
        FROM analysis_base)
        SELECT is_maker,side,bin,count(*) n,
        fsum(100.0*(CAST(Y AS DOUBLE)-P)) sum_payoff,
        fsum(100.0*(CAST(Y AS DOUBLE)/P-1.0)) sum_return,
        fsum(P) sum_price,fsum(CAST(Y AS DOUBLE)) sum_win,
        count(*) FILTER(WHERE duration_eligible AND bin IN (1,10)) duration_rows,
        count(*) FILTER(WHERE duration_eligible AND bin IN (1,10) AND L>1) lifespan_gt1_rows,
        count(*) FILTER(WHERE duration_eligible AND bin IN (1,10) AND R>1) remaining_gt1_rows
        FROM direct GROUP BY is_maker,side,bin ORDER BY is_maker,side,bin""").fetchall()
    fields = ("is_maker", "side", "bin", "n", "sum_payoff", "sum_return", "sum_price", "sum_win",
              "duration_rows", "lifespan_gt1_rows", "remaining_gt1_rows")
    groups = [dict(zip(fields, row)) for row in values]
    require(all(type(g["is_maker"]) is bool and g["side"] in ("BUY", "SELL") and
                1 <= g["bin"] <= 10 for g in groups), "raw group role/action/bin differs")
    require(sum(g["n"] for g in groups) == base["rows"]["baseline_all_roles"] and
            sum(g["n"] for g in groups if not g["is_maker"]) == base["rows"]["primary_taker"],
            "raw/effective baseline watermark differs")
    primary = [g for g in groups if not g["is_maker"]]
    duration = {key: sum(g[key] for g in primary) for key in
                ("duration_rows", "lifespan_gt1_rows", "remaining_gt1_rows")}
    require(duration == data["sample"]["duration_reconciliation"]["source"] ==
            data["sample"]["duration_reconciliation"]["group_cache"] and
            duration["duration_rows"] == data["sample"]["duration_tail_rows"], "raw common-duration counts differ")
    require(all(m["n_observations"] == duration["duration_rows"] for m in data["table2"]) and
            data["appendix_a1"]["n_observations"] == duration["duration_rows"] and
            all(m["n_observations"] == duration[key] for panel, key in
                (("L_gt1", "lifespan_gt1_rows"), ("R_gt1", "remaining_gt1_rows")) for m in data["table3"][panel]),
            "raw/model duration population differs")
    definitions = {"taker_direction": lambda g: not g["is_maker"],
                   "all_buy": lambda g: g["side"] == "BUY",
                   "maker_buy": lambda g: g["is_maker"] and g["side"] == "BUY",
                   "taker_buy": lambda g: not g["is_maker"] and g["side"] == "BUY"}
    tables = [("table1", data["table1"], definitions["taker_direction"])] + [
        ("a2_" + t["convention"], t, definitions[t["convention"]]) for t in data["appendix_a2"]]
    checks = []
    for label, table, select in tables:
        cells = {}
        for bin_ in range(1, 11):
            selected = [g for g in groups if select(g) and g["bin"] == bin_]
            n = sum(g["n"] for g in selected)
            cells[bin_] = {"n": n, **{key: math.fsum(g[key] for g in selected) / n if n else None
                for key in ("sum_payoff", "sum_return", "sum_price", "sum_win")}}
        require(sum(c["n"] for c in cells.values()) == (data["sample"]["rows"] if label == "table1" else table["counts"]["rows"]),
                "raw convention count differs: " + label)
        for row in table["profile_rows"]:
            cell = cells[row["bin"]]
            require(row["n_observations"] == cell["n"], "raw bin count differs: " + label)
            key = "sum_payoff" if row["outcome"] == "payoff_cents" else "sum_return"
            if not row["suppressed"]:
                close(row["CR0"]["estimate"], cell[key], label + ":D" + str(row["bin"]) + ":" + row["outcome"])
            for field, raw in (("mean_price", "sum_price"), ("win_rate", "sum_win")):
                if field in row and row[field] is not None:
                    close(row[field], cell[raw], label + ":" + field)
        for row in table["gap_rows"]:
            require(row["n_observations"] == cells[1]["n"] + cells[10]["n"], "raw tail-gap count differs")
            if not row["suppressed"]:
                key = "sum_payoff" if row["outcome"] == "payoff_cents" else "sum_return"
                close(row["CR0"]["estimate"], cells[10][key] - cells[1][key], label + ":" + row["outcome"] + ":gap")
        if label != "table1":
            require(table["counts"]["tail_rows"] == cells[1]["n"] + cells[10]["n"], "raw convention tail count differs")
        checks.append({"table": label, "bins": [{"bin": b, **c} for b, c in cells.items()]})
    require(data["sample"]["tail_rows"] == sum(g["n"] for g in primary if g["bin"] in (1, 10)), "raw primary tails differ")
    return {"raw_groups": groups, "profile_checks": checks, "duration_counts": duration}


def admit(args, source):
    reads = Reads()
    wrapper, wrapper_id = read_json(args.filtered_input_dir / "manifest.json", args.filtered_manifest_sha256, reads, "wrapper_manifest")
    # Footer admission charges complete physical-file bytes conservatively.
    files = wrapper.get("reference_files", [])
    require(files and sum(i["stat"]["bytes"] for i in files) <= 100_000_000_000, "bounded physical inventory required")
    reads.charge(sum(i["stat"]["bytes"] for i in files) + 8 * inputs.CAPS["maximum_manifest_bytes"], "logical_input_footer_and_JSON_admission")
    base, base_ids, files, monthly = filtered.load_stage(args.filtered_input_dir,
        args.filtered_manifest_sha256, args.filtered_acceptance_sha256)
    require(base == wrapper and base["source"]["head"] == args.input_source_head and
            base["wallet_filter"] == filtered.EXPECTED_WALLET_FILTER, "accepted frozen filtered input/source differs")
    manifest, mid = read_json(args.estimate_dir / "manifest.json", args.estimate_manifest_sha256, reads, "estimate_manifest")
    receipt, rid = read_json(args.estimate_dir / "acceptance.json", args.estimate_acceptance_sha256, reads, "estimate_receipt")
    data, did = read_json(args.estimate_dir / "estimates.json", receipt.get("estimates_sha256"), reads, "estimate_summary")
    require(not (args.estimate_dir / "failure.json").exists() and
            manifest.get("status") == "estimates_complete" and receipt.get("status") == "estimates_reopened_accepted" and
            receipt.get("manifest_sha256") == mid["sha256"] and receipt.get("source_head") == manifest["source"]["head"] == args.estimate_source_head and
            receipt.get("all_outputs_reopened") is True and manifest.get("estimates_json", {}).get("sha256") == did["sha256"],
            "accepted estimate/source bindings differ")
    require(did["stat"]["bytes"] == manifest["estimates_json"].get("bytes") and
            all(manifest.get("reconciliation", {}).get(key) is True for key in
                ("all_inputs_reopened", "all_outputs_reopened", "common_duration_population",
                 "buy_role_partition", "expected_grids_serialized")), "estimate acceptance gates incomplete")
    report.validate_estimates(data)
    report.validate_sample_scope_binding(data, manifest, receipt)
    require(contract.validate_estimate_filter_binding(data, manifest, receipt) == base["wallet_filter"] and
            manifest["preflight"]["base_manifest"] == base and manifest["preflight"]["base_binding"] == base_ids,
            "estimate effective-input watermark differs")
    saved.count_checks(data, manifest)
    models = numerical_checks(data)
    score, sid = read_json(args.saved_score_audit, args.saved_score_sha256, reads, "saved_score_receipt")
    expected_input = {"directory": str(args.estimate_dir), "manifest_sha256": mid["sha256"],
        "acceptance_sha256": rid["sha256"], "estimates_sha256": did["sha256"],
        "producer_source_head": args.estimate_source_head, "sample_scope": "all_available_dates",
        "wallet_filter": base["wallet_filter"], "filtered_input_binding": data["filtered_input_binding"]}
    require(not (args.saved_score_audit.parent / "failure.json").exists() and
            score.get("schema_version") == "kaushik_replication_saved_score_audit_v2" and
            score.get("status") == "saved_scores_reconciled" and score.get("input") == expected_input and
            score.get("audit_source", {}).get("head") == args.saved_score_source_head and
            all(score.get("checks", {}).get(k) is True for k in SCORE_GATES), "saved-score receipt does not bind these estimates")
    for head in (args.input_source_head, args.estimate_source_head, args.saved_score_source_head):
        require(re.fullmatch(r"[0-9a-f]{40}", head or ""), "explicit producer/audit source HEAD required")
    target = args.run_dir.resolve()
    bindings = [*base_ids.values(), *base["reference_bindings"].values(), mid, rid, did, sid]
    inputs.validate_destination(target, [i["path"] for i in [*files, *bindings]])
    reserve = CAPS["spill_bytes"] + CAPS["minimum_free_bytes"] + CAPS["maximum_output_bytes"]
    free = shutil.disk_usage(target.parent).free
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    ram = int(available[1]) * 1024 if available else 0
    require(free >= reserve and ram >= CAPS["minimum_available_ram_bytes"] and (os.cpu_count() or 0) >= CAPS["threads"], "resource admission fails")
    planned = reads.bytes + 8 * sum(i["stat"]["bytes"] for i in files) + 32 * inputs.CAPS["maximum_manifest_bytes"]
    require(planned <= CAPS["maximum_read_bytes"], "read plan exceeds ceiling")
    return {"schema_version": "kaushik_exchange_filtered_release_preflight_v1", "status": "preflight_complete",
        "source": source, "target": str(target), "base": base, "base_binding": base_ids,
        "estimate_binding": {"manifest": mid, "acceptance": rid, "estimates": did},
        "saved_score_binding": sid,
        "files": files, "monthly_paths": monthly, "bindings": bindings, "estimate_manifest": manifest,
        "data": data, "models": models, "caps": CAPS, "metadata_read_bytes": reads.bytes,
        "planned_read_bytes": planned, "required_free_bytes": reserve,
        "observed_free_bytes": free, "observed_available_ram_bytes": ram}


def run_audit(reviewed, fresh, review_id, command):
    from production_guard import require_production_host
    require_production_host()
    reads = Reads()
    reads.charge(fresh["metadata_read_bytes"], "fresh_metadata_admission")
    reads.charge(8 * inputs.CAPS["maximum_manifest_bytes"], "review_source_and_publication_JSON_reserved")
    require(inputs.read_json(review_id["path"], review_id["sha256"]) == (reviewed, review_id), "reviewed JSON drift")
    for key in fresh:
        if key not in ("observed_free_bytes", "observed_available_ram_bytes"):
            require(reviewed.get(key) == fresh[key], "reviewed preflight drift: " + key)
    target = Path(fresh["target"])
    stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    con, published = None, False
    try:
        for item in fresh["files"]:
            reads.charge(item["stat"]["bytes"], "physical_hash_before")
            require(inputs.stat_identity(item["path"]) == item["stat"] and inputs.sha256(item["path"]) == item["expected_sha256"], "physical input content drift")
        con = duckdb.connect()
        masks.configure(con, stage / "spill")
        filtered.create_analysis_view(con, fresh["base"]["wallet_filter"]["parent_binding"]["directory"],
            fresh["monthly_paths"], fresh["base"]["mask_paths"])
        with inputs.copy_ceiling(CAPS["spill_bytes"]):
            raw = raw_checks(con, fresh["data"], fresh["base"], reads, sum(i["stat"]["bytes"] for i in fresh["files"]))
        con.close(); con = None
        for item in fresh["files"]:
            reads.charge(item["stat"]["bytes"], "physical_hash_after")
            require(inputs.stat_identity(item["path"]) == item["stat"] and inputs.sha256(item["path"]) == item["expected_sha256"], "physical input changed during audit")
        for item in [*fresh["bindings"], review_id]:
            read_json(item["path"], item["sha256"], reads, "binding_reopen_after")
            require(inputs.stat_identity(item["path"]) == item["stat"], "JSON binding stat drift")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "audit source drift")
        require(reads.bytes <= fresh["planned_read_bytes"], "charged reads exceed reviewed plan")
        result = {"schema_version": "kaushik_exchange_filtered_release_audit_v1", "status": "exchange_filtered_release_reconciled",
            "source": fresh["source"], "input_bindings": fresh["bindings"], "reviewed_preflight": review_id,
            "input": {"manifest": fresh["base_binding"]["manifest"], "acceptance": fresh["base_binding"]["acceptance"]},
            "estimates": fresh["estimate_binding"], "saved_score_audit": fresh["saved_score_binding"],
            "wallet_filter": fresh["base"]["wallet_filter"], "filtered_input_binding": fresh["data"]["filtered_input_binding"],
            "source_row_counts": fresh["base"]["rows"], "original_source_row_counts": fresh["base"]["original_rows"],
            "model_diagnostics": fresh["models"], **raw,
            "checks": dict.fromkeys(("raw_Table1_A2_means_counts_and_gaps", "raw_common_duration_and_panel_counts",
                "twelve_model_numerical_gates", "complete_grids_and_withholding", "saved_score_receipt_bound",
                "effective_filtered_input_watermark", "physical_inputs_reopened_unchanged"), True),
            "caps": CAPS, "read_ledger": {"charged_bytes": reads.bytes, "planned_read_bytes": fresh["planned_read_bytes"],
                "ceiling_bytes": CAPS["maximum_read_bytes"], "scans": reads.scans,
                "method": "Complete physical file bytes, including masks, charged before every declared pass; bounded JSON/source reserves."},
            "relative_tolerance": RTOL, "absolute_tolerance": ATOL, "command": command,
            "environment": {"python": sys.version, "duckdb": duckdb.__version__, "platform": sys.platform},
            "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
            "limitations": "Independent raw C/Q means and counts only; no coefficient refit, raw sports timing/window verification, native-action or unique-fill certification."}
        require(len(json.dumps(result, allow_nan=False).encode()) <= CAPS["maximum_output_bytes"], "compact audit output ceiling")
        inputs.write_json(stage / "audit.json", result)
        require(inputs.read_json(stage / "audit.json")[0] == result and
                inputs._directory_bytes(stage) <= CAPS["maximum_output_bytes"] and
                shutil.disk_usage(stage).free >= CAPS["minimum_free_bytes"], "saved output/free-floor gate")
        inputs.atomic_publish(stage, target); published = True
        require(inputs.read_json(target / "audit.json")[0] == result, "published audit differs")
        return result
    except BaseException as error:
        root = target if published else stage
        if not (root / "failure.json").exists():
            inputs.write_json(root / "failure.json", {"status": "filtered_release_incomplete", "reason": str(error)})
        raise
    finally:
        if con is not None:
            con.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("filtered-input-dir", "estimate-dir", "saved-score-audit", "run-dir"):
        parser.add_argument("--" + flag, type=Path, required=True)
    for flag in ("filtered-manifest-sha256", "filtered-acceptance-sha256", "input-source-head",
                 "estimate-manifest-sha256", "estimate-acceptance-sha256", "estimate-source-head",
                 "saved-score-sha256", "saved-score-source-head", "expected-head"):
        parser.add_argument("--" + flag, required=True)
    parser.add_argument("--preflight-dir", type=Path)
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()
    require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production Python differs")
    args.filtered_input_dir, args.estimate_dir = args.filtered_input_dir.resolve(), args.estimate_dir.resolve()
    fresh = admit(args, source_snapshot(args.expected_head))
    if args.preflight_dir:
        require(not args.reviewed_preflight and not args.reviewed_preflight_sha256, "preflight/body modes conflict")
        inputs.validate_destination(args.preflight_dir, [i["path"] for i in fresh["bindings"]])
        args.preflight_dir.mkdir(exist_ok=False)
        inputs.write_json(args.preflight_dir / "manifest.json", fresh)
        print(json.dumps({"status": "preflight_complete", "manifest": str(args.preflight_dir / "manifest.json")}))
        return 0
    require(args.reviewed_preflight and args.reviewed_preflight_sha256, "body requires separately reviewed preflight")
    reviewed, review_id = inputs.read_json(args.reviewed_preflight, args.reviewed_preflight_sha256)
    result = run_audit(reviewed, fresh, review_id, [sys.executable, *sys.argv])
    print(json.dumps({"status": result["status"], "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
