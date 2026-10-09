#!/usr/bin/env python3
"""Render a saved, completed MLB count rebuild; no data scans or estimation."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re

MAX_BYTES = 1024 * 1024
PRODUCER = "scripts/rebuild_mlb_unfiltered_samples.py"
SPEC = "docs/analysis_specs/mlb_unfiltered_rebuild_v1.md"
INPUTS = {"raw", "candidates", "timestamp_provenance", "cache", "phase", "wallet_flags", "old_exact"}
DEFINITIONS = {
    "prefilter": "all distinct candidate fills; no price-range or bot exclusion; invalid payload/amount/timestamp blocks publication",
    "all_trades": "accepted frozen phase market cohort; timestamp <= actual_end_utc; 0 < price < 1; flagged buyers retained",
    "filtered_trades": "same cohort/time; 0.01 < price < 0.99; NOT coalesce(current shared flag.is_nonhuman,false)",
    "time": "exact cached UTC Unix seconds; all pregame history retained without lower cutoff; recorded event end included",
    "observation": "legacy inferred outcome-token BUY per resolved fill, not certified counterparty own action",
    "exclusion_order": ["outside_accepted_market_rows", "post_end_rows", "invalid_sample_price_rows", "accepted_valid_price_rows"],
    "filtered_exclusion_order": ["filtered_extreme_price_exclusions", "filtered_flagged_interior_exclusions", "new_accepted_filtered_rows"],
    "flag_support": "missing/null support counted in all_trades; current null/missing flags retain COALESCEfalse",
}
QA = (
    ("old_exact_full_payload_subset", "Legacy exact payload preserved"),
    ("old_loader_counts_reproduced", "Old accepted-cohort counts reproduced"),
    ("output_full_payload_reopened", "Published output payload reopened"),
    ("exact_timestamps_verified", "Exact timestamps verified"),
    ("source_fill_ids_one_to_one", "Distinct source fill identifiers: one-to-one"),
    ("metadata_unique", "Market metadata joins: unique"),
    ("lowercase_flag_keys_unique", "Shared flag joins: unique wallet keys"),
    ("inputs_hashes_unchanged", "Frozen input identities unchanged"),
    ("filtered_subset_of_all", "Filtered rows are a subset of all rows"),
)
COUNT_KEYS = (
    "old_accepted_all_rows", "old_accepted_filtered_rows", "new_accepted_all_rows", "new_accepted_filtered_rows",
    "restored_all_rows", "restored_filtered_rows", "distinct_candidate_fills", "prefilter_exact_rows", "old_exact_rows",
    "outside_accepted_market_rows", "post_end_rows", "invalid_sample_price_rows", "accepted_valid_price_rows",
    "filtered_extreme_price_exclusions", "filtered_flagged_interior_exclusions",
)
SUPPORT_KEYS = (
    "candidate_markets", "accepted_metadata_markets", "accepted_metadata_events",
    "new_all_markets", "new_all_events", "new_filtered_markets", "new_filtered_events",
    "old_all_markets", "old_all_events", "old_filtered_markets", "old_filtered_events",
)
INDEPENDENT_COUNTS = (set(COUNT_KEYS) - {"distinct_candidate_fills"}) | set(SUPPORT_KEYS) | {
    "accepted_missing_flag_rows", "accepted_null_flag_rows",
}
INDEPENDENT_GATES = {
    "native_id_uniqueness", "full_payload_membership", "exact_cache", "metadata_and_flag_uniqueness",
    "winner_token_mapping", "sample_membership", "sample_enrichment", "attrition",
}


class ReportBlocked(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReportBlocked(message)


def count(value) -> int:
    require(type(value) is int and value >= 0, "counts must be nonnegative integers")
    return value


def fingerprint(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def validate(summary: dict) -> dict:
    require(summary["schema_version"] == 1 and summary["status"] == "mlb_unfiltered_samples_complete",
            "refusing report: completed MLB summary required")
    require(summary["data_certified"] is False and summary["scientific_estimators_rerun"] is False,
            "count repair must not imply canon certification or scientific reruns")
    require(all(summary["reconciliation"].get(key) is True for key, _ in QA), "a required reconciliation gate failed")
    require(all(summary["definitions"][key] == value for key, value in DEFINITIONS.items()),
            "saved sample/cohort/timing definitions differ from the report contract")
    source = summary["source"]
    require(isinstance(source["expected_head"], str) and re.fullmatch(r"[0-9a-f]{40}", source["expected_head"]) is not None
            and source["expected_head"] == source["head_before"] == source["head_after"],
            "committed producer source identity changed")
    files = list(source["files"].values())
    require(all(count(item["bytes"]) > 0 and fingerprint(item["sha256"]) for item in files),
            "producer source fingerprints incomplete")
    for required in (PRODUCER, SPEC):
        require(any(item["path"] == required or item["path"].endswith("/" + required) for item in files),
                "committed producer/spec identity missing")
    require(INPUTS <= set(summary["inputs"]), "frozen input inventory incomplete")
    require(all(count(item["bytes"]) > 0 and fingerprint(item["sha256"])
                for item in summary["inputs"].values()), "frozen input fingerprints incomplete")
    require(set(COUNT_KEYS) | INDEPENDENT_COUNTS | {"raw_candidate_rows", "duplicate_payload_rows"}
            <= set(summary["counts"]), "saved count inventory incomplete")
    c = {key: count(value) for key, value in summary["counts"].items()}
    require(c["raw_candidate_rows"] == c["distinct_candidate_fills"] + c["duplicate_payload_rows"],
            "recorded source deduplication counts do not reconcile")
    require(c["distinct_candidate_fills"] == c["prefilter_exact_rows"], "source-fill/exact-row count mismatch")
    require(c["prefilter_exact_rows"] == c["outside_accepted_market_rows"] + c["post_end_rows"] +
            c["invalid_sample_price_rows"] + c["accepted_valid_price_rows"], "sequential all-sample exclusions do not reconcile")
    require(c["new_accepted_all_rows"] == c["accepted_valid_price_rows"] and
            c["new_accepted_filtered_rows"] + c["filtered_extreme_price_exclusions"] +
            c["filtered_flagged_interior_exclusions"] == c["new_accepted_all_rows"],
            "sequential filtered-sample exclusions do not reconcile")
    require(0 < c["old_accepted_filtered_rows"] <= c["old_accepted_all_rows"] <= c["old_exact_rows"] <= c["prefilter_exact_rows"],
            "legacy support/baseline count mismatch")
    for sample in ("all", "filtered"):
        require(c["old_accepted_" + sample + "_rows"] + c["restored_" + sample + "_rows"] ==
                c["new_accepted_" + sample + "_rows"], "restored count differs from new minus old")
    expected = {"exact_trades.parquet": c["prefilter_exact_rows"], "all_trades.parquet": c["new_accepted_all_rows"],
                "filtered_trades.parquet": c["new_accepted_filtered_rows"]}
    for name, rows in expected.items():
        output = summary["outputs"][name]
        path = Path(output["path"])
        require(path.parts == (name,) and count(output["rows"]) == rows and
                count(output["bytes"]) > 0 and fingerprint(output["sha256"]) and output["schema"],
                "published output metadata does not reconcile")
    artifact = summary.get("summary_artifact", {})
    require(artifact.get("path") == "summary.json" and count(artifact.get("bytes")) > 0 and fingerprint(artifact.get("sha256")),
            "completed build manifest, not compact summary, required")
    return c


def validate_receipt(summary: dict, receipt: dict, manifest_identity: dict) -> None:
    require(receipt.get("schema_version") == 1 and receipt.get("status") == "mlb_unfiltered_saved_artifact_qa_complete"
            and receipt.get("data_certified") is False and receipt.get("scientific_estimators_rerun") is False,
            "completed independent saved-artifact QA required")
    binding = receipt.get("manifest", {})
    require(count(binding.get("bytes")) == manifest_identity["bytes"] and fingerprint(binding.get("sha256"))
            and binding["sha256"] == manifest_identity["sha256"], "independent QA manifest identity mismatch")
    require(receipt.get("expected_head") == summary["source"]["expected_head"] and
            fingerprint(receipt.get("reviewer_source_sha256")), "independent reviewer source identity mismatch")
    gates = receipt.get("gates", {})
    require(set(gates) == INDEPENDENT_GATES and all(value is True for value in gates.values()),
            "independent QA gate missing, extra or failed")
    require(INDEPENDENT_COUNTS <= set(receipt.get("counts", {})), "independent QA count inventory incomplete")
    for key, value in receipt["counts"].items():
        count(value)
        if key in summary["counts"]:
            require(value == summary["counts"][key], "independent QA count differs: " + key)


def number(value: int) -> str:
    return r"\num{" + str(count(value)) + "}"


def render_tex(summary: dict, receipt: dict, manifest_identity: dict) -> str:
    c = validate(summary)
    validate_receipt(summary, receipt, manifest_identity)
    lines = [r"\documentclass[10pt,letterpaper]{article}", r"\usepackage[T1]{fontenc}",
             r"\usepackage[margin=0.7in]{geometry}", r"\usepackage{booktabs,siunitx,threeparttable}",
             r"\sisetup{group-separator={,},group-minimum-digits=4}", r"\setlength{\parindent}{0pt}",
             r"\setlength{\parskip}{5pt}", r"\renewcommand{\arraystretch}{1.12}",
             r"\begin{document}", r"{\Large MLB sample-count restoration}\hfill 9 October 2026\par",
             "Inferred BUY rows on the same accepted MLB cohort and exact UTC timestamps. Added rows existed in retained resolved data "
             "but were discarded by the old extract; its `all trades' branch was already filtered. Source: "
             r"\texttt{rebuild\_mlb\_unfiltered\_samples.py} and its audited manifest.\par",
             r"\subsection*{Accepted-cohort inferred BUY rows}", r"\par\begin{threeparttable}\small",
             r"\begin{tabular}{@{}lrrrr@{}}\toprule",
             r"Sample & Old rows & Rebuilt rows & Added rows & Increase vs old \\\midrule"]
    for label, sample in (("All", "all"), ("Filtered", "filtered")):
        old, new, added = (c[key] for key in ("old_accepted_" + sample + "_rows",
                                           "new_accepted_" + sample + "_rows", "restored_" + sample + "_rows"))
        lines.append(label + " & " + " & ".join(number(value) for value in (old, new, added)) +
                     f" & {100 * added / old:.2f}" + r"\% \\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Increase = added rows / old rows. All retains flagged buyers with $0<P<1$; filtered requires $0.01<P<0.99$ and no current shared flag. All pregame history and the recorded end are included; post-end rows are excluded.",
              r"\end{tablenotes}\end{threeparttable}\par", r"\subsection*{Sequential exclusion accounting}",
              r"\par\begin{threeparttable}\small", r"\begin{tabular}{@{}lr@{}}\toprule",
              r"Population or exclusion & Rows \\\midrule"]
    accounting = (("Candidate fills / prefilter inferred BUYs", "prefilter_exact_rows"),
                  ("Outside accepted market cohort", "outside_accepted_market_rows"),
                  ("After recorded game end", "post_end_rows"), ("Invalid sample price", "invalid_sample_price_rows"),
                  ("All accepted rows", "accepted_valid_price_rows"),
                  ("Interior-price rule exclusions from all", "filtered_extreme_price_exclusions"),
                  ("Current flagged interior-price rows excluded", "filtered_flagged_interior_exclusions"),
                  ("Filtered accepted rows", "new_accepted_filtered_rows"))
    for index, (label, field) in enumerate(accounting):
        if index == 5:
            lines.append(r"\midrule")
        lines.append(label + " & " + number(c[field]) + r" \\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Exclusions are sequential: cohort, end, then $0<P<1$; filtering removes extreme prices before flagged interior-price rows. Invalid native payloads, amounts or timestamps block publication rather than being silently omitted.",
              r"\end{tablenotes}\end{threeparttable}\par", r"\subsection*{Reconciliation checks}",
              r"\par\begin{threeparttable}\small", r"\begin{tabular}{@{}ll@{}}\toprule",
              r"Required check & Result \\\midrule"]
    lines.extend(label + r" & Pass \\" for _, label in QA)
    lines.append(r"Independent saved-artifact review & Pass \\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Frozen labels and inferred BUYs do not certify native own actions, flag correctness or collection completeness. Broader canon remains unrepaired; no FLB, calibration or regression estimates were rerun.",
              r"\end{tablenotes}\end{threeparttable}\par", r"\end{document}"]
    return "\n".join(lines) + "\n"


def unique_object(pairs: list) -> dict:
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON key")
        value[key] = item
    return value


def read_json(path: Path) -> tuple[dict, dict]:
    size = path.stat().st_size
    require(0 < size <= MAX_BYTES, "JSON input exceeds the size gate")
    raw = path.read_bytes()
    require(len(raw) == size, "JSON input changed while reading")
    value = json.loads(raw, object_pairs_hook=unique_object,
                       parse_constant=lambda token: (_ for _ in ()).throw(ReportBlocked(token)))
    require(isinstance(value, dict), "JSON object required")
    return value, {"path": str(path.resolve()), "bytes": size, "sha256": hashlib.sha256(raw).hexdigest()}


def publish(summary_path: Path, qa_receipt_path: Path, destination: Path) -> dict:
    require(not destination.exists(), "immutable report directory already exists")
    summary, summary_identity = read_json(summary_path)
    receipt, receipt_identity = read_json(qa_receipt_path)
    tex = render_tex(summary, receipt, summary_identity).encode()
    manifest = {"schema_version": 1, "status": "complete_saved_count_report", "data_certified": False,
                "scientific_estimators_rerun": False,
                "build_manifest": summary_identity, "qa_receipt": receipt_identity,
                "source": summary["source"], "inputs": summary["inputs"], "data_outputs": summary["outputs"],
                "definitions": summary["definitions"], "counts": summary["counts"], "reconciliation": summary["reconciliation"],
                "independent_qa": {key: receipt[key] for key in
                                   ("expected_head", "reviewer_source_sha256", "counts", "gates")},
                "renderer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "layout": {"planned_maximum_pages": 2, "compiled_or_visually_verified": False, "assets_required": False},
                "output": {"name": "mlb_unfiltered_rebuild.tex", "bytes": len(tex), "sha256": hashlib.sha256(tex).hexdigest()}}
    encoded = (json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(tex) + len(encoded) <= MAX_BYTES, "complete report stage exceeds the size gate")
    destination.mkdir(parents=True, exist_ok=False)
    for name, payload in (("mlb_unfiltered_rebuild.tex", tex), ("manifest.json", encoded)):
        partial = destination / (name + ".partial")
        with partial.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, destination / name)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True, help="completed build manifest.json")
    parser.add_argument("--qa-receipt", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    result = publish(args.summary, args.qa_receipt, args.run_dir)
    print(json.dumps({"status": result["status"], "output": result["output"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
