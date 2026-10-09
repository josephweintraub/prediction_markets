#!/usr/bin/env python3
"""Render the completed wallet diagnosis from compact, saved evidence only.

No DuckDB, production data, network access, or TeX compiler is used. The census
summary is a completion barrier and must have an independently verified transfer
receipt. Output stages are immutable; manifest.json is published last.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any

MAX_JSON_BYTES = 1024 * 1024
COMPLETE = "published_pair_census_complete"
FROZEN_BLOB = "19bacf3c87494b782ca1c87213f0e8d72191ecb5"
FROZEN_SQL = "be2b10e0f338ae9b9adec236d64e56ae4f2a4dbcf401bfaa67628bfed8402d0f"
FROZEN_BOT_SHA = "52ee90215b81346852fa775cc1f9df6d72b7b7d5dcbc6ef357c1b7e379fb42fe"
FLAG_PROBE_SHA = "02bcf8f999ca69701ea72decfab685d9f51afb8632ee33255f3eeb5393354780"
FIELDS = {"proxyWallet", "timestamp", "conditionId", "usdcSize", "price", "side",
          "outcome", "eventSlug", "is_maker", "counterparty", "year_month"}
MODES = ("full_label", "label_omitted_diagnostic")
CATEGORIES = ("correct_only", "copied_only", "both_compatible", "neither")
REPORT_NAMES = (
    "Sep20 multisport FLB time regressions v3, filtered and all trades",
    "Sep20 multisport terminal-reversal audit v2",
    "Sep20 ATP swing audit v2",
    "Oct2 sports profit-taking comparison",
    "Oct3 terminal-pattern alternatives",
    "Oct2 tennis timing and terminal maker sequences",
)


class ReportBlocked(ValueError):
    """Evidence is incomplete, inconsistent, or outside this frozen report."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReportBlocked(message)


def count(value: Any) -> int:
    require(type(value) is int and value >= 0, "count must be a nonnegative integer")
    return value


def canonical_months() -> list[str]:
    return [f"{year:04d}-{month:02d}" for year in range(2022, 2027)
            for month in range(1, 13) if "2022-11" <= f"{year:04d}-{month:02d}" <= "2026-06"]


def unique_object(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> tuple[dict, dict]:
    size = path.stat().st_size
    require(0 < size <= MAX_JSON_BYTES, f"compact JSON size gate failed: {path}")
    raw = path.read_bytes()
    require(len(raw) == size, f"input changed during read: {path}")
    value = json.loads(raw, object_pairs_hook=unique_object,
                       parse_constant=lambda token: (_ for _ in ()).throw(ReportBlocked(token)))
    require(isinstance(value, dict), f"JSON object required: {path}")
    return value, {"path": str(path.resolve()), "bytes": len(raw),
                   "sha256": hashlib.sha256(raw).hexdigest()}


def validate_runtime(report: dict, version: str) -> dict:
    require(report["environment"]["duckdb_version"] == version, "runtime version mismatch")
    require(report["source_snapshot"]["git_blob"] == FROZEN_BLOB and
            report["source_snapshot"]["sql_template_sha256"] == FROZEN_SQL,
            "runtime does not use the frozen Stage 6 SQL")
    require(report["production_inputs_read"] is False and
            report["production_builder_imported_or_executed"] is False,
            "runtime evidence must be synthetic-only")
    require(report["status"] == ("complete_synthetic_default_discrepancy" if version == "1.5.0"
                                 else "complete_synthetic_default_matches"), "runtime incomplete")
    require(report["control_failure_cases"] == [], "runtime control failed")
    require(len(report["input_fixture"]["resolved_rows"]) == 2 and
            report["input_fixture"]["all_wallet_pairs_distinct"] is True, "runtime fixture changed")
    by_key = {}
    for case in report["cases"]:
        key = (case["optimizer"], case["timestamp_mode"], case["threads"])
        require(key not in by_key, "duplicate runtime case")
        by_key[key] = case
        if case["status"] == "optimizer_unavailable":
            require(version == "1.4.4" and key[0] == "common_subplan_disabled",
                    "unexpected unavailable runtime control")
            continue
        require(case["status"] == "complete", "runtime case incomplete")
        observed, expected = case["output_rows"], case["expected_rows"]
        require(len(observed) == len(expected) == 4, "runtime output support changed")
        require(all(set(row) == FIELDS for row in observed + expected), "runtime field set changed")
        actual_match = observed == expected
        require(case["exact_rows_match"] is actual_match, "runtime match flag inconsistent")
        failing = version == "1.5.0" and key[0:2] == ("default", "cached")
        require(actual_match is not failing, "unexpected runtime result for frozen fixture")
        if failing:
            for actual, intended in zip(observed, expected):
                changes = {field for field in FIELDS if actual[field] != intended[field]}
                require(changes == (set() if intended["is_maker"] else
                                    {"proxyWallet", "counterparty"}),
                        "runtime discrepancy is not confined to non-maker wallet fields")
                if not intended["is_maker"]:
                    require(actual["proxyWallet"] == intended["counterparty"] and
                            actual["counterparty"] == intended["proxyWallet"],
                            "runtime discrepancy is not the copied maker pair")
    required = {(optimizer, timestamp, threads) for optimizer in
                ("default", "common_subplan_disabled", "all_disabled")
                for timestamp in ("cached", "approx") for threads in (1, 4)}
    require(set(by_key) == required, "runtime twelve-case matrix incomplete")
    defaults = sorted(case["name"] for key, case in by_key.items()
                      if case["status"] == "complete" and not case["exact_rows_match"])
    require(sorted(report["default_discrepancy_cases"]) == defaults,
            "runtime discrepancy summary inconsistent")
    if version == "1.5.0":
        require("__common_subplan" in by_key[("default", "cached", 1)]["explain"]["physical_plan"]
                and "__common_subplan" not in
                by_key[("common_subplan_disabled", "cached", 1)]["explain"]["physical_plan"],
                "saved plans do not establish the common-subplan control")
    return by_key


def validate_metrics(record: dict) -> None:
    for name in ("root", "clean"):
        support = record["support"][name]
        total, makers, nonmakers = (count(support[key]) for key in
                                    ("row_count", "maker_rows", "nonmaker_rows"))
        require(makers + nonmakers == total and support["missing_role_rows"] == 0 and
                support["invalid_rows"] == 0, "census role/integrity reconciliation failed")
        for mode in MODES:
            metrics = record[name][mode]
            require(metrics["maker_rows"] == makers and metrics["nonmaker_rows"] == nonmakers,
                    "census pair support differs from row support")
            for hypothesis in ("correct", "copied"):
                matched = count(metrics["matched_" + hypothesis])
                require(matched + count(metrics["excess_observed_" + hypothesis]) == nonmakers and
                        matched + count(metrics["missing_observed_" + hypothesis]) == makers,
                        "census capacity reconciliation failed")
            for field in ("common_classes", "maker_rows", "nonmaker_rows"):
                require(sum(count(metrics["compatibility"][cat][field]) for cat in CATEGORIES)
                        == count(metrics[field]), "census compatibility categories do not reconcile")
            require(count(metrics["observed_shared_candidate_capacity"]) <=
                    min(count(metrics["shared_candidate_capacity"]), nonmakers),
                    "census shared capacity exceeds support")
    cleaning = record["cleaning"]
    root, clean = (record["support"][name]["row_count"] for name in ("root", "clean"))
    surplus = count(cleaning["root_value_row_surplus"])
    require(count(cleaning["root_distinct_rows"]) + surplus == root and
            root - surplus - count(cleaning["expected_clean_only_rows"]) +
            count(cleaning["clean_only_rows"]) == clean, "root DISTINCT/clean count law failed")


def integer_leaves(record: dict, prefix: tuple = ()) -> dict:
    result = {}
    for key, value in record.items():
        path = prefix + (key,)
        if isinstance(value, dict):
            result.update(integer_leaves(value, path))
        elif type(value) is int:
            result[path] = count(value)
    return result


def validate_census(summary: dict, receipt: dict, identity: dict) -> None:
    require(summary["status"] == COMPLETE and summary["census"]["status"] == COMPLETE,
            "refusing final report: census incomplete")
    census = summary["census"]
    require(census["final_input_identity_reopened"] is True, "final input identity not reopened")
    months = canonical_months()
    require(census["completed_months"] == months and sorted(census["months"]) == months,
            "census must complete all 44 frozen months")
    require(receipt["status"] == "verified_compact_pair_census_transfer" and
            receipt["production_exit_code"] == 0 and
            receipt["canonical_head"] == summary["expected_head"], "census transfer not verified")
    remote = receipt["remote_manifest"]
    require(remote["bytes"] == summary["manifest_bytes"] and
            remote["sha256"] == summary["manifest_sha256"] and remote["downloaded"] is False,
            "remote full manifest identity mismatch")
    require(re.fullmatch(r"[0-9a-f]{64}", remote["sha256"]) is not None and
            re.fullmatch(r"[0-9a-f]{40}", summary["expected_head"]) is not None,
            "invalid census provenance fingerprint")
    matches = [item for item in receipt["artifacts"] if
               Path(item["local_path"]).resolve() == Path(identity["path"])]
    require(len(matches) == 1 and matches[0]["bytes"] == identity["bytes"] and
            matches[0]["sha256"] == identity["sha256"] and
            matches[0]["remote_local_equal"] is True, "compact summary transfer identity mismatch")
    global_record = census["global"]
    validate_metrics(global_record)
    sums: dict[tuple, int] = {}
    for month in months:
        validate_metrics(census["months"][month])
        for path, value in integer_leaves(census["months"][month]).items():
            sums[path] = sums.get(path, 0) + value
    require(sums == integer_leaves(global_record), "monthly integer counts do not sum to global census")
    for name in ("root", "clean"):
        require(global_record["support"][name]["row_count"] == summary["footer_rows"][name],
                "global rows differ from frozen footers")
    require(census["root_distinct_to_clean_reconciled"] is
            (global_record["cleaning"]["failed_full11_reconciliation_leaves"] == 0),
            "cleaning reconciliation status inconsistent")


def validate_provenance(lineage: dict, history: dict) -> None:
    require(lineage["status"] == "source_and_compact_manifest_review_complete_with_provenance_unknowns"
            and history["status"] == "complete_bounded_source_log_and_footer_review",
            "source/lineage review incomplete")
    require(tuple(row["report"] for row in lineage["report_lineage"]) == REPORT_NAMES,
            "report lineage inventory changed; review reader-facing labels")
    require(all(row["input_class"] and row["wallet_usage"] and row["reconciliation_required"]
                for row in lineage["report_lineage"]), "report lineage evidence incomplete")
    require(lineage["shared_sports_flags"]["producer_status"] == "unverified" and
            history["wallet_flags_provenance"]["sports_artifact_producer"] == "unknown" and
            history["wallet_flags_provenance"]["sports_artifact_wrong_labels_proven"] is False,
            "flag provenance changed; reader-facing interpretation requires review")
    require(history["verified_source"]["builder"]["git_blob"] == FROZEN_BLOB and
            history["pre_last_backfill_backup"]["footer"]["created_by"] ==
            "DuckDB version v1.5.0 (build 3a3967aa81)" and
            history["pre_last_backfill_backup"]["footer"]["data_bodies_read"] is False and
            history["historical_runtime_identity"]["unknown"], "historical source boundary changed")
    mlb = lineage["mlb_upstream_filter_review"]
    require(mlb["status"] == "confirmed_source_and_compact_artifact_restriction_no_result_impact_measurement"
            and mlb["all_trades_restores_upstream_exclusions"] is False and
            "0.01 < price < 0.99" in mlb["upstream_filters"] and
            "exclude buyer proxyWallet with learnability/cache is_nonhuman=true" in mlb["upstream_filters"],
            "MLB upstream filter boundary changed")
    counts = mlb["build_counts"]
    require(count(counts["distinct_fills"]) - count(counts["price_exclusions"]) -
            count(counts["bot_exclusions"]) == count(counts["output_buy_rows"]),
            "MLB recorded build counts do not reconcile")
    collision = lineage["existing_native_collision_evidence"]
    require(collision["status"] == "bounded_positive_reconstructed_value_collision_evidence_not_clean_removal_attribution"
            and collision["parent_status_preserved"] == "bounded_reconciliation_incomplete"
            and collision["parent_audit_exit_code"] == 2 and set(collision["value_fields"]) == FIELDS,
            "native collision evidence boundary changed")
    require(collision["native_role_identity"] ==
            ["lower(exchange_address)", "transaction_hash", "log_index", "is_maker"] and
            [window["name"] for window in collision["windows"]] == ["pre_cutoff", "post_migration"],
            "native collision identity/windows changed")
    for window in collision["windows"]:
        duration = (datetime.fromisoformat(window["end_utc_exclusive"].replace("Z", "+00:00")) -
                    datetime.fromisoformat(window["start_utc"].replace("Z", "+00:00"))).total_seconds()
        require(duration == 60 and window["clean_removal_attribution_valid"] is False and
                count(window["expanded_to_root_left_only_rows"]) > 0 and
                count(window["expanded_to_root_right_only_rows"]) > 0,
                "native collision failed reconciliation/attribution boundary changed")
        require(count(window["equal_value_distinct_native_groups"]) > 0 and
                count(window["distinct_native_role_value_surplus"]) >=
                window["equal_value_distinct_native_groups"] and
                count(window["row_count"]) - count(window["value_groups"]) ==
                window["distinct_native_role_value_surplus"] + count(window["same_native_role_value_surplus"]),
                "native collision saved counts do not reconcile")


def tex_escape(value: Any) -> str:
    escapes = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$",
               "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}",
               "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(escapes.get(char, char) for char in str(value))


def validate_flag_mechanism(evidence: dict) -> dict:
    """Check saved synthetic evidence; never execute or estimate flag behavior."""
    require(evidence["status"] == "synthetic_mechanism_complete" and
            evidence["data_certified"] is False and
            evidence["resource_contract"]["rows_per_connection"] == 20,
            "synthetic flag mechanism incomplete")
    hashes = evidence["source_sha256"]
    require(hashes["analysis/bot_filter.py"] == FROZEN_BOT_SHA and
            hashes["scripts/audit_polymarket_wallet_flag_mechanism.py"] == FLAG_PROBE_SHA,
            "synthetic flag source fingerprint changed")
    require(set(evidence["cases"]) == {"fast_100_seconds", "slow_300_seconds"},
            "synthetic flag controls incomplete")
    wallets = ("synthetic-wallet-A", "synthetic-wallet-B")
    for name, spacing in (("fast_100_seconds", 100), ("slow_300_seconds", 300)):
        case = evidence["cases"][name]
        records = case["source_records"]
        require(case["spacing_seconds"] == spacing and len(records) == 10 and
                len({row["source_record_id"] for row in records}) == 10 and
                len({row["token_id"] for row in records}) == 10,
                "synthetic flag source support changed")
        require(all(row["timestamp"] == records[0]["timestamp"] + index * spacing and
                    (row["maker"], row["taker"], row["maker_side"], row["cash"], row["price"]) ==
                    (*wallets, "BUY", 1.0, 0.5) for index, row in enumerate(records)),
                "synthetic flag fixed timing/action fixture changed")
        fixtures = case["exact_expanded_fixtures"]
        keys = sorted((FIELDS | {"source_record_id"}) - {"proxyWallet", "counterparty"})
        correct, copied = (fixtures[key] for key in ("correct_swapped", "copied_pair"))
        require(len(correct) == len(copied) == 20 and
                all(set(row) == FIELDS | {"source_record_id"} for row in correct + copied),
                "synthetic flag expanded support changed")
        require(Counter(tuple(row[key] for key in keys) for row in correct) ==
                Counter(tuple(row[key] for key in keys) for row in copied),
                "synthetic flag nonwallet fields are not conserved")
        source_by_id = {record["source_record_id"]: record for record in records}
        for construction, rows in (("correct_swapped", correct), ("copied_pair", copied)):
            require(Counter((row["source_record_id"], row["is_maker"]) for row in rows) ==
                    Counter({(record["source_record_id"], role): 1 for record in records
                             for role in (True, False)}), "synthetic flag source/role conservation failed")
            for row in rows:
                source = source_by_id[row["source_record_id"]]
                payload = {"timestamp": source["timestamp"], "conditionId": source["token_id"],
                           "usdcSize": source["cash"], "price": source["price"],
                           "outcome": source["outcome"], "eventSlug": source["event_label"],
                           "year_month": datetime.fromtimestamp(source["timestamp"], timezone.utc).strftime("%Y-%m")}
                require(all(row[field] == value for field, value in payload.items()),
                        "synthetic flag expanded payload differs from its source record")
            require(all((row["proxyWallet"], row["counterparty"], row["side"]) ==
                        ((*wallets, "BUY") if row["is_maker"] else
                         (*wallets, "SELL") if construction == "copied_pair" else
                         (*reversed(wallets), "SELL")) for row in rows),
                    "synthetic flag wallet construction changed")
            result = case["results"][construction]
            a, b = (result["wallets"][wallet] for wallet in wallets)
            require(result["expanded_row_count"] == result["summary"]["total_trades"] == 20 and
                    result["gross_recorded_cash"] == 20.0 and
                    (a["n_trades"], b["n_trades"]) ==
                    ((20, 0) if construction == "copied_pair" else (10, 10)),
                    "synthetic flag builder support not conserved")
            expected_flag = spacing == 100 and construction == "copied_pair"
            expected_medians = ((0.0, None) if construction == "copied_pair" else (100.0, 100.0))
            if spacing == 300:
                expected_medians = (None, None)
            require((a["median_iti"], b["median_iti"]) == expected_medians and
                    a["is_nonhuman"] is expected_flag and a["flag_a_definite"] is expected_flag and
                    a["present_in_wallet_flags"] is True and
                    b["present_in_wallet_flags"] is (construction == "correct_swapped") and
                    b["is_nonhuman"] is (False if construction == "correct_swapped" else None),
                    "saved synthetic flag outcome differs from the frozen mechanism")
    return evidence["cases"]["fast_100_seconds"]


def number(value: int) -> str:
    return r"\num{" + str(count(value)) + "}"


def capacity(value: int, denominator: int) -> str:
    percentage = "not defined" if denominator == 0 else f"{100 * value / denominator:.2f}\\%"
    return number(value) + " (" + percentage + ")"


def render_tex(reports: dict[str, dict], summary: dict, lineage: dict, history: dict,
               flag_evidence: dict) -> str:
    """Render validated saved quantities; no data estimation occurs here."""
    matrices = {version: validate_runtime(report, version) for version, report in reports.items()}
    require(set(reports) == {"1.4.4", "1.5.0", "1.5.6"}, "three saved runtime versions required")
    require(all(report["input_fixture"]["resolved_rows"] == reports["1.5.0"]["input_fixture"]["resolved_rows"]
                for report in reports.values()), "cross-version synthetic fixtures differ")
    validate_provenance(lineage, history)
    fast_flags = validate_flag_mechanism(flag_evidence)
    global_record = summary["census"]["global"]
    lines = [r"\documentclass[10pt,letterpaper]{article}",
             r"\usepackage[T1]{fontenc}", r"\usepackage[margin=0.7in]{geometry}",
             r"\usepackage{booktabs,array,siunitx,threeparttable}",
             r"\sisetup{group-separator={,},group-minimum-digits=4}",
             r"\setlength{\parindent}{0pt}", r"\setlength{\parskip}{5pt}",
             r"\setlength{\tabcolsep}{5pt}", r"\renewcommand{\arraystretch}{1.12}",
             r"\begin{document}", r"{\Large Polymarket wallet-row diagnosis}\hfill 8 October 2026",
             r"\smallskip", "Frozen Stage 6 SQL runtime tests, exhaustive published-row capacities, and report-input lineage.",
             r"\subsection*{Correct source, incorrect runtime projection}",
             "The frozen source explicitly swaps wallet and counterparty for the synthetic non-maker row. "
             "DuckDB 1.5.0 instead copies the maker pair under the default optimizer with the cached-timestamp join. "
             "The other nine output fields remain correct in both synthetic BUY and SELL examples.",
             r"\begin{threeparttable}", r"\small",
             r"\begin{tabular}{@{}llll@{}}\toprule",
             r"Synthetic BUY example & Side & Expected wallet / counterparty & Observed in 1.5.0 \\\midrule"]
    case = matrices["1.5.0"][("default", "cached", 1)]
    rows = [(a, e) for a, e in zip(case["output_rows"], case["expected_rows"])
            if e["conditionId"] == case["expected_rows"][0]["conditionId"]]
    for actual, intended in rows:
        label = "Maker" if intended["is_maker"] else "Synthetic non-maker"
        pair = lambda row: tex_escape(row["proxyWallet"].removeprefix("wallet_")) + " / " + tex_escape(row["counterparty"].removeprefix("wallet_"))
        lines.append(f"{label} & {tex_escape(intended['side'])} & {pair(intended)} & {pair(actual)} " + r"\\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item A and B are distinct synthetic wallets, not identified native executions. Opposite-side expansion does not prove a counterparty's own economic action.",
              r"\end{tablenotes}\end{threeparttable}", r"\subsection*{Version and optimizer controls}",
              r"\begin{threeparttable}\small",
              r"\begin{tabular}{@{}llcccc@{}}\toprule",
              r"DuckDB & Platform & \multicolumn{3}{c}{Cached timestamp join} & No join \\",
              r"\cmidrule(lr){3-5} & & Default & Common subplan off & All optimizers off & Default \\\midrule"]
    for version in ("1.4.4", "1.5.0", "1.5.6"):
        report, matrix = reports[version], matrices[version]
        cells = []
        for optimizer, timestamp in (("default", "cached"), ("common_subplan_disabled", "cached"),
                                     ("all_disabled", "cached"), ("default", "approx")):
            cases = [matrix[(optimizer, timestamp, threads)] for threads in (1, 4)]
            cells.append("Unavailable" if cases[0]["status"] == "optimizer_unavailable" else
                         f"{sum(item['exact_rows_match'] for item in cases)}/2 match")
        lines.append(f"{version} & {tex_escape(report['environment']['system'])} & " + " & ".join(cells) + r" \\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Each cell covers one- and four-thread runs, each comparing all 11 fields in four expanded rows from two synthetic source records. The frozen SQL is unchanged; bounded memory and thread settings differ from the historical writer. No production writer or repair was run.",
              r"\end{tablenotes}\end{threeparttable}",
              "Disabling only common-subplan optimization removes the 1.5.0 error. Its saved plan uses a shared subplan; the disabled control does not. "
              "Version 1.5.6 passes this fixture, not a certification of production data.",
              "The preserved March backup footer identifies the same DuckDB 1.5.0 build. "
              "It is the file before the last event-label backfill, not proven untouched Stage 6 output. "
              "The exact historical invocation and native-to-published row identity remain unknown.",
              r"\newpage", r"\subsection*{Whole-history published-row census}",
              "November 2022--June 2026: all 44 frozen months. Root and clean are expanded published rows, not native execution counts. "
              "Within exact timestamp/token/cash/price/outcome/event-label/month classes, compare non-maker multiplicities with maker-derived reversed or copied wallet-pair capacities.",
              r"\begin{threeparttable}\small", r"\begin{tabular}{@{}lrr@{}}\toprule",
              r"Published-row quantity & Root & Clean \\\midrule"]
    for label, field in (("Expanded rows", "row_count"), ("Maker rows", "maker_rows"),
                         ("Non-maker rows", "nonmaker_rows")):
        lines.append(label + " & " + " & ".join(number(global_record["support"][name][field])
                                                for name in ("root", "clean")) + r" \\")
    lines.append(r"\midrule")
    for label, field in (("Excess observed vs reversed capacity", "excess_observed_correct"),
                         ("Missing observed vs reversed capacity", "missing_observed_correct"),
                         ("Excess observed vs copied capacity", "excess_observed_copied"),
                         ("Missing observed vs copied capacity", "missing_observed_copied")):
        lines.append(label + " & " + " & ".join(capacity(global_record[name]["full_label"][field],
                        global_record[name]["full_label"]["nonmaker_rows"]) for name in ("root", "clean")) + r" \\")
    lines.append(r"\midrule")
    for label, category in (("Rows in reversed-only compatible classes", "correct_only"),
                            ("Rows in copied-only compatible classes", "copied_only"),
                            ("Rows in both-compatible classes", "both_compatible"),
                            ("Rows in neither-compatible classes", "neither")):
        lines.append(label + " & " + " & ".join(capacity(global_record[name]["full_label"]["compatibility"][category]["nonmaker_rows"],
                        global_record[name]["full_label"]["nonmaker_rows"]) for name in ("root", "clean")) + r" \\")
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Percentages use each relation's non-maker-row count, including missing-capacity quantities. These are structural capacities, not identified incorrect executions. Both-compatible classes can contain reciprocal makers or self-wallet rows; reversed and copied matches overlap and must not be added."]
    omitted = [capacity(global_record[name]["label_omitted_diagnostic"]["excess_observed_correct"],
                        global_record[name]["label_omitted_diagnostic"]["nonmaker_rows"])
               for name in ("root", "clean")]
    cleaning = global_record["cleaning"]
    collision_groups = "/".join(number(window["equal_value_distinct_native_groups"])
                                for window in lineage["existing_native_collision_evidence"]["windows"])
    lines += [r"\item Omitting event labels only: excess vs reversed capacity, root " + omitted[0] + ", clean " + omitted[1] + ".",
              r"\item Root full-11-field DISTINCT to clean: " +
              ("reconciled" if summary["census"]["root_distinct_to_clean_reconciled"] else "not reconciled") +
              "; missing clean rows " + number(cleaning["expected_clean_only_rows"]) +
              "; excess clean rows " + number(cleaning["clean_only_rows"]) + ".",
              r"\item Exact-value row surplus: root " + number(cleaning["root_value_row_surplus"]) +
              ", clean " + number(cleaning["clean_value_row_surplus"]) + ". " +
              "This is not proven replay duplication. Two predeclared one-minute reconstructions contain " +
              collision_groups + " same-value groups spanning distinct native-role IDs. Full published-row reconciliation failed; attribution to particular clean removals remains unproved.",
              r"\end{tablenotes}\end{threeparttable}", r"\subsection*{Report-input dependencies requiring reconciliation}",
              r"\begin{threeparttable}\footnotesize",
              r"\begin{tabular}{@{}p{0.22\textwidth}p{0.31\textwidth}p{0.42\textwidth}@{}}\toprule",
              r"Report & Verified input boundary & Reconciliation needed; not measured impact \\\midrule"]
    impact_rows = (
        ("Sep20 FLB / reversals / ATP", "Resolved exact/inferred BUYs", "Actor filters; wallet support/concentration; clustered uncertainty; actual-BUY population."),
        ("Sep20 MLB all trades", "Already-filtered inferred BUY extract (" +
         f"{lineage['mlb_upstream_filter_review']['build_counts']['output_buy_rows']:,}" + " rows)",
         "The `all trades' branch does not restore upstream interior-price or bot exclusions; reconcile a common rebuilt cohort."),
        ("Oct2 profit / Oct3 alternatives", "Own-action ledger; FIFO matching", "Shared flags and filtered contrasts; source/FIFO continuity if actions change."),
        ("Oct2 tennis timing / sequences", "Timing: inferred fills; sequences: own-maker actions", "Timing BUY population and filters; maker-component flag provenance."),
    )
    for row in impact_rows:
        lines.append(" & ".join(tex_escape(cell) for cell in row) + r" \\")
    mlb_counts = lineage["mlb_upstream_filter_review"]["build_counts"]
    flag_correct = fast_flags["results"]["correct_swapped"]["wallets"]["synthetic-wallet-A"]
    flag_copied = fast_flags["results"]["copied_pair"]["wallets"]["synthetic-wallet-A"]
    lines += [r"\bottomrule\end{tabular}", r"\begin{tablenotes}\footnotesize",
              r"\item Upstream MLB candidate fill population (not the final accepted game cohort): " +
              number(mlb_counts["distinct_fills"]) + r" $\rightarrow$ " +
              number(mlb_counts["output_buy_rows"]) + " retained inferred BUY rows; " +
              number(mlb_counts["bot_exclusions"]) + " bot and " +
              number(mlb_counts["price_exclusions"]) + " price exclusions. " +
              "The Sep20 `all trades' branch cannot restore these upstream exclusions; this is not a measured final-cohort shortfall.",
              r"\item Shared sports flags have a verified reused fingerprint but an unknown producer; incorrect labels are not established. The separate learnability-cache helper does not establish sports-flag lineage.",
              r"\item Conditional synthetic flag mechanism: " + number(len(fast_flags["source_records"])) +
              " source records " + number(fast_flags["spacing_seconds"]) +
              " seconds apart. Correct A/B expansion gives each wallet " + number(flag_correct["n_trades"]) +
              " rows, a " + number(int(flag_correct["median_iti"])) +
              "-second median inter-trade interval and no flag; copied A has " + number(flag_copied["n_trades"]) +
              " rows, a " + number(int(flag_copied["median_iti"])) +
              "-second median and a criterion-A nonhuman flag (median interval below one second). Historical producer and wrong-label prevalence remain unknown.",
              r"\item Wallet-only corrections do not establish identical economic BUY populations, weights or membership. Unchanged rows and inputs can preserve all-actor point estimates. MINT (paired-token creation) and MERGE (paired-token redemption) can change BUY populations; wallet errors, action inference and FIFO assumptions remain separate.",
              r"\end{tablenotes}\end{threeparttable}", r"\end{document}"]
    return "\n".join(lines) + "\n"


def build_report(audit_root: Path, runtime_144: Path, runtime_156: Path, destination: Path) -> dict:
    require(not destination.exists(), "immutable report directory already exists")
    paths = {"runtime_1_5_0": audit_root / "01_runtime_duckdb_1_5_0/report.json",
             "runtime_1_4_4": runtime_144, "runtime_1_5_6": runtime_156,
             "lineage": audit_root / "lineage_review.json", "history": audit_root / "historical_provenance.json",
             "census": audit_root / "03_pair_census_v1/summary.json",
             "flag_mechanism": audit_root / "03b_flag_mechanism_v1/evidence.json",
             "transfer": audit_root / "03_pair_census_v1/verified_transfer.json"}
    documents, identities = {}, {}
    for key, path in paths.items():
        documents[key], identities[key] = read_json(path)
        require(destination.resolve() not in path.resolve().parents and destination.resolve() != path.resolve(),
                "output overlaps an input")
    validate_census(documents["census"], documents["transfer"], identities["census"])
    reports = {version: documents["runtime_" + version.replace(".", "_")]
               for version in ("1.4.4", "1.5.0", "1.5.6")}
    tex = render_tex(reports, documents["census"], documents["lineage"], documents["history"],
                     documents["flag_mechanism"]).encode()
    require(len(tex) <= MAX_JSON_BYTES, "report output exceeds 1MiB")
    manifest = {"schema_version": 1, "status": "complete_saved_evidence_report",
                "data_certified": False, "inputs": identities,
                "renderer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "census_canonical_head": documents["census"]["expected_head"],
                "layout": {"planned_maximum_pages": 2, "explicit_page_breaks": 1,
                           "compiled_or_visually_verified": False, "assets_required": False},
                "output": {"name": "wallet_diagnosis.tex", "bytes": len(tex),
                           "sha256": hashlib.sha256(tex).hexdigest()},
                "scope": "Saved synthetic controls, exhaustive published-row capacities and source-only report dependencies; no estimation or production repair."}
    encoded = (json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(tex) + len(encoded) <= MAX_JSON_BYTES, "immutable stage exceeds 1MiB")
    destination.mkdir(parents=True, exist_ok=False)
    for name, raw in (("wallet_diagnosis.tex", tex), ("manifest.json", encoded)):
        partial = destination / (name + ".partial")
        with partial.open("xb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, destination / name)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-root", type=Path, required=True)
    parser.add_argument("--runtime-1-4-4", type=Path, required=True)
    parser.add_argument("--runtime-1-5-6", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path)
    args = parser.parse_args()
    result = build_report(args.audit_root, args.runtime_1_4_4, args.runtime_1_5_6,
                          args.run_dir or args.audit_root / "04_report_v1")
    print(json.dumps({"status": result["status"], "output": result["output"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
