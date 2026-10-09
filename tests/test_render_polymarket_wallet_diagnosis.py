"""Small deterministic fixtures; no production data or TeX compiler required."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/render_polymarket_wallet_diagnosis.py"
SPEC = importlib.util.spec_from_file_location("wallet_diagnosis_renderer", SCRIPT)
renderer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(renderer)


def runtime(version):
    expected = []
    for token, maker, taker, side in (("token1", "wallet_A", "wallet_B", "BUY"),
                                      ("token2", "wallet_C", "wallet_D", "SELL")):
        for is_maker in (True, False):
            expected.append({"proxyWallet": maker if is_maker else taker,
                             "counterparty": taker if is_maker else maker,
                             "is_maker": is_maker,
                             "side": side if is_maker else ("SELL" if side == "BUY" else "BUY"),
                             "timestamp": 1772388001, "conditionId": token, "usdcSize": 4.0,
                             "price": 0.4, "outcome": "Yes", "eventSlug": "fixture_event",
                             "year_month": "2026-03"})
    cases = []
    for optimizer in ("default", "common_subplan_disabled", "all_disabled"):
        for timestamp in ("cached", "approx"):
            for threads in (1, 4):
                case = {"optimizer": optimizer, "timestamp_mode": timestamp, "threads": threads,
                        "name": f"{optimizer}_{timestamp}_threads{threads}"}
                if version == "1.4.4" and optimizer == "common_subplan_disabled":
                    case["status"] = "optimizer_unavailable"
                else:
                    actual = copy.deepcopy(expected)
                    failing = version == "1.5.0" and optimizer == "default" and timestamp == "cached"
                    if failing:
                        for row in actual:
                            if not row["is_maker"]:
                                row["proxyWallet"], row["counterparty"] = row["counterparty"], row["proxyWallet"]
                    case.update(status="complete", output_rows=actual, expected_rows=copy.deepcopy(expected),
                                exact_rows_match=not failing,
                                explain={"physical_plan": "__common_subplan_1" if failing else "two scans"})
                cases.append(case)
    return {"status": "complete_synthetic_default_discrepancy" if version == "1.5.0" else
                      "complete_synthetic_default_matches",
            "environment": {"duckdb_version": version, "system": "Linux" if version == "1.5.0" else "Darwin"},
            "source_snapshot": {"git_blob": renderer.FROZEN_BLOB, "sql_template_sha256": renderer.FROZEN_SQL},
            "production_inputs_read": False, "production_builder_imported_or_executed": False,
            "control_failure_cases": [],
            "default_discrepancy_cases": [item["name"] for item in cases if item.get("exact_rows_match") is False],
            "input_fixture": {"resolved_rows": [{"maker": "wallet_A", "taker": "wallet_B"},
                                                  {"maker": "wallet_C", "taker": "wallet_D"}],
                              "all_wallet_pairs_distinct": True}, "cases": cases}


def metric_record():
    support = {"row_count": 4, "maker_rows": 2, "nonmaker_rows": 2,
               "missing_role_rows": 0, "invalid_rows": 0}
    categories = {name: {"common_classes": 0, "maker_rows": 0, "nonmaker_rows": 0,
                         "singleton_role_classes": 0} for name in renderer.CATEGORIES}
    categories["copied_only"] = {"common_classes": 2, "maker_rows": 2, "nonmaker_rows": 2,
                                 "singleton_role_classes": 2}
    metrics = {"common_classes": 2, "maker_rows": 2, "nonmaker_rows": 2,
               "excess_observed_correct": 2, "missing_observed_correct": 2,
               "excess_observed_copied": 0, "missing_observed_copied": 0,
               "matched_correct": 0, "matched_copied": 2,
               "shared_candidate_capacity": 0, "observed_shared_candidate_capacity": 0,
               "compatibility": categories}
    return {"support": {name: copy.deepcopy(support) for name in ("root", "clean")},
            "cleaning": {"root_value_row_surplus": 0, "root_distinct_rows": 4,
                         "clean_value_row_surplus": 0,
                         "expected_clean_only_rows": 0, "clean_only_rows": 0,
                         "failed_full11_reconciliation_leaves": 0},
            **{name: {mode: copy.deepcopy(metrics) for mode in renderer.MODES} for name in ("root", "clean")}}


def summed(records):
    result = {}
    for key, value in records[0].items():
        result[key] = summed([record[key] for record in records]) if isinstance(value, dict) else sum(record[key] for record in records)
    return result


def census():
    months = {month: metric_record() for month in renderer.canonical_months()}
    return {"schema_version": 1, "status": renderer.COMPLETE, "expected_head": "a" * 40,
            "manifest_bytes": 27000000, "manifest_sha256": "b" * 64,
            "footer_rows": {"root": 176, "clean": 176},
            "census": {"status": renderer.COMPLETE, "completed_months": list(months),
                       "months": months, "global": summed(list(months.values())),
                       "final_input_identity_reopened": True, "root_distinct_to_clean_reconciled": True}}


def source_reviews():
    lineage = {"status": "source_and_compact_manifest_review_complete_with_provenance_unknowns",
               "report_lineage": [{"report": name, "input_class": "fixture boundary", "wallet_usage": ["actor"],
                                    "reconciliation_required": ["filters"]} for name in renderer.REPORT_NAMES],
               "shared_sports_flags": {"producer_status": "unverified"},
               "mlb_upstream_filter_review": {
                   "status": "confirmed_source_and_compact_artifact_restriction_no_result_impact_measurement",
                   "all_trades_restores_upstream_exclusions": False,
                   "upstream_filters": ["0.01 < price < 0.99",
                                        "exclude buyer proxyWallet with learnability/cache is_nonhuman=true"],
                   "build_counts": {"distinct_fills": 100, "price_exclusions": 10,
                                    "bot_exclusions": 30, "output_buy_rows": 60}},
               "existing_native_collision_evidence": {
                   "status": "bounded_positive_reconstructed_value_collision_evidence_not_clean_removal_attribution",
                   "parent_status_preserved": "bounded_reconciliation_incomplete", "parent_audit_exit_code": 2,
                   "value_fields": sorted(renderer.FIELDS),
                   "native_role_identity": ["lower(exchange_address)", "transaction_hash", "log_index", "is_maker"],
                   "windows": [{"name": name, "start_utc": "2026-03-01T18:00:00Z",
                                "end_utc_exclusive": "2026-03-01T18:01:00Z", "row_count": 3, "value_groups": 2,
                                "equal_value_distinct_native_groups": 1, "distinct_native_role_value_surplus": 1,
                                "same_native_role_value_surplus": 0, "expanded_to_root_left_only_rows": 3,
                                "expanded_to_root_right_only_rows": 3, "clean_removal_attribution_valid": False}
                               for name in ("pre_cutoff", "post_migration")]}}
    history = {"status": "complete_bounded_source_log_and_footer_review",
               "verified_source": {"builder": {"git_blob": renderer.FROZEN_BLOB}},
               "pre_last_backfill_backup": {"footer": {"created_by": "DuckDB version v1.5.0 (build 3a3967aa81)",
                                                        "data_bodies_read": False}},
               "historical_runtime_identity": {"unknown": "Historical invocation unknown"},
               "wallet_flags_provenance": {"sports_artifact_producer": "unknown", "sports_artifact_wrong_labels_proven": False}}
    return lineage, history


def flag_mechanism():
    wallets = ("synthetic-wallet-A", "synthetic-wallet-B")
    cases = {}
    for name, spacing in (("fast_100_seconds", 100), ("slow_300_seconds", 300)):
        records = [{"source_record_id": f"record{index}", "maker": wallets[0], "taker": wallets[1],
                    "timestamp": 1772388000 + index * spacing, "token_id": f"token{index}",
                    "maker_side": "BUY", "cash": 1.0, "price": 0.5,
                    "outcome": "YES", "event_label": "fixture_event"} for index in range(10)]
        fixtures, results = {}, {}
        for construction in ("correct_swapped", "copied_pair"):
            rows = []
            for record in records:
                for role in (True, False):
                    pair = wallets if role or construction == "copied_pair" else tuple(reversed(wallets))
                    rows.append({"source_record_id": record["source_record_id"], "proxyWallet": pair[0],
                                 "counterparty": pair[1], "timestamp": record["timestamp"],
                                 "conditionId": record["token_id"], "side": "BUY" if role else "SELL",
                                 "is_maker": role, "usdcSize": 1.0, "price": 0.5, "outcome": "YES",
                                 "eventSlug": "fixture_event", "year_month": "2026-03"})
            copied = construction == "copied_pair"
            a_flag = copied and spacing == 100
            a_median = (0.0 if copied else 100.0) if spacing == 100 else None
            b_median = 100.0 if not copied and spacing == 100 else None
            fixtures[construction] = rows
            results[construction] = {"expanded_row_count": 20, "gross_recorded_cash": 20.0,
                                     "summary": {"total_trades": 20}, "wallets": {
                                         wallets[0]: {"n_trades": 20 if copied else 10, "median_iti": a_median,
                                                      "is_nonhuman": a_flag, "flag_a_definite": a_flag,
                                                      "present_in_wallet_flags": True},
                                         wallets[1]: {"n_trades": 0 if copied else 10, "median_iti": b_median,
                                                      "is_nonhuman": None if copied else False,
                                                      "present_in_wallet_flags": not copied}}}
        cases[name] = {"spacing_seconds": spacing, "source_records": records,
                       "exact_expanded_fixtures": fixtures, "results": results}
    return {"status": "synthetic_mechanism_complete", "data_certified": False,
            "source_sha256": {"analysis/bot_filter.py": renderer.FROZEN_BOT_SHA,
                              "scripts/audit_polymarket_wallet_flag_mechanism.py": renderer.FLAG_PROBE_SHA},
            "resource_contract": {"rows_per_connection": 20}, "cases": cases}


class RendererTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.audit = self.base / "audit"
        self.lineage, self.history = source_reviews()
        self.summary = census()
        self.reports = {version: runtime(version) for version in ("1.4.4", "1.5.0", "1.5.6")}
        self.flags = flag_mechanism()
        self.write("audit/lineage_review.json", self.lineage)
        self.write("audit/historical_provenance.json", self.history)
        self.write("audit/01_runtime_duckdb_1_5_0/report.json", self.reports["1.5.0"])
        self.write("runtime144/report.json", self.reports["1.4.4"])
        self.write("runtime156/report.json", self.reports["1.5.6"])
        self.write("audit/03b_flag_mechanism_v1/evidence.json", self.flags)
        self.refresh_summary()

    def tearDown(self):
        self.temp.cleanup()

    def write(self, relative, value):
        path = self.base / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")

    def refresh_summary(self):
        self.write("audit/03_pair_census_v1/summary.json", self.summary)
        _, identity = renderer.read_json(self.audit / "03_pair_census_v1/summary.json")
        self.receipt = {"status": "verified_compact_pair_census_transfer", "canonical_head": "a" * 40,
                        "production_exit_code": 0,
                        "remote_manifest": {"path": "/fixture/manifest.json", "bytes": self.summary["manifest_bytes"],
                                            "sha256": self.summary["manifest_sha256"], "downloaded": False},
                        "artifacts": [{"local_path": identity["path"], "remote_path": "/fixture/summary.json",
                                       "bytes": identity["bytes"], "sha256": identity["sha256"], "remote_local_equal": True}]}
        self.write("audit/03_pair_census_v1/verified_transfer.json", self.receipt)

    def build(self, destination=None):
        return renderer.build_report(self.audit, self.base / "runtime144/report.json", self.base / "runtime156/report.json",
                                     destination or self.audit / "04_report_v1")

    def test_complete_fixture_is_deterministic_and_portable(self):
        first = self.build()
        second = self.build(self.base / "second_stage")
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertEqual(first["output"], second["output"])
        self.assertEqual(tex.count(r"\newpage"), 1)
        self.assertIn(r"\num{176}", tex)
        self.assertIn(r"\num{88} (100.00\%)", tex)
        self.assertIn("A / B & A / B", tex)
        self.assertIn("B / A & A / B", tex)
        self.assertIn("1.4.4 & Darwin & 2/2 match & Unavailable", tex)
        self.assertIn("1.5.0 & Linux & 0/2 match & 2/2 match", tex)
        self.assertIn("1.5.6 & Darwin & 2/2 match & 2/2 match", tex)
        self.assertIn("structural capacities, not identified incorrect executions", tex)
        self.assertIn("same-value groups spanning distinct native-role IDs", tex)
        self.assertIn("inferred BUY extract (60 rows)", tex)
        self.assertIn("does not restore upstream interior-price or bot exclusions", tex)
        self.assertNotIn("https://", tex)
        self.assertNotIn(r"\includegraphics", tex)
        self.assertNotIn("Conclusion", tex)
        self.assertTrue(tex.endswith("\\end{tablenotes}\\end{threeparttable}\n\\end{document}\n"))
        self.assertLess(first["output"]["bytes"], 16000)
        self.assertFalse(first["layout"]["compiled_or_visually_verified"])

    def test_existing_stage_is_never_overwritten(self):
        self.build()
        path = self.audit / "04_report_v1/wallet_diagnosis.tex"
        before = path.read_bytes()
        with self.assertRaises(renderer.ReportBlocked):
            self.build()
        self.assertEqual(path.read_bytes(), before)

    def test_incomplete_census_refuses_before_output_creation(self):
        self.summary["census"]["status"] = "blocked_census"
        self.refresh_summary()
        with self.assertRaisesRegex(renderer.ReportBlocked, "census incomplete"):
            self.build()
        self.assertFalse((self.audit / "04_report_v1").exists())

    def test_missing_month_or_unreopened_identity_refuses(self):
        for mutate in (lambda value: value["census"]["completed_months"].pop(),
                       lambda value: value["census"].update(final_input_identity_reopened=False)):
            changed = copy.deepcopy(self.summary)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_census(changed, self.receipt, renderer.read_json(self.audit / "03_pair_census_v1/summary.json")[1])

    def test_transfer_must_bind_actual_summary_and_full_manifest(self):
        identity = renderer.read_json(self.audit / "03_pair_census_v1/summary.json")[1]
        for mutate in (lambda value: value["artifacts"][0].update(sha256="c" * 64),
                       lambda value: value["remote_manifest"].update(bytes=1),
                       lambda value: value.update(canonical_head="d" * 40),
                       lambda value: value["artifacts"][0].update(remote_local_equal=False)):
            changed = copy.deepcopy(self.receipt)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_census(self.summary, changed, identity)

    def test_capacity_and_monthly_reconciliations_are_gates(self):
        for mutate in (lambda value: value["census"]["global"]["root"]["full_label"].update(matched_correct=1),
                       lambda value: value["census"]["months"]["2022-11"]["root"]["full_label"]["compatibility"]["copied_only"].update(common_classes=3),
                       lambda value: value["census"]["global"]["cleaning"].update(root_distinct_rows=175)):
            changed = copy.deepcopy(self.summary)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_census(changed, self.receipt, renderer.read_json(self.audit / "03_pair_census_v1/summary.json")[1])

    def test_completed_cleaning_difference_is_shown_not_hidden(self):
        for record in self.summary["census"]["months"].values():
            record["cleaning"].update(expected_clean_only_rows=1, clean_only_rows=1,
                                      failed_full11_reconciliation_leaves=1)
        self.summary["census"]["global"] = summed(list(self.summary["census"]["months"].values()))
        self.summary["census"]["root_distinct_to_clean_reconciled"] = False
        self.refresh_summary()
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertIn("not reconciled; missing clean rows \\num{44}; excess clean rows \\num{44}", tex)

    def test_runtime_nonwallet_changes_or_missing_control_refuse(self):
        for mutate in (lambda value: value["cases"][0]["output_rows"][1].update(price=0.5),
                       lambda value: value["cases"].pop(),
                       lambda value: value["cases"][0]["explain"].update(physical_plan="no shared plan")):
            changed = copy.deepcopy(self.reports["1.5.0"])
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_runtime(changed, "1.5.0")

    def test_unknown_flags_and_historical_invocation_are_preserved(self):
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertIn("exact historical invocation", tex)
        self.assertIn("unknown producer", tex)
        self.assertIn("incorrect labels are not established", tex)
        self.assertIn("MINT (paired-token creation) and MERGE (paired-token redemption) can change BUY populations", tex)
        self.assertIn("do not establish identical economic BUY populations, weights or membership", tex)

    def test_saved_mlb_flow_and_value_surplus_keep_population_boundaries(self):
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertIn("Upstream MLB candidate fill population (not the final accepted game cohort): "
                      r"\num{100} $\rightarrow$ \num{60} retained inferred BUY rows; "
                      r"\num{30} bot and \num{10} price exclusions.", tex)
        self.assertIn("cannot restore these upstream exclusions; this is not a measured final-cohort shortfall", tex)
        self.assertIn(r"Exact-value row surplus: root \num{0}, clean \num{0}. This is not proven replay duplication.", tex)

    def test_four_reader_groups_preserve_six_report_inventory(self):
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        impact = tex.split("Report-input dependencies requiring reconciliation", 1)[1]
        rows = impact.split(r"\midrule", 1)[1].split(r"\bottomrule", 1)[0]
        self.assertEqual(rows.count(r"\\"), 4)
        self.assertEqual(len(self.lineage["report_lineage"]), 6)
        self.assertIn("Own-action ledger; FIFO matching", rows)

    def test_native_collisions_do_not_certify_parent_or_clean_removals(self):
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertIn(r"one-minute reconstructions contain \num{1}/\num{1}", tex)
        self.assertIn("Full published-row reconciliation failed; attribution to particular clean removals remains unproved", tex)
        for mutate in (lambda value: value["existing_native_collision_evidence"].update(parent_status_preserved="complete"),
                       lambda value: value["existing_native_collision_evidence"]["windows"][0].update(clean_removal_attribution_valid=True)):
            changed = copy.deepcopy(self.lineage)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_provenance(changed, self.history)

    def test_synthetic_flag_note_requires_saved_complete_conserved_evidence(self):
        self.build()
        tex = (self.audit / "04_report_v1/wallet_diagnosis.tex").read_text()
        self.assertIn(r"\num{10} source records \num{100} seconds apart", tex)
        self.assertIn(r"a \num{100}-second median inter-trade interval and no flag", tex)
        self.assertIn(r"copied A has \num{20} rows, a \num{0}-second median and a criterion-A nonhuman flag (median interval below one second)", tex)
        self.assertIn("Historical producer and wrong-label prevalence remain unknown", tex)
        for mutate in (lambda value: value.update(status="incomplete"),
                       lambda value: value["source_sha256"].update({"analysis/bot_filter.py": "0" * 64}),
                       lambda value: value["cases"]["fast_100_seconds"]["exact_expanded_fixtures"]["copied_pair"].pop(),
                       lambda value: value["cases"]["fast_100_seconds"]["exact_expanded_fixtures"]["copied_pair"][0].update(price=0.6),
                       lambda value: [row.update(timestamp=0, conditionId="unrelated-token", usdcSize=2.0)
                                      for rows in value["cases"]["fast_100_seconds"]["exact_expanded_fixtures"].values()
                                      for row in rows]):
            changed = copy.deepcopy(self.flags)
            mutate(changed)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.validate_flag_mechanism(changed)

    def test_json_size_duplicate_and_nonfinite_gates(self):
        path = self.base / "invalid.json"
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b" " * (renderer.MAX_JSON_BYTES + 1)):
            path.write_bytes(raw)
            with self.assertRaises(renderer.ReportBlocked):
                renderer.read_json(path)

    def test_tex_escaping_and_zero_denominator(self):
        self.assertEqual(renderer.tex_escape("A&B_1%"), r"A\&B\_1\%")
        self.assertEqual(renderer.capacity(0, 0), r"\num{0} (not defined)")


if __name__ == "__main__":
    unittest.main()
