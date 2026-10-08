"""Synthetic census tests: no production data, guard bypass or service calls."""
from __future__ import annotations

import copy
from collections import Counter
import hashlib
import json
import os
import random
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_lineage as lineage
from scripts import audit_polymarket_wallet_pairs as census

MONTH = "2026-03"
SECOND = lineage.epoch("2026-03-01T18:00:00Z")
SCHEMA = pa.schema([(field, pa.int64() if field == "timestamp" else
                     pa.float64() if field in {"price", "usdcSize"} else
                     pa.bool_() if field == "is_maker" else pa.string())
                    for field in lineage.VALUE_FIELDS])


def maker(wallet="A", counterparty="B", **changes):
    return {"proxyWallet": wallet, "timestamp": SECOND, "conditionId": "9" * 77,
            "usdcSize": 3.871, "price": 0.98, "side": "BUY", "outcome": "YES",
            "eventSlug": "event", "is_maker": True, "counterparty": counterparty,
            "year_month": MONTH, **changes}


def nonmaker(row, *, copied=False, **changes):
    return {**row, "is_maker": False, "side": "SELL" if row["side"] == "BUY" else "BUY",
            "proxyWallet": row["proxyWallet"] if copied else row["counterparty"],
            "counterparty": row["counterparty"] if copied else row["proxyWallet"], **changes}


def distinct(rows):
    result, seen = [], set()
    for row in rows:
        key = tuple(row[field] for field in lineage.VALUE_FIELDS)
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


def fixture(con, rows, clean=None):
    con.register("root_leaf", pa.Table.from_pylist(rows, schema=SCHEMA))
    con.register("clean_leaf", pa.Table.from_pylist(distinct(rows) if clean is None else clean, schema=SCHEMA))
    support = {name: census.leaf_support(con, name + "_leaf", MONTH) for name in ("root", "clean")}
    return census.audit_leaf(con, support)


class PairCensusTests(unittest.TestCase):
    def setUp(self):
        self.con = census.connection()
        self.addCleanup(self.con.close)

    def test_correct_and_copied_constructions_have_exact_opposite_classification(self):
        for copied, category in ((False, "correct_only"), (True, "copied_only")):
            with self.subTest(copied=copied):
                row = maker()
                result = fixture(self.con, [row, nonmaker(row, copied=copied)])
                metrics = result["root"]["full_label"]
                self.assertEqual(metrics["compatibility"][category]["common_classes"], 1)
                self.assertEqual(metrics["compatibility"][category]["nonmaker_rows"], 1)
                self.assertEqual(metrics["excess_observed_correct"], int(copied))
                self.assertEqual(metrics["missing_observed_correct"], int(copied))
                self.assertEqual(metrics["excess_observed_copied"], int(not copied))
                self.assertEqual(metrics["matched_correct"], int(not copied))
                self.assertEqual(metrics["matched_copied"], int(copied))
                self.assertEqual(result["cleaning"]["failed_full11_reconciliation_leaves"], 0)

    def test_self_wallet_is_both_compatible_with_shared_capacity(self):
        row = maker("A", "A")
        metrics = fixture(self.con, [row, nonmaker(row)])["root"]["full_label"]
        self.assertEqual(metrics["compatibility"]["both_compatible"]["nonmaker_rows"], 1)
        self.assertEqual(metrics["observed_shared_candidate_capacity"], 1)
        self.assertEqual(metrics["shared_candidate_capacity"], 1)
        self.assertEqual(metrics["matched_correct"] + metrics["matched_copied"], 2)

    def test_reciprocal_distinct_pairs_are_both_compatible_without_self_wallets(self):
        first, second = maker("A", "B"), maker("B", "A")
        result = fixture(self.con, [first, second, nonmaker(first, copied=True), nonmaker(second, copied=True)])
        metrics = result["root"]["full_label"]
        self.assertEqual(metrics["compatibility"]["both_compatible"]["common_classes"], 1)
        self.assertEqual(metrics["observed_shared_candidate_capacity"], 2)
        self.assertEqual(metrics["multiple_maker_value_classes"], 1)
        self.assertEqual(result["support"]["root"]["self_wallet_rows"], 0)

    def test_mixed_constructions_in_one_common_class_are_neither(self):
        first, second = maker("A", "B"), maker("C", "D")
        metrics = fixture(self.con, [first, second, nonmaker(first), nonmaker(second, copied=True)])["root"]["full_label"]
        self.assertEqual(metrics["compatibility"]["neither"]["nonmaker_rows"], 2)
        self.assertEqual(metrics["matched_correct"], 1)
        self.assertEqual(metrics["matched_copied"], 1)
        self.assertEqual(metrics["excess_observed_correct"], 1)
        self.assertEqual(metrics["shared_candidate_capacity"], 0)

    def test_side_is_part_of_wallet_class_and_both_maker_sides_flip(self):
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                row = maker(side=side)
                good = fixture(self.con, [row, nonmaker(row)])["root"]["full_label"]
                bad = fixture(self.con, [row, nonmaker(row, side=side)])["root"]["full_label"]
                self.assertEqual(good["compatibility"]["correct_only"]["common_classes"], 1)
                self.assertEqual(bad["compatibility"]["neither"]["common_classes"], 1)

    def test_case_and_float_payload_are_not_normalized_or_rounded(self):
        row = maker("A", "b")
        for altered in (nonmaker(row, proxyWallet="B"), nonmaker(row, price=0.9800000000000001)):
            with self.subTest(altered=altered):
                metrics = fixture(self.con, [row, altered])["root"]["full_label"]
                self.assertEqual(metrics["excess_observed_correct"], 1)
                self.assertEqual(metrics["missing_observed_correct"], 1)

    def test_unequal_counts_preserve_occurrences_and_common_class_support(self):
        row, observed = maker(), nonmaker(maker())
        result = fixture(self.con, [row, row, row, observed, observed])
        metrics = result["root"]["full_label"]
        self.assertEqual(metrics["maker_rows"], 3)
        self.assertEqual(metrics["nonmaker_rows"], 2)
        self.assertEqual(metrics["matched_correct"], 2)
        self.assertEqual(metrics["missing_observed_correct"], 1)
        self.assertEqual(metrics["unequal_role_count_classes"], 1)
        self.assertEqual(metrics["multioccurrence_classes"], 1)
        self.assertEqual(metrics["multiple_maker_value_classes"], 0)
        self.assertEqual(result["cleaning"]["root_value_row_surplus"], 3)
        self.assertEqual(result["clean"]["full_label"]["compatibility"]["correct_only"]["common_classes"], 1)

    def test_empty_role_classes_are_retained(self):
        for rows, empty, excess, missing in (([maker()], "empty_nonmaker_classes", 0, 1),
                ([nonmaker(maker())], "empty_maker_classes", 1, 0)):
            with self.subTest(empty=empty):
                metrics = fixture(self.con, rows)["root"]["full_label"]
                self.assertEqual(metrics[empty], 1)
                self.assertEqual(metrics["compatibility"]["neither"]["common_classes"], 1)
                self.assertEqual(metrics["excess_observed_correct"], excess)
                self.assertEqual(metrics["missing_observed_correct"], missing)

    def test_empty_leaf_is_additive_zero(self):
        result = fixture(self.con, [])
        self.assertEqual(result["root"]["full_label"]["common_classes"], 0)
        self.assertEqual(result["cleaning"]["root_distinct_rows"], 0)

    def test_label_omission_is_separate_and_null_labels_match_null_safely(self):
        row = maker(eventSlug=None)
        result = fixture(self.con, [row, nonmaker(row, eventSlug="published")])
        self.assertEqual(result["root"]["full_label"]["common_classes"], 2)
        self.assertEqual(result["root"]["full_label"]["excess_observed_correct"], 1)
        self.assertEqual(result["root"]["label_omitted_diagnostic"]["compatibility"]["correct_only"]["common_classes"], 1)
        same_null = fixture(self.con, [row, nonmaker(row)])
        self.assertEqual(same_null["root"]["full_label"]["compatibility"]["correct_only"]["common_classes"], 1)

    def test_label_omission_merges_maker_value_classes(self):
        first, second = maker(eventSlug="a"), maker(eventSlug="b")
        result = fixture(self.con, [first, second, nonmaker(first), nonmaker(second)])
        self.assertEqual(result["root"]["full_label"]["common_classes"], 2)
        omitted = result["root"]["label_omitted_diagnostic"]
        self.assertEqual(omitted["common_classes"], 1)
        self.assertEqual(omitted["multiple_maker_value_classes"], 0)
        self.assertEqual(omitted["maker_rows"], 2)

    def test_full11_cleaning_counts_partial_deduplication_surplus(self):
        row, observed = maker(), nonmaker(maker())
        result = fixture(self.con, [row, row, observed, observed], [row, row, observed])
        cleaning = result["cleaning"]
        self.assertEqual(cleaning["root_value_row_surplus"], 2)
        self.assertEqual(cleaning["clean_value_row_surplus"], 1)
        self.assertEqual(cleaning["expected_clean_only_rows"], 0)
        self.assertEqual(cleaning["clean_only_value_classes"], 0)
        self.assertEqual(cleaning["clean_only_rows"], 1)
        self.assertEqual(cleaning["failed_full11_reconciliation_leaves"], 1)

    def test_full11_cleaning_detects_label_or_cash_change_despite_equal_counts(self):
        row, observed = maker(), nonmaker(maker())
        for change in ({"eventSlug": "different"}, {"usdcSize": 3.872}):
            with self.subTest(change=change):
                result = fixture(self.con, [row, observed], [row, {**observed, **change}])
                self.assertEqual(result["cleaning"]["expected_clean_only_rows"], 1)
                self.assertEqual(result["cleaning"]["clean_only_rows"], 1)

    def test_second_boundary_chunk_additivity_including_missing_roles(self):
        first, second = maker(), maker(timestamp=SECOND + 1, eventSlug="next")
        rows = [first, nonmaker(first, copied=True), second, nonmaker(second),
                maker(timestamp=SECOND + 2), nonmaker(maker(timestamp=SECOND + 3))]
        full = fixture(self.con, rows)
        parts = [fixture(self.con, [row for row in rows if row["timestamp"] == second])
                 for second in range(SECOND, SECOND + 4)]
        merged = census.sum_records(parts)
        for name in ("root", "clean", "cleaning"):
            self.assertEqual(merged[name], full[name])
        self.assertAlmostEqual(merged["support"]["root"]["gross_recorded_cash"], full["support"]["root"]["gross_recorded_cash"])

    def test_resource_refusal_and_complete_second_boundary(self):
        support = {name: {"invalid_rows": 0, "row_count": 1, "logical_payload_bytes": 10}
                   for name in ("root", "clean")}
        self.assertTrue(census.admitted(support))
        support["root"]["row_count"] = census.MAX_ROWS + 1
        self.assertFalse(census.admitted(support))
        with self.assertRaisesRegex(lineage.AuditBlocked, "not admitted"):
            census.audit_leaf(self.con, support)
        support["root"]["row_count"] = 1
        support["root"]["logical_payload_bytes"] = census.MAX_PAYLOAD
        self.assertFalse(census.admitted(support))
        self.assertEqual(census.split_seconds(10, 13), ((10, 11), (11, 13)))
        with self.assertRaisesRegex(lineage.AuditBlocked, "complete UTC second"):
            census.split_seconds(10, 11)

    def test_invalid_rows_are_counted_then_refused(self):
        for changes in ({"side": "OTHER"}, {"is_maker": None}, {"usdcSize": float("nan")},
                        {"year_month": "2026-04"}, {"proxyWallet": ""}):
            with self.subTest(changes=changes):
                self.con.register("invalid", pa.Table.from_pylist([maker(**changes)], schema=SCHEMA))
                support = census.leaf_support(self.con, "invalid", MONTH)
                self.assertGreater(support["invalid_rows"], 0)
                with self.assertRaisesRegex(lineage.AuditBlocked, "irregular rows were counted"):
                    census.admitted({"root": support, "clean": support})

    def test_runtime_settings_record_no_spill_and_optimizer_availability(self):
        settings = census.runtime_settings(self.con)
        self.assertEqual(settings["threads"], 4)
        self.assertEqual(settings["max_temp_directory_size"], "0 bytes")
        self.assertEqual(settings["temp_directory"], "")
        self.assertEqual(settings["timezone"], "UTC")
        if settings["common_subplan_available"]:
            self.assertIn("common_subplan", settings["disabled_optimizers"])

    def test_randomized_capacities_against_independent_python_counters(self):
        rng = random.Random(83519)
        for case in range(30):
            rows = []
            for _ in range(25):
                row = maker(rng.choice("ABC"), rng.choice("ABC"), timestamp=SECOND+rng.randrange(3),
                    side=rng.choice(("BUY", "SELL")), eventSlug=rng.choice((None, "", "event")),
                    usdcSize=rng.choice((1.0, 3.871)), price=rng.choice((0.5, 0.98)))
                rows += [row] * rng.choice((1, 1, 2))
                if rng.random() > 0.12:
                    observed = nonmaker(row, copied=rng.random() > 0.5)
                    if rng.random() < 0.2:
                        observed = {**observed, "proxyWallet": rng.choice("ABC"), "eventSlug": rng.choice((None, "", "event"))}
                    rows += [observed] * rng.choice((1, 1, 2))
            result = fixture(self.con, rows)
            for omit in (False, True):
                with self.subTest(case=case, omit=omit):
                    common = tuple(field for field in census.COMMON if not omit or field != "eventSlug")
                    fields = common + census.WALLETS
                    key = lambda row: tuple(row[field] for field in fields)
                    n = Counter(key(row) for row in rows if not row["is_maker"])
                    c = Counter(key(nonmaker(row)) for row in rows if row["is_maker"])
                    u = Counter(key(nonmaker(row, copied=True)) for row in rows if row["is_maker"])
                    keys = set(n) | set(c) | set(u)
                    metrics = result["root"]["label_omitted_diagnostic" if omit else "full_label"]
                    expected = {"nonmaker_rows": sum(n.values()), "maker_rows": sum(c.values()),
                        "excess_observed_correct": sum((n-c).values()), "missing_observed_correct": sum((c-n).values()),
                        "excess_observed_copied": sum((n-u).values()), "missing_observed_copied": sum((u-n).values()),
                        "matched_correct": sum(min(n[k], c[k]) for k in keys),
                        "matched_copied": sum(min(n[k], u[k]) for k in keys),
                        "shared_candidate_capacity": sum(min(c[k], u[k]) for k in keys),
                        "observed_shared_candidate_capacity": sum(min(n[k], c[k], u[k]) for k in keys),
                        "observed_rows_in_shared_candidate_classes": sum(n[k] for k in keys if c[k] and u[k]),
                        "observed_shared_candidate_classes": sum(bool(n[k] and c[k] and u[k]) for k in keys)}
                    self.assertEqual({field: metrics[field] for field in expected}, expected)
                    expected_profiles = {name: {"common_classes": 0, "maker_rows": 0, "nonmaker_rows": 0,
                                               "singleton_role_classes": 0} for name in census.CATEGORIES}
                    for h in {k[:len(common)] for k in keys}:
                        nc = Counter({k: value for k, value in n.items() if k[:len(common)] == h})
                        cc = Counter({k: value for k, value in c.items() if k[:len(common)] == h})
                        uc = Counter({k: value for k, value in u.items() if k[:len(common)] == h})
                        category = "both_compatible" if nc == cc and nc == uc else "correct_only" if nc == cc else "copied_only" if nc == uc else "neither"
                        profile = expected_profiles[category]
                        profile["common_classes"] += 1
                        profile["maker_rows"] += sum(cc.values())
                        profile["nonmaker_rows"] += sum(nc.values())
                        profile["singleton_role_classes"] += int(sum(cc.values()) == sum(nc.values()) == 1)
                    self.assertEqual(metrics["compatibility"], expected_profiles)


class ApprovalAndSnapshotTests(unittest.TestCase):
    def test_expected_head_and_digest_are_exact(self):
        census.require_expected_head("a" * 40, "a" * 40)
        for value in ("bad", "A" * 40, "g" * 40, "b" * 40):
            with self.subTest(value=value), self.assertRaises(lineage.AuditBlocked):
                census.require_expected_head("a" * 40, value)
        encoded = b'{"status":"preflight_complete"}'

        class ChangingFile:
            reads = 0

            def read_bytes(self):
                self.reads += 1
                return encoded if self.reads == 1 else b"{}"

        source = ChangingFile()
        digest = hashlib.sha256(encoded).hexdigest()
        value, actual = census.read_reviewed_manifest(source, digest)
        self.assertEqual(source.reads, 1)
        self.assertEqual(actual, digest)
        self.assertEqual(value["status"], "preflight_complete")
        for bad in ("b" * 64, "bad", "A" * 64):
            with self.subTest(bad=bad), self.assertRaises(lineage.AuditBlocked):
                census.read_reviewed_manifest(source, bad)

    def fixture_preflight(self, directory, months=(MONTH,), second_pair=False):
        inputs = {name: str(Path(directory) / name) for name in ("root", "clean")}
        for name, path in inputs.items():
            for month in months:
                stamp = SECOND if month == MONTH else lineage.month_bounds(month)[0] + 64800
                row = maker(timestamp=stamp, year_month=month)
                rows = [row, nonmaker(row)]
                if second_pair:
                    row = maker(timestamp=stamp+1, year_month=month)
                    rows += [row, nonmaker(row, copied=True)]
                partition = Path(path) / ("year_month=" + month)
                partition.mkdir(parents=True)
                table = pa.Table.from_pylist(rows, schema=SCHEMA)
                if name == "root":
                    table = table.drop(["year_month"])
                pq.write_table(table, partition / "data.parquet")
        contract, digest = census.load_contract()
        count = (4 if second_pair else 2) * len(months)
        with patch.object(lineage, "canonical_months", return_value=months), patch.object(census, "EXPECTED_ROWS", {"root": count, "clean": count}):
            manifest = census.preflight(inputs, "a" * 40, {}, contract, digest)
        return manifest

    def test_preflight_created_by_scan_plans_reopen_and_read_only_body(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            self.assertEqual(len(manifest["initial_plan"]), 1)
            for name in ("root", "clean"):
                self.assertTrue(manifest["inventories"][name][0]["created_by"])
                plans = manifest["initial_plan"][0]["source_scan_plans"][name]
                self.assertEqual(plans["admission"]["parquet_scan_operators"], 1)
                self.assertEqual(plans["grouping"]["parquet_scan_operators"], 1)
            with patch.object(lineage, "canonical_months", return_value=(MONTH,)):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "published_pair_census_complete")
            self.assertTrue(result["final_input_identity_reopened"])
            self.assertTrue(result["root_distinct_to_clean_reconciled"])
            self.assertEqual(result["global"]["support"]["root"]["row_count"], 2)
            self.assertGreater(result["resource_profile"]["peak_rss_bytes"], 0)
            self.assertEqual(result["admission_compressed_footprint_bytes"], result["grouping_compressed_footprint_bytes"])
            census.verify_frozen([info for infos in manifest["inventories"].values() for info in infos])

    def test_complete_inventory_reopen_after_each_of_two_months(self):
        with tempfile.TemporaryDirectory() as directory:
            months = (MONTH, "2026-04")
            manifest = self.fixture_preflight(directory, months=months)
            with patch.object(lineage, "canonical_months", return_value=months):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "published_pair_census_complete")
            self.assertEqual(result["completed_months"], list(months))
            self.assertEqual(result["global"]["support"]["root"]["row_count"], 4)
            self.assertTrue(result["final_input_identity_reopened"])

    def test_parquet_backed_empty_intervals_are_preserved_with_footer_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            original = manifest["initial_plan"][0]
            manifest["initial_plan"] = [{**original, "lower_inclusive": lower, "upper_exclusive": upper}
                for lower, upper in ((SECOND-1, SECOND), (SECOND, SECOND+1), (SECOND+1, SECOND+2))]
            con = census.connection()
            try:
                census.leaf_views(con, manifest["inventories"], MONTH, SECOND+1, SECOND+2)
                query = census.support_query("root_leaf", MONTH)
                with self.assertRaises(lineage.AuditBlocked):
                    census.scan_plan(con, query)
                empty_plan = census.scan_plan(con, query, empty_overlap=True)
                self.assertEqual(empty_plan["parquet_scan_operators"], 0)
                self.assertTrue(empty_plan["footer_proved_empty_result"])
            finally:
                con.close()
            with patch.object(lineage, "canonical_months", return_value=(MONTH,)):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "published_pair_census_complete")
            self.assertEqual([leaf["metrics"]["support"]["root"]["row_count"] for leaf in result["leaves"]], [0, 2, 0])
            self.assertEqual(result["global"]["support"]["root"]["row_count"], 2)

    def test_stale_runtime_caps_sources_and_snapshots_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            for field in ("expected_head", "environment", "caps", "source_sha256", "inventories", "initial_plan"):
                altered = copy.deepcopy(manifest)
                altered[field] = "changed"
                with self.subTest(field=field), self.assertRaisesRegex(lineage.AuditBlocked, "reviewed preflight"):
                    census.verify_reviewed(manifest, altered)
            census.verify_reviewed(manifest, copy.deepcopy(manifest))
            info = manifest["inventories"]["root"][0]
            changed_creator = {**info, "created_by": "unapproved writer"}
            with self.assertRaisesRegex(lineage.AuditBlocked, "creator"):
                census.verify_frozen([changed_creator])
            stat = Path(info["path"]).stat()
            os.utime(info["path"], ns=(stat.st_atime_ns, stat.st_mtime_ns + 1))
            with self.assertRaises(lineage.AuditBlocked):
                census.verify_frozen([info])

    def test_body_runtime_drift_and_final_snapshot_failure_stay_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            altered = copy.deepcopy(manifest)
            altered["environment"]["runtime_settings"]["threads"] = 99
            result = census.run_census(altered)
            self.assertEqual(result["status"], "blocked_census")
            self.assertFalse(result["final_input_identity_reopened"])
            original = census.verify_frozen
            calls = []

            def fail_final(infos):
                calls.append(1)
                if len(calls) == 3:
                    raise lineage.AuditBlocked("injected final identity failure")
                return original(infos)

            with patch.object(lineage, "canonical_months", return_value=(MONTH,)), patch.object(census, "verify_frozen", side_effect=fail_final):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "blocked_census")
            self.assertFalse(result["final_input_identity_reopened"])

    def test_recursive_bisection_is_exhaustive(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            # A narrow approved fixture interval contains all input records.
            manifest["initial_plan"][0]["lower_inclusive"] = SECOND
            manifest["initial_plan"][0]["upper_exclusive"] = SECOND + 2
            with patch.object(lineage, "canonical_months", return_value=(MONTH,)), patch.object(census, "MAX_ROWS", 1):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "blocked_census")
            self.assertEqual(result["splits"][0]["lower_inclusive"], SECOND)
            self.assertIn("complete UTC second", result["failure_reason"])

    def test_successful_bisection_preserves_two_complete_second_classes(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory, second_pair=True)
            manifest["initial_plan"][0]["lower_inclusive"] = SECOND
            manifest["initial_plan"][0]["upper_exclusive"] = SECOND + 2
            with patch.object(lineage, "canonical_months", return_value=(MONTH,)), patch.object(census, "MAX_ROWS", 2):
                result = census.run_census(manifest)
            self.assertEqual(result["status"], "published_pair_census_complete")
            self.assertEqual([(leaf["lower_inclusive"], leaf["upper_exclusive"]) for leaf in result["leaves"]],
                             [(SECOND, SECOND+1), (SECOND+1, SECOND+2)])
            metrics = result["global"]["root"]["full_label"]
            self.assertEqual(metrics["compatibility"]["correct_only"]["nonmaker_rows"], 1)
            self.assertEqual(metrics["compatibility"]["copied_only"]["nonmaker_rows"], 1)
            self.assertEqual(metrics["nonmaker_rows"], 2)

    def test_rejected_full_month_expands_to_complete_days_first(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            # Force a fixture resource refusal without altering published rows.
            with patch.object(lineage, "canonical_months", return_value=(MONTH,)), patch.object(census, "MAX_ROWS", 1):
                result = census.run_census(manifest)
            first = result["splits"][0]
            self.assertEqual(first["split_kind"], "complete_utc_days")
            self.assertEqual((first["lower_inclusive"], first["upper_exclusive"]), lineage.month_bounds(MONTH))
            self.assertEqual(result["splits"][1]["split_kind"], "integer_second_bisection")
            self.assertFalse(result["final_input_identity_reopened"])

    def test_output_no_overwrite_and_finite_exact_json(self):
        with self.assertRaises(ValueError):
            census.exact_json({"cash": float("inf")})
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            destination = Path(directory) / "stage"
            census.write_immutable(destination, manifest)
            saved = json.loads((destination / "summary.json").read_bytes())
            self.assertEqual(saved["manifest_sha256"], hashlib.sha256((destination / "manifest.json").read_bytes()).hexdigest())
            self.assertLess((destination / "summary.json").stat().st_size, census.MAX_SUMMARY)
            with self.assertRaises(FileExistsError):
                census.write_immutable(destination, manifest)

    def test_interrupted_publication_has_manifest_but_no_complete_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.fixture_preflight(directory)
            destination = Path(directory) / "interrupted_stage"
            real_replace = os.replace
            published = []

            def interrupt_summary(source, target):
                published.append(Path(target).name)
                if Path(target).name == "summary.json":
                    raise KeyboardInterrupt()
                real_replace(source, target)

            with patch.object(census.os, "replace", side_effect=interrupt_summary):
                with self.assertRaises(KeyboardInterrupt):
                    census.write_immutable(destination, manifest)
            self.assertEqual(published, ["manifest.json", "summary.json"])
            self.assertTrue((destination / "manifest.json").is_file())
            self.assertFalse((destination / "summary.json").exists())
            self.assertTrue((destination / "summary.json.partial").is_file())


if __name__ == "__main__":
    unittest.main()
