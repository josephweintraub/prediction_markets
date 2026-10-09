"""Synthetic immutable-file repair tests; no production data or entrypoint runs."""
from __future__ import annotations

from collections import Counter
import copy
import hashlib
import json
from pathlib import Path
import resource
import signal
import tempfile
import unittest
from unittest import mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_polymarket_wallet_pairs as pairs
from scripts import repair_polymarket_wallet_attribution as repair


MONTH = "2026-03"
LOWER, UPPER = repair.lineage.month_bounds(MONTH)


def copied_rows():
    result = []
    for maker, counterparty, timestamp, token, side in (
        ("A", "B", LOWER + 1, "1001", "BUY"),
        ("D", "E", LOWER + 86401, "1002", "SELL"),
        ("S", "S", LOWER + 2, "1003", "BUY"),
        ("C", "F", LOWER + 3, "1004", "BUY"),
        ("F", "C", LOWER + 3, "1004", "BUY"),
    ):
        for is_maker in (True, False):
            result.append(dict(zip(repair.FIELDS, (
                maker, timestamp, token, 1.234567890123, 0.37,
                side if is_maker else ("SELL" if side == "BUY" else "BUY"),
                "Yes", "", is_maker, counterparty, MONTH))))
    # Preserve repeated published values independently of their native lineage.
    result.extend(copy.deepcopy(result[:2]))
    return sorted(result, key=lambda row: row["timestamp"])


def transformed(rows):
    result = copy.deepcopy(rows)
    for row in result:
        if row["is_maker"] is False:
            row["proxyWallet"], row["counterparty"] = row["counterparty"], row["proxyWallet"]
    return result


def schema_for(fields):
    return pa.schema([(field, pa.int64() if field == "timestamp" else
                       pa.float64() if field in {"usdcSize", "price"} else
                       pa.bool_() if field == "is_maker" else pa.string()) for field in fields])


def write_rows(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([{field: row[field] for field in fields} for row in rows],
                                     schema=schema_for(fields)), path, compression="zstd", row_group_size=4)


class WalletRepairTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="wallet_repair_fixture_")
        self.root = Path(self.temporary.name)
        self.caps = mock.patch.dict(repair.CAPS, {"memory_limit": "64MB", "threads": 1,
                                                 "minimum_free_bytes": 1024})
        self.caps.start()
        self.source = {"head": "a" * 40, "sha256": {"fixture": "b" * 64}}
        self.binding = {"census": {"sha256": "c" * 64}, "census_qa": {"sha256": "d" * 64}}
        self.original = copied_rows()
        seen = set()
        clean = []
        for row in self.original:
            value = tuple(row[field] for field in repair.FIELDS)
            if value not in seen:
                clean.append(row)
                seen.add(value)
        self.rows = {"root": self.original, "clean": clean}
        self.paths = {name: self.root / (name + ".parquet") / ("year_month=" + MONTH) / "data.parquet"
                      for name in repair.RELATIONS}
        infos = {}
        for name, path in self.paths.items():
            fields = tuple(field for field in repair.FIELDS if field != "year_month" or name == "clean")
            write_rows(path, self.rows[name], fields)
            info = repair.lineage.footer_info(path, repair.RELATIONS[name])
            info["partition_month"] = MONTH
            infos[name] = [info]
        con = repair.connection(self.root)
        try:
            records = []
            for lower, upper in ((LOWER, LOWER + 86400), (LOWER + 86400, UPPER)):
                metrics = {"support": {}}
                for name, path in self.paths.items():
                    fields = pq.ParquetFile(path).schema_arrow.names
                    repair.leaf_view(con, name + "_leaf", path, fields, MONTH, lower, upper)
                    metrics["support"][name] = pairs.leaf_support(con, name + "_leaf", MONTH)
                    con.execute("CREATE OR REPLACE TEMP TABLE " + name + "_groups AS " + pairs.grouping_query(name))
                    metrics[name] = {"full_label": pairs.pair_metrics(con, name + "_groups")}
                records.append({"month": MONTH, "lower_inclusive": lower, "upper_exclusive": upper, "metrics": metrics})
            support = {name: pairs.sum_records([record["metrics"]["support"][name] for record in records])
                       for name in repair.RELATIONS}
        finally:
            con.close()
        self.manifest = {"status": repair.COMPLETE, "data_certified": False,
                         "inventories": infos, "inputs": {name: str(path.parent.parent) for name, path in self.paths.items()},
                         "footer_rows": {name: len(rows) for name, rows in self.rows.items()},
                         "census": {"status": repair.COMPLETE, "data_certified": False,
                                    "final_input_identity_reopened": True, "completed_months": [MONTH],
                                    "months": {MONTH: {"support": support}}, "leaves": records,
                                    "root_distinct_to_clean_reconciled": True,
                                    "global": {"support": support, "cleaning": {"failed_full11_reconciliation_leaves": 0}}}}
        self.qa = {"status": "complete_saved_wallet_census_manifest_qa", "data_certified": False,
                   "exit_profile": {"exit_status": 0}, "final_input_identity_reopened_recorded": True,
                   "completed_months": [MONTH]}
        self.plan = repair.admit_census(self.manifest, self.qa, months=[MONTH])
        self.target = self.root / "new_run"

    def tearDown(self):
        self.caps.stop()
        self.temporary.cleanup()

    def reviewed(self):
        return repair.preflight(self.plan, self.target, self.binding, self.source)

    def build(self, reviewed=None):
        fresh = self.reviewed()
        with mock.patch.object(repair, "source_snapshot", return_value=self.source):
            return repair.build_run(self.plan, self.target, reviewed or fresh, fresh, command=["synthetic"])

    def test_full_repair_preserves_exact_multisets_schema_originals_and_cleaning(self):
        original_hashes = {name: repair.sha256(path) for name, path in self.paths.items()}
        result = self.build()
        self.assertEqual(result["status"], "repair_complete")
        self.assertEqual(result["downstream_adoption"], "pending")
        self.assertFalse(result["data_certified"])
        actual = {}
        for name in repair.RELATIONS:
            path = self.target / name / ("year_month=" + MONTH) / "data.parquet"
            fields = pq.ParquetFile(self.paths[name]).schema_arrow.names
            self.assertTrue(pq.ParquetFile(path).schema_arrow.equals(pq.ParquetFile(self.paths[name]).schema_arrow))
            rows = pq.ParquetFile(path).read().to_pylist()
            for row in rows:
                row.setdefault("year_month", MONTH)
            actual[name] = Counter(tuple(row[field] for field in repair.FIELDS) for row in rows)
            expected = Counter(tuple(row[field] for field in repair.FIELDS) for row in transformed(self.rows[name]))
            self.assertEqual(actual[name], expected)
            self.assertEqual(original_hashes[name], repair.sha256(self.paths[name]))
            self.assertEqual(len(rows), len(self.rows[name]))
            self.assertEqual(len(fields), 10 if name == "root" else 11)
            manifest = json.loads((path.parent / "manifest.json").read_text())
            self.assertEqual(manifest["output"]["sha256"], repair.sha256(path))
            self.assertEqual(len(manifest["reconciliation"]), 2)
        self.assertEqual(set(actual["root"]), set(actual["clean"]))
        self.assertEqual(set(actual["clean"].values()), {1})
        self.assertEqual(sum(charge["bytes"] for charge in result["read_charges"]), result["resource_profile"]["read_bytes_charged"])

    def test_self_wallet_and_reciprocal_collision_rows_are_retained(self):
        self.build()
        rows = pq.ParquetFile(self.target / "root" / ("year_month=" + MONTH) / "data.parquet").read().to_pylist()
        self.assertEqual(sum(row["proxyWallet"] == row["counterparty"] == "S" for row in rows), 2)
        self.assertEqual(sum(row["conditionId"] == "1004" for row in rows), 4)

    def test_default_admission_requires_all_frozen_months(self):
        with self.assertRaisesRegex(repair.RepairBlocked, "month coverage"):
            repair.admit_census(self.manifest, self.qa)

    def test_copied_pair_proof_and_cleaning_gate_cannot_be_relaxed(self):
        for path in ("copied", "cleaning", "exit", "reopen"):
            with self.subTest(path=path):
                manifest, qa = copy.deepcopy(self.manifest), copy.deepcopy(self.qa)
                if path == "copied":
                    manifest["census"]["leaves"][0]["metrics"]["root"]["full_label"]["excess_observed_copied"] = 1
                elif path == "cleaning":
                    manifest["census"]["root_distinct_to_clean_reconciled"] = False
                elif path == "exit":
                    qa["exit_profile"]["exit_status"] = 2
                else:
                    manifest["census"]["final_input_identity_reopened"] = False
                with self.assertRaises(repair.RepairBlocked):
                    repair.admit_census(manifest, qa, months=[MONTH])

    def test_missing_key_null_role_gap_and_overlap_proofs_are_refused(self):
        for change in ("missing_key", "null_role", "gap", "overlap"):
            with self.subTest(change=change):
                manifest = copy.deepcopy(self.manifest)
                if change == "missing_key":
                    del manifest["inventories"]["root"][0]["fields"]["counterparty"]
                elif change == "null_role":
                    manifest["census"]["leaves"][0]["metrics"]["support"]["root"]["missing_role_rows"] = 1
                elif change == "gap":
                    manifest["census"]["leaves"][1]["lower_inclusive"] += 1
                else:
                    manifest["census"]["leaves"][1]["lower_inclusive"] -= 1
                with self.assertRaises(repair.RepairBlocked):
                    repair.admit_census(manifest, self.qa, months=[MONTH])

    def test_changed_or_double_repaired_physical_input_is_refused(self):
        fields = pq.ParquetFile(self.paths["root"]).schema_arrow.names
        write_rows(self.paths["root"], transformed(self.rows["root"]), fields)
        with self.assertRaisesRegex(ValueError, "identity mismatch|frozen input changed"):
            self.reviewed()
        self.assertFalse(self.target.exists())

    def test_existing_or_original_overlapping_destination_is_refused(self):
        self.target.mkdir()
        with self.assertRaisesRegex(repair.RepairBlocked, "already exists"):
            self.reviewed()
        with self.assertRaisesRegex(repair.RepairBlocked, "overlaps"):
            repair.preflight(self.plan, self.paths["root"].parent / "new_run", self.binding, self.source)

    def test_insufficient_capacity_fails_before_output_creation(self):
        with mock.patch.object(repair.shutil, "disk_usage", return_value=mock.Mock(free=0)):
            with self.assertRaisesRegex(repair.RepairBlocked, "insufficient capacity"):
                self.reviewed()
        self.assertFalse(self.target.exists())

    def test_unexpected_original_parquet_file_is_refused(self):
        extra = self.paths["root"].parent / "extra.parquet"
        fields = pq.ParquetFile(self.paths["root"]).schema_arrow.names
        write_rows(extra, self.rows["root"], fields)
        with self.assertRaisesRegex(ValueError, "directory/file set changed"):
            self.reviewed()
        self.assertFalse(self.target.exists())

    def test_actual_combined_leaf_payload_is_gated_before_exact_comparison(self):
        item = self.plan[0]
        path = self.root / "corrected_for_payload.parquet"
        fields = pq.ParquetFile(self.paths["root"]).schema_arrow.names
        write_rows(path, transformed(self.rows["root"]), fields)
        info = repair.lineage.footer_info(path, "root_transformed")
        con = repair.connection(self.root)
        try:
            with mock.patch.dict(repair.CAPS, {"maximum_leaf_payload_bytes": 1}), \
                 mock.patch.object(repair, "full_difference", side_effect=AssertionError("must not group")):
                with self.assertRaisesRegex(repair.RepairBlocked, "logical payload"):
                    repair.check_leaf(con, item, path, info, item["leaves"][0], {"read_bytes": 0})
        finally:
            con.close()

    def test_saved_relation_pair_payload_is_gated_before_preflight_or_copy(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["census"]["leaves"][0]["metrics"]["support"]["root"]["logical_payload_bytes"] = \
            repair.CAPS["maximum_leaf_payload_bytes"] // 2 + 1
        with self.assertRaisesRegex(repair.RepairBlocked, "resource admission"):
            repair.admit_census(manifest, self.qa, months=[MONTH])
        self.assertFalse(self.target.exists())

    def test_bounded_leaf_tables_are_used_and_dropped_before_next_leaf(self):
        item = self.plan[0]
        path = self.root / "corrected_materialization.parquet"
        fields = pq.ParquetFile(self.paths["root"]).schema_arrow.names
        write_rows(path, transformed(self.rows["root"]), fields)
        info = repair.lineage.footer_info(path, "root_transformed")
        con = repair.connection(self.root)
        try:
            budget = {"read_bytes": 0}
            for leaf in item["leaves"]:
                with mock.patch.object(repair, "full_difference", wraps=repair.full_difference) as exact:
                    repair.check_leaf(con, item, path, info, leaf, budget)
                self.assertEqual([call.args[1:3] for call in exact.call_args_list],
                                 [("expected_values", "output_values"), ("original_values", "output_values")])
                self.assertTrue(all(call.args[5] == 0 for call in exact.call_args_list))
                self.assertEqual(con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name IN "
                                             "('original_values','output_values')").fetchone()[0], 0)
                self.assertEqual(con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name='expected_values'").fetchone()[0], 0)
            charges = budget["read_charges"]
            self.assertEqual(sum(value["stage"].startswith("leaf_counts:") for value in charges), 4)
            self.assertEqual(sum(value["stage"].startswith("leaf_materialize:") for value in charges), 4)
            self.assertTrue(all(value["bytes"] == 0 for value in charges if value["stage"].startswith("exact:")))
            with mock.patch.object(repair, "full_difference", side_effect=repair.RepairBlocked("exact failure")):
                with self.assertRaisesRegex(repair.RepairBlocked, "exact failure"):
                    repair.check_leaf(con, item, path, info, item["leaves"][0], {"read_bytes": 0})
            self.assertEqual(con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name IN "
                                         "('original_values','output_values')").fetchone()[0], 0)
        finally:
            con.close()

    def test_copy_write_ceiling_is_enforced_and_process_settings_restored(self):
        previous = resource.getrlimit(resource.RLIMIT_FSIZE)
        previous_signal = signal.getsignal(signal.SIGXFSZ)
        path = self.root / "over_limit.bin"
        with self.assertRaises(OSError):
            with repair.copy_size_limit(1024):
                self.assertLessEqual(resource.getrlimit(resource.RLIMIT_FSIZE)[0], 1024)
                self.assertEqual(signal.getsignal(signal.SIGXFSZ), signal.SIG_IGN)
                with path.open("wb") as stream:
                    stream.write(b"x" * 2048)
        self.assertLessEqual(path.stat().st_size, 1024)
        self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), previous)
        self.assertEqual(signal.getsignal(signal.SIGXFSZ), previous_signal)
        stricter = (512, previous[1])
        resource.setrlimit(resource.RLIMIT_FSIZE, stricter)
        try:
            with repair.copy_size_limit(1024):
                self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE)[0], 512)
            self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), stricter)
        finally:
            resource.setrlimit(resource.RLIMIT_FSIZE, previous)

    def test_read_budget_refuses_query_before_execution(self):
        con = mock.Mock()
        budget = {"read_bytes": repair.CAPS["maximum_read_bytes"]}
        with self.assertRaisesRegex(repair.RepairBlocked, "read budget"):
            repair.full_difference(con, "left", "right", repair.FIELDS, budget, 1)
        con.execute.assert_not_called()

    def check_corrupt_output(self, change):
        item = self.plan[0]
        rows = transformed(self.rows["root"])
        false_indices = [index for index, row in enumerate(rows) if row["is_maker"] is False]
        if change == "wallet":
            rows[false_indices[0]]["counterparty"] = "WRONG"
        elif change == "nonwallet":
            rows[false_indices[0]]["price"] = 0.38
        elif change == "multiplicity":
            rows[false_indices[-1]] = copy.deepcopy(rows[false_indices[0]])
        elif change == "null_role":
            rows[false_indices[0]]["is_maker"] = None
        elif change == "outside_month":
            rows[false_indices[0]]["timestamp"] = UPPER
        path = self.root / ("corrupt_" + change + ".parquet")
        fields = pq.ParquetFile(self.paths["root"]).schema_arrow.names
        write_rows(path, rows, fields)
        info = repair.lineage.footer_info(path, "root_transformed")
        con = repair.connection(self.root)
        try:
            with self.assertRaises(repair.RepairBlocked):
                for leaf in item["leaves"]:
                    repair.check_leaf(con, item, path, info, leaf, {"read_bytes": 0})
        finally:
            con.close()

    def test_full_multiset_checks_reject_wallet_other_field_multiplicity_null_and_outside_rows(self):
        for change in ("wallet", "nonwallet", "multiplicity", "null_role", "outside_month"):
            with self.subTest(change=change):
                self.check_corrupt_output(change)

    def test_reviewed_preflight_differences_are_refused_before_copy(self):
        reviewed = self.reviewed()
        reviewed["caps"]["threads"] += 1
        with mock.patch.object(repair, "repair_file", side_effect=AssertionError("must not copy")):
            with self.assertRaisesRegex(repair.RepairBlocked, "reviewed contract"):
                self.build(reviewed)
        self.assertFalse(self.target.exists())

    def test_failure_retains_unpublished_stage_and_preserves_originals(self):
        hashes = {name: repair.sha256(path) for name, path in self.paths.items()}
        with mock.patch.object(repair, "check_leaf", side_effect=repair.RepairBlocked("synthetic QA failure")):
            with self.assertRaisesRegex(repair.RepairBlocked, "synthetic QA failure"):
                self.build()
        self.assertFalse(self.target.exists())
        pending = list(self.root.glob(".new_run.staging-*"))
        self.assertEqual(len(pending), 1)
        self.assertEqual(json.loads((pending[0] / "failure.json").read_text())["status"], "repair_incomplete")
        self.assertTrue(list(pending[0].rglob("data.parquet")))
        self.assertEqual(hashes, {name: repair.sha256(path) for name, path in self.paths.items()})

    def test_source_change_blocks_atomic_final_publication(self):
        fresh = self.reviewed()
        with mock.patch.object(repair, "source_snapshot", return_value={"head": "changed"}):
            with self.assertRaisesRegex(repair.RepairBlocked, "source changed before publication"):
                repair.build_run(self.plan, self.target, fresh, fresh)
        self.assertFalse(self.target.exists())

    def test_atomic_publication_cannot_replace_existing_target(self):
        staging = self.root / "staging"
        target = self.root / "existing"
        staging.mkdir()
        target.mkdir()
        (target / "keep.txt").write_text("original")
        with self.assertRaises(OSError):
            repair.atomic_publish(staging, target)
        self.assertEqual((target / "keep.txt").read_text(), "original")
        self.assertTrue(staging.exists())

    def test_digest_binding_duplicate_keys_and_nonfinite_metadata_are_refused(self):
        for raw, expected in ((b'{"a":1}', "0" * 64), (b'{"a":1,"a":2}', None),
                              (b'{"value":NaN}', None), (b'{"value":1e999}', None)):
            with self.subTest(raw=raw):
                path = self.root / "metadata.json"
                path.write_bytes(raw)
                with self.assertRaises(ValueError):
                    repair.read_json(path, expected or hashlib.sha256(raw).hexdigest(), 1024)

    def test_cli_production_guard_runs_before_source_or_data_discovery(self):
        argv = ["repair", "--census-manifest", "never.json", "--census-qa", "never_qa.json",
                "--expected-head", "a" * 40, "--run-dir", "never_output"]
        with mock.patch.object(repair.sys, "argv", argv), \
             mock.patch("production_guard.require_production_host", side_effect=RuntimeError("fixture blocked")), \
             mock.patch.object(repair, "source_snapshot", side_effect=AssertionError("must not inspect source")), \
             mock.patch.object(repair, "load_census", side_effect=AssertionError("must not inspect data")):
            with self.assertRaisesRegex(RuntimeError, "fixture blocked"):
                repair.main()


if __name__ == "__main__":
    unittest.main()
