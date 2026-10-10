"""Tiny synthetic flags/identity/publication tests; no production data reads."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.bot_filter import build_wallet_flags
from scripts import rebuild_polymarket_wallet_flags as rebuild

TRADE_SCHEMA = pa.schema([(name, pa.string() if kind == "string" else pa.int64() if kind == "int64"
                          else pa.float64() if kind == "double" else pa.bool_()) for name, kind in rebuild.TRADE_TYPES.items()])
SMALL_CAPS = {**rebuild.CAPS, "memory_limit": "128MB", "threads": 1, "spill_bytes": 0,
              "minimum_free_bytes": 0, "maximum_output_bytes": 32 * 1024**2,
              "maximum_metadata_bytes": 2 * 1024**2, "maximum_read_bytes": 2 * 1024**3}


def trade(wallet="a", counterparty="b", timestamp=rebuild.START_TIMESTAMP, role=True, cash=1.0, side="BUY"):
    return {"proxyWallet": wallet, "counterparty": counterparty, "timestamp": timestamp, "conditionId": "123",
            "usdcSize": cash, "price": 0.5, "side": side, "outcome": "YES", "eventSlug": "fixture",
            "is_maker": role, "year_month": "2024-01"}


def expanded(spacing=100, n=10, *, duplicates=False, self_wallet=False):
    legacy, corrected = [], []
    for index in range(n):
        first = trade(timestamp=rebuild.START_TIMESTAMP + index * spacing)
        second = {**first, "is_maker": False, "side": "SELL"}
        legacy.extend((first, second))
        corrected.extend((first, {**second, "proxyWallet": "b", "counterparty": "a"}))
    if duplicates:
        legacy.extend(deepcopy(legacy[:2]))
        corrected.extend(deepcopy(corrected[:2]))
    if self_wallet:
        first = trade("self", "self", rebuild.START_TIMESTAMP + 10, True)
        second = {**first, "is_maker": False, "side": "SELL"}
        legacy.extend((first, second))
        corrected.extend((first, second))
    return legacy, corrected


def write_trades(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=TRADE_SCHEMA), path, row_group_size=4)


def flag_connection(rows):
    con = duckdb.connect(":memory:")
    con.execute("SET threads=1")
    con.execute("SET memory_limit='128MB'")
    con.execute("SET max_temp_directory_size='0B'")
    con.execute("SET TimeZone='UTC'")
    table = pa.Table.from_pylist(rows, schema=TRADE_SCHEMA)
    con.register("fixture", table)
    con.execute(f"CREATE TEMP VIEW trades AS SELECT * FROM fixture WHERE timestamp>={rebuild.START_TIMESTAMP}")
    build_wallet_flags(con, verbose=False)
    return con


def write_flags(path, rows, *, unknown=False):
    con = flag_connection(rows)
    try:
        if unknown:
            con.execute("UPDATE wallet_flags SET flag_e=NULL,is_nonhuman=NULL WHERE proxyWallet='a'")
        con.execute("COPY wallet_flags TO " + rebuild.literal(str(path)) + " (FORMAT PARQUET)")
    finally:
        con.close()


def fixture_plan(root: Path, *, rows=None, unknown=False):
    legacy_rows, corrected_rows = expanded(duplicates=True, self_wallet=True) if rows is None else rows
    before = trade("prior", "prior", rebuild.START_TIMESTAMP - 1)
    legacy_rows, corrected_rows = [*legacy_rows, before], [*corrected_rows, before]
    legacy_root, corrected_root = root / "legacy", root / "repair" / "clean"
    old_path = legacy_root / "year_month=2024-01" / "data.parquet"
    new_path = corrected_root / "year_month=2024-01" / "data.parquet"
    write_trades(old_path, legacy_rows)
    write_trades(new_path, corrected_rows)
    metadata_path = new_path.parent / "manifest.json"
    rebuild.write_json(metadata_path, {"fixture": "saved monthly repair"})
    old, new = rebuild.footer(old_path), rebuild.footer(new_path)
    original = {"path": str(old_path), "relation": "clean", "month": "2024-01", "stat": old["stat"],
                "footer_sha256": old["footer_sha256"], "rows": old["rows"], "schema": old["schema"]}
    repaired = {"path": "data.parquet", "bytes": new["stat"]["bytes"], "rows": new["rows"],
                "schema": new["schema"], "sha256": rebuild.sha256(new_path)}
    output = {"relation": "clean", "month": "2024-01", "path": "clean/year_month=2024-01",
              "input": {**original, "sha256": rebuild.sha256(old_path)}, "output": repaired,
              "manifest_sha256": rebuild.sha256(metadata_path)}
    producer = {"status": "repair_complete", "data_certified": False, "downstream_adoption": "pending",
                "inputs": [original], "outputs": [output], "rows": {"clean": len(legacy_rows)},
                "reconciliation": {"exact_full11_multisets": True}}
    qa = {"status": "complete_saved_wallet_repair_manifest_qa", "data_certified": False, "downstream_adoption": "pending",
          "inputs": {"manifest": {"sha256": rebuild.REPAIR_SHA256, "bytes": 1_582_547}}, "exit_profile": {"exit_status": 0},
          "reconciliation": {"all_recorded_full11_and_other9_differences_zero": True}, "rows": producer["rows"],
          "coverage": [{"relation": "clean", "month": "2024-01", "rows": len(legacy_rows),
                        "monthly_manifest_sha256": output["manifest_sha256"]}]}
    plan = rebuild.repair_plan(producer, qa, legacy_root, corrected_root, months=("2024-01",))
    bindings = {}
    for label, value in (("repair_manifest", producer), ("repair_qa", qa)):
        path = root / (label + ".json")
        rebuild.write_json(path, value)
        bindings[label] = {"path": str(path), "bytes": path.stat().st_size, "sha256": rebuild.sha256(path)}
    historical = {}
    for label in ("learnability", "pipeline_data"):
        path = root / (label + ".parquet")
        write_flags(path, legacy_rows, unknown=unknown)
        historical[label] = (path, rebuild.sha256(path))
    source = {"head": "1" * 40, "sha256": {"analysis/bot_filter.py": rebuild.CLASSIFIER_SHA256}}
    target = root / "published_flags"
    preflight = rebuild.preflight(plan, legacy_root, corrected_root, historical, target, bindings, source)
    return plan, preflight, target, producer, qa


class ClassifierTests(unittest.TestCase):
    def test_exact_iti_gate_and_boundaries(self):
        rows = []
        for spacing in (0, 1, 9, 10, 119, 120, 121):
            wallet = "iti_" + str(spacing)
            rows.extend((trade(wallet), trade(wallet, timestamp=rebuild.START_TIMESTAMP + spacing)))
        con = flag_connection(rows)
        try:
            actual = {row[0]: row[1:] for row in con.execute(
                "SELECT proxyWallet,median_iti,flag_a_definite,flag_a_likely,is_nonhuman FROM wallet_flags").fetchall()}
            self.assertEqual(actual["iti_0"], (0.0, True, False, True))
            self.assertEqual(actual["iti_1"], (1.0, False, True, False))
            self.assertEqual(actual["iti_9"], (9.0, False, True, False))
            self.assertEqual(actual["iti_10"], (10.0, False, False, False))
            self.assertEqual(actual["iti_119"][0], 119.0)
            self.assertIsNone(actual["iti_120"][0])
            self.assertIsNone(actual["iti_121"][0])
        finally:
            con.close()

    def test_exact_trade_day_size_and_hour_count_thresholds(self):
        rows = []
        for n in (50, 51, 200, 201, 500, 501):
            rows.extend(trade("n_" + str(n), timestamp=rebuild.START_TIMESTAMP + i * 10) for i in range(n))
        for n in (500, 501):
            rows.extend(trade("hour_" + str(n), timestamp=rebuild.START_TIMESTAMP + (i % 24) * 3600) for i in range(n))
        con = flag_connection(rows)
        try:
            actual = {row[0]: row[1:] for row in con.execute(
                "SELECT proxyWallet,flag_b_likely,flag_b_definite,flag_c,flag_e FROM wallet_flags").fetchall()}
            self.assertFalse(actual["n_50"][3])
            self.assertTrue(actual["n_51"][3])
            self.assertFalse(actual["n_200"][0])
            self.assertTrue(actual["n_201"][0])
            self.assertFalse(actual["n_500"][1])
            self.assertTrue(actual["n_501"][1])
            self.assertFalse(actual["hour_500"][2])
            self.assertTrue(actual["hour_501"][2])
        finally:
            con.close()

    def test_exact_size_cv_and_hour_hhi_strict_inequalities(self):
        rows = []
        for name, difference in (("cv_below", 0.049), ("cv_boundary", 0.05), ("cv_above", 0.051)):
            rows.extend(trade(name, timestamp=rebuild.START_TIMESTAMP + i, cash=1 + (-1 if i % 2 else 1) * difference)
                        for i in range(100))
        # Two hours at 10% and sixteen at 5% give analytical HHI=.06;
        # twenty equal hours give .05 and sixteen equal hours give .0625.
        for name, counts in (("hhi_boundary", [100, 100] + [50] * 16),
                             ("hhi_below", [50] * 20), ("hhi_above", [64] * 16)):
            rows.extend(trade(name, timestamp=rebuild.START_TIMESTAMP + hour * 3600)
                        for hour, count in enumerate(counts) for _ in range(count))
        con = flag_connection(rows)
        try:
            actual = {row[0]: row[1:] for row in con.execute("SELECT proxyWallet,flag_e,flag_c FROM wallet_flags").fetchall()}
            self.assertTrue(actual["cv_below"][0])
            self.assertFalse(actual["cv_boundary"][0])
            self.assertFalse(actual["cv_above"][0])
            self.assertTrue(actual["hhi_below"][1])
            self.assertFalse(actual["hhi_boundary"][1])
            self.assertFalse(actual["hhi_above"][1])
        finally:
            con.close()

    def test_copied_zero_median_vs_corrected_positive_and_sparse_gate(self):
        for spacing, expected in ((100, 100.0), (300, None)):
            legacy, corrected = expanded(spacing=spacing)
            con0, con1 = flag_connection(legacy), flag_connection(corrected)
            try:
                med0 = con0.execute("SELECT median_iti FROM wallet_flags WHERE proxyWallet='a'").fetchone()[0]
                med1 = con1.execute("SELECT median_iti FROM wallet_flags WHERE proxyWallet='a'").fetchone()[0]
                self.assertEqual(med0, 0.0 if spacing == 100 else None)
                self.assertEqual(med1, expected)
                self.assertEqual(con0.execute("SELECT sum(n_trades) FROM wallet_flags").fetchone()[0], 20)
                self.assertEqual(con1.execute("SELECT sum(n_trades) FROM wallet_flags").fetchone()[0], 20)
            finally:
                con0.close()
                con1.close()


class RebuildTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="wallet_flags_fixture_",
                                                     dir=Path(tempfile.gettempdir()).resolve())
        self.root = Path(self.temporary.name)
        self.caps = patch.dict(rebuild.CAPS, SMALL_CAPS)
        self.caps.start()

    def tearDown(self):
        self.caps.stop()
        self.temporary.cleanup()

    def run_fixture(self, *, unknown=False, rows=None):
        plan, fresh, target, _, _ = fixture_plan(self.root, unknown=unknown, rows=rows)
        with patch.object(rebuild, "source_snapshot", return_value=fresh["source"]):
            summary = rebuild.build_run(plan, target, deepcopy(fresh), fresh, command=["synthetic-only"])
        return summary, target

    def test_complete_publication_conserves_cutoff_duplicates_self_and_all_sides(self):
        summary, target = self.run_fixture()
        self.assertEqual(summary["status"], "wallet_flags_rebuild_complete")
        self.assertFalse(summary["data_certified"])
        self.assertEqual(summary["downstream_adoption"], "pending")
        self.assertTrue(summary["reconciliation"]["exact_source_wallet_keys_and_counts"])
        for vintage in ("legacy", "corrected"):
            rows = summary["builds"][vintage]["rows"]
            self.assertEqual(rows["total_rows"], 25)
            self.assertEqual(rows["admitted_rows"], 24)
            self.assertEqual(rows["excluded_before_start_rows"], 1)
            self.assertEqual(summary["builds"][vintage]["flags"]["trades"], 24)
        self.assertEqual(summary["builds"]["legacy"]["flags"]["wallets"], 2)
        self.assertEqual(summary["builds"]["corrected"]["flags"]["wallets"], 3)
        con = duckdb.connect()
        try:
            self.assertEqual(con.execute("SELECT n_trades FROM read_parquet(?) WHERE proxyWallet='self'",
                                         [str(target / "wallet_flags.parquet")]).fetchone()[0], 2)
            self.assertEqual(con.execute("SELECT n_trades FROM read_parquet(?) WHERE proxyWallet='b'",
                                         [str(target / "wallet_flags.parquet")]).fetchone()[0], 11)
        finally:
            con.close()
        pair = summary["comparisons"][0]
        self.assertEqual(pair["comparison"], "corrected_vs_legacy_recomputed")
        self.assertTrue(pair["repair_only"])
        self.assertEqual(pair["right_only_wallets"], 1)
        self.assertEqual(len(summary["comparisons"]), 5)
        self.assertTrue((target / "repair_only_flag_pairs.parquet").is_file())
        counts_charges = [item for item in summary["resource_profile"]["charges"]
                          if item["stage"].startswith("source_wallet_counts:")]
        self.assertEqual(len(counts_charges), 2)
        self.assertLess(summary["resource_profile"]["charged_read_bytes"], rebuild.CAPS["maximum_read_bytes"])

    def test_historical_nulls_stay_unknown_and_selection_coalesces_false(self):
        summary, _ = self.run_fixture(unknown=True)
        comparison = next(item for item in summary["comparisons"] if item["comparison"] == "legacy_recomputed_vs_pipeline_data")
        criterion = next(item for item in comparison["criteria"] if item["criterion"] == "is_nonhuman")
        self.assertEqual(criterion["left_unknown"], 1)
        self.assertEqual(criterion["common_left_unknown_right_true"], 1)
        self.assertEqual(criterion["common_enter"], 0)
        self.assertEqual(criterion["selection_common_enter"], 1)
        self.assertEqual(summary["builds"]["legacy"]["flags"]["flag_null_counts"]["is_nonhuman"], 0)

    def test_single_wallet_and_empty_admitted_cohort(self):
        # Every raw row is below the cutoff; no wallet or classification is
        # fabricated. The published empty flag Parquet retains the exact schema.
        rows = ([trade(timestamp=rebuild.START_TIMESTAMP - 2)], [trade(timestamp=rebuild.START_TIMESTAMP - 2)])
        summary, target = self.run_fixture(rows=rows)
        self.assertEqual(summary["builds"]["legacy"]["rows"]["admitted_rows"], 0)
        self.assertEqual(summary["builds"]["corrected"]["flags"]["wallets"], 0)
        self.assertEqual(pq.ParquetFile(target / "wallet_flags.parquet").metadata.num_rows, 0)

    def test_null_blank_and_case_colliding_wallets_fail(self):
        for wallets in ((None,), ("",), ("   ",), (" a ",), ("A", "a"), ("a", " a ")):
            with self.subTest(wallets=wallets):
                con = flag_connection([trade(wallet) for wallet in wallets])
                try:
                    with self.assertRaisesRegex(rebuild.RebuildBlocked, "wallet keys"):
                        rebuild.validate_flags(con, "wallet_flags", expected_trades=len(wallets))
                finally:
                    con.close()

    def test_exact_source_wallet_coverage_and_counts_reject_balanced_corruption(self):
        plan, fresh, _, _, _ = fixture_plan(self.root)
        original = rebuild.build_wallet_flags
        for label, sql in (("substituted_key", "UPDATE wallet_flags SET proxyWallet='ghost' WHERE proxyWallet='a'"),
                           ("shifted_counts", "UPDATE wallet_flags SET n_trades=CASE WHEN proxyWallet='a' "
                            "THEN n_trades-1 ELSE n_trades+1 END")):
            with self.subTest(label=label):
                staging = self.root / label
                staging.mkdir()

                def altered(con, verbose=False):
                    stats = original(con, verbose=verbose)
                    con.execute(sql)
                    return stats

                with patch.object(rebuild, "build_wallet_flags", side_effect=altered), \
                     self.assertRaisesRegex(rebuild.RebuildBlocked, "exact source raw-wallet coverage/per-wallet"):
                    rebuild.rebuild_one(fresh["datasets"]["legacy"], "legacy", staging,
                                        {"read_bytes": 0, "charges": []})
                self.assertFalse((staging / "legacy_recomputed_flags.parquet").exists())

    def test_padded_source_keys_fail_before_classifier(self):
        rows = ([trade(" a ")], [trade(" a ")])
        plan, fresh, _, _, _ = fixture_plan(self.root, rows=rows)
        staging = self.root / "padded_source"
        staging.mkdir()
        with patch.object(rebuild, "build_wallet_flags", side_effect=AssertionError("classifier should not run")), \
             self.assertRaisesRegex(rebuild.RebuildBlocked, "trade integrity"):
            rebuild.rebuild_one(fresh["datasets"]["legacy"], "legacy", staging, {"read_bytes": 0, "charges": []})

    def test_padded_historical_flag_keys_fail_lower_only_consumer_contract(self):
        con = flag_connection([trade("a")])
        try:
            con.execute("UPDATE wallet_flags SET proxyWallet=' a '")
            with self.assertRaisesRegex(rebuild.RebuildBlocked, "padded"):
                rebuild.validate_flags(con, "wallet_flags", allow_unknown=True)
        finally:
            con.close()

    def test_recomputed_null_flags_fail_historical_null_counts_admitted(self):
        con = flag_connection([trade()])
        try:
            con.execute("UPDATE wallet_flags SET flag_c=NULL")
            with self.assertRaisesRegex(rebuild.RebuildBlocked, "flag payload"):
                rebuild.validate_flags(con, "wallet_flags")
            result = rebuild.validate_flags(con, "wallet_flags", allow_unknown=True)
            self.assertEqual(result["flag_null_counts"]["flag_c"], 1)
        finally:
            con.close()

    def test_metadata_only_preflight_never_runs_classifier_or_decodes_trade_rows(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        with patch.object(rebuild, "build_wallet_flags", side_effect=AssertionError("classifier ran")), \
             patch.object(rebuild.duckdb, "connect", side_effect=AssertionError("DuckDB opened")):
            newer = rebuild.preflight(plan, Path(fresh["datasets"]["legacy"]["root"]), Path(fresh["datasets"]["corrected"]["root"]),
                {name: (Path(info["path"]), info["expected_content_sha256"]) for name, info in fresh["historical_flags"].items()},
                target, fresh["binding"], fresh["source"])
        self.assertEqual(newer["datasets"], fresh["datasets"])
        self.assertEqual(newer["planned_read_bytes"], 8 * newer["trade_input_bytes"] +
                         3 * sum(info["stat"]["bytes"] for info in newer["historical_flags"].values()) +
                         16 * rebuild.CAPS["maximum_output_bytes"])

    def test_full_layout_extra_partition_file_and_symlink_fail(self):
        plan, fresh, _, _, _ = fixture_plan(self.root)
        dataset = fresh["datasets"]["legacy"]
        path = Path(dataset["root"])
        extra = path / "year_month=2024-02"
        extra.mkdir()
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "root layout"):
            rebuild.dataset_snapshot(path, plan, "legacy")
        extra.rmdir()
        extra_file = path / "year_month=2024-01" / "unexpected.parquet"
        extra_file.touch()
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "partition layout"):
            rebuild.dataset_snapshot(path, plan, "legacy")
        extra_file.unlink()
        link = self.root / "flags_link.parquet"
        link.symlink_to(Path(fresh["historical_flags"]["learnability"]["path"]))
        with self.assertRaises(ValueError):
            rebuild.footer(link)

    def test_stat_footer_content_drift_and_wrong_hash_fail_closed(self):
        plan, fresh, _, _, _ = fixture_plan(self.root)
        info = fresh["datasets"]["legacy"]["files"][0]
        altered = deepcopy(info)
        altered["stat"]["mtime_ns"] += 1
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "input stat"):
            rebuild.verify_content([altered], {"read_bytes": 0, "charges": []}, "fixture")
        altered = deepcopy(info)
        altered["expected_content_sha256"] = "0" * 64
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "content hash"):
            rebuild.verify_content([altered], {"read_bytes": 0, "charges": []}, "fixture")
        # Rewriting even equal rows changes the frozen stat/physical footer.
        path = Path(info["path"])
        rows = pq.read_table(path, partitioning=None).to_pylist()
        pq.write_table(pa.Table.from_pylist(rows, schema=TRADE_SCHEMA), path, row_group_size=3)
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "metadata|identity"):
            rebuild.dataset_snapshot(Path(fresh["datasets"]["legacy"]["root"]), plan, "legacy")

    def test_reviewed_contract_changes_block_before_output(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        for field in ("source", "contract", "caps", "datasets", "historical_flags", "binding"):
            altered = deepcopy(fresh)
            altered[field] = {}
            with self.subTest(field=field), self.assertRaisesRegex(rebuild.RebuildBlocked, "reviewed contract"):
                rebuild.build_run(plan, target, altered, fresh)
            self.assertFalse(target.exists())
        altered = deepcopy(fresh)
        altered["observed_free_bytes"] += 1
        rebuild.reviewed_contract(altered, fresh)

    def test_failure_retains_staging_evidence_and_inputs(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        original_hash = rebuild.sha256(Path(plan[0]["legacy"]["path"]))
        with patch.object(rebuild, "rebuild_one", side_effect=RuntimeError("synthetic failure")), \
             self.assertRaisesRegex(RuntimeError, "synthetic failure"):
            rebuild.build_run(plan, target, deepcopy(fresh), fresh)
        stages = list(self.root.glob(".published_flags.staging-*"))
        self.assertEqual(len(stages), 1)
        failed = json.loads((stages[0] / "failure.json").read_text())
        self.assertEqual(failed["status"], "wallet_flags_rebuild_incomplete")
        self.assertFalse(target.exists())
        self.assertEqual(rebuild.sha256(Path(plan[0]["legacy"]["path"])), original_hash)

    def test_final_input_reopen_detects_late_layout_change(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        original = rebuild.comparison_outputs

        def drift(*args):
            result = original(*args)
            (Path(fresh["datasets"]["corrected"]["root"]) / "new_partition").mkdir()
            return result

        with patch.object(rebuild, "comparison_outputs", side_effect=drift), \
             patch.object(rebuild, "source_snapshot", return_value=fresh["source"]), \
             self.assertRaisesRegex(rebuild.RebuildBlocked, "root layout"):
            rebuild.build_run(plan, target, deepcopy(fresh), fresh)
        self.assertFalse(target.exists())
        self.assertTrue(list(self.root.glob(".published_flags.staging-*/failure.json")))

    def test_final_source_reopen_blocks_publication(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        with patch.object(rebuild, "source_snapshot", return_value={"head": "2" * 40, "sha256": {}}), \
             self.assertRaisesRegex(rebuild.RebuildBlocked, "source identity changed"):
            rebuild.build_run(plan, target, deepcopy(fresh), fresh)
        self.assertFalse(target.exists())
        self.assertTrue(list(self.root.glob(".published_flags.staging-*/failure.json")))

    def test_null_timestamps_fail_before_classifier(self):
        rows = ([trade(timestamp=None)], [trade(timestamp=None)])
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "timestamp footer"):
            fixture_plan(self.root, rows=rows)

    def test_output_exact_reopen_detects_changed_payload(self):
        con = flag_connection([trade()])
        try:
            path = self.root / "flags.parquet"
            con.execute("COPY wallet_flags TO " + rebuild.literal(str(path)) + " (FORMAT PARQUET)")
            con.execute("UPDATE wallet_flags SET is_nonhuman=NOT is_nonhuman")
            with self.assertRaisesRegex(rebuild.RebuildBlocked, "saved output differs"):
                rebuild.exact_reopen(con, "wallet_flags", path, {"read_bytes": 0, "charges": []})
        finally:
            con.close()

    def test_atomic_no_replace_preserves_existing_target(self):
        target, staging = self.root / "target", self.root / "staging"
        target.mkdir()
        staging.mkdir()
        (target / "existing").write_text("preserve")
        with self.assertRaises(OSError):
            rebuild.atomic_publish(staging, target)
        self.assertEqual((target / "existing").read_text(), "preserve")
        self.assertTrue(staging.is_dir())

    def test_overwrite_and_input_overlap_block_preflight(self):
        plan, fresh, target, _, _ = fixture_plan(self.root)
        target.mkdir()
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "fresh"):
            rebuild.disjoint_output(target, [], [])
        with self.assertRaisesRegex(rebuild.RebuildBlocked, "overlaps"):
            rebuild.disjoint_output(Path(fresh["datasets"]["legacy"]["root"]) / "new_run",
                                    [Path(fresh["datasets"]["legacy"]["root"])], [])

    def test_read_budget_and_real_copy_ceiling(self):
        with patch.dict(rebuild.CAPS, {"maximum_read_bytes": 3}):
            budget = {"read_bytes": 0, "charges": []}
            rebuild.charge(budget, 3, "admitted")
            with self.assertRaisesRegex(rebuild.RebuildBlocked, "before query"):
                rebuild.charge(budget, 1, "not admitted")
            self.assertEqual(budget["read_bytes"], 3)
        con = duckdb.connect()
        try:
            with patch.dict(rebuild.CAPS, {"maximum_output_bytes": rebuild.CAPS["maximum_metadata_bytes"] + 256}), \
                 self.assertRaises(Exception):
                rebuild.copy_output(con, "SELECT i FROM range(10000) t(i)", "too_large.parquet", self.root,
                                    {"read_bytes": 0, "charges": []})
            self.assertFalse((self.root / "too_large.parquet").exists())
        finally:
            con.close()

    def test_saved_repair_identity_limits_coverage_and_count_gates(self):
        _, fresh, _, producer, qa = fixture_plan(self.root)
        legacy, corrected = (Path(fresh["datasets"][name]["root"]) for name in ("legacy", "corrected"))
        for kind in ("certified", "adopted", "missing_month", "wrong_hash", "bad_count"):
            doc, receipt = deepcopy(producer), deepcopy(qa)
            if kind == "certified":
                doc["data_certified"] = True
            elif kind == "adopted":
                receipt["downstream_adoption"] = "adopted"
            elif kind == "missing_month":
                doc["outputs"] = []
            elif kind == "wrong_hash":
                receipt["inputs"]["manifest"]["sha256"] = "0" * 64
            else:
                doc["outputs"][0]["output"]["rows"] += 1
            with self.subTest(kind=kind), self.assertRaises(rebuild.RebuildBlocked):
                rebuild.repair_plan(doc, receipt, legacy, corrected, months=("2024-01",))


class ProductionAdmissionTests(unittest.TestCase):
    def test_cli_body_requires_reviewed_digest_before_guard_or_data(self):
        args = []
        for name in ("repair-manifest", "repair-qa", "legacy-clean", "corrected-clean", "learnability-flags", "pipeline-data-flags", "run-dir"):
            args += ["--" + name, "/not_opened/" + name]
        args += ["--learnability-flags-sha256", "0" * 64, "--pipeline-data-flags-sha256", "0" * 64,
                 "--expected-head", "0" * 40, "--expected-source-sha256", "analysis/bot_filter.py=" + rebuild.CLASSIFIER_SHA256,
                 "--reviewed-preflight", "/not_opened/preflight.json"]
        with patch("production_guard.require_production_host", side_effect=AssertionError("guard should not be reached")), \
             self.assertRaisesRegex(rebuild.RebuildBlocked, "separately reviewed"):
            rebuild.main(args)

    def test_cli_local_production_guard_blocks_before_opening_data(self):
        args = []
        for name in ("repair-manifest", "repair-qa", "legacy-clean", "corrected-clean", "learnability-flags", "pipeline-data-flags", "run-dir"):
            args += ["--" + name, "/not_opened/" + name]
        args += ["--learnability-flags-sha256", "0" * 64, "--pipeline-data-flags-sha256", "0" * 64,
                 "--expected-head", "0" * 40, "--expected-source-sha256", "analysis/bot_filter.py=" + rebuild.CLASSIFIER_SHA256,
                 "--preflight", "/not_opened/preflight"]
        with patch("production_guard.platform.system", return_value="Darwin"), \
             patch.object(rebuild, "load_repair", side_effect=AssertionError("data should not be opened")), \
             self.assertRaisesRegex(RuntimeError, "Production computation is blocked"):
            rebuild.main(args)

    def test_expected_source_head_dirty_checkout_and_hash_drift_fail(self):
        hashes = {path: "0" * 64 for path in rebuild.SOURCE_PATHS}
        hashes["analysis/bot_filter.py"] = rebuild.CLASSIFIER_SHA256
        for head, status, message in (("2" * 40, "", "HEAD"), ("1" * 40, " M tracked.py\n", "clean")):
            with self.subTest(status=status), patch.object(rebuild.subprocess, "check_output", side_effect=[head, status]), \
                 self.assertRaisesRegex(rebuild.RebuildBlocked, message):
                rebuild.source_snapshot("1" * 40, hashes)
        with patch.object(rebuild.subprocess, "check_output", side_effect=["1" * 40, ""]), \
             patch.object(rebuild.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)), \
             patch.object(rebuild, "sha256", return_value="f" * 64), \
             self.assertRaisesRegex(rebuild.RebuildBlocked, "source differs"):
            rebuild.source_snapshot("1" * 40, hashes)

    def test_reviewed_preflight_digest_rejects_rehashed_or_wrong_document(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as directory:
            path = Path(directory) / "reviewed.json"
            raw = b'{"status":"preflight_complete"}\n'
            path.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "SHA256"):
                rebuild.read_json(path, "0" * 64, 1024)
            document, _ = rebuild.read_json(path, hashlib.sha256(raw).hexdigest(), 1024)
            with self.assertRaisesRegex(rebuild.RebuildBlocked, "reviewed contract"):
                rebuild.reviewed_contract(document, {"status": "preflight_complete", "source": {"head": "1" * 40}})


if __name__ == "__main__":
    unittest.main()
