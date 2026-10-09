"""Metadata-only synthetic evidence fixtures: no builders or Parquet imports."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import audit_polymarket_wallet_repair_manifest as audit


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode(value))
    return {"path": str(path.resolve()), "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def distribute(total, number):
    return [total // number + (index < total % number) for index in range(number)]


class RepairManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="repair_manifest_metadata_")
        self.base = Path(self.temporary.name).resolve()
        self.run = self.base / "repair"
        self.destination = self.base / "qa"
        self.census_path, self.qa_path = self.base / "census.json", self.base / "census_qa.json"
        self.profile_path, self.receipt_path = self.base / "repair.time.txt", self.base / "repair.receipt.json"
        self.head = "a" * 40
        self.documents = {}
        census = {"status": "published_pair_census_complete", "data_certified": False,
            "inputs": dict(audit.INPUT_BASES), "footer_rows": dict(audit.TOTALS), "inventories": {},
            "census": {"status": "published_pair_census_complete", "data_certified": False,
                "completed_months": audit.months(), "final_input_identity_reopened": True,
                "root_distinct_to_clean_reconciled": True,
                "global": {"cleaning": {"failed_full11_reconciliation_leaves": 0}}, "leaves": []}}
        for month in audit.months():
            lower, upper = audit.bounds(month)
            widths = distribute(upper - lower, 7)
            for width in widths:
                census["census"]["leaves"].append({"month": month, "lower_inclusive": lower,
                    "upper_exclusive": lower + width, "metrics": {"support": {}}})
                lower += width
        self.monthly = {}
        originals, outputs, charges = [], [], []
        for relation in audit.RELATIONS:
            census["inventories"][relation] = []
            maker_counts = distribute(audit.TOTALS[relation] // 2, 44)
            for month, makers in zip(audit.months(), maker_counts):
                lower, upper = audit.bounds(month)
                schema = "synthetic frozen schema " + relation
                path = str(Path(audit.INPUT_BASES[relation]) / ("year_month=" + month) / "original.parquet")
                info = {"partition_month": month, "relation": audit.RELATIONS[relation], "path": path,
                    "schema": schema, "rows": 2 * makers, "bytes": 1_000_000, "mtime_ns": 10,
                    "footer_sha256": "b" * 64, "row_groups": [{"rows": 2 * makers,
                        "compressed_bytes": 20_000, "stats": {"timestamp": {"min": lower, "max": upper - 1, "null_count": 0}}}]}
                census["inventories"][relation].append(info)
                original = {"relation": relation, "month": month, "path": path, "schema": schema,
                    "rows": 2 * makers, "footer_sha256": "b" * 64,
                    "stat": {"device": 1, "inode": 2, "bytes": 1_000_000, "mtime_ns": 10, "ctime_ns": 11}}
                originals.append(original)
                emitted = {"path": "data.parquet", "schema": schema, "rows": 2 * makers,
                           "bytes": 100_000, "sha256": "c" * 64}
                doc = {"schema_version": "polymarket_wallet_repair_file_v1", "status": "repair_file_complete",
                    "relation": relation, "month": month, "data_certified": False,
                    "input": {**copy.deepcopy(original), "sha256": "d" * 64}, "output": emitted,
                    "maximum_copy_file_bytes": 2_000_000, "separately_reserved_metadata_bytes": 1024**2,
                    "grain": "Original published wallet-row multiplicity", "transform": audit.TRANSFORM,
                    "counterparty_action": audit.ACTION, "reconciliation": [],
                    "timestamp_pruning": {"source_overlap_bytes": 140_000, "output_overlap_bytes": 35_000,
                        "preserve_insertion_order": True, "global_sort_performed": False}}
                file = {"relation": relation, "month": month}
                def charge(stage, amount, leaf=None):
                    charges.append({"stage": stage, "bytes": amount, "file": file, "leaf": leaf})
                charge("input_sha256_before", 1_000_000); charge("wallet_only_copy", 1_000_000)
                old_leaves = [leaf for leaf in census["census"]["leaves"] if leaf["month"] == month]
                for old, leaf_makers in zip(old_leaves, distribute(makers, 7)):
                    leaf = {"lower_inclusive": old["lower_inclusive"], "upper_exclusive": old["upper_exclusive"]}
                    values = {"row_count": 2 * leaf_makers, "maker_rows": leaf_makers,
                        "nonmaker_rows": leaf_makers, "invalid_rows": 0, "logical_payload_bytes": 400 * leaf_makers}
                    old["metrics"]["support"][relation] = copy.deepcopy(values)
                    doc["reconciliation"].append({**leaf, "counts": values,
                        "materialized_pair_payload_bytes": 800 * leaf_makers,
                        "full11_expected_to_output": {"left_only_rows": 0, "right_only_rows": 0},
                        "unchanged_other9_fields": {"left_only_rows": 0, "right_only_rows": 0}})
                    charge("leaf_counts:original_leaf", 20_000, leaf); charge("leaf_counts:output_leaf", 5_000, leaf)
                    charge("leaf_materialize:original", 20_000, leaf); charge("leaf_materialize:output", 5_000, leaf)
                    for stage in ("exact:expected_values:except_all:output_values", "exact:output_values:except_all:expected_values",
                                  "exact:original_values:except_all:output_values", "exact:output_values:except_all:original_values"):
                        charge(stage, 0, leaf)
                charge("output_sha256", 100_000); charge("input_sha256_after", 1_000_000)
                self.monthly[relation, month] = doc
                outputs.append({"relation": relation, "month": month, "path": relation + "/year_month=" + month,
                    "input": copy.deepcopy(doc["input"]), "output": copy.deepcopy(emitted),
                    "manifest_sha256": "0" * 64, "validation_leaves": 7})
        self.census = census
        census_id = write(self.census_path, census)
        self.qa = {"status": "complete_saved_wallet_census_manifest_qa", "data_certified": False,
            "exit_profile": {"exit_status": 0}, "completed_months": audit.months(), "leaf_count": 308,
            "final_input_identity_reopened_recorded": True, "inputs": {"manifest": census_id}}
        qa_id = write(self.qa_path, self.qa)
        self.digests = mock.patch.multiple(audit, CENSUS_SHA256=census_id["sha256"], QA_SHA256=qa_id["sha256"])
        self.digests.start()
        command = ["scripts/repair_polymarket_wallet_attribution.py", "--census-manifest", str(self.census_path),
            "--census-qa", str(self.qa_path), "--expected-head", self.head, "--run-dir", str(self.run),
            "--reviewed-preflight", str(self.base / "preflight.json"), "--approved-preflight-sha256", "e" * 64]
        self.manifest = {"schema_version": "polymarket_wallet_repair_v1", "status": "repair_complete",
            "data_certified": False, "downstream_adoption": "pending", "caps": dict(audit.CAPS),
            "source": {"head": self.head, "sha256": dict(audit.SOURCES)},
            "binding": {"census": census_id, "census_qa": qa_id}, "rows": dict(audit.TOTALS),
            "leaf_memory_contract": audit.LEAF_MEMORY, "write_contract": audit.WRITE_CONTRACT,
            "command": command, "inputs": originals, "outputs": outputs, "read_charges": charges,
            "environment": {"python": "synthetic", "duckdb": "1.5.0", "platform": "linux"},
            "resource_profile": {"read_bytes_charged": sum(value["bytes"] for value in charges),
                "peak_rss_bytes": 1, "wall_seconds": 1.0,
                "memory_note": "64GB bounds DuckDB-managed memory, not process RSS; admitted leaf-pair tables are dropped before next leaf, spill is forbidden."},
            "reconciliation": {"exact_full11_multisets": True, "unchanged_other9_fields": True,
                "original_file_counts_types_and_multiplicities": True,
                "root_distinct_to_clean_preserved_by_bijective_transform": True, "original_inputs_reopened": True},
            "limits": "Wallet-only published representation repair; no native identity/action certification, cleaning changes, flag/base rebuild or scientific rerun.",
            "downstream_gate": "Previously saved wallet flags and analytic bases require independent lineage/revalidation before adoption of this repaired vintage."}
        self.receipt = {"status": "repair_complete", "run_dir": str(self.run), "rows": dict(audit.TOTALS),
            "downstream_adoption": "pending", "reviewed_preflight": {"path": str(self.base / "preflight.json"),
                "bytes": 1024, "sha256": "e" * 64}}
        self.profile = '\tCommand being timed: "/home/ubuntu/venv/bin/python -u ' + ' '.join(command) + '"\n\tExit status: 0\n'
        self.save()

    def tearDown(self):
        self.digests.stop(); self.temporary.cleanup()

    def save(self):
        for record in self.manifest["outputs"]:
            key = record["relation"], record["month"]
            if key in self.monthly:
                identity = write(self.run / key[0] / ("year_month=" + key[1]) / "manifest.json", self.monthly[key])
                record["manifest_sha256"] = identity["sha256"]
        identity = write(self.run / "manifest.json", self.manifest)
        self.expected = identity["sha256"]
        write(self.run / "summary.json", {**self.manifest, "manifest_sha256": self.expected})
        write(self.receipt_path, self.receipt)
        self.profile_path.write_text(self.profile)

    def build(self, **kwargs):
        return audit.build_review(self.run, self.census_path, self.qa_path, self.profile_path,
            self.receipt_path, kwargs.pop("destination", self.destination),
            kwargs.pop("expected_digest", self.expected), kwargs.pop("expected_head", self.head), **kwargs)

    def refuse(self):
        self.save()
        with self.assertRaises((ValueError, KeyError, TypeError)):
            self.build()
        self.assertFalse(self.destination.exists())

    def test_complete_metadata_only_receipt_reconciles_all_files_leaves_and_totals(self):
        result = self.build()
        self.assertEqual(result["file_count"], 88)
        self.assertEqual(result["relation_leaf_count"], 616)
        self.assertEqual(result["rows"], audit.TOTALS)
        self.assertEqual(len(result["coverage"]), 88)
        self.assertEqual(result["charged_read_bytes"], self.manifest["resource_profile"]["read_bytes_charged"])
        self.assertFalse(result["data_certified"])
        self.assertEqual(result["downstream_adoption"], "pending")
        self.assertIn("no trade rows", result["limits"])
        self.assertTrue((self.destination / "receipt.json").is_file())
        self.assertLess((self.destination / "receipt.json").stat().st_size, audit.MAX_RECEIPT)

    def test_expected_digest_and_head_fail_closed(self):
        for kwargs in ({"expected_digest": "0" * 64}, {"expected_head": "f" * 40}):
            with self.assertRaises(audit.ReviewBlocked):
                self.build(**kwargs)
        self.assertFalse(self.destination.exists())

    def test_summary_must_be_exact_projection(self):
        summary = json.loads((self.run / "summary.json").read_text())
        summary["rows"]["root"] += 1
        write(self.run / "summary.json", summary)
        with self.assertRaisesRegex(audit.ReviewBlocked, "summary"):
            self.build()

    def test_duplicate_missing_or_out_of_order_month_is_refused(self):
        self.manifest["outputs"][1] = copy.deepcopy(self.manifest["outputs"][0])
        self.refuse()

    def test_nonzero_exact_difference_is_refused_even_when_status_true(self):
        self.monthly["root", "2022-11"]["reconciliation"][0]["full11_expected_to_output"]["right_only_rows"] = 1
        self.refuse()

    def test_leaf_gap_or_duplicate_is_refused(self):
        self.monthly["root", "2022-11"]["reconciliation"][1]["lower_inclusive"] += 1
        self.refuse()

    def test_float_and_bool_counts_are_not_integers(self):
        self.monthly["root", "2022-11"]["reconciliation"][0]["unchanged_other9_fields"]["left_only_rows"] = False
        self.refuse()

    def test_float_role_count_is_refused(self):
        leaf = self.monthly["root", "2022-11"]["reconciliation"][0]
        leaf["counts"]["maker_rows"] = float(leaf["counts"]["maker_rows"])
        self.refuse()

    def test_changed_payload_and_role_support_are_refused(self):
        self.monthly["clean", "2026-06"]["reconciliation"][0]["counts"]["logical_payload_bytes"] += 1
        self.refuse()

    def test_output_path_traversal_and_schema_change_are_refused(self):
        self.manifest["outputs"][0]["path"] = "../root/year_month=2022-11"
        self.refuse()

    def test_schema_change_in_month_and_top_cannot_change_original(self):
        self.monthly["root", "2022-11"]["output"]["schema"] = "wrong schema"
        self.manifest["outputs"][0]["output"]["schema"] = "wrong schema"
        self.refuse()

    def test_source_hash_and_caps_are_frozen(self):
        self.manifest["source"]["sha256"]["production_guard.py"] = "0" * 64
        self.refuse()

    def test_fixed_census_or_qa_bytes_are_required(self):
        self.census_path.write_bytes(self.census_path.read_bytes() + b" ")
        with self.assertRaisesRegex(audit.ReviewBlocked, "digest"):
            self.build()

    def test_monthly_manifest_hash_is_required(self):
        path = self.run / "root/year_month=2022-11/manifest.json"
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(audit.ReviewBlocked, "monthly identity"):
            self.build()

    def test_charged_read_membership_and_total_are_reconciled(self):
        self.manifest["read_charges"].pop(3)
        self.refuse()

    def test_recorded_source_footprint_cannot_be_changed(self):
        self.manifest["read_charges"][2]["bytes"] += 1
        self.refuse()

    def test_nonzero_or_duplicate_timed_exit_is_refused(self):
        self.profile += "\tExit status: 0\n"
        self.refuse()

    def test_timed_command_and_stdout_are_bound_to_same_run(self):
        self.profile = self.profile.replace(str(self.run), str(self.base / "wrong"))
        self.refuse()

    def test_stdout_mismatch_is_refused(self):
        self.receipt["rows"]["clean"] -= 2
        self.refuse()

    def test_certification_and_adoption_cannot_be_promoted(self):
        self.manifest["downstream_adoption"] = "complete"
        self.refuse()

    def test_duplicate_json_keys_and_nonfinite_numbers_are_refused(self):
        path = self.base / "bad.json"
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}'):
            path.write_bytes(raw)
            with self.assertRaises(audit.ReviewBlocked):
                audit.read_saved(path, 1024)

    def test_symlinked_monthly_metadata_is_refused(self):
        path = self.run / "root/year_month=2022-11/manifest.json"
        target = self.base / "external.json"
        target.write_bytes(path.read_bytes()); path.unlink(); path.symlink_to(target)
        with self.assertRaisesRegex(audit.ReviewBlocked, "non-symlink"):
            self.build()

    def test_existing_receipt_is_preserved(self):
        self.build()
        before = (self.destination / "receipt.json").read_bytes()
        with self.assertRaisesRegex(audit.ReviewBlocked, "already exists"):
            self.build()
        self.assertEqual((self.destination / "receipt.json").read_bytes(), before)

    def test_no_parquet_or_builder_dependency_is_imported(self):
        source = Path(audit.__file__).read_text()
        self.assertNotIn("import duckdb", source)
        self.assertNotIn("import pyarrow", source)
        self.assertNotIn("from scripts import repair", source)

    def test_extra_certification_claim_is_refused(self):
        self.manifest["native_certified"] = True
        self.refuse()

    def test_extra_monthly_adoption_claim_is_refused(self):
        self.monthly["root", "2022-11"]["old_flags_adopted"] = True
        self.refuse()

    def test_contradictory_timed_failure_evidence_is_refused(self):
        self.profile += "Command terminated by signal 9\n"
        self.refuse()

    def test_no_current_data_or_output_file_is_required(self):
        self.assertFalse(list(self.run.rglob("*.parquet")))
        self.build()

    def test_nonempty_output_cannot_claim_zero_parquet_footprint(self):
        self.manifest["read_charges"][3]["bytes"] = 0
        self.manifest["read_charges"][5]["bytes"] = 0
        self.manifest["resource_profile"]["read_bytes_charged"] -= 10_000
        self.monthly["root", "2022-11"]["timestamp_pruning"]["output_overlap_bytes"] -= 5_000
        self.refuse()

    def test_untested_producer_runtime_is_refused(self):
        self.manifest["environment"]["duckdb"] = "0.0.0"
        self.refuse()


if __name__ == "__main__":
    unittest.main()
