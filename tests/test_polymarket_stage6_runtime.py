"""Fixture-only runtime tests; default optimizer behavior is observational."""
from __future__ import annotations

import ast
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import audit_polymarket_stage6_runtime as runtime

# Exact run_stage6 COPY source fragment, independently bound to its frozen hash.
FROZEN_SQL_TEMPLATE = "f\"\"\"\n        COPY (\n            WITH expanded AS (\n                -- MAKER row\n                SELECT\n                    rt.maker AS proxyWallet,\n                    {ts_expr} AS timestamp,\n                    CASE WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id\n                         ELSE rt.taker_asset_id END AS conditionId,\n                    CASE WHEN rt.outcome_token_side = 'maker'\n                         THEN rt.taker_amount_filled / {usdc_scale}.0\n                         ELSE rt.maker_amount_filled / {usdc_scale}.0 END AS usdcSize,\n                    CASE WHEN rt.outcome_token_side = 'maker'\n                         THEN (rt.taker_amount_filled / {usdc_scale}.0)\n                              / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)\n                         ELSE (rt.maker_amount_filled / {usdc_scale}.0)\n                              / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0) END AS price,\n                    CASE WHEN rt.outcome_token_side = 'maker' THEN 'SELL' ELSE 'BUY' END AS side,\n                    rt.outcome,\n                    rt.event_slug AS eventSlug,\n                    TRUE AS is_maker,\n                    rt.taker AS counterparty\n                FROM read_parquet('{RESOLVED_TRADES_PATH}') rt\n                {ts_join}\n\n                UNION ALL\n\n                -- TAKER row\n                SELECT\n                    rt.taker AS proxyWallet,\n                    {ts_expr} AS timestamp,\n                    CASE WHEN rt.outcome_token_side = 'maker' THEN rt.maker_asset_id\n                         ELSE rt.taker_asset_id END AS conditionId,\n                    CASE WHEN rt.outcome_token_side = 'maker'\n                         THEN rt.taker_amount_filled / {usdc_scale}.0\n                         ELSE rt.maker_amount_filled / {usdc_scale}.0 END AS usdcSize,\n                    CASE WHEN rt.outcome_token_side = 'maker'\n                         THEN (rt.taker_amount_filled / {usdc_scale}.0)\n                              / NULLIF(rt.maker_amount_filled / {ctf_scale}.0, 0)\n                         ELSE (rt.maker_amount_filled / {usdc_scale}.0)\n                              / NULLIF(rt.taker_amount_filled / {ctf_scale}.0, 0) END AS price,\n                    CASE WHEN rt.outcome_token_side = 'maker' THEN 'BUY' ELSE 'SELL' END AS side,\n                    rt.outcome,\n                    rt.event_slug AS eventSlug,\n                    FALSE AS is_maker,\n                    rt.maker AS counterparty\n                FROM read_parquet('{RESOLVED_TRADES_PATH}') rt\n                {ts_join}\n            )\n            SELECT\n                proxyWallet, timestamp, conditionId, usdcSize, price,\n                side, outcome, eventSlug, is_maker, counterparty,\n                strftime(to_timestamp(timestamp), '%Y-%m') AS year_month\n            FROM expanded\n            WHERE price IS NOT NULL AND price > 0 AND price <= 1\n        )\n        TO '{TRADES_OUTPUT_DIR}' (\n            FORMAT PARQUET,\n            PARTITION_BY (year_month),\n            COMPRESSION ZSTD,\n            OVERWRITE_OR_IGNORE\n        )\n    \"\"\""
TEST_SOURCE = "raise RuntimeError('builder import is forbidden')\ndef run_stage6():\n    con.execute(" + FROZEN_SQL_TEMPLATE + ")\n"
TEMPLATE_SHA256 = "be2b10e0f338ae9b9adec236d64e56ae4f2a4dbcf401bfaa67628bfed8402d0f"


class Stage6RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="stage6_unit_")
        self.root = Path(self.temporary.name)
        self.builder = self.root / "fixture_builder.py"
        self.builder.write_text(TEST_SOURCE)

    def tearDown(self):
        self.temporary.cleanup()

    def fixture_gate(self):
        # The test replaces only the expected identity of this synthetic source.
        # The CLI exposes no override of the actual frozen production blob.
        return mock.patch.object(runtime, "FROZEN_BUILDER_BLOB",
                                 runtime.git_blob(TEST_SOURCE.encode()))

    def test_frozen_production_blob_is_fixed(self):
        self.assertEqual(runtime.FROZEN_BUILDER_BLOB,
                         "19bacf3c87494b782ca1c87213f0e8d72191ecb5")

    def test_static_extraction_preserves_exact_template_without_import(self):
        with self.fixture_gate():
            template, metadata = runtime.extract_frozen_copy(self.builder)
        self.assertEqual(metadata["sql_template_source"], FROZEN_SQL_TEMPLATE)
        self.assertEqual(metadata["sql_template_sha256"], TEMPLATE_SHA256)
        self.assertFalse(metadata["builder_imported_or_executed"])
        self.assertIsInstance(template, ast.JoinedStr)

    def test_unknown_source_identity_refuses_before_fixture_execution(self):
        with mock.patch.object(runtime, "run_case", side_effect=AssertionError("must not run")):
            with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "frozen Git blob"):
                runtime.build_report(self.builder)

    def test_interpolation_refuses_expressions_without_evaluating_them(self):
        bad_source = TEST_SOURCE.replace("{ts_expr}", "{__import__('os').getcwd()}")
        self.builder.write_text(bad_source)
        with mock.patch.object(runtime, "FROZEN_BUILDER_BLOB", runtime.git_blob(bad_source.encode())):
            with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "unapproved expression"):
                runtime.extract_frozen_copy(self.builder)

    def test_full_matrix_preserves_default_observation_and_correct_controls(self):
        with self.fixture_gate():
            report = runtime.build_report(self.builder)
        self.assertFalse(report["production_inputs_read"])
        self.assertFalse(report["historical_writer_or_engine_version_proven"])
        self.assertTrue(report["scratch_removed"])
        self.assertLessEqual(report["actual_scratch_bytes"], runtime.MAX_SCRATCH_BYTES)
        self.assertEqual(len(report["cases"]), 12)
        self.assertEqual(report["control_failure_cases"], [])
        complete = [case for case in report["cases"] if case["status"] == "complete"]
        for case in complete:
            self.assertEqual(len(case["output_rows"]), 4)
            self.assertTrue(case["row_count_matches"])
            self.assertTrue(case["maker_rows_match"])
            self.assertEqual(set(case["output_rows"][0]), set(runtime.VALUE_FIELDS))
            self.assertTrue(case["explain"])
            if case["optimizer"] != "default":
                self.assertTrue(case["exact_rows_match"], case["name"])
            else:
                self.assertTrue(case["default_behavior_is_observational"])
                self.assertEqual(case["exact_rows_match"], case["output_rows"] == case["expected_rows"])
            if not case["exact_rows_match"]:
                self.assertEqual({field for mismatch in case["row_mismatches"]
                                  for field in mismatch["fields"]},
                                 {"proxyWallet", "counterparty"})
        default_failures = [case["name"] for case in complete
                            if case["optimizer"] == "default" and not case["exact_rows_match"]]
        self.assertEqual(report["default_discrepancy_cases"], default_failures)
        unsupported = [case for case in report["cases"] if case["status"] == "optimizer_unavailable"]
        self.assertTrue(all(case["optimizer"] == "common_subplan_disabled" for case in unsupported))
        self.assertEqual({case["threads"] for case in complete}, {1, 4})
        self.assertEqual({case["timestamp_mode"] for case in complete}, {"cached", "approx"})

    def test_expected_rows_distinguish_maker_actions_and_counterparty_wallets(self):
        rows = runtime.expected_rows("cached")
        self.assertEqual([(row["proxyWallet"], row["counterparty"], row["side"], row["is_maker"])
                          for row in rows], [
            ("wallet_A", "wallet_B", "BUY", True),
            ("wallet_B", "wallet_A", "SELL", False),
            ("wallet_C", "wallet_D", "SELL", True),
            ("wallet_D", "wallet_C", "BUY", False)])
        self.assertEqual([row["price"] for row in rows], [0.98, 0.98, 0.4, 0.4])

    def test_immutable_publication_roundtrips_and_refuses_overwrite(self):
        destination = self.root / "published"
        report = {"status": "fixture", "price": 0.98, "data_certified": False}
        runtime.write_immutable(destination, report)
        self.assertEqual(json.loads((destination / "report.json").read_text()), report)
        self.assertFalse((destination / "report.json.partial").exists())
        with self.assertRaises(FileExistsError):
            runtime.write_immutable(destination, report)

    def test_report_cap_refuses_before_publication(self):
        destination = self.root / "never_created"
        with mock.patch.object(runtime, "MAX_REPORT_BYTES", 1):
            with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "no truncation"):
                runtime.write_immutable(destination, {"complete": "evidence"})
        self.assertFalse(destination.exists())

    def test_scratch_cap_is_enforced(self):
        file = self.root / "fixture"
        file.write_bytes(b"12")
        with mock.patch.object(runtime, "MAX_SCRATCH_BYTES", 1):
            with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "scratch exceeds"):
                runtime.scratch_bytes(self.root)

    def test_nonfinite_evidence_is_rejected(self):
        with self.assertRaises(ValueError):
            runtime.exact_json({"price": float("nan")})

    def test_isolated_public_wheel_identity_is_version_bound(self):
        provenance = self.root / "pip_report.json"
        source = {"install": [{"metadata": {"name": "duckdb", "version": "1.5.6"},
                  "download_info": {"url": "https://files.pythonhosted.org/duckdb.whl",
                                    "archive_info": {"hashes": {"sha256": "a" * 64}}}}]}
        provenance.write_text(json.dumps(source))
        result = runtime.package_provenance(provenance, "1.5.6")
        self.assertEqual(result["wheel_sha256"], "a" * 64)
        with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "differs from loaded"):
            runtime.package_provenance(provenance, "1.4.4")
        source["install"][0]["download_info"]["url"] = "https://files.pythonhosted.org/duckdb.whl?token=unapproved"
        provenance.write_text(json.dumps(source))
        with self.assertRaisesRegex(runtime.RuntimeAuditBlocked, "public PyPI"):
            runtime.package_provenance(provenance, "1.5.6")


if __name__ == "__main__":
    unittest.main()
