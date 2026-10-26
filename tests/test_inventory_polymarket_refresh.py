"""Tiny synthetic readiness fixtures; never open project trading data."""
from __future__ import annotations

from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import inventory_polymarket_refresh as inventory


class RefreshInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        self.private = self.root / "rpc-private"

    def populate(self, *, statistics=True):
        for path in self.paths.values():
            pq.write_table(pa.table({"block_number": [30, 10, 20], "timestamp": [300, 100, 200],
                                     "wallet": ["private-wallet-body"] * 3}),
                           path, row_group_size=2, write_statistics=statistics)

    def run_inventory(self, **kwargs):
        return inventory.build_inventory(self.paths, disk_path=self.root, **kwargs)

    def test_footer_only_counts_and_declared_extrema(self):
        self.populate()
        with patch.object(pq, "read_table", side_effect=AssertionError("no bodies")), \
                patch.object(pq, "ParquetFile", side_effect=AssertionError("no file body reader")):
            result = self.run_inventory()
        self.assertEqual(result["status"], "readiness_inventory_complete")
        self.assertEqual(result["parquet_body_reads"], 0)
        self.assertTrue(result["final_input_identity_unchanged"])
        self.assertEqual(result["consumed"]["files"], 7)
        raw = result["inputs"]["raw_events"]
        self.assertEqual(raw["rows_declared"], 3)
        extrema = raw["files"][0]["extrema"]["timestamp"]
        self.assertEqual(extrema["declared_global_minimum"], 100)
        self.assertEqual(extrema["declared_global_maximum"], 300)
        self.assertIsNone(extrema["independently_observed_exact_maximum"])
        self.assertNotIn("private-wallet-body", json.dumps(result))

    def test_missing_statistics_remain_unknown(self):
        self.populate(statistics=False)
        result = self.run_inventory()
        clock = result["inputs"]["raw_events"]["files"][0]["extrema"]["timestamp"]
        self.assertEqual(clock["groups_missing_minmax"], 2)
        self.assertFalse(clock["all_nonempty_nonnull_groups_have_minmax"])
        self.assertIsNone(clock["declared_global_minimum"])
        self.assertIsNone(clock["declared_global_maximum"])
        self.assertIsNone(clock["null_rows_declared"])

    def test_all_null_clock_is_not_zero(self):
        self.populate()
        pq.write_table(pa.table({"timestamp": pa.array([None, None], type=pa.int64())}), self.paths["raw_events"])
        result = self.run_inventory()
        clock = result["inputs"]["raw_events"]["files"][0]["extrema"]["timestamp"]
        self.assertEqual(clock["all_null_groups"], 1)
        self.assertEqual(clock["null_rows_declared"], 2)
        self.assertIsNone(clock["declared_global_maximum"])

    def test_missing_source_is_recorded_and_others_continue(self):
        self.populate()
        self.paths["deduped_events"].unlink()
        result = self.run_inventory()
        self.assertEqual(result["status"], "readiness_inventory_incomplete")
        self.assertEqual(result["inputs"]["deduped_events"]["status"], "missing")
        self.assertEqual(result["consumed"]["files"], 6)

    def test_requested_symlink_is_recorded_not_silently_rewritten(self):
        self.populate()
        source = self.paths["raw_events"]
        linked = self.root / "raw-link.parquet"
        linked.symlink_to(source)
        self.paths["raw_events"] = linked
        result = self.run_inventory()
        identity = result["inputs"]["raw_events"]["topology"]["input"]
        self.assertTrue(identity["is_symlink"])
        self.assertEqual(identity["resolved_path"], str(source.resolve()))

    def test_directory_files_are_sorted_and_nested_symlinks_block(self):
        self.populate()
        folder = self.root / "raw-dir"
        folder.mkdir()
        for name in ("z.parquet", "a.parquet"):
            pq.write_table(pa.table({"block_number": [1]}), folder / name)
        files, _ = inventory.collect_parquet_files(folder)
        self.assertEqual([path.name for path in files], ["a.parquet", "z.parquet"])
        (folder / "link").symlink_to(self.paths["raw_events"])
        with self.assertRaisesRegex(inventory.ReadinessError, "nested_symlink"):
            inventory.collect_parquet_files(folder)

    def test_footer_limits_block_without_body_scan(self):
        self.populate()
        for limit, value, code in (("maximum_single_footer_bytes", 1, "single_footer_byte_limit"),
                                    ("maximum_total_footer_bytes", 1, "total_footer_byte_limit"),
                                    ("maximum_row_groups", 0, "row_group_limit"),
                                    ("maximum_files", 0, "file_limit")):
            with self.subTest(limit=limit), patch.dict(inventory.LIMITS, {limit: value}):
                with self.assertRaisesRegex(inventory.ReadinessError, code):
                    inventory.inspect_footer(self.paths["raw_events"], inventory.Budget())

    def test_directory_limits(self):
        folder = self.root / "folder"
        folder.mkdir()
        pq.write_table(pa.table({"timestamp": [1]}), folder / "a.parquet")
        with patch.dict(inventory.LIMITS, {"maximum_directory_entries": 0}):
            with self.assertRaisesRegex(inventory.ReadinessError, "directory_entry_limit"):
                inventory.collect_parquet_files(folder)

    def test_rejected_footer_consumes_budget_before_next_payload(self):
        self.populate()
        path = self.paths["raw_events"]
        with path.open("rb") as stream:
            stream.seek(-8, os.SEEK_END)
            length = int.from_bytes(stream.read(4), "little")
        budget = inventory.Budget()
        with patch.dict(inventory.LIMITS, {"maximum_total_footer_bytes": length}), \
                patch.object(pq, "read_metadata", side_effect=ValueError("malformed")) as decoder:
            with self.assertRaisesRegex(ValueError, "malformed"):
                inventory.inspect_footer(path, budget)
            with self.assertRaisesRegex(inventory.ReadinessError, "total_footer_byte_limit"):
                inventory.inspect_footer(path, budget)
        self.assertEqual(decoder.call_count, 1)
        self.assertEqual(budget.files, 2)
        self.assertEqual(budget.footer_bytes, length)
        self.assertEqual(budget.metadata_read_bytes, length + 24)

    def test_input_drift_during_inventory_is_not_accepted(self):
        self.populate()
        original = inventory.inspect_source
        changed = False

        def mutate(path, budget):
            nonlocal changed
            result = original(path, budget)
            if not changed:
                changed = True
                value = self.paths["raw_events"].stat()
                os.utime(self.paths["raw_events"], ns=(value.st_atime_ns, value.st_mtime_ns + 1_000_000))
            return result

        with patch.object(inventory, "inspect_source", side_effect=mutate):
            result = self.run_inventory()
        self.assertFalse(result["final_input_identity_unchanged"])
        self.assertEqual(result["status"], "readiness_inventory_incomplete")

    def test_manifest_only_copies_whitelisted_labels(self):
        self.populate()
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps({"schema_version": "repair_v1", "status": "repair_complete",
                                        "credential": "secret-must-not-appear"}))
        result = self.run_inventory(repair_manifest=manifest)
        self.assertEqual(result["repair_manifest"]["declared_status"], "repair_complete")
        self.assertNotIn("secret-must-not-appear", json.dumps(result))
        manifest.write_text('{"status":"a","status":"b"}')
        self.assertEqual(self.run_inventory(repair_manifest=manifest)["repair_manifest"]["error_code"],
                         "duplicate_json_key")

    def test_unknown_source_keys_fail_before_access(self):
        with self.assertRaisesRegex(inventory.ReadinessError, "source_key_inventory_differs"):
            inventory.build_inventory({"wrong": self.root / "absent"})

    def test_exclusive_writer_never_overwrites(self):
        output = self.root / "new.json"
        inventory.write_exclusive(output, {"a": 1})
        with self.assertRaises(FileExistsError):
            inventory.write_exclusive(output, {"a": 2})
        self.assertEqual(json.loads(output.read_text()), {"a": 1})

    def test_production_cli_blocks_local_before_source_or_output(self):
        output = self.root / "forbidden.json"
        errors = io.StringIO()
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture refusal")), \
                patch.object(inventory, "build_inventory", side_effect=AssertionError("must not inventory")) as build, \
                patch.object(inventory, "path_identity", side_effect=AssertionError("must not open sources")) as identity, \
                redirect_stderr(errors):
            with self.assertRaises(SystemExit) as stopped:
                inventory.main(["--output", str(output)])
        self.assertEqual(stopped.exception.code, 2)
        self.assertIn("canonical mounted EC2", errors.getvalue())
        build.assert_not_called()
        identity.assert_not_called()
        self.assertFalse(output.exists())

    def rpc_transport(self, changes=None):
        changes = changes or {}
        calls = []

        def transport(endpoint, payload):
            calls.append(payload)
            result = {1: "0x89", 2: "0x100", 3: {"number": "0x100", "hash": "0x" + "a" * 64,
                                                       "timestamp": "0x65000000"}}[payload["id"]]
            result = changes.get(payload["id"], result)
            return {"jsonrpc": "2.0", "id": payload["id"], "result": result}
        return transport, calls

    def test_rpc_exact_header_uses_three_calls_no_secret_output(self):
        transport, calls = self.rpc_transport()
        secret = "https://rpc.example.invalid/SECRET-ACCESS-KEY"
        with patch.dict(os.environ, {"POLYGON_RPC_URL": secret}):
            result = inventory.rpc_readiness(self.private, transport)
        self.assertEqual(result["status"], "passed")
        self.assertEqual(result["calls_attempted"], 3)
        self.assertEqual(calls[2]["params"], ["0x100", False])
        self.assertEqual(result["chain_id"], 137)
        self.assertNotIn("SECRET", json.dumps(result))
        self.assertFalse(result["trade_logs_requested"])

    def test_rpc_rejects_wrong_chain_number_hash_timestamp_and_quantity(self):
        cases = [({1: "0x1"}, "rpc_wrong_chain", 1),
                 ({2: "0x0100"}, "rpc_invalid_hex_quantity", 2),
                 ({3: {"number": "0x101", "hash": "0x" + "a" * 64, "timestamp": "0x1"}},
                  "rpc_header_number_mismatch", 3),
                 ({3: {"number": "0x100", "hash": "bad", "timestamp": "0x1"}},
                  "rpc_invalid_block_hash", 3),
                 ({3: {"number": "0x100", "hash": "0x" + "a" * 64, "timestamp": "0x0"}},
                  "rpc_invalid_header_timestamp", 3)]
        for changes, code, count in cases:
            with self.subTest(code=code), patch.dict(os.environ, {"POLYGON_RPC_URL": "https://rpc.invalid/key"}):
                transport, calls = self.rpc_transport(changes)
                result = inventory.rpc_readiness(self.private, transport)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_code"], code)
                self.assertEqual(len(calls), count)

    def test_rpc_reply_id_shape_and_transport_errors_are_sanitized(self):
        for reply in ({"jsonrpc": "2.0", "id": True, "result": "0x89"},
                      {"jsonrpc": "2.0", "id": 2, "result": "0x89"},
                      {"jsonrpc": "2.0", "id": 1, "result": "0x89", "error": "SECRET"}):
            with self.subTest(reply=reply), patch.dict(os.environ, {"POLYGON_RPC_URL": "https://rpc.invalid/SECRET"}):
                result = inventory.rpc_readiness(self.private, lambda *_: reply)
                self.assertEqual(result["status"], "failed")
                self.assertNotIn("SECRET", json.dumps(result))
        with patch.dict(os.environ, {"POLYGON_RPC_URL": "https://rpc.invalid/SECRET"}):
            def fail(*_):
                raise ValueError("https://rpc.invalid/SECRET")
            result = inventory.rpc_readiness(self.private, fail)
        self.assertEqual(result["error_code"], "rpc_transport_decode_or_configuration_failure")
        self.assertNotIn("SECRET", json.dumps(result))

    def test_private_rpc_file_permission_and_missing_config(self):
        with patch.dict(os.environ, {"POLYGON_RPC_URL": ""}):
            result = inventory.rpc_readiness(self.private)
            self.assertEqual(result["error_code"], "rpc_configuration_unavailable")
            self.private.write_text("https://rpc.invalid/SECRET")
            self.private.chmod(0o644)
            self.assertEqual(inventory.rpc_readiness(self.private)["error_code"], "rpc_private_file_permissions")
            self.private.chmod(0o600)
            transport, _ = self.rpc_transport()
            result = inventory.rpc_readiness(self.private, transport)
            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["configuration_source"], "private_file")
            self.assertNotIn("SECRET", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
