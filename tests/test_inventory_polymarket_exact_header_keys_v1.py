"""Small literal-source fixtures; no production data, RPC or source discovery."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import inventory_polymarket_exact_header_keys_v1 as inventory

HEAD = "a" * 40
SOURCE = {"head": HEAD, "files": {"fixture": {"sha256": "b" * 64}}, "tracked_worktree_clean": True}
FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def block_hash(number):
    return "0x" + f"{number:064x}"


def entry(path, role):
    raw = path.read_bytes()
    length = int.from_bytes(raw[-8:-4], "little")
    actual = inventory.file_identity(path)
    result = {"path": str(path), "source_role": role, "bytes": actual.pop("bytes"), "identity": actual,
              "footer_sha256": hashlib.sha256(raw[-8-length:]).hexdigest()}
    if role == "native_raw":
        result["payload_sha256"] = hashlib.sha256(raw).hexdigest()
    return result


class HeaderKeyInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.legacy = self.root / "legacy.parquet"
        self.native = self.root / "native.parquet"
        self.roster = self.root / "roster.json"
        self.output = self.root / "run"
        self.source_patch = patch.object(inventory, "source_snapshot", return_value=copy.deepcopy(SOURCE))
        self.snapshot = self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.disk_patch = patch.object(inventory.shutil, "disk_usage", return_value=FREE)
        self.disk = self.disk_patch.start()
        self.addCleanup(self.disk_patch.stop)
        self.publish_legacy([10, 10, 20, 50])
        self.publish_native([20, 30, 30], [block_hash(20), block_hash(30), block_hash(30)])
        self.publish_roster()

    def publish_legacy(self, blocks, *, kind=pa.int64(), extra=None):
        columns = {"block_number": pa.array(blocks, type=kind),
                   "unused_private_field": extra or ["private source value must never be projected"] * len(blocks)}
        pq.write_table(pa.table(columns), self.legacy, row_group_size=2, compression="zstd")

    def publish_native(self, blocks, hashes, *, kind=pa.uint64(), hash_kind=pa.string(), extra=None):
        pq.write_table(pa.table({"block_number": pa.array(blocks, type=kind),
                                 "blockHash": pa.array(hashes, type=hash_kind),
                                 "unused_private_field": extra or ["never project private contents"] * len(blocks)}),
                       self.native, row_group_size=2, compression="zstd")

    def publish_roster(self, files=None):
        self.files = files if files is not None else [entry(self.legacy, "legacy_raw"), entry(self.native, "native_raw")]
        self.roster.write_bytes(inventory.json_bytes({"schema_version": "polymarket_exact_header_key_roster_v1", "files": self.files}))
        self.roster_sha = hashlib.sha256(self.roster.read_bytes()).hexdigest()

    def execute(self, **kwargs):
        return inventory.run_inventory(self.roster, self.roster_sha, HEAD, self.output, **kwargs)

    def assert_blocked(self, result, code=None):
        self.assertEqual(result["status"], "blocked")
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertTrue((self.output / "FAILED.json").exists())
        if code:
            self.assertEqual(result["error_code"], code)

    def test_union_sorted_nullable_hashes_exact_rows_and_sparse_keys(self):
        result = self.execute()
        self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
        self.assertEqual(result["rows_declared"], 7)
        self.assertEqual(result["rows_scanned"], 7)
        self.assertEqual(result["required_keys"], 4)
        self.assertTrue(result["literal_roster_only"])
        self.assertFalse(result["final_universe_admitted"])
        self.assertFalse(result["historical_v3_admitted"])
        rows = pq.read_table(self.output / "required_keys.parquet").to_pylist()
        self.assertEqual(rows, [{"block_number": 10, "expected_block_hash": None},
                                {"block_number": 20, "expected_block_hash": block_hash(20)},
                                {"block_number": 30, "expected_block_hash": block_hash(30)},
                                {"block_number": 50, "expected_block_hash": None}])
        audit = pq.read_table(self.output / "source_audit.parquet").to_pylist()
        self.assertEqual([row["rows_scanned"] for row in audit], [4, 3])
        self.assertEqual([row["payload_sha256_verified"] for row in audit], [False, True])
        self.assertEqual(result["usage"]["native_payload_bytes_rehashed"], self.native.stat().st_size)
        self.assertEqual(result["usage"]["projected_compressed_bytes_read"], result["planned_reads"]["projected_compressed_read_bytes"])
        self.assertEqual(result["usage"]["footer_bytes_read"], result["planned_reads"]["footer_read_bytes"])
        for row in audit:
            self.assertEqual(row["null_block_rows"] + row["invalid_block_rows"], 0)
            self.assertEqual(row["null_hash_rows"] + row["invalid_hash_rows"] + row["conflicting_hash_blocks"], 0)
        complete = json.loads((self.output / "COMPLETE.json").read_text())
        manifest = self.output / complete["manifest"]["path"]
        self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(), complete["manifest"]["sha256"])
        self.assertFalse(complete["final_universe_admitted"])
        self.assertFalse(any(path.suffix in (".tmp", ".db") for path in self.output.iterdir()))
        self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), inventory.CAPS["all_artifact_bytes"])
        for path in self.output.glob("*.json"):
            self.assertNotIn("private source value", path.read_text())

    def test_small_batches_merge_across_files_and_preserve_every_source_count(self):
        with patch.object(inventory, "BATCH_ROWS", 2), patch.object(inventory, "AUDIT_BATCH_ROWS", 1):
            result = self.execute()
        self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
        self.assertEqual(result["rows_scanned"], 7)
        self.assertEqual(result["required_keys"], 4)

    def test_large_multibatch_projection_with_dictionary_and_plain_column_chunks(self):
        legacy_count, native_count = 2 * inventory.BATCH_ROWS + 37, 2 * inventory.BATCH_ROWS + 71
        legacy_blocks = [100 + (legacy_count - 1 - index) // 3 for index in range(legacy_count)]
        native_blocks = [40_000 + (19 * index) % (inventory.BATCH_ROWS + 101) for index in range(native_count)]
        native_hashes = [block_hash(number) for number in native_blocks]
        legacy_noise = [hashlib.sha256(f"unused-legacy-{index}".encode()).hexdigest() for index in range(legacy_count)]
        native_noise = [hashlib.sha256(f"unused-native-{index}".encode()).hexdigest() for index in range(native_count)]
        expected_count = len(set(legacy_blocks) | set(native_blocks))
        for case, legacy_group, native_group, native_dictionary, native_compression in (
                ("dictionary", 2 * inventory.BATCH_ROWS + 17, inventory.BATCH_ROWS + 19, True, "zstd"),
                ("plain", inventory.BATCH_ROWS // 3, 3 * inventory.BATCH_ROWS, False, None)):
            with self.subTest(case=case):
                self.output = self.root / ("large_" + case)
                pq.write_table(pa.table({"block_number": pa.array(legacy_blocks, type=pa.int64()),
                                         "unused_private_field": legacy_noise}), self.legacy,
                               row_group_size=legacy_group, compression="zstd", use_dictionary=False,
                               data_page_size=1024)
                pq.write_table(pa.table({"block_number": pa.array(native_blocks, type=pa.uint64()),
                                         "blockHash": native_hashes, "unused_private_field": native_noise}),
                               self.native, row_group_size=native_group, compression=native_compression,
                               use_dictionary=native_dictionary, data_page_size=1024)
                self.publish_roster()
                calls = []
                original = inventory.ProjectedReader.read
                def tracked(stream, size=-1):
                    first = stream.tell()
                    answer = original(stream, size)
                    calls.append((str(stream.stream.name), first, size))
                    return answer
                with patch.object(inventory.ProjectedReader, "read", tracked):
                    result = self.execute()
                self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
                self.assertEqual(result["rows_scanned"], legacy_count + native_count)
                self.assertEqual(result["required_keys"], expected_count)
                self.assertEqual(result["usage"]["projected_compressed_bytes_read"],
                                 result["planned_reads"]["projected_compressed_read_bytes"])
                self.assertLess(result["usage"]["projected_compressed_bytes_read"],
                                self.legacy.stat().st_size + self.native.stat().st_size)
                audit = pq.read_table(self.output / "source_audit.parquet").to_pylist()
                self.assertEqual([row["rows_scanned"] for row in audit], [legacy_count, native_count])
                self.assertTrue(all(row["rows_scanned"] == row["valid_rows"] for row in audit))
                key_file = pq.ParquetFile(self.output / "required_keys.parquet")
                self.assertGreaterEqual(key_file.metadata.num_row_groups, 2)
                self.assertEqual(key_file.metadata.num_rows, expected_count)
                for source in (self.legacy, self.native):
                    self.assertTrue(any(path == str(source) and size > 0 for path, _, size in calls))

    def test_uppercase_native_hashes_are_normalized_without_changing_raw_inputs(self):
        self.publish_native([20], ["0x" + "AB" * 32])
        self.publish_roster()
        before = self.native.read_bytes()
        result = self.execute()
        self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
        rows = pq.read_table(self.output / "required_keys.parquet").to_pylist()
        self.assertEqual(next(row["expected_block_hash"] for row in rows if row["block_number"] == 20), "0x" + "ab" * 32)
        self.assertEqual(self.native.read_bytes(), before)

    def test_invalid_blocks_and_hashes_preserve_per_file_waterfall_and_block_output(self):
        self.publish_legacy([None, 0, -1, 10])
        self.publish_native([None, 0, 2**63, 20, 30, 40],
                            [block_hash(1), block_hash(2), block_hash(3), None, "invalid", block_hash(40)])
        self.publish_roster()
        result = self.execute()
        self.assert_blocked(result, "invalid_or_conflicting_source_rows")
        self.assertFalse((self.output / "required_keys.parquet").exists())
        audit = pq.read_table(self.output / "source_audit.parquet").to_pylist()
        self.assertEqual(audit[0]["rows_scanned"], 4)
        self.assertEqual((audit[0]["null_block_rows"], audit[0]["invalid_block_rows"], audit[0]["valid_rows"]), (1, 2, 1))
        self.assertEqual((audit[1]["rows_scanned"], audit[1]["null_block_rows"], audit[1]["invalid_block_rows"]), (6, 1, 2))
        self.assertEqual((audit[1]["null_hash_rows"], audit[1]["invalid_hash_rows"], audit[1]["valid_rows"]), (1, 1, 1))

    def test_conflicting_hashes_within_and_across_files_are_reported_for_all_sources(self):
        self.publish_native([20, 20, 30], [block_hash(20), block_hash(99), block_hash(30)])
        other = self.root / "other.parquet"
        pq.write_table(pa.table({"block_number": pa.array([30], type=pa.uint64()), "blockHash": [block_hash(99)]}), other)
        self.publish_roster([entry(self.legacy, "legacy_raw"), entry(self.native, "native_raw"), entry(other, "native_raw")])
        result = self.execute()
        self.assert_blocked(result, "invalid_or_conflicting_source_rows")
        self.assertEqual(result["global_conflicting_hash_blocks"], 2)
        audit = pq.read_table(self.output / "source_audit.parquet").to_pylist()
        self.assertEqual([row["conflicting_hash_blocks"] for row in audit], [0, 2, 1])

    def test_empty_literal_sources_can_inventory_zero_keys_without_universe_claim(self):
        self.publish_legacy([])
        self.publish_native([], [])
        self.publish_roster()
        result = self.execute()
        self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
        self.assertEqual(result["required_keys"], 0)
        self.assertEqual(pq.read_table(self.output / "required_keys.parquet").num_rows, 0)
        self.assertFalse(result["final_universe_admitted"])

    def test_native_sha_mandatory_and_legacy_full_hash_explicitly_forbidden(self):
        for change in (lambda files: files[1].pop("payload_sha256"),
                       lambda files: files[0].update(payload_sha256="a" * 64),
                       lambda files: files[1].update(payload_sha256=None)):
            files = copy.deepcopy(self.files)
            change(files)
            self.publish_roster(files)
            with self.assertRaises(inventory.InventoryBlocked):
                self.execute()
            self.assertFalse(self.output.exists())
            self.publish_roster()

    def test_roster_schema_duplicates_ids_and_literal_paths_fail_before_body(self):
        cases = [(lambda files: files.append(copy.deepcopy(files[0])), "duplicate_source_path"),
                 (lambda files: files[0].update(source_role="other"), "source_role"),
                 (lambda files: files[0].update(path="*.parquet"), "source_path_not_literal_absolute"),
                 (lambda files: files[0].update(bytes=True), "source_byte_count_invalid"),
                 (lambda files: files[0]["identity"].update(inode=True), "source_identity_schema"),
                 (lambda files: files[0].update(footer_sha256="bad"), "source_footer_sha256")]
        for change, code in cases:
            with self.subTest(code=code):
                files = copy.deepcopy(self.files)
                change(files)
                self.publish_roster(files)
                with self.assertRaisesRegex(inventory.InventoryBlocked, code), \
                        patch.object(inventory, "scan_sources") as scan:
                    self.execute()
                scan.assert_not_called()
                self.assertFalse(self.output.exists())
                self.publish_roster()

    def test_hardlink_alias_and_symlink_source_are_not_extra_independent_sources(self):
        alias = self.root / "alias.parquet"
        import os
        os.link(self.legacy, alias)
        self.publish_roster([entry(self.legacy, "legacy_raw"), entry(alias, "legacy_raw")])
        with self.assertRaisesRegex(inventory.InventoryBlocked, "duplicate_physical_source"):
            self.execute()
        alias.unlink()
        alias.symlink_to(self.legacy)
        self.publish_roster()
        files = copy.deepcopy(self.files)
        files[0]["path"] = str(alias)
        self.publish_roster(files)
        with self.assertRaisesRegex(inventory.InventoryBlocked, "source_path_not_literal_absolute"):
            self.execute()

    def test_wrong_roster_sha_byte_count_file_count_and_full_hash_sum_fail_before_scan(self):
        self.roster_sha = "0" * 64
        with self.assertRaisesRegex(inventory.InventoryBlocked, "roster_sha256_differs"):
            self.execute()
        self.publish_roster()
        for field, limit, code in (("roster_bytes", 1, "roster_byte_limit"),
                                   ("files", 1, "roster_file_count_limit"),
                                   ("native_payload_rehash_bytes", 1, "planned_payload_rehash_byte_limit")):
            with self.subTest(field=field), patch.dict(inventory.CAPS, {field: limit}), \
                    patch.object(inventory, "read_footer") as footer:
                with self.assertRaisesRegex(inventory.InventoryBlocked, code):
                    self.execute()
                footer.assert_not_called()
                self.assertFalse(self.output.exists())

    def test_bad_late_footer_or_schema_prevents_all_body_reads(self):
        files = copy.deepcopy(self.files)
        files[1]["footer_sha256"] = "0" * 64
        self.publish_roster(files)
        with patch.object(inventory, "scan_sources") as scan, patch.object(inventory, "rehash_native") as hashes:
            self.assert_blocked(self.execute(), "source_footer_differs")
        scan.assert_not_called(); hashes.assert_not_called()

    def test_schema_types_and_native_camelcase_hash_are_frozen(self):
        cases = ("legacy_unsigned", "native_signed", "missing_hash", "wrong_hash_type")
        for index, case in enumerate(cases):
            self.output = self.root / ("schema_" + str(index))
            self.publish_legacy([10])
            self.publish_native([20], [block_hash(20)])
            if case == "legacy_unsigned":
                self.publish_legacy([10], kind=pa.uint64())
            elif case == "native_signed":
                self.publish_native([20], [block_hash(20)], kind=pa.int64())
            elif case == "missing_hash":
                pq.write_table(pa.table({"block_number": pa.array([20], type=pa.uint64()), "block_hash": [block_hash(20)]}), self.native)
            else:
                self.publish_native([20], [20], hash_kind=pa.int64())
            self.publish_roster()
            with patch.object(inventory, "scan_sources") as scan:
                self.assert_blocked(self.execute())
            scan.assert_not_called()

    def test_entire_read_plan_is_admitted_before_any_payload_or_projection(self):
        for field, limit in (("footer_read_bytes", 2_000), ("projected_compressed_read_bytes", 1)):
            self.output = self.root / ("cap_" + field)
            with patch.dict(inventory.CAPS, {field: limit}), \
                    patch.object(inventory, "scan_sources") as scan, patch.object(inventory, "rehash_native") as hashes:
                result = self.execute()
                self.assert_blocked(result)
                self.assertEqual(result["usage"]["projected_compressed_bytes_read"], 0)
                self.assertEqual(result["usage"]["native_payload_bytes_rehashed"], 0)
            scan.assert_not_called(); hashes.assert_not_called()

    def test_three_pass_footer_reservation_fails_before_body_even_when_first_pass_fits(self):
        total = sum(int.from_bytes(Path(item["path"]).read_bytes()[-8:-4], "little") + 12 for item in self.files)
        with patch.dict(inventory.CAPS, {"footer_read_bytes": 2 * total}), \
                patch.object(inventory, "scan_sources") as scan:
            result = self.execute()
        self.assert_blocked(result, "planned_footer_read_bytes_limit")
        self.assertEqual(result["usage"]["footer_bytes_read"], total)
        self.assertEqual(result["usage"]["native_payload_bytes_rehashed"], 0)
        scan.assert_not_called()

    def test_native_payload_hash_mismatch_is_never_a_completed_inventory(self):
        files = copy.deepcopy(self.files)
        files[1]["payload_sha256"] = "0" * 64
        self.publish_roster(files)
        self.assert_blocked(self.execute(), "native_payload_sha256_differs")

    def test_same_size_raw_body_mutation_is_caught_after_projected_scan(self):
        original = inventory.aggregate_batch
        def mutate(*args):
            result = original(*args)
            with self.native.open("r+b") as stream:
                stream.seek(10); old = stream.read(1); stream.seek(10); stream.write(bytes([old[0] ^ 1]))
            return result
        with patch.object(inventory, "aggregate_batch", side_effect=mutate):
            self.assert_blocked(self.execute(), "source_identity_differs")

    def test_same_size_roster_or_source_code_evidence_mutation_blocks_final_commit(self):
        original = inventory.scan_sources
        def mutate(*args):
            original(*args)
            raw = self.roster.read_bytes()
            self.roster.write_bytes(raw.replace(b'"native_raw"', b'"legacy_raw"', 1))
        with patch.object(inventory, "scan_sources", side_effect=mutate):
            self.assert_blocked(self.execute(), "roster_sha256_differs")
        self.output = self.root / "source_drift"
        self.publish_roster()
        self.snapshot.side_effect = [SOURCE, {**SOURCE, "head": "c" * 40}]
        self.assert_blocked(self.execute(), "source_changed_during_run")

    def test_same_size_published_evidence_mutation_is_detected(self):
        original = inventory.read_footer
        calls = [0]
        def mutate(*args):
            result = original(*args)
            calls[0] += 1
            if calls[0] == 5:
                path = self.output / "source_audit.parquet"
                with path.open("r+b") as stream:
                    stream.seek(10); old = stream.read(1); stream.seek(10); stream.write(bytes([old[0] ^ 1]))
            return result
        with patch.object(inventory, "read_footer", side_effect=mutate):
            self.assert_blocked(self.execute(), "published_output_changed")

    def test_existing_or_partial_destination_is_not_resumed_or_overwritten(self):
        self.output.mkdir(); keep = self.output / "keep"; keep.write_bytes(b"preserve")
        with self.assertRaisesRegex(inventory.InventoryBlocked, "output_not_new_absolute_directory"):
            self.execute()
        self.assertEqual(keep.read_bytes(), b"preserve")

    def test_output_and_runtime_caps_leave_no_complete_marker(self):
        with patch.dict(inventory.CAPS, {"all_artifact_bytes": inventory.CONTROL_RESERVE + 10}):
            self.assert_blocked(self.execute(), "artifact_byte_limit")
        self.output = self.root / "timeout"
        now = [0]
        original = inventory.scan_sources
        def expire(*args):
            original(*args)
            now[0] = 2_401
        with patch.object(inventory, "scan_sources", side_effect=expire):
            self.assert_blocked(self.execute(clock=lambda: now[0]), "runtime_limit")

    def test_checked_output_counts_open_staging_and_all_controls(self):
        self.output.mkdir()
        budget = inventory.Budget(self.output)
        with patch.dict(inventory.CAPS, {"all_artifact_bytes": inventory.CONTROL_RESERVE + 100}):
            with inventory.CheckedOutput(budget, ".large.pending") as stream:
                stream.write(b"x" * 75)
                self.assertEqual(budget.retained(), 75)
                with self.assertRaisesRegex(inventory.InventoryBlocked, "artifact_byte_limit"):
                    stream.write(b"y" * 26)
                self.assertEqual(budget.retained(), 75)

    def test_projection_wrapper_refuses_any_other_column_or_unbounded_read(self):
        self.output.mkdir()
        budget = inventory.Budget(self.output)
        _, plan = inventory.read_footer(self.files[1], budget)
        with inventory.ProjectedReader(self.files[1], plan["ranges"], budget) as stream:
            with self.assertRaisesRegex(inventory.InventoryBlocked, "unbounded_projection_read"):
                stream.read()
            stream.seek(0)
            with self.assertRaisesRegex(inventory.InventoryBlocked, "unadmitted_projection_read"):
                stream.read(4)
        self.assertEqual(budget.projected_bytes, 0)

    def test_projection_actual_read_ceiling_is_checked_before_the_file_read(self):
        self.output.mkdir()
        budget = inventory.Budget(self.output)
        _, plan = inventory.read_footer(self.files[1], budget)
        first, last = plan["ranges"][0]
        with inventory.ProjectedReader(self.files[1], plan["ranges"], budget) as stream, \
                patch.dict(inventory.CAPS, {"projected_compressed_read_bytes": 1}):
            stream.seek(first)
            with self.assertRaisesRegex(inventory.InventoryBlocked, "projection_read_byte_limit"):
                stream.read(min(2, last-first))
            self.assertEqual(stream.tell(), first)
        self.assertEqual(budget.projected_bytes, 0)

    def test_database_caps_no_spill_and_lazy_batch_registration(self):
        con = inventory.initialize_database()
        try:
            self.assertEqual(con.execute("SELECT current_setting('threads')").fetchone()[0], 4)
            self.assertEqual(con.execute("SELECT current_setting('temp_directory')").fetchone()[0], "")
            self.assertIn("0 bytes", str(con.execute("SELECT current_setting('max_temp_directory_size')").fetchone()[0]))
        finally:
            con.close()

    def test_atomic_complete_never_overwrites_existing_marker(self):
        temporary, target = self.root / "pending", self.root / "complete"
        temporary.write_bytes(b"new"); target.write_bytes(b"old")
        with self.assertRaisesRegex(inventory.InventoryBlocked, "atomic_publication_refused"):
            inventory.atomic_publish(temporary, target)
        self.assertEqual(target.read_bytes(), b"old")
        self.assertEqual(temporary.read_bytes(), b"new")

    def test_failed_final_publication_preserves_pending_evidence_without_complete(self):
        original = inventory.atomic_publish
        def refuse_last(temporary, target):
            if target.name == "COMPLETE.json":
                raise inventory.InventoryBlocked("atomic_publication_refused")
            return original(temporary, target)
        with patch.object(inventory, "atomic_publish", side_effect=refuse_last):
            self.assert_blocked(self.execute(), "atomic_publication_refused")
        self.assertTrue((self.output / ".COMPLETE.json.pending").is_file())
        self.assertTrue((self.output / "manifest.json").is_file())

    def test_complete_file_directory_and_rename_faults_leave_charged_pending_only(self):
        original_publish = inventory.publish_json
        original_fsync = inventory.os.fsync
        original_atomic = inventory.atomic_publish
        for failure in ("file_fsync", "directory_fsync", "rename"):
            self.output = self.root / failure
            def inject(budget, name, value, **kwargs):
                if name != "COMPLETE.json":
                    return original_publish(budget, name, value, **kwargs)
                def fsync(descriptor):
                    info = os.fstat(descriptor)
                    pending = budget.output / ".COMPLETE.json.pending"
                    is_marker = pending.exists() and (info.st_dev, info.st_ino) == (
                        pending.stat().st_dev, pending.stat().st_ino)
                    if (failure == "file_fsync" and is_marker) or (
                            failure == "directory_fsync" and stat.S_ISDIR(info.st_mode)):
                        raise OSError("injected completion flush failure")
                    return original_fsync(descriptor)
                def rename(temporary, target):
                    if failure == "rename":
                        raise inventory.InventoryBlocked("atomic_publication_refused")
                    return original_atomic(temporary, target)
                with patch.object(inventory.os, "fsync", side_effect=fsync), \
                        patch.object(inventory, "atomic_publish", side_effect=rename):
                    return original_publish(budget, name, value, **kwargs)
            with self.subTest(failure=failure), patch.object(inventory, "publish_json", side_effect=inject):
                result = self.execute()
            self.assert_blocked(result)
            pending = self.output / ".COMPLETE.json.pending"
            self.assertTrue(pending.is_file())
            self.assertGreater(pending.stat().st_size, 0)
            self.assertEqual(result["usage"]["retained_bytes_before_terminal"],
                             sum(path.stat().st_size for path in self.output.iterdir() if path.name != "FAILED.json"))
            self.assertLessEqual(sum(path.stat().st_size for path in self.output.iterdir()), inventory.CAPS["all_artifact_bytes"])
            self.assertTrue((self.output / "required_keys.parquet").is_file())
            self.assertTrue((self.output / "manifest.json").is_file())

    def test_successful_complete_rename_follows_directory_flush_and_is_last_artifact_io(self):
        events = []
        committed = [False]
        original_publish = inventory.publish_json
        original_fsync = inventory.os.fsync
        original_atomic = inventory.atomic_publish
        original_hash = inventory.hash_output
        def marker(budget, name, value, **kwargs):
            if name != "COMPLETE.json":
                return original_publish(budget, name, value, **kwargs)
            def fsync(descriptor):
                self.assertFalse(committed[0])
                events.append("directory_fsync" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file_fsync")
                return original_fsync(descriptor)
            def rename(temporary, target):
                self.assertEqual(target.name, "COMPLETE.json")
                events.append("atomic_commit")
                answer = original_atomic(temporary, target)
                committed[0] = True
                return answer
            with patch.object(inventory.os, "fsync", side_effect=fsync), \
                    patch.object(inventory, "atomic_publish", side_effect=rename):
                return original_publish(budget, name, value, **kwargs)
        def hash_before_commit(*args, **kwargs):
            self.assertFalse(committed[0])
            return original_hash(*args, **kwargs)
        with patch.object(inventory, "publish_json", side_effect=marker), \
                patch.object(inventory, "hash_output", side_effect=hash_before_commit):
            result = self.execute()
        self.assertEqual(result["status"], "literal_header_key_inventory_complete", result)
        self.assertTrue(committed[0])
        self.assertEqual(events, ["file_fsync", "directory_fsync", "atomic_commit"])

    def test_unknown_output_artifact_and_symlink_are_not_excluded_from_quota_checks(self):
        self.output.mkdir()
        budget = inventory.Budget(self.output)
        extra = self.output / "unregistered"
        extra.write_bytes(b"not owned")
        with self.assertRaisesRegex(inventory.InventoryBlocked, "unexpected_output_artifact"):
            budget.retained()
        extra.unlink(); extra.symlink_to(self.roster); budget.paths.add(extra.name)
        with self.assertRaisesRegex(inventory.InventoryBlocked, "unexpected_output_artifact"):
            budget.retained()

    def test_disk_floor_refuses_initial_output_and_bounded_writes(self):
        self.disk.return_value = shutil._ntuple_diskusage(10**12, 0, inventory.CAPS["minimum_free_bytes"])
        with self.assertRaisesRegex(inventory.InventoryBlocked, "free_space_floor"):
            self.execute()
        self.assertFalse(self.output.exists())
        self.output.mkdir()
        budget = inventory.Budget(self.output)
        with inventory.CheckedOutput(budget, ".pending") as stream:
            with self.assertRaisesRegex(inventory.InventoryBlocked, "free_space_floor"):
                stream.write(b"x")
        self.assertEqual((self.output / ".pending").stat().st_size, 0)

    def test_real_source_snapshot_binds_exact_committed_blobs(self):
        self.source_patch.stop()
        root = self.root / "source"; root.mkdir()
        for name in inventory.SOURCE_FILES:
            path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b"fixture\n")
        subprocess.run(["git", "init", "--quiet", str(root)], check=True)
        subprocess.run(["git", "-C", str(root), "add", *inventory.SOURCE_FILES], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "--quiet", "-m", "Fixture"], check=True)
        head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        real_code_state = inventory.shared.code_state
        with patch.object(inventory, "REPO", root), \
                patch.object(inventory.shared, "code_state", side_effect=lambda: real_code_state(root)):
            self.assertEqual(inventory.source_snapshot(head)["head"], head)
            subprocess.run(["git", "-C", str(root), "update-index", "--assume-unchanged", "production_guard.py"], check=True)
            (root / "production_guard.py").write_bytes(b"changed\n")
            with self.assertRaisesRegex(inventory.InventoryBlocked, "source_dependency_differs_from_commit"):
                inventory.source_snapshot(head)

    def test_process_address_space_and_deadline_limits_are_set_and_restored(self):
        with patch.object(inventory.resource, "getrlimit", return_value=(-1, -1)), \
                patch.object(inventory.resource, "setrlimit") as limits, \
                patch.object(inventory.signal, "getsignal", return_value=inventory.signal.SIG_DFL), \
                patch.object(inventory.signal, "signal") as handlers, patch.object(inventory.signal, "alarm") as alarm:
            with inventory.process_limits():
                pass
        self.assertEqual(limits.call_args_list[0].args[1], (48_000_000_000, -1))
        self.assertEqual(limits.call_args_list[-1].args[1], (-1, -1))
        self.assertEqual(alarm.call_args_list[0].args, (2400,))
        self.assertEqual(alarm.call_args_list[-1].args, (0,))
        self.assertEqual(handlers.call_args_list[-1].args[1], inventory.signal.SIG_DFL)

    def test_source_and_resource_admission_before_scan_and_cli_guard_before_reads(self):
        self.snapshot.side_effect = inventory.InventoryBlocked("source_head_or_clean_state_differs")
        with self.assertRaisesRegex(inventory.InventoryBlocked, "source_head_or_clean_state_differs"):
            self.execute()
        self.assertFalse(self.output.exists())
        with patch.object(inventory, "require_production_host", side_effect=RuntimeError("guard")), \
                patch.object(inventory, "read_roster") as reads, patch.object(inventory, "process_limits") as limits:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                inventory.main(["--roster", str(self.roster), "--roster-sha256", self.roster_sha,
                                "--expected-head", HEAD, "--output-dir", str(self.output)])
            reads.assert_not_called(); limits.assert_not_called()


if __name__ == "__main__":
    unittest.main()
