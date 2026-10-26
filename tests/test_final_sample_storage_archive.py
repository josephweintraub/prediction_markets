"""Tiny fixtures and mocked rclone; never touch the production mount or Dropbox."""

import inspect
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import archive_final_sample_storage as archive
from scripts import archive_wallet_repair_storage as original_archive


class FakeRclone:
    def __init__(self):
        self.files = {}
        self.exists = False
        self.commands = []
        self.remote_hash_override = None
        self.after_copy = None
        self.fail_upload = False

    def __call__(self, command, log=None):
        self.commands.append(command)
        if command[0] == "git":
            return subprocess.CompletedProcess(command, 0, "a" * 40 + "\n", "")
        if command[:2] == ["rclone", "version"]:
            return subprocess.CompletedProcess(command, 0, "rclone fixture\n", "")
        if command[:2] == ["rclone", "lsjson"]:
            location = command[2]
            if "--stat" in command:
                return subprocess.CompletedProcess(command, 0 if self.exists else 3,
                                                   "{}" if self.exists else "",
                                                   "" if self.exists else "directory not found")
            remote = location == archive.REMOTE_ROOT
            if remote:
                files = self.files
            else:
                root = Path(location)
                files = {str(path.relative_to(root)): path.read_bytes()
                         for path in root.rglob("*") if path.is_file()}
            output = [{"Path": name, "Size": len(data), "IsDir": False,
                       "Hashes": {"DropboxHash": (self.remote_hash_override if remote
                                  and self.remote_hash_override is not None else archive.dropbox_hash(data))}}
                      for name, data in sorted(files.items())]
            return subprocess.CompletedProcess(command, 0, json.dumps(output), "")
        if command[:2] == ["rclone", "copy"]:
            self.exists = True
            root = Path(command[2])
            prefix = command[3].removeprefix(archive.REMOTE_ROOT + "/")
            for path in root.rglob("*"):
                if path.is_file():
                    self.files[prefix + "/" + str(path.relative_to(root))] = path.read_bytes()
            if self.after_copy:
                self.after_copy(root)
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[:2] == ["rclone", "copyto"]:
            if self.fail_upload:
                return subprocess.CompletedProcess(command, 1, "", "fixture upload failed")
            name = command[3].removeprefix(archive.REMOTE_ROOT + "/")
            self.files[name] = Path(command[2]).read_bytes()
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(f"Unexpected external command: {command}")


class StorageArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "data"
        self.root.mkdir()
        (self.root / "runs").mkdir()
        self.run = self.root / "runs/fixture"
        self.patches = [patch.object(archive, "DATA_ROOT", self.root),
                        patch.object(archive, "RUN_DIR", self.run),
                        patch.object(archive, "EXPECTED_COUNTS",
                                     {target: (1, 3, 9) for target in archive.TARGETS})]
        for item in self.patches:
            item.start()
        self.fake = FakeRclone()
        self.patches.append(patch.object(archive, "execute", self.fake))
        self.patches[-1].start()
        archive.COMMAND_RECORDS.clear()
        for index, target in enumerate(archive.TARGETS):
            directory = self.root / target / "month=fixture"
            directory.mkdir(parents=True)
            (directory / "part.parquet").write_bytes(f"fixture-{index}".encode())
            (self.root / target / "empty").mkdir()
        self.protected = self.root / "runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean"
        self.protected.mkdir(parents=True)
        (self.protected / "current.parquet").write_bytes(b"protected corrected data")
        self.raw_source = self.root / "pipeline_data/resolved_trades.parquet"
        self.raw_source.parent.mkdir()
        self.raw_source.write_bytes(b"protected native raw data")
        self.metadata_source = self.root / "pipeline_data/token_map.parquet"
        self.metadata_source.write_bytes(b"protected token metadata")
        self.paper_source = self.root / "runs/2026-10-14_kaushik_exchange_filtered_estimates_v1/estimates.json"
        self.paper_source.parent.mkdir()
        self.paper_source.write_bytes(b"protected completed paper results")

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temporary.cleanup()

    def first_file(self):
        return self.root / archive.TARGETS[0] / "month=fixture/part.parquet"

    def assert_all_sources_present(self):
        for target in archive.TARGETS:
            self.assertTrue((self.root / target / "month=fixture/part.parquet").is_file())
        self.assert_protected_sources_present()

    def assert_protected_sources_present(self):
        for path, expected in (
                (self.protected / "current.parquet", b"protected corrected data"),
                (self.raw_source, b"protected native raw data"),
                (self.metadata_source, b"protected token metadata"),
                (self.paper_source, b"protected completed paper results")):
            self.assertEqual(path.read_bytes(), expected)

    def test_archive_preserves_sources_and_verifies_both_control_files(self):
        result = archive.archive()
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["files"], 2)
        self.assert_all_sources_present()
        manifest = archive.load_verified()
        self.assertEqual(manifest["source_head"], "a" * 40)
        self.assertEqual(len(manifest["archive_files"]), 2)
        self.assertTrue(manifest["commands"])
        for target in archive.TARGETS:
            self.assertEqual(manifest["per_target"][target]["file_count"], 1)
        self.assertEqual(len(self.fake.files), 4)
        self.assertEqual((self.run / archive.MANIFEST_NAME).stat().st_mode & 0o777, 0o444)
        for command in self.fake.commands:
            if command[1] == "copy":
                self.assertIn("--immutable", command)
                self.assertIn("--checksum", command)
                self.assertIn("--create-empty-src-dirs", command)

    def test_existing_archive_fails_before_local_run_creation(self):
        self.fake.exists = True
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            archive.archive()
        self.assertFalse(self.run.exists())
        self.assert_all_sources_present()

    def test_remote_failure_is_not_treated_as_nonexistence(self):
        with patch.object(archive, "execute", return_value=subprocess.CompletedProcess([], 1, "", "auth error")):
            with self.assertRaisesRegex(RuntimeError, "Cannot establish"):
                archive.archive()
        self.assertFalse(self.run.exists())

    def test_source_symlink_is_refused_before_copy(self):
        (self.root / archive.TARGETS[0] / "link").symlink_to(self.first_file())
        with self.assertRaisesRegex(RuntimeError, "Link"):
            archive.archive()
        self.assertFalse(self.run.exists())

    def test_hardlinked_file_is_refused(self):
        os.link(self.first_file(), self.root / archive.TARGETS[0] / "other.parquet")
        with self.assertRaisesRegex(RuntimeError, "hardlinked"):
            archive.inventory()

    def test_casefold_collision_is_refused(self):
        # Exercise colliding discovery names even on a case-insensitive Mac.
        root = self.root / archive.TARGETS[0]
        original = os.scandir
        class Children:
            def __enter__(self):
                return [SimpleNamespace(name=name, path=str(self_path))
                        for name in ("Part.parquet", "part.parquet")]
            def __exit__(self, *arguments):
                return False
        self_path = self.first_file()
        def collision_listing(path):
            return Children() if path == root else original(path)
        with patch.object(archive.os, "scandir", collision_listing):
            with self.assertRaisesRegex(RuntimeError, "case-insensitive"):
                archive.inventory()

    def test_changed_admission_counts_fail_before_any_copy(self):
        (self.root / archive.TARGETS[0] / "new.parquet").write_bytes(b"new")
        with self.assertRaisesRegex(RuntimeError, "initial file/directory/byte counts changed"):
            archive.archive()
        self.assertFalse(self.run.exists())
        self.assertFalse(any(command[1] == "copy" for command in self.fake.commands))

    def test_admission_checkpoint_exists_before_first_copy(self):
        def check_checkpoint(root):
            self.assertTrue((self.run / archive.ADMISSION_NAME).is_file())
            self.assertEqual((self.run / archive.ADMISSION_NAME).stat().st_mode & 0o777, 0o444)
        self.fake.after_copy = check_checkpoint
        archive.archive()

    def test_duplicate_json_keys_and_boolean_sizes_are_refused(self):
        with self.assertRaisesRegex(RuntimeError, "Duplicate JSON key"):
            archive.strict_json('{"status":"VERIFIED","status":"OTHER"}')
        listing = '[{"Path":"part","Size":true,"Hashes":{"DropboxHash":"' + "0" * 64 + '"}}]'
        with patch.object(archive, "checked", return_value=listing):
            with self.assertRaisesRegex(RuntimeError, "Missing or invalid"):
                archive.hash_listing("fixture")

    def test_source_changed_during_copy_fails_without_removing_anything(self):
        def alter(root):
            if root == self.root / archive.TARGETS[0]:
                self.first_file().write_bytes(b"changed fixture")
        self.fake.after_copy = alter
        with self.assertRaisesRegex(RuntimeError, "differ|changed"):
            archive.archive()
        self.assert_all_sources_present()

    def test_missing_remote_hash_fails_closed(self):
        self.fake.remote_hash_override = ""
        with self.assertRaisesRegex(RuntimeError, "Missing or invalid"):
            archive.archive()
        self.assert_all_sources_present()

    def test_wrong_remote_hash_fails_closed(self):
        self.fake.remote_hash_override = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "hashes differ"):
            archive.archive()
        self.assert_all_sources_present()

    def test_control_upload_failure_leaves_sources_and_blocks_removal(self):
        self.fake.fail_upload = True
        with self.assertRaisesRegex(RuntimeError, "Command failed"):
            archive.archive()
        self.assert_all_sources_present()
        with self.assertRaisesRegex(RuntimeError, "Current remote"):
            archive.remove()
        self.assert_all_sources_present()

    def test_remote_extra_file_blocks_all_removal(self):
        archive.archive()
        self.fake.files["unapproved-extra"] = b"unexpected"
        with self.assertRaisesRegex(RuntimeError, "Current remote"):
            archive.remove()
        self.assert_all_sources_present()

    def test_changed_remaining_local_file_blocks_all_removal(self):
        archive.archive()
        self.first_file().write_bytes(b"new local source")
        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            archive.remove()
        self.assert_all_sources_present()

    def test_new_local_file_blocks_all_removal(self):
        archive.archive()
        (self.root / archive.TARGETS[1] / "new.parquet").write_bytes(b"new")
        with self.assertRaisesRegex(RuntimeError, "Unrecorded"):
            archive.remove()
        self.assert_all_sources_present()

    def test_partial_removal_resumes_and_preserves_repaired_raw_metadata_paper(self):
        archive.archive()
        original_remove = archive.remove_node
        count = 0
        def interrupt_after_first(name, record, frozen):
            nonlocal count
            if count == 1:
                raise RuntimeError("fixture interruption")
            original_remove(name, record, frozen)
            count += 1
        with patch.object(archive, "remove_node", interrupt_after_first):
            with self.assertRaisesRegex(RuntimeError, "fixture interruption"):
                archive.remove()
        self.assertEqual(archive.remove()["status"], "REMOVED")
        self.assertEqual(archive.remove()["status"], "REMOVED")
        for target in archive.TARGETS:
            self.assertFalse((self.root / target).exists())
        self.assert_protected_sources_present()

    def test_corrupt_local_manifest_blocks_removal(self):
        archive.archive()
        path = self.run / archive.MANIFEST_NAME
        path.chmod(0o644)
        manifest = json.loads(path.read_text())
        manifest["inventory"]["entries"]["../outside"] = {"kind": "file", "stat": {}}
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(RuntimeError, "Invalid relative"):
            archive.remove()
        self.assert_all_sources_present()

    def test_production_guard_refuses_before_any_command_or_run_write(self):
        import production_guard
        with patch.object(production_guard, "require_production_host", side_effect=RuntimeError("guard refusal")):
            with self.assertRaisesRegex(RuntimeError, "guard refusal"):
                archive.main(["archive"])
        self.assertFalse(self.run.exists())
        self.assertEqual(self.fake.commands, [])

    def test_remove_requires_root_active_job_confirmation(self):
        with patch.object(archive, "environment_gate"):
            with self.assertRaisesRegex(RuntimeError, "confirm-no-active-jobs"):
                archive.main(["remove"])
        self.assertFalse(self.run.exists())

    def test_exact_88_file_shape_removes_only_verified_originals(self):
        for index, target in enumerate(archive.TARGETS):
            root = self.root / target
            (root / "month=fixture/part.parquet").unlink()
            (root / "month=fixture").rmdir()
            (root / "empty").rmdir()
            for month in range(44):
                directory = root / f"year_month=fixture{month:02d}"
                directory.mkdir()
                (directory / "part.parquet").write_bytes(f"fixture-{index}".encode())
        with patch.object(archive, "EXPECTED_COUNTS",
                          {target: (44, 45, 396) for target in archive.TARGETS}):
            result = archive.archive()
            self.assertEqual(result["files"], 88)
            manifest = archive.load_verified()
            self.assertEqual(len(manifest["archive_files"]), 88)
            self.assertEqual(manifest["totals"]["directories"], 90)
            self.assertEqual(len(self.fake.files), 90)
            self.assertEqual(archive.remove()["archive_files"], 88)
        self.assertTrue(all(not (self.root / target).exists() for target in archive.TARGETS))
        self.assert_protected_sources_present()

    def test_protected_and_prefix_collision_paths_are_not_approved(self):
        for name in (
                "pipeline_root_output/trades.parquet_other/part.parquet",
                "pipeline_output/trades_clean.parquet_other/part.parquet",
                "pipeline_data/resolved_trades.parquet",
                "runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/current.parquet",
                "runs/2026-10-14_kaushik_exchange_filtered_estimates_v1/estimates.json"):
            with self.subTest(path=name):
                with self.assertRaisesRegex(RuntimeError, "outside the approved targets"):
                    archive.approved_path(name)
        self.assert_all_sources_present()


class FrozenArchiveProfileTests(unittest.TestCase):
    def test_two_approved_original_tables_have_exact_frozen_profile(self):
        self.assertEqual(archive.TARGETS, (
            "pipeline_root_output/trades.parquet",
            "pipeline_output/trades_clean.parquet"))
        self.assertEqual(archive.EXPECTED_COUNTS, {
            archive.TARGETS[0]: (44, 45, 41335439747),
            archive.TARGETS[1]: (44, 45, 23800759225)})
        self.assertEqual(sum(value[0] for value in archive.EXPECTED_COUNTS.values()), 88)
        self.assertEqual(sum(value[2] for value in archive.EXPECTED_COUNTS.values()), 65136198972)
        self.assertEqual(archive.RUN_DIR, Path("/mnt/data/runs/2026-10-26_final_sample_storage_archive_v1"))
        self.assertEqual(archive.REMOTE_ROOT,
                         "dropbox:Polymarket Data and Code/Archives/2026-10-26_final_sample_storage_archive_v1")
        self.assertEqual(archive.EXPECTED_UUID, original_archive.EXPECTED_UUID)

    def test_nonprofile_safety_functions_are_source_identical(self):
        originals = {name: value for name, value in vars(original_archive).items()
                     if inspect.isfunction(value) and value.__module__ == original_archive.__name__}
        replacements = {name: value for name, value in vars(archive).items()
                        if inspect.isfunction(value) and value.__module__ == archive.__name__}
        self.assertEqual(set(replacements), set(originals))
        for name in sorted(originals.keys() - {"restore_text"}):
            with self.subTest(function=name):
                self.assertEqual(inspect.getsource(replacements[name]),
                                 inspect.getsource(originals[name]))

    def test_restore_contract_names_only_approved_paths_and_warns_legacy_identity(self):
        text = archive.restore_text().decode()
        for target in archive.TARGETS:
            self.assertIn(str(archive.DATA_ROOT / target), text)
            self.assertIn(archive.REMOTE_ROOT + "/" + target, text)
        self.assertIn("65,136,198,972 bytes (65.14 GB decimal), not 90 GB", text)
        self.assertIn("legacy synthetic counterparty wallet attribution is unsafe", text)
        self.assertIn("bytes, not original filesystem inodes", text)
        self.assertIn("Corrected repair copies", text)
        self.assertIn("outside this archive/removal contract", text)
        self.assertNotIn("quotes_ticks", text)


if __name__ == "__main__":
    unittest.main()
