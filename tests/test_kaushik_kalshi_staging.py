"""Tiny local staging fixtures: no network, rclone or production data access."""
from contextlib import ExitStack
import hashlib
import json
from pathlib import Path
import stat
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from analysis.kaushik_kalshi_replication import staging as s


class KalshiStagingTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="kalshi-stage-fixture-")
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name).resolve()
        self.run = self.root / "run_v1"
        self.run.mkdir()
        self.payloads = {"trades_all/dt=2026-01-01/part-000.parquet": b"first synthetic shard",
                         "trades_all/dt=2026-01-02/part-000.parquet": b"second synthetic shard"}
        self.entries = [s.ManifestEntry(name, len(raw), hashlib.sha256(raw).hexdigest())
                        for name, raw in self.payloads.items()]
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.guard = stack.enter_context(patch.object(s.production_guard, "require_production_host"))
        stack.enter_context(patch.object(s, "RUN_ROOT", self.root))
        self.space = stack.enter_context(patch.object(s.shutil, "disk_usage", return_value=SimpleNamespace(free=100_000_000_000)))
        self.transfer = stack.enter_context(patch.object(s.subprocess, "run", side_effect=self.copy_fixture))
        self.callback_paths = []

    def copy_fixture(self, command, *, check, capture_output=False, text=False):
        self.assertTrue(check)
        source = command[2].removeprefix("dropbox:Polymarket Data and Code/kalshi/")
        size = len(self.payloads[source])
        if command[:2] == ["rclone", "lsjson"]:
            self.assertEqual(command[3:], ["--stat", "--no-mimetype"])
            self.assertTrue(capture_output and text)
            info = {"Name": Path(source).name, "Path": Path(source).name, "Size": size,
                    "IsDir": False, "ID": "fixture-id", "ModTime": "2026-01-03T00:00:00Z"}
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(info))
        self.assertEqual(command[:2], ["rclone", "copyto"])
        self.assertFalse(capture_output or text)
        self.assertNotIn("--max-size", command)
        self.assertEqual(command[4:], ["--immutable", "--transfers", "1", "--checkers", "1", "--multi-thread-streams", "0",
                                      "--max-transfer", f"{size + 1}B", "--cutoff-mode", "HARD"])
        destination = Path(command[3])
        self.assertFalse(destination.exists())
        destination.write_bytes(self.payloads[source])

    def process(self, path, entry):
        self.assertEqual(path.read_bytes(), self.payloads[entry.path])
        self.callback_paths.append(path)
        return {"summary": entry.path, "rows": 2}

    def execute(self, entries=None, process=None, **changes):
        kwargs = {"remote_root": "dropbox:Polymarket Data and Code/kalshi/", "run_dir": self.run,
                  "output_spill_reserve_bytes": 4_000_000_000, "process": process or self.process}
        kwargs.update(changes)
        return s.stage_and_process(self.entries if entries is None else entries, **kwargs)

    def staged_files(self):
        return list(self.run.glob("source_stage_v1/shard-*/*"))

    def test_serial_processing_verification_receipts_and_exact_cleanup(self):
        sentinel = self.run / "user-owned.parquet"
        sentinel.write_bytes(b"preserve me")
        original_write = s._write_receipt

        def durable_before_cleanup(path, receipt):
            self.assertTrue(Path(receipt["temporary_source"]).exists())
            original_write(path, receipt)

        def process(path, entry):
            self.assertEqual(len(self.staged_files()), 1)
            return self.process(path, entry)

        with patch.object(s, "_write_receipt", side_effect=durable_before_cleanup):
            receipts = self.execute(process=process)
        self.assertEqual(len(receipts), 2)
        self.assertEqual(self.staged_files(), [])
        self.assertEqual(sentinel.read_bytes(), b"preserve me")
        self.assertEqual(self.transfer.call_count, 4)
        self.assertEqual([call.args[0][1] for call in self.transfer.call_args_list],
                         ["lsjson", "copyto", "lsjson", "copyto"])
        for path, entry in zip(receipts, self.entries):
            receipt = json.loads(path.read_text())
            self.assertEqual(receipt["entry"], {"path": entry.path, "size_bytes": entry.size_bytes, "sha256": entry.sha256})
            self.assertEqual(receipt["status"], "processed_verified")
            self.assertEqual(receipt["callback_provenance"]["summary"], entry.path)
            self.assertEqual(receipt["remote_stat"]["Size"], entry.size_bytes)
            self.assertNotIn("--max-size", receipt["transfer_command"])

    def test_production_guard_precedes_validation_and_io(self):
        self.guard.side_effect = RuntimeError("guard blocked")
        with self.assertRaisesRegex(RuntimeError, "guard blocked"):
            self.execute(entries=[])
        self.transfer.assert_not_called()
        self.assertFalse((self.run / "source_stage_v1").exists())

    def test_manifest_rejects_traversal_ambiguity_hashes_and_sizes(self):
        for path in ("../source", "/source", "a/../b", "a//b", "./source", "a\\b", "a:b", "a/", "a\nfile"):
            with self.subTest(path=path), self.assertRaises(s.StagingBlocked):
                s.ManifestEntry(path, 1, "a" * 64)
        for size in (0, -1, True, 1.0, "1"):
            with self.subTest(size=size), self.assertRaises(s.StagingBlocked):
                s.ManifestEntry("source", size, "a" * 64)
        for digest in ("A" * 64, "a" * 63, "", None):
            with self.subTest(digest=digest), self.assertRaises(s.StagingBlocked):
                s.ManifestEntry("source", 1, digest)

    def test_duplicate_entries_fail_before_transfer_or_stage(self):
        with self.assertRaisesRegex(s.StagingBlocked, "duplicate"):
            self.execute(entries=[self.entries[0], self.entries[0]])
        self.transfer.assert_not_called()
        self.assertFalse((self.run / "source_stage_v1").exists())

    def test_existing_stage_never_overwritten_or_deleted(self):
        stage = self.run / "source_stage_v1"
        stage.mkdir()
        sentinel = stage / "existing.parquet"
        sentinel.write_bytes(b"user")
        with self.assertRaisesRegex(s.StagingBlocked, "immutable stage"):
            self.execute()
        self.assertEqual(sentinel.read_bytes(), b"user")
        self.transfer.assert_not_called()

    def test_capacity_includes_largest_shard_floor_and_declared_reserve(self):
        required = s.FREE_SPACE_FLOOR_BYTES + 4_000_000_000 + max(e.size_bytes for e in self.entries) + 1
        self.space.return_value.free = required - 1
        with self.assertRaisesRegex(s.StagingBlocked, "insufficient free space"):
            self.execute()
        self.transfer.assert_not_called()
        self.assertFalse((self.run / "source_stage_v1").exists())
        self.space.return_value.free = required
        self.assertEqual(len(self.execute()), 2)

    def test_capacity_rechecked_between_shards(self):
        observed = []

        def process(path, entry):
            observed.append(entry.path)
            self.space.return_value.free = s.FREE_SPACE_FLOOR_BYTES
            return {"done": True}

        with self.assertRaisesRegex(s.StagingBlocked, "insufficient free space"):
            self.execute(process=process)
        self.assertEqual(observed, [self.entries[0].path])
        self.assertEqual(self.transfer.call_count, 2)
        self.assertEqual(self.staged_files(), [])
        self.assertTrue((self.run / "source_stage_v1/receipts/000000.json").exists())

    def test_callback_floor_failure_preserves_source_without_receipt(self):
        def exhaust_reserve(path, entry):
            self.space.return_value.free = s.FREE_SPACE_FLOOR_BYTES - 1
            return {"done": True}

        with self.assertRaisesRegex(s.StagingBlocked, "insufficient free space"):
            self.execute(process=exhaust_reserve)
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(list(self.run.glob("source_stage_v1/receipts/*")), [])

    def test_size_or_hash_mismatch_never_reaches_callback_and_preserves_copy(self):
        for corruption in (b"wrong", b"x" * len(self.payloads[self.entries[0].path])):
            with self.subTest(corruption=corruption):
                run = self.root / ("bad-" + str(len(corruption)))
                run.mkdir()

                def corrupt(command, *, check, **options):
                    if command[1] == "lsjson":
                        return self.copy_fixture(command, check=check, **options)
                    Path(command[3]).write_bytes(corruption)

                with patch.object(s.subprocess, "run", side_effect=corrupt):
                    with self.assertRaisesRegex(s.StagingBlocked, "mismatch"):
                        self.execute(entries=[self.entries[0]], run_dir=run)
                self.assertEqual(self.callback_paths, [])
                self.assertEqual(len(list(run.glob("source_stage_v1/shard-*/*"))), 1)
                self.assertEqual(list(run.glob("source_stage_v1/receipts/*")), [])

    def test_transfer_failure_preserves_partial_and_does_not_process(self):
        def fail(command, *, check, **options):
            if command[1] == "lsjson":
                return self.copy_fixture(command, check=check, **options)
            Path(command[3]).write_bytes(b"partial")
            raise subprocess.CalledProcessError(1, command)

        self.transfer.side_effect = fail
        with self.assertRaises(subprocess.CalledProcessError):
            self.execute()
        self.assertEqual(self.callback_paths, [])
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(self.staged_files()[0].read_bytes(), b"partial")

    def test_copy_that_produces_no_file_never_reaches_callback(self):
        def skip(command, *, check, **options):
            if command[1] == "lsjson":
                return self.copy_fixture(command, check=check, **options)

        self.transfer.side_effect = skip
        with self.assertRaises(FileNotFoundError):
            self.execute()
        self.assertEqual(self.callback_paths, [])
        self.assertEqual(list(self.run.glob("source_stage_v1/receipts/*")), [])

    def test_remote_stat_mismatch_rejects_before_any_copy(self):
        entry = self.entries[0]
        valid = {"Name": Path(entry.path).name, "Size": entry.size_bytes, "IsDir": False}
        invalid = [dict(valid, Size=entry.size_bytes + 1), dict(valid, Size=True),
                   dict(valid, IsDir=True), dict(valid, Name="another.parquet"), []]
        for index, metadata in enumerate(invalid):
            with self.subTest(metadata=metadata):
                run = self.root / f"stat-mismatch-{index}"
                run.mkdir()
                commands = []

                def wrong_stat(command, *, check, capture_output, text):
                    commands.append(command)
                    self.assertEqual(command[1], "lsjson")
                    return subprocess.CompletedProcess(command, 0, stdout=json.dumps(metadata))

                with patch.object(s.subprocess, "run", side_effect=wrong_stat):
                    with self.assertRaisesRegex(s.StagingBlocked, "remote stat"):
                        self.execute(entries=[entry], run_dir=run)
                self.assertEqual(len(commands), 1)
                self.assertEqual(self.callback_paths, [])
                self.assertEqual(list(run.glob("source_stage_v1/shard-*")), [])

    def test_remote_stat_error_is_not_retried_as_a_transfer(self):
        self.transfer.side_effect = subprocess.CalledProcessError(1, ["rclone", "lsjson"])
        with self.assertRaises(subprocess.CalledProcessError):
            self.execute()
        self.assertEqual(self.transfer.call_count, 1)
        self.assertEqual(self.transfer.call_args.args[0][1], "lsjson")
        self.assertEqual(self.callback_paths, [])
        self.assertEqual(self.staged_files(), [])

    def test_invalid_remote_stat_json_is_not_ignored(self):
        self.transfer.return_value = subprocess.CompletedProcess(["rclone", "lsjson"], 0, stdout="not JSON")
        self.transfer.side_effect = None
        with self.assertRaises(json.JSONDecodeError):
            self.execute()
        self.assertEqual(self.transfer.call_count, 1)
        self.assertEqual(self.callback_paths, [])
        self.assertEqual(self.staged_files(), [])

    def test_callback_failure_preserves_verified_source_without_receipt(self):
        def fail(path, entry):
            raise ValueError("callback failed")

        with self.assertRaisesRegex(ValueError, "callback failed"):
            self.execute(process=fail)
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(list(self.run.glob("source_stage_v1/receipts/*")), [])

    def test_callback_mutation_or_replacement_preserves_input(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement):
                run = self.root / str(replacement)
                run.mkdir()

                def mutate(path, entry):
                    raw = path.read_bytes()
                    if replacement:
                        path.unlink()
                    path.write_bytes(raw)
                    return {"done": True}

                with self.assertRaisesRegex(s.StagingBlocked, "changed"):
                    self.execute(entries=[self.entries[0]], process=mutate, run_dir=run)
                self.assertEqual(len(list(run.glob("source_stage_v1/shard-*/*"))), 1)
                self.assertEqual(list(run.glob("source_stage_v1/receipts/*")), [])

    def test_symlink_destination_is_rejected_without_deleting_target(self):
        target = self.root / "outside.parquet"
        target.write_bytes(self.payloads[self.entries[0].path])

        def symlink(command, *, check, **options):
            if command[1] == "lsjson":
                return self.copy_fixture(command, check=check, **options)
            Path(command[3]).symlink_to(target)

        self.transfer.side_effect = symlink
        with self.assertRaisesRegex(s.StagingBlocked, "symlink"):
            self.execute()
        self.assertTrue(target.exists())
        self.assertTrue(self.staged_files()[0].is_symlink())
        self.assertEqual(self.callback_paths, [])

    def test_receipt_failure_preserves_verified_source(self):
        with patch.object(s, "_write_receipt", side_effect=OSError("receipt fsync failed")):
            with self.assertRaisesRegex(OSError, "fsync failed"):
                self.execute()
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(len(self.callback_paths), 1)

    def test_actual_receipt_fsync_failure_preserves_source_and_partial_receipt(self):
        original_fsync = s.os.fsync

        def fail_receipt_file(descriptor):
            if not stat.S_ISDIR(s.os.fstat(descriptor).st_mode):
                raise OSError("receipt not durable")
            return original_fsync(descriptor)

        with patch.object(s.os, "fsync", side_effect=fail_receipt_file):
            with self.assertRaisesRegex(OSError, "not durable"):
                self.execute()
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(len(list(self.run.glob("source_stage_v1/receipts/*"))), 1)

    def test_cleanup_failure_keeps_source_and_durable_receipt(self):
        with patch.object(Path, "unlink", side_effect=OSError("cleanup blocked")):
            with self.assertRaisesRegex(OSError, "cleanup blocked"):
                self.execute()
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(len(list(self.run.glob("source_stage_v1/receipts/*"))), 1)

    def test_nonjson_callback_provenance_preserves_source(self):
        with self.assertRaises(ValueError):
            self.execute(process=lambda path, entry: {"invalid": float("nan")})
        self.assertEqual(len(self.staged_files()), 1)
        self.assertEqual(list(self.run.glob("source_stage_v1/receipts/*")), [])

    def test_existing_receipt_never_overwritten(self):
        sentinel = b"preexisting receipt"

        def collide(path, entry):
            receipt = self.run / "source_stage_v1/receipts/000000.json"
            receipt.write_bytes(sentinel)
            return {"done": True}

        with self.assertRaises(FileExistsError):
            self.execute(process=collide)
        self.assertEqual((self.run / "source_stage_v1/receipts/000000.json").read_bytes(), sentinel)
        self.assertEqual(len(self.staged_files()), 1)

    def test_invalid_callback_provenance_and_reserve_fail_closed(self):
        with self.assertRaisesRegex(s.StagingBlocked, "reserve"):
            self.execute(output_spill_reserve_bytes=-1)
        self.transfer.assert_not_called()
        with self.assertRaisesRegex(s.StagingBlocked, "provenance"):
            self.execute(process=lambda path, entry: None)
        self.assertEqual(len(self.staged_files()), 1)

    def test_invalid_remote_and_noncanonical_run_directory_rejected(self):
        for remote in ("s3:bucket/data", "dropbox:../elsewhere", "dropbox:/absolute"):
            with self.subTest(remote=remote), self.assertRaises(s.StagingBlocked):
                self.execute(remote_root=remote)
        with self.assertRaisesRegex(s.StagingBlocked, "canonical run root"):
            self.execute(run_dir=self.root)
        link = self.root / "run-link"
        link.symlink_to(self.run, target_is_directory=True)
        with self.assertRaisesRegex(s.StagingBlocked, "symlink"):
            self.execute(run_dir=link)
        self.transfer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
