"""Small, synthetic fixtures for the explicit research handoff packager."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import build_research_handoff as handoff


class ResearchHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "source.py"
        self.source.write_bytes(b"print('small fixture')\n")
        self.output = self.root / "release"

    def entry(self, source=None, destination="code/source.py", role="source"):
        source = source or self.source
        return {
            "source": str(source), "destination": destination,
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "role": role,
        }

    def plan(self, entries=None):
        return {"release_name": "joseph_code_fixture_v1", "files": entries or [self.entry()]}

    def build(self):
        return handoff.build_handoff(self.plan(), self.output)

    def test_round_trip_is_deterministic_and_does_not_mutate_source(self):
        original = self.source.read_bytes()
        manifest = self.build()
        self.assertEqual(handoff.verify_handoff(self.output), manifest)
        self.assertEqual((self.output / "code/source.py").read_bytes(), original)
        self.assertEqual(self.source.read_bytes(), original)
        other = self.root / "other_release"
        handoff.build_handoff(self.plan(), other)
        for name in (handoff.MANIFEST, handoff.COMPLETE):
            self.assertEqual((self.output / name).read_bytes(), (other / name).read_bytes())
        self.assertNotIn("source", manifest["files"][0])

    def test_explicit_summary_and_document_archives_are_allowed(self):
        entries = []
        for name in ("summary.json", "report.tex", "figure.pdf", "source.zip", "source.tar.gz"):
            path = self.root / name
            path.write_bytes(b"fixture, not a production artifact")
            entries.append(self.entry(path, "explicit/" + name))
        handoff.build_handoff(self.plan(entries), self.output)
        self.assertEqual(len(handoff.verify_handoff(self.output)["files"]), 5)

    def test_parquet_evidence_roles_require_separate_grain_audit_but_round_trip(self):
        for role in ("aggregate_summary", "bounded_metadata"):
            with self.subTest(role=role):
                path = self.root / (role + ".parquet")
                # Deliberately not real Parquet: this gate checks assertion and
                # bytes only, not the separate schema/row-count audit.
                path.write_bytes(b"synthetic evidence payload")
                output = self.root / role
                entry = self.entry(path, "evidence/" + path.name, role)
                handoff.build_handoff(self.plan([entry]), output)
                self.assertEqual(handoff.verify_handoff(output)["files"][0]["role"], role)

    def test_parquet_wrong_role_and_renaming_are_rejected(self):
        path = self.root / "summary.parquet"
        path.write_bytes(b"synthetic evidence payload")
        for role in ("source", "summary", "aggregate_summary ", "raw_data"):
            with self.subTest(role=role):
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(self.plan([self.entry(path, "evidence/summary.parquet", role)]), self.output)
        with self.assertRaisesRegex(handoff.HandoffError, "suffixes"):
            handoff.build_handoff(self.plan([self.entry(path, "evidence/summary.bin", "aggregate_summary")]), self.output)

    def test_parquet_oversize_is_rejected_before_hashing_or_output_creation(self):
        path = self.root / "summary.parquet"
        path.write_bytes(b"synthetic")
        entry = self.entry(path, "evidence/summary.parquet", "aggregate_summary")
        with path.open("ab") as handle:
            handle.truncate(handoff.MAX_EVIDENCE_PARQUET_BYTES + 1)
        with mock.patch.object(handoff, "_fingerprint") as fingerprint:
            with self.assertRaisesRegex(handoff.HandoffError, "8 MiB"):
                handoff.build_handoff(self.plan([entry]), self.output)
        fingerprint.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_shared_admission_gate_enforces_exact_parquet_size_boundary(self):
        for role in ("aggregate_summary", "bounded_metadata"):
            handoff.check_file_admission("summary.parquet", role, handoff.MAX_EVIDENCE_PARQUET_BYTES)
            with self.assertRaisesRegex(handoff.HandoffError, "8 MiB"):
                handoff.check_file_admission("summary.parquet", role, handoff.MAX_EVIDENCE_PARQUET_BYTES + 1)
            for forbidden in ("secret.pem", "secret.key", ".env", "rclone.conf", "raw.duckdb"):
                with self.subTest(role=role, forbidden=forbidden):
                    with self.assertRaises(handoff.HandoffError):
                        handoff.check_file_admission(forbidden, role, 1)

    def test_parquet_drift_and_oversize_are_rejected_during_verify(self):
        path = self.root / "summary.parquet"
        path.write_bytes(b"synthetic evidence payload")
        entry = self.entry(path, "evidence/summary.parquet", "bounded_metadata")
        handoff.build_handoff(self.plan([entry]), self.output)
        target = self.output / "evidence/summary.parquet"
        target.write_bytes(b"changed")
        with self.assertRaisesRegex(handoff.HandoffError, "Size/hash"):
            handoff.verify_handoff(self.output)
        with target.open("ab") as handle:
            handle.truncate(handoff.MAX_EVIDENCE_PARQUET_BYTES + 1)
        with self.assertRaisesRegex(handoff.HandoffError, "8 MiB"):
            handoff.verify_handoff(self.output)

    def test_parquet_manifest_cannot_assert_an_oversize_member(self):
        path = self.root / "summary.parquet"
        path.write_bytes(b"synthetic evidence payload")
        handoff.build_handoff(self.plan([self.entry(path, "evidence/summary.parquet", "aggregate_summary")]), self.output)
        manifest_path = self.output / handoff.MANIFEST
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["files"][0]["size"] = handoff.MAX_EVIDENCE_PARQUET_BYTES + 1
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(handoff.HandoffError, "8 MiB"):
            handoff.verify_handoff(self.output)

    def test_source_drift_is_rejected_before_creating_output(self):
        plan = self.plan()
        self.source.write_bytes(b"changed")
        with self.assertRaisesRegex(handoff.HandoffError, "hash"):
            handoff.build_handoff(plan, self.output)
        self.assertFalse(self.output.exists())

    def test_verify_rejects_payload_drift(self):
        self.build()
        (self.output / "code/source.py").write_bytes(b"changed")
        with self.assertRaisesRegex(handoff.HandoffError, "Size/hash"):
            handoff.verify_handoff(self.output)

    def test_verify_rejects_missing_and_extra_files(self):
        self.build()
        target = self.output / "code/source.py"
        target.unlink()
        with self.assertRaisesRegex(handoff.HandoffError, "Inventory mismatch"):
            handoff.verify_handoff(self.output)
        target.write_bytes(self.source.read_bytes())
        (self.output / "unexpected.txt").write_text("extra", encoding="utf-8")
        with self.assertRaisesRegex(handoff.HandoffError, "Inventory mismatch"):
            handoff.verify_handoff(self.output)

    def test_duplicate_and_case_colliding_destinations_are_rejected(self):
        for destinations in (("code/a.py", "code/a.py"), ("code/A.py", "CODE/a.py"), ("Code/a.py", "code/b.py"), ("manifest.json",)):
            with self.subTest(destinations=destinations):
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(self.plan([self.entry(destination=d) for d in destinations]), self.output)
                self.assertFalse(self.output.exists())

    def test_nonportable_destinations_are_rejected(self):
        for destination in ("../source.py", "/source.py", "a/../source.py", "a\\source.py", "a//b", "./a", "C:/a", "a/", "a\n.py"):
            with self.subTest(destination=destination):
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(self.plan([self.entry(destination=destination)]), self.output)

    def test_file_directory_collision_is_rejected(self):
        entries = [self.entry(destination="code"), self.entry(destination="code/source.py")]
        with self.assertRaisesRegex(handoff.HandoffError, "collision"):
            handoff.build_handoff(self.plan(entries), self.output)

    def test_source_and_parent_symlinks_are_rejected(self):
        link = self.root / "link.py"
        link.symlink_to(self.source)
        with self.assertRaisesRegex(handoff.HandoffError, "Symlink"):
            handoff.build_handoff(self.plan([self.entry(link)]), self.output)
        directory = self.root / "linked_directory"
        directory.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(handoff.HandoffError, "Symlink"):
            handoff.build_handoff(self.plan([self.entry(directory / "source.py")]), self.output)

    def test_directory_source_is_rejected(self):
        entry = self.entry()
        entry["source"] = str(self.root)
        with self.assertRaisesRegex(handoff.HandoffError, "regular file"):
            handoff.build_handoff(self.plan([entry]), self.output)

    def test_forbidden_source_or_destination_names_are_rejected(self):
        for name in ("secret.pem", "secret.key", ".env", ".env.local", "rclone.conf", "trades.parquet", "cache.duckdb", "credentials"):
            with self.subTest(name=name):
                source = self.root / name
                source.write_bytes(b"synthetic")
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(self.plan([self.entry(source)]), self.output)
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(self.plan([self.entry(destination="files/" + name)]), self.output)

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir()
        sentinel = self.output / "keep.txt"
        sentinel.write_bytes(b"user data")
        with self.assertRaisesRegex(handoff.HandoffError, "already exists"):
            self.build()
        self.assertEqual(sentinel.read_bytes(), b"user data")

    def test_copy_failure_leaves_no_completion_marker(self):
        with mock.patch.object(handoff, "copy_checked_file", side_effect=OSError("fixture failure")):
            with self.assertRaisesRegex(OSError, "fixture failure"):
                self.build()
        self.assertTrue(self.output.exists())
        self.assertFalse((self.output / handoff.COMPLETE).exists())
        with self.assertRaisesRegex(handoff.HandoffError, "incomplete"):
            handoff.verify_handoff(self.output)

    def test_copy_rechecks_source_after_prevalidation(self):
        copy = handoff.copy_checked_file

        def changed_before_copy(source, destination, size, sha256):
            self.source.write_bytes(b"changed after prevalidation")
            copy(source, destination, size, sha256)

        with mock.patch.object(handoff, "copy_checked_file", side_effect=changed_before_copy):
            with self.assertRaisesRegex(handoff.HandoffError, "changed while copying"):
                self.build()
        self.assertFalse((self.output / handoff.COMPLETE).exists())

    def test_verify_rejects_symlinks_and_extra_empty_directories(self):
        self.build()
        link = self.output / "linked.py"
        link.symlink_to(self.source)
        with self.assertRaisesRegex(handoff.HandoffError, "Symlink"):
            handoff.verify_handoff(self.output)
        link.unlink()
        (self.output / "extra").mkdir()
        with self.assertRaisesRegex(handoff.HandoffError, "directory"):
            handoff.verify_handoff(self.output)

    def test_manifest_and_completion_marker_are_bound(self):
        self.build()
        path = self.output / handoff.COMPLETE
        marker = json.loads(path.read_text(encoding="utf-8"))
        marker["file_count"] += 1
        path.write_text(json.dumps(marker), encoding="utf-8")
        with self.assertRaisesRegex(handoff.HandoffError, "Completion marker"):
            handoff.verify_handoff(self.output)

    def test_strict_schema_and_sha256(self):
        plans = [self.plan(), self.plan(), self.plan(), self.plan()]
        plans[0]["unexpected"] = True
        del plans[1]["files"][0]["sha256"]
        plans[2]["files"][0]["sha256"] = "not a hash"
        plans[3]["files"][0]["source"] = "relative.py"
        for plan in plans:
            with self.subTest(plan=plan):
                with self.assertRaises(handoff.HandoffError):
                    handoff.build_handoff(plan, self.output)

    def test_cli_round_trip_and_error_exit(self):
        plan_path = self.root / "plan.json"
        plan_path.write_text(json.dumps(self.plan()), encoding="utf-8")
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(handoff.main(["build", "--plan", str(plan_path), "--output", str(self.output)]), 0)
            self.assertEqual(handoff.main(["verify", "--bundle", str(self.output)]), 0)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(handoff.main(["build", "--plan", str(plan_path), "--output", str(self.output)]), 1)


if __name__ == "__main__":
    unittest.main()
