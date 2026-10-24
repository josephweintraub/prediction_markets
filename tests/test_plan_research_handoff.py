"""Synthetic fixtures for explicit research-handoff plan generation."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts import build_research_handoff as builder
from scripts import plan_research_handoff as planner


class PlanResearchHandoffTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "snapshot"
        self.artifacts = self.root / "artifacts"
        self.source.mkdir()
        self.artifacts.mkdir()
        (self.source / "module.py").write_bytes(b"# synthetic source\n")
        (self.artifacts / "summary.json").write_bytes(b'{"fixture": true}\n')
        self.output = self.root / "plan.json"

    def selection(self):
        return {
            "release_name": "joseph_code_fixture_v1",
            "source_files": ["module.py"],
            "extra_files": [{
                "root": "artifacts", "path": "summary.json",
                "destination": "studies/summary.json", "role": "summary",
            }],
        }

    def write(self, selection=None):
        return planner.write_plan(
            self.selection() if selection is None else selection,
            self.source, self.artifacts, self.output,
        )

    def test_plan_round_trip_builds_and_preserves_source(self):
        before = (self.source / "module.py").read_bytes()
        plan = self.write()
        self.assertEqual(json.loads(self.output.read_text()), plan)
        self.assertEqual(set(plan), {"release_name", "files"})
        self.assertEqual([entry["destination"] for entry in plan["files"]], [
            "code/module.py", "studies/summary.json",
        ])
        bundle = self.root / "release"
        manifest = builder.build_handoff(plan, bundle)
        self.assertEqual(builder.verify_handoff(bundle), manifest)
        self.assertEqual((self.source / "module.py").read_bytes(), before)
        again = planner.make_plan(self.selection(), self.source, self.artifacts)
        self.assertEqual(again, plan)

    def test_missing_file_does_not_publish_plan(self):
        selection = self.selection()
        selection["source_files"] = ["missing.py"]
        with self.assertRaises(FileNotFoundError):
            self.write(selection)
        self.assertFalse(self.output.exists())

    def test_symlink_files_parents_and_roots_are_rejected(self):
        (self.source / "linked.py").symlink_to(self.source / "module.py")
        (self.source / "linked_dir").symlink_to(self.artifacts, target_is_directory=True)
        for relative in ("linked.py", "linked_dir/summary.json"):
            with self.subTest(relative=relative):
                selection = self.selection()
                selection["source_files"] = [relative]
                with self.assertRaisesRegex(builder.HandoffError, "Symlink"):
                    self.write(selection)
                self.assertFalse(self.output.exists())
        linked_root = self.root / "linked_root"
        linked_root.symlink_to(self.source, target_is_directory=True)
        with self.assertRaisesRegex(builder.HandoffError, "Symlink"):
            planner.make_plan(self.selection(), linked_root, self.artifacts)

    def test_traversal_absolute_and_glob_selectors_are_rejected(self):
        for relative in ("../artifacts/summary.json", "a/../../module.py", "/module.py", "./module.py", "*.py", "module?.py", "module[1].py"):
            with self.subTest(relative=relative):
                selection = self.selection()
                selection["source_files"] = [relative]
                with self.assertRaises(builder.HandoffError):
                    self.write(selection)
                selection = self.selection()
                selection["extra_files"][0]["path"] = relative
                with self.assertRaises(builder.HandoffError):
                    self.write(selection)
                self.assertFalse(self.output.exists())

    def test_duplicate_destinations_are_rejected(self):
        selection = self.selection()
        selection["extra_files"][0]["destination"] = "code/module.py"
        with self.assertRaisesRegex(builder.HandoffError, "Duplicate"):
            self.write(selection)
        self.assertFalse(self.output.exists())

    def test_existing_output_is_not_overwritten(self):
        self.output.write_bytes(b"user contents")
        with self.assertRaisesRegex(builder.HandoffError, "already exists"):
            self.write()
        self.assertEqual(self.output.read_bytes(), b"user contents")

    def test_cannot_write_plan_into_source_snapshot(self):
        with self.assertRaisesRegex(builder.HandoffError, "outside"):
            planner.write_plan(self.selection(), self.source, self.artifacts, self.source / "plan.json")
        self.assertFalse((self.source / "plan.json").exists())

    def test_directory_and_relative_roots_are_rejected(self):
        selection = self.selection()
        selection["source_files"] = ["nested"]
        (self.source / "nested").mkdir()
        with self.assertRaisesRegex(builder.HandoffError, "regular file"):
            self.write(selection)
        with self.assertRaisesRegex(builder.HandoffError, "absolute"):
            planner.make_plan(self.selection(), Path("relative"), self.artifacts)

    def test_strict_selection_schema_and_role(self):
        for change in ("unknown_key", "wrong_root", "empty_role", "nonlist_source", "nonlist_extra", "extra_key"):
            with self.subTest(change=change):
                selection = self.selection()
                if change == "unknown_key":
                    selection["unknown"] = True
                elif change == "wrong_root":
                    selection["extra_files"][0]["root"] = "elsewhere"
                elif change == "empty_role":
                    selection["extra_files"][0]["role"] = "  "
                elif change == "nonlist_source":
                    selection["source_files"] = "module.py"
                elif change == "nonlist_extra":
                    selection["extra_files"] = {}
                else:
                    selection["extra_files"][0]["unknown"] = True
                with self.assertRaises(builder.HandoffError):
                    self.write(selection)
                self.assertFalse(self.output.exists())

    def test_source_extra_file_and_empty_source_list(self):
        selection = self.selection()
        selection["source_files"] = []
        selection["extra_files"] = [{
            "root": "source", "path": "module.py", "destination": "reference/module.py",
            "role": "reference_source",
        }]
        self.assertEqual(self.write(selection)["files"][0]["destination"], "reference/module.py")

    def evidence_selection(self, role="aggregate_summary", destination="studies/summary.parquet"):
        selection = self.selection()
        selection["source_files"] = []
        selection["extra_files"] = [{
            "root": "artifacts", "path": "summary.parquet",
            "destination": destination, "role": role,
        }]
        return selection

    def test_explicit_evidence_parquet_roles_round_trip(self):
        # Admission roles are assertions, not a Parquet schema audit.
        (self.artifacts / "summary.parquet").write_bytes(b"synthetic evidence fixture")
        for role in ("aggregate_summary", "bounded_metadata"):
            with self.subTest(role=role):
                plan = planner.make_plan(self.evidence_selection(role), self.source, self.artifacts)
                bundle = self.root / role
                builder.build_handoff(plan, bundle)
                self.assertEqual(builder.verify_handoff(bundle)["files"][0]["role"], role)

    def test_generic_parquet_and_suffix_renaming_are_rejected(self):
        (self.artifacts / "summary.parquet").write_bytes(b"fixture")
        for selection in (
            self.evidence_selection("summary"),
            self.evidence_selection(destination="studies/summary.json"),
        ):
            with self.subTest(selection=selection):
                with self.assertRaises(builder.HandoffError):
                    self.write(selection)
                self.assertFalse(self.output.exists())

    def test_oversized_evidence_parquet_rejected_before_hashing(self):
        path = self.artifacts / "summary.parquet"
        with path.open("wb") as handle:
            handle.truncate(builder.MAX_EVIDENCE_PARQUET_BYTES + 1)
        with mock.patch.object(builder, "_fingerprint", side_effect=AssertionError("must not hash")):
            with self.assertRaisesRegex(builder.HandoffError, "8 MiB"):
                self.write(self.evidence_selection())
        self.assertFalse(self.output.exists())

    def test_evidence_size_admission_rechecked_after_hashing(self):
        (self.artifacts / "summary.parquet").write_bytes(b"fixture")
        with mock.patch.object(builder, "_fingerprint", return_value=(builder.MAX_EVIDENCE_PARQUET_BYTES + 1, "0" * 64)):
            with self.assertRaisesRegex(builder.HandoffError, "8 MiB"):
                self.write(self.evidence_selection())
        self.assertFalse(self.output.exists())

    def test_cli_and_direct_script_entrypoint(self):
        selection_path = self.root / "selection.json"
        selection_path.write_text(json.dumps(self.selection()), encoding="utf-8")
        arguments = [
            "--source-root", str(self.source), "--artifact-root", str(self.artifacts),
            "--selection", str(selection_path), "--output", str(self.output),
        ]
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(planner.main(arguments), 0)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(planner.main(arguments), 1)
        arguments[-1] = str(self.root / "second_plan.json")
        completed = subprocess.run(
            [sys.executable, "-B", str(Path(planner.__file__).resolve()), *arguments],
            cwd=self.root, capture_output=True, text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)["file_count"], 2)


if __name__ == "__main__":
    unittest.main()
