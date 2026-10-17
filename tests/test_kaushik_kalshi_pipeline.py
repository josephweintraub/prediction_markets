"""Input CLI fixture integration; all transfers/production admission are mocked."""
from contextlib import ExitStack
from dataclasses import asdict
from datetime import date
import csv
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import build_kaushik_kalshi_inputs as runner
from analysis.kaushik_kalshi_replication import source_contract, staging


class KalshiPipelineTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory(prefix="kalshi-pipeline-fixture-")
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name).resolve()
        self.admission = self.root / "admission"
        self.admission.mkdir()
        self.sources = self.root / "sources"
        self.sources.mkdir()
        self.paths = {}
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)

    def fixture_admission(self, event_category="Sports"):
        metadata_path = self.sources / "b1_0.parquet"
        rows = [{"ticker": ticker, "event_ticker": "KXTEST-EVENT", "market_type": "binary", "status": "finalized", "result": "yes",
                 "open_time": "2026-03-01T00:00:00Z", "settlement_ts": "2026-04-01T00:00:00Z", "notional_value": 1.,
                 "settlement_value": "", "_source": "bulk_v2", "title": "Will Alpha win?", "yes_sub_title": "Alpha"}
                for ticker in ("KXTEST-A", "KXMVE-EXCLUDED")]
        pq.write_table(pa.Table.from_pylist(rows), metadata_path)
        self.paths["markets_metadata/b1_0.parquet"] = metadata_path
        references = {
            "events": [{"event_ticker": "KXTEST-EVENT", "category": event_category, "series_ticker": "KXMLBGAME"}],
            "series": [{"ticker": "KXMLBGAME", "category": "Sports"}],
            "collections": [{"collection_ticker": "KXMVE-COLLECTION", "associated_events": "[]"}],
        }
        for key, rows in references.items():
            pq.write_table(pa.Table.from_pylist(rows), self.admission / (key + ".parquet"))
        for ordinal, day in enumerate(("2026-03-25", "2026-03-26")):
            rows = [{"trade_id": f"00000000-0000-0000-0000-{ordinal*10+index+1:012d}", "ticker": "KXTEST-A",
                     "created_time": day + "T12:00:00Z", "trade_date": date.fromisoformat(day), "taker_side": "yes", "size": .25,
                     "yes_price": price, "no_price": 1-price, "source": "fixture"}
                    for index, price in enumerate((.05, .95, .5))]
            path = self.sources / (day + ".parquet")
            pq.write_table(pa.Table.from_pylist(rows), path)
            self.paths[f"trades_all/dt={day}/part-000.parquet"] = path
        manifests = {
            "trade_manifest": ("MANIFEST_kalshi.csv", ["file", "bytes", "sha256"],
                [[key, path.stat().st_size, runner.sha256(path)] for key, path in self.paths.items() if key.startswith("trades_all/")]),
            "metadata_manifest": ("markets_metadata__MANIFEST.csv", ["path", "bytes", "rows", "sha256"],
                [[source_contract.METADATA_PREFIX + "b1_0.parquet", metadata_path.stat().st_size, 2, runner.sha256(metadata_path)]]),
        }
        plan = {"schema_version": "kaushik_kalshi_admission_plan_v1", "sports_winner_series": runner.SPORTS_WINNER_SERIES,
                "references": {}}
        pins = {}
        for key, (name, fields, rows) in manifests.items():
            text = io.StringIO(newline="")
            writer = csv.writer(text)
            writer.writerow(fields)
            writer.writerows(rows)
            path = self.admission / name
            path.write_bytes(text.getvalue().encode())
            plan[key] = {"path": name, "bytes": path.stat().st_size, "sha256": runner.sha256(path)}
            pins[key] = {key: value for key, value in plan[key].items() if key != "path"}
        for key in references:
            path = self.admission / (key + ".parquet")
            plan["references"][key] = {"path": path.name, "bytes": path.stat().st_size, "sha256": runner.sha256(path)}
            pins[key] = {key: value for key, value in plan["references"][key].items() if key != "path"}
        runner.write_json(self.admission / "admission_plan.json", plan)
        self.stack.enter_context(patch.object(runner, "PINNED", pins))
        self.stack.enter_context(patch.object(runner, "EXPECTED_METADATA_FILES", 1))
        self.stack.enter_context(patch.object(runner, "EXPECTED_METADATA_ROWS", 2))
        self.stack.enter_context(patch.object(runner, "EXPECTED_METADATA_BYTES", metadata_path.stat().st_size))
        self.stack.enter_context(patch.object(source_contract, "validate_trade_days", return_value={
            "files": 2, "bytes": sum(path.stat().st_size for key, path in self.paths.items() if key.startswith("trades_all/")),
            "first_day": "2026-03-25", "last_day": "2026-03-26", "documented_empty_days": [], "maximum_shard_bytes": 10000}))
        return plan

    def stage_fixture(self, entries, *, remote_root, run_dir, output_spill_reserve_bytes, process):
        self.assertEqual(remote_root, runner.REMOTE_ROOT)
        self.assertEqual(output_spill_reserve_bytes, 16_000_000_000)
        receipts = run_dir / "source_stage_v1" / "receipts"
        receipts.mkdir(parents=True)
        result = []
        for index, entry in enumerate(entries):
            source = self.paths[entry.path]
            self.assertEqual((source.stat().st_size, runner.sha256(source)), (entry.size_bytes, entry.sha256))
            callback = process(source, entry)
            path = receipts / f"{index:06d}.json"
            runner.write_json(path, {"entry": asdict(entry), "callback": callback})
            result.append(path)
        return tuple(result)

    def admit_fixture_runtime(self):
        self.stack.enter_context(patch.object(runner.production_guard, "require_production_host"))
        self.stack.enter_context(patch.object(runner, "preflight", return_value={"reservation_bytes": 40_000_000_000, **runner.CAPS}))
        self.stack.enter_context(patch.object(runner, "source_snapshot", return_value={"head": "a"*40, "files": {"fixture_source.py": "b"*64}}))
        self.stack.enter_context(patch.object(runner, "available_ram_bytes", return_value=240_000_000_000))
        self.stack.enter_context(patch.object(runner.shutil, "disk_usage", return_value=SimpleNamespace(free=100_000_000_000)))
        self.stack.enter_context(patch.object(staging, "stage_and_process", side_effect=self.stage_fixture))

    def args(self, command, output, **changes):
        values = {"command": command, "output_dir": output, "expected_head": "a"*40,
                  "admission_root": self.admission, "admission_plan": None}
        values.update(changes)
        return SimpleNamespace(**values)

    def test_complete_fixture_metadata_then_trades_publish_only_candidates(self):
        self.fixture_admission()
        self.admit_fixture_runtime()
        metadata_dir = self.root / "metadata-run"
        metadata = runner.run_production(self.args("metadata", metadata_dir))
        self.assertEqual(metadata["status"], "metadata_complete_pending_independent_audit")
        self.assertEqual(metadata["counts"]["source_rows"], 2)
        self.assertFalse((metadata_dir / "acceptance.json").exists())
        metadata_hash = runner.sha256(metadata_dir / "candidate_manifest.json")
        trade_dir = self.root / "trade-run"
        result = runner.run_production(self.args("trades", trade_dir, metadata_dir=metadata_dir,
                                       metadata_manifest_sha256=metadata_hash))
        self.assertEqual(result["status"], "inputs_complete_pending_independent_audit")
        self.assertEqual(result["counts"], {"source_rows": 6, "baseline_rows": 6, "duration_tail_rows": 4,
                                            "excluded_rows": 0, "source_days": 2})
        self.assertEqual(result["identity"]["distinct_native_trade_ids"], 6)
        self.assertEqual(result["input_cache"]["prepared_metadata_rows"], 1)
        self.assertTrue(result["input_cache"]["released_before_global_id_proof"])
        self.assertFalse((trade_dir / "acceptance.json").exists())
        self.assertFalse((trade_dir / "failure.json").exists())
        for record in result["artifacts"].values():
            self.assertTrue((trade_dir / record["path"]).is_file())
            self.assertTrue(record["path"].startswith("cells/final/"))
            self.assertEqual(runner.sha256(trade_dir / record["path"]), record["sha256"])
        self.assertTrue(all(path.exists() for path in self.paths.values()))

    def test_source_plan_pins_reference_hashes_and_mapping(self):
        plan = self.fixture_admission()
        admitted = runner.load_admission_plan(self.admission)
        self.assertEqual(len(admitted["metadata_entries"]), 1)
        self.assertEqual(admitted["metadata_counts"], {"markets_metadata/b1_0.parquet": 2})
        (self.admission / "events.parquet").write_bytes(b"changed")
        with self.assertRaisesRegex(runner.PipelineBlocked, "size/type"):
            runner.load_admission_plan(self.admission)
        self.assertEqual(plan["sports_winner_series"], runner.SPORTS_WINNER_SERIES)

    def test_winner_whitelist_does_not_add_a_native_category_filter(self):
        self.fixture_admission(event_category="Culture")
        self.admit_fixture_runtime()
        target = self.root / "winner-metadata"
        result = runner.run_production(self.args("metadata", target))
        item = result["artifacts"]["sports_b1_0"]
        rows = pq.read_table(target / item["path"]).to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["category"], rows[0]["sport"], rows[0]["series_ticker"]), ("Culture", "mlb", "KXMLBGAME"))

    def test_full_metadata_inventory_counts_are_required(self):
        self.fixture_admission()
        with patch.object(runner, "EXPECTED_METADATA_ROWS", 3):
            with self.assertRaisesRegex(runner.PipelineBlocked, "full pinned metadata inventory"):
                runner.load_admission_plan(self.admission)

    def test_structural_metadata_proof_cannot_be_replaced_by_claimed_acceptance(self):
        self.fixture_admission()
        self.admit_fixture_runtime()
        metadata_dir = self.root / "metadata"
        manifest = runner.run_production(self.args("metadata", metadata_dir))
        proof = metadata_dir / manifest["global_metadata_proof"]["path"]
        proof.write_text('{"status":"accepted"}')
        with self.assertRaisesRegex(runner.PipelineBlocked, "size/type|hash"):
            runner.admit_metadata_stage(metadata_dir, runner.sha256(metadata_dir / "candidate_manifest.json"),
                                         runner.load_admission_plan(self.admission))

    def test_exact_metadata_artifact_roles_cannot_be_omitted(self):
        self.fixture_admission()
        self.admit_fixture_runtime()
        target = self.root / "metadata"
        manifest = runner.run_production(self.args("metadata", target))
        manifest["artifacts"].pop("metadata_b1_0")
        (target / "candidate_manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(runner.PipelineBlocked, "artifact-role"):
            runner.admit_metadata_stage(target, runner.sha256(target / "candidate_manifest.json"),
                                         runner.load_admission_plan(self.admission))

    def test_failure_marker_preserves_owned_outputs_and_sources(self):
        self.fixture_admission()
        self.admit_fixture_runtime()
        output = self.root / "failed"
        with patch.object(runner, "build_metadata", side_effect=ValueError("fixture failure")):
            with self.assertRaisesRegex(ValueError, "fixture failure"):
                runner.run_production(self.args("metadata", output))
        marker = json.loads((output / "failure.json").read_text())
        self.assertEqual(marker["reason"], "fixture failure")
        self.assertFalse((output / "candidate_manifest.json").exists())
        self.assertTrue(all(path.exists() for path in self.paths.values()))

    def test_guard_precedes_preflight_manifest_or_body_io(self):
        with patch.object(runner.production_guard, "require_production_host", side_effect=RuntimeError("guard blocked")), \
             patch.object(runner, "preflight") as unopened:
            with self.assertRaisesRegex(RuntimeError, "guard blocked"):
                runner.run_production(self.args("metadata", self.root / "not-created"))
            unopened.assert_not_called()
        self.assertFalse((self.root / "not-created").exists())

    def test_preflight_capacity_ram_runtime_and_immutable_destination(self):
        with patch.object(runner, "RUN_ROOT", self.root), patch.object(runner.sys, "executable", "/home/ubuntu/venv/bin/python"), \
             patch.object(runner, "available_ram_bytes", return_value=240_000_000_000) as ram, \
             patch.object(runner.os, "cpu_count", return_value=4), \
             patch.object(runner.shutil, "disk_usage", return_value=SimpleNamespace(free=40_000_000_000)) as disk:
            result = runner.preflight(self.root / "new")
            self.assertEqual(result["reservation_bytes"], 40_000_000_000)
            disk.return_value.free -= 1
            with self.assertRaisesRegex(runner.PipelineBlocked, "reservation"):
                runner.preflight(self.root / "new")
            disk.return_value.free += 1
            ram.return_value -= 1
            with self.assertRaisesRegex(runner.PipelineBlocked, "240GB"):
                runner.preflight(self.root / "new")
            ram.return_value += 1
            (self.root / "new").mkdir()
            with self.assertRaisesRegex(runner.PipelineBlocked, "immutable"):
                runner.preflight(self.root / "new")

    def test_relative_paths_hashes_json_publication_and_no_overwrite(self):
        path = self.root / "manifest.json"
        record = runner.write_json(path, {"state": "pending"})
        self.assertEqual(runner.read_json(path, record["sha256"])[0], {"state": "pending"})
        with self.assertRaises(FileExistsError):
            runner.write_json(path, {"state": "accepted"})
        for value in ("../manifest.json", "/manifest.json", "a//b", "a\\b"):
            with self.subTest(value=value), self.assertRaises(runner.PipelineBlocked):
                runner._relative(self.root, value)
        path.write_text('{"x":1,"x":2}')
        with self.assertRaisesRegex(runner.PipelineBlocked, "duplicate JSON"):
            runner.read_json(path)

    def test_source_snapshot_includes_new_modules_tests_and_rejects_dirty_sources(self):
        repo = self.root / "repo"
        paths = ["production_guard.py", "analysis/kaushik_kalshi_replication/new_module.py",
                 "tests/test_kaushik_kalshi_new_module.py", "scripts/audit_kaushik_kalshi_fixture.py"]
        for relative in paths:
            path = repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# synthetic source\n")
        with patch.object(runner, "REPO", repo), patch.object(runner, "POLY_SOURCE", ("production_guard.py",)), \
             patch.object(runner.subprocess, "check_output", return_value="a"*40+"\n"), \
             patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as git:
            snapshot = runner.source_snapshot("a"*40)
            self.assertEqual(set(snapshot["files"]), set(paths))
            self.assertEqual(len(git.call_args_list), 2)
            git.return_value.returncode = 1
            with self.assertRaisesRegex(runner.PipelineBlocked, "tracked, committed"):
                runner.source_snapshot("a"*40)

    def test_persistent_output_ceiling_preserves_files(self):
        (self.root / "artifact").write_bytes(b"1234")
        with patch.object(runner, "CAPS", {**runner.CAPS, "output_bytes": 3}):
            with self.assertRaisesRegex(runner.PipelineBlocked, "persistent output ceiling"):
                runner._persistent_output_bytes(self.root)
        self.assertEqual((self.root / "artifact").read_bytes(), b"1234")

    def test_help_requires_no_third_party_packages(self):
        result = subprocess.run([sys.executable, "-S", str(Path(runner.__file__)), "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("metadata", result.stdout)
        self.assertIn("trades", result.stdout)


if __name__ == "__main__":
    unittest.main()
