"""Offline adaptive-campaign resource, priming and immutable-reuse fixtures."""
from contextlib import ExitStack, redirect_stderr
import copy
import io
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

import test_polymarket_native_collection_campaign as fixtures
import test_collect_polymarket_native_increment_v2 as collector_fixtures
from scripts import run_polymarket_native_collection_campaign_v2 as campaign


class AdaptiveCampaignTests(unittest.TestCase):
    def setUp(self):
        fixtures.CampaignTests.setUp(self)
        self.admission_root = self.root / "new-admission"
        self.admission_root.mkdir()
        self.old_binding = copy.deepcopy(self.binding)
        new_plan_path = self.admission_root / "source-plan.json"
        self.plan_path.rename(new_plan_path)
        self.plan_path = new_plan_path
        self.binding = {**self.binding, "path": str(self.plan_path)}
        self.bridge = self.admission_root / "COMPLETE.json"
        self.bridge.write_text("{}")
        self.proof = {"old_plan": copy.deepcopy(self.plan), "old_binding": self.old_binding,
                      "fresh_plan": self.plan, "fresh_binding": self.binding,
                      "bridge_binding": {"path": str(self.bridge), "sha256": "f" * 64},
                      "witness_binding": {"path": str(self.admission_root / "equivalence.json"), "sha256": "e" * 64},
                      "admission_tree_inventory": {"path": str(self.admission_root), "bytes": 1000, "files": []},
                      "prep_charge": {**campaign.PREP_CHARGE, "output_bytes": 1000}}
        self.grid = [{"chunk": f"c{index:06d}", "first_block": 1000 + (index - 1) * 50,
                      "last_block": 1049 + (index - 1) * 50} for index in range(1, 5)]
        self.reused = {"c000001", "c000003"}
        accepted = {}
        for chunk in self.grid:
            if chunk["chunk"] in self.reused:
                accepted[chunk["chunk"]] = {"chunk": chunk, "status": "accepted_raw_child",
                    "accepted": {"first_block": chunk["first_block"], "last_block": chunk["last_block"], "rows": 50,
                                 "manifest_sha256": "d" * 64, "usage": {"rpc_calls": 7, "response_bytes": 1000, "runtime_seconds": 0.2}},
                    "external_attempt": "/old/" + chunk["chunk"], "producer_head": campaign.OLD_HEAD}
        self.baseline = {"campaign": str(self.prior), "accepted": accepted, "bytes": 1000, "rpc_calls": 45663,
                         "response_bytes": 76324722328, "runtime_seconds": 66000.0,
                         "root_controls": {}, "prior_inventories": [], "attempt_inventories": []}
        self.at_launch_accepted = []
        self.validate_count = 0
        self.expected_contracts = []
        self.reuse_reads = []
        self.bridge_reads = []

    def launcher(self, argv):
        self.at_launch_accepted.append(self.validate_count)
        return fixtures.CampaignTests.launcher(self, argv)

    def identity(self, pid):
        return fixtures.CampaignTests.identity(self, pid)

    def usage(self, raw):
        return {"rpc_calls": 7, "response_bytes": 1000, "runtime_seconds": 0.2}

    def validate(self, raw, expected):
        self.validate_count += 1
        self.expected_contracts.append(copy.deepcopy(expected))
        return {"first_block": expected["first_block"], "last_block": expected["last_block"],
                "rows": 50, "manifest_sha256": "d" * 64, "usage": self.usage(raw), "split_nodes": 1}

    def read_reuse(self, *args):
        self.reuse_reads.append(copy.deepcopy(args[:5]))
        return copy.deepcopy(self.baseline)

    def read_bridge(self, *args):
        self.bridge_reads.append(args)
        return copy.deepcopy(self.proof)

    def execute(self, **changes):
        kwargs = {"reuse_campaign": self.prior, "reuse_plan_file_sha256": campaign.OLD_PLAN_FILE_SHA,
                  "reuse_source_head": campaign.OLD_HEAD, "workers": 4, "launcher": self.launcher,
                  "reboot_bridge": self.bridge, "reboot_bridge_sha256": "f" * 64,
                  "clock": self.clock, "sleep": self.clock.sleep, "identity": self.identity}
        kwargs.update(changes)
        with ExitStack() as stack:
            stack.enter_context(patch.object(campaign, "FIRST", 1000))
            stack.enter_context(patch.object(campaign, "LAST", 1199))
            stack.enter_context(patch.object(campaign, "REUSED", self.reused))
            stack.enter_context(patch.object(campaign.legacy, "chunks", return_value=self.grid))
            stack.enter_context(patch.object(campaign, "load_reuse", side_effect=self.read_reuse))
            stack.enter_context(patch.object(campaign, "validate_reboot_bridge", side_effect=self.read_bridge))
            stack.enter_context(patch.object(campaign.pilot, "read_plan", return_value=(self.plan, self.binding, self.original)))
            stack.enter_context(patch.object(campaign.inventory, "code_state", return_value=fixtures.SOURCE))
            stack.enter_context(patch.object(campaign.inventory, "resource_state", return_value={"ram_bytes": {"MemAvailable": 128 * 1024**3}}))
            stack.enter_context(patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)))
            stack.enter_context(patch.object(campaign.legacy, "child_usage", side_effect=self.usage))
            stack.enter_context(patch.object(campaign, "validate_child", side_effect=self.validate))
            return campaign.run_campaign(self.plan_path, "a" * 64, "b" * 40, fixtures.HEAD, self.output, **kwargs)

    def test_noncontiguous_imports_remain_external_and_failed_gap_is_fresh(self):
        result = self.execute()
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(result["rows"], 200)
        self.assertEqual(len(self.launched), 2)
        fresh_first = [int(argv[argv.index("--first-block") + 1]) for child, argv in self.launched]
        self.assertEqual(fresh_first, [1050, 1150])
        self.assertFalse((self.output / "chunks/c000001").exists())
        self.assertFalse((self.output / "chunks/c000003").exists())
        self.assertEqual(result["rpc_calls_charged"], 45777)
        self.assertEqual(result["response_bytes_charged"], 76593159784)

    def test_bridge_freezes_old_and_fresh_bindings_and_charges_new_tree_once(self):
        result = self.execute()
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(len(self.bridge_reads), 2)
        self.assertTrue(all(args == (self.bridge, "f" * 64, self.plan_path, "a" * 64, "b" * 40)
                            for args in self.bridge_reads))
        self.assertTrue(all(args[3:5] == (self.proof["old_plan"], self.old_binding) for args in self.reuse_reads))
        self.assertTrue(all(value["source_plan"] == self.binding for value in self.expected_contracts))
        self.assertTrue(all(argv[argv.index("--source-plan") + 1] == str(self.plan_path)
                            for child, argv in self.launched))
        owner = campaign.load_json(self.output / "PLAN.json")
        self.assertEqual(owner["plan"]["reboot_admission"], self.proof)
        self.assertEqual(owner["plan"]["reuse"]["rpc_calls"], 45663)
        self.assertEqual(owner["plan"]["reuse"]["response_bytes"], 76324722328)
        self.assertEqual(result["aggregate_bytes"], 2000 + campaign.legacy.tree_bytes(self.output)
                         - next((self.output / "sessions").glob("*/RESULT.json")).stat().st_size)

    def test_fresh_binding_mismatch_blocks_before_old_reads_or_new_output(self):
        self.proof["fresh_binding"] = {**self.binding, "sha256": "0" * 64}
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_fresh_plan_binding_differs"):
            self.execute()
        self.assertFalse(self.reuse_reads)
        self.assertFalse(self.launched)
        self.assertFalse(self.output.exists())

    def test_unverified_bridge_blocks_before_import_or_new_output(self):
        def refuse(*args):
            raise campaign.pilot.PilotBlocked("fixture_bridge_hash_differs")
        self.read_bridge = refuse
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "fixture_bridge_hash_differs"):
            self.execute()
        self.assertFalse(self.reuse_reads)
        self.assertFalse(self.output.exists())

    def test_preparation_charge_cannot_be_reduced_or_detached_from_tree_bytes(self):
        saved = copy.deepcopy(self.proof)
        for field in (*campaign.PREP_CHARGE, "output_bytes"):
            with self.subTest(field=field):
                self.proof = copy.deepcopy(saved)
                self.proof["prep_charge"][field] -= 1
                with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_preparation_charge_differs"):
                    self.execute()
        self.assertFalse(self.launched)
        self.assertFalse(self.output.exists())

    def test_preparation_ceilings_require_integer_not_equal_float_or_boolean(self):
        saved = copy.deepcopy(self.proof)
        for field in campaign.PREP_CHARGE:
            for value in (float(campaign.PREP_CHARGE[field]), True):
                with self.subTest(field=field, value=value):
                    self.proof = copy.deepcopy(saved)
                    self.proof["prep_charge"][field] = value
                    with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_preparation_charge_differs"):
                        self.execute()
        self.assertFalse(self.output.exists())

    def test_preparation_runtime_counts_inside_existing_parent_deadline(self):
        self.baseline["runtime_seconds"] = campaign.CAPS["runtime_seconds"] - campaign.PREP_CHARGE["runtime_seconds"]
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_runtime_limit"):
            self.execute()
        self.assertFalse(self.launched)

    def test_preparation_tree_bytes_count_inside_existing_global_ceiling(self):
        self.baseline["bytes"] = campaign.CAPS["output_bytes"] - campaign.legacy.PARENT_RESERVE - 999
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_initial_control_budget"):
            self.execute()
        self.assertFalse(self.output.exists())

    def test_preparation_response_charge_is_reserved_before_any_child_launch(self):
        self.baseline["response_bytes"] = campaign.CAPS["response_bytes"] - 4 * 1024**3 - 268435456 + 1
        result = self.execute()
        self.assertEqual(result["error_code"], "campaign_response_reservation_limit")
        self.assertFalse(self.launched)

    def test_admission_tree_cannot_overlap_old_new_or_prior_roots(self):
        for path in (self.prior, self.output, self.root):
            with self.subTest(path=str(path)):
                self.proof["admission_tree_inventory"]["path"] = str(path)
                with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_admission_tree_overlaps_retained_roots"):
                    self.execute()
        self.proof["admission_tree_inventory"]["path"] = str(self.admission_root)
        self.baseline["prior_inventories"] = [{"path": str(self.admission_root)}]
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_admission_tree_overlaps_retained_roots"):
            self.execute()
        self.assertFalse(self.launched)

    def test_bridge_and_fresh_source_controls_must_be_inside_charged_tree(self):
        outside = self.root / "outside-admission"
        self.proof["admission_tree_inventory"]["path"] = str(outside)
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_reboot_admission_controls_outside_charged_tree"):
            self.execute()
        self.assertFalse(self.launched)

    def test_changed_bridge_or_admission_tree_blocks_final_publication(self):
        old_validate = self.validate
        def mutate(raw, expected):
            result = old_validate(raw, expected)
            self.proof["witness_binding"]["sha256"] = "0" * 64
            return result
        self.validate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "adaptive_reboot_bridge_or_admission_tree_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_changed_bridge_on_resume_is_not_a_new_preparation_budget(self):
        self.outcomes = [1]
        self.execute()
        self.proof["witness_binding"]["sha256"] = "0" * 64
        self.identity = lambda pid: None
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_resume_plan_differs"):
            self.execute(resume=True)
        self.assertEqual(len(self.launched), 1)

    def test_final_bridge_hashing_cannot_publish_after_deadline(self):
        old_read = self.read_bridge
        def slow(*args):
            value = old_read(*args)
            if len(self.bridge_reads) > 1:
                self.clock.value += campaign.CAPS["runtime_seconds"]
            return value
        self.read_bridge = slow
        result = self.execute()
        self.assertEqual(result["error_code"], "adaptive_campaign_runtime_limit")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_first_fresh_chunk_is_accepted_before_any_parallel_launch(self):
        self.delays = [4, 0]
        result = self.execute()
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(self.at_launch_accepted, [0, 1])
        self.assertTrue((self.output / "PRIMED.json").exists())

    def test_prime_failure_stops_without_launching_other_pending_ranges(self):
        self.outcomes = [1]
        result = self.execute()
        self.assertEqual(result["status"], "failed_raw_campaign")
        self.assertEqual(len(self.launched), 1)
        self.assertEqual(result["accepted_chunks"], 2)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_failed_prime_is_retained_and_resume_uses_fresh_attempt_and_charges_once(self):
        self.outcomes = [1, 0, 0]
        self.execute()
        original = (self.output / "chunks/c000002/a01/EXIT.json").read_bytes()
        old_pids = {child.pid for child, argv in self.launched}
        self.identity = lambda pid: None if pid in old_pids else {"pid": pid, "start_ticks": pid * 10, "state": "R"}
        result = self.execute(resume=True)
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(result["rpc_calls_charged"], 45784)
        self.assertEqual(result["response_bytes_charged"], 76324722328 + 268435456 + 3 * 1000)
        self.assertEqual((self.output / "chunks/c000002/a01/EXIT.json").read_bytes(), original)
        self.assertTrue((self.output / "chunks/c000002/a02/EXIT.json").exists())

    def test_edited_failed_usage_cannot_undercharge_resumed_campaign(self):
        self.outcomes = [1]
        self.execute()
        receipt = self.output / "chunks/c000002/a01/EXIT.json"
        value = json.loads(receipt.read_text())
        value["usage"]["rpc_calls"] = 0
        receipt.write_text(json.dumps(value))
        self.identity = lambda pid: None
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "adaptive_campaign_returned_usage_changed")
        self.assertEqual(len(self.launched), 1)

    def test_final_reuse_revalidation_change_blocks_complete(self):
        count = 0
        original = copy.deepcopy(self.baseline)
        def changed(*args, **kwargs):
            nonlocal count
            count += 1
            result = copy.deepcopy(original)
            if count > 1:
                result["bytes"] += 1
            return result
        # execute provides its own load_reuse patch; a validate callback can
        # mutate the frozen external-baseline fixture between its two reads.
        old_validate = self.validate
        def mutate(raw, expected):
            result = old_validate(raw, expected)
            self.baseline["bytes"] += 1
            return result
        self.validate = mutate
        result = self.execute()
        self.assertEqual(result["error_code"], "adaptive_campaign_reuse_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_global_caps_include_baseline_instead_of_resetting_on_new_source(self):
        self.baseline["rpc_calls"] = campaign.CAPS["rpc_calls"] - 1199
        result = self.execute()
        self.assertEqual(result["error_code"], "campaign_call_reservation_limit")
        self.assertFalse(self.launched)

    def test_slow_reservation_cannot_launch_child_after_parent_deadline(self):
        normal = campaign.legacy.reservations
        def slow(**kwargs):
            normal(**kwargs)
            self.clock.value += campaign.CAPS["runtime_seconds"]
        with patch.object(campaign.legacy, "reservations", side_effect=slow):
            result = self.execute()
        self.assertEqual(result["error_code"], "adaptive_campaign_runtime_limit")
        self.assertFalse(self.launched)

    def test_unknown_returned_status_is_not_a_silent_failed_attempt(self):
        self.outcomes = [1]
        self.execute()
        path = self.output / "chunks/c000002/a01/EXIT.json"
        ended = campaign.load_json(path)
        ended["status"] = "unrecognized"
        path.write_bytes(campaign.json_bytes(ended))
        self.identity = lambda pid: None
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "adaptive_campaign_returned_binding_differs")
        self.assertEqual(len(self.launched), 1)

    def test_changed_earlier_new_payload_is_reopened_before_parent_complete(self):
        def change_on_second_launch(index, child):
            if index == 1:
                path = self.output / "chunks/c000002/a01/raw/fixture.json"
                path.write_text("changed after acceptance")
        self.after_launch = change_on_second_launch
        result = self.execute()
        self.assertEqual(result["error_code"], "adaptive_campaign_final_child_artifact_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_runtime_includes_old_wall_session_not_parallel_child_sum(self):
        self.baseline["runtime_seconds"] = campaign.CAPS["runtime_seconds"]
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_runtime_limit"):
            self.execute()
        self.assertFalse(self.launched)

    def test_negative_or_boolean_restored_session_duration_is_not_budget_credit(self):
        self.outcomes = [1]
        self.execute()
        path = next((self.output / "sessions").glob("*/RESULT.json"))
        original = json.loads(path.read_text())
        self.identity = lambda pid: None
        for bad in (-1000, True):
            with self.subTest(duration=bad):
                value = copy.deepcopy(original)
                value["session_elapsed_seconds"] = bad
                path.write_text(json.dumps(value))
                with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_session_result_invalid"):
                    self.execute(resume=True)
        self.assertEqual(len(self.launched), 1)

    def test_expired_all_accepted_resume_does_not_skip_runtime_gate(self):
        self.execute()
        (self.output / "COMPLETE.json").unlink()
        session = next((self.output / "sessions").iterdir())
        (session / "RESULT.json").unlink()
        path = session / "START.json"
        value = json.loads(path.read_text())
        from datetime import datetime, timedelta, timezone
        value["started_utc"] = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        path.write_text(json.dumps(value))
        self.identity = lambda pid: None
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "adaptive_campaign_runtime_limit"):
            self.execute(resume=True)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_file_versions_are_unchanged(self):
        self.assertEqual(campaign.legacy.file_digest(Path(campaign.v1.__file__)), campaign.adaptive.V1_SHA256)
        self.assertEqual(campaign.legacy.file_digest(Path(campaign.legacy.__file__)), campaign.OLD_CONTROLLER_SHA)

    def test_cli_requires_explicit_bridge_and_guards_before_any_source_read(self):
        argv = ["--source-plan", str(self.plan_path), "--source-plan-sha256", "a" * 64,
                "--plan-source-head", "b" * 40, "--expected-head", fixtures.HEAD,
                "--output-dir", str(self.output), "--reuse-campaign", str(self.prior),
                "--reuse-plan-file-sha256", campaign.OLD_PLAN_FILE_SHA, "--reuse-source-head", campaign.OLD_HEAD]
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            campaign.main(argv)
        argv += ["--reboot-bridge", str(self.bridge), "--reboot-bridge-sha256", "f" * 64]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("not production")), \
             patch.object(campaign.pilot, "read_plan") as read_plan, \
             patch.object(campaign, "validate_reboot_bridge") as bridge, \
             redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            campaign.main(argv)
        read_plan.assert_not_called()
        bridge.assert_not_called()

    def test_hierarchical_control_inventory_excludes_large_child_tree(self):
        root = self.root / "hierarchical"
        (root / "chunks/c1/a01").mkdir(parents=True)
        (root / "PLAN.json").write_text("control")
        for index in range(10001):
            (root / "chunks/c1/a01" / str(index)).touch()
        controls = campaign.root_controls(root)
        self.assertEqual(controls["bytes"], 7)
        self.assertEqual(len(controls["files"]), 1)
        compact = campaign.compact_inventory(root, controls)
        self.assertEqual(compact["bytes"], 7)
        self.assertEqual(compact["files"], 1)
        self.assertNotIn("PLAN.json", campaign.json_bytes(compact).decode())

    def test_sealed_old_result_pins_wall_runtime_not_parallel_child_sum(self):
        session = self.root / "old-session"
        session.mkdir()
        result = {"accepted_chunks": 19, "aggregate_bytes": 1828839120, "analysis_admitted": False,
                  "children_still_running": False, "collection_complete": False,
                  "error_code": "child_nonzero_timeout_or_log_limit", "plan_sha256": campaign.OLD_PLAN_SHA,
                  "response_bytes_charged": 76324722328, "rpc_calls_charged": 45663,
                  "run_id": "939b6d26-5470-43bc-882d-37a0af73679e", "session_elapsed_seconds": 1307.7926040820003,
                  "status": "failed_raw_campaign"}
        (session / "START.json").write_bytes(campaign.json_bytes({"plan_sha256": campaign.OLD_PLAN_SHA}))
        (session / "RESULT.json").write_bytes(campaign.json_bytes(result))
        self.assertEqual(campaign.legacy.file_digest(session / "RESULT.json"), campaign.OLD_RESULT_SHA)
        checked = campaign.sealed_parent_result(session, result["run_id"])
        self.assertEqual(checked["session_elapsed_seconds"], 1307.7926040820003)
        result["session_elapsed_seconds"] = 0
        (session / "RESULT.json").write_bytes(campaign.json_bytes(result))
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "reuse_sealed_parent_result_hash_differs"):
            campaign.sealed_parent_result(session, result["run_id"])

    def genuine_child(self):
        fixture = collector_fixtures.AdaptiveCollectorTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        result = fixture.execute()
        self.assertEqual(result["status"], "raw_collection_complete")
        raw = fixture.output
        return raw, json.loads((raw / "OWNER.json").read_text())["contract"]

    def test_real_adaptive_child_validator_reopens_split_and_leaf_artifacts(self):
        raw, expected = self.genuine_child()
        checked = campaign.validate_child(raw, expected)
        self.assertEqual(checked["rows"], 10)
        self.assertEqual(checked["split_nodes"], 1)

    def test_wrong_marker_or_manifest_status_run_or_raw_flag_cannot_pass(self):
        raw, expected = self.genuine_child()
        marker_path, manifest_path = raw / "COMPLETE.json", raw / "manifest.json"
        original_marker = json.loads(marker_path.read_text())
        original_manifest = json.loads(manifest_path.read_text())
        cases = [("marker", "status", "failed"), ("manifest", "status", "failed"),
                 ("manifest", "run_id", "different"), ("manifest", "raw_payload_only", False),
                 ("manifest", "rows", True)]
        for target, field, value in cases:
            with self.subTest(target=target, field=field):
                marker, manifest = copy.deepcopy(original_marker), copy.deepcopy(original_manifest)
                if target == "marker":
                    marker[field] = value
                else:
                    manifest[field] = value
                manifest_path.write_bytes(campaign.json_bytes(manifest))
                marker["manifest_sha256"] = campaign.legacy.file_digest(manifest_path)
                marker_path.write_bytes(campaign.json_bytes(marker))
                with self.assertRaises(campaign.pilot.PilotBlocked):
                    campaign.validate_child(raw, expected)

    def reuse_fixture(self):
        """Twenty tiny real attempt trees; mock only old body/usage validators."""
        root = self.root / "old-campaign"
        for name in ("chunks", "sessions/s1", "manifests"):
            (root / name).mkdir(parents=True, exist_ok=True)
        (root / "LOCK").touch()
        usage_by_path, certificates, priors = {}, {}, []
        native_usage = [(7, 12204156, 2.34), (10, 33087903, 4.36), (83, 159982356, 31.78)]
        for index in range(6):
            path = self.root / f"old-prior-{index}"
            path.mkdir()
            (path / "metadata.json").write_text('{"fixture":true}')
            if index < 3:
                calls, responses, seconds = native_usage[index]
                usage = {"rpc_calls": calls, "response_bytes": responses, "runtime_seconds": seconds}
                policy = "native_collector_durable_request_and_session_receipts"
                usage_by_path[str(path)] = usage
            else:
                usage = {"rpc_calls": 10000, "response_bytes": 17179869184, "runtime_seconds": 21600}
                policy = "explicit_conservative_diagnostic_charge_not_observed_use"
            priors.append({"path": str(path), "inventory": campaign.legacy.tree_inventory(path, cap=campaign.CAPS["output_bytes"]),
                           "usage": usage, "usage_policy": policy})
        source = {**fixtures.SOURCE, "head": campaign.OLD_HEAD}
        frozen = {"source": source, "collector_sha256": campaign.adaptive.V1_SHA256, "source_plan": self.binding,
                  "limits": campaign.CAPS, "chunks": campaign.legacy.chunks(campaign.FIRST, campaign.LAST), "prior_raw_runs": priors}
        plan_sha = campaign.digest(campaign.json_bytes(frozen))
        run_id = "fixture-old-parent"
        (root / "PLAN.json").write_bytes(campaign.json_bytes({"run_id": run_id, "plan_sha256": plan_sha, "plan": frozen}))
        file_sha = campaign.legacy.file_digest(root / "PLAN.json")
        accepted_index = 0
        for chunk in frozen["chunks"][:20]:
            attempt = root / "chunks" / chunk["chunk"] / "a01"
            raw = attempt / "raw"
            raw.mkdir(parents=True)
            expected = campaign.legacy.expected_child_contract(self.plan, self.binding, source, chunk)
            (raw / "OWNER.json").write_bytes(campaign.json_bytes({"contract": expected,
                               "contract_sha256": campaign.digest(campaign.json_bytes(expected))}))
            (raw / "payload.bin").write_bytes(chunk["chunk"].encode())
            if chunk["chunk"] in campaign.REUSED:
                responses = 2485197106 if accepted_index == 0 else 1200000000
                usage = {"rpc_calls": 803, "response_bytes": responses, "runtime_seconds": 200.0}
                checked = {"first_block": chunk["first_block"], "last_block": chunk["last_block"], "rows": 1,
                           "manifest_sha256": "d" * 64, "usage": usage}
                certificates[str(raw)] = checked
                status = "accepted_raw_child"
                accepted_index += 1
                (raw / "COMPLETE.json").write_text('{"fixture":true}')
            else:
                usage = {"rpc_calls": 306, "response_bytes": 494643255, "runtime_seconds": 97.80666999099958}
                checked, status = None, "failed_raw_child"
            usage_by_path[str(raw)] = usage
            (attempt / "START.json").write_bytes(campaign.json_bytes({"plan_sha256": plan_sha, "chunk": chunk}))
            (attempt / "PROCESS.json").write_bytes(campaign.json_bytes({"pid": 20000 + accepted_index, "start_ticks": 1}))
            files = campaign.legacy.tree_inventory(raw, cap=campaign.legacy.CHILD_CAPS["max_output_bytes"])
            (attempt / "INVENTORY.json").write_bytes(campaign.json_bytes(files))
            ended = {"plan_sha256": plan_sha, "chunk": chunk, "status": status, "accepted": checked,
                     "usage": usage, "raw_inventory_sha256": campaign.digest(campaign.json_bytes(files))}
            (attempt / "EXIT.json").write_bytes(campaign.json_bytes(ended))
        result = {"plan_sha256": plan_sha, "run_id": run_id, "status": "failed_raw_campaign", "children_still_running": False,
                  "accepted_chunks": 19, "rpc_calls_charged": 45663, "response_bytes_charged": 76324722328,
                  "session_elapsed_seconds": 1307.7926040820003}
        (root / "sessions/s1/START.json").write_bytes(campaign.json_bytes({"plan_sha256": plan_sha}))
        (root / "sessions/s1/RESULT.json").write_bytes(campaign.json_bytes(result))
        result_sha = campaign.legacy.file_digest(root / "sessions/s1/RESULT.json")
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(campaign, "OLD_PLAN_FILE_SHA", file_sha))
        stack.enter_context(patch.object(campaign, "OLD_PLAN_SHA", plan_sha))
        stack.enter_context(patch.object(campaign, "OLD_RESULT_SHA", result_sha))
        stack.enter_context(patch.object(campaign.legacy, "child_usage", side_effect=lambda path: copy.deepcopy(usage_by_path[str(path)])))
        def validate_old(raw, expected):
            owner = campaign.load_json(raw / "OWNER.json")
            campaign.require(owner.get("contract") == expected
                             and owner.get("contract_sha256") == campaign.digest(campaign.json_bytes(expected)),
                             "fixture_old_producer_changed")
            return copy.deepcopy(certificates[str(raw)])
        stack.enter_context(patch.object(campaign.legacy, "validate_child", side_effect=validate_old))
        def load():
            return campaign.load_reuse(root, file_sha, campaign.OLD_HEAD, self.plan, self.binding, identity=lambda pid: None)
        return root, load

    def test_real_hierarchical_importer_reconstructs_exact_once_only_totals_and_noncontiguous_keys(self):
        root, load = self.reuse_fixture()
        checked = load()
        self.assertEqual(checked["rpc_calls"], 45663)
        self.assertEqual(checked["response_bytes"], 76324722328)
        self.assertEqual(set(checked["accepted"]), campaign.REUSED)
        self.assertNotIn("c000018", checked["accepted"])
        self.assertEqual(len(checked["attempt_inventories"]), 20)
        self.assertEqual(len(checked["prior_inventories"]), 6)
        self.assertAlmostEqual(checked["runtime_seconds"], 64800 + 2.34 + 4.36 + 31.78 + 1307.7926040820003)
        self.assertEqual(checked["bytes"], campaign.legacy.tree_bytes(root) +
                         sum(campaign.legacy.tree_bytes(Path(item["path"])) for item in checked["prior_inventories"]))

    def test_real_importer_rejects_old_payload_or_inventory_or_parent_runtime_mutation(self):
        root, load = self.reuse_fixture()
        paths = [root / "chunks/c000001/a01/raw/payload.bin", root / "chunks/c000001/a01/INVENTORY.json",
                 root / "sessions/s1/RESULT.json"]
        for path in paths:
            with self.subTest(path=str(path.relative_to(root))):
                saved = path.read_bytes()
                path.write_bytes(saved + b" ")
                with self.assertRaises(campaign.pilot.PilotBlocked):
                    load()
                path.write_bytes(saved)

    def test_real_importer_rejects_old_chunks_ancestor_symlink(self):
        root, load = self.reuse_fixture()
        (root / "chunks").rename(root / "actual-chunks")
        (root / "chunks").symlink_to(root / "actual-chunks", target_is_directory=True)
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "reuse_root_directory_topology_invalid"):
            load()

    def test_real_importer_refuses_changed_old_producer_even_with_updated_local_inventory(self):
        root, load = self.reuse_fixture()
        attempt = root / "chunks/c000001/a01"
        raw = attempt / "raw"
        owner_path = raw / "OWNER.json"
        owner = campaign.load_json(owner_path)
        owner["contract"]["source"]["head"] = "e" * 40
        owner["contract_sha256"] = campaign.digest(campaign.json_bytes(owner["contract"]))
        owner_path.write_bytes(campaign.json_bytes(owner))
        files = campaign.legacy.tree_inventory(raw, cap=campaign.legacy.CHILD_CAPS["max_output_bytes"])
        (attempt / "INVENTORY.json").write_bytes(campaign.json_bytes(files))
        ended = campaign.load_json(attempt / "EXIT.json")
        ended["raw_inventory_sha256"] = campaign.digest(campaign.json_bytes(files))
        (attempt / "EXIT.json").write_bytes(campaign.json_bytes(ended))
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "fixture_old_producer_changed"):
            load()

    def test_real_importer_does_not_trust_changed_native_prior_usage(self):
        root, load = self.reuse_fixture()
        original = campaign.legacy.child_usage.side_effect
        def changed(path):
            value = original(path)
            if str(path).endswith("old-prior-0"):
                value["rpc_calls"] = 0
            return value
        campaign.legacy.child_usage.side_effect = changed
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "reuse_native_prior_usage_changed"):
            load()


if __name__ == "__main__":
    unittest.main()
