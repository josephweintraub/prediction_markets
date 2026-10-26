"""Offline controller fixtures with fake children, no API or production host."""
from contextlib import ExitStack, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from scripts import run_polymarket_native_collection_campaign as campaign


HEAD = "c" * 40
SOURCE = {"status": "inspected", "head": HEAD, "tracked_worktree_clean": True,
          "tracked_dirty_entries": 0, "untracked_files_checked": False}


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def sleep(self, duration):
        self.value += duration


class FakeChild:
    def __init__(self, pid, *, outcome=0, delayed=0):
        self.pid, self.outcome, self.delayed = pid, outcome, delayed
        self.overflow, self.terminated, self.killed = False, False, False

    def pump(self):
        pass

    def poll(self):
        if self.terminated or self.killed:
            return -15
        if self.delayed:
            self.delayed -= 1
            return None
        return self.outcome

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True

    def receipt(self):
        return {"stderr": {"bytes": 10, "sha256": hashlib.sha256(b"SECRET_URL").hexdigest()},
                "stdout": {"bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()}}


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.output = self.root / "campaign"
        self.prior = self.root / "prior"
        self.prior.mkdir()
        (self.prior / "sessions/p1").mkdir(parents=True)
        prior_contract = {"schema_version": "polymarket_native_collection_contract_v1",
                          "raw_payload_only": True, "analysis_admitted": False}
        prior_sha = campaign.digest(campaign.json_bytes(prior_contract))
        (self.prior / "OWNER.json").write_bytes(campaign.json_bytes({"contract": prior_contract, "contract_sha256": prior_sha}))
        (self.prior / "sessions/p1/START.json").write_bytes(campaign.json_bytes({"contract_sha256": prior_sha}))
        (self.prior / "evidence.json").write_text('{"preserved":true}')
        self.plan_path = self.root / "source-plan.json"
        self.plan_path.write_text("source-plan-fixture")
        self.plan = {"provisional_append_start": {"start_block": 1000, "raw_footer_declared_maximum_block": 999},
                     "last_included_header": {"number": 30999}, "end_exclusive_utc": "2026-10-02T00:00:00Z",
                     "inventory_binding": {"path": str(self.root / "inventory.json")}}
        self.binding = {"path": str(self.plan_path), "sha256": "a" * 64, "source_head": "b" * 40,
                        "identity": {"fixture": True}}
        self.original = {"inputs": {}, "repair_manifest": None}
        self.clock = Clock()
        self.launched, self.outcomes, self.delays = [], [], []
        self.after_launch = None

    def launcher(self, argv):
        index = len(self.launched)
        raw = Path(argv[argv.index("--output-dir") + 1])
        raw.mkdir()
        (raw / "fixture.json").write_text('{"native_fixture":true}')
        child = FakeChild(10000 + index, outcome=self.outcomes[index] if index < len(self.outcomes) else 0,
                          delayed=self.delays[index] if index < len(self.delays) else 0)
        self.launched.append((child, argv))
        if self.after_launch:
            self.after_launch(index, child)
        return child

    def identity(self, pid):
        # Only newly launched fake PIDs are returned as live; returned child
        # processes are proven absent on the next invocation.
        for child, argv in self.launched:
            if child.pid == pid:
                return {"pid": pid, "start_ticks": pid * 10, "state": "R"}
        return None

    def usage(self, raw):
        if raw == self.prior:
            return {"rpc_calls": 2, "response_bytes": 100, "runtime_seconds": 0.1}
        return {"rpc_calls": 7, "response_bytes": 1000, "runtime_seconds": 0.2}

    def validate(self, raw, expected):
        return {"rows": expected["last_block"] - expected["first_block"] + 1,
                "manifest_sha256": "d" * 64, "usage": self.usage(raw),
                "first_block": expected["first_block"], "last_block": expected["last_block"]}

    def genuine_control_fixture(self):
        """Reviewed-child-shaped control evidence; payload hash QA, not decoding."""
        raw = self.root / "raw-control-fixture"
        raw.mkdir()
        chunk = campaign.chunks(1000, 1049)[0]
        expected = campaign.expected_child_contract(self.plan, self.binding, SOURCE, chunk)
        contract_sha = campaign.digest(campaign.json_bytes(expected))
        owner = {"run_id": "fixture", "contract": expected, "contract_sha256": contract_sha}
        (raw / "OWNER.json").write_bytes(campaign.json_bytes(owner))
        window = campaign.collector.windows(1000, 1049, 50)[0]
        attempt = raw / "attempts" / (window["window"] + ".a1")
        attempt.mkdir(parents=True)
        session = raw / "sessions/s1"
        (session / "rpc").mkdir(parents=True)
        (session / "START.json").write_bytes(campaign.json_bytes({"started_utc": campaign.now()}))
        (session / "RESULT.json").write_bytes(campaign.json_bytes({"session_elapsed_seconds": 0.2}))
        query = {"fromBlock": hex(1000), "toBlock": hex(1049), "address": list(campaign.pilot.ADDRESSES),
                 "topics": [expected["topics"]]}
        predicates = [("eth_chainId", []), ("eth_getBlockByNumber", [hex(30999), False]),
                      ("eth_getBlockByNumber", [hex(31000), False]),
                      ("eth_getBlockByNumber", [hex(1000), False]),
                      ("eth_getBlockByNumber", [hex(1049), False]), ("eth_getLogs", [query]),
                      ("eth_getBlockByNumber", [hex(1049), False])]
        ledger = []
        for identifier, (method, params) in enumerate(predicates, 1):
            start = {"id": identifier, "method": method, "params": params, "status": "requested"}
            finished = {**start, "status": "response_shape_validated", "response_bytes": 100, "response_sha256": "a" * 64}
            (session / "rpc" / f"r{identifier:09d}.START.json").write_bytes(campaign.json_bytes(start))
            (session / "rpc" / f"r{identifier:09d}.FINISH.json").write_bytes(campaign.json_bytes(finished))
            if identifier >= 4:
                ledger.append({**finished, "status": "validated"})
        payload = attempt / "payload.parquet"
        payload.write_bytes(b"synthetic fixture for reference integrity only")
        requests = attempt / "REQUESTS.json"
        requests.write_bytes(campaign.json_bytes({"calls": ledger}))
        def ref(path):
            return {"path": str(path.relative_to(raw)), "bytes": path.stat().st_size, "sha256": campaign.file_digest(path)}
        rows, evidence = campaign.pilot.validate_logs([], window, {}, 6)
        record = {"status": "raw_shard_validated", "contract_sha256": contract_sha, "window": window,
                  "attempt": 1, "first_header": {"number": 1000}, "last_header": {"number": 1049},
                  "log_request_id": 6, "payload": ref(payload), "request_ledger": ref(requests), "evidence": evidence,
                  "contract_intervals": campaign.collector.interval_receipts(window, evidence, campaign.pilot.ADDRESSES)}
        (attempt / "SUCCESS.json").write_bytes(campaign.json_bytes(record))
        manifest = {"schema_version": "polymarket_native_collection_manifest_v1", "status": "raw_collection_complete",
                    "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True,
                    "contract": expected, "contract_sha256": contract_sha, "run_id": "fixture", "windows": [record],
                    "rows": 0, "distinct_native_keys": 0}
        (raw / "manifest.json").write_bytes(campaign.json_bytes(manifest))
        marker = {"status": "raw_collection_complete_not_analysis_admission", "run_id": "fixture",
                  "contract_sha256": contract_sha, "manifest_sha256": campaign.file_digest(raw / "manifest.json")}
        (raw / "COMPLETE.json").write_bytes(campaign.json_bytes(marker))
        return raw, expected

    def execute(self, **changes):
        args = {"first_block": 1000, "last_block": 11049, "workers": 2, "prior_runs": [self.prior],
                "max_total_rpc_calls": 10000, "max_total_response_bytes": 100 * 1024**3,
                "max_runtime_seconds": 10000, "max_output_bytes": 20_000_000_000,
                "launcher": self.launcher, "clock": self.clock, "sleep": self.clock.sleep,
                "identity": self.identity}
        args.update(changes)
        with ExitStack() as stack:
            stack.enter_context(patch.object(campaign.pilot, "read_plan", return_value=(self.plan, self.binding, self.original)))
            stack.enter_context(patch.object(campaign.inventory, "code_state", return_value=SOURCE))
            stack.enter_context(patch.object(campaign.inventory, "resource_state", return_value={"ram_bytes": {"MemAvailable": 128 * 1024**3}}))
            stack.enter_context(patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)))
            stack.enter_context(patch.object(campaign, "child_usage", side_effect=self.usage))
            stack.enter_context(patch.object(campaign, "validate_child", side_effect=self.validate))
            return campaign.run_campaign(self.plan_path, "a" * 64, "b" * 40, HEAD, self.output, **args)

    def mark_returned_processes_absent(self):
        self.identity = lambda pid: None

    def test_full_literal_coverage_and_row_count_under_two_children(self):
        result = self.execute()
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(result["rows"], 10050)
        self.assertEqual(result["accepted_chunks"], 2)
        self.assertEqual(result["rpc_calls_charged"], 16)
        self.assertFalse(result["analysis_admitted"])
        self.assertEqual(len(self.launched), 2)
        marker = json.loads((self.output / "COMPLETE.json").read_text())
        manifest = self.output / marker["manifest_path"]
        self.assertEqual(marker["manifest_sha256"], campaign.file_digest(manifest))
        for child, argv in self.launched:
            self.assertIn("50", argv)
            self.assertIn("1200", argv)
            self.assertIn(str(campaign.CHILD_CAPS["max_output_bytes"]), argv)

    def test_child_failure_stops_new_launches_and_drains_already_running(self):
        self.outcomes = [1, 0]
        self.delays = [0, 4]
        result = self.execute(last_block=21049)
        self.assertEqual(result["status"], "failed_raw_campaign")
        self.assertEqual(len(self.launched), 2)
        self.assertFalse(self.launched[1][0].terminated)
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertTrue(list((self.output / "sessions").glob("*/FAILED.json")))

    def test_resource_reservation_prevents_launch_before_output_cap_exceeded(self):
        result = self.execute(max_output_bytes=campaign.CHILD_BYTES + campaign.PARENT_RESERVE)
        self.assertEqual(result["error_code"], "campaign_output_reservation_limit")
        self.assertEqual(len(self.launched), 0)

    def test_global_call_and_response_reservations_are_required_before_launch(self):
        for caps, expected in (({"max_total_rpc_calls": 1201}, "campaign_call_reservation_limit"),
                               ({"max_total_response_bytes": campaign.CHILD_CAPS["max_total_response_bytes"]},
                                "campaign_response_reservation_limit")):
            self.output = self.root / expected
            result = self.execute(**caps)
            self.assertEqual(result["error_code"], expected)
            self.assertEqual(self.launched, [])

    def test_failed_child_resume_uses_fresh_directory_and_retains_success_and_charges(self):
        self.outcomes = [1, 0]
        first = self.execute()
        self.assertEqual(first["accepted_chunks"], 1)
        old_failed = self.output / "chunks/c000001/a01/EXIT.json"
        old_bytes = old_failed.read_bytes()
        self.mark_returned_processes_absent()
        # New children still require a birth proof, but previous PIDs are absent.
        prior_pids = {child.pid for child, _ in self.launched}
        self.identity = lambda pid: None if pid in prior_pids else {"pid": pid, "start_ticks": pid * 10, "state": "R"}
        result = self.execute(resume=True)
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(len(self.launched), 3)
        self.assertTrue((self.output / "chunks/c000001/a02/EXIT.json").exists())
        self.assertEqual(old_failed.read_bytes(), old_bytes)
        self.assertEqual(result["rpc_calls_charged"], 23)

    def test_resume_refuses_a_live_prior_child_without_signalling_it(self):
        self.outcomes = [1, 0]
        self.execute()
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "prior_child_still_running")
        self.assertTrue(all(not child.terminated for child, _ in self.launched))
        self.assertEqual(len(self.launched), 2)

    def test_resume_returned_payload_hash_change_blocks_before_child_launch(self):
        self.outcomes = [1, 0]
        self.execute()
        self.mark_returned_processes_absent()
        path = self.output / "chunks/c000002/a01/raw/fixture.json"
        path.write_text("corrupt")
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "returned_child_inventory_changed")
        self.assertEqual(len(self.launched), 2)

    def test_returned_failed_usage_cannot_be_mutated_to_undercharge_resume(self):
        self.outcomes = [1, 0]
        self.execute()
        self.mark_returned_processes_absent()
        path = self.output / "chunks/c000001/a01/EXIT.json"
        receipt = json.loads(path.read_text())
        receipt["usage"]["rpc_calls"] = 0
        path.write_text(json.dumps(receipt))
        result = self.execute(resume=True)
        self.assertEqual(result["error_code"], "returned_child_usage_differs_from_durable_journals")
        self.assertEqual(len(self.launched), 2)
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_immutable_plan_and_completed_runs_are_not_overwritten(self):
        self.execute()
        original = (self.output / "PLAN.json").read_bytes()
        with self.assertRaises(FileExistsError):
            self.execute()
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "campaign_resume_plan_differs"):
            self.execute(resume=True, workers=1)
        self.assertEqual((self.output / "PLAN.json").read_bytes(), original)

    def test_prior_raw_inventory_change_prevents_complete(self):
        self.after_launch = lambda index, child: (self.prior / "evidence.json").write_text('{"changed":true}')
        result = self.execute()
        self.assertEqual(result["error_code"], "prior_raw_inventory_changed")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_expired_all_accepted_resume_without_previous_result_never_completes(self):
        self.execute()
        (self.output / "COMPLETE.json").unlink()
        old_session = next((self.output / "sessions").iterdir())
        (old_session / "RESULT.json").unlink()
        start_path = old_session / "START.json"
        started = json.loads(start_path.read_text())
        from datetime import datetime, timedelta, timezone
        started["started_utc"] = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        start_path.write_text(json.dumps(started))
        self.mark_returned_processes_absent()
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "campaign_runtime_limit"):
            self.execute(resume=True)
        self.assertFalse((self.output / "COMPLETE.json").exists())
        self.assertEqual(len(self.launched), 2)

    def test_expensive_final_inventory_hashing_cannot_escape_deadline(self):
        real_inventory = campaign.tree_inventory
        visits = 0
        def slow_final_inventory(root, **kwargs):
            nonlocal visits
            value = real_inventory(root, **kwargs)
            if root == self.prior:
                visits += 1
                if visits > 1:
                    self.clock.value += 20000
            return value
        with patch.object(campaign, "tree_inventory", side_effect=slow_final_inventory):
            result = self.execute()
        self.assertEqual(result["error_code"], "campaign_runtime_limit")
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_diagnostic_prior_has_explicit_conservative_usage_and_retained_bytes(self):
        diagnostic = self.root / "preflight"
        diagnostic.mkdir()
        (diagnostic / "probe.json").write_text('{"capacity_pilot":true}')
        result = self.execute(diagnostic_runs=[diagnostic], diagnostic_rpc_call_cap=10000,
                              diagnostic_response_byte_cap=17179869184, diagnostic_runtime_second_cap=21600,
                              max_total_rpc_calls=100000, max_total_response_bytes=100 * 1024**3,
                              max_runtime_seconds=30000)
        self.assertEqual(result["status"], "raw_campaign_complete")
        self.assertEqual(result["rpc_calls_charged"], 10016)
        self.assertEqual(result["response_bytes_charged"], 17179871284)
        frozen = json.loads((self.output / "PLAN.json").read_text())["plan"]
        prior = next(item for item in frozen["prior_raw_runs"] if item["path"] == str(diagnostic))
        self.assertEqual(prior["usage"]["runtime_seconds"], 21600)
        self.assertEqual(prior["inventory"]["bytes"], (diagnostic / "probe.json").stat().st_size)
        self.assertIn("not_observed_use", prior["usage_policy"])

    def test_unrecognized_prior_without_native_sessions_is_not_zero_charged(self):
        (self.prior / "sessions").rename(self.prior / "not-native-sessions")
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "native_prior_sessions_missing"):
            self.execute()
        self.assertFalse(self.launched)

    def test_empty_or_misidentified_native_sessions_cannot_get_zero_usage(self):
        (self.prior / "sessions/p1/START.json").unlink()
        (self.prior / "sessions/p1").rmdir()
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "native_prior_sessions_empty_or_contract_mismatch"):
            self.execute()
        self.assertFalse(self.launched)

    def test_native_prior_owner_contract_hash_is_verified_without_current_head_requirement(self):
        path = self.prior / "OWNER.json"
        owner = json.loads(path.read_text())
        owner["contract"]["source"] = {"head": "old_valid_producer"}
        path.write_text(json.dumps(owner))
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "native_prior_owner_contract_invalid"):
            self.execute()
        owner["contract_sha256"] = campaign.digest(campaign.json_bytes(owner["contract"]))
        path.write_bytes(campaign.json_bytes(owner))
        (self.prior / "sessions/p1/START.json").write_bytes(campaign.json_bytes({"contract_sha256": owner["contract_sha256"]}))
        result = self.execute()
        self.assertEqual(result["status"], "raw_campaign_complete")

    def test_timeout_terminates_only_owned_child_and_parent_exits_after_it(self):
        self.delays = [100000]
        def sleep(duration):
            self.clock.value += 1800
        result = self.execute(last_block=1049, workers=1, sleep=sleep)
        self.assertEqual(result["status"], "failed_raw_campaign")
        self.assertTrue(self.launched[0][0].terminated)
        self.assertFalse(result["children_still_running"])

    def test_logs_store_only_digests_not_child_plaintext_or_secret_errors(self):
        self.outcomes = [1]
        result = self.execute(last_block=1049, workers=1)
        for path in self.output.rglob("*.json"):
            self.assertNotIn("SECRET_URL", path.read_text())
        self.assertEqual(result["status"], "failed_raw_campaign")

    def test_literal_chunk_gaps_or_overlap_block_parent_completion(self):
        planned = campaign.chunks(1000, 11049)
        records = [{"chunk": item, "status": "accepted_raw_child",
                    "accepted": {"first_block": item["first_block"], "last_block": item["last_block"], "rows": 1}}
                   for item in planned]
        self.assertEqual(campaign.verify_parent_coverage(records, planned), 2)
        records[1]["accepted"]["first_block"] -= 1
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "campaign_range_gap_or_overlap"):
            campaign.verify_parent_coverage(records, planned)

    def test_real_acceptor_reopens_completion_range_request_predicates_and_hashes(self):
        raw, expected = self.genuine_control_fixture()
        checked = campaign.validate_child(raw, expected)
        self.assertEqual(checked["rows"], 0)
        self.assertEqual(checked["usage"]["rpc_calls"], 7)
        self.assertEqual(checked["first_block"], 1000)
        self.assertEqual(checked["last_block"], 1049)

    def test_real_acceptor_rejects_mutated_payload_or_wrong_source_head(self):
        raw, expected = self.genuine_control_fixture()
        payload = next((raw / "attempts").glob("*/payload.parquet"))
        payload.write_bytes(b"corrupt")
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "child_shard_hash_differs"):
            campaign.validate_child(raw, expected)
        changed = {**expected, "source": {**SOURCE, "head": "d" * 40}}
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "child_contract_differs"):
            campaign.validate_child(raw, changed)

    def test_usage_conservatively_charges_pending_requests_without_finishes(self):
        raw, expected = self.genuine_control_fixture()
        finish = raw / "sessions/s1/rpc/r000000007.FINISH.json"
        finish.unlink()
        checked = campaign.child_usage(raw)
        self.assertEqual(checked["rpc_calls"], 7)
        self.assertEqual(checked["response_bytes"], 600 + campaign.pilot.MAX_RESPONSE_BYTES + 1)

    def test_real_acceptor_rejects_wrong_literal_range_even_with_valid_markers(self):
        raw, expected = self.genuine_control_fixture()
        wrong = {**expected, "last_block": expected["last_block"] + 1}
        with self.assertRaisesRegex(campaign.pilot.PilotBlocked, "child_contract_differs"):
            campaign.validate_child(raw, wrong)

    def test_production_guard_precedes_source_or_process_use(self):
        argv = ["--source-plan", "/never/read", "--source-plan-sha256", "x", "--plan-source-head", "x",
                "--expected-head", "x", "--output-dir", "/never/write", "--first-block", "1", "--last-block", "2",
                "--workers", "2", "--max-total-rpc-calls", "10000", "--max-total-response-bytes", "100000000000",
                "--max-runtime-seconds", "10000", "--max-output-bytes", "128000000000", "--prior-raw-run", "/never/read/prior"]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(campaign.pilot, "read_plan") as read, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                campaign.main(argv)
        read.assert_not_called()


if __name__ == "__main__":
    unittest.main()
