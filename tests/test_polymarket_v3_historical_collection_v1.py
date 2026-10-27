"""Small synthetic parent/child admission fixtures. No EC2, network or real data."""
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import unittest
from unittest.mock import patch

from scripts import run_polymarket_v3_historical_collection_v1 as wrapper
from tests import test_collect_polymarket_native_increment as legacy
code, header, COLLECTOR_HEAD, PLAN_HEAD = legacy.code, legacy.header, legacy.COLLECTOR_HEAD, legacy.PLAN_HEAD


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.case = legacy.CollectorTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.root = self.case.root
        self.profile = {**wrapper.PROFILES["probe"], "first_block": 900, "last_block": 999}
        self.full = {**wrapper.PROFILES["full"], "first_block": 850, "last_block": 999}
        self.source = {"head": COLLECTOR_HEAD, "files": {}, "code_state": code(COLLECTOR_HEAD)}
        self.sparse_path = self.root / "sparse.json"
        self.admission_path = self.root / "admission.json"
        self.output = self.root / "wrapper"
        self.source_changes = False
        self.child_changes = None
        self.exit_override = None
        self.loop_error = None
        self.real_constructor = False
        self.created = self.drained = 0
        binding = {"path": str(self.case.plan_path), "sha256": self.case.plan_digest,
                   "source_head": PLAN_HEAD, "identity": wrapper.inventory.path_identity(self.case.plan_path)}
        self.sparse = {"schema_version": "polymarket_v3_sparse_history_probe_v2", "status": "bounded_v3_sparse_history_complete",
            "address": wrapper.ADDRESS, "chain_id": 137, "zero_prefix_verified": True,
            "conservative_event_start_supported": True, "requested_upgraded_event_ledger_complete": True,
            "final_input_identity_unchanged": True, "reopened_headers_unchanged": True,
            "safe_event_collection_start_block": 850, "source_plan_binding": binding,
            "headers": [header(849), header(850), *[self.case.plan[name] for name in
                        ("raw_footer_tail_header", "last_included_header", "first_excluded_header")]],
            "zero_prefix_ranges": [], "upgrade_ranges": [], "request_ledger": [], "upgrades": []}
        upgrade = {"block_number": wrapper.shared.prior.UPGRADE_BLOCK,
                   "transaction_hash": wrapper.shared.prior.UPGRADE_TRANSACTION, "transaction_index": 62,
                   "log_index": wrapper.shared.prior.UPGRADE_LOG_INDEX,
                   "new_implementation": wrapper.shared.prior.NEW_IMPLEMENTATION}
        self.sparse["upgrades"] = [upgrade]
        self.sparse["upgrade_block_proofs"] = [{"in_block_upgrades": [upgrade],
            "before_block_slot": {"implementation_address": wrapper.shared.prior.OLD_IMPLEMENTATION},
            "after_block_slot": {"implementation_address": wrapper.shared.prior.NEW_IMPLEMENTATION},
            "whole_block_implementation_assignment_admitted": False}]
        self.sparse["receipt_proofs"] = [{"receipt_status": 1, "transaction_hash": upgrade["transaction_hash"],
                                          "upgrade_log_indices": [wrapper.shared.prior.UPGRADE_LOG_INDEX]}]
        for kind, end, topic, count in (("zero_prefix", 849, None, 0), ("upgraded", 2999, wrapper.shared.UPGRADED_TOPIC, 1)):
            identifier = len(self.sparse["request_ledger"]) + 1
            query = {"address": wrapper.ADDRESS, "fromBlock": "0x0", "toBlock": hex(end)}
            if topic:
                query["topics"] = [topic]
            sha = hashlib.sha256(kind.encode()).hexdigest()
            self.sparse["request_ledger"].append({"id": identifier, "method": "eth_getLogs", "params": [query],
                "status": "range_validated", "returned_log_count": count, "response_sha256": sha})
            self.sparse["zero_prefix_ranges" if not topic else "upgrade_ranges"].append({
                "first_block": 0, "last_block": end, "request_id": identifier, "log_count": count, "response_sha256": sha})
        self.sparse["rpc_calls_attempted"] = 2
        self.sparse_sha = self.write(self.sparse_path, self.sparse)
        campaign = self.root / "v2"
        (campaign / "manifests").mkdir(parents=True)
        (campaign / "sessions/s1").mkdir(parents=True)
        frozen = {"source_plan": {"sha256": self.case.plan_digest}, "reuse": {"runtime_seconds": 20_000},
                  "reboot_admission": {"prep_charge": {"runtime_seconds": 1200}}}
        semantic = wrapper.digest(wrapper.child.json_bytes(frozen))
        owner = {"plan": frozen, "plan_sha256": semantic, "run_id": "fixture-v2"}
        manifest = {"schema_version": "polymarket_native_campaign_manifest_v2", "status": "raw_campaign_complete",
            "collection_complete": True, "analysis_admitted": False, "raw_payload_only": True,
            "run_id": owner["run_id"], "plan_sha256": semantic, "rpc_calls_charged": 497_667,
            "response_bytes_charged": 571_146_511_990}
        mb = self.ref(campaign / "manifests/m.json", manifest)
        marker = {"manifest_path": "manifests/m.json", "manifest_sha256": mb["sha256"],
                  "status": "raw_campaign_complete_not_analysis_admission", "plan_sha256": semantic, "run_id": owner["run_id"]}
        ended = {"run_id": owner["run_id"], "plan_sha256": semantic, "status": "raw_campaign_complete",
                 "children_still_running": False, "analysis_admitted": False, "collection_complete": True,
                 "rpc_calls_charged": 497_667, "response_bytes_charged": 571_146_511_990,
                 "session_elapsed_seconds": 26_453, "aggregate_bytes": 39_161_003_314}
        self.admission = {"schema_version": "polymarket_v3_historical_global_admission_v1",
            "status": "root_global_budget_admitted", "all_prior_roots_and_diagnostics_included": True,
            "prior_retained_bytes": 39_170_000_000, "prior_rpc_calls": 498_000,
            "prior_response_bytes": 572_000_000_000, "prior_runtime_seconds": 50_000,
            "v2_manifest": mb, "v2_complete": self.ref(campaign / "COMPLETE.json", marker),
            "v2_plan": self.ref(campaign / "PLAN.json", owner),
            "v2_session_results": [self.ref(campaign / "sessions/s1/RESULT.json", ended)]}
        self.admission_sha = self.write(self.admission_path, self.admission)

    def write(self, path, value):
        raw = wrapper.encode(value)
        path.write_bytes(raw)
        return wrapper.digest(raw)

    def ref(self, path, value):
        return {"path": str(path), "sha256": self.write(path, value)}

    def stack(self):
        stack = ExitStack()
        stack.enter_context(patch.object(wrapper, "PROFILES", {"probe": self.profile, "full": self.full}))
        stack.enter_context(patch.object(wrapper, "PLAN_SHA", self.case.plan_digest))
        stack.enter_context(patch.object(wrapper, "PLAN_HEAD", PLAN_HEAD))
        stack.enter_context(patch.object(wrapper, "SPARSE_SHA", self.sparse_sha))
        stack.enter_context(patch.object(wrapper, "FIRST", 850))
        stack.enter_context(patch.object(wrapper, "CUTOFF", 2999))
        stack.enter_context(patch.object(wrapper.shared, "RUN_ROOT", self.root))
        stack.enter_context(patch.object(wrapper.shared.prior, "validate_plan_blocks"))
        snapshots = 0
        def source_snapshot(_):
            nonlocal snapshots
            snapshots += 1
            return {**self.source, "head": "d" * 40} if self.source_changes and snapshots > 1 else self.source
        stack.enter_context(patch.object(wrapper, "source_snapshot", side_effect=source_snapshot))
        stack.enter_context(patch.object(wrapper.resource, "setrlimit"))
        stack.enter_context(patch.object(wrapper.inventory, "resource_state", return_value={
            "ram_bytes": {"MemAvailable": 16 * 1024**3}, "disk_bytes": {"free": 10**12}}))
        stack.enter_context(patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)))
        outer = self
        class FixtureChild:
            def __init__(self, command):
                outer.created += 1
                arguments = {command[index][2:]: command[index + 1] for index in range(2, len(command), 2)}
                outer.case.output = Path(arguments["output-dir"])
                opts = {name.replace("-", "_"): int(arguments[name]) for name in (
                    "first-block", "last-block", "window-blocks", "max-rpc-calls", "max-output-bytes",
                    "max-runtime-seconds", "max-rows", "max-ram-bytes", "max-total-response-bytes")}
                ended = outer.case.execute(mode="v3-historical", **opts)
                self.stdout = bytearray(json.dumps(ended).encode())
                self.stderr_bytes, self.stderr_hash, self.overflow = 0, hashlib.sha256(), False
                self.code = 0 if ended["status"] == "raw_collection_complete" else 2
                if outer.child_changes:
                    outer.child_changes(outer.case.output, ended)
                if outer.exit_override is not None:
                    self.code = outer.exit_override
                self.command_sha256 = "unset"
            def proof(self):
                return {"boot_id": "fixture-boot", "birth": {"pid": 123, "pgid": 123, "session": 123,
                        "start_ticks": 456}, "command_sha256": self.command_sha256}
            def poll(self): return None if outer.loop_error else self.code
            def pump(self):
                if outer.loop_error:
                    error, outer.loop_error = outer.loop_error, None
                    raise error
            def drain(self): outer.drained += 1
        if not self.real_constructor:
            stack.enter_context(patch.object(wrapper, "OwnedChild", FixtureChild))
        return stack

    def execute(self, **changes):
        args = dict(profile_name="probe", plan_path=self.case.plan_path, plan_sha=self.case.plan_digest,
            plan_head=PLAN_HEAD, expected_head=COLLECTOR_HEAD, sparse_path=self.sparse_path,
            sparse_sha=self.sparse_sha, admission_path=self.admission_path, admission_sha=self.admission_sha,
            output=self.output)
        args.update(changes)
        with self.stack():
            return wrapper._run(**args)

    def marker(self):
        return self.output / "controls/COMPLETE.json"

    def test_probe_accepts_one_fresh_child_and_seals_native_proof(self):
        result = self.execute()
        self.assertEqual(result["status"], wrapper.SUCCESS)
        self.assertEqual(result["accepted_child"]["rows"], 1)
        self.assertEqual(self.created, 1)
        self.assertGreater(self.drained, 0)
        marker = json.loads(self.marker().read_text())
        self.assertEqual(marker["result_sha256"], wrapper.digest((self.output / "controls/RESULT.json").read_bytes()))
        self.assertEqual(result["usage_charged"]["runtime_seconds"], 1200)
        self.assertFalse(result["analysis_admitted"])

    def test_existing_output_never_adopted_or_resumed(self):
        self.output.mkdir()
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "output_not_fresh"):
            self.execute()
        self.assertEqual(self.created, 0)

    def test_nonzero_child_with_valid_fake_complete_is_rejected(self):
        self.exit_override = 2
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertTrue((self.output / "raw/COMPLETE.json").exists())
        self.assertFalse(self.marker().exists())

    def test_success_stdout_but_failed_durable_result_is_rejected(self):
        def mutate(raw, _):
            path = next((raw / "sessions").iterdir()) / "RESULT.json"
            value = json.loads(path.read_text()); value["status"] = "failed_raw_collection"
            self.write(path, value)
        self.child_changes = mutate
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_same_length_owner_uuid_mutation_is_rejected(self):
        def mutate(raw, _):
            path = raw / "OWNER.json"; data = path.read_bytes(); value = json.loads(data)
            old = value["run_id"]; new = ("0" if old[0] != "0" else "1") + old[1:]
            path.write_bytes(data.replace(old.encode(), new.encode(), 1))
        self.child_changes = mutate
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_child_complete_directory_fsync_repro_is_rejected(self):
        original = wrapper.child.publish
        real_fsync = wrapper.child.os.fsync
        def changed(path, raw):
            if path.name != "COMPLETE.json": return original(path, raw)
            count = 0
            def fsync(fd):
                nonlocal count
                count += 1
                if count == 2: raise OSError("fixture directory fsync")
                return real_fsync(fd)
            with patch.object(wrapper.child.os, "fsync", side_effect=fsync): return original(path, raw)
        with patch.object(wrapper.child, "publish", side_effect=changed):
            result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertTrue((self.output / "raw/COMPLETE.json").exists())
        self.assertFalse(self.marker().exists())

    def test_plan_tail_header_drift_repro_is_rejected(self):
        def mutate(payload, reply):
            if payload["method"] == "eth_getBlockByNumber" and payload["params"][0] == hex(999):
                reply["result"]["hash"] = "0x" + "f" * 64
            return reply
        self.case.rpc.mutate = mutate
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_missing_success_request_payload_and_gap_fail_closed(self):
        for target in ("SUCCESS.json", "REQUESTS.json", "payload.parquet"):
            with self.subTest(target=target):
                self.output = self.root / target.replace(".", "-")
                self.child_changes = lambda raw, _, target=target: next((raw / "attempts").glob("*/" + target)).unlink()
                self.assertEqual(self.execute()["status"], "blocked")
                self.assertFalse(self.marker().exists())

    def test_same_length_durable_rpc_mutation_fails_reconciliation(self):
        def mutate(raw, _):
            path = next((raw / "sessions").glob("*/rpc/r000000006.FINISH.json"))
            value = path.read_bytes(); changed = value.replace(b'"response_sha256": "', b'"response_sha256": "0', 1)
            changed = changed.replace(b'"response_sha256": "0' + json.loads(value)["response_sha256"].encode(),
                                      b'"response_sha256": "' + b'f' * 64)
            self.assertEqual(len(value), len(changed)); path.write_bytes(changed)
        self.child_changes = mutate
        self.assertEqual(self.execute()["status"], "blocked")

    def test_zero_probe_cannot_admit_full(self):
        self.case.rpc.empty = True
        result = self.execute()
        self.assertEqual(result.get("error_code"), "probe_has_no_positive_native_rows")
        self.assertFalse(self.marker().exists())

    def test_real_transport_retry_is_charged_and_reconciled(self):
        failed = False
        def mutate(payload, reply):
            nonlocal failed
            if payload["method"] == "eth_getLogs" and not failed:
                failed = True
                raise OSError("fixture unknown response")
            return reply
        self.case.rpc.mutate = mutate
        result = self.execute()
        self.assertEqual(result["status"], wrapper.SUCCESS)
        self.assertEqual(result["accepted_child"]["usage"]["rpc_calls"], 10)
        self.assertGreater(result["accepted_child"]["usage"]["response_bytes"], wrapper.pilot.MAX_RESPONSE_BYTES)

    def test_interruption_drains_owned_child_and_never_admits_child_complete(self):
        self.loop_error = KeyboardInterrupt("fixture interruption")
        result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["children_still_running"])
        self.assertGreater(self.drained, 0)
        self.assertFalse(self.marker().exists())

    def test_source_snapshot_change_after_child_blocks_completion(self):
        self.source_changes = True
        self.assertEqual(self.execute()["status"], "blocked")
        self.assertFalse(self.marker().exists())

    def test_plan_and_sparse_input_mutations_after_child_block_completion(self):
        for path in (self.case.plan_path, self.sparse_path):
            with self.subTest(path=path.name):
                self.output = self.root / ("changed-" + path.name)
                original = path.read_bytes()
                self.child_changes = lambda _, ended, path=path: path.write_bytes(path.read_bytes() + b" ")
                self.assertEqual(self.execute()["status"], "blocked")
                self.assertFalse(self.marker().exists())
                path.write_bytes(original)
                self.sparse["source_plan_binding"]["identity"] = wrapper.inventory.path_identity(self.case.plan_path)
                self.sparse_sha = self.write(self.sparse_path, self.sparse)

    def test_missing_rpc_finish_and_changed_response_counter_block_completion(self):
        for mutation in ("missing", "counter"):
            self.output = self.root / mutation
            def mutate(raw, _, mutation=mutation):
                if mutation == "missing":
                    next((raw / "sessions").glob("*/rpc/r000000006.FINISH.json")).unlink()
                else:
                    path = next((raw / "sessions").iterdir()) / "RESULT.json"
                    value = json.loads(path.read_text()); value["response_bytes_charged"] += 1
                    self.write(path, value)
            self.child_changes = mutate
            self.assertEqual(self.execute()["status"], "blocked")
            self.assertFalse(self.marker().exists())

    def test_symlink_and_unexpected_artifact_block_completion(self):
        for mutation in ("symlink", "extra"):
            self.output = self.root / mutation
            def mutate(raw, _, mutation=mutation):
                path = next((raw / "attempts").glob("*/payload.parquet"))
                if mutation == "symlink":
                    saved = self.root / "saved.parquet"; saved.write_bytes(path.read_bytes())
                    path.unlink(); path.symlink_to(saved)
                else:
                    (raw / "unexpected.json").write_bytes(b"{}")
            self.child_changes = mutate
            self.assertEqual(self.execute()["status"], "blocked")
            self.assertFalse(self.marker().exists())

    def test_full_requires_positive_parent_probe_and_inclusive_prior_charge(self):
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "positive_probe_required"):
            self.execute(profile_name="full")
        self.assertEqual(self.created, 0)
        probe = self.execute()
        marker_sha = wrapper.digest(self.marker().read_bytes())
        probe_dir = self.output
        self.output = self.root / "full"
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "omits_accepted_probe"):
            self.execute(profile_name="full", accepted_probe_dir=probe_dir, accepted_probe_sha=marker_sha)
        for key in wrapper.GLOBAL_CAPS:
            self.admission["prior_" + key] += probe["usage_charged"][key]
        self.admission_sha = self.write(self.admission_path, self.admission)
        result = self.execute(profile_name="full", accepted_probe_dir=probe_dir, accepted_probe_sha=marker_sha)
        self.assertEqual(result["status"], wrapper.SUCCESS)
        self.assertEqual(result["accepted_child"]["windows"], 2)

    def test_sparse_literal_gap_or_nonzero_or_wrong_predicate_blocks_before_child(self):
        for mutate in (lambda v: v["zero_prefix_ranges"][0].update(first_block=1),
                       lambda v: v["zero_prefix_ranges"][0].update(log_count=1),
                       lambda v: v["request_ledger"][0]["params"][0].update(topics=[]),
                       lambda v: v.update(requested_upgraded_event_ledger_complete=False)):
            original = json.loads(json.dumps(self.sparse)); mutate(original)
            self.sparse_sha = self.write(self.sparse_path, original)
            with self.assertRaises(wrapper.shared.HistoryBlocked): self.execute()
            self.assertEqual(self.created, 0)

    def test_bad_input_sha_and_global_understatement_block_before_child(self):
        self.admission["prior_rpc_calls"] = 1
        self.admission_sha = self.write(self.admission_path, self.admission)
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "global_prior"):
            self.execute()
        self.assertEqual(self.created, 0)

    def test_global_reservation_cannot_exceed_frozen_capacity(self):
        self.admission["prior_retained_bytes"] = wrapper.GLOBAL_CAPS["retained_bytes"]
        self.admission_sha = self.write(self.admission_path, self.admission)
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "global_prior"):
            self.execute()

    def test_all_four_global_priors_are_required_numeric_and_above_minima(self):
        for key in wrapper.GLOBAL_CAPS:
            prior = self.admission["prior_" + key]
            for invalid in (True, -1, 0):
                with self.subTest(key=key, invalid=invalid):
                    self.admission["prior_" + key] = invalid
                    self.admission_sha = self.write(self.admission_path, self.admission)
                    with self.assertRaises(wrapper.shared.HistoryBlocked): self.execute()
            self.admission["prior_" + key] = prior

    def test_omitted_v2_session_and_changed_bound_manifest_block_before_child(self):
        self.admission["v2_session_results"] = []
        self.admission_sha = self.write(self.admission_path, self.admission)
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "session_references"):
            self.execute()
        self.assertEqual(self.created, 0)

    def test_global_plan_file_hash_is_distinct_from_semantic_plan_hash(self):
        self.admission["v2_plan"]["sha256"] = json.loads(Path(self.admission["v2_plan"]["path"]).read_text())["plan_sha256"]
        self.admission_sha = self.write(self.admission_path, self.admission)
        with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "input_hash"):
            self.execute()

    def test_profile_source_blob_and_guard_contracts(self):
        self.assertEqual(wrapper.PROFILES["probe"]["first_block"], 88977538)
        self.assertEqual(len(wrapper.child.windows(87479447, 88978537, 100)), 14991)
        command = wrapper.command_for(Path("/fixture/plan"), COLLECTOR_HEAD, Path("/fixture/raw"), wrapper.PROFILES["full"], Path("/private/endpoint"))
        self.assertNotIn("--resume", command)
        self.assertEqual(command[command.index("--max-output-bytes") + 1], "7900000000")
        with patch.object(wrapper, "require_production_host", side_effect=RuntimeError("fixture")), \
                patch.object(wrapper, "read_bound") as read:
            with self.assertRaises(RuntimeError): wrapper.run(profile_name="probe", plan_path=Path("/never"),
                plan_sha="x", plan_head="x", expected_head="x", sparse_path=Path("/never"), sparse_sha="x",
                admission_path=Path("/never"), admission_sha="x", output=Path("/never"))
            read.assert_not_called()

    def test_source_snapshot_requires_exact_committed_blob(self):
        with patch.object(wrapper.inventory, "code_state", return_value=code(COLLECTOR_HEAD)), \
                patch.object(wrapper.subprocess, "run", return_value=type("Git", (), {"returncode": 0, "stdout": b"not committed bytes"})()):
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "source_blob"):
                wrapper.source_snapshot(COLLECTOR_HEAD)

    def test_parent_same_length_control_change_blocks_complete(self):
        original = wrapper.ParentControls.journal
        def mutate(controls, value):
            returned = original(controls, value)
            if value["event"] == "child_independently_accepted":
                path = controls.root / "OWNER.json"; raw = path.read_bytes()
                path.write_bytes(raw.replace(b'"profile":"probe"', b'"profile":"pr0be"', 1))
            return returned
        with patch.object(wrapper.ParentControls, "journal", side_effect=mutate, autospec=True):
            with self.assertRaises(wrapper.shared.HistoryBlocked): self.execute()
        self.assertFalse(self.marker().exists())

    def test_parent_completion_file_fsync_failure_leaves_only_charged_pending(self):
        original = wrapper.ParentControls.write
        def fail(controls, name, value, **kwargs):
            if name == "COMPLETE.json":
                with patch.object(wrapper.shared.os, "fsync", side_effect=OSError("fixture marker fsync")):
                    return original(controls, name, value, **kwargs)
            return original(controls, name, value, **kwargs)
        with patch.object(wrapper.ParentControls, "write", autospec=True, side_effect=fail):
            with self.assertRaises(OSError): self.execute()
        self.assertFalse(self.marker().exists())
        self.assertTrue((self.output / "controls/.COMPLETE.pending").exists())

    def test_parent_marker_directory_fsync_failure_leaves_no_complete(self):
        original = wrapper.ParentControls.write
        real_fsync = wrapper.shared.os.fsync
        def fail(controls, name, value, **kwargs):
            def fsync(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode): raise OSError("fixture directory fsync")
                return real_fsync(fd)
            if name == "COMPLETE.json":
                with patch.object(wrapper.shared.os, "fsync", side_effect=fsync):
                    return original(controls, name, value, **kwargs)
            return original(controls, name, value, **kwargs)
        with patch.object(wrapper.ParentControls, "write", autospec=True, side_effect=fail):
            with self.assertRaises(OSError): self.execute()
        self.assertFalse(self.marker().exists())
        self.assertTrue((self.output / "controls/.COMPLETE.pending").exists())

    def test_parent_marker_atomic_competitor_is_never_overwritten(self):
        original = wrapper.shared.atomic_completion
        def compete(temporary, destination):
            destination.write_bytes(b"competing marker")
            return original(temporary, destination)
        with patch.object(wrapper.shared, "atomic_completion", side_effect=compete):
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "atomic_completion_refused"):
                self.execute()
        self.assertEqual(self.marker().read_bytes(), b"competing marker")

    def test_parent_control_reservation_and_free_floor_are_enforced(self):
        with patch.object(wrapper.shared, "RUN_ROOT", self.root), \
                patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)):
            controls = wrapper.ParentControls(self.root / "controlled", {"fixture": True})
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "control_byte_cap"):
                controls.write("RESULT.json", b"x" * wrapper.CONTROL_CAP, final=True)
            with patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, wrapper.FREE_FLOOR)):
                with self.assertRaises(wrapper.shared.HistoryBlocked): controls.journal({"event": "fixture"})

    def test_runtime_installs_ceiling_and_restores_original_state(self):
        original = (wrapper.resource.RLIM_INFINITY, wrapper.resource.RLIM_INFINITY)
        with patch.object(wrapper.resource, "getrlimit", return_value=original), \
                patch.object(wrapper.resource, "setrlimit") as limits, \
                patch.object(wrapper.signal, "getsignal", return_value="old"), \
                patch.object(wrapper.signal, "getitimer", return_value=(0.0, 0.0)), \
                patch.object(wrapper.signal, "signal"), patch.object(wrapper.signal, "setitimer") as alarm:
            with wrapper.runtime_limits(1200): pass
        self.assertEqual(limits.call_args_list[0].args[1], (wrapper.PARENT_LIVE_RAM, original[1]))
        self.assertEqual(limits.call_args_list[-1].args[1], original)
        self.assertEqual(alarm.call_args_list[0].args[1], 1200)
        self.assertEqual(alarm.call_args_list[-1].args[1], 0)

    def process_stack(self, *, birth=True, pipe_error=False, reused=False, lingering=False, popen_error=False):
        """Use the REAL constructor/drainer, replacing only OS process fixtures."""
        stack = ExitStack()
        self.real_constructor = True
        self.process_events, self.live = [], {"leader": True, "descendant": lingering}
        outer = self
        class Pipe:
            def __init__(self, fd): self.fd, self.closed = fd, False
            def fileno(self): return self.fd
            def close(self): self.closed = True
        class Process:
            pid = 123
            def __init__(self): self.stdout, self.stderr, self.returncode = Pipe(10), Pipe(11), None
            def poll(self):
                if not outer.live["leader"]: self.returncode = -15
                return self.returncode
            def wait(self, timeout=None):
                outer.process_events.append(("wait", timeout))
                outer.live["leader"] = False
                self.returncode = -15
                return self.returncode
            def terminate(self): outer.process_events.append(("unbound_terminate",)); outer.live["leader"] = False
            def kill(self): outer.process_events.append(("unbound_kill",)); outer.live["leader"] = False
        self.process = Process()
        popen = stack.enter_context(patch.object(wrapper.subprocess, "Popen", side_effect=OSError("fixture Popen") if popen_error else None,
                                                return_value=self.process))
        self.popen_mock = popen
        original_text, original_iter = Path.read_text, Path.iterdir
        def read_text(path, *args, **kwargs):
            if str(path) == "/proc/sys/kernel/random/boot_id": return "12345678-1234-1234-1234-123456789012"
            return original_text(path, *args, **kwargs)
        def iterdir(path):
            if str(path) == "/proc": return iter([Path("/proc/123"), Path("/proc/456")])
            return original_iter(path)
        captures = 0
        def identity(pid):
            nonlocal captures
            if pid == 123:
                captures += 1
                if not birth or not outer.live["leader"]: return None
                return {"pid": 123, "start_ticks": 999 if reused and captures > 1 else 456,
                        "state": "S", "pgid": 123, "session": 123}
            if pid == 456 and outer.live["descendant"]:
                return {"pid": 456, "start_ticks": 457, "state": "S", "pgid": 123, "session": 123}
            return None
        def killpg(pgid, sig):
            outer.process_events.append(("killpg", pgid, sig))
            self.assertEqual(pgid, 123)
            outer.live["leader"] = False
            if sig == wrapper.signal.SIGKILL or not lingering: outer.live["descendant"] = False
        ticks = [0.0]
        stack.enter_context(patch.object(Path, "read_text", read_text))
        stack.enter_context(patch.object(Path, "iterdir", iterdir))
        stack.enter_context(patch.object(wrapper, "process_identity", side_effect=identity))
        stack.enter_context(patch.object(wrapper.os, "set_blocking", side_effect=OSError("fixture pipe setup") if pipe_error else None))
        stack.enter_context(patch.object(wrapper.os, "read", return_value=b""))
        stack.enter_context(patch.object(wrapper.os, "killpg", side_effect=killpg))
        stack.enter_context(patch.object(wrapper.time, "monotonic", side_effect=lambda: ticks[0]))
        stack.enter_context(patch.object(wrapper.time, "sleep", side_effect=lambda seconds: ticks.__setitem__(0, ticks[0] + seconds)))
        return stack

    def test_real_constructor_failed_birth_preserves_process_unknown_and_no_terminal_false(self):
        with self.process_stack(birth=False, lingering=True):
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "owned_child_liveness_unknown"):
                self.execute()
        receipt = json.loads((self.output / "controls/PROCESS.json").read_text())
        self.assertEqual(receipt["pid"], 123)
        self.assertIsNone(receipt["birth"])
        self.assertFalse(receipt["ownership_proven"])
        self.assertEqual(receipt["liveness"], "unknown")
        self.assertTrue(self.live["descendant"])
        self.assertEqual(self.process_events, [])
        self.assertFalse((self.output / "controls/RESULT.json").exists())
        self.assertFalse(self.marker().exists())

    def test_real_constructor_pipe_failure_drains_lingering_descendant_before_terminal(self):
        with self.process_stack(pipe_error=True, lingering=True):
            result = self.execute()
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["children_still_running"])
        self.assertFalse(self.live["leader"] or self.live["descendant"])
        signals = [item[2] for item in self.process_events if item[0] == "killpg"]
        self.assertEqual(signals, [wrapper.signal.SIGTERM, wrapper.signal.SIGKILL])
        receipt = json.loads((self.output / "controls/CHILD_EXIT.json").read_text())
        self.assertTrue(receipt["owned_group_drained"])
        self.assertEqual(receipt["process"]["liveness"], "proven_drained")
        self.assertFalse(self.marker().exists())

    def test_real_constructor_identity_reuse_never_signals_or_claims_drained(self):
        with self.process_stack(pipe_error=True, reused=True, lingering=True):
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "child_pid_reused"):
                self.execute()
        self.assertTrue(self.live["descendant"])
        self.assertFalse(any(item[0] == "killpg" for item in self.process_events))
        self.assertFalse((self.output / "controls/RESULT.json").exists())
        self.assertFalse(self.marker().exists())

    def test_real_constructor_popen_failure_is_unknown_not_false_idle(self):
        with self.process_stack(popen_error=True):
            with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "owned_child_liveness_unknown"):
                self.execute()
        receipt = json.loads((self.output / "controls/PROCESS.json").read_text())
        self.assertIsNone(receipt["pid"])
        self.assertEqual(receipt["liveness"], "unknown")
        self.assertFalse((self.output / "controls/RESULT.json").exists())
        self.assertEqual(self.process_events, [])

    def test_real_constructor_source_failure_launches_no_process(self):
        with self.process_stack():
            with self.stack():
                with patch.object(wrapper, "source_snapshot", side_effect=wrapper.shared.HistoryBlocked("fixture_source_changed")):
                    with self.assertRaisesRegex(wrapper.shared.HistoryBlocked, "fixture_source_changed"):
                        wrapper._run(profile_name="probe", plan_path=self.case.plan_path, plan_sha=self.case.plan_digest,
                            plan_head=PLAN_HEAD, expected_head=COLLECTOR_HEAD, sparse_path=self.sparse_path,
                            sparse_sha=self.sparse_sha, admission_path=self.admission_path,
                            admission_sha=self.admission_sha, output=self.output)
            self.popen_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
