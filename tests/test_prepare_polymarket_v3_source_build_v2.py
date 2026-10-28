"""Synthetic root admissions and fake compiler only; never production inputs."""
from contextlib import ExitStack
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import prepare_polymarket_v3_source_build_v2 as stage
from tests import test_polymarket_v3_source_build_v1 as fixtures

HEAD = "a" * 40
SOURCE = {"head": HEAD, "files": {"fixture": {"bytes": 7, "sha256": stage.digest(b"fixture")}}}
NOW = datetime(2026, 10, 28, 20, 0, tzinfo=timezone.utc).timestamp()
BOOT = "41823275-3e51-4d7f-9080-0c50aa148763"
BIRTH = {"pid": 123, "start_ticks": 456, "pgid": 123, "session": 123,
    "boot_id": "12345678-1234-1234-1234-123456789abc"}


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = value if type(value) is bytes else stage.encode(value)
    path.write_bytes(raw)
    return {"path": str(path), "bytes": len(raw), "sha256": stage.digest(raw)}


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.SourceBuildTests(); self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root; self.output = self.root / "execution"
        self.acq = self.root / "acquisition"; self.acq.mkdir()
        self.binary = self.acq / "attempt01.part"
        self.binary.write_bytes(self.fixture.binary.read_bytes()); self.binary.chmod(0o600)
        self.original = self.binary.read_bytes(); self.calls = []; self.ticks = 0
        self.op = self.root / "op"; self.op.mkdir()
        self.data = self.root / "data"; self.data.mkdir()
        self.oldqa = self.root / "oldqa"; self.oldqa.mkdir()
        self.runtime = self.fixture.runtime_path
        self.accepted = self.root / "terminal"; self.accepted.mkdir()
        source_raw = b"fixture"
        bindings = {name: put(self.root / "metadata_evidence" / (name + ".json"), {"fixture": name}) for name in
            ("manifest", "complete", "producer_start", "launch", "caller_exit", "admission", "root_drain")}
        bindings["root_drain"] = put(self.accepted / "root_drain.json", {"fixture": "root_drain"})
        prior = copy.deepcopy(stage.MINIMUM)
        prior["runtime_seconds"] = 70000
        terminal = {"schema_version": "polymarket_metadata_v6_terminal_acceptance_v1",
            "status": "root_reopened_completed_raw_pagination_only", "metadata_run_dir": str(stage.METADATA_RUN),
            "producer_head": stage.METADATA_HEAD, "source": SOURCE, "bindings": bindings,
            "prior_usage": prior, "prior_usage_as_of_utc": datetime.fromtimestamp(NOW, timezone.utc).isoformat(),
            "owned_producers_drained": True, "metadata_pagination_complete": True,
            "shared_external_controls": {"references": [], "actual_bytes": 0, "actual_refs": 0,
                "scope": "bound_external_controls_excludes_this_acceptor_output_directory", "own_output_control_reservation_bytes": 65536},
            **{key: False for key in ("metadata_universe_admitted", "outcomes_admitted", "categories_admitted", "analysis_admitted", "final_sample_admitted")}}
        terminal_ref = put(self.accepted / "RESULT.json", terminal)
        marker = {"schema_version": "polymarket_metadata_v6_terminal_acceptance_completion_v1", "status": terminal["status"],
            "result": {"path": "RESULT.json", "bytes": terminal_ref["bytes"], "sha256": terminal_ref["sha256"], "category": "control"},
            "root_drain": {"path": "root_drain.json", "bytes": bindings["root_drain"]["bytes"], "sha256": bindings["root_drain"]["sha256"], "category": "control"},
            "metadata_universe_admitted": False, "analysis_admitted": False, "final_sample_admitted": False}
        terminal_complete = put(self.accepted / "COMPLETE.json", marker)
        binary_ref = {"path": str(self.binary), "bytes": len(self.original), "sha256": stage.verifier.COMPILER_SHA}
        reported = {**binary_ref, "path": "attempt01.part", "identity": {"fixture": True}}
        result = put(self.acq / "RESULT.json", {"status": "authenticated_solc_binary_not_executed",
            "compiler_sha256": stage.verifier.COMPILER_SHA, "compiler_executed": False, "binary": reported})
        complete = put(self.acq / "COMPLETE.json", {"status": "authenticated_solc_binary_not_executed",
            "result": {"path": "RESULT.json", "bytes": result["bytes"], "sha256": result["sha256"]}, "binary": reported})
        acceptance = put(self.op / "ROOT_ACCEPTANCE.json", {"schema_version": "polymarket_solc_v2_root_acceptance_v1",
            "result": result, "complete": complete, "binary": binary_ref, "compiler_executed": False, "sample_admitted": False,
            "owned_pid_absent": True, "owned_group_and_session_empty": True,
            "launch": put(self.op / "LAUNCH.json", {"fixture": True}), "exit": put(self.op / "EXIT.json", {"fixture": True})})
        self.acquisition_pin = (acceptance["bytes"], acceptance["sha256"])
        commit = put(self.op / "COMMIT.json", {"schema_version": "polymarket_v3_source_build_commit_v2",
            "status": "clean_committed_and_pushed", "source_head": HEAD, "git_status_empty": True,
            "origin_commit_matches": True, "source_files": SOURCE["files"]})
        host = put(self.op / "HOST.json", {"schema_version": "polymarket_v3_source_build_fresh_host_v2",
            "status": "root_verified_current_host_drained", "source_head": HEAD, "source_clean": True,
            "data_uuid": stage.UUID, "data_serial": stage.SERIAL, "data_mount": "/mnt/data",
            "data_st_dev": self.data.stat().st_dev, "boot_id": BOOT, "checked_utc": datetime.fromtimestamp(NOW, timezone.utc).isoformat(),
            "agents_need_EC2": False, "live_owned_births": 0, "unknown_owned_births": 0,
            "relevant_compute_notebook_transfer_consumers": []})
        acq_value = json.loads(Path(acceptance["path"]).read_bytes())
        self.external = put(self.op / "EXTERNAL.json", {"schema_version": "polymarket_source_build_shared_external_controls_v2",
            "references": [acceptance, result, complete, acq_value["launch"], acq_value["exit"],
                commit, host, terminal_ref, terminal_complete, bindings["root_drain"]],
            "future_reserved_bytes": stage.CAPS["control_bytes"], "future_reserved_refs": 16})
        self.admission = {"schema_version": stage.SCHEMA, "status": stage.STATUS, "consumer_head": HEAD,
            "run_dir": str(self.output), "caps": copy.deepcopy(stage.CAPS),
            "terminal_acceptance": {"result": terminal_ref, "complete": terminal_complete},
            "acquisition": {"result": result, "complete": complete, "root_acceptance": acceptance, "binary": binary_ref},
            "source_packages": {era: {"path": str(path), "bytes": path.stat().st_size, "sha256": stage.digest(path.read_bytes())}
                for era, path in (("old", self.fixture.old), ("new", self.fixture.new))},
            "runtime_observation": {"path": str(self.runtime), "bytes": self.runtime.stat().st_size, "sha256": stage.digest(self.runtime.read_bytes())},
            "source_commit": commit, "host_proof": host, "external_controls": self.external,
            "prior_usage": prior, "fresh_preparation_anchor_utc": stage.ANCHOR}
        self.admission_path = self.op / "ADMISSION.json"

    def terminal_runtime(self, runtime, asof):
        refs = self.admission["terminal_acceptance"]
        old_result = refs["result"]; old_marker = refs["complete"]
        result = json.loads(Path(old_result["path"]).read_bytes())
        result["prior_usage"]["runtime_seconds"] = runtime
        if asof is None: result.pop("prior_usage_as_of_utc", None)
        else: result["prior_usage_as_of_utc"] = asof
        new_result = put(Path(old_result["path"]), result)
        marker = json.loads(Path(old_marker["path"]).read_bytes())
        marker["result"].update(bytes=new_result["bytes"], sha256=new_result["sha256"])
        new_marker = put(Path(old_marker["path"]), marker)
        self.admission["terminal_acceptance"] = {"result": new_result, "complete": new_marker}
        ext_path = Path(self.admission["external_controls"]["path"])
        ext = json.loads(ext_path.read_bytes()); replacements = {new_result["path"]: new_result, new_marker["path"]: new_marker}
        ext["references"] = [replacements.get(item["path"], item) for item in ext["references"]]
        self.admission["external_controls"] = put(ext_path, ext)

    def executor(self, argv, stdin, out, err, timeout, check, process):
        self.calls.append((argv, timeout)); check(); process(copy.deepcopy(BIRTH))
        if stdin is None: out(stage.verifier.VERSION_STDOUT)
        else:
            self.assertEqual(argv, [str(self.output / "solc"), "--standard-json", "--no-import-callback"])
            era = stdin.name.split(".")[0]; out(stage.encode(self.fixture.compiled[era]))
        return {"exit_code": 0, "owned_group_drained": True, "birth": copy.deepcopy(BIRTH)}

    def execute(self, *, executor=None, check_only=False, snapshot=None):
        ref = put(self.admission_path, self.admission)
        original_read = Path.read_text; original_stat = Path.stat
        def read_text(path, *args, **kwargs):
            return BOOT if str(path) == "/proc/sys/kernel/random/boot_id" else original_read(path, *args, **kwargs)
        def path_stat(path, *args, **kwargs):
            return original_stat(self.data if str(path) == "/mnt/data" else path, *args, **kwargs)
        def git(argv, **kwargs):
            return b"7\n" if "-s" in argv else b"fixture"
        with ExitStack() as stack:
            for name, value in (("OUTPUT", self.output), ("ACQUISITION_ROOT", self.acq), ("BINARY_BYTES", len(self.original)),
                                ("ACQUISITION_ACCEPTANCE_PIN", self.acquisition_pin), ("OLD_QA", self.oldqa)):
                stack.enter_context(patch.object(stage, name, value))
            stack.enter_context(patch.object(stage, "source_snapshot", side_effect=snapshot or (lambda *_: copy.deepcopy(SOURCE))))
            stack.enter_context(patch.object(stage.subprocess, "check_output", side_effect=git))
            stack.enter_context(patch.object(Path, "read_text", read_text)); stack.enter_context(patch.object(Path, "stat", path_stat))
            stack.enter_context(patch.object(stage.verifier, "group_absent", return_value=True))
            stack.enter_context(patch.object(stage.verifier.shared, "resource_state", return_value={"ram_bytes": {"MemAvailable": 10**12}}))
            return stage.run(self.admission_path, ref["sha256"], HEAD, self.output, check_only=check_only,
                executor=executor or self.executor, clock=lambda: self.ticks, utc=lambda: NOW)

    def test_exact_copy_original_nonexec_three_invocations_full_builds_and_cumulative_usage(self):
        result = self.execute(); self.assertEqual(result["status"], stage.SUCCESS, result)
        self.assertEqual(self.binary.read_bytes(), self.original); self.assertFalse(self.binary.stat().st_mode & 0o111)
        self.assertEqual((self.output / "solc").read_bytes(), self.original)
        self.assertEqual((self.output / "solc").stat().st_mode & 0o777, 0o500)
        self.assertEqual([call[1] for call in self.calls], [10, 600, 600])
        self.assertEqual(result["cumulative_usage"]["runtime_seconds"], 71800)
        self.assertEqual(result["cumulative_usage"]["http_attempts"], 60715)
        self.assertEqual(result["cumulative_usage"]["retained_bytes_charged"]["other"], 16865544 + stage.NEW_RESERVATION["other"])
        self.assertTrue((self.output / "COMPLETE.json").exists()); self.assertFalse(result["final_sample_admitted"])

    def test_check_only_never_creates_output_or_dispatches(self):
        result = self.execute(check_only=True)
        self.assertEqual(result["status"], "source_build_readiness_only_not_executed")
        self.assertFalse(self.output.exists()); self.assertEqual(self.calls, [])

    def test_copy_reduces_allowance_before_first_compiler_dispatch(self):
        with patch.dict(stage.NEW_RESERVATION, other=len(self.original)-1):
            result = self.execute()
        self.assertEqual(result["status"], "blocked"); self.assertEqual(self.calls, [])
        self.assertTrue((self.output / "START.json").exists())
        self.assertFalse((self.output / "COMPLETE.json").exists())

    def test_existing_copy_consumes_other_allowance_before_invocation(self):
        self.output.mkdir()
        with patch.object(stage.verifier.shutil, "disk_usage", return_value=SimpleNamespace(free=10**12)):
            writer = stage.Artifacts(self.output, lambda: 0, lambda: None, self.admission["prior_usage"], 100)
            writer.write("solc", b"x"*16)
            with patch.dict(stage.NEW_RESERVATION, other=20), self.assertRaisesRegex(Exception, "stage_other_limit_including_copy"):
                writer.check(5)
        self.assertEqual(self.calls, [])

    def test_preserve_wrong_original_without_dispatch(self):
        self.binary.write_bytes(b"wrong")
        with self.assertRaises(Exception): self.execute()
        self.assertEqual(self.calls, []); self.assertEqual(self.binary.read_bytes(), b"wrong")

    def test_original_executable_is_rejected(self):
        self.binary.chmod(0o700)
        with self.assertRaisesRegex(Exception, "original_compiler_must_remain_nonexecutable"): self.execute()

    def test_missing_terminal_marker_and_false_drain_block_before_output(self):
        self.admission["terminal_acceptance"]["complete"]["sha256"] = "0" * 64
        with self.assertRaises(Exception): self.execute()
        self.assertFalse(self.output.exists()); self.assertEqual(self.calls, [])

    def test_cumulative_usage_cannot_refund_or_exceed_runtime(self):
        for key, value in (("http_attempts", 60714), ("runtime_seconds", 86000), ("runtime_seconds", True)):
            with self.subTest(key=key, value=value):
                old = self.admission["prior_usage"][key]; self.admission["prior_usage"][key] = value
                with self.assertRaises(Exception): self.execute()
                self.admission["prior_usage"][key] = old
        self.assertEqual(self.calls, [])

    def test_timeout_partial_receipt_and_unknown_ownership_never_complete(self):
        def timed(argv, stdin, out, err, timeout, check, process):
            out(b"bounded-prefix")
            exc = stage.Error("compiler_child_timeout")
            exc.compiler_receipt = {"exit_code": None, "owned_group_drained": False, "birth": None}
            raise exc
        result = self.execute(executor=timed)
        self.assertEqual(result["status"], "blocked"); self.assertTrue(result["ownership_unknown"])
        self.assertEqual((self.output / "version.stdout").read_bytes(), b"bounded-prefix")
        self.assertFalse((self.output / "COMPLETE.json").exists()); self.assertTrue((self.output / "FAILED.json").exists())

    def test_source_callback_late_admission_or_pending_mutation_blocks(self):
        for which in ("admission", "pending", "executable", "original", "oldqa"):
            with self.subTest(which=which):
                if self.output.exists():
                    import shutil
                    shutil.rmtree(self.output)
                def snapshot(*_):
                    p = self.output / ".COMPLETE.json.pending"
                    if p.exists():
                        if which == "oldqa": (self.oldqa / "unexpected.py").write_bytes(b"unexpected")
                        else:
                            target = {"admission": self.admission_path, "pending": p, "executable": self.output / "solc", "original": self.binary}[which]
                            raw = target.read_bytes(); target.write_bytes(raw[:-1] + b" ")
                    return copy.deepcopy(SOURCE)
                result = self.execute(snapshot=snapshot)
                self.assertEqual(result["status"], "blocked")
                self.assertFalse((self.output / "COMPLETE.json").exists())
                if which == "original": self.binary.write_bytes(self.original); self.binary.chmod(0o600)
                if which == "oldqa": (self.oldqa / "unexpected.py").unlink()

    def test_external_closure_cannot_drop_bound_terminal_outputs(self):
        value = json.loads(Path(self.external["path"]).read_bytes())
        value["references"] = value["references"][:3]
        self.admission["external_controls"] = put(Path(self.external["path"]), value)
        with self.assertRaisesRegex(Exception, "external_required_closure_missing"): self.execute()
        self.assertEqual(self.calls, [])

    def test_acquisition_direct_and_nested_controls_cannot_escape_shared_pool(self):
        path = Path(self.admission["external_controls"]["path"])
        good = json.loads(path.read_bytes()); acq = self.admission["acquisition"]
        accepted = json.loads(Path(acq["root_acceptance"]["path"]).read_bytes())
        for missing in [acq[key] for key in ("result", "complete", "root_acceptance")] + [accepted["launch"], accepted["exit"]]:
            with self.subTest(path=missing["path"]):
                value = copy.deepcopy(good)
                value["references"] = [ref for ref in value["references"] if ref["path"] != missing["path"]]
                self.admission["external_controls"] = put(path, value)
                with self.assertRaisesRegex(Exception, "external_required_closure_missing"): self.execute()
                self.assertFalse(self.output.exists())
        self.assertEqual(self.calls, [])

    def test_larger_old_formal_runtime_carries_nonoverlap_instead_of_max_reset(self):
        self.terminal_runtime(84000, stage.ANCHOR)
        self.admission["prior_usage"]["runtime_seconds"] = 84000
        with self.assertRaisesRegex(Exception, "prior_runtime_nonoverlap_missing"): self.execute()
        self.assertFalse(self.output.exists()); self.assertEqual(self.calls, [])

    def test_formal_elapsed_is_carried_once_from_its_current_asof(self):
        asof = datetime.fromtimestamp(NOW-300, timezone.utc).isoformat()
        self.terminal_runtime(70000, asof)
        self.admission["prior_usage"]["runtime_seconds"] = 70300
        result = self.execute()
        self.assertEqual(result["status"], stage.SUCCESS)
        self.assertEqual(result["cumulative_usage"]["runtime_seconds"], 72100)
        self.assertEqual(result["cumulative_usage_as_of_utc"], datetime.fromtimestamp(NOW, timezone.utc).isoformat())

    def test_missing_future_or_naive_formal_asof_fails_closed(self):
        for asof in (None, "2026-10-28T20:00:01Z", "2026-10-28T20:00:00"):
            with self.subTest(asof=asof):
                self.terminal_runtime(70000, asof)
                with self.assertRaises(Exception): self.execute()
                self.assertFalse(self.output.exists())
        self.assertEqual(self.calls, [])

    def test_stale_or_different_host_blocks_before_output(self):
        ref = self.admission["host_proof"]; path = Path(ref["path"]); good = json.loads(path.read_bytes())
        for field, value in (("boot_id", "different"), ("source_clean", False), ("data_st_dev", True),
                             ("checked_utc", "2026-10-28T19:00:00Z")):
            with self.subTest(field=field):
                changed = dict(good); changed[field] = value
                self.admission["host_proof"] = put(path, changed)
                with self.assertRaises(Exception): self.execute()
                self.assertFalse(self.output.exists())
        self.assertEqual(self.calls, [])

    def test_reservation_cannot_be_borrowed_from_other_metadata_partitions(self):
        self.admission["prior_usage"]["retained_bytes_charged"]["other"] = stage.CAPS["metadata_other_bytes"]
        with self.assertRaisesRegex(Exception, "shared_metadata_reservation_unavailable"): self.execute()
        self.assertFalse(self.output.exists()); self.assertEqual(self.calls, [])

    def test_whole_deadline_before_dispatch_preserves_original(self):
        def snapshot(*_): self.ticks = 1800; return copy.deepcopy(SOURCE)
        with self.assertRaisesRegex(Exception, "whole_runtime_limit"): self.execute(snapshot=snapshot)
        self.assertEqual(self.calls, []); self.assertEqual(self.binary.read_bytes(), self.original)

    def test_no_overwrite(self):
        self.output.mkdir(); put(self.output / "keep", b"preserve")
        with self.assertRaises(Exception): self.execute()
        self.assertEqual((self.output / "keep").read_bytes(), b"preserve")

    def test_guard_precedes_actual_input_or_compiler_reads(self):
        with patch.object(stage, "require_production_host", side_effect=RuntimeError("guard")), patch.object(stage, "run") as run:
            with self.assertRaisesRegex(RuntimeError, "guard"):
                stage.main(["--admission", "/none", "--admission-sha256", "a"*64, "--expected-head", HEAD,
                    "--run-dir", str(stage.OUTPUT)])
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
