"""Harmless real child fixtures for the fixed detached caller; never a producer/API."""
from __future__ import annotations

import copy
import ast
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts import launch_polymarket_oct1_stage_v6 as launch

HEAD="a"*40
BOOT="1ad746ce-103e-4e8f-a2f5-ba4df810e899"
SOURCE={"head":HEAD,"files":{"fixture":{"bytes":7,"sha256":"b"*64}}}
NATIVE_POPEN=subprocess.Popen
NATIVE_ATOMIC=launch.atomic_publish
UNPATCHED_STAGES=copy.deepcopy(launch.STAGES)


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.admission=self.root/"admission.json"
        self.write_admission()
        self.output=self.root/"producer-output";self.prefix=self.root/"caller"
        self.births={};self.cached={};self.children=[];self.calls=[]
        p=patch.object(launch,"RUN_ROOT",self.root);p.start();self.addCleanup(p.stop)
        p=patch.object(launch,"source_snapshot",return_value=copy.deepcopy(SOURCE))
        self.snapshot=p.start();self.addCleanup(p.stop)
        # A tiny fixture timeout is only injected into this pure test seam.
        p=patch.object(launch,"STAGES",{k:(v[0],8) for k,v in launch.STAGES.items()})
        p.start();self.addCleanup(p.stop)
        p=patch.object(launch,"CLEANUP_SECONDS",3);p.start();self.addCleanup(p.stop)
        self.addCleanup(self.reap_fixtures)

    def reap_fixtures(self):
        for child in self.children:
            if getattr(child,"_child_created",False) and getattr(child,"pid",None):
                try:
                    try:child.wait(timeout=.1)
                    except subprocess.TimeoutExpired:child.kill();child.wait(timeout=2)
                except (ProcessLookupError,ChildProcessError):pass
                if child.stdout is not None:child.stdout.close()

    def identities(self,pid):
        # Fixture-only identity seam: actual pid/group/session, stable synthetic
        # birth/boot. The Linux-only fixture below exercises /proc instead.
        if pid not in self.births:self.births[pid]=time.monotonic_ns()
        try:
            value={"hostname":"fixture","boot_id":BOOT,"pid":pid,"birth_ticks":self.births[pid],
                   "process_group":os.getpgid(pid),"session":os.getsid(pid)}
            self.cached[pid]=value
        except ProcessLookupError:
            # Darwin's getpgid/getsid hide a WNOWAIT zombie. The fixture keeps
            # the captured identity; production uses Linux /proc, not this seam.
            if sys.platform.startswith("linux") or pid not in self.cached:raise
            value=self.cached[pid]
        return dict(value)

    def members(self,owner):
        self.assertEqual(self.identities(owner["pid"]),owner)
        return {owner["pid"]:{"birth_ticks":owner["birth_ticks"],"state":"fixture"}}

    def producer(self,code="print('fixture')",after=None):
        case=self
        class FixturePopen(NATIVE_POPEN):
            def __init__(child,args,**kwargs):
                case.calls.append({"args":args,"kwargs":dict(kwargs)})
                super().__init__([sys.executable,"-c","import time;time.sleep(.03);"+code],**kwargs)
                case.children.append(child)
                if after is not None:after(child)
        return FixturePopen

    def write_admission(self,stage="headers",prior=0):
        value={"schema_version":launch.ADMISSIONS[stage][0],"status":launch.ADMISSIONS[stage][1]}
        if stage=="headers":value["prior"]={"rpc":{"elapsed_seconds":prior}}
        else:value["prior_ledger"]={"runtime_seconds":prior}
        self.admission.write_bytes(launch.encode(value));self.sha=launch.digest(self.admission.read_bytes())

    def execute(self,code="print('fixture')",**kwargs):
        return launch.run_stage(kwargs.pop("stage","headers"),self.admission,self.sha,HEAD,self.output,self.prefix,
            popen_type=kwargs.pop("popen_type",self.producer(code)),identities=kwargs.pop("identities",self.identities),
            members=kwargs.pop("members",self.members),**kwargs)

    def receipt(self,name):return json.loads(Path(str(self.prefix)+"."+name+".json").read_bytes())

    def test_zero_exit_natural_eof_owned_session_and_fixed_command(self):
        result=self.execute()
        self.assertEqual(result["producer_exit_code"],0);self.assertIs(result["owned_group_drained"],True)
        self.assertIs(result["children_still_running"],False);self.assertTrue(result["stdout_eof_observed"])
        self.assertEqual(result["stdout_bytes_observed"],8)
        self.assertEqual(result["stdout_sha256"],hashlib.sha256(b"fixture\n").hexdigest())
        owner=result["child"];self.assertEqual(owner["pid"],owner["process_group"])
        self.assertEqual(owner["pid"],owner["session"]);self.assertEqual(owner["boot_id"],BOOT)
        self.assertNotEqual(owner["pid"],os.getpid());self.assertFalse(result["host_idle_claimed"])
        self.assertEqual(self.receipt("START")["status"],"caller_started_producer_not_yet_dispatched")
        self.assertEqual(self.receipt("LAUNCH")["child"],owner)
        self.assertEqual(self.receipt("EXIT"),result)
        call=self.calls[0];self.assertTrue(call["kwargs"]["start_new_session"])
        self.assertEqual(call["kwargs"]["stdin"],subprocess.DEVNULL)
        self.assertEqual(call["args"],launch.command("headers",self.admission,self.sha,HEAD,self.output))
        self.assertIn("--rpc-private-file",call["args"]);self.assertNotIn("shell",call["kwargs"])

    def test_nonzero_native_exit_is_preserved(self):
        result=self.execute("print('failed');raise SystemExit(7)")
        self.assertEqual(result["producer_exit_code"],7);self.assertTrue(result["owned_group_drained"])
        self.assertFalse(result["ownership_unknown"]);self.assertIsNone(result["error_code"])

    def test_metadata_only_full_fixed_cli(self):
        for stage in ("metadata","shell","headers; echo bad",""):
            with self.assertRaises(launch.Blocked):launch.command(stage,self.admission,self.sha,HEAD,self.output)

    def test_old_metadata_schema_or_status_rejected_before_dispatch(self):
        for schema,status in (("polymarket_market_metadata_campaign_admission_v4","root_compact_metadata_campaign_admitted"),
                ("polymarket_market_metadata_campaign_admission_v5","root_compact_metadata_continuation_admitted"),
                ("polymarket_market_metadata_campaign_admission_v6","root_compact_metadata_budget_continuation_admitted"),
                ("polymarket_market_metadata_campaign_admission_v5","root_exact_header_campaign_admitted")):
            value={"schema_version":schema,"status":status,"prior_ledger":{"runtime_seconds":0}}
            self.admission.write_bytes(launch.encode(value));self.sha=launch.digest(self.admission.read_bytes())
            with self.assertRaises(launch.Blocked):self.execute(stage="metadata")
            self.assertEqual(self.calls,[])

    def test_fixed_header_v7_entrypoint_and_schema_no_overrides(self):
        args=launch.command("headers",self.admission,self.sha,HEAD,self.output)
        self.assertEqual(args[:2],[str(launch.PYTHON),"scripts/collect_polymarket_exact_header_campaign_v7.py"])
        self.assertEqual(UNPATCHED_STAGES["headers"][1],72*3600)
        self.assertEqual(set(UNPATCHED_STAGES),{"headers"})
        self.assertEqual(launch.ADMISSIONS["headers"],("polymarket_exact_header_campaign_admission_v7",
                                                     "root_exact_header_campaign_admitted"))
        self.assertEqual(set(launch.ADMISSIONS),{"headers"})
        self.assertEqual(launch.SCHEMA,"polymarket_oct1_stage_launcher_v6")

    def test_v4_header_admission_rejected_zero_children(self):
        value={"schema_version":"polymarket_exact_header_campaign_admission_v4",
               "status":"root_exact_header_campaign_admitted","prior":{"rpc":{"elapsed_seconds":0}}}
        self.admission.write_bytes(launch.encode(value));self.sha=launch.digest(self.admission.read_bytes())
        with self.assertRaisesRegex(launch.Blocked,"admission_schema_differs"):self.execute()
        self.assertEqual(self.calls,[])

    def test_frozen_v5_header_admission_rejected_zero_children(self):
        value={"schema_version":"polymarket_exact_header_campaign_admission_v5",
               "status":"root_exact_header_campaign_admitted","prior":{"rpc":{"elapsed_seconds":0}}}
        self.admission.write_bytes(launch.encode(value));self.sha=launch.digest(self.admission.read_bytes())
        with self.assertRaisesRegex(launch.Blocked,"admission_schema_differs"):self.execute()
        self.assertEqual(self.calls,[])

    def test_frozen_v6_header_admission_rejected_zero_children(self):
        value={"schema_version":"polymarket_exact_header_campaign_admission_v6",
               "status":"root_exact_header_campaign_admitted","prior":{"rpc":{"elapsed_seconds":0}}}
        self.admission.write_bytes(launch.encode(value));self.sha=launch.digest(self.admission.read_bytes())
        with self.assertRaises(launch.Blocked):self.execute()
        self.assertEqual(self.calls,[])

    def test_v5_machinery_exact_ast_outside_fixed_profile_constants(self):
        allowed={"STAGES","ADMISSIONS","SOURCE_FILES","SCHEMA","JSON_LIMIT"}
        def normalized(path):
            tree=ast.parse(path.read_bytes())
            tree.body=[node for index,node in enumerate(tree.body) if not
                (index==0 and isinstance(node,ast.Expr) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str))
                and not(isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in allowed for target in node.targets for t in ast.walk(target)))]
            return ast.dump(tree,include_attributes=False)
        self.assertEqual(normalized(launch.REPO/"scripts/launch_polymarket_oct1_stage_v5.py"),
            normalized(launch.REPO/"scripts/launch_polymarket_oct1_stage_v6.py"))
        self.assertEqual((launch.CONTROL_LIMIT,launch.LOG_LIMIT,launch.JSON_LIMIT),(2*1024**2,960*1024,8192))

    def test_v4_process_deadline_machinery_unchanged(self):
        old=launch.REPO/"scripts/launch_polymarket_oct1_stage_v4.py"
        self.assertEqual(launch.digest(old.read_bytes()),"445323b00af6fb5e712731a64db38aeb862e810a9a1cf2333c7ac90f38d2d458")
        allowed={"STAGES","ADMISSIONS","SOURCE_FILES","SCHEMA","JSON_LIMIT"}
        def machinery(path):
            tree=ast.parse(path.read_bytes())
            tree.body=[node for index,node in enumerate(tree.body) if not
                (index==0 and isinstance(node,ast.Expr) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str))
                and not (isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in allowed for target in node.targets for t in ast.walk(target)))]
            return ast.dump(tree,include_attributes=False)
        self.assertEqual(machinery(old),machinery(Path(launch.__file__)))

    def test_source_bindings_include_preserved_and_actual_profiles(self):
        self.assertEqual(set(launch.SOURCE_FILES),{"production_guard.py",
            "scripts/launch_polymarket_oct1_stage_v1.py","tests/test_launch_polymarket_oct1_stage_v1.py",
            "scripts/launch_polymarket_oct1_stage_v2.py","tests/test_launch_polymarket_oct1_stage_v2.py",
            "scripts/launch_polymarket_oct1_stage_v3.py","tests/test_launch_polymarket_oct1_stage_v3.py",
            "scripts/launch_polymarket_oct1_stage_v4.py","tests/test_launch_polymarket_oct1_stage_v4.py",
            "scripts/launch_polymarket_oct1_stage_v5.py","tests/test_launch_polymarket_oct1_stage_v5.py",
            "scripts/launch_polymarket_oct1_stage_v6.py","tests/test_launch_polymarket_oct1_stage_v6.py",
            "scripts/collect_polymarket_exact_header_campaign_v7.py","tests/test_polymarket_exact_header_campaign_v7.py",
            "scripts/collect_polymarket_exact_header_campaign_v4.py",
            "scripts/collect_polymarket_exact_header_campaign_v5.py",
            "tests/test_polymarket_exact_header_campaign_v6.py",
            "scripts/collect_polymarket_exact_header_campaign_v6.py",
            "tests/test_polymarket_exact_header_campaign_v5.py",
            "scripts/collect_polymarket_market_metadata_campaign_v4.py",
            "scripts/collect_polymarket_market_metadata_campaign_v5.py",
            "scripts/collect_polymarket_market_metadata_campaign_v6.py",
            "tests/test_polymarket_market_metadata_campaign_v5.py",
            "tests/test_polymarket_market_metadata_campaign_v6.py"})
        self.assertEqual(len(launch.SOURCE_FILES),len(set(launch.SOURCE_FILES)))

    def test_v3_machinery_exact_ast_equivalence_and_original_pins(self):
        old_path=launch.REPO/"scripts/launch_polymarket_oct1_stage_v3.py"
        test_path=launch.REPO/"tests/test_launch_polymarket_oct1_stage_v3.py"
        self.assertEqual(launch.digest(old_path.read_bytes()),"eb2f7684c56a7ef6fa74bac6dba796220c425f04074705b876c3e44e146f880a")
        self.assertEqual(launch.digest(test_path.read_bytes()),"840e44ef4878efda296591dc63235b67ed9d34bbbbc500edba609e1704f88169")
        allowed={"STAGES","ADMISSIONS","SOURCE_FILES","SCHEMA","JSON_LIMIT"}
        def machinery(raw):
            tree=ast.parse(raw)
            tree.body=[node for i,node in enumerate(tree.body) if not
                (i==0 and isinstance(node,ast.Expr) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str))
                and not (isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id in allowed for target in node.targets for t in ast.walk(target)))]
            return ast.dump(tree,include_attributes=False)
        self.assertEqual(machinery(old_path.read_bytes()),machinery(Path(launch.__file__).read_bytes()))

    def test_both_authenticated_prior_paths_debit_original_clock(self):
        for stage in ("headers",):
            with self.subTest(stage=stage):
                self.write_admission(stage,1.25)
                self.prefix=self.root/("caller-"+stage)
                result=self.execute(stage=stage)
                self.assertEqual(result["admission_prior_runtime_seconds"],1.25)
                self.assertEqual(result["remaining_wall_seconds_at_start"],6.75)
                self.assertEqual(result["producer_exit_code"],0)
                self.assertEqual(self.calls[-1]["args"],launch.command(stage,self.admission,self.sha,HEAD,self.output))

    def test_bad_missing_or_exhausted_priors_zero_dispatch(self):
        for stage in ("headers",):
            for prior in (True,-1,float("nan"),float("inf"),5,8):
                with self.subTest(stage=stage,prior=prior):
                    # Nonfinite fixture bytes intentionally bypass allow_nan=False.
                    value={"schema_version":launch.ADMISSIONS[stage][0],"status":launch.ADMISSIONS[stage][1]}
                    if stage=="headers":value["prior"]={"rpc":{"elapsed_seconds":prior}}
                    else:value["prior_ledger"]={"runtime_seconds":prior}
                    self.admission.write_text(json.dumps(value));self.sha=launch.digest(self.admission.read_bytes())
                    with self.assertRaises(launch.Blocked):self.execute(stage=stage)
            self.admission.write_bytes(launch.encode({"schema_version":launch.ADMISSIONS[stage][0],
                "status":launch.ADMISSIONS[stage][1]}));self.sha=launch.digest(self.admission.read_bytes())
            with self.assertRaises(launch.Blocked):self.execute(stage=stage)
        self.assertEqual(self.calls,[])

    def test_inherited_prior_shortens_actual_child_watchdog(self):
        stages={k:(v[0],1.5) for k,v in launch.STAGES.items()}
        self.write_admission(prior=.4);began=time.monotonic()
        with patch.object(launch,"STAGES",stages),patch.object(launch,"CLEANUP_SECONDS",.5):
            result=self.execute("import time;time.sleep(20)")
        self.assertLess(time.monotonic()-began,1.1)
        self.assertEqual(result["error_code"],"caller_timeout");self.assertTrue(result["owned_group_drained"])

    def test_final_aggregate_alarm_during_drain_is_not_swallowed(self):
        stages={k:(v[0],.9) for k,v in launch.STAGES.items()}
        # Simulate a genuinely blocking drainage operation, not elapsed-clock
        # bookkeeping. Final original alarm must escape, leaving no idle EXIT.
        with patch.object(launch,"STAGES",stages),patch.object(launch,"CLEANUP_SECONDS",.4), \
             patch.object(launch,"drain",side_effect=lambda *a,**k:time.sleep(2)), \
             self.assertRaises(launch.Stop) as caught:
            self.execute("import time;time.sleep(20)")
        self.assertEqual(caught.exception.code,"caller_timeout")
        self.assertFalse(Path(str(self.prefix)+".EXIT.json").exists())
        self.assertEqual(self.snapshot.call_count,2)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_prefix_reuse_refused_before_spawn(self):
        self.execute()
        with self.assertRaises(launch.Blocked):self.execute()
        self.assertEqual(len(self.calls),1)

    def test_wrong_admission_sha_zero_children(self):
        self.sha="0"*64
        with self.assertRaises(launch.Blocked):self.execute()
        self.assertEqual(self.calls,[]);self.assertFalse(Path(str(self.prefix)+".START.json").exists())

    def test_admission_mutation_between_start_and_spawn(self):
        original=launch.Receipts.publish
        def publish(receipts,name,value,**kwargs):
            result=original(receipts,name,value,**kwargs)
            if name=="START.json":self.admission.write_bytes(b'{"fixture":null}\n')
            return result
        with patch.object(launch.Receipts,"publish",publish):result=self.execute()
        self.assertEqual(self.calls,[]);self.assertEqual(result["error_code"],"launch_input_changed")
        self.assertFalse(result["source_and_admission_unchanged"])

    def test_source_changed_after_child_never_successful_bindings(self):
        def after(child):self.snapshot.return_value={"head":HEAD,"files":{"fixture":"changed"}}
        result=self.execute(popen_type=self.producer(after=after))
        self.assertFalse(result["source_and_admission_unchanged"]);self.assertTrue(result["owned_group_drained"])

    def test_log_prefix_capped_but_complete_count_and_hash(self):
        count=launch.LOG_LIMIT+12345
        result=self.execute(f"import os; data=b'x'*{count}; offset=0\nwhile offset<len(data):offset+=os.write(1,data[offset:])")
        self.assertEqual(result["stdout_bytes_observed"],count)
        self.assertEqual(result["stdout_sha256"],hashlib.sha256(b"x"*count).hexdigest())
        self.assertEqual(result["stdout_prefix_bytes_retained"],launch.LOG_LIMIT)
        self.assertTrue(result["stdout_truncated"]);self.assertTrue(result["stdout_eof_observed"])
        self.assertEqual(Path(str(self.prefix)+".log").read_bytes(),b"x"*launch.LOG_LIMIT)
        self.assertLessEqual(sum(p.stat().st_size for p in self.root.glob("caller.*")),launch.CONTROL_LIMIT)

    def test_constructor_raises_after_actual_spawn_handle_is_drained(self):
        def after(child):raise launch.Stop("caller_timeout")
        result=self.execute(popen_type=self.producer("import time;time.sleep(20)",after))
        self.assertEqual(result["error_code"],"caller_timeout");self.assertTrue(result["owned_group_drained"])
        self.assertFalse(result["ownership_unknown"]);self.assertIsNotNone(self.children[0].returncode)
        self.assertEqual(self.receipt("LAUNCH")["child"],result["child"])

    def test_missing_birth_never_signals_or_claims_drained(self):
        def identities(pid):
            if pid!=os.getpid():raise launch.Blocked("fixture_missing_birth")
            return self.identities(pid)
        with patch.object(launch.os,"killpg") as kill:
            result=self.execute("import time;time.sleep(20)",identities=identities)
        kill.assert_not_called();self.assertTrue(result["ownership_unknown"])
        self.assertIsNone(result["owned_group_drained"]);self.assertIsNone(result["children_still_running"])
        self.assertIsNone(self.children[0].returncode)

    def test_identity_reuse_never_signals_unbound_group(self):
        counts={}
        def identities(pid):
            value=self.identities(pid);counts[pid]=counts.get(pid,0)+1
            if pid!=os.getpid() and counts[pid]>1:value["birth_ticks"]+=1
            return value
        with patch.object(launch.os,"killpg") as kill:
            result=self.execute(identities=identities)
        kill.assert_not_called();self.assertTrue(result["ownership_unknown"])
        self.assertIsNone(result["children_still_running"])

    def test_partial_constructor_without_handle_is_unknown(self):
        class NoHandle(NATIVE_POPEN):
            def __init__(child,*args,**kwargs):raise RuntimeError("fixture")
        with patch.object(launch.os,"killpg") as kill:result=self.execute(popen_type=NoHandle)
        kill.assert_not_called();self.assertTrue(result["ownership_unknown"])
        self.assertIsNone(result["children_still_running"])

    def test_real_timeout_and_reap_stay_inside_original_total(self):
        stages={k:(v[0],1.4) for k,v in launch.STAGES.items()}
        began=time.monotonic()
        with patch.object(launch,"STAGES",stages),patch.object(launch,"CLEANUP_SECONDS",.5):
            result=self.execute("import time;time.sleep(20)")
        self.assertLess(time.monotonic()-began,1.4)
        self.assertEqual(result["error_code"],"caller_timeout");self.assertTrue(result["owned_group_drained"])
        self.assertIsNotNone(self.children[0].returncode)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_real_slow_preflight_has_original_deadline_and_restores_handlers(self):
        stages={k:(v[0],.7) for k,v in launch.STAGES.items()}
        old_alarm=signal.getsignal(signal.SIGALRM);old_term=signal.getsignal(signal.SIGTERM)
        self.snapshot.side_effect=lambda _:time.sleep(2)
        began=time.monotonic()
        with patch.object(launch,"STAGES",stages),patch.object(launch,"CLEANUP_SECONDS",.2), \
             self.assertRaises(launch.Stop) as caught:self.execute()
        self.assertEqual(caught.exception.code,"caller_timeout");self.assertLess(time.monotonic()-began,.7)
        self.assertEqual(self.calls,[]);self.assertFalse(Path(str(self.prefix)+".START.json").exists())
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))
        self.assertEqual(signal.getsignal(signal.SIGALRM),old_alarm)
        self.assertEqual(signal.getsignal(signal.SIGTERM),old_term)

    def test_actual_child_inherits_unlimited_fsize_not_log_cap(self):
        result=self.execute("import resource;print(resource.getrlimit(resource.RLIMIT_FSIZE))")
        self.assertEqual(Path(str(self.prefix)+".log").read_text().strip(),str((resource.RLIM_INFINITY,)*2))
        self.assertEqual(result["producer_exit_code"],0)
        self.assertNotIn("preexec_fn",self.calls[0]["kwargs"])

    def test_finite_fsize_blocks_before_constructor(self):
        with patch.object(launch.resource,"getrlimit",return_value=(1024,2048)):
            result=self.execute()
        self.assertEqual(self.calls,[])
        self.assertEqual(result["error_code"],"producer_must_not_inherit_file_size_limit")

    def test_directory_fsync_is_after_each_atomic_rename(self):
        events=[];original_fsync=launch.os.fsync;original_publish=launch.atomic_publish
        def fsync(fd):events.append("fsync");return original_fsync(fd)
        def publish(pending,target):
            events.append("rename-enter");result=original_publish(pending,target);events.append("rename-return");return result
        with patch.object(launch.os,"fsync",fsync),patch.object(launch,"atomic_publish",publish):self.execute()
        for i,event in enumerate(events):
            if event=="rename-enter":self.assertEqual(events[i+1:i+3],["fsync","rename-return"])

    def test_final_directory_fsync_failure_raises_not_success(self):
        original_fsync=launch.os.fsync;original_publish=launch.atomic_publish
        def publish(pending,target):
            if str(target).endswith(".EXIT.json"):
                with patch.object(launch.os,"fsync",side_effect=OSError("fixture flush failure")):
                    return original_publish(pending,target)
            return original_publish(pending,target)
        with patch.object(launch,"atomic_publish",publish),self.assertRaises(OSError):self.execute()
        self.assertTrue(Path(str(self.prefix)+".EXIT.json").exists())
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_pending_flush_failure_never_publishes_start(self):
        with patch.object(launch.os,"fsync",side_effect=OSError("fixture")),self.assertRaises(OSError):self.execute()
        self.assertEqual(self.calls,[]);self.assertFalse(Path(str(self.prefix)+".START.json").exists())

    def test_control_cap_fails_before_start_or_spawn(self):
        with patch.object(launch,"CONTROL_LIMIT",2),self.assertRaises(launch.Blocked):self.execute()
        self.assertEqual(self.calls,[])

    def test_no_fallible_artifact_io_after_exit_publish(self):
        original=launch.atomic_publish;committed=False
        original_bound=launch.bound
        def publish(pending,target):
            nonlocal committed
            result=original(pending,target)
            if str(target).endswith(".EXIT.json"):committed=True
            return result
        def bound(*args,**kwargs):
            self.assertFalse(committed,"artifact reopened after terminal publish")
            return original_bound(*args,**kwargs)
        with patch.object(launch,"atomic_publish",publish),patch.object(launch,"bound",bound):result=self.execute()
        self.assertEqual(result["producer_exit_code"],0)

    def test_production_guard_runs_before_binding_reads(self):
        args=["--stage","headers","--admission",str(self.admission),"--admission-sha256",self.sha,
              "--expected-head",HEAD,"--run-dir",str(self.output),"--receipt-prefix",str(self.prefix)]
        with patch.object(launch,"require_production_host",side_effect=RuntimeError("guard")), \
             patch.object(launch,"source_snapshot") as snapshot,self.assertRaises(RuntimeError):launch.main(args)
        snapshot.assert_not_called();self.assertEqual(self.calls,[])

    def test_wrong_output_or_symlink_never_spawns(self):
        self.output.mkdir()
        with self.assertRaises(launch.Blocked):self.execute()
        self.assertEqual(self.calls,[])

    def test_eight_kibibyte_header_receipt_bound_is_enforced_before_spawn(self):
        huge={"head":HEAD,"files":{"oversized":"x"*8192}}
        with patch.object(launch,"source_snapshot",return_value=huge):
            with self.assertRaisesRegex(launch.Blocked,"receipt_json_limit"):self.execute()
        self.assertEqual(self.calls,[])
        self.assertFalse(Path(str(self.prefix)+".START.json").exists())

    @unittest.skipUnless(sys.platform.startswith("linux"),"real /proc ownership proof requires Linux")
    def test_linux_native_process_birth_and_group_drain(self):
        result=self.execute(identities=launch.process_identity,members=launch.group_members)
        self.assertTrue(result["owned_group_drained"]);self.assertEqual(result["producer_exit_code"],0)


if __name__=="__main__":unittest.main()
