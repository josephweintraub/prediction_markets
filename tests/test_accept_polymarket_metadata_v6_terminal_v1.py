"""Tiny completed metadata walk and host fixtures; no production reads/requests."""
import copy
from contextlib import ExitStack
from datetime import timedelta
import json
import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import accept_polymarket_metadata_v6_terminal_v1 as accept
from tests import test_polymarket_market_metadata_campaign_v6 as previous

write, bound = previous.old.v4.write, previous.old.v4.bound
BOOT = "2ad746ce-103e-4e8f-a2f5-ba4df810e899"


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        c = previous.BudgetContinuationTests(); c.setUp(); self.addCleanup(c.doCleanups)
        self.c = c; self.root, self.ops = c.root, c.ops
        self.qa = self.root / "old_qa"; self.qa.mkdir()
        qa_ref = write(self.qa / "fixture.txt", b"fixture QA", raw=True)
        process_ref = write(self.ops / "child.PROCESS.json", {"pid": 100, "birth_ticks": 100,
            "boot_id": "1ad746ce-103e-4e8f-a2f5-ba4df810e899"})
        roster = write(self.ops / "finalization_shutdown_drain_v2.json", {"process_receipts": [{"binding": process_ref}]})
        audit_ref = write(self.ops / "old_audit.json", {"scope": "synthetic"})
        old_ledger = write(self.ops / "old_external.json", {"schema_version": "polymarket_metadata_v5_preparation_external_controls_v1",
            "isolated_qa_root": str(self.qa), "direct_controls": [roster], "nested_files": [qa_ref]})
        drain_path = Path(c.admission["v5_prefix"]["drain"]["path"])
        drain = json.loads(drain_path.read_bytes())
        drain["external_control_closure"] = {"external_closure_ref": old_ledger,
            "future_actual_controls": [], "audit_source": audit_ref}
        drain["bindings"]["old_roster"] = roster
        drain_ref = write(drain_path, drain); c.admission["v5_prefix"]["drain"] = drain_ref
        c.pin["drain"] = (drain_ref["bytes"], drain_ref["sha256"])
        direct = c.admission["prior_ledger"]["root_controls"]
        nested = write(self.ops / "operator_source.py", b"# synthetic operator\n", raw=True)
        ledger = write(self.ops / "full_metadata_v6_external_ledger.json", {"schema_version": "polymarket_metadata_v6_preparation_external_controls_v1",
            "consumer_head": previous.HEAD, "prior_terminal_drain": drain_ref,
            "shared_external_control_ceiling_bytes": accept.CAPS["external_bytes"], "no_second_reservation": True,
            "new_direct_controls": direct, "new_nested_files": [nested]})
        c.admission["prior_ledger"]["root_controls"] = direct + [ledger]
        c.admission["prior_ledger"]["retained_bytes_charged"]["control"] += ledger["bytes"] + 10000
        result = c.execute(); self.assertEqual(result["status"], accept.acquisition.SUCCESS)
        self.manifest = json.loads((c.output / "manifest.json").read_bytes())
        start = json.loads((c.output / "START.json").read_bytes())
        child = {"hostname": "fixture", "boot_id": "1ad746ce-103e-4e8f-a2f5-ba4df810e899",
            "pid": 60, "birth_ticks": 70, "process_group": 60, "session": 60}
        caller = child | {"pid": 50, "birth_ticks": 60, "process_group": 50, "session": 50}
        launch = {"schema_version": "polymarket_oct1_stage_launcher_v4", "stage": "metadata", "status": "producer_dispatched_owned_session",
            "source": start["source"], "admission": start["admission"], "command": ["fixture"], "output_dir": str(c.output),
            "child": child, "caller": caller, "ownership_unknown": False, "retry_or_resume": False}
        launch_ref = write(self.ops / "full_metadata_v6.LAUNCH.json", launch)
        log = write(self.ops / "full_metadata_v6.log", b"synthetic terminal\n", raw=True)
        exited = launch | {"status": "producer_exited", "error_code": None, "producer_exit_code": 0,
            "owned_group_drained": True, "children_still_running": False, "stdout_eof_observed": True,
            "source_and_admission_unchanged": True, "stdout_truncated": False,
            "stdout_bytes_observed": log["bytes"], "stdout_prefix_bytes_retained": log["bytes"], "stdout_sha256": log["sha256"]}
        exit_ref = write(self.ops / "full_metadata_v6.EXIT.json", exited)
        self.terminal = {"manifest": bound(c.output / "manifest.json"), "complete": bound(c.output / "COMPLETE.json"),
            "producer_start": bound(c.output / "START.json"), "launch": launch_ref,
            "caller_exit": exit_ref, "admission": bound(c.admission_path)}
        check = write(self.ops / "full_metadata_v6_root_admission_check_v1.json", {"scope": "fixture"})
        caller_start = write(self.ops / "full_metadata_v6.START.json", launch)
        self.dispatch = write(self.ops / "full_metadata_v6_root_dispatch_review_v1.json", {
            "schema_version": "polymarket_metadata_v6_root_dispatch_review_v1", "source_head": previous.HEAD,
            "admission": self.terminal["admission"], "launch": launch_ref, "builder_check": check,
            "caller_start": caller_start, "external_ledger": ledger})
        old_refs = [roster, qa_ref, old_ledger, audit_ref, drain_ref]
        controls = old_refs + direct + [nested, ledger] + [self.terminal["admission"], check, caller_start,
            launch_ref, exit_ref, log, self.dispatch]
        self.counts = accept.COUNTS | {"v4_files": 9, "v5_files": 6, "prefix_pages": 6,
            "prefix_rows": start["replay"]["raw_rows"], "new_pages": 3, "new_rows": 3,
            "inventory_records": 6, "old_process_receipts": 1, "old_external_refs": len(old_refs),
            "old_external_bytes": sum(r["bytes"] for r in old_refs), "external_refs": len(controls),
            "external_actual_bytes": sum(r["bytes"] for r in controls)}
        self.roster_pin = (roster["bytes"], roster["sha256"])
        self.base = copy.deepcopy(accept.BASE)
        self.base.update(http_attempts=result["usage"]["http_attempts_charged"],
            response_bytes_charged=result["usage"]["response_bytes_charged"])
        self.source = {"head": "f"*40, "files": copy.deepcopy(c.source["files"])}
        for name in accept.SOURCE_FILES[len(accept.acquisition.SOURCE_FILES):]:
            raw = (accept.REPO/name).read_bytes(); self.source["files"][name] = {"bytes":len(raw),"sha256":accept.digest(raw)}
        for name in accept.acquisition.SOURCE_FILES:
            c.parent.fixture.blobs[name] = (accept.REPO/name).read_bytes()
        artifacts = [{"path":str(accept.REPO/name), **self.source["files"][name]}
            for name in accept.SOURCE_FILES[len(accept.acquisition.SOURCE_FILES):]]
        self.output = self.root / "accepted"; self.admission_path = self.ops / "metadata_v6_terminal_acceptance_admission.json"
        self.admission = {"schema_version": accept.ADMISSION, "status":"root_metadata6_terminal_reopen_admitted",
            "consumer_head":self.source["head"], "run_dir":str(self.output), "caps":accept.CAPS,
            "terminal":self.terminal, "dispatch":self.dispatch,"prior_usage":copy.deepcopy(self.base),
            "new_artifacts":artifacts,"stdout_reserved_bytes":4096,"boot_id":BOOT,"data_st_dev":os.stat(self.root).st_dev,
            "prior_read_bytes":0}
        self.tick=0.; self.now=accept.ANCHOR

    def processes(self, _):
        return {os.getpid(): {"pid":os.getpid(),"parent_pid":0,"birth_ticks":999,"group":os.getpid(),"session":os.getpid(),
            "state":"R","kernel_thread_flag":False,"cmdline_empty":False,"full_exe":"/fixture/python",
            "comm":"python","program":None,"uid":1000,"user_service":None}}

    def contexts(self):
        stack=ExitStack(); c=self.c
        for name,value in {"RUN_ROOT":self.root,"OPERATIONS":self.ops,"RUN":c.output,"HEAD":previous.HEAD,
            "OLD_QA":self.qa,"OLD_ROSTER_PIN":self.roster_pin,"DISPATCH_PIN":(self.dispatch["bytes"],self.dispatch["sha256"]),
            "COUNTS":self.counts,"BASE":self.base,"PROCESS_ROOTS":(self.ops,c.output,c.parent.old_root,c.v5root,self.qa),
            "PINS":{k:(v["bytes"],v["sha256"]) for k,v in self.terminal.items() if k in accept.PINS}}.items():
            stack.enter_context(patch.object(accept,name,value))
        stack.enter_context(patch.object(accept,"source_snapshot",return_value=self.source))
        stack.enter_context(patch.object(accept.time,"monotonic",side_effect=lambda:self.tick))
        stack.enter_context(patch.object(accept.shutil,"disk_usage",return_value=previous.old.v4.old_fixtures.FREE))
        stack.enter_context(patch.object(accept,"git_bytes",side_effect=lambda reader,args,maximum=1024**2:c.parent.fixture.git(["git",*args])))
        for module,names in ((accept.acquisition,{"RUN_ROOT":self.root,"OPERATIONS":self.ops,"PIN":c.pin,"APPROVAL_PIN":c.approval_pin}),
            (accept.acquisition.previous,{"RUN_ROOT":self.root,"OPERATIONS":self.ops,"PIN":c.parent.pin}),
            (accept.acquisition.v4,{"RUN_ROOT":self.root})):
            for name,value in names.items():stack.enter_context(patch.object(module,name,value))
        return stack

    def execute(self, **kwargs):
        binding=write(self.admission_path,self.admission)
        with self.contexts():
            return accept.run_acceptance(self.admission_path,binding["sha256"],self.source["head"],self.output,
                snapshot=kwargs.pop("snapshot",self.processes),boot_reader=lambda:BOOT,utc_now=lambda:self.now,**kwargs)

    def test_completed_raw_walk_is_accepted_without_universe_claims(self):
        result=self.execute()
        self.assertEqual(result["status"],accept.STATUS)
        self.assertEqual(result["prior_usage"]["runtime_seconds"],self.base["runtime_seconds"]+600)
        self.assertEqual(result["prior_usage_as_of_utc"],self.now.isoformat().replace("+00:00","Z"))
        self.assertEqual(result["reconciliation"]["new_http_attempts"],3)
        self.assertFalse(result["historical_semantic_replay_repeated"])
        self.assertTrue(result["owned_producers_drained"])
        self.assertTrue(all(result[k] is False for k in accept.FALSE_SCOPE))
        self.assertEqual({p.name for p in self.output.iterdir()},{"RESULT.json","COMPLETE.json","root_drain.json"})
        self.assertLessEqual(sum(p.stat().st_size for p in self.output.iterdir()),65536*3)

    def test_prior_usage_cannot_refund_floors_or_preparation(self):
        self.admission["prior_usage"]["response_bytes_charged"]-=1
        with self.assertRaisesRegex(Exception,"prior_usage_below_floor"):self.execute()
        self.assertFalse(self.output.exists())

    def test_reserved_runtime_must_fit(self):
        self.admission["prior_usage"]["runtime_seconds"]=85800
        with self.assertRaisesRegex(Exception,"shared_runtime_exhausted"):self.execute()

    def test_fresh_utc_preparation_is_not_reset_to_old_floor(self):
        self.now+=timedelta(seconds=1)
        with self.assertRaisesRegex(Exception,"prior_usage_below_floor"):self.execute()

    def test_mutated_compressed_body_blocks_without_decoding(self):
        p=self.c.output/"responses/a00001.body.gz";p.write_bytes(p.read_bytes()[:-1]+b"x")
        with self.assertRaisesRegex(Exception,"input_hash_or_identity_changed"):self.execute()
        self.assertFalse(self.output.exists())

    def test_extra_output_in_retained_root_is_not_ignored(self):
        (self.c.output/"unlisted").write_bytes(b"x")
        with self.assertRaisesRegex(Exception,"tree_roster_changed"):self.execute()

    def test_terminal_pin_and_completion_link_are_required(self):
        self.admission["terminal"]["complete"]["sha256"]="0"*64
        with self.assertRaises(Exception):self.execute()

    def test_exact_external_qa_tree_is_retained(self):
        (self.qa/"unlisted").write_bytes(b"x")
        with self.assertRaisesRegex(Exception,"tree_roster_changed"):self.execute()

    def test_an_unrelated_same_executable_is_not_acceptor(self):
        def snapshot(r):
            p=self.processes(r);p[99999]=p[os.getpid()]|{"pid":99999,"parent_pid":1,"birth_ticks":1001};return p
        with self.assertRaisesRegex(Exception,"unclassified_compute"):self.execute(snapshot=snapshot)

    def test_no_executable_userspace_process_is_not_silently_dropped(self):
        def snapshot(r):
            p=self.processes(r);p[99999]=p[os.getpid()]|{"pid":99999,"full_exe":None,"parent_pid":1};return p
        with self.assertRaisesRegex(Exception,"unclassified_executable_missing"):self.execute(snapshot=snapshot)

    def test_live_saved_stage_birth_blocks(self):
        def snapshot(r):
            p=self.processes(r);p[100]=p[os.getpid()]|{"pid":100,"birth_ticks":100,"parent_pid":1};return p
        self.admission["boot_id"]="1ad746ce-103e-4e8f-a2f5-ba4df810e899"
        with self.assertRaises(Exception):self.execute(snapshot=snapshot)

    def test_mutation_after_pending_staging_blocks_final_marker(self):
        original=accept.Output.write
        def output(writer,name,value):
            result=original(writer,name,value)
            if name==".COMPLETE.pending":
                path=self.c.output/"responses/a00001.body.gz";path.write_bytes(path.read_bytes()[:-1]+b"x")
            return result
        with patch.object(accept.Output,"write",output),self.assertRaises(Exception):self.execute()
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_own_birth_changes_before_final_census_blocks(self):
        calls=0
        def snapshot(r):
            nonlocal calls;calls+=1;p=self.processes(r)
            if calls>=3:p[os.getpid()]["birth_ticks"]+=1
            return p
        with self.assertRaisesRegex(Exception,"acceptor_ancestry_changed"):self.execute(snapshot=snapshot)
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_final_output_verification_cannot_hide_late_admission_mutation(self):
        original=accept.Output.reopen
        def reopen(writer):
            original(writer)
            self.admission_path.write_bytes(self.admission_path.read_bytes()[:-1]+b" ")
        with patch.object(accept.Output,"reopen",reopen),self.assertRaisesRegex(Exception,"input_identity_changed"):
            self.execute()
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_deadline_crossed_during_final_census_never_publishes(self):
        calls=0
        def snapshot(r):
            nonlocal calls;calls+=1
            if calls>=3:self.tick=600
            return self.processes(r)
        with self.assertRaisesRegex(Exception,"runtime_limit"):self.execute(snapshot=snapshot)
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_small_stdout_and_new_stage_reservation_are_in_same_pool(self):
        self.admission["stdout_reserved_bytes"]=65537
        with self.assertRaisesRegex(Exception,"new_stage_artifact_limit"):self.execute()

    def test_stdout_minimum_covers_the_fixed_success_message(self):
        self.admission["stdout_reserved_bytes"]=1023
        with self.assertRaisesRegex(Exception,"new_stage_artifact_limit"):self.execute()

    def test_preparation_read_debit_cannot_reset_the_input_ceiling(self):
        self.admission["prior_read_bytes"]=accept.CAPS["input_read_bytes"]
        with self.assertRaisesRegex(Exception,"input_read_limit"):self.execute()

    def test_new_source_copy_must_be_present_and_exact(self):
        self.admission["new_artifacts"].pop()
        with self.assertRaisesRegex(Exception,"new_source_artifacts_missing"):self.execute()

    def test_wrong_external_ledger_admission_join_is_not_an_acceptance(self):
        p=Path(json.loads(Path(self.dispatch["path"]).read_bytes())["external_ledger"]["path"])
        value=json.loads(p.read_bytes());value["new_direct_controls"]=[];write(p,value)
        with self.assertRaises(Exception):self.execute()

    def test_atomic_marker_refuses_existing_destination(self):
        original=accept.Output.write
        def write_output(writer,name,value):
            result=original(writer,name,value)
            if name==".COMPLETE.pending":(writer.root/"COMPLETE.json").write_bytes(b"competing marker")
            return result
        with patch.object(accept.Output,"write",write_output),self.assertRaises(Exception):self.execute()
        self.assertEqual((self.output/"COMPLETE.json").read_bytes(),b"competing marker")


class PrimitiveTests(unittest.TestCase):
    def absence_fixture(self):
        binding={"path":"/mnt/data/runs/2026-10-26_polymarket_native_append_campaign_v1/chunks/c000006/a01/PROCESS.json",
            "bytes":60,"sha256":"1a0fcb1631b4ffe1f987a6d2292eedf6c99d706cc17a4ed40e128d04f4355569"}
        row={"binding":binding,"pid":13168,"birth_ticks":345586,"matching_live_birth":False}
        value={"pid":13168,"start_ticks":345586,"state":"S"}
        certificate={"schema_version":"polymarket_oct1_terminal_shutdown_drain_v2","boot_id":"1ad746ce-103e-4e8f-a2f5-ba4df810e899",
            "checked_utc":"2026-10-27T22:40:44.079016+00:00","shutdown_ready":True,"agents_and_transfers_released":True,
            "live_receipt_births":[],"unknown_process_receipts":[],"all_non_kernel_executables_classified":[{"pid":1}]}
        pin={"path":str(accept.OPERATIONS/"finalization_shutdown_drain_v2.json"),"bytes":accept.OLD_ROSTER_PIN[0],"sha256":accept.OLD_ROSTER_PIN[1]}
        return row,value,certificate,pin

    def test_pinned_historical_absence_does_not_invent_original_boot(self):
        row,value,certificate,pin=self.absence_fixture()
        accept.certified_old_absence(row,value,certificate,pin,BOOT)
        self.assertNotIn("boot_id",value)

    def test_historical_absence_requires_exact_certificate_tuple_and_flags(self):
        for field,new in (("shutdown_ready",False),("agents_and_transfers_released",False),
            ("live_receipt_births",[{}]),("unknown_process_receipts",[{}]),("boot_id","invalid")):
            row,value,certificate,pin=self.absence_fixture();certificate[field]=new
            with self.assertRaises(Exception):accept.certified_old_absence(row,value,certificate,pin,BOOT)
        for mutate in (lambda row,value,pin:row.update(matching_live_birth=True),
            lambda row,value,pin:value.update(start_ticks=345587),lambda row,value,pin:pin.update(sha256="0"*64)):
            row,value,certificate,pin=self.absence_fixture();mutate(row,value,pin)
            with self.assertRaises(Exception):accept.certified_old_absence(row,value,certificate,pin,BOOT)

    def test_same_observation_boot_is_not_original_birth_evidence(self):
        row,value,certificate,pin=self.absence_fixture()
        with self.assertRaisesRegex(Exception,"historical_absence_boot_unproven"):
            accept.certified_old_absence(row,value,certificate,pin,certificate["boot_id"])

    def test_nested_child_exit_uses_only_nearest_explicit_valid_boot(self):
        value={"boot_id":"b9008fdc-0b94-4daf-b498-13403e96c475",
            "process":{"birth":{"pid":19709,"start_ticks":520591}}}
        saved=list(accept.births(value));self.assertEqual(saved[0]["boot_id"],value["boot_id"])
        self.assertNotIn("boot_id",value["process"]["birth"])
        self.assertEqual(saved[0]["birth_ticks"],520591)
        for bad in ({"process":value["process"]},value|{"boot_id":"bad"},
            value|{"process":{"boot_id":None,"birth":{"pid":19709,"start_ticks":520591}}}):
            with self.assertRaisesRegex(Exception,"saved_birth_schema"):list(accept.births(bad))

    def old_roots_fixture(self, extra=None):
        roots=["/mnt/data/runs/2026-10-27_polymarket_market_metadata_pilot_v1",
            "/mnt/data/runs/2026-10-27_polymarket_raw_metadata_probe_v2",
            "/mnt/data/runs/2026-10-27_polymarket_raw_metadata_probe_v2b",
            "/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/v3_source_lookup_v1",
            "/mnt/data/runs/2026-10-27_polymarket_market_metadata_full_v3",
            "/mnt/data/runs/2026-10-27_polymarket_solc_binary_v2",
            "/mnt/data/runs/2026-10-27_polymarket_compact_metadata_pilot_v4"]
        if extra:roots.append(extra)
        inventory_ref={"path":"/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1/full_metadata_v4_prior_inventory_v3.json",
            "bytes":1018896,"sha256":"c185181586f674763b95b13e84bf873d38aca9eb7b5b8a88bd6ba16645fbfede"}
        inventory={"schema_version":"polymarket_metadata_prior_inventory_v4","files":[
            {"path":root+"/fixture.json","bytes":1,"sha256":"0"*64,"category":"control"} for root in roots]}
        prefix={"root":"/mnt/data/runs/prefix",**{name:{"path":"/mnt/data/runs/prefix/"+name,
            "bytes":1,"sha256":"0"*64} for name in ("admission","start","failure","attempts","pages")}}
        class ReachedPrefix(Exception):pass
        class Reader:
            def json(self,binding):
                if binding==prefix["admission"]:return {"prior_ledger":{"inventory":inventory_ref,"roots":roots}}
                self_binding=inventory_ref
                if binding!=self_binding:raise AssertionError("unexpected inventory reference")
                return inventory
            def lines(self,*_):raise ReachedPrefix()
        return prefix,Reader(),ReachedPrefix

    def test_actual_seven_prior_roots_include_only_the_pinned_nested_lookup(self):
        prefix,reader,reached=self.old_roots_fixture()
        with self.assertRaises(reached):accept.retained_closure({"v4_prefix":prefix},{},reader)

    def test_adjacent_nested_roots_remain_outside_old_root_scope(self):
        for extra in (str(accept.OPERATIONS/"v3_source_lookup_v2"),
            str(accept.OPERATIONS/"other"/"v3_source_lookup_v1")):
            prefix,reader,_=self.old_roots_fixture(extra)
            with self.assertRaisesRegex(Exception,"old_root_scope"):
                accept.retained_closure({"v4_prefix":prefix},{},reader)

    def test_malformed_saved_birth_cannot_be_proved_dead_by_boot_mismatch(self):
        with self.assertRaisesRegex(Exception,"saved_birth_schema"):
            accept.absent({"pid":False,"birth_ticks":None,"boot_id":"wrong"},{},BOOT)

    def test_guard_runs_before_any_admission_read(self):
        with patch.object(accept,"require_production_host",side_effect=RuntimeError("guard")):
            with self.assertRaisesRegex(RuntimeError,"guard"):
                accept.main(["--admission","/missing","--admission-sha256","0"*64,"--expected-head","f"*40,"--run-dir","/missing"])

    def test_zero_requests_and_tight_limits_are_literal(self):
        self.assertEqual(accept.CAPS["http_attempts"],0)
        self.assertEqual(accept.CAPS["all_new_artifact_bytes"],262144)
        self.assertEqual(accept.CAPS["single_new_artifact_bytes"],65536)
        self.assertEqual(accept.CAPS["input_read_bytes"],16*1024**3)

    def test_live_saved_birth_blocks_with_valid_current_boot(self):
        birth={"pid":10,"birth_ticks":20,"boot_id":BOOT}
        with self.assertRaisesRegex(Exception,"saved_birth_live"):
            accept.absent(birth,{10:{"birth_ticks":20}},BOOT)

    def test_charged_input_reads_fail_closed_without_truncation(self):
        r=accept.Reader(lambda:None);r.bytes=accept.CAPS["input_read_bytes"]
        with self.assertRaisesRegex(Exception,"input_read_limit"):r.charge(1)

    def test_single_output_cap_and_total_new_artifacts_both_enforced(self):
        with tempfile.TemporaryDirectory() as folder:
            output=accept.Output(Path(folder),0,1,lambda:None)
            with self.assertRaisesRegex(Exception,"new_stage_artifact_limit"):
                output.write("oversized.json",{"value":"x"*65536})
            output=accept.Output(Path(folder),262141,1,lambda:None)
            with self.assertRaisesRegex(Exception,"new_stage_artifact_limit"):output.write("over.json",{})

    def test_exact_own_ancestry_only_is_allowed_not_executable_name(self):
        item={"pid":10,"parent_pid":0,"birth_ticks":20,"full_exe":"/fixture/python","comm":"python",
            "group":10,"session":10,"state":"R","uid":1000,"program":None,"user_service":None}
        bound_chain=accept.own_ancestry({10:item},10)
        records,_=accept.classify({10:item},bound_chain);self.assertEqual(len(records),1)
        with self.assertRaisesRegex(Exception,"acceptor_ancestry_changed"):
            accept.classify({10:item|{"birth_ticks":21}},bound_chain)

    def test_exhausted_reader_cannot_read_a_journal_byte(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder).resolve()/"journal";p.write_bytes(b"{}\n");binding=bound(p)
            r=accept.Reader(lambda:None);r.bytes=accept.CAPS["input_read_bytes"]
            class Tracked(io.BytesIO):
                calls=0
                def readline(self,size):self.calls+=1;return super().readline(size)
            stream=Tracked(b"{}\n")
            with patch.object(Path,"open",return_value=stream):
                with self.assertRaisesRegex(Exception,"input_read_limit"):list(r.lines(binding))
                self.assertEqual(stream.calls,0)

    def test_local_read_exception_keeps_entire_pre_authorized_allowance(self):
        r=accept.Reader(lambda:None)
        def fail(_):raise OSError("unused error text")
        with self.assertRaises(OSError):r.bounded(fail,7)
        self.assertEqual(r.bytes,7)

    def test_completed_short_local_reads_charge_observed_not_full_line_cap(self):
        r=accept.Reader(lambda:None);self.assertEqual(r.bounded(lambda n:b"abc",1024**2),b"abc")
        self.assertEqual(r.bytes,3)

    def test_git_cannot_start_when_physical_read_budget_is_exhausted(self):
        r=accept.Reader(lambda:None);r.bytes=accept.CAPS["input_read_bytes"]
        with patch.object(accept.subprocess,"Popen") as spawn,self.assertRaisesRegex(Exception,"input_read_limit"):
            accept.git_bytes(r,["rev-parse","HEAD"])
        spawn.assert_not_called()


if __name__=="__main__":unittest.main()
