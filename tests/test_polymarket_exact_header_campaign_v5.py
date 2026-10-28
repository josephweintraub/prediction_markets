"""Synthetic paced continuation fixtures. No API or production inputs."""
from contextlib import ExitStack,nullcontext
import copy
import inspect
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_exact_header_campaign_v5 as full
from tests import test_polymarket_exact_header_campaign_v4 as prior
from tests import test_polymarket_exact_header_campaign_v3 as fixture


class ContinuationFixture(prior.RecoveryTests):
    # Reuse setup utilities without inheriting the older test cases.
    def make_inputs(self):
        self.K=10000
        return fixture.FullCampaignTests.make_inputs(self)

    def setUp(self):
        prior.RecoveryTests.setUp(self)
        self.admission["prior"]["rpc"]["elapsed_seconds"]=13200
        number=[0]
        def effects(payload,allowance,timeout):
            if isinstance(payload,list):
                number[0]+=1
                return self.error_reply(payload,[429]*12 if number[0]==97 else [])
            raw=full.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,
                "error_code":None,"proven_oversize":False}
        self.effects=effects
        result=prior.RecoveryTests.execute(self)
        self.assertEqual(result["error_code"],"rpc_item_error_not_retryable")
        self.v4_root=self.output;self.v4_admission=self.admission_path
        owner=json.loads((self.v4_root/"OWNER.json").read_bytes())
        self.source["files"]={name:dict(item) for name,item in owner["source"]["files"].items()}
        for name in full.SOURCE_FILES[len(full.previous.SOURCE_FILES):]:
            self.blobs[name]=(full.REPO/name).read_bytes()
            self.source["files"][name]={"bytes":len(self.blobs[name]),"sha256":full.digest(self.blobs[name])}
        refs={name:self.ref(self.v4_root/path) for name,path in (("owner","OWNER.json"),("failed","FAILED.json"),
            ("root_journal","rpc.jsonl"),("chunk_journal","chunks/c000000/a02/rpc.jsonl"),
            ("diagnostic","chunks/c000000/a02/error_r000000318.json"))}
        refs["admission"]=self.ref(self.v4_admission)
        failed=json.loads((self.v4_root/"FAILED.json").read_bytes())
        printed=copy.deepcopy(failed);printed["usage"]["new_retained_bytes"]["control"]+=refs["failed"]["bytes"]
        refs["stdout"]=self.save(self.root/"v4_stdout.log",printed)
        child={**owner["process"],"process_group":123,"session":123}
        caller={**owner["process"],"pid":122,"process_group":122,"session":122}
        common={"schema_version":"polymarket_oct1_stage_launcher_v1","stage":"headers",
            "output_dir":str(self.v4_root),"admission":owner["admission"],"child":child,"caller":caller,
            "command":["fixture"],"ownership_unknown":False,"final_sample_admitted":False}
        exit_value={**common,"producer_exit_code":1,"owned_group_drained":True,"children_still_running":False,
            "source_and_admission_unchanged":True,"stdout_eof_observed":True,"stdout_truncated":False,
            "stdout_sha256":refs["stdout"]["sha256"],"stdout_bytes_observed":refs["stdout"]["bytes"],
            "elapsed_seconds":2.0}
        refs["exit"]=self.save(self.root/"v4_exit.json",exit_value)
        refs["launch"]=self.save(self.root/"v4_launch.json",common)
        refs["root_drain"]=self.save(self.root/"v4_root_drain.json",{
            "schema_version":"polymarket_header_v4_root_drain_v1","source_head":prior.NEXT,
            "header_owned_processes_drained":True,"remaining_owned_group_members":[],"final_sample_admitted":False,
            "elapsed_seconds":0.01,
            "process_receipts_checked":0,"references":[refs[name] for name in ("failed","exit","diagnostic")],
            "owners":[{"current":None,"owner":exit_value[name]} for name in ("caller","child")]})
        self.v4_refs=refs
        self.v4_cumulative={key:failed["usage"][key] for key in full.V4_INHERITED_RPC}
        self.v4_cumulative["elapsed_seconds"]=14400
        self.v4_new={key:self.v4_cumulative[key]-full.V4_INHERITED_RPC[key] for key in full.V4_NEW_RPC}
        _,starts,finishes=full.journal_usage(Path(refs["chunk_journal"]["path"]))
        span=(max(item["t"] for item in finishes.values())-min(item["t"] for item in starts.values()))/1000
        self.charge={"schema_version":"polymarket_header_v4_charge_attestation_v1",
            "status":"root_verified_v4_failure_charges","source_head":prior.NEXT,"run_dir":str(self.v4_root),
            "refs":{name:refs[name] for name in ("owner","failed","root_drain","exit","root_journal","chunk_journal")},
            "global_new_runtime_seconds":1200,"chunk_runtime_seconds":{"0":120},
            "inherited_rpc":dict(full.V4_INHERITED_RPC),"cumulative_rpc":self.v4_cumulative,
            "raw_clocks_only":True,"final_sample_admitted":False,"checked_utc":"2026-10-27T21:00:00+00:00",
            "observed_caller_elapsed_seconds":2.0,"observed_chunk_journal_span_seconds":span}
        self.charge_path=self.root/"charge.json"
        self.admission["prior"]["v4_charge_attestation"]=self.save(self.charge_path,self.charge)
        self.admission["prior"]["roots"].append(str(self.v4_root))
        for name,item in refs.items():
            self.admission["prior"]["files"].append(item|{"category":"control",
                "chunk":0 if name in {"chunk_journal","diagnostic"} else None})
            if name not in {"owner","failed","root_journal","chunk_journal","diagnostic"}:
                self.admission["prior"]["standalone_control_files"].append(item["path"])
        self.admission["prior"]["files"].append(self.admission["prior"]["v4_charge_attestation"]|
            {"category":"control","chunk":None})
        self.admission["prior"]["standalone_control_files"].append(str(self.charge_path))
        self.admission["prior"]["rpc"]=dict(self.v4_cumulative)
        self.output=self.root/"v5";self.admission_path=self.root/"v5_admission.json"
        self.admission.update(schema_version=full.SCHEMA,run_dir=str(self.output),conservative_new_runtime_seconds=5000)
        self.v4_head=prior.NEXT
        self.calls,self.sleeps,self.effects=[],[],None

    def execute(self,source_effect=None,transport=None):
        run=full.run_campaign
        contexts=(patch.object(full,"RUN_ROOT",self.root),patch.object(full,"V4_ROOT",self.v4_root),
            patch.object(full,"V4_REFS",self.v4_refs),patch.object(full,"V4_HEAD",self.v4_head),
            patch.object(full,"V4_CUMULATIVE_RPC",self.v4_cumulative),patch.object(full,"V4_NEW_RPC",self.v4_new),
            patch.object(full,"owner_identity",return_value={"hostname":"fixture","boot_id":fixture.BOOT,"pid":123,"birth_ticks":456}),
            patch.object(full,"source_snapshot",side_effect=source_effect,return_value=self.source),
            patch.object(full.previous,"run_campaign",side_effect=run),
            patch.object(full,"request_deadline",return_value=nullcontext()))
        with ExitStack() as stack:
            for context in contexts:stack.enter_context(context)
            return prior.RecoveryTests.execute(self,transport=transport)

    def refresh_charge(self):
        item=self.save(self.charge_path,self.charge);self.admission["prior"]["v4_charge_attestation"]=item
        next(row for row in self.admission["prior"]["files"] if row["path"]==str(self.charge_path)).update(item)


# Export no inherited older fixtures: only this class is a unittest TestCase.
class V5ContinuationTests(unittest.TestCase):
    def setUp(self):
        self.case=ContinuationFixture(methodName="runTest");self.case.setUp();self.addCleanup(self.case.doCleanups)

    def test_full_unsealed_inventory_refetched_and_old_attempts_charges_preserved(self):
        c=self.case;result=c.execute();self.assertEqual(result["status"],full.SUCCESS,result)
        self.assertEqual(result["accepted_keys"],10000)
        batches=[rows for rows in c.calls if isinstance(rows,list)]
        self.assertEqual([int(row["params"][0],16) for rows in batches for row in rows],list(range(1,10001)))
        self.assertTrue((c.output/"chunks/c000000/a03/COMPLETE.json").is_file())
        owner=json.loads((c.output/"OWNER.json").read_bytes())
        self.assertEqual(owner["schema_version"],"polymarket_exact_header_owner_v5")
        self.assertEqual(result["usage"]["prior_rpc"]["elapsed_seconds"],14400)
        self.assertEqual(result["usage"]["http_attempts"],c.v4_cumulative["http_attempts"]+102)

    def test_charge_fields_and_cumulative_floors_fail_before_dispatch(self):
        c=self.case;original=copy.deepcopy(c.charge)
        for mutate in (lambda x:x.update(global_new_runtime_seconds=1199),
                lambda x:x.update(chunk_runtime_seconds={"0":119}),lambda x:x.update(source_head="f"*40),
                lambda x:x["cumulative_rpc"].update(elapsed_seconds=13200),lambda x:x.update(extra=True)):
            c.charge=copy.deepcopy(original);mutate(c.charge);c.refresh_charge()
            with self.assertRaises(full.Blocked):c.execute()
            self.assertFalse(c.calls);self.assertFalse(c.output.exists())

    def test_each_old_v4_evidence_mutation_blocks_before_dispatch(self):
        c=self.case
        for name in ("owner","failed","root_journal","chunk_journal","diagnostic","stdout","exit","root_drain"):
            path=Path(c.v4_refs[name]["path"]);raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            with self.assertRaises(full.Blocked):c.execute()
            self.assertFalse(c.calls);self.assertFalse(c.output.exists());path.write_bytes(raw)

    def test_extra_v4_body_or_false_owner_drain_is_not_adopted(self):
        c=self.case;extra=c.v4_root/"headers.parquet";extra.write_bytes(b"unsealed")
        with self.assertRaisesRegex(full.Blocked,"v4_failed_tree"):c.execute()
        self.assertFalse(c.calls);extra.unlink()
        c.owner_status="unknown"
        with self.assertRaisesRegex(full.Blocked,"v4_owner_live_or_unknown"):c.execute()
        self.assertFalse(c.calls)

    def test_old_v4_producer_epoch_and_committed_blob_are_not_waived(self):
        c=self.case
        c.v4_head="f"*40
        with self.assertRaisesRegex(full.Blocked,"v4_owner_or_inputs_differs"):c.execute()
        c.v4_head=prior.NEXT
        name="scripts/collect_polymarket_exact_header_campaign_v4.py"
        c.blobs[name]=b"different committed producer bytes"
        with self.assertRaisesRegex(full.Blocked,"producer_committed_blob_differs"):c.execute()
        self.assertFalse(c.calls);self.assertFalse(c.output.exists())

    def test_all_cumulative_counter_and_runtime_floors_are_preserved(self):
        c=self.case
        for key in c.v4_cumulative:
            previous=c.admission["prior"]["rpc"][key]
            c.admission["prior"]["rpc"][key]=previous-1
            with self.assertRaisesRegex(full.Blocked,"prior_v5_rpc_understated"):c.execute()
            c.admission["prior"]["rpc"][key]=previous
            self.assertFalse(c.calls);self.assertFalse(c.output.exists())

    def test_infeasible_paced_forecast_stops_before_output_or_api(self):
        c=self.case;c.admission["conservative_new_runtime_seconds"]=100
        with self.assertRaisesRegex(full.Blocked,"runtime_feasibility_not_admitted"):c.execute()
        self.assertFalse(c.calls);self.assertFalse(c.output.exists())

    def test_preflight_wall_time_is_included_in_feasibility_before_api(self):
        c=self.case;c.admission["conservative_new_runtime_seconds"]=full.CAPS["runtime_seconds"]-14400-10
        original=full.validate_inputs
        def delayed(*args):
            result=original(*args);c.tick+=20;return result
        with patch.object(full,"validate_inputs",side_effect=delayed):
            with self.assertRaisesRegex(full.Blocked,"runtime_feasibility_not_admitted"):c.execute()
        self.assertFalse(c.calls);self.assertFalse(c.output.exists())

    def test_slow_sealed_progress_stops_before_next_chunk_without_losing_seal(self):
        c=self.case;keys=[{"block_number":n,"block_hash":None} for n in range(1,101)]
        original=full.Budget.seal
        def seal(budget,path,*args,**kwargs):
            result=original(budget,path,*args,**kwargs)
            if path.startswith("chunks/"):c.tick+=3000
            return result
        with patch.object(full,"key_chunks",return_value=iter([keys,keys])),patch.object(full.Budget,"seal",seal):
            result=c.execute()
        self.assertEqual(result["error_code"],"remaining_runtime_not_fit",result)
        self.assertEqual(result["accepted_keys"],100)
        self.assertTrue((c.output/"chunks/c000000/a03/COMPLETE.json").exists())
        self.assertEqual(len([row for row in c.calls if isinstance(row,list)]),1)

    def test_late_charge_and_old_journal_mutations_withhold_completion(self):
        c=self.case
        def changed(_):
            if (c.output/".COMPLETE.json.pending").exists():
                path=Path(c.v4_refs["chunk_journal"]["path"]);raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            return c.source
        result=c.execute(source_effect=changed)
        self.assertEqual(result["error_code"],"input_identity_changed",result)
        self.assertFalse((c.output/"COMPLETE.json").exists())


class PacedRPCTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.tick=0.0;self.calls=[];self.sleeps=[]
        self.prior={"bytes":dict.fromkeys(("baseline","accepted","control","failed"),0),"files":{},
            "rpc":dict.fromkeys(("http_attempts","method_items","response_bytes","elapsed_seconds"),0),
            "observed_max_call_seconds":0.127}
        self.b=full.Budget(self.root,self.prior,0,0);self.b.next_dispatch_at=0;self.b.remaining_keys=10000
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        for context in (patch.object(full.inventory,"resource_state",return_value=fixture.RESOURCES),
                patch.object(full.time,"monotonic",side_effect=lambda:self.tick),
                patch.object(full.time,"sleep",side_effect=self.sleep),
                patch.object(full,"request_deadline",return_value=nullcontext())):self.stack.enter_context(context)

    def sleep(self,seconds):self.sleeps.append(seconds);self.tick+=seconds

    def transport(self,endpoint,payload,*,byte_limit,timeout):
        self.calls.append((self.tick,payload));self.tick+=0.01
        rows=payload if isinstance(payload,list) else [payload]
        values=[{"jsonrpc":"2.0","id":row["id"],"result":"0x89" if row["method"]=="eth_chainId"
            else fixture.rpc_header(int(row["params"][0],16))} for row in rows]
        raw=full.encode(values if isinstance(payload,list) else values[0])
        return {"raw":raw,"charged_bytes":len(raw),"status":200,"complete":True,"error_code":None,"proven_oversize":False}

    def keys(self,first=1,count=100):return [{"block_number":n,"block_hash":None} for n in range(first,first+count)]

    def errors(self,codes):
        def transport(endpoint,payload,**kwargs):
            self.calls.append((self.tick,payload));self.tick+=0.01
            raw=full.encode([{"jsonrpc":"2.0","id":row["id"],"error":{"code":codes[i%len(codes)],
                "message":"SECRET_KEY https://rpc.invalid/SECRET_KEY"}} for i,row in enumerate(payload)])
            return {"raw":raw,"charged_bytes":len(raw),"status":200,"complete":True,
                "error_code":None,"proven_oversize":False}
        return transport

    def test_shared_start_to_start_pacing_includes_chain_and_new_instances(self):
        full.RPC("private",self.b,self.transport,"one.jsonl").call(self.keys())
        full.RPC("private",self.b,self.transport,"two.jsonl").call(self.keys(101,50))
        full.RPC("private",self.b,self.transport,"three.jsonl").call()
        self.assertAlmostEqual(self.calls[0][0],0)
        self.assertAlmostEqual(self.calls[1][0],100/150)
        self.assertAlmostEqual(self.calls[2][0],150/150)
        self.assertEqual(self.b.rpc["method_items"],151)

    def test_no_catchup_after_delay_or_start_journal_overhead(self):
        full.RPC("private",self.b,self.transport,"one.jsonl").call(self.keys())
        self.tick=10
        full.RPC("private",self.b,self.transport,"two.jsonl").call(self.keys(101))
        full.RPC("private",self.b,self.transport,"three.jsonl").call(self.keys(201))
        self.assertAlmostEqual(self.calls[1][0],10)
        self.assertAlmostEqual(self.calls[2][0],10+100/150)

    def test_actual_dispatch_mark_is_after_start_journal_fsync_not_before(self):
        append=self.b.append;first=[True]
        def slow(relative,item,**kwargs):
            result=append(relative,item,**kwargs)
            if item["e"]=="s" and first[0]:first[0]=False;self.tick+=2
            return result
        with patch.object(self.b,"append",side_effect=slow):
            full.RPC("private",self.b,self.transport,"one.jsonl").call(self.keys())
            full.RPC("private",self.b,self.transport,"two.jsonl").call(self.keys(101))
        self.assertAlmostEqual(self.calls[0][0],2)
        self.assertAlmostEqual(self.calls[1][0],2+100/150)

    def test_numeric429_retries_exact_batch_three_times_with_2_4_backoff_no_split(self):
        rpc=full.RPC("private",self.b,self.errors([429]),"rpc.jsonl")
        with self.assertRaisesRegex(full.Blocked,"retryable_rpc_item_error"):rpc.call(self.keys())
        self.assertEqual(len(self.calls),3);self.assertEqual(rpc.batch_size,100)
        self.assertEqual(self.sleeps,[2.0,4.0]);self.assertEqual(self.b.rpc["method_items"],300)
        self.assertEqual([[row["params"] for row in rows] for _,rows in self.calls],[self.calls[0][1] and
            [row["params"] for row in self.calls[0][1]]]*3)
        for path in self.root.rglob("*"):
            if path.is_file():self.assertNotIn(b"SECRET_KEY",path.read_bytes())

    def test_mixed429_batch_then_success_retries_all_keys_and_adopts_only_complete_batch(self):
        error=self.errors([429]);good=self.transport;first=[True]
        def transport(endpoint,payload,**kwargs):
            if first[0]:
                first[0]=False;reply=error(endpoint,payload,**kwargs);rows=full.decode(reply["raw"])
                for index,row in enumerate(rows[12:],12):
                    row.pop("error");row["result"]=fixture.rpc_header(index+1)
                reply["raw"]=full.encode(rows);reply["charged_bytes"]=len(reply["raw"]);return reply
            return good(endpoint,payload,**kwargs)
        rows=full.RPC("private",self.b,transport,"rpc.jsonl").call(self.keys())
        self.assertEqual([row["block_number"] for row in rows],list(range(1,101)))
        self.assertEqual([len(batch) for _,batch in self.calls],[100,100]);self.assertEqual(self.sleeps,[2.0])
        error_record=next(self.root.glob("error_r*.json"))
        self.assertEqual(json.loads(error_record.read_bytes())["error_code_counts"],{"429":12})

    def test_http429_uses_same_bounded_backoff_and_other_transients_keep_old_delays(self):
        def http_error(endpoint,payload,**kwargs):
            self.calls.append((self.tick,payload));self.tick+=.01
            return {"raw":b"{}","charged_bytes":2,"status":429,"complete":True,
                "error_code":None,"proven_oversize":False}
        with self.assertRaisesRegex(full.Blocked,"retryable_http_failure"):
            full.RPC("private",self.b,http_error,"http.jsonl").call(self.keys())
        self.assertEqual(self.sleeps,[2.,4.]);self.calls=[];self.sleeps=[]
        with self.assertRaisesRegex(full.Blocked,"retryable_rpc_item_error"):
            full.RPC("private",self.b,self.errors([-32603]),"rpc.jsonl").call(self.keys())
        self.assertEqual([delay for delay in self.sleeps if delay in (.5,1.)],[.5,1.])

    def test_mixed429_and_permanent_or_bool_does_not_retry(self):
        for index,codes in enumerate(([429,-32602],[True],["429"])):
            self.calls=[]
            with self.assertRaises(full.Blocked):full.RPC("private",self.b,self.errors(codes),str(index)+".jsonl").call(self.keys())
            self.assertEqual(len(self.calls),1)

    def test_pacing_wait_checks_whole_and_chunk_deadline_before_dispatch(self):
        self.b.next_dispatch_at=100;self.b.prior["rpc"]["elapsed_seconds"]=full.CAPS["runtime_seconds"]-1
        with self.assertRaisesRegex(full.Blocked,"runtime_limit"):
            full.RPC("private",self.b,self.transport,"rpc.jsonl").call(self.keys())
        self.assertFalse(self.calls);self.assertEqual(self.b.rpc["http_attempts"],0)

    def test_chunk_pacing_and_global_counter_ceiling_never_reset(self):
        self.b.next_dispatch_at=2;self.b.chunk=0;self.b.chunk_started=0
        self.b.chunk_base={**self.b.rpc,"elapsed_seconds":full.CAPS["chunk_runtime_seconds"]-1}
        self.b.chunk_start_rpc=dict(self.b.rpc)
        with self.assertRaisesRegex(full.Blocked,"chunk_runtime_limit"):
            full.RPC("private",self.b,self.transport,"rpc.jsonl").call(self.keys())
        self.assertFalse(self.calls)
        self.b.chunk=None;self.b.next_dispatch_at=0;self.b.rpc["http_attempts"]=full.CAPS["http_attempts"]
        with self.assertRaisesRegex(full.Blocked,"http_attempts_limit"):
            full.RPC("private",self.b,self.transport,"rpc.jsonl").call(self.keys())
        self.assertFalse(self.calls)

    def test_proven_size_split_remains_full_coverage_and_is_runtime_gated(self):
        good=self.transport
        def transport(endpoint,payload,**kwargs):
            if len(payload)>50:
                self.calls.append((self.tick,payload));self.tick+=.01
                raw=full.encode({"jsonrpc":"2.0","id":None,"error":{"code":-32005,"message":"batch size limit exceeded"}})
                return {"raw":raw,"charged_bytes":len(raw),"status":200,"complete":True,
                    "error_code":None,"proven_oversize":False}
            return good(endpoint,payload,**kwargs)
        rows=full.RPC("private",self.b,transport,"rpc.jsonl").fetch(self.keys())
        self.assertEqual([row["block_number"] for row in rows],list(range(1,101)))
        self.assertEqual([len(rows) for _,rows in self.calls],[100,50,50])

    def test_forecast_counts_latency_overlapping_pacing_and_full_key_coverage(self):
        expected=329015*(100/150)+full.FORECAST_RESERVE_SECONDS
        self.assertEqual(full.forecast_seconds(32901442,100,.127),expected)
        self.assertGreater(full.forecast_seconds(32901442,100,1),full.CAPS["runtime_seconds"])
        self.assertLess(expected+14400,full.CAPS["runtime_seconds"])


class EntrypointTests(unittest.TestCase):
    def test_own_source_or_test_mutation_during_final_previous_snapshot_is_blocked(self):
        names=full.SOURCE_FILES[len(full.previous.SOURCE_FILES):]
        for changed_name in names:
            with tempfile.TemporaryDirectory() as directory:
                repo=Path(directory).resolve();blobs={name:b"original literal source\n" for name in names}
                for name,raw in blobs.items():
                    path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
                count=[0];old={"head":"d"*40,"files":{}}
                def snapshot(head):
                    count[0]+=1
                    if count[0]==2:
                        raw=blobs[changed_name];(repo/changed_name).write_bytes(b"X"+raw[1:])
                    return old
                with patch.object(full,"REPO",repo),patch.object(full.previous,"source_snapshot",side_effect=snapshot),\
                     patch.object(full.scale,"git_blob",side_effect=lambda head,name:blobs[name]):
                    with self.assertRaisesRegex(full.Blocked,"source_reopen_changed"):full.source_snapshot("d"*40)
                self.assertEqual(count[0],2)

    def test_new_source_and_test_blobs_extend_unchanged_previous_projection(self):
        head="d"*40
        old={"head":head,"files":{name:{"bytes":1,"sha256":"0"*64} for name in full.previous.SOURCE_FILES}}
        blobs={name:(full.REPO/name).read_bytes() for name in full.SOURCE_FILES[len(full.previous.SOURCE_FILES):]}
        with patch.object(full.previous,"source_snapshot",return_value=old),\
             patch.object(full.scale,"git_blob",side_effect=lambda h,name:blobs[name]):
            actual=full.source_snapshot(head)
            self.assertEqual(full.previous_source(actual),old)
            blobs[full.SOURCE_FILES[-1]]=b"wrong test blob"
            with self.assertRaisesRegex(full.Blocked,"source_blob_differs"):full.source_snapshot(head)

    def test_guard_before_limits_inputs_or_collection_and_unchanged_transport_caps(self):
        with patch.object(full,"require_production_host",side_effect=RuntimeError("guard")),\
             patch.object(full,"run_campaign") as run,patch.object(full,"process_limits") as limits,\
             patch.object(Path,"read_bytes") as read:
            with self.assertRaisesRegex(RuntimeError,"guard"):
                full.main(["--admission","/none","--admission-sha256","0"*64,"--expected-head",prior.NEXT,
                    "--run-dir","/mnt/data/runs/none","--rpc-private-file","/none"])
        run.assert_not_called();limits.assert_not_called();read.assert_not_called()
        self.assertIs(full.http_response,full.previous.http_response);self.assertIs(full.request_deadline,full.previous.request_deadline)
        self.assertEqual(full.CAPS,full.previous.CAPS)

    def test_exact_header_validation_segment_remains_unchanged(self):
        def segment(call):
            source=inspect.getsource(call)
            return source[source.index('                    values = decode(reply["raw"])'):
                source.index('            except Deadline as exc:')]
        self.assertEqual(segment(full.RPC.call),segment(full.previous.RPC.call))

    def test_cli_has_no_new_batch_source_or_cap_overrides(self):
        result=subprocess.run([sys.executable,full.__file__,"--help"],capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        for name in ("--batch","--max-","--endpoint","--workers","--resume","--rate"):
            self.assertNotIn(name,result.stdout)

    @unittest.skipUnless(sys.platform.startswith("linux"),"actual Linux address-space/import admission")
    def test_linux_import_and_address_space_limits_without_rpc_or_production_reads(self):
        program="""from scripts import collect_polymarket_exact_header_campaign_v5 as m
import resource
import pyarrow as pa
with m.process_limits() as debit:
    assert 0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= 8*1024**3
    debit(14400,m.time.monotonic())
    assert pa.array([1,2,3]).to_pylist() == [1,2,3]
"""
        result=subprocess.run([sys.executable,"-c",program],cwd=full.REPO,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)


def load_tests(loader,tests,pattern):
    suite=unittest.TestSuite()
    for case in (V5ContinuationTests,PacedRPCTests,EntrypointTests):suite.addTests(loader.loadTestsFromTestCase(case))
    return suite


if __name__=="__main__":unittest.main()
