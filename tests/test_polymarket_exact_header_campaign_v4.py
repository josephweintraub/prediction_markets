"""Synthetic source-versioned recovery fixtures; no real API/data access."""
from contextlib import nullcontext
import copy
import http.client
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from scripts import collect_polymarket_exact_header_campaign_v4 as recovery
from tests import test_polymarket_exact_header_campaign_v3 as fixture

NEXT = "c"*40


class RecoveryTests(unittest.TestCase):
    ref,save,make_inputs,refresh_ledger = (fixture.FullCampaignTests.ref,fixture.FullCampaignTests.save,
        fixture.FullCampaignTests.make_inputs,fixture.FullCampaignTests.refresh_ledger)
    transport = fixture.FullCampaignTests.transport

    def setUp(self):
        fixture.FullCampaignTests.setUp(self)
        def failure(payload,allowance,timeout):
            if not isinstance(payload,list):
                raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            else:
                raw=recovery.encode([{"jsonrpc":"2.0","id":row["id"],"error":{"code":-32603,"message":"old failure"}}
                    for row in payload])
            return {"raw":raw,"charged_bytes":len(raw),"status":200,"complete":True,"error_code":None,"proven_oversize":False}
        self.effects=failure
        failed=fixture.FullCampaignTests.execute(self)
        self.assertEqual(failed["error_code"],"rpc_reply_shape")
        self.old_root=fixture.FullCampaignTests.prepare_continuation(self,failed)
        self.old_bytes={str(path):path.read_bytes() for path in self.old_root.rglob("*") if path.is_file()}
        self.legacy_source=copy.deepcopy(self.source)
        self.prior_head=fixture.HEAD
        for name in recovery.SOURCE_FILES[len(recovery.legacy.SOURCE_FILES):]:
            self.blobs[name]=(recovery.REPO/name).read_bytes()
            self.source["files"][name]={"bytes":len(self.blobs[name]),"sha256":recovery.digest(self.blobs[name])}
        self.source["head"]=NEXT
        self.admission.update(schema_version=recovery.SCHEMA,consumer_head=NEXT)
        self.admission["prior"]["rpc"]=dict(recovery.PRIOR_FLOOR)
        self.sleeps=[]
        self.make_observation()
        self.make_reboot()
        self.failed_stdout=self.root/"full_header_v1.log"
        self.failed_stdout.write_bytes((self.old_root/"FAILED.json").read_bytes())
        self.failed_stdout_ref=self.ref(self.failed_stdout)
        self.admission["prior"]["standalone_control_files"].append(str(self.failed_stdout))
        self.admission["prior"]["files"].append(self.failed_stdout_ref|{"category":"control","chunk":None})
        self.old_bytes={str(path):path.read_bytes() for path in self.old_root.rglob("*") if path.is_file()}

    def make_reboot(self):
        prior=self.admission["prior"];owner_path=self.old_root/"OWNER.json"
        owner=json.loads(owner_path.read_bytes());current=copy.deepcopy(owner["inputs"])
        self.current_data_device=current["keys"]["identity"]["device"]
        for item in owner["inputs"].values():item["identity"]["device"]=recovery.OLD_DATA_DEVICE
        owner_ref=self.save(owner_path,owner)
        drain_path=Path(prior["campaigns"][0]["drain"]["path"])
        drain=json.loads(drain_path.read_bytes());drain["owner_sha256"]=owner_ref["sha256"]
        drain_ref=self.save(drain_path,drain);prior["campaigns"][0]["drain"]=drain_ref
        for item in prior["files"]:
            if item["path"] in (str(owner_path),str(drain_path)):item.update(self.ref(Path(item["path"])))
        private_identity=recovery.pilot.identity(self.private)
        self.reboot_env={"boot_id":recovery.CURRENT_BOOT_ID,"uuid":recovery.DATA_UUID,"fstype":"ext4",
            "serial":recovery.DATA_SERIAL,"device_bytes":str(750*1024**3),"data_device":self.current_data_device,
            "root_uuid":"0df68d9a-e70c-413e-9a10-736ae3f00a7a","root_device":private_identity["device"]}
        self.certificate={"schema_version":"polymarket_header_reboot_equivalence_v4",
            "status":"root_verified_same_volume_device_reassignment","old_boot_id":fixture.BOOT,
            "current_boot_id":recovery.CURRENT_BOOT_ID,
            "data_volume":{"uuid":recovery.DATA_UUID,"serial":recovery.DATA_SERIAL,"device_bytes":750*1024**3,
                "old_device":recovery.OLD_DATA_DEVICE,"current_device":self.current_data_device},
            "old_owner":owner_ref,"old_drain":drain_ref,
            "inputs":{name:{"old":old,"current":current[name]} for name,old in owner["inputs"].items()},
            "private_authority":{"reference":self.admission["private_file"],"old":private_identity,
                "current":private_identity,"root_uuid":self.reboot_env["root_uuid"]}}
        self.certificate_path=self.root/"header_reboot_equivalence.json"
        self.refresh_certificate()
        prior["standalone_control_files"].append(str(self.certificate_path))
        prior["files"].append(self.admission["reboot_equivalence"]|{"category":"control","chunk":None})

    def refresh_certificate(self):
        captured=self.save(self.certificate_path,self.certificate)
        self.admission["reboot_equivalence"]=captured
        for item in self.admission["prior"]["files"]:
            if item["path"]==str(self.certificate_path):item.update(captured)

    def make_observation(self):
        self.obs=self.root/"diagnostic";self.obs.mkdir()
        start=self.save(self.obs/"START.json",{"fixture":"raw diagnostic only"})
        req=self.save(self.obs/"request.json",[{"jsonrpc":"2.0","id":12110+i,
            "method":"eth_getBlockByNumber","params":[hex(i+1),False]} for i in range(100)])
        raw=self.save(self.obs/"response.raw",[{"jsonrpc":"2.0","id":12110+i,"result":fixture.rpc_header(i+1)} for i in range(100)])
        result={"schema_version":"polymarket_header_error_observation_v2","status":"provider_observation_only",
            "body_complete":True,"http_attempts":1,"method_items":100,"timestamps_adopted":0,
            "campaign_retry_or_resume":False,"final_sample_admitted":False,"bindings_unchanged":True,
            "child":{"owned_child_reaped":True},"response_sha256":raw["sha256"],
            "response_bytes_received":raw["bytes"],"response_bytes_charged":raw["bytes"],"elapsed_seconds":0.01,
            "artifacts":{name:{key:item[key] for key in ("bytes","sha256")} for name,item in
                (("START.json",start),("request.json",req),("response.raw",raw))}}
        result_ref=self.save(self.obs/"RESULT.json",result)
        controls={}
        for name in ("launch","exit","root_admission"):
            controls[name]=self.save(self.root/("diagnostic_"+name+".json"),{"fixture":name})
        accepted={"schema_version":"polymarket_single_header_observation_root_acceptance_v1",
            "http_requests_sent":1,"method_items_sent":100,"timestamps_adopted":0,"collection_restarted":False,
            "final_sample_admitted":False,"original_failure_cause_established":False,
            "producer_and_curl_pids_absent":True,"owned_group_and_session_empty":True,
            "cumulative_http_attempts":220,"cumulative_method_items":12309,"cumulative_response_bytes":101435357,
            "authoritative_cumulative_header_runtime_reservation_seconds":7680,
            "request":req,"raw_body":raw,"result":result_ref,**controls}
        self.acceptance=self.save(self.root/"diagnostic_acceptance.json",accepted)
        prior=self.admission["prior"]
        prior["observations"]=[{"run_dir":str(self.obs),"root_acceptance":self.acceptance}]
        prior["roots"].append(str(self.obs))
        for item in (start,req,raw,result_ref):
            prior["files"].append(item|{"category":"failed" if item==raw else "control","chunk":None})
        for item in (*controls.values(),self.acceptance):
            prior["standalone_control_files"].append(item["path"])
            prior["files"].append(item|{"category":"control","chunk":None})

    def execute(self,source_effect=None,transport=None):
        admission=self.save(self.admission_path,self.admission)
        def blob(head,name):
            self.assertIn(head,(fixture.HEAD,fixture.PRODUCER,NEXT));return self.blobs[name]
        with patch.object(recovery,"RUN_ROOT",self.root),patch.object(recovery.legacy,"RUN_ROOT",self.root),\
             patch.object(recovery.legacy,"ORIGINAL_PILOT_INPUT",self.original_ref),\
             patch.object(recovery,"OBSERVATION_ACCEPTANCE",self.acceptance),\
             patch.object(recovery,"FAILED_STDOUT_ECHO",self.failed_stdout_ref),\
             patch.object(recovery,"PRIOR_SOURCE_HEAD",self.prior_head),\
             patch.object(recovery,"OLD_BOOT_ID",fixture.BOOT),\
             patch.object(recovery,"CURRENT_DATA_DEVICE",self.current_data_device),\
             patch.object(recovery,"OLD_PRIVATE_DEVICE",self.reboot_env["root_device"]),\
             patch.object(recovery,"CURRENT_PRIVATE_DEVICE",self.reboot_env["root_device"]),\
             patch.object(recovery,"reboot_environment",side_effect=lambda:self.reboot_env),\
             patch.object(recovery,"source_snapshot",side_effect=source_effect,return_value=self.source),\
             patch.object(recovery.scale,"git_blob",side_effect=blob),\
             patch.object(recovery.inventory,"resource_state",return_value=fixture.RESOURCES),\
             patch.object(recovery.inventory,"load_rpc_endpoint",return_value=("https://rpc.invalid/SECRET_KEY","private_file")),\
             patch.object(recovery,"owner_identity",return_value={"hostname":"fixture","boot_id":fixture.BOOT,"pid":123,"birth_ticks":456}),\
             patch.object(recovery.legacy,"owner_state",side_effect=lambda _:self.owner_status),\
             patch.object(recovery.time,"monotonic",side_effect=lambda:self.tick),\
             patch.object(recovery.time,"sleep",side_effect=lambda seconds:(self.sleeps.append(seconds),setattr(self,"tick",self.tick+seconds))),\
             patch.object(recovery,"request_deadline",return_value=nullcontext()):
            return recovery.run_campaign(self.admission_path,admission["sha256"],NEXT,self.output,self.private,
                transport=transport or self.transport)

    def assert_blocked(self,result,code):
        self.assertEqual(result["status"],"blocked",result);self.assertEqual(result["error_code"],code)
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_same_full_inventory_is_fetched_without_adopting_unsealed_or_diagnostic_headers(self):
        result=self.execute();self.assertEqual(result["status"],recovery.SUCCESS,result)
        batches=[payload for payload in self.calls if isinstance(payload,list)]
        self.assertEqual([len(items) for items in batches],[100,100,50])
        self.assertEqual([int(item["params"][0],16) for batch in batches for item in batch],list(range(1,self.K+1)))
        self.assertEqual(result["accepted_keys"],self.K)
        self.assertEqual(result["usage"]["http_attempts"],225)
        self.assertEqual(result["usage"]["method_items"],12561)
        self.assertGreaterEqual(result["usage"]["elapsed_seconds"],7680)
        self.assertEqual({path:Path(path).read_bytes() for path in self.old_bytes},self.old_bytes)
        self.assertFalse((self.obs/"COMPLETE.json").exists())
        owner=json.loads((self.output/"OWNER.json").read_bytes())
        self.assertEqual(owner["schema_version"],"polymarket_exact_header_owner_v4")
        self.assertEqual(owner["source"]["head"],NEXT)
        self.assertEqual(owner["reboot_equivalence"]["sha256"],self.admission["reboot_equivalence"]["sha256"])
        self.assertNotEqual(owner["inputs"]["keys"]["identity"]["device"],
            self.certificate["inputs"]["keys"]["old"]["identity"]["device"])

    def test_reboot_certificate_schema_volume_boot_and_private_fields_fail_before_dispatch(self):
        original=copy.deepcopy(self.certificate)
        mutations=(lambda c:c.update(status="not_accepted"),lambda c:c.update(old_boot_id="wrong"),
            lambda c:c.update(current_boot_id="wrong"),lambda c:c["data_volume"].update(uuid="wrong"),
            lambda c:c["data_volume"].update(serial="wrong"),lambda c:c["data_volume"].update(device_bytes=1),
            lambda c:c["data_volume"].update(old_device=True),lambda c:c["private_authority"].update(root_uuid="wrong"),
            lambda c:c["private_authority"]["current"].update(uid=99999),
            lambda c:c["private_authority"]["old"].update(inode=1),lambda c:c["inputs"].pop("keys"))
        for mutate in mutations:
            self.certificate=copy.deepcopy(original);mutate(self.certificate);self.refresh_certificate()
            with self.assertRaises(recovery.Blocked):self.execute()
            self.assertFalse(self.calls);self.assertFalse(self.output.exists())

    def test_reboot_live_boot_volume_root_or_private_device_differs_fail_before_dispatch(self):
        original=dict(self.reboot_env)
        for field,value in (("boot_id","wrong"),("uuid","wrong"),("serial","wrong"),("fstype","xfs"),
                ("device_bytes","1"),("data_device",1),("root_uuid","wrong"),("root_device",1)):
            self.reboot_env={**original,field:value}
            with self.assertRaises(recovery.Blocked):self.execute()
            self.assertFalse(self.calls);self.assertFalse(self.output.exists())

    def test_reboot_every_nondevice_input_field_and_full_sha_are_mandatory(self):
        original=copy.deepcopy(self.certificate)
        for name in original["inputs"]:
            for field in ("inode","bytes","mode","mtime_ns","ctime_ns"):
                self.certificate=copy.deepcopy(original)
                self.certificate["inputs"][name]["old"]["identity"][field]+=1
                self.refresh_certificate()
                with self.assertRaisesRegex(recovery.Blocked,"reboot_input_nondevice_identity_differs"):self.execute()
                self.assertFalse(self.calls)
            self.certificate=copy.deepcopy(original)
            self.certificate["inputs"][name]["old"]["sha256"]="0"*64;self.refresh_certificate()
            with self.assertRaisesRegex(recovery.Blocked,"reboot_input_binding_differs"):self.execute()
            self.assertFalse(self.calls)

    def test_reboot_owner_drain_and_certificate_roster_bindings_are_mandatory(self):
        original=copy.deepcopy(self.certificate)
        for name in ("old_owner","old_drain"):
            self.certificate=copy.deepcopy(original);self.certificate[name]["sha256"]="0"*64;self.refresh_certificate()
            with self.assertRaises(recovery.Blocked):self.execute()
            self.assertFalse(self.calls)
        self.certificate=original;self.refresh_certificate()
        self.admission["prior"]["standalone_control_files"].remove(str(self.certificate_path))
        with self.assertRaisesRegex(recovery.Blocked,"reboot_certificate_not_standalone"):self.execute()
        self.assertFalse(self.calls)

    def test_reboot_certificate_hash_cannot_be_replaced_by_similar_unpinned_json(self):
        self.admission["reboot_equivalence"]["sha256"]="0"*64
        with self.assertRaisesRegex(recovery.Blocked,"reference_changed"):self.execute()
        self.assertFalse(self.calls);self.assertFalse(self.output.exists())

    def test_reboot_all_nine_artifacts_are_rehashed_even_after_earlier_input_validation(self):
        original=recovery.validate_inputs
        def changed(*args):
            value=original(*args);path=Path(self.inputs["scale_journal"]["path"])
            raw=path.read_bytes();path.write_bytes(b"X"+raw[1:]);return value
        with patch.object(recovery,"validate_inputs",side_effect=changed):
            with self.assertRaisesRegex(recovery.Blocked,"reference_changed"):self.execute()
        self.assertFalse(self.calls);self.assertFalse(self.output.exists())

    def test_late_reboot_certificate_or_environment_change_withholds_completion(self):
        def source(_):
            if (self.output/".COMPLETE.json.pending").exists():
                raw=self.certificate_path.read_bytes();self.certificate_path.write_bytes(raw[:-1]+b" ")
            return self.source
        self.assert_blocked(self.execute(source_effect=source),"input_identity_changed")

    def test_current_reboot_binding_is_checked_before_next_dispatch(self):
        original=self.transport
        def transport(*args,**kwargs):
            value=original(*args,**kwargs)
            if len(self.calls)==1:
                raw=self.certificate_path.read_bytes();self.certificate_path.write_bytes(raw[:-1]+b" ")
            return value
        self.assert_blocked(self.execute(transport=transport),"request_input_changed")
        self.assertEqual(len(self.calls),1)

    def test_fixed_failed_stdout_echo_is_reinserted_and_charged_exactly_once(self):
        original=recovery.Budget;captured={}
        def budget(output,prior,*args):
            captured.update(prior);return original(output,prior,*args)
        with patch.object(recovery,"Budget",side_effect=budget):result=self.execute()
        self.assertEqual(result["status"],recovery.SUCCESS,result)
        self.assertIn(str(self.failed_stdout),captured["files"])
        self.assertEqual(captured["files"][str(self.failed_stdout)]["category"],"control")
        expected=sum(item["bytes"] for item in self.admission["prior"]["files"] if item["category"]=="control")
        self.assertEqual(captured["bytes"]["control"],expected)

    def test_fixed_failed_stdout_echo_same_size_mutation_is_rejected_before_dispatch(self):
        raw=self.failed_stdout.read_bytes();self.failed_stdout.write_bytes(raw[:-1]+b" ")
        with self.assertRaisesRegex(recovery.Blocked,"reference_changed"):self.execute()
        self.assertFalse(self.calls);self.assertFalse(self.output.exists())

    def test_fixed_failed_stdout_requires_equal_rostered_failed_source(self):
        path=self.old_root/"FAILED.json";value=json.loads(path.read_bytes());value["accepted_keys"]=1
        capture=self.save(path,value)
        next(item for item in self.admission["prior"]["files"] if item["path"]==str(path)).update(capture)
        with self.assertRaisesRegex(recovery.Blocked,"fixed_failed_stdout_source_differs"):self.execute()
        self.assertFalse(self.calls)

    def test_fixed_failed_stdout_missing_source_or_wrong_category_is_rejected(self):
        entries=self.admission["prior"]["files"]
        self.admission["prior"]["files"]=[item for item in entries if item["path"]!=str(self.old_root/"FAILED.json")]
        with self.assertRaisesRegex(recovery.Blocked,"fixed_failed_stdout_source_missing"):self.execute()
        self.admission["prior"]["files"]=entries
        item=next(item for item in entries if item["path"]==str(self.failed_stdout))
        for field,value in (("category","baseline"),("category","failed"),("chunk",0)):
            previous=item[field];item[field]=value
            with self.assertRaisesRegex(recovery.Blocked,"fixed_failed_stdout_not_rostered"):self.execute()
            item[field]=previous
        self.assertFalse(self.calls)

    def test_fixed_failed_stdout_does_not_allow_renamed_or_extra_protocol_echoes(self):
        extra=self.root/"other.log";extra.write_bytes(self.failed_stdout.read_bytes())
        self.admission["prior"]["standalone_control_files"].append(str(extra))
        self.admission["prior"]["files"].append(self.ref(extra)|{"category":"control","chunk":None})
        with self.assertRaisesRegex(recovery.Blocked,"standalone_protocol_artifact_forbidden"):self.execute()
        self.admission["prior"]["standalone_control_files"].remove(str(self.failed_stdout))
        self.admission["prior"]["files"]=[item for item in self.admission["prior"]["files"] if item["path"]!=str(self.failed_stdout)]
        with self.assertRaisesRegex(recovery.Blocked,"fixed_failed_stdout_not_rostered"):self.execute()
        self.assertFalse(self.calls)

    def test_late_failed_stdout_echo_mutation_withholds_completion(self):
        def source(_):
            if (self.output/".COMPLETE.json.pending").exists():
                raw=self.failed_stdout.read_bytes();self.failed_stdout.write_bytes(raw[:-1]+b" ")
            return self.source
        self.assert_blocked(self.execute(source_effect=source),"input_identity_changed")

    def error_reply(self,payload,codes):
        values=[]
        for index,item in enumerate(payload):
            if index<len(codes):values.append({"jsonrpc":"2.0","id":item["id"],
                "error":{"code":codes[index],"message":"SECRET_KEY https://rpc.invalid/SECRET_KEY"}})
            else:values.append({"jsonrpc":"2.0","id":item["id"],"result":fixture.rpc_header(int(item["params"][0],16))})
        raw=recovery.encode(list(reversed(values)))
        return {"raw":raw,"charged_bytes":len(raw),"status":200,"complete":True,"error_code":None,"proven_oversize":False}

    def test_mixed_transient_errors_retry_complete_batch_and_do_not_accept_partial_results(self):
        failed=[False]
        def effects(payload,allowance,timeout):
            if isinstance(payload,list) and not failed[0]:failed[0]=True;return self.error_reply(payload,[-32603,-32002,-32005])
            if isinstance(payload,list):return self.error_reply(payload,[])
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects;result=self.execute()
        self.assertEqual(result["status"],recovery.SUCCESS,result)
        batches=[items for items in self.calls if isinstance(items,list)]
        self.assertEqual([item["params"] for item in batches[0]],[item["params"] for item in batches[1]])
        diagnostics=list(self.output.rglob("error_r*.json"));self.assertEqual(len(diagnostics),1)
        saved=json.loads(diagnostics[0].read_bytes())
        self.assertEqual(saved["error_code_counts"],{"-32603":1,"-32002":1,"-32005":1})
        self.assertFalse(saved["provider_text_retained"])
        for path in self.output.rglob("*"):
            if path.is_file():self.assertNotIn(b"SECRET_KEY",path.read_bytes())

    def test_transient_error_exhaustion_is_three_attempts_same_keys_and_never_seals(self):
        def effects(payload,allowance,timeout):
            if isinstance(payload,list):return self.error_reply(payload,[-32005])
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects;result=self.execute();self.assert_blocked(result,"retryable_rpc_item_error")
        self.assertEqual(len([items for items in self.calls if isinstance(items,list)]),3)
        self.assertEqual(len(list(self.output.rglob("error_r*.json"))),3)
        self.assertEqual(result["usage"]["http_attempts"],224)
        self.assertEqual(self.sleeps,[0.5,1.0])

    def test_permanent_unknown_bool_and_malformed_item_errors_fail_without_retry(self):
        for index,code in enumerate((-32600,-32601,-32602,-32700,-32000,12345,True,"secret")):
            self.output=self.root/("bad_error_"+str(index));self.admission["run_dir"]=str(self.output);self.calls=[]
            def effects(payload,allowance,timeout):
                if isinstance(payload,list):return self.error_reply(payload,[code])
                raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
                return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
            self.effects=effects;result=self.execute()
            self.assert_blocked(result,"rpc_error_schema" if type(code) is not int else "rpc_item_error_not_retryable")
            self.assertEqual(len([items for items in self.calls if isinstance(items,list)]),1)

    def test_every_cumulative_floor_and_missing_observation_control_blocks_before_requests(self):
        for key in recovery.PRIOR_FLOOR:
            previous=self.admission["prior"]["rpc"][key];self.admission["prior"]["rpc"][key]=previous-1
            with self.assertRaisesRegex(recovery.Blocked,"prior_authoritative_charge_understated"):self.execute()
            self.admission["prior"]["rpc"][key]=previous
        self.assertEqual(self.calls,[])
        target=self.acceptance["path"]
        self.admission["prior"]["files"]=[item for item in self.admission["prior"]["files"] if item["path"]!=target]
        with self.assertRaisesRegex(recovery.Blocked,"observation_acceptance_not_rostered"):self.execute()
        self.assertEqual(self.calls,[])

    def test_diagnostic_extra_file_or_reclassified_raw_body_cannot_be_hidden(self):
        extra=self.obs/"headers.parquet";extra.write_bytes(b"not adopted")
        with self.assertRaisesRegex(recovery.Blocked,"observation_roster_or_category"):self.execute()
        extra.unlink()
        next(item for item in self.admission["prior"]["files"] if item["path"]==str(self.obs/"response.raw"))["category"]="accepted"
        with self.assertRaisesRegex(recovery.Blocked,"observation_roster_or_category"):self.execute()
        self.assertEqual(self.calls,[])

    def test_diagnostic_subcaps_fail_closed_without_sealing_or_resetting_rpc_usage(self):
        def effects(payload,allowance,timeout):
            if isinstance(payload,list):return self.error_reply(payload,[-32603])
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects
        with patch.object(recovery,"DIAGNOSTIC_TOTAL_BYTES",1):result=self.execute()
        self.assert_blocked(result,"diagnostic_total_limit")
        self.assertEqual(result["usage"]["http_attempts"],222)

    def test_unproven_error_does_not_shrink_batch(self):
        self.effects=lambda payload,allowance,timeout:self.error_reply(payload,[-32602]) if isinstance(payload,list) else {
            "raw":recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"}),"charged_bytes":len(recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})),
            "complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.assert_blocked(self.execute(),"rpc_item_error_not_retryable")
        self.assertEqual([len(items) for items in self.calls if isinstance(items,list)],[100])

    def test_producer_blob_drift_and_wrong_prior_epoch_are_not_waived(self):
        name=recovery.legacy.SOURCE_FILES[0];original=self.blobs[name];self.blobs[name]=b"drift"
        with self.assertRaisesRegex(recovery.Blocked,"producer_committed_blob_differs"):self.execute()
        self.blobs[name]=original
        self.prior_head="d"*40
        with self.assertRaisesRegex(recovery.Blocked,"reboot_old_owner_or_drain_differs"):self.execute()
        self.assertEqual(self.calls,[])

    def test_live_prior_owner_and_changed_inputs_block_before_output(self):
        self.owner_status="unknown"
        with self.assertRaisesRegex(recovery.Blocked,"prior_owner_live_or_unknown"):self.execute()
        self.assertEqual(self.calls,[])

    def test_only_proven_refusal_splits_and_still_covers_every_key(self):
        def effects(payload,allowance,timeout):
            if isinstance(payload,list) and len(payload)>50:
                raw=recovery.encode({"jsonrpc":"2.0","id":None,"error":{"code":-32005,"message":"batch size limit exceeded"}})
                return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
            if isinstance(payload,list):return self.error_reply(payload,[])
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects;result=self.execute();self.assertEqual(result["status"],recovery.SUCCESS,result)
        self.assertEqual([len(rows) for rows in self.calls if isinstance(rows,list)],[100,50,50,50,50,50])
        self.assertEqual(result["accepted_keys"],self.K)

    def test_duplicate_or_missing_ids_never_become_retryable_item_errors(self):
        for index,mode in enumerate(("duplicate","missing")):
            self.output=self.root/("ids_"+mode);self.admission["run_dir"]=str(self.output);self.calls=[]
            def effects(payload,allowance,timeout):
                if isinstance(payload,list):
                    reply=self.error_reply(payload,[-32603]);values=recovery.decode(reply["raw"])
                    if mode=="duplicate":values[0]["id"]=values[1]["id"]
                    else:values[0]["id"]=987654321
                    reply["raw"]=recovery.encode(values);reply["charged_bytes"]=len(reply["raw"]);return reply
                raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
                return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
            self.effects=effects;result=self.execute()
            self.assert_blocked(result,"rpc_reply_shape" if mode=="duplicate" else "rpc_reply_ids")
            self.assertEqual(len([rows for rows in self.calls if isinstance(rows,list)]),1)

    def test_invalid_available_header_with_transient_other_item_is_not_accepted_or_retried(self):
        def effects(payload,allowance,timeout):
            if isinstance(payload,list):
                reply=self.error_reply(payload,[-32603]);values=recovery.decode(reply["raw"])
                next(item for item in values if "result" in item)["result"]["number"]="0x0"
                reply["raw"]=recovery.encode(values);reply["charged_bytes"]=len(reply["raw"]);return reply
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects;self.assert_blocked(self.execute(),"rpc_returned_block_number_differs")
        self.assertEqual(len([rows for rows in self.calls if isinstance(rows,list)]),1)

    def test_unknown_transport_is_full_charged_three_times_with_closed_durable_intents(self):
        self.effects=lambda *args:(_ for _ in ()).throw(OSError("https://rpc.invalid/SECRET_KEY"))
        result=self.execute();self.assert_blocked(result,"transport_failure")
        self.assertEqual(len(self.calls),3)
        usage,starts,finishes=recovery.journal_usage(self.output/"rpc.jsonl")
        self.assertEqual(usage["response_bytes"],3*recovery.CAPS["individual_response_bytes"])
        self.assertEqual(len(starts),len(finishes))
        for path in self.output.rglob("*"):
            if path.is_file():self.assertNotIn(b"SECRET_KEY",path.read_bytes())

    def test_deadline_prefix_is_hashed_not_published_and_opaque_charges_are_never_refunded(self):
        def effects(*args):
            error=recovery.Deadline("request_deadline");error.raw=b"SECRET_KEY";error.status=200;raise error
        self.effects=effects;result=self.execute();self.assert_blocked(result,"request_deadline")
        usage,starts,finishes=recovery.journal_usage(self.output/"rpc.jsonl")
        self.assertEqual(usage["response_bytes"],3*recovery.CAPS["individual_response_bytes"])
        self.assertTrue(all(item["z"]==10 and item["h"] is None and item["x"]==recovery.digest(b"SECRET_KEY") for item in finishes.values()))
        self.assertEqual(len(starts),len(finishes))
        for path in self.output.rglob("*"):
            if path.is_file():self.assertNotIn(b"SECRET_KEY",path.read_bytes())

    def test_whole_stage_deadline_and_insufficient_backoff_budget_never_retry(self):
        def effects(*args):raise recovery.Deadline("runtime_limit")
        self.effects=effects;self.assert_blocked(self.execute(),"runtime_limit")
        self.assertEqual(len(self.calls),1);self.assertEqual(self.sleeps,[])
        self.output=self.root/"backoff_budget";self.admission["run_dir"]=str(self.output);self.calls=[]
        self.effects=lambda *args:(_ for _ in ()).throw(OSError("temporary"))
        with patch.object(recovery,"RETRY_DELAYS",(300000,300000)):
            self.assert_blocked(self.execute(),"runtime_limit")
        self.assertEqual(len(self.calls),1);self.assertEqual(self.sleeps,[])

    def test_invalid_adapter_flags_and_status_are_opaque_and_never_leak_text(self):
        for index,change in enumerate(({"proven_oversize":None},{"status":"SECRET_KEY"},{"charged_bytes":-1})):
            self.output=self.root/("adapter_"+str(index));self.admission["run_dir"]=str(self.output);self.calls=[]
            self.effects=lambda *args:{"raw":b"prefix","charged_bytes":6,"complete":True,"status":200,
                "error_code":None,"proven_oversize":False,**change}
            result=self.execute();self.assert_blocked(result,"transport_contract")
            usage,starts,finishes=recovery.journal_usage(self.output/"rpc.jsonl")
            self.assertEqual(usage["response_bytes"],recovery.CAPS["individual_response_bytes"])
            self.assertEqual(len(starts),len(finishes))
            for path in self.output.rglob("*"):
                if path.is_file():self.assertNotIn(b"SECRET_KEY",path.read_bytes())

    def test_global_request_and_response_caps_cannot_reset(self):
        for index,(key,code) in enumerate((("http_attempts","http_attempts_limit"),("method_items","method_items_limit"),
                                         ("response_bytes","response_bytes_limit"))):
            self.output=self.root/("cap_"+str(index));self.admission["run_dir"]=str(self.output);self.calls=[]
            previous=self.admission["prior"]["rpc"][key];self.admission["prior"]["rpc"][key]=recovery.CAPS[key]
            self.assert_blocked(self.execute(),code);self.admission["prior"]["rpc"][key]=previous
            self.assertEqual(self.calls,[])

    def test_logical_chunk_http_method_response_and_runtime_caps_include_old_attempt(self):
        self.output.mkdir()
        prior={"bytes":dict.fromkeys(("baseline","accepted","control","failed"),0),"files":{},
            "rpc":dict.fromkeys(("http_attempts","method_items","response_bytes","elapsed_seconds"),0)}
        for key,cap,code in (("http_attempts","chunk_http_attempts","chunk_http_attempts_limit"),
            ("method_items","chunk_method_items","chunk_method_items_limit"),
            ("response_bytes","chunk_response_bytes","response_bytes_limit"),
            ("elapsed_seconds","chunk_runtime_seconds","chunk_runtime_limit")):
            budget=recovery.Budget(self.output,prior,0,0)
            budget.chunk,budget.chunk_started=0,0;budget.chunk_base=dict(prior["rpc"])
            budget.chunk_base[key]=recovery.CAPS[cap];budget.chunk_start_rpc=dict(budget.rpc)
            with patch.object(recovery.inventory,"resource_state",return_value=fixture.RESOURCES),\
                 patch.object(recovery.time,"monotonic",return_value=0):
                with self.assertRaisesRegex(recovery.Blocked,code):
                    recovery.RPC("private",budget,self.transport,"chunks/c000000/a02/rpc.jsonl").call([{"block_number":1,"block_hash":None}])
        self.assertEqual(self.calls,[])

    def test_marker_rename_fault_withholds_completion(self):
        original=recovery.scale.atomic_publish
        def publish(a,b):
            if Path(b)==self.output/"COMPLETE.json":raise OSError("final rename fault")
            return original(a,b)
        with patch.object(recovery.scale,"atomic_publish",publish):result=self.execute()
        self.assert_blocked(result,"header_campaign_runtime_failure")
        self.assertTrue((self.output/".COMPLETE.json.pending").exists())

    def test_late_diagnostic_body_identity_change_blocks_seal(self):
        changed=[False]
        def effects(payload,allowance,timeout):
            if not changed[0]:
                changed[0]=True;path=self.obs/"response.raw";raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            if isinstance(payload,list):return self.error_reply(payload,[])
            raw=recovery.encode({"jsonrpc":"2.0","id":payload["id"],"result":"0x89"})
            return {"raw":raw,"charged_bytes":len(raw),"complete":True,"status":200,"error_code":None,"proven_oversize":False}
        self.effects=effects;self.assert_blocked(self.execute(),"input_identity_changed")

    def test_source_snapshot_binds_new_files_without_weakening_legacy_projection(self):
        with patch.object(recovery.legacy,"source_snapshot",return_value={"head":NEXT,"files":self.legacy_source["files"]}),\
             patch.object(recovery.scale,"git_blob",side_effect=lambda head,name:self.blobs[name]):
            snapshot=recovery.source_snapshot(NEXT)
            self.assertEqual(snapshot,self.source)
            self.blobs[recovery.SOURCE_FILES[-1]]=b"different test bytes"
            with self.assertRaisesRegex(recovery.Blocked,"source_blob_differs"):recovery.source_snapshot(NEXT)

    def test_same_size_late_source_mutation_withholds_completion(self):
        saved=copy.deepcopy(self.source)
        def changed(*_):
            if self.calls:return {**saved,"head":"f"*40}
            return saved
        result=self.execute(source_effect=changed)
        self.assert_blocked(result,"source_changed")

    def test_guard_precedes_real_reads_limits_and_collection(self):
        with patch.object(recovery,"require_production_host",side_effect=RuntimeError("guard")),\
             patch.object(recovery,"run_campaign") as run,patch.object(recovery,"process_limits") as limits,\
             patch.object(Path,"read_bytes") as read:
            with self.assertRaisesRegex(RuntimeError,"guard"):
                recovery.main(["--admission","/none","--admission-sha256","0"*64,"--expected-head",NEXT,
                    "--run-dir","/mnt/data/runs/none","--rpc-private-file","/none"])
        run.assert_not_called();limits.assert_not_called();read.assert_not_called()


class TransportDeadlineTests(unittest.TestCase):
    def test_process_limits_keep_exact_as_ceiling_and_restore_debited_outer_timer(self):
        previous=signal.getsignal(signal.SIGALRM);limits=[(-1,-1)];changes=[]
        def setlimit(kind,value):
            self.assertEqual(kind,resource.RLIMIT_AS);changes.append(value);limits[0]=value
        try:
            signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(AssertionError("outer")))
            signal.setitimer(signal.ITIMER_REAL,1);began=time.monotonic()
            with patch.object(recovery.resource,"getrlimit",side_effect=lambda _:limits[0]),\
                 patch.object(recovery.resource,"setrlimit",side_effect=setlimit):
                with recovery.process_limits() as debit:
                    self.assertEqual(limits[0][0],8*1024**3)
                    debit(7680,began);time.sleep(0.02)
                    with recovery.request_deadline(0.04):time.sleep(0.01)
            remaining,_=signal.getitimer(signal.ITIMER_REAL)
            self.assertLessEqual(remaining,1-(time.monotonic()-began)+0.005)
            self.assertGreater(remaining,0.5)
            self.assertEqual(changes,[(8*1024**3,-1),(-1,-1)])
        finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)

    @unittest.skipUnless(sys.platform.startswith("linux"),"actual Linux address-space admission")
    def test_linux_import_and_actual_resource_startup_without_rpc_or_input_reads(self):
        program="""from scripts import collect_polymarket_exact_header_campaign_v4 as m
import resource
import pyarrow as pa
with m.process_limits() as debit:
    assert 0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= 8*1024**3
    debit(7680,m.time.monotonic())
    assert pa.array([1,2,3]).to_pylist() == [1,2,3]
"""
        result=subprocess.run([sys.executable,"-c",program],cwd=recovery.REPO,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)

    def response(self,mode):
        class Response:
            status=200;headers={}
            def __init__(self):self.reads=0;self.close_attempted=False
            def geturl(self):return "https://rpc.invalid/SECRET_KEY"
            def read(self,count):
                self.reads+=1
                if mode=="read" and self.reads==2:time.sleep(1)
                return b"prefix" if self.reads==1 else b""
            def close(self):
                self.close_attempted=True
                if mode in ("read","close"):time.sleep(1)
        return Response()

    def test_hard_blocking_read_or_close_does_not_swallow_alarm_or_lose_returned_prefix(self):
        for mode in ("read","close"):
            response=self.response(mode)
            class Opener:
                def open(self,req,timeout):return response
            began=time.monotonic()
            with patch.object(recovery.request,"build_opener",return_value=Opener()):
                with self.assertRaises(recovery.Deadline) as caught:
                    with recovery.request_deadline(0.04):
                        recovery.http_response("https://rpc.invalid/SECRET_KEY",[],byte_limit=100,timeout=0.04)
            self.assertLess(time.monotonic()-began,0.3)
            self.assertEqual(caught.exception.raw,b"prefix")
            if mode=="read":self.assertFalse(response.close_attempted)
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))

    def test_hard_open_deadline_and_nested_outer_timer_restore_without_refund(self):
        class Opener:
            def open(self,req,timeout):time.sleep(1)
        previous=signal.getsignal(signal.SIGALRM)
        try:
            signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(AssertionError("outer")))
            signal.setitimer(signal.ITIMER_REAL,1);began=time.monotonic()
            with patch.object(recovery.request,"build_opener",return_value=Opener()):
                with self.assertRaises(recovery.Deadline):
                    with recovery.request_deadline(0.04):
                        recovery.http_response("https://rpc.invalid/SECRET_KEY",[],byte_limit=100,timeout=0.04)
            remaining,_=signal.getitimer(signal.ITIMER_REAL)
            self.assertLessEqual(remaining,1-(time.monotonic()-began)+0.005)
            self.assertGreater(remaining,0.5)
        finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,previous)

    def test_incomplete_read_partial_is_retained_and_full_charged(self):
        response=self.response("normal")
        def read(count):raise http.client.IncompleteRead(b"partial",10)
        response.read=read
        class Opener:
            def open(self,req,timeout):return response
        with patch.object(recovery.request,"build_opener",return_value=Opener()):
            with recovery.request_deadline(1):reply=recovery.http_response("https://rpc.invalid/SECRET_KEY",[],byte_limit=100,timeout=1)
        self.assertEqual(reply["raw"],b"partial");self.assertEqual(reply["charged_bytes"],100)
        self.assertFalse(reply["complete"])

    def test_fixed_caps_and_no_cli_batch_or_budget_overrides(self):
        self.assertEqual(recovery.CAPS,recovery.legacy.CAPS)
        self.assertEqual(recovery.CAPS["initial_batch_size"],100)
        result=subprocess.run([sys.executable,recovery.__file__,"--help"],capture_output=True,text=True)
        self.assertEqual(result.returncode,0)
        for name in ("--batch","--max-","--endpoint","--workers","--resume"):self.assertNotIn(name,result.stdout)


if __name__=="__main__":unittest.main()
