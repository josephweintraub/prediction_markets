"""Bounded synthetic header-only preparation; no API/EC2/real trade data."""
from contextlib import ExitStack
import copy
from datetime import datetime,timezone
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from scripts import prepare_polymarket_exact_header_campaign_v6 as p
from tests import test_polymarket_exact_header_campaign_v5 as old


class PreparationTests(unittest.TestCase):
    def setUp(self):
        c=old.ContinuationFixture(methodName="runTest");c.setUp();self.addCleanup(c.doCleanups);self.c=c
        self.paths={name:c.root/getattr(p,name).name for name in
            ("REQUEST","HOST","QA","LOG","COMMIT","CERT","LEDGER","ADMISSION","CHECK")}
        owner=json.loads(Path(c.v4_refs["owner"]["path"]).read_bytes());owner["process"]["boot_id"]=c.certificate["current_boot_id"]
        c.v4_refs["owner"]=c.save(Path(c.v4_refs["owner"]["path"]),owner)
        c.charge["refs"]["owner"]=c.v4_refs["owner"];c.refresh_charge()
        self.post={name:c.save(c.root/("post_"+name+".json"),{"fixture":name}) for name in ("check","caller","dispatch")}
        self.post["charge"]=c.admission["prior"]["v4_charge_attestation"]
        self.starts=[c.save(c.root/(name+".START.json"),{"fixture":"old untouched START"}) for name in ("old_header","old_dispatch")]
        self.source={"head":old.prior.NEXT,"files":{}}
        for name in p.SOURCE_FILES:
            raw=(p.REPO/name).read_bytes();c.blobs[name]=raw
            self.source["files"][name]={"bytes":len(raw),"sha256":p.digest(raw)}
        self.env=dict(c.reboot_env,boot_id="41823275-3e51-4d7f-9080-0c50aa148763")
        self.host={"schema_version":"polymarket_header_v6_fresh_host_preflight_v1",
            "status":"root_verified_header_recovery_host","checked_utc":datetime.now(timezone.utc).isoformat(),
            "source_head":self.source["head"],"source_clean":True,"instance_id":"i-0f5b31a268af53938","environment":self.env,
            "available_ram_bytes":10**12,"available_disk_bytes":10**12,"live_owned_births":0,"unknown_owned_births":0,
            "relevant_compute_notebook_transfer_consumers":[],"agents_need_EC2":False,"all_saved_PROCESS_births_checked":True,
            "process_receipts_checked":603,"monitor_ancestry_explicitly_checked":True,"production_stage_running":False}
        host=c.save(self.paths["HOST"],self.host)
        self.paths["LOG"].write_bytes(b"small synthetic canonical Linux pass\n");log=c.ref(self.paths["LOG"])
        self.qa={"schema_version":"polymarket_header_v6_linux_fixture_receipt_v1","status":"passed","source_head":self.source["head"],
            "source_clean_before":True,"source_clean_after":True,"exit_code":0,"command":p.QA_COMMAND,"interpreter":p.QA_COMMAND[0],
            "tests_run":1,"tests_skipped":0,"tested_source_files":self.source["files"],"log":log}
        qa=c.save(self.paths["QA"],self.qa)
        commit=c.save(self.paths["COMMIT"],{"schema_version":"polymarket_header_v6_source_commit_v1",
            "status":"clean_committed_and_pushed","source_head":self.source["head"],"git_status_empty":True,
            "origin_commit_matches":True,"source_files":self.source["files"]})
        self.request={"schema_version":"polymarket_header_v6_preparation_request_v1",
            "status":"root_ready_for_bounded_header_admission_preparation","source_head":self.source["head"],
            "host_proof":host,"linux_qa":qa,"source_commit":commit,"extra_prior_controls":[],"extra_external_controls":[],
            "root_complete_header_control_roster":True}
        self.request_ref=c.save(self.paths["REQUEST"],self.request)

    def refresh(self):
        self.request_ref=self.c.save(self.paths["REQUEST"],self.request)

    def execute(self,*,source_effect=None,write_effect=None,prior_effect=None):
        c=self.c
        with ExitStack() as stack:
            for key,value in {**self.paths,"OP":c.root,"RUN":c.root/"prepared_headers","PRIVATE":c.private,
                "ACTUAL_KEYS":c.K,"OLD_POST":self.post,"OLD_STARTS":self.starts,"QA_COUNT":1}.items():
                stack.enter_context(patch.object(p,key,value))
            for context in (
                patch.object(p,"source_snapshot",side_effect=source_effect,return_value=self.source),
                patch.object(p.m,"OLD_CERTIFICATE",c.certificate_path and c.ref(c.certificate_path)),
                patch.object(p.m,"V4_REFS",c.v4_refs),patch.object(p.m.previous,"V4_REFS",c.v4_refs),
                patch.object(p.m.previous,"V4_ROOT",c.v4_root),patch.object(p.m.previous,"V4_HEAD",c.v4_head),
                patch.object(p.m.previous,"V4_NEW_RPC",c.v4_new),patch.object(p.m.previous,"V4_CUMULATIVE_RPC",c.v4_cumulative),
                patch.object(p.m.previous.previous,"PRIOR_SOURCE_HEAD",c.prior_head),
                patch.object(p.m.previous.previous,"RUN_ROOT",c.root),
                patch.object(p.m.previous.previous,"OBSERVATION_ACCEPTANCE",c.acceptance),
                patch.object(p.m.previous.previous,"FAILED_STDOUT_ECHO",c.failed_stdout_ref),
                patch.object(p.m.previous.previous,"FAILED_SOURCE",c.failed_source_ref),
                patch.object(p.m.previous.previous,"OLD_BOOT_ID",old.fixture.BOOT),
                patch.object(p.m.previous.previous,"CURRENT_DATA_DEVICE",c.current_data_device),
                patch.object(p.m.previous.previous,"OLD_PRIVATE_DEVICE",c.reboot_env["root_device"]),
                patch.object(p.m.previous.previous,"CURRENT_PRIVATE_DEVICE",c.reboot_env["root_device"]),
                patch.object(p.m.legacy,"RUN_ROOT",c.root),patch.object(p.m.legacy,"ORIGINAL_PILOT_INPUT",c.original_ref),
                patch.object(p.m.legacy,"owner_state",return_value="dead"),
                patch.object(p.m.scale,"git_blob",side_effect=lambda head,name:c.blobs[name]),
                patch.object(p.m.inventory,"resource_state",return_value=old.fixture.RESOURCES),
                patch.object(p.m,"reboot_environment",side_effect=lambda:dict(self.env)),
                patch.object(p.m,"RUN_ROOT",c.root)):
                stack.enter_context(context)
            if write_effect:stack.enter_context(patch.object(p,"write",side_effect=write_effect))
            if prior_effect:stack.enter_context(patch.object(p.m,"validate_prior",side_effect=prior_effect))
            return p.prepare(self.paths["REQUEST"],self.request_ref["sha256"],self.source["head"])

    def test_zero_http_actual_toy_validation_preserves_all_charges_and_a03_boundary(self):
        result=self.execute();self.assertEqual(result["provider_requests_sent"],0);self.assertFalse(result["producer_launched"])
        value=json.loads(self.paths["ADMISSION"].read_bytes());check=json.loads(self.paths["CHECK"].read_bytes())
        self.assertEqual(value["schema_version"],p.m.SCHEMA)
        self.assertEqual(value["prior"]["rpc"]["http_attempts"],318)
        self.assertEqual(value["prior"]["rpc"]["method_items"],22010)
        self.assertEqual(value["prior"]["rpc"]["response_bytes"],self.c.v4_cumulative["response_bytes"])
        self.assertGreaterEqual(value["prior"]["rpc"]["elapsed_seconds"],p.runtime_floor(time.time(),reserve=0))
        self.assertTrue(check["first_chunk_is_last_attempt"]);self.assertEqual(check["first_chunk_attempt"],3)
        self.assertEqual(value["prior"]["v4_charge_attestation"],self.post["charge"])
        for original in self.starts:self.assertNotIn(original["path"],value["prior"]["standalone_control_files"])
        ledger=json.loads(self.paths["LEDGER"].read_bytes());actual=sum(x["bytes"] for x in ledger["files"])+self.paths["LEDGER"].stat().st_size
        self.assertEqual(check["external_existing_bytes"],actual);self.assertLessEqual(actual+p.FUTURE,p.EXTERNAL)
        self.assertTrue(all(Path(x["path"]).exists() for x in ledger["files"]))

    def test_old_sources_caps_and_fixed_anchor_not_overridden(self):
        self.assertEqual(p.ANCHOR,"2026-10-28T17:45:44Z");self.assertEqual(p.HARD_SECONDS,1200)
        self.assertEqual(p.PREPARATION_RESERVE_SECONDS,1800);self.assertEqual(p.m.CAPS["runtime_seconds"],259200)
        anchor=datetime.fromisoformat(p.ANCHOR.replace("Z","+00:00")).timestamp()
        self.assertEqual(p.runtime_floor(anchor+1),16201)
        with self.assertRaises(p.Blocked):p.runtime_floor(anchor-1)

    def test_unknown_request_field_or_incomplete_roster_blocks_before_any_output(self):
        self.request["extra"]=True;self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.assertFalse(self.paths["CERT"].exists())

    def test_stale_host_or_unknown_consumer_blocks(self):
        self.host["production_stage_running"]=True
        self.request["host_proof"]=self.c.save(self.paths["HOST"],self.host);self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.assertFalse(self.paths["CERT"].exists())

    def test_linux_wrong_command_count_skip_or_source_map_blocks(self):
        original=copy.deepcopy(self.qa)
        for key,value in (("command",["/bin/false"]),("tests_run",0),("tests_skipped",1),("tested_source_files",{})):
            with self.subTest(key=key):
                qa=copy.deepcopy(original);qa[key]=value
                self.request["linux_qa"]=self.c.save(self.paths["QA"],qa);self.refresh()
                with self.assertRaises(p.Blocked):self.execute()
                self.assertFalse(self.paths["CERT"].exists())

    def test_bad_private_path_or_missing_old_start_rejected(self):
        path=Path(self.starts[0]["path"]);path.unlink()
        with self.assertRaises((p.Blocked,OSError)):self.execute()
        self.assertFalse(self.paths["CHECK"].exists())

    def test_existing_output_is_never_overwritten(self):
        self.paths["ADMISSION"].write_bytes(b"preserve")
        with self.assertRaises(p.Blocked):self.execute()
        self.assertEqual(self.paths["ADMISSION"].read_bytes(),b"preserve")

    def test_shared_external_pool_is_not_reset_per_file_or_nested_source(self):
        with self.assertRaises(p.Blocked):p.external_budget([{"path":"/x","bytes":p.EXTERNAL-p.FUTURE+1}])
        with self.assertRaises(p.Blocked):p.external_budget([{"path":"/x","bytes":0},{"path":"/x","bytes":0}])
        with self.assertRaises(p.Blocked):p.external_budget([{"path":"/"+str(i),"bytes":0} for i in range(256)])

    def test_final_source_mutated_input_or_pending_withholds_check(self):
        def source(_):
            pending=self.paths["CHECK"].with_name("."+self.paths["CHECK"].name+".pending")
            if pending.exists():
                raw=self.paths["HOST"].read_bytes();self.paths["HOST"].write_bytes(raw[:-1]+b" ")
            return self.source
        with self.assertRaises(p.Blocked):self.execute(source_effect=source)
        self.assertFalse(self.paths["CHECK"].exists())

    def test_final_prior_callback_added_untracked_file_withholds_check(self):
        original=p.m.validate_prior
        def prior(*args):
            value=original(*args)
            pending=self.paths["CHECK"].with_name("."+self.paths["CHECK"].name+".pending")
            if pending.exists():(self.c.old_root/"late_untracked").write_bytes(b"unadmitted")
            return value
        with self.assertRaises(p.Blocked):self.execute(prior_effect=prior)
        self.assertFalse(self.paths["CHECK"].exists())

    def test_final_prior_callback_mutated_inherited_owner_withholds_check(self):
        original=p.m.validate_prior
        def prior(*args):
            value=original(*args)
            pending=self.paths["CHECK"].with_name("."+self.paths["CHECK"].name+".pending")
            if pending.exists():
                path=self.c.old_root/"OWNER.json";raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            return value
        with self.assertRaisesRegex(p.Blocked,"final_prior_binding_changed"):self.execute(prior_effect=prior)
        self.assertFalse(self.paths["CHECK"].exists())

    def test_final_pending_body_mutation_withholds_check(self):
        original=p.write
        def write(path,value,reader):
            result=original(path,value,reader)
            if path.name.endswith(".pending"):
                raw=path.read_bytes();path.write_bytes(raw[:-1]+b" ")
            return result
        with self.assertRaises(p.Blocked):self.execute(write_effect=write)
        self.assertFalse(self.paths["CHECK"].exists())

    def test_infeasible_runtime_refuses_before_certificate_creation(self):
        with patch.object(p,"runtime_floor",return_value=p.m.CAPS["runtime_seconds"]):
            with self.assertRaisesRegex(p.Blocked,"paced_runtime_not_fit"):self.execute()
        self.assertFalse(self.paths["CERT"].exists())

    def test_guard_before_production_input_or_limits(self):
        with patch.object(p,"require_production_host",side_effect=RuntimeError("guard")),patch.object(p,"prepare") as prepare:
            with self.assertRaisesRegex(RuntimeError,"guard"):
                p.main(["--preparation-request","/none","--preparation-request-sha256","0"*64,"--expected-head","a"*40])
        prepare.assert_not_called()


if __name__=="__main__":unittest.main()
