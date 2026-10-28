"""Same-volume reboot fixtures, synthetic inputs only; never API/production."""
from contextlib import ExitStack,nullcontext
import ast
import copy
import inspect
import json
import stat
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from scripts import collect_polymarket_exact_header_campaign_v6 as full
from tests import test_polymarket_exact_header_campaign_v5 as old

NEW_BOOT="41823275-3e51-4d7f-9080-0c50aa148763"


class CertificateTests(unittest.TestCase):
    def setUp(self):
        self.c=old.ContinuationFixture(methodName="runTest");self.c.setUp();self.addCleanup(self.c.doCleanups)
        c=self.c;self.oldcert=c.admission["reboot_equivalence"]
        owner4=json.loads(Path(c.v4_refs["owner"]["path"]).read_bytes())
        owner4["process"]["boot_id"]=c.certificate["current_boot_id"]
        c.v4_refs["owner"]=c.save(Path(c.v4_refs["owner"]["path"]),owner4)
        next(item for item in c.admission["prior"]["files"] if item["path"]==c.v4_refs["owner"]["path"]).update(c.v4_refs["owner"])
        self.inputs={name:full.reference(value,full.CAPS["baseline_bytes"]) for name,value in c.inputs.items()}
        self.env=dict(c.reboot_env,boot_id=NEW_BOOT)
        self.newcert=c.root/"header_reboot_equivalence_v6.json"
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        for context in (
            patch.object(full,"OLD_CERTIFICATE",self.oldcert),patch.object(full,"V4_REFS",c.v4_refs),
            patch.object(full.previous,"V4_REFS",c.v4_refs),patch.object(full.previous,"V4_HEAD",c.v4_head),
            patch.object(full.previous.previous,"OLD_BOOT_ID",old.fixture.BOOT),
            patch.object(full.previous.previous,"CURRENT_DATA_DEVICE",c.current_data_device),
            patch.object(full.previous.previous,"OLD_PRIVATE_DEVICE",c.reboot_env["root_device"]),
            patch.object(full.previous.previous,"CURRENT_PRIVATE_DEVICE",c.reboot_env["root_device"]),
            patch.object(full.previous.previous,"PRIOR_SOURCE_HEAD",c.prior_head),
            patch.object(full.legacy,"ORIGINAL_PILOT_INPUT",c.original_ref),
            patch.object(full.legacy,"owner_state",return_value="dead"),
            patch.object(full,"reboot_environment",side_effect=lambda:dict(self.env))):
            self.stack.enter_context(context)
        self.value=full.certificate_value(self.inputs,c.admission["private_file"])
        self.refresh()

    def refresh(self):
        c=self.c;new=c.save(self.newcert,self.value)
        c.admission["reboot_equivalence"]=new
        files=c.admission["prior"]["files"]
        if not any(item["path"]==new["path"] for item in files):
            files.append(new|{"category":"control","chunk":None})
            c.admission["prior"]["standalone_control_files"].append(new["path"])
        else:next(item for item in files if item["path"]==new["path"]).update(new)
        self.ref=new

    def validate(self):
        return full.validate_reboot(self.ref,self.c.admission["prior"],self.inputs,self.c.admission["private_file"])

    def test_new_boot_same_identity_preserves_both_historical_owner_inputs(self):
        actual=self.validate()
        self.assertEqual(actual["environment"]["boot_id"],NEW_BOOT)
        self.assertEqual(actual["old_inputs"],{k:v["old"] for k,v in self.c.certificate["inputs"].items()})
        self.assertEqual(actual["historical_v4_inputs"],{k:v["current"] for k,v in self.c.certificate["inputs"].items()})
        self.assertEqual(actual["original_request"]["current"],self.value["original_pilot_request"]["current"])

    def test_complete_certificate_admits_only_measured_device_reassignment(self):
        original_ref=full.reference;original_identity=full.pilot.identity
        paths={value["path"] for value in self.inputs.values()}|{self.c.original_ref["path"]}
        self.env.update(data_device=66304,root_device=66307)
        self.inputs=copy.deepcopy(self.inputs)
        for value in self.inputs.values():value["identity"]["device"]=66304
        def reference(value,*args):
            result=original_ref(value,*args)
            if result["path"] in paths:result["identity"]["device"]=66304
            if result["path"]==str(self.c.private):result["identity"]["device"]=66307
            return result
        def identity(path):
            result=original_identity(path)
            if str(path) in paths:result["device"]=66304
            if Path(path)==self.c.private:result["device"]=66307
            return result
        with patch.object(full,"reference",side_effect=reference),patch.object(full.pilot,"identity",side_effect=identity):
            self.value=full.certificate_value(self.inputs,self.c.admission["private_file"]);self.refresh()
            actual=self.validate()
        self.assertEqual(actual["environment"]["data_device"],66304)
        self.assertEqual(actual["private_current"]["device"],66307)

    def test_only_device_projection_accepts_new_device_and_rejects_every_other_field(self):
        before={"device":66305,"inode":10,"bytes":100,"mtime_ns":20,"ctime_ns":30,"mode":33152}
        after=dict(before,device=66304)
        full.only_device(before,after,66305,66304)
        for field in before:
            if field=="device":continue
            changed=dict(after);changed[field]+=1
            with self.subTest(field=field),self.assertRaises(full.Blocked):full.only_device(before,changed,66305,66304)
        with self.assertRaises(full.Blocked):full.only_device(before,dict(after,device=True),66305,1)
        with self.assertRaises(full.Blocked):full.only_device(before,dict(after,extra=0),66305,66304)

    def test_wrong_boot_volume_root_or_boolean_device_rejected(self):
        saved=copy.deepcopy(self.value)
        for field,new in (("boot_id","12345678-1234-4234-8234-123456789abc"),("uuid","wrong"),
                          ("serial","wrong"),("device_bytes","1"),("root_uuid","wrong"),("data_device",True)):
            self.value=copy.deepcopy(saved);self.value["current_environment"][field]=new;self.refresh()
            with self.subTest(field=field),self.assertRaises(full.Blocked):self.validate()

    def test_input_roster_or_nondevice_change_is_not_a_generic_waiver(self):
        original=copy.deepcopy(self.value)
        for mutate in (lambda x:x["inputs"].pop("keys"),lambda x:x["inputs"]["keys"]["v4"]["identity"].update(inode=1),
                       lambda x:x["inputs"]["keys"]["current"].update(sha256="0"*64),lambda x:x.update(extra=True)):
            self.value=copy.deepcopy(original);mutate(self.value);self.refresh()
            with self.assertRaises(full.Blocked):self.validate()

    def test_original_request_private_and_old_proof_refs_are_not_waived(self):
        saved=copy.deepcopy(self.value)
        for mutate in (lambda x:x["original_pilot_request"]["current"].update(keys=999),
                       lambda x:x["private_authority"]["current"].update(uid=999),
                       lambda x:x["old_v4_drain"].update(sha256="0"*64),
                       lambda x:x["previous_certificate"].update(sha256="0"*64)):
            self.value=copy.deepcopy(saved);mutate(self.value);self.refresh()
            with self.assertRaises(full.Blocked):self.validate()

    def test_old_certificate_payload_mutation_and_live_owner_block(self):
        raw=Path(self.oldcert["path"]).read_bytes();Path(self.oldcert["path"]).write_bytes(raw[:-1]+b" ")
        with self.assertRaises(full.Blocked):self.validate()
        Path(self.oldcert["path"]).write_bytes(raw)
        with patch.object(full.legacy,"owner_state",return_value="unknown"),self.assertRaises(full.Blocked):self.validate()

    def test_certified_projection_isolated_from_fresh_collection_inputs(self):
        reboot=self.validate();source={"head":"d"*40,"files":{name:{} for name in full.SOURCE_FILES}}
        sentinel={"fresh_prior":True}
        with patch.object(full.previous,"validate_prior",return_value=sentinel) as prior:
            self.assertIs(full.validate_prior(self.c.admission["prior"],self.c.root/"new",source,self.inputs,reboot),sentinel)
            self.assertEqual(prior.call_args.args[3],reboot["historical_v4_inputs"])
            self.assertNotEqual(prior.call_args.args[3],reboot["old_inputs"])
        self.assertEqual(self.inputs,{name:pair["current"] for name,pair in self.value["inputs"].items()})

    def test_proof_must_be_charged_control_exactly_once(self):
        self.c.admission["prior"]["standalone_control_files"].append(self.ref["path"])
        with self.assertRaises(full.Blocked):self.validate()

    def campaign(self,prior_effect=None,repo=None):
        c=self.c
        c.charge["refs"]["owner"]=c.v4_refs["owner"];c.refresh_charge()
        for name in full.SOURCE_FILES[len(full.previous.SOURCE_FILES):]:
            c.blobs[name]=(full.REPO/name).read_bytes()
            c.source["files"][name]={"bytes":len(c.blobs[name]),"sha256":full.digest(c.blobs[name])}
        c.admission["schema_version"]=full.SCHEMA
        with ExitStack() as stack:
            for context in (patch.object(full,"RUN_ROOT",c.root),patch.object(full,"source_snapshot",return_value=c.source),
                    patch.object(full,"owner_identity",return_value={"hostname":"fixture","boot_id":NEW_BOOT,"pid":123,"birth_ticks":456}),
                    patch.object(full,"request_deadline",return_value=nullcontext()),
                    patch.object(full.previous,"run_campaign",side_effect=full.run_campaign)):
                stack.enter_context(context)
            if prior_effect:stack.enter_context(patch.object(full,"validate_prior",side_effect=prior_effect))
            if repo is not None:stack.enter_context(patch.object(full,"REPO",repo))
            result=c.execute()
        return result

    def test_full_synthetic_campaign_uses_last_a03_and_fresh_owner_with_all_old_charges(self):
        c=self.c;result=self.campaign()
        self.assertEqual(result["status"],full.SUCCESS,result)
        self.assertEqual(result["accepted_keys"],10000)
        self.assertTrue((c.output/"chunks/c000000/a03/COMPLETE.json").exists())
        owner=json.loads((c.output/"OWNER.json").read_bytes())
        self.assertEqual(owner["schema_version"],"polymarket_exact_header_owner_v6")
        self.assertEqual(owner["process"]["boot_id"],NEW_BOOT)
        self.assertEqual(owner["inputs"],self.inputs)
        self.assertEqual(result["usage"]["prior_rpc"],c.v4_cumulative)

    def test_final_prior_callback_mutated_admission_withholds_campaign_completion(self):
        original=full.validate_prior;c=self.c
        def prior(*args):
            value=original(*args)
            if (c.output/".COMPLETE.json.pending").exists():
                raw=c.admission_path.read_bytes();c.admission_path.write_bytes(raw[:-1]+b" ")
            return value
        result=self.campaign(prior)
        self.assertEqual(result["status"],"blocked",result)
        self.assertFalse((c.output/"COMPLETE.json").exists())

    def test_final_prior_callback_mutated_current_certificate_withholds_completion(self):
        original=full.validate_prior;c=self.c
        def prior(*args):
            value=original(*args)
            if (c.output/".COMPLETE.json.pending").exists():
                raw=self.newcert.read_bytes();self.newcert.write_bytes(raw[:-1]+b" ")
            return value
        result=self.campaign(prior)
        self.assertEqual(result["status"],"blocked",result)
        self.assertFalse((c.output/"COMPLETE.json").exists())

    def test_final_prior_callback_mutated_own_source_withholds_completion(self):
        original=full.validate_prior;c=self.c;repo=c.root/"source_copy";repo.mkdir()
        for name in full.SOURCE_FILES:
            path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((full.REPO/name).read_bytes())
        def prior(*args):
            value=original(*args)
            if (c.output/".COMPLETE.json.pending").exists():
                path=repo/"scripts/collect_polymarket_exact_header_campaign_v6.py"
                raw=path.read_bytes();path.write_bytes(b"X"+raw[1:])
            return value
        result=self.campaign(prior,repo)
        self.assertEqual(result["status"],"blocked",result)
        self.assertFalse((c.output/"COMPLETE.json").exists())


class InvariantTests(unittest.TestCase):
    def test_rpc_budget_transport_and_retry_boundary_are_identical_objects(self):
        for name in ("CAPS","RPC","Budget","http_response","request_deadline","process_limits","forecast_seconds"):
            self.assertIs(getattr(full,name),getattr(full.previous,name))
        self.assertEqual(full.CAPS["attempts_per_chunk"],3)
        self.assertEqual(full.previous.V4_CUMULATIVE_RPC,{"http_attempts":318,"method_items":22010,
            "response_bytes":174515481,"elapsed_seconds":14400})
        self.assertEqual(full.RATE_LIMIT_DELAYS,(2.,4.))

    def test_orchestration_ast_diff_only_version_environment_and_diagnostic_qualification(self):
        def normalized(function):
            raw=inspect.getsource(function).replace("_v6","_v5").replace("previous.reboot_environment()","reboot_environment()")
            raw=raw.replace("previous.previous.DIAGNOSTIC_","previous.DIAGNOSTIC_")
            if "            # These callbacks" in raw:
                first=raw.index("            # These callbacks")
                last=raw.index("        b.check()",first)
                raw=raw[:first]+raw[last:]
            return ast.dump(ast.parse(raw),include_attributes=False)
        self.assertEqual(normalized(full.run_campaign),normalized(full.previous.run_campaign))

    def test_new_source_late_mutation_fails_snapshot(self):
        names=full.SOURCE_FILES[len(full.previous.SOURCE_FILES):]
        for changed in names:
            with tempfile.TemporaryDirectory() as directory:
                repo=Path(directory).resolve();literal={name:b"immutable literal source\n" for name in names}
                for name,raw in literal.items():(repo/name).parent.mkdir(parents=True,exist_ok=True);(repo/name).write_bytes(raw)
                count=[0];old_source={"head":"d"*40,"files":{}}
                def snapshot(_):
                    count[0]+=1
                    if count[0]==2:(repo/changed).write_bytes(b"X"+literal[changed][1:])
                    return old_source
                with patch.object(full,"REPO",repo),patch.object(full.previous,"source_snapshot",side_effect=snapshot),\
                     patch.object(full.scale,"git_blob",side_effect=lambda head,name:literal[name]):
                    with self.assertRaises(full.Blocked):full.source_snapshot("d"*40)

    def test_guard_precedes_all_production_inputs(self):
        with patch.object(full,"require_production_host",side_effect=RuntimeError("guard")),\
             patch.object(full,"run_campaign") as run,patch.object(full,"process_limits") as limits:
            with self.assertRaisesRegex(RuntimeError,"guard"):
                full.main(["--admission","/none","--admission-sha256","0"*64,"--expected-head","d"*40,
                    "--run-dir","/mnt/data/runs/new","--rpc-private-file","/none"])
        run.assert_not_called();limits.assert_not_called()

    @unittest.skipUnless(sys.platform.startswith("linux"),"actual Linux address-space/import admission")
    def test_linux_import_and_address_space_without_api_or_production_input(self):
        program="""from scripts import collect_polymarket_exact_header_campaign_v6 as m
import resource
import pyarrow as pa
with m.process_limits() as debit:
    assert 0 < resource.getrlimit(resource.RLIMIT_AS)[0] <= 8*1024**3
    debit(14400,m.time.monotonic())
    assert pa.array([1,2,3]).to_pylist()==[1,2,3]
"""
        value=subprocess.run([sys.executable,"-c",program],cwd=full.REPO,capture_output=True,text=True,timeout=10)
        self.assertEqual(value.returncode,0,value.stderr)


class MountedSourceTests(unittest.TestCase):
    def environment(self,*,source="/dev/nvme2n1",uuid=full.DATA_UUID,serial=full.DATA_SERIAL,fstype="ext4",rdev=66304):
        replies=[SimpleNamespace(stdout=f"{source} {uuid} {fstype}\n".encode(),stderr=b""),
            SimpleNamespace(stdout=f"{serial} {750*1024**3}\n".encode(),stderr=b""),
            SimpleNamespace(stdout=(full.ROOT_UUID+"\n").encode(),stderr=b"")]
        def info(path):
            if str(path)==source:return SimpleNamespace(st_mode=stat.S_IFBLK,st_rdev=rdev)
            return SimpleNamespace(st_dev=66304 if str(path)=="/mnt/data" else 66306)
        with patch.object(full.subprocess,"run",side_effect=replies) as run,patch.object(Path,"lstat",info),\
             patch.object(Path,"stat",info),patch.object(Path,"read_text",return_value=NEW_BOOT):
            value=full.reboot_environment()
            self.assertEqual(run.call_args_list[1].args[0][-1],source)
        return value

    def test_actual_mounted_nvme_source_not_old_nvme_index(self):
        value=self.environment()
        self.assertEqual(value["data_device"],66304);self.assertEqual(value["boot_id"],NEW_BOOT)

    def test_wrong_serial_uuid_fstype_or_mount_rdev_rejected(self):
        for kwargs in ({"serial":"wrong"},{"uuid":"wrong"},{"fstype":"xfs"},{"rdev":1}):
            with self.subTest(kwargs=kwargs),self.assertRaises(full.Blocked):self.environment(**kwargs)


if __name__=="__main__":unittest.main()
