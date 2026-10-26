"""Synthetic reboot identities only; no EC2, real metadata, mount or RPC access."""
from contextlib import redirect_stderr
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import test_plan_polymarket_refresh as fixtures
from scripts import rebind_polymarket_refresh_after_reboot as bridge

inventory, planner = bridge.inventory, bridge.planner
OLD_HEAD, PLAN_HEAD, NEW_HEAD = (letter * 40 for letter in "abc")


class RebootRebindTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.device = bridge.OLD_DEVICE
        original_identity = inventory.path_identity
        def fake_identity(path):
            identity = original_identity(path)
            if identity.get("status") == "present" and self.root in path.resolve().parents:
                for role in ("entry", "target"):
                    identity[role]["device"] = self.device
            return identity
        for target, name, value in ((inventory, "path_identity", fake_identity), (bridge, "MOUNT", self.root)):
            patched = patch.object(target, name, value)
            patched.start()
            self.addCleanup(patched.stop)
        self.paths = {name: self.root / (name + ".parquet") for name in inventory.DEFAULT_PATHS}
        for name in bridge.NAMES:
            pq.write_table(pa.table({"block_number": [2, 5], "timestamp": [20, 50]}), self.paths[name])
        self.repair = self.root / "repair.json"
        self.repair.write_bytes(bridge.encoded({"schema_version": "repair_fixture", "status": "repair_complete"}))
        with patch.object(inventory, "code_state", return_value=fixtures.code(OLD_HEAD)):
            self.old = inventory.build_inventory(self.paths, repair_manifest=self.repair, disk_path=self.root)
        self.inv_path = self.root / "old_inventory.json"
        self.inv_path.write_bytes(bridge.encoded(self.old))
        self.rpc = fixtures.FakeRPC()
        with patch.object(inventory, "code_state", return_value=fixtures.code(PLAN_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")):
            self.old_plan = planner.plan_refresh(self.inv_path, bridge.sha(self.inv_path.read_bytes()), OLD_HEAD,
                                                PLAN_HEAD, transport=self.rpc)
        self.assertEqual(self.old_plan["status"], "source_plan_ready_for_review")
        self.plan_path = self.root / "old_plan.json"
        self.plan_path.write_bytes(bridge.encoded(self.old_plan))
        self.old_binding = {"path": str(self.plan_path), "sha256": bridge.sha(self.plan_path.read_bytes()),
                            "source_head": PLAN_HEAD, "identity": inventory.path_identity(self.plan_path)}
        frozen = {"source_plan": self.old_binding}
        owner = {"run_id": "fixture-run", "plan": frozen, "plan_sha256": bridge.sha(bridge.encoded(frozen))}
        self.campaign_path, self.terminal_path = self.root / "old_campaign.json", self.root / "old_terminal.json"
        self.campaign_path.write_bytes(bridge.encoded(owner))
        self.terminal_path.write_bytes(bridge.encoded({"run_id": owner["run_id"], "plan_sha256": owner["plan_sha256"],
                 "status": "failed_raw_campaign", "children_still_running": False}))
        self.volume = {"schema_version": "polymarket_data_filesystem_expansion_v1", "volume_id": bridge.VOLUME,
                       "uuid": bridge.UUID, "device": bridge.DEVICE, "device_bytes": bridge.CAPACITY, "exit_code": 0,
                       "after": {"filesystem_bytes": 791520346112}, "protected_input_count": 6,
                       "protected_input_identities_unchanged": True,
                       "protected_input_identities": {self.old["inputs"][name]["path"]: self.old["inputs"][name]["files"][0]["identity"]
                                                        for name in bridge.NAMES}}
        self.volume_path = self.root / "volume.json"
        self.volume_path.write_bytes(bridge.encoded(self.volume))
        for name, path in (("OLD_INVENTORY_SHA", self.inv_path), ("OLD_PLAN_SHA", self.plan_path),
                           ("OLD_CAMPAIGN_SHA", self.campaign_path), ("OLD_TERMINAL_SHA", self.terminal_path),
                           ("VOLUME_RECEIPT_SHA", self.volume_path)):
            patched = patch.object(bridge, name, bridge.sha(path.read_bytes()))
            patched.start()
            self.addCleanup(patched.stop)
        self.device = bridge.NEW_DEVICE
        self.storage = {"mount": str(self.root), "device": bridge.DEVICE, "uuid": bridge.UUID,
                        "volume_id": bridge.VOLUME, "serial": bridge.SERIAL, "device_bytes": bridge.CAPACITY,
                        "filesystem_bytes": 791520346112, "linux_device": bridge.NEW_DEVICE,
                        "boot_id": "12345678-1234-1234-1234-123456789abc"}
        self.output = self.root / "admission"

    def produce(self, **kwargs):
        with patch.object(inventory, "code_state", return_value=fixtures.code(NEW_HEAD)), \
                patch.object(inventory, "load_rpc_endpoint", return_value=("https://rpc.invalid/SECRET", "fixture")), \
                patch.object(shutil, "disk_usage", return_value=shutil._ntuple_diskusage(10**12, 0, 10**12)):
            return bridge.produce(self.plan_path, self.campaign_path, self.terminal_path, self.volume_path,
                                  self.output, NEW_HEAD, storage=lambda: copy.deepcopy(self.storage),
                                  transport=kwargs.pop("transport", self.rpc), **kwargs)

    def validate(self, receipt):
        with patch.object(bridge, "capture_storage", return_value=copy.deepcopy(self.storage)):
            return bridge.validate_bridge(self.output / "COMPLETE.json", receipt["bridge"]["sha256"],
                                          self.output / "fresh_source_plan.json", receipt["fresh_plan"]["sha256"], NEW_HEAD)

    def test_fresh_strict_reader_and_narrow_equivalence_preserve_old_bytes(self):
        originals = {path: path.read_bytes() for path in (self.inv_path, self.plan_path, self.campaign_path,
                                                         self.terminal_path, self.volume_path)}
        receipt = self.produce()
        value = self.validate(receipt)
        self.assertEqual(value["old_binding"], self.old_binding)
        self.assertEqual(value["old_plan"], self.old_plan)
        self.assertNotEqual(value["old_binding"], value["fresh_binding"])
        self.assertEqual(value["fresh_plan"]["last_included_header"], self.old_plan["last_included_header"])
        self.assertEqual(value["prep_charge"], {"rpc_calls": 100, "response_bytes": 268435456, "runtime_seconds": 1200,
                                               "output_bytes": value["admission_tree_inventory"]["bytes"]})
        witness = json.loads((self.output / "equivalence.json").read_bytes())
        self.assertEqual(len(witness["allowed_device_transition"]["input_leaves"]), 26)
        self.assertEqual({path:path.read_bytes() for path in originals}, originals)
        self.assertEqual({item.name for item in self.output.iterdir()}, bridge.FILES)
        self.assertNotIn("SECRET", json.dumps(value))

    def test_all_nondevice_identity_fields_and_numeric_types_are_strict(self):
        old = self.old["inputs"]["raw_events"]["files"][0]["identity"]
        current = inventory.path_identity(self.paths["raw_events"])
        bridge.device_equivalent(old, current)
        for role, field, value in (("entry","inode",float(current["entry"]["inode"])),
                                   ("target","device",float(bridge.NEW_DEVICE)),
                                   ("entry","mtime_ns",current["entry"]["mtime_ns"]+1)):
            altered=copy.deepcopy(current);altered[role][field]=value
            with self.subTest(field=field), self.assertRaisesRegex(bridge.RebindBlocked,"nondevice_identity"):
                bridge.device_equivalent(old, altered)

    def test_fresh_footer_and_row_counts_cannot_change_or_become_floats(self):
        fresh=copy.deepcopy(self.old)
        fresh["inputs"],_=bridge.migrate_inputs(self.old["inputs"])
        for role in ("entry","target"):fresh["repair_manifest"]["identity"][role]["device"]=bridge.NEW_DEVICE
        bridge.compare_inventories(self.old,fresh)
        for field,value in (("rows_declared",float(fresh["inputs"]["raw_events"]["files"][0]["rows_declared"])),
                            ("footer_sha256","f"*64)):
            altered=copy.deepcopy(fresh);altered["inputs"]["raw_events"]["files"][0][field]=value
            with self.subTest(field=field),self.assertRaisesRegex(bridge.RebindBlocked,"input_metadata"):
                bridge.compare_inventories(self.old,altered)

    def test_volume_uuid_serial_capacity_device_and_boot_are_not_interchangeable(self):
        for key,value in (("uuid","bad"),("serial","volOTHER"),("device_bytes",bridge.CAPACITY-1),
                          ("linux_device",float(bridge.NEW_DEVICE)),("boot_id","bad")):
            altered=copy.deepcopy(self.storage);altered[key]=value
            with self.subTest(key=key),self.assertRaises(bridge.RebindBlocked):
                bridge.check_storage(self.volume,altered,self.old)

    def test_changed_boot_or_controls_invalidate_completed_certificate(self):
        receipt=self.produce()
        self.storage["boot_id"]="abcdefab-1234-1234-1234-123456789abc"
        with self.assertRaisesRegex(bridge.RebindBlocked,"equivalence_witness"):
            self.validate(receipt)
        self.storage["boot_id"]="12345678-1234-1234-1234-123456789abc"
        self.campaign_path.write_bytes(self.campaign_path.read_bytes()+b" ")
        with self.assertRaisesRegex(bridge.RebindBlocked,"hash_differs"):
            self.validate(receipt)

    def test_changed_input_or_unexpected_missing_source_blocks_before_rpc(self):
        self.paths["raw_events"].touch()
        previous=len(self.rpc.calls)
        with self.assertRaisesRegex(bridge.RebindBlocked,"nondevice_identity"):
            self.produce()
        self.assertFalse((self.output/"COMPLETE.json").exists())
        self.assertEqual(len(self.rpc.calls),previous)

    def test_failed_rpc_attempts_are_reserved_before_transport_without_secret_errors(self):
        budget=bridge.Budget(self.output)
        def bad(*args):raise RuntimeError("SECRET")
        wrapped=budget.transport(bad)
        with self.assertRaises(RuntimeError):wrapped("https://rpc.invalid/SECRET",{"id":1})
        self.assertEqual(budget.calls,1)
        self.assertEqual(budget.responses,65537)
        with patch.object(bridge,"MAX_CALLS",1):
            with self.assertRaisesRegex(bridge.RebindBlocked,"rpc_call_limit"):
                wrapped("hidden",{"id":2})
        self.assertEqual(budget.calls,1)

    def test_native_endpoint_or_wrong_chain_failure_never_publishes_complete(self):
        self.rpc.overrides["eth_chainId"]=lambda *_:"0x1"
        with self.assertRaises(bridge.RebindBlocked):self.produce()
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_existing_output_is_never_adopted(self):
        self.output.mkdir()
        with self.assertRaisesRegex(bridge.RebindBlocked,"already_exists"):self.produce()
        self.assertEqual(list(self.output.iterdir()),[])

    def test_output_mutation_during_final_old_controls_reopen_blocks_complete(self):
        original=bridge.source_controls
        calls=0
        def mutate(*args):
            nonlocal calls
            calls+=1
            value=original(*args)
            if calls==2:
                path=self.output/"fresh_inventory.json"
                path.write_bytes(path.read_bytes()+b" ")
            return value
        with patch.object(bridge,"source_controls",side_effect=mutate):
            with self.assertRaisesRegex(bridge.RebindBlocked,"hash_differs"):
                self.produce()
        self.assertFalse((self.output/"COMPLETE.json").exists())

    def test_extra_admission_files_and_changed_witness_fail(self):
        receipt=self.produce()
        (self.output/"EXTRA.json").write_bytes(b"{}")
        with self.assertRaisesRegex(bridge.RebindBlocked,"tree_differs"):self.validate(receipt)

    def test_deadline_output_and_free_floor_are_checked_before_writes(self):
        budget=bridge.Budget(self.output,clock=lambda:0)
        budget.clock=lambda:bridge.MAX_SECONDS
        with self.assertRaisesRegex(bridge.RebindBlocked,"runtime_limit"):budget.check()
        with patch.object(bridge,"MAX_OUTPUT",1):
            with self.assertRaisesRegex(bridge.RebindBlocked,"output_byte_limit"):
                bridge.Budget(self.output).check(2)
        with patch.object(shutil,"disk_usage",return_value=shutil._ntuple_diskusage(10**12,0,bridge.FREE_FLOOR)):
            with self.assertRaisesRegex(bridge.RebindBlocked,"free_floor"):bridge.Budget(self.output).check(1)

    def test_typed_plan_fields_cannot_change_and_guard_precedes_inputs(self):
        altered=copy.deepcopy(self.old_plan)
        altered["last_included_header"]["number"]=float(altered["last_included_header"]["number"])
        with self.assertRaisesRegex(bridge.RebindBlocked,"scientific_plan"):
            bridge.compare_plans(self.old_plan,altered)
        args=["--old-source-plan","/never","--old-campaign-plan","/never","--old-parent-result","/never",
              "--volume-receipt","/never","--output-dir","/never","--expected-head",NEW_HEAD]
        with patch("production_guard.require_production_host",side_effect=RuntimeError("fixture")), \
                patch.object(bridge,"produce") as producer,redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):bridge.main(args)
        producer.assert_not_called()

    def test_main_hard_deadline_cannot_be_swallowed_by_helper_exception_handler(self):
        args=["--old-source-plan","/fixture","--old-campaign-plan","/fixture","--old-parent-result","/fixture",
              "--volume-receipt","/fixture","--output-dir","/mnt/data/runs/fixture","--expected-head",NEW_HEAD]
        handlers=[]
        caught=[]
        def register(number,handler):
            handlers.append(handler)
            return None
        def swallowing_helper(*args,**kwargs):
            try:handlers[0](None,None)
            except Exception:caught.append(True)
            self.fail("fatal deadline was swallowed")
        with patch("production_guard.require_production_host"), \
                patch.object(inventory,"resource_state",return_value={"ram_bytes":{"MemAvailable":16*1024**3}}), \
                patch.object(bridge.resource,"setrlimit") as ram, \
                patch.object(bridge.signal,"signal",side_effect=register),patch.object(bridge.signal,"alarm") as alarm, \
                patch.object(bridge,"produce",side_effect=swallowing_helper):
            with self.assertRaises(SystemExit) as deadline:bridge.main(args)
        self.assertEqual(deadline.exception.code,"bridge_runtime_limit")
        self.assertEqual(caught,[])
        ram.assert_called_once_with(bridge.resource.RLIMIT_AS,(8*1024**3,8*1024**3))
        self.assertEqual(alarm.call_args_list[0].args,(1200,))
        self.assertEqual(alarm.call_args_list[-1].args,(0,))
        self.assertEqual(bridge.MAX_OUTPUT,8_000_000)

    def test_storage_filesystem_total_and_integer_types_must_match_exactly(self):
        for key,value in (("filesystem_bytes",self.storage["filesystem_bytes"]+1),
                           ("device_bytes",float(bridge.CAPACITY)),("filesystem_bytes",float(self.storage["filesystem_bytes"]))):
            changed=copy.deepcopy(self.storage);changed[key]=value
            with self.subTest(key=key),self.assertRaises(bridge.RebindBlocked):
                bridge.check_storage(self.volume,changed,self.old)

    def test_live_storage_probe_uses_only_fixed_bounded_readonly_commands(self):
        class Reply:
            stderr=""
            def __init__(self,stdout):self.stdout=stdout
        replies=[Reply(f"{bridge.DEVICE} {bridge.UUID} ext4\n"),Reply(f"{bridge.DEVICE} {bridge.SERIAL} {bridge.CAPACITY}\n")]
        with patch.object(bridge.subprocess,"run",side_effect=replies) as command, \
                patch.object(Path,"read_text",return_value=self.storage["boot_id"]), \
                patch.object(Path,"stat",return_value=type("Stat",(),{"st_dev":bridge.NEW_DEVICE})()), \
                patch.object(shutil,"disk_usage",return_value=shutil._ntuple_diskusage(791520346112,0,10**12)):
            value=bridge.capture_storage()
        self.assertEqual(value,self.storage)
        self.assertEqual([entry.args[0][0] for entry in command.call_args_list],["findmnt","lsblk"])
        self.assertTrue(all(entry.kwargs["timeout"]==10 and entry.kwargs["check"] for entry in command.call_args_list))


if __name__=="__main__":unittest.main()
