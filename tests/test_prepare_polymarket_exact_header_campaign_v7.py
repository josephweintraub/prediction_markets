"""Small synthetic stopped-prefix preparation. No network or production inputs."""
from contextlib import ExitStack
import copy
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch
from scripts import prepare_polymarket_exact_header_campaign_v7 as p
from tests import test_polymarket_exact_header_campaign_v7 as stopped


class PreparationTests(unittest.TestCase):
    def setUp(self):
        c=stopped.StoppedFixture(methodName='runTest');c.setUp();self.addCleanup(c.doCleanups);self.c=c
        self.paths={name:c.ops/getattr(p,name).name for name in ('REQUEST','HOST','QA','LOG','COMMIT','ADMISSION','CHECK')}
        self.paths['LEDGER']=c.ops/'prepared_external.json'
        self.private=c.ops/'private';self.private.write_bytes(b'fixture-private-authority')
        # The stopped-prefix fixture already binds its immutable key file into
        # base/files and original_reference. Preserve that complete binding.
        c.base['roots']=[]
        c.admission.update(status='root_exact_header_campaign_admitted',private_file=c.save(c.ops/'private_ref.json',{}),
            reboot_equivalence=c.certificate,inputs={name:p.ref(v) for name,v in c.inputs.items()})
        c.admission['private_file']=p.ref(p.m.bound(self.private))
        c.refs['admission']=c.save(Path(c.refs['admission']['path']),c.admission)
        c.owner.update(inputs=c.inputs,admission=p.m.reference(c.refs['admission']))
        c.refs['owner']=c.save(Path(c.refs['owner']['path']),c.owner)
        c.launch['admission']=p.m.reference(c.refs['admission'])
        c.refs['launch']=c.save(Path(c.refs['launch']['path']),c.launch)
        exited=json.loads(Path(c.refs['exit']['path']).read_bytes());exited['admission']=c.launch['admission']
        c.refs['exit']=c.save(Path(c.refs['exit']['path']),exited)
        updates={r['path']:r for r in c.refs.values()}
        c.drain.update(owner=c.refs['owner'],launch=c.refs['launch'],caller_exit=c.refs['exit'],
            checked_utc=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat())
        c.drain['external_control_closure']=[updates.get(r['path'],r) for r in c.drain['external_control_closure']]
        c.refs['drain']=c.save(Path(c.refs['drain']['path']),c.drain);c.refresh()
        self.source={'head':'f'*40,'files':{}}
        for name in p.SOURCE_FILES:
            raw=(p.REPO/name).read_bytes();self.source['files'][name]={'bytes':len(raw),'sha256':p.digest(raw)}
        self.caller_source={'head':self.source['head'],'files':{name:self.source['files'][name] for name in p.caller.SOURCE_FILES}}
        self.environment={'boot_id':c.process['boot_id'],'uuid':p.m.previous.DATA_UUID,'fstype':'ext4',
            'serial':p.m.previous.DATA_SERIAL,'device_bytes':str(750*1024**3),'data_device':1,
            'root_uuid':p.m.previous.ROOT_UUID,'root_device':2}
        self.host={'schema_version':'polymarket_header_v7_fresh_host_preflight_v1','status':'root_verified_header_recovery_host',
            'checked_utc':datetime.now(timezone.utc).isoformat(),'source_head':self.source['head'],'source_clean':True,
            'instance_id':'i-0f5b31a268af53938','environment':self.environment,'available_ram_bytes':10**12,'available_disk_bytes':10**12,
            'live_owned_births':0,'unknown_owned_births':0,'relevant_compute_notebook_transfer_consumers':[],
            'agents_need_EC2':False,'all_saved_PROCESS_births_checked':True,'process_receipts_checked':603,
            'monitor_ancestry_explicitly_checked':True,'production_stage_running':False}
        host=c.save(self.paths['HOST'],self.host)
        self.paths['LOG'].write_bytes(b'bounded synthetic pass\n');log=p.ref(p.m.bound(self.paths['LOG']))
        self.qa={'schema_version':'polymarket_header_v7_linux_fixture_receipt_v1','status':'passed','source_head':self.source['head'],
            'source_clean_before':True,'source_clean_after':True,'exit_code':0,'command':p.QA_COMMAND,'interpreter':p.QA_COMMAND[0],
            'tests_run':1,'tests_skipped':0,'tested_source_files':self.source['files'],'log':log}
        qa=c.save(self.paths['QA'],self.qa)
        commit=c.save(self.paths['COMMIT'],{'schema_version':'polymarket_header_v7_source_commit_v1','status':'clean_committed_and_pushed',
            'source_head':self.source['head'],'git_status_empty':True,'origin_commit_matches':True,'source_files':self.source['files']})
        self.request={'schema_version':'polymarket_header_v7_preparation_request_v1','status':'root_ready_for_bounded_header_admission_preparation',
            'source_head':self.source['head'],'stopped_v6':c.value['stopped_v6'],'host_proof':host,'linux_qa':qa,'source_commit':commit,
            'extra_prior_controls':[],'extra_external_controls':[],'root_complete_header_control_roster':True,
            'fresh_preparation_anchor_utc':c.drain['checked_utc'],'fresh_preparation_reserve_seconds':1800,'cumulative_rpc':c.rpc}
        self.refresh()

    def refresh(self):self.request_ref=self.c.save(self.paths['REQUEST'],self.request)

    def execute(self,*,source_effect=None,prior_effect=None,write_effect=None,host_refresh=None):
        c=self.c
        with ExitStack() as stack:
            for key,value in {**self.paths,'OP':c.ops,'RUN':c.output,'PRIVATE':self.private,'ACTUAL_KEYS':3,'QA_COUNT':1}.items():
                stack.enter_context(patch.object(p,key,value))
            for context in (patch.object(p,'source_snapshot',side_effect=source_effect,return_value=self.source),
                patch.object(p.caller,'source_snapshot',return_value=self.caller_source),
                patch.object(p.m,'V6_REFS',{key:c.refs[key] for key in ('owner','admission','launch')}),
                patch.object(p.m,'validate_inputs',return_value=(c.inputs,{'seconds_per_key':.001,'bytes_per_key':10})),
                patch.object(p.m,'validate_reboot',return_value=c.reboot),
                patch.object(p.m.previous,'reboot_environment',return_value=self.environment)):
                stack.enter_context(context)
            if prior_effect:stack.enter_context(patch.object(p.m,'validate_prior',side_effect=prior_effect))
            if write_effect:stack.enter_context(patch.object(p,'write',side_effect=write_effect))
            return p.prepare(self.paths['REQUEST'],self.request_ref['sha256'],self.source['head'],host_refresh=host_refresh)

    def test_real_toy_stopped_seal_reused_and_all_cumulative_charges_preserved(self):
        result=self.execute();value=json.loads(self.paths['ADMISSION'].read_bytes());check=json.loads(self.paths['CHECK'].read_bytes())
        self.assertEqual(value['prior']['base'],self.c.admission['prior'])
        self.assertEqual(value['prior']['stopped_v6'],self.request['stopped_v6'])
        for key in ('http_attempts','method_items','response_bytes'):self.assertEqual(value['prior']['rpc'][key],self.c.rpc[key])
        self.assertGreaterEqual(value['prior']['rpc']['elapsed_seconds'],1820)
        self.assertEqual(check['reused_keys'],3);self.assertEqual(check['remaining_keys'],0)
        self.assertIsNone(check['next_attempt']);self.assertFalse(result['producer_launched']);self.assertEqual(result['provider_requests_sent'],0)
        self.assertEqual(check['method_items_per_second'],400);self.assertLessEqual(check['caller_control_encoded_maximum_bytes'],8192)
        ledger=json.loads(self.paths['LEDGER'].read_bytes())
        self.assertEqual(set(ledger),{'schema_version','files','future_reserved_bytes','future_reserved_files','ceiling_bytes'})
        self.assertEqual(value['prior']['external_controls']['sha256'],p.digest(self.paths['LEDGER'].read_bytes()))
        self.assertLessEqual(sum(x['bytes'] for x in ledger['files'])+self.paths['LEDGER'].stat().st_size+p.FUTURE,p.EXTERNAL)

    def test_runtime_windows_disjoint_and_no_refund(self):
        at='2026-10-29T17:00:00+00:00';when=p.timestamp(at)
        self.assertEqual(p.runtime_floor(32350,12000,at,when+100),46250)
        with self.assertRaises(p.Blocked):p.runtime_floor(32350,12000,at,when-1)
        with self.assertRaises(p.Blocked):p.runtime_floor(32350,12000,at,when,600)

    def test_unknown_field_anchor_mismatch_or_reserved_window_blocks(self):
        self.request['unknown']=1;self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.request.pop('unknown');self.request['fresh_preparation_reserve_seconds']=0;self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.assertFalse(self.paths['LEDGER'].exists())

    def test_anchor_must_equal_pinned_drain(self):
        self.request['fresh_preparation_anchor_utc']='2026-10-29T17:00:00+00:00';self.refresh()
        with self.assertRaises(p.Blocked):self.execute()

    def test_live_consumer_or_stale_host_blocks_before_output(self):
        self.host['production_stage_running']=True
        self.request['host_proof']=self.c.save(self.paths['HOST'],self.host);self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.assertFalse(self.paths['LEDGER'].exists())

    def test_refresh_cannot_replace_stale_initial_immutable_host(self):
        self.host['checked_utc']=(datetime.now(timezone.utc)-timedelta(seconds=301)).isoformat()
        self.request['host_proof']=self.c.save(self.paths['HOST'],self.host);self.refresh()
        calls=[]
        def refresh():calls.append(True);return dict(self.host,checked_utc=datetime.now(timezone.utc).isoformat())
        with self.assertRaisesRegex(p.Blocked,'host_identity_or_stale'):self.execute(host_refresh=refresh)
        self.assertEqual(calls,[]);self.assertFalse(self.paths['LEDGER'].exists())

    def test_current_refresh_after_long_full_replay_keeps_immutable_host_and_semantic_checks(self):
        clock=[float(int(time.time())+1)];calls=[];original=p.m.validate_prior;replays=[0]
        initial=self.paths['HOST'].read_bytes()
        def prior(*args):
            result=original(*args);replays[0]+=1
            if replays[0]==1:clock[0]+=301
            return result
        def refresh():
            calls.append(clock[0]);return dict(self.host,checked_utc=datetime.fromtimestamp(clock[0],timezone.utc).isoformat())
        with patch.object(p.time,'time',side_effect=lambda:clock[0]):result=self.execute(prior_effect=prior,host_refresh=refresh)
        self.assertFalse(result['producer_launched']);self.assertEqual(len(calls),2);self.assertEqual(replays[0],3)
        self.assertEqual(self.paths['HOST'].read_bytes(),initial);self.assertTrue(self.paths['CHECK'].is_file())

    def test_standalone_no_refresh_still_blocks_host_staleness_after_full_replay(self):
        clock=[time.time()];original=p.m.validate_prior
        def prior(*args):
            result=original(*args);clock[0]+=301;return result
        with patch.object(p.time,'time',side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(p.Blocked,'host_identity_or_stale'):self.execute(prior_effect=prior)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_refresh_revalidates_current_census_and_late_callback_mutation(self):
        calls=[]
        def refresh():
            calls.append(True)
            if len(calls)==2:self.private.write_bytes(b'x'*self.private.stat().st_size)
            return dict(self.host,checked_utc=datetime.now(timezone.utc).isoformat())
        with self.assertRaises(p.Blocked):self.execute(host_refresh=refresh)
        self.assertEqual(len(calls),2);self.assertFalse(self.paths['CHECK'].exists())

    def test_refresh_cannot_admit_live_consumer(self):
        def refresh():return dict(self.host,production_stage_running=True)
        with self.assertRaisesRegex(p.Blocked,'host_not_drained'):self.execute(host_refresh=refresh)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_wrong_linux_command_count_or_skips_blocks(self):
        original=copy.deepcopy(self.qa)
        for key,bad in [('command',['/bin/false']),('tests_run',0),('tests_skipped',1)]:
            with self.subTest(key=key):
                value=copy.deepcopy(original);value[key]=bad
                self.request['linux_qa']=self.c.save(self.paths['QA'],value);self.refresh()
                with self.assertRaises(p.Blocked):self.execute()
                self.assertFalse(self.paths['LEDGER'].exists())

    def test_understated_request_counter_rejected_by_real_stopped_validator(self):
        self.request['cumulative_rpc']=dict(self.c.rpc,method_items=0);self.refresh()
        with self.assertRaises(p.Blocked):self.execute()
        self.assertFalse(self.paths['CHECK'].exists())

    def test_same_pool_not_second_pool_or_unbounded_future(self):
        self.assertEqual(p.FUTURE,1245184);self.assertEqual(p.EXTERNAL,2097152)
        with self.assertRaises(p.Blocked):p.external_budget([{'path':'/x','bytes':p.EXTERNAL-p.FUTURE+1}])
        with self.assertRaises(p.Blocked):p.external_budget([{'path':'/x','bytes':0},{'path':'/x','bytes':0}])

    def test_existing_admission_not_overwritten(self):
        self.paths['ADMISSION'].write_bytes(b'keep')
        with self.assertRaises(p.Blocked):self.execute()
        self.assertEqual(self.paths['ADMISSION'].read_bytes(),b'keep')

    def test_final_source_callback_changed_request_withholds_check(self):
        def source(_):
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                raw=self.paths['REQUEST'].read_bytes();self.paths['REQUEST'].write_bytes(raw[:-1]+b' ')
            return self.source
        with self.assertRaises(p.Blocked):self.execute(source_effect=source)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_final_prior_callback_changed_owner_withholds_check(self):
        original=p.m.validate_prior
        def prior(*args):
            result=original(*args)
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                path=self.c.v6/'OWNER.json';raw=path.read_bytes();path.write_bytes(raw[:-1]+b' ')
            return result
        with self.assertRaises(p.Blocked):self.execute(prior_effect=prior)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_final_prior_callback_changed_private_withholds_check(self):
        original=p.m.validate_prior
        def prior(*args):
            result=original(*args)
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                self.private.write_bytes(b'x'*self.private.stat().st_size)
            return result
        with self.assertRaises(p.Blocked):self.execute(prior_effect=prior)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_pending_mutation_or_large_check_withholds_marker(self):
        original=p.write
        def write(path,value,reader,maximum=p.MAX_CONTROL):
            result=original(path,value,reader,maximum)
            if path.name.endswith('.pending'):
                raw=path.read_bytes();path.write_bytes(raw[:-1]+b' ')
            return result
        with self.assertRaises(p.Blocked):self.execute(write_effect=write)
        self.assertFalse(self.paths['CHECK'].exists())

    def test_header_only_caller_bound_has_no_metadata_or_cap_override(self):
        self.assertEqual(set(p.caller.STAGES),{'headers'});self.assertEqual(p.caller.JSON_LIMIT,8192)
        self.assertEqual(p.m.CAPS,p.m.previous.CAPS);self.assertEqual(p.HARD_SECONDS,1200)

    def test_guard_precedes_any_production_input_read(self):
        args=['--preparation-request',str(self.paths['REQUEST']),'--preparation-request-sha256',self.request_ref['sha256'],
            '--expected-head',self.source['head']]
        with patch.object(p,'require_production_host',side_effect=RuntimeError('guard')),patch.object(p,'prepare') as run:
            with self.assertRaises(RuntimeError):p.main(args)
        run.assert_not_called()


if __name__=='__main__':unittest.main()
