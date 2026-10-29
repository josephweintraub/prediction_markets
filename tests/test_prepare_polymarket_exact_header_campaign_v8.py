"""Local synthetic preparation fixtures. No production reads, RPC or lifecycle."""
from contextlib import ExitStack
import copy
from datetime import datetime,timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from scripts import prepare_polymarket_exact_header_campaign_v8 as p
from tests import test_polymarket_exact_header_campaign_v8 as cold


class UnitTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.reader=p.Reader(lambda:None)

    def save(self,name,value,size=None):
        path=self.root/name;raw=p.encode(value)
        if size is not None:self.assertLessEqual(len(raw),size);raw+=b' '*(size-len(raw))
        path.write_bytes(raw);return p.ref(p.m.bound(path,p.m.MAX_LEDGER))

    def fixture(self):
        rows=[]
        for i in range(58):
            path=self.root/f'retained_{i}.bin';path.write_bytes(b'x'*813845 if i==0 else b'')
            rows.append(p.ref(p.m.bound(path,p.m.MAX_LEDGER)))
        ledger={'schema_version':'polymarket_header_v7_external_control_ledger_v1','files':rows,
          'future_reserved_bytes':1245184,'future_reserved_files':9,'ceiling_bytes':2097152}
        ledger_ref=self.save('old_ledger.json',ledger,11369)
        base={'rpc':{'elapsed_seconds':32350}}
        stopped={'path':str(self.root/'old_handoff.json'),'bytes':1,'sha256':'a'*64}
        admission={'prior':{'base':base,'stopped_v6':stopped,'rpc':dict(p.RETAINED_RPC,elapsed_seconds=104777)}}
        refs={'admission':self.save('expired_admission.json',admission,148155),
          'check':self.save('old_check.json',{},7358),'failure':self.save('old_failure.json',{},626)}
        terminal=self.save('terminal.json',{},1965)
        return ledger,ledger_ref,refs,terminal,base,stopped

    def contexts(self,ledger,refs,terminal):
        stack=ExitStack();self.addCleanup(stack.close)
        for name,value in [('OLD_V7_LEDGER',ledger),('EXPIRED_V7_REFS',refs),('EXPIRED_V7_TERMINAL',terminal)]:
            stack.enter_context(patch.object(p.m,name,value))

    def test_actual_shape_58_to62_mandatory_closure_rehashed(self):
        _,ledger,refs,terminal,base,stopped=self.fixture();self.contexts(ledger,refs,terminal)
        result=p.retained_controls(self.reader,base,stopped)
        self.assertEqual(len(result),63);self.assertEqual(sum(v['bytes'] for v in result),983318)
        self.assertEqual(len(self.reader.saved),63);self.reader.reopen()

    def test_one_omission_duplicate_or_bad_total_blocks(self):
        value,_,refs,terminal,base,stopped=self.fixture()
        for rows in (value['files'][:-1],value['files'][:-1]+[value['files'][0]]):
            ledger=self.save('changed_ledger.json',dict(value,files=rows),11369)
            with patch.object(p.m,'OLD_V7_LEDGER',ledger),patch.object(p.m,'EXPIRED_V7_REFS',refs),patch.object(p.m,'EXPIRED_V7_TERMINAL',terminal):
                with self.assertRaises(p.Blocked):p.retained_controls(p.Reader(lambda:None),base,stopped)

    def test_retained_payload_mutation_blocks(self):
        value,ledger,refs,terminal,base,stopped=self.fixture();self.contexts(ledger,refs,terminal)
        path=Path(value['files'][0]['path']);path.write_bytes(b'y'*path.stat().st_size)
        with self.assertRaises(p.Blocked):p.retained_controls(self.reader,base,stopped)

    def test_old_runtime_floor_cannot_be_refunded(self):
        _,ledger,refs,terminal,base,stopped=self.fixture()
        admission={'prior':{'base':base,'stopped_v6':stopped,'rpc':dict(p.RETAINED_RPC,elapsed_seconds=104776)}}
        refs['admission']=self.save('expired_admission.json',admission,148155);self.contexts(ledger,refs,terminal)
        with self.assertRaises(p.Blocked):p.retained_controls(self.reader,base,stopped)

    def test_fixed_runtime_anchor_and_no_old_reservation_refund(self):
        at=p.timestamp(p.RUNTIME_ANCHOR)
        self.assertEqual(p.runtime_floor(32350,69325.343523669,p.RUNTIME_ANCHOR,at),104777+1800)
        self.assertEqual(p.runtime_floor(32350,69325.343523669,p.RUNTIME_ANCHOR,at+5000),106676+1800)
        self.assertEqual(p.runtime_floor(32350,69325.343523669,p.RUNTIME_ANCHOR,at+5000,0),106676)
        for args in [(1,69325.343523669,p.RUNTIME_ANCHOR,at),(32350,1,p.RUNTIME_ANCHOR,at),
          (32350,69325.343523669,'2026-10-29T17:34:09.715898Z',at),(32350,69325.343523669,p.RUNTIME_ANCHOR,at-1)]:
            with self.assertRaises(p.Blocked):p.runtime_floor(*args)
        with self.assertRaises(p.Blocked):p.runtime_floor(32350,69325.343523669,p.RUNTIME_ANCHOR,at,600)

    def test_no_second_pool_and_new_log_exact512k(self):
        self.assertEqual((p.FUTURE,p.FUTURE_FILES,p.EXTERNAL),(786432,9,2097152))
        self.assertEqual(p.caller.LOG_LIMIT,524288);self.assertEqual(p.HARD_SECONDS,1200)
        with self.assertRaises(p.Blocked):p.external_budget([{'path':'/a','bytes':2097152-786432+1}])
        with self.assertRaises(p.Blocked):p.external_budget([{'path':'/a','bytes':1},{'path':'/a','bytes':1}])

    def test_reader_reopen_keeps_full_private_identity_separate(self):
        ref=self.save('private',{'fixture':'not a credential'})
        captured=self.reader.bind(ref);full=p.m.pilot.identity(Path(ref['path']))
        self.assertEqual(len(full),9);self.assertNotEqual(captured['identity'],full)
        self.reader.reopen();Path(ref['path']).write_bytes(b'x'*ref['bytes'])
        with self.assertRaises(p.Blocked):self.reader.reopen()

    def test_guard_before_preparation_or_input_read(self):
        args=['--preparation-request','/fixture/request','--preparation-request-sha256','a'*64,'--expected-head','b'*40]
        with patch.object(p,'require_production_host',side_effect=RuntimeError('guard')),patch.object(p,'prepare') as run:
            with self.assertRaises(RuntimeError):p.main(args)
        run.assert_not_called()


class PreparationTests(unittest.TestCase):
    """Exercise real old6 and new8 validators, including a measured device hop."""
    def setUp(self):
        c=cold.ColdBootFixture('runTest');c.setUp();self.addCleanup(c.doCleanups);self.c=c
        c.device_hop()
        # Only synthetic preparation outputs are removed. Historical receipts,
        # their hashes and all replay validators stay untouched.
        Path(c.certificate_ref['path']).unlink();Path(c.ledger['path']).unlink()
        self.paths={name:c.ops/getattr(p,name).name for name in
          ('REQUEST','HOST','QA','LOG','COMMIT','CERT','LEDGER','ADMISSION','CHECK')}
        self.source={'head':'f'*40,'files':{}}
        for name in p.SOURCE_FILES:
            raw=(p.REPO/name).read_bytes();c.c.c.blobs[name]=raw
            self.source['files'][name]={'bytes':len(raw),'sha256':p.digest(raw)}
        self.caller_source={'head':self.source['head'],'files':{
          name:self.source['files'][name] for name in p.caller.SOURCE_FILES}}
        self.clock=[p.timestamp(c.drain['checked_utc'])+10]
        self.host={'schema_version':'polymarket_header_v8_fresh_host_preflight_v1',
          'status':'root_verified_header_recovery_host','checked_utc':self.utc(),'source_head':self.source['head'],
          'source_clean':True,'instance_id':'i-0f5b31a268af53938','environment':dict(c.env),
          'available_ram_bytes':10**12,'available_disk_bytes':10**12,'live_owned_births':0,'unknown_owned_births':0,
          'relevant_compute_notebook_transfer_consumers':[],'agents_need_EC2':False,
          'all_saved_PROCESS_births_checked':True,'process_receipts_checked':603,
          'monitor_ancestry_explicitly_checked':True,'production_stage_running':False}
        host=c.save(self.paths['HOST'],self.host)
        self.paths['LOG'].write_bytes(b'synthetic full historical replay\n')
        log=p.ref(p.m.bound(self.paths['LOG']))
        self.qa={'schema_version':'polymarket_header_v8_linux_fixture_receipt_v1','status':'passed',
          'source_head':self.source['head'],'source_clean_before':True,'source_clean_after':True,'exit_code':0,
          'command':p.QA_COMMAND,'interpreter':p.QA_COMMAND[0],'tests_run':1,'tests_skipped':0,
          'tested_source_files':self.source['files'],'log':log}
        qa=c.save(self.paths['QA'],self.qa)
        commit=c.save(self.paths['COMMIT'],{'schema_version':'polymarket_header_v8_source_commit_v1',
          'status':'clean_committed_and_pushed','source_head':self.source['head'],'git_status_empty':True,
          'origin_commit_matches':True,'source_files':self.source['files']})
        self.request={'schema_version':'polymarket_header_v8_preparation_request_v1',
          'status':'root_ready_for_bounded_header_admission_preparation','source_head':self.source['head'],
          'stopped_v6':c.handoff_ref,'host_proof':host,'linux_qa':qa,'source_commit':commit,
          'extra_prior_controls':[],'extra_external_controls':[],'root_complete_header_control_roster':True,
          'fresh_preparation_anchor_utc':c.drain['checked_utc'],'fresh_preparation_reserve_seconds':1800,
          'cumulative_rpc':dict(c.rpc)}
        self.refresh()

    def utc(self):return datetime.fromtimestamp(self.clock[0],timezone.utc).isoformat()
    def refresh(self):self.request_ref=self.c.save(self.paths['REQUEST'],self.request)

    def execute(self,*,source_effect=None,prior_effect=None,write_effect=None,host_refresh=None,reference_effect=None):
        c=self.c;profile=p.m.OLD_V7_PROFILE
        with ExitStack() as stack:
            replacements={**self.paths,'OP':c.ops,'RUN':c.output,'PRIVATE':c.private,'ACTUAL_KEYS':c.c.c.K,'QA_COUNT':1,
              'RUNTIME_BASE':c.base_rpc['elapsed_seconds'],'RUNTIME_V6_CHARGE':c.handoff['runtime_charge_seconds'],
              'RUNTIME_ANCHOR':c.drain['checked_utc'],'RETAINED_RUNTIME_FLOOR':c.expired['prior']['rpc']['elapsed_seconds'],
              'RETAINED_RPC':{k:v for k,v in c.rpc.items() if k!='elapsed_seconds'},
              'RETAINED_FILE_COUNT':profile['ledger_files'],'RETAINED_FILE_BYTES':profile['ledger_file_bytes'],
              'RETAINED_CLOSURE_COUNT':profile['required_files'],'RETAINED_CLOSURE_BYTES':profile['required_bytes']}
            for key,value in replacements.items():stack.enter_context(patch.object(p,key,value))
            for context in (patch.object(p,'source_snapshot',side_effect=source_effect,return_value=self.source),
              patch.object(p.caller,'source_snapshot',return_value=self.caller_source),
              patch.object(p.time,'time',side_effect=lambda:self.clock[0]),
              patch.object(p.m.previous,'reboot_environment',side_effect=lambda:dict(c.env)),
              patch.object(p.m.inventory,'resource_state',return_value=cold.old6.old.fixture.RESOURCES)):
                stack.enter_context(context)
            if prior_effect:stack.enter_context(patch.object(p.m,'validate_prior',side_effect=prior_effect))
            if write_effect:stack.enter_context(patch.object(p,'write',side_effect=write_effect))
            if reference_effect:stack.enter_context(patch.object(p.m,'reference',side_effect=reference_effect))
            return p.prepare(self.paths['REQUEST'],self.request_ref['sha256'],self.source['head'],host_refresh=host_refresh)

    def test_real_cold_chain_full9_private_and_fresh_original_request(self):
        result=self.execute()
        admission=json.loads(self.paths['ADMISSION'].read_bytes());check=json.loads(self.paths['CHECK'].read_bytes())
        self.assertEqual(admission['prior']['base'],self.c.base)
        self.assertEqual(admission['prior']['stopped_v6'],self.c.handoff_ref)
        for key in ('http_attempts','method_items','response_bytes'):
            self.assertEqual(admission['prior']['rpc'][key],self.c.rpc[key])
        self.assertGreaterEqual(admission['prior']['rpc']['elapsed_seconds'],
          self.c.base_rpc['elapsed_seconds']+self.c.handoff['runtime_charge_seconds']+10+1800)
        cert=json.loads(self.paths['CERT'].read_bytes())
        self.assertEqual(cert['private_authority']['current']['device'],77002)
        self.assertEqual(len(cert['private_authority']['current']),9)
        self.assertEqual(cert['original_pilot_request']['current']['identity']['device'],77001)
        self.assertNotEqual(cert['original_pilot_request']['current'],cert['original_pilot_request']['v6'])
        self.assertEqual((check['reused_keys'],check['remaining_keys']),(self.c.c.c.K,0))
        self.assertEqual((result['provider_requests_sent'],result['producer_launched']),(0,False))
        ledger=json.loads(self.paths['LEDGER'].read_bytes());paths=[item['path'] for item in ledger['files']]
        self.assertEqual(paths.count(str(self.paths['CERT'])),1)
        retained=json.loads(Path(p.m.OLD_V7_LEDGER['path']).read_bytes())
        self.assertTrue({item['path'] for item in retained['files']}<=set(paths))
        self.assertLessEqual(sum(item['bytes'] for item in ledger['files'])+self.paths['LEDGER'].stat().st_size+p.FUTURE,p.EXTERNAL)
        self.assertLessEqual(check['caller_control_encoded_maximum_bytes'],8192)

    def test_unknown_or_reserved_window_request_blocks(self):
        self.request['unknown']=True;self.refresh()
        with self.assertRaisesRegex(p.Blocked,'request_schema'):self.execute()
        self.assertFalse(self.paths['CERT'].exists())
        self.request.pop('unknown');self.request['fresh_preparation_reserve_seconds']=0;self.refresh()
        with self.assertRaisesRegex(p.Blocked,'request_schema'):self.execute()
        self.assertFalse(self.paths['CERT'].exists())

    def test_real_consumer_carry_floor_matches_preparer_without_second_reserve(self):
        c=self.c
        with patch.object(p,'RUNTIME_BASE',c.base_rpc['elapsed_seconds']),\
          patch.object(p,'RUNTIME_V6_CHARGE',c.handoff['runtime_charge_seconds']),\
          patch.object(p,'RUNTIME_ANCHOR',c.drain['checked_utc']),\
          patch.object(p,'RETAINED_RUNTIME_FLOOR',c.rpc['elapsed_seconds']),\
          patch.object(p.m,'RETAINED_RUNTIME_FLOOR',c.rpc['elapsed_seconds']):
            base=p.runtime_floor(c.base_rpc['elapsed_seconds'],c.handoff['runtime_charge_seconds'],
              c.drain['checked_utc'],self.clock[0],reserve=0)
            self.assertEqual(base,p.m.required_carried_runtime(c.value,self.clock[0]))
            self.assertEqual(base+1800,p.runtime_floor(c.base_rpc['elapsed_seconds'],
              c.handoff['runtime_charge_seconds'],c.drain['checked_utc'],self.clock[0]))
    def test_anchor_mismatch_or_understated_counter_blocks(self):
        self.request['fresh_preparation_anchor_utc']='2026-10-29T17:00:01+00:00';self.refresh()
        with self.assertRaisesRegex(p.Blocked,'preparation_anchor_not_drain'):self.execute()
        self.request['fresh_preparation_anchor_utc']=self.c.drain['checked_utc']
        self.request['cumulative_rpc']['method_items']=0;self.refresh()
        with self.assertRaisesRegex(p.Blocked,'retained_charge_refunded'):self.execute()
        self.assertFalse(self.paths['CHECK'].exists())

    def test_stale_initial_host_cannot_be_replaced_by_callback(self):
        self.host['checked_utc']=datetime.fromtimestamp(self.clock[0]-301,timezone.utc).isoformat()
        self.request['host_proof']=self.c.save(self.paths['HOST'],self.host);self.refresh();calls=[]
        def refresh():calls.append(1);return dict(self.host,checked_utc=self.utc())
        with self.assertRaisesRegex(p.Blocked,'host_identity_or_stale'):self.execute(host_refresh=refresh)
        self.assertEqual(calls,[]);self.assertFalse(self.paths['CERT'].exists())

    def test_wrong_qa_or_live_host_blocks_before_certificate(self):
        self.qa['tests_skipped']=1;self.request['linux_qa']=self.c.save(self.paths['QA'],self.qa);self.refresh()
        with self.assertRaisesRegex(p.Blocked,'linux_qa'):self.execute()
        self.qa['tests_skipped']=0;self.request['linux_qa']=self.c.save(self.paths['QA'],self.qa);self.refresh()
        with self.assertRaisesRegex(p.Blocked,'host_not_drained'):
            self.execute(host_refresh=lambda:dict(self.host,production_stage_running=True))
        self.assertFalse(self.paths['CHECK'].exists())

    def test_host_refresh_after_real_replay_does_not_reset_reservation(self):
        original=p.m.validate_prior;replays=[];calls=[];initial=self.paths['HOST'].read_bytes()
        def prior(*args):
            result=original(*args);replays.append(1)
            if len(replays)==1:self.clock[0]+=301
            return result
        def refresh():calls.append(1);return dict(self.host,checked_utc=self.utc())
        result=self.execute(prior_effect=prior,host_refresh=refresh)
        self.assertFalse(result['producer_launched']);self.assertEqual((len(replays),len(calls)),(3,2))
        self.assertEqual(self.paths['HOST'].read_bytes(),initial)
        check=json.loads(self.paths['CHECK'].read_bytes());self.assertEqual(check['preparation_seconds_reserved'],1800)

    def test_late_callback_private_or_source_change_withholds_check(self):
        calls=[]
        def refresh():
            calls.append(1)
            if len(calls)==2:self.c.private.write_bytes(b'x'*self.c.private.stat().st_size)
            return dict(self.host,checked_utc=self.utc())
        with self.assertRaises(p.Blocked):self.execute(host_refresh=refresh)
        self.assertEqual(len(calls),2);self.assertFalse(self.paths['CHECK'].exists())

    def test_final_source_callback_request_mutation_withholds_check(self):
        changed=[]
        def source(_):
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                raw=self.paths['REQUEST'].read_bytes();self.paths['REQUEST'].write_bytes(raw[:-1]+b' ')
                changed.append(1)
            return self.source
        with self.assertRaises(p.Blocked):self.execute(source_effect=source)
        self.assertEqual(changed,[1]);self.assertFalse(self.paths['CHECK'].exists())

    def test_final_source_snapshot_change_withholds_check(self):
        changed=[]
        def source(_):
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                value=copy.deepcopy(self.source);value['files'][p.SOURCE_FILES[-1]]['sha256']='0'*64
                changed.append(1);return value
            return self.source
        with self.assertRaisesRegex(p.Blocked,'source_changed'):self.execute(source_effect=source)
        self.assertEqual(changed,[1]);self.assertFalse(self.paths['CHECK'].exists())

    def test_real_prior_callback_input_mutation_withholds_check(self):
        original=p.m.validate_prior;changed=[]
        def prior(*args):
            result=original(*args)
            if self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending').exists():
                path=Path(self.c.inputs['scale_contract']['path']);path.write_bytes(b'x'*path.stat().st_size)
                changed.append(1)
            return result
        with self.assertRaises(p.Blocked):self.execute(prior_effect=prior)
        self.assertEqual(changed,[1]);self.assertFalse(self.paths['CHECK'].exists())

    def test_pending_change_cannot_publish(self):
        original=p.write;changed=[]
        def write(path,value,reader,maximum=p.MAX_CONTROL):
            result=original(path,value,reader,maximum)
            if path.name.endswith('.pending'):
                raw=path.read_bytes();path.write_bytes(raw[:-1]+b' ')
                changed.append(1)
            return result
        with self.assertRaises(p.Blocked):self.execute(write_effect=write)
        self.assertEqual(changed,[1]);self.assertFalse(self.paths['CHECK'].exists())

    def test_pending_mutation_after_full_reopen_is_not_adopted(self):
        original=p.m.reference;reads=[]
        pending=self.paths['CHECK'].with_name('.'+self.paths['CHECK'].name+'.pending')
        def reference(value,maximum=p.m.MAX_JSON):
            if value['path']==str(pending):
                reads.append(1)
                if len(reads)==3:
                    raw=pending.read_bytes();pending.write_bytes(raw[:-1]+b' ')
            return original(value,maximum)
        with self.assertRaises(p.Blocked):self.execute(reference_effect=reference)
        self.assertEqual(len(reads),3);self.assertFalse(self.paths['CHECK'].exists())

    def test_existing_admission_or_certificate_never_overwritten(self):
        self.paths['CERT'].write_bytes(b'keep')
        with self.assertRaisesRegex(p.Blocked,'fresh_paths_required'):self.execute()
        self.assertEqual(self.paths['CERT'].read_bytes(),b'keep');self.assertFalse(self.paths['CHECK'].exists())


if __name__=='__main__':unittest.main()
