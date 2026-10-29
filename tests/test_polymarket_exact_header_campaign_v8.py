"""Synthetic cold-boot, real historical replay and stopped-prefix fixtures."""
from contextlib import ExitStack,nullcontext
from copy import deepcopy
from pathlib import Path
import ast
import inspect
import json
import math
import tempfile
import unittest
from unittest.mock import patch
from scripts import collect_polymarket_exact_header_campaign_v8 as m
from scripts import collect_polymarket_exact_header_campaign_v7 as before
from scripts import launch_polymarket_oct1_stage_v7 as caller7
from tests import test_polymarket_exact_header_campaign_v6 as old6

NEW_BOOT='93083635-9263-4fc3-a7f9-453a18f9faf8'


class ColdBootFixture(unittest.TestCase):
    """Real V3/V4/V5/V6 base, real9-input certificate, real outer prefix.

Only source/Git/OS/HTTP fixture boundaries are replaced. No validate_reboot,
validate_prior or private-identity validator returns are mocked.
"""
    def setUp(self):
        self.c=old6.CertificateTests('runTest');self.c.setUp();self.addCleanup(self.c.doCleanups)
        c=self.c.c;result=self.c.campaign();self.assertEqual(result['status'],m.SUCCESS,result)
        self.root=c.root;self.ops=self.root/'operations8';self.ops.mkdir()
        self.v6=c.output;self.output=self.root/'v8';self.stopped=self
        # Build a synthetic stopped tree from the pre-root-publication state.
        (self.v6/'COMPLETE.json').unlink()
        self.inputs={name:m.reference(value,m.CAPS['baseline_bytes']) for name,value in c.inputs.items()}
        self.private=c.private;self.private_ref=c.admission['private_file']
        self.base=deepcopy(c.admission['prior']);self.base_rpc=dict(self.base['rpc'])
        self.env=dict(self.c.env,boot_id=NEW_BOOT)
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        contexts=(patch.object(m,'RUN_ROOT',self.root),patch.object(m,'RUN',self.output),patch.object(m,'V6_ROOT',self.v6),patch.object(m,'OP_ROOT',self.ops),
          patch.object(m,'HANDOFF_PATH',self.ops/'full_header_v6_stopped_handoff_v1.json'),
          patch.object(m,'EXIT_PATH',self.ops/'full_header_v6.EXIT.json'),
          patch.object(m,'DRAIN_PATH',self.ops/'full_header_v6_root_drain_v7_handoff_v1.json'),
          patch.object(m,'OLD_EXTERNAL_PATH',self.ops/'header_v6_external_control_ledger_v1.json'),
          patch.object(m,'CERTIFICATE_PATH',self.ops/'header_reboot_equivalence_v8_v1.json'),
          patch.object(m,'ACTUAL_KEYS',c.K),patch.object(m,'reboot_environment',side_effect=lambda:dict(self.env)),
          patch.object(m.previous.previous,'V4_ROOT',c.v4_root),patch.object(m.previous.previous,'V4_REFS',c.v4_refs),
          patch.object(m.previous.previous,'V4_HEAD',c.v4_head),patch.object(m.previous.previous,'V4_CUMULATIVE_RPC',c.v4_cumulative),
          patch.object(m.previous.previous,'V4_NEW_RPC',c.v4_new),patch.object(m.previous.previous.previous,'RUN_ROOT',self.root),
          patch.object(m.previous.previous.previous,'OBSERVATION_ACCEPTANCE',c.acceptance),
          patch.object(m.previous.previous.previous,'FAILED_STDOUT_ECHO',c.failed_stdout_ref),
          patch.object(m.previous.previous.previous,'FAILED_SOURCE',c.failed_source_ref),
          patch.object(m.legacy,'RUN_ROOT',self.root),patch.object(m.scale,'git_blob',side_effect=lambda head,name:c.blobs[name]))
        for context in contexts:self.stack.enter_context(context)
        external=self.save(m.OLD_EXTERNAL_PATH,{'files':[]})
        self.base['files'].append(external|{'category':'control','chunk':None})
        self.base['standalone_control_files'].append(external['path'])
        self.admission=deepcopy(c.admission);self.admission['prior']=self.base
        admission_ref=self.save(c.admission_path,self.admission)
        owner_path=self.v6/'OWNER.json';owner=json.loads(owner_path.read_bytes())
        owner['admission']=m.reference(admission_ref);owner_ref=self.save(owner_path,owner)
        self.owner=owner;self.process=owner['process']
        self.stack.enter_context(patch.object(m,'V6_HEAD',owner['source']['head']))
        self.source={'head':'e'*40,'files':dict(owner['source']['files'])}
        for name in m.SOURCE_FILES[len(m.previous.SOURCE_FILES):]:
            raw=(m.REPO/name).read_bytes();c.blobs[name]=raw
            self.source['files'][name]={'bytes':len(raw),'sha256':m.digest(raw)}
        child=owner['process']|{'process_group':owner['process']['pid'],'session':owner['process']['pid']}
        caller=child|{'pid':122,'birth_ticks':1,'process_group':122,'session':122}
        launch={'schema_version':'polymarket_oct1_stage_launcher_v5','stage':'headers','status':'producer_dispatched_owned_session',
          'source':{'head':m.V6_HEAD},'output_dir':str(self.v6),'admission':m.reference(admission_ref),
          'ownership_unknown':False,'retry_or_resume':False,'caller':caller,'child':child,'command':['fixed']}
        launch_ref=self.save(self.ops/'full_header_v6.LAUNCH.json',launch)
        # Obtain the REAL frozen old6 replay, including its fresh request identity.
        old_context=self.c.validate()
        self.prior=m.previous.validate_prior(self.base,self.output,m.previous_source(self.source),self.inputs,old_context)
        times={};spans=[]
        for path in self.v6.rglob('rpc.jsonl'):
            if path.parent==self.v6:
                _,starts,finishes=m.journal_usage(path)
            else:
                chunk=int(path.parent.parent.name[1:]);keys=m.legacy.key_chunk_at(self.inputs['keys']['path'],chunk)
                _,starts,finishes=m.journal_usage(path,(chunk*m.CAPS['keys_per_chunk'],keys))
                manifest=json.loads((path.parent/'manifest.json').read_bytes())
                floor=self.prior['chunk_usage'].get(chunk,{}).get('elapsed_seconds',0)
                times[str(chunk)]=max(0,manifest['active_seconds_charged']-floor)
            if starts:spans.append((max(x['t'] for x in finishes.values())-min(x['t'] for x in starts.values()))/1000)
        charge=math.ceil(max([sum(times.values()),*spans,0]))+1
        self.rpc={key:result['usage'][key] for key in self.base_rpc}
        self.rpc['elapsed_seconds']=max(self.rpc['elapsed_seconds'],self.base_rpc['elapsed_seconds']+charge)
        exit_ref=self.save(m.EXIT_PATH,launch|{'status':'caller_failed_closed','error_code':'caller_interrupted',
          'producer_exit_code':-15,'owned_group_drained':True,'children_still_running':False,'stdout_eof_observed':True,
          'source_and_admission_unchanged':True,'elapsed_seconds':charge})
        self.refs={'owner':owner_ref,'admission':admission_ref,'launch':launch_ref,'exit':exit_ref}
        closed={item['path']:item for item in (external,admission_ref,launch_ref,exit_ref)}
        for name in m.OLD_FUTURE_NAMES:
            path=self.ops/name
            if str(path) not in closed:closed[str(path)]=self.save(path,{})
        self.drain={'schema_version':'polymarket_header_v6_root_drain_v7_handoff_v1','status':'root_verified_stopped_header_v6',
          'run_dir':str(self.v6),'source_head':m.V6_HEAD,'owner':owner_ref,'launch':launch_ref,'caller_exit':exit_ref,
          'header_owned_processes_drained':True,'remaining_owned_group_members':[],'no_owned_workers_or_transfers':True,
          'host_idle_claimed':False,'final_sample_admitted':False,'checked_utc':'2026-10-29T17:00:00+00:00',
          'external_control_closure':list(closed.values())}
        self.refs['drain']=self.save(m.DRAIN_PATH,self.drain)
        self.stack.enter_context(patch.object(m,'V6_REFS',{key:self.refs[key] for key in ('owner','admission','launch')}))
        files=[]
        for path in sorted(self.v6.rglob('*')):
            if not path.is_file():continue
            chunk=int(path.parent.parent.name[1:]) if 'chunks' in path.parts else None
            category='accepted' if path.name=='headers.parquet' and (path.parent/'COMPLETE.json').exists() else 'failed' if path.name=='headers.parquet' else 'control'
            files.append(m.ref(m.bound(path))|{'category':category,'chunk':chunk})
        self.handoff={'schema_version':'polymarket_header_v6_stopped_handoff_v1','status':'root_bound_stopped_v6_header_prefix',
          'run_dir':str(self.v6),'source_head':m.V6_HEAD,'refs':self.refs,'files':files,'runtime_charge_seconds':charge,
          'chunk_runtime_seconds':times,'raw_clocks_only':True,'final_sample_admitted':False}
        self.handoff_ref=self.save(m.HANDOFF_PATH,self.handoff)
        self.stack.enter_context(patch.object(m,'STOPPED_V6',self.handoff_ref))
        historical={**closed,**{item['path']:item for item in (self.handoff_ref,self.refs['drain'])}}
        for name in ('scripts/collect_polymarket_exact_header_campaign_v7.py',
          'tests/test_polymarket_exact_header_campaign_v7.py','scripts/launch_polymarket_oct1_stage_v6.py',
          'tests/test_launch_polymarket_oct1_stage_v6.py'):
            item=m.ref(m.bound(m.REPO/name));historical[item['path']]=item
        old_ledger=self.save(self.ops/'header_v7_external_control_ledger_v1.json',{
          'schema_version':'polymarket_header_v7_external_control_ledger_v1','files':list(historical.values()),
          'future_reserved_bytes':1245184,'future_reserved_files':9,'ceiling_bytes':2097152})
        self.stack.enter_context(patch.object(m,'OLD_V7_LEDGER',old_ledger))
        self.expired={'schema_version':'polymarket_exact_header_campaign_admission_v7','status':'root_exact_header_campaign_admitted',
          'consumer_head':'dcd2a8c1b8188d922b747b313a6c8ecdb931279b',
          'run_dir':str(self.root/'2026-10-29_polymarket_exact_header_campaign_v7'),'caps':m.CAPS,'actual_keys':c.K,
          'inputs':{name:m.ref(item) for name,item in self.inputs.items()},'private_file':self.private_ref,
          'prior':{'base':self.base,'stopped_v6':self.handoff_ref,'rpc':dict(self.rpc),'external_controls':old_ledger},
          'raw_clocks_only':True,'final_sample_admitted':False}
        expired_ref=self.save(self.ops/'full_header_v7_admission_v1.json',self.expired)
        check_ref=self.save(self.ops/'full_header_v7_root_admission_check_v1.json',{
          'schema_version':'polymarket_header_v7_preparation_check_v1','status':'header_only_admission_prepared_not_launched',
          'admission':expired_ref,'external_ledger':old_ledger,'prior_rpc':dict(self.rpc),'next_chunk':None,'next_attempt':None,
          'reused_keys':c.K,'remaining_keys':0,'method_items_per_second':400})
        failed_ref=self.save(self.ops/'full_header_v7_root_dispatch_v1.json',{
          'schema_version':'polymarket_header_v7_root_dispatch_v1','status':'root_dispatch_failed_requires_investigation',
          'error_code':'private_identity_changed','source_head':self.expired['consumer_head'],'constructor_entered':False,
          'ownership_unknown':False,'caller_may_be_running':False})
        self.stack.enter_context(patch.object(m,'EXPIRED_V7_REFS',{'admission':expired_ref,'check':check_ref,'failure':failed_ref}))
        terminal=self.save(self.ops/'header_v7_unlaunched_terminal_v1.json',{
          'schema_version':'polymarket_header_v7_unlaunched_terminal_local_checkpoint_v1',
          'status':'unlaunched_expired_admission_host_stopped_monitor_paused','checked_utc':'2026-10-29T18:32:07Z',
          'source_head':self.expired['consumer_head'],'run_start_launch_exit_log_all_absent':True,'new_provider_requests':0,
          'new_collector_launches':0,'AWS_independently_verified_state':'stopped','saved_process_receipts_checked':603,
          'unknown_consumers':0,'final_sample_admitted':False})
        self.stack.enter_context(patch.object(m,'EXPIRED_V7_TERMINAL',terminal))
        required=[*historical.values(),old_ledger,expired_ref,check_ref,failed_ref]
        self.stack.enter_context(patch.object(m,'OLD_V7_PROFILE',{'ledger_files':len(historical),
          'ledger_file_bytes':sum(item['bytes'] for item in historical.values()),'required_files':len(required),
          'required_bytes':sum(item['bytes'] for item in required),'next_chunk':None,'next_attempt':None,'reused_keys':c.K}))
        self.certificate_value=m.certificate_value(self.inputs,self.private_ref)
        self.certificate_ref=self.save(m.CERTIFICATE_PATH,self.certificate_value)
        names=m.EXTERNAL_SOURCE_FILES|set(caller7.SOURCE_FILES)
        self.assertEqual(len(names),43)
        self.historical_qa_blobs={name:('synthetic frozen QA1 epoch '+name).encode() for name in names}
        self.stack.enter_context(patch.object(m.scale,'git_blob',side_effect=lambda head,name:
          self.historical_qa_blobs[name] if head==m.FAILED_QA_HEAD else c.blobs[name]))
        path=self.ops/'full_header_v8_linux_fixtures_v1.log';path.write_bytes(b'synthetic failed historical QA\n')
        log=m.ref(m.bound(path))
        self.failed_qa={'schema_version':'polymarket_header_v8_linux_fixture_receipt_v1','status':'failed_closed',
          'source_head':m.FAILED_QA_HEAD,'source_clean_before':True,'source_clean_after':True,'exit_code':1,
          'failures':1,'errors':0,'tests_run':247,'tests_skipped':0,'provider_requests':0,
          'command':['/home/ubuntu/venv/bin/python','-m','unittest',*(
            'tests.'+name for name in ('test_polymarket_exact_header_campaign_v8','test_polymarket_exact_header_campaign_v6',
            'test_polymarket_exact_header_campaign_v5','test_polymarket_exact_header_campaign_v4',
            'test_polymarket_exact_header_campaign_v3','test_launch_polymarket_oct1_stage_v7',
            'test_prepare_polymarket_exact_header_campaign_v8'))],
          'interpreter':'/home/ubuntu/venv/bin/python','log':log,'log_truncated':False,
          'full_log_bytes':log['bytes'],'full_log_sha256':log['sha256'],
          'tested_source_files':{name:{'bytes':len(raw),'sha256':m.digest(raw)} for name,raw in self.historical_qa_blobs.items()}}
        self.failed_qa_refs={'qa':self.save(self.ops/'full_header_v8_linux_fixtures_v1.json',self.failed_qa),'log':log}
        self.stack.enter_context(patch.object(m,'FAILED_QA1_REFS',self.failed_qa_refs))
        current={item['path']:item for item in [*required,terminal,self.certificate_ref,*self.failed_qa_refs.values()]}
        self.ledger=self.save(self.ops/'header_v8_external_control_ledger_v1.json',{
          'schema_version':m.EXTERNAL_SCHEMA,'files':list(current.values()),'future_reserved_bytes':786432,
          'future_reserved_files':9,'ceiling_bytes':2097152})
        self.value={'base':self.base,'stopped_v6':self.handoff_ref,'rpc':dict(self.rpc),'external_controls':self.ledger}
        self.reboot=m.validate_reboot(self.certificate_ref,self.value,self.inputs,self.private_ref)

    def save(self,path,value):
        path.write_bytes(m.encode(value));return m.ref(m.bound(path))
    def validate(self):return m.validate_prior(self.value,self.output,self.source,self.inputs,self.reboot)
    def refresh_certificate(self,value=None):
        self.certificate_value=m.certificate_value(self.inputs,self.private_ref) if value is None else value
        self.certificate_ref=self.save(m.CERTIFICATE_PATH,self.certificate_value)
        ledger=json.loads(Path(self.ledger['path']).read_bytes())
        next(item for item in ledger['files'] if item['path']==str(m.CERTIFICATE_PATH)).update(self.certificate_ref)
        self.ledger=self.save(Path(self.ledger['path']),ledger);self.value['external_controls']=self.ledger
    def refresh_ledger(self,value):
        self.ledger=self.save(Path(self.ledger['path']),value);self.value['external_controls']=self.ledger
    def device_hop(self):
        """Replace only the synthetic OS stat boundary, not any validator."""
        generic=m.legacy.identity;private=m.pilot.identity
        def current_generic(path):
            result=generic(path)
            if self.root in Path(path).parents:result['device']=77002 if Path(path)==self.private else 77001
            return result
        def current_private(path):
            result=private(path)
            if self.root in Path(path).parents:result['device']=77002 if Path(path)==self.private else 77001
            return result
        modules=(m,m.previous,m.previous.previous,m.previous.previous.previous,m.legacy)
        for module in modules:self.stack.enter_context(patch.object(module,'identity',side_effect=current_generic))
        self.stack.enter_context(patch.object(m.pilot,'identity',side_effect=current_private))
        self.env.update(data_device=77001,root_device=77002)
        self.inputs={name:m.reference(m.ref(item),m.CAPS['baseline_bytes']) for name,item in self.inputs.items()}
        self.refresh_certificate()
        self.reboot=m.validate_reboot(self.certificate_ref,self.value,self.inputs,self.private_ref)
    def campaign(self,source_effect=None,prior_effect=None,transport=None,declared=None):
        value={'schema_version':m.SCHEMA,'status':'root_exact_header_campaign_admitted','caps':m.CAPS,
          'consumer_head':self.source['head'],'run_dir':str(self.output),'raw_clocks_only':True,
          'final_sample_admitted':False,'inputs':{name:m.ref(item) for name,item in self.inputs.items()},
          'private_file':self.private_ref,'actual_keys':self.c.c.K,'conservative_new_runtime_seconds':5000,
          'prior':deepcopy(self.value),'reboot_equivalence':self.certificate_ref}
        if declared is not None:value['prior']['rpc']['elapsed_seconds']=declared
        admission=self.save(self.ops/'full_header_v8_admission_v1.json',value)
        with ExitStack() as stack:
            for context in (patch.object(m,'source_snapshot',side_effect=source_effect,return_value=self.source),
              patch.object(m,'RETAINED_RUNTIME_FLOOR',0),patch.object(m.time,'time',return_value=m.datetime.fromisoformat(self.drain['checked_utc']).timestamp()),
              patch.object(m,'request_deadline',return_value=nullcontext()),
              patch.object(m.inventory,'resource_state',return_value=old6.old.fixture.RESOURCES),
              patch.object(m,'owner_identity',return_value={'hostname':'fixture','boot_id':NEW_BOOT,'pid':124,'birth_ticks':1})):
                stack.enter_context(context)
            if prior_effect:stack.enter_context(patch.object(m,'validate_prior',side_effect=prior_effect))
            return m.run_campaign(Path(admission['path']),admission['sha256'],self.source['head'],self.output,
                self.private,transport=transport or self.c.c.transport)


class ColdBootTests(unittest.TestCase):
    def case(self):
        case=ColdBootFixture('runTest');case.setUp();self.addCleanup(case.doCleanups);return case

    def test_failed_qa1_reopens_all43_original_epoch_blobs_not_current_source(self):
        case=self.case()
        def frozen(head,name):
            self.assertEqual(head,m.FAILED_QA_HEAD);return case.historical_qa_blobs[name]
        with patch.object(m.scale,'git_blob',side_effect=frozen) as blobs:
            proof=m.failed_qa1_proof()
        self.assertEqual(blobs.call_count,43)
        self.assertEqual({call.args[1] for call in blobs.call_args_list},set(case.historical_qa_blobs))
        self.assertEqual({key:m.ref(value) for key,value in proof['captures'].items()},case.failed_qa_refs)
        self.assertNotEqual(proof['qa']['tested_source_files']['scripts/collect_polymarket_exact_header_campaign_v8.py'],
          case.source['files']['scripts/collect_polymarket_exact_header_campaign_v8.py'])
        self.assertIn('full_header_v8_linux_fixtures_v2.json',m.NEW_OP_NAMES)
        self.assertIn('full_header_v8_linux_fixtures_v2.log',m.NEW_OP_NAMES)

    def test_failed_qa1_and_log_are_mandatory_in_consumer_closure(self):
        case=self.case();saved=json.loads(Path(case.ledger['path']).read_bytes())
        for role,binding in case.failed_qa_refs.items():
            ledger=deepcopy(saved);ledger['files']=[item for item in ledger['files'] if item['path']!=binding['path']]
            case.refresh_ledger(ledger)
            with self.subTest(role=role),self.assertRaisesRegex(m.Blocked,'external_old_controls_omitted'):case.validate()

    def test_failed_qa1_and_log_missing_or_mutated_cannot_be_skipped(self):
        case=self.case()
        for role,binding in case.failed_qa_refs.items():
            path=Path(binding['path']);raw=path.read_bytes();path.write_bytes(raw[:-1]+b' ')
            with self.subTest(role=role),self.assertRaises(m.Blocked):case.validate()
            path.write_bytes(raw);path.unlink()
            with self.subTest(role=role,missing=True),self.assertRaises(OSError):case.validate()
            path.write_bytes(raw)

    def test_failed_qa1_wrong_schema_epoch_counts_and43_name_map_rejected(self):
        case=self.case();saved=deepcopy(case.failed_qa)
        changes=(lambda v:v.update(schema_version='wrong'),lambda v:v.update(status='passed'),
          lambda v:v.update(source_head=case.source['head']),lambda v:v.update(tests_run=248),
          lambda v:v['tested_source_files'].pop('production_guard.py'),
          lambda v:v['tested_source_files'].update(unrecognized=v['tested_source_files'].pop('production_guard.py')),
          lambda v:v['tested_source_files']['production_guard.py'].update(bytes=True))
        for index,change in enumerate(changes):
            value=deepcopy(saved);change(value)
            refs=dict(case.failed_qa_refs,qa=case.save(Path(case.failed_qa_refs['qa']['path']),value))
            with self.subTest(index=index),patch.object(m,'FAILED_QA1_REFS',refs),self.assertRaises(m.Blocked):m.failed_qa1_proof()

    def test_failed_qa1_original_blob_drift_and_late_log_drift_rejected(self):
        case=self.case();name='scripts/collect_polymarket_exact_header_campaign_v8.py'
        raw=case.historical_qa_blobs[name];case.historical_qa_blobs[name]=raw+b' changed'
        with self.assertRaisesRegex(m.Blocked,'failed_qa1_source_epoch'):m.failed_qa1_proof()
        case.historical_qa_blobs[name]=raw
        def changed(head,name):
            path=Path(case.failed_qa_refs['log']['path']);value=path.read_bytes();path.write_bytes(value[:-1]+b' ')
            return case.historical_qa_blobs[name]
        with patch.object(m.scale,'git_blob',side_effect=changed),self.assertRaises(m.Blocked):m.failed_qa1_proof()

    def test_full_cold_certificate_and_real_old6_prior(self):
        case=self.case()
        self.assertEqual(case.reboot['private_current'],m.pilot.identity(case.private))
        self.assertEqual(len(case.reboot['private_current']),9)
        self.assertEqual(case.reboot['original_request']['current'],m.pilot.read_requests(
          Path(case.reboot['original_reference']['path']),case.reboot['original_reference']['sha256'])[1])
        prior=case.validate()
        self.assertIn(0,prior['sealed']);self.assertEqual(prior['rpc'],case.rpc)

    def test_real_device_hop_preserves_fresh_request_through_all_historical_validators(self):
        case=self.case();case.device_hop();prior=case.validate()
        self.assertIn(0,prior['sealed'])
        self.assertEqual(case.reboot['private_current']['device'],77002)
        request=case.reboot['original_request']['current']
        self.assertEqual(request['identity']['device'],77001)
        self.assertNotEqual(request,case.certificate_value['original_pilot_request']['v6'])
        stale=deepcopy(case.reboot)
        stale['original_request']['current']=case.certificate_value['original_pilot_request']['v6']
        with self.assertRaisesRegex(m.Blocked,'original_pilot_scale_binding_differs'):
            m.validate_prior(case.value,case.output,case.source,case.inputs,stale)

    def test_real_synthetic_campaign_current_owner8_and_completion8(self):
        case=self.case();case.device_hop();result=case.campaign()
        self.assertEqual(result['status'],m.SUCCESS,result)
        owner=json.loads((case.output/'OWNER.json').read_bytes())
        self.assertEqual(owner['schema_version'],'polymarket_exact_header_owner_v8')
        self.assertEqual(owner['inputs'],case.inputs)
        self.assertEqual(owner['reboot_equivalence'],case.reboot['binding'])
        self.assertEqual(owner['producer_start_utc'],m.datetime.fromisoformat(case.drain['checked_utc']).timestamp())
        complete=json.loads((case.output/'COMPLETE.json').read_bytes())
        self.assertEqual(complete['schema_version'],'polymarket_exact_header_campaign_completion_v8')
        self.assertFalse(complete['final_sample_admitted'])

    def test_certificate_requires_real_private_nine_fields_and_complete_roster(self):
        case=self.case();saved=deepcopy(case.certificate_value)
        changes=(lambda v:v['private_authority'].update(current=case.reboot['private_binding']['identity']),
          lambda v:v['inputs'].pop('keys'),lambda v:v.update(extra=True),
          lambda v:v['original_pilot_request']['current'].update(normalized_keys_sha256='0'*64))
        for change in changes:
            with self.subTest(change=changes.index(change)):
                value=deepcopy(saved);change(value);case.refresh_certificate(value)
                with self.assertRaises(m.Blocked):m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)

    def test_every_nondevice_private_identity_field_and_input_hash_are_bound(self):
        case=self.case();saved=deepcopy(case.certificate_value)
        for field in saved['private_authority']['current']:
            if field=='device':continue
            value=deepcopy(saved);before=value['private_authority']['current'][field]
            value['private_authority']['current'][field]=before+1 if type(before) is int else before+'x';case.refresh_certificate(value)
            with self.subTest(field=field),self.assertRaises(m.Blocked):
                m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)
        value=deepcopy(saved);value['inputs']['keys']['current']['sha256']='0'*64;case.refresh_certificate(value)
        with self.assertRaises(m.Blocked):m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)

    def test_boot_volume_root_size_and_boolean_device_are_not_waived(self):
        case=self.case();saved=deepcopy(case.certificate_value)
        old_boot=saved['inputs'] and case.owner['process']['boot_id']
        for field,value in (('boot_id',old_boot),('uuid','wrong'),('serial','wrong'),('fstype','apfs'),
          ('device_bytes',str(500*1024**3)),('root_uuid','wrong'),('data_device',True),('root_device',True)):
            changed=deepcopy(saved);changed['current_environment'][field]=value;case.refresh_certificate(changed)
            with self.subTest(field=field),self.assertRaises(m.Blocked):
                m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)

    def test_old_proofs_pin_and_unknown_process_rejected(self):
        case=self.case();value=deepcopy(case.certificate_value);value['previous_certificate']['sha256']='0'*64
        case.refresh_certificate(value)
        with self.assertRaises(m.Blocked):m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)
        case.refresh_certificate()
        with patch.object(m.legacy,'owner_state',return_value='unknown'),self.assertRaises(m.Blocked):
            m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)

    def test_certificate_must_be_in_new_ledger_not_added_to_literal_old_base(self):
        case=self.case();ledger=json.loads(Path(case.ledger['path']).read_bytes())
        ledger['files']=[item for item in ledger['files'] if item['path']!=str(m.CERTIFICATE_PATH)]
        case.refresh_ledger(ledger)
        with self.assertRaisesRegex(m.Blocked,'cold_certificate_not_new_ledger'):
            m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)
        case.value['base']=deepcopy(case.base)
        case.value['base']['files'].append(case.certificate_ref|{'category':'control','chunk':None})
        with self.assertRaises(m.Blocked):case.validate()

    def test_expired_v7_controls_and_all_four_resource_floors_are_retained(self):
        case=self.case()
        for key in case.rpc:
            case.value['rpc']=dict(case.rpc);case.value['rpc'][key]-=1
            with self.subTest(key=key),self.assertRaises(m.Blocked):case.validate()
        case.value['rpc']=dict(case.rpc)
        ledger=json.loads(Path(case.ledger['path']).read_bytes())
        ledger['files']=[item for item in ledger['files'] if item['path']!=m.EXPIRED_V7_REFS['failure']['path']]
        case.refresh_ledger(ledger)
        with self.assertRaisesRegex(m.Blocked,'external_old_controls_omitted'):case.validate()

    def test_expired_unlaunched_preparation_runtime_cannot_reset_to_stopped6_floor(self):
        case=self.case();refs=deepcopy(m.EXPIRED_V7_REFS);old=deepcopy(refs)
        expired=deepcopy(case.expired);expired['prior']['rpc']['elapsed_seconds']+=11
        refs['admission']=case.save(Path(refs['admission']['path']),expired)
        check=json.loads(Path(refs['check']['path']).read_bytes())
        check.update(admission=refs['admission'],prior_rpc=expired['prior']['rpc'])
        refs['check']=case.save(Path(refs['check']['path']),check)
        profile=dict(m.OLD_V7_PROFILE,required_bytes=m.OLD_V7_PROFILE['required_bytes']+
          sum(refs[key]['bytes']-old[key]['bytes'] for key in refs))
        case.stack.enter_context(patch.object(m,'EXPIRED_V7_REFS',refs))
        case.stack.enter_context(patch.object(m,'OLD_V7_PROFILE',profile))
        ledger=json.loads(Path(case.ledger['path']).read_bytes())
        for key in ('admission','check'):
            next(item for item in ledger['files'] if item['path']==refs[key]['path']).update(refs[key])
        case.refresh_ledger(ledger);case.refresh_certificate()
        case.reboot=m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)
        with self.assertRaisesRegex(m.Blocked,'expired_v7_charge_refunded'):case.validate()

    def test_external_pool_future_reservation_and_finite_path_allowlist(self):
        case=self.case();saved=json.loads(Path(case.ledger['path']).read_bytes())
        for field,value in (('future_reserved_bytes',m.EXTERNAL_FUTURE_BYTES-1),('future_reserved_files',8),
          ('future_reserved_bytes',2*1024**2),('future_reserved_files',256)):
            ledger=deepcopy(saved);ledger[field]=value;case.refresh_ledger(ledger)
            with self.subTest(field=field,value=value),self.assertRaises(m.Blocked):case.validate()
        ledger=deepcopy(saved);ledger['files'].append(case.save(case.ops/'unnamed_control.json',{}));case.refresh_ledger(ledger)
        with self.assertRaisesRegex(m.Blocked,'external_ref_scope'):case.validate()

    def test_full_namespace_extra_file_and_symlink_rejected(self):
        case=self.case();path=case.v6/'untracked.json';path.write_bytes(b'{}')
        with self.assertRaises(m.Blocked):case.validate()
        path.unlink();path.symlink_to(case.private)
        with self.assertRaisesRegex(m.Blocked,'v6_tree_type'):case.validate()

    def test_late_native_payload_mutation_blocks_final_reopen(self):
        case=self.case();original=m.verify_chunk;changed=False
        def verify(*args):
            nonlocal changed
            result=original(*args)
            if not changed and Path(args[0]).is_relative_to(case.v6):
                path=Path(args[0])/'headers.parquet';raw=path.read_bytes();path.write_bytes(raw[:-1]+bytes([raw[-1]^1]));changed=True
            return result
        with patch.object(m,'verify_chunk',side_effect=verify),self.assertRaises(m.Blocked):case.validate()
        self.assertTrue(changed)

    def test_inherited_native_hash_and_adjacent_clock_checks_remain_effective(self):
        case=self.case();root=Path(case.validate()['sealed'][0]['root'])
        keys=m.legacy.key_chunk_at(case.inputs['keys']['path'],0);rows,_=m.verify_chunk(root,keys,0)
        changed=deepcopy(keys);changed[0]['block_hash']='0x'+'0'*64
        with self.assertRaises((m.Blocked,m.pilot.PilotBlocked)):m.read_headers(root/'headers.parquet',changed)
        bad=dict(rows[1],timestamp=rows[0]['timestamp']-1)
        with self.assertRaises(m.pilot.PilotBlocked):m.pilot.validate_clock([rows[0],bad])

    def test_late_certificate_callback_private_change_is_detected(self):
        case=self.case();original=m.expired_v7_proof
        def changed():
            value=original();raw=case.private.read_bytes();case.private.write_bytes(raw[:-1]+b' ');return value
        with patch.object(m,'expired_v7_proof',side_effect=changed),self.assertRaises(m.Blocked):
            m.validate_reboot(case.certificate_ref,case.value,case.inputs,case.private_ref)

    def test_all_postdrain_wall_time_is_carried_once_with_exact_boundary(self):
        case=self.case();anchor=m.datetime.fromisoformat(case.drain['checked_utc']).timestamp()
        base=case.base_rpc['elapsed_seconds']+case.handoff['runtime_charge_seconds']
        self.assertEqual(m.required_carried_runtime(case.value,anchor),max(104777,math.ceil(base)))
        with patch.object(m,'RETAINED_RUNTIME_FLOOR',0):
            self.assertEqual(m.required_carried_runtime(case.value,anchor+1800),math.ceil(base+1800))
            self.assertEqual(m.required_carried_runtime(case.value,anchor+1800.1),math.ceil(base+1800.1))
        for invalid in (anchor-1,True,float('nan')):
            with self.subTest(invalid=invalid),self.assertRaises(m.Blocked):m.required_carried_runtime(case.value,invalid)

    def test_understated_postdrain_carry_rejects_before_output_or_transport(self):
        case=self.case()
        with self.assertRaisesRegex(m.Blocked,'postdrain_runtime_understated'):
            case.campaign(declared=case.base_rpc['elapsed_seconds']+case.handoff['runtime_charge_seconds']-1,
              transport=lambda *a,**k:self.fail('transport invoked'))
        self.assertFalse(case.output.exists())

    def test_pending_callback_mutation_withholds_root_completion(self):
        case=self.case();original=m.validate_prior
        def changed(*args):
            value=original(*args)
            if (case.output/'.COMPLETE.json.pending').exists():
                raw=case.private.read_bytes();case.private.write_bytes(raw[:-1]+b' ')
            return value
        result=case.campaign(prior_effect=changed)
        self.assertEqual(result['status'],'blocked',result)
        self.assertFalse((case.output/'COMPLETE.json').exists())


class PolicyTests(unittest.TestCase):
    def test_alternate_fresh_run_rejected_before_input_or_transport(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();alternate=root/'alternate'
            with patch.object(m,'RUN_ROOT',root),patch.object(m,'RUN',root/'fixed'),\
              patch.object(m,'read_json',side_effect=AssertionError('input read')),self.assertRaisesRegex(m.Blocked,'destination_not_fresh'):
                m.run_campaign(root/'missing','0'*64,'e'*40,alternate,root/'private',
                  transport=lambda *a,**k:self.fail('transport invoked'))
            self.assertFalse(alternate.exists())

    def test_full_transport_pacing_forecast_and_journal_ast_are_unchanged_from7(self):
        for name in ('forecast_seconds','wait_for_pace','paced_start','remaining_key_count',
          'required_new_runtime','stopped_journal_usage'):
            with self.subTest(name=name):
                self.assertEqual(ast.dump(ast.parse(inspect.getsource(getattr(m,name)))),
                  ast.dump(ast.parse(inspect.getsource(getattr(before,name)))))
        self.assertEqual(ast.dump(ast.parse(inspect.getsource(m.RPC))),ast.dump(ast.parse(inspect.getsource(before.RPC))))
        self.assertIs(m.RPC.call.__globals__,m.__dict__)
        self.assertIs(m.CAPS,m.previous.CAPS)
        self.assertEqual(m.METHOD_ITEMS_PER_SECOND,400.)
        self.assertEqual(m.previous.previous.METHOD_ITEMS_PER_SECOND,150.)
        self.assertEqual(m.EXTERNAL_FUTURE_BYTES,786432)
        self.assertEqual(m.EXTERNAL_FUTURE_FILES,9)

    def pacing(self):
        case=old6.old.PacedRPCTests('runTest');case.setUp();self.addCleanup(case.doCleanups)
        self.addCleanup(patch.stopall);patch.object(m,'request_deadline',return_value=nullcontext()).start()
        return case

    def test_400_start_to_start_shared_budget_with_chain_no_catchup(self):
        case=self.pacing()
        m.RPC('private',case.b,case.transport,'one.jsonl').call(case.keys())
        m.RPC('private',case.b,case.transport,'two.jsonl').call(case.keys(101,50))
        m.RPC('private',case.b,case.transport,'three.jsonl').call()
        self.assertEqual([round(item[0],6) for item in case.calls],[0.,.25,.375])
        case.tick=10
        m.RPC('private',case.b,case.transport,'four.jsonl').call(case.keys(201))
        m.RPC('private',case.b,case.transport,'five.jsonl').call(case.keys(301))
        self.assertEqual([round(item[0],6) for item in case.calls[-2:]],[10.,10.25])

    def test_429_no_secret_diagnostics_and_every_retry_charged(self):
        case=self.pacing()
        with self.assertRaises(m.Blocked):m.RPC('private',case.b,case.errors([429]),'rpc.jsonl').call(case.keys())
        self.assertEqual([round(item[0],6) for item in case.calls],[0.,2.01,6.02])
        self.assertEqual(case.b.rpc['http_attempts'],3);self.assertEqual(case.b.rpc['method_items'],300)
        self.assertNotIn('SECRET_KEY',''.join(path.read_text() for path in case.root.rglob('*.json')))

    def test_open_durable_request_and_short_torn_tail_never_refund(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory).resolve()/'rpc.jsonl'
            start={'e':'s','i':1,'k':-1,'n':0,'j':1,'b':m.CAPS['individual_response_bytes'],'t':1,
              'h':m.digest(m.encode({'jsonrpc':'2.0','id':1,'method':'eth_chainId','params':[]}))}
            path.write_bytes(m.encode(start)+b'{"')
            usage,starts,finishes,tail=m.stopped_journal_usage(path)
            self.assertEqual(usage,{'http_attempts':1,'method_items':1,'response_bytes':m.CAPS['individual_response_bytes']})
            self.assertEqual(tail,b'{"');self.assertEqual(len(starts),1);self.assertFalse(finishes)
            path.write_bytes(m.encode(start)+b'{}\n')
            with self.assertRaises(m.Blocked):m.stopped_journal_usage(path)

    def test_guard_runs_before_any_production_input_read(self):
        with patch.object(m,'require_production_host',side_effect=RuntimeError('fixture_guard')),\
          patch.object(m,'run_campaign') as run,self.assertRaisesRegex(RuntimeError,'fixture_guard'):
            m.main(['--admission','/missing','--admission-sha256','0'*64,'--expected-head','0'*40,
              '--run-dir',str(m.RUN),'--rpc-private-file','/missing'])
        run.assert_not_called()


if __name__=='__main__':unittest.main()
