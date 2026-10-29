"""Synthetic stopped-prefix and 400/s pacing fixtures. No network or real data."""
from contextlib import ExitStack,nullcontext
from copy import deepcopy
from pathlib import Path
import ast
import inspect
import json
import tempfile
import unittest
from unittest.mock import patch
from scripts import collect_polymarket_exact_header_campaign_v7 as m
from tests import test_polymarket_exact_header_campaign_v5 as old


class PacingTests(unittest.TestCase):
    def setUp(self):
        self.c=old.PacedRPCTests();self.c.setUp();self.addCleanup(self.c.doCleanups)
        self.addCleanup(patch.stopall)
        patch.object(m,"request_deadline",return_value=nullcontext()).start()

    def test_own_defining_globals_and_old_rate_unchanged(self):
        self.assertIs(m.RPC.call.__globals__,m.__dict__)
        self.assertIs(m.CAPS,m.previous.CAPS)
        self.assertEqual(m.previous.previous.METHOD_ITEMS_PER_SECOND,150.)
        self.assertEqual(m.METHOD_ITEMS_PER_SECOND,400.)

    def test_shared_400_rate_includes_chain_and_new_instances(self):
        c=self.c
        m.RPC("private",c.b,c.transport,"one.jsonl").call(c.keys())
        m.RPC("private",c.b,c.transport,"two.jsonl").call(c.keys(101,50))
        m.RPC("private",c.b,c.transport,"three.jsonl").call()
        self.assertEqual([round(item[0],6) for item in c.calls],[0.,.25,.375])
        self.assertEqual(c.b.rpc["method_items"],151)

    def test_no_catchup_after_idle_or_durable_start(self):
        c=self.c
        m.RPC("private",c.b,c.transport,"one.jsonl").call(c.keys())
        c.tick=10
        original=c.b.append
        def slow(path,item,**kw):
            value=original(path,item,**kw)
            if item["e"]=="s":c.tick+=.2
            return value
        with patch.object(c.b,"append",side_effect=slow):
            m.RPC("private",c.b,c.transport,"two.jsonl").call(c.keys(101))
        m.RPC("private",c.b,c.transport,"three.jsonl").call(c.keys(201))
        self.assertAlmostEqual(c.calls[1][0],10.2)
        self.assertAlmostEqual(c.calls[2][0],10.45)

    def test_numeric_429_retries_same_batch_2_then_4_seconds_no_secret_text(self):
        c=self.c
        with self.assertRaises(m.Blocked):m.RPC("private",c.b,c.errors([429]),"rpc.jsonl").call(c.keys())
        self.assertEqual([round(item[0],6) for item in c.calls],[0.,2.01,6.02])
        self.assertEqual(c.b.rpc["http_attempts"],3)
        self.assertEqual(c.b.rpc["method_items"],300)
        self.assertNotIn("SECRET_KEY",''.join(p.read_text() for p in c.root.rglob('*.json')))

    def test_durable_start_failure_has_full_conservative_counters(self):
        c=self.c;original=c.b.append
        def fail(path,item,**kw):
            original(path,item,**kw)
            if item["e"]=="s":raise m.Blocked("runtime_limit")
        with patch.object(c.b,"append",side_effect=fail),self.assertRaises(m.Blocked):
            m.RPC("private",c.b,c.transport,"rpc.jsonl").call(c.keys())
        self.assertEqual(c.calls,[])
        self.assertEqual(c.b.rpc["http_attempts"],1)
        self.assertEqual(c.b.rpc["method_items"],100)
        self.assertEqual(c.b.rpc["response_bytes"],m.CAPS["individual_response_bytes"])

    def test_forecast_uses_400_but_retains_reserve_and_measured_bound(self):
        self.assertEqual(m.forecast_seconds(40000,100,0),3700.)
        self.assertEqual(m.forecast_seconds(40000,100,1),4000.)
        self.assertEqual(m.previous.forecast_seconds(40000,100,0),400*100/150+3600)


class StoppedFixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.ops=self.root/'operations';self.ops.mkdir()
        self.v6=self.root/'v6';self.v6.mkdir();self.output=self.root/'v7';self.tick=0.
        self.keys=[{"block_number":i,"block_hash":None} for i in range(1,4)]
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        for ctx in (patch.object(m,'RUN_ROOT',self.root),patch.object(m,'V6_ROOT',self.v6),patch.object(m,'OP_ROOT',self.ops),
                    patch.object(m,'HANDOFF_PATH',self.ops/'full_header_v6_stopped_handoff_v1.json'),
                    patch.object(m,'EXIT_PATH',self.ops/'full_header_v6.EXIT.json'),
                    patch.object(m,'DRAIN_PATH',self.ops/'full_header_v6_root_drain_v7_handoff_v1.json'),
                    patch.object(m,'OLD_EXTERNAL_PATH',self.ops/'header_v6_external_control_ledger_v1.json'),
                    patch.object(m,'ACTUAL_KEYS',3),patch.object(m.legacy,'owner_state',return_value='dead'),
                    patch.object(m.legacy,'producer_source'),patch.object(m.legacy,'key_chunk_at',return_value=self.keys),
                    patch.object(m.inventory,'resource_state',return_value=old.fixture.RESOURCES),
                    patch.object(m.time,'monotonic',side_effect=lambda:self.tick),patch.object(m.time,'sleep',side_effect=self.sleep),
                    patch.object(m,'request_deadline',return_value=nullcontext())):self.stack.enter_context(ctx)
        (self.root/'keys').write_bytes(b'fixture key file')
        self.inputs={'keys':m.bound(self.root/'keys')}
        self.source={'head':'f'*40,'files':{name:{'bytes':1,'sha256':'0'*64} for name in m.SOURCE_FILES}}
        empty=dict.fromkeys(('baseline','accepted','control','failed'),0)
        self.base_rpc=dict.fromkeys(('http_attempts','method_items','response_bytes','elapsed_seconds'),0)
        self.base={'files':[],'rpc':self.base_rpc}
        self.prior={'files':{},'bytes':empty,'rpc':dict(self.base_rpc),'sealed':{},'attempts':{},'chunk_usage':{},
            'observed_max_call_seconds':.01,'observation_elapsed_seconds':.01}
        key_entry=m.ref(self.inputs['keys'])|{'category':'baseline','chunk':None}
        self.base['files'].append(key_entry)
        self.prior['files'][key_entry['path']]=self.inputs['keys']|{'category':'baseline','chunk':None}
        self.prior['bytes']['baseline']+=key_entry['bytes']
        self.admission={'schema_version':m.previous.SCHEMA,'consumer_head':m.V6_HEAD,'run_dir':str(self.v6),'caps':m.CAPS,
            'actual_keys':3,'prior':self.base,'inputs':{name:m.ref(v) for name,v in self.inputs.items()},
            'raw_clocks_only':True,'final_sample_admitted':False}
        self.certificate=self.save(self.ops/'certificate.json',{})
        self.private=self.root/'private';self.private.write_bytes(b'https://rpc.invalid/fixture')
        self.private_ref=m.ref(m.bound(self.private))
        self.reboot={'binding':m.reference(self.certificate),'original_reference':self.inputs['keys'],
            'private_binding':m.reference(self.private_ref),'private_current':m.pilot.identity(self.private),
            'environment':{'fixture':True}}
        self.process={'hostname':'fixture','boot_id':'5b441f68-0ef0-424b-b917-58e5b03b0b8d','pid':20,'birth_ticks':30}
        self.owner={'schema_version':'polymarket_exact_header_owner_v6','run_dir':str(self.v6),'source':{'head':m.V6_HEAD,'files':{}},
            'inputs':self.inputs,'caps':m.CAPS,'process':self.process,'raw_clocks_only':True,'final_sample_admitted':False,
            'reboot_equivalence':self.reboot['binding']}
        external=self.save(m.OLD_EXTERNAL_PATH,{'files':[]})
        item=external|{'category':'control','chunk':None};self.base['files'].append(item)
        self.prior['files'][external['path']]=m.reference(external)|{'category':'control','chunk':None}
        self.prior['bytes']['control']+=external['bytes']
        admission_ref=self.save(self.ops/'full_header_v6_admission_v1.json',self.admission)
        self.owner['admission']=m.reference(admission_ref)
        owner_ref=self.save(self.v6/'OWNER.json',self.owner)
        child=self.process|{'process_group':20,'session':20}
        caller=child|{'pid':10,'birth_ticks':15,'process_group':10,'session':10}
        self.launch={'schema_version':'polymarket_oct1_stage_launcher_v5','stage':'headers','status':'producer_dispatched_owned_session',
            'source':{'head':m.V6_HEAD},'output_dir':str(self.v6),'admission':m.reference(admission_ref),
            'ownership_unknown':False,'retry_or_resume':False,'caller':caller,'child':child,'command':['fixed']}
        launch_ref=self.save(self.ops/'full_header_v6.LAUNCH.json',self.launch)
        exit_ref=self.save(m.EXIT_PATH,self.launch|{'status':'producer_exited','error_code':None,'producer_exit_code':-15,'owned_group_drained':True,
            'children_still_running':False,'stdout_eof_observed':True,'source_and_admission_unchanged':True,'elapsed_seconds':10.})
        self.refs={'owner':owner_ref,'admission':admission_ref,'launch':launch_ref,'exit':exit_ref}
        old_closure=[external,admission_ref,launch_ref,exit_ref]
        for name in m.OLD_FUTURE_NAMES:
            if str(self.ops/name) not in {item['path'] for item in old_closure}:old_closure.append(self.save(self.ops/name,{}))
        self.drain={'schema_version':'polymarket_header_v6_root_drain_v7_handoff_v1','status':'root_verified_stopped_header_v6',
            'run_dir':str(self.v6),'source_head':m.V6_HEAD,'owner':owner_ref,'launch':launch_ref,'caller_exit':exit_ref,
            'header_owned_processes_drained':True,'remaining_owned_group_members':[],'no_owned_workers_or_transfers':True,
            'host_idle_claimed':False,'final_sample_admitted':False,'checked_utc':'2026-10-29T17:00:00+00:00',
            'external_control_closure':old_closure}
        self.refs['drain']=self.save(m.DRAIN_PATH,self.drain)
        self.stack.enter_context(patch.object(m,'V6_REFS',{k:self.refs[k] for k in ('owner','admission','launch')}))
        self.stack.enter_context(patch.object(m.previous,'validate_prior',side_effect=lambda *args:deepcopy(self.prior)))
        b=m.Budget(self.v6,deepcopy(self.prior),admission_ref['bytes'],0);b.next_dispatch_at=0;b.remaining_keys=3
        b.files['OWNER.json']={**{k:owner_ref[k] for k in ('bytes','sha256')},'category':'control','chunk':None}
        b.used['control']+=owner_ref['bytes']
        b.chunk=0;b.chunk_started=0;b.chunk_base=dict(self.base_rpc);b.chunk_start_rpc=dict(b.rpc)
        base='chunks/c000000/a01'
        rows=m.RPC('private',b,self.transport,base+'/rpc.jsonl').fetch(self.keys)
        headers=b.write(base+'/headers.parquet',m.scale.compact_parquet(rows),'failed')
        usage,starts,finishes=m.journal_usage(self.v6/base/'rpc.jsonl')
        manifest={'schema_version':'polymarket_exact_header_chunk_v3','status':'exact_header_chunk_complete_raw_clocks_only',
            'chunk':0,'attempt':1,'offset':0,'keys':3,'keys_sha256':m.digest(m.encode(self.keys)),
            'headers':headers|{'path':'headers.parquet'},'rpc_usage':usage,'active_seconds_charged':b.chunk_elapsed(),
            'raw_clocks_only':True,'final_sample_admitted':False}
        mb=b.write(base+'/manifest.json',m.encode(manifest))
        b.seal(base+'/COMPLETE.json',{'status':manifest['status'],'manifest':mb|{'path':'manifest.json'}},lambda:None)
        self.rpc=dict(b.rpc,elapsed_seconds=20.)
        self.handoff={'schema_version':'polymarket_header_v6_stopped_handoff_v1','status':'root_bound_stopped_v6_header_prefix',
            'run_dir':str(self.v6),'source_head':m.V6_HEAD,'refs':self.refs,'files':[],
            'runtime_charge_seconds':20.,'chunk_runtime_seconds':{'0':5.},'raw_clocks_only':True,'final_sample_admitted':False}
        self.refresh()

    def sleep(self,value):self.tick+=value

    def transport(self,endpoint,payload,**kw):
        self.tick+=.01
        requests=payload if isinstance(payload,list) else [payload]
        if hasattr(self,'requests'):self.requests.append(payload)
        rows=[{'jsonrpc':'2.0','id':r['id'],'result':'0x89' if r['method']=='eth_chainId' else old.fixture.rpc_header(int(r['params'][0],16))} for r in requests]
        raw=m.encode(rows if isinstance(payload,list) else rows[0])
        return {'raw':raw,'charged_bytes':len(raw),'complete':True,'status':200,'error_code':None,'proven_oversize':False}

    def save(self,path,value):
        path.write_bytes(m.encode(value));return m.ref(m.bound(path))

    def refresh(self):
        files=[]
        for p in sorted(self.v6.rglob('*')):
            if not p.is_file():continue
            chunk=0 if 'chunks' in p.parts else None
            category='accepted' if p.name=='headers.parquet' and (p.parent/'COMPLETE.json').exists() else 'failed' if p.name=='headers.parquet' else 'control'
            files.append(m.ref(m.bound(p))|{'category':category,'chunk':chunk})
        self.handoff['files']=files
        self.value={'base':self.base,'stopped_v6':self.save(m.HANDOFF_PATH,self.handoff),'rpc':self.rpc}
        refs={x['path']:x for x in self.drain['external_control_closure']}
        refs.update({x['path']:x for x in (self.value['stopped_v6'],*self.refs.values()) if self.v6 not in Path(x['path']).parents})
        external={'schema_version':m.EXTERNAL_SCHEMA,'files':list(refs.values()),'future_reserved_bytes':m.EXTERNAL_FUTURE_BYTES,
            'future_reserved_files':m.EXTERNAL_FUTURE_FILES,'ceiling_bytes':m.CAPS['external_control_reservation_bytes']}
        self.value['external_controls']=self.save(self.ops/'header_v7_external_control_ledger_v1.json',external)

    def validate(self):return m.validate_prior(self.value,self.output,self.source,self.inputs,self.reboot)


class CarryTests(StoppedFixture):
    def test_each_new_seal_semantics_checked_once(self):
        with patch.object(m,'verify_chunk',wraps=m.verify_chunk) as verify:
            self.assertIn(0,self.validate()['sealed'])
        self.assertEqual(verify.call_count,1)
        self.assertEqual(verify.call_args.args[0],self.v6/'chunks/c000000/a01')

    def test_late_payload_drift_still_blocks_after_semantic_cache(self):
        verify=m.verify_chunk
        def changed(*args):
            result=verify(*args)
            path=args[0]/'headers.parquet';raw=path.read_bytes()
            path.write_bytes(raw[:-1]+bytes([raw[-1]^1]))
            return result
        with patch.object(m,'verify_chunk',side_effect=changed),self.assertRaisesRegex(m.Blocked,'v6_final_reopen_changed'):
            self.validate()

    def test_seal_runtime_contains_old_chunk_floor_exactly_once(self):
        self.base_rpc['elapsed_seconds']=130.;self.prior['rpc']['elapsed_seconds']=130.
        self.prior['chunk_usage'][0]={'http_attempts':0,'method_items':0,'response_bytes':0,'elapsed_seconds':120.}
        self.rpc['elapsed_seconds']=150.
        self.refs['admission']=self.save(Path(self.refs['admission']['path']),self.admission)
        self.owner['admission']=m.reference(self.refs['admission'])
        self.refs['owner']=self.save(Path(self.refs['owner']['path']),self.owner)
        self.launch['admission']=m.reference(self.refs['admission'])
        self.refs['launch']=self.save(Path(self.refs['launch']['path']),self.launch)
        path=Path(self.refs['exit']['path']);exited=json.loads(path.read_bytes());exited['admission']=self.launch['admission']
        self.refs['exit']=self.save(path,exited)
        self.drain.update(owner=self.refs['owner'],launch=self.refs['launch'],caller_exit=self.refs['exit'])
        updates={v['path']:v for v in self.refs.values()}
        self.drain['external_control_closure']=[updates.get(v['path'],v) for v in self.drain['external_control_closure']]
        self.refs['drain']=self.save(m.DRAIN_PATH,self.drain)
        m.V6_REFS.update({key:self.refs[key] for key in ('owner','admission','launch')})
        base=self.v6/'chunks/c000000/a01';manifest=json.loads((base/'manifest.json').read_bytes())
        manifest['active_seconds_charged']=125.
        mb=self.save(base/'manifest.json',manifest)
        self.save(base/'COMPLETE.json',{'status':manifest['status'],'manifest':mb|{'path':'manifest.json'}})
        self.refresh();prior=self.validate()
        self.assertEqual(prior['chunk_usage'][0]['elapsed_seconds'],125.)
        self.assertEqual(prior['rpc']['elapsed_seconds'],150.)
        self.handoff['chunk_runtime_seconds']['0']=4.;self.refresh()
        with self.assertRaisesRegex(m.Blocked,'seal_runtime_understated'):self.validate()

    def test_exact_seal_reused_by_reference_and_all_bytes_charged(self):
        prior=self.validate();self.assertEqual(prior['sealed'][0]['root'],str(self.v6/'chunks/c000000/a01'))
        self.assertEqual(prior['rpc'],self.rpc);self.assertEqual(len(prior['attempts'][0]),1)
        self.assertEqual(prior['chunk_usage'][0]['method_items'],3)
        self.assertGreater(prior['bytes']['accepted'],0);self.assertFalse(self.output.exists())

    def test_live_or_unknown_old_owner_blocks(self):
        for state in ('live','unknown'):
            with self.subTest(state=state),patch.object(m.legacy,'owner_state',return_value=state),self.assertRaises(m.Blocked):self.validate()

    def test_torn_tail_or_unclosed_start_never_becomes_sealed_coverage(self):
        p=self.v6/'chunks/c000000/a01/rpc.jsonl';p.write_bytes(p.read_bytes()+b'{"e":"s"')
        self.refresh()
        with self.assertRaises(m.Blocked):self.validate()

    def test_unsealed_headers_and_partial_tail_preserved_and_full_reservation_charged(self):
        base=self.v6/'chunks/c000000/a01';(base/'COMPLETE.json').unlink()
        p=base/'rpc.jsonl';p.write_bytes(p.read_bytes()+b'{"e":"s"')
        self.rpc['http_attempts']+=1;self.rpc['method_items']+=100;self.rpc['response_bytes']+=m.CAPS['individual_response_bytes']
        self.refresh();prior=self.validate()
        self.assertEqual(prior['sealed'],{});self.assertGreater(prior['bytes']['failed'],0)
        self.assertEqual(prior['chunk_usage'][0]['http_attempts'],2)
        self.assertEqual(p.read_bytes()[-8:],b'{"e":"s"')

    def test_missing_extra_or_symlink_tree_files_block(self):
        p=self.v6/'unexpected';p.write_bytes(b'x')
        with self.assertRaises(m.Blocked):self.validate()
        p.unlink();p.symlink_to(self.ops/'certificate.json')
        with self.assertRaises(m.Blocked):self.validate()

    def test_payload_drift_and_wrong_native_expected_hash_block(self):
        p=self.v6/'chunks/c000000/a01/headers.parquet';raw=p.read_bytes();p.write_bytes(raw[:-1]+b'x')
        with self.assertRaises(m.Blocked):self.validate()

    def test_cumulative_attempt_response_and_runtime_cannot_reset(self):
        for key in ('http_attempts','method_items','response_bytes','elapsed_seconds'):
            old_value=self.rpc[key];self.rpc[key]-=1
            with self.subTest(key=key),self.assertRaises(m.Blocked):self.validate()
            self.rpc[key]=old_value

    def test_old_controls_cannot_be_dropped_or_shared_pool_reset(self):
        p=Path(self.value['external_controls']['path']);value=json.loads(p.read_bytes())
        value['files']=value['files'][1:];self.value['external_controls']=self.save(p,value)
        with self.assertRaises(m.Blocked):self.validate()

    def test_future_partition_and_ceiling_remain_fixed(self):
        self.assertEqual(m.EXTERNAL_FUTURE_BYTES,1245184)
        p=Path(self.value['external_controls']['path']);value=json.loads(p.read_bytes());value['future_reserved_bytes']-=1
        self.value['external_controls']=self.save(p,value)
        with self.assertRaises(m.Blocked):self.validate()

    def test_controlled_caller_interruption_without_failed_file_is_accepted(self):
        path=Path(self.refs['exit']['path']);exited=json.loads(path.read_bytes())
        exited.update(status='caller_failed_closed',error_code='caller_interrupted',producer_exit_code=-15)
        self.refs['exit']=self.save(path,exited);self.drain['caller_exit']=self.refs['exit']
        self.drain['external_control_closure']=[self.refs['exit'] if x['path']==str(path) else x for x in self.drain['external_control_closure']]
        self.refs['drain']=self.save(m.DRAIN_PATH,self.drain);self.refresh()
        prior=self.validate();self.assertIn(0,prior['sealed']);self.assertFalse((self.v6/'FAILED.json').exists())

    def test_sigterm_open_durable_start_charges_full_allowance_and_retains_attempt(self):
        base=self.v6/'chunks/c000000/a01';(base/'COMPLETE.json').unlink()
        journal=base/'rpc.jsonl'
        requests=[{'jsonrpc':'2.0','id':4+i,'method':'eth_getBlockByNumber','params':[hex(k['block_number']),False]} for i,k in enumerate(self.keys)]
        start={'e':'s','i':2,'k':0,'n':3,'j':4,'b':m.CAPS['individual_response_bytes'],'t':1000,'h':m.digest(m.encode(requests))}
        with journal.open('ab') as handle:handle.write(m.encode(start))
        self.rpc['http_attempts']+=1;self.rpc['method_items']+=3;self.rpc['response_bytes']+=m.CAPS['individual_response_bytes']
        self.refresh();prior=self.validate()
        self.assertEqual(prior['sealed'],{});self.assertEqual(len(prior['attempts'][0]),1)
        self.assertEqual(prior['chunk_usage'][0]['method_items'],6)
        self.assertEqual(prior['chunk_usage'][0]['response_bytes'],self.rpc['response_bytes'])
        self.assertTrue(journal.read_bytes().endswith(m.encode(start)))

    def test_duplicate_roster_entries_or_unknown_large_torn_records_block(self):
        self.handoff['files'].append(deepcopy(self.handoff['files'][0]))
        self.value['stopped_v6']=self.save(m.HANDOFF_PATH,self.handoff)
        with self.assertRaises(m.Blocked):self.validate()
        self.refresh();path=self.v6/'chunks/c000000/a01/rpc.jsonl'
        path.write_bytes(path.read_bytes()+b'x'*m.CAPS['journal_record_bytes'])
        self.refresh()
        with self.assertRaises(m.Blocked):self.validate()

    def test_changed_native_expected_hash_blocks_carried_seal(self):
        self.keys[0]['block_hash']='0x'+'f'*64
        with self.assertRaises(m.Blocked):self.validate()

    def test_published_failed_usage_is_an_additional_floor(self):
        published=dict(self.rpc,response_bytes=self.rpc['response_bytes']+1,prior_rpc=self.base_rpc,
            new_retained_bytes=dict.fromkeys(('baseline','accepted','control','failed'),0))
        self.save(self.v6/'FAILED.json',{'schema_version':'polymarket_exact_header_campaign_v6','status':'blocked',
            'usage':published,'raw_clocks_only':True,'final_sample_admitted':False})
        self.refresh()
        with self.assertRaisesRegex(m.Blocked,'published_charge_refunded'):self.validate()

    def test_remaining_forecast_excludes_valid_seal_not_full_inventory(self):
        prior=self.validate();remaining=m.remaining_key_count(3,prior,self.inputs['keys']['path'])
        self.assertEqual(remaining,0)
        measured={'seconds_per_key':1000.}
        self.assertEqual(m.required_new_runtime(remaining,measured,prior),3600.)
        self.assertGreater(m.required_new_runtime(3,measured,prior),5000.)
        with patch.object(m.legacy,'key_chunk_at',return_value=self.keys[:-1]),self.assertRaises(m.Blocked):
            m.remaining_key_count(3,prior,self.inputs['keys']['path'])

    def test_campaign_carried_seal_uses_remaining_forecast_and_no_duplicate_key_rpc(self):
        admission={'schema_version':m.SCHEMA,'status':'root_exact_header_campaign_admitted','caps':m.CAPS,
            'consumer_head':self.source['head'],'run_dir':str(self.output),'raw_clocks_only':True,'final_sample_admitted':False,
            'inputs':{name:m.ref(value) for name,value in self.inputs.items()},'private_file':self.private_ref,
            'actual_keys':3,'conservative_new_runtime_seconds':3600.,'prior':self.value,'reboot_equivalence':self.certificate}
        path=self.ops/'v7_admission.json';binding=self.save(path,admission);self.requests=[]
        with patch.object(m,'source_snapshot',return_value=self.source),\
             patch.object(m,'validate_inputs',return_value=(self.inputs,{'seconds_per_key':1000.,'bytes_per_key':80.})),\
             patch.object(m,'validate_reboot',return_value=self.reboot),\
             patch.object(m,'reboot_environment',return_value=self.reboot['environment']),\
             patch.object(m.inventory,'load_rpc_endpoint',return_value=('private','private_file')),\
             patch.object(m,'key_chunks',side_effect=lambda _:iter([self.keys])),\
             patch.object(m,'owner_identity',return_value=self.process):
            result=m.run_campaign(path,binding['sha256'],self.source['head'],self.output,self.private,transport=self.transport)
        self.assertEqual(result['status'],m.SUCCESS,result)
        self.assertEqual(result['accepted_keys'],3);self.assertEqual(len(self.requests),2)
        self.assertTrue(all(isinstance(p,dict) and p['method']=='eth_chainId' for p in self.requests))
        self.assertTrue((self.output/'COMPLETE.json').is_file());self.assertFalse((self.output/'chunks').exists())


class SourceTests(unittest.TestCase):
    def test_no_old_global_assignments_and_guard_before_inputs(self):
        tree=ast.parse(inspect.getsource(m))
        for node in ast.walk(tree):
            if isinstance(node,(ast.Assign,ast.AugAssign)):
                targets=node.targets if isinstance(node,ast.Assign) else [node.target]
                self.assertFalse(any(isinstance(t,ast.Attribute) and isinstance(t.value,ast.Name) and t.value.id in {'previous','legacy','scale','pilot'} for t in targets))
        with patch.object(m,'require_production_host',side_effect=RuntimeError('guard')),patch.object(m,'run_campaign') as run:
            with self.assertRaisesRegex(RuntimeError,'guard'):
                m.main(['--admission','/none','--admission-sha256','0'*64,'--expected-head','f'*40,'--run-dir','/none','--rpc-private-file','/none'])
        run.assert_not_called()


if __name__=='__main__':unittest.main()
