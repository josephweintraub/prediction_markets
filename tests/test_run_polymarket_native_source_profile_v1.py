"""Tiny local mechanics/admission faults; no real-data production bypass."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import os
from pathlib import Path
import signal
import stat
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts import run_polymarket_native_source_profile_v1 as m


def ref(path,raw):return {'path':str(path),'bytes':len(raw),'sha256':m.digest(raw)}


class DriverTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve()
        self.disk=patch.object(m.shutil,'disk_usage',return_value=type('D',(),{'free':10**12})())
        self.disk.start()
    def tearDown(self):self.disk.stop();self.tmp.cleanup()

    def meter(self):return m.Meter(m._CLOCK(),m._CLOCK()+100,1)

    def fixture_admission(self):
        now=datetime.now(timezone.utc)
        oldown={'pid':99,'birth_ticks':4,'group':99,'session':99,'uid':0,'full_exe':'/usr/bin/python3.12'}
        mounts={'data_uuid':m.preparation.DATA_UUID,'root_uuid':m.preparation.ROOT_UUID,
                'data_serial':m.preparation.DATA_SERIAL,'data_device_bytes':m.preparation.DATA_BYTES,
                'data_fstype':'ext4','data_st_dev':999,'root_st_dev':888}
        pre={'schema_version':'native_clock_readonly_host_inspection_v5',
             'status':'read_only_inspection_complete_not_stage_admission','boot_id':'boot',
             'old_process_receipts_checked':603,'receipt_absence_relative_to_same_final_census':True,
             'checked_utc_at_last_census_start':now.isoformat(),'own_process':oldown,'mounts':mounts}
        pre_raw=m.encode(pre)
        exited={'schema_version':'polymarket_native_profile_preflight_exit_v1',
                'status':'root_verified_preflight_exited','exit_code':0,'boot_id':'boot',
                'preflight_sha256':m.digest(pre_raw),'own_process':oldown}
        exit_raw=m.encode(exited);resource_raw=b'resource\n'
        modules={'production_guard.py':b'# guard\n','scripts/run_polymarket_native_source_profile_v1.py':b'# driver\n'}
        value={'schema_version':'polymarket_native_source_profile_admission_v1',
               'status':'root_admitted_native_footer_profile','run_dir':str(m.RUN),
               'live_checkout_head':m.HEAD,'operator_source_head':'a'*40,'boot_id':'boot',
               'issued_utc':now.isoformat(),'valid_until_utc':(now+timedelta(seconds=300)).isoformat(),
               'cumulative_runtime_seconds':194802,'runtime_ceiling_seconds':259200,
               'caps':{'read_bytes':m.READ_LIMIT,'footer_bytes':m.FOOTER_LIMIT,
                 'address_space_bytes':m.AS_LIMIT,'runtime_seconds':m.RUNTIME,'all_new_artifact_bytes':m.OUTPUT_CAP,
                 'minimum_free_bytes':m.FREE_FLOOR,'spill_bytes':0,'workers':1},
               'preflight':ref('/local/measurement.json',pre_raw),
               'preflight_exit':ref('/local/exit.json',exit_raw),
               'historical_resource':ref('/local/resource.json',resource_raw),
               'volume_identity':mounts,'native_accounting':{
                 'observed_retained_and_scratch_bytes':100,'observed_spill_bytes':0,
                 'git_metadata_and_private_index_bytes':16000000,
                 'retained_and_scratch_ceiling_bytes':64000000000,'spill_ceiling_bytes':16000000000,
                 'new_run_reservation_bytes':m.OUTPUT_CAP,'old_fixed_future_bytes':786432,
                 'old_fixed_future_slots':9,'new_external_control_bytes':36864,'new_external_control_slots':4},
               'source_bundle':{name:{'bytes':len(raw),'sha256':m.digest(raw)} for name,raw in modules.items()}}
        return value,pre_raw,exit_raw,resource_raw,modules

    def admit(self,value,pre,exited,resource,modules):
        raw=m.encode(value)
        return m.admission(raw,m.digest(raw),pre,exited,resource,modules,m._CLOCK())

    def test_valid_actual_admission_contract(self):
        args=self.fixture_admission();value,pre=self.admit(*args)
        self.assertEqual(value['volume_identity']['data_st_dev'],999)
        self.assertEqual(pre['old_process_receipts_checked'],603)

    def test_root_hash_not_boolean(self):
        value,pre,exited,resource,modules=self.fixture_admission()
        with self.assertRaises(m.Blocked):m.admission(m.encode(value),'0'*64,pre,exited,resource,modules,m._CLOCK())
        value['status']='accepted';value['accepted']=True
        with self.assertRaises(m.Blocked):self.admit(value,pre,exited,resource,modules)

    def test_admission_changes_fail_closed(self):
        for mutate in (lambda v:v['caps'].update(read_bytes=m.READ_LIMIT+1),
                       lambda v:v.update(live_checkout_head='b'*40),
                       lambda v:v.update(run_dir=str(m.OP/'new')),
                       lambda v:v.update(cumulative_runtime_seconds=259201),
                       lambda v:v['volume_identity'].update(data_uuid='wrong'),
                       lambda v:v['native_accounting'].update(old_fixed_future_slots=8),
                       lambda v:v['native_accounting'].update(observed_retained_and_scratch_bytes=64000000000),
                       lambda v:v.update(issued_utc=(datetime.now(timezone.utc)-timedelta(seconds=301)).isoformat())):
            with self.subTest(mutate=mutate):
                args=list(self.fixture_admission());mutate(args[0])
                with self.assertRaises(m.Blocked):self.admit(*args)

    def test_bundle_extra_or_modified_file(self):
        args=list(self.fixture_admission());args[4]['production_guard.py']=b'changed'
        with self.assertRaises(m.Blocked):self.admit(*args)
        args=list(self.fixture_admission());args[4]['../escape.py']=b'bad'
        with self.assertRaises(m.Blocked):self.admit(*args)

    def test_exit_or_preflight_mutation(self):
        args=list(self.fixture_admission());args[2]=args[2]+b' '
        with self.assertRaises(m.Blocked):self.admit(*args)

    def test_deadline_nonfinite_and_earlier(self):
        for value in (float('inf'),float('-inf'),float('nan'),True):
            with self.subTest(value=value):
                with self.assertRaises(m.Blocked):m.Meter(m._CLOCK(),value,1)
        with self.assertRaises(m.Blocked):m.Meter(m._CLOCK()-1201,m._CLOCK()+1,1)

    def test_reservation_prevents_overread_before_io(self):
        meter=self.meter();meter.reserve(m.READ_LIMIT-10)
        with self.assertRaises(m.Blocked):meter.charge('source_footer',10)
        meter.final_charge('control_reopen',5)
        self.assertEqual(meter.reads['control_reopen'],5)
        with self.assertRaises(m.Blocked):meter.final_charge('control_reopen',m.READ_LIMIT)

    def test_footer_subcap(self):
        meter=self.meter();meter.reads['source_footer']=m.FOOTER_LIMIT
        with self.assertRaises(m.Blocked):meter.charge('source_footer',1)

    def test_actual_reader_cache_final_and_mutation(self):
        raw=b'{"small":1}\n';path=self.root/'control.json';path.write_bytes(raw)
        meter=self.meter()
        with patch.object(m,'DATA',self.root):
            reader=m.Reader(meter,'');binding=ref(path,raw)
            self.assertEqual(reader.raw(binding),raw);spent=sum(meter.reads.values())
            self.assertEqual(reader.json(binding),{'small':1});self.assertEqual(sum(meter.reads.values()),spent)
            meter.reserve(len(raw)+1);reader.frozen_paths=set(reader.controls);reader.reopen()
            self.assertEqual(meter.reads['control_reopen'],len(raw)+1)
            path.write_bytes(b'{"small":2}\n')
            with self.assertRaises(m.Blocked):reader.raw(binding)

    def test_reader_no_raw_parquet_or_generic_large_json(self):
        raw=b'{}\n';meter=self.meter()
        for name in ('required_keys.parquet','payload.parquet'):
            path=self.root/name;path.write_bytes(raw)
            with patch.object(m,'DATA',self.root):
                with self.assertRaises(m.Blocked):m.Reader(meter,'').raw(ref(path,raw))
        path=self.root/'ordinary.json';path.write_bytes(raw)
        binding=ref(path,raw);binding['bytes']=9*1024**2
        with patch.object(m,'DATA',self.root):
            with self.assertRaises(m.Blocked):m.Reader(meter,'').raw(binding)

    def test_reader_alias_rejected(self):
        target=self.root/'target.json';target.write_bytes(b'{}\n')
        link=self.root/'alias.json';link.symlink_to(target)
        with patch.object(m,'DATA',self.root):
            with self.assertRaises(m.Blocked):m.Reader(self.meter(),'').raw(ref(link,b'{}\n'))

    def test_empty_sealed_operational_logs_are_read_and_reopened_without_refund(self):
        path=self.root/'empty.log';path.write_bytes(b'');meter=self.meter()
        with patch.object(m,'DATA',self.root):
            reader=m.Reader(meter,'');binding=ref(path,b'')
            reader.register_header_ref(binding);self.assertEqual(reader.raw(binding),b'')
            meter.reserve(1);reader.reopen()
            self.assertEqual(meter.reads['control_initial'],1)
            self.assertEqual(meter.reads['control_reopen'],1)
        with self.assertRaises(m.Blocked):m.control_reference({**binding,'sha256':'f'*64})
        with self.assertRaises(m.Blocked):m.control_reference({**binding,'path':str(self.root/'empty.json')})

    def test_header_registration_denies_canonical_configuration_and_authority(self):
        data=self.root/'data';repo=self.root/'repo';data.mkdir();repo.mkdir()
        with patch.object(m,'DATA',data),patch.object(m,'REPO',repo):
            reader=m.Reader(self.meter(),'')
            for path in (repo/'configuration.json',repo/'x.log',data/'required_keys.parquet',
                         self.root/'.polygon_rpc_url'):
                with self.subTest(path=path),self.assertRaises(m.Blocked):
                    reader.register_header_ref(ref(path,b'{}\n'))
            for path in (repo/'source.py',repo/'methods.md',data/'operational.py'):
                reader.register_header_ref(ref(path,b'{}\n'))

    def test_reader_frozen_closure_no_late_reference(self):
        path=self.root/'late.json';path.write_bytes(b'{}\n')
        with patch.object(m,'DATA',self.root):
            reader=m.Reader(self.meter(),'');reader.frozen_paths=set()
            with self.assertRaises(m.Blocked):reader.raw(ref(path,b'{}\n'))

    def test_original_projection_exact_rolling_codec_and_nondevice(self):
        controls=[{'path':str(self.root/'a.json'),'bytes':5,'sha256':'a'*64,
                   'identity':{'device':999,'inode':1,'mode':stat.S_IFREG|0o600,
                               'uid':0,'gid':0,'mtime_ns':9,'ctime_ns':10}}]
        old=deepcopy(controls);old[0]['identity']['device']=m.OLD_DEVICE
        sha=hashlib.sha256(m.encode(old[0])).hexdigest()
        with patch.multiple(m,CONTROL_COUNT=1,CONTROL_BYTES=5,CONTROL_SHA=sha):
            result=m.original_projection(controls,999)
            self.assertEqual(result['old_identity_projection_sha256'],sha)
            self.assertEqual(controls[0]['identity']['device'],999)
            controls[0]['identity']['ctime_ns']+=1
            with self.assertRaises(m.Blocked):m.original_projection(controls,999)

    def test_git_command_uses_no_shell_no_optional_locks(self):
        meter=self.meter()
        with patch.object(m,'command',return_value=b'ok') as command:
            self.assertEqual(m.git(meter,['cat-file','blob','a'*40+':production_guard.py'],5),b'ok')
            argv=command.call_args.args[1]
            self.assertIn('--no-optional-locks',argv);self.assertIn('core.fsmonitor=false',argv)
            self.assertIn('safe.directory='+str(m.REPO),argv)

    def test_source_epochs_maps_from_authenticated_records_only(self):
        qa={'schema_version':'polymarket_header_v8_linux_fixture_receipt_v1','source_head':m.QA_HEAD,
            'status':'failed_closed','log':m.QA_LOG,'tested_source_files':{'a.py':{'bytes':3,'sha256':'a'*64}}}
        class R:
            def json(self,ref):return qa if ref==m.QA_REF else {}
            def raw(self,ref):return b'log'
        old={'head':'b'*40,'files':{'b.py':'b'*64}}
        inv={'head':'b'*40,'files':{'b.py':{'bytes':7,'sha256':'b'*64}}}
        with patch.object(m,'QA_ROSTER_SHA',m.digest(m.encode(['a.py']))):
            # Exact43 is a production invariant, not silently weakened by map SHA.
            with self.assertRaises(m.Blocked):m.source_epochs(R(),{'source':old},{'source':inv},{'reboot_equivalence':m.QA_REF})

    def test_real_tiny_footer_aggregate_old_audit_current_identity_and_empty_file(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from tests.test_profile_polymarket_native_clock_sources_v1 import source
        currents=[source(self.root/'legacy.parquet','legacy_raw',3),
                  source(self.root/'native.parquet','native_raw',2),
                  source(self.root/'empty.parquet','native_raw',0)]
        originals=deepcopy(currents);rows=[]
        class Count:
            def charge(self,*args):pass
        for index,(original,current) in enumerate(zip(originals,currents)):
            original['identity']['device']=m.OLD_DEVICE
            _,plan=m.profile.inventory.read_footer(current,Count())
            rows.append({'source_index':index,'path':original['path'],'source_role':original['source_role'],
              'bytes':original['bytes'],'identity_json':m.encode(original['identity']).decode().strip(),
              'footer_sha256':original['footer_sha256'],'payload_sha256':original.get('payload_sha256'),
              'payload_sha256_verified':original['source_role']=='native_raw',
              'rows_declared':plan['rows_declared'],'rows_scanned':plan['rows_declared'],
              'valid_rows':plan['rows_declared'],'projected_compressed_bytes':plan['projected_compressed_bytes'],
              **{name:0 for name in m.proof.FAILURE_FIELDS}})
        audit=self.root/'audit.parquet'
        pq.write_table(pa.Table.from_pylist(rows,schema=m.profile.inventory.audit_schema()),audit)
        meter=self.meter();result=m.aggregate(originals,currents,audit.read_bytes(),meter)
        self.assertEqual(result['files'],3);self.assertEqual(result['rows'],5)
        self.assertEqual(result['roles']['native_raw']['zero_row_files'],1)
        self.assertFalse(result['trade_data_pages_read']);self.assertFalse(result['source_payload_rehashed'])
        self.assertEqual(result['roles']['legacy_raw']['rows'],3)
        projected=m.project_sources(originals,currents[0]['identity']['device'])
        self.assertEqual(projected,currents)
        changed=deepcopy(originals);changed[0]['identity']['ctime_ns']+=1
        with self.assertRaises(m.Blocked):m.project_sources(changed,currents[0]['identity']['device'])

    def test_tiny_actual_aggregate_phantom_audit_fails(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        row={field.name:None for field in m.profile.inventory.audit_schema()}
        output=pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist([row],schema=m.profile.inventory.audit_schema()),output)
        with self.assertRaisesRegex(m.Blocked,'extra_audit'):
            m.aggregate([],[],output.getvalue().to_pybytes(),self.meter())

    def test_valid_source_epoch_map_merges_exact_duplicate_not_physical_sources(self):
        files={f'scripts/f{i}.py':{'bytes':i+1,'sha256':format(i,'064x')} for i in range(43)}
        qa={'schema_version':'polymarket_header_v8_linux_fixture_receipt_v1','source_head':m.QA_HEAD,
            'status':'failed_closed','log':m.QA_LOG,'tested_source_files':files}
        class R:
            def json(self,ref):return qa if ref==m.QA_REF else {}
            def raw(self,ref):return b'log'
        old={'head':'b'*40,'files':{'guard.py':'b'*64}}
        inv={'head':'b'*40,'files':{'guard.py':{'bytes':7,'sha256':'b'*64}}}
        with patch.object(m,'QA_ROSTER_SHA',m.digest(m.encode(sorted(files)))):
            epochs=m.source_epochs(R(),{'source':old},{'source':inv},{'reboot_equivalence':m.QA_REF})
        self.assertEqual(len(epochs),44)
        self.assertEqual(next(e for e in epochs if e['path']=='guard.py')['bytes'],7)

    def test_operational_order_no_footer_before_full_read_fit(self):
        import inspect
        source=inspect.getsource(m._execute)
        self.assertLess(source.index('original_projection('),source.index('aggregate(originals'))
        self.assertLess(source.index('complete_full_closure_does_not_fit_before_footer'),source.index('aggregate(originals'))
        self.assertLess(source.index('prove_git_blobs(meter,epochs)'),source.index('aggregate(originals'))
        self.assertNotIn('run_fixture(',source);self.assertNotIn('source_snapshot(',source)

    def test_final_measured_receipt_prebuilt_before_primitive_not_postcommit(self):
        import inspect
        source=inspect.getsource(m._execute)
        receipt=source.index("context['final_producer_receipt']={")
        primitive=source.index('final_primitive(reader,currents,publisher,source_files,meter.deadline)')
        self.assertLess(source.index('current_census(meter,preflight,maximum_processes=host'),receipt)
        self.assertLess(receipt,primitive)
        self.assertIn('observed_usage_after_all_reopens_and_last_census',source)
        end=source[source.rindex("return context['prebuilt_terminal_stdout']"):]
        self.assertNotIn('usage(',end);self.assertNotIn('_CLOCK(',end);self.assertNotIn('.read(',end)
        self.assertLess(source.index("context['prebuilt_terminal_stdout']=encode("),primitive)
        wrapper=m.stdin_wrapper('a'*64,'b'*64,'c'*64)
        tail=wrapper[wrapper.index('sys.stdout.original.buffer.write(result)'):]
        self.assertNotIn('dumps(',tail);self.assertNotIn('.encode(',tail);self.assertNotIn('check(',tail)

    def header_fixture(self,*,duplicate=False,unknown=False,nested=False,missing=False,unclassified=False,
                       nested_schema='polymarket_header_v6_external_control_ledger_v1'):
        store={};bindings={}
        def add(name,value,raw=None):
            path=str(m.OP/name);raw=m.encode(value) if raw is None else raw
            store[path]=raw;binding=ref(path,raw);bindings[name]=binding;return binding
        certificate=add('certificate.json',{'schema_version':'polymarket_header_reboot_equivalence_v8'})
        prior_files=[certificate]
        if nested:
            leaf=add('nested_leaf.json',{'bound':'nested'})
            if not unclassified:prior_files.append(leaf)
        adm=add('full_header_v8_admission_v1.json',{'prior':{'rpc':{},'base':{'files':prior_files}},
                'reboot_equivalence':certificate})
        source={'head':m.HEAD,'files':{f'scripts/f{i}.py':{'bytes':1,'sha256':'a'*64} for i in range(43)}}
        checked={'schema_version':'polymarket_header_v8_preparation_check_v1',
           'status':'header_only_admission_prepared_not_launched','admission':adm,'source':source,
           'prior_rpc':{},'next_chunk':102,'next_attempt':2,'reused_keys':10200000}
        qa=add('full_header_v8_linux_fixtures_v1.json',{'schema_version':'old_failed_QA_preserved'})
        log=add('full_header_v8_linux_fixtures_v1.log',None,b'log')
        rows=[add('header_v7_unlaunched_terminal_v1.json',{}),
              add('full_header_v7_root_dispatch_v1.json',{}),qa,log]
        if nested:
            rows.append(add('nested_ledger.json',{'schema_version':nested_schema,
              'files':[leaf],'future_reserved_bytes':0,'future_reserved_files':9,'ceiling_bytes':2097152}))
            if missing:del store[leaf['path']]
        while len(rows)<78:rows.append(add(f'flat{len(rows)}.json',{}))
        if duplicate:rows[-1]=rows[0]
        ledger=add('external.json',{'schema_version':'unknown' if unknown else 'polymarket_header_v8_external_control_ledger_v1',
                'files':rows,'future_reserved_bytes':786432,'future_reserved_files':9,'ceiling_bytes':2097152})
        checked['external_ledger']=ledger;checkref=add('full_header_v8_root_admission_check_v1.json',checked)
        caller={'stage':'headers','admission':adm,'caller':{'pid':12},'final_sample_admitted':False}
        start=add('full_header_v8.START.json',caller);launch=add('full_header_v8.LAUNCH.json',caller)
        dispatch=add('full_header_v8_root_dispatch_v1.json',{'schema_version':'polymarket_header_v8_root_dispatch_v1',
           'status':'caller_and_producer_dispatched_owned_births_verified','source_head':m.HEAD,
           'admission':adm,'ownership_unknown':False,'caller':{'pid':12},'external_ledger':ledger,
           'preparation_check':checkref,'start':start,'launch':launch})
        stdout=add('full_header_v8.log',None,b'')
        direct={'admission':adm,'dispatch':dispatch,'complete':add('COMPLETE.json',{}),
                'manifest':add('manifest.json',{}),'caller_exit':add('full_header_v8.EXIT.json',{
                'producer_exit_code':0,'stdout_bytes_observed':0,'stdout_prefix_bytes_retained':0,
                'stdout_sha256':stdout['sha256'],'stdout_prefix_sha256':stdout['sha256'],
                'stdout_truncated':False,'stdout_eof_observed':True})}
        terminal=add('full_header_v8_terminal_acceptance_v1.json',{})
        class R:
            def __init__(self):self.selected={};self.reads=[]
            def register_header_ref(self,ref):
                prior=self.selected.setdefault(ref['path'],ref)
                m.require(prior==ref,'conflict')
            def raw(self,ref):
                m.require(ref['path'] in store,'missing_nested_control')
                raw=store[ref['path']];m.control_reference(ref)
                m.require(len(raw)==ref['bytes'] and m.digest(raw)==ref['sha256'],'fixture_control_changed')
                self.reads.append(ref['path']);return raw
            def json(self,ref):return m.decode(self.raw(ref))
        return R(),store[adm['path']],direct,qa,log,store,terminal

    def test_header_closure_reopens_79_flat_and_extra_nested_control(self):
        reader,adm,direct,qa,log,store,terminal=self.header_fixture(nested=True)
        with patch.object(m.proof,'header_terminal_adapter',return_value={'direct_bindings':direct}), \
             patch.object(m,'QA_REF',qa),patch.object(m,'QA_LOG',log),patch.object(m.proof,'HEADER_TERMINAL',terminal):
            result=m.bind_header_closure(reader,b'terminal',adm)
        self.assertEqual(result['external_flat_refs'],79)
        self.assertIn(str(m.OP/'nested_leaf.json'),reader.reads)
        self.assertIn(str(m.OP/'full_header_v8_root_admission_check_v1.json'),reader.reads)
        self.assertFalse(result['header_rows_or_journals_reexecuted'])
        self.assertFalse(result['private_authority_body_read'])
        self.assertEqual(result['external_pool_with_full_old_future_and_new4_slots'],92)
        self.assertEqual(result['external_pool_with_full_old_future_and_new4_bytes'],
            sum(ref['bytes'] for ref in m.decode(store[str(m.OP/'external.json')])['files'])
            +len(store[str(m.OP/'external.json')])+786432+36864)

    def test_header_closure_duplicate_unknown_or_missing_nested_ref_blocks(self):
        for changes in ({'duplicate':True},{'unknown':True},{'nested':True,'missing':True},
                        {'nested':True,'unclassified':True},
                        {'nested':True,'nested_schema':'polymarket_header_v6_external_control_ledger_v2'},
                        {'nested':True,'nested_schema':'polymarket_header_v9_external_control_ledger_v1'}):
            with self.subTest(changes=changes):
                reader,adm,direct,qa,log,store,terminal=self.header_fixture(**changes)
                with patch.object(m.proof,'header_terminal_adapter',return_value={'direct_bindings':direct}), \
                     patch.object(m,'QA_REF',qa),patch.object(m,'QA_LOG',log), \
                     patch.object(m.proof,'HEADER_TERMINAL',terminal),self.assertRaises(m.Blocked):
                    m.bind_header_closure(reader,b'terminal',adm)

    def test_header_closure_mutated_nested_bytes_block(self):
        reader,adm,direct,qa,log,store,terminal=self.header_fixture(nested=True)
        store[str(m.OP/'nested_leaf.json')]=b'changed'
        with patch.object(m.proof,'header_terminal_adapter',return_value={'direct_bindings':direct}), \
             patch.object(m,'QA_REF',qa),patch.object(m,'QA_LOG',log), \
             patch.object(m.proof,'HEADER_TERMINAL',terminal),self.assertRaises(m.Blocked):
            m.bind_header_closure(reader,b'terminal',adm)

    def test_wrapper_timer_before_stdin_and_project_imports(self):
        source=m.stdin_wrapper('a'*64,'b'*64,'c'*64)
        compile(source,'<authenticated-wrapper>','exec')
        self.assertLess(source.index('signal.setitimer'),source.index('sys.stdin.buffer.read'))
        self.assertLess(source.index('resource.setrlimit'),source.index('from scripts import'))
        self.assertIn('native_profile_unbundled_project_import',source)
        self.assertIn('sys.dont_write_bytecode=True',source)
        self.assertIn('len(raw)<=4*1024**2',source)

    def test_normal_production_guard_precedes_bootstrap_failure(self):
        with patch.object(m,'require_production_host',side_effect=RuntimeError('guard')):
            with self.assertRaisesRegex(RuntimeError,'guard'):m.run_production()
        with patch.object(m,'require_production_host'),patch.object(m,'_BOOTSTRAP',None):
            with self.assertRaisesRegex(m.Blocked,'before_IO'):m.run_production()

    def test_publisher_real_tiny_complete_marker_last(self):
        meter=self.meter();meter.reserve(2*m.OUTPUT_CAP)
        out=self.root/'run'
        publisher=m.Publisher(out,meter)
        publisher.publish('START.json',{'start':1});publisher.publish('profile.json',{'rows':2})
        calls=[]
        result=publisher.publish('COMPLETE.json',{'done':True},
                final_check=lambda:calls.append('bytes'),final_primitive=lambda:calls.append('primitive'))
        self.assertEqual(calls,['bytes','primitive']);self.assertTrue((out/'COMPLETE.json').is_file())
        self.assertEqual(result['sha256'],m.digest((out/'COMPLETE.json').read_bytes()))

    def test_publisher_oversized_output_blocks_not_drop(self):
        meter=self.meter();meter.reserve(2*m.OUTPUT_CAP);publisher=m.Publisher(self.root/'out',meter)
        with self.assertRaises(m.Blocked):publisher.publish('profile.json',{'huge':'x'*m.OUTPUT_CAP})
        self.assertEqual(list(publisher.root.iterdir()),[])

    def test_final_primitive_rejects_callback_late_parent_alias(self):
        control=self.root/'inputs';control.mkdir();path=control/'a.json';path.write_bytes(b'{}\n')
        saved=m.identity(path)
        class R:controls={str(path):{'path':str(path),'bytes':saved['bytes'],
                       'identity':{k:saved[k] for k in m.roster.STAT_FIELDS}}}
        class P:root=self.root/'out';files={}
        P.root.mkdir()
        moved=self.root/'old';control.rename(moved);control.symlink_to(moved,target_is_directory=True)
        with self.assertRaisesRegex(m.Blocked,'parent_alias'):
            m.final_primitive(R(),[],P(),{},m._CLOCK()+100)

    def test_final_primitive_checks_missing_git_file_appears(self):
        path=self.root/'HEAD';path.write_bytes(b'ref: changed')
        class R:controls={}
        class P:root=self.root/'out';files={}
        P.root.mkdir()
        with self.assertRaisesRegex(m.Blocked,'appeared'):
            m.final_primitive(R(),[],P(),{str(path):None},m._CLOCK()+100)


if __name__=='__main__':unittest.main()
