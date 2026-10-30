"""Tiny local mechanics/admission faults; no real-data production bypass."""
from copy import deepcopy
from contextlib import ExitStack
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

from scripts import run_polymarket_native_source_profile_v2 as m


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
        boot='00000000-0000-4000-8000-000000000001'
        oldown={'pid':99,'birth_ticks':4,'group':99,'session':99,'uid':0,'full_exe':'/usr/bin/python3.12'}
        mounts={'data_uuid':m.preparation.DATA_UUID,'root_uuid':m.preparation.ROOT_UUID,
                'data_serial':m.preparation.DATA_SERIAL,'data_device_bytes':m.preparation.DATA_BYTES,
                'data_fstype':'ext4','data_st_dev':999,'root_st_dev':888}
        pre={'schema_version':'native_clock_readonly_host_inspection_v5',
             'status':'read_only_inspection_complete_not_stage_admission','boot_id':boot,
             'old_process_receipts_checked':603,'receipt_absence_relative_to_same_final_census':True,
             'checked_utc_at_last_census_start':now.isoformat(),'own_process':oldown,'mounts':mounts}
        pre_raw=m.encode(pre)
        exited={'schema_version':'polymarket_native_profile_preflight_exit_v1',
                'status':'root_verified_preflight_exited','exit_code':0,'boot_id':boot,
                'preflight_sha256':m.digest(pre_raw),'own_process':oldown}
        exit_raw=m.encode(exited);resource_raw=b'resource\n'
        modules={name:('# fixture '+name+'\n').encode() for name in m.RUNTIME_FILES}
        value={'schema_version':'polymarket_native_source_profile_admission_v3',
               'driver_schema_version':m.SCHEMA,
               'status':'root_admitted_native_footer_profile','run_dir':str(m.RUN),
               'live_checkout_head':m.HEAD,'operator_source_head':'a'*40,'boot_id':boot,
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
                 'old_fixed_future_slots':9,'new_external_control_bytes':41876,'new_external_control_slots':7,
                 'new_operator_external_control_bytes':0,'new_operator_external_control_slots':0,
                 'prior_failed_footer_runtime_seconds':2400,'prior_failed_footer_output_reservation_bytes':2*m.OUTPUT_CAP},
               'source_bundle':{name:{'bytes':len(raw),'sha256':m.digest(raw)} for name,raw in modules.items()}}
        test_raw=b'# synthetic fixture test source\n'
        value['fixture_test_sources']={name:{'bytes':len(test_raw),'sha256':m.digest(test_raw)}
                                       for name in m.TEST_FILES}
        self.context_for(value)
        return value,pre_raw,exit_raw,resource_raw,modules

    def admit(self,value,pre,exited,resource,modules):
        raw=m.encode(value)
        return m.admission(raw,m.digest(raw),pre,exited,resource,modules,m._CLOCK())

    def test_valid_actual_admission_contract(self):
        args=self.fixture_admission();value,pre=self.admit(*args)
        self.assertEqual(value['volume_identity']['data_st_dev'],999)
        self.assertEqual(pre['old_process_receipts_checked'],603)
        self._native_history_subtests()

    def context_for(self,value):
        records={};policy={}
        for role,(name,_,_) in m.NATIVE_HISTORY.items():
            if role=='old_admission':schema='polymarket_native_source_profile_admission_v1'
            elif role=='failed_v2_admission':schema='polymarket_native_source_profile_admission_v2'
            else:schema='polymarket_native_footer_profile_'+('qa' if role.endswith('qa') else 'external_closure')+('_v1' if role.startswith('old_') else '_v2')
            body={'schema_version':schema}
            if role in {'current_qa','current_closure'}:body['source_commit']=m.HISTORICAL_OPERATOR_HEAD
            if role=='failed_v2_admission':body['operator_source_head']=m.HISTORICAL_OPERATOR_HEAD
            raw=m.encode(body);policy[role]=(name,len(raw),m.digest(raw))
            records[role]={'path':str(m.OP/name),'raw_UTF8':raw.decode()}
        historical={role:ref(records[role]['path'],records[role]['raw_UTF8'].encode()) for role in records}
        tested={**value['source_bundle'],**value['fixture_test_sources']}
        log='Synthetic Linux fixture log; not remote evidence.\n'
        linux={'platform':'Linux','tests_run':m.QA_TESTS,'skipped':0,'failures':0,'errors':0,
            'command_modules':m.QA_MODULES,'owned_child_drained':True,
            'child_identity':{'available':True,'executable':'/usr/bin/python3.12','uid':0,'boot_id':value['boot_id'],
                'pid':10,'parent_pid':9,'process_group':10,'session':10,'start_ticks':42},
            'terminal_absence':{'available':True,'pid':10,'pid_absent':True,'process_group_absent':True,
                'session_absent':True,'process_stat_rows_checked':1},
            'log_bytes':len(log.encode()),'log_sha256':m.digest(log.encode())}
        qa={'schema_version':'polymarket_native_footer_profile_RUN_qa_v1',
            'status':'root_accepted_Linux_fixtures_and_owned_drain','source_commit':value['operator_source_head'],
            'parent':m.HEAD,'tested_source_files':tested,'Linux_QA':linux,'log_utf8':log}
        closure={'schema_version':'polymarket_native_footer_profile_RUN_closure_v1',
            'status':'root_frozen_RUN_internal_controls_no_external_slot_change',
            'source_commit':value['operator_source_head'],'parent':m.HEAD,
            'runtime_sources':value['source_bundle'],'fixture_test_sources':value['fixture_test_sources'],
            'historical_logical_references':historical,'historical_external_pool_bytes':2094547,
            'historical_external_pool_refs':95,'full_old_future_bytes':786432,'full_old_future_slots':9,
            'unchanged_pool_ceiling_bytes':2097152,'unchanged_pool_ref_ceiling':256,
            'new_external_control_bytes':0,'new_external_control_refs':0,
            'new_control_location':'START.json.operator_run_controls','run_artifact_allowance_bytes':m.OUTPUT_CAP}
        controls={'qa_utf8':m.encode(qa).decode(),'closure_utf8':m.encode(closure).decode()}
        value['run_artifact_controls']={role:{'bytes':len(raw.encode()),'sha256':m.digest(raw.encode())}
                                      for role,raw in controls.items()}
        raw=m.encode(value)
        context={'admission':raw,'admission_sha':m.digest(raw),'native_controls':records,
            'operator_controls':controls,
            'fixture_test_sources_utf8':{name:'# synthetic fixture test source\n' for name in m.TEST_FILES}}
        return context,policy

    def rebind(self,context,value):
        value['run_artifact_controls']={role:{'bytes':len(raw.encode()),'sha256':m.digest(raw.encode())}
                                      for role,raw in context['operator_controls'].items()}
        context['admission']=m.encode(value);context['admission_sha']=m.digest(context['admission'])

    def _native_history_subtests(self):
        value=self.fixture_admission()[0];context,policy=self.context_for(value)
        header={'external_pool_with_full_old_future_and_new4_bytes':2089535,
                'external_pool_with_full_old_future_and_new4_slots':92}
        with patch.object(m,'NATIVE_HISTORY',policy):
            result=m.native_history(context,value,header)
            self.assertEqual(result['combined_reserved_bytes'],2094547)
            self.assertEqual(result['combined_reserved_refs'],95)
            self.assertEqual(len(result['inline_logical_references']),6)
            self.assertEqual(result['new_operator_external_refs'],0)
            self.assertEqual(result['run_internal_control_capture']['admission_utf8'],context['admission'].decode())
            self.assertFalse(result['remote_filesystem_identities_claimed'])
            for role in policy:
                for fault in ('missing','path','body'):
                    damaged=deepcopy(context)
                    if fault=='missing':del damaged['native_controls'][role]
                    elif fault=='path':damaged['native_controls'][role]['path']=str(m.OP/'wrong.json')
                    else:damaged['native_controls'][role]['raw_UTF8']='{}'
                    with self.subTest(role=role,fault=fault),self.assertRaises(m.Blocked):
                        m.native_history(damaged,value,header)
            for role in ('qa_utf8','closure_utf8'):
                damaged=deepcopy(context);damaged['operator_controls'][role]+=' '
                with self.subTest(role=role),self.assertRaises(m.Blocked):m.native_history(damaged,value,header)
            for key,bad in (('external_pool_with_full_old_future_and_new4_bytes',2089534),
                            ('external_pool_with_full_old_future_and_new4_slots',91)):
                with self.subTest(key=key),self.assertRaises(m.Blocked):m.native_history(context,value,header|{key:bad})
        self.assertEqual(m.NATIVE_HISTORY['failed_v2_admission'][1:],
            (5018,'7e24d98ae8e113e947f4fe008607f307d17fcb3231dead341667e5226e673dc0'))

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
        self.assertLess(source.index('emit_process_receipt('),source.index('host_checks('))
        self.assertLess(source.index('history=native_history('),source.index('aggregate(originals'))
        self.assertIn("2*host['process_count']*6150",source)
        self._process_record_subtests()
        self._ancestry_order_subtests()
        self._early_process_subtests()

    def _process_fixture(self,*,uids=(1000,0,0,0),after_uids=None,owners=(0,0),
                         state='S',after_birth=44,executables=None,status=None,kernel=False):
        stack=ExitStack();self.addCleanup(stack.close)
        def proc_stat(birth):
            parts=['0']*30;parts[0]=state;parts[1]='11';parts[2]='22';parts[3]='33'
            parts[6]=str(0x00200000 if kernel else 0);parts[19]=str(birth)
            return b'123 (fixture) '+(' '.join(parts)).encode()
        statuses=[status if status is not None else b'Name:\tfixture\nUid:\t'+b'\t'.join(str(x).encode() for x in uids)+b'\n',
          b'Uid:\t'+b'\t'.join(str(x).encode() for x in (after_uids if after_uids is not None else uids))+b'\n']
        stats=[proc_stat(44),proc_stat(after_birth)];calls=[]
        def read(meter,path,cap=65536):
            meter.charge('proc_or_mount_proof',cap+1);calls.append((path,cap))
            if path.endswith('/status'):return statuses.pop(0)
            if path.endswith('/stat'):return stats.pop(0)
            if path.endswith('/cmdline'):return b''
            if path=='/proc/sys/kernel/random/boot_id':return b'boot\n'
            raise AssertionError(path)
        stack.enter_context(patch.object(m,'proc_bytes',side_effect=read))
        stack.enter_context(patch.object(m.os,'stat',side_effect=[type('S',(),{'st_uid':uid})() for uid in owners]))
        stack.enter_context(patch.object(m.os,'readlink',side_effect=executables or ['/usr/bin/python3.12']*2))
        return stack,calls

    def _process_record_subtests(self):
        with self.subTest(case='observed_sudo_real1000_effective0'):
            stack,calls=self._process_fixture();meter=self.meter()
            with stack:record=m.process_record(meter,123)
            self.assertEqual(record['uid'],0);self.assertEqual(record['uids'],(1000,0,0,0))
            self.assertEqual(sum(meter.reads.values()),1+6148)
            self.assertEqual(sum(path.endswith('/status') for path,_ in calls),2)
        for index in range(4):
            values=[1000,0,0,0];values[index]+=1
            with self.subTest(case='UID_quad_transition',index=index):
                stack,_=self._process_fixture(after_uids=tuple(values))
                with stack,self.assertRaisesRegex(m.Blocked,'^native_profile_process_UID_race$'):m.process_record(self.meter(),123)
        for owners in ((1000,1000),(0,1000)):
            with self.subTest(case='directory_owner',owners=owners):
                stack,_=self._process_fixture(owners=owners)
                with stack,self.assertRaisesRegex(m.Blocked,'^native_profile_process_UID_race$'):m.process_record(self.meter(),123)
        for raw in (b'Uid: 0 0 0\n',b'Uid: 0 0 0 0 0\n',b'Uid: -1 0 0 0\n',
                    b'Uid: 0 True 0 0\n',b'Uid: 0 0 0 0\nUid: 0 0 0 0\n'):
            with self.subTest(case='strict_UID_schema',raw=raw):
                stack,_=self._process_fixture(status=raw)
                with stack,self.assertRaisesRegex(m.Blocked,'^native_profile_proc_uid$'):m.process_record(self.meter(),123)
        for kwargs,reason in (({'state':'Z'},'native_profile_userspace_zombie'),
                              ({'after_birth':45},'native_profile_process_birth_race'),
                              ({'executables':['/usr/bin/python3.12','/usr/bin/other']},'native_profile_process_executable_race')):
            with self.subTest(case='preserved_precise_guard',reason=reason):
                stack,_=self._process_fixture(**kwargs)
                with stack,self.assertRaisesRegex(m.Blocked,'^'+reason+'$'):m.process_record(self.meter(),123)
        with self.subTest(case='kernel_UID_quad_and_max_read_charge'):
            stack,_=self._process_fixture(uids=(0,0,0,0),kernel=True);meter=self.meter()
            with stack:record=m.process_record(meter,123)
            self.assertIsNone(record['full_exe']);self.assertEqual(sum(meter.reads.values()),1+6150)

    def _ancestry_order_subtests(self):
        expected=[('/usr/bin/python3.12',0),('/usr/bin/sudo',0),('/usr/bin/timeout',1000),
          ('/usr/sbin/sshd',1000),('/usr/sbin/sshd',0),('/usr/sbin/sshd',0),('/usr/lib/systemd/systemd',0)]
        for wrong in (False,True):
            pairs=list(expected)
            if wrong:pairs[1:3]=[('/usr/bin/timeout',0),('/usr/bin/sudo',0)]
            pids=[700,701,702,703,704,705,1]
            records={pid:{'pid':pid,'parent_pid':pids[i+1] if i<6 else 0,'group':pid,'session':pid,
              'birth_ticks':10+i,'flags':0,'state':'S','full_exe':pairs[i][0],'uid':pairs[i][1]}
              for i,pid in enumerate(pids)}
            old=[{'pid':800+i,'birth_ticks':1,'group':900+i,'session':950+i} for i in range(7)]
            pre={'current_nonkernel_census':[],'bound_ancestry':old,'own_process':old[0]}
            with self.subTest(case='exact_launcher_order',wrong=wrong),ExitStack() as stack:
                stack.enter_context(patch.object(m.os,'scandir',return_value=[type('P',(),{'name':str(pid)})() for pid in pids]))
                stack.enter_context(patch.object(m.os,'getpid',return_value=700))
                stack.enter_context(patch.object(m,'process_record',side_effect=lambda meter,pid:records[pid]))
                if wrong:
                    with self.assertRaisesRegex(m.Blocked,'owned_ancestry_executable'):m.current_census(self.meter(),pre)
                else:self.assertEqual(len(m.current_census(self.meter(),pre)[1]),7)

    def _early_process_subtests(self):
        class Output:
            def __init__(self):self.buffer=self;self.raw=b'';self.used=0
            def write(self,raw):self.raw+=raw;self.used+=len(raw);return len(raw)
            def flush(self):pass
        with self.subTest(case='own_metered_early_receipt'):
            stack,calls=self._process_fixture(uids=(0,0,0,0));output=Output();meter=self.meter()
            with stack,patch.object(m.os,'getpid',return_value=123),patch.object(m.sys,'stdout',output):
                m.emit_process_receipt(meter,{'boot_id':'boot'},'a'*64)
            value=m.decode(output.raw)
            self.assertEqual(value['process']['pid'],123);self.assertEqual(value['process']['uids'],[0,0,0,0])
            self.assertEqual(value['boot_id'],'boot');self.assertLessEqual(len(output.raw),2048)
            self.assertEqual(value['read_usage']['metadata_bytes_charged'],1+6148+129)
            self.assertEqual(value['provider_requests'],value['footer_bytes_read'],value['trade_payload_bytes_read'])
        with self.subTest(case='host_failure_preserves_early_birth_without_input_IO'):
            stack,_=self._process_fixture(uids=(0,0,0,0));output=Output()
            context={'admission':b'{}','admission_sha':'b'*64,'preflight':b'{}','preflight_exit':b'{}',
              'historical_resource':b'{}','modules':{},'started':m._CLOCK(),'deadline':m._CLOCK()+60,'bootstrap_bytes':1}
            with stack,ExitStack() as patches:
                patches.enter_context(patch.object(m,'require_production_host'))
                patches.enter_context(patch.object(m,'_BOOTSTRAP',context))
                patches.enter_context(patch.object(m.os,'getpid',return_value=123))
                patches.enter_context(patch.object(m.sys,'stdout',output))
                patches.enter_context(patch.object(m,'admission',return_value=({'boot_id':'boot'},{})))
                patches.enter_context(patch.object(m,'host_checks',side_effect=m.Blocked('synthetic_host_failure')))
                git=patches.enter_context(patch.object(m,'git_state'))
                lineage=patches.enter_context(patch.object(m.roster,'lineage'))
                writer=patches.enter_context(patch.object(m,'Publisher'))
                with self.assertRaisesRegex(m.Blocked,'synthetic_host_failure'):m._execute(context)
                git.assert_not_called();lineage.assert_not_called();writer.assert_not_called()
            self.assertEqual(m.decode(output.raw)['status'],'producer_process_observed_before_host_checks')

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


    def test_new_QA_requires_exact_new_epoch_and_tested_source_namespace(self):
        for field,bad in (('source_commit',m.HISTORICAL_OPERATOR_HEAD),('parent','b'*40),
                          ('tested_source_files',{})):
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            qa=m.decode(context['operator_controls']['qa_utf8'].encode());qa[field]=bad
            context['operator_controls']['qa_utf8']=m.encode(qa).decode();self.rebind(context,value)
            with patch.object(m,'NATIVE_HISTORY',policy),self.subTest(field=field),self.assertRaises(m.Blocked):
                m.run_control_capture(context,value,{})
        value=self.fixture_admission()[0];value['operator_source_head']=m.HISTORICAL_OPERATOR_HEAD
        context,policy=self.context_for(value)
        with patch.object(m,'NATIVE_HISTORY',policy),self.assertRaisesRegex(m.Blocked,'new_source_epoch'):
            m.run_control_capture(context,value,{})

    def test_new_QA_counts_log_and_owned_drain_are_required_after_hash_rebinding(self):
        changes=[('tests_run',True),('tests_run',m.QA_TESTS-1),('skipped',1),('failures',1),('errors',1),
                 ('platform','Darwin'),('command_modules',[]),('owned_child_drained',False),
                 ('log_bytes',True),('log_sha256','a'*64)]
        for field,bad in changes:
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            qa=m.decode(context['operator_controls']['qa_utf8'].encode());qa['Linux_QA'][field]=bad
            context['operator_controls']['qa_utf8']=m.encode(qa).decode();self.rebind(context,value)
            with self.subTest(field=field,bad=bad),self.assertRaises(m.Blocked):
                m.run_control_capture(context,value,{})
        for field in ('pid_absent','process_group_absent','session_absent'):
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            qa=m.decode(context['operator_controls']['qa_utf8'].encode())
            qa['Linux_QA']['terminal_absence'][field]=False
            context['operator_controls']['qa_utf8']=m.encode(qa).decode();self.rebind(context,value)
            with self.subTest(field=field),self.assertRaises(m.Blocked):m.run_control_capture(context,value,{})
        for field,bad in (('uid',False),('boot_id','not-a-boot'),
                          ('boot_id','11111111-1111-4111-8111-111111111111'),
                          ('process_group',11),('session',11),('pid',True)):
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            qa=m.decode(context['operator_controls']['qa_utf8'].encode())
            qa['Linux_QA']['child_identity'][field]=bad
            context['operator_controls']['qa_utf8']=m.encode(qa).decode();self.rebind(context,value)
            with self.subTest(child_field=field,bad=bad),self.assertRaises(m.Blocked):
                m.run_control_capture(context,value,{})
        for bad in (None,True,11):
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            qa=m.decode(context['operator_controls']['qa_utf8'].encode())
            if bad is None:del qa['Linux_QA']['terminal_absence']['pid']
            else:qa['Linux_QA']['terminal_absence']['pid']=bad
            context['operator_controls']['qa_utf8']=m.encode(qa).decode();self.rebind(context,value)
            with self.subTest(absence_pid=bad),self.assertRaises(m.Blocked):m.run_control_capture(context,value,{})

    def test_new_closure_cannot_reassign_header_future_or_add_external_ref(self):
        for field,bad in (('new_external_control_bytes',1),('new_external_control_refs',1),
            ('full_old_future_bytes',0),('full_old_future_slots',8),('historical_external_pool_refs',94),
            ('historical_external_pool_bytes',2094546),('run_artifact_allowance_bytes',m.OUTPUT_CAP+1),
            ('new_control_location','OP/new_admission.json')):
            value=self.fixture_admission()[0];context,policy=self.context_for(value)
            closure=m.decode(context['operator_controls']['closure_utf8'].encode());closure[field]=bad
            context['operator_controls']['closure_utf8']=m.encode(closure).decode();self.rebind(context,value)
            historical={role:ref(record['path'],record['raw_UTF8'].encode()) for role,record in context['native_controls'].items()}
            with self.subTest(field=field),self.assertRaises(m.Blocked):m.run_control_capture(context,value,historical)

    def test_test_source_is_authenticated_and_added_to_committed_epoch_proof(self):
        value=self.fixture_admission()[0];context,policy=self.context_for(value)
        context['fixture_test_sources_utf8'][m.TEST_PATH]+='changed'
        with self.assertRaisesRegex(m.Blocked,'test_source_binding'):m.run_control_capture(context,value,{})
        import inspect
        source=inspect.getsource(m._execute)
        self.assertIn("for name,bound in sorted(value['fixture_test_sources'].items())",source)
        self.assertLess(source.index('module_epochs.extend('),source.index('prove_git_blobs(meter,module_epochs)'))

    def test_runtime_roster_rejects_frozen_V1_import_or_extra_test_module(self):
        for name in ('scripts/run_polymarket_native_source_profile_v1.py',m.TEST_PATH,'scripts/extra.py'):
            args=list(self.fixture_admission());args[4][name]=b'# extra\n'
            args[0]['source_bundle'][name]={'bytes':8,'sha256':m.digest(b'# extra\n')}
            with self.subTest(name=name),self.assertRaises(m.Blocked):self.admit(*args)
        self.assertEqual(len(m.RUNTIME_FILES),15)

    def test_all_six_historical_controls_are_fixed_original_bytes(self):
        self.assertEqual(len(m.NATIVE_HISTORY),6)
        for name in ('old_admission','old_qa','old_closure','current_qa','current_closure','failed_v2_admission'):
            self.assertIsNotNone(m.NATIVE_HISTORY[name][2])
        self.assertEqual(m.NATIVE_HISTORY['current_qa'][1],1727)
        self.assertEqual(m.NATIVE_HISTORY['current_closure'][1],3337)

    def test_START_embeds_exact_new_controls_with_no_new_filename_and_reopens(self):
        value=self.fixture_admission()[0];context,policy=self.context_for(value)
        header={'external_pool_with_full_old_future_and_new4_bytes':2089535,
                'external_pool_with_full_old_future_and_new4_slots':92}
        with patch.object(m,'NATIVE_HISTORY',policy):history=m.native_history(context,value,header)
        document=m.start_record(value,context,{'boot_id':value['boot_id']},{},{},{},history)
        self.assertNotIn('run_internal_control_capture',document['native_history_control_closure'])
        self.assertNotIn('historical_logical_control_capture',document['native_history_control_closure'])
        meter=self.meter();meter.reserve(2*m.OUTPUT_CAP);publisher=m.Publisher(self.root/'captured',meter)
        saved=publisher.publish('START.json',document)
        raw=(publisher.root/'START.json').read_bytes();body=m.decode(raw)
        self.assertEqual(body['operator_run_controls']['admission_utf8'].encode(),context['admission'])
        self.assertEqual(body['operator_run_controls']['qa_utf8'],context['operator_controls']['qa_utf8'])
        self.assertEqual(body['operator_run_controls']['closure_utf8'],context['operator_controls']['closure_utf8'])
        self.assertEqual(body['historical_logical_controls'],context['native_controls'])
        self.assertEqual(saved['sha256'],m.digest(raw))
        self.assertGreater(meter.reads['generated_output_reopen'],0)
        self.assertLessEqual(len(raw),m.FILE_OUTPUT_CAP)
        with self.assertRaises(m.Blocked):publisher.publish('admission.json',value)
        with self.assertRaises(m.Blocked):publisher.publish('START.json',{})

    def test_RUN_capture_cannot_fit_by_dropping_control_bytes_or_raising_artifact_cap(self):
        value=self.fixture_admission()[0];context,policy=self.context_for(value)
        for role,cap in m.RUN_CONTROL_CAPS.items():
            damaged=deepcopy(context);damaged['operator_controls'][role]='x'*(cap+1)
            with self.subTest(role=role),self.assertRaises(m.Blocked):m.run_control_capture(damaged,value,{})
        meter=self.meter();meter.reserve(2*m.OUTPUT_CAP);publisher=m.Publisher(self.root/'no_fit',meter)
        with self.assertRaises(m.Blocked):
            publisher.publish('START.json',{'operator_run_controls':{'raw':'x'*m.FILE_OUTPUT_CAP}})
        history={'run_internal_control_capture':{'raw':'x'*m.FILE_OUTPUT_CAP},
                 'historical_logical_control_capture':{}}
        with patch.object(m,'Publisher') as untouched,self.assertRaisesRegex(m.Blocked,'before_RUN_creation'):
            m.start_record(value,context,{'boot_id':value['boot_id']},{},{},{},history)
        untouched.assert_not_called()

    def test_unrelated_frozen_mechanisms_are_exact_copies_without_monkeypatch(self):
        import inspect
        # Literal function-byte pins from frozen driver bf6681f888b9165c... .
        # Do not import unbundled V1 code into the current QA namespace.
        pins={
            'Meter':'2493f76886b7e414bb7e559fa0a984f1492620a1bc9e1a1086dff5b3d2c9ab64',
            'Reader':'42f4c949aaa7b5a03ec4e0c827b715f1f292fbc49758f1f7e54de1faa5898f83',
            'Publisher':'0f69a3942ad5733c3f5a2d8f61998c78d67090323db826e3a0a7cf80b47a7d76',
            'identity':'6bf90b6899a0be52bfaacd2a8b4c7977e15068440850a6249ede86c01fdc669a',
            'literal_parents':'762d681ab39db95f9708b57cbd88d5045db0737ec21a72e713de558b54372732',
            'process_record':'5305837c4904e08072707071d0d4a0b15c0e031e124ce7e923ad49baed52660c',
            'current_census':'e9ee43ebf07a52d4d38286da377782f6d780082cb0452d406dbe365f763c3f0b',
            'host_checks':'e4de69344573a6612dd8e94da107d32cc6fe424c99f39c3bf400f2f7d9879010',
            'aggregate':'7ace9045e20627249282e3fbd14322f9b180411f37c5d0039731dc5174c64183',
            'source_epochs':'465740c66ce03c75096b42232e5abcff85c09a0a1f1912208a439484c75671f8',
            'prove_git_blobs':'da95a7d2ff2261f8d258fb57d4c27a95dc3def820f37b5f4a89a2caa131448df',
            'bind_header_closure':'600cb86ba94d595e3fae53da6e2ba07fa74d35d70ecd158d1b503f801357cef4',
            'final_primitive':'c304321a5c5d438cf62280c67a8302e494c4b62743fc40eb707e194d0c6041f3'}
        for name,digest in pins.items():
            with self.subTest(name=name):
                self.assertEqual(m.digest(inspect.getsource(getattr(m,name)).encode()),digest)

    def test_new_bootstrap_is_strict_V2_namespace_and_captures_new_controls(self):
        source=m.stdin_wrapper('a'*64,'b'*64,'c'*64)
        compile(source,'<new-authenticated-wrapper>','exec')
        self.assertIn('from scripts import run_polymarket_native_source_profile_v2 as driver',source)
        self.assertNotIn('run_polymarket_native_source_profile_v1',source)
        self.assertIn("'operator_controls','fixture_test_sources_utf8'",source)
        self.assertLess(source.index('native_profile_module_roster'),source.index('from scripts import'))
        self.assertLess(source.index('native_profile_bootstrap_test_binding'),source.index('from scripts import'))
        import inspect
        execute=inspect.getsource(m._execute)
        self.assertIn("'operator_run_controls':history['run_internal_control_capture']",inspect.getsource(m.start_record))
        self.assertLess(execute.index('start=start_record('),execute.index('publisher=Publisher('))
        self.assertIn("context['operator_controls']==operator_capture",execute)
        self.assertIn("context['fixture_test_sources_utf8']==test_capture",execute)
        args=list(self.fixture_admission());args[0]['additional_OP_reference']={'path':str(m.OP/'new.json')}
        with self.assertRaisesRegex(m.Blocked,'admission_fields'):self.admit(*args)


if __name__=='__main__':unittest.main()

