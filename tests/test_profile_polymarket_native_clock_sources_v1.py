"""Executable tiny footer/profile fixtures; no real trade files or infrastructure."""
from copy import deepcopy
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import profile_polymarket_native_clock_sources_v1 as m

FREE = shutil._ntuple_diskusage(10**12, 0, 10**12)


def reference(path):
    raw = path.read_bytes()
    return {'path': str(path), 'bytes': len(raw), 'sha256': m.digest(raw)}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(m.encode(value))
    return reference(path)


def legacy_schema():
    integer = {'maker_amount_filled','taker_amount_filled','fee','block_number','log_index'}
    return pa.schema([(name, pa.int64() if name in integer else pa.string()) for name in sorted(m.core.LEGACY_FIELDS)])


def native_schema():
    integer = {'block_number','log_index','transaction_index','source_request_id'}
    boolean = {'removed','provider_block_timestamp_present'}
    return pa.schema([(name, pa.uint64() if name in integer else pa.bool_() if name in boolean
        else pa.list_(pa.string()) if name == 'topics' else pa.string()) for name in sorted(m.core.wire.ROW_FIELDS)])


def source(path, role, rows, row_group_size=2, schema=None):
    schema = schema or (legacy_schema() if role == 'legacy_raw' else native_schema())
    data = []
    for index in range(rows):
        row = {}
        for field in schema:
            row[field.name] = index+1 if pa.types.is_integer(field.type) else False if pa.types.is_boolean(field.type) else ['0x'+'1'*64] if pa.types.is_list(field.type) else 'opaque-field-'+str(index)
        if 'blockHash' in row: row['blockHash'] = '0x'+format(index+1, '064x')
        data.append(row)
    pq.write_table(pa.Table.from_pylist(data, schema=schema), path, row_group_size=row_group_size, compression='zstd')
    raw = path.read_bytes(); length = int.from_bytes(raw[-8:-4], 'little')
    entry = {'path': str(path), 'source_role': role, 'bytes': len(raw), 'identity': m.roster.identity(path),
        'footer_sha256': m.digest(raw[-8-length:])}
    if role == 'native_raw': entry['payload_sha256'] = m.digest(raw)
    return entry


class SourceProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.output = self.root/'out'
        self.entries = [source(self.root/'legacy.parquet','legacy_raw',3),
            source(self.root/'native.parquet','native_raw',2,1), source(self.root/'empty.parquet','native_raw',0)]
        self.refs = self.inputs()
        disk = patch.object(m.shutil, 'disk_usage', return_value=FREE); disk.start(); self.addCleanup(disk.stop)

    def inputs(self):
        rows, footers = [], 0
        class Meter:
            def charge(self, *args): pass
        for index, entry in enumerate(self.entries):
            _, plan = m.inventory.read_footer(entry, Meter())
            footers += plan['footer_read_bytes_per_pass']
            rows.append({'source_index': index, 'path': entry['path'], 'source_role': entry['source_role'],
                'bytes': entry['bytes'], 'identity_json': m.encode(entry['identity']).decode().strip(),
                'footer_sha256': entry['footer_sha256'], 'payload_sha256': entry.get('payload_sha256'),
                'payload_sha256_verified': entry['source_role'] == 'native_raw',
                'rows_declared': plan['rows_declared'], 'rows_scanned': plan['rows_declared'], 'valid_rows': plan['rows_declared'],
                **{name: 0 for name in m.proof.FAILURE_FIELDS}, 'projected_compressed_bytes': plan['projected_compressed_bytes']})
        roster = write(self.root/'roster.json', {'schema_version':'polymarket_exact_header_key_roster_v1','files':self.entries})
        audit_path = self.root/'source_audit.parquet'
        pq.write_table(pa.Table.from_pylist(rows, schema=m.inventory.audit_schema()), audit_path, compression='zstd')
        audit = reference(audit_path)
        manifest = {'schema_version':'polymarket_exact_header_key_inventory_v1','status':'literal_header_key_inventory_complete',
            'literal_roster_only':True,'final_universe_admitted':False,'required_keys':m.proof.KEYS_REQUIRED,
            'roster':{'path':roster['path'],'sha256':roster['sha256'],
                'identity':{**m.roster.identity(self.root/'roster.json'),'bytes':roster['bytes']}},
            'source_audit':{**audit,'path':'source_audit.parquet'},'source_files':len(self.entries),
            'required_keys_parquet':{'path':'required_keys.parquet','bytes':123,'sha256':'b'*64},
            'rows_declared':sum(row['rows_declared'] for row in rows),'rows_scanned':sum(row['rows_scanned'] for row in rows),
            'failure_counts':{name:0 for name in m.proof.FAILURE_FIELDS},'global_conflicting_hash_blocks':0,
            'source_inputs_reopened':True,'roster_reopened':True,'source_reopened':True,
            'source':{'head':'c'*40,'tracked_worktree_clean':True,
                'files':{name:{'bytes':100,'sha256':'d'*64} for name in m.inventory.SOURCE_FILES}},
            'planned_reads':{'footer_read_bytes':2*footers,'projected_compressed_read_bytes':100,'native_payload_rehash_bytes':200}}
        manifest_ref = write(self.root/'manifest.json',manifest)
        complete = write(self.root/'COMPLETE.json',{'schema_version':'polymarket_exact_header_key_inventory_completion_v1',
            'status':manifest['status'],'manifest':{**manifest_ref,'path':'manifest.json'},
            'literal_roster_only':True,'final_universe_admitted':False})
        return {'roster':roster,'source_audit':audit,'inventory_manifest':manifest_ref,'inventory_complete':complete}

    def run_profile(self):
        return m.run_fixture(self.refs,self.output,started=time.monotonic())

    def test_executable_profile_conserves_files_rows_bytes_and_zero_row_sources(self):
        result = self.run_profile(); profile = result['profile']
        self.assertEqual(profile['files'],3); self.assertEqual(profile['rows'],5)
        self.assertEqual(profile['roles']['legacy_raw']['rows'],3)
        self.assertEqual(profile['roles']['native_raw']['zero_row_files'],1)
        self.assertEqual(sum(role['source_bytes'] for role in profile['roles'].values()),sum(e['bytes'] for e in self.entries))
        self.assertEqual(result['observed_usage_after_final_reopens']['raw_trade_body_bytes_read'],0)
        self.assertEqual(result['observed_usage_after_final_reopens']['source_payload_full_hash_bytes_read'],0)
        self.assertFalse(result['production_admitted'])
        self.assertFalse(result['trade_reconciliation_or_analysis_ready_admitted'])
        self.assertEqual(sorted(p.name for p in self.output.iterdir()),['COMPLETE.json','START.json','profile.json'])
        completion=m.decode((self.output/'COMPLETE.json').read_bytes())
        self.assertEqual(completion['all_metadata_read_bytes_charged_after_final_reopens'],
            result['observed_usage_after_final_reopens']['metadata_bytes_charged'])
        self.assertEqual(completion['all_reads_charged_after_marker_staging_and_final_reopens'],
            result['observed_usage_after_final_reopens']['bytes_by_kind'])

    def test_full_literal_schema_and_every_nested_identity_leaf_are_profiled(self):
        result = self.run_profile()['profile']
        schemas = list(result['literal_schema_variants'].values())
        native = next(item['schema'] for item in schemas if item['source_role']=='native_raw')
        self.assertEqual({field['name'] for field in native['fields']},m.core.wire.ROW_FIELDS)
        columns = result['roles']['native_raw']['leaf_columns']
        self.assertIn('topics.list.element',columns)
        self.assertTrue({'transactionHash','logIndex','address','exchange_address'} <= set(columns))
        self.assertEqual(result['roles']['legacy_raw']['max_row_group_rows'],2)
        self.assertGreater(columns['transactionHash']['uncompressed_bytes'],0)
        self.assertEqual(columns['removed']['zero_byte_chunk_count'],1)
        self.assertEqual(sum(v['compressed_bytes'] for v in columns.values()),result['roles']['native_raw']['all_field_compressed_bytes'])

    def test_exact_per_file_extent_hash_is_independently_reproducible_in_source_order(self):
        actual = self.run_profile()['profile']['ordered_complete_file_profile_projection_sha256']
        rolling = hashlib.sha256(); meter = m.Meter(time.monotonic())
        for index,entry in enumerate(self.entries):
            value = m.profile_footer(entry,meter)
            rolling.update(m.encode({'source_index':index,'source_binding':entry,'profile':value}))
        self.assertEqual(actual,rolling.hexdigest())
        wrong = hashlib.sha256()
        for index,entry in enumerate(reversed(self.entries)):
            wrong.update(m.encode({'source_index':index,'source_binding':entry,'profile':m.profile_footer(entry,m.Meter(time.monotonic()))}))
        self.assertNotEqual(actual,wrong.hexdigest())

    def test_raw_files_are_read_only_at_magic_trailer_and_bound_footer_extents(self):
        original_open = Path.open; reads = []
        allowed = {}
        for entry in self.entries:
            path=Path(entry['path']); raw=path.read_bytes(); length=int.from_bytes(raw[-8:-4],'little')
            allowed[path] = [(0,4),(len(raw)-8-length,len(raw))]
        class Spy:
            def __init__(self,path,stream): self.path,self.stream=path,stream
            def __enter__(self): return self
            def __exit__(self,*args): self.stream.close()
            def seek(self,*args): return self.stream.seek(*args)
            def read(self,size=-1):
                first=self.stream.tell(); last=first+size
                assert size>=0 and any(a<=first<=last<=b for a,b in allowed[self.path]),'RAW DATA PAGE READ'
                reads.append((self.path,first,size)); return self.stream.read(size)
        def opened(path,*args,**kwargs):
            stream=original_open(path,*args,**kwargs)
            return Spy(path,stream) if path in allowed else stream
        with patch.object(Path,'open',new=opened): result=self.run_profile()
        self.assertEqual(sum(size for _,_,size in reads),result['observed_usage_after_final_reopens']['bytes_by_kind']['source_footer'])
        self.assertEqual(len(reads),3*len(self.entries))

    def test_hash_cost_qualification_does_not_claim_legacy_or_native_body_rehash(self):
        profile = self.run_profile()['profile']
        self.assertEqual(profile['full_payload_hash_read_cost_bytes']['legacy_raw'],self.entries[0]['bytes'])
        self.assertEqual(profile['full_payload_hash_read_cost_bytes']['native_raw'],sum(e['bytes'] for e in self.entries[1:]))
        self.assertIn('NOT_full_payload_SHA',profile['legacy_hash_qualification'])
        self.assertIn('NOT_rehashed',profile['native_hash_qualification'])
        self.assertFalse(profile['actual_trade_stream_runtime_RAM_index_compression_or_output_fit_proved'])

    def test_saved_audit_row_mismatch_and_missing_or_extra_source_fail(self):
        rows=list(m.audit_rows(Path(self.refs['source_audit']['path']).read_bytes()))
        for change in ('rows','index','missing','extra','hashflag'):
            bad=deepcopy(rows)
            if change=='rows': bad[0]['rows_declared']+=1
            if change=='index': bad[0]['source_index']=1
            if change=='missing': bad.pop()
            if change=='extra': bad.append(deepcopy(bad[-1]))
            if change=='hashflag': bad[1]['payload_sha256_verified']=False
            with self.subTest(change=change),self.assertRaises(m.Blocked):
                m.aggregate(self.entries,bad,m.Meter(time.monotonic()))

    def test_footer_SHA_and_literal_column_schema_fail_closed(self):
        entry=deepcopy(self.entries[0]); entry['footer_sha256']='0'*64
        with self.assertRaisesRegex(m.Blocked,'profile_source_footer_SHA'):m.profile_footer(entry,m.Meter(time.monotonic()))
        path=self.root/'bad.parquet'; entry=source(path,'legacy_raw',1,schema=pa.schema([('block_number',pa.int64())]))
        with self.assertRaisesRegex(m.Blocked,'profile_literal_schema_fields'):m.profile_footer(entry,m.Meter(time.monotonic()))

    def test_complete_per_file_extent_map_keeps_row_groups_leaf_identity_and_empty_chunks(self):
        value=m.profile_footer(self.entries[0],m.Meter(time.monotonic()))
        self.assertEqual(value['row_group_physical_rows'],[2,1])
        self.assertEqual(len(value['row_group_leaf_extents']),2*len(m.core.LEGACY_FIELDS))
        self.assertEqual(value['row_group_leaf_extents'][0][0],0)
        self.assertEqual(value['row_group_leaf_extents'][-1][0],1)
        empty=m.profile_footer(self.entries[-1],m.Meter(time.monotonic()))
        boolean=next(row for row in empty['row_group_leaf_extents'] if row[1]=='removed')
        self.assertEqual(boolean[2:],[None,None,0,0,0])

    def test_external_overlap_outside_and_invalid_extent_numbers_are_rejected(self):
        entry=self.entries[0]; raw=Path(entry['path']).read_bytes();length=int.from_bytes(raw[-8:-4],'little')
        real=pq.read_metadata(pa.BufferReader(b'PAR1'+raw[-8-length:]))
        first=min(value for value in (real.row_group(0).column(0).dictionary_page_offset,
            real.row_group(0).column(0).data_page_offset) if value is not None and value>=4)
        for mode in ('external','overlap','outside','negative_compressed','boolean_uncompressed','negative_rows'):
            class Column:
                def __init__(self,column,index):self.column,self.index=column,index
                def __getattr__(self,name):
                    if mode=='external' and name=='file_path':return '/unbound/other.parquet'
                    if mode=='overlap' and self.index==1 and name in ('dictionary_page_offset','data_page_offset'):return first
                    if mode=='outside' and name in ('dictionary_page_offset','data_page_offset'):return len(raw)
                    if mode=='negative_compressed' and name=='total_compressed_size':return -1
                    if mode=='boolean_uncompressed' and name=='total_uncompressed_size':return False
                    return getattr(self.column,name)
            class Group:
                def __init__(self,group):self.group=group
                def __getattr__(self,name):
                    if mode=='negative_rows' and name=='num_rows':return -1
                    return getattr(self.group,name)
                def column(self,index):return Column(self.group.column(index),index)
            class Metadata:
                def __getattr__(self,name):return getattr(real,name)
                def row_group(self,index):return Group(real.row_group(index))
            with self.subTest(mode=mode),patch.object(pq,'read_metadata',return_value=Metadata()),self.assertRaises(m.Blocked):
                m.profile_footer(entry,m.Meter(time.monotonic()))

    def test_same_bytes_source_replacement_is_not_adopted(self):
        path=Path(self.entries[0]['path']); raw=path.read_bytes(); path.unlink(); path.write_bytes(raw)
        with self.assertRaisesRegex(m.Blocked,'literal_source_identity_or_size_changed'):self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())

    def test_control_SHA_change_is_charged_and_rejected_before_publication(self):
        path=Path(self.refs['inventory_manifest']['path']); path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaisesRegex(m.Blocked,'profile_control_size_changed'):self.run_profile()
        self.assertFalse(self.output.exists())
        refs=deepcopy(self.refs); refs['inventory_manifest']['bytes']=path.stat().st_size
        meter=m.Meter(time.monotonic()); reader=m.BoundReader(refs,meter)
        with self.assertRaisesRegex(m.Blocked,'profile_control_SHA_or_identity_changed'):reader.raw('inventory_manifest')
        self.assertEqual(meter.usage()['metadata_bytes_charged'],refs['inventory_manifest']['bytes'])

    def test_unknown_fixture_input_namespace_and_unaccepted_output_never_complete(self):
        (self.root/'unknown.pending').write_bytes(b'preserved')
        with self.assertRaisesRegex(m.Blocked,'profile_fixture_input_namespace_changed'):self.run_profile()
        self.assertFalse(self.output.exists())

    def test_unknown_empty_input_directory_is_not_adopted(self):
        (self.root/'unknown-empty-directory').mkdir()
        with self.assertRaisesRegex(m.Blocked,'profile_fixture_input_unknown_directory_or_special_file'):self.run_profile()
        self.assertFalse(self.output.exists())

    def test_late_input_change_and_output_replacement_withhold_COMPLETE(self):
        original=m.BoundReader.reopen
        def late(reader):
            original(reader)
            path=self.output/'profile.json'; raw=path.read_bytes(); path.unlink(); path.write_bytes(raw)
        with patch.object(m.BoundReader,'reopen',new=late),self.assertRaisesRegex(m.Blocked,'profile_output_reopen_changed'):
            self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())
        self.assertTrue((self.output/'.COMPLETE.json.pending').exists())

    def test_late_control_replacement_is_caught_by_final_full_reopen(self):
        original=m.BoundReader.reopen
        def late(reader):
            path=Path(reader.refs['roster']['path']);raw=path.read_bytes();path.unlink();path.write_bytes(raw)
            original(reader)
        with patch.object(m.BoundReader,'reopen',new=late),self.assertRaises(m.Blocked):self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())

    def test_source_body_mutation_after_final_control_read_withholds_COMPLETE(self):
        original=m.BoundReader.reopen
        def late(reader):
            original(reader)
            path=Path(self.entries[0]['path']);raw=path.read_bytes()
            path.write_bytes(raw[:4]+bytes([raw[4]^1])+raw[5:])
        with patch.object(m.BoundReader,'reopen',new=late),self.assertRaisesRegex(m.Blocked,'profile_primitive_bound_identity_changed'):
            self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())

    def test_output_disk_callback_after_inputs_cannot_mutate_source_control_or_namespace(self):
        for mode in ('source','control','namespace'):
            case=SourceProfileTests('test_executable_profile_conserves_files_rows_bytes_and_zero_row_sources')
            case.setUp()
            try:
                phase={'after_inputs':False,'mutated':False};original=m.BoundReader.reopen
                disk=m.shutil.disk_usage
                def finish(reader):original(reader);phase['after_inputs']=True
                def resource(path):
                    if phase['after_inputs'] and not phase['mutated']:
                        phase['mutated']=True
                        if mode=='source':
                            target=Path(case.entries[0]['path']);raw=target.read_bytes()
                            target.write_bytes(raw[:4]+bytes([raw[4]^1])+raw[5:])
                        if mode=='control':
                            target=Path(case.refs['inventory_manifest']['path']);raw=target.read_bytes()
                            target.write_bytes(raw[:-1]+b' ')
                        if mode=='namespace':(case.root/'late-unbound').write_bytes(b'preserved')
                    return disk(path)
                with self.subTest(mode=mode),patch.object(m.BoundReader,'reopen',new=finish),patch.object(m.shutil,'disk_usage',side_effect=resource),self.assertRaises(m.Blocked):
                    case.run_profile()
                self.assertTrue(phase['mutated'])
                self.assertFalse((case.output/'COMPLETE.json').exists())
                self.assertTrue((case.output/'.COMPLETE.json.pending').exists())
            finally:case.doCleanups()

    def test_no_injected_meter_clock_or_resource_callback_after_final_primitive_gate(self):
        phase={'final':False};primitive=m.primitive_gate
        def final(*args):primitive(*args);phase['final']=True
        def clock():
            if phase['final']:raise AssertionError('mutable clock called after final primitive gate')
            return time.monotonic()
        with patch.object(m,'primitive_gate',side_effect=final):
            result=m.run_fixture(self.refs,self.output,started=time.monotonic(),clock=clock)
        self.assertTrue(phase['final'])
        self.assertEqual(result['status'],'footer_profile_complete_fixture_only_not_reconciliation_admission')

    def test_marker_commit_is_last_no_postcommit_IO_checks_or_directory_fsync(self):
        phase={'committed':False};atomic=m.roster.atomic_commit;opened=Path.open;fsync=m.os.fsync;disk=m.shutil.disk_usage
        def commit(pending,destination):
            atomic(pending,destination)
            if destination.name=='COMPLETE.json':phase['committed']=True
        def checked(function):
            def call(*args,**kwargs):
                if phase['committed']:raise AssertionError('fallible I/O after COMPLETE publication')
                return function(*args,**kwargs)
            return call
        with patch.object(m.roster,'atomic_commit',side_effect=commit),patch.object(Path,'open',new=checked(opened)),patch.object(m.os,'fsync',side_effect=checked(fsync)),patch.object(m.shutil,'disk_usage',side_effect=checked(disk)):
            result=self.run_profile()
        self.assertTrue(phase['committed'])
        self.assertTrue(result['root_independent_reopen_and_postcommit_directory_sync_required'])
        self.assertFalse(result['postcommit_directory_durability_claimed_here'])
        marker=m.decode((self.output/'COMPLETE.json').read_bytes())
        self.assertTrue(marker['root_independent_reopen_and_postcommit_directory_sync_required'])

    def test_precommit_completion_fsync_failure_withholds_marker(self):
        original=m.os.fsync
        def fsync(descriptor):
            if (self.output/'.COMPLETE.json.pending').exists():raise OSError('synthetic precommit fsync failure')
            return original(descriptor)
        with patch.object(m.os,'fsync',side_effect=fsync),self.assertRaises(OSError):self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())
        self.assertTrue((self.output/'.COMPLETE.json.pending').exists())

    def test_final_fsync_cannot_replace_input_root_with_same_files_via_symlink(self):
        phase={'after_inputs':False,'mutated':False};reopen=m.BoundReader.reopen;fsync=m.os.fsync
        moved=self.root.with_name(self.root.name+'-moved')
        def reader_done(reader):reopen(reader);phase['after_inputs']=True
        def last_callback(descriptor):
            if phase['after_inputs'] and not phase['mutated']:
                self.root.rename(moved);self.root.symlink_to(moved,target_is_directory=True);phase['mutated']=True
            return fsync(descriptor)
        try:
            with patch.object(m.BoundReader,'reopen',new=reader_done),patch.object(m.os,'fsync',side_effect=last_callback),self.assertRaisesRegex(m.Blocked,'profile_primitive_root_or_ancestor_not_literal_directory'):
                self.run_profile()
            self.assertTrue(phase['mutated'])
            self.assertFalse((self.output/'COMPLETE.json').exists())
        finally:
            if self.root.is_symlink():self.root.unlink();moved.rename(self.root)

    def test_output_is_fresh_immutable_and_complete_binds_profile(self):
        self.run_profile(); before={p.name:p.read_bytes() for p in self.output.iterdir()}
        complete=m.decode(before['COMPLETE.json'])
        self.assertEqual(complete['profile']['sha256'],m.digest(before['profile.json']))
        with self.assertRaisesRegex(m.Blocked,'profile_output_not_fresh_literal'):self.run_profile()
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.output.iterdir()})

    def test_saved_footer_plan_is_enforced_before_extra_footer_read(self):
        meter=m.Meter(time.monotonic());meter.footer_ceiling=4
        with self.assertRaisesRegex(m.Blocked,'dedicated_source_footer_subcap'):m.profile_footer(self.entries[0],meter)
        self.assertEqual(meter.reads['source_footer'],4)

    def test_tiny_retained_cap_fails_without_overwrite_or_completed_marker(self):
        with patch.dict(m.PROPOSED,all_new_retained_bytes=64),self.assertRaisesRegex(m.Blocked,'profile_all_retained_byte_limit'):
            self.run_profile()
        self.assertFalse((self.output/'COMPLETE.json').exists())


class BoundsAndEntryTests(unittest.TestCase):
    def test_inclusive_deadline_precedes_input_IO_and_is_not_reset(self):
        with patch.object(Path,'open',side_effect=AssertionError('I/O before deadline')):
            with self.assertRaisesRegex(m.Blocked,'inclusive_profile_deadline'):m.Meter(0,lambda:1200)
        with self.assertRaisesRegex(m.Blocked,'outer_start_required'):m.Meter(True)

    def test_meter_retains_failed_charges_and_reserves_final_reads(self):
        meter=m.Meter(0,lambda:1);meter.charge('control',100)
        meter.reserve(m.PROPOSED['metadata_read_bytes']-100)
        with self.assertRaisesRegex(m.Blocked,'aggregate_metadata_read_limit_or_final_reserve'):meter.charge('source_footer',1)
        self.assertEqual(meter.usage()['metadata_bytes_charged'],100)

    def test_known_roster_audit_and_1GB_footer_plan_fits_but_old_full_control_replay_cannot_be_assumed(self):
        refs={name:{'path':'/mnt/data/'+name,'bytes':size,'sha256':'a'*64}
            for name,size in (('roster',65678058),('source_audit',14600000),('small',1000000))}
        plan=m.read_plan(refs,1000000000)
        self.assertLess(plan['metadata_read_upper_bytes'],2000000000)
        refs['all_old_lineage_JSON']={'path':'/mnt/data/old.json','bytes':522862870,'sha256':'b'*64}
        with self.assertRaisesRegex(m.Blocked,'planned_metadata_control_footer_output_reopens_do_not_fit'):
            m.read_plan(refs,1000000000)

    def test_caps_are_prospective_not_old_allowances_or_measured_index_fit(self):
        value=m.contract()
        self.assertFalse(value['production_admitted']);self.assertFalse(value['source_data_pages_read'])
        self.assertEqual(value['proposed_invocation_sublimits_not_admission_or_reset']['metadata_read_bytes'],2000000000)
        self.assertEqual(value['proposed_invocation_sublimits_not_admission_or_reset']['runtime_seconds_inclusive_outer_wrapper'],1200)
        self.assertEqual(value['proposed_invocation_sublimits_not_admission_or_reset']['all_new_retained_bytes'],64*1024**2)
        self.assertFalse(value['legacy_footer_identity_is_full_payload_SHA'])
        self.assertTrue(value['full_hash_cost_reported_not_performed'])

    def test_real_entry_guards_and_blocks_before_input_read(self):
        with patch.object(m,'require_production_host') as guard,patch.object(Path,'open',side_effect=AssertionError('real input opened')):
            with self.assertRaisesRegex(m.Blocked,'admission_missing_before_IO'):m.run_production('/mnt/data/roster.json')
            guard.assert_called_once_with()

    def test_contract_only_CLI_does_not_claim_or_attempt_production(self):
        with patch.object(m,'require_production_host',side_effect=AssertionError('not production')):
            with redirect_stdout(io.StringIO()) as stream:self.assertEqual(m.main(['--describe-contract']),0)
        self.assertFalse(json.loads(stream.getvalue())['production_admitted'])


if __name__=='__main__':unittest.main()
