"""Authenticated-stdin, four-file append/clock pilot; NOT an append release.

No economics, metadata, filtering, deduplication, providers or full-history scan.
Only the root bootstrap may enter production; small tests exercise private
composition seams, never the production guard with real local inputs.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
import base64
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import sys
import time

from production_guard import require_production_host
from scripts import run_polymarket_native_source_profile_v3 as host
from scripts import prepare_polymarket_append_inputs_v2 as inputs
from scripts import stream_polymarket_append_native_rows_v2 as native
from scripts import read_polymarket_append_clocks_v2 as clocks
from scripts import decode_polymarket_append_legacy_v2_v2 as bridge
from pipeline import decode_polymarket_v3_fill_v1 as wire

SCHEMA='polymarket_append_source_clock_pilot_v3'
DRIVER_PATH='scripts/run_polymarket_append_source_pilot_v3.py'
TEST_PATH='tests/test_run_polymarket_append_source_pilot_v3.py'
RUN=Path('/mnt/data/runs/2026-10-31_polymarket_append_source_pilot_v3')
REPO=Path('/home/ubuntu/prediction_markets')
DATA_ROOT=Path('/mnt/data/runs')
HEAD='443e3448cccfc22e2e75e86ea075699af1478ebc'
READ_LIMIT=2000000000
RUNTIME=1200
AS_LIMIT=8*1024**3
WORKING=1024**3
OUTPUT_CAP=1024**2
ADMISSION_CAP=16*1024  # New inline pilot control, not an old 8KiB OP slot.
STDOUT_CAP=64*1024
ROW_LIMIT=200000
CONTROL_LIMIT=32*1024**2
FREE_FLOOR=20000000000
CAPS={'read_bytes':READ_LIMIT,'runtime_seconds':RUNTIME,'address_space_bytes':AS_LIMIT,
      'working_bytes':WORKING,'all_new_artifact_bytes':OUTPUT_CAP,'stdout_bytes':STDOUT_CAP,
      'minimum_free_bytes':FREE_FLOOR,'spill_bytes':0,'workers':1,'raw_files':4,'raw_rows':ROW_LIMIT}
BUILD_REF={'path':'/mnt/data/runs/2026-10-28_polymarket_v3_source_build_v2/RESULT.json','bytes':35234,
           'sha256':'092ef0c618bc3f1ff33dd6fd23871482f3c9cfc51e9753f2bb381092a9435456'}
PARENT={'bytes':3231,'sha256':'3d50b848938b54e4d9ebdbe74aeb7aebd83c8dfdb6a743527e20031ea8d834ba'}
OLD_PARENT_END='2026-10-31T00:36:30Z'
PARENT_END='2026-10-31T01:56:30Z'
CONTINUATION={'bytes':5755,'sha256':'59d3d99fe077afd6868942a9f481001ebc0b55dd436338670a20f9a3103909af'}
PRIOR_ROOT_SUCCESS={'bytes':4676,'sha256':'8ccbf8f242d556346e7f0132e66d6477bd229b6d2bd431adc67183b37a1e25e3'}
DIAGNOSTIC={'bytes':9776,'sha256':'da39bebba06deae246ae294876a8af332c6d8fd5bb876fbcd966a4c72e06a094'}
FAILED_PILOT_HEAD='0a81a70a0e69ec4db2b8181cccd0f3905ef7f43a'
QA_AUXILIARY={'scripts/prepare_polymarket_append_inputs_v1.py','scripts/stream_polymarket_append_native_rows_v1.py',
              'scripts/read_polymarket_append_clocks_v1.py','scripts/run_polymarket_append_source_pilot_v1.py',
              'scripts/run_polymarket_append_source_pilot_v2.py'}
QA_FIXTURES={
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_optimized_pilot_continuation_reservation_2026-10-31_v1.json':(CONTINUATION['bytes'],CONTINUATION['sha256']),
 'tests/test_polymarket_exact_header_roster_v1.py':(35250,'0f93b7183cc37de935305a0da686790e2478886093fb44628c511049eaec2c4c'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/native_clock_root_final_resource_drain_remote_stdout_lossless_2026-10-30_v4.json':(46755,'1abdc2a40a5408a775d5e3412fca284fa9b7c52a051842626f856c6b84525203'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/header_v8_whole_acceptance_root_reopen_2026-10-30_v1.json':(7172,'f412eb0666b401fe51cc95b286502bea82ba75473e5df3b8dff3ea78e5579c23'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/header_v8_actual_dispatch_mirrors_v1/full_header_v8_admission_v1.json':(148155,'236ae088a430beb33d647b3ddca33ab7da9ecc54814447b1f9252433fa0e83df'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_only_source_controls_and_fourth_profile_drain_2026-10-30_v1.json':(289060,'4a7666eb7d6475e0e679852f2ef4e9f7dd16265d20964b2a2fcc53d8566522e0'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_selected_child_original_controls_2026-10-30_v1.json':(422849,'e6efdaa73626fb4ff52d3fdc42fa4eb728a58b845258d873b4e3f0a6298de060'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_only_preparation_reservation_2026-10-30_v4.json':(2503,'8fcd9652043c16706e68c7e5670fbbb5f4072dc44369f1cf4028beb73c782ea6'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_only_preparation_reservation_2026-10-30_v5.json':(PARENT['bytes'],PARENT['sha256']),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_source_pilot_actual_execution_2026-10-30_v1.jsonl':(103984,'a4dd8b1238ccf0fef61d0ac0917c6f1e75a06d987a0a231c1687fc1c40e05264'),
 'output/polymarket_oct1_finalization_2026-10-27_v1/append_pilot_first_header_footer_diagnostic_2026-10-30_v1.json':(DIAGNOSTIC['bytes'],DIAGNOSTIC['sha256'])}
FAILURES={
 'one':(803,'e38cff473c2b6b16fb06a4e73ef89e9bc5c84e142fefdf511f79855ec656c445','0c9a337ca8ff898acf736439d0b6ca33025152d8'),
 'two':(1853,'f685e9a24e04d37986f91721d39e028472367451ed2e2e72c432918222075f7d','665290b351f237ac7436c5643667b4de39284f8c'),
 'three':(108722,'6221d8765fd370fa46fd8d0945fb65f55055d6e58ac06d3bd8bd6f6fab6f07f6','3b995f1fd99e6cd7f750de38f023ae3c08635d2f'),
 'four':(108966,'482274cfd98fa5547a2d183e98ee5d7bdc6189050215af8259148c95b98e9837','77616d45e79d1074eb36db619c97c6cb225bf4b5'),
 'five':(103984,'a4dd8b1238ccf0fef61d0ac0917c6f1e75a06d987a0a231c1687fc1c40e05264',FAILED_PILOT_HEAD)}
RUNTIME_FILES=frozenset('''pipeline/decode_polymarket_v3_fill_v1.py
pipeline/normalize_polymarket_v3_archive_v1.py
production_guard.py
scripts/audit_polymarket_native_overlap.py
scripts/build_polymarket_exact_header_roster_v1.py
scripts/build_polymarket_native_clock_layer_v1.py
scripts/collect_polymarket_exact_headers_v1.py
scripts/collect_polymarket_native_increment.py
scripts/collect_polymarket_native_increment_v2.py
scripts/decode_polymarket_append_legacy_v2_v2.py
scripts/inventory_polymarket_exact_header_keys_v1.py
scripts/inventory_polymarket_refresh.py
scripts/plan_polymarket_refresh.py
scripts/prepare_polymarket_append_inputs_v2.py
scripts/prepare_polymarket_native_clock_layer_v1.py
scripts/prepare_polymarket_native_source_profile_v1.py
scripts/probe_polymarket_increment_capacity.py
scripts/probe_polymarket_protocol_activity.py
scripts/profile_polymarket_native_clock_sources_v1.py
scripts/project_polymarket_append_rows_v1.py
scripts/read_polymarket_append_clocks_v2.py
scripts/replay_polymarket_native_profile_lineage_v1.py
scripts/run_polymarket_native_source_profile_v3.py
scripts/stream_polymarket_append_native_rows_v2.py'''.splitlines())|{DRIVER_PATH}
QA_MODULES=tuple('tests.test_'+name for name in (
 'build_polymarket_native_clock_layer_v1','prepare_polymarket_native_clock_layer_v1',
 'profile_polymarket_native_clock_sources_v1','prepare_polymarket_native_source_profile_v1',
 'replay_polymarket_native_profile_lineage_v1','run_polymarket_native_source_profile_v3',
 'prepare_polymarket_append_inputs_v1','prepare_polymarket_append_inputs_v2',
 'stream_polymarket_append_native_rows_v1','stream_polymarket_append_native_rows_v2',
 'read_polymarket_append_clocks_v1','decode_polymarket_append_legacy_v2_v2',
 'project_polymarket_append_rows_v1','run_polymarket_append_source_pilot_v1',
 'read_polymarket_append_clocks_v2','run_polymarket_append_source_pilot_v2',
 'run_polymarket_append_source_pilot_v3'))
QA_TESTS=534  # Exact old 494 plus 40 dedicated optimized V3 fixture methods.
_BOOTSTRAP=None
_LSTAT,_SCANDIR,_CLOCK=os.lstat,os.scandir,time.monotonic
Blocked=inputs.InputBlocked
require,encode,decode,digest=inputs.require,inputs.encode,inputs.decode,inputs.digest


class Meter(host.Meter):
    """Single cumulative read ledger; no component gets another 2GB allowance."""
    def __init__(self,started,deadline,bootstrap):
        super().__init__(started,deadline,bootstrap)
        self.shared=ROW_LIMIT*1024+CONTROL_LIMIT*2+16*1024**2+OUTPUT_CAP
        self.peak_working=0

    def memory(self,size):
        total=self.shared+size
        self.peak_working=max(self.peak_working,total)
        require(total<=WORKING,'pilot_aggregate_working_limit')

    def usage(self,**_):
        return {'read_bytes_charged':sum(self.reads.values()),'bytes_by_kind':dict(self.reads),
                'reserved_final_reads':self.reserved_final_reads,'peak_logical_working_bytes':self.peak_working,
                'elapsed_inclusive_seconds':_CLOCK()-self.started,'read_limit':READ_LIMIT}


class Component(clocks._Meter):
    def __init__(self,parent,kind,rows,files,file_refs):
        self.parent,self.kind=parent,kind
        super().__init__({'source_bytes':READ_LIMIT,'read_bytes':READ_LIMIT,'rows':rows,
            'batches':1000000,'working_bytes':WORKING,'runtime_seconds':RUNTIME,
            'io_operations':10000000,'files':files,'file_refs':file_refs},_CLOCK)
        self.deadline=parent.deadline

    def check(self):
        self.parent.check();super().check()

    def read(self,count,phase):
        self.parent.charge(self.kind+'_'+phase,count);super().read(count,phase)

    def primitive_working(self,count=0):
        super().primitive_working(count)
        # One fresh child walk; historical peak is safe only with constant
        # parent.shared/limits and fail-terminal producer execution.
        self.parent.memory(self.usage['peak_working_bytes'])


class Reader:
    """Actual bounded control files and explicit absent candidate observations."""
    def __init__(self,meter):
        self.meter=meter;self.files={};self.absent=set();self.control_bytes=0

    def callback(self,request):
        require(type(request) is dict and set(request)=={'path','maximum_bytes'},'pilot_control_request')
        path=Path(inputs.literal(request['path']))
        require(DATA_ROOT in path.parents and path.suffix=='.json','pilot_control_not_payload')
        maximum=request['maximum_bytes'];require(type(maximum) is int and 0<maximum<=inputs.MAX_CONTROL,'pilot_control_cap')
        try:before=host.identity(path)
        except FileNotFoundError:
            self.absent.add(str(path));return None
        require(str(path) not in self.absent,'pilot_absent_control_appeared')
        require(before['bytes']<=maximum,'pilot_control_size')
        self.control_bytes+=before['bytes'];require(self.control_bytes<=CONTROL_LIMIT,'pilot_control_union_limit')
        self.meter.charge('control',before['bytes']+1)
        with path.open('rb',buffering=0) as stream:raw=stream.read(before['bytes']+1)
        require(len(raw)==before['bytes'] and host.identity(path)==before,'pilot_control_race')
        saved={'identity':before,'bytes':len(raw),'sha256':digest(raw)}
        require(str(path) not in self.files or self.files[str(path)]==saved,'pilot_control_changed')
        self.files[str(path)]=saved
        return raw

    def full_reopen(self):
        for name,saved in sorted(self.files.items()):
            self.meter.final_charge('control_final',saved['bytes']+1)
            with open(name,'rb',buffering=0) as stream:raw=stream.read(saved['bytes']+1)
            require(len(raw)==saved['bytes'] and digest(raw)==saved['sha256']
                    and host.identity(name)==saved['identity'],'pilot_control_final_changed')


def selection(plan,reader):
    children=plan['campaign_children_to_resolve']
    chosen=[next(r for r in children if r['collector_version']==1),
            next(r for r in children if r['collector_version']==2),children[-1]]
    expanded=[inputs.resolve_child(r,reader.callback) for r in chosen]
    records=[deepcopy(plan['probe']['payloads'][0])]+[deepcopy(r['payloads'][0]) for r in expanded]
    require(len({r['path'] for r in records})==4 and sum(r['rows'] for r in records)<=ROW_LIMIT,
            'pilot_fixed_full_file_selection_row_limit')
    for index,record in enumerate(records):
        record.update(source_file_index=index,source_group='append_probe_v2' if index==0 else 'append_campaign_v2')
        require(inputs.FIRST<=record['first_block']<=record['last_block']<=inputs.LAST,'pilot_selected_window_range')
    # Authenticate the selected original request/SUCCESS bytes, but do not
    # claim a complete collection-journal replay from these four examples.
    selected_controls([plan['probe'],*expanded],records,reader)
    return records,expanded


def selected_controls(groups,records,reader):
    for group,record in zip(groups,records):
        parent=str(Path(record['path']).parent)
        refs=[r for r in group['request_ledgers'] if str(Path(r['path']).parent)==parent]
        refs += [r for r in group['success_and_split_controls'] if r['path']==parent+'/SUCCESS.json']
        require(len(refs)==2,'pilot_selected_request_SUCCESS_refs')
        for ref in refs:
            raw=reader.callback({'path':ref['path'],'maximum_bytes':inputs.MAX_CONTROL})
            require(type(raw) is bytes and len(raw)==ref['bytes'] and digest(raw)==ref['sha256'],
                    'pilot_selected_control_full_SHA')


def footer_hints(records,ranges,meter):
    """Only literal footer bytes; stats route but never authenticate a clock."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    component=Component(meter,'header_footer',40000000,330,330)
    selected=[];captures={}
    for record in records:
        path=Path(record['path']);before=clocks._file_identity(path)
        require(before[6]==record['bytes'],'pilot_header_length')
        parts=[]
        with clocks._Source(path,before,component) as source:
            source.phase='arrow'  # Footer hints are NOT full-payload SHA reads.
            source.seek(record['bytes']-8);tail=source.read(8)
            length=int.from_bytes(tail[:4],'little')
            require(tail[4:]==b'PAR1' and 0<length<=1024**2 and length+12<=record['bytes'],'pilot_footer_bound')
            source.seek(record['bytes']-8-length);raw=source.read(length)
            source.seek(0);magic=source.read(4);require(magic==b'PAR1','pilot_parquet_magic')
            parts=[(record['bytes']-8,tail),(record['bytes']-8-length,raw),(0,magic)]
        component.hold('footer_buffer',4*(length+12))
        metadata=pq.read_metadata(pa.BufferReader(magic+raw+tail))
        require(metadata.schema.to_arrow_schema().equals(clocks.header_schema(),check_metadata=True)
                and metadata.num_rows==record['rows'],'pilot_header_footer_schema_or_rows')
        overlaps=False
        for group in range(metadata.num_row_groups):
            column=metadata.row_group(group).column(0);stats=column.statistics
            require(column.file_path in ('',None) and column.compression=='ZSTD'
                    and stats is not None and stats.has_min_max and stats.null_count==0
                    and stats.num_values==metadata.row_group(group).num_rows
                    and type(stats.min) is int and type(stats.max) is int and 0<stats.min<=stats.max<2**63,
                    'pilot_integer_footer_routing_stats')
            overlaps|=any(first<=stats.max and last>=stats.min for first,last in ranges)
        if overlaps:selected.append(record['source_file_index'])
        captures[str(path)]={'identity':before,'parts':[(offset,len(raw),digest(raw)) for offset,raw in parts]}
        component.drop('footer_buffer');del metadata,parts,raw,tail,magic
    require(selected,'pilot_no_header_routing_candidates')
    return selected,captures


def strict_row(row,locator,wire_admission):
    address=row.get('address');require(type(address) is str,'pilot_native_address')
    if address.lower() in bridge.EMITTERS:
        value=bridge.decode_row(row,locator)
        require(value['status']=='decoded_representation_not_admitted','pilot_strict_legacy_V2_wire')
        return value['trace']['protocol_generation'],value['event']['block_number'],value['trace']['native_observed_block_hash']
    require(address.lower()==wire.archive.EXCHANGE,'pilot_unknown_native_emitter')
    value=wire.decode_native_row(row,wire_admission)
    return 'v3_'+value['provenance']['source_era'],value['event']['block_number'],value['event']['block_hash']


class _NativeConsumer:
    """Sealed production consumer: fixed wire checks and bounded scalar state.

    No caller callback is accepted by the production stream. The parent's
    fixed ROW_LIMIT*1024 pool reserves all simultaneous consumer state; every
    batch remeasures it, and row/key counts are checked on every observation.
    """
    def __init__(self,records,wire_admission):
        self.windows={r['source_file_index']:(r['first_block'],r['last_block']) for r in records}
        self.wire_admission=wire_admission
        self.demands={};self.multiplicity=Counter();self.protocols=Counter()
        self.clock_values={};self.clock_locators={};self.processed=0

    def observe(self,row,locator):
        protocol,block,observed=strict_row(row,locator,self.wire_admission)
        first,last=self.windows[locator['source_file_index']]
        require(first<=block<=last,'pilot_raw_block_outside_selected_window')
        require(block not in self.demands or self.demands[block]==observed,'pilot_conflicting_native_block_hash')
        self.demands[block]=observed
        self.multiplicity[(block,protocol)]+=1;self.protocols[protocol]+=1;self.processed+=1
        require(self.processed<=ROW_LIMIT and len(self.demands)<=ROW_LIMIT
                and len(self.multiplicity)<=ROW_LIMIT,'pilot_demand_memory_bound')

    def clock(self,row,file_index,ordinal):
        block,hashed,_,timestamp=row
        require(block in self.demands and block not in self.clock_values,'pilot_duplicate_demanded_clock')
        require(hashed==self.demands[block] and timestamp<inputs.CUTOFF,'pilot_clock_hash_or_cutoff')
        self.clock_values[block]=timestamp;self.clock_locators[block]=(file_index,ordinal)

    def measure(self):
        size=native._python_bytes([self.windows,self.demands,dict(self.multiplicity),
            dict(self.protocols),self.clock_values,self.clock_locators])
        require(size<=ROW_LIMIT*1024,'pilot_retained_consumer_working_bound')
        return size


def stream_native(records,meter,consumer):
    """Fixed four-file stream; no arbitrary row callback or per-row roster walk."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    require(type(consumer) is _NativeConsumer,'pilot_sealed_native_consumer_required')
    component=Component(meter,'native',ROW_LIMIT,4,4)
    scalar=[{k:r[k] for k in native.RECORD_FIELDS} for r in records]
    captured=native._records(scalar,DATA_ROOT,component)
    component.keep('manifest',scalar);component.hold('reader_state',16384)
    component.add('source_bytes',sum(r['bytes'] for r in records))
    require(sum(r['rows'] for r in records)<=ROW_LIMIT,'pilot_native_declared_row_limit')
    native._reopen(captured)
    for record,before,ancestors in captured:
        ordinal=0;path=Path(record['path'])
        active=[(record,before,ancestors)]
        with native._Source(path,before,component) as source:
            hashed=hashlib.sha256()
            while source.tell()<record['bytes']:
                hashed.update(source.read(min(native.HASH_CHUNK,record['bytes']-source.tell())))
            require(hashed.hexdigest()==record['sha256'],'pilot_full_native_SHA')
            source.seek(0);source.phase='arrow';component.hold('arrow_compressed',2*record['bytes'])
            with pa.PythonFile(source,mode='r') as buffer:
                parquet=pq.ParquetFile(buffer,pre_buffer=False,buffer_size=0,
                    thrift_string_size_limit=1024**2,thrift_container_size_limit=10000)
                require(parquet.schema_arrow.equals(native.native_schema(),check_metadata=True)
                        and parquet.metadata.num_rows==record['rows'],'pilot_native_schema_count')
                size=0
                for group in range(parquet.metadata.num_row_groups):
                    for index in range(parquet.metadata.num_columns):
                        column=parquet.metadata.row_group(group).column(index)
                        require(column.file_path in ('',None) and column.compression=='ZSTD','pilot_external_native_column')
                        require(type(column.total_uncompressed_size) is int and column.total_uncompressed_size>=0,
                                'pilot_native_decode_size')
                        size+=column.total_uncompressed_size
                component.hold('arrow_decoded',size)
                for batch in parquet.iter_batches(batch_size=native.BATCH_ROWS,use_threads=False):
                    component.add('batches',1);component.hold('batch',batch.nbytes)
                    require(0<batch.num_rows<=native.BATCH_ROWS and ordinal+batch.num_rows<=record['rows'],
                            'pilot_native_batch_physical_bound')
                    reserve=32*batch.nbytes+4096*batch.num_rows
                    component.hold('rows_buffer',reserve)
                    rows=batch.to_pylist()
                    require(native._python_bytes(rows)<=reserve,'pilot_row_conversion_bound')
                    component.keep('rows_buffer',rows)
                    # Count is reserved before this bounded batch. A failure
                    # cannot claim all reserved rows were actually processed.
                    component.add('rows',batch.num_rows)
                    component.hold('locator',16384)
                    # Wire helpers serialize/deep-copy one bounded row. Keep
                    # that transient allocation covered in addition to the
                    # live Arrow/Python batch, before entering any row helper.
                    component.hold('wire_temporaries',32*batch.nbytes+8*1024**2)
                    native._reopen(active);consumer.measure()
                    for row in rows:
                        locator={'source_path':record['path'],'source_file_index':record['source_file_index'],
                                 'physical_row_ordinal':ordinal}
                        consumer.observe(row,locator)
                        ordinal+=1
                    consumer.measure();component.check();native._reopen(active)
                    del row,rows,batch,locator
                    component.hold('wire_temporaries',0)
                    component.drop('locator');component.drop('rows_buffer');component.hold('batch',0)
                require(ordinal==record['rows'],'pilot_native_physical_count')
                native._reopen(active);del parquet
            component.hold('arrow_decoded',0);component.hold('arrow_compressed',0)
        component.add('files',1)
    component.check();component.primitive_working();consumer.measure();native._reopen(captured)
    return {'usage':dict(component.usage),'identities':{r['path']:s for r,s,_ in captured},
        'physical_rows_newly_wire_checked':consumer.processed,
        'row_counter_semantics':'reserved_batch_rows; success_equals_physically_processed_rows',
        'all_reference_identity_checks':'entry_and_final; active_file_at_reads_and_batches',
        'arbitrary_consumer_callbacks_accepted':False}


def _header_reuse_contract(controls,headers):
    """Authenticate raw accepted controls, never an accepted=True object."""
    require(type(controls) is tuple and len(controls)==3,'pilot_header_raw_controls_required')
    acceptance,manifest,complete=controls
    for body,role in zip(controls,('header_acceptance','headers','headers_complete')):
        host.bytes_binding(body,inputs.ANCHORS[role])
    derived=inputs._headers(acceptance,decode(manifest),decode(complete))
    actual=[{**{k:r[k] for k in ('path','bytes','sha256')},'rows':r['keys'],'source_file_index':r['chunk']}
            for r in derived]
    require(headers==actual and all(type(r[k]) is type(a[k]) for r,a in zip(headers,actual) for k in a),
            'pilot_header_reference_not_accepted_manifest')
    return {'receipt':dict(inputs.ANCHORS['header_acceptance']),
        'manifest':dict(inputs.ANCHORS['headers']),'whole_campaign_accepted_keys':32901442,
        'upstream_integer_order_and_cross_chunk_parent_checks_reused':True,
        'whole_campaign_reverified_here':False}


def _header_controls(reader,headers):
    controls=tuple(reader.callback({'path':inputs.ANCHORS[role]['path'],'maximum_bytes':inputs.MAX_CONTROL})
                   for role in ('header_acceptance','headers','headers_complete'))
    _header_reuse_contract(controls,headers)
    return controls


def _stream_demanded_clocks(headers,indices,meter,consumer):
    """Full-SHA selected files; Arrow vector lookup; Python checks only demands.

    Called only after _header_reuse_contract in the authenticated composition.
    This private reader itself confers no upstream acceptance authority.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    require(type(consumer) is _NativeConsumer and consumer.demands,'pilot_sealed_clock_demand_required')
    require(type(indices) is list and all(type(i) is int for i in indices)
            and all(a<b for a,b in zip(indices,indices[1:])),'pilot_header_selected_indices')
    refs=tuple(clocks._reference(r,clocks.RECORD_FIELDS) for r in headers)
    require(len(refs)<=330 and len({r[0] for r in refs})==len(refs)
            and all(a[4]<b[4] for a,b in zip(refs,refs[1:])),'pilot_header_reference_order')
    selected=tuple(r for r in refs if r[4] in indices)
    require(len(selected)==len(indices)>0,'pilot_header_unknown_selected_index')
    component=Component(meter,'clock',40000000,330,330)
    # Owned immutable scalar bindings are measured once, then reserved as
    # bytes; no mutable caller-container size is cached or discounted.
    component.hold('immutable_bindings',4*clocks._python_bytes((refs,selected))+65536)
    captured=[];seen=set()
    for record in selected:
        path=Path(record[0]);identity=clocks._file_identity(path);ancestors=clocks._ancestors(path)
        require(identity[6]==record[1] and identity[:2] not in seen,'pilot_header_identity_or_alias')
        seen.add(identity[:2]);captured.append((record,identity,ancestors))
    component.keep('selected_captures',captured);component.hold('reader_state',16384)
    component.add('file_refs',len(refs));component.add('source_bytes',sum(r[1] for r in selected))
    component.hold('demand_array',len(consumer.demands)*512+65536)
    wanted=pa.array(sorted(consumer.demands),type=pa.int64())
    clocks._reopen(captured);file_counts=[]
    for record,identity,ancestors in captured:
        path,size,sha,expected_rows,index=record;ordinal=0;matched=0
        active=[(record,identity,ancestors)]
        with clocks._Source(Path(path),identity,component) as source:
            hashed=hashlib.sha256()
            while source.tell()<size:hashed.update(source.read(min(clocks.HASH_CHUNK,size-source.tell())))
            require(hashed.hexdigest()==sha,'pilot_full_clock_SHA')
            source.seek(0);source.phase='arrow';component.hold('arrow_compressed',2*size)
            with pa.PythonFile(source,mode='r') as buffer:
                parquet=pq.ParquetFile(buffer,pre_buffer=False,buffer_size=0,
                    thrift_string_size_limit=1024**2,thrift_container_size_limit=10000)
                require(parquet.schema_arrow.equals(clocks.header_schema(),check_metadata=True)
                        and parquet.metadata.num_rows==expected_rows,'pilot_clock_schema_count')
                decoded=0
                for group in range(parquet.metadata.num_row_groups):
                    for column in range(4):
                        item=parquet.metadata.row_group(group).column(column)
                        require(item.file_path in ('',None) and item.compression=='ZSTD','pilot_external_header_column')
                        require(type(item.total_uncompressed_size) is int and item.total_uncompressed_size>=0,
                                'pilot_header_decode_size')
                        decoded+=item.total_uncompressed_size
                component.hold('arrow_decoded',decoded)
                for batch in parquet.iter_batches(batch_size=clocks.BATCH_ROWS,use_threads=False):
                    component.add('batches',1);component.hold('batch',batch.nbytes)
                    require(0<batch.num_rows<=clocks.BATCH_ROWS and ordinal+batch.num_rows<=expected_rows,
                            'pilot_clock_batch_physical_bound')
                    component.add('rows',batch.num_rows)
                    reserve=32*batch.nbytes+4096*batch.num_rows
                    component.hold('lookup_buffers',reserve);clocks._reopen(active)
                    mask=pc.is_in(batch.column(0),value_set=wanted)
                    positions=pc.indices_nonzero(mask).to_pylist()
                    matched_batch=batch.filter(mask)
                    component.hold('matched_arrow',matched_batch.nbytes)
                    values=matched_batch.to_pylist()
                    require(clocks._python_bytes([positions,values])<=reserve,'pilot_clock_lookup_conversion_bound')
                    component.keep('matched_rows',values)
                    for offset,row in zip(positions,values):
                        require(type(offset) is int and 0<=offset<batch.num_rows,'pilot_clock_vector_locator')
                        value=clocks._header(clocks._canonical_header(row),None)
                        consumer.clock(value,index,ordinal+offset)
                    require(len(positions)==len(values),'pilot_clock_vector_count')
                    matched+=len(values);component.usage['emitted_rows']+=len(values)
                    component.usage['callbacks']+=bool(values)
                    consumer.measure();ordinal+=batch.num_rows
                    component.check();clocks._reopen(active)
                    del positions,values,mask,matched_batch,batch
                    component.drop('matched_rows');component.hold('matched_arrow',0)
                    component.hold('lookup_buffers',0);component.hold('batch',0)
                require(ordinal==expected_rows,'pilot_clock_physical_count');del parquet
            component.hold('arrow_decoded',0);component.hold('arrow_compressed',0)
        file_counts.append({'source_file_index':index,'observed_rows':ordinal,'newly_checked_demand_rows':matched})
        component.add('files',1)
    require(tuple(clocks._reference(r,clocks.RECORD_FIELDS) for r in headers)==refs
            and tuple(indices)==tuple(r[4] for r in selected),'pilot_header_binding_changed')
    require(component.usage['rows']==sum(r[3] for r in selected)
            and component.usage['hash_read_bytes']==component.usage['source_bytes'],'pilot_clock_read_count_law')
    component.hold('report',4*clocks._python_bytes((refs,captured,file_counts))+65536)
    names=('device','inode','mode','nlink','uid','gid','bytes','mtime_ns','ctime_ns')
    result={'usage':dict(component.usage),'file_counts':file_counts,
        'source_identities':{r[0]:dict(zip(names,s)) for r,s,_ in captured},
        'all_file_bindings':[dict(zip(clocks.RECORD_FIELDS,r)) for r in refs],
        'unselected_file_indices':[r[4] for r in refs if r[4] not in indices],
        'vectorized_payload_rows_read':component.usage['rows'],
        'physical_accepted_header_rows_reused':sum(r[3] for r in selected),
        'demanded_header_rows_newly_checked':len(consumer.clock_values),
        'full_selected_payload_SHA_before_lookup':True,'full_new_order_parent_replay_performed':False}
    component.keep('report',result);component.check();component.primitive_working()
    clocks._reopen(captured)
    return result


def _project_pilot(records,headers,indices,meter,wire_admission,header_controls):
    reuse=_header_reuse_contract(header_controls,headers)
    consumer=_NativeConsumer(records,wire_admission)
    native_began=_CLOCK();raw=stream_native(records,meter,consumer)
    native_seconds=_CLOCK()-native_began
    clock_began=_CLOCK();clock_result=_stream_demanded_clocks(headers,indices,meter,consumer)
    clock_seconds=_CLOCK()-clock_began
    require(set(consumer.clock_values)==set(consumer.demands),'pilot_missing_exact_clock_coverage')
    months=Counter()
    for (block,protocol),count in consumer.multiplicity.items():
        month=datetime.fromtimestamp(consumer.clock_values[block],timezone.utc).strftime('%Y-%m')
        months[(protocol,month)]+=count
    require(sum(consumer.protocols.values())==sum(months.values())==sum(r['rows'] for r in records)
            ==consumer.processed,'pilot_row_clock_month_conservation')
    return {'raw_rows':consumer.processed,'physical_raw_rows_newly_wire_checked':consumer.processed,
        'demanded_block_keys':len(consumer.demands),'matched_block_keys':len(consumer.clock_values),
        'protocol_counts':dict(consumer.protocols),
        'protocol_month_counts':[{'protocol':p,'year_month':m,'rows':n} for (p,m),n in sorted(months.items())],
        'native_component':raw,'clock_component':clock_result,'header_acceptance_reuse':reuse,
        'matched_header_physical_locators':[{'block_number':block,'source_file_index':loc[0],
            'physical_row_ordinal':loc[1]} for block,loc in sorted(consumer.clock_locators.items())],
        'observed_hash_compared_with_joined_hash':True,'provider_timestamp_fallback':False,
        'consumer_logical_bytes':consumer.measure(),'consumer_reserved_bytes':ROW_LIMIT*1024,
        'native_scan_seconds':native_seconds,'clock_scan_seconds':clock_seconds,
        'economic_projection':False,'metadata_join':False,'deduplication':False,'final_dataset_admitted':False}


def first_pilot_history(raw,diagnostic):
    """Actual V1 failure/format evidence, not a fresh-host absence certificate."""
    size,sha,_=FAILURES['five']
    require(type(raw) is bytes and len(raw)==size and digest(raw)==sha,'pilot_V1_failure_full_pin')
    require(type(diagnostic) is bytes and len(diagnostic)==DIAGNOSTIC['bytes']
        and digest(diagnostic)==DIAGNOSTIC['sha256'],'pilot_original_binary32_diagnostic_pin')
    rows=[decode(line) for line in raw.splitlines()]
    admissions=[r for r in rows if r.get('event')=='root_append_pilot_actual_admission']
    exits=[r for r in rows if r.get('event')=='root_append_pilot_actual_exit']
    require(len(admissions)==len(exits)==1 and type(admissions[0].get('admission_utf8')) is str,'pilot_V1_failure_event_roster')
    old_raw=admissions[0]['admission_utf8'].encode();old=decode(old_raw);ended=exits[0]
    require(old.get('schema_version')=='polymarket_append_source_pilot_admission_v1'
        and old.get('operator_source_head')==FAILED_PILOT_HEAD
        and old.get('run_dir')=='/mnt/data/runs/2026-10-30_polymarket_append_source_pilot_v1'
        and old.get('caps')==CAPS and type(ended.get('exit_code')) is int and ended['exit_code']==2,
        'pilot_V1_original_epoch_and_scope')
    chunks=[r['raw_UTF8'] for r in rows if r.get('event')=='root_append_pilot_producer_stdout']
    require(chunks and all(type(c) is str for c in chunks),'pilot_V1_saved_stdout_chunks')
    stdout=''.join(chunks);encoded=stdout.encode()
    require(type(ended.get('stdout_bytes')) is int and ended['stdout_bytes']==len(encoded)
        and ended.get('stdout_sha256')==digest(encoded),'pilot_V1_stdout_full_binding')
    decoder=json.JSONDecoder();messages=[];remaining=stdout
    while remaining.strip():
        remaining=remaining.lstrip();value,n=decoder.raw_decode(remaining);messages.append(value);remaining=remaining[n:]
    require(len(messages)==2 and messages[0].get('status')=='producer_process_observed_before_host_checks'
        and messages[0].get('admission_sha256')==digest(old_raw)
        and messages[1]=={'schema_version':'polymarket_append_source_clock_pilot_v1','status':'pilot_withheld',
                         'error_code':'pilot_header_footer_schema_or_rows'},'pilot_V1_known_pre_RUN_failure')
    info=decode(diagnostic)
    require(info.get('schema_version')=='append_pilot_first_header_footer_diagnostic_v1'
        and type(info.get('actual_num_rows')) is int and info['actual_num_rows']==100000
        and [f.get('type') for f in info.get('schema_fields',[])]==['int64','fixed_size_binary[32]','fixed_size_binary[32]','int64']
        and [f.get('name') for f in info['schema_fields']]==list(clocks.FIELDS)
        and info.get('pilot_RUN_absent') is True and info.get('owned_group_and_session_absent') is True
        and type(info.get('header_body_or_native_trade_reads')) is int and info['header_body_or_native_trade_reads']==0
        and type(info.get('remote_writes')) is int and info['remote_writes']==0
        and info.get('boot_id')==messages[0].get('boot_id'),'pilot_V1_original_format_and_scoped_drain')
    process=messages[0]['process'];birth=info.get('failed_pilot_owned_pid_birth')
    require(type(birth) is dict and birth=={key:process[key] for key in ('pid','birth_ticks','group','session')},
            'pilot_V1_saved_owned_birth')
    footer=base64.b64decode(info['original_footer_envelope_base64'],validate=True)
    require(len(footer)==info['original_footer_envelope_bytes']<=8192
        and digest(footer)==info['original_footer_envelope_sha256'],'pilot_V1_original_footer_bytes')


def optimized_continuation(raw):
    """New exact prospective component; old V2 reservation is never relabeled."""
    require(type(raw) is bytes and len(raw)==CONTINUATION['bytes']
            and digest(raw)==CONTINUATION['sha256'],'pilot_new_continuation_full_pin')
    value=decode(raw)
    require(value.get('schema_version')=='polymarket_append_optimized_pilot_continuation_reservation_v1'
        and value.get('status')=='prospective_root_reservation_requires_independent_review_and_actual_stage_admissions'
        and value.get('prospective_RUN')==str(RUN),'pilot_new_continuation_schema')
    components={'new_local_native_only_implementation_independent_review_and_fixture_QA':1800,
        'new_source_preparation_with_Linux_QA_and_root_reopen':600,
        'new_fresh_full_host_resource_census_and_owned_exit':600,
        'new_optimized_four_source_clock_pilot':1200,
        'new_pilot_terminal_root_verification_and_owned_exit':600}
    require(value.get('components_seconds')==components
        and all(type(value['components_seconds'][k]) is int for k in components)
        and type(value.get('retained_prior_runtime_seconds')) is int and value['retained_prior_runtime_seconds']==221008
        and type(value.get('additional_prospective_runtime_seconds')) is int
        and value['additional_prospective_runtime_seconds']==sum(components.values())==4800
        and type(value.get('conservative_carry_after_seconds')) is int
        and value['conservative_carry_after_seconds']==221008+4800==225808
        and value.get('unchanged_runtime_ceiling_seconds')==259200
        and value.get('remaining_after_full_reservation_seconds')==259200-225808
        and value.get('new_window_start_utc')==OLD_PARENT_END and value.get('new_absolute_deadline_utc')==PARENT_END,
        'pilot_new_continuation_arithmetic')
    require(value.get('old_preparation')=={'path':'append_only_preparation_reservation_2026-10-30_v5.json',
        **PARENT,'absolute_deadline_utc':OLD_PARENT_END,'full_reservation_retained':True}
        and value.get('prior_root_continuation')=={'path':'append_root_preflight_continuation_2026-10-30_v2.json',
            'bytes':3104,'sha256':'4462f941d63e2e8e1187711d2c6d0e022ef654a4d2ee08887294a5f6c801daf5'},
        'pilot_new_continuation_original_parents')
    require(value.get('current_native_upper_bytes')==49296236502
        and value.get('new_artifact_reservations_bytes')=={'Git_metadata_and_private_index':16777216,
            'Linux_QA_scratch_and_logs':16777216,'new_pilot_RUN_and_full_stdout':OUTPUT_CAP}
        and value.get('new_artifact_sum_bytes')==34603008
        and value.get('native_upper_after_full_reservations_bytes')==49296236502+34603008==49330839510
        and value.get('unchanged_native_retained_and_scratch_ceiling_bytes')==64000000000
        and value.get('unchanged_native_spill_ceiling_bytes')==16000000000
        and value.get('new_spill_reservation_bytes')==0 and value.get('new_OP_refs')==0
        and value.get('shared_external_pool_bytes_unchanged')==2094547
        and value.get('shared_external_pool_refs_unchanged')==95
        and value.get('whole_header_future_reservation_bytes_unchanged')==786432
        and value.get('whole_header_future_slots_unchanged')==9,'pilot_new_continuation_storage')
    require(value.get('prospective_pilot_subcaps')=={'runtime_seconds':RUNTIME,'read_bytes':READ_LIMIT,
        'RUN_plus_full_stdout_bytes':OUTPUT_CAP,'AS_bytes':AS_LIMIT,'working_bytes':WORKING,
        'minimum_free_bytes':FREE_FLOOR,'spill_bytes':0}
        and value.get('baseline_or_archive_convention_changes') is False
        and value.get('final_extended_dataset_admitted') is False
        and value.get('release_admitted') is False,'pilot_new_continuation_scope')
    return value


def prior_successful_pilot_root(raw):
    """Historical exact terminal-byte proof, explicitly NOT current drain."""
    require(type(raw) is bytes and len(raw)==PRIOR_ROOT_SUCCESS['bytes']
            and digest(raw)==PRIOR_ROOT_SUCCESS['sha256'],'pilot_prior_successful_root_full_pin')
    value=decode(raw);body=value.get('terminal_checker_stdout_UTF8')
    require(value.get('schema_version')=='polymarket_append_pilot_root_terminal_integration_v2'
        and value.get('status')=='read_only_terminal_reopen_verified_not_final_admission'
        and type(body) is str and value.get('terminal_checker_stdout')=={'bytes':len(body.encode()),'sha256':digest(body.encode())}
        and value.get('canonical_RUN_bytes')==89466 and value.get('RUN_plus_FULL_producer_stdout_bytes')==91575<=OUTPUT_CAP
        and value.get('current_owned_drain_verified') is False
        and value.get('new_resource_admission') is False and value.get('final_dataset_admitted') is False,
        'pilot_prior_successful_root_scope')
    checked=decode(body.encode())
    require(checked.get('schema_version')=='polymarket_append_pilot_terminal_reopen_v4'
        and checked.get('status')=='terminal_bytes_identities_and_receipt_laws_verified_not_admitted'
        and checked.get('source_commit')=='ad6133e0dc0cdb0ab3c848301860c1df1764e6b9'
        and checked.get('raw_rows_producer_receipt')==23887 and checked.get('demanded_keys_producer_receipt')==200
        and checked.get('raw_files_rehashed')==4 and checked.get('selected_header_files_rehashed')==3
        and checked.get('selected_header_indices')==[271,273,328]
        and checked.get('owned_process_drain_verified') is False
        and checked.get('trade_rows_or_clock_join_replayed') is False,
        'pilot_prior_successful_checker_binding')
    return value


def admission(context,meter):
    raw=context['admission'];value=decode(raw)
    require(digest(raw)==context['admission_sha'] and len(raw)<=ADMISSION_CAP,'pilot_admission_SHA')
    fields={'schema_version','status','run_dir','live_checkout_head','operator_source_head','boot_id',
        'issued_utc','valid_until_utc','cumulative_runtime_seconds','runtime_ceiling_seconds','caps',
        'preflight','preflight_exit','volume_identity','native_accounting','source_bundle','fixture_test_sources','qa_auxiliary_sources','parent_reservation',
        'linux_QA','prior_failures','prior_pilot_format_diagnostic','v3_wire_admission','external_pool','optimized_continuation',
        'prior_successful_pilot_root'}
    require(type(value) is dict and set(value)==fields and value['schema_version']=='polymarket_append_source_clock_pilot_admission_v3'
        and value['status']=='root_prospectively_admitted_four_source_optimized_clock_pilot_v3' and value['run_dir']==str(RUN)
        and value['live_checkout_head']==HEAD and value['caps']==CAPS
        and all(type(value['caps'][k]) is type(v) for k,v in CAPS.items()),'pilot_admission_contract')
    require(type(value['operator_source_head']) is str and re.fullmatch('[0-9a-f]{40}',value['operator_source_head']),
            'pilot_committed_operator_epoch')
    now=time.time();require(0<=now-host.utc(value['issued_utc'])<=300 and now<host.utc(value['valid_until_utc']), 'pilot_admission_freshness')
    require(type(value['cumulative_runtime_seconds']) is int and value['cumulative_runtime_seconds']==225808
        and value['runtime_ceiling_seconds']==259200,'pilot_retained_cumulative_runtime')
    parent_raw=context['parent_reservation'];require(type(parent_raw) is bytes and value['parent_reservation']==PARENT
        and len(parent_raw)==PARENT['bytes'] and digest(parent_raw)==PARENT['sha256'],'pilot_parent_reservation_pin')
    parent=decode(parent_raw)
    require(parent['conservative_carry_seconds']==219808 and parent['combined_absolute_deadline_utc']==OLD_PARENT_END
        and parent['combined_preparation_reservation_seconds']==sum(parent['components_seconds'].values())==15000
        and parent['components_seconds']['new_reviewed_append_only_control_or_bounded_pilot']==1200
        and parent['all_four_failed_footer_attempt_runtime_seconds_retained']==4800
        and parent['failed_append_pilot_runtime_seconds_retained']==1200
        and parent['failed_append_pilot_RUN_and_stdout_reservation_bytes_retained']==OUTPUT_CAP
        and host.utc(value['valid_until_utc'])<=host.utc(PARENT_END),'pilot_parent_component_no_second_charge')
    require(value['optimized_continuation']==CONTINUATION,'pilot_new_continuation_reference')
    optimized_continuation(context['optimized_continuation'])
    require(value['prior_successful_pilot_root']==PRIOR_ROOT_SUCCESS,'pilot_prior_successful_root_reference')
    prior_successful_pilot_root(context['prior_successful_pilot_root'])
    require(host.utc(OLD_PARENT_END)<=now,'pilot_new_window_not_started')
    meter.deadline=min(meter.deadline,_CLOCK()+host.utc(value['valid_until_utc'])-now,
                       _CLOCK()+host.utc(PARENT_END)-now)
    meter.check()
    for name in ('preflight','preflight_exit','linux_QA'):host.bytes_binding(context[name],value[name])
    preflight=decode(context['preflight']);ended=decode(context['preflight_exit']);qa=decode(context['linux_QA'])
    require(preflight.get('schema_version')=='native_clock_readonly_host_inspection_v5'
        and preflight.get('status')=='read_only_inspection_complete_not_stage_admission'
        and preflight.get('boot_id')==value['boot_id'] and preflight.get('old_process_receipts_checked')==603
        and preflight.get('receipt_absence_relative_to_same_final_census') is True
        and 0<=now-host.utc(preflight['checked_utc_at_last_census_start'])<=300,'pilot_fresh_V5')
    require(ended.get('schema_version')=='polymarket_native_profile_preflight_exit_v1'
        and ended.get('status')=='root_verified_preflight_exited' and type(ended.get('exit_code')) is int and ended['exit_code']==0
        and ended.get('boot_id')==value['boot_id'] and ended.get('preflight_sha256')==digest(context['preflight'])
        and ended.get('own_process')==preflight.get('own_process'),'pilot_owned_V5_exit')
    require(value['volume_identity']==preflight.get('mounts'),'pilot_preflight_mount_binding')
    modules=context['modules'];require(set(modules)==set(value['source_bundle'])==set(RUNTIME_FILES),'pilot_exact_runtime_bundle')
    for name,body in modules.items():
        bound=value['source_bundle'][name];require(type(bound) is dict and set(bound)=={'bytes','sha256'}
            and type(bound['bytes']) is int and
            type(body) is bytes and 0<len(body)<=1024**2 and bound=={'bytes':len(body),'sha256':digest(body)},'pilot_source_binding')
    require(sum(len(b) for b in modules.values())<=2*1024**2,'pilot_source_total')
    tests=value['fixture_test_sources'];require(type(tests) is dict and
        set(tests)=={name.replace('.','/')+'.py' for name in QA_MODULES},'pilot_test_sources')
    for name,bound in tests.items():
        require(set(bound)=={'bytes','sha256'} and type(bound['bytes']) is int and 0<bound['bytes']<=1024**2
            and type(bound['sha256']) is str and re.fullmatch('[0-9a-f]{64}',bound['sha256']),'pilot_test_source_pin')
    auxiliary=value['qa_auxiliary_sources']
    require(type(auxiliary) is dict and set(auxiliary)==QA_AUXILIARY,'pilot_QA_auxiliary_sources')
    for bound in auxiliary.values():
        require(type(bound) is dict and set(bound)=={'bytes','sha256'} and type(bound['bytes']) is int
            and 0<bound['bytes']<=1024**2 and type(bound['sha256']) is str and re.fullmatch('[0-9a-f]{64}',bound['sha256']),
            'pilot_QA_auxiliary_pin')
    added={name.replace('tests/test_','scripts/',1):value['source_bundle'].get(name.replace('tests/test_','scripts/',1),auxiliary.get(name.replace('tests/test_','scripts/',1))) for name in tests}
    require(all(added.values()) and qa.get('schema_version')=='append_source_git_result_v3'
        and qa.get('status')=='source_commit_prepared_not_producer_admission'
        and qa.get('commit')==value['operator_source_head'] and qa.get('parent')==HEAD
        and qa.get('host_boot')==value['boot_id'] and qa.get('sources')==added|tests
        and qa.get('test_suite_executed') is True and qa.get('producer_dispatched') is False,
        'pilot_actual_Linux_QA_source_epoch')
    require(qa.get('optimized_continuation')==CONTINUATION
        and type(qa.get('additional_runtime_reservation_seconds')) is int and qa['additional_runtime_reservation_seconds']==0
        and qa.get('native_upper_after_full_reservations_bytes')==49330839510,'pilot_preparer_continuation_binding')
    linux=qa.get('Linux_QA');require(type(linux) is dict and linux.get('command_modules')==list(QA_MODULES)
        and type(linux.get('tests_run')) is int and linux['tests_run']==QA_TESTS and QA_TESTS>0
        and all(type(linux.get(k)) is int and linux[k]==0 for k in ('skipped','failures','errors'))
        and linux.get('owned_child_drained') is True,'pilot_actual_Linux_QA')
    child,absence=linux.get('child_identity'),linux.get('terminal_absence')
    require(type(child) is dict and child.get('available') is True and child.get('boot_id')==value['boot_id']
        and child.get('uid')==0 and type(child['uid']) is int and child.get('executable')=='/usr/bin/python3.12'
        and all(type(child.get(k)) is int and child[k]>0 for k in ('pid','parent_pid','process_group','session','start_ticks'))
        and child['pid']==child['process_group']==child['session'] and type(absence) is dict
        and all(absence.get(k) is True for k in ('available','pid_absent','process_group_absent','session_absent'))
        and type(absence.get('process_stat_rows_checked')) is int and absence['process_stat_rows_checked']>0,
        'pilot_Linux_QA_actual_birth_and_owned_drain')
    fixtures={name:{'bytes':v[0],'sha256':v[1]} for name,v in QA_FIXTURES.items()}
    compact=(json.dumps(fixtures,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
    require(type(qa.get('QA_fixture_file_count')) is int and qa['QA_fixture_file_count']==len(fixtures)==11
        and qa.get('QA_fixture_files_sha256')==digest(compact),'pilot_QA_fixture_bindings')
    require(set(context['prior_failures'])==set(value['prior_failures'])==set(FAILURES),'pilot_four_failures_required')
    for role,(size,sha,head) in FAILURES.items():
        raw=context['prior_failures'][role];require(type(raw) is bytes and len(raw)==size and digest(raw)==sha
            and value['prior_failures'][role]=={'bytes':size,'sha256':sha,'source_head':head},'pilot_failed_history_pin')
    require(value['prior_pilot_format_diagnostic']==DIAGNOSTIC,'pilot_original_diagnostic_reference')
    first_pilot_history(context['prior_failures']['five'],context['prior_pilot_format_diagnostic'])
    account=value['native_accounting'];require(account.get('retained_and_scratch_ceiling_bytes')==64000000000
        and type(account.get('observed_retained_and_scratch_bytes')) is int
        and 0<=account['observed_retained_and_scratch_bytes'] and account['observed_retained_and_scratch_bytes']+OUTPUT_CAP<=64000000000
        and account.get('spill_ceiling_bytes')==16000000000 and type(account.get('observed_spill_bytes')) is int
        and account['observed_spill_bytes']==0
        and account.get('new_RUN_reservation_bytes')==OUTPUT_CAP and account.get('prior_failed_profile_runtime_seconds')==4800
        and account.get('prior_failed_append_pilot_runtime_seconds')==1200
        and account.get('prior_failed_append_pilot_artifact_reservation_bytes')==OUTPUT_CAP
        and account.get('qualified_upper_with_all_reservations_bytes')==49330839510,
        'pilot_native_category_and_spent_four_reservations')
    pool=value['external_pool'];require(pool.get('ceiling_bytes')==2097152 and pool.get('ceiling_refs')==256
        and pool.get('old_future_bytes')==786432 and pool.get('old_future_refs')==9
        and type(pool.get('reserved_bytes')) is int and 2094547<=pool['reserved_bytes']<=2097152
        and type(pool.get('reserved_refs')) is int and 95<=pool['reserved_refs']<=256
        and pool.get('new_OP_refs')==0,'pilot_same_external_pool')
    contract=value['v3_wire_admission'];wire.archive.validate_admission(contract)
    require(contract['build_receipt']==BUILD_REF,'pilot_existing_build_receipt')
    host.bytes_binding(context['v3_build_receipt'],BUILD_REF)
    return value,preflight


def git_proof(meter,epochs,operator,*,final=False):
    host.git_state(meter,final=final);host.prove_git_blobs(meter,epochs,final=final)
    parent=host.git(meter,['rev-parse',operator+'^'],64,final=final).decode().strip()
    require(parent==HEAD,'pilot_operator_parent_epoch')
    raw=host.git(meter,['diff-tree','--no-commit-id','--name-status','-r',operator],16384,final=final)
    pairs=raw.decode().splitlines()
    allowed={name.replace('tests/test_','scripts/',1) if name.startswith('tests/test_') else name
             for name in (module.replace('.','/')+'.py' for module in QA_MODULES)}
    allowed|={name.replace('.','/')+'.py' for name in QA_MODULES}
    require(len(pairs)==len(allowed) and set(pairs)=={'A\t'+name for name in allowed},'pilot_ADD_only_source_diff')


def _full_hash(path,saved,meter,kind,*,final=False):
    before=host.identity(path);require(before==saved['identity'],'pilot_file_identity_changed')
    charge=meter.final_charge if final else meter.charge
    charge(kind,saved['bytes']+1);hashed=hashlib.sha256();remaining=saved['bytes']
    with open(path,'rb',buffering=0) as stream:
        while remaining:
            raw=stream.read(min(65536,remaining));require(raw,'pilot_short_final_read')
            remaining-=len(raw);hashed.update(raw)
        require(stream.read(1)==b'','pilot_source_grew')
    require(hashed.hexdigest()==saved['sha256'] and host.identity(path)==before,'pilot_final_full_SHA_changed')


def _primitive(reader,payloads,footers,publisher,source_files,deadline):
    bound={name:item['identity'] for name,item in reader.files.items()}
    bound.update({name:item['identity'] for name,item in payloads.items()})
    bound.update({name:item for name,item in source_files.items() if item is not None})
    for name,item in publisher.files.items():
        bound[str(publisher.root/name)]={**item['identity'],'bytes':item['bytes']}
    for name,saved in bound.items():
        value=_LSTAT(name)
        current={'device':value.st_dev,'inode':value.st_ino,'mode':value.st_mode,'uid':value.st_uid,
                 'gid':value.st_gid,'mtime_ns':value.st_mtime_ns,'ctime_ns':value.st_ctime_ns,'bytes':value.st_size}
        require(stat.S_ISREG(value.st_mode) and value.st_nlink==1 and current==saved,'pilot_final_primitive_binding')
        for parent in Path(name).parents:require(stat.S_ISDIR(_LSTAT(parent).st_mode),'pilot_final_ancestor_alias')
    for name in reader.absent|{name for name,item in source_files.items() if item is None}:
        try:_LSTAT(name)
        except FileNotFoundError:pass
        else:raise Blocked('pilot_observed_absence_changed')
    for name,item in footers.items():
        value=_LSTAT(name)
        require(stat.S_ISREG(value.st_mode) and value.st_nlink==1 and clocks._stat(value)==item['identity'],'pilot_footer_identity_changed')
        for parent in Path(name).parents:require(stat.S_ISDIR(_LSTAT(parent).st_mode),'pilot_final_ancestor_alias')
    require({p.name for p in _SCANDIR(publisher.root)}==set(publisher.files),'pilot_final_RUN_namespace')
    require(_CLOCK()<deadline,'pilot_final_deadline')


def _execute(context):
    require_production_host()
    require(context is _BOOTSTRAP and _BOOTSTRAP is not None,'pilot_authenticated_bootstrap_required')
    require(Path.cwd()==REPO and DATA_ROOT==Path('/mnt/data/runs') and sys.executable=='/home/ubuntu/venv/bin/python','pilot_canonical_interpreter_cwd')
    meter=Meter(context['started'],context['deadline'],context['bootstrap_bytes'])
    value,preflight=admission(context,meter)
    return _compose(context,meter,value,preflight)


def _compose(context,meter,value,preflight):
    """Private composition seam, never itself a caller authentication object."""
    process=host.process_record(meter,os.getpid())
    boot=host.proc_bytes(meter,'/proc/sys/kernel/random/boot_id',128).decode().strip()
    require(boot==value['boot_id'] and process['uid']==0 and process['full_exe']=='/usr/bin/python3.12','pilot_early_process_identity')
    early=encode({'schema_version':SCHEMA,'status':'producer_process_observed_before_host_checks',
        'process':process,'boot_id':boot,'admission_sha256':context['admission_sha'],'usage':meter.usage(),
        'footer_bytes':0,'trade_body_bytes':0})
    require(len(early)<=2048,'pilot_process_receipt_bound');sys.stdout.buffer.write(early);sys.stdout.flush()
    current=host.host_checks(meter,value,preflight)
    epochs=[{'head':value['operator_source_head'],'path':name,'bytes':len(raw),'sha256':digest(raw)} for name,raw in context['modules'].items()]
    epochs.extend({'head':value['operator_source_head'],'path':name,**ref} for name,ref in value['fixture_test_sources'].items())
    epochs.extend({'head':value['operator_source_head'],'path':name,**ref} for name,ref in value['qa_auxiliary_sources'].items())
    git_proof(meter,epochs,value['operator_source_head'])
    reader=Reader(meter);plan=inputs.prepare_plan(reader.callback);records,children=selection(plan,reader)
    headers=[{**{key:record[key] for key in ('path','bytes','sha256')},'rows':record['keys'],'source_file_index':record['chunk']}
             for record in plan['header_parquets']]
    header_controls=_header_controls(reader,headers)
    footer_began=_CLOCK()
    indices,footers=footer_hints(headers,[(r['first_block'],r['last_block']) for r in records],meter)
    footer_seconds=_CLOCK()-footer_began
    selected_headers=[r for r in headers if r['source_file_index'] in indices]
    payloads={}
    for record in [*records,*selected_headers]:
        identity=host.identity(record['path']);require(identity['bytes']==record['bytes'],'pilot_selected_length')
        payloads[record['path']]={k:record[k] for k in ('bytes','sha256')}|{'identity':identity}
    source_files={}
    branch=host.git(meter,['symbolic-ref','--quiet','HEAD'],1024).decode().strip()
    require(branch.startswith('refs/heads/') and '..' not in Path(branch).parts,'pilot_live_branch_literal')
    for name in ('production_guard.py','.git/HEAD','.git/index','.git/config','.git/packed-refs','.git/'+branch):
        path=REPO/name
        try:ident=host.identity(path)
        except FileNotFoundError:
            require(name in {'.git/packed-refs','.git/'+branch},'pilot_missing_required_git_control')
            source_files[str(path)]=None;continue
        require(ident['bytes']<=1024**2,'pilot_live_source_cap')
        meter.charge('live_source',ident['bytes']+1)
        with path.open('rb',buffering=0) as stream:raw=stream.read(ident['bytes']+1)
        reader.files[str(path)]={'identity':ident,'bytes':len(raw),'sha256':digest(raw)};source_files[str(path)]=ident
    final_proof=2*current['process_count']*6150+100000
    final_git=sum(e['bytes']+1 for e in epochs)+256*1024+66+65+16385
    footer_bytes=sum(sum(size for _,size,_ in f['parts']) for f in footers.values())
    final_reserve=sum(r['bytes']+1 for r in reader.files.values())+sum(r['bytes']+1 for r in payloads.values())+footer_bytes+final_git+final_proof+2*OUTPUT_CAP
    # Full hash + conservative two Arrow-file reads; final hash is reserved
    # separately. Fixed selection fails before any raw/header body read.
    bodies=3*sum(r['bytes'] for r in [*records,*selected_headers])+len(records+selected_headers)*65536
    require(sum(meter.reads.values())+final_reserve+bodies<=READ_LIMIT,'pilot_actual_read_forecast_does_not_fit_before_body')
    meter.reserve(final_reserve)
    publisher=host.Publisher(RUN,meter)
    publisher.publish('START.json',{'schema_version':SCHEMA,'status':'four_source_pilot_started',
        'admission_sha256':context['admission_sha'],'selected_raw':records,'selected_header_indices':indices,
        'seed_control_pins':inputs.ANCHORS,'prior_failures':value['prior_failures'],
        'source_head':value['operator_source_head'],'boot_id':value['boot_id'],
        'optimized_continuation':{**CONTINUATION,'raw_UTF8':context['optimized_continuation'].decode()},
        'prior_successful_pilot_root':{**PRIOR_ROOT_SUCCESS,'raw_UTF8':context['prior_successful_pilot_root'].decode()},
        'forecast_total_read_bytes':sum(meter.reads.values())+final_reserve+bodies,
        'full_dataset_admitted':False})
    result=_project_pilot(records,headers,indices,meter,value['v3_wire_admission'],header_controls)
    require(result['raw_rows']==23887 and result['demanded_block_keys']==result['matched_block_keys']==200,
            'pilot_fixed_actual_source_measurement_counts')
    result['header_footer_routing_seconds']=footer_seconds
    publisher.publish('profile.json',{'schema_version':SCHEMA,'status':'four_source_clock_pilot_verified',
        'admission_sha256':context['admission_sha'],'summary':result,'usage':meter.usage(),
        'unselected_header_payloads_not_newly_verified':330-len(indices),'full_dataset_admitted':False})
    inline={k:deepcopy(context[k]) for k in ('admission','preflight','preflight_exit','linux_QA','prior_failures','prior_pilot_format_diagnostic','v3_build_receipt','modules','parent_reservation','optimized_continuation','prior_successful_pilot_root')}
    def final_check():
        git_proof(meter,epochs,value['operator_source_head'],final=True)
        meter.reserved_final_reads-=final_proof;require(meter.reserved_final_reads>=0,'pilot_final_proof_reserve')
        host.host_checks(meter,value,preflight,maximum_processes=current['process_count'])
        reader.full_reopen()
        for name,saved in payloads.items():_full_hash(name,saved,meter,'payload_final',final=True)
        for name,capture in footers.items():
            for offset,size,sha in capture['parts']:
                meter.final_charge('footer_hint_final',size)
                with open(name,'rb',buffering=0) as stream:stream.seek(offset);raw=stream.read(size)
                require(len(raw)==size and digest(raw)==sha,'pilot_footer_hint_final_changed')
    def primitive():
        host.current_census(meter,preflight,maximum_processes=current['process_count'])
        require(all(context[k]==v for k,v in inline.items()),'pilot_inline_context_changed')
        pending=publisher.files['.COMPLETE.json.pending']
        context['stdout']=encode({'schema_version':SCHEMA,'status':'four_source_clock_pilot_verified_not_full_dataset',
            'COMPLETE':{k:pending[k] for k in ('path','bytes','sha256')},'admission_sha256':context['admission_sha'],
            'raw_files':4,'raw_rows':result['raw_rows'],'usage':meter.usage(),
            'root_independent_reopen_directory_sync_and_owned_drain_required':True})
        require(getattr(sys.stdout,'used',0)+len(context['stdout'])<=STDOUT_CAP,'pilot_stdout_budget')
        _primitive(reader,payloads,footers,publisher,source_files,meter.deadline)
    publisher.publish('COMPLETE.json',{'schema_version':SCHEMA,'status':'four_source_clock_pilot_verified_not_full_dataset',
        'admission_sha256':context['admission_sha'],'raw_files':4,'raw_rows':result['raw_rows'],
        'demanded_block_keys':result['demanded_block_keys'],'profile':{k:publisher.files['profile.json'][k] for k in ('path','bytes','sha256')},
        'final_dataset_admitted':False,'root_independent_reopen_directory_sync_and_owned_drain_required':True},
        final_check=final_check,final_primitive=primitive)
    return context['stdout']


def run_production():
    require_production_host()
    require(_BOOTSTRAP is not None,'pilot_authenticated_stdin_missing_before_IO')
    return _execute(_BOOTSTRAP)


def stdin_wrapper(driver_sha,envelope_sha,admission_sha):
    for value in (driver_sha,envelope_sha,admission_sha):require(type(value) is str and re.fullmatch('[0-9a-f]{64}',value),'pilot_bootstrap_pin')
    return '''import os,sys,time,signal,resource
began=time.monotonic()
old=signal.getitimer(signal.ITIMER_REAL)
if time.time()<1793406990:raise RuntimeError('pilot_new_window_not_started')
seconds=min(1200,1793411790-time.time(),old[0] if old[0]>0 else 1200)
if seconds<=0:raise RuntimeError('pilot_parent_reservation_expired')
signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(RuntimeError('pilot_outer_deadline')))
signal.setitimer(signal.ITIMER_REAL,seconds)
resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3))
sys.dont_write_bytecode=True
os.environ['PYTHONDONTWRITEBYTECODE']='1'
import hashlib,json,importlib.abc,importlib.util
class Output:
 def __init__(self,original):self.original=original;self.used=0;self.buffer=self
 def write(self,value):
  value=value.encode() if isinstance(value,str) else value
  self.used+=len(value)
  if self.used>65536:raise RuntimeError('pilot_stdout_limit')
  return self.original.buffer.write(value)
 def flush(self):return self.original.flush()
sys.stdout=Output(sys.stdout)
def check(ok,code):
 if not ok:raise RuntimeError(code)
def unique(pairs):
 result={}
 for key,value in pairs:
  check(key not in result,'pilot_duplicate_bootstrap_key');result[key]=value
 return result
raw=sys.stdin.buffer.read(8*1024**2+1)
check(len(raw)<=8*1024**2 and hashlib.sha256(raw).hexdigest()==ENVELOPE,'pilot_envelope_SHA')
obj=json.loads(raw,object_pairs_hook=unique)
check(set(obj)=={'admission_utf8','preflight_utf8','preflight_exit_utf8','linux_QA_utf8','prior_failures_utf8','prior_pilot_format_diagnostic_utf8','v3_build_receipt_utf8','parent_reservation_utf8','optimized_continuation_utf8','prior_successful_pilot_root_utf8','modules'},'pilot_bootstrap_fields')
check(set(obj['modules'])==set(MODULES) and all(type(v) is str for v in obj['modules'].values()),'pilot_bootstrap_modules')
modules={k:v.encode() for k,v in obj['modules'].items()}
check(hashlib.sha256(modules[DRIVER_PATH]).hexdigest()==DRIVER,'pilot_driver_SHA')
check(hashlib.sha256(obj['admission_utf8'].encode()).hexdigest()==ADMISSION,'pilot_admission_SHA')
declared=json.loads(obj['admission_utf8'],object_pairs_hook=unique)
check(type(declared.get('source_bundle')) is dict and set(declared['source_bundle'])==set(modules),'pilot_bootstrap_source_roster')
for name,body in modules.items():
 check(declared['source_bundle'][name]=={'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()},'pilot_bootstrap_source_binding_before_import')
class Loader(importlib.abc.MetaPathFinder,importlib.abc.Loader):
 def find_spec(self,name,path=None,target=None):
  if name.replace('.','/')+'.py' in modules:return importlib.util.spec_from_loader(name,self)
  if name in {'scripts','pipeline'}:return importlib.util.spec_from_loader(name,self,is_package=True)
  if name=='production_guard' or name.startswith(('scripts.','pipeline.')):raise ImportError('pilot_unbundled_project_import')
 def create_module(self,spec):return None
 def exec_module(self,module):
  if module.__name__ in {'scripts','pipeline'}:module.__path__=[];return
  name=module.__name__.replace('.','/')+'.py';module.__file__='/home/ubuntu/prediction_markets/'+name
  exec(compile(modules[name],module.__file__,'exec'),module.__dict__)
sys.meta_path.insert(0,Loader())
from scripts import run_polymarket_append_source_pilot_v3 as driver
driver._BOOTSTRAP={'started':began,'deadline':began+seconds,'bootstrap_bytes':len(raw)+1,'modules':modules,
 'admission':obj['admission_utf8'].encode(),'admission_sha':ADMISSION,'preflight':obj['preflight_utf8'].encode(),
 'preflight_exit':obj['preflight_exit_utf8'].encode(),'linux_QA':obj['linux_QA_utf8'].encode(),
 'parent_reservation':obj['parent_reservation_utf8'].encode(),
 'optimized_continuation':obj['optimized_continuation_utf8'].encode(),
 'prior_successful_pilot_root':obj['prior_successful_pilot_root_utf8'].encode(),
 'prior_pilot_format_diagnostic':obj['prior_pilot_format_diagnostic_utf8'].encode(),
 'v3_build_receipt':obj['v3_build_receipt_utf8'].encode(),'prior_failures':{k:v.encode() for k,v in obj['prior_failures_utf8'].items()}}
try:result=driver.run_production()
except Exception as error:
 code=str(error) if type(error).__module__.startswith(('scripts.','production_guard')) else type(error).__name__
 sys.stdout.write(json.dumps({'schema_version':'polymarket_append_source_clock_pilot_v3','status':'pilot_withheld','error_code':code[:160]})+'\\n');sys.stdout.flush();raise SystemExit(2)
sys.stdout.original.buffer.write(result);sys.stdout.original.flush()
'''.replace('ENVELOPE',repr(envelope_sha)).replace('DRIVER_PATH',repr(DRIVER_PATH)).replace('DRIVER',repr(driver_sha)).replace('ADMISSION',repr(admission_sha)).replace('MODULES',repr(sorted(RUNTIME_FILES)))


if __name__=='__main__':run_production()


