"""Root-admitted, one-worker native-source FOOTER profile, no trade pages.

Production is entered only by the authenticated stdin bootstrap below.  The
bootstrap pins the entire admission and module envelope, arms the earlier hard
deadline before stdin/imports, and installs reviewed modules without helper
copies.  The existing lineage and footer implementations are not modified.
This profile does NOT produce the row clock layer or admit a research sample.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import math
import os
from pathlib import Path
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time

from production_guard import require_production_host
from scripts import replay_polymarket_native_profile_lineage_v1 as replay

profile, preparation = replay.profile, replay.preparation
proof, roster, core = profile.proof, profile.roster, profile.core
Blocked, require = profile.Blocked, profile.require
encode, decode, digest = profile.encode, profile.decode, profile.digest
REPO = Path('/home/ubuntu/prediction_markets')
DATA = Path('/mnt/data')
RUN = DATA/'runs/2026-10-30_polymarket_native_clock_source_profile_v1'
OP = Path(proof.OP)
HEAD = proof.HEADER_HEAD
READ_LIMIT, FOOTER_LIMIT = 2000000000, 1000000000
AS_LIMIT, OUTPUT_CAP, STDOUT_CAP = 8*1024**3, 1024**2, 64*1024
FILE_OUTPUT_CAP = OUTPUT_CAP-STDOUT_CAP
FREE_FLOOR, RUNTIME = 20000000000, 1200
OLD_DEVICE = 66309
CONTROL_COUNT, CONTROL_BYTES = 1766, 569642935
SOURCE_FILES, PHYSICAL_ROWS = 131356, 2015058879
CONTROL_SHA = replay.ACTUAL_CONTROL_PROJECTION
QA_HEAD = '8542073491092e1acf35f83bca4639596002d494'
QA_ROSTER_SHA = '719989bc8dce47db129a782ca86cd1e03fbd7e645f5c8a3ac64274eaf5af4682'
QA_REF = {'path':str(OP/'full_header_v8_linux_fixtures_v1.json'),'bytes':7309,
          'sha256':'a014633a765785f2ed737ac06dc5be8636e5cb5f8d9789e18206bf7576eaa9b0'}
QA_LOG = {'path':str(OP/'full_header_v8_linux_fixtures_v1.log'),'bytes':1176,
          'sha256':'9df14d7cae4c3561cf5c0b79fe356dd9df0ecaaa4eaa744c8700d0607579c6e1'}
_CLOCK, _LSTAT, _SCANDIR = time.monotonic, os.lstat, os.scandir
_BOOTSTRAP = None


def integer(value, code, minimum=0):
    require(type(value) is int and value >= minimum, code)
    return value


def relative(name):
    require(type(name) is str and name and not Path(name).is_absolute()
            and str(Path(name)) == name and '..' not in Path(name).parts,
            'native_profile_source_relative_path')
    return name


def utc(value):
    require(type(value) is str, 'native_profile_UTC_schema')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0,
            'native_profile_UTC_timezone')
    return result.timestamp()


def bytes_binding(raw, ref):
    bound = proof.reference(ref)
    require(type(raw) is bytes and len(raw) == bound['bytes']
            and digest(raw) == bound['sha256'], 'native_profile_supplied_bytes_binding')
    return bound


def control_reference(ref):
    """Sealed empty logs are valid controls, NOT a refunded reserved slot."""
    if type(ref) is dict and ref.get('bytes')==0:
        require(set(ref)=={'path','bytes','sha256'} and type(ref['bytes']) is int
                and type(ref['path']) is str and Path(ref['path']).is_absolute()
                and str(Path(ref['path']))==ref['path'] and '..' not in Path(ref['path']).parts
                and Path(ref['path']).suffix in {'.log','.jsonl'}
                and ref['sha256']==digest(b''),'native_profile_empty_operational_control_ref')
        return deepcopy(ref)
    return proof.reference(ref)


class Meter:
    """Shared reserve-before-I/O ledger, compatible with accepted Publisher."""
    def __init__(self, started, deadline, bootstrap_bytes):
        require(type(started) in (int,float) and type(deadline) in (int,float)
                and math.isfinite(started) and math.isfinite(deadline)
                and started <= _CLOCK() < deadline <= started+RUNTIME,
                'native_profile_outer_deadline')
        self.started, self.deadline, self.clock = started, deadline, _CLOCK
        self.reads = Counter(); self.reserved_final_reads = 0
        self.footer_ceiling = FOOTER_LIMIT
        self.charge('bootstrap', integer(bootstrap_bytes,'native_profile_bootstrap_bytes',1))

    def check(self):
        require(_CLOCK() < self.deadline, 'native_profile_hard_deadline')
        require(shutil.disk_usage(DATA).free >= FREE_FLOOR, 'native_profile_free_floor')

    def charge(self, kind, size):
        self.check(); integer(size,'native_profile_read_charge')
        require(sum(self.reads.values())+size+self.reserved_final_reads <= READ_LIMIT,
                'native_profile_shared_2GB_read_limit')
        require(kind != 'source_footer' or self.reads[kind]+size <= FOOTER_LIMIT,
                'native_profile_1GB_footer_subcap')
        self.reads[kind] += size

    def reserve(self, size):
        integer(size,'native_profile_final_reserve')
        require(sum(self.reads.values())+size <= READ_LIMIT,
                'native_profile_complete_read_plan_does_not_fit')
        self.reserved_final_reads = size

    def final_charge(self, kind, size):
        require(self.reserved_final_reads >= size, 'native_profile_final_reserve_underflow')
        self.reserved_final_reads -= size; self.charge(kind,size)

    def usage(self, *, final=False):
        return {'metadata_bytes_charged':sum(self.reads.values()),
                'bytes_by_kind':dict(self.reads),
                'planned_final_reads_remaining':self.reserved_final_reads,
                'elapsed_inclusive_seconds':_CLOCK()-self.started,
                'read_limit_bytes':READ_LIMIT,'footer_subcap_bytes':FOOTER_LIMIT,
                'raw_trade_body_bytes_read':0,'source_payload_full_hash_bytes_read':0}


def literal_parents(path):
    for node in Path(path).parents:
        require(stat.S_ISDIR(_LSTAT(node).st_mode), 'native_profile_literal_ancestor')


def identity(path):
    path = Path(path); literal_parents(path)
    value = _LSTAT(path)
    require(stat.S_ISREG(value.st_mode) and value.st_nlink == 1,
            'native_profile_regular_single_link')
    return {**{key:getattr(value,{'device':'st_dev','inode':'st_ino','mode':'st_mode',
          'uid':'st_uid','gid':'st_gid','mtime_ns':'st_mtime_ns','ctime_ns':'st_ctime_ns'}[key])
          for key in roster.STAT_FIELDS}, 'bytes':value.st_size}


class Reader:
    """Actual fixed producer paths, sealed refs; no fixture/path discovery API."""
    def __init__(self, meter, historical_manifest_path):
        self.meter, self.historical_manifest_path = meter, historical_manifest_path
        self.controls, self.cache, self.requests = {}, {}, Counter()
        self.phase = 'initial'; self.frozen_paths = None
        self.header_refs = {}

    def register_header_ref(self,ref):
        ref=control_reference(ref);path=Path(ref['path'])
        require((DATA in path.parents and path.suffix in {'.json','.jsonl','.log','.py','.md'})
                or (REPO in path.parents and path.suffix in {'.py','.md'}),
                'native_profile_header_control_not_payload_or_authority')
        require(self.header_refs.setdefault(str(path),ref)==ref,
                'native_profile_conflicting_header_control_reference')

    def cap(self,path):
        if path == self.historical_manifest_path:return 128*1024**2
        if path == proof.KNOWN_ROSTER['path']:return 64*1024**2
        if path.endswith('/source_audit.parquet'):
            require(path == str(Path(proof.KNOWN_ROSTER['path']).parent.parent/
                    '2026-10-27_polymarket_exact_header_key_inventory_v1/source_audit.parquet'),
                    'native_profile_only_exact_generated_audit')
            return 128*1024**2
        return 8*1024**2

    def raw(self, ref, *, limit=None):
        ref = control_reference(ref); path = Path(ref['path'])
        registered=self.header_refs.get(str(path))
        require((DATA in path.parents and path.suffix in {'.json','.jsonl','.log','.parquet'})
                or registered==ref,
                'native_profile_control_path_type')
        require(path.suffix != '.parquet' or path.name == 'source_audit.parquet',
                'native_profile_no_raw_or_key_body')
        cap = self.cap(str(path))
        if limit is not None:cap = min(cap,integer(limit,'native_profile_control_limit',1))
        require(ref['bytes'] <= cap, 'native_profile_control_role_cap')
        if self.frozen_paths is not None:require(str(path) in self.frozen_paths,'native_profile_late_control')
        current = identity(path); require(current['bytes'] == ref['bytes'],'native_profile_control_size')
        saved = {**ref,'identity':{key:current[key] for key in roster.STAT_FIELDS}}
        require(self.controls.setdefault(str(path),saved) == saved,'native_profile_control_binding_conflict')
        self.requests[(self.phase,str(path))] += 1
        if str(path) in self.cache:return self.cache[str(path)]
        n = ref['bytes']+1
        if self.phase == 'final':self.meter.final_charge('control_reopen',n)
        else:self.meter.charge('control_initial',n)
        with path.open('rb',buffering=0) as stream:raw = stream.read(n)
        require(len(raw) == ref['bytes'] and digest(raw) == ref['sha256']
                and identity(path) == current,'native_profile_control_bytes_or_identity')
        self.cache[str(path)] = raw
        return raw

    def json(self,ref,*,limit=None):return decode(self.raw(ref,limit=limit))

    def observed(self,path,*,limit=1024**2):
        path = Path(path)
        auxiliary={str(Path(proof.KNOWN_ROSTER['path']).parent/'START.json'),
                   str(Path(proof.KNOWN_ROSTER['path']).parent.parent/
                     '2026-10-27_polymarket_exact_header_key_inventory_v1/START.json'),
                   str(Path(proof.KNOWN_ROSTER['path']).parent.parent/
                     '2026-10-27_polymarket_exact_header_key_inventory_v1/PREFLIGHT.json')}
        require((path.name in {'OWNER.json','COMPLETE.json'} or str(path) in auxiliary) and DATA in path.parents,
                'native_profile_only_fixed_child_observation')
        current = identity(path); require(current['bytes'] <= limit,'native_profile_observed_control_cap')
        name = str(path)
        if name in self.controls:
            ref = {key:self.controls[name][key] for key in ('path','bytes','sha256')}
            self.raw(ref,limit=limit); return ref
        require(self.frozen_paths is None,'native_profile_late_observed_control')
        self.meter.charge('control_initial',current['bytes']+1)
        with path.open('rb',buffering=0) as stream:raw = stream.read(current['bytes']+1)
        require(len(raw) == current['bytes'] and identity(path) == current,'native_profile_observed_changed')
        ref = {'path':name,'bytes':len(raw),'sha256':digest(raw)}
        self.controls[name] = {**ref,'identity':{key:current[key] for key in roster.STAT_FIELDS}}
        self.cache[name] = raw
        return ref

    def derived(self,path,sha256):
        return self.json({'path':str(path),'bytes':identity(path)['bytes'],'sha256':sha256})

    def reopen(self):
        self.phase = 'final'; self.cache.clear()
        for name in sorted(self.controls):
            self.raw({key:self.controls[name][key] for key in ('path','bytes','sha256')})


def original_projection(controls,current_device):
    projected=[]
    for item in controls:
        item=deepcopy(item)
        require(item['identity']['device'] == current_device,'native_profile_control_not_on_data_volume')
        item['identity']['device']=OLD_DEVICE;projected.append(item)
    require(len(projected)==CONTROL_COUNT and sum(item['bytes'] for item in projected)==CONTROL_BYTES
            and replay.control_projection_digest(projected)==CONTROL_SHA,
            'native_profile_original_1766_control_commitment')
    return {'controls':CONTROL_COUNT,'unique_bytes':CONTROL_BYTES,'old_identity_projection_sha256':CONTROL_SHA,
            'current_identity_projection_sha256':replay.control_projection_digest(controls),
            'device_only_projection':{'old':OLD_DEVICE,'current':current_device}}


def project_sources(entries,current_device):
    result=[]
    for entry in entries:
        entry=core.source_binding(entry);current=identity(entry['path'])
        require(entry['identity']['device']==OLD_DEVICE and current['device']==current_device
                and current['bytes']==entry['bytes']
                and all(current[key]==value for key,value in entry['identity'].items() if key!='device'),
                'native_profile_source_not_device_only')
        result.append({**entry,'identity':{key:current[key] for key in roster.STAT_FIELDS}})
    return result


def command(meter,args,cap,*,final=False):
    """Bound stdout BEFORE spawn; no shell, temp file, stderr or refund."""
    integer(cap,'native_profile_command_stdout_cap',1)
    if final:meter.final_charge('git_or_proof_final',cap+1)
    else:meter.charge('git_or_proof',cap+1)
    process=subprocess.Popen(args,cwd=REPO,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0',
                             'GIT_CONFIG_NOSYSTEM':'1','PYTHONDONTWRITEBYTECODE':'1'})
    try:
        raw=process.stdout.read(cap+1)
        require(len(raw)<=cap,'native_profile_command_output_bound')
        code=process.wait(timeout=max(0.001,meter.deadline-_CLOCK()))
        require(code==0,'native_profile_command_failed')
        return raw
    finally:
        if process.poll() is None:process.kill();process.wait()
        process.stdout.close()


def git(meter,args,cap,*,final=False):
    return command(meter,['git','--no-optional-locks','-c','core.fsmonitor=false',
                         '-c','safe.directory='+str(REPO),*args],cap,final=final)


def git_state(meter,*,final=False):
    require(git(meter,['rev-parse','HEAD'],64,final=final).decode().strip()==HEAD,
            'native_profile_live_checkout_HEAD')
    require(git(meter,['status','--porcelain=v1','--untracked-files=all'],256*1024,final=final)==b'',
            'native_profile_live_checkout_not_clean')


def bind_header_closure(reader,terminal_raw,header_admission_raw):
    """Reuse terminalV2, reopen its finite operational/control closure only.

    Accepted terminal hashes remain the authority for completed 330 header
    chunks. This does not replay their header rows, journals, old baseline
    Parquet, or the private endpoint. Named certificate/control links are
    followed; generic arbitrary JSON references are deliberately NOT followed.
    Every selected raw control is captured by this invocation's same Reader.
    """
    terminal=proof.header_terminal_adapter(terminal_raw,proof.HEADER_TERMINAL)
    direct=terminal['direct_bindings'];selected={};queue=[];parsed={};sources=[]
    def add(ref):
        if type(ref) is dict and {'path','bytes','sha256'}<=set(ref):
            ref={key:ref[key] for key in ('path','bytes','sha256')}
        ref=control_reference(ref);old=selected.get(ref['path'])
        require(old is None or old==ref,'native_profile_header_closure_conflicting_reference')
        if old is not None:return
        require(len(selected)<512,'native_profile_header_control_closure_count_bound')
        reader.register_header_ref(ref);selected[ref['path']]=ref;queue.append(ref)
    for ref in direct.values():add(ref)
    add(proof.HEADER_TERMINAL)
    dispatch=reader.json(direct['dispatch']);header_admission=decode(header_admission_raw)
    require(bytes_binding(header_admission_raw,direct['admission'])==direct['admission'],
            'native_profile_header_admission_direct_binding')
    require(dispatch.get('schema_version')=='polymarket_header_v8_root_dispatch_v1'
            and dispatch.get('status')=='caller_and_producer_dispatched_owned_births_verified'
            and dispatch.get('source_head')==HEAD and dispatch.get('admission')==direct['admission']
            and dispatch.get('ownership_unknown') is False,'native_profile_header_dispatch_contract')
    ledger_ref=proof.reference(dispatch['external_ledger']);ledger=reader.json(ledger_ref)
    require(type(ledger) is dict and set(ledger)=={'schema_version','files','future_reserved_bytes',
            'future_reserved_files','ceiling_bytes'} and
            ledger['schema_version']=='polymarket_header_v8_external_control_ledger_v1'
            and type(ledger['files']) is list and len(ledger['files'])==78
            and ledger['future_reserved_bytes']==786432 and ledger['future_reserved_files']==9
            and ledger['ceiling_bytes']==2097152,'native_profile_header_external_ledger_contract')
    flat=[control_reference(ref) for ref in [*ledger['files'],ledger_ref]]
    require(len({ref['path'] for ref in flat})==79
            and sum(ref['bytes'] for ref in flat)+786432<=2097152,
            'native_profile_header_79_ref_union_or_pool')
    for ref in flat:add(ref)
    for key in ('preparation_check','start','launch'):add(dispatch[key])
    exited=reader.json(direct['caller_exit'])
    require(all(type(exited.get(key)) is int and exited[key]>=0
                for key in ('stdout_bytes_observed','stdout_prefix_bytes_retained'))
            and exited['stdout_prefix_bytes_retained']<=512*1024
            and exited['stdout_bytes_observed']>=exited['stdout_prefix_bytes_retained']
            and type(exited.get('stdout_truncated')) is bool
            and exited.get('stdout_eof_observed') is True,'native_profile_header_stdout_counts')
    require(exited['stdout_truncated'] is (exited['stdout_bytes_observed']>exited['stdout_prefix_bytes_retained'])
            and (not exited['stdout_truncated'] or exited['stdout_prefix_bytes_retained']==512*1024)
            and (exited['stdout_truncated'] or exited.get('stdout_sha256')==exited.get('stdout_prefix_sha256')),
            'native_profile_header_stdout_qualification')
    add({'path':str(OP/'full_header_v8.log'),'bytes':exited['stdout_prefix_bytes_retained'],
         'sha256':exited['stdout_prefix_sha256']})
    add(header_admission['reboot_equivalence'])
    add(QA_REF);add(QA_LOG)
    checked=reader.json(dispatch['preparation_check'])
    require(checked.get('schema_version')=='polymarket_header_v8_preparation_check_v1'
            and checked.get('status')=='header_only_admission_prepared_not_launched'
            and checked.get('admission')==direct['admission']
            and checked.get('external_ledger')==ledger_ref
            and checked.get('source',{}).get('head')==HEAD
            and type(checked.get('source',{}).get('files')) is dict
            and len(checked['source']['files'])==43
            and checked.get('prior_rpc')==header_admission['prior']['rpc']
            and checked.get('next_chunk')==102 and checked.get('next_attempt')==2
            and checked.get('reused_keys')==10200000,
            'native_profile_header_preparation_check')
    sources.append(checked['source'])
    prior_categories={}
    def category_files(items):
        require(type(items) is list,'native_profile_prior_header_category_files')
        for item in items:
            require(type(item) is dict and {'path','bytes','sha256'}<=set(item),
                    'native_profile_prior_header_category_ref')
            ref=control_reference({key:item[key] for key in ('path','bytes','sha256')})
            require(prior_categories.setdefault(ref['path'],ref)==ref,
                    'native_profile_prior_header_category_conflict')
    category_files(header_admission['prior']['base']['files'])
    index=0
    while index<len(queue):
        ref=queue[index];index+=1;raw=reader.raw(ref)
        if not ref['path'].endswith('.json'):continue
        value=decode(raw)
        if type(value) is list:
            require(ref['path']==str(OP/'header_pilot_keys_v1.json') and len(value)==1000,
                    'native_profile_only_original_header_request_array')
            continue
        require(type(value) is dict,'native_profile_header_control_JSON_object')
        parsed[ref['path']]=value;schema=value.get('schema_version','')
        if type(schema) is str and 'external_control_ledger' in schema:
            require(set(value)=={'schema_version','files','future_reserved_bytes','future_reserved_files','ceiling_bytes'}
                    and schema in {'polymarket_header_v6_external_control_ledger_v1',
                                   'polymarket_header_v7_external_control_ledger_v1',
                                   'polymarket_header_v8_external_control_ledger_v1'}
                    and type(value['files']) is list and len(value['files'])<=256
                    and type(value['future_reserved_bytes']) is int and value['future_reserved_bytes']>=0
                    and type(value['future_reserved_files']) is int and value['future_reserved_files']==9
                    and value['ceiling_bytes']==2097152,'native_profile_nested_header_ledger_schema')
            children=[control_reference(item) for item in value['files']]
            require(len({item['path'] for item in children})==len(children)
                    and sum(item['bytes'] for item in children)+ref['bytes']+value['future_reserved_bytes']<=2097152,
                    'native_profile_nested_header_ledger_union_or_pool')
            if schema=='polymarket_header_v7_external_control_ledger_v1':
                require(len(children)==58 and sum(item['bytes'] for item in children)==813845
                        and value['future_reserved_bytes']==1245184,'native_profile_expired_v7_ledger')
            for child in children:add(child)
        # Fixed operational certificates and OWNER links, not data baselines.
        if type(schema) is str and ('reboot_equivalence' in schema or 'exact_header_owner' in schema):
            for key in ('previous_certificate','old_v3_owner','old_v3_drain','old_v4_owner','old_v4_drain',
                        'old_v6_owner','old_owner','old_drain','stopped_v6','expired_v7_admission',
                        'expired_v7_terminal','reboot_equivalence'):
                if key in value:add(value[key])
            request=value.get('original_pilot_request')
            if type(request) is dict and 'reference' in request:add(request['reference'])
        if type(schema) is str and ('preparation_request' in schema or 'stopped_handoff' in schema):
            for key in ('stopped_v6','host_proof','linux_qa','source_commit'):
                if key in value:add(value[key])
            for key in ('extra_prior_controls','extra_external_controls'):
                if key in value:
                    require(type(value[key]) is list,'native_profile_nested_extra_controls')
                    for child in value[key]:add(child)
            for child in value.get('refs',{}).values():add(child)
            if 'stopped_handoff' in schema:category_files(value['files'])
        if 'external_control_closure' in value:
            require(type(value['external_control_closure']) is list,'native_profile_nested_external_closure')
            for child in value['external_control_closure']:add(child)
        if type(schema) is str and 'linux_fixture_receipt' in schema:
            if 'log' in value:add(value['log'])
            if value.get('source_head')==HEAD:
                require(value.get('status')=='passed' and type(value.get('exit_code')) is int and value['exit_code']==0
                        and type(value.get('tests_skipped')) is int and value['tests_skipped']==0
                        and value.get('tested_source_files')==checked['source']['files'],
                        'native_profile_accepted_header_source_QA')
        if type(schema) is str and schema.endswith('_preparation_check_v1'):
            for key in ('external_ledger','admission'):
                if key in value:add(value[key])
    # The V7 unlaunched/failed historical admission and old QA1 remain separate
    # evidence, never reinterpreted as a successful current producer run.
    require(str(OP/'header_v7_unlaunched_terminal_v1.json') in selected
            and str(OP/'full_header_v7_root_dispatch_v1.json') in selected
            and QA_REF['path'] in selected and QA_LOG['path'] in selected,
            'native_profile_required_failed_expired_header_controls')
    for key in ('start','launch'):
        saved=parsed[dispatch[key]['path']]
        require(saved.get('stage')=='headers' and saved.get('admission',{}).get('sha256')==direct['admission']['sha256']
                and saved.get('caller')==dispatch.get('caller') and saved.get('final_sample_admitted') is False,
                'native_profile_header_caller_binding')
    require(type(parsed[direct['caller_exit']['path']].get('producer_exit_code')) is int
            and parsed[direct['caller_exit']['path']]['producer_exit_code']==0,
            'native_profile_header_caller_exit')
    # Fixed future roles remain reserved at their FULL allowances even when a
    # log is short or the historical prime marker is absent. Newly selected
    # external controls cannot silently expand the immutable shared2MiB pool.
    future={str(OP/name):cap for name,cap in (
       ('full_header_v8_admission_v1.json',192*1024),('full_header_v8_root_admission_check_v1.json',16*1024),
       ('full_header_v8.START.json',8192),('full_header_v8.LAUNCH.json',8192),('full_header_v8.EXIT.json',8192),
       ('full_header_v8.log',512*1024),('full_header_v8_root_dispatch_v1.json',8192),
       ('full_header_v8_prime_acceptance_v1.json',8192),('full_header_v8_terminal_acceptance_v1.json',8192))}
    require(sum(future.values())==786432 and len(future)==9,'native_profile_header_future_roles')
    flat_map={ref['path']:ref for ref in flat}
    fixed_run={direct[key]['path']:direct[key] for key in ('manifest','complete')}
    for path,ref in selected.items():
        if path in flat_map:require(ref==flat_map[path],'native_profile_header_flat_reference_conflict')
        elif path in future:require(ref['bytes']<=future[path],'native_profile_header_future_role_cap')
        elif path in fixed_run:require(ref==fixed_run[path],'native_profile_header_RUN_reference_conflict')
        else:require(prior_categories.get(path)==ref,'native_profile_unclassified_external_header_control')
    pool=sum(item['bytes'] for item in flat)+sum(future.values())+36864
    require(pool<=2097152 and len(flat)+len(future)+4<=256,
            'native_profile_header_same_2MiB_pool_with_new4')
    return {'references':[selected[name] for name in sorted(selected)],
            'direct_and_nested_control_files':len(selected),
            'direct_and_nested_control_bytes':sum(item['bytes'] for item in selected.values()),
            'external_flat_refs':79,'old_fixed_future_bytes':786432,'old_fixed_future_slots':9,
            'external_pool_with_full_old_future_and_new4_bytes':pool,
            'external_pool_with_full_old_future_and_new4_slots':len(flat)+len(future)+4,
            'external_pool_headroom_bytes':2097152-pool,
            'source_epochs':sources,'header_rows_or_journals_reexecuted':False,
            'private_authority_body_read':False}


def source_epochs(reader,roster_manifest,inventory_manifest,header_admission,extra_sources=()):
    # Authenticate the old failed fixture record as an EPOCH MAP, not current QA.
    qa=reader.json(QA_REF);reader.raw(QA_LOG)
    files=qa.get('tested_source_files')
    require(qa.get('schema_version')=='polymarket_header_v8_linux_fixture_receipt_v1'
            and qa.get('source_head')==QA_HEAD and qa.get('status')=='failed_closed'
            and qa.get('log')==QA_LOG and type(files) is dict and len(files)==43
            and digest(encode(sorted(files)))==QA_ROSTER_SHA,'native_profile_original_43_source_map')
    reader.json(header_admission['reboot_equivalence'])
    epochs=[]
    for source in (roster_manifest.get('source'),inventory_manifest.get('source'),
                   {'head':QA_HEAD,'files':files},*extra_sources):
        require(type(source) is dict and type(source.get('head')) is str
                and roster.HEAD.fullmatch(source['head']) and type(source.get('files')) is dict
                and source['files'],'native_profile_authenticated_epoch_source')
        for name,binding in sorted(source['files'].items()):
            relative(name)
            if type(binding) is str:
                require(roster.SHA.fullmatch(binding),'native_profile_epoch_SHA')
                epochs.append({'head':source['head'],'path':name,'sha256':binding,'bytes':None})
            else:
                require(type(binding) is dict and set(binding)=={'bytes','sha256'}
                        and roster.SHA.fullmatch(binding['sha256'])
                        and type(binding['bytes']) is int and 0<binding['bytes']<=8*1024**2,
                        'native_profile_epoch_file_schema')
                epochs.append({'head':source['head'],'path':name,**binding})
    unique={}
    for item in epochs:
        key=(item['head'],item['path'])
        if key in unique:
            old=unique[key];require(old['sha256']==item['sha256'] and
                  (old['bytes'] is None or item['bytes'] is None or old['bytes']==item['bytes']),
                  'native_profile_epoch_conflicting_blob')
            if old['bytes'] is None:old['bytes']=item['bytes']
        else:unique[key]=item
    return list(unique.values())


def prove_git_blobs(meter,epochs,*,final=False):
    for item in epochs:
        if item['bytes'] is None:
            raw=git(meter,['cat-file','-s',item['head']+':'+item['path']],64,final=final)
            try:item['bytes']=int(raw.strip())
            except ValueError:raise Blocked('native_profile_epoch_blob_size')
        require(type(item['bytes']) is int and 0<item['bytes']<=8*1024**2,
                'native_profile_epoch_blob_cap')
        raw=git(meter,['cat-file','blob',item['head']+':'+item['path']],item['bytes'],final=final)
        require(len(raw)==item['bytes'] and digest(raw)==item['sha256'],
                'native_profile_epoch_blob_not_committed_bytes')


def admission(raw,expected_sha,preflight_raw,exit_raw,resource_raw,modules,started):
    require(type(expected_sha) is str and roster.SHA.fullmatch(expected_sha)
            and digest(raw)==expected_sha and len(raw)<=8192,'native_profile_root_admission_SHA')
    value=decode(raw)
    require(value.get('schema_version')=='polymarket_native_source_profile_admission_v1'
            and value.get('status')=='root_admitted_native_footer_profile'
            and value.get('run_dir')==str(RUN) and value.get('live_checkout_head')==HEAD,
            'native_profile_root_admission_schema')
    require(type(value.get('operator_source_head')) is str and roster.HEAD.fullmatch(value['operator_source_head']),
            'native_profile_operator_source_head')
    require(value.get('caps')=={'read_bytes':READ_LIMIT,'footer_bytes':FOOTER_LIMIT,
            'address_space_bytes':AS_LIMIT,'runtime_seconds':RUNTIME,'all_new_artifact_bytes':OUTPUT_CAP,
            'minimum_free_bytes':FREE_FLOOR,'spill_bytes':0,'workers':1},'native_profile_admitted_caps')
    now=time.time();require(now<utc(value['valid_until_utc'])
            and 0<=now-utc(value['issued_utc'])<=300,'native_profile_admission_freshness')
    integer(value.get('cumulative_runtime_seconds'),'native_profile_cumulative_runtime',1)
    require(value['cumulative_runtime_seconds']<=259200 and value.get('runtime_ceiling_seconds')==259200,
            'native_profile_cumulative_runtime_ceiling')
    for name,body in (('preflight',preflight_raw),('preflight_exit',exit_raw),('historical_resource',resource_raw)):
        bytes_binding(body,value[name])
    preflight,exited=decode(preflight_raw),decode(exit_raw)
    require(preflight.get('status')=='read_only_inspection_complete_not_stage_admission'
            and preflight.get('schema_version')=='native_clock_readonly_host_inspection_v5'
            and preflight.get('boot_id')==value.get('boot_id')
            and preflight.get('old_process_receipts_checked')==603
            and preflight.get('receipt_absence_relative_to_same_final_census') is True
            and 0<=now-utc(preflight['checked_utc_at_last_census_start'])<=300,
            'native_profile_fresh_preflight_consumer_proof')
    require(exited.get('schema_version')=='polymarket_native_profile_preflight_exit_v1'
            and exited.get('status')=='root_verified_preflight_exited'
            and exited.get('exit_code')==0 and exited.get('boot_id')==value['boot_id']
            and exited.get('preflight_sha256')==digest(preflight_raw)
            and exited.get('own_process')==preflight.get('own_process'),
            'native_profile_preflight_owned_exit')
    mounts=value.get('volume_identity')
    require(type(mounts) is dict and mounts==preflight.get('mounts')
            and mounts.get('data_uuid')==preparation.DATA_UUID
            and mounts.get('root_uuid')==preparation.ROOT_UUID
            and mounts.get('data_serial')==preparation.DATA_SERIAL
            and mounts.get('data_device_bytes')==preparation.DATA_BYTES
            and mounts.get('data_fstype')=='ext4' and type(mounts.get('data_st_dev')) is int,
            'native_profile_same_volume_admission')
    account=value.get('native_accounting',{})
    for key in ('observed_retained_and_scratch_bytes','observed_spill_bytes','git_metadata_and_private_index_bytes'):
        integer(account.get(key),'native_profile_native_accounting')
    require(account.get('retained_and_scratch_ceiling_bytes')==64000000000
            and account.get('spill_ceiling_bytes')==16000000000
            and account.get('new_run_reservation_bytes')==OUTPUT_CAP
            and account['observed_retained_and_scratch_bytes']+OUTPUT_CAP<=64000000000
            and account['observed_spill_bytes']<=16000000000
            and account['git_metadata_and_private_index_bytes']<=16*1024**2
            and account.get('old_fixed_future_bytes')==786432
            and account.get('old_fixed_future_slots')==9
            and account.get('new_external_control_bytes')==36864
            and account.get('new_external_control_slots')==4,'native_profile_native_category_accounting')
    require(type(modules) is dict and type(value.get('source_bundle')) is dict
            and set(modules)==set(value['source_bundle']) and modules,
            'native_profile_authenticated_source_bundle')
    for name,body in modules.items():
        relative(name);require(type(body) is bytes,'native_profile_module_bytes')
        bound=value['source_bundle'][name]
        require(type(bound) is dict and set(bound)=={'bytes','sha256'}
                and len(body)==bound['bytes'] and digest(body)==bound['sha256'],
                'native_profile_module_admission_binding')
    require('scripts/run_polymarket_native_source_profile_v1.py' in modules
            and 'production_guard.py' in modules,'native_profile_driver_and_guard_bundle')
    require(0<=_CLOCK()-started<RUNTIME,'native_profile_bootstrap_elapsed')
    return value,preflight


def proc_bytes(meter,path,cap=65536):
    meter.charge('proc_or_mount_proof',cap+1)
    with open(path,'rb',buffering=0) as stream:raw=stream.read(cap+1)
    require(len(raw)<=cap,'native_profile_proc_read_cap')
    return raw


def process_record(meter,pid):
    def fields_of(raw):
        tail=raw[raw.rindex(b')')+2:].split()
        return {'pid':pid,'parent_pid':int(tail[1]),'group':int(tail[2]),'session':int(tail[3]),
                'birth_ticks':int(tail[19]),'flags':int(tail[6]),'state':tail[0].decode()}
    raw=proc_bytes(meter,f'/proc/{pid}/stat',1024)
    try:
        fields=fields_of(raw);state=fields['state']
        status=proc_bytes(meter,f'/proc/{pid}/status',2048)
        uids=[line for line in status.splitlines() if line.startswith(b'Uid:')]
        require(len(uids)==1,'native_profile_proc_uid');fields['uid']=int(uids[0].split()[1])
        kernel=bool(fields['flags']&0x00200000)
        if kernel:
            cmd=proc_bytes(meter,f'/proc/{pid}/cmdline',1)
            require(fields['uid']==0 and not cmd and state!='Z','native_profile_kernel_identity')
            fields['full_exe']=None
        else:
            require(state!='Z','native_profile_userspace_zombie')
            fields['full_exe']=os.readlink(f'/proc/{pid}/exe')
            require(fields['full_exe'].startswith('/') and not fields['full_exe'].endswith(' (deleted)'),
                    'native_profile_userspace_actual_executable')
        after=fields_of(proc_bytes(meter,f'/proc/{pid}/stat',1024))
        require(all(after[key]==fields[key] for key in ('pid','parent_pid','group','session','birth_ticks','flags'))
                and after['state']!='Z','native_profile_process_birth_race')
        require(os.stat(f'/proc/{pid}').st_uid==fields['uid'],'native_profile_process_UID_race')
        if not kernel:require(os.readlink(f'/proc/{pid}/exe')==fields['full_exe'],
                              'native_profile_process_executable_race')
        return fields
    except (OSError,ValueError,IndexError):raise Blocked('native_profile_process_unreadable_or_raced')


def current_census(meter,preflight,*,maximum_processes=20000):
    def pids():
        values={int(item.name) for item in os.scandir('/proc') if item.name.isdecimal()}
        require(len(values)<=min(20000,maximum_processes),'native_profile_PID_count_bound');return values
    before=pids();records={pid:process_record(meter,pid) for pid in sorted(before)}
    require(before==pids()==set(records),'native_profile_PID_set_race')
    own=[];pid=os.getpid();seen=set()
    while pid:
        require(pid in records and pid not in seen,'native_profile_owned_ancestry')
        seen.add(pid);item=records[pid];own.append(item);pid=item['parent_pid']
    expected=[('/usr/bin/python3.12',0),('/usr/bin/sudo',0),('/usr/bin/timeout',1000),
              ('/usr/sbin/sshd',1000),('/usr/sbin/sshd',0),('/usr/sbin/sshd',0),
              ('/usr/lib/systemd/systemd',0)]
    require([(item['full_exe'],item['uid']) for item in own]==expected and own[-1]['pid']==1,
            'native_profile_owned_ancestry_executable')
    prior={item['pid']:item for item in preflight['current_nonkernel_census']}
    keys=('pid','parent_pid','group','session','birth_ticks','uid','full_exe')
    for pid,item in records.items():
        if item['full_exe'] is None or pid in seen:continue
        saved=prior.get(pid)
        require(saved is not None and all(item[key]==saved[key] for key in keys)
                and saved['classification']=='current_exact_unit_package_service',
                'native_profile_new_or_changed_consumer')
    old_owned=preflight['bound_ancestry'][:-2]
    require(len(old_owned)==5 and old_owned[0]==preflight['own_process'],
            'native_profile_preflight_owned_roster')
    for old in old_owned:
        saved=records.get(old['pid'])
        require(saved is None or saved['birth_ticks']!=old['birth_ticks'],
                'native_profile_preflight_process_still_live')
    old_groups={item[key] for item in old_owned for key in ('group','session')}
    require(all(item['group'] not in old_groups and item['session'] not in old_groups
                for item in records.values()),'native_profile_preflight_group_session_still_live')
    return records,own


def host_checks(meter,value,preflight,*,maximum_processes=20000):
    require(os.geteuid()==0,'native_profile_root_only')
    boot=proc_bytes(meter,'/proc/sys/kernel/random/boot_id',128).decode().strip()
    require(boot==value['boot_id'],'native_profile_current_boot')
    mounts=decode(command(meter,['findmnt','--json','--output','TARGET,FSTYPE,UUID,SOURCE','--target','/mnt/data'],8192))
    rows=mounts.get('filesystems');require(type(rows) is list and len(rows)==1,'native_profile_data_mount_unique')
    row=rows[0];require(row.get('target')=='/mnt/data' and row.get('fstype')=='ext4'
            and row.get('uuid')==preparation.DATA_UUID,'native_profile_current_data_mount')
    root=decode(command(meter,['findmnt','--json','--output','TARGET,UUID','--target','/'],8192))
    require(root.get('filesystems')==[{'target':'/','uuid':preparation.ROOT_UUID}],
            'native_profile_current_root_volume')
    device=decode(command(meter,['lsblk','--json','--bytes','--output','SERIAL,SIZE',row['source']],8192))
    disks=device.get('blockdevices');require(type(disks) is list and len(disks)==1
            and str(disks[0].get('serial','')).replace('-','')==preparation.DATA_SERIAL.replace('-','')
            and int(disks[0].get('size',0))==preparation.DATA_BYTES,'native_profile_current_serial_size')
    require(_LSTAT(DATA).st_dev==value['volume_identity']['data_st_dev']
            and _LSTAT('/').st_dev==value['volume_identity']['root_st_dev'],
            'native_profile_current_device_numbers')
    limit=resource.getrlimit(resource.RLIMIT_AS)
    require(0<limit[0]<=AS_LIMIT and 0<limit[1]<=AS_LIMIT,'native_profile_actual_AS_limit')
    require(signal.getitimer(signal.ITIMER_REAL)[0]>0,'native_profile_outer_timer_not_armed')
    memory=proc_bytes(meter,'/proc/meminfo',65536)
    available=[int(line.split()[1])*1024 for line in memory.splitlines() if line.startswith(b'MemAvailable:')]
    require(len(available)==1 and available[0]>=AS_LIMIT,'native_profile_available_memory')
    records,own=current_census(meter,preflight,maximum_processes=maximum_processes)
    return {'boot_id':boot,'data_device':_LSTAT(DATA).st_dev,'MemAvailable_bytes':available[0],
            'data_free_bytes':shutil.disk_usage(DATA).free,'process_count':len(records)}


def aggregate(originals,currents,audit_raw,meter):
    """Same accepted aggregation, original audit versus current footer binding."""
    rows=iter(profile.audit_rows(audit_raw));roles={};schemas={};rolling=hashlib.sha256()
    for index,(original,current) in enumerate(zip(originals,currents)):
        row=next(rows,None);require(row is not None,'native_profile_missing_audit')
        expected=profile.check_audit(row,original,index)
        value=profile.profile_footer(current,meter)
        require(value['rows']==expected and value['block_hash_inventory_compressed_bytes']==row['projected_compressed_bytes'],
                'native_profile_footer_audit_difference')
        schema_id=digest(encode(value['schema']))
        saved=schemas.setdefault(schema_id,{'schema':value['schema'],'source_role':original['source_role'],'files':0})
        require(saved['schema']==value['schema'] and saved['source_role']==original['source_role'],
                'native_profile_schema_conflict');saved['files']+=1
        role=roles.setdefault(original['source_role'],{'files':0,'zero_row_files':0,'rows':0,'source_bytes':0,
              'row_groups':0,'max_row_group_rows':0,'footer_bytes_read':0,'all_field_compressed_bytes':0,
              'all_field_uncompressed_bytes':0,'leaf_columns':{}})
        role['files']+=1;role['zero_row_files']+=int(value['rows']==0)
        for key in ('rows','source_bytes','row_groups','footer_bytes_read','all_field_compressed_bytes','all_field_uncompressed_bytes'):
            role[key]+=value[key]
        role['max_row_group_rows']=max(role['max_row_group_rows'],value['max_row_group_rows'])
        for leaf in value['leaf_columns']:
            saved=role['leaf_columns'].setdefault(leaf['leaf_path'],{**deepcopy(leaf),
                  'extent_count':0,'row_group_chunk_count':0,'zero_byte_chunk_count':0,
                  'compressed_bytes':0,'uncompressed_bytes':0,'max_row_group_compressed_bytes':0,
                  'max_row_group_uncompressed_bytes':0,'encodings':[],'compression':[]})
            for key in ('extent_count','row_group_chunk_count','zero_byte_chunk_count','compressed_bytes','uncompressed_bytes'):
                saved[key]+=leaf[key]
            for key in ('max_row_group_compressed_bytes','max_row_group_uncompressed_bytes'):
                saved[key]=max(saved[key],leaf[key])
            for key in ('encodings','compression'):saved[key]=sorted(set(saved[key])|set(leaf[key]))
        rolling.update(encode({'source_index':index,'original_source_binding':original,
                              'current_source_binding':current,'profile':value}))
    require(len(originals)==len(currents) and next(rows,None) is None,'native_profile_extra_audit')
    return {'files':len(originals),'rows':sum(item['rows'] for item in roles.values()),'roles':roles,
            'literal_schema_variants':schemas,'ordered_full_profile_sha256':rolling.hexdigest(),
            'legacy_footer_identity_is_full_payload_SHA':False,'source_payload_rehashed':False,
            'trade_data_pages_read':False,'new_per_file_or_per_row_table':False}


class Publisher(profile.Publisher):
    def namespace(self):
        retained=super().namespace()
        require(retained<=FILE_OUTPUT_CAP,'native_profile_all_new_1MiB_including_stdout')
        return retained

    def publish(self,name,value,**kwargs):
        require(self.namespace()+len(encode(value))<=FILE_OUTPUT_CAP,
                'native_profile_all_new_1MiB_including_stdout')
        return super().publish(name,value,**kwargs)


def final_primitive(reader,currents,publisher,source_files,deadline):
    """Last pass: only captured lstat/scandir/monotonic, no callbacks or bodies."""
    bound={item['path']:{**item['identity'],'bytes':item['bytes']} for item in reader.controls.values()}
    bound.update({item['path']:{**item['identity'],'bytes':item['bytes']} for item in currents})
    bound.update({name:item for name,item in source_files.items() if item is not None})
    for name,item in publisher.files.items():bound[str(publisher.root/name)]={**item['identity'],'bytes':item['bytes']}
    parents=set()
    for name,saved in bound.items():
        value=_LSTAT(name)
        require(stat.S_ISREG(value.st_mode) and value.st_nlink==1,'native_profile_final_regular')
        current={'device':value.st_dev,'inode':value.st_ino,'mode':value.st_mode,'uid':value.st_uid,
                 'gid':value.st_gid,'mtime_ns':value.st_mtime_ns,'ctime_ns':value.st_ctime_ns,'bytes':value.st_size}
        require(current==saved,'native_profile_final_literal_binding_changed')
        parents.update(Path(name).parents)
    for path in parents:require(stat.S_ISDIR(_LSTAT(path).st_mode),'native_profile_final_parent_alias')
    for name,saved in source_files.items():
        if saved is None:
            try:_LSTAT(name)
            except FileNotFoundError:pass
            else:raise Blocked('native_profile_final_missing_git_control_appeared')
            for path in Path(name).parents:
                require(stat.S_ISDIR(_LSTAT(path).st_mode),'native_profile_final_missing_git_ancestor')
    # Exact compact seal namespaces; historical source dirs are a fixed literal
    # roster, not filesystem-discovered observations. Never scan raw data trees.
    for root,names in ((publisher.root,set(publisher.files)),
            (Path(proof.KNOWN_ROSTER['path']).parent,{'START.json','roster.json','manifest.json','COMPLETE.json'}),
            (Path(proof.KNOWN_ROSTER['path']).parent.parent/'2026-10-27_polymarket_exact_header_key_inventory_v1',
             {'START.json','PREFLIGHT.json','source_audit.parquet','required_keys.parquet','manifest.json','COMPLETE.json'})):
        found=set()
        with _SCANDIR(root) as iterator:
            for item in iterator:
                require(item.name in names,'native_profile_final_seal_namespace_added')
                require(stat.S_ISREG(item.stat(follow_symlinks=False).st_mode),'native_profile_final_seal_alias')
                found.add(item.name)
        require(found==names,'native_profile_final_seal_namespace_missing')
    require(_CLOCK()<deadline,'native_profile_final_deadline')


def _execute(context):
    """Private operational composition; callers cannot pass an accepted flag."""
    require_production_host()  # Before any real input path is opened.
    require(type(context) is dict and context is _BOOTSTRAP,'native_profile_authenticated_bootstrap_required')
    meter=Meter(context['started'],context['deadline'],context['bootstrap_bytes'])
    value,preflight=admission(context['admission'],context['admission_sha'],context['preflight'],
             context['preflight_exit'],context['historical_resource'],context['modules'],context['started'])
    host=host_checks(meter,value,preflight);git_state(meter)
    module_epochs=[{'head':value['operator_source_head'],'path':name,'bytes':len(body),'sha256':digest(body)}
                   for name,body in sorted(context['modules'].items())]
    prove_git_blobs(meter,module_epochs)
    historical=preparation.actual_resource(context['historical_resource'])
    refs={name:item['ref'] for name,item in historical['controls'].items()}
    # Descriptor is the only bootstrap control; exact known SHA and size before
    # choosing the one explicitly admitted >8MiB historical JSON role.
    descriptor_ref=refs['descriptor'];require(descriptor_ref['sha256']==proof.KNOWN_DESCRIPTOR['sha256'],
                                            'native_profile_known_descriptor')
    bootstrap_reader=Reader(meter,'')
    desc=bootstrap_reader.json(descriptor_ref)
    reader=bootstrap_reader;reader.historical_manifest_path=desc['historical_full']['raw_manifest']['path']
    pre=reader.json(desc['preflight'])
    legacy,native=roster.lineage(reader,desc,pre)  # UNCHANGED accepted lineage.
    reader.json(desc['header_ledger'])
    closure=original_projection(list(reader.controls.values()),host['data_device'])
    raw={name:reader.raw(ref) for name,ref in refs.items()}
    preparation.actual_small_controls(context['historical_resource'],raw)
    header_closure=bind_header_closure(reader,raw['header_terminal'],raw['header_admission'])
    listing=reader.json(proof.KNOWN_ROSTER)
    require(listing.get('schema_version')=='polymarket_exact_header_key_roster_v1',
            'native_profile_original_roster_schema')
    originals=[core.source_binding(item) for item in listing['files']]
    roster_manifest=decode(raw['roster_manifest']);inventory_manifest=decode(raw['inventory_manifest'])
    audit_ref={**inventory_manifest['source_audit'],'path':str(Path(refs['inventory_manifest']['path']).parent/'source_audit.parquet')}
    audit_raw=reader.raw(audit_ref)
    for root,names in ((Path(proof.KNOWN_ROSTER['path']).parent,('START.json',)),
            (Path(refs['inventory_manifest']['path']).parent,('START.json','PREFLIGHT.json'))):
        for name in names:reader.observed(root/name)
    conservation=replay.reconcile(legacy,native,originals,profile.audit_rows(audit_raw),roster_manifest['counts'])
    require(conservation['source_files']==SOURCE_FILES and conservation['declared_physical_rows']==PHYSICAL_ROWS
            and roster_manifest['source_files']==SOURCE_FILES and inventory_manifest['source_files']==SOURCE_FILES
            and inventory_manifest['rows_declared']==inventory_manifest['rows_scanned']==PHYSICAL_ROWS,
            'native_profile_actual_order_count_row_conservation')
    currents=project_sources(originals,host['data_device'])
    epochs=source_epochs(reader,roster_manifest,inventory_manifest,decode(raw['header_admission']),
                         header_closure['source_epochs'])
    prove_git_blobs(meter,epochs)
    # Prove the live guard and all imported bundle modules against Git objects;
    # preserve literal current checkout files separately from the new operator
    # epoch. No checkout copies, source mutations or old source_snapshot calls.
    source_files={};source_hashes={}
    for item in historical['large_input_stat_only'].values():
        bound=item['binding'];current=identity(bound['path']);old=item['identity']
        require(current['bytes']==bound['bytes'] and current['device']==host['data_device']
                and all(current[key]==val for key,val in old.items() if key!='device'),
                'native_profile_original_large_input_not_device_only')
        source_files[bound['path']]=current
    for name in ('production_guard.py',):
        current=identity(REPO/name);meter.charge('current_source',current['bytes']+1)
        with (REPO/name).open('rb',buffering=0) as stream:body=stream.read(current['bytes']+1)
        expected=next(item for item in epochs if item['head']==inventory_manifest['source']['head'] and item['path']==name)
        require(digest(body)==expected['sha256'] and identity(REPO/name)==current,'native_profile_live_guard_bytes')
        source_files[str(REPO/name)]=current
        source_hashes[str(REPO/name)]=digest(body)
    branch=git(meter,['symbolic-ref','--quiet','HEAD'],1024).decode().strip()
    relative(branch);require(branch.startswith('refs/heads/'),'native_profile_literal_git_branch')
    for name in ('.git/HEAD','.git/index','.git/config','.git/packed-refs','.git/'+branch):
        path=REPO/name
        if not path.exists():
            require(name in {'.git/index','.git/packed-refs','.git/'+branch},'native_profile_required_git_control')
            source_files[str(path)]=None
            continue
        current=identity(path);require(current['bytes']<=1024**2,'native_profile_git_control_cap')
        meter.charge('current_source',current['bytes']+1)
        with path.open('rb',buffering=0) as stream:body=stream.read(current['bytes']+1)
        require(len(body)==current['bytes'] and identity(path)==current,'native_profile_git_control_race')
        source_files[str(path)]=current;source_hashes[str(path)]=digest(body)
    reader.frozen_paths=set(reader.controls)
    footer_upper=integer(inventory_manifest['planned_reads']['footer_read_bytes'],'native_profile_footer_read_plan')
    require(footer_upper%2==0 and footer_upper//2<=FOOTER_LIMIT,'native_profile_one_footer_pass_plan')
    footer_upper//=2
    control_final=sum(item['bytes']+1 for item in reader.controls.values())
    git_final=sum(item['bytes']+1 for item in [*epochs,*module_epochs])+65+256*1024+1
    # Two final complete censuses (host check and last pre-primitive gate).
    # Per PID: two <=1024B stats, <=2048B status, <=1B kernel cmdline plus
    # EOF allowances; no userspace argv is read. Count growth blocks BEFORE I/O.
    proof_final=2*host['process_count']*4101+100000
    reserve=control_final+git_final+sum(source_files[name]['bytes']+1 for name in source_hashes)+2*OUTPUT_CAP+proof_final
    require(sum(meter.reads.values())+reserve+footer_upper<=READ_LIMIT,
            'native_profile_complete_full_closure_does_not_fit_before_footer')
    meter.reserve(reserve)
    plan={'already_charged_bytes':sum(meter.reads.values()),'final_reserve_bytes':reserve,
          'footer_upper_bytes':footer_upper,'total_upper_bytes':sum(meter.reads.values())+reserve+footer_upper,
          'remaining_upper_bytes':READ_LIMIT-sum(meter.reads.values())-reserve-footer_upper}
    publisher=Publisher(RUN,meter)
    publisher.publish('START.json',{'schema_version':profile.SCHEMA,'status':'root_admitted_footer_profile_started',
          'admission_sha256':context['admission_sha'],'boot_id':host['boot_id'],'read_plan':plan,
          'original_control_closure':closure,
          'header_control_closure':{key:item for key,item in header_closure.items() if key not in {'references','source_epochs'}},
          'native_accounting':value['native_accounting']})
    result=aggregate(originals,currents,audit_raw,meter)
    require(result['files']==SOURCE_FILES and result['rows']==PHYSICAL_ROWS
            and meter.reads['source_footer']<=footer_upper,'native_profile_footer_complete_conservation')
    publisher.publish('profile.json',{'schema_version':profile.SCHEMA,'status':'native_source_footer_profile_complete',
          'admission_sha256':context['admission_sha'],'read_plan':plan,'profile':result,
          'lineage':conservation,'control_closure':closure,'usage_before_final_reopens':meter.usage(),
          'row_clock_layer_produced':False,'final_sample_admitted':False})
    def final_check():
        # All commands/proc/resource callbacks precede the LAST full input-byte
        # pass; publisher then reopens output bytes and calls the primitive.
        git_state(meter,final=True);prove_git_blobs(meter,[*epochs,*module_epochs],final=True)
        meter.reserved_final_reads-=proof_final
        require(meter.reserved_final_reads>=0,'native_profile_proof_reserve_underflow')
        host_checks(meter,value,preflight,maximum_processes=host['process_count'])
        for name,saved in source_files.items():
            if name not in source_hashes:continue
            meter.final_charge('current_source_final',saved['bytes']+1)
            with open(name,'rb',buffering=0) as stream:body=stream.read(saved['bytes']+1)
            require(len(body)==saved['bytes'] and digest(body)==source_hashes[name] and identity(name)==saved,
                    'native_profile_live_guard_final')
        reader.reopen()
    def terminal_primitive():
        current_census(meter,preflight,maximum_processes=host['process_count'])
        pending=publisher.files['.COMPLETE.json.pending']
        context['final_producer_receipt']={'schema_version':'polymarket_native_source_profile_producer_receipt_v1',
              'status':'native_footer_profile_marker_committed',
              'COMPLETE':{key:pending[key] for key in ('path','bytes','sha256')},
              'admission_sha256':context['admission_sha'],'boot_id':host['boot_id'],
              'observed_usage_after_all_reopens_and_last_census':meter.usage(final=True),
              'all_new_artifact_ceiling_including_stdout_bytes':OUTPUT_CAP,
              'root_independent_reopen_and_directory_sync_required':True,
              'row_clock_layer_produced':False,'final_sample_admitted':False}
        context['prebuilt_terminal_stdout']=encode(context['final_producer_receipt'])
        require(getattr(sys.stdout,'used',0)+len(context['prebuilt_terminal_stdout'])<=STDOUT_CAP,
                'native_profile_precommit_stdout_cap')
        final_primitive(reader,currents,publisher,source_files,meter.deadline)
    publisher.publish('COMPLETE.json',{'schema_version':profile.SCHEMA,
          'status':'native_source_footer_profile_complete','profile':{key:publisher.files['profile.json'][key]
          for key in ('path','bytes','sha256')},'admission_sha256':context['admission_sha'],
          'files':SOURCE_FILES,'physical_rows':PHYSICAL_ROWS,'raw_trade_body_bytes_read':0,
          'source_payload_full_hash_bytes_read':0,'row_clock_layer_produced':False,
          'final_sample_admitted':False,'root_independent_reopen_and_directory_sync_required':True},
          final_check=final_check,final_primitive=terminal_primitive)
    return context['prebuilt_terminal_stdout']  # Bytes prebuilt BEFORE primitive/commit; no postcommit serialization/check.


def run_production():
    require_production_host()
    require(_BOOTSTRAP is not None,'native_profile_authenticated_stdin_admission_missing_before_IO')
    return _execute(_BOOTSTRAP)


def stdin_wrapper(driver_sha, envelope_sha, admission_sha):
    """Return stdlib bootstrap to pass to python -c under root timeout1200.

    Input is canonical JSON {admission_utf8,preflight_utf8,preflight_exit_utf8,
    historical_resource_utf8,modules:{relative source path:UTF8}}. Exact envelope
    and admission SHA literals are reviewed by root before dispatch. No new
    helper file is written. PYTHONDONTWRITEBYTECODE=1 must be set externally too.
    """
    for value in (driver_sha,envelope_sha,admission_sha):
        require(type(value) is str and roster.SHA.fullmatch(value),'native_profile_bootstrap_pin')
    return '''import os,sys,time,signal,resource
began=time.monotonic()
prior=signal.getitimer(signal.ITIMER_REAL)
seconds=min(1200,prior[0]) if prior[0]>0 else 1200
signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(RuntimeError('native_profile_outer_deadline')))
signal.setitimer(signal.ITIMER_REAL,seconds)
resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3))
sys.dont_write_bytecode=True
os.environ['PYTHONDONTWRITEBYTECODE']='1'
import hashlib,json,importlib.abc,importlib.util,types
class BoundedOutput:
 def __init__(self,original):self.original=original;self.used=0;self.buffer=self
 def write(self,value):
  body=value.encode() if isinstance(value,str) else value
  self.used+=len(body)
  if self.used>65536:raise RuntimeError('native_profile_stdout_limit')
  return self.original.buffer.write(body)
 def flush(self):return self.original.flush()
sys.stdout=BoundedOutput(sys.stdout)
raw=sys.stdin.buffer.read(4*1024**2+1)
def check(ok,code):
 if not ok:raise RuntimeError(code)
check(len(raw)<=4*1024**2 and hashlib.sha256(raw).hexdigest()==ENVELOPE_SHA,'native_profile_envelope_pin')
def unique(pairs):
 result={}
 for k,v in pairs:
  check(k not in result,'native_profile_duplicate_envelope_key');result[k]=v
 return result
obj=json.loads(raw,object_pairs_hook=unique)
check(set(obj)=={'admission_utf8','preflight_utf8','preflight_exit_utf8','historical_resource_utf8','modules'},'native_profile_envelope_schema')
modules={k:v.encode() for k,v in obj['modules'].items()}
for name in modules:
 check(name and not name.startswith('/') and '..' not in name.split('/') and name.endswith('.py'),'native_profile_module_path')
check(hashlib.sha256(modules['scripts/run_polymarket_native_source_profile_v1.py']).hexdigest()==DRIVER_SHA,'native_profile_driver_pin')
check(hashlib.sha256(obj['admission_utf8'].encode()).hexdigest()==ADMISSION_SHA,'native_profile_admission_pin')
class Loader(importlib.abc.MetaPathFinder,importlib.abc.Loader):
 def find_spec(self,fullname,path=None,target=None):
  name=fullname.replace('.','/')+'.py'
  if name in modules:return importlib.util.spec_from_loader(fullname,self)
  package=fullname.replace('.','/')+'/__init__.py'
  if package in modules:return importlib.util.spec_from_loader(fullname,self,is_package=True)
  if fullname in {'scripts','pipeline'}:return importlib.util.spec_from_loader(fullname,self,is_package=True)
  if fullname=='production_guard' or fullname.startswith(('scripts.','pipeline.')):raise ImportError('native_profile_unbundled_project_import')
 def create_module(self,spec):return None
 def exec_module(self,module):
  name=module.__name__.replace('.','/')+'.py'
  if module.__spec__.submodule_search_locations is not None:
   module.__path__=[];name=module.__name__.replace('.','/')+'/__init__.py'
   if name not in modules:return
  module.__file__='/home/ubuntu/prediction_markets/'+name
  exec(compile(modules[name],module.__file__,'exec'),module.__dict__)
sys.meta_path.insert(0,Loader())
from scripts import run_polymarket_native_source_profile_v1 as driver
driver._BOOTSTRAP={'started':began,'deadline':began+seconds,'bootstrap_bytes':len(raw)+1,
 'admission':obj['admission_utf8'].encode(),'admission_sha':ADMISSION_SHA,
 'preflight':obj['preflight_utf8'].encode(),'preflight_exit':obj['preflight_exit_utf8'].encode(),
 'historical_resource':obj['historical_resource_utf8'].encode(),'modules':modules}
try:
 result=driver.run_production()
except Exception as exc:
 code=str(exc) if type(exc).__module__.startswith(('scripts.','production_guard')) else type(exc).__name__
 output=(json.dumps({'status':'native_footer_profile_withheld','error_code':code[:160]},sort_keys=True)+'\\n').encode()
 check(len(output)<=65536,'native_profile_stdout_limit');sys.stdout.buffer.write(output);raise SystemExit(2)
sys.stdout.original.buffer.write(result)
sys.stdout.original.flush()
'''.replace('ENVELOPE_SHA',repr(envelope_sha)).replace('DRIVER_SHA',repr(driver_sha)).replace('ADMISSION_SHA',repr(admission_sha))


if __name__=='__main__':
    run_production()
