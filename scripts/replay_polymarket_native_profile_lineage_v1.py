"""Metered explicit-control reader and synthetic lineage replay, real I/O CLOSED.

The existing roster.lineage implementation is reused unchanged. This module does
not inherit its spent counters, call source_snapshot, read source payloads or
footers, run Git, stage outputs, or authenticate a real source generation.
"""
from __future__ import annotations

import argparse
import hashlib
import math
import os
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
import signal
import stat
import sys
import tempfile
import time

_CLOCK=time.monotonic
_LSTAT=os.lstat
_SCANDIR=os.scandir

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host
from scripts import prepare_polymarket_native_source_profile_v1 as preparation
from scripts import profile_polymarket_native_clock_sources_v1 as profile

roster,proof=profile.roster,profile.proof
Blocked=profile.Blocked
require,encode,decode,digest=profile.require,profile.encode,profile.decode,profile.digest
SCHEMA='polymarket_native_profile_lineage_reader_candidate_v1'
READ_LIMIT=2000000000
ACTUAL_SOURCE_FILES=131356
ACTUAL_CONTROL_FILES=1766
ACTUAL_CONTROL_PROJECTION='30a64ce5bf7979a668e06fad078cc39d55cef7df3eaf50b01ff07d80637cc359'
ROLE_CAPS={'control':8*1024**2,'historical_manifest':128*1024**2,
           'roster':64*1024**2,'generated_audit':128*1024**2}
FIXTURE_FILES=256
CHARGE_KINDS={'bootstrap','control_initial','control_reopen','git_blob','current_source',
              'source_footer','generated_output_reopen','other_proof'}
PROJECTION_CODEC='sha256_concatenated_canonical_JSON_lines_sorted_by_literal_path'


def integer(value,code,minimum=0):
    require(type(value) is int and value>=minimum,code)
    return value


def control_projection_digest(controls):
    rolling=hashlib.sha256()
    for item in sorted(controls,key=lambda item:item['path']):rolling.update(encode(item))
    return rolling.hexdigest()


class Meter:
    """One invocation ceiling; supplied earlier absolute deadline never resets."""
    def __init__(self,deadline,bootstrap_bytes,*,clock=time.monotonic):
        require(type(deadline) in (int,float) and not isinstance(deadline,bool)
                and math.isfinite(deadline),'lineage_absolute_deadline')
        self.deadline,self.clock=deadline,clock
        self.reads=Counter();self.reserved=Counter();self.active=0
        self.check();self.charge('bootstrap',integer(bootstrap_bytes,'lineage_bootstrap_bytes',1))

    def check(self):require(self.clock()<self.deadline,'lineage_caller_deadline')

    def charge(self,kind,size):
        self.check();integer(size,'lineage_read_charge')
        consume=min(size,self.reserved[kind])
        require(kind in CHARGE_KINDS and sum(self.reads.values())+size+sum(self.reserved.values())-consume<=READ_LIMIT,
                'lineage_shared_2GB_read_limit')
        self.reserved[kind]-=consume
        self.reads[kind]+=size

    def reserve(self,kind,size):
        self.check();integer(size,'lineage_read_reservation')
        require(kind in CHARGE_KINDS and sum(self.reads.values())+sum(self.reserved.values())+size<=READ_LIMIT,
                'lineage_shared_2GB_read_limit')
        self.reserved[kind]+=size

    @contextmanager
    def hard_timer(self):
        self.check();began=_CLOCK();remaining=self.deadline-self.clock()
        previous=signal.getitimer(signal.ITIMER_REAL)
        duration=min(remaining,previous[0]) if previous[0]>0 else remaining
        require(duration>0,'lineage_caller_deadline')
        handler=signal.getsignal(signal.SIGALRM)
        def expired(*_):raise Blocked('lineage_caller_deadline')
        signal.signal(signal.SIGALRM,expired)
        signal.setitimer(signal.ITIMER_REAL,duration)
        self.active+=1
        try:yield
        finally:
            self.active-=1
            signal.signal(signal.SIGALRM,handler)
            elapsed=_CLOCK()-began
            signal.setitimer(signal.ITIMER_REAL,max(0.000001,previous[0]-elapsed) if previous[0]>0 else 0,previous[1])

    def usage(self):
        return {'read_bytes':sum(self.reads.values()),'bytes_by_kind':dict(self.reads),
                'planned_read_reservations_bytes':dict(self.reserved),
                'read_limit_bytes':READ_LIMIT,'caller_absolute_deadline':self.deadline,
                'prior_historical_read_counter_imported':False}


class FixtureReader:
    """Explicit temporary controls/current identities; not a real-data reader.

Same-phase caching avoids rereading observed()+json(), never an identity check.
reopen() changes phase and physically rehashes every consumed reference exactly
once. All attempted read allowances, including EOF probes/failures, stay charged.
    """
    def __init__(self,root,declarations,files,meter):
        root=Path(root);temporary=Path(tempfile.gettempdir()).resolve()
        require(root.is_absolute() and root.resolve()==root and root!=temporary and temporary in root.parents,
                'lineage_fixture_namespace_only_no_production')
        require(type(meter) is Meter and type(declarations) is list and 0<len(declarations)<=FIXTURE_FILES
                and type(files) is list and 0<len(files)<=FIXTURE_FILES,'lineage_fixture_declarations')
        self.root,self.meter=root,meter
        self.declared={};self.declared_files=set();self.controls={};self.cache={}
        self.phase='initial';self.requests=Counter();self.physical=Counter()
        for item in declarations:
            require(type(item) is dict and set(item)=={'reference','identity','role'}
                    and item['role'] in ROLE_CAPS,'lineage_declaration_schema')
            ref=proof.reference(item['reference']);path=Path(ref['path']);role=item['role']
            require(root in path.parents and ref['bytes']<=ROLE_CAPS[role], 'lineage_role_size_or_path')
            require(path.suffix in {'.json','.jsonl'} or role=='generated_audit'
                    and path.name=='source_audit.parquet','lineage_no_raw_or_key_payload')
            require(role!='historical_manifest' or path.name=='manifest.json','lineage_large_manifest_role')
            require(role!='roster' or path.name=='roster.json','lineage_roster_role')
            identity=item['identity']
            require(type(identity) is dict and set(identity)==roster.STAT_FIELDS
                    and all(type(value) is int and value>=0 for value in identity.values())
                    and stat.S_ISREG(identity['mode']),'lineage_current_identity_schema')
            require(ref['path'] not in self.declared,'lineage_duplicate_declaration')
            self.declared[ref['path']]={'reference':ref,'identity':deepcopy(identity),'role':role}
        require(sum(item['role']=='historical_manifest' for item in self.declared.values())<=1,
                'lineage_multiple_large_historical_manifests')
        for name in files:
            require(type(name) is str and Path(name).is_absolute() and str(Path(name))==name
                    and '..' not in Path(name).parts and root in Path(name).parents
                    and name not in self.declared_files,'lineage_fixture_file_roster')
            self.declared_files.add(name)
        require(set(self.declared)<=self.declared_files,'lineage_control_not_declared_file')

    def namespace(self):
        self.meter.check();actual=set();nodes=0
        for path in self.root.rglob('*'):
            self.meter.check();nodes+=1
            require(nodes<=FIXTURE_FILES*8 and not path.is_symlink(),'lineage_fixture_namespace_bound_or_alias')
            if path.is_file():actual.add(str(path))
            else:require(path.is_dir(),'lineage_fixture_special_file')
        require(actual==self.declared_files,'lineage_fixture_namespace_changed')

    def _registered(self,ref,limit):
        self.meter.check();require(self.meter.active>0,'lineage_hard_timer_required_before_IO')
        bound=proof.reference(ref);declared=self.declared.get(bound['path'])
        require(declared is not None and declared['reference']==bound,'lineage_unregistered_or_conflicting_reference')
        if limit is not None:integer(limit,'lineage_explicit_role_limit',1)
        cap=min(ROLE_CAPS[declared['role']],limit) if limit is not None else ROLE_CAPS[declared['role']]
        require(bound['bytes']<=cap,'lineage_role_size_or_requested_limit')
        path=Path(bound['path']);current=roster.identity(path)
        require(current==declared['identity'] and path.stat().st_size==bound['bytes'],'lineage_current_identity_changed')
        return bound,declared,current

    def raw(self,reference,*,limit=None):
        bound,declared,before=self._registered(reference,limit);name=bound['path']
        self.requests[(self.phase,name)]+=1
        if name in self.cache:
            require(roster.identity(Path(name))==before,'lineage_cached_identity_changed')
            return self.cache[name]
        kind='control_reopen' if self.phase.startswith('reopen:') else 'control_initial'
        self.meter.charge(kind,bound['bytes']+1)
        self.physical[(self.phase,name)]+=1
        with Path(name).open('rb',buffering=0) as handle:raw=handle.read(bound['bytes']+1)
        require(len(raw)==bound['bytes'] and digest(raw)==bound['sha256']
                and roster.identity(Path(name))==before,'lineage_control_bytes_or_identity_changed')
        saved={**bound,'identity':deepcopy(before)}
        require(self.controls.setdefault(name,saved)==saved,'lineage_consumed_binding_changed')
        self.cache[name]=raw
        return raw

    def json(self,reference,*,limit=None):return decode(self.raw(reference,limit=limit))

    def observed(self,path,*,limit=1024**2):
        declared=self.declared.get(str(path))
        require(declared is not None,'lineage_derived_path_not_explicitly_registered')
        self.raw(declared['reference'],limit=limit)
        return deepcopy(declared['reference'])

    def derived(self,path,sha256):
        declared=self.declared.get(str(path))
        require(declared is not None and declared['reference']['sha256']==sha256,'lineage_derived_SHA_not_registered')
        return self.json(declared['reference'])

    def reopen(self):
        consumed=list(self.controls.values());self.cache.clear()
        self.meter.reserve('control_reopen',sum(item['bytes']+1 for item in consumed))
        number=int(self.phase.split(':')[1])+1 if self.phase.startswith('reopen:') else 1
        self.phase='reopen:'+str(number)
        self.namespace()
        for item in consumed:self.raw({key:item[key] for key in ('path','bytes','sha256')})
        # Namespace/stats run before a final byte phase; no cache can cross it.
        self.namespace()
        for item in consumed:
            require(roster.identity(Path(item['path']))==item['identity'],'lineage_final_control_identity_changed')
        self._primitive_finish()

    def _primitive_finish(self):
        """No injected clock or resource callbacks after the last namespace gate."""
        for path in (self.root,*self.root.parents):
            require(stat.S_ISDIR(_LSTAT(path).st_mode),'lineage_final_ancestor_not_literal')
        expected_dirs={str(parent) for name in self.declared_files for parent in Path(name).parents
                       if parent==self.root or self.root in parent.parents}
        actual=set();stack=[str(self.root)];nodes=0
        while stack:
            directory=stack.pop()
            with _SCANDIR(directory) as iterator:
                for item in iterator:
                    nodes+=1;require(nodes<=FIXTURE_FILES*8,'lineage_final_namespace_bound')
                    path=os.path.join(directory,item.name);value=item.stat(follow_symlinks=False)
                    if stat.S_ISDIR(value.st_mode):
                        require(path in expected_dirs,'lineage_final_unknown_directory');stack.append(path)
                    else:
                        require(stat.S_ISREG(value.st_mode) and value.st_nlink==1,'lineage_final_nonregular_file')
                        actual.add(path)
                        if path in self.controls:
                            current={'device':value.st_dev,'inode':value.st_ino,'mode':value.st_mode,
                                'uid':value.st_uid,'gid':value.st_gid,'mtime_ns':value.st_mtime_ns,'ctime_ns':value.st_ctime_ns}
                            saved=self.controls[path]
                            require(current==saved['identity'] and value.st_size==saved['bytes'],
                                    'lineage_final_control_binding_changed')
        require(actual==self.declared_files,'lineage_final_exact_namespace_changed')
        require(_CLOCK()<self.meter.deadline,'lineage_caller_deadline')

    def inventory(self):
        projection=[deepcopy(self.controls[name]) for name in sorted(self.controls)]
        return {'unique_controls':len(projection),'unique_control_bytes':sum(item['bytes'] for item in projection),
                'binding_projection_sha256':control_projection_digest(projection),
                'binding_projection_codec':PROJECTION_CODEC,
                'requested_reads':[{'phase':phase,'path':name,'calls':count} for (phase,name),count in sorted(self.requests.items())],
                'physical_reads':[{'phase':phase,'path':name,'reads':count} for (phase,name),count in sorted(self.physical.items())],
                'usage':self.meter.usage(),'actual_production_closure_authenticated':False}


def epoch_schedule(items):
    """Declare future metered Git/blob reads; do not execute/verify or charge them."""
    require(type(items) is list and items,'lineage_explicit_epoch_schedule_required')
    result=[];seen=set()
    for item in items:
        require(type(item) is dict and set(item)=={'head','path','bytes','sha256','reads'}
                and type(item['head']) is str and roster.HEAD.fullmatch(item['head'])
                and type(item['path']) is str and not Path(item['path']).is_absolute()
                and item['path'] not in {'','.'} and str(Path(item['path']))==item['path']
                and '..' not in Path(item['path']).parts and type(item['sha256']) is str
                and roster.SHA.fullmatch(item['sha256']),'lineage_epoch_blob_schedule_schema')
        integer(item['bytes'],'lineage_epoch_blob_bytes',1);integer(item['reads'],'lineage_epoch_blob_read_count',1)
        key=item['head'],item['path'];require(key not in seen,'lineage_duplicate_epoch_blob');seen.add(key)
        result.append(deepcopy(item))
    return {'schedule':result,'declared_Git_read_bytes':sum(item['bytes']*item['reads'] for item in result),
            'original_source_epoch_blobs_verified_here':False}


def projected_control_inventory(controls,resource_raw):
    """Original-device projection candidate; never rewrite original JSON bytes.

Only data-volume controls are supported. Root-volume or other-device cases are
unreviewed blockers. The caller must obtain controls from its internally owned
byte/identity reader; this pure calculation is not a production proof token.
    """
    preparation.actual_resource(resource_raw)
    require(type(controls) is list and controls,'lineage_control_projection_roster')
    originals=[];currents=[];seen=set()
    for item in controls:
        require(type(item) is dict and set(item)=={'path','bytes','sha256','identity'},'lineage_control_projection_schema')
        ref=proof.reference({key:item[key] for key in ('path','bytes','sha256')})
        require(Path('/mnt/data') in Path(ref['path']).parents and ref['path'] not in seen,
                'lineage_control_projection_nondata_or_duplicate')
        seen.add(ref['path']);current=item['identity']
        require(type(current) is dict and set(current)==roster.STAT_FIELDS
                and all(type(value) is int and value>=0 for value in current.values())
                and current['device']==preparation.CURRENT_DEVICE,'lineage_control_projection_device_or_identity')
        original={**current,'device':preparation.OLD_DEVICE}
        preparation._project_identity(original,current)
        currents.append(deepcopy(item));originals.append({**ref,'identity':original})
    originals.sort(key=lambda item:item['path']);currents.sort(key=lambda item:item['path'])
    return {'unique_controls':len(originals),'unique_control_bytes':sum(item['bytes'] for item in originals),
            'original_device_projection_sha256':control_projection_digest(originals),
            'current_identity_projection_sha256':control_projection_digest(currents),
            'projection_codec':PROJECTION_CODEC,
            'required_actual_controls':ACTUAL_CONTROL_FILES,'required_actual_control_bytes':569642935,
            'required_actual_original_projection':ACTUAL_CONTROL_PROJECTION,
            'original_control_bytes_rewritten':False,'actual_production_closure_authenticated':False}


def reconcile(legacy,native,entries,audit_rows,counts):
    require(type(native) is list and type(entries) is list and 0<len(entries)<=profile.PROPOSED['files'],
            'lineage_derived_sequence_schema')
    derived=[(legacy,'legacy_raw'),*[(item,'native_raw') for item in native]]
    require(len(derived)==len(entries),'lineage_derived_source_count')
    rows=iter(audit_rows);groups={};paths=set();physical=set()
    for index,(entry,(original,role)) in enumerate(zip(entries,derived)):
        entry=profile.core.source_binding(entry)
        require(entry['path'] not in paths and (entry['identity']['device'],entry['identity']['inode']) not in physical,
                'lineage_source_alias_or_duplicate')
        paths.add(entry['path']);physical.add((entry['identity']['device'],entry['identity']['inode']))
        require(entry['path']==original['path'] and entry['bytes']==original['bytes'] and entry['source_role']==role,
                'lineage_original_source_sequence_changed')
        if role=='native_raw':require(entry['payload_sha256']==original['payload_sha256'],'lineage_native_payload_binding')
        else:
            require(entry['footer_sha256']==original['footer_sha256']
                    and entry['identity']=={key:original['identity']['target'][key] for key in roster.STAT_FIELDS},
                    'lineage_legacy_original_identity_or_footer_changed')
        row=next(rows,None);require(row is not None,'lineage_missing_audit_row')
        declared=integer(original.get('rows_declared'),'lineage_declared_physical_rows')
        require(profile.check_audit(row,entry,index)==declared,'lineage_audit_row_conservation')
        group=original.get('group','legacy_raw')
        saved=groups.setdefault(group,{'files':0,'bytes':0,'rows_declared':0,'zero_row_files':0})
        saved['files']+=1;saved['bytes']+=entry['bytes'];saved['rows_declared']+=declared;saved['zero_row_files']+=int(declared==0)
    require(next(rows,None) is None and groups==counts,'lineage_extra_audit_or_group_counts_changed')
    return {'source_files':len(entries),'declared_physical_rows':sum(item['rows_declared'] for item in groups.values()),
            'source_groups':groups,'physical_multiplicity_preserved':True,
            'legacy_footer_identity_is_full_payload_SHA':False,'actual_production_generation_authenticated':False}


def replay_fixture(reader,bindings,audit_rows,source_epoch_schedule):
    require(type(reader) is FixtureReader,'lineage_temp_fixture_reader_required')
    require(type(bindings) is dict and set(bindings)=={'descriptor','roster','roster_manifest'},'lineage_fixture_bindings')
    epochs=epoch_schedule(source_epoch_schedule)
    reader.meter.reserve('git_blob',epochs['declared_Git_read_bytes'])
    with reader.meter.hard_timer():
        reader.namespace()
        desc=reader.json(bindings['descriptor']);listing=reader.json(bindings['roster'])
        manifest=reader.json(bindings['roster_manifest'])
        require(desc.get('schema_version')=='polymarket_exact_header_roster_descriptor_v1'
                and listing.get('schema_version')=='polymarket_exact_header_key_roster_v1'
                and manifest.get('descriptor')==bindings['descriptor'],'lineage_fixture_descriptor_roster')
        large={name for name,item in reader.declared.items() if item['role']=='historical_manifest'}
        require(large=={desc['historical_full']['raw_manifest']['path']},'lineage_historical_role_not_exact_descriptor')
        preflight=reader.json(desc['preflight'])
        legacy,native=roster.lineage(reader,desc,preflight)
        reader.json(desc['header_ledger'])
        result=reconcile(legacy,native,listing['files'],audit_rows,manifest['counts'])
        require(result['source_files']==manifest['source_files'],'lineage_roster_manifest_file_count')
        reader.reopen()
        return {'schema_version':SCHEMA,'status':'synthetic_lineage_replayed_not_actual_admission',
                'fixture_only':True,'production_admitted':False,'reconciliation':result,
                'controls':reader.inventory(),'source_epoch_schedule':epochs,
                'actual_required_source_files':ACTUAL_SOURCE_FILES,
                'actual_required_controls':ACTUAL_CONTROL_FILES,
                'actual_required_original_projection':ACTUAL_CONTROL_PROJECTION,
                'actual_full_closure_replayed_here':False,'source_payload_or_footer_bytes_read':0,
                'device_projection_applied_to_original_control_bytes':False}


def contract():
    return {'schema_version':SCHEMA,'status':'local_fixture_reader_candidate_real_entry_closed',
            'shared_read_ceiling_bytes':READ_LIMIT,'historical_manifest_explicit_role_cap_bytes':ROLE_CAPS['historical_manifest'],
            'actual_source_files_required':ACTUAL_SOURCE_FILES,'actual_unique_controls_required':ACTUAL_CONTROL_FILES,
            'actual_original_control_projection_required':ACTUAL_CONTROL_PROJECTION,
            'old_prior_JSON_counter_used':False,'original_control_bytes_rewritten':False,
            'original_Git_blobs_verified':False,'production_input_IO_enabled':False,
            'missing_actual_gates':['original19108Bsource_plan','complete_original1766control_ref_union',
                'actual131356ordered_roster_and_audit_reconciliation','fresh_source_identity_and_device_only_projection',
                'actual_metered_source_epoch_blob_proof','shared_budget_outer_resource_and_lifecycle_admission']}


def run_production(*args,**kwargs):
    require_production_host()
    raise Blocked('native_profile_lineage_actual_driver_admission_missing_before_IO')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--describe-contract',action='store_true')
    args=parser.parse_args(argv)
    if args.describe_contract:print(encode(contract()).decode(),end='');return 0
    run_production();return 2


if __name__=='__main__':raise SystemExit(main())
