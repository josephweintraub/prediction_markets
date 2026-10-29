#!/usr/bin/env python3
"""Zero-HTTP preparation of the fixed 400-header/second stopped-v6 continuation."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import math
import os
from pathlib import Path
import resource
import signal
import sys
import time

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v7 as m
from scripts import launch_polymarket_oct1_stage_v6 as caller
from scripts import prepare_polymarket_exact_header_campaign_v6 as old

OP=old.OP
RUN=Path('/mnt/data/runs/2026-10-29_polymarket_exact_header_campaign_v7')
REQUEST=OP/'header_v7_preparation_request_v1.json'
HOST=OP/'full_header_v7_host_preflight_v1.json'
QA=OP/'full_header_v7_linux_fixtures_v1.json'
LOG=OP/'full_header_v7_linux_fixtures_v1.log'
COMMIT=OP/'full_header_v7_source_commit_v1.json'
LEDGER=OP/'header_v7_external_control_ledger_v1.json'
ADMISSION=OP/'full_header_v7_admission_v1.json'
CHECK=OP/'full_header_v7_root_admission_check_v1.json'
PRIVATE=old.PRIVATE
ACTUAL_KEYS=32901442
HARD_SECONDS=1200
PREPARATION_RESERVE_SECONDS=1800
MAX_CONTROL=1024**2
ADMISSION_BYTES=192*1024
CHECK_BYTES=16*1024
ROOT_CONTROL_BYTES=8192
EXTERNAL=2*1024**2
FUTURE=caller.LOG_LIMIT+ADMISSION_BYTES+CHECK_BYTES+6*8192
FUTURE_FILES=9
SOURCE_FILES=tuple(dict.fromkeys((*m.SOURCE_FILES,*caller.SOURCE_FILES,
    'scripts/prepare_polymarket_exact_header_campaign_v7.py','tests/test_prepare_polymarket_exact_header_campaign_v7.py',
    'docs/analysis_specs/polymarket_exact_header_paced_continuation_v7.md')))
NEW_SOURCE_FILES=tuple(name for name in SOURCE_FILES if name not in old.SOURCE_FILES)
QA_COMMAND=['/home/ubuntu/venv/bin/python','-m','unittest',
    'tests.test_polymarket_exact_header_campaign_v7','tests.test_polymarket_exact_header_campaign_v6',
    'tests.test_polymarket_exact_header_campaign_v5','tests.test_polymarket_exact_header_campaign_v4',
    'tests.test_polymarket_exact_header_campaign_v3','tests.test_launch_polymarket_oct1_stage_v6',
    'tests.test_prepare_polymarket_exact_header_campaign_v7']
QA_COUNT=244  # Exact seven-module TestLoader count; zero skips required on Linux.
Blocked,require,encode,digest,ref=m.Blocked,m.require,m.encode,m.digest,m.ref


class Reader:
    def __init__(self,check):self.saved={};self.check=check
    def bind(self,value,maximum=MAX_CONTROL):
        self.check();capture=m.reference(value,maximum)
        require(capture['path'] not in self.saved or self.saved[capture['path']]==capture,'input_changed')
        self.saved[capture['path']]=capture;return capture
    def load(self,value,maximum=MAX_CONTROL):
        capture=self.bind(value,maximum);decoded,again=m.read_json(value,maximum)
        require(again==capture,'input_changed');return decoded
    def reopen(self):
        for value in list(self.saved.values()):
            self.check();require(m.reference(ref(value),max(value['bytes'],1))==value,'late_input_changed')


def source_snapshot(head):
    base=m.source_snapshot(head);other=caller.source_snapshot(head);files=dict(base['files']);captured={}
    require(other['head']==head,'caller_head')
    for name,value in other['files'].items():
        require(name not in files or files[name]==value,'source_overlap');files[name]=value
    for name in SOURCE_FILES:
        value=m.bound(REPO/name,MAX_CONTROL);raw=m.scale.git_blob(head,name)
        require(len(raw)==value['bytes'] and digest(raw)==value['sha256'],'source_blob_differs')
        captured[name]=value;files[name]={key:value[key] for key in ('bytes','sha256')}
    require(m.source_snapshot(head)==base and caller.source_snapshot(head)==other,'source_changed')
    for name,value in captured.items():require(m.bound(REPO/name,MAX_CONTROL)==value,'source_reopen_changed')
    return {'head':head,'files':files}


def timestamp(value):
    require(type(value) is str,'preparation_anchor_not_utc')
    parsed=datetime.fromisoformat(value)
    require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds()==0 and parsed.isoformat()==value,
      'preparation_anchor_not_utc')
    return parsed.timestamp()


def runtime_floor(base,charge,anchor,now,reserve=PREPARATION_RESERVE_SECONDS):
    m.legacy.elapsed(base);m.legacy.elapsed(charge)
    beginning=timestamp(anchor)
    require(now>=beginning,'preparation_clock_before_anchor')
    require(type(reserve) is int and reserve in (0,PREPARATION_RESERVE_SECONDS),'preparation_reserve')
    return math.ceil(base+charge+now-beginning)+reserve


def host_gate(value,head,environment,resources,now):
    expected={'schema_version','status','checked_utc','source_head','source_clean','instance_id','environment',
      'available_ram_bytes','available_disk_bytes','live_owned_births','unknown_owned_births',
      'relevant_compute_notebook_transfer_consumers','agents_need_EC2','all_saved_PROCESS_births_checked',
      'process_receipts_checked','monitor_ancestry_explicitly_checked','production_stage_running'}
    require(type(value) is dict and set(value)==expected and value['schema_version']=='polymarket_header_v7_fresh_host_preflight_v1'
      and value['status']=='root_verified_header_recovery_host','host_schema')
    require(0<=now-timestamp(value['checked_utc'])<=300 and value['source_head']==head and value['source_clean'] is True
      and value['instance_id']=='i-0f5b31a268af53938' and value['environment']==environment,'host_identity_or_stale')
    m.previous.environment_valid(environment)
    require(all(type(value[key]) is int and value[key]==0 for key in ('live_owned_births','unknown_owned_births'))
      and value['relevant_compute_notebook_transfer_consumers']==[] and value['agents_need_EC2'] is False
      and value['all_saved_PROCESS_births_checked'] is True and type(value['process_receipts_checked']) is int
      and value['process_receipts_checked']>=603 and value['monitor_ancestry_explicitly_checked'] is True
      and value['production_stage_running'] is False,'host_not_drained')
    require(type(value['available_ram_bytes']) is int and min(value['available_ram_bytes'],resources['ram_bytes']['MemAvailable'])
      >=m.CAPS['minimum_available_ram_bytes'] and type(value['available_disk_bytes']) is int
      and min(value['available_disk_bytes'],resources['disk_bytes']['free'])>=m.CAPS['minimum_free_bytes']
      +m.CAPS['chunk_artifact_bytes']+m.FINAL_RESERVE,'host_resources')


def fixture_gate(qa,commit,source,log):
    require(QA_COUNT>0 and qa.get('schema_version')=='polymarket_header_v7_linux_fixture_receipt_v1'
      and qa.get('status')=='passed' and qa.get('source_head')==source['head']
      and qa.get('source_clean_before') is True and qa.get('source_clean_after') is True
      and type(qa.get('exit_code')) is int and qa['exit_code']==0 and qa.get('command')==QA_COMMAND
      and qa.get('interpreter')==QA_COMMAND[0] and type(qa.get('tests_run')) is int and qa['tests_run']==QA_COUNT
      and type(qa.get('tests_skipped')) is int and qa['tests_skipped']==0 and qa.get('tested_source_files')==source['files']
      and qa.get('log')==log,'linux_qa')
    require(commit.get('schema_version')=='polymarket_header_v7_source_commit_v1' and commit.get('status')=='clean_committed_and_pushed'
      and commit.get('source_head')==source['head'] and commit.get('git_status_empty') is True
      and commit.get('origin_commit_matches') is True and commit.get('source_files')==source['files'],'source_commit')


def external_budget(values,ledger_bytes=0):
    require(len({item['path'] for item in values})==len(values),'external_duplicate')
    require(len(values)+FUTURE_FILES+(1 if ledger_bytes else 0)<=256
      and sum(item['bytes'] for item in values)+ledger_bytes+FUTURE<=EXTERNAL,'external_budget')


def write(path,value,reader,maximum=MAX_CONTROL):
    raw=encode(value)
    require(len(raw)<=maximum and not path.exists() and not path.is_symlink(),'output_exists_or_limit')
    with path.open('xb') as stream:
        os.chmod(path,0o600);require(stream.write(raw)==len(raw),'short_write');stream.flush();os.fsync(stream.fileno())
    fd=os.open(OP,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
    try:os.fsync(fd)
    finally:os.close(fd)
    return ref(reader.bind({'path':str(path),'bytes':len(raw),'sha256':digest(raw)},maximum))


def caller_envelope(source,admission_binding,head,prior):
    # Conservative decimal-width stand-ins, never a fabricated process proof.
    identity={'hostname':'x'*255,'boot_id':'f'*8+'-ffff-ffff-ffff-'+'f'*12,
      'pid':2**63-1,'birth_ticks':2**64-1,'process_group':2**63-1,'session':2**63-1}
    base={'schema_version':caller.SCHEMA,'stage':'headers','command':caller.command('headers',ADMISSION,
      admission_binding['sha256'],head,RUN),'source':source,'admission':admission_binding,'output_dir':str(RUN),
      'caller':identity,'timeout_seconds':m.CAPS['runtime_seconds'],'admission_prior_runtime_seconds':prior,
      'remaining_wall_seconds_at_start':m.CAPS['runtime_seconds']-prior,'cleanup_seconds_included':30,
      'retry_or_resume':False,'host_idle_claimed':False,'final_sample_admitted':False}
    # Fixed field superset bounds START, LAUNCH and EXIT, including every
    # observed lifecycle status/error string and cumulative stdout counters.
    bound={**base,'status':'x'*128,'child':identity,'child_pid_observed':2**63-1,'ownership_unknown':False,
      'producer_exit_code':-2**31,'owned_group_drained':False,'children_still_running':False,
      'source_and_admission_unchanged':False,'error_code':'x'*256,'elapsed_seconds':259200.123456789,
      'stdout_bytes_observed':2**64-1,'stdout_prefix_bytes_retained':caller.LOG_LIMIT,'stdout_sha256':'f'*64,
      'stdout_prefix_sha256':'f'*64,'stdout_truncated':False,'stdout_eof_observed':False}
    require(caller.JSON_LIMIT==8192 and len(caller.encode(bound))<=caller.JSON_LIMIT,'caller_envelope_does_not_fit')
    return len(caller.encode(bound))


def prepare(request_path,request_sha,head,*,host_refresh=None):
    began=time.monotonic()
    def check():require(time.monotonic()-began<HARD_SECONDS,'preparation_runtime')
    require(host_refresh is None or callable(host_refresh),'host_refresh_callback_invalid')
    require(Path(request_path)==REQUEST and not RUN.exists() and not RUN.is_symlink()
      and all(not path.exists() and not path.is_symlink() for path in (LEDGER,ADMISSION,CHECK,CHECK.with_name('.'+CHECK.name+'.pending'))),
      'fresh_paths_required')
    reader=Reader(check)
    request_ref={'path':str(REQUEST),'bytes':m.identity(REQUEST)['bytes'],'sha256':request_sha}
    value=reader.load(request_ref)
    expected={'schema_version','status','source_head','stopped_v6','host_proof','linux_qa','source_commit',
      'extra_prior_controls','extra_external_controls','root_complete_header_control_roster',
      'fresh_preparation_anchor_utc','fresh_preparation_reserve_seconds','cumulative_rpc'}
    require(type(value) is dict and set(value)==expected and value['schema_version']=='polymarket_header_v7_preparation_request_v1'
      and value['status']=='root_ready_for_bounded_header_admission_preparation' and value['source_head']==head
      and value['root_complete_header_control_roster'] is True and value['fresh_preparation_reserve_seconds']==PREPARATION_RESERVE_SECONDS,
      'request_schema')
    require(Path(value['stopped_v6']['path'])==m.HANDOFF_PATH and Path(value['host_proof']['path'])==HOST
      and Path(value['linux_qa']['path'])==QA and Path(value['source_commit']['path'])==COMMIT,'proof_paths')
    handoff=reader.load(value['stopped_v6'],m.MAX_LEDGER)
    old_admission=reader.load(handoff['refs']['admission'],m.MAX_LEDGER)
    drain=reader.load(handoff['refs']['drain'],m.MAX_LEDGER)
    require(value['fresh_preparation_anchor_utc']==drain['checked_utc'],'preparation_anchor_not_drain')
    source=source_snapshot(head)
    for name,binding in source['files'].items():reader.bind({'path':str(REPO/name),**binding})
    host=reader.load(value['host_proof'])
    def fresh(*,initial=False):
        # The immutable starting proof is mandatory. Only the reviewed root
        # dispatcher may supply an in-memory current census at later gates;
        # the standalone CLI supplies no callback and keeps the same limit.
        current=host_refresh() if not initial and host_refresh is not None else host
        host_gate(current,head,m.previous.reboot_environment(),m.inventory.resource_state(OP),time.time());check()
    fresh(initial=True)
    qa=reader.load(value['linux_qa']);commit=reader.load(value['source_commit']);log=qa.get('log')
    require(type(log) is dict and Path(log.get('path',''))==LOG,'qa_log_path');reader.bind(log,65536)
    fixture_gate(qa,commit,source,log)
    inputs,measured=m.validate_inputs(old_admission['inputs'],source,ACTUAL_KEYS)
    for binding in inputs.values():reader.bind(ref(binding),m.CAPS['baseline_bytes'])
    reader.bind(old_admission['private_file'],16384);reader.bind(old_admission['reboot_equivalence'])
    require(old_admission['actual_keys']==ACTUAL_KEYS and old_admission['caps']==m.CAPS
      and old_admission['private_file']['path']==str(PRIVATE),'base_admission')
    external={}
    def add(binding):
        captured=reader.bind(binding,m.MAX_LEDGER)
        previous=external.get(binding['path'])
        require(previous is None or previous==binding,'external_conflicting_reference')
        external[binding['path']]=dict(binding);return captured
    require(type(drain['external_control_closure']) is list,'old_external_closure')
    for binding in drain['external_control_closure']:add(binding)
    add(value['stopped_v6'])
    for binding in handoff['refs'].values():
        if m.V6_ROOT not in Path(binding['path']).parents:add(binding)
    direct=[request_ref,value['host_proof'],value['linux_qa'],value['source_commit'],log]
    for binding in direct:add(binding)
    require(type(value['extra_prior_controls']) is list and type(value['extra_external_controls']) is list
      and len(value['extra_prior_controls'])+len(value['extra_external_controls'])<=200,'extra_control_roster')
    for binding in value['extra_prior_controls']+value['extra_external_controls']:
        path=Path(binding['path'])
        require(path.parent==OP and path.name.startswith(('header_','full_header_','exact_header_','full_stage_')),'extra_control_scope')
        m.legacy.standalone_operational_control(path);add(binding)
    for name in NEW_SOURCE_FILES:add({'path':str(REPO/name),**source['files'][name]})
    external_budget(list(external.values()))
    ledger={'schema_version':m.EXTERNAL_SCHEMA,'files':list(external.values()),
      'future_reserved_bytes':FUTURE,'future_reserved_files':FUTURE_FILES,'ceiling_bytes':EXTERNAL}
    external_budget(list(external.values()),len(encode(ledger)))
    ledger_ref=write(LEDGER,ledger,reader);add(ledger_ref)
    base=m.decode(encode(old_admission['prior']))
    prior={'base':base,'stopped_v6':value['stopped_v6'],'rpc':dict(base['rpc']),'external_controls':ledger_ref}
    # The journal minimum is subsequently established by the collector's exact
    # stopped-run replay; root supplies its conservatively bounded cumulative RPC.
    require(type(value['cumulative_rpc']) is dict and set(value['cumulative_rpc'])==set(prior['rpc']),'root_rpc_schema')
    for key,count in value['cumulative_rpc'].items():
        m.legacy.elapsed(count) if key=='elapsed_seconds' else m.legacy.integer(count,m.CAPS[key])
    prior['rpc']=dict(value['cumulative_rpc'])
    prior['rpc']['elapsed_seconds']=max(prior['rpc']['elapsed_seconds'],runtime_floor(base['rpc']['elapsed_seconds'],
      handoff['runtime_charge_seconds'],value['fresh_preparation_anchor_utc'],time.time()))
    reboot=m.validate_reboot(old_admission['reboot_equivalence'],prior,inputs,old_admission['private_file'])
    accepted=m.validate_prior(prior,RUN,source,inputs,reboot)
    sealed=accepted['sealed'];keys_per=m.CAPS['keys_per_chunk']
    reused=sum(min(keys_per,ACTUAL_KEYS-chunk*keys_per) for chunk in sealed)
    remaining=ACTUAL_KEYS-reused
    projection=math.ceil(max(m.forecast_seconds(remaining,m.CAPS['initial_batch_size'],accepted['observed_max_call_seconds']),
      2*measured['seconds_per_key']*remaining,accepted['observation_elapsed_seconds']*math.ceil(remaining/m.CAPS['initial_batch_size'])))
    require(prior['rpc']['elapsed_seconds']+projection+time.monotonic()-began<m.CAPS['runtime_seconds'],'runtime_not_fit')
    admission={key:old_admission[key] for key in ('status','caps','raw_clocks_only','final_sample_admitted','inputs','private_file','actual_keys','reboot_equivalence')}
    admission.update(schema_version=m.SCHEMA,consumer_head=head,run_dir=str(RUN),prior=prior,
      conservative_new_runtime_seconds=projection)
    require(len(encode(admission))<=ADMISSION_BYTES,'admission_byte_envelope')
    roots=old.physical_sets({'roots':[str(m.V6_ROOT),*base['roots']]})
    def final_gate():
        fresh();require(source_snapshot(head)==source,'source_changed')
        require(runtime_floor(base['rpc']['elapsed_seconds'],handoff['runtime_charge_seconds'],
          value['fresh_preparation_anchor_utc'],time.time(),reserve=0)<=prior['rpc']['elapsed_seconds'],'preparation_reservation_expired')
        require(m.validate_reboot(old_admission['reboot_equivalence'],prior,inputs,old_admission['private_file'])==reboot,'reboot_changed')
        require(m.validate_prior(prior,RUN,source,inputs,reboot)==accepted,'prior_changed')
        for binding in accepted['files'].values():
            require(m.bound(Path(binding['path']),m.CAPS['global_header_bytes'])==
              {key:binding[key] for key in ('path','bytes','sha256','identity')},'final_prior_binding_changed')
        require(old.physical_sets({'roots':[str(m.V6_ROOT),*base['roots']]})==roots,'prior_file_set_changed')
        external_budget(list(external.values()));reader.reopen();check()
    final_gate();admission_ref=write(ADMISSION,admission,reader,ADMISSION_BYTES)
    caller_max=caller_envelope(caller.source_snapshot(head),m.bound(ADMISSION,ADMISSION_BYTES),head,prior['rpc']['elapsed_seconds'])
    chunks=math.ceil(ACTUAL_KEYS/keys_per);next_chunk=next((i for i in range(chunks) if i not in sealed),None)
    next_attempt=None if next_chunk is None else len(accepted['attempts'].get(next_chunk,set()))+1
    require(next_attempt is None or next_attempt<=m.CAPS['attempts_per_chunk'],'chunk_attempts_exhausted')
    result={'schema_version':'polymarket_header_v7_preparation_check_v1','status':'header_only_admission_prepared_not_launched',
      'source':source,'request':request_ref,'stopped_v6':value['stopped_v6'],'external_ledger':ledger_ref,'admission':admission_ref,
      'prior_rpc':prior['rpc'],'reused_keys':reused,'remaining_keys':remaining,'next_chunk':next_chunk,'next_attempt':next_attempt,
      'conservative_new_runtime_seconds':projection,'preparation_anchor_utc':value['fresh_preparation_anchor_utc'],
      'preparation_seconds_reserved':PREPARATION_RESERVE_SECONDS,'operator_hard_runtime_seconds':HARD_SECONDS,
      'external_existing_bytes':sum(x['bytes'] for x in external.values()),'external_future_reserved_bytes':FUTURE,
      'external_ceiling_bytes':EXTERNAL,'caller_control_encoded_maximum_bytes':caller_max,'method_items_per_second':400,
      'provider_requests_sent':0,'producer_launched':False,'raw_clocks_only':True,'final_sample_admitted':False,
      'root_external_reopen_before_launch_and_terminal_required':True}
    pending=CHECK.with_name('.'+CHECK.name+'.pending');write(pending,result,reader,CHECK_BYTES)
    final_gate();check_ref=ref(m.bound(pending,CHECK_BYTES));m.scale.atomic_publish(pending,CHECK)
    return {'status':result['status'],'admission':admission_ref,'check':dict(check_ref,path=str(CHECK)),
      'provider_requests_sent':0,'producer_launched':False}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--preparation-request',type=Path,required=True)
    p.add_argument('--preparation-request-sha256',required=True);p.add_argument('--expected-head',required=True);args=p.parse_args(argv)
    require_production_host();require(Path(sys.executable)==Path('/home/ubuntu/venv/bin/python'),'production_interpreter')
    resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3));resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    require(signal.getitimer(signal.ITIMER_REAL)==(0.,0.),'existing_timer')
    def expired(*_):raise Blocked('preparation_runtime')
    signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,HARD_SECONDS)
    try:result=prepare(args.preparation_request,args.preparation_request_sha256,args.expected_head)
    except BaseException:
        print('{"status":"header_v7_preparation_blocked","provider_requests_sent":0,"producer_launched":false}');return 1
    finally:signal.setitimer(signal.ITIMER_REAL,0)
    print(encode(result).decode().strip());return 0


if __name__=='__main__':raise SystemExit(main())
