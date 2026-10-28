#!/usr/bin/env python3
"""Fixed header-only, zero-HTTP v6 admission preparation; never launches a caller."""
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
from scripts import collect_polymarket_exact_header_campaign_v6 as m
from scripts import launch_polymarket_oct1_stage_v5 as caller
OP=m.previous.OP_ROOT
RUN=Path("/mnt/data/runs/2026-10-28_polymarket_exact_header_campaign_v6")
REQUEST=OP/"header_v6_preparation_request_v1.json"
HOST=OP/"full_header_v6_host_preflight_v1.json"
QA=OP/"full_header_v6_linux_fixtures_v2.json"
LOG=OP/"full_header_v6_linux_fixtures_v2.log"
COMMIT=OP/"full_header_v6_source_commit_v1.json"
CERT=OP/"header_reboot_equivalence_v6_v1.json"
LEDGER=OP/"header_v6_external_control_ledger_v1.json"
ADMISSION=OP/"full_header_v6_admission_v1.json"
CHECK=OP/"full_header_v6_root_admission_check_v1.json"
PRIVATE=Path("/home/ubuntu/.polygon_rpc_url")
ACTUAL_KEYS=32901442
ANCHOR="2026-10-28T17:45:44Z"
HARD_SECONDS=1200
PREPARATION_RESERVE_SECONDS=1800  # This operator <=1200 plus <=600 root launch/control window.
MAX_CONTROL=1024**2
EXTERNAL=m.CAPS["external_control_reservation_bytes"]
# One shared pool, not a fresh allowance per file. Four 64KiB admission
# segments plus seven <=64KiB controls, and the existing 1MiB caller log.
FUTURE=1024**2+11*65536
FUTURE_FILES=9  # admission/check, caller START/LAUNCH/EXIT/log, root dispatch/accept/checkpoint.
SOURCE_FILES=tuple(dict.fromkeys((*m.SOURCE_FILES,*caller.SOURCE_FILES,
    "scripts/prepare_polymarket_exact_header_campaign_v6.py","tests/test_prepare_polymarket_exact_header_campaign_v6.py",
    "docs/analysis_specs/polymarket_exact_header_reboot_recovery_v6.md")))
QA_COMMAND=["/home/ubuntu/venv/bin/python","-m","unittest",
    "tests.test_polymarket_exact_header_campaign_v6","tests.test_polymarket_exact_header_campaign_v5",
    "tests.test_polymarket_exact_header_campaign_v4","tests.test_polymarket_exact_header_campaign_v3",
    "tests.test_launch_polymarket_oct1_stage_v5","tests.test_prepare_polymarket_exact_header_campaign_v6"]
QA_COUNT=209  # Exact six-module TestLoader count; zero skips required on Linux.
OLD_POST={
    "check":{"path":str(OP/"full_acquisition_root_admission_checks_v3.json"),"bytes":2002,
        "sha256":"f3b3d17423ef85d57f91b02e593482f6b34720e7f6391cb8ee1a9ea14fb802dc"},
    "caller":{"path":str(OP/"full_header_v4.CALLER.json"),"bytes":1289,
        "sha256":"7c0470ea5ffeb49c0074977adf33555c79995380f90be70ed3a7c006dc575e25"},
    "dispatch":{"path":str(OP/"full_acquisition_dispatch_v3.COMPLETE.json"),"bytes":3507,
        "sha256":"d4c883a882be896e967bfe549ab2b910874c896b2c67ae0b0ad429fad2eeadb0"},
    "charge":{"path":str(OP/"header_v4_charge_attestation_v1.json"),"bytes":1886,
        "sha256":"a583a50b9a7decae9f947e0b07f428b7be658b6804752fc315776489bc7f615e"}}
OLD_STARTS=[
    {"path":str(OP/"full_header_v4.START.json"),"bytes":2167,
     "sha256":"afe2e59f07499e275c1b80994a03debb55f3e9124e6a9f7d4f2c18ac2bb4b2da"},
    {"path":str(OP/"full_acquisition_dispatch_v3.START.json"),"bytes":1080,
     "sha256":"d9dc2070b19194413dde6212d88d1419ad09a825ba79e1f908bc4d64054cfb4d"}]
Blocked,require,encode,digest,ref=m.Blocked,m.require,m.encode,m.digest,m.ref


class Reader:
    def __init__(self,check):self.saved={};self.check=check
    def bind(self,value,maximum=MAX_CONTROL):
        self.check()
        require(type(value) is dict and set(value)=={"path","bytes","sha256"},"control_ref_schema")
        capture=m.reference(value,maximum)
        require(capture["path"] not in self.saved or self.saved[capture["path"]]==capture,"input_changed")
        self.saved[capture["path"]]=capture;return capture
    def load(self,value,maximum=MAX_CONTROL):
        capture=self.bind(value,maximum);decoded,again=m.read_json(value,maximum)
        require(again==capture,"input_changed");return decoded
    def reopen(self):
        for value in list(self.saved.values()):require(m.reference(ref(value),max(value["bytes"],1))==value,"late_input_changed")


def source_snapshot(head):
    source=m.source_snapshot(head);other=caller.source_snapshot(head);files=dict(source["files"])
    require(other["head"]==head,"caller_head")
    for name,value in other["files"].items():
        require(name not in files or files[name]==value,"source_overlap");files[name]=value
    captures={}
    for name in SOURCE_FILES:
        actual=m.bound(REPO/name,MAX_CONTROL);raw=m.scale.git_blob(head,name)
        require(len(raw)==actual["bytes"] and digest(raw)==actual["sha256"],"source_blob_differs")
        captures[name]=actual;files[name]={key:actual[key] for key in ("bytes","sha256")}
    require(m.source_snapshot(head)==source and caller.source_snapshot(head)==other,"source_changed")
    for name,value in captures.items():require(m.bound(REPO/name,MAX_CONTROL)==value,"source_reopen_changed")
    return {"head":head,"files":files}


def runtime_floor(now,reserve=PREPARATION_RESERVE_SECONDS):
    anchor=datetime.fromisoformat(ANCHOR.replace("Z","+00:00")).timestamp()
    require(now>=anchor,"preparation_clock_before_anchor")
    return 14400+math.ceil(now-anchor)+reserve


def host_gate(value,head,environment,resources,now):
    expected={"schema_version","status","checked_utc","source_head","source_clean","instance_id","environment",
        "available_ram_bytes","available_disk_bytes","live_owned_births","unknown_owned_births",
        "relevant_compute_notebook_transfer_consumers","agents_need_EC2","all_saved_PROCESS_births_checked",
        "process_receipts_checked","monitor_ancestry_explicitly_checked","production_stage_running"}
    require(type(value) is dict and set(value)==expected and
        value["schema_version"]=="polymarket_header_v6_fresh_host_preflight_v1" and
        value["status"]=="root_verified_header_recovery_host","host_schema")
    checked=datetime.fromisoformat(value["checked_utc"].replace("Z","+00:00"))
    require(checked.tzinfo is not None and 0<=now-checked.timestamp()<=300,"host_stale")
    require(value["source_head"]==head and value["source_clean"] is True and
        value["instance_id"]=="i-0f5b31a268af53938" and value["environment"]==environment,"host_identity")
    m.environment_valid(environment)
    require(all(type(value[key]) is int and value[key]==0 for key in ("live_owned_births","unknown_owned_births")) and
        value["relevant_compute_notebook_transfer_consumers"]==[] and value["agents_need_EC2"] is False and
        value["all_saved_PROCESS_births_checked"] is True and type(value["process_receipts_checked"]) is int and
        value["process_receipts_checked"]>=603 and value["monitor_ancestry_explicitly_checked"] is True and
        value["production_stage_running"] is False,"host_not_drained")
    require(type(value["available_ram_bytes"]) is int and min(value["available_ram_bytes"],resources["ram_bytes"]["MemAvailable"])
        >=m.CAPS["minimum_available_ram_bytes"] and type(value["available_disk_bytes"]) is int and
        min(value["available_disk_bytes"],resources["disk_bytes"]["free"])>=
        m.CAPS["minimum_free_bytes"]+m.CAPS["chunk_artifact_bytes"]+m.FINAL_RESERVE,"host_resources")


def external_budget(values):
    require(len({value["path"] for value in values})==len(values),"external_duplicate")
    require(len(values)+FUTURE_FILES<=256 and sum(value["bytes"] for value in values)+FUTURE<=EXTERNAL,"external_budget")
    return sum(value["bytes"] for value in values)


def physical_sets(prior):
    result={}
    for name in prior["roots"]:
        root=Path(name);observed=set()
        for path in root.rglob("*"):
            require(not path.is_symlink() and (path.is_dir() or path.is_file()),"prior_tree_type")
            if path.is_file():observed.add(str(path))
        result[name]=observed
    return result


def write(path,value,reader):
    raw=encode(value);require(len(raw)<=MAX_CONTROL and not path.exists() and not path.is_symlink(),"output_exists_or_limit")
    with path.open("xb") as handle:
        require(handle.write(raw)==len(raw),"short_write");handle.flush();os.fsync(handle.fileno())
    fd=os.open(OP,os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
    try:os.fsync(fd)
    finally:os.close(fd)
    return ref(reader.bind({"path":str(path),"bytes":len(raw),"sha256":digest(raw)}))


def fixture_gate(qa,commit,source,log):
    require(QA_COUNT>0 and qa.get("schema_version")=="polymarket_header_v6_linux_fixture_receipt_v1" and
        qa.get("status")=="passed" and qa.get("source_head")==source["head"] and qa.get("source_clean_before") is True and
        qa.get("source_clean_after") is True and type(qa.get("exit_code")) is int and qa["exit_code"]==0 and
        qa.get("command")==QA_COMMAND and qa.get("interpreter")==QA_COMMAND[0] and
        type(qa.get("tests_run")) is int and qa["tests_run"]==QA_COUNT and type(qa.get("tests_skipped")) is int and
        qa["tests_skipped"]==0 and qa.get("tested_source_files")==source["files"] and qa.get("log")==log,"linux_qa")
    require(commit.get("schema_version")=="polymarket_header_v6_source_commit_v1" and
        commit.get("status")=="clean_committed_and_pushed" and commit.get("source_head")==source["head"] and
        commit.get("git_status_empty") is True and commit.get("origin_commit_matches") is True and
        commit.get("source_files")==source["files"],"source_commit")


def prepare(request_path,request_sha,head):
    began=time.monotonic()
    def check():require(time.monotonic()-began<HARD_SECONDS,"preparation_runtime")
    require(Path(request_path)==REQUEST and not RUN.exists() and not RUN.is_symlink() and
        all(not path.exists() and not path.is_symlink() for path in (CERT,LEDGER,ADMISSION,CHECK,CHECK.with_name("."+CHECK.name+".pending"))),
        "fresh_paths_required")
    reader=Reader(check)
    request_ref={"path":str(REQUEST),"bytes":m.identity(REQUEST)["bytes"],"sha256":request_sha}
    request=reader.load(request_ref)
    require(type(request) is dict and set(request)=={"schema_version","status","source_head","host_proof","linux_qa",
        "source_commit","extra_prior_controls","extra_external_controls","root_complete_header_control_roster"} and
        request["schema_version"]=="polymarket_header_v6_preparation_request_v1" and
        request["status"]=="root_ready_for_bounded_header_admission_preparation" and request["source_head"]==head and
        request["root_complete_header_control_roster"] is True,"preparation_request_schema")
    require(Path(request["host_proof"]["path"])==HOST and Path(request["linux_qa"]["path"])==QA and
        Path(request["source_commit"]["path"])==COMMIT,"proof_paths")
    host=reader.load(request["host_proof"]);source=source_snapshot(head)
    for name,value in source["files"].items():reader.bind({"path":str(REPO/name),**value})
    environment=m.reboot_environment()
    def fresh():host_gate(host,head,m.reboot_environment(),m.inventory.resource_state(OP),time.time());check()
    fresh()
    qa=reader.load(request["linux_qa"]);commit=reader.load(request["source_commit"])
    log=qa.get("log");require(type(log) is dict and Path(log.get("path",""))==LOG,"qa_log_path");reader.bind(log,65536)
    fixture_gate(qa,commit,source,log)
    base=reader.load(m.V4_REFS["admission"],m.MAX_LEDGER)
    require(base.get("schema_version")==m.previous.previous.SCHEMA and base.get("consumer_head")==m.previous.V4_HEAD and
        base.get("caps")==m.CAPS and base.get("actual_keys")==ACTUAL_KEYS,"base_admission")
    inputs,measured=m.validate_inputs(base["inputs"],source,base["actual_keys"])
    for value in inputs.values():reader.bind(ref(value),m.CAPS["baseline_bytes"])
    require(base["private_file"]["path"]==str(PRIVATE),"private_authority_path")
    reader.bind(base["private_file"],16384)
    candidate=m.certificate_value(inputs,base["private_file"])
    require(candidate["current_environment"]==environment,"boot_changed")
    prior=m.decode(encode(base["prior"]));prior["roots"].append(str(m.previous.V4_ROOT))
    prior["rpc"]=dict(m.previous.V4_CUMULATIVE_RPC)
    prior["rpc"]["elapsed_seconds"]=runtime_floor(time.time())
    forecast=math.ceil(max(m.forecast_seconds(base["actual_keys"],m.CAPS["initial_batch_size"],0),
        2*measured["seconds_per_key"]*base["actual_keys"]))
    require(prior["rpc"]["elapsed_seconds"]+forecast<m.CAPS["runtime_seconds"],"paced_runtime_not_fit")
    require(max(80,1.25*measured["bytes_per_key"])*base["actual_keys"]<=m.CAPS["accepted_bytes"],"accepted_disk_projection")
    cert_ref=write(CERT,candidate,reader)
    files={value["path"]:value for value in prior["files"]}
    require(len(files)==len(prior["files"]),"base_duplicate")
    def add(value,chunk=None,standalone=True):
        reader.bind(value,m.MAX_LEDGER)
        item={**value,"category":"control","chunk":chunk}
        require(value["path"] not in files,"new_prior_duplicate")
        files[value["path"]]=item;prior["files"].append(item)
        if standalone:prior["standalone_control_files"].append(value["path"])
    for key,value in m.V4_REFS.items():
        add(value,0 if key in {"chunk_journal","diagnostic"} else None,
            key not in {"owner","failed","root_journal","chunk_journal","diagnostic"})
    for value in OLD_POST.values():add(value)
    prior["v4_charge_attestation"]=OLD_POST["charge"]
    require(type(request["extra_prior_controls"]) is list and type(request["extra_external_controls"]) is list and
        len(request["extra_prior_controls"])+len(request["extra_external_controls"])<=200,"extra_control_roster")
    extra=[]
    for value in request["extra_prior_controls"]:
        path=Path(value["path"]);require(path.parent==OP and path.name.startswith(("header_","full_header_","exact_header_","full_stage_")),
            "extra_prior_scope")
        m.legacy.standalone_operational_control(path);add(value);extra.append(value)
    direct=[request_ref,request["host_proof"],request["linux_qa"],request["source_commit"],log]
    for value in direct:add(value)
    add(cert_ref)
    sources=[]
    for name in SOURCE_FILES:
        # Existing producer dependencies remain canonically bound, but only
        # this newly introduced seven-file unit debits this external pool.
        if name in NEW_SOURCE_FILES:
            value={"path":str(REPO/name),**source["files"][name]};reader.bind(value);sources.append(value)
    external=OLD_STARTS+direct+extra+[cert_ref]+sources
    for value in OLD_STARTS:reader.bind(value)
    for value in request["extra_external_controls"]:
        require(Path(value["path"]).parent==OP and Path(value["path"]).name.startswith(("header_","full_header_")),
            "extra_external_scope");reader.bind(value);external.append(value)
    require(not any(value["path"] in prior["standalone_control_files"] for value in OLD_STARTS),"old_start_promoted")
    external_budget(external)
    ledger={"schema_version":"polymarket_header_v6_external_control_ledger_v1","status":"root_header_controls_bound_not_launched",
        "source_head":head,"files":external,"combined_bytes":sum(value["bytes"] for value in external),
        "future_reserved_bytes":FUTURE,"future_reserved_files":FUTURE_FILES,"ceiling_bytes":EXTERNAL,
        "old_starts_untouched_not_standalone":True,"root_reopen_before_launch_and_terminal_required":True}
    require(sum(value["bytes"] for value in external)+len(encode(ledger))+FUTURE<=EXTERNAL,"external_budget")
    ledger_ref=write(LEDGER,ledger,reader);add(ledger_ref);external.append(ledger_ref);external_budget(external)
    admission={key:base[key] for key in ("status","caps","raw_clocks_only","final_sample_admitted","inputs","private_file","actual_keys")}
    admission.update(schema_version=m.SCHEMA,consumer_head=head,run_dir=str(RUN),reboot_equivalence=cert_ref,
        prior=prior,conservative_new_runtime_seconds=forecast)
    reboot=m.validate_reboot(cert_ref,prior,inputs,base["private_file"])
    accepted=m.validate_prior(prior,RUN,source,inputs,reboot)
    root_sets=physical_sets(prior)
    projection=math.ceil(max(forecast,m.forecast_seconds(base["actual_keys"],m.CAPS["initial_batch_size"],
        accepted["observed_max_call_seconds"]),accepted["observation_elapsed_seconds"]*
        math.ceil(base["actual_keys"]/m.CAPS["initial_batch_size"])))
    admission["conservative_new_runtime_seconds"]=projection
    require(len(encode(admission))<=4*65536 and prior["rpc"]["elapsed_seconds"]+projection+time.monotonic()-began<m.CAPS["runtime_seconds"],
        "final_runtime_or_admission_size_not_fit")
    def final_gate():
        fresh();require(source_snapshot(head)==source,"source_changed")
        require(runtime_floor(time.time(),reserve=0)<=prior["rpc"]["elapsed_seconds"],"preparation_reservation_expired")
        require(m.validate_reboot(cert_ref,prior,inputs,base["private_file"])==reboot,"certificate_changed")
        require(m.validate_prior(prior,RUN,source,inputs,reboot)==accepted,"prior_changed")
        for value in accepted["files"].values():
            require(m.bound(Path(value["path"]),m.CAPS["global_header_bytes"])==
                {key:value[key] for key in ("path","bytes","sha256","identity")},"final_prior_binding_changed")
        require(physical_sets(prior)==root_sets,"prior_tree_set_changed")
        external_budget(external);reader.reopen();check()
    final_gate();admission_ref=write(ADMISSION,admission,reader)
    result={"schema_version":"polymarket_header_v6_preparation_check_v1","status":"header_only_admission_prepared_not_launched",
        "source":source,"request":request_ref,"certificate":cert_ref,"external_ledger":ledger_ref,"admission":admission_ref,
        "prior_rpc":prior["rpc"],"conservative_new_runtime_seconds":projection,"external_existing_bytes":sum(x["bytes"] for x in external),
        "external_future_reserved_bytes":FUTURE,"external_ceiling_bytes":EXTERNAL,"runtime_anchor_utc":ANCHOR,
        "remaining_preparation_reservation_seconds":PREPARATION_RESERVE_SECONDS,"operator_hard_runtime_seconds":HARD_SECONDS,
        "first_chunk_attempt":3,"first_chunk_is_last_attempt":True,"provider_requests_sent":0,"producer_launched":False,
        "raw_clocks_only":True,"final_sample_admitted":False,"root_external_reopen_before_launch_and_terminal_required":True}
    pending=CHECK.with_name("."+CHECK.name+".pending");check_ref=write(pending,result,reader)
    final_gate();m.scale.atomic_publish(pending,CHECK)
    return {"status":result["status"],"admission":admission_ref,"check":dict(check_ref,path=str(CHECK)),
        "provider_requests_sent":0,"producer_launched":False}


NEW_SOURCE_FILES={"scripts/collect_polymarket_exact_header_campaign_v6.py","tests/test_polymarket_exact_header_campaign_v6.py",
    "scripts/launch_polymarket_oct1_stage_v5.py","tests/test_launch_polymarket_oct1_stage_v5.py",
    "scripts/prepare_polymarket_exact_header_campaign_v6.py","tests/test_prepare_polymarket_exact_header_campaign_v6.py",
    "docs/analysis_specs/polymarket_exact_header_reboot_recovery_v6.md"}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--preparation-request",type=Path,required=True)
    parser.add_argument("--preparation-request-sha256",required=True);parser.add_argument("--expected-head",required=True)
    args=parser.parse_args(argv);require_production_host()
    require(Path(sys.executable)==Path("/home/ubuntu/venv/bin/python"),"production_interpreter_required")
    resource.setrlimit(resource.RLIMIT_AS,(8*1024**3,8*1024**3));resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    resource.setrlimit(resource.RLIMIT_FSIZE,(m.MAX_LEDGER,m.MAX_LEDGER))
    require(signal.getitimer(signal.ITIMER_REAL)==(0.,0.),"existing_timer")
    def expired(*_):raise Blocked("preparation_runtime")
    signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,HARD_SECONDS)
    try:
        result=prepare(args.preparation_request,args.preparation_request_sha256,args.expected_head)
    except (m.Blocked,m.pilot.PilotBlocked,OSError,ValueError,KeyError,TypeError):
        print('{"status":"header_v6_preparation_blocked","provider_requests_sent":0,"producer_launched":false}');return 1
    finally:signal.setitimer(signal.ITIMER_REAL,0)
    print(encode(result).decode().strip());return 0


if __name__=="__main__":raise SystemExit(main())
