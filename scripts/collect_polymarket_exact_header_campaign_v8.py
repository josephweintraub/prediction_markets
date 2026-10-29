#!/usr/bin/env python3
"""400-method-item/s continuation of one fully drained, authenticated v6 prefix.

Only immutable sealed chunks are reused. Old failed/unsealed bytes and every
reservation remain charged. Raw RPC clocks do not admit the scientific sample.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
from datetime import datetime,timezone
import math
from pathlib import Path
import re
import subprocess
import sys
import time
REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from production_guard import require_production_host
from scripts import collect_polymarket_exact_header_campaign_v6 as previous
legacy,scale,pilot,inventory=previous.legacy,previous.scale,previous.pilot,previous.inventory
CAPS=previous.CAPS
SOURCE_FILES=(*previous.SOURCE_FILES,"scripts/collect_polymarket_exact_header_campaign_v8.py",
    "tests/test_polymarket_exact_header_campaign_v8.py")
SCHEMA="polymarket_exact_header_campaign_admission_v8"
SUCCESS,RUN_ROOT=previous.SUCCESS,previous.RUN_ROOT
MAX_JSON,MAX_LEDGER,FINAL_RESERVE=previous.MAX_JSON,previous.MAX_LEDGER,previous.FINAL_RESERVE
Blocked,Deadline,require=previous.Blocked,previous.Deadline,previous.require
encode,decode,digest=previous.encode,previous.decode,previous.digest
identity,bound,reference,read_json,ref=previous.identity,previous.bound,previous.reference,previous.read_json,previous.ref
Budget,owner_identity=previous.Budget,previous.owner_identity
integer=legacy.integer
key_chunks,read_headers,verify_chunk,journal_usage=previous.key_chunks,previous.read_headers,previous.verify_chunk,previous.journal_usage
request_deadline,http_response,process_limits=previous.request_deadline,previous.http_response,previous.process_limits
reboot_environment=previous.reboot_environment
environment_valid=previous.environment_valid
only_device=previous.only_device
TRANSIENT_RPC_CODES,RETRY_DELAYS,RATE_LIMIT_DELAYS=previous.TRANSIENT_RPC_CODES,previous.RETRY_DELAYS,previous.RATE_LIMIT_DELAYS
METHOD_ITEMS_PER_SECOND=400.0
FORECAST_RESERVE_SECONDS=previous.FORECAST_RESERVE_SECONDS
diagnostic=previous.previous.diagnostic
ACTUAL_KEYS=32901442
RETAINED_RUNTIME_FLOOR=104777
V6_HEAD="045deabcdb9b8a295680d1b6e3411b85fe80eeea"
V6_ROOT=RUN_ROOT/"2026-10-28_polymarket_exact_header_campaign_v6"
OP_ROOT=previous.previous.OP_ROOT
HANDOFF_PATH=OP_ROOT/"full_header_v6_stopped_handoff_v1.json"
RUN=RUN_ROOT/"2026-10-29_polymarket_exact_header_campaign_v8"
EXTERNAL_SCHEMA="polymarket_header_v8_external_control_ledger_v1"
V6_REFS={
    "owner":{"path":str(V6_ROOT/"OWNER.json"),"bytes":7242,"sha256":"528e86b929f146733b4410dd0485f090cf6ed0d2e1d6501d66520542d867c95e"},
    "admission":{"path":str(OP_ROOT/"full_header_v6_admission_v1.json"),"bytes":147605,"sha256":"fb7cb17a0a057fa6e80688e0904340632e2e5ee02ce0892ddb3b74de1a528ac4"},
    "launch":{"path":str(OP_ROOT/"full_header_v6.LAUNCH.json"),"bytes":4663,"sha256":"f676fe5ea444e76ff2690aeb0dd25c1f376f450fe4201a06630a21d8b9ad8f89"}}
EXIT_PATH=OP_ROOT/"full_header_v6.EXIT.json"
DRAIN_PATH=OP_ROOT/"full_header_v6_root_drain_v7_handoff_v1.json"
OLD_EXTERNAL_PATH=OP_ROOT/"header_v6_external_control_ledger_v1.json"
EXTERNAL_FUTURE_BYTES=786432
EXTERNAL_FUTURE_FILES=9
CERTIFICATE_PATH=OP_ROOT/"header_reboot_equivalence_v8_v1.json"
CERT_SCHEMA="polymarket_header_reboot_equivalence_v8"
CERT_STATUS="root_verified_same_volume_cold_boot"
CERT_KEYS={"schema_version","status","previous_certificate","old_v3_owner","old_v3_drain","old_v4_owner",
    "old_v4_drain","old_v6_owner","stopped_v6","expired_v7_admission","expired_v7_terminal",
    "current_environment","inputs","original_pilot_request","private_authority"}
OLD_V7_LEDGER={"path":str(OP_ROOT/"header_v7_external_control_ledger_v1.json"),"bytes":11369,
    "sha256":"7e0d3152ab363d87b580a5da8accb64131fa8a1c1b32660cb24727b31f6b43fd"}
OLD_V7_PROFILE={"ledger_files":58,"ledger_file_bytes":813845,"required_files":62,"required_bytes":981353,
    "next_chunk":102,"next_attempt":2,"reused_keys":10200000}
EXPIRED_V7_REFS={
    "admission":{"path":str(OP_ROOT/"full_header_v7_admission_v1.json"),"bytes":148155,"sha256":"63c84d4213f29340e9a389f18f718fdbdb909d7beb162b9ce6d9cf4cff135fbf"},
    "check":{"path":str(OP_ROOT/"full_header_v7_root_admission_check_v1.json"),"bytes":7358,"sha256":"70b99a522908012a29e364fc09f32ddf792d3df66efa4834342e9e711d1c6268"},
    "failure":{"path":str(OP_ROOT/"full_header_v7_root_dispatch_v1.json"),"bytes":626,"sha256":"5e4cb14b3c20ad4531c68ba6d958f19a8ccd62dfed061c5c9c9fba97bc5baf55"}}
EXPIRED_V7_TERMINAL={"path":str(OP_ROOT/"header_v7_unlaunched_terminal_v1.json"),"bytes":1965,
    "sha256":"01bfc18fd3e7d33d7dcbffac61d8815a1574d5e71ffb5925bff353609f9a73bf"}
STOPPED_V6={"path":str(HANDOFF_PATH),"bytes":97593,
    "sha256":"46019e7c0e812ba0259add42387ab85e967d7549edeb39c601931eac5c6587b5"}
FAILED_QA_HEAD="8542073491092e1acf35f83bca4639596002d494"
FAILED_QA1_REFS={
    "qa":{"path":str(OP_ROOT/"full_header_v8_linux_fixtures_v1.json"),"bytes":7309,
        "sha256":"a014633a765785f2ed737ac06dc5be8636e5cb5f8d9789e18206bf7576eaa9b0"},
    "log":{"path":str(OP_ROOT/"full_header_v8_linux_fixtures_v1.log"),"bytes":1176,
        "sha256":"9df14d7cae4c3561cf5c0b79fe356dd9df0ecaaa4eaa744c8700d0607579c6e1"}}
FAILED_QA_SOURCE_ROSTER_SHA256="719989bc8dce47db129a782ca86cd1e03fbd7e645f5c8a3ac64274eaf5af4682"
NEW_OP_NAMES={"header_v7_unlaunched_terminal_v1.json","header_reboot_equivalence_v8_v1.json",
    "header_v8_preparation_request_v1.json","full_header_v8_host_preflight_v1.json","full_header_v8_source_commit_v1.json",
    "full_header_v8_linux_fixtures_v1.json","full_header_v8_linux_fixtures_v1.log","header_v8_external_control_ledger_v1.json",
    "full_header_v8_linux_fixtures_v2.json","full_header_v8_linux_fixtures_v2.log",
    "full_header_v8_admission_v1.json","full_header_v8_root_admission_check_v1.json","full_header_v8.START.json",
    "full_header_v8.LAUNCH.json","full_header_v8.EXIT.json","full_header_v8.log","full_header_v8_root_dispatch_v1.json",
    "full_header_v8_prime_acceptance_v1.json","full_header_v8_terminal_acceptance_v1.json"}
EXTERNAL_SOURCE_FILES=set(SOURCE_FILES)|{
    "scripts/launch_polymarket_oct1_stage_v7.py","tests/test_launch_polymarket_oct1_stage_v7.py",
    "scripts/prepare_polymarket_exact_header_campaign_v8.py","tests/test_prepare_polymarket_exact_header_campaign_v8.py",
    "docs/analysis_specs/polymarket_exact_header_fresh_admission_v8.md"}
OLD_FUTURE_NAMES=("full_header_v6_admission_v1.json","full_header_v6_root_admission_check_v1.json",
    "full_header_v6.START.json","full_header_v6.LAUNCH.json","full_header_v6.EXIT.json","full_header_v6.log",
    "full_header_v6_root_dispatch_v1.json","full_header_v6_prime_acceptance_v1.json")



def source_snapshot(head):
    source=previous.source_snapshot(head);files=dict(source["files"]);captures={}
    for name in SOURCE_FILES[len(previous.SOURCE_FILES):]:
        value=bound(REPO/name,MAX_JSON);raw=scale.git_blob(head,name)
        require(len(raw)==value["bytes"] and digest(raw)==value["sha256"],"source_blob_differs")
        captures[name]=value;files[name]={key:value[key] for key in ("bytes","sha256")}
    require(previous.source_snapshot(head)==source,"source_changed_during_snapshot")
    for name,expected in files.items():
        current=bound(REPO/name,MAX_JSON)
        require({key:current[key] for key in ("bytes","sha256")}==expected,"source_final_content_changed")
    for name,value in captures.items():require(bound(REPO/name,MAX_JSON)==value,"source_reopen_changed")
    return {"head":head,"files":files}


def previous_source(source):
    return {"head":source["head"],"files":{name:source["files"][name] for name in previous.SOURCE_FILES}}


def validate_inputs(value,source,actual_keys):
    return previous.validate_inputs(value,previous_source(source),actual_keys)


def failed_qa1_proof():
    """Preserve failed evidence against its committed epoch, never today's files."""
    qa,qb=read_json(FAILED_QA1_REFS["qa"]);lb=reference(FAILED_QA1_REFS["log"])
    require(type(qa) is dict,"failed_qa1_schema")
    command=["/home/ubuntu/venv/bin/python","-m","unittest",*("tests."+name for name in (
        "test_polymarket_exact_header_campaign_v8","test_polymarket_exact_header_campaign_v6",
        "test_polymarket_exact_header_campaign_v5","test_polymarket_exact_header_campaign_v4",
        "test_polymarket_exact_header_campaign_v3","test_launch_polymarket_oct1_stage_v7",
        "test_prepare_polymarket_exact_header_campaign_v8"))]
    require(qa.get("schema_version")=="polymarket_header_v8_linux_fixture_receipt_v1"
        and qa.get("status")=="failed_closed" and qa.get("source_head")==FAILED_QA_HEAD
        and qa.get("source_clean_before") is True and qa.get("source_clean_after") is True
        and qa.get("command")==command and qa.get("interpreter")==command[0]
        and qa.get("log")==FAILED_QA1_REFS["log"] and qa.get("log_truncated") is False
        and qa.get("full_log_bytes")==lb["bytes"] and qa.get("full_log_sha256")==lb["sha256"],"failed_qa1_schema")
    for key,expected in (("exit_code",1),("failures",1),("errors",0),("tests_run",247),("tests_skipped",0),("provider_requests",0)):
        require(type(qa.get(key)) is int and qa[key]==expected,"failed_qa1_counts")
    files=qa.get("tested_source_files")
    require(type(files) is dict and len(files)==43
        and digest(encode(sorted(files)))==FAILED_QA_SOURCE_ROSTER_SHA256,"failed_qa1_source_roster")
    for name,expected in files.items():
        require(type(expected) is dict and set(expected)=={"bytes","sha256"}
            and type(expected["bytes"]) is int and 0<=expected["bytes"]<=MAX_JSON
            and type(expected["sha256"]) is str and pilot.SHA.fullmatch(expected["sha256"]),"failed_qa1_source_schema")
        raw=scale.git_blob(FAILED_QA_HEAD,name)
        require(len(raw)==expected["bytes"] and digest(raw)==expected["sha256"],"failed_qa1_source_epoch")
    for capture in (qb,lb):require(reference(ref(capture))==capture,"failed_qa1_late_change")
    return {"qa":qa,"captures":{"qa":qb,"log":lb}}


def required_carried_runtime(prior_value,producer_start_utc):
    """Keep stopped/expired preparation wall time inside the original72h cap."""
    require(type(producer_start_utc) in (int,float) and math.isfinite(producer_start_utc)
        and producer_start_utc>0,"producer_start_utc_invalid")
    require(prior_value["stopped_v6"]==STOPPED_V6,"handoff_path")
    handoff,_=read_json(STOPPED_V6,MAX_LEDGER)
    require(handoff["schema_version"]=="polymarket_header_v6_stopped_handoff_v1"
        and handoff["status"]=="root_bound_stopped_v6_header_prefix" and handoff["run_dir"]==str(V6_ROOT)
        and handoff["source_head"]==V6_HEAD and Path(handoff["refs"]["drain"]["path"])==DRAIN_PATH,"runtime_handoff_differs")
    drain,_=read_json(handoff["refs"]["drain"],MAX_LEDGER)
    checked=datetime.fromisoformat(drain["checked_utc"])
    require(type(drain["checked_utc"]) is str and checked.tzinfo is not None
        and checked.utcoffset()==timezone.utc.utcoffset(None) and checked.isoformat()==drain["checked_utc"]
        and drain["schema_version"]=="polymarket_header_v6_root_drain_v7_handoff_v1"
        and drain["status"]=="root_verified_stopped_header_v6" and drain["run_dir"]==str(V6_ROOT)
        and drain["source_head"]==V6_HEAD and producer_start_utc>=checked.timestamp(),"runtime_drain_utc_invalid")
    return max(RETAINED_RUNTIME_FLOOR,math.ceil(legacy.elapsed(prior_value["base"]["rpc"]["elapsed_seconds"])
        +legacy.elapsed(handoff["runtime_charge_seconds"])+producer_start_utc-checked.timestamp()))


def expired_v7_proof():
    ledger,lb=read_json(OLD_V7_LEDGER,MAX_LEDGER)
    require(type(ledger) is dict and set(ledger)=={"schema_version","files","future_reserved_bytes","future_reserved_files","ceiling_bytes"}
        and ledger["schema_version"]=="polymarket_header_v7_external_control_ledger_v1"
        and len(ledger["files"])==OLD_V7_PROFILE["ledger_files"]
        and sum(item["bytes"] for item in ledger["files"])==OLD_V7_PROFILE["ledger_file_bytes"]
        and ledger["future_reserved_bytes"]==1245184 and ledger["future_reserved_files"]==9
        and ledger["ceiling_bytes"]==2097152,"old_v7_ledger_profile")
    required={}
    for item in [*ledger["files"],OLD_V7_LEDGER,*EXPIRED_V7_REFS.values()]:
        require(type(item) is dict and set(item)=={"path","bytes","sha256"} and item["path"] not in required,
            "old_v7_required_ref_schema")
        required[item["path"]]=reference(item,MAX_LEDGER)
    require(len(required)==OLD_V7_PROFILE["required_files"]
        and sum(item["bytes"] for item in required.values())==OLD_V7_PROFILE["required_bytes"],"old_v7_required_union")
    admission=read_json(EXPIRED_V7_REFS["admission"],MAX_LEDGER)[0]
    checked=read_json(EXPIRED_V7_REFS["check"],MAX_LEDGER)[0]
    failed=read_json(EXPIRED_V7_REFS["failure"])[0]
    require(admission["schema_version"]=="polymarket_exact_header_campaign_admission_v7"
        and admission["status"]=="root_exact_header_campaign_admitted"
        and admission["consumer_head"]=="dcd2a8c1b8188d922b747b313a6c8ecdb931279b"
        and admission["run_dir"]==str(RUN_ROOT/"2026-10-29_polymarket_exact_header_campaign_v7")
        and admission["caps"]==CAPS and admission["actual_keys"]==ACTUAL_KEYS
        and admission["raw_clocks_only"] is True and admission["final_sample_admitted"] is False,"expired_v7_admission")
    require(checked["schema_version"]=="polymarket_header_v7_preparation_check_v1"
        and checked["status"]=="header_only_admission_prepared_not_launched"
        and checked["admission"]==EXPIRED_V7_REFS["admission"] and checked["external_ledger"]==OLD_V7_LEDGER
        and checked["prior_rpc"]==admission["prior"]["rpc"] and checked["next_chunk"]==OLD_V7_PROFILE["next_chunk"]
        and checked["next_attempt"]==OLD_V7_PROFILE["next_attempt"] and checked["reused_keys"]==OLD_V7_PROFILE["reused_keys"]
        and checked["remaining_keys"]+checked["reused_keys"]==ACTUAL_KEYS and checked["method_items_per_second"]==400,
        "expired_v7_check")
    require(failed["schema_version"]=="polymarket_header_v7_root_dispatch_v1"
        and failed["status"]=="root_dispatch_failed_requires_investigation" and failed["error_code"]=="private_identity_changed"
        and failed["source_head"]==admission["consumer_head"] and failed["constructor_entered"] is False
        and failed["ownership_unknown"] is False and failed["caller_may_be_running"] is False,"expired_v7_failure")
    terminal,tb=read_json(EXPIRED_V7_TERMINAL)
    require(terminal["schema_version"]=="polymarket_header_v7_unlaunched_terminal_local_checkpoint_v1"
        and terminal["status"]=="unlaunched_expired_admission_host_stopped_monitor_paused"
        and terminal["checked_utc"]=="2026-10-29T18:32:07Z" and terminal["source_head"]==admission["consumer_head"]
        and terminal["run_start_launch_exit_log_all_absent"] is True
        and type(terminal["new_provider_requests"]) is int and terminal["new_provider_requests"]==0
        and type(terminal["new_collector_launches"]) is int and terminal["new_collector_launches"]==0
        and terminal["AWS_independently_verified_state"]=="stopped" and terminal["saved_process_receipts_checked"]==603
        and terminal["unknown_consumers"]==0 and terminal["final_sample_admitted"] is False,"expired_v7_terminal")
    return {"admission":admission,"required":required,"terminal_binding":tb,"ledger_binding":lb}


def certificate_value(inputs,private_ref):
    """Candidate only; no write, environment override or validation bypass."""
    owner,_=read_json(V6_REFS["owner"]);old_ref=ref(owner["reboot_equivalence"])
    six,_=read_json(old_ref);environment=reboot_environment();environment_valid(environment)
    _,request=pilot.read_requests(Path(legacy.ORIGINAL_PILOT_INPUT["path"]),legacy.ORIGINAL_PILOT_INPUT["sha256"])
    require(request["keys"]==1000,"original_request_key_count")
    expired_v7_proof();reference(STOPPED_V6,MAX_LEDGER)
    return {"schema_version":CERT_SCHEMA,"status":CERT_STATUS,"previous_certificate":old_ref,
        **{key:six[key] for key in ("old_v3_owner","old_v3_drain","old_v4_owner","old_v4_drain")},
        "old_v6_owner":V6_REFS["owner"],"stopped_v6":STOPPED_V6,"expired_v7_admission":EXPIRED_V7_REFS["admission"],
        "expired_v7_terminal":EXPIRED_V7_TERMINAL,"current_environment":environment,
        "inputs":{name:{"v3":pair["v3"],"v4":pair["v4"],"v6":pair["current"],"current":inputs[name]}
            for name,pair in six["inputs"].items()},
        "original_pilot_request":{"reference":six["original_pilot_request"]["reference"],
            **{key:six["original_pilot_request"][key] for key in ("v3","v4")},
            "v6":six["original_pilot_request"]["current"],"current":request},
        "private_authority":{"reference":private_ref,**{key:six["private_authority"][key] for key in ("original","v4")},
            "v6":six["private_authority"]["current"],"current":pilot.identity(Path(private_ref["path"])),"root_uuid":previous.ROOT_UUID}}



def validate_reboot(certificate_ref,prior_value,inputs,private_ref):
    require(type(prior_value) is dict and set(prior_value)=={"base","stopped_v6","rpc","external_controls"},"prior_v8_schema")
    require(Path(certificate_ref["path"])==CERTIFICATE_PATH,"cold_certificate_path")
    value,binding=read_json(certificate_ref)
    require(type(value) is dict and set(value)==CERT_KEYS and value["schema_version"]==CERT_SCHEMA
        and value["status"]==CERT_STATUS and value["old_v6_owner"]==V6_REFS["owner"]
        and value["stopped_v6"]==prior_value["stopped_v6"]==STOPPED_V6 and value["expired_v7_admission"]==EXPIRED_V7_REFS["admission"]
        and value["expired_v7_terminal"]==EXPIRED_V7_TERMINAL,"cold_certificate_schema")
    owner6,ob6=read_json(V6_REFS["owner"])
    six_ref=ref(owner6["reboot_equivalence"])
    require(value["previous_certificate"]==six_ref,"cold_previous_certificate_ref")
    six,six_binding=read_json(six_ref)
    require(type(six) is dict and set(six)==previous.CERT_KEYS and six["schema_version"]==previous.CERT_SCHEMA
        and six["status"]==previous.CERT_STATUS and six["previous_certificate"]==previous.OLD_CERTIFICATE,
        "historical_v6_certificate_schema")
    old,old_binding=read_json(previous.OLD_CERTIFICATE)
    require(set(old)=={"schema_version","status","old_boot_id","current_boot_id","data_volume","old_owner","old_drain",
        "inputs","private_authority","original_pilot_request"} and old["schema_version"]=="polymarket_header_reboot_equivalence_v4"
        and old["status"]=="root_verified_same_volume_device_reassignment" and old["old_boot_id"]==previous.previous.previous.OLD_BOOT_ID
        and old["current_boot_id"]==previous.previous.previous.CURRENT_BOOT_ID,"historical_certificate_schema")
    require(old["data_volume"]=={"uuid":previous.DATA_UUID,"serial":previous.DATA_SERIAL,"device_bytes":750*1024**3,
        "old_device":previous.previous.previous.OLD_DATA_DEVICE,"current_device":previous.previous.previous.CURRENT_DATA_DEVICE},
        "historical_volume_differs")
    environment=reboot_environment();environment_valid(environment);environment_valid(six["current_environment"])
    require(value["current_environment"]==environment and environment["boot_id"]!=six["current_environment"]["boot_id"]
        and owner6["process"]["boot_id"]==six["current_environment"]["boot_id"],"cold_boot_or_mount_differs")
    require(value["old_v3_owner"]==six["old_v3_owner"]==old["old_owner"] and
        value["old_v3_drain"]==six["old_v3_drain"]==old["old_drain"] and
        value["old_v4_owner"]==six["old_v4_owner"]==previous.V4_REFS["owner"] and
        value["old_v4_drain"]==six["old_v4_drain"]==previous.V4_REFS["root_drain"],"historical_proof_refs_differs")
    owner3,ob3=read_json(value["old_v3_owner"]);drain3,db3=read_json(value["old_v3_drain"])
    owner4,ob4=read_json(previous.V4_REFS["owner"]);drain4,db4=read_json(previous.V4_REFS["root_drain"])
    require(owner3.get("schema_version")=="polymarket_exact_header_owner_v3" and
        owner3.get("source",{}).get("head")==previous.previous.previous.PRIOR_SOURCE_HEAD and
        owner3.get("process",{}).get("boot_id")==old["old_boot_id"] and
        drain3.get("owner_sha256")==ob3["sha256"] and drain3.get("run_dir")==owner3.get("run_dir") and
        owner4.get("schema_version")=="polymarket_exact_header_owner_v4" and
        owner4.get("source",{}).get("head")==previous.previous.V4_HEAD and
        owner4.get("process",{}).get("boot_id")==old["current_boot_id"] and
        drain4.get("header_owned_processes_drained") is True,"historical_owner_or_drain_differs")
    require(owner6.get("schema_version")=="polymarket_exact_header_owner_v6" and owner6.get("run_dir")==str(V6_ROOT)
        and owner6.get("source",{}).get("head")==V6_HEAD and owner6.get("caps")==CAPS,"historical_v6_owner_differs")
    require(all(legacy.owner_state(owner["process"])=="dead" for owner in (owner3,owner4,owner6)),"historical_owner_live_or_unknown")
    base=prior_value["base"]
    require(base.get("campaigns")==[{"run_dir":owner3["run_dir"],"drain":value["old_v3_drain"]}],"historical_campaign_reference_differs")
    require(type(value["inputs"]) is dict and set(value["inputs"])==set(inputs)==set(old["inputs"])
        ==set(six["inputs"]) and len(inputs)==9,"reboot_input_roster_differs")
    v3_inputs={};v4_inputs={};v6_inputs={}
    for name,current in inputs.items():
        pair=value["inputs"][name];prior=six["inputs"][name]
        require(type(pair) is dict and set(pair)=={"v3","v4","v6","current"} and
            pair["v3"]==prior["v3"]==old["inputs"][name]["old"] and
            pair["v4"]==prior["v4"]==old["inputs"][name]["current"] and pair["v6"]==prior["current"]
            and pair["current"]==current and ref(pair["v3"])==ref(pair["v4"])==ref(pair["v6"])==ref(current),
            "cold_input_binding_differs")
        only_device(pair["v3"]["identity"],pair["v4"]["identity"],previous.previous.previous.OLD_DATA_DEVICE,
            previous.previous.previous.CURRENT_DATA_DEVICE)
        only_device(pair["v4"]["identity"],pair["v6"]["identity"],previous.previous.previous.CURRENT_DATA_DEVICE,six["current_environment"]["data_device"])
        only_device(pair["v6"]["identity"],current["identity"],six["current_environment"]["data_device"],environment["data_device"])
        require(reference(ref(current),CAPS["baseline_bytes"])==current,"cold_input_rehash_differs")
        v3_inputs[name]=pair["v3"];v4_inputs[name]=pair["v4"];v6_inputs[name]=pair["v6"]
    require(owner3.get("inputs")==v3_inputs and owner4.get("inputs")==v4_inputs and owner6.get("inputs")==v6_inputs,
        "historical_owner_inputs_differs")
    original=value["original_pilot_request"];prior=six["original_pilot_request"]
    require(type(original) is dict and set(original)=={"reference","v3","v4","v6","current"}
        and original["reference"]==legacy.ORIGINAL_PILOT_INPUT==old["original_pilot_request"]["reference"]==prior["reference"]
        and base.get("original_pilot_input")==original["reference"] and original["v3"]==prior["v3"]==old["original_pilot_request"]["old"]
        and original["v4"]==prior["v4"]==old["original_pilot_request"]["current"] and original["v6"]==prior["current"],
        "cold_original_reference_differs")
    original_reference=reference(original["reference"],MAX_JSON)
    keys,request=pilot.read_requests(Path(original_reference["path"]),original_reference["sha256"])
    require(len(keys)==1000 and original["current"]==request,"cold_original_current_differs")
    contract=read_json(ref(inputs["scale_contract"]))[0]
    require(original["v3"]==contract.get("request") and type(request.get("keys")) is int and request["keys"]==1000 and
        all(original["v3"][key]==original["v4"][key]==original["v6"][key]==request[key]
            for key in ("sha256","keys","normalized_keys_sha256")),"cold_original_content_differs")
    only_device(original["v3"]["identity"],original["v4"]["identity"],previous.previous.previous.OLD_DATA_DEVICE,previous.previous.previous.CURRENT_DATA_DEVICE)
    only_device(original["v4"]["identity"],original["v6"]["identity"],previous.previous.previous.CURRENT_DATA_DEVICE,six["current_environment"]["data_device"])
    only_device(original["v6"]["identity"],request["identity"],six["current_environment"]["data_device"],environment["data_device"])
    authority=value["private_authority"];prior=six["private_authority"]
    require(type(authority) is dict and set(authority)=={"reference","original","v4","v6","current","root_uuid"}
        and authority["reference"]==private_ref==prior["reference"]==old["private_authority"]["reference"]
        and authority["root_uuid"]==previous.ROOT_UUID==prior["root_uuid"]
        and authority["original"]==prior["original"]==old["private_authority"]["old"]==contract.get("rpc_authority_private_file_identity")
        and authority["v4"]==prior["v4"]==old["private_authority"]["current"] and authority["v6"]==prior["current"],
        "cold_private_reference_differs")
    actual_private=reference(private_ref);current_private=pilot.identity(Path(private_ref["path"]))
    require(authority["current"]==current_private and len(current_private)==9,"cold_private_current_differs")
    only_device(authority["original"],authority["v4"],previous.previous.previous.OLD_PRIVATE_DEVICE,previous.previous.previous.CURRENT_PRIVATE_DEVICE)
    only_device(authority["v4"],authority["v6"],previous.previous.previous.CURRENT_PRIVATE_DEVICE,six["current_environment"]["root_device"])
    only_device(authority["v6"],current_private,six["current_environment"]["root_device"],environment["root_device"])
    owner3_admission=read_json(ref(owner3["admission"]),MAX_LEDGER)[0]
    require(owner3_admission.get("private_file")==private_ref,"historical_private_reference_differs")
    only_device(owner6["reboot_equivalence"]["identity"],six_binding["identity"],six["current_environment"]["data_device"],environment["data_device"])
    files={item["path"]:item for item in base["files"]};require(len(files)==len(base["files"]),"prior_duplicate")
    for capture in (six_binding,old_binding,ob3,db3,ob4,db4):
        require(capture["path"] in files and ref(files[capture["path"]])==ref(capture)
            and files[capture["path"]]["category"]=="control" and files[capture["path"]]["chunk"] is None,"cold_old_proof_not_rostered")
    require(base["standalone_control_files"].count(six_ref["path"])==1,"historical_v6_certificate_not_exact_once")
    external=read_json(prior_value["external_controls"],MAX_LEDGER)[0]
    require(sum(item==ref(binding) for item in external["files"])==1,"cold_certificate_not_new_ledger")
    expired=expired_v7_proof()
    require(expired["admission"]["inputs"]=={name:ref(item) for name,item in inputs.items()}
        and expired["admission"]["private_file"]==private_ref and expired["admission"]["prior"]["base"]==base
        and expired["admission"]["prior"]["stopped_v6"]==value["stopped_v6"],"expired_v7_certificate_lineage")
    for capture in (binding,six_binding,old_binding,ob3,db3,ob4,db4,ob6,original_reference,actual_private):
        require(reference(ref(capture),MAX_LEDGER)==capture,"cold_proof_late_change")
    require(all(identity(Path(item["path"]))==item["identity"] for item in inputs.values())
        and pilot.identity(Path(private_ref["path"]))==current_private,"cold_current_identity_late_change")
    return {"binding":binding,"old_certificate_binding":six_binding,"historical_v6_binding":owner6["reboot_equivalence"],
        "old_inputs":v3_inputs,"historical_v4_inputs":v4_inputs,"historical_v6_inputs":v6_inputs,
        "private_binding":actual_private,"private_current":current_private,"environment":environment,
        "original_request":{"old":original["v3"],"current":request},"original_reference":original_reference}



def forecast_seconds(keys,batch_size,observed_max):
    require(type(keys) is int and 0<=keys<=CAPS["keys"] and type(batch_size) is int and
        0<batch_size<=CAPS["initial_batch_size"],"forecast_key_or_batch_invalid")
    legacy.elapsed(observed_max,CAPS["request_seconds"])
    return math.ceil(keys/batch_size)*max(batch_size/METHOD_ITEMS_PER_SECOND,observed_max)+FORECAST_RESERVE_SECONDS


def wait_for_pace(budget):
    delay=max(0,budget.next_dispatch_at-time.monotonic())
    if delay:
        require(budget.remaining()>delay,"runtime_limit")
        if budget.chunk is not None:
            require(budget.chunk_elapsed()+delay<CAPS["chunk_runtime_seconds"],"chunk_runtime_limit")
        time.sleep(delay);budget.check()


def paced_start(budget,items):
    """Mark the actual dispatch, after any source checks and journal fsync."""
    now=time.monotonic()
    budget.next_dispatch_at=max(now,budget.next_dispatch_at)+items/METHOD_ITEMS_PER_SECOND


def remaining_key_count(total,prior,keys_path):
    accepted=0
    for chunk in prior["sealed"]:
        require(type(chunk) is int and 0<=chunk*CAPS["keys_per_chunk"]<total,"sealed_chunk_outside_inventory")
        width=len(legacy.key_chunk_at(keys_path,chunk))
        require(width==min(CAPS["keys_per_chunk"],total-chunk*CAPS["keys_per_chunk"]),"sealed_chunk_width_differs")
        accepted+=width
    require(0<=accepted<=total,"sealed_coverage_exceeds_inventory")
    return total-accepted


def required_new_runtime(keys,measured,prior):
    return max(2*measured["seconds_per_key"]*keys,
        prior["observation_elapsed_seconds"]*math.ceil(keys/CAPS["initial_batch_size"]),
        forecast_seconds(keys,CAPS["initial_batch_size"],prior["observed_max_call_seconds"]))


def stopped_journal_usage(path, key_context=None):
    """Parse a complete durable prefix; retain and conservatively debit its tail."""
    import io
    capture=bound(Path(path),CAPS["chunk_artifact_bytes"])
    data=Path(path).read_bytes(); require(bound(Path(path),CAPS["chunk_artifact_bytes"])==capture,"stopped_journal_changed")
    split=data.rfind(b"\n")+1; complete,tail=data[:split],data[split:]
    require(len(tail)<CAPS["journal_record_bytes"],"partial_journal_tail_limit")
    starts, finishes, last, last_time = {}, {}, 0, 0
    with io.BytesIO(complete) as stream:
        for raw in stream:
            require(len(raw) <= CAPS["journal_record_bytes"] and raw.endswith(b"\n"), "journal_record_invalid")
            item = decode(raw); call = item.get("i")
            require(type(call) is int and 0 < call <= CAPS["http_attempts"], "journal_call_invalid")
            if item.get("e") == "s":
                require(set(item) == {"e", "i", "k", "n", "j", "b", "t", "h"}, "journal_start_schema")
                require(call not in starts and call > last and item.get("n", -1) >= 0, "journal_start_invalid")
                require(not starts or last in finishes, "journal_overlapping_serial_requests")
                integer(item.get("b"), CAPS["individual_response_bytes"])
                integer(item.get("n"), CAPS["initial_batch_size"])
                require(item["b"] > 0 and type(item["t"]) is int and item["t"] >= last_time
                        and type(item["j"]) is int and 0 < item["j"] <= CAPS["method_items"]
                        and type(item["h"]) is str and pilot.SHA.fullmatch(item["h"]), "journal_start_invalid")
                if item["n"] == 0:
                    require(item["k"] == -1 and item["h"] == digest(encode({"jsonrpc": "2.0", "id": item["j"],
                        "method": "eth_chainId", "params": []})), "journal_chain_request_differs")
                else:
                    require(type(item["k"]) is int and 0 <= item["k"] < CAPS["keys"] and
                            item["k"] + item["n"] <= CAPS["keys"] and item["j"] + item["n"] - 1 <= CAPS["method_items"],
                            "journal_key_locator_invalid")
                    if key_context is not None:
                        offset, keys = key_context
                        require(offset <= item["k"] and item["k"] + item["n"] <= offset + len(keys), "journal_key_locator_invalid")
                        requests = [{"jsonrpc": "2.0", "id": item["j"] + index, "method": "eth_getBlockByNumber",
                                     "params": [hex(key["block_number"]), False]}
                                    for index, key in enumerate(keys[item["k"] - offset:item["k"] - offset + item["n"]])]
                        require(item["h"] == digest(encode(requests)), "journal_request_digest_differs")
                starts[call], last = item, call
            else:
                require(item.get("e") == "f" and call in starts and call not in finishes, "journal_finish_invalid")
                require(set(item) == {"e", "i", "b", "z", "h", "x", "p", "r", "s", "t"}, "journal_finish_schema")
                integer(item.get("b"), starts[call]["b"])
                integer(item.get("z"), item["b"])
                require(item["r"] in {"validated", "failed", "split"} and type(item["t"]) is int
                        and item["t"] >= starts[call]["t"] and (item["s"] is None or
                        type(item["s"]) is int and 100 <= item["s"] <= 599)
                        and type(item["x"]) is str and pilot.SHA.fullmatch(item["x"]), "journal_finish_invalid")
                if item["h"] is None:
                    require(item["b"] == starts[call]["b"] and item["r"] != "validated", "journal_unknown_body_charge_refund")
                else:
                    require(type(item["h"]) is str and pilot.SHA.fullmatch(item["h"]) and
                            item["h"] == item["x"] and item["b"] == item["z"], "journal_complete_body_proof_invalid")
                if item["r"] == "validated":
                    require(item["s"] == 200 and (item["p"] is None if starts[call]["n"] == 0 else
                        type(item["p"]) is str and pilot.SHA.fullmatch(item["p"])), "journal_validated_projection_invalid")
                else:
                    require(item["p"] is None and (item["r"] != "split" or starts[call]["n"] > 1), "journal_failed_projection_invalid")
                finishes[call] = item
            last_time = item["t"]
    return {"http_attempts": len(starts), "method_items": sum(max(1, item["n"]) for item in starts.values()),
            "response_bytes": sum(finishes[call]["b"] if call in finishes else item["b"] for call, item in starts.items())}, starts, finishes, tail


def validate_prior(value,output,source,inputs,reboot):
    """Admit only the fixed stopped-v6 tree, never its unsealed headers."""
    require(type(value) is dict and set(value)=={"base","stopped_v6","rpc","external_controls"},"prior_v8_schema")
    require(value["stopped_v6"]==STOPPED_V6 and Path(value["stopped_v6"]["path"])==HANDOFF_PATH,"handoff_path")
    require(set(inputs)==set(reboot["historical_v6_inputs"]) and all(ref(inputs[name])==ref(old)
        for name,old in reboot["historical_v6_inputs"].items()),"historical_v6_projection_not_certified")
    expired=expired_v7_proof();failed_qa=failed_qa1_proof()
    handoff,hb=read_json(value["stopped_v6"],MAX_LEDGER)
    require(type(handoff) is dict and set(handoff)=={"schema_version","status","run_dir","source_head","refs","files",
        "runtime_charge_seconds","chunk_runtime_seconds","raw_clocks_only","final_sample_admitted"} and
        handoff["schema_version"]=="polymarket_header_v6_stopped_handoff_v1" and
        handoff["status"]=="root_bound_stopped_v6_header_prefix" and handoff["run_dir"]==str(V6_ROOT) and
        handoff["source_head"]==V6_HEAD and handoff["raw_clocks_only"] is True and handoff["final_sample_admitted"] is False,
        "handoff_schema")
    refs=handoff["refs"]
    require(type(refs) is dict and set(refs)=={"owner","admission","launch","exit","drain"} and
        all(refs[key]==expected for key,expected in V6_REFS.items()) and
        Path(refs["exit"]["path"])==EXIT_PATH and Path(refs["drain"]["path"])==DRAIN_PATH,"v6_proof_refs")
    captures={key:reference(item,MAX_LEDGER) for key,item in refs.items()}
    owner,old_admission,launch,exited,drain=(read_json(refs[key],MAX_LEDGER)[0] for key in ("owner","admission","launch","exit","drain"))
    require(old_admission.get("schema_version")==previous.SCHEMA and old_admission.get("consumer_head")==V6_HEAD and
        old_admission.get("run_dir")==str(V6_ROOT) and old_admission.get("caps")==CAPS and
        old_admission.get("actual_keys")==ACTUAL_KEYS and old_admission.get("prior")==value["base"] and
        old_admission.get("inputs")=={name:ref(item) for name,item in inputs.items()} and
        old_admission.get("raw_clocks_only") is True and old_admission.get("final_sample_admitted") is False,
        "v6_admission_changed")
    require(owner.get("schema_version")=="polymarket_exact_header_owner_v6" and owner.get("run_dir")==str(V6_ROOT) and
        owner.get("source",{}).get("head")==V6_HEAD and owner.get("caps")==CAPS and owner.get("inputs")==reboot["historical_v6_inputs"] and
        ref(owner.get("admission",{}))==refs["admission"] and owner.get("raw_clocks_only") is True and
        owner.get("final_sample_admitted") is False and owner.get("reboot_equivalence")==reboot["historical_v6_binding"],"v6_owner_changed")
    legacy.producer_source(owner["source"],previous_source(source),previous.SOURCE_FILES)
    require(legacy.owner_state(owner.get("process",{}))=="dead","v6_owner_live_or_unknown")
    require(launch.get("schema_version")=="polymarket_oct1_stage_launcher_v5" and launch.get("stage")=="headers" and
        launch.get("status")=="producer_dispatched_owned_session" and launch.get("source",{}).get("head")==V6_HEAD and
        launch.get("output_dir")==str(V6_ROOT) and ref(launch.get("admission",{}))==refs["admission"] and
        launch.get("ownership_unknown") is False and launch.get("retry_or_resume") is False,"v6_launch_changed")
    terminal=(exited.get("status")=="producer_exited" and exited.get("error_code") is None and
        type(exited.get("producer_exit_code")) is int and exited["producer_exit_code"]!=0) or (
        exited.get("status")=="caller_failed_closed" and exited.get("error_code")=="caller_interrupted" and
        type(exited.get("producer_exit_code")) is int and exited["producer_exit_code"]==-15)
    require(exited.get("schema_version")==launch["schema_version"] and exited.get("stage")=="headers" and
        terminal and exited.get("output_dir")==str(V6_ROOT) and
        all(exited.get(key)==launch.get(key) for key in ("source","admission","command","caller","child")) and
        exited.get("owned_group_drained") is True and exited.get("ownership_unknown") is False and
        exited.get("children_still_running") is False and exited.get("stdout_eof_observed") is True and
        exited.get("source_and_admission_unchanged") is True,"v6_exit_not_drained")
    for role in ("caller","child"):
        p=exited.get(role,{})
        require(type(p) is dict and set(p)=={"hostname","boot_id","pid","birth_ticks","process_group","session"} and
            type(p["pid"]) is int and p["pid"]==p["process_group"]==p["session"] and
            legacy.owner_state({key:p[key] for key in ("hostname","boot_id","pid","birth_ticks")})=="dead",
            "v6_caller_live_or_unknown")
    require({key:exited["child"][key] for key in owner["process"]}==owner["process"],"v6_owner_child_differs")
    require(type(drain) is dict and set(drain)=={"schema_version","status","run_dir","source_head","owner","launch","caller_exit",
        "header_owned_processes_drained","remaining_owned_group_members","no_owned_workers_or_transfers","host_idle_claimed",
        "final_sample_admitted","checked_utc","external_control_closure"} and drain["schema_version"]=="polymarket_header_v6_root_drain_v7_handoff_v1" and
        drain["status"]=="root_verified_stopped_header_v6" and drain["run_dir"]==str(V6_ROOT) and drain["source_head"]==V6_HEAD and
        drain["owner"]==refs["owner"] and drain["launch"]==refs["launch"] and drain["caller_exit"]==refs["exit"] and
        drain["header_owned_processes_drained"] is True and drain["remaining_owned_group_members"]==[] and
        drain["no_owned_workers_or_transfers"] is True and drain["host_idle_claimed"] is False and
        drain["final_sample_admitted"] is False,"v6_root_drain_invalid")
    checked=datetime.fromisoformat(drain["checked_utc"])
    require(type(drain["checked_utc"]) is str and checked.tzinfo is not None and
        checked.utcoffset()==timezone.utc.utcoffset(None) and checked.isoformat()==drain["checked_utc"],"v6_drain_utc_invalid")
    prior=deepcopy(previous.validate_prior(value["base"],output,previous_source(source),inputs,reboot))
    historical_bytes=dict(prior["bytes"]);published_retained=None
    require(V6_ROOT.resolve()==V6_ROOT and V6_ROOT.parent==RUN_ROOT and V6_ROOT!=output and
        not (V6_ROOT/"COMPLETE.json").exists(),"v6_prefix_not_stopped_partial")
    entries=handoff["files"]
    require(type(entries) is list and 0<len(entries)<=legacy.MAX_FILES,"v6_file_roster_limit")
    actual=set()
    for path in V6_ROOT.rglob("*"):
        require(not path.is_symlink() and (path.is_file() or path.is_dir()),"v6_tree_type")
        if path.is_file():actual.add(str(path))
        require(len(actual)<=legacy.MAX_FILES,"v6_file_roster_limit")
    files={}
    for item in entries:
        require(type(item) is dict and set(item)=={"path","bytes","sha256","category","chunk"} and
            item["path"] in actual and item["path"] not in files and item["path"] not in prior["files"],"v6_file_roster_invalid")
        path=Path(item["path"]);parts=path.relative_to(V6_ROOT).parts
        if len(parts)==1:
            require(parts[0] in {"OWNER.json","rpc.jsonl","FAILED.json","manifest.json","artifact_inventory.json",
                ".COMPLETE.json.pending"} and item["chunk"] is None and item["category"]=="control","v6_root_artifact_invalid")
        else:
            require(len(parts)==4 and parts[0]=="chunks" and re.fullmatch(r"c[0-9]{6}",parts[1]) and
                re.fullmatch(r"a0[1-3]",parts[2]) and type(item["chunk"]) is int and item["chunk"]==int(parts[1][1:]) and
                (parts[3] in {"rpc.jsonl","headers.parquet","manifest.json","COMPLETE.json",".COMPLETE.json.pending"} or
                 re.fullmatch(r"error_r[0-9]{9}\.json",parts[3])),"v6_chunk_artifact_invalid")
            expected="accepted" if parts[3]=="headers.parquet" and (path.parent/"COMPLETE.json").is_file() else "failed" if parts[3]=="headers.parquet" else "control"
            require(item["category"]==expected,"v6_artifact_category")
        files[item["path"]]=reference(ref(item),CAPS["global_header_bytes"])|{"category":item["category"],"chunk":item["chunk"]}
    require(set(files)==actual and refs["owner"]["path"] in files and ref(files[refs["owner"]["path"]])==refs["owner"],"v6_file_roster_incomplete")
    totals=dict.fromkeys(("http_attempts","method_items","response_bytes"),0);calls={};chunks=set();last=None;seal_edges={}
    times=handoff["chunk_runtime_seconds"]
    require(type(times) is dict,"v6_chunk_runtime_schema")
    for path,item in sorted(files.items()):
        if Path(path).name!="rpc.jsonl":continue
        chunk=item["chunk"]
        keys=legacy.key_chunk_at(inputs["keys"]["path"],chunk) if chunk is not None else None
        usage,starts,finishes,tail=stopped_journal_usage(path,(chunk*CAPS["keys_per_chunk"],keys) if keys is not None else None)
        for call,start in starts.items():
            require(call not in calls,"v6_duplicate_call")
            calls[call]=start
        if tail:
            # A torn record cannot justify a refund or new accepted coverage.
            usage={key:usage[key]+extra for key,extra in (("http_attempts",1),("method_items",CAPS["initial_batch_size"]),
                ("response_bytes",CAPS["individual_response_bytes"]))}
        for key in totals:totals[key]+=usage[key]
        if chunk is None:continue
        require(chunk not in chunks,"v6_multiple_attempts_same_chunk")
        chunks.add(chunk);attempt=str(Path(path).parent)
        require(Path(attempt).name==f"a{len(prior['attempts'].get(chunk,set()))+1:02d}","v6_attempt_sequence_differs")
        prior["attempts"].setdefault(chunk,set()).add(attempt)
        span=(max([r["t"] for r in (*starts.values(),*finishes.values())])-min(r["t"] for r in starts.values()))/1000 if starts else 0
        require(str(chunk) in times and legacy.elapsed(times[str(chunk)],CAPS["chunk_runtime_seconds"])>=span,"v6_chunk_runtime_understated")
        agg=prior["chunk_usage"].setdefault(chunk,dict.fromkeys((*totals,"elapsed_seconds"),0))
        for key,count in usage.items():agg[key]+=count
        agg["elapsed_seconds"]+=times[str(chunk)]
        marker=Path(path).parent/"COMPLETE.json"
        if marker.is_file():
            require(chunk not in prior["sealed"] and not tail,"v6_duplicate_or_torn_seal")
            rows,_=verify_chunk(marker.parent,keys,chunk*CAPS["keys_per_chunk"])
            seal_edges[chunk]=(rows[0],rows[-1])
            manifest=read_json(ref(files[str(marker.parent/"manifest.json")]))[0]
            require(agg["elapsed_seconds"]>=manifest["active_seconds_charged"],"v6_seal_runtime_understated")
            prior["sealed"][chunk]={"root":str(marker.parent),"journal":path,"source_root":str(V6_ROOT)}
    require(set(times)=={str(chunk) for chunk in chunks},"v6_chunk_runtime_roster")
    charge=legacy.elapsed(handoff["runtime_charge_seconds"])
    require(charge>=legacy.elapsed(exited.get("elapsed_seconds")) and sum(times.values())<=charge,"v6_runtime_charge_understated")
    base_rpc=value["base"]["rpc"];rpc=value["rpc"]
    require(type(rpc) is dict and set(rpc)==set(base_rpc),"v7_rpc_schema")
    first_method=base_rpc["method_items"]+1
    for index,call in enumerate(sorted(calls),base_rpc["http_attempts"]+1):
        require(call==index and calls[call]["j"]==first_method,"v6_call_or_method_continuity")
        first_method+=max(1,calls[call]["n"])
    for key in totals:
        legacy.integer(rpc[key],CAPS[key]);require(rpc[key]>=base_rpc[key]+totals[key],"v6_rpc_charge_understated")
    require(legacy.elapsed(rpc["elapsed_seconds"])>=base_rpc["elapsed_seconds"]+charge,"v6_global_runtime_understated")
    for key,floor in expired["admission"]["prior"]["rpc"].items():
        require(rpc[key]>=floor,"expired_v7_charge_refunded")
    if str(V6_ROOT/"FAILED.json") in files:
        failed=read_json(ref(files[str(V6_ROOT/"FAILED.json")]))[0]
        require(failed.get("schema_version")=="polymarket_exact_header_campaign_v6" and failed.get("status")=="blocked" and
            failed.get("raw_clocks_only") is True and failed.get("final_sample_admitted") is False,"v6_failure_schema")
        published=failed.get("usage",{})
        for key in rpc:
            count=legacy.elapsed(published.get(key)) if key=="elapsed_seconds" else legacy.integer(published.get(key),CAPS[key])
            require(rpc[key]>=count,"v6_published_charge_refunded")
        require(published.get("prior_rpc")==base_rpc,"v6_failure_prior_differs")
        published_retained=published.get("new_retained_bytes")
        require(type(published_retained) is dict and set(published_retained)==set(historical_bytes),"v6_published_retained_schema")
        for key,count in published_retained.items():legacy.integer(count,CAPS[key+"_bytes"])
    prior["rpc"]=dict(rpc)
    # New seals were semantically verified above; reuse only their edge rows.
    # Historical seals still undergo this full check; final byte reopens follow.
    for chunk,seal in sorted(prior["sealed"].items()):
        if chunk in seal_edges:first,final=seal_edges[chunk]
        else:
            keys=legacy.key_chunk_at(inputs["keys"]["path"],chunk)
            rows,_=verify_chunk(Path(seal["root"]),keys,chunk*CAPS["keys_per_chunk"])
            first,final=rows[0],rows[-1]
        if last is not None and last[0]+1==chunk:pilot.validate_clock([last[1],first])
        last=(chunk,final)
    for path,item in files.items():prior["files"][path]=item;prior["bytes"][item["category"]]+=item["bytes"]
    for capture in (hb,*captures.values()):
        if capture["path"] not in prior["files"]:
            prior["files"][capture["path"]]=capture|{"category":"control","chunk":None};prior["bytes"]["control"]+=capture["bytes"]
    external,eb=read_json(value["external_controls"],MAX_LEDGER)
    require(type(external) is dict and set(external)=={"schema_version","files","future_reserved_bytes","future_reserved_files","ceiling_bytes"} and
        external["schema_version"]==EXTERNAL_SCHEMA and external["ceiling_bytes"]==CAPS["external_control_reservation_bytes"] and
        type(external["files"]) is list and type(external["future_reserved_files"]) is int and external["future_reserved_files"]>=EXTERNAL_FUTURE_FILES and
        type(external["future_reserved_bytes"]) is int and external["future_reserved_bytes"]>=EXTERNAL_FUTURE_BYTES,"external_ledger_schema")
    require(all(type(item) is dict and set(item)=={"path","bytes","sha256"} for item in external["files"]),"external_ref_schema")
    require(len(external["files"])+external["future_reserved_files"]+1<=256 and
        sum(legacy.integer(item.get("bytes"),CAPS["external_control_reservation_bytes"]) for item in external["files"])+
        eb["bytes"]+external["future_reserved_bytes"]<=CAPS["external_control_reservation_bytes"],"external_shared_pool_exceeded")
    old_external_ref=next(ref(item) for item in value["base"]["files"] if item["path"]==str(OLD_EXTERNAL_PATH))
    old_external=read_json(old_external_ref,MAX_LEDGER)[0]
    old_paths={item["path"] for item in old_external["files"]}
    historical_paths=set(expired["required"])
    require(old_paths<=historical_paths,"old_v7_omits_v6_history")
    closure={}
    for item in external["files"]:
        require(type(item) is dict and set(item)=={"path","bytes","sha256"} and item["path"] not in closure and
            (Path(item["path"]).parent==OP_ROOT and Path(item["path"]).name in NEW_OP_NAMES or
             Path(item["path"]) in {REPO/name for name in EXTERNAL_SOURCE_FILES} or item["path"] in historical_paths),"external_ref_scope")
        closure[item["path"]]=reference(item,MAX_LEDGER)
    required={item["path"]:item for item in old_external["files"]}
    required[old_external_ref["path"]]=old_external_ref
    closed={}
    require(type(drain["external_control_closure"]) is list,"v6_external_closure_schema")
    for item in drain["external_control_closure"]:
        require(type(item) is dict and set(item)=={"path","bytes","sha256"} and item["path"] not in closed,"v6_external_closure_schema")
        closed[item["path"]]=item
    require(all(path in closed and closed[path]==item for path,item in required.items()) and
        all(str(OP_ROOT/name) in closed for name in OLD_FUTURE_NAMES),"v6_external_closure_incomplete")
    required.update(closed)
    required.update({path:ref(item) for path,item in expired["required"].items()})
    for item in (expired["terminal_binding"],reboot["binding"],*failed_qa["captures"].values()):required[item["path"]]=ref(item)
    for item in (hb,*captures.values()):
        if item["path"] not in files:required[item["path"]]=ref(item)
    require(all(path in closure and ref(closure[path])==ref(item) for path,item in required.items()),"external_old_controls_omitted")
    require(len(closure)+external["future_reserved_files"]+1<=256 and
        sum(item["bytes"] for item in closure.values())+eb["bytes"]+external["future_reserved_bytes"]<=CAPS["external_control_reservation_bytes"],
        "external_shared_pool_exceeded")
    for capture in (*closure.values(),eb):
        if capture["path"] not in prior["files"]:
            prior["files"][capture["path"]]=capture|{"category":"control","chunk":None};prior["bytes"]["control"]+=capture["bytes"]
    if published_retained is not None:
        for key,count in published_retained.items():prior["bytes"][key]=max(prior["bytes"][key],historical_bytes[key]+count)
    for chunk,usage in prior["chunk_usage"].items():
        require(len(prior["attempts"][chunk])<=CAPS["attempts_per_chunk"] and all(usage[key]<=CAPS["chunk_"+key] for key in totals) and
            usage["elapsed_seconds"]<=CAPS["chunk_runtime_seconds"] and
            sum(item["bytes"] for item in prior["files"].values() if item["chunk"]==chunk)<=CAPS["chunk_artifact_bytes"],"v6_chunk_cap")
    require(all(prior["bytes"][key]<=CAPS[key+"_bytes"] for key in prior["bytes"]) and
        prior["bytes"]["control"]+CAPS["external_control_reservation_bytes"]+FINAL_RESERVE<=CAPS["control_bytes"] and
        sum(prior["bytes"].values())+CAPS["external_control_reservation_bytes"]+FINAL_RESERVE<=CAPS["global_header_bytes"],"v6_retained_byte_cap")
    for item in prior["files"].values():
        require(bound(Path(item["path"]),CAPS["global_header_bytes"])=={key:item[key] for key in ("path","bytes","sha256","identity")},"v6_final_reopen_changed")
    require({str(path) for path in V6_ROOT.rglob("*") if path.is_file()}==actual,"v6_late_roster_changed")
    return prior


class RPC(previous.RPC):
    def call(self, keys=None, offset=0, *, terminal=False):
        b = self.budget; n = 0 if keys is None else len(keys)
        for attempt in range(CAPS["attempts_per_batch"]):
            b.check();wait_for_pace(b)
            if hasattr(b,"before_request"): b.before_request()
            for key,extra in (("http_attempts",1),("method_items",max(1,n))):
                require(b.rpc[key]+extra+(0 if terminal else 1) <= CAPS[key],key+"_limit")
                if b.chunk is not None:
                    require(b.chunk_base[key]+b.rpc[key]-b.chunk_start_rpc[key]+extra <= CAPS["chunk_"+key],
                        "chunk_"+key+"_limit")
            allowance = min(CAPS["individual_response_bytes"],CAPS["response_bytes"]-b.rpc["response_bytes"])
            if b.chunk is not None:
                allowance = min(allowance,CAPS["chunk_response_bytes"]-b.chunk_base["response_bytes"]
                    -b.rpc["response_bytes"]+b.chunk_start_rpc["response_bytes"])
            require(allowance>0,"response_bytes_limit")
            call,first_id = b.rpc["http_attempts"]+1,b.rpc["method_items"]+1
            requests = [{"jsonrpc":"2.0","id":first_id+i,"method":"eth_chainId" if keys is None else "eth_getBlockByNumber",
                "params":[] if keys is None else [hex(key["block_number"]),False]}
                for i,key in enumerate([None] if keys is None else keys)]
            payload = requests[0] if keys is None else requests
            before_response=b.rpc["response_bytes"]
            b.rpc["http_attempts"]+=1; b.rpc["method_items"]+=max(1,n); b.rpc["response_bytes"]+=allowance
            b.append(self.journal,{"e":"s","i":call,"k":-1 if keys is None else self.key_offset+offset,"n":n,
                "j":first_id,"b":allowance,"t":int(time.monotonic()*1000),"h":digest(encode(payload))})
            reply = {"raw":b"","charged_bytes":allowance,"complete":False,"status":None,
                "error_code":"transport_failure","proven_oversize":False}
            code,split,retry,rows,codes,projection = None,False,False,None,[],None
            stage = None; interrupted = None; response_digest = None
            try:
                remaining = min(CAPS["request_seconds"],b.remaining(),
                    CAPS["chunk_runtime_seconds"]-b.chunk_elapsed() if b.chunk is not None else b.remaining())
                with request_deadline(remaining,"request_deadline" if remaining == CAPS["request_seconds"] else
                    "chunk_runtime_limit" if b.chunk is not None and CAPS["chunk_runtime_seconds"]-b.chunk_elapsed() <= b.remaining() else "runtime_limit"):
                    paced_start(b,max(1,n))
                    reply = self.transport(self.endpoint,payload,byte_limit=allowance,timeout=remaining)
                    require(type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance and
                        type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance and
                        type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool,"transport_contract")
                    if reply["complete"] is False: reply["charged_bytes"] = allowance
                    else: require(reply["charged_bytes"] == len(reply["raw"]),"transport_contract")
                    response_digest = digest(reply["raw"])
                    code = reply.get("error_code")
                    split = (reply["proven_oversize"] is True and code == "response_declared_oversize") or (reply.get("status") == 413 and code is None)
                    require(not split,"proven_batch_size_refusal"); require(code is None,code or "transport_failure")
                    if reply.get("status") == 429 or type(reply.get("status")) is int and 500 <= reply["status"] <= 599:
                        retry=True; raise Blocked("retryable_http_failure")
                    require(reply.get("status") == 200 and reply["complete"] is True,"http_or_body_incomplete")
                    values = decode(reply["raw"])
                    if keys is not None and type(values) is dict and set(values) == {"jsonrpc","id","error"}:
                        err = values["error"]
                        split = (values["jsonrpc"] == "2.0" and values["id"] is None and type(err) is dict and
                            type(err.get("code")) is int and err["code"] in (-32600,-32005) and
                            type(err.get("message")) is str and err["message"].lower() in {
                                "batch requests are not supported","batch size limit exceeded","batch request limit exceeded"})
                        require(not split,"proven_batch_size_refusal")
                    values = [values] if keys is None else values
                    require(type(values) is list and len(values) == len(requests),"rpc_batch_shape")
                    replies = {}
                    for item in values:
                        require(type(item) is dict and set(item) in ({"jsonrpc","id","result"},{"jsonrpc","id","error"}) and
                            item["jsonrpc"] == "2.0" and type(item["id"]) is int and item["id"] not in replies,"rpc_reply_shape")
                        replies[item["id"]] = item
                    require(set(replies) == {item["id"] for item in requests},"rpc_reply_ids")
                    partial = []
                    for req,key in zip(requests,[None] if keys is None else keys):
                        item = replies[req["id"]]
                        if "error" in item:
                            err = item["error"]
                            require(type(err) is dict and {"code","message"} <= set(err) <= {"code","message","data"} and
                                type(err["code"]) is int and -(2**31) <= err["code"] < 2**31 and type(err["message"]) is str,
                                "rpc_error_schema")
                            codes.append(err["code"])
                        elif keys is None: require(pilot.quantity(item["result"]) == 137,"wrong_chain")
                        else: partial.append(pilot.validate_header(item["result"],key))
                    if partial: pilot.validate_clock(partial)
                    if codes:
                        retry = all(code in TRANSIENT_RPC_CODES for code in codes)
                        raise Blocked("retryable_rpc_item_error" if retry else "rpc_item_error_not_retryable")
                    rows = None if keys is None else partial
                    if rows is not None: projection = digest(scale.projection_bytes(rows))
            except Deadline as exc:
                returned = reply.get("raw",b"") if type(reply) is dict else b""
                observed = exc.raw or (returned if type(returned) is bytes and len(returned) <= allowance else b"")
                status = exc.status if exc.status is not None else reply.get("status") if type(reply) is dict else None
                reply = {"raw":observed,"charged_bytes":allowance,"complete":False,"status":status,
                    "error_code":exc.code,"proven_oversize":False}
                code=exc.code; stage = code if code != "request_deadline" else None
            except (KeyboardInterrupt,SystemExit) as exc:
                interrupted=exc; code="interrupted"
                observed=getattr(exc,"raw",b"")
                reply={"raw":observed if type(observed) is bytes and len(observed)<=allowance else b"",
                    "charged_bytes":allowance,"complete":False,"status":getattr(exc,"status",None),"proven_oversize":False}
                response_digest=None
            except (Blocked,pilot.PilotBlocked) as exc: code=str(exc)
            except BaseException: code="transport_failure"
            if not (type(reply) is dict and type(reply.get("raw")) is bytes and len(reply["raw"]) <= allowance and
                    type(reply.get("charged_bytes")) is int and len(reply["raw"]) <= reply["charged_bytes"] <= allowance and
                    type(reply.get("complete")) is bool and type(reply.get("proven_oversize")) is bool):
                observed = reply.get("raw",b"") if type(reply) is dict else b""
                observed = observed[:allowance] if type(observed) is bytes else b""
                reply={"raw":observed,"charged_bytes":allowance,"complete":False,"status":None}
                response_digest = None
                code="transport_contract"
            if reply.get("status") is not None and not (type(reply["status"]) is int and 100 <= reply["status"] <= 599):
                reply.update(status=None,complete=False);code="transport_contract"
            if code == "transport_contract":reply["complete"]=False
            if reply["complete"] is False: reply["charged_bytes"]=allowance
            b.rpc["response_bytes"] = before_response + reply["charged_bytes"]
            response_digest = response_digest or digest(reply["raw"])
            finish = {"e":"f","i":call,"b":reply["charged_bytes"],"z":len(reply["raw"]),
                "h":response_digest if reply["complete"] else None,"x":response_digest,
                "p":projection if code is None else None,"r":"validated" if code is None else "split" if split else "failed",
                "s":reply.get("status"),"t":int(time.monotonic()*1000)}
            b.append(self.journal,finish,failure=code is not None)
            if code is None: return rows
            diagnostic(b,self.journal,finish,code,codes)
            if interrupted is not None: raise interrupted
            if stage is not None: raise Blocked(stage)
            if split:
                require(keys is not None and len(keys)>1,"singleton_size_refusal")
                reduced=min(self.batch_size,max(1,len(keys)//2))
                require(forecast_seconds(b.remaining_keys,reduced,b.prior["observed_max_call_seconds"])<b.remaining(),
                    "reduced_batch_runtime_not_fit")
                self.batch_size=reduced;return None
            retry = retry or code in {"transport_failure","request_deadline","response_incomplete","content_length_differs","response_close_failure"}
            require(retry and attempt+1 < CAPS["attempts_per_batch"],code or "rpc_failure")
            delay=(RATE_LIMIT_DELAYS if 429 in codes or reply.get("status")==429 else RETRY_DELAYS)[attempt]
            require(b.remaining()>delay,"runtime_limit")
            if b.chunk is not None:require(b.chunk_elapsed()+delay<CAPS["chunk_runtime_seconds"],"chunk_runtime_limit")
            time.sleep(delay);b.check()
        raise Blocked("batch_attempts_exhausted")


def run_campaign(admission_path, admission_sha, head, output, private, *, transport=http_response, arm_prior=None):
    started = time.monotonic(); producer_start_utc=time.time();output, private = Path(output), Path(private)
    require(output.is_absolute() and output.parent == RUN_ROOT and output==RUN and output.parent.resolve() == output.parent
            and output.parent.is_dir() and not output.exists() and not output.is_symlink(), "destination_not_fresh")
    admission, admission_binding = read_json({"path": str(admission_path), "bytes": identity(admission_path)["bytes"], "sha256": admission_sha}, MAX_LEDGER)
    require(type(admission) is dict and set(admission) == {"schema_version", "status", "caps", "consumer_head", "run_dir",
        "raw_clocks_only", "final_sample_admitted", "inputs", "private_file", "actual_keys", "conservative_new_runtime_seconds", "prior",
        "reboot_equivalence"}
        and admission["schema_version"] == SCHEMA and admission["status"] == "root_exact_header_campaign_admitted"
        and admission["caps"] == CAPS and admission["consumer_head"] == head and admission["run_dir"] == str(output)
        and admission["raw_clocks_only"] is True and admission["final_sample_admitted"] is False, "root_admission_not_exact")
    source = source_snapshot(head)
    # Tighten the outer alarm before large SHA/Parquet admission reads. The
    # subsequent ledger validation independently checks this declared charge.
    declared_runtime = legacy.elapsed(admission["prior"]["rpc"]["elapsed_seconds"])
    require(declared_runtime + time.monotonic() - started < CAPS["runtime_seconds"], "runtime_limit")
    if arm_prior: arm_prior(declared_runtime, started)
    require(declared_runtime>=required_carried_runtime(admission["prior"],producer_start_utc),"postdrain_runtime_understated")
    K = legacy.integer(admission["actual_keys"], CAPS["keys"]); require(K == ACTUAL_KEYS, "key_sample_not_exact")
    inputs, measured = validate_inputs(admission["inputs"], source, K)
    reboot=validate_reboot(admission["reboot_equivalence"],admission["prior"],inputs,admission["private_file"])
    prior = validate_prior(admission["prior"], output, source, inputs,reboot)
    for value in inputs.values():
        require(value["path"] in prior["files"] and ref(prior["files"][value["path"]]) == ref(value), "input_absent_from_global_ledger")
    remaining_K=remaining_key_count(K,prior,inputs["keys"]["path"])
    projection = legacy.elapsed(admission["conservative_new_runtime_seconds"])
    require(projection>=required_new_runtime(remaining_K,measured,prior) and projection + prior["rpc"]["elapsed_seconds"] + time.monotonic()-started < CAPS["runtime_seconds"], "runtime_feasibility_not_admitted")
    require(max(80, 1.25 * measured["bytes_per_key"]) * K <= CAPS["accepted_bytes"], "header_disk_projection_not_fit")
    private_binding = reference(admission["private_file"])
    require(private_binding["path"] == str(private) and private.stat().st_size <= 16 * 1024, "private_configuration_binding")
    endpoint, configuration = inventory.load_rpc_endpoint(private)
    require(configuration == "private_file", "private_rpc_authority_required")
    require(reboot["private_current"]==pilot.identity(private) and private_binding==reboot["private_binding"],
        "rpc_authority_differs_from_reboot")
    for path in [admission_binding["path"], private_binding["path"], *prior["files"]]:
        require(Path(path) != output and output not in Path(path).parents and Path(path) not in output.parents, "input_output_overlap")
    resources = inventory.resource_state(output.parent)
    require(resources["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "available_ram_floor")
    require(resources["disk_bytes"]["free"] >= CAPS["minimum_free_bytes"] + CAPS["chunk_artifact_bytes"] + FINAL_RESERVE,
            "disk_floor")
    require(prior["rpc"]["elapsed_seconds"] == declared_runtime, "prior_runtime_changed")
    output.mkdir(mode=0o700)
    b = Budget(output, prior, admission_binding["bytes"], started)
    b.next_dispatch_at=time.monotonic();b.remaining_keys=remaining_K
    new_active_seconds,new_accepted_keys=0.0,0
    source_identities = {name: identity(REPO / name) for name in SOURCE_FILES}
    def before_request():
        for value in (admission_binding, private_binding, inputs["keys"],reboot["binding"],reboot["original_reference"]):
            require(identity(Path(value["path"])) == value["identity"], "request_input_changed")
        require(all(identity(REPO / name) == value for name, value in source_identities.items()), "request_source_changed")
    b.before_request = before_request
    completed, last, accepted = {}, None, 0
    def external(full=False):
        require(source_snapshot(head) == source, "source_changed")
        for value in [admission_binding, private_binding, *inputs.values(), *prior["files"].values()]:
            require(identity(Path(value["path"])) == value["identity"], "input_identity_changed")
        require(inventory.load_rpc_endpoint(private) == (endpoint, configuration), "private_authority_changed")
        require(identity(Path(reboot["binding"]["path"]))==reboot["binding"]["identity"] and
            reboot_environment()==reboot["environment"],"reboot_certificate_or_environment_changed")
        if full:
            require(read_json(ref(admission_binding), MAX_LEDGER)[1] == admission_binding, "admission_changed")
            require(reference(admission["private_file"]) == private_binding, "private_authority_changed")
            for value in inputs.values(): require(bound(Path(value["path"]), CAPS["baseline_bytes"]) == value, "input_changed")
            require(validate_reboot(admission["reboot_equivalence"],admission["prior"],inputs,admission["private_file"])==reboot,
                "reboot_certificate_changed")
            require(validate_prior(admission["prior"], output, source, inputs,reboot) == prior, "prior_artifacts_or_roster_changed")
            # These callbacks can inspect complex historical evidence. Reopen
            # current bindings after all of them, before the atomic seal.
            require(source_snapshot(head)==source,"source_changed")
            for value in [admission_binding,private_binding,*inputs.values(),*prior["files"].values(),
                    reboot["binding"],reboot["original_reference"]]:
                require(bound(Path(value["path"]),CAPS["global_header_bytes"])==
                    {key:value[key] for key in ("path","bytes","sha256","identity")},"final_binding_changed")
            require(all(identity(REPO/name)==value for name,value in source_identities.items()),"final_source_identity_changed")
        b.check()
    try:
        b.write("OWNER.json", encode({"schema_version": "polymarket_exact_header_owner_v8", "run_dir": str(output),
            "source": source, "process": owner_identity(), "inputs": inputs, "caps": CAPS, "admission": admission_binding,
            "producer_start_utc":producer_start_utc,
            "reboot_equivalence":reboot["binding"],
            "raw_clocks_only": True, "final_sample_admitted": False}))
        external()
        RPC(endpoint, b, transport, "rpc.jsonl").call()
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            b.remaining_keys=remaining_K-new_accepted_keys
            if new_accepted_keys:
                require(max(forecast_seconds(b.remaining_keys,CAPS["initial_batch_size"],prior["observed_max_call_seconds"]),
                    b.remaining_keys*new_active_seconds/new_accepted_keys+FORECAST_RESERVE_SECONDS)<b.remaining(),
                    "remaining_runtime_not_fit")
            offset = chunk * CAPS["keys_per_chunk"]
            if chunk in prior["sealed"]:
                root = Path(prior["sealed"][chunk]["root"])
                rows, header_binding = verify_chunk(root, keys, offset)
            else:
                attempt = len(prior["attempts"].get(chunk, set())) + 1
                require(attempt <= CAPS["attempts_per_chunk"], "chunk_attempts_exhausted")
                b.chunk, b.chunk_started = chunk, time.monotonic()
                b.chunk_base = prior["chunk_usage"].get(chunk, dict.fromkeys(("http_attempts", "method_items", "response_bytes", "elapsed_seconds"), 0))
                b.chunk_start_rpc = dict(b.rpc)
                base = f"chunks/c{chunk:06d}/a{attempt:02d}"
                rows = RPC(endpoint, b, transport, base + "/rpc.jsonl", offset).fetch(keys)
                payload = scale.compact_parquet(rows)
                require(len(payload) <= CAPS["chunk_artifact_bytes"], "chunk_parquet_byte_limit")
                b.check(len(payload), "accepted")  # Accepted allocation must fit before staging.
                headers = b.write(base + "/headers.parquet", payload, "failed")
                require(read_headers(output / headers["path"], keys) == rows, "chunk_header_reopen_differs")
                usage, starts, finishes = journal_usage(output / (base + "/rpc.jsonl"))
                require(len(starts) == len(finishes), "chunk_open_request")
                manifest = {"schema_version": "polymarket_exact_header_chunk_v3", "status": "exact_header_chunk_complete_raw_clocks_only",
                    "chunk": chunk, "attempt": attempt, "offset": offset, "keys": len(keys), "keys_sha256": digest(encode(keys)),
                    "headers": {**headers, "path": "headers.parquet"}, "rpc_usage": usage,
                    "active_seconds_charged": b.chunk_elapsed(), "raw_clocks_only": True, "final_sample_admitted": False}
                mb = b.write(base + "/manifest.json", encode(manifest))
                b.seal(base + "/COMPLETE.json", {"status": manifest["status"], "manifest": {**mb, "path": "manifest.json"}}, external)
                b.files[headers["path"]]["category"] = "accepted"
                b.used["failed"] -= len(payload); b.used["accepted"] += len(payload)
                root = output / base
                rows, header_binding = verify_chunk(root, keys, offset)
                b.check()
                new_active_seconds+=time.monotonic()-b.chunk_started;new_accepted_keys+=len(keys)
                b.chunk = None
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; accepted += len(rows)
            completed[chunk] = {"root": str(root), "headers": header_binding, "keys": len(keys)}
        require(accepted == K and len(completed) == math.ceil(K / CAPS["keys_per_chunk"]), "exact_key_coverage_failed")
        RPC(endpoint, b, transport, "rpc.jsonl").call(terminal=True)
        external(full=True); b.reopen()
        observed = dict.fromkeys(("http_attempts", "method_items", "response_bytes"), 0)
        for name in b.files:
            if Path(name).name != "rpc.jsonl": continue
            usage, starts, finishes = journal_usage(output / name)
            require(len(starts) == len(finishes), "campaign_unclosed_request")
            for key in observed: observed[key] += usage[key]
        require(all(b.rpc[key] - prior["rpc"][key] == observed[key] for key in observed), "campaign_charge_reconciliation")
        # Independent ordered final pass catches late mutations of every sealed
        # chunk, with native expected hashes and adjacent cross-chunk parents.
        last, reopened_count = None, 0
        for chunk, keys in enumerate(key_chunks(inputs["keys"]["path"])):
            rows, header_binding = verify_chunk(Path(completed[chunk]["root"]), keys, chunk * CAPS["keys_per_chunk"])
            require(header_binding == completed[chunk]["headers"], "final_chunk_changed")
            if last: pilot.validate_clock([last, rows[0]])
            last = rows[-1]; reopened_count += len(rows)
        require(reopened_count == K, "final_key_coverage_failed")
        inventory_binding = b.write("artifact_inventory.json", encode(b.files))
        manifest = {"schema_version": "polymarket_exact_header_campaign_v8", "status": SUCCESS, "source": source,
            "recovery_policy": {"transient_rpc_codes":sorted(TRANSIENT_RPC_CODES),"diagnostic_record_bytes":previous.previous.previous.DIAGNOSTIC_RECORD_BYTES,
                "diagnostic_chunk_bytes":previous.previous.previous.DIAGNOSTIC_CHUNK_BYTES,"diagnostic_total_bytes":previous.previous.previous.DIAGNOSTIC_TOTAL_BYTES,
                "retry_delays_seconds":RETRY_DELAYS,"rate_limit_delays_seconds":RATE_LIMIT_DELAYS,
                "method_items_per_second":METHOD_ITEMS_PER_SECOND,"pacing":"shared_start_to_start_no_catchup",
                "forecast_reserve_seconds":FORECAST_RESERVE_SECONDS},
            "inputs": inputs, "admission": admission_binding, "caps": CAPS, "requested_keys": K, "accepted_keys": K,
            "reboot_equivalence":reboot["binding"],
            "missing_keys": 0, "duplicate_keys": 0, "fallback_rows": 0, "exact_key_coverage": True,
            "chunks": completed, "artifact_inventory": inventory_binding, "usage": b.usage(),
            "raw_clocks_only": True, "final_sample_admitted": False, "history_completeness_certified": False,
            "cryptographic_header_verification": False, "timestamp_semantics": "RPC-reported integer-second block time shared by fills in the same block"}
        mb = b.write("manifest.json", encode(manifest))
        b.seal("COMPLETE.json", {"schema_version": "polymarket_exact_header_campaign_completion_v8", "status": SUCCESS,
            "manifest": mb, "raw_clocks_only": True, "final_sample_admitted": False}, lambda: external(full=True))
        return {"status": SUCCESS, "run_dir": str(output), "accepted_keys": K, "usage": manifest["usage"], "final_sample_admitted": False}
    except BaseException as exc:
        code = exc.code if isinstance(exc,Deadline) else str(exc) if isinstance(exc, (Blocked, pilot.PilotBlocked)) else "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "header_campaign_runtime_failure"
        result = {"schema_version": "polymarket_exact_header_campaign_v8", "status": "blocked", "error_code": code,
                  "usage": b.usage(), "accepted_keys": accepted, "raw_clocks_only": True, "final_sample_admitted": False}
        try: b.write("FAILED.json", encode(result), failure=True)
        except BaseException: pass
        if isinstance(exc, (KeyboardInterrupt, SystemExit)): raise
        return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admission",type=Path,required=True);parser.add_argument("--admission-sha256",required=True)
    parser.add_argument("--expected-head",required=True);parser.add_argument("--run-dir",type=Path,required=True)
    parser.add_argument("--rpc-private-file",type=Path,required=True)
    args=parser.parse_args(argv);require_production_host()
    try:
        require(Path(sys.executable)==Path("/home/ubuntu/venv/bin/python"),"production_interpreter_required")
        with process_limits() as debit:
            result=run_campaign(args.admission,args.admission_sha256,args.expected_head,args.run_dir,
                args.rpc_private_file,arm_prior=debit)
    except Deadline as exc:result={"status":"blocked","error_code":exc.code}
    except (Blocked,pilot.PilotBlocked) as exc:result={"status":"blocked","error_code":str(exc)}
    except (OSError,ValueError,TypeError,KeyError,subprocess.SubprocessError):
        result={"status":"blocked","error_code":"admission_or_environment_failure"}
    print(encode(result).decode().strip());return 0 if result["status"]==SUCCESS else 1



if __name__=="__main__":raise SystemExit(main())
