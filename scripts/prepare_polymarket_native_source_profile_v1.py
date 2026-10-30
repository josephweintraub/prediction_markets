"""Pure preparation evidence and cost checks; real source profiling stays CLOSED.

Only supplied immutable bytes and explicit integer schedules are inspected. No
function reads a file, profiles a footer, launches a process or calls a fixture
driver. Device projection is structural, not a fresh observation of a source.
The accepted profiler/preparer remain unchanged. A declared cost fit never
authenticates completeness of the required historical/header/Git closure.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path: sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import prepare_polymarket_native_clock_layer_v1 as proof
from scripts import profile_polymarket_native_clock_sources_v1 as profile

Blocked = proof.Blocked
require, encode, decode, digest = proof.require, proof.encode, proof.decode, proof.digest
SCHEMA = 'polymarket_native_source_profile_preparation_v1'
RESOURCE = {'bytes':46755,'sha256':'1abdc2a40a5408a775d5e3412fca284fa9b7c52a051842626f856c6b84525203'}
CONTROL_NAMES = {'header_terminal','header_admission','descriptor','inventory_complete',
                 'inventory_manifest','roster_complete','roster_manifest','source_plan'}
OLD_DEVICE, CURRENT_DEVICE = 66309, 66305
DATA_UUID = 'd0cd087b-94c4-428c-bae9-ae28929059f6'
ROOT_UUID = '0df68d9a-e70c-413e-9a10-736ae3f00a7a'
DATA_SERIAL = 'vol0acbba4565a64f792'
DATA_BYTES = 805306368000
READ_LIMIT = 2000000000
OUTPUT_CAP = 16 * 1024**2
MINIMUM_FREE = 20000000000
IDENTITY_FIELDS = proof.inventory.STAT_FIELDS
KINDS = {'accepted_control','roster','generated_audit','original_source_epoch','current_source'}


def number(value,code,minimum=0):
    require(type(value) is int and value>=minimum,code)
    return value


def actual_resource(raw):
    """Authenticate this exact lossless historical measurement, not today's host."""
    require(type(raw) is bytes and len(raw)==RESOURCE['bytes'] and digest(raw)==RESOURCE['sha256'],
            'profile_actual_resource_bytes_or_SHA')
    value=decode(raw)
    require(value.get('schema_version')=='native_clock_readonly_host_inspection_v4'
            and value.get('status')=='read_only_inspection_complete_not_stage_admission',
            'profile_actual_resource_scope')
    mounts=value.get('mounts')
    require(type(mounts) is dict and mounts.get('data_uuid')==DATA_UUID
            and mounts.get('root_uuid')==ROOT_UUID and mounts.get('data_serial')==DATA_SERIAL
            and type(mounts.get('data_device_bytes')) is int and mounts['data_device_bytes']==DATA_BYTES
            and type(mounts.get('data_st_dev')) is int and mounts['data_st_dev']==CURRENT_DEVICE
            and mounts.get('data_fstype')=='ext4','profile_actual_resource_volume')
    require(value.get('source',{}).get('head')==proof.HEADER_HEAD
            and value['source'].get('tracked_and_untracked_clean') is True,
            'profile_actual_resource_source')
    for name in ('remote_files_written','provider_requests','trade_payload_bytes_read','footer_bytes_read'):
        require(type(value.get(name)) is int and value[name]==0,'profile_actual_resource_not_readonly')
    require(value.get('native_stage_admitted') is False and value.get('whole_data_certified') is False
            and value.get('full_closure_accepted') is False,'profile_actual_resource_not_admission')
    require(set(value.get('controls',{}))==CONTROL_NAMES,'profile_actual_resource_controls')
    for control in value['controls'].values():
        proof.reference(control['ref'])
        identity=control['identity']
        require(type(identity) is dict and all(type(item) is int and item>=0 for item in identity.values()),
                'profile_actual_resource_lossless_identity')
        if 'raw_UTF8' in control:
            supplied=control['raw_UTF8'].encode()
            require(len(supplied)==control['ref']['bytes'] and digest(supplied)==control['ref']['sha256'],
                    'profile_resource_recovered_control_bytes')
    for name in ('MemAvailable_bytes','data_disk_free_bytes'):
        number(value['resources_final'].get(name),'profile_actual_resource_measurement')
    return value


def missing_control_bytes(resource_raw,controls):
    actual_resource(resource_raw)
    require(type(controls) is dict and set(controls)<=CONTROL_NAMES,'profile_small_control_roster')
    return sorted(CONTROL_NAMES-set(controls))


def actual_inventory(resource_raw,complete_raw,manifest_raw):
    value=actual_resource(resource_raw);saved=value['controls']
    ref=saved['inventory_manifest']['ref']
    require(type(manifest_raw) is bytes and len(manifest_raw)==ref['bytes'] and digest(manifest_raw)==ref['sha256'],
            'profile_actual_inventory_manifest_bytes')
    manifest=decode(manifest_raw)
    key=proof.reference({**manifest['required_keys_parquet'],
        'path':str(Path(saved['inventory_manifest']['ref']['path']).parent/'required_keys.parquet')})
    inputs={name:deepcopy(saved[name]['ref']) for name in ('inventory_complete','inventory_manifest')}
    inputs.update(keys=key,roster=deepcopy(value['large_input_stat_only']['roster']['binding']))
    result=proof.inventory_control_adapter(complete_raw,manifest_raw,inputs)
    return {'inputs':inputs,'saved_inventory':result,
            'original_inventory_manifest':manifest,'resource_vintage_not_current_host':True}


def actual_small_controls(resource_raw,controls):
    """No parsed mirror, resource projection or accepted=True can replace bytes."""
    value=actual_resource(resource_raw)
    require(not missing_control_bytes(resource_raw,controls),'profile_actual_small_control_bytes_missing')
    saved=value['controls'];parsed={}
    for name in CONTROL_NAMES:
        raw=controls[name];ref=saved[name]['ref']
        require(type(raw) is bytes and len(raw)==ref['bytes'] and digest(raw)==ref['sha256'],
                'profile_actual_small_control_bytes_or_SHA')
        parsed[name]=decode(raw)
    inventory=actual_inventory(resource_raw,controls['inventory_complete'],controls['inventory_manifest'])
    proof.bind_header_admission(controls['header_terminal'],controls['header_admission'],inventory['inputs'])
    descriptor=parsed['descriptor'];listing=parsed['roster_manifest'];marker=parsed['roster_complete']
    relative=lambda name:{**saved[name]['ref'],'path':Path(saved[name]['ref']['path']).name}
    require(descriptor.get('schema_version')=='polymarket_exact_header_roster_descriptor_v1'
            and descriptor.get('source_plan')==saved['source_plan']['ref']
            and saved['descriptor']['ref']['sha256']==proof.KNOWN_DESCRIPTOR['sha256'],
            'profile_actual_descriptor_binding')
    require(listing.get('schema_version')=='polymarket_exact_header_roster_build_v1'
            and listing.get('descriptor')==saved['descriptor']['ref']
            and listing.get('roster')=={**inventory['inputs']['roster'],'path':'roster.json'},
            'profile_actual_roster_binding')
    status='sealed_source_roster_complete_not_inventory_admission'
    require(listing.get('status')==marker.get('status')==status
            and marker.get('manifest')==relative('roster_manifest')
            and all(item.get('final_universe_admitted') is False and item.get('analysis_admitted') is False
                    for item in (listing,marker)),'profile_actual_roster_completion')
    endpoint=proof.endpoint_from_source_plan(parsed['source_plan'])
    return {'input_refs':{name:deepcopy(saved[name]['ref']) for name in sorted(CONTROL_NAMES)},
            'inventory':inventory,'endpoint':endpoint,
            'full_nested_closure_authenticated_here':False,'production_admitted':False}


def _project_identity(original,current):
    require(type(original) is dict and type(current) is dict and set(original)==set(current)
            and set(original) in (IDENTITY_FIELDS,IDENTITY_FIELDS|{'bytes'}),
            'profile_device_projection_identity_schema')
    require(all(type(item) is int and item>=0 for item in (*original.values(),*current.values())),
            'profile_device_projection_integer_identity')
    require(original['device']==OLD_DEVICE and current['device']==CURRENT_DEVICE
            and all(current[key]==item for key,item in original.items() if key!='device'),
            'profile_device_projection_not_device_only')
    return deepcopy(current)


def device_only_projection(original,current,resource_raw):
    actual_resource(resource_raw)
    return {'original_identity':deepcopy(original),'projected_identity':_project_identity(original,current),
            'device_mapping':[OLD_DEVICE,CURRENT_DEVICE],
            'qualification':'structural_projection_not_fresh_source_identity_observation'}


def project_source_bindings(originals,currents,resource_raw):
    actual_resource(resource_raw)
    require(type(originals) is list and type(currents) is list and 0<len(originals)==len(currents)
            <=profile.PROPOSED['files'],'profile_projection_source_count_or_order')
    result=[]
    for original,current in zip(originals,currents):
        original=profile.core.source_binding(original);current=profile.core.source_binding(current)
        require({key:item for key,item in original.items() if key!='identity'}
                =={key:item for key,item in current.items() if key!='identity'},
                'profile_projection_changed_source_binding_or_order')
        _project_identity(original['identity'],current['identity']);result.append(deepcopy(current))
    return {'source_bindings':result,'physical_multiplicity_policy':'preserve_order_and_all_rows_no_dedup',
            'current_source_identities_observed_here':False,'production_admitted':False}


def declared_read_plan(resource_raw,manifest_raw,schedule,bootstrap_read_bytes,*,retained_output_cap_bytes=OUTPUT_CAP):
    """Exact supplied-schedule arithmetic, NEVER a closure-completeness token."""
    value=actual_resource(resource_raw);ref=value['controls']['inventory_manifest']['ref']
    require(type(manifest_raw) is bytes and len(manifest_raw)==ref['bytes'] and digest(manifest_raw)==ref['sha256'],
            'profile_plan_inventory_manifest_bytes')
    manifest=decode(manifest_raw);two=number(manifest['planned_reads']['footer_read_bytes'],'profile_plan_saved_footer')
    require(two%2==0 and two//2<=profile.PROPOSED['single_source_footer_pass_bytes'],'profile_plan_footer_subcap')
    number(bootstrap_read_bytes,'profile_plan_bootstrap_bytes',1)
    number(retained_output_cap_bytes,'profile_plan_output_cap',1024**2)
    require(retained_output_cap_bytes<=OUTPUT_CAP,'profile_plan_output_cap_increase_forbidden')
    require(type(schedule) is list and schedule,'profile_plan_explicit_schedule_required')
    seen={};initial=final=0;kinds=set()
    for item in schedule:
        require(type(item) is dict and set(item)=={'reference','kind','initial_reads','final_reads'}
                and item['kind'] in KINDS,'profile_plan_schedule_schema')
        bound=proof.reference(item['reference']);number(item['initial_reads'],'profile_plan_initial_reads',1)
        number(item['final_reads'],'profile_plan_final_reads',1)
        require(bound['path'] not in seen,'profile_plan_duplicate_or_conflicting_path')
        require(not bound['path'].endswith('.parquet') or item['kind']=='generated_audit'
                and bound==value['large_input_stat_only']['source_audit']['binding'],
                'profile_plan_raw_payload_or_key_body_forbidden')
        seen[bound['path']]=bound;kinds.add(item['kind'])
        initial+=bound['bytes']*item['initial_reads'];final+=bound['bytes']*item['final_reads']
    required=[item['ref'] for item in value['controls'].values()]
    required += [value['large_input_stat_only'][name]['binding'] for name in ('roster','source_audit')]
    require(all(seen.get(item['path'])==item for item in required)
            and {'original_source_epoch','current_source'}<=kinds,'profile_plan_required_closure_or_source_cost_missing')
    total=bootstrap_read_bytes+initial+final+two//2+2*retained_output_cap_bytes
    require(total<=READ_LIMIT,'profile_full_declared_read_plan_exceeds_unchanged_2GB')
    require(value['resources_final']['data_disk_free_bytes']>=MINIMUM_FREE+retained_output_cap_bytes,
            'profile_saved_disk_measurement_insufficient')
    return {'schema_version':SCHEMA+'_declared_read_plan','initial_reads_bytes':initial,
            'final_reads_bytes':final,'bootstrap_reads_bytes':bootstrap_read_bytes,
            'one_source_footer_pass_bytes':two//2,'all_new_retained_output_cap_bytes':retained_output_cap_bytes,
            'two_output_reopen_reservations_bytes':2*retained_output_cap_bytes,'total_read_upper_bytes':total,
            'read_ceiling_bytes':READ_LIMIT,'remaining_declared_read_bytes':READ_LIMIT-total,
            'required_key_body_or_raw_trade_pages_read':False,'full_nested_closure_authenticated_here':False,
            'fresh_resource_or_runtime_admission':False,'production_admitted':False,
            'aggregate_output_fit_proved':False,'oversized_output_policy':'block_not_drop_or_increase_cap',
            'qualification':'declared_schedule_arithmetic_only_no_actual_full_closure_proof'}


def prepare_actual_candidate(resource_raw,controls,*,schedule,bootstrap_read_bytes):
    actual_small_controls(resource_raw,controls)
    declared_read_plan(resource_raw,controls['inventory_manifest'],schedule,bootstrap_read_bytes)
    raise Blocked('profile_actual_full_nested_closure_and_source_epoch_proof_still_missing')


def contract():
    return {'schema_version':SCHEMA,'status':'pure_actual_evidence_and_plan_adapter_production_closed',
            'resource_receipt':deepcopy(RESOURCE),'unchanged_metadata_read_ceiling_bytes':READ_LIMIT,
            'prospective_smaller_all_new_output_cap_bytes':OUTPUT_CAP,
            'explicit_optional_lower_output_cap_bytes':1024**2,
            'legacy_footer_identity_is_full_payload_SHA':False,'current_source_identity_observation':False,
            'source_row_or_footer_scan_performed':False,'fixture_driver_called':False,
            'full_nested_closure_authenticated_here':False,'production_input_IO_enabled':False,
            'remaining_actual_controls':['source_plan_original_bytes','full_direct_and_nested_control_ref_union',
                'original_and_current_source_Git_byte_read_schedule','fresh_current_source_nondevice_identities',
                'inclusive_outer_wrapper_resource_and_shared_budget_admission']}


def run_production(*args,**kwargs):
    require_production_host()
    raise Blocked('native_source_profile_actual_admission_missing_before_IO')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--describe-contract',action='store_true')
    args=parser.parse_args(argv)
    if args.describe_contract:print(encode(contract()).decode(),end='');return 0
    run_production();return 2


if __name__=='__main__':raise SystemExit(main())
