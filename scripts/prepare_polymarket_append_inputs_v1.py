"""Fixed append-only control proof PLAN; no physical-source authentication/I/O.

The pure reader callback supplies bounded bytes, never a trusted admission flag.
Only root's forthcoming authenticated production bootstrap may reopen physical
identities, source epochs, complete control closure and finite resource limits.
This module never opens a trade/header payload, requests providers or writes data.
"""
from copy import deepcopy
import base64
import hashlib
import json
from pathlib import PurePosixPath as Path
import re
import time
import zlib

from production_guard import require_production_host
from scripts import collect_polymarket_native_increment as old
from scripts import collect_polymarket_native_increment_v2 as adaptive
from scripts import prepare_polymarket_native_clock_layer_v1 as header_adapter
from scripts import project_polymarket_append_rows_v1 as projection

SCHEMA = 'polymarket_append_input_proof_plan_v1'
BASE = '/mnt/data/runs/'
CAMPAIGN = BASE + '2026-10-26_polymarket_native_append_campaign_v2'
INHERITED = BASE + '2026-10-26_polymarket_native_append_campaign_v1'
PROBE = BASE + '2026-10-26_polymarket_native_append_probe_v2'
BASELINE = BASE + '2026-10-09_polymarket_wallet_attribution_repair_v1'
HEADER = BASE + '2026-10-29_polymarket_exact_header_campaign_v8'
OP = BASE + '2026-10-27_polymarket_oct1_finalization_v1'
FIRST, LAST, CUTOFF = 88978538, 94796039, 1790899200
APPEND_ROWS, APPEND_FILES, BASELINE_ROWS = 473342502, 116364, 2036128538
CAPTURE_BYTES = 289060
CAPTURE_SHA = '4a7666eb7d6475e0e679852f2ef4e9f7dd16265d20964b2a2fcc53d8566522e0'
MAX_CONTROL = 4 * 1024**2
MAX_FIXTURE_READ = 32 * 1024**2
MAX_REQUESTS = 4096
SCOPE = {'production_admitted': False, 'callback_is_physical_authentication': False,
         'current_remote_identities_verified': False, 'data_body_reads': 0,
         'provider_requests': 0, 'new_collection_authorized': False,
         'baseline_rewritten': False, 'deduplication_performed': False,
         'producer_source_Git_blobs_authenticated': False, 'all_payload_hashes_verified': False}


class InputBlocked(ValueError): pass


def require(value, code):
    if not value: raise InputBlocked(code)


def digest(raw): return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()


def _object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate_JSON_key')
        value[key] = item
    return value


def decode(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_CONTROL, 'bounded_control_bytes')
    try:
        return json.loads(raw, object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(InputBlocked('nonfinite_JSON')))
    except (UnicodeError, json.JSONDecodeError):
        raise InputBlocked('invalid_control_JSON') from None


def integer(value, minimum=0): return type(value) is int and minimum <= value < 2**64


def literal(value, *, absolute=True):
    require(type(value) is str and 0 < len(value) <= 4096 and
            all(ord(c) >= 32 and not 127 <= ord(c) <= 159 for c in value), 'unsafe_control_path')
    path = Path(value)
    require(path.is_absolute() is absolute and str(path) == value and '..' not in path.parts,
            'unsafe_control_path')
    return value


def ref(value, root=None):
    require(type(value) is dict and set(value) == {'path', 'bytes', 'sha256'}, 'strict_reference_schema')
    path = literal(value['path'], absolute=root is None)
    require(integer(value['bytes'], 1) and type(value['sha256']) is str and
            re.fullmatch('[0-9a-f]{64}', value['sha256']) is not None, 'reference_types')
    return {'path': path if root is None else root + '/' + path,
            'bytes': value['bytes'], 'sha256': value['sha256']}


def _anchor(path, size, sha): return {'path': path, 'bytes': size, 'sha256': sha}


ANCHORS = {
 'campaign': _anchor(CAMPAIGN+'/manifests/bc81639712b44761b2dfcc39816bd777.json',706309,'c5611226d9beb7c81dad4b2a5191edb2dbf3cfd987fbfb9a32b4d5c18a0e952e'),
 'campaign_complete': _anchor(CAMPAIGN+'/COMPLETE.json',359,'7791859102be55a8272ae494b4e3b7c8f5e0d00f9945c986fe191ac977716280'),
 'probe': _anchor(PROBE+'/manifest.json',90745,'92af7ab19e0de9deab0f4237b09343cbc393eb7a6ff545afa175a562aaf9477c'),
 'probe_complete': _anchor(PROBE+'/COMPLETE.json',295,'0ea90e72382f2fd878e53b80f9ed1a972f537ec3b6918d540af1f661dc5a4564'),
 'source_plan': _anchor(BASE+'2026-10-26_polymarket_reboot_admission_v1/fresh_source_plan.json',19108,'b07dfa71169029913a2281a2cb9df3d567d620be89aaa14b84eca5e674e0f688'),
 'headers': _anchor(HEADER+'/manifest.json',159659,'2ce001422ec2de18bd3cef8521b510a1c24f3a8f930ae238c091729a43fe69c0'),
 'headers_complete': _anchor(HEADER+'/COMPLETE.json',314,'9de9c2e80e6124ef86e18b7bf7ecebe48cc9c8ace82e0c08b12d1b543819b323'),
 'header_acceptance': _anchor(OP+'/full_header_v8_terminal_acceptance_v1.json',5491,'1f4efc3734bf87e6e7adeaf4adee8f720593382ec0d9847eaffb955c01f1aed3'),
 'baseline': _anchor(BASELINE+'/manifest.json',1582547,'f142313238e9be25541f97dac4ba24de7ad6b49484812e81e7c6640eefd50daa'),
}


def captured_controls(raw):
    """Decode ONLY the exact root capture; identities are historical, not fresh."""
    require(type(raw) is bytes and len(raw) == CAPTURE_BYTES and digest(raw) == CAPTURE_SHA, 'actual_capture_pin')
    capture = decode(raw); records = capture.get('controls')
    require(type(records) is list and len(records) == len(ANCHORS), 'exact_nine_controls')
    expected = {r['path']: r for r in ANCHORS.values()}; result = {}
    for record in records:
        require(type(record) is dict and set(record) == {'path','bytes','sha256','identity','raw_zlib_base64'},
                'capture_record_schema')
        pin = {key: record[key] for key in ('path','bytes','sha256')}
        require(pin == expected.get(record['path']) and record['path'] not in result, 'capture_control_roster')
        require(type(record['raw_zlib_base64']) is str and len(record['raw_zlib_base64']) <= MAX_CONTROL * 2,
                'compressed_control_bound')
        try:
            compressed = base64.b64decode(record['raw_zlib_base64'], validate=True)
            decompressor = zlib.decompressobj()
            body = decompressor.decompress(compressed, pin['bytes'] + 1)
        except (ValueError, zlib.error):
            raise InputBlocked('compressed_control_invalid') from None
        require(decompressor.eof and not decompressor.unused_data and not decompressor.unconsumed_tail
                and len(body) == pin['bytes'] and digest(body) == pin['sha256'], 'decompressed_control_pin')
        result[record['path']] = body
    return result


class Reader:
    """Fixture byte channel only, NOT a physical source/receipt authority."""
    def __init__(self, callback, clock=time.monotonic):
        require(callable(callback) and callable(clock), 'reader_callable')
        self.callback, self.clock, self.start = callback, clock, clock()
        self.bytes = 0; self.calls = 0; self.saved = {}

    def check(self): require(self.clock() - self.start < 30, 'fixture_control_runtime_limit')

    def read(self, path, *, expected=None, optional=False):
        literal(path); self.check(); self.calls += 1
        require(self.calls <= MAX_REQUESTS, 'fixture_control_request_limit')
        raw = self.callback({'path': path, 'maximum_bytes': MAX_CONTROL})
        self.check()
        if raw is None:
            require(optional, 'required_control_absent'); return None
        require(type(raw) is bytes and len(raw) <= MAX_CONTROL, 'bounded_reader_bytes')
        self.bytes += len(raw); require(self.bytes <= MAX_FIXTURE_READ, 'fixture_control_read_limit')
        pin = _anchor(path, len(raw), digest(raw)); ref(pin)
        require(expected is None or pin == ref(expected), 'nested_control_pin')
        require(path not in self.saved or self.saved[path] == pin, 'reader_control_changed')
        self.saved[path] = pin
        return raw

    def reopen(self):
        for path, pin in list(self.saved.items()): self.read(path, expected=pin)
        self.check()


def _baseline(value):
    require(value.get('schema_version') == 'polymarket_wallet_repair_v1' and value.get('status') == 'repair_complete'
            and value.get('rows',{}).get('clean') == BASELINE_ROWS, 'baseline_status_or_total')
    records = value.get('outputs'); require(type(records) is list and len(records) == 88, 'baseline_88_outputs')
    clean = [r for r in records if r.get('relation') == 'clean']
    require(len(clean) == 44, 'baseline_44_clean_outputs')
    schema = '\n'.join(name+': '+kind for name, kind in projection.CLEAN_SCHEMA)
    result = []; months = []
    for record in clean:
        month = record.get('month'); require(type(month) is str and re.fullmatch(r'20[0-9]{2}-(?:0[1-9]|1[0-2])',month), 'baseline_month')
        require(record.get('path') == 'clean/year_month='+month, 'baseline_exact_partition_path')
        output = record.get('output'); require(type(output) is dict and output.get('path') == 'data.parquet'
            and output.get('schema') == schema and integer(output.get('rows')), 'baseline_eleven_physical_fields')
        pin = ref({key: output[key] for key in ('path','bytes','sha256')}, BASELINE+'/'+record['path'])
        result.append(pin | {'rows':output['rows'],'year_month':month,'physical_schema':schema})
        months.append(month)
    require(len(set(months)) == 44 and months == sorted(months) and months[0] == '2022-11'
            and months[-1] == '2026-06' and sum(r['rows'] for r in result) == BASELINE_ROWS, 'baseline_month_or_row_conservation')
    return result


def _source_plan(value):
    require(value.get('schema_version') == 'polymarket_refresh_source_plan_v1' and
            value.get('status') == 'source_plan_ready_for_review' and value.get('collection_approved') is False,
            'source_plan_is_provenance_not_collection_authority')
    require(value.get('end_exclusive_unix_seconds') == CUTOFF and value.get('end_exclusive_utc') == '2026-10-02T00:00:00Z'
            and value.get('provisional_append_start',{}).get('start_block') == FIRST
            and value.get('provisional_append_start',{}).get('raw_footer_declared_maximum_block') == FIRST-1,
            'append_frontier_or_cutoff')
    last, excluded = value['last_included_header'], value['first_excluded_header']
    require(type(last['number']) is int and last['number'] == LAST and type(last['timestamp']) is int and last['timestamp'] == CUTOFF-1
            and type(excluded['number']) is int and excluded['number'] == LAST+1 and type(excluded['timestamp']) is int
            and excluded['timestamp'] == CUTOFF and excluded['parentHash'] == last['hash']
            and set(value['cutoff_proof']) == {'adjacent_block_numbers','first_time_at_or_after_cutoff',
                'last_time_strictly_before_cutoff','parent_hash_matches'}
            and all(v is True for v in value['cutoff_proof'].values()),
            'adjacent_exact_cutoff_proof')


def _windows(manifest, root, first, last, version):
    records = manifest.get('windows'); require(type(records) is list and 0 < len(records) <= 1000, 'bounded_child_windows')
    contract = manifest['contract']; addresses = tuple(old.pilot.ADDRESSES)
    require(contract.get('schema_version') == f'polymarket_native_collection_contract_v{version}', 'contract_schema_epoch')
    for item in records:
        window=item.get('window',{})
        require(type(window) is dict and set(window) == {'window','first_block','last_block','blocks'}
                and integer(window['first_block'],1) and integer(window['last_block'],1) and integer(window['blocks'],1)
                and first <= window['first_block'] <= window['last_block'] <= last
                and window['blocks'] == window['last_block']-window['first_block']+1 <= 50
                and window['window'] == f"b{window['first_block']:012d}-{window['last_block']:012d}", 'typed_exact_window')
        require(type(item.get('evidence')) is dict and integer(item['evidence'].get('rows'))
                and type(item['evidence'].get('counts')) is list
                and all(type(c) is dict and integer(c.get('rows')) for c in item['evidence']['counts'])
                and type(item.get('contract_intervals')) is list
                and all(type(c) is dict and integer(c.get('rows')) for c in item['contract_intervals']), 'typed_window_count')
    try:
        if version == 1:
            old.verify_coverage(records, old.windows(first,last,50), addresses)
        else:
            roots = old.windows(first,last,50)
            require(manifest.get('roots') == roots, 'adaptive_child_roots')
            adaptive.verify_tree(roots, manifest['splits'], records)
    except old.pilot.PilotBlocked as error:
        raise InputBlocked(str(error)) from None
    result=[]; ledgers=[]; controls=[]; paths=set()
    for item in sorted(records,key=lambda r:r['window']['first_block']):
        attempt = item.get('attempt'); require(integer(attempt,1) and attempt <= 3, 'window_attempt_bound')
        window=item['window']; rows=item['evidence'].get('rows')
        require(integer(rows) and item.get('contract_sha256') == manifest['contract_sha256'], 'window_count_or_contract')
        parent = ('attempts/'+window['window']+'.a'+str(attempt) if version == 1 else
                  'nodes/'+window['window']+'/a'+str(attempt))
        for key, name in (('payload','payload.parquet'),('request_ledger','REQUESTS.json')):
            require(item[key].get('path') == parent+'/'+name, 'exact_window_relative_reference')
        payload=ref(item['payload'],root); ledger=ref(item['request_ledger'],root)
        require(payload['path'] not in paths and ledger['path'] not in paths, 'duplicate_payload_or_ledger')
        paths.update((payload['path'],ledger['path']))
        result.append(payload|{'rows':rows,'first_block':window['first_block'],'last_block':window['last_block']})
        ledgers.append(ledger)
        success=encode(item);controls.append(_anchor(root+'/'+parent+'/SUCCESS.json',len(success),digest(success)))
    require(sum(r['rows'] for r in result) == manifest.get('rows') == manifest.get('distinct_native_keys'), 'child_row_conservation')
    if version == 2:
        for item in manifest['splits']:
            ledgers.append(ref(item['request_ledger'],root))
            raw=encode(item);controls.append(_anchor(root+'/nodes/'+item['window']['window']+'/SPLIT.json',len(raw),digest(raw)))
    return {'payloads':result,'request_ledgers':ledgers,'success_and_split_controls':controls}


def _probe(value, complete):
    require(value.get('schema_version') == 'polymarket_native_collection_manifest_v1'
            and value.get('status') == 'raw_collection_complete' and value.get('collection_complete') is True
            and value.get('raw_payload_only') is True and value.get('analysis_admitted') is False
            and type(value.get('rows')) is int and value['rows'] == 146230, 'probe_raw_completion')
    require(complete.get('manifest_sha256') == ANCHORS['probe']['sha256'] and complete.get('run_id') == value.get('run_id')
            and complete.get('contract_sha256') == value.get('contract_sha256')
            and complete.get('status') == 'raw_collection_complete_not_analysis_admission', 'probe_complete_binding')
    contract=value['contract'];_contract(contract,FIRST,88979537)
    require(value['contract_sha256'] == digest(encode(contract)), 'probe_contract_hash')
    result=_windows(value,PROBE,FIRST,88979537,1)
    require(len(result['payloads']) == 20, 'probe_twenty_payloads')
    return result|{'producer_head':contract['source']['head']}


def _contract(contract, first, last):
    require(type(contract) is dict and contract.get('mode') == 'append' and contract.get('first_block') == first
            and contract.get('last_block') == last and contract.get('end_exclusive_utc') == '2026-10-02T00:00:00Z'
            and contract.get('addresses') == list(old.pilot.ADDRESSES)
            and contract.get('topics') == sorted(set(old.pilot.EXPECTED_TOPICS.values()))
            and contract.get('raw_payload_only') is True and contract.get('analysis_admitted') is False
            and contract.get('provider_block_timestamp_used_as_clock') is False, 'child_contract_scope')
    source=contract.get('source',{});require(type(source.get('head')) is str and re.fullmatch('[0-9a-f]{40}',source['head'])
            and source.get('status') == 'inspected' and source.get('tracked_worktree_clean') is True, 'child_producer_epoch')


def _campaign(value, complete):
    require(value.get('schema_version') == 'polymarket_native_campaign_manifest_v2' and value.get('status') == 'raw_campaign_complete'
            and value.get('collection_complete') is True and value.get('raw_payload_only') is True
            and value.get('analysis_admitted') is False and type(value.get('rows')) is int and value['rows'] == 473196272,
            'campaign_raw_completion')
    require(complete.get('manifest_path') == ANCHORS['campaign']['path'][len(CAMPAIGN)+1:]
            and complete.get('manifest_sha256') == ANCHORS['campaign']['sha256']
            and complete.get('run_id') == value.get('run_id') and complete.get('plan_sha256') == value.get('plan_sha256')
            and complete.get('status') == 'raw_campaign_complete_not_analysis_admission', 'campaign_complete_binding')
    records=value.get('accepted_chunks');require(type(records) is list and len(records) == 582, 'campaign_582_seals')
    result=[]; cursor=88979538; total=0; inherited=[]
    for index,item in enumerate(records,1):
        chunk={'chunk':f'c{index:06d}','first_block':cursor,'last_block':min(LAST,cursor+9999)}
        require(item.get('chunk') == chunk and item.get('status') == 'accepted_raw_child', 'campaign_chunk_gap_duplicate_or_order')
        accepted=item.get('accepted',{});require(integer(accepted.get('rows')) and accepted.get('first_block') == cursor
            and accepted.get('last_block') == chunk['last_block'] and type(accepted.get('manifest_sha256')) is str
            and re.fullmatch('[0-9a-f]{64}',accepted['manifest_sha256']), 'accepted_child_fields')
        entry={'chunk':chunk,'rows':accepted['rows'],'manifest_sha256':accepted['manifest_sha256']}
        if 'external_attempt' in item:
            path=literal(item['external_attempt'])
            require(path == INHERITED+'/chunks/'+chunk['chunk']+'/a01' and
                    item.get('producer_head') == '19202a820bef569f449a34fc731107f16470e155', 'inherited_literal_path_or_epoch')
            entry.update(attempt_candidates=[path],producer_head=item['producer_head'],collector_version=1)
            inherited.append(index)
        else:
            require(item.get('exit_code') == 0 and type(item.get('exit_code')) is int and item.get('error_code') is None
                    and item.get('plan_sha256') == value['plan_sha256'], 'fresh_parent_exit_fields')
            raw=encode(item)
            entry.update(attempt_candidates=[CAMPAIGN+'/chunks/'+chunk['chunk']+f'/a{a:02d}' for a in (1,2,3)],
                expected_exit_bytes=len(raw),expected_exit_sha256=digest(raw),producer_head=None,collector_version=2)
        result.append(entry);cursor=chunk['last_block']+1;total+=accepted['rows']
    require(cursor == LAST+1 and total == value['rows'] and inherited == [*range(1,18),19,20], 'campaign_range_rows_or_inherited_set')
    return result


def _headers(raw, manifest, complete):
    accepted=header_adapter.header_terminal_adapter(raw,ANCHORS['header_acceptance'])
    require(accepted['direct_bindings']['manifest'] == ANCHORS['headers'] and
            accepted['direct_bindings']['complete'] == ANCHORS['headers_complete'], 'accepted_header_manifest_binding')
    require(complete.get('manifest') == {'path':'manifest.json','bytes':ANCHORS['headers']['bytes'],'sha256':ANCHORS['headers']['sha256']}
            and complete.get('schema_version') == 'polymarket_exact_header_campaign_completion_v8'
            and complete.get('status') == 'literal_exact_header_campaign_complete_raw_clocks_only', 'header_complete_binding')
    chunks=manifest.get('chunks');require(type(chunks) is dict and set(chunks) == {str(i) for i in range(330)}
            and manifest.get('accepted_keys') == 32901442 and manifest.get('exact_key_coverage') is True
            and manifest.get('missing_keys') == 0 and manifest.get('duplicate_keys') == 0
            and manifest.get('raw_clocks_only') is True and manifest.get('final_sample_admitted') is False, 'header_seal_counts')
    result=[]
    for i in range(330):
        item=chunks[str(i)];bound=item['headers'];pin=ref({key:bound[key] for key in ('path','bytes','sha256')})
        root=literal(item['root']); require(pin['path'] == root+'/headers.parquet' and type(item['keys']) is int
            and item['keys'] == min(100000,32901442-i*100000), 'header_parquet_reference_or_width')
        result.append(pin|{'keys':item['keys'],'chunk':i})
    require(len({r['path'] for r in result}) == 330, 'duplicate_header_parquet')
    return result


def prepare_plan(callback):
    """No payload reads; output is a fixed proof/request plan, never admission."""
    reader=Reader(callback);raws={key:reader.read(pin['path'],expected=pin) for key,pin in ANCHORS.items()}
    values={key:decode(raw) for key,raw in raws.items()}
    baseline=_baseline(values['baseline']);_source_plan(values['source_plan'])
    probe=_probe(values['probe'],values['probe_complete'])
    children=_campaign(values['campaign'],values['campaign_complete'])
    require(sum(r['rows'] for r in probe['payloads'])+sum(r['rows'] for r in children) == APPEND_ROWS,
            'append_parent_probe_row_conservation')
    headers=_headers(raws['header_acceptance'],values['headers'],values['headers_complete'])
    result={'schema_version':SCHEMA,'status':'append_only_control_plan_not_production_admission',
        'anchors':deepcopy(ANCHORS),'baseline':baseline,'baseline_rows':BASELINE_ROWS,'baseline_physical_columns':11,
        'baseline_partition_field':'year_month','first_block':FIRST,'last_block':LAST,'cutoff_exclusive_unix':CUTOFF,
        'expected_append_rows':APPEND_ROWS,'expected_append_payload_files':APPEND_FILES,
        'probe':probe,'campaign_children_to_resolve':children,'header_parquets':headers,
        'derived_payload_files_so_far':len(probe['payloads']),'campaign_payload_file_count_derived':False,
        'all_child_manifests_expanded':False,'remaining_gates':['resolve_each_child_and_reconcile_exact_116364_files_and_473342502_rows',
            'replay_request_success_split_receipts_and_authenticate_all_producer_source_epochs',
            'fresh_physical_file_identity_full_payload_SHA_and_finite_global_read_runtime_RAM_output_admission',
            'exact_header_join_historical_retention_full_value_DISTINCT_overlap_and_atomic_release'],**SCOPE}
    reader.reopen();result['fixture_control_bytes_read']=reader.bytes
    return result


def resolve_child(request, callback):
    """Expand one supplied parent-derived request; all resulting pins are controls.

    This performs no full RPC-journal replay or body/file-identity validation.
    A caller-created request/dictionary is NOT external source authentication.
    """
    require(type(request) is dict and type(request.get('collector_version')) is int
            and request['collector_version'] in (1,2), 'child_request_schema')
    chunk=request['chunk'];version=request['collector_version'];reader=Reader(callback)
    fields={'chunk','rows','manifest_sha256','attempt_candidates','producer_head','collector_version'}
    require(set(request) == fields | ({'expected_exit_bytes','expected_exit_sha256'} if version == 2 else set()), 'child_request_exact_fields')
    require(type(chunk) is dict and set(chunk) == {'chunk','first_block','last_block'}
            and type(chunk['chunk']) is str and re.fullmatch('c[0-9]{6}',chunk['chunk'])
            and integer(chunk['first_block'],1) and integer(chunk['last_block'],1)
            and FIRST <= chunk['first_block'] <= chunk['last_block'] <= LAST
            and chunk['last_block']-chunk['first_block'] < 10000 and integer(request['rows'])
            and type(request['manifest_sha256']) is str and re.fullmatch('[0-9a-f]{64}',request['manifest_sha256']), 'child_request_types')
    candidates=request['attempt_candidates'];require(type(candidates) is list and len(candidates) == (1 if version == 1 else 3), 'child_attempt_candidates')
    expected_root=INHERITED if version == 1 else CAMPAIGN
    require(candidates == [expected_root+'/chunks/'+chunk['chunk']+f'/a{a:02d}' for a in ((1,) if version == 1 else (1,2,3))], 'exact_attempt_candidate_set')
    if version == 2:
        require(integer(request['expected_exit_bytes'],1) and request['expected_exit_bytes'] <= MAX_CONTROL
                and type(request['expected_exit_sha256']) is str and re.fullmatch('[0-9a-f]{64}',request['expected_exit_sha256'])
                and request['producer_head'] is None, 'fresh_EXIT_request_types')
    else:
        require(request['producer_head'] == '19202a820bef569f449a34fc731107f16470e155', 'inherited_producer_request_epoch')
    matches=[]
    for candidate in candidates:
        literal(candidate)
        expected_root=INHERITED if version == 1 else CAMPAIGN
        require(candidate in [expected_root+'/chunks/'+chunk['chunk']+f'/a{a:02d}' for a in (1,2,3)], 'child_attempt_escape')
        if version == 1: matches.append(candidate);continue
        raw=reader.read(candidate+'/EXIT.json',optional=True)
        if raw is not None and len(raw) == request['expected_exit_bytes'] and digest(raw) == request['expected_exit_sha256']:
            matches.append(candidate)
    require(len(matches) == 1, 'unique_parent_matching_child_EXIT_required')
    root=matches[0]+'/raw';manifest_raw=reader.read(root+'/manifest.json')
    require(digest(manifest_raw) == request['manifest_sha256'], 'child_manifest_parent_SHA')
    manifest=decode(manifest_raw);owner=decode(reader.read(root+'/OWNER.json'));complete=decode(reader.read(root+'/COMPLETE.json'))
    require(manifest.get('schema_version') == f'polymarket_native_collection_manifest_v{version}'
            and manifest.get('status') == 'raw_collection_complete' and manifest.get('collection_complete') is True
            and manifest.get('raw_payload_only') is True and manifest.get('analysis_admitted') is False, 'child_raw_manifest_schema')
    contract=manifest['contract'];_contract(contract,chunk['first_block'],chunk['last_block']);sha=digest(encode(contract))
    require(manifest.get('contract_sha256') == owner.get('contract_sha256') == complete.get('contract_sha256') == sha
            and owner.get('contract') == contract and owner.get('run_id') == manifest.get('run_id') == complete.get('run_id')
            and complete.get('manifest_sha256') == request['manifest_sha256']
            and complete.get('status') == 'raw_collection_complete_not_analysis_admission', 'child_OWNER_COMPLETE_linkage')
    require(request.get('producer_head') in (None,contract['source']['head']), 'inherited_producer_epoch_changed')
    require(integer(manifest.get('rows')) and manifest['rows'] == request['rows'], 'parent_child_row_count')
    result=_windows(manifest,root,chunk['first_block'],chunk['last_block'],version)
    reader.reopen()
    return result|{'chunk':deepcopy(chunk),'producer_head':contract['source']['head'],'root':root,
        'rows':manifest['rows'],'manifest':deepcopy(reader.saved[root+'/manifest.json']),
        'OWNER':deepcopy(reader.saved[root+'/OWNER.json']),'COMPLETE':deepcopy(reader.saved[root+'/COMPLETE.json']),
        'full_request_journal_replayed':False,'fixture_control_bytes_read':reader.bytes,**SCOPE}


def run_production(*_args, **_kwargs):
    require_production_host()
    raise InputBlocked('authenticated_append_input_production_bootstrap_not_admitted')


if __name__ == '__main__': run_production()
