"""LOCAL fixture-only proof adapters for the row-preserving clock-layer candidate.

No invocation in this module admits production input I/O. The public production
entrypoint guards Linux/canonical checkout/mount, then blocks before opening an
input. Fixture replay calls the existing sealed-lineage replayer, not a supplied
acceptance callback. Its result is explicitly untrusted for production, even
when every synthetic control and identity is reopened successfully.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import build_polymarket_native_clock_layer_v1 as core
from scripts import build_polymarket_exact_header_roster_v1 as roster
from scripts import inventory_polymarket_exact_header_keys_v1 as inventory
from scripts import probe_polymarket_protocol_activity as protocol

Blocked = core.Blocked
require, encode, decode, digest = core.require, core.encode, core.decode, core.digest
SCHEMA = 'polymarket_native_clock_proof_preparation_candidate_v1'
OP = '/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1'
HEADER_TERMINAL = {'path': OP+'/full_header_v8_terminal_acceptance_v1.json', 'bytes': 5491,
    'sha256': '1f4efc3734bf87e6e7adeaf4adee8f720593382ec0d9847eaffb955c01f1aed3'}
HEADER_HEAD = '443e3448cccfc22e2e75e86ea075699af1478ebc'
KEYS_REQUIRED, NATIVE_HASHES_REQUIRED, HEADER_SEALS_REQUIRED = 32901442, 5884493, 330
KNOWN_DESCRIPTOR = {'path': OP+'/header_roster_descriptor_v1.json',
    'sha256': 'cbcbf530213cfc0f44d58da5c07ab997ce812a3fe8885da77d28ffc70480afee'}
KNOWN_ROSTER = {'path': '/mnt/data/runs/2026-10-27_polymarket_exact_header_roster_v1/roster.json',
    'bytes': 65678058, 'sha256': '1e0e9663a4507f2ba1d5d478c7774d1a212823f315ef17a123fc8a3ea84c02cb'}
FAILURE_FIELDS = {'null_block_rows', 'invalid_block_rows', 'null_hash_rows',
    'invalid_hash_rows', 'conflicting_hash_blocks'}
FIXTURE_CONTROL_BYTES = 8 * 1024**2
FIXTURE_READ_BYTES = 64 * 1024**2
FIXTURE_FILES = 256
BLOCKERS = (
    'actual_descriptor_and_inventory_controls_not_available_as_small_local_mirrors',
    'actual_full_lineage_roster_inventory_and_header_closure_replay_not_executed',
    'original_source_epoch_git_blob_proof_adapter_not_reviewed',
    'legacy_full_content_binding_and_finite_read_policy_not_admitted',
    'production_physical_parquet_stream_and_global_replay_conflict_index_not_reviewed',
    'finite_actual_runtime_read_RAM_retained_scratch_spill_feasibility_not_admitted',
    'production_exact_namespace_atomic_publication_and_final_reopens_not_reviewed',
)


def contract():
    return {'schema_version': SCHEMA, 'status': 'local_fixture_adapter_not_production_admission',
        'production_admitted': False, 'production_input_IO_enabled': False,
        'actual_header_receipt': deepcopy(HEADER_TERMINAL),
        'known_missing_descriptor': deepcopy(KNOWN_DESCRIPTOR), 'known_roster_not_read_locally': deepcopy(KNOWN_ROSTER),
        'observed_hash_distinct_from_joined_hash': True, 'physical_multiplicity_preserved': True,
        'legacy_footer_identity_is_full_payload_SHA': False,
        'inherited_envelope_not_new_admission': {'retained_and_scratch_bytes': 64000000000,
            'spill_bytes': 16000000000, 'minimum_free_bytes': 20000000000},
        'production_blockers': list(BLOCKERS), 'archive_or_sample_admitted': False,
        'resource_or_execution_receipt': False}


def reference(value):
    require(type(value) is dict and set(value) == {'path', 'bytes', 'sha256'}, 'proof_reference_schema')
    path = value.get('path')
    require(type(path) is str and Path(path).is_absolute() and str(Path(path)) == path
        and '..' not in Path(path).parts, 'proof_reference_literal_path')
    require(type(value['bytes']) is int and value['bytes'] > 0
        and type(value['sha256']) is str and roster.SHA.fullmatch(value['sha256']), 'proof_reference_bytes_or_SHA')
    return deepcopy(value)


def header_terminal_adapter(raw, ref):
    """Authenticate the exact published receipt, not caller acceptance booleans.

    This pure adapter does not reopen its remote closure or claim that a local
    copy proves current remote file identities. The parent independently reopened
    that closure; a future production consumer must do its own final reopen.
    """
    require(reference(ref) == HEADER_TERMINAL, 'not_actual_published_terminal_reference')
    require(type(raw) is bytes and len(raw) == ref['bytes'] and digest(raw) == ref['sha256'],
        'terminal_receipt_bytes_or_SHA')
    value = decode(raw)
    require(type(value) is dict and value.get('schema_version') == 'polymarket_header_v8_terminal_acceptance_v2'
        and value.get('status') == 'whole_literal_header_campaign_independently_verified_raw_clocks_only'
        and value.get('source_head') == HEADER_HEAD, 'terminal_receipt_epoch_or_status')
    for name, expected in (('required_keys_checked', KEYS_REQUIRED),
            ('native_expected_hash_assertions_checked', NATIVE_HASHES_REQUIRED),
            ('sealed_chunks_checked', HEADER_SEALS_REQUIRED), ('old_sealed_chunks_checked', 102),
            ('fresh_sealed_chunks_checked', 228), ('provider_requests_sent', 0)):
        require(type(value.get(name)) is int and value[name] == expected, 'terminal_exact_counts')
    for name in ('integer_clocks_and_cross_chunk_parents_checked', 'journals_retry_diagnostics_and_charges_checked',
            'current_exact_namespace_checked', 'full_declared_closure_and_final_reopens_checked',
            'raw_clocks_only', 'published_to_reserved_terminal_slot', 'historical_prime_missing_preserved'):
        require(value.get(name) is True, 'terminal_required_gate')
    for name in ('final_sample_admitted', 'history_completeness_certified',
            'cryptographic_header_verification', 'historical_prime_required_retroactively'):
        require(value.get(name) is False, 'terminal_scope_not_raw_clocks')
    direct = {name: reference(value[name]) for name in ('admission', 'dispatch', 'manifest', 'complete', 'caller_exit')}
    return {'schema_version': 'polymarket_native_clock_header_receipt_binding_v1',
        'receipt': deepcopy(ref), 'direct_bindings': direct, 'receipt_source_head': value['source_head'],
        'required_keys': value['required_keys_checked'], 'native_expected_hashes': value['native_expected_hash_assertions_checked'],
        'sealed_chunks': value['sealed_chunks_checked'], 'actual_receipt_bytes_authenticated': True,
        'current_remote_closure_reopened_here': False, 'production_admitted': False}


def bind_header_admission(terminal_raw, admission_raw, inputs):
    """Link explicit inventory/roster inputs to the actual accepted admission.

    Input bytes are still subject to their own adapters; references alone do not
    certify the required-key Parquet or source payload bodies.
    """
    # Re-authenticate raw terminal bytes; never trust a caller-created adapter
    # dictionary or an `accepted=True` assertion as an authentication token.
    adapter = header_terminal_adapter(terminal_raw, HEADER_TERMINAL)
    bound = reference(adapter['direct_bindings']['admission'])
    require(type(admission_raw) is bytes and len(admission_raw) == bound['bytes']
        and digest(admission_raw) == bound['sha256'], 'admission_bytes_or_SHA')
    admission = decode(admission_raw)
    require(type(admission) is dict and admission.get('consumer_head') == HEADER_HEAD
        and type(admission.get('actual_keys')) is int and admission['actual_keys'] == KEYS_REQUIRED
        and admission.get('raw_clocks_only') is True and admission.get('final_sample_admitted') is False,
        'header_admission_scope')
    names = {'inventory_complete', 'inventory_manifest', 'keys', 'roster'}
    require(type(inputs) is dict and set(inputs) == names, 'native_header_input_roster')
    result = {name: reference(inputs[name]) for name in names}
    require(all(admission.get('inputs', {}).get(name) == result[name] for name in names),
        'native_header_inputs_not_accepted_bindings')
    return result


def inventory_control_adapter(marker_raw, manifest_raw, inputs):
    """Pure actual-schema reconciliation; no key scan or body hash is claimed."""
    require(type(inputs) is dict and set(inputs) == {'inventory_complete', 'inventory_manifest', 'keys', 'roster'},
        'inventory_input_roster')
    inputs = {name: reference(value) for name, value in inputs.items()}
    for raw, name in ((marker_raw, 'inventory_complete'), (manifest_raw, 'inventory_manifest')):
        require(type(raw) is bytes and len(raw) == inputs[name]['bytes']
            and digest(raw) == inputs[name]['sha256'], 'inventory_control_bytes_or_SHA')
    marker, manifest = decode(marker_raw), decode(manifest_raw)
    root = Path(inputs['inventory_manifest']['path']).parent
    relative = lambda name: {'path': Path(inputs[name]['path']).name,
        **{key: inputs[name][key] for key in ('bytes', 'sha256')}}
    require(type(marker) is dict and type(manifest) is dict
        and marker.get('schema_version') == 'polymarket_exact_header_key_inventory_completion_v1'
        and manifest.get('schema_version') == 'polymarket_exact_header_key_inventory_v1'
        and marker.get('status') == manifest.get('status') == 'literal_header_key_inventory_complete'
        and Path(inputs['inventory_complete']['path']) == root/'COMPLETE.json'
        and marker.get('manifest') == relative('inventory_manifest'), 'inventory_not_sealed')
    require(marker.get('literal_roster_only') is True and marker.get('final_universe_admitted') is False
        and manifest.get('literal_roster_only') is True and manifest.get('final_universe_admitted') is False,
        'inventory_scope')
    failures = manifest.get('failure_counts')
    require(type(failures) is dict and set(failures) == FAILURE_FIELDS
        and all(type(number) is int and number == 0 for number in failures.values())
        and type(manifest.get('global_conflicting_hash_blocks')) is int
        and manifest['global_conflicting_hash_blocks'] == 0, 'inventory_failure_or_unknown_counter')
    require(type(manifest.get('required_keys')) is int and manifest['required_keys'] == KEYS_REQUIRED
        and type(manifest.get('source_files')) is int and manifest['source_files'] > 0
        and type(manifest.get('rows_declared')) is int and manifest['rows_declared'] >= 0
        and type(manifest.get('rows_scanned')) is int and manifest['rows_scanned'] == manifest['rows_declared'],
        'inventory_counts')
    require(manifest.get('required_keys_parquet') == relative('keys')
        and Path(inputs['keys']['path']) == root/'required_keys.parquet', 'inventory_key_binding')
    saved = manifest.get('roster')
    require(type(saved) is dict and set(saved) == {'path', 'identity', 'sha256'}
        and saved['path'] == inputs['roster']['path'] and saved['sha256'] == inputs['roster']['sha256'],
        'inventory_roster_binding')
    require(type(saved['identity']) is dict and set(saved['identity']) == inventory.STAT_FIELDS | {'bytes'}
        and all(type(number) is int and number >= 0 for number in saved['identity'].values())
        and saved['identity']['bytes'] == inputs['roster']['bytes'], 'inventory_roster_identity')
    require(all(manifest.get(name) is True for name in ('source_inputs_reopened', 'roster_reopened', 'source_reopened')),
        'inventory_saved_reopen_gates')
    source = manifest.get('source')
    require(type(source) is dict and set(source) == {'head', 'files', 'tracked_worktree_clean'}
        and type(source['head']) is str and roster.HEAD.fullmatch(source['head'])
        and source['tracked_worktree_clean'] is True and type(source['files']) is dict
        and set(source['files']) == set(inventory.SOURCE_FILES), 'inventory_source_epoch')
    for item in source['files'].values():
        require(type(item) is dict and set(item) == {'bytes', 'sha256'} and type(item['bytes']) is int
            and item['bytes'] > 0 and type(item['sha256']) is str and roster.SHA.fullmatch(item['sha256']),
            'inventory_source_blob_binding')
    return {'required_keys': manifest['required_keys'], 'source_files': manifest['source_files'],
        'physical_rows_reconciled_in_saved_inventory': manifest['rows_scanned'],
        'original_source_epoch': deepcopy(source), 'roster_identity_at_inventory': deepcopy(saved['identity']),
        'required_key_body_replayed_here': False, 'native_payload_body_rehashed_here': False,
        'production_admitted': False}


def endpoint_from_source_plan(plan):
    """Use the saved exact bracket, never the plan's summary proof booleans."""
    require(type(plan) is dict and plan.get('schema_version') == 'polymarket_refresh_source_plan_v1'
        and plan.get('status') == 'source_plan_ready_for_review'
        and plan.get('end_exclusive_utc') == core.END_EXCLUSIVE_UTC
        and type(plan.get('end_exclusive_unix_seconds')) is int
        and plan['end_exclusive_unix_seconds'] == core.CUTOFF_SECONDS, 'source_plan_endpoint_schema')
    try:
        pair = [protocol.header_fields(plan.get(name)) for name in ('last_included_header', 'first_excluded_header')]
    except protocol.ProbeBlocked:
        raise Blocked('source_plan_endpoint_header') from None
    result = {'end_exclusive_utc': core.END_EXCLUSIVE_UTC}
    for name, item in zip(('last_included', 'first_excluded'), pair):
        result[name] = {'block_number': item['number'], 'block_hash': item['hash'],
            'parent_hash': item['parentHash'], 'timestamp': item['timestamp']}
    core.fixture_headers([], result)  # Integer UTC and literal adjacency/parent checks.
    require(result['last_included']['block_number'] == roster.END, 'endpoint_not_frozen_source_end')
    return result


class FixtureControlReader(roster.Reader):
    """Explicit small temporary fixture namespace; NEVER a production reader.

    Declared controls are exact references, not discovered/adopted files. The
    complete file namespace also includes untouched synthetic payload files.
    Local identities here prove only these fixture files, not remote identities.
    """
    def __init__(self, root, controls, files):
        root = Path(root)
        temporary = Path(tempfile.gettempdir()).resolve()
        require(root.is_absolute() and root.resolve() == root and root != temporary
            and temporary in root.parents and not root.is_symlink(), 'fixture_root_only_not_production')
        require(type(controls) is list and 0 < len(controls) <= FIXTURE_FILES
            and type(files) is list and 0 < len(files) <= FIXTURE_FILES, 'fixture_namespace_bound')
        self.root, self.declared, self.declared_files = root, {}, set()
        for item in controls:
            bound = reference(item); path = Path(bound['path'])
            require(root in path.parents and bound['bytes'] <= FIXTURE_CONTROL_BYTES,
                'fixture_control_path_or_size')
            require(bound['path'] not in self.declared, 'fixture_duplicate_control')
            self.declared[bound['path']] = bound
        for name in files:
            require(type(name) is str and root in Path(name).parents and str(Path(name)) == name
                and '..' not in Path(name).parts and name not in self.declared_files, 'fixture_declared_file')
            self.declared_files.add(name)
        require(set(self.declared) <= self.declared_files, 'fixture_control_not_in_namespace')
        require(all(Path(name).suffix not in {'.json', '.jsonl'} or name in self.declared
            for name in self.declared_files), 'unknown_fixture_control_schema')
        self.read_bytes, self.source_files = 0, []
        super().__init__(time.monotonic(), time.monotonic)
        self.namespace()

    def namespace(self):
        actual, nodes = set(), 0
        for path in self.root.rglob('*'):
            nodes += 1
            require(nodes <= FIXTURE_FILES * 8, 'fixture_namespace_node_bound')
            require(not path.is_symlink(), 'fixture_namespace_symlink')
            if path.is_file():
                actual.add(str(path))
                require(len(actual) <= FIXTURE_FILES, 'fixture_namespace_bound')
            else:
                require(path.is_dir(), 'fixture_namespace_special_file')
        require(actual == self.declared_files, 'fixture_namespace_changed_or_unknown_file')

    def raw(self, ref, *, limit=None):
        bound = reference(ref)
        require(self.declared.get(bound['path']) == bound, 'unregistered_or_conflicting_fixture_control')
        require(bound['bytes'] <= min(limit or FIXTURE_CONTROL_BYTES, FIXTURE_CONTROL_BYTES)
            and self.read_bytes + bound['bytes'] <= FIXTURE_READ_BYTES, 'fixture_read_bound_not_production_cap')
        self.read_bytes += bound['bytes']
        return super().raw(bound, limit=FIXTURE_CONTROL_BYTES)

    def observed(self, path, *, limit=1024**2):
        bound = self.declared.get(str(path))
        require(bound is not None and bound['bytes'] <= limit, 'unknown_derived_fixture_control')
        self.raw(bound, limit=limit)
        return deepcopy(bound)

    def bind_sources(self, entries):
        require(type(entries) is list and 0 < len(entries) <= core.FIXTURE_MAX_FILES, 'fixture_source_bound')
        physical = set()
        for item in entries:
            entry = core.source_binding(item)
            require(entry['path'] in self.declared_files and self.root in Path(entry['path']).parents,
                'fixture_source_not_declared')
            key = entry['identity']['device'], entry['identity']['inode']
            require(key not in physical, 'duplicate_physical_source')
            physical.add(key)
            inventory.reopen_identity(entry)
        require(self.declared_files == set(self.declared) | {item['path'] for item in entries},
            'fixture_namespace_not_exact_controls_and_derived_sources')
        self.source_files = deepcopy(entries)

    def finish(self):
        require(set(self.controls) == set(self.declared), 'unknown_or_unconsumed_fixture_controls')
        self.namespace()
        for item in self.source_files:
            inventory.reopen_identity(item)
        # Primitive full bytes and identities LAST, after all namespace/stats.
        self.reopen()


def replay_source_roster_fixture(reader, bindings):
    """Derive refs/rows with the frozen replayer; no public replay callback.

    Only a temporary fixture reader is accepted. The actual 65.7-MB roster and
    real payloads cannot enter this interface. Missing controls raise, rather
    than converting caller flags into an accepted context.
    """
    require(type(reader) is FixtureControlReader, 'fixture_reader_required_production_IO_blocked')
    names = {'descriptor', 'roster', 'roster_manifest', 'roster_complete'}
    require(type(bindings) is dict and set(bindings) == names, 'roster_fixture_input_roster')
    refs = {name: reference(value) for name, value in bindings.items()}
    desc, listing, manifest, marker = (reader.json(refs[name]) for name in
        ('descriptor', 'roster', 'roster_manifest', 'roster_complete'))
    require(type(desc) is dict and set(desc) == {'schema_version', 'preflight', 'source_plan', 'legacy_inventory',
        'legacy_raw_path', 'initial_native', 'append_campaign', 'historical_full', 'header_ledger'}
        and desc['schema_version'] == 'polymarket_exact_header_roster_descriptor_v1', 'descriptor_schema')
    for name, fields in (('initial_native', {'root', 'owner', 'complete', 'manifest', 'producer_head'}),
            ('append_campaign', {'root', 'plan', 'complete', 'manifest', 'producer_head'}),
            ('historical_full', {'root', 'owner', 'complete', 'result', 'raw_owner', 'raw_complete', 'raw_manifest', 'producer_head'})):
        require(type(desc[name]) is dict and set(desc[name]) == fields, 'descriptor_source_schema')
    require(type(listing) is dict and set(listing) == {'schema_version', 'files'}
        and listing['schema_version'] == 'polymarket_exact_header_key_roster_v1'
        and type(listing['files']) is list, 'source_roster_schema')
    status = 'sealed_source_roster_complete_not_inventory_admission'
    root = Path(refs['roster_manifest']['path']).parent
    relative = lambda name: {'path': Path(refs[name]['path']).name,
        **{key: refs[name][key] for key in ('bytes', 'sha256')}}
    require(type(manifest) is dict and type(marker) is dict
        and manifest.get('schema_version') == 'polymarket_exact_header_roster_build_v1'
        and manifest.get('status') == marker.get('status') == status
        and manifest.get('descriptor') == refs['descriptor']
        and Path(refs['roster']['path']) == root/'roster.json'
        and Path(refs['roster_manifest']['path']) == root/'manifest.json'
        and Path(refs['roster_complete']['path']) == root/'COMPLETE.json'
        and manifest.get('roster') == relative('roster')
        and marker.get('manifest') == relative('roster_manifest')
        and all(item.get('final_universe_admitted') is False and item.get('analysis_admitted') is False
            for item in (manifest, marker)), 'roster_not_bound_sealed_generation')
    start = reader.json(reader.observed(root/'START.json'))
    require(type(start) is dict and start.get('schema_version') == manifest['schema_version']
        and start.get('status') == 'blocked' and start.get('descriptor') == refs['descriptor']
        and start.get('source') == manifest.get('source') and start.get('caps') == manifest.get('caps')
        and start.get('literal_source_generation_only') is True
        and start.get('final_universe_admitted') is False and start.get('analysis_admitted') is False,
        'roster_start_binding')
    preflight = reader.json(desc['preflight'])
    require(desc['preflight']['sha256'] == roster.PREFLIGHT_SHA
        and preflight.get('schema_version') == 'polymarket_header_source_roster_preflight_v1'
        and preflight.get('status') == 'sealed_roster_scale_preflight_complete_not_inventory_admission'
        and preflight.get('read_accounting', {}).get('control_json_bytes_including_prior_inspection_reads') == roster.PRIOR_JSON
        and preflight['read_accounting'].get('parquet_footer_bytes_read') == roster.PRIOR_FOOTER
        and type(preflight['read_accounting'].get('trade_payload_body_bytes_read')) is int
        and preflight['read_accounting']['trade_payload_body_bytes_read'] == 0
        and type(preflight['read_accounting'].get('payload_sha256_rehash_bytes')) is int
        and preflight['read_accounting']['payload_sha256_rehash_bytes'] == 0
        and preflight.get('final_sample_admitted') is False,
        'preflight_not_frozen')
    # This is the existing full historical/probe/582-grid/adaptive-window replay,
    # including preserved zero-log intervals and original producer-head checks.
    legacy, native = roster.lineage(reader, desc, preflight)
    entries = [core.source_binding(item) for item in listing['files']]
    derived = [(legacy, 'legacy_raw'), *[(item, 'native_raw') for item in native]]
    require(len(entries) == len(derived) == manifest.get('source_files'), 'roster_derived_file_count')
    paths = set()
    groups = {}
    for entry, (original, role) in zip(entries, derived):
        require(entry['path'] not in paths, 'duplicate_source_path')
        paths.add(entry['path'])
        require(entry['path'] == original['path'] and entry['bytes'] == original['bytes']
            and entry['source_role'] == role, 'roster_differs_from_derived_lineage')
        if role == 'native_raw':
            require(entry['payload_sha256'] == original['payload_sha256'], 'native_payload_binding_differs')
        else:
            require(entry['footer_sha256'] == original['footer_sha256']
                and entry['identity'] == {key: original['identity']['target'][key] for key in inventory.STAT_FIELDS},
                'legacy_footer_identity_not_frozen')
        require(type(original.get('rows_declared')) is int and original['rows_declared'] >= 0,
            'derived_physical_row_count')
        group = original.get('group', 'legacy_raw')
        summary = groups.setdefault(group, {'files': 0, 'bytes': 0, 'rows_declared': 0, 'zero_row_files': 0})
        summary['files'] += 1; summary['bytes'] += entry['bytes']
        summary['rows_declared'] += original['rows_declared']; summary['zero_row_files'] += int(original['rows_declared'] == 0)
    require(groups == manifest.get('counts'), 'roster_group_rows_or_multiplicity_differ')
    # The old accounting control is bound but is NOT recertified as a new budget.
    reader.json(desc['header_ledger'])
    plan = reader.json(desc['source_plan'])
    endpoint = endpoint_from_source_plan(plan)
    reader.bind_sources(entries)
    reader.finish()
    controls = [deepcopy(reader.controls[name]) for name in sorted(reader.controls)]
    return {'schema_version': SCHEMA, 'status': 'synthetic_lineage_controls_replayed_not_production_verified',
        'fixture_only': True, 'production_bindings_verified': False, 'production_admitted': False,
        'source_files': len(entries), 'declared_physical_rows': sum(item['rows_declared'] for item, _ in derived),
        'source_bindings': entries, 'source_group_declarations': groups, 'endpoint': endpoint,
        'original_producer_heads': {name: desc[name]['producer_head'] for name in
            ('initial_native', 'append_campaign', 'historical_full')},
        'reused_native_producer_head': roster.REUSED_HEAD,
        'original_source_git_blobs_verified': False,
        'direct_and_nested_fixture_controls': controls,
        'control_binding_projection_sha256': digest(encode(controls)),
        'source_payload_body_bytes_read_here': 0, 'source_footer_bytes_read_here': 0,
        'legacy_footer_identity_is_full_payload_SHA': False,
        'native_payload_SHA_reverified_here': False,
        'physical_ordinal_policy': 'zero_based_actual_Parquet_file_row_group_then_row_order_no_dedup',
        'hash_policy': 'native_observed_hash_never_relabelled_from_joined_header_hash',
        'archive_or_sample_admitted': False, 'production_blockers': list(BLOCKERS)}


def run_production(*args, **kwargs):
    require_production_host()
    raise Blocked('native_clock_preparation_production_adapter_not_reviewed_before_IO')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--describe-contract', action='store_true')
    args = parser.parse_args(argv)
    if args.describe_contract:
        print(encode(contract()).decode(), end='')
        return 0
    run_production()
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
