"""Fixture-first, row-preserving native-locator/exact-clock layer candidate.

This is NOT a production adapter or data admission. The production entrypoint
guards the canonical Linux mount and then fails closed BEFORE opening inputs.
Only the bounded synthetic core and temporary fixture publication are enabled.
No ABI/economic decoding, wallet correction, archive transformation, filtering,
deduplication, API call, source discovery, or approximate execution clock occurs.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tempfile

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from pipeline import decode_polymarket_v3_fill_v1 as wire
from scripts import collect_polymarket_exact_headers_v1 as clocks
from scripts import inventory_polymarket_exact_header_keys_v1 as inventory
from scripts import build_polymarket_exact_header_roster_v1 as roster

SCHEMA = 'polymarket_native_clock_layer_candidate_v1'
END_EXCLUSIVE_UTC = '2026-10-02T00:00:00Z'
CUTOFF_SECONDS = int(datetime(2026, 10, 2, tzinfo=timezone.utc).timestamp())
MAX_TIMESTAMP = 253402300799
LEGACY_FIELDS = frozenset(('order_hash', 'maker', 'taker', 'maker_asset_id', 'taker_asset_id',
    'maker_amount_filled', 'taker_amount_filled', 'fee', 'block_number', 'transaction_hash',
    'log_index', 'exchange_address'))
FIXTURE_MAX_ROWS = 4096
FIXTURE_MAX_FILES = 32
FIXTURE_MAX_HEADERS = 8192
FIXTURE_MAX_BYTES = 8 * 1024**2
PRODUCTION_BLOCKERS = (
    'accepted_source_roster_manifest_endpoint_lineage_adapter_not_implemented',
    'actual_whole_header_terminal_v2_acceptance_adapter_not_implemented',
    'legacy_full_content_binding_and_finite_read_policy_not_admitted',
    'production_physical_parquet_row_stream_and_schema_adapter_not_reviewed',
    'finite_runtime_read_RAM_retained_scratch_spill_feasibility_not_admitted',
    'global_replay_conflict_index_within_existing_envelope_not_implemented',
    'production_namespace_binding_output_reopen_publisher_not_implemented',
)
Blocked = roster.Blocked
require, encode, digest, decode = roster.require, roster.encode, roster.digest, roster.decode


def candidate_contract():
    """Explicit requirements, not an invocation admission or new resource cap."""
    return {'schema_version': SCHEMA, 'status': 'fixture_candidate_production_hard_blocked',
        'physical_key': ['source_file_index_in_bound_manifest', 'zero_based_physical_row_ordinal'],
        'physical_order': 'literal_parquet_file_row_group_then_row_order',
        'native_key': ['observed_emitting_exchange', 'observed_transaction_hash', 'observed_log_index'],
        'end_exclusive_utc': END_EXCLUSIVE_UTC, 'execution_clock': 'header_integer_UTC_seconds_only',
        'required_proofs': ['SHA_bound_accepted_source_roster', 'full_accepted_raw_lineage_and_manifests',
            'accepted_exact_endpoint_bracket', 'actual_whole_header_terminal_acceptance_v2'],
        'whole_header_keys_required': 32901442, 'native_expected_hash_assertions_required': 5884493,
        'header_seals_required': 330,
        'inherited_envelope_not_production_admission': {'retained_and_scratch_bytes': 64000000000,
            'spill_bytes': 16000000000, 'minimum_free_bytes': 20000000000},
        'finite_runtime_read_RAM_and_output_feasibility': None,
        'legacy_binding_qualification': 'footer_SHA_and_exact_identity_are_NOT_a_full_payload_SHA',
        'raw_payload_storage': 'immutable_original_source_file_plus_physical_row_reference_no_payload_copy',
        'row_fingerprint_semantics': 'canonical_JSON_source_row_values_NOT_Parquet_encoded_bytes_or_RPC_body_bytes',
        'replay_qualification': 'observed_source_representation_equality_not_unique_economic_fills',
        'production_blockers': list(PRODUCTION_BLOCKERS), 'production_admitted': False,
        'archive_or_sample_admitted': False, 'economic_semantics_admitted': False}


def source_binding(value):
    """Pure validation of the existing roster shape; no filesystem claim."""
    require(type(value) is dict and value.get('source_role') in {'legacy_raw', 'native_raw'}, 'source_role')
    names = {'path', 'source_role', 'bytes', 'identity', 'footer_sha256'}
    if value['source_role'] == 'native_raw':
        names.add('payload_sha256')
    require(set(value) == names, 'source_binding_fields')
    path = value['path']
    require(type(path) is str and PurePosixPath(path).is_absolute() and PurePosixPath(path).suffix == '.parquet'
        and str(PurePosixPath(path)) == path and '..' not in PurePosixPath(path).parts, 'source_literal_path')
    require(type(value['bytes']) is int and value['bytes'] >= 12, 'source_binding_bytes')
    identity = value['identity']
    require(type(identity) is dict and set(identity) == inventory.STAT_FIELDS
        and all(type(part) is int and part >= 0 for part in identity.values())
        and stat.S_ISREG(identity['mode']), 'source_binding_identity')
    for name in names & {'footer_sha256', 'payload_sha256'}:
        require(type(value[name]) is str and inventory.SHA.fullmatch(value[name]), 'source_binding_SHA')
    return deepcopy(value)


def _block(value):
    require(type(value) is int and 0 < value <= inventory.MAX_BLOCK, 'invalid_required_block_key')
    return value


def _identity(exchange, transaction, index):
    """Only observed components; missing/malformed components are not invented."""
    issues = []
    for name, value, pattern in (('exchange_address', exchange, wire.archive.ADDRESS),
        ('transaction_hash', transaction, wire.archive.HASH)):
        if not (type(value) is str and pattern.fullmatch(value)):
            issues.append(name + '_unavailable_or_invalid')
    if not (type(index) is int and 0 <= index < 2**64):
        issues.append('log_index_unavailable_or_invalid')
    if issues:
        return None, issues
    return (exchange.lower(), transaction.lower(), index), []


def project_legacy_row(row):
    require(type(row) is dict and set(row) == LEGACY_FIELDS, 'legacy_raw_row_schema')
    block = _block(row['block_number'])
    key, issues = _identity(row['exchange_address'], row['transaction_hash'], row['log_index'])
    # Legacy raw is already decoded source representation, not an RPC body.
    return {'block_number': block, 'native_observed_block_hash': None,
        'native_identity': key, 'identity_qualification': issues,
        'observed_payload_family': 'legacy_decoded_raw_row_v1',
        'observed_payload_sha256': digest(encode(row)),
        'physical_row_payload_sha256': digest(encode(row))}


def project_native_row(row):
    """Reuse locator/schema helpers only; NEVER call V3 ABI/economic decoding."""
    require(type(row) is dict and set(row) == wire.ROW_FIELDS, 'native_raw_row_schema')
    try:
        block = _block(wire.rpc_uint64(row['blockNumber']))
        require(wire.uint64(row['block_number']) == block, 'raw_cached_block_number_differs')
        transaction_index = wire.rpc_uint64(row['transactionIndex'])
        require(wire.uint64(row['transaction_index']) == transaction_index, 'raw_cached_transaction_index_differs')
    except wire.DecodeError:
        raise Blocked('native_block_or_transaction_locator_schema') from None
    require(row['removed'] is False, 'removed_source_log')
    observed_hash = row['blockHash']
    require(type(observed_hash) is str and wire.archive.HASH.fullmatch(observed_hash), 'native_observed_hash_invalid')
    address = row['address']
    if type(address) is str and wire.archive.ADDRESS.fullmatch(address) and row['exchange_address'] is not None:
        require(type(row['exchange_address']) is str and row['exchange_address'] == address.lower(),
            'raw_cached_exchange_address_differs')
    raw_index = row['logIndex']
    if raw_index is None:
        index = None
    else:
        try:
            index = wire.rpc_uint64(raw_index)
        except wire.DecodeError:
            index = None
        if index is not None and row['log_index'] is not None:
            try:
                require(wire.uint64(row['log_index']) == index, 'raw_cached_log_index_differs')
            except wire.DecodeError:
                raise Blocked('cached_log_index_schema') from None
    present = row['provider_block_timestamp_present']
    require(type(present) is bool and (row['blockTimestamp'] is None if not present else
        type(row['blockTimestamp']) is str and 0 < len(row['blockTimestamp']) <= 128
        and all(32 <= ord(char) <= 126 for char in row['blockTimestamp'])), 'provider_timestamp_provenance_schema')
    key, issues = _identity(address, row['transactionHash'], index)
    wire_payload = {name: row[name] for name in sorted(wire.RPC_FIELDS - {'blockTimestamp'})}
    # Collector window/request tags and provider-only time remain reachable in
    # the immutable source row. They do not change exact wire-payload equality.
    return {'block_number': block, 'native_observed_block_hash': observed_hash.lower(),
        'native_identity': key, 'identity_qualification': issues,
        'observed_payload_family': 'native_RPC_wire_row_v1',
        'observed_payload_sha256': digest(encode(wire_payload)),
        'physical_row_payload_sha256': digest(encode(row))}


def validated_header(value):
    require(type(value) is dict and set(value) == {'block_number', 'block_hash', 'parent_hash', 'timestamp'},
        'header_schema')
    number = _block(value['block_number']); timestamp = value['timestamp']
    require(type(timestamp) is int and 0 < timestamp <= MAX_TIMESTAMP, 'header_integer_UTC_timestamp')
    try:
        return clocks.validate_header({'number': hex(number), 'hash': value['block_hash'],
            'parentHash': value['parent_hash'], 'timestamp': hex(timestamp)},
            {'block_number': number, 'block_hash': None})
    except clocks.PilotBlocked:
        raise Blocked('invalid_header_projection') from None


def fixture_headers(values, endpoint):
    require(type(values) is list and len(values) <= FIXTURE_MAX_HEADERS, 'fixture_header_bound')
    result = {}
    for value in values:
        header = validated_header(value)
        require(header['block_number'] not in result, 'duplicate_header_key_join_fanout')
        result[header['block_number']] = header
    require(type(endpoint) is dict and set(endpoint) == {'end_exclusive_utc', 'last_included', 'first_excluded'}
        and endpoint['end_exclusive_utc'] == END_EXCLUSIVE_UTC, 'endpoint_fixture_schema')
    before, after = (validated_header(endpoint[name]) for name in ('last_included', 'first_excluded'))
    require(before['block_number'] + 1 == after['block_number'] and before['timestamp'] < CUTOFF_SECONDS
        <= after['timestamp'] and after['parent_hash'] == before['block_hash'], 'endpoint_exact_cutoff_bracket')
    for header in (before, after):
        old = result.get(header['block_number'])
        require(old is None or old == header, 'endpoint_header_binding_differs')
        result[header['block_number']] = header
    try:
        clocks.validate_clock(list(result.values()))
    except clocks.PilotBlocked:
        raise Blocked('nonmonotone_or_parent_mismatched_header_clock') from None
    return result


def fixture_input_digest(sources, headers, endpoint):
    try:
        raw = encode({'sources': sources, 'headers': headers, 'endpoint': endpoint})
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise Blocked('fixture_input_not_lossless_finite_JSON') from None
    require(len(raw) <= FIXTURE_MAX_BYTES, 'fixture_input_byte_bound_not_production_feasibility')
    return digest(raw)


def build_fixture_layer(sources, headers, endpoint):
    """Bounded synthetic core. Lists/dictionaries here are NOT a full-data driver."""
    require(type(sources) is list and 0 < len(sources) <= FIXTURE_MAX_FILES, 'fixture_source_bound')
    total = 0
    for source in sources:
        require(type(source) is dict and set(source) == {'binding', 'rows_declared', 'row_groups'}
            and type(source['rows_declared']) is int and source['rows_declared'] >= 0
            and type(source['row_groups']) is list and all(type(group) is list for group in source['row_groups']),
            'fixture_source_schema')
        total += sum(len(group) for group in source['row_groups'])
    require(total <= FIXTURE_MAX_ROWS, 'fixture_row_bound_not_production_feasibility')
    inputs_sha = fixture_input_digest(sources, headers, endpoint)
    cache = fixture_headers(headers, endpoint)
    records, bindings, paths, physical_files, groups = [], [], set(), set(), defaultdict(list)
    for file_index, source in enumerate(sources):
        binding = source_binding(source['binding'])
        physical = binding['identity']['device'], binding['identity']['inode']
        require(binding['path'] not in paths and physical not in physical_files, 'duplicate_declared_source_file')
        paths.add(binding['path']); physical_files.add(physical); bindings.append(binding)
        ordinal = 0
        for group in source['row_groups']:
            for raw in group:
                projected = project_legacy_row(raw) if binding['source_role'] == 'legacy_raw' else project_native_row(raw)
                block = projected['block_number']
                require(block in cache, 'missing_required_header_no_fallback')
                header = cache[block]; observed = projected['native_observed_block_hash']
                require(observed is None or observed == header['block_hash'], 'native_observed_joined_hash_mismatch')
                timestamp = header['timestamp']
                key = projected.pop('native_identity')
                row = {'source_file_index': file_index, 'physical_row_ordinal': ordinal, **projected,
                    'native_identity_exchange': key[0] if key else None,
                    'native_identity_transaction_hash': key[1] if key else None,
                    'native_identity_log_index': key[2] if key else None,
                    'header_joined_block_hash': header['block_hash'], 'timestamp_UTC_seconds': timestamp,
                    'UTC_month': datetime.fromtimestamp(timestamp, timezone.utc).strftime('%Y-%m'),
                    'exclusive_cutoff_bucket': 'before_exclusive_cutoff' if timestamp < CUTOFF_SECONDS else 'at_or_after_exclusive_cutoff',
                    'hash_qualification': 'native_hash_matched_joined_header' if observed is not None else
                        'joined_header_only_no_native_hash_assertion',
                    'native_relation_status': 'native_identity_unavailable' if key is None else None}
                if key is not None:
                    groups[key].append(len(records))
                records.append(row); ordinal += 1
        require(ordinal == source['rows_declared'], 'physical_source_row_count_not_conserved')
    for indices in groups.values():
        families = defaultdict(set)
        for index in indices:
            row = records[index]; families[row['observed_payload_family']].add(row['observed_payload_sha256'])
        conflict = any(len(values) > 1 for values in families.values())
        status = 'conflicting_observed_payload' if conflict else 'cross_representation_unreconciled' if len(families) > 1 \
            else 'identical_observed_replay' if len(indices) > 1 else 'unique_observed_native_identity'
        for index in indices:
            records[index]['native_relation_status'] = status
    require(len(records) == total == sum(source['rows_declared'] for source in sources), 'global_physical_row_conservation')
    result = {'schema_version': 'polymarket_native_clock_fixture_result_v1',
        'status': 'fixture_complete_not_production_or_analysis_admission', 'fixture_only': True,
        'source_bindings': bindings, 'records': records, 'physical_rows': total,
        'UTC_month_rows': dict(sorted(Counter(row['UTC_month'] for row in records).items())),
        'cutoff_bucket_rows': dict(sorted(Counter(row['exclusive_cutoff_bucket'] for row in records).items())),
        'native_relation_rows': dict(sorted(Counter(row['native_relation_status'] for row in records).items())),
        'input_projection_sha256': inputs_sha,
        'fallback_rows': 0, 'missing_header_rows': 0, 'header_join_fanout_rows': 0,
        'row_fingerprint_semantics': 'canonical_JSON_source_row_values_NOT_Parquet_encoded_bytes_or_RPC_body_bytes',
        'provider_timestamp_used_as_execution_clock': False, 'physical_rows_dropped': 0,
        'economic_semantics_admitted': False, 'production_bindings_verified': False,
        'archive_or_sample_admitted': False}
    result['output_projection_sha256'] = digest(encode(result))
    return result


def publish_fixture(result, output, final_input_digest):
    """Tiny temporary fixture publication only; never the production publisher."""
    require(type(result) is dict and result.get('fixture_only') is True
        and result.get('production_bindings_verified') is False
        and type(result.get('physical_rows')) is int and 0 <= result['physical_rows'] <= FIXTURE_MAX_ROWS
        and callable(final_input_digest), 'fixture_publication_only')
    require(result.get('output_projection_sha256') == digest(encode({name: value for name, value in result.items()
        if name != 'output_projection_sha256'})), 'fixture_result_changed')
    expected_input_digest, expected_status = result['input_projection_sha256'], result['status']
    output = Path(output); temporary_root = Path(tempfile.gettempdir()).resolve()
    require(output.is_absolute() and output.parent.resolve() == output.parent and temporary_root in output.parents
        and Path('/mnt/data') not in output.parents and not output.exists() and not output.is_symlink(),
        'fixture_output_must_be_fresh_temporary_directory')
    raw = encode(result)
    require(len(raw) + 4096 <= FIXTURE_MAX_BYTES, 'fixture_publication_byte_bound')
    require(final_input_digest() == expected_input_digest, 'fixture_input_changed')
    output.mkdir(mode=0o700)
    target, pending = output/'fixture.json', output/'.fixture.json.pending'
    def stage(path, payload):
        with path.open('xb') as stream:
            os.fchmod(stream.fileno(), 0o600)
            require(stream.write(payload) == len(payload), 'fixture_short_write')
            stream.flush(); os.fsync(stream.fileno())
        return inventory.file_identity(path), digest(payload)
    identity, sha = stage(pending, raw)
    require(pending.read_bytes() == raw and inventory.file_identity(pending) == identity, 'fixture_output_reopen_changed')
    require(final_input_digest() == expected_input_digest, 'fixture_input_changed')
    require({path.name for path in output.iterdir()} == {pending.name}, 'fixture_output_namespace_changed')
    require(pending.read_bytes() == raw and inventory.file_identity(pending) == identity, 'fixture_output_reopen_changed')
    inventory.fsync_directory(output); inventory.atomic_publish(pending, target)
    marker = {'schema_version': 'polymarket_native_clock_fixture_completion_v1', 'fixture_only': True,
        'status': expected_status, 'payload': {'path': 'fixture.json', 'bytes': len(raw), 'sha256': sha}}
    marker_raw = encode(marker); marker_pending = output/'.COMPLETE.json.pending'
    marker_identity, _ = stage(marker_pending, marker_raw)
    target_identity = inventory.file_identity(target)
    inventory.fsync_directory(output)
    require(final_input_digest() == expected_input_digest, 'fixture_input_changed')
    require({path.name for path in output.iterdir()} == {'fixture.json', marker_pending.name},
        'fixture_output_namespace_changed')
    require(target.read_bytes() == raw and inventory.file_identity(target) == target_identity
        and marker_pending.read_bytes() == marker_raw and inventory.file_identity(marker_pending) == marker_identity,
        'fixture_final_output_reopen_changed')
    inventory.atomic_publish(marker_pending, output/'COMPLETE.json')
    return marker


def run_production(*_args, **_kwargs):
    """The guard precedes ALL production input or output filesystem access."""
    require_production_host()
    raise Blocked('native_clock_production_adapter_not_reviewed')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--describe-contract', action='store_true')
    parser.add_argument('--admission'); parser.add_argument('--output'); parser.add_argument('--expected-head')
    args = parser.parse_args(argv)
    if args.describe_contract:
        print(encode(candidate_contract()).decode().strip()); return 0
    run_production(args.admission, args.output, args.expected_head)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
