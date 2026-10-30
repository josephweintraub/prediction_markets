"""Metered no-trade-body footer profiler; real production admission is CLOSED.

The executable fixture runner reads SHA-bound control/roster/generated-audit
inputs, source magic/trailer/footers ONLY, and publishes immutable aggregates.
It never reads a raw Parquet data page, estimates throughput/index compression,
deduplicates trades, makes RPC calls, or creates a two-billion-row sidecar.
The real proof/source/current-boot adapter is not implemented: production guards
the canonical host and fails before input I/O. Proposed invocation sublimits do
not reset or replace previous stage charges or the existing native envelope.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import time

# Trusted deadline/namespace primitives are captured at import. The injected
# fixture meter clock and resource callbacks must never run after the last gate.
_PRIMITIVE_CLOCK = time.monotonic
_PRIMITIVE_SCANDIR = os.scandir
_PRIMITIVE_LSTAT = os.lstat

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import build_polymarket_native_clock_layer_v1 as core
from scripts import prepare_polymarket_native_clock_layer_v1 as proof
from scripts import inventory_polymarket_exact_header_keys_v1 as inventory
from scripts import build_polymarket_exact_header_roster_v1 as roster

Blocked = core.Blocked
require, encode, decode, digest = core.require, core.encode, core.decode, core.digest
SCHEMA = 'polymarket_native_clock_source_profile_v1'
PROPOSED = {'runtime_seconds_inclusive_outer_wrapper': 1200, 'address_space_bytes': 8 * 1024**3,
    'workers': 1, 'spill_bytes': 0, 'metadata_read_bytes': 2000000000,
    'single_source_footer_pass_bytes': 1000000000, 'all_new_retained_bytes': 64 * 1024**2,
    'minimum_free_bytes': 20000000000, 'footer_bytes_per_file': 32 * 1024**2,
    'roster_bytes': 64 * 1024**2, 'source_audit_bytes': 128 * 1024**2,
    'control_bytes_per_file': 8 * 1024**2, 'files': 150000}
BLOCKERS = ('actual_full_direct_and_nested_accepted_control_adapter_not_reviewed',
    'actual_original_source_Git_blob_and_current_boot_mount_identity_adapter_not_reviewed',
    'actual_inclusive_outer_deadline_RLIMIT_AS_and_lifecycle_admission_not_reviewed',
    'actual_metadata_read_and_retained_capacity_fit_not_measured_or_admitted',
    'legacy_full_payload_SHA_policy_not_admitted',
    'trade_stream_global_replay_index_publisher_and_full_reconciliation_not_admitted')


def contract():
    return {'schema_version': SCHEMA, 'status': 'executable_footer_core_production_entry_hardblocked',
        'proposed_invocation_sublimits_not_admission_or_reset': deepcopy(PROPOSED),
        'existing_native_category_retained_and_scratch_ceiling_bytes': 64000000000,
        'existing_native_category_spill_ceiling_bytes': 16000000000,
        'source_data_pages_read': False, 'new_per_row_or_per_file_table': False,
        'publication': 'aggregate_schema_role_leaf_costs_plus_source_order_full_profile_rolling_SHA',
        'source_roster_and_generated_audit_referenced_not_copied': True,
        'full_hash_cost_reported_not_performed': True,
        'legacy_footer_identity_is_full_payload_SHA': False,
        'production_admitted': False, 'production_blockers': list(BLOCKERS)}


def number(value, code, *, minimum=0):
    require(type(value) is int and value >= minimum, code)
    return value


def entry_identity(entry):
    identity = roster.identity(Path(entry['path']))
    require(identity == entry['identity'] and Path(entry['path']).stat().st_size == entry['bytes'],
        'literal_source_identity_or_size_changed')
    return identity


class Meter:
    """A prospective invocation meter, NEVER a new cumulative allowance."""
    def __init__(self, started, clock=time.monotonic):
        require(type(started) in (int, float) and not isinstance(started, bool), 'outer_start_required')
        self.started, self.clock = started, clock
        self.reads = defaultdict(int)
        self.footer_ceiling = PROPOSED['single_source_footer_pass_bytes']
        self.reserved_final_reads = 0
        self.check()

    def check(self):
        elapsed = self.clock() - self.started
        require(0 <= elapsed < PROPOSED['runtime_seconds_inclusive_outer_wrapper'], 'inclusive_profile_deadline')

    def charge(self, kind, size):
        self.check(); number(size, 'invalid_read_charge')
        require(sum(self.reads.values()) + size + self.reserved_final_reads <= PROPOSED['metadata_read_bytes'],
            'aggregate_metadata_read_limit_or_final_reserve')
        require(kind != 'source_footer' or self.reads[kind] + size <= self.footer_ceiling,
            'dedicated_source_footer_subcap')
        self.reads[kind] += size  # Failed reads remain charged; no refund.

    def reserve(self, size):
        number(size, 'invalid_final_read_reserve')
        require(sum(self.reads.values()) + size <= PROPOSED['metadata_read_bytes'], 'planned_final_reopens_do_not_fit')
        self.reserved_final_reads = size

    def usage(self, *, final=False):
        return {'metadata_bytes_charged': sum(self.reads.values()), 'bytes_by_kind': dict(self.reads),
            'planned_final_reads_remaining': self.reserved_final_reads,
            'elapsed_inclusive_seconds': (_PRIMITIVE_CLOCK() if final else self.clock())-self.started,
            'raw_trade_body_bytes_read': 0, 'source_payload_full_hash_bytes_read': 0}


def read_plan(refs, footer_upper_bound):
    """Conservative initial+final full control reads plus bounded output reopens."""
    require(type(refs) is dict and refs, 'profile_reference_roster')
    values = [proof.reference(item) for item in refs.values()]
    require(len({item['path'] for item in values}) == len(values), 'duplicate_profile_control_path')
    number(footer_upper_bound, 'footer_upper_bound')
    require(footer_upper_bound <= PROPOSED['single_source_footer_pass_bytes'], 'planned_source_footer_subcap')
    control_twice = 2 * sum(item['bytes'] for item in values)
    output_reopens = 2 * PROPOSED['all_new_retained_bytes']
    require(control_twice + footer_upper_bound + output_reopens <= PROPOSED['metadata_read_bytes'],
        'planned_metadata_control_footer_output_reopens_do_not_fit')
    return {'control_initial_and_final_bytes': control_twice, 'dedicated_single_footer_pass_upper_bytes': footer_upper_bound,
        'generated_output_reopen_upper_bytes': output_reopens,
        'metadata_read_upper_bytes': control_twice+footer_upper_bound+output_reopens,
        'footers_replayed_again': False, 'final_source_stat_identity_reopens': True}


class BoundReader:
    def __init__(self, refs, meter):
        self.refs = {name: proof.reference(item) for name, item in refs.items()}
        self.meter, self.captured = meter, {}

    def raw(self, name):
        ref = self.refs[name]; path = Path(ref['path'])
        limit = PROPOSED['roster_bytes'] if name == 'roster' else PROPOSED['source_audit_bytes'] if name == 'source_audit' else PROPOSED['control_bytes_per_file']
        require(ref['bytes'] <= limit, 'profile_control_size_bound')
        before = roster.identity(path)
        require(path.stat().st_size == ref['bytes'], 'profile_control_size_changed')
        self.meter.charge('control_roster_generated_audit', ref['bytes'])
        with path.open('rb', buffering=0) as stream: raw = stream.read(ref['bytes'])
        require(len(raw) == ref['bytes'] and digest(raw) == ref['sha256'] and roster.identity(path) == before,
            'profile_control_SHA_or_identity_changed')
        bound = {**ref, 'identity': before}
        require(self.captured.setdefault(name, bound) == bound, 'profile_control_binding_conflict')
        return raw

    def reopen(self):
        for name in self.refs:
            ref = self.refs[name]
            # This reserved read becomes an actual charge, not a refund.
            self.meter.reserved_final_reads -= ref['bytes']
            require(self.meter.reserved_final_reads >= 0, 'final_read_reserve_underflow')
            self.raw(name)


def field_schema(schema):
    return {'serialized_arrow_schema_sha256': digest(schema.serialize().to_pybytes()),
        'fields': [{'name': field.name, 'type': str(field.type), 'nullable': field.nullable,
            'metadata': None if field.metadata is None else [[key.hex(), value.hex()] for key, value in sorted(field.metadata.items())]}
            for field in schema],
        'schema_metadata': None if schema.metadata is None else [[key.hex(), value.hex()] for key, value in sorted(schema.metadata.items())]}


def profile_footer(entry, meter):
    """Read exactly magic + trailer + SHA-bound footer, never a data page."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    entry = core.source_binding(entry)
    entry_identity(entry); size = entry['bytes']
    with Path(entry['path']).open('rb', buffering=0) as stream:
        meter.charge('source_footer', 4); magic = stream.read(4)
        require(magic == b'PAR1', 'profile_parquet_magic')
        meter.charge('source_footer', 8); stream.seek(-8, os.SEEK_END); trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b'PAR1', 'profile_parquet_trailer')
        length = int.from_bytes(trailer[:4], 'little')
        require(0 < length <= size-12 and length+12 <= PROPOSED['footer_bytes_per_file'], 'profile_footer_length_or_bound')
        meter.charge('source_footer', length); stream.seek(-8-length, os.SEEK_END); raw = stream.read(length)
        require(len(raw) == length and digest(raw+trailer) == entry['footer_sha256'], 'profile_source_footer_SHA')
    metadata = pq.read_metadata(pa.BufferReader(b'PAR1'+raw+trailer))
    schema = metadata.schema.to_arrow_schema()
    names = core.LEGACY_FIELDS if entry['source_role'] == 'legacy_raw' else core.wire.ROW_FIELDS
    require(len(schema.names) == len(set(schema.names)) and set(schema.names) == names, 'profile_literal_schema_fields')
    native = entry['source_role'] == 'native_raw'
    require(schema.field('block_number').type == (pa.uint64() if native else pa.int64()), 'profile_block_type')
    if native:
        ints = {'block_number', 'log_index', 'transaction_index', 'source_request_id'}
        booleans = {'removed', 'provider_block_timestamp_present'}
        for field in schema:
            expected = pa.uint64() if field.name in ints else pa.bool_() if field.name in booleans else pa.list_(pa.string()) if field.name == 'topics' else pa.string()
            require(field.type == expected, 'profile_native_literal_type')
    rows, groups, max_rows, ranges, columns = number(metadata.num_rows, 'footer_row_count'), number(metadata.num_row_groups, 'footer_row_group_count'), 0, [], {}
    number(metadata.num_columns, 'footer_leaf_column_count', minimum=1)
    group_rows, row_group_rows, extents = 0, [], []
    for group_index in range(groups):
        meter.check(); group = metadata.row_group(group_index)
        count = number(group.num_rows, 'footer_row_group_rows'); group_rows += count; max_rows = max(max_rows, count)
        row_group_rows.append(count)
        require(type(group.num_columns) is int and group.num_columns == metadata.num_columns, 'footer_group_column_count')
        for column_index in range(metadata.num_columns):
            column = group.column(column_index); leaf = metadata.schema.column(column_index).path
            root = leaf.split('.')[0]
            require(root in names and column.path_in_schema == leaf and column.file_path in (None, ''), 'profile_external_or_unknown_leaf')
            offsets = (column.dictionary_page_offset, column.data_page_offset)
            require(all(value is None or type(value) is int and value >= 0 for value in offsets), 'profile_invalid_page_offset')
            starts = [value for value in offsets
                if type(value) is int and value >= 4]
            compressed = number(column.total_compressed_size, 'footer_compressed_size')
            uncompressed = number(column.total_uncompressed_size, 'footer_uncompressed_size')
            values = number(column.num_values, 'footer_leaf_value_count_not_physical_rows')
            first, last = None, None
            # Empty Boolean chunks legitimately have no data/dictionary page,
            # zero sizes and zero values. Preserve them without inventing an
            # offset or treating list leaf values as physical-row counts.
            if not starts:
                require(count == 0 and compressed == uncompressed == 0
                    and values == 0, 'profile_column_start_absent')
            else:
                first = min(starts); last = first+compressed
                require(first <= last <= size-length-8, 'profile_extent_outside_source_body')
                if compressed: ranges.append([first, last])
            extents.append([group_index, leaf, first, last, compressed, uncompressed, values])
            part = columns.setdefault(leaf, {'root_field': root, 'leaf_path': leaf, 'extent_count': 0,
                'row_group_chunk_count': 0, 'zero_byte_chunk_count': 0,
                'compressed_bytes': 0, 'uncompressed_bytes': 0, 'max_row_group_compressed_bytes': 0,
                'max_row_group_uncompressed_bytes': 0, 'encodings': [], 'compression': []})
            part['extent_count'] += int(compressed > 0); part['row_group_chunk_count'] += 1
            part['zero_byte_chunk_count'] += int(compressed == 0)
            part['compressed_bytes'] += compressed; part['uncompressed_bytes'] += uncompressed
            part['max_row_group_compressed_bytes'] = max(part['max_row_group_compressed_bytes'], compressed)
            part['max_row_group_uncompressed_bytes'] = max(part['max_row_group_uncompressed_bytes'], uncompressed)
            part['encodings'] = sorted(set(part['encodings']) | set(column.encodings))
            part['compression'] = sorted(set(part['compression']) | {column.compression})
    require(group_rows == rows, 'footer_group_row_conservation')
    ranges.sort()
    require(all(a[1] <= b[0] for a,b in zip(ranges, ranges[1:])), 'profile_overlapping_leaf_extents')
    expected_roots = set(names)
    require(groups == 0 or {part['root_field'] for part in columns.values()} == expected_roots, 'profile_projection_missing_root_field')
    entry_identity(entry)
    return {'source_role': entry['source_role'], 'source_bytes': size, 'rows': rows, 'row_groups': groups,
        'max_row_group_rows': max_rows, 'footer_bytes_read': length+12, 'footer_sha256': entry['footer_sha256'],
        'schema': field_schema(schema), 'leaf_columns': [columns[key] for key in sorted(columns)],
        'row_group_physical_rows': row_group_rows,
        'row_group_leaf_extents': extents,
        'extent_tuple_fields': ['row_group_index','leaf_path','first_byte','exclusive_last_byte',
            'compressed_bytes','uncompressed_bytes','leaf_values_NOT_physical_rows'],
        'projected_ranges': ranges, 'all_field_compressed_bytes': sum(last-first for first,last in ranges),
        'all_field_uncompressed_bytes': sum(part['uncompressed_bytes'] for part in columns.values()),
        'block_hash_inventory_compressed_bytes': sum(part['compressed_bytes'] for part in columns.values()
            if part['root_field'] in ({'block_number', 'blockHash'} if native else {'block_number'})),
        'raw_trade_body_bytes_read': 0, 'full_payload_hash_reverified': False}


def audit_rows(raw):
    """Generated audit Parquet is permitted metadata, not raw trade payload."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    value = pq.ParquetFile(pa.BufferReader(raw))
    require(value.schema_arrow == inventory.audit_schema(), 'profile_saved_source_audit_schema')
    for batch in value.iter_batches(batch_size=1024, use_threads=False):
        yield from batch.to_pylist()


def check_audit(row, entry, index):
    require(type(row) is dict and type(row.get('source_index')) is int and row['source_index'] == index,
        'profile_audit_literal_source_order')
    require(all(row.get(name) == entry.get(name) for name in ('path', 'source_role', 'bytes', 'footer_sha256', 'payload_sha256'))
        and decode(row['identity_json'].encode()) == entry['identity'], 'profile_audit_source_binding')
    require(all(type(row.get(name)) is int and row[name] == 0 for name in proof.FAILURE_FIELDS), 'profile_audit_failures')
    count = number(row.get('rows_declared'), 'profile_audit_rows')
    require(type(row.get('rows_scanned')) is int and type(row.get('valid_rows')) is int
        and row['rows_scanned'] == row['valid_rows'] == count, 'profile_audit_physical_row_conservation')
    require(row.get('payload_sha256_verified') is (entry['source_role'] == 'native_raw'), 'profile_audit_hash_qualification')
    number(row.get('projected_compressed_bytes'), 'profile_audit_old_projection_bytes')
    return count


def aggregate(entries, rows, meter):
    rolling, schemas, roles, schema_state_bytes = hashlib.sha256(), {}, {}, 0
    iterator = iter(rows)
    for index, entry in enumerate(entries):
        row = next(iterator, None); require(row is not None, 'profile_audit_missing_source')
        expected_rows = check_audit(row, entry, index)
        value = profile_footer(entry, meter)
        require(value['rows'] == expected_rows and value['block_hash_inventory_compressed_bytes'] == row['projected_compressed_bytes'],
            'profile_footer_differs_from_saved_audit')
        schema_id = digest(encode(value['schema']))
        if schema_id not in schemas:
            schema_state_bytes += len(encode(value['schema']))
            require(schema_state_bytes <= PROPOSED['all_new_retained_bytes'], 'profile_schema_aggregate_cannot_fit_retained_limit')
        saved = schemas.setdefault(schema_id, {'schema': value['schema'], 'source_role': entry['source_role'], 'files': 0})
        require(saved['schema'] == value['schema'] and saved['source_role'] == entry['source_role'], 'profile_schema_binding_conflict')
        saved['files'] += 1
        role = roles.setdefault(entry['source_role'], {'files': 0, 'rows': 0, 'source_bytes': 0, 'zero_row_files': 0,
            'row_groups': 0, 'max_row_group_rows': 0, 'footer_bytes_read': 0,
            'all_field_compressed_bytes': 0, 'all_field_uncompressed_bytes': 0, 'leaf_columns': {}})
        role['files'] += 1; role['zero_row_files'] += int(value['rows'] == 0)
        for name in ('rows', 'source_bytes', 'row_groups', 'footer_bytes_read', 'all_field_compressed_bytes', 'all_field_uncompressed_bytes'):
            role[name] += value[name]
        role['max_row_group_rows'] = max(role['max_row_group_rows'], value['max_row_group_rows'])
        for leaf in value['leaf_columns']:
            saved_leaf = role['leaf_columns'].setdefault(leaf['leaf_path'], {**deepcopy(leaf), 'extent_count': 0,
                'row_group_chunk_count': 0, 'zero_byte_chunk_count': 0,
                'compressed_bytes': 0, 'uncompressed_bytes': 0, 'max_row_group_compressed_bytes': 0,
                'max_row_group_uncompressed_bytes': 0, 'encodings': [], 'compression': []})
            for name in ('extent_count', 'row_group_chunk_count', 'zero_byte_chunk_count', 'compressed_bytes', 'uncompressed_bytes'):
                saved_leaf[name] += leaf[name]
            for name in ('max_row_group_compressed_bytes', 'max_row_group_uncompressed_bytes'): saved_leaf[name] = max(saved_leaf[name], leaf[name])
            for name in ('encodings', 'compression'): saved_leaf[name] = sorted(set(saved_leaf[name]) | set(leaf[name]))
        # Full per-file profiles include every literal extent and schema, but
        # only their source-index-ordered commitment is retained, not a table.
        rolling.update(encode({'source_index': index, 'source_binding': entry, 'profile': value}))
    require(next(iterator, None) is None, 'profile_audit_extra_source')
    return {'files': len(entries), 'rows': sum(role['rows'] for role in roles.values()), 'roles': roles,
        'literal_schema_variants': schemas, 'ordered_complete_file_profile_projection_sha256': rolling.hexdigest(),
        'full_payload_hash_read_cost_bytes': {role: data['source_bytes'] for role,data in roles.items()},
        'new_per_file_or_per_row_table_written': False,
        'legacy_hash_qualification': 'bound_footer_and_identity_NOT_full_payload_SHA',
        'native_hash_qualification': 'accepted_inventory_SHA_evidence_retained_NOT_rehashed_in_this_profile',
        'actual_trade_stream_runtime_RAM_index_compression_or_output_fit_proved': False}


class Publisher:
    """Fresh exact namespace, no-replace commit; failed/pending bytes preserved."""
    def __init__(self, output, meter):
        self.root, self.meter, self.files = Path(output), meter, {}
        require(self.root.is_absolute() and self.root.parent.resolve() == self.root.parent
            and not self.root.exists() and not self.root.is_symlink(), 'profile_output_not_fresh_literal')
        require(shutil.disk_usage(self.root.parent).free >= PROPOSED['minimum_free_bytes']+PROPOSED['all_new_retained_bytes'],
            'profile_output_free_floor_and_reserve')
        self.root.mkdir(mode=0o700)

    def namespace(self):
        self.meter.check(); names = set()
        for path in self.root.iterdir():
            require(path.name in self.files and stat.S_ISREG(path.lstat().st_mode) and path.lstat().st_nlink == 1,
                'profile_output_unknown_namespace')
            names.add(path.name)
        require(names == set(self.files), 'profile_output_missing_file')
        retained = sum(path.stat().st_size for path in self.root.iterdir())
        require(retained <= PROPOSED['all_new_retained_bytes'], 'profile_all_retained_byte_limit')
        require(shutil.disk_usage(self.root).free >= PROPOSED['minimum_free_bytes'], 'profile_free_floor')
        return retained

    def reopen(self, names=None):
        self.namespace()
        for name in self.files if names is None else names:
            saved = self.files[name]
            path = self.root/name; before = roster.identity(path)
            self.meter.reserved_final_reads -= saved['bytes']
            require(self.meter.reserved_final_reads >= 0, 'profile_output_read_reserve_underflow')
            self.meter.charge('generated_output_reopen', saved['bytes'])
            with path.open('rb', buffering=0) as stream: raw = stream.read(saved['bytes'])
            require(len(raw) == saved['bytes'] and path.stat().st_size == saved['bytes'] and digest(raw) == saved['sha256']
                and before == saved['identity'] == roster.identity(path), 'profile_output_reopen_changed')

    def publish(self, name, value, *, final_check=None, final_primitive=None):
        require(name in {'START.json', 'profile.json', 'COMPLETE.json', 'FAILED.json'} and name not in self.files,
            'profile_publication_name_or_overwrite')
        raw = encode(value); retained = self.namespace()
        require(retained+len(raw) <= PROPOSED['all_new_retained_bytes'], 'profile_all_retained_byte_limit')
        require(shutil.disk_usage(self.root).free >= PROPOSED['minimum_free_bytes']+len(raw), 'profile_free_floor')
        pending, destination = self.root/('.'+name+'.pending'), self.root/name
        require(not pending.exists() and not pending.is_symlink() and not destination.exists() and not destination.is_symlink(),
            'profile_publication_already_exists')
        with pending.open('xb') as stream:
            require(stream.write(raw) == len(raw), 'profile_output_short_write'); stream.flush(); os.fsync(stream.fileno())
        saved = {'path': name, 'bytes': len(raw), 'sha256': digest(raw), 'identity': roster.identity(pending)}
        self.files[pending.name] = saved
        self.reopen([pending.name])
        if final_check is not None:
            final_check()
            self.reopen()  # Full output bytes/identities AFTER the input gates.
        if name == 'COMPLETE.json':
            require(final_check is not None and final_primitive is not None, 'profile_terminal_gates_required')
            terminal_binding = {key: saved[key] for key in ('path', 'bytes', 'sha256')}
            # Marker-LAST: the consumer must independently reopen and sync the
            # committed directory. No post-commit durability is claimed here.
            descriptor = os.open(self.root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try: os.fsync(descriptor)
            finally: os.close(descriptor)
            final_primitive()
            roster.atomic_commit(pending, destination)
            return terminal_binding  # No fallible I/O/check/clock after commit.
        if final_primitive is not None:
            # No meter clock, disk callback, or input/output helper callback may
            # run between this primitive pass and the no-replace commit.
            final_primitive()
        roster.atomic_commit(pending, destination)
        self.files[name] = self.files.pop(pending.name)
        committed = _PRIMITIVE_LSTAT(destination)
        require(stat.S_ISREG(committed.st_mode) and committed.st_nlink == 1, 'profile_atomic_commit_not_regular')
        committed_identity = {'device': committed.st_dev, 'inode': committed.st_ino, 'mode': committed.st_mode,
            'uid': committed.st_uid, 'gid': committed.st_gid, 'mtime_ns': committed.st_mtime_ns,
            'ctime_ns': committed.st_ctime_ns}
        require(all(committed_identity[key] == self.files[name]['identity'][key]
            for key in inventory.STAT_FIELDS if key != 'ctime_ns'), 'profile_atomic_commit_identity_changed')
        self.files[name]['identity'] = committed_identity
        descriptor = os.open(self.root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
        return {key: saved[key] for key in ('path', 'bytes', 'sha256')}


def primitive_gate(input_root, output, expected_inputs, expected_outputs, started):
    """Callback-free identity/namespace/deadline pass immediately before commit.

    Full control/output bytes were just metered and reopened. Exact seven-field
    identities plus size bind those bytes and every original source across all
    intervening resource callbacks; source bodies/footers are NOT read again.
    """
    def snapshot(root, expected, excluded=None):
        root = str(root)
        # scandir follows its root argument: explicitly reject a replaced root
        # or aliased literal ancestor before traversing any directory entries.
        for literal in (Path(root), *Path(root).parents):
            directory_identity = _PRIMITIVE_LSTAT(str(literal))
            require(stat.S_ISDIR(directory_identity.st_mode), 'profile_primitive_root_or_ancestor_not_literal_directory')
        directories = {str(parent) for name in expected for parent in Path(name).parents
            if str(parent) == root or root in {str(item) for item in parent.parents}}
        found, stack, nodes = {}, [root], 0
        while stack:
            directory = stack.pop()
            with _PRIMITIVE_SCANDIR(directory) as iterator:
                for item in iterator:
                    path = os.path.join(directory, item.name)
                    if path == excluded: continue
                    nodes += 1
                    require(nodes <= 2048, 'profile_primitive_namespace_bound')
                    saved = item.stat(follow_symlinks=False)
                    if stat.S_ISDIR(saved.st_mode):
                        require(path in directories, 'profile_primitive_unknown_input_directory')
                        stack.append(path)
                    else:
                        require(stat.S_ISREG(saved.st_mode) and saved.st_nlink == 1, 'profile_primitive_nonregular_input')
                        found[path] = {'device': saved.st_dev, 'inode': saved.st_ino, 'mode': saved.st_mode,
                            'uid': saved.st_uid, 'gid': saved.st_gid, 'mtime_ns': saved.st_mtime_ns,
                            'ctime_ns': saved.st_ctime_ns, 'bytes': saved.st_size}
        require(set(found) == set(expected), 'profile_primitive_exact_namespace_changed')
        require(found == expected, 'profile_primitive_bound_identity_changed')
    snapshot(input_root, expected_inputs, str(output))
    snapshot(output, expected_outputs)
    elapsed = _PRIMITIVE_CLOCK()-started
    require(0 <= elapsed < PROPOSED['runtime_seconds_inclusive_outer_wrapper'], 'profile_primitive_deadline')


def _parse_inputs(reader):
    raw = {name: reader.raw(name) for name in reader.refs}
    listing = decode(raw['roster']); manifest = decode(raw['inventory_manifest']); marker = decode(raw['inventory_complete'])
    require(type(listing) is dict and set(listing) == {'schema_version', 'files'}
        and listing['schema_version'] == 'polymarket_exact_header_key_roster_v1'
        and type(listing['files']) is list and 0 < len(listing['files']) <= PROPOSED['files'], 'profile_roster_schema')
    entries = [core.source_binding(item) for item in listing['files']]
    require(len({item['path'] for item in entries}) == len(entries)
        and len({(item['identity']['device'],item['identity']['inode']) for item in entries}) == len(entries), 'profile_source_alias_or_duplicate')
    root = Path(reader.refs['inventory_manifest']['path']).parent
    relative = lambda name: {'path': Path(reader.refs[name]['path']).name,
        **{key: reader.refs[name][key] for key in ('bytes','sha256')}}
    require(marker.get('schema_version') == 'polymarket_exact_header_key_inventory_completion_v1'
        and marker.get('status') == manifest.get('status') == 'literal_header_key_inventory_complete'
        and marker.get('manifest') == relative('inventory_manifest')
        and Path(reader.refs['inventory_complete']['path']) == root/'COMPLETE.json'
        and manifest.get('schema_version') == 'polymarket_exact_header_key_inventory_v1'
        and manifest.get('literal_roster_only') is True and manifest.get('final_universe_admitted') is False
        and marker.get('literal_roster_only') is True and marker.get('final_universe_admitted') is False,
        'profile_inventory_not_bound_complete')
    require(manifest.get('source_audit') == relative('source_audit')
        and Path(reader.refs['source_audit']['path']) == root/'source_audit.parquet', 'profile_source_audit_binding')
    saved_roster = manifest.get('roster')
    require(type(saved_roster) is dict and set(saved_roster) == {'path','sha256','identity'}
        and saved_roster['path'] == reader.refs['roster']['path'] and saved_roster['sha256'] == reader.refs['roster']['sha256']
        and saved_roster['identity'] == {**reader.captured['roster']['identity'], 'bytes': reader.refs['roster']['bytes']},
        'profile_roster_inventory_identity_binding')
    require(type(manifest.get('source_files')) is int and manifest['source_files'] == len(entries)
        and type(manifest.get('rows_declared')) is int and type(manifest.get('rows_scanned')) is int
        and manifest['rows_declared'] == manifest['rows_scanned']
        and type(manifest.get('failure_counts')) is dict and set(manifest['failure_counts']) == proof.FAILURE_FIELDS
        and all(type(value) is int and value == 0 for value in manifest['failure_counts'].values())
        and type(manifest.get('global_conflicting_hash_blocks')) is int and manifest['global_conflicting_hash_blocks'] == 0,
        'profile_inventory_counts_or_failures')
    require(type(manifest.get('planned_reads')) is dict
        and type(manifest['planned_reads'].get('footer_read_bytes')) is int
        and 0 <= manifest['planned_reads']['footer_read_bytes'] <= 2*PROPOSED['single_source_footer_pass_bytes']
        and manifest['planned_reads']['footer_read_bytes'] % 2 == 0, 'profile_saved_two_pass_footer_plan')
    keys = manifest.get('required_keys_parquet')
    require(type(keys) is dict and set(keys) == {'path','bytes','sha256'} and keys['path'] == 'required_keys.parquet',
        'profile_bound_required_keys_reference')
    # Reuse the existing exact inventory-control adapter. Required-key data is
    # referenced, NOT reopened here; whole-header acceptance remains a separate
    # missing production adapter rather than a caller-supplied Boolean.
    proof.inventory_control_adapter(raw['inventory_complete'], raw['inventory_manifest'], {
        **{name: reader.refs[name] for name in ('roster','inventory_manifest','inventory_complete')},
        'keys': {**keys, 'path': str(root/keys['path'])}})
    return entries, raw['source_audit'], manifest


def run_fixture(refs, output, *, started, clock=time.monotonic):
    """Executable tiny local profile; identities certify ONLY these fixtures."""
    require(type(refs) is dict and set(refs) == {'roster','source_audit','inventory_manifest','inventory_complete'}, 'profile_input_roster')
    refs = {name: proof.reference(value) for name,value in refs.items()}
    output = Path(output); temporary = Path(tempfile.gettempdir()).resolve()
    require(output.is_absolute() and output.parent.resolve() == output.parent and temporary in output.parents
        and output.parent != temporary, 'profile_fixture_only_not_production')
    require(all(output.parent in Path(item['path']).parents for item in refs.values()), 'profile_fixture_controls_only')
    meter = Meter(started, clock)
    plan = read_plan(refs, PROPOSED['single_source_footer_pass_bytes'])
    reader = BoundReader(refs, meter); entries, audit_raw, manifest = _parse_inputs(reader)
    require(len(entries) <= core.FIXTURE_MAX_FILES and all(output.parent in Path(item['path']).parents for item in entries),
        'profile_fixture_sources_only')
    actual_upper = manifest['planned_reads']['footer_read_bytes']//2
    meter.footer_ceiling = actual_upper
    plan = read_plan(refs, actual_upper)
    known_files = {item['path'] for item in refs.values()} | {item['path'] for item in entries}
    known_directories = {str(parent) for name in known_files for parent in Path(name).parents
        if output.parent == parent or output.parent in parent.parents}
    def input_namespace():
        files, nodes = set(), 0
        for path in output.parent.rglob('*'):
            if output == path or output in path.parents: continue
            nodes += 1; meter.check()
            require(nodes <= 2048, 'profile_fixture_input_namespace_node_bound')
            require(not path.is_symlink(), 'profile_fixture_input_namespace_symlink')
            if path.is_file(): files.add(str(path))
            else: require(path.is_dir() and str(path) in known_directories, 'profile_fixture_input_unknown_directory_or_special_file')
            require(len(files) <= 256, 'profile_fixture_input_namespace_bound')
        require(files == known_files, 'profile_fixture_input_namespace_changed')
    input_namespace()
    # Initial reads already charged. Reserve each final input once and a
    # conservative 2x retained-output bound; the plan includes every actual read.
    meter.reserve(sum(item['bytes'] for item in refs.values())+2*PROPOSED['all_new_retained_bytes'])
    writer = Publisher(output, meter)
    try:
        writer.publish('START.json', {'schema_version': SCHEMA, 'status': 'fixture_profile_started_not_admitted',
            'inputs': refs, 'proposed_sublimits_not_new_allowance': PROPOSED, 'read_plan': plan})
        summary = aggregate(entries, audit_rows(audit_raw), meter)
        require(summary['files'] == manifest['source_files'] and summary['rows'] == manifest['rows_scanned']
            and sum(role['source_bytes'] for role in summary['roles'].values()) == sum(item['bytes'] for item in entries),
            'profile_aggregate_source_conservation')
        require(meter.reads['source_footer'] <= actual_upper, 'profile_exceeds_saved_footer_read_plan')
        result = {'schema_version': SCHEMA, 'status': 'footer_profile_complete_fixture_only_not_reconciliation_admission',
            'fixture_only': True, 'production_admitted': False, 'inputs': refs,
            'source_order': 'literal_bound_roster_index_not_discovery', 'profile': summary,
            'original_inventory_source_epoch': manifest.get('source'),
            'original_source_git_blobs_verified_here': False, 'full_header_terminal_closure_reopened_here': False,
            'proposed_sublimits_not_new_allowance': PROPOSED, 'read_plan': plan,
            'observed_usage_at_profile_construction': meter.usage(), 'remaining_production_blockers': list(BLOCKERS),
            'trade_reconciliation_or_analysis_ready_admitted': False}
        profile = writer.publish('profile.json', result)
        def final_inputs():
            input_namespace()
            for item in entries: entry_identity(item)
            reader.reopen()  # Full input bytes/identities, no source data pages.
        def final_primitives():
            input_identities = {item['path']: {**item['identity'], 'bytes': item['bytes']} for item in entries}
            input_identities.update({item['path']: {**item['identity'], 'bytes': item['bytes']}
                for item in reader.captured.values()})
            output_identities = {str(output/name): {**item['identity'], 'bytes': item['bytes']}
                for name,item in writer.files.items()}
            primitive_gate(output.parent, output, input_identities, output_identities, meter.started)
        completion = {'schema_version': SCHEMA+'_completion', 'status': result['status'],
            'profile': profile, 'fixture_only': True, 'production_admitted': False,
            'root_independent_reopen_and_postcommit_directory_sync_required': True,
            'postcommit_directory_durability_claimed_here': False,
            'all_reads_charged_after_marker_staging_and_final_reopens': {},
            'all_metadata_read_bytes_charged_after_final_reopens': 0}
        # Exact fixed point: this marker is reopened twice; all already-owned
        # outputs and all bound inputs are reopened once at its final gate.
        final_inputs_bytes = sum(item['bytes'] for item in refs.values())
        prior_outputs_bytes = sum(item['bytes'] for item in writer.files.values())
        for _ in range(8):
            marker_bytes = len(encode(completion))
            predicted = dict(meter.reads)
            predicted['control_roster_generated_audit'] = predicted.get('control_roster_generated_audit', 0)+final_inputs_bytes
            predicted['generated_output_reopen'] = predicted.get('generated_output_reopen', 0)+prior_outputs_bytes+2*marker_bytes
            total = sum(predicted.values())
            if (completion['all_reads_charged_after_marker_staging_and_final_reopens'] == predicted
                    and completion['all_metadata_read_bytes_charged_after_final_reopens'] == total):
                break
            completion['all_reads_charged_after_marker_staging_and_final_reopens'] = predicted
            completion['all_metadata_read_bytes_charged_after_final_reopens'] = total
        else: raise Blocked('profile_completion_read_accounting_not_stable')
        finished = []
        primitive_inputs = final_primitives
        def terminal_primitives():
            require(dict(meter.reads) == completion['all_reads_charged_after_marker_staging_and_final_reopens']
                and sum(meter.reads.values()) == completion['all_metadata_read_bytes_charged_after_final_reopens'],
                'profile_final_read_accounting_differs')
            finished.append({**result, 'observed_usage_after_final_reopens': meter.usage(final=True),
                'root_independent_reopen_and_postcommit_directory_sync_required': True,
                'postcommit_directory_durability_claimed_here': False})
            primitive_inputs()  # Deadline/identity/namespace primitives LAST.
        writer.publish('COMPLETE.json', completion, final_check=final_inputs, final_primitive=terminal_primitives)
        return finished[0]  # Prebuilt result; no post-commit I/O/check/clock.
    except BaseException as exc:
        try:
            writer.publish('FAILED.json', {'schema_version': SCHEMA, 'status': 'profile_failed_unaccepted',
                'error_code': str(exc) if isinstance(exc, Blocked) else 'profile_IO_metadata_or_resource_failure',
                'usage': meter.usage(), 'production_admitted': False})
        except BaseException:
            pass  # Preserve all existing pending/failed bytes, never overwrite.
        raise


def run_production(*args, **kwargs):
    require_production_host()
    raise Blocked('native_source_profile_proof_source_boot_resource_admission_missing_before_IO')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--describe-contract', action='store_true')
    args = parser.parse_args(argv)
    if args.describe_contract:
        print(encode(contract()).decode(), end=''); return 0
    run_production()
    return 2


if __name__ == '__main__':
    raise SystemExit(main())
