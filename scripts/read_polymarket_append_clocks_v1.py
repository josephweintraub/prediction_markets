"""Bounded stored-header clock reader candidate, not an authenticated admission.

No collection, provider timestamps, global history replay or trade joins. Only
explicit selected files are opened. The consumer owns demanded-key coverage and
native block-hash comparison, and must stage callback effects until completion.
The private finite-limit core is reusable by a separately reviewed real driver;
the public production entry remains blocked. Local fixtures are tiny temp files.
"""
from __future__ import annotations

import hashlib
import io
import math
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time

from production_guard import require_production_host

SCHEMA = 'polymarket_append_stored_header_clock_reader_v1'
FIELDS = ('block_number', 'block_hash', 'parent_hash', 'timestamp')
RECORD_FIELDS = ('path', 'bytes', 'sha256', 'rows', 'source_file_index')
ROOT_FIELDS = ('path', 'bytes', 'sha256')
APPEND_BLOCK_RANGE = (88978538, 94796039)
BATCH_ROWS, HASH_CHUNK = 64, 64 * 1024
MAX_UTC = 253402300799
FIXTURE_MAX = {'source_bytes': 1024**2, 'read_bytes': 16*1024**2,
    'rows': 4096, 'batches': 4096, 'working_bytes': 16*1024**2,
    'runtime_seconds': 30, 'io_operations': 10000, 'files': 32, 'file_refs': 32}


class ClockReadBlocked(ValueError):
    """No completion; spent read counters are attached by the fixture wrapper."""


def require(value, code):
    if not value:
        raise ClockReadBlocked(code)


def header_schema():
    import pyarrow as pa
    return pa.schema([('block_number', pa.int64()), ('block_hash', pa.string()),
                      ('parent_hash', pa.string()), ('timestamp', pa.int64())])


def _integer(value, minimum=0):
    return type(value) is int and minimum <= value < 2**64


def _path(value):
    require(type(value) is str and value and len(value) <= 4096 and
            all(ord(c) >= 32 and not 127 <= ord(c) <= 159 for c in value), 'literal_path_schema')
    p = Path(value)
    require(p.is_absolute() and str(p) == value and '..' not in p.parts, 'literal_absolute_path')
    return p


def _stat(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_uid,
            value.st_gid, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _file_identity(path):
    try:
        value = os.lstat(path)
    except OSError:
        raise ClockReadBlocked('source_missing_or_unreadable') from None
    require(stat.S_ISREG(value.st_mode) and not stat.S_ISLNK(value.st_mode) and value.st_nlink == 1, 'source_not_literal_regular_file')
    return _stat(value)


def _ancestors(path):
    """Only stable directory object/owner/mode, not mutable child-list mtimes."""
    result = {}
    for parent in reversed(path.parents):
        try:
            value = os.lstat(parent)
        except OSError:
            raise ClockReadBlocked('source_ancestor_unreadable') from None
        require(stat.S_ISDIR(value.st_mode) and not stat.S_ISLNK(value.st_mode), 'source_ancestor_not_literal_directory')
        result[str(parent)] = (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid)
    return result


def _limits(value, *, fixture=False):
    require(type(value) is dict and set(value) == set(FIXTURE_MAX), 'finite_clock_limits_schema')
    require(all(type(v) is int and 0 < v < 2**63 for v in value.values()), 'finite_clock_limit_value')
    if fixture:
        require(all(value[k] <= cap for k, cap in FIXTURE_MAX.items()), 'fixture_limit_exceeded')
    return dict(value)


class _Meter:
    """Cooperative loader accounting only, not whole-process or consumer RSS."""
    def __init__(self, limits, clock):
        self.limits = _limits(limits)
        require(callable(clock), 'clock_not_callable')
        self.clock = clock
        self.live, self.retained = {}, {}
        self.usage = {k: 0 for k in ('source_bytes', 'read_bytes', 'hash_read_bytes',
            'arrow_read_bytes', 'rows', 'batches', 'files', 'file_refs', 'io_operations',
            'emitted_rows', 'callbacks', 'peak_working_bytes')}
        self.started = self._now()
        self.deadline = self.started + self.limits['runtime_seconds']

    def _now(self):
        value = self.clock()
        require(type(value) in (int, float) and math.isfinite(value), 'clock_nonfinite')
        return value

    def check(self):
        require(self._now() < self.deadline, 'clock_runtime_limit')
        self.primitive_working()

    def add(self, key, count):
        self.check()
        require(_integer(count), 'meter_count_schema')
        self.usage[key] += count
        require(self.usage[key] <= self.limits[key], 'clock_' + key + '_limit')

    def read(self, count, phase):
        self.add('read_bytes', count)
        self.usage[phase + '_read_bytes'] += count

    def primitive_working(self, count=0):
        require(_integer(count), 'working_count_schema')
        total = sum(self.live.values()) + sum(_python_bytes(v) for v in self.retained.values()) + count
        self.usage['peak_working_bytes'] = max(self.usage['peak_working_bytes'], total)
        require(total <= self.limits['working_bytes'], 'clock_working_bytes_limit')

    def working(self, count):
        self.check()
        self.primitive_working(count)

    def hold(self, name, count):
        require(type(name) is str and _integer(count), 'working_component_schema')
        self.live[name] = count
        self.check()

    def keep(self, name, value):
        self.live.pop(name, None)
        self.retained[name] = value
        self.check()

    def drop(self, name):
        self.retained.pop(name, None)
        self.live.pop(name, None)
        self.check()


class _Source(io.RawIOBase):
    """Same owned fd for SHA and Arrow; every read/seek is bounded and metered."""
    def __init__(self, path, expected, meter):
        super().__init__()
        self.path, self.expected, self.meter, self.phase = path, expected, meter, 'hash'
        self.fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            self.stable()
        except BaseException:
            os.close(self.fd)
            super().close()
            raise

    def stable(self):
        require(_stat(os.fstat(self.fd)) == self.expected and
                _file_identity(self.path) == self.expected, 'source_identity_changed')

    def readable(self): return True
    def seekable(self): return True

    def tell(self):
        self.meter.add('io_operations', 1)
        return os.lseek(self.fd, 0, os.SEEK_CUR)

    def seek(self, offset, whence=os.SEEK_SET):
        self.meter.add('io_operations', 1)
        require(type(offset) is int and type(whence) is int and whence in (0, 1, 2), 'source_seek_schema')
        position = offset + (0 if whence == 0 else os.lseek(self.fd, 0, os.SEEK_CUR)
                             if whence == 1 else self.expected[6])
        require(0 <= position <= self.expected[6], 'source_seek_outside_file')
        self.stable()
        return os.lseek(self.fd, position, os.SEEK_SET)

    def read(self, size=-1):
        self.meter.add('io_operations', 1)
        require(type(size) is int and size >= -1, 'source_read_schema')
        position = os.lseek(self.fd, 0, os.SEEK_CUR)
        count = self.expected[6] - position if size == -1 else min(size, self.expected[6] - position)
        self.meter.read(count, self.phase)
        # Read charging may itself check the supplied clock. The aggregate
        # allocation gate must follow that callback, not precede it.
        self.meter.working(count + sys.getsizeof(b''))
        self.stable()
        raw = os.read(self.fd, count)
        require(len(raw) == count, 'source_short_read')
        self.stable()
        return raw

    def readinto(self, target):
        self.meter.hold('readinto_target', len(target))
        try:
            raw = self.read(len(target)); target[:len(raw)] = raw
            count = len(raw); del raw, target
            return count
        finally:
            self.meter.live.pop('readinto_target', None)
            self.meter.primitive_working()

    def close(self):
        if not self.closed:
            os.close(self.fd)
        super().close()


def _python_bytes(value, active=None):
    """Conservative materialized container accounting, with shared refs counted."""
    active = set() if active is None else active
    require(id(value) not in active, 'cyclic_materialized_value')
    size = sys.getsizeof(value)
    if type(value) in (dict, list, tuple, set):
        active.add(id(value))
        if type(value) is dict:
            size += sum(_python_bytes(k, active) + _python_bytes(v, active) for k, v in value.items())
        else:
            size += sum(_python_bytes(v, active) for v in value)
        active.remove(id(value))
    return size


def _reference(value, fields):
    require(type(value) is dict and len(value) == len(fields) and set(value) == set(fields), 'exact_clock_reference_schema')
    _path(value['path'])
    require(_integer(value['bytes'], 1) and type(value['sha256']) is str and
            re.fullmatch('[0-9a-f]{64}', value['sha256']) is not None, 'clock_reference_binding')
    if 'rows' in fields:
        require(_integer(value['rows']) and _integer(value['source_file_index']), 'clock_reference_counts')
    return tuple(value[key] for key in fields)


def _freeze(records, selected_indices, root_binding, lower, upper, limits):
    """Freeze literal scalar bindings before the first supplied clock callback."""
    require(type(records) is list and 0 < len(records) <= limits['file_refs'], 'finite_explicit_clock_files')
    require(type(selected_indices) is list and 0 < len(selected_indices) <= limits['files'], 'explicit_selected_indices')
    require(all(_integer(v) for v in selected_indices) and
            all(a < b for a, b in zip(selected_indices, selected_indices[1:])), 'selected_indices_order_or_duplicate')
    require(type(lower) is int and type(upper) is int and 0 < lower <= upper < 2**63, 'clock_block_range')
    all_refs = tuple(_reference(v, RECORD_FIELDS) for v in records)
    require(all(a[4] < b[4] for a, b in zip(all_refs, all_refs[1:])), 'all_file_indices_order_or_duplicate')
    require(len({v[0] for v in all_refs}) == len(all_refs), 'duplicate_clock_path')
    selected = tuple(v for v in all_refs if v[4] in selected_indices)
    require(len(selected) == len(selected_indices), 'unknown_selected_file_index')
    require(sum(v[1] for v in selected) <= limits['source_bytes'], 'declared_clock_source_bytes_limit')
    require(sum(v[3] for v in selected) <= limits['rows'], 'declared_clock_rows_limit')
    frozen = {'records_input': records, 'root_input': root_binding, 'selection_input': selected_indices,
        'all': all_refs, 'selected': selected, 'root': _reference(root_binding, ROOT_FIELDS),
        'selection': tuple(selected_indices), 'lower': lower, 'upper': upper}
    require(_python_bytes(frozen) + 16384 <= limits['working_bytes'], 'clock_binding_working_limit')
    return frozen


def _same_reference(value, expected, fields):
    return type(value) is dict and len(value) == len(fields) and set(value) == set(fields) and all(
        type(value[key]) is type(old) and value[key] == old for key, old in zip(fields, expected))


def _unchanged(frozen):
    records, selection = frozen['records_input'], frozen['selection_input']
    require(type(records) is list and len(records) == len(frozen['all']) and
        all(_same_reference(value, old, RECORD_FIELDS) for value, old in zip(records, frozen['all'])) and
        _same_reference(frozen['root_input'], frozen['root'], ROOT_FIELDS) and
        type(selection) is list and len(selection) == len(frozen['selection']) and
        all(type(v) is int and v == old for v, old in zip(selection, frozen['selection'])),
        'clock_input_binding_changed')


def _reopen(captured):
    for record, identity, ancestors in captured:
        path = Path(record[0])
        require(_ancestors(path) == ancestors and _file_identity(path) == identity, 'source_identity_changed')


def _gate(frozen, captured, meter):
    meter.check()
    _unchanged(frozen)
    _reopen(captured)


def _header(row, previous):
    require(type(row) is dict and tuple(row) == FIELDS and
        type(row['block_number']) is int and 0 < row['block_number'] < 2**63 and
        type(row['timestamp']) is int and 0 < row['timestamp'] <= MAX_UTC and
        all(type(row[key]) is str and re.fullmatch('0x[0-9a-f]{64}', row[key]) is not None
            for key in ('block_hash', 'parent_hash')), 'integer_UTC_or_canonical_header_key')
    current = tuple(row[key] for key in FIELDS)
    if previous is not None:
        require(current[0] != previous[0], 'duplicate_or_conflicting_clock_block')
        require(current[0] > previous[0], 'nonmonotone_clock_block')
        require(current[3] >= previous[3], 'nonmonotone_clock_timestamp')
        if current[0] == previous[0] + 1:
            require(current[2] == previous[1], 'adjacent_clock_parent_conflict')
    return current


def _stream_clocks(frozen, consume, *, meter):
    """Private finite core; supplied bindings/meter do NOT authenticate a source.

    Consumer receives tuple((header_tuple, (file_index, physical_ordinal, path)),...).
    Coverage is exactly selected files/range observations, never demanded keys.
    A real reviewed driver must enforce shared inclusive hard timer/AS and root
    authentication independently. This function never upgrades supplied dicts.
    """
    require(callable(consume), 'clock_consumer_not_callable')
    meter.keep('bindings', frozen)
    meter.hold('reader_state', 16384)
    _unchanged(frozen)
    captured = []
    meter.keep('captured', captured)
    seen_inodes = set()
    meter.keep('seen_inodes', seen_inodes)
    for record in frozen['selected']:
        path = Path(record[0])
        meter.hold('capture_build', 8192 + 4096 * len(path.parents))
        ancestors, identity = _ancestors(path), _file_identity(path)
        require(identity[6] == record[1], 'source_length_differs')
        require(identity[:2] not in seen_inodes, 'duplicate_clock_inode')
        seen_inodes.add(identity[:2])
        captured.append((record, identity, ancestors))
        meter.working(0)
    meter.hold('capture_build', 0)
    meter.add('file_refs', len(frozen['all']))
    meter.add('source_bytes', sum(v[1] for v in frozen['selected']))
    _gate(frozen, captured, meter)
    import pyarrow as pa
    import pyarrow.parquet as pq
    previous = None
    file_counts = []
    meter.keep('file_counts', file_counts)
    for record, identity, _ in captured:
        path, size, sha, expected_rows, index = record
        ordinal, emitted, first, last = 0, 0, None, None
        with _Source(Path(path), identity, meter) as source:
            hashed = hashlib.sha256()
            while source.tell() < size:
                hashed.update(source.read(min(HASH_CHUNK, size-source.tell())))
            require(hashed.hexdigest() == sha, 'full_clock_source_sha256_differs')
            source.seek(0); source.phase = 'arrow'
            meter.hold('arrow_compressed', 2*size)
            with pa.PythonFile(source, mode='r') as arrow_source:
                parquet = pq.ParquetFile(arrow_source, pre_buffer=False, buffer_size=0,
                    thrift_string_size_limit=1024**2, thrift_container_size_limit=10000)
                require(parquet.schema_arrow.equals(header_schema(), check_metadata=True), 'exact_header_arrow_schema_differs')
                require(parquet.metadata.num_rows == expected_rows, 'clock_manifest_footer_row_count_differs')
                decoded = 0
                for group in range(parquet.metadata.num_row_groups):
                    for column in range(4):
                        item = parquet.metadata.row_group(group).column(column)
                        require(item.file_path in ('', None) and item.compression == 'ZSTD', 'external_or_non_zstd_header_column')
                        require(type(item.total_uncompressed_size) is int and item.total_uncompressed_size >= 0,
                                'header_uncompressed_size_schema')
                        decoded += item.total_uncompressed_size
                meter.hold('arrow_decoded', decoded)
                for batch in parquet.iter_batches(batch_size=BATCH_ROWS, use_threads=False):
                    meter.add('batches', 1)
                    meter.hold('batch', batch.nbytes)
                    require(0 < batch.num_rows <= BATCH_ROWS and ordinal+batch.num_rows <= expected_rows,
                            'clock_batch_or_row_count_differs')
                    meter.add('rows', batch.num_rows)
                    reserve = 32*batch.nbytes+4096*batch.num_rows
                    meter.hold('python_rows', reserve)
                    rows = batch.to_pylist()
                    require(_python_bytes(rows) <= reserve, 'clock_python_conversion_reservation_exceeded')
                    meter.keep('python_rows', rows)
                    meter.hold('output_batch', reserve)
                    output = []
                    for row in rows:
                        previous = _header(row, previous)
                        if first is None: first = previous[0]
                        last = previous[0]
                        if frozen['lower'] <= previous[0] <= frozen['upper']:
                            output.append((previous, (index, ordinal, path)))
                        ordinal += 1
                    require(_python_bytes(output) <= reserve, 'clock_output_reservation_exceeded')
                    meter.keep('output_batch', output)
                    if output:
                        meter.hold('callback_tuple', _python_bytes(output))
                        payload = tuple(output)
                        meter.keep('callback_tuple', payload)
                        _gate(frozen, captured, meter)
                        require(consume(payload) is None, 'clock_consumer_return_must_be_none')
                        emitted += len(payload)
                        meter.usage['emitted_rows'] += len(payload)
                        meter.usage['callbacks'] += 1
                        _gate(frozen, captured, meter)
                        del payload
                        meter.drop('callback_tuple')
                    del row, rows, output, batch
                    meter.drop('python_rows'); meter.drop('output_batch'); meter.hold('batch', 0)
                require(ordinal == expected_rows, 'clock_observed_row_count_differs')
                file_counts.append({'source_file_index': index, 'observed_rows': ordinal,
                    'emitted_rows': emitted, 'first_block': first, 'last_block': last})
                _gate(frozen, captured, meter)
                del parquet
            meter.hold('arrow_decoded', 0); meter.hold('arrow_compressed', 0)
        meter.add('files', 1)
    require(meter.usage['rows'] == sum(v[3] for v in frozen['selected']) and
            meter.usage['hash_read_bytes'] == meter.usage['source_bytes'], 'clock_final_count_invariant')
    reserve = 4*_python_bytes(frozen)+4*_python_bytes(captured)+65536
    meter.hold('report', reserve)
    identity_names = ('device', 'inode', 'mode', 'nlink', 'uid', 'gid', 'bytes', 'mtime_ns', 'ctime_ns')
    result = {'schema_version': SCHEMA, 'status': 'stored_clock_component_complete_not_admitted',
        'root_binding': dict(zip(ROOT_FIELDS, frozen['root'])),
        'all_file_bindings': [dict(zip(RECORD_FIELDS, v)) for v in frozen['all']],
        'selected_file_indices': list(frozen['selection']),
        'unselected_file_indices': [v[4] for v in frozen['all'] if v[4] not in frozen['selection']],
        'unselected_rows_not_read_or_claimed': sum(v[3] for v in frozen['all'] if v[4] not in frozen['selection']),
        'requested_block_range': [frozen['lower'], frozen['upper']],
        'file_counts': file_counts,
        'source_identities': {v[0]: dict(zip(identity_names, identity)) for v, identity, _ in captured},
        'usage': dict(meter.usage), 'external_manifest_authenticated': False,
        'production_admitted': False, 'demanded_key_coverage_claimed': False,
        'native_observed_hash_comparison_performed': False, 'provider_clock_used': False,
        'cryptographic_header_verification_performed': False, 'unselected_payloads_opened': False,
        'consumer_retained_memory_accounted': False, 'whole_process_memory_enforced': False,
        'data_written': False}
    require(_python_bytes(result) <= reserve, 'clock_report_reservation_exceeded')
    meter.keep('report', result)
    _gate(frozen, captured, meter)
    result['usage'] = dict(meter.usage)
    # LAST: only primitive memory/input/identity checks, no clock or callbacks.
    meter.primitive_working()
    _unchanged(frozen)
    _reopen(captured)
    return result


def stream_fixture(records, selected_indices, consume, *, root_binding,
                   lower, upper, fixture_root, limits=None, clock=time.monotonic):
    """Tiny temp fixtures only. Never accepts an authenticated production flag."""
    limits = _limits(dict(FIXTURE_MAX) if limits is None else limits, fixture=True)
    frozen = _freeze(records, selected_indices, root_binding, lower, upper, limits)
    root = _path(str(fixture_root))
    temporary = Path(tempfile.gettempdir()).resolve()
    require(root.is_relative_to(temporary) and root != temporary and not root.is_relative_to(Path('/mnt')),
            'temporary_clock_fixture_root_only')
    require(all(Path(v[0]).is_relative_to(root) and Path(v[0]) != root for v in frozen['all']),
            'clock_file_outside_fixture_root')
    require(sum(v[1] for v in frozen['all']) <= limits['source_bytes'] and
            sum(v[3] for v in frozen['all']) <= limits['rows'], 'fixture_total_source_or_row_limit')
    meter = _Meter(limits, clock)
    try:
        return _stream_clocks(frozen, consume, meter=meter)
    except BaseException as error:
        error.clock_usage = dict(meter.usage)
        raise


def run_production(*_args, **_kwargs):
    require_production_host()
    raise ClockReadBlocked('trusted_header_root_context_and_finite_production_limits_not_admitted')


if __name__ == '__main__':
    run_production()
