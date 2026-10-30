"""Bounded physical Parquet streaming candidate, NOT a source admission.

Only explicit tiny temporary fixture files can be read through the public API.
The future production driver must authenticate the two append manifests/roots,
finite whole-stage limits, source code and process ownership independently.
Callbacks must stage any effects: an exception withholds completion, not rows
already observed by a callback. No decoding, filtering, dedup or clocks occur.
"""
from __future__ import annotations

from copy import deepcopy
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

SCHEMA = 'polymarket_append_physical_native_stream_v1'
BATCH_ROWS = 64
HASH_CHUNK = 64 * 1024
MAX_FILES = 32
GROUPS = ('append_probe_v2', 'append_campaign_v2')
FIELDS = ('address', 'blockHash', 'blockNumber', 'blockTimestamp', 'data', 'logIndex',
          'removed', 'topics', 'transactionHash', 'transactionIndex', 'block_number',
          'log_index', 'transaction_index', 'exchange_address', 'window',
          'source_request_id', 'provider_block_timestamp_present')
RECORD_FIELDS = {'path', 'bytes', 'sha256', 'rows', 'source_file_index', 'source_group'}
FIXTURE_MAX = {'source_bytes': 1024**2, 'read_bytes': 16 * 1024**2, 'rows': 4096,
               'batches': 4096, 'working_bytes': 16 * 1024**2,
               'runtime_seconds': 30, 'io_operations': 10000}
SCOPE = {'fixture_only': True, 'external_manifest_authenticated': False,
         'production_admitted': False, 'execution_clock_verified': False,
         'provider_timestamp_used_as_execution_clock': False,
         'deduplication_performed': False, 'data_written': False,
         'hard_process_memory_enforcement_implemented': False,
         'fixture_runtime_checks_are_cooperative': True}


class StreamBlocked(ValueError):
    """Fixed non-sensitive failure code; no successful completion is returned."""


def require(value, code):
    if not value:
        raise StreamBlocked(code)


def native_schema():
    import pyarrow as pa
    return pa.schema([(name, pa.bool_() if name == 'removed' else
                       pa.list_(pa.field('element', pa.string())) if name == 'topics' else pa.string())
                      for name in FIELDS[:10]] +
                     [(name, pa.uint64()) for name in FIELDS[10:13]] +
                     [('exchange_address', pa.string()), ('window', pa.string()),
                      ('source_request_id', pa.uint64()),
                      ('provider_block_timestamp_present', pa.bool_())])


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
        raise StreamBlocked('source_missing_or_unreadable') from None
    require(stat.S_ISREG(value.st_mode) and not stat.S_ISLNK(value.st_mode), 'source_not_literal_regular_file')
    return _stat(value)


def _ancestors(path):
    """Only stable directory object/owner/mode, not mutable child-list mtimes."""
    result = {}
    for parent in reversed(path.parents):
        try:
            value = os.lstat(parent)
        except OSError:
            raise StreamBlocked('source_ancestor_unreadable') from None
        require(stat.S_ISDIR(value.st_mode) and not stat.S_ISLNK(value.st_mode), 'source_ancestor_not_literal_directory')
        result[str(parent)] = (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid)
    return result


class Meter:
    def __init__(self, limits, clock):
        require(type(limits) is dict and set(limits) == set(FIXTURE_MAX), 'finite_fixture_limits_schema')
        for key, maximum in FIXTURE_MAX.items():
            value = limits[key]
            require(type(value) is int and 0 < value <= maximum, 'finite_fixture_limit_value')
        require(callable(clock), 'clock_not_callable')
        self.limits, self.clock = dict(limits), clock
        self.started = self._now()
        self.deadline = self.started + limits['runtime_seconds']
        self.usage = {key: 0 for key in ('source_bytes', 'read_bytes', 'hash_read_bytes',
                     'arrow_read_bytes', 'rows', 'batches', 'files', 'io_operations', 'peak_working_bytes')}

    def _now(self):
        value = self.clock()
        require(type(value) in (int, float) and math.isfinite(value), 'clock_nonfinite')
        return value

    def check(self):
        require(self._now() < self.deadline, 'stream_runtime_limit')

    def add(self, key, count):
        self.check()
        require(_integer(count), 'meter_count_schema')
        self.usage[key] += count
        require(self.usage[key] <= self.limits[key], 'stream_' + key + '_limit')

    def read(self, count, phase):
        self.add('read_bytes', count)  # Failed reads retain their reserved charge.
        self.usage[phase + '_read_bytes'] += count

    def working(self, count):
        self.check()
        self.usage['peak_working_bytes'] = max(self.usage['peak_working_bytes'], count)
        require(count <= self.limits['working_bytes'], 'stream_working_bytes_limit')


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
        self.meter.working(count)
        self.meter.read(count, self.phase)
        self.stable()
        raw = os.read(self.fd, count)
        require(len(raw) == count, 'source_short_read')
        self.stable()
        return raw

    def readinto(self, target):
        raw = self.read(len(target)); target[:len(raw)] = raw; return len(raw)

    def close(self):
        if not self.closed:
            os.close(self.fd)
        super().close()


def _python_bytes(value):
    """Conservative materialized container accounting, with shared refs counted."""
    size = sys.getsizeof(value)
    if type(value) is dict:
        size += sum(_python_bytes(k) + _python_bytes(v) for k, v in value.items())
    elif type(value) in (list, tuple):
        size += sum(_python_bytes(v) for v in value)
    return size


def _records(records, root):
    require(type(records) is list and 0 < len(records) <= MAX_FILES, 'explicit_fixture_file_list')
    captured, paths, inodes, indices = [], set(), set(), set()
    for record in records:
        require(type(record) is dict and set(record) == RECORD_FIELDS, 'exact_manifest_record_schema')
        path = _path(record['path'])
        require(path.is_relative_to(root) and path != root, 'file_outside_explicit_fixture_root')
        require(_integer(record['bytes'], 1) and _integer(record['rows']) and
                _integer(record['source_file_index']) and type(record['sha256']) is str and
                re.fullmatch('[0-9a-f]{64}', record['sha256']) is not None and
                type(record['source_group']) is str and record['source_group'] in GROUPS, 'manifest_record_types')
        ancestors, identity = _ancestors(path), _file_identity(path)
        require(identity[6] == record['bytes'], 'source_length_differs')
        require(str(path) not in paths and identity[:2] not in inodes and
                record['source_file_index'] not in indices, 'duplicate_path_inode_or_file_index')
        paths.add(str(path)); inodes.add(identity[:2]); indices.add(record['source_file_index'])
        captured.append((deepcopy(record), identity, ancestors))
    return captured


def _reopen(captured):
    for record, identity, ancestors in captured:
        path = Path(record['path'])
        require(_ancestors(path) == ancestors and _file_identity(path) == identity, 'source_identity_changed')


def _stream_fixture(records, consume, *, fixture_root, meter):
    """Deliver immutable-value raw rows + locator, in explicit file/physical order.

    Supplied records are NOT authenticated receipts. Full SHA is verified before
    any callback for that file; all callbacks and mutable clocks precede final
    primitive identity checks. A future caller must not publish partial effects.
    """
    root = _path(str(fixture_root))
    temporary = Path(tempfile.gettempdir()).resolve()
    require(root.is_relative_to(temporary) and root != temporary and
            not root.is_relative_to(Path('/mnt')), 'temporary_fixture_root_only')
    require(callable(consume), 'consumer_not_callable')
    captured = _records(records, root)
    meter.working(_python_bytes(captured))
    meter.add('source_bytes', sum(record['bytes'] for record, _, _ in captured))
    require(sum(record['rows'] for record, _, _ in captured) <= meter.limits['rows'], 'declared_row_limit')
    meter.check(); _reopen(captured)
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = native_schema()
    for record, identity, _ in captured:
        path = Path(record['path'])
        with _Source(path, identity, meter) as source:
            digest = hashlib.sha256()
            while source.tell() < record['bytes']:
                digest.update(source.read(min(HASH_CHUNK, record['bytes'] - source.tell())))
            require(digest.hexdigest() == record['sha256'], 'full_source_sha256_differs')
            source.seek(0); source.phase = 'arrow'
            with pa.PythonFile(source, mode='r') as arrow_source:
                parquet = pq.ParquetFile(arrow_source, pre_buffer=False, buffer_size=0,
                                         thrift_string_size_limit=1024**2, thrift_container_size_limit=10000)
                require(parquet.schema_arrow.equals(schema, check_metadata=True), 'exact_native_arrow_schema_differs')
                require(parquet.metadata.num_rows == record['rows'], 'manifest_footer_row_count_differs')
                for group in range(parquet.metadata.num_row_groups):
                    columns = [parquet.metadata.row_group(group).column(i)
                               for i in range(parquet.metadata.num_columns)]
                    require(all(column.file_path in ('', None) for column in columns), 'external_column_file_forbidden')
                    width = sum(column.total_uncompressed_size for column in columns)
                    require(type(width) is int and 0 <= width <= meter.limits['working_bytes'] // 4,
                            'row_group_uncompressed_working_bound')
                ordinal = 0
                for batch in parquet.iter_batches(batch_size=BATCH_ROWS, use_threads=False):
                    meter.add('batches', 1)
                    meter.working(batch.nbytes + _python_bytes(captured))
                    require(0 < batch.num_rows <= BATCH_ROWS and
                            ordinal + batch.num_rows <= record['rows'], 'batch_or_row_count_differs')
                    values = batch.to_pylist()
                    meter.working(batch.nbytes + _python_bytes(values) + _python_bytes(captured))
                    meter.check(); _reopen(captured)
                    for row in values:
                        require(set(row) == set(FIELDS), 'native_python_row_schema_differs')
                        meter.add('rows', 1)
                        locator = {'source_path': record['path'], 'source_file_index': record['source_file_index'],
                                   'physical_row_ordinal': ordinal}
                        consume(row, locator)
                        ordinal += 1
                        meter.check(); _reopen(captured)
                    del values, batch
                require(ordinal == record['rows'], 'physical_row_count_differs')
                meter.check(); _reopen(captured)
            meter.usage['files'] += 1
    names = ('device', 'inode', 'mode', 'nlink', 'uid', 'gid', 'bytes', 'mtime_ns', 'ctime_ns')
    result = {'schema_version': SCHEMA, 'status': 'fixture_stream_complete_not_admitted',
              'usage': dict(meter.usage), 'file_counts': [dict(path=r['path'], rows=r['rows'],
                  bytes=r['bytes'], sha256=r['sha256'], source_file_index=r['source_file_index'],
                  source_group=r['source_group']) for r, _, _ in captured],
              'source_identities': {r['path']: dict(zip(names, identity))
                                    for r, identity, _ in captured}, **SCOPE}
    meter.check(); _reopen(captured)  # LAST: no user clock/callback after this gate.
    return result


def stream_fixture(records, consume, *, fixture_root, limits=None, clock=time.monotonic):
    """Small-file candidate only; failures retain spent counters on the error.

    ``error.stream_usage`` never constitutes a completed stream or an admission.
    The production adapter still needs its own inclusive hard timer/AS limits.
    """
    meter = Meter(dict(FIXTURE_MAX) if limits is None else limits, clock)
    try:
        return _stream_fixture(records, consume, fixture_root=fixture_root, meter=meter)
    except BaseException as error:
        error.stream_usage = dict(meter.usage)
        raise


def run_production(*_args, **_kwargs):
    require_production_host()
    raise StreamBlocked('authenticated_append_stream_production_driver_not_admitted')


if __name__ == '__main__':
    run_production()
