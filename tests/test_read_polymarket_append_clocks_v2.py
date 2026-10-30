"""Tiny stored-header fixtures only; no accepted payloads, providers or host IO."""
from copy import deepcopy
import hashlib
import ast
import base64
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import read_polymarket_append_clocks_v2 as m
from scripts import read_polymarket_append_clocks_v1 as old


class StoredClockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='append_clock_fixture_')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.binding = {'path': str(self.root/'manifest.json'), 'bytes': 1, 'sha256': 'a'*64}

    def row(self, number=10, timestamp=None):
        return {'block_number': number, 'block_hash': '0x'+format(number, '064x'),
                'parent_hash': '0x'+format(number-1, '064x'),
                'timestamp': 1700000000+number if timestamp is None else timestamp}

    def file(self, rows=None, *, index=0, name=None, schema=None, compression='zstd'):
        rows = [self.row()] if rows is None else rows
        path = self.root/(name or f'headers-{index}.parquet')
        schema = m.header_schema() if schema is None else schema
        physical = [dict(row) for row in rows]
        for row in physical:
            for key in ('block_hash', 'parent_hash'):
                if key in schema.names and pa.types.is_fixed_size_binary(schema.field(key).type):
                    value = row[key]
                    if type(value) is str and len(value) == 66 and value.startswith('0x'):
                        row[key] = bytes.fromhex(value[2:])
        pq.write_table(pa.Table.from_pylist(physical, schema=schema),
                       path, compression=compression, row_group_size=41)
        raw = path.read_bytes()
        return {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                'rows': len(rows), 'source_file_index': index}

    def limits(self, **changes):
        return dict(m.FIXTURE_MAX) | changes

    def execute(self, records=None, consume=None, *, selected=None, lower=1, upper=1000, **kwargs):
        records = [self.file()] if records is None else records
        selected = [v['source_file_index'] for v in records] if selected is None else selected
        return m.stream_fixture(records, selected, (lambda _: None) if consume is None else consume,
            root_binding=self.binding, lower=lower, upper=upper, fixture_root=self.root, **kwargs)

    def test_exact_collector_four_columns_int64_and_zstd(self):
        from scripts import collect_polymarket_exact_header_campaign_v2 as collector
        rows = [self.row(10), self.row(11)]
        raw = collector.compact_parquet(rows)
        path = self.root/'collector.parquet'; path.write_bytes(raw)
        record = {'path': str(path), 'bytes': len(raw), 'sha256': m.hashlib.sha256(raw).hexdigest(),
                  'rows': 2, 'source_file_index': 0}
        out = []; result = self.execute([record], lambda batch: out.extend(batch))
        self.assertEqual(m.header_schema(), pa.schema([('block_number', pa.int64()),
            ('block_hash', pa.binary(32)), ('parent_hash', pa.binary(32)), ('timestamp', pa.int64())]))
        self.assertEqual([v[0] for v in out], [tuple(row[k] for k in m.FIELDS) for row in rows])
        self.assertEqual(result['usage']['emitted_rows'], 2)

    def test_batch_range_order_counts_and_physical_locators(self):
        first = self.file([self.row(i) for i in range(1, 132)], index=2)
        second = self.file([self.row(i) for i in range(132, 135)], index=7)
        output = []; sizes = []
        def consume(batch):
            self.assertIs(type(batch), tuple)
            sizes.append(len(batch)); output.extend(batch)
        result = self.execute([first, second], consume, lower=64, upper=133)
        self.assertEqual([v[0][0] for v in output], list(range(64, 134)))
        self.assertEqual([v[1][1] for v in output], list(range(63, 131))+[0, 1])
        self.assertEqual([v[1][0] for v in output], [2]*68+[7]*2)
        self.assertLessEqual(max(sizes), m.BATCH_ROWS)
        self.assertEqual(result['usage']['rows'], 134)
        self.assertEqual(result['usage']['emitted_rows'], 70)
        self.assertEqual(result['usage']['hash_read_bytes'], first['bytes']+second['bytes'])
        self.assertGreater(result['usage']['arrow_read_bytes'], 0)
        self.assertEqual(result['usage']['read_bytes'],
            result['usage']['hash_read_bytes']+result['usage']['arrow_read_bytes'])

    def test_unselected_missing_file_never_opened_and_not_claimed_coverage(self):
        first = self.file([self.row(10)], index=1)
        absent = self.file([self.row(11)], index=2); Path(absent['path']).unlink()
        opened = []; original = m.os.open
        def tracked(path, *args, **kwargs):
            opened.append(str(path)); return original(path, *args, **kwargs)
        with patch.object(m.os, 'open', side_effect=tracked):
            result = self.execute([first, absent], selected=[1])
        self.assertEqual(opened, [first['path']])
        self.assertEqual(result['all_file_bindings'], [first, absent])
        self.assertEqual(result['root_binding'], self.binding)
        self.assertEqual(result['unselected_file_indices'], [2])
        self.assertEqual(result['unselected_rows_not_read_or_claimed'], 1)
        for key in ('production_admitted', 'external_manifest_authenticated',
                    'demanded_key_coverage_claimed', 'native_observed_hash_comparison_performed',
                    'provider_clock_used', 'whole_process_memory_enforced',
                    'consumer_retained_memory_accounted'):
            self.assertFalse(result[key])

    def test_empty_or_no_range_rows_do_not_fabricate_clocks(self):
        for rows in ([], [self.row(10)]):
            result = self.execute([self.file(rows)], lambda _: self.fail('outside callback'), lower=20, upper=30)
            self.assertEqual(result['usage']['emitted_rows'], 0)
            self.assertEqual(result['usage']['rows'], len(rows))

    def test_exact_inclusive_range_edges_and_documented_append_range(self):
        self.assertEqual(m.APPEND_BLOCK_RANGE, (88978538, 94796039))
        out = []; self.execute([self.file([self.row(i) for i in range(9, 13)])],
            lambda b: out.extend(b), lower=10, upper=11)
        self.assertEqual([row[0][0] for row in out], [10, 11])

    def test_full_payload_corruption_with_valid_footer_fails_before_arrow(self):
        record = self.file(); path = Path(record['path']); raw = path.read_bytes()
        length = int.from_bytes(raw[-8:-4], 'little'); changed = bytearray(raw); changed[12] ^= 1
        path.write_bytes(changed); self.assertEqual(raw[-length-8:], changed[-length-8:])
        with patch.object(pq, 'ParquetFile', side_effect=AssertionError('Arrow before full SHA')):
            with self.assertRaisesRegex(m.ClockReadBlocked, 'full_clock_source_sha256'):
                self.execute([record])

    def test_missing_selected_payload_and_length_or_hash_mismatch(self):
        record = self.file()
        for changes in ({'bytes': record['bytes']+1}, {'sha256': '0'*64}):
            with self.subTest(changes=changes), self.assertRaises(m.ClockReadBlocked):
                self.execute([record | changes])
        Path(record['path']).unlink()
        with self.assertRaisesRegex(m.ClockReadBlocked, 'source_missing'):
            self.execute([record])

    def test_strict_reference_types_extra_flags_and_sorted_explicit_selection(self):
        record = self.file()
        for changes in ({'bytes': True}, {'rows': True}, {'rows': -1}, {'source_file_index': True},
                        {'sha256': 'A'*64}, {'authenticated': True}, {'keys': 1}):
            with self.subTest(changes=changes), self.assertRaises(m.ClockReadBlocked):
                self.execute([record | changes])
        for selected in ([], (), None, [0, 0], [1], [True], [2, 1]):
            # None means public convenience default in execute(), so call directly.
            with self.subTest(selected=selected), self.assertRaises(m.ClockReadBlocked):
                m.stream_fixture([record], selected, lambda _: None, root_binding=self.binding,
                    lower=1, upper=20, fixture_root=self.root)
        for bad in ({}, self.binding | {'accepted': True}, self.binding | {'bytes': True}):
            with self.subTest(binding=bad), self.assertRaises(m.ClockReadBlocked):
                m.stream_fixture([record], [0], lambda _: None, root_binding=bad,
                    lower=1, upper=20, fixture_root=self.root)

    def test_duplicate_paths_indices_inodes_and_nonliteral_paths(self):
        first = self.file(index=0); second = self.file([self.row(11)], index=1)
        for records in ([first, first], [first, second | {'source_file_index': 0}],
                        [second, first], [first, second | {'path': first['path']}]):
            with self.subTest(records=records), self.assertRaises(m.ClockReadBlocked):
                self.execute(records)
        for path in ('relative', first['path']+'/', str(self.root/'..'/'bad'), first['path']+'\n'):
            with self.subTest(path=path), self.assertRaises(m.ClockReadBlocked):
                self.execute([first | {'path': path}])
        alias = self.root/'hardlink.parquet'; os.link(first['path'], alias)
        with self.assertRaises(m.ClockReadBlocked): self.execute([first])

    def test_file_parent_symlinks_and_directory_rejected(self):
        first = self.file(); link = self.root/'link'; link.symlink_to(first['path'])
        directory = self.root/'real'; directory.mkdir()
        child = self.file(name='real/child.parquet')
        linkdir = self.root/'linked'; linkdir.symlink_to(directory, target_is_directory=True)
        for bad in (first | {'path': str(link)}, child | {'path': str(linkdir/'child.parquet')},
                    first | {'path': str(directory)}):
            with self.subTest(path=bad['path']), self.assertRaises(m.ClockReadBlocked): self.execute([bad])

    def test_exact_schema_rejects_reorder_extra_unsigned_metadata_and_non_zstd(self):
        schema = m.header_schema()
        alternatives = [pa.schema(list(schema)[::-1]), pa.schema(list(schema)[:-1]),
            pa.schema(list(schema)+[pa.field('extra', pa.string())]),
            schema.set(0, pa.field('block_number', pa.uint64())),
            schema.set(3, pa.field('timestamp', pa.float64())), schema.with_metadata({'x': 'y'})]
        for i, candidate in enumerate(alternatives):
            with self.subTest(i=i), self.assertRaisesRegex(m.ClockReadBlocked, 'exact_header_arrow_schema'):
                self.execute([self.file(schema=candidate, name=f'wrong{i}.parquet')])
        with self.assertRaisesRegex(m.ClockReadBlocked, 'non_zstd'):
            self.execute([self.file(compression='snappy')])

    def test_null_integer_hash_UTC_bounds_and_case_are_not_coerced(self):
        for key, value in [('block_number', None), ('block_number', 0), ('block_number', -1),
            ('timestamp', None), ('timestamp', 0), ('timestamp', -1),
            ('timestamp', m.MAX_UTC+1), ('block_hash', None), ('parent_hash', None)]:
            with self.subTest(key=key, value=value), self.assertRaisesRegex(
                    m.ClockReadBlocked, 'integer_UTC_or_canonical|exact_binary32'):
                self.execute([self.file([self.row() | {key: value}])])
        # The normalized semantic validator remains unchanged, including bool.
        with self.assertRaises(m.ClockReadBlocked):m._header(self.row() | {'timestamp': True}, None)
        with self.assertRaises(m.ClockReadBlocked):
            m._header(self.row() | {'block_hash': '0x'+'AB'*32}, None)

    def test_same_block_agreeing_or_conflicting_is_not_fanned_out(self):
        for second in (self.row(10), self.row(10) | {'timestamp': 1700000999}):
            with self.subTest(second=second), self.assertRaisesRegex(m.ClockReadBlocked, 'duplicate_or_conflicting'):
                self.execute([self.file([self.row(10), second])])

    def test_nonmonotone_keys_timestamps_and_adjacent_parent_are_blocked(self):
        cases = [[self.row(11), self.row(10)],
            [self.row(10), self.row(11, 1700000001)],
            [self.row(10), self.row(11) | {'parent_hash': '0x'+'f'*64}]]
        for rows in cases:
            with self.subTest(rows=rows), self.assertRaises(m.ClockReadBlocked): self.execute([self.file(rows)])

    def test_cross_file_duplicates_nonmonotonicity_and_parent_conflicts(self):
        for row in (self.row(10), self.row(9), self.row(11) | {'parent_hash': '0x'+'f'*64}):
            first = self.file([self.row(10)], index=0)
            second = self.file([row], index=1)
            with self.subTest(row=row), self.assertRaises(m.ClockReadBlocked): self.execute([first, second])

    def test_sparse_keys_allowed_but_no_demand_or_contiguous_coverage_claim(self):
        rows = [self.row(10), self.row(15)]
        output = []; result = self.execute([self.file(rows)], lambda b: output.extend(b), lower=10, upper=15)
        self.assertEqual([v[0][0] for v in output], [10, 15])
        self.assertFalse(result['demanded_key_coverage_claimed'])

    def test_declared_footer_and_observed_row_count_all_bound(self):
        record = self.file()
        for count in (0, 2):
            with self.subTest(count=count), self.assertRaisesRegex(m.ClockReadBlocked, 'footer_row_count'):
                self.execute([record | {'rows': count}])
        original = pq.ParquetFile
        class MissingRows:
            def __init__(self, *args, **kwargs):
                self.real = original(*args, **kwargs)
                self.schema_arrow, self.metadata = self.real.schema_arrow, self.real.metadata
            def iter_batches(self, **kwargs): return iter(())
        with patch.object(pq, 'ParquetFile', MissingRows), self.assertRaisesRegex(m.ClockReadBlocked, 'observed_row_count'):
            self.execute([record])

    def test_callback_payload_is_deeply_immutable_and_return_must_be_none(self):
        def callback(batch):
            self.assertIs(type(batch), tuple)
            self.assertIs(type(batch[0]), tuple)
            self.assertIs(type(batch[0][0]), tuple)
            self.assertIs(type(batch[0][1]), tuple)
            with self.assertRaises(TypeError): batch[0][0][0] = 1
        self.execute(consume=callback)
        with self.assertRaisesRegex(m.ClockReadBlocked, 'return_must_be_none'):
            self.execute(consume=lambda _: True)

    def test_callback_exception_has_spent_hash_Arrow_row_charges(self):
        def callback(_): raise RuntimeError('fixture consumer failure')
        with self.assertRaisesRegex(RuntimeError, 'consumer failure') as caught:self.execute(consume=callback)
        usage = caught.exception.clock_usage
        self.assertGreater(usage['hash_read_bytes'], 0)
        self.assertGreater(usage['arrow_read_bytes'], 0)
        self.assertEqual(usage['rows'], 1)

    def test_callback_source_mode_same_length_mutation_or_replacement_fails(self):
        for kind in ('body', 'mode', 'replace'):
            record = self.file(); path = Path(record['path'])
            def callback(_):
                if kind == 'mode': os.chmod(path, 0o600)
                elif kind == 'replace':
                    raw = path.read_bytes(); path.unlink(); path.write_bytes(raw)
                else:
                    raw = bytearray(path.read_bytes()); raw[12] ^= 1; path.write_bytes(raw)
            with self.subTest(kind=kind), self.assertRaisesRegex(m.ClockReadBlocked, 'source_identity_changed'):
                self.execute([record], callback)

    def test_callback_previous_or_future_selected_payload_mutation_fails(self):
        for target_index in (0, 1):
            first = self.file([self.row(10)], index=0)
            second = self.file([self.row(11)], index=1)
            records = [first, second]
            def callback(_): os.chmod(records[target_index]['path'], 0o600)
            with self.subTest(target=target_index), self.assertRaisesRegex(m.ClockReadBlocked, 'source_identity_changed'):
                self.execute(records, callback)

    def test_callback_parent_directory_alias_fails(self):
        directory = self.root/'real'; directory.mkdir()
        record = self.file(name='real/child.parquet')
        def callback(_):
            directory.rename(self.root/'moved'); directory.symlink_to(self.root/'moved', target_is_directory=True)
        with self.assertRaises(m.ClockReadBlocked): self.execute([record], callback)

    def test_callback_binding_or_selection_mutation_fails(self):
        for kind in ('root', 'record', 'selection'):
            record = self.file(); selection = [0]
            def callback(_):
                if kind == 'root': self.binding['sha256'] = 'b'*64
                elif kind == 'record': record['rows'] = True
                else: selection.append(7)
            with self.subTest(kind=kind), self.assertRaisesRegex(m.ClockReadBlocked, 'input_binding_changed'):
                self.execute([record], callback, selected=selection)
            self.binding['sha256'] = 'a'*64

    def test_first_clock_cannot_rebind_already_supplied_file_manifest(self):
        record = self.file(); calls = [0]
        def clock():
            if calls[0] == 0: record['sha256'] = '0'*64
            calls[0] += 1; return 0.0
        with self.assertRaisesRegex(m.ClockReadBlocked, 'input_binding_changed'):self.execute([record], clock=clock)

    def test_callback_and_final_clock_runtime_and_late_file_mutation_block(self):
        now = [0.0]
        def callback(_): now[0] = 31.0
        with self.assertRaisesRegex(m.ClockReadBlocked, 'runtime_limit'):
            self.execute(consume=callback, clock=lambda: now[0])
        record = self.file(); ended = [False]; original = m._Source.close
        def close(source): original(source); ended[0] = True
        def clock():
            if ended[0]: os.chmod(record['path'], 0o600)
            return 0.0
        with patch.object(m._Source, 'close', close), self.assertRaisesRegex(m.ClockReadBlocked, 'source_identity_changed'):
            self.execute([record], clock=clock)

    def test_last_report_clock_mutation_has_primitive_input_gate(self):
        record = self.file(); original = m._Meter.keep; report = [False]
        def keep(meter, name, value):
            if name == 'report': report[0] = True
            return original(meter, name, value)
        def clock():
            if report[0]: record['rows'] = True
            return 0.0
        with patch.object(m._Meter, 'keep', keep), self.assertRaisesRegex(m.ClockReadBlocked, 'input_binding_changed'):
            self.execute([record], clock=clock)

    def test_source_rows_read_batches_working_operations_and_file_caps(self):
        record = self.file([self.row(i) for i in range(1, 66)])
        for key, value in [('source_bytes', 1), ('rows', 64), ('read_bytes', record['bytes']),
                           ('batches', 1), ('working_bytes', 1), ('io_operations', 1)]:
            with self.subTest(key=key), self.assertRaises(m.ClockReadBlocked):
                self.execute([record], limits=self.limits(**{key: value}))
        second = self.file([self.row(66)], index=1)
        for key in ('files', 'file_refs'):
            with self.subTest(key=key), self.assertRaises(m.ClockReadBlocked):
                self.execute([record, second], limits=self.limits(**{key: 1}))

    def test_finite_fixture_limits_and_ranges_no_booleans_or_expansion(self):
        record = self.file()
        for changes in ({'rows': True}, {'rows': 0}, {'read_bytes': float('inf')},
                        {'source_bytes': 1024**2+1}, {'runtime_seconds': 31}):
            with self.subTest(changes=changes), self.assertRaises(m.ClockReadBlocked):
                self.execute([record], limits=self.limits(**changes))
        for lower, upper in ((0, 10), (True, 10), (10, 9), (1, 2**63)):
            with self.subTest(lower=lower, upper=upper), self.assertRaises(m.ClockReadBlocked):
                self.execute([record], lower=lower, upper=upper)
        for value in (float('nan'), float('inf'), True):
            with self.subTest(clock=value), self.assertRaises(m.ClockReadBlocked):
                self.execute([record], clock=lambda: value)

    def test_late_clock_buffer_growth_remeasured_after_read_charge(self):
        record = self.file(); original = m._Meter.add; armed = [False]
        def add(meter, key, count):
            if key == 'read_bytes': armed[0] = True
            return original(meter, key, count)
        def clock():
            if armed[0]: record['unexpected_buffer'] = 'X'*(2*1024**2)
            return 0.0
        with patch.object(m._Meter, 'add', add), self.assertRaises(m.ClockReadBlocked):
            self.execute([record], clock=clock, limits=self.limits(working_bytes=1024**2))

    def test_private_core_has_explicit_finite_limits_not_hidden_fixture_caps(self):
        record = self.file(); limits = self.limits(source_bytes=2*1024**2, rows=8192, runtime_seconds=60)
        frozen = m._freeze([record], [0], self.binding, 1, 20, m._limits(limits))
        result = m._stream_clocks(frozen, lambda _: None, meter=m._Meter(limits, lambda: 0.0))
        self.assertEqual(result['usage']['rows'], 1)
        self.assertFalse(result['production_admitted'])
        with self.assertRaisesRegex(m.ClockReadBlocked, 'fixture_limit_exceeded'):
            self.execute([record], limits=limits)

    def test_all_330_bindings_preserved_but_only_explicit_selected_file_opened(self):
        selected = self.file(index=329)
        records = [dict(path=str(self.root/f'not-opened-{i}.parquet'), bytes=1,
                        sha256='f'*64, rows=100000, source_file_index=i) for i in range(329)] + [selected]
        limits = self.limits(file_refs=330)
        frozen = m._freeze(records, [329], self.binding, 1, 20, m._limits(limits))
        opened = []; original = m.os.open
        def tracked(path, *args, **kwargs):
            opened.append(str(path)); return original(path, *args, **kwargs)
        with patch.object(m.os, 'open', side_effect=tracked):
            result = m._stream_clocks(frozen, lambda _: None, meter=m._Meter(limits, lambda: 0.0))
        self.assertEqual(opened, [selected['path']])
        self.assertEqual(result['all_file_bindings'], records)
        self.assertEqual(result['unselected_file_indices'], list(range(329)))
        self.assertEqual(result['unselected_rows_not_read_or_claimed'], 32900000)
        self.assertEqual(result['usage']['rows'], 1)
        self.assertEqual(result['usage']['file_refs'], 330)

    def test_complete_decoded_column_reservation_blocks_compressed_large_value(self):
        rows = [self.row(number) for number in range(1, 4097)]
        physical = [row | {'block_hash': bytes.fromhex(row['block_hash'][2:]),
                           'parent_hash': bytes.fromhex(row['parent_hash'][2:])} for row in rows]
        path = self.root/'compact-large.parquet'
        pq.write_table(pa.Table.from_pylist(physical, schema=m.header_schema()), path,
                       compression='zstd', row_group_size=4096)
        raw = path.read_bytes()
        record = {'path': str(path), 'bytes': len(raw), 'sha256': m.hashlib.sha256(raw).hexdigest(),
                  'rows': len(rows), 'source_file_index': 0}
        held = []; original = m._Meter.hold
        def hold(meter, name, count):
            held.append((name, count)); return original(meter, name, count)
        with patch.object(m._Meter, 'hold', hold), self.assertRaisesRegex(m.ClockReadBlocked, 'working_bytes_limit'):
            self.execute([record], lambda _: self.fail('decoded rows reached consumer'),
                         limits=self.limits(working_bytes=200000))
        self.assertTrue(any(name == 'arrow_decoded' and count > 200000 for name, count in held))

    def test_mutation_during_Arrow_read_is_caught_before_callback(self):
        record = self.file(); original = m._Source.read
        def read(source, size=-1):
            if source.phase == 'arrow':
                raw = bytearray(Path(record['path']).read_bytes()); raw[12] ^= 1
                Path(record['path']).write_bytes(raw)
            return original(source, size)
        with patch.object(m._Source, 'read', read), self.assertRaisesRegex(m.ClockReadBlocked, 'source_identity_changed'):
            self.execute([record], lambda _: self.fail('mutated row reached callback'))

    def test_same_fd_for_hash_and_Arrow_failed_reads_charged_and_seek_bound(self):
        record = self.file(); reads = []; original = m._Source.read
        def read(source, size=-1):
            reads.append((source.fd, source.phase)); return original(source, size)
        with patch.object(m._Source, 'read', read):self.execute([record])
        self.assertEqual(len({fd for fd, _ in reads}), 1)
        self.assertEqual({phase for _, phase in reads}, {'hash', 'arrow'})
        meter = m._Meter(self.limits(), lambda: 0.0)
        with m._Source(Path(record['path']), m._file_identity(Path(record['path'])), meter) as source:
            for args in ((-1, 0), (record['bytes']+1, 0), (0, 3), (True, 0)):
                with self.subTest(args=args), self.assertRaises(m.ClockReadBlocked):source.seek(*args)
            with patch.object(m.os, 'read', side_effect=OSError('fixture IO failure')):
                with self.assertRaises(OSError):source.read(3)
            self.assertEqual(meter.usage['hash_read_bytes'], 3)

    def test_no_discovery_full_table_pandas_or_writes_and_production_is_hardblocked(self):
        record = self.file()
        with patch.object(pq, 'read_table', side_effect=AssertionError('full table')), \
             patch.object(pq, 'write_table', side_effect=AssertionError('write')), \
             patch.object(Path, 'glob', side_effect=AssertionError('discovery')):
            result = self.execute([record])
        self.assertFalse(result['data_written'])
        with patch.object(m, 'require_production_host', side_effect=RuntimeError('host first')) as guard:
            with self.assertRaisesRegex(RuntimeError, 'host first'):m.run_production({'accepted': True})
        guard.assert_called_once_with()
        with patch.object(m, 'require_production_host', return_value=None), \
             patch.object(m.os, 'open', side_effect=AssertionError('IO before context')):
            with self.assertRaisesRegex(m.ClockReadBlocked, 'trusted_header_root_context'):
                m.run_production({'accepted': True})


# Original accepted footer only; not a payload or an external admission.
ACTUAL_FOOTER_BYTES = 6197
ACTUAL_FOOTER_SHA = '60b0bf31390d1b66a32b91c0851083f6b599be2983d8dae0a79483a3b6ed6c26'
ACTUAL_FOOTER_BASE64 = "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EBUCADwpBhkmAKCcAQAAACYAHBUOGTUABhAZGAtwYXJlbnRfaGFzaBUMFqCcARbcmykWopQpJt7xggcmkunbBhw2ACgg//JrBZX1C7gcc/MFCu/nrfJB+F5hJpaliBbbMICkYW0YIAAEklcuvnC1g7aiQebqLt6tauutlKDSL56/WB95s5MuEREAGSwVBBUAFQIAFQAVEBUCADwpBhkmAKCcAQAAACYAHBUEGTUABhAZGAl0aW1lc3RhbXAVDBagnAEWpPULFsiOBCbegIcHJrT9hAccGAgAiUBlAAAAABgIAKMqZQAAAAAWACgIAIlAZQAAAAAYCACjKmUAAAAAEREAGSwVBBUAFQIAFQAVEBUCADwpBhkmAKCcAQAAABaAomoWoJwBJtLprgYWqqJaABkcGAxBUlJPVzpzY2hlbWEYoAMvLy8vL3pBQkFBQVFBQUFBQUFBS0FBd0FCZ0FGQUFnQUNnQUFBQUFCQkFBTUFBQUFDQUFJQUFBQUJBQUlBQUFBQkFBQUFBUUFBQUM4QUFBQWNBQUFBRHdBQUFBRUFBQUFaUC8vL3dBQUFRSVFBQUFBSEFBQUFBUUFBQUFBQUFBQUNRQUFBSFJwYldWemRHRnRjQUFBQUZELy8vOEFBQUFCUUFBQUFKai8vLzhBQUFFUEVBQUFBQndBQUFBRUFBQUFBQUFBQUFzQUFBQndZWEpsYm5SZmFHRnphQURPLy8vL0lBQUFBTWovLy84QUFBRVBFQUFBQUNRQUFBQUVBQUFBQUFBQUFBb0FBQUJpYkc5amExOW9ZWE5vQUFBQUFBWUFDQUFFQUFZQUFBQWdBQUFBRUFBVUFBZ0FCZ0FIQUF3QUFBQVFBQkFBQUFBQUFBRUNFQUFBQUNnQUFBQUVBQUFBQUFBQUFBd0FBQUJpYkc5amExOXVkVzFpWlhJQUFBQUFDQUFNQUFnQUJ3QUlBQUFBQUFBQUFVQUFBQUFBQUFBQQAYIHBhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gMjMuMC4xGUwcAAAcAAAcAAAcAAAAKRgAAFBBUjE="


class ExactCampaignFormatTests(unittest.TestCase):
    def test_actual_original_footer_has_exact_binary32_schema_and_100000_rows(self):
        raw = base64.b64decode(ACTUAL_FOOTER_BASE64, validate=True)
        self.assertEqual((len(raw), hashlib.sha256(raw).hexdigest()), (ACTUAL_FOOTER_BYTES, ACTUAL_FOOTER_SHA))
        metadata = pq.read_metadata(pa.BufferReader(raw))
        self.assertEqual(metadata.num_rows, 100000)
        self.assertEqual(metadata.num_row_groups, 10)
        schema = metadata.schema.to_arrow_schema()
        self.assertTrue(schema.equals(m.header_schema(), check_metadata=True))
        self.assertFalse(schema.equals(old.header_schema(), check_metadata=True))
        self.assertIsNone(schema.metadata)
        for field in schema:
            self.assertTrue(field.nullable)
            self.assertIsNone(field.metadata)
        for group in range(metadata.num_row_groups):
            for column in range(4):
                self.assertEqual(metadata.row_group(group).column(column).compression, 'ZSTD')

    def test_binary32_normalizes_all_byte_values_without_input_alias_or_case_choice(self):
        value = {'block_number': 10, 'block_hash': bytes(range(32)),
                 'parent_hash': b'\xff'*32, 'timestamp': 1700000010}
        before = deepcopy(value); normalized = m._canonical_header(value)
        self.assertEqual(normalized, value | {'block_hash': '0x'+bytes(range(32)).hex(),
            'parent_hash': '0x'+'ff'*32})
        self.assertEqual(m._header(normalized, None), tuple(normalized[key] for key in m.FIELDS))
        normalized['block_hash'] = 'changed'
        self.assertEqual(value, before)

    def test_binary_adapter_rejects_null_string_wrong_width_mutable_or_extra_values(self):
        row = {'block_number': 10, 'block_hash': b'\x01'*32,
               'parent_hash': b'\x02'*32, 'timestamp': 1700000010}
        for key in ('block_hash', 'parent_hash'):
            for value in (None, '0x'+'01'*32, b'', b'x'*31, b'x'*33, bytearray(32), memoryview(bytes(32)), True):
                with self.subTest(key=key, kind=type(value).__name__), self.assertRaisesRegex(
                        m.ClockReadBlocked, 'exact_binary32'):
                    m._canonical_header(row | {key: value})
        for value in (row | {'extra': 1}, dict(reversed(list(row.items()))), [], None):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(m.ClockReadBlocked):
                m._canonical_header(value)

    def test_exact_physical_schema_rejects_strings_variable_binary_wrong_width_and_nulls(self):
        with tempfile.TemporaryDirectory(prefix='append_clock_binary_schema_') as temp:
            root = Path(temp).resolve()
            binding = {'path': str(root/'manifest.json'), 'bytes': 1, 'sha256': 'a'*64}
            for kind in (pa.string(), pa.binary(), pa.binary(31), pa.binary(33), pa.null()):
                with self.subTest(kind=kind):
                    schema = m.header_schema().set(1, pa.field('block_hash', kind))
                    val = None if pa.types.is_null(kind) else ('0x'+'01'*32 if pa.types.is_string(kind)
                        else b'x'*(kind.byte_width if pa.types.is_fixed_size_binary(kind) else 32))
                    row = {'block_number': 10, 'block_hash': val, 'parent_hash': b'y'*32, 'timestamp': 1700000010}
                    path = root/'wrong.parquet'; pq.write_table(pa.Table.from_pylist([row], schema=schema),
                        path, compression='zstd')
                    raw = path.read_bytes()
                    record = {'path': str(path), 'bytes': len(raw), 'sha256': m.hashlib.sha256(raw).hexdigest(),
                              'rows': 1, 'source_file_index': 0}
                    with self.assertRaisesRegex(m.ClockReadBlocked, 'exact_header_arrow_schema'):
                        m.stream_fixture([record], [0], lambda _: self.fail('bad schema callback'),
                            root_binding=binding, lower=1, upper=20, fixture_root=root)

    def test_entire_source_AST_is_only_version_exact_schema_and_binary_normalization(self):
        prior = Path(old.__file__).read_text()
        current = Path(m.__file__).read_text()
        expected = prior.replace("SCHEMA = 'polymarket_append_stored_header_clock_reader_v1'",
                                 "SCHEMA = 'polymarket_append_stored_header_clock_reader_v2'")
        expected = expected.replace("('block_hash', pa.string())", "('block_hash', pa.binary(32))")
        expected = expected.replace("('parent_hash', pa.string())", "('parent_hash', pa.binary(32))")
        expected = expected.replace("previous = _header(row, previous)",
                                    "previous = _header(_canonical_header(row), previous)")
        tree = ast.parse(current)
        tree.body = [node for node in tree.body if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                     or node.name != '_canonical_header']
        old_tree = ast.parse(expected)
        tree.body = tree.body[1:]; old_tree.body = old_tree.body[1:]
        self.assertEqual(ast.dump(tree, include_attributes=False), ast.dump(old_tree, include_attributes=False))



if __name__ == '__main__':
    unittest.main()
