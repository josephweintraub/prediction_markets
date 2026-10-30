"""Tiny temporary Parquet fixtures only; no accepted inputs or network."""
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import stream_polymarket_append_native_rows_v2 as m
from tests import test_decode_polymarket_append_legacy_v2_v2 as bridge_fixtures


class PhysicalStreamTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='append_stream_fixture_')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.make_row = bridge_fixtures.NativeBridgeTests().row

    def file(self, rows=None, *, name='rows.parquet', index=7, group='append_campaign_v2', schema=None):
        rows = [self.make_row()] if rows is None else rows
        path = self.root / name
        pq.write_table(pa.Table.from_pylist(rows, schema=m.native_schema() if schema is None else schema),
                       path, compression='zstd', row_group_size=41)
        raw = path.read_bytes()
        return {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                'rows': len(rows), 'source_file_index': index, 'source_group': group}

    def execute(self, records=None, consume=None, **kwargs):
        if records is None: records = [self.file()]
        if consume is None: consume = lambda row, locator: None
        return m.stream_fixture(records, consume, fixture_root=self.root, **kwargs)

    def limits(self, **changes): return dict(m.FIXTURE_MAX) | changes

    def test_exact_collector_schema_seventeen_fields_uint64_not_probe_uint8(self):
        schema = m.native_schema()
        self.assertEqual(tuple(schema.names), m.FIELDS)
        self.assertEqual(len(schema), 17)
        self.assertEqual(schema.field('source_request_id').type, pa.uint64())
        self.assertEqual(schema.field('topics').type, pa.list_(pa.string()))
        for group in m.GROUPS:
            record = self.file(name=group + '.parquet', group=group)
            result = self.execute([record]); self.assertEqual(result['usage']['rows'], 1)

    def test_order_and_zero_based_ordinals_across_batches_files_and_row_groups(self):
        rows = [self.make_row(i % 4) | {'source_request_id': i} for i in range(131)]
        first = self.file(rows, name='z.parquet', index=91)
        second = self.file(rows[:2], name='a.parquet', index=2, group='append_probe_v2')
        observed = []
        result = self.execute([first, second], lambda row, locator: observed.append((row, locator)))
        self.assertEqual([row for row, _ in observed], rows + rows[:2])
        self.assertEqual([loc['physical_row_ordinal'] for _, loc in observed], list(range(131)) + [0, 1])
        self.assertEqual([loc['source_file_index'] for _, loc in observed], [91] * 131 + [2] * 2)
        self.assertEqual(result['usage']['rows'], 133)
        self.assertEqual(result['usage']['batches'], 4)
        self.assertEqual(result['usage']['files'], 2)
        self.assertEqual(result['usage']['hash_read_bytes'], first['bytes'] + second['bytes'])
        self.assertGreater(result['usage']['arrow_read_bytes'], 0)
        self.assertEqual(result['usage']['read_bytes'], result['usage']['hash_read_bytes'] + result['usage']['arrow_read_bytes'])
        self.assertEqual(result['file_counts'], [first, second])
        self.assertEqual(result['source_identities'][first['path']]['bytes'], first['bytes'])
        self.assertNotIn('atime_ns', result['source_identities'][first['path']])

    def test_empty_file_is_counted_with_no_callback_no_fabricated_row(self):
        result = self.execute([self.file([])], lambda *_: self.fail('empty callback'))
        self.assertEqual((result['usage']['rows'], result['usage']['files']), (0, 1))

    def test_raw_case_and_provider_timestamp_preserved_without_clock_guess(self):
        row = self.make_row(2)
        row.update(transactionHash='0x' + 'AB' * 32, blockTimestamp='opaque provider string',
                   provider_block_timestamp_present=True)
        row['topics'][1] = '0x' + 'CD' * 32
        observed = []
        result = self.execute([self.file([row])], lambda value, _: observed.append(value))
        self.assertEqual(observed, [row]); self.assertNotIn('timestamp', observed[0])
        self.assertFalse(result['execution_clock_verified'])
        self.assertFalse(result['provider_timestamp_used_as_execution_clock'])

    def test_full_body_corruption_with_same_valid_footer_rejected_before_arrow_or_callback(self):
        record = self.file(); path = Path(record['path']); raw = path.read_bytes()
        footer_length = int.from_bytes(raw[-8:-4], 'little'); changed = bytearray(raw)
        self.assertGreater(len(raw) - footer_length - 8, 16)
        changed[12] ^= 1; path.write_bytes(changed)
        self.assertEqual(raw[-footer_length-8:], changed[-footer_length-8:])
        self.assertEqual(pq.read_metadata(pa.BufferReader(changed)).num_rows, 1)
        with patch.object(pq, 'ParquetFile', side_effect=AssertionError('Arrow before full hash')):
            with self.assertRaisesRegex(m.StreamBlocked, 'full_source_sha256_differs'):
                self.execute([record], lambda *_: self.fail('unverified row'))

    def test_length_wrong_hash_and_strict_pin_types(self):
        record = self.file()
        for changes in ({'bytes': record['bytes'] + 1}, {'bytes': True}, {'rows': True},
                        {'rows': -1}, {'sha256': 'A' * 64}, {'sha256': '0' * 64},
                        {'source_file_index': True}, {'source_group': 'legacy_history'},
                        {'authenticated': True}):
            with self.subTest(changes=changes), self.assertRaises(m.StreamBlocked):
                self.execute([record | changes], lambda *_: self.fail('bad pin row'))

    def test_missing_file_rejected_before_emission(self):
        record = self.file(); Path(record['path']).unlink()
        with self.assertRaisesRegex(m.StreamBlocked, 'missing_or_unreadable'):
            self.execute([record])

    def test_duplicate_paths_inodes_and_file_indices_rejected_before_any_callback(self):
        record = self.file(); alias = self.root / 'alias.parquet'; os.link(record['path'], alias)
        second = self.file(name='second.parquet')
        for records in ([record, record], [record, record | {'path': str(alias), 'source_file_index': 8}],
                        [record, second]):
            with self.subTest(records=records), self.assertRaisesRegex(m.StreamBlocked, 'duplicate_path_inode_or_file_index'):
                self.execute(records, lambda *_: self.fail('duplicate callback'))

    def test_file_and_parent_symlinks_and_nonregular_source_rejected(self):
        record = self.file(); alias = self.root / 'link.parquet'; alias.symlink_to(record['path'])
        nested = self.root / 'real'; nested.mkdir(); child = self.file(name='real/child.parquet')
        linkdir = self.root / 'linkdir'; linkdir.symlink_to(nested, target_is_directory=True)
        for rec in (record | {'path': str(alias)}, child | {'path': str(linkdir / 'child.parquet')},
                    record | {'path': str(nested)}):
            with self.subTest(rec=rec), self.assertRaises(m.StreamBlocked): self.execute([rec])

    def test_literal_paths_no_parent_segments_controls_or_outside_fixture(self):
        record = self.file()
        for value in ('relative.parquet', str(self.root / '..' / 'rows.parquet'), record['path'] + '\x00',
                      record['path'] + '\n', '/mnt/data/actual.parquet', str(self.root / '.' / 'rows.parquet') + '/'):
            with self.subTest(path=value), self.assertRaises(m.StreamBlocked): self.execute([record | {'path': value}])

    def test_schema_missing_extra_reordered_wrong_unsigned_width_and_metadata_rejected(self):
        expected = m.native_schema()
        alternatives = [pa.schema(list(expected)[:-1]), pa.schema(list(expected) + [pa.field('extra', pa.string())]),
                        pa.schema(list(expected)[::-1]), expected.set(15, pa.field('source_request_id', pa.uint8())),
                        expected.set(10, pa.field('block_number', pa.int64())), expected.with_metadata({'untrusted': 'yes'})]
        for i, schema in enumerate(alternatives):
            record = self.file([self.make_row() | {'source_request_id': 3}], name=f'wrong{i}.parquet', schema=schema)
            with self.subTest(i=i), self.assertRaisesRegex(m.StreamBlocked, 'exact_native_arrow_schema_differs'):
                self.execute([record], lambda *_: self.fail('schema callback'))

    def test_exact_footer_and_stream_row_counts(self):
        record = self.file()
        for rows in (0, 2):
            with self.subTest(rows=rows), self.assertRaisesRegex(m.StreamBlocked, 'manifest_footer_row_count_differs'):
                self.execute([record | {'rows': rows}], lambda *_: self.fail('row mismatch callback'))

    def test_null_raw_values_are_preserved_for_downstream_strict_wire_quarantine(self):
        row = self.make_row() | {'address': None, 'topics': None, 'removed': None}
        observed = []; self.execute([self.file([row])], lambda value, _: observed.append(value))
        self.assertEqual(observed, [row])  # Type schema, not silent ABI normalization.

    def test_identity_owner_mode_inode_size_mtime_and_ctime_all_bound_no_atime(self):
        record = self.file(); identity = m._file_identity(Path(record['path']))
        self.assertEqual(len(identity), 9); self.assertEqual(identity[4:6], (os.getuid(), os.getgid()))
        before = Path(record['path']).stat()
        os.utime(record['path'], ns=(before.st_atime_ns + 1000000000, before.st_mtime_ns))
        # atime itself is absent; this metadata operation still changes ctime.
        self.assertEqual(m._file_identity(Path(record['path']))[7], identity[7])
        self.assertNotIn('atime', m._stat.__code__.co_names)

    def test_callback_same_length_source_mutation_withholds_completion(self):
        record = self.file([self.make_row()] * 2); path = Path(record['path'])
        def mutate(*_):
            raw = bytearray(path.read_bytes()); raw[12] ^= 1; path.write_bytes(raw)
        with self.assertRaisesRegex(m.StreamBlocked, 'source_identity_changed'):
            self.execute([record], mutate)

    def test_callback_other_future_or_already_read_file_mutation_withholds_completion(self):
        first = self.file(name='first.parquet', index=1); second = self.file(name='second.parquet', index=2)
        for target in (first, second):
            records = [self.file(name='first.parquet', index=1), self.file(name='second.parquet', index=2)]
            def mutate(*_): os.chmod(target['path'], 0o600)
            with self.subTest(target=target), self.assertRaisesRegex(m.StreamBlocked, 'source_identity_changed'):
                self.execute(records, mutate)

    def test_callback_file_replacement_and_parent_alias_rejected(self):
        nested = self.root / 'nested'; nested.mkdir()
        record = self.file(name='nested/rows.parquet')
        def replace(*_):
            nested.rename(self.root / 'moved'); nested.symlink_to(self.root / 'moved', target_is_directory=True)
        with self.assertRaisesRegex(m.StreamBlocked, 'ancestor_not_literal_directory'):
            self.execute([record], replace)

    def test_callback_exception_propagates_without_success(self):
        def fail(*_): raise RuntimeError('consumer blocked')
        with self.assertRaisesRegex(RuntimeError, 'consumer blocked') as caught: self.execute(consume=fail)
        self.assertEqual(caught.exception.stream_usage['rows'], 1)
        self.assertGreater(caught.exception.stream_usage['hash_read_bytes'], 0)
        self.assertGreater(caught.exception.stream_usage['arrow_read_bytes'], 0)

    def test_manifest_mutation_by_callback_cannot_change_captured_locators_or_counts(self):
        record = self.file([self.make_row()] * 2); observed = []
        def mutate(row, locator):
            record['source_file_index'] = 999; record['rows'] = 999
            observed.append(locator)
        result = self.execute([record], mutate)
        self.assertEqual([x['source_file_index'] for x in observed], [7, 7])
        self.assertEqual(result['file_counts'][0]['rows'], 2)

    def test_rows_source_bytes_batches_working_read_and_operation_caps(self):
        record = self.file([self.make_row()] * 65)
        for key, value in (('source_bytes', 1), ('read_bytes', record['bytes']), ('rows', 64),
                           ('batches', 1), ('working_bytes', 1), ('io_operations', 1)):
            with self.subTest(key=key), self.assertRaises(m.StreamBlocked):
                self.execute([record], limits=self.limits(**{key: value}))

    def test_finite_limits_strict_no_bool_zero_unbounded_or_larger_profile(self):
        record = self.file()
        for changes in ({'runtime_seconds': True}, {'read_bytes': 0}, {'rows': -1},
                        {'source_bytes': m.FIXTURE_MAX['source_bytes'] + 1}, {'runtime_seconds': float('inf')}):
            with self.subTest(changes=changes), self.assertRaises(m.StreamBlocked):
                self.execute([record], limits=self.limits(**changes))

    def test_callback_runtime_expiry_cannot_return_completion(self):
        now = [0.0]
        def callback(*_): now[0] = 31.0
        with self.assertRaisesRegex(m.StreamBlocked, 'stream_runtime_limit'):
            self.execute(consume=callback, clock=lambda: now[0])

    def test_last_clock_callback_late_mutation_caught_by_primitive_final_gate(self):
        record = self.file(); path = Path(record['path']); ended = [False]
        original_close = m._Source.close
        def close(source):
            original_close(source)
            ended[0] = True
        def clock():
            if ended[0]:
                raw = bytearray(path.read_bytes()); raw[12] ^= 1; path.write_bytes(raw)
            return 0.0
        with patch.object(m._Source, 'close', close), self.assertRaisesRegex(m.StreamBlocked, 'source_identity_changed'):
            self.execute([record], clock=clock)

    def test_arrow_reads_and_seeks_all_use_same_metered_fd(self):
        record = self.file(); phase_reads = []; phase_seeks = []
        original_read, original_seek = m._Source.read, m._Source.seek
        def read(source, size=-1):
            phase_reads.append((source.phase, source.fd, size))
            return original_read(source, size)
        def seek(source, offset, whence=0):
            phase_seeks.append((source.phase, source.fd, offset, whence))
            return original_seek(source, offset, whence)
        with patch.object(m._Source, 'read', read), patch.object(m._Source, 'seek', seek):
            result = self.execute([record])
        self.assertEqual({phase for phase, _, _ in phase_reads}, {'hash', 'arrow'})
        self.assertEqual(len({fd for _, fd, _ in phase_reads}), 1)
        self.assertTrue(any(phase == 'arrow' for phase, *_ in phase_seeks))
        self.assertEqual(result['usage']['hash_read_bytes'], record['bytes'])

    def test_same_length_mutation_during_arrow_read_is_detected_before_callback(self):
        record = self.file(); path = Path(record['path']); original_read = m._Source.read
        def read(source, size=-1):
            if source.phase == 'arrow':
                raw = bytearray(path.read_bytes()); raw[12] ^= 1; path.write_bytes(raw)
            return original_read(source, size)
        with patch.object(m._Source, 'read', read), self.assertRaisesRegex(m.StreamBlocked, 'source_identity_changed'):
            self.execute([record], lambda *_: self.fail('changed row'))

    def test_row_group_uncompressed_all_nested_columns_bound_before_callback(self):
        row = self.make_row() | {'data': 'A' * 200000}
        record = self.file([row])
        with self.assertRaisesRegex(m.StreamBlocked, 'row_group_uncompressed_working_bound'):
            self.execute([record], lambda *_: self.fail('large row'), limits=self.limits(working_bytes=100000))

    def test_external_parquet_column_paths_forbidden_before_callback(self):
        record = self.file()
        class Column:
            file_path = 'outside.parquet'; total_uncompressed_size = 1
        class Group:
            def column(self, _): return Column()
        class Metadata:
            num_rows = 1; num_row_groups = 1; num_columns = 17
            def row_group(self, _): return Group()
        class Parquet:
            schema_arrow = m.native_schema(); metadata = Metadata()
            def iter_batches(self, **_): raise AssertionError('external iterator')
        with patch.object(pq, 'ParquetFile', return_value=Parquet()), \
             self.assertRaisesRegex(m.StreamBlocked, 'external_column_file_forbidden'):
            self.execute([record], lambda *_: self.fail('external row'))

    def test_real_current_collector_serializer_matches_reader_schema_without_coercion(self):
        from scripts import collect_polymarket_native_increment as collector
        row = self.make_row(2); raw = collector.parquet_payload([row])
        path = self.root / 'collector.parquet'; path.write_bytes(raw)
        record = {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
                  'rows': 1, 'source_file_index': 0, 'source_group': 'append_campaign_v2'}
        observed = []; result = self.execute([record], lambda value, _: observed.append(value))
        self.assertEqual(observed, [row]); self.assertEqual(result['usage']['rows'], 1)

    def test_no_arrow_read_table_pandas_write_or_default_discovery(self):
        record = self.file()
        with patch.object(pq, 'read_table', side_effect=AssertionError('full table')), \
             patch.object(pq, 'write_table', side_effect=AssertionError('write')):
            result = self.execute([record])
        self.assertFalse(result['data_written']); self.assertFalse(result['production_admitted'])
        for records in ([], (), None):
            with self.subTest(records=records), self.assertRaises(m.StreamBlocked):
                m.stream_fixture(records, lambda *_: None, fixture_root=self.root)

    def test_raw_reader_rejects_outside_seeks_and_charges_failed_reads_before_io(self):
        record = self.file(); path = Path(record['path']); meter = m.Meter(self.limits(), lambda: 0.0)
        with m._Source(path, m._file_identity(path), meter) as source:
            for args in ((-1, 0), (record['bytes'] + 1, 0), (0, 3), (True, 0)):
                with self.subTest(args=args), self.assertRaises(m.StreamBlocked): source.seek(*args)
            with patch.object(m.os, 'read', side_effect=OSError('failed read')):
                with self.assertRaises(OSError): source.read(3)
            self.assertEqual(meter.usage['hash_read_bytes'], 3)

    def test_production_guard_then_hardblock_before_source_input_io(self):
        with patch.object(m, 'require_production_host', side_effect=RuntimeError('host')):
            with self.assertRaisesRegex(RuntimeError, 'host'): m.run_production(authenticated=True)
        with patch.object(m, 'require_production_host'), patch.object(m.os, 'open', side_effect=AssertionError('read')):
            with self.assertRaisesRegex(m.StreamBlocked, 'production_driver_not_admitted'):
                m.run_production({'authenticated': True})

    def test_four_file_old_peak_omits_simultaneous_capture_hash_and_manifest(self):
        from scripts import stream_polymarket_append_native_rows_v1 as old
        records = [self.file(name=f'{i}.parquet', index=i) for i in range(4)]
        old_result = old.stream_fixture(records, lambda *_: None, fixture_root=self.root)
        old_peak = old_result['usage']['peak_working_bytes']
        captured_bytes = m._python_bytes(m._records(records, self.root))
        self.assertGreater(captured_bytes + min(m.HASH_CHUNK, records[0]['bytes']), old_peak)
        with self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'):
            self.execute(records, limits=self.limits(working_bytes=old_peak))
        result = self.execute(records)
        self.assertGreaterEqual(result['usage']['peak_working_bytes'],
                                captured_bytes + records[0]['bytes'] + m._python_bytes(records))

    def test_source_hash_read_adds_retained_components_before_allocating(self):
        record = self.file(); meter = m.Meter(self.limits(working_bytes=20201), lambda: 0.0)
        retained = b'x' * 16156
        meter.keep('captured_fixture', retained)
        with m._Source(Path(record['path']), m._file_identity(Path(record['path'])), meter) as source, \
             patch.object(m.os, 'read', side_effect=AssertionError('over-budget allocation')):
            with self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'):
                source.read(record['bytes'])

    def test_reader_keeps_arrow_compressed_decoded_batch_rows_and_locator_together(self):
        record = self.file([self.make_row()] * 2); observations = []
        original = m.Meter.working
        def working(meter, count):
            original(meter, count)
            if 'locator' in meter.retained:
                observations.append((dict(meter.live), set(meter.retained), meter.usage['peak_working_bytes']))
        with patch.object(m.Meter, 'working', working): self.execute([record])
        self.assertTrue(observations)
        for live, retained, _ in observations:
            self.assertTrue({'arrow_compressed', 'arrow_decoded', 'batch'} <= set(live))
            self.assertTrue({'input_manifest', 'captured', 'python_rows', 'locator'} <= retained)
            self.assertEqual(live['arrow_compressed'], 2 * record['bytes'])

    def test_callback_growth_of_rows_locator_or_input_manifest_is_counted(self):
        for target in ('row', 'locator', 'manifest'):
            record = self.file(); records = [record]
            def mutate(row, locator):
                value = 'x' * 100000
                if target == 'row': row['data'] = value
                elif target == 'locator': locator['extra'] = value
                else: records.append({'untrusted_callback_extra': value})
            with self.subTest(target=target), self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'):
                self.execute(records, mutate, limits=self.limits(working_bytes=90000))

    def test_complete_report_reservation_checked_before_materialization(self):
        record = self.file(); original = m.Meter.hold; reached = []
        def hold(meter, name, count):
            if name == 'report':
                reached.append(True)
                meter.limits['working_bytes'] = sum(meter.live.values()) + sum(
                    m._python_bytes(x) for x in meter.retained.values()) + count - 1
            return original(meter, name, count)
        with patch.object(m.Meter, 'hold', hold), self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'):
            self.execute([record])
        self.assertEqual(reached, [True])

    def test_last_clock_growth_is_remeasured_without_any_later_callback(self):
        record = self.file(); records = [record]; ready = [False]
        original_keep = m.Meter.keep
        def keep(meter, name, value):
            original_keep(meter, name, value)
            if name == 'report': ready[0] = True
        def clock():
            if ready[0] and len(records) == 1:
                records.append({'late': 'x' * 200000})
            return 0.0
        with patch.object(m.Meter, 'keep', keep), self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'):
            self.execute(records, clock=clock, limits=self.limits(working_bytes=100000))

    def test_callback_cycles_block_instead_of_hiding_or_recursing_unbounded(self):
        def mutate(row, _): row['cycle'] = row
        with self.assertRaisesRegex(m.StreamBlocked, 'cyclic_materialized_value'):
            self.execute(consume=mutate)

    def test_read_charge_callback_precedes_last_aggregate_allocation_gate(self):
        record = self.file(); retained = []; meter = m.Meter(self.limits(working_bytes=50000), lambda: 0.0)
        meter.keep('manifest', retained)
        original = meter.read
        def read_charge(count, phase):
            original(count, phase)
            retained.append('x' * 50000)
        meter.read = read_charge
        with m._Source(Path(record['path']), m._file_identity(Path(record['path'])), meter) as source, \
             patch.object(m.os, 'read', side_effect=AssertionError('allocation after late growth')):
            with self.assertRaisesRegex(m.StreamBlocked, 'stream_working_bytes_limit'): source.read(3)
        self.assertEqual(meter.usage['hash_read_bytes'], 3)


if __name__ == '__main__': unittest.main()

