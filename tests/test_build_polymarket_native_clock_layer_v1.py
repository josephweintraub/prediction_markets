"""Small synthetic native-clock fixtures; no production files or APIs."""
from copy import deepcopy
from contextlib import redirect_stdout
import io
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from scripts import build_polymarket_native_clock_layer_v1 as m


def h(number):
    return '0x'+format(number, '064x')


def header(number, timestamp):
    return {'block_number': number, 'block_hash': h(number), 'parent_hash': h(number-1), 'timestamp': timestamp}


def endpoint():
    return {'end_exclusive_utc': m.END_EXCLUSIVE_UTC,
        'last_included': header(100, m.CUTOFF_SECONDS-1), 'first_excluded': header(101, m.CUTOFF_SECONDS)}


def headers():
    return [header(1, 1), header(2, m.CUTOFF_SECONDS-86401), header(3, m.CUTOFF_SECONDS-86400)]


def binding(index=0, role='legacy_raw'):
    value = {'path': f'/mnt/data/synthetic_fixture/source{index}.parquet', 'source_role': role, 'bytes': 100,
        'identity': {'device': 1, 'inode': index+1, 'mode': stat.S_IFREG | 0o600,
            'uid': 1, 'gid': 1, 'mtime_ns': 1, 'ctime_ns': 1}, 'footer_sha256': 'a'*64}
    if role == 'native_raw':
        value['payload_sha256'] = 'b'*64
    return value


def legacy(block=1, log=0):
    return {'order_hash': h(55), 'maker': '0x'+'1'*40, 'taker': '0x'+'2'*40,
        'maker_asset_id': '0', 'taker_asset_id': '123', 'maker_amount_filled': 100,
        'taker_amount_filled': 50, 'fee': 1, 'block_number': block,
        'transaction_hash': h(77), 'log_index': log, 'exchange_address': '0x'+'3'*40}


def native(block=2, log=0):
    return {'address': '0x'+'3'*40, 'blockHash': h(block), 'blockNumber': hex(block),
        'data': '0x'+'0'*320, 'logIndex': hex(log), 'removed': False,
        'topics': [h(10), h(55), h(11), h(12)], 'transactionHash': h(77),
        'transactionIndex': '0x0', 'blockTimestamp': 'provider-only-not-an-execution-clock',
        'block_number': block, 'log_index': log, 'transaction_index': 0,
        'exchange_address': '0x'+'3'*40, 'window': 'synthetic-window', 'source_request_id': 1,
        'provider_block_timestamp_present': True}


def source(rows, index=0, role='legacy_raw', groups=None):
    return {'binding': binding(index, role), 'rows_declared': len(rows),
        'row_groups': [deepcopy(rows)] if groups is None else deepcopy(groups)}


class PhysicalAndClockTests(unittest.TestCase):
    def test_zero_based_physical_ordinals_span_row_groups_without_deduplication(self):
        row = legacy(); sources = [source([row, row], groups=[[row], [], [row]])]
        before = deepcopy(sources)
        result = m.build_fixture_layer(sources, headers(), endpoint())
        self.assertEqual(sources, before)
        self.assertEqual([row['physical_row_ordinal'] for row in result['records']], [0, 1])
        self.assertEqual([row['source_file_index'] for row in result['records']], [0, 0])
        self.assertEqual(result['physical_rows'], 2)
        self.assertEqual(result['native_relation_rows'], {'identical_observed_replay': 2})
        self.assertEqual(result['physical_rows_dropped'], 0)

    def test_skinny_records_reach_exact_original_source_without_payload_copy(self):
        result = m.build_fixture_layer([source([legacy()])], headers(), endpoint())
        row = result['records'][0]
        self.assertEqual(result['source_bindings'][row['source_file_index']], binding())
        self.assertFalse({'maker', 'taker', 'fee', 'data', 'topics', 'maker_amount_filled'} & set(row))
        self.assertEqual(row['physical_row_payload_sha256'], m.digest(m.encode(legacy())))
        self.assertFalse(result['production_bindings_verified'])
        self.assertFalse(result['archive_or_sample_admitted'])

    def test_legacy_missing_hash_is_not_relabelled_as_native_observed_hash(self):
        row = m.build_fixture_layer([source([legacy()])], headers(), endpoint())['records'][0]
        self.assertIsNone(row['native_observed_block_hash'])
        self.assertEqual(row['header_joined_block_hash'], h(1))
        self.assertEqual(row['hash_qualification'], 'joined_header_only_no_native_hash_assertion')

    def test_native_hash_matches_header_and_provider_timestamp_is_never_used(self):
        result = m.build_fixture_layer([source([native()], role='native_raw')], headers(), endpoint())
        row = result['records'][0]
        self.assertEqual(row['native_observed_block_hash'], row['header_joined_block_hash'])
        self.assertEqual(row['timestamp_UTC_seconds'], m.CUTOFF_SECONDS-86401)
        self.assertEqual(row['UTC_month'], '2026-09')
        self.assertFalse(result['provider_timestamp_used_as_execution_clock'])

    def test_exact_cutoff_buckets_and_UTC_months_keep_every_physical_row(self):
        rows = [legacy(1, 1), legacy(3, 2), legacy(100, 3), legacy(101, 4)]
        result = m.build_fixture_layer([source(rows)], headers(), endpoint())
        self.assertEqual(result['physical_rows'], 4)
        self.assertEqual(result['UTC_month_rows'], {'1970-01': 1, '2026-10': 3})
        self.assertEqual(result['cutoff_bucket_rows'], {'at_or_after_exclusive_cutoff': 1, 'before_exclusive_cutoff': 3})
        self.assertEqual(result['records'][-1]['timestamp_UTC_seconds'], m.CUTOFF_SECONDS)

    def test_empty_source_file_is_conserved_in_manifest_without_synthetic_rows(self):
        result = m.build_fixture_layer([source([], 0), source([legacy()], 1)], headers(), endpoint())
        self.assertEqual(len(result['source_bindings']), 2)
        self.assertEqual(result['physical_rows'], 1)
        self.assertEqual(result['records'][0]['source_file_index'], 1)

    def test_missing_hash_mismatch_duplicate_header_and_clock_errors_fail_closed(self):
        scenarios = [([header(2, 2)], [source([legacy()])], 'missing_required_header_no_fallback'),
            (headers()+[headers()[0]], [source([legacy()])], 'duplicate_header_key_join_fanout')]
        bad = native(); bad['blockHash'] = h(999)
        scenarios.append((headers(), [source([bad], role='native_raw')], 'native_observed_joined_hash_mismatch'))
        for cache, sources, error in scenarios:
            with self.subTest(error=error):
                with self.assertRaisesRegex(m.Blocked, error): m.build_fixture_layer(sources, cache, endpoint())
        cache = headers(); cache[1]['timestamp'] = 0
        with self.assertRaisesRegex(m.Blocked, 'header_integer_UTC_timestamp'):
            m.build_fixture_layer([source([legacy()])], cache, endpoint())
        cache = headers(); cache[2]['timestamp'] = 1
        with self.assertRaisesRegex(m.Blocked, 'nonmonotone_or_parent_mismatched_header_clock'):
            m.build_fixture_layer([source([legacy()])], cache, endpoint())

    def test_integer_header_and_block_types_exclude_booleans_floats_and_invalids(self):
        for value in (True, 1.5, 0, -1, m.MAX_TIMESTAMP+1):
            with self.subTest(timestamp=value):
                cache = headers(); cache[0]['timestamp'] = value
                with self.assertRaises(m.Blocked): m.build_fixture_layer([source([legacy()])], cache, endpoint())
        for value in (True, 1.5, None, 0, -1, 2**63):
            with self.subTest(block=value):
                row = legacy(); row['block_number'] = value
                with self.assertRaises(m.Blocked): m.build_fixture_layer([source([row])], headers(), endpoint())

    def test_exact_endpoint_bracket_and_boundary_bindings_cannot_be_approximated(self):
        for change in ('wrong_cutoff', 'not_adjacent', 'wrong_parent', 'bad_timestamp'):
            value = endpoint()
            if change == 'wrong_cutoff': value['end_exclusive_utc'] = '2026-10-01T00:00:00Z'
            if change == 'not_adjacent': value['first_excluded']['block_number'] += 1
            if change == 'wrong_parent': value['first_excluded']['parent_hash'] = h(2)
            if change == 'bad_timestamp': value['last_included']['timestamp'] = m.CUTOFF_SECONDS
            with self.subTest(change=change):
                with self.assertRaises(m.Blocked): m.build_fixture_layer([source([legacy()])], headers(), value)


class IdentityAndRepresentationTests(unittest.TestCase):
    def test_missing_legacy_identity_components_retain_rows_with_explicit_qualification(self):
        rows = []
        for field in ('exchange_address', 'transaction_hash', 'log_index'):
            row = legacy(); row[field] = None; rows.append(row)
        result = m.build_fixture_layer([source(rows)], headers(), endpoint())
        self.assertEqual(result['physical_rows'], 3)
        self.assertEqual(result['native_relation_rows'], {'native_identity_unavailable': 3})
        self.assertTrue(all(row['identity_qualification'] for row in result['records']))
        self.assertTrue(all(row['native_identity_transaction_hash'] is None for row in result['records']))

    def test_missing_native_identity_does_not_fall_back_to_cached_components(self):
        row = native(); row['logIndex'] = None
        result = m.build_fixture_layer([source([row], role='native_raw')], headers(), endpoint())
        self.assertEqual(result['native_relation_rows'], {'native_identity_unavailable': 1})
        self.assertIsNone(result['records'][0]['native_identity_log_index'])

    def test_cached_native_locators_must_not_disagree_with_raw_locators(self):
        for field, value in (('block_number', 3), ('log_index', 9), ('transaction_index', 9),
            ('exchange_address', '0x'+'4'*40), ('blockNumber', '0x02'), ('removed', True)):
            with self.subTest(field=field):
                row = native(); row[field] = value
                with self.assertRaises(m.Blocked): m.build_fixture_layer([source([row], role='native_raw')], headers(), endpoint())

    def test_identical_native_payloads_ignore_only_collector_and_provider_provenance(self):
        a, b = native(), native(); b['window'] = 'another-window'; b['source_request_id'] = 2
        b['blockTimestamp'] = 'another-provider-only-time'
        result = m.build_fixture_layer([source([a, b], role='native_raw')], headers(), endpoint())
        self.assertEqual(result['native_relation_rows'], {'identical_observed_replay': 2})
        self.assertNotEqual(result['records'][0]['physical_row_payload_sha256'], result['records'][1]['physical_row_payload_sha256'])
        self.assertEqual(result['records'][0]['observed_payload_sha256'], result['records'][1]['observed_payload_sha256'])

    def test_conflicting_payloads_at_same_identity_are_retained_not_resolved(self):
        a, b = legacy(), legacy(); b['fee'] = 999
        result = m.build_fixture_layer([source([a, b])], headers(), endpoint())
        self.assertEqual(result['native_relation_rows'], {'conflicting_observed_payload': 2})
        self.assertEqual(result['physical_rows'], 2)

    def test_cross_representation_identity_is_not_silently_reconciled(self):
        result = m.build_fixture_layer([source([legacy(2)]), source([native()], 1, 'native_raw')], headers(), endpoint())
        self.assertEqual(result['native_relation_rows'], {'cross_representation_unreconciled': 2})
        self.assertEqual(result['physical_rows'], 2)

    def test_value_equal_records_at_different_native_identity_are_not_replays(self):
        result = m.build_fixture_layer([source([legacy(log=0), legacy(log=1)])], headers(), endpoint())
        self.assertEqual(result['native_relation_rows'], {'unique_observed_native_identity': 2})

    def test_distinct_physical_files_and_every_row_are_preserved(self):
        result = m.build_fixture_layer([source([legacy()], 0), source([legacy()], 1)], headers(), endpoint())
        self.assertEqual([(row['source_file_index'], row['physical_row_ordinal']) for row in result['records']], [(0, 0), (1, 0)])
        self.assertEqual(result['native_relation_rows'], {'identical_observed_replay': 2})


class AdmissionAndPublicationTests(unittest.TestCase):
    def test_existing_legacy_binding_is_not_falsely_full_payload_SHA(self):
        self.assertNotIn('payload_sha256', m.source_binding(binding()))
        value = binding(); value['payload_sha256'] = 'b'*64
        with self.assertRaisesRegex(m.Blocked, 'source_binding_fields'): m.source_binding(value)
        value = binding(role='native_raw'); del value['payload_sha256']
        with self.assertRaisesRegex(m.Blocked, 'source_binding_fields'): m.source_binding(value)

    def test_declared_count_duplicates_aliases_and_fixture_limits_are_fail_closed(self):
        value = source([legacy()]); value['rows_declared'] = 0
        with self.assertRaisesRegex(m.Blocked, 'physical_source_row_count_not_conserved'):
            m.build_fixture_layer([value], headers(), endpoint())
        for value in ([source([legacy()]), source([legacy()])], [source([legacy()]*(m.FIXTURE_MAX_ROWS+1))]):
            with self.assertRaises(m.Blocked): m.build_fixture_layer(value, headers(), endpoint())
        a, b = source([legacy()], 0), source([legacy()], 1)
        b['binding']['identity']['inode'] = a['binding']['identity']['inode']
        with self.assertRaisesRegex(m.Blocked, 'duplicate_declared_source_file'):
            m.build_fixture_layer([a, b], headers(), endpoint())

    def test_real_driver_guards_host_then_blocks_before_all_production_IO(self):
        events = []
        def guard(): events.append('guard')
        with patch.object(m, 'require_production_host', side_effect=guard), patch.object(Path, 'open', side_effect=AssertionError('data opened')):
            with self.assertRaisesRegex(m.Blocked, 'native_clock_production_adapter_not_reviewed'):
                m.run_production('/mnt/data/not_read', '/mnt/data/not_created', 'f'*40)
        self.assertEqual(events, ['guard'])
        with patch.object(m, 'require_production_host', side_effect=RuntimeError('wrong host')):
            with self.assertRaisesRegex(RuntimeError, 'wrong host'): m.run_production()

    def test_contract_lists_real_proof_and_feasibility_blockers_without_admitting_caps(self):
        value = m.candidate_contract()
        self.assertFalse(value['production_admitted'])
        self.assertIsNone(value['finite_runtime_read_RAM_and_output_feasibility'])
        self.assertEqual(value['inherited_envelope_not_production_admission'],
            {'retained_and_scratch_bytes': 64000000000, 'spill_bytes': 16000000000, 'minimum_free_bytes': 20000000000})
        self.assertIn('actual_whole_header_terminal_v2_acceptance_adapter_not_implemented', value['production_blockers'])
        with patch.object(m, 'require_production_host', side_effect=AssertionError('describe must be inert')):
            out = io.StringIO()
            with redirect_stdout(out): self.assertEqual(m.main(['--describe-contract']), 0)
        self.assertEqual(m.decode(out.getvalue().encode())['status'], 'fixture_candidate_production_hard_blocked')

    def publication(self, folder, callback=None):
        sources, cache, bracket = [source([legacy()])], headers(), endpoint()
        result = m.build_fixture_layer(sources, cache, bracket)
        out = Path(folder).resolve()/'result'
        current = lambda: m.fixture_input_digest(sources, cache, bracket)
        return result, out, current if callback is None else callback

    def test_temporary_fixture_publication_reopens_payload_and_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            result, out, current = self.publication(folder)
            marker = m.publish_fixture(result, out, current)
            self.assertEqual({p.name for p in out.iterdir()}, {'fixture.json', 'COMPLETE.json'})
            self.assertEqual(m.decode((out/'fixture.json').read_bytes()), result)
            self.assertEqual(marker, m.decode((out/'COMPLETE.json').read_bytes()))
            with self.assertRaises(m.Blocked): m.publish_fixture(result, out, current)

    def test_late_fixture_input_namespace_and_output_mutations_withhold_COMPLETE(self):
        for change in ('input', 'namespace', 'output'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as folder:
                result, out, current = self.publication(folder)
                calls = 0
                def callback():
                    nonlocal calls
                    calls += 1
                    if calls == 3:
                        if change == 'input': return 'f'*64
                        if change == 'namespace': (out/'extra').write_bytes(b'extra')
                        if change == 'output': (out/'fixture.json').write_bytes(b'changed')
                    return current()
                with self.assertRaises(m.Blocked): m.publish_fixture(result, out, callback)
                self.assertFalse((out/'COMPLETE.json').exists())
                self.assertTrue((out/'.COMPLETE.json.pending').exists())

    def test_fixture_publisher_cannot_be_used_as_a_production_data_publisher(self):
        result = m.build_fixture_layer([source([legacy()])], headers(), endpoint())
        with self.assertRaisesRegex(m.Blocked, 'fixture_output_must_be_fresh_temporary_directory'):
            m.publish_fixture(result, Path('/mnt/data/runs/not_permitted'), lambda: result['input_projection_sha256'])

    def test_modified_fixture_result_or_nonfinite_payload_cannot_be_published(self):
        result = m.build_fixture_layer([source([legacy()])], headers(), endpoint())
        with tempfile.TemporaryDirectory() as folder:
            result['records'][0]['timestamp_UTC_seconds'] += 1
            with self.assertRaisesRegex(m.Blocked, 'fixture_result_changed'):
                m.publish_fixture(result, Path(folder).resolve()/'bad', lambda: result['input_projection_sha256'])
        row = legacy(); row['fee'] = float('nan')
        with self.assertRaisesRegex(m.Blocked, 'fixture_input_not_lossless_finite_JSON'):
            m.build_fixture_layer([source([row])], headers(), endpoint())

    def test_late_result_rewrite_does_not_change_the_captured_input_binding(self):
        with tempfile.TemporaryDirectory() as folder:
            result, out, current = self.publication(folder)
            calls = 0
            def callback():
                nonlocal calls
                calls += 1
                if calls == 3:
                    result['input_projection_sha256'] = 'f'*64
                    return 'f'*64
                return current()
            with self.assertRaisesRegex(m.Blocked, 'fixture_input_changed'):
                m.publish_fixture(result, out, callback)
            self.assertFalse((out/'COMPLETE.json').exists())


if __name__ == '__main__':
    unittest.main()
