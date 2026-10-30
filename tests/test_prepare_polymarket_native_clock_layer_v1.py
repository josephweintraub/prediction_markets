"""Small synthetic control replay; actual small header mirrors only for compatibility.

No production roster, source payload, required-key Parquet, network or EC2 I/O.
The existing roster fixtures patch a tiny literal grid, never production source.
"""
from copy import deepcopy
from contextlib import redirect_stdout
import importlib.util
import io
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from scripts import prepare_polymarket_native_clock_layer_v1 as m


def ref(path):
    raw = path.read_bytes()
    return {'path': str(path), 'bytes': len(raw), 'sha256': m.digest(raw)}


def h(number):
    return '0x'+format(number, '064x')


def endpoint_plan(end):
    return {'schema_version': 'polymarket_refresh_source_plan_v1', 'status': 'source_plan_ready_for_review',
        'end_exclusive_utc': m.core.END_EXCLUSIVE_UTC, 'end_exclusive_unix_seconds': m.core.CUTOFF_SECONDS,
        'last_included_header': {'number': end, 'hash': h(end), 'parentHash': h(end-1), 'timestamp': m.core.CUTOFF_SECONDS-1},
        'first_excluded_header': {'number': end+1, 'hash': h(end+1), 'parentHash': h(end), 'timestamp': m.core.CUTOFF_SECONDS},
        # These summaries are intentionally false: exact fields, not flags, govern.
        'cutoff_proof': {'adjacent_block_numbers': False, 'parent_hash_matches': False}}


def inventory_fixture():
    base = '/mnt/data/synthetic_inventory'
    inputs = {name: {'path': base+'/'+file, 'bytes': 100, 'sha256': 'a'*64} for name, file in (
        ('inventory_manifest', 'manifest.json'), ('inventory_complete', 'COMPLETE.json'),
        ('keys', 'required_keys.parquet'), ('roster', 'roster.json'))}
    identity = {'device': 1, 'inode': 2, 'mode': stat.S_IFREG | 0o600, 'uid': 1, 'gid': 1,
        'mtime_ns': 1, 'ctime_ns': 1, 'bytes': inputs['roster']['bytes']}
    manifest = {'schema_version': 'polymarket_exact_header_key_inventory_v1',
        'status': 'literal_header_key_inventory_complete', 'literal_roster_only': True,
        'final_universe_admitted': False, 'failure_counts': {name: 0 for name in m.FAILURE_FIELDS},
        'global_conflicting_hash_blocks': 0, 'required_keys': m.KEYS_REQUIRED,
        'source_files': 2, 'rows_declared': 100, 'rows_scanned': 100,
        'required_keys_parquet': {**inputs['keys'], 'path': 'required_keys.parquet'},
        'roster': {'path': inputs['roster']['path'], 'sha256': inputs['roster']['sha256'], 'identity': identity},
        'source_inputs_reopened': True, 'roster_reopened': True, 'source_reopened': True,
        'source': {'head': 'b'*40, 'tracked_worktree_clean': True,
            'files': {name: {'bytes': 50, 'sha256': 'c'*64} for name in m.inventory.SOURCE_FILES}}}
    marker = {'schema_version': 'polymarket_exact_header_key_inventory_completion_v1',
        'status': manifest['status'], 'manifest': None,
        'literal_roster_only': True, 'final_universe_admitted': False}
    return inputs, manifest, marker


def seal_inventory(inputs, manifest, marker):
    manifest_raw = m.encode(manifest)
    inputs['inventory_manifest'].update(bytes=len(manifest_raw), sha256=m.digest(manifest_raw))
    marker['manifest'] = {**inputs['inventory_manifest'], 'path': 'manifest.json'}
    marker_raw = m.encode(marker)
    inputs['inventory_complete'].update(bytes=len(marker_raw), sha256=m.digest(marker_raw))
    return marker_raw, manifest_raw


class ActualSmallHeaderCompatibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base = Path(__file__).resolve().parents[1]/'output/polymarket_oct1_finalization_2026-10-27_v1'
        path = base/'header_v8_whole_acceptance_root_reopen_2026-10-30_v1.json'
        if not path.is_file():
            raise AssertionError('Known small actual header mirror required; no silent skip.')
        assert path.stat().st_size <= 16384
        reopened = m.decode(path.read_bytes())
        cls.terminal_raw = m.encode(reopened['acceptance'])
        assert reopened['ref'] == m.HEADER_TERMINAL
        path = base/'header_v8_actual_dispatch_mirrors_v1/full_header_v8_admission_v1.json'
        assert 0 < path.stat().st_size <= 1024**2
        cls.admission_raw = path.read_bytes()
        cls.inputs = {name: m.decode(cls.admission_raw)['inputs'][name] for name in
            ('inventory_complete', 'inventory_manifest', 'keys', 'roster')}

    def test_exact_actual_5491_byte_receipt_is_authenticated(self):
        result = m.header_terminal_adapter(self.terminal_raw, m.HEADER_TERMINAL)
        self.assertEqual(len(self.terminal_raw), 5491)
        self.assertEqual(result['required_keys'], 32901442)
        self.assertEqual(result['native_expected_hashes'], 5884493)
        self.assertEqual(result['sealed_chunks'], 330)
        self.assertFalse(result['production_admitted'])
        self.assertFalse(result['current_remote_closure_reopened_here'])

    def test_exact_actual_admission_binds_existing_inventory_and_roster_refs(self):
        result = m.bind_header_admission(self.terminal_raw, self.admission_raw, self.inputs)
        self.assertEqual(result['roster'], m.KNOWN_ROSTER)
        self.assertEqual(result['keys']['bytes'], 293488549)
        self.assertEqual(result['inventory_manifest']['bytes'], 2689)

    def test_mutated_terminal_cannot_be_rehashed_as_a_new_acceptance(self):
        value = m.decode(self.terminal_raw); value['required_keys_checked'] -= 1
        raw = m.encode(value)
        with self.assertRaisesRegex(m.Blocked, 'terminal_receipt_bytes_or_SHA'):
            m.header_terminal_adapter(raw, m.HEADER_TERMINAL)
        forged = {**m.HEADER_TERMINAL, 'bytes': len(raw), 'sha256': m.digest(raw)}
        with self.assertRaisesRegex(m.Blocked, 'not_actual_published_terminal_reference'):
            m.header_terminal_adapter(raw, forged)

    def test_caller_created_adapter_dictionary_cannot_authenticate(self):
        with self.assertRaisesRegex(m.Blocked, 'terminal_receipt_bytes_or_SHA'):
            m.bind_header_admission({'receipt': m.HEADER_TERMINAL}, self.admission_raw, self.inputs)

    def test_altered_admission_or_input_generation_fails(self):
        with self.assertRaisesRegex(m.Blocked, 'admission_bytes_or_SHA'):
            m.bind_header_admission(self.terminal_raw, self.admission_raw+b' ', self.inputs)
        inputs = deepcopy(self.inputs); inputs['roster']['sha256'] = 'f'*64
        with self.assertRaisesRegex(m.Blocked, 'native_header_inputs_not_accepted_bindings'):
            m.bind_header_admission(self.terminal_raw, self.admission_raw, inputs)


class InventoryStructureTests(unittest.TestCase):
    def call(self, mutate=None):
        inputs, manifest, marker = inventory_fixture()
        if mutate: mutate(inputs, manifest, marker)
        raw = seal_inventory(inputs, manifest, marker)
        return m.inventory_control_adapter(*raw, inputs)

    def test_actual_schema_preserves_original_epoch_without_claiming_body_replay(self):
        result = self.call()
        self.assertEqual(result['original_source_epoch']['head'], 'b'*40)
        self.assertEqual(result['physical_rows_reconciled_in_saved_inventory'], 100)
        self.assertFalse(result['required_key_body_replayed_here'])
        self.assertFalse(result['native_payload_body_rehashed_here'])

    def test_inventory_control_hashes_are_checked_before_schema(self):
        inputs, manifest, marker = inventory_fixture()
        marker_raw, manifest_raw = seal_inventory(inputs, manifest, marker)
        with self.assertRaisesRegex(m.Blocked, 'inventory_control_bytes_or_SHA'):
            m.inventory_control_adapter(marker_raw, manifest_raw+b' ', inputs)

    def test_failure_counter_roster_types_and_nonzero_values_fail(self):
        for change in ('missing', 'extra', 'boolean', 'nonzero'):
            def mutate(i, v, c):
                if change == 'missing': v['failure_counts'].pop('null_hash_rows')
                if change == 'extra': v['failure_counts']['unknown'] = 0
                if change == 'boolean': v['failure_counts']['null_hash_rows'] = False
                if change == 'nonzero': v['failure_counts']['null_hash_rows'] = 1
            with self.subTest(change=change), self.assertRaisesRegex(m.Blocked, 'inventory_failure_or_unknown_counter'):
                self.call(mutate)

    def test_row_and_required_key_counts_are_exact_not_boolean(self):
        for field, value in (('rows_scanned', 99), ('required_keys', True), ('source_files', True)):
            with self.subTest(field=field), self.assertRaisesRegex(m.Blocked, 'inventory_counts'):
                self.call(lambda i, v, c: v.update({field: value}))

    def test_wrong_key_path_or_roster_identity_or_source_roster_fails(self):
        cases = [('inventory_key_binding', lambda i,v,c: v['required_keys_parquet'].update(path='other.parquet')),
            ('inventory_roster_identity', lambda i,v,c: v['roster']['identity'].update(bytes=99)),
            ('inventory_source_epoch', lambda i,v,c: v['source']['files'].update(unknown={'bytes': 1, 'sha256': 'a'*64}))]
        for code, mutate in cases:
            with self.subTest(code=code), self.assertRaisesRegex(m.Blocked, code): self.call(mutate)


class RealExistingLineageFixtureTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).with_name('test_polymarket_exact_header_roster_v1.py')
        spec = importlib.util.spec_from_file_location('native_clock_roster_fixtures', path)
        old = importlib.util.module_from_spec(spec); spec.loader.exec_module(old)
        self.old = old
        self.fixture = old.SourceRosterTests('test_literal_lineage_preserves_zero_rows_compact_inventory_schema')
        original_write = old.write
        def write(path, value, **kwargs):
            if path.name == 'source_plan.json': value = {**value, **endpoint_plan(599)}
            return original_write(path, value, **kwargs)
        self.addCleanup(self.fixture.doCleanups)
        with patch.object(old, 'write', side_effect=write): self.fixture.setUp()
        result = self.fixture.execute()
        self.assertEqual(result['status'], 'sealed_source_roster_complete_not_inventory_admission')
        root = self.fixture.root
        files = sorted(str(path) for path in root.rglob('*') if path.is_file())
        controls = [ref(Path(name)) for name in files if Path(name).suffix in {'.json', '.jsonl'}]
        self.reader = m.FixtureControlReader(root, controls, files)
        self.bindings = {'descriptor': self.fixture.descriptor_ref,
            'roster': ref(self.fixture.output/'roster.json'), 'roster_manifest': ref(self.fixture.output/'manifest.json'),
            'roster_complete': ref(self.fixture.output/'COMPLETE.json')}

    def run_replay(self):
        return m.replay_source_roster_fixture(self.reader, self.bindings)

    def test_existing_replayer_derives_every_file_zero_log_interval_and_physical_rows(self):
        with patch.object(m.roster, 'lineage', wraps=m.roster.lineage) as replay:
            result = self.run_replay()
        self.assertEqual(replay.call_count, 1)
        self.assertEqual(result['source_files'], 10)
        self.assertEqual(result['declared_physical_rows'], sum(g['rows_declared'] for g in result['source_group_declarations'].values()))
        self.assertEqual(result['source_group_declarations']['historical_full']['zero_row_files'], 1)
        self.assertEqual(result['endpoint']['last_included']['block_number'], 599)
        self.assertEqual(result['original_producer_heads']['historical_full'], self.old.HISTORY_HEAD)
        self.assertEqual(result['reused_native_producer_head'], self.old.OLD_HEAD)
        self.assertFalse(result['production_bindings_verified'])
        self.assertFalse(result['original_source_git_blobs_verified'])
        self.assertFalse(result['legacy_footer_identity_is_full_payload_SHA'])
        self.assertEqual(result['source_payload_body_bytes_read_here'], 0)
        self.assertFalse(result['archive_or_sample_admitted'])

    def test_no_public_replay_callback_or_caller_verified_boolean_is_accepted(self):
        bindings = {**self.bindings, 'lineage_verified': True}
        with self.assertRaisesRegex(m.Blocked, 'roster_fixture_input_roster'):
            m.replay_source_roster_fixture(self.reader, bindings)
        with self.assertRaisesRegex(m.Blocked, 'fixture_reader_required_production_IO_blocked'):
            m.replay_source_roster_fixture(object(), self.bindings)

    def test_changed_control_bytes_are_not_adopted(self):
        path = Path(self.fixture.desc['historical_full']['raw_manifest']['path'])
        path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaises(m.Blocked): self.run_replay()

    def test_partial_or_extra_input_binding_fails_before_replay(self):
        for field in self.bindings:
            bindings = deepcopy(self.bindings); bindings.pop(field)
            with self.subTest(field=field), self.assertRaisesRegex(m.Blocked, 'roster_fixture_input_roster'):
                m.replay_source_roster_fixture(self.reader, bindings)

    def test_source_identity_change_is_detected_without_payload_body_read(self):
        path = self.fixture.legacy
        raw = path.read_bytes(); path.write_bytes(raw)
        with self.assertRaises(ValueError): self.run_replay()

    def test_unknown_control_or_namespace_file_fails_closed(self):
        (self.fixture.root/'unexpected.json').write_bytes(b'{}\n')
        with self.assertRaisesRegex(m.Blocked, 'fixture_namespace_changed_or_unknown_file'): self.run_replay()

    def test_final_control_reopen_detects_late_same_bytes_replacement(self):
        original = self.reader.reopen
        def late():
            path = Path(self.fixture.desc['preflight']['path'])
            raw = path.read_bytes(); path.unlink(); path.write_bytes(raw)
            original()
        with patch.object(self.reader, 'reopen', side_effect=late), self.assertRaises(m.Blocked):
            self.run_replay()

    def test_final_namespace_runs_after_source_binding(self):
        original = self.reader.finish
        def late():
            (self.fixture.root/'late.pending').write_bytes(b'pending')
            original()
        with patch.object(self.reader, 'finish', side_effect=late):
            with self.assertRaisesRegex(m.Blocked, 'fixture_namespace_changed_or_unknown_file'): self.run_replay()

    def test_unconsumed_declared_control_is_not_silent_coverage(self):
        path = self.fixture.root/'unknown.json'; path.write_bytes(b'{}\n')
        files = sorted(str(p) for p in self.fixture.root.rglob('*') if p.is_file())
        controls = [ref(Path(name)) for name in files if Path(name).suffix in {'.json', '.jsonl'}]
        reader = m.FixtureControlReader(self.fixture.root, controls, files)
        with self.assertRaisesRegex(m.Blocked, 'unknown_or_unconsumed_fixture_controls'):
            m.replay_source_roster_fixture(reader, self.bindings)

    def test_declared_pending_file_is_not_part_of_derived_source_namespace(self):
        path = self.fixture.root/'unrelated.pending'; path.write_bytes(b'preserved pending')
        files = sorted(str(p) for p in self.fixture.root.rglob('*') if p.is_file())
        controls = [ref(Path(name)) for name in files if Path(name).suffix in {'.json', '.jsonl'}]
        reader = m.FixtureControlReader(self.fixture.root, controls, files)
        with self.assertRaisesRegex(m.Blocked, 'fixture_namespace_not_exact_controls_and_derived_sources'):
            m.replay_source_roster_fixture(reader, self.bindings)


class EndpointAndProductionBoundaryTests(unittest.TestCase):
    def test_exact_integer_endpoint_ignores_summary_booleans(self):
        result = m.endpoint_from_source_plan(endpoint_plan(m.roster.END))
        self.assertEqual(result['last_included']['timestamp'], m.core.CUTOFF_SECONDS-1)

    def test_endpoint_approximation_wrong_parent_wrong_end_and_bool_fail(self):
        for change in ('time', 'parent', 'end', 'boolean'):
            value = endpoint_plan(m.roster.END)
            if change == 'time': value['last_included_header']['timestamp'] = m.core.CUTOFF_SECONDS
            if change == 'parent': value['first_excluded_header']['parentHash'] = h(1)
            if change == 'end': value = endpoint_plan(m.roster.END-1)
            if change == 'boolean': value['last_included_header']['number'] = True
            with self.subTest(change=change), self.assertRaises(m.Blocked): m.endpoint_from_source_plan(value)

    def test_real_paths_and_unknown_JSON_cannot_enter_fixture_reader(self):
        with self.assertRaisesRegex(m.Blocked, 'fixture_root_only_not_production'):
            m.FixtureControlReader(Path('/mnt/data'), [], [])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); path = root/'unknown.json'; path.write_bytes(b'{}\n')
            with self.assertRaises(m.Blocked): m.FixtureControlReader(root, [ref(path)], [str(path), str(path)])

    def test_production_guard_then_hard_block_precedes_input_read(self):
        with patch.object(m, 'require_production_host') as guard, patch.object(Path, 'open', side_effect=AssertionError('input read')):
            with self.assertRaisesRegex(m.Blocked, 'production_adapter_not_reviewed_before_IO'):
                m.run_production('/mnt/data/real.parquet')
            guard.assert_called_once_with()

    def test_contract_is_not_a_resource_execution_or_sample_admission(self):
        result = m.contract()
        self.assertFalse(result['resource_or_execution_receipt'])
        self.assertFalse(result['production_admitted'])
        self.assertFalse(result['production_input_IO_enabled'])
        self.assertFalse(result['legacy_footer_identity_is_full_payload_SHA'])
        self.assertEqual(result['inherited_envelope_not_new_admission']['retained_and_scratch_bytes'], 64000000000)
        self.assertIn('actual_full_lineage_roster_inventory_and_header_closure_replay_not_executed', result['production_blockers'])
        with patch.object(m, 'require_production_host', side_effect=AssertionError('guard not needed')):
            with redirect_stdout(io.StringIO()) as stream: self.assertEqual(m.main(['--describe-contract']), 0)
        self.assertFalse(m.decode(stream.getvalue().encode())['production_admitted'])


if __name__ == '__main__':
    unittest.main()
