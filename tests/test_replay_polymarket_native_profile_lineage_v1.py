"""Bounded temporary fixtures; no real roster/audit/trade/source-epoch reads."""
from copy import deepcopy
import hashlib
import importlib.util
from pathlib import Path
import signal
import stat
import tempfile
import time
import unittest
from unittest.mock import patch

from scripts import replay_polymarket_native_profile_lineage_v1 as m


def ref(path):
    raw=path.read_bytes();return {'path':str(path),'bytes':len(raw),'sha256':m.digest(raw)}


def declaration(path,role='control',reference=None):
    return {'reference':reference or ref(path),'identity':m.roster.identity(path),'role':role}


def epochs():
    return [{'head':'a'*40,'path':'scripts/source.py','bytes':100,'sha256':'b'*64,'reads':2}]


class MeterReaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve();self.path=self.root/'control.json';self.path.write_bytes(b'{"n":1}\n')
        self.meter=m.Meter(time.monotonic()+60,10)

    def reader(self,declared=None,files=None):
        return m.FixtureReader(self.root,declared or [declaration(self.path)],files or [str(self.path)],self.meter)

    def test_no_historical_counter_and_shared_categories_or_reservations_cannot_exceed_2GB(self):
        self.assertEqual(self.meter.usage()['read_bytes'],10)
        self.meter.charge('control_initial',m.READ_LIMIT-12);self.meter.reserve('git_blob',2)
        with self.assertRaises(m.Blocked):self.meter.charge('source_footer',1)
        self.meter.charge('git_blob',2)
        self.assertEqual(self.meter.usage()['read_bytes'],m.READ_LIMIT)
        for value in (True,-1):
            with self.subTest(value=value),self.assertRaises(m.Blocked):self.meter.charge('current_source',value)

    def test_deadline_must_be_finite_numeric_before_timer_or_IO(self):
        for value in (float('inf'),float('-inf'),float('nan'),True,False):
            with self.subTest(value=value),patch.object(m.signal,'setitimer') as timer, \
                 patch.object(Path,'open',side_effect=AssertionError('I/O')),self.assertRaises(m.Blocked):
                m.Meter(value,1)
            timer.assert_not_called()

    def test_hard_timer_is_required_before_any_control_open(self):
        reader=self.reader()
        with patch.object(Path,'open',side_effect=AssertionError('I/O before timer')):
            with self.assertRaisesRegex(m.Blocked,'hard_timer_required'):reader.raw(ref_without_io(self.path))

    def test_earlier_existing_timer_is_preserved_not_reset(self):
        calls=[]
        with patch.object(m.signal,'getitimer',return_value=(0.5,0)),patch.object(m.signal,'getsignal',return_value=signal.SIG_DFL), \
             patch.object(m.signal,'signal'),patch.object(m.signal,'setitimer',side_effect=lambda *args:calls.append(args)), \
             patch.object(m,'_CLOCK',side_effect=[10,10.1]):
            with self.meter.hard_timer():self.assertEqual(self.meter.active,1)
        self.assertAlmostEqual(calls[0][1],0.5);self.assertAlmostEqual(calls[1][1],0.4)
        self.assertEqual(self.meter.active,0)

    def test_expired_deadline_blocks_before_IO(self):
        reader=self.reader();self.meter.deadline=time.monotonic()-1
        with patch.object(Path,'open',side_effect=AssertionError('I/O')):
            with self.assertRaisesRegex(m.Blocked,'caller_deadline'):
                with self.meter.hard_timer():reader.raw(ref_without_io(self.path))

    def test_observed_then_json_caches_bytes_but_rechecks_identity_and_read_counts(self):
        reader=self.reader();original=ref(self.path)
        with self.meter.hard_timer():
            self.assertEqual(reader.observed(self.path),original)
            self.assertEqual(reader.json(original),{'n':1})
            self.assertEqual(reader.inventory()['physical_reads'][0]['reads'],1)
            self.assertEqual(reader.inventory()['requested_reads'][0]['calls'],2)
            self.assertEqual(self.meter.usage()['read_bytes'],10+original['bytes']+1)
            reader.reopen()
        inventory=reader.inventory()
        self.assertEqual(sum(item['reads'] for item in inventory['physical_reads']),2)
        self.assertEqual(self.meter.usage()['read_bytes'],10+2*(original['bytes']+1))

    def test_cached_reference_and_identity_cannot_change(self):
        reader=self.reader();original=ref(self.path)
        with self.meter.hard_timer():
            reader.raw(original)
            with self.assertRaises(m.Blocked):reader.raw(original|{'sha256':'0'*64})
            self.path.write_bytes(b'{"n":2}\n')
            with self.assertRaises(m.Blocked):reader.raw(original)

    def test_unregistered_observed_or_derived_path_is_not_adopted(self):
        reader=self.reader()
        with self.meter.hard_timer():
            for call in (lambda:reader.observed(self.root/'unknown.json'),lambda:reader.derived(self.path,'0'*64)):
                with self.assertRaises(m.Blocked):call()

    def test_failed_hash_read_remains_charged_without_refund(self):
        wrong=ref(self.path)|{'sha256':'0'*64};reader=self.reader([declaration(self.path,reference=wrong)])
        with self.meter.hard_timer(),self.assertRaises(m.Blocked):reader.raw(wrong)
        self.assertEqual(self.meter.usage()['read_bytes'],10+wrong['bytes']+1)
        self.assertEqual(sum(reader.physical.values()),1)

    def test_historical_manifest_role_supports_over8MiB_without_global_cap_change(self):
        path=self.root/'manifest.json';path.write_bytes(b'"'+b'x'*(8*1024**2)+b'"')
        bound=ref(path)
        with self.assertRaises(m.Blocked):self.reader([declaration(path)], [str(path)])
        reader=self.reader([declaration(path,'historical_manifest')],[str(path)])
        with self.meter.hard_timer():
            self.assertEqual(len(reader.raw(bound)),bound['bytes'])
            with self.assertRaises(m.Blocked):reader.raw(bound,limit=8*1024**2)
        self.assertEqual(m.profile.PROPOSED['control_bytes_per_file'],8*1024**2)
        self.assertEqual(m.ROLE_CAPS['historical_manifest'],128*1024**2)

    def test_raw_trade_or_required_keys_cannot_be_registered_as_controls(self):
        for name in ('raw.parquet','required_keys.parquet'):
            path=self.root/name;path.write_bytes(b'fixture only')
            with self.subTest(name=name),self.assertRaises(m.Blocked):self.reader([declaration(path,'generated_audit')],[str(path)])

    def test_real_data_namespace_and_symlinks_are_rejected(self):
        with self.assertRaises(m.Blocked):m.FixtureReader('/mnt/data',[],[],self.meter)
        alias=self.root/'alias.json';alias.symlink_to(self.path)
        with self.meter.hard_timer(),self.assertRaises(m.Blocked):self.reader().namespace()

    def test_late_namespace_addition_after_callback_is_caught_by_primitive_finish(self):
        reader=self.reader();original_namespace=reader.namespace;calls=[]
        def late():
            original_namespace();calls.append(1)
            if len(calls)==2:(self.root/'late_OWNER.json').write_bytes(b'{}\n')
        with self.meter.hard_timer():
            reader.raw(ref(self.path))
            with patch.object(reader,'namespace',side_effect=late),self.assertRaises(m.Blocked):reader.reopen()

    def test_epoch_schedule_is_reserved_not_mistaken_for_Git_authentication(self):
        result=m.epoch_schedule(epochs())
        self.assertEqual(result['declared_Git_read_bytes'],200)
        self.assertFalse(result['original_source_epoch_blobs_verified_here'])
        for change in ({'head':'bad'},{'path':'../secret'},{'path':''},{'reads':True},{'bytes':0}):
            with self.subTest(change=change),self.assertRaises(m.Blocked):m.epoch_schedule([epochs()[0]|change])

    def test_production_normal_guard_then_hardblock_without_IO_or_oldReader(self):
        with patch.object(m,'require_production_host') as guard,patch.object(Path,'open',side_effect=AssertionError('I/O')), \
             patch.object(m.roster,'Reader',side_effect=AssertionError('old spent counters')), \
             patch.object(m.roster,'source_snapshot',side_effect=AssertionError('unmetered Git')):
            with self.assertRaisesRegex(m.Blocked,'actual_driver_admission_missing_before_IO'):m.run_production(verified=True)
        guard.assert_called_once_with()


def ref_without_io(path):
    raw=b'{"n":1}\n';return {'path':str(path),'bytes':len(raw),'sha256':m.digest(raw)}


class ExistingLineageTests(unittest.TestCase):
    def setUp(self):
        path=Path(__file__).with_name('test_polymarket_exact_header_roster_v1.py')
        spec=importlib.util.spec_from_file_location('metered_lineage_existing_fixture',path)
        old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
        self.fixture=old.SourceRosterTests('test_literal_lineage_preserves_zero_rows_compact_inventory_schema')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        # Existing tiny fixture generation alone builds its expected roster.
        # Candidate execution below is separately spied: no payload/footer/Git.
        result=self.fixture.execute();self.assertEqual(result['status'],'sealed_source_roster_complete_not_inventory_admission')
        self.root=self.fixture.root;self.desc=m.decode(self.fixture.descriptor.read_bytes())
        self.entries=m.decode((self.fixture.output/'roster.json').read_bytes())['files']
        self.manifest=m.decode((self.fixture.output/'manifest.json').read_bytes())
        files=sorted(str(path) for path in self.root.rglob('*') if path.is_file())
        declared=[]
        for name in files:
            path=Path(name)
            if path.suffix not in {'.json','.jsonl'}:continue
            role='historical_manifest' if name==self.desc['historical_full']['raw_manifest']['path'] else 'roster' if path.name=='roster.json' else 'control'
            declared.append(declaration(path,role))
        self.meter=m.Meter(time.monotonic()+60,100)
        self.reader=m.FixtureReader(self.root,declared,files,self.meter)
        self.bindings={'descriptor':self.fixture.descriptor_ref,'roster':ref(self.fixture.output/'roster.json'),
                       'roster_manifest':ref(self.fixture.output/'manifest.json')}
        derived=[self.fixture.frozen_legacy]
        for item,group in ((self.fixture.old,'reused_v1'),(self.fixture.fresh,'fresh_v2'),
                           (self.fixture.probe,'initial_probe_v1'),(self.fixture.history,'historical_full')):
            derived.extend(self.fixture.refs(item,group))
        self.legacy=derived[0];self.native=derived[1:]
        by_path={item['path']:item for item in derived}
        self.audit=[]
        for index,entry in enumerate(self.entries):
            n=by_path[entry['path']]['rows_declared']
            self.audit.append({'source_index':index,**entry,'identity_json':m.encode(entry['identity']).decode(),
                'rows_declared':n,'rows_scanned':n,'valid_rows':n,'projected_compressed_bytes':0,
                'payload_sha256_verified':entry['source_role']=='native_raw',
                **{name:0 for name in m.proof.FAILURE_FIELDS}})

    def run_replay(self):
        return m.replay_fixture(self.reader,self.bindings,self.audit,epochs())

    def test_unchanged_full_lineage_replayer_preserves_zero_rows_and_order_without_payload_reads(self):
        original_open=Path.open
        def opened(path,*args,**kwargs):
            if path.suffix=='.parquet':raise AssertionError('payload/footer read')
            return original_open(path,*args,**kwargs)
        with patch.object(Path,'open',opened),patch.object(m.roster,'Reader',side_effect=AssertionError('old Reader')), \
             patch.object(m.roster,'source_snapshot',side_effect=AssertionError('old unmetered Git')):
            result=self.run_replay()
        self.assertEqual(result['reconciliation']['source_files'],10)
        self.assertEqual(result['reconciliation']['source_groups']['historical_full']['zero_row_files'],1)
        self.assertFalse(result['production_admitted']);self.assertFalse(result['actual_full_closure_replayed_here'])
        self.assertEqual(result['actual_required_source_files'],131356)
        self.assertEqual(result['controls']['usage']['planned_read_reservations_bytes']['git_blob'],200)
        self.assertEqual(result['source_payload_or_footer_bytes_read'],0)
        self.assertTrue(any(row['calls']>row2['reads'] for row in result['controls']['requested_reads']
                            for row2 in result['controls']['physical_reads'] if (row['phase'],row['path'])==(row2['phase'],row2['path'])))

    def test_audit_or_order_change_cannot_drop_or_relabel_sources(self):
        for entries,audit in ((self.entries[::-1],self.audit),(self.entries,self.audit[:-1]),
                              (self.entries,self.audit+[self.audit[0]])):
            with self.subTest(entries_first=entries[0]['path']),self.assertRaises(ValueError):
                m.reconcile(self.legacy,self.native,entries,audit,self.manifest['counts'])

    def test_original_audit_identity_and_physical_rows_are_exact(self):
        for change in ({'rows_scanned':999},{'source_index':True},{'identity_json':'{}'}):
            rows=deepcopy(self.audit);rows[0].update(change)
            with self.subTest(change=change),self.assertRaises(ValueError):m.reconcile(self.legacy,self.native,self.entries,rows,self.manifest['counts'])

    def test_full_original_group_counts_cannot_be_replaced_by_same_total(self):
        counts=deepcopy(self.manifest['counts']);counts['historical_full']['zero_row_files']+=1
        with self.assertRaises(ValueError):m.reconcile(self.legacy,self.native,self.entries,self.audit,counts)

    def test_large_role_is_only_the_descriptor_bound_historical_manifest(self):
        for item in self.reader.declared.values():
            if item['role']=='historical_manifest':item['role']='control'
        with self.assertRaisesRegex(m.Blocked,'historical_role_not_exact_descriptor'):self.run_replay()


class OriginalProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=Path(__file__).resolve().parents[1]/'output/polymarket_oct1_finalization_2026-10-27_v1/native_clock_root_final_resource_drain_remote_stdout_lossless_2026-10-30_v4.json'
        if path.stat().st_size!=46755:raise AssertionError('small receipt size')
        cls.raw=path.read_bytes();m.preparation.actual_resource(cls.raw)

    def test_projection_preserves_original_bytes_and_every_nondevice_integer(self):
        identity={'device':66305,'inode':123,'mode':stat.S_IFREG|0o644,'uid':1000,'gid':1000,
                  'mtime_ns':1793117709345260357,'ctime_ns':1793117709345260359}
        controls=[{'path':'/mnt/data/a.json','bytes':10,'sha256':'a'*64,'identity':identity}]
        before=deepcopy(controls);result=m.projected_control_inventory(controls,self.raw)
        self.assertEqual(controls,before);self.assertEqual(result['unique_control_bytes'],10)
        self.assertNotEqual(result['original_device_projection_sha256'],result['current_identity_projection_sha256'])
        self.assertFalse(result['actual_production_closure_authenticated'])
        self.assertEqual(result['required_actual_controls'],1766)
        original=deepcopy(controls[0]);original['identity']['device']=66309
        old_rolling=hashlib.sha256();old_rolling.update(m.encode(original))
        new_rolling=hashlib.sha256();new_rolling.update(m.encode(controls[0]))
        self.assertEqual(result['original_device_projection_sha256'],old_rolling.hexdigest())
        self.assertEqual(result['current_identity_projection_sha256'],new_rolling.hexdigest())
        self.assertNotEqual(result['original_device_projection_sha256'],m.digest(m.encode([original])))
        self.assertEqual(result['projection_codec'],m.PROJECTION_CODEC)
        for change in ({'device':66306},{'mtime_ns':float(identity['mtime_ns'])}):
            wrong=[controls[0]|{'identity':identity|change}]
            with self.subTest(change=change),self.assertRaises(ValueError):m.projected_control_inventory(wrong,self.raw)

    def test_projection_uses_sorted_per_control_lines_not_array_encoding(self):
        identity={'device':66305,'inode':1,'mode':stat.S_IFREG|0o644,'uid':1000,'gid':1000,'mtime_ns':9,'ctime_ns':10}
        controls=[{'path':'/mnt/data/z.json','bytes':2,'sha256':'a'*64,'identity':identity},
                  {'path':'/mnt/data/a.json','bytes':3,'sha256':'b'*64,'identity':identity|{'inode':2}}]
        result=m.projected_control_inventory(controls,self.raw)
        expected=hashlib.sha256()
        for item in sorted(controls,key=lambda item:item['path']):
            expected.update(m.encode({**item,'identity':item['identity']|{'device':66309}}))
        self.assertEqual(result['original_device_projection_sha256'],expected.hexdigest())


if __name__=='__main__':unittest.main()
