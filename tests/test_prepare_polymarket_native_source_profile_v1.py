"""Small pure fixtures and explicitly pinned small historical receipts only."""
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import stat
import unittest
from unittest.mock import patch

from scripts import prepare_polymarket_native_source_profile_v1 as m

HERE=Path(__file__).resolve().parents[1]/'output/polymarket_oct1_finalization_2026-10-27_v1'


def small(path,size):
    if path.stat().st_size!=size:raise AssertionError('small receipt size changed')
    return path.read_bytes()


class PreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw=small(HERE/'native_clock_root_final_resource_drain_remote_stdout_lossless_2026-10-30_v4.json',46755)
        cls.resource=m.actual_resource(cls.raw)
        cls.controls={name:item['raw_UTF8'].encode() for name,item in cls.resource['controls'].items() if 'raw_UTF8' in item}
        wrapper=json.loads(small(HERE/'header_v8_whole_acceptance_root_reopen_2026-10-30_v1.json',7172))
        cls.controls['header_terminal']=m.encode(wrapper['acceptance'])
        cls.controls['header_admission']=small(HERE/'header_v8_actual_dispatch_mirrors_v1/full_header_v8_admission_v1.json',148155)
        for name,raw in cls.controls.items():
            ref=cls.resource['controls'][name]['ref']
            assert len(raw)==ref['bytes'] and m.digest(raw)==ref['sha256']
        cls.manifest=cls.controls['inventory_manifest']

    def identity(self,device=m.OLD_DEVICE,inode=11):
        return {'device':device,'inode':inode,'mode':stat.S_IFREG|0o644,'uid':1000,'gid':1000,
                'mtime_ns':1793117709345260357,'ctime_ns':1793117709345260359}

    def source(self,name,role='native_raw',device=m.OLD_DEVICE,inode=11):
        value={'path':'/mnt/data/fixture/'+name+'.parquet','source_role':role,'bytes':100,
               'footer_sha256':'a'*64,'identity':self.identity(device,inode)}
        if role=='native_raw':value['payload_sha256']='b'*64
        return value

    def schedule(self):
        refs=[item['ref'] for item in self.resource['controls'].values()]
        refs += [self.resource['large_input_stat_only'][name]['binding'] for name in ('roster','source_audit')]
        result=[]
        for ref in refs:
            kind='roster' if ref['path'].endswith('/roster.json') else 'generated_audit' if ref['path'].endswith('.parquet') else 'accepted_control'
            result.append({'reference':deepcopy(ref),'kind':kind,'initial_reads':1,'final_reads':1})
        for name,kind in (('old','original_source_epoch'),('current','current_source')):
            result.append({'reference':{'path':'/home/ubuntu/prediction_markets/'+name+'.py','bytes':100,'sha256':'a'*64},
                           'kind':kind,'initial_reads':1,'final_reads':1})
        return result

    def test_actual_lossless_resource_pinned_and_not_current_or_stage_authority(self):
        value=m.actual_resource(self.raw)
        self.assertEqual(value['mounts']['data_st_dev'],66305)
        identity=value['controls']['descriptor']['identity']
        self.assertEqual(identity['mtime_ns'],1793117709345260357)
        self.assertIs(type(identity['mtime_ns']),int)
        self.assertFalse(value['native_stage_admitted'])

    def test_parsed_dict_and_scalar_verified_flags_are_not_proof(self):
        for value in (self.resource,True,{'accepted':True},b'{}'):
            with self.subTest(value_type=type(value)),self.assertRaises(m.Blocked):m.actual_resource(value)

    def test_lossy_or_reformatted_resource_mirror_cannot_replace_original_bytes(self):
        altered=deepcopy(self.resource)
        altered['controls']['descriptor']['identity']['mtime_ns']=float(1793117709345260357)
        with self.assertRaises(m.Blocked):m.actual_resource(json.dumps(altered).encode())
        with self.assertRaises(m.Blocked):m.actual_resource(self.raw+b'\n')

    def test_actual_inventory_controls_reuse_existing_accepted_adapter(self):
        result=m.actual_inventory(self.raw,self.controls['inventory_complete'],self.manifest)
        self.assertEqual(result['saved_inventory']['required_keys'],32901442)
        self.assertEqual(result['saved_inventory']['roster_identity_at_inventory']['device'],66309)
        self.assertFalse(result['saved_inventory']['native_payload_body_rehashed_here'])

    def test_wrong_inventory_bytes_block_before_decode(self):
        for value in (True,{},b'{}',self.manifest+b'\n'):
            with self.subTest(value_type=type(value)),self.assertRaises(m.Blocked):
                m.actual_inventory(self.raw,self.controls['inventory_complete'],value)

    def test_missing_actual_original_source_plan_is_explicit_blocker(self):
        self.assertEqual(m.missing_control_bytes(self.raw,self.controls),['source_plan'])
        with self.assertRaisesRegex(m.Blocked,'profile_actual_small_control_bytes_missing'):
            m.actual_small_controls(self.raw,self.controls)

    def test_source_plan_projection_is_not_original_serialization(self):
        controls=self.controls|{'source_plan':m.encode(self.resource['controls']['source_plan']['endpoint_projection'])}
        with self.assertRaisesRegex(m.Blocked,'profile_actual_small_control_bytes_or_SHA'):
            m.actual_small_controls(self.raw,controls)

    def test_unknown_control_names_and_boolean_control_rosters_block(self):
        for controls in (True,self.controls|{'accepted':True}):
            with self.subTest(controls_type=type(controls)),self.assertRaises(m.Blocked):m.missing_control_bytes(self.raw,controls)

    def test_device_only_projection_is_lossless_without_source_IO(self):
        original=self.identity();current=original|{'device':66305}
        with patch.object(Path,'open',side_effect=AssertionError('source I/O')):
            result=m.device_only_projection(original,current,self.raw)
        self.assertEqual(result['original_identity'],original)
        self.assertEqual(result['projected_identity'],current)
        self.assertIn('not_fresh',result['qualification'])
        self.assertEqual(original['device'],66309)

    def test_every_nondevice_identity_change_blocks(self):
        original=self.identity()
        for key in m.IDENTITY_FIELDS-{'device'}:
            current=original|{'device':66305,key:original[key]+1}
            with self.subTest(key=key),self.assertRaises(m.Blocked):m.device_only_projection(original,current,self.raw)

    def test_projection_requires_exact_original_and_current_device_and_integer_types(self):
        original=self.identity();current=original|{'device':66305}
        for left,right in ((original|{'device':66304},current),(original,current|{'device':66306}),
                           (original,current|{'mtime_ns':float(current['mtime_ns'])}),
                           (original,current|{'uid':True}),(original,current|{'unverified':1})):
            with self.subTest(left_device=left['device'],right_device=right['device']),self.assertRaises(m.Blocked):
                m.device_only_projection(left,right,self.raw)

    def test_size_in_identity_must_also_be_unchanged(self):
        original=self.identity()|{'bytes':100};current=original|{'device':66305}
        self.assertEqual(m.device_only_projection(original,current,self.raw)['projected_identity']['bytes'],100)
        with self.assertRaises(m.Blocked):m.device_only_projection(original,current|{'bytes':101},self.raw)

    def test_source_order_roles_hashes_and_count_are_preserved(self):
        originals=[self.source('one','legacy_raw',inode=11),self.source('two',inode=12)]
        currents=[{**item,'identity':item['identity']|{'device':66305}} for item in originals]
        result=m.project_source_bindings(originals,currents,self.raw)
        self.assertEqual(result['source_bindings'],currents)
        self.assertEqual(len(result['source_bindings']),2)
        self.assertFalse(result['current_source_identities_observed_here'])
        for wrong in (currents[:1],list(reversed(currents)),[currents[0],currents[1]|{'payload_sha256':'c'*64}]):
            with self.subTest(wrong_count=len(wrong)),self.assertRaises(m.Blocked):m.project_source_bindings(originals,wrong,self.raw)

    def test_projection_does_not_change_source_size_or_footer(self):
        original=self.source('one');current={**original,'identity':original['identity']|{'device':66305}}
        for change in ({'bytes':101},{'footer_sha256':'c'*64},{'path':'/mnt/data/fixture/other.parquet'}):
            with self.subTest(change=change),self.assertRaises(m.Blocked):m.project_source_bindings([original],[current|change],self.raw)

    def test_declared_plan_uses_real_footer_upper_and_smaller_output_under_same_ceiling(self):
        schedule=self.schedule();before=deepcopy(m.profile.PROPOSED)
        plan=m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        expected=2*sum(item['reference']['bytes'] for item in schedule)+683846870+2*16*1024**2+1024
        self.assertEqual(plan['total_read_upper_bytes'],expected)
        self.assertEqual(plan['one_source_footer_pass_bytes'],683846870)
        self.assertEqual(plan['read_ceiling_bytes'],2000000000)
        self.assertEqual(plan['all_new_retained_output_cap_bytes'],16777216)
        self.assertEqual(m.profile.PROPOSED,before)
        self.assertFalse(plan['full_nested_closure_authenticated_here']);self.assertFalse(plan['production_admitted'])

    def test_read_multiplicity_is_counted_not_collapsed(self):
        schedule=self.schedule();baseline=m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        schedule[0]['initial_reads']=2
        plan=m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        self.assertEqual(plan['total_read_upper_bytes']-baseline['total_read_upper_bytes'],schedule[0]['reference']['bytes'])

    def test_optional_one_MiB_cap_is_explicit_reduction_not_output_fit_proof(self):
        schedule=self.schedule();default=m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        smaller=m.declared_read_plan(self.raw,self.manifest,schedule,1024,retained_output_cap_bytes=1024**2)
        self.assertEqual(default['total_read_upper_bytes']-smaller['total_read_upper_bytes'],31457280)
        self.assertFalse(smaller['aggregate_output_fit_proved'])
        self.assertEqual(smaller['oversized_output_policy'],'block_not_drop_or_increase_cap')
        for cap in (True,0,1024**2-1,m.OUTPUT_CAP+1):
            with self.subTest(cap=cap),self.assertRaises(m.Blocked):
                m.declared_read_plan(self.raw,self.manifest,schedule,1024,retained_output_cap_bytes=cap)

    def test_historical_reopen_counter_only_gives_conditional_arithmetic(self):
        schedule=self.schedule()
        schedule.append({'reference':{'path':'/mnt/data/unverified_conditional_lineage_union.json',
            'bytes':569642935,'sha256':'a'*64},'kind':'accepted_control','initial_reads':1,'final_reads':1})
        with self.assertRaisesRegex(m.Blocked,'unchanged_2GB'):
            m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        smaller=m.declared_read_plan(self.raw,self.manifest,schedule,1024,retained_output_cap_bytes=1024**2)
        self.assertLess(smaller['total_read_upper_bytes'],2000000000)
        self.assertFalse(smaller['full_nested_closure_authenticated_here'])

    def test_missing_control_source_or_Git_cost_blocks(self):
        schedule=self.schedule()
        for wrong in (schedule[1:],schedule[:-1],schedule[:-2],[]):
            with self.subTest(length=len(wrong)),self.assertRaises(m.Blocked):m.declared_read_plan(self.raw,self.manifest,wrong,1024)

    def test_duplicate_or_conflicting_path_is_not_implicitly_unioned(self):
        schedule=self.schedule()
        for extra in (deepcopy(schedule[0]),deepcopy(schedule[0])):
            extra['reference']['sha256']='f'*64
            with self.assertRaises(m.Blocked):m.declared_read_plan(self.raw,self.manifest,schedule+[extra],1024)

    def test_invalid_or_omitted_read_counts_do_not_understate_cost(self):
        for key,wrong in (('initial_reads',True),('final_reads',0),('final_reads',-1)):
            schedule=self.schedule();schedule[0][key]=wrong
            with self.subTest(key=key,wrong=wrong),self.assertRaises(m.Blocked):m.declared_read_plan(self.raw,self.manifest,schedule,1024)
        for wrong in (0,-1,True):
            with self.subTest(bootstrap=wrong),self.assertRaises(m.Blocked):m.declared_read_plan(self.raw,self.manifest,self.schedule(),wrong)

    def test_over_ceiling_blocks_instead_of_raising_limit_or_skipping_ref(self):
        schedule=self.schedule();schedule.append({'reference':{'path':'/mnt/data/huge.json','bytes':600000000,'sha256':'a'*64},
                                                 'kind':'accepted_control','initial_reads':1,'final_reads':1})
        with self.assertRaisesRegex(m.Blocked,'unchanged_2GB'):m.declared_read_plan(self.raw,self.manifest,schedule,1024)

    def test_raw_trade_or_required_key_Parquet_body_cannot_enter_plan(self):
        for path in ('/mnt/data/raw.parquet',self.resource['large_input_stat_only']['keys']['binding']['path']):
            schedule=self.schedule();schedule.append({'reference':{'path':path,'bytes':100,'sha256':'a'*64},
                'kind':'generated_audit','initial_reads':1,'final_reads':1})
            with self.subTest(path=path),self.assertRaises(m.Blocked):m.declared_read_plan(self.raw,self.manifest,schedule,1024)

    def test_boolean_or_dictionary_closure_acceptance_cannot_enable_candidate(self):
        with patch.object(m,'actual_small_controls',return_value={'verified':True}):
            with self.assertRaisesRegex(m.Blocked,'full_nested_closure_and_source_epoch_proof_still_missing'):
                m.prepare_actual_candidate(self.raw,self.controls,schedule=self.schedule(),bootstrap_read_bytes=1024)

    def test_production_normal_guard_then_unconditional_block_before_IO_or_fixture(self):
        with patch.object(m,'require_production_host') as guard,patch.object(Path,'open',side_effect=AssertionError('I/O')), \
             patch.object(m.profile,'run_fixture',side_effect=AssertionError('fixture loophole')):
            with self.assertRaisesRegex(m.Blocked,'actual_admission_missing_before_IO'):
                m.run_production(accepted=True,resource_verified=True)
        guard.assert_called_once_with()

    def test_contract_is_pure_and_does_not_claim_stage_fullhash_or_fit_admission(self):
        with patch.object(Path,'open',side_effect=AssertionError('I/O')),contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(m.main(['--describe-contract']),0)
        value=json.loads(out.getvalue())
        self.assertFalse(value['production_input_IO_enabled'])
        self.assertFalse(value['legacy_footer_identity_is_full_payload_SHA'])
        self.assertFalse(value['fixture_driver_called'])


if __name__=='__main__':unittest.main()
