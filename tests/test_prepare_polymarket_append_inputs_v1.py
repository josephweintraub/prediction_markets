"""Control-only fixtures. No trade/header bodies, filesystem stats or APIs."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts import prepare_polymarket_append_inputs_v1 as m


class AppendInputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        p=Path('output/polymarket_oct1_finalization_2026-10-27_v1/append_only_source_controls_and_fourth_profile_drain_2026-10-30_v1.json')
        assert p.stat().st_size == m.CAPTURE_BYTES
        cls.capture=p.read_bytes();cls.raws=m.captured_controls(cls.capture)
        cls.values={key:m.decode(cls.raws[pin['path']]) for key,pin in m.ANCHORS.items()}

    def callback(self, request): return self.raws.get(request['path'])

    def test_actual_nine_controls_bound_full_bytes_not_projection_or_callback_flag(self):
        self.assertEqual(len(self.raws),9)
        for pin in m.ANCHORS.values():
            raw=self.raws[pin['path']];self.assertEqual(len(raw),pin['bytes']);self.assertEqual(m.digest(raw),pin['sha256'])
        plan=m.prepare_plan(self.callback)
        self.assertFalse(plan['production_admitted']);self.assertFalse(plan['callback_is_physical_authentication'])
        self.assertFalse(plan['new_collection_authorized']);self.assertEqual(plan['data_body_reads'],0)
        self.assertEqual(plan['fixture_control_bytes_read'],2*sum(pin['bytes'] for pin in m.ANCHORS.values()))

    def test_actual_baseline_preserves44_clean_eleven_physical_columns_not_ROOT_ten(self):
        clean=m._baseline(self.values['baseline'])
        self.assertEqual(len(clean),44);self.assertEqual(sum(r['rows'] for r in clean),2036128538)
        self.assertTrue(all(len(r['physical_schema'].splitlines())==11 for r in clean))
        self.assertTrue(all('/clean/year_month=' in r['path'] and r['path'].endswith('/data.parquet') for r in clean))
        self.assertEqual(sum(r['bytes'] for r in clean),29838148561)
        self.assertEqual(clean[-1]['year_month'],'2026-06')

    def test_ROOT_or_ten_column_or_changed_baseline_schema_never_substituted(self):
        for change in ('ten','root','rows','month','path','bool'):
            value=deepcopy(self.values['baseline']);record=next(r for r in value['outputs'] if r['relation']=='clean')
            if change=='ten':record['output']['schema']='\n'.join(record['output']['schema'].splitlines()[:-1])
            elif change=='root':record['relation']='root'
            elif change=='rows':record['output']['rows']+=1
            elif change=='month':record['month']='2022-12'
            elif change=='path':record['path']='clean/../root'
            else:record['output']['rows']=True
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):m._baseline(value)

    def test_actual_campaign582_with19_explicit_inherited_old_producer_roots(self):
        records=m._campaign(self.values['campaign'],self.values['campaign_complete'])
        old=[r for r in records if r['collector_version']==1];new=[r for r in records if r['collector_version']==2]
        self.assertEqual((len(records),len(old),len(new)),(582,19,563))
        self.assertEqual(sum(r['rows'] for r in records),473196272)
        self.assertTrue(all(r['attempt_candidates'][0].startswith(m.INHERITED+'/') for r in old))
        self.assertTrue(all(r['producer_head']=='19202a820bef569f449a34fc731107f16470e155' for r in old))
        self.assertTrue(all(r['producer_head'] is None and len(r['attempt_candidates'])==3 for r in new))
        self.assertEqual(records[17]['chunk']['chunk'],'c000018')
        self.assertEqual(records[-1]['chunk']['last_block'],94796039)

    def test_actual_probe20_payloads_with_request_ledgers_not_capacity_probe(self):
        result=m._probe(self.values['probe'],self.values['probe_complete'])
        self.assertEqual(len(result['payloads']),20);self.assertEqual(len(result['request_ledgers']),20)
        self.assertEqual(sum(r['rows'] for r in result['payloads']),146230)
        self.assertTrue(all(r['path'].startswith(m.PROBE+'/attempts/') for r in result['payloads']))
        self.assertEqual(result['producer_head'],'96e7062a7d2ce826b3a637c4f9e3375392af3c14')

    def test_actual_header330_PARQUETS_reuses_whole_acceptance_not_JSONL_or_verifier(self):
        raw=self.raws[m.ANCHORS['header_acceptance']['path']]
        records=m._headers(raw,self.values['headers'],self.values['headers_complete'])
        self.assertEqual(len(records),330);self.assertEqual(sum(r['keys'] for r in records),32901442)
        self.assertTrue(all(r['path'].endswith('/headers.parquet') for r in records))
        self.assertEqual(records[-1]['keys'],1442)
        self.assertIn('exact_header_campaign_v6',records[0]['path'])
        self.assertIn('exact_header_campaign_v8',records[-1]['path'])

    def test_plan_explicitly_withholds_campaign_filecount_until_actual_children(self):
        result=m.prepare_plan(self.callback)
        self.assertEqual(result['expected_append_rows'],473342502)
        self.assertEqual(result['expected_append_payload_files'],116364)
        self.assertEqual(result['derived_payload_files_so_far'],20)
        self.assertFalse(result['campaign_payload_file_count_derived'])
        self.assertFalse(result['all_child_manifests_expanded'])
        self.assertEqual(result['first_block'],88978538);self.assertEqual(result['last_block'],94796039)
        self.assertEqual(result['cutoff_exclusive_unix'],1790899200)

    def test_gap_duplicate_order_changed_row_or_inherited_path_blocks(self):
        for change in ('gap','duplicate','order','rows','external','epoch'):
            value=deepcopy(self.values['campaign']);rows=value['accepted_chunks']
            if change=='gap':rows[2]['chunk']['first_block']+=1
            elif change=='duplicate':rows[2]=deepcopy(rows[1])
            elif change=='order':rows[0],rows[1]=rows[1],rows[0]
            elif change=='rows':rows[0]['accepted']['rows']+=1
            elif change=='external':rows[0]['external_attempt']=rows[0]['external_attempt'].replace('campaign_v1','campaign_v2')
            else:rows[0]['producer_head']='a'*40
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):
                m._campaign(value,self.values['campaign_complete'])

    def test_source_plan_cutoff_provenance_is_NOT_collection_approval(self):
        m._source_plan(self.values['source_plan'])
        for field,value in (('collection_approved',True),('end_exclusive_unix_seconds',1790899201),
                            ('end_exclusive_utc','2026-10-03T00:00:00Z')):
            with self.subTest(field=field),self.assertRaises(m.InputBlocked):
                m._source_plan(self.values['source_plan']|{field:value})

    def test_unsafe_relative_refs_absolute_escapes_controls_and_bool_sizes(self):
        for path in ('../x','/absolute','a//b','a/./b','x\x00','x\n'):
            with self.subTest(path=path),self.assertRaises(m.InputBlocked):
                m.ref({'path':path,'bytes':1,'sha256':'a'*64},m.PROBE)
        for size in (True,0,-1):
            with self.subTest(size=size),self.assertRaises(m.InputBlocked):
                m.ref({'path':'/mnt/data/a','bytes':size,'sha256':'a'*64})

    def test_duplicate_JSON_keys_nonfinite_bad_utf8_and_compressed_capture_pin(self):
        for raw in (b'{"a":1,"a":2}',b'{"x":NaN}',b'\xff'):
            with self.subTest(raw=raw),self.assertRaises(m.InputBlocked):m.decode(raw)
        with self.assertRaisesRegex(m.InputBlocked,'actual_capture_pin'):
            m.captured_controls(self.capture[:-1]+b' ')

    def test_compressed_control_trailing_stream_bomb_or_missing_roster_fails(self):
        import base64,zlib
        capture=json.loads(self.capture)
        for change in ('extra','length','bomb','duplicate'):
            value=deepcopy(capture);record=value['controls'][0]
            if change=='extra':record['raw_zlib_base64']+=base64.b64encode(b'x').decode()
            elif change=='length':record['bytes']+=1
            elif change=='bomb':record['raw_zlib_base64']=base64.b64encode(zlib.compress(b'x'*(m.MAX_CONTROL+1))).decode()
            else:value['controls'][1]=deepcopy(record)
            raw=m.encode(value)
            with self.subTest(change=change),patch.object(m,'CAPTURE_BYTES',len(raw)),patch.object(m,'CAPTURE_SHA',m.digest(raw)),self.assertRaises(m.InputBlocked):
                m.captured_controls(raw)

    def test_initial_or_final_reader_byte_mutation_blocks_and_no_body_requests(self):
        seen=[];count={}
        def reader(request):
            path=request['path'];seen.append(path);count[path]=count.get(path,0)+1
            raw=self.raws[path]
            return raw if count[path]==1 else raw[:-1]+b' '
        with self.assertRaisesRegex(m.InputBlocked,'nested_control_pin'):m.prepare_plan(reader)
        self.assertTrue(set(seen)<=set(self.raws));self.assertFalse(any(p.endswith('.parquet') for p in seen))
        with self.assertRaisesRegex(m.InputBlocked,'required_control_absent'):m.prepare_plan(lambda _:None)
        with self.assertRaisesRegex(m.InputBlocked,'bounded_reader_bytes'):m.prepare_plan(lambda _:{'authenticated':True})

    def child(self,version=2,attempt=2):
        first=88979538;last=first+49;chunk={'chunk':'c000001','first_block':first,'last_block':last}
        head='19202a820bef569f449a34fc731107f16470e155' if version==1 else 'c'*40
        contract={'schema_version':f'polymarket_native_collection_contract_v{version}','mode':'append',
            'first_block':first,'last_block':last,'addresses':list(m.old.pilot.ADDRESSES),
            'topics':sorted(set(m.old.pilot.EXPECTED_TOPICS.values())),'end_exclusive_utc':'2026-10-02T00:00:00Z',
            'raw_payload_only':True,'analysis_admitted':False,'provider_block_timestamp_used_as_clock':False,
            'source':{'head':head,'status':'inspected','tracked_worktree_clean':True}}
        sha=m.digest(m.encode(contract));window=m.old.windows(first,last,50)[0]
        evidence={'rows':1,'counts':[{'address':a,'rows':int(i==0)} for i,a in enumerate(m.old.pilot.ADDRESSES)]}
        directory=('attempts/'+window['window']+'.a1' if version==1 else 'nodes/'+window['window']+'/a1')
        item={'window':window,'attempt':1,'status':'raw_shard_validated','contract_sha256':sha,'evidence':evidence,
            'contract_intervals':m.old.interval_receipts(window,evidence,m.old.pilot.ADDRESSES),
            'payload':{'path':directory+'/payload.parquet','bytes':100,'sha256':'a'*64},
            'request_ledger':{'path':directory+'/REQUESTS.json','bytes':200,'sha256':'b'*64}}
        manifest={'schema_version':f'polymarket_native_collection_manifest_v{version}','status':'raw_collection_complete',
            'run_id':'fixture','contract':contract,'contract_sha256':sha,'windows':[item],'rows':1,
            'distinct_native_keys':1,'collection_complete':True,'raw_payload_only':True,'analysis_admitted':False}
        if version==2:manifest.update(roots=[window],splits=[])
        parent=m.INHERITED if version==1 else m.CAMPAIGN
        attempts=[parent+'/chunks/c000001/a01'] if version==1 else [parent+f'/chunks/c000001/a{a:02d}' for a in (1,2,3)]
        selected=attempts[0 if version==1 else attempt-1];root=selected+'/raw'
        raw=m.encode(manifest);request={'collector_version':version,'chunk':chunk,'rows':1,
            'producer_head':head if version==1 else None,'attempt_candidates':attempts,'manifest_sha256':m.digest(raw)}
        values={root+'/manifest.json':raw,
            root+'/OWNER.json':m.encode({'run_id':'fixture','contract':contract,'contract_sha256':sha}),
            root+'/COMPLETE.json':m.encode({'run_id':'fixture','contract_sha256':sha,'manifest_sha256':m.digest(raw),
                'status':'raw_collection_complete_not_analysis_admission'})}
        if version==2:
            ended=m.encode({'fixture_parent_exit':'accepted'})
            request.update(expected_exit_bytes=len(ended),expected_exit_sha256=m.digest(ended))
            values[selected+'/EXIT.json']=ended
        return request,values

    def test_both_child_versions_derive_payload_ledger_success_and_actual_epoch(self):
        for version in (1,2):
            request,values=self.child(version);seen=[]
            def callback(r):seen.append(r['path']);return values.get(r['path'])
            result=m.resolve_child(request,callback)
            self.assertEqual(len(result['payloads']),1);self.assertEqual(len(result['request_ledgers']),1)
            self.assertEqual(len(result['success_and_split_controls']),1)
            self.assertEqual(result['rows'],1);self.assertFalse(result['full_request_journal_replayed'])
            self.assertFalse(result['production_admitted']);self.assertFalse(any(p.endswith('.parquet') for p in seen))
            self.assertEqual(result['producer_head'],'19202a820bef569f449a34fc731107f16470e155' if version==1 else 'c'*40)

    def test_fresh_child_uses_parent_matching_a02_NOT_assumed_a01(self):
        request,values=self.child(2,2)
        result=m.resolve_child(request,lambda r:values.get(r['path']))
        self.assertIn('/a02/raw',result['root'])
        values[request['attempt_candidates'][0]+'/EXIT.json']=values[request['attempt_candidates'][1]+'/EXIT.json']
        with self.assertRaisesRegex(m.InputBlocked,'unique_parent_matching_child_EXIT_required'):
            m.resolve_child(request,lambda r:values.get(r['path']))

    def test_child_owner_complete_manifest_SHA_and_final_callback_linkage(self):
        for change in ('manifest','owner','complete','epoch','late'):
            request,values=self.child(1);root=request['attempt_candidates'][0]+'/raw';counts={}
            if change=='manifest':values[root+'/manifest.json']+=b' '
            elif change=='owner':value=m.decode(values[root+'/OWNER.json']);value['contract_sha256']='0'*64;values[root+'/OWNER.json']=m.encode(value)
            elif change=='complete':value=m.decode(values[root+'/COMPLETE.json']);value['manifest_sha256']='0'*64;values[root+'/COMPLETE.json']=m.encode(value)
            elif change=='epoch':request['producer_head']='d'*40
            def callback(r):
                path=r['path'];counts[path]=counts.get(path,0)+1
                raw=values.get(path)
                return raw+b' ' if change=='late' and counts[path]>1 else raw
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):m.resolve_child(request,callback)

    def test_nested_payload_escape_or_count_conflict_is_not_accepted(self):
        for change in ('path','count','gap'):
            request,values=self.child(1);root=request['attempt_candidates'][0]+'/raw'
            value=m.decode(values[root+'/manifest.json'])
            if change=='path':value['windows'][0]['payload']['path']='../body.parquet'
            elif change=='count':value['windows'][0]['evidence']['rows']=2
            else:value['windows'][0]['window']['first_block']+=1
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):
                m._windows(value,root,request['chunk']['first_block'],request['chunk']['last_block'],1)

    def test_reader_byte_runtime_and_callback_bounds_no_public_admission_booleans(self):
        with self.assertRaisesRegex(m.InputBlocked,'bounded_reader_bytes'):
            m.Reader(lambda _:b'x'*(m.MAX_CONTROL+1)).read('/mnt/data/control.json')
        with patch.object(m,'MAX_FIXTURE_READ',2),self.assertRaisesRegex(m.InputBlocked,'fixture_control_read_limit'):
            m.Reader(lambda _:b'123').read('/mnt/data/control.json')
        now=[0.0];reader=m.Reader(lambda _:now.__setitem__(0,31) or b'{}',clock=lambda:now[0])
        with self.assertRaisesRegex(m.InputBlocked,'fixture_control_runtime_limit'):reader.read('/mnt/data/control.json')

    def test_strict_nested_window_boolean_count_block_and_contract_epoch_rejected(self):
        for change in ('boolcount','boolblock','intervalbool','wrong_epoch','window_extra'):
            request,values=self.child(1);root=request['attempt_candidates'][0]+'/raw'
            value=m.decode(values[root+'/manifest.json'])
            if change=='boolcount':value['windows'][0]['evidence']['rows']=True
            elif change=='boolblock':value['windows'][0]['window']['blocks']=True
            elif change=='intervalbool':value['windows'][0]['contract_intervals'][0]['rows']=True
            elif change=='wrong_epoch':value['contract']['schema_version']='polymarket_native_collection_contract_v2'
            else:value['windows'][0]['window']['extra']='untrusted'
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):
                m._windows(value,root,request['chunk']['first_block'],request['chunk']['last_block'],1)

    def test_child_request_bool_version_extra_fields_or_duplicate_candidate_set_blocks_before_reader(self):
        request,_=self.child(2)
        for change in ({'collector_version':True},{'authenticated':True},{'rows':True},
                       {'attempt_candidates':[request['attempt_candidates'][0]]*3},
                       {'expected_exit_bytes':True},{'manifest_sha256':'x'*64}):
            with self.subTest(change=change),self.assertRaises(m.InputBlocked):
                m.resolve_child(request|change,lambda _:self.fail('bad request reader'))

    def test_production_guard_then_unconditional_bootstrap_hardblock(self):
        with patch.object(m,'require_production_host',side_effect=RuntimeError('host')):
            with self.assertRaisesRegex(RuntimeError,'host'):m.run_production(authenticated=True)
        with patch.object(m,'require_production_host'),patch.object(m,'prepare_plan',side_effect=AssertionError('I/O')):
            with self.assertRaisesRegex(m.InputBlocked,'production_bootstrap_not_admitted'):m.run_production({'accepted':True})


if __name__=='__main__':unittest.main()
