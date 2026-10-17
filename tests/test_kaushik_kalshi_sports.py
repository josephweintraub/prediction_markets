"""Fresh native fixture claims, exact clocks and independent game-score checks."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_kalshi_replication import sports as s


def arrow_schema(fields):
    return pa.schema([(name,pa.string() if kind=='string' else pa.float64() if kind=='double'
                       else pa.bool_() if kind=='bool' else pa.int64()) for name,kind in fields.items()])


def fixture(games=32,repeats=17):
    markets,prints=[],[]
    for i in range(games):
        ticker='WINNER-'+str(i)
        start=1_700_000_000.+i*86400
        market={'ticker':ticker,'sport':'nfl','game_key':str(i),'actual_start_seconds':start,
            'actual_end_seconds':start+600,'timing_quality':'provider_actual_clock','provenance':'synthetic reviewed provider',
            'yes_payout':i%2,'native_result':'yes' if i%2 else 'no','native_yes_result_verified':True,
            'native_event_ticker':'EVENT-'+str(i),'provider_game_id':'provider-'+str(i),
            'identity_receipt_sha256':'1'*64,'result_receipt_sha256':'2'*64}
        markets.append(market)
        for elapsed in (-60.,300.):
            for price in (.05,.95):
                for _ in range(repeats):
                    y=i%2
                    prints.append({'ticker':ticker,'claim_id':'kalshi:claim:'+ticker+':yes',
                        'event_cluster':'kalshi:event:EVENT-'+str(i),'sport':'nfl','timestamp':start+elapsed,
                        'P':price,'Y':y,'payoff':100*(y-price),'roi':100*(y/price-1),'bin':1 if price<.1 else 10})
    return prints,markets


def write_json(path,value):
    path.write_text(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n')
    return {'path':path.name,'bytes':path.stat().st_size,'sha256':s.io.sha256(path)}


def file_record(path):
    return {'path':path.name,**s.io._output_info(path)}


class NativeSportsTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='native-sports-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.con=duckdb.connect()
        self.addCleanup(self.con.close)
        self.prints,self.markets=fixture()
        self.register()
        self.disk=patch.object(s.estimate.shutil,'disk_usage',return_value=type('Disk',(),{'free':10**15})())
        self.disk.start()
        self.addCleanup(self.disk.stop)

    def register(self):
        self.con.register('candidate_fixture',pa.Table.from_pylist(self.prints,schema=arrow_schema(s.CANDIDATE_FIELDS)))
        self.con.register('map_fixture',pa.Table.from_pylist(self.markets,schema=arrow_schema(s.MAP_FIELDS)))

    def run_model(self):
        output=self.root/'output'
        output.mkdir()
        return s.estimate_relations(self.con,candidates_relation='candidate_fixture',market_map_relation='map_fixture',
            output_dir=output,input_binding={'fixture':'fresh_native_only'})

    def test_complete_fresh_grids_and_separate_print_game_support(self):
        result=self.run_model()
        self.assertEqual((len(result['phase_rows']),len(result['profile_rows']),len(result['window_rows'])),(60,400,280))
        self.assertEqual(len(result['score_artifacts']),30)
        self.assertEqual(set(result['aggregate_artifacts']),{'phase','window'})
        self.assertEqual(result['candidate_rows'],2176)
        self.assertEqual(result['eligible_rows'],2176)
        row=next(row for row in result['profile_rows'] if row['scope']=='nfl' and row['phase']=='pregame'
                 and row['bin']==1 and row['outcome']=='payoff_cents')
        self.assertEqual((row['n_observations'],row['n_clusters']),(544,32))
        self.assertFalse(row['suppressed'])
        self.assertTrue(row['paper_support'] and row['project_support'])
        absent=next(row for row in result['profile_rows'] if row['scope']=='nba')
        self.assertTrue(absent['suppressed'])
        self.assertIsNone(absent['CR0'])
        self.assertIn('empty_cell',absent['suppression_reasons'])
        self.assertEqual(json.loads((self.root/'output'/'sports_estimates.json').read_text()),result)

    def test_joint_game_covariance_matches_independent_native_print_scores(self):
        result=self.run_model()
        saved=result['joint_artifacts']['sport_nfl_phases']
        cells=('D1:pregame','D10:pregame','D1:in_play','D10:in_play')
        vectors=[]
        for target in ('payoff','roi'):
            for cell in cells:
                band,phase=cell.split(':')
                kept=[row for row in self.prints if row['bin']==int(band[1:]) and
                      ('pregame' if row['timestamp']<self.markets[int(row['ticker'].split('-')[-1])]['actual_start_seconds'] else 'in_play')==phase]
                mean=np.mean([row[target] for row in kept])
                vectors.append([sum(row[target]-mean for row in kept if row['ticker']=='WINNER-'+str(game))/len(kept)
                                for game in range(32)])
        scores=np.array(vectors).T
        np.testing.assert_allclose(saved['covariance_CR0'],scores.T@scores,atol=1e-10,rtol=1e-10)
        self.assertNotEqual(saved['covariance_CR0'][0][1],0)
        gap=next(row for row in result['phase_rows'] if row['scope']=='nfl' and row['phase']=='in_play_minus_pregame'
                 and row['outcome']=='roi_percent')
        contrast=np.array([0,0,0,0,1,-1,-1,1])
        self.assertAlmostEqual(gap['CR0']['standard_error']**2,float(np.sum((scores@contrast)**2)),places=7)

    def test_native_claim_outcome_and_map_fanout_contradictions_block(self):
        self.markets.append(deepcopy(self.markets[0]))
        self.register()
        with self.assertRaisesRegex(s.SportsBlocked,'not unique'): self.run_model()

    def test_matched_native_payout_contradiction_blocks_not_excludes(self):
        self.prints[0]['Y']=1-self.prints[0]['Y']
        row=self.prints[0]
        row['payoff'],row['roi']=100*(row['Y']-row['P']),100*(row['Y']/row['P']-1)
        self.register()
        with self.assertRaisesRegex(s.SportsBlocked,'payout contradicts'): self.run_model()

    def test_no_claim_uses_verified_opposite_payout_without_role_inference(self):
        row=self.prints[0]
        row['claim_id']='kalshi:claim:'+row['ticker']+':no'
        row['Y']=1-self.markets[0]['yes_payout']
        row['payoff'],row['roi']=100*(row['Y']-row['P']),100*(row['Y']/row['P']-1)
        self.register()
        result=self.run_model()
        self.assertEqual(result['eligible_rows'],len(self.prints))

    def test_literal_clock_boundaries_no_endpoint_grace_and_missing_map(self):
        self.prints,self.markets=fixture(1,1)
        market=self.markets[0]
        market['actual_end_seconds']=market['actual_start_seconds']+10800
        template=deepcopy(self.prints[0])
        elapsed=(-86401,-86400,-21600,-3600,-900,-.000001,0,900,1800,3600,7200,10800,10800.000001)
        self.prints=[{**template,'timestamp':market['actual_start_seconds']+value} for value in elapsed]
        self.prints.append({**template,'ticker':'NO-MAP','claim_id':'kalshi:claim:NO-MAP:yes'})
        self.register()
        s._prepare(self.con,'candidate_fixture','map_fixture',s.grouped.ReadLedger(),0,s.SportsLimits())
        counts=dict(self.con.execute('SELECT sports_reason,count(*) FROM kk_sports_joined GROUP BY 1').fetchall())
        self.assertEqual(counts,{'eligible':12,'after_actual_end':1,'missing_accepted_map':1})
        self.assertEqual(self.con.execute("SELECT phase FROM kk_sports_base WHERE timestamp=?",[market['actual_start_seconds']]).fetchone()[0],'in_play')
        self.assertEqual(self.con.execute('SELECT count(*) FROM kk_sports_base WHERE timestamp=?',[market['actual_end_seconds']]).fetchone()[0],1)
        at_end=self.con.execute('SELECT clock_window FROM kk_sports_windows WHERE timestamp=? ORDER BY 1',[market['actual_end_seconds']]).fetchall()
        self.assertEqual(at_end,[('final_5to0m',),('live_2hplus',)])
        before=self.con.execute('SELECT clock_window FROM kk_sports_windows WHERE timestamp=?',[market['actual_start_seconds']-86400]).fetchall()
        self.assertEqual(before,[('pre_24to6h',)])

    def test_500_print_guard_is_independent_of_30_game_guard(self):
        self.prints,self.markets=fixture(32,1)
        self.register()
        result=self.run_model()
        row=next(row for row in result['profile_rows'] if row['scope']=='nfl' and row['phase']=='pregame' and row['bin']==1)
        self.assertTrue(row['paper_support'])
        self.assertFalse(row['project_support'])
        self.assertEqual(row['suppression_reasons'],['observation_support_below_floor'])

    def test_30_game_guard_still_applies_above_500_prints(self):
        self.prints,self.markets=fixture(29,18)
        self.register()
        result=self.run_model()
        row=next(row for row in result['profile_rows'] if row['scope']=='nfl' and row['phase']=='pregame' and row['bin']==1)
        self.assertEqual(row['n_observations'],522)
        self.assertTrue(row['project_support'])
        self.assertFalse(row['paper_support'])
        self.assertEqual(row['suppression_reasons'],['cluster_support_below_floor'])

    def test_same_game_clock_conflicts_are_blocked_before_aggregation(self):
        self.markets[1]['game_key']=self.markets[0]['game_key']
        self.register()
        with self.assertRaisesRegex(s.SportsBlocked,'clock/provider identity conflict'): self.run_model()

    def admission_fixture(self):
        root=self.root/'accepted'
        root.mkdir()
        (root/'cells').mkdir()
        path=root/'cells'/'sports_candidates.parquet'
        pq.write_table(pa.Table.from_pylist(self.prints,schema=arrow_schema(s.CANDIDATE_FIELDS)),path)
        record=file_record(path)
        day={'schema_version':'kaushik_kalshi_day_cells_v1','status':'durable_pending_global_admission',
             'source_day':'2026-01-01','sports_candidate_rows':len(self.prints),'artifacts':{'sports_candidates':record}}
        day_record=write_json(root/'receipt.json',day)
        index_record=write_json(root/'daily_receipts.json',{'schema_version':'kaushik_kalshi_receipt_inventory_v1','receipts':[day_record]})
        manifest={'schema_version':'kaushik_kalshi_inputs_v1','status':'accepted','daily_receipts':index_record,
                  'counts':{'source_days':1},'sports':{'candidate_rows':len(self.prints)},
                  'scientific_contract':{'sample_scope':'all_available_dates','combos':'excluded'}}
        m=write_json(root/'manifest.json',manifest)
        receipt={'schema_version':'kaushik_kalshi_inputs_acceptance_v1','status':'accepted','manifest_sha256':m['sha256'],
                 'checks':{key:True for key in s.estimate.REQUIRED_ACCEPTANCE_CHECKS}}
        ac=write_json(root/'acceptance.json',receipt)
        map_root=self.root/'accepted-map'
        map_root.mkdir()
        path=map_root/'map.parquet'
        pq.write_table(pa.Table.from_pylist(self.markets,schema=arrow_schema(s.MAP_FIELDS)),path)
        mm=write_json(map_root/'manifest.json',{'schema_version':'kaushik_kalshi_sports_map_v1','status':'sports_map_complete_pending_independent_audit',
            'artifacts':{'market_map':file_record(path)},'timing_qualification':'fixture verified actual clocks'})
        ma=write_json(map_root/'acceptance.json',{'schema_version':'kaushik_kalshi_sports_map_acceptance_v1',
            'status':'sports_map_reopened_accepted','manifest_sha256':mm['sha256'],'checks':{key:True for key in s.MAP_CHECKS}})
        return {'input_manifest_path':root/'manifest.json','input_manifest_sha256':m['sha256'],
                'input_acceptance_path':root/'acceptance.json','input_acceptance_sha256':ac['sha256'],
                'map_manifest_path':map_root/'manifest.json','map_manifest_sha256':mm['sha256'],
                'map_acceptance_path':map_root/'acceptance.json','map_acceptance_sha256':ma['sha256']}

    def test_exact_accepted_hashes_native_files_and_receipt_admission(self):
        args=self.admission_fixture()
        binding=s.admit_saved_inputs(self.con,**args)
        self.assertEqual(binding['candidate_rows'],len(self.prints))
        self.assertEqual(self.con.execute('SELECT count(*) FROM kk_sports_candidates').fetchone()[0],len(self.prints))
        self.assertEqual(binding['map_manifest_sha256'],args['map_manifest_sha256'])
        self.assertTrue(Path(binding['market_map']['path']).is_absolute())

    def test_identity_candidates_or_pending_result_receipts_never_admitted(self):
        args=self.admission_fixture()
        path=args['map_acceptance_path']
        value=json.loads(path.read_text())
        value['checks']['native_yes_results_verified']=False
        record=write_json(path,value)
        args['map_acceptance_sha256']=record['sha256']
        with self.assertRaisesRegex(s.SportsBlocked,'independently accepted'): s.admit_saved_inputs(self.con,**args)

    def wrapper_fixture(self):
        args=self.admission_fixture()
        core=self.root/'core-estimates'
        core.mkdir()
        body=write_json(core/'estimates.json',{'fixture':'accepted core estimates'})
        manifest=write_json(core/'manifest.json',{'schema_version':'kaushik_kalshi_estimate_stage_v1','status':'estimates_complete',
            'estimates_sha256':body['sha256'],'input_binding':{'manifest':{'sha256':args['input_manifest_sha256']}}})
        accepted=write_json(core/'acceptance.json',{'schema_version':'kaushik_kalshi_estimate_acceptance_v1',
            'status':'estimates_reopened_accepted','all_outputs_reopened':True,'manifest_sha256':manifest['sha256'],
            'estimates_sha256':body['sha256']})
        args.update(core_manifest_path=core/'manifest.json',core_manifest_sha256=manifest['sha256'],
            core_acceptance_path=core/'acceptance.json',core_acceptance_sha256=accepted['sha256'],
            output_dir=self.root/'guarded-output',expected_source_head='a'*40)
        return args

    def test_guarded_wrapper_reopens_and_publishes_only_pending_independent_qa(self):
        args=self.wrapper_fixture()
        with patch.object(s.production_guard,'require_production_host'),patch.object(s.sys,'executable','/home/ubuntu/venv/bin/python'),\
             patch.object(s,'_source_snapshot',return_value={'head':'a'*40,'files':{'fixture':'b'*64}}),\
             patch.object(s,'_available_ram',return_value=32_000_000_000),patch.object(s,'RUN_ROOT',self.root.resolve()):
            result=s.estimate_saved(**args)
        acceptance=json.loads((args['output_dir']/'acceptance.json').read_text())
        self.assertEqual(acceptance['status'],'sports_estimates_reopened_accepted')
        self.assertEqual(acceptance['independent_saved_score_audit'],'pending')
        self.assertTrue(acceptance['all_outputs_reopened'])
        self.assertEqual(len(result['outputs']),32)
        self.assertEqual(result['runtime']['memory_limit'],'16GB')
        self.assertTrue(args['input_manifest_path'].exists())
        with self.assertRaises(FileExistsError):
            s.io.write_json(args['output_dir']/'acceptance.json',{})

    def test_production_guard_precedes_manifest_or_body_io(self):
        with patch.object(s.production_guard,'require_production_host',side_effect=RuntimeError('guard blocked')),\
             patch.object(s.io,'read_json') as unopened:
            with self.assertRaisesRegex(RuntimeError,'guard blocked'):
                s.estimate_saved(input_manifest_path='missing',input_manifest_sha256='1'*64,input_acceptance_path='missing',input_acceptance_sha256='1'*64,
                    map_manifest_path='missing',map_manifest_sha256='1'*64,map_acceptance_path='missing',map_acceptance_sha256='1'*64,
                    core_manifest_path='missing',core_manifest_sha256='1'*64,core_acceptance_path='missing',core_acceptance_sha256='1'*64,
                    output_dir=self.root/'not-created',expected_source_head='a'*40)
            unopened.assert_not_called()
        self.assertFalse((self.root/'not-created').exists())


if __name__=='__main__':
    unittest.main()
