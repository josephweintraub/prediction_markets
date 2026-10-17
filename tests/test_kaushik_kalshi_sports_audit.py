"""Independent sports QA counterexamples; all sources are tiny temp fixtures."""
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_kaushik_kalshi_sports as audit
from tests import test_kaushik_kalshi_sports as score_fixture
from tests import test_kaushik_kalshi_sports_binding as map_fixture


class ProducerCoreBindingTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory(prefix='sports-core-binding-fixture-')
        self.addCleanup(self.folder.cleanup)
        self.root=Path(self.folder.name).resolve();self.path=self.root/'manifest.json'
        self.path.write_bytes(b'{"fixture":"actual producer JSON binding"}\n')
        self.io=score_fixture.s.io
        self.value,self.record=self.io.read_json(self.path)
        self.patch=patch.object(audit,'DATA_ROOT',self.root);self.patch.start();self.addCleanup(self.patch.stop)

    def test_actual_producer_record_reopens_without_mutating_saved_identity(self):
        original=deepcopy(self.record);bound=audit.Bound()
        self.assertEqual(set(self.record),{'path','sha256','stat'})
        self.assertEqual(audit._bound_core_json(bound,self.record),self.value)
        self.assertEqual(self.record,original)
        self.assertEqual(bound.records()[0]['bytes'],self.record['stat']['bytes'])
        bound.check_drift()

    def test_malformed_stat_and_conflicting_top_level_size_are_rejected(self):
        cases=[]
        for name in ('device','inode','bytes','mtime_ns','ctime_ns'):
            row=deepcopy(self.record);del row['stat'][name];cases.append(row)
            row=deepcopy(self.record);row['stat'][name]=True;cases.append(row)
        row=deepcopy(self.record);row['stat']['bytes']=0;cases.append(row)
        row=deepcopy(self.record);row['stat']['unexpected']=1;cases.append(row)
        row=deepcopy(self.record);row['bytes']=row['stat']['bytes']+1;cases.append(row)
        for row in cases:
            with self.subTest(record_fields=set(row),stat=row.get('stat')),self.assertRaises(audit.AuditBlocked):
                audit._bound_core_json(audit.Bound(),row)

    def test_hash_size_mtime_and_other_recorded_identity_drift_reject(self):
        row=deepcopy(self.record);row['sha256']='f'*64
        with self.assertRaisesRegex(audit.AuditBlocked,'body size/hash differs'):
            audit._bound_core_json(audit.Bound(),row)
        for name in ('bytes','mtime_ns','ctime_ns','device','inode'):
            row=deepcopy(self.record);row['stat'][name]+=1
            with self.subTest(field=name),self.assertRaisesRegex(audit.AuditBlocked,'recorded stat identity differs'):
                audit._bound_core_json(audit.Bound(),row)
        self.path.write_bytes(self.path.read_bytes()+b' ')
        with self.assertRaisesRegex(audit.AuditBlocked,'recorded stat identity differs'):
            audit._bound_core_json(audit.Bound(),self.record)

    def test_same_bytes_mtime_change_during_reopen_is_not_a_pass(self):
        bound=audit.Bound();original=bound.json
        def read_then_change_time(record):
            value=original(record)
            current=self.path.stat()
            os.utime(self.path,ns=(current.st_atime_ns,current.st_mtime_ns+1_000_000))
            return value
        with patch.object(bound,'json',side_effect=read_then_change_time),\
             self.assertRaisesRegex(audit.AuditBlocked,'stat drift during reopen'):
            audit._bound_core_json(bound,self.record)


class IndependentSportsAuditTests(unittest.TestCase):
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)

    def map_stage(self,transform=None):
        fixture=map_fixture.SportsMapStageTests('test_guarded_metadata_stage_exact_proofs_atomic_reopen_no_acceptance')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        if transform is not None:transform(fixture)
        manifest=fixture.run_stage();root=fixture.runs/'sports-map'
        for key,value in {'DATA_ROOT':fixture.root,'RUN_ROOT':fixture.runs,'METADATA_FILES':1,'METADATA_ROWS':1,'MLB_GAMES':1}.items():
            self.stack.enter_context(patch.object(audit,key,value))
        self.stack.enter_context(patch.object(audit,'_historical_source',side_effect=lambda snapshot,head,required:{'head':head,'files':len(snapshot['files'])}))
        return fixture,root,manifest

    def audit_map(self,root):
        con=duckdb.connect();self.addCleanup(con.close)
        return audit.audit_map(con,manifest_path=root/'manifest.json',manifest_sha256=audit.bodies.digest(root/'manifest.json'),expected_producer_head='1'*40)

    def scores(self):
        fixture=score_fixture.NativeSportsTests('test_complete_fresh_grids_and_separate_print_game_support')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        summary=fixture.run_model();root=fixture.root.resolve()/'output'
        outputs={**summary['score_artifacts'],**{'aggregate_'+k:r for k,r in summary['aggregate_artifacts'].items()}}
        return fixture,root,summary,outputs

    def audit_scores(self,fixture,root,summary,outputs):
        con=duckdb.connect();self.addCleanup(con.close)
        con.register('cache_fixture',pa.Table.from_pylist(fixture.prints,schema=score_fixture.arrow_schema(score_fixture.s.CANDIDATE_FIELDS)))
        con.register('map_fixture',pa.Table.from_pylist(fixture.markets,schema=score_fixture.arrow_schema(score_fixture.s.MAP_FIELDS)))
        return audit.audit_game_scores(con,candidates_relation='cache_fixture',map_relation='map_fixture',summary=summary,root=root,outputs=outputs)

    def test_primary_source_map_and_raw_clock_quality_are_independently_reopened(self):
        _,root,manifest=self.map_stage();report,bindings=self.audit_map(root)
        self.assertEqual(report['status'],'sports_map_sources_reconciled')
        self.assertEqual(set(report['checks']),set(audit.MAP_CHECKS));self.assertTrue(all(report['checks'].values()))
        self.assertEqual(report['counts'],manifest['counts']);self.assertFalse(report['raw_tape_read'])
        self.assertEqual(report['proof_modes']['provider_clock_conventions_verified'],audit.CLOCK_PROOF)
        self.assertTrue(any(r['path'].endswith('nfl.parquet') for r in bindings))
        self.assertFalse((root/'acceptance.json').exists())

    def test_mutated_primary_source_is_not_a_receipt_only_pass(self):
        fixture,root,_=self.map_stage()
        path=fixture.source/'nfl.parquet';table=pq.ParquetFile(path).read();rows=table.to_pylist();rows[0]['winning_team_id']='B'
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
        with self.assertRaisesRegex(audit.AuditBlocked,'body size/hash differs'):
            self.audit_map(root)

    def test_provider_winner_is_literal_not_inferred_from_native_yes_label(self):
        fixture,root,manifest=self.map_stage()
        path=root/manifest['artifacts']['binding_proofs']['path'];table=pq.ParquetFile(path).read();proofs=table.to_pylist()
        result=json.loads(proofs[0]['result_receipt_json']);result['provider_result']['winner_participant_id']='B'
        encoded=audit._canonical(result);hashed=hashlib.sha256(encoded.encode()).hexdigest()
        proofs[0].update(result_receipt_json=encoded,result_receipt_sha256=hashed)
        pq.write_table(pa.Table.from_pylist(proofs,schema=table.schema),path)
        manifest['artifacts']['binding_proofs']=audit.bodies._record(root,path,parquet=True)
        path=root/manifest['artifacts']['market_map']['path'];table=pq.ParquetFile(path).read();rows=table.to_pylist()
        rows[0]['result_receipt_sha256']=hashed;provenance=json.loads(rows[0]['provenance']);provenance['result_receipt_sha256']=hashed
        rows[0]['provenance']=json.dumps(provenance,sort_keys=True)
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path);manifest['artifacts']['market_map']=audit.bodies._record(root,path,parquet=True)
        (root/'manifest.json').write_text(json.dumps(manifest))
        with self.assertRaisesRegex(audit.AuditBlocked,'full provider completed/incomplete snapshot receipts differ'):
            self.audit_map(root)

    def test_unknown_or_changed_map_clocks_fail_against_primary_convention(self):
        _,root,original=self.map_stage();path=root/original['artifacts']['market_map']['path']
        snapshot=path.read_bytes();table=pq.ParquetFile(path).read();base=table.to_pylist()
        for mutation in ({'actual_start_seconds':base[0]['actual_start_seconds']+1},{'actual_end_seconds':base[0]['actual_start_seconds']},
                         {'timing_quality':'unreviewed_guessed_actual_clock'}):
            pq.write_table(pa.Table.from_pylist([{**row,**mutation} for row in base],schema=table.schema),path)
            manifest={**original,'artifacts':{**original['artifacts'],'market_map':audit.bodies._record(root,path,parquet=True)}}
            (root/'manifest.json').write_text(json.dumps(manifest))
            with self.subTest(mutation=mutation),self.assertRaisesRegex(audit.AuditBlocked,'clock values/convention'):
                self.audit_map(root)
        path.write_bytes(snapshot)

    def test_unverified_utc_clock_or_opaque_identifier_cannot_be_normalized(self):
        for raw in ('2026-01-02T24:00:00Z','2026-1-02T01:00:00Z','2026-01-02T01:00:00+01:00','2026-01-02T01:00:00.1234567Z'):
            with self.subTest(raw=raw),self.assertRaises(audit.AuditBlocked):audit._utc(raw)
        self.assertTrue(audit._opaque('GDP-232022 Q4'))
        for raw in (' GDP','GDP ','GDP\tQ4','GDP\nQ4','GDP\x7fQ4'):self.assertFalse(audit._opaque(raw))

    def test_atp_first_set_question_is_not_a_full_match_proposition(self):
        labels={'native-A':{'alpha'},'native-B':{'beta'}}
        for title in ('Will Alpha win the first set of the match?','Will Alpha win the first game of the match?',
                      'Will Alpha win the first half of the match?'):
            with self.subTest(title=title),self.assertRaisesRegex(audit.AuditBlocked,'sub-match/period'):
                audit._native_proposition({'title':title,'yes_sub_title':'Alpha'},labels,'atp')
        self.assertEqual(audit._native_proposition({'title':'Alpha wins','yes_sub_title':'Alpha'},labels,'atp'),('named_participant','native-A'))

    def test_malformed_completed_snapshot_cannot_disappear_next_to_valid_result(self):
        def payload(flags,complete=True):
            return {'events':[{'competitions':[{'id':'G1','status':{'type':{'state':'post' if complete else 'in','completed':complete}},
                'competitors':[{'team':{'id':str(i),'displayName':name},'winner':flags[i],'score':str(2-i)}
                    for i,name in enumerate(('Alpha','Beta'))]}]}]}
        source={'path':'fixture.json','sha256':'a'*64}
        good=audit._scoreboard_snapshots(payload((True,False)),'cbb',source)
        bad=audit._scoreboard_snapshots(payload((True,True)),'cbb',source)
        self.assertEqual(audit._reconcile_snapshots(good)['winner_participant_id'],'0')
        with self.assertRaisesRegex(audit.AuditBlocked,'malformed completed'):
            audit._reconcile_snapshots(good+bad)
        incomplete=audit._scoreboard_snapshots(payload((False,False),False),'cbb',source)
        self.assertFalse(incomplete[0]['completed'])
        self.assertEqual(incomplete[0]['reason'],'provider_result_not_completed')
        expected=audit._expected_provider_result(good+incomplete)
        self.assertEqual(len(expected['completed_source_snapshots']),1)
        self.assertEqual(len(expected['ignored_incomplete_source_snapshots']),1)

    def cbb_sources(self,fixture,*,bad_completed):
        fixture.files['native_events']=fixture.parquet(fixture.source/'events.parquet',[map_fixture.event(series='KXNCAAMBGAME')])
        path=fixture.source/'milestones.parquet';rows=pq.ParquetFile(path).read().to_pylist()
        details=json.loads(rows[0]['details']);details['league']='NCAAMB';rows[0]['details']=json.dumps(details)
        fixture.files['native_milestones']=fixture.parquet(path,rows)
        path=fixture.source/'entities.parquet';rows=pq.ParquetFile(path).read().to_pylist()
        for row in rows:row.update(type='basketball_team',details=json.dumps({'league':'NCAAMB'}))
        fixture.files['native_entities']=fixture.parquet(path,rows)
        path=fixture.stage/fixture.metadata['artifacts']['sports_shard']['path'];rows=pq.ParquetFile(path).read().to_pylist();rows[0]['sport']='cbb'
        record=fixture.parquet(path,rows);fixture.metadata['artifacts']['sports_shard']={**record,'path':str(path.relative_to(fixture.stage))}
        fixture.files['metadata_manifest']=fixture.text(fixture.stage/'candidate_manifest.json',fixture.metadata)
        path=fixture.source/'oldproof.parquet';rows=pq.ParquetFile(path).read().to_pylist()
        rows[0].update(provider_participants='Alpha | Beta',provider_event_name='Alpha at Beta')
        proof=fixture.parquet(path,rows)
        path=fixture.source/'binding.json';binding=json.loads(path.read_text())
        binding['inputs']=[{**proof,'role':'six_proof'} if item['role']=='six_proof' else item for item in binding['inputs']]
        fixture.files['provider_binding']=fixture.text(path,binding)
        def competition(flags,complete):
            return {'id':'C1','status':{'type':{'state':'post' if complete else 'in','completed':complete}},'competitors':[
                {'team':{'id':str(i),'displayName':name},'winner':flags[i],'score':str(2-i)} for i,name in enumerate(('Alpha','Beta'))]}
        competitions=[competition((False,False),False),competition((True,False),True)]
        if bad_completed:competitions.append(competition((True,True),True))
        source=fixture.text(fixture.cache/'scoreboards/cbb/20260102.json',{'events':[{'competitions':competitions}]})
        fixture.files['provider_index']=fixture.text(fixture.source/'provider_index.json',{'files':[{**source,'path':'scoreboards/cbb/20260102.json'}]})

    def test_blocked_provider_result_retains_all_primary_failure_and_incomplete_proofs(self):
        _,root,manifest=self.map_stage(lambda fixture:self.cbb_sources(fixture,bad_completed=True))
        report,_=self.audit_map(root)
        self.assertEqual(manifest['counts'],{'source_markets':1,'mapped_markets':0,'excluded_markets':1})
        self.assertEqual(report['provider_failure_receipts_reopened'],1)
        path=root/manifest['artifacts']['exclusions']['path'];table=pq.ParquetFile(path).read();rows=table.to_pylist()
        receipt=json.loads(rows[0]['provider_result_receipt_json']);result=receipt['provider_result']
        self.assertEqual(len(result['completed_source_snapshots']),2)
        self.assertEqual(len(result['ignored_incomplete_source_snapshots']),1)
        self.assertEqual(result['reason'],'contradictory_or_ambiguous_completed_provider_result_snapshot')
        result['completed_source_snapshots'].pop()
        encoded=audit._canonical(receipt);rows[0].update(provider_result_receipt_json=encoded,provider_result_receipt_sha256=hashlib.sha256(encoded.encode()).hexdigest())
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path);manifest['artifacts']['exclusions']=audit.bodies._record(root,path,parquet=True)
        (root/'manifest.json').write_text(json.dumps(manifest))
        with self.assertRaisesRegex(audit.AuditBlocked,'failure full completed/incomplete snapshots differ'):
            self.audit_map(root)

    def test_complete_scoreboard_map_preserves_raw_quality_and_ignored_incomplete(self):
        _,root,manifest=self.map_stage(lambda fixture:self.cbb_sources(fixture,bad_completed=False))
        report,_=self.audit_map(root)
        self.assertEqual(report['counts']['mapped_markets'],1)
        row=pq.ParquetFile(root/manifest['artifacts']['market_map']['path']).read().to_pylist()[0]
        self.assertEqual(row['timing_quality'],'retained_provider_live_play_boundaries | provider_quality=fixture_observed_plays')

    def test_unrelated_exclusion_requires_paired_null_provider_receipt(self):
        def unrelated(fixture):
            path=fixture.stage/fixture.metadata['artifacts']['sports_shard']['path'];rows=pq.ParquetFile(path).read().to_pylist();rows[0]['market_reason']='unresolved_or_nonbinary_result'
            record=fixture.parquet(path,rows);fixture.metadata['artifacts']['sports_shard']={**record,'path':str(path.relative_to(fixture.stage))}
            fixture.files['metadata_manifest']=fixture.text(fixture.stage/'candidate_manifest.json',fixture.metadata)
        _,root,manifest=self.map_stage(unrelated);self.audit_map(root)
        path=root/manifest['artifacts']['exclusions']['path'];table=pq.ParquetFile(path).read();rows=table.to_pylist()
        self.assertIsNone(rows[0]['provider_result_receipt_json']);self.assertIsNone(rows[0]['provider_result_receipt_sha256'])
        rows[0]['provider_result_receipt_sha256']='a'*64;pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
        manifest['artifacts']['exclusions']=audit.bodies._record(root,path,parquet=True);(root/'manifest.json').write_text(json.dumps(manifest))
        with self.assertRaisesRegex(audit.AuditBlocked,'paired-null provider failure receipt'):self.audit_map(root)

    def test_conflicting_completed_scores_and_valid_epl_draw_are_distinct(self):
        base={'sport':'epl','provider_game_id':'G','source_binding':{},'source_pointer':'events[0]','invalid':False,
            'winner_participant_id':None,'is_draw':True,'participants':[{'id':'A','name_options':['Alpha'],'score':'2'},
                {'id':'B','name_options':['Beta'],'score':'2'}]}
        self.assertTrue(audit._reconcile_snapshots([base])['is_draw'])
        bad=deepcopy(base);bad['participants'][0]['score']='3'
        with self.assertRaisesRegex(audit.AuditBlocked,'snapshots conflict'):audit._reconcile_snapshots([base,bad])

    def test_all_game_cells_scores_covariance_and_full_grids_reconcile(self):
        fixture,root,summary,outputs=self.scores()
        result=self.audit_scores(fixture,root,summary,outputs)
        self.assertEqual((result['candidate_rows'],result['eligible_rows'],len(result['joints'])),(2176,2176,30))
        self.assertEqual({r['coefficient_dimension'] for r in result['joints']},{8,40,56})

    def test_independent_means_do_not_take_saved_coefficients_on_faith(self):
        fixture,root,summary,outputs=self.scores()
        joint=summary['joint_artifacts']['sport_nfl_profiles']
        row=next(r for r in joint['estimates'] if not r['suppressed']);row['CR0']['estimate']+=1
        with self.assertRaisesRegex(ValueError,'direct game-cell mean'):
            self.audit_scores(fixture,root,summary,outputs)

    def test_covariance_and_support_are_recomputed_not_accepted_flags(self):
        fixture,root,summary,outputs=self.scores()
        summary['joint_artifacts']['sport_pooled_phases']['covariance_CR0'][0][0]+=1
        with self.assertRaisesRegex(ValueError,'covariance_CR0'):
            self.audit_scores(fixture,root,summary,outputs)

    def test_updated_score_footer_hash_still_cannot_hide_changed_game_score(self):
        fixture,root,summary,outputs=self.scores();key='sport_nfl_profiles'
        record=summary['score_artifacts'][key];path=root/record['path'];table=pq.ParquetFile(path).read()
        values=table.to_pylist();values[0]['projected_score'][0]+=.1
        pq.write_table(pa.Table.from_pylist(values,schema=table.schema),path)
        record.update(audit.bodies._record(root,path,parquet=True),row_groups=pq.ParquetFile(path).metadata.num_row_groups)
        with self.assertRaisesRegex(audit.AuditBlocked,'direct game-centered scores differ'):
            self.audit_scores(fixture,root,summary,outputs)

    def test_changed_aggregate_and_complete_grid_holes_block(self):
        fixture,root,summary,outputs=self.scores();path=root/'phase_totals.parquet'
        table=pq.ParquetFile(path).read();rows=table.to_pylist();rows[0]['sum_payoff']+=1
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
        with self.assertRaisesRegex(audit.AuditBlocked,'game-cell means/counts differ'):
            self.audit_scores(fixture,root,summary,outputs)
        broken=deepcopy(summary);broken['profile_rows'].pop()
        with self.assertRaisesRegex(audit.AuditBlocked,'complete unique sports estimate grid'):
            audit._grid_rows(broken)

    def test_production_guard_precedes_all_source_io(self):
        with patch('production_guard.require_production_host',side_effect=RuntimeError('fixture host denied')):
            with self.assertRaisesRegex(RuntimeError,'host denied'):
                audit.run_production(SimpleNamespace(expected_audit_head='1'*40))

    def test_map_release_is_separate_bound_and_exclusive(self):
        fixture,root,_=self.map_stage();report,bindings=self.audit_map(root)
        target=fixture.runs/'map-audit';target.mkdir();source={'head':'3'*40,'files':{'fixture.py':'4'*64}}
        report=audit._save_report(target,report,bindings,source)
        digest=audit.bodies.digest(target/'audit_candidate.json')
        with patch.object(audit,'_guard',return_value=source),patch.object(audit,'_own_source',return_value=source):
            result=audit.publish_map(audit_dir=target,audit_sha256=digest,expected_audit_head=source['head'])
            self.assertEqual(result['status'],'sports_map_reopened_accepted')
            self.assertEqual(result['manifest_sha256'],report['manifest_sha256'])
            self.assertEqual(result['proof_modes']['provider_clock_conventions_verified'],audit.CLOCK_PROOF)
            with self.assertRaisesRegex(audit.AuditBlocked,'preexisting receipt'):
                audit.publish_map(audit_dir=target,audit_sha256=digest,expected_audit_head=source['head'])

    def test_cancelling_contrast_preserves_zero_score_statistic_and_declared_dots(self):
        fixture,root,summary,outputs=self.scores();joint=summary['joint_artifacts']['sport_pooled_phases']
        checks=audit.numerical.mean_checks(joint,[],[row for row in summary['phase_rows'] if row['scope']=='pooled'],phases=True)
        zero=[row for row,weights,_ in checks if not row['suppressed'] and row['CR0']['standard_error']==0]
        self.assertTrue(zero)
        self.assertTrue(all(row['CR0']['normal_statistic'] is None and row['CR0']['normal_p_value'] is None for row in zero))
        self.audit_scores(fixture,root,summary,outputs)

    def test_saved_score_worker_reopens_complete_native_and_published_map_chain(self):
        fixture,map_root,map_manifest=self.map_stage();report,records=self.audit_map(map_root)
        audit_root=fixture.runs/'map-audit';audit_root.mkdir();source={'head':'3'*40,'files':{'fixture.py':'4'*64}}
        audit._save_report(audit_root,report,records,source)
        with patch.object(audit,'_guard',return_value=source),patch.object(audit,'_own_source',return_value=source):
            audit.publish_map(audit_dir=audit_root,audit_sha256=audit.bodies.digest(audit_root/'audit_candidate.json'),expected_audit_head=source['head'])
        map_path=map_root/map_manifest['artifacts']['market_map']['path'];market=pq.ParquetFile(map_path).read().to_pylist()[0]
        prints=[];day='2026-01-02'
        for elapsed,price in ((-60.,.05),(300.,.95)):
            prints.append({'ticker':'M1','claim_id':'kalshi:claim:M1:yes','event_cluster':'kalshi:event:NATIVE-1','sport':'nfl',
                'timestamp':market['actual_start_seconds']+elapsed,'P':price,'Y':1,'payoff':100*(1-price),'roi':100*(1/price-1),'bin':1 if price<.1 else 10})
        native=fixture.runs/'native-input';folder=native/'cells'/('day='+day);folder.mkdir(parents=True)
        path=folder/'sports_candidates.parquet';pq.write_table(pa.Table.from_pylist(prints,schema=score_fixture.arrow_schema(score_fixture.s.CANDIDATE_FIELDS)),path)
        cache=audit.bodies._record(native/'cells',path,parquet=True)
        receipt_path=folder/'receipt.json';fixture.text(receipt_path,{'schema_version':'kaushik_kalshi_day_cells_v1','status':'durable_pending_global_admission',
            'source_day':day,'sports_candidate_rows':2,'artifacts':{'sports_candidates':cache}})
        index_path=native/'daily_receipts.json';fixture.text(index_path,{'schema_version':'kaushik_kalshi_receipt_inventory_v1','receipts':[audit.bodies._record(native,receipt_path)]})
        input_record=fixture.text(native/'manifest.json',{'schema_version':'kaushik_kalshi_inputs_v1','status':'accepted','counts':{'source_days':1},
            'sports':{'candidate_rows':2},'scientific_contract':{'sample_scope':'all_available_dates','combos':'excluded'},'daily_receipts':audit.bodies._record(native,index_path)})
        accepted_record=fixture.text(native/'acceptance.json',{'schema_version':'kaushik_kalshi_inputs_acceptance_v1','status':'accepted',
            'manifest_sha256':input_record['sha256'],'checks':{key:True for key in audit.bodies.CHECKS}})
        core_root=fixture.runs/'core';core_root.mkdir();core_estimates=fixture.text(core_root/'estimates.json',{'fixture':'separately accepted core; no sports numeric dependency'})
        core=fixture.text(core_root/'manifest.json',{'schema_version':'kaushik_kalshi_estimate_stage_v1','status':'estimates_complete',
            'estimates_sha256':core_estimates['sha256'],'input_binding':{'manifest':input_record}})
        core_acceptance=fixture.text(core_root/'acceptance.json',{'schema_version':'kaushik_kalshi_estimate_acceptance_v1','status':'estimates_reopened_accepted',
            'all_outputs_reopened':True,'manifest_sha256':core['sha256'],'estimates_sha256':core_estimates['sha256']})
        # Match estimate_saved's real shared-reader records; flat fixture-only
        # path/bytes/hash records concealed the production audit mismatch.
        core_binding={role:score_fixture.s.io.read_json(record['path'],record['sha256'])[1]
            for role,record in (('manifest',core),('acceptance',core_acceptance))}
        binding={'input_manifest_sha256':input_record['sha256'],'input_acceptance_sha256':accepted_record['sha256'],
            'map_manifest_sha256':audit.bodies.digest(map_root/'manifest.json'),'map_acceptance_sha256':audit.bodies.digest(map_root/'acceptance.json'),
            'candidate_rows':2,'candidate_files':[{'path':str(path),'bytes':cache['bytes'],'sha256':cache['sha256'],'rows':2}],
            'market_map':{**map_manifest['artifacts']['market_map'],'path':str(map_path)},'timing_qualification':map_manifest['timing_qualification'],
            'core_manifest_sha256':core['sha256'],'core_acceptance_sha256':core_acceptance['sha256'],'core_estimates_sha256':core_estimates['sha256']}
        con=duckdb.connect();self.addCleanup(con.close)
        con.register('candidate_fixture',pa.Table.from_pylist(prints,schema=score_fixture.arrow_schema(score_fixture.s.CANDIDATE_FIELDS)))
        con.register('map_fixture',pq.ParquetFile(map_path).read());sports_root=fixture.runs/'sports';sports_root.mkdir()
        summary=score_fixture.s.estimate_relations(con,candidates_relation='candidate_fixture',market_map_relation='map_fixture',output_dir=sports_root,input_binding=binding)
        outputs={**summary['score_artifacts'],**{'aggregate_'+k:r for k,r in summary['aggregate_artifacts'].items()}}
        sports_manifest=fixture.text(sports_root/'manifest.json',{'schema_version':'kaushik_kalshi_sports_estimate_stage_v1','status':'sports_estimates_complete',
            'source':fixture.snapshot,'sports_sha256':audit.bodies.digest(sports_root/'sports_estimates.json'),'input_binding':binding,
            'core_estimates_sha256':core_estimates['sha256'],'core_binding':core_binding,'outputs':outputs})
        def run():
            connection=duckdb.connect();self.addCleanup(connection.close)
            with patch.object(audit.bodies,'expected_days',return_value={day}):
                return audit.audit_scores(connection,sports_dir=sports_root,sports_manifest_sha256=sports_manifest['sha256'],
                    sports_sha256=audit.bodies.digest(sports_root/'sports_estimates.json'),expected_producer_head=fixture.head,
                    input_manifest_path=native/'manifest.json',input_acceptance_path=native/'acceptance.json',map_manifest_path=map_root/'manifest.json',map_acceptance_path=map_root/'acceptance.json')
        result,bindings=run()
        self.assertEqual(result['status'],'saved_scores_reconciled');self.assertEqual(len(result['details']['joints']),30)
        self.assertTrue(all(result['checks'].values()));self.assertFalse(result['raw_tape_read'])
        self.assertTrue(any(r['path']==str(path) for r in bindings))
        path.write_bytes(path.read_bytes()+b'corrupt fixture body')
        with self.assertRaisesRegex(audit.AuditBlocked,'body size/hash differs'):run()


if __name__=='__main__':unittest.main()
