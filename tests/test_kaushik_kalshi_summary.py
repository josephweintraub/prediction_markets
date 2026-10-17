"""Small accepted-cell summary fixtures; no network, production data or outputs."""
from datetime import datetime,timezone
from pathlib import Path
import unittest
from unittest.mock import patch

import duckdb

from analysis.kaushik_kalshi_replication import summary as reader
from scripts import audit_kaushik_kalshi_inputs as audit
from tests import test_kaushik_kalshi_input_audit as audit_fixtures


class KalshiReaderSummaryTests(unittest.TestCase):
    def fixture(self):
        fixture=audit_fixtures.KalshiInputAuditTests('test_independent_raw_replay_then_separate_publication')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        pipeline,trade_dir,metadata_dir=fixture.make_inputs()
        _,audit_dir=fixture.run_replay(trade_dir)
        audit.publish_acceptance(input_dir=trade_dir,candidate_sha256=audit.digest(trade_dir/'candidate_manifest.json'),
            audit_dir=audit_dir,audit_sha256=audit.digest(audit_dir/'audit_candidate.json'),expected_audit_head='b'*40)
        return fixture,pipeline,trade_dir,metadata_dir

    def args(self,trade_dir):
        return dict(manifest_path=trade_dir/'manifest.json',manifest_sha256=audit.digest(trade_dir/'manifest.json'),
            acceptance_path=trade_dir/'acceptance.json',acceptance_sha256=audit.digest(trade_dir/'acceptance.json'),expected_input_source_head='a'*40)

    def con(self):
        con=duckdb.connect();self.addCleanup(con.close);return con

    def test_accepted_summary_counts_times_coverage_and_exact_body_provenance(self):
        _,pipeline,trade_dir,metadata_dir=self.fixture()
        before={path:audit.digest(path) for path in pipeline.root.rglob('*') if path.is_file()}
        con=self.con()
        result,provenance=reader.build_reader_metadata(con,**self.args(trade_dir))
        self.assertEqual({key:result[key] for key in ('source_rows','baseline_rows','tail_rows','duration_tail_rows','markets','normalized_claims','clusters','unique_event_clusters','market_fallback_clusters')},
            {'source_rows':6,'baseline_rows':6,'tail_rows':4,'duration_tail_rows':4,'markets':1,'normalized_claims':1,'clusters':1,'unique_event_clusters':1,'market_fallback_clusters':0})
        self.assertEqual(result['first_execution_utc'],'2026-03-25T12:00:00.000000Z')
        self.assertEqual(result['last_execution_utc'],'2026-03-26T12:00:00.000000Z')
        self.assertEqual(result['categories'],[{'category':'Sports','n_observations':6,'share':1.0}])
        self.assertEqual(result['claim_support'],{'claims':1,'observations':4,'both_tail_claims':1,'both_tail_claim_observations':4})
        self.assertEqual(sum(row['rows'] for row in result['duration_exclusions']),4)
        self.assertEqual(dict((row['reason'],row['rows']) for row in result['duration_exclusions'])['eligible'],4)
        coverage={row['source_version']:row for row in result['duration_coverage']}
        self.assertEqual(coverage['bulk_v2'],{'source_version':'bulk_v2','tail_rows':4,'duration_tail_rows':4,'first_utc_day':'2026-03-25','last_utc_day':'2026-03-26'})
        self.assertEqual((coverage['forward']['duration_tail_rows'],coverage['forward']['first_utc_day'],coverage['forward']['last_utc_day']),(0,None,None))
        self.assertFalse(provenance['raw_tape_read']);self.assertFalse(provenance['saved_file_writes'])
        reopened={row['path'] for row in provenance['body_bindings']}
        self.assertIn(str(metadata_dir/'candidate_manifest.json'),reopened)
        self.assertIn(str(trade_dir/'cells/day=2026-03-25/claims.parquet'),reopened)
        self.assertFalse(reopened & {str(path) for path in pipeline.paths.values()})
        self.assertEqual(before,{path:audit.digest(path) for path in pipeline.root.rglob('*') if path.is_file()})
        self.assertEqual(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'reader_%'").fetchone()[0],0)

    def test_unaccepted_manifest_and_body_drift_fail_closed(self):
        _,_,trade_dir,_=self.fixture()
        body=trade_dir/'cells/final/claims.parquet'
        body.write_bytes(body.read_bytes()+b'changed')
        con=self.con()
        with self.assertRaisesRegex(reader.SummaryBlocked,'Parquet size/hash'):
            reader.build_reader_metadata(con,**self.args(trade_dir))
        self.assertEqual(con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'reader_%'").fetchone()[0],0)

    def test_guard_precedes_any_saved_body_read(self):
        with patch.object(reader.production_guard,'require_production_host',side_effect=RuntimeError('guard blocked')), \
             patch.object(reader,'_canonical') as unopened:
            with self.assertRaisesRegex(RuntimeError,'guard blocked'):
                reader.build_reader_metadata(None,manifest_path=Path('/never/read'),manifest_sha256='a'*64,
                    acceptance_path=Path('/never/read2'),acceptance_sha256='b'*64,expected_input_source_head='c'*40)
            unopened.assert_not_called()

    def test_preexisting_temp_names_are_preserved(self):
        _,_,trade_dir,_=self.fixture()
        con=self.con();con.execute('CREATE TEMP TABLE reader_metadata(value INTEGER)');con.execute('INSERT INTO reader_metadata VALUES(7)')
        with self.assertRaisesRegex(reader.SummaryBlocked,'already exists'):
            reader.build_reader_metadata(con,**self.args(trade_dir))
        self.assertEqual(con.execute('SELECT * FROM reader_metadata').fetchall(),[(7,)])

    def metrics_fixture(self):
        con=self.con()
        con.execute('CREATE TABLE reader_baseline(event_cluster VARCHAR,bin TINYINT,n_rows BIGINT)')
        con.execute("INSERT INTO reader_baseline VALUES('kalshi:event:GDP-232022 Q4',1,9),('kalshi:market:FALLBACK',10,1)")
        con.execute('CREATE TABLE reader_duration(event_cluster VARCHAR,n_rows BIGINT,lifespan_gt1 BOOLEAN,remaining_gt1 BOOLEAN)')
        con.execute("INSERT INTO reader_duration VALUES('kalshi:event:GDP-232022 Q4',2,true,false)")
        con.execute('CREATE TABLE reader_claims(claim_id VARCHAR,ticker VARCHAR,category VARCHAR,n_rows BIGINT,n_d1 BIGINT,n_d10 BIGINT,duration_rows BIGINT)')
        con.execute("INSERT INTO reader_claims VALUES('a','a','Economics',9,9,0,2),('b','b','Economics',1,0,1,0)")
        con.execute('CREATE TABLE reader_markets(ticker VARCHAR)');con.execute("INSERT INTO reader_markets VALUES('a'),('b')")
        con.execute('CREATE TABLE reader_categories(category VARCHAR,n_rows BIGINT)');con.execute("INSERT INTO reader_categories VALUES('Economics',10)")
        con.execute('CREATE TABLE reader_claim(claim_fe_code BIGINT,n_rows BIGINT,a0 DOUBLE)');con.execute('INSERT INTO reader_claim VALUES(0,2,0)')
        con.execute('CREATE TABLE reader_native_day_claims(source_version VARCHAR,source_day VARCHAR,n_rows BIGINT,n_d1 BIGINT,n_d10 BIGINT,duration_rows BIGINT,opening_seconds DOUBLE,endpoint_seconds DOUBLE)')
        con.execute("""INSERT INTO reader_native_day_claims VALUES
          ('bulk_v2','2026-03-20',2,2,0,0,NULL,NULL),
          ('bulk_v2','2026-03-21',2,2,0,0,1,NULL),
          ('forward','2026-03-22',2,2,0,0,2,1),
          ('bulk_v2','2026-03-25',3,3,0,2,1,9),
          ('forward','2026-03-26',1,0,1,0,1,9)""")
        low=datetime(2026,3,20,tzinfo=timezone.utc).timestamp();high=datetime(2026,3,26,tzinfo=timezone.utc).timestamp()
        manifest={'counts':{'source_rows':12,'baseline_rows':10,'duration_tail_rows':2},'sample_metadata':{
            'baseline_execution_seconds_min':low,'baseline_execution_seconds_max':high,
            'duration_tail_execution_seconds_min':low+5*86400,'duration_tail_execution_seconds_max':low+5*86400,
            'execution_seconds_min':0,'execution_seconds_max':high+10*86400}}
        reasons={'eligible':2,'missing_or_invalid_open_time':2,'missing_or_invalid_settlement_ts':2,'nonpositive_lifespan':2,'outside_recorded_open_settlement_interval':2}
        return con,manifest,{'source_version_policy':{'bulk_v2':{},'forward':{},'event_v1':{}}},reasons

    def test_tail_clock_partition_and_source_coverage_are_not_all_band_counts(self):
        con,manifest,meta,reasons=self.metrics_fixture()
        result=reader._summarize(con,manifest,meta,12,{'eligible':10,'invalid_size':2},reasons)
        self.assertEqual({row['reason']:row['rows'] for row in result['duration_exclusions']},reasons)
        self.assertEqual(result['first_execution_utc'],'2026-03-20T00:00:00.000000Z')
        self.assertEqual(result['archive_observed_first_execution_utc'],'1970-01-01T00:00:00.000000Z')
        coverage={row['source_version']:row for row in result['duration_coverage']}
        self.assertEqual((coverage['bulk_v2']['tail_rows'],coverage['bulk_v2']['duration_tail_rows']), (7,2))
        self.assertEqual((coverage['bulk_v2']['first_utc_day'],coverage['bulk_v2']['last_utc_day']),('2026-03-25','2026-03-25'))
        self.assertEqual((coverage['forward']['tail_rows'],coverage['forward']['duration_tail_rows'],coverage['forward']['first_utc_day']), (3,0,None))
        self.assertEqual((result['unique_event_clusters'],result['market_fallback_clusters']),(1,1))

    def test_missing_baseline_extrema_never_fall_back_to_archive(self):
        con,manifest,meta,reasons=self.metrics_fixture()
        manifest['sample_metadata']['baseline_execution_seconds_min']=None
        with self.assertRaisesRegex(reader.SummaryBlocked,'baseline execution bounds unavailable'):
            reader._summarize(con,manifest,meta,12,{'eligible':10,'invalid_size':2},reasons)

    def test_clock_and_saved_tail_reason_disagreement_blocks(self):
        con,manifest,meta,reasons=self.metrics_fixture()
        reasons['missing_or_invalid_open_time']-=1
        with self.assertRaisesRegex(reader.SummaryBlocked,'tail-only'):
            reader._summarize(con,manifest,meta,12,{'eligible':10,'invalid_size':2},reasons)


if __name__=='__main__':unittest.main()
