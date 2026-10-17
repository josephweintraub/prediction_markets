"""Tiny Parquet/SQL oracle fixtures; no production sources or network."""
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
import uuid

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_kalshi_replication import build as b
from analysis.kaushik_kalshi_replication import estimate
from analysis.kaushik_kalshi_replication import inputs as k
from tests.test_kaushik_kalshi_inputs import market_raw, prepare, trade
from tests.test_kaushik_kalshi_estimate import dense_oracle


SCHEMA = pa.schema([(name, pa.date32() if kind=='date' else pa.float64() if kind=='double'
                     else pa.bool_() if kind=='bool' else pa.string())
                    for name,kind in {**k.TRADE_FIELDS,**k.OPTIONAL_TRADE_FIELDS}.items()])


class KalshiBuildTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='kalshi-build-fixture-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.raw_dir=self.root/'raw'
        self.out=self.root/'out'
        self.raw_dir.mkdir()
        self.out.mkdir()
        self.con=duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("SET TimeZone='UTC'")
        b.initialize_ids(self.con)
        self.disk=patch.object(b.shutil,'disk_usage',return_value=SimpleNamespace(free=200_000_000_000))
        self.disk.start()
        self.addCleanup(self.disk.stop)
        self.market=prepare()
        self.metadata([self.market])

    def metadata(self, prepared):
        rows=[{**{name:item[name] for name in ('ticker','event_cluster','category','opening_seconds','endpoint_seconds','result')},
               'market_reason':item['eligibility_reason']} for item in prepared]
        schema=pa.schema([(name,pa.float64() if name.endswith('_seconds') else pa.string())
                          for name in ('ticker','event_cluster','category','market_reason','opening_seconds','endpoint_seconds','result')])
        self.con.register('prepared_metadata',pa.Table.from_pylist(rows,schema=schema))

    def raw(self, rows, day='2026-03-25', *, start_id=1, keep_ids=False, schema=SCHEMA):
        values=[]
        for i,row in enumerate(rows):
            value=dict(row)
            if not keep_ids:
                value['trade_id']=str(uuid.UUID(int=start_id+i))
            values.append(value)
        path=self.raw_dir/(day+'.parquet')
        pq.write_table(pa.Table.from_pylist(values,schema=schema),path)
        return path,values

    def process(self, rows, day='2026-03-25', start_id=1, **options):
        path,values=self.raw(rows,day,start_id=start_id)
        receipt=b.process_day(self.con,path,day,self.out,expected_rows=len(rows),**options)
        return receipt,values

    def test_sql_matches_pure_first_failure_oracle(self):
        combo=prepare(market_raw(ticker='COMBO'),combo_state='combo')
        missing=prepare(market_raw(ticker='NO-CLOCK',settlement_ts=None))
        self.metadata([self.market,combo,missing])
        rows=[trade(),trade(taker_side='no'),trade(yes_price=.1,no_price=.9),
              trade(yes_price=.009,no_price=.991),trade(yes_price=.999,no_price=.001),
              trade(ticker='COMBO'),trade(ticker='NO-CLOCK'),trade(ticker='UNMATCHED'),
              trade(size=0),trade(size=float('inf')),trade(yes_price=float('nan')),
              trade(yes_price=.2),trade(created_time='bad'),trade(created_time='2026-03-25T01:00:00+01:00'),
              trade(created_time='2026-03-25T00:00:00'),trade(trade_date=date(2026,3,24)),
              trade(created_time='2026-3-25T12:34:56Z'),trade(created_time='2026-03-25T24:00:00Z'),
              trade(created_time='2026-03-25T12:34:56.1234567Z'),trade(created_time='2026-02-30T12:34:56Z'),
              trade(created_time='2026-03-25 12:34:56+00:00'),trade(created_time='2026-03-25T12:34:56.123456-00:00'),
              trade(taker_side='bad'),trade(taker_outcome_side='no'),trade(taker_outcome_side='YES'),
              trade(taker_book_side='ask'),trade(taker_book_side='buy'),
              trade(taker_outcome_side='yes',taker_book_side='bid'),trade(ticker=' invalid '),
              trade(ticker='space\u00a0ticker')]
        path,values=self.raw(rows)
        b.annotate_day(self.con,path,'2026-03-25',expected_rows=len(rows))
        columns=('eligibility_reason','duration_reason','P','Y','bin','claim_id','L','R','xL','xR')
        sql=self.con.execute('SELECT '+','.join(columns)+' FROM kk_annotated ORDER BY source_ordinal').fetchall()
        lookup={item['ticker']:item for item in (self.market,combo,missing)}
        for ordinal,(raw,actual) in enumerate(zip(values,sql)):
            expected=k.normalize_trade(raw,lookup.get(raw['ticker']),source_date='2026-03-25',source_ordinal=ordinal)
            with self.subTest(ordinal=ordinal):
                for name,value in zip(columns,actual):
                    if isinstance(expected[name],float):
                        self.assertAlmostEqual(value,expected[name],places=9)
                    else:
                        self.assertEqual(value,expected[name],name)
        self.assertEqual(b.finalize_ids(self.con,len(rows))['distinct_native_trade_ids'],len(rows))

    def test_canonical_uuid_gate_applies_to_excluded_rows(self):
        raw=trade(size=0,trade_id='ABCDEF00-0000-0000-0000-000000000001')
        path,_=self.raw([raw],keep_ids=True)
        with self.assertRaisesRegex(b.BuildBlocked,'canonical UUID'):
            b.annotate_day(self.con,path,'2026-03-25',expected_rows=1)
        self.assertEqual(self.con.execute('SELECT count(*) FROM native_ids').fetchone()[0],0)

    def test_opaque_native_identifier_spaces_match_oracle_without_key_rewriting(self):
        ticker='GDP-232022 Q4-YES'
        market=prepare(market_raw(ticker=ticker,event_ticker='GDP-232022 Q4'),category='Economics')
        self.metadata([market])
        invalid=[' '+ticker,ticker+' ',ticker.replace(' ','\t'),ticker.replace(' ','\u00a0'),
                 ticker.replace(' ','\x00'),ticker.replace(' ','\x7f')]
        rows=[trade(ticker=ticker)]+[trade(ticker=value) for value in invalid]
        path,values=self.raw(rows)
        b.annotate_day(self.con,path,'2026-03-25',expected_rows=len(rows))
        columns=('eligibility_reason','duration_reason','P','Y','bin','claim_id','event_cluster')
        actual=self.con.execute('SELECT '+','.join(columns)+' FROM kk_annotated ORDER BY source_ordinal').fetchall()
        for ordinal,(raw,row) in enumerate(zip(values,actual)):
            expected=k.normalize_trade(raw,market if raw['ticker']==ticker else None,
                                       source_date='2026-03-25',source_ordinal=ordinal)
            with self.subTest(ordinal=ordinal):
                self.assertEqual(row,tuple(expected[name] for name in columns))
        self.assertEqual(actual[0][0],'eligible')
        self.assertEqual(actual[0][-1],'kalshi:event:GDP-232022 Q4')
        self.assertEqual(b.finalize_ids(self.con,len(rows))['distinct_native_trade_ids'],len(rows))

    def test_exact_id_gate_catches_crossday_duplicate_among_exclusions(self):
        day1,_=self.raw([trade()],start_id=1)
        b.annotate_day(self.con,day1,'2026-03-25',expected_rows=1)
        day2,_=self.raw([trade(created_time='2026-03-26T00:00:00Z',trade_date=date(2026,3,26),size=0)],
                        '2026-03-26',start_id=1)
        b.annotate_day(self.con,day2,'2026-03-26',expected_rows=1)
        with self.assertRaisesRegex(b.BuildBlocked,'duplicate native'):
            b.finalize_ids(self.con,2)
        with self.assertRaisesRegex(b.BuildBlocked,'conservation'):
            b.finalize_ids(self.con,1)

    def test_schema_footer_and_join_gates(self):
        path,_=self.raw([trade()])
        with self.assertRaisesRegex(b.BuildBlocked,'footer count'):
            b.annotate_day(self.con,path,'2026-03-25',expected_rows=2)
        self.metadata([self.market,self.market])
        with self.assertRaisesRegex(b.BuildBlocked,'fanout'):
            b.annotate_day(self.con,path,'2026-03-25',expected_rows=1)
        wrong=pa.schema([(field.name,pa.int64() if field.name=='size' else field.type) for field in SCHEMA])
        path,_=self.raw([trade(size=1)],schema=wrong)
        with self.assertRaisesRegex(k.InputBlocked,'type differs'):
            b.annotate_day(self.con,path,'2026-03-25',expected_rows=1)

    def test_day_cells_restore_direct_raw_crossproducts(self):
        rows=[trade(created_time='2026-03-25T01:00:00Z'),trade(created_time='2026-03-25T03:00:00Z'),
              trade(created_time='2026-03-25T04:00:00Z',yes_price=.95,no_price=.05),
              trade(created_time='2026-03-25T05:00:00Z',yes_price=.95,no_price=.05),trade(size=0)]
        receipt,raw=self.process(rows)
        self.assertEqual((receipt['source_rows'],receipt['baseline_rows'],receipt['duration_rows']), (5,4,4))
        self.assertEqual(sum(item['rows'] for item in receipt['exclusions']),5)
        self.assertEqual(receipt['artifacts']['exclusions']['rows'],1)
        self.assertTrue((self.out/'day=2026-03-25'/'receipt.json').is_file())
        actual=[k.normalize_trade(row,self.market,source_date='2026-03-25',source_ordinal=i) for i,row in enumerate(raw)]
        kept=[row for row in actual if row['duration_eligible']]
        for kind,width,features in (('duration_raw',4,lambda r:[r['xL'],r['xR'],r['payoff'],r['roi']]),
                                     ('claim_raw',6,lambda r:[float(r['bin']==10),float(r['bin']==10)*r['xL'],r['xR'],float(r['bin']==10)*r['xR'],r['payoff'],r['roi']])):
            cells=pq.read_table(self.out/receipt['artifacts'][kind]['path']).to_pylist()
            restored=np.zeros((width,width))
            total=np.zeros(width)
            for cell in cells:
                mean=np.array([cell['a'+str(i)]/cell['n_rows'] for i in range(width)])
                total+=cell['n_rows']*mean
                for i in range(width):
                    for j in range(i,width):
                        value=cell['n_rows']*mean[i]*mean[j]+cell[f'c{i}_{j}']
                        restored[i,j]+=value
                        if i!=j: restored[j,i]+=value
            direct=np.array([features(row) for row in kept])
            np.testing.assert_allclose(total,direct.sum(axis=0),rtol=1e-12,atol=1e-10)
            np.testing.assert_allclose(restored,direct.T@direct,rtol=1e-12,atol=1e-8)
        baseline=pq.read_table(self.out/receipt['artifacts']['baseline']['path']).to_pylist()
        self.assertAlmostEqual(sum(row['sum_payoff'] for row in baseline),sum(row['payoff'] for row in kept))

    def test_partial_groups_survive_global_codes_and_match_estimator_schema(self):
        day1,_=self.process([trade(),trade(created_time='2026-03-25T02:00:00Z')])
        day2,_=self.process([trade(created_time='2026-03-26T01:00:00Z',trade_date=date(2026,3,26)),
                             trade(created_time='2026-03-26T02:00:00Z',trade_date=date(2026,3,26),yes_price=.95,no_price=.05)],
                            '2026-03-26',start_id=3)
        summary=b.finalize_cells(self.con,self.out,[day1,day2],expected_source_rows=4)
        self.assertEqual(summary['baseline_rows'],4)
        self.assertEqual(summary['duration_rows'],4)
        self.assertEqual(summary['artifacts']['duration']['rows'],3)
        for name in ('baseline','duration','claim'):
            schema=pq.ParquetFile(self.out/summary['artifacts'][name]['path']).schema_arrow
            estimate._check_schema(schema,name)
        for family in ('cat','price','month'):
            items=pq.read_table(self.out/summary['artifacts'][family+'_dictionary']['path']).to_pylist()
            self.assertEqual(sorted(row[family+'_code'] for row in items),list(range(len(items))))
            self.assertEqual({row['tail'] for row in items},{0,1})
        claims=pq.read_table(self.out/summary['artifacts']['claims']['path']).to_pylist()
        self.assertEqual(len(claims),1)
        self.assertEqual((claims[0]['n_d1'],claims[0]['n_d10']), (3,1))
        with self.assertRaisesRegex(b.BuildBlocked,'immutable final'):
            b.finalize_cells(self.con,self.out,[day1,day2],expected_source_rows=4)

    def test_claim_payout_changes_between_days_fail_closed(self):
        day1,_=self.process([trade()])
        self.metadata([prepare(market_raw(result='no'))])
        day2,_=self.process([trade(created_time='2026-03-26T00:00:00Z',trade_date=date(2026,3,26))],
                            '2026-03-26',start_id=2)
        with self.assertRaisesRegex(b.BuildBlocked,'claim event/category/payout'):
            b.finalize_cells(self.con,self.out,[day1,day2],expected_source_rows=2)

    def test_output_capacity_fails_without_releasing_source(self):
        path,_=self.raw([trade()])
        with patch.object(b.shutil,'disk_usage',return_value=SimpleNamespace(free=1)):
            with self.assertRaisesRegex(b.BuildBlocked,'free-floor'):
                b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1)
        self.assertTrue(path.is_file())
        self.assertFalse((self.out/'day=2026-03-25'/'receipt.json').exists())

    def test_hard_artifact_ceiling_preserves_failed_file_and_source(self):
        path,_=self.raw([trade()])
        limits=b.BuildLimits(output_bytes=100_000,artifact_bytes=128,transient_bytes=100_000,json_bytes=64)
        with self.assertRaisesRegex(b.BuildBlocked,'artifact exceeds'):
            b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1,limits=limits)
        self.assertTrue(path.is_file())
        self.assertTrue((self.out/'day=2026-03-25'/'baseline.parquet').is_file())

    def test_sports_candidates_require_explicit_unique_mapping(self):
        self.con.register('sports_map',pa.table({'ticker':[self.market['ticker']],'sport':['nba']}))
        receipt,_=self.process([trade(),trade(size=0)],sports_candidates_relation='sports_map')
        self.assertEqual(receipt['sports_candidate_rows'],1)
        rows=pq.read_table(self.out/receipt['artifacts']['sports_candidates']['path']).to_pylist()
        self.assertEqual(rows[0]['sport'],'nba')
        self.assertNotIn('game_id',rows[0])
        self.assertEqual(receipt['sports_candidate_admission'],'unbound candidates only; no game/timing admission')

    def test_unknown_metadata_exclusions_keep_empty_cells_schema(self):
        receipt,_=self.process([trade(ticker='UNMATCHED')])
        summary=b.finalize_cells(self.con,self.out,[receipt],expected_source_rows=1)
        self.assertEqual((summary['baseline_rows'],summary['duration_rows']), (0,0))
        for name in ('baseline','duration','claim'):
            info=summary['artifacts'][name]
            self.assertEqual(info['rows'],0)
            estimate._check_schema(pq.ParquetFile(self.out/info['path']).schema_arrow,name)

    def test_legacy_schema_without_optional_fields_and_src_audit(self):
        legacy=pa.schema([field for field in SCHEMA if field.name in k.TRADE_FIELDS])
        path,_=self.raw([trade()],schema=legacy)
        receipt=b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1)
        self.assertFalse(receipt['source_src_audit']['field_present'])
        self.assertNotIn('sports_candidates',receipt['artifacts'])
        count=self.con.execute('SELECT count(*) FROM native_ids').fetchone()[0]
        with self.assertRaisesRegex(b.BuildBlocked,'immutable daily'):
            b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1)
        self.assertEqual(self.con.execute('SELECT count(*) FROM native_ids').fetchone()[0],count)

    def test_src_disagreement_is_audit_only(self):
        receipt,_=self.process([trade(src='different')])
        self.assertEqual(receipt['baseline_rows'],1)
        self.assertEqual(receipt['source_src_audit']['disagreement_rows'],1)

    def test_native_market_clock_drift_fails_global_invariants(self):
        day1,_=self.process([trade()])
        changed=market_raw()
        changed['settlement_ts']+=86400
        self.metadata([prepare(changed)])
        day2,_=self.process([trade(created_time='2026-03-26T00:00:00Z',trade_date=date(2026,3,26))],
                            '2026-03-26',start_id=2)
        with self.assertRaisesRegex(b.BuildBlocked,'market clocks'):
            b.finalize_cells(self.con,self.out,[day1,day2],expected_source_rows=2)

    def test_duplicate_sports_candidate_mapping_fails_without_guessing(self):
        self.con.register('sports_map',pa.table({'ticker':[self.market['ticker']]*2,'sport':['nba','nba']}))
        path,_=self.raw([trade()])
        with self.assertRaisesRegex(b.BuildBlocked,'ticker is not unique'):
            b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1,sports_candidates_relation='sports_map')
        self.assertTrue(path.is_file())

    def test_daily_cache_preserves_bits_and_all_outputs_reuse_it(self):
        path,_=self.raw([trade(taker_side='no',yes_price=.8999999999999999,no_price=.1,src='catchup1')])
        before_hash=b.io.sha256(path)
        materialize=b._materialize_daily_cache
        observed=[]
        def cached(con,n):
            before=con.execute('SELECT P,xL,xR,payoff,roi FROM kk_annotated ORDER BY source_ordinal').fetchall()
            info=materialize(con,n)
            after=con.execute('SELECT P,xL,xR,payoff,roi FROM kk_annotated ORDER BY source_ordinal').fetchall()
            self.assertEqual([[None if x is None else float(x).hex() for x in row] for row in before],
                             [[None if x is None else float(x).hex() for x in row] for row in after])
            # Remove every lazy raw-source/join view. Daily output queries must
            # succeed entirely from the owned materialized relation.
            for name in ('kk_measures','kk_checked','kk_joined','kk_source'):
                con.execute('DROP VIEW '+name)
            observed.append(info['rows'])
            return info
        with patch.object(b,'_materialize_daily_cache',side_effect=cached):
            receipt=b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1)
        self.assertEqual(observed,[1])
        self.assertEqual(receipt['daily_cache']['rows'],1)
        self.assertEqual(b.io.sha256(path),before_hash)
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],0)

    def test_failed_callback_releases_cache_and_cannot_implicitly_retry(self):
        path,_=self.raw([trade()])
        limits=b.BuildLimits(output_bytes=100_000,artifact_bytes=128,transient_bytes=100_000,json_bytes=64)
        with self.assertRaisesRegex(b.BuildBlocked,'artifact exceeds'):
            b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1,limits=limits)
        self.assertTrue(path.is_file())
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],0)
        count=self.con.execute('SELECT count(*) FROM native_ids').fetchone()[0]
        with self.assertRaisesRegex(b.BuildBlocked,'immutable daily'):
            b.process_day(self.con,path,'2026-03-25',self.out,expected_rows=1)
        self.assertEqual(self.con.execute('SELECT count(*) FROM native_ids').fetchone()[0],count)
        receipt,_=self.process([trade(created_time='2026-03-26T00:00:00Z',trade_date=date(2026,3,26))],
                                '2026-03-26',start_id=2)
        self.assertEqual(receipt['daily_cache']['rows'],1)
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],0)

    def test_cache_cleanup_refuses_views_and_unowned_tables(self):
        self.con.execute('CREATE TEMP VIEW kk_daily_cache AS SELECT 1 AS cache_value')
        with self.assertRaisesRegex(b.BuildBlocked,'is a view'):
            b.release_daily_cache(self.con)
        self.assertEqual(self.con.execute('SELECT * FROM kk_daily_cache').fetchone()[0],1)
        self.con.execute('DROP VIEW kk_daily_cache')
        self.con.execute('CREATE TEMP TABLE kk_daily_cache(cache_value INTEGER)')
        with self.assertRaisesRegex(b.BuildBlocked,'owned TEMP TABLE'):
            b.finalize_ids(self.con,0)
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],1)

    def test_commented_cache_uses_authoritative_temp_database_identity(self):
        self.con.execute('CREATE TEMP TABLE kk_daily_cache(cache_value INTEGER)')
        self.con.execute('COMMENT ON TABLE temp.main.kk_daily_cache IS '+b.io.literal(b._DAILY_CACHE_OWNER))
        row=self.con.execute("SELECT database_name,schema_name,temporary,comment FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()
        self.assertEqual((row[0],row[1],row[3]),('temp','main',b._DAILY_CACHE_OWNER))
        if duckdb.__version__=='1.4.4':
            self.assertFalse(row[2], 'fixture must reproduce the 1.4.4 COMMENT catalog-flag bug')
        b.release_daily_cache(self.con)
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],0)

    def test_persistent_same_comment_is_never_an_owned_temporary_cache(self):
        self.con.execute('CREATE TABLE main.kk_daily_cache(cache_value INTEGER)')
        self.con.execute('COMMENT ON TABLE main.kk_daily_cache IS '+b.io.literal(b._DAILY_CACHE_OWNER))
        self.assertNotEqual(self.con.execute("SELECT database_name FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],'temp')
        with self.assertRaisesRegex(b.BuildBlocked,'owned TEMP TABLE'):
            b.release_daily_cache(self.con)
        self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0],1)

    def test_explicit_known_compounds_are_counted_before_unmatched_metadata(self):
        prefixes=['KXMVE','KXOSCARWINNERS','KXCITIESWEATHER']
        receipt,_=self.process([trade(ticker='KXMVE-EXAMPLE'),trade(ticker='KXOSCARWINNERS-EXAMPLE'),
                                trade(ticker='KXCITIESWEATHER-EXAMPLE'),trade(ticker='OTHER'),
                                trade(ticker='KXMVE-BADPRICE',yes_price=0,no_price=1)],
                               known_compound_prefixes=prefixes)
        counts={row['eligibility_reason']:row['rows'] for row in receipt['exclusions']}
        self.assertEqual(counts,{'combo_excluded':3,'unadmitted_market':1,'invalid_native_price':1})
        self.assertEqual(receipt['known_compound_prefixes'],prefixes)

    def test_raw_prints_through_builder_and_estimator_match_dense_ols_and_joint_cr0(self):
        rng=np.random.default_rng(198331)
        grouped_days={}
        markets=[]
        lookup={}
        for event in range(18):
            endpoint=datetime(2026,3+event%2,2,tzinfo=timezone.utc)
            for claim in range(3):
                lifespan=(.8,1.,1.000001,1.8,2.9,5.)[(event+claim)%6]
                ticker=f'FIXTURE-{event}-{claim}'
                raw_market=market_raw(ticker=ticker,event_ticker=f'FIXTURE-EVENT-{event}',
                    result='yes' if (event+claim)%2 else 'no',
                    open_time=(endpoint-timedelta(days=lifespan)).isoformat(),settlement_ts=endpoint.timestamp())
                prepared=prepare(raw_market,category=f'Native-{event%3}')
                markets.append(prepared)
                lookup[ticker]=prepared
                for visit in range(16):
                    price=(.035,.065,.925,.965)[(visit+claim+event)%4]
                    remaining=1. if visit==0 and lifespan>=1 else lifespan*(.04+.91*rng.random())
                    timestamp=endpoint-timedelta(days=remaining)
                    side='no' if visit%5==0 else 'yes'
                    raw=trade(ticker=ticker,created_time=timestamp.isoformat(),trade_date=timestamp.date(),
                              taker_side=side,yes_price=price if side=='yes' else 1-price,
                              no_price=price if side=='no' else 1-price)
                    # Deliberate unequal cell sizes: every copy is a distinct
                    # native print, not frequency expansion inside the builder.
                    grouped_days.setdefault(timestamp.date().isoformat(),[]).extend([dict(raw) for _ in range(1+visit%3)])
                    if visit==1:
                        middle={**raw,'yes_price':.4,'no_price':.6,'taker_side':'yes'}
                        grouped_days[timestamp.date().isoformat()].append(middle)
        self.metadata(markets)
        receipts=[]
        native=[]
        next_id=1
        for day,rows in sorted(grouped_days.items()):
            receipt,values=self.process(rows,day,start_id=next_id)
            receipts.append(receipt)
            next_id+=len(values)
            for ordinal,raw in enumerate(values):
                row=k.normalize_trade(raw,lookup[raw['ticker']],source_date=day,source_ordinal=ordinal)
                self.assertTrue(row['baseline_eligible'])
                native.append({**row,'tail':int(row['bin']==10),'month':row['trade_month'],'claim':row['claim_id']})
        summary=b.finalize_cells(self.con,self.out,receipts,expected_source_rows=len(native))
        self.assertGreater(self.con.execute('SELECT max(n_rows) FROM kk_parts_duration_raw').fetchone()[0],1)
        self.assertGreater(self.con.execute('SELECT count(*)-count(DISTINCT (event_cluster,claim_id)) FROM kk_parts_claim_raw').fetchone()[0],0)
        for kind in ('baseline','duration','claim'):
            path=self.out/summary['artifacts'][kind]['path']
            self.con.execute(f"CREATE TEMP VIEW built_{kind} AS SELECT * FROM read_parquet({b.io.literal(path)},hive_partitioning=false)")
        output=self.root/'estimates'
        output.mkdir()
        duration=[row for row in native if row['duration_eligible']]
        result=estimate.estimate_relations(self.con,baseline_relation='built_baseline',duration_relation='built_duration',
            claim_relation='built_claim',output_dir=output,
            sample_metadata={'baseline_rows':len(native),'duration_tail_rows':len(duration)},
            resource_limits=estimate.ResourceLimits())
        models=[(model,duration,False) for model in result['table2']]
        for panel,selected in (('L_gt1',[row for row in duration if row['L']>1]),
                               ('R_gt1',[row for row in duration if row['R']>1])):
            models.extend((model,selected,False) for model in result['table3'][panel])
        models.append((result['appendix_a1'],duration,True))
        self.assertEqual(len(models),12)
        for model,selected,claim_fe in models:
            with self.subTest(model=model['model_id']):
                self.assertFalse(model['suppressed'],model['suppression_reasons'])
                values,covariance=dense_oracle(selected,model['spec'],claim_fe=claim_fe)
                actual=[row['CR0']['estimate'] for row in model['joint']['estimates']]
                np.testing.assert_allclose(actual,values,atol=3e-7,rtol=3e-8)
                np.testing.assert_allclose(model['joint']['covariance_CR0'],covariance,atol=3e-6,rtol=3e-7)
                self.assertEqual(model['n_observations'],len(selected))
        joint=result['table1']['joint']
        targets={'price':'P','win_rate':'Y','payoff_cents':'payoff','roi_percent':'roi'}
        keys=[(target,field,band) for target,field in targets.items() for band in sorted({row['bin'] for row in native})]
        scores=[]
        for event in sorted({row['event_cluster'] for row in native}):
            event_scores=[]
            for target,field,band in keys:
                selected=[row for row in native if row['bin']==band]
                mean=np.mean([row[field] for row in selected])
                index=joint['names'].index(f'{target}|D{band}')
                self.assertAlmostEqual(joint['estimates'][index]['CR0']['estimate'],mean)
                event_scores.append(sum(row[field]-mean for row in selected if row['event_cluster']==event)/len(selected))
            scores.append(event_scores)
        indices=[joint['names'].index(f'{target}|D{band}') for target,_,band in keys]
        actual_covariance=np.array(joint['covariance_CR0'],dtype=float)[np.ix_(indices,indices)]
        np.testing.assert_allclose(actual_covariance,np.array(scores).T@np.array(scores),rtol=1e-11,atol=1e-9)


if __name__=='__main__':
    unittest.main()
