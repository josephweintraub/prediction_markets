"""Pure full-CLEAN projection checks; no real datasets or production writes."""
from copy import deepcopy
import unittest
from unittest.mock import patch
from scripts import project_polymarket_append_rows_v1 as m
from tests.test_build_polymarket_native_clock_layer_v1 import legacy,h
from tests import test_decode_polymarket_v3_fill_v1 as decoder_fixtures

class AppendProjectionTests(unittest.TestCase):
    def setUp(self):
        self.event=legacy();self.event.update(exchange_address=m.LEGACY_EXCHANGES[0],maker_amount_filled=25,taker_amount_filled=50)
        self.mapping={'token_id':'123','outcome':'Third literal outcome','event_slug':'','resolved':True}
        self.headers=[{'block_number':1,'block_hash':h(1),'timestamp':1782864000}]
        self.locator={'source_path':'/mnt/data/synthetic_append/source.parquet','source_file_index':3,'physical_row_ordinal':0}
    def execute(self,**kwargs):
        return m.project_legacy(kwargs.get('event',self.event),kwargs.get('token_rows',[self.mapping]),
            kwargs.get('headers',self.headers),kwargs.get('locator',self.locator),
            observed_block_hash=kwargs.get('observed_block_hash'))
    def test_two_correct_wallet_roles_and_exact_eleven_fields(self):
        result=self.execute();self.assertEqual(result['status'],'projected_not_admitted');self.assertEqual(len(result['rows']),2)
        a,b=result['rows'];self.assertEqual(tuple(a),m.VALUE_FIELDS)
        self.assertEqual((a['proxyWallet'],a['counterparty'],a['side'],a['is_maker']),(self.event['maker'],self.event['taker'],'BUY',True))
        self.assertEqual((b['proxyWallet'],b['counterparty'],b['side'],b['is_maker']),(self.event['taker'],self.event['maker'],'SELL',False))
        self.assertFalse({'native_identity','source_path','P','Y','claim_id'} & set(a))
    def test_outcome_asset_maker_reverses_sides_not_wallets(self):
        event=deepcopy(self.event);event.update(maker_asset_id='123',taker_asset_id='0',maker_amount_filled=50,taker_amount_filled=25)
        result=self.execute(event=event);a,b=result['rows'];self.assertEqual((a['side'],b['side']),('SELL','BUY'))
        self.assertEqual(a['proxyWallet'],event['maker']);self.assertEqual(b['proxyWallet'],event['taker'])
        self.assertEqual(a['price'],0.5)
    def test_gross_binary64_scale_then_divide_fee_not_net_priced(self):
        event=deepcopy(self.event);event.update(maker_amount_filled=1,taker_amount_filled=5,fee=999)
        result=self.execute(event=event);row=result['rows'][0]
        self.assertEqual(row['price'],(1/1_000_000.0)/(5/1_000_000.0));self.assertEqual(row['usdcSize'],1/1_000_000.0)
        self.assertEqual(result['trace']['fee_integer'],999);self.assertEqual(result['raw_source'],event)
    def test_exact_schema_physical_types(self):
        from scripts import audit_polymarket_clean_integrity as baseline
        self.assertEqual(m.CLEAN_SCHEMA,baseline.CLEAN_SCHEMA)
        row=self.execute()['rows'][0]
        for name,kind in m.CLEAN_SCHEMA:
            expected={'string':str,'int64':int,'double':float,'bool':bool}[kind]
            self.assertIs(type(row[name]),expected)
    def test_no_price_trim_exchange_wallet_or_paper_taker_filter(self):
        event=deepcopy(self.event);event.update(maker=m.archive.EXCHANGE,taker=m.LEGACY_EXCHANGES[1],maker_amount_filled=50,taker_amount_filled=50)
        result=self.execute(event=event);self.assertEqual(len(result['rows']),2)
        self.assertEqual(result['rows'][0]['price'],1.0);self.assertEqual(result['rows'][0]['proxyWallet'],m.archive.EXCHANGE)
        self.assertTrue(result['rows'][0]['is_maker'])
    def test_no_binary_or_module_classifier_on_legacy_full_token(self):
        token=str((255<<248)+123);event=deepcopy(self.event);event['taker_asset_id']=token
        mapping=self.mapping|{'token_id':token,'outcome':'literal label not Yes/No'}
        result=self.execute(event=event,token_rows=[mapping]);self.assertEqual(result['rows'][0]['conditionId'],token)
        self.assertEqual(result['rows'][0]['outcome'],mapping['outcome'])
    def test_nullable_and_empty_event_slug_retained_without_fallback(self):
        for slug in (None,'','literal-event'):
            with self.subTest(slug=slug):self.assertIs(self.execute(token_rows=[self.mapping|{'event_slug':slug}])['rows'][0]['eventSlug'],slug)
    def test_UTC_month_and_exclusive_cutoff(self):
        for timestamp,status in ((m.CUTOFF_SECONDS-1,'projected_not_admitted'),(m.CUTOFF_SECONDS,'outside_append_cutoff')):
            result=self.execute(headers=[self.headers[0]|{'timestamp':timestamp}])
            with self.subTest(timestamp=timestamp):
                self.assertEqual(result['status'],status)
                if result['rows']:self.assertEqual(result['rows'][0]['year_month'],'2026-10')
                else:self.assertEqual(result['raw_source'],self.event)
    def test_header_hash_observed_distinct_and_exact_assertion(self):
        result=self.execute();self.assertIsNone(result['trace']['native_observed_block_hash'])
        self.assertEqual(result['trace']['joined_block_hash'],h(1))
        self.assertEqual(self.execute(observed_block_hash=h(1))['status'],'projected_not_admitted')
        self.assertEqual(self.execute(observed_block_hash=h(9))['reason'],'native_observed_header_hash_differs')
    def test_header_missing_duplicate_wrong_key_and_bool_clock_quarantine(self):
        for rows in ([],self.headers*2,[self.headers[0]|{'block_number':2}],[self.headers[0]|{'timestamp':True}]):
            with self.subTest(rows=rows):self.assertEqual(self.execute(headers=rows)['status'],'quarantined')
    def test_legacy_block_is_positive_signed_int64_not_uint256(self):
        maximum=2**63-1
        result=self.execute(event=self.event|{'block_number':maximum},
            headers=[self.headers[0]|{'block_number':maximum}])
        self.assertEqual(result['status'],'projected_not_admitted')
        for value in (0,-1,True,2**63,2**255):
            with self.subTest(value=value):
                event=self.event|{'block_number':value}
                result=self.execute(event=event,headers=[self.headers[0]|{'block_number':value}])
                self.assertEqual(result['reason'],'legacy_block_schema');self.assertFalse(result['rows'])
                self.assertEqual(result['raw_source'],event)
    def test_legacy_log_index_is_uint64_not_uint256(self):
        for value in (0,2**64-1):
            with self.subTest(value=value):
                result=self.execute(event=self.event|{'log_index':value})
                self.assertEqual(result['status'],'projected_not_admitted')
                self.assertEqual(result['trace']['native_identity'][2],value)
        for value in (-1,True,2**64,2**255):
            with self.subTest(value=value):
                event=self.event|{'log_index':value};result=self.execute(event=event)
                self.assertEqual(result['reason'],'legacy_log_index_schema');self.assertFalse(result['rows'])
                self.assertEqual(result['raw_source'],event)
    def test_legacy_amounts_and_tokens_keep_full_uint256_range(self):
        token=str(m.archive.UINT256)
        event=self.event|{'taker_asset_id':token,'maker_amount_filled':m.archive.UINT256,
            'taker_amount_filled':m.archive.UINT256,'fee':m.archive.UINT256}
        result=self.execute(event=event,token_rows=[self.mapping|{'token_id':token}])
        self.assertEqual(result['status'],'projected_not_admitted')
        self.assertEqual(result['rows'][0]['conditionId'],token)
        self.assertEqual(result['rows'][0]['price'],1.0)
        self.assertEqual(result['trace']['gross_cash_integer'],m.archive.UINT256)
        self.assertEqual(result['trace']['fee_integer'],m.archive.UINT256)
    def test_mapping_exact_ID_no_synonyms_no_ambiguous_or_unresolved_guess(self):
        for rows in ([],[self.mapping]*2,[self.mapping|{'token_id':'0123'}],[self.mapping|{'resolved':False}],
                     [self.mapping|{'resolved':1}],[self.mapping|{'winning_outcome':'guess'}]):
            with self.subTest(rows=rows):self.assertEqual(self.execute(token_rows=rows)['status'],'quarantined')
    def test_bad_amounts_assets_and_emitter_retained_in_quarantine(self):
        for changes in ({'maker_amount_filled':True},{'maker_amount_filled':0},{'fee':-1},
                        {'maker_asset_id':'0','taker_asset_id':'0'},{'maker_asset_id':'1','taker_asset_id':'2'},
                        {'exchange_address':'0x'+'f'*40}):
            event=self.event|changes
            with self.subTest(changes=changes):
                result=self.execute(event=event);self.assertEqual(result['status'],'quarantined');self.assertEqual(result['raw_source'],event)
    def test_locator_stays_separate_and_bool_or_relative_path_rejected(self):
        result=self.execute();self.assertEqual(result['trace']['physical_source'],self.locator)
        for changes in ({'physical_row_ordinal':True},{'source_path':'relative.parquet'},{'source_file_index':-1}):
            with self.subTest(changes=changes):self.assertEqual(self.execute(locator=self.locator|changes)['status'],'quarantined')
    def test_physical_source_path_rejects_NUL_and_all_control_character_ranges(self):
        for character in ('\x00','\t','\n','\r','\x1f','\x7f','\x80','\x9f'):
            with self.subTest(character=repr(character)):
                result=self.execute(locator=self.locator|{'source_path':'/mnt/data/a'+character+'.parquet'})
                self.assertEqual(result['reason'],'physical_source_literal_path');self.assertFalse(result['rows'])
    def test_wallet_casing_preserved_and_native_identity_canonical_separate(self):
        event=self.event|{'maker':'0x'+'AB'*20,'transaction_hash':'0x'+'EF'*32}
        result=self.execute(event=event);self.assertEqual(result['rows'][0]['proxyWallet'],event['maker'])
        self.assertEqual(result['trace']['native_identity'][1],event['transaction_hash'].lower())
    def test_repeated_equal_value_rows_are_not_deduplicated(self):
        first=self.execute();second=self.execute(locator=self.locator|{'physical_row_ordinal':1})
        self.assertEqual(first['rows'],second['rows']);self.assertNotEqual(first['trace']['physical_source'],second['trace']['physical_source'])
        self.assertFalse(first['deduplication_performed']);self.assertEqual(len(first['rows'])+len(second['rows']),4)
    def test_mutations_cannot_alias_input_or_other_output_role(self):
        before=deepcopy([self.event,self.mapping,self.headers,self.locator]);result=self.execute()
        result['rows'][0]['proxyWallet']='changed';result['raw_source']['maker']='changed'
        result['trace']['physical_source']['source_file_index']=999
        self.assertEqual([self.event,self.mapping,self.headers,self.locator],before)
        self.assertEqual(result['rows'][1]['counterparty'],self.event['maker'])
    def test_V3_decodes_era_but_never_imports_binary_module_or_wallet_filter(self):
        fixture=decoder_fixtures.DecoderTests();fixture.setUp()
        # Decoder's fixture has exact wire/build evidence; no data files.
        header={'block_number':fixture.row['block_number'],'block_hash':fixture.row['blockHash'],'timestamp':1782864000}
        result=m.project_v3(fixture.row,fixture.admission,[header],self.locator)
        self.assertEqual(result['status'],'quarantined');self.assertEqual(result['reason'],'v3_full_clean_compatibility_not_admitted')
        self.assertFalse(result['rows']);self.assertEqual(result['raw_source'],fixture.row)
        self.assertIn('decoded_native',result['trace'])
    def test_V3_provider_timestamp_never_becomes_execution_clock(self):
        fixture=decoder_fixtures.DecoderTests();fixture.setUp()
        row=fixture.row|{'blockTimestamp':'provider-not-a-clock','provider_block_timestamp_present':True}
        header={'block_number':row['block_number'],'block_hash':row['blockHash'],'timestamp':1782864000}
        with patch.object(m.archive,'normalize_event',side_effect=AssertionError('paper adapter called')):
            result=m.project_v3(row,fixture.admission,[header],self.locator)
        self.assertEqual(result['reason'],'v3_full_clean_compatibility_not_admitted')
        self.assertEqual(result['trace']['exact_timestamp'],header['timestamp'])
        self.assertIsNone(result['trace']['decoded_native']['event']['block_timestamp'])
        self.assertEqual(result['raw_source']['blockTimestamp'],'provider-not-a-clock')
        self.assertEqual(m.project_v3(row,fixture.admission,[],self.locator)['reason'],'exact_header_not_singleton')
    def test_V3_bad_wire_build_or_observed_hash_is_explicit_quarantine(self):
        fixture=decoder_fixtures.DecoderTests();fixture.setUp()
        header={'block_number':fixture.row['block_number'],'block_hash':fixture.row['blockHash'],'timestamp':1782864000}
        for row,admission,headers in ((fixture.row|{'removed':True},fixture.admission,[header]),
            (fixture.row,{},[header]),(fixture.row,fixture.admission,[header|{'block_hash':h(99)}])):
            with self.subTest(row=row,admission=admission,headers=headers):
                result=m.project_v3(row,admission,headers,self.locator)
                self.assertEqual(result['status'],'quarantined');self.assertFalse(result['rows'])
                self.assertNotEqual(result['reason'],'v3_full_clean_compatibility_not_admitted')
                self.assertEqual(result['raw_source'],row)
    def test_V3_cutoff_and_nested_provenance_do_not_alias_input(self):
        fixture=decoder_fixtures.DecoderTests();fixture.setUp()
        header={'block_number':fixture.row['block_number'],'block_hash':fixture.row['blockHash'],'timestamp':m.CUTOFF_SECONDS}
        before=deepcopy(fixture.row);result=m.project_v3(fixture.row,fixture.admission,[header],self.locator)
        self.assertEqual(result['status'],'outside_append_cutoff');self.assertFalse(result['rows'])
        result['trace']['decoded_native']['provenance']['raw_native_row']['topics'][1]='changed'
        result['raw_source']['topics'][1]='changed'
        self.assertEqual(fixture.row,before)
    def test_nonfinite_or_nonJSON_input_hardblocks_before_projection(self):
        for value in (float('nan'),float('inf'),object()):
            with self.subTest(value=value),self.assertRaisesRegex(m.ProjectionError,'fixture_input_not_finite_JSON'):
                self.execute(event=self.event|{'unexpected':value})
    def test_fixture_only_bounds_and_guard_first_hardblock(self):
        with patch.object(m,'MAX_FIXTURE_INPUT_BYTES',1),self.assertRaises(m.ProjectionError):self.execute()
        with patch.object(m,'require_production_host',side_effect=RuntimeError('host')):
            with self.assertRaisesRegex(RuntimeError,'host'):m.run_production()
        with patch.object(m,'require_production_host'):
            with self.assertRaisesRegex(m.ProjectionError,'production_adapter_not_admitted'):m.run_production()
    def test_contract_full_clean_not_study_or_production_admission(self):
        value=m.contract();self.assertEqual(value['value_fields'],list(m.VALUE_FIELDS))
        self.assertFalse(value['production_admitted']);self.assertFalse(value['raw_or_native_retention_policy_applied'])

if __name__=='__main__':unittest.main()
