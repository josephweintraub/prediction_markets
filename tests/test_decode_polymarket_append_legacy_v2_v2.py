"""Small synthetic legacy/V2 native bridge fixtures; no external I/O."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from scripts import decode_polymarket_append_legacy_v2_v2 as m
from tests import test_polymarket_native_overlap as fixtures

MISSING=object()

class NativeBridgeTests(unittest.TestCase):
    def setUp(self):
        self.locator={'source_path':'/mnt/data/fixture/append.parquet','source_file_index':5,'physical_row_ordinal':0}
    def row(self,index=0):
        value=deepcopy(fixtures.native_logs()[index])
        return value|{'blockTimestamp':None,'block_number':int(value['blockNumber'],16),
            'log_index':int(value['logIndex'],16),'transaction_index':int(value['transactionIndex'],16),
            'exchange_address':value['address'].lower(),'window':'b000000000001-000000000100',
            'source_request_id':2**40,'provider_block_timestamp_present':False}
    def data(self,row,words):return row|{'data':'0x'+''.join(f'{word:064x}' for word in words)}
    def execute(self,row=MISSING,locator=MISSING):
        return m.decode_row(self.row() if row is MISSING else row,self.locator if locator is MISSING else locator)
    def reject(self,row,reason=None):
        result=self.execute(row);self.assertEqual(result['status'],'quarantined');self.assertIsNone(result['event'])
        self.assertEqual(result['raw_source'],row)
        if reason is not None:self.assertEqual(result['reason'],reason)
    def test_all_four_emitters_equal_existing_old_extractor_oracle(self):
        self.assertEqual(m.EMITTERS,tuple(m.projection.LEGACY_EXCHANGES))
        for index,address in enumerate(m.EMITTERS):
            with self.subTest(address=address):
                row=self.row(index);result=self.execute(row)
                self.assertEqual(result['status'],'decoded_representation_not_admitted')
                self.assertEqual(tuple(result['event']),m.EVENT_FIELDS)
                self.assertEqual(tuple(result['event'].values()),m.oracle.decode_old_representation(row))
                self.assertEqual(result['event']['maker_asset_id'],'103' if index==3 else '0')
                self.assertEqual(result['event']['taker_asset_id'],'0' if index==3 else str(100+index))
    def test_V2_both_side_enum_words_map_assets_without_altering_amounts_or_fee(self):
        for index in (2,3):
            for side in (0,1):
                with self.subTest(index=index,side=side):
                    row=self.data(self.row(index),[side,987,7,13,29,31,37]);event=self.execute(row)['event']
                    self.assertEqual((event['maker_asset_id'],event['taker_asset_id']),('0','987') if side==0 else ('987','0'))
                    self.assertEqual((event['maker_amount_filled'],event['taker_amount_filled'],event['fee']),(7,13,29))
    def test_full_uint256_tokens_amounts_and_fee_are_not_truncated(self):
        maximum=2**256-1
        for index in (0,2):
            words=[0,maximum,maximum,maximum,maximum]+([maximum,maximum] if index==2 else [])
            with self.subTest(index=index):
                event=self.execute(self.data(self.row(index),words))['event']
                self.assertEqual(event['taker_asset_id'],str(maximum))
                self.assertEqual(event['maker_amount_filled'],maximum);self.assertEqual(event['fee'],maximum)
    def test_V2_builder_and_metadata_raw_words_are_retained(self):
        row=self.data(self.row(2),[0,123,10,20,1,int('ab'*32,16),int('cd'*32,16)])
        row['data']='0x'+row['data'][2:].upper();value=self.execute(row)
        self.assertEqual(value['trace']['V2_builder_word'],'0x'+'AB'*32)
        self.assertEqual(value['trace']['V2_metadata_word'],'0x'+'CD'*32)
        self.assertEqual(value['raw_source'],row);self.assertEqual(len(value['trace']['raw_data_words']),7)
        legacy=self.execute();self.assertIsNone(legacy['trace']['V2_builder_word'])
    def test_empty_short_extra_nonhex_or_odd_words_never_reach_oracle(self):
        for index in (0,2):
            row=self.row(index)
            for data in ('0x',row['data'][:-1],row['data']+'0',row['data']+'00'*32,'0x'+'g'*len(row['data'][2:]),None):
                with self.subTest(index=index,data=data),patch.object(m.oracle,'decode_old_representation',side_effect=AssertionError('reached')):
                    self.reject(row|{'data':data},'fixed_five_or_seven_word_schema')
    def test_emitting_generation_requires_its_own_topic_and_exact_word_count(self):
        for index,other in ((0,2),(2,0)):
            row=self.row(index);other_row=self.row(other)
            changed=deepcopy(row);changed['topics'][0]=other_row['topics'][0]
            with self.subTest(index=index):
                self.reject(changed,'topic_contradicts_emitting_contract_generation')
                self.reject(row|{'data':other_row['data']},'fixed_five_or_seven_word_schema')
    def test_full_side_word_rejects_outside_enum_not_uint8_cast(self):
        for side in (2,256,2**255):
            with self.subTest(side=side):self.reject(self.data(self.row(2),[side,123,10,20,1,0,0]),'noncanonical_side_enum_or_padding')
    def test_topics_count_hashes_and_indexed_address_padding(self):
        row=self.row()
        alternatives=[row['topics'][:3],row['topics']+[row['topics'][0]],
            [row['topics'][0],'0x'+'z'*64,*row['topics'][2:]]]
        for topics in alternatives:
            with self.subTest(topics=topics):self.reject(row|{'topics':topics},'order_filled_topic_schema')
        for field in (2,3):
            changed=deepcopy(row);changed['topics'][field]='0x'+'1'+'0'*23+changed['topics'][field][-40:]
            with self.subTest(field=field):self.reject(changed,'noncanonical_indexed_address_padding')
    def test_hashes_and_addresses_require_actual_strings_and_hex_widths(self):
        for field in ('blockHash','transactionHash','address'):
            for value in (True,None,'0x123','0x'+'z'*(40 if field=='address' else 64)):
                with self.subTest(field=field,value=value):self.reject(self.row()|{field:value})
    def test_raw_cached_locator_equality_and_strict_uint64(self):
        for raw,cached in (('blockNumber','block_number'),('logIndex','log_index'),('transactionIndex','transaction_index')):
            row=self.row()
            for value in (True,-1,2**64,row[cached]+1):
                with self.subTest(raw=raw,value=value):self.reject(row|{cached:value})
            for value in ('0x00','0X1','0x1g',None,True,'0x10000000000000000'):
                with self.subTest(raw=raw,value=value):self.reject(row|{raw:value})
    def test_uint64_locator_extrema_and_int64_block_ceiling(self):
        maximum=2**64-1;row=self.row()|{'block_number':2**63-1,'blockNumber':hex(2**63-1),
            'log_index':maximum,'logIndex':hex(maximum),'transaction_index':maximum,
            'transactionIndex':hex(maximum),'source_request_id':maximum}
        value=self.execute(row);self.assertEqual(value['status'],'decoded_representation_not_admitted')
        self.assertEqual(value['event']['log_index'],maximum)
        for block in (0,2**63):
            with self.subTest(block=block):self.reject(row|{'block_number':block,'blockNumber':hex(block)},'legacy_compatible_block_int64')
    def test_request_window_presence_fields_are_strict_and_bounded(self):
        for change in ({'source_request_id':True},{'source_request_id':-1},{'source_request_id':2**64},
            {'window':''},{'window':'a'*129},{'window':'with space'},{'window':'a\x00'},
            {'provider_block_timestamp_present':1}):
            with self.subTest(change=change):self.reject(self.row()|change)
    def test_provider_clock_is_opaque_never_decoded_to_execution_time(self):
        row=self.row()|{'provider_block_timestamp_present':True,'blockTimestamp':'not an execution clock'}
        value=self.execute(row);self.assertEqual(value['status'],'decoded_representation_not_admitted')
        self.assertEqual(value['raw_source']['blockTimestamp'],row['blockTimestamp'])
        self.assertFalse(value['provider_timestamp_used_as_execution_clock']);self.assertFalse(value['execution_clock_verified'])
        self.assertNotIn('timestamp',value['event'])
        for change in ({'provider_block_timestamp_present':False},{'blockTimestamp':None},{'blockTimestamp':'x'*129},{'blockTimestamp':'a\n'}):
            with self.subTest(change=change):self.reject(row|change,'provider_timestamp_presence_differs')
    def test_exact_seventeen_fields_no_public_boolean_admission(self):
        row=self.row();self.assertEqual(set(row),m.ROW_FIELDS)
        for value in (row|{'authenticated':True},{key:item for key,item in row.items() if key!='window'},[],None):
            with self.subTest(value=value):self.reject(value,'native_parquet_row_schema')
    def test_V3_unknown_and_removed_rows_never_supported(self):
        self.reject(self.row(4),'V3_or_unknown_emitting_contract_not_supported')
        unknown='0x'+'f'*40
        self.reject(self.row()|{'address':unknown,'exchange_address':unknown},'V3_or_unknown_emitting_contract_not_supported')
        for removed in (True,0,None):
            with self.subTest(removed=removed):self.reject(self.row()|{'removed':removed},'removed_native_log')
    def test_cached_emitter_must_equal_canonical_raw_emitter(self):
        row=self.row()
        self.reject(row|{'exchange_address':m.EMITTERS[1]},'collector_exchange_address_differs')
        self.reject(row|{'exchange_address':row['address'].upper()},'collector_exchange_address_differs')
    def test_original_extractor_literal_case_not_comparison_oracle_case(self):
        row=deepcopy(self.row(2));row['address']='0x'+row['address'][2:].upper()
        row['transactionHash']='0x'+'EF'*32;row['topics'][1]='0x'+'AB'*32
        row['topics'][2]='0x'+'0'*24+m.oracle.pilot.ADDRESSES[4][2:].upper()
        value=self.execute(row)
        self.assertEqual(value['event']['order_hash'],row['topics'][1])
        self.assertEqual(value['event']['transaction_hash'],row['transactionHash'])
        self.assertEqual(value['event']['maker'],'0x'+row['topics'][2][-40:])
        self.assertEqual(value['event']['taker'],'0x'+row['topics'][3][-40:])
        self.assertEqual(value['trace']['native_identity'][1],row['transactionHash'].lower())
        self.assertEqual(value['raw_source'],row)
    def test_no_collateral_XOR_amount_or_token_eligibility_filter_in_wire_bridge(self):
        for index,words in ((0,[0,0,0,0,0]),(2,[0,0,0,0,0,0,0])):
            with self.subTest(index=index):self.assertEqual(self.execute(self.data(self.row(index),words))['status'],'decoded_representation_not_admitted')
    def test_bridge_output_composes_with_frozen_legacy_projection_for_both_V2_sides(self):
        for side in (0,1):
            row=self.data(self.row(2),[side,123,250000 if side==0 else 1000000,1000000 if side==0 else 250000,999,0,0])
            decoded=self.execute(row)
            header={'block_number':row['block_number'],'block_hash':row['blockHash'],'timestamp':1782864000}
            projected=m.projection.project_legacy(decoded['event'],[{'token_id':'123','outcome':'Literal outcome','event_slug':'','resolved':True}],
                [header],decoded['trace']['physical_source'],observed_block_hash=decoded['trace']['native_observed_block_hash'])
            with self.subTest(side=side):
                self.assertEqual(projected['status'],'projected_not_admitted');self.assertEqual(len(projected['rows']),2)
                self.assertEqual(projected['rows'][0]['price'],0.25)
    def test_raw_words_locator_event_and_provenance_are_not_aliased(self):
        row=self.row(2);before=deepcopy([row,self.locator]);value=self.execute(row)
        value['raw_source']['topics'][1]='changed';value['trace']['physical_source']['physical_row_ordinal']=999
        value['trace']['raw_data_words'][0]='changed';value['event']['maker']='changed'
        self.assertEqual([row,self.locator],before)
        first=self.execute(row);second=self.execute(row,self.locator|{'physical_row_ordinal':1})
        self.assertEqual(first['event'],second['event']);self.assertFalse(first['deduplication_performed'])
    def test_physical_source_guards_remain_separate_from_event(self):
        for change in ({'source_path':'/mnt/data/a\x00.parquet'},{'physical_row_ordinal':True},{'source_file_index':-1}):
            with self.subTest(change=change):self.reject_with_locator(self.locator|change)
        self.assertNotIn('source_path',self.execute()['event'])
    def reject_with_locator(self,locator):
        value=self.execute(locator=locator);self.assertEqual(value['status'],'quarantined');self.assertIsNone(value['event'])
    def test_fixture_limit_and_production_guard_then_hardblock(self):
        with patch.object(m.projection,'MAX_FIXTURE_INPUT_BYTES',1),self.assertRaises(m.DecodeError):self.execute()
        with patch.object(m,'require_production_host',side_effect=RuntimeError('host')):
            with self.assertRaisesRegex(RuntimeError,'host'):m.run_production()
        with patch.object(m,'require_production_host'):
            with self.assertRaisesRegex(m.DecodeError,'production_adapter_not_admitted'):m.run_production()
        self.assertFalse(m.contract()['production_admitted'])


    def test_literal_wallet_case_survives_corrected_full_clean_projection(self):
        row=self.data(self.row(2),[0,123,250000,1000000,0,0,0])
        row['topics'][2]='0x'+'0'*24+'AB'*20
        row['topics'][3]='0x'+'0'*24+'Cd'*20
        decoded=self.execute(row)
        out=m.projection.project_legacy(decoded['event'],
            [{'token_id':'123','outcome':'literal','event_slug':'','resolved':True}],
            [{'block_number':row['block_number'],'block_hash':row['blockHash'],'timestamp':1782864000}],
            self.locator,observed_block_hash=decoded['trace']['native_observed_block_hash'])
        self.assertEqual(out['rows'][0]['proxyWallet'],'0x'+'AB'*20)
        self.assertEqual(out['rows'][1]['proxyWallet'],'0x'+'Cd'*20)
        self.assertEqual(out['rows'][1]['counterparty'],'0x'+'AB'*20)

if __name__=='__main__':unittest.main()
