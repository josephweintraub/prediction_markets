"""Strict, pure native-row bridge to the historical 12-column representation.

This checks wire and cached-row consistency, not external source authentication,
collection coverage, execution clocks, retention or economic interpretation.
No I/O occurs. V3 and unknown emitting contracts are explicitly quarantined.
"""
from copy import deepcopy
import re

from production_guard import require_production_host
from pipeline import decode_polymarket_v3_fill_v1 as shared
from scripts import audit_polymarket_native_overlap as oracle
from scripts import project_polymarket_append_rows_v1 as projection

SCHEMA='polymarket_append_legacy_v2_wire_bridge_v2'
EMITTERS=tuple(oracle.OLD_ADDRESSES)
ROW_FIELDS=frozenset(shared.ROW_FIELDS)
EVENT_FIELDS=tuple(oracle.FIELDS)
LEGACY_DATA=re.compile(r'0x[0-9a-fA-F]{320}')
V2_DATA=re.compile(r'0x[0-9a-fA-F]{448}')
SCOPE={'representation_compatibility_only':True,'external_source_authenticated':False,
    'external_endpoint_receipt_authenticated':False,'external_collection_authenticated':False,
    'execution_clock_verified':False,'provider_timestamp_used_as_execution_clock':False,
    'deduplication_performed':False,'archive_or_sample_admitted':False,'production_admitted':False}
SOURCE_RULES={
    'legacy':{'sealed_bundle_path':'pipeline/extraction/extract_orderfilled.py',
        'sha256':'bbaa3e7eed8e7997c32a89806f9365c80e0497d0bcca0a105526f4d5ab297590'},
    'v2':{'sealed_bundle_path':'pipeline/extraction/extract_orderfilled_v2.py',
        'sha256':'98db7e8141694e28da04132a35b46b39190b444824d62c2448ca7f8c08012eac'},
}
require=projection.require
DecodeError=projection.ProjectionError


def decode_row(row,locator):
    """Return a retained quarantine or an exact old-extractor-compatible event.

    ``event`` can be supplied to project_legacy with the returned observed hash.
    That separate core still checks prices, token mapping and a supplied exact
    header; this bridge supplies no timestamp and makes no public proof object.
    """
    projection._fixture_bound([row,locator])
    result={'schema_version':SCHEMA,'status':'quarantined','reason':None,'event':None,
        'raw_source':deepcopy(row),'trace':{'physical_source':deepcopy(locator)},**SCOPE}
    try:
        result['trace']['physical_source']=projection._locator(locator)
        require(type(row) is dict and set(row)==ROW_FIELDS,'native_parquet_row_schema')
        require(row['removed'] is False,'removed_native_log')
        require(type(row['address']) is str and shared.archive.ADDRESS.fullmatch(row['address']) is not None,
            'native_emitting_address_schema')
        address=row['address'].lower()
        require(address in EMITTERS,'V3_or_unknown_emitting_contract_not_supported')
        require(type(row['exchange_address']) is str and row['exchange_address']==address,
            'collector_exchange_address_differs')
        for field in ('blockHash','transactionHash'):
            require(type(row[field]) is str and shared.archive.HASH.fullmatch(row[field]) is not None,
                'native_hash_schema')
        locators={}
        for raw,cached in (('blockNumber','block_number'),('logIndex','log_index'),
            ('transactionIndex','transaction_index')):
            value=shared.rpc_uint64(row[raw])
            require(shared.uint64(row[cached])==value,'raw_cached_native_locator_differs')
            locators[cached]=value
        require(0<locators['block_number']<=2**63-1,'legacy_compatible_block_int64')
        shared.uint64(row['source_request_id'])
        require(type(row['window']) is str and 0<len(row['window'])<=128
            and all(33<=ord(c)<=126 for c in row['window']),'collector_window_schema')
        present=row['provider_block_timestamp_present'];timestamp=row['blockTimestamp']
        require(type(present) is bool,'provider_timestamp_presence_schema')
        require((not present and timestamp is None) or (present and type(timestamp) is str
            and 0<len(timestamp)<=128 and all(32<=ord(c)<=126 for c in timestamp)),
            'provider_timestamp_presence_differs')
        topics=row['topics']
        require(type(topics) is list and len(topics)==4 and all(type(t) is str
            and shared.archive.HASH.fullmatch(t) is not None for t in topics),'order_filled_topic_schema')
        require(topics[0].lower()==oracle.pilot.EXPECTED_TOPICS[address],
            'topic_contradicts_emitting_contract_generation')
        require(all(t[2:26]=='0'*24 for t in topics[2:]),'noncanonical_indexed_address_padding')
        generation='legacy' if address in EMITTERS[:2] else 'v2'
        pattern=LEGACY_DATA if generation=='legacy' else V2_DATA
        require(type(row['data']) is str and pattern.fullmatch(row['data']) is not None,
            'fixed_five_or_seven_word_schema')
        words=['0x'+row['data'][offset:offset+64] for offset in range(2,len(row['data']),64)]
        if generation=='v2':
            require(int(words[0],16) in (0,1),'noncanonical_side_enum_or_padding')
        values=oracle.decode_old_representation(row)
        event=dict(zip(EVENT_FIELDS,values))
        # The overlap oracle canonicalizes comparison keys; the sealed original
        # extractor preserves literal hash/wallet case in archive value columns.
        event.update(order_hash=topics[1], maker='0x'+topics[2][-40:],
            taker='0x'+topics[3][-40:], transaction_hash=row['transactionHash'])
        require(len(values)==12 and set(event)==projection.LEGACY_FIELDS,'internal_extractor_representation')
        result['trace'].update(protocol_generation=generation,raw_data_words=words,
            native_identity=(address,row['transactionHash'].lower(),locators['log_index']),
            native_observed_block_hash=row['blockHash'].lower(),transaction_index=locators['transaction_index'],
            extractor_rule_reference=deepcopy(SOURCE_RULES[generation]),
            V2_builder_word=words[5] if generation=='v2' else None,
            V2_metadata_word=words[6] if generation=='v2' else None)
        result.update(status='decoded_representation_not_admitted',reason='old_extractor_representation_only',event=event)
    except (DecodeError,shared.DecodeError,oracle.OverlapBlocked) as error:
        result.update(status='quarantined',reason=str(error),event=None)
    return result


def contract():
    return {'schema_version':SCHEMA,'status':'pure_fixture_candidate_no_external_admission',
        'exact_emitters':list(EMITTERS),'native_fields':sorted(ROW_FIELDS),'legacy_fields':list(EVENT_FIELDS),
        'archive_case':'literal_topics_wallets_order_hash_transactionHash; emitter_lower_only',
        'wire_word_counts':{'legacy':5,'v2':7},'source_rule_references':deepcopy(SOURCE_RULES),
        'fixture_input_bound_not_production_admission':projection.MAX_FIXTURE_INPUT_BYTES,
        'remaining_gates':['actual_source_and_endpoint_receipt_authentication',
            'source_coverage_and_native_identity_reconciliation','supplied_exact_header_and_token_mapping',
            'historical_retention_monthly_DISTINCT_and_finite_resource_publication'],**SCOPE}


def run_production(*_args,**_kwargs):
    require_production_host();raise DecodeError('legacy_V2_bridge_production_adapter_not_admitted')


if __name__=='__main__':raise SystemExit('Pure fixture API only; no production entry.')
