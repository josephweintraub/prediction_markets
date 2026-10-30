"""Pure append projection candidate: repaired full CLEAN values, no production I/O.

This is NOT the filtered-paper adapter. It applies no exchange-wallet, module,
binary-market, taker-only, bot or study eligibility filter, and performs no
deduplication. Full-CLEAN V3 compatibility remains explicitly quarantined.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime,timezone
import hashlib
import json
import math
from pathlib import PurePosixPath

from production_guard import require_production_host
from pipeline import decode_polymarket_v3_fill_v1 as wire

archive=wire.archive
VALUE_FIELDS=archive.VALUE_FIELDS
CLEAN_SCHEMA=(('proxyWallet','string'),('timestamp','int64'),('conditionId','string'),
    ('usdcSize','double'),('price','double'),('side','string'),('outcome','string'),
    ('eventSlug','string'),('is_maker','bool'),('counterparty','string'),('year_month','string'))
LEGACY_FIELDS={'order_hash','maker','taker','maker_asset_id','taker_asset_id','maker_amount_filled',
    'taker_amount_filled','fee','block_number','transaction_hash','log_index','exchange_address'}
LEGACY_EXCHANGES=archive.HISTORICAL_EXCHANGES
CUTOFF_SECONDS=1790899200
MAX_FIXTURE_INPUT_BYTES=1024**2
SCHEMA='polymarket_full_clean_append_projection_v1'
SCOPE={'projection_only':True,'production_admitted':False,'external_header_proof_verified':False,
    'global_metadata_uniqueness_verified':False,'append_lower_boundary_verified':False,
    'deduplication_performed':False,'economic_own_counterparty_action_claimed':False,
    'archive_or_sample_admitted':False}

class ProjectionError(ValueError):pass
def require(value,reason):
    if not value:raise ProjectionError(reason)
def encoded(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def _fixture_bound(value):
    try:raw=encoded(value)
    except (ValueError,TypeError,OverflowError,RecursionError):raise ProjectionError('fixture_input_not_finite_JSON') from None
    require(len(raw)<=MAX_FIXTURE_INPUT_BYTES,'fixture_input_byte_bound_not_production_cap')
def _uint(value,reason,*,positive=False):
    require(type(value) is int and (0<value if positive else 0<=value) and value<=archive.UINT256,reason)
    return value
def _locator(value):
    require(type(value) is dict and set(value)=={'source_path','source_file_index','physical_row_ordinal'},'physical_locator_schema')
    path=value['source_path']
    require(type(path) is str and PurePosixPath(path).is_absolute() and str(PurePosixPath(path))==path
        and '..' not in PurePosixPath(path).parts and path.endswith('.parquet')
        and not any(ord(char)<32 or 127<=ord(char)<160 for char in path),'physical_source_literal_path')
    for name in ('source_file_index','physical_row_ordinal'):
        require(type(value[name]) is int and 0<=value[name]<2**64,'physical_locator_integer')
    return deepcopy(value)
def _result(raw,locator):
    return {'schema_version':SCHEMA,'status':'quarantined','reason':None,'rows':[],
        'raw_source':deepcopy(raw),'trace':{'physical_source':deepcopy(locator)},**SCOPE}
def _header(headers,block,observed):
    require(type(headers) is list and len(headers)==1,'exact_header_not_singleton')
    row=headers[0]
    require(type(row) is dict and set(row)=={'block_number','block_hash','timestamp'}
        and type(row['block_number']) is int and row['block_number']==block
        and type(row['block_hash']) is str and archive.HASH.fullmatch(row['block_hash']) is not None
        and type(row['timestamp']) is int and 0<row['timestamp']<=253402300799,'exact_header_key_or_clock')
    if observed is not None:
        require(type(observed) is str and archive.HASH.fullmatch(observed) is not None
                and observed.lower()==row['block_hash'].lower(),'native_observed_header_hash_differs')
    return row
def _metadata(rows,token):
    require(type(rows) is list and len(rows)==1,'token_mapping_missing_or_ambiguous')
    row=rows[0]
    require(type(row) is dict and set(row)=={'token_id','outcome','event_slug','resolved'},'token_mapping_schema')
    require(type(row['token_id']) is str and archive.decimal_token(row['token_id'])
            and row['token_id']==token,'exact_full_token_id_mismatch')
    require(type(row['outcome']) is str and bool(row['outcome'].strip())
            and (row['event_slug'] is None or type(row['event_slug']) is str)
            and type(row['resolved']) is bool,'token_mapping_schema')
    require(row['resolved'] is True,'unresolved_token_mapping')
    return row

def project_legacy(event,token_rows,headers,locator,*,observed_block_hash=None):
    """One typed legacy collateral fill -> two corrected full-CLEAN role values.

    Caller owns source admission, mirror/rank retention, global metadata proof,
    overlap and final monthly full-value DISTINCT. No such decisions occur here.
    """
    _fixture_bound([event,token_rows,headers,locator,observed_block_hash])
    result=_result(event,locator)
    try:
        result['trace']['physical_source']=_locator(locator)
        require(type(event) is dict and set(event)==LEGACY_FIELDS,'legacy_event_schema')
        for name in ('maker','taker','exchange_address'):
            require(type(event[name]) is str and archive.ADDRESS.fullmatch(event[name]) is not None,'legacy_address_schema')
        for name in ('order_hash','transaction_hash'):
            require(type(event[name]) is str and archive.HASH.fullmatch(event[name]) is not None,'legacy_hash_schema')
        require(event['exchange_address'].lower() in LEGACY_EXCHANGES,'unsupported_legacy_emitting_exchange')
        block=_uint(event['block_number'],'legacy_block_schema',positive=True)
        require(block<=2**63-1,'legacy_block_schema')
        index=_uint(event['log_index'],'legacy_log_index_schema')
        require(index<2**64,'legacy_log_index_schema')
        for name in ('maker_amount_filled','taker_amount_filled','fee'):_uint(event[name],'legacy_amount_schema')
        for name in ('maker_asset_id','taker_asset_id'):
            require(type(event[name]) is str and (event[name]=='0' or archive.decimal_token(event[name])), 'legacy_asset_schema')
        maker_collateral=event['maker_asset_id']=='0';taker_collateral=event['taker_asset_id']=='0'
        require(maker_collateral!=taker_collateral,'legacy_collateral_asset_not_exactly_one')
        token=event['taker_asset_id'] if maker_collateral else event['maker_asset_id']
        header=_header(headers,block,observed_block_hash);timestamp=header['timestamp']
        result['trace'].update(native_identity=(event['exchange_address'].lower(),event['transaction_hash'].lower(),event['log_index']),
          native_observed_block_hash=observed_block_hash.lower() if observed_block_hash is not None else None,
          joined_block_hash=header['block_hash'].lower(),exact_timestamp=timestamp,
          source_representation='legacy_collateral_OrderFilled',
          raw_source_projection_sha256=hashlib.sha256(encoded(event)).hexdigest())
        if timestamp>=CUTOFF_SECONDS:
            result.update(status='outside_append_cutoff',reason='execution_at_or_after_exclusive_cutoff');return result
        metadata=_metadata(token_rows,token)
        cash_integer=event['maker_amount_filled'] if maker_collateral else event['taker_amount_filled']
        quantity_integer=event['taker_amount_filled'] if maker_collateral else event['maker_amount_filled']
        require(cash_integer>0 and quantity_integer>0,'nonpositive_gross_cash_or_quantity')
        cash=cash_integer/1_000_000.0;quantity=quantity_integer/1_000_000.0;price=cash/quantity
        require(math.isfinite(cash) and math.isfinite(price) and cash>0 and 0<price<=1,'invalid_full_clean_gross_price')
        month=datetime.fromtimestamp(timestamp,timezone.utc).strftime('%Y-%m')
        for is_maker,wallet,counterparty,side in (
            (True,event['maker'],event['taker'],'BUY' if maker_collateral else 'SELL'),
            (False,event['taker'],event['maker'],'SELL' if maker_collateral else 'BUY')):
            row={'proxyWallet':wallet,'timestamp':timestamp,'conditionId':token,'usdcSize':cash,'price':price,
                 'side':side,'outcome':metadata['outcome'],'eventSlug':metadata['event_slug'],
                 'is_maker':is_maker,'counterparty':counterparty,'year_month':month}
            require(tuple(row)==VALUE_FIELDS,'internal_full_clean_schema');result['rows'].append(row)
        result['trace'].update(gross_cash_integer=cash_integer,gross_quantity_integer=quantity_integer,
                               fee_integer=event['fee'])
        result.update(status='projected_not_admitted',reason='full_clean_value_projection_only')
    except ProjectionError as error:result.update(status='quarantined',reason=str(error),rows=[])
    return result

def project_v3(raw_row,build_admission,headers,locator):
    """Decode/identify era only. Never inherit the binary filtered-paper adapter."""
    _fixture_bound([raw_row,build_admission,headers,locator]);result=_result(raw_row,locator)
    try:
        result['trace']['physical_source']=_locator(locator)
        decoded=wire.decode_native_row(raw_row,build_admission)
        event=decoded['event'];header=_header(headers,event['block_number'],event['block_hash'])
        result['trace'].update(decoded_native=deepcopy(decoded),native_identity=decoded['provenance']['native_identity'],
                              native_observed_block_hash=event['block_hash'],joined_block_hash=header['block_hash'].lower(),
                              exact_timestamp=header['timestamp'])
        result['reason']='v3_full_clean_compatibility_not_admitted'
        if header['timestamp']>=CUTOFF_SECONDS:
            result.update(status='outside_append_cutoff',reason='execution_at_or_after_exclusive_cutoff')
    except (ProjectionError,wire.DecodeError,archive.AdmissionError,archive.RowError) as error:
        result.update(status='quarantined',reason=str(error),rows=[])
    return result

def contract():
    return {'schema_version':SCHEMA,'status':'pure_fixture_candidate_no_production_admission',
        'value_fields':list(VALUE_FIELDS),'physical_clean_schema':list(CLEAN_SCHEMA),
        'end_exclusive_utc':'2026-10-02T00:00:00Z','raw_or_native_retention_policy_applied':False,
        'row_count_law':'projected_legacy_input=2_value_rows; all_other_records_explicit_status_plus_raw_source',
        'gross_binary64_order':'cash_integer/1e6,quantity_integer/1e6,then_cash_divided_by_quantity',
        'price_semantics':'baseline_recorded_gross_amount_ratio_not_certified_execution_price',
        'wallet_case':'literal_source_value_preserved','V3_status':'quarantined_until_full_CLEAN_compatibility_admitted',
        'fixture_input_limit_bytes_not_production_admission':MAX_FIXTURE_INPUT_BYTES,
        'remaining_gates':['actual_baseline_source_schema_and_boundary_admission','source_native_mirror_rank_and_overlap_reconciliation',
            'globally_unique_authoritative_token_resolution_mapping','V3_full_CLEAN_compatibility',
            'monthly_full_value_DISTINCT_and_count_conservation','finite_actual_resource_fit_and_immutable_publication'],**SCOPE}

def run_production(*_args,**_kwargs):
    require_production_host();raise ProjectionError('append_projection_production_adapter_not_admitted')

if __name__=='__main__':raise SystemExit('Pure fixture API only; no production entry.')
