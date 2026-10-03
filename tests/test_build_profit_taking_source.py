from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.diagnostics.build_profit_taking_source import prepare_source,reconcile_source
from analysis.diagnostics.profit_taking_actions import LEGACY_EXCHANGE_ADDRESSES,V2_EXCHANGE_ADDRESSES,RAW_FIELDS

OLD=sorted(LEGACY_EXCHANGE_ADDRESSES)[0]
NEW=sorted(V2_EXCHANGE_ADDRESSES)[0]


def raw(side="BUY",token="1",q=100,c=40,*,wallet="passive",taker="active",log=1,tx="tx",fee=0,exchange=OLD,order=None):
    return dict(zip(RAW_FIELDS,(order or f"order-{log}",wallet,taker,"0" if side=="BUY" else token,
        token if side=="BUY" else "0",c if side=="BUY" else q,q if side=="BUY" else c,
        fee,1,tx,log,exchange)))


def setup(tmp_path:Path,records,*,surplus_cases=None,surplus_original_rows=None):
    source=tmp_path/"raw.parquet"; tokens=tmp_path/"tokens.parquet"
    pq.write_table(pa.Table.from_pylist(records),source)
    pq.write_table(pa.Table.from_pylist([{"token_id":"1","market_id":"market","complement_token_id":"2"},
        {"token_id":"2","market_id":"market","complement_token_id":"1"}]),tokens)
    con=duckdb.connect()
    counts=prepare_source(con,source,tokens)
    counts.update(reconcile_source(con,surplus_cases,surplus_original_rows))
    return con,counts


@pytest.mark.parametrize("active_side,passive_side,token,cash,kind",[
    ("BUY","SELL","1",40,"NORMAL"),("SELL","BUY","1",40,"NORMAL"),
    ("BUY","BUY","2",60,"MINT"),("SELL","SELL","2",60,"MERGE")])
@pytest.mark.parametrize("exchange",[OLD,NEW])
def test_exact_sql_recovery_generations_and_mechanisms(tmp_path,active_side,passive_side,token,cash,kind,exchange):
    records=[raw(passive_side,token,exchange=exchange),raw(active_side,c=cash,wallet="active",taker=exchange,log=2,exchange=exchange)]
    con,counts=setup(tmp_path,records)
    assert counts["accepted_batches"]==1 and counts["accepted_own_actions"]==2
    assert con.execute("SELECT kind,quantity_micro,active_cash_micro FROM accepted_links").fetchall()==[(kind,100,cash)]
    assert con.execute("SELECT aggregate_reconciled FROM own_actions ORDER BY log_index").fetchall()==[(False,),(True,)]
    con.close()


@pytest.mark.parametrize("side,q,c,refund",[("BUY",100,90,50),("SELL",150,40,50),("BUY",100,150,110)])
def test_reserved_source_amounts_do_not_become_execution_price(tmp_path,side,q,c,refund):
    records=[raw("SELL" if side=="BUY" else "BUY"),raw(side,q=q,c=c,wallet="active",taker=OLD,log=2)]
    con,counts=setup(tmp_path,records)
    assert counts["accepted_refund_batches"]==1
    result=con.execute("SELECT maker_amount_filled,taker_amount_filled,refund_making_micro FROM own_actions WHERE source_role='active_aggregate'").fetchone()
    assert result==((40,100,refund) if side=="BUY" else (100,40,refund))
    con.close()


def test_source_pass_recovers_foreign_direct_prefix_in_scoped_transaction(tmp_path):
    # A token-filter-only scan would hide this direct/unrelated prefix and
    # mistakenly accept the scoped subset as a complete batch.
    records=[raw(token="99",wallet="foreign",taker="operator",log=1),
        raw("SELL",log=2),raw("BUY",wallet="active",taker=OLD,log=3)]
    con,counts=setup(tmp_path,records)
    assert counts["selected_raw_rows"]==3 and counts["rejected_relevant_batches"]==1
    assert counts["accepted_own_actions"]==0
    con.close()


def test_unscoped_complete_batches_do_not_contaminate_later_scoped_batches(tmp_path):
    records=[raw("SELL",token="99",log=1),raw("BUY",token="99",wallet="active",taker=OLD,log=2),
        raw("SELL",log=3),raw("BUY",wallet="active",taker=OLD,log=4)]
    con,counts=setup(tmp_path,records)
    assert counts["unscoped_batches"]==1 and counts["accepted_batches"]==1
    con.close()


def test_real_partial_order_fills_preserved_exact_replays_only_removed(tmp_path):
    records=[raw("SELL",log=1,order="same-order"),raw("SELL",log=2,order="same-order"),
        raw("BUY",q=200,c=80,wallet="active",taker=OLD,log=3)]
    con,counts=setup(tmp_path,records+[dict(records[0])])
    assert counts["exact_replays_removed"]==1 and counts["accepted_passive_actions"]==2
    assert counts["accepted_active_actions"]==1
    con.close()


def test_orphan_direct_fill_kept_and_blocks_complete_history(tmp_path):
    con,counts=setup(tmp_path,[raw()])
    assert counts["orphan_scoped_logs"]==1 and counts["accepted_own_actions"]==0
    assert con.execute("SELECT count(*) FROM orphan_logs").fetchone()[0]==1
    con.close()


@pytest.mark.parametrize("change,status",[
    ({"taker_amount_filled":101},"aggregate_received_amount_mismatch"),
    ({"maker_amount_filled":39},"reserved_making_below_effective_spending"),
    ({"maker":"wrong"},"invalid_asset_amount_fee_or_wallet")])
def test_irregular_batches_never_silently_accept(tmp_path,change,status):
    records=[raw("SELL"),raw("BUY",wallet="active",taker=OLD,log=2)]; records[1].update(change)
    con,counts=setup(tmp_path,records)
    assert counts["rejected_relevant_batches"]==1
    assert con.execute("SELECT status FROM batches").fetchone()[0]==status
    con.close()


def test_conflicting_log_payload_blocks_source(tmp_path):
    first=raw(); conflict=dict(first); conflict["maker"]="different"
    with pytest.raises(ValueError,match="Conflicting"):
        setup(tmp_path,[first,conflict])


def test_block_global_log_identity_conflict_blocks_source(tmp_path):
    with pytest.raises(ValueError,match="block-global"):
        setup(tmp_path,[raw(tx="first"),raw(tx="second")])


def test_legacy_buy_fee_cannot_exhaust_acquired_quantity(tmp_path):
    records=[raw("BUY",token="2",fee=100),raw("BUY",c=60,wallet="active",taker=OLD,log=2)]
    con,counts=setup(tmp_path,records)
    assert counts["rejected_relevant_batches"]==1 and counts["accepted_own_actions"]==0
    assert con.execute("SELECT status FROM batches").fetchone()[0]=='nonpositive_net_acquisition'
    con.close()


@pytest.mark.parametrize("exchange,fee_rule,version",[(OLD,"received_asset","legacy_reserved_making_v1"),
    (NEW,"collateral_extra_buy","ctf_exchange_v2_v1")])
def test_version_specific_fee_marker_and_original_source_amounts(tmp_path,exchange,fee_rule,version):
    records=[raw("SELL",fee=1,exchange=exchange),raw("BUY",wallet="active",taker=exchange,fee=2,log=2,exchange=exchange)]
    con,counts=setup(tmp_path,records)
    assert counts["accepted_batches"]==1
    assert con.execute("SELECT DISTINCT fee_rule,source_contract_version FROM own_actions").fetchall()==[(fee_rule,version)]
    con.close()


def surplus_case(records):
    active=records[-1]
    return {'active_execution_id':f"{active['exchange_address']}:{active['transaction_hash']}:{active['log_index']}",
        'settlement_surplus_cash_micro':8_000_000,'effective_quantity_micro':100,
        'effective_cash_micro':40,'original_making_micro':active['maker_amount_filled'],
        'original_taking_micro':active['taker_amount_filled'],'settlement_surplus_proof_id':'synthetic-native-proof'}


def test_proven_legacy_sell_surplus_preserves_all_actions_and_separates_execution_cash(tmp_path):
    records=[raw('BUY'),raw('SELL',c=8_000_040,wallet='active',taker=OLD,log=2)]
    con,counts=setup(tmp_path,records,surplus_cases=[surplus_case(records)],surplus_original_rows=records)
    assert counts['accepted_batches']==1 and counts['accepted_own_actions']==2
    assert counts['accepted_settlement_surplus_batches']==1
    assert counts['accepted_settlement_surplus_cash_micro']==8_000_000
    assert con.execute('''SELECT taker_amount_filled,original_taker_amount_filled,
        settlement_surplus_cash_micro FROM own_actions ORDER BY log_index''').fetchall()==[(100,100,0),(40,8_000_040,8_000_000)]
    assert con.execute('SELECT settlement_surplus_proof_id FROM batches').fetchone()[0]=='synthetic-native-proof'
    assert con.execute('SELECT settlement_surplus_proof_status FROM batches').fetchone()[0]=='verified_complete_native_sell_collateral_surplus'
    con.close()


def test_unproven_positive_sell_receiving_surplus_stays_rejected(tmp_path):
    records=[raw('BUY'),raw('SELL',c=8_000_040,wallet='active',taker=OLD,log=2)]
    con,counts=setup(tmp_path,records)
    assert counts['rejected_relevant_batches']==1 and counts['accepted_own_actions']==0
    assert counts['accepted_settlement_surplus_batches']==0
    assert con.execute('SELECT status FROM batches').fetchone()[0]=='aggregate_received_amount_mismatch'
    con.close()


@pytest.mark.parametrize('failure',['BUY','V2','wrong_cash','wrong_quantity','negative_surplus'])
def test_proof_never_relaxes_buy_surplus_v2_or_contradictory_amounts(tmp_path,failure):
    exchange=NEW if failure=='V2' else OLD
    active_side='BUY' if failure=='BUY' else 'SELL'
    records=[raw('SELL' if active_side=='BUY' else 'BUY',exchange=exchange),
             raw(active_side,c=8_000_040,q=100,wallet='active',taker=exchange,exchange=exchange,log=2)]
    if failure=='BUY': records[-1]['taker_amount_filled']=8_000_100
    proof=surplus_case(records)
    if failure=='wrong_cash': proof['effective_cash_micro']=41
    if failure=='wrong_quantity': proof['effective_quantity_micro']=101
    if failure=='negative_surplus': proof['settlement_surplus_cash_micro']=-1
    with pytest.raises(ValueError,match='do not match every accepted'):
        setup(tmp_path,records,surplus_cases=[proof],surplus_original_rows=records)


def test_complete_native_proof_cannot_match_only_a_subset_of_raw_group(tmp_path):
    records=[raw('BUY'),raw('SELL',c=8_000_040,wallet='active',taker=OLD,log=2)]
    with pytest.raises(ValueError,match='exact complete raw'):
        setup(tmp_path,[*records,raw(token='99',log=3)],surplus_cases=[surplus_case(records)],surplus_original_rows=records)
