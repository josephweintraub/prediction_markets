from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import production_guard

from analysis.diagnostics.profit_taking_actions import EXCHANGE_ADDRESSES, RAW_FIELDS, V2_EXCHANGE_ADDRESSES
from analysis.diagnostics.profit_taking_source_audit import (
    choose_discovery_groups, choose_pilot_blocks, complete_group_indices,
    inspect_batches, parquet_metadata, validate_complements, normalize_receipt_log,
    choose_receipt_transactions, LEGACY_TOPIC, V2_TOPIC,
)

EXCHANGE = sorted(EXCHANGE_ADDRESSES)[0]
OTHER_EXCHANGE = sorted(EXCHANGE_ADDRESSES)[1]
COMPLEMENTS = {"1":"2","2":"1"}


def row(side="BUY", token="1", q=100, c=40, *, wallet="passive", taker="active",
        block=1, log=1, fee=0, tx="tx", order=None):
    return dict(zip(RAW_FIELDS, (order or f"order-{log}",wallet,taker,
        "0" if side=="BUY" else token, token if side=="BUY" else "0",
        c if side=="BUY" else q, q if side=="BUY" else c,fee,block,tx,log,EXCHANGE)))


@pytest.mark.parametrize("active_side,passive_side,passive_token,active_cash,kind",[
    ("BUY","SELL","1",40,"normal"),("SELL","BUY","1",40,"normal"),
    ("BUY","BUY","2",60,"mint"),("SELL","SELL","2",60,"merge"),
])
def test_bounded_batch_recovery_all_mechanisms(active_side,passive_side,passive_token,active_cash,kind):
    maker=row(passive_side,passive_token)
    active=row(active_side,c=active_cash,wallet="active",taker=EXCHANGE,log=2)
    audits,counts=inspect_batches([maker,active],COMPLEMENTS)
    assert counts["accepted"]==1
    assert audits[0][f"{kind}_legs"]==1
    assert audits[0]["effective_cash_micro"]==active_cash


@pytest.mark.parametrize("side,original_q,original_c,refund",[("BUY",100,90,50),("SELL",150,40,50)])
def test_refund_is_explicit_not_a_tolerance(side,original_q,original_c,refund):
    maker=row("SELL" if side=="BUY" else "BUY")
    active=row(side,q=original_q,c=original_c,wallet="active",taker=EXCHANGE,log=2)
    audits,counts=inspect_batches([maker,active],COMPLEMENTS)
    assert counts["accepted_refund_batches"]==1
    assert audits[0]["refund_making_micro"]==refund


def test_multiple_aggregates_segment_one_transaction_and_keep_partial_order_fills():
    records=[row("SELL",log=1,order="repeat"),
             row("BUY",c=40,wallet="active",taker=EXCHANGE,log=2),
             row("SELL",log=3,order="repeat"),
             row("BUY",c=40,wallet="active",taker=EXCHANGE,log=4)]
    audits,counts=inspect_batches(records+[dict(records[0])],COMPLEMENTS)
    assert counts["accepted"]==2 and counts["exact_replay_rows"]==1
    assert [a["passive_logs"] for a in audits]==[1,1]


@pytest.mark.parametrize("change",[{"taker_amount_filled":101},{"maker_amount_filled":39},{"maker":"wrong"}])
def test_contradictory_aggregate_fail_closed(change):
    records=[row("SELL"),row("BUY",wallet="active",taker=EXCHANGE,log=2)]
    records[1].update(change)
    audits,counts=inspect_batches(records,COMPLEMENTS)
    assert counts["rejected"]==1 and audits[0]["reason"]


def test_unassigned_and_unscoped_logs_are_not_silently_reconstructed():
    audits,counts=inspect_batches([row(token="3"),row(token="3",wallet="active",taker=EXCHANGE,log=2),row(log=3)],COMPLEMENTS)
    assert audits[0]["status"]=="unscoped_batch"
    assert counts["unassigned_scoped_logs"]==1


def test_received_asset_fees_retain_source_evidence():
    audits,counts=inspect_batches([row("SELL",fee=1),row("BUY",wallet="active",taker=EXCHANGE,log=2,fee=2)],COMPLEMENTS)
    assert counts["accepted_nonzero_fee_batches"]==1
    assert audits[0]["active_fee_micro"]==2 and audits[0]["passive_fee_logs"]==1


def test_footer_only_inspection_and_schema_gate(tmp_path:Path):
    path=tmp_path/"raw.parquet"
    pq.write_table(pa.Table.from_pylist([row(),row(block=4,log=2)]),path,row_group_size=1)
    info,groups=parquet_metadata(path)
    assert info["rows"]==2 and info["block_min"]==1 and info["block_max"]==4
    assert len(info["footer_sha256"])==64 and len(groups)==2
    assert info["source_content_hash"]=="not_computed_in_bounded_pilot"
    pq.write_table(pa.table({"wrong":[1]}),path)
    with pytest.raises(ValueError,match="schema"):
        parquet_metadata(path)


def test_symmetric_unique_token_spine():
    assert validate_complements([{"token_id":"1","complement_token_id":"2","market_id":"m"},{"token_id":"2","complement_token_id":"1","market_id":"m"}])==COMPLEMENTS
    with pytest.raises(ValueError):
        validate_complements([{"token_id":"1","complement_token_id":"2"}])


@pytest.mark.parametrize("field,value",[("market_id",None),("market_id",""),("token_id",None),
    ("complement_token_id"," "),("token_id","0")])
def test_null_blank_or_collateral_token_identity_never_coerced(field,value):
    rows=[{"token_id":"1","complement_token_id":"2","market_id":"m"},
          {"token_id":"2","complement_token_id":"1","market_id":"m"}]
    rows[0][field]=value
    with pytest.raises(ValueError):
        validate_complements(rows)


def test_bounded_rowgroup_and_block_selection():
    groups=[{"row_group":i,"block_min":1+10*i,"block_max":10+10*i,"address_min":a,"address_max":a}
            for i,a in enumerate(sorted(EXCHANGE_ADDRESSES))]
    assert choose_discovery_groups(groups,[1,20],len(EXCHANGE_ADDRESSES))==list(range(len(EXCHANGE_ADDRESSES)))
    assert complete_group_indices(groups,[5,15],2)==[0,1]
    with pytest.raises(ValueError,match="cap"):
        complete_group_indices(groups,[5,15],1)
    rows=[row(block=n,log=n) for n in range(1,10)]
    chosen=choose_pilot_blocks(rows,4)
    assert len(chosen)==4 and 1 in chosen and 9 in chosen


@pytest.mark.parametrize("v2,side",[(False,0),(False,1),(True,0),(True,1)])
def test_native_receipt_event_normalization(v2,side):
    address=sorted(V2_EXCHANGE_ADDRESSES)[0] if v2 else EXCHANGE
    words=[side,99,40,100,2,0,0] if v2 else [0 if side==0 else 99,99 if side==0 else 0,40,100,2]
    native={"address":address,"topics":[V2_TOPIC if v2 else LEGACY_TOPIC,"0x"+"01"*32,
        "0x"+"00"*12+"02"*20,"0x"+"00"*12+"03"*20],
        "data":"0x"+"".join(f"{w:064x}" for w in words),"blockNumber":"0x10",
        "transactionHash":"0x"+"04"*32,"logIndex":"0x2","removed":False}
    result=normalize_receipt_log(native)
    assert result["maker_asset_id"]==("0" if side==0 else "99")
    assert result["taker_asset_id"]==("99" if side==0 else "0")
    assert result["maker"]=="0x"+"02"*20 and result["fee"]==2
    native["topics"][0]=LEGACY_TOPIC if v2 else V2_TOPIC
    with pytest.raises(ValueError,match="generation"):
        normalize_receipt_log(native)


def test_receipt_selection_is_bounded_and_mechanism_aware():
    audits,_=inspect_batches([row("SELL"),row("BUY",wallet="active",taker=EXCHANGE,log=2)],COMPLEMENTS)
    assert choose_receipt_transactions(audits,10)==["tx"]
    with pytest.raises(ValueError):
        choose_receipt_transactions(audits,21)


def test_v2_fee_rule_does_not_treat_buy_cash_fee_as_token_loss():
    records=[row("SELL",fee=1),row("BUY",wallet="active",taker=sorted(V2_EXCHANGE_ADDRESSES)[0],log=2,fee=2)]
    for record in records:
        record["exchange_address"]=sorted(V2_EXCHANGE_ADDRESSES)[0]
    audits,counts=inspect_batches(records,COMPLEMENTS)
    assert counts["accepted"]==1 and audits[0]["fee_rule"]=="collateral_extra_buy"


@pytest.mark.parametrize("system,guard_path,mounted",[
    ("Darwin","/home/ubuntu/prediction_markets/production_guard.py",True),
    ("Linux","/tmp/noncanonical/production_guard.py",True),
    ("Linux","/home/ubuntu/prediction_markets/production_guard.py",False),
])
def test_shared_production_guard_refuses_nonproduction_environment(monkeypatch,system,guard_path,mounted):
    monkeypatch.setattr(production_guard.platform,"system",lambda:system)
    monkeypatch.setattr(production_guard,"__file__",guard_path)
    monkeypatch.setattr(production_guard.os.path,"ismount",lambda path:mounted)
    with pytest.raises(RuntimeError,match="canonical EC2 environment"):
        production_guard.require_production_host()
