from __future__ import annotations

from pathlib import Path
from argparse import Namespace
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import production_guard

from analysis.diagnostics.profit_taking_actions import EXCHANGE_ADDRESSES, RAW_FIELDS, V2_EXCHANGE_ADDRESSES
from analysis.diagnostics.profit_taking_source_audit import (
    choose_discovery_groups, choose_pilot_blocks, complete_group_indices,
    inspect_batches, parquet_metadata, validate_complements, normalize_receipt_log,
    choose_receipt_transactions, LEGACY_TOPIC, V2_TOPIC, full_source_support,
    retrieve_complete_transaction_groups, run_full_source_audit, merge_native_coverage,
    rejected_receipt_comparison, run_rejected_source_batch, decode_asset_transfers,
    ERC20_TRANSFER_TOPIC, ERC1155_SINGLE_TOPIC, ERC1155_BATCH_TOPIC,
    GET_COLLATERAL_SELECTOR, GET_CTF_SELECTOR,
)
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint,fingerprint

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


def published_source(tmp_path: Path, records: list[dict]) -> tuple[Path, Path, Path]:
    from analysis.diagnostics.build_profit_taking_source import prepare_source,reconcile_source
    raw_path=tmp_path/"raw.parquet"
    tokens=tmp_path/"tokens.parquet"
    pq.write_table(pa.Table.from_pylist(records),raw_path,row_group_size=1)
    pq.write_table(pa.Table.from_pylist([
        {"token_id":"1","market_id":"m","complement_token_id":"2"},
        {"token_id":"2","market_id":"m","complement_token_id":"1"}]),tokens)
    source=tmp_path/"source"; source.mkdir()
    con=duckdb.connect()
    try:
        counts=prepare_source(con,raw_path,tokens)
        counts.update(reconcile_source(con))
        con.execute("""CREATE TABLE exclusions AS SELECT e.*,b.status exclusion_reason FROM events e
            JOIN batches b ON e.transaction_hash=b.transaction_hash AND e.exchange_address=b.exchange_address
            AND e.batch_number=b.batch_number WHERE b.status NOT IN ('accepted','unscoped_batch')""")
        outputs={"batch_audit.parquet":"batches","batch_links.parquet":"accepted_links",
                 "orphan_logs.parquet":"orphan_logs","source_exclusions.parquet":"exclusions"}
        for name,relation in outputs.items():
            con.execute(f"COPY {relation} TO '{source/name}' (FORMAT PARQUET)")
    finally:
        con.close()
    status='complete' if not counts['rejected_relevant_batches'] and not counts['orphan_scoped_logs'] else 'blocked_source_reconciliation'
    summary={"status":status,"counts":counts,"raw_source":parquet_metadata(raw_path)[0]}
    (source/"summary.json").write_text(json.dumps(summary))
    manifest={"status":status,"counts":counts,"inputs":{"market_tokens":fingerprint(tokens)},
              "outputs":{p.name:artifact_fingerprint(p) for p in source.iterdir()}}
    (source/"manifest.json").write_text(json.dumps(manifest))
    return source,raw_path,tokens


def full_audit_args(tmp_path: Path, source: Path, raw_path: Path, tokens: Path, **changes) -> Namespace:
    values={"full_source":source,"raw_events":raw_path,"market_tokens":tokens,
            "receipt_limit":10,"max_complete_groups":256,"max_wide_bytes":1024**3,
            "run_dir":tmp_path/"audit"}
    values.update(changes)
    return Namespace(**values)


@pytest.fixture
def synthetic_git_provenance(monkeypatch):
    monkeypatch.setattr("analysis.diagnostics.profit_taking_source_audit.subprocess.check_output",
                        lambda command,**kwargs: "synthetic-test-commit\n" if command[1]=='rev-parse' else "")


def test_full_source_merge_selection_covers_every_observed_address(tmp_path,synthetic_git_provenance):
    records=[]
    for block,address in enumerate(sorted(EXCHANGE_ADDRESSES),1):
        passive=row("SELL",token="2",block=block,tx=f"tx-{block}")
        active=row("SELL",block=block,tx=f"tx-{block}",c=60,wallet="active",taker=address,log=2)
        passive["exchange_address"]=active["exchange_address"]=address
        records.extend([passive,active])
    source,raw_path,tokens=published_source(tmp_path,records)
    support,candidates=full_source_support(source,tokens,4)
    assert support["status"]=='pending_native_merge_receipts'
    assert support["merge_observed_addresses"]==sorted(EXCHANGE_ADDRESSES)
    assert len(candidates)==4 and sum(s["legs"] for s in support["accepted_match_support"])==4
    with pytest.raises(ValueError,match="every observed"):
        full_source_support(source,tokens,3)
    saved=run_full_source_audit(full_audit_args(tmp_path,source,raw_path,tokens))
    audits=pq.read_table(tmp_path/"audit/batch_audit.parquet").to_pylist()
    assert saved["status"]=='pending_native_merge_receipts'
    assert len(choose_receipt_transactions(audits,10))==4
    assert saved["read_budget"]["compressed_wide_read_bytes"]<=1024**3


def test_blocked_source_audit_has_reasons_and_no_partial_mechanism_profile_or_raw_read(tmp_path,monkeypatch,synthetic_git_provenance):
    source,raw_path,tokens=published_source(tmp_path,[
        row("SELL",token="2"),row("SELL",c=60,wallet="active",taker=EXCHANGE,log=2),
        row(block=2,tx="orphan",log=3)])
    monkeypatch.setattr("analysis.diagnostics.profit_taking_source_audit.parquet_metadata",
                        lambda path: pytest.fail("Blocked stage must not read raw source"))
    saved=run_full_source_audit(full_audit_args(tmp_path,source,raw_path,tokens))
    assert saved["status"]=='blocked_source_reconciliation'
    assert saved["counts"]["accepted_batches"]==1 and saved["counts"]["orphan_scoped_logs"]==1
    assert saved["accepted_match_support"]==[]
    assert sum(o["original_logs"] for o in saved["orphan_log_counts"] if o["scoped"])==1
    assert not (tmp_path/"audit/raw_pilot.parquet").exists()


def test_rejected_full_source_audit_preserves_exact_original_log_reason_counts(tmp_path):
    source,_,tokens=published_source(tmp_path,[row("SELL"),
        row("BUY",q=101,wallet="active",taker=EXCHANGE,log=2)])
    support,candidates=full_source_support(source,tokens,4)
    assert support["status"]=='blocked_source_reconciliation' and candidates==[]
    assert support["excluded_original_log_reasons"]==[
        {"exclusion_reason":"aggregate_received_amount_mismatch","original_logs":2}]


def test_merge_raw_pilot_keeps_unscoped_sibling_batch_for_complete_native_log_set(tmp_path,synthetic_git_provenance):
    source,raw_path,tokens=published_source(tmp_path,[
        row("SELL",token="99"),row("BUY",token="99",wallet="active",taker=EXCHANGE,log=2),
        row("SELL",token="2",log=3),row("SELL",c=60,wallet="active",taker=EXCHANGE,log=4)])
    saved=run_full_source_audit(full_audit_args(tmp_path,source,raw_path,tokens))
    pilot=pq.read_table(tmp_path/"audit/raw_pilot.parquet").to_pylist()
    audits=pq.read_table(tmp_path/"audit/batch_audit.parquet").to_pylist()
    assert len(pilot)==4 and [a["aggregate_log_index"] for a in audits]==[4]
    assert saved["merge_pilot_counts"]["unscoped_batch"]==1


@pytest.mark.parametrize("cap",['row_groups','wide_bytes'])
def test_merge_complete_group_budget_is_hard_cap(tmp_path,cap):
    source,raw_path,tokens=published_source(tmp_path,[
        row("SELL",token="2"),row("SELL",c=60,wallet="active",taker=EXCHANGE,log=2)])
    _,candidates=full_source_support(source,tokens,4)
    _,groups=parquet_metadata(raw_path)
    with pytest.raises(ValueError,match="cap"):
        retrieve_complete_transaction_groups(raw_path,groups,candidates,
                                             1 if cap=='row_groups' else 256,
                                             0 if cap=='wide_bytes' else 1024**3)


def test_full_source_without_merge_does_not_read_raw_or_prepare_receipts(tmp_path,monkeypatch,synthetic_git_provenance):
    source,raw_path,tokens=published_source(tmp_path,[row("SELL"),
        row("BUY",wallet="active",taker=EXCHANGE,log=2)])
    monkeypatch.setattr("analysis.diagnostics.profit_taking_source_audit.parquet_metadata",
                        lambda path: pytest.fail("No observed MERGE means no raw pilot"))
    saved=run_full_source_audit(full_audit_args(tmp_path,source,raw_path,tokens))
    assert saved["status"]=='complete_no_merge_native_gate_not_applicable'
    assert not (tmp_path/"audit/raw_pilot.parquet").exists()


def test_merge_read_requires_original_full_source_footer(tmp_path):
    source,raw_path,tokens=published_source(tmp_path,[row("SELL",token="2"),
        row("SELL",c=60,wallet="active",taker=EXCHANGE,log=2)])
    pq.write_table(pa.Table.from_pylist([row(token="2")]),raw_path)
    with pytest.raises(ValueError,match="source footer"):
        run_full_source_audit(full_audit_args(tmp_path,source,raw_path,tokens))
    assert not (tmp_path/"audit").exists()


def test_full_source_saved_artifact_tampering_blocks_support(tmp_path):
    source,_,tokens=published_source(tmp_path,[row("SELL"),
        row("BUY",wallet="active",taker=EXCHANGE,log=2)])
    manifest=json.loads((source/"manifest.json").read_text())
    manifest["outputs"]["batch_links.parquet"]["sha256"]="0"*64
    (source/"manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match="input changed"):
        full_source_support(source,tokens,4)


@pytest.mark.parametrize("failure",[None,'missing_address','unverified_receipt','normal_only','empty_evidence'])
def test_merge_native_gate_requires_all_observed_addresses_and_real_merge_evidence(failure):
    audits=[{"status":"accepted","exchange_address":address,"merge_legs":1,"transaction_hash":f"tx-{index}"}
            for index,address in enumerate(sorted(EXCHANGE_ADDRESSES))]
    evidence=[{"status":"verified","transaction_hash":a["transaction_hash"]} for a in audits]
    if failure=='missing_address': audits.pop()
    if failure=='unverified_receipt': evidence[0]["status"]='unavailable'
    if failure=='normal_only': audits[0]["merge_legs"]=0
    if failure=='empty_evidence': evidence=[]
    coverage=merge_native_coverage(audits,evidence,sorted(EXCHANGE_ADDRESSES))
    assert coverage["merge_native_gate_status"]==('verified' if failure is None else 'blocked_native_merge_evidence')


def rejected_case():
    tx='0x'+'ab'*32; passive='0x'+'11'*20; active='0x'+'22'*20; operator='0x'+'33'*20
    collateral='0x'+'aa'*20; ctf='0x'+'bb'*20
    originals=[row('BUY',q=90_000,c=83_700,wallet=passive,taker=active,fee=677,
                   log=4,tx=tx,order='0x'+'01'*32),
               row('SELL',q=90_000,c=8_083_700,wallet=active,taker=EXCHANGE,
                   log=6,tx=tx,order='0x'+'02'*32)]
    def native_log(address,topics,words,log):
        return {'address':address,'topics':topics,'data':'0x'+''.join(f'{w:064x}' for w in words),
                'blockNumber':'0x1','transactionHash':tx,'logIndex':hex(log),'removed':False}
    address_topic=lambda wallet:'0x'+'00'*12+wallet[2:]
    filled=lambda r:native_log(EXCHANGE,[LEGACY_TOPIC,r['order_hash'],address_topic(r['maker']),address_topic(r['taker'])],
        [int(r['maker_asset_id']),int(r['taker_asset_id']),r['maker_amount_filled'],r['taker_amount_filled'],r['fee']],r['log_index'])
    token=lambda sender,receiver,quantity,log:native_log(ctf,
        [ERC1155_SINGLE_TOPIC,address_topic(EXCHANGE),address_topic(sender),address_topic(receiver)],[1,quantity],log)
    cash=lambda sender,receiver,amount,log:native_log(collateral,
        [ERC20_TRANSFER_TOPIC,address_topic(sender),address_topic(receiver)],[amount],log)
    receipt={'status':'0x1','blockNumber':'0x1','transactionHash':tx,'logs':[
        token(active,EXCHANGE,90_000,0),cash(passive,EXCHANGE,83_700,1),
        token(EXCHANGE,passive,89_323,2),token(EXCHANGE,operator,677,3),filled(originals[0]),
        cash(EXCHANGE,active,8_083_700,5),filled(originals[1])]}
    return originals,receipt,collateral,ctf


def test_rejected_native_comparison_separates_matched_cash_from_actual_surplus_payout():
    original,receipt,collateral,ctf=rejected_case()
    result,native,transfers,wallets=rejected_receipt_comparison(original,receipt,collateral,ctf,COMPLEMENTS)
    assert result['status']=='verified_native_observations_only'
    assert result['native_original_log_set_exact_match'] and len(native)==2 and len(transfers)==5
    assert result['matched_cash_raw']==83_700 and result['matched_quantity_raw']==90_000
    assert result['unexplained_receiving_asset_excess_raw']==8_000_000
    assert result['exchange_collateral_flow']=={'incoming_raw':83_700,'outgoing_raw':8_083_700,'net_incoming_raw':-8_000_000}
    assert next(w for w in wallets if w['roles']=='active')['incoming_raw']==8_083_700
    assert next(w for w in wallets if w['roles']=='passive')['outgoing_raw']==83_700


def test_rejected_exclusion_subset_does_not_certify_a_larger_native_transaction():
    original,receipt,collateral,ctf=rejected_case()
    extra=dict(receipt['logs'][4]); extra['topics']=list(extra['topics'])
    extra['topics'][1]='0x'+'99'*32; extra['logIndex']='0x8'
    receipt['logs'].append(extra)
    result,native,_,_=rejected_receipt_comparison(original,receipt,collateral,ctf,COMPLEMENTS)
    assert result['status']=='blocked_native_problem_evidence'
    assert not result['native_original_log_set_exact_match'] and len(native)==3


def test_transfer_batch_arrays_are_decoded_without_losing_native_logs():
    _,receipt,collateral,ctf=rejected_case()
    log=dict(receipt['logs'][0]);log['topics']=list(log['topics']);log['topics'][0]=ERC1155_BATCH_TOPIC
    words=[64,160,2,1,2,2,100,200]
    log['data']='0x'+''.join(f'{w:064x}' for w in words)
    receipt['logs']=[log]
    transfers=decode_asset_transfers(receipt,collateral,ctf)
    assert [(r['asset_id'],r['amount_raw']) for r in transfers]==[('1',100),('2',200)]
    words[1]=128;log['data']='0x'+''.join(f'{w:064x}' for w in words)
    with pytest.raises(ValueError,match='arrays'):
        decode_asset_transfers(receipt,collateral,ctf)


@pytest.mark.parametrize('failure',[None,'getter_unavailable','network_unavailable'])
def test_saved_rejected_batch_diagnostic_is_bounded_sanitized_and_never_changes_source(tmp_path,monkeypatch,synthetic_git_provenance,failure):
    import requests
    original,receipt,collateral,ctf=rejected_case()
    source,_,tokens=published_source(tmp_path,original)
    source_before=(source/'manifest.json').read_bytes()
    calls=[]
    class Response:
        status_code=200
        def __init__(self,value): self.value=value
        def json(self): return self.value
    class Session:
        def post(self,url,json,timeout):
            calls.append(json)
            if failure=='network_unavailable':
                raise requests.RequestException('do-not-record-endpoint-credential')
            if json['method']=='eth_getTransactionReceipt': return Response({'result':receipt})
            selector=json['params'][0]['data']
            if selector==GET_COLLATERAL_SELECTOR and failure=='getter_unavailable':
                return Response({'error':{'code':-32000,'message':'do-not-record-endpoint-credential'}})
            address=collateral if selector==GET_COLLATERAL_SELECTOR else ctf
            return Response({'result':'0x'+'00'*12+address[2:]})
    monkeypatch.setenv('POLYGON_RPC_URL','https://synthetic.invalid/do-not-record-endpoint-credential')
    monkeypatch.setattr(requests,'Session',Session)
    run_dir=tmp_path/'problem-native'
    result=run_rejected_source_batch(source,tokens,run_dir)
    assert len(calls)==(1 if failure=='network_unavailable' else 3)
    assert sum(c['method']=='eth_getTransactionReceipt' for c in calls)==1
    assert result['status']==('verified_native_observations_only' if failure is None else 'blocked_native_problem_evidence')
    assert (source/'manifest.json').read_bytes()==source_before
    assert 'do-not-record-endpoint-credential' not in (run_dir/'summary.json').read_text()
    assert 'do-not-record-endpoint-credential' not in (run_dir/'manifest.json').read_text()
    assert pq.ParquetFile(run_dir/'original_rows.parquet').metadata.num_rows==2


@pytest.mark.parametrize('mode',['support','rejected_native'])
def test_audit_and_native_problem_spine_lineage_fail_before_rpc(tmp_path,monkeypatch,mode):
    import requests
    original,_,_,_=rejected_case()
    source,_,tokens=published_source(tmp_path,original)
    manifest=json.loads((source/'manifest.json').read_text())
    manifest['inputs']['market_tokens']['sha256']='0'*64
    (source/'manifest.json').write_text(json.dumps(manifest))
    monkeypatch.setattr(requests,'Session',lambda:pytest.fail('No RPC before token-spine lineage gate'))
    with pytest.raises(ValueError,match='token spine'):
        if mode=='support': full_source_support(source,tokens,10)
        else: run_rejected_source_batch(source,tokens,tmp_path/'native')
