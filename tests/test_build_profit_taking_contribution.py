from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.diagnostics.build_profit_taking_contribution import (
    _publish, build_contribution, parse_args, verify_parent_stages,
)
from analysis.diagnostics.build_profit_taking_ledger import build_ledger, parse_args as ledger_args
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint, fingerprint


EXCHANGE="0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"


def test_publication_preserves_aggregate_quantity_cash_above_double_integer_limit(tmp_path: Path) -> None:
    con=duckdb.connect()
    path=tmp_path/"large_totals.parquet"
    try:
        con.execute("CREATE TABLE executions(quantity BIGINT,cash BIGINT)")
        con.executemany("INSERT INTO executions VALUES (?,?)",[(2**53+2,2**53),(5,3)])
        con.execute("""CREATE TABLE totals AS SELECT 'atp' sport,count(*)::BIGINT n_executions,
            sum(quantity)::HUGEINT gross_quantity_micro,sum(cash)::HUGEINT gross_cash_micro,
            .125::DOUBLE calibration FROM executions""")
        assert _publish(con,"totals",path,"sport")==1
        saved=pq.read_table(path)
        row=saved.to_pylist()[0]
        assert saved.schema.field("gross_quantity_micro").type==pa.decimal128(38,0)
        assert saved.schema.field("gross_cash_micro").type==pa.decimal128(38,0)
        assert row["gross_quantity_micro"]==Decimal(2**53+7)
        assert row["gross_cash_micro"]==Decimal(2**53+3)
        assert isinstance(row["gross_quantity_micro"],Decimal)
        assert saved.schema.field("n_executions").type==pa.int64()
        assert row["n_executions"]==2
        assert saved.schema.field("calibration").type==pa.float64()
        assert row["calibration"]==.125
        reopened=con.execute(f"SELECT gross_quantity_micro,gross_cash_micro FROM read_parquet('{path}')").fetchone()
        assert reopened==(Decimal(2**53+7),Decimal(2**53+3))
    finally:
        con.close()


def own(side: str, token: str, block: int, log: int, wallet: str, qty: int, cash: int,
        *, active: bool=False) -> dict:
    making=cash if side=="BUY" else qty
    taking=qty if side=="BUY" else cash
    return dict(order_hash=f"order{block}-{log}",maker=wallet,taker=EXCHANGE if active else "counterparty",
        maker_asset_id="0" if side=="BUY" else token,taker_asset_id=token if side=="BUY" else "0",
        maker_amount_filled=making,taker_amount_filled=taking,fee=0,block_number=block,
        transaction_hash=f"tx{block}",log_index=log,exchange_address=EXCHANGE,
        execution_id=f"{EXCHANGE}:tx{block}:{log}",market_id="m",
        source_role="active_aggregate" if active else "passive",source_status="verified_own_action",
        source_contract_version="legacy_reserved_making_v1",fee_rule="received_asset",
        aggregate_reconciled=active,original_maker_amount_filled=making,
        original_taker_amount_filled=taking,refund_making_micro=0,settlement_surplus_cash_micro=0)


def fixture(tmp_path: Path):
    source=tmp_path/"source"
    source.mkdir()
    actions=[own("BUY","1",1,1,"seller",1000,200),own("SELL","1",1,2,"old-other",1000,200,active=True),
             own("SELL","1",2,1,"seller",200,190),own("BUY","2",2,2,"minter",800,790),
             own("BUY","1",2,3,"new-buyer",1000,200,active=True)]
    links=[dict(maker_execution_id=actions[0]["execution_id"],active_execution_id=actions[1]["execution_id"],
                kind="NORMAL",quantity_micro=1000,passive_cash_micro=200,active_cash_micro=200),
           dict(maker_execution_id=actions[2]["execution_id"],active_execution_id=actions[4]["execution_id"],
                kind="NORMAL",quantity_micro=200,passive_cash_micro=190,active_cash_micro=190),
           dict(maker_execution_id=actions[3]["execution_id"],active_execution_id=actions[4]["execution_id"],
                kind="MINT",quantity_micro=800,passive_cash_micro=790,active_cash_micro=10)]
    own_path,links_path=source/"own_actions.parquet",source/"batch_links.parquet"
    pq.write_table(pa.Table.from_pylist(actions),own_path)
    pq.write_table(pa.Table.from_pylist(links),links_path)
    tokens=tmp_path/"tokens.parquet"
    pq.write_table(pa.Table.from_pylist([
        dict(market_id="m",token_id="1",complement_token_id="2",won=True),
        dict(market_id="m",token_id="2",complement_token_id="1",won=False),
    ]),tokens)
    (source/"manifest.json").write_text(json.dumps(dict(status="complete",
        counts=dict(accepted_own_actions=len(actions),rejected_relevant_batches=0,orphan_scoped_logs=0),
        inputs=dict(market_tokens=fingerprint(tokens)),
        outputs={own_path.name:artifact_fingerprint(own_path),links_path.name:artifact_fingerprint(links_path)})))
    ledger=tmp_path/"ledger"
    build_ledger(ledger_args(["--own-actions",str(own_path),"--market-tokens",str(tokens),
        "--source-manifest",str(source/"manifest.json"),"--run-dir",str(ledger),"--threads","1","--memory-limit","1GB"]))
    clocks,blocks,flags=(tmp_path/(name+".parquet") for name in ("clocks","blocks","flags"))
    con=duckdb.connect()
    try:
        con.execute(f"""COPY (SELECT 'm' market_id,'atp' sport,'game' event_id,DATE '1970-01-01' market_date,
            to_timestamp(100) actual_start_utc,to_timestamp(200) actual_end_utc) TO '{clocks}' (FORMAT PARQUET)""")
    finally:
        con.close()
    pq.write_table(pa.Table.from_pylist([dict(block_number=1,timestamp=50),dict(block_number=2,timestamp=199)]),blocks)
    pq.write_table(pa.Table.from_pylist([dict(proxyWallet="new-buyer",is_nonhuman=True)]),flags)
    return parse_args(["--own-actions",str(own_path),"--batch-links",str(links_path),
        "--action-tags",str(ledger/"action_tags.parquet"),"--source-manifest",str(source/"manifest.json"),
        "--ledger-manifest",str(ledger/"manifest.json"),"--market-tokens",str(tokens),
        "--market-clocks",str(clocks),"--block-timestamps",str(blocks),"--wallet-flags",str(flags),
        "--run-dir",str(tmp_path/"contribution"),"--threads","1","--memory-limit","1GB"])


def test_serial_runner_publishes_both_grains_compact_summaries_and_conservation(tmp_path: Path) -> None:
    args=fixture(tmp_path)
    manifest=build_contribution(args)
    assert manifest["completion_status"]=="complete"
    assert manifest["counts"]["own_order_event"]==3
    assert manifest["counts"]["matched_execution"]==4
    assert manifest["counts"]["profiles"]==16200
    assert manifest["counts"]["tails"]==1620
    assert manifest["counts"]["late_delta"]==162
    assert manifest["counts"]["support"]==540
    con=duckdb.connect()
    try:
        con.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet('{args.run_dir}/profiles.parquet')")
        own_d10=con.execute("SELECT n_executions,calibration_raw,exit_contribution_raw,suppressed FROM p WHERE grain='own_order_event' AND sport='atp' AND sample='all_trades' AND \"window\"='live_99_100' AND weighting='fill' AND price_bin=10").fetchone()
        matched_d10=con.execute("SELECT n_executions,calibration_raw,exit_contribution_raw,suppressed FROM p WHERE grain='matched_execution' AND sport='atp' AND sample='all_trades' AND \"window\"='live_99_100' AND weighting='fill' AND price_bin=10").fetchone()
        assert own_d10==(1,pytest.approx(-.9875),pytest.approx(0),True)
        assert matched_d10==(2,pytest.approx((.05-.9875)/2),pytest.approx(.025),True)
        assert con.execute("SELECT count(*) FROM p WHERE calibration IS NOT NULL").fetchone()[0]==0
        totals=pq.read_table(args.run_dir/"grain_totals.parquet").to_pylist()
        assert {row["gross_quantity_micro"] for row in totals}=={2800}
        assert {row["gross_cash_micro"] for row in totals}=={1190}
        assert {row["n_own_buy_events"] for row in totals}=={3}
        assert totals[0]["quantity_weighted_calibration"]==pytest.approx(totals[1]["quantity_weighted_calibration"])
        assert manifest["contract"]["uncertainty"].startswith("Not estimated")
    finally:
        con.close()
    with pytest.raises(FileExistsError):
        build_contribution(args)


@pytest.mark.parametrize("failure",[
    "blocked_source","blocked_ledger","source_count","source_hash","link_hash","token_lineage",
    "ledger_hash","ledger_count","ledger_float_count","ledger_input_lineage","wrong_source_parent","wrong_ledger_parent",
])
def test_parent_gates_fail_closed_before_publication(tmp_path: Path,failure: str) -> None:
    args=fixture(tmp_path)
    source=json.loads(args.source_manifest.read_text())
    ledger=json.loads(args.ledger_manifest.read_text())
    if failure=="blocked_source": source["status"]="blocked_source_reconciliation"
    elif failure=="blocked_ledger": ledger["completion_status"]="partial"
    elif failure=="source_count": source["counts"]["accepted_own_actions"]+=1
    elif failure=="source_hash": source["outputs"]["own_actions.parquet"]["sha256"]="wrong"
    elif failure=="link_hash": source["outputs"]["batch_links.parquet"]["sha256"]="wrong"
    elif failure=="token_lineage": source["inputs"]["market_tokens"]["sha256"]="wrong"
    elif failure=="ledger_hash": ledger["outputs"]["action_tags.parquet"]["sha256"]="wrong"
    elif failure=="ledger_count": ledger["counts"]["output_actions"]+=1
    elif failure=="ledger_float_count": ledger["counts"]["output_actions"]=float(ledger["counts"]["output_actions"])
    elif failure=="ledger_input_lineage": ledger["inputs"]["own_actions"]["sha256"]="wrong"
    elif failure=="wrong_source_parent": args.source_manifest=tmp_path/"manifest.json"
    elif failure=="wrong_ledger_parent": args.ledger_manifest=tmp_path/"manifest.json"
    args.source_manifest.write_text(json.dumps(source))
    args.ledger_manifest.write_text(json.dumps(ledger))
    with pytest.raises(ValueError):
        build_contribution(args)
    assert not args.run_dir.exists()


@pytest.mark.parametrize("failure",["block_float","clock_duration","clock_duplicate","clock_wrong_sport",
    "token_wrong_outcome","flag_conflict","missing_block","flag_type"])
def test_metadata_gates_fail_closed_and_leave_no_published_run(tmp_path: Path,failure: str) -> None:
    args=fixture(tmp_path)
    if failure=="block_float":
        pq.write_table(pa.Table.from_pylist([dict(block_number=1,timestamp=50.0),dict(block_number=2,timestamp=199.0)]),args.block_timestamps)
    elif failure=="missing_block":
        pq.write_table(pa.Table.from_pylist([dict(block_number=1,timestamp=50)]),args.block_timestamps)
    elif failure.startswith("clock_"):
        table=pq.read_table(args.market_clocks)
        rows=table.to_pylist()
        if failure=="clock_duration": rows[0]["actual_end_utc"]=rows[0]["actual_start_utc"]
        elif failure=="clock_duplicate": rows.append(dict(rows[0]))
        else: rows[0]["sport"]="wta"
        pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),args.market_clocks)
    elif failure=="token_wrong_outcome":
        rows=pq.read_table(args.market_tokens).to_pylist()
        rows[1]["won"]=True
        pq.write_table(pa.Table.from_pylist(rows),args.market_tokens)
        source=json.loads(args.source_manifest.read_text());source["inputs"]["market_tokens"]=fingerprint(args.market_tokens)
        args.source_manifest.write_text(json.dumps(source))
        ledger=json.loads(args.ledger_manifest.read_text());ledger["inputs"]["market_tokens"]=fingerprint(args.market_tokens)
        ledger["inputs"]["source_manifest"]=fingerprint(args.source_manifest)
        args.ledger_manifest.write_text(json.dumps(ledger))
    elif failure=="flag_conflict":
        pq.write_table(pa.Table.from_pylist([dict(proxyWallet="same",is_nonhuman=True),dict(proxyWallet="SAME",is_nonhuman=False)]),args.wallet_flags)
    else:
        pq.write_table(pa.Table.from_pylist([dict(proxyWallet="same",is_nonhuman="yes")]),args.wallet_flags)
    with pytest.raises(ValueError):
        build_contribution(args)
    assert not args.run_dir.exists()


def test_cli_rejects_local_production_without_running_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    from analysis.diagnostics import build_profit_taking_contribution as module
    import production_guard
    monkeypatch.setattr(production_guard.platform,"system",lambda:"Darwin")
    monkeypatch.setattr(module,"parse_args",lambda:object())
    monkeypatch.setattr(module,"build_contribution",lambda _:pytest.fail("local production builder must not run"))
    with pytest.raises(RuntimeError,match="canonical EC2"):
        module.main()


def test_input_mutation_is_detected_and_not_published(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from analysis.diagnostics import build_profit_taking_contribution as module
    args=fixture(tmp_path)
    original=module.create_sport_contribution_summaries
    changed=False
    def mutate(con,relation,**kwargs):
        nonlocal changed
        result=original(con,relation,**kwargs)
        if not changed:
            changed=True
            # Adding a duplicate consistent flag remains schema-valid, but is
            # nevertheless a change to a frozen metadata input.
            rows=pq.read_table(args.wallet_flags).to_pylist()
            rows.append(dict(rows[0]))
            pq.write_table(pa.Table.from_pylist(rows),args.wallet_flags)
        return result
    monkeypatch.setattr(module,"create_sport_contribution_summaries",mutate)
    with pytest.raises(ValueError,match="input changed"):
        build_contribution(args)
    assert not args.run_dir.exists()


def test_disk_reserve_failure_starts_no_stage(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    from collections import namedtuple
    from analysis.diagnostics import build_profit_taking_contribution as module
    args=fixture(tmp_path)
    usage=namedtuple("usage","total used free")
    monkeypatch.setattr(module.shutil,"disk_usage",lambda _:usage(20*1024**3,19*1024**3,1024**3))
    with pytest.raises(ValueError,match="Insufficient disk reserve"):
        build_contribution(args)
    assert not args.run_dir.exists()


def test_production_parent_gate_requires_native_proof(tmp_path: Path) -> None:
    from analysis.diagnostics.build_profit_taking_contribution import INPUT_NAMES
    args=fixture(tmp_path)
    paths={name:Path(getattr(args,name)) for name in INPUT_NAMES}
    assert verify_parent_stages(paths)["native_source_gates"] is None
    with pytest.raises(ValueError,match="verified native"):
        verify_parent_stages(paths,require_native=True)


def test_native_proof_if_present_must_match_saved_parent_evidence(tmp_path: Path) -> None:
    args=fixture(tmp_path)
    ledger=json.loads(args.ledger_manifest.read_text())
    ledger["native_source_gates"]={"status":"verified_no_observed_merge"}
    args.ledger_manifest.write_text(json.dumps(ledger))
    with pytest.raises(ValueError,match="source-audit manifest path"):
        build_contribution(args)


def test_saved_ledger_surplus_proof_must_match_exact_source_proof_object(tmp_path: Path) -> None:
    args=fixture(tmp_path)
    ledger=json.loads(args.ledger_manifest.read_text())
    ledger["source_stage_gates"]["settlement_surplus_gates"]["policy"]="changed proof policy"
    args.ledger_manifest.write_text(json.dumps(ledger))
    with pytest.raises(ValueError,match="Ledger settlement-surplus proof differs"):
        build_contribution(args)
    assert not args.run_dir.exists()


def test_mechanism_summary_keeps_merge_sales_without_synthetic_buy_observations(tmp_path: Path) -> None:
    args=fixture(tmp_path)
    actions=pq.read_table(args.own_actions).to_pylist()
    passive=own("SELL","2",3,1,"merger",300,3)
    active=own("SELL","1",3,2,"seller",300,297,active=True)
    actions.extend((passive,active))
    pq.write_table(pa.Table.from_pylist(actions),args.own_actions)
    links=pq.read_table(args.batch_links).to_pylist()
    links.append(dict(maker_execution_id=passive["execution_id"],active_execution_id=active["execution_id"],
                      kind="MERGE",quantity_micro=300,passive_cash_micro=3,active_cash_micro=297))
    pq.write_table(pa.Table.from_pylist(links),args.batch_links)
    source=json.loads(args.source_manifest.read_text())
    source["counts"]["accepted_own_actions"]=len(actions)
    source["outputs"]={args.own_actions.name:artifact_fingerprint(args.own_actions),args.batch_links.name:artifact_fingerprint(args.batch_links)}
    args.source_manifest.write_text(json.dumps(source))
    ledger=tmp_path/"ledger-with-merge"
    build_ledger(ledger_args(["--own-actions",str(args.own_actions),"--market-tokens",str(args.market_tokens),
        "--source-manifest",str(args.source_manifest),"--run-dir",str(ledger),"--threads","1","--memory-limit","1GB"]))
    args.action_tags=ledger/"action_tags.parquet"
    args.ledger_manifest=ledger/"manifest.json"
    blocks=pq.read_table(args.block_timestamps).to_pylist()+[dict(block_number=3,timestamp=200)]
    pq.write_table(pa.Table.from_pylist(blocks),args.block_timestamps)
    result=build_contribution(args)
    assert result["counts"]["own_order_event"]==3 and result["counts"]["matched_execution"]==4
    summary=pq.read_table(args.run_dir/"mechanism_summary.parquet").to_pylist()
    row=next(row for row in summary if row["sport"]=="atp" and row["window"]=="live_99_100" and row["role"]=="all")
    assert row["own_actions"]==5 and row["own_sells"]==3
    assert row["favorite_sell_events"]==2 and row["primary_exit_events"]==2
    assert row["primary_exit_quantity_micro"]==500
    assert row["merge_disposal_events"]==2
    assert row["merge_disposal_gross_quantity_micro"]==600
    assert row["allocated_primary_merge_quantity_micro"]==pytest.approx(300)
    assert row["allocated_unmatched_merge_quantity_micro"]==pytest.approx(300)
    assert row["population"]=="all_own_actions_no_price_or_actor_filter"


@pytest.mark.parametrize("mutate_receipt",[False,True])
def test_positive_native_surplus_proof_is_reopened_before_contribution_publication(
    tmp_path: Path,monkeypatch: pytest.MonkeyPatch,mutate_receipt: bool,
) -> None:
    """Reuse the actual synthetic native receipt fixture, not a mocked proof."""
    from argparse import Namespace
    import test_profit_taking_source_audit as native_fixture
    from analysis.diagnostics import build_profit_taking_source as source_builder
    from analysis.diagnostics import build_profit_taking_contribution as module

    args=fixture(tmp_path)
    native_root=tmp_path/"native-case"
    native_root.mkdir()
    original_published_source=native_fixture.published_source
    def source_with_resolved_tokens(path,records):
        source,raw,tokens=original_published_source(path,records)
        rows=pq.read_table(tokens).to_pylist()
        for record in rows: record["won"]=record["token_id"]=="1"
        pq.write_table(pa.Table.from_pylist(rows),tokens)
        manifest=json.loads((source/"manifest.json").read_text())
        manifest["inputs"]["market_tokens"]=fingerprint(tokens)
        (source/"manifest.json").write_text(json.dumps(manifest))
        return source,raw,tokens
    monkeypatch.setattr(native_fixture,"published_source",source_with_resolved_tokens)
    monkeypatch.setattr(source_builder,"require_native_pilot",lambda *paths:None)
    monkeypatch.setattr(source_builder.subprocess,"check_output",
        lambda command,**kwargs:"synthetic-test-commit\n" if command[1]=="rev-parse" else "")
    proof,tokens=native_fixture.saved_surplus_proof(native_root)
    parent=Path(json.loads(proof.read_text())["inputs"]["source_manifest"]["path"])
    raw=Path(json.loads(parent.read_text())["inputs"]["raw_events"]["path"])
    for name in ("pilot","receipts"):
        directory=native_root/name;directory.mkdir();(directory/"manifest.json").write_text("{}")
    source=native_root/"source-verified"
    source_builder.run(Namespace(raw_events=raw,market_tokens=tokens,
        source_pilot=native_root/"pilot",receipt_pilot=native_root/"receipts",
        run_dir=source,temp_directory=native_root/"spill",threads=1,memory_limit="1GB",
        surplus_receipt_manifest=[proof],source_replay_manifest=parent))
    ledger=native_root/"ledger-verified"
    build_ledger(ledger_args(["--own-actions",str(source/"own_actions.parquet"),"--market-tokens",str(tokens),
        "--source-manifest",str(source/"manifest.json"),"--run-dir",str(ledger),"--threads","1","--memory-limit","1GB"]))
    args.own_actions=source/"own_actions.parquet"
    args.batch_links=source/"batch_links.parquet"
    args.source_manifest=source/"manifest.json"
    args.action_tags=ledger/"action_tags.parquet"
    args.ledger_manifest=ledger/"manifest.json"
    args.market_tokens=tokens
    original_summary=module.create_sport_contribution_summaries
    changed=False
    def mutate_after_classification(con,relation,**kwargs):
        nonlocal changed
        result=original_summary(con,relation,**kwargs)
        if mutate_receipt and not changed:
            changed=True
            (proof.parent/"native_receipt.json").write_text("{}")
        return result
    monkeypatch.setattr(module,"create_sport_contribution_summaries",mutate_after_classification)
    if mutate_receipt:
        with pytest.raises(ValueError,match="native proof artifact fingerprint"):
            build_contribution(args)
        assert not args.run_dir.exists()
    else:
        manifest=build_contribution(args)
        gate=manifest["parent_stage_gates"]["source"]["settlement_surplus_gates"]
        assert gate["status"]=="verified_complete_native_sell_collateral_surplus"
        assert len(gate["cases"])==1 and len(gate["proof_lineage"])==1
        assert gate["cases"][0]["settlement_surplus_cash_micro"]==8_000_000
        assert manifest["counts"]["own_order_event"]==manifest["counts"]["matched_execution"]==1
        totals=pq.read_table(args.run_dir/"grain_totals.parquet").to_pylist()
        assert {row["gross_cash_micro"] for row in totals}=={Decimal(83_700)}
