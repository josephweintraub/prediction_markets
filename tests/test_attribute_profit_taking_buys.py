from __future__ import annotations

import duckdb
import pytest

from analysis.diagnostics.attribute_profit_taking_buys import (
    HISTORY_STATUS, LINK_COLUMNS, OWN_COLUMNS, TAG_COLUMNS,
    create_buy_attribution, enrich_buy_attribution,
)
from analysis.diagnostics.profit_taking_contribution import create_contribution_summaries


LEGACY="0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
V2="0xe111180000d2663c0091e4f400237545b87b996b"


def action(tx: str,log: int,side: str,token: str,quantity: int,cash: int,
           *,role: str="passive",wallet: str="wallet",fee: int=0,block: int=1,exchange: str=LEGACY) -> dict:
    return dict(execution_id=f"{exchange}:{tx}:{log}",market_id="m",maker=wallet,
        maker_asset_id="0" if side=="BUY" else token,taker_asset_id=token if side=="BUY" else "0",
        maker_amount_filled=cash if side=="BUY" else quantity,
        taker_amount_filled=quantity if side=="BUY" else cash,fee=fee,block_number=block,
        transaction_hash=tx,log_index=log,exchange_address=exchange,source_role=role,
        source_status="verified_own_action",source_contract_version="legacy_reserved_making_v1" if exchange==LEGACY else "ctf_exchange_v2_v1",
        fee_rule="received_asset" if exchange==LEGACY else "collateral_extra_buy",aggregate_reconciled=role=="active_aggregate")


def tag(own: dict,*,primary: int=0,hedge: int=0,unmatched: int=0) -> dict:
    side="BUY" if own["maker_asset_id"]=="0" else "SELL"
    q=own["taker_amount_filled"] if side=="BUY" else own["maker_amount_filled"]
    cash=own["maker_amount_filled"] if side=="BUY" else own["taker_amount_filled"]
    net=q-(own["fee"] if own["fee_rule"]=="received_asset" else 0) if side=="BUY" else None
    return dict(execution_id=own["execution_id"],market_id=own["market_id"],
        token_id=own["taker_asset_id"] if side=="BUY" else own["maker_asset_id"],
        wallet=own["maker"],side=side,block_number=own["block_number"],
        transaction_hash=own["transaction_hash"],log_index=own["log_index"],
        exchange_address=own["exchange_address"],gross_quantity_micro=q,gross_cash_micro=cash,
        fee_micro=own["fee"],net_acquired_quantity_micro=net,primary_exit_quantity_micro=primary,
        hedge_profitable_quantity_micro=hedge,unmatched_disposal_quantity_micro=unmatched,
        primary_exit_fraction=primary/q,hedge_fraction=hedge/net if net else 0,
        unmatched_disposal_fraction=unmatched/q,history_status=HISTORY_STATUS,
        fee_rule=own["fee_rule"],source_contract_version=own["source_contract_version"])


def link(maker: dict,active: dict,kind: str,q: int,maker_cash: int,active_cash: int) -> dict:
    return dict(maker_execution_id=maker["execution_id"],active_execution_id=active["execution_id"],
                kind=kind,quantity_micro=q,passive_cash_micro=maker_cash,active_cash_micro=active_cash)


def connection(actions: list[dict],tags: list[dict],links: list[dict]) -> duckdb.DuckDBPyConnection:
    con=duckdb.connect()
    integer_fields={"maker_amount_filled","taker_amount_filled","fee","block_number","log_index",
                    "gross_quantity_micro","gross_cash_micro","fee_micro","net_acquired_quantity_micro",
                    "primary_exit_quantity_micro","hedge_profitable_quantity_micro","unmatched_disposal_quantity_micro",
                    "quantity_micro","passive_cash_micro","active_cash_micro"}
    fractions={"primary_exit_fraction","hedge_fraction","unmatched_disposal_fraction"}
    for relation,columns,records in (("own",OWN_COLUMNS,actions),("tags",TAG_COLUMNS,tags),("links",LINK_COLUMNS,links)):
        schema=",".join(f"{name} "+("BIGINT" if name in integer_fields else "DOUBLE" if name in fractions
                                    else "BOOLEAN" if name=="aggregate_reconciled" else "VARCHAR") for name in columns)
        con.execute(f"CREATE TABLE {relation}({schema})")
        if records:
            con.executemany(f"INSERT INTO {relation} VALUES ({','.join('?' for _ in columns)})",
                            [tuple(record[name] for name in columns) for record in records])
    return con


def rows(con: duckdb.DuckDBPyConnection,relation: str) -> dict[str,dict]:
    cursor=con.execute(f"SELECT * FROM {relation} ORDER BY execution_id")
    columns=[item[0] for item in cursor.description]
    return {row[0]:dict(zip(columns,row)) for row in cursor.fetchall()}


def metadata(con: duckdb.DuckDBPyConnection,*,winner: str="1",timestamp: int=199) -> None:
    con.execute("CREATE TABLE tokens(token_id VARCHAR,market_id VARCHAR,complement_token_id VARCHAR,won BOOLEAN)")
    con.executemany("INSERT INTO tokens VALUES (?,?,?,?)",[("1","m","2",winner=="1"),("2","m","1",winner=="2")])
    con.execute("""CREATE TABLE clocks AS SELECT 'm' market_id,'atp' sport,'match' event_id,
        DATE '1970-01-01' market_date,to_timestamp(100) actual_start_utc,to_timestamp(200) actual_end_utc""")
    con.execute("CREATE TABLE blocks(block_number BIGINT,timestamp BIGINT)")
    con.execute("INSERT INTO blocks VALUES (1,?)",[timestamp])
    con.execute("CREATE TABLE flags(proxyWallet VARCHAR,is_nonhuman BOOLEAN)")
    con.execute("INSERT INTO flags VALUES ('BOT',TRUE)")


def normal_fixture() -> tuple[list[dict],list[dict],list[dict]]:
    seller=action("normal",1,"SELL","1",100,95,wallet="seller")
    buyer=action("normal",2,"BUY","1",100,95,role="active_aggregate",wallet="bot")
    return [seller,buyer],[tag(seller,primary=40,unmatched=10),tag(buyer)],[link(seller,buyer,"NORMAL",100,95,95)]


def test_normal_passive_favorite_sale_to_original_active_buy() -> None:
    con=connection(*normal_fixture())
    try:
        names=create_buy_attribution(con,"own","tags","links")
        result=rows(con,names["buy_tags"])
        assert list(result)==[f"{LEGACY}:normal:2"]
        buy=result[f"{LEGACY}:normal:2"]
        assert buy["exit_fraction"]==.4
        assert buy["qualified_hedge_fraction"]==0
        assert buy["unmatched_seller_acquisition_fraction"]==.1
        assert buy["source_role"]=="active_aggregate"
        assert not buy["is_synthetic"]
        assert buy["price"]==.95
        assert "won" not in buy and "residual" not in buy
    finally:
        con.close()


def test_active_sale_fraction_is_proportional_across_true_passive_buys() -> None:
    b1=action("active-sell",1,"BUY","1",40,36,wallet="b1")
    b2=action("active-sell",2,"BUY","1",60,55,wallet="b2")
    sell=action("active-sell",3,"SELL","1",100,91,role="active_aggregate",wallet="seller")
    con=connection([b1,b2,sell],[tag(b1),tag(b2),tag(sell,primary=30,unmatched=20)],
                   [link(b1,sell,"NORMAL",40,36,36),link(b2,sell,"NORMAL",60,55,55)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        output=rows(con,names["buy_tags"])
        assert len(output)==2
        assert all(row["exit_fraction"]==pytest.approx(.3) for row in output.values())
        assert all(row["unmatched_seller_acquisition_fraction"]==pytest.approx(.2) for row in output.values())
        assert sum(row["raw_exit_quantity_micro"] for row in output.values())==30
    finally:
        con.close()


def test_mixed_active_aggregate_retains_one_unit_vwap_and_crossed_price_diagnostics() -> None:
    sell=action("mixed",1,"SELL","1",20,19,wallet="seller")
    mint_buy=action("mixed",2,"BUY","2",80,79,wallet="mint-buyer")
    active_buy=action("mixed",3,"BUY","1",100,20,role="active_aggregate",wallet="active-buyer")
    con=connection([sell,mint_buy,active_buy],[tag(sell,primary=10,unmatched=4),tag(mint_buy),tag(active_buy)],
                   [link(sell,active_buy,"NORMAL",20,19,19),link(mint_buy,active_buy,"MINT",80,79,1)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        output=rows(con,names["buy_tags"])
        active=output[active_buy["execution_id"]]
        assert len(output)==2  # Own active aggregate plus true passive MINT BUY.
        assert active["price"]==.2
        assert active["raw_exit_quantity_micro"]==10
        assert active["exit_fraction"]==0
        assert active["crossed_price_exit_quantity_micro"]==10
        assert active["crossed_bin_exit_quantity_micro"]==10
        assert active["unmatched_seller_acquisition_fraction"]==0
        assert active["unmatched_counterparty_disposal_quantity_micro"]==4
        assert con.execute(f"SELECT n_records FROM {names['attribution_diagnostics']} WHERE measure='qualified_exit_crosses_original_buy_price_half'").fetchone()[0]==1
    finally:
        con.close()


def test_matched_grain_uses_actual_leg_prices_and_same_own_action_tags() -> None:
    seller=action("mixed-legs",1,"SELL","1",20,19,wallet="seller")
    mint=action("mixed-legs",2,"BUY","2",80,79,wallet="mint-buyer")
    buyer=action("mixed-legs",3,"BUY","1",100,20,role="active_aggregate",wallet="buyer")
    con=connection([seller,mint,buyer],[tag(seller,primary=10,unmatched=4),tag(mint),tag(buyer,hedge=30)],
        [link(seller,buyer,"NORMAL",20,19,19),link(mint,buyer,"MINT",80,79,1)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        own=rows(con,names["buy_tags"])[buyer["execution_id"]]
        matched=rows(con,names["matched_buy_tags"])
        assert len(matched)==3  # NORMAL actual BUY plus both complementary MINT BUYs.
        favorite=matched[seller["execution_id"]+":buy_active"]
        underdog=matched[mint["execution_id"]+":buy_active"]
        assert own["price"]==.2 and own["exit_fraction"]==0
        assert favorite["price"]==.95 and favorite["exit_fraction"]==.5
        assert favorite["qualified_hedge_fraction"]==0
        assert favorite["crossed_price_hedge_quantity_micro"]==pytest.approx(6)
        assert favorite["crossed_own_buy_favorite_gate_quantity_micro"]==20
        assert underdog["price"]==.0125 and underdog["qualified_hedge_fraction"]==.3
        assert underdog["raw_exit_quantity_micro"]==0
        assert all(not record["is_synthetic"] for record in matched.values())
        assert all(record["label_allocation_status"].endswith("not_per_leg_profit_certification") for record in matched.values())
        assert sum(record["gross_quantity_micro"] for record in matched.values())==sum(record["gross_quantity_micro"] for record in rows(con,names["buy_tags"]).values())
        assert sum(record["gross_cash_micro"] for record in matched.values())==sum(record["gross_cash_micro"] for record in rows(con,names["buy_tags"]).values())
    finally:
        con.close()


@pytest.mark.parametrize("exchange,net",[(LEGACY,900),(V2,1000)])
def test_matched_active_hedge_keeps_fee_rule_and_quantity_proportions(exchange: str,net: int) -> None:
    s1=action("fee-legs",1,"SELL","2",400,20,wallet="s1",exchange=exchange)
    s2=action("fee-legs",2,"SELL","2",600,60,wallet="s2",exchange=exchange)
    buy=action("fee-legs",3,"BUY","2",1000,80,role="active_aggregate",wallet="hedger",fee=100,exchange=exchange)
    con=connection([s1,s2,buy],[tag(s1),tag(s2),tag(buy,hedge=300)],
        [link(s1,buy,"NORMAL",400,20,20),link(s2,buy,"NORMAL",600,60,60)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        output=rows(con,names["matched_buy_tags"])
        assert len(output)==2
        assert all(record["qualified_hedge_fraction"]==pytest.approx(300/net) for record in output.values())
        assert sum(record["net_acquired_quantity_micro"] for record in output.values())==pytest.approx(net)
        assert sum(record["allocated_fee_micro"] for record in output.values())==pytest.approx(100)
        assert sum(record["hedge_profitable_quantity_micro"] for record in output.values())==pytest.approx(300)
        assert {record["price"] for record in output.values()}=={.05,.1}
        recon=con.execute(f"SELECT own_quantity_micro,matched_quantity_micro,own_cash_micro,matched_cash_micro FROM {names['grain_reconciliation']}").fetchone()
        assert recon==(1000,1000,80,80)
    finally:
        con.close()


def test_matched_mint_has_two_buy_observations_and_merge_has_none() -> None:
    mint_passive=action("mint-grain",1,"BUY","2",100,4,wallet="hedger")
    mint_active=action("mint-grain",2,"BUY","1",100,96,role="active_aggregate",wallet="favorite")
    merge_passive=action("merge-grain",3,"SELL","2",100,2,wallet="seller2")
    merge_active=action("merge-grain",4,"SELL","1",100,98,role="active_aggregate",wallet="seller1")
    con=connection([mint_passive,mint_active,merge_passive,merge_active],
        [tag(mint_passive,hedge=40),tag(mint_active),tag(merge_passive),tag(merge_active,primary=50)],
        [link(mint_passive,mint_active,"MINT",100,4,96),link(merge_passive,merge_active,"MERGE",100,2,98)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        output=rows(con,names["matched_buy_tags"])
        assert set(output)=={mint_passive["execution_id"]+":buy_passive",mint_passive["execution_id"]+":buy_active"}
        assert all(record["batch_kind"]=="MINT" and record["exit_fraction"]==0 for record in output.values())
        assert output[mint_passive["execution_id"]+":buy_passive"]["qualified_hedge_fraction"]==.4
    finally:
        con.close()


def test_matched_metadata_preserves_quantity_weighted_calibration_and_support() -> None:
    seller1=action("calibration-grain",1,"SELL","1",40,20,wallet="s1")
    seller2=action("calibration-grain",2,"SELL","1",60,54,wallet="s2")
    buyer=action("calibration-grain",3,"BUY","1",100,74,role="active_aggregate",wallet="buyer")
    con=connection([seller1,seller2,buyer],[tag(seller1),tag(seller2,primary=30),tag(buyer)],
        [link(seller1,buyer,"NORMAL",40,20,20),link(seller2,buyer,"NORMAL",60,54,54)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        metadata(con)
        own=enrich_buy_attribution(con,names["buy_tags"],"tokens","clocks","blocks","flags",output="own_enriched")
        matched=enrich_buy_attribution(con,names["matched_buy_tags"],"tokens","clocks","blocks","flags",output="matched_enriched")
        own_total=con.execute(f"SELECT count(*),sum(gross_quantity_micro),sum(gross_cash_micro),sum(gross_quantity_micro*residual) FROM {own}").fetchone()
        matched_total=con.execute(f"SELECT count(*),sum(gross_quantity_micro),sum(gross_cash_micro),sum(gross_quantity_micro*residual) FROM {matched}").fetchone()
        assert own_total[:3]==(1,100,74)
        assert matched_total[:3]==(2,100,74)
        assert own_total[3]==pytest.approx(matched_total[3])
        assert own_total[3]==pytest.approx(26)
    finally:
        con.close()


def test_zero_one_price_matched_buys_retained_until_focal_filtering() -> None:
    passive=action("boundary-mint",1,"BUY","2",100,0,wallet="zero")
    active=action("boundary-mint",2,"BUY","1",100,100,role="active_aggregate",wallet="one")
    con=connection([passive,active],[tag(passive),tag(active)],
                   [link(passive,active,"MINT",100,0,100)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        assert {record["price"] for record in rows(con,names["matched_buy_tags"]).values()}=={0,1}
        metadata(con)
        enriched=enrich_buy_attribution(con,names["matched_buy_tags"],"tokens","clocks","blocks","flags")
        assert con.execute(f"SELECT count(*) FROM {enriched}").fetchone()[0]==2
        assert con.execute(f"SELECT min(price),max(price) FROM {enriched}").fetchone()==(0,1)
    finally:
        con.close()


def test_mint_has_two_actual_buys_and_merge_creates_no_synthetic_buy() -> None:
    m_buy=action("mint",1,"BUY","2",100,4,wallet="hedger")
    a_buy=action("mint",2,"BUY","1",100,96,role="active_aggregate",wallet="speculator")
    m_sell=action("merge",3,"SELL","2",100,2,wallet="merger1")
    a_sell=action("merge",4,"SELL","1",100,98,role="active_aggregate",wallet="merger2")
    con=connection([m_buy,a_buy,m_sell,a_sell],
        [tag(m_buy,hedge=40),tag(a_buy),tag(m_sell),tag(a_sell,primary=50)],
        [link(m_buy,a_buy,"MINT",100,4,96),link(m_sell,a_sell,"MERGE",100,2,98)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        output=rows(con,names["buy_tags"])
        assert set(output)=={m_buy["execution_id"],a_buy["execution_id"]}
        assert all(row["exit_fraction"]==0 for row in output.values())
        assert output[m_buy["execution_id"]]["qualified_hedge_fraction"]==.4
        assert output[a_buy["execution_id"]]["unmatched_seller_acquisition_fraction"]==0
        assert con.execute(f"SELECT n_records FROM {names['attribution_diagnostics']} WHERE measure='merge_links_no_actual_buy'").fetchone()[0]==1
    finally:
        con.close()


def test_hedge_fee_fraction_uses_qualified_net_over_total_net_acquired() -> None:
    buyer=action("fee",1,"BUY","2",1000,50,wallet="hedger",fee=100)
    seller=action("fee",2,"SELL","2",1000,50,role="active_aggregate",wallet="seller")
    con=connection([buyer,seller],[tag(buyer,hedge=300),tag(seller)],
                   [link(buyer,seller,"NORMAL",1000,50,50)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        result=rows(con,names["buy_tags"])[buyer["execution_id"]]
        assert result["qualified_hedge_fraction"]==pytest.approx(300/900)
        assert result["qualified_hedge_fraction"]!=pytest.approx(300/1000)
        assert result["unmatched_seller_acquisition_fraction"]==0
        assert result["history_status"]==HISTORY_STATUS
    finally:
        con.close()


def test_metadata_after_tags_preserves_support_and_outcomes_never_classify() -> None:
    snapshots=[]
    for winner in ("1","2"):
        con=connection(*normal_fixture())
        try:
            names=create_buy_attribution(con,"own","tags","links")
            metadata(con,winner=winner)
            # Verify UTC day explicitly even when the connection's display zone is not UTC.
            con.execute("SET TimeZone='America/New_York'")
            result=enrich_buy_attribution(con,names["buy_tags"],"tokens","clocks","blocks","flags")
            output=rows(con,result)
            assert len(output)==1
            buy=output[f"{LEGACY}:normal:2"]
            assert buy["realized_time"]==.99 and buy["seconds_to_end"]==1
            assert str(buy["trade_day"])=="1970-01-01"
            assert buy["is_nonhuman"] and buy["wallet_flag_present"]
            assert buy["unknown_history_fraction"]==.1
            assert buy["hedge_fraction"]==0
            snapshots.append((buy["exit_fraction"],buy["residual"]))
            profile=create_contribution_summaries(con,result,support_floor=1,prefix="estimate")
            assert con.execute(f"SELECT n_executions FROM {profile['profile']} WHERE weighting='fill' AND price_bin=10").fetchone()[0]==1
        finally:
            con.close()
    assert snapshots[0][0]==snapshots[1][0]==.4
    assert snapshots[0][1]==pytest.approx(.05)
    assert snapshots[1][1]==pytest.approx(-.95)


@pytest.mark.parametrize("timestamp,time",[(50,-.5),(100,0),(200,1),(250,1.5)])
def test_all_history_buy_phases_are_retained_until_caller_selects(timestamp: int,time: float) -> None:
    con=connection(*normal_fixture())
    try:
        names=create_buy_attribution(con,"own","tags","links")
        metadata(con,timestamp=timestamp)
        result=enrich_buy_attribution(con,names["buy_tags"],"tokens","clocks","blocks","flags")
        assert con.execute(f"SELECT count(*),min(realized_time) FROM {result}").fetchone()==(1,time)
    finally:
        con.close()


@pytest.mark.parametrize("mutation",[
    "duplicate_action","duplicate_tag","missing_tag","unknown_tag","unverified_source",
    "source_fee_rule","unreconciled_aggregate","fraction_wrong_denominator","sell_hedge",
    "duplicate_link","missing_link","bad_active_cash","bad_kind","same_direction_normal",
    "wrong_tag_wallet","fraction_exceeds_quantity","unknown_history_on_fresh_buy","wrong_tag_fee_rule",
])
def test_attribution_gates_fail_closed(mutation: str) -> None:
    con=connection(*normal_fixture())
    changes={
        "duplicate_action":"INSERT INTO own SELECT * FROM own LIMIT 1",
        "duplicate_tag":"INSERT INTO tags SELECT * FROM tags LIMIT 1",
        "missing_tag":"DELETE FROM tags WHERE side='BUY'",
        "unknown_tag":"UPDATE tags SET execution_id='unknown' WHERE side='BUY'",
        "unverified_source":"UPDATE own SET source_status='unsupported'",
        "source_fee_rule":"UPDATE own SET fee_rule='unknown'",
        "unreconciled_aggregate":"UPDATE own SET aggregate_reconciled=FALSE WHERE source_role='active_aggregate'",
        "fraction_wrong_denominator":"UPDATE tags SET primary_exit_fraction=.5 WHERE side='SELL'",
        "sell_hedge":"UPDATE tags SET hedge_profitable_quantity_micro=1,hedge_fraction=.01 WHERE side='SELL'",
        "duplicate_link":"INSERT INTO links SELECT * FROM links",
        "missing_link":"DELETE FROM links",
        "bad_active_cash":"UPDATE links SET active_cash_micro=94",
        "bad_kind":"UPDATE links SET kind='MINT'",
        "same_direction_normal":"UPDATE own SET maker_asset_id='0',taker_asset_id='1',maker_amount_filled=95,taker_amount_filled=100 WHERE source_role='passive'",
        "wrong_tag_wallet":"UPDATE tags SET wallet='wrong'",
        "fraction_exceeds_quantity":"UPDATE tags SET primary_exit_quantity_micro=110,primary_exit_fraction=1.1 WHERE side='SELL'",
        "unknown_history_on_fresh_buy":"UPDATE tags SET unmatched_disposal_quantity_micro=10,unmatched_disposal_fraction=.1 WHERE side='BUY'",
        "wrong_tag_fee_rule":"UPDATE tags SET fee_rule='collateral_extra_buy'",
    }
    try:
        con.execute(changes[mutation])
        with pytest.raises(ValueError):
            create_buy_attribution(con,"own","tags","links")
    finally:
        con.close()


@pytest.mark.parametrize("mutation",["duplicate_block","missing_block","conflicting_flag","bad_winner","wrong_market","missing_clock"])
def test_metadata_gates_fail_closed(mutation: str) -> None:
    con=connection(*normal_fixture())
    try:
        names=create_buy_attribution(con,"own","tags","links")
        metadata(con)
        changes={
            "duplicate_block":"INSERT INTO blocks SELECT * FROM blocks",
            "missing_block":"DELETE FROM blocks",
            "conflicting_flag":"INSERT INTO flags VALUES ('bot',FALSE)",
            "bad_winner":"UPDATE tokens SET won=TRUE",
            "wrong_market":"UPDATE tokens SET market_id='wrong'",
            "missing_clock":"DELETE FROM clocks",
        }
        con.execute(changes[mutation])
        with pytest.raises(ValueError):
            enrich_buy_attribution(con,names["buy_tags"],"tokens","clocks","blocks","flags")
    finally:
        con.close()


def test_adapter_never_replaces_existing_output() -> None:
    con=connection(*normal_fixture())
    try:
        create_buy_attribution(con,"own","tags","links")
        with pytest.raises(ValueError,match="Refusing to replace"):
            create_buy_attribution(con,"own","tags","links")
    finally:
        con.close()


def test_v2_buy_fee_is_extra_collateral_not_a_token_deduction() -> None:
    buyer=action("v2",1,"BUY","2",1000,50,wallet="hedger",fee=100,exchange=V2)
    seller=action("v2",2,"SELL","2",1000,50,role="active_aggregate",wallet="seller",exchange=V2)
    con=connection([buyer,seller],[tag(buyer,hedge=300),tag(seller)],
                   [link(buyer,seller,"NORMAL",1000,50,50)])
    try:
        names=create_buy_attribution(con,"own","tags","links")
        result=rows(con,names["buy_tags"])[buyer["execution_id"]]
        assert result["net_acquired_quantity_micro"]==1000
        assert result["qualified_hedge_fraction"]==.3
        assert result["price"]==.05  # Before-fee calibration pricing is unchanged.
        assert result["source_contract_version"]=="ctf_exchange_v2_v1"
        metadata(con)
        enriched=enrich_buy_attribution(con,names["buy_tags"],"tokens","clocks","blocks","flags")
        assert con.execute(f"SELECT usdc FROM {enriched}").fetchone()[0]==50/1e6
    finally:
        con.close()


@pytest.mark.parametrize("mutation",[
    "UPDATE own SET exchange_address='unknown',execution_id='unknown:'||transaction_hash||':'||log_index",
    "UPDATE own SET source_contract_version='ctf_exchange_v2_v1'",
    "UPDATE own SET fee_rule='collateral_extra_buy'",
])
def test_address_version_fee_markers_cannot_be_mixed(mutation: str) -> None:
    con=connection(*normal_fixture())
    try:
        con.execute(mutation)
        with pytest.raises(ValueError,match="source certificates"):
            create_buy_attribution(con,"own","tags","links")
    finally:
        con.close()
