from __future__ import annotations

from fractions import Fraction

import duckdb
import pytest

from analysis.diagnostics.profit_taking_contribution import (
    create_contribution_summaries, create_sport_contribution_summaries, validate_executions,
)


def connection(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("""CREATE TABLE buys(execution_id VARCHAR,market_id VARCHAR,side VARCHAR,
        is_synthetic BOOLEAN,price DOUBLE,residual DOUBLE,usdc DOUBLE,
        exit_fraction DOUBLE,hedge_fraction DOUBLE,unknown_history_fraction DOUBLE)""")
    if rows:
        con.executemany("INSERT INTO buys VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    return con


def buy(identity: str, market: str, price: float, won: int, cash: float,
        *, exit: float=0, hedge: float=0, unknown: float=0) -> tuple:
    return identity,market,"BUY",False,price,won-price,cash,exit,hedge,unknown


def one(con: duckdb.DuckDBPyConnection, relation: str, where: str) -> dict:
    cursor = con.execute(f"SELECT * FROM {relation} WHERE {where}")
    values = cursor.fetchone()
    assert values is not None
    return dict(zip((field[0] for field in cursor.description),values))


def test_hand_reconciled_fractional_quantity_and_spread() -> None:
    con = connection([
        buy("l1","m1",.05,0,1,hedge=.25,unknown=.5),
        buy("l2","m2",.08,1,3,hedge=.5),
        buy("f1","m1",.95,1,2,exit=.4,unknown=.6),
        buy("f2","m2",.92,0,6,exit=.25),
    ])
    try:
        names = create_contribution_summaries(con,"buys",support_floor=1)
        d1 = one(con,names["profile"],"weighting='fill' AND price_bin=1")
        d10 = one(con,names["profile"],"weighting='fill' AND price_bin=10")
        tail = one(con,names["tails"],"weighting='fill'")
        assert d1["calibration"] == pytest.approx((-.05+.92)/2)
        assert d1["hedge_contribution"] == pytest.approx((-.05*.25+.92*.5)/2)
        assert d10["exit_contribution"] == pytest.approx((.05*.4-.92*.25)/2)
        assert d10["remaining_contribution"] == pytest.approx((.05*.6-.92*.75)/2)
        assert tail["spread"] == pytest.approx(Fraction(-87,100))
        assert tail["spread"] == pytest.approx(sum(tail[key] for key in (
            "exit_spread_contribution","hedge_spread_contribution","remaining_spread_contribution")))
        assert d1["hedge_contributing_executions"] == 2
        assert d1["exit_contributing_executions"] == 0
        assert d1["unknown_history_weight_share"] == .25
        assert d10["n_executions"] == 2
        assert con.execute(f"SELECT count(*) FROM {names['profile']}").fetchone()[0] == 30
        assert con.execute(f"SELECT count(*) FROM {names['tails']}").fetchone()[0] == 3
        assert tail["uncertainty_status"] == "not_estimated_descriptive"
    finally:
        con.close()


def test_dollar_and_equal_market_use_original_denominators() -> None:
    con = connection([
        buy("a1","large",.95,1,9,exit=1),
        buy("a2","large",.95,1,1),
        buy("b1","small",.92,0,1),
    ])
    try:
        names = create_contribution_summaries(con,"buys",support_floor=1)
        dollar = one(con,names["profile"],"weighting='dollar' AND price_bin=10")
        equal = one(con,names["profile"],"weighting='equal_market' AND price_bin=10")
        fill = one(con,names["profile"],"weighting='fill' AND price_bin=10")
        assert dollar["calibration"] == pytest.approx((10*.05-.92)/11)
        assert dollar["exit_contribution"] == pytest.approx(9*.05/11)
        assert dollar["exit_weight_share"] == pytest.approx(9/11)
        assert equal["calibration"] == pytest.approx((.05-.92)/2)
        assert equal["exit_contribution"] == pytest.approx(.9*.05/2)
        assert equal["remaining_contribution"] == pytest.approx((.1*.05-.92)/2)
        assert equal["exit_weight_share"] == .45
        assert equal["weight_total"] == 2
        assert equal["n_executions"] == 3
        assert equal["exit_contributing_executions"] == 1
        assert fill["exit_contribution"] == pytest.approx(.05/3)
        # Renormalizing to the exit subgroup would give .05, which is not its contribution.
        assert equal["exit_contribution"] != pytest.approx(.05)
    finally:
        con.close()


def test_equal_market_weights_are_separate_for_each_original_price_bin() -> None:
    con = connection([
        buy("a1","m",.05,0,100,hedge=1),buy("a2","m",.95,1,1,exit=1),
        buy("b1","n",.04,1,1),buy("b2","n",.96,0,100),
    ])
    try:
        names = create_contribution_summaries(con,"buys",support_floor=1)
        row = one(con,names["tails"],"weighting='equal_market'")
        assert row["d1_weight_total"] == 2
        assert row["d10_weight_total"] == 2
        assert row["exit_spread_contribution"] == pytest.approx(.05/2)
        assert row["hedge_spread_contribution"] == pytest.approx(.05/2)
        assert row["spread"] == pytest.approx((.05-.96)/2-(-.05+.96)/2)
    finally:
        con.close()


def test_supplied_hedge_fraction_prorates_full_buy_using_net_received_basis() -> None:
    # Gross BUY quantity is 10; a one-token fee leaves 9 acquired tokens. Three
    # qualified hedge tokens allocate 3/9 of the whole actual BUY, not 3/10.
    hedge = float(Fraction(3,9))
    con = connection([buy("fee-buy","m",.05,0,.5,hedge=hedge),
                      buy("other","m",.08,0,.8)])
    try:
        names = create_contribution_summaries(con,"buys",support_floor=1)
        row = one(con,names["profile"],"weighting='dollar' AND price_bin=1")
        assert row["hedge_weight"] == pytest.approx(.5*Fraction(3,9))
        assert row["hedge_contribution"] == pytest.approx(-.05*.5*Fraction(3,9)/1.3)
        assert row["hedge_contribution"] != pytest.approx(-.05*.5*Fraction(3,10)/1.3)
        assert row["calibration"] == pytest.approx(row["hedge_contribution"]+row["remaining_contribution"])
    finally:
        con.close()


def test_default_support_uses_original_executions_not_tag_fraction_or_links() -> None:
    rows = [buy(f"l{i}","m",.05,0,.05,hedge=.1 if i==0 else 0) for i in range(500)]
    rows += [buy(f"f{i}","m",.95,1,.95,exit=.5 if i==0 else 0) for i in range(499)]
    con = connection(rows)
    try:
        names = create_contribution_summaries(con,"buys")
        low = one(con,names["profile"],"weighting='fill' AND price_bin=1")
        high = one(con,names["profile"],"weighting='fill' AND price_bin=10")
        tail = one(con,names["tails"],"weighting='fill'")
        assert not low["suppressed"]
        assert low["hedge_contributing_executions"] == 1
        assert low["hedge_contribution"] == pytest.approx(-.05*.1/500)
        assert high["suppressed"] and high["calibration"] is None
        assert high["calibration_raw"] == pytest.approx(.05)
        assert tail["suppressed"] and tail["spread"] is None
        assert tail["spread_raw"] == pytest.approx(.1)
    finally:
        con.close()


def test_empty_bins_and_zero_dollars_do_not_become_zero_estimates() -> None:
    con = connection([buy("free","m",0,0,0,unknown=1)])
    try:
        names = create_contribution_summaries(con,"buys",support_floor=1)
        dollar = one(con,names["profile"],"weighting='dollar' AND price_bin=1")
        fill = one(con,names["profile"],"weighting='fill' AND price_bin=1")
        empty = one(con,names["profile"],"weighting='fill' AND price_bin=2")
        assert dollar["n_executions"] == 1
        assert dollar["calibration_raw"] is None and dollar["calibration"] is None
        assert dollar["support_status"] == "no_positive_weight"
        assert fill["calibration"] == 0
        assert fill["unknown_history_weight_share"] == 1
        assert empty["calibration_raw"] is None and empty["calibration"] is None
        assert empty["n_executions"] == 0 and empty["suppressed"]
    finally:
        con.close()


def test_duplicate_lot_rows_fail_instead_of_multiplying_fill_support() -> None:
    row = buy("same-fill","m",.95,1,1,exit=.25)
    con = connection([row,row])
    try:
        with pytest.raises(ValueError,match="Duplicate execution IDs"):
            create_contribution_summaries(con,"buys",support_floor=1)
    finally:
        con.close()


@pytest.mark.parametrize("column,value",[
    ("execution_id",None),("execution_id",""),("market_id",None),
    ("side","SELL"),("is_synthetic",True),("is_synthetic",None),
    ("price",float("nan")),("price",1.1),("residual",float("inf")),
    ("residual",.2),("usdc",-1),("exit_fraction",None),("exit_fraction",1.1),
    ("exit_fraction",-.1),("hedge_fraction",.1),("unknown_history_fraction",1.1),
])
def test_invalid_original_buy_inputs_fail_closed(column: str,value) -> None:
    columns = ("execution_id","market_id","side","is_synthetic","price","residual",
               "usdc","exit_fraction","hedge_fraction","unknown_history_fraction")
    row = list(buy("f","m",.95,1,1,exit=.5))
    row[columns.index(column)] = value
    con = connection([tuple(row)])
    try:
        with pytest.raises(ValueError):
            validate_executions(con,"buys")
    finally:
        con.close()


def test_half_price_cannot_be_favorite_exit_or_complement_hedge() -> None:
    for fractions in ({"exit":.1},{"hedge":.1}):
        con = connection([buy("half","m",.5,1,1,**fractions)])
        try:
            with pytest.raises(ValueError,match="current-price regime"):
                validate_executions(con,"buys")
        finally:
            con.close()


def test_unknown_history_fraction_is_mandatory_and_not_silently_zero() -> None:
    con = connection([buy("f","m",.95,1,1)])
    try:
        con.execute("ALTER TABLE buys DROP COLUMN unknown_history_fraction")
        with pytest.raises(ValueError,match="unknown_history_fraction"):
            create_contribution_summaries(con,"buys")
    finally:
        con.close()


def test_no_replace_or_unsafe_relation_names() -> None:
    con = connection([buy("f","m",.95,1,1)])
    try:
        with pytest.raises(ValueError,match="simple SQL identifiers"):
            create_contribution_summaries(con,"buys; DROP TABLE buys")
        create_contribution_summaries(con,"buys",support_floor=1)
        with pytest.raises(ValueError,match="Refusing to replace"):
            create_contribution_summaries(con,"buys",support_floor=1)
        assert con.execute("SELECT count(*) FROM buys").fetchone()[0] == 1
    finally:
        con.close()


@pytest.mark.parametrize("floor",[0,-1,True,1.5])
def test_invalid_support_floor(floor) -> None:
    con = connection([])
    try:
        with pytest.raises(ValueError,match="positive integer"):
            create_contribution_summaries(con,"buys",support_floor=floor)
    finally:
        con.close()


def test_empty_original_population_retains_full_suppressed_grid() -> None:
    con = connection([])
    try:
        names = create_contribution_summaries(con,"buys")
        assert con.execute(f"SELECT count(*) FROM {names['profile']} WHERE suppressed").fetchone()[0] == 30
        assert con.execute(f"SELECT count(*) FROM {names['profile']} WHERE calibration_raw IS NOT NULL").fetchone()[0] == 0
        assert con.execute(f"SELECT count(*) FROM {names['tails']} WHERE suppressed AND spread IS NULL").fetchone()[0] == 3
    finally:
        con.close()


def test_own_event_and_genuine_leg_grains_conserve_cash_but_not_dollar_price_mean() -> None:
    # One true BUY order event received 10 units at .91 and 90 at .95. Its VWAP
    # is .946. Both grains allocate the SAME original .4 own-action tag. These
    # are real execution legs, not two acquisition-lot links to the same fill.
    quantities=(Fraction(10),Fraction(90))
    prices=(Fraction(91,100),Fraction(95,100))
    cash=tuple(q*p for q,p in zip(quantities,prices))
    quantity=sum(quantities)
    dollars=sum(cash)
    vwap=dollars/quantity
    legs=connection([buy("leg-1","m",float(prices[0]),1,float(cash[0]),exit=.4),
                     buy("leg-2","m",float(prices[1]),1,float(cash[1]),exit=.4)])
    own=connection([buy("own-event","m",float(vwap),1,float(dollars),exit=.4)])
    try:
        leg_tables=create_contribution_summaries(legs,"buys",support_floor=1)
        own_tables=create_contribution_summaries(own,"buys",support_floor=1)
        l=one(legs,leg_tables["profile"],"weighting='dollar' AND price_bin=10")
        o=one(own,own_tables["profile"],"weighting='dollar' AND price_bin=10")
        variance=sum(q*(p-vwap)**2 for q,p in zip(quantities,prices))/quantity
        assert l["dollars"]==pytest.approx(o["dollars"])
        assert l["exit_weight"]==pytest.approx(o["exit_weight"])
        assert l["n_executions"]==2 and o["n_executions"]==1
        assert o["calibration"]-l["calibration"]==pytest.approx(float(variance/vwap))
        assert o["exit_contribution"]-l["exit_contribution"]==pytest.approx(.4*float(variance/vwap))
        assert sum(q*(1-p) for q,p in zip(quantities,prices))==quantity*(1-vwap)
        assert set(legs.execute(f"SELECT DISTINCT weighting FROM {leg_tables['profile']}").fetchnumpy()["weighting"]) \
            == {"fill","dollar","equal_market"}  # Quantity is a conservation check, not a fourth report estimand.
    finally:
        legs.close()
        own.close()


def test_homogeneous_price_leg_split_preserves_all_weighted_components() -> None:
    own=connection([buy("own-event","m",.95,1,19,exit=.25,unknown=.2)])
    legs=connection([buy("leg-1","m",.95,1,4.75,exit=.25,unknown=.2),
                     buy("leg-2","m",.95,1,14.25,exit=.25,unknown=.2)])
    try:
        a=create_contribution_summaries(own,"buys",support_floor=1)
        b=create_contribution_summaries(legs,"buys",support_floor=1)
        for weighting in ("fill","dollar","equal_market"):
            o=one(own,a["profile"],f"weighting='{weighting}' AND price_bin=10")
            l=one(legs,b["profile"],f"weighting='{weighting}' AND price_bin=10")
            for field in ("calibration","exit_contribution","hedge_contribution","remaining_contribution",
                          "unknown_history_weight_share"):
                assert l[field]==pytest.approx(o[field])
            assert l["n_executions"]==2 and o["n_executions"]==1
    finally:
        legs.close()
        own.close()


def timed_connection(records: list[tuple[tuple,float,float,bool]]) -> duckdb.DuckDBPyConnection:
    con=connection([record[0] for record in records])
    for definition in ("sport VARCHAR DEFAULT 'atp'","realized_time DOUBLE","seconds_to_end DOUBLE",
                       "is_nonhuman BOOLEAN","gross_quantity_micro BIGINT DEFAULT 1000000"):
        con.execute("ALTER TABLE buys ADD COLUMN "+definition)
    if records:
        con.executemany("UPDATE buys SET realized_time=?,seconds_to_end=?,is_nonhuman=? WHERE execution_id=?",
                        [(time,seconds,bot,record[0]) for record,time,seconds,bot in records])
    return con


def test_grouped_fixed_grid_and_literal_window_boundaries() -> None:
    boundaries=[("deep-pre",-25,2600),("short-pre",-.1,110),("start",0,200),
                ("third-1",1/3,100),("third-2",2/3,80),("80",.8,60),("90",.9,30),
                ("95",.95,20),("99",.99,10),("end",1,0),("post",1.001,-.1),("120",.5,120)]
    con=timed_connection([(buy(identity,"m",.95,1,.95,exit=.25),time,seconds,False)
                          for identity,time,seconds in boundaries])
    expected={"pregame":{"deep-pre","short-pre"},
        "live":{"start","third-1","third-2","80","90","95","99","end","120"},
        "live_first_third":{"start"},"live_middle_third":{"third-1","120"},
        "live_final_third":{"third-2","80","90","95","99","end"},
        "live_80_90":{"80"},"live_90_95":{"90"},"live_95_99":{"95"},
        "live_99_100":{"99","end"},"last_120_seconds":{"third-1","third-2","80","90","95","99","end","120"}}
    try:
        names=create_sport_contribution_summaries(con,"buys",support_floor=1)
        assert con.execute(f"SELECT count(*) FROM {names['profile']}").fetchone()[0]==9*3*10*10*3
        assert con.execute(f"SELECT count(*) FROM {names['tails']}").fetchone()[0]==9*3*10*3
        assert con.execute(f"SELECT count(*) FROM {names['late_delta']}").fetchone()[0]==9*3*3
        for window,identities in expected.items():
            actual={row[0] for row in con.execute(
                f'SELECT execution_id FROM {names["focal"]} WHERE sport=\'atp\' AND sample=\'filtered\' AND "window"=?',
                [window]).fetchall()}
            assert actual==identities
        assert con.execute(f'SELECT count(*) FROM {names["profile"]} WHERE sport<>\'atp\' AND NOT suppressed').fetchone()[0]==0
    finally:
        con.close()


def test_grouped_focal_samples_do_not_filter_matching_history_or_copy_boundary_prices() -> None:
    records=[(buy("human","m",.05,0,.05,hedge=.25),.999,1,False),
             (buy("bot","m",.05,0,.05,hedge=.25),.999,1,True),
             (buy("one-cent","m",.01,0,.01),.999,1,False),
             (buy("99-cents","m",.99,1,.99),.999,1,False),
             (buy("zero-price","m",0,0,0),.999,1,False),
             (buy("one-price","m",1,1,1),.999,1,False)]
    con=timed_connection(records)
    try:
        names=create_sport_contribution_summaries(con,"buys",support_floor=1)
        expected={"filtered":{"human"},"interior_all_actors":{"human","bot"},
                  "all_trades":{"human","bot","one-cent","99-cents"}}
        for sample,identities in expected.items():
            actual={row[0] for row in con.execute(
                f'SELECT execution_id FROM {names["focal"]} WHERE "window"=\'live_99_100\' AND sample=?',[sample]).fetchall()}
            assert actual==identities
        assert con.execute("SELECT count(*) FROM buys").fetchone()[0]==6
    finally:
        con.close()


def test_grouped_equal_market_denominators_are_original_specific_sample_and_window() -> None:
    records=[(buy("large-tag","large",.95,1,9,exit=1),.999,1,False),
             (buy("large-other","large",.95,1,1),.999,1,False),
             (buy("small-other","small",.92,0,1),.999,1,False),
             # This bot belongs to an earlier window and all-actor live samples.
             # Neither it nor repeated overlapping windows may dilute late weights.
             (buy("earlier-bot","large",.95,1,90,exit=1),.97,30,True)]
    con=timed_connection(records)
    try:
        names=create_sport_contribution_summaries(con,"buys",support_floor=1)
        for sample in ("filtered","interior_all_actors","all_trades"):
            row=one(con,names["profile"],f'sport=\'atp\' AND sample=\'{sample}\' AND "window"=\'live_99_100\' AND weighting=\'equal_market\' AND price_bin=10')
            assert row["weight_total"]==2
            assert row["calibration"]==pytest.approx((.05-.92)/2)
            assert row["exit_contribution"]==pytest.approx(.9*.05/2)
            assert row["n_executions"]==3
            assert row["exit_contributing_executions"]==1
            assert row["gross_quantity_micro"]==3_000_000
            assert row["allocated_exit_gross_quantity_micro"]+row["allocated_hedge_gross_quantity_micro"] \
                +row["allocated_remaining_gross_quantity_micro"]==pytest.approx(row["gross_quantity_micro"])
    finally:
        con.close()


def test_grouped_late_change_is_final_minus_previous_with_additive_pp_components() -> None:
    records=[(buy("early-low","m",.05,0,.05,hedge=.5),.97,30,False),
             (buy("early-high","m",.95,1,.95,exit=.2),.97,30,False),
             (buy("late-low","m",.06,0,.06,hedge=.25),.995,5,False),
             (buy("late-high","m",.94,1,.94,exit=.4),.995,5,False)]
    con=timed_connection(records)
    try:
        names=create_sport_contribution_summaries(con,"buys",support_floor=1)
        row=one(con,names["late_delta"],"sport='atp' AND sample='filtered' AND weighting='fill'")
        assert row["contrast"]=="live_99_100_minus_live_95_99"
        assert not row["suppressed"]
        assert row["spread_delta"]==pytest.approx(.02)
        assert row["exit_spread_delta"]==pytest.approx(.014)
        assert row["hedge_spread_delta"]==pytest.approx(-.01)
        assert row["remaining_spread_delta"]==pytest.approx(.016)
        assert row["spread_delta_pp"]==pytest.approx(2)
        assert row["exit_spread_delta_pp"]==pytest.approx(1.4)
        assert row["hedge_spread_delta_pp"]==pytest.approx(-1)
        assert row["remaining_spread_delta_pp"]==pytest.approx(1.6)
        assert row["spread_delta_pp"]==pytest.approx(sum(row[name] for name in (
            "exit_spread_delta_pp","hedge_spread_delta_pp","remaining_spread_delta_pp")))
        assert row["uncertainty_status"]=="not_estimated_descriptive"
    finally:
        con.close()


def test_grouped_late_change_requires_support_in_all_four_tails() -> None:
    records=[]
    for phase,time,seconds in (("early",.97,30),("late",.995,5)):
        for bin_name,price,outcome in (("low",.05,0),("high",.95,1)):
            n=499 if phase=="late" and bin_name=="low" else 500
            records.extend((buy(f"{phase}-{bin_name}-{i}","m",price,outcome,price),time,seconds,False) for i in range(n))
    con=timed_connection(records)
    try:
        names=create_sport_contribution_summaries(con,"buys")
        row=one(con,names["late_delta"],"sport='atp' AND sample='filtered' AND weighting='fill'")
        assert row["previous_d1_n_executions"]==500 and row["previous_d10_n_executions"]==500
        assert row["final_d1_n_executions"]==499 and row["final_d10_n_executions"]==500
        assert row["suppressed"]
        assert row["spread_delta_raw"]==pytest.approx(0)
        for name in ("spread_delta","exit_spread_delta","hedge_spread_delta","remaining_spread_delta",
                     "spread_delta_pp","exit_spread_delta_pp","hedge_spread_delta_pp","remaining_spread_delta_pp"):
            assert row[name] is None
    finally:
        con.close()


@pytest.mark.parametrize("field,value",[("sport","wta"),("realized_time",None),
                                        ("seconds_to_end",float("inf")),("is_nonhuman",None),
                                        ("gross_quantity_micro",0)])
def test_grouped_invalid_scope_and_timing_fail_closed(field: str,value) -> None:
    con=timed_connection([(buy("x","m",.95,1,.95),.999,1,False)])
    try:
        con.execute(f"UPDATE buys SET {field}=?",[value])
        with pytest.raises(ValueError):
            create_sport_contribution_summaries(con,"buys")
    finally:
        con.close()
