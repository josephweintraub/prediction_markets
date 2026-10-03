from __future__ import annotations

from fractions import Fraction

import duckdb
import pytest

from analysis.diagnostics.profit_taking_contribution import (
    create_contribution_summaries, validate_executions,
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
