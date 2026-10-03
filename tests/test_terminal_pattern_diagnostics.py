"""Independent small-fixture checks of the frozen terminal diagnostic contract."""
from __future__ import annotations

from contextlib import contextmanager
from argparse import Namespace
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import production_guard
from analysis.diagnostics import build_terminal_pattern_diagnostics as diagnostics
from analysis.diagnostics.build_profit_taking_contribution import _publish
from analysis.diagnostics.build_terminal_pattern_diagnostics import (
    create_terminal_diagnostics, reconcile_baseline,
)


def observation(identity: str, market: str, event: str, price: float, won: int,
                time: float, *, quantity: int = 10, flagged: bool = False,
                seconds_to_end: float | None = None, sport: str = "atp") -> tuple:
    quantity_micro = quantity * 1_000_000
    cash_micro = round(price * quantity_micro)
    return (identity, identity, market, event, sport, "BUY", False, price, float(won),
            won - price, cash_micro / 1_000_000, quantity_micro, cash_micro,
            time, (1-time)*1_000 if seconds_to_end is None else seconds_to_end,
            flagged, 0.0, 0.0, 0.0)


@contextmanager
def database(rows: list[tuple]) -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE buys(execution_id VARCHAR, own_execution_id VARCHAR,
            market_id VARCHAR, event_id VARCHAR, sport VARCHAR, side VARCHAR,
            is_synthetic BOOLEAN, price DOUBLE, won DOUBLE, residual DOUBLE,
            usdc DOUBLE, gross_quantity_micro BIGINT, gross_cash_micro BIGINT,
            realized_time DOUBLE, seconds_to_end DOUBLE, is_nonhuman BOOLEAN,
            exit_fraction DOUBLE, hedge_fraction DOUBLE, unknown_history_fraction DOUBLE)""")
        if rows:
            con.executemany("INSERT INTO buys VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        yield con
    finally:
        con.close()


def one(con: duckdb.DuckDBPyConnection, relation: str, predicate: str) -> dict:
    cursor = con.execute(f'SELECT * FROM "{relation}" WHERE {predicate}')
    rows = cursor.fetchall()
    assert len(rows) == 1
    return dict(zip((column[0] for column in cursor.description), rows[0]))


def moment(con: duckdb.DuckDBPyConnection, names: dict[str, str], *,
           window: str = "live_99_100", price_bin: int = 1,
           weighting: str = "fill", sample: str = "all_trades",
           relation: str = "terminal_moments", sport: str = "atp") -> dict:
    return one(con, names[relation], f"sport='{sport}' AND sample='{sample}' "
               f"AND weighting='{weighting}' AND \"window\"='{window}' AND price_bin={price_bin}")


def delta(con: duckdb.DuckDBPyConnection, names: dict[str, str], *,
          weighting: str = "fill", sample: str = "all_trades",
          relation: str = "late_identity", sport: str = "atp") -> dict:
    # The sequential-filter table compares all three populations in one row.
    sample_predicate = "" if relation == "filter_contrasts" else f" AND sample='{sample}'"
    return one(con, names[relation], f"sport='{sport}'{sample_predicate} AND weighting='{weighting}'")


def four_cells(market: str = "m", event: str = "e", *, quantity: int = 10,
               previous_prices: tuple[float, float] = (.05, .95),
               final_prices: tuple[float, float] = (.05, .95),
               previous_outcomes: tuple[int, int] = (0, 1),
               final_outcomes: tuple[int, int] = (0, 1), flagged: bool = False,
               sport: str = "atp", copies: int = 1) -> list[tuple]:
    rows = []
    for phase, time, prices, outcomes in (
            ("previous", .97, previous_prices, previous_outcomes),
            ("final", .995, final_prices, final_outcomes)):
        for tail, price, won in zip((1, 10), prices, outcomes):
            for number in range(copies):
                rows.append(observation(f"{market}-{phase}-{tail}-{number}", market, event,
                                        price, won, time, quantity=quantity,
                                        flagged=flagged, sport=sport))
    return rows


@pytest.mark.parametrize("weighting", ["fill", "dollar", "equal_market"])
def test_pure_price_shift_has_zero_outcome_term(weighting: str) -> None:
    with database(four_cells(previous_prices=(.04, .96), final_prices=(.08, .92))) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        row = delta(con, names, weighting=weighting)
        assert not row["suppressed"]
        assert row["outcome_gap_delta"] == pytest.approx(0)
        assert row["price_gap_delta"] == pytest.approx(-.08)
        assert row["spread_delta"] == pytest.approx(.08)
        for window in ("live_95_99", "live_99_100"):
            for price_bin in (1, 10):
                cell = moment(con, names, window=window, price_bin=price_bin, weighting=weighting)
                assert cell["calibration"] == pytest.approx(cell["mean_outcome"]-cell["mean_price"])
                assert cell["weighted_residual_sum"] == pytest.approx(
                    cell["weighted_outcome_sum"]-cell["weighted_price_sum"])


@pytest.mark.parametrize("weighting", ["fill", "dollar", "equal_market"])
def test_pure_outcome_shift_has_zero_price_term(weighting: str) -> None:
    rows = four_cells(previous_outcomes=(1, 0), final_outcomes=(0, 1))
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        row = delta(con, names, weighting=weighting)
        assert row["outcome_gap_delta"] == pytest.approx(2)
        assert row["price_gap_delta"] == pytest.approx(0)
        assert row["spread_delta"] == pytest.approx(2)


def test_literal_price_filters_and_fixed_tail_edges() -> None:
    prices = [(0, False), (.005, False), (.01, False), (.05, False), (.05, True),
              (.1, False), (.5, False), (.9, False), (.95, False), (.95, True),
              (.99, False), (.999, False), (1, False)]
    rows = [observation(str(number), "m", "e", price, int(price>.5), .995, flagged=flagged)
            for number, (price, flagged) in enumerate(prices)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for sample, expected in (("all_trades", (4, 5)),
                                 ("interior_all_actors", (2, 3)), ("filtered", (1, 2))):
            assert tuple(moment(con, names, sample=sample, price_bin=price_bin)["n_executions"]
                         for price_bin in (1, 10)) == expected


def test_filter_change_is_sequential_and_not_wallet_change_only() -> None:
    rows = four_cells("ordinary", "ordinary", final_prices=(.06, .94))
    rows += four_cells("flagged", "flagged", flagged=True,
                       previous_outcomes=(1, 0), final_outcomes=(0, 1))
    rows += four_cells("boundary", "boundary", previous_prices=(.005, .995),
                       final_prices=(.005, .995), previous_outcomes=(0, 1),
                       final_outcomes=(1, 0))
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            all_value = delta(con, names, weighting=weighting)["spread_delta"]
            interior = delta(con, names, weighting=weighting, sample="interior_all_actors")["spread_delta"]
            filtered = delta(con, names, weighting=weighting, sample="filtered")["spread_delta"]
            row = delta(con, names, weighting=weighting, relation="filter_contrasts")
            assert not row["suppressed"]
            assert row["boundary_price_filter_change"] == pytest.approx(interior-all_value)
            assert row["flagged_wallet_filter_change"] == pytest.approx(filtered-interior)
            assert row["filtered_minus_all_change"] == pytest.approx(filtered-all_value)
            assert row["filtered_minus_all_change"] == pytest.approx(
                row["boundary_price_filter_change"]+row["flagged_wallet_filter_change"])
            assert row["boundary_price_filter_change"] != pytest.approx(0)
            assert not row["boundary_price_filter_suppressed"]
            assert not row["flagged_wallet_filter_suppressed"]
            assert not row["filtered_minus_all_suppressed"]


def test_price_component_stays_visible_when_only_filtered_sample_is_sparse() -> None:
    rows = four_cells("unflagged", "u") + four_cells("flagged", "f", flagged=True)
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=2)
        assert not delta(con, names)["suppressed"]
        assert not delta(con, names, sample="interior_all_actors")["suppressed"]
        assert delta(con, names, sample="filtered")["suppressed"]
        row = delta(con, names, relation="filter_contrasts")
        assert row["suppressed"]
        assert not row["boundary_price_filter_suppressed"]
        assert row["boundary_price_filter_change"] == pytest.approx(0)
        assert row["flagged_wallet_filter_suppressed"]
        assert row["filtered_minus_all_suppressed"]
        for field in ("flagged_wallet_filter_change", "filtered_minus_all_change"):
            assert row[field] is None


def test_actor_component_uses_supported_interior_and_filtered_pair() -> None:
    rows = four_cells("unflagged", "unflagged", previous_outcomes=(1, 0))
    rows += four_cells("flagged", "flagged", flagged=True)
    # The actual populations are nested, so a supported Interior+Filtered pair
    # necessarily has supported All too. No impossible sample memberships are
    # introduced merely to manufacture a third-sample failure.
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            row = delta(con, names, weighting=weighting, relation="filter_contrasts")
            interior = delta(con, names, weighting=weighting, sample="interior_all_actors")
            filtered = delta(con, names, weighting=weighting, sample="filtered")
            assert not row["flagged_wallet_filter_suppressed"]
            assert row["flagged_wallet_filter_change"] == pytest.approx(
                filtered["spread_delta"]-interior["spread_delta"])
            assert row["flagged_wallet_filter_change"] == pytest.approx(1)
            assert not row["filtered_minus_all_suppressed"]


def test_primary_literal_normalized_boundaries_and_no_post_end_leakage() -> None:
    times = [.949999, .95, .989999, .99, 1, 1.000001]
    rows = [observation(str(number), "m", "e", .05, 0, time)
            for number, time in enumerate(times)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        assert moment(con, names, window="live_95_99")["n_executions"] == 2
        assert moment(con, names, window="live_99_100")["n_executions"] == 2


def test_disjoint_seconds_windows_include_exact_end_but_not_end_plus_one_as_live() -> None:
    relative = [-121, -120, -61, -60, -1, 0, 1, 60, 61, 120, 121]
    rows = [observation(f"central-{offset}", "m", "e", .5, 1, 1+offset/1000,
                        seconds_to_end=-offset) for offset in relative]
    rows += [observation(f"tail-{offset}", "m", "e", .05, 0, 1+offset/1000,
                         seconds_to_end=-offset) for offset in (-1, 0, 1)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        counts = dict(end_minus120_minus60=2, end_minus60_zero=5,
                      end_zero_plus60=3, end_plus60_plus120=2)
        for window, count in counts.items():
            row = one(con, names["boundary_counts"],
                      f"sport='atp' AND sample='all_trades' AND \"window\"='{window}'")
            assert row["n_executions"] == count
            assert row["central_n_executions"] == (3 if window == "end_minus60_zero" else 2)
            assert row["exact_end_n_executions"] == (2 if window == "end_minus60_zero" else 0)
            assert row["exact_end_share"] == pytest.approx(2/5 if window == "end_minus60_zero" else 0)
        assert moment(con, names)["n_executions"] == 2
        post = moment(con, names, window="end_zero_plus60", relation="boundary_summary")
        assert post["n_executions"] == 1
        assert post["calibration"] == pytest.approx(-.05)


def test_original_fill_dollar_and_market_tail_denominators() -> None:
    rows = [observation("winner", "large", "e1", .95, 1, .995, quantity=9),
            observation("same-market-loser", "large", "e1", .95, 0, .995, quantity=1),
            observation("small", "small", "e2", .95, 0, .995, quantity=1)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting, win_rate, weight_total in (("fill", 1/3, 3),
                                                  ("dollar", 9/11, .95*11),
                                                  ("equal_market", .45, 2)):
            row = moment(con, names, price_bin=10, weighting=weighting)
            assert row["n_executions"] == 3
            assert row["n_markets"] == 2
            assert row["n_events"] == 2
            assert row["mean_outcome"] == pytest.approx(win_rate)
            assert row["mean_price"] == pytest.approx(.95)
            assert row["calibration"] == pytest.approx(win_rate-.95)
            assert row["weight_total"] == pytest.approx(weight_total)


def test_leaveout_removes_every_market_of_one_dominant_event_and_reweights() -> None:
    rows = []
    for number in range(3):
        rows += four_cells(f"dominant-{number}", "dominant", quantity=1000,
                           previous_outcomes=(1, 0), sport="epl")
    rows += four_cells("ordinary-a", "ordinary-a", sport="epl")
    rows += four_cells("ordinary-b", "ordinary-b", sport="epl")
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            row = delta(con, names, relation="event_leaveout", weighting=weighting, sport="epl")
            assert row["removed_event_id"] == "dominant"
            assert row["removed_event_n_markets"] == 3
            assert row["removed_event_gross_cash_micro"] == 6_000_000_000
            assert not row["suppressed"]
            assert row["spread_delta"] == pytest.approx(0)
            assert all(row[field] == 2 for field in (
                "previous_d1_n_executions", "previous_d10_n_executions",
                "final_d1_n_executions", "final_d10_n_executions"))
            cell = moment(con, names, sport="epl", price_bin=10, weighting=weighting,
                          relation="event_leaveout_moments")
            assert cell["n_markets"] == 2 and cell["n_events"] == 2
            assert cell["calibration"] == pytest.approx(.05)


def test_leaveout_event_selection_is_gross_dollars_not_fill_count() -> None:
    rows = four_cells("big-dollar", "big-dollar", quantity=1000)
    rows += four_cells("many-small", "many-small", quantity=1, copies=100)
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            row = delta(con, names, relation="event_leaveout", weighting=weighting)
            assert row["removed_event_id"] == "big-dollar"
            assert row["final_d1_n_executions"] == 100


def test_balanced_membership_requires_every_tail_window_cell() -> None:
    complete = four_cells("complete", "complete")
    incomplete = [row for row in four_cells("incomplete", "incomplete",
                                           previous_outcomes=(1, 0))
                  if row[0] != "incomplete-previous-10-0"]
    with database(complete+incomplete) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            row = delta(con, names, relation="balanced_summary", weighting=weighting)
            assert row["spread_delta"] == pytest.approx(0)
            assert all(row[field] == 1 for field in (
                "previous_d1_n_executions", "previous_d10_n_executions",
                "final_d1_n_executions", "final_d10_n_executions"))
            cell = moment(con, names, relation="balanced_moments", weighting=weighting)
            assert cell["n_markets"] == 1 and cell["n_events"] == 1
            assert cell["original_n_executions"] == 2
            assert cell["count_share"] == pytest.approx(.5)
            assert cell["dollar_share"] == pytest.approx(.5)


def test_empty_balanced_membership_retains_null_estimates_not_zero() -> None:
    rows = [row for row in four_cells() if row[0] != "m-previous-10-0"]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        row = delta(con, names, relation="balanced_summary")
        assert row["suppressed"] and row["spread_delta"] is None
        assert row["final_d1_n_executions"] == 0


@pytest.mark.parametrize("copies", [499, 500])
def test_default_five_hundred_observation_floor_and_null_suppression(copies: int) -> None:
    with database(four_cells(copies=copies)) as con:
        names = create_terminal_diagnostics(con, "buys")
        for weighting in ("fill", "dollar", "equal_market"):
            cell = moment(con, names, weighting=weighting)
            row = delta(con, names, weighting=weighting)
            assert cell["n_executions"] == copies
            assert bool(cell["suppressed"]) == (copies < 500)
            assert bool(row["suppressed"]) == (copies < 500)
            if copies < 500:
                assert all(cell[field] is None for field in ("mean_price", "mean_outcome", "calibration"))
                assert all(row[field] is None for field in ("spread_delta", "outcome_gap_delta", "price_gap_delta"))
                assert cell["weight_total"] > 0
                assert cell["weighted_price_sum"] > 0
            else:
                assert cell["calibration"] == pytest.approx(-.05)
                assert row["spread_delta"] == pytest.approx(0)


def test_all_four_primary_tails_must_pass_the_floor() -> None:
    rows = four_cells(copies=500)
    rows = [row for row in rows if row[0] != "m-final-1-499"]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys")
        row = delta(con, names)
        assert row["final_d1_n_executions"] == 499
        assert row["previous_d1_n_executions"] == 500
        assert row["previous_d10_n_executions"] == 500
        assert row["final_d10_n_executions"] == 500
        assert row["suppressed"] and row["spread_delta"] is None


def test_cent_band_equal_market_weights_reconstruct_original_parent_tail() -> None:
    rows = [observation("a-one", "a", "a", .02, 0, .995, quantity=50),
            observation("a-two", "a", "a", .08, 1, .995, quantity=25),
            observation("b", "b", "b", .08, 0, .995, quantity=125)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        for weighting in ("fill", "dollar", "equal_market"):
            parent = moment(con, names, weighting=weighting)
            cursor = con.execute(f'SELECT * FROM "{names["price_bands"]}" WHERE '
                                 f"sport='atp' AND sample='all_trades' AND weighting='{weighting}' "
                                 "AND \"window\"='live_99_100' AND price_bin=1 AND n_executions>0")
            bands = [dict(zip((column[0] for column in cursor.description), row))
                     for row in cursor.fetchall()]
            assert {row["price_cent"] for row in bands} == {2, 8}
            for field in ("n_executions", "weight_total", "weighted_outcome_sum", "weighted_price_sum", "weighted_residual_sum"):
                assert sum(row[field] for row in bands) == pytest.approx(parent[field])
            for field in ("count_share", "dollar_share", "weight_share"):
                assert sum(row[field] for row in bands) == pytest.approx(1)
            assert sum(row["weight_share"]*row["calibration"] for row in bands) == pytest.approx(parent["calibration"])
            if weighting == "equal_market":
                low = next(row for row in bands if row["price_cent"] == 2)
                assert low["weight_total"] == pytest.approx(1/3)
                assert low["weight_share"] == pytest.approx(1/6)
                assert parent["weight_total"] == pytest.approx(2)


def test_sparse_cent_bands_preserve_shares_but_suppress_conditional_estimates() -> None:
    rows = [observation(f"rare-{number}", "a", "a", .02, 0, .995) for number in range(499)]
    rows += [observation(f"supported-{number}", "b", "b", .08, 1, .995) for number in range(500)]
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys")
        parent = moment(con, names)
        assert parent["n_executions"] == 999 and not parent["suppressed"]
        for weighting in ("fill", "dollar", "equal_market"):
            for cent, count in ((2, 499), (8, 500)):
                row = one(con, names["price_bands"],
                          f"sport='atp' AND sample='all_trades' AND weighting='{weighting}' "
                          f"AND \"window\"='live_99_100' AND price_bin=1 AND price_cent={cent}")
                assert row["n_executions"] == count
                assert row["count_share"] == pytest.approx(count/999)
                assert row["dollar_share"] > 0 and row["weight_share"] > 0
                assert bool(row["suppressed"]) == (count < 500)
                if count < 500:
                    assert all(row[field] is None for field in ("mean_price", "mean_outcome", "calibration"))
                    assert row["weighted_price_sum"] > 0
                else:
                    assert row["calibration"] == pytest.approx(.92)


def save_fixture_baseline(con: duckdb.DuckDBPyConnection, names: dict[str, str],
                          destination: Path, *, grain: str = "matched_execution",
                          mutation: str | None = None) -> None:
    """Generate tiny test outputs, never read or modify production artifacts."""
    destination.mkdir()
    con.execute(f'CREATE TABLE baseline_profiles AS SELECT \'{grain}\'::VARCHAR grain,* '
                f'FROM "{names["terminal_moments"]}"')
    con.execute(f'CREATE TABLE baseline_delta AS SELECT \'{grain}\'::VARCHAR grain,* '
                f'FROM "{names["late_identity"]}"')
    target_cell = "sport='atp' AND sample='all_trades' AND weighting='fill' AND \"window\"='live_99_100' AND price_bin=1"
    mutations = {
        "count": "n_executions=n_executions+1",
        "market_count": "n_markets=n_markets+1",
        "positive_count": "n_positive_weight_executions=n_positive_weight_executions+1",
        "quantity": "gross_quantity_micro=gross_quantity_micro+1",
        "calibration": "calibration=calibration+.01",
        "calibration_null": "calibration=NULL",
        "weight": "weight_total=weight_total+1",
        "weight_null": "weight_total=NULL",
        "dollars_null": "dollars=NULL",
        "suppression": "suppressed=NOT suppressed",
    }
    if mutation in mutations:
        con.execute(f"UPDATE baseline_profiles SET {mutations[mutation]} WHERE {target_cell}")
    elif mutation == "spread":
        con.execute("UPDATE baseline_delta SET spread_delta=spread_delta+.01 "
                    "WHERE sport='atp' AND sample='all_trades' AND weighting='fill'")
    elif mutation == "spread_null":
        con.execute("UPDATE baseline_delta SET spread_delta=NULL "
                    "WHERE sport='atp' AND sample='all_trades' AND weighting='fill'")
    elif mutation is not None:
        raise ValueError(mutation)
    for relation, filename in (("baseline_profiles", "profiles.parquet"),
                               ("baseline_delta", "late_delta.parquet")):
        path = str(destination/filename).replace("'", "''")
        con.execute(f"COPY {relation} TO '{path}' (FORMAT PARQUET)")


@pytest.mark.parametrize("grain", ["own_order_event", "matched_execution"])
def test_baseline_reproduction_accepts_identical_fixture_grain(tmp_path: Path, grain: str) -> None:
    with database(four_cells()) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        baseline = tmp_path/"baseline"
        save_fixture_baseline(con, names, baseline, grain=grain)
        reconcile_baseline(con, names["terminal_moments"], names["late_identity"],
                           baseline, grain, target="fixture_reconciliation")
        assert con.execute("SELECT count(*) FROM fixture_reconciliation").fetchone()[0] == 324


@pytest.mark.parametrize("mutation", [
    "count", "market_count", "positive_count", "quantity", "calibration", "calibration_null",
    "weight", "weight_null", "dollars_null", "suppression", "spread", "spread_null",
])
def test_baseline_reproduction_rejects_changed_support_weight_estimate_or_null(
        tmp_path: Path, mutation: str) -> None:
    with database(four_cells()) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        baseline = tmp_path/"baseline"
        save_fixture_baseline(con, names, baseline, mutation=mutation)
        with pytest.raises(ValueError, match="baseline.*reproduction failed"):
            reconcile_baseline(con, names["terminal_moments"], names["late_identity"],
                               baseline, "matched_execution", target="fixture_reconciliation")


def test_optional_baseline_gate_runs_before_secondary_diagnostics(tmp_path: Path) -> None:
    with database(four_cells()) as con:
        names = create_terminal_diagnostics(con, "buys", prefix="reference", support_floor=1)
        baseline = tmp_path/"baseline"
        save_fixture_baseline(con, names, baseline, mutation="count")
        with pytest.raises(ValueError, match="baseline.*reproduction failed"):
            create_terminal_diagnostics(con, "buys", prefix="rejected", support_floor=1,
                                        baseline_dir=baseline, grain="matched_execution")
        for table in ("rejected_price_bands", "rejected_filter_contrasts", "rejected_boundary_summary"):
            assert con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [table]).fetchone()[0] == 0


def test_production_builder_blocks_nonproduction_host_before_any_input_read(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(production_guard.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(diagnostics, "open_inputs", lambda *args: pytest.fail("Read attempted before host guard"))
    # No input paths are supplied: the real host guard must fail before touching
    # arguments or any file. The guard is not mocked into allowing production.
    with pytest.raises(RuntimeError, match="Production computation is blocked"):
        diagnostics.build_diagnostics(Namespace())


def test_cli_has_no_fixture_or_production_guard_bypass() -> None:
    arguments = []
    for name in diagnostics.INPUT_NAMES+("baseline_dir", "run_dir"):
        arguments.extend(("--"+name.replace("_", "-"), "/unused-fixture-path"))
    # Supply every required argument so an unknown flag, not missing inputs,
    # is what rejects the attempted bypass.
    diagnostics.parse_args(arguments)
    with pytest.raises(SystemExit):
        diagnostics.parse_args(arguments+["--allow-local"])


def test_cent_and_event_micro_units_remain_exact_above_double_integer_limit(tmp_path: Path) -> None:
    quantity_micro = ((2**54)//20+1)*20
    rows = []
    for row in four_cells():
        values = list(row)
        cash_micro = quantity_micro//20 if values[7] < .5 else quantity_micro*19//20
        values[10] = cash_micro/1_000_000
        values[11] = quantity_micro
        values[12] = cash_micro
        rows.append(tuple(values))
    with database(rows) as con:
        names = create_terminal_diagnostics(con, "buys", support_floor=1)
        selected = one(con, names["selected_events"], "sport='atp' AND sample='all_trades'")
        assert selected["removed_event_gross_cash_micro"] == 2*quantity_micro
        high = one(con, names["price_bands"], "sport='atp' AND sample='all_trades' "
                   "AND weighting='fill' AND \"window\"='live_99_100' AND price_cent=95")
        assert high["gross_quantity_micro"] == quantity_micro
        assert high["gross_cash_micro"] == quantity_micro*19//20
        destination = tmp_path/"bands.parquet"
        _publish(con, names["price_bands"], destination, 'sport,sample,"window",weighting,price_cent')
        table = pq.read_table(destination)
        assert table.schema.field("gross_quantity_micro").type == pa.decimal128(38, 0)
        saved = next(row for row in table.to_pylist() if row["sport"] == "atp"
                     and row["sample"] == "all_trades" and row["weighting"] == "fill"
                     and row["window"] == "live_99_100" and row["price_cent"] == 95)
        assert saved["gross_quantity_micro"] == Decimal(quantity_micro)
        assert saved["gross_cash_micro"] == Decimal(quantity_micro*19//20)
