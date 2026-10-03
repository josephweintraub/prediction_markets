"""Describe alternative accounting/composition explanations for terminal FLB.

Reuse completed verified source/FIFO stages; do not reconstruct history or raw
events. The helper accepts small synthetic relations. Only the guarded CLI may
read real production inputs. All estimates are descriptive, with the inherited
500-observation rule and original market-by-tail weights inside cent bands.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import re
import shutil
import sys
from typing import Any

import duckdb

from analysis.diagnostics.attribute_profit_taking_buys import create_buy_attribution, enrich_buy_attribution
from analysis.diagnostics.build_profit_taking_contribution import (
    GRAINS, INPUT_NAMES, SPILL_CAP_BYTES, _publish, open_inputs, verify_parent_stages,
)
from analysis.diagnostics.profit_taking_contribution import SAMPLES, SPORTS, SUPPORT_FLOOR, WEIGHTS, validate_executions
from analysis.sports_game_dynamics.artifacts import artifact_fingerprint, fingerprint, fresh_run, quoted, require_columns, write_json
from production_guard import require_production_host


PRIMARY_WINDOWS = ("live_95_99", "live_99_100")
END_WINDOWS = ("end_minus120_minus60", "end_minus60_zero", "end_zero_plus60", "end_plus60_plus120")
DIAGNOSTIC_COLUMNS = (
    "execution_id", "own_execution_id", "market_id", "event_id", "sport", "side", "is_synthetic",
    "price", "won", "residual", "usdc", "gross_quantity_micro", "gross_cash_micro",
    "realized_time", "seconds_to_end", "is_nonhuman", "exit_fraction", "hedge_fraction", "unknown_history_fraction",
)
COMPACT_OUTPUTS = (
    "terminal_moments", "late_identity", "price_bands", "filter_contrasts", "event_concentration",
    "event_leaveout", "event_leaveout_moments", "balanced_summary", "balanced_moments",
    "boundary_summary", "boundary_counts", "grain_totals", "baseline_reconciliation",
)
OUTPUT_ORDERS = {
    "terminal_moments": 'grain,sport,sample,"window",weighting,price_bin',
    "late_identity": "grain,sport,sample,weighting",
    "price_bands": 'grain,sport,sample,"window",weighting,price_bin,price_cent',
    "filter_contrasts": "grain,sport,weighting",
    "event_concentration": 'grain,sport,sample,"window",weighting,price_bin',
    "event_leaveout": "grain,sport,sample,weighting",
    "event_leaveout_moments": 'grain,sport,sample,"window",weighting,price_bin',
    "balanced_summary": "grain,sport,sample,weighting",
    "balanced_moments": 'grain,sport,sample,"window",weighting,price_bin',
    "boundary_summary": 'grain,sport,sample,"window",weighting,price_bin',
    "boundary_counts": 'grain,sport,sample,"window"',
    "grain_totals": "grain,sport",
    "baseline_reconciliation": 'grain,sport,sample,"window",weighting,price_bin',
}


def _ident(name: str) -> str:
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError("Relation names must be simple SQL identifiers")
    return '"'+name+'"'


def _values(values: tuple[Any, ...]) -> str:
    return ",".join("("+repr(value)+")" for value in values)


def _zero(con: duckdb.DuckDBPyConnection, query: str, message: str) -> None:
    if con.execute(query).fetchone()[0]:
        raise ValueError(message)


def _sample_case() -> str:
    return """CASE s.sample WHEN 'filtered' THEN price>.01 AND price<.99 AND NOT is_nonhuman
        WHEN 'interior_all_actors' THEN price>.01 AND price<.99 ELSE price>0 AND price<1 END"""


def _cells(con: duckdb.DuckDBPyConnection, source: str, target: str, extra: str = "") -> None:
    """Aggregate one actual BUY population without changing original support."""
    con.execute(f"""CREATE TEMP TABLE {_ident(target)} AS SELECT sport,sample,"window",price_bin,
        market_id,event_id{extra},count(*)::BIGINT n_executions,
        count(*) FILTER(WHERE usdc>0)::BIGINT n_positive_cash_executions,
        sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,sum(gross_cash_micro)::HUGEINT gross_cash_micro,
        sum(usdc)::DOUBLE dollars,sum(won)::DOUBLE fill_outcome_sum,sum(price)::DOUBLE fill_price_sum,
        sum(residual)::DOUBLE fill_residual_sum,sum(usdc*won)::DOUBLE dollar_outcome_sum,
        sum(usdc*price)::DOUBLE dollar_price_sum,sum(usdc*residual)::DOUBLE dollar_residual_sum
        FROM {_ident(source)} GROUP BY ALL""")


def _moments(con: duckdb.DuckDBPyConnection, cells: str, target: str,
             windows: tuple[str, ...], bins: tuple[int, ...], floor: int) -> None:
    """Recompute each original weighting on this selected market population."""
    con.execute(f"""CREATE TEMP TABLE {_ident(target)} AS WITH weighted AS (
        SELECT c.*,w.weighting,
            CASE w.weighting WHEN 'fill' THEN n_executions WHEN 'dollar' THEN dollars
                ELSE CASE WHEN dollars>0 THEN 1.0 ELSE 0.0 END END::DOUBLE weight_total,
            CASE w.weighting WHEN 'fill' THEN fill_outcome_sum WHEN 'dollar' THEN dollar_outcome_sum
                ELSE coalesce(dollar_outcome_sum/nullif(dollars,0),0) END::DOUBLE weighted_outcome_sum,
            CASE w.weighting WHEN 'fill' THEN fill_price_sum WHEN 'dollar' THEN dollar_price_sum
                ELSE coalesce(dollar_price_sum/nullif(dollars,0),0) END::DOUBLE weighted_price_sum,
            CASE w.weighting WHEN 'fill' THEN fill_residual_sum WHEN 'dollar' THEN dollar_residual_sum
                ELSE coalesce(dollar_residual_sum/nullif(dollars,0),0) END::DOUBLE weighted_residual_sum
        FROM {_ident(cells)} c CROSS JOIN (VALUES {_values(WEIGHTS)}) w(weighting)),
        grouped AS (SELECT sport,sample,"window",price_bin,weighting,
            sum(n_executions)::BIGINT n_executions,count(DISTINCT market_id)::BIGINT n_markets,
            count(DISTINCT event_id)::BIGINT n_events,
            sum(CASE WHEN weighting='fill' THEN n_executions ELSE n_positive_cash_executions END)::BIGINT n_positive_weight_executions,
            count(DISTINCT market_id) FILTER(WHERE weight_total>0)::BIGINT n_positive_weight_markets,
            sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,sum(gross_cash_micro)::HUGEINT gross_cash_micro,
            sum(dollars)::DOUBLE dollars,sum(weight_total)::DOUBLE weight_total,
            sum(weighted_outcome_sum)::DOUBLE weighted_outcome_sum,
            sum(weighted_price_sum)::DOUBLE weighted_price_sum,
            sum(weighted_residual_sum)::DOUBLE weighted_residual_sum FROM weighted GROUP BY ALL),
        grid AS (SELECT s.sport,p.sample,t.window,b.price_bin,w.weighting
            FROM (VALUES {_values(SPORTS)}) s(sport) CROSS JOIN (VALUES {_values(SAMPLES)}) p(sample)
            CROSS JOIN (VALUES {_values(windows)}) t("window") CROSS JOIN (VALUES {_values(bins)}) b(price_bin)
            CROSS JOIN (VALUES {_values(WEIGHTS)}) w(weighting)),
        joined AS (SELECT g.*,coalesce(m.n_executions,0)::BIGINT n_executions,
            coalesce(m.n_markets,0)::BIGINT n_markets,coalesce(m.n_events,0)::BIGINT n_events,
            coalesce(m.n_positive_weight_executions,0)::BIGINT n_positive_weight_executions,
            coalesce(m.n_positive_weight_markets,0)::BIGINT n_positive_weight_markets,
            coalesce(m.gross_quantity_micro,0)::HUGEINT gross_quantity_micro,
            coalesce(m.gross_cash_micro,0)::HUGEINT gross_cash_micro,
            coalesce(m.dollars,0)::DOUBLE dollars,coalesce(m.weight_total,0)::DOUBLE weight_total,
            coalesce(m.weighted_outcome_sum,0)::DOUBLE weighted_outcome_sum,
            coalesce(m.weighted_price_sum,0)::DOUBLE weighted_price_sum,
            coalesce(m.weighted_residual_sum,0)::DOUBLE weighted_residual_sum
            FROM grid g LEFT JOIN grouped m USING(sport,sample,"window",price_bin,weighting)),
        qualified AS (SELECT *,n_executions<{floor} OR weight_total<=0 suppressed FROM joined)
        SELECT *,CASE WHEN n_executions<{floor} THEN 'insufficient_original_support'
            WHEN weight_total<=0 THEN 'no_positive_weight' ELSE 'supported_descriptive' END support_status,
            CASE WHEN NOT suppressed THEN weighted_outcome_sum/weight_total END::DOUBLE mean_outcome,
            CASE WHEN NOT suppressed THEN weighted_price_sum/weight_total END::DOUBLE mean_price,
            CASE WHEN NOT suppressed THEN weighted_residual_sum/weight_total END::DOUBLE calibration,
            'not_estimated_descriptive'::VARCHAR uncertainty_status FROM qualified""")
    _zero(con, f"""SELECT count(*) FROM {_ident(target)} WHERE
        abs(weighted_residual_sum-weighted_outcome_sum+weighted_price_sum)>1e-10*greatest(weight_total,1)
        OR (NOT suppressed AND abs(calibration-mean_outcome+mean_price)>1e-12)""",
        "Outcome-minus-price accounting identity failed")


def _identity(con: duckdb.DuckDBPyConnection, moments: str, target: str) -> None:
    con.execute(f"""CREATE TEMP TABLE {_ident(target)} AS WITH tails AS (
        SELECT a.sport,a.sample,a.window,a.weighting,a.n_executions d1_n_executions,
            b.n_executions d10_n_executions,a.suppressed OR b.suppressed suppressed,
            b.mean_outcome-a.mean_outcome outcome_gap,b.mean_price-a.mean_price price_gap,
            b.calibration-a.calibration spread
        FROM {_ident(moments)} a JOIN {_ident(moments)} b USING(sport,sample,"window",weighting)
        WHERE a.price_bin=1 AND b.price_bin=10), differences AS (
        SELECT a.sport,a.sample,a.weighting,'live_99_100_minus_live_95_99'::VARCHAR contrast,
            a.d1_n_executions previous_d1_n_executions,a.d10_n_executions previous_d10_n_executions,
            b.d1_n_executions final_d1_n_executions,b.d10_n_executions final_d10_n_executions,
            a.suppressed OR b.suppressed suppressed,
            b.spread-a.spread spread_delta,b.outcome_gap-a.outcome_gap outcome_gap_delta,
            b.price_gap-a.price_gap price_gap_delta
        FROM tails a JOIN tails b USING(sport,sample,weighting)
        WHERE a.window='live_95_99' AND b.window='live_99_100')
        SELECT *,CASE WHEN suppressed THEN 'insufficient_four_tail_support_or_weight'
            ELSE 'supported_descriptive' END support_status,
            100*spread_delta::DOUBLE spread_delta_pp,'not_estimated_descriptive'::VARCHAR uncertainty_status
        FROM differences""")
    _zero(con, f"""SELECT count(*) FROM {_ident(target)} WHERE
        (suppressed AND (spread_delta IS NOT NULL OR outcome_gap_delta IS NOT NULL OR price_gap_delta IS NOT NULL))
        OR (NOT suppressed AND abs(spread_delta-outcome_gap_delta+price_gap_delta)>1e-12)""",
        "Terminal spread price/outcome identity or NULL support failed")


def _retained_support(con: duckdb.DuckDBPyConnection, selected: str, original: str) -> None:
    con.execute(f"""ALTER TABLE {_ident(selected)} ADD COLUMN original_n_executions BIGINT""")
    con.execute(f"""ALTER TABLE {_ident(selected)} ADD COLUMN count_share DOUBLE""")
    con.execute(f"""ALTER TABLE {_ident(selected)} ADD COLUMN dollar_share DOUBLE""")
    con.execute(f"""ALTER TABLE {_ident(selected)} ADD COLUMN market_share DOUBLE""")
    con.execute(f"""UPDATE {_ident(selected)} s SET original_n_executions=o.n_executions,
        count_share=s.n_executions/nullif(o.n_executions,0)::DOUBLE,
        dollar_share=s.dollars/nullif(o.dollars,0)::DOUBLE,
        market_share=s.n_markets/nullif(o.n_markets,0)::DOUBLE
        FROM {_ident(original)} o WHERE s.sport=o.sport AND s.sample=o.sample AND s.window=o.window
        AND s.price_bin=o.price_bin AND s.weighting=o.weighting""")


def create_terminal_diagnostics(con: duckdb.DuckDBPyConnection, relation: str, *,
                                prefix: str = "terminal", support_floor: int = SUPPORT_FLOOR,
                                baseline_dir: Path | None = None, grain: str | None = None) -> dict[str, str]:
    """Build deterministic summaries from one validated actual-BUY grain.

    ``seconds_to_end=end-timestamp`` is positive before end. Literal windows
    are [-120,-60),[-60,0],(0,60],(60,120] in timestamp-minus-end seconds.
    The equal-market cent weights keep the original market/tail denominator.
    Event removal uses exact gross dollars over all four primary cells, ties
    break by event_id, and removes every market of that event for all weights.
    Balanced membership requires positive dollars in both tails/windows.
    """
    if isinstance(support_floor, bool) or not isinstance(support_floor, int) or support_floor < 1:
        raise ValueError("support_floor must be a positive integer")
    if (baseline_dir is None) != (grain is None) or (grain is not None and grain not in GRAINS):
        raise ValueError("Baseline reproduction requires its directory and a frozen grain together")
    _ident(prefix)
    source = _ident(relation)
    validate_executions(con, relation)
    observed = require_columns(con, relation, DIAGNOSTIC_COLUMNS, "Terminal diagnostic input")
    for field in ("gross_quantity_micro", "gross_cash_micro"):
        if observed[field] not in {"BIGINT", "HUGEINT", "INTEGER", "UBIGINT"}:
            raise ValueError("Gross cash and quantity must use exact integer micro-units")
    _zero(con, f"""SELECT count(*) FROM {source} WHERE own_execution_id IS NULL OR trim(own_execution_id::VARCHAR)=''
        OR event_id IS NULL
        OR trim(event_id::VARCHAR)='' OR sport IS NULL OR sport NOT IN ({','.join(repr(s) for s in SPORTS)})
        OR won IS NULL OR won NOT IN (0,1) OR abs(residual-won+price)>1e-10
        OR realized_time IS NULL OR NOT isfinite(realized_time)
        OR seconds_to_end IS NULL OR NOT isfinite(seconds_to_end) OR is_nonhuman IS NULL
        OR gross_quantity_micro IS NULL OR gross_quantity_micro<=0 OR gross_cash_micro IS NULL
        OR gross_cash_micro<0 OR gross_cash_micro>gross_quantity_micro
        OR abs(price-gross_cash_micro/gross_quantity_micro::DOUBLE)>1e-12
        OR abs(usdc-gross_cash_micro/1e6::DOUBLE)>1e-12*greatest(usdc,1)""",
        "Invalid terminal outcomes, exact execution amounts, actor labels or clocks")
    _zero(con, f"""SELECT count(*) FROM (SELECT market_id FROM {source} GROUP BY 1
        HAVING count(DISTINCT sport)<>1 OR count(DISTINCT event_id)<>1)""",
        "Market identity maps to conflicting sports or events")
    keys = ("primary_focal", "market_cells", "terminal_moments", "late_identity", "price_bands",
            "filter_contrasts", "event_concentration", "selected_events", "event_leaveout_moments",
            "event_leaveout", "balanced_membership", "balanced_moments", "balanced_summary",
            "boundary_summary", "boundary_counts")
    names = {key: prefix+"_"+key for key in keys}
    for name in names.values():
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [name]).fetchone()[0] \
                or con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?", [name]).fetchone()[0]:
            raise ValueError("Refusing to replace an existing diagnostic relation: "+name)
    focal = names["primary_focal"]
    con.execute(f"""CREATE TEMP VIEW {_ident(focal)} AS SELECT b.*,s.sample,t.window,
        least(floor(price*10)::INTEGER+1,10) price_bin FROM {source} b
        CROSS JOIN (VALUES {_values(SAMPLES)}) s(sample) CROSS JOIN (VALUES {_values(PRIMARY_WINDOWS)}) t("window")
        WHERE {_sample_case()} AND (price<.1 OR price>=.9) AND
        CASE t.window WHEN 'live_95_99' THEN realized_time>=.95 AND realized_time<.99
            ELSE realized_time>=.99 AND realized_time<=1 END""")
    cells = names["market_cells"]
    _cells(con, focal, cells)
    _moments(con, cells, names["terminal_moments"], PRIMARY_WINDOWS, (1,10), support_floor)
    _identity(con, names["terminal_moments"], names["late_identity"])
    if baseline_dir is not None:
        # Production invokes this gate before optional mechanism diagnostics.
        # Small synthetic helper callers never need production-stage inputs.
        reconcile_baseline(con,names["terminal_moments"],names["late_identity"],baseline_dir,grain,
                           target="baseline_reconciliation")
    con.execute(f"""CREATE TEMP TABLE {_ident(names['filter_contrasts'])} AS
        WITH changes AS (SELECT a.sport,a.weighting,a.suppressed all_suppressed,
            i.suppressed interior_suppressed,f.suppressed filtered_suppressed,
            a.suppressed OR i.suppressed OR f.suppressed suppressed,
            a.suppressed OR i.suppressed boundary_price_filter_suppressed,
            i.suppressed OR f.suppressed flagged_wallet_filter_suppressed,
            a.spread_delta all_spread_delta,i.spread_delta interior_spread_delta,f.spread_delta filtered_spread_delta,
            i.spread_delta-a.spread_delta boundary_change,
            f.spread_delta-i.spread_delta actor_change,f.spread_delta-a.spread_delta total_change
        FROM {_ident(names['late_identity'])} a JOIN {_ident(names['late_identity'])} i USING(sport,weighting)
        JOIN {_ident(names['late_identity'])} f USING(sport,weighting)
        WHERE a.sample='all_trades' AND i.sample='interior_all_actors' AND f.sample='filtered')
        SELECT sport,weighting,all_suppressed,interior_suppressed,filtered_suppressed,suppressed,
            boundary_price_filter_suppressed,flagged_wallet_filter_suppressed,
            suppressed filtered_minus_all_suppressed,
            all_spread_delta,interior_spread_delta,filtered_spread_delta,
            CASE WHEN NOT boundary_price_filter_suppressed THEN boundary_change END boundary_price_filter_change,
            CASE WHEN NOT flagged_wallet_filter_suppressed THEN actor_change END flagged_wallet_filter_change,
            CASE WHEN NOT suppressed THEN total_change END filtered_minus_all_change,
            'boundary_then_flagged_actor'::VARCHAR filter_order,
            'not_estimated_descriptive'::VARCHAR uncertainty_status FROM changes""")
    _zero(con, f"""SELECT count(*) FROM {_ident(names['filter_contrasts'])} WHERE NOT suppressed AND
        abs(filtered_minus_all_change-boundary_price_filter_change-flagged_wallet_filter_change)>1e-12""",
        "Sequential filter identity failed")
    _price_bands(con, focal, cells, names["terminal_moments"], names["price_bands"], prefix, support_floor)
    _composition(con, cells, names, prefix, support_floor)
    _boundaries(con, source, names, prefix, support_floor)
    return names


def _price_bands(con: duckdb.DuckDBPyConnection, focal: str, cells: str, moments: str,
                 target: str, prefix: str, floor: int) -> None:
    band_focal, band_cells = prefix+"_band_focal", prefix+"_band_cells"
    con.execute(f"""CREATE TEMP VIEW {_ident(band_focal)} AS SELECT *,
        floor(price*100)::INTEGER price_cent FROM {_ident(focal)}""")
    _cells(con, band_focal, band_cells, ",price_cent")
    con.execute(f"""CREATE TEMP TABLE {_ident(target)} AS WITH weighted AS (
        SELECT b.*,w.weighting,CASE w.weighting WHEN 'fill' THEN b.n_executions WHEN 'dollar' THEN b.dollars
            ELSE coalesce(b.dollars/nullif(t.dollars,0),0) END::DOUBLE weight_total,
            CASE w.weighting WHEN 'fill' THEN b.fill_outcome_sum WHEN 'dollar' THEN b.dollar_outcome_sum
                ELSE coalesce(b.dollar_outcome_sum/nullif(t.dollars,0),0) END::DOUBLE weighted_outcome_sum,
            CASE w.weighting WHEN 'fill' THEN b.fill_price_sum WHEN 'dollar' THEN b.dollar_price_sum
                ELSE coalesce(b.dollar_price_sum/nullif(t.dollars,0),0) END::DOUBLE weighted_price_sum,
            CASE w.weighting WHEN 'fill' THEN b.fill_residual_sum WHEN 'dollar' THEN b.dollar_residual_sum
                ELSE coalesce(b.dollar_residual_sum/nullif(t.dollars,0),0) END::DOUBLE weighted_residual_sum
        FROM {_ident(band_cells)} b JOIN {_ident(cells)} t USING(sport,sample,"window",price_bin,market_id,event_id)
        CROSS JOIN (VALUES {_values(WEIGHTS)}) w(weighting)), grouped AS (
        SELECT sport,sample,"window",price_bin,price_cent,weighting,sum(n_executions)::BIGINT n_executions,
            count(DISTINCT market_id)::BIGINT n_markets,count(DISTINCT event_id)::BIGINT n_events,
            sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,sum(gross_cash_micro)::HUGEINT gross_cash_micro,
            sum(dollars)::DOUBLE dollars,sum(weight_total)::DOUBLE weight_total,
            sum(weighted_outcome_sum)::DOUBLE weighted_outcome_sum,sum(weighted_price_sum)::DOUBLE weighted_price_sum,
            sum(weighted_residual_sum)::DOUBLE weighted_residual_sum FROM weighted GROUP BY ALL), grid AS (
        SELECT m.*,c.price_cent FROM {_ident(moments)} m CROSS JOIN range(0,100) c(price_cent)
        WHERE (m.price_bin=1 AND c.price_cent<10) OR (m.price_bin=10 AND c.price_cent>=90)), joined AS (
        SELECT g.sport,g.sample,g.window,g.price_bin,g.price_cent::INTEGER price_cent,g.weighting,
            coalesce(b.n_executions,0)::BIGINT n_executions,coalesce(b.n_markets,0)::BIGINT n_markets,
            coalesce(b.n_events,0)::BIGINT n_events,coalesce(b.gross_quantity_micro,0)::HUGEINT gross_quantity_micro,
            coalesce(b.gross_cash_micro,0)::HUGEINT gross_cash_micro,coalesce(b.dollars,0)::DOUBLE dollars,
            coalesce(b.weight_total,0)::DOUBLE weight_total,
            coalesce(b.weighted_outcome_sum,0)::DOUBLE weighted_outcome_sum,
            coalesce(b.weighted_price_sum,0)::DOUBLE weighted_price_sum,
            coalesce(b.weighted_residual_sum,0)::DOUBLE weighted_residual_sum,
            coalesce(b.n_executions,0)/nullif(g.n_executions,0)::DOUBLE count_share,
            coalesce(b.dollars,0)/nullif(g.dollars,0)::DOUBLE dollar_share,
            coalesce(b.weight_total,0)/nullif(g.weight_total,0)::DOUBLE weight_share
        FROM grid g LEFT JOIN grouped b USING(sport,sample,"window",price_bin,price_cent,weighting)), qualified AS (
        SELECT *,n_executions<{floor} OR weight_total<=0 suppressed FROM joined)
        SELECT *,CASE WHEN NOT suppressed THEN weighted_outcome_sum/weight_total END::DOUBLE mean_outcome,
            CASE WHEN NOT suppressed THEN weighted_price_sum/weight_total END::DOUBLE mean_price,
            CASE WHEN NOT suppressed THEN weighted_residual_sum/weight_total END::DOUBLE calibration,
            CASE WHEN suppressed THEN 'insufficient_original_support_or_weight' ELSE 'supported_descriptive' END support_status,
            'original_market_tail_window_weights'::VARCHAR weight_contract,
            'not_estimated_descriptive'::VARCHAR uncertainty_status FROM qualified""")
    _zero(con, f"""SELECT count(*) FROM (SELECT sport,sample,"window",price_bin,weighting,
        sum(n_executions)::BIGINT n_executions,sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,
        sum(gross_cash_micro)::HUGEINT gross_cash_micro,sum(weight_total) weight_total,
        sum(weighted_outcome_sum) weighted_outcome_sum,sum(weighted_price_sum) weighted_price_sum,
        sum(weighted_residual_sum) weighted_residual_sum FROM {_ident(target)} GROUP BY ALL) b
        JOIN {_ident(moments)} m USING(sport,sample,"window",price_bin,weighting)
        WHERE b.n_executions<>m.n_executions OR b.gross_quantity_micro<>m.gross_quantity_micro
        OR b.gross_cash_micro<>m.gross_cash_micro OR
        abs(b.weight_total-m.weight_total)>1e-10*greatest(m.weight_total,1)
        OR abs(b.weighted_outcome_sum-m.weighted_outcome_sum)>1e-10*greatest(m.weight_total,1)
        OR abs(b.weighted_price_sum-m.weighted_price_sum)>1e-10*greatest(m.weight_total,1)
        OR abs(b.weighted_residual_sum-m.weighted_residual_sum)>1e-10*greatest(m.weight_total,1)""",
        "Cent bands changed original tail weight, numerators or exact support")
    con.execute(f"DROP TABLE {_ident(band_cells)}")
    con.execute(f"DROP VIEW {_ident(band_focal)}")


def _composition(con: duckdb.DuckDBPyConnection, cells: str, names: dict[str,str], prefix: str, floor: int) -> None:
    con.execute(f"""CREATE TEMP TABLE {_ident(names['event_concentration'])} AS WITH events AS (
        SELECT sport,sample,"window",price_bin,event_id,w.weighting,count(*)::BIGINT n_markets,
            sum(CASE w.weighting WHEN 'fill' THEN n_executions WHEN 'dollar' THEN dollars
                ELSE CASE WHEN dollars>0 THEN 1.0 ELSE 0.0 END END)::DOUBLE event_weight
        FROM {_ident(cells)} CROSS JOIN (VALUES {_values(WEIGHTS)}) w(weighting) GROUP BY ALL), ranked AS (
        SELECT *,row_number() OVER(PARTITION BY sport,sample,"window",price_bin,weighting
            ORDER BY event_weight DESC,event_id) event_rank FROM events), sums AS (
        SELECT sport,sample,"window",price_bin,weighting,count(*)::BIGINT n_events,
            sum(n_markets)::BIGINT n_markets,sum(event_weight) weight_total,
            max(CASE WHEN event_rank=1 THEN event_id END) top1_event_id,
            max(CASE WHEN event_rank=1 THEN event_weight END)/nullif(sum(event_weight),0) top1_weight_share,
            sum(CASE WHEN event_rank<=10 THEN event_weight ELSE 0 END)/nullif(sum(event_weight),0) top10_weight_share
        FROM ranked GROUP BY ALL)
        SELECT m.sport,m.sample,m.window,m.price_bin,m.weighting,m.n_events,m.n_markets,m.weight_total,
            s.top1_event_id,s.top1_weight_share,s.top10_weight_share
        FROM {_ident(names['terminal_moments'])} m LEFT JOIN sums s USING(sport,sample,"window",price_bin,weighting)""")
    con.execute(f"""CREATE TEMP TABLE {_ident(names['selected_events'])} AS WITH event_totals AS (
        SELECT sport,sample,event_id,sum(gross_cash_micro)::HUGEINT removed_event_gross_cash_micro,
            count(DISTINCT market_id)::BIGINT removed_event_n_markets FROM {_ident(cells)} GROUP BY ALL)
        SELECT sport,sample,event_id removed_event_id,removed_event_gross_cash_micro,removed_event_n_markets
        FROM event_totals QUALIFY row_number() OVER(PARTITION BY sport,sample
            ORDER BY removed_event_gross_cash_micro DESC,event_id)=1""")
    leave_cells = prefix+"_leave_cells"
    con.execute(f"""CREATE TEMP VIEW {_ident(leave_cells)} AS SELECT c.* FROM {_ident(cells)} c
        JOIN {_ident(names['selected_events'])} e USING(sport,sample) WHERE c.event_id<>e.removed_event_id""")
    _moments(con, leave_cells, names["event_leaveout_moments"], PRIMARY_WINDOWS, (1,10), floor)
    _retained_support(con, names["event_leaveout_moments"], names["terminal_moments"])
    _identity(con, names["event_leaveout_moments"], names["event_leaveout"])
    con.execute(f"""ALTER TABLE {_ident(names['event_leaveout'])} ADD COLUMN removed_event_id VARCHAR""")
    con.execute(f"""ALTER TABLE {_ident(names['event_leaveout'])} ADD COLUMN removed_event_gross_cash_micro HUGEINT""")
    con.execute(f"""ALTER TABLE {_ident(names['event_leaveout'])} ADD COLUMN removed_event_n_markets BIGINT""")
    con.execute(f"""UPDATE {_ident(names['event_leaveout'])} t SET removed_event_id=e.removed_event_id,
        removed_event_gross_cash_micro=e.removed_event_gross_cash_micro,removed_event_n_markets=e.removed_event_n_markets
        FROM {_ident(names['selected_events'])} e WHERE t.sport=e.sport AND t.sample=e.sample""")
    con.execute(f"DROP VIEW {_ident(leave_cells)}")
    con.execute(f"""CREATE TEMP TABLE {_ident(names['balanced_membership'])} AS
        SELECT sport,sample,market_id,event_id FROM {_ident(cells)} WHERE dollars>0 GROUP BY ALL
        HAVING count(DISTINCT "window"||':'||price_bin::VARCHAR)=4""")
    balanced_cells = prefix+"_balanced_cells"
    con.execute(f"""CREATE TEMP VIEW {_ident(balanced_cells)} AS SELECT c.* FROM {_ident(cells)} c
        JOIN {_ident(names['balanced_membership'])} b USING(sport,sample,market_id,event_id)""")
    _moments(con, balanced_cells, names["balanced_moments"], PRIMARY_WINDOWS, (1,10), floor)
    _retained_support(con, names["balanced_moments"], names["terminal_moments"])
    _identity(con, names["balanced_moments"], names["balanced_summary"])
    con.execute(f"""ALTER TABLE {_ident(names['balanced_summary'])} ADD COLUMN selection_contract VARCHAR
        DEFAULT 'positive_dollars_in_both_tails_and_both_primary_windows'""")
    con.execute(f"DROP VIEW {_ident(balanced_cells)}")


def _boundaries(con: duckdb.DuckDBPyConnection, source: str, names: dict[str,str], prefix: str, floor: int) -> None:
    focal, cells = prefix+"_end_focal", prefix+"_end_cells"
    con.execute(f"""CREATE TEMP VIEW {_ident(focal)} AS SELECT b.*,s.sample,t.window,
        least(floor(price*10)::INTEGER+1,10) price_bin FROM {source} b
        CROSS JOIN (VALUES {_values(SAMPLES)}) s(sample) CROSS JOIN (VALUES {_values(END_WINDOWS)}) t("window")
        WHERE {_sample_case()} AND CASE t.window
            WHEN 'end_minus120_minus60' THEN seconds_to_end>60 AND seconds_to_end<=120
            WHEN 'end_minus60_zero' THEN seconds_to_end>=0 AND seconds_to_end<=60
            WHEN 'end_zero_plus60' THEN seconds_to_end<0 AND seconds_to_end>=-60
            ELSE seconds_to_end<-60 AND seconds_to_end>=-120 END""")
    _cells(con, focal, cells)
    _moments(con, cells, names["boundary_summary"], END_WINDOWS, tuple(range(1,11)), floor)
    con.execute(f"""CREATE TEMP TABLE {_ident(names['boundary_counts'])} AS WITH counts AS (
        SELECT sport,sample,"window",count(*)::BIGINT n_executions,
            sum(gross_cash_micro)::HUGEINT gross_cash_micro,
            count(*) FILTER(WHERE seconds_to_end=0)::BIGINT exact_end_n_executions,
            sum(CASE WHEN seconds_to_end=0 THEN gross_cash_micro ELSE 0 END)::HUGEINT exact_end_gross_cash_micro,
            count(*) FILTER(WHERE price>=.1 AND price<=.9)::BIGINT central_n_executions,
            sum(CASE WHEN price>=.1 AND price<=.9 THEN gross_cash_micro ELSE 0 END)::HUGEINT central_gross_cash_micro
        FROM {_ident(focal)} GROUP BY ALL), grid AS (
        SELECT s.sport,p.sample,t.window FROM (VALUES {_values(SPORTS)}) s(sport)
            CROSS JOIN (VALUES {_values(SAMPLES)}) p(sample) CROSS JOIN (VALUES {_values(END_WINDOWS)}) t("window"))
        SELECT g.*,coalesce(c.n_executions,0)::BIGINT n_executions,
            coalesce(c.gross_cash_micro,0)::HUGEINT gross_cash_micro,
            coalesce(c.exact_end_n_executions,0)::BIGINT exact_end_n_executions,
            coalesce(c.exact_end_gross_cash_micro,0)::HUGEINT exact_end_gross_cash_micro,
            coalesce(c.central_n_executions,0)::BIGINT central_n_executions,
            coalesce(c.central_gross_cash_micro,0)::HUGEINT central_gross_cash_micro,
            c.exact_end_n_executions/nullif(c.n_executions,0)::DOUBLE exact_end_share,
            c.exact_end_gross_cash_micro/nullif(c.gross_cash_micro,0)::DOUBLE exact_end_dollar_share,
            'count_or_cash_within_named_disjoint_end_window'::VARCHAR share_denominator
        FROM grid g LEFT JOIN counts c USING(sport,sample,"window")""")
    _zero(con, f"""SELECT count(*) FROM (SELECT execution_id,sample FROM {_ident(focal)} GROUP BY ALL HAVING count(*)<>1)""",
        "Literal end windows overlap")
    _zero(con, f"""SELECT count(*) FROM {_ident(names['boundary_summary'])} m JOIN (
        SELECT sport,sample,"window",sum(n_executions)::BIGINT n_executions,
            sum(gross_cash_micro)::HUGEINT gross_cash_micro FROM {_ident(names['boundary_summary'])}
        WHERE weighting='fill' GROUP BY ALL) s USING(sport,sample,"window")
        JOIN {_ident(names['boundary_counts'])} c USING(sport,sample,"window")
        WHERE s.n_executions<>c.n_executions OR s.gross_cash_micro<>c.gross_cash_micro""",
        "End profiles do not reconcile to disjoint exact counts/cash")
    con.execute(f"DROP TABLE {_ident(cells)}")
    con.execute(f"DROP VIEW {_ident(focal)}")


def verify_baseline(paths: dict[str,Path], baseline_dir: Path) -> dict[str,Any]:
    """Require every completed baseline output and exactly matching input lineage."""
    manifest_path = baseline_dir/"manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("completion_status") != "complete" or manifest.get("stage") != "sports_profit_taking_two_grain_contributions_v1":
        raise ValueError("Completed two-grain baseline stage is required")
    for name in INPUT_NAMES:
        if manifest.get("inputs",{}).get(name) != fingerprint(paths[name]):
            raise ValueError("Baseline input lineage mismatch: "+name)
    if manifest.get("contract",{}).get("grains") != list(GRAINS) or manifest.get("contract",{}).get("samples") != list(SAMPLES) \
            or manifest.get("contract",{}).get("weights") != list(WEIGHTS):
        raise ValueError("Baseline grains, samples or weights differ from the frozen contract")
    for name, expected in manifest.get("outputs",{}).items():
        if Path(name).name != name or artifact_fingerprint(baseline_dir/name) != expected:
            raise ValueError("Completed baseline output fingerprint mismatch: "+name)
    required = {"profiles.parquet", "tails.parquet", "late_delta.parquet", "support.parquet", "grain_totals.parquet"}
    if not required.issubset(manifest.get("outputs",{})):
        raise ValueError("Completed baseline lacks required profile, support or grain totals")
    return {"manifest":fingerprint(manifest_path), "outputs":manifest["outputs"]}


def reconcile_baseline(con: duckdb.DuckDBPyConnection, moments: str, identity: str, baseline_dir: Path,
                       grain: str, *, target: str) -> None:
    """Independently reproduce every primary tail cell, support and late contrast."""
    con.execute(f"""CREATE TEMP TABLE {_ident(target)} AS SELECT m.sport,m.sample,m.window,m.weighting,m.price_bin,
        m.n_executions,m.n_markets,m.n_positive_weight_executions,m.n_positive_weight_markets,
        m.gross_quantity_micro,m.dollars,m.weight_total,m.suppressed,m.calibration,
        p.n_executions baseline_n_executions,p.n_markets baseline_n_markets,
        p.n_positive_weight_executions baseline_n_positive_weight_executions,
        p.n_positive_weight_markets baseline_n_positive_weight_markets,
        p.gross_quantity_micro baseline_gross_quantity_micro,p.dollars baseline_dollars,
        p.weight_total baseline_weight_total,p.suppressed baseline_suppressed,p.calibration baseline_calibration
        FROM {_ident(moments)} m FULL JOIN (SELECT * FROM read_parquet('{quoted(baseline_dir/'profiles.parquet')}')
        WHERE grain='{grain}' AND "window" IN ('live_95_99','live_99_100') AND price_bin IN (1,10)) p
        USING(sport,sample,"window",weighting,price_bin)""")
    _zero(con, f"""SELECT count(*) FROM {_ident(target)} WHERE
        n_executions IS DISTINCT FROM baseline_n_executions OR suppressed IS DISTINCT FROM baseline_suppressed
        OR n_markets IS DISTINCT FROM baseline_n_markets
        OR n_positive_weight_executions IS DISTINCT FROM baseline_n_positive_weight_executions
        OR n_positive_weight_markets IS DISTINCT FROM baseline_n_positive_weight_markets
        OR gross_quantity_micro IS DISTINCT FROM baseline_gross_quantity_micro
        OR dollars IS NULL OR baseline_dollars IS NULL OR NOT isfinite(dollars) OR NOT isfinite(baseline_dollars)
        OR weight_total IS NULL OR baseline_weight_total IS NULL
        OR NOT isfinite(weight_total) OR NOT isfinite(baseline_weight_total)
        OR abs(dollars-baseline_dollars)>1e-10*greatest(baseline_dollars,1)
        OR abs(weight_total-baseline_weight_total)>1e-10*greatest(baseline_weight_total,1)
        OR (calibration IS NULL)<>(baseline_calibration IS NULL)
        OR (calibration IS NOT NULL AND NOT isfinite(calibration))
        OR (baseline_calibration IS NOT NULL AND NOT isfinite(baseline_calibration))
        OR abs(calibration-baseline_calibration)>1e-10""", "Completed baseline tail support/calibration reproduction failed")
    _zero(con, f"""SELECT count(*) FROM {_ident(identity)} m FULL JOIN (
        SELECT * FROM read_parquet('{quoted(baseline_dir/'late_delta.parquet')}') WHERE grain='{grain}') b
        USING(sport,sample,weighting) WHERE m.sport IS NULL OR b.sport IS NULL
        OR m.previous_d1_n_executions IS DISTINCT FROM b.previous_d1_n_executions
        OR m.previous_d10_n_executions IS DISTINCT FROM b.previous_d10_n_executions
        OR m.final_d1_n_executions IS DISTINCT FROM b.final_d1_n_executions
        OR m.final_d10_n_executions IS DISTINCT FROM b.final_d10_n_executions
        OR m.suppressed IS DISTINCT FROM b.suppressed OR (m.spread_delta IS NULL)<>(b.spread_delta IS NULL)
        OR (m.spread_delta IS NOT NULL AND NOT isfinite(m.spread_delta))
        OR (b.spread_delta IS NOT NULL AND NOT isfinite(b.spread_delta))
        OR abs(m.spread_delta-b.spread_delta)>1e-10""", "Completed baseline terminal contrast reproduction failed")


def build_diagnostics(args: argparse.Namespace) -> dict[str,Any]:
    """Run the verified immutable production workflow; no fixture bypass flag."""
    require_production_host()
    if not Path(__file__).resolve().is_relative_to(Path("/home/ubuntu/prediction_markets")):
        raise RuntimeError("Diagnostic production builder requires canonical source")
    if args.threads < 1 or not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", args.memory_limit):
        raise ValueError("Positive threads and explicit MB/GB memory limit required")
    paths = {name:Path(getattr(args,name)).resolve() for name in INPUT_NAMES}
    baseline_dir = Path(args.baseline_dir).resolve()
    if any(not p.is_file() for p in paths.values()):
        raise FileNotFoundError("Complete stages and metadata inputs are required")
    parents = verify_parent_stages(paths, require_native=True)
    baseline = verify_baseline(paths, baseline_dir)
    initial = {name:fingerprint(path) for name,path in paths.items()}
    existing = Path(args.run_dir).resolve().parent
    while not existing.exists():
        existing = existing.parent
    free_bytes = shutil.disk_usage(existing).free
    required_free = SPILL_CAP_BYTES+4*1024**3
    if free_bytes < required_free:
        raise ValueError(f"Insufficient disk reserve: free={free_bytes}, required={required_free}")
    counts: dict[str,int] = {}
    with fresh_run(args.run_dir, (*paths.values(), baseline_dir)) as staging:
        con = duckdb.connect()
        try:
            con.execute(f"SET threads={args.threads}")
            con.execute(f"SET memory_limit='{args.memory_limit}'")
            con.execute("SET max_temp_directory_size='12GB'")
            con.execute("SET TimeZone='UTC'")
            if args.temp_directory:
                con.execute(f"SET temp_directory='{quoted(args.temp_directory)}'")
            open_inputs(con, paths)
            attribution = create_buy_attribution(con,"own_actions","action_tags","batch_links",prefix="attribution")
            for grain, relation in zip(GRAINS,(attribution["buy_tags"],attribution["matched_buy_tags"])):
                print(f"{datetime.now(timezone.utc).isoformat()} summarizing {grain}",file=sys.stderr,flush=True)
                enriched = enrich_buy_attribution(con,relation,"market_tokens","market_clocks","block_timestamps","wallet_flags",output="current_enriched")
                con.execute("CREATE TEMP TABLE current_executions AS SELECT "+",".join(DIAGNOSTIC_COLUMNS)+f" FROM {_ident(enriched)}")
                counts[grain] = int(con.execute("SELECT count(*) FROM current_executions").fetchone()[0])
                names = create_terminal_diagnostics(con,"current_executions",prefix="diagnostic",
                                                    baseline_dir=baseline_dir,grain=grain)
                con.execute("""CREATE TEMP TABLE grain_totals AS SELECT sport,count(*)::BIGINT n_executions,
                    count(DISTINCT own_execution_id)::BIGINT n_own_buy_events,
                    sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,sum(gross_cash_micro)::HUGEINT gross_cash_micro,
                    sum(gross_quantity_micro*residual)::DOUBLE quantity_residual_numerator
                    FROM current_executions GROUP BY sport""")
                for output in COMPACT_OUTPUTS:
                    rel = names.get(output,output)
                    if grain == GRAINS[0]:
                        con.execute(f"CREATE TEMP TABLE all_{output} AS SELECT '{grain}'::VARCHAR grain,* FROM {_ident(rel)}")
                    else:
                        con.execute(f"INSERT INTO all_{output} SELECT '{grain}',* FROM {_ident(rel)}")
                # These three identity-bearing tables stay on EC2; they contain
                # market/event support, never wallet identities or FIFO links.
                for key in ("market_cells","balanced_membership","selected_events"):
                    path = staging/(grain+"_"+key+".parquet")
                    order = {'market_cells':'sport,sample,"window",price_bin,market_id',
                             'balanced_membership':'sport,sample,market_id',
                             'selected_events':'sport,sample'}[key]
                    counts[path.name] = _publish(con,names[key],path,order)
                for name in names.values():
                    if name != names["primary_focal"]:
                        con.execute(f"DROP TABLE {_ident(name)}")
                con.execute(f"DROP VIEW {_ident(names['primary_focal'])}")
                con.execute("DROP TABLE baseline_reconciliation")
                con.execute("DROP TABLE grain_totals")
                con.execute("DROP TABLE current_executions")
                con.execute(f"DROP VIEW {_ident(enriched)}")
            _zero(con,"""SELECT count(*) FROM (SELECT * FROM all_grain_totals WHERE grain='own_order_event') a
                FULL JOIN (SELECT * FROM all_grain_totals WHERE grain='matched_execution') b USING(sport)
                WHERE a.gross_quantity_micro IS DISTINCT FROM b.gross_quantity_micro
                OR a.gross_cash_micro IS DISTINCT FROM b.gross_cash_micro
                OR a.n_own_buy_events IS DISTINCT FROM b.n_own_buy_events
                OR a.quantity_residual_numerator IS NULL OR b.quantity_residual_numerator IS NULL
                OR NOT isfinite(a.quantity_residual_numerator) OR NOT isfinite(b.quantity_residual_numerator)
                OR abs(a.quantity_residual_numerator-b.quantity_residual_numerator)>1e-10*greatest(a.gross_quantity_micro,1)""",
                "Cross-grain exact cash, quantity, own-action or quantity-residual conservation failed")
            _zero(con,f"""SELECT count(*) FROM all_grain_totals a FULL JOIN
                read_parquet('{quoted(baseline_dir/'grain_totals.parquet')}') b USING(grain,sport)
                WHERE a.n_executions IS DISTINCT FROM b.n_executions
                OR a.n_own_buy_events IS DISTINCT FROM b.n_own_buy_events
                OR a.gross_quantity_micro IS DISTINCT FROM b.gross_quantity_micro
                OR a.gross_cash_micro IS DISTINCT FROM b.gross_cash_micro
                OR a.quantity_residual_numerator IS NULL OR b.quantity_residual_numerator IS NULL
                OR NOT isfinite(a.quantity_residual_numerator) OR NOT isfinite(b.quantity_residual_numerator)
                OR abs(a.quantity_residual_numerator-b.quantity_residual_numerator)>1e-10*greatest(a.gross_quantity_micro,1)""",
                "All-history grain support/exposure differs from completed baseline")
            for output in COMPACT_OUTPUTS:
                counts[output] = _publish(con,"all_"+output,staging/(output+".parquet"),OUTPUT_ORDERS[output])
        finally:
            con.close()
        if {name:fingerprint(path) for name,path in paths.items()} != initial:
            raise ValueError("An immutable input changed during diagnostics")
        if verify_parent_stages(paths,require_native=True) != parents or verify_baseline(paths,baseline_dir) != baseline:
            raise ValueError("A completed parent or its native proof changed during diagnostics")
        write_json(staging/"summary.json",{"analysis":"terminal_pattern_diagnostics_v1","counts":counts,
            "support_floor":SUPPORT_FLOOR,"uncertainty_status":"not_estimated_descriptive",
            "gates":{"baseline_reproduced":True,"exact_cross_grain_conservation":True,
                     "price_outcome_identity":True,"cent_band_tail_reconciliation":True,
                     "sequential_filter_identity":True,"disjoint_end_windows":True}})
        manifest = {"stage":"terminal_pattern_diagnostics_v1","schema_version":1,"completion_status":"complete",
            "created_at_utc":datetime.now(timezone.utc).isoformat(),"command":sys.argv,
            "command_arguments":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
            "environment":{"python":platform.python_version(),"duckdb":duckdb.__version__},
            "inputs":initial,"parent_stage_gates":parents,"baseline_stage_gates":baseline,
            "code":{name:fingerprint(Path(__file__).resolve().parents[2]/name) for name in (
                "analysis/diagnostics/build_terminal_pattern_diagnostics.py",
                "analysis/diagnostics/build_profit_taking_contribution.py",
                "analysis/diagnostics/attribute_profit_taking_buys.py",
                "analysis/diagnostics/profit_taking_contribution.py",
                "analysis/diagnostics/build_profit_taking_ledger.py",
                "analysis/diagnostics/profit_taking_source_audit.py",
                "analysis/diagnostics/profit_taking_actions.py",
                "analysis/sports_game_dynamics/artifacts.py","production_guard.py")},
            "spec":fingerprint(Path(__file__).resolve().parents[2]/"docs/analysis_specs/terminal_pattern_diagnostics_v1.md"),
            "contract":{"grains":list(GRAINS),"sports":list(SPORTS),"samples":list(SAMPLES),"weights":list(WEIGHTS),
                "primary_windows":list(PRIMARY_WINDOWS),"end_windows":list(END_WINDOWS),"support_floor":SUPPORT_FLOOR,
                "cent_weights":"Original market×tail×window dollar normalization; shares/numerators add to parent tail.",
                "filters":"0<P<1; then .01<P<.99; then unflagged. Each component requires its2 contributing samples; full summed sequential decomposition requires all3 support.",
                "event_removal":"One highest-exact-gross-dollar event over four primary cells per grain/sport/sample; all propositions removed together; tie event_id ascending.",
                "balanced":"Positive dollars in both tails and both primary windows; recompute original weights on selected membership.",
                "end_counts":"Exact-end and central .1<=P<=.9 counts/cash inside four named disjoint literal windows; exact-end share denominator is its named window.",
                "clocks":"Inherited accepted exact block timing; ATP scheduled-start/archive-duration qualifications retained. No corrected outcome-knowledge time.",
                "uncertainty":"Descriptive; no new CI, significance or causal claims.",
                "private_outputs":"Market/event membership tables remain EC2; only COMPACT_OUTPUTS plus JSON/manifests are eligible for compact transfer."},
            "counts":counts,"disk_preflight":{"free_bytes":free_bytes,"required_free_bytes":required_free,"spill_cap_bytes":SPILL_CAP_BYTES},
            "outputs":{p.name:artifact_fingerprint(p) for p in sorted(staging.iterdir())}}
        write_json(staging/"manifest.json",manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (*INPUT_NAMES,"baseline_dir","run_dir"):
        parser.add_argument("--"+name.replace("_","-"),type=Path,required=True)
    parser.add_argument("--threads",type=int,default=8)
    parser.add_argument("--memory-limit",default="100GB")
    parser.add_argument("--temp-directory",type=Path)
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    print(json.dumps(build_diagnostics(args)["counts"],sort_keys=True))


if __name__ == "__main__":
    main()
