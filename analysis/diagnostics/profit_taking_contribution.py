"""Exact descriptive FLB accounting over original actual BUY executions.

The caller supplies already verified source actions and outcome-blind FIFO tags.
This module does not reconstruct positions, fabricate complementary BUY records,
read raw data, or identify a causal price effect. A linked lot is not an additional
execution. All component estimates retain the original bin's denominator.
"""
from __future__ import annotations

import re

import duckdb

from analysis.sports_game_dynamics.artifacts import require_columns


SUPPORT_FLOOR = 500
INPUT_COLUMNS = (
    "execution_id", "market_id", "side", "is_synthetic", "price", "residual",
    "usdc", "exit_fraction", "hedge_fraction", "unknown_history_fraction",
)
WEIGHTS = ("fill", "dollar", "equal_market")
SPORTS = ("mlb", "nfl", "nba", "nhl", "cbb", "cfb", "atp", "epl", "wnba")
SAMPLES = ("filtered", "interior_all_actors", "all_trades")
WINDOWS = (
    "pregame", "live", "live_first_third", "live_middle_third", "live_final_third",
    "live_80_90", "live_90_95", "live_95_99", "live_99_100", "last_120_seconds",
)


def _identifier(name: str) -> str:
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError("Relation names must be simple SQL identifiers")
    return '"' + name + '"'


def validate_executions(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    """Fail closed on multiplied links or unverifiable original-BUY inputs.

    ``unknown_history_fraction`` is a supplied diagnostic fraction of focal
    quantity whose classification is unknown. It does not establish coverage of
    opening balances or nontrade flows, and is not silently interpreted as zero.
    ``residual`` must be consistent with a binary outcome minus execution price;
    the implied outcome is used only in this validation, never in tag assignment.
    """
    source = _identifier(relation)
    require_columns(con, relation, INPUT_COLUMNS, "Original BUY contribution input")
    if con.execute(f"""SELECT count(*) FROM {source} WHERE execution_id IS NULL
        OR trim(CAST(execution_id AS VARCHAR))='' OR market_id IS NULL
        OR trim(CAST(market_id AS VARCHAR))='' OR upper(CAST(side AS VARCHAR))<>'BUY'
        OR side IS NULL OR try_cast(is_synthetic AS BOOLEAN) IS DISTINCT FROM FALSE""").fetchone()[0]:
        raise ValueError("Every row must identify an original, nonsynthetic actual BUY")
    if con.execute(f"""SELECT count(*) FROM (SELECT execution_id FROM {source}
        GROUP BY execution_id HAVING count(*)<>1)""").fetchone()[0]:
        raise ValueError("Duplicate execution IDs: collapse lot links before estimating")
    invalid_numbers = " OR ".join(
        f"try_cast({column} AS DOUBLE) IS NULL OR NOT isfinite(try_cast({column} AS DOUBLE))"
        for column in INPUT_COLUMNS[4:]
    )
    if con.execute(f"SELECT count(*) FROM {source} WHERE {invalid_numbers}").fetchone()[0]:
        raise ValueError("Prices, residuals, amounts and fractions must be finite and nonnull")
    if con.execute(f"""SELECT count(*) FROM {source} WHERE price<0 OR price>1 OR usdc<0
        OR exit_fraction<0 OR exit_fraction>1 OR hedge_fraction<0 OR hedge_fraction>1
        OR unknown_history_fraction<0 OR unknown_history_fraction>1
        OR exit_fraction+hedge_fraction>1
        OR (exit_fraction>0 AND price<=.5) OR (hedge_fraction>0 AND price>=.5)
        OR least(abs(residual+price),abs(residual+price-1))>1e-10""").fetchone()[0]:
        raise ValueError("Invalid binary residual, amount, fraction or current-price regime")
    return int(con.execute(f"SELECT count(*) FROM {source}").fetchone()[0])


def create_contribution_summaries(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    *,
    prefix: str = "profit_taking",
    support_floor: int = SUPPORT_FLOOR,
) -> dict[str, str]:
    """Create compact profile and tail tables from a caller-owned lazy relation.

    The relation is one already selected sport/sample/window population. Input
    has one row per original own-order BUY log, not per matched acquisition lot
    or counterparty leg. For an active aggregate, its effective-cash/quantity
    VWAP determines the original price bin and its fill weight is still one.
    Direct-exit fractions map the seller's profitable, net-exposure-reducing
    gross disposed fraction to matched gross BUY quantity, divided by that BUY's
    gross quantity. Hedge fractions divide qualified net acquired tokens by
    total net acquired tokens. This net/net convention prorates the entire BUY's
    fill and dollar weights; fees are reflected upstream in gross cash divided
    by net acquired tokens when determining locked profit. Source, refund, FIFO,
    profit and net-exposure gates belong upstream.

    Weights are one per execution, original execution dollars, or original
    dollars divided by market-by-bin dollars. Equal-market bin estimates average
    positive-dollar market VWAPs, with no subgroup reweighting or paired-market
    intersection. Zero-dollar executions remain in original support counts.

    Raw additive points are saved even in suppressed cells. Reader-facing points
    are null below the original-execution floor or with zero total weight. Counts
    of contributing execution IDs and unknown-history weight shares are separate
    diagnostics, not proof of complete inventory. Estimates are descriptive;
    uncertainty is explicitly not estimated here rather than duplicating the
    project's clustered-variance engine.

    The caller owns connection lifetime and immutable artifact publication. This
    function never replaces an existing relation or scans data into pandas.
    """
    if isinstance(support_floor, bool) or not isinstance(support_floor, int) or support_floor<1:
        raise ValueError("support_floor must be a positive integer")
    _identifier(prefix)
    validate_executions(con, relation)
    names = {key: prefix+"_"+key for key in ("weighted", "profile", "tails")}
    for name in names.values():
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [name]).fetchone()[0] \
                or con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?", [name]).fetchone()[0]:
            raise ValueError(f"Refusing to replace existing contribution relation: {name}")
    source = _identifier(relation)
    weighted, profile, tails = (_identifier(names[key]) for key in ("weighted", "profile", "tails"))
    con.execute(f"""CREATE TEMP VIEW {weighted} AS
        WITH original AS (SELECT execution_id,market_id,price::DOUBLE price,
            residual::DOUBLE residual,usdc::DOUBLE usdc,exit_fraction::DOUBLE exit_fraction,
            hedge_fraction::DOUBLE hedge_fraction,
            unknown_history_fraction::DOUBLE unknown_history_fraction,
            least(floor(price::DOUBLE*10)::INTEGER+1,10) price_bin FROM {source}),
        totals AS (SELECT *,sum(usdc) OVER(PARTITION BY market_id,price_bin) market_bin_usdc
                   FROM original)
        SELECT t.*,w.weighting,
            CASE w.weighting WHEN 'fill' THEN 1.0 WHEN 'dollar' THEN usdc
                 ELSE coalesce(usdc/nullif(market_bin_usdc,0),0) END::DOUBLE weight
        FROM totals t CROSS JOIN (VALUES ('fill'),('dollar'),('equal_market')) w(weighting)""")
    con.execute(f"""CREATE TEMP TABLE {profile} AS
        WITH moments AS (SELECT weighting,price_bin,count(*)::BIGINT n_executions,
            count(DISTINCT market_id)::BIGINT n_markets,
            count(*) FILTER(WHERE weight>0)::BIGINT n_positive_weight_executions,
            count(DISTINCT market_id) FILTER(WHERE weight>0)::BIGINT n_positive_weight_markets,
            count(*) FILTER(WHERE exit_fraction>0)::BIGINT exit_contributing_executions,
            count(*) FILTER(WHERE hedge_fraction>0)::BIGINT hedge_contributing_executions,
            count(*) FILTER(WHERE unknown_history_fraction>0)::BIGINT unknown_history_executions,
            sum(usdc)::DOUBLE dollars,sum(weight)::DOUBLE weight_total,
            sum(weight*exit_fraction)::DOUBLE exit_weight,
            sum(weight*hedge_fraction)::DOUBLE hedge_weight,
            sum(weight*(1-exit_fraction-hedge_fraction))::DOUBLE remaining_weight,
            sum(weight*unknown_history_fraction)::DOUBLE unknown_history_weight,
            sum(weight*residual)/nullif(sum(weight),0)::DOUBLE calibration_raw,
            sum(weight*exit_fraction*residual)/nullif(sum(weight),0)::DOUBLE exit_contribution_raw,
            sum(weight*hedge_fraction*residual)/nullif(sum(weight),0)::DOUBLE hedge_contribution_raw,
            sum(weight*(1-exit_fraction-hedge_fraction)*residual)/nullif(sum(weight),0)::DOUBLE remaining_contribution_raw
            FROM {weighted} GROUP BY weighting,price_bin),
        grid AS (SELECT w.weighting,b.price_bin::INTEGER price_bin
                 FROM (VALUES ('fill'),('dollar'),('equal_market')) w(weighting)
                 CROSS JOIN range(1,11) b(price_bin)),
        joined AS (SELECT g.*,
            coalesce(m.n_executions,0)::BIGINT n_executions,
            coalesce(m.n_markets,0)::BIGINT n_markets,
            coalesce(m.n_positive_weight_executions,0)::BIGINT n_positive_weight_executions,
            coalesce(m.n_positive_weight_markets,0)::BIGINT n_positive_weight_markets,
            coalesce(m.exit_contributing_executions,0)::BIGINT exit_contributing_executions,
            coalesce(m.hedge_contributing_executions,0)::BIGINT hedge_contributing_executions,
            coalesce(m.unknown_history_executions,0)::BIGINT unknown_history_executions,
            coalesce(m.dollars,0)::DOUBLE dollars,coalesce(m.weight_total,0)::DOUBLE weight_total,
            coalesce(m.exit_weight,0)::DOUBLE exit_weight,coalesce(m.hedge_weight,0)::DOUBLE hedge_weight,
            coalesce(m.remaining_weight,0)::DOUBLE remaining_weight,
            coalesce(m.unknown_history_weight,0)::DOUBLE unknown_history_weight,
            m.calibration_raw,m.exit_contribution_raw,m.hedge_contribution_raw,m.remaining_contribution_raw
            FROM grid g LEFT JOIN moments m USING(weighting,price_bin)),
        qualified AS (SELECT *,n_executions<{support_floor} OR weight_total<=0 suppressed,
            CASE WHEN n_executions<{support_floor} THEN 'insufficient_original_support'
                 WHEN weight_total<=0 THEN 'no_positive_weight' ELSE 'supported_descriptive' END support_status
            FROM joined)
        SELECT *,unknown_history_weight/nullif(weight_total,0)::DOUBLE unknown_history_weight_share,
            exit_weight/nullif(weight_total,0)::DOUBLE exit_weight_share,
            hedge_weight/nullif(weight_total,0)::DOUBLE hedge_weight_share,
            CASE WHEN NOT suppressed THEN calibration_raw END calibration,
            CASE WHEN NOT suppressed THEN exit_contribution_raw END exit_contribution,
            CASE WHEN NOT suppressed THEN hedge_contribution_raw END hedge_contribution,
            CASE WHEN NOT suppressed THEN remaining_contribution_raw END remaining_contribution,
            'not_estimated_descriptive'::VARCHAR uncertainty_status
        FROM qualified ORDER BY weighting,price_bin""")
    if con.execute(f"""SELECT count(*) FROM {profile} WHERE weight_total>0 AND
        (abs(calibration_raw-exit_contribution_raw-hedge_contribution_raw-remaining_contribution_raw)>1e-12
         OR abs(weight_total-exit_weight-hedge_weight-remaining_weight)>1e-10*greatest(weight_total,1))""").fetchone()[0]:
        raise ValueError("Fixed-denominator contribution reconciliation failed")
    con.execute(f"""CREATE TEMP TABLE {tails} AS SELECT a.weighting,
        a.n_executions d1_n_executions,b.n_executions d10_n_executions,
        a.exit_contributing_executions d1_exit_contributing_executions,
        b.exit_contributing_executions d10_exit_contributing_executions,
        a.hedge_contributing_executions d1_hedge_contributing_executions,
        b.hedge_contributing_executions d10_hedge_contributing_executions,
        a.unknown_history_executions d1_unknown_history_executions,
        b.unknown_history_executions d10_unknown_history_executions,
        a.unknown_history_weight_share d1_unknown_history_weight_share,
        b.unknown_history_weight_share d10_unknown_history_weight_share,
        a.weight_total d1_weight_total,b.weight_total d10_weight_total,
        a.suppressed d1_suppressed,b.suppressed d10_suppressed,
        a.suppressed OR b.suppressed suppressed,
        CASE WHEN a.suppressed OR b.suppressed THEN 'insufficient_tail_support_or_weight'
             ELSE 'supported_descriptive' END support_status,
        b.calibration_raw-a.calibration_raw spread_raw,
        b.exit_contribution_raw-a.exit_contribution_raw exit_spread_contribution_raw,
        b.hedge_contribution_raw-a.hedge_contribution_raw hedge_spread_contribution_raw,
        b.remaining_contribution_raw-a.remaining_contribution_raw remaining_spread_contribution_raw,
        b.calibration-a.calibration spread,
        b.exit_contribution-a.exit_contribution exit_spread_contribution,
        b.hedge_contribution-a.hedge_contribution hedge_spread_contribution,
        b.remaining_contribution-a.remaining_contribution remaining_spread_contribution,
        'not_estimated_descriptive'::VARCHAR uncertainty_status
        FROM {profile} a JOIN {profile} b USING(weighting)
        WHERE a.price_bin=1 AND b.price_bin=10 ORDER BY a.weighting""")
    return names


def create_sport_contribution_summaries(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    *,
    prefix: str = "sports_profit_taking",
    support_floor: int = SUPPORT_FLOOR,
) -> dict[str, str]:
    """Summarize one verified grain across the frozen sport/sample/window grid.

    All negatives are pregame. Live is inclusive [0,1]; thirds and the first
    three terminal windows are left-closed/right-open, the final window includes
    1. The final120 seconds additionally requires live time. Filters act only on
    focal BUY observations, never the source history. Counts use this caller's
    actual observation grain. Equal-market normalization is within the original
    sport/sample/window/market/bin population. Component gross quantity means
    q times the mechanism fraction, not physical net hedge inventory.
    """
    if isinstance(support_floor, bool) or not isinstance(support_floor, int) or support_floor < 1:
        raise ValueError("support_floor must be a positive integer")
    _identifier(prefix)
    validate_executions(con, relation)
    require_columns(con, relation, (
        "sport", "realized_time", "seconds_to_end", "is_nonhuman", "gross_quantity_micro",
    ), "Timed sports BUY contributions")
    source = _identifier(relation)
    sports_sql = ",".join(f"('{sport}')" for sport in SPORTS)
    samples_sql = ",".join(f"('{sample}')" for sample in SAMPLES)
    windows_sql = ",".join(f"('{window}')" for window in WINDOWS)
    if con.execute(f"""SELECT count(*) FROM {source} WHERE sport IS NULL
        OR sport NOT IN ({','.join(repr(s) for s in SPORTS)})
        OR realized_time IS NULL OR NOT isfinite(realized_time)
        OR seconds_to_end IS NULL OR NOT isfinite(seconds_to_end)
        OR is_nonhuman IS NULL OR gross_quantity_micro IS NULL OR gross_quantity_micro<=0""").fetchone()[0]:
        raise ValueError("Invalid sport, clock, actor flag or gross BUY quantity")
    names = {key: prefix+"_"+key for key in ("focal", "weighted", "profile", "tails", "late_delta")}
    for name in names.values():
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [name]).fetchone()[0] \
                or con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?", [name]).fetchone()[0]:
            raise ValueError(f"Refusing to replace existing sports contribution relation: {name}")
    focal,weighted,profile,tails,late_delta = (_identifier(names[key]) for key in names)
    con.execute(f"""CREATE TEMP VIEW {focal} AS SELECT b.*,s.sample,t.window,
        least(floor(b.price*10)::INTEGER+1,10) price_bin
        FROM {source} b CROSS JOIN (VALUES {samples_sql}) s(sample)
        CROSS JOIN (VALUES {windows_sql}) t("window")
        WHERE CASE s.sample WHEN 'filtered' THEN price>.01 AND price<.99 AND NOT is_nonhuman
            WHEN 'interior_all_actors' THEN price>.01 AND price<.99 ELSE price>0 AND price<1 END
        AND CASE t.window WHEN 'pregame' THEN realized_time<0
            WHEN 'live' THEN realized_time BETWEEN 0 AND 1
            WHEN 'live_first_third' THEN realized_time>=0 AND realized_time<1.0/3
            WHEN 'live_middle_third' THEN realized_time>=1.0/3 AND realized_time<2.0/3
            WHEN 'live_final_third' THEN realized_time>=2.0/3 AND realized_time<=1
            WHEN 'live_80_90' THEN realized_time>=.80 AND realized_time<.90
            WHEN 'live_90_95' THEN realized_time>=.90 AND realized_time<.95
            WHEN 'live_95_99' THEN realized_time>=.95 AND realized_time<.99
            WHEN 'live_99_100' THEN realized_time>=.99 AND realized_time<=1
            ELSE realized_time BETWEEN 0 AND 1 AND seconds_to_end BETWEEN 0 AND 120 END""")
    con.execute(f"""CREATE TEMP VIEW {weighted} AS WITH totals AS (
        SELECT *,sum(usdc) OVER(PARTITION BY sport,sample,"window",market_id,price_bin) market_bin_usdc FROM {focal})
        SELECT t.*,w.weighting,CASE w.weighting WHEN 'fill' THEN 1.0 WHEN 'dollar' THEN usdc
            ELSE coalesce(usdc/nullif(market_bin_usdc,0),0) END::DOUBLE weight
        FROM totals t CROSS JOIN (VALUES ('fill'),('dollar'),('equal_market')) w(weighting)""")
    con.execute(f"""CREATE TEMP TABLE {profile} AS WITH moments AS (
        SELECT sport,sample,"window",weighting,price_bin,count(*)::BIGINT n_executions,
            count(DISTINCT market_id)::BIGINT n_markets,
            count(*) FILTER(WHERE weight>0)::BIGINT n_positive_weight_executions,
            count(DISTINCT market_id) FILTER(WHERE weight>0)::BIGINT n_positive_weight_markets,
            count(*) FILTER(WHERE exit_fraction>0)::BIGINT exit_contributing_executions,
            count(*) FILTER(WHERE hedge_fraction>0)::BIGINT hedge_contributing_executions,
            count(*) FILTER(WHERE unknown_history_fraction>0)::BIGINT unknown_history_executions,
            sum(gross_quantity_micro)::HUGEINT gross_quantity_micro,
            sum(gross_quantity_micro*exit_fraction)::DOUBLE allocated_exit_gross_quantity_micro,
            sum(gross_quantity_micro*hedge_fraction)::DOUBLE allocated_hedge_gross_quantity_micro,
            sum(gross_quantity_micro*(1-exit_fraction-hedge_fraction))::DOUBLE allocated_remaining_gross_quantity_micro,
            sum(gross_quantity_micro*unknown_history_fraction)::DOUBLE unmatched_history_gross_quantity_micro,
            sum(usdc)::DOUBLE dollars,sum(weight)::DOUBLE weight_total,
            sum(weight*exit_fraction)::DOUBLE exit_weight,sum(weight*hedge_fraction)::DOUBLE hedge_weight,
            sum(weight*(1-exit_fraction-hedge_fraction))::DOUBLE remaining_weight,
            sum(weight*unknown_history_fraction)::DOUBLE unknown_history_weight,
            sum(weight*residual)/nullif(sum(weight),0)::DOUBLE calibration_raw,
            sum(weight*exit_fraction*residual)/nullif(sum(weight),0)::DOUBLE exit_contribution_raw,
            sum(weight*hedge_fraction*residual)/nullif(sum(weight),0)::DOUBLE hedge_contribution_raw,
            sum(weight*(1-exit_fraction-hedge_fraction)*residual)/nullif(sum(weight),0)::DOUBLE remaining_contribution_raw
        FROM {weighted} GROUP BY ALL),
        grid AS (SELECT s.sport,p.sample,t.window,w.weighting,b.price_bin::INTEGER price_bin
            FROM (VALUES {sports_sql}) s(sport) CROSS JOIN (VALUES {samples_sql}) p(sample)
            CROSS JOIN (VALUES {windows_sql}) t("window")
            CROSS JOIN (VALUES ('fill'),('dollar'),('equal_market')) w(weighting) CROSS JOIN range(1,11) b(price_bin)),
        joined AS (SELECT g.*,coalesce(m.n_executions,0)::BIGINT n_executions,
            coalesce(m.n_markets,0)::BIGINT n_markets,
            coalesce(m.n_positive_weight_executions,0)::BIGINT n_positive_weight_executions,
            coalesce(m.n_positive_weight_markets,0)::BIGINT n_positive_weight_markets,
            coalesce(m.exit_contributing_executions,0)::BIGINT exit_contributing_executions,
            coalesce(m.hedge_contributing_executions,0)::BIGINT hedge_contributing_executions,
            coalesce(m.unknown_history_executions,0)::BIGINT unknown_history_executions,
            coalesce(m.gross_quantity_micro,0)::HUGEINT gross_quantity_micro,
            coalesce(m.allocated_exit_gross_quantity_micro,0)::DOUBLE allocated_exit_gross_quantity_micro,
            coalesce(m.allocated_hedge_gross_quantity_micro,0)::DOUBLE allocated_hedge_gross_quantity_micro,
            coalesce(m.allocated_remaining_gross_quantity_micro,0)::DOUBLE allocated_remaining_gross_quantity_micro,
            coalesce(m.unmatched_history_gross_quantity_micro,0)::DOUBLE unmatched_history_gross_quantity_micro,
            coalesce(m.dollars,0)::DOUBLE dollars,coalesce(m.weight_total,0)::DOUBLE weight_total,
            coalesce(m.exit_weight,0)::DOUBLE exit_weight,coalesce(m.hedge_weight,0)::DOUBLE hedge_weight,
            coalesce(m.remaining_weight,0)::DOUBLE remaining_weight,
            coalesce(m.unknown_history_weight,0)::DOUBLE unknown_history_weight,
            m.calibration_raw,m.exit_contribution_raw,m.hedge_contribution_raw,m.remaining_contribution_raw
        FROM grid g LEFT JOIN moments m USING(sport,sample,"window",weighting,price_bin)),
        qualified AS (SELECT *,n_executions<{support_floor} OR weight_total<=0 suppressed,
            CASE WHEN n_executions<{support_floor} THEN 'insufficient_original_support'
                WHEN weight_total<=0 THEN 'no_positive_weight' ELSE 'supported_descriptive' END support_status FROM joined)
        SELECT *,exit_weight/nullif(weight_total,0)::DOUBLE exit_weight_share,
            hedge_weight/nullif(weight_total,0)::DOUBLE hedge_weight_share,
            unknown_history_weight/nullif(weight_total,0)::DOUBLE unknown_history_weight_share,
            CASE WHEN NOT suppressed THEN calibration_raw END calibration,
            CASE WHEN NOT suppressed THEN exit_contribution_raw END exit_contribution,
            CASE WHEN NOT suppressed THEN hedge_contribution_raw END hedge_contribution,
            CASE WHEN NOT suppressed THEN remaining_contribution_raw END remaining_contribution,
            'not_estimated_descriptive'::VARCHAR uncertainty_status FROM qualified
        ORDER BY sport,sample,"window",weighting,price_bin""")
    if con.execute(f"""SELECT count(*) FROM {profile} WHERE weight_total>0 AND
        (abs(calibration_raw-exit_contribution_raw-hedge_contribution_raw-remaining_contribution_raw)>1e-12
        OR abs(weight_total-exit_weight-hedge_weight-remaining_weight)>1e-10*greatest(weight_total,1)
        OR abs(gross_quantity_micro-allocated_exit_gross_quantity_micro-allocated_hedge_gross_quantity_micro-
            allocated_remaining_gross_quantity_micro)>1e-10*greatest(gross_quantity_micro,1))""").fetchone()[0]:
        raise ValueError("Grouped fixed-denominator or allocated gross-quantity reconciliation failed")
    con.execute(f"""CREATE TEMP TABLE {tails} AS SELECT a.sport,a.sample,a.window,a.weighting,
        a.n_executions d1_n_executions,b.n_executions d10_n_executions,
        a.n_markets d1_n_markets,b.n_markets d10_n_markets,
        a.exit_contributing_executions d1_exit_contributing_executions,b.exit_contributing_executions d10_exit_contributing_executions,
        a.hedge_contributing_executions d1_hedge_contributing_executions,b.hedge_contributing_executions d10_hedge_contributing_executions,
        a.unknown_history_executions d1_unknown_history_executions,b.unknown_history_executions d10_unknown_history_executions,
        a.unknown_history_weight_share d1_unknown_history_weight_share,b.unknown_history_weight_share d10_unknown_history_weight_share,
        a.weight_total d1_weight_total,b.weight_total d10_weight_total,
        a.suppressed d1_suppressed,b.suppressed d10_suppressed,a.suppressed OR b.suppressed suppressed,
        CASE WHEN a.suppressed OR b.suppressed THEN 'insufficient_tail_support_or_weight' ELSE 'supported_descriptive' END support_status,
        b.calibration_raw-a.calibration_raw spread_raw,
        b.exit_contribution_raw-a.exit_contribution_raw exit_spread_contribution_raw,
        b.hedge_contribution_raw-a.hedge_contribution_raw hedge_spread_contribution_raw,
        b.remaining_contribution_raw-a.remaining_contribution_raw remaining_spread_contribution_raw,
        b.calibration-a.calibration spread,b.exit_contribution-a.exit_contribution exit_spread_contribution,
        b.hedge_contribution-a.hedge_contribution hedge_spread_contribution,
        b.remaining_contribution-a.remaining_contribution remaining_spread_contribution,
        'not_estimated_descriptive'::VARCHAR uncertainty_status
        FROM {profile} a JOIN {profile} b USING(sport,sample,"window",weighting)
        WHERE a.price_bin=1 AND b.price_bin=10 ORDER BY a.sport,a.sample,a.window,a.weighting""")
    con.execute(f"""CREATE TEMP TABLE {late_delta} AS WITH differences AS (
        SELECT a.sport,a.sample,a.weighting,'live_99_100_minus_live_95_99'::VARCHAR contrast,
            a.d1_n_executions previous_d1_n_executions,a.d10_n_executions previous_d10_n_executions,
            b.d1_n_executions final_d1_n_executions,b.d10_n_executions final_d10_n_executions,
            a.suppressed OR b.suppressed suppressed,
            b.spread_raw-a.spread_raw spread_delta_raw,
            b.exit_spread_contribution_raw-a.exit_spread_contribution_raw exit_spread_delta_raw,
            b.hedge_spread_contribution_raw-a.hedge_spread_contribution_raw hedge_spread_delta_raw,
            b.remaining_spread_contribution_raw-a.remaining_spread_contribution_raw remaining_spread_delta_raw
        FROM {tails} a JOIN {tails} b USING(sport,sample,weighting)
        WHERE a.window='live_95_99' AND b.window='live_99_100')
        SELECT *,CASE WHEN suppressed THEN 'insufficient_four_tail_support_or_weight' ELSE 'supported_descriptive' END support_status,
            CASE WHEN NOT suppressed THEN spread_delta_raw END spread_delta,
            CASE WHEN NOT suppressed THEN exit_spread_delta_raw END exit_spread_delta,
            CASE WHEN NOT suppressed THEN hedge_spread_delta_raw END hedge_spread_delta,
            CASE WHEN NOT suppressed THEN remaining_spread_delta_raw END remaining_spread_delta,
            CASE WHEN NOT suppressed THEN 100*spread_delta_raw END spread_delta_pp,
            CASE WHEN NOT suppressed THEN 100*exit_spread_delta_raw END exit_spread_delta_pp,
            CASE WHEN NOT suppressed THEN 100*hedge_spread_delta_raw END hedge_spread_delta_pp,
            CASE WHEN NOT suppressed THEN 100*remaining_spread_delta_raw END remaining_spread_delta_pp,
            'not_estimated_descriptive'::VARCHAR uncertainty_status FROM differences ORDER BY sport,sample,weighting""")
    return names
