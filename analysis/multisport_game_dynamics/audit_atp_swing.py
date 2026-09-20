"""Audit the unusually large ATP calibration swing over normalized match time.

The production ATP clock uses an ESPN scheduled start plus an independently
matched completed-match duration.  This diagnostic measures the resulting
clock-alignment evidence and recomputes the D10-minus-D1 trajectory under
simple clock shifts, equal-event weighting, and concentration checks.
"""
from __future__ import annotations

import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
    write_parquet,
)


SHIFTS_MINUTES = (0, 30, 60, 90, 120)
SAMPLES = (
    ("filtered_trades", "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"),
    ("interior_all_buyers", "price>0.01 AND price<0.99"),
    ("all_trades", "price>0 AND price<1"),
)
SEGMENTS = (
    ("pregame_6h", -21600.0, 0.0, "offset_from_start>=-21600 AND offset_from_start<0"),
    ("synthetic_live", 0.0, None, "offset_from_start>=0 AND seconds_after_end<=0"),
    ("post_0_30m", 0.0, 1800.0, "seconds_after_end>0 AND seconds_after_end<=1800"),
    ("post_30_60m", 1800.0, 3600.0, "seconds_after_end>1800 AND seconds_after_end<=3600"),
    ("post_60_120m", 3600.0, 7200.0, "seconds_after_end>3600 AND seconds_after_end<=7200"),
    ("post_120_360m", 7200.0, 21600.0, "seconds_after_end>7200 AND seconds_after_end<=21600"),
)

CLOCK_SEGMENT_SCHEMA = (
    ("sample", "VARCHAR"), ("segment", "VARCHAR"),
    ("low_seconds", "DOUBLE"), ("high_seconds", "DOUBLE"),
    ("tail", "VARCHAR"), ("n_fills", "BIGINT"),
    ("n_events", "BIGINT"), ("n_wallets", "BIGINT"),
    ("dollars", "DOUBLE"), ("mean_price", "DOUBLE"),
    ("win_rate", "DOUBLE"), ("mean_calibration", "DOUBLE"),
    ("central_10_90_share", "DOUBLE"),
    ("terminal_consistent_share", "DOUBLE"),
)

EVENT_CLOCK_SCHEMA = (
    ("event_slug", "VARCHAR"), ("market_date", "DATE"),
    ("scheduled_start_utc", "TIMESTAMP WITH TIME ZONE"),
    ("synthetic_end_utc", "TIMESTAMP WITH TIME ZONE"),
    ("duration_minutes", "DOUBLE"), ("n_filtered", "BIGINT"),
    ("n_pregame_6h", "BIGINT"), ("n_synthetic_live", "BIGINT"),
    ("n_post_6h", "BIGINT"), ("n_post_central_10_90", "BIGINT"),
    ("n_post_central_20_80", "BIGINT"),
    ("post_terminal_consistent_share", "DOUBLE"),
    ("last_post_central_seconds", "DOUBLE"),
    ("last_post_central_20_80_seconds", "DOUBLE"),
    ("clock_clean_30m", "BOOLEAN"),
)

CLOCK_QUALITY_SCHEMA = (
    ("event_count", "BIGINT"), ("events_with_post_6h", "BIGINT"),
    ("events_with_post_central_10_90", "BIGINT"),
    ("events_with_post_central_20_80", "BIGINT"),
    ("events_central_after_30m", "BIGINT"),
    ("events_central_after_60m", "BIGINT"),
    ("events_central_after_120m", "BIGINT"),
    ("share_events_central_after_30m", "DOUBLE"),
    ("share_events_central_after_60m", "DOUBLE"),
    ("share_events_central_after_120m", "DOUBLE"),
    ("median_last_post_central_seconds", "DOUBLE"),
    ("p90_last_post_central_seconds", "DOUBLE"),
)

MARKET_SCOPE_SCHEMA = (
    ("accepted_market_count", "BIGINT"),
    ("accepted_event_count", "BIGINT"),
    ("set_term_count", "BIGINT"),
    ("game_term_count", "BIGINT"),
    ("round_term_count", "BIGINT"),
    ("ordinal_term_count", "BIGINT"),
)

TAIL_TIME_SCHEMA = (
    ("sample", "VARCHAR"), ("clock_shift_minutes", "INTEGER"),
    ("event_filter", "VARCHAR"), ("time_bin", "INTEGER"),
    ("time_low", "DOUBLE"), ("time_high", "DOUBLE"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
    ("d1_mean_calibration", "DOUBLE"),
    ("d10_mean_calibration", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"),
    ("paired_event_count", "BIGINT"),
    ("equal_event_spread", "DOUBLE"),
)

SLOPE_SCHEMA = (
    ("sample", "VARCHAR"), ("clock_shift_minutes", "INTEGER"),
    ("event_filter", "VARCHAR"), ("n_fills", "BIGINT"),
    ("n_events", "BIGINT"), ("d1_time_slope", "DOUBLE"),
    ("d10_time_slope", "DOUBLE"),
    ("tail_spread_time_slope", "DOUBLE"),
)

CONCENTRATION_SCHEMA = (
    ("time_bin", "INTEGER"), ("tail", "VARCHAR"),
    ("n_fills", "BIGINT"), ("n_events", "BIGINT"),
    ("n_wallets", "BIGINT"), ("event_fill_hhi", "DOUBLE"),
    ("top_event_fill_share", "DOUBLE"),
    ("top10_event_fill_share", "DOUBLE"),
    ("wallet_fill_hhi", "DOUBLE"),
    ("top_wallet_fill_share", "DOUBLE"),
    ("top10_wallet_fill_share", "DOUBLE"),
)

CALENDAR_SCHEMA = (
    ("year", "INTEGER"), ("time_half", "VARCHAR"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
    ("d1_mean_calibration", "DOUBLE"),
    ("d10_mean_calibration", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"),
)

DECILE_SCHEMA = (
    ("time_third", "INTEGER"), ("price_decile", "INTEGER"),
    ("n_fills", "BIGINT"), ("n_events", "BIGINT"),
    ("mean_price", "DOUBLE"), ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"),
)

EVENT_CONTRIBUTION_SCHEMA = (
    ("time_bin", "INTEGER"), ("absolute_rank", "INTEGER"),
    ("event_slug", "VARCHAR"), ("market_date", "DATE"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_mean_calibration", "DOUBLE"),
    ("d10_mean_calibration", "DOUBLE"),
    ("spread_contribution", "DOUBLE"),
)


def _dict_rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _create_base(con: duckdb.DuckDBPyConnection, exact_buys: Path) -> None:
    con.execute(
        f"""
        CREATE TEMP TABLE atp AS
        SELECT event_slug::VARCHAR event_slug,market_date::DATE market_date,
               market_id::VARCHAR market_id,"timestamp"::BIGINT trade_timestamp,
               proxyWallet::VARCHAR proxyWallet,
               buyer_is_flagged_nonhuman::BOOLEAN buyer_is_flagged_nonhuman,
               price::DOUBLE price,won::DOUBLE won,usdc::DOUBLE usdc,
               actual_start_utc,actual_end_utc,
               (won::DOUBLE-price)::DOUBLE calibration_error,
               ("timestamp"-epoch(actual_start_utc))::DOUBLE offset_from_start,
               ("timestamp"-epoch(actual_end_utc))::DOUBLE seconds_after_end,
               (epoch(actual_end_utc)-epoch(actual_start_utc))::DOUBLE duration_seconds,
               ("timestamp"-epoch(actual_start_utc))/
                 (epoch(actual_end_utc)-epoch(actual_start_utc))::DOUBLE original_time,
               least(floor(price*10)::INTEGER+1,10) price_decile,
               timezone('UTC',to_timestamp("timestamp"))::DATE trade_day
        FROM read_parquet('{quoted(exact_buys)}')
        WHERE sport='atp' AND price>0 AND price<1
        """
    )
    bad = con.execute(
        """
        SELECT count(*) FROM atp
        WHERE event_slug IS NULL OR trim(event_slug)='' OR market_id IS NULL
           OR trade_timestamp IS NULL OR proxyWallet IS NULL
           OR won NOT IN (0,1) OR usdc<=0 OR NOT isfinite(usdc)
           OR actual_end_utc<=actual_start_utc OR duration_seconds<=0
           OR abs(calibration_error-(won-price))>1e-12
        """
    ).fetchone()[0]
    if bad:
        raise ValueError(f"Invalid ATP exact rows: {bad}")


def _validate_timing(
    con: duckdb.DuckDBPyConnection, event_timing: Path
) -> None:
    row = con.execute(
        f"""
        WITH expected AS (
          SELECT event_slug,actual_start_utc,actual_end_utc
          FROM read_parquet('{quoted(event_timing)}') WHERE sport='atp'
        ), observed AS (
          SELECT event_slug,min(actual_start_utc) actual_start_utc,
                 min(actual_end_utc) actual_end_utc
          FROM atp GROUP BY event_slug
        )
        SELECT (SELECT count(*) FROM expected)::BIGINT expected_events,
               (SELECT count(*) FROM observed)::BIGINT observed_events,
               count(*) FILTER(WHERE e.event_slug IS NULL OR o.event_slug IS NULL)::BIGINT missing,
               count(*) FILTER(WHERE e.event_slug IS NOT NULL AND o.event_slug IS NOT NULL
                                 AND (e.actual_start_utc<>o.actual_start_utc
                                      OR e.actual_end_utc<>o.actual_end_utc))::BIGINT mismatched
        FROM expected e FULL OUTER JOIN observed o USING(event_slug)
        """
    ).fetchone()
    if tuple(row) != (1355, 1355, 0, 0):
        raise ValueError(f"ATP timing lineage mismatch: {tuple(row)}")


def _market_scope_rows(
    con: duckdb.DuckDBPyConnection,
    candidate_markets: Path,
    event_timing: Path,
) -> list[tuple[Any, ...]]:
    text = "lower(coalesce(m.question,'')||' '||coalesce(m.group_item_title,''))"
    return [con.execute(
        f"""
        SELECT count(*)::BIGINT accepted_market_count,
               count(DISTINCT m.event_slug)::BIGINT accepted_event_count,
               count(*) FILTER(WHERE regexp_matches({text},
                 '(^|[^a-z])(set|sets)([^a-z]|$)'))::BIGINT set_term_count,
               count(*) FILTER(WHERE regexp_matches({text},
                 '(^|[^a-z])(game|games)([^a-z]|$)'))::BIGINT game_term_count,
               count(*) FILTER(WHERE regexp_matches({text},
                 '(^|[^a-z])round([^a-z]|$)'))::BIGINT round_term_count,
               count(*) FILTER(WHERE regexp_matches({text},
                 '(^|[^a-z])(first|second|third|fourth|fifth)([^a-z]|$)'))::BIGINT ordinal_term_count
        FROM read_parquet('{quoted(candidate_markets)}') m
        JOIN read_parquet('{quoted(event_timing)}') t USING(sport,event_slug)
        WHERE m.sport='atp'
        """
    ).fetchone()]


def _build_event_clock(con: duckdb.DuckDBPyConnection) -> None:
    filtered = "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"
    con.execute(
        f"""
        CREATE TEMP TABLE event_clock AS
        SELECT event_slug,min(market_date)::DATE market_date,
               min(actual_start_utc) scheduled_start_utc,
               min(actual_end_utc) synthetic_end_utc,
               min(duration_seconds)/60.0 duration_minutes,
               count(*)::BIGINT n_filtered,
               count(*) FILTER(WHERE offset_from_start>=-21600 AND offset_from_start<0)::BIGINT n_pregame_6h,
               count(*) FILTER(WHERE offset_from_start>=0 AND seconds_after_end<=0)::BIGINT n_synthetic_live,
               count(*) FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600)::BIGINT n_post_6h,
               count(*) FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600
                                AND price>0.10 AND price<0.90)::BIGINT n_post_central_10_90,
               count(*) FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600
                                AND price>0.20 AND price<0.80)::BIGINT n_post_central_20_80,
               avg((CASE WHEN won=1 THEN price>=0.90 ELSE price<=0.10 END)::INTEGER)
                 FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600)::DOUBLE
                 post_terminal_consistent_share,
               max(seconds_after_end) FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600
                                             AND price>0.10 AND price<0.90)::DOUBLE
                 last_post_central_seconds,
               max(seconds_after_end) FILTER(WHERE seconds_after_end>0 AND seconds_after_end<=21600
                                             AND price>0.20 AND price<0.80)::DOUBLE
                 last_post_central_20_80_seconds,
               (coalesce(max(seconds_after_end) FILTER(
                  WHERE seconds_after_end>0 AND seconds_after_end<=21600
                    AND price>0.10 AND price<0.90),0)<=1800)::BOOLEAN clock_clean_30m
        FROM atp WHERE {filtered}
        GROUP BY event_slug
        """
    )


def _clock_segment_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    output: list[tuple[Any, ...]] = []
    for sample, sample_filter in SAMPLES:
        for segment, low, high, condition in SEGMENTS:
            rows = _dict_rows(
                con,
                f"""
                SELECT CASE WHEN price_decile=1 THEN 'D1'
                            WHEN price_decile=10 THEN 'D10' ELSE 'ALL' END tail,
                       count(*)::BIGINT n_fills,
                       count(DISTINCT event_slug)::BIGINT n_events,
                       count(DISTINCT proxyWallet)::BIGINT n_wallets,
                       sum(usdc)::DOUBLE dollars,avg(price)::DOUBLE mean_price,
                       avg(won)::DOUBLE win_rate,
                       avg(calibration_error)::DOUBLE mean_calibration,
                       avg((price>0.10 AND price<0.90)::INTEGER)::DOUBLE central_share,
                       avg((CASE WHEN won=1 THEN price>=0.90 ELSE price<=0.10 END)::INTEGER)::DOUBLE
                         terminal_consistent_share
                FROM atp
                WHERE {sample_filter} AND {condition}
                GROUP BY GROUPING SETS ((),(price_decile))
                HAVING GROUPING(price_decile)=1 OR price_decile IN (1,10)
                ORDER BY tail
                """,
            )
            for row in rows:
                output.append((
                    sample, segment, low, high, row["tail"], int(row["n_fills"]),
                    int(row["n_events"]), int(row["n_wallets"]), float(row["dollars"]),
                    float(row["mean_price"]), float(row["win_rate"]),
                    float(row["mean_calibration"]), float(row["central_share"]),
                    float(row["terminal_consistent_share"]),
                ))
    return output


def _event_clock_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    names = [name for name, _ in EVENT_CLOCK_SCHEMA]
    return [tuple(row[name] for name in names) for row in _dict_rows(
        con, "SELECT * FROM event_clock ORDER BY event_slug"
    )]


def _clock_quality_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    row = con.execute(
        """
        SELECT count(*)::BIGINT event_count,
               count(*) FILTER(WHERE n_post_6h>0)::BIGINT events_with_post_6h,
               count(*) FILTER(WHERE n_post_central_10_90>0)::BIGINT events_with_post_central_10_90,
               count(*) FILTER(WHERE n_post_central_20_80>0)::BIGINT events_with_post_central_20_80,
               count(*) FILTER(WHERE last_post_central_seconds>1800)::BIGINT events_central_after_30m,
               count(*) FILTER(WHERE last_post_central_seconds>3600)::BIGINT events_central_after_60m,
               count(*) FILTER(WHERE last_post_central_seconds>7200)::BIGINT events_central_after_120m,
               avg((last_post_central_seconds>1800)::INTEGER)::DOUBLE share_events_central_after_30m,
               avg((last_post_central_seconds>3600)::INTEGER)::DOUBLE share_events_central_after_60m,
               avg((last_post_central_seconds>7200)::INTEGER)::DOUBLE share_events_central_after_120m,
               median(last_post_central_seconds) FILTER(WHERE last_post_central_seconds IS NOT NULL)::DOUBLE,
               quantile_cont(last_post_central_seconds,0.9)
                 FILTER(WHERE last_post_central_seconds IS NOT NULL)::DOUBLE
        FROM event_clock
        """
    ).fetchone()
    return [tuple(row)]


def _event_filter_sql(event_filter: str) -> str:
    if event_filter == "all_events":
        return "TRUE"
    if event_filter == "clock_clean_30m":
        return "e.clock_clean_30m"
    raise ValueError(event_filter)


def _tail_time_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    output: list[tuple[Any, ...]] = []
    for sample, sample_filter in SAMPLES:
        for shift in SHIFTS_MINUTES:
            time = f"(a.trade_timestamp-(epoch(a.actual_start_utc)+{shift}*60.0))/a.duration_seconds"
            for event_filter in ("all_events", "clock_clean_30m"):
                event_clause = _event_filter_sql(event_filter)
                con.execute("DROP VIEW IF EXISTS selected_tail")
                con.execute(
                    f"""
                    CREATE TEMP VIEW selected_tail AS
                    SELECT a.*,({time})::DOUBLE shifted_time,
                           least(floor(({time})*10)::INTEGER+1,10) time_bin
                    FROM atp a JOIN event_clock e USING(event_slug)
                    WHERE {sample_filter} AND a.price_decile IN (1,10)
                      AND {event_clause} AND ({time})>=0 AND ({time})<=1
                    """
                )
                fill = {
                    int(row["time_bin"]): row
                    for row in _dict_rows(
                        con,
                        """
                        SELECT time_bin,
                          count(*) FILTER(WHERE price_decile=1)::BIGINT d1_n,
                          count(*) FILTER(WHERE price_decile=10)::BIGINT d10_n,
                          count(DISTINCT event_slug) FILTER(WHERE price_decile=1)::BIGINT d1_events,
                          count(DISTINCT event_slug) FILTER(WHERE price_decile=10)::BIGINT d10_events,
                          avg(calibration_error) FILTER(WHERE price_decile=1)::DOUBLE d1_mean,
                          avg(calibration_error) FILTER(WHERE price_decile=10)::DOUBLE d10_mean
                        FROM selected_tail GROUP BY time_bin
                        """,
                    )
                }
                paired = {
                    int(row["time_bin"]): row
                    for row in _dict_rows(
                        con,
                        """
                        WITH event_tail AS (
                          SELECT time_bin,event_slug,price_decile,
                                 avg(calibration_error)::DOUBLE mean_calibration
                          FROM selected_tail GROUP BY 1,2,3
                        ), event_spread AS (
                          SELECT time_bin,event_slug,
                            max(mean_calibration) FILTER(WHERE price_decile=10)-
                            max(mean_calibration) FILTER(WHERE price_decile=1) spread
                          FROM event_tail GROUP BY 1,2
                          HAVING count(DISTINCT price_decile)=2
                        )
                        SELECT time_bin,count(*)::BIGINT paired_event_count,
                               avg(spread)::DOUBLE equal_event_spread
                        FROM event_spread GROUP BY 1
                        """,
                    )
                }
                for time_bin in range(1, 11):
                    row = fill.get(time_bin, {})
                    pair = paired.get(time_bin, {})
                    d1 = row.get("d1_mean")
                    d10 = row.get("d10_mean")
                    output.append((
                        sample, shift, event_filter, time_bin,
                        (time_bin - 1) / 10.0, time_bin / 10.0,
                        int(row.get("d1_n", 0)), int(row.get("d10_n", 0)),
                        int(row.get("d1_events", 0)), int(row.get("d10_events", 0)),
                        None if d1 is None else float(d1),
                        None if d10 is None else float(d10),
                        None if d1 is None or d10 is None else float(d10 - d1),
                        int(pair.get("paired_event_count", 0)),
                        None if pair.get("equal_event_spread") is None
                        else float(pair["equal_event_spread"]),
                    ))
    return output


def _slope_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    output: list[tuple[Any, ...]] = []
    for sample, sample_filter in SAMPLES:
        for shift in SHIFTS_MINUTES:
            time = f"(a.trade_timestamp-(epoch(a.actual_start_utc)+{shift}*60.0))/a.duration_seconds"
            for event_filter in ("all_events", "clock_clean_30m"):
                event_clause = _event_filter_sql(event_filter)
                row = con.execute(
                    f"""
                    SELECT count(*)::BIGINT,count(DISTINCT a.event_slug)::BIGINT,
                           regr_slope(a.calibration_error,{time}) FILTER(WHERE a.price_decile=1)::DOUBLE,
                           regr_slope(a.calibration_error,{time}) FILTER(WHERE a.price_decile=10)::DOUBLE
                    FROM atp a JOIN event_clock e USING(event_slug)
                    WHERE {sample_filter} AND a.price_decile IN (1,10)
                      AND {event_clause} AND ({time})>=0 AND ({time})<=1
                    """
                ).fetchone()
                d1, d10 = float(row[2]), float(row[3])
                output.append((sample, shift, event_filter, int(row[0]), int(row[1]),
                               d1, d10, d10 - d1))
    return output


def _concentration_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    filtered = "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"
    con.execute(
        f"""
        CREATE TEMP VIEW original_tail AS
        SELECT *,least(floor(original_time*10)::INTEGER+1,10) time_bin,
               CASE WHEN price_decile=1 THEN 'D1' ELSE 'D10' END tail
        FROM atp WHERE {filtered} AND price_decile IN (1,10)
          AND original_time>=0 AND original_time<=1
        """
    )
    def concentration(identifier: str) -> dict[tuple[int, str], tuple[float, float, float]]:
        return {
            (int(row["time_bin"]), row["tail"]):
            (float(row["hhi"]), float(row["top1"]), float(row["top10"]))
            for row in _dict_rows(
                con,
                f"""
                WITH grouped AS (
                  SELECT time_bin,tail,{identifier} id,count(*)::DOUBLE n
                  FROM original_tail GROUP BY 1,2,3
                ), ranked AS (
                  SELECT *,sum(n) OVER(PARTITION BY time_bin,tail) total,
                         row_number() OVER(PARTITION BY time_bin,tail ORDER BY n DESC,id) rank
                  FROM grouped
                )
                SELECT time_bin,tail,sum(power(n/total,2))::DOUBLE hhi,
                       max(n/total)::DOUBLE top1,
                       sum(CASE WHEN rank<=10 THEN n ELSE 0 END)/max(total)::DOUBLE top10
                FROM ranked GROUP BY 1,2
                """,
            )
        }
    event = concentration("event_slug")
    wallet = concentration("proxyWallet")
    rows = _dict_rows(
        con,
        """
        SELECT time_bin,tail,count(*)::BIGINT n_fills,
               count(DISTINCT event_slug)::BIGINT n_events,
               count(DISTINCT proxyWallet)::BIGINT n_wallets
        FROM original_tail GROUP BY 1,2 ORDER BY 1,2
        """,
    )
    return [
        (int(row["time_bin"]), row["tail"], int(row["n_fills"]),
         int(row["n_events"]), int(row["n_wallets"]),
         *event[(int(row["time_bin"]), row["tail"])],
         *wallet[(int(row["time_bin"]), row["tail"])])
        for row in rows
    ]


def _calendar_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    filtered = "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"
    rows = _dict_rows(
        con,
        f"""
        SELECT year(market_date)::INTEGER AS "year",
               CASE WHEN original_time<0.5 THEN 'early' ELSE 'late' END time_half,
               count(*) FILTER(WHERE price_decile=1)::BIGINT d1_n,
               count(*) FILTER(WHERE price_decile=10)::BIGINT d10_n,
               count(DISTINCT event_slug) FILTER(WHERE price_decile=1)::BIGINT d1_events,
               count(DISTINCT event_slug) FILTER(WHERE price_decile=10)::BIGINT d10_events,
               avg(calibration_error) FILTER(WHERE price_decile=1)::DOUBLE d1_mean,
               avg(calibration_error) FILTER(WHERE price_decile=10)::DOUBLE d10_mean
        FROM atp WHERE {filtered} AND price_decile IN (1,10)
          AND original_time>=0 AND original_time<=1
        GROUP BY 1,2 ORDER BY 1,2
        """,
    )
    return [
        (int(row["year"]), row["time_half"], int(row["d1_n"]), int(row["d10_n"]),
         int(row["d1_events"]), int(row["d10_events"]), float(row["d1_mean"]),
         float(row["d10_mean"]), float(row["d10_mean"] - row["d1_mean"]))
        for row in rows
    ]


def _decile_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    filtered = "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"
    rows = _dict_rows(
        con,
        f"""
        SELECT least(floor(original_time*3)::INTEGER+1,3) time_third,
               price_decile,count(*)::BIGINT n_fills,
               count(DISTINCT event_slug)::BIGINT n_events,
               avg(price)::DOUBLE mean_price,avg(won)::DOUBLE win_rate,
               avg(calibration_error)::DOUBLE mean_calibration
        FROM atp WHERE {filtered} AND original_time>=0 AND original_time<=1
        GROUP BY 1,2 ORDER BY 1,2
        """,
    )
    names = [name for name, _ in DECILE_SCHEMA]
    return [tuple(row[name] for name in names) for row in rows]


def _event_contribution_rows(
    con: duckdb.DuckDBPyConnection,
) -> list[tuple[Any, ...]]:
    filtered = "price>0.01 AND price<0.99 AND NOT buyer_is_flagged_nonhuman"
    rows = _dict_rows(
        con,
        f"""
        WITH tail_event AS (
          SELECT least(floor(original_time*10)::INTEGER+1,10) time_bin,
                 event_slug,min(market_date)::DATE market_date,price_decile,
                 count(*)::BIGINT n_fills,
                 avg(calibration_error)::DOUBLE mean_calibration
          FROM atp WHERE {filtered} AND price_decile IN (1,10)
            AND original_time>=0 AND original_time<=1
          GROUP BY 1,2,4
        ), totals AS (
          SELECT time_bin,price_decile,sum(n_fills)::DOUBLE total_fills
          FROM tail_event GROUP BY 1,2
        ), contributions AS (
          SELECT x.time_bin,x.event_slug,x.market_date,x.price_decile,x.n_fills,
                 x.mean_calibration,
                 CASE WHEN x.price_decile=10
                      THEN x.mean_calibration*x.n_fills/t.total_fills
                      ELSE -x.mean_calibration*x.n_fills/t.total_fills END contribution
          FROM tail_event x JOIN totals t USING(time_bin,price_decile)
        ), event_rows AS (
          SELECT time_bin,event_slug,min(market_date)::DATE market_date,
                 max(n_fills) FILTER(WHERE price_decile=1)::BIGINT d1_n,
                 max(n_fills) FILTER(WHERE price_decile=10)::BIGINT d10_n,
                 max(mean_calibration) FILTER(WHERE price_decile=1)::DOUBLE d1_mean,
                 max(mean_calibration) FILTER(WHERE price_decile=10)::DOUBLE d10_mean,
                 sum(contribution)::DOUBLE spread_contribution
          FROM contributions GROUP BY 1,2
        ), ranked AS (
          SELECT *,row_number() OVER(
            PARTITION BY time_bin ORDER BY abs(spread_contribution) DESC,event_slug
          )::INTEGER absolute_rank
          FROM event_rows
        )
        SELECT time_bin,absolute_rank,event_slug,market_date,
               coalesce(d1_n,0)::BIGINT d1_n,coalesce(d10_n,0)::BIGINT d10_n,
               d1_mean d1_mean_calibration,d10_mean d10_mean_calibration,
               spread_contribution
        FROM ranked WHERE absolute_rank<=25
        ORDER BY time_bin,absolute_rank
        """,
    )
    names = [name for name, _ in EVENT_CONTRIBUTION_SCHEMA]
    return [tuple(row[name] for name in names) for row in rows]


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    exact = Path(args.exact_buys).expanduser().resolve()
    timing = Path(args.event_timing).expanduser().resolve()
    candidates = Path(args.candidate_markets).expanduser().resolve()
    if not exact.is_file() or not timing.is_file() or not candidates.is_file():
        raise FileNotFoundError(
            [str(p) for p in (exact, timing, candidates) if not p.is_file()]
        )
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET threads=16")
    con.execute("SET memory_limit='200GB'")
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    try:
        _create_base(con, exact)
        _validate_timing(con, timing)
        _build_event_clock(con)
        market_scope = _market_scope_rows(con, candidates, timing)
        clock_segments = _clock_segment_rows(con)
        event_clock = _event_clock_rows(con)
        clock_quality = _clock_quality_rows(con)
        tail_time = _tail_time_rows(con)
        slopes = _slope_rows(con)
        concentration = _concentration_rows(con)
        calendar = _calendar_rows(con)
        deciles = _decile_rows(con)
        event_contributions = _event_contribution_rows(con)
    finally:
        con.close()

    inputs = (exact, timing, candidates)
    with fresh_run(args.run_dir, inputs) as staging:
        specs = (
            ("market_scope_summary.parquet", MARKET_SCOPE_SCHEMA, market_scope,
             ("accepted_event_count",)),
            ("clock_segment_summary.parquet", CLOCK_SEGMENT_SCHEMA, clock_segments,
             ("sample", "segment", "tail")),
            ("event_clock_audit.parquet", EVENT_CLOCK_SCHEMA, event_clock,
             ("event_slug",)),
            ("clock_quality_summary.parquet", CLOCK_QUALITY_SCHEMA, clock_quality,
             ("event_count",)),
            ("tail_time_sensitivity.parquet", TAIL_TIME_SCHEMA, tail_time,
             ("sample", "clock_shift_minutes", "event_filter", "time_bin")),
            ("slope_sensitivity.parquet", SLOPE_SCHEMA, slopes,
             ("sample", "clock_shift_minutes", "event_filter")),
            ("concentration_summary.parquet", CONCENTRATION_SCHEMA, concentration,
             ("time_bin", "tail")),
            ("calendar_summary.parquet", CALENDAR_SCHEMA, calendar,
             ("year", "time_half")),
            ("decile_time_summary.parquet", DECILE_SCHEMA, deciles,
             ("time_third", "price_decile")),
            ("event_contribution_summary.parquet", EVENT_CONTRIBUTION_SCHEMA,
             event_contributions, ("time_bin", "absolute_rank")),
        )
        for name, schema, rows, order in specs:
            write_parquet(staging / name, schema, rows, order)
        outputs = {name: artifact_fingerprint(staging / name)
                   for name, _, _, _ in specs}
        manifest = {
            "schema_version": 1,
            "stage": "atp_normalized_time_swing_audit_v1",
            "status": "complete",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "command": sys.argv,
            "environment": {
                "python": platform.python_version(),
                "duckdb": duckdb.__version__,
                "platform": platform.platform(),
            },
            "code": {"script": fingerprint(Path(__file__))},
            "inputs": {"exact_buys": fingerprint(exact),
                       "event_timing": fingerprint(timing),
                       "candidate_markets": fingerprint(candidates)},
            "definitions": {
                "production_clock": (
                    "ESPN scheduled start plus uniquely matched completed-match duration; "
                    "not an observed ATP start or end"
                ),
                "calibration": "eventual bought-contract outcome minus purchase price",
                "tail_spread": "mean D10 calibration minus mean D1 calibration",
                "clock_clean_30m": (
                    "no filtered 0.10 < price < 0.90 fill more than 30 minutes "
                    "after the synthetic end; diagnostic proxy, not authoritative timing"
                ),
                "shift_sensitivity": (
                    "moves both synthetic start and end later by the stated fixed minutes"
                ),
                "slope_sensitivity": (
                    "difference between separate unadjusted D10 and D1 OLS time slopes; "
                    "point estimate only"
                ),
            },
            "counts": {name.removesuffix(".parquet"): len(rows)
                       for name, _, rows, _ in specs},
            "outputs": outputs,
        }
        write_json(staging / "manifest.json", manifest)
    return manifest


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exact-buys", required=True)
    parser.add_argument("--event-timing", required=True)
    parser.add_argument("--candidate-markets", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main() -> None:
    print(json.dumps(run_audit(parse_args()), sort_keys=True))


if __name__ == "__main__":
    main()
