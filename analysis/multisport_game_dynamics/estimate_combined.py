"""Estimate common bought-contract calibration profiles for nine sport cohorts."""
from __future__ import annotations

import argparse
import itertools
import json
import math
from collections import defaultdict
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

from .contracts import SPORT_CONFIGS


MIN_CELL_N = 500
RETAINED_NEW_SPORTS = ("nhl", "cbb", "atp", "epl", "cfb", "wnba")
REPORT_SPORTS = ("mlb", "nfl", "nba", *RETAINED_NEW_SPORTS)
EXCLUDED_REPORT_SPORTS = ("wta", "ufc")
LEGACY_PHASES = {
    "mlb": (
        ("pregame", "Pregame"),
        ("innings_1_3", "Innings 1--3"),
        ("innings_4_6", "Innings 4--6"),
        ("innings_7_plus", "Innings 7+"),
    ),
    "nfl": (
        ("pregame", "Pregame"),
        ("quarter_1", "Quarter 1"),
        ("quarter_2", "Quarter 2"),
        ("quarter_3", "Quarter 3"),
        ("quarter_4_plus", "Quarter 4 and overtime"),
    ),
    "nba": (
        ("pregame", "Pregame"),
        ("quarter_1", "Quarter 1"),
        ("quarter_2", "Quarter 2"),
        ("quarter_3", "Quarter 3"),
        ("quarter_4_plus", "Quarter 4 and overtime"),
    ),
}

PHASE_PROFILE_SCHEMA = (
    ("sport", "VARCHAR"), ("phase", "VARCHAR"), ("phase_label", "VARCHAR"),
    ("phase_order", "INTEGER"), ("price_decile", "INTEGER"), ("price_bin", "VARCHAR"),
    ("trade_count", "BIGINT"), ("event_count", "BIGINT"), ("dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
    ("mean_price", "DOUBLE"), ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"), ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"), ("calibration_ci95_high", "DOUBLE"),
)
CLOSING_PROFILE_SCHEMA = (
    ("sport", "VARCHAR"), ("close_sample", "VARCHAR"),
    ("price_decile", "INTEGER"), ("price_bin", "VARCHAR"),
    ("close_count", "BIGINT"), ("event_count", "BIGINT"), ("dollars", "DOUBLE"),
    ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
    ("mean_price", "DOUBLE"), ("win_rate", "DOUBLE"),
    ("mean_calibration", "DOUBLE"), ("calibration_se", "DOUBLE"),
    ("calibration_ci95_low", "DOUBLE"), ("calibration_ci95_high", "DOUBLE"),
    ("brier_score", "DOUBLE"),
)
TAIL_SCHEMA = (
    ("analysis_scope", "VARCHAR"), ("sport", "VARCHAR"), ("sample", "VARCHAR"),
    ("phase", "VARCHAR"), ("phase_label", "VARCHAR"), ("phase_order", "INTEGER"),
    ("d1_n", "BIGINT"), ("d10_n", "BIGINT"),
    ("d1_events", "BIGINT"), ("d10_events", "BIGINT"),
    ("d1_mean_calibration", "DOUBLE"), ("d10_mean_calibration", "DOUBLE"),
    ("spread_d10_minus_d1", "DOUBLE"), ("spread_se", "DOUBLE"),
    ("spread_ci95_low", "DOUBLE"), ("spread_ci95_high", "DOUBLE"),
    ("point_pattern", "VARCHAR"), ("suppressed", "BOOLEAN"), ("status", "VARCHAR"),
)


def _price_bin(decile: int) -> str:
    return f"[{(decile-1)/10:.1f},{decile/10:.1f}{']' if decile == 10 else ')'}"


def _rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _phase_dimensions() -> list[tuple[str, str, str, int]]:
    dimensions: list[tuple[str, str, str, int]] = []
    for sport, phases in LEGACY_PHASES.items():
        dimensions.extend((sport, key, label, order) for order, (key, label) in enumerate(phases, 1))
    for sport in RETAINED_NEW_SPORTS:
        config = SPORT_CONFIGS[sport]
        dimensions.extend((sport, phase.key, phase.label, phase.order) for phase in config.phases)
    return dimensions


def _variance_components(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    mean_relation: str,
    keys: tuple[str, ...],
    clusters: tuple[str, ...],
) -> dict[tuple[Any, ...], float]:
    values: dict[tuple[Any, ...], float] = defaultdict(float)
    key_sql = ",".join(f"r.{key}" for key in keys)
    using = ",".join(keys)
    for size in range(1, len(clusters) + 1):
        sign = 1 if size % 2 else -1
        for subset in itertools.combinations(clusters, size):
            cluster_sql = ",".join(f"r.{name}" for name in subset)
            query = f"""
            SELECT {','.join(keys)},sum(score*score)::DOUBLE component
            FROM (
              SELECT {key_sql},{cluster_sql},
                     sum(r.calibration_error-m.mean_calibration)::DOUBLE score
              FROM {relation} r JOIN {mean_relation} m USING({using})
              GROUP BY {key_sql},{cluster_sql}
            ) GROUP BY {','.join(keys)}
            """
            for row in _rows(con, query):
                key = tuple(row[name] for name in keys)
                values[key] += sign * float(row["component"])
    return values


def _tail_variance_components(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    tail_relation: str,
    keys: tuple[str, ...],
    clusters: tuple[str, ...],
) -> dict[tuple[Any, ...], float]:
    values: dict[tuple[Any, ...], float] = defaultdict(float)
    key_sql = ",".join(f"r.{key}" for key in keys)
    using = ",".join(keys)
    for size in range(1, len(clusters) + 1):
        sign = 1 if size % 2 else -1
        for subset in itertools.combinations(clusters, size):
            cluster_sql = ",".join(f"r.{name}" for name in subset)
            query = f"""
            SELECT {','.join(keys)},sum(score*score)::DOUBLE component
            FROM (
              SELECT {key_sql},{cluster_sql},sum(
                CASE WHEN r.price_decile=10
                  THEN (r.calibration_error-t.d10_mean_calibration)/t.d10_n
                  ELSE -(r.calibration_error-t.d1_mean_calibration)/t.d1_n END
              )::DOUBLE score
              FROM {relation} r JOIN {tail_relation} t USING({using})
              WHERE r.price_decile IN (1,10) AND t.d1_n>0 AND t.d10_n>0
              GROUP BY {key_sql},{cluster_sql}
            ) GROUP BY {','.join(keys)}
            """
            for row in _rows(con, query):
                key = tuple(row[name] for name in keys)
                values[key] += sign * float(row["component"])
    return values


def _pattern(d1: float, d10: float) -> str:
    if d1 < 0 < d10:
        return "classic signs"
    if d10 < 0 < d1:
        return "reverse signs"
    if d1 >= 0 and d10 >= 0:
        return "both positive"
    return "both negative"


def estimate_combined(
    new_trade_run: str | Path,
    mlb_phase: str | Path,
    mlb_closes: str | Path,
    nfl_phase: str | Path,
    nfl_closes: str | Path,
    nfl_exact: str | Path,
    nfl_eligible: str | Path,
    nba_phase: str | Path,
    nba_closes: str | Path,
    nba_exact: str | Path,
    nba_eligible: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    new_run = Path(new_trade_run).expanduser().resolve()
    paths = [Path(value).expanduser().resolve() for value in (
        mlb_phase, mlb_closes, nfl_phase, nfl_closes, nfl_exact, nfl_eligible,
        nba_phase, nba_closes, nba_exact, nba_eligible,
    )]
    new_phase = new_run / "phase_trades.parquet"
    new_closes = new_run / "closing_lines.parquet"
    inputs = [new_phase, new_closes, *paths]
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    (mlb_phase_path, mlb_close_path, nfl_phase_path, nfl_close_path, nfl_exact_path,
     nfl_eligible_path, nba_phase_path, nba_close_path, nba_exact_path,
     nba_eligible_path) = paths

    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target, inputs) as staging:
        con = duckdb.connect()
        try:
            retained_new_sql = ",".join(f"'{sport}'" for sport in RETAINED_NEW_SPORTS)
            con.execute("""CREATE TABLE phase_dimension(
                sport VARCHAR,phase VARCHAR,phase_label VARCHAR,phase_order INTEGER)""")
            con.executemany("INSERT INTO phase_dimension VALUES (?,?,?,?)", _phase_dimensions())
            con.execute(f"""
                CREATE TABLE phase_obs AS
                SELECT sport,event_slug::VARCHAR event_id,market_id,market_date,
                       phase,phase_order,price,won::DOUBLE won,calibration_error,usdc,
                       proxyWallet,trade_day
                FROM read_parquet('{quoted(new_phase)}')
                WHERE sport IN ({retained_new_sql})
                UNION ALL
                SELECT 'mlb',p.game_pk::VARCHAR,p.market_id,p.official_date,p.phase,d.phase_order,
                       p.price,p.won::DOUBLE,p.calibration_error,p.usdc,p.proxyWallet,p.trade_day_utc
                FROM read_parquet('{quoted(mlb_phase_path)}') p
                JOIN phase_dimension d ON d.sport='mlb' AND d.phase=p.phase
                WHERE p.analysis_eligible
                UNION ALL
                SELECT p.sport,p.game_id::VARCHAR,p.market_id,p.official_date,p.phase,d.phase_order,
                       p.price,(p.token_id=p.winning_token_id)::DOUBLE,
                       ((p.token_id=p.winning_token_id)::DOUBLE-p.price)::DOUBLE,
                       p.usdc,p.proxyWallet,p.trade_day
                FROM read_parquet('{quoted(nfl_phase_path)}') p
                JOIN phase_dimension d ON d.sport='nfl' AND d.phase=p.phase
                WHERE p.analysis_eligible
                UNION ALL
                SELECT p.sport,p.game_id::VARCHAR,p.market_id,p.official_date,p.phase,d.phase_order,
                       p.price,(p.token_id=p.winning_token_id)::DOUBLE,
                       ((p.token_id=p.winning_token_id)::DOUBLE-p.price)::DOUBLE,
                       p.usdc,p.proxyWallet,p.trade_day
                FROM read_parquet('{quoted(nba_phase_path)}') p
                JOIN phase_dimension d ON d.sport='nba' AND d.phase=p.phase
                WHERE p.analysis_eligible
            """)
            con.execute("ALTER TABLE phase_obs ADD COLUMN price_decile INTEGER")
            con.execute("UPDATE phase_obs SET price_decile=least(floor(price*10)::INTEGER+1,10)")
            invalid_phase = con.execute("""
                SELECT count(*) FROM phase_obs
                WHERE price<=0.01 OR price>=0.99 OR won NOT IN (0,1)
                  OR abs(calibration_error-(won-price))>1e-12
            """).fetchone()[0]
            if invalid_phase:
                raise ValueError(f"Invalid normalized phase observations: {invalid_phase}")

            def legacy_close_sql(sport: str, close_path: Path, exact_path: Path,
                                 eligible_path: Path, definition: str, sample: str) -> str:
                prefix = "primary" if definition == "primary" else "sensitivity"
                return f"""
                SELECT '{sport}'::VARCHAR sport,c.game_id::VARCHAR event_id,c.market_id,
                       c.official_date::DATE market_date,'{sample}'::VARCHAR close_sample,
                       e.price,(e.token_id=u.winning_token_id)::DOUBLE won,e.usdc,
                       e.proxyWallet,e.timestamp,
                       ((e.token_id=u.winning_token_id)::DOUBLE-e.price)::DOUBLE calibration_error
                FROM read_parquet('{quoted(close_path)}') c
                JOIN read_parquet('{quoted(exact_path)}') e
                  ON e.transaction_hash=c.{prefix}_transaction_hash
                 AND e.log_index=c.{prefix}_log_index
                 AND e.exchange_address=c.{prefix}_exchange_address
                JOIN read_parquet('{quoted(eligible_path)}') u ON u.market_id=c.market_id
                WHERE c.{prefix}_has_close
                """

            close_parts = [
                f"""SELECT sport,event_slug::VARCHAR event_id,market_id,
                           market_date::DATE market_date,close_sample,
                           price::DOUBLE price,won::DOUBLE won,usdc::DOUBLE usdc,
                           proxyWallet::VARCHAR proxyWallet,
                           "timestamp"::BIGINT close_timestamp,
                           calibration_error::DOUBLE calibration_error
                    FROM read_parquet('{quoted(new_closes)}')
                    WHERE sport IN ({retained_new_sql})""",
                f"""SELECT 'mlb',game_pk::VARCHAR,market_id,official_date,'all_trades',
                           primary_price,(primary_token_id=winning_token_id)::DOUBLE,primary_usdc,
                           primary_buyer,primary_close_timestamp,
                           ((primary_token_id=winning_token_id)::DOUBLE-primary_price)::DOUBLE
                    FROM read_parquet('{quoted(mlb_close_path)}') WHERE primary_has_close""",
                f"""SELECT 'mlb',game_pk::VARCHAR,market_id,official_date,'filtered_trades',
                           sensitivity_price,(sensitivity_token_id=winning_token_id)::DOUBLE,sensitivity_usdc,
                           sensitivity_buyer,sensitivity_close_timestamp,
                           ((sensitivity_token_id=winning_token_id)::DOUBLE-sensitivity_price)::DOUBLE
                    FROM read_parquet('{quoted(mlb_close_path)}') WHERE sensitivity_has_close""",
                legacy_close_sql("nfl", nfl_close_path, nfl_exact_path, nfl_eligible_path,
                                 "primary", "all_trades"),
                legacy_close_sql("nfl", nfl_close_path, nfl_exact_path, nfl_eligible_path,
                                 "sensitivity", "filtered_trades"),
                legacy_close_sql("nba", nba_close_path, nba_exact_path, nba_eligible_path,
                                 "primary", "all_trades"),
                legacy_close_sql("nba", nba_close_path, nba_exact_path, nba_eligible_path,
                                 "sensitivity", "filtered_trades"),
            ]
            con.execute("CREATE TABLE close_obs AS " + " UNION ALL ".join(close_parts))
            con.execute("ALTER TABLE close_obs ADD COLUMN price_decile INTEGER")
            con.execute("UPDATE close_obs SET price_decile=least(floor(price*10)::INTEGER+1,10)")
            invalid_close = con.execute("""
                SELECT count(*) FROM close_obs WHERE price<=0 OR price>=1 OR won NOT IN (0,1)
                  OR abs(calibration_error-(won-price))>1e-12
            """).fetchone()[0]
            if invalid_close:
                raise ValueError(f"Invalid normalized closing observations: {invalid_close}")

            con.execute("""
                CREATE TABLE phase_means AS
                SELECT sport,phase,phase_order,price_decile,count(*)::BIGINT trade_count,
                       count(DISTINCT event_id)::BIGINT event_count,sum(usdc)::DOUBLE dollars,
                       avg(price)::DOUBLE mean_price,avg(won)::DOUBLE win_rate,
                       avg(calibration_error)::DOUBLE mean_calibration
                FROM phase_obs GROUP BY 1,2,3,4
            """)
            phase_var = _variance_components(
                con, "phase_obs", "phase_means",
                ("sport", "phase", "phase_order", "price_decile"),
                ("trade_day", "proxyWallet", "event_id"),
            )
            phase_means = {
                (row["sport"], row["phase"], row["phase_order"], row["price_decile"]): row
                for row in _rows(con, "SELECT * FROM phase_means")
            }
            phase_output: list[tuple[Any, ...]] = []
            for sport, phase, label, order in _phase_dimensions():
                for decile in range(1, 11):
                    key = (sport, phase, order, decile)
                    row = phase_means.get(key)
                    n = int(row["trade_count"]) if row else 0
                    suppressed = n < MIN_CELL_N
                    values = [None] * 6
                    events = int(row["event_count"]) if row else 0
                    dollars = float(row["dollars"]) if row else 0.0
                    if row and not suppressed:
                        se = math.sqrt(max(phase_var.get(key, 0.0), 0.0)) / n
                        mean = float(row["mean_calibration"])
                        values = [float(row["mean_price"]), float(row["win_rate"]), mean,
                                  se, mean-1.96*se, mean+1.96*se]
                    phase_output.append((
                        sport,phase,label,order,decile,_price_bin(decile),n,events,dollars,
                        suppressed,f"suppressed_n_lt_{MIN_CELL_N}" if suppressed else "reported",*values,
                    ))

            con.execute("""
                CREATE TABLE close_means AS
                SELECT sport,close_sample,price_decile,count(*)::BIGINT close_count,
                       count(DISTINCT event_id)::BIGINT event_count,sum(usdc)::DOUBLE dollars,
                       avg(price)::DOUBLE mean_price,avg(won)::DOUBLE win_rate,
                       avg(calibration_error)::DOUBLE mean_calibration,
                       avg(calibration_error*calibration_error)::DOUBLE brier_score
                FROM close_obs GROUP BY 1,2,3
            """)
            close_var = _variance_components(
                con, "close_obs", "close_means",
                ("sport", "close_sample", "price_decile"), ("market_date",),
            )
            close_means = {
                (row["sport"], row["close_sample"], row["price_decile"]): row
                for row in _rows(con, "SELECT * FROM close_means")
            }
            closing_output: list[tuple[Any, ...]] = []
            for sport in REPORT_SPORTS:
                for sample in ("all_trades", "filtered_trades"):
                    for decile in range(1, 11):
                        key = (sport, sample, decile)
                        row = close_means.get(key)
                        n = int(row["close_count"]) if row else 0
                        suppressed = n < MIN_CELL_N
                        values = [None] * 7
                        events = int(row["event_count"]) if row else 0
                        dollars = float(row["dollars"]) if row else 0.0
                        if row and not suppressed:
                            se = math.sqrt(max(close_var.get(key, 0.0), 0.0)) / n
                            mean = float(row["mean_calibration"])
                            values = [float(row["mean_price"]), float(row["win_rate"]), mean,
                                      se, mean-1.96*se, mean+1.96*se,float(row["brier_score"])]
                        closing_output.append((
                            sport,sample,decile,_price_bin(decile),n,events,dollars,suppressed,
                            f"suppressed_n_lt_{MIN_CELL_N}" if suppressed else "reported",*values,
                        ))

            con.execute("""
                CREATE TABLE phase_tails AS
                SELECT sport,phase,phase_order,
                       count(*) FILTER(WHERE price_decile=1)::BIGINT d1_n,
                       count(*) FILTER(WHERE price_decile=10)::BIGINT d10_n,
                       count(DISTINCT event_id) FILTER(WHERE price_decile=1)::BIGINT d1_events,
                       count(DISTINCT event_id) FILTER(WHERE price_decile=10)::BIGINT d10_events,
                       avg(calibration_error) FILTER(WHERE price_decile=1)::DOUBLE d1_mean_calibration,
                       avg(calibration_error) FILTER(WHERE price_decile=10)::DOUBLE d10_mean_calibration
                FROM phase_obs GROUP BY 1,2,3
            """)
            phase_tail_var = _tail_variance_components(
                con,"phase_obs","phase_tails",("sport","phase","phase_order"),
                ("trade_day","proxyWallet","event_id"),
            )
            con.execute("""
                CREATE TABLE close_tails AS
                SELECT sport,close_sample,
                       count(*) FILTER(WHERE price_decile=1)::BIGINT d1_n,
                       count(*) FILTER(WHERE price_decile=10)::BIGINT d10_n,
                       count(DISTINCT event_id) FILTER(WHERE price_decile=1)::BIGINT d1_events,
                       count(DISTINCT event_id) FILTER(WHERE price_decile=10)::BIGINT d10_events,
                       avg(calibration_error) FILTER(WHERE price_decile=1)::DOUBLE d1_mean_calibration,
                       avg(calibration_error) FILTER(WHERE price_decile=10)::DOUBLE d10_mean_calibration
                FROM close_obs GROUP BY 1,2
            """)
            close_tail_var = _tail_variance_components(
                con,"close_obs","close_tails",("sport","close_sample"),("market_date",),
            )
            label_by_phase = {(s,p,o): label for s,p,label,o in _phase_dimensions()}
            tail_output: list[tuple[Any, ...]] = []
            close_tail_rows = {
                (row["sport"],row["close_sample"]):row
                for row in _rows(con, "SELECT * FROM close_tails")
            }
            for sport in REPORT_SPORTS:
                for sample in ("all_trades","filtered_trades"):
                    key = (sport,sample)
                    row = close_tail_rows.get(key)
                    n1,n10 = (int(row["d1_n"]),int(row["d10_n"])) if row else (0,0)
                    suppressed = n1 < MIN_CELL_N or n10 < MIN_CELL_N
                    values: list[Any] = [None] * 7
                    if row and not suppressed:
                        d1,d10 = float(row["d1_mean_calibration"]),float(row["d10_mean_calibration"])
                        spread = d10-d1
                        se = math.sqrt(max(close_tail_var.get(key,0.0),0.0))
                        values = [d1,d10,spread,se,spread-1.96*se,spread+1.96*se,_pattern(d1,d10)]
                    tail_output.append((
                        "closing",sport,sample,None,None,None,n1,n10,
                        int(row["d1_events"]) if row else 0,int(row["d10_events"]) if row else 0,*values,
                        suppressed,f"suppressed_tail_n_lt_{MIN_CELL_N}" if suppressed else "reported",
                    ))
            phase_tail_rows = {
                (row["sport"],row["phase"],row["phase_order"]):row
                for row in _rows(con, "SELECT * FROM phase_tails")
            }
            for sport,phase,label,order in _phase_dimensions():
                key = (sport,phase,order)
                row = phase_tail_rows.get(key)
                n1,n10 = (int(row["d1_n"]),int(row["d10_n"])) if row else (0,0)
                suppressed = n1 < MIN_CELL_N or n10 < MIN_CELL_N
                values = [None] * 7
                if row and not suppressed:
                    d1,d10 = float(row["d1_mean_calibration"]),float(row["d10_mean_calibration"])
                    spread = d10-d1
                    se = math.sqrt(max(phase_tail_var.get(key,0.0),0.0))
                    values = [d1,d10,spread,se,spread-1.96*se,spread+1.96*se,_pattern(d1,d10)]
                tail_output.append((
                    "trade_phase",sport,"filtered_trades",phase,label,order,n1,n10,
                    int(row["d1_events"]) if row else 0,int(row["d10_events"]) if row else 0,*values,
                    suppressed,f"suppressed_tail_n_lt_{MIN_CELL_N}" if suppressed else "reported",
                ))

            con.execute(f"COPY phase_obs TO '{quoted(staging/'normalized_phase_trades.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            con.execute(f"COPY close_obs TO '{quoted(staging/'normalized_closing_lines.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            phase_rows = int(con.execute("SELECT count(*) FROM phase_obs").fetchone()[0])
            close_rows = int(con.execute("SELECT count(*) FROM close_obs").fetchone()[0])
            phase_by_sport = dict(con.execute(
                "SELECT sport,count(*) FROM phase_obs GROUP BY 1 ORDER BY 1"
            ).fetchall())
            close_by_sport = dict(con.execute(
                "SELECT sport,count(*) FROM close_obs GROUP BY 1 ORDER BY 1"
            ).fetchall())
        finally:
            con.close()

        write_parquet(staging/"phase_calibration.parquet",PHASE_PROFILE_SCHEMA,phase_output,
                      ("sport","phase_order","price_decile"))
        write_parquet(staging/"closing_calibration.parquet",CLOSING_PROFILE_SCHEMA,closing_output,
                      ("sport","close_sample","price_decile"))
        write_parquet(staging/"flb_spreads.parquet",TAIL_SCHEMA,tail_output,
                      ("analysis_scope","sport","sample","phase_order"))
        manifest = {
            "schema_version": 2,
            "stage": "nine_cohort_common_bought_contract_calibration_v2",
            "estimand": "eventual outcome of bought contract minus its actual purchase price",
            "included_sports": list(REPORT_SPORTS),
            "excluded_sports": list(EXCLUDED_REPORT_SPORTS),
            "suppression_threshold": MIN_CELL_N,
            "phase_weighting": "equal BUY fill",
            "closing_weighting": "equal final pregame market close",
            "uncertainty": {
                "phase": "Cameron-Gelbach-Miller by UTC trade day, buyer wallet, and event",
                "closing": "one-way market-date clusters",
                "tail": "joint D10-minus-D1 cluster score including covariance",
            },
            "counts": {"phase_rows": phase_rows,"closing_rows": close_rows,
                       "phase_rows_by_sport": phase_by_sport,"closing_rows_by_sport": close_by_sport,
                       "phase_profile_rows": len(phase_output),"closing_profile_rows": len(closing_output),
                       "tail_rows": len(tail_output)},
            "inputs": {f"input_{index:02d}": fingerprint(path) for index,path in enumerate(inputs,1)},
            "outputs": {
                name: artifact_fingerprint(staging/name)
                for name in ("normalized_phase_trades.parquet","normalized_closing_lines.parquet",
                             "phase_calibration.parquet","closing_calibration.parquet","flb_spreads.parquet")
            },
        }
        write_json(staging/"estimator_manifest.json",manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "new_trade_run","mlb_phase","mlb_closes","nfl_phase","nfl_closes","nfl_exact",
        "nfl_eligible","nba_phase","nba_closes","nba_exact","nba_eligible","run_dir",
    ):
        parser.add_argument("--"+name.replace("_","-"),required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(json.dumps(estimate_combined(**vars(args)),sort_keys=True))


if __name__ == "__main__":
    main()
