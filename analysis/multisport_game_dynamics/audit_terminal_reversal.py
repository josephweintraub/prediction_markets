"""Audit the D10-minus-D1 calibration reversal near recorded event end.

This diagnostic rebuilds the exact-fill analysis universe used by
``estimate_flb_decay.py`` and saves compact, immutable descriptions of
normalized-time and seconds-to-end windows.  It does not change the estimator
or its published artifacts.
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

from analysis.multisport_game_dynamics.estimate_flb_decay import (
    SPORTS,
    _create_exact_observations,
)
from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    write_json,
    write_parquet,
)


WINDOWS = (
    ("t80_90", "normalized_time", 0.80, 0.90,
     "realized_time>=0.80 AND realized_time<0.90"),
    ("t90_95", "normalized_time", 0.90, 0.95,
     "realized_time>=0.90 AND realized_time<0.95"),
    ("t95_99", "normalized_time", 0.95, 0.99,
     "realized_time>=0.95 AND realized_time<0.99"),
    ("t99_100", "normalized_time", 0.99, 1.00,
     "realized_time>=0.99 AND realized_time<=1.0"),
    ("t999_100", "normalized_time", 0.999, 1.00,
     "realized_time>=0.999 AND realized_time<=1.0"),
    ("last_600s", "seconds_to_end", 0.0, 600.0,
     "seconds_to_end>=0 AND seconds_to_end<=600"),
    ("last_300s", "seconds_to_end", 0.0, 300.0,
     "seconds_to_end>=0 AND seconds_to_end<=300"),
    ("last_120s", "seconds_to_end", 0.0, 120.0,
     "seconds_to_end>=0 AND seconds_to_end<=120"),
    ("last_60s", "seconds_to_end", 0.0, 60.0,
     "seconds_to_end>=0 AND seconds_to_end<=60"),
    ("last_30s", "seconds_to_end", 0.0, 30.0,
     "seconds_to_end>=0 AND seconds_to_end<=30"),
)

TAIL_SCHEMA = (
    ("sample", "VARCHAR"), ("window_id", "VARCHAR"),
    ("window_basis", "VARCHAR"), ("window_low", "DOUBLE"),
    ("window_high", "DOUBLE"), ("sport", "VARCHAR"),
    ("tail", "VARCHAR"), ("n_fills", "BIGINT"),
    ("n_events", "BIGINT"), ("n_wallets", "BIGINT"),
    ("n_days", "BIGINT"), ("dollars", "DOUBLE"),
    ("mean_price", "DOUBLE"), ("median_price", "DOUBLE"),
    ("p05_price", "DOUBLE"), ("p95_price", "DOUBLE"),
    ("win_rate", "DOUBLE"), ("mean_calibration", "DOUBLE"),
    ("sd_calibration", "DOUBLE"),
    ("median_seconds_to_end", "DOUBLE"),
    ("p05_seconds_to_end", "DOUBLE"),
    ("p95_seconds_to_end", "DOUBLE"),
    ("exact_end_share", "DOUBLE"),
    ("near_boundary_price_share", "DOUBLE"),
    ("wallet_fill_hhi", "DOUBLE"),
    ("top_wallet_fill_share", "DOUBLE"),
    ("top10_wallet_fill_share", "DOUBLE"),
    ("event_fill_hhi", "DOUBLE"),
    ("top_event_fill_share", "DOUBLE"),
    ("top10_event_fill_share", "DOUBLE"),
)

EVENT_SCHEMA = (
    ("sample", "VARCHAR"), ("window_id", "VARCHAR"),
    ("window_basis", "VARCHAR"), ("window_low", "DOUBLE"),
    ("window_high", "DOUBLE"), ("sport", "VARCHAR"),
    ("events_with_both_tails", "BIGINT"),
    ("median_event_spread", "DOUBLE"),
    ("mean_event_spread", "DOUBLE"),
    ("share_events_positive", "DOUBLE"),
)

PRICE_SCHEMA = (
    ("sample", "VARCHAR"), ("window_id", "VARCHAR"),
    ("sport", "VARCHAR"), ("tail", "VARCHAR"),
    ("price_band_low", "DOUBLE"), ("price_band_high", "DOUBLE"),
    ("n_fills", "BIGINT"), ("fill_share", "DOUBLE"),
    ("win_rate", "DOUBLE"), ("mean_calibration", "DOUBLE"),
)

RECONCILIATION_SCHEMA = (
    ("sample", "VARCHAR"), ("sport", "VARCHAR"), ("n_fills", "BIGINT"),
)


def _dict_rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _concentration(
    con: duckdb.DuckDBPyConnection,
    where_sql: str,
    identifier: str,
) -> dict[tuple[str, str], tuple[float, float, float]]:
    rows = _dict_rows(
        con,
        f"""
        WITH grouped AS (
          SELECT sport,CASE WHEN price_decile=1 THEN 'D1' ELSE 'D10' END tail,
                 {identifier} cluster_id,count(*)::DOUBLE n
          FROM audit_base
          WHERE price_decile IN (1,10) AND {where_sql}
          GROUP BY 1,2,3
        ), ranked AS (
          SELECT *,sum(n) OVER(PARTITION BY sport,tail) total,
                 row_number() OVER(
                   PARTITION BY sport,tail ORDER BY n DESC,cluster_id
                 ) rank
          FROM grouped
        )
        SELECT sport,tail,
               sum(power(n/total,2))::DOUBLE hhi,
               max(n/total)::DOUBLE top1,
               sum(CASE WHEN rank<=10 THEN n ELSE 0 END)/max(total)::DOUBLE top10
        FROM ranked GROUP BY 1,2
        """,
    )
    return {
        (row["sport"], row["tail"]):
        (float(row["hhi"]), float(row["top1"]), float(row["top10"]))
        for row in rows
    }


def _collect_sample(
    con: duckdb.DuckDBPyConnection,
    sample: str,
    sample_filter: str,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    con.execute("DROP VIEW IF EXISTS audit_base")
    con.execute(
        f"""
        CREATE TEMP VIEW audit_base AS
        SELECT *,
               (epoch(actual_end_utc)-trade_timestamp)::DOUBLE seconds_to_end
        FROM observations WHERE {sample_filter}
        """
    )
    tail_rows: list[tuple[Any, ...]] = []
    event_rows: list[tuple[Any, ...]] = []
    price_rows: list[tuple[Any, ...]] = []
    for window_id, basis, low, high, condition in WINDOWS:
        wallet = _concentration(con, condition, "proxyWallet")
        event = _concentration(con, condition, "event_cluster")
        stats = _dict_rows(
            con,
            f"""
            SELECT sport,CASE WHEN price_decile=1 THEN 'D1' ELSE 'D10' END tail,
                   count(*)::BIGINT n_fills,
                   count(DISTINCT event_cluster)::BIGINT n_events,
                   count(DISTINCT proxyWallet)::BIGINT n_wallets,
                   count(DISTINCT trade_day)::BIGINT n_days,
                   sum(usdc)::DOUBLE dollars,
                   avg(price)::DOUBLE mean_price,median(price)::DOUBLE median_price,
                   quantile_cont(price,0.05)::DOUBLE p05_price,
                   quantile_cont(price,0.95)::DOUBLE p95_price,
                   avg(won)::DOUBLE win_rate,
                   avg(calibration_error)::DOUBLE mean_calibration,
                   stddev_samp(calibration_error)::DOUBLE sd_calibration,
                   median(seconds_to_end)::DOUBLE median_seconds_to_end,
                   quantile_cont(seconds_to_end,0.05)::DOUBLE p05_seconds_to_end,
                   quantile_cont(seconds_to_end,0.95)::DOUBLE p95_seconds_to_end,
                   avg((seconds_to_end=0)::INTEGER)::DOUBLE exact_end_share,
                   avg((CASE WHEN price_decile=1 THEN price<=0.02
                             ELSE price>=0.98 END)::INTEGER)::DOUBLE
                     near_boundary_price_share
            FROM audit_base
            WHERE price_decile IN (1,10) AND {condition}
            GROUP BY 1,2 ORDER BY 1,2
            """,
        )
        for row in stats:
            key = (row["sport"], row["tail"])
            wallet_values = wallet.get(key, (0.0, 0.0, 0.0))
            event_values = event.get(key, (0.0, 0.0, 0.0))
            tail_rows.append((
                sample, window_id, basis, low, high, row["sport"], row["tail"],
                int(row["n_fills"]), int(row["n_events"]), int(row["n_wallets"]),
                int(row["n_days"]), float(row["dollars"]),
                float(row["mean_price"]), float(row["median_price"]),
                float(row["p05_price"]), float(row["p95_price"]),
                float(row["win_rate"]), float(row["mean_calibration"]),
                float(row["sd_calibration"]),
                float(row["median_seconds_to_end"]),
                float(row["p05_seconds_to_end"]),
                float(row["p95_seconds_to_end"]),
                float(row["exact_end_share"]),
                float(row["near_boundary_price_share"]),
                *wallet_values, *event_values,
            ))
        event_stats = _dict_rows(
            con,
            f"""
            WITH means AS (
              SELECT sport,event_cluster,price_decile,
                     avg(calibration_error)::DOUBLE mean_calibration
              FROM audit_base
              WHERE price_decile IN (1,10) AND {condition}
              GROUP BY 1,2,3
            ), paired AS (
              SELECT sport,event_cluster,
                     max(mean_calibration) FILTER(WHERE price_decile=10)-
                     max(mean_calibration) FILTER(WHERE price_decile=1) spread
              FROM means GROUP BY 1,2
              HAVING count(DISTINCT price_decile)=2
            )
            SELECT sport,count(*)::BIGINT events_with_both_tails,
                   median(spread)::DOUBLE median_event_spread,
                   avg(spread)::DOUBLE mean_event_spread,
                   avg((spread>0)::INTEGER)::DOUBLE share_events_positive
            FROM paired GROUP BY 1 ORDER BY 1
            """,
        )
        event_rows.extend(
            (sample, window_id, basis, low, high, row["sport"],
             int(row["events_with_both_tails"]), float(row["median_event_spread"]),
             float(row["mean_event_spread"]), float(row["share_events_positive"]))
            for row in event_stats
        )
        if window_id in {"t90_95", "t95_99", "t99_100", "t999_100"}:
            bands = _dict_rows(
                con,
                f"""
                WITH grouped AS (
                  SELECT sport,
                         CASE WHEN price_decile=1 THEN 'D1' ELSE 'D10' END tail,
                         floor(price*100)::INTEGER band,count(*)::BIGINT n_fills,
                         avg(won)::DOUBLE win_rate,
                         avg(calibration_error)::DOUBLE mean_calibration
                  FROM audit_base
                  WHERE price_decile IN (1,10) AND {condition}
                  GROUP BY 1,2,3
                )
                SELECT *,n_fills/sum(n_fills) OVER(PARTITION BY sport,tail)::DOUBLE fill_share
                FROM grouped ORDER BY sport,tail,band
                """,
            )
            price_rows.extend(
                (sample, window_id, row["sport"], row["tail"],
                 int(row["band"]) / 100.0, (int(row["band"]) + 1) / 100.0,
                 int(row["n_fills"]), float(row["fill_share"]),
                 float(row["win_rate"]), float(row["mean_calibration"]))
                for row in bands
            )
    return tail_rows, event_rows, price_rows


def _open_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET threads=16")
    con.execute("SET memory_limit='200GB'")
    con.execute("SET temp_directory='/mnt/data/tmp'")
    con.execute("SET max_temp_directory_size='400GB'")
    return con


def run_audit(args: argparse.Namespace) -> dict[str, Any]:
    inputs = [
        Path(args.new_exact), Path(args.mlb_exact), Path(args.mlb_phase),
        Path(args.nfl_exact), Path(args.nfl_phase), Path(args.nba_exact),
        Path(args.nba_phase), Path(args.wallet_flags),
    ]
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    all_tail: list[tuple[Any, ...]] = []
    all_event: list[tuple[Any, ...]] = []
    all_price: list[tuple[Any, ...]] = []
    reconciliation: list[tuple[Any, ...]] = []
    specifications = (
        ("all_trades", "all_trades", "TRUE"),
        ("interior_all_buyers", "all_trades", "price>0.01 AND price<0.99"),
        ("filtered_trades", "filtered_trades", "TRUE"),
    )
    for sample, loader_sample, sample_filter in specifications:
        con = _open_connection()
        try:
            _create_exact_observations(
                con, *inputs[:-1], inputs[-1], loader_sample,
            )
            reconciliation.extend(
                (sample, sport, int(n))
                for sport, n in con.execute(
                    f"SELECT sport,count(*) FROM observations WHERE {sample_filter} "
                    "GROUP BY 1 ORDER BY 1"
                ).fetchall()
            )
            tail, event, price = _collect_sample(con, sample, sample_filter)
            all_tail.extend(tail)
            all_event.extend(event)
            all_price.extend(price)
        finally:
            con.close()

    with fresh_run(args.run_dir, inputs) as staging:
        write_parquet(staging / "window_tail_summary.parquet", TAIL_SCHEMA, all_tail,
                      ("sample", "window_id", "sport", "tail"))
        write_parquet(staging / "event_spread_summary.parquet", EVENT_SCHEMA, all_event,
                      ("sample", "window_id", "sport"))
        write_parquet(staging / "price_band_summary.parquet", PRICE_SCHEMA, all_price,
                      ("sample", "window_id", "sport", "tail", "price_band_low"))
        write_parquet(staging / "sample_reconciliation.parquet",
                      RECONCILIATION_SCHEMA, reconciliation, ("sample", "sport"))
        outputs = {
            name: artifact_fingerprint(staging / name)
            for name in (
                "window_tail_summary.parquet", "event_spread_summary.parquet",
                "price_band_summary.parquet", "sample_reconciliation.parquet",
            )
        }
        manifest = {
            "schema_version": 1,
            "stage": "multisport_terminal_reversal_audit_v1",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "command": sys.argv,
            "environment": {
                "python": platform.python_version(),
                "duckdb": duckdb.__version__,
                "platform": platform.platform(),
            },
            "code": {"script": fingerprint(Path(__file__))},
            "observation_unit": "exact resolved moneyline BUY fill",
            "calibration": "eventual bought-contract outcome minus purchase price",
            "tails": {"D1": "price decile 1", "D10": "price decile 10"},
            "samples": {
                "filtered_trades": "0.01 < price < 0.99; flagged buyers excluded",
                "interior_all_buyers": "0.01 < price < 0.99; flagged buyers included",
                "all_trades": "0 < price < 1; flagged buyers included",
            },
            "time": "(exact block timestamp-recorded start)/(recorded end-recorded start)",
            "windows": [
                {"id": row[0], "basis": row[1], "low": row[2], "high": row[3]}
                for row in WINDOWS
            ],
            "inputs": {f"input_{index:02d}": fingerprint(path)
                       for index, path in enumerate(inputs, start=1)},
            "outputs": outputs,
            "counts": {
                "window_tail_summary": len(all_tail),
                "event_spread_summary": len(all_event),
                "price_band_summary": len(all_price),
                "sample_reconciliation": len(reconciliation),
            },
            "completion_status": "complete",
        }
        write_json(staging / "manifest.json", manifest)
    return manifest


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--new-exact", required=True)
    parser.add_argument("--mlb-exact", required=True)
    parser.add_argument("--mlb-phase", required=True)
    parser.add_argument("--nfl-exact", required=True)
    parser.add_argument("--nfl-phase", required=True)
    parser.add_argument("--nba-exact", required=True)
    parser.add_argument("--nba-phase", required=True)
    parser.add_argument("--wallet-flags", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def main() -> None:
    print(json.dumps(run_audit(parse_args()), sort_keys=True))


if __name__ == "__main__":
    main()
