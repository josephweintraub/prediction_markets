"""Synthetic-only demonstration of wallet construction and existing bot flags.

No production data is read. Ten fixed maker BUY records are expanded under two
wallet-field constructions and passed to the unchanged existing flag builder.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from analysis.bot_filter import build_wallet_flags

WALLET_A, WALLET_B = "synthetic-wallet-A", "synthetic-wallet-B"
SOURCE_COUNT = 10
START = 1772388000  # 2026-03-01 18:00:00 UTC
SCENARIOS = (("fast_100_seconds", 100), ("slow_300_seconds", 300))
FIELDS = ("source_record_id", "proxyWallet", "timestamp", "conditionId", "usdcSize", "price",
          "side", "outcome", "eventSlug", "is_maker", "counterparty", "year_month")
FLAGS = ("flag_a_definite", "flag_a_likely", "flag_b_definite", "flag_b_likely", "flag_c", "flag_e", "is_nonhuman")
SOURCE_PATHS = ("analysis/bot_filter.py", "scripts/audit_polymarket_wallet_flag_mechanism.py",
                "tests/test_polymarket_wallet_flag_mechanism.py")
MAX_OUTPUT_BYTES = 128 * 1024


def source_records(spacing: int) -> list[dict]:
    if spacing not in {100, 300}:
        raise ValueError("Only the two fixed synthetic timing scenarios are admitted")
    return [{"source_record_id": f"synthetic-record-{index:02d}", "maker": WALLET_A,
             "taker": WALLET_B, "timestamp": START+index*spacing,
             "token_id": str(10**76+index), "cash": 1.0, "price": 0.5,
             "maker_side": "BUY", "outcome": "YES", "event_label": f"synthetic-event-{index:02d}"}
            for index in range(SOURCE_COUNT)]


def expand(records: list[dict], *, copied: bool) -> list[dict]:
    rows = []
    for record in records:
        maker = {"source_record_id": record["source_record_id"], "proxyWallet": record["maker"],
                 "counterparty": record["taker"], "timestamp": record["timestamp"],
                 "conditionId": record["token_id"], "usdcSize": record["cash"], "price": record["price"],
                 "side": "BUY", "is_maker": True, "outcome": record["outcome"], "eventSlug": record["event_label"],
                 "year_month": datetime.fromtimestamp(record["timestamp"], timezone.utc).strftime("%Y-%m")}
        counter = {**maker, "side": "SELL", "is_maker": False,
                   "proxyWallet": record["maker"] if copied else record["taker"],
                   "counterparty": record["taker"] if copied else record["maker"]}
        rows.extend((maker, counter))
    return rows


def require_conservation(records: list[dict], correct: list[dict], copied: list[dict]) -> None:
    if len(records) != SOURCE_COUNT or len(correct) != 20 or len(copied) != 20:
        raise ValueError("Synthetic source/expanded row counts do not reconcile")
    fields = tuple(field for field in FIELDS if field not in {"proxyWallet", "counterparty"})
    if Counter(tuple(row[field] for field in fields) for row in correct) != Counter(
            tuple(row[field] for field in fields) for row in copied):
        raise ValueError("The constructions differ beyond their wallet fields")
    for rows in (correct, copied):
        counts = Counter((row["source_record_id"], row["is_maker"]) for row in rows)
        if counts != Counter({(record["source_record_id"], role): 1
                              for record in records for role in (True, False)}):
            raise ValueError("Every synthetic source record must have one row per role")


def run_builder(rows: list[dict]) -> dict:
    import duckdb

    con = duckdb.connect(":memory:")
    try:
        for statement in ("SET threads=1", "SET memory_limit='128MB'", "SET temp_directory=''",
                          "SET max_temp_directory_size='0B'", "SET TimeZone='UTC'"):
            con.execute(statement)
        definitions = ",".join(f'"{field}" ' + ("BIGINT" if field == "timestamp" else
                               "DOUBLE" if field in {"usdcSize", "price"} else
                               "BOOLEAN" if field == "is_maker" else "VARCHAR") for field in FIELDS)
        con.execute(f"CREATE TEMP TABLE fixture_trades ({definitions})")
        con.executemany("INSERT INTO fixture_trades VALUES (" + ",".join("?" for _ in FIELDS) + ")",
                        [tuple(row[field] for field in FIELDS) for row in rows])
        con.execute("CREATE TEMP VIEW trades AS SELECT * FROM fixture_trades")
        summary = build_wallet_flags(con, verbose=False)
        cursor = con.execute("SELECT * FROM wallet_flags ORDER BY proxyWallet")
        names = [column[0] for column in cursor.description]
        observed = {row[0]: dict(zip(names, row)) for row in cursor.fetchall()}
        wallets = {}
        for wallet in (WALLET_A, WALLET_B):
            times = sorted(row["timestamp"] for row in rows if row["proxyWallet"] == wallet)
            intervals = [second-first for first, second in zip(times, times[1:])]
            approximate = (times[-1]-times[0])/(len(times)-1) if len(times) >= 2 else None
            actual = observed.get(wallet)
            wallets[wallet] = ({**actual, "present_in_wallet_flags": True} if actual is not None else
                {"proxyWallet": wallet, "present_in_wallet_flags": False, "n_trades": 0,
                 "median_iti": None, **{flag: None for flag in FLAGS}})
            wallets[wallet].update(approx_mean_iti_seconds=approximate,
                candidate_gate_passed=approximate is not None and approximate < 120,
                fixture_interval_count=len(intervals), fixture_zero_interval_count=intervals.count(0))
        if summary["total_trades"] != len(rows) or sum(row["n_trades"] for row in wallets.values()) != len(rows):
            raise ValueError("Existing flag builder did not conserve synthetic wallet rows")
        return {"summary": summary, "wallets": wallets, "expanded_row_count": len(rows),
                "gross_recorded_cash": sum(row["usdcSize"] for row in rows)}
    finally:
        con.close()


def build_demonstration() -> dict:
    import duckdb

    hashes = {path: hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in SOURCE_PATHS}
    cases = {}
    for name, spacing in SCENARIOS:
        records = source_records(spacing)
        correct, copied = expand(records, copied=False), expand(records, copied=True)
        require_conservation(records, correct, copied)
        results = {"correct_swapped": run_builder(correct), "copied_pair": run_builder(copied)}
        for construction, result in results.items():
            a, b = result["wallets"][WALLET_A], result["wallets"][WALLET_B]
            expected_a_rows = 20 if construction == "copied_pair" else 10
            expected_b_rows = 0 if construction == "copied_pair" else 10
            expected_a_median = (0.0 if construction == "copied_pair" else 100.0) if spacing == 100 else None
            expected_b_median = 100.0 if spacing == 100 and construction == "correct_swapped" else None
            if (a["n_trades"], b["n_trades"], a["median_iti"], b["median_iti"]) != (
                    expected_a_rows, expected_b_rows, expected_a_median, expected_b_median):
                raise ValueError("Existing builder no longer reproduces the frozen synthetic expectations")
            expected_flag = spacing == 100 and construction == "copied_pair"
            if a["is_nonhuman"] != expected_flag or a["flag_a_definite"] != expected_flag:
                raise ValueError("Existing composite flag differs from the frozen synthetic expectation")
        cases[name] = {"spacing_seconds": spacing, "source_records": records,
                       "exact_expanded_fixtures": {"correct_swapped": correct, "copied_pair": copied},
                       "results": results}
    if hashes != {path: hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in SOURCE_PATHS}:
        raise ValueError("Source changed during the synthetic demonstration")
    return {"schema_version": 1, "status": "synthetic_mechanism_complete", "data_certified": False,
        "question": "Can copied wallet-pair construction alone create zero median inter-trade intervals and criterion-A flags?",
        "source_sha256": hashes, "environment": {"python": sys.version, "duckdb": duckdb.__version__},
        "resource_contract": {"connections": "Four separate in-memory synthetic fixture connections",
            "rows_per_connection": 20, "threads": 1, "memory_limit": "128MB", "spill": "0B"},
        "construction_note": "Counterparty SELL is the legacy synthetic opposite-side representation; this fixture does not certify any counterparty economic action.",
        "candidate_gate": "Existing builder computes median ITI only when approximate span/(n-1) is below 120 seconds. Uncomputed medians remain null; absent wallets retain null flags.",
        "interpretation": "Conditional mechanism demonstration only. It does not prove historical flags were built from copied rows, establish incorrect historical labels, identify real bot behavior, or certify economic actions.",
        "cases": cases}


def write_immutable(destination: Path, evidence: dict) -> None:
    encoded = json.dumps(evidence, indent=2, sort_keys=True, allow_nan=False).encode()+b"\n"
    if len(encoded) > MAX_OUTPUT_BYTES:
        raise ValueError("Complete synthetic evidence exceeds the output cap; no truncation")
    destination.mkdir(parents=True, exist_ok=False)
    partial = destination/"evidence.json.partial"
    with partial.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, destination/"evidence.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.run_dir.exists():
        raise FileExistsError("Immutable synthetic output directory already exists")
    evidence = build_demonstration()
    write_immutable(args.run_dir, evidence)
    print(json.dumps({"status": evidence["status"], "run_dir": str(args.run_dir), "synthetic_only": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
