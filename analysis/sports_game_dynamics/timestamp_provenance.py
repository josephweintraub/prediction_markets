"""Sport-scoped exact Polygon block-timestamp declaration and verification."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from .artifacts import (
    ArtifactError, INTEGER_TYPES, fingerprint, fresh_run, quoted, require_columns,
    require_exact_schema, require_sport, resolved, schema, write_json,
)
from .schemas import ELIGIBLE_SCHEMA
from .adapter_handoff import load_and_verify_adapter_handoff
from .phase_contract import phase_contract_fingerprint


METHOD = "polygon_rpc_block_timestamp"
RAW_REQUIRED = {"condition_id", "block_number"}


def _scoped_blocks(
    con: duckdb.DuckDBPyConnection, raw_path: Path, eligible_path: Path
) -> tuple[int, ...]:
    con.execute(f"CREATE VIEW raw_source AS SELECT * FROM read_parquet('{quoted(raw_path)}')")
    con.execute(f"CREATE VIEW eligible AS SELECT * FROM read_parquet('{quoted(eligible_path)}')")
    require_columns(con, "raw_source", RAW_REQUIRED, "Resolved EVM fills")
    require_exact_schema(con, "eligible", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff")
    bad = con.execute(
        """SELECT count(*) FROM raw_source raw JOIN eligible e
           ON raw.condition_id=e.market_id
           WHERE raw.condition_id IS NULL OR raw.block_number IS NULL"""
    ).fetchone()[0]
    if bad:
        raise ArtifactError("Scoped EVM identity/timestamp keys cannot be null")
    return tuple(
        row[0]
        for row in con.execute(
            """SELECT DISTINCT raw.block_number::BIGINT
               FROM raw_source raw JOIN eligible e ON raw.condition_id=e.market_id
               ORDER BY 1"""
        ).fetchall()
    )


def validate_exact_cache(
    sport: str,
    raw_trades_path: str | Path,
    eligible_path: str | Path,
    cache_path: str | Path,
    adapter_provenance_path: str | Path,
    phase_contract_path: str | Path,
) -> dict[str, Any]:
    sport = require_sport(sport)
    raw, eligible, cache, adapter_provenance, phase_contract = map(
        resolved,
        (raw_trades_path, eligible_path, cache_path, adapter_provenance_path, phase_contract_path),
    )
    adapter = load_and_verify_adapter_handoff(
        adapter_provenance, sport, eligible, phase_contract
    )
    con = duckdb.connect()
    try:
        scoped = _scoped_blocks(con, raw, eligible)
        if not scoped:
            raise ArtifactError(f"No {sport} candidate-source blocks were found")
        con.execute(f"CREATE VIEW exact_cache AS SELECT * FROM read_parquet('{quoted(cache)}')")
        eligible_sports = con.execute(
            f"SELECT count(DISTINCT sport),min(sport) FROM read_parquet('{quoted(eligible)}')"
        ).fetchone()
        if eligible_sports != (1, sport):
            raise ArtifactError("Eligible-moneyline handoff does not match requested sport")
        observed_schema = schema(con, "exact_cache")
        if set(observed_schema) != {"block_number", "timestamp"}:
            raise ArtifactError("Exact timestamp cache must contain only block_number,timestamp")
        if any(observed_schema[name] not in INTEGER_TYPES for name in observed_schema):
            raise ArtifactError("Exact timestamp cache keys must be integer typed")
        bad = con.execute(
            """SELECT count(*)-count(DISTINCT block_number),
                      count(*) FILTER (WHERE block_number IS NULL OR timestamp IS NULL)
               FROM exact_cache"""
        ).fetchone()
        if bad != (0, 0):
            raise ArtifactError("Exact cache must have one non-null timestamp per block")
        if con.execute("SELECT count(*) FROM exact_cache WHERE timestamp<=0").fetchone()[0]:
            raise ArtifactError("Exact cache timestamps must be positive Unix seconds")
        missing = tuple(
            row[0] for row in con.execute(
                """SELECT source.block_number FROM
                     (SELECT unnest(?)::BIGINT block_number) source
                     ANTI JOIN exact_cache USING(block_number) ORDER BY 1""",
                [list(scoped)],
            ).fetchall()
        )
        if missing:
            raise ArtifactError(f"Exact cache coverage is incomplete; missing={missing[:10]}")
        return {
            "sport": sport,
            "method": METHOD,
            "timestamp_unit": "unix_seconds",
            "source_distinct_blocks": len(scoped),
            "cache_matched_blocks": len(scoped),
            "missing_blocks": 0,
            "fallback_rows": 0,
            "raw_trades": fingerprint(raw),
            "eligible_moneylines": fingerprint(eligible),
            "cache": fingerprint(cache),
            "adapter_provenance": fingerprint(adapter_provenance),
            "phase_contract": phase_contract_fingerprint(phase_contract),
            "timing_source_provider": adapter["source_provider"],
            "timing_source_status": adapter["source_status"],
            "timing_semantics": adapter["phase_contract"]["timing_semantics"],
        }
    finally:
        con.close()


def build_timestamp_declaration(
    sport: str,
    raw_trades_path: str | Path,
    eligible_path: str | Path,
    cache_path: str | Path,
    adapter_provenance_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    inputs = (raw_trades_path, eligible_path, cache_path, adapter_provenance_path, phase_contract_path)
    audit = validate_exact_cache(sport, *inputs)
    declaration = {"schema_version": 1, **audit}
    with fresh_run(run_dir, inputs) as staging:
        write_json(staging / "timestamp_provenance.json", declaration)
    return declaration


def load_and_verify_declaration(
    path: str | Path,
    sport: str,
    raw_trades_path: str | Path,
    eligible_path: str | Path,
    cache_path: str | Path,
    adapter_provenance_path: str | Path,
    phase_contract_path: str | Path,
) -> dict[str, Any]:
    source = resolved(path)
    value = json.loads(source.read_text(encoding="utf-8"))
    expected = {"schema_version": 1, **validate_exact_cache(
        sport, raw_trades_path, eligible_path, cache_path,
        adapter_provenance_path, phase_contract_path,
    )}
    if value != expected:
        raise ArtifactError("Timestamp declaration does not match current immutable inputs")
    return value


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sport", required=True)
    parser.add_argument("--raw-trades", required=True)
    parser.add_argument("--eligible", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--adapter-provenance", required=True)
    parser.add_argument("--phase-contract", required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    print(json.dumps(build_timestamp_declaration(
        args.sport, args.raw_trades, args.eligible, args.cache,
        args.adapter_provenance, args.phase_contract, args.run_dir,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
