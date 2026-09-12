"""Publish the audited NFL Stage-03 result in the shared 18-column handoff."""
from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import duckdb

from analysis.sports_game_dynamics.adapter_handoff import (
    _verify_native_lineage,
    build_adapter_handoff,
    load_and_verify_adapter_handoff,
)
from analysis.sports_game_dynamics.artifacts import ArtifactError, quoted, write_parquet
from analysis.sports_game_dynamics.phase_contract import phase_contract_fingerprint
from analysis.sports_game_dynamics.schemas import ELIGIBLE_SCHEMA

from .build_validated_universe import (
    ELIGIBLE_OUTPUT,
    ELIGIBLE_SCHEMA as NFL_ELIGIBLE_SCHEMA,
    verify_validated_run,
)
from .artifact_manifest import require_parquet_schema


SOURCE_PROVIDER = "ESPN site API (third-party undocumented endpoint)"
SOURCE_STATUS = "third_party_undocumented"
PROVENANCE_OUTPUT = "adapter_provenance.json"


def _rows(validated_run: Path, contract_path: Path) -> list[tuple[object, ...]]:
    summary = verify_validated_run(validated_run)
    eligible = validated_run / ELIGIBLE_OUTPUT
    require_parquet_schema(eligible, NFL_ELIGIBLE_SCHEMA, "NFL eligible source")
    contract = phase_contract_fingerprint(contract_path)
    if summary.get("phase_contract_sha256") != contract["sha256"]:
        raise ArtifactError("NFL validated run does not use the requested phase contract")

    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW eligible AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        invalid = con.execute(
            "SELECT count(*) FROM eligible WHERE phase_contract_sha256 IS DISTINCT FROM ?",
            [contract["sha256"]],
        ).fetchone()[0]
        if invalid:
            raise ArtifactError("NFL eligible rows do not preserve the phase-contract SHA")
        return con.execute(
            """SELECT 'nfl'::VARCHAR, market_id, game_id, official_date,
                      CAST(away_team_id AS VARCHAR), away_team_name,
                      CAST(home_team_id AS VARCHAR), home_team_name,
                      away_token_id, home_token_id,
                      CAST(winning_team_id AS VARCHAR), winning_token_id,
                      scheduled_start_utc, actual_start_utc, period_2_start_utc,
                      period_3_start_utc, period_4_start_utc, actual_end_utc
               FROM eligible ORDER BY market_id"""
        ).fetchall()
    finally:
        con.close()


def build_downstream_handoff(
    validated_run_dir: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, object]:
    validated = Path(validated_run_dir).expanduser().resolve()
    contract = Path(phase_contract_path).expanduser().resolve()
    target = Path(run_dir).expanduser().resolve()
    if target == validated or target in validated.parents or validated in target.parents:
        raise ArtifactError("NFL downstream handoff output overlaps its validated input")
    rows = _rows(validated, contract)
    with tempfile.TemporaryDirectory(prefix="nfl-handoff-") as temporary:
        temporary_path = Path(temporary)
        eligible = temporary_path / ELIGIBLE_OUTPUT
        write_parquet(eligible, ELIGIBLE_SCHEMA, rows, ("market_id",))
        lineage = _verify_native_lineage("nfl", validated, contract, eligible)
        value = build_adapter_handoff(lineage, eligible, contract, target)
    load_and_verify_adapter_handoff(
        target / PROVENANCE_OUTPUT, "nfl", target / ELIGIBLE_OUTPUT, contract
    )
    return value


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validated-run-dir", required=True)
    parser.add_argument("--phase-contract", required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    print(build_downstream_handoff(
        args.validated_run_dir, args.phase_contract, args.run_dir
    ))


if __name__ == "__main__":
    main()
