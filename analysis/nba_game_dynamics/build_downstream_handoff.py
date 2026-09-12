"""Publish the audited NBA Stage-03 result in the shared 18-column handoff."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import duckdb

from analysis.sports_game_dynamics.adapter_handoff import (
    _verify_native_lineage,
    build_adapter_handoff,
    load_and_verify_adapter_handoff,
)
from analysis.sports_game_dynamics.artifacts import (
    ArtifactError,
    quoted,
    write_parquet,
)
from analysis.sports_game_dynamics.phase_contract import (
    load_phase_contract,
    phase_contract_fingerprint,
)
from analysis.sports_game_dynamics.schemas import ELIGIBLE_SCHEMA

from .artifact_manifest import require_parquet_schema, verify_fingerprint
from .build_validated_universe import (
    AUDIT_OUTPUT,
    ELIGIBLE_OUTPUT,
    ELIGIBLE_SCHEMA as NBA_ELIGIBLE_SCHEMA,
    MANIFEST_OUTPUT,
    SUMMARY_OUTPUT,
)


SOURCE_PROVIDER = "NBA official data API"
SOURCE_STATUS = "official"
PROVENANCE_OUTPUT = "adapter_provenance.json"


def _rows(validated_run: Path, contract_path: Path) -> list[tuple[object, ...]]:
    manifest_path = validated_run / MANIFEST_OUTPUT
    summary_path = validated_run / SUMMARY_OUTPUT
    if not manifest_path.is_file() or not summary_path.is_file():
        raise FileNotFoundError("NBA validated run lacks its manifest or summary")
    expected_files = {AUDIT_OUTPUT, ELIGIBLE_OUTPUT, SUMMARY_OUTPUT, MANIFEST_OUTPUT}
    if not validated_run.is_dir() or {path.name for path in validated_run.iterdir()} != expected_files:
        raise ArtifactError("NBA validated run file set is incomplete or unexpected")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    outputs = manifest.get("outputs") if isinstance(manifest, dict) else None
    inputs = manifest.get("inputs") if isinstance(manifest, dict) else None
    if (
        manifest.get("schema_version") != 1
        or manifest.get("stage") != "nba_validated_universe"
        or not isinstance(outputs, dict)
        or set(outputs) != {AUDIT_OUTPUT, ELIGIBLE_OUTPUT, SUMMARY_OUTPUT}
        or not isinstance(inputs, dict)
    ):
        raise ArtifactError("NBA validated manifest identity or file declaration is invalid")
    for record in inputs.values():
        verify_fingerprint(record)
    verified_outputs = {
        name: verify_fingerprint(record, base_dir=validated_run)
        for name, record in outputs.items()
    }
    if any(verified_outputs[name] != validated_run / name for name in verified_outputs):
        raise ArtifactError("NBA validated output paths do not match their declarations")
    eligible = verified_outputs[ELIGIBLE_OUTPUT]
    require_parquet_schema(eligible, NBA_ELIGIBLE_SCHEMA, "NBA eligible source")

    contract = phase_contract_fingerprint(contract_path)
    phase_contract = load_phase_contract(contract_path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest_contract = manifest.get("inputs", {}).get("phase_contract")
    if (
        summary.get("phase_contract_sha256") != contract["sha256"]
        or not isinstance(manifest_contract, dict)
        or manifest_contract.get("sha256") != contract["sha256"]
        or summary.get("timestamp_semantics") != {
            "actual_start_event": phase_contract.actual_start_event,
            "actual_end_event": phase_contract.actual_end_event,
            "period_boundary_event": phase_contract.period_boundary_event,
        }
    ):
        raise ArtifactError("NBA validated run does not use the requested phase contract")

    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW eligible AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        invalid = con.execute(
            """SELECT count(*) FROM eligible WHERE
               phase_contract_sha256 IS DISTINCT FROM ?
               OR actual_start_event IS DISTINCT FROM ?
               OR period_boundary_event IS DISTINCT FROM ?
               OR actual_end_event IS DISTINCT FROM ?
               OR timestamp_source IS DISTINCT FROM 'official_nba_livedata_timeActual'
               OR provider_provenance_sha256 IS DISTINCT FROM ?""",
            [
                contract["sha256"],
                phase_contract.actual_start_event,
                phase_contract.period_boundary_event,
                phase_contract.actual_end_event,
                summary.get("provider_provenance_sha256"),
            ],
        ).fetchone()[0]
        if invalid:
            raise ArtifactError("NBA eligible rows do not preserve contract/event semantics")
        return con.execute(
            """SELECT 'nba'::VARCHAR, market_id, game_id, official_date,
                      CAST(away_team_id AS VARCHAR), away_team_name,
                      CAST(home_team_id AS VARCHAR), home_team_name,
                      away_token_id, home_token_id,
                      CAST(winner_team_id AS VARCHAR), winning_token_id,
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
        raise ArtifactError("NBA downstream handoff output overlaps its validated input")
    rows = _rows(validated, contract)
    with tempfile.TemporaryDirectory(prefix="nba-handoff-") as temporary:
        temporary_path = Path(temporary)
        eligible = temporary_path / ELIGIBLE_OUTPUT
        write_parquet(eligible, ELIGIBLE_SCHEMA, rows, ("market_id",))
        lineage = _verify_native_lineage("nba", validated, contract, eligible)
        value = build_adapter_handoff(lineage, eligible, contract, target)
    load_and_verify_adapter_handoff(
        target / PROVENANCE_OUTPUT, "nba", target / ELIGIBLE_OUTPUT, contract
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
