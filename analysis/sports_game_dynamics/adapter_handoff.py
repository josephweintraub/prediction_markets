"""Immutable provenance contract for a sport adapter's standardized handoff."""
from __future__ import annotations

import argparse
import copy
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import duckdb

from .artifacts import (
    ArtifactError, artifact_fingerprint, fingerprint, fresh_run,
    matches_artifact_fingerprint, quoted, require_exact_schema, require_sport,
    resolved, write_json,
)
from .phase_contract import load_phase_contract, phase_contract_fingerprint
from .schemas import ELIGIBLE_SCHEMA


_LINEAGE_SEAL = object()
_NATIVE_SOURCE = {
    "nba": ("NBA official data API", "official"),
    "nfl": (
        "ESPN site API (third-party undocumented endpoint)",
        "third_party_undocumented",
    ),
}


@dataclass(frozen=True)
class VerifiedNativeLineage:
    """Opaque result of verifying one sport's complete native Stage-03 chain."""

    sport: str
    validated_run: Path
    source_provider: str
    source_status: str
    native_lineage: Mapping[str, Any]
    eligible_moneylines: Mapping[str, Any]
    _seal: object = field(repr=False, compare=False)


def _contract_record(path: Path) -> dict[str, Any]:
    contract = load_phase_contract(path)
    fingerprint = phase_contract_fingerprint(path)
    return {
        "sport": contract.sport,
        "schema_version": contract.schema_version,
        "contract_version": contract.contract_version,
        "bytes": fingerprint["bytes"],
        "sha256": fingerprint["sha256"],
        "timing_semantics": {
            "actual_start_event": contract.actual_start_event,
            "period_boundary_event": contract.period_boundary_event,
            "actual_end_event": contract.actual_end_event,
            "overtime_policy": contract.overtime_policy,
        },
    }


def _json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise ArtifactError(f"{label} must be a JSON object")
    return value


def _same_file_fingerprint(left: Any, right: Any) -> bool:
    return (
        isinstance(left, Mapping)
        and isinstance(right, Mapping)
        and all(left.get(key) == right.get(key) for key in ("path", "bytes", "sha256"))
    )


def _verify_nba_native_lineage(
    validated_run: Path, contract_path: Path
) -> dict[str, Any]:
    from analysis.nba_game_dynamics.artifact_manifest import verify_fingerprint
    from analysis.nba_game_dynamics.build_game_timing import (
        MANIFEST_OUTPUT as TIMING_MANIFEST_OUTPUT,
        PROVENANCE_OUTPUT as PROVIDER_PROVENANCE_OUTPUT,
        SUMMARY_OUTPUT as TIMING_SUMMARY_OUTPUT,
        verify_game_timing_run,
    )
    from analysis.nba_game_dynamics.build_validated_universe import (
        AUDIT_OUTPUT,
        ELIGIBLE_OUTPUT,
        MANIFEST_OUTPUT,
        SUMMARY_OUTPUT,
    )

    expected_files = {AUDIT_OUTPUT, ELIGIBLE_OUTPUT, SUMMARY_OUTPUT, MANIFEST_OUTPUT}
    if not validated_run.is_dir() or {
        path.name for path in validated_run.iterdir()
    } != expected_files:
        raise ArtifactError("NBA native Stage-03 file set is incomplete or unexpected")
    manifest_path = validated_run / MANIFEST_OUTPUT
    summary_path = validated_run / SUMMARY_OUTPUT
    manifest = _json_object(manifest_path, "NBA native Stage-03 manifest")
    expected_keys = {"schema_version", "stage", "inputs", "schemas", "outputs"}
    inputs = manifest.get("inputs")
    outputs = manifest.get("outputs")
    if (
        set(manifest) != expected_keys
        or manifest.get("schema_version") != 1
        or manifest.get("stage") != "nba_validated_universe"
        or not isinstance(inputs, dict)
        or set(inputs) != {
            "candidates", "timing_manifest", "universe_tokens", "token_map",
            "phase_contract",
        }
        or not isinstance(outputs, dict)
        or set(outputs) != {AUDIT_OUTPUT, ELIGIBLE_OUTPUT, SUMMARY_OUTPUT}
    ):
        raise ArtifactError("NBA native Stage-03 manifest identity is invalid")
    for record in inputs.values():
        verify_fingerprint(record)
    for name, record in outputs.items():
        if verify_fingerprint(record, base_dir=validated_run) != validated_run / name:
            raise ArtifactError("NBA native Stage-03 output path mismatch")

    requested_contract = phase_contract_fingerprint(contract_path)
    if not _same_file_fingerprint(inputs["phase_contract"], requested_contract):
        raise ArtifactError("NBA native Stage-03 phase-contract lineage mismatch")
    timing_manifest_path = verify_fingerprint(inputs["timing_manifest"])
    if timing_manifest_path.name != TIMING_MANIFEST_OUTPUT:
        raise ArtifactError("NBA native timing-manifest path mismatch")
    timing_run = timing_manifest_path.parent
    verify_game_timing_run(timing_run)
    timing_manifest = _json_object(timing_manifest_path, "NBA native timing manifest")
    timing_outputs = timing_manifest.get("outputs")
    if not isinstance(timing_outputs, dict):
        raise ArtifactError("NBA native timing outputs are invalid")
    timing_summary_path = verify_fingerprint(
        timing_outputs.get(TIMING_SUMMARY_OUTPUT), base_dir=timing_run
    )
    provider_path = verify_fingerprint(
        timing_outputs.get(PROVIDER_PROVENANCE_OUTPUT), base_dir=timing_run
    )
    provider = _json_object(provider_path, "NBA provider provenance")
    resources = provider.get("resources")
    if not isinstance(resources, list) or not resources:
        raise ArtifactError("NBA provider provenance lacks source evidence")
    source_evidence: list[dict[str, Any]] = []
    for resource in resources:
        if not isinstance(resource, dict):
            raise ArtifactError("NBA provider source evidence is invalid")
        cache_path = resource.get("cache_path")
        url = resource.get("url")
        if not isinstance(cache_path, str) or not isinstance(url, str) or not url:
            raise ArtifactError("NBA provider source evidence identity is invalid")
        observed = fingerprint(cache_path)
        if (
            observed["bytes"] != resource.get("bytes")
            or observed["sha256"] != resource.get("sha256")
        ):
            raise ArtifactError("NBA provider source evidence fingerprint mismatch")
        source_evidence.append({
            "kind": "provider_resource",
            "url": url,
            "fingerprint": observed,
        })

    summary = _json_object(summary_path, "NBA native Stage-03 summary")
    provider_record = fingerprint(provider_path)
    if (
        summary.get("phase_contract_sha256") != requested_contract["sha256"]
        or summary.get("provider_provenance_sha256") != provider_record["sha256"]
    ):
        raise ArtifactError("NBA native Stage-03 summary lineage mismatch")
    return {
        "stage": "nba_validated_universe",
        "manifest": fingerprint(manifest_path),
        "summary": fingerprint(summary_path),
        "native_eligible": fingerprint(validated_run / ELIGIBLE_OUTPUT),
        "timing_manifest": fingerprint(timing_manifest_path),
        "timing_summary": fingerprint(timing_summary_path),
        "provider_provenance": provider_record,
        "source_evidence": source_evidence,
    }


def _verify_nfl_native_lineage(
    validated_run: Path, contract_path: Path
) -> dict[str, Any]:
    from analysis.nfl_game_dynamics.artifact_manifest import verify_fingerprint
    from analysis.nfl_game_dynamics.build_game_timing import verify_timing_run
    from analysis.nfl_game_dynamics.build_validated_universe import (
        ELIGIBLE_OUTPUT,
        MANIFEST_OUTPUT,
        SUMMARY_OUTPUT,
        verify_validated_run,
    )

    summary = verify_validated_run(validated_run)
    manifest_path = validated_run / MANIFEST_OUTPUT
    summary_path = validated_run / SUMMARY_OUTPUT
    manifest = _json_object(manifest_path, "NFL native Stage-03 manifest")
    timing_manifest_path = verify_fingerprint(manifest["inputs"]["timing_manifest"])
    if timing_manifest_path.name != "timing_manifest.json":
        raise ArtifactError("NFL native timing-manifest path mismatch")
    timing_run = timing_manifest_path.parent
    timing_summary = verify_timing_run(timing_run)
    timing_manifest = _json_object(timing_manifest_path, "NFL native timing manifest")
    requested_contract = phase_contract_fingerprint(contract_path)
    if not _same_file_fingerprint(
        timing_manifest.get("inputs", {}).get("phase_contract"), requested_contract
    ):
        raise ArtifactError("NFL native Stage-03 phase-contract lineage mismatch")
    if (
        timing_summary.get("source") != "ESPN site API"
        or timing_summary.get("source_status") != "third_party_undocumented"
        or summary.get("timing_source") != "ESPN site API (third-party undocumented)"
    ):
        raise ArtifactError("NFL native provider identity/status is invalid")

    timing_outputs = timing_manifest.get("outputs")
    if not isinstance(timing_outputs, dict):
        raise ArtifactError("NFL native timing outputs are invalid")
    timing_summary_path = verify_fingerprint(
        timing_outputs.get("summary.json"), base_dir=timing_run
    )
    cache_inputs = timing_manifest.get("cache_inputs")
    if not isinstance(cache_inputs, dict):
        raise ArtifactError("NFL native source-evidence declaration is invalid")
    source_evidence: list[dict[str, Any]] = []
    for record in cache_inputs.get("scoreboards", ()):
        path = verify_fingerprint(record)
        source_evidence.append({
            "kind": "scoreboard",
            "fingerprint": fingerprint(path),
        })
    summaries = cache_inputs.get("summaries")
    if not isinstance(summaries, dict):
        raise ArtifactError("NFL native summary-evidence declaration is invalid")
    for game_id, record in sorted(summaries.items()):
        path = verify_fingerprint(record)
        source_evidence.append({
            "kind": "game_summary",
            "game_id": game_id,
            "fingerprint": fingerprint(path),
        })
    if not source_evidence:
        raise ArtifactError("NFL native lineage lacks provider source evidence")
    return {
        "stage": "nfl_validated_universe",
        "manifest": fingerprint(manifest_path),
        "summary": fingerprint(summary_path),
        "native_eligible": fingerprint(validated_run / ELIGIBLE_OUTPUT),
        "timing_manifest": fingerprint(timing_manifest_path),
        "timing_summary": fingerprint(timing_summary_path),
        "provider_provenance": None,
        "source_evidence": source_evidence,
    }


def _native_projection_rows(
    sport: str, validated_run: Path, contract_path: Path
) -> list[tuple[object, ...]]:
    """Recompute the standardized projection through the audited sport adapter."""

    if sport == "nba":
        from analysis.nba_game_dynamics.build_downstream_handoff import _rows
    elif sport == "nfl":
        from analysis.nfl_game_dynamics.build_downstream_handoff import _rows
    else:  # guarded by require_sport plus _NATIVE_SOURCE, retained fail-closed
        raise ArtifactError(f"No native eligible projection for {sport!r}")
    return _rows(validated_run, contract_path)


def _verify_exact_native_projection(
    sport: str,
    validated_run: Path,
    contract_path: Path,
    eligible: Path,
) -> None:
    """Require exact schema and multiset equality with the native Stage-03 projection."""

    from .artifacts import write_parquet

    rows = _native_projection_rows(sport, validated_run, contract_path)
    with tempfile.TemporaryDirectory(prefix=f"{sport}-native-projection-") as temporary:
        expected = Path(temporary) / "eligible_moneylines.parquet"
        write_parquet(expected, ELIGIBLE_SCHEMA, rows, ("market_id",))
        con = duckdb.connect()
        try:
            con.execute(
                f"CREATE VIEW published AS SELECT * FROM read_parquet('{quoted(eligible)}')"
            )
            con.execute(
                f"CREATE VIEW expected AS SELECT * FROM read_parquet('{quoted(expected)}')"
            )
            require_exact_schema(
                con, "published", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff"
            )
            require_exact_schema(
                con, "expected", ELIGIBLE_SCHEMA, "Native eligible projection"
            )
            mismatch = con.execute(
                """SELECT count(*) FROM (
                       (SELECT * FROM published EXCEPT ALL SELECT * FROM expected)
                       UNION ALL
                       (SELECT * FROM expected EXCEPT ALL SELECT * FROM published)
                   )"""
            ).fetchone()[0]
        finally:
            con.close()
    if mismatch:
        raise ArtifactError(
            "Eligible-moneyline handoff differs from the exact native Stage-03 projection"
        )


def _verify_native_lineage(
    sport: str,
    validated_run_dir: str | Path,
    phase_contract_path: str | Path,
    eligible_path: str | Path,
) -> VerifiedNativeLineage:
    """Verify and seal native lineage for use by trusted sport wrappers."""

    sport = require_sport(sport)
    if sport not in _NATIVE_SOURCE:
        raise ArtifactError(f"No verified native Stage-03 lineage adapter for {sport!r}")
    validated_run = resolved(validated_run_dir)
    contract_path = resolved(phase_contract_path)
    eligible = resolved(eligible_path)
    contract_record = _contract_record(contract_path)
    if contract_record["sport"] != sport:
        raise ArtifactError("Native lineage sport does not match phase contract")
    native_lineage = (
        _verify_nba_native_lineage(validated_run, contract_path)
        if sport == "nba"
        else _verify_nfl_native_lineage(validated_run, contract_path)
    )
    _verify_exact_native_projection(
        sport, validated_run, contract_path, eligible
    )
    provider, status = _NATIVE_SOURCE[sport]
    return VerifiedNativeLineage(
        sport=sport,
        validated_run=validated_run,
        source_provider=provider,
        source_status=status,
        native_lineage=copy.deepcopy(native_lineage),
        eligible_moneylines=artifact_fingerprint(eligible),
        _seal=_LINEAGE_SEAL,
    )


def _reopen_verified_lineage(
    value: Mapping[str, Any], sport: str, contract_path: Path, eligible: Path
) -> VerifiedNativeLineage:
    native = value.get("native_lineage")
    if not isinstance(native, Mapping) or set(native) != {
        "stage", "manifest", "summary", "native_eligible", "timing_manifest",
        "timing_summary", "provider_provenance", "source_evidence",
    }:
        raise ArtifactError("Adapter provenance native lineage schema is invalid")
    manifest = native.get("manifest")
    manifest_path = manifest.get("path") if isinstance(manifest, Mapping) else None
    if not isinstance(manifest_path, str) or not Path(manifest_path).is_absolute():
        raise ArtifactError("Adapter provenance native manifest path is invalid")
    observed = _verify_native_lineage(
        sport, Path(manifest_path).parent, contract_path, eligible
    )
    if native != observed.native_lineage:
        raise ArtifactError("Adapter provenance native lineage fingerprint mismatch")
    return observed


def build_adapter_handoff(
    native_lineage: VerifiedNativeLineage,
    eligible_path: str | Path,
    phase_contract_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    """Publish a handoff only from a sealed, reverified native Stage-03 chain."""

    if (
        not isinstance(native_lineage, VerifiedNativeLineage)
        or native_lineage._seal is not _LINEAGE_SEAL
    ):
        raise ArtifactError("Adapter handoff requires verified native Stage-03 lineage")
    eligible, contract_path = map(resolved, (eligible_path, phase_contract_path))
    observed = _verify_native_lineage(
        native_lineage.sport,
        native_lineage.validated_run,
        contract_path,
        eligible,
    )
    if observed != native_lineage:
        raise ArtifactError("Verified native lineage changed before adapter publication")
    sport = native_lineage.sport
    contract_record = _contract_record(contract_path)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW eligible AS SELECT * FROM read_parquet('{quoted(eligible)}')")
        require_exact_schema(con, "eligible", ELIGIBLE_SCHEMA, "Eligible-moneyline handoff")
        stats = con.execute(
            """SELECT count(*),count(DISTINCT market_id),count(DISTINCT game_id),
                      count(DISTINCT sport),min(sport),
                      count(*) FILTER (WHERE actual_start_utc IS NULL OR period_2_start_utc IS NULL
                        OR period_3_start_utc IS NULL OR period_4_start_utc IS NULL
                        OR actual_end_utc IS NULL)
               FROM eligible"""
        ).fetchone()
        invalid_order = con.execute(
            """SELECT count(*) FROM eligible WHERE NOT (
               actual_start_utc<period_2_start_utc AND period_2_start_utc<period_3_start_utc
               AND period_3_start_utc<period_4_start_utc AND period_4_start_utc<actual_end_utc)"""
        ).fetchone()[0]
        null_dimension = con.execute(
            """SELECT count(*) FROM eligible WHERE
               market_id IS NULL OR trim(market_id)='' OR game_id IS NULL OR trim(game_id)=''
               OR official_date IS NULL OR away_team_id IS NULL OR trim(away_team_id)=''
               OR home_team_id IS NULL OR trim(home_team_id)=''
               OR away_team_name IS NULL OR trim(away_team_name)=''
               OR home_team_name IS NULL OR trim(home_team_name)=''
               OR away_token_id IS NULL OR trim(away_token_id)=''
               OR home_token_id IS NULL OR trim(home_token_id)=''
               OR winning_team_id IS NULL OR winning_token_id IS NULL
               OR away_token_id=home_token_id
               OR winning_token_id NOT IN (away_token_id,home_token_id)
               OR winning_team_id != CASE WHEN winning_token_id=home_token_id
                    THEN home_team_id ELSE away_team_id END"""
        ).fetchone()[0]
    finally:
        con.close()
    if stats[0] == 0 or stats[0] != stats[1] or stats[0] != stats[2]:
        raise ArtifactError("Adapter handoff must be nonempty and one-to-one by market/game")
    if stats[3] != 1 or stats[4] != sport or stats[5] or invalid_order or null_dimension:
        raise ArtifactError("Adapter handoff dimensions/timing fail the frozen contract")
    value = {
        "schema_version": 2,
        "sport": sport,
        "source_provider": native_lineage.source_provider,
        "source_status": native_lineage.source_status,
        "phase_contract": contract_record,
        "eligible_moneylines": artifact_fingerprint(eligible),
        "native_lineage": copy.deepcopy(native_lineage.native_lineage),
    }
    with fresh_run(
        run_dir, (eligible, contract_path, native_lineage.validated_run)
    ) as staging:
        shutil.copyfile(eligible, staging / eligible.name)
        write_json(staging / "adapter_provenance.json", value)
    return value


def load_and_verify_adapter_handoff(
    path: str | Path,
    sport: str,
    eligible_path: str | Path,
    phase_contract_path: str | Path,
) -> dict[str, Any]:
    source, eligible, contract_path = map(
        resolved, (path, eligible_path, phase_contract_path)
    )
    value = _json_object(source, "Adapter provenance")
    if set(value) != {
        "schema_version", "sport", "source_provider", "source_status",
        "phase_contract", "eligible_moneylines", "native_lineage",
    }:
        raise ArtifactError("Adapter provenance schema is invalid")
    sport = require_sport(sport)
    if value.get("schema_version") != 2 or value.get("sport") != sport:
        raise ArtifactError("Adapter provenance sport/schema mismatch")
    if value.get("phase_contract") != _contract_record(contract_path):
        raise ArtifactError("Adapter provenance phase-contract SHA/semantics mismatch")
    if not matches_artifact_fingerprint(value.get("eligible_moneylines"), eligible):
        raise ArtifactError("Adapter provenance eligible artifact mismatch")
    observed = _reopen_verified_lineage(value, sport, contract_path, eligible)
    if (
        value.get("source_provider") != observed.source_provider
        or value.get("source_status") != observed.source_status
    ):
        raise ArtifactError("Adapter provenance provider identity/status mismatch")
    return value


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sport", required=True, choices=tuple(_NATIVE_SOURCE))
    parser.add_argument("--validated-run-dir", required=True)
    parser.add_argument("--eligible", required=True)
    parser.add_argument("--phase-contract", required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args(argv)
    lineage = _verify_native_lineage(
        args.sport, args.validated_run_dir, args.phase_contract, args.eligible
    )
    print(json.dumps(build_adapter_handoff(
        lineage, args.eligible, args.phase_contract, args.run_dir,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
