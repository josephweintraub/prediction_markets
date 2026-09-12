"""Select strict preliminary NFL single-game moneyline candidates."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import tempfile
from pathlib import Path

import duckdb

from .artifact_manifest import (
    ArtifactManifestError,
    file_fingerprint,
    require_parquet_schema,
    verify_fingerprint,
)


NFL_SLUG_PATTERN = r"^nfl-([a-z0-9]+)-([a-z0-9]+)-(\d{4}-\d{2}-\d{2})$"
NFL_VERSUS_QUESTION_PATTERN = r"^[a-z0-9][a-z0-9 .&-]* vs\.? [a-z0-9][a-z0-9 .&-]*$"
REQUIRED_COLUMNS = {
    "market_id", "event_slug", "question", "n_tokens", "n_trades_raw",
    "n_buy_filtered", "usd_buy_filtered", "first_trade_at", "last_trade_at",
}
CANDIDATE_OUTPUT = "candidate_markets.parquet"
DIAGNOSTIC_OUTPUT = "candidate_diagnostics.parquet"
MANIFEST_OUTPUT = "market_universe_manifest.json"
CANDIDATE_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"), ("date", "DATE"),
    ("team_1_slug", "VARCHAR"), ("team_2_slug", "VARCHAR"),
    ("question", "VARCHAR"), ("n_tokens", "BIGINT"),
    ("n_trades_raw", "DOUBLE"), ("n_buy_filtered", "DOUBLE"),
    ("usd_buy_filtered", "DOUBLE"),
    ("first_trade_at", "TIMESTAMPTZ"), ("last_trade_at", "TIMESTAMPTZ"),
)
DIAGNOSTIC_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"),
    ("question", "VARCHAR"), ("n_tokens", "BIGINT"),
    ("n_trades_raw", "DOUBLE"), ("n_buy_filtered", "DOUBLE"),
    ("usd_buy_filtered", "DOUBLE"),
    ("first_trade_at", "TIMESTAMPTZ"), ("last_trade_at", "TIMESTAMPTZ"),
    ("slug_matches", "BOOLEAN"), ("team_1_slug", "VARCHAR"),
    ("team_2_slug", "VARCHAR"), ("date", "DATE"),
    ("question_is_team_versus", "BOOLEAN"),
    ("exclusion_reason", "VARCHAR"), ("is_candidate", "BOOLEAN"),
)


def _safe_relation(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe DuckDB relation name: {name!r}")
    return name


def _quote_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve()).replace("'", "''")


def _validate_source(con: duckdb.DuckDBPyConnection, relation: str) -> None:
    relation = _safe_relation(relation)
    columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        raise ValueError(f"NFL market source is missing required columns: {missing}")


def diagnostic_query(relation: str) -> str:
    relation = _safe_relation(relation)
    slug_pattern = NFL_SLUG_PATTERN.replace("'", "''")
    question_pattern = NFL_VERSUS_QUESTION_PATTERN.replace("'", "''")
    return f"""
        WITH parsed AS (
            SELECT market_id, event_slug, question, n_tokens, n_trades_raw,
                   n_buy_filtered, usd_buy_filtered, first_trade_at, last_trade_at,
                   regexp_full_match(lower(event_slug), '{slug_pattern}') AS slug_matches,
                   NULLIF(regexp_extract(lower(event_slug), '{slug_pattern}', 1), '') AS team_1_slug,
                   NULLIF(regexp_extract(lower(event_slug), '{slug_pattern}', 2), '') AS team_2_slug,
                   TRY_CAST(NULLIF(regexp_extract(lower(event_slug), '{slug_pattern}', 3), '') AS DATE) AS date,
                   regexp_full_match(lower(trim(question)), '{question_pattern}') AS question_is_team_versus
            FROM {relation}
            WHERE event_slug ILIKE 'nfl-%'
        )
        SELECT *,
               CASE
                 WHEN NOT slug_matches THEN 'slug_pattern_mismatch'
                 WHEN date IS NULL THEN 'invalid_date'
                 WHEN question IS NULL OR trim(question) = '' THEN 'missing_question'
                 WHEN n_tokens IS NULL OR n_tokens <> 2 THEN 'not_exactly_two_tokens'
                 WHEN contains(question, ':') THEN 'question_contains_colon'
                 WHEN NOT question_is_team_versus THEN 'question_not_team_versus'
                 ELSE NULL
               END AS exclusion_reason,
               exclusion_reason IS NULL AS is_candidate
        FROM parsed
    """


def candidate_query(relation: str) -> str:
    diagnostics = diagnostic_query(relation)
    return f"""
        SELECT market_id::VARCHAR AS market_id, event_slug::VARCHAR AS event_slug,
               date::DATE AS date, team_1_slug::VARCHAR AS team_1_slug,
               team_2_slug::VARCHAR AS team_2_slug, question::VARCHAR AS question,
               n_tokens::BIGINT AS n_tokens, n_trades_raw::DOUBLE AS n_trades_raw,
               n_buy_filtered::DOUBLE AS n_buy_filtered,
               usd_buy_filtered::DOUBLE AS usd_buy_filtered,
               first_trade_at::TIMESTAMPTZ AS first_trade_at,
               last_trade_at::TIMESTAMPTZ AS last_trade_at
        FROM ({diagnostics})
        WHERE is_candidate
        ORDER BY date, team_1_slug, team_2_slug, market_id
    """


def build_market_universe(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    source_path: str | Path,
    run_dir: str | Path,
) -> dict[str, int]:
    """Publish candidates and diagnostics together through one directory rename."""

    _validate_source(con, relation)
    diagnostics = diagnostic_query(relation)
    duplicates = con.execute(
        f"""SELECT event_slug, list(market_id ORDER BY market_id)
             FROM ({diagnostics}) WHERE is_candidate
             GROUP BY event_slug HAVING count(*) > 1 ORDER BY event_slug"""
    ).fetchall()
    if duplicates:
        raise ValueError(f"Duplicate NFL moneyline candidates; refusing to guess: {duplicates}")
    candidates = candidate_query(relation)
    candidate_count = int(con.execute(f"SELECT count(*) FROM ({candidates})").fetchone()[0])
    diagnostic_count = int(con.execute(f"SELECT count(*) FROM ({diagnostics})").fetchone()[0])
    if candidate_count == 0:
        raise ValueError("Strict NFL candidate universe is empty")
    source = Path(source_path).expanduser().resolve()
    target = Path(run_dir).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if target == source or target in source.parents or source in target.parents:
        raise ValueError("NFL market-universe run must not overlap its input")
    if target.exists():
        raise FileExistsError(f"Immutable NFL market-universe run exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        candidate_path = staging / CANDIDATE_OUTPUT
        diagnostic_path = staging / DIAGNOSTIC_OUTPUT
        con.execute(f"COPY ({candidates}) TO '{_quote_path(candidate_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        diagnostic_columns = ",".join(
            f'"{name}"::{kind} AS "{name}"' for name, kind in DIAGNOSTIC_SCHEMA
        )
        con.execute(
            f"COPY (SELECT {diagnostic_columns} FROM ({diagnostics}) ORDER BY market_id) "
            f"TO '{_quote_path(diagnostic_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        require_parquet_schema(candidate_path, CANDIDATE_SCHEMA, CANDIDATE_OUTPUT)
        require_parquet_schema(diagnostic_path, DIAGNOSTIC_SCHEMA, DIAGNOSTIC_OUTPUT)
        stats = {
            "candidate_markets": candidate_count,
            "diagnostic_rows": diagnostic_count,
            "excluded_rows": diagnostic_count - candidate_count,
        }
        manifest = {
            "schema_version": 1,
            "stage": "nfl_market_universe",
            "input": file_fingerprint(source),
            "counts": stats,
            "schemas": {
                CANDIDATE_OUTPUT: [list(row) for row in CANDIDATE_SCHEMA],
                DIAGNOSTIC_OUTPUT: [list(row) for row in DIAGNOSTIC_SCHEMA],
            },
            "outputs": {
                CANDIDATE_OUTPUT: file_fingerprint(candidate_path, relative_to=staging),
                DIAGNOSTIC_OUTPUT: file_fingerprint(diagnostic_path, relative_to=staging),
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        verify_market_universe_run(staging)
        staging.rename(target)
        verify_market_universe_run(target)
        return stats
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_market_universe_run(run_dir: str | Path) -> dict[str, int]:
    run = Path(run_dir).expanduser().resolve()
    expected_files = {CANDIDATE_OUTPUT, DIAGNOSTIC_OUTPUT, MANIFEST_OUTPUT}
    if not run.is_dir() or {path.name for path in run.iterdir()} != expected_files:
        raise ArtifactManifestError("NFL market-universe run file set is incomplete or unexpected")
    manifest = json.loads((run / MANIFEST_OUTPUT).read_text(encoding="utf-8"))
    if set(manifest) != {"schema_version", "stage", "input", "counts", "schemas", "outputs"}:
        raise ArtifactManifestError("NFL market-universe manifest keys mismatch")
    if manifest["schema_version"] != 1 or manifest["stage"] != "nfl_market_universe":
        raise ArtifactManifestError("NFL market-universe manifest identity mismatch")
    verify_fingerprint(manifest["input"])
    schemas = {CANDIDATE_OUTPUT: CANDIDATE_SCHEMA, DIAGNOSTIC_OUTPUT: DIAGNOSTIC_SCHEMA}
    if manifest["schemas"] != {name: [list(row) for row in schema] for name, schema in schemas.items()}:
        raise ArtifactManifestError("NFL market-universe declared schemas mismatch")
    if set(manifest["outputs"]) != set(schemas):
        raise ArtifactManifestError("NFL market-universe output manifest mismatch")
    for name, schema in schemas.items():
        if verify_fingerprint(manifest["outputs"][name], base_dir=run) != run / name:
            raise ArtifactManifestError("NFL market-universe output path mismatch")
        require_parquet_schema(run / name, schema, name)
    con = duckdb.connect()
    try:
        candidate_count = int(con.execute(f"SELECT count(*) FROM read_parquet('{_quote_path(run / CANDIDATE_OUTPUT)}')").fetchone()[0])
        diagnostic_count = int(con.execute(f"SELECT count(*) FROM read_parquet('{_quote_path(run / DIAGNOSTIC_OUTPUT)}')").fetchone()[0])
    finally:
        con.close()
    observed = {
        "candidate_markets": candidate_count,
        "diagnostic_rows": diagnostic_count,
        "excluded_rows": diagnostic_count - candidate_count,
    }
    if manifest["counts"] != observed:
        raise ArtifactManifestError("NFL market-universe counts do not reconcile")
    return observed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    source = Path(args.markets).expanduser().resolve()
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW source_markets AS SELECT * FROM read_parquet('{_quote_path(source)}')")
        stats = build_market_universe(con, "source_markets", source, args.run_dir)
    finally:
        con.close()
    print(stats)


if __name__ == "__main__":
    main()
