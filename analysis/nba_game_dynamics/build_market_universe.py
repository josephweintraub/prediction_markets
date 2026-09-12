"""Build preliminary Polymarket NBA single-game moneyline candidates.

NBA totals and props frequently share the game event slug, so the MLB
``question has no colon`` rule is not sufficient.  This selector requires an
exact two-team ``<label> vs[.] <label>`` question and two tokens.  Every
NBA-prefixed row remains in diagnostics.  When multiple otherwise-valid rows
claim one event, every ambiguous row is excluded rather than aborting the run.
The slug positions are observed values, not official home/away identity.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import tempfile
from pathlib import Path

import duckdb

try:
    from .artifact_manifest import (
        ArtifactManifestError,
        file_fingerprint,
        require_parquet_schema,
        verify_fingerprint,
    )
except ImportError:  # pragma: no cover
    from artifact_manifest import (
        ArtifactManifestError,
        file_fingerprint,
        require_parquet_schema,
        verify_fingerprint,
    )


NBA_SLUG_PATTERN = r"^nba-([a-z0-9]+)-([a-z0-9]+)-(\d{4}-\d{2}-\d{2})$"
NBA_MONEYLINE_QUESTION_PATTERN = r"(?i)^[^:]+\s+vs\.?\s+[^:]+$"
REQUIRED_COLUMNS = (
    "market_id",
    "event_slug",
    "question",
    "n_tokens",
    "n_trades_raw",
    "n_buy_filtered",
    "usd_buy_filtered",
    "first_trade_at",
    "last_trade_at",
)
OUTPUT_COLUMNS = (
    "market_id",
    "event_slug",
    "date",
    "away",
    "home",
    "question",
    "n_tokens",
    "n_trades_raw",
    "n_buy_filtered",
    "usd_buy_filtered",
    "first_trade_at",
    "last_trade_at",
)
CANDIDATE_OUTPUT = "candidate_markets.parquet"
DIAGNOSTIC_OUTPUT = "candidate_diagnostics.parquet"
MANIFEST_OUTPUT = "market_universe_manifest.json"
CANDIDATE_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"), ("date", "DATE"),
    ("away", "VARCHAR"), ("home", "VARCHAR"), ("question", "VARCHAR"),
    ("n_tokens", "BIGINT"), ("n_trades_raw", "DOUBLE"),
    ("n_buy_filtered", "DOUBLE"), ("usd_buy_filtered", "DOUBLE"),
    ("first_trade_at", "TIMESTAMPTZ"), ("last_trade_at", "TIMESTAMPTZ"),
)
DIAGNOSTIC_SCHEMA = (
    ("market_id", "VARCHAR"), ("event_slug", "VARCHAR"),
    ("question", "VARCHAR"), ("n_tokens", "BIGINT"),
    ("n_trades_raw", "DOUBLE"), ("n_buy_filtered", "DOUBLE"),
    ("usd_buy_filtered", "DOUBLE"), ("first_trade_at", "TIMESTAMPTZ"),
    ("last_trade_at", "TIMESTAMPTZ"), ("slug_matches", "BOOLEAN"),
    ("away", "VARCHAR"), ("home", "VARCHAR"), ("date", "DATE"),
    ("question_matches", "BOOLEAN"), ("exclusion_reason", "VARCHAR"),
    ("is_candidate", "BOOLEAN"),
)


def _quote_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve()).replace("'", "''")


def _validate_relation(con: duckdb.DuckDBPyConnection, relation: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError(f"Unsafe DuckDB relation name: {relation!r}")
    columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
    missing = sorted(set(REQUIRED_COLUMNS) - columns)
    if missing:
        raise ValueError(f"NBA market source is missing required columns: {missing}")


def diagnostic_query(relation: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError(f"Unsafe DuckDB relation name: {relation!r}")
    slug = NBA_SLUG_PATTERN.replace("'", "''")
    question = NBA_MONEYLINE_QUESTION_PATTERN.replace("'", "''")
    return f"""
        WITH parsed AS (
            SELECT
                market_id, event_slug, question, n_tokens, n_trades_raw,
                n_buy_filtered, usd_buy_filtered, first_trade_at, last_trade_at,
                regexp_full_match(lower(event_slug), '{slug}') AS slug_matches,
                NULLIF(regexp_extract(lower(event_slug), '{slug}', 1), '') AS away,
                NULLIF(regexp_extract(lower(event_slug), '{slug}', 2), '') AS home,
                TRY_CAST(NULLIF(regexp_extract(lower(event_slug), '{slug}', 3), '') AS DATE) AS date,
                regexp_full_match(trim(question), '{question}') AS question_matches
            FROM {relation}
            WHERE event_slug ILIKE 'nba-%'
        ), classified AS (
            SELECT *,
                CASE
                    WHEN NOT slug_matches THEN 'slug_pattern_mismatch'
                    WHEN date IS NULL THEN 'invalid_date'
                    WHEN question IS NULL OR trim(question) = '' THEN 'missing_question'
                    WHEN NOT question_matches THEN 'question_not_two_team_matchup'
                    WHEN n_tokens IS NULL OR n_tokens <> 2 THEN 'not_two_tokens'
                    ELSE NULL
                END AS base_exclusion_reason
            FROM parsed
        ), counted AS (
            SELECT *, count(*) FILTER (WHERE base_exclusion_reason IS NULL)
                OVER (PARTITION BY event_slug) AS valid_event_rows
            FROM classified
        )
        SELECT * EXCLUDE (base_exclusion_reason, valid_event_rows),
            CASE
                WHEN base_exclusion_reason IS NOT NULL THEN base_exclusion_reason
                WHEN valid_event_rows > 1 THEN 'duplicate_event_candidate'
                ELSE NULL
            END AS exclusion_reason,
            exclusion_reason IS NULL AS is_candidate
        FROM counted
    """


def candidate_query(relation: str) -> str:
    columns = ", ".join(
        f'"{name}"::{kind} AS "{name}"' for name, kind in CANDIDATE_SCHEMA
    )
    return f"""
        SELECT {columns}
        FROM ({diagnostic_query(relation)}) diagnostics
        WHERE is_candidate
        ORDER BY date, away, home, market_id
    """


def assert_one_candidate_per_event(
    con: duckdb.DuckDBPyConnection, relation: str
) -> None:
    duplicates = con.execute(
        f"""
        SELECT event_slug, count(*) AS candidate_count,
               list(market_id ORDER BY market_id) AS market_ids
        FROM ({diagnostic_query(relation)}) diagnostics
        WHERE is_candidate
        GROUP BY event_slug
        HAVING count(*) > 1
        ORDER BY event_slug
        """
    ).fetchall()
    if duplicates:
        details = "; ".join(
            f"{slug}: {count} candidates {market_ids}"
            for slug, count, market_ids in duplicates
        )
        raise ValueError(f"NBA duplicate exclusion invariant failed: {details}")


def build_market_universe(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    source_path: str | Path,
    run_dir: str | Path,
) -> dict[str, int]:
    """Atomically publish the fixed-schema candidate/diagnostic pair."""

    _validate_relation(con, relation)
    assert_one_candidate_per_event(con, relation)
    source = Path(source_path).expanduser().resolve()
    output = Path(run_dir).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if output == source or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("NBA market-universe run must not overlap its source")
    if output.exists():
        raise FileExistsError(f"Immutable NBA market-universe run exists: {output}")
    diagnostic_sql = diagnostic_query(relation)
    candidate_sql = candidate_query(relation)
    candidate_count = int(con.execute(f"SELECT count(*) FROM ({candidate_sql})").fetchone()[0])
    diagnostic_count = int(con.execute(f"SELECT count(*) FROM ({diagnostic_sql})").fetchone()[0])
    stats = {
        "candidate_markets": candidate_count,
        "diagnostic_rows": diagnostic_count,
        "excluded_rows": diagnostic_count - candidate_count,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:
        con.execute(
            f"COPY ({candidate_sql}) TO '{_quote_path(staging / CANDIDATE_OUTPUT)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        diagnostic_columns = ", ".join(
            f'"{name}"::{kind} AS "{name}"' for name, kind in DIAGNOSTIC_SCHEMA
        )
        con.execute(
            f"COPY (SELECT {diagnostic_columns} FROM ({diagnostic_sql}) ORDER BY market_id) "
            f"TO '{_quote_path(staging / DIAGNOSTIC_OUTPUT)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        schemas = {
            CANDIDATE_OUTPUT: CANDIDATE_SCHEMA,
            DIAGNOSTIC_OUTPUT: DIAGNOSTIC_SCHEMA,
        }
        manifest = {
            "schema_version": 1,
            "stage": "nba_market_universe",
            "input": file_fingerprint(source),
            "counts": stats,
            "schemas": {name: [list(row) for row in schema] for name, schema in schemas.items()},
            "outputs": {
                name: file_fingerprint(staging / name, relative_to=staging)
                for name in schemas
            },
        }
        (staging / MANIFEST_OUTPUT).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        verify_market_universe_run(staging)
        if output.exists():
            raise FileExistsError(f"NBA market-universe run appeared during build: {output}")
        staging.rename(output)
        verify_market_universe_run(output)
        return stats
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def verify_market_universe_run(run_dir: str | Path) -> dict[str, int]:
    run = Path(run_dir).expanduser().resolve()
    expected_files = {CANDIDATE_OUTPUT, DIAGNOSTIC_OUTPUT, MANIFEST_OUTPUT}
    if not run.is_dir() or {path.name for path in run.iterdir()} != expected_files:
        raise ArtifactManifestError("NBA market-universe run file set is incomplete or unexpected")
    manifest = json.loads((run / MANIFEST_OUTPUT).read_text(encoding="utf-8"))
    if set(manifest) != {"schema_version", "stage", "input", "counts", "schemas", "outputs"}:
        raise ArtifactManifestError("NBA market-universe manifest keys mismatch")
    if manifest["schema_version"] != 1 or manifest["stage"] != "nba_market_universe":
        raise ArtifactManifestError("NBA market-universe manifest identity mismatch")
    verify_fingerprint(manifest["input"])
    schemas = {CANDIDATE_OUTPUT: CANDIDATE_SCHEMA, DIAGNOSTIC_OUTPUT: DIAGNOSTIC_SCHEMA}
    declared = {name: [list(row) for row in schema] for name, schema in schemas.items()}
    if manifest["schemas"] != declared or set(manifest["outputs"]) != set(schemas):
        raise ArtifactManifestError("NBA market-universe schema/output declaration mismatch")
    for name, schema in schemas.items():
        if verify_fingerprint(manifest["outputs"][name], base_dir=run) != run / name:
            raise ArtifactManifestError("NBA market-universe output path mismatch")
        require_parquet_schema(run / name, schema, name)
    con = duckdb.connect()
    try:
        candidates = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{_quote_path(run / CANDIDATE_OUTPUT)}')"
        ).fetchone()[0])
        diagnostics = int(con.execute(
            f"SELECT count(*) FROM read_parquet('{_quote_path(run / DIAGNOSTIC_OUTPUT)}')"
        ).fetchone()[0])
    finally:
        con.close()
    observed = {"candidate_markets": candidates, "diagnostic_rows": diagnostics, "excluded_rows": diagnostics - candidates}
    if manifest["counts"] != observed:
        raise ArtifactManifestError("NBA market-universe counts do not reconcile")
    return observed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    con = duckdb.connect()
    try:
        con.execute(
            f"CREATE VIEW source_markets AS SELECT * FROM "
            f"read_parquet('{_quote_path(args.markets)}')"
        )
        stats = build_market_universe(con, "source_markets", args.markets, args.run_dir)
    finally:
        con.close()
    print(
        f"Wrote {stats['candidate_markets']:,} candidates; retained "
        f"{stats['diagnostic_rows']:,} NBA-prefixed diagnostic rows "
        f"({stats['excluded_rows']:,} excluded)."
    )


if __name__ == "__main__":
    main()
