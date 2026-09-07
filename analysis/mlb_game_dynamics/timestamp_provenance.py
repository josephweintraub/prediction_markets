"""Hard validation gates for exact trade timestamp provenance.

The active trade transform contains a linear block-time fallback.  Analyses
that depend on first pitch or inning boundaries therefore must not infer
timestamp quality from the trade schema alone.  They require a declaration
that identifies the exact block-timestamp cache and records zero missing
blocks and zero fallback-derived rows for the build.  That declaration alone
does not prove a trade artifact is exact: an MLB analysis extract must retain
``block_number`` and ``timestamp`` and pass :func:`verify_extract_timestamps`.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping

import duckdb


EXACT_METHOD = "polygon_rpc_block_timestamp"
REQUIRED_BUILD_FIELDS = (
    "used_exact_cache",
    "source_distinct_blocks",
    "cache_distinct_blocks",
    "missing_blocks",
    "fallback_rows",
)
INTEGER_TYPES = {
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
}


class TimestampProvenanceError(ValueError):
    """Raised when exact timestamp provenance is absent or cannot be verified."""


def load_timestamp_provenance(path: str | Path) -> dict[str, Any]:
    declaration_path = Path(path).expanduser().resolve()
    with declaration_path.open(encoding="utf-8") as handle:
        declaration = json.load(handle)
    if not isinstance(declaration, dict):
        raise TimestampProvenanceError(
            f"Timestamp provenance declaration must be an object: {declaration_path}"
        )
    declaration["_declaration_path"] = str(declaration_path)
    return declaration


def _resolve_path(raw_path: str, declaration: Mapping[str, Any]) -> Path:
    path = Path(raw_path).expanduser()
    origin = declaration.get("_declaration_path")
    if not path.is_absolute() and origin:
        path = Path(str(origin)).parent / path
    return path.resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_timestamp_provenance(
    declaration: Mapping[str, Any],
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """Verify a declaration and its exact block-timestamp cache only.

    Merely declaring an exact method is insufficient: coverage metadata must
    say that every source block was cached and no output rows used a fallback,
    and the cache must contain exactly one non-null timestamp per block.

    Passing this function still does not prove that another trade artifact
    used the cache.  Boundary-sensitive runners must additionally call
    :func:`verify_extract_timestamps` on their block-bearing analysis extract.
    """

    if declaration.get("schema_version") != 1:
        raise TimestampProvenanceError("Unknown timestamp provenance schema version")
    if declaration.get("method") != EXACT_METHOD:
        raise TimestampProvenanceError(
            f"Timestamp method must be {EXACT_METHOD!r}; "
            f"found {declaration.get('method')!r}"
        )
    if declaration.get("timestamp_unit") != "unix_seconds":
        raise TimestampProvenanceError("Timestamp unit must be declared as 'unix_seconds'")

    build = declaration.get("build_metadata")
    if not isinstance(build, Mapping):
        raise TimestampProvenanceError("Exact timestamp build_metadata is required")
    missing_fields = [field for field in REQUIRED_BUILD_FIELDS if field not in build]
    if missing_fields:
        raise TimestampProvenanceError(
            f"Timestamp build_metadata is missing fields: {missing_fields}"
        )
    if build["used_exact_cache"] is not True:
        raise TimestampProvenanceError("Trade build did not declare use of the exact cache")

    numeric_fields = REQUIRED_BUILD_FIELDS[1:]
    if any(
        not isinstance(build[field], int) or isinstance(build[field], bool)
        for field in numeric_fields
    ):
        raise TimestampProvenanceError("Timestamp coverage fields must be integer counts")
    if build["source_distinct_blocks"] <= 0:
        raise TimestampProvenanceError("source_distinct_blocks must be positive")
    if build["missing_blocks"] != 0:
        raise TimestampProvenanceError("Exact timestamp gate rejects builds with missing blocks")
    if build["fallback_rows"] != 0:
        raise TimestampProvenanceError("Exact timestamp gate rejects fallback-derived rows")
    if build["cache_distinct_blocks"] != build["source_distinct_blocks"]:
        raise TimestampProvenanceError(
            "Timestamp cache coverage does not equal the source distinct-block count"
        )

    cache = declaration.get("cache")
    if not isinstance(cache, Mapping) or not cache.get("path"):
        raise TimestampProvenanceError("Exact block-timestamp cache declaration is required")
    if cache.get("format") != "parquet":
        raise TimestampProvenanceError("Exact block-timestamp cache must be Parquet")
    declared_sha256 = cache.get("sha256")
    if not isinstance(declared_sha256, str) or not re.fullmatch(
        r"[0-9a-fA-F]{64}", declared_sha256
    ):
        raise TimestampProvenanceError(
            "Exact block-timestamp cache requires a valid declared SHA-256"
        )
    cache_path = _resolve_path(str(cache["path"]), declaration)
    if not cache_path.is_file():
        raise FileNotFoundError(f"Exact block-timestamp cache is missing: {cache_path}")

    escaped = str(cache_path).replace("'", "''")
    schema = {
        row[0]: row[1]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
        ).fetchall()
    }
    required_columns = {"block_number", "timestamp"} | set(
        cache.get("required_columns", [])
    )
    missing_columns = sorted(required_columns - schema.keys())
    if missing_columns:
        raise TimestampProvenanceError(
            f"Timestamp cache is missing columns: {missing_columns}"
        )
    bad_types = {
        name: schema[name]
        for name in ("block_number", "timestamp")
        if schema[name] not in INTEGER_TYPES
    }
    if bad_types:
        raise TimestampProvenanceError(
            f"Timestamp cache block_number/timestamp must be integers: {bad_types}"
        )

    rows, distinct_blocks, null_blocks, null_timestamps = con.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT block_number),
               COUNT(*) FILTER (WHERE block_number IS NULL),
               COUNT(*) FILTER (WHERE timestamp IS NULL)
        FROM read_parquet('{escaped}')
        """
    ).fetchone()
    observed_rows = int(rows)
    observed_distinct = int(distinct_blocks)
    if null_blocks or null_timestamps:
        raise TimestampProvenanceError("Timestamp cache contains null blocks or timestamps")
    if observed_rows != observed_distinct:
        raise TimestampProvenanceError("Timestamp cache must contain exactly one row per block")
    if observed_distinct != build["cache_distinct_blocks"]:
        raise TimestampProvenanceError(
            "Observed timestamp-cache block count does not match build_metadata"
        )
    if "rows" in cache and observed_rows != cache["rows"]:
        raise TimestampProvenanceError(
            f"Timestamp cache row mismatch: expected {cache['rows']}, found {observed_rows}"
        )
    observed_bytes = cache_path.stat().st_size
    if "bytes" in cache and observed_bytes != cache["bytes"]:
        raise TimestampProvenanceError(
            f"Timestamp cache byte mismatch: expected {cache['bytes']}, found {observed_bytes}"
        )
    observed_sha256 = _sha256(cache_path)
    if observed_sha256.lower() != declared_sha256.lower():
        raise TimestampProvenanceError("Timestamp cache SHA-256 mismatch")

    return {
        "status": "passed",
        "scope": "cache_declaration_only",
        "analysis_extract_verified": False,
        "method": EXACT_METHOD,
        "timestamp_unit": "unix_seconds",
        "cache": {
            "path": str(cache_path),
            "rows": observed_rows,
            "distinct_blocks": observed_distinct,
            "bytes": observed_bytes,
            "sha256": observed_sha256,
        },
        "build_metadata": dict(build),
    }


def _validate_extract_relation(
    con: duckdb.DuckDBPyConnection, relation: str
) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise TimestampProvenanceError(f"Unsafe DuckDB relation name: {relation!r}")
    schema = {
        row[0]: row[1]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    missing = sorted({"block_number", "timestamp"} - schema.keys())
    if missing:
        raise TimestampProvenanceError(
            f"MLB analysis extract must retain block_number and timestamp; missing {missing}"
        )
    bad_types = {
        name: schema[name]
        for name in ("block_number", "timestamp")
        if schema[name] not in INTEGER_TYPES
    }
    if bad_types:
        raise TimestampProvenanceError(
            f"MLB extract block_number/timestamp must be integers: {bad_types}"
        )


def verify_extract_timestamps(
    declaration: Mapping[str, Any],
    con: duckdb.DuckDBPyConnection,
    extract_relation: str,
) -> dict[str, Any]:
    """Prove an MLB analysis extract agrees row-for-row with the exact cache.

    The cache may cover the full Polymarket source while the extract contains
    only MLB trades.  Set equality is therefore checked between the extract's
    distinct block IDs and the cache subset selected by those IDs.  Every
    extract row must then carry exactly the cached timestamp for its block.
    """

    _validate_extract_relation(con, extract_relation)
    cache_report = validate_timestamp_provenance(declaration, con)
    cache_path = str(cache_report["cache"]["path"]).replace("'", "''")

    rows, distinct_blocks, null_blocks, null_timestamps = con.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT block_number),
               COUNT(*) FILTER (WHERE block_number IS NULL),
               COUNT(*) FILTER (WHERE timestamp IS NULL)
        FROM {extract_relation}
        """
    ).fetchone()
    extract_rows = int(rows)
    extract_distinct_blocks = int(distinct_blocks)
    if extract_rows == 0:
        raise TimestampProvenanceError("MLB analysis extract contains no rows")
    if null_blocks or null_timestamps:
        raise TimestampProvenanceError(
            "MLB analysis extract contains null block numbers or timestamps"
        )

    missing_count = int(con.execute(
        f"""
        WITH extract_blocks AS (
            SELECT DISTINCT block_number FROM {extract_relation}
        ), cache_blocks AS (
            SELECT block_number FROM read_parquet('{cache_path}')
        ), missing AS (
            SELECT e.block_number
            FROM extract_blocks e
            ANTI JOIN cache_blocks c USING (block_number)
        )
        SELECT COUNT(*) FROM missing
        """
    ).fetchone()[0])
    if missing_count:
        missing_sample = [
            row[0]
            for row in con.execute(
                f"""
                SELECT DISTINCT e.block_number
                FROM {extract_relation} e
                ANTI JOIN read_parquet('{cache_path}') c USING (block_number)
                ORDER BY e.block_number
                LIMIT 10
                """
            ).fetchall()
        ]
        raise TimestampProvenanceError(
            "MLB extract block set is not equal to its exact-cache subset; "
            f"{missing_count} blocks are missing from the cache, sample={missing_sample}"
        )

    cache_subset_blocks = int(
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{cache_path}') cache
            SEMI JOIN (
                SELECT DISTINCT block_number FROM {extract_relation}
            ) extract USING (block_number)
            """
        ).fetchone()[0]
    )
    if cache_subset_blocks != extract_distinct_blocks:
        raise TimestampProvenanceError(
            "MLB extract distinct block IDs do not equal the exact-cache subset"
        )

    mismatch_count = int(con.execute(
        f"""
        SELECT COUNT(*)
        FROM {extract_relation} e
        JOIN read_parquet('{cache_path}') c USING (block_number)
        WHERE e.timestamp != c.timestamp
        """
    ).fetchone()[0])
    if mismatch_count:
        mismatch_sample = con.execute(
            f"""
            SELECT e.block_number, e.timestamp AS extract_timestamp,
                   c.timestamp AS exact_timestamp
            FROM {extract_relation} e
            JOIN read_parquet('{cache_path}') c USING (block_number)
            WHERE e.timestamp != c.timestamp
            LIMIT 10
            """
        ).fetchall()
        raise TimestampProvenanceError(
            f"MLB extract has {mismatch_count} rows with altered/non-exact timestamps; "
            f"sample={mismatch_sample}"
        )

    return {
        **cache_report,
        "scope": "analysis_extract",
        "analysis_extract_verified": True,
        "extract": {
            "relation": extract_relation,
            "rows": extract_rows,
            "distinct_blocks": extract_distinct_blocks,
            "cache_subset_distinct_blocks": cache_subset_blocks,
            "timestamp_mismatches": 0,
        },
    }


def _declaration_from_vintage(vintage: Mapping[str, Any]) -> dict[str, Any]:
    raw = vintage.get("timestamp_provenance")
    if raw is None:
        raise TimestampProvenanceError(
            f"Data vintage {vintage.get('id')!r} has no timestamp_provenance declaration"
        )
    if isinstance(raw, str):
        vintage_origin = vintage.get("_declaration_path")
        path = Path(raw).expanduser()
        if not path.is_absolute() and vintage_origin:
            path = Path(str(vintage_origin)).parent / path
        declaration = load_timestamp_provenance(path)
    elif isinstance(raw, Mapping):
        declaration = dict(raw)
        if vintage.get("_declaration_path"):
            declaration["_declaration_path"] = vintage["_declaration_path"]
    else:
        raise TimestampProvenanceError(
            "timestamp_provenance must be an object or a path to a JSON declaration"
        )
    return declaration


def validate_vintage_timestamp_provenance(
    vintage: Mapping[str, Any],
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    """Validate the timestamp declaration embedded in or referenced by a vintage."""

    declaration = _declaration_from_vintage(vintage)
    report = validate_timestamp_provenance(declaration, con)
    report["vintage_id"] = vintage.get("id")
    return report


def verify_vintage_extract_timestamps(
    vintage: Mapping[str, Any],
    con: duckdb.DuckDBPyConnection,
    extract_relation: str,
) -> dict[str, Any]:
    """Runner-facing gate: resolve a vintage declaration and verify the extract."""

    declaration = _declaration_from_vintage(vintage)
    report = verify_extract_timestamps(declaration, con, extract_relation)
    report["vintage_id"] = vintage.get("id")
    return report
