"""Small immutable-artifact helpers for shared sports game dynamics."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import duckdb


INTEGER_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
}


class ArtifactError(ValueError):
    """Raised when an immutable artifact fails its frozen contract."""


def resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def quoted(path: str | Path) -> str:
    return str(resolved(path)).replace("'", "''")


def fingerprint(path: str | Path) -> dict[str, str | int]:
    source = resolved(path)
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return {"path": str(source), "bytes": source.stat().st_size, "sha256": digest.hexdigest()}


def artifact_fingerprint(path: str | Path) -> dict[str, str | int]:
    """Fingerprint an output with a stable run-relative filename."""
    source = resolved(path)
    return {**fingerprint(source), "path": source.name}


def matches_artifact_fingerprint(value: Any, path: str | Path) -> bool:
    return isinstance(value, Mapping) and value == artifact_fingerprint(path)


def schema(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ArtifactError(f"Unsafe relation name: {relation!r}")
    return {row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}


def require_columns(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    required: Iterable[str],
    label: str,
) -> dict[str, str]:
    observed = schema(con, relation)
    missing = sorted(set(required) - set(observed))
    if missing:
        raise ArtifactError(f"{label} is missing required columns: {missing}")
    return observed


def require_exact_schema(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    expected: tuple[tuple[str, str], ...],
    label: str,
) -> None:
    observed = tuple(schema(con, relation).items())
    if observed != expected:
        raise ArtifactError(f"{label} schema mismatch; expected={expected}, observed={observed}")


def write_parquet(
    path: Path,
    row_schema: tuple[tuple[str, str], ...],
    rows: Iterable[tuple[Any, ...]],
    order_by: tuple[str, ...],
) -> None:
    con = duckdb.connect()
    try:
        definition = ",".join(f'"{name}" {kind}' for name, kind in row_schema)
        con.execute(f"CREATE TABLE artifact({definition})")
        materialized = list(rows)
        if materialized:
            placeholders = ",".join("?" for _ in row_schema)
            con.executemany(f"INSERT INTO artifact VALUES ({placeholders})", materialized)
        ordering = ",".join(f'"{name}"' for name in order_by)
        con.execute(
            f"COPY (SELECT * FROM artifact ORDER BY {ordering}) "
            f"TO '{quoted(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


@contextmanager
def fresh_run(target_path: str | Path, inputs: Iterable[str | Path] = ()) -> Iterator[Path]:
    target = resolved(target_path)
    if target.exists():
        raise FileExistsError(f"Immutable run already exists: {target}")
    for raw_input in inputs:
        source = resolved(raw_input)
        if target == source or target in source.parents or source in target.parents:
            raise ArtifactError(f"Output run overlaps input: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.staging-", dir=target.parent))
    try:
        yield staging
        os.replace(staging, target)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def require_sport(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ArtifactError(f"Invalid sport key: {value!r}")
    return value
