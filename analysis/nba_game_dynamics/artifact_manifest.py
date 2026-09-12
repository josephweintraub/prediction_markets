"""Small immutable-artifact fingerprint and exact Parquet-schema checks."""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

import duckdb


class ArtifactManifestError(ValueError):
    """Raised when a serialized artifact no longer matches its declaration."""


def file_fingerprint(
    path: str | Path, *, relative_to: str | Path | None = None
) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    payload = source.read_bytes()
    stored = source
    if relative_to is not None:
        stored = Path(source.relative_to(Path(relative_to).expanduser().resolve()))
    return {
        "path": str(stored),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def verify_fingerprint(
    record: Any, *, base_dir: str | Path | None = None
) -> Path:
    if not isinstance(record, dict) or set(record) != {"path", "bytes", "sha256"}:
        raise ArtifactManifestError("Fingerprint must contain exactly path/bytes/sha256")
    raw = record["path"]
    if not isinstance(raw, str) or not raw:
        raise ArtifactManifestError("Fingerprint path must be a nonempty string")
    path = Path(raw)
    if not path.is_absolute():
        if base_dir is None:
            raise ArtifactManifestError("Relative fingerprint requires a base directory")
        path = Path(base_dir).expanduser().resolve() / path
    observed = file_fingerprint(path)
    if observed["bytes"] != record["bytes"] or observed["sha256"] != record["sha256"]:
        raise ArtifactManifestError(f"Fingerprint mismatch: {path}")
    return path.resolve()


def normalized_schema(
    schema: Iterable[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    aliases = {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}
    return tuple((name, aliases.get(kind.upper(), kind.upper())) for name, kind in schema)


def parquet_schema(path: str | Path) -> tuple[tuple[str, str], ...]:
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    quoted = str(source).replace("'", "''")
    con = duckdb.connect()
    try:
        return tuple(
            (row[0], row[1].upper())
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{quoted}')"
            ).fetchall()
        )
    finally:
        con.close()


def require_parquet_schema(
    path: str | Path, schema: Iterable[tuple[str, str]], label: str
) -> None:
    expected = normalized_schema(schema)
    observed = parquet_schema(path)
    if observed != expected:
        raise ArtifactManifestError(
            f"{label} Parquet schema mismatch; expected={expected}, observed={observed}"
        )
