"""Load and enforce committed data-vintage declarations."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


FOUNDATIONAL_ARTIFACTS = (
    "trades_clean",
    "market_flags",
    "wallet_flags",
    "universe_markets",
    "universe_tokens",
    "code_maps",
    "build_universe_coverage",
    "flb_base_meta",
)


def load_vintage(path: str | Path) -> dict[str, Any]:
    vintage_path = Path(path).expanduser().resolve()
    with vintage_path.open(encoding="utf-8") as handle:
        vintage = json.load(handle)
    if not vintage.get("id") or not isinstance(vintage.get("artifacts"), dict):
        raise ValueError(f"Invalid data-vintage declaration: {vintage_path}")
    vintage["_declaration_path"] = str(vintage_path)
    return vintage


def _files_for(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(item for item in path.rglob("*.parquet") if item.is_file())
    return [path]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _parquet_expression(path: Path) -> str:
    if path.is_dir():
        source = str(path / "**" / "*.parquet")
    else:
        source = str(path)
    return source.replace("'", "''")


def validate_artifact(
    name: str, spec: dict[str, Any], con: duckdb.DuckDBPyConnection
) -> dict[str, Any]:
    path = Path(spec["path"])
    if not path.exists():
        raise FileNotFoundError(f"Vintage artifact {name!r} is missing: {path}")
    files = _files_for(path)
    if not files:
        raise FileNotFoundError(f"Vintage artifact {name!r} contains no parquet files: {path}")

    observed: dict[str, Any] = {
        "path": str(path),
        "bytes": sum(item.stat().st_size for item in files),
        "file_count": len(files),
        "latest_mtime_ns": max(item.stat().st_mtime_ns for item in files),
    }
    if "bytes" in spec and observed["bytes"] != spec["bytes"]:
        raise ValueError(
            f"Vintage byte mismatch for {name}: expected {spec['bytes']:,}, "
            f"found {observed['bytes']:,}"
        )
    if "file_count" in spec and observed["file_count"] != spec["file_count"]:
        raise ValueError(
            f"Vintage file-count mismatch for {name}: expected {spec['file_count']}, "
            f"found {observed['file_count']}"
        )

    if spec.get("format") == "parquet":
        source = _parquet_expression(path)
        observed["rows"] = int(
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{source}')").fetchone()[0]
        )
        if "rows" in spec and observed["rows"] != spec["rows"]:
            raise ValueError(
                f"Vintage row mismatch for {name}: expected {spec['rows']:,}, "
                f"found {observed['rows']:,}"
            )
        schema = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{source}')"
            ).fetchall()
        }
        missing = sorted(set(spec.get("required_columns", [])) - schema)
        if missing:
            raise ValueError(f"Vintage schema mismatch for {name}; missing columns: {missing}")
        observed["columns"] = sorted(schema)
    if "sha256" in spec:
        if len(files) != 1:
            raise ValueError(f"SHA-256 validation requires one file for {name}")
        observed["sha256"] = _sha256(files[0])
        if observed["sha256"] != spec["sha256"]:
            raise ValueError(f"Vintage SHA-256 mismatch for {name}")
    return observed


def validate_vintage(
    vintage: dict[str, Any], window: str, con: duckdb.DuckDBPyConnection
) -> dict[str, Any]:
    artifact_names = [*FOUNDATIONAL_ARTIFACTS, f"flb_base_{window}"]
    missing = [name for name in artifact_names if name not in vintage["artifacts"]]
    if missing:
        raise ValueError(f"Vintage {vintage['id']!r} lacks required artifacts: {missing}")
    observed = {
        name: validate_artifact(name, vintage["artifacts"][name], con)
        for name in artifact_names
    }
    return {"vintage_id": vintage["id"], "status": "passed", "artifacts": observed}

