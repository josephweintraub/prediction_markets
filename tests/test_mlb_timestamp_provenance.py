from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

from timestamp_provenance import (  # noqa: E402
    TimestampProvenanceError,
    validate_timestamp_provenance,
    validate_vintage_timestamp_provenance,
    verify_extract_timestamps,
    verify_vintage_extract_timestamps,
)


def _exact_declaration(cache_path: Path) -> dict:
    cache_sha256 = hashlib.sha256(cache_path.read_bytes()).hexdigest()
    return {
        "schema_version": 1,
        "method": "polygon_rpc_block_timestamp",
        "timestamp_unit": "unix_seconds",
        "cache": {
            "path": str(cache_path),
            "format": "parquet",
            "rows": 3,
            "sha256": cache_sha256,
            "required_columns": ["block_number", "timestamp"],
        },
        "build_metadata": {
            "used_exact_cache": True,
            "source_distinct_blocks": 3,
            "cache_distinct_blocks": 3,
            "missing_blocks": 0,
            "fallback_rows": 0,
        },
    }


def _cache(tmp_path: Path) -> Path:
    path = tmp_path / "block_timestamps.parquet"
    pd.DataFrame(
        {
            "block_number": [40_000_001, 40_000_002, 40_000_003],
            "timestamp": [1_743_500_001, 1_743_500_003, 1_743_500_005],
        }
    ).to_parquet(path, index=False)
    return path


def test_exact_timestamp_provenance_passes_and_integrates_with_vintage(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    vintage = {
        "id": "fixture-vintage",
        "timestamp_provenance": _exact_declaration(cache),
    }
    con = duckdb.connect()

    report = validate_vintage_timestamp_provenance(vintage, con)
    con.close()

    assert report["status"] == "passed"
    assert report["vintage_id"] == "fixture-vintage"
    assert report["cache"]["rows"] == 3
    assert report["cache"]["distinct_blocks"] == 3
    assert report["analysis_extract_verified"] is False


def test_extract_timestamps_are_verified_against_exact_cache(tmp_path: Path) -> None:
    cache = _cache(tmp_path)
    declaration = _exact_declaration(cache)
    extract = pd.DataFrame(
        {
            "trade_id": ["a", "b", "c"],
            "block_number": [40_000_001, 40_000_001, 40_000_003],
            "timestamp": [1_743_500_001, 1_743_500_001, 1_743_500_005],
        }
    )
    con = duckdb.connect()
    con.register("mlb_extract", extract)

    report = verify_vintage_extract_timestamps(
        {"id": "fixture-vintage", "timestamp_provenance": declaration},
        con,
        "mlb_extract",
    )
    con.close()

    assert report["analysis_extract_verified"] is True
    assert report["scope"] == "analysis_extract"
    assert report["vintage_id"] == "fixture-vintage"
    assert report["extract"]["rows"] == 3
    assert report["extract"]["distinct_blocks"] == 2
    assert report["extract"]["cache_subset_distinct_blocks"] == 2


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"method": "linear_block_interpolation"}, "Timestamp method must be"),
        ({"build_metadata": {"fallback_rows": 12}}, "fallback-derived rows"),
        ({"build_metadata": {"missing_blocks": 1}}, "missing blocks"),
        ({"build_metadata": {"used_exact_cache": False}}, "did not declare use"),
    ],
)
def test_timestamp_gate_rejects_nonexact_or_incomplete_builds(
    tmp_path: Path, mutation: dict, message: str
) -> None:
    declaration = _exact_declaration(_cache(tmp_path))
    for key, value in mutation.items():
        if key == "build_metadata":
            declaration[key].update(value)
        else:
            declaration[key] = value
    con = duckdb.connect()

    with pytest.raises(TimestampProvenanceError, match=message):
        validate_timestamp_provenance(declaration, con)
    con.close()


def test_timestamp_gate_checks_one_cache_row_per_block(tmp_path: Path) -> None:
    cache = tmp_path / "block_timestamps.parquet"
    pd.DataFrame(
        {
            "block_number": [40_000_001, 40_000_001, 40_000_003],
            "timestamp": [1_743_500_001, 1_743_500_001, 1_743_500_005],
        }
    ).to_parquet(cache, index=False)
    declaration = _exact_declaration(cache)
    con = duckdb.connect()

    with pytest.raises(TimestampProvenanceError, match="one row per block"):
        validate_timestamp_provenance(declaration, con)
    con.close()


def test_timestamp_gate_requires_and_verifies_cache_sha256(tmp_path: Path) -> None:
    declaration = _exact_declaration(_cache(tmp_path))
    del declaration["cache"]["sha256"]
    con = duckdb.connect()
    with pytest.raises(TimestampProvenanceError, match="requires a valid declared SHA-256"):
        validate_timestamp_provenance(declaration, con)

    declaration = _exact_declaration(_cache(tmp_path))
    declaration["cache"]["sha256"] = "0" * 64
    with pytest.raises(TimestampProvenanceError, match="SHA-256 mismatch"):
        validate_timestamp_provenance(declaration, con)
    con.close()


def test_extract_gate_rejects_same_count_but_different_block_set(tmp_path: Path) -> None:
    declaration = _exact_declaration(_cache(tmp_path))
    extract = pd.DataFrame(
        {
            "block_number": [40_000_001, 40_000_002, 40_000_004],
            "timestamp": [1_743_500_001, 1_743_500_003, 1_743_500_007],
        }
    )
    con = duckdb.connect()
    con.register("mlb_extract", extract)

    with pytest.raises(TimestampProvenanceError, match="block set.*missing from the cache"):
        verify_extract_timestamps(declaration, con, "mlb_extract")
    con.close()


def test_extract_gate_rejects_altered_timestamp(tmp_path: Path) -> None:
    declaration = _exact_declaration(_cache(tmp_path))
    extract = pd.DataFrame(
        {
            "block_number": [40_000_001, 40_000_002],
            "timestamp": [1_743_500_001, 1_743_500_999],
        }
    )
    con = duckdb.connect()
    con.register("mlb_extract", extract)

    with pytest.raises(TimestampProvenanceError, match="altered/non-exact timestamps"):
        verify_extract_timestamps(declaration, con, "mlb_extract")
    con.close()


@pytest.mark.parametrize(
    ("column", "message"),
    [
        ("block_number", "null block numbers or timestamps"),
        ("timestamp", "null block numbers or timestamps"),
    ],
)
def test_extract_gate_rejects_null_keys(
    tmp_path: Path, column: str, message: str
) -> None:
    declaration = _exact_declaration(_cache(tmp_path))
    extract = pd.DataFrame(
        {
            "block_number": pd.Series([40_000_001, 40_000_002], dtype="Int64"),
            "timestamp": pd.Series([1_743_500_001, 1_743_500_003], dtype="Int64"),
        }
    )
    extract.loc[1, column] = pd.NA
    con = duckdb.connect()
    con.register("mlb_extract", extract)

    with pytest.raises(TimestampProvenanceError, match=message):
        verify_extract_timestamps(declaration, con, "mlb_extract")
    con.close()


def test_timestamp_gate_rejects_vintage_without_declaration() -> None:
    con = duckdb.connect()
    with pytest.raises(TimestampProvenanceError, match="no timestamp_provenance"):
        validate_vintage_timestamp_provenance({"id": "undeclared"}, con)
    con.close()
