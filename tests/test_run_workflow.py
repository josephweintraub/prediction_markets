from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest


ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "calibration_heterogeneity"
sys.path.insert(0, str(ENGINE_DIR))

from data_vintage import load_vintage, validate_vintage  # noqa: E402
from run_schemes import parse_args, run_analysis  # noqa: E402


def _fixture_vintage(tmp_path: Path) -> Path:
    analysis_root = tmp_path / "analysis_data"
    schemes = analysis_root / "schemes"
    schemes.mkdir(parents=True)

    n = 50
    low_market = np.repeat([0, 1], n // 2)
    high_market = np.repeat([0, 1], n // 2)
    market_code = np.concatenate([low_market, high_market])
    price = np.concatenate([np.full(n, 0.05), np.full(n, 0.95)])
    won = np.concatenate(
        [(np.arange(n) % 10 == 0).astype(float), (np.arange(n) % 10 != 0).astype(float)]
    )
    base = pd.DataFrame(
        {
            "market_code": market_code,
            "token_code": market_code,
            "wallet_code": np.arange(2 * n),
            "day": np.arange(2 * n),
            "price": price,
            "ret": won - price,
            "won": won,
            "usdc": np.where(market_code == 0, 1.0, 10.0),
        }
    )
    base_path = analysis_root / "flb_base_full.parquet"
    base.to_parquet(base_path, index=False)

    code_maps = pd.DataFrame(
        {"kind": ["market", "market"], "code": [0, 1], "value": ["m0", "m1"]}
    )
    code_maps_path = analysis_root / "code_maps.parquet"
    code_maps.to_parquet(code_maps_path, index=False)
    pd.DataFrame({"market_id": ["m0", "m1"], "slice": ["all", "all"]}).to_parquet(
        schemes / "scheme_all.parquet", index=False
    )

    artifact_paths: dict[str, Path] = {
        "trades_clean": tmp_path / "trades.parquet",
        "market_flags": tmp_path / "market_flags.parquet",
        "wallet_flags": tmp_path / "wallet_flags.parquet",
        "universe_markets": tmp_path / "universe_markets.parquet",
        "universe_tokens": tmp_path / "universe_tokens.parquet",
        "code_maps": code_maps_path,
        "flb_base_full": base_path,
    }
    for name, path in artifact_paths.items():
        if path.exists():
            continue
        pd.DataFrame({"fixture": [name]}).to_parquet(path, index=False)
    coverage_path = tmp_path / "coverage.json"
    metadata_path = tmp_path / "base_meta.json"
    coverage_path.write_text("{}\n", encoding="utf-8")
    metadata_path.write_text("{}\n", encoding="utf-8")
    artifact_paths["build_universe_coverage"] = coverage_path
    artifact_paths["flb_base_meta"] = metadata_path

    artifacts = {}
    for name, path in artifact_paths.items():
        spec = {"path": str(path), "format": "json" if path.suffix == ".json" else "parquet"}
        if name == "flb_base_full":
            spec["rows"] = len(base)
        artifacts[name] = spec
    vintage = {
        "id": "synthetic-fixture-v1",
        "analysis_root": str(analysis_root),
        "artifacts": artifacts,
    }
    vintage_path = tmp_path / "vintage.json"
    vintage_path.write_text(json.dumps(vintage), encoding="utf-8")
    return vintage_path


def test_small_fixture_runs_end_to_end_and_writes_immutable_manifest(tmp_path: Path) -> None:
    vintage = _fixture_vintage(tmp_path)
    run_root = tmp_path / "runs"
    args = parse_args(
        [
            "--window", "full",
            "--schemes", "all",
            "--min-trades", "1",
            "--min-decile-trades", "1",
            "--vintage", str(vintage),
            "--run-root", str(run_root),
            "--run-id", "fixture",
            "--threads", "1",
        ]
    )

    run_dir = run_analysis(args)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    deciles = pd.read_parquet(run_dir / "tables" / "flb_deciles_all_full.parquet")

    assert manifest["status"] == "completed"
    assert manifest["validations"]["data_vintage"]["status"] == "passed"
    assert manifest["validations"]["result_tables"]["all"]["status"] == "passed"
    assert {item["kind"] for item in manifest["outputs"]} == {
        "deciles", "summary", "dropped_slices"
    }
    assert {"cal_error_mkt", "cal_mkt_p_bonferroni", "cal_mkt_p_fdr_bh"} <= set(
        deciles.columns
    )
    with pytest.raises(FileExistsError):
        run_analysis(args)


def test_vintage_validation_fails_on_row_count_mismatch(tmp_path: Path) -> None:
    vintage_path = _fixture_vintage(tmp_path)
    payload = json.loads(vintage_path.read_text(encoding="utf-8"))
    payload["artifacts"]["flb_base_full"]["rows"] += 1
    vintage_path.write_text(json.dumps(payload), encoding="utf-8")

    con = duckdb.connect()
    with pytest.raises(ValueError, match="Vintage row mismatch"):
        validate_vintage(load_vintage(vintage_path), "full", con)
    con.close()
