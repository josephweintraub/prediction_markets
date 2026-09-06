from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest


ENGINE_DIR = Path(__file__).parents[1] / "analysis" / "embedding_difficulty"
sys.path.insert(0, str(ENGINE_DIR))

from run_schemes import select_scheme_files  # noqa: E402


def test_select_scheme_files_rejects_empty_directory(tmp_path: Path) -> None:
    (tmp_path / "schemes").mkdir()

    with pytest.raises(FileNotFoundError, match="No scheme files"):
        select_scheme_files(str(tmp_path), None)


def test_select_scheme_files_rejects_missing_requested_name(tmp_path: Path) -> None:
    scheme_dir = tmp_path / "schemes"
    scheme_dir.mkdir()
    pd.DataFrame({"market_id": ["m1"], "slice": ["all"]}).to_parquet(
        scheme_dir / "scheme_present.parquet"
    )

    with pytest.raises(FileNotFoundError, match="missing"):
        select_scheme_files(str(tmp_path), ["present", "missing"])


def test_select_scheme_files_returns_only_requested_names(tmp_path: Path) -> None:
    scheme_dir = tmp_path / "schemes"
    scheme_dir.mkdir()
    frame = pd.DataFrame({"market_id": ["m1"], "slice": ["all"]})
    frame.to_parquet(scheme_dir / "scheme_first.parquet")
    frame.to_parquet(scheme_dir / "scheme_second.parquet")

    selected = select_scheme_files(str(tmp_path), ["second"])

    assert [Path(path).name for path in selected] == ["scheme_second.parquet"]
