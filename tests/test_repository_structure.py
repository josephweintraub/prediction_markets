from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).parents[1]
LIVE_MARKDOWN = [
    ROOT / "README.md",
    ROOT / "CLAUDE.md",
    ROOT / "pipeline" / "README.md",
    ROOT / "analysis" / "calibration_heterogeneity" / "README.md",
    ROOT / "analysis" / "stage0_v2" / "README.md",
    *sorted((ROOT / "docs").glob("*.md")),
]


def test_expected_repository_entry_points_exist() -> None:
    expected = [
        ROOT / "docs" / "project_status.md",
        ROOT / "docs" / "methods_reference.md",
        ROOT / "docs" / "workflow.md",
        ROOT / "docs" / "decisions.md",
        ROOT / "analysis" / "calibration_heterogeneity" / "flb_engine.py",
        ROOT / "analysis" / "calibration_heterogeneity" / "run_schemes.py",
        ROOT / "pipeline" / "refresh.py",
    ]

    missing = [str(path.relative_to(ROOT)) for path in expected if not path.exists()]
    assert not missing, f"Missing repository entry points: {missing}"


def test_live_markdown_relative_links_resolve() -> None:
    broken: list[str] = []
    link_pattern = re.compile(r"\[[^]]*\]\(([^)]+)\)")
    for document in LIVE_MARKDOWN:
        text = document.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(text):
            target = raw_target.split("#", 1)[0]
            if not target or "://" in target or target.startswith("mailto:"):
                continue
            resolved = (document.parent / target).resolve()
            if not resolved.exists():
                broken.append(f"{document.relative_to(ROOT)} -> {raw_target}")

    assert not broken, "Broken live-document links:\n" + "\n".join(broken)


def test_no_live_rpc_credentials_are_committed() -> None:
    credential_pattern = re.compile(
        rb"polygon-mainnet[.]g[.]alchemy[.]com/v2/[A-Za-z0-9_-]{16,}"
    )
    offenders: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or ".git" in path.parts or "archive" in path.parts:
            continue
        try:
            payload = path.read_bytes()
        except OSError:
            continue
        if credential_pattern.search(payload):
            offenders.append(str(path.relative_to(ROOT)))

    assert not offenders, f"Live RPC credentials found in: {offenders}"
