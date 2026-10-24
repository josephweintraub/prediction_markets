#!/usr/bin/env python3
"""Hash explicit file selectors into a research-handoff build plan.

The source root must be a screened, exported source snapshot, not a checkout
discovered by this utility. No directories are searched and no archives are
opened. Selection schema (no additional keys)::

    {"release_name": "joseph_code_v1", "source_files": ["README.md"],
     "extra_files": [{"root": "artifacts", "path": "study/report.tex",
                      "destination": "studies/report.tex", "role": "report"}]}

Every source_files entry maps to code/<path>, with role source. Extra files
select exactly one regular file from either source or artifacts. Existing
plans are never replaced; changing a selected body requires a fresh plan.
Evidence Parquet needs an aggregate_summary or bounded_metadata role and the
builder's size admission. Those roles assert, but do not audit, aggregate grain.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__:
    from . import build_research_handoff as builder
else:
    import build_research_handoff as builder


def _root(value: Path | str, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise builder.HandoffError(f"{label} must be an absolute directory")
    builder._no_symlinks(path)
    if not path.is_dir():
        raise builder.HandoffError(f"{label} must be a directory: {path}")
    return path


def _selected_file(root: Path, value: object) -> Path:
    relative = builder._destination(value)
    if any(character in relative for character in "*?[]"):
        raise builder.HandoffError(f"Glob selectors are not allowed: {relative!r}")
    path = root.joinpath(*relative.split("/"))
    # Keep the containment check explicit; never resolve away a symlink.
    if not path.is_relative_to(root):
        raise builder.HandoffError(f"Selected path crosses its root: {relative!r}")
    return path


def make_plan(selection: dict, source_root: Path | str, artifact_root: Path | str) -> dict:
    """Validate an explicit selection and hash only its individual files."""
    selection = builder._exact_keys(
        selection, {"release_name", "source_files", "extra_files"}, "selection"
    )
    release_name = builder._release_name(selection["release_name"])
    roots = {
        "source": _root(source_root, "source_root"),
        "artifacts": _root(artifact_root, "artifact_root"),
    }
    if not isinstance(selection["source_files"], list):
        raise builder.HandoffError("source_files must be a list")
    if not isinstance(selection["extra_files"], list):
        raise builder.HandoffError("extra_files must be a list")
    entries = []
    for relative in selection["source_files"]:
        path = _selected_file(roots["source"], relative)
        entries.append({
            "source": str(path), "destination": "code/" + relative,
            "sha256": "0" * 64, "role": "source",
        })
    for index, extra in enumerate(selection["extra_files"]):
        extra = builder._exact_keys(
            extra, {"root", "path", "destination", "role"}, f"extra_files[{index}]"
        )
        if not isinstance(extra["root"], str) or extra["root"] not in roots:
            raise builder.HandoffError("extra_files root must be source or artifacts")
        path = _selected_file(roots[extra["root"]], extra["path"])
        entries.append({
            "source": str(path), "destination": extra["destination"],
            "sha256": "0" * 64, "role": extra["role"],
        })
    # Reuse the builder's strict source/destination/name/role contract before
    # reading bodies, including credential-like names and destination collisions.
    entries = builder._validate_entries(entries, plan=True)
    for entry in entries:
        path = Path(entry["source"])
        builder._no_symlinks(path)
        size = path.stat().st_size
        for selected_path in (entry["source"], entry["destination"]):
            builder.check_file_admission(selected_path, entry["role"], size)
        actual_size, entry["sha256"] = builder._fingerprint(path)
        for selected_path in (entry["source"], entry["destination"]):
            builder.check_file_admission(selected_path, entry["role"], actual_size)
    return {"release_name": release_name, "files": entries}


def write_plan(
    selection: dict, source_root: Path | str, artifact_root: Path | str,
    output: Path | str,
) -> dict:
    """Create a new JSON plan outside the read-only source snapshot."""
    source_root = _root(source_root, "source_root")
    output = Path(output).absolute()
    builder._no_symlinks(output.parent)
    if output.is_relative_to(source_root):
        raise builder.HandoffError("Plan output must be outside the source snapshot")
    if output.exists() or output.is_symlink():
        raise builder.HandoffError(f"Output already exists; choose a new plan: {output}")
    plan = make_plan(selection, source_root, artifact_root)
    builder._write_exclusive(output, builder._json_bytes(plan))
    return plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        with builder._open_source(args.selection.absolute()) as handle:
            selection = json.load(handle)
        plan = write_plan(selection, args.source_root, args.artifact_root, args.output)
    except (OSError, ValueError) as error:
        print(f"Handoff plan failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"release_name": plan["release_name"], "file_count": len(plan["files"]), "status": "ok"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
