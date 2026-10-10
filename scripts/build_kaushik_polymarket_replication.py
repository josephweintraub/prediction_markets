#!/usr/bin/env python3
"""Build guarded immutable Polymarket inputs; no estimation or native-action claim."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from production_guard import require_production_host


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--preflight-dir", type=Path)
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    require_production_host()
    from analysis.kaushik_polymarket_replication import build_inputs as builder
    builder.require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production requires /home/ubuntu/venv/bin/python")
    fresh = builder.preflight(args.contract, args.run_dir, args.expected_head)
    if args.preflight_dir:
        builder.require(not args.reviewed_preflight and not args.reviewed_preflight_sha256, "preflight/body modes are exclusive")
        builder.validate_destination(args.preflight_dir, [args.contract, *[item["path"] for item in fresh["trades"]],
                                                         *[item["path"] for item in fresh["metadata"].values()]])
        args.preflight_dir.mkdir(exist_ok=False)
        builder.write_json(args.preflight_dir / "manifest.json", fresh)
        print(json.dumps({"status": "preflight_complete", "manifest": str(args.preflight_dir / "manifest.json")}))
        return 0
    builder.require(args.reviewed_preflight and args.reviewed_preflight_sha256, "body requires the separately reviewed preflight binding")
    reviewed, reviewed_identity = builder.read_json(args.reviewed_preflight, args.reviewed_preflight_sha256)
    result = builder.build_stage(reviewed, fresh, [sys.executable, *sys.argv], reviewed_identity)
    print(json.dumps({"status": result["status"], "run_dir": str(args.run_dir), "rows": result["rows"],
                      "reviewed_preflight": reviewed_identity}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
