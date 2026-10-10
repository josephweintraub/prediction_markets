#!/usr/bin/env python3
"""Produce guarded immutable September 28 Polymarket replication estimates."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--base-manifest-sha256", required=True)
    parser.add_argument("--base-acceptance-sha256", required=True)
    parser.add_argument("--sports-binding", type=Path, required=True)
    parser.add_argument("--sports-binding-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--preflight-dir", type=Path)
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    parser.add_argument("--sports-metadata-only", action="store_true", help="prove provider map against claims without reading trade bodies")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()
    from analysis.kaushik_polymarket_replication import run_estimates as runner
    from analysis.kaushik_polymarket_replication import build_inputs as inputs
    fresh = runner.preflight(args.base_dir, args.base_manifest_sha256, args.base_acceptance_sha256,
        args.sports_binding, args.sports_binding_sha256, args.run_dir, args.expected_head,
        sports_metadata_only=args.sports_metadata_only)
    if args.preflight_dir:
        runner.require(not args.reviewed_preflight and not args.reviewed_preflight_sha256, "preflight/body modes are exclusive")
        inputs.validate_destination(args.preflight_dir, [args.base_dir, args.sports_binding])
        args.preflight_dir.mkdir(exist_ok=False)
        inputs.write_json(args.preflight_dir / "manifest.json", fresh)
        print(json.dumps({"status": "preflight_complete", "manifest": str(args.preflight_dir / "manifest.json")}))
        return 0
    runner.require(args.reviewed_preflight and args.reviewed_preflight_sha256, "body requires separately reviewed estimator preflight")
    reviewed, reviewed_id = inputs.read_json(args.reviewed_preflight, args.reviewed_preflight_sha256)
    result = (runner.run_sports_metadata_stage if args.sports_metadata_only else runner.run_stage)(
        reviewed, fresh, [sys.executable, *sys.argv], reviewed_identity=reviewed_id)
    print(json.dumps({"status": result["status"], "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
