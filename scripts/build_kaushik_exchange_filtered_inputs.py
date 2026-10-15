#!/usr/bin/env python3
"""Admit and publish logical exchange-filtered inputs without copying a base."""
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
    parser.add_argument("--parent-dir", type=Path, required=True)
    parser.add_argument("--parent-manifest-sha256", required=True)
    parser.add_argument("--parent-acceptance-sha256", required=True)
    parser.add_argument("--mask-audit-dir", type=Path, required=True)
    parser.add_argument("--mask-audit-sha256", required=True)
    parser.add_argument("--mask-acceptance-sha256", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--preflight-dir", type=Path)
    parser.add_argument("--reviewed-preflight", type=Path)
    parser.add_argument("--reviewed-preflight-sha256")
    args = parser.parse_args(argv)
    from production_guard import require_production_host
    require_production_host()
    from analysis.kaushik_polymarket_replication import filtered_inputs as filtered
    source = filtered.source_snapshot(args.expected_head)
    policy = filtered.make_wallet_filter(args.parent_dir, args.parent_manifest_sha256, args.parent_acceptance_sha256,
        args.mask_audit_dir, args.mask_audit_sha256, args.mask_acceptance_sha256)
    fresh = filtered.preflight(policy, args.run_dir, source)
    if args.preflight_dir:
        filtered.require(not args.reviewed_preflight and not args.reviewed_preflight_sha256,
                         "preflight and body modes are exclusive")
        filtered.inputs.validate_destination(args.preflight_dir, [args.parent_dir, args.mask_audit_dir])
        args.preflight_dir.mkdir(exist_ok=False)
        filtered.inputs.write_json(args.preflight_dir / "manifest.json", fresh)
        print(json.dumps({"status": "preflight_complete", "manifest": str(args.preflight_dir / "manifest.json")}))
        return 0
    filtered.require(args.reviewed_preflight and args.reviewed_preflight_sha256, "body requires reviewed preflight")
    reviewed, identity = filtered.inputs.read_json(args.reviewed_preflight, args.reviewed_preflight_sha256)
    result = filtered.build_stage(reviewed, fresh, [sys.executable, *sys.argv], identity)
    print(json.dumps({"status": result["status"], "run_dir": str(args.run_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
