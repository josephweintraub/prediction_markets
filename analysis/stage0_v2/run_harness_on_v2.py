"""Run regression harness against stage0_v2 (Phase A) normalizer."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import importlib, stage0_v2, harness
importlib.reload(stage0_v2)
importlib.reload(harness)
from stage0_v2 import normalize
from harness import run_harness

r = run_harness(normalize)
print(r.report())

if r.invariant_failures:
    print("\n\n=== DETAILED INVARIANT FAILURES (regression risk) ===")
    for f in r.invariant_failures:
        print(f"\n[{f['name']}] {f['kind']}: {f['reason']}")
        for s, t in f.get("witnesses", [])[:6]:
            print(f"  {s}")
            print(f"    -> {t}")
