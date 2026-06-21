"""Regression harness for the Kalshi normalizer.

Loads assertions from `kalshi_harness_assertions.json` and runs three kinds:

  * collapse — all items in a group must normalize to the SAME
    (event_template, market_template) pair.
  * distinguish — all items in a group must normalize to DIFFERENT pairs.
  * edge_cases — assertions that the normalizer doesn't crash and emits a
    reasonable template (no_substitution_error: input != empty, no error).
  * expected_improvements — non-blocking; reported but does not fail.

Usage:
    python kalshi_harness.py
Exit code 0 if all blocking assertions pass.
"""
import json
import sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from kalshi_normalize import normalize, ticker_prefix_of

ASSERT = HERE / "kalshi_harness_assertions.json"


def pair(item):
    pfx = ticker_prefix_of(item["ticker"])
    mt = normalize(item["question"], pfx)
    return (pfx, mt)


def run_collapse(group):
    pairs = [pair(it) for it in group["items"]]
    distinct = set(pairs)
    if len(distinct) == 1:
        return True, f"all {len(pairs)} collapsed → {list(distinct)[0]}"
    return False, f"FAILED — got {len(distinct)} distinct pairs:\n      " + \
                  "\n      ".join(f"{p}  ← {it['ticker']}" for p, it in zip(pairs, group['items']))


def run_distinguish(group):
    pairs = [pair(it) for it in group["items"]]
    if len(set(pairs)) == len(pairs):
        return True, f"all {len(pairs)} pairs distinct"
    seen = {}
    dupes = []
    for p, it in zip(pairs, group["items"]):
        if p in seen:
            dupes.append(f"{p} ← both '{seen[p]}' and '{it['ticker']}'")
        else:
            seen[p] = it["ticker"]
    return False, "FAILED — collisions:\n      " + "\n      ".join(dupes)


def run_edge_case(group):
    kind = group.get("kind", "no_substitution_error")
    if kind == "no_substitution_error":
        for it in group["items"]:
            try:
                _, mt = pair(it)
                if mt is None:
                    return False, f"FAILED on {it['ticker']} — got None"
            except Exception as e:
                return False, f"FAILED on {it['ticker']} — {type(e).__name__}: {e}"
        return True, f"all {len(group['items'])} items processed without error"
    return True, f"(unknown kind '{kind}' — skipped)"


def main():
    data = json.loads(ASSERT.read_text())
    blocking = 0
    blocking_pass = 0
    flagged = 0

    print("=== COLLAPSE ===")
    for g in data["collapse"]:
        blocking += 1
        ok, msg = run_collapse(g)
        blocking_pass += int(ok)
        mark = "✓" if ok else "✗"
        print(f"  {mark} {g['name']}: {msg}")

    print("\n=== DISTINGUISH ===")
    for g in data["distinguish"]:
        blocking += 1
        ok, msg = run_distinguish(g)
        blocking_pass += int(ok)
        mark = "✓" if ok else "✗"
        print(f"  {mark} {g['name']}: {msg}")

    print("\n=== EDGE CASES ===")
    for g in data["edge_cases"]:
        blocking += 1
        ok, msg = run_edge_case(g)
        blocking_pass += int(ok)
        mark = "✓" if ok else "✗"
        print(f"  {mark} {g['name']}: {msg}")

    print("\n=== EXPECTED IMPROVEMENTS (non-blocking) ===")
    for g in data.get("expected_improvements", []):
        flagged += 1
        # Most expected_improvements are stated as collapse desires that
        # currently DON'T pass — report current behavior.
        pairs = [pair(it) for it in g["items"]]
        distinct = set(pairs)
        status = "currently collapses" if len(distinct) == 1 else f"currently {len(distinct)} distinct"
        print(f"  • {g['name']}: {status}")

    print(f"\n=== SUMMARY ===  {blocking_pass}/{blocking} blocking pass, {flagged} expected-improvement flags")
    if blocking_pass < blocking:
        sys.exit(1)


if __name__ == "__main__":
    main()
