"""Regression harness for the Stage 0 normalizer.

Two kinds of assertions:
  COLLAPSE:    a list of slugs must all produce the same template
  DISTINGUISH: a list of slugs must all produce DIFFERENT templates

Plus an EXPECTED_IMPROVEMENTS category for groupings the current normalizer
gets wrong that Phase A is intended to fix.

Usage:
    from harness import run_harness
    result = run_harness(normalize_fn)
    print(result.report)
    assert result.invariant_passes == result.invariant_total
"""
from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path

ASSERTIONS_PATH = Path(__file__).parent / "harness_assertions.json"


@dataclass
class Result:
    invariant_total: int = 0
    invariant_passes: int = 0
    invariant_failures: list[dict] = field(default_factory=list)
    improvement_total: int = 0
    improvement_passes: int = 0
    improvement_details: list[dict] = field(default_factory=list)

    @property
    def all_invariants_pass(self) -> bool:
        return self.invariant_passes == self.invariant_total

    def report(self) -> str:
        lines = []
        lines.append("=" * 70)
        lines.append(f"INVARIANTS:    {self.invariant_passes}/{self.invariant_total} passing")
        if self.invariant_failures:
            lines.append(f"  [!] {len(self.invariant_failures)} INVARIANT FAILURES (regression):")
            for f in self.invariant_failures:
                lines.append(f"    - [{f['name']}] {f['reason']}")
                for s, t in f.get("witnesses", [])[:4]:
                    lines.append(f"        {s}")
                    lines.append(f"          -> {t}")
        lines.append(f"\nIMPROVEMENTS:  {self.improvement_passes}/{self.improvement_total} now passing")
        for d in self.improvement_details:
            status = "PASS" if d["pass"] else "FAIL"
            lines.append(f"  [{status}] {d['name']}")
        lines.append("=" * 70)
        return "\n".join(lines)


def _check_collapse(slugs, normalize_fn) -> tuple[bool, str, list]:
    tmpls = [(s, normalize_fn(s)) for s in slugs]
    distinct = set(t for _, t in tmpls)
    if len(distinct) == 1:
        return True, "all collapsed", tmpls
    return False, f"expected 1 template, got {len(distinct)}", tmpls


def _check_distinguish(slugs, min_distinct, normalize_fn) -> tuple[bool, str, list]:
    tmpls = [(s, normalize_fn(s)) for s in slugs]
    distinct = set(t for _, t in tmpls)
    if len(distinct) >= min_distinct:
        return True, f"{len(distinct)} distinct (need {min_distinct})", tmpls
    return False, f"expected >={min_distinct} distinct, got {len(distinct)}", tmpls


def _check_improvement(item, normalize_fn) -> tuple[bool, str]:
    kind = item.get("kind", "collapse")
    if kind == "collapse":
        ok, reason, _ = _check_collapse(item["slugs"], normalize_fn)
        return ok, reason
    elif kind == "collapse_after":
        # All slugs should map to the same template
        ok, reason, _ = _check_collapse(item["slugs"], normalize_fn)
        return ok, reason
    elif kind == "collapse_namespace":
        # val_slugs + valorant_slugs should all produce the same template
        all_slugs = item["val_slugs"] + item["valorant_slugs"]
        ok, reason, _ = _check_collapse(all_slugs, normalize_fn)
        return ok, reason
    elif kind == "distinguish_groups":
        # group_a slugs all collapse to one template; group_b slugs all collapse to a different template
        a_tmpls = set(normalize_fn(s) for s in item["group_a"])
        b_tmpls = set(normalize_fn(s) for s in item["group_b"])
        if len(a_tmpls) == 1 and len(b_tmpls) == 1 and a_tmpls != b_tmpls:
            return True, "groups distinct and internally collapsed"
        return False, f"group_a={a_tmpls}, group_b={b_tmpls}"
    elif kind == "no_substitution":
        forbidden = item["forbidden_substring"]
        for s in item["slugs"]:
            t = normalize_fn(s)
            if forbidden in t:
                return False, f"slug '{s}' produced template containing '{forbidden}': '{t}'"
        return True, "no slug contains forbidden substring"
    elif kind == "each_contains":
        required = item["must_contain"]
        for s in item["slugs"]:
            t = normalize_fn(s)
            if required not in t:
                return False, f"slug '{s}' produced '{t}' which lacks '{required}'"
        return True, f"all slugs contain '{required}'"
    else:
        return False, f"unknown improvement kind: {kind}"


def run_harness(normalize_fn, assertions_path=ASSERTIONS_PATH) -> Result:
    """Run all assertions against a candidate normalizer.

    `normalize_fn(slug: str) -> str` should return the template for a raw slug.
    """
    with open(assertions_path) as f:
        assertions = json.load(f)

    r = Result()

    # COLLAPSE invariants
    for item in assertions["collapse"]:
        r.invariant_total += 1
        ok, reason, tmpls = _check_collapse(item["slugs"], normalize_fn)
        if ok:
            r.invariant_passes += 1
        else:
            r.invariant_failures.append({
                "name": item["name"],
                "kind": "collapse",
                "reason": reason,
                "witnesses": tmpls,
            })

    # DISTINGUISH invariants
    for item in assertions["distinguish"]:
        r.invariant_total += 1
        ok, reason, tmpls = _check_distinguish(item["slugs"], item["min_distinct"], normalize_fn)
        if ok:
            r.invariant_passes += 1
        else:
            r.invariant_failures.append({
                "name": item["name"],
                "kind": "distinguish",
                "reason": reason,
                "witnesses": tmpls,
            })

    # EXPECTED IMPROVEMENTS (not invariants — failures here are tracked but not blocking)
    for item in assertions["expected_improvements"]:
        r.improvement_total += 1
        ok, reason = _check_improvement(item, normalize_fn)
        if ok:
            r.improvement_passes += 1
        r.improvement_details.append({
            "name": item["name"],
            "pass": ok,
            "reason": reason,
        })

    return r


if __name__ == "__main__":
    # Smoke test: run against the current lookup-based normalizer
    import pandas as pd
    print("Loading current normalizer as lookup table...")
    df = pd.read_parquet(
        "/Users/josephweintraub/prediction_markets/analysis/output/stage2_per_contract.parquet",
        columns=["market_slug", "market_template"]
    ).drop_duplicates("market_slug").set_index("market_slug")["market_template"]

    def current_normalizer(slug: str) -> str:
        return df.get(slug, f"<UNKNOWN:{slug}>")

    r = run_harness(current_normalizer)
    print(r.report())
