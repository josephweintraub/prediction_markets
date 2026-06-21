"""Exhaustive audit of all 7,153 prefixes to classify normalization needs.

For each prefix:
  - n_tickers (total contracts in family)
  - n_templates (distinct normalized question shapes after Stage 0)
  - n_templates / n_tickers (variation ratio — high means each ticker has its
    own template, suggesting entity names not collapsed)
  - sample of templates (for pattern recognition)

Classification:
  * SINGLE_TEMPLATE     — exactly 1 template; no abstraction-signal need
  * NEEDS_NORM_SPORTS   — multiple templates following a known sports-prop
                          pattern (player/team in fixed positions) → regex
                          extension would collapse them
  * NEEDS_NORM_OTHER    — multiple templates with entity variation that don't
                          match the known sports patterns; flagged for manual
                          inspection
  * HETEROGENEOUS       — multiple templates with no apparent shared structure
                          (multi-purpose prefix); accept individual LLM calls
  * LOW_VOLUME_TAIL     — 1-5 templates AND ≤5 tickers; long-tail markets

Output: kalshi_prefix_audit.csv  +  kalshi_prefix_audit.md  (top 100 list with
classification + reasoning)
"""
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

SRC = Path("/mnt/data/kalshi/kalshi_templates.parquet")
PER_TICKER = Path("/mnt/data/kalshi/kalshi_per_ticker.parquet")
OUT_CSV = Path("/home/ubuntu/kalshi/kalshi_prefix_audit.csv")
OUT_MD  = Path("/home/ubuntu/kalshi/kalshi_prefix_audit.md")
OUT_SAMPLES_JSON = Path("/home/ubuntu/kalshi/kalshi_prefix_samples.json")

# Known sports-prop patterns already identified (for classification).
# Format: (label, prefix-set, regex-on-template).
KNOWN_PATTERNS = [
    ("nhl_goal",          {"KXNHLGOAL"},
     re.compile(r"^.+ at .+: Anytime Goal: .+$")),
    ("nhl_first_goal",    {"KXNHLFIRSTGOAL"},
     re.compile(r"^.+ at .+: First Goal: .+$")),
    ("nfl_anytd",         {"KXNFLANYTD"},
     re.compile(r"^.+ at .+: Anytime Touchdown Scorer: .+$")),
    ("nfl_firsttd",       {"KXNFLFIRSTTD"},
     re.compile(r"^.+ at .+: First Touchdown Scorer: .+$")),
    ("ncaa_total",        {"KXNCAAMBTOTAL", "KXNCAAWBTOTAL"},
     re.compile(r"^.+ at .+: Total Points$")),
    ("ncaa_game",         {"KXNCAAMBGAME", "KXNCAAWBGAME"},
     re.compile(r"^.+ at .+ Winner\??$")),
    ("ncaa_spread",       {"KXNCAAMBSPREAD", "KXNCAAWBSPREAD"},
     re.compile(r"^.+ wins by over <NUM> Points\??$")),
    ("nhl_pts",           {"KXNHLPTS"},
     re.compile(r"^.+ records <NUM>\+ points$")),
    ("nhl_ast",           {"KXNHLAST"},
     re.compile(r"^.+ records <NUM>\+ assists$")),
    ("atp_wta_match",     {"KXATPMATCH", "KXATPCHALLENGERMATCH", "KXWTAMATCH"},
     re.compile(r"^Will .+ win the .+ vs .+ match\??$")),
    ("pga_tour",          {"KXPGATOUR"},
     re.compile(r"^Will .+ win the .+\??$")),
]


def main():
    print(f">>> loading {SRC}")
    df = pd.read_parquet(SRC)
    print(f"    {len(df):,} templates, {df['event_template'].nunique():,} prefixes")

    print(">>> aggregating per prefix")
    per_prefix = df.groupby("event_template").agg(
        n_templates=("market_template", "count"),
        n_tickers=("n_tickers", "sum"),
        n_tickers_top=("n_tickers", "max"),
    ).reset_index()
    per_prefix["variation_ratio"] = per_prefix["n_templates"] / per_prefix["n_tickers"]
    per_prefix = per_prefix.sort_values("n_tickers", ascending=False).reset_index(drop=True)

    # For each prefix, pull up to 5 sample templates (largest n_tickers within prefix)
    print(">>> sampling 5 templates per prefix")
    df_sorted = df.sort_values(["event_template", "n_tickers"], ascending=[True, False])
    sample_lookup = {}
    for prefix, sub in df_sorted.groupby("event_template", sort=False):
        sample_lookup[prefix] = sub["market_template"].head(5).tolist()
    per_prefix["sample_templates"] = per_prefix["event_template"].map(sample_lookup)

    # Classify each prefix.
    print(">>> classifying")
    def classify(row):
        pfx = row["event_template"]
        nT = row["n_templates"]
        nt = row["n_tickers"]
        samples = row["sample_templates"]

        # SINGLE_TEMPLATE: trivially handled
        if nT == 1:
            return "SINGLE_TEMPLATE"

        # Known sports-prop pattern hit (regex matches the majority sample)
        for label, prefixes, rgx in KNOWN_PATTERNS:
            if pfx in prefixes:
                # confirm at least one of the top-5 samples matches
                if any(rgx.match(t or "") for t in samples):
                    return f"NEEDS_NORM_SPORTS:{label}"

        # Low-volume long-tail
        if nT <= 5 and nt <= 10:
            return "LOW_VOLUME_TAIL"

        # Heuristic: are the templates structurally similar?
        # Take the first 30 chars of each and see how diverse they are.
        prefixes_of_templates = {(t or "")[:30] for t in samples}
        if len(prefixes_of_templates) == 1:
            return "STRUCTURED_SIMILAR_BUT_NOT_KNOWN_SPORTS"  # candidates for new patterns
        # All templates start the same way → structured; mark for inspection
        if len({(t or "")[:15] for t in samples}) <= 2:
            return "PARTIALLY_STRUCTURED"
        return "HETEROGENEOUS"

    per_prefix["class"] = per_prefix.apply(classify, axis=1)
    class_counts = per_prefix["class"].value_counts()
    print("\n=== CLASSIFICATION COUNTS ===")
    for cls, n in class_counts.items():
        nticks = per_prefix.loc[per_prefix["class"] == cls, "n_tickers"].sum()
        print(f"  {cls:50s} {n:>5,} prefixes  covering {nticks:>10,} tickers")

    # Save CSV (without sample_templates list for readability)
    csv_out = per_prefix.copy()
    csv_out["sample_template_1"] = csv_out["sample_templates"].apply(lambda l: l[0] if l else "")
    csv_out["sample_template_2"] = csv_out["sample_templates"].apply(lambda l: l[1] if len(l)>1 else "")
    csv_out["sample_template_3"] = csv_out["sample_templates"].apply(lambda l: l[2] if len(l)>2 else "")
    csv_out["sample_template_4"] = csv_out["sample_templates"].apply(lambda l: l[3] if len(l)>3 else "")
    csv_out["sample_template_5"] = csv_out["sample_templates"].apply(lambda l: l[4] if len(l)>4 else "")
    csv_out.drop(columns=["sample_templates"]).to_csv(OUT_CSV, index=False)
    print(f"\n>>> wrote {OUT_CSV} ({OUT_CSV.stat().st_size/1024:.0f} KB)")

    # Save JSON with all samples for downstream pattern inspection
    import json
    payload = []
    for _, r in per_prefix.iterrows():
        payload.append({
            "prefix": r["event_template"],
            "n_tickers": int(r["n_tickers"]),
            "n_templates": int(r["n_templates"]),
            "variation_ratio": float(r["variation_ratio"]),
            "class": r["class"],
            "samples": r["sample_templates"],
        })
    OUT_SAMPLES_JSON.write_text(json.dumps(payload, default=str, indent=2))
    print(f">>> wrote {OUT_SAMPLES_JSON} ({OUT_SAMPLES_JSON.stat().st_size/1024:.0f} KB)")

    # Write the top-100 markdown report focused on the most impactful prefixes:
    # SORT by (class priority * n_tickers).
    class_priority = {
        "NEEDS_NORM_SPORTS": 0,
        "STRUCTURED_SIMILAR_BUT_NOT_KNOWN_SPORTS": 1,
        "PARTIALLY_STRUCTURED": 2,
        "HETEROGENEOUS": 3,
        "SINGLE_TEMPLATE": 4,
        "LOW_VOLUME_TAIL": 5,
    }
    def cls_pri(c):
        for k in class_priority:
            if c.startswith(k): return class_priority[k]
        return 99
    per_prefix["pri"] = per_prefix["class"].apply(cls_pri)
    sorted_pref = per_prefix.sort_values(["pri", "n_tickers"], ascending=[True, False])

    lines = []
    lines.append("# Kalshi Prefix Audit — Normalization Classification\n")
    lines.append("## Class counts\n")
    lines.append("| Class | Prefixes | Tickers |")
    lines.append("|---|---:|---:|")
    for cls in class_counts.index:
        n = int(class_counts[cls])
        nticks = int(per_prefix.loc[per_prefix['class']==cls, 'n_tickers'].sum())
        lines.append(f"| {cls} | {n:,} | {nticks:,} |")
    lines.append("")
    lines.append("## Top 100 prefixes by class priority\n")
    lines.append("| # | Prefix | n_tickers | n_templates | Class | Top 3 templates |")
    lines.append("|---|---|---:|---:|---|---|")
    for i, r in enumerate(sorted_pref.head(100).itertuples(index=False), 1):
        samples = r.sample_templates or []
        sample_summary = " ¦ ".join((s or "")[:60] for s in samples[:3])
        sample_summary = sample_summary.replace("|", "\\|")
        lines.append(f"| {i} | {r.event_template} | {r.n_tickers:,} | {r.n_templates:,} | {r._5} | {sample_summary} |")
    OUT_MD.write_text("\n".join(lines))
    print(f">>> wrote {OUT_MD} ({OUT_MD.stat().st_size/1024:.0f} KB)")


if __name__ == "__main__":
    main()
