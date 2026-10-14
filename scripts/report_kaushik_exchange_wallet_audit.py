#!/usr/bin/env python3
"""Render accepted exchange-wallet count evidence; never fit estimates or read trades."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re

EXCHANGES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
DIAGNOSTIC = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
POLICY = {
    "policy": "exclude_source_exchange_wallets", "addresses": list(EXCHANGES),
    "fields": ["proxyWallet", "counterparty"], "comparison": "lowercase_exact_no_trim",
    "operator": "either", "null_blank_match": False,
    "diagnostic_only_addresses": [DIAGNOSTIC],
    "unsupported_eligible_match_blocks_admission": True,
    "emitting_contract_filter": False, "other_filters_added": False,
}
SCOPES = ("full", "cutoff")
METRICS = ("baseline", "tails", "common_duration", "lifespan_gt1", "remaining_gt1")
LABELS = ("All price bins", "D1 and D10", "Common duration sample",
          "Original lifespan $>1$ day", "Remaining maturity $>1$ day")
PARTS = ("original", "removed", "retained")
ROLES = ("all_roles", "primary_taker", "all_buy", "maker_buy", "taker_buy")
MONTHS = tuple(f"{year}-{month:02d}" for year in range(2022, 2027)
               for month in range(1, 13) if "2022-11" <= f"{year}-{month:02d}" <= "2026-06")
CHECKS = ("source_mask_locator_and_fields_exact", "reference_multiset_partition",
          "BUY_role_partition", "source_inputs_reopened_unchanged", "unsupported_eligible_matches_absent")
MAX_JSON_BYTES = 16_000_000


def require(condition, message):
    if not condition:
        raise ValueError("exchange-wallet report blocked: " + message)


def count(value):
    require(type(value) is int and value >= 0, "counts must be nonnegative integers")
    return value


def read_json(path):
    path = Path(path)
    require(path.is_file() and path.stat().st_size <= MAX_JSON_BYTES, "missing or oversized JSON")
    raw = path.read_bytes()
    require(len(raw) <= MAX_JSON_BYTES, "JSON grew beyond read ceiling")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=unique,
                       parse_constant=lambda value: require(False, "nonfinite JSON value"))
    require(isinstance(value, dict), "JSON object required")
    return value, {"path": str(path.resolve()), "bytes": len(raw),
                   "sha256": hashlib.sha256(raw).hexdigest()}


def validate(audit, acceptance, audit_sha256):
    require(audit.get("schema_version") == "kaushik_exchange_wallet_audit_v1" and
            audit.get("status") == "exchange_wallet_audit_complete" and
            audit.get("scientific_admission") is True, "audit is not scientifically admitted")
    require(acceptance.get("schema_version") == "kaushik_exchange_wallet_acceptance_v1" and
            acceptance.get("status") == "exchange_wallet_masks_reopened_accepted" and
            acceptance.get("scientific_admission") is True and
            acceptance.get("all_masks_reopened") is True, "acceptance is incomplete")
    require(re.fullmatch(r"[0-9a-f]{64}", audit_sha256 or "") and
            acceptance.get("audit_sha256") == audit_sha256, "audit SHA differs from acceptance")
    require(all(json.dumps(item.get("policy"), sort_keys=True) == json.dumps(POLICY, sort_keys=True)
                for item in (audit, acceptance)),
            "exact four-address/two-wallet exclusion policy differs")
    head = audit.get("source", {}).get("head")
    require(re.fullmatch(r"[0-9a-f]{40}", head or "") and acceptance.get("source_head") == head,
            "accepted source HEAD differs")
    require(all(audit.get("checks", {}).get(key) is True for key in CHECKS), "saved gates are incomplete")
    require(json.dumps(audit.get("totals"), sort_keys=True) ==
            json.dumps(acceptance.get("totals"), sort_keys=True), "acceptance count totals differ")
    require(set(audit.get("totals", {})) == set(SCOPES) and
            set(audit.get("role_totals", {})) == set(SCOPES), "scope grid differs")
    months = audit.get("months", [])
    require(isinstance(months, list) and [m.get("month") for m in months] == list(MONTHS),
            "complete ordered 44-month evidence required")
    groups = []
    raw_known = {scope: 0 for scope in SCOPES}
    for month in months:
        require(month.get("EXCEPT_ALL_differences") == [0, 0] and
                all(type(value) is int for value in month["EXCEPT_ALL_differences"]) and
                type(month.get("unsupported_eligible_rows")) is int and
                month["unsupported_eligible_rows"] == 0, "monthly membership gate differs")
        eligible = month.get("eligible_groups")
        raw = month.get("raw_groups")
        require(isinstance(eligible, list) and isinstance(raw, list), "monthly support groups missing")
        for group in eligible:
            require(group.get("side") in ("BUY", "SELL") and type(group.get("is_maker")) is bool and
                    type(group.get("removed")) is bool and group.get("unsupported") is False,
                    "eligible role/action/membership differs")
            addresses = (group.get("matched_proxy_address"), group.get("matched_counterparty_address"))
            require(all(a is None or a in EXCHANGES for a in addresses) and
                    group["removed"] == any(a in EXCHANGES for a in addresses),
                    "eligible address/membership differs")
            for metric in METRICS:
                require(count(group["cutoff_" + metric]) <= count(group["full_" + metric]),
                        "cutoff support exceeds full support")
            for scope in SCOPES:
                values = [count(group[scope + "_" + metric]) for metric in METRICS]
                require(values[0] >= values[1] >= values[2] >= max(values[3:]), "metric nesting differs")
        for group in raw:
            addresses = (group.get("matched_proxy_address"), group.get("matched_counterparty_address"))
            require(any(a is not None for a in addresses) and
                    all(a is None or a in (*EXCHANGES, DIAGNOSTIC) for a in addresses),
                    "raw address support differs")
            require(count(group["cutoff_rows"]) <= count(group["full_rows"]), "raw cutoff nesting differs")
        for scope in SCOPES:
            summary = month.get("raw_minus_eligible", {}).get(scope, {})
            require(set(summary) == {"raw_matched", "eligible_matched", "noneligible_matched"},
                    "raw/eligible count grid differs")
            for value in summary.values():
                count(value)
            raw_n = sum(count(g[scope + "_rows"]) for g in raw)
            eligible_n = sum(count(g[scope + "_baseline"]) for g in eligible if g["removed"])
            known_n = sum(count(g[scope + "_rows"]) for g in raw if
                          g.get("matched_proxy_address") in EXCHANGES or g.get("matched_counterparty_address") in EXCHANGES)
            require(summary == {"raw_matched": raw_n, "eligible_matched": eligible_n,
                                "noneligible_matched": raw_n - eligible_n} and known_n >= eligible_n,
                    "raw/eligible distinct-record counts differ")
            raw_known[scope] += known_n
            require(set(month.get("counts", {}).get(scope, {})) == set(METRICS), "monthly metric grid differs")
            for metric in METRICS:
                expected = {part: sum(count(g[scope + "_" + metric]) for g in eligible if
                            part == "original" or g["removed"] == (part == "removed")) for part in PARTS}
                require(month["counts"][scope][metric] == expected, "monthly count totals differ")
                for value in month["counts"][scope][metric].values():
                    count(value)
        require(count(month.get("mask_rows")) == sum(g["full_rows"] for g in raw), "mask/raw row count differs")
        groups.extend(eligible)
    for scope in SCOPES:
        require(set(audit["totals"][scope]) == set(METRICS) and
                set(audit["role_totals"][scope]) == set(METRICS), "metric grid differs")
        for metric in METRICS:
            require(set(audit["totals"][scope][metric]) == set(PARTS) and
                    set(audit["role_totals"][scope][metric]) == set(PARTS), "count partition grid differs")
            for part in PARTS:
                selected = [g for g in groups if part == "original" or g["removed"] == (part == "removed")]
                predicates = (lambda g: True, lambda g: not g["is_maker"], lambda g: g["side"] == "BUY",
                              lambda g: g["side"] == "BUY" and g["is_maker"],
                              lambda g: g["side"] == "BUY" and not g["is_maker"])
                roles = {role: sum(count(g[scope + "_" + metric]) for g in selected if predicate(g))
                         for role, predicate in zip(ROLES, predicates)}
                require(audit["role_totals"][scope][metric][part] == roles and
                        count(audit["totals"][scope][metric][part]) == roles["all_roles"],
                        "global totals or role partitions differ")
                for value in audit["role_totals"][scope][metric][part].values():
                    count(value)
            for role in ROLES:
                values = audit["role_totals"][scope][metric]
                require(values["original"][role] == values["retained"][role] + values["removed"][role],
                        "original/removed/retained role identity differs")
    require(set(audit.get("exact_no_change", {})) == set(SCOPES) and
            all(audit["exact_no_change"][s] is (audit["totals"][s]["baseline"]["removed"] == 0) for s in SCOPES),
            "saved no-change flag differs")
    return raw_known


def render_source(audit, acceptance, audit_sha256):
    raw_known = validate(audit, acceptance, audit_sha256)
    unchanged = all(audit["exact_no_change"].values())
    result_note = (
        "No eligible archive records are removed in either scope. With source, model and provider bindings held fixed, "
        "this exclusion leaves the earlier estimates unchanged; no new estimates were fitted."
        if unchanged else
        "The exclusion changes eligible archive membership. Affected estimates require refitting; "
        "this count audit is not a regression rerun."
    )
    rows = []
    for scope, label in (("full", "All available dates"), ("cutoff", "Before 25 March 2026 UTC")):
        rows.append(r"\multicolumn{4}{l}{\textit{" + label + r"}} \\")
        for metric, label in zip(METRICS, LABELS):
            values = audit["role_totals"][scope][metric]
            rows.append(" & ".join([label, *(f"{values[part]['primary_taker']:,}" for part in PARTS)]) + r" \\")
        if scope == "full":
            rows.append(r"\addlinespace")
    evidence_rows = []
    for label, getter in (
        ("Four-address raw source matches", lambda s: raw_known[s]),
        ("Already ineligible source matches", lambda s: raw_known[s] - audit["totals"][s]["baseline"]["removed"]),
        ("Eligible all-role removals", lambda s: audit["totals"][s]["baseline"]["removed"]),
    ):
        evidence_rows.append(" & ".join([label, *(f"{getter(s):,}" for s in SCOPES)]) + r" \\")
    return r"""\documentclass[10pt]{article}
\usepackage[margin=0.8in]{geometry}
\usepackage{booktabs}
\pagestyle{empty}
\setlength{\parindent}{0pt}
\setlength{\parskip}{5pt}
\begin{document}
{\Large Exchange-wallet exclusion: sample comparison}\par
Repaired CLEAN archive records, November 2022--June 2026; the cutoff scope uses $t<25$ March 2026, 00:00 UTC.

Exclude a record when either source wallet, \texttt{proxyWallet} or \texttt{counterparty}, exactly matches one of four reviewed legacy/V2 exchange addresses after lowercasing. No emitting-contract or additional filter applies. Kaushik's exact address list was not supplied. Counts preserve archive multiplicity; they are not unique native fills.

""" + result_note + r"""

\smallskip
\textbf{Primary taker-role observations}\par
\begin{center}
\begin{tabular}{lrrr}
\toprule
Sample & Original & Removed & Retained \\
\midrule
""" + "\n".join(rows) + r"""
\bottomrule
\end{tabular}
\end{center}
{\footnotesize D1 and D10 are the fixed price bins $(0,.10)$ and $[.90,1)$. The common duration sample has valid native creation/end-date clocks and includes these tails. The two one-day restrictions apply separately to that common sample. BUY claims are retained and recorded SELL claims are complemented under the existing archive convention.}

{\footnotesize Source: accepted saved counts from \texttt{scripts/audit\_kaushik\_exchange\_wallets.py}, bound by its audit and acceptance JSONs. No trade data are read by this renderer.}

\medskip
\textbf{Source and role reconciliation}\par
\begin{center}
\begin{tabular}{lrr}
\toprule
Records & All dates & Before cutoff \\
\midrule
""" + "\n".join(evidence_rows) + r"""
\bottomrule
\end{tabular}
\end{center}
{\footnotesize Each matching source record is counted once even when both wallet fields match. Raw counts include records already excluded by the original eligibility rules; eligible all-role removals include maker and taker representations. Diagnostic-only addresses do not enter these counts or the exclusion.}
\end{document}
"""


def build_report(audit_json, acceptance_json, output_dir):
    output_dir = Path(output_dir)
    require(not output_dir.exists(), "immutable output directory already exists")
    audit, audit_id = read_json(audit_json)
    acceptance, acceptance_id = read_json(acceptance_json)
    source = render_source(audit, acceptance, audit_id["sha256"]).encode("utf-8")
    manifest = {
        "schema_version": "kaushik_exchange_wallet_report_v1", "status": "portable_source_complete",
        "inputs": {"audit": audit_id, "acceptance": acceptance_id},
        "renderer": {"path": "scripts/report_kaushik_exchange_wallet_audit.py",
                     "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        "output": {"name": "exchange_wallet_comparison.tex", "bytes": len(source),
                   "sha256": hashlib.sha256(source).hexdigest()},
        "estimates_fitted": False, "compiled_or_visually_verified": False,
    }
    manifest_raw = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    require(len(source) + len(manifest_raw) <= MAX_JSON_BYTES, "report exceeds output ceiling")
    output_dir.mkdir(parents=True, exist_ok=False)
    for name, raw in (("exchange_wallet_comparison.tex", source), ("manifest.json", manifest_raw)):
        with (output_dir / name).open("xb") as stream:
            stream.write(raw)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-json", type=Path, required=True)
    parser.add_argument("--acceptance-json", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_report(args.audit_json, args.acceptance_json, args.output_dir)
    print(json.dumps({"status": result["status"], "output_dir": str(args.output_dir.resolve())}))


if __name__ == "__main__":
    main()
