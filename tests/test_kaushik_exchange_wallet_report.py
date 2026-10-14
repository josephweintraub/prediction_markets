"""Synthetic saved-evidence admission and LaTeX tests; no production summaries."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from scripts import report_kaushik_exchange_wallet_audit as report


def fixtures(removed=False):
    groups = []
    for maker, side in ((False, "BUY"), (False, "SELL"), (True, "BUY"), (True, "SELL")):
        for match in ((False, True) if removed else (False,)):
            group = {"side": side, "is_maker": maker, "removed": match, "unsupported": False,
                     "matched_proxy_address": report.EXCHANGES[0] if match else None,
                     "matched_counterparty_address": report.EXCHANGES[0] if match else None}
            for scope, values in (("full", (1200, 800, 600, 400, 200)), ("cutoff", (900, 500, 300, 100, 50))):
                group.update({scope + "_" + metric: value for metric, value in zip(report.METRICS, values)})
            groups.append(group)
    def totals(items):
        return {scope: {metric: {part: sum(g[scope + "_" + metric] for g in items if
                       part == "original" or g["removed"] == (part == "removed")) for part in report.PARTS}
                       for metric in report.METRICS} for scope in report.SCOPES}
    def roles(items):
        result = {}
        for scope in report.SCOPES:
            result[scope] = {}
            for metric in report.METRICS:
                result[scope][metric] = {}
                for part in report.PARTS:
                    selected = [g for g in items if part == "original" or g["removed"] == (part == "removed")]
                    predicates = (lambda g: True, lambda g: not g["is_maker"], lambda g: g["side"] == "BUY",
                                  lambda g: g["side"] == "BUY" and g["is_maker"],
                                  lambda g: g["side"] == "BUY" and not g["is_maker"])
                    result[scope][metric][part] = {role: sum(g[scope + "_" + metric] for g in selected if predicate(g))
                                                  for role, predicate in zip(report.ROLES, predicates)}
        return result
    raw = [{"side": "BUY", "is_maker": False, "matched_proxy_address": report.EXCHANGES[0],
            "matched_counterparty_address": report.EXCHANGES[0], "full_rows": 5000 if removed else 20,
            "cutoff_rows": 3800 if removed else 10},
           {"side": "SELL", "is_maker": True, "matched_proxy_address": report.DIAGNOSTIC,
            "matched_counterparty_address": None, "full_rows": 9, "cutoff_rows": 4}]
    months = []
    for index, month in enumerate(report.MONTHS):
        eligible, raw_groups = (groups, raw) if index == 0 else ([], [])
        intersections = {}
        for scope in report.SCOPES:
            raw_n = sum(g[scope + "_rows"] for g in raw_groups)
            eligible_n = sum(g[scope + "_baseline"] for g in eligible if g["removed"])
            intersections[scope] = {"raw_matched": raw_n, "eligible_matched": eligible_n,
                                    "noneligible_matched": raw_n - eligible_n}
        months.append({"month": month, "eligible_groups": eligible, "raw_groups": raw_groups,
                       "counts": totals(eligible), "raw_minus_eligible": intersections,
                       "mask_rows": sum(g["full_rows"] for g in raw_groups),
                       "EXCEPT_ALL_differences": [0, 0], "unsupported_eligible_rows": 0})
    audit = {"schema_version": "kaushik_exchange_wallet_audit_v1", "status": "exchange_wallet_audit_complete",
             "scientific_admission": True, "policy": deepcopy(report.POLICY), "source": {"head": "a" * 40},
             "checks": dict.fromkeys(report.CHECKS, True), "months": months,
             "totals": totals(groups), "role_totals": roles(groups),
             "exact_no_change": dict.fromkeys(report.SCOPES, not removed)}
    acceptance = {"schema_version": "kaushik_exchange_wallet_acceptance_v1",
                  "status": "exchange_wallet_masks_reopened_accepted", "scientific_admission": True,
                  "all_masks_reopened": True, "policy": deepcopy(report.POLICY),
                  "source_head": "a" * 40, "totals": deepcopy(audit["totals"]), "audit_sha256": "b" * 64}
    return audit, acceptance


class ReportTests(unittest.TestCase):
    def test_zero_exclusion_is_conditional_no_change_and_all_expected_rows_render(self):
        audit, acceptance = fixtures()
        source = report.render_source(audit, acceptance, "b" * 64)
        self.assertEqual(source, report.render_source(audit, acceptance, "b" * 64))
        for label in report.LABELS:
            self.assertEqual(source.count(label + " &"), 2)
        self.assertIn("2,400 & 0 & 2,400", source)
        self.assertIn("1,800 & 0 & 1,800", source)
        self.assertIn("source, model and provider bindings held fixed", source)
        self.assertIn("no new estimates were fitted", source)
        self.assertIn("Kaushik's exact address list was not supplied", source)
        self.assertIn("not unique native fills", source)
        self.assertIn(r"\usepackage{booktabs}", source)
        self.assertNotIn("http", source)
        self.assertNotIn("sha256", source)
        self.assertNotIn(r"\section", source)
        self.assertEqual(source.count(r"\begin{tabular}"), 2)

    def test_nonzero_requires_refit_and_two_wallet_matches_are_counted_once(self):
        audit, acceptance = fixtures(removed=True)
        source = report.render_source(audit, acceptance, "b" * 64)
        self.assertIn("4,800 & 2,400 & 2,400", source)
        self.assertIn("Four-address raw source matches & 5,000 & 3,800", source)
        self.assertIn("Already ineligible source matches & 200 & 200", source)
        self.assertIn("Eligible all-role removals & 4,800 & 3,600", source)
        self.assertIn("Affected estimates require refitting", source)
        self.assertIn("not a regression rerun", source)
        self.assertNotIn("earlier estimates unchanged", source)

    def test_unsupported_statuses_admission_and_digest_fail(self):
        changes = [("audit", "status", "exchange_wallet_audit_incomplete"),
                   ("audit", "scientific_admission", False), ("acceptance", "status", "pending"),
                   ("acceptance", "scientific_admission", False), ("acceptance", "all_masks_reopened", False),
                   ("acceptance", "audit_sha256", "c" * 64), ("acceptance", "source_head", "c" * 40)]
        for document, key, value in changes:
            audit, acceptance = fixtures()
            (audit if document == "audit" else acceptance)[key] = value
            with self.subTest(document=document, key=key), self.assertRaises(ValueError):
                report.render_source(audit, acceptance, "b" * 64)

    def test_policy_drift_fails(self):
        changes = [("addresses", list(report.EXCHANGES[:-1])), ("fields", ["proxyWallet"]),
                   ("comparison", "prefix"), ("operator", "both"), ("null_blank_match", True),
                   ("emitting_contract_filter", True), ("other_filters_added", True),
                   ("emitting_contract_filter", 0)]
        for key, value in changes:
            audit, acceptance = fixtures()
            audit["policy"][key] = value
            acceptance["policy"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                report.render_source(audit, acceptance, "b" * 64)

    def test_saved_gates_months_and_no_change_flag_fail_closed(self):
        for kind in ("gate", "missing_month", "duplicate_month", "native_partition", "no_change"):
            audit, acceptance = fixtures()
            if kind == "gate": audit["checks"][report.CHECKS[0]] = False
            if kind == "missing_month": audit["months"].pop()
            if kind == "duplicate_month": audit["months"][-1]["month"] = audit["months"][0]["month"]
            if kind == "native_partition": audit["months"][0]["EXCEPT_ALL_differences"] = [0, 1]
            if kind == "no_change": audit["exact_no_change"]["full"] = False
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                report.render_source(audit, acceptance, "b" * 64)

    def test_count_total_role_and_raw_disagreements_fail(self):
        for kind in ("acceptance", "global", "monthly", "role", "bool_count", "raw", "mask", "nesting"):
            audit, acceptance = fixtures()
            if kind == "acceptance": acceptance["totals"]["full"]["baseline"]["original"] += 1
            if kind == "global":
                audit["totals"]["full"]["baseline"]["original"] += 1
                acceptance["totals"] = deepcopy(audit["totals"])
            if kind == "monthly": audit["months"][0]["counts"]["full"]["baseline"]["original"] += 1
            if kind == "role": audit["role_totals"]["full"]["baseline"]["original"]["maker_buy"] += 1
            if kind == "bool_count": audit["months"][0]["eligible_groups"][0]["full_baseline"] = True
            if kind == "raw": audit["months"][0]["raw_minus_eligible"]["full"]["raw_matched"] += 1
            if kind == "mask": audit["months"][0]["mask_rows"] += 1
            if kind == "nesting": audit["months"][0]["eligible_groups"][0]["cutoff_baseline"] = 1300
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                report.render_source(audit, acceptance, "b" * 64)

    def test_invalid_summary_fails_before_output_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            audit, acceptance = fixtures()
            audit_path, acceptance_path = folder / "audit.json", folder / "acceptance.json"
            raw = json.dumps(audit).encode()
            audit_path.write_bytes(raw)
            acceptance["audit_sha256"] = hashlib.sha256(raw).hexdigest()
            acceptance["scientific_admission"] = False
            acceptance_path.write_text(json.dumps(acceptance))
            destination = folder / "report"
            with self.assertRaisesRegex(ValueError, "acceptance"):
                report.build_report(audit_path, acceptance_path, destination)
            self.assertFalse(destination.exists())

    def test_existing_directory_preserved_before_input_reads(self):
        with tempfile.TemporaryDirectory() as folder:
            destination = Path(folder) / "report"
            destination.mkdir()
            marker = destination / "user-file.txt"
            marker.write_text("preserve")
            with self.assertRaisesRegex(ValueError, "already exists"):
                report.build_report("absent-audit", "absent-acceptance", destination)
            self.assertEqual(marker.read_text(), "preserve")

    def test_duplicate_and_nonfinite_json_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fixture.json"
            for raw in ('{"x":1,"x":2}', '{"x":NaN}'):
                path.write_text(raw)
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    report.read_json(path)


if __name__ == "__main__":
    unittest.main()
