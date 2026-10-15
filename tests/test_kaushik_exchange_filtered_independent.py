"""Independent bounded fixtures for the exchange-mask input and lineage layer.

No production source, accepted result, infrastructure or network is accessed.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
import math
from pathlib import Path
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs as inputs
from analysis.kaushik_polymarket_replication import exchange_filter_contract as contract
from analysis.kaushik_polymarket_replication import filtered_inputs as filtered
from scripts import audit_kaushik_exchange_wallets as audit
from scripts import audit_kaushik_exchange_filtered_release as release
from tests import test_kaushik_exchange_wallets_independent_qa as source_oracle
from tests.test_kaushik_exchange_wallets_independent_qa import (
    BASE_FIELDS, BASE_SCHEMA, MONTH, RAW_SCHEMA,
    exact_match, independent_base,
)


def binding(directory, *, audit=False, digest="a"):
    first = "audit" if audit else "manifest"
    return {"directory": directory, first + "_path": directory + "/" + first + ".json",
            first + "_sha256": digest * 64,
            "acceptance_path": directory + "/acceptance.json",
            "acceptance_sha256": "b" * 64}


def filtered_documents():
    wallet_filter = {"wallet_policy": deepcopy(contract.POLICY),
                     "input_representation": "parent_base_plus_mask",
                     "parent_binding": binding("/synthetic/parent"),
                     "mask_audit_binding": binding("/synthetic/audit", audit=True)}
    wrapper = binding("/synthetic/wrapper", digest="c")
    identities = {name: {"path": wrapper[name + "_path"], "sha256": wrapper[name + "_sha256"]}
                  for name in ("manifest", "acceptance")}
    data = {"schema_version": contract.ESTIMATE_SCHEMAS[2][0],
            "sample_scope": "all_available_dates", "wallet_filter": deepcopy(wallet_filter),
            "filtered_input_binding": deepcopy(wrapper),
            "definitions": {"sample_scope": "all_available_dates", "cutoff_utc": None,
                            "cutoff_seconds": None, "wallet_filter": deepcopy(wallet_filter)}}
    manifest = {"schema_version": contract.ESTIMATE_SCHEMAS[2][1],
                "sample_scope": "all_available_dates", "wallet_filter": deepcopy(wallet_filter),
                "filtered_input_binding": deepcopy(wrapper), "base_binding": deepcopy(identities),
                "preflight": {"base_binding": deepcopy(identities),
                              "base_manifest": {"schema_version": contract.FILTERED_INPUT_SCHEMA,
                                                "sample_scope": "all_available_dates",
                                                "wallet_filter": deepcopy(wallet_filter)}}}
    receipt = {"schema_version": contract.ESTIMATE_SCHEMAS[2][2],
               "sample_scope": "all_available_dates", "wallet_filter": deepcopy(wallet_filter),
               "filtered_input_binding": deepcopy(wrapper)}
    return data, manifest, receipt


class IndependentFilterLineageTests(unittest.TestCase):
    def test_original_legacy_admission_and_absent_filter_are_preserved(self):
        documents = [{"schema_version": version} for version in contract.ESTIMATE_SCHEMAS[1]]
        self.assertIsNone(contract.validate_estimate_filter_binding(*documents))
        for index in range(3):
            altered = deepcopy(documents)
            altered[index]["wallet_filter"] = None
            with self.subTest(index=index), self.assertRaises(ValueError):
                contract.validate_estimate_filter_binding(*altered)
        documents[1]["preflight"] = {"base_manifest": {"schema_version": contract.FILTERED_INPUT_SCHEMA}}
        with self.assertRaises(ValueError):
            contract.validate_estimate_filter_binding(*documents)
        documents[1]["preflight"] = {"base_manifest": {
            "schema_version": "kaushik_replication_inputs_v1",
            "input_preflight": {"contract": {"input_representation": "parent_base_plus_mask"}}}}
        with self.assertRaises(ValueError):
            contract.validate_estimate_filter_binding(*documents)

    def test_valid_filtered_lineage_and_cross_document_mutations(self):
        documents = filtered_documents()
        self.assertEqual(contract.validate_estimate_filter_binding(*documents), documents[0]["wallet_filter"])
        mutations = (
            lambda d: d[0].update(schema_version=contract.ESTIMATE_SCHEMAS[1][0]),
            lambda d: d[1].update(schema_version=contract.ESTIMATE_SCHEMAS[1][1]),
            lambda d: d[2].update(schema_version=contract.ESTIMATE_SCHEMAS[1][2]),
            lambda d: d[1]["filtered_input_binding"].update(manifest_sha256="d" * 64),
            lambda d: d[2]["filtered_input_binding"].update(acceptance_sha256="d" * 64),
            lambda d: d[1]["preflight"]["base_manifest"].update(schema_version="kaushik_replication_inputs_v1"),
            lambda d: d[1]["preflight"].pop("base_binding"),
            lambda d: d[1].pop("base_binding"),
            lambda d: d[0]["definitions"].pop("wallet_filter"),
            lambda d: d[0]["definitions"]["wallet_filter"]["wallet_policy"].update(null_blank_match=0),
            lambda d: d[0].update(sample_scope="paper_cutoff"),
            lambda d: d[1]["preflight"]["base_manifest"].update(sample_scope="paper_cutoff"),
            lambda d: d[2].pop("wallet_filter"),
            lambda d: d[2].pop("filtered_input_binding"),
            lambda d: d[2]["wallet_filter"]["parent_binding"].update(manifest_sha256="d" * 64),
        )
        for index, mutation in enumerate(mutations):
            altered = deepcopy(documents)
            mutation(altered)
            with self.subTest(index=index), self.assertRaises(ValueError):
                contract.validate_estimate_filter_binding(*altered)

    def test_policy_is_literal_and_boolean_types_are_not_integers(self):
        original = filtered_documents()[0]["wallet_filter"]
        mutations = (
            lambda f: f["wallet_policy"]["addresses"].append("0xe3333700ca9d93003f00f0f71f8515005f6c00aa"),
            lambda f: f["wallet_policy"]["addresses"].reverse(),
            lambda f: f["wallet_policy"].update(comparison="trim_lowercase"),
            lambda f: f["wallet_policy"].update(emitting_contract_filter=0),
            lambda f: f["wallet_policy"].update(unsupported_eligible_match_blocks_admission=1),
            lambda f: f.update(input_representation="physical_filtered_base"),
            lambda f: f.update(parent_binding=binding("relative/parent")),
            lambda f: f.update(parent_binding=binding("/synthetic/a/../parent")),
            lambda f: f.update(parent_binding=binding("/synthetic//parent")),
            lambda f: f["parent_binding"].update(manifest_path="/synthetic/audit/manifest.json"),
            lambda f: f["mask_audit_binding"].update(audit_sha256="A" * 64),
            lambda f: f.update(mask_audit_binding=binding("/synthetic/parent", audit=True)),
        )
        for index, mutation in enumerate(mutations):
            altered = deepcopy(original)
            mutation(altered)
            with self.subTest(index=index), self.assertRaises(ValueError):
                contract.validate_wallet_filter(altered)


class IndependentLogicalInputTests(unittest.TestCase):
    def setUp(self):
        source = source_oracle.IndependentExchangeFixture()
        source.setUp()
        self.addCleanup(source.doCleanups)
        self.source, self.con = source, source.con
        # Preserve two further copies of an eligible nonmatching source record.
        # These differ only in their original physical locator after projection.
        source.rows.extend([dict(source.rows[5]), dict(source.rows[5])])
        source.base_rows = independent_base(source.rows)
        pq.write_table(pa.Table.from_pylist(source.rows, schema=RAW_SCHEMA), source.raw)
        self.parent = source.folder / "parent"
        self.base = self.parent / ("year_month=" + MONTH) / "base.parquet"
        self.base.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(source.base_rows, schema=BASE_SCHEMA), self.base)
        self.con.execute(f"COPY claims TO {inputs.literal(self.parent / 'claims.parquet')} (FORMAT PARQUET)")
        inputs.write_json(self.parent / "manifest.json", {"outputs": {
            "year_month=" + MONTH + "/base.parquet": {}}})
        self.mask = self.parent / ("year_month=" + MONTH) / "wallet_mask.parquet"
        audit.create_source_view(self.con, source.raw, MONTH)
        self.con.execute(f"COPY ({audit.source_mask_sql(MONTH)}) TO {inputs.literal(self.mask)} (FORMAT PARQUET)")

    def references(self, *, base=None, mask=None):
        return {"parent": {"months": [MONTH], "manifest": {"input_preflight": {"trades": [
                    {"month": MONTH, "rows": len(self.source.rows)}]}},
                    "files": {"year_month=" + MONTH + "/base.parquet": {
                        "info": {"rows": len(self.source.base_rows)}}}},
                "monthly_paths": [str(base or self.base)], "mask_paths": [str(mask or self.mask)]}

    def test_lazy_filtered_relation_equals_independent_physical_multiset(self):
        retained = [row for row in self.source.base_rows
                    if not any(exact_match(self.source.rows[row["source_ordinal"]][field])
                               for field in ("proxyWallet", "counterparty"))]
        physical = self.parent / "physical_oracle"
        (physical / ("year_month=" + MONTH)).mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(retained, schema=BASE_SCHEMA),
                       physical / ("year_month=" + MONTH) / "base.parquet")
        self.con.execute(f"COPY claims TO {inputs.literal(physical / 'claims.parquet')} (FORMAT PARQUET)")
        filtered.validate_locators(self.con, self.references(), filtered.ReadLedger())
        filtered.create_analysis_view(self.con, self.parent, [str(self.base)], [str(self.mask)])
        inputs.create_analysis_view(self.con, physical, name="physical_oracle")
        self.assertEqual(self.con.execute("DESCRIBE analysis_base").fetchall(),
                         self.con.execute("DESCRIBE physical_oracle").fetchall())
        actual = Counter(self.con.execute("SELECT * FROM analysis_base").fetchall())
        oracle = Counter(self.con.execute("SELECT * FROM physical_oracle").fetchall())
        self.assertEqual(actual, oracle)
        self.assertEqual(sum(actual.values()), len(retained))
        selected = self.con.execute("SELECT " + ",".join(BASE_FIELDS) + " FROM analysis_base").fetchall()
        self.assertEqual(Counter(selected), Counter(tuple(row[field] for field in BASE_FIELDS) for row in retained))
        # Dropping locators exposes four equal observations that must survive.
        payload = [i for i, name in enumerate(BASE_FIELDS) if name not in ("source_month", "source_ordinal")]
        duplicates = Counter(tuple(row[i] for i in payload) for row in selected)
        self.assertEqual(max(duplicates.values()), 4)
        self.assertEqual(self.con.execute("SELECT count(*) FROM analysis_base WHERE payoff<>100*(Y-P) OR roi<>100*(Y/P-1)").fetchone()[0], 0)

    def test_malformed_masks_and_duplicate_parent_locators_block(self):
        original = pq.read_table(self.mask).to_pylist()
        mutations = (
            lambda rows: rows.append(dict(rows[0])),
            lambda rows: rows[0].update(source_ordinal=None),
            lambda rows: rows[0].update(source_ordinal=-1),
            lambda rows: rows[0].update(source_ordinal=len(self.source.rows)),
            lambda rows: rows[0].update(source_month="2026-02"),
            lambda rows: rows[0].update(known_exchange_match=False),
            lambda rows: rows[0].update(proxy_match=None),
            lambda rows: rows[0].update(matched_proxy_address="0x" + "0" * 40),
        )
        for index, mutation in enumerate(mutations):
            rows = deepcopy(original)
            mutation(rows)
            path = self.mask.parent / (f"bad_{index}.parquet")
            pq.write_table(pa.Table.from_pylist(rows, schema=audit.MASK_SCHEMA), path)
            with self.subTest(index=index), self.assertRaises(inputs.InputBlocked):
                filtered.validate_locators(self.con, self.references(mask=path), filtered.ReadLedger())
        rows = [*self.source.base_rows, dict(self.source.base_rows[0])]
        path = self.base.parent / "duplicate_parent.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=BASE_SCHEMA), path)
        with self.assertRaises(inputs.InputBlocked):
            filtered.validate_locators(self.con, self.references(base=path), filtered.ReadLedger())

    def test_missing_extra_and_duplicate_mask_fields_block_before_query(self):
        original = pq.read_table(self.mask)
        tables = [original.drop(["known_exchange_match"]),
                  original.append_column("unreviewed_field", pa.array([True] * len(original))),
                  pa.Table.from_arrays([*original.columns, original.column(0)],
                                       names=[*original.schema.names, original.schema.names[0]])]
        for index, table in enumerate(tables):
            path = self.mask.parent / (f"bad_schema_{index}.parquet")
            pq.write_table(table, path)
            with self.subTest(index=index), self.assertRaises(inputs.InputBlocked):
                filtered.validate_locators(self.con, self.references(mask=path), filtered.ReadLedger())

    def test_effective_waterfall_adds_only_newly_removed_eligible_records(self):
        result = audit.audit_month(self.con, self.base, self.mask, MONTH, len(self.source.rows))
        groups = result["eligible_groups"]
        original = []
        for maker, side in sorted({(r["is_maker"], r["side"]) for r in self.source.base_rows}):
            selected = [g for g in groups if g["is_maker"] is maker and g["side"] == side]
            n = sum(g["full_baseline"] for g in selected)
            original.append({"reason": "eligible", "is_maker": maker, "side": side,
                             "rows": n, "scope_rows": n,
                             "duration_tail_rows": sum(g["full_common_duration"] for g in selected)})
        excluded = len(self.source.rows) - len(self.source.base_rows)
        original.append({"reason": "original_invalid", "is_maker": False, "side": "BUY",
                         "rows": excluded, "scope_rows": excluded, "duration_tail_rows": 0})
        parent = {"rows": {"source": len(self.source.rows)}, "exclusions": {MONTH: original}}
        unchanged = deepcopy(parent)
        recorded = {"months": [result], "role_totals": {"full": {"baseline": {
            "retained": audit.role_counts(groups, "full", "baseline", "retained")}}}}
        rows, exclusions = filtered.effective_accounting(parent, recorded)
        self.assertEqual(parent, unchanged)
        self.assertEqual([r for r in exclusions[MONTH] if r["reason"] == "original_invalid"], [original[-1]])
        removed = [r for r in exclusions[MONTH] if r["reason"] == "exchange_wallet"]
        newly_removed = result["counts"]["full"]["baseline"]["removed"]
        self.assertEqual(sum(r["rows"] for r in removed), newly_removed)
        self.assertGreater(pq.ParquetFile(self.mask).metadata.num_rows, newly_removed)
        self.assertEqual(rows["source"], len(self.source.rows))
        self.assertEqual(rows["excluded"], excluded + newly_removed)
        self.assertEqual(sum(r["rows"] for r in exclusions[MONTH]), rows["source"])
        self.assertEqual(sum(r["rows"] for r in exclusions[MONTH] if r["reason"] == "eligible"), rows["baseline_all_roles"])

    def raw_mean_fixture(self):
        retained = [row for row in self.source.base_rows
                    if not any(exact_match(self.source.rows[row["source_ordinal"]][field])
                               for field in ("proxyWallet", "counterparty"))]
        filtered.create_analysis_view(self.con, self.parent, [str(self.base)], [str(self.mask)])
        duration = self.source.expected_counts(self.source.base_rows, "full", "retained", primary=True)
        counts = {"duration_rows": duration["common_duration"], "lifespan_gt1_rows": duration["lifespan_gt1"],
                  "remaining_gt1_rows": duration["remaining_gt1"]}
        select = {"taker_direction": lambda r: not r["is_maker"],
                  "all_buy": lambda r: r["side"] == "BUY",
                  "maker_buy": lambda r: r["is_maker"] and r["side"] == "BUY",
                  "taker_buy": lambda r: not r["is_maker"] and r["side"] == "BUY"}
        tables = {}
        for name, predicate in select.items():
            chosen = [r for r in retained if predicate(r)]
            profiles, means = [], {}
            for bin_ in range(1, 11):
                cell = [r for r in chosen if next((b for b in range(1, 10) if r["P"] < b / 10), 10) == bin_]
                for outcome in ("payoff_cents", "roi_percent"):
                    mean = (math.fsum(100 * (r["Y"] - r["P"]) if outcome == "payoff_cents" else
                        100 * (r["Y"] / r["P"] - 1) for r in cell) / len(cell)) if cell else None
                    means[(outcome, bin_)] = mean
                    profiles.append({"outcome": outcome, "bin": bin_, "n_observations": len(cell),
                                     "suppressed": not cell, "CR0": {"estimate": mean} if cell else None})
            tails = sum(r["P"] < .1 or r["P"] >= .9 for r in chosen)
            tables[name] = {"convention": name, "counts": {"rows": len(chosen), "tail_rows": tails},
                "profile_rows": profiles, "gap_rows": [{"outcome": outcome, "n_observations": tails,
                    "suppressed": means[(outcome, 1)] is None or means[(outcome, 10)] is None,
                    "CR0": {"estimate": means[(outcome, 10)] - means[(outcome, 1)]}
                        if means[(outcome, 1)] is not None and means[(outcome, 10)] is not None else None}
                    for outcome in ("payoff_cents", "roi_percent")]}
        data = {"sample": {"rows": sum(not r["is_maker"] for r in retained),
            "tail_rows": sum(not r["is_maker"] and (r["P"] < .1 or r["P"] >= .9) for r in retained),
            "duration_tail_rows": counts["duration_rows"], "duration_reconciliation": {"source": counts, "group_cache": counts}},
            "table1": deepcopy(tables["taker_direction"]), "appendix_a2": list(tables.values()),
            "table2": [{"n_observations": counts["duration_rows"]} for _ in range(5)],
            "table3": {panel: [{"n_observations": counts[key]} for _ in range(3)] for panel, key in
                (("L_gt1", "lifespan_gt1_rows"), ("R_gt1", "remaining_gt1_rows"))},
            "appendix_a1": {"n_observations": counts["duration_rows"]}}
        base = {"rows": {"baseline_all_roles": len(retained), "primary_taker": data["sample"]["rows"]}}
        return data, base

    def test_raw_release_oracle_rejects_mean_count_gap_and_duration_corruption(self):
        data, base = self.raw_mean_fixture()
        reads = release.Reads()
        observed = release.raw_checks(self.con, data, base, reads, 12345)
        self.assertEqual(reads.bytes, 12345)
        self.assertEqual(reads.scans, {"raw_role_action_bin_means_and_duration_counts": 12345})
        self.assertEqual(len(observed["profile_checks"]), 5)
        def change_mean(d):
            next(r for r in d["table1"]["profile_rows"] if not r["suppressed"])["CR0"]["estimate"] += .01
        def change_gap(d):
            next(r for r in d["appendix_a2"][0]["gap_rows"] if not r["suppressed"])["CR0"]["estimate"] += .01
        mutations = (change_mean, change_gap,
            lambda d: d["table1"]["profile_rows"][0].update(n_observations=999),
            lambda d: d["appendix_a2"][1]["counts"].update(rows=999),
            lambda d: d["table2"][0].update(n_observations=999),
            lambda d: d["sample"]["duration_reconciliation"]["source"].update(duration_rows=999))
        for index, mutation in enumerate(mutations):
            changed = deepcopy(data)
            mutation(changed)
            with self.subTest(index=index), self.assertRaises(inputs.InputBlocked):
                release.raw_checks(self.con, changed, base, release.Reads(), 12345)


class IndependentReleaseNumericalTests(unittest.TestCase):
    def test_production_entry_guard_precedes_any_artifact_read(self):
        argv = []
        for flag in ("filtered-input-dir", "estimate-dir", "saved-score-audit", "run-dir"):
            argv.extend(("--" + flag, "/nonexistent/fixture"))
        for flag in ("filtered-manifest-sha256", "filtered-acceptance-sha256", "input-source-head",
                     "estimate-manifest-sha256", "estimate-acceptance-sha256", "estimate-source-head",
                     "saved-score-sha256", "saved-score-source-head", "expected-head"):
            argv.extend(("--" + flag, "a" * (40 if flag.endswith("head") else 64)))
        with patch("production_guard.require_production_host", side_effect=RuntimeError("guard-first")), \
                patch.object(inputs, "read_json") as reads:
            with self.assertRaisesRegex(RuntimeError, "guard-first"):
                release.main(argv)
            reads.assert_not_called()

    def test_numerical_rank_and_projection_failure_require_withholding(self):
        def model(index):
            return {"model_id": str(index), "n_observations": 20, "suppressed": False,
                "suppression_reasons": [], "spec": {"effects": ["cat_code"]},
                "slopes": [{"suppressed": False, "suppression_reasons": []}],
                "joint": {"metadata": {"residualized_design_rank": 2, "structural_column_count": 2,
                    "condition_number_scaled_gram": 1.2, "cluster_score_normal_equation_error": 1e-14,
                    "cluster_score_normal_equation_tolerance": 1e-8,
                    "absorption": {"converged": True, "iterations": 2,
                                   "tolerance": 1e-10, "maximum_scaled_group_mean": 1e-12}}}}
        models = [model(i) for i in range(12)]
        data = {"table2": models[:5], "table3": {"L_gt1": models[5:8], "R_gt1": models[8:11]}, "appendix_a1": models[11]}
        self.assertEqual(len(release.numerical_checks(data)), 12)
        for key, value in (("residualized_design_rank", 1), ("condition_number_scaled_gram", None),
                           ("cluster_score_normal_equation_error", .1)):
            changed = deepcopy(data)
            changed["table2"][0]["joint"]["metadata"][key] = value
            with self.subTest(key=key), self.assertRaises(inputs.InputBlocked):
                release.numerical_checks(changed)
        for change in ({"converged": False}, {"maximum_scaled_group_mean": .01}, {"iterations": 101}):
            changed = deepcopy(data)
            changed["table2"][0]["joint"]["metadata"]["absorption"].update(change)
            with self.subTest(change=change), self.assertRaises(inputs.InputBlocked):
                release.numerical_checks(changed)


if __name__ == "__main__":
    unittest.main()
