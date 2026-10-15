"""Tiny accepted-stage fixtures; no real data or production guard bypass."""
from __future__ import annotations

from copy import deepcopy
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import io
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs as inputs
from analysis.kaushik_polymarket_replication import exchange_filter_contract as contract
from analysis.kaushik_polymarket_replication import filtered_inputs as filtered
from analysis.kaushik_polymarket_replication import run_estimates as driver
from scripts import audit_kaushik_exchange_wallets as audit

BASE_SCHEMA = pa.schema([(name, kind) for name, kind in zip(audit.FIELDS,
    (pa.int64(), pa.string(), pa.int32(), pa.int32(), pa.int64(), pa.float64(), pa.float64(),
     pa.int8(), pa.float64(), pa.string(), pa.bool_(), pa.bool_()))])
EXCLUSIONS = pa.schema([("source_ordinal", pa.int64()), ("eligibility_reason", pa.string()),
                        ("side", pa.string()), ("is_maker", pa.bool_())])


def accepted_fixture(folder):
    """Two months share ordinals; duplicate values retain distinct locators."""
    folder = Path(folder)
    parent, masks = folder / "parent", folder / "mask_audit"
    parent.mkdir(); masks.mkdir()
    start = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp())
    claims = [{"claim_code": i, "token_id": f"t{i}", "market_id": "m0", "event_cluster": "event:g0",
               "cluster_source": "native_event", "category": "Sports", "opening_seconds": float(start-86400),
               "endpoint_seconds": float(start+90*86400)} for i in (0, 1)]
    pq.write_table(pa.Table.from_pylist(claims), parent / "claims.parquet")
    outputs, waterfall, sources, months, base_records = {}, {}, [], [], []
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(f"CREATE TEMP VIEW claims AS SELECT * FROM read_parquet({inputs.literal(parent/'claims.parquet')})")
    for month in ("2026-04", "2026-05"):
        stamp = int(datetime.fromisoformat(month + "-02T00:00:00+00:00").timestamp())
        partition, mask_partition = parent / ("year_month="+month), masks / ("year_month="+month)
        partition.mkdir(); mask_partition.mkdir()
        values = []
        for ordinal, (side, maker, p) in enumerate((("BUY",False,.05), ("BUY",False,.05),
            ("BUY",True,.95), ("SELL",False,.05), ("SELL",True,.95), ("BUY",False,.1))):
            sell = side == "SELL"
            values.append(dict(zip(audit.FIELDS, (ordinal, month, int(sell), 0, stamp, p,
                1-p if sell else p, 0 if sell else 1, 3., side, maker, True))))
        base_records += values
        pq.write_table(pa.Table.from_pylist(values, schema=BASE_SCHEMA), partition / "base.parquet")
        pq.write_table(pa.Table.from_pylist([{"source_ordinal": i, "eligibility_reason": "invalid_recorded_price",
            "side": "BUY", "is_maker": False} for i in (6,7)], schema=EXCLUSIONS), partition / "exclusions.parquet")
        rows = []
        for ordinal in (1,6):
            rows.append({"source_month": month, "source_ordinal": ordinal,
                "matched_proxy_address": audit.EXCHANGES[2], "matched_counterparty_address": None,
                "proxy_match": True, "counterparty_match": False, "known_exchange_match": True,
                "unsupported_candidate_match": False})
        # A diagnostic-only address in an already-ineligible record is retained
        # in mask evidence but adds no eligible removal.
        rows.append({**rows[-1], "source_ordinal":7, "matched_proxy_address":audit.DIAGNOSTIC_EXCHANGES[0],
                     "known_exchange_match":False, "unsupported_candidate_match":True})
        mask_path = mask_partition / "wallet_mask.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=audit.MASK_SCHEMA), mask_path)
        outputs[str(mask_path.relative_to(masks))] = inputs._output_info(mask_path)
        month_result = audit.audit_month(con, partition / "base.parquet", mask_path, month, 8)
        months.append(month_result)
        cells = {}
        for row in values:
            key = row["is_maker"], row["side"]
            cell = cells.setdefault(key, {"reason":"eligible", "is_maker":key[0], "side":key[1],
                                         "rows":0,"scope_rows":0,"duration_tail_rows":0})
            cell["rows"] += 1; cell["scope_rows"] += 1
            cell["duration_tail_rows"] += row["P"]<.1 or row["P"]>=.9
        waterfall[month] = [*cells.values(), {"reason":"invalid_recorded_price", "is_maker":False,
            "side":"BUY", "rows":2,"scope_rows":2,"duration_tail_rows":0}]
        sources.append({"month":month,"rows":8})
    files = [parent / "claims.parquet", *sorted(parent.glob("year_month=*/*.parquet"))]
    inputs.create_analysis_view(con, parent)
    support = inputs.support_counts(con)
    manifest = {"schema_version":"kaushik_replication_inputs_v1", "status":"inputs_complete",
        "source":{"head":"b"*40}, "sample_scope":"all_available_dates", "binding":{}, "inputs":{},
        "definitions":inputs.sample_definitions("all_available_dates"), "input_preflight":{
            "contract":{"sample_scope":"all_available_dates"}, "trades":sources}, "metadata_health":{},
        "rows":{"source":16,"baseline_all_roles":12,"excluded":4,"primary_taker":8},
        "outputs":{str(p.relative_to(parent)):inputs._output_info(p) for p in files},
        "exclusions":waterfall, "support":support,
        "reconciliation":dict.fromkeys(("all_records_accounted","archive_multiplicity_preserved","no_join_fanout",
            "unique_reciprocal_binary_complements","outputs_reopened"),True)}
    inputs.write_json(parent / "manifest.json", manifest)
    inputs.write_json(parent / "acceptance.json", {"schema_version":"kaushik_replication_input_acceptance_v1",
        "status":"inputs_reopened_accepted", "source_head":"b"*40, "sample_scope":"all_available_dates",
        "manifest_sha256":inputs.sha256(parent / "manifest.json"), "rows":manifest["rows"],
        "output_schema_hash_count_support_reopened":True})
    groups = [g for m in months for g in m["eligible_groups"]]
    totals = {scope:{metric:{part:sum(m["counts"][scope][metric][part] for m in months)
        for part in ("original","retained","removed")} for metric in audit.METRICS} for scope in ("full","cutoff")}
    roles = {scope:{metric:{part:audit.role_counts(groups,scope,metric,part)
        for part in ("original","retained","removed")} for metric in audit.METRICS} for scope in ("full","cutoff")}
    recorded = {"schema_version":"kaushik_exchange_wallet_audit_v1", "status":"exchange_wallet_audit_complete",
        "source":{"head":"c"*40}, "policy":deepcopy(contract.POLICY), "scientific_admission":True,
        "months":months, "totals":totals, "role_totals":roles, "outputs":outputs,
        "exact_no_change":{"full":False,"cutoff":True},
        "reference_binding":{"manifest":inputs.read_json(parent / "manifest.json")[1],
                             "acceptance":inputs.read_json(parent / "acceptance.json")[1]},
        "checks":dict.fromkeys(("source_mask_locator_and_fields_exact","reference_multiset_partition","BUY_role_partition",
            "source_inputs_reopened_unchanged","unsupported_eligible_matches_absent"),True)}
    inputs.write_json(masks / "audit.json", recorded)
    inputs.write_json(masks / "acceptance.json", {"schema_version":"kaushik_exchange_wallet_acceptance_v1",
        "status":"exchange_wallet_masks_reopened_accepted", "source_head":"c"*40,
        "policy":deepcopy(contract.POLICY), "scientific_admission":True,"all_masks_reopened":True,
        "audit_sha256":inputs.sha256(masks / "audit.json"),"totals":totals})
    con.close()
    wallet_filter = filtered.make_wallet_filter(parent, inputs.sha256(parent / "manifest.json"),
        inputs.sha256(parent / "acceptance.json"), masks, inputs.sha256(masks / "audit.json"),
        inputs.sha256(masks / "acceptance.json"))
    return wallet_filter, base_records


class FilteredInputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.folder = Path(self.tmp.name)
        self.wallet_filter, self.original = accepted_fixture(self.folder)
        self.refs = filtered.load_references(self.wallet_filter)

    def tearDown(self):
        self.tmp.cleanup()

    def test_exact_lazy_membership_and_derived_values_equal_physical_filter(self):
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone='UTC'")
            filtered.validate_locators(con,self.refs,filtered.ReadLedger())
            filtered.create_analysis_view(con,self.refs["parent"]["directory"],self.refs["monthly_paths"],self.refs["mask_paths"])
            expected = [r for r in self.original if r["source_ordinal"] != 1]
            actual = con.execute("SELECT " + ",".join(audit.FIELDS) + " FROM analysis_base ORDER BY source_month,source_ordinal").fetchall()
            self.assertEqual(actual,[tuple(r[k] for k in audit.FIELDS) for r in expected])
            con.execute("CREATE TEMP TABLE materialized AS SELECT * FROM analysis_base_unfiltered_parent WHERE source_ordinal<>1")
            for left,right in (("analysis_base","materialized"),("materialized","analysis_base")):
                self.assertEqual(con.execute(f"SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})").fetchone()[0],0)
            filtered.reconcile_population(con,self.refs["audit"],filtered.ReadLedger(),sum(i["stat"]["bytes"] for i in self.refs["files"]))
            rows,waterfall = filtered.effective_accounting(self.refs["parent"]["manifest"],self.refs["audit"])
            self.assertEqual(rows,{"source":16,"baseline_all_roles":10,"excluded":6,"primary_taker":6})
            self.assertEqual(sum(r["rows"] for m in waterfall.values() for r in m if r["reason"]=="exchange_wallet"),2)
            self.assertEqual(sum(r["rows"] for m in waterfall.values() for r in m if r["reason"]=="invalid_recorded_price"),4)
        finally:
            con.close()

    def test_logical_producer_publishes_no_parquet_and_driver_resolves_parent(self):
        source={"head":"d"*40,"files":{}}
        fresh={"schema_version":"kaushik_replication_filtered_preflight_v1","status":"preflight_complete",
            "target":str(self.folder / "logical"),"source":source,"wallet_filter":self.wallet_filter,
            "references":self.refs,"caps":filtered.CAPS,"required_free_bytes":1,
            "planned_read_bytes":1_000_000_000,"join_planning_policy":audit.JOIN_PLANNING_POLICY}
        review=self.folder / "review.json"
        inputs.write_json(review,fresh)
        reviewed,identity=inputs.read_json(review)
        with patch("production_guard.require_production_host"),patch.object(inputs,"validate_destination"), \
             patch.object(filtered,"source_snapshot",return_value=source), \
             patch.object(filtered.shutil,"disk_usage",return_value=SimpleNamespace(free=10**15)):
            result=filtered.build_stage(reviewed,fresh,["synthetic-only"],identity)
        target=Path(fresh["target"])
        self.assertEqual({p.name for p in target.iterdir() if p.is_file()},{"manifest.json","acceptance.json"})
        self.assertFalse(list(target.rglob("*.parquet")))
        manifest,binding,files,monthly=driver.load_base(target,inputs.sha256(target / "manifest.json"),inputs.sha256(target / "acceptance.json"))
        self.assertEqual(manifest,result)
        access=driver.base_access(target,manifest,files,monthly)
        self.assertEqual(access["claims_path"],str(Path(self.wallet_filter["parent_binding"]["directory"]) / "claims.parquet"))
        self.assertEqual(len(files),len(self.refs["files"]))
        self.assertEqual(contract.binding_from_identities(binding["manifest"],binding["acceptance"])["directory"],str(target.resolve()))

    def test_filtered_full_grid_fits_equal_independent_physical_retained_fixture(self):
        from tests.test_kaushik_replication_driver import sports_copy_fixture
        fixture_dir=self.folder / "model_fixture"
        fixture_dir.mkdir()
        initial=duckdb.connect()
        records=sports_copy_fixture(initial,fixture_dir)
        initial.close()
        parent=fixture_dir / "accepted_fixture"
        monthly=sorted(parent.glob("year_month=*/base.parquet"))
        claim_clocks={r["claim_code"]:r for r in pq.ParquetFile(parent / "claims.parquet").read().to_pylist()}
        for path in monthly:
            table=pq.ParquetFile(path).read()
            rows=table.to_pylist()
            for row in rows:
                clock=claim_clocks[row["claim_code"]]
                row["duration_eligible"]=(clock["endpoint_seconds"]>clock["opening_seconds"] and
                    clock["opening_seconds"]<=row["timestamp"]<=clock["endpoint_seconds"])
            pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
        inventory={str(p.relative_to(parent)):inputs._output_info(p) for p in monthly}
        inputs.write_json(parent / "manifest.json",{"outputs":inventory})
        masks=[]
        for path in monthly:
            month=path.parent.name.split("=",1)[1]
            mask_dir=fixture_dir / "masks" / path.parent.name
            mask_dir.mkdir(parents=True)
            values=[{"source_month":month,"source_ordinal":r["source_ordinal"],
                "matched_proxy_address":audit.EXCHANGES[2],"matched_counterparty_address":None,
                "proxy_match":True,"counterparty_match":False,"known_exchange_match":True,
                "unsupported_candidate_match":False} for r in records
                if r["source_month"]==month and r["source_ordinal"]%5==0]
            mask=mask_dir / "wallet_mask.parquet"
            pq.write_table(pa.Table.from_pylist(values,schema=audit.MASK_SCHEMA),mask)
            masks.append(str(mask))
        sports_map=fixture_dir / "physical_sports_map.parquet"
        sports={"schema_version":"kaushik_replication_sports_binding_v1","sports":list(driver.SPORTS),
            "provider_cohort_admitted":True,"retained_identity_result_timing_proof":True,
            "coverage_qualification":"tiny synthetic provider-covered fixture",
            "inputs":[{"role":"sports_market_map","path":str(sports_map),"sha256":inputs.sha256(sports_map)}]}
        fake_binding={"directory":str((self.folder / "wrapper").resolve()),
            "manifest_path":str((self.folder / "wrapper/manifest.json").resolve()),"manifest_sha256":"d"*64,
            "acceptance_path":str((self.folder / "wrapper/acceptance.json").resolve()),"acceptance_sha256":"e"*64}
        results=[]
        for physical in (False,True):
            con=duckdb.connect()
            try:
                con.execute("SET TimeZone='UTC'")
                filtered.create_analysis_view(con,parent,[str(p.resolve()) for p in monthly],masks)
                if physical:
                    # Independent exclusion predicate on the original relation,
                    # without consulting the mask-driven view.
                    con.execute("CREATE TEMP TABLE physical_retained AS SELECT * FROM analysis_base_unfiltered_parent WHERE source_ordinal%5<>0")
                    con.execute("CREATE OR REPLACE TEMP VIEW analysis_base AS SELECT * FROM physical_retained")
                con.execute(f"CREATE TEMP VIEW claims AS SELECT * FROM read_parquet({inputs.literal(parent/'claims.parquet')})")
                support=inputs.support_counts(con)
                kept=sum(r["rows"] for r in support)
                n=sum(r["rows"] for r in support if not r["is_maker"])
                manifest={"schema_version":contract.FILTERED_INPUT_SCHEMA,"sample_scope":"all_available_dates",
                    "wallet_filter":self.wallet_filter,"definitions":inputs.sample_definitions("all_available_dates"),
                    "input_preflight":{"contract":{"sample_scope":"all_available_dates"}},
                    "rows":{"source":len(records),"baseline_all_roles":kept,"primary_taker":n,"excluded":len(records)-kept},
                    "support":support,"exclusions":{},"metadata_health":{}}
                with redirect_stdout(io.StringIO()):
                    results.append(driver.estimate_all(con,fixture_dir / ("physical_fit" if physical else "lazy_fit"),
                        manifest,sports,base_bytes=123,filtered_input_binding=fake_binding))
            finally:
                con.close()
        def compare(left,right):
            if isinstance(left,dict):
                self.assertEqual(set(left),set(right))
                for key in left:
                    compare(left[key],right[key])
            elif isinstance(left,list):
                self.assertEqual(len(left),len(right))
                for a,b in zip(left,right): compare(a,b)
            elif isinstance(left,float):
                np.testing.assert_allclose(left,right,rtol=2e-8,atol=1e-9)
            else:
                self.assertEqual(left,right)
        for value in results:
            self.assertEqual(value["schema_version"],contract.ESTIMATE_SCHEMAS[2][0])
            self.assertEqual(value["wallet_filter"],value["definitions"]["wallet_filter"])
            self.assertEqual(len(value["table2"]),5)
            self.assertEqual([len(value["sports"][k]) for k in ("phase_rows","profile_rows","window_rows")],[60,400,280])
        for key in ("categories","table1"):
            if key=="table1":
                for field in ("profile_rows","gap_rows"):
                    compare(results[0][key][field],results[1][key][field])
            else: compare(results[0][key],results[1][key])
        models=lambda r:[*r["table2"],*r["table3"]["L_gt1"],*r["table3"]["R_gt1"],r["appendix_a1"]]
        for left,right in zip(models(results[0]),models(results[1])):
            for key in ("model_id","spec","n_observations","n_clusters","suppressed","suppression_reasons","slopes"):
                compare(left[key],right[key])
            for key in ("estimates","covariance_CR0","covariance_cluster_count_adjusted"):
                compare(left["joint"][key],right["joint"][key])
        for left,right in zip(results[0]["appendix_a2"],results[1]["appendix_a2"]):
            for key in ("counts","profile_rows","gap_rows"): compare(left[key],right[key])
        for key in ("phase_rows","profile_rows","window_rows"):
            compare(results[0]["sports"][key],results[1]["sports"][key])

    def test_missing_extra_or_drifted_mask_block(self):
        path=Path(self.refs["mask_paths"][0])
        extra=path.parent / "extra.parquet"
        pq.write_table(pa.Table.from_pylist([],schema=audit.MASK_SCHEMA),extra)
        with self.assertRaisesRegex(ValueError,"mask Parquet"):
            filtered.load_references(self.wallet_filter)
        extra.unlink()
        rows=pq.ParquetFile(path).read().to_pylist()
        pq.write_table(pa.Table.from_pylist(rows+[rows[0]],schema=audit.MASK_SCHEMA),path)
        with self.assertRaisesRegex(ValueError,"footer/schema/count"):
            filtered.load_references(self.wallet_filter)

    def test_reference_policy_and_receipt_mutations_block(self):
        for key,value in (("operator","both"),("null_blank_match",0),("emitting_contract_filter",True)):
            invalid=deepcopy(self.wallet_filter)
            invalid["wallet_policy"][key]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,"policy differs"):
                filtered.load_references(invalid)
        parent=Path(self.wallet_filter["parent_binding"]["directory"])
        inputs.write_json(parent / "failure.json",{"status":"failed"})
        with self.assertRaisesRegex(ValueError,"without failure"):
            filtered.load_references(self.wallet_filter)

    def test_legacy_hidden_filter_and_receipt_schema_rejected(self):
        parent=Path(self.wallet_filter["parent_binding"]["directory"])
        original=inputs.read_json(parent / "manifest.json")[0]
        invalid=deepcopy(original)
        invalid["input_preflight"]["contract"]["wallet_policy"]=deepcopy(contract.POLICY)
        with patch.object(inputs,"read_json",return_value=(invalid,{"sha256":"0"*64})):
            with self.assertRaisesRegex(ValueError,"input contract carries"):
                driver.load_base(parent,"0"*64,"1"*64)


if __name__ == "__main__":
    unittest.main()
