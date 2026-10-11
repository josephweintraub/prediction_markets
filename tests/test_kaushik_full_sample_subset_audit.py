"""Tiny saved-input fixtures for the independent, guarded subset audit."""
from collections import Counter
from copy import deepcopy
import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs as inputs
from scripts import audit_kaushik_full_sample_subset as audit


SCHEMA = pa.schema([(name, kind) for name,kind in zip(audit.FIELDS,
    (pa.int64(),pa.string(),pa.int32(),pa.int32(),pa.int64(),pa.float64(),pa.float64(),
     pa.int8(),pa.float64(),pa.string(),pa.bool_(),pa.bool_()))])


def rows():
    def row(ordinal, t, p=.03, side="BUY", maker=False, payout=1):
        return dict(zip(audit.FIELDS,(ordinal,"2026-03",1,1,t,p,
            1-p if side == "SELL" else p,payout,3.,side,maker,True)))
    old = [row(0,audit.CUTOFF-10),row(1,audit.CUTOFF-1,.8,"SELL",True,0),row(2,audit.CUTOFF-10)]
    full = old+[row(3,audit.CUTOFF,.9,maker=True,payout=0),
                row(4,audit.CUTOFF+1,.7,"SELL"),row(5,audit.CUTOFF+2,.005)]
    return old,full


def save_json(path, value):
    # Fixture-only resigning permits deliberate mutations without modifying a
    # production acceptance or any repository source.
    path.write_text(json.dumps(value,sort_keys=True,indent=2)+"\n")
    return inputs.sha256(path)


def stage(root, name, values, policy):
    folder = root/name
    month = folder/"year_month=2026-03"
    month.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(values,schema=SCHEMA),month/"base.parquet")
    excluded = 9-len(values)
    pq.write_table(pa.Table.from_pylist([{"source_ordinal":n+len(values),"eligibility_reason":"fixture_excluded",
        "side":"BUY","is_maker":False} for n in range(excluded)]),month/"exclusions.parquet")
    pq.write_table(pa.Table.from_pylist([{"claim_code":1,"claim_id":"yes","Y":1},
                                      {"claim_code":2,"claim_id":"no","Y":0}]),folder/"claims.parquet")
    support = [{"is_maker":maker,"side":side,"rows":n} for (maker,side),n in
               sorted(Counter((r["is_maker"],r["side"]) for r in values).items())]
    waterfall = [{"reason":"eligible","rows":len(values)},
                 {"reason":"at_or_after_cutoff" if policy == "paper_cutoff" else "invalid_recorded_price","rows":excluded}]
    for r in waterfall:
        r["precut_rows" if policy == "paper_cutoff" else "scope_rows"] = (
            r["rows"] if policy != "paper_cutoff" or r["reason"] == "eligible" else 0)
    manifest = {"schema_version":"kaushik_replication_inputs_v1","status":"inputs_complete",
        "source":{"head":"1"*40},"sample_scope":policy,
        "reconciliation":{k:True for k in ("all_records_accounted","archive_multiplicity_preserved",
            "no_join_fanout","unique_reciprocal_binary_complements","outputs_reopened")},
        "definitions":{**inputs.sample_definitions(policy),"observation":"unchanged physical rows"},
        "input_preflight":{"contract":{"sample_scope":policy,"categories":"unchanged",
                                        "caps":dict(inputs.CAPS,memory_limit="64GB")}},
        "inputs":{"same44sourceinventory":"a"*64},"rows":{"source":9,"baseline_all_roles":len(values),
            "excluded":excluded,"primary_taker":sum(not r["is_maker"] for r in values)},
        "outputs":{str(p.relative_to(folder)):inputs._output_info(p) for p in
                   (folder/"claims.parquet",month/"base.parquet",month/"exclusions.parquet")},
        "exclusions":{"2026-03":waterfall},"support":support}
    resign(folder,manifest)
    return folder


def resign(folder, manifest):
    digest = save_json(folder/"manifest.json",manifest)
    save_json(folder/"acceptance.json",{"schema_version":"kaushik_replication_input_acceptance_v1",
        "status":"inputs_reopened_accepted","manifest_sha256":digest,"source_head":manifest["source"]["head"],
        "output_schema_hash_count_support_reopened":True,"rows":manifest["rows"],
        "sample_scope":manifest.get("sample_scope","paper_cutoff")})


def bindings(root):
    previous,full = rows()
    a = stage(root,"previous",previous,"paper_cutoff")
    b = stage(root,"full",full,"all_available_dates")
    return a,b


def inspect(a,b,target):
    with patch.object(audit.shutil,"disk_usage",return_value=SimpleNamespace(free=40_000_000_000)):
        return audit.preflight(a,inputs.sha256(a/"manifest.json"),inputs.sha256(a/"acceptance.json"),
            b,inputs.sha256(b/"manifest.json"),inputs.sha256(b/"acceptance.json"),target,
            {"head":"2"*40,"files":{"fixture":"b"*64}})


def run(fresh, reviewed=None, identity=None):
    with contextlib.redirect_stdout(io.StringIO()), patch.object(audit.shutil,"disk_usage",
            return_value=SimpleNamespace(free=40_000_000_000)):
        return audit.audit_subset(fresh if reviewed is None else reviewed,fresh,identity)


class IndependentFullSampleSubsetAuditQA(unittest.TestCase):
    def test_exact_approved_input_memory_retry_is_recorded_without_changing_audit_caps(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); manifest=inputs.read_json(b/"manifest.json")[0]
            manifest["input_preflight"]["contract"]["caps"]["memory_limit"]="192GB"; resign(b,manifest)
            fresh=inspect(a,b,root/"new_audit")
            self.assertEqual(fresh["resource_only_differences"],[{"path":"caps.memory_limit",
                "previous":"64GB","full":"192GB","classification":"approved_resource_only",
                "reason":"input support DISTINCT memory-only retry; scientific contract unchanged"}])
            result,_=run(fresh)
            self.assertEqual(result["input_contracts"],fresh["input_contracts"])
            self.assertEqual(result["resource_only_differences"],fresh["resource_only_differences"])
            self.assertEqual(result["input_contracts"]["previous"]["caps"]["memory_limit"],"64GB")
            self.assertEqual(result["input_contracts"]["full"]["caps"]["memory_limit"],"192GB")
            self.assertEqual(result["caps"]["memory_limit"],"64GB")
            self.assertEqual(result["input_bindings"]["full"]["manifest"]["sha256"],inputs.sha256(b/"manifest.json"))

    def test_resource_exception_rejects_other_values_directions_caps_and_science(self):
        mutations=(lambda old,new:new["caps"].update(memory_limit="96GB"),
            lambda old,new:new["caps"].update(memory_limit="192gb"),
            lambda old,new:new["caps"].update(memory_limit=192_000_000_000),
            lambda old,new:old["caps"].update(memory_limit="192GB"),
            lambda old,new:(old["caps"].update(memory_limit="192GB"),new["caps"].update(memory_limit="192GB")),
            lambda old,new:new.pop("caps"),lambda old,new:old.pop("caps"),
            lambda old,new:new["caps"].pop("memory_limit"),
            lambda old,new:(old["caps"].pop("threads"),new["caps"].pop("threads")),
            lambda old,new:(old["caps"].update(unapproved_new_cap=1),new["caps"].update(unapproved_new_cap=1)),
            lambda old,new:new["caps"].update(memory_limit="192GB",threads=5),
            lambda old,new:new["caps"].update(memory_limit="192GB",spill_bytes=32_000_000_000),
            lambda old,new:new["caps"].update(memory_limit="192GB",unapproved_new_cap=1),
            lambda old,new:(new["caps"].update(memory_limit="192GB"),new.update(categories="changed")))
        for mutation in mutations:
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary); a,b=bindings(root)
                old=inputs.read_json(a/"manifest.json")[0]; new=inputs.read_json(b/"manifest.json")[0]
                mutation(old["input_preflight"]["contract"],new["input_preflight"]["contract"])
                resign(a,old); resign(b,new)
                with self.assertRaises(ValueError): inspect(a,b,root/"new_audit")

    def test_resource_difference_or_complete_contract_record_drift_fails_closed(self):
        for name in ("resource_only_differences","input_contracts"):
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
                fresh[name]=[{}] if name == "resource_only_differences" else {}
                with self.assertRaisesRegex(ValueError,"resource exception/provenance drift"): run(fresh)

    def test_exact_monthly_multiset_boundary_roles_and_read_ledger(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); a,b = bindings(root)
            before = {str(p):inputs.sha256(p) for folder in (a,b) for p in folder.rglob("*") if p.is_file()}
            fresh = inspect(a,b,root/"new_audit")
            result,staging = run(fresh)
            self.assertEqual(result["status"],"accepted_cutoff_subset_equal")
            self.assertEqual(result["resource_only_differences"],[])
            self.assertTrue(all(result["checks"].values()))
            self.assertEqual(result["totals"]["previous"],dict(zip(audit.PARTITIONS,(3,2,2,0,2))))
            self.assertEqual(result["totals"]["full"],dict(zip(audit.PARTITIONS,(6,4,4,1,3))))
            self.assertEqual(result["totals"]["added"],dict(zip(audit.PARTITIONS,(3,2,2,1,1))))
            self.assertEqual(result["months"][0]["exact_cutoff_rows"],1)
            # Old/full base have 8/10 conservative passes including footer;
            # claim map5, exclusions3, and each manifest/receipt2 JSON reads.
            expected = 0
            for key,multiplier in (("previous",8),("full",10)):
                for name,item in fresh[key]["files"].items():
                    expected += item["info"]["bytes"]*(5 if name == "claims.parquet" else
                        3 if name.endswith("exclusions.parquet") else multiplier)
                expected += 2*sum(v["stat"]["bytes"] for v in fresh[key]["bindings"].values())
            self.assertEqual(result["read_bytes"],expected)
            self.assertLessEqual(expected,result["planned_read_bytes"])
            self.assertTrue(staging.is_dir()); self.assertFalse((root/"new_audit").exists())
            self.assertEqual(before,{str(p):inputs.sha256(p) for folder in (a,b) for p in folder.rglob("*") if p.is_file()})

    def test_changed_pre_cut_field_fails_both_direction_equality(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); a,b = bindings(root)
            values = rows()[1]; values[0]["usdcSize"] = 3.0000000000000004
            pq.write_table(pa.Table.from_pylist(values,schema=SCHEMA),b/"year_month=2026-03/base.parquet")
            manifest = inputs.read_json(b/"manifest.json")[0]
            manifest["outputs"]["year_month=2026-03/base.parquet"] = inputs._output_info(b/"year_month=2026-03/base.parquet")
            resign(b,manifest)
            with self.assertRaisesRegex(ValueError,"multiset differs"):
                run(inspect(a,b,root/"new_audit"))
            self.assertFalse((root/"new_audit").exists())

    def test_duplicate_locator_and_invalid_new_price_are_rejected(self):
        for mutation in (lambda r:r[5].update(source_ordinal=4),lambda r:r[5].update(P=0.),
                         lambda r:r[5].update(recorded_price=float("inf")),lambda r:r[5].update(is_maker=None)):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary); a,b=bindings(root); values=rows()[1]; mutation(values)
                path=b/"year_month=2026-03/base.parquet"
                pq.write_table(pa.Table.from_pylist(values,schema=SCHEMA),path)
                manifest=inputs.read_json(b/"manifest.json")[0]; manifest["outputs"][str(path.relative_to(b))]=inputs._output_info(path)
                resign(b,manifest)
                with self.assertRaisesRegex(ValueError,"locator/analytical eligibility"):
                    run(inspect(a,b,root/"new_audit"))

    def test_claim_map_multiplicity_change_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); path=b/"claims.parquet"
            pq.write_table(pa.Table.from_pylist([{"claim_code":1,"claim_id":"yes","Y":1}]*2+
                [{"claim_code":2,"claim_id":"no","Y":0}]),path)
            manifest=inputs.read_json(b/"manifest.json")[0]; manifest["outputs"]["claims.parquet"]=inputs._output_info(path); resign(b,manifest)
            with self.assertRaisesRegex(ValueError,"claim/complement/category/clock map"):
                run(inspect(a,b,root/"new_audit"))

    def test_full_policy_source_contract_and_support_metadata_fail_closed(self):
        mutations = (lambda m:m.update(sample_scope="paper_cutoff"),
            lambda m:m["input_preflight"].pop("contract"),
            lambda m:m["inputs"].update(same44sourceinventory="c"*64),
            lambda m:m["definitions"].update(observation="changed"),
            lambda m:m["input_preflight"]["contract"].update(categories="changed"),
            lambda m:m["support"][0].update(rows=100),
            lambda m:m["exclusions"]["2026-03"][1].update(reason="at_or_after_cutoff"))
        for mutation in mutations:
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary); a,b=bindings(root); manifest=inputs.read_json(b/"manifest.json")[0]
                mutation(manifest); resign(b,manifest)
                with self.assertRaises(ValueError): inspect(a,b,root/"new_audit")

    def test_missing_paper_scope_is_legacy_compatible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); manifest=inputs.read_json(a/"manifest.json")[0]
            manifest.pop("sample_scope"); manifest["definitions"].pop("sample_scope")
            manifest["definitions"].pop("cutoff_seconds"); manifest["input_preflight"]["contract"].pop("sample_scope")
            resign(a,manifest); receipt=inputs.read_json(a/"acceptance.json")[0]; receipt.pop("sample_scope"); save_json(a/"acceptance.json",receipt)
            result,_=run(inspect(a,b,root/"new_audit")); self.assertTrue(all(result["checks"].values()))

    def test_hash_drift_blocks_before_any_duckdb_scan(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
            fresh["full"]["files"]["claims.parquet"]["info"]["sha256"]="0"*64
            with patch.object(audit.duckdb,"connect",side_effect=AssertionError("scan started")):
                with self.assertRaisesRegex(ValueError,"fingerprint differs"): run(fresh)

    def test_reviewed_preflight_drift_and_existing_target_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
            reviewed=deepcopy(fresh); reviewed["caps"]=dict(fresh["caps"],threads=9)
            with self.assertRaisesRegex(ValueError,"reviewed subset preflight"): run(fresh,reviewed)
            (root/"new_audit").mkdir()
            with self.assertRaisesRegex(ValueError,"new nonoverlapping"): inspect(a,b,root/"new_audit")

    def test_bound_preflight_reopens_exactly_and_device_stat_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
            path=root/"reviewed.json"; digest=save_json(path,fresh); reviewed,identity=inputs.read_json(path,digest)
            result,_=run(fresh,reviewed,identity)
            unbound,_=run(fresh)
            self.assertEqual(result["read_bytes"]-unbound["read_bytes"],2*identity["stat"]["bytes"])
            changed=deepcopy(identity); changed["stat"]["device"]+=1
            with self.assertRaisesRegex(ValueError,"reviewed preflight JSON drift"): run(fresh,reviewed,changed)

    def test_accepted_body_drift_after_comparison_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
            original=audit.partition_counts
            changed=False
            def change_after_scan(con, relation, predicate="true"):
                nonlocal changed
                counts=original(con,relation,predicate)
                if not changed:
                    changed=True
                    path=b/"year_month=2026-03/exclusions.parquet"
                    with path.open("ab") as stream: stream.write(b"changed after prehash")
                return counts
            with patch.object(audit,"partition_counts",side_effect=change_after_scan):
                with self.assertRaisesRegex(ValueError,"input drift after comparison"): run(fresh)
            self.assertFalse((root/"new_audit").exists())

    def test_read_ceiling_blocks_before_next_query_and_preflight_plan(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); a,b=bindings(root); fresh=inspect(a,b,root/"new_audit")
            with patch.dict(audit.CAPS,maximum_read_bytes=fresh["metadata_admission_charged_bytes"]):
                with patch.object(audit.duckdb,"connect",side_effect=AssertionError("scan started")):
                    with self.assertRaisesRegex(ValueError,"read ceiling"): run(fresh)
            with patch.dict(audit.CAPS,maximum_read_bytes=1):
                with self.assertRaisesRegex(ValueError,"read ceiling"): inspect(a,b,root/"new_audit")

    def test_disk_floor_exact_boundary_and_sparse_files_do_not_overcredit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); (root/"spill").mkdir()
            reserve=audit.CAPS["minimum_free_bytes"]+audit.CAPS["spill_bytes"]+audit.CAPS["maximum_output_bytes"]
            with patch.object(audit.shutil,"disk_usage",return_value=SimpleNamespace(free=reserve)):
                audit.reserve_disk(root)
            with patch.object(audit.shutil,"disk_usage",return_value=SimpleNamespace(free=reserve-1)):
                with self.assertRaisesRegex(ValueError,"reservation"): audit.reserve_disk(root)
            sparse=root/"spill/sparse.tmp"
            with sparse.open("wb") as stream: stream.truncate(audit.CAPS["spill_bytes"])
            stat=sparse.stat(); occupied=min(stat.st_size,stat.st_blocks*512)
            self.assertLess(occupied,stat.st_size)
            with patch.object(audit.shutil,"disk_usage",return_value=SimpleNamespace(free=reserve-occupied-1)):
                with self.assertRaisesRegex(ValueError,"reservation"): audit.reserve_disk(root)

    def test_production_guard_blocks_before_source_or_data_discovery(self):
        argv=["--previous-dir","/not/read","--previous-manifest-sha256","a"*64,
            "--previous-acceptance-sha256","b"*64,"--full-dir","/not/read2",
            "--full-manifest-sha256","c"*64,"--full-acceptance-sha256","d"*64,
            "--expected-head","e"*40,"--run-dir","/not/write","--preflight-dir","/not/write2"]
        with patch.object(audit,"source_snapshot",side_effect=AssertionError("source discovery started")),\
             patch.object(audit,"preflight",side_effect=AssertionError("data discovery started")):
            with self.assertRaises(Exception) as raised: audit.main(argv)
        self.assertNotIsInstance(raised.exception,AssertionError)


if __name__ == "__main__":
    unittest.main()
