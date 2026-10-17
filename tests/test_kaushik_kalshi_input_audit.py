"""Independent audit fixtures only: no production data, network, or EC2."""
from contextlib import ExitStack
from dataclasses import asdict
from datetime import date
import csv
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_kaushik_kalshi_inputs as audit
from scripts import build_kaushik_kalshi_inputs as runner
from analysis.kaushik_kalshi_replication import staging
from tests import test_kaushik_kalshi_pipeline as pipeline_fixtures


class KalshiInputAuditTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.folder = tempfile.TemporaryDirectory(prefix="kalshi-independent-audit-fixture-")
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name).resolve()

    def make_inputs(self,*,last_row_excluded=False,missing_settlement_clocks=False):
        fixture = pipeline_fixtures.KalshiPipelineTests("test_complete_fixture_metadata_then_trades_publish_only_candidates")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        plan = fixture.fixture_admission()
        if missing_settlement_clocks:
            path=fixture.paths['markets_metadata/b1_0.parquet']
            table=pq.ParquetFile(path).read();rows=table.to_pylist()
            for row in rows:row['settlement_ts']=None
            pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
            text=io.StringIO(newline='');writer=csv.writer(text);writer.writerow(['path','bytes','rows','sha256'])
            writer.writerow(['contract_matching/runs/markets_meta_all_v2/b1_0.parquet',path.stat().st_size,len(rows),audit.digest(path)])
            manifest_path=fixture.admission/'markets_metadata__MANIFEST.csv';manifest_path.write_text(text.getvalue())
            plan['metadata_manifest'].update(bytes=manifest_path.stat().st_size,sha256=audit.digest(manifest_path))
            runner.PINNED['metadata_manifest']={key:value for key,value in plan['metadata_manifest'].items() if key!='path'}
            fixture.stack.enter_context(patch.object(runner,'EXPECTED_METADATA_BYTES',path.stat().st_size))
        if last_row_excluded:
            for index,(source_path,path) in enumerate(fixture.paths.items()):
                if not source_path.startswith('trades_all/'):continue
                table=pq.ParquetFile(path).read()
                rows=table.to_pylist()
                rows.append({**rows[-1],'ticker':'KXMVE-EXCLUDED','trade_id':f'00000000-0000-0000-0000-{1000+index:012d}'})
                pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
            text=io.StringIO(newline='');writer=csv.writer(text);writer.writerow(['file','bytes','sha256'])
            writer.writerows((source_path,path.stat().st_size,audit.digest(path)) for source_path,path in fixture.paths.items() if source_path.startswith('trades_all/'))
            path=fixture.admission/'MANIFEST_kalshi.csv';path.write_text(text.getvalue())
            plan['trade_manifest'].update(bytes=path.stat().st_size,sha256=audit.digest(path))
            runner.PINNED['trade_manifest']={key:value for key,value in plan['trade_manifest'].items() if key!='path'}
        if last_row_excluded or missing_settlement_clocks:
            (fixture.admission/'admission_plan.json').write_text(json.dumps(plan))

        def stage(entries, *, remote_root, run_dir, output_spill_reserve_bytes, process):
            target = run_dir / "source_stage_v1" / "receipts"
            target.mkdir(parents=True)
            paths = []
            for index, entry in enumerate(entries):
                provenance = process(fixture.paths[entry.path], entry)
                path = target / f"{index:06d}.json"
                runner.write_json(path, {"schema_version": "kalshi_source_stage_receipt_v1",
                    "status": "processed_verified", "entry": asdict(entry),
                    "remote": runner.REMOTE_ROOT + "/" + entry.path,
                    "verified_identity": {"bytes": entry.size_bytes}, "callback_provenance": provenance})
                paths.append(path)
            return tuple(paths)

        fixture.stage_fixture = stage
        fixture.admit_fixture_runtime()
        metadata_dir = fixture.root / "metadata"
        runner.run_production(fixture.args("metadata", metadata_dir))
        trade_dir = fixture.root / "trades"
        runner.run_production(fixture.args("trades", trade_dir, metadata_dir=metadata_dir,
            metadata_manifest_sha256=runner.sha256(metadata_dir / "candidate_manifest.json")))
        days = {"2026-03-25", "2026-03-26"}
        self.stack.enter_context(patch.object(audit, "METADATA_FILES", 1))
        self.stack.enter_context(patch.object(audit, "METADATA_ROWS", 2))
        self.stack.enter_context(patch.object(audit, "expected_days", return_value=days))
        self.stack.enter_context(patch.object(audit, "RAW_REPLAY_DAYS", tuple(sorted(days))))
        self.stack.enter_context(patch.object(audit, "PINNED_MANIFESTS", {
            key: plan[key]["sha256"] for key in ("trade_manifest", "metadata_manifest")}))
        self.stack.enter_context(patch.object(audit, "PINNED_REFS", {
            key: (item["bytes"], item["sha256"]) for key, item in plan["references"].items()}))
        self.stack.enter_context(patch.object(audit, "verify_source_snapshot", return_value={
            "commit": "a"*40, "files": 1, "proof_mode": "fixture_historical_commit"}))
        return fixture, trade_dir, metadata_dir

    def run_audit(self, trade_dir):
        con = duckdb.connect()
        self.addCleanup(con.close)
        self.con = con
        return audit.audit_saved_inputs(con, input_dir=trade_dir,
            candidate_sha256=audit.digest(trade_dir / "candidate_manifest.json"), expected_builder_head="a"*40)

    def run_replay(self, trade_dir):
        report, bindings = self.run_audit(trade_dir)
        target = self.root / "audit-run"
        target.mkdir()
        report, bindings = audit.replay_inputs(self.con, input_dir=trade_dir, audit_dir=target, report=report, bindings=bindings)
        body = target / "body_bindings.parquet"
        pq.write_table(pa.Table.from_pylist(bindings), body)
        report.update(audit_source_head="b"*40, body_bindings=audit._record(target, body, parquet=True))
        audit._write_json(target / "audit_candidate.json", report)
        return report, target

    def rewrite_day(self,trade_dir,day,role,table,*,sports_delta=0):
        """Update the whole synthetic receipt DAG, not merely a stale hash."""
        manifest=json.loads((trade_dir/'candidate_manifest.json').read_text())
        receipt_path=trade_dir/f'cells/day={day}/receipt.json'
        receipt=json.loads(receipt_path.read_text())
        path=trade_dir/'cells'/receipt['artifacts'][role]['path']
        pq.write_table(table,path)
        receipt['artifacts'][role].update(audit._record(trade_dir/'cells',path,parquet=True))
        receipt['sports_candidate_rows']+=sports_delta
        receipt_path.write_text(json.dumps(receipt,sort_keys=True))
        receipt_record=audit._record(trade_dir,receipt_path)
        index_path=trade_dir/manifest['daily_receipts']['path']
        index=json.loads(index_path.read_text())
        index['receipts']=[receipt_record if item['path']==receipt_record['path'] else item for item in index['receipts']]
        index_path.write_text(json.dumps(index,sort_keys=True));manifest['daily_receipts']=audit._record(trade_dir,index_path)
        stage_index_path=trade_dir/manifest['staging_receipts']['path']
        stage_index=json.loads(stage_index_path.read_text())
        for item in stage_index['receipts']:
            stage_path=trade_dir/item['path'];stage=json.loads(stage_path.read_text())
            if stage['entry']['path']!='trades_all/dt='+day+'/part-000.parquet':continue
            stage['callback_provenance']['day_receipt']=receipt_record
            stage_path.write_text(json.dumps(stage,sort_keys=True));item.update(audit._record(trade_dir,stage_path))
        stage_index_path.write_text(json.dumps(stage_index,sort_keys=True));manifest['staging_receipts']=audit._record(trade_dir,stage_index_path)
        if sports_delta:
            manifest['sports']['candidate_rows']+=sports_delta
            final_path=trade_dir/'cells/final/manifest.json';final=json.loads(final_path.read_text())
            final['sports_candidate_rows']+=sports_delta;final_path.write_text(json.dumps(final,sort_keys=True))
        (trade_dir/'candidate_manifest.json').write_text(json.dumps(manifest,sort_keys=True))

    def rewrite_final(self,trade_dir,role,table):
        candidate_path=trade_dir/'candidate_manifest.json';manifest=json.loads(candidate_path.read_text())
        path=trade_dir/manifest['artifacts'][role]['path'];pq.write_table(table,path)
        item={**manifest['artifacts'][role],**audit._record(trade_dir,path,parquet=True)};manifest['artifacts'][role]=item
        final_path=trade_dir/'cells/final/manifest.json';final=json.loads(final_path.read_text())
        final['artifacts'][role]={**item,'path':'final/'+role+'.parquet'}
        final_path.write_text(json.dumps(final,sort_keys=True));candidate_path.write_text(json.dumps(manifest,sort_keys=True))

    def test_complete_saved_audit_withholds_raw_dependent_acceptance(self):
        fixture, trade_dir, metadata_dir = self.make_inputs()
        report, bodies = self.run_audit(trade_dir)
        self.assertEqual(report["status"], "saved_proofs_passed_raw_replay_pending")
        self.assertEqual(report["counts"]["baseline_rows"], 6)
        self.assertEqual(report["final_cells"]["duration_tail_rows"], 4)
        self.assertEqual(report["metadata"]["known_compound_rows"], 1)
        self.assertEqual(set(report["checks"]), set(audit.CHECKS))
        for key in audit.RAW_DEPENDENT:
            self.assertIs(report["checks"][key], False)
        self.assertTrue(all(report["checks"][key] for key in audit.CHECKS if key not in audit.RAW_DEPENDENT))
        self.assertEqual(len({item["path"] for item in bodies}), len(bodies))
        self.assertTrue(any(item["path"].endswith("/day=2026-03-25/exclusions.parquet") for item in bodies))
        self.assertTrue(report["raw_replay_plan"]["complete_required_day_inventory"])
        self.assertFalse((trade_dir / "acceptance.json").exists())
        self.assertFalse((metadata_dir / "acceptance.json").exists())
        self.assertTrue(all(path.is_file() for path in fixture.paths.values()))

    def test_explicit_candidate_sha_cannot_be_bypassed(self):
        _, trade_dir, _ = self.make_inputs()
        con = duckdb.connect()
        self.addCleanup(con.close)
        with self.assertRaisesRegex(audit.AuditBlocked, "hash/drift"):
            audit.audit_saved_inputs(con, input_dir=trade_dir, candidate_sha256="0"*64, expected_builder_head="a"*40)

    def test_every_final_body_is_reopened(self):
        _, trade_dir, _ = self.make_inputs()
        manifest = json.loads((trade_dir / "candidate_manifest.json").read_text())
        path = trade_dir / manifest["artifacts"]["claim_dictionary"]["path"]
        path.write_bytes(path.read_bytes() + b"corrupt")
        with self.assertRaisesRegex(audit.AuditBlocked, "body size/hash"):
            self.run_audit(trade_dir)

    def test_daily_staging_receipt_source_binding_is_not_trusted(self):
        _, trade_dir, _ = self.make_inputs()
        path = trade_dir / "source_stage_v1/receipts/000000.json"
        payload = json.loads(path.read_text())
        payload["entry"]["sha256"] = "0"*64
        path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(audit.AuditBlocked, "body size/hash"):
            self.run_audit(trade_dir)

    def test_source_blob_validation_uses_explicit_historical_commit(self):
        blob = b"fixture committed source\n"
        head = "a"*40
        files = {path: hashlib.sha256(blob).hexdigest() for path in audit.SOURCE_FILES}
        commands = []

        def output(command, **kwargs):
            commands.append(command)
            return head + "\n" if command[1] == "rev-parse" else blob

        with patch.object(audit.subprocess, "check_output", side_effect=output):
            result = audit.verify_source_snapshot({"head": head, "files": files}, head)
        self.assertEqual(result["proof_mode"], "git_show_explicit_historical_builder_commit")
        self.assertTrue(all(command[2].startswith(head + ":") for command in commands if command[1] == "show"))
        self.assertFalse(any("HEAD:" in " ".join(command) for command in commands))

    def test_json_duplicate_nonfinite_path_and_no_overwrite_fail_closed(self):
        path = self.root / "audit.json"
        audit._write_json(path, {"status": "pending"})
        with self.assertRaises(FileExistsError):
            audit._write_json(path, {"status": "accepted"})
        for raw in ('{"a":1,"a":2}', '{"a":NaN}'):
            path.write_text(raw)
            with self.assertRaises(audit.AuditBlocked):
                audit.read_json(path)
        for value in ("../outside", "a//b", "/absolute", "a\\b", "a/./b"):
            with self.subTest(value=value), self.assertRaises(audit.AuditBlocked):
                audit._relative_path(value)

    def test_production_guard_precedes_source_and_body_io(self):
        args = SimpleNamespace(output_dir=self.root / "not-created", expected_audit_head="a"*40)
        with patch.object(audit.production_guard, "require_production_host", side_effect=RuntimeError("guard blocked")), \
             patch.object(audit.subprocess, "check_output") as git:
            with self.assertRaisesRegex(RuntimeError, "guard blocked"):
                audit.production_saved(args)
            git.assert_not_called()
        self.assertFalse(args.output_dir.exists())

    def test_moment_sums_psd_and_frequency_are_independently_checked(self):
        con = duckdb.connect()
        self.addCleanup(con.close)
        row = {"n_rows": 2, **{"a"+str(i): 0. for i in range(6)},
               **{f"c{i}_{j}": 0. for i in range(6) for j in range(i, 6)}}
        row.update(a0=1., c0_0=.5)
        con.register("moments", pa.Table.from_pylist([row]))
        self.assertEqual(audit._moments(con, "moments", 6)["observations"], 2)
        for update, reason in (({"a0": .5}, "H sum"), ({"c1_1": -1.}, "semidefinite"),
                               ({"n_rows": 0}, "frequency"), ({"a2": float("inf")}, "nonfinite")):
            con.unregister("moments")
            con.register("moments", pa.Table.from_pylist([{**row, **update}]))
            with self.subTest(update=update), self.assertRaisesRegex(audit.AuditBlocked, reason):
                audit._moments(con, "moments", 6)

    def test_independent_raw_replay_then_separate_publication(self):
        fixture, trade_dir, _ = self.make_inputs()
        before = (trade_dir / "candidate_manifest.json").read_bytes()
        report, target = self.run_replay(trade_dir)
        self.assertEqual(report["status"], "all_input_proofs_passed_pending_acceptance")
        self.assertTrue(all(value is True for value in report["checks"].values()))
        self.assertEqual(report["raw_replay"]["source_rows"], 6)
        self.assertEqual(len(report["raw_replay"]["proofs"]), 2)
        self.assertFalse((trade_dir / "acceptance.json").exists())
        args = dict(input_dir=trade_dir, candidate_sha256=audit.digest(trade_dir / "candidate_manifest.json"),
                    audit_dir=target, audit_sha256=audit.digest(target / "audit_candidate.json"), expected_audit_head="b"*40)
        result = audit.publish_acceptance(**args)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["manifest_sha256"], audit.digest(trade_dir / "manifest.json"))
        self.assertEqual(before, (trade_dir / "candidate_manifest.json").read_bytes())
        self.assertTrue(all(path.exists() for path in fixture.paths.values()))
        with self.assertRaisesRegex(audit.AuditBlocked, "overwritten"):
            audit.publish_acceptance(**args)

    def test_raw_replay_ceiling_blocks_before_any_transfer(self):
        _, trade_dir, _ = self.make_inputs()
        report, bindings = self.run_audit(trade_dir)
        target = self.root / "bounded-audit"
        target.mkdir()
        with patch.object(audit, "RAW_REPLAY_BYTES", 1), patch.object(staging, "stage_and_process") as transfer:
            with self.assertRaisesRegex(audit.AuditBlocked, "exceeds 2GB"):
                audit.replay_inputs(self.con, input_dir=trade_dir, audit_dir=target, report=report, bindings=bindings)
            transfer.assert_not_called()

    def test_raw_replay_disagreement_preserves_sources_and_never_accepts(self):
        fixture, trade_dir, _ = self.make_inputs()
        report, bindings = self.run_audit(trade_dir)
        target = self.root / "replay-disagreement"
        target.mkdir()
        # This simulates a builder disagreement without modifying any input body.
        with patch.object(audit, "_compare_replay_day", side_effect=audit.AuditBlocked("direct moments disagree")):
            with self.assertRaisesRegex(audit.AuditBlocked, "direct moments"):
                audit.replay_inputs(self.con, input_dir=trade_dir, audit_dir=target, report=report, bindings=bindings)
        self.assertFalse((trade_dir / "acceptance.json").exists())
        self.assertTrue(all(path.exists() for path in fixture.paths.values()))

    def test_publication_reopens_all_bodies_and_rejects_drift(self):
        _, trade_dir, _ = self.make_inputs()
        _, target = self.run_replay(trade_dir)
        body = trade_dir / "cells/day=2026-03-25/claim_raw.parquet"
        body.write_bytes(body.read_bytes() + b"drift")
        with self.assertRaisesRegex(audit.AuditBlocked, "body drift"):
            audit.publish_acceptance(input_dir=trade_dir, candidate_sha256=audit.digest(trade_dir / "candidate_manifest.json"),
                audit_dir=target, audit_sha256=audit.digest(target / "audit_candidate.json"), expected_audit_head="b"*40)
        self.assertFalse((trade_dir / "manifest.json").exists())
        self.assertFalse((trade_dir / "acceptance.json").exists())

    def test_independent_raw_normalization_price_orientation_and_first_failure(self):
        con = duckdb.connect()
        self.addCleanup(con.close)
        con.execute("CREATE TABLE audit_replay_metadata(ticker VARCHAR,event_cluster VARCHAR,category VARCHAR,market_reason VARCHAR,opening_seconds DOUBLE,endpoint_seconds DOUBLE,result VARCHAR)")
        con.execute("INSERT INTO audit_replay_metadata VALUES ('TEST','kalshi:event:GDP-232022 Q4','Economics','eligible',1772323200,1775001600,'yes')")
        con.execute("CREATE TEMP TABLE audit_sample_ids(trade_id UUID NOT NULL)")
        base = {"trade_id":"", "ticker":"TEST", "created_time":"2026-03-25T12:00:00Z", "trade_date":date(2026,3,25),
                "taker_side":"yes", "size":.25,"yes_price":.05,"no_price":.95,"source":"fixture"}
        changes = [{}, {"taker_side":"no"}, {"created_time":"2026-03-25T24:00:00Z"}, {"size":0},
                   {"ticker":"KXMVE-MISSING"}, {"ticker":"UNKNOWN"}, {"yes_price":.05,"no_price":.94},
                   {"created_time":"2026-03-25T12:00:00+01:00"}, {"trade_date":date(2026,3,26)}]
        rows = [{**base, **change, "trade_id":f"00000000-0000-0000-0000-{index+1:012d}"} for index,change in enumerate(changes)]
        path = self.root / "sample.parquet"
        pq.write_table(pa.Table.from_pylist(rows),path)
        audit._replay_prints(con,path,"2026-03-25",len(rows))
        observed=con.execute("SELECT eligibility_reason,P,Y,bin,event_cluster FROM audit_sample_prints ORDER BY source_ordinal").fetchall()
        self.assertEqual([row[0] for row in observed], ['eligible','eligible','invalid_timestamp','invalid_size','combo_excluded',
            'unadmitted_market','native_price_complement_disagreement','invalid_timestamp','invalid_or_mismatched_trade_date'])
        self.assertEqual(observed[0][1:],(.05,1,1,'kalshi:event:GDP-232022 Q4'))
        self.assertEqual(observed[1][1:],(.95,0,10,'kalshi:event:GDP-232022 Q4'))

    def test_audit_parquet_writer_has_hard_global_byte_ceiling(self):
        con=duckdb.connect()
        self.addCleanup(con.close)
        path=self.root/'too-large.parquet'
        with patch.object(audit,'OUTPUT_LIMIT',3),patch.object(audit,'PUBLICATION_RESERVE',0):
            with self.assertRaisesRegex(audit.AuditBlocked,'hard audit output'):
                audit._write_parquet(con,self.root,path,"SELECT 1 x")
        self.assertLessEqual(path.stat().st_size,3)

    def test_probe_actual_same_shaped_bytes_never_admit_inputs(self):
        _,trade_dir,metadata_dir=self.make_inputs()
        con=duckdb.connect();self.addCleanup(con.close)
        target=self.root/'capacity-probe';target.mkdir()
        report,bindings=audit.probe_inputs(con,metadata_dir=metadata_dir,
            metadata_manifest_sha256=audit.digest(metadata_dir/'candidate_manifest.json'),expected_builder_head='a'*40,audit_dir=target)
        self.assertEqual(report['status'],'capacity_probe_only_no_input_acceptance')
        self.assertEqual(report['actual_source_rows'],6)
        self.assertEqual(report['actual_combined_cell_bytes'],report['actual_daily_cell_bytes']+report['actual_final_cell_bytes'])
        self.assertEqual(len(report['days']),2)
        self.assertEqual(set(report['final_sample_artifacts']),{'baseline','duration','claim','categories','markets','claims',
            'cat_dictionary','price_dictionary','month_dictionary','claim_dictionary'})
        self.assertTrue(report['provisional_extrapolation']['not_capacity_admission'])
        self.assertTrue(bindings)
        self.assertNotIn('checks',report)
        self.assertFalse((trade_dir/'manifest.json').exists())
        self.assertFalse((trade_dir/'acceptance.json').exists())

    def test_publish_cannot_promote_a_saved_only_audit(self):
        _,trade_dir,_=self.make_inputs()
        report,_=self.run_audit(trade_dir)
        target=self.root/'saved-only';target.mkdir()
        report['audit_source_head']='b'*40
        audit._write_json(target/'audit_candidate.json',report)
        with self.assertRaisesRegex(audit.AuditBlocked,'binding differs'):
            audit.publish_acceptance(input_dir=trade_dir,candidate_sha256=audit.digest(trade_dir/'candidate_manifest.json'),
                audit_dir=target,audit_sha256=audit.digest(target/'audit_candidate.json'),expected_audit_head='b'*40)
        self.assertFalse((trade_dir/'acceptance.json').exists())

    def test_last_raw_row_excluded_uses_raw_not_analytic_count_bound(self):
        _,trade_dir,_=self.make_inputs(last_row_excluded=True)
        report,_=self.run_audit(trade_dir)
        self.assertEqual(report['counts']['source_rows'],8)
        self.assertEqual(report['counts']['baseline_rows'],6)
        self.assertEqual(report['counts']['duration_tail_rows'],4)
        self.assertEqual(report['counts']['excluded_rows'],2)
        for day in ('2026-03-25','2026-03-26'):
            receipt=json.loads((trade_dir/f'cells/day={day}/receipt.json').read_text())
            self.assertEqual((receipt['source_rows'],receipt['baseline_rows'],receipt['duration_rows']),(4,3,2))
            rows=pq.ParquetFile(trade_dir/'cells'/receipt['artifacts']['exclusions']['path']).read().to_pylist()
            self.assertEqual(rows,[{'source_ordinal':3,'eligibility_reason':'combo_excluded'}])

    def test_excluded_ordinal_outside_raw_footer_remains_blocked(self):
        _,trade_dir,_=self.make_inputs(last_row_excluded=True)
        day='2026-03-25';receipt_path=trade_dir/f'cells/day={day}/receipt.json'
        receipt=json.loads(receipt_path.read_text())
        manifest=json.loads((trade_dir/'candidate_manifest.json').read_text())
        entry={'trades_all/dt='+day+'/part-000.parquet':{}}
        path=trade_dir/'cells'/receipt['artifacts']['exclusions']['path']
        for invalid_ordinal in (-1,4):
            pq.write_table(pa.table({'source_ordinal':pa.array([invalid_ordinal],type=pa.int64()),
                                    'eligibility_reason':pa.array(['combo_excluded'],type=pa.string())}),path)
            receipt['artifacts']['exclusions']=audit._record(trade_dir/'cells',path,parquet=True)
            con=duckdb.connect();self.addCleanup(con.close)
            with self.subTest(source_ordinal=invalid_ordinal),patch.object(audit,'_receipt_index',return_value=[({'path':f'cells/day={day}/receipt.json'},receipt)]):
                with self.assertRaisesRegex(audit.AuditBlocked,'physical locator invalid'):
                    audit._daily_receipts(con,trade_dir,manifest,entry,audit.Binder())

    def test_final_claim_required_fields_and_tail_counts_are_null_safe(self):
        _,trade_dir,_=self.make_inputs()
        original=json.loads((trade_dir/'candidate_manifest.json').read_text())
        path=trade_dir/original['artifacts']['claims']['path']
        table=pq.ParquetFile(path).read();base=table.to_pylist()
        mutations=[{field:None} for field in ('claim_id','ticker','taker_side','event_cluster','category','Y','n_rows','n_d1','n_d10','duration_rows')]
        mutations += [{'taker_side':None,'Y':0},{'n_d1':-1},{'n_d10':-1},{'n_d1':7},{'n_d10':7},
                      {'duration_rows':-1},{'duration_rows':5},{'Y':2},{'n_d1':2**63-1,'n_d10':2**63-1}]
        for mutation in mutations:
            pq.write_table(pa.Table.from_pylist([{**row,**mutation} for row in base],schema=table.schema),path)
            manifest={**original,'artifacts':{**original['artifacts'],'claims':audit._record(trade_dir,path,parquet=True)}}
            con=duckdb.connect();self.addCleanup(con.close)
            with self.subTest(mutation=mutation),self.assertRaisesRegex(audit.AuditBlocked,'required saved fields/values invalid: claims'):
                audit._final_relations(con,trade_dir,manifest,audit.Binder())

    def test_every_daily_analytic_required_field_rejects_null_before_reduction(self):
        _,trade_dir,_=self.make_inputs()
        day='2026-03-26';receipt_path=trade_dir/f'cells/day={day}/receipt.json'
        original=json.loads(receipt_path.read_text());manifest=json.loads((trade_dir/'candidate_manifest.json').read_text())
        entries={'trades_all/dt='+day+'/part-000.parquet':{}}
        fields={role:tuple(audit._analytic_schema(role)) for role in
                ('baseline','duration_raw','claim_raw','categories','markets','claims','sports_candidates')}
        fields['markets']=tuple(field for field in fields['markets'] if field not in ('opening_seconds','endpoint_seconds'))
        for role,names in fields.items():
            path=trade_dir/'cells'/original['artifacts'][role]['path']
            snapshot=path.read_bytes();table=pq.ParquetFile(path).read();base=table.to_pylist()
            for field in names:
                rows=[{**row,field:None} for row in base];pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
                receipt={**original,'artifacts':{**original['artifacts'],role:{**original['artifacts'][role],**audit._record(trade_dir/'cells',path,parquet=True)}}}
                con=duckdb.connect();self.addCleanup(con.close)
                with self.subTest(role=role,field=field),patch.object(audit,'_receipt_index',return_value=[({'path':f'cells/day={day}/receipt.json'},receipt)]):
                    with self.assertRaisesRegex(audit.AuditBlocked,'required saved fields/values invalid: '+role):
                        audit._daily_receipts(con,trade_dir,manifest,entries,audit.Binder())
            path.write_bytes(snapshot)

    def test_nonfinite_daily_sums_moments_and_cached_values_fail_before_reduction(self):
        _,trade_dir,_=self.make_inputs()
        day='2026-03-26';original=json.loads((trade_dir/f'cells/day={day}/receipt.json').read_text())
        manifest=json.loads((trade_dir/'candidate_manifest.json').read_text())
        entries={'trades_all/dt='+day+'/part-000.parquet':{}}
        for role in ('baseline','duration_raw','claim_raw','sports_candidates'):
            path=trade_dir/'cells'/original['artifacts'][role]['path'];snapshot=path.read_bytes()
            table=pq.ParquetFile(path).read();base=table.to_pylist()
            for field,kind in audit._analytic_schema(role).items():
                if kind!='DOUBLE':continue
                for value in (float('nan'),float('inf'),float('-inf')):
                    rows=[{**row,field:value} for row in base]
                    pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
                    receipt={**original,'artifacts':{**original['artifacts'],role:{**original['artifacts'][role],**audit._record(trade_dir/'cells',path,parquet=True)}}}
                    con=duckdb.connect()
                    try:
                        with self.subTest(role=role,field=field,value=value),patch.object(audit,'_receipt_index',return_value=[({'path':f'cells/day={day}/receipt.json'},receipt)]):
                            with self.assertRaisesRegex(audit.AuditBlocked,'required saved fields/values invalid: '+role):
                                audit._daily_receipts(con,trade_dir,manifest,entries,audit.Binder())
                    finally:con.close()
            path.write_bytes(snapshot)

    def test_unsampled_day_sports_duplicate_cannot_hide_in_updated_receipt_dag(self):
        _,trade_dir,_=self.make_inputs();day='2026-03-26'
        path=trade_dir/f'cells/day={day}/sports_candidates.parquet';table=pq.ParquetFile(path).read()
        rows=table.to_pylist();rows.append(dict(rows[-1]))
        self.rewrite_day(trade_dir,day,'sports_candidates',pa.Table.from_pylist(rows,schema=table.schema),sports_delta=1)
        with patch.object(audit,'RAW_REPLAY_DAYS',('2026-03-25',)):
            with self.assertRaisesRegex(audit.AuditBlocked,'per-day per-claim sports cache multiplicity'):
                self.run_audit(trade_dir)

    def test_unsampled_cache_price_change_must_reconcile_native_winner_event_bins(self):
        _,trade_dir,_=self.make_inputs();day='2026-03-26'
        path=trade_dir/f'cells/day={day}/sports_candidates.parquet';table=pq.ParquetFile(path).read()
        rows=table.to_pylist();row=rows[0]
        row['P']=.06;row['payoff']=100*(row['Y']-row['P']);row['roi']=100*(row['Y']/row['P']-1)
        self.rewrite_day(trade_dir,day,'sports_candidates',pa.Table.from_pylist(rows,schema=table.schema))
        with patch.object(audit,'RAW_REPLAY_DAYS',('2026-03-25',)):
            with self.assertRaisesRegex(audit.AuditBlocked,'all-day sports cache/native winner-event baseline totals'):
                self.run_audit(trade_dir)

    def test_global_sports_cache_count_cannot_be_only_a_manifest_assertion(self):
        _,trade_dir,_=self.make_inputs()
        path=trade_dir/'candidate_manifest.json';manifest=json.loads(path.read_text())
        manifest['sports']['candidate_rows']+=1;path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(audit.AuditBlocked,'complete sports cache body/footer/manifest row counts'):
            self.run_audit(trade_dir)

    def test_unsampled_null_baseline_sum_cannot_disappear_through_sum(self):
        _,trade_dir,_=self.make_inputs();day='2026-03-26'
        path=trade_dir/f'cells/day={day}/baseline.parquet';table=pq.ParquetFile(path).read();rows=table.to_pylist()
        for row in rows:
            if row['bin']==6:row['sum_roi']=None
        self.rewrite_day(trade_dir,day,'baseline',pa.Table.from_pylist(rows,schema=table.schema))
        path=trade_dir/'cells/final/baseline.parquet';table=pq.ParquetFile(path).read();rows=table.to_pylist()
        for row in rows:
            if row['bin']==6:row['sum_roi']=100.
        self.rewrite_final(trade_dir,'baseline',pa.Table.from_pylist(rows,schema=table.schema))
        with patch.object(audit,'RAW_REPLAY_DAYS',('2026-03-25',)):
            with self.assertRaisesRegex(audit.AuditBlocked,'required saved fields/values invalid: baseline'):
                self.run_audit(trade_dir)

    def test_all_zero_duration_null_claim_counts_fail_full_saved_audit(self):
        _,trade_dir,_=self.make_inputs(missing_settlement_clocks=True)
        report,_=self.run_audit(trade_dir);self.assertEqual(report['counts']['duration_tail_rows'],0)
        for day in ('2026-03-25','2026-03-26'):
            path=trade_dir/f'cells/day={day}/claims.parquet';table=pq.ParquetFile(path).read()
            self.rewrite_day(trade_dir,day,'claims',pa.Table.from_pylist([{**row,'duration_rows':None} for row in table.to_pylist()],schema=table.schema))
        path=trade_dir/'cells/final/claims.parquet';table=pq.ParquetFile(path).read()
        self.rewrite_final(trade_dir,'claims',pa.Table.from_pylist([{**row,'duration_rows':None} for row in table.to_pylist()],schema=table.schema))
        with self.assertRaisesRegex(audit.AuditBlocked,'required saved fields/values invalid: claims'):
            self.run_audit(trade_dir)


if __name__ == "__main__":
    unittest.main()
