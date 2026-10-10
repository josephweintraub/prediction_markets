from __future__ import annotations

from datetime import datetime, timezone
import json
import io
from contextlib import redirect_stderr
from pathlib import Path
import tempfile
import resource
import signal
import unittest
from unittest.mock import patch

import duckdb

from scripts import rebuild_sports_wallet_samples as rebuild

HEAD = "1" * 40


def write_parquet(path, schema, rows):
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE fixture("+schema+")")
        if rows:
            con.executemany("INSERT INTO fixture VALUES("+",".join("?" for _ in rows[0])+")", rows)
        con.execute(f"COPY fixture TO '{rebuild.quote(path)}' (FORMAT PARQUET)")
    finally:
        con.close()


def alter(path, sql):
    con = duckdb.connect()
    replacement = path.with_suffix(".replacement.parquet")
    try:
        con.execute(f"CREATE TABLE changed AS SELECT * FROM read_parquet('{rebuild.quote(path)}')")
        con.execute(sql)
        con.execute(f"COPY changed TO '{rebuild.quote(replacement)}' (FORMAT PARQUET)")
        replacement.replace(path)
    finally:
        con.close()


def fixture(tmp_path):
    paths = {name: tmp_path/(name+".parquet") for name in rebuild.DATA_ROLES}
    base = 1750000000
    start = datetime.fromtimestamp(base, timezone.utc)
    end = datetime.fromtimestamp(base+100, timezone.utc)
    new_schema = ("sport VARCHAR,event_slug VARCHAR,market_id VARCHAR,market_date DATE,game_id VARCHAR,"
        "actual_start_utc TIMESTAMPTZ,actual_end_utc TIMESTAMPTZ,token_id VARCHAR,outcome VARCHAR,won BOOLEAN,"
        "block_number BIGINT,timestamp BIGINT,transaction_hash VARCHAR,log_index INTEGER,exchange_address VARCHAR,"
        "proxyWallet VARCHAR,counterparty VARCHAR,buyer_is_flagged_nonhuman BOOLEAN,price DOUBLE,usdc DOUBLE")
    new_rows = []
    for sport in (*rebuild.SPORTS[3:], "wta"):
        new_rows.append((sport,sport+"-event",sport+"-market",start.date(),sport+"-game",start,end,"yes","Yes",True,
                         100,base-100000,"tx-"+sport,1,"exchange","base","seller",False,.05,1.0000000000000002))
        if sport not in ("nhl", "wta"):
            new_rows.append((sport,sport+"-event",sport+"-market",start.date(),sport+"-game",start,end,"yes","Yes",True,
                             101,base,"tx-"+sport+"-actor",2,"exchange","actor","seller",True,.95,1.))
    for index, actor, flagged, price, timestamp in ((2,"actor",True,.95,base+100),
        (3,"nullable",False,.5,base), (4,"missing",False,.01,base),
        (5,"missing",False,.99,base), (6,"base",False,.4,base+101),
        (7,"base",False,0.,base), (8,"base",False,.05,base-100000)):
        new_rows.append(("nhl","nhl-event","nhl-market",start.date(),"nhl-game",start,end,"yes","Yes",True,
                         100+index,timestamp,"tx-nhl-"+str(index),index,"exchange",actor,"seller",flagged,price,1.))
    write_parquet(paths["new_exact"], new_schema, new_rows)
    write_parquet(paths["new_phase"], new_schema, [row for row in new_rows if row[12].startswith("tx-") and row[13]==1])
    for sport in ("nfl", "nba"):
        exact_schema = ("sport VARCHAR,market_id VARCHAR,token_id VARCHAR,block_number BIGINT,timestamp BIGINT,"
            "transaction_hash VARCHAR,log_index INTEGER,exchange_address VARCHAR,proxyWallet VARCHAR,counterparty VARCHAR,"
            "is_maker BOOLEAN,buyer_is_flagged_nonhuman BOOLEAN,price DOUBLE,usdc DOUBLE")
        rows = [(sport,sport+"-market","yes",100,base-1,"tx-"+sport+"-base",1,"exchange","BASE","seller",True,False,.5,1.),
                (sport,sport+"-market","yes",101,base+100,"tx-"+sport+"-actor",2,"exchange","actor","seller",False,True,.95,2.),
                (sport,sport+"-market","yes",102,base,"tx-"+sport+"-missing",3,"exchange","missing","seller",False,False,.5,1.)]
        write_parquet(paths[sport+"_exact"], exact_schema, rows)
        write_parquet(paths[sport+"_phase"], "market_id VARCHAR,game_id VARCHAR,official_date DATE,winning_token_id VARCHAR,actual_start_utc TIMESTAMPTZ,actual_end_utc TIMESTAMPTZ",
                      [(sport+"-market",sport+"-event",start.date(),"yes",start,end)])
    mlb_schema = ("market_id VARCHAR,token_id VARCHAR,block_number BIGINT,timestamp BIGINT,transaction_hash VARCHAR,"
        "log_index INTEGER,exchange_address VARCHAR,proxyWallet VARCHAR,counterparty VARCHAR,is_maker BOOLEAN,"
        "outcome VARCHAR,winning_outcome VARCHAR,price DOUBLE,usdcSize DOUBLE")
    old = [("mlb-market","yes",100,base-1,"tx-mlb-base",1,"exchange","base","seller",True,"Yes","Yes",.05,1.),
           ("mlb-market","yes",101,base+100,"tx-mlb-actor",2,"exchange","actor","seller",False,"Yes","Yes",.95,1.)]
    restored = old+[("mlb-market","yes",102,base,"tx-mlb-new",3,"exchange","missing","seller",True,"Yes","Yes",.5,1.),
                   ("outside","yes",103,base,"tx-mlb-outside",4,"exchange","base","seller",True,"Yes","Yes",.5,1.)]
    write_parquet(paths["old_mlb_exact"], mlb_schema, old)
    write_parquet(paths["restored_mlb_exact"], mlb_schema, restored)
    write_parquet(paths["mlb_phase"], "market_id VARCHAR,game_pk BIGINT,official_date DATE,winning_outcome VARCHAR,actual_start_utc TIMESTAMPTZ,actual_end_utc TIMESTAMPTZ",
                  [("mlb-market",123,start.date(),"Yes",start,end)])
    for role, rows in (("historic_flags",[("base",False),("actor",True),("nullable",None)]),
                       ("f0_flags",[("base",True),("actor",False),("nullable",False)]),
                       ("f1_flags",[("base",False),("actor",False),("nullable",False)])):
        write_parquet(paths[role], "proxyWallet VARCHAR,is_nonhuman BOOLEAN", rows)
    contract = {"schema_version": 1, "inputs": {}, "production_contract": False,
                "expected_counts": {"old_H": {"all": {"mlb":2,"nfl":3,"nba":3,"nhl":6,"cbb":2,"atp":2,"epl":2,"cfb":2,"wnba":2},
                                                      "filtered": {"mlb":1,"nfl":2,"nba":2,"nhl":3,"cbb":1,"atp":1,"epl":1,"cfb":1,"wnba":1}},
                                    "restored_H": {"all": {"mlb":3,"nfl":3,"nba":3,"nhl":6,"cbb":2,"atp":2,"epl":2,"cfb":2,"wnba":2},
                                                           "filtered": {"mlb":2,"nfl":2,"nba":2,"nhl":3,"cbb":1,"atp":1,"epl":1,"cfb":1,"wnba":1}}}}
    return paths, contract


def bind(paths, contract):
    contract["inputs"] = {name: {"path": str(path.resolve()), "bytes": path.stat().st_size,
                                "sha256": rebuild.sha256(path)} for name, path in paths.items()}
    return contract


def run_fixture(paths, contract, target):
    bind(paths, contract)
    reviewed = rebuild.preflight(paths, target, HEAD, contract)
    return rebuild.build_run(paths, target, HEAD, reviewed, contract, ["fixture_only"])


INVALID_INPUTS = [
    ("historic_flags", "INSERT INTO changed VALUES('BASE',false)", "nonunique normalized"),
    ("historic_flags", "UPDATE changed SET proxyWallet=' ' WHERE proxyWallet='base'", "flag keys"),
    ("f1_flags", "UPDATE changed SET is_nonhuman=NULL WHERE proxyWallet='base'", "new flag regime has null"),
    ("nfl_exact", "UPDATE changed SET proxyWallet=' ' WHERE proxyWallet='actor'", "invalid exact actor"),
    ("nba_exact", "INSERT INTO changed SELECT * FROM changed LIMIT 1", "duplicate native identity"),
    ("new_exact", "UPDATE changed SET buyer_is_flagged_nonhuman=NULL WHERE proxyWallet='base'", "null embedded"),
    ("restored_mlb_exact", "UPDATE changed SET usdcSize=usdcSize+1e-12 WHERE transaction_hash='tx-mlb-base'", "payload missing or changed"),
    ("new_phase", "UPDATE changed SET actual_end_utc=actual_end_utc+INTERVAL 1 SECOND WHERE sport='nhl'", "payload missing or changed"),
    ("mlb_phase", "INSERT INTO changed SELECT market_id,game_pk,official_date,winning_outcome,actual_start_utc,actual_end_utc+INTERVAL 1 SECOND FROM changed", "nonunique frozen"),
    ("nba_phase", "UPDATE changed SET actual_start_utc=NULL", "invalid frozen"),
]


class SampleStageTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="sports-samples-fixture-")
        self.addCleanup(temporary.cleanup)
        self.tmp_path = Path(temporary.name)
        for name, value in (("read_head", lambda: HEAD), ("available_memory", lambda: 200_000_000_000),
            ("CAPS", {**rebuild.CAPS, "memory_limit": "256MB", "memory_bytes": 256_000_000,
                      "threads": 1, "spill_bytes": 0, "output_bytes": 20_000_000,
                      "metadata_reserve_bytes": 2_000_000, "minimum_free_bytes": 0})):
            replacement = patch.object(rebuild, name, value)
            replacement.start()
            self.addCleanup(replacement.stop)

    def make_fixture(self, name="inputs"):
        folder = self.tmp_path/name
        folder.mkdir()
        return fixture(folder)

    def test_complete_stage_separates_coverage_policy_and_repair(self):
        paths, contract = self.make_fixture()
        result = run_fixture(paths, contract, self.tmp_path/"samples")
        self.assertEqual(result["status"], "sports_wallet_samples_complete")
        self.assertEqual((len(result["outputs"]), len(result["cases"])), (9,5))
        self.assertTrue(all(result["gates"].values()) and all(result["reconciliation"].values()))
        cases = result["cases"]
        self.assertEqual([cases[name]["observation_counts"]["mlb"] for name in
            ("historic_filtered","restored_historical","restored_legacy_recomputed","restored_repaired","restored_all")], [1,2,2,3,3])
        nhl = next(row for row in result["scenarios"]["restored_H"]["counts"] if row["sport"] == "nhl")
        self.assertEqual(tuple(nhl[name] for name in ("missing_flag_all","null_flag_all","extreme","post_end","invalid_price","pregame_all","end_equal_all")), (2,1,2,1,1,2,1))
        self.assertEqual((nhl["missing_flag_filtered"],nhl["null_flag_filtered"]), (0,1))
        self.assertTrue(all(len(case["engine_inputs"]) == 9 for case in cases.values()))
        con = duckdb.connect()
        self.addCleanup(con.close)
        output = self.tmp_path/"samples/F1/exact_buys.parquet"
        self.assertEqual(con.execute(f"SELECT count(*) FROM read_parquet('{output}') WHERE sport='wta'").fetchone()[0], 1)
        self.assertEqual(con.execute(f"SELECT count(*) FROM read_parquet('{output}') WHERE sport='nhl' AND price=.05").fetchone()[0], 2)
        self.assertEqual(con.execute(f"SELECT usdc FROM read_parquet('{output}') WHERE transaction_hash='tx-cbb'").fetchone()[0], 1.0000000000000002)
        self.assertEqual(json.loads((self.tmp_path/"samples/summary.json").read_text()), result)
        with self.assertRaisesRegex(ValueError, "destination exists"):
            run_fixture(paths, contract, self.tmp_path/"samples")

    def test_stale_embedded_historical_flags_each_branch_fail(self):
        for role in ("new_exact", "nfl_exact", "nba_exact"):
            with self.subTest(role=role):
                paths, contract = self.make_fixture(role)
                target = self.tmp_path/(role+"-sample")
                alter(paths[role], "UPDATE changed SET buyer_is_flagged_nonhuman=true WHERE lower(proxyWallet)='base'")
                with self.assertRaisesRegex(ValueError, "historical embedded flags differ"):
                    run_fixture(paths, contract, target)
                self.assertFalse(target.exists())
                evidence = list(self.tmp_path.glob("."+target.name+".staging-*/failure.json"))
                self.assertEqual(len(evidence), 1)
                self.assertEqual(json.loads(evidence[0].read_text())["status"], "blocked_sports_wallet_samples")

    def test_invalid_inputs_fail_closed(self):
        for index, (role, sql, match) in enumerate(INVALID_INPUTS):
            with self.subTest(role=role, mutation=sql):
                paths, contract = self.make_fixture(str(index))
                target = self.tmp_path/("samples-"+str(index))
                alter(paths[role], sql)
                with self.assertRaisesRegex(ValueError, match):
                    run_fixture(paths, contract, target)
                self.assertFalse(target.exists())

    def test_metadata_preflight_never_queries_trade_data(self):
        paths, contract = self.make_fixture()
        bind(paths, contract)
        with patch.object(rebuild.duckdb, "connect", side_effect=AssertionError("trade query in preflight")):
            receipt = rebuild.preflight(paths, self.tmp_path/"samples", HEAD, contract)
        self.assertFalse(receipt["full_input_hashes_verified"])
        self.assertLess(receipt["planned_read_bytes"], 1024**4)

    def test_drift_after_reviewed_preflight_blocks(self):
        paths, contract = self.make_fixture()
        bind(paths, contract)
        reviewed = rebuild.preflight(paths, self.tmp_path/"samples", HEAD, contract)
        alter(paths["nfl_exact"], "UPDATE changed SET usdc=usdc+1 WHERE proxyWallet='actor'")
        with self.assertRaisesRegex(ValueError, "byte contract|reviewed preflight"):
            rebuild.build_run(paths, self.tmp_path/"samples", HEAD, reviewed, contract)

    def test_source_drift_after_queries_preserves_failure(self):
        paths, contract = self.make_fixture()
        original = rebuild.audit_scenario
        with patch.object(rebuild, "read_head", return_value=HEAD) as source:
            def drifting(*args, **kwargs):
                value = original(*args, **kwargs)
                source.return_value = "2"*40
                return value
            with patch.object(rebuild, "audit_scenario", side_effect=drifting):
                with self.assertRaisesRegex(ValueError, "source drift"):
                    run_fixture(paths, contract, self.tmp_path/"samples")
        self.assertEqual(len(list(self.tmp_path.glob(".samples.staging-*/failure.json"))), 1)

    def test_bit_exact_comparison_distinguishes_signed_zero_and_preserves_null(self):
        con = duckdb.connect()
        self.addCleanup(con.close)
        rebuild.register_bits(con)
        con.execute("CREATE TABLE a(value DOUBLE)")
        con.execute("CREATE TABLE b(value DOUBLE)")
        con.executemany("INSERT INTO a VALUES(?)", [(0.,),(None,)])
        con.executemany("INSERT INTO b VALUES(?)", [(-0.,),(None,)])
        with self.assertRaisesRegex(ValueError, "payload missing or changed"):
            rebuild.payload_equal(con,"a","b",("value",))
        con.execute("DELETE FROM b")
        con.execute("INSERT INTO b SELECT * FROM a")
        rebuild.payload_equal(con,"a","b",("value",))
        con.execute("INSERT INTO b SELECT * FROM a LIMIT 1")
        with self.assertRaisesRegex(ValueError, "multiplicity changed"):
            rebuild.payload_equal(con,"a","b",("value",))

    def test_reviewed_destination_is_frozen(self):
        paths, contract = self.make_fixture()
        bind(paths, contract)
        reviewed = rebuild.preflight(paths, self.tmp_path/"samples", HEAD, contract)
        with self.assertRaisesRegex(ValueError, "reviewed preflight differs: target"):
            rebuild.build_run(paths, self.tmp_path/"other", HEAD, reviewed, contract)

    def test_output_drift_before_publication_preserves_failure(self):
        paths, contract = self.make_fixture()
        original = rebuild.audit_scenario
        def drifting(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[3] == "restored_F1":
                output = args[2]/"H/nfl_exact.parquet"
                alter(output, "UPDATE changed SET usdc=usdc+1")
            return result
        with patch.object(rebuild, "audit_scenario", side_effect=drifting):
            with self.assertRaisesRegex(ValueError, "output layout/content changed"):
                run_fixture(paths, contract, self.tmp_path/"samples")
        self.assertEqual(len(list(self.tmp_path.glob(".samples.staging-*/failure.json"))), 1)

    def test_paired_parent_contract_and_repair_binding_fail_closed(self):
        paths, _ = self.make_fixture()
        paths["paired_flags_manifest"] = (self.tmp_path/"manifest.json").resolve()
        paths["f0_flags"] = (self.tmp_path/"legacy_recomputed_flags.parquet").resolve()
        paths["f1_flags"] = (self.tmp_path/"wallet_flags.parquet").resolve()
        rows = {"total_rows":5,"admitted_rows":3,"excluded_before_start_rows":2,"null_timestamp_rows":0,"invalid_admitted_rows":0}
        paired = {"schema_version":"polymarket_wallet_flags_rebuild_v1","status":"wallet_flags_rebuild_complete",
            "data_certified":False,"downstream_adoption":"pending",
            "contract":{"timestamp_lower_inclusive":1590969600,"sides":"all published sides","timezone":"UTC",
                        "classifier":"analysis.bot_filter.build_wallet_flags","classifier_sha256":rebuild.CLASSIFIER_SHA,
                        "repair_only_pair":"corrected_vs_legacy_recomputed"},
            "source":{"head":HEAD,"sha256":{"analysis/bot_filter.py":rebuild.CLASSIFIER_SHA}},
            "binding":{"repair_manifest":{"sha256":rebuild.REPAIR_SHA},"repair_qa":{"sha256":rebuild.REPAIR_QA_SHA}},
            "inputs":{"historical_flags":{"pipeline_data":{"expected_content_sha256":rebuild.FROZEN_HASHES["historic_flags"]}}},
            "outputs":[{"path":paths[role].name,"sha256":"a"*64,"rows":3} for role in ("f0_flags","f1_flags")],
            "builds":{name:{"rows":dict(rows),"flags":{"trades":3,"invalid_keys":0,"invalid_payload":0,"wallets":3,"normalized_wallets":3,"flag_null_counts":{"is_nonhuman":0}}} for name in ("legacy","corrected")},
            "reconciliation":{name:True for name in ("admitted_trade_counts_conserved","unique_normalized_nonblank_wallets","non_null_boolean_flags","flag_outputs_exactly_reopened","all_inputs_layout_metadata_and_content_reopened","exact_source_wallet_keys_and_counts")}}
        self.assertEqual(set(rebuild.paired_flag_specs(paired,paths)), {"f0_flags","f1_flags"})
        for section, key, value, message in (("contract","timestamp_lower_inclusive",0,"population differs"),
                ("contract","classifier_sha256","b"*64,"classifier binding differs"),
                ("binding","repair_qa",{"sha256":"b"*64},"repair receipt"),
                ("reconciliation","non_null_boolean_flags",False,"reconciliation incomplete"),
                ("reconciliation","exact_source_wallet_keys_and_counts",False,"reconciliation incomplete")):
            with self.subTest(section=section,key=key):
                bad = json.loads(json.dumps(paired))
                bad[section][key] = value
                with self.assertRaisesRegex(ValueError,message):
                    rebuild.paired_flag_specs(bad,paths)
        bad = json.loads(json.dumps(paired))
        for build in ("legacy", "corrected"):
            bad["builds"][build]["rows"].update(null_timestamp_rows=1, excluded_before_start_rows=1)
        with self.assertRaisesRegex(ValueError, "paired build counts/gates differ"):
            rebuild.paired_flag_specs(bad, paths)

    def test_oversized_parquet_footer_is_rejected_before_metadata_parse(self):
        paths, contract = self.make_fixture()
        bind(paths,contract)
        with patch.dict(rebuild.CAPS, maximum_footer_bytes=1):
            with self.assertRaisesRegex(ValueError,"oversized Parquet footer"):
                rebuild.preflight(paths,self.tmp_path/"samples",HEAD,contract)

    def test_production_metadata_size_admitted_before_hashing(self):
        paths = {name:self.tmp_path/(name+".json") for name in rebuild.INPUT_ROLES}
        for name in rebuild.EVIDENCE_ROLES:
            paths[name].write_text("{}")
        with patch.dict(rebuild.CAPS, maximum_json_bytes=1), patch.object(rebuild,"sha256",side_effect=AssertionError("hash before size admission")):
            with self.assertRaisesRegex(ValueError,"metadata input size outside cap"):
                rebuild.production_contract(paths)

    def test_copy_write_ceiling_is_real_and_restores_process_limits(self):
        paths, contract = self.make_fixture()
        previous = resource.getrlimit(resource.RLIMIT_FSIZE)
        previous_signal = signal.getsignal(signal.SIGXFSZ)
        with patch.dict(rebuild.CAPS, output_bytes=rebuild.CAPS["metadata_reserve_bytes"]+128):
            with self.assertRaises((duckdb.IOException, duckdb.Error, OSError)):
                run_fixture(paths,contract,self.tmp_path/"samples")
        self.assertEqual(resource.getrlimit(resource.RLIMIT_FSIZE), previous)
        self.assertEqual(signal.getsignal(signal.SIGXFSZ), previous_signal)
        self.assertFalse((self.tmp_path/"samples").exists())
        failures = list(self.tmp_path.glob(".samples.staging-*/failure.json"))
        self.assertEqual(len(failures),1)
        partial = list(failures[0].parent.rglob("*.parquet"))
        self.assertTrue(all(path.stat().st_size <= 128 for path in partial))

    def test_atomic_publish_does_not_replace_existing_directory(self):
        target, staging = self.tmp_path/"complete", self.tmp_path/"staging"
        target.mkdir()
        staging.mkdir()
        (target/"original").write_text("preserve")
        with self.assertRaises(OSError):
            rebuild.atomic_publish(staging,target)
        self.assertEqual((target/"original").read_text(), "preserve")
        self.assertTrue(staging.exists())

    def test_cli_cannot_override_frozen_hashes_counts_or_resource_caps(self):
        required = [part for name in rebuild.INPUT_ROLES for part in ("--"+name.replace("_","-"),"/fixture/"+name)]
        required.extend(["--run-dir","/fixture/result","--expected-head",HEAD,"--preflight-only","--preflight-dir","/fixture/preflight"])
        for option in ("--expected-counts","--memory-limit","--expected-new-exact-sha256"):
            with self.subTest(option=option), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    rebuild.parse_args([*required,option,"1"])
        self.assertTrue(rebuild.parse_args(required).preflight_only)


if __name__ == "__main__":
    unittest.main()
