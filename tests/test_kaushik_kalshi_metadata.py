"""Synthetic DuckDB metadata fixtures; no production files or network."""
from datetime import datetime, timezone
import json
import unittest

import duckdb

from analysis.kaushik_kalshi_replication import inputs, metadata as m


def row(ticker="KXTEST-A", **changes):
    item = {"ticker": ticker, "event_ticker": "KXTEST-EVENT", "market_type": "binary",
            "status": "finalized", "result": "yes", "open_time": "2026-03-01T00:00:00Z",
            "settlement_ts": "2026-04-01T00:00:00Z", "notional_value": 1.0,
            "settlement_value": "", "_source": "bulk_v2", "title": "Will Alpha win?",
            "yes_sub_title": "Alpha", "no_sub_title": "Other"}
    item.update(changes)
    return item


def native_policy(**changes):
    values = {"payout_rule": "native_notional", "notional_value_unit": "dollars",
              "payout_evidence_rule": "fixture_current_api_dollars",
              "settlement_ts_encoding": "ISO8601_UTC", "settlement_ts_evidence_rule": "fixture_source_encoding"}
    values.update(changes)
    return m.SourceVersionPolicy(**values)


def venue_policy(**changes):
    values = {"payout_rule": "standard_binary_venue_contract", "notional_value_unit": "dollars",
              "payout_evidence_rule": "fixture_admitted_venue_binary_payout_terms_and_source_sentinel",
              "allow_missing_or_zero_notional_under_venue_rule": True,
              "settlement_ts_encoding": "ISO8601_UTC", "settlement_ts_evidence_rule": "fixture_source_encoding"}
    values.update(changes)
    return m.SourceVersionPolicy(**values)


class KalshiMetadataTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("CREATE TABLE events(event_ticker VARCHAR,category VARCHAR,series_ticker VARCHAR)")
        self.con.execute("INSERT INTO events VALUES ('KXTEST-EVENT','Sports','KXTEST')")
        self.con.execute("CREATE TABLE series(ticker VARCHAR,category VARCHAR)")
        self.con.execute("INSERT INTO series VALUES ('KXTEST','Politics')")
        self.con.execute("CREATE TABLE collections(collection_ticker VARCHAR,associated_event_tickers VARCHAR)")
        self.con.execute("INSERT INTO collections VALUES ('KXMVE-COLLECTION','[\"KXTEST-EVENT\"]')")
        self.refs = m.NativeReferences("events", "fixture_pinned_events_and_official_nonmultivariate_endpoint",
                                       "series", "fixture_pinned_series", "collections", "fixture_pinned_collections")

    def source(self, rows, name="source"):
        self.con.execute(f"CREATE TABLE {name}(ticker VARCHAR,event_ticker VARCHAR,market_type VARCHAR,status VARCHAR,result VARCHAR,"
                         "open_time VARCHAR,settlement_ts VARCHAR,notional_value DOUBLE,settlement_value VARCHAR,_source VARCHAR,"
                         "title VARCHAR,yes_sub_title VARCHAR,no_sub_title VARCHAR)")
        columns = list(row())
        if rows:
            self.con.executemany(f"INSERT INTO {name} VALUES (" + ",".join("?" for _ in columns) + ")",
                                 [[item.get(column) for column in columns] for item in rows])
        return name

    def project(self, rows, *, name="source", prefix="stage", source_shard="b1_0", policies=None):
        relation = self.source(rows, name)
        return m.project_metadata_shard(self.con, relation, source_shard=source_shard,
                    source_version_policy={"bulk_v2": native_policy()} if policies is None else policies,
                    native_references=self.refs, output_prefix=prefix)

    def projected(self, result):
        cursor = self.con.execute("SELECT * FROM " + result["projection_relation"] + " ORDER BY ticker")
        names = [field[0] for field in cursor.description]
        return [dict(zip(names, values)) for values in cursor.fetchall()]

    def test_event_membership_native_category_cluster_and_compact_sports_map(self):
        result = self.project([row()])
        prepared = self.projected(result)[0]
        self.assertEqual((prepared["market_reason"], prepared["combo_state"], prepared["category"]), ("eligible", "single", "Sports"))
        self.assertEqual(prepared["event_cluster"], "kalshi:event:KXTEST-EVENT")
        self.assertEqual(prepared["series_ticker"], "KXTEST")
        self.assertEqual(prepared["schema_version"], inputs.SCHEMA_VERSION)
        sports = self.con.execute("SELECT ticker,series_ticker,title,yes_sub_title FROM " + result["sports_relation"]).fetchone()
        self.assertEqual(sports, ("KXTEST-A", "KXTEST", "Will Alpha win?", "Alpha"))
        self.assertEqual(result["reference_health"]["series"]["role"], "audit_only_no_category_or_combo_precedence")

    def test_no_prefix_complement_assumption_and_known_compounds_override_events(self):
        rows = [row(), row("ORDINARY-UNKNOWN", event_ticker="NOT-IN-REFERENCE"),
                row("KXMVE-ABC"), row("KXOSCARWINNERS-ABC"), row("KXCITIESWEATHER-ABC")]
        result = self.project(rows)
        prepared = self.projected(result)
        self.assertEqual(len(prepared), 2)
        self.assertEqual(prepared[1]["market_reason"], "unadmitted_combo_classification")
        self.assertEqual(self.con.execute("SELECT count(*) FROM " + result["raw_keys_relation"]).fetchone()[0], 5)
        self.assertEqual(self.con.execute('SELECT sum("rows") FROM ' + result["combo_keys_relation"]).fetchone()[0], 3)
        self.assertEqual(self.con.execute('SELECT sum("rows") FROM ' + result["exclusions_relation"]).fetchone()[0], 5)
        self.assertEqual(self.con.execute('SELECT sum("rows") FROM ' + result["exclusions_relation"] + " WHERE market_reason='combo_excluded'").fetchone()[0], 3)

    def test_native_legacy_event_internal_ascii_space_is_opaque_and_eligible(self):
        event = "GDP-232022 Q4"
        self.con.execute("INSERT INTO events VALUES (?, 'Economics', 'KXGDP')", [event])
        result = self.project([row("GDP-MARKET", event_ticker=event)])
        prepared = self.projected(result)[0]
        self.assertEqual(prepared["event_ticker"], event)
        self.assertEqual(prepared["event_cluster"], "kalshi:event:" + event)
        self.assertEqual((prepared["market_reason"], prepared["category"]), ("eligible", "Economics"))
        normalized = inputs.normalize_trade(
            {"trade_id": "fixture", "ticker": "GDP-MARKET", "created_time": "2026-03-25T00:00:00Z",
             "trade_date": "2026-03-25", "taker_side": "yes", "size": 1,
             "yes_price": .05, "no_price": .95, "source": "fixture"},
            prepared, source_date="2026-03-25", source_ordinal=0)
        self.assertTrue(normalized["baseline_eligible"])
        self.assertEqual(normalized["event_cluster"], "kalshi:event:" + event)

    def test_native_identifier_still_rejects_edge_space_tabs_newlines_and_controls(self):
        values = [" GDP-232022 Q4", "GDP-232022 Q4 ", "GDP-232022\tQ4",
                  "GDP-232022\nQ4", "GDP-232022\rQ4", "GDP-232022\x01Q4",
                  "GDP-232022\x7fQ4", "GDP-232022\u0085Q4", "GDP-232022\u2003Q4"]
        self.con.execute("CREATE TABLE identifiers(raw VARCHAR)")
        self.con.executemany("INSERT INTO identifiers VALUES (?)", [(value,) for value in values])
        self.assertEqual(self.con.execute("SELECT count(*) FROM identifiers WHERE " + m._valid_identifier("raw")).fetchone()[0], 0)
        for index, event in enumerate(values):
            with self.subTest(event=repr(event)):
                result = self.project([row("BAD-" + str(index), event_ticker=event)],
                                      name="bad_source_" + str(index), prefix="bad_stage_" + str(index))
                self.assertNotEqual(self.projected(result)[0]["market_reason"], "eligible")

    def test_strict_dollar_notional_never_guesses_legacy_cents_or_sentinels(self):
        result = self.project([row("ONE"), row("HUNDRED", notional_value=100), row("ZERO", notional_value=0),
                               row("MISSING", notional_value=None), row("NAN", notional_value=float("nan"))])
        prepared = {item["ticker"]: item for item in self.projected(result)}
        self.assertEqual(prepared["ONE"]["market_reason"], "eligible")
        for ticker in ("HUNDRED", "ZERO", "MISSING", "NAN"):
            self.assertEqual(prepared[ticker]["market_reason"], "missing_or_nonunit_face_value")
        self.assertEqual(prepared["HUNDRED"]["face_value_dollars"], 100)
        self.assertEqual(prepared["ZERO"]["raw_notional_value"], 0)

    def test_explicit_venue_rule_keeps_zero_missing_raw_and_rejects_positive_contradictions(self):
        rows = [row("ZERO", _source="forward", notional_value=0),
                row("MISSING", _source="event_v1", notional_value=None),
                row("BAD", _source="forward", notional_value=100)]
        result = self.project(rows, policies={"forward": venue_policy(), "event_v1": venue_policy()})
        prepared = {item["ticker"]: item for item in self.projected(result)}
        for ticker, raw in (("ZERO", 0), ("MISSING", None)):
            self.assertEqual(prepared[ticker]["market_reason"], "eligible")
            self.assertEqual(prepared[ticker]["face_value_dollars"], 1)
            self.assertEqual(prepared[ticker]["raw_notional_value"], raw)
            self.assertEqual(prepared[ticker]["face_value_basis"], "admitted_venue_standard_binary")
        self.assertEqual(prepared["BAD"]["market_reason"], "missing_or_nonunit_face_value")
        self.assertEqual(self.con.execute("SELECT notional_value FROM source WHERE ticker='ZERO'").fetchone()[0], 0)

    def test_source_policy_absence_never_falls_back(self):
        result = self.project([row(_source="unadmitted-new-version")])
        prepared = self.projected(result)[0]
        self.assertEqual(prepared["market_reason"], "unadmitted_unit_payout")
        self.assertIsNone(prepared["endpoint_seconds"])
        self.assertIsNone(prepared["payout_evidence_rule"])

    def test_missing_unadmitted_or_malformed_clocks_preserve_baseline(self):
        result = self.project([row("MISSING", settlement_ts=None), row("BAD", settlement_ts="not a clock"),
                               row("NAIVE", settlement_ts="2026-04-01T00:00:00"),
                               row("OFFSET", settlement_ts="2026-04-01T01:00:00+01:00"),
                               row("BAD-OPEN", open_time="bad")])
        for prepared in self.projected(result):
            self.assertEqual(prepared["market_reason"], "eligible")
            self.assertTrue(prepared["endpoint_seconds"] is None or prepared["opening_seconds"] is None)
            trade = {"trade_id": "id", "ticker": prepared["ticker"], "created_time": "2026-03-25T00:00:00Z",
                     "trade_date": "2026-03-25", "taker_side": "yes", "size": .25,
                     "yes_price": .05, "no_price": .95, "source": "fixture"}
            normalized = inputs.normalize_trade(trade, prepared, source_date="2026-03-25", source_ordinal=0)
            self.assertTrue(normalized["baseline_eligible"])
            self.assertFalse(normalized["duration_eligible"])

    def test_explicit_unix_string_encoding_and_utc_subsecond_precision(self):
        timestamp = datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp() + .125
        result = self.project([row(settlement_ts=str(timestamp))],
                              policies={"bulk_v2": native_policy(settlement_ts_encoding="unix_seconds_string")})
        self.assertEqual(self.projected(result)[0]["endpoint_seconds"], timestamp)
        iso = self.project([row(settlement_ts="2026-04-01T00:00:00.125Z")], name="source2", prefix="stage2", source_shard="b1_1")
        self.assertEqual(self.projected(iso)[0]["endpoint_seconds"], timestamp)
        bad = self.project([row(settlement_ts=str(timestamp * 1000))], name="source3", prefix="stage3", source_shard="b1_2",
                           policies={"bulk_v2": native_policy(settlement_ts_encoding="unix_seconds_string")})
        self.assertIsNone(self.projected(bad)[0]["endpoint_seconds"])

    def test_metadata_clock_shape_matches_shared_python_oracle(self):
        cases = ["2026-03-01T00:00:00Z", "2026-03-01T00:00:00.123456Z",
                 "2026-03-01T00:00:00+00:00", "2026-03-01T24:00:00Z",
                 "2026-3-01T00:00:00Z", "2026-03-1T00:00:00Z",
                 "2026-03-01T00:00:00+01:00", "2026-03-01T00:00:00.1234567Z",
                 "2026-02-30T00:00:00Z", None]
        self.con.execute("CREATE TABLE clocks(raw VARCHAR)")
        self.con.executemany("INSERT INTO clocks VALUES (?)", [(value,) for value in cases])
        results = self.con.execute("SELECT raw," + m._iso_seconds("raw") + " FROM clocks").fetchall()
        for raw, seconds in results:
            with self.subTest(raw=raw):
                self.assertEqual(seconds, inputs._seconds(raw, "ISO8601_UTC"))
        for raw in cases[3:]:
            self.assertIsNone(inputs._seconds(raw, "ISO8601_UTC"))

    def test_settlement_value_units_and_result_conflicts_are_explicit(self):
        result = self.project([row("UNITS", settlement_value="1")])
        self.assertEqual(self.projected(result)[0]["market_reason"], "unadmitted_settlement_value_units")
        policy = native_policy(settlement_value_unit="dollars", settlement_value_evidence_rule="fixture_verified_settlement_dollars")
        checked = self.project([row("AGREE", settlement_value="1"), row("CONFLICT", settlement_value="0"),
                                row("INVALID", settlement_value="bad"), row("LOSS", result="no", settlement_value="0")],
                               name="checked", prefix="checked", policies={"bulk_v2": policy})
        reasons = {item["ticker"]: item["market_reason"] for item in self.projected(checked)}
        self.assertEqual(reasons, {"AGREE": "eligible", "CONFLICT": "settlement_result_conflict",
                                   "INVALID": "settlement_result_conflict", "LOSS": "eligible"})

    def test_unresolved_scalar_and_category_missing_are_retained_with_reasons(self):
        self.con.execute("UPDATE events SET category=NULL")
        result = self.project([row(), row("ACTIVE", status="active"), row("SCALAR", result="scalar"), row("TYPE", market_type="scalar")])
        prepared = {item["ticker"]: item for item in self.projected(result)}
        self.assertEqual(prepared["KXTEST-A"]["category"], "Unclassified")
        self.assertEqual(prepared["KXTEST-A"]["market_reason"], "eligible")
        self.assertEqual(prepared["ACTIVE"]["market_reason"], "not_finalized")
        self.assertEqual(prepared["SCALAR"]["market_reason"], "nonbinary_or_missing_result")
        self.assertEqual(prepared["TYPE"]["market_reason"], "nonbinary_or_missing_market_type")

    def test_duplicate_metadata_is_never_distincted_and_conflicts_survive(self):
        with self.assertRaisesRegex(m.MetadataBlocked, "uniqueness") as caught:
            self.project([row(), row(result="no")])
        health = caught.exception.health
        self.assertEqual((health["rows"], health["duplicate_ticker_keys"], health["conflicting_result_keys"]), (2, 1, 1))
        self.assertEqual(self.con.execute("SELECT count(*) FROM stage_raw_keys").fetchone()[0], 2)

    def test_native_event_join_fanout_is_blocked_without_resolving_precedence(self):
        self.con.execute("INSERT INTO events VALUES ('KXTEST-EVENT','Politics','OTHER')")
        with self.assertRaisesRegex(m.MetadataBlocked, "events reference uniqueness") as caught:
            self.project([row()])
        self.assertEqual((caught.exception.health["rows"], caught.exception.health["distinct_keys"]), (2, 1))
        self.assertEqual(self.con.execute("SELECT count(*) FROM stage_raw_keys").fetchone()[0], 1)

    def test_global_gate_catches_duplicate_excluded_compounds_across_shards(self):
        first = self.project([row("KXMVE-DUPLICATE")], source_shard="b1_0")
        second = self.project([row("KXMVE-DUPLICATE", result="no")], name="source2", prefix="stage2", source_shard="f1_0")
        self.con.execute("CREATE VIEW all_keys AS SELECT * FROM " + first["raw_keys_relation"] + " UNION ALL SELECT * FROM " + second["raw_keys_relation"])
        with self.assertRaisesRegex(m.MetadataBlocked, "global metadata ticker uniqueness") as caught:
            m.validate_global_metadata_keys(self.con, "all_keys", expected_source_shards=["b1_0", "f1_0"])
        self.assertEqual((caught.exception.health["rows"], caught.exception.health["conflicting_result_keys"]), (2, 1))
        self.assertEqual(self.con.execute("SELECT count(*) FROM " + first["projection_relation"]).fetchone()[0], 0)

    def test_global_gate_requires_exact_inventory_and_rejects_identical_duplicates(self):
        first = self.project([row()], source_shard="b1_0")
        second = self.project([row()], name="source2", prefix="stage2", source_shard="b1_1")
        self.con.execute("CREATE VIEW all_keys AS SELECT * FROM " + first["raw_keys_relation"] + " UNION ALL SELECT * FROM " + second["raw_keys_relation"])
        with self.assertRaisesRegex(m.MetadataBlocked, "uniqueness") as caught:
            m.validate_global_metadata_keys(self.con, "all_keys", expected_source_shards=["b1_0", "b1_1"])
        self.assertEqual(caught.exception.health["conflicting_result_keys"], 0)
        with self.assertRaisesRegex(m.MetadataBlocked, "coverage"):
            m.validate_global_metadata_keys(self.con, first["raw_keys_relation"], expected_source_shards=["b1_0", "missing"])
        health = m.validate_global_metadata_keys(self.con, first["raw_keys_relation"], expected_source_shards=["b1_0"])
        self.assertEqual(health["duplicate_ticker_keys"], 0)

    def test_inspection_is_bounded_records_raw_units_and_does_not_admit_policy(self):
        source = self.source([row(), row("ZERO", _source="forward", notional_value=0),
                              row("MISSING", _source="event_v1", notional_value=None),
                              row("TEXT", settlement_ts="bad", settlement_value="bad")])
        summary = m.inspect_metadata_shard(self.con, source, maximum_notional_frequencies=1)
        groups = {item["source_version"]: item for item in summary["source_summaries"]}
        self.assertEqual(groups["forward"]["notional_zero"], 1)
        self.assertEqual(groups["event_v1"]["notional_missing"], 1)
        self.assertEqual(groups["bulk_v2"]["endpoint_parseable_iso8601_utc"], 1)
        self.assertEqual(groups["bulk_v2"]["settlement_value_present"], 1)
        self.assertEqual(summary["key_health"]["rows"], 4)
        self.assertEqual(len(summary["notional_frequencies"]), 3)
        self.assertTrue(summary["clock_parseability_is_not_encoding_admission"])
        json.dumps(summary, allow_nan=False)

    def test_policy_evidence_and_immutable_relation_names_fail_closed(self):
        for options in ({"payout_rule": "native_notional"}, {"notional_value_unit": "cents"},
                        {"settlement_ts_encoding": "ISO8601_UTC"}, {"settlement_value_unit": "dollars"},
                        {"allow_missing_or_zero_notional_under_venue_rule": True}):
            with self.subTest(options=options), self.assertRaises(m.MetadataBlocked):
                m.SourceVersionPolicy(**options)
        result = self.project([row()])
        with self.assertRaisesRegex(m.MetadataBlocked, "immutable metadata projection"):
            m.project_metadata_shard(self.con, "source", source_shard="b1_0", source_version_policy={"bulk_v2": native_policy()},
                                     native_references=self.refs, output_prefix="stage")
        self.assertEqual(self.con.execute("SELECT count(*) FROM " + result["projection_relation"]).fetchone()[0], 1)

    def test_explicit_cleanup_drops_only_owned_temporary_views(self):
        result = self.project([row()])
        m.drop_projection_views(self.con, result)
        for name in result["temporary_relations"]:
            self.assertEqual(self.con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name=?", [name]).fetchone()[0], 0)
        self.assertEqual(self.con.execute("SELECT count(*) FROM source").fetchone()[0], 1)
        self.assertEqual(self.con.execute("SELECT count(*) FROM events").fetchone()[0], 1)

    def test_cleanup_validates_all_names_before_any_drop(self):
        result = self.project([row()])
        self.con.execute("CREATE TABLE user_owned(value INTEGER)")
        changed = {**result, "temporary_relations": [*result["temporary_relations"], "user_owned"]}
        with self.assertRaisesRegex(m.MetadataBlocked, "cleanup refused"):
            m.drop_projection_views(self.con, changed)
        self.assertEqual(self.con.execute("SELECT count(*) FROM " + result["projection_relation"]).fetchone()[0], 1)
        self.assertEqual(self.con.execute("SELECT count(*) FROM user_owned").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
