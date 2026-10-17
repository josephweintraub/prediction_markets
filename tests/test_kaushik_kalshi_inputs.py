"""Small synthetic contracts; no production data or network access."""
from datetime import date, datetime, timezone
import math
import unittest

from analysis.kaushik_kalshi_replication import inputs as k


def seconds(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def market_raw(**changes):
    row = {"ticker": "KXTEST-26APR01-YES", "event_ticker": "KXTEST-26APR01",
           "market_type": "binary", "status": "finalized", "result": "yes",
           "open_time": "2026-03-01T00:00:00Z",
           "settlement_ts": seconds("2026-04-01T00:00:00Z")}
    row.update(changes)
    return row


def prepare(raw=None, **changes):
    options = {"combo_state": "single", "combo_evidence_rule": "fixture_native_single_contract",
               "category": "Sports", "category_evidence_rule": "fixture_admitted_native_category",
               "unit_payoff_admitted": True, "face_value_dollars": 1.0,
               "payout_evidence_rule": "fixture_verified_dollar_face_value",
               "settlement_ts_encoding": "unix_seconds"}
    options.update(changes)
    return k.prepare_market(market_raw() if raw is None else raw, **options)


def trade(**changes):
    row = {"trade_id": "native-trade-1", "ticker": "KXTEST-26APR01-YES",
           "created_time": "2026-03-25T00:00:00.100669Z", "trade_date": date(2026, 3, 25),
           "taker_side": "yes", "size": .125, "yes_price": .05, "no_price": .95,
           "source": "catchup1"}
    row.update(changes)
    return row


def normalize(raw=None, market=None, **changes):
    options = {"source_date": "2026-03-25", "source_ordinal": 0}
    options.update(changes)
    return k.normalize_trade(trade() if raw is None else raw,
                             prepare() if market is None else market, **options)


class KalshiInputTests(unittest.TestCase):
    def test_yes_no_claims_and_fractional_size_have_one_print_weight(self):
        yes = normalize()
        no = normalize(trade(taker_side="no"))
        self.assertEqual((yes["P"], yes["Y"], yes["weight"], yes["size"]), (.05, 1, 1, .125))
        self.assertAlmostEqual(yes["payoff"], 95)
        self.assertAlmostEqual(yes["roi"], 1900)
        self.assertAlmostEqual(yes["capital"], .00625)
        self.assertEqual((no["P"], no["Y"], no["weight"]), (.95, 0, 1))
        self.assertAlmostEqual(no["payoff"], -95)
        self.assertAlmostEqual(no["roi"], -100)
        self.assertNotEqual(yes["claim_id"], no["claim_id"])
        self.assertEqual(yes["event_cluster"], no["event_cluster"])
        losing_yes = normalize(market=prepare(market_raw(result="no")))
        winning_no = normalize(trade(taker_side="no"), prepare(market_raw(result="no")))
        self.assertEqual((losing_yes["Y"], winning_no["Y"]), (0, 1))

    def test_selects_recorded_binary64_no_price_without_reconstruction(self):
        yes, no = .8999999999999999, .1
        self.assertNotEqual(no, 1 - yes)
        result = normalize(trade(taker_side="no", yes_price=yes, no_price=no))
        self.assertTrue(result["baseline_eligible"])
        self.assertEqual(result["P"].hex(), no.hex())
        self.assertEqual(result["bin"], 2)

    def test_price_bin_edges_and_no_default_extreme_trim(self):
        for price, expected in ((.009, 1), (math.nextafter(.1, 0), 1), (.1, 2),
                                (.2, 3), (.3, 4), (math.nextafter(.9, 0), 9),
                                (.9, 10), (.999, 10)):
            with self.subTest(price=price):
                result = normalize(trade(yes_price=price, no_price=1-price))
                self.assertTrue(result["baseline_eligible"])
                self.assertEqual(result["bin"], expected)

    def test_all_available_scope_has_no_march_25_cutoff(self):
        self.assertTrue(normalize()["baseline_eligible"])
        result = normalize(trade(created_time="2026-10-01T23:59:59Z", trade_date="2026-10-01"),
                           source_date="2026-10-01")
        self.assertTrue(result["baseline_eligible"])
        self.assertFalse(result["duration_eligible"])
        contract = k.definitions(settlement_ts_encoding="unix_seconds")
        self.assertEqual(contract["sample_scope"], "all_available_dates")
        self.assertIsNone(contract["cutoff_utc"])

    def test_native_timestamp_precision_and_date_partition(self):
        result = normalize()
        self.assertEqual(result["timestamp"], seconds("2026-03-25T00:00:00.100669Z"))
        self.assertEqual(result["source_ordinal"], 0)
        self.assertEqual(result["trade_month"], "2026-03")
        self.assertEqual(normalize(trade(trade_date="2026-03-24"))["eligibility_reason"],
                         "invalid_or_mismatched_trade_date")
        self.assertEqual(normalize(source_date="2026-03-24")["eligibility_reason"], "source_day_mismatch")
        for timestamp in (None, "bad", "2026-03-25T00:00:00", "2026-03-25T01:00:00+01:00"):
            with self.subTest(timestamp=timestamp):
                self.assertEqual(normalize(trade(created_time=timestamp))["eligibility_reason"], "invalid_timestamp")

    def test_source_inventory_bounds_and_locator_contract(self):
        for day in ("2021-06-29", "2026-10-02", "invalid", None):
            with self.subTest(day=day), self.assertRaisesRegex(k.InputBlocked, "source date"):
                normalize(source_date=day)
        for ordinal in (-1, True, .5, None):
            with self.subTest(ordinal=ordinal), self.assertRaisesRegex(k.InputBlocked, "ordinal"):
                normalize(source_ordinal=ordinal)
        first = normalize(trade(created_time="2021-06-30T00:00:00Z", trade_date="2021-06-30"),
                          source_date="2021-06-30")
        self.assertTrue(first["baseline_eligible"])

    def test_missing_settlement_does_not_use_close_or_expiration(self):
        raw = market_raw(settlement_ts=None, close_time="2026-04-01T00:00:00Z",
                         expiration_time="2026-04-01T00:00:00Z")
        result = normalize(market=prepare(raw))
        self.assertTrue(result["baseline_eligible"])
        self.assertFalse(result["duration_eligible"])
        self.assertEqual(result["duration_reason"], "missing_or_invalid_settlement_ts")
        self.assertIsNone(result["endpoint_seconds"])
        self.assertIsNone(result["xR"])

    def test_invalid_or_missing_clocks_only_remove_duration(self):
        for changes, expected in (({"open_time": None}, "missing_or_invalid_open_time"),
                                  ({"settlement_ts": "bad"}, "missing_or_invalid_settlement_ts"),
                                  ({"settlement_ts": seconds("2026-02-28T00:00:00Z")}, "nonpositive_lifespan"),
                                  ({"open_time": "2026-03-26T00:00:00Z"}, "trade_before_open_time"),
                                  ({"settlement_ts": seconds("2026-03-24T00:00:00Z")}, "trade_after_settlement_ts")):
            with self.subTest(changes=changes):
                result = normalize(market=prepare(market_raw(**changes)))
                self.assertTrue(result["baseline_eligible"])
                self.assertEqual(result["duration_reason"], expected)
                self.assertIsNone(result["xL"])

    def test_duration_endpoints_inclusive_with_unrounded_days(self):
        raw_market = market_raw(open_time="2026-03-25T00:00:00Z",
                                settlement_ts=seconds("2026-03-26T00:00:00Z"))
        opened = normalize(trade(created_time="2026-03-25T00:00:00Z"), prepare(raw_market))
        self.assertEqual((opened["L"], opened["R"], opened["xL"], opened["xR"]), (1, 1, 1, 1))
        settled = normalize(trade(created_time="2026-03-26T00:00:00Z", trade_date="2026-03-26"),
                            prepare(raw_market), source_date="2026-03-26")
        self.assertTrue(settled["duration_eligible"])
        self.assertEqual((settled["R"], settled["xR"]), (0, 0))
        middle = normalize(trade(yes_price=.5, no_price=.5), prepare(raw_market))
        self.assertTrue(middle["baseline_eligible"])
        self.assertEqual(middle["duration_reason"], "not_tail")
        self.assertIsNone(middle["xL"])

    def test_explicit_settlement_encoding_never_guesses_units(self):
        iso = prepare(market_raw(settlement_ts="2026-04-01T00:00:00Z"),
                      settlement_ts_encoding="ISO8601_UTC")
        self.assertTrue(normalize(market=iso)["duration_eligible"])
        wrong = prepare(market_raw(settlement_ts="2026-04-01T00:00:00Z"))
        self.assertIsNone(wrong["endpoint_seconds"])
        milliseconds = prepare(market_raw(settlement_ts=seconds("2026-04-01T00:00:00Z")*1000))
        self.assertIsNone(milliseconds["endpoint_seconds"])
        with self.assertRaisesRegex(k.InputBlocked, "encoding"):
            prepare(settlement_ts_encoding="infer_from_value")

    def test_canonical_utc_format_and_microsecond_precision_are_explicit(self):
        for timestamp in ('2026-03-25T12:34:56Z','2026-03-25 12:34:56+00:00',
                          '2026-03-25T12:34:56.1-00:00','2026-03-25T12:34:56.123456Z'):
            with self.subTest(timestamp=timestamp):
                self.assertTrue(normalize(trade(created_time=timestamp))['baseline_eligible'])
        for timestamp in ('2026-3-25T12:34:56Z','2026-03-25T24:00:00Z',
                          '2026-03-25T12:60:00Z','2026-03-25T12:34:60Z',
                          '2026-03-25T12:34:56.1234567Z','2026-02-30T12:34:56Z',
                          '2026-03-25T12:34:56+00','2026-03-25T12:34:56+01:00'):
            with self.subTest(timestamp=timestamp):
                self.assertEqual(normalize(trade(created_time=timestamp))['eligibility_reason'],'invalid_timestamp')
        invalid=prepare(market_raw(open_time='2026-03-25T24:00:00Z',settlement_ts='2026-03-25T12:34:56.1234567Z'),
                        settlement_ts_encoding='ISO8601_UTC')
        self.assertIsNone(invalid['opening_seconds'])
        self.assertIsNone(invalid['endpoint_seconds'])

    def test_unit_payout_requires_explicit_normalized_dollars(self):
        raw = market_raw(notional_value=100)
        self.assertTrue(normalize(market=prepare(raw))["baseline_eligible"])
        for options, expected in (({"unit_payoff_admitted": False}, "unadmitted_unit_payout"),
                                  ({"payout_evidence_rule": ""}, "unadmitted_unit_payout"),
                                  ({"face_value_dollars": None}, "missing_or_nonunit_face_value"),
                                  ({"face_value_dollars": 100}, "missing_or_nonunit_face_value"),
                                  ({"face_value_dollars": True}, "missing_or_nonunit_face_value")):
            with self.subTest(options=options):
                result = normalize(market=prepare(raw, **options))
                self.assertEqual(result["eligibility_reason"], expected)
                self.assertEqual(result["duration_reason"], "baseline_ineligible")
        with self.assertRaisesRegex(k.InputBlocked, "boolean"):
            prepare(unit_payoff_admitted="True")

    def test_combo_state_is_admitted_upstream_and_unknown_fails_closed(self):
        for options, expected in (({"combo_state": "combo"}, "combo_excluded"),
                                  ({"combo_state": "unknown"}, "unadmitted_combo_classification"),
                                  ({"combo_evidence_rule": None}, "unadmitted_combo_classification")):
            with self.subTest(options=options):
                self.assertEqual(normalize(market=prepare(**options))["eligibility_reason"], expected)
        with self.assertRaisesRegex(k.InputBlocked, "combo state"):
            prepare(combo_state="probably_single")

    def test_only_finalized_binary_results_are_admitted(self):
        for changes, expected in (({"market_type": "scalar"}, "nonbinary_or_missing_market_type"),
                                  ({"market_type": None}, "nonbinary_or_missing_market_type"),
                                  ({"status": "active"}, "not_finalized"),
                                  ({"status": "determined"}, "not_finalized"),
                                  ({"result": "scalar"}, "nonbinary_or_missing_result"),
                                  ({"result": ""}, "nonbinary_or_missing_result")):
            with self.subTest(changes=changes):
                self.assertEqual(normalize(market=prepare(market_raw(**changes)))["eligibility_reason"], expected)

    def test_native_category_and_event_missing_policies(self):
        missing = prepare(market_raw(event_ticker=""), category=None, category_evidence_rule=None)
        result = normalize(market=missing)
        self.assertTrue(result["baseline_eligible"])
        self.assertEqual((result["category"], result["cluster_source"]), ("Unclassified", "market_fallback"))
        self.assertEqual(result["event_cluster"], "kalshi:market:KXTEST-26APR01-YES")
        self.assertTrue(missing["category_missing"])
        self.assertEqual(normalize(market=prepare(category_evidence_rule=""))["eligibility_reason"],
                         "unadmitted_native_category")
        self.assertEqual(normalize(market=prepare(market_raw(event_ticker=" bad event ")))["eligibility_reason"],
                         "invalid_native_event_ticker")

    def test_ticker_mapping_is_exact_and_missing_metadata_is_excluded(self):
        result = k.normalize_trade(trade(), None, source_date="2026-03-25", source_ordinal=0)
        self.assertEqual(result["eligibility_reason"], "unadmitted_market")
        self.assertEqual(normalize(market=prepare(market_raw(ticker="DIFFERENT")))["eligibility_reason"],
                         "trade_market_ticker_disagreement")
        for value in (None, "", " padded ", "embedded space"):
            with self.subTest(value=value):
                self.assertEqual(normalize(trade(trade_id=value))["eligibility_reason"], "invalid_trade_identifier")

    def test_invalid_price_and_size_records_remain_annotated(self):
        for field in ("yes_price", "no_price"):
            for value in (0, 1, -1, math.nan, math.inf, "0.05", True):
                with self.subTest(field=field, value=value):
                    result = normalize(trade(**{field: value}))
                    self.assertEqual(result["eligibility_reason"], "invalid_native_price")
                    self.assertEqual(result["source_ordinal"], 0)
                    self.assertIsNone(result["payoff"])
        for value in (0, -1, math.nan, math.inf, "1", True):
            with self.subTest(size=value):
                self.assertEqual(normalize(trade(size=value))["eligibility_reason"], "invalid_size")
        self.assertEqual(normalize(trade(yes_price=.2))["eligibility_reason"],
                         "native_price_complement_disagreement")
        self.assertEqual(normalize(trade(taker_side="YES"))["eligibility_reason"], "invalid_taker_side")

    def test_schema_validation_blocks_missing_fields_types_and_drift(self):
        schema = {**k.TRADE_FIELDS, **k.OPTIONAL_TRADE_FIELDS}
        self.assertEqual(k.validate_trade_schema(schema), schema)
        for updates in ({"size": "int64"}, {"yes_price": "string"}, {"price": "double"}):
            with self.subTest(updates=updates), self.assertRaises(k.InputBlocked):
                k.validate_trade_schema({**schema, **updates})
        missing = dict(schema)
        del missing["trade_id"]
        with self.assertRaisesRegex(k.InputBlocked, "fields absent"):
            k.validate_trade_schema(missing)
        raw = trade()
        del raw["size"]
        with self.assertRaisesRegex(k.InputBlocked, "fields absent"):
            normalize(raw)

    def test_optional_modern_direction_fields_must_agree(self):
        for options in ({}, {"taker_outcome_side": None, "taker_book_side": ""},
                        {"taker_outcome_side": "yes", "taker_book_side": "bid"}):
            with self.subTest(options=options):
                self.assertTrue(normalize(trade(**options))["baseline_eligible"])
        for options, expected in (({"taker_outcome_side": "YES"}, "invalid_taker_outcome_side"),
                                  ({"taker_outcome_side": "no"}, "taker_outcome_side_disagreement"),
                                  ({"taker_book_side": "buy"}, "invalid_taker_book_side"),
                                  ({"taker_book_side": "ask"}, "taker_book_side_disagreement")):
            with self.subTest(options=options):
                self.assertEqual(normalize(trade(**options))["eligibility_reason"], expected)
        no = normalize(trade(taker_side="no", taker_outcome_side="no", taker_book_side="ask"))
        self.assertTrue(no["baseline_eligible"])

    def test_src_is_explicit_provenance_only_and_disagreement_is_retained(self):
        agreed = normalize(trade(src="catchup1"))
        self.assertFalse(agreed["source_src_disagreement"])
        changed = normalize(trade(src="backfill"))
        self.assertTrue(changed["baseline_eligible"])
        self.assertTrue(changed["source_src_disagreement"])
        self.assertEqual(changed["recorded_src"], "backfill")

    def test_inputs_are_not_mutated_and_no_role_rows_are_created(self):
        raw = trade()
        prepared = prepare()
        before_raw, before_market = dict(raw), dict(prepared)
        result = normalize(raw, prepared)
        self.assertEqual(raw, before_raw)
        self.assertEqual(prepared, before_market)
        self.assertIsInstance(result, dict)
        self.assertNotIn("wallet", result)
        self.assertNotIn("is_maker", result)
        self.assertTrue(result["analytic_outcomes_finite"])
        self.assertTrue(result["estimation_eligible"])


if __name__ == "__main__":
    unittest.main()
