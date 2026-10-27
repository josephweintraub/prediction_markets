"""Synthetic single-record resolution fixtures; no APIs or production data."""
from __future__ import annotations

import builtins
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline import normalize_polymarket_clob_claims_v1 as normalizer


LOCATOR = "clob/p000017/r000123"


def market(number=1, labels=("Red Sox", "Yankees"), winner=0):
    return {"condition_id": "0x"+f"{number:064x}", "closed": True,
        "question": "Literal fixture question", "market_slug": "a-market-not-an-event",
        "tokens": [{"token_id": str((255 << 248)+number*2+index), "outcome": label,
                    "winner": index == winner, "price": 0.01 if index == winner else 0.99}
                   for index, label in enumerate(labels)]}


class ClobClaimNormalizerTests(unittest.TestCase):
    def normalize(self, value=None, locator=LOCATOR):
        return normalizer.normalize_market(market() if value is None else value, locator)

    def rejected(self, value, reason, locator=LOCATOR):
        raw = copy.deepcopy(value)
        result = normalizer.normalize_market(value, locator)
        self.assertEqual(result["status"], "quarantined", result)
        self.assertEqual(result["reason"], reason)
        self.assertEqual(result["waterfall_class"], "quarantined_"+reason)
        self.assertEqual(result["claim_candidates"], [])
        self.assertEqual(result["provenance"]["raw_market"], raw)
        self.assertEqual(value, raw)
        self.assertEqual(result["candidate_markets"], 0)
        self.assertEqual(result["quarantined_markets"], 1)
        return result

    def assert_pair(self, result, raw):
        self.assertEqual(result["status"], "candidate", result)
        self.assertEqual(result["waterfall_class"], "resolved_binary_candidate")
        first, second = result["claim_candidates"]
        self.assertEqual(first["complement_token_id"], second["token_id"])
        self.assertEqual(second["complement_token_id"], first["token_id"])
        self.assertEqual(first["complement_label"], second["outcome_label"])
        self.assertEqual(second["complement_label"], first["outcome_label"])
        self.assertEqual(first["Y"], 1-second["Y"])
        self.assertIs(type(first["Y"]), int)
        self.assertEqual(first["winning_outcome_label"], second["winning_outcome_label"])
        winner = next(token["outcome"] for token in raw["tokens"] if token["winner"] is True)
        self.assertEqual(first["winning_outcome_label"], winner)
        for row, token in zip((first, second), raw["tokens"]):
            self.assertEqual(row["token_id"], token["token_id"])
            self.assertEqual(row["condition_id"], raw["condition_id"])
            self.assertEqual(row["market_id"], raw["condition_id"])
            self.assertEqual(row["outcome"], row["outcome_label"])
            self.assertEqual(row["outcome"], token["outcome"])
            self.assertEqual(row["winning_outcome"], row["winning_outcome_label"])
            self.assertEqual(row["Y"], int(row["outcome"] == winner))
            self.assertEqual(row["source_locator"], LOCATOR)
            self.assertEqual(row["n_outcomes"], 2)
            self.assertIs(row["resolved"], True)

    def test_two_team_binary_uses_literal_winner_not_highest_price(self):
        raw = market(); result = self.normalize(raw)
        self.assert_pair(result, raw)
        self.assertEqual([row["Y"] for row in result["claim_candidates"]], [1, 0])
        self.assertEqual(result["provenance"]["literal_token_winner_flags"][0]["winner"], True)
        self.assertFalse(result["provenance"]["prices_used_for_resolution"])

    def test_yes_no_negrisk_market_is_a_binary_claim_pair_without_event_level_collapse(self):
        raw = market(labels=("Yes", "No"), winner=1)
        raw.update(neg_risk=True, neg_risk_market_id="0x"+"a"*64, neg_risk_request_id="request")
        self.assert_pair(self.normalize(raw), raw)

    def test_multiple_binary_conditions_in_one_event_stay_separate(self):
        records = [market(number=number, labels=("Yes", "No"), winner=number % 2) for number in (1, 2, 3)]
        for raw in records:raw["event_slug"] = "same-multiple-binary-event"
        results = [self.normalize(raw) for raw in records]
        claims = [row for result in results for row in result["claim_candidates"]]
        self.assertEqual(len(claims), 6)
        self.assertEqual(len({row["market_id"] for row in claims}), 3)
        self.assertEqual(len({row["token_id"] for row in claims}), 6)
        for result, raw in zip(results, records):self.assert_pair(result, raw)

    def test_generic_legacy_hash_ids_are_not_filtered_by_top_byte(self):
        for prefix in (0, 1, 2, 3, 127, 255):
            with self.subTest(prefix=prefix):
                raw = market()
                for index, token in enumerate(raw["tokens"]):token["token_id"] = str((prefix << 248)+index+1)
                result = self.normalize(raw);self.assert_pair(result, raw)
                self.assertFalse(result["provenance"]["module_classifier_applied"])

    def test_full_uint256_ids_remain_exact_decimal_strings(self):
        raw = market()
        raw["tokens"][0]["token_id"] = str(normalizer.UINT256)
        raw["tokens"][1]["token_id"] = "1"
        result = self.normalize(raw);self.assert_pair(result, raw)
        self.assertEqual(result["claim_candidates"][0]["token_id"], str(2**256-1))

    def test_ids_never_coerce_trim_round_or_normalize_leading_zeros(self):
        for token_id in (None, True, False, 123, 1.5, "", " ", "0", "01", " 1", "1 ", "+1", "1.0", "1e20", "0x123", str(2**256), "9"*79):
            with self.subTest(token_id=token_id):
                raw = market();raw["tokens"][0]["token_id"] = token_id
                self.rejected(raw, "token_id_noncanonical_or_out_of_range")

    def test_token_outcome_and_winner_fields_must_be_present(self):
        for key, reason in (("token_id", "token_id_missing"), ("outcome", "outcome_missing"), ("winner", "winner_missing")):
            raw = market();del raw["tokens"][1][key]
            result = self.rejected(raw, reason)
            self.assertEqual(result["provenance"]["literal_token_winner_flags"][1]["winner_present"], key != "winner")

    def test_duplicate_ids_and_literal_labels_quarantine_entire_record(self):
        for field, reason in (("token_id", "duplicate_token_id"), ("outcome", "duplicate_outcome_label")):
            raw = market();raw["tokens"][1][field] = raw["tokens"][0][field]
            self.rejected(raw, reason)

    def test_labels_are_literal_strings_without_trimming_or_synonym_maps(self):
        raw = market(labels=(" Atlético Madrid ", "ATLÉTICO MADRID"))
        self.assert_pair(self.normalize(raw), raw)
        for label in (None, True, 0, [], "", " \t\n"):
            raw = market();raw["tokens"][0]["outcome"] = label
            self.rejected(raw, "outcome_not_nonblank_string")

    def test_winner_requires_explicit_boolean_not_truthiness_or_prices(self):
        for winner in (None, 0, 1, 0.0, 1.0, "true", "false", "True", [], {}):
            raw = market();raw["tokens"][0]["winner"] = winner
            self.rejected(raw, "winner_not_boolean")

    def test_no_winner_or_multiple_winners_are_not_inferred_from_price_extremes(self):
        for wins, reason in (((False, False), "winner_not_identified"), ((True, True), "contradictory_multiple_winners")):
            for prices in ((1.0, 0.0), (0.999999, 0.000001), (0.0, 1.0)):
                raw = market()
                for token, winner, price in zip(raw["tokens"], wins, prices):token.update(winner=winner, price=price)
                self.rejected(raw, reason)

    def test_open_with_winner_is_explicit_contradictory_status_quarantine(self):
        raw = market();raw["closed"] = False
        result = self.rejected(raw, "contradictory_open_market_with_winner")
        self.assertEqual([row["winner"] for row in result["provenance"]["literal_token_winner_flags"]], [True, False])

    def test_closed_is_required_literal_boolean_not_guessed_from_winner(self):
        raw = market();del raw["closed"];self.rejected(raw, "closed_missing")
        for value in (None, 0, 1, "true", "closed"):
            raw = market();raw["closed"] = value
            self.rejected(raw, "closed_not_boolean")

    def test_condition_is_exact_literal_hex_id_without_case_rewriting(self):
        raw = market();raw["condition_id"] = "0x"+"AB"*32
        result = self.normalize(raw);self.assert_pair(result, raw)
        raw = market();del raw["condition_id"];self.rejected(raw, "condition_id_missing")
        for value in (None, True, 1, "", "0x123", "0X"+"ab"*32, "0x"+"g"*64):
            raw = market();raw["condition_id"] = value;self.rejected(raw, "condition_id_invalid")

    def test_market_and_token_arrays_cannot_be_guessed_or_truncated_to_two(self):
        for value in (None, True, "market", [], 1):self.rejected(value, "market_not_object")
        raw = market();del raw["tokens"];self.rejected(raw, "tokens_missing")
        for value in (None, {}, "[]", tuple(market()["tokens"])):
            raw = market();raw["tokens"] = value;self.rejected(raw, "tokens_not_array")
        for count in (0, 1, 3):
            raw = market();raw["tokens"] = (raw["tokens"]*2)[:count];self.rejected(raw, "nonbinary_token_count")
        raw = market();raw["tokens"][1] = None;self.rejected(raw, "token_not_object")

    def test_supplied_n_outcomes_must_not_disagree_with_two_tokens(self):
        raw = market();raw["n_outcomes"] = 2;self.assert_pair(self.normalize(raw), raw)
        for value in (True, 2.0, "2", None):
            raw = market();raw["n_outcomes"] = value;self.rejected(raw, "n_outcomes_not_integer")
        raw = market();raw["n_outcomes"] = 3;self.rejected(raw, "n_outcomes_token_count_conflict")

    def test_market_slug_is_never_used_as_event_slug_fallback(self):
        result = self.normalize()
        self.assertTrue(all(row["event_slug"] is None for row in result["claim_candidates"]))
        self.assertEqual(result["provenance"]["raw_market"]["market_slug"], "a-market-not-an-event")

    def test_explicit_event_aliases_require_agreement_and_preserve_literal_values(self):
        for fields in ({"event_slug": "literal-event"}, {"eventSlug": "literal-event"},
                       {"event_slug": "literal-event", "eventSlug": "literal-event"},
                       {"event_slug": None}, {"event_slug": ""}):
            raw = market();raw.update(fields);result = self.normalize(raw);self.assert_pair(result, raw)
            self.assertEqual(result["provenance"]["literal_event_slug_fields"], fields)
            expected = next(iter(fields.values()))
            self.assertTrue(all(row["event_slug"] == expected for row in result["claim_candidates"]))
        for fields in ({"event_slug": "a", "eventSlug": "b"}, {"event_slug": None, "eventSlug": "b"}):
            raw = market();raw.update(fields);self.rejected(raw, "event_slug_alias_conflict")
        raw = market();raw["event_slug"] = 1;self.rejected(raw, "event_slug_schema")

    def test_clock_fields_are_preserved_but_not_parsed_renamed_or_filled(self):
        raw = market();raw.update(end_date_iso="2026-10-01T20:00:00Z", game_start_time="provider literal",
                                  createdAt=None, resolutionTime={"uninterpreted": "clock"})
        result = self.normalize(raw);self.assert_pair(result, raw)
        self.assertEqual(result["provenance"]["available_clock_fields"],
            {key: raw[key] for key in ("end_date_iso", "game_start_time", "createdAt", "resolutionTime")})
        self.assertFalse(result["provenance"]["timestamps_parsed"])
        self.assertNotIn("created_at", result["claim_candidates"][0])
        self.assertNotIn("end_date", result["claim_candidates"][0])

    def test_unknown_additive_metadata_and_input_objects_are_retained_without_mutation(self):
        raw = market();raw["future_schema"] = {"nested": ["literal", {"value": 7}]}
        raw["tokens"][0]["future_token_field"] = [1, 2]
        before = copy.deepcopy(raw);result = self.normalize(raw)
        self.assertEqual(raw, before);self.assertEqual(result["provenance"]["raw_market"], before)
        result["provenance"]["raw_market"]["future_schema"]["nested"][1]["value"] = 8
        result["provenance"]["literal_token_winner_flags"][0]["winner"] = False
        self.assertEqual(raw, before)
        self.assertEqual(result["provenance"]["raw_market"]["tokens"][0]["winner"], True)

    def test_every_input_has_exactly_one_waterfall_class_and_scope_remains_unadmitted(self):
        records = [market(), None, {}, {"closed": False}, {**market(), "closed": False}]
        results = [normalizer.normalize_market(raw, LOCATOR) for raw in records]
        self.assertEqual(sum(row["input_markets"] for row in results), len(records))
        self.assertEqual(sum(row["candidate_markets"]+row["quarantined_markets"] for row in results), len(records))
        for row in results:
            self.assertTrue(row["waterfall_class"])
            for field in normalizer.SCOPE:self.assertIs(row[field], False)

    def test_missing_source_locator_quarantines_even_an_otherwise_valid_market(self):
        for value in (None, True, 1, "", " \n"):
            result = self.rejected(market(), "source_locator_missing_or_invalid", locator=value)
            self.assertEqual(result["provenance"]["source_locator"], value)

    def test_core_has_no_filesystem_or_api_execution(self):
        with patch.object(builtins, "open", side_effect=AssertionError("unexpected file read")):
            result = self.normalize()
        self.assertEqual(result["status"], "candidate")

    def test_candidate_projection_matches_archive_spine_names_and_y_complements(self):
        from pipeline import normalize_polymarket_v3_archive_v1 as archive
        raw = market();result = self.normalize(raw)
        pair = [{key: row[key] for key in archive.SPINE_FIELDS} for row in result["claim_candidates"]]
        first, second = archive.token_pair(int(raw["tokens"][0]["token_id"]), [pair[0]], [pair[1]])
        self.assertEqual(first["complement_token_id"], second["token_id"])
        self.assertEqual(result["claim_candidates"][0]["Y"], 1-result["claim_candidates"][1]["Y"])

    def test_synthetic_metadata_projection_passes_existing_prepare_metadata(self):
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq
        from analysis.kaushik_polymarket_replication.build_inputs import prepare_metadata
        raw = market();raw.update(created_at="2026-09-01T00:00:00Z", end_date="2026-10-01T00:00:00Z", event_slug="literal-event")
        claims = self.normalize(raw)["claim_candidates"]
        tables = {
            "spine": pa.Table.from_pylist([{key: row[key] for key in ("token_id", "market_id", "winning_outcome")} for row in claims]),
            "token_map": pa.Table.from_pylist([{key: row[key] for key in ("token_id", "condition_id", "outcome")} for row in claims]),
            "native": pa.Table.from_pylist([{key: raw[key] for key in ("condition_id", "created_at", "end_date", "event_slug")} | {"n_outcomes": 2}]),
            "categories": pa.Table.from_pylist([], schema=pa.schema([("mkt", pa.string()), ("prim", pa.string())])),
        }
        with tempfile.TemporaryDirectory() as temporary:
            paths = {}
            for kind, table in tables.items():
                path = Path(temporary)/(kind+".parquet");pq.write_table(table, path);paths[kind] = str(path)
            with duckdb.connect() as con:
                summary = prepare_metadata(con, paths, sample_scope="all_available_dates")
                actual = con.execute("SELECT token_id,complement_token_id,Y FROM claims ORDER BY token_id").fetchall()
        self.assertEqual(summary["admitted_claims"], 2)
        self.assertEqual(summary["admitted_conditions"], 1)
        self.assertEqual(sum(row[2] for row in actual), 1)
        self.assertEqual(actual[0][1], actual[1][0])
        self.assertEqual(actual[1][1], actual[0][0])


if __name__ == "__main__":
    unittest.main()
