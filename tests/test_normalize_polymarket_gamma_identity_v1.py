"""Local synthetic documented-shape Gamma identity tests, no production reads."""
from __future__ import annotations

import builtins
import copy
import json
import unittest
from unittest.mock import patch

from pipeline import normalize_polymarket_gamma_identity_v1 as normalizer


LOCATOR = "gamma/v2-closed/p000017/r000003"


def market(version="v1", module=1, labels=("Yes", "No")):
    condition = (module << 248) | (123 << 120) | (137 << 24)
    result = {"id": "fixture", "version": version,
        "conditionId": "0x" + f"{condition:064x}", "outcomes": json.dumps(labels),
        "closed": False, "resolutionStatus": "active", "outcomePrices": "[0.999,0.001]",
        "createdAt": "2026-10-01T00:00:00.123456Z", "endDate": "2026-10-02T00:00:00Z",
        "events": [{"id": "event-1", "slug": "literal-event", "future_event": [1]}]}
    if version == "v1":
        result["clobTokenIds"] = json.dumps([str(normalizer.UINT256), "1"])
    else:
        result["positionIds"] = [str(condition), str(condition + 1)]
    return result


class GammaIdentityTests(unittest.TestCase):
    def normalize(self, value=None, locator=LOCATOR):
        return normalizer.normalize_market(market() if value is None else value, locator)

    def rejected(self, raw, reason, locator=LOCATOR):
        before = copy.deepcopy(raw)
        result = normalizer.normalize_market(raw, locator)
        self.assertEqual(result["status"], "quarantined", result)
        self.assertEqual(result["reason"], reason, result)
        self.assertEqual(result["waterfall_class"], "quarantined_" + reason)
        self.assertIsNone(result["identity"])
        self.assertEqual(result["provenance"]["raw_market"], before)
        self.assertEqual(raw, before)
        return result

    def candidate(self, raw):
        before = copy.deepcopy(raw)
        result = self.normalize(raw)
        self.assertEqual(result["status"], "identity_candidate", result)
        self.assertEqual(result["provenance"]["raw_market"], before)
        self.assertEqual(raw, before)
        identity = result["identity"]
        first, second = identity["tokens"]
        self.assertEqual(first["complement_token_id"], second["token_id"])
        self.assertEqual(second["complement_token_id"], first["token_id"])
        self.assertEqual(first["complement_label"], second["outcome"])
        self.assertEqual(identity["source_locator"], LOCATOR)
        self.assertEqual(result["economic_eligibility"], "not_admitted")
        self.assertEqual(result["resolution_status"], "not_inferred")
        for flag in normalizer.SCOPE:
            self.assertIs(result[flag], False)
        self.assertFalse(any(key in token for token in identity["tokens"] for key in ("Y", "winner", "resolved")))
        return result

    def test_documented_v1_json_text_and_full_uint256_literal_ids(self):
        raw = market(); result = self.candidate(raw)
        identity = result["identity"]
        self.assertEqual(identity["position_system"], "ctf_v1")
        self.assertEqual(identity["selected_id_field"], "clobTokenIds")
        self.assertEqual([token["token_id"] for token in identity["tokens"]], [str(2**256 - 1), "1"])
        self.assertIsNone(identity["module_id"])

    def test_documented_v2_array_and_literal_outcome_alignment(self):
        raw = market("v2", labels=("Red Sox", "Yankees"))
        result = self.candidate(raw)
        self.assertEqual(result["identity"]["position_system"], "position_manager_v2")
        self.assertEqual([row["outcome"] for row in result["identity"]["tokens"]], ["Red Sox", "Yankees"])
        self.assertEqual([row["outcome_index"] for row in result["identity"]["tokens"]], [0, 1])

    def test_both_id_fields_coexist_but_explicit_version_alone_selects(self):
        for version in ("v1", "v2"):
            raw = market(version)
            other = "positionIds" if version == "v1" else "clobTokenIds"
            for unrelated in (["8", "9"], "malformed unused JSON", None, {"opaque": True}):
                raw[other] = unrelated
                result = self.candidate(raw)
                self.assertEqual(result["identity"]["selected_id_field"], "clobTokenIds" if version == "v1" else "positionIds")
                self.assertEqual(result["provenance"]["raw_market"][other], unrelated)

    def test_version_is_required_and_never_guessed_from_fields_or_token_bits(self):
        raw = market(); del raw["version"]
        self.rejected(raw, "version_missing")
        for value in (None, True, 1, "V1", "v3", "", " v2"):
            raw = market("v2"); raw["version"] = value
            self.rejected(raw, "version_missing_or_unsupported")

    def test_selected_id_field_missing_never_falls_back(self):
        for version, selected, other in (("v1", "clobTokenIds", "positionIds"), ("v2", "positionIds", "clobTokenIds")):
            raw = market(version); del raw[selected]; raw[other] = ["1", "2"]
            self.rejected(raw, selected + "_missing")

    def test_v1_ids_require_json_array_text_not_coerced_python_arrays(self):
        for value, reason in ((["1", "2"], "clobTokenIds_not_json_text"), (None, "clobTokenIds_not_json_text"),
                              ("[", "clobTokenIds_invalid_json"), ("{}", "clobTokenIds_not_array")):
            raw = market(); raw["clobTokenIds"] = value; self.rejected(raw, reason)

    def test_v2_ids_require_literal_array_not_json_text(self):
        for value in (None, True, {}, '["1","2"]', ("1", "2")):
            raw = market("v2"); raw["positionIds"] = value
            self.rejected(raw, "positionIds_not_array")

    def test_ids_never_trim_round_coerce_or_accept_noncanonical_decimal(self):
        for version in ("v1", "v2"):
            for value in (None, True, 1, 1.0, "", "0", "01", " 1", "1 ", "+1", "1e2", "1.0", "0xff", str(2**256), "9" * 79):
                raw = market(version); ids = [value, "2"]
                raw["clobTokenIds" if version == "v1" else "positionIds"] = json.dumps(ids) if version == "v1" else ids
                with self.subTest(version=version, value=value):
                    self.rejected(raw, "token_id_noncanonical_or_out_of_range")

    def test_duplicate_tokens_count_mismatch_and_nonbinary_outcomes(self):
        raw = market(); raw["clobTokenIds"] = '["1","1"]'; self.rejected(raw, "duplicate_token_id")
        for values in ([], ["1"], ["1", "2", "3"]):
            raw = market(); raw["clobTokenIds"] = json.dumps(values)
            self.rejected(raw, "id_outcome_count_mismatch")
        for labels in ([], ["A"], ["A", "B", "C"]):
            raw = market(); raw["outcomes"] = json.dumps(labels)
            self.rejected(raw, "nonbinary_outcome_count")

    def test_literal_labels_and_documented_outcomes_json_text(self):
        raw = market(labels=(" Atlético Madrid ", "ATLÉTICO MADRID"))
        self.assertEqual(self.candidate(raw)["identity"]["tokens"][0]["outcome"], " Atlético Madrid ")
        for value, reason in ((None, "outcomes_not_json_text"), (["A", "B"], "outcomes_not_json_text"),
                              ("[", "outcomes_invalid_json"), ("{}", "outcomes_not_array"),
                              ('["A","A"]', "duplicate_outcome_label"), ('["A",null]', "outcome_not_nonblank_string"),
                              ('["A"," "]', "outcome_not_nonblank_string")):
            raw = market(); raw["outcomes"] = value; self.rejected(raw, reason)
        raw = market(); del raw["outcomes"]; self.rejected(raw, "outcomes_missing")

    def test_conflicting_explicit_outcome_count_quarantines(self):
        raw = market(); raw["n_outcomes"] = 2; self.candidate(raw)
        for value in (True, 2.0, "2", None):
            raw = market(); raw["n_outcomes"] = value; self.rejected(raw, "n_outcomes_not_integer")
        raw = market(); raw["n_outcomes"] = 3; self.rejected(raw, "n_outcomes_token_count_conflict")

    def test_v1_condition_is_literal_bytes32_and_never_module_classified(self):
        raw = market(); raw["conditionId"] = "0x" + "AB" * 32
        self.assertEqual(self.candidate(raw)["identity"]["condition_id_compat32"], raw["conditionId"])
        for value in ("0x" + "a" * 62, "0x123", "0X" + "a" * 64, "0x" + "g" * 64):
            raw = market(); raw["conditionId"] = value; self.rejected(raw, "v1_condition_id_not_bytes32")
        for prefix in (0, 1, 2, 3, 255):
            raw = market(); raw["clobTokenIds"] = json.dumps([str((prefix << 248) + 1), str((prefix << 248) + 2)])
            self.assertIsNone(self.candidate(raw)["identity"]["module_id"])

    def test_v2_bytes31_and_compat_bytes32_same_relation_no_literal_rewrite(self):
        raw = market("v2"); padded = raw["conditionId"]
        raw["conditionId"] = padded[:-2].upper().replace("0X", "0x")
        result = self.candidate(raw)
        self.assertEqual(result["identity"]["condition_id_raw"], raw["conditionId"])
        self.assertEqual(result["identity"]["condition_id_compat32"], raw["conditionId"] + "00")
        raw["conditionId"] = padded; self.candidate(raw)

    def test_v2_last_byte_width_condition_and_outcome_relations_reject(self):
        raw = market("v2"); raw["conditionId"] = raw["conditionId"][:-2] + "01"
        self.rejected(raw, "v2_condition_id_nonzero_outcome_byte")
        for value in ("0x123", "0x" + "a" * 60, "0x" + "a" * 66):
            raw = market("v2"); raw["conditionId"] = value; self.rejected(raw, "v2_condition_id_invalid_width")
        raw = market("v2"); raw["positionIds"][1] = str(int(raw["positionIds"][1]) + 256)
        self.rejected(raw, "position_condition_mismatch")
        raw = market("v2"); raw["positionIds"][1] = str(int(raw["positionIds"][1]) + 1)
        self.rejected(raw, "position_outcome_bytes_not_binary_pair")

    def test_position_array_order_is_retained_not_sorted_or_label_inferred(self):
        raw = market("v2", labels=("No", "Yes")); raw["positionIds"].reverse()
        tokens = self.candidate(raw)["identity"]["tokens"]
        self.assertEqual([row["outcome_index"] for row in tokens], [1, 0])
        self.assertEqual([row["outcome"] for row in tokens], ["No", "Yes"])
        self.assertEqual([row["array_index"] for row in tokens], [0, 1])

    def test_all_observed_modules_remain_identity_only_not_economic_admission(self):
        for module, expected in ((1, "binary_module_observed"), (2, "negrisk_module_observed"),
                                 (3, "combinatorial_module_observed"), (0, "unknown_module_observed"),
                                 (255, "unknown_module_observed")):
            raw = market("v2", module=module); result = self.candidate(raw)
            self.assertEqual(result["identity"]["module_id"], module)
            self.assertEqual(result["identity"]["module_status"], expected)
            self.assertEqual(result["identity"]["economic_eligibility"], "not_admitted")

    def test_unresolved_identity_stays_valid_and_gamma_status_prices_never_make_y(self):
        for fields in ({}, {"closed": False, "resolutionStatus": "active"},
                       {"closed": True, "resolutionStatus": "resolved", "outcomePrices": "[1,0]"},
                       {"closed": "nonsense", "winner": True, "resolved": True}):
            raw = market(); raw.update(fields); self.candidate(raw)

    def test_exact_optional_clock_parse_preserves_offsets_fraction_and_raw(self):
        raw = market(); raw["createdAt"] = "2026-10-01T02:30:00.123456+02:30"
        clocks = self.candidate(raw)["clock_evidence"]
        self.assertEqual(clocks["created_at"]["utc"], "2026-10-01T00:00:00.123456Z")
        self.assertEqual(clocks["created_at"]["epoch_microseconds"], 1_790_812_800_123456)
        self.assertEqual(clocks["created_at"]["raw"], raw["createdAt"])
        self.assertEqual(clocks["interval_status"], "positive")

    def test_invalid_or_missing_clock_is_not_identity_drop_and_never_falls_back(self):
        for value, status in ((None, "null"), (1, "invalid_rfc3339"), ("2026-10-01", "invalid_rfc3339"),
                              ("2026-10-01T00:00:00", "invalid_rfc3339"), ("2026-02-30T00:00:00Z", "invalid_rfc3339"),
                              ("2026-10-01T00:00:00+00:60", "invalid_rfc3339"),
                              ("2026-10-01T00:00:00+24:00", "invalid_rfc3339"),
                              ("2026-10-01T00:00:00.1234567Z", "unsupported_submicrosecond_precision"),
                              ("2026-10-01T00:00:00-00:00", "unknown_timezone_offset")):
            raw = market(); raw["createdAt"] = value
            self.assertEqual(self.candidate(raw)["clock_evidence"]["created_at"]["status"], status)
        raw = market(); del raw["createdAt"]; raw["startDate"] = "2026-09-30T00:00:00Z"
        self.assertEqual(self.candidate(raw)["clock_evidence"]["created_at"]["status"], "missing")
        raw = market(); raw["endDate"] = raw["createdAt"]
        self.assertEqual(self.candidate(raw)["clock_evidence"]["interval_status"], "nonpositive")

    def test_event_relation_retains_every_field_and_never_market_slug_fallback(self):
        raw = market(); result = self.candidate(raw)
        self.assertEqual(result["event_evidence"]["status"], "unique_explicit_event")
        self.assertEqual(result["event_evidence"]["event_slug"], "literal-event")
        self.assertEqual(result["event_evidence"]["entries"][0]["raw"], raw["events"][0])
        del raw["events"]; raw["slug"] = "not-an-event"
        result = self.candidate(raw)
        self.assertEqual(result["event_evidence"]["status"], "missing")
        self.assertIsNone(result["event_evidence"]["event_slug"])

    def test_multiple_or_malformed_event_evidence_is_not_arbitrarily_selected(self):
        for events, expected in ((None, "null"), ({}, "events_not_array"), ([], "empty"),
                                 ([None], "incomplete_or_malformed"), ([{"slug": "a"}], "incomplete_or_malformed"),
                                 ([{"id": "1", "slug": "a"}, {"id": "2", "slug": "b"}], "ambiguous"),
                                 ([{"id": "1", "slug": "a"}, {"id": "2", "slug": "a"}], "ambiguous"),
                                 ([{"id": "1", "slug": "a"}, {"id": "1", "slug": "b"}], "ambiguous")):
            raw = market(); raw["events"] = events; result = self.candidate(raw)
            self.assertEqual(result["event_evidence"]["status"], expected)
            self.assertEqual(result["event_evidence"]["raw"], events)
            self.assertIsNone(result["event_evidence"]["event_slug"])
        raw = market(); raw["events"] *= 2
        self.assertEqual(len(self.candidate(raw)["event_evidence"]["entries"]), 2)

    def test_unknown_fields_provenance_and_nested_outputs_are_independent_copies(self):
        raw = market(); raw["future"] = {"nested": [1, 2]}; before = copy.deepcopy(raw)
        result = self.candidate(raw)
        result["provenance"]["raw_market"]["future"]["nested"].append(3)
        result["event_evidence"]["entries"][0]["raw"]["future_event"].append(2)
        self.assertEqual(raw, before)

    def test_ordinary_irregular_records_and_locator_have_reasoned_conservation(self):
        records = [None, [], True, "raw", 1, {}, market(), market("v2")]
        results = [normalizer.normalize_market(value, LOCATOR) for value in records]
        self.assertEqual(sum(row["input_markets"] for row in results), len(records))
        self.assertEqual(sum(row["candidate_markets"] + row["quarantined_markets"] for row in results), len(records))
        for result in results:
            self.assertTrue(result["waterfall_class"])
        for locator in (None, True, 1, "", " \n"):
            result = self.rejected(market(), "source_locator_missing_or_invalid", locator)
            self.assertEqual(result["provenance"]["source_locator"], locator)
        raw = market(); del raw["conditionId"]; self.rejected(raw, "condition_id_missing")
        for version in ("v1", "v2"):
            raw = market(version); raw["conditionId"] = None; self.rejected(raw, "condition_id_invalid")

    def test_pure_normalizer_has_no_filesystem_or_network_work(self):
        with patch.object(builtins, "open", side_effect=AssertionError("unexpected filesystem work")):
            self.candidate(market("v2"))


if __name__ == "__main__":
    unittest.main()
