"""Synthetic fixtures only; never open production metadata or trades."""
from __future__ import annotations

from contextlib import redirect_stdout
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import reconstruct_polymarket_categories as categories


class CategoryReconstructionTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"tag": tag, "prim_cat": label, "excluded": False} for tag, label in (
            ("Sports", "Sports"), ("NBA", "Sports"), ("Politics", "Politics"),
            ("Crypto", "Crypto"), ("Mentions", "Mentions"), ("Iran", "Geopolitics"),
            ("Peace Deal", "Iran"))]
        self.rows.append({"tag": "Games", "prim_cat": None, "excluded": True})
        self.tag_map = categories.compile_tag_map(self.rows)

    def candidate(self, tags):
        return categories.candidate_from_tags(tags, self.tag_map)

    def test_plurality_is_deterministic_and_duplicate_tags_do_not_add_votes(self):
        first = self.candidate(["Sports", "Politics", "NBA", "Politics", "new-tag", "Games"])
        other = self.candidate(["Games", "new-tag", "Politics", "NBA", "Sports"])
        self.assertEqual(first, other)
        self.assertEqual(first["category"], "Sports")
        self.assertEqual(first["votes"], {"Politics": 1, "Sports": 2})
        self.assertEqual(first["unknown_tags"], ["new-tag"])
        self.assertEqual(first["excluded_tags"], ["Games"])

    def test_ties_and_absent_votes_are_explicit_unclassified(self):
        for tags, reason in ((["Sports", "Politics"], "vote_tie"),
                             (["Games", "new-tag"], "no_voting_tags"),
                             ([], "no_voting_tags"), (None, "no_voting_tags")):
            with self.subTest(tags=tags):
                decision = self.candidate(tags)
                self.assertEqual((decision["category"], decision["reason"]), ("Unclassified", reason))

    def test_every_frozen_category_remains_available_without_a_crosswalk(self):
        rows = [{"tag": "fixture-" + str(i), "prim_cat": label, "excluded": False}
                for i, label in enumerate(categories.CATEGORIES)]
        tag_map = categories.compile_tag_map(rows)
        for row in rows:
            with self.subTest(category=row["prim_cat"]):
                result = categories.candidate_from_tags([row["tag"]], tag_map)
                self.assertEqual((result["category"], result["reason"]),
                                 (row["prim_cat"], "unique_plurality"))

    def test_supported_overrides_and_unresolved_relative_precedence(self):
        for tags, expected in ((["Mentions", "Sports", "NBA"], ("Mentions", "override_mentions")),
                               (["Iran", "Sports", "NBA"], ("Iran", "override_iran")),
                               (["New Iran tag"], ("Iran", "override_iran")),
                               (["Mentions", "Iran"], ("Unclassified", "override_conflict"))):
            with self.subTest(tags=tags):
                decision = self.candidate(tags)
                self.assertEqual((decision["category"], decision["reason"]), expected)
        self.assertEqual(self.candidate(["Peace Deal"])["category"], "Iran")
        self.assertEqual(self.candidate(["iran", " mentions "])["reason"], "no_voting_tags")

    def test_malformed_tags_are_not_silently_parsed_or_dropped(self):
        for tags in ('["Sports"]', {"label": "Sports"}, ["Sports", None], ["Sports", "  "], [True]):
            with self.subTest(tags=tags):
                decision = self.candidate(tags)
                self.assertEqual((decision["category"], decision["reason"]),
                                 ("Unclassified", "malformed_tags"))

    def test_saved_labels_are_preserved_even_when_metadata_disagrees_or_is_missing(self):
        metadata = [{"condition_id": "a", "tags": ["Sports"]},
                    {"condition_id": "b", "tags": ["Iran"]},
                    {"condition_id": "new", "tags": ["Crypto"]}]
        accepted = [{"mkt": "a", "prim": "Politics"}, {"mkt": "b", "prim": None},
                    {"mkt": "missing", "prim": "Unclassified"}]
        original = copy.deepcopy((metadata, accepted))
        result = categories.reconstruct_rows(metadata, accepted, self.tag_map)
        self.assertEqual([(r["mkt"], r["prim"]) for r in result],
                         [("a", "Politics"), ("b", None), ("missing", "Unclassified"), ("new", "Crypto")])
        self.assertTrue(all(r["label_source"] == "accepted_saved_label" for r in result[:3]))
        self.assertEqual(result[2]["reconstruction_candidate"]["reason"], "metadata_missing")
        self.assertEqual((metadata, accepted), original)

    def test_outcomes_prices_and_returns_cannot_change_assignment(self):
        source = [{"condition_id": "new", "tags": ["Sports"], "winning_outcome": "Yes",
                   "price": 0.1, "return": -0.1}]
        changed = [{**source[0], "winning_outcome": "No", "price": 0.9, "return": 99}]
        self.assertEqual(categories.reconstruct_rows(source, [], self.tag_map),
                         categories.reconstruct_rows(changed, [], self.tag_map))

    def test_confusion_is_shadow_candidate_not_the_saved_label_preservation_path(self):
        metadata = [{"condition_id": "a", "tags": ["Sports", "future"]},
                    {"condition_id": "b", "tags": ["Politics"]},
                    {"condition_id": "c", "tags": ["Sports", "Politics"]},
                    {"condition_id": "new", "tags": ["Crypto"]}]
        accepted = [{"mkt": "a", "prim": "Politics"}, {"mkt": "b", "prim": "Politics"},
                    {"mkt": "c", "prim": None}, {"mkt": "old", "prim": "Sports"}]
        result = categories.summarize_validation(metadata, accepted, self.tag_map)
        self.assertEqual((result["overlap_markets"], result["agreement"], result["assigned_agreement"]), (3, 2, 1))
        self.assertEqual((result["candidate_assigned"], result["candidate_unclassified"]), (2, 1))
        self.assertEqual(result["confusion"]["Politics"]["Sports"], 1)
        self.assertEqual(result["confusion"]["Unclassified"]["Unclassified"], 1)
        self.assertEqual((result["metadata_without_saved_label"], result["saved_label_without_metadata"]), (1, 1))
        self.assertEqual(sum(sum(row.values()) for row in result["confusion"].values()), 3)
        self.assertEqual(sum(row["markets"] for row in result["tag_evidence"]), 3)
        self.assertEqual(sum(result["candidate_category_counts"].values()), 3)
        self.assertEqual(sum(result["saved_category_counts"].values()), 3)
        self.assertEqual(result["unknown_tag_market_counts"], {"future": 1})

    def test_empty_overlap_does_not_imply_perfect_agreement_or_drop_saved_rows(self):
        saved = [{"mkt": "old", "prim": "Sports"}]
        summary = categories.summarize_validation([], saved, self.tag_map)
        self.assertEqual((summary["overlap_markets"], summary["agreement"],
                          summary["saved_label_without_metadata"]), (0, 0, 1))
        self.assertEqual(sum(sum(row.values()) for row in summary["confusion"].values()), 0)
        self.assertEqual(categories.reconstruct_rows([], saved, self.tag_map)[0]["prim"], "Sports")

    def test_duplicate_or_invalid_metadata_and_saved_keys_fail_closed(self):
        cases = [([{"condition_id": "a", "tags": []}] * 2, [], "duplicate_condition_id"),
                 ([], [{"mkt": "a", "prim": "Sports"}] * 2, "duplicate_mkt"),
                 ([{"condition_id": "", "tags": []}], [], "invalid_condition_id"),
                 ([{"condition_id": "a"}], [], "missing_condition_id_or_tags"),
                 ([], [{"mkt": "a", "prim": "Other"}], "saved_label_outside")]
        for metadata, saved, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(categories.ReconstructionError, error):
                categories.reconstruct_rows(metadata, saved, self.tag_map)

    def test_map_duplicates_invalid_categories_and_exclusion_types_fail_closed(self):
        cases = [(self.rows * 2, "duplicate_tag_map_label"),
                 ([{"tag": "x", "prim_cat": "Other", "excluded": False}], "invalid_tag_category"),
                 ([{"tag": "x", "prim_cat": "Sports", "excluded": True}], "invalid_tag_category"),
                 ([{"tag": "x", "prim_cat": None, "excluded": 1}], "invalid_tag_exclusion"),
                 ([{"tag": "x", "prim_cat": "Sports", "excluded": False, "weight": 5}], "tag_map_fields_differ")]
        for rows, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(categories.ReconstructionError, error):
                categories.compile_tag_map(rows)

    def test_file_loader_requires_exact_frozen_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fixture-map.json"
            path.write_text(json.dumps(self.rows))
            with self.assertRaisesRegex(categories.ReconstructionError, "frozen_tag_map_hash_differs"):
                categories.load_frozen_tag_map(path)
            expected = hashlib.sha256(path.read_bytes()).hexdigest()
            with patch.object(categories, "FROZEN_TAG_MAP_SHA256", expected):
                self.assertEqual(categories.load_frozen_tag_map(path), self.tag_map)

    def test_rule_description_is_detached_deterministic_and_reads_no_data(self):
        contract = categories.rule_contract()
        self.assertEqual(contract, categories.rule_contract())
        contract["rule"]["categories"].clear()
        self.assertEqual(categories.rule_contract()["rule"]["categories"], list(categories.CATEGORIES))
        output = io.StringIO()
        with patch.object(Path, "read_bytes", side_effect=AssertionError("no data reads")), redirect_stdout(output):
            self.assertEqual(categories.main(["--describe-rule"]), 0)
        self.assertEqual(json.loads(output.getvalue()), categories.rule_contract())


if __name__ == "__main__":
    unittest.main()
