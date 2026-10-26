"""Tiny synthetic Parquet fixtures; production files and APIs are never opened."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import reconstruct_polymarket_categories as candidate
from scripts import validate_polymarket_category_reconstruction as validator


class CategoryParityValidatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.metadata = self.root / "metadata.parquet"
        self.labels = self.root / "labels.parquet"
        self.tag_map = candidate.compile_tag_map([
            {"tag": "Sports", "prim_cat": "Sports", "excluded": False},
            {"tag": "Politics", "prim_cat": "Politics", "excluded": False},
            {"tag": "Crypto", "prim_cat": "Crypto", "excluded": False},
            {"tag": "Games", "prim_cat": None, "excluded": True}])
        self.populate()

    def populate(self, metadata=None, labels=None):
        metadata = metadata if metadata is not None else [
            {"condition_id": "a", "tags": ["Sports", "unknown"]},
            {"condition_id": "b", "tags": ["Politics"]},
            {"condition_id": "c", "tags": ["Politics", "Sports"]},
            {"condition_id": "new", "tags": ["Crypto"]}]
        labels = labels if labels is not None else [
            {"mkt": "a", "prim": "Politics"}, {"mkt": "b", "prim": "Politics"},
            {"mkt": "c", "prim": None}, {"mkt": "old", "prim": "Sports"}]
        self.metadata_rows, self.label_rows = metadata, labels
        pq.write_table(pa.Table.from_pylist(metadata, schema=pa.schema([
            ("condition_id", pa.large_string()), ("tags", pa.list_(pa.string()))])), self.metadata,
            row_group_size=2)
        pq.write_table(pa.Table.from_pylist(labels, schema=pa.schema([
            ("mkt", pa.string()), ("prim", pa.string())])), self.labels, row_group_size=2)

    def compare(self):
        with duckdb.connect() as con:
            validator.configure(con)
            health = validator.prepare_views(con, self.metadata, self.labels)
            return validator.stream_comparison(con, self.tag_map, health)

    def test_footer_projection_matches_observed_historical_schema_without_body_reader(self):
        with patch.object(pq, "read_table", side_effect=AssertionError("no bodies")), \
                patch.object(pq, "ParquetFile", side_effect=AssertionError("no normal file reader")):
            result = validator.inspect_footer(self.metadata, "metadata", 4)
        self.assertEqual(result["projection"], ["condition_id", "tags"])
        self.assertEqual(result["rows"], 4)
        self.assertEqual(result["body_rows_read"], 0)
        self.assertTrue(any(path.startswith("tags.") for path in result["projected_leaf_paths"]))
        self.assertGreater(result["projected_compressed_bytes"], 0)

    def test_unknown_schema_is_refused_instead_of_cast_or_column_guess(self):
        pq.write_table(pa.table({"condition_id": ["a"], "tags": ['["Sports"]']}), self.metadata)
        with self.assertRaisesRegex(validator.ValidationError, "historical_metadata_schema_differs"):
            validator.inspect_footer(self.metadata, "metadata", 1)

    def test_footer_row_and_byte_caps_precede_body_scans(self):
        with self.assertRaisesRegex(validator.ValidationError, "footer_row_count"):
            validator.inspect_footer(self.metadata, "metadata", 3)
        with patch.dict(validator.CAPS, {"single_footer_bytes": 1}), \
                patch.object(pq, "read_metadata", side_effect=AssertionError("must reject before decode")):
            with self.assertRaisesRegex(validator.ValidationError, "footer_byte_limit"):
                validator.inspect_footer(self.metadata, "metadata", 4)

    def test_streamed_shadow_counts_equal_frozen_small_helper(self):
        observed = self.compare()
        expected = candidate.summarize_validation(self.metadata_rows, self.label_rows, self.tag_map)
        self.assertEqual(observed["confusion"], expected["confusion"])
        self.assertEqual(observed["tag_evidence_overlap"], expected["tag_evidence"])
        self.assertEqual(observed["reason_counts_overlap"], expected["reason_counts"])
        self.assertEqual(observed["unknown_tag_market_counts_overlap"], {"unknown": 1})
        self.assertEqual(observed["counts"]["agreement"], 2)
        self.assertEqual(observed["counts"]["changed_saved_labels"], 0)
        self.assertEqual(observed["counts"]["preserved_raw_null_labels"], 1)
        self.assertEqual(observed["counts"]["saved_label_without_metadata"], 1)
        self.assertEqual(observed["counts"]["union"], 5)
        self.assertFalse(observed["labels_published"])
        self.assertEqual(self.compare()["virtual_merge_sha256"], observed["virtual_merge_sha256"])

    def test_duplicate_metadata_or_saved_rows_are_not_deduplicated(self):
        for side in ("metadata", "labels"):
            with self.subTest(side=side):
                self.populate()
                self.populate(self.metadata_rows * 2 if side == "metadata" else self.metadata_rows,
                              self.label_rows * 2 if side == "labels" else self.label_rows)
                with self.assertRaisesRegex(validator.ValidationError, "duplicate_or_invalid"):
                    self.compare()

    def test_invalid_saved_label_is_not_replaced_by_reconstruction(self):
        self.populate(labels=[{"mkt": "a", "prim": "Other"}])
        with self.assertRaisesRegex(validator.ValidationError, "saved_label_outside"):
            self.compare()

    def test_nulls_missing_metadata_and_malformed_tags_are_accounted(self):
        self.populate(metadata=[{"condition_id": "a", "tags": [None]},
                                {"condition_id": "b", "tags": ["Mentions", "Iran"]}],
                      labels=[{"mkt": "a", "prim": None}, {"mkt": "b", "prim": "Sports"},
                              {"mkt": "absent", "prim": None}])
        result = self.compare()
        self.assertEqual(result["counts"]["preserved_raw_null_labels"], 2)
        self.assertEqual(result["reason_counts_union"],
                         {"malformed_tags": 1, "metadata_missing": 1, "override_conflict": 1})

    def test_summary_state_limits_fail_without_silent_truncation(self):
        with patch.dict(validator.CAPS, {"unknown_tag_types": 0}):
            with self.assertRaisesRegex(validator.ValidationError, "unknown_tag_state_limit"):
                self.compare()
        with patch.dict(validator.CAPS, {"evidence_strata": 0}):
            with self.assertRaisesRegex(validator.ValidationError, "evidence_state_limit"):
                self.compare()

    def test_empty_inputs_conserve_counts_and_have_no_false_agreement(self):
        self.populate(metadata=[], labels=[])
        result = self.compare()
        self.assertEqual(result["counts"]["union"], 0)
        self.assertEqual(result["counts"]["agreement"], 0)
        self.assertEqual(sum(sum(r.values()) for r in result["confusion"].values()), 0)

    def test_exclusive_atomic_json_writer_reopens_and_never_overwrites(self):
        path = self.root / "receipt.json"
        record = validator.write_json(path, {"status": "first"})
        self.assertEqual(record["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
        with self.assertRaises(FileExistsError):
            validator.write_json(path, {"status": "second"})
        self.assertEqual(json.loads(path.read_text()), {"status": "first"})

    def test_input_hash_detects_different_bytes(self):
        first = validator.sha256_file(self.metadata, validator.CAPS["single_input_bytes"])
        self.populate(metadata=[{"condition_id": "a", "tags": ["Crypto"]}])
        self.assertNotEqual(first, validator.sha256_file(self.metadata, validator.CAPS["single_input_bytes"]))

    def test_resource_projection_gate_refuses_before_memory_or_disk_checks(self):
        footer = validator.inspect_footer(self.metadata, "metadata", 4)
        with patch.dict(validator.CAPS, {"projected_compressed_read_bytes": 1}), \
                patch.object(Path, "read_text", side_effect=AssertionError("already refused")):
            with self.assertRaisesRegex(validator.ValidationError, "projected_scan_byte_limit"):
                validator.admit_resources([footer], self.root)

    def test_date_only_vintage_does_not_invent_midnight_or_utc_instant(self):
        result = validator.vintage_record("2026-06-21")
        self.assertEqual(result["metadata_vintage_declared"], "2026-06-21")
        self.assertEqual(result["metadata_vintage_precision"], "date_only")
        self.assertEqual(result["metadata_vintage_scope"],
                         "documented source generation date; exact retrieval instant unknown")
        self.assertNotIn("T00:00", json.dumps(result))
        self.assertEqual(validator.vintage_record("2026-06-21T13:45:00-04:00")["metadata_vintage_precision"],
                         "timestamp_with_offset")
        with self.assertRaisesRegex(validator.ValidationError, "requires_timezone"):
            validator.vintage_record("2026-06-21T13:45:00")
        with self.assertRaisesRegex(validator.ValidationError, "vintage_invalid"):
            validator.vintage_record("2026-02-30")

    def test_production_guard_precedes_all_source_and_data_access(self):
        with patch.object(validator, "require_production_host", side_effect=RuntimeError("refused")), \
                patch.object(validator, "bind_source", side_effect=AssertionError("no source inspection")), \
                patch.object(validator, "inspect_footer", side_effect=AssertionError("no data inspection")):
            with self.assertRaisesRegex(RuntimeError, "refused"):
                validator.execute(argparse.Namespace())

    def test_help_does_not_inspect_sources_or_data(self):
        with patch.object(validator, "require_production_host", side_effect=AssertionError("help only")), \
                redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as result:
            validator.main(["--help"])
        self.assertEqual(result.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
