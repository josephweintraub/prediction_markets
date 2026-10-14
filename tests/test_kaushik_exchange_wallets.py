"""Small source/reference fixtures; no real data or production guard bypass."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from scripts import audit_kaushik_exchange_wallets as audit


RAW_SCHEMA = pa.schema([("proxyWallet", pa.string()), ("timestamp", pa.int64()),
    ("conditionId", pa.string()), ("usdcSize", pa.float64()), ("price", pa.float64()),
    ("side", pa.string()), ("outcome", pa.string()), ("eventSlug", pa.string()),
    ("is_maker", pa.bool_()), ("counterparty", pa.string()), ("year_month", pa.string())])
BASE_SCHEMA = pa.schema([(name, kind) for name, kind in zip(audit.FIELDS,
    (pa.int64(), pa.string(), pa.int32(), pa.int32(), pa.int64(), pa.float64(), pa.float64(),
     pa.int8(), pa.float64(), pa.string(), pa.bool_(), pa.bool_()))])
EXCLUSION_SCHEMA = pa.schema([("source_ordinal", pa.int64()), ("eligibility_reason", pa.string()),
                             ("side", pa.string()), ("is_maker", pa.bool_())])
MONTH = "2026-03"
STAMP = int(datetime(2026, 3, 10, tzinfo=timezone.utc).timestamp())


def raw_record(**changes):
    value = {"proxyWallet": "private-person-a", "counterparty": "private-person-b", "timestamp": STAMP,
        "conditionId": "yes", "usdcSize": 3., "price": .05, "side": "BUY", "outcome": "Yes",
        "eventSlug": "mechanical-updown", "is_maker": False, "year_month": MONTH}
    value.update(changes)
    return value


def base_record(ordinal, raw, duration=True):
    sell = raw["side"] == "SELL"
    return dict(zip(audit.FIELDS, (ordinal, MONTH, int(sell), 0, raw["timestamp"], raw["price"],
        1. - raw["price"] if sell else raw["price"], 0 if sell else 1, raw["usdcSize"],
        raw["side"], raw["is_maker"], duration)))


def save_fixture(folder, con, records, admitted=None, claims=None, duration=True):
    folder = Path(folder)
    raw_path, base_path, mask_path = folder / "raw.parquet", folder / "base.parquet", folder / "mask.parquet"
    pq.write_table(pa.Table.from_pylist(records, schema=RAW_SCHEMA), raw_path)
    admitted = list(range(len(records))) if admitted is None else admitted
    pq.write_table(pa.Table.from_pylist([base_record(i, records[i], duration) for i in admitted], schema=BASE_SCHEMA), base_path)
    excluded = [{"source_ordinal": i, "eligibility_reason": "invalid_recorded_price", "side": records[i]["side"],
                 "is_maker": records[i]["is_maker"]} for i in range(len(records)) if i not in admitted]
    pq.write_table(pa.Table.from_pylist(excluded, schema=EXCLUSION_SCHEMA), folder / "exclusions.parquet")
    opening = float(STAMP - 10 * 86400)
    endpoint = float(STAMP + 30 * 86400)
    claims = claims or [{"claim_code": i, "opening_seconds": opening, "endpoint_seconds": endpoint} for i in (0, 1)]
    con.register("fixture_claims", pa.Table.from_pylist(claims))
    con.execute("CREATE OR REPLACE TEMP VIEW claims AS SELECT * FROM fixture_claims")
    audit.create_source_view(con, raw_path, MONTH)
    con.execute(f"COPY ({audit.source_mask_sql(MONTH)}) TO {audit.inputs.literal(mask_path)} (FORMAT PARQUET)")
    return raw_path, base_path, mask_path


def accepted_fixture(folder, con, *, eligible_v3=False):
    """Complete small input/control lineage for preflight and publication tests."""
    records = [raw_record(proxyWallet=audit.DIAGNOSTIC_EXCHANGES[0] if eligible_v3 else audit.EXCHANGES[0]),
               raw_record(is_maker=True), raw_record(counterparty=audit.DIAGNOSTIC_EXCHANGES[0], price=0.)]
    raw, base, _ = save_fixture(folder, con, records, admitted=[0, 1])
    reference = folder / "reference"
    month = reference / ("year_month=" + MONTH)
    month.mkdir(parents=True)
    pq.write_table(pq.read_table(base), month / "base.parquet")
    pq.write_table(pq.read_table(folder / "exclusions.parquet"), month / "exclusions.parquet")
    claims = con.execute("SELECT * FROM claims").fetch_arrow_table()
    pq.write_table(claims, reference / "claims.parquet")
    metadata_rows = {
        "token_map": [{"token_id": "yes", "condition_id": "condition", "outcome": "Yes"}],
        "spine": [{"token_id": "yes", "market_id": "condition", "winning_outcome": "Yes"}],
        "native": [{"condition_id": "condition", "n_outcomes": 2, "created_at": "2026-03-01T00:00:00Z",
                    "end_date": "2026-04-09T00:00:00Z", "event_slug": "fixture"}],
        "categories": [{"mkt": "condition", "prim": "Sports"}]}
    metadata = {}
    for name, rows in metadata_rows.items():
        path = folder / (name + ".parquet")
        pq.write_table(pa.Table.from_pylist(rows), path)
        metadata[name] = {"path": str(path), "sha256": audit.inputs.sha256(path)}
    contract = {"schema_version": "kaushik_replication_input_contract_v1", "sample_scope": "all_available_dates",
                "metadata": metadata, "caps": audit.inputs.CAPS}
    contract_path, repair_path = folder / "contract.json", folder / "repair.json"
    audit.inputs.write_json(contract_path, contract)
    audit.inputs.write_json(repair_path, {"schema_version": "polymarket_wallet_repair_v1", "status": "repair_complete"})
    binding = {"contract": audit.inputs.read_json(contract_path)[1], "repair_manifest": audit.inputs.read_json(repair_path)[1]}
    raw_info = {**audit.inputs.footer(raw, "trades"), "month": MONTH, "expected_sha256": audit.inputs.sha256(raw)}
    native_info = {name: {**audit.inputs.footer(item["path"], name), "expected_sha256": item["sha256"]}
                   for name, item in metadata.items()}
    source_inputs = {str(raw): raw_info["expected_sha256"], **{i["path"]: i["sha256"] for i in metadata.values()}}
    manifest = {"schema_version": "kaushik_replication_inputs_v1", "status": "inputs_complete",
        "source": {"head": "b" * 40}, "sample_scope": "all_available_dates", "binding": binding,
        "definitions": audit.inputs.sample_definitions("all_available_dates"), "inputs": source_inputs,
        "input_preflight": {"contract": contract, "trades": [raw_info], "metadata": native_info},
        "rows": {"source": 3, "baseline_all_roles": 2, "excluded": 1, "primary_taker": 1},
        "reconciliation": {k: True for k in ("all_records_accounted", "archive_multiplicity_preserved",
            "no_join_fanout", "unique_reciprocal_binary_complements", "outputs_reopened")},
        "outputs": {str(p.relative_to(reference)): audit.inputs._output_info(p) for p in
            (reference / "claims.parquet", month / "base.parquet", month / "exclusions.parquet")},
        "exclusions": {MONTH: [{"reason": "eligible", "rows": 2, "scope_rows": 2},
                               {"reason": "invalid_recorded_price", "rows": 1, "scope_rows": 1}]},
        "support": [{"is_maker": False, "side": "BUY", "rows": 1, "duration_tail_rows": 1},
                    {"is_maker": True, "side": "BUY", "rows": 1, "duration_tail_rows": 1}]}
    audit.inputs.write_json(reference / "manifest.json", manifest)
    audit.inputs.write_json(reference / "acceptance.json", {"schema_version": "kaushik_replication_input_acceptance_v1",
        "status": "inputs_reopened_accepted", "manifest_sha256": audit.inputs.sha256(reference / "manifest.json"),
        "source_head": manifest["source"]["head"], "rows": manifest["rows"], "sample_scope": "all_available_dates",
        "output_schema_hash_count_support_reopened": True})
    raw_files = [{"month": MONTH, "path": str(raw), "sha256": raw_info["expected_sha256"],
                  "bytes": raw_info["stat"]["bytes"], "rows": 3}]
    return reference, (contract, raw_files, binding)


class TraceConnection:
    def __init__(self, con):
        self.con = con
        self.queries = []

    def execute(self, query, *args, **kwargs):
        self.queries.append(query)
        return self.con.execute(query, *args, **kwargs)


class ExchangeWalletFixtures(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="kaushik-exchange-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)

    def test_literal_four_address_either_field_case_and_unique_row_counts(self):
        records = [raw_record(**{field: address.upper()}) for address in audit.EXCHANGES
                   for field in ("proxyWallet", "counterparty")]
        records += [raw_record(proxyWallet=audit.EXCHANGES[0], counterparty=audit.EXCHANGES[1]),
                    raw_record(proxyWallet=audit.EXCHANGES[0], counterparty=audit.EXCHANGES[0]),
                    raw_record(proxyWallet=None, counterparty=""),
                    raw_record(proxyWallet=" " + audit.EXCHANGES[0]),
                    raw_record(counterparty=audit.EXCHANGES[0] + " "),
                    raw_record(proxyWallet=audit.EXCHANGES[0][:-1] + "0"), raw_record()]
        _, base, mask = save_fixture(self.folder, self.con, records)
        self.assertEqual(audit.validate_mask(self.con, mask, MONTH, len(records)), 10)
        audit.compare_source_mask(self.con, mask, MONTH)
        result = audit.audit_month(self.con, base, mask, MONTH, len(records))
        self.assertEqual(result["counts"]["full"]["baseline"], {"original": 15, "retained": 5, "removed": 10})
        hits = audit.address_field_counts(result["eligible_groups"], "full_baseline")
        self.assertEqual(sum(row["rows"] for row in hits), 12)
        self.assertEqual(set(pq.ParquetFile(mask).schema_arrow.names), set(audit.MASK_FIELDS))
        serialized = repr(pq.read_table(mask).to_pylist())
        self.assertNotIn("private-person", serialized)
        self.assertNotIn("proxyWallet", pq.ParquetFile(mask).schema_arrow.names)

    def test_cutoff_equality_roles_actions_and_duplicate_economic_rows(self):
        records = [raw_record(timestamp=audit.inputs.CUTOFF_SECONDS - 1, proxyWallet=audit.EXCHANGES[0]),
            raw_record(timestamp=audit.inputs.CUTOFF_SECONDS, counterparty=audit.EXCHANGES[1], side="SELL", is_maker=True),
            raw_record(timestamp=audit.inputs.CUTOFF_SECONDS + 1, proxyWallet=audit.EXCHANGES[2], is_maker=True),
            raw_record(side="SELL"), raw_record(side="SELL")]
        _, base, mask = save_fixture(self.folder, self.con, records)
        result = audit.audit_month(self.con, base, mask, MONTH, len(records))
        self.assertEqual(result["counts"]["cutoff"]["baseline"], {"original": 3, "retained": 2, "removed": 1})
        self.assertEqual(result["counts"]["full"]["baseline"], {"original": 5, "retained": 2, "removed": 3})
        self.assertEqual(result["EXCEPT_ALL_differences"], [0, 0])
        self.assertEqual(audit.role_counts(result["eligible_groups"], "full", "baseline", "removed"),
                         dict(zip(audit.subset.PARTITIONS, (3, 1, 2, 1, 1))))
        self.assertEqual(self.con.execute("SELECT source_ordinal FROM prior_base ORDER BY source_ordinal").fetchall(), [(i,) for i in range(5)])

    def test_duration_strict_one_day_boundaries_and_primary_counts(self):
        records = [raw_record(proxyWallet=audit.EXCHANGES[0]), raw_record(), raw_record(timestamp=STAMP + 1)]
        claims = [{"claim_code": i, "opening_seconds": float(STAMP), "endpoint_seconds": float(STAMP + 86400)} for i in (0, 1)]
        _, base, mask = save_fixture(self.folder, self.con, records, claims=claims)
        result = audit.audit_month(self.con, base, mask, MONTH, len(records))
        self.assertEqual(result["counts"]["full"]["common_duration"]["original"], 3)
        self.assertEqual(result["counts"]["full"]["lifespan_gt1"]["original"], 0)
        self.assertEqual(result["counts"]["full"]["remaining_gt1"]["original"], 0)
        self.con.execute("CREATE OR REPLACE TEMP VIEW claims AS SELECT claim_code,opening_seconds,endpoint_seconds+1 endpoint_seconds FROM fixture_claims")
        result = audit.audit_month(self.con, base, mask, MONTH, len(records))
        self.assertEqual(result["counts"]["full"]["lifespan_gt1"]["original"], 3)
        self.assertEqual(result["counts"]["full"]["remaining_gt1"]["original"], 2)

    def test_noneligible_source_matches_and_v3_diagnostic_are_retained_for_audit(self):
        records = [raw_record(proxyWallet=audit.EXCHANGES[0]),
                   raw_record(proxyWallet=audit.EXCHANGES[0], price=0.),
                   raw_record(counterparty=audit.DIAGNOSTIC_EXCHANGES[0], price=0.), raw_record()]
        _, base, mask = save_fixture(self.folder, self.con, records, admitted=[0, 3])
        result = audit.audit_month(self.con, base, mask, MONTH, len(records))
        self.assertEqual(result["mask_rows"], 3)
        self.assertEqual(result["unsupported_eligible_rows"], 0)
        intersection = audit.exclusion_intersection(self.con, self.folder / "exclusions.parquet", len(records))
        self.assertEqual(intersection["matched_noneligible_rows"], 2)
        self.assertEqual(intersection["first_failure_counts"][0]["eligibility_reason"], "invalid_recorded_price")
        raw = audit.raw_match_counts(self.con, MONTH)
        self.assertEqual(sum(r["full_rows"] for r in raw), 3)
        self.assertEqual(sum(r["full_baseline"] for r in result["eligible_groups"] if r["matched_proxy_address"] or r["matched_counterparty_address"]), 1)

    def test_v3_eligible_blocks_even_when_other_field_is_supported(self):
        records = [raw_record(proxyWallet=audit.DIAGNOSTIC_EXCHANGES[0], counterparty=audit.EXCHANGES[0])]
        _, base, mask = save_fixture(self.folder, self.con, records)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "V3"):
            audit.audit_month(self.con, base, mask, MONTH, 1)
        result = audit.audit_month(self.con, base, mask, MONTH, 1, fail_on_unsupported=False)
        self.assertEqual(result["unsupported_eligible_rows"], 1)
        self.assertEqual(result["counts"]["full"]["baseline"]["removed"], 1)

    def test_mask_missing_added_duplicate_or_false_flag_fails_closed(self):
        _, _, mask = save_fixture(self.folder, self.con, [raw_record(proxyWallet=audit.EXCHANGES[0]), raw_record()])
        good = pq.read_table(mask).to_pylist()
        variants = [[], good * 2, [{**good[0], "source_ordinal": 2}],
                    [{**good[0], "source_month": "2026-04"}], [{**good[0], "proxy_match": False}],
                    [{**good[0], "known_exchange_match": False}], [{**good[0], "matched_proxy_address": "private-person-a"}],
                    [{**good[0], "matched_proxy_address": audit.EXCHANGES[0].upper()}]]
        for index, rows in enumerate(variants):
            bad = self.folder / f"bad-{index}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=audit.MASK_SCHEMA), bad)
            with self.subTest(index=index), self.assertRaises(audit.inputs.InputBlocked):
                audit.validate_mask(self.con, bad, MONTH, 2)
                audit.compare_source_mask(self.con, bad, MONTH)
        added = self.folder / "valid-looking-added.parquet"
        pq.write_table(pa.Table.from_pylist(good + [{**good[0], "source_ordinal": 1}], schema=audit.MASK_SCHEMA), added)
        audit.validate_mask(self.con, added, MONTH, 2)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "source/mask"):
            audit.compare_source_mask(self.con, added, MONTH)

    def test_bad_base_locators_duration_or_exclusion_overlap_fail_closed(self):
        _, base, mask = save_fixture(self.folder, self.con, [raw_record(proxyWallet=audit.EXCHANGES[0]), raw_record()])
        original = pq.read_table(base).to_pylist()
        for index, rows in enumerate((original + [original[0]], [{**original[0], "source_ordinal": 2}],
            [{**original[0], "source_month": "2026-04"}], [{**original[0], "duration_eligible": False}],
            [{**original[0], "P": .3}])):
            bad = self.folder / f"base-bad-{index}.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=BASE_SCHEMA), bad)
            with self.subTest(index=index), self.assertRaises(audit.inputs.InputBlocked):
                audit.audit_month(self.con, bad, mask, MONTH, 2)
        audit.audit_month(self.con, base, mask, MONTH, 2)
        exclusions = self.folder / "overlap.parquet"
        pq.write_table(pa.Table.from_pylist([{"source_ordinal": 0, "eligibility_reason": "invalid_size", "side": "BUY", "is_maker": False}], schema=EXCLUSION_SCHEMA), exclusions)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "partition"):
            audit.exclusion_intersection(self.con, exclusions, 2)

    def test_physical_setting_scalar_readback_and_drift(self):
        self.assertEqual(audit.configure(self.con, self.folder / "spill"), audit.JOIN_PLANNING_POLICY)
        self.con.execute("SET disabled_optimizers=''")
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "physical join"):
            audit.planning_policy(self.con)

    def test_guard_precedes_any_source_or_data_access(self):
        args = ["--reference-dir", "/missing", "--reference-manifest-sha256", "a" * 64,
                "--reference-acceptance-sha256", "b" * 64, "--expected-head", "c" * 40,
                "--run-dir", "/mnt/data/runs/new"]
        with patch("production_guard.require_production_host", side_effect=RuntimeError("guard blocked")), \
             patch.object(audit, "source_snapshot") as source, patch.object(audit, "preflight") as data:
            with self.assertRaisesRegex(RuntimeError, "guard blocked"):
                audit.main(args)
            source.assert_not_called(); data.assert_not_called()

    def test_caps_and_policy_fail_before_body_io_and_immutable_destination(self):
        for key, value in (("caps", {**audit.CAPS, "threads": 5}),
                           ("policy", {**audit.POLICY, "null_blank_match": True})):
            fresh = {"caps": deepcopy(audit.CAPS), "policy": deepcopy(audit.POLICY), key: value}
            with self.subTest(key=key), self.assertRaisesRegex(audit.inputs.InputBlocked, "caps/policy"):
                audit.audit_stage(fresh, fresh)
        self.assertEqual(audit.CAPS["maximum_output_bytes"] + audit.CAPS["spill_bytes"] + audit.CAPS["minimum_free_bytes"], 44_000_000_000)
        first, target, second = self.folder / "first", self.folder / "target", self.folder / "second"
        first.mkdir(); second.mkdir()
        audit.inputs.atomic_publish(first, target)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "immutable"):
            audit.inputs.atomic_publish(second, target)

    def test_read_and_output_caps_boundaries(self):
        ledger = audit.ReadLedger()
        ledger.charge(audit.CAPS["maximum_read_bytes"], "exact")
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "read ceiling"):
            ledger.charge(1, "excess")
        with patch.object(audit.shutil, "disk_usage", return_value=SimpleNamespace(free=44_000_000_000)) as disk:
            audit._reserve(self.folder, audit.CAPS["maximum_mask_file_bytes"])
            disk.return_value = SimpleNamespace(free=43_999_999_999)
            with self.assertRaisesRegex(audit.inputs.InputBlocked, "reservation"):
                audit._reserve(self.folder)

    def test_resource_only_retry_changes_exactly_two_memory_admission_caps(self):
        previous = {"memory_limit": "64GB", "threads": 4, "spill_bytes": 16_000_000_000,
            "minimum_available_ram_bytes": 70_000_000_000, "minimum_free_bytes": 20_000_000_000,
            "maximum_output_bytes": 8_000_000_000, "maximum_mask_file_bytes": 1_000_000_000,
            "maximum_read_bytes": 1_000_000_000_000, "maximum_json_bytes": 16_000_000}
        self.assertEqual(set(audit.CAPS), set(previous))
        changes = {key: (previous[key], value) for key, value in audit.CAPS.items() if value != previous[key]}
        self.assertEqual(changes, {"memory_limit": ("64GB", "192GB"),
                                  "minimum_available_ram_bytes": (70_000_000_000, 240_000_000_000)})

    def test_empty_mask_exact_identity_matches_independent_twelve_field_except_all(self):
        records = [raw_record(), raw_record(), raw_record(side="SELL", price=.9000000000000001, is_maker=True),
                   raw_record(timestamp=audit.inputs.CUTOFF_SECONDS), raw_record(timestamp=audit.inputs.CUTOFF_SECONDS + 1)]
        _, base, mask = save_fixture(self.folder, self.con, records)
        trace, ledger = TraceConnection(self.con), audit.ReadLedger()
        result = audit.audit_month(trace, base, mask, MONTH, len(records), ledger=ledger)
        self.assertEqual(result["mask_rows"], 0)
        self.assertEqual(result["partition_proof_method"], "empty_mask_exact_identity")
        self.assertFalse(result["EXCEPT_ALL_queries_executed"])
        self.assertFalse(any("EXCEPT ALL" in query.upper() for query in trace.queries))
        self.assertNotIn("reference_exact_partition", ledger.scans)
        self.assertEqual(result["counts"]["full"]["baseline"], {"original": 5, "retained": 5, "removed": 0})
        self.assertEqual(result["counts"]["cutoff"]["baseline"], {"original": 3, "retained": 3, "removed": 0})
        fields = ",".join(audit.inputs.ident(f) for f in audit.FIELDS)
        original = f"SELECT {fields} FROM read_parquet({audit.inputs.literal(base)},hive_partitioning=false)"
        retained = f"SELECT {fields} FROM partitioned WHERE NOT removed"
        # These fixture-only oracle queries are deliberately independent of the
        # fast path and preserve identical economic rows at distinct locators.
        differences = [self.con.execute(f"SELECT count(*) FROM ({left} EXCEPT ALL ({right}))").fetchone()[0]
                       for left, right in ((original, retained), (retained, original))]
        self.assertEqual(differences, result["EXCEPT_ALL_differences"])
        self.assertEqual(self.con.execute(f"SELECT {fields} FROM partitioned WHERE NOT removed ORDER BY source_ordinal").fetchall(),
                         self.con.execute(original + " ORDER BY source_ordinal").fetchall())

    def test_nonempty_masks_always_execute_guarded_full_payload_join(self):
        for removed in (True, False):
            with self.subTest(removed=removed), tempfile.TemporaryDirectory() as temp:
                con = duckdb.connect()
                try:
                    records = [raw_record(proxyWallet=audit.EXCHANGES[0], price=.05 if removed else 0.), raw_record()]
                    _, base, mask = save_fixture(temp, con, records, admitted=[0, 1] if removed else [1])
                    trace, ledger = TraceConnection(con), audit.ReadLedger()
                    result = audit.audit_month(trace, base, mask, MONTH, 2, ledger=ledger)
                    self.assertEqual(result["partition_proof_method"], "unique_locator_full_payload_join")
                    self.assertFalse(result["EXCEPT_ALL_queries_executed"])
                    self.assertFalse(any("EXCEPT ALL" in query.upper() for query in trace.queries))
                    payload = [query for query in trace.queries if "FULL OUTER JOIN candidate_union" in query]
                    self.assertEqual(len(payload), 1)
                    self.assertEqual(payload[0].count("IS DISTINCT FROM"), 12)
                    guard = [query for query in trace.queries if "FROM candidate_union" in query and "count(DISTINCT" in query]
                    self.assertEqual(len(guard), 1)
                    self.assertLess(trace.queries.index(guard[0]), trace.queries.index(payload[0]))
                    self.assertIn("candidate_partition_locator_validity", ledger.scans)
                    self.assertIn("reference_exact_partition", ledger.scans)
                    self.assertEqual(result["counts"]["full"]["baseline"]["removed"], int(removed))
                finally:
                    con.close()

    def partition_fixture(self):
        records = [raw_record(), raw_record(), raw_record(is_maker=True)]
        original = [base_record(index, row) for index, row in enumerate(records)]
        self.con.register("original_payload", pa.Table.from_pylist(original, schema=BASE_SCHEMA))
        self.con.execute("CREATE OR REPLACE TEMP VIEW prior_base AS SELECT * FROM original_payload")
        self.install_candidate(original)
        return original

    def install_candidate(self, rows):
        self.con.register("candidate_payload", pa.Table.from_pylist(rows, schema=BASE_SCHEMA))
        self.con.execute("CREATE OR REPLACE TEMP VIEW candidate_union AS SELECT * FROM candidate_payload")

    def partition_oracle(self):
        fields = ",".join(audit.inputs.ident(field) for field in audit.FIELDS)
        return [self.con.execute(f"SELECT count(*) FROM (SELECT {fields} FROM {left} "
                                f"EXCEPT ALL SELECT {fields} FROM {right})").fetchone()[0]
                for left, right in (("prior_base", "candidate_union"), ("candidate_union", "prior_base"))]

    def test_all_twelve_candidate_payload_mutations_detected_with_exact_oracle_counts(self):
        original = self.partition_fixture()
        changes = dict(zip(audit.FIELDS, (7, "2026-04", 3, 4, STAMP + 1,
            .05000000000000001, .05000000000000001, 0, 3.0000000000000004, "SELL", True, False)))
        for field, value in changes.items():
            candidate = deepcopy(original)
            candidate[1][field] = value
            self.install_candidate(candidate)
            trace = TraceConnection(self.con)
            with self.subTest(field=field):
                self.assertEqual(self.partition_oracle(), [1, 1])
                if field == "source_month":
                    with self.assertRaisesRegex(audit.inputs.InputBlocked, "candidate partition locator"):
                        audit.partition_payload_differences(trace, MONTH, 10, len(original))
                    self.assertFalse(any("FULL OUTER JOIN" in query for query in trace.queries))
                else:
                    self.assertEqual(audit.partition_payload_differences(trace, MONTH, 10, len(original)), [1, 1])

    def test_candidate_duplicate_missing_extra_and_invalid_locators_reject_before_join(self):
        original = self.partition_fixture()
        variants = [original + [original[0]], original[:-1], original + [{**original[0], "source_ordinal": 7}],
                    original[:-1] + [original[0]]]
        for changes in ({"source_ordinal": None}, {"source_ordinal": -1}, {"source_ordinal": 10},
                        {"source_month": None}, {"source_month": "2026-04"}):
            candidate = deepcopy(original)
            candidate[1].update(changes)
            variants.append(candidate)
        for index, candidate in enumerate(variants):
            self.install_candidate(candidate)
            trace = TraceConnection(self.con)
            with self.subTest(index=index), self.assertRaises(audit.inputs.InputBlocked):
                audit.partition_payload_differences(trace, MONTH, 10, len(original))
            self.assertFalse(any("FULL OUTER JOIN" in query for query in trace.queries))

    def test_both_relation_names_types_and_order_guard_before_join(self):
        self.partition_fixture()
        fields = ",".join(audit.inputs.ident(field) for field in audit.FIELDS)
        variants = ("SELECT * EXCLUDE(Y) FROM {source}",
                    "SELECT * EXCLUDE(Y),Y::INTEGER Y FROM {source}",
                    "SELECT * REPLACE(claim_code::BIGINT AS claim_code) FROM {source}",
                    "SELECT * EXCLUDE(Y),Y renamed_Y FROM {source}")
        for relation, source in (("prior_base", "original_payload"), ("candidate_union", "candidate_payload")):
            for query in variants:
                self.con.execute(f"CREATE OR REPLACE TEMP VIEW {relation} AS " + query.format(source=source))
                trace = TraceConnection(self.con)
                with self.subTest(relation=relation, query=query), \
                     self.assertRaisesRegex(audit.inputs.InputBlocked, "12-column names/types"):
                    audit.partition_payload_differences(trace, MONTH, 10, 3)
                self.assertFalse(any("FULL OUTER JOIN" in entry for entry in trace.queries))
                self.con.execute(f"CREATE OR REPLACE TEMP VIEW {relation} AS SELECT {fields} FROM {source}")

    def test_nonempty_reference_type_drift_blocks_despite_unchanged_field_names(self):
        _, base, mask = save_fixture(self.folder, self.con, [raw_record(proxyWallet=audit.EXCHANGES[0]), raw_record()])
        changed = pq.read_table(base).cast(pa.schema([(field.name, pa.int64() if field.name == "claim_code" else field.type)
                                                    for field in BASE_SCHEMA]))
        path = self.folder / "wrong-types.parquet"
        pq.write_table(changed, path)
        trace = TraceConnection(self.con)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "prior_base exact 12-column names/types"):
            audit.audit_month(trace, path, mask, MONTH, 2)
        self.assertFalse(any("FULL OUTER JOIN candidate_union" in query for query in trace.queries))

    def test_nonempty_join_ledger_remains_within_original_partition_scan_bound(self):
        self.partition_fixture()
        ledger = audit.ReadLedger()
        self.assertEqual(audit.partition_payload_differences(self.con, MONTH, 10, 3,
                         ledger=ledger, base_bytes=11, mask_bytes=7, claims_bytes=5), [0, 0])
        self.assertEqual(ledger.scans, {"candidate_partition_locator_validity": 46,
                                        "reference_exact_partition": 57})
        self.assertEqual(ledger.bytes, 5 * 11 + 4 * 7 + 4 * 5)
        self.assertLessEqual(ledger.bytes, 6 * 11 + 4 * 7 + 4 * 5)
        self.assertIn("tests/test_exchange_partition_payload_equivalence.py", audit.SOURCE_FILES)

    def test_empty_mask_still_rejects_bad_reference_missing_claims_and_fanout(self):
        _, base, mask = save_fixture(self.folder, self.con, [raw_record(), raw_record()])
        rows = pq.read_table(base).to_pylist()
        invalid = self.folder / "empty-mask-invalid-base.parquet"
        pq.write_table(pa.Table.from_pylist([{**rows[0], "P": 0.}], schema=BASE_SCHEMA), invalid)
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "reference locator/analytical"):
            audit.audit_month(self.con, invalid, mask, MONTH, 2)
        self.con.execute("CREATE OR REPLACE TEMP VIEW claims AS SELECT * FROM fixture_claims WHERE claim_code<>0")
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "claims/duration"):
            audit.audit_month(self.con, base, mask, MONTH, 2)
        self.con.execute("CREATE OR REPLACE TEMP VIEW claims AS SELECT * FROM fixture_claims UNION ALL SELECT * FROM fixture_claims WHERE claim_code=0")
        with self.assertRaisesRegex(audit.inputs.InputBlocked, "fanout"):
            audit.audit_month(self.con, base, mask, MONTH, 2)

    def test_empty_mask_observed_flags_are_explicitly_validated(self):
        _, base, mask = save_fixture(self.folder, self.con, [raw_record()])
        real_rows = audit._rows
        def damaged_groups(con, query):
            groups = real_rows(con, query)
            if "FROM partitioned GROUP BY ALL" in query:
                groups[0]["removed"] = True
            return groups
        with patch.object(audit, "_rows", side_effect=damaged_groups), \
             self.assertRaisesRegex(audit.inputs.InputBlocked, "empty mask observed"):
            audit.audit_month(self.con, base, mask, MONTH, 1)

    def preflight_fixture(self, *, eligible_v3=False):
        reference, loaded = accepted_fixture(self.folder, self.con, eligible_v3=eligible_v3)
        source = {"head": "a" * 40, "files": {}}
        with patch.object(audit, "SOURCE_MONTHS", (MONTH,)), \
             patch.object(audit.inputs, "load_contract", return_value=loaded), \
             patch.object(audit.inputs, "validate_destination"), \
             patch.object(audit.os, "cpu_count", return_value=4), \
             patch.object(Path, "read_text", return_value=f"MemAvailable: {audit.CAPS['minimum_available_ram_bytes'] // 1024} kB\n"), \
             patch.object(audit.shutil, "disk_usage", return_value=SimpleNamespace(free=44_000_000_000)):
            fresh = audit.preflight(reference, audit.inputs.sha256(reference / "manifest.json"),
                audit.inputs.sha256(reference / "acceptance.json"), self.folder / "published-audit", source)
        return fresh, loaded

    def test_preflight_capacity_hash_version_schema_and_source_month_admission(self):
        fresh, loaded = self.preflight_fixture()
        self.assertEqual(fresh["required_free_bytes"], 44_000_000_000)
        self.assertEqual(fresh["join_planning_policy"], audit.JOIN_PLANNING_POLICY)
        self.assertEqual(fresh["policy"], audit.POLICY)
        reference = fresh["reference"]
        with patch.object(audit, "SOURCE_MONTHS", (MONTH,)), \
             patch.object(audit.inputs, "load_contract", return_value=loaded) as load, \
             patch.object(audit.inputs, "validate_destination"), patch.object(audit.os, "cpu_count", return_value=4) as cpu, \
             patch.object(Path, "read_text", return_value=f"MemAvailable: {audit.CAPS['minimum_available_ram_bytes'] // 1024} kB\n") as ram, \
             patch.object(audit.shutil, "disk_usage", return_value=SimpleNamespace(free=44_000_000_000)) as disk:
            def check():
                return audit.preflight(reference["directory"], reference["bindings"]["manifest"]["sha256"],
                    reference["bindings"]["acceptance"]["sha256"], self.folder / "second-audit", fresh["source"])
            disk.return_value = SimpleNamespace(free=43_999_999_999)
            with self.assertRaisesRegex(audit.inputs.InputBlocked, "reservation"): check()
            disk.return_value = SimpleNamespace(free=44_000_000_000)
            ram.return_value = f"MemAvailable: {audit.CAPS['minimum_available_ram_bytes'] // 1024 - 1} kB\n"
            with self.assertRaisesRegex(audit.inputs.InputBlocked, "RAM"): check()
            ram.return_value = f"MemAvailable: {audit.CAPS['minimum_available_ram_bytes'] // 1024} kB\n"; cpu.return_value = 3
            with self.assertRaisesRegex(audit.inputs.InputBlocked, "four-thread"): check()
            cpu.return_value = 4
            altered = deepcopy(loaded); altered[1][0]["sha256"] = "f" * 64
            load.return_value = altered
            with self.assertRaisesRegex(audit.inputs.InputBlocked, "binding"): check()
            load.return_value = loaded
            with patch.object(audit, "SOURCE_MONTHS", (MONTH, "2026-04")):
                with self.assertRaisesRegex(audit.inputs.InputBlocked, "44-month"): check()
        with self.assertRaises(audit.inputs.InputBlocked):
            audit.subset.load_stage(reference["directory"], "f" * 64,
                reference["bindings"]["acceptance"]["sha256"], "all_available_dates", audit.ReadLedger())

    def test_complete_fixture_stage_reopens_masks_and_seals_final_counts_ledger(self):
        fresh, _ = self.preflight_fixture()
        review_path = self.folder / "review.json"
        audit.inputs.write_json(review_path, fresh)
        review, identity = audit.inputs.read_json(review_path)
        before = {i["path"]: audit.inputs.sha256(i["path"]) for i in fresh["raw_sources"]}
        with patch.object(audit, "source_snapshot", return_value=fresh["source"]), \
             patch.object(audit.shutil, "disk_usage", return_value=SimpleNamespace(free=44_000_000_000)):
            result = audit.audit_stage(review, fresh, identity, ["fixture-only"])
        target = Path(fresh["target"])
        receipt, _ = audit.inputs.read_json(target / "acceptance.json")
        self.assertEqual(result["status"], "exchange_wallet_audit_complete")
        self.assertEqual(result["totals"]["full"]["baseline"], {"original": 2, "retained": 1, "removed": 1})
        self.assertEqual(result["role_totals"]["full"]["common_duration"]["removed"]["primary_taker"], 1)
        self.assertEqual(result["months"][0]["raw_minus_eligible"]["full"],
                         {"raw_matched": 2, "eligible_matched": 1, "noneligible_matched": 1})
        self.assertEqual(receipt["status"], "exchange_wallet_masks_reopened_accepted")
        self.assertEqual(receipt["audit_sha256"], audit.inputs.sha256(target / "audit.json"))
        self.assertEqual(receipt["charged_read_bytes"], result["read_ledger"]["charged_bytes"])
        self.assertLessEqual(receipt["charged_read_bytes"], fresh["planned_read_bytes"])
        self.assertEqual(before, {path: audit.inputs.sha256(path) for path in before})
        self.assertFalse(any(p.name == "base.parquet" for p in target.rglob("*")))

    def test_v3_body_preserves_diagnostics_but_never_publishes_acceptance(self):
        fresh, _ = self.preflight_fixture(eligible_v3=True)
        review_path = self.folder / "review.json"
        audit.inputs.write_json(review_path, fresh)
        review, identity = audit.inputs.read_json(review_path)
        with patch.object(audit, "source_snapshot", return_value=fresh["source"]), \
             patch.object(audit.shutil, "disk_usage", return_value=SimpleNamespace(free=44_000_000_000)), \
             self.assertRaisesRegex(audit.inputs.InputBlocked, "V3"):
            audit.audit_stage(review, fresh, identity, ["fixture-only"])
        self.assertFalse(Path(fresh["target"]).exists())
        failures = list(self.folder.glob(".published-audit.staging-*/failure.json"))
        self.assertEqual(len(failures), 1)
        failure, _ = audit.inputs.read_json(failures[0])
        self.assertFalse(failure["scientific_admission"])
        self.assertEqual(failure["months"][0]["unsupported_eligible_rows"], 1)


if __name__ == "__main__":
    unittest.main()
