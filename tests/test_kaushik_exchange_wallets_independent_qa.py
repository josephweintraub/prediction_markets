"""Independent small source/BASE oracles for exchange-wallet removal.

Every fixture is synthetic.  No accepted production source or artifact is read.
"""
from __future__ import annotations

from collections import Counter
import math
from pathlib import Path
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_polymarket_replication import build_inputs as inputs
from scripts import audit_kaushik_exchange_wallets as audit


FROZEN_EXCHANGES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
V3 = "0xe3333700ca9d93003f00f0f71f8515005f6c00aa"
MONTH = "2026-03"
CUTOFF = 1774396800
BASE_FIELDS = (
    "source_ordinal", "source_month", "claim_code", "recorded_token_code",
    "timestamp", "recorded_price", "P", "Y", "usdcSize", "side", "is_maker",
    "duration_eligible",
)
BASE_SCHEMA = pa.schema(zip(BASE_FIELDS, (
    pa.int64(), pa.string(), pa.int32(), pa.int32(), pa.int64(),
    pa.float64(), pa.float64(), pa.int8(), pa.float64(), pa.string(),
    pa.bool_(), pa.bool_(),
)))
RAW_SCHEMA = pa.schema([
    ("proxyWallet", pa.string()), ("timestamp", pa.int64()),
    ("conditionId", pa.string()), ("usdcSize", pa.float64()),
    ("price", pa.float64()), ("side", pa.string()), ("outcome", pa.string()),
    ("eventSlug", pa.string()), ("is_maker", pa.bool_()),
    ("counterparty", pa.string()), ("year_month", pa.string()),
    ("emitter", pa.string()),
])


def source_rows():
    a, b, c, d = FROZEN_EXCHANGES

    def row(wallet="alice", counterparty="bob", **changes):
        value = {
            "proxyWallet": wallet, "counterparty": counterparty,
            "timestamp": CUTOFF - 86400, "conditionId": "100", "usdcSize": 3.,
            "price": .05, "side": "BUY", "outcome": "Yes", "eventSlug": "",
            "is_maker": False, "year_month": MONTH, "emitter": a,
        }
        value.update(changes)
        return value

    original = row(a, "alice")
    return [
        original,
        row("alice", a, price=.95, side="SELL", is_maker=True),
        dict(original),  # Same economic values, separate original physical ordinal.
        row(c, d, price=.95),
        row(a.upper(), None, price=.95, is_maker=True),
        row(" " + a, "alice"),
        row(a + " ", "alice", price=.95),
        row(None, ""),
        row("", None, price=.95, is_maker=True),
        row(timestamp=CUTOFF, price=.5),
        row("alice", b, timestamp=CUTOFF - 1, price=.1),
        row("alice", c, timestamp=CUTOFF - 2, conditionId="300", price=.95),
        row(conditionId="200", price=.95),  # L=1 day and R=1 day exactly.
        row(conditionId="200", timestamp=CUTOFF - 86399),
        row(d, "alice", price=1.2),  # Matched source row, absent from accepted BASE.
        row(b, "alice", outcome="wrong"),
        row(conditionId="400", price=.95),  # Invalid duration, valid baseline.
        row(conditionId="unknown"),
        row(timestamp=None),
        row(side="invalid"),
        row("alice", b, timestamp=CUTOFF, price=.95),
    ]


def exact_match(value, addresses=FROZEN_EXCHANGES):
    return value is not None and value.lower() in addresses


def metadata_rows():
    conditions = (("a", "100", "101"), ("b", "200", "201"),
                  ("c", "300", "301"), ("d", "400", "401"))
    return {
        "token_map": [{"token_id": token, "condition_id": "0x" + market,
                       "outcome": label}
                      for market, yes, no in conditions
                      for token, label in ((yes, "Yes"), (no, "No"))],
        "spine": [{"token_id": token, "market_id": "0x" + market,
                   "winning_outcome": "Yes"}
                  for market, yes, no in conditions for token in (yes, no)],
        "native": [{"condition_id": "0x" + market, "n_outcomes": 2,
                    "created_at": None if market == "c" else "2026-03-24T00:00:00Z",
                    "end_date": {"a": "2026-03-26T00:00:00Z",
                                 "b": "2026-03-25T00:00:00Z", "c": None,
                                 "d": "2026-03-23T00:00:00Z"}[market],
                    "event_slug": "event-" + market}
                   for market, _, _ in conditions],
        "categories": [{"mkt": "0x" + market, "prim": "Sports"}
                       for market, _, _ in conditions],
    }


def independent_base(rows):
    """Construct accepted rows directly in Python, not through the audit SQL."""
    values = []
    token_codes = {str(100 * market + outcome): 2 * (market - 1) + outcome
                   for market in range(1, 5) for outcome in range(2)}
    for ordinal, row in enumerate(rows):
        stamp, price, token = row["timestamp"], row["price"], row["conditionId"]
        if (stamp is None or stamp < 0 or row["is_maker"] is None or
                row["side"] not in ("BUY", "SELL") or
                price is None or not math.isfinite(price) or not 0 < price < 1 or
                not math.isfinite(row["usdcSize"]) or row["usdcSize"] <= 0 or
                token not in token_codes or row["outcome"] != "Yes"):
            continue
        recorded_code = token_codes[token]
        code = recorded_code ^ 1 if row["side"] == "SELL" else recorded_code
        normal_price = 1 - price if row["side"] == "SELL" else price
        payout = 1 - recorded_code % 2
        if row["side"] == "SELL":
            payout = 1 - payout
        endpoint = {"100": CUTOFF + 86400, "200": CUTOFF,
                    "300": None, "400": CUTOFF - 172800}[token]
        opening = None if token == "300" else CUTOFF - 86400
        duration = (opening is not None and endpoint is not None and
                    endpoint > opening and opening <= stamp <= endpoint)
        values.append(dict(zip(BASE_FIELDS, (
            ordinal, MONTH, code, recorded_code, stamp, price, normal_price,
            payout, row["usdcSize"], row["side"], row["is_maker"], duration,
        ))))
    return values


class IndependentExchangeFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kaushik-exchange-independent-")
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        paths = {}
        for name, values in metadata_rows().items():
            paths[name] = str(self.folder / (name + ".parquet"))
            pq.write_table(pa.Table.from_pylist(values), paths[name])
        inputs.prepare_metadata(self.con, paths, sample_scope="all_available_dates")
        self.rows = source_rows()
        self.raw = self.folder / "raw.parquet"
        self.base = self.folder / "base.parquet"
        pq.write_table(pa.Table.from_pylist(self.rows, schema=RAW_SCHEMA), self.raw,
                       row_group_size=4)
        self.base_rows = independent_base(self.rows)
        pq.write_table(pa.Table.from_pylist(self.base_rows, schema=BASE_SCHEMA), self.base)
        self.mask_number = 0

    def mask(self):
        audit.create_source_view(self.con, self.raw, MONTH)
        self.mask_number += 1
        path = self.folder / f"mask_{self.mask_number}.parquet"
        self.con.execute(f"COPY ({audit.source_mask_sql(MONTH)}) TO {inputs.literal(path)} "
                         "(FORMAT PARQUET)")
        return path

    def expected_counts(self, rows, scope, part, primary=False):
        result = Counter({name: 0 for name in audit.METRICS})
        for row in rows:
            source = self.rows[row["source_ordinal"]]
            removed = exact_match(source["proxyWallet"]) or exact_match(source["counterparty"])
            if ((scope == "cutoff" and row["timestamp"] >= CUTOFF) or
                    (part != "original" and removed != (part == "removed")) or
                    (primary and row["is_maker"])):
                continue
            result["baseline"] += 1
            tail = row["P"] < .1 or row["P"] >= .9
            result["tails"] += int(tail)
            duration = tail and row["duration_eligible"]
            result["common_duration"] += int(duration)
            endpoint = {"100": CUTOFF + 86400, "200": CUTOFF,
                        "300": None, "400": CUTOFF - 172800}[source["conditionId"]]
            opening = None if source["conditionId"] == "300" else CUTOFF - 86400
            result["lifespan_gt1"] += int(duration and (endpoint - opening) / 86400 > 1)
            result["remaining_gt1"] += int(duration and (endpoint - row["timestamp"]) / 86400 > 1)
        return dict(result)

    def test_literal_lowercase_or_preserves_expanded_rows_and_physical_multiplicity(self):
        self.assertEqual(tuple(audit.EXCHANGES), FROZEN_EXCHANGES)
        self.assertEqual(tuple(audit.DIAGNOSTIC_EXCHANGES), (V3,))
        self.assertEqual(tuple(audit.FIELDS), BASE_FIELDS)
        mask = self.mask()
        actual = pq.read_table(mask).to_pylist()
        expected = []
        for ordinal, row in enumerate(self.rows):
            proxy = row["proxyWallet"].lower() if exact_match(row["proxyWallet"]) else None
            other = row["counterparty"].lower() if exact_match(row["counterparty"]) else None
            if proxy is not None or other is not None:
                expected.append(dict(zip(audit.MASK_FIELDS, (
                    MONTH, ordinal, proxy, other, proxy is not None, other is not None,
                    True, False,
                ))))
        self.assertEqual(sorted(actual, key=lambda row: row["source_ordinal"]), expected)
        self.assertEqual([row["source_ordinal"] for row in expected], [0, 1, 2, 3, 4, 10, 11, 14, 15, 20])
        # Both expanded representations, and an economically identical third row,
        # survive independently in the mask.  Emitter-only and whitespace fail.
        self.assertEqual(audit.validate_mask(self.con, mask, MONTH, len(self.rows)), 10)
        audit.compare_source_mask(self.con, mask, MONTH)
        self.assertTrue(pq.ParquetFile(mask).schema_arrow.equals(audit.MASK_SCHEMA))
        self.assertFalse(any(value in ("alice", "bob") for row in actual for value in row.values()))

    def test_python_counts_match_all_roles_primary_cutoff_and_duration_boundaries(self):
        source_bindings = {path.name: inputs.sha256(path) for path in self.folder.glob("*.parquet")}
        mask = self.mask()
        result = audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows))
        self.assertEqual(len(self.base_rows), 16)
        self.assertEqual(result["mask_rows"], 10)  # Includes two already-invalid source records.
        self.assertEqual(result["EXCEPT_ALL_differences"], [0, 0])
        for scope in ("full", "cutoff"):
            for part in ("original", "retained", "removed"):
                expected = self.expected_counts(self.base_rows, scope, part)
                self.assertEqual({metric: result["counts"][scope][metric][part]
                                  for metric in audit.METRICS}, expected)
                primary = {metric: sum(group[f"{scope}_{metric}"] for group in result["eligible_groups"]
                                       if not group["is_maker"] and (part == "original" or
                                          group["removed"] == (part == "removed")))
                           for metric in audit.METRICS}
                self.assertEqual(primary, self.expected_counts(self.base_rows, scope, part, primary=True))
                for metric in audit.METRICS:
                    roles = audit.role_counts(result["eligible_groups"], scope, metric, part)
                    self.assertEqual(roles["all_roles"], expected[metric])
                    self.assertEqual(roles["primary_taker"], primary[metric])
                    self.assertEqual(roles["all_buy"], roles["maker_buy"] + roles["taker_buy"])
        self.assertEqual(result["counts"]["full"]["baseline"],
                         {"original": 16, "retained": 8, "removed": 8})
        self.assertEqual(result["counts"]["cutoff"]["baseline"],
                         {"original": 14, "retained": 7, "removed": 7})
        # Exactly L=1 and R=1 remain in the common duration sample; neither is >1.
        for part in ("original", "retained"):
            count = result["counts"]["full"]["common_duration"][part]
            self.assertGreater(count, result["counts"]["full"]["lifespan_gt1"][part])
            self.assertGreater(count, result["counts"]["full"]["remaining_gt1"][part])
        self.assertEqual(source_bindings,
                          {path.name: inputs.sha256(path) for path in self.folder.glob("*.parquet")
                          if not path.name.startswith("mask_")})

    def test_address_field_counts_do_not_replace_distinct_removed_record_counts(self):
        mask = self.mask()
        result = audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows))
        raw = audit.raw_match_counts(self.con, MONTH)
        for scope in ("full", "cutoff"):
            for accepted in (False, True):
                expected = Counter()
                ordinals = ({row["source_ordinal"] for row in self.base_rows}
                            if accepted else range(len(self.rows)))
                for ordinal in ordinals:
                    row = self.rows[ordinal]
                    stamp = row["timestamp"]
                    if scope == "cutoff" and (stamp is None or stamp < 0 or stamp >= CUTOFF):
                        continue
                    for field in ("proxyWallet", "counterparty"):
                        if exact_match(row[field]):
                            side = row["side"] if row["side"] in ("BUY", "SELL") else "invalid_or_missing"
                            expected[(side, row["is_maker"], row[field].lower(), field)] += 1
                actual = audit.address_field_counts(result["eligible_groups"] if accepted else raw,
                            f"{scope}_baseline" if accepted else f"{scope}_rows")
                actual = Counter({(row["side"], row["is_maker"], row["address"], row["field"]): row["rows"]
                                  for row in actual if row["rows"]})
                self.assertEqual(actual, expected)
        hits = sum(row["rows"] for row in audit.address_field_counts(result["eligible_groups"], "full_baseline"))
        self.assertEqual(hits, 9)
        self.assertEqual(result["counts"]["full"]["baseline"]["removed"], 8)

    def test_noneligible_v3_is_diagnostic_but_eligible_v3_blocks_admission(self):
        invalid = dict(self.rows[0], proxyWallet=V3, price=2.)
        self.rows.append(invalid)
        self.raw = self.folder / "raw_v3_invalid.parquet"
        pq.write_table(pa.Table.from_pylist(self.rows, schema=RAW_SCHEMA), self.raw)
        mask = self.mask()
        result = audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows))
        self.assertEqual(result["unsupported_eligible_rows"], 0)
        self.assertEqual(result["mask_rows"], 11)
        self.assertEqual(result["counts"]["full"]["baseline"]["removed"], 8)
        self.rows.append(dict(self.rows[0], proxyWallet=V3))
        self.raw = self.folder / "raw_v3_eligible.parquet"
        pq.write_table(pa.Table.from_pylist(self.rows, schema=RAW_SCHEMA), self.raw)
        self.base_rows = independent_base(self.rows)
        self.base = self.folder / "base_v3_eligible.parquet"
        pq.write_table(pa.Table.from_pylist(self.base_rows, schema=BASE_SCHEMA), self.base)
        mask = self.mask()
        with self.assertRaisesRegex(inputs.InputBlocked, "diagnostic-only V3"):
            audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows))
        diagnostic = audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows),
                                       fail_on_unsupported=False)
        self.assertEqual(diagnostic["unsupported_eligible_rows"], 1)
        self.assertEqual(diagnostic["counts"]["full"]["baseline"],
                         {"original": 17, "retained": 9, "removed": 8})

    def test_source_mask_records_absent_from_base_reconcile_original_exclusions(self):
        mask = self.mask()
        audit.audit_month(self.con, self.base, mask, MONTH, len(self.rows))
        reasons = {14: "invalid_recorded_price", 15: "outcome_label_disagreement",
                   17: "unadmitted_token_pair", 18: "invalid_timestamp",
                   19: "invalid_archive_action"}
        schema = pa.schema([("source_ordinal", pa.int64()), ("eligibility_reason", pa.string()),
                            ("side", pa.string()), ("is_maker", pa.bool_())])
        rows = [{"source_ordinal": ordinal, "eligibility_reason": reason,
                 "side": self.rows[ordinal]["side"], "is_maker": self.rows[ordinal]["is_maker"]}
                for ordinal, reason in reasons.items()]
        path = self.folder / "exclusions.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        result = audit.exclusion_intersection(self.con, path, len(self.rows))
        self.assertEqual(result["original_exclusion_rows"], 5)
        self.assertEqual(result["matched_noneligible_rows"], 2)
        self.assertEqual({(row["eligibility_reason"], row["side"], row["is_maker"]): row["rows"]
                          for row in result["first_failure_counts"]},
                         {("invalid_recorded_price", "BUY", False): 1,
                          ("outcome_label_disagreement", "BUY", False): 1})
        mutations = (
            lambda values: values[0].update(eligibility_reason="eligible"),
            lambda values: values[0].update(source_ordinal=0),
            lambda values: values.pop(),
            lambda values: values.append(dict(values[0])),
        )
        for number, mutation in enumerate(mutations):
            changed = [dict(row) for row in rows]
            mutation(changed)
            mutated = self.folder / f"mutated_exclusions_{number}.parquet"
            pq.write_table(pa.Table.from_pylist(changed, schema=schema), mutated)
            with self.assertRaises(inputs.InputBlocked):
                audit.exclusion_intersection(self.con, mutated, len(self.rows))

    def test_mutated_locator_address_flag_and_source_membership_fail_closed(self):
        mask = self.mask()
        original = sorted(pq.read_table(mask).to_pylist(), key=lambda row: row["source_ordinal"])
        mutations = (
            lambda rows: rows.append(dict(rows[0])),
            lambda rows: rows[0].update(source_ordinal=len(self.rows)),
            lambda rows: rows[0].update(source_month="2026-02"),
            lambda rows: rows[0].update(matched_proxy_address="alice"),
            lambda rows: rows[0].update(proxy_match=False),
            lambda rows: rows[0].update(known_exchange_match=False),
        )
        for number, mutation in enumerate(mutations):
            changed = [dict(row) for row in original]
            mutation(changed)
            mutated_mask = self.folder / f"mutated_mask_{number}.parquet"
            pq.write_table(pa.Table.from_pylist(changed, schema=audit.MASK_SCHEMA), mutated_mask)
            with self.subTest(mutation=number), self.assertRaises(inputs.InputBlocked):
                audit.validate_mask(self.con, mutated_mask, MONTH, len(self.rows))
        missing_mask = self.folder / "missing_mask.parquet"
        pq.write_table(pa.Table.from_pylist(original[1:], schema=audit.MASK_SCHEMA), missing_mask)
        audit.validate_mask(self.con, missing_mask, MONTH, len(self.rows))
        with self.assertRaisesRegex(inputs.InputBlocked, "source/mask"):
            audit.compare_source_mask(self.con, missing_mask, MONTH)
        changed = [dict(row) for row in self.base_rows]
        changed.append(dict(changed[0]))
        mutated_base = self.folder / "mutated_base.parquet"
        pq.write_table(pa.Table.from_pylist(changed, schema=BASE_SCHEMA), mutated_base)
        with self.assertRaisesRegex(inputs.InputBlocked, "reference locator"):
            audit.audit_month(self.con, mutated_base, mask, MONTH, len(self.rows))


if __name__ == "__main__":
    unittest.main()
