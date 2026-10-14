"""Small mathematical payload-comparison oracles; never production admission.

These helpers compare synthetic relations only.  They do not change the running
audit or replace its eligibility, clock, source, schema, or resource gates.
"""
from copy import deepcopy
import unittest

import duckdb


FIELDS = (
    "source_ordinal", "source_month", "claim_code", "recorded_token_code",
    "timestamp", "recorded_price", "P", "Y", "usdcSize", "side", "is_maker",
    "duration_eligible",
)
TYPES = ("BIGINT", "VARCHAR", "INTEGER", "INTEGER", "BIGINT", "DOUBLE",
         "DOUBLE", "TINYINT", "DOUBLE", "VARCHAR", "BOOLEAN", "BOOLEAN")
FIELD_SQL = ",".join(FIELDS)
TABLE_SQL = ",".join(f"{name} {kind}" for name, kind in zip(FIELDS, TYPES))


def fixture_rows():
    def row(ordinal, month="2026-03", **changes):
        value = dict(zip(FIELDS, (ordinal, month, 0, 0, 1774396700, .05, .05,
                                  1, 3., "BUY", False, True)))
        value.update(changes)
        return value
    return [row(0), row(1), row(2, is_maker=True),
            row(0, "2026-04", timestamp=1774396900)]


class TraceConnection:
    def __init__(self, con):
        self.con = con
        self.queries = []

    def execute(self, query, *args):
        self.queries.append(query)
        return self.con.execute(query, *args)


def except_queries(original="original", candidate="candidate_union"):
    left = f"SELECT {FIELD_SQL} FROM {original}"
    right = f"SELECT {FIELD_SQL} FROM {candidate}"
    return [f"SELECT count(*) FROM (({a}) EXCEPT ALL ({b}))"
            for a, b in ((left, right), (right, left))]


def join_query(original="original", candidate="candidate_union"):
    mismatch = " OR ".join(f"o.{field} IS DISTINCT FROM c.{field}" for field in FIELDS)
    return f"""SELECT
        count(*) FILTER(WHERE old_present AND (NOT new_present OR payload_diff)) old_minus_candidate,
        count(*) FILTER(WHERE new_present AND (NOT old_present OR payload_diff)) candidate_minus_old
      FROM (SELECT o.source_ordinal IS NOT NULL old_present,
                   c.source_ordinal IS NOT NULL new_present,
                   ({mismatch}) payload_diff
            FROM {original} o FULL OUTER JOIN {candidate} c
            ON o.source_month=c.source_month AND o.source_ordinal=c.source_ordinal) compared"""


def guard_relation(con, relation):
    schema = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    if tuple((row[0], row[1]) for row in schema) != tuple(zip(FIELDS, TYPES)):
        raise ValueError("schema guard")
    n, unique, bad = con.execute(f"""SELECT count(*),
        count(DISTINCT (r.source_month,r.source_ordinal)),
        count(*) FILTER(WHERE r.source_month IS NULL OR r.source_ordinal IS NULL
            OR b.source_rows IS NULL OR r.source_ordinal<0 OR r.source_ordinal>=b.source_rows)
        FROM {relation} r LEFT JOIN source_bounds b USING(source_month)""").fetchone()
    if n != unique or bad:
        raise ValueError("locator guard")
    return n


def guarded_join_differences(con):
    original_n = guard_relation(con, "original")
    candidate_n = guard_relation(con, "candidate_union")
    if candidate_n != original_n:
        raise ValueError("count guard")
    return list(con.execute(join_query()).fetchone())


class PayloadEquivalenceTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.con.execute("SET threads=4")
        self.con.execute("SET disabled_optimizers='join_order,build_side_probe_side'")
        self.con.execute("CREATE TEMP TABLE source_bounds(source_month VARCHAR,source_rows BIGINT)")
        self.con.execute("INSERT INTO source_bounds VALUES ('2026-03',10),('2026-04',10)")
        for relation in ("original", "candidate_retained", "candidate_removed"):
            self.con.execute(f"CREATE TEMP TABLE {relation}({TABLE_SQL})")
        self.con.execute(f"CREATE TEMP VIEW candidate_union AS SELECT {FIELD_SQL} FROM candidate_retained "
                         f"UNION ALL SELECT {FIELD_SQL} FROM candidate_removed")
        self.original = fixture_rows()
        self.install(self.original, self.original)

    def install(self, original, candidate):
        for relation in ("original", "candidate_retained", "candidate_removed"):
            self.con.execute(f"DELETE FROM {relation}")
        for relation, rows in (("original", original), ("candidate_retained", candidate[::2]),
                               ("candidate_removed", candidate[1::2])):
            if rows:
                self.con.executemany(f"INSERT INTO {relation} VALUES ({','.join('?' for _ in FIELDS)})",
                                     [tuple(row[field] for field in FIELDS) for row in rows])

    def oracle(self):
        return [self.con.execute(query).fetchone()[0] for query in except_queries()]

    def test_original_twelve_field_partition_and_repeated_economic_values(self):
        self.assertEqual(guarded_join_differences(self.con), [0, 0])
        self.assertEqual(self.oracle(), [0, 0])
        self.assertEqual(self.original[0]["P"], self.original[1]["P"])
        self.assertNotEqual(self.original[0]["source_ordinal"], self.original[1]["source_ordinal"])
        # Ordinal zero occurs in both months: the locator must include month.
        self.assertEqual(self.original[0]["source_ordinal"], self.original[3]["source_ordinal"])
        trace = TraceConnection(self.con)
        self.assertEqual(guarded_join_differences(trace), [0, 0])
        payload = [query for query in trace.queries if "FULL OUTER JOIN" in query]
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0].count("IS DISTINCT FROM"), 12)
        self.assertNotIn("EXCEPT ALL", payload[0])

    def test_every_payload_field_changed_is_detected_and_matches_set_difference(self):
        changes = dict(zip(FIELDS, (7, "2026-04", 3, 4, 1774396701, .05000000000000001,
                                    .05000000000000001, 0, 3.0000000000000004,
                                    "SELL", True, False)))
        for field, value in changes.items():
            candidate = deepcopy(self.original)
            candidate[1][field] = value
            self.install(self.original, candidate)
            with self.subTest(field=field):
                self.assertEqual(guarded_join_differences(self.con), [1, 1])
                self.assertEqual(self.oracle(), [1, 1])

    def test_missing_extra_and_balanced_key_replacement_are_rejected(self):
        extra = dict(self.original[1], source_ordinal=8)
        for candidate, expected in ((self.original[:-1], [1, 0]),
                                    (self.original + [extra], [0, 1]),
                                    (self.original[:-1] + [extra], [1, 1])):
            self.install(self.original, candidate)
            self.assertEqual(list(self.con.execute(join_query()).fetchone()), expected)
            self.assertEqual(self.oracle(), expected)
            if len(candidate) != len(self.original):
                with self.assertRaisesRegex(ValueError, "count guard"):
                    guarded_join_differences(self.con)
            else:
                self.assertEqual(guarded_join_differences(self.con), expected)

    def test_duplicate_candidate_is_a_counterexample_without_uniqueness_guard(self):
        candidate = self.original + [dict(self.original[0])]
        self.install(self.original, candidate)
        # A bare join sees two equal matches and can falsely report equality.
        self.assertEqual(list(self.con.execute(join_query()).fetchone()), [0, 0])
        self.assertEqual(self.oracle(), [0, 1])
        trace = TraceConnection(self.con)
        with self.assertRaisesRegex(ValueError, "locator guard"):
            guarded_join_differences(trace)
        self.assertFalse(any("FULL OUTER JOIN" in query for query in trace.queries))

    def test_duplicate_original_and_balanced_duplicate_candidate_are_blocked(self):
        for original, candidate in ((self.original + [dict(self.original[0])], self.original),
                                    (self.original, self.original[:-1] + [dict(self.original[0])])):
            self.install(original, candidate)
            with self.assertRaisesRegex(ValueError, "locator guard"):
                guarded_join_differences(self.con)
        changed_duplicate = dict(self.original[0], P=.06)
        self.install(self.original, self.original[:-1] + [changed_duplicate])
        with self.assertRaisesRegex(ValueError, "locator guard"):
            guarded_join_differences(self.con)

    def test_null_negative_out_of_range_unknown_month_and_wrong_schema_block(self):
        for changes in ({"source_ordinal": None}, {"source_ordinal": -1}, {"source_ordinal": 10},
                        {"source_month": None}, {"source_month": "2026-05"}):
            candidate = deepcopy(self.original)
            candidate[1].update(changes)
            self.install(self.original, candidate)
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "locator guard"):
                guarded_join_differences(self.con)
        self.con.execute("CREATE OR REPLACE TEMP VIEW candidate_union AS SELECT * EXCLUDE(Y) FROM candidate_retained")
        with self.assertRaisesRegex(ValueError, "schema guard"):
            guarded_join_differences(self.con)

    def test_sql_null_payload_equality_and_empty_relations_match_oracle(self):
        # This is an equality-semantics test, not admission of null analytic data.
        original = deepcopy(self.original)
        original[1]["P"] = None
        self.install(original, original)
        self.assertEqual(guarded_join_differences(self.con), self.oracle())
        candidate = deepcopy(original)
        candidate[1]["P"] = .05
        self.install(original, candidate)
        self.assertEqual(guarded_join_differences(self.con), [1, 1])
        self.assertEqual(self.oracle(), [1, 1])
        self.install([], [])
        self.assertEqual(guarded_join_differences(self.con), [0, 0])
        self.assertEqual(self.oracle(), [0, 0])


if __name__ == "__main__":
    unittest.main()
