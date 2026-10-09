"""Run only the extracted Stage6 guard and COPY on tiny synthetic inputs."""
from __future__ import annotations

import ast
import hashlib
from pathlib import Path
import tempfile
import unittest

import duckdb

from scripts import audit_polymarket_stage6_runtime as runtime


BUILDER = Path(__file__).resolve().parents[1] / "pipeline/transform/build_trades.py"
FROZEN_COPY_SHA256 = "be2b10e0f338ae9b9adec236d64e56ae4f2a4dbcf401bfaa67628bfed8402d0f"
EXPECTED_ROWS = [
    ("wallet_A", 1772388001, "100000000000000000001", 3.871, 0.98,
     "BUY", "Down", "synthetic_buy_event", True, "wallet_B", "2026-03"),
    ("wallet_B", 1772388001, "100000000000000000001", 3.871, 0.98,
     "SELL", "Down", "synthetic_buy_event", False, "wallet_A", "2026-03"),
    ("wallet_C", 1772388003, "100000000000000000002", 4.0, 0.4,
     "SELL", "Up", "synthetic_sell_event", True, "wallet_D", "2026-03"),
    ("wallet_D", 1772388003, "100000000000000000002", 4.0, 0.4,
     "BUY", "Up", "synthetic_sell_event", False, "wallet_C", "2026-03"),
]


def extracted_source():
    """Do not import the builder: its module body opens logs/output paths."""
    source = BUILDER.read_text()
    tree = ast.parse(source)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    stage6 = functions["run_stage6"]
    templates = [node for node in ast.walk(stage6)
                 if isinstance(node, ast.JoinedStr) and any(
                     isinstance(part, ast.Constant) and isinstance(part.value, str)
                     and "WITH expanded AS" in part.value for part in node.values)]
    if len(templates) != 1:
        raise AssertionError("expected one unchanged Stage6 expansion COPY")
    guard_tree = ast.Module(body=[functions["_guard_stage6_wallet_projection"]], type_ignores=[])
    namespace = {}
    exec(compile(guard_tree, str(BUILDER), "exec"), namespace)
    return source, functions, templates[0], namespace["_guard_stage6_wallet_projection"]


class AbsentOptimizerConnection:
    def __init__(self):
        self.queries = []

    def execute(self, query, parameters=None):
        self.queries.append((query, parameters))
        if query != "SELECT 1 FROM duckdb_optimizers() WHERE name='common_subplan'":
            raise AssertionError("absent optimizer must not read or change settings")
        return self

    def fetchone(self):
        return None


class Stage6WalletProjectionGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source, cls.functions, cls.template, guard = extracted_source()
        cls.guard = staticmethod(guard)

    def test_stage6_copy_remains_exactly_the_frozen_source(self):
        segment = ast.get_source_segment(self.source, self.template)
        self.assertEqual(hashlib.sha256(segment.encode()).hexdigest(), FROZEN_COPY_SHA256)

    def test_guard_preserves_existing_settings_and_is_idempotent(self):
        con = duckdb.connect()
        try:
            con.execute("SET disabled_optimizers='filter_pushdown,join_order'")
            available = con.execute(
                "SELECT 1 FROM duckdb_optimizers() WHERE name='common_subplan'").fetchone()
            self.guard(con)
            first = con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0]
            self.guard(con)
            second = con.execute("SELECT current_setting('disabled_optimizers')").fetchone()[0]
            self.assertEqual(first, second)
            expected = {"filter_pushdown", "join_order"}
            if available:
                expected.add("common_subplan")
            self.assertEqual(set(first.split(",")), expected)
        finally:
            con.close()

    def test_absent_optimizer_returns_without_setting_mutation(self):
        con = AbsentOptimizerConnection()
        self.guard(con)
        self.assertEqual(len(con.queries), 1)

    def test_production_entry_and_optimizer_guards_precede_output_mutation(self):
        stage6 = self.functions["run_stage6"]
        body = stage6.body[1:]  # Skip the docstring; never execute run_stage6.
        self.assertEqual(ast.unparse(body[0]),
                         "sys.path.insert(0, str(Path(__file__).resolve().parents[2]))")
        self.assertIsInstance(body[1], ast.ImportFrom)
        self.assertEqual(body[1].module, "production_guard")
        self.assertEqual([name.name for name in body[1].names], ["require_production_host"])
        self.assertEqual(ast.unparse(body[2]), "require_production_host()")
        calls = [node for node in ast.walk(stage6) if isinstance(node, ast.Call)]
        production = next(node for node in calls
                          if isinstance(node.func, ast.Name) and node.func.id == "require_production_host")
        connection = next(node for node in calls
                          if isinstance(node.func, ast.Name) and node.func.id == "get_con")
        guard = next(node for node in calls
                     if isinstance(node.func, ast.Name) and node.func.id == "_guard_stage6_wallet_projection")
        mutations = [node for node in calls if isinstance(node.func, ast.Attribute)
                     and node.func.attr in {"rmtree", "mkdir", "execute"}]
        self.assertLess(production.lineno, connection.lineno)
        self.assertLess(connection.lineno, guard.lineno)
        self.assertTrue(mutations)
        self.assertTrue(all(guard.lineno < node.lineno for node in mutations))

    def test_optimizer_guard_is_called_only_by_stage6(self):
        callers = [name for name, function in self.functions.items()
                   for node in ast.walk(function)
                   if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id == "_guard_stage6_wallet_projection"]
        self.assertEqual(callers, ["run_stage6"])

    def run_synthetic_copy(self, threads, guarded):
        with tempfile.TemporaryDirectory(prefix="stage6_wallet_guard_") as temporary:
            root = Path(temporary)
            fixture = root / "resolved_synthetic.parquet"
            output = root / "expanded"
            con = duckdb.connect()
            try:
                con.execute("SET memory_limit='64MB'")
                con.execute("SET max_temp_directory_size='0B'")
                con.execute("SET preserve_insertion_order=false")
                con.execute("SET TimeZone='UTC'")
                con.execute(f"SET threads={threads}")
                integers = {"maker_amount_filled", "taker_amount_filled", "fee", "block_number", "log_index"}
                con.execute("CREATE TABLE synthetic_resolved(" + ",".join(
                    field + (" BIGINT" if field in integers else " VARCHAR")
                    for field in runtime.FIXTURE_FIELDS) + ")")
                con.executemany("INSERT INTO synthetic_resolved VALUES (" +
                                ",".join("?" for _ in runtime.FIXTURE_FIELDS) + ")",
                                [tuple(row[field] for field in runtime.FIXTURE_FIELDS)
                                 for row in runtime.FIXTURE_ROWS])
                con.execute("COPY synthetic_resolved TO ? (FORMAT PARQUET,COMPRESSION ZSTD)", [str(fixture)])
                con.execute("CREATE TABLE block_ts(block_number BIGINT,timestamp BIGINT)")
                con.executemany("INSERT INTO block_ts VALUES (?,?)",
                                [(row["block_number"], row["timestamp"]) for row in runtime.TIMESTAMP_ROWS])
                if guarded:
                    self.guard(con)
                sql = runtime.render_copy(self.template, {
                    "usdc_scale": 1000000, "ctf_scale": 1000000,
                    "RESOLVED_TRADES_PATH": fixture, "TRADES_OUTPUT_DIR": output,
                    "ts_expr": f"COALESCE(bt.timestamp, {runtime.APPROX_EXPR})",
                    "ts_join": "LEFT JOIN block_ts bt ON rt.block_number = bt.block_number",
                })
                con.execute(sql)
                cursor = con.execute("SELECT " + ",".join(runtime.VALUE_FIELDS) +
                                     " FROM read_parquet(?) ORDER BY conditionId,is_maker DESC",
                                     [str(output / "**/*.parquet")])
                names = [column[0] for column in cursor.description]
                rows = cursor.fetchall()
                self.assertEqual(names, list(runtime.VALUE_FIELDS))
                self.assertLess(sum(path.stat().st_size for path in root.rglob("*") if path.is_file()),
                                1024**2)
                return rows
            finally:
                con.close()

    def test_actual_guard_corrects_all_eleven_fields_with_cached_join(self):
        for threads in (1, 4):
            with self.subTest(threads=threads, duckdb=duckdb.__version__):
                self.assertEqual(self.run_synthetic_copy(threads, guarded=True), EXPECTED_ROWS)

    @unittest.skipUnless(duckdb.__version__ == "1.5.0", "known default-optimizer regression is specific to 1.5.0")
    def test_unguarded_150_fixture_reproduces_only_wallet_field_errors(self):
        for threads in (1, 4):
            with self.subTest(threads=threads):
                actual = self.run_synthetic_copy(threads, guarded=False)
                self.assertEqual(len(actual), len(EXPECTED_ROWS))
                mismatched_fields = {field for observed, expected in zip(actual, EXPECTED_ROWS)
                                     for field, left, right in zip(runtime.VALUE_FIELDS, observed, expected)
                                     if left != right}
                self.assertEqual(mismatched_fields, {"proxyWallet", "counterparty"})


if __name__ == "__main__":
    unittest.main()
