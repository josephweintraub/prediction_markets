"""Read-only, bounded Polymarket native-to-published lineage audit.

Preflight reads Parquet footers and the small timestamp cache only. Production
body reads require a separately reviewed preflight manifest. Imports and fixture
helpers do not invoke the production guard. No external service is contacted.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BASELINE_COMMIT = "e7ce00d070ae7055de5c23ab51f03b4d6124efef"
OLD_EXCHANGES = (
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
)
NEW_EXCHANGES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
EXCHANGES = OLD_EXCHANGES + NEW_EXCHANGES
RAW_FIELDS = (
    "order_hash", "maker", "taker", "maker_asset_id", "taker_asset_id",
    "maker_amount_filled", "taker_amount_filled", "fee", "block_number",
    "transaction_hash", "log_index", "exchange_address",
)
RESOLVED_FIELDS = RAW_FIELDS + (
    "condition_id", "outcome", "market_slug", "event_slug", "question",
    "outcome_token_side", "winning_outcome",
)
VALUE_FIELDS = (
    "proxyWallet", "timestamp", "conditionId", "usdcSize", "price", "side",
    "outcome", "eventSlug", "is_maker", "counterparty", "year_month",
)
NATIVE_KEY = "lower(exchange_address), transaction_hash, log_index"
MAX_ROWS = 2_000_000
MAX_COMBINED_ROWS = 4 * MAX_ROWS
MAX_COMBINED_UNCOMPRESSED_BYTES = 512 * 1024**2
MAX_OUTPUT_BYTES = 1_073_741_824
WINDOWS = (
    ("pre_cutoff", "2026-03-01T18:00:00Z", "2026-03-01T18:01:00Z"),
    ("post_migration", "2026-06-22T18:00:00Z", "2026-06-22T18:01:00Z"),
)
DEFAULT_INPUTS = {
    "raw": "/mnt/data/pipeline_data/raw_events.parquet",
    "resolved": "/mnt/data/pipeline_data/resolved_trades.parquet",
    "root_transformed": "/mnt/data/pipeline_root_output/trades.parquet",
    "clean": "/mnt/data/pipeline_output/trades_clean.parquet",
    "timestamps": "/mnt/data/pipeline_data/block_timestamps.parquet",
    "token_map": "/mnt/data/pipeline_data/token_map.parquet",
    "resolutions": "/mnt/data/pipeline_data/resolved_markets.parquet",
}
FROZEN_SOURCES = (
    "pipeline/config.py", "pipeline/extraction/dedup.py",
    "pipeline/extraction/extract_orderfilled.py", "pipeline/extraction/extract_orderfilled_v2.py",
    "pipeline/transform/build_trades.py", "scripts/build_clean.py", "scripts/resort_clean.py",
)


class AuditBlocked(ValueError):
    """The supplied inputs cannot support the stated bounded contract."""


def epoch(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def dependencies() -> None:
    global duckdb, pa, pc, pq
    import duckdb
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq


def connection(memory_limit: str = "8GB"):
    dependencies()
    con = duckdb.connect(":memory:")
    for statement in (
        "SET threads=4", f"SET memory_limit='{memory_limit}'", "SET temp_directory=''",
        "SET max_temp_directory_size='0B'", "SET enable_object_cache=false",
        "SET preserve_insertion_order=false", "SET TimeZone='UTC'",
    ):
        con.execute(statement)
    return con


def qname(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def scalar(con, sql: str) -> dict[str, Any]:
    cursor = con.execute(sql)
    values = cursor.fetchone()
    return dict(zip((item[0] for item in cursor.description), values))


def required_types(name: str) -> dict[str, str]:
    integer = {"maker_amount_filled", "taker_amount_filled", "fee", "block_number", "log_index"}
    if name in {"raw", "resolved"}:
        return {field: "int64" if field in integer else "string"
                for field in (RAW_FIELDS if name == "raw" else RESOLVED_FIELDS)}
    if name in {"root_transformed", "clean"}:
        return {field: ("int64" if field == "timestamp" else
                        "double" if field in {"usdcSize", "price"} else
                        "bool" if field == "is_maker" else "string")
                for field in VALUE_FIELDS if field != "year_month" or name == "clean"}
    if name == "timestamps":
        return {"block_number": "int64", "timestamp": "int64"}
    if name == "token_map":
        return {field: "string" for field in
                ("token_id", "condition_id", "outcome", "market_slug", "event_slug", "question")}
    return {field: "string" for field in ("token_id", "condition_id", "winning_outcome")}


def footer_info(path: Path, name: str) -> dict[str, Any]:
    """Read no row body; freeze the exact footer and filesystem identity."""
    dependencies()
    before = path.stat()
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    for field, kind in required_types(name).items():
        if field not in schema.names:
            raise AuditBlocked(f"{name}: missing required schema field {field}")
        actual = schema.field(field).type
        valid = ((pa.types.is_string(actual) or pa.types.is_large_string(actual))
                 if kind == "string" else str(actual) == kind)
        if not valid:
            raise AuditBlocked(f"{name}: schema type mismatch for {field}: {actual}")
    with path.open("rb") as stream:
        stream.seek(-8, 2)
        trailer = stream.read(8)
        length, magic = struct.unpack("<I4s", trailer)
        if magic != b"PAR1" or length + 8 > before.st_size:
            raise AuditBlocked(f"{name}: invalid Parquet footer")
        stream.seek(-(length + 8), 2)
        digest = hashlib.sha256(stream.read(length) + trailer).hexdigest()
    groups = []
    for index in range(pf.metadata.num_row_groups):
        group = pf.metadata.row_group(index)
        stats = {}
        for col_index in range(group.num_columns):
            col = group.column(col_index)
            if col.path_in_schema not in {"block_number", "timestamp"}:
                continue
            stat = col.statistics
            stats[col.path_in_schema] = (
                {"min": stat.min, "max": stat.max, "null_count": stat.null_count}
                if stat is not None and stat.has_min_max else None)
        groups.append({"index": index, "rows": group.num_rows,
                       "uncompressed_bytes": group.total_byte_size,
                       "compressed_bytes": sum(group.column(i).total_compressed_size
                                               for i in range(group.num_columns)),
                       "stats": stats})
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise AuditBlocked(f"{name}: source changed during footer read")
    return {"path": str(path), "bytes": before.st_size, "mtime_ns": before.st_mtime_ns,
            "rows": pf.metadata.num_rows, "schema": str(schema), "footer_sha256": digest,
            "source_content_hash": "not_computed_read_only_bounded_audit",
            "row_groups": groups}


def select_groups(inventories: list[dict], column: str, lower: int,
                  upper_exclusive: int, cap: int = MAX_ROWS) -> dict:
    """Select every overlapping group, including both complete boundaries."""
    chosen = []
    for info in inventories:
        for group in info["row_groups"]:
            stats = group["stats"].get(column)
            if (stats is None or stats.get("null_count") != 0 or
                    stats["min"] is None or stats["max"] is None):
                raise AuditBlocked(f"{column}: missing or null row-group boundary statistics")
            if stats["min"] > stats["max"]:
                raise AuditBlocked(f"{column}: reversed row-group statistics")
            if stats["min"] < upper_exclusive and stats["max"] >= lower:
                chosen.append({"path": info["path"], **group})
    rows = sum(group["rows"] for group in chosen)
    result = {"column": column, "lower_inclusive": lower,
              "upper_exclusive": upper_exclusive, "groups": chosen,
              "selected_rows": rows,
              "selected_compressed_bytes": sum(group["compressed_bytes"] for group in chosen),
              "selected_uncompressed_bytes": sum(group["uncompressed_bytes"] for group in chosen)}
    if rows > cap:
        raise AuditBlocked(f"complete row-group selection requires {rows} rows; cap={cap}")
    return result


def timestamp_fences(con, path: str, start: int, end: int) -> dict:
    """Exact cached blocks bracket all seconds of the immutable UTC window."""
    result = scalar(con, f"""SELECT
        max(block_number) FILTER(WHERE timestamp < {start}) lower_fence_block,
        min(block_number) FILTER(WHERE timestamp >= {end}) upper_fence_block,
        min(block_number) FILTER(WHERE timestamp >= {start} AND timestamp < {end}) first_window_block,
        max(block_number) FILTER(WHERE timestamp >= {start} AND timestamp < {end}) last_window_block
        FROM read_parquet({literal(path)})""")
    if any(value is None for value in result.values()):
        raise AuditBlocked("timestamp cache cannot bracket complete declared window")
    low, high = result["lower_fence_block"], result["upper_fence_block"]
    if not low < result["first_window_block"] <= result["last_window_block"] < high:
        raise AuditBlocked("timestamp cache has nonmonotonic or ambiguous window fences")
    check = scalar(con, f"""WITH b AS (
        SELECT block_number,count(*) n,count(DISTINCT timestamp) nt,
               min(timestamp) ts FROM read_parquet({literal(path)})
        WHERE block_number BETWEEN {low} AND {high} GROUP BY 1),
        ordered AS (SELECT *,lag(ts) OVER(ORDER BY block_number) prev FROM b)
        SELECT count(*) cache_blocks, count(*) FILTER(WHERE n<>1 OR nt<>1) ambiguous_blocks,
          count(*) FILTER(WHERE ts IS NULL OR ts < prev) invalid_clock,
          min(ts) minimum_timestamp,max(ts) maximum_timestamp FROM ordered""")
    if check["ambiguous_blocks"] or check["invalid_clock"]:
        raise AuditBlocked("timestamp cache has conflicting/null/nonmonotonic window keys")
    if check["minimum_timestamp"] >= start or check["maximum_timestamp"] < end:
        raise AuditBlocked("timestamp cache fence seconds do not bracket the declared window")
    return {**result, **check, "start_timestamp": start, "end_timestamp_exclusive": end,
            "all_integer_blocks_cached": check["cache_blocks"] == high - low + 1}


def inventories_for(path: str, name: str, month: str | None = None) -> list[dict]:
    location = Path(path)
    if location.is_file():
        files = [location]
    elif location.is_dir() and month:
        files = sorted((location / f"year_month={month}").glob("*.parquet"))
    else:
        raise AuditBlocked(f"{name}: missing expected input/partition layout")
    if not files:
        raise AuditBlocked(f"{name}: missing declared month partition")
    return [footer_info(file, name) for file in files]


def preflight(inputs: dict[str, str], con) -> dict:
    output = {"schema_version": 1, "status": "preflight_complete", "baseline_commit": BASELINE_COMMIT,
              "environment": {"python": sys.version, "platform": sys.platform,
                              "duckdb": duckdb.__version__, "pyarrow": pa.__version__},
              "caps": {"rows_per_relation_window": MAX_ROWS, "combined_rows_per_window": MAX_COMBINED_ROWS,
                       "combined_uncompressed_bytes_per_window": MAX_COMBINED_UNCOMPRESSED_BYTES,
                       "preflight_duckdb_memory_limit": "8GB", "body_duckdb_memory_limit": "4GB",
                       "arrow_memory_note": "Arrow is outside DuckDB accounting; full selected footer bytes are conservatively capped before body reads.",
                       "threads": 4, "disk_spill": "0B",
                       "max_output_bytes": MAX_OUTPUT_BYTES},
              "inputs": inputs, "inventories": {}, "windows": [], "blocks": []}
    fixed = {}
    for name in ("raw", "resolved", "timestamps", "token_map", "resolutions"):
        fixed[name] = inventories_for(inputs[name], name)
        output["inventories"][name] = fixed[name]
    for name, begin, finish in WINDOWS:
        window = {"name": name, "start_utc": begin, "end_utc_exclusive": finish,
                  "selections": {}, "status": "preflight_complete"}
        output["windows"].append(window)
        try:
            start, end = epoch(begin), epoch(finish)
            fences = timestamp_fences(con, inputs["timestamps"], start, end)
            window["fences"] = fences
            month = begin[:7]
            for relation in ("raw", "resolved", "root_transformed", "clean"):
                if relation in fixed:
                    infos = fixed[relation]
                    column, lower, upper = "block_number", fences["lower_fence_block"], fences["upper_fence_block"] + 1
                else:
                    key = f"{relation}/{month}"
                    infos = output["inventories"].setdefault(key, inventories_for(inputs[relation], relation, month))
                    column, lower, upper = "timestamp", start, end
                window["selections"][relation] = select_groups(infos, column, lower, upper)
            combined = sum(item["selected_rows"] for item in window["selections"].values())
            window["combined_selected_rows"] = combined
            if combined > MAX_COMBINED_ROWS:
                raise AuditBlocked(f"combined selected rows {combined} exceed {MAX_COMBINED_ROWS}")
            combined_bytes = sum(item["selected_uncompressed_bytes"] for item in window["selections"].values())
            window["combined_selected_uncompressed_bytes"] = combined_bytes
            if combined_bytes > MAX_COMBINED_UNCOMPRESSED_BYTES:
                raise AuditBlocked(f"combined selected uncompressed bytes {combined_bytes} exceed {MAX_COMBINED_UNCOMPRESSED_BYTES}")
        except AuditBlocked as error:
            window["status"] = "blocked"
            window["reason"] = str(error)
            output["blocks"].append({"window": name, "reason": str(error)})
            output["status"] = "preflight_blocked"
    return output


def read_selection(selection: dict, fields: tuple[str, ...]) -> pa.Table:
    """Never use a relation-wide scan; every selected body group is explicit."""
    dependencies()
    tables = []
    for group in selection["groups"]:
        pf = pq.ParquetFile(group["path"])
        physical = [field for field in fields if field in pf.schema_arrow.names]
        table = pf.read_row_group(group["index"], columns=physical)
        if table.num_rows != group["rows"]:
            raise AuditBlocked("selected row-group count changed after preflight")
        if "year_month" in fields and "year_month" not in table.column_names:
            parts = [part.split("=", 1)[1] for part in Path(group["path"]).parts
                     if part.startswith("year_month=")]
            if len(parts) != 1:
                raise AuditBlocked("missing physical and unique Hive year_month")
            table = table.append_column("year_month", pa.array([parts[0]] * table.num_rows))
        mask = pc.and_(pc.greater_equal(table[selection["column"]], selection["lower_inclusive"]),
                       pc.less(table[selection["column"]], selection["upper_exclusive"]))
        tables.append(table.filter(mask).select(list(fields)))
    if not tables:
        raise AuditBlocked("declared window has no selected groups")
    return pa.concat_tables(tables)


def native_views(con, relation: str = "raw") -> None:
    values = ",".join(qname(field) for field in RAW_FIELDS)
    addresses = ",".join(literal(address) for address in EXCHANGES)
    old = ",".join(literal(address) for address in OLD_EXCHANGES)
    con.execute(f"""CREATE OR REPLACE VIEW native_groups AS SELECT {NATIVE_KEY},
        count(*) source_rows,count(DISTINCT ({values})) payloads
        FROM {relation} GROUP BY 1,2,3""")
    con.execute(f"""CREATE OR REPLACE VIEW unique_native AS SELECT DISTINCT {values}
        FROM {relation}""")
    con.execute(f"""CREATE OR REPLACE VIEW roles AS SELECT *,
        lower(exchange_address) IN ({addresses}) known_exchange,
        lower(taker)=lower(exchange_address) exchange_facing,
        lower(taker) IN ({addresses}) taker_is_any_exchange,
        CASE WHEN maker_asset_id='0' THEN maker_amount_filled
             WHEN taker_asset_id='0' THEN taker_amount_filled END collateral_micro
        FROM unique_native""")
    # Preserve the actual canonical filter's case-sensitive taker comparison.
    con.execute(f"""CREATE OR REPLACE VIEW canonical_ranked AS SELECT *,
        row_number() OVER(PARTITION BY transaction_hash,order_hash ORDER BY log_index) rn
        FROM {relation} WHERE taker NOT IN ({old})""")
    con.execute("CREATE OR REPLACE VIEW canonical_retained AS SELECT * EXCLUDE(rn) FROM canonical_ranked WHERE rn=1")


def native_summary(con, relation: str = "raw") -> dict:
    native_views(con, relation)
    identity = scalar(con, """SELECT count(*) native_keys,
        coalesce(sum(source_rows-1) FILTER(WHERE payloads=1),0) identical_replay_surplus,
        count(*) FILTER(WHERE payloads>1) conflicting_native_keys
        FROM native_groups""")
    integrity = scalar(con, f"""SELECT count(*) observed_source_rows,
        count(*) FILTER(WHERE transaction_hash IS NULL OR trim(transaction_hash)=''
          OR order_hash IS NULL OR trim(order_hash)='' OR log_index IS NULL OR log_index<0
          OR exchange_address IS NULL OR trim(exchange_address)='') invalid_native_keys,
        count(*) FILTER(WHERE maker IS NULL OR taker IS NULL OR maker_asset_id IS NULL
          OR taker_asset_id IS NULL OR block_number IS NULL
          OR maker_amount_filled<=0 OR taker_amount_filled<=0
          OR maker_amount_filled IS NULL OR taker_amount_filled IS NULL) invalid_payload,
        count(*) FILTER(WHERE (maker_asset_id='0')=(taker_asset_id='0')) invalid_asset_xor
        FROM {relation}""")
    roles = scalar(con, """SELECT count(*) distinct_payload_rows,
        count(*) FILTER(WHERE NOT known_exchange) unknown_exchange_rows,
        count(*) FILTER(WHERE exchange_facing) exchange_facing_rows,
        count(*) FILTER(WHERE NOT exchange_facing) nonaggregate_candidate_rows,
        count(*) FILTER(WHERE taker_is_any_exchange AND NOT exchange_facing) other_exchange_taker_rows,
        coalesce(sum(collateral_micro::HUGEINT),0) all_native_recorded_collateral_micro,
        coalesce(sum(collateral_micro::HUGEINT) FILTER(WHERE exchange_facing),0) aggregate_recorded_collateral_micro,
        coalesce(sum(collateral_micro::HUGEINT) FILTER(WHERE NOT exchange_facing),0) nonaggregate_recorded_collateral_micro
        FROM roles""")
    repeated = scalar(con, """SELECT count(*) repeated_tx_order_candidate_groups,
        coalesce(sum(n),0) candidate_logs_in_repeated_groups,
        coalesce(sum(n-1),0) candidate_logs_beyond_first
        FROM (SELECT transaction_hash,order_hash,count(*) n FROM roles
          WHERE known_exchange AND NOT taker_is_any_exchange GROUP BY 1,2 HAVING count(*)>1)""")
    retention = scalar(con, """SELECT count(*) canonical_retained_native_rows FROM canonical_retained""")
    return {"identity": identity, "integrity": integrity, "roles": roles,
            "repeated_order_candidates": repeated, "canonical_retention": retention,
            "denominator_note": "Collateral sums count native records. Aggregates and maker legs overlap; these are not unique economic volume or wallet flows.",
            "repeated_order_note": "Distinct nonaggregate native logs sharing transaction/order are candidates requiring full batch evidence; no genuine-fill assertion is made."}


def create_expected_resolved(con) -> None:
    con.execute("""CREATE OR REPLACE VIEW expected_mapped AS SELECT e.*,
        coalesce(t1.condition_id,t2.condition_id) condition_id,
        coalesce(t1.outcome,t2.outcome) outcome,
        coalesce(t1.market_slug,t2.market_slug) market_slug,
        coalesce(t1.event_slug,t2.event_slug) event_slug,
        coalesce(t1.question,t2.question) question,
        CASE WHEN t1.token_id IS NOT NULL THEN 'maker'
             WHEN t2.token_id IS NOT NULL THEN 'taker' END outcome_token_side
        FROM canonical_retained e LEFT JOIN token_map t1 ON e.maker_asset_id=t1.token_id
        LEFT JOIN token_map t2 ON e.taker_asset_id=t2.token_id""")
    con.execute("""CREATE OR REPLACE VIEW expected_resolved AS SELECT m.*,r.winning_outcome
        FROM expected_mapped m INNER JOIN resolutions r
          ON CASE WHEN m.outcome_token_side='maker' THEN m.maker_asset_id
                  WHEN m.outcome_token_side='taker' THEN m.taker_asset_id END=r.token_id
        WHERE m.condition_id IS NOT NULL""")


def touched_metadata_summary(con) -> dict:
    """Reproducible joins alone do not establish valid market/winner metadata."""
    return scalar(con, """SELECT
        (SELECT count(*) FROM token_map m JOIN touched_tokens t USING(token_id)
          WHERE m.token_id IS NULL OR trim(m.token_id)='' OR m.condition_id IS NULL
            OR trim(m.condition_id)='' OR m.outcome IS NULL OR trim(m.outcome)='') invalid_touched_token_metadata,
        (SELECT count(*) FROM resolutions r JOIN touched_tokens t USING(token_id)
          WHERE r.token_id IS NULL OR trim(r.token_id)='' OR r.condition_id IS NULL
            OR trim(r.condition_id)='' OR r.winning_outcome IS NULL OR trim(r.winning_outcome)='') invalid_touched_resolution_metadata,
        (SELECT count(*) FROM resolutions r JOIN touched_tokens t USING(token_id)
          LEFT JOIN token_map m USING(token_id)
          WHERE m.token_id IS NULL OR r.condition_id IS DISTINCT FROM m.condition_id) resolution_market_conflicts,
        (SELECT count(*) FROM resolutions r JOIN touched_tokens t USING(token_id)
          WHERE NOT EXISTS(SELECT 1 FROM token_map m WHERE m.condition_id=r.condition_id
                           AND m.outcome=r.winning_outcome)) winner_outside_market_outcome_universe""")


def create_expansion(con, relation: str, output: str) -> None:
    con.execute(f"""CREATE OR REPLACE VIEW {output} AS WITH base AS (
        SELECT r.*,t.timestamp exact_timestamp,
          CASE WHEN outcome_token_side='maker' THEN maker_asset_id ELSE taker_asset_id END token,
          CASE WHEN outcome_token_side='maker' THEN taker_amount_filled/1000000.0
               ELSE maker_amount_filled/1000000.0 END cash,
          CASE WHEN outcome_token_side='maker'
            THEN (taker_amount_filled/1000000.0)/nullif(maker_amount_filled/1000000.0,0)
            ELSE (maker_amount_filled/1000000.0)/nullif(taker_amount_filled/1000000.0,0) END execution_price
        FROM {relation} r LEFT JOIN timestamp_slice t USING(block_number)),
        expanded AS (SELECT *,maker proxyWallet,taker counterparty,TRUE is_maker,
          CASE WHEN outcome_token_side='maker' THEN 'SELL' ELSE 'BUY' END side FROM base
        UNION ALL SELECT *,taker proxyWallet,maker counterparty,FALSE is_maker,
          CASE WHEN outcome_token_side='maker' THEN 'BUY' ELSE 'SELL' END side FROM base)
        SELECT proxyWallet,exact_timestamp AS timestamp,token conditionId,cash usdcSize,
          execution_price price,side,outcome,event_slug eventSlug,is_maker,counterparty,
          strftime(to_timestamp(exact_timestamp),'%Y-%m') year_month,
          exchange_address,transaction_hash,log_index
        FROM expanded WHERE execution_price>0 AND execution_price<=1""")


def value_summary(con, relation: str, identities: bool = False) -> dict:
    fields = ",".join(qname(field) for field in VALUE_FIELDS)
    result = scalar(con, f"SELECT count(*) row_count,coalesce(sum(usdcSize),0) expanded_recorded_cash FROM {relation}")
    grouped = scalar(con, f"""SELECT count(*) value_groups,coalesce(sum(n-1),0) value_surplus,
        count(*) FILTER(WHERE n>1) repeated_value_groups FROM (
        SELECT {fields},count(*) n FROM {relation} GROUP BY {fields})""")
    if identities:
        grouped.update(scalar(con, f"""SELECT
            coalesce(sum(n-ids),0) same_native_role_value_surplus,
            coalesce(sum(ids-1),0) distinct_native_role_value_surplus,
            count(*) FILTER(WHERE ids>1) equal_value_distinct_native_groups
            FROM (SELECT {fields},count(*) n,
                count(DISTINCT (lower(exchange_address),transaction_hash,log_index,is_maker)) ids
                FROM {relation} GROUP BY {fields})"""))
    return {**result, **grouped,
            "cash_note": "Expanded maker/counterparty record cash; not unique execution dollars."}


def multiplicity_difference(con, left: str, right: str, fields: tuple[str, ...]) -> dict:
    """EXCEPT ALL compares payload multiplicities, not merely key membership."""
    selected = ",".join(qname(field) for field in fields)
    return scalar(con, f"""SELECT
        (SELECT count(*) FROM (SELECT {selected} FROM {left} EXCEPT ALL SELECT {selected} FROM {right})) left_only_rows,
        (SELECT count(*) FROM (SELECT {selected} FROM {right} EXCEPT ALL SELECT {selected} FROM {left})) right_only_rows""")


def audit_window(inputs: dict[str, str], window: dict) -> dict:
    if window["status"] != "preflight_complete":
        raise AuditBlocked("a blocked preflight cannot authorize body reads")
    con = connection("4GB")
    try:
        fences = window["fences"]
        for name, fields in (("raw", RAW_FIELDS), ("resolved", RESOLVED_FIELDS),
                             ("root_transformed", VALUE_FIELDS), ("clean", VALUE_FIELDS)):
            con.register(name, read_selection(window["selections"][name], fields))
        path = literal(inputs["timestamps"])
        con.execute(f"""CREATE VIEW timestamp_slice AS SELECT * FROM read_parquet({path})
            WHERE block_number BETWEEN {fences['lower_fence_block']} AND {fences['upper_fence_block']}""")
        for name in ("raw", "resolved"):
            coverage = scalar(con, f"""SELECT count(*) missing_exact_timestamp
                FROM {name} r LEFT JOIN timestamp_slice t USING(block_number) WHERE t.block_number IS NULL""")
            if coverage["missing_exact_timestamp"]:
                raise AuditBlocked(f"{name}: missing exact required timestamps in complete fence blocks")
        for name in ("raw", "resolved"):
            con.execute(f"""CREATE VIEW {name}_window AS SELECT r.* FROM {name} r
                JOIN timestamp_slice t USING(block_number)
                WHERE t.timestamp >= {fences['start_timestamp']}
                  AND t.timestamp < {fences['end_timestamp_exclusive']}""")
        output = {"name": window["name"], "status": "bounded_reconciliation_complete",
                  "native": native_summary(con, "raw_window"), "gates": []}
        defects = {**output["native"]["integrity"], **output["native"]["roles"],
                   **output["native"]["identity"]}
        for field in ("invalid_native_keys", "invalid_payload", "invalid_asset_xor",
                      "unknown_exchange_rows", "conflicting_native_keys"):
            if defects[field]:
                output["gates"].append({"gate": field, "count": defects[field]})
        if output["gates"]:
            output["status"] = "blocked_native_integrity"
            return output
        for name in ("token_map", "resolutions"):
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({literal(inputs[name])})")
        con.execute("""CREATE VIEW touched_tokens AS
            SELECT maker_asset_id token_id FROM canonical_retained WHERE maker_asset_id<>'0'
            UNION SELECT taker_asset_id FROM canonical_retained WHERE taker_asset_id<>'0'""")
        for name in ("token_map", "resolutions"):
            check = scalar(con, f"""SELECT count(*) duplicate_touched_keys FROM (
                SELECT m.token_id FROM {name} m JOIN touched_tokens t USING(token_id)
                GROUP BY m.token_id HAVING count(*)>1)""")
            if check["duplicate_touched_keys"]:
                output["gates"].append({"gate": f"{name}_duplicate_touched_keys", **check})
        if output["gates"]:
            output["status"] = "blocked_metadata_fanout"
            return output
        output["metadata_integrity"] = touched_metadata_summary(con)
        if any(output["metadata_integrity"].values()):
            output["gates"].append({"gate": "touched_metadata_integrity", **output["metadata_integrity"]})
            output["status"] = "blocked_metadata_integrity"
            return output
        create_expected_resolved(con)
        output["waterfall"] = scalar(con, f"""SELECT
            (SELECT count(*) FROM raw_window) raw_observed_rows,
            (SELECT count(*) FROM raw_window WHERE taker IN ({','.join(literal(x) for x in OLD_EXCHANGES)})) old_address_excluded_rows,
            (SELECT count(*) FROM canonical_ranked WHERE rn>1) tx_order_rank_removed_rows,
            (SELECT count(*) FROM canonical_retained) retained_native_rows,
            (SELECT count(*) FROM expected_mapped WHERE condition_id IS NULL) missing_mapping_rows,
            (SELECT count(*) FROM expected_mapped WHERE condition_id IS NOT NULL)
              -(SELECT count(*) FROM expected_resolved) mapped_without_resolution_rows,
            (SELECT count(*) FROM expected_resolved) expected_resolved_rows,
            (SELECT count(*) FROM resolved_window) current_resolved_rows""")
        output["native_membership"] = multiplicity_difference(
            con, "expected_resolved", "resolved_window", RAW_FIELDS)
        output["resolved_payload_membership"] = multiplicity_difference(
            con, "expected_resolved", "resolved_window", RESOLVED_FIELDS)
        for gate in ("native_membership", "resolved_payload_membership"):
            if any(output[gate].values()):
                output["gates"].append({"gate": gate, **output[gate]})
        create_expansion(con, "resolved_window", "expanded")
        output["expanded_from_current_resolved"] = value_summary(con, "expanded", identities=True)
        output["root_transformed"] = value_summary(con, "root_transformed")
        output["clean"] = value_summary(con, "clean")
        output["expanded_to_root_membership"] = multiplicity_difference(
            con, "expanded", "root_transformed", VALUE_FIELDS)
        values = ','.join(qname(field) for field in VALUE_FIELDS)
        con.execute(f"CREATE VIEW expected_clean AS SELECT DISTINCT {values} FROM root_transformed")
        output["distinct_root_to_clean_membership"] = multiplicity_difference(
            con, "expected_clean", "clean", VALUE_FIELDS)
        for gate in ("expanded_to_root_membership", "distinct_root_to_clean_membership"):
            if any(output[gate].values()):
                output["gates"].append({"gate": gate, **output[gate]})
        output["clean_removal"] = {
            "removed_expanded_value_rows": output["root_transformed"]["row_count"]-output["clean"]["row_count"],
            "removed_expanded_recorded_cash": output["root_transformed"]["expanded_recorded_cash"]-output["clean"]["expanded_recorded_cash"],
            "attribution_valid": not any(output["expanded_to_root_membership"].values())}
        if output["gates"]:
            output["status"] = "bounded_reconciliation_failed"
        output["scope_note"] = "Only the predeclared complete UTC window was audited. No universe-wide certification or execution-direction validation follows."
        return output
    finally:
        con.close()


def write_immutable(run_dir: Path, result: dict) -> None:
    data = json.dumps(result, sort_keys=True, indent=2, default=str).encode()
    if len(data) > MAX_OUTPUT_BYTES:
        raise AuditBlocked("scalar manifest exceeds output byte cap")
    run_dir.mkdir(parents=True, exist_ok=False)
    partial = run_dir / "manifest.json.partial"
    with partial.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(partial, run_dir / "manifest.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true", help="footers and timestamp cache only; never large input bodies")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--reviewed-preflight", help="separate reviewed immutable manifest required for body execution")
    for name, path in DEFAULT_INPUTS.items():
        parser.add_argument("--" + name.replace("_", "-"), default=path)
    args = parser.parse_args()
    from production_guard import require_production_host
    require_production_host()
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                            capture_output=True, text=True).stdout.strip()
    unchanged = subprocess.run(["git", "diff", "--quiet", BASELINE_COMMIT, "--", *FROZEN_SOURCES], cwd=ROOT)
    if unchanged.returncode != 0:
        raise AuditBlocked("frozen canonical pipeline sources differ from baseline git blobs")
    destination = Path(args.run_dir)
    if destination.exists():
        raise AuditBlocked("immutable output run directory already exists")
    inputs = {name: getattr(args, name) for name in DEFAULT_INPUTS}
    con = connection()
    try:
        result = preflight(inputs, con)
    finally:
        con.close()
    result["command"] = sys.argv
    result["actual_audit_commit"] = commit
    result["audit_script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    result["execution_note"] = "Preflight only; no raw/resolved/transformed/clean row body was read."
    if not args.preflight:
        if not args.reviewed_preflight:
            raise AuditBlocked("body reads require --reviewed-preflight from a separately reviewed run")
        script = str(Path(__file__).resolve().relative_to(ROOT))
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", script], cwd=ROOT,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        clean_script = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", script], cwd=ROOT)
        if tracked.returncode or clean_script.returncode:
            raise AuditBlocked("body audit script must be committed and unchanged")
        reviewed = json.loads(Path(args.reviewed_preflight).read_text())
        if reviewed.get("status") != "preflight_complete":
            raise AuditBlocked("reviewed preflight is blocked or incomplete")
        for field in ("inputs", "caps", "inventories", "windows", "baseline_commit", "audit_script_sha256", "environment"):
            if reviewed.get(field) != result[field]:
                raise AuditBlocked(f"current {field} differs from reviewed frozen preflight")
        if result["status"] != "preflight_complete":
            raise AuditBlocked("current preflight blocks production body reads")
        result["window_results"] = []
        for window in result["windows"]:
            try:
                result["window_results"].append(audit_window(inputs, window))
            except (AuditBlocked, duckdb.Error) as error:
                result["window_results"].append({"name": window["name"], "status": "blocked", "reason": str(error)})
        result["status"] = ("bounded_reconciliation_complete" if all(
            item["status"] == "bounded_reconciliation_complete" for item in result["window_results"])
            else "bounded_reconciliation_incomplete")
        result["execution_note"] = "Complete selected row groups read under separately reviewed preflight; scalar evidence only, no source identifiers published."
    write_immutable(destination, result)
    print(json.dumps({"status": result["status"], "run_dir": str(destination),
                      "blocks": result["blocks"], "caps": result["caps"]}))
    return 0 if result["status"] in {"preflight_complete", "bounded_reconciliation_complete"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
