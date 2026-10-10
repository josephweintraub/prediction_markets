"""Immutable, bounded archive-record inputs for the September 28 replication.

The production runner admits footers and resources before reading Parquet bodies.
The small SQL helpers are deliberately usable on synthetic fixtures. Archive
roles/actions are descriptive conventions, not certified native wallet actions.
"""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import duckdb
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[2]
CUTOFF_ISO = "2026-03-25T00:00:00Z"
CUTOFF_SECONDS = int(datetime(2026, 3, 25, tzinfo=timezone.utc).timestamp())
CATEGORY_LABELS = ("Crypto", "Culture", "Economy", "Esports", "Finance", "Geopolitics",
                   "Iran", "Mentions", "Politics", "Sports", "Tech", "Weather", "Unclassified")
REPAIR_ROOT = Path("/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1")
METADATA_PATHS = {
    "spine": "/mnt/data/pipeline_output/market_flags.parquet",
    "token_map": "/mnt/data/pipeline_data/token_map.parquet",
    "native": "/mnt/data/learnability/native/native_market_meta.parquet",
    "categories": "/mnt/data/learnability/native/market_native_categories.parquet",
}
REQUIRED_FIELDS = {
    "trades": {"proxyWallet", "timestamp", "conditionId", "usdcSize", "price", "side",
               "outcome", "eventSlug", "is_maker", "counterparty", "year_month"},
    "spine": {"token_id", "market_id", "winning_outcome"},
    "token_map": {"token_id", "condition_id", "outcome"},
    "native": {"condition_id", "n_outcomes", "created_at", "end_date", "event_slug"},
    "categories": {"mkt", "prim"},
}
CAPS = {"memory_limit": "32GB", "threads": 4, "spill_bytes": 4_000_000_000,
        "minimum_free_bytes": 20_000_000_000, "maximum_output_bytes": 60_000_000_000,
        "maximum_month_file_bytes": 4_000_000_000, "maximum_metadata_file_bytes": 1_000_000_000,
        "maximum_read_bytes": 8_000_000_000_000, "maximum_manifest_bytes": 16_000_000,
        "maximum_metadata_rows": 4_000_000}
SOURCE_FILES = ("analysis/kaushik_polymarket_replication/build_inputs.py",
                "scripts/build_kaushik_polymarket_replication.py",
                "tests/test_kaushik_replication_inputs.py",
                "docs/analysis_specs/kaushik_polymarket_replication_v1.md")


class InputBlocked(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InputBlocked(message)


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def ident(value):
    return '"' + str(value).replace('"', '""') + '"'


def stat_identity(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "regular non-symlink file required: " + str(path))
    item = path.stat()
    return {"device": item.st_dev, "inode": item.st_ino, "bytes": item.st_size,
            "mtime_ns": item.st_mtime_ns, "ctime_ns": item.st_ctime_ns}


def sha256(path):
    before = stat_identity(path)
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    require(stat_identity(path) == before, "input changed while hashing: " + str(path))
    return digest.hexdigest()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def read_json(path, expected_sha=None):
    before = stat_identity(path)
    require(0 < before["bytes"] <= CAPS["maximum_manifest_bytes"], "JSON size cap exceeded")
    raw = Path(path).read_bytes()
    require(before == stat_identity(path), "metadata changed during read")
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha is not None:
        require(re.fullmatch(r"[0-9a-f]{64}", expected_sha or "") and digest == expected_sha,
                "JSON hash binding differs")
    value = json.loads(raw, object_pairs_hook=_unique_object,
                       parse_constant=lambda token: (_ for _ in ()).throw(InputBlocked(token)))
    require(type(value) is dict, "JSON object required")
    return value, {"path": str(Path(path).resolve()), "sha256": digest, "stat": before}


def write_json(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= CAPS["maximum_manifest_bytes"], "output JSON exceeds size cap")
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def footer(path, kind):
    before = stat_identity(path)
    item = pq.ParquetFile(path)
    fields = set(item.schema_arrow.names)
    require(REQUIRED_FIELDS[kind] <= fields, "required " + kind + " fields absent")
    if kind == "trades":
        require(fields == REQUIRED_FIELDS[kind], "archive schema must retain exactly the admitted 11 fields")
        require(str(item.schema_arrow.field("timestamp").type) == "int64", "timestamp must be int64 Unix seconds")
        require(str(item.schema_arrow.field("is_maker").type) == "bool", "archive role must be boolean")
        require(str(item.schema_arrow.field("price").type) == "double", "recorded price must be native binary64, without conversion")
        require(str(item.schema_arrow.field("usdcSize").type) == "double", "archive gross cash field must be native binary64")
    with Path(path).open("rb") as stream:
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        require(trailer[4:] == b"PAR1", "invalid Parquet trailer")
        size = int.from_bytes(trailer[:4], "little")
        require(0 < size <= before["bytes"] - 8, "invalid Parquet footer length")
        stream.seek(-8-size, os.SEEK_END)
        fingerprint = hashlib.sha256(stream.read(size) + trailer).hexdigest()
    require(stat_identity(path) == before, "input changed during footer read")
    return {"path": str(Path(path).resolve()), "stat": before, "rows": item.metadata.num_rows,
            "row_groups": item.metadata.num_row_groups, "schema": str(item.schema_arrow),
            "footer_sha256": fingerprint}


def source_snapshot(expected_head):
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or ""), "40-character committed source HEAD required")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    require(head == expected_head, "source HEAD differs")
    for name in SOURCE_FILES:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", name], cwd=REPO, capture_output=True)
        clean = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name], cwd=REPO)
        require(tracked.returncode == clean.returncode == 0, "replication source must be committed and unchanged: " + name)
    return {"head": head, "files": {name: sha256(REPO / name) for name in SOURCE_FILES}}


def load_contract(path):
    contract, binding = read_json(path)
    require(contract.get("schema_version") == "kaushik_replication_input_contract_v1", "input contract version differs")
    require(contract.get("category_labels") == list(CATEGORY_LABELS), "retained native taxonomy/order differs")
    require(contract.get("caps") == CAPS, "resource caps differ from committed limits")
    require(contract.get("opening_field") == "created_at" and contract.get("unavailable_opening_field") == "start_date"
            and contract.get("endpoint_field") == "end_date" and contract.get("native_clock_encoding") == "ISO8601_UTC",
            "clock field/encoding contract differs")
    require(contract.get("outcome_label_rule") == "exact_string", "outcome label agreement rule differs")
    require(contract.get("archive_cash_semantics") == "gross_execution_cash_quantity_equals_usdcSize_over_recorded_price",
            "archive cash field semantics not admitted")
    metadata = contract.get("metadata", {})
    require(set(metadata) == set(METADATA_PATHS), "metadata input inventory differs")
    for name, item in metadata.items():
        require(item.get("path") == METADATA_PATHS[name] and re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")),
                "canonical metadata path/digest absent: " + name)
    repair = contract.get("repair_manifest", {})
    require(repair.get("path") == str(REPAIR_ROOT / "manifest.json"), "repaired CLEAN manifest path differs")
    manifest, repair_id = read_json(repair["path"], repair.get("sha256"))
    require(manifest.get("status") == "repair_complete" and manifest.get("schema_version") == "polymarket_wallet_repair_v1",
            "repair completion/version absent")
    gates = manifest.get("reconciliation", {})
    require(all(gates.get(name) is True for name in ("exact_full11_multisets", "unchanged_other9_fields",
                "original_file_counts_types_and_multiplicities", "original_inputs_reopened")), "repair reconciliation incomplete")
    records = [item for item in manifest.get("outputs", []) if item.get("relation") == "clean"]
    months = [f"{year:04d}-{month:02d}" for year in range(2022, 2027) for month in range(1, 13)
              if "2022-11" <= f"{year:04d}-{month:02d}" <= "2026-06"]
    require(len(records) == 44 and sorted(item["month"] for item in records) == months, "44-month repaired CLEAN coverage differs")
    files = []
    for item in sorted(records, key=lambda record: record["month"]):
        expected = REPAIR_ROOT / "clean" / ("year_month=" + item["month"]) / "data.parquet"
        require(item["path"] == str(expected.parent.relative_to(REPAIR_ROOT)), "repair month directory binding differs")
        output = item["output"]
        require(re.fullmatch(r"[0-9a-f]{64}", output.get("sha256", "")), "repair output digest absent")
        require(output.get("path") in ("data.parquet", str(expected)), "repair output file binding differs")
        files.append({"month": item["month"], "path": str(expected), "sha256": output["sha256"],
                      "rows": output["rows"], "bytes": output["bytes"]})
    require(sum(item["rows"] for item in files) == manifest["rows"]["clean"], "repair CLEAN totals differ")
    return contract, files, {"contract": binding, "repair_manifest": repair_id}


def validate_destination(target, inputs):
    target = Path(target).resolve()
    require(target.parent == Path("/mnt/data/runs") and not target.exists(), "new top-level /mnt/data/runs directory required")
    require(target != REPAIR_ROOT, "prior repaired run is immutable")
    for value in inputs:
        path = Path(value).resolve()
        require(target != path and target not in path.parents and path not in target.parents, "destination overlaps input")


def preflight(contract_path, target, expected_head):
    contract, files, binding = load_contract(contract_path)
    source = source_snapshot(expected_head)
    paths = {name: item["path"] for name, item in contract["metadata"].items()}
    validate_destination(target, [*paths.values(), *(item["path"] for item in files), contract_path, binding["repair_manifest"]["path"]])
    metadata = {name: {**footer(path, name), "expected_sha256": contract["metadata"][name]["sha256"]} for name, path in paths.items()}
    for value in metadata.values():
        require(0 < value["rows"] <= CAPS["maximum_metadata_rows"], "metadata row cap exceeded")
    trades = []
    for item in files:
        entry = footer(item["path"], "trades")
        require(entry["rows"] == item["rows"] and entry["stat"]["bytes"] == item["bytes"], "repair output footer/stat binding differs")
        trades.append({**entry, "month": item["month"], "expected_sha256": item["sha256"]})
    planned = 16 * sum(item["stat"]["bytes"] for item in [*metadata.values(), *trades]) + 4 * CAPS["maximum_output_bytes"]
    require(planned <= CAPS["maximum_read_bytes"], "planned read ceiling exceeded")
    free = shutil.disk_usage(Path(target).parent).free
    required_free = CAPS["maximum_output_bytes"] + CAPS["spill_bytes"] + CAPS["minimum_free_bytes"]
    require(free >= required_free, "capacity below output/spill/free-floor reservation")
    require((os.cpu_count() or 0) >= CAPS["threads"], "available CPU count below contract")
    available = re.search(r"^MemAvailable:\s+(\d+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    require(available and int(available.group(1)) * 1024 >= 36_000_000_000, "insufficient available memory for 32GB DuckDB limit")
    return {"schema_version": "kaushik_replication_input_preflight_v1", "status": "preflight_complete",
            "target": str(Path(target).resolve()), "source": source, "binding": binding, "contract": contract,
            "caps": CAPS, "metadata": metadata, "trades": trades, "planned_read_bytes": planned,
            "required_free_bytes": required_free, "observed_free_bytes": free,
            "limitations": "Footer/stat/resource admission only; no Parquet bodies read. COPY ceilings are hard limits, not compression forecasts."}


def configure(con, spill):
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit=" + literal(CAPS["memory_limit"]))
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute("SET max_temp_directory_size=" + literal(str(CAPS["spill_bytes"]) + "B"))
    con.execute("SET temp_directory=" + literal(spill))
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_object_cache=false")


def prepare_metadata(con, paths, category_labels=CATEGORY_LABELS):
    """Build one-to-one claims; ambiguous metadata cannot multiply a trade."""
    require(tuple(category_labels) == CATEGORY_LABELS, "category taxonomy differs")
    con.execute("SET TimeZone='UTC'")
    for name in REQUIRED_FIELDS.keys() - {"trades"}:
        con.execute(f"CREATE OR REPLACE TEMP VIEW raw_{name} AS SELECT * FROM read_parquet({literal(paths[name])}, hive_partitioning=false)")
    known = ",".join(literal(value) for value in CATEGORY_LABELS[:-1])
    unknown = con.execute(f"SELECT count(*) FROM raw_categories WHERE prim IS NOT NULL AND CAST(prim AS VARCHAR) NOT IN ({known})").fetchone()[0]
    require(unknown == 0, "native category labels outside retained taxonomy")
    con.execute("""CREATE OR REPLACE TEMP TABLE token_health AS
        SELECT CAST(token_id AS VARCHAR) token_id, count(*) map_rows,
               min(CAST(condition_id AS VARCHAR)) market_id, min(CAST(outcome AS VARCHAR)) outcome_label
        FROM raw_token_map GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE spine_health AS
        SELECT CAST(token_id AS VARCHAR) token_id, count(*) spine_rows,
               min(CAST(market_id AS VARCHAR)) market_id,
               min(CAST(winning_outcome AS VARCHAR)) winning_outcome_label,
               count(winning_outcome) observed_payout_rows
        FROM raw_spine GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE condition_health AS
        SELECT CAST(condition_id AS VARCHAR) market_id, count(*) map_rows,
               count(DISTINCT CAST(token_id AS VARCHAR)) tokens,
               count(DISTINCT CAST(outcome AS VARCHAR)) labels
        FROM raw_token_map GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE native_health AS
        SELECT CAST(condition_id AS VARCHAR) market_id, count(*) native_rows,
               min(try_cast(n_outcomes AS INTEGER)) n_outcomes,
               min(nullif(CAST(event_slug AS VARCHAR),'')) event_slug,
               count(DISTINCT nullif(CAST(event_slug AS VARCHAR),'')) distinct_event_slugs,
               min(epoch(try_cast(created_at AS TIMESTAMPTZ))) opening_seconds,
               min(epoch(try_cast(end_date AS TIMESTAMPTZ))) endpoint_seconds
        FROM raw_native GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE category_health AS
        SELECT CAST(mkt AS VARCHAR) market_id, count(*) category_rows,
               min(CAST(prim AS VARCHAR)) category FROM raw_categories GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE pair_tokens AS
        SELECT t.token_id,t.market_id,t.outcome_label,s.winning_outcome_label,
               CASE WHEN t.outcome_label=s.winning_outcome_label THEN 1 ELSE 0 END Y,
               n.event_slug,n.opening_seconds,n.endpoint_seconds,
               coalesce(c.category,'Unclassified') category
        FROM token_health t JOIN spine_health s ON t.token_id=s.token_id
        JOIN condition_health h ON t.market_id=h.market_id
        JOIN native_health n ON t.market_id=n.market_id
        LEFT JOIN category_health c ON t.market_id=c.market_id
        WHERE t.map_rows=1 AND s.spine_rows=1 AND s.observed_payout_rows=1
          AND t.market_id=s.market_id AND nullif(s.winning_outcome_label,'') IS NOT NULL
          AND h.map_rows=2 AND h.tokens=2 AND h.labels=2
          AND n.native_rows=1 AND n.n_outcomes=2
          AND coalesce(c.category_rows,1)=1
          AND nullif(t.token_id,'') IS NOT NULL AND nullif(t.market_id,'') IS NOT NULL
          AND nullif(t.outcome_label,'') IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE admitted_pairs AS
        SELECT market_id FROM pair_tokens GROUP BY 1
        HAVING count(*)=2 AND count(DISTINCT token_id)=2 AND count(DISTINCT outcome_label)=2
          AND count(DISTINCT winning_outcome_label)=1 AND sum(Y)=1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE coded_tokens AS
        SELECT cast(row_number() OVER (ORDER BY token_id)-1 AS INTEGER) claim_code,p.*
        FROM pair_tokens p JOIN admitted_pairs a USING(market_id)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE claims AS
        SELECT a.claim_code,a.token_id,a.market_id,a.outcome_label,a.winning_outcome_label,cast(a.Y AS TINYINT) Y,
               b.claim_code complement_code,b.token_id complement_token_id,b.outcome_label complement_label,
               CASE WHEN a.event_slug IS NOT NULL THEN 'event:'||a.event_slug ELSE 'market:'||a.market_id END event_cluster,
               CASE WHEN a.event_slug IS NOT NULL THEN 'native_event' ELSE 'market_fallback' END cluster_source,
               a.category,a.opening_seconds,a.endpoint_seconds,
               'native_created_at_fallback_start_date_unavailable' opening_source,
               'native_end_date' endpoint_source
        FROM coded_tokens a JOIN coded_tokens b ON a.market_id=b.market_id AND a.token_id<>b.token_id""")
    rows, unique_tokens, unique_codes = con.execute("SELECT count(*),count(DISTINCT token_id),count(DISTINCT claim_code) FROM claims").fetchone()
    require(rows == unique_tokens == unique_codes and rows <= 2_147_483_647, "claim grain/encoding uniqueness failed")
    bad_pairs = con.execute("""SELECT count(*) FROM claims a JOIN claims b ON a.complement_code=b.claim_code
        WHERE a.market_id<>b.market_id OR a.Y+b.Y<>1 OR b.complement_code<>a.claim_code""").fetchone()[0]
    require(bad_pairs == 0, "binary complement reciprocity failed")
    return {"admitted_claims": rows, "admitted_conditions": rows // 2,
            "metadata_rows": {name: con.execute(f"SELECT count(*) FROM raw_{name}").fetchone()[0]
                              for name in ("spine", "token_map", "native", "categories")},
            "blank_or_null_token_map_keys": con.execute("SELECT count(*) FROM raw_token_map WHERE token_id IS NULL OR CAST(token_id AS VARCHAR)='' OR condition_id IS NULL OR CAST(condition_id AS VARCHAR)='' OR outcome IS NULL OR CAST(outcome AS VARCHAR)=''").fetchone()[0],
            "blank_or_null_spine_keys": con.execute("SELECT count(*) FROM raw_spine WHERE token_id IS NULL OR CAST(token_id AS VARCHAR)='' OR market_id IS NULL OR CAST(market_id AS VARCHAR)=''").fetchone()[0],
            "blank_or_null_native_keys": con.execute("SELECT count(*) FROM raw_native WHERE condition_id IS NULL OR CAST(condition_id AS VARCHAR)=''").fetchone()[0],
            "blank_or_null_category_keys": con.execute("SELECT count(*) FROM raw_categories WHERE mkt IS NULL OR CAST(mkt AS VARCHAR)=''").fetchone()[0],
            "duplicate_token_map_keys": con.execute("SELECT count(*) FROM token_health WHERE map_rows<>1").fetchone()[0],
            "duplicate_spine_keys": con.execute("SELECT count(*) FROM spine_health WHERE spine_rows<>1").fetchone()[0],
            "duplicate_native_conditions": con.execute("SELECT count(*) FROM native_health WHERE native_rows<>1").fetchone()[0],
            "ambiguous_event_map_conditions": con.execute("SELECT count(*) FROM native_health WHERE distinct_event_slugs>1").fetchone()[0],
            "duplicate_category_conditions": con.execute("SELECT count(*) FROM category_health WHERE category_rows<>1").fetchone()[0],
            "spine_condition_disagreement_tokens": con.execute("SELECT count(*) FROM token_health t JOIN spine_health s USING(token_id) WHERE t.market_id<>s.market_id").fetchone()[0],
            "native_event_claims": con.execute("SELECT count(*) FROM claims WHERE cluster_source='native_event'").fetchone()[0],
            "market_fallback_claims": con.execute("SELECT count(*) FROM claims WHERE cluster_source='market_fallback'").fetchone()[0],
            "missing_opening_claims": con.execute("SELECT count(*) FROM claims WHERE opening_seconds IS NULL OR NOT isfinite(opening_seconds)").fetchone()[0],
            "missing_endpoint_claims": con.execute("SELECT count(*) FROM claims WHERE endpoint_seconds IS NULL OR NOT isfinite(endpoint_seconds)").fetchone()[0],
            "nonpositive_lifetime_claims": con.execute("SELECT count(*) FROM claims WHERE endpoint_seconds<=opening_seconds").fetchone()[0],
            "endpoint_after_execution_cutoff_claims": con.execute("SELECT count(*) FROM claims WHERE endpoint_seconds>=?", [CUTOFF_SECONDS]).fetchone()[0],
            "unavailable_opening_field": "start_date", "opening_field": "created_at", "endpoint_field": "end_date"}


def annotate_month(con, path, month):
    """Create a lazy all-record audit view, retaining physical row multiplicity."""
    require(re.fullmatch(r"\d{4}-\d{2}", month or ""), "source month syntax invalid")
    con.execute(f"CREATE OR REPLACE TEMP VIEW source_month AS SELECT *,file_row_number::BIGINT source_ordinal FROM read_parquet({literal(path)}, hive_partitioning=false,file_row_number=true)")
    con.execute("""CREATE OR REPLACE TEMP VIEW joined_month AS
        SELECT s.*, c.claim_code recorded_token_code,c.complement_code,
               c.Y recorded_Y,c.outcome_label native_label,c.opening_seconds,c.endpoint_seconds,
               CASE WHEN side='SELL' THEN 1-price::DOUBLE ELSE price::DOUBLE END P,
               CASE WHEN side='SELL' THEN 1-c.Y ELSE c.Y END Y,
               CASE WHEN side='SELL' THEN c.complement_code ELSE c.claim_code END claim_code
        FROM source_month s LEFT JOIN claims c ON CAST(s.conditionId AS VARCHAR)=c.token_id""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW annotated_month AS
        SELECT *, CASE
          WHEN timestamp IS NULL OR timestamp<0 THEN 'invalid_timestamp'
          WHEN timestamp>={CUTOFF_SECONDS} THEN 'at_or_after_cutoff'
          WHEN is_maker IS NULL THEN 'missing_archive_role'
          WHEN side IS NULL OR side NOT IN ('BUY','SELL') THEN 'invalid_archive_action'
          WHEN price IS NULL OR NOT isfinite(price::DOUBLE) OR price<=0 OR price>=1 THEN 'invalid_recorded_price'
          WHEN usdcSize IS NULL OR NOT isfinite(usdcSize::DOUBLE) OR usdcSize<=0 THEN 'invalid_size'
          WHEN recorded_token_code IS NULL THEN 'unadmitted_token_pair'
          WHEN outcome IS NULL OR CAST(outcome AS VARCHAR)<>native_label THEN 'outcome_label_disagreement'
          WHEN NOT isfinite(P) OR P<=0 OR P>=1 THEN 'invalid_normalized_price'
          ELSE 'eligible' END eligibility_reason,
          coalesce(isfinite(opening_seconds) AND isfinite(endpoint_seconds)
            AND endpoint_seconds>opening_seconds AND timestamp>=opening_seconds
            AND timestamp<=endpoint_seconds,false) duration_clock_valid
        FROM joined_month""")


def month_counts(con, expected_rows, month):
    rows, locators, fanout, mismatched_month = con.execute(f"""SELECT count(*),count(DISTINCT source_ordinal),
        count(*)-count(DISTINCT source_ordinal),count(*) FILTER(WHERE year_month IS NULL OR year_month<>{literal(month)})
        FROM annotated_month""").fetchone()
    require(rows == locators == expected_rows and fanout == mismatched_month == 0, "trade locator/month/no-fanout reconciliation failed")
    # Confine non-null archive timestamps to their source partition, including excluded records.
    year, number = map(int, month.split("-"))
    begin = int(datetime(year, number, 1, tzinfo=timezone.utc).timestamp())
    end = int(datetime(year + (number == 12), number % 12 + 1, 1, tzinfo=timezone.utc).timestamp())
    outside = con.execute(f"SELECT count(*) FROM annotated_month WHERE timestamp IS NOT NULL AND (timestamp<{begin} OR timestamp>={end})").fetchone()[0]
    require(outside == 0, "source timestamp outside its immutable month partition")
    records = con.execute("""SELECT eligibility_reason,side,is_maker,count(*) AS row_count,
        count(*) FILTER(WHERE timestamp IS NOT NULL AND timestamp>=0 AND timestamp< ?) precut_rows,
        count(*) FILTER(WHERE eligibility_reason='eligible' AND duration_clock_valid AND (P<.1 OR P>=.9)) duration_tail_rows
        FROM annotated_month GROUP BY ALL ORDER BY eligibility_reason,side,is_maker""", [CUTOFF_SECONDS]).fetchall()
    return [{"reason": reason, "side": side, "is_maker": maker, "rows": count,
             "precut_rows": precut, "duration_tail_rows": duration} for reason, side, maker, count, precut, duration in records]


@contextmanager
def copy_ceiling(maximum_bytes):
    previous = resource.getrlimit(resource.RLIMIT_FSIZE)
    previous_signal = signal.getsignal(signal.SIGXFSZ)
    finite = [value for value in previous if value != resource.RLIM_INFINITY]
    ceiling = min([maximum_bytes, *finite])
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    try:
        resource.setrlimit(resource.RLIMIT_FSIZE, (ceiling, previous[1]))
        yield
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, previous)
        signal.signal(signal.SIGXFSZ, previous_signal)


def atomic_publish(staging, target):
    staging, target = Path(staging), Path(target)
    require(staging.is_dir() and not target.exists(), "immutable destination exists or stage absent")
    library = ctypes.CDLL(None, use_errno=True)
    if platform.system() == "Linux":
        require(hasattr(library, "renameat2"), "Linux atomic no-replace rename unavailable")
        result = library.renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    elif platform.system() == "Darwin":
        result = library.renamex_np(os.fsencode(staging), os.fsencode(target), 4)
    else:
        raise InputBlocked("atomic no-replace directory publication unavailable")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(target))


def create_analysis_view(con, stage, name="analysis_base", explicit_month_paths=None):
    """Restore derived estimand/clocks lazily; select is_maker=false for primary."""
    require(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name), "invalid view identifier")
    stage = Path(stage)
    sources = literal(stage / 'year_month=*' / 'base.parquet')
    if explicit_month_paths is not None:
        manifest, _ = read_json(stage / "manifest.json")
        expected = [str((stage / relative).resolve()) for relative in sorted(manifest["outputs"])
                    if re.fullmatch(r"year_month=\d{4}-\d{2}/base\.parquet", relative)]
        provided = [str(Path(path).resolve()) for path in explicit_month_paths]
        require(provided and provided == expected, "explicit month paths differ from sorted immutable manifest inventory")
        sources = "[" + ",".join(literal(path) for path in provided) + "]"
    con.execute(f"""CREATE OR REPLACE TEMP VIEW {ident(name)} AS WITH joined AS (
        SELECT b.*,c.token_id claim_id,r.token_id recorded_token_id,c.market_id,c.event_cluster,c.cluster_source,c.category,
          c.opening_seconds,c.endpoint_seconds,(c.endpoint_seconds-c.opening_seconds)/86400.0 L,
          (c.endpoint_seconds-b.timestamp)/86400.0 R,
          strftime(to_timestamp(b.timestamp),'%Y-%m') trade_month,
          CASE WHEN b.P<.1 THEN 1 WHEN b.P<.2 THEN 2 WHEN b.P<.3 THEN 3 WHEN b.P<.4 THEN 4
               WHEN b.P<.5 THEN 5 WHEN b.P<.6 THEN 6 WHEN b.P<.7 THEN 7 WHEN b.P<.8 THEN 8 WHEN b.P<.9 THEN 9 ELSE 10 END bin
        FROM read_parquet({sources},hive_partitioning=false) b
        JOIN read_parquet({literal(stage / 'claims.parquet')}) c USING(claim_code)
        JOIN read_parquet({literal(stage / 'claims.parquet')}) r ON b.recorded_token_code=r.claim_code)
        SELECT *,100*(Y-P) payoff,100*(Y/P-1) roi,usdcSize/recorded_price quantity,
          (usdcSize/recorded_price)*P capital,
          CASE WHEN duration_eligible THEN log2(1+L) END xL,
          CASE WHEN duration_eligible THEN log2(1+R) END xR FROM joined""")


def _directory_bytes(path):
    return sum(item.stat().st_size for item in Path(path).rglob("*") if item.is_file())


def _reserve_output(staging, ceiling):
    used = _directory_bytes(staging)
    require(used + ceiling <= CAPS["maximum_output_bytes"], "remaining total output budget below hard COPY ceiling")
    require(shutil.disk_usage(staging).free >= ceiling + CAPS["minimum_free_bytes"] + CAPS["spill_bytes"],
            "disk floor/spill reservation fails before COPY")


def support_counts(con):
    values = con.execute("""SELECT is_maker,side,category,bin,count(*) AS row_count,count(DISTINCT event_cluster) clusters,
        count(*) FILTER(WHERE duration_eligible AND bin IN (1,10)) duration_tail_rows,
        count(*) FILTER(WHERE NOT isfinite(quantity) OR NOT isfinite(capital)) numerical_amount_anomalies,
        count(*) FILTER(WHERE NOT isfinite(payoff) OR NOT isfinite(roi)) nonfinite_analytic_outcomes
        FROM analysis_base GROUP BY ALL ORDER BY is_maker,side,category,bin""").fetchall()
    keys = ("is_maker", "side", "category", "bin", "rows", "clusters", "duration_tail_rows",
            "numerical_amount_anomalies", "nonfinite_analytic_outcomes")
    return [dict(zip(keys, row)) for row in values]


def build_stage(reviewed, fresh, command, reviewed_identity=None):
    from production_guard import require_production_host
    require_production_host()
    require(isinstance(reviewed_identity, dict), "reviewed preflight file identity required")
    reopened_review, reopened_identity = read_json(reviewed_identity["path"], reviewed_identity["sha256"])
    require(reopened_review == reviewed and reopened_identity == reviewed_identity, "reviewed preflight content/stat drift")
    for field in ("schema_version", "status", "target", "source", "binding", "contract", "caps", "metadata", "trades", "planned_read_bytes", "required_free_bytes"):
        require(reviewed[field] == fresh[field], "reviewed footer/resource contract drift: " + field)
    target = Path(fresh["target"])
    validate_destination(target, [item["path"] for item in [*fresh["metadata"].values(), *fresh["trades"]]])
    staging = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
    started = time.monotonic()
    outputs, counts, frozen = {}, {}, {}
    con = None
    try:
        for item in [*fresh["metadata"].values(), *fresh["trades"]]:
            require(stat_identity(item["path"]) == item["stat"], "input stat drift before hashing")
            require(sha256(item["path"]) == item["expected_sha256"], "input content digest differs: " + item["path"])
            frozen[item["path"]] = item["expected_sha256"]
        con = duckdb.connect()
        configure(con, staging / "spill")
        health = prepare_metadata(con, {name: item["path"] for name, item in fresh["metadata"].items()})
        _reserve_output(staging, CAPS["maximum_metadata_file_bytes"])
        with copy_ceiling(CAPS["maximum_metadata_file_bytes"]):
            con.execute(f"COPY claims TO {literal(staging / 'claims.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
        outputs["claims.parquet"] = _output_info(staging / "claims.parquet")
        for item in fresh["trades"]:
            month = item["month"]
            folder = staging / ("year_month=" + month)
            folder.mkdir()
            annotate_month(con, item["path"], month)
            summary = month_counts(con, item["rows"], month)
            counts[month] = summary
            eligible = sum(row["rows"] for row in summary if row["reason"] == "eligible")
            for name, query, expected in (
                ("base.parquet", "SELECT source_ordinal,year_month::VARCHAR source_month,cast(claim_code AS INTEGER) claim_code,cast(recorded_token_code AS INTEGER) recorded_token_code,timestamp,price::DOUBLE recorded_price,P::DOUBLE P,Y::TINYINT Y,usdcSize::DOUBLE usdcSize,side,is_maker,duration_clock_valid duration_eligible FROM annotated_month WHERE eligibility_reason='eligible'", eligible),
                ("exclusions.parquet", "SELECT source_ordinal,eligibility_reason,side,is_maker FROM annotated_month WHERE eligibility_reason<>'eligible'", item["rows"]-eligible)):
                output = folder / name
                _reserve_output(staging, CAPS["maximum_month_file_bytes"])
                with copy_ceiling(CAPS["maximum_month_file_bytes"]):
                    con.execute(f"COPY ({query}) TO {literal(output)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)")
                info = _output_info(output)
                require(info["rows"] == expected, "reopened output count differs")
                reopened, locators = con.execute(f"SELECT count(*),count(DISTINCT source_ordinal) FROM read_parquet({literal(output)},hive_partitioning=false)").fetchone()
                require(reopened == locators == expected, "reopened locator uniqueness differs")
                outputs[str(output.relative_to(staging))] = info
            print(json.dumps({"stage": "month_complete", "month": month, "input_rows": item["rows"], "eligible_rows": eligible}), flush=True)
        create_analysis_view(con, staging)
        support = support_counts(con)
        total_input = sum(item["rows"] for item in fresh["trades"])
        eligible = sum(row["rows"] for values in counts.values() for row in values if row["reason"] == "eligible")
        require(sum(row["rows"] for values in counts.values() for row in values) == total_input, "all-record exclusion waterfall incomplete")
        require(sum(row["rows"] for row in support) == eligible, "published support does not exhaust baseline")
        require(con.execute("SELECT count(*) FROM analysis_base").fetchone()[0] == eligible, "published metadata join fanout/loss")
        for name, item in fresh["metadata"].items():
            require(footer(item["path"], name) == {key: item[key] for key in ("path", "stat", "rows", "row_groups", "schema", "footer_sha256")}, "metadata footer drift")
        for item in [*fresh["metadata"].values(), *fresh["trades"]]:
            require(stat_identity(item["path"]) == item["stat"] and sha256(item["path"]) == frozen[item["path"]], "input drift before publication")
        for binding in [*fresh["binding"].values(), reviewed_identity]:
            require(stat_identity(binding["path"]) == binding["stat"] and sha256(binding["path"]) == binding["sha256"], "bound JSON metadata drift before publication")
        require(source_snapshot(fresh["source"]["head"]) == fresh["source"], "source drift before publication")
        manifest = {"schema_version": "kaushik_replication_inputs_v1", "status": "inputs_complete", "source": fresh["source"],
            "binding": fresh["binding"], "reviewed_preflight": reviewed_identity,
            "input_preflight": reviewed, "caps": CAPS, "command": command,
            "inputs": frozen, "outputs": outputs, "metadata_health": health, "exclusions": counts, "support": support,
            "rows": {"source": total_input, "baseline_all_roles": eligible, "excluded": total_input-eligible,
                     "primary_taker": sum(row["rows"] for row in support if row["is_maker"] is False)},
            "analytic_anomalies": {"all_roles": sum(row["nonfinite_analytic_outcomes"] for row in support),
                                   "primary_taker": sum(row["nonfinite_analytic_outcomes"] for row in support if row["is_maker"] is False)},
            "estimation_gate": "Require zero nonfinite analytic outcomes in every selected convention; retain anomalies without silently dropping rows.",
            "publication_acceptance": "A matching acceptance.json with status inputs_reopened_accepted is required; manifest inputs_complete alone is insufficient.",
            "definitions": {"cutoff_utc": CUTOFF_ISO, "cutoff_seconds": CUTOFF_SECONDS,
                "observation": "archive published wallet-role row; physical multiplicity preserved; not certified native execution/action",
                "primary": "is_maker=false; BUY retains claim; SELL complements verified binary native token/P/Y",
                "quantity": "usdcSize/recorded_price", "capital": "quantity*normalized_P", "fees": "excluded gross execution amounts",
                "outcomes": "payoff=100*(Y-P); individual_return=100*(Y/P-1)",
                "resolution": "spine winning_outcome is common condition winner label; Y equals exact native token outcome-label agreement; complementary pair has one winning token",
                "source_locator": "source month plus original Parquet physical file_row_number, zero-based",
                "price": "normalized binary64; no rounding; exact fixed effects use stored P",
                "duration": "created_at fallback (start_date unavailable), native end_date; L>0, R>=0, t>=opening; baseline admits missing/after-end clocks",
                "cluster": "unique native event_slug; otherwise namespaced condition fallback; trade/token-map slugs unused",
                "filters": "strict 0<P<1, finite positive size, eventual binary payout; no bot/updown/lifecycle filters",
                "sports": "not admitted by this stage; separate complete-candidate and provider proof required"},
            "reconciliation": {"all_records_accounted": True, "archive_multiplicity_preserved": True,
                "no_join_fanout": True, "unique_reciprocal_binary_complements": True, "outputs_reopened": True},
            "environment": {"python": sys.version, "duckdb": duckdb.__version__, "platform": sys.platform},
            "wall_seconds": time.monotonic()-started}
        write_json(staging / "manifest.json", manifest)
        require(_directory_bytes(staging) <= CAPS["maximum_output_bytes"] and shutil.disk_usage(staging).free >= CAPS["minimum_free_bytes"], "final output/free-floor gate failed")
        con.close()
        con = None
        atomic_publish(staging, target)
        # Publication is not acceptance until the saved directory reopens.
        reopened = duckdb.connect()
        try:
            configure(reopened, target / "spill")
            create_analysis_view(reopened, target)
            require(reopened.execute("SELECT count(*) FROM analysis_base").fetchone()[0] == eligible, "post-publication row reconciliation failed")
            require(support_counts(reopened) == support, "post-publication support reconciliation failed")
            for relative, info in outputs.items():
                require(_output_info(target / relative) == info, "post-publication schema/hash drift")
        finally:
            reopened.close()
        write_json(target / "acceptance.json", {"schema_version": "kaushik_replication_input_acceptance_v1",
            "status": "inputs_reopened_accepted", "manifest_sha256": sha256(target / "manifest.json"),
            "rows": manifest["rows"], "output_schema_hash_count_support_reopened": True,
            "source_head": fresh["source"]["head"], "accepted_at_utc": datetime.now(timezone.utc).isoformat()})
        return manifest
    except BaseException as error:
        if staging.exists() and not (staging / "failure.json").exists():
            write_json(staging / "failure.json", {"status": "inputs_incomplete", "error_type": type(error).__name__,
                                                 "reason": str(error), "completed_months": list(counts)})
        raise
    finally:
        if con is not None:
            con.close()


def _output_info(path):
    item = pq.ParquetFile(path)
    return {"bytes": Path(path).stat().st_size, "sha256": sha256(path), "rows": item.metadata.num_rows,
            "schema": str(item.schema_arrow), "row_groups": item.metadata.num_row_groups}
