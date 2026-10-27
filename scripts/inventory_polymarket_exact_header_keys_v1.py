"""Inventory exact-header keys from a SHA-bound, literal Parquet source roster.

No source discovery, glob expansion, RPC, timestamp inference or final-universe
admission. Legacy bodies are projected, never fully hashed. Native payloads must
carry their existing SHA-256 proof and are rehashed within the frozen read cap.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from production_guard import require_production_host
from scripts import inventory_polymarket_refresh as shared

CAPS = {
    "roster_bytes": 64 * 1024**2, "files": 150_000,
    "footer_read_bytes": 2_000_000_000, "projected_compressed_read_bytes": 2_000_000_000,
    "native_payload_rehash_bytes": 64_000_000_000, "all_artifact_bytes": 1_000_000_000,
    "spill_bytes": 0, "duckdb_memory_limit": "24GB", "address_space_bytes": 48_000_000_000,
    "minimum_available_ram_bytes": 48_000_000_000, "threads": 4,
    "runtime_seconds": 2_400, "minimum_free_bytes": 20_000_000_000,
}
CONTROL_RESERVE = 2 * 1024**2
BATCH_ROWS = 65_536
AUDIT_BATCH_ROWS = 1_024
MAX_BLOCK = 2**63 - 1
SHA = re.compile(r"[0-9a-f]{64}")
STAT_FIELDS = {"device", "inode", "mode", "uid", "gid", "mtime_ns", "ctime_ns"}
SOURCE_FILES = ("production_guard.py", "scripts/inventory_polymarket_refresh.py",
                "scripts/inventory_polymarket_exact_header_keys_v1.py",
                "tests/test_inventory_polymarket_exact_header_keys_v1.py")
SCOPE = "Literal roster only; sparse occupied-block gaps are normal, not evidence of missing trades."


class InventoryBlocked(ValueError):
    """Fixed non-sensitive error codes only."""


def require(ok, code):
    if not ok:
        raise InventoryBlocked(code)


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def file_identity(path):
    info = Path(path).lstat()
    require(stat.S_ISREG(info.st_mode) and not Path(path).is_symlink(), "input_not_regular_file")
    return {"device": info.st_dev, "inode": info.st_ino, "mode": info.st_mode,
            "uid": info.st_uid, "gid": info.st_gid, "mtime_ns": info.st_mtime_ns,
            "ctime_ns": info.st_ctime_ns, "bytes": info.st_size}


def source_snapshot(expected_head):
    require(type(expected_head) is str and re.fullmatch(r"[0-9a-f]{40}", expected_head), "invalid_expected_head")
    state = shared.code_state()
    require(state.get("status") == "inspected" and state.get("head") == expected_head
            and state.get("tracked_worktree_clean") is True, "source_head_or_clean_state_differs")
    bindings = {}
    for name in SOURCE_FILES:
        result = subprocess.run(["git", "show", expected_head + ":" + name], cwd=REPO,
                                capture_output=True, timeout=10)
        path = REPO / name
        before = file_identity(path)
        require(before["bytes"] <= 1024**2, "source_file_byte_limit")
        raw = path.read_bytes()
        require(before == file_identity(path), "source_changed_during_read")
        require(result.returncode == 0 and result.stdout == raw, "source_dependency_differs_from_commit")
        bindings[name] = {"bytes": len(raw), "sha256": digest(raw)}
    return {"head": expected_head, "files": bindings, "tracked_worktree_clean": True}


def read_roster(path, expected_sha256):
    path = Path(path)
    require(type(expected_sha256) is str and SHA.fullmatch(expected_sha256), "invalid_roster_sha256")
    before = file_identity(path)
    require(0 < before["bytes"] <= CAPS["roster_bytes"], "roster_byte_limit")
    with path.open("rb") as stream:
        raw = stream.read(CAPS["roster_bytes"] + 1)
    require(len(raw) <= CAPS["roster_bytes"] and before == file_identity(path), "roster_changed_during_read")
    require(digest(raw) == expected_sha256, "roster_sha256_differs")
    try:
        value = json.loads(raw, object_pairs_hook=shared.unique_object,
                           parse_constant=lambda _: (_ for _ in ()).throw(InventoryBlocked("roster_nonfinite_json")))
    except Exception:
        raise InventoryBlocked("roster_invalid_json") from None
    require(type(value) is dict and set(value) == {"schema_version", "files"}
            and value["schema_version"] == "polymarket_exact_header_key_roster_v1", "roster_schema")
    files = value["files"]
    require(type(files) is list and 0 < len(files) <= CAPS["files"], "roster_file_count_limit")
    paths, physical = set(), set()
    for item in files:
        require(type(item) is dict and item.get("source_role") in {"legacy_raw", "native_raw"}, "source_role")
        fields = {"path", "source_role", "bytes", "identity", "footer_sha256"}
        if item["source_role"] == "native_raw":
            fields.add("payload_sha256")
        require(set(item) == fields, "source_entry_schema")
        require(type(item["path"]) is str, "source_path_type")
        candidate = Path(item["path"])
        require(candidate.is_absolute() and candidate.suffix == ".parquet"
                and str(candidate) == item["path"] and candidate.resolve() == candidate, "source_path_not_literal_absolute")
        require(item["path"] not in paths, "duplicate_source_path")
        paths.add(item["path"])
        require(type(item["bytes"]) is int and item["bytes"] >= 12, "source_byte_count_invalid")
        expected = item["identity"]
        require(type(expected) is dict and set(expected) == STAT_FIELDS
                and all(type(part) is int and part >= 0 for part in expected.values()), "source_identity_schema")
        require(stat.S_ISREG(expected["mode"]), "source_identity_not_regular")
        key = expected["device"], expected["inode"]
        require(key not in physical, "duplicate_physical_source")
        physical.add(key)
        require(type(item["footer_sha256"]) is str and SHA.fullmatch(item["footer_sha256"]), "source_footer_sha256")
        if item["source_role"] == "native_raw":
            require(type(item["payload_sha256"]) is str and SHA.fullmatch(item["payload_sha256"]), "native_payload_sha256")
    require(sum(item["bytes"] for item in files if item["source_role"] == "native_raw")
            <= CAPS["native_payload_rehash_bytes"], "planned_payload_rehash_byte_limit")
    return files, {"path": str(path.absolute()), "identity": before, "sha256": expected_sha256}


def reopen_identity(entry):
    actual = file_identity(Path(entry["path"]))
    require(actual == {**entry["identity"], "bytes": entry["bytes"]}, "source_identity_differs")


class Budget:
    def __init__(self, output, clock=time.monotonic):
        self.output, self.clock, self.started = output, clock, clock()
        self.footer_bytes = self.projected_bytes = self.payload_bytes = 0
        self.paths = set()

    def time(self):
        require(self.clock() - self.started < CAPS["runtime_seconds"], "runtime_limit")

    def charge(self, kind, amount):
        self.time()
        attribute, ceiling = {"footer": ("footer_bytes", CAPS["footer_read_bytes"]),
                              "projection": ("projected_bytes", CAPS["projected_compressed_read_bytes"]),
                              "payload": ("payload_bytes", CAPS["native_payload_rehash_bytes"])}[kind]
        require(type(amount) is int and amount >= 0 and getattr(self, attribute) + amount <= ceiling,
                kind + "_read_byte_limit")
        setattr(self, attribute, getattr(self, attribute) + amount)

    def retained(self):
        total = 0
        for path in self.output.iterdir():
            info = path.lstat()
            require(path.name in self.paths and stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                    "unexpected_output_artifact")
            total += info.st_size
        return total

    def space(self, extra=0, *, failure=False):
        if not failure:
            self.time()
        require(self.retained() + extra + (0 if failure else CONTROL_RESERVE) <= CAPS["all_artifact_bytes"],
                "artifact_byte_limit")
        require(shutil.disk_usage(self.output).free >= extra + CAPS["minimum_free_bytes"], "free_space_floor")

    def usage(self):
        return {"footer_bytes_read": self.footer_bytes, "projected_compressed_bytes_read": self.projected_bytes,
                "native_payload_bytes_rehashed": self.payload_bytes,
                "retained_bytes_before_terminal": self.retained(), "elapsed_seconds": self.clock() - self.started}


def read_footer(entry, budget):
    import pyarrow as pa
    import pyarrow.parquet as pq
    reopen_identity(entry)
    size = entry["bytes"]
    with Path(entry["path"]).open("rb", buffering=0) as stream:
        budget.charge("footer", 12)
        require(stream.read(4) == b"PAR1", "invalid_parquet_magic")
        stream.seek(-8, os.SEEK_END)
        trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "invalid_parquet_trailer")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= size - 12, "invalid_footer_length")
        budget.charge("footer", length)
        stream.seek(-8 - length, os.SEEK_END)
        raw = stream.read(length)
        require(len(raw) == length and digest(raw + trailer) == entry["footer_sha256"], "source_footer_differs")
    metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + raw + trailer))
    schema = metadata.schema.to_arrow_schema()
    require(len(schema.names) == len(set(schema.names)), "duplicate_parquet_column")
    native = entry["source_role"] == "native_raw"
    require("block_number" in schema.names and schema.field("block_number").type == (pa.uint64() if native else pa.int64()),
            "source_block_schema_differs")
    columns = ["block_number"]
    if native:
        require("blockHash" in schema.names and schema.field("blockHash").type == pa.string(), "source_native_hash_schema_differs")
        columns.append("blockHash")
    ranges = []
    for name in columns:
        indices = [index for index in range(metadata.num_columns) if metadata.schema.column(index).path == name]
        require(len(indices) == 1, "source_projection_not_flat")
        for group in range(metadata.num_row_groups):
            column = metadata.row_group(group).column(indices[0])
            require(column.file_path in (None, ""), "external_parquet_column_file")
            starts = [part for part in (column.dictionary_page_offset, column.data_page_offset) if part is not None and part >= 4]
            require(starts and column.total_compressed_size >= 0, "invalid_column_extent")
            first, count = min(starts), column.total_compressed_size
            require(first + count <= size - length - 8, "column_extent_outside_body")
            if count:
                ranges.append((first, first + count))
    ranges.sort()
    require(all(previous[1] <= current[0] for previous, current in zip(ranges, ranges[1:])), "overlapping_projected_column_extents")
    reopen_identity(entry)
    return metadata, {"rows_declared": metadata.num_rows, "row_groups": metadata.num_row_groups,
                      "footer_read_bytes_per_pass": length + 12, "projected_columns": columns,
                      "projected_compressed_bytes": sum(last - first for first, last in ranges), "ranges": ranges}


def rehash_native(entry, budget):
    if entry["source_role"] != "native_raw":
        return False
    reopen_identity(entry)
    hasher, count = hashlib.sha256(), 0
    with Path(entry["path"]).open("rb") as stream:
        while count < entry["bytes"]:
            size = min(1024**2, entry["bytes"] - count)
            budget.charge("payload", size)
            raw = stream.read(size)
            require(len(raw) == size, "native_payload_short_read")
            hasher.update(raw)
            count += len(raw)
    require(hasher.hexdigest() == entry["payload_sha256"], "native_payload_sha256_differs")
    reopen_identity(entry)
    return True


class ProjectedReader(io.RawIOBase):
    """Refuse reads outside the footer-admitted block/hash column extents."""
    def __init__(self, entry, ranges, budget):
        super().__init__()
        self.stream = Path(entry["path"]).open("rb", buffering=0)
        self.ranges, self.budget = ranges, budget

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.stream.tell()

    def seek(self, offset, whence=os.SEEK_SET):
        return self.stream.seek(offset, whence)

    def read(self, size=-1):
        first = self.stream.tell()
        require(type(size) is int and size >= 0, "unbounded_projection_read")
        if size:
            require(any(lower <= first and first + size <= upper for lower, upper in self.ranges),
                    "unadmitted_projection_read")
            self.budget.charge("projection", size)
        raw = self.stream.read(size)
        require(len(raw) == size, "projection_short_read")
        return raw

    def readinto(self, buffer):
        raw = self.read(len(buffer))
        buffer[:len(raw)] = raw
        return len(raw)

    def close(self):
        if not self.closed:
            self.stream.close()
        super().close()


def atomic_publish(temporary, destination):
    library = ctypes.CDLL(None, use_errno=True)
    if hasattr(library, "renameat2"):
        code = library.renameat2(-100, os.fsencode(temporary), -100, os.fsencode(destination), 1)
    elif hasattr(library, "renamex_np"):
        code = library.renamex_np(os.fsencode(temporary), os.fsencode(destination), 4)
    else:
        raise InventoryBlocked("atomic_publication_unavailable")
    require(code == 0, "atomic_publication_refused")


def fsync_directory(path):
    """Seal staged files and prior renames before the final commit syscall."""
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class CheckedOutput(io.RawIOBase):
    def __init__(self, budget, name):
        super().__init__()
        self.budget, self.path = budget, budget.output / name
        require(not self.path.exists() and not self.path.is_symlink(), "output_exists")
        budget.paths.add(name)
        self.stream = self.path.open("xb", buffering=0)

    def writable(self):
        return True

    def tell(self):
        return self.stream.tell()

    def write(self, raw):
        self.budget.space(len(raw))
        count = self.stream.write(raw)
        require(count == len(raw), "short_output_write")
        return count

    def flush(self):
        if not self.closed and not self.stream.closed:
            self.stream.flush()

    def close(self):
        if not self.closed:
            self.stream.flush()
            os.fsync(self.stream.fileno())
            self.stream.close()
        super().close()


def hash_output(path, budget):
    hasher, size = hashlib.sha256(), 0
    with path.open("rb") as stream:
        while raw := stream.read(1024**2):
            budget.time()
            hasher.update(raw)
            size += len(raw)
    return {"path": path.name, "bytes": size, "sha256": hasher.hexdigest()}


def publish_json(budget, name, value, *, failure=False):
    raw = json_bytes(value)
    budget.space(len(raw), failure=failure)
    target, pending = budget.output / name, budget.output / ("." + name + ".pending")
    require(not target.exists() and not target.is_symlink() and not pending.exists(), "output_exists")
    budget.paths.add(pending.name)
    with pending.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    require(pending.read_bytes() == raw, "control_reopen_differs")
    if not failure:
        budget.time()
    fsync_directory(budget.output)
    if not failure:
        budget.time()
    # Especially for COMPLETE, the no-replace rename is the last fallible
    # artifact operation. All directory/file flushes and reopenings precede it.
    atomic_publish(pending, target)
    budget.paths.remove(pending.name)
    budget.paths.add(name)
    return {"path": name, "bytes": len(raw), "sha256": digest(raw)}


def initialize_database():
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{CAPS['duckdb_memory_limit']}'")
    con.execute(f"SET threads={CAPS['threads']}")
    con.execute("SET temp_directory=''")
    con.execute("SET max_temp_directory_size='0B'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("CREATE TABLE required_keys(block_number BIGINT PRIMARY KEY,min_hash VARCHAR,max_hash VARCHAR)")
    con.execute("CREATE TABLE native_source_keys(source_index INTEGER,block_number BIGINT,PRIMARY KEY(source_index,block_number))")
    return con


def aggregate_batch(con, table, plans, budget):
    budget.time()
    con.register("incoming", table)
    try:
        con.execute(f"""CREATE OR REPLACE TEMP VIEW normalized AS SELECT *,
            block_number IS NOT NULL AND block_number>0 AND block_number<={MAX_BLOCK} block_valid,
            source_role='legacy_raw' OR (block_hash IS NOT NULL AND
                regexp_full_match(block_hash,'0x[0-9a-fA-F]{{64}}')) hash_valid
            FROM incoming""")
        counts = con.execute(f"""SELECT source_index,count(*),
            count(*) FILTER(WHERE block_number IS NULL),
            count(*) FILTER(WHERE block_number IS NOT NULL AND NOT block_valid),
            count(*) FILTER(WHERE source_role='native_raw' AND block_hash IS NULL),
            count(*) FILTER(WHERE source_role='native_raw' AND block_hash IS NOT NULL AND NOT hash_valid),
            count(*) FILTER(WHERE block_valid AND hash_valid)
            FROM normalized GROUP BY source_index""").fetchall()
        names = ("rows_scanned", "null_block_rows", "invalid_block_rows", "null_hash_rows", "invalid_hash_rows", "valid_rows")
        for index, *values in counts:
            for name, value in zip(names, values):
                plans[index][name] += value
        con.execute("""CREATE OR REPLACE TEMP TABLE batch_keys AS
            SELECT CAST(block_number AS BIGINT) block_number,min(lower(block_hash)) min_hash,max(lower(block_hash)) max_hash
            FROM normalized WHERE block_valid AND hash_valid GROUP BY block_number""")
        con.execute("""INSERT INTO required_keys SELECT * FROM batch_keys ON CONFLICT(block_number) DO UPDATE SET
            min_hash=least(required_keys.min_hash,excluded.min_hash),max_hash=greatest(required_keys.max_hash,excluded.max_hash)""")
        con.execute("""INSERT INTO native_source_keys SELECT DISTINCT source_index,CAST(block_number AS BIGINT)
            FROM normalized WHERE source_role='native_raw' AND block_valid AND hash_valid ON CONFLICT DO NOTHING""")
    finally:
        con.execute("DROP VIEW IF EXISTS normalized")
        con.unregister("incoming")


def scan_sources(con, entries, plans, budget):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    pending, pending_rows = [], 0
    for index, (entry, plan) in enumerate(zip(entries, plans)):
        budget.time()
        plan["payload_sha256_verified"] = rehash_native(entry, budget)
        metadata, reopened = read_footer(entry, budget)
        require(all(reopened[key] == plan[key] for key in reopened), "source_footer_plan_changed")
        if metadata.num_rows == 0:
            # Some Arrow versions request offset zero for empty column chunks.
            # The admitted footer already proves there are no body rows to read.
            reopen_identity(entry)
            continue
        with ProjectedReader(entry, plan["ranges"], budget) as stream:
            parquet = pq.ParquetFile(stream, metadata=metadata, pre_buffer=False, buffer_size=0)
            for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=plan["projected_columns"], use_threads=False):
                count = batch.num_rows
                block = pc.cast(batch.column(batch.schema.get_field_index("block_number")), pa.decimal128(20, 0))
                native = entry["source_role"] == "native_raw"
                hashes = batch.column(batch.schema.get_field_index("blockHash")) if native else pa.nulls(count, pa.string())
                table = pa.table({"source_index": pc.fill_null(pa.nulls(count, pa.int32()), index),
                                  "source_role": pc.fill_null(pa.nulls(count, pa.string()), entry["source_role"]),
                                  "block_number": block, "block_hash": hashes})
                offset = 0
                while offset < count:
                    take = min(count - offset, BATCH_ROWS - pending_rows)
                    pending.append(table.slice(offset, take))
                    pending_rows += take
                    offset += take
                    if pending_rows == BATCH_ROWS:
                        aggregate_batch(con, pa.concat_tables(pending), plans, budget)
                        pending, pending_rows = [], 0
        reopen_identity(entry)
    if pending:
        aggregate_batch(con, pa.concat_tables(pending), plans, budget)
    for index, count in con.execute("""SELECT n.source_index,count(*) FROM native_source_keys n JOIN required_keys k
            USING(block_number) WHERE k.min_hash IS DISTINCT FROM k.max_hash GROUP BY n.source_index""").fetchall():
        plans[index]["conflicting_hash_blocks"] = count


def audit_schema():
    import pyarrow as pa
    return pa.schema([("source_index", pa.int32()), ("path", pa.string()), ("source_role", pa.string()),
                      ("bytes", pa.int64()), ("identity_json", pa.string()), ("footer_sha256", pa.string()),
                      ("payload_sha256", pa.string()), ("payload_sha256_verified", pa.bool_()),
                      ("rows_declared", pa.int64()), ("rows_scanned", pa.int64()), ("valid_rows", pa.int64()),
                      ("null_block_rows", pa.int64()), ("invalid_block_rows", pa.int64()),
                      ("null_hash_rows", pa.int64()), ("invalid_hash_rows", pa.int64()),
                      ("conflicting_hash_blocks", pa.int64()), ("projected_compressed_bytes", pa.int64())])


def audit_batches(entries, plans):
    import pyarrow as pa
    schema = audit_schema()
    for first in range(0, len(entries), AUDIT_BATCH_ROWS):
        rows = []
        for index in range(first, min(first + AUDIT_BATCH_ROWS, len(entries))):
            entry, plan = entries[index], plans[index]
            row = {name: plan[name] for name in schema.names if name in plan}
            row.update(source_index=index, path=entry["path"], source_role=entry["source_role"], bytes=entry["bytes"],
                       identity_json=json_bytes(entry["identity"]).decode().strip(), footer_sha256=entry["footer_sha256"],
                       payload_sha256=entry.get("payload_sha256"))
            rows.append(row)
        yield pa.RecordBatch.from_pylist(rows, schema=schema)


def write_parquet(budget, name, schema, batches):
    import pyarrow.parquet as pq
    pending = "." + name + ".pending"
    with CheckedOutput(budget, pending) as stream:
        with pq.ParquetWriter(stream, schema, compression="zstd") as writer:
            for batch in batches:
                writer.write_batch(batch)
    path = budget.output / pending
    bound = hash_output(path, budget)
    target = budget.output / name
    require(not target.exists() and not target.is_symlink(), "output_exists")
    budget.time()
    fsync_directory(budget.output)
    budget.time()
    atomic_publish(path, target)
    budget.paths.remove(pending)
    budget.paths.add(name)
    return {**bound, "path": name}


def compare_batches(expected, actual, budget):
    sentinel = object()
    left, right = next(expected, sentinel), next(actual, sentinel)
    li = ri = count = 0
    while left is not sentinel and right is not sentinel:
        budget.time()
        require(left.schema == right.schema, "published_schema_differs")
        take = min(left.num_rows - li, right.num_rows - ri)
        require(left.slice(li, take).equals(right.slice(ri, take)), "published_rows_differ")
        count += take
        li, ri = li + take, ri + take
        if li == left.num_rows:
            left, li = next(expected, sentinel), 0
        if ri == right.num_rows:
            right, ri = next(actual, sentinel), 0
    require(left is sentinel and right is sentinel, "published_row_count_differs")
    return count


def run_inventory(roster, roster_sha256, expected_head, output, *, clock=time.monotonic):
    import pyarrow as pa
    import pyarrow.parquet as pq
    output, roster = Path(output), Path(roster)
    require(output.is_absolute() and output.parent.is_dir() and output.parent.resolve() == output.parent
            and not output.exists() and not output.is_symlink(), "output_not_new_absolute_directory")
    require(output != roster.resolve() and output not in roster.resolve().parents, "output_overlaps_roster")
    entries, roster_binding = read_roster(roster, roster_sha256)
    source = source_snapshot(expected_head)
    for entry in entries:
        require(output != Path(entry["path"]) and output not in Path(entry["path"]).parents, "output_overlaps_source")
    require(shutil.disk_usage(output.parent).free >= CAPS["all_artifact_bytes"] + CAPS["minimum_free_bytes"], "free_space_floor")
    output.mkdir(mode=0o700)
    budget, con, plans = Budget(output, clock), None, []
    result = {"schema_version": "polymarket_exact_header_key_inventory_v1", "status": "blocked",
              "scope": SCOPE, "literal_roster_only": True, "final_universe_admitted": False,
              "historical_v3_admitted": False, "roster": roster_binding, "source": source, "caps": CAPS}
    try:
        start = publish_json(budget, "START.json", result)
        for entry in entries:
            metadata, plan = read_footer(entry, budget)
            del metadata
            plan.update({name: 0 for name in ("rows_scanned", "valid_rows", "null_block_rows", "invalid_block_rows",
                                           "null_hash_rows", "invalid_hash_rows", "conflicting_hash_blocks")})
            plan["payload_sha256_verified"] = False
            plans.append(plan)
        # Three metadata-only passes: admission, projected-reader construction,
        # and final independent source reopening. No body is read before this gate.
        planned = {"footer_read_bytes": 3 * sum(item["footer_read_bytes_per_pass"] for item in plans),
                   "projected_compressed_read_bytes": sum(item["projected_compressed_bytes"] for item in plans),
                   "native_payload_rehash_bytes": sum(item["bytes"] for item in entries if item["source_role"] == "native_raw")}
        for key, value in planned.items():
            require(value <= CAPS[key], "planned_" + key + "_limit")
        result["planned_reads"] = planned
        publish_json(budget, "PREFLIGHT.json", {"status": "literal_roster_body_reads_admitted", "planned_reads": planned,
                                               "files": len(entries), "rows_declared": sum(item["rows_declared"] for item in plans)})
        con = initialize_database()
        scan_sources(con, entries, plans, budget)
        audit_bound = write_parquet(budget, "source_audit.parquet", audit_schema(), audit_batches(entries, plans))
        audit_count = compare_batches(iter(audit_batches(entries, plans)),
                                     iter(pq.ParquetFile(output / audit_bound["path"]).iter_batches(batch_size=AUDIT_BATCH_ROWS)), budget)
        require(audit_count == len(entries), "source_audit_count_differs")
        failure_counts = {key: sum(plan[key] for plan in plans) for key in
                          ("null_block_rows", "invalid_block_rows", "null_hash_rows", "invalid_hash_rows", "conflicting_hash_blocks")}
        result["source_audit"] = audit_bound
        result["failure_counts"] = failure_counts
        result["global_conflicting_hash_blocks"] = con.execute(
            "SELECT count(*) FROM required_keys WHERE min_hash IS DISTINCT FROM max_hash").fetchone()[0]
        result["rows_declared"] = sum(plan["rows_declared"] for plan in plans)
        result["rows_scanned"] = sum(plan["rows_scanned"] for plan in plans)
        require(all(plan["rows_declared"] == plan["rows_scanned"] for plan in plans), "source_row_count_differs")
        require(all(value == 0 for value in failure_counts.values()), "invalid_or_conflicting_source_rows")
        sql = "SELECT block_number,min_hash expected_block_hash FROM required_keys ORDER BY block_number"
        reader = con.execute(sql).fetch_record_batch(BATCH_ROWS)
        keys_bound = write_parquet(budget, "required_keys.parquet", reader.schema, reader)
        expected = con.execute(sql).fetch_record_batch(BATCH_ROWS)
        reopened_keys = pq.ParquetFile(output / keys_bound["path"])
        require(reopened_keys.schema_arrow == expected.schema, "published_schema_differs")
        key_count = compare_batches(iter(expected), iter(reopened_keys.iter_batches(batch_size=BATCH_ROWS)), budget)
        require(key_count == con.execute("SELECT count(*) FROM required_keys").fetchone()[0], "distinct_key_count_differs")
        for entry, plan in zip(entries, plans):
            _, final = read_footer(entry, budget)
            require(all(final[key] == plan[key] for key in final), "source_footer_changed_at_finalization")
        for entry in entries:
            reopen_identity(entry)
        final_entries, final_binding = read_roster(roster, roster_sha256)
        require(final_binding == roster_binding and final_entries == entries, "roster_changed_during_run")
        require(source_snapshot(expected_head) == source, "source_changed_during_run")
        require(hash_output(output / audit_bound["path"], budget) == audit_bound
                and hash_output(output / keys_bound["path"], budget) == keys_bound, "published_output_changed")
        require(hash_output(output / start["path"], budget) == start, "start_receipt_changed")
        con.close()
        con = None
        result.update(status="literal_header_key_inventory_complete", required_keys=key_count, source_files=len(entries),
                      required_keys_parquet=keys_bound, source_inputs_reopened=True, roster_reopened=True,
                      source_reopened=True, usage=budget.usage(), completed_utc=datetime.now(timezone.utc).isoformat())
        manifest = publish_json(budget, "manifest.json", result)
        require(hash_output(output / manifest["path"], budget) == manifest, "manifest_reopen_differs")
        publish_json(budget, "COMPLETE.json", {"schema_version": "polymarket_exact_header_key_inventory_completion_v1",
                     "status": result["status"], "manifest": manifest, "literal_roster_only": True,
                     "final_universe_admitted": False})
    except BaseException as exc:
        result.update(status="blocked", error_code=str(exc) if isinstance(exc, InventoryBlocked) else
                      "interrupted" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "inventory_io_or_resource_failure",
                      files_footer_admitted=len(plans), usage=budget.usage())
        try:
            publish_json(budget, "FAILED.json", result, failure=True)
        except Exception:
            result["failure_receipt_published"] = False
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
    finally:
        if con is not None:
            con.close()
    return result


@contextmanager
def process_limits():
    old = resource.getrlimit(resource.RLIMIT_AS)
    maximum = CAPS["address_space_bytes"] if old[0] < 0 else min(old[0], CAPS["address_space_bytes"])
    previous = signal.getsignal(signal.SIGALRM)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (maximum, old[1]))
        signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(InventoryBlocked("runtime_limit")))
        signal.alarm(CAPS["runtime_seconds"])
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
        resource.setrlimit(resource.RLIMIT_AS, old)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roster", required=True, type=Path)
    parser.add_argument("--roster-sha256", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    require_production_host()  # Before roster/source reads or output writes.
    try:
        require(Path(sys.executable) == Path("/home/ubuntu/venv/bin/python"), "production_interpreter_differs")
        require(args.output_dir.is_absolute() and args.output_dir.parent == Path("/mnt/data/runs"), "production_output_invalid")
        require(shared.resource_state()["ram_bytes"]["MemAvailable"] >= CAPS["minimum_available_ram_bytes"], "insufficient_available_ram")
        for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            os.environ[name] = "1"
        with process_limits():
            result = run_inventory(args.roster, args.roster_sha256, args.expected_head, args.output_dir)
    except Exception:
        parser.error("Exact-header inventory admission failed; no completion published.")
    print(json.dumps({"status": result["status"], "required_keys": result.get("required_keys"),
                      "final_universe_admitted": False}, sort_keys=True))
    return 0 if result["status"] == "literal_header_key_inventory_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
