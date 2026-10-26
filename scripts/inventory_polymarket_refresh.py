"""Bounded source-readiness evidence for an EC2-only Polymarket refresh.

This command reads Parquet footers, not trade bodies. Footer declarations are
not independently observed execution extrema or a completeness certificate.
It makes no cutoff, deduplication, outcome, or scientific-sample decision.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
from typing import Any, Callable
import urllib.parse
import urllib.request


REPO = Path(__file__).resolve().parents[1]
DEFAULT_PATHS = {
    name: Path("/mnt/data/pipeline_data") / (name + ".parquet")
    for name in ("raw_events", "deduped_events", "resolved_trades",
                 "block_timestamps", "token_map", "gamma_markets", "resolved_markets")
}
LIMITS = {
    "maximum_files": 256,
    "maximum_directory_entries": 4096,
    "maximum_directory_depth": 4,
    "maximum_row_groups": 200_000,
    "maximum_single_footer_bytes": 128 * 1024**2,
    "maximum_total_footer_bytes": 512 * 1024**2,
    "maximum_manifest_bytes": 8 * 1024**2,
    "maximum_output_bytes": 16 * 1024**2,
    "maximum_rpc_response_bytes": 64 * 1024,
    "maximum_rpc_calls": 3,
}
EXTREMA_COLUMNS = {"block_number", "blockNumber", "timestamp", "block_timestamp"}
HEX_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")
HEX_HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")


class ReadinessError(ValueError):
    """Only fixed, non-sensitive machine codes cross the evidence boundary."""


def require(condition: bool, code: str) -> None:
    if not condition:
        raise ReadinessError(code)


@dataclass
class Budget:
    files: int = 0
    row_groups: int = 0
    footer_bytes: int = 0
    metadata_read_bytes: int = 0


def _stat_record(value: os.stat_result) -> dict[str, int]:
    return {"device": value.st_dev, "inode": value.st_ino,
            "mode": value.st_mode, "uid": value.st_uid, "gid": value.st_gid,
            "bytes": value.st_size, "mtime_ns": value.st_mtime_ns,
            "ctime_ns": value.st_ctime_ns}


def path_identity(path: Path) -> dict[str, Any]:
    """Capture both a requested link and its resolved target; exclude atime."""
    try:
        link = path.lstat()
    except FileNotFoundError:
        return {"status": "missing"}
    resolved = path.resolve(strict=False)
    result: dict[str, Any] = {
        "status": "present", "requested_path": str(path.absolute()),
        "resolved_path": str(resolved), "entry": _stat_record(link),
        "is_symlink": stat.S_ISLNK(link.st_mode),
    }
    if result["is_symlink"]:
        result["symlink_target"] = os.readlink(path)
    try:
        result["target"] = _stat_record(resolved.stat())
    except FileNotFoundError:
        result["status"] = "dangling_symlink"
    return result


def collect_parquet_files(path: Path) -> tuple[list[Path], dict[str, Any]]:
    """Enumerate only one explicitly supplied source, with bounded traversal."""
    initial = path_identity(path)
    require(initial["status"] == "present", "source_missing_or_dangling")
    target = Path(initial["resolved_path"])
    if target.is_file():
        require(target.suffix == ".parquet", "source_is_not_parquet")
        return [target], {"input": initial, "directories": []}
    require(target.is_dir(), "source_is_not_regular_file_or_directory")
    files: list[Path] = []
    directories: list[dict[str, Any]] = []
    entries_seen = 0

    def visit(folder: Path, depth: int) -> None:
        nonlocal entries_seen
        require(depth <= LIMITS["maximum_directory_depth"], "directory_depth_limit")
        directories.append({"path": str(folder), "identity": path_identity(folder)})
        entries = []
        with os.scandir(folder) as stream:
            for entry in stream:
                entries_seen += 1
                require(entries_seen <= LIMITS["maximum_directory_entries"], "directory_entry_limit")
                entries.append(entry)
        for entry in sorted(entries, key=lambda item: item.name):
            child = Path(entry.path)
            # Explicit top-level symlinks are recorded; nested directory links
            # are not followed into unrequested trees.
            require(not entry.is_symlink(), "nested_symlink_not_inspected")
            if entry.is_dir(follow_symlinks=False):
                visit(child, depth + 1)
            elif entry.is_file(follow_symlinks=False) and child.suffix == ".parquet":
                files.append(child)
                require(len(files) <= LIMITS["maximum_files"], "file_limit")

    visit(target, 0)
    require(bool(files), "no_parquet_files")
    return sorted(files), {"input": initial, "directories": directories}


def _stat_scalar(value: Any) -> Any:
    if type(value) is int or type(value) is bool:
        return value
    if type(value) is float:
        require(math.isfinite(value), "nonfinite_footer_statistic")
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    # Only clock/block columns are selected. Do not publish arbitrary byte or
    # text statistics if an unadmitted source uses a different representation.
    raise ReadinessError("unsupported_footer_statistic_type")


def column_extrema(metadata: Any, index: int) -> dict[str, Any]:
    chunks = [metadata.row_group(group).column(index)
              for group in range(metadata.num_row_groups)]
    minima, maxima = [], []
    missing_groups = 0
    all_null_groups = 0
    null_rows = 0
    null_count_known = True
    for group, chunk in enumerate(chunks):
        statistics = chunk.statistics
        rows = metadata.row_group(group).num_rows
        if statistics is not None and statistics.has_null_count:
            null_rows += statistics.null_count
        else:
            null_count_known = False
        if rows == 0:
            continue
        if statistics is not None and statistics.has_null_count and statistics.null_count == rows:
            all_null_groups += 1
            continue
        if statistics is None or not statistics.has_min_max:
            missing_groups += 1
            continue
        minima.append(_stat_scalar(statistics.min))
        maxima.append(_stat_scalar(statistics.max))
    complete = missing_groups == 0
    minimum = min(minima) if minima else None
    maximum = max(maxima) if maxima else None
    return {
        "row_groups": len(chunks), "groups_with_minmax": len(minima),
        "all_null_groups": all_null_groups, "groups_missing_minmax": missing_groups,
        "null_rows_declared": null_rows if null_count_known else None,
        "all_nonempty_nonnull_groups_have_minmax": complete,
        "available_group_minimum": minimum, "available_group_maximum": maximum,
        "declared_global_minimum": minimum if complete else None,
        "declared_global_maximum": maximum if complete else None,
        "independently_observed_exact_minimum": None,
        "independently_observed_exact_maximum": None,
        "semantics": "Footer-declared extrema only; missing row-group statistics leave global bounds unknown; no trade body was read.",
    }


def inspect_footer(path: Path, budget: Budget) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    before = path_identity(path)
    require(before["status"] == "present" and stat.S_ISREG(before["target"]["mode"]),
            "parquet_target_not_regular")
    require(budget.files < LIMITS["maximum_files"], "file_limit")
    size = before["target"]["bytes"]
    require(size >= 12, "invalid_parquet_size")
    budget.files += 1
    with path.open("rb") as stream:
        budget.metadata_read_bytes += 4
        require(stream.read(4) == b"PAR1", "invalid_parquet_header")
        stream.seek(-8, os.SEEK_END)
        budget.metadata_read_bytes += 8
        trailer = stream.read(8)
        require(len(trailer) == 8 and trailer[4:] == b"PAR1", "invalid_parquet_trailer")
        length = int.from_bytes(trailer[:4], "little")
        require(0 < length <= size - 12, "invalid_footer_length")
        require(length <= LIMITS["maximum_single_footer_bytes"], "single_footer_byte_limit")
        require(budget.footer_bytes + length <= LIMITS["maximum_total_footer_bytes"],
                "total_footer_byte_limit")
        # Reserve the payload before reading: malformed or subsequently changed
        # inputs still consume the shared byte/file budget.
        budget.footer_bytes += length
        budget.metadata_read_bytes += length
        stream.seek(-8 - length, os.SEEK_END)
        body = stream.read(length)
        require(len(body) == length, "short_footer_read")
    # The decoder receives only the footer and magic, so cannot scan a column
    # body even if its implementation changes its normal file-open behavior.
    metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + body + trailer))
    budget.row_groups += metadata.num_row_groups
    require(budget.row_groups <= LIMITS["maximum_row_groups"], "row_group_limit")
    schema = metadata.schema.to_arrow_schema()
    require(len(schema.names) == len(set(schema.names)), "duplicate_schema_field")
    extrema = {}
    for name in sorted(EXTREMA_COLUMNS.intersection(schema.names)):
        # Native refresh inputs are flat tables. Nested statistics indexing is
        # not interchangeable with top-level Arrow field indexing.
        arrow_index = schema.get_field_index(name)
        leaf_indices = [index for index in range(metadata.num_columns)
                        if metadata.schema.column(index).path == name]
        require(len(leaf_indices) == 1, "clock_column_not_flat_scalar")
        extrema[name] = {"arrow_type": str(schema.field(arrow_index).type),
                         "unit_and_time_source": "Not admitted by this readiness inventory.",
                         **column_extrema(metadata, leaf_indices[0])}
    require(before == path_identity(path), "input_changed_during_footer_read")
    return {
        "path": str(path), "identity": before, "bytes": size,
        "rows_declared": metadata.num_rows, "row_groups": metadata.num_row_groups,
        "schema": [{"name": field.name, "type": str(field.type), "nullable": field.nullable}
                   for field in schema],
        "footer_bytes": length, "footer_sha256": hashlib.sha256(body + trailer).hexdigest(),
        "metadata_read_bytes": length + 12, "extrema": extrema,
    }


def inspect_source(path: Path, budget: Budget) -> dict[str, Any]:
    initial = path_identity(path)
    if initial["status"] != "present":
        return {"status": "missing", "path": str(path), "identity": initial,
                "error_code": "source_missing_or_dangling"}
    files, topology = collect_parquet_files(path)
    records = [inspect_footer(item, budget) for item in files]
    final_files, final_topology = collect_parquet_files(path)
    require(files == final_files and topology == final_topology,
            "input_topology_changed_during_inventory")
    return {"status": "inspected", "path": str(path), "topology": topology,
            "file_count": len(records), "bytes": sum(item["bytes"] for item in records),
            "rows_declared": sum(item["rows_declared"] for item in records), "files": records}


def inspect_manifest(path: Path) -> dict[str, Any]:
    before = path_identity(path)
    if before["status"] != "present":
        return {"status": "missing", "path": str(path), "identity": before}
    require(stat.S_ISREG(before["target"]["mode"]), "manifest_not_regular")
    require(0 < before["target"]["bytes"] <= LIMITS["maximum_manifest_bytes"], "manifest_byte_limit")
    raw = path.read_bytes()
    require(before == path_identity(path), "manifest_changed_during_read")
    value = json.loads(raw, object_pairs_hook=unique_object,
                       parse_constant=lambda _: (_ for _ in ()).throw(ReadinessError("nonfinite_json")))
    require(type(value) is dict, "manifest_not_object")
    result = {"status": "inspected", "path": str(path), "identity": before,
              "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
              "manifest_values_copied": False}
    for key in ("schema_version", "status"):
        item = value.get(key)
        if type(item) is str and re.fullmatch(r"[A-Za-z0-9_ -]{1,100}", item):
            result["declared_" + key] = item
    return result


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def code_state(repo: Path = REPO) -> dict[str, Any]:
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo,
                              capture_output=True, text=True, timeout=10, check=True).stdout.strip()
        require(bool(re.fullmatch(r"[0-9a-f]{40}", head)), "invalid_git_head")
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=repo,
                               capture_output=True, text=True, timeout=10, check=True).stdout
        require(len(dirty) <= 1024**2, "git_status_byte_limit")
        return {"status": "inspected", "head": head, "tracked_worktree_clean": not bool(dirty),
                "tracked_dirty_entries": len(dirty.splitlines()),
                "untracked_files_checked": False}
    except Exception:
        return {"status": "unknown", "error_code": "git_state_unavailable"}


def resource_state(disk_path: Path = Path("/mnt/data")) -> dict[str, Any]:
    result: dict[str, Any] = {"disk_path": str(disk_path)}
    try:
        usage = shutil.disk_usage(disk_path)
        result["disk_bytes"] = {"total": usage.total, "used": usage.used, "free": usage.free}
    except OSError:
        result["disk_error_code"] = "disk_stat_unavailable"
    try:
        raw = Path("/proc/meminfo").read_text()
        values = {name: int(number) * 1024 for name, number in
                  re.findall(r"^(MemTotal|MemAvailable):\s+(\d+) kB$", raw, re.MULTILINE)}
        require(set(values) == {"MemTotal", "MemAvailable"}, "ram_fields_absent")
        result["ram_bytes"] = values
    except Exception:
        result["ram_error_code"] = "ram_stat_unavailable"
    return result


def load_rpc_endpoint(private_file: Path) -> tuple[str, str]:
    endpoint = os.environ.get("POLYGON_RPC_URL", "")
    source = "environment"
    if not endpoint:
        before = path_identity(private_file)
        require(before["status"] == "present" and not before["is_symlink"], "rpc_configuration_unavailable")
        entry = before["target"]
        require(stat.S_ISREG(entry["mode"]) and entry["uid"] == os.geteuid()
                and stat.S_IMODE(entry["mode"]) & 0o077 == 0,
                "rpc_private_file_permissions")
        require(0 < entry["bytes"] <= 8192, "rpc_configuration_size")
        endpoint = private_file.read_text().strip()
        require(before == path_identity(private_file), "rpc_configuration_changed")
        source = "private_file"
    require(len(endpoint) <= 8192 and not any(character.isspace() for character in endpoint),
            "rpc_configuration_invalid")
    parsed = urllib.parse.urlsplit(endpoint)
    require(parsed.scheme == "https" and bool(parsed.hostname) and not parsed.fragment
            and parsed.username is None and parsed.password is None,
            "rpc_configuration_invalid")
    return endpoint, source


def _http_rpc(endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(endpoint, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=20) as response:
        data = response.read(LIMITS["maximum_rpc_response_bytes"] + 1)
    require(len(data) <= LIMITS["maximum_rpc_response_bytes"], "rpc_response_byte_limit")
    return json.loads(data, object_pairs_hook=unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ReadinessError("rpc_nonfinite_json")))


def _quantity(value: Any) -> int:
    require(type(value) is str and bool(HEX_QUANTITY.fullmatch(value)), "rpc_invalid_hex_quantity")
    return int(value, 16)


def rpc_readiness(private_file: Path, transport: Callable = _http_rpc) -> dict[str, Any]:
    result: dict[str, Any] = {"status": "failed", "calls_attempted": 0,
                              "maximum_calls": LIMITS["maximum_rpc_calls"],
                              "endpoint_disclosed": False, "trade_logs_requested": False}
    try:
        endpoint, source = load_rpc_endpoint(private_file)
        result["configuration_source"] = source

        def call(method: str, params: list[Any]) -> Any:
            identifier = result["calls_attempted"] + 1
            require(identifier <= LIMITS["maximum_rpc_calls"], "rpc_call_limit")
            result["calls_attempted"] = identifier
            reply = transport(endpoint, {"jsonrpc": "2.0", "id": identifier,
                                         "method": method, "params": params})
            require(type(reply) is dict and set(reply) == {"jsonrpc", "id", "result"},
                    "rpc_invalid_reply_shape")
            require(reply["jsonrpc"] == "2.0" and type(reply["id"]) is int
                    and reply["id"] == identifier, "rpc_reply_id_mismatch")
            return reply["result"]

        chain_id = _quantity(call("eth_chainId", []))
        require(chain_id == 137, "rpc_wrong_chain")
        result["chain_id"] = chain_id
        block = _quantity(call("eth_blockNumber", []))
        require(block > 0, "rpc_nonpositive_block")
        header = call("eth_getBlockByNumber", [hex(block), False])
        require(type(header) is dict, "rpc_header_not_object")
        returned_block = _quantity(header.get("number"))
        timestamp = _quantity(header.get("timestamp"))
        require(returned_block == block, "rpc_header_number_mismatch")
        require(type(header.get("hash")) is str and bool(HEX_HASH.fullmatch(header["hash"])),
                "rpc_invalid_block_hash")
        require(0 < timestamp <= 253402300799, "rpc_invalid_header_timestamp")
        result.update(status="passed", requested_block=block, returned_block=returned_block,
                      block_hash=header["hash"], timestamp_unix_seconds=timestamp,
                      timestamp_utc=datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
                      semantics="One exact requested block header from the configured Polygon RPC; not historical coverage or log completeness.")
    except ReadinessError as exc:
        result["error_code"] = str(exc)
    except Exception:
        # Transport/decode/config exceptions can embed endpoint credentials.
        result["error_code"] = "rpc_transport_decode_or_configuration_failure"
    return result


def _safe_failure(exc: Exception) -> str:
    return str(exc) if isinstance(exc, ReadinessError) else "input_metadata_unavailable_or_invalid"


def build_inventory(paths: dict[str, Path], *, repair_manifest: Path | None = None,
                    check_rpc: bool = False, rpc_private_file: Path | None = None,
                    transport: Callable = _http_rpc,
                    disk_path: Path = Path("/mnt/data")) -> dict[str, Any]:
    require(set(paths) == set(DEFAULT_PATHS), "source_key_inventory_differs")
    require(all(isinstance(path, Path) for path in paths.values()), "source_paths_must_be_paths")
    before = {name: path_identity(path) for name, path in paths.items()}
    manifest_before = path_identity(repair_manifest) if repair_manifest is not None else None
    source_before = code_state()
    budget = Budget()
    records = {}
    for name, path in sorted(paths.items()):
        try:
            records[name] = inspect_source(path, budget)
        except Exception as exc:
            records[name] = {"status": "failed", "path": str(path), "error_code": _safe_failure(exc)}
    manifest = None
    if repair_manifest is not None:
        try:
            manifest = inspect_manifest(repair_manifest)
        except Exception as exc:
            manifest = {"status": "failed", "path": str(repair_manifest), "error_code": _safe_failure(exc)}
    rpc = (rpc_readiness(rpc_private_file or Path.home() / ".polygon_rpc_url", transport)
           if check_rpc else {"status": "not_requested", "calls_attempted": 0})
    identity_ok = True
    for name, path in paths.items():
        try:
            unchanged = before[name] == path_identity(path)
            entry = records[name]
            if entry["status"] == "inspected":
                files, topology = collect_parquet_files(path)
                unchanged = unchanged and topology == entry["topology"]
                unchanged = unchanged and [str(item) for item in files] == [item["path"] for item in entry["files"]]
                unchanged = unchanged and all(item["identity"] == path_identity(Path(item["path"]))
                                              for item in entry["files"])
            entry["final_identity_unchanged"] = unchanged
            identity_ok = identity_ok and unchanged
        except Exception:
            records[name]["final_identity_unchanged"] = False
            identity_ok = False
    if repair_manifest is not None:
        unchanged = manifest_before == path_identity(repair_manifest)
        manifest["final_identity_unchanged"] = unchanged
        identity_ok = identity_ok and unchanged
    source_after = code_state()
    source_unchanged = source_before == source_after
    complete = (identity_ok and source_unchanged and all(item["status"] == "inspected" for item in records.values())
                and (manifest is None or manifest["status"] == "inspected")
                and rpc["status"] in ("not_requested", "passed"))
    return {
        "schema_version": "polymarket_refresh_readiness_inventory_v1",
        "status": "readiness_inventory_complete" if complete else "readiness_inventory_incomplete",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Explicit-source footer/schema/resource inventory and optional three-call RPC readiness only.",
        "scientific_sample_decisions_made": False, "parquet_body_reads": 0,
        "limits": dict(LIMITS), "consumed": vars(budget), "inputs": records,
        "repair_manifest": manifest, "rpc_readiness": rpc,
        "code_before": source_before, "code_after": source_after,
        "code_state_unchanged": source_unchanged, "final_input_identity_unchanged": identity_ok,
        "resources": resource_state(disk_path),
        "limitations": ["Footer counts/statistics are producer declarations, not body-level validation.",
                        "Missing extrema/statistics are unknown, not zeros or inferred endpoint dates.",
                        "No native identity, timestamp coverage, market outcome, wallet or collection-completeness certification.",
                        "No cutoff or final paper cohort has been selected by this command.",
                        "File identity plus footer hash is not a full Parquet body-content hash."],
    }


def write_exclusive(path: Path, value: dict[str, Any]) -> None:
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    require(len(raw) <= LIMITS["maximum_output_bytes"], "output_byte_limit")
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New exclusive JSON path; parent must already exist")
    for name, path in DEFAULT_PATHS.items():
        parser.add_argument("--" + name.replace("_", "-"), type=Path, default=path)
    parser.add_argument("--repair-manifest", type=Path)
    parser.add_argument("--check-rpc", action="store_true")
    parser.add_argument("--rpc-private-file", type=Path, default=Path.home() / ".polygon_rpc_url")
    args = parser.parse_args(argv)
    sys.path.insert(0, str(REPO))
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        parser.error("Production inventory requires the canonical mounted EC2 checkout.")
    paths = {name: getattr(args, name) for name in DEFAULT_PATHS}
    require(not args.output.exists() and not args.output.is_symlink(), "output_already_exists")
    output = args.output.resolve()
    for source in [*paths.values(), *([args.repair_manifest] if args.repair_manifest else [])]:
        require(output != source.resolve() and source.resolve() not in output.parents,
                "output_overlaps_input")
    result = build_inventory(paths, repair_manifest=args.repair_manifest, check_rpc=args.check_rpc,
                             rpc_private_file=args.rpc_private_file)
    write_exclusive(args.output, result)
    print(json.dumps({"status": result["status"], "files_inspected": result["consumed"]["files"],
                      "parquet_body_reads": 0, "rpc_status": result["rpc_readiness"]["status"]}, sort_keys=True))
    return 0 if result["status"] == "readiness_inventory_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
