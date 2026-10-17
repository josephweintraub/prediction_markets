"""Pure admission of the two inspected Kalshi source manifest formats."""
from __future__ import annotations

import csv
from datetime import date, timedelta
import hashlib
import io
from pathlib import PurePosixPath
import re

from .staging import ManifestEntry


TRADE_MANIFEST_SHA256 = "37aae4a94b1125cea81eb2222caba0daaa089eb9c241a0e6dd90bb0144fdf0ee"
METADATA_MANIFEST_SHA256 = "0ad2a1c6167f5171a5323e3bee021a27ae58516ee68ca23aab64d10ca5683ecf"
METADATA_PREFIX = "contract_matching/runs/markets_meta_all_v2/"
FIRST_DAY, LAST_DAY = date(2021, 6, 30), date(2026, 10, 1)
DOCUMENTED_EMPTY_DAYS = (date(2021, 7, 3),)


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _read(raw, expected_sha256, fields):
    _require(isinstance(raw, bytes) and len(raw) <= 1_000_000, "bounded manifest bytes required")
    _require(hashlib.sha256(raw).hexdigest() == expected_sha256, "source manifest SHA-256 differs")
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8"), newline=""))
    _require(reader.fieldnames == list(fields), "source manifest header differs")
    rows = list(reader)
    _require(rows and all(None not in row and all(value is not None for value in row.values()) for row in rows),
             "source manifest has malformed rows")
    return rows


def parse_trade_manifest(raw, *, expected_sha256=TRADE_MANIFEST_SHA256):
    """Select only exact documented daily trade paths, never quote inputs."""
    rows = _read(raw, expected_sha256, ("file", "bytes", "sha256"))
    _require(len({row["file"] for row in rows}) == len(rows), "duplicate source manifest paths")
    entries = []
    for row in rows:
        path = row["file"]
        if path.startswith("trades_all/"):
            match = re.fullmatch(r"trades_all/dt=(\d{4}-\d{2}-\d{2})/part-000\.parquet", path)
            _require(match is not None, "unexpected daily trade path")
            parsed = date.fromisoformat(match.group(1))
            _require(parsed.isoformat() == match.group(1), "noncanonical source day")
            entries.append(ManifestEntry(path, int(row["bytes"]), row["sha256"]))
        else:
            _require(re.fullmatch(r"bid_ask_1m/month=\d{4}-\d{2}/part-000\.parquet", path) is not None,
                     "unadmitted non-trade source manifest path")
    _require(entries, "daily trade inventory is empty")
    return tuple(sorted(entries, key=lambda entry: entry.path))


def validate_trade_days(entries, *, first_day=FIRST_DAY, last_day=LAST_DAY,
                        documented_empty_days=DOCUMENTED_EMPTY_DAYS):
    """Freeze inventory coverage; absent days are not inferred to be empty."""
    _require(first_day <= last_day, "invalid inventory date window")
    actual = [date.fromisoformat(entry.path.split("dt=", 1)[1].split("/", 1)[0]) for entry in entries]
    _require(len(actual) == len(set(actual)), "duplicate daily trade files")
    empty = set(documented_empty_days)
    _require(all(first_day <= day <= last_day for day in empty), "documented empty day outside inventory")
    expected = {first_day + timedelta(days=i) for i in range((last_day-first_day).days + 1)} - empty
    _require(set(actual) == expected, "trade inventory date coverage differs")
    return {"files": len(entries), "bytes": sum(entry.size_bytes for entry in entries),
            "first_day": first_day.isoformat(), "last_day": last_day.isoformat(),
            "documented_empty_days": sorted(day.isoformat() for day in empty),
            "maximum_shard_bytes": max(entry.size_bytes for entry in entries)}


def parse_metadata_manifest(raw, *, expected_sha256=METADATA_MANIFEST_SHA256):
    """Bind the inspected S3-relative paths to unique Dropbox basenames."""
    rows = _read(raw, expected_sha256, ("path", "bytes", "rows", "sha256"))
    entries, counts, seen = [], {}, set()
    for row in rows:
        path = row["path"]
        _require(path.startswith(METADATA_PREFIX), "metadata source prefix differs")
        name = path.removeprefix(METADATA_PREFIX)
        _require(PurePosixPath(name).name == name and re.fullmatch(r"(?:b[12]|f[12]|v)_\d+\.parquet", name),
                 "metadata path is not a documented flat shard")
        _require(name not in seen, "metadata basename collision")
        seen.add(name)
        entry = ManifestEntry("markets_metadata/" + name, int(row["bytes"]), row["sha256"])
        count = int(row["rows"])
        _require(count > 0, "positive metadata footer count required")
        entries.append(entry)
        counts[entry.path] = count
    return tuple(sorted(entries, key=lambda entry: entry.path)), counts
