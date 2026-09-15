"""Collect, match, and validate sport-specific event phase timing."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
)

from .contracts import SPORT_CONFIGS, get_sport_config
from .provider_extractors import (
    CompetitionRecord,
    ProviderDataError,
    derive_tennis_elapsed_thirds,
    extract_standard_wallclock_boundaries,
    extract_ufc_core_wallclock_boundaries,
    flatten_espn_standard,
    flatten_espn_tennis,
    flatten_espn_ufc,
    match_name_pair,
    names_match,
    normalize_name,
)


BASE_URL = "https://site.api.espn.com"
CORE_UFC = "https://sports.core.api.espn.com/v2/sports/mma/leagues/ufc"
TENNIS_ARCHIVE = (
    "https://raw.githubusercontent.com/Aneeshers/tennis-sackmann-archive/"
    "main/{tour}/{tour}_matches_{year}.csv"
)
COLLEGE_GROUPS = {"cbb": "50", "cfb": "80"}


def _scoreboard_url(sport: str, day: str) -> str:
    config = get_sport_config(sport)
    query = f"dates={day}&limit=200"
    if sport in COLLEGE_GROUPS:
        query += f"&groups={COLLEGE_GROUPS[sport]}"
    return f"{BASE_URL}{config.espn_scoreboard_path}?{query}"


def _fetch_bytes(url: str, retries: int = 5) -> bytes:
    request = urllib.request.Request(url)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if attempt + 1 == retries:
                raise
            time.sleep(min(2 ** attempt, 16))
    raise RuntimeError("unreachable")


def _write_fetched(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _download_jobs(jobs: Iterable[tuple[str, Path]], workers: int = 20) -> None:
    materialized = list(jobs)
    if not materialized:
        return
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_fetch_bytes, url): (url, path) for url, path in materialized}
        for future in as_completed(futures):
            url, path = futures[future]
            try:
                _write_fetched(path, future.result())
            except Exception as exc:  # retained in one compact failure, never silently skipped
                errors.append(f"{url}: {type(exc).__name__}: {exc}")
    if errors:
        raise RuntimeError(f"Provider collection failed for {len(errors)} resources: {errors[:10]}")


def _load_json(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ProviderDataError(f"Provider payload must be an object: {path}")
    return value


def _rows(con: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, Any]]:
    cursor = con.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _scoreboard_records(sport: str, payload: Mapping[str, Any]) -> tuple[CompetitionRecord, ...]:
    if sport in {"atp", "wta"}:
        rows = flatten_espn_tennis(payload)
        target = "men's singles" if sport == "atp" else "women's singles"
        return tuple(
            row for row in rows
            if (row.grouping_name or "").strip().casefold() == target
        )
    if sport == "ufc":
        return flatten_espn_ufc(payload)
    return flatten_espn_standard(payload)


def _completed_result_matches(candidate: Mapping[str, Any], record: CompetitionRecord) -> bool:
    if record.status_state != "post":
        return False
    mapping = match_name_pair(
        (candidate["participant_1"], candidate["participant_2"]),
        tuple(item.name for item in record.competitors),
    )
    if mapping is None:
        return False
    winners = [item for item in record.competitors if item.winner is True]
    undecided = [item for item in record.competitors if item.winner is None]
    result = str(candidate["result_label"])
    if candidate["sport"] == "epl":
        if undecided:
            return False
        if len(winners) == 0:
            return result.casefold() == "draw"
        return len(winners) == 1 and names_match(result, winners[0].name)
    return len(winners) == 1 and not undecided and names_match(result, winners[0].name)


def _candidate_match(
    candidate: Mapping[str, Any], records: Iterable[CompetitionRecord]
) -> tuple[CompetitionRecord | None, str | None]:
    pair = (candidate["participant_1"], candidate["participant_2"])
    matches = []
    for record in records:
        if match_name_pair(pair, tuple(item.name for item in record.competitors)) is None:
            continue
        if candidate["sport"] in {"atp", "wta"}:
            delta = abs((record.scheduled_start_utc.date() - candidate["market_date"]).days)
            # Tennis market slugs can be dated when the tournament market is
            # listed, several days before the scheduled match. The exact pair,
            # completed result, and unique competition remain mandatory.
            if delta > 7:
                continue
        matches.append(record)
    if not matches:
        return None, "no_provider_pair_match"
    completed = [row for row in matches if _completed_result_matches(candidate, row)]
    if not completed:
        return None, "provider_result_mismatch_or_nonfinal"
    by_competition = {row.competition_id: row for row in completed}
    if len(by_competition) != 1:
        return None, "ambiguous_provider_pair_match"
    return next(iter(by_competition.values())), None


def _provider_index(source: Path) -> dict[str, Any]:
    records = []
    digest = hashlib.sha256()
    for path in sorted(item for item in source.rglob("*") if item.is_file()):
        relative = path.relative_to(source).as_posix()
        data = path.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        record = f"{relative}\t{len(data)}\t{sha}\n"
        digest.update(record.encode("utf-8"))
        records.append({"path": relative, "bytes": len(data), "sha256": sha})
    return {
        "schema_version": 1,
        "method": "sha256_of_sorted_path_bytes_sha256_records",
        "file_count": len(records),
        "total_bytes": sum(row["bytes"] for row in records),
        "inventory_sha256": digest.hexdigest(),
        "files": records,
    }


def _archive_rows(path: Path, tour: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                minutes = int(float(row.get("minutes") or ""))
                tournament_date = datetime.strptime(row["tourney_date"], "%Y%m%d").date()
            except (ValueError, TypeError, KeyError):
                continue
            score = str(row.get("score") or "").upper()
            if minutes <= 0 or any(marker in score for marker in ("RET", "W/O", "DEF", "ABD")):
                continue
            rows.append({
                "tour": tour,
                "winner": row["winner_name"],
                "loser": row["loser_name"],
                "tournament": row["tourney_name"],
                "tournament_date": tournament_date,
                "minutes": minutes,
                "round": row.get("round"),
            })
    return rows


def _tennis_duration(
    candidate: Mapping[str, Any], record: CompetitionRecord, archive: list[dict[str, Any]]
) -> tuple[int | None, str | None]:
    pair = tuple(item.name for item in record.competitors)
    possible = [
        row for row in archive
        if row["tour"] == candidate["sport"]
        and 0 <= (candidate["market_date"] - row["tournament_date"]).days <= 21
        and match_name_pair(pair, (row["winner"], row["loser"])) is not None
        and names_match(str(candidate["result_label"]), row["winner"])
    ]
    if len(possible) > 1:
        named = [row for row in possible if names_match(record.event_name, row["tournament"])]
        if len(named) == 1:
            possible = named
    if len(possible) != 1:
        return None, "no_unique_completed_duration" if not possible else "ambiguous_completed_duration"
    return int(possible[0]["minutes"]) * 60, None


def _phase_rows(
    sport: str,
    event_slug: str,
    boundaries: Mapping[int, datetime],
    actual_end: datetime,
) -> list[tuple[Any, ...]]:
    config = get_sport_config(sport)
    starts = dict(boundaries)
    final_period = config.fold_from_period
    if sport == "ufc" and starts:
        final_period = min(final_period, max(starts))
    required = tuple(range(1, final_period + 1))
    if not all(period in starts for period in required):
        raise ProviderDataError(f"missing required period starts: {required}")
    rows = []
    for period in required:
        phase = config.phases[period]
        start = starts[period]
        end = starts[period + 1] if period < final_period else actual_end
        if end <= start:
            raise ProviderDataError("phase boundary is not strictly increasing")
        rows.append((sport,event_slug,phase.key,phase.label,phase.order,start,end))
    return rows


def build_timing(candidate_run_dir: str | Path, run_dir: str | Path) -> dict[str, Any]:
    candidate_run = Path(candidate_run_dir).expanduser().resolve()
    candidate_events_path = candidate_run / "candidate_events.parquet"
    if not candidate_events_path.is_file():
        raise FileNotFoundError(candidate_events_path)
    target = Path(run_dir).expanduser().resolve()
    with fresh_run(target, (candidate_events_path,)) as staging:
        source = staging / "source_cache"
        con = duckdb.connect()
        try:
            candidates = _rows(
                con,
                f"SELECT * FROM read_parquet('{quoted(candidate_events_path)}') "
                "ORDER BY sport,market_date,event_slug",
            )
        finally:
            con.close()
        scoreboard_jobs = []
        for sport, market_date in sorted({(row["sport"],row["market_date"]) for row in candidates}):
            day = market_date.strftime("%Y%m%d")
            url = _scoreboard_url(sport, day)
            scoreboard_jobs.append((url,source/"scoreboards"/sport/f"{day}.json"))
        _download_jobs(scoreboard_jobs)

        archive_jobs = []
        for tour in ("atp", "wta"):
            years = sorted({row["market_date"].year for row in candidates if row["sport"] == tour})
            for year in years:
                archive_jobs.append((TENNIS_ARCHIVE.format(tour=tour,year=year),
                                     source/"tennis_archive"/tour/f"{year}.csv"))
        _download_jobs(archive_jobs,workers=4)
        archive: list[dict[str, Any]] = []
        for _, path in archive_jobs:
            archive.extend(_archive_rows(path,path.parent.name))

        records_by_day: dict[tuple[str,date],tuple[CompetitionRecord,...]] = {}
        parse_errors: dict[tuple[str,date],str] = {}
        for row in candidates:
            key = (row["sport"],row["market_date"])
            if key in records_by_day or key in parse_errors:
                continue
            path = source/"scoreboards"/row["sport"]/f"{row['market_date'].strftime('%Y%m%d')}.json"
            try:
                records_by_day[key] = _scoreboard_records(row["sport"],_load_json(path))
            except Exception as exc:
                parse_errors[key] = f"scoreboard_parse_error:{type(exc).__name__}:{exc}"

        match_rows: list[dict[str, Any]] = []
        matched: dict[str,CompetitionRecord] = {}
        for candidate in candidates:
            key = (candidate["sport"],candidate["market_date"])
            if key in parse_errors:
                record, reason = None, parse_errors[key]
            else:
                record, reason = _candidate_match(candidate,records_by_day.get(key,()))
            if record is not None:
                matched[candidate["event_slug"]] = record
            match_rows.append({
                **candidate,
                "provider_event_id": record.event_id if record else None,
                "provider_competition_id": record.competition_id if record else None,
                "provider_event_name": record.event_name if record else None,
                "provider_start_utc": record.scheduled_start_utc if record else None,
                "provider_participants": " | ".join(item.name for item in record.competitors) if record else None,
                "match_exclusion_reason": reason,
            })
        conflicts: dict[tuple[str,str],list[dict[str,Any]]] = defaultdict(list)
        for row in match_rows:
            if row["provider_competition_id"]:
                conflicts[(row["sport"],row["provider_competition_id"])].append(row)
        for rows in conflicts.values():
            if len(rows) > 1:
                for row in rows:
                    row["match_exclusion_reason"] = "duplicate_candidate_to_provider_competition"
                    matched.pop(row["event_slug"],None)

        standard_jobs = []
        for row in match_rows:
            if row["event_slug"] not in matched or row["sport"] in {"atp","wta","ufc"}:
                continue
            config = get_sport_config(row["sport"])
            url = f"{BASE_URL}{config.espn_summary_path}?event={row['provider_event_id']}"
            standard_jobs.append((url,source/"summaries"/row["sport"]/f"{row['provider_event_id']}.json"))
        _download_jobs(dict((str(path),(url,path)) for url,path in standard_jobs).values())

        ufc_list_jobs = []
        for row in match_rows:
            if row["event_slug"] not in matched or row["sport"] != "ufc":
                continue
            url = (f"{CORE_UFC}/events/{row['provider_event_id']}/competitions/"
                   f"{row['provider_competition_id']}/plays?limit=1000")
            ufc_list_jobs.append((url,source/"ufc"/"lists"/f"{row['provider_competition_id']}.json"))
        _download_jobs(ufc_list_jobs)
        ufc_play_jobs = []
        ufc_refs: dict[str,list[tuple[int,Path]]] = defaultdict(list)
        for _, list_path in ufc_list_jobs:
            competition_id = list_path.stem
            payload = _load_json(list_path)
            items = payload.get("items")
            if not isinstance(items,list):
                continue
            for index,item in enumerate(items):
                ref = item.get("$ref") if isinstance(item,Mapping) else None
                if not isinstance(ref,str):
                    continue
                url = ref.replace("http://","https://",1)
                path = source/"ufc"/"plays"/competition_id/f"{index:03d}.json"
                ufc_play_jobs.append((url,path))
                ufc_refs[competition_id].append((index,path))
        _download_jobs(ufc_play_jobs)

        timing_rows: list[tuple[Any,...]] = []
        boundary_rows: list[tuple[Any,...]] = []
        timing_reasons = Counter()
        for row in match_rows:
            if row["event_slug"] not in matched:
                row["timing_exclusion_reason"] = "not_matched"
                continue
            record = matched[row["event_slug"]]
            try:
                duration_minutes = None
                if row["sport"] in {"atp","wta"}:
                    duration_seconds, reason = _tennis_duration(row,record,archive)
                    if reason or duration_seconds is None:
                        raise ProviderDataError(reason or "missing_tennis_duration")
                    parsed = derive_tennis_elapsed_thirds(record.scheduled_start_utc,duration_seconds)
                    quality = "elapsed_thirds_from_espn_start_and_archived_duration"
                    duration_minutes = duration_seconds / 60
                elif row["sport"] == "ufc":
                    paths = [path for _,path in sorted(ufc_refs.get(record.competition_id,()))]
                    plays = [_load_json(path) for path in paths]
                    parsed = extract_ufc_core_wallclock_boundaries(plays)
                    quality = "literal_espn_core_round_wallclocks"
                else:
                    summary_path = source/"summaries"/row["sport"]/f"{record.event_id}.json"
                    parsed = extract_standard_wallclock_boundaries(_load_json(summary_path))
                    quality = "literal_espn_play_wallclocks"
                phases = _phase_rows(row["sport"],row["event_slug"],parsed.period_starts,parsed.actual_end_utc)
                actual_start = dict(parsed.period_starts)[1]
                timing_rows.append((
                    row["sport"],row["event_slug"],record.competition_id,row["market_date"],
                    row["provider_event_id"],record.event_name,row["provider_participants"],
                    actual_start,parsed.actual_end_utc,len(phases),duration_minutes,
                    get_sport_config(row["sport"]).timing_source,
                    get_sport_config(row["sport"]).timing_source_status,quality,
                ))
                boundary_rows.extend(phases)
                row["timing_exclusion_reason"] = None
            except Exception as exc:
                reason = f"timing_parse_error:{type(exc).__name__}:{exc}"
                row["timing_exclusion_reason"] = reason
                timing_reasons[reason] += 1

        accepted_by_sport = Counter(row[0] for row in timing_rows)
        missing_sports = sorted(set(SPORT_CONFIGS)-set(accepted_by_sport))
        if missing_sports:
            raise ValueError(f"No timing-eligible events for sports: {missing_sports}")
        output = duckdb.connect()
        try:
            output.execute("""CREATE TABLE event_timing(
                sport VARCHAR,event_slug VARCHAR,game_id VARCHAR,market_date DATE,
                provider_event_id VARCHAR,provider_event_name VARCHAR,provider_participants VARCHAR,
                actual_start_utc TIMESTAMPTZ,actual_end_utc TIMESTAMPTZ,live_phase_count INTEGER,
                duration_minutes DOUBLE,timing_source VARCHAR,timing_source_status VARCHAR,
                timing_quality VARCHAR)""")
            output.executemany("INSERT INTO event_timing VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",timing_rows)
            output.execute(f"COPY event_timing TO '{quoted(staging/'event_timing.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            output.execute("""CREATE TABLE phase_boundaries(
                sport VARCHAR,event_slug VARCHAR,phase VARCHAR,phase_label VARCHAR,
                phase_order INTEGER,start_utc TIMESTAMPTZ,end_utc TIMESTAMPTZ)""")
            output.executemany("INSERT INTO phase_boundaries VALUES (?,?,?,?,?,?,?)",boundary_rows)
            output.execute(f"COPY phase_boundaries TO '{quoted(staging/'phase_boundaries.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            output.execute("""CREATE TABLE match_audit(
                sport VARCHAR,event_slug VARCHAR,market_date DATE,participant_1 VARCHAR,
                participant_2 VARCHAR,result_label VARCHAR,market_count INTEGER,
                provider_event_id VARCHAR,provider_competition_id VARCHAR,provider_event_name VARCHAR,
                provider_start_utc TIMESTAMPTZ,provider_participants VARCHAR,
                match_exclusion_reason VARCHAR,timing_exclusion_reason VARCHAR,
                eligible BOOLEAN)""")
            output.executemany(
                "INSERT INTO match_audit VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(
                    row["sport"],row["event_slug"],row["market_date"],row["participant_1"],
                    row["participant_2"],row["result_label"],row["market_count"],
                    row["provider_event_id"],row["provider_competition_id"],row["provider_event_name"],
                    row["provider_start_utc"],row["provider_participants"],
                    row["match_exclusion_reason"],row.get("timing_exclusion_reason"),
                    row["match_exclusion_reason"] is None and row.get("timing_exclusion_reason") is None,
                ) for row in match_rows],
            )
            output.execute(f"COPY match_audit TO '{quoted(staging/'match_audit.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
        finally:
            output.close()

        index = _provider_index(source)
        write_json(staging/"provider_index.json",index)
        manifest = {
            "schema_version": 1,
            "stage": "multisport_provider_match_and_timing_v1",
            "sports": list(SPORT_CONFIGS),
            "phase_policy": "literal provider periods; later periods fold into the final phase",
            "tennis_policy": "elapsed thirds from ESPN start plus unique matched completed duration; not set boundaries",
            "counts": {
                "candidate_events": len(candidates),
                "matched_events": len(matched),
                "timing_eligible_events": len(timing_rows),
                "timing_eligible_by_sport": dict(sorted(accepted_by_sport.items())),
                "match_exclusions": dict(sorted(Counter(
                    row["match_exclusion_reason"] for row in match_rows if row["match_exclusion_reason"]
                ).items())),
                "timing_exclusions": dict(sorted(timing_reasons.items())),
            },
            "inputs": {"candidate_events": fingerprint(candidate_events_path)},
            "provider_inventory": {
                key: index[key] for key in ("file_count","total_bytes","inventory_sha256")
            },
            "outputs": {
                name: artifact_fingerprint(staging/name)
                for name in ("event_timing.parquet","phase_boundaries.parquet","match_audit.parquet",
                             "provider_index.json")
            },
        }
        write_json(staging/"timing_manifest.json",manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-run",required=True)
    parser.add_argument("--run-dir",required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(json.dumps(build_timing(args.candidate_run,args.run_dir),sort_keys=True))


if __name__ == "__main__":
    main()
