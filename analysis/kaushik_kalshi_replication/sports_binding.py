"""Strict sports identity/result proofs plus an immutable metadata-only reader.

Pure fixture functions never collect data or estimate. The guarded producer
reopens explicit saved metadata bindings; it cannot create independent
acceptance. Milestone end_date is never a game clock.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import unicodedata
from typing import Mapping


WINNER_SERIES = {"KXMLBGAME": "mlb", "KXNFLGAME": "nfl", "KXNBAGAME": "nba",
    "KXNHLGAME": "nhl", "KXNCAAMBGAME": "cbb", "KXATPMATCH": "atp",
    "KXEPLGAME": "epl", "KXNCAAFGAME": "cfb", "KXWNBAGAME": "wnba"}
LEAGUES = {"mlb": "MLB", "nfl": "NFL", "nba": "NBA", "nhl": "NHL",
    "cbb": "NCAAMB", "cfb": "NCAAFB", "epl": "EPL", "wnba": "WNBA"}
SCHEMA_VERSION = "kaushik_kalshi_sports_binding_review_v1"
STRUCTURED_TARGET_SOURCE = {"path": "/mnt/data/runs/2026-10-16_kalshi_source_admission_v2/structured_targets.parquet",
    "bytes": 72_093_384, "rows": 281_156,
    "sha256": "f316e1c9daa5e4c24478dca84179a70f3122af3f07bbfe6f8f2a3cac16326508"}
DATA_ROOT = Path("/mnt/data")
RUN_ROOT = DATA_ROOT / "runs"
REPO_ROOT = Path("/home/ubuntu/prediction_markets")
PRODUCTION_PYTHON = Path("/home/ubuntu/venv/bin/python")
EXPECTED_MLB_RETAINED_GAMES = 3696


class BindingBlocked(ValueError):
    pass


def require(value, reason):
    if not value:
        raise BindingBlocked(reason)


def name_key(value):
    """Whitespace/case normalization only; no accent/punctuation/word folding."""
    require(isinstance(value, str) and bool(value.strip()), "nonempty participant name required")
    require(not any(ord(char) < 32 for char in value), "control character in participant name")
    return " ".join(unicodedata.normalize("NFC", value).split()).casefold()


def _id(value):
    # Native identifiers are opaque, including the verified legacy internal
    # ASCII-space event key. Preserve bytes; never normalize keys into slugs.
    return isinstance(value, str) and bool(value) and value == value.strip() and not any(
        ord(char) < 32 or (char.isspace() and char != " ") for char in value)


def _json(value, kind):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError) as error:
            raise BindingBlocked("malformed native JSON") from error
    require(type(value) is kind, "native JSON type differs")
    return value


def _utc(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset().total_seconds() != 0:
        return None
    return value.astimezone(timezone.utc)


def _day(value):
    if isinstance(value, datetime):
        return None
    if type(value) is date:
        return value
    if isinstance(value, str):
        try:
            result = date.fromisoformat(value)
            return result if result.isoformat() == value else None
        except ValueError:
            pass
    return None


def _pair(value):
    """Parse explicit textual pair labels, not the opaque ticker suffix."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    text = re.sub(r"^Basketball Play-In: ", "", text)
    text = re.sub(r"(?:: Game [0-9]+)? Winner\?$", "", text)
    matches = [(separator, text.split(separator)) for separator in (" at ", " vs ") if separator in text]
    if len(matches) != 1 or len(matches[0][1]) != 2:
        return None
    separator, names = matches[0]
    names = tuple(name.strip() for name in names)
    if not all(names) or name_key(names[0]) == name_key(names[1]):
        return None
    return {"names": names, "orientation": "away_home" if separator == " at " else "unknown"}


def entity_identities(records, *, evidence):
    """Exact UUID/name/identity-label bridge from the pinned native reference.

    Only literal name, market, team_name and short_name values are labels. No
    concatenation, abbreviation expansion, source-ID namespace guess or use of
    current standings/results is allowed. All supplied IDs must be unique.
    """
    require(isinstance(evidence, str) and evidence.strip(), "pinned entity evidence required")
    output, seen = {}, set()
    kind_sports = {"baseball_team": {"MLB": "mlb"}, "football_team": {"NFL": "nfl", "NCAAFB": "cfb"},
        "basketball_team": {"NBA": "nba", "WNBA": "wnba", "NCAAMB": "cbb"},
        "hockey_team": {"NHL": "nhl"}, "soccer_team": {"EPL": "epl"}}
    for entity in records:
        require(isinstance(entity, Mapping) and _id(entity.get("id")), "native entity ID required")
        require(entity["id"] not in seen, "duplicate native entity ID")
        seen.add(entity["id"])
        kind = entity.get("type")
        if kind not in kind_sports and kind != "tennis_competitor":
            continue
        details = _json(entity.get("details"), dict)
        sport = "atp" if kind == "tennis_competitor" and details.get("gender") == "male" else kind_sports.get(kind, {}).get(details.get("league"))
        # The inspected soccer-team reference has no league value. Soccer kind
        # plus a separately proved EPL milestone supplies league context later.
        if kind == "soccer_team" and details.get("league") is None:
            sport = "soccer"
        if sport is None:
            continue
        labels = [entity.get("name"), *[details.get(name) for name in ("market", "team_name", "short_name")]]
        labels = {name_key(value): value for value in labels if isinstance(value, str) and value.strip()}
        require(labels and isinstance(entity.get("name"), str) and entity["name"].strip(), "native entity participant name absent")
        candidate_options = list(labels.values())
        abbreviation = details.get("abbreviation")
        if isinstance(abbreviation, str) and entity["name"].startswith(abbreviation + " "):
            candidate_options.append(entity["name"][len(abbreviation) + 1:])
        output[entity["id"]] = {"sport": sport, "name": entity["name"], "name_options": list(labels.values()),
            "candidate_name_options": candidate_options,
            "source_id": entity.get("source_id"), "source_ids": entity.get("source_ids"), "evidence": evidence}
    return output


def native_candidates(events, milestones, *, entities=None):
    """Extract nine-family candidates; every selected event receives a reason.

    Milestone primary links must be unique. Name/UUID associations are not
    inferred from title order. Explicit `A at B` event text supplies away/home
    names, while details independently supplies native away/home UUIDs.
    """
    links, event_ids, milestone_ids, duplicate_links = defaultdict(list), set(), set(), {}
    for milestone in milestones:
        require(isinstance(milestone, Mapping) and _id(milestone.get("id")), "native milestone ID absent")
        require(milestone["id"] not in milestone_ids, "duplicate native milestone ID")
        milestone_ids.add(milestone["id"])
        primary = _json(milestone.get("primary_event_tickers"), list)
        require(all(_id(item) for item in primary), "invalid primary milestone links")
        duplicate_links[milestone["id"]] = [key for key, count in Counter(primary).items() if count > 1]
        for event_id in primary:
            links[event_id].append(milestone)
    output = []
    for event in events:
        require(isinstance(event, Mapping) and _id(event.get("event_ticker")), "native event ID absent")
        event_id = event["event_ticker"]
        require(event_id not in event_ids, "duplicate native event ID")
        event_ids.add(event_id)
        if event.get("series_ticker") not in WINNER_SERIES:
            continue
        sport = WINNER_SERIES[event["series_ticker"]]
        row = {"event_ticker": event_id, "series_ticker": event["series_ticker"], "sport": sport,
               "event_title": event.get("title"), "event_sub_title": event.get("sub_title"),
               "status": "native_candidate", "reason": None, "orientation": "unknown"}
        found = links[event_id]
        if len(found) != 1:
            row.update(reason="missing_primary_milestone" if not found else "ambiguous_primary_milestone",
                       milestone_count=len(found))
            output.append(row)
            continue
        milestone = found[0]
        details = _json(milestone.get("details"), dict)
        row.update(milestone_id=milestone["id"], milestone_title=milestone.get("title"),
                   native_source_id=milestone.get("source_id"), native_source_ids=milestone.get("source_ids"),
                   repeated_primary_links=duplicate_links[milestone["id"]])
        pair, event_pair = _pair(milestone.get("title")), _pair(event.get("title"))
        start = _utc(milestone.get("start_date"))
        league_ok = (details.get("tour") == "ATP" and details.get("gender") == "men") if sport == "atp" else (
            details.get("league") in ("EPL", "English Premier") if sport == "epl" else details.get("league") == LEAGUES[sport])
        ids = (details.get("first_competitor_id"), details.get("second_competitor_id")) if sport == "atp" else (details.get("away_team_id"), details.get("home_team_id"))
        if duplicate_links[milestone["id"]]:
            row["reason"] = "duplicate_milestone_primary_link"
        elif not league_ok:
            row["reason"] = "milestone_sport_disagreement"
        elif pair is None:
            row["reason"] = "unparsed_native_participant_pair"
        elif not all(_id(value) for value in ids) or ids[0] == ids[1]:
            row["reason"] = "missing_or_invalid_native_participant_ids"
        elif start is None:
            row["reason"] = "missing_or_invalid_scheduled_start"
        else:
            row.update(participant_names=list(pair["names"]), participant_ids=list(ids),
                       event_pair_names=list(event_pair["names"]) if event_pair else None,
                       scheduled_start_utc=start.isoformat(), scheduled_utc_date=start.date().isoformat(),
                       native_date_source="unique_primary_milestone.start_date",
                       orientation="away_home" if event_pair and event_pair["orientation"] == "away_home" else "unknown",
                       away_name=event_pair["names"][0] if event_pair and event_pair["orientation"] == "away_home" else None,
                       home_name=event_pair["names"][1] if event_pair and event_pair["orientation"] == "away_home" else None,
                       away_native_id=ids[0] if sport != "atp" else None,
                       home_native_id=ids[1] if sport != "atp" else None)
            if entities is not None:
                if not all(value in entities for value in ids):
                    row["reason"] = "missing_native_participant_entity"
                elif any(entities[value]["sport"] not in (sport, "soccer" if sport == "epl" else sport) for value in ids):
                    row["reason"] = "native_participant_entity_sport_disagreement"
                else:
                    row.update(participant_names=[entities[value]["name"] for value in ids],
                        participant_name_options=[entities[value]["name_options"] for value in ids],
                        candidate_name_options=[entities[value]["candidate_name_options"] for value in ids],
                        participant_name_source="native_UUID_join_pinned_structured_targets",
                        entity_evidence=[entities[value]["evidence"] for value in ids],
                        milestone_pair_names=list(pair["names"]),
                        orientation="unknown" if sport == "atp" else "away_home",
                        away_name=entities[ids[0]]["name"] if sport != "atp" else None,
                        home_name=entities[ids[1]]["name"] if sport != "atp" else None)
        output.append(row)
    return output


def provider_games(records):
    """Validate caller-normalized retained game identities and authoritative clocks.

    Required fields: sport, provider_game_id, participant_names (exactly two),
    actual_start_utc, actual_end_utc, timing_qualification, evidence.
    Optional scheduled_start_utc is preferred for exact UTC date matching;
    otherwise actual_start_utc supplies the explicit date. market_date is only
    an outlier diagnostic, never the matching key. ATP remains timing-qualified.
    """
    output, ids = [], set()
    for record in records:
        require(isinstance(record, Mapping), "provider game record required")
        sport, game_id = record.get("sport"), record.get("provider_game_id")
        require(sport in WINNER_SERIES.values() and _id(game_id), "invalid provider sport/game identity")
        require((sport, game_id) not in ids, "duplicate provider game ID")
        ids.add((sport, game_id))
        names = record.get("participant_names")
        require(isinstance(names, (tuple, list)) and len(names) == 2 and len({name_key(name) for name in names}) == 2,
                "bijective provider participant pair required")
        require(isinstance(record.get("evidence"), str) and record["evidence"].strip(), "provider identity/timing evidence required")
        require(isinstance(record.get("timing_qualification"), str) and record["timing_qualification"].strip(), "explicit timing qualification required")
        if sport == "atp":
            require(record["timing_qualification"] == "scheduled_start_plus_archived_duration", "ATP timing qualification must remain explicit")
        start, end = _utc(record.get("actual_start_utc")), _utc(record.get("actual_end_utc"))
        scheduled = _utc(record.get("scheduled_start_utc"))
        reason = None
        if start is None or end is None or end <= start:
            reason = "invalid_provider_play_clocks"
        elif record.get("scheduled_start_utc") is not None and scheduled is None:
            reason = "invalid_provider_scheduled_start"
        market_day = _day(record.get("market_date"))
        if reason is None and market_day is not None and abs((start.date() - market_day).days) > 1:
            reason = "provider_market_date_outlier_pending_identity_review"
        boundary = scheduled or start
        orientation = "unknown"
        away, home = record.get("away_name"), record.get("home_name")
        if away is not None or home is not None:
            require(away is not None and home is not None and {name_key(away), name_key(home)} == {name_key(name) for name in names}, "provider home/away proof disagrees with pair")
            orientation = "away_home"
        output.append({**dict(record), "participant_names": list(names), "reason": reason,
            "actual_start_utc": start.isoformat() if start else None,
            "actual_end_utc": end.isoformat() if end else None,
            "scheduled_start_utc": scheduled.isoformat() if scheduled else None,
            "match_utc_date": boundary.date().isoformat() if boundary else None,
            "date_source": "scheduled_start_utc" if scheduled else "actual_start_utc",
            "orientation": orientation})
    return output


def _alias_map(aliases):
    result = {}
    for alias in aliases:
        require(isinstance(alias, Mapping) and alias.get("sport") in WINNER_SERIES.values(), "sport-scoped alias required")
        require(alias.get("reviewed") is True and isinstance(alias.get("evidence"), str) and alias["evidence"].strip(),
                "every admitted alias requires independent review/evidence")
        native_id = alias.get("native_entity_id")
        require(native_id is None or _id(native_id), "invalid alias native entity ID")
        key, value = (alias["sport"], native_id, name_key(alias.get("native_name"))), name_key(alias.get("provider_name"))
        require(key not in result or result[key] == value, "conflicting reviewed native alias")
        result[key] = value
    return result


def alias_receipts(records, *, native_entities, providers):
    """Validate human-reviewed, outcome-blind aliases and their source receipts.

    A receipt needs sport/native_entity_id/native_name/provider_name, reviewed
    true, review_id, evidence and native_source/provider_source objects carrying
    path, SHA-256 and identity field names. ID-to-name and reverse-name collisions
    block rather than resolving by frequency. This never approves proposals.
    """
    roster = defaultdict(set)
    for provider in providers:
        for value in provider["participant_names"]:
            roster[provider["sport"]].add(name_key(value))
    output, by_native, by_provider, seen = [], {}, {}, set()
    for record in records:
        require(isinstance(record, Mapping) and record.get("reviewed") is True and _id(record.get("review_id")), "explicit independent alias review receipt required")
        entity_id, sport = record.get("native_entity_id"), record.get("sport")
        require(entity_id in native_entities and native_entities[entity_id]["sport"] in (sport, "soccer" if sport == "epl" else sport), "alias native entity/sport proof differs")
        native_name, provider_name = name_key(record.get("native_name")), name_key(record.get("provider_name"))
        require(native_name in {name_key(name) for name in native_entities[entity_id]["name_options"]}, "alias native literal field proof differs")
        require(provider_name in roster[sport], "alias provider literal field proof differs")
        for role in ("native_source", "provider_source"):
            proof = record.get(role, {})
            require(isinstance(proof, Mapping) and isinstance(proof.get("path"), str) and proof["path"]
                and re.fullmatch(r"[0-9a-f]{64}", proof.get("sha256", "")) and isinstance(proof.get("fields"), list)
                and proof["fields"] and all(isinstance(field, str) and field for field in proof["fields"]), "alias field/source fingerprint evidence required")
        key, reverse = (sport, entity_id), (sport, provider_name)
        require(key not in by_native or by_native[key] == provider_name, "native entity has conflicting reviewed provider names")
        require(reverse not in by_provider or by_provider[reverse] == entity_id, "provider name maps to multiple reviewed native entities")
        identity = (sport, entity_id, native_name)
        require(identity not in seen, "duplicate reviewed alias receipt")
        seen.add(identity)
        by_native[key], by_provider[reverse] = provider_name, entity_id
        output.append(dict(record))
    _alias_map(output)
    return {"schema_version": "kaushik_kalshi_sports_alias_receipts_v1", "status": "reviewed_identity_aliases_only",
        "aliases": output, "alias_rows": len(output), "native_entities": len(by_native), "provider_names": len(by_provider),
        "native_to_provider_one_to_one": True, "provider_to_native_one_to_one": True,
        "sports_admitted": False, "native_yes_proposition_and_result_proof": "pending"}


def bind_candidates(native, providers, *, reviewed_aliases=()):
    """Unique exact unordered pair + UTC date; never nearest/fuzzy matching.

    Output is candidate identity, not accepted sports observations. Unknown
    orientation is harmless only for a future explicitly named YES claim; a
    home/away-referential proposition still requires orientation proof.
    """
    aliases = _alias_map(reviewed_aliases)
    def resolved(sport, value, native_id=None):
        key = name_key(value)
        return aliases.get((sport, native_id, key), aliases.get((sport, None, key), key))
    provider_index, provider_ids, native_ids, by_provider = defaultdict(list), set(), set(), defaultdict(list)
    for provider in providers:
        identity = (provider["sport"], provider["provider_game_id"])
        require(identity not in provider_ids, "duplicate provider game identity")
        provider_ids.add(identity)
        if provider.get("reason") is None:
            provider_index[(provider["sport"], provider["match_utc_date"])].append(provider)
    output = []
    for event in native:
        require(event["event_ticker"] not in native_ids, "duplicate native event identity")
        native_ids.add(event["event_ticker"])
        row = {"event_ticker": event["event_ticker"], "sport": event["sport"], "milestone_id": event.get("milestone_id"),
               "status": "unmatched", "reason": event.get("reason"), "provider_game_id": None,
               "native_yes_proposition_and_result_proof": "pending", "sports_admitted": False}
        if row["reason"] is None:
            options = event.get("participant_name_options", [[name] for name in event["participant_names"]])
            require(len(options) == 2 and all(isinstance(values, (list, tuple)) and values for values in options), "two explicit native participant label sets required")
            options = [set(resolved(event["sport"], value, event["participant_ids"][i] if event.get("participant_name_source") else None) for value in values) for i, values in enumerate(options)]
            event_pair = event.get("event_pair_names")
            if not event.get("participant_name_source") and len({resolved(event["sport"], name) for name in event["participant_names"]}) != 2:
                row["reason"] = "nonbijective_native_alias_pair"
            elif not event.get("participant_name_source") and event_pair and {resolved(event["sport"], name) for name in event_pair} != set.union(*options):
                row["reason"] = "native_event_milestone_names_disagree"
            else:
                choices = []
                for provider in provider_index[(event["sport"], event["scheduled_utc_date"])]:
                    names = [name_key(name) for name in provider["participant_names"]]
                    assignments = [order for order in ((0, 1), (1, 0)) if all(names[order[i]] in options[i] for i in range(2))]
                    if len(assignments) == 1:
                        choices.append((provider, [names[assignments[0][i]] for i in range(2)]))
                    elif len(assignments) > 1:
                        row["reason"] = "ambiguous_native_entity_label_assignment"
                if row["reason"] is None and len(choices) != 1:
                    row["reason"] = "no_exact_provider_pair_date" if not choices else "ambiguous_provider_pair_date_doubleheader"
                elif row["reason"] is None:
                    provider, mapping = choices[0]
                    row["candidate_key"] = [event["sport"], sorted(mapping), event["scheduled_utc_date"]]
                    by_provider[(event["sport"], provider["provider_game_id"])].append((event, row, provider, mapping))
        output.append(row)
    for values in by_provider.values():
        if len(values) != 1:
            for _, row, _, _ in values:
                row["reason"] = "ambiguous_native_pair_date"
            continue
        event, row, provider, mapping = values[0]
        orientation = "unknown"
        if event.get("orientation") == provider.get("orientation") == "away_home":
            if event.get("participant_name_source"):
                away, home = mapping
            else:
                away, home = resolved(event["sport"], event["away_name"]), resolved(event["sport"], event["home_name"])
            if away != name_key(provider["away_name"]) or home != name_key(provider["home_name"]):
                orientation = "conflicting_away_home"
            else:
                orientation = "verified_away_home"
        labels = [event.get("event_pair_names"), event.get("milestone_pair_names")]
        labels = [value for value in labels if value]
        labels_verified = all({resolved(event["sport"], name) for name in value} == set(mapping) for value in labels)
        row.update(status="identity_candidate_pending_claim_result_proof", reason=None,
            provider_game_id=provider["provider_game_id"], participant_mapping=[
                {"native_name": name, "native_participant_id": event["participant_ids"][i] if event.get("participant_name_source") else None,
                 "provider_name_key": mapping[i]} for i, name in enumerate(event["participant_names"])],
            orientation=orientation, identity_proof="bijective_unordered_participants_and_exact_UTC_date_one_to_one",
            orientation_conflict=orientation == "conflicting_away_home",
            home_away_proposition_admissible=orientation == "verified_away_home",
            native_orientation={"away_name": event.get("away_name"), "home_name": event.get("home_name"),
                "away_native_id": event.get("away_native_id"), "home_native_id": event.get("home_native_id")},
            provider_orientation={"away_name": provider.get("away_name"), "home_name": provider.get("home_name")},
            native_pair_labels=labels,
            title_label_review="exact_or_reviewed_aliases_reconciled" if labels_verified else "pending_title_alias_review",
            native_participant_name_source=event.get("participant_name_source", "explicit_native_pair_labels"),
            date_source=provider["date_source"], match_utc_date=event["scheduled_utc_date"],
            actual_start_utc=provider["actual_start_utc"], actual_end_utc=provider["actual_end_utc"],
            timing_qualification=provider["timing_qualification"], provider_evidence=provider["evidence"],
            raw_provider_timing_quality=provider.get("raw_provider_timing_quality"),
            provider_source_bindings=provider.get("source_bindings", {}))
    selected = [(row["sport"], row["provider_game_id"]) for row in output if row["provider_game_id"] is not None]
    require(len(selected) == len(set(selected)), "candidate matching is not one-to-one")
    return output


def propose_aliases(native, providers):
    """Outcome-blind review proposals; never consumed automatically by binding.

    Both participants must anchor through exact labels or whole-word literal
    prefix/suffix; sharing a single opponent/date cannot establish identity.
    Exactly one provider game/assignment must remain on that sport/UTC date.
    Cross-game and
    opponent consistency are reported, not treated as independent review.
    """
    games = defaultdict(list)
    for row in providers:
        if row.get("reason") is None:
            games[(row["sport"], row["match_utc_date"])].append(row)
    evidence = defaultdict(list)
    for event in native:
        if event.get("reason") is not None:
            continue
        names = event["participant_names"]
        options = event.get("candidate_name_options", event.get("participant_name_options", [[name] for name in names]))
        choices = []
        for provider in games[(event["sport"], event["scheduled_utc_date"])]:
            values = provider["participant_names"]
            for order in ((0, 1), (1, 0)):
                anchor = []
                for i in range(2):
                    right = name_key(values[order[i]])
                    candidates = [name_key(value) for value in options[i]]
                    anchor.append(any(left == right or right.startswith(left + " ") or right.endswith(" " + left) for left in candidates))
                if all(anchor):
                    choices.append((provider, [values[order[0]], values[order[1]]], anchor))
        # Two assignments for the same game are ambiguous too.
        if len(choices) != 1:
            continue
        provider, targets, anchor = choices[0]
        for i in range(2):
            exact_options = event.get("participant_name_options", [[name] for name in names])
            if name_key(targets[i]) in {name_key(value) for value in exact_options[i]}:
                continue
            native_id = event["participant_ids"][i] if event.get("participant_name_source") else None
            evidence[(event["sport"], native_id, name_key(names[i]), name_key(targets[i]))].append({
                "event_ticker": event["event_ticker"], "provider_game_id": provider["provider_game_id"],
                "native_name": names[i], "provider_name": targets[i], "opponent": name_key(targets[1-i]),
                "seed": "both_participants_literal_word_prefix_suffix_or_exact"})
    targets_by_native = defaultdict(set)
    for sport, native_id, left, right in evidence:
        targets_by_native[(sport, native_id or ("unproved-label:" + left))].add(right)
    proposals = []
    for (sport, native_id, left, right), support in sorted(evidence.items()):
        games_seen = {row["provider_game_id"] for row in support}
        opponents = {row["opponent"] for row in support}
        conflict = len(targets_by_native[(sport, native_id or ("unproved-label:" + left))]) != 1
        proposals.append({"sport": sport, "native_entity_id": native_id, "native_name": support[0]["native_name"], "provider_name": support[0]["provider_name"],
            "reviewed": False, "status": "conflicting_proposal" if conflict else "pending_independent_alias_review",
            "support_games": len(games_seen), "support_opponents": len(opponents),
            "cross_game_consistent": not conflict and len(games_seen) >= 2 and len(opponents) >= 2,
            "examples": support[:3], "evidence": "candidate-only participant/date reconciliation; no outcome fields used"})
    return proposals


def review_summary(native, providers, bindings, proposals=()):
    """Small renderer-friendly prototype inventory, explicitly not final results."""
    by_sport = {}
    for sport in WINNER_SERIES.values():
        n = [row for row in native if row["sport"] == sport]
        p = [row for row in providers if row["sport"] == sport]
        b = [row for row in bindings if row["sport"] == sport]
        by_sport[sport] = {"native_events": len(n), "retained_provider_games": len(p),
            "identity_candidates": sum(row["provider_game_id"] is not None for row in b),
            "binding_reasons": dict(Counter(row.get("reason") or "identity_candidate" for row in b)),
            "provider_exclusions": dict(Counter(row["reason"] for row in p if row.get("reason"))),
            "native_scheduled_first_UTC": min((row["scheduled_utc_date"] for row in n if row.get("scheduled_utc_date")), default=None),
            "native_scheduled_last_UTC": max((row["scheduled_utc_date"] for row in n if row.get("scheduled_utc_date")), default=None)}
    return {"schema_version": SCHEMA_VERSION, "status": "prototype_not_accepted", "sports_results_produced": False,
        "scope": "retained_provider_covered_overlap_not_full_Kalshi_game_census", "winner_series": WINNER_SERIES,
        "matching": "unique exact sport/unordered participant pair/UTC date; reviewed aliases only; no nearest or fuzzy matching",
        "outcome_gate": "pending native YES proposition and independently agreeing provider winner/draw proof",
        "native_end_date_used_as_actual_clock": False, "EPL_draw_claims": "must be separately proved, not dropped",
        "ATP_clock_qualification": "scheduled start plus archived duration; date outliers pending identity proof",
        "alias_proposals": len(proposals), "reviewed_aliases_automatically_created": 0, "by_sport": by_sport}


def _canonical_receipt(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return encoded, hashlib.sha256(encoded.encode()).hexdigest()


def scoreboard_results(payload, sport, *, source_binding):
    """Project literal saved ESPN participant/completed/winner fields only.

    This does not search dates or select a competition by its outcome. Repeated
    source snapshots must be reconciled separately by exact competition ID.
    Incomplete/ambiguous results remain explicit exclusions. No play data,
    odds, prices, comments or market outcome labels are interpreted.
    """
    require(sport in ("atp", "cbb", "cfb", "epl", "nhl", "wnba"), "unsupported scoreboard result sport")
    require(isinstance(payload, Mapping) and isinstance(payload.get("events"), list), "saved scoreboard events required")
    require(isinstance(source_binding, Mapping) and source_binding.get("path")
        and re.fullmatch(r"[0-9a-f]{64}", source_binding.get("sha256", "")), "saved scoreboard source binding required")
    output = []
    for event_index, event in enumerate(payload["events"]):
        require(isinstance(event, Mapping), "scoreboard event object required")
        groups = event.get("groupings", []) if sport == "atp" else [event]
        require(isinstance(groups, list), "scoreboard grouping list required")
        for group_index, group in enumerate(groups):
            require(isinstance(group, Mapping), "scoreboard grouping object required")
            if sport == "atp":
                label = group.get("grouping", group)
                require(isinstance(label, Mapping), "tennis grouping metadata required")
                if str(label.get("name", label.get("displayName", ""))).casefold() != "men's singles":
                    continue
            competitions = group.get("competitions", [])
            require(isinstance(competitions, list), "scoreboard competition list required")
            for competition_index, competition in enumerate(competitions):
                require(isinstance(competition, Mapping), "scoreboard competition object required")
                game_id = competition.get("id")
                if not _id(game_id):
                    continue
                pointer = f"events[{event_index}]" + (f".groupings[{group_index}]" if sport == "atp" else "") + f".competitions[{competition_index}]"
                row = {"sport": sport, "provider_game_id": game_id, "source_binding": dict(source_binding),
                    "source_pointer": pointer, "reason": None, "completed": False, "is_draw": False,
                    "winner_participant_id": None, "participants": []}
                status = competition.get("status", event.get("status", {}))
                kind = status.get("type", {}) if isinstance(status, Mapping) else {}
                kind = kind if isinstance(kind, Mapping) else {}
                row["completed_status"] = dict(kind)
                row["completed"] = kind.get("state") == "post" and kind.get("completed") is True
                competitors = competition.get("competitors")
                if not isinstance(competitors, list) or len(competitors) != 2:
                    row["reason"] = "provider_result_participant_pair_missing"
                else:
                    for index, competitor in enumerate(competitors):
                        if not isinstance(competitor, Mapping):
                            row["reason"] = "provider_result_participant_object_invalid"
                            break
                        entity = competitor.get("team", competitor.get("athlete", competitor))
                        if not isinstance(entity, Mapping):
                            row["reason"] = "provider_result_participant_object_invalid"
                            break
                        participant_id = entity.get("id", competitor.get("id"))
                        names = {name_key(entity[key]): entity[key] for key in (
                            "displayName", "fullName", "name", "shortName", "shortDisplayName", "location", "nickname", "abbreviation")
                            if isinstance(entity.get(key), str) and entity[key].strip()}
                        if type(participant_id) not in (str, int) or not _id(str(participant_id)) or not names:
                            row["reason"] = "provider_result_participant_identity_missing"
                            break
                        score = competitor.get("score")
                        if isinstance(score, Mapping):
                            score = score.get("value", score.get("displayValue"))
                        row["participants"].append({"id": str(participant_id), "name_options": list(names.values()),
                            "winner": competitor.get("winner"), "score": score,
                            "source_pointer": pointer + f".competitors[{index}]"})
                    if row["reason"] is None:
                        people = row["participants"]
                        winners = [person for person in people if person["winner"] is True]
                        flags_valid = all(type(person["winner"]) is bool for person in people)
                        if len({person["id"] for person in people}) != 2:
                            row["reason"] = "provider_result_duplicate_participant_id"
                        elif not row["completed"]:
                            row["reason"] = "provider_result_not_completed"
                        elif not flags_valid:
                            row["reason"] = "provider_result_winner_flags_missing"
                        elif len(winners) == 1:
                            row["winner_participant_id"] = winners[0]["id"]
                        elif sport == "epl" and not winners and all(re.fullmatch(r"[0-9]+", str(person["score"])) for person in people) and int(people[0]["score"]) == int(people[1]["score"]):
                            row["is_draw"] = True
                        else:
                            row["reason"] = "provider_result_winner_or_draw_ambiguous"
                output.append(row)
    return output


def reconcile_result_snapshots(records):
    """Exact-ID reconciliation; no completed contradictory evidence is lost."""
    grouped = defaultdict(list)
    for record in records:
        grouped[(record["sport"], record["provider_game_id"])].append(record)
    output = []
    for key, snapshots in sorted(grouped.items()):
        complete = [record for record in snapshots if record.get("completed") is True]
        evidence = [{"source_binding": record.get("source_binding"), "source_pointer": record.get("source_pointer"),
            "completed_status": record.get("completed_status"), "reason": record.get("reason"),
            "participants": record.get("participants"), "winner_participant_id": record.get("winner_participant_id"),
            "is_draw": record.get("is_draw")} for record in complete]
        ignored = [{"source_binding": record.get("source_binding"), "source_pointer": record.get("source_pointer"),
            "completed_status": record.get("completed_status"), "reason": record.get("reason") or "snapshot_not_completed"}
            for record in snapshots if record.get("completed") is not True]
        review = {"completed_source_snapshots": evidence, "ignored_incomplete_source_snapshots": ignored}
        if not complete:
            output.append({"sport": key[0], "provider_game_id": key[1], "completed": False,
                "reason": "no_unambiguous_completed_provider_result_snapshot", **review})
            continue
        if any(record.get("reason") is not None for record in complete):
            output.append({"sport": key[0], "provider_game_id": key[1], "completed": False,
                "reason": "contradictory_or_ambiguous_completed_provider_result_snapshot", **review})
            continue
        def signature(record):
            # A completed snapshot must not silently disagree about final
            # scores. Normalize only numeric/string representations of the
            # same literal scalar; missing vs present remains a conflict.
            return (tuple(sorted((person["id"], str(person.get("score"))) for person in record["participants"])),
                    record["winner_participant_id"], record["is_draw"])
        if len({signature(record) for record in complete}) != 1:
            output.append({"sport": key[0], "provider_game_id": key[1], "completed": False,
                "reason": "conflicting_completed_provider_result_snapshots", **review})
            continue
        first = dict(complete[0])
        people = {person["id"]: dict(person) for person in first["participants"]}
        for participant_id, person in people.items():
            labels = {name_key(label): label for record in complete for row in record["participants"]
                if row["id"] == participant_id for label in row["name_options"]}
            person["name_options"] = list(labels.values())
        first["participants"] = list(people.values())
        first.update(review)
        output.append(first)
    return output


def native_yes_propositions(markets, identity_candidates, provider_results, native_entities, *, source_bindings):
    """Interpret the inspected literal YES-subtitle + full-game winner syntax.

    Never read no_sub_title as the opposing participant: inspected native YES
    and NO subtitles usually repeat the same proposition label. Only exact
    native-UUID labels or literal provider labels tied to the proved participant
    ID can resolve the YES name. Unsupported syntax remains an exclusion.
    """
    identities = {row["event_ticker"]: row for row in identity_candidates}
    results = {(row["sport"], row["provider_game_id"]): row for row in provider_results}
    require(len(identities) == len(identity_candidates) and len(results) == len(provider_results), "unique identity/result inputs required for YES parser")
    propositions, exclusions = [], []
    for market in markets:
        ticker, label, title = market.get("ticker"), market.get("yes_sub_title"), market.get("title")
        identity = identities.get(market.get("event_ticker"))
        reason = None
        if identity is None or identity.get("provider_game_id") is None:
            reason = "native_yes_missing_unique_game_identity"
        elif not isinstance(label, str) or not label or label != label.strip() or not isinstance(title, str):
            reason = "native_yes_subtitle_missing_or_invalid"
        result = results.get((identity["sport"], identity["provider_game_id"])) if identity else None
        if reason is None and (result is None or result.get("reason") is not None):
            reason = "native_yes_provider_result_identity_missing"
        names_by_id = {}
        if reason is None:
            for mapping in identity["participant_mapping"]:
                native_id = mapping.get("native_participant_id")
                entity = native_entities.get(native_id)
                people = [person for person in result["participants"] if mapping["provider_name_key"] in {name_key(value) for value in person["name_options"]}]
                if entity is None or len(people) != 1:
                    reason = "native_yes_participant_uuid_name_proof_missing"
                    break
                names_by_id[native_id] = {name_key(value) for value in (*entity["name_options"], *people[0]["name_options"])}
        def named(value):
            return [native_id for native_id, labels in names_by_id.items() if name_key(value) in labels]
        kind, native_id, syntax = None, None, None
        if reason is None:
            label_matches = named(label)
            draw = identity["sport"] == "epl" and name_key(label) in ("tie", "draw")
            if not draw and len(label_matches) != 1:
                reason = "native_yes_label_not_unique_proved_participant"
            else:
                native_id = label_matches[0] if label_matches else None
                pair_title = re.sub(r"^Game [0-9]+: ", "", title)
                pair = _pair(pair_title) if pair_title.endswith(" Winner?") else None
                win = re.fullmatch(r"(.+) wins", title)
                # These are the inspected *full-match* legacy templates, not a
                # wildcard between 'win the' and 'match'. The named inner pair
                # must also be a literal native event/milestone pair. Unknown
                # templates (including first-set/first-X) remain unadmitted.
                question = re.fullmatch(r"Will (.+?) win the ([^:?!]+) vs ([^:?!]+): Round Of (2|4|8|16|32|64|128) match\?", title) if identity["sport"] == "atp" else None
                if question is not None and {name_key(question.group(2)), name_key(question.group(3))} not in [
                    {name_key(value) for value in pair_labels} for pair_labels in identity.get("native_pair_labels", [])]:
                    question = None
                round_question = re.fullmatch(r"Will (.+?) be a winner of the round of (?:2|4|8|16|32|64|128) of Wimbledon Men Singles\?", title) if identity["sport"] == "atp" else None
                if pair is not None:
                    assignments = [named(value) for value in pair["names"]]
                    if all(len(values) == 1 for values in assignments) and len({values[0] for values in assignments}) == 2:
                        syntax = "explicit_pair_Winner_question_with_named_YES_subtitle"
                elif win or question or round_question:
                    captured = (win or question or round_question).group(1)
                    if not draw and named(captured) == [native_id]:
                        syntax = "explicit_named_match_winner_title_and_agreeing_YES_subtitle"
                elif draw and title == "Tie is the result":
                    syntax = "explicit_EPL_tie_result_title_and_Tie_YES_subtitle"
                if syntax is None:
                    reason = "unadmitted_native_yes_winner_title_syntax_or_labels"
                else:
                    kind = "draw" if draw else "named_participant"
        if reason is not None:
            exclusions.append({"ticker": ticker, "reason": reason})
            continue
        source = source_bindings.get(ticker)
        require(isinstance(source, Mapping) and source.get("path") and re.fullmatch(r"[0-9a-f]{64}", source.get("sha256", "")), "explicit per-market metadata body binding required")
        propositions.append({"ticker": ticker, "kind": kind, "native_participant_id": native_id,
            "field": "yes_sub_title", "text": label, "native_title": title, "syntax_rule": syntax,
            "reviewed": True, "review_basis": "inspected native full-game winner field contract, exact participant-label proof",
            "source_binding": dict(source)})
    return {"propositions": propositions, "exclusions": exclusions}


def prove_market_rows(markets, identity_candidates, provider_results, propositions):
    """Fixture-accessible strict map/proof core; no files or acceptance claim.

    A proposition is an explicit, source-bound reviewed interpretation of an
    actual native title/question/YES-subtitle field. Until actual source syntax
    is admitted, the caller must not manufacture these records from event order
    or ticker suffixes. The guarded reader will own body/source admission.
    """
    identities, results, props = {}, {}, {}
    for row in identity_candidates:
        require(row["event_ticker"] not in identities, "duplicate event identity candidate")
        identities[row["event_ticker"]] = row
    for row in provider_results:
        key = (row["sport"], row["provider_game_id"])
        require(key not in results, "provider result snapshots require independent exact-ID reconciliation")
        results[key] = row
    for row in propositions:
        require(_id(row.get("ticker")) and row["ticker"] not in props, "unique explicit native proposition records required")
        props[row["ticker"]] = row
    accepted, proofs, exclusions, seen, clocks = [], [], [], set(), {}
    for market in markets:
        ticker = market.get("ticker")
        require(_id(ticker) and ticker not in seen, "unique native market ticker required")
        seen.add(ticker)
        identity = identities.get(market.get("event_ticker"))
        prop, reason = props.get(ticker), None
        if market.get("market_reason") != "eligible" or market.get("status") != "finalized" or market.get("market_type") != "binary" or market.get("combo_state") != "single":
            reason = "native_market_not_admitted_final_binary_single"
        elif market.get("unit_payoff_admitted") is not True or market.get("face_value_dollars") != 1 or market.get("result") not in ("yes", "no"):
            reason = "native_binary_unit_result_unproved"
        elif identity is None or identity.get("status") != "identity_candidate_pending_claim_result_proof" or identity.get("reason") is not None:
            reason = "missing_unique_game_identity"
        elif market.get("sport") != identity["sport"]:
            reason = "native_market_identity_sport_disagreement"
        elif prop is None or prop.get("reviewed") is not True:
            reason = "native_yes_proposition_unproved"
        elif prop.get("field") not in ("title", "question", "yes_sub_title") or not isinstance(prop.get("text"), str) or not prop["text"] or market.get(prop["field"]) != prop["text"]:
            reason = "native_yes_proposition_field_binding_differs"
        elif not isinstance(prop.get("source_binding"), Mapping) or not prop["source_binding"].get("path") or not re.fullmatch(r"[0-9a-f]{64}", prop["source_binding"].get("sha256", "")):
            reason = "native_yes_proposition_source_unproved"
        result = results.get((identity["sport"], identity["provider_game_id"])) if identity else None
        # A failed completed provider reconciliation is the actual blocker,
        # even when the upstream proposition parser also correctly withheld a
        # proposition because that same provider result was unavailable.
        if reason in (None, "native_yes_proposition_unproved") and (result is None or result.get("reason") is not None or result.get("completed") is not True):
            reason = "independent_provider_completed_result_missing"
        participant_to_id, target_id, payout = {}, None, None
        if reason is None:
            people = result.get("participants", [])
            if len(people) != 2 or len({person["id"] for person in people}) != 2:
                reason = "provider_result_pair_unproved"
            elif type(result.get("is_draw")) is not bool or (result["is_draw"] and (identity["sport"] != "epl" or result.get("winner_participant_id") is not None)) or (not result["is_draw"] and result.get("winner_participant_id") not in {person["id"] for person in people}):
                reason = "provider_winner_or_draw_identity_unproved"
            elif not isinstance(result.get("source_binding"), Mapping) or not result["source_binding"].get("path") or not re.fullmatch(r"[0-9a-f]{64}", result["source_binding"].get("sha256", "")) or not result.get("source_pointer"):
                reason = "provider_result_source_field_binding_missing"
            else:
                for mapping in identity["participant_mapping"]:
                    matches = [person["id"] for person in people if mapping["provider_name_key"] in {name_key(value) for value in person["name_options"]}]
                    if len(matches) != 1:
                        reason = "provider_result_participant_name_identity_ambiguous"
                        break
                    participant_to_id[mapping["native_participant_id"]] = matches[0]
                if reason is None and (len(participant_to_id) != 2 or len(set(participant_to_id.values())) != 2):
                    reason = "provider_result_participant_mapping_not_bijective"
        if reason is None:
            kind = prop.get("kind")
            if kind in ("home", "away"):
                if identity.get("home_away_proposition_admissible") is not True:
                    reason = "home_away_relative_proposition_orientation_unproved"
                else:
                    target_id = participant_to_id.get(identity["native_orientation"][kind + "_native_id"])
            elif kind == "named_participant":
                target_id = participant_to_id.get(prop.get("native_participant_id"))
                if target_id is None:
                    reason = "named_yes_participant_not_in_proved_game"
            elif kind == "draw" and identity["sport"] == "epl":
                payout = int(result.get("is_draw") is True)
            else:
                reason = "unadmitted_yes_proposition_kind"
            if reason is None and payout is None:
                if target_id is None:
                    reason = "yes_participant_identity_unproved"
                else:
                    payout = int(not result.get("is_draw") and result.get("winner_participant_id") == target_id)
            if reason is None and payout != int(market["result"] == "yes"):
                reason = "native_provider_yes_result_disagreement"
        if reason is not None:
            provider_failure = reason in {"independent_provider_completed_result_missing", "provider_result_pair_unproved",
                "provider_winner_or_draw_identity_unproved", "provider_result_source_field_binding_missing",
                "provider_result_participant_name_identity_ambiguous", "provider_result_participant_mapping_not_bijective",
                "native_provider_yes_result_disagreement"}
            receipt, receipt_hash = (None, None)
            if provider_failure:
                receipt, receipt_hash = _canonical_receipt({"exclusion_reason": reason, "sport": market.get("sport"),
                    "provider_game_id": identity.get("provider_game_id") if identity else None, "provider_result": result})
            exclusions.append({"ticker": ticker, "native_event_ticker": market.get("event_ticker"), "sport": market.get("sport"), "reason": reason,
                "provider_result_receipt_json": receipt, "provider_result_receipt_sha256": receipt_hash})
            continue
        start, end = _utc(identity.get("actual_start_utc")), _utc(identity.get("actual_end_utc"))
        require(start is not None and end is not None and end > start, "proved game clocks invalid")
        game_key = "provider:" + identity["sport"] + ":" + identity["provider_game_id"]
        quality = identity["timing_qualification"]
        raw_quality = identity.get("raw_provider_timing_quality")
        if raw_quality is not None:
            require(isinstance(raw_quality, str) and bool(raw_quality.strip()), "literal provider timing quality required")
            quality += " | provider_quality=" + raw_quality
        clock = (start.timestamp(), end.timestamp(), quality)
        require(game_key not in clocks or clocks[game_key] == clock, "provider game clocks differ across native markets")
        clocks[game_key] = clock
        identity_json, identity_hash = _canonical_receipt(identity)
        result_json, result_hash = _canonical_receipt({"native_ticker": ticker, "native_result": market["result"],
            "native_yes_proposition": dict(prop), "native_to_provider_participant_ids": participant_to_id,
            "provider_result": result, "yes_payout": payout})
        proofs.append({"ticker": ticker, "identity_receipt_json": identity_json, "identity_receipt_sha256": identity_hash,
            "result_receipt_json": result_json, "result_receipt_sha256": result_hash})
        accepted.append({"ticker": ticker, "sport": identity["sport"], "game_key": game_key,
            "actual_start_seconds": clock[0], "actual_end_seconds": clock[1], "timing_quality": clock[2],
            "provenance": json.dumps({"identity_receipt_sha256": identity_hash, "result_receipt_sha256": result_hash,
                "raw_provider_timing_quality": identity.get("raw_provider_timing_quality")}, sort_keys=True),
            "yes_payout": payout, "native_result": market["result"], "native_yes_result_verified": True,
            "native_event_ticker": market["event_ticker"], "provider_game_id": identity["provider_game_id"],
            "identity_receipt_sha256": identity_hash, "result_receipt_sha256": result_hash})
    require(len(accepted) + len(exclusions) == len(seen), "sports map market reconciliation failed")
    return {"status": "sports_map_rows_complete_pending_independent_audit", "market_map": accepted,
        "binding_proofs": proofs, "exclusions": exclusions, "source_markets": len(seen), "independent_acceptance": "not_created"}


def build_sports_map_stage(*, config_path, config_sha256, output_dir, expected_source_head):
    """Guarded, immutable metadata-only producer; never writes acceptance.json.

    Config contains explicit bound files metadata_manifest/native_events/
    native_milestones/native_entities/provider_binding/provider_index/mlb_results/
    alias_receipt, metadata_source_head, provider_cache_root, and a reader-facing
    timing_qualification. Paths are never guessed. Retained provider role paths
    and hashes come from the reviewed binding, including the MLB nine-column
    projection that proves the retained subset. No API or raw-price read exists.
    """
    import os
    import shutil
    import tempfile
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    import production_guard
    from scripts import build_kaushik_kalshi_inputs as runner
    from analysis.kaushik_polymarket_replication import build_inputs as io

    production_guard.require_production_host()
    require(Path(__file__).resolve().parents[2] == REPO_ROOT, "canonical sports map source required")
    require(Path(sys_executable()).resolve() == PRODUCTION_PYTHON.resolve(), "canonical production Python required")
    source = runner.source_snapshot(expected_source_head)
    target = Path(output_dir)
    require(target.is_absolute() and target.resolve() == target and target.parent == RUN_ROOT and not os.path.lexists(target), "new top-level sports map stage required")
    require(shutil.disk_usage(target.parent).free >= 22_000_000_000, "sports map output/transient/20GB floor reservation failed")
    observed, bindings = {}, {}

    def bound(record, *, maximum=100_000_000, json_body=False):
        require(isinstance(record, Mapping) and type(record.get("bytes")) is int and 0 < record["bytes"] <= maximum
            and re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", "")), "explicit bounded sports input identity required")
        path = Path(record.get("path", ""))
        require(path.is_absolute() and path.resolve(strict=True) == path and path.is_file() and not path.is_symlink()
            and path.is_relative_to(DATA_ROOT) and not path.is_relative_to(target), "canonical regular sports input required")
        before = io.stat_identity(path)
        require(before["bytes"] == record["bytes"], "sports input byte size differs")
        raw = path.read_bytes() if json_body else None
        require((hashlib.sha256(raw).hexdigest() if raw is not None else io.sha256(path)) == record["sha256"]
            and io.stat_identity(path) == before, "sports input body hash/drift gate failed")
        observed[str(path)] = before
        bindings[str(path)] = dict(record)
        if not json_body:
            return path
        value = json.loads(raw, object_pairs_hook=runner._unique,
            parse_constant=lambda token: (_ for _ in ()).throw(BindingBlocked("nonfinite sports JSON value: " + token)))
        require(type(value) is dict, "sports JSON object required")
        return value

    config_record = {"path": str(Path(config_path)), "bytes": Path(config_path).stat().st_size, "sha256": config_sha256}
    config = bound(config_record, maximum=1_000_000, json_body=True)
    require(config.get("schema_version") == "kaushik_kalshi_sports_map_build_config_v1" and config.get("status") == "reviewed", "reviewed sports build config required")
    require(isinstance(config.get("timing_qualification"), str) and config["timing_qualification"].strip(), "explicit reader-facing clock qualification required")
    files = config.get("files", {})
    roles = {"metadata_manifest", "native_events", "native_milestones", "native_entities", "provider_binding", "provider_index", "mlb_results", "alias_receipt"}
    require(isinstance(files, Mapping) and set(files) == roles, "exact explicit sports source-role inventory required")
    metadata = bound(files["metadata_manifest"], maximum=1_000_000, json_body=True)
    metadata_root = Path(files["metadata_manifest"]["path"]).parent
    require(not (metadata_root / "failure.json").exists() and metadata.get("schema_version") == "kaushik_kalshi_metadata_stage_v1"
        and metadata.get("status") == "metadata_complete_pending_independent_audit"
        and metadata.get("source", {}).get("head") == config.get("metadata_source_head"), "complete pinned native metadata stage required")
    require(re.fullmatch(r"[0-9a-f]{40}", config.get("metadata_source_head", "")), "explicit metadata source commit required")
    require(metadata.get("counts") == {"source_rows": runner.EXPECTED_METADATA_ROWS, "source_shards": runner.EXPECTED_METADATA_FILES}, "full native metadata source inventory differs")
    def relative_record(record):
        require(isinstance(record, Mapping), "metadata artifact binding absent")
        path = runner._relative(metadata_root, record.get("path"))
        return {**record, "path": str(path)}
    global_proof = bound(relative_record(metadata.get("global_metadata_proof")), maximum=1_000_000, json_body=True)
    health = global_proof.get("health", {})
    require(global_proof.get("status") == "structural_global_metadata_keys_passed" and global_proof.get("source") == metadata["source"]
        and health.get("rows") == health.get("ticker_groups") == health.get("nonnull_ticker_groups") == runner.EXPECTED_METADATA_ROWS
        and health.get("duplicate_ticker_keys") == health.get("invalid_ticker_rows") == health.get("conflicting_result_keys") == health.get("conflicting_status_keys") == 0,
        "native metadata global-key proof incomplete")
    expected_shards = health.get("expected_source_shards")
    require(isinstance(expected_shards, list) and len(expected_shards) == len(set(expected_shards)) == runner.EXPECTED_METADATA_FILES
        and set(health.get("observed_source_shards", [])) == set(expected_shards), "native metadata all-source-shard proof incomplete")
    expected_artifacts = {label + "_" + Path(shard).stem: str(Path("artifacts") / label / Path(shard).name)
        for shard in expected_shards for label in ("metadata", "sports", "exclusions")}
    require(type(metadata.get("artifacts")) is dict and set(metadata["artifacts"]) == set(expected_artifacts)
        and len({record.get("path") for record in metadata["artifacts"].values()}) == len(expected_artifacts)
        and all(record.get("path") == expected_artifacts[key] for key, record in metadata["artifacts"].items()), "exact sports metadata role/path inventory differs")
    provider_binding = bound(files["provider_binding"], maximum=1_000_000, json_body=True)
    require(provider_binding.get("schema_version") == "kaushik_replication_sports_binding_v1"
        and provider_binding.get("provider_cohort_admitted") is True and provider_binding.get("retained_identity_result_timing_proof") is True,
        "reviewed retained provider cohort required")
    provider_roles = {row["role"]: row for row in provider_binding.get("inputs", [])}
    require(len(provider_roles) == len(provider_binding.get("inputs", [])) and {"six_timing", "six_proof", "nfl_moneylines", "nba_moneylines", "mlb_phase"} <= set(provider_roles), "retained provider roles missing/duplicated")
    def provider_path(role, *, maximum=100_000_000):
        record = provider_roles[role]
        path = Path(record["path"])
        return bound({**record, "bytes": path.stat().st_size}, maximum=maximum)
    events_path, milestones_path, entities_path = [bound(files[role]) for role in ("native_events", "native_milestones", "native_entities")]
    index = bound(files["provider_index"], maximum=5_000_000, json_body=True)
    alias_review = bound(files["alias_receipt"], maximum=1_000_000, json_body=True)
    require(alias_review.get("schema_version") == "kaushik_kalshi_sports_alias_semantic_review_v1"
        and alias_review.get("status") == "root_semantic_review_recorded_pending_field_reopen", "explicit semantic alias review required")
    cache_root = Path(config.get("provider_cache_root", ""))
    require(cache_root.is_absolute() and cache_root.resolve(strict=True) == cache_root and cache_root.is_dir()
        and cache_root.is_relative_to(DATA_ROOT) and not cache_root.is_relative_to(target), "explicit canonical saved provider cache required")
    c = duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    c.execute("SET threads=1")
    c.execute("SET max_temp_directory_size='0B'")
    c.execute("SET TimeZone='UTC'")
    c.execute("SET enable_object_cache=false")
    def rows(query):
        cursor = c.execute(query)
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]
    def table(path, columns="*", where=None):
        return rows("SELECT " + columns + " FROM read_parquet(" + io.literal(path) + ",hive_partitioning=false)" + (" WHERE " + where if where else ""))
    stage, published = None, False
    try:
        selected = "series_ticker IN (" + ",".join(io.literal(key) for key in WINNER_SERIES) + ")"
        native_events = table(events_path, "event_ticker,series_ticker,title,sub_title", selected)
        selected_ids = {row["event_ticker"] for row in native_events}
        # Only compact, already-authorized native identity metadata are read.
        native_milestones = table(milestones_path, "id,primary_event_tickers,title,details,start_date,source_id,source_ids")
        native_milestones = [row for row in native_milestones if any(key in selected_ids for key in _json(row["primary_event_tickers"], list))]
        entities = entity_identities(table(entities_path, "id,name,type,details,source_id,source_ids",
            "type IN ('baseball_team','football_team','basketball_team','hockey_team','soccer_team','tennis_competitor')"), evidence="pinned native structured targets " + files["native_entities"]["sha256"])
        native = native_candidates(native_events, native_milestones, entities=entities)
        timing_path, proof_path = provider_path("six_timing"), provider_path("six_proof")
        allowed_sports = "sport IN ('atp','cbb','cfb','epl','nhl','wnba')"
        timing = table(timing_path, "sport,event_slug,game_id,market_date,actual_start_utc,actual_end_utc,timing_quality", allowed_sports)
        old_proof = table(proof_path, "sport,event_slug,provider_competition_id,provider_start_utc,provider_participants,provider_event_name", "eligible AND " + allowed_sports)
        proof_index = {(row["sport"], row["event_slug"]): row for row in old_proof}
        require(len(proof_index) == len(old_proof), "retained provider identity proof join is not unique")
        game_rows = []
        for row in timing:
            proof = proof_index.get((row["sport"], row["event_slug"]))
            require(proof is not None and proof["provider_competition_id"] == row["game_id"], "retained game/competition identity proof differs")
            names = [value.strip() for value in proof["provider_participants"].split("|")]
            orientation = _pair(proof["provider_event_name"])
            game_rows.append({"sport": row["sport"], "provider_game_id": row["game_id"], "participant_names": names,
                "market_date": row["market_date"], "scheduled_start_utc": proof["provider_start_utc"],
                "actual_start_utc": row["actual_start_utc"], "actual_end_utc": row["actual_end_utc"],
                "timing_qualification": "scheduled_start_plus_archived_duration" if row["sport"] == "atp" else "retained_provider_live_play_boundaries",
                "raw_provider_timing_quality": row["timing_quality"], "evidence": str(timing_path) + " + exact eligible " + str(proof_path),
                "source_bindings": {"timing": bindings[str(timing_path)], "identity": bindings[str(proof_path)]},
                **({"away_name": orientation["names"][0], "home_name": orientation["names"][1]} if orientation and orientation["orientation"] == "away_home"
                   and {name_key(value) for value in orientation["names"]} == {name_key(value) for value in names} else {})})
        result_rows = []
        for sport, role in (("nfl", "nfl_moneylines"), ("nba", "nba_moneylines"), ("mlb", "mlb_phase")):
            retained_path = provider_path(role, maximum=200_000_000)
            if sport == "mlb":
                fields = "market_id,game_pk,official_date,away_team_id,away_team_name,home_team_id,home_team_name,actual_start_utc,actual_end_utc"
                projection = rows("SELECT DISTINCT " + fields + " FROM read_parquet(" + io.literal(retained_path) + ")")
                require(len({row["game_pk"] for row in projection}) == len({row["market_id"] for row in projection}) == len(projection) == EXPECTED_MLB_RETAINED_GAMES, "retained MLB nine-column identity projection differs")
                outcome_path = bound(files["mlb_results"])
                candidates = table(outcome_path, "game_pk,official_date,away_team_id,away_team_name,home_team_id,home_team_name,winning_team_id,scheduled_start_utc,actual_start_utc,actual_end_utc")
                by_id = {row["game_pk"]: row for row in candidates}
                require(len(by_id) == len(candidates), "MLB outcome game IDs not unique")
                source_rows = []
                for projection_row in projection:
                    require(projection_row["game_pk"] in by_id, "retained MLB game lacks outcome")
                    row = by_id[projection_row["game_pk"]]
                    require(all(row[name] == projection_row[name] for name in projection_row if name != "market_id"), "retained MLB identity/clock fields disagree with outcome table")
                    source_rows.append(row)
            else:
                outcome_path = retained_path
                source_rows = table(retained_path, "game_id,official_date,away_team_id,away_team_name,home_team_id,home_team_name,winning_team_id,scheduled_start_utc,actual_start_utc,actual_end_utc")
            for row in source_rows:
                game_id = str(row["game_pk"] if sport == "mlb" else row["game_id"])
                names = [row["away_team_name"], row["home_team_name"]]
                game_rows.append({"sport": sport, "provider_game_id": game_id, "participant_names": names,
                    "away_name": names[0], "home_name": names[1], "market_date": row["official_date"],
                    "scheduled_start_utc": row["scheduled_start_utc"], "actual_start_utc": row["actual_start_utc"], "actual_end_utc": row["actual_end_utc"],
                    "timing_qualification": "retained_provider_live_play_boundaries", "evidence": str(outcome_path),
                    "source_bindings": {"identity_result_timing": bindings[str(outcome_path)],
                        **({"retained_subset_nine_metadata_columns": bindings[str(retained_path)]} if sport == "mlb" else {})}})
                result_rows.append({"sport": sport, "provider_game_id": game_id, "completed": True, "reason": None, "is_draw": False,
                    "winner_participant_id": str(row["winning_team_id"]), "participants": [
                        {"id": str(row[side + "_team_id"]), "name_options": [row[side + "_team_name"]]} for side in ("away", "home")],
                    "completed_status": "retained audited finalized provider moneyline identity/result/timing contract",
                    "source_binding": bindings[str(outcome_path)], "source_pointer": "game_pk=" + game_id if sport == "mlb" else "game_id=" + game_id})
        targets = {(row["sport"], row["provider_game_id"]) for row in game_rows if row["sport"] not in ("mlb", "nba", "nfl")}
        entries = [row for row in index.get("files", []) if len(row["path"].split("/")) == 3 and row["path"].split("/")[0] == "scoreboards"
            and row["path"].split("/")[1] in {"atp", "cbb", "cfb", "epl", "nhl", "wnba"}]
        require(entries and len({row["path"] for row in entries}) == len(entries) and sum(row["bytes"] for row in entries) <= 1_000_000_000, "bounded explicit saved scoreboard inventory required")
        for entry in entries:
            require("\\" not in entry["path"] and all(part not in ("", ".", "..") for part in entry["path"].split("/"))
                and not Path(entry["path"]).is_absolute(), "provider cache path traversal")
            payload = bound({**entry, "path": str(cache_root / entry["path"])}, maximum=5_000_000, json_body=True)
            result_rows.extend(row for row in scoreboard_results(payload, entry["path"].split("/")[1], source_binding=bindings[str(cache_root / entry["path"])])
                if (row["sport"], row["provider_game_id"]) in targets)
        results = reconcile_result_snapshots(result_rows)
        games = provider_games(game_rows)
        for alias in alias_review.get("approved_aliases", []):
            for role in ("native_source", "provider_source"):
                source_record = alias.get(role, {})
                reopened = bindings.get(source_record.get("path"), {})
                require(reopened.get("sha256") == source_record.get("sha256")
                    and (role != "native_source" or source_record.get("path") == str(entities_path)), "reviewed alias source was not reopened exactly")
        aliases = alias_receipts(alias_review.get("approved_aliases", []), native_entities=entities, providers=games)
        identities = bind_candidates(native, games, reviewed_aliases=aliases["aliases"])
        for identity in identities:
            identity["native_source_bindings"] = {role: files[role] for role in ("native_events", "native_milestones", "native_entities", "alias_receipt")}
        markets, market_sources = [], {}
        sports_records = [record for key, record in metadata.get("artifacts", {}).items() if key.startswith("sports_")]
        require(len(sports_records) == runner.EXPECTED_METADATA_FILES, "full sports metadata shard inventory required")
        for record in sports_records:
            path = bound(relative_record(record))
            footer = pq.ParquetFile(path)
            require(footer.metadata.num_rows == record.get("rows") and str(footer.schema_arrow) == record.get("schema"), "native sports metadata footer/schema differs")
            shard_rows = table(path)
            for row in shard_rows:
                require(row["ticker"] not in market_sources, "native sports ticker duplicated across metadata shards")
                market_sources[row["ticker"]] = bindings[str(path)]
            markets.extend(shard_rows)
        parsed = native_yes_propositions(markets, identities, results, entities, source_bindings=market_sources)
        output = prove_market_rows(markets, identities, results, parsed["propositions"])
        parser_reasons = {row["ticker"]: row["reason"] for row in parsed["exclusions"]}
        for row in output["exclusions"]:
            if row["reason"] == "native_yes_proposition_unproved":
                row["reason"] = parser_reasons.get(row["ticker"], row["reason"])
        require(runner.source_snapshot(expected_source_head) == source, "sports map source drift")
        require(all(io.stat_identity(path) == identity for path, identity in observed.items()), "sports map input drift")
        stage = Path(tempfile.mkdtemp(prefix="." + target.name + ".staging-", dir=target.parent))
        schemas = {
            "market_map": pa.schema([(name, pa.float64() if name in ("actual_start_seconds", "actual_end_seconds") else pa.int8() if name == "yes_payout" else pa.bool_() if name == "native_yes_result_verified" else pa.string()) for name in (
                "ticker", "sport", "game_key", "actual_start_seconds", "actual_end_seconds", "timing_quality", "provenance", "yes_payout", "native_result", "native_yes_result_verified", "native_event_ticker", "provider_game_id", "identity_receipt_sha256", "result_receipt_sha256")]),
            "binding_proofs": pa.schema([(name, pa.string()) for name in ("ticker", "identity_receipt_json", "identity_receipt_sha256", "result_receipt_json", "result_receipt_sha256")]),
            "exclusions": pa.schema([(name, pa.string()) for name in ("ticker", "native_event_ticker", "sport", "reason", "provider_result_receipt_json", "provider_result_receipt_sha256")])}
        filenames = {"market_map": "sports_market_map.parquet", "binding_proofs": "sports_binding_proofs.parquet", "exclusions": "sports_binding_exclusions.parquet"}
        artifacts = {}
        for role, name in filenames.items():
            path = stage / name
            require(shutil.disk_usage(stage).free >= 22_000_000_000, "sports map 20GB floor/output reservation failed")
            value = pa.Table.from_pylist(output[role], schema=schemas[role])
            with io.copy_ceiling(1_000_000_000):
                pq.write_table(value, path, compression="zstd")
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            require(pq.read_table(path).equals(value), "sports map output failed exact reopen")
            artifacts[role] = {**io._output_info(path), "path": name}
        require(sum(record["bytes"] for record in artifacts.values()) <= 1_000_000_000, "sports map total output ceiling failed")
        manifest = {"schema_version": "kaushik_kalshi_sports_map_v1", "status": "sports_map_complete_pending_independent_audit",
            "source": source, "input_config": config_record, "input_metadata": files["metadata_manifest"], "input_bindings": bindings,
            "artifacts": artifacts, "counts": {"source_markets": len(markets), "mapped_markets": len(output["market_map"]), "excluded_markets": len(output["exclusions"])},
            "timing_qualification": config["timing_qualification"], "resources": {"memory_limit": "2GB", "threads": 1, "spill_bytes": 0, "output_bytes": 1_000_000_000, "minimum_free_bytes": 20_000_000_000},
            "independent_acceptance": "pending; producer never creates acceptance.json"}
        io.write_json(stage / "manifest.json", manifest)
        require(io.read_json(stage / "manifest.json")[0] == manifest, "sports map manifest failed reopen")
        require(runner.source_snapshot(expected_source_head) == source and all(io.stat_identity(path) == identity for path, identity in observed.items()),
            "sports map source/input drift before publication")
        descriptor = os.open(stage, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        io.atomic_publish(stage, target)
        published = True
        descriptor = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        require(io.read_json(target / "manifest.json")[0] == manifest and all(io._output_info(target / record["path"]) == {key: record[key] for key in ("bytes", "sha256", "rows", "schema", "row_groups")} for record in artifacts.values()), "published sports map failed reopen")
        require(shutil.disk_usage(target).free >= 20_000_000_000, "published sports map violates 20GB free floor")
        return manifest
    except BaseException as error:
        failed = target if published else stage
        if failed is not None and failed.exists() and not (failed / "failure.json").exists():
            io.write_json(failed / "failure.json", {"status": "sports_map_incomplete", "reason": str(error), "error_type": type(error).__name__})
        raise
    finally:
        c.close()


def sys_executable():
    import sys
    return sys.executable


def main(argv=None):
    """Explicit saved-input CLI; has no collection, discovery or bypass mode."""
    import argparse
    parser = argparse.ArgumentParser(description="Build strict native-YES/provider sports map proofs from pinned saved metadata")
    parser.add_argument("--config", required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--expected-source-head", required=True)
    args = parser.parse_args(argv)
    manifest = build_sports_map_stage(config_path=args.config, config_sha256=args.config_sha256,
        output_dir=args.output_dir, expected_source_head=args.expected_source_head)
    print(json.dumps({"status": manifest["status"], "output_dir": args.output_dir, "counts": manifest["counts"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
