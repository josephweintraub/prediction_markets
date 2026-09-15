"""Build a strict, resolved moneyline candidate universe for eight sport keys."""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint,
    fingerprint,
    fresh_run,
    quoted,
    write_json,
)

from .provider_extractors import split_matchup


SPORTS = ("nhl", "cbb", "cfb", "wnba", "epl", "atp", "wta", "ufc")
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})$")


def _read(con: duckdb.DuckDBPyConnection, query: str, params: list[Any]) -> list[dict[str, Any]]:
    cursor = con.execute(query, params)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _event_pair(sport: str, rows: list[dict[str, Any]]) -> tuple[str, str] | None:
    if sport == "epl":
        propositions = [
            str(row.get("group_item_title") or "").strip()
            for row in rows
            if row.get("group_item_title")
            and not str(row["group_item_title"]).strip().casefold().startswith("draw")
        ]
        distinct = sorted(set(propositions))
        if len(distinct) == 2:
            return distinct[0], distinct[1]
        draw_questions = [row["question"] for row in rows if "draw" in str(row["question"]).casefold()]
        return split_matchup(draw_questions[0]) if len(draw_questions) == 1 else None
    if len(rows) != 1:
        return None
    text = str(rows[0].get("group_item_title") or rows[0]["question"])
    if sport in {"atp", "wta"} and ":" in text:
        text = text.rsplit(":", 1)[1]
    return split_matchup(text)


def build_candidates(
    market_meta_path: str | Path,
    token_map_path: str | Path,
    universe_tokens_path: str | Path,
    run_dir: str | Path,
) -> dict[str, Any]:
    market_meta = Path(market_meta_path).expanduser().resolve()
    token_map = Path(token_map_path).expanduser().resolve()
    universe_tokens = Path(universe_tokens_path).expanduser().resolve()
    con = duckdb.connect()
    try:
        query = f"""
        WITH meta AS (
          SELECT condition_id::VARCHAR market_id, event_slug::VARCHAR event_slug,
                 question::VARCHAR question, group_item_title::VARCHAR group_item_title,
                 neg_risk::BOOLEAN neg_risk,
                 lower(regexp_extract(event_slug, '^[^-]+'))::VARCHAR sport,
                 try_cast(regexp_extract(event_slug, '(\\d{{4}}-\\d{{2}}-\\d{{2}})$', 1) AS DATE) market_date,
                 row_number() OVER (PARTITION BY condition_id ORDER BY event_slug, market_slug) rn
          FROM read_parquet(?)
          WHERE sports_market_type='moneyline'
            AND lower(regexp_extract(event_slug, '^[^-]+')) IN ({','.join('?' for _ in SPORTS)})
        ), tokens AS (
          SELECT t.condition_id::VARCHAR market_id, t.token_id::VARCHAR token_id,
                 t.outcome::VARCHAR outcome, u.winning_outcome::VARCHAR winning_outcome
          FROM read_parquet(?) t
          JOIN read_parquet(?) u
            ON u.market_id=t.condition_id AND u.token_id=t.token_id
        ), valid_markets AS (
          SELECT market_id, count(*) token_count,
                 count(DISTINCT token_id) distinct_tokens,
                 count(DISTINCT outcome) distinct_outcomes,
                 count(DISTINCT winning_outcome) winner_labels,
                 sum((outcome=winning_outcome)::INTEGER) winning_tokens
          FROM tokens GROUP BY market_id
        )
        SELECT m.market_id,m.event_slug,m.question,m.group_item_title,m.neg_risk,
               m.sport,m.market_date
        FROM meta m JOIN valid_markets v USING(market_id)
        WHERE m.rn=1 AND m.market_date IS NOT NULL
          AND v.token_count=2 AND v.distinct_tokens=2 AND v.distinct_outcomes=2
          AND v.winner_labels=1 AND v.winning_tokens=1
        ORDER BY m.sport,m.market_date,m.event_slug,m.market_id
        """
        markets = _read(
            con,
            query,
            [str(market_meta), *SPORTS, str(token_map), str(universe_tokens)],
        )
        token_rows = _read(
            con,
            """
            SELECT t.condition_id::VARCHAR market_id,t.token_id::VARCHAR token_id,
                   t.outcome::VARCHAR outcome,
                   (t.outcome=u.winning_outcome)::BOOLEAN won
            FROM read_parquet(?) t
            JOIN read_parquet(?) u
              ON u.market_id=t.condition_id AND u.token_id=t.token_id
            WHERE t.condition_id IN (SELECT unnest(?::VARCHAR[]))
            ORDER BY market_id,token_id
            """,
            [str(token_map), str(universe_tokens), [row["market_id"] for row in markets]],
        )
    finally:
        con.close()

    by_event: dict[tuple[str, str], list[dict[str, Any]]] = {}
    tokens_by_market: dict[str, list[dict[str, Any]]] = {}
    for row in markets:
        by_event.setdefault((row["sport"], row["event_slug"]), []).append(row)
    for row in token_rows:
        tokens_by_market.setdefault(row["market_id"], []).append(row)
    diagnostics: list[tuple[Any, ...]] = []
    accepted_market_ids: set[str] = set()
    accepted_events: list[tuple[Any, ...]] = []
    exclusions = Counter()
    for (sport, event_slug), rows in sorted(by_event.items()):
        expected_markets = 3 if sport == "epl" else 1
        reason = None
        if len(rows) != expected_markets:
            reason = "unexpected_markets_per_event"
        pair = _event_pair(sport, rows) if reason is None else None
        if pair is None or not all(part.strip() for part in pair) or pair[0].casefold() == pair[1].casefold():
            reason = reason or "unparseable_matchup"
        if reason is None and sport == "epl" and not all(row["neg_risk"] for row in rows):
            reason = "soccer_not_negative_risk"
        result_label = None
        if reason is None and sport != "epl":
            winners = [token["outcome"] for token in tokens_by_market[rows[0]["market_id"]] if token["won"]]
            if len(winners) != 1:
                reason = "invalid_event_resolution"
            else:
                result_label = winners[0]
        elif reason is None:
            winners = []
            for market in rows:
                yes_tokens = [
                    token for token in tokens_by_market[market["market_id"]]
                    if str(token["outcome"]).strip().casefold() == "yes" and token["won"]
                ]
                if yes_tokens:
                    winners.append(str(market.get("group_item_title") or market["question"]))
            if len(winners) != 1:
                reason = "invalid_event_resolution"
            else:
                result_label = "draw" if winners[0].strip().casefold().startswith("draw") else winners[0]
        if reason:
            exclusions[reason] += 1
        else:
            accepted_events.append((
                sport, event_slug, rows[0]["market_date"], pair[0].strip(), pair[1].strip(),
                result_label, len(rows), "accepted",
            ))
            accepted_market_ids.update(row["market_id"] for row in rows)
        for row in rows:
            diagnostics.append((
                sport, event_slug, row["market_id"], row["market_date"], row["question"],
                row["group_item_title"], row["neg_risk"], reason is None, reason,
            ))
    accepted_tokens = [row for row in token_rows if row["market_id"] in accepted_market_ids]
    if not accepted_events or not accepted_tokens:
        raise ValueError("Strict resolved moneyline candidate universe is empty")

    target = Path(run_dir).expanduser().resolve()
    inputs = (market_meta, token_map, universe_tokens)
    with fresh_run(target, inputs) as staging:
        out = duckdb.connect()
        try:
            out.execute("""CREATE TABLE events(
                sport VARCHAR,event_slug VARCHAR,market_date DATE,
                participant_1 VARCHAR,participant_2 VARCHAR,result_label VARCHAR,
                market_count INTEGER,status VARCHAR)""")
            out.executemany("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)", accepted_events)
            out.execute(f"COPY events TO '{quoted(staging/'candidate_events.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            out.execute("""CREATE TABLE diagnostics(
                sport VARCHAR,event_slug VARCHAR,market_id VARCHAR,market_date DATE,
                question VARCHAR,group_item_title VARCHAR,neg_risk BOOLEAN,
                accepted BOOLEAN,exclusion_reason VARCHAR)""")
            out.executemany("INSERT INTO diagnostics VALUES (?,?,?,?,?,?,?,?,?)", diagnostics)
            out.execute(f"COPY diagnostics TO '{quoted(staging/'candidate_diagnostics.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            out.execute("""CREATE TABLE tokens(
                market_id VARCHAR,token_id VARCHAR,outcome VARCHAR,won BOOLEAN)""")
            out.executemany(
                "INSERT INTO tokens VALUES (?,?,?,?)",
                [(row["market_id"], row["token_id"], row["outcome"], row["won"]) for row in accepted_tokens],
            )
            out.execute(f"COPY tokens TO '{quoted(staging/'candidate_tokens.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
            out.execute("""CREATE TABLE markets(
                sport VARCHAR,event_slug VARCHAR,market_id VARCHAR,market_date DATE,
                question VARCHAR,group_item_title VARCHAR,neg_risk BOOLEAN)""")
            out.executemany(
                "INSERT INTO markets VALUES (?,?,?,?,?,?,?)",
                [(row["sport"], row["event_slug"], row["market_id"], row["market_date"],
                  row["question"], row["group_item_title"], row["neg_risk"])
                 for row in markets if row["market_id"] in accepted_market_ids],
            )
            out.execute(f"COPY markets TO '{quoted(staging/'candidate_markets.parquet')}' (FORMAT PARQUET,COMPRESSION ZSTD)")
        finally:
            out.close()
        manifest = {
            "schema_version": 1,
            "stage": "multisport_strict_moneyline_candidates_v1",
            "sports": list(SPORTS),
            "filters": {
                "market_type": "sports_market_type = moneyline",
                "resolution": "exactly two distinct tokens/outcomes and one winning token",
                "soccer": "exactly three negative-risk binary propositions per event",
            },
            "counts": {
                "events": len(accepted_events),
                "markets": len(accepted_market_ids),
                "tokens": len(accepted_tokens),
                "events_by_sport": dict(Counter(row[0] for row in accepted_events)),
                "event_exclusions": dict(sorted(exclusions.items())),
            },
            "inputs": {"market_meta": fingerprint(market_meta), "token_map": fingerprint(token_map),
                       "universe_tokens": fingerprint(universe_tokens)},
        }
        write_json(staging / "candidate_manifest.json", manifest)
        manifest["outputs"] = {
            name: artifact_fingerprint(staging / name)
            for name in ("candidate_events.parquet", "candidate_markets.parquet",
                         "candidate_tokens.parquet", "candidate_diagnostics.parquet")
        }
        write_json(staging / "candidate_manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-meta", required=True)
    parser.add_argument("--token-map", required=True)
    parser.add_argument("--universe-tokens", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(json.dumps(build_candidates(args.market_meta, args.token_map, args.universe_tokens, args.run_dir), sort_keys=True))


if __name__ == "__main__":
    main()
