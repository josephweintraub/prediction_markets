"""Build preliminary Polymarket MLB single-game moneyline candidates.

The pilot deliberately uses a narrow, inspectable definition:

* ``event_slug`` fully matches ``mlb-<team-1>-<team-2>-YYYY-MM-DD``;
* the question is present and contains no colon; and
* exactly one candidate market exists for each event slug.

Rows with an ``mlb-`` prefix are retained in a diagnostics table, including
the reason a row failed the definition.  Duplicate candidates raise instead
of being ranked or silently deduplicated.  These are preliminary candidates,
not confirmed games; the legacy ``away``/``home`` output columns preserve the
observed slug positions and a later step determines their official orientation
from MLB schedule data.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb


MLB_SLUG_PATTERN = r"^mlb-([a-z0-9]+)-([a-z0-9]+)-(\d{4}-\d{2}-\d{2})$"
REQUIRED_COLUMNS = (
    "market_id",
    "event_slug",
    "question",
    "n_tokens",
    "n_trades_raw",
    "n_buy_filtered",
    "usd_buy_filtered",
    "first_trade_at",
    "last_trade_at",
)
OUTPUT_COLUMNS = (
    "market_id",
    "event_slug",
    "date",
    "away",
    "home",
    "question",
    "n_tokens",
    "n_trades_raw",
    "n_buy_filtered",
    "usd_buy_filtered",
    "first_trade_at",
    "last_trade_at",
)


def _quote_path(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve()).replace("'", "''")


def _validate_relation(con: duckdb.DuckDBPyConnection, relation: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError(f"Unsafe DuckDB relation name: {relation!r}")
    columns = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    }
    missing = sorted(set(REQUIRED_COLUMNS) - columns)
    if missing:
        raise ValueError(f"MLB market source is missing required columns: {missing}")


def diagnostic_query(relation: str) -> str:
    """Return the query that classifies every MLB-prefixed source row."""

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation):
        raise ValueError(f"Unsafe DuckDB relation name: {relation!r}")
    pattern = MLB_SLUG_PATTERN.replace("'", "''")
    return f"""
        WITH parsed AS (
            SELECT
                market_id,
                event_slug,
                question,
                n_tokens,
                n_trades_raw,
                n_buy_filtered,
                usd_buy_filtered,
                first_trade_at,
                last_trade_at,
                regexp_full_match(event_slug, '{pattern}') AS slug_matches,
                NULLIF(regexp_extract(event_slug, '{pattern}', 1), '') AS away,
                NULLIF(regexp_extract(event_slug, '{pattern}', 2), '') AS home,
                TRY_CAST(NULLIF(regexp_extract(event_slug, '{pattern}', 3), '') AS DATE) AS date
            FROM {relation}
            WHERE event_slug ILIKE 'mlb-%'
        )
        SELECT
            *,
            CASE
                WHEN NOT slug_matches THEN 'slug_pattern_mismatch'
                WHEN date IS NULL THEN 'invalid_date'
                WHEN question IS NULL OR TRIM(question) = '' THEN 'missing_question'
                WHEN contains(question, ':') THEN 'question_contains_colon'
                ELSE NULL
            END AS exclusion_reason,
            exclusion_reason IS NULL AS is_candidate
        FROM parsed
    """


def candidate_query(relation: str) -> str:
    """Return the ordered strict-candidate query for an already validated relation."""

    diagnostics = diagnostic_query(relation)
    columns = ", ".join(f'"{column}"' for column in OUTPUT_COLUMNS)
    return f"""
        SELECT {columns}
        FROM ({diagnostics}) diagnostics
        WHERE is_candidate
        ORDER BY date, away, home, market_id
    """


def assert_one_candidate_per_event(
    con: duckdb.DuckDBPyConnection, relation: str
) -> None:
    """Fail loudly when the definition selects more than one market for an event."""

    diagnostics = diagnostic_query(relation)
    duplicates = con.execute(
        f"""
        SELECT event_slug, COUNT(*) AS candidate_count,
               list(market_id ORDER BY market_id) AS market_ids
        FROM ({diagnostics}) diagnostics
        WHERE is_candidate
        GROUP BY event_slug
        HAVING COUNT(*) > 1
        ORDER BY event_slug
        """
    ).fetchall()
    if duplicates:
        details = "; ".join(
            f"{slug}: {count} candidates {market_ids}"
            for slug, count, market_ids in duplicates
        )
        raise ValueError(f"Duplicate MLB moneyline candidates; refusing to guess: {details}")


def build_market_universe(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    output_path: str | Path,
    diagnostics_path: str | Path,
) -> dict[str, int]:
    """Validate, classify, and write candidate and diagnostic Parquet files."""

    _validate_relation(con, relation)
    assert_one_candidate_per_event(con, relation)

    output = Path(output_path).expanduser().resolve()
    diagnostics_output = Path(diagnostics_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_output.parent.mkdir(parents=True, exist_ok=True)

    diagnostics = diagnostic_query(relation)
    candidates = candidate_query(relation)
    con.execute(
        f"COPY ({diagnostics}) TO '{_quote_path(diagnostics_output)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    con.execute(
        f"COPY ({candidates}) TO '{_quote_path(output)}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    candidate_count = int(
        con.execute(f"SELECT COUNT(*) FROM ({candidates}) candidates").fetchone()[0]
    )
    diagnostic_count = int(
        con.execute(f"SELECT COUNT(*) FROM ({diagnostics}) diagnostics").fetchone()[0]
    )
    excluded_count = diagnostic_count - candidate_count
    return {
        "candidate_markets": candidate_count,
        "diagnostic_rows": diagnostic_count,
        "excluded_rows": excluded_count,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", required=True, help="Input universe_markets Parquet")
    parser.add_argument("--output", required=True, help="Candidate-market Parquet output")
    parser.add_argument(
        "--diagnostics", required=True, help="MLB-prefixed candidate diagnostics Parquet"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    con = duckdb.connect()
    try:
        con.execute(
            f"CREATE VIEW source_markets AS "
            f"SELECT * FROM read_parquet('{_quote_path(args.markets)}')"
        )
        stats = build_market_universe(
            con, "source_markets", args.output, args.diagnostics
        )
    finally:
        con.close()
    print(
        f"Wrote {stats['candidate_markets']:,} candidates; "
        f"retained {stats['diagnostic_rows']:,} MLB-prefixed diagnostic rows "
        f"({stats['excluded_rows']:,} excluded)."
    )


if __name__ == "__main__":
    main()
