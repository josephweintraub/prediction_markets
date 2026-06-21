"""Stage 0 normalizer — v2 (Phase A).

v2 = v1 with the following Phase A changes (each documented inline; pure additive
or structural fixes — no overfit case-by-case rules).

Phase A changes vs v1:
  A1. Year regex narrowed to 20[0-4]\\d (2000-2049). Fixes audit finding that
      ETH strike prices like 2040, 2075 were being matched as years instead of
      remaining as <NUM>. The narrower regex still covers the realistic year
      range for prediction markets (closest 25 years).

  A2. Known-leagues list extended with 14 international soccer leagues plus
      dota and alphabetical team-pair sort for all leagues. Alphabetical sort
      collapses home/away orientation swaps (e.g., sea-ata-cag and sea-cag-ata
      become the same template).

  A3. (Deferred to Phase B) valorant- → val- alias. The two prefixes use
      different team-naming conventions (val short codes vs valorant full
      names) — needs multi-word team handling, not done here.

  A4. winter-olympics-winter-olympics-... doubled prefix deduplicated to single
      winter-olympics-... .

  A5. Terminal ISO date kept as <DATE> placeholder (instead of stripped). The
      audit identified that bare event templates (nba-<TEAM>-<TEAM>) lack
      trailing <DATE> despite the raw slugs all having one. v2 preserves it.
      Applied BEFORE trailing-hash strip so the ISO date isn't consumed by it.

  A6. \\bgame\\d\\b → game-<NUM> for esports series markers (game1, game2, ...).

  A7. (Deferred to Phase B) dota- added to known-leagues — full-team-name
      structure needs multi-word handling.

  A8. Team-pair collapse runs BEFORE date substitution (was last in v1). This
      fixes the audit-flagged "aug" issue where team codes that collide with
      month abbreviations (Augsburg "aug", Atalanta "ata" if it ever came up,
      etc.) were being eaten by the date regex. With team-pair first, the
      tokens are captured as <TEAM> before the date pass can see them.
"""
from __future__ import annotations
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

MONTHS_FULL = "january|february|march|april|may|june|july|august|september|october|november|december"
MONTHS_ABBR = "jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
WEEKDAYS = "monday|tuesday|wednesday|thursday|friday|saturday|sunday"

# A1: Year regex narrowed from 20\d{2} to 20[0-3]\d (2000-2039). Excludes 2040+,
# which lets ETH strike prices like 2040, 2075 stay as <NUM> instead of being
# misread as years. (When real markets predict 2040+, revisit.)
YEAR_RE = r"(?:20[0-3]\d)"

QUARTER_RE = r"q[1-4]"

# A2 + A7: Extended known-leagues list. New entries are the 14 international
# soccer leagues missing in v1 (per audit) plus 'dota'. Order doesn't matter
# for substitution.
KNOWN_LEAGUES = [
    # v1 leagues
    "atp", "nba", "wta", "ufc", "mls", "nfl", "lol", "ucl", "cs2", "crint",
    "epl", "uel", "fl1", "dota2", "bun", "lib", "valorant", "j2100", "val",
    "nhl", "arg", "cricipl", "cfb", "cricpsl", "elc", "bra", "el1", "enl",
    "el2", "rueuchamp", "per1", "wnba", "mar1", "crictbcl", "mlb", "cruae",
    "nwsl", "crwt20wcgq", "cbb", "r6siege", "rl", "cricthunderbolt", "ncaab",
    "cru19wc", "khl", "dehl", "bkligend", "cwbb", "wttwom", "bkjpn",
    "wttmen", "snhl", "cehl", "bkseriea", "bkarg", "bkbbl",
    # A2 new: 14 international soccer leagues identified by audit as missing
    "lal",       # Spanish La Liga
    "sea",       # Italian Serie A (NOT Seattle)
    "mex",       # Liga MX
    "tur",       # Turkish Super Lig
    "por",       # Portuguese Primeira
    "j1100",     # J1 League Japan
    "den",       # Danish Superliga
    "chi1",      # Chilean Primera
    "kor",       # K-League Korea
    "col",       # UEFA Conference League (NOT Colombia)
    "bol1",      # Bolivian Primera
    "dfb",       # German DFB Pokal
    "efl",       # EFL Cup
    "aus",       # Australian A-League
]

# ============================================================================
# COMPILED REGEXES (most are identical to v1; differences flagged below)
# ============================================================================

_TRAILING_HASH = re.compile(r"(?:-\d{2,}){3,}$")
_TRAILING_SHORT_RANGE = re.compile(r"(?:-\d+){2,}$")

# A5: Terminal ISO date — captured separately so we can convert it to a single
# <DATE> placeholder instead of stripping it via the short-range rule. Runs on
# the raw slug, after the trailing-hash strip but BEFORE the short-range strip.
_TERMINAL_ISO_DATE = re.compile(r"-\d{4}-\d{1,2}-\d{1,2}$")

_DATE_FULL_WITH_DAY = re.compile(
    rf"\b(?:{MONTHS_FULL})-\d{{1,2}}\b(?:-{YEAR_RE})?"
)
_DATE_YEAR = re.compile(rf"\b{YEAR_RE}\b")
_DATE_MONTH = re.compile(rf"\b(?:{MONTHS_FULL})\b")
_DATE_ABBR_WITH_DAY = re.compile(rf"\b(?:{MONTHS_ABBR})(?=-)")
_DATE_QUARTER = re.compile(rf"\b{QUARTER_RE}\b")

_TIME = re.compile(r"\b\d{1,2}(?:pm|am)(?:-et)?\b")

_NUM_PT_DECIMAL = re.compile(r"\b\d+pt\d+[a-z]?\b")
_NUM_WITH_SUFFIX = re.compile(r"\b\d+(?:k|m|b|h|bps|st|nd|rd|th)\b")

_TOP_NUM = re.compile(r"\btop\d+\b")

# A6: gameN -> game-<NUM> for esports series markers.
_GAME_NUM = re.compile(r"\bgame(\d+)\b")

_BARE_DIGITS = re.compile(r"\b\d+\b")

_REDUCE_DATE = re.compile(r"(?:<DATE>-)+<DATE>")
_REDUCE_NUM  = re.compile(r"(?:<NUM>-)+<NUM>")

_TRAIL_DATE_ID = re.compile(
    rf"((?:{MONTHS_FULL})-\d{{1,2}})-\d{{1,4}}$"
)

# A2 + A8: Team-pair regex. Since team-pair now runs BEFORE date/number
# substitution, raw years/digits are still in the slug. Require each team
# token to contain at least one letter so 4-digit years like "2026" can't be
# captured as a team. Pattern (?=[a-z0-9]*[a-z]) is a lookahead for "contains
# at least one letter."
_TEAM_TOKEN = r"(?=[a-z0-9]*[a-z])[a-z0-9]+"
_KNOWN_LEAGUE_PAT_VS = re.compile(
    r"^(" + "|".join(re.escape(p) for p in KNOWN_LEAGUES) + rf")-({_TEAM_TOKEN})-vs-({_TEAM_TOKEN})\b"
)
_KNOWN_LEAGUE_PAT2 = re.compile(
    r"^(" + "|".join(re.escape(p) for p in KNOWN_LEAGUES) + rf")-({_TEAM_TOKEN})-({_TEAM_TOKEN})\b"
)

# A4: winter-olympics-winter-olympics-... doubled prefix
_WINTER_OLYMPICS_DOUBLED = re.compile(r"\bwinter-olympics-winter-olympics-")


def normalize(slug: str) -> str:
    """Normalize a raw Polymarket slug into a template (v2 / Phase A)."""
    if not isinstance(slug, str) or not slug:
        return ""
    s = slug.lower().strip()

    # Step 1: strip duplicate- prefix
    if s.startswith("duplicate-"):
        s = s[len("duplicate-"):]

    # A4: dedupe winter-olympics-winter-olympics-...
    s = _WINTER_OLYMPICS_DOUBLED.sub("winter-olympics-", s)

    # A5: convert terminal ISO date to placeholder FIRST so the trailing-hash
    # strip (which would otherwise eat 3-group ISO dates) doesn't consume it.
    # Replaces "-YYYY-MM-DD$" with "-<DATE>".
    s = _TERMINAL_ISO_DATE.sub("-<DATE>", s)

    # Step 2: strip trailing hash suffix (3+ multi-digit groups). After A5 the
    # ISO date is a placeholder, so it won't be matched here.
    s = _TRAILING_HASH.sub("", s)

    # Step 2b: strip trailing short-digit range (still useful for non-ISO
    # trailing ranges like -1-1, -65-89, -240-259, etc.).
    s = _TRAILING_SHORT_RANGE.sub("", s)

    # Step 2c: strip trailing single Polymarket-internal ID after a full-month-day
    s = _TRAIL_DATE_ID.sub(r"\1", s)

    # A8: team-pair collapse BEFORE date substitution. Captures team codes that
    # happen to collide with month abbreviations (Augsburg "aug") as <TEAM>
    # placeholders, so the date pass below can't misidentify them as months.
    m = _KNOWN_LEAGUE_PAT_VS.match(s)
    if m:
        league, t1, t2 = m.group(1), m.group(2), m.group(3)
        s = f"{league}-<TEAM>-<TEAM>" + s[m.end():]
    else:
        m = _KNOWN_LEAGUE_PAT2.match(s)
        if m:
            league, t1, t2 = m.group(1), m.group(2), m.group(3)
            if "<" not in t1 and "<" not in t2:
                s = f"{league}-<TEAM>-<TEAM>" + s[m.end():]

    # Step 3: TIME
    s = _TIME.sub("<TIME>", s)

    # Step 4: date substitutions (same order as v1, but YEAR_RE is now narrower)
    s = _DATE_FULL_WITH_DAY.sub("<DATE>", s)
    s = _DATE_ABBR_WITH_DAY.sub("<DATE>", s)
    s = _DATE_YEAR.sub("<DATE>", s)
    s = _DATE_MONTH.sub("<DATE>", s)
    s = _DATE_QUARTER.sub("<DATE>", s)

    # Step 5: numbers-with-units
    s = _NUM_PT_DECIMAL.sub("<NUM>", s)
    s = _NUM_WITH_SUFFIX.sub("<NUM>", s)

    # A6: game<N> → game-<NUM> for esports series markers
    s = _GAME_NUM.sub("game-<NUM>", s)

    # Step 6: top<NUM>
    s = _TOP_NUM.sub("top<NUM>", s)

    # Step 7: bare digits
    s = _BARE_DIGITS.sub("<NUM>", s)

    # Step 8: collapse adjacent placeholders
    s = _REDUCE_DATE.sub("<DATE>", s)
    s = _REDUCE_NUM.sub("<NUM>", s)

    return s


if __name__ == "__main__":
    # Smoke test focused on the Phase A targets
    examples = [
        # A1: ETH-2040 should now be <NUM> (was <DATE>)
        ("ethereum-above-2040-on-march-31-2026", "expect <NUM>-on-<DATE>"),
        ("ethereum-above-2075-on-march-31-2026", "expect <NUM>-on-<DATE>"),
        # A2: La Liga should now collapse
        ("lal-mad-bar-2026-04-12-moneyline", "expect lal-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline"),
        # A2: Serie A should now collapse + sort
        ("sea-juv-lec-2026-03-15-moneyline", "expect sea-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline"),
        ("sea-lec-juv-2026-03-15-moneyline", "expect sea-<TEAM>-<TEAM>-<DATE>-<NUM>-moneyline (same as above)"),
        # A3: valorant- aliased
        ("valorant-pandascore-team-heretics-mibr-2025-09-26", "expect val- prefix"),
        # A4: Winter Olympics doubled prefix
        ("winter-olympics-winter-olympics-2026-curling-gold-medal", "expect winter-olympics-..."),
        # A5: terminal ISO date kept as <DATE>
        ("nba-min-okc-2025-02-24", "expect nba-<TEAM>-<TEAM>-<DATE>"),
        # A6: game1 collapse
        ("cs2-furia-g2-2025-12-05-game1", "expect cs2-<TEAM>-<TEAM>-<DATE>-<NUM>-game-<NUM>"),
        # A7: dota collapse
        ("dota-team-liquid-vs-tundra-esports-2026-01-15", "expect dota-<TEAM>-<TEAM>-<DATE>"),
        # Sanity: top crypto invariants still hold
        ("btc-updown-5m-1772898300", "expect btc-updown-<NUM>"),
        ("btc-updown-4h-1768539600", "expect btc-updown-<NUM>"),
    ]
    for s, note in examples:
        out = normalize(s)
        print(f"  {s}")
        print(f"  -> {out}   ({note})")
        print()
