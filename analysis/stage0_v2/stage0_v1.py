"""Reconstructed Stage 0 normalizer — v1.

Goal: reproduce the current Polymarket slug → template normalization that
produced the 106,230 market_templates and 31,482 event_templates in the
existing dataset. The original source was lost (ran interactively on EC2);
this version is recovered from the (raw_slug, template) pairs we have.

Rules in order:
  1.  Strip leading `duplicate-` prefix
  2.  Strip trailing hash-like multi-digit suffixes (`-227-967-547-...`)
  3.  Replace date tokens (month names, year-with-month, ranges, bare years
      2020-2029, q1/q2/q3/q4) with <DATE>
  4.  Replace time-of-day tokens (`\\d+pm`, `\\d+am`, optional `-et` suffix) with <TIME>
  5.  Replace numeric-with-unit tokens (`\\d+k`, `\\d+m`, `\\d+b`, `\\d+bps`, `\\d+pt\\d+`) with <NUM>
  6.  Replace `top\\d+` with `top<NUM>`
  7.  Replace bare digits with <NUM>
  8.  Collapse adjacent identical placeholders (`<DATE>-<DATE>` -> `<DATE>`, etc.)
  9.  For known sports league prefixes, collapse the two team-name tokens into <TEAM>-<TEAM>

The public function is `normalize(slug: str) -> str`.
"""
from __future__ import annotations
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

MONTHS_FULL = "january|february|march|april|may|june|july|august|september|october|november|december"
MONTHS_ABBR = "jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"  # 'may' overlaps with full
WEEKDAYS = "monday|tuesday|wednesday|thursday|friday|saturday|sunday"
YEAR_RE = r"(?:20\d{2})"  # 2000-2099 — matches current (over-broad) behavior; Phase A tightens this
QUARTER_RE = r"q[1-4]"

# Known sports / esports league prefixes that trigger <TEAM>-<TEAM> collapse.
# Recovered from the data: any template starting with `^{prefix}-<TEAM>-<TEAM>`.
KNOWN_LEAGUES = [
    # ranked by template count
    "atp", "nba", "wta", "ufc", "mls", "nfl", "lol", "ucl", "cs2", "crint",
    "epl", "uel", "fl1", "dota2", "bun", "lib", "valorant", "j2100", "val",
    "nhl", "arg", "cricipl", "cfb", "cricpsl", "elc", "bra", "el1", "enl",
    "el2", "rueuchamp", "per1", "wnba", "mar1", "crictbcl", "mlb", "cruae",
    "nwsl", "crwt20wcgq", "cbb", "r6siege", "rl", "cricthunderbolt", "ncaab",
    "cru19wc", "khl", "dehl", "bkligend", "cwbb", "wttwom", "bkjpn",
    "wttmen", "snhl", "cehl", "bkseriea", "bkarg", "bkbbl",
]

# ============================================================================
# COMPILED REGEXES
# ============================================================================

# Step 2: trailing hash suffix — pattern like "-227-967-547-..." (3+ multi-digit
# segments at the end of the slug). Polymarket uses this to disambiguate
# colliding slugs.
_TRAILING_HASH = re.compile(r"(?:-\d{2,}){3,}$")

# Step 2b: trailing digit-group range — Polymarket appends these as range-summary
# suffixes (score `-1-1`, week-range `-65-89`, ISO date `-2025-12-30`, internal
# IDs `-240-259`, TSA-passenger range `-2200000-624`). Matches 2+ trailing groups
# of any digit count.
_TRAILING_SHORT_RANGE = re.compile(r"(?:-\d+){2,}$")

# Step 3 (a): FULL month name + day (+ optional year) — greedy capture into
# single <DATE> placeholder. Restricted to full month names (not abbreviations)
# so that "oct-4" produces <DATE>-<NUM> per current behavior. The (?!\d)
# negative lookahead prevents \d{1,2} from grabbing the leading digits of a
# 4-digit year (e.g., "january-2026" should match just "january", not "january-20").
_DATE_FULL_WITH_DAY = re.compile(
    rf"\b(?:{MONTHS_FULL})-\d{{1,2}}\b(?:-{YEAR_RE})?"
)

# Step 3 (b): year only — bare "20XX"
_DATE_YEAR = re.compile(rf"\b{YEAR_RE}\b")

# Step 3 (c): bare full month — standalone full month names only. Abbreviations
# (jan, feb, mar...) stay as literals when standalone; they only get substituted
# when followed by a day digit via _DATE_ABBR_WITH_DAY below.
_DATE_MONTH = re.compile(rf"\b(?:{MONTHS_FULL})\b")

# Step 3 (d): abbreviation followed by ANY hyphen (i.e., not at end of slug).
# Matches "oct-4", "feb-7", "dec-meeting" but not standalone trailing "jan".
# Substitutes just the abbreviation; following content (digit or word) is left.
_DATE_ABBR_WITH_DAY = re.compile(rf"\b(?:{MONTHS_ABBR})(?=-)")

# Step 3 (d): quarter — "q1", "q2", "q3", "q4"
_DATE_QUARTER = re.compile(rf"\b{QUARTER_RE}\b")

# Step 4: time-of-day — "11pm-et", "5am", "12pm-et", etc.
_TIME = re.compile(r"\b\d{1,2}(?:pm|am)(?:-et)?\b")

# Step 5: numbers with common units
# Decimal form like 2pt5, 119pt5, 1pt22c — Polymarket's URL-safe decimal encoding.
# Optional trailing letter handles cents (1pt22c) and similar.
_NUM_PT_DECIMAL = re.compile(r"\b\d+pt\d+[a-z]?\b")
# Digits followed by an allow-listed unit. Critically restrictive: temperature
# units (f, c) and distance units (km, m as in meters) are NOT in this list
# because those are the distinguishing parameters of weather/race markets and
# must stay literal. The bps/k/m/b/h units identify a number's magnitude or
# interval and SHOULD collapse.
_NUM_WITH_SUFFIX = re.compile(r"\b\d+(?:k|m|b|h|bps|st|nd|rd|th)\b")

# Step 6: top<NUM>
_TOP_NUM = re.compile(r"\btop\d+\b")

# Step 7: bare digits (any remaining numeric runs)
_BARE_DIGITS = re.compile(r"\b\d+\b")

# Step 8a: strip trailing numeric ranges like "-1-1" / "-1-2" / "-95-100"
# (typically score-range / week-range / ID suffix). Two or more trailing
# <NUM> placeholders are stripped; a single trailing <NUM> (e.g. a spread
# value) is preserved.
_TRAILING_NUM_RANGE = re.compile(r"(?:-<NUM>){2,}$")

# Step 8b: collapse adjacent identical placeholders
_REDUCE_DATE = re.compile(r"(?:<DATE>-)+<DATE>")
_REDUCE_NUM  = re.compile(r"(?:<NUM>-)+<NUM>")

# Step 2c (raw-level): trailing single ID after a FULL-month-day. Targets the
# "december-31-983" case while leaving "mar-7" (abbrev) and "jan-31-feb-7"
# (abbrev as second date) untouched. Captures the (month-day) and drops the
# trailing -\d+ id.
_TRAIL_DATE_ID = re.compile(
    rf"((?:{MONTHS_FULL})-\d{{1,2}})-\d{{1,4}}$"
)

# Step 9: team-pair collapse for known leagues
_KNOWN_LEAGUE_PAT = re.compile(
    r"^(" + "|".join(re.escape(p) for p in KNOWN_LEAGUES) + r")-([a-z0-9]+)-vs-([a-z0-9]+)\b"
)
_KNOWN_LEAGUE_PAT2 = re.compile(
    r"^(" + "|".join(re.escape(p) for p in KNOWN_LEAGUES) + r")-([a-z0-9]+)-([a-z0-9]+)\b"
)


# ============================================================================
# NORMALIZE
# ============================================================================

def normalize(slug: str) -> str:
    """Normalize a raw Polymarket slug into a template."""
    if not isinstance(slug, str) or not slug:
        return ""
    s = slug.lower().strip()

    # Step 1: strip duplicate- prefix
    if s.startswith("duplicate-"):
        s = s[len("duplicate-"):]

    # Step 2: strip trailing hash suffix (3+ multi-digit groups)
    s = _TRAILING_HASH.sub("", s)

    # Step 2b: strip trailing short-digit range (-1-1, -65-89, etc.) — applied
    # on the RAW slug before placeholders take effect.
    s = _TRAILING_SHORT_RANGE.sub("", s)

    # Step 2c: strip trailing single Polymarket-internal ID after a full-month-day
    # ("december-31-983" -> "december-31"). Abbreviation+day combinations like
    # "mar-7" / "feb-7" are intentionally preserved.
    s = _TRAIL_DATE_ID.sub(r"\1", s)

    # Step 3: TIME first (so time-digits like "11am" don't get absorbed by date)
    s = _TIME.sub("<TIME>", s)

    # Step 4: date substitutions (order matters: most-specific first)
    s = _DATE_FULL_WITH_DAY.sub("<DATE>", s)
    s = _DATE_ABBR_WITH_DAY.sub("<DATE>", s)
    s = _DATE_YEAR.sub("<DATE>", s)
    s = _DATE_MONTH.sub("<DATE>", s)
    s = _DATE_QUARTER.sub("<DATE>", s)

    # Step 5: numbers-with-units (before bare digits, so we don't lose context)
    s = _NUM_PT_DECIMAL.sub("<NUM>", s)
    s = _NUM_WITH_SUFFIX.sub("<NUM>", s)

    # Step 6: top<NUM>
    s = _TOP_NUM.sub("top<NUM>", s)

    # Step 7: bare digits
    s = _BARE_DIGITS.sub("<NUM>", s)

    # Step 8: collapse adjacent placeholders
    s = _REDUCE_DATE.sub("<DATE>", s)
    s = _REDUCE_NUM.sub("<NUM>", s)

    # Step 9: team-pair collapse for known leagues — handle -vs- form first
    m = _KNOWN_LEAGUE_PAT.match(s)
    if m:
        s = f"{m.group(1)}-<TEAM>-<TEAM>" + s[m.end():]
    else:
        m = _KNOWN_LEAGUE_PAT2.match(s)
        if m:
            # Don't collapse if the two captured tokens are placeholders
            t1, t2 = m.group(2), m.group(3)
            if "<" not in t1 and "<" not in t2:
                s = f"{m.group(1)}-<TEAM>-<TEAM>" + s[m.end():]

    return s


if __name__ == "__main__":
    # Smoke test
    examples = [
        "nba-mia-sac-2026-01-20-total-236pt5",
        "btc-updown-5m-1772898300",
        "fed-decreases-interest-rates-by-25-bps-after-january-2026-meeting",
        "highest-temperature-in-nyc-on-april-10-2026",
        "duplicate-will-mike-waltz-be-the-first-to-leave-the-trump-cabinet-before-2027",
        "bitcoin-up-or-down-february-5-3pm-et",
        "2026-masters-tournament-top20-brooks-koepka",
        "will-bitcoin-reach-100k-in-january-2026",
        "cs2-furia-g2-2025-12-05-game1",
        "elon-musk-of-tweets-february-5-february-7-65-89",
    ]
    for s in examples:
        print(f"  {s}")
        print(f"  -> {normalize(s)}\n")
