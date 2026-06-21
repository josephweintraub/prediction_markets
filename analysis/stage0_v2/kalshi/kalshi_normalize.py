"""Stage 0 normalizer for Kalshi questions.

Three regimes:

1. **Parlay prefixes** (`KXMVE*`, `KXOSCARWINNERS`, `KXCITIESWEATHER`):
   compound multi-leg propositions. Filtered out of the output and not LLM-
   classified. Templates collapse to `<PARLAY_LEGS>`.

2. **Regular single-contract prefixes — base normalization**:
   * dates  → `<DATE>`  (e.g. "Jan 14, 2025")
   * times  → `<TIME>`  (e.g. "12pm EST")
   * strikes → `<NUM>` (decimal numbers, $-amounts with thousands
                       commas, comparator-anchored integers, player-prop `<N>+`)

3. **Regular single-contract prefixes — entity collapse** (v2 extension):
   For high-template-variation sports/topical prefixes where each ticker
   carries a player/team/city/song/tournament name, collapse those entity
   slots into `<PLAYER>`, `<TEAM>`, `<CITY>`, `<SONG>`, `<TOURNAMENT>`
   placeholders. Mirrors Polymarket's slug-level team-pair collapse via
   known-prefix dictionaries. Prefix-scoped to prevent false positives.

The 16 patterns implemented are documented in
`normalization_audit_report.md`.
"""
import re

# ============================================================================
# DATE / TIME / NUMBER PRIMITIVES
# ============================================================================

MONTHS = (r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|"
          r"January|February|March|April|June|July|August|September|October|November|December)")

# "Jan 14, 2025"  /  "January 14, 2025"
_DATE_FULL = re.compile(rf"\b{MONTHS}\s+\d{{1,2}},\s*\d{{4}}\b")

# "Jan 14"  /  "January 14"  (no year, when not followed by comma)
_DATE_MONTH_DAY = re.compile(rf"\b{MONTHS}\s+\d{{1,2}}\b(?!,)")

# Time-of-day: "5pm EST", "12am EDT", "5pm" with optional 3-4 letter tz
_TIME = re.compile(r"\b\d{1,2}(?:am|pm)(?:\s+[A-Z]{2,4})?\b", re.IGNORECASE)
# Also handle "5 PM EDT" with space + uppercase
_TIME_SPACE = re.compile(r"\b\d{1,2}\s+(?:AM|PM)\s+[A-Z]{2,4}\b")

# Player-prop style: "20+", "25+", "1.5+"
_PLAYER_PROP_NUM = re.compile(r"\b\d+(?:\.\d+)?\+")

# Comparator-anchored number (e.g., "above 6374.99", "below 1.5"). Replaces
# only the number, preserving the comparator word.
_COMPARATOR_NUM = re.compile(
    r"\b(above|below|over|under|between)\s+\$?\d+(?:,\d{3})*(?:\.\d+)?",
    re.IGNORECASE,
)
_AND_NUM = re.compile(r"\band\s+\$?\d+(?:,\d{3})*(?:\.\d+)?\b", re.IGNORECASE)

# Standalone dollar amount with thousands commas (or without). Catches
# patterns like "$66,750 or above" where the comparator follows the number.
_DOLLAR_AMOUNT = re.compile(r"\$\d{1,3}(?:,\d{3})+(?:\.\d+)?")

# Standalone comma-grouped number (no $)
_COMMA_NUM = re.compile(r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b")

# Standalone decimal
_DECIMAL = re.compile(r"\b\d+\.\d+\b")


# ============================================================================
# PARLAY DETECTION (filter, not collapse-meaningful)
# ============================================================================

# KXMVE is a family prefix; KXOSCARWINNERS / KXCITIESWEATHER are compound
# multi-leg single prefixes.
_PARLAY_PREFIX = re.compile(r"^KXMVE|^KXOSCARWINNERS$|^KXCITIESWEATHER$")


def is_parlay_prefix(ticker_prefix: str) -> bool:
    return bool(ticker_prefix and _PARLAY_PREFIX.match(ticker_prefix))


def _normalize_parlay_question(_question: str) -> str:
    return "<PARLAY_LEGS>"


# ============================================================================
# ENTITY COLLAPSE PATTERNS (v2 extension — see audit report)
# ============================================================================

# Group A1: `<PLAYER>: <NUM>+ <STAT>`  — NHL/NBA/EPL/UCL/NFL player stat props
A_STAT_PREFIXES_COLON = {
    "KXNHLPTS", "KXNHLAST", "KXNHLGOAL",
    "KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT", "KXNBASTL", "KXNBABLK",
    "KXEPLGOAL", "KXUCLGOAL",
    "KXNFLRSHYDS", "KXNFLPASSYDS",
}
_A_STAT_COLON = re.compile(
    r"^(?P<player>.+?):\s+<NUM>\+\s+(?P<stat>points|assists|goals|rebounds|threes|steals|blocks|rushing yards|passing yards|receiving yards|receptions)$"
)

# Group A2: `<PLAYER> records <NUM>+ <STAT>` (variant phrasing, same prefixes)
_A_STAT_RECORDS = re.compile(
    r"^(?P<player>.+?)\s+records\s+<NUM>\+\s+(?P<stat>points|assists|goals|rebounds|threes|steals|blocks|rushing yards|passing yards|receiving yards|receptions)$"
)

# Group A3: NHL goal/first-goal has a second co-existing format with team-pair prefix.
A_NHL_TEAM_AT_TEAM_PREFIXES = {"KXNHLGOAL", "KXNHLFIRSTGOAL"}
_A3_NHL_TEAM_AT_TEAM = re.compile(
    r"^(?P<t1>.+?)\s+at\s+(?P<t2>.+?):\s+(?P<kind>Anytime|First) Goal:\s+(?P<player>.+?)$"
)

# Group B: `<PLAYER>: First Goalscorer`
B_FIRSTGOAL_PREFIXES = {"KXNHLFIRSTGOAL"}
_B_FIRSTGOAL = re.compile(r"^(?P<player>.+?):\s+First Goalscorer$")

# Group C: `<PLAYER> records <NUM>+ (receiving yards|receptions)` — NFL stats
C_NFL_REC_PREFIXES = {"KXNFLRECYDS", "KXNFLREC"}
_C_NFL_REC = re.compile(
    r"^(?P<player>.+?)\s+records\s+<NUM>\+\s+(?P<stat>receiving yards|receptions)$"
)

# Group D: `<TEAM> at <TEAM>: (Spread )?Total Points( Total Points)?\??`
D_TOTAL_PREFIXES = {
    "KXNCAAMBTOTAL", "KXNBATOTAL", "KXNCAAFTOTAL", "KXNFLTOTAL", "KXNHLTOTAL",
    "KXNCAAWBTOTAL", "KXNCAAFBTOTAL",
}
_D_TOTAL_AT  = re.compile(
    r"^(?P<t1>.+?)\s+at\s+(?P<t2>.+?):\s+(?:Spread\s+)?Total Points(?:\s+Total Points)?\??$"
)
_D_TOTAL_VS  = re.compile(
    r"^(?P<t1>.+?)\s+vs\s+(?P<t2>.+?):\s+Total(?:\s+(?:Points|Goals))?\??$"
)

# Group E: `<TEAM> at <TEAM> Winner?` (also "vs ... winner?")
E_GAME_PREFIXES = {
    # NCAA
    "KXNCAAMBGAME", "KXNCAAWBGAME", "KXNCAABBGAME", "KXNCAAFBGAME",
    "KXNCAAWFGAME", "KXNCAAFGAME", "KXNCAAHOCKEYGAME",
    # Pro leagues
    "KXMLBGAME", "KXMLBSTGAME", "KXNHLGAME", "KXNBAGAME", "KXMLSGAME",
    # Soccer leagues
    "KXEFLCHAMPIONSHIPGAME", "KXEPLGAME", "KXSERIEAGAME", "KXLALIGAGAME",
    "KXLIGUE1GAME", "KXBUNDESLIGAGAME",
}
_E_GAME_AT = re.compile(r"^(?P<t1>.+?)\s+at\s+(?P<t2>.+?)\s+Winner\??$")
_E_GAME_VS = re.compile(r"^(?P<t1>.+?)\s+vs\s+(?P<t2>.+?)\s+[Ww]inner\??$")

# Group F: `<TEAM> vs <TEAM>: First Half Total?` (also NBA / NHL forms)
F_FIRST_HALF_PREFIXES = {
    "KXNCAAMB1HTOTAL", "KXNCAAWB1HTOTAL", "KXNBA1HTOTAL", "KXNHL1HTOTAL",
}
_F_FIRST_HALF = re.compile(
    r"^(?P<t1>.+?)\s+vs\s+(?P<t2>.+?):\s+First Half Total\??$"
)

# Group F2: First Half Winner — `<TEAM> vs <TEAM>: First Half Winner?`
F2_FIRST_HALF_WINNER_PREFIXES = {
    "KXNCAAMB1HWINNER", "KXNCAAWB1HWINNER", "KXNBA1HWINNER",
}
_F2_FIRST_HALF_WIN = re.compile(
    r"^(?P<t1>.+?)\s+vs\s+(?P<t2>.+?):\s+First Half Winner\??$"
)

# Group G: `<TEAM> wins by over <NUM> Points?` (case-insensitive Points)
G_SPREAD_PREFIXES = {
    "KXNCAAMBSPREAD", "KXNBASPREAD", "KXNCAAFSPREAD", "KXNFLSPREAD",
    "KXNCAAWBSPREAD",
}
_G_SPREAD = re.compile(
    r"^(?P<team>.+?)\s+wins by over\s+<NUM>\s+[Pp]oints\??$"
)

# Group H: `Will <TEAM> win the 1H by over <NUM> points?`
H_1H_SPREAD_PREFIXES = {"KXNCAAMB1HSPREAD", "KXNCAAWB1HSPREAD"}
_H_1H_SPREAD = re.compile(
    r"^Will\s+(?P<team>.+?)\s+win the 1H by over\s+<NUM>\s+points\??$"
)

# Group I1: `<TEAM> at <TEAM>: (Anytime|First|Two or More) Touchdown[s]? Scorer: <PLAYER>`
I_TD_PREFIXES = {"KXNFLANYTD", "KXNFLFIRSTTD", "KXNFL2TD"}
_I1_TD_AT = re.compile(
    r"^(?P<t1>.+?)\s+at\s+(?P<t2>.+?):\s+(?P<kind>Anytime|First|Two or More) Touchdowns? Scorer:\s+(?P<player>.+?)$"
)
# Group I2: short variant — `<PLAYER>: (Anytime|First|Two or More) Touchdown[s]?`
_I2_TD_SHORT = re.compile(
    r"^(?P<player>.+?):\s+(?P<kind>Anytime|First|Two or More) Touchdowns?$"
)

# Group J: PGA / DP World Tour tournament — `Will <PLAYER> win the <TOURNAMENT>?`
J_PGA_PREFIXES = {"KXPGATOUR", "KXDPWORLDTOUR", "KXLPGATOUR"}
_J_PGA = re.compile(r"^Will\s+(?P<player>.+?)\s+win the\s+(?P<tourney>.+?)\??$")

# Group J2: PGA make-cut — `Will <PLAYER> make the cut in <TOURNAMENT>?`
J2_PGA_MAKECUT_PREFIXES = {"KXPGAMAKECUT", "KXDPWORLDMAKECUT"}
_J2_PGA_MAKECUT = re.compile(
    r"^Will\s+(?P<player>.+?)\s+make the cut in\s+(?P<tourney>.+?)(?:\s+golf tournament)?\??$"
)

# Group J3: PGA round-N leader — `Will <PLAYER> lead at the end of round N in <TOURNAMENT>?`
J3_PGA_RLEAD_PREFIXES = {"KXPGAR1LEAD", "KXPGAR2LEAD", "KXPGAR3LEAD"}
_J3_PGA_RLEAD = re.compile(
    r"^Will\s+(?P<player>.+?)\s+lead at the end of round\s+\d+\s+in (?:the\s+)?(?P<tourney>.+?)\??$"
)

# Group K: tennis match — `Will <PLAYER> win the <PLAYER> vs <PLAYER> ... match?`
# Optional trailing ": Round Of N" or "Qualification Round N" qualifier.
K_TENNIS_PREFIXES = {
    "KXATPMATCH", "KXATPCHALLENGERMATCH", "KXWTAMATCH", "KXTABLETENNIS",
    "KXWTACHALLENGERMATCH",
}
_K_TENNIS = re.compile(
    r"^Will\s+(?P<winner>.+?)\s+win the\s+(?P<p1>.+?)\s+vs\s+(?P<p2>.+?)"
    r"(?P<rest>\s*:\s*[^?]*?)?\s+(?:Table Tennis )?(?:Match Winner|match)\??$"
)
# K2: alternate ATP/WTA format — `Will <PLAYER> be a winner of the round of <NUM> of <TOURNAMENT>?`
_K_TENNIS_ROUND_OF = re.compile(
    r"^Will\s+(?P<player>.+?)\s+be a winner of the\s+(?:round of\s+\d+|quarterfinal|semifinal|final)\s+of\s+(?P<tourney>.+?)\??$"
)

# Group L: Spotify song — `Will the top Song on <DATE> be <SONG>?`
# Also `Top USA Song on Spotify on <DATE>?` — the latter has no entity to collapse.
L_SPOTIFY_PREFIXES = {
    "KXSPOTIFYD", "KXSPOTIFYGLOBALD", "KXSPOTIFY2D", "KXSPOTIFYARTISTD",
    "KXSPOTIFYGLOBALARTISTD",
}
_L_SPOTIFY_SONG   = re.compile(
    r"^Will the (?:top|runner-up top)\s+Song\s+on\s+<DATE>\s+be\s+(?P<song>.+?)\??$",
    re.IGNORECASE,
)
_L_SPOTIFY_ARTIST = re.compile(
    r"^Will the top Artist on\s+<DATE>\s+be\s+(?P<artist>.+?)\??$",
    re.IGNORECASE,
)

# Group M: High temperature with city-in-text — `Will the **high temp in <CITY>** be <NUM>-<NUM>° on <DATE>?`
# Original (non-KX) HIGH* and KX-prefix KXHIGH* variants — same shape.
M_HIGH_TEMP_CITY_PREFIXES = {
    "HIGHNY", "HIGHCHI", "HIGHMIA", "HIGHLAX", "HIGHATL", "HIGHAUS",
    "HIGHBOS", "HIGHDC", "HIGHDEN", "HIGHHOU", "HIGHPHL", "HIGHPHIL",
    "HIGHPHX", "HIGHPDX", "HIGHSEA", "HIGHSFO",
    "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLAX", "KXHIGHATL",
    "KXHIGHAUS", "KXHIGHBOS", "KXHIGHDC", "KXHIGHDEN", "KXHIGHHOU",
    "KXHIGHPHIL", "KXHIGHPHX", "KXHIGHPDX", "KXHIGHSEA", "KXHIGHSFO",
}
_M_WEATHER_CITY = re.compile(
    r"^Will the (?:\*\*)?high temp in\s+(?P<city>.+?)(?:\*\*)?\s+be\s+"
    r"\d+-\d+°\s+on\s+<DATE>\??$"
)

# Group M2: Maximum temperature without city (city is encoded in prefix only)
M_MAX_TEMP_PREFIXES = {
    "KXHIGHTLV", "KXHIGHTSFO", "KXHIGHTSEA", "KXHIGHTDC", "KXHIGHTPHX",
    "KXHIGHTATL", "KXHIGHTMIN", "KXHIGHTNOLA", "KXHIGHTHOU", "KXHIGHTMIA",
    "KXHIGHTLAX", "KXHIGHTCHI", "KXHIGHTNYC", "KXHIGHTDEN", "KXHIGHTAUS",
    "KXHIGHTPHIL", "KXHIGHTBOS",
}
_M_MAX_TEMP = re.compile(
    r"^Will the maximum temperature be\s+\d+-\d+°\s+on\s+<DATE>\??$"
)

# Group M3: Minimum temperature without city
M_MIN_TEMP_PREFIXES = {
    "KXLOWTNYC", "KXLOWTMIA", "KXLOWTDEN", "KXLOWTAUS", "KXLOWTPHIL",
    "KXLOWTCHI", "KXLOWTLAX", "KXLOWTSFO", "KXLOWTSEA", "KXLOWTDC",
    "KXLOWTPHX", "KXLOWTATL", "KXLOWTMIN", "KXLOWTNOLA", "KXLOWTHOU",
    "KXLOWTBOS", "KXLOWTTLV",
}
_M_MIN_TEMP = re.compile(
    r"^Will the minimum temperature be\s+\d+-\d+°\s+on\s+<DATE>\??$"
)

# Group N: NBA team total — `Will <TEAM> score over <NUM> points?`
N_TEAM_TOTAL_PREFIXES = {"KXNBATEAMTOTAL", "KXNFLTEAMTOTAL"}
_N_TEAM_SCORE = re.compile(
    r"^Will\s+(?P<team>.+?)\s+score over\s+<NUM>\s+[Pp]oints\??$"
)

# Group O: WTI single-side comparator without space (e.g. "<67", ">75")
# Applies to any prefix — fixes a data-quality split where Kalshi sometimes
# writes the comparator with no space.
_BARE_COMPARATOR = re.compile(r"(?<![<>])[<>]\d+(?:\.\d+)?")

# Group P: PGA top-N finish — `<TOURNAMENT>: Will <PLAYER> finish top <NUM>?`
P_PGATOP_PREFIXES = {"KXPGATOP20", "KXPGATOP10", "KXPGATOP5"}
_P_PGATOP = re.compile(
    r"^(?P<tourney>.+?):\s+Will\s+(?P<player>.+?)\s+finish top\s+\d+\??$"
)

# Group Q: Esports total maps — `Will over <NUM> maps be played in the <T> vs. <T> <GAME> match?`
Q_ESPORTS_MAPS_PREFIXES = {
    "KXCS2TOTALMAPS", "KXLOLTOTALMAPS", "KXDOTA2TOTALMAPS",
    "KXVALORANTTOTALMAPS",
}
_Q_ESPORTS_MAPS = re.compile(
    r"^Will over\s+<NUM>\s+maps be played in the\s+(?P<t1>.+?)\s+vs\.\s+(?P<t2>.+?)"
    r"\s+(?P<game>CS2|League of Legends|LoL|Dota 2|Valorant)\s+match\??$"
)

# Group R: March Madness qualification — `Will <TEAM> qualify for the Men's March Madness <ROUND>?`
R_MARMAD_PREFIXES = {"KXMARMADROUND", "KXWMARMADROUND"}
_R_MARMAD = re.compile(
    r"^Will\s+(?P<team>.+?)\s+qualify for the\s+(?:Men's|Women's)\s+March Madness\s+(?P<round>.+?)\??$"
)

# Group S: House race — `Will <PARTY> win the House race for <DISTRICT>?`
S_HOUSE_RACE_PREFIXES = {"KXHOUSERACE"}
_S_HOUSE = re.compile(
    r"^Will\s+(?P<party>Republican|Democrat|Democratic|Independent|Libertarian|Green)\s+win the House race for\s+(?P<district>.+?)\??$"
)

# Group T: NFL season wins — `Will the <TEAM> pro football team win more than <NUM> times this season?`
T_NFL_WINS_PREFIXES = {"KXNFLWINS"}
_T_NFL_WINS = re.compile(
    r"^Will the\s+(?P<team>.+?)\s+pro football team win more than\s+\d+\s+times this season\??$"
)

# Group U: Soccer-league championship — `Will <TEAM> win (the) <LEAGUE>(?: champion)?`
# Covers Serie A, Bundesliga / German Bundesliga, La Liga, EPL.
U_SOCCER_CHAMP_PREFIXES = {
    "KXSERIEA", "KXBUNDESLIGA", "KXLALIGA", "KXEPL", "KXPREMIERLEAGUE",
}
_U_SOCCER_CHAMP = re.compile(
    r"^Will\s+(?:the\s+)?(?P<team>.+?)\s+win\s+(?:the\s+)?"
    r"(?P<league>Serie A|Bundesliga|German Bundesliga|La Liga|Premier League|EPL)"
    r"(?:\s+champion)?\??$"
)

# Group V: NBA double-double / triple-double — `<PLAYER>: Double Double` / `Triple Double`
V_DOUBLE_PREFIXES = {"KXNBA2D", "KXNBA3D"}
_V_DOUBLE = re.compile(r"^(?P<player>.+?):\s+(?P<kind>Double Double|Triple Double)$")

# Group X: Esports map / game / match patterns
# X1: "Will <TEAM> win map <NUM> in the <TEAM> vs. <TEAM> match?"
X_ESPORTS_MAP_PREFIXES = {
    "KXCS2MAP", "KXLOLMAP", "KXDOTA2MAP", "KXVALORANTMAP", "KXR6MAP",
}
_X1_ESPORTS_MAP = re.compile(
    r"^Will\s+(?P<winner>.+?)\s+win\s+map\s+\d+\s+in the\s+(?P<t1>.+?)\s+vs\.\s+(?P<t2>.+?)\s+match\??$"
)
# X2: "Will <TEAM> win the <TEAM> vs. <TEAM> CS2|LoL|... match?"
X_ESPORTS_GAME_PREFIXES = {
    "KXCS2GAME", "KXLOLGAME", "KXDOTA2GAME", "KXVALORANTGAME", "KXR6GAME",
}
_X2_ESPORTS_GAME = re.compile(
    r"^Will\s+(?P<winner>.+?)\s+win the\s+(?P<t1>.+?)\s+vs\.\s+(?P<t2>.+?)"
    r"\s+(?P<game>CS2|LoL|League of Legends|Dota 2|Valorant|R6|Rainbow Six)\s+match\??$"
)

# Group Y: Tennis set winner — `Will <PLAYER> win set <NUM> in the <PLAYER> vs <PLAYER> match`
Y_TENNIS_SET_PREFIXES = {"KXATPSETWINNER", "KXWTASETWINNER"}
_Y_TENNIS_SET = re.compile(
    r"^Will\s+(?P<winner>.+?)\s+win\s+set\s+\d+\s+in the\s+(?P<p1>.+?)\s+vs\s+(?P<p2>.+?)\s+match\??$"
)

# Group AB: NASCAR race winner — `<RACE_NAME>Winner` (no space — Kalshi quirk)
AB_NASCAR_PREFIXES = {"KXNASCARRACE", "KXNASCARTRUCK", "KXNASCARXFINITY"}
_AB_NASCAR = re.compile(r"^(?P<race>.+?)\s*Winner$")

# Group AC: Olympic gold medal — `Will <ATHLETE_OR_COUNTRY> win the gold medal in <EVENT>?`
AC_OLYMPIC_PREFIXES = {
    "KXWOFREESKI", "KXWOXC", "KXWOSKI", "KXWOSNOWBOARD", "KXWOBOBSLEIGH",
    "KXWOLUGE", "KXWOICEHOCKEY", "KXWOSKELETON", "KXWOBIATHLON",
    "KXWOFIGURESKATE", "KXWOSPEEDSKATE", "KXWOCURLING",
}
_AC_OLYMPIC = re.compile(
    r"^Will\s+(?P<winner>.+?)\s+win the gold medal in\s+(?P<sport>.+?)\??$"
)

# Group AD: First Goalscorer alt — `No Goal: First Goalscorer` short / coexisting form
# Affects KXEPLFIRSTGOAL and KXUCLFIRSTGOAL alongside team-format. Their existing
# "<PLAYER>: First Goalscorer" rule (Group B) already handles player forms.
AD_EPL_UCL_FIRSTGOAL_PREFIXES = {"KXEPLFIRSTGOAL", "KXUCLFIRSTGOAL"}
_AD_FIRSTGOAL = re.compile(r"^(?P<player>.+?):\s+First Goalscorer$")


# ============================================================================
# DRIVER
# ============================================================================

def ticker_prefix_of(ticker: str) -> str:
    m = re.match(r"^[A-Z0-9]+", ticker or "")
    return m.group(0) if m else ""


def _apply_entity_collapses(s: str, prefix: str) -> str:
    """Apply prefix-scoped entity-collapse regexes. Returns the first matching
    rewrite, or the input unchanged if no pattern applies.
    """
    # Pattern A1+A2 (NHL/NBA/EPL stat — colon form is canonical; "records" variant unifies to colon)
    if prefix in A_STAT_PREFIXES_COLON:
        m = _A_STAT_COLON.match(s)
        if m:
            return f"<PLAYER>: <NUM>+ {m.group('stat')}"
        m = _A_STAT_RECORDS.match(s)
        if m:
            return f"<PLAYER>: <NUM>+ {m.group('stat')}"
    # Pattern A3 (NHL Goal team-at-team alternate format)
    if prefix in A_NHL_TEAM_AT_TEAM_PREFIXES:
        m = _A3_NHL_TEAM_AT_TEAM.match(s)
        if m:
            return f"<TEAM> at <TEAM>: {m.group('kind')} Goal: <PLAYER>"
    # Pattern B (NHL first goalscorer)
    if prefix in B_FIRSTGOAL_PREFIXES:
        m = _B_FIRSTGOAL.match(s)
        if m:
            return "<PLAYER>: First Goalscorer"
    # Pattern C (NFL receptions / receiving yards) — also unify to colon form
    if prefix in C_NFL_REC_PREFIXES:
        m = _C_NFL_REC.match(s)
        if m:
            return f"<PLAYER>: <NUM>+ {m.group('stat')}"
    # Pattern D (team-at/vs-team total points/goals)
    if prefix in D_TOTAL_PREFIXES:
        m = _D_TOTAL_AT.match(s)
        if m:
            return "<TEAM> at <TEAM>: Total Points"
        m = _D_TOTAL_VS.match(s)
        if m:
            unit = "Goals" if "Goals" in s else "Points"
            return f"<TEAM> vs <TEAM>: Total {unit}"
    # Pattern E (team-at/vs-team winner)
    if prefix in E_GAME_PREFIXES:
        m = _E_GAME_AT.match(s)
        if m:
            return "<TEAM> at <TEAM> Winner?"
        m = _E_GAME_VS.match(s)
        if m:
            return "<TEAM> vs <TEAM> Winner?"
    # Pattern F (1H team-vs-team total)
    if prefix in F_FIRST_HALF_PREFIXES:
        m = _F_FIRST_HALF.match(s)
        if m:
            return "<TEAM> vs <TEAM>: First Half Total?"
    # Pattern F2 (1H team-vs-team winner)
    if prefix in F2_FIRST_HALF_WINNER_PREFIXES:
        m = _F2_FIRST_HALF_WIN.match(s)
        if m:
            return "<TEAM> vs <TEAM>: First Half Winner?"
    # Pattern G (team spread)
    if prefix in G_SPREAD_PREFIXES:
        m = _G_SPREAD.match(s)
        if m:
            return "<TEAM> wins by over <NUM> Points?"
    # Pattern H (1H team spread)
    if prefix in H_1H_SPREAD_PREFIXES:
        m = _H_1H_SPREAD.match(s)
        if m:
            return "Will <TEAM> win the 1H by over <NUM> points?"
    # Pattern I1+I2 (NFL touchdown scorer)
    if prefix in I_TD_PREFIXES:
        m = _I1_TD_AT.match(s)
        if m:
            return f"<TEAM> at <TEAM>: {m.group('kind')} Touchdown Scorer: <PLAYER>"
        m = _I2_TD_SHORT.match(s)
        if m:
            return f"<PLAYER>: {m.group('kind')} Touchdown"
    # Pattern J (PGA / DP World / LPGA tournament winner)
    if prefix in J_PGA_PREFIXES:
        m = _J_PGA.match(s)
        if m:
            return "Will <PLAYER> win the <TOURNAMENT>?"
    # Pattern J2 (make-cut)
    if prefix in J2_PGA_MAKECUT_PREFIXES:
        m = _J2_PGA_MAKECUT.match(s)
        if m:
            return "Will <PLAYER> make the cut in <TOURNAMENT>?"
    # Pattern J3 (round-N leader)
    if prefix in J3_PGA_RLEAD_PREFIXES:
        m = _J3_PGA_RLEAD.match(s)
        if m:
            return "Will <PLAYER> lead at the end of round <NUM> in <TOURNAMENT>?"
    # Pattern K (tennis / table tennis match)
    if prefix in K_TENNIS_PREFIXES:
        m = _K_TENNIS.match(s)
        if m:
            qual = (m.group("rest") or "").strip()
            if qual:
                return "Will <PLAYER> win the <PLAYER> vs <PLAYER> : <QUAL> match?"
            return "Will <PLAYER> win the <PLAYER> vs <PLAYER> match?"
        m = _K_TENNIS_ROUND_OF.match(s)
        if m:
            return "Will <PLAYER> be a winner of the <ROUND> of <TOURNAMENT>?"
    # Pattern L (Spotify song / artist)
    if prefix in L_SPOTIFY_PREFIXES:
        m = _L_SPOTIFY_SONG.match(s)
        if m:
            return "Will the top Song on <DATE> be <SONG>?"
        m = _L_SPOTIFY_ARTIST.match(s)
        if m:
            return "Will the top Artist on <DATE> be <ARTIST>?"
    # Pattern M (high temperature with city name in text)
    if prefix in M_HIGH_TEMP_CITY_PREFIXES:
        m = _M_WEATHER_CITY.match(s)
        if m:
            return "Will the high temp in <CITY> be <NUM>-<NUM>° on <DATE>?"
    # Pattern M2 (maximum temperature, city encoded in prefix)
    if prefix in M_MAX_TEMP_PREFIXES:
        m = _M_MAX_TEMP.match(s)
        if m:
            return "Will the maximum temperature be <NUM>-<NUM>° on <DATE>?"
    # Pattern M3 (minimum temperature, city encoded in prefix)
    if prefix in M_MIN_TEMP_PREFIXES:
        m = _M_MIN_TEMP.match(s)
        if m:
            return "Will the minimum temperature be <NUM>-<NUM>° on <DATE>?"
    # Pattern N (team total score)
    if prefix in N_TEAM_TOTAL_PREFIXES:
        m = _N_TEAM_SCORE.match(s)
        if m:
            return "Will <TEAM> score over <NUM> points?"
    # Pattern P (PGA top-N finish)
    if prefix in P_PGATOP_PREFIXES:
        m = _P_PGATOP.match(s)
        if m:
            return "<TOURNAMENT>: Will <PLAYER> finish top <NUM>?"
    # Pattern Q (esports total maps); unify "LoL" → "League of Legends" canonical form
    if prefix in Q_ESPORTS_MAPS_PREFIXES:
        m = _Q_ESPORTS_MAPS.match(s)
        if m:
            game = "League of Legends" if m.group("game") == "LoL" else m.group("game")
            return f"Will over <NUM> maps be played in the <TEAM> vs. <TEAM> {game} match?"
    # Pattern R (March Madness qualification)
    if prefix in R_MARMAD_PREFIXES:
        m = _R_MARMAD.match(s)
        if m:
            mens_womens = "Men's" if "Men's" in s else "Women's"
            return f"Will <TEAM> qualify for the {mens_womens} March Madness <ROUND>?"
    # Pattern S (House race)
    if prefix in S_HOUSE_RACE_PREFIXES:
        m = _S_HOUSE.match(s)
        if m:
            return "Will <PARTY> win the House race for <DISTRICT>?"
    # Pattern T (NFL season wins)
    if prefix in T_NFL_WINS_PREFIXES:
        m = _T_NFL_WINS.match(s)
        if m:
            return "Will the <TEAM> pro football team win more than <NUM> times this season?"
    # Pattern U (soccer league championship)
    if prefix in U_SOCCER_CHAMP_PREFIXES:
        m = _U_SOCCER_CHAMP.match(s)
        if m:
            league = m.group("league")
            # Canonicalize Bundesliga aliases
            league = "Bundesliga" if league == "German Bundesliga" else league
            league = "Premier League" if league == "EPL" else league
            return f"Will <TEAM> win the {league}?"
    # Pattern V (NBA double-double / triple-double)
    if prefix in V_DOUBLE_PREFIXES:
        m = _V_DOUBLE.match(s)
        if m:
            return f"<PLAYER>: {m.group('kind')}"
    # Pattern X1 (esports map winner)
    if prefix in X_ESPORTS_MAP_PREFIXES:
        m = _X1_ESPORTS_MAP.match(s)
        if m:
            return "Will <TEAM> win map <NUM> in the <TEAM> vs. <TEAM> match?"
    # Pattern X2 (esports game winner)
    if prefix in X_ESPORTS_GAME_PREFIXES:
        m = _X2_ESPORTS_GAME.match(s)
        if m:
            game = m.group("game")
            game = "League of Legends" if game == "LoL" else game
            game = "Rainbow Six" if game == "R6" else game
            return f"Will <TEAM> win the <TEAM> vs. <TEAM> {game} match?"
    # Pattern Y (tennis set winner)
    if prefix in Y_TENNIS_SET_PREFIXES:
        m = _Y_TENNIS_SET.match(s)
        if m:
            return "Will <PLAYER> win set <NUM> in the <PLAYER> vs <PLAYER> match"
    # Pattern AB (NASCAR)
    if prefix in AB_NASCAR_PREFIXES:
        m = _AB_NASCAR.match(s)
        if m:
            return "<RACE> Winner"
    # Pattern AC (Olympic gold medal)
    if prefix in AC_OLYMPIC_PREFIXES:
        m = _AC_OLYMPIC.match(s)
        if m:
            return "Will <ATHLETE> win the gold medal in <EVENT>?"
    # Pattern AD (EPL/UCL First Goalscorer alternate)
    if prefix in AD_EPL_UCL_FIRSTGOAL_PREFIXES:
        m = _AD_FIRSTGOAL.match(s)
        if m:
            return "<PLAYER>: First Goalscorer"
    return s


def _normalize_regular_question(q: str, prefix: str = "") -> str:
    if not q:
        return ""
    s = q
    # Dates first (they may contain numbers).
    s = _DATE_FULL.sub("<DATE>", s)
    s = _DATE_MONTH_DAY.sub("<DATE>", s)
    # Times (both forms)
    s = _TIME_SPACE.sub("<TIME>", s)
    s = _TIME.sub("<TIME>", s)
    # Player-prop "X+" before comparator (preserves the +)
    s = _PLAYER_PROP_NUM.sub("<NUM>+", s)
    # Comparator-anchored numbers (preserve comparator word)
    def _comp(m: re.Match) -> str:
        return f"{m.group(1)} <NUM>"
    s = _COMPARATOR_NUM.sub(_comp, s)
    s = _AND_NUM.sub("and <NUM>", s)
    # Standalone dollar amounts with thousands commas (e.g. "$66,750")
    s = _DOLLAR_AMOUNT.sub("$<NUM>", s)
    # Standalone comma-grouped integers / decimals (e.g. "5,549.99")
    s = _COMMA_NUM.sub("<NUM>", s)
    # Standalone decimals
    s = _DECIMAL.sub("<NUM>", s)
    # Bare comparators "<67", ">75" (no space) — WTI data-quality fix
    s = _BARE_COMPARATOR.sub(lambda m: m.group(0)[0] + "<NUM>", s)
    # Whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # Prefix-scoped entity-name collapse
    s = _apply_entity_collapses(s, prefix)
    return s


def normalize(question: str, ticker_prefix: str) -> str:
    """Stage 0 normalizer entrypoint.

    Dispatches to the parlay collapser for parlay-family prefixes; otherwise
    normalizes dates/times/strikes and applies prefix-scoped entity collapse.
    """
    if is_parlay_prefix(ticker_prefix):
        return _normalize_parlay_question(question)
    return _normalize_regular_question(question, ticker_prefix or "")
