"""Polymarket SYSTEM_A + FEWSHOT_A prompt constants, side-effect-free.

Lifted verbatim from `stage2_test.py` Option A (combined event+market
classification). Importing `stage2_test.py` directly triggers a live API run
because the test harness lives at module-level — this file is the clean import
target for the Kalshi pipeline.
"""
import json

MODEL = "claude-sonnet-4-5"

POLYMARKET_CATEGORIES = [
    "Politics", "Sports", "Crypto", "Esports", "Iran", "Finance",
    "Geopolitics", "Tech", "Culture", "Economy", "Weather",
    "Mentions", "Elections",
]
CATS_STR = ", ".join(POLYMARKET_CATEGORIES)

SYSTEM_A = f"""\
You classify Polymarket prediction market contracts. Each contract has a market \
(the specific question being bet on) and a parent event (the umbrella question \
that this market sits under). You extract structured tags at BOTH levels: \
event-level (the generic event topic) and market-level (this specific contract).

You receive three inputs per contract:
  - event_template: the normalized parent event slug (placeholders <DATE>, <NUM>, <TIME>, <TEAM> mark variable parameters)
  - market_template: the normalized specific contract slug
  - question: the human-readable question for ONE sample market under this event (use as context for entity disambiguation; do NOT classify at the specific instance level — abstract to the template level)

For each contract, output JSON with these fields:

  - event_subjects: ranked list of entities/topics the parent EVENT is about (most central first). E.g. ["US Presidential Election 2024"]. Multiple subjects allowed for compound events (e.g. ["Russia", "Ukraine", "Russo-Ukrainian War"]).
  - event_action: noun phrase (1-4 words) for the verb-essence of the parent event. E.g. "election winner".
  - event_info_type: lowercase snake_case for the kind of public info determining outcomes (e.g. "election_outcome", "central_bank_announcement", "geopolitical_event", "sports_game_data", "weather_data").
  - event_resolution_type: exactly one of "data_driven_numeric" or "event_observable".
  - market_subjects: ranked list of entities for THIS specific market. Often a narrower subset of event_subjects (e.g. ["Donald Trump"] for a Trump-specific market within a presidential election event).
  - market_action: noun phrase for the verb-essence of the specific market (e.g. "specific candidate winning", "team win").
  - market_info_type: same vocabulary style as event_info_type.
  - market_resolution_type: same as event_resolution_type.
  - categories: an array of one or more of these EXACT category names: {CATS_STR}. Pick all that apply.
  - snippet: a verbatim 3-5 word fragment copied from either the template or question (hallucination check).

Rules:
  - Subjects are noun phrases, not full sentences. Use proper names ("Donald Trump", "Bitcoin", "Federal Reserve").
  - Use your knowledge of named entities, sports leagues, crypto projects, etc. to disambiguate cryptic slug abbreviations using the question text.
  - DO NOT introduce instance-specific entities (e.g. don't say "Los Angeles Lakers" just because the question mentions them — abstract to "NBA basketball game" if the market_template uses generic <TEAM> placeholders).
  - Return JSON only.
"""

FEWSHOT_A = """\
Example 1 (Politics + Elections, multi-market event):
event_template:  presidential-election-winner-<DATE>
market_template: will-donald-trump-win-the-<DATE>-us-presidential-election
question:        Will Donald Trump win the 2024 US Presidential Election?
Output:
{
  "event_subjects": ["US Presidential Election 2024"],
  "event_action": "election winner",
  "event_info_type": "election_outcome",
  "event_resolution_type": "event_observable",
  "market_subjects": ["Donald Trump", "US Presidential Election 2024"],
  "market_action": "specific candidate winning",
  "market_info_type": "election_outcome",
  "market_resolution_type": "event_observable",
  "categories": ["Politics", "Elections"],
  "snippet": "will-donald-trump-win"
}

Example 2 (Crypto, single-market event):
event_template:  will-bitcoin-hit-<NUM>k-in-<DATE>
market_template: will-bitcoin-hit-<NUM>k-in-<DATE>
question:        Will Bitcoin hit $250k in 2024?
Output:
{
  "event_subjects": ["Bitcoin price"],
  "event_action": "price threshold cross",
  "event_info_type": "crypto_price_data",
  "event_resolution_type": "data_driven_numeric",
  "market_subjects": ["Bitcoin price"],
  "market_action": "price threshold cross",
  "market_info_type": "crypto_price_data",
  "market_resolution_type": "data_driven_numeric",
  "categories": ["Crypto"],
  "snippet": "will-bitcoin-hit"
}

Example 3 (Sports, team-pair collapsed):
event_template:  nor-<TEAM>-<TEAM>-<DATE>
market_template: nor-<TEAM>-<TEAM>-<DATE>-bog
question:        Will FK Bodø/Glimt win on 2025-10-26?
Output:
{
  "event_subjects": ["Norwegian Eliteserien soccer match"],
  "event_action": "game outcome",
  "event_info_type": "sports_game_data",
  "event_resolution_type": "event_observable",
  "market_subjects": ["Norwegian Eliteserien soccer match"],
  "market_action": "team win",
  "market_info_type": "sports_game_data",
  "market_resolution_type": "event_observable",
  "categories": ["Sports"],
  "snippet": "nor"
}

Example 4 (Iran + Geopolitics + Politics, geopolitical event):
event_template:  us-strikes-iran-by-<DATE>
market_template: us-strikes-iran-by-<DATE>
question:        Will the US strike Iran by March 31?
Output:
{
  "event_subjects": ["United States", "Iran", "US-Iran military conflict"],
  "event_action": "military strike",
  "event_info_type": "geopolitical_event",
  "event_resolution_type": "event_observable",
  "market_subjects": ["United States", "Iran", "US-Iran military conflict"],
  "market_action": "military strike",
  "market_info_type": "geopolitical_event",
  "market_resolution_type": "event_observable",
  "categories": ["Iran", "Geopolitics", "Politics"],
  "snippet": "us-strikes-iran"
}

Example 5 (Esports + Sports, team-pair collapsed — note abstraction):
event_template:  lol-<TEAM>-<TEAM>
market_template: lol-<TEAM>-<TEAM>
question:        T1 vs Gen.G match at LCK Spring 2026
Output:
{
  "event_subjects": ["League of Legends esports match"],
  "event_action": "match outcome",
  "event_info_type": "esports_match_data",
  "event_resolution_type": "event_observable",
  "market_subjects": ["League of Legends esports match"],
  "market_action": "match outcome",
  "market_info_type": "esports_match_data",
  "market_resolution_type": "event_observable",
  "categories": ["Esports", "Sports"],
  "snippet": "lol"
}
NOTE: question mentions specific teams (T1, Gen.G) but the template uses <TEAM>-<TEAM> placeholders. Subjects must abstract away the specific teams; do not list "T1" or "Gen.G".

Example 6 (Finance + Economy, multi-market event):
event_template:  fed-decision-in-<DATE>
market_template: fed-decreases-interest-rates-by-<NUM>-bps-after-<DATE>-meeting
question:        Will the Fed decrease interest rates by 25 bps after December 2025 meeting?
Output:
{
  "event_subjects": ["Federal Reserve monetary policy"],
  "event_action": "FOMC interest rate decision",
  "event_info_type": "central_bank_announcement",
  "event_resolution_type": "data_driven_numeric",
  "market_subjects": ["Federal Reserve monetary policy", "interest rates"],
  "market_action": "specific rate decrease",
  "market_info_type": "central_bank_announcement",
  "market_resolution_type": "data_driven_numeric",
  "categories": ["Finance", "Economy"],
  "snippet": "fed-decreases-interest-rates"
}

Example 7 (Tech, single-market event):
event_template:  will-anthropic-have-the-best-ai-model-at-the-end-of-<DATE>
market_template: will-anthropic-have-the-best-ai-model-at-the-end-of-<DATE>
question:        Will Anthropic have the best AI model at the end of March 2026?
Output:
{
  "event_subjects": ["Anthropic", "AI model leaderboards"],
  "event_action": "top model ranking",
  "event_info_type": "ai_industry_ranking",
  "event_resolution_type": "event_observable",
  "market_subjects": ["Anthropic", "AI model leaderboards"],
  "market_action": "top model ranking",
  "market_info_type": "ai_industry_ranking",
  "market_resolution_type": "event_observable",
  "categories": ["Tech"],
  "snippet": "will-anthropic-have-the-best"
}

Example 8 (Culture, multi-candidate award event):
event_template:  oscars-<DATE>-best-actor-winner
market_template: will-timothe-chalamet-win-best-actor-at-the-<NUM>-academy-awards
question:        Will Timothée Chalamet win Best Actor at the 98th Academy Awards?
Output:
{
  "event_subjects": ["Academy Awards Best Actor"],
  "event_action": "award winner",
  "event_info_type": "awards_ceremony",
  "event_resolution_type": "event_observable",
  "market_subjects": ["Timothée Chalamet", "Academy Awards Best Actor"],
  "market_action": "specific actor winning",
  "market_info_type": "awards_ceremony",
  "market_resolution_type": "event_observable",
  "categories": ["Culture"],
  "snippet": "will-timothe-chalamet-win"
}

Example 9 (Economy + Finance, single-market event):
event_template:  what-will-us-inflation-be-from-<DATE>-to-<DATE>
market_template: will-monthly-inflation-increase-by-<NUM>-or-less-in-<DATE>
question:        Will monthly inflation increase by 0.1% or less in April?
Output:
{
  "event_subjects": ["US inflation rate"],
  "event_action": "monthly inflation measurement",
  "event_info_type": "economic_data_release",
  "event_resolution_type": "data_driven_numeric",
  "market_subjects": ["US inflation rate"],
  "market_action": "monthly inflation threshold",
  "market_info_type": "economic_data_release",
  "market_resolution_type": "data_driven_numeric",
  "categories": ["Economy", "Finance"],
  "snippet": "monthly-inflation-increase-by"
}

Example 10 (Weather, single-market event):
event_template:  highest-temperature-in-london-on-<DATE>
market_template: highest-temperature-in-london-on-<DATE>
question:        Will the highest temperature in London on August 30 be 75°F or higher?
Output:
{
  "event_subjects": ["London weather"],
  "event_action": "daily high temperature",
  "event_info_type": "weather_data",
  "event_resolution_type": "data_driven_numeric",
  "market_subjects": ["London weather", "temperature threshold"],
  "market_action": "temperature threshold reach",
  "market_info_type": "weather_data",
  "market_resolution_type": "data_driven_numeric",
  "categories": ["Weather"],
  "snippet": "highest-temperature-in-london"
}

Example 11 (Mentions + Politics, multi-market speech-content event):
event_template:  what-will-trump-say-during-<DATE>-speech
market_template: will-trump-say-tariffs-during-<DATE>-speech
question:        Will Trump say "tariffs" during his March 4 speech?
Output:
{
  "event_subjects": ["Donald Trump", "presidential speech"],
  "event_action": "speech content",
  "event_info_type": "political_speech",
  "event_resolution_type": "event_observable",
  "market_subjects": ["Donald Trump", "presidential speech"],
  "market_action": "specific word utterance",
  "market_info_type": "political_speech",
  "market_resolution_type": "event_observable",
  "categories": ["Mentions", "Politics"],
  "snippet": "will-trump-say-tariffs"
}

Example 12 (TEMPLATE-LEVEL ABSTRACTION — counter-example of what NOT to do):
event_template:  nba-<TEAM>-<TEAM>
market_template: nba-<TEAM>-<TEAM>
question:        Will the Los Angeles Lakers beat the New York Knicks at Madison Square Garden on December 25, 2025?
Output:
{
  "event_subjects": ["NBA basketball game"],
  "event_action": "game outcome",
  "event_info_type": "sports_game_data",
  "event_resolution_type": "event_observable",
  "market_subjects": ["NBA basketball game"],
  "market_action": "game outcome",
  "market_info_type": "sports_game_data",
  "market_resolution_type": "event_observable",
  "categories": ["Sports"],
  "snippet": "nba"
}
NOTE: The question mentions specific entities ("Los Angeles Lakers", "New York Knicks", "Madison Square Garden") and a specific date, but the template uses <TEAM>-<TEAM> placeholders. The subjects MUST NOT include any of those specific entities — abstract to the generic "NBA basketball game" since the template covers all NBA matchups. Do not include team names, venue names, or specific dates that the template normalization abstracted away.
"""


def _full_system(base_system: str, fewshot: str) -> str:
    return base_system + "\n\nExamples:\n" + fewshot


def _parse_response(raw: str) -> dict:
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)
