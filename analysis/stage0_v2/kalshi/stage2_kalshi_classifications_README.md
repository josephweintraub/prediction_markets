# Kalshi Contract Classifications

**Dataset:** `stage2_per_contract_kalshi.parquet` (6,520,137 rows, one per single-contract ticker)
**Cost:** $14.71 (Anthropic Batch API + prompt caching)
**Date built:** 2026-05-28

---

## Goal

Build a per-contract dataset of LLM-extracted descriptive tags (subjects, action, info-type, resolution mechanic, Polymarket category) for every Kalshi single-contract ticker, using the identical schema as `stage2_per_contract_augmented.parquet` (the Polymarket sister dataset), so that downstream learnability scoring and favorite-longshot-bias (FLB) analyses can pool across both platforms.

Every contract carries two parallel sets of tags: an **event-level** view (the umbrella event family the contract sits under, e.g. "Bitcoin daily-close strike contracts") and a **market-level** view (the specific outcome, e.g. "Bitcoin above $101,750 on Feb 5, 2025"). This preserves the distinction between "this event is about Bitcoin daily price" and "this contract is specifically about the $101,750 strike."

The categories, info-types, and resolution-types use the same vocabulary as the Polymarket dataset (13-category taxonomy: Politics, Sports, Crypto, Esports, Iran, Finance, Geopolitics, Tech, Culture, Economy, Weather, Mentions, Elections; resolution_type is one of `data_driven_numeric` or `event_observable`).

---

## Output schema (per-contract)

One row per Kalshi `ticker`. Columns:

| Column | Type | Description |
| --- | --- | --- |
| `ticker` | string | Kalshi ticker, the platform's primary contract identifier (e.g. `KXBTCD-25FEB0511-T101749.99`) |
| `event_ticker` | string | Parent event ticker (e.g. `KXBTCD-25FEB0511`); groups all strike variants for the same event |
| `question` | string | Human-readable question Kalshi displays to users |
| `contract_subtitle` | string | Short outcome description (e.g. "$101,750 or above") |
| `yes_sub_title`, `no_sub_title` | string | Outcome descriptions for each side of the binary |
| `market_type` | string | Kalshi market type (almost always `binary`) |
| `strike_type` | string | Kalshi strike type: `greater`, `between`, `structured`, `custom`, etc. |
| `floor_strike`, `cap_strike` | double | Numeric strike values where applicable (0 for non-numeric structures) |
| `status`, `result` | string | Lifecycle status and resolution result |
| `open_time`, `close_time`, `primary_contract_date` | string | Lifecycle timestamps |
| `volume`, `open_interest` | int64 | Activity metrics at snapshot time |
| `dollar_volume`, `dollar_open_interest` | double | Dollarized activity metrics |
| `event_template` | string | Ticker prefix, alphabetic-numeric root before the first dash (e.g. `KXBTCD`). The Kalshi-side analog of Polymarket's `event_template`. |
| `market_template` | string | Normalized question text (Stage 0: dates / numbers / time-of-day / team names / player names collapsed to placeholders). Analog of Polymarket's `market_template`. |
| `event_subjects` | list[string] | LLM-extracted ranked list of entities the parent event family is about (most central first) |
| `event_action` | string | Short noun phrase describing the verb-essence of the event family |
| `event_info_type` | string | Lowercase snake_case for the kind of public info determining outcomes |
| `event_resolution_type` | string | `data_driven_numeric` or `event_observable` |
| `market_subjects` | list[string] | Same as event_subjects but for the specific market level |
| `market_action` | string | Same as event_action but for the specific market |
| `market_info_type` | string | Same as event_info_type but for the specific market |
| `market_resolution_type` | string | Same as event_resolution_type but for the specific market |
| `categories` | list[string] | Multi-assignment from Polymarket's 13-category list |
| `snippet` | string | Verbatim 3-5 word fragment from the template or question (hallucination check) |
| `extraction_error` | string | Non-null if the LLM call for that prefix failed (10 prefixes errored at the LLM level) |

**Polymarket categories** (reused unchanged so cross-platform analyses can pool): Politics, Sports, Crypto, Esports, Iran, Finance, Geopolitics, Tech, Culture, Economy, Weather, Mentions, Elections.

---

## Pipeline

The pipeline has two real stages plus a deterministic propagation join. Stage 0 differs structurally from the Polymarket pipeline because Kalshi data is structurally different (see below). Stage 1 reuses the Polymarket prompt verbatim.

### Stage 0: Question normalization + parlay filter (deterministic Python)

**Why Stage 0 looks different from Polymarket's.** Polymarket slugs are word-based and meaningful (`nba-lakers-celtics-2026-04-15-moneyline`), and the platform's Stage 0 normalizer collapsed team/player names into `<TEAM>` placeholders at the slug level. Kalshi tickers are opaque codes (`KXNHLGOAL-25DEC30CARPIT-CARASVECHNIKOV37`); the semantically meaningful field is the natural-language `question`. So our Stage 0 operates on question text instead of slugs.

**Parlay filter.** Kalshi has a primitive Polymarket doesn't: compound multi-leg "AND" propositions sold as single tickers. These live under known prefix families (`KXMVE*` for the multi-variate event sports parlays, plus `KXOSCARWINNERS` and `KXCITIESWEATHER` which are also compound-prediction in shape). They aren't single bettable propositions and shouldn't be in a contract-level analysis dataset, so they get filtered out:

- 27,984,655 parlay-family tickers dropped (81% of raw Kalshi)
- 6,520,137 single-contract tickers retained

**Question normalization** (applied in order, deterministic regex):

1. Replace full dates ("Jan 14, 2025") and month-day combinations ("Jan 14") with `<DATE>`
2. Replace times of day ("5pm EST", "12am EDT") with `<TIME>`
3. Replace player-prop strike values ("20+", "25+") with `<NUM>+`
4. Replace comparator-anchored numbers ("above 6374.99", "below 1.5", "between 5525 and 5549.99") with `<NUM>`, preserving the comparator word
5. Replace standalone dollar amounts with thousands commas ("$66,750") with `$<NUM>`
6. Replace standalone comma-grouped integers and decimals ("5,549.99", "1.07339") with `<NUM>`
7. Replace bare comparators with no space ("`<67`", "`>75`") with `<<NUM>` / `><NUM>`
8. Run prefix-scoped entity-collapse rules (30+ of them) that mirror the Polymarket slug-level team-pair collapse but apply to natural-language question text

**Prefix-scoped entity collapses** (catalog, abbreviated):

| Pattern family | Affected prefixes | Becomes |
| --- | --- | --- |
| Player stat props | KXNHLPTS / AST / GOAL, KXNBAPTS / REB / AST / 3PT / STL / BLK, KXEPLGOAL, KXUCLGOAL, KXNFLRSHYDS / PASSYDS | `<PLAYER>: <NUM>+ <stat>` |
| NHL goal team-pair | KXNHLGOAL, KXNHLFIRSTGOAL | `<TEAM> at <TEAM>: Anytime|First Goal: <PLAYER>` |
| NFL touchdown scorer | KXNFLANYTD, KXNFLFIRSTTD, KXNFL2TD | `<TEAM> at <TEAM>: Anytime|First|Two-or-More Touchdown Scorer: <PLAYER>` |
| Team-vs-team game total | KXNCAAMBTOTAL, KXNBATOTAL, KXNCAAFTOTAL, KXNFLTOTAL, KXNHLTOTAL | `<TEAM> at <TEAM>: Total Points` |
| Team-vs-team game winner | KXNCAA*GAME, KXMLBGAME, KXNHLGAME, KXNBAGAME, KXEPLGAME, KXSERIEAGAME, etc. | `<TEAM> at <TEAM> Winner?` / `<TEAM> vs <TEAM> Winner?` |
| Team spread | KXNCAAMBSPREAD, KXNBASPREAD, KXNCAAFSPREAD, KXNFLSPREAD | `<TEAM> wins by over <NUM> Points?` |
| 1H winner / total | KXNCAAMB1HWINNER, KXNCAAMB1HTOTAL, KXNBA1HTOTAL | `<TEAM> vs <TEAM>: First Half Winner?` / `... Total?` |
| Tennis match | KXATPMATCH, KXWTAMATCH, KXATPCHALLENGERMATCH, KXTABLETENNIS | `Will <PLAYER> win the <PLAYER> vs <PLAYER> match?` |
| Tennis set / "round of N" | KXATPSETWINNER, KXATPMATCH (alt format) | `Will <PLAYER> win set <NUM> in the <PLAYER> vs <PLAYER> match` / `Will <PLAYER> be a winner of the <ROUND> of <TOURNAMENT>?` |
| PGA tournament | KXPGATOUR, KXDPWORLDTOUR | `Will <PLAYER> win the <TOURNAMENT>?` |
| PGA make-cut / round-N leader | KXPGAMAKECUT, KXPGAR1LEAD | `Will <PLAYER> make the cut in <TOURNAMENT>?` / `Will <PLAYER> lead at the end of round <NUM> in <TOURNAMENT>?` |
| Esports map / game | KXCS2MAP, KXLOLMAP, KXDOTA2MAP, KXCS2GAME, KXLOLGAME, etc. | `Will <TEAM> win map <NUM> in the <TEAM> vs. <TEAM> match?` |
| Esports total maps | KXCS2TOTALMAPS, KXLOLTOTALMAPS | `Will over <NUM> maps be played in the <TEAM> vs. <TEAM> <GAME> match?` |
| NBA double-double / triple-double | KXNBA2D, KXNBA3D | `<PLAYER>: Double Double` |
| Soccer championship | KXSERIEA, KXBUNDESLIGA, KXLALIGA, KXEPL | `Will <TEAM> win the Serie A?` (one row per league) |
| Olympic gold medal | KXWOFREESKI, KXWOXC, etc. | `Will <ATHLETE> win the gold medal in <EVENT>?` |
| NASCAR race | KXNASCARRACE | `<RACE> Winner` |
| Spotify top song / artist | KXSPOTIFYD, KXSPOTIFY2D, KXSPOTIFYGLOBALD, KXSPOTIFYARTISTD | `Will the top Song on <DATE> be <SONG>?` / `Will the top Artist on <DATE> be <ARTIST>?` |
| March Madness qualification | KXMARMADROUND | `Will <TEAM> qualify for the Men's March Madness <ROUND>?` |
| House race | KXHOUSERACE | `Will <PARTY> win the House race for <DISTRICT>?` |
| NFL season wins | KXNFLWINS | `Will the <TEAM> pro football team win more than <NUM> times this season?` |
| Weather high temp with city in question | KXHIGHNY, KXHIGHCHI, KXHIGHAUS, KXHIGHMIA, etc. | `Will the high temp in <CITY> be <NUM>-<NUM>° on <DATE>?` |
| Weather max temp (city in prefix) | KXHIGHTSEA, KXHIGHTLV, KXHIGHTSFO, etc. | `Will the maximum temperature be <NUM>-<NUM>° on <DATE>?` |
| Weather min temp | KXLOWTNYC, KXLOWTMIA, KXLOWTLAX, etc. | `Will the minimum temperature be <NUM>-<NUM>° on <DATE>?` |
| NBA team total | KXNBATEAMTOTAL | `Will <TEAM> score over <NUM> points?` |

Each pattern is scoped to a named set of prefixes so it can't false-positive on unrelated prefixes. 76 regression-harness assertions (collapse + distinguish + edge cases) verify the normalizer behaviour on canonical examples; all pass.

**Worked example:**

```
4 raw Kalshi ticker questions under KXBTCD:
  Bitcoin price  on Feb 5, 2025?      (note Kalshi double-space)
  Bitcoin price  on Feb 22, 2025?
  Bitcoin price  on Mar 10, 2025?
  Bitcoin price  on Apr 15, 2025?

  all collapse to one (event_template, market_template):
  (KXBTCD, "Bitcoin price on <DATE>?")
```

That pair gets sent to the LLM once, and the classification propagates back to all 587,762 KXBTCD tickers via the join described below.

**Outcome of Stage 0**:

- 81,267 distinct (event_template, market_template) pairs (down from 191,777 with only date/number normalization)
- 7,153 distinct ticker prefixes (the LLM call grain, see Stage 1)

### Stage 1: Per-prefix extraction (LLM, production)

**Why per-prefix instead of per-pair.** Polymarket made 107,849 LLM calls, one per (event_template, market_template) pair, because each pair represented a genuinely distinct event type already collapsed at the slug level (NBA moneyline vs NBA spread vs NHL moneyline, etc.). For Kalshi, after Stage 0:

- 94% of tickers fall under prefixes with a single template (per-prefix and per-pair are identical there)
- For the remaining 6% of tickers (under prefixes with multiple templates, e.g. KXNHLGOAL with 350 templates after entity collapse), the LLM extractions would be identical because the within-prefix variation is in entity names, not topic. KXNHLGOAL with a Hurricanes-vs-Penguins template and a Bruins-vs-Maple-Leafs template both produce `event_subjects: ["NHL hockey game"]`, `categories: ["Sports"]`. Running both is redundant.

So we LLM-classify at the prefix grain: one call per `event_template` (7,153 total), with a placeholder-rich representative template and a sample question for entity disambiguation. The extraction propagates to every ticker under that prefix.

**Prompt**: identical to Polymarket Stage 2 production (`SYSTEM_A` + `FEWSHOT_A`, 12 few-shot examples covering all 13 categories, ~2,940 tokens). Each call's user message:

- `event_template`: the ticker prefix (e.g. `KXBTCD`)
- `market_template`: the normalized question text with placeholders
- `question`: a sample full original question text (whitespace cleaned)

Returns the same JSON schema as Polymarket Stage 2: `event_subjects`, `event_action`, `event_info_type`, `event_resolution_type`, `market_subjects`, `market_action`, `market_info_type`, `market_resolution_type`, `categories`, `snippet`.

**Worked example output** for the KXBTCD prefix above:

```json
{
  "event_subjects": ["Bitcoin price"],
  "event_action": "price observation",
  "event_info_type": "crypto_price_data",
  "event_resolution_type": "data_driven_numeric",
  "market_subjects": ["Bitcoin price"],
  "market_action": "price observation",
  "market_info_type": "crypto_price_data",
  "market_resolution_type": "data_driven_numeric",
  "categories": ["Crypto"],
  "snippet": "Bitcoin price on"
}
```

**Production run** (Anthropic Batch API + prompt caching):

- Model: `claude-sonnet-4-5`, `temperature=0`
- Prompt caching with `cache_control: {type: "ephemeral"}` on the ~2,940-token system block
- Submitted as a single batch (88.9 MB total, comfortably under the Anthropic 256 MB limit)
- 7,153 requests submitted, **7,143 succeeded (99.86%)**, 10 errored at the LLM level
- Wall time: 14.1 minutes
- Cache hit rate: high (24.1M cache reads vs 1.07M cache writes)
- **Cost: $14.55** for the full batch ($14.71 including the 45-prompt validation smoke test)

### Propagation

`stage2_per_contract_kalshi.parquet` is `kalshi_per_ticker.parquet ⨝ kalshi_full_extracted.jsonl` on `event_template`, computed as a pandas left join. Deterministic, no API calls. 6,520,137 rows. 100% of single-contract tickers have a matching prefix in the extractions; the 10 prefix-level LLM errors mean 10 prefix families have null extraction fields (`extraction_error` populated) but every ticker under those prefixes inherits the same error consistently.

---

## Top-line stats

**Resolution-type breakdown (event-level)**:

- `data_driven_numeric`: 6,224,381 (95.5%), dominated by daily crypto price strikes (KXBTCD/KXETHD/KXSOLD/etc.) and S&P / Nasdaq close strikes
- `event_observable`: 295,738 (4.5%), mostly sports game outcomes, election outcomes, and political resignations

**Per-contract category coverage** (multi-assignment, so doesn't sum to 100%):

| Category | Contracts | % of total |
| --- | --- | --- |
| Crypto | 3,605,931 | 55.3% |
| Finance | 2,162,938 | 33.2% |
| Economy | 2,157,604 | 33.1% |
| Sports | 598,707 | 9.2% |
| Culture | 63,078 | 1.0% |
| Weather | 52,045 | 0.8% |
| Politics | 32,371 | 0.5% |
| Mentions | 29,845 | 0.5% |
| Esports | 13,019 | 0.2% |
| Tech | 11,302 | 0.2% |
| Elections | 7,245 | 0.1% |
| Geopolitics | 1,515 | 0.0% |
| Iran | 95 | 0.0% |

Average 1.35 categories per contract. Note the heavy Crypto / Finance / Economy skew: Kalshi is structurally weighted toward daily-price and index strike contracts, while Polymarket skewed harder toward sports + crypto. For platform-comparison work, condition on category before comparing distributions.

**Top `event_info_type` values** (the key dimension for learnability scoring):

| Info type | Contracts |
| --- | --- |
| crypto_price_data | 3,605,072 |
| stock_market_data | 1,306,602 |
| forex_price_data | 793,613 |
| sports_game_data | 527,280 |
| weather_data | 51,508 |
| sports_tournament_data | 19,217 |
| financial_market_data | 18,353 |
| forex_market_data | 17,434 |
| music_streaming_data | 17,077 |
| commodity_price_data | 12,170 |
| esports_match_data | 12,123 |
| awards_ceremony | 10,337 |
| political_speech | 9,829 |
| election_outcome | 6,741 |
| economic_data_release | 6,172 |

---

## Known limitations

1. **Per-prefix LLM grain trades off within-prefix variation.** For ~10% of prefixes there is genuine within-prefix template variation that the per-prefix call doesn't capture. The variation is overwhelmingly entity-level (different speech events, different strike values within a multi-strike format) rather than topic-level, so the inherited classification is still correct, but a small minority of prefixes mix multiple bet structures (above / below / between for the same instrument) where the per-prefix call sees only one variant. For analyses that need per-pair granularity within these prefixes, run a targeted second-pass LLM batch on the ~10K non-dominant templates (cost ~$10).

2. **One LLM judgment call required manual correction.** KXNHLPTS extraction originally returned `["AHL hockey game"]` instead of `["NHL hockey game"]` because the representative player (Mavrik Bourque) has played in both the AHL and NHL. Patched in both the JSONL and parquet (34,024 ticker rows). Cross-checked the JSONL for any other "AHL " mentions; the only other one was KXAHLGAME, which is correctly tagged.

3. **Parlay-family tickers are filtered out entirely**, not classified. 27.98M tickers under `KXMVE*`, `KXOSCARWINNERS`, and `KXCITIESWEATHER` are dropped. These are compound multi-leg propositions that aren't single bettable contracts and don't fit the per-contract classification schema. If you need to analyze parlay markets, treat them as a separate dataset.

4. **10 LLM-level extraction errors** out of 7,153 prefixes. These have `extraction_error` populated and null extraction fields; easy to filter out for any analysis.

5. **Some Kalshi-prefix codes are opaque to the LLM.** For example, KXBETBG correctly resolves as "BET Awards Best Group" because the LLM knows BET; more obscure short prefixes like KXWAINWRIGHTBANANAS ("Will Wainwright attend Savannah Bananas game") get reasonable but not perfect subjects because the LLM lacks specific context. Cross-checked the top 50 prefixes by ticker count manually; all look correct.

6. **Subject vocabularies are LLM-generated free-form**, not from a fixed taxonomy. Same caveat as Polymarket: within an entity family ("Bitcoin price" vs "Bitcoin" vs "Bitcoin (BTC) price"), variants are stable but not perfectly canonical. Downstream code may want a lightweight variant merge for recurrence-counting purposes.

7. **Volume / open_interest in this snapshot.** Many tickers carry 0 volume / open_interest because the snapshot point predates close for newer contracts. The metric is still useful for analyses that condition on activity, but not as a direct trade-count.

---

## Files

All in Dropbox at `Polymarket Data and Code/kalshi/stage2_classifications/`:

| File | Size | Description |
| --- | --- | --- |
| `stage2_per_contract_kalshi.parquet` | 226 MB | Primary deliverable. One row per Kalshi single-contract ticker. Recommended for analysis. |
| `kalshi_full_extracted.jsonl` | 4.7 MB | Prefix-level LLM extractions (7,153 rows, source of truth before per-contract propagation). |
| `stage2_kalshi_classifications_README.md` | (this file) | Methodology + schema. |

The raw Kalshi questions/metadata source dataset lives one level up in Dropbox at `Polymarket Data and Code/kalshi/kalshi_contract_questions_dates_available.parquet` (3.88 GB, 34.5M rows including parlays).

---

## Cost breakdown

| Component | Tokens | Rate | Raw cost | After 50% batch discount |
| --- | --- | --- | --- | --- |
| Input (uncached user content) | 425,217 | $3 / MTok | $1.28 | $0.64 |
| Output | 1,104,924 | $15 / MTok | $16.57 | $8.29 |
| Cache write | 1,067,772 | $3.75 / MTok | $4.00 | $2.00 |
| Cache read | 24,139,400 | $0.30 / MTok | $7.24 | $3.62 |
| **Total (full batch)** | | | **$29.09** | **$14.55** |

Adding the 45-prompt validation smoke test ($0.16) brings the grand total to **$14.71**. Without caching the same run would have cost ~$48; without per-prefix dedup (running per-pair at 81K calls) would have cost ~$160. The combination of per-prefix dedup, prompt caching, and the batch API kept it under $15.
