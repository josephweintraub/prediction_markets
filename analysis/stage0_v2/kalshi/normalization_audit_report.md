# Kalshi Normalization Audit Report

After parlay filtering, we have 6.52M single-contract tickers across 7,153 prefixes producing 191,777 distinct (event_template, market_template) pairs.

## Summary classification

| Classification | Prefixes | Tickers |
|---|---:|---:|
| SINGLE_TEMPLATE (already well-normalized) | 3,750 | 4,392,705 |
| HETEROGENEOUS (semantically diverse, can't pattern-collapse) | 1,492 | 442,813 |
| LOW_VOLUME_TAIL (1–5 tickers, 1–5 templates) | 1,098 | 4,202 |
| PARTIALLY_STRUCTURED (mixed patterns) | 469 | 135,683 |
| STRUCTURED_SIMILAR (template-rich, candidates for new patterns) | 332 | 1,396,807 |
| NEEDS_NORM_SPORTS (already-identified known patterns) | 12 | 148,929 |

Total tickers: 6,520,137

## Critical finding: my original 10-pattern proposal was insufficient

The deep inspection of the top-50 multi-template prefixes (sorted by ticker count) revealed **at least 16 distinct entity-collapse patterns** spanning sports props, weather, music charts, FX variants, and game/spread/total markets. Several patterns I initially guessed for (e.g., NHL goal = "Team at Team: Anytime Goal: Player") were wrong; the actual KXNHLGOAL templates are `<PLAYER>: <NUM>+ goals`.

## Pattern catalog (all 16 identified)

| # | Pattern (regex sketch) | Collapse to | Affected prefixes | Tickers | Templates |
|---|---|---|---|---:|---:|
| A1 | `<PLAYER>: <NUM>+ <STAT>` | same with `<PLAYER>` placeholder | KXNHLPTS/AST/GOAL, KXNBAPTS/REB/AST/3PT, KXEPLGOAL | ~141K | ~17K |
| A2 | `<PLAYER> records <NUM>+ <STAT>` (variant) | same | same (mixed format) | (shared with A1) | (shared) |
| B  | `<PLAYER>: First Goalscorer` | with `<PLAYER>` | KXNHLFIRSTGOAL | 16K | 6,599 |
| C  | `<PLAYER> records <NUM>+ (receiving yards\|receptions)` | with `<PLAYER>` | KXNFLRECYDS, KXNFLREC | 17.6K | 597 |
| D  | `<TEAM> at <TEAM>: Total Points` (sometimes "Spread Total Points") | with `<TEAM>` | KXNCAAMBTOTAL, KXNBATOTAL, KXNCAAFTOTAL, KXNFLTOTAL, KXNHLTOTAL | ~73K | ~8K |
| E  | `<TEAM> at <TEAM> Winner?` (also "vs" variant) | with `<TEAM>` | KXNCAAMBGAME, KXNCAAWBGAME, KXNCAABBGAME | ~25K | ~10K |
| F  | `<TEAM> vs <TEAM>: First Half Total?` | with `<TEAM>` | KXNCAAMB1HTOTAL | 9.5K | 1,015 |
| G  | `<TEAM> wins by over <NUM> Points?` | with `<TEAM>` | KXNCAAMBSPREAD, KXNBASPREAD, KXNCAAFSPREAD, KXNFLSPREAD | ~80K | ~570 |
| H  | `Will <TEAM> win the 1H by over <NUM> points?` | with `<TEAM>` | KXNCAAMB1HSPREAD | 11K | 366 |
| I1 | `<TEAM> at <TEAM>: (Anytime\|First) Touchdown Scorer: <PLAYER>` | with placeholders | KXNFLANYTD, KXNFLFIRSTTD | 11K | ~10.8K |
| I2 | `<PLAYER>: (Anytime\|First) Touchdown` (short variant) | with `<PLAYER>` | (same prefixes) | (shared) | (shared) |
| J  | `Will <PLAYER> win the <TOURNAMENT>?` (prefix-restricted) | with placeholders | KXPGATOUR only | 5.4K | 5,013 |
| K  | `Will <PLAYER> win the <PLAYER> vs <PLAYER> ... match?` | with placeholders | KXATPMATCH, KXATPCHALLENGERMATCH, KXWTAMATCH, KXTABLETENNIS | ~19K | ~17K |
| L  | `Will the top Song on <DATE> be <SONG_TITLE>?` (one of many formats per prefix) | with `<SONG>` | KXSPOTIFYD, KXSPOTIFYGLOBALD, KXSPOTIFY2D, KXSPOTIFYARTISTD | ~25K | ~738 |
| M  | `Will the high temp in <CITY> be <NUM>-<NUM>° on <DATE>?` | with `<CITY>` | HIGHNY, HIGHCHI, possibly others | ~11K | ~1.3K |
| N  | `Will <TEAM> score over <NUM> points?` | with `<TEAM>` | KXNBATEAMTOTAL | 5.1K | 30 |

Total impact: ~16 patterns cover ~400K tickers and ~75K templates (39% of templates after MVE filter). After applying, total template count would drop from 191,777 to roughly 115,000.

## Data quality issues discovered

Beyond pattern collapse, I found several issues in the raw Kalshi data worth flagging in the paper:

1. **Comma-grouped dollar amounts not normalized** (e.g., BTCD: `Bitcoin price $66,750 or above`). Current Stage 0 only handles `$X` immediately after comparator words. Need to add a standalone `$<digits>,<digits>` rule. **Fix needed.**
2. **Duplicate phrase: "Spread Total Points Total Points?"** (KXNFLTOTAL) and "Total Points Total Points?" (KXNCAAFTOTAL). These look like data-entry duplications. Pattern D's regex should accept either form. **Handled by D.**
3. **Double question marks `??`** in WTI templates. Cosmetic. Leave as-is.
4. **Markdown bold** in HIGHNY: `**high temp in NYC**`. Leave or strip? Currently leaves — pattern M should handle the `**`. **Handled by M.**
5. **`records` vs `:` for player stats** (NHL): both `Pavel Dorofeyev: <NUM>+ points` and `Ivan Demidov records <NUM>+ points` exist for same prefix. Need both patterns A1 + A2. **Handled.**
6. **`at` vs `vs` for team pairs**: KXNCAAMBGAME uses `at`, KXNCAABBGAME uses `vs`. Need both forms. **Handled.**
7. **Case variants for "Points"/"points"**: Pattern G regex needs to be case-insensitive. **Handled.**

## What WON'T be normalized (and why)

- **3,750 single-template prefixes**: nothing to collapse.
- **1,492 HETEROGENEOUS prefixes**: their templates are semantically diverse (different question types under one prefix). Each template would need its own classification. These are mostly low-volume (avg ~300 tickers/prefix).
- **1,098 LOW_VOLUME_TAIL**: 4,202 tickers total — negligible. One LLM call per unique template is fine.

## Trade-offs (paper-relevant)

| Path | Templates after | LLM calls (per-prefix) | LLM cost | Abstraction quality |
|---|---:|---:|---:|---|
| v1 (current) | 191,777 | 7,153 | ~$15 | High risk of over-specific subjects for top-10 sports prefixes |
| v2 (16 patterns) | ~115,000 | 7,153 | ~$15 | Clean placeholder signals on ~400K tickers in heavy-volume sports prefixes |
| v2 + per-template LLM | ~115,000 | ~115,000 | ~$115 | Most rigorous; captures all within-prefix variation |

The v2 normalizer + per-prefix LLM is the recommended path: same cost as v1 ($15), much cleaner LLM outputs for the sports prefixes.

## My recommendation

Proceed with **v2 normalizer extension** implementing all 16 patterns + the standalone-$-amount fix. Update the harness with new collapse / distinguish assertions for each pattern (~30 new test cases). Re-run on the full dataset. Re-audit to confirm no major patterns left. Then return to G2 with cleaner inputs.

Engineering estimate: ~1 hour to implement and harness-verify; ~10 minutes to re-run on EC2.

---

## Results — v2.3 final (after extension + residue iteration)

| Stage | Templates | Reduction vs v1 |
|---|---:|---:|
| v1 (Stage 0 base only) | 191,777 | — |
| v2.0 (16 patterns + $-comma + parlay filter) | 133,274 | -30% |
| v2.1 (+ 7 more residue patterns: temperatures, esports maps, March Madness, House race, NFL wins, PGA top-N, WTI `<X`) | 123,769 | -36% |
| v2.2 (+ KXNFL2TD `Two-or-More`, LoL/League of Legends unification) | 122,248 | -36% |
| **v2.3 (+ NHL Goal Team-at-Team alternate, ATP `round of N` alternate, NCAA `: Total` short form, picker fix)** | **108,652** | **-43%** |

Harness: 76/76 blocking assertions pass. 3 known-unhandled cases logged as expected_improvements (FX KX-prefix migration; team/player collapse improvements that v2.3 actually now does).

24 prefix-scoped entity-collapse patterns implemented:
- A1+A2+A3 (NHL/NBA/EPL player stats: colon, records, team-at-team variants)
- B (NHL first goalscorer)
- C (NFL receptions / receiving yards)
- D (team-at/vs total points + `: Total` short)
- E (team-at/vs winner — `at` and `vs` forms)
- F (NCAA 1H total)
- G (NCAA/NBA/NFL spread)
- H (NCAA 1H spread)
- I1+I2 (NFL Anytime/First/Two-or-More TD scorer — full + short)
- J (PGA tournament winner)
- K + K2 (Tennis: 'X vs Y match' + 'round of N of <TOURNAMENT>')
- L (Spotify song / artist)
- M + M2 + M3 (high temp with city / max temp / min temp)
- N (NBA/NFL team total)
- O (WTI bare comparator `<N`/`>N`)
- P (PGA top-N finish)
- Q (CS2/LoL/Dota2/Valorant total maps)
- R (March Madness qualification)
- S (House race party-vs-district)
- T (NFL season wins)

The remaining 1,469 HETEROGENEOUS prefixes (235K tickers, 3.6% of dataset) contain genuinely diverse content per prefix (mention markets like KXTRUMPMENTIONB, multi-purpose long-tail political prefixes) and are not regex-collapsible without semantic loss. They get per-template within-prefix variation but identical per-prefix LLM extraction.

Final template-pair count: **108,652 across 7,153 prefixes**. LLM batch grain stays per-prefix (7,153 calls, ~$15 estimated).
