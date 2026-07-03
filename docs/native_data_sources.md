> **Status 2026-07-02:** the §7 re-pull was **executed 2026-06-21** → `/mnt/data/learnability/native/native_market_meta.parquet` (1.44M markets, 99.98% coverage — but **closed markets only**, contrary to §6's pull-both-halves advice; open markets are not covered). The tags→category map shipped 2026-07-01 (`analysis/learnability/native/final_tag_map_v1.json`). The field inventory and field→dimension map below remain the working reference; universe counts ("620K") and request estimates predate the June data extension.

# Polymarket Native Data — Inventory & Field Guide

_Companion to `dimension_guide.md`. Purpose: **replace the LLM-derived contract labels** (`event_template`, `event_info_type` regex, subject-list lengths) that drive the current 22 learnability dimensions with **platform-native fields** that Polymarket already publishes. This doc records what native data exists, where it comes from (with clickable links), how clean it is, and which muddy LLM dim each native field retires._

> **Why this exists.** Every current dim is an LLM approximation of something Polymarket labels directly. We dumped 10 columns from Gamma; the live API returns **78 fields per market, 47 per event, 26 per series**. The native fields are cleaner, free, and auditable. This is the basis for ditching the AI labelling.

---

## 0. The headline: native fields are dramatically cleaner

One column (`resolutionSource`) splits anchorable-vs-judgment markets near-perfectly, with **zero** LLM involvement:

| primary_cat | n | has source | price-feed | official scorer | blank = judgment |
|---|--:|--:|--:|--:|--:|
| Crypto | 177K | 79% | 78.8% | 0% | 21% |
| Sports | 169K | 79% | 0% | 50.8% | 21% |
| Esports | 20K | 84% | 0% | 0% | 16% |
| Finance | 15K | 75% | 0% | 0% | 25% |
| Politics | 21K | 0.9% | 0% | 0% | **99.1%** |
| Mentions | 15K | 0.1% | 0% | 0% | **99.9%** |
| Culture | 15K | 7% | 0% | 0% | **93%** |
| Geopolitics | 4K | 1.6% | 0% | 0% | **98.4%** |
| Weather | 17K | 36% | 0% | 0% | 64% |

The "anchored" categories (Crypto/Sports/Esports/Finance, 75–85% sourced) vs the "judgment" categories (Politics/Mentions/Culture/Geopolitics, ~99% blank) fall straight out of the field. Weather's 36% is a genuine mix, not noise. Compare to `dim_resolution_type`, which needed an LLM and still produced an uninterpretable `unknown` junk bucket.

---

## 1. What we have locally today (snapshots — and their gaps)

| File (`analysis/output/`) | Rows | Cols | Source | Coverage of the 620K augmented markets |
|---|--:|--:|---|---|
| `stage2_per_contract_augmented.parquet` | 1.12M tokens / 620K markets | 31 | LLM pipeline (Stage 0 + Stage 2) | 100% (this *is* the universe) |
| `gamma_markets_full.parquet` | 406K markets | **10** | Gamma `/markets` (Feb 2026 pull) | ~42% join |
| `market_resolutions.parquet` | 398K markets | 8 | Gamma (incl. `resolutionSource`) | ~41% join |
| `event_categories.parquet` | 315K slugs | 3 | Gamma `/events` (`category`, `tags`) | ~39% of slugs |

**Two problems with the local snapshots:** (a) they pulled only a handful of the available fields, and (b) they cover only ~40% of the analyzed universe. Both are fixed by one re-pull (§7).

---

## 2. The live APIs — where the native data comes from

All read endpoints below are **public, no auth, JSON**. Paste any URL into a browser to see the raw object.

### Gamma API — `https://gamma-api.polymarket.com`
The metadata API. This is the one that replaces the LLM labels.
- **`/markets`** — one row per market (YES/NO outcome pair). **78 fields.** → [`/markets?limit=1&closed=true`](https://gamma-api.polymarket.com/markets?limit=1&closed=true)
- **`/events`** — one row per event (groups related markets); carries `series`, `tags`, `liquidity`, `commentCount`, `competitive`. **47 fields.** → [`/events?limit=1&closed=true`](https://gamma-api.polymarket.com/events?limit=1&closed=true)
- **`/series`** — the recurring-market grouping; carries **`recurrence`** ("daily" …). **26 fields.** → [`/series?slug=nba`](https://gamma-api.polymarket.com/series?slug=nba)
- Filter by slug for any market/event we have: [`/events?slug=nba-lal-gsw-2025-10-05`](https://gamma-api.polymarket.com/events?slug=nba-lal-gsw-2025-10-05)

### CLOB API — `https://clob.polymarket.com`
The order-book / price API. Not needed to replace labels, but gives **price history** if we want a trajectory measure later.
- **`/prices-history?market=<clobTokenId>&interval=max`** — time series of the token price.

### Data API — `https://data-api.polymarket.com`
Wallet/position level. Relevant if we ever build trader-experience features.
- **`/holders`, `/trades`, `/positions`, `/value`** — by market or wallet.

_(Exact pagination, rate limits, enum value sets, and CLOB/data-api params are being finalized by a docs-research pass — see §6, filling shortly.)_

---

## 3. Worked examples — real markets, click through and see for yourself

Each block: the question, the **Polymarket page**, the **Gamma API call** (full native JSON), and the native fields that matter. Note the contrast in `resolutionSource` (objective feed → official → social → **blank/judgment**).

**① Crypto up/down — objective price feed, recurring, fast feedback**
- Q: *Bitcoin Up or Down — November 19, 8:45AM–9:00AM ET*
- Page: https://polymarket.com/event/btc-updown-15m-1763559900 · API: https://gamma-api.polymarket.com/events?slug=btc-updown-15m-1763559900
- `resolutionSource = data.chain.link/streams/btc-usd` · `tags = [Up or Down, Crypto Prices, Recurring, 15M]` · life: created 11-18 → closed 11-19 · `event_action = price direction`

**② NBA game — official scorekeeper, native daily series**
- Q: *Lakers vs. Warriors*
- Page: https://polymarket.com/event/nba-lal-gsw-2025-10-05 · API: https://gamma-api.polymarket.com/events?slug=nba-lal-gsw-2025-10-05
- `resolutionSource = nba.com` · `series = {slug: nba-2026, recurrence: daily}` · `volume = 183,283` · `commentCount = 527` · `tags = [Sports, NBA, Games]`

**③ Tweet market — social-media source**
- Q: *Will Elon Musk post 580+ tweets from Dec 12–19, 2025?*
- Page: https://polymarket.com/event/elon-musk-of-tweets-december-12-december-19
- `resolutionSource = x.com/elonmusk` · `tags = [Culture, Politics, Tweet Markets]` · `market_action = tweet count threshold`

**④ Tennis — official scorer**
- Q: *Next Gen ATP Finals, Group B: Blockx vs Engel*
- Page: https://polymarket.com/event/atp-blockx-engel-2025-12-17
- `resolutionSource = atptour.com/en/scores/current` · `tags = [Tennis, Sports, Games]`

**⑤ Election — JUDGMENT, no source**
- Q: *Will the Conservatives win 120–139 seats in the next Canadian Election?*
- Page: https://polymarket.com/event/seats-conservatives-win-in-canadian-election
- `resolutionSource = "" (blank)` · `tags = [Canadian Election, Politics, Global Elections]` · `event_action = seat count outcome`

**⑥ Geopolitics / military — JUDGMENT, no source**
- Q: *Another US strike on Venezuela on January 12?*
- Page: https://polymarket.com/event/another-us-strike-on-venezuela-on
- `resolutionSource = "" (blank)` · `tags = [Politics, Venezuela, Geopolitics]` · `event_action = military strike`

**⑦ Awards — JUDGMENT, no source**
- Q: *Will 'KONOSUBA 3' win Crunchyroll's Best Isekai Award for 2025?*
- Page: https://polymarket.com/event/crunchyroll-best-isekai
- `resolutionSource = "" (blank)` · `tags = [Culture, Awards, anime]` · `event_action = award winner`

**⑧ Sports prop — native bet-structure (`sportsMarketType` + `line`)**
- From a live `/markets` pull: *Games Total: O/U 2.5* → `sportsMarketType = totals`, `line = 2.5`, `resolutionSource = hltv.org`, `negRisk = false`. Polymarket labels the bet *structure* (moneyline / spreads / totals) natively — no need to parse "over/under threshold" out of an LLM `market_action`.

---

## 4. The replacement map — native field → learnability channel → LLM dim it retires

This is the core of "ditch the AI labelling." Each native field comes from the Gamma object noted; the right-hand column lists the current `dim_*` it replaces (see `dimension_guide.md`).

### Channel A — Repetition & feedback ("do people learn with reps?")
| Native field | From | Replaces |
|---|---|---|
| `series` / `seriesSlug` / `series.id` | `/events`, `/series` | `dim_event_family_size`, `dim_event_slug_size`, `dim_group_strict_size` |
| `series.recurrence` ("daily"/"weekly"/…) | `/series` | `dim_recurrence_class` (the 100/60/0.5 magic-constant heuristic) |
| # prior events in same `series` before `createdAt` | `/events` per series | `dim_prior_settlements_bin__*` (all 3) |
| `createdAt → closedTime` (and `endDate`, `umaEndDate`, `gameStartTime`) | `/markets` | `dim_contract_horizon` (currently trade-span = endogenous) |

→ **8 LLM-heuristic dims collapse into the native `series` + timestamps.**

### Channel B — Anchorability / objectivity ("is there a reference to price against?")
| Native field | From | Replaces |
|---|---|---|
| `resolutionSource` (URL → feed / official / social / **blank**) | `/markets` | `dim_resolution_type`, `dim_info_type_supergroup` |
| `automaticallyResolved` (bool) | `/markets` | — (new, cleaner objectivity flag) |
| `umaResolutionStatus` / `umaResolutionStatuses` (proposed/resolved/**disputed**) | `/markets` | — (new: resolution *ambiguity* signal) |
| `description` (full resolution criteria text) | `/markets`,`/events` | — (parse for ambiguity directly, if wanted) |

### Channel C — Proposition complexity / structure ("how hard is the proposition?")
| Native field | From | Replaces |
|---|---|---|
| `sportsMarketType` (moneyline/spreads/totals) + `line` | `/markets` | `dim_market_specificity`, much of `market_action` |
| `negRisk` / `enableNegRisk` / `negRiskAugmented` | `/markets`,`/events` | `dim_outcomes_per_event` (was a condition_id-count proxy) |
| `outcomes` length | `/markets` | cardinality, directly |

### Confounder to NAME, not call learnability — sophistication / attention / liquidity
| Native field | From | Use |
|---|---|---|
| `liquidity`, `liquidityClob` | `/events` | market depth |
| `volume`, `volumeNum`, `volume24hr/1wk/1mo/1yr`, `openInterest` | `/events`,`/markets` | replaces `dim_dollar_volume_tier` + family-vol dims with native, time-resolved volume |
| `commentCount` | `/events`,`/series` | crowd size / attention (new) |
| `competitive` | `/events`,`/series` | Polymarket's own "how contested" score (new) |
| `spread`, `bestAsk` | `/markets` | microstructure |

### Category itself
| Native field | From | Replaces |
|---|---|---|
| `category`, `tags[]` (human-curated) | `/events` | `dim_primary_category` (LLM `categories[0]`) |

---

## 5. Evidence the native fields are clean (not just available)

- **Anchorability** — §0 table: `resolutionSource` cleanly separates anchored (75–85% sourced) from judgment (~99% blank) categories. No `unknown`/`other` junk bucket.
- **Recurrence** — `/events?slug=nba-lal-gsw-2025-10-05` → `series.recurrence = "daily"`; `/series?slug=nba` confirms cadence. The native series links every NBA game to one family with a cadence label, vs the LLM `event_template` normalizer's mis-grouping.
- **Coverage of recurrence is split across two native fields** (worth noting): the `Recurring` *tag* flags crypto/weather dailies (Crypto 95%, Weather 89% Recurring-tagged) but **not** sports (0% tagged) — for sports the recurrence lives in the `series` object instead. So the clean recurrence feature = `series.recurrence` **OR** `tags ∋ Recurring`, not either alone.

---

## 6. API access mechanics — pagination, rate limits, enums, bulk-pull

All read endpoints are public (no auth). Confirmed against the live API + official docs (`docs.polymarket.com/api-reference`).

**Pagination** (Gamma `/markets`, `/events`, `/series`):
- *Offset* — `?limit=&offset=`; practical cap **~500/request**; bare JSON array, no page metadata. Fine for a one-shot pull.
- *Keyset/cursor* — `/markets/keyset`, `/events/keyset`; `?limit=1..100&after_cursor=<token>` → `{ "<resource>":[...], "next_cursor":"..." }`; `next_cursor` omitted on last page (`offset` rejected, HTTP 422). **Use this for the bulk pull** — stable under concurrent inserts.
- **Gotcha:** `closed`/`active` default to *unfiltered* — pull `closed=true` AND `closed=false&active=true` separately to cover the whole universe.

**Rate limits** (req / 10s): Gamma overall 4,000 · `/events` 500 · `/markets` 300. CLOB overall 9,000 · `/prices-history` 1,000. Data API overall 1,000 · `/trades` 200. → a 620K-market re-pull at 500/page ≈ **~1,240 requests**, a few minutes well within limits.

**Enum value sets** (enumerated from the live API — docs don't list them):

| Field | Values | Notes |
|---|---|---|
| `series.recurrence` | `daily`, `weekly`, `monthly`, `annual` | NBA/NFL = daily, FOMC = monthly. Filter via `?recurrence=` |
| `series.seriesType` | `single` (only value observed) | |
| `sportsMarketType` | `moneyline`, `spreads`, `totals` (+ null = non-sports) | paired with `line` (signed for spreads) |
| `umaResolutionStatuses` | array of `proposed` / `disputed` / `resolved` | chronological oracle log; `["proposed","disputed","proposed","resolved"]` = a **contested** resolution. **JSON-string-encoded — `json.loads` it** |
| `umaResolutionStatus` | `resolved` or null | terminal status |
| `automaticallyResolved` / `automaticallyActive` | bool | auto = UMA-oracle pipeline (objective) vs manual admin |
| `negRisk` / `enableNegRisk` / `negRiskAugmented` | bool | native **multi-outcome (winner-take-all) flag** — use instead of counting condition_ids; *augmented* = has placeholder "Other" outcomes |

**CLOB price history** (only if we later want a trajectory measure): `GET /prices-history?market=<CLOB tokenId, 77-digit>&interval=max&fidelity=60` → `{"history":[{"t":unix,"p":0..1}]}`. **Always set `fidelity`** (minutes) for closed/illiquid markets or it returns empty. `market` = token id, **not** conditionId. `interval` ∈ {max, all, 1m, 1w, 1d, 6h, 1h}. Batch variant: `/prices-history-batch`.

**Data API** (trader/holder features, all public, key by `conditionId`): `/trades`, `/holders`, `/positions`, `/v1/market-positions` (per-market PnL leaderboard: `avgPrice`/`realizedPnl`/`totalPnl`), `/value`, `/activity`.

**Encoding gotcha:** on the `/markets` object, `outcomes`, `outcomePrices`, `clobTokenIds`, and `umaResolutionStatuses` come back as **stringified JSON** — `json.loads` each before use.

**Join keys:** `conditionId` (0x 64-hex) = a market (Gamma + Data API); the 77-digit decimal `clobTokenIds` = one outcome token (CLOB price-history keys on this). Matches the EC2 trades-schema convention in `CLAUDE.md`.

Docs: [list-markets](https://docs.polymarket.com/api-reference/markets/list-markets.md) · [keyset](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination.md) · [list-events](https://docs.polymarket.com/api-reference/events/list-events.md) · [series](https://docs.polymarket.com/api-reference/series/list-series.md) · [prices-history](https://docs.polymarket.com/api-reference/markets/get-prices-history.md) · [rate-limits](https://docs.polymarket.com/api-reference/rate-limits.md) · [neg-risk](https://docs.polymarket.com/developers/neg-risk/overview)

---

## 7. Proposed data task: one native re-pull

**Goal:** land a `native_market_meta.parquet` keyed by `conditionId` covering **all 620K analyzed markets**, carrying the ~20 on-thesis native fields (the §4 tables) so every learnability dim can be rebuilt natively.

**Steps**
1. From the augmented universe, get the 620K distinct `conditionId` (and their `event_slug`s).
2. Bulk-pull Gamma `/markets` (paginated) → keep: `conditionId`, `resolutionSource`, `automaticallyResolved`, `umaResolutionStatus(es)`, `sportsMarketType`, `line`, `negRisk`, `outcomes`, `createdAt`, `endDate`, `closedTime`, `umaEndDate`, `gameStartTime`, `volumeNum`, `spread`, `description`.
3. Bulk-pull Gamma `/events` → keep: `series`/`seriesSlug`, `tags`, `category`, `liquidity`, `volume*`, `openInterest`, `commentCount`, `competitive`.
4. Bulk-pull Gamma `/series` → keep: `recurrence`, `seriesType`, and the event list per series (for prior-instance counts).
5. Derive native features: anchorability ladder (from `resolutionSource`), recurrence class (from `series.recurrence` ∪ `Recurring` tag), feedback lag (`closedTime − createdAt`), prior-instances-in-series-at-open, bet-structure (`sportsMarketType`), cardinality (`negRisk` / `outcomes`).
6. Audit enum coverage: distribution of `recurrence`, `umaResolutionStatus`, `sportsMarketType`, `negRisk` over the real universe (confirms each split is populated, not a dead label).

**Open questions / caveats**
- Coverage: confirm all 620K `conditionId` are resolvable via Gamma (some very old markets may 404 → fall back to the existing resolutions file).
- `competitive` / `liquidity` come back `None` on some closed events (saw it on the NBA event) — check populated-rate before relying on them.
- Decide grain: `series` is the clean recurrence unit, but a few series are huge (NBA = thousands) — same family-size-imbalance issue as today, just measured honestly.
