# FLB Learnability Dimensions — Working Guide

_Companion to `learnability_writeup.md`. One entry per dimension: how it's derived, what it's conceptually doing, current results, and an assessment to drive our review. We go through these one at a time — say "good" to keep one as-is, or propose a change and I'll test it._

## Data & sources

**Platform.** Polymarket is an on-chain prediction market on Polygon: every trade is a USDC-settled fill recorded in the blockchain logs. All data here is reconstructed from those logs — there is no private exchange feed. Coverage runs from Polymarket's early days (timestamps filtered from 2020-06-01) through the 2026-01-23 data ingest.

**Datasets used**

| Dataset | Grain | Size | What it is |
|---|---|---|---|
| Clean trades parquet | one trade leg | 1,377,065,934 rows (~15.7 GB) | Canonical trade history, rebuilt from Polygon event logs by the repo's 6-stage extraction pipeline (Alchemy RPC). Full-row exact duplicates removed (~4.06% — ingestion replays); re-sorted by `conditionId, timestamp`. Lives on EC2; all FLB numbers come from this set. |
| Augmented per-contract classification | one outcome-token (`token_id`) | 1,117,358 contracts × 31 cols | Per-contract metadata + LLM-extracted fields: `event_info_type`, `event_resolution_type`, `categories` (13-category taxonomy), `event/market_subjects`, `event/market_action`, and normalized `event/market_template`. Built from Polymarket **Gamma API** metadata + a two-stage LLM pipeline (Stage 0 slug normalization → Stage 2 Anthropic Batch API tagging). This is the source of every per-contract `dim_*`. |
| Market resolutions | one market (`conditionId`) | resolved markets | `winning_outcome` per settled market — defines each trade's `won` and return `ret = won − price` for calibration. |
| Bot / wallet flags | one wallet | 255,261 flagged (~21%) | Non-human wallet classifier (inter-trade interval, trades/day, hour-of-day HHI, size CV). Flagged wallets are excluded from the FLB calculations. |

**Trade-row fields the analysis touches.** `proxyWallet` (trader), `counterparty`, `timestamp`, `conditionId` (the 77-digit per-outcome token id), `price`, `side` (BUY/SELL), `outcome`, `usdcSize` (dollar size), `eventSlug`.

**Two id conventions (load-bearing).** `token_id` = a single YES *or* NO outcome (77-digit decimal). `condition_id` = the per-market `0x` id that YES and NO **share**. Count-of-contracts dimensions count distinct `condition_id` (markets), not `token_id` (outcomes) — the "markets-unit fix" referenced throughout.

**The working set for FLB.** From the 1.377B clean rows we keep BUY-side trades, drop bot wallets and up/down crypto markets, require the market to have resolved, require `0.01 < price < 0.99`, and restrict to the lifecycle window. That leaves ≈ **85M** BUY trades before windowing, and e.g. **53.0M** in the full-window run / **24.1M** in the mature (25–80%) window. Each trade contributes its calibration error `won − price`; a dimension just labels which slice each trade falls into.

## How to read each entry
- **Derivation** — the exact column(s), verbatim bins/thresholds/regex, and whether the value is per-contract metadata (from `stage2_per_contract_augmented.parquet`) or a trades-scan aggregate.
- **Concept** — what the dimension partitions and the learnability hypothesis behind it.
- **Results** — the D10-D1 calibration-error spread (= the FLB slope; positive = classic FLB, longshots lose / favorites win) with 3-way clustered-SE t-stats, both **count-weighted** (cnt, each trade = 1) and **dollar-weighted** ($, by `usdcSize`), across three lifecycle windows: **mature** 25-80%, **closing** 80-100%, **full** 0-100%. Stars: |t|>1.96 \*, >2.58 \*\*, >3.29 \*\*\*.
- **Assessment** — whether the hypothesis is borne out, confounds, SE-degenerate slices to distrust, and a concrete change worth considering.

## Provenance & caveats
- **Result numbers** are transcribed from the canonical `learnability_writeup.md` (post the markets-unit fix, where count-of-contracts dims count distinct `condition_id` = markets, not `token_id` = outcomes).
- Note: during generation the local `/tmp/v6_results/` parquet cache was found stale (pre-fix) and quarantined; the writeup is the single source of truth until those parquets are rebuilt from EC2 next session.

## Contents

1. `dim_resolution_type` — Does the outcome have an objective data feed?
2. `dim_info_type_supergroup` — Finer-grained classification of the data source
3. `dim_primary_category` — Polymarket's 13-category taxonomy
4. `dim_subject_specificity` — How many entities does the event resolve on?
5. `dim_event_family_size` — How many contracts share the event template?
6. `dim_outcomes_per_event` — Distinct markets per event_slug (binary = 1 market)
7. `dim_market_specificity` — Does the specific market narrow its parent event?
8. `dim_dollar_volume_tier` — Quartiles of per-token dollar volume
9. `dim_contract_horizon` — Duration from first to last trade
10. `dim_recurrence_class` — Family size x time-span heuristic
11. `dim_group_strict_size` — Family size on event_slug x market_template
12. `dim_event_slug_size` — Family size on event_slug
13. `dim_family_vol_tier` — Family total dollar-volume terciles
14. `dim_family_size_x_vol` — 3x3 cross-tab of family size x volume
15. `dim_vol_per_contract_tier` — Dollar-per-contract quintiles
16. `dim_vol_per_contract_residualized` — Volume per contract, residualized on size
17. `dim_text_novelty` — Semantic isolation by best-neighbor cosine
18. `dim_text_neighbors_strict` — Count of slugs above 0.75 cosine
19. `dim_prior_settlements_bin__event_template` — Prior settled markets at trade time
20. `dim_prior_settlements_bin__event_slug` — Prior settled markets (event_slug grouping)
21. `dim_prior_settlements_bin__dim_group_strict` — Prior settled markets (strict grouping)
22. `dim_market_type` — Up/down vs non-up/down sensitivity

---
## 1. `dim_resolution_type` — Does the outcome have an objective data feed?

**Derivation.** Pure per-contract metadata, no trades scan: `analysis/learnability/dimensions.py -> add_dim_resolution_type` reads the single column `event_resolution_type` from `stage2_per_contract_augmented.parquet` and copies it through with `.fillna("unknown")` — no binning, no regex, no thresholds. That source column is an LLM-assigned label taking exactly two real values (`data_driven_numeric`, 782,068 contracts; `event_observable`, 335,242 contracts) plus 48 NULLs that become the `unknown` slice. This is NOT a count-of-contracts dim, so the condition_id-vs-token_id convention does not apply to its construction (the trade-level summary still reports `n_markets` = distinct condition_id alongside `n_contracts`).

**Concept.** Partitions contracts by whether resolution rides on an objective numeric feed (`data_driven_numeric`: crypto/stock prices, weather, scores) versus subjective event observation (`event_observable`: elections, resignations, rulings). Hypothesis (writeup ll.134–136): data-driven outcomes have public reference series so traders price them well → calibrated; event-observable outcomes lack a feed → classic FLB.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| event_observable | 16.8M | +0.0250 (t+2.7**) | +0.0360 (t+2.1*) | +0.0306 (t+6.6***) | +0.0254 (t+3.9***) |
| data_driven_numeric | 7.3M | +0.0058 (t+0.6) | +0.0213 (t+2.0*) | +0.0158 (t+2.2*) | +0.0060 (t+0.9) |
| unknown | — | — | — | — | +0.0947 (t+16.9***) |

**Assessment.**
- Hypothesis is borne out in sign and direction, weakly: `event_observable` carries real, significant FLB (mature cnt +0.0250 t+2.7; closing +0.0306 t+6.6***), while `data_driven_numeric` is essentially flat on the count-weighted measure (mature +0.0058 t+0.6; full +0.0060 t+0.9). It is a two-level gradient, not a monotone sweep — the binary nature limits it to "feed → calibrated vs no-feed → FLB."
- The data-driven slice's calibration is fragile: it *does* show significant dollar-weighted FLB (mature $ +0.0213 t+2.0, closing $ +0.0411 t+5.7-equivalent at t_dol+5.73, full $ +0.0204 t+3.1) even while count-weighted is flat — so large-dollar trades in data-driven markets still overpay longshots; the "calibrated" read holds only for the typical trade, not the typical dollar.
- The `unknown` slice is SE-DEGENERATE junk: it is the 48 NULL-resolution-type contracts, surfacing at trade level as only **33 contracts / 18 markets / 8,417 trades**. Its enormous t-stats (full cnt t+16.9, $ t+19.9) come from spanning a handful of clusters — ignore entirely. It is correctly suppressed below the 5k floor in mature and closing, and leaks in only in the full window; treat as noise, not signal.
- Volume/category confound: in the per-contract file `data_driven_numeric` outnumbers `event_observable` 2.3:1 (782K vs 335K contracts), yet at trade level `event_observable` has >2x the trades (16.8M vs 7.3M mature). Event-observable markets are far more heavily traded (politics/awards), so this dim is heavily entangled with `dim_primary_category` and `dim_info_type_supergroup` — the FLB gap may be a category effect, not a resolution-mechanism effect.
- Fully dependent on a single LLM-assigned label with no human audit visible here; the two-value coarseness means there is no internal cross-check, and any LLM mislabel flows straight into the slice.
- Concrete change: drop or floor-suppress the `unknown` slice in the full window (it adds nothing and posts misleading stars), and consider replacing this binary with a finer feed-granularity split (e.g. continuous price-feed vs discrete-score vs binary-event vs human-judged) — or residualize against `dim_primary_category` — to test whether the resolution-mechanism signal survives the category confound.


> **Update (this session) — proposed 3-way recut tested.** The binary `event_observable` / `data_driven_numeric` split is muddy: the Stage-2 LLM filed identical `sports_game_data` markets inconsistently (177K `event_observable` vs 126K `data_driven_numeric` — a 59/41 split on the same market type). A deterministic 3-way recut off `event_info_type` — **scored_contest** (sports/esports games), **numeric_feed** (prices, weather, all `*_data` feeds), **judgment** (elections, rulings, awards) — was run on EC2 (clean trades, updown-excluded, bot-filtered; identical 24.08M-trade mature universe). Findings: FLB lives **entirely in `judgment`** (mature +0.041, t+9.5\*\*\*, ~2x the old `event_observable` because sports no longer dilutes it); **`scored_contest` is flat mid-life** (-0.003) and only develops FLB in the **closing** window (+0.020, t+6.3\*\*\*, public-money regime); **`numeric_feed`** is flat count-weighted but mildly +dollar-weighted (+0.023, t+3.3\*\*\*). The recut also eliminates the uninterpretable `unknown` bucket (old full +0.095, t+16.9). **Not yet wired into the pipeline — pending your decision.**

## 2. `dim_info_type_supergroup` — Finer-grained data-source class

**Derivation.** Per-contract metadata, no trades scan: reads the single column `event_info_type` from `stage2_per_contract_augmented.parquet` and runs it through a 6-rule ordered regex table (`INFO_TYPE_PATTERNS`), first-match-wins, else `"other"` (also `"other"` if the field is null/empty) — see `analysis/learnability/dimensions.py -> add_dim_info_type_supergroup` / `info_type_supergroup`. The patterns (all `re.I`): `market_data` = `(crypto|stock|equity|forex|fx|commodity|treasury|yield|interest_rate|bond).*?(price|return|level|yield|rate|data)`; `sports_data` = `sports?|game|team|player|league|tournament|match`; `weather_data` = `weather|temperature|rainfall|snow|hurricane|storm`; `awards` = `award|oscar|emmy|grammy|nobel|hall_of_fame|mvp|cup|championship`; `politics_governance` = `election|vote|poll|policy|legislation|congress|senate|president|gov|resignation|appointment`; `culture_media` = `film|movie|music|tv|show|book|art|celebrity|media|streaming`. This is a categorical label dim, not a count-of-contracts dim, so the condition_id-vs-token_id convention does not apply (the parquet carries `n_markets` = distinct condition_id alongside `n_contracts` for reference).

**Concept.** Partitions every traded contract into one of 7 information-source classes derived from the LLM-assigned `event_info_type`. Learnability hypothesis (writeup): high-frequency, objectively-resolved feeds (`market_data`, `weather_data`) should be well-calibrated, while sparse/judgment-laden classes (`politics_governance`, `awards`) should carry strong FLB.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| sports_data | 7.89M | -0.0063 (t-0.3) | -0.0331 (t-0.6) | +0.0203 (t+6.3***) | -0.0009 (t-0.1) |
| politics_governance | 5.72M | +0.0456 (t+8.1***) | +0.0581 (t+5.6***) | +0.0550 (t+5.7***) | +0.0476 (t+7.4***) |
| other | 5.71M | +0.0443 (t+10.4***) | +0.0579 (t+10.3***) | +0.0317 (t+3.8***) | +0.0349 (t+7.7***) |
| market_data | 2.40M | +0.0000 (t+0.0) | +0.0324 (t+3.3**) | -0.0084 (t-0.5) | +0.0026 (t+0.2) |
| culture_media | 1.45M | -0.0132 (t-1.3) | -0.0164 (t-1.3) | +0.0300 (t+3.4**) | -0.0027 (t-0.4) |
| weather_data | 673K | -0.0140 (t-2.4) | -0.0191 (t-1.1) | +0.0120 (t+2.4) | -0.0017 (t-0.4) |
| awards | 236K | +0.0041 (t+0.1) | +0.0743 (t+3.1**) | +0.0537 (t+4.8***) | +0.0275 (t+1.4) |

**Assessment.**
- **Hypothesis partially borne out, no clean monotone gradient.** The two judgment-heavy classes behave as predicted — `politics_governance` is the strongest FLB slice (mature +0.0456***, robust across all windows and both weightings) and `awards` shows strong FLB once it has volume (closing +0.0537***, dollar-weighted +0.0743**/+0.0814***). But `market_data` and `weather_data` are essentially flat-to-mildly-reversed on the count weighting (mature -0.014 to +0.000), so the "calibrated feeds" half holds, while `sports_data` is flat-but-reversed at the dollar weighting (mature -0.033) — not a smooth ordering across the 7 classes.
- **`other` is a top-3 slice and a serious confound.** The catch-all residual (5.71M mature trades, 3rd largest) posts the single most significant mature spread (+0.0443, t+10.4***) — so a large chunk of the "FLB signal" in this dimension lives in an uninterpretable junk bucket fed by null/unmatched `event_info_type`, not in any named source class. It tracks `politics_governance` almost exactly, suggesting much of it is mislabeled politics/long-tail event contracts.
- **Strength: clean weighting/window robustness for the political slices.** `politics_governance` is significant and same-signed in all 3 windows × both weightings (mature/closing/full, cnt+$), the most stable result in the table; `awards` and `politics_governance` both strengthen on the dollar weighting, consistent with real money concentrating the bias rather than a small-trade artifact.
- **No SE-degeneracy.** Every slice clears the 5k-trade floor in all three windows (smallest is `awards`, 236K mature / 1,845 markets), all |t| are modest (max ≈10.4), and even `awards` spans thousands of contracts — no single-event-family blow-up.
- **Weaknesses: LLM-label dependence, ordered-regex fragility, and category redundancy.** Every assignment rests on the LLM's `event_info_type` string plus a hand-written first-match regex (e.g. `championship`/`cup` route to `awards`, so sports cups can be miscategorized; `gov` is a loose substring). The dimension overlaps heavily with `dim_primary_category` (sports/politics/crypto) — it may be largely a re-skin of category with an added residual bucket.
- **Concrete change:** split or drop the `other` bucket (it is both large and the dominant FLB carrier) — either expand `INFO_TYPE_PATTERNS` to absorb its mass into named classes, or report it separately and exclude it from any "data-source learnability gradient" claim, so the gradient is read only across the 6 genuine classes.

## 3. `dim_primary_category` — Polymarket 13-category taxonomy

**Derivation.** Per-contract metadata, no trades scan. `analysis/learnability/dimensions.py -> add_dim_primary_category` reads one column, `categories` (a list per contract from `stage2_per_contract_augmented.parquet`), and applies `_first_cat`: returns `str(lst[0])` — the FIRST element of the category list — or the literal `"Uncategorized"` when the list is `None`, empty (`len == 0`), or not list-like (`TypeError`). No regex, no binning, no quantiles; the slice label is just the LLM/Gamma-assigned top category verbatim (Sports, Politics, Crypto, Tech, Finance, Geopolitics, Esports, Culture, Iran, Weather, Mentions, Economy, plus the `Uncategorized` residual). This is a categorical label, NOT a count-of-contracts dimension, so the condition_id-vs-token_id (markets-unit) convention does not apply here.

**Concept.** Partitions every BUY trade by the contract's primary subject category, then runs the standard 10-decile FLB calibration within each category. Writeup hypothesis: high-information / repeatedly-priced categories (Sports, Crypto, Esports) should be well-calibrated, while thin or one-off categories — explicitly Politics, Geopolitics, Finance — should show strong classic FLB.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Sports | 6.9M | -0.0064 (t-0.3) | -0.0343 (t-0.6) | +0.0217 (t+6.4***) | -0.0002 (t-0.0) |
| Politics | 6.8M | +0.0438 (t+8.7***) | +0.0599 (t+7.1***) | +0.0518 (t+6.5***) | +0.0454 (t+8.2***) |
| Crypto | 2.7M | +0.0030 (t+0.2) | +0.0337 (t+3.7***) | -0.0017 (t-0.1) | +0.0063 (t+0.5) |
| Tech | 1.7M | +0.0128 (t+1.5) | +0.0236 (t+2.5*) | +0.0359 (t+4.4***) | +0.0078 (t+1.1) |
| Finance | 1.1M | +0.0511 (t+12.1***) | +0.0623 (t+10.3***) | +0.0563 (t+9.1***) | +0.0483 (t+10.8***) |
| Geopolitics | 1.0M | +0.0510 (t+8.8***) | +0.0712 (t+13.1***) | +0.0462 (t+5.6***) | +0.0443 (t+7.6***) |
| Esports | 994K | -0.0080 (t-0.6) | -0.0142 (t-0.6) | +0.0132 (t+1.5) | -0.0068 (t-0.5) |
| Culture | 896K | +0.0443 (t+6.5***) | +0.0577 (t+7.5***) | +0.0445 (t+6.2***) | +0.0329 (t+6.1***) |
| Iran | 851K | +0.0084 (t+0.3) | +0.0244 (t+0.8) | -0.0441 (t-1.2) | -0.0037 (t-0.2) |
| Weather | 743K | -0.0130 (t-2.0) | -0.0106 (t-0.7) | +0.0107 (t+2.2*) | -0.0014 (t-0.3) |
| Mentions | 311K | +0.0219 (t+2.4*) | +0.0573 (t+6.1***) | +0.0153 (t+1.9) | +0.0079 (t+1.0) |
| Economy | 138K | +0.0217 (t+1.4) | -0.0015 (t-0.0) | +0.0306 (t+2.3*) | +0.0239 (t+2.1*) |
| Uncategorized | — | — | — | — | +0.0947 (t+16.9***) |

**Assessment.**
- Hypothesis broadly borne out but as a CLUSTER, not a clean gradient: the strong-FLB block (Finance +0.0511, Geopolitics +0.0510, Politics +0.0438, Culture +0.0443, all t>6 ***) is well separated from the calibrated/null block (Sports -0.0064, Crypto +0.0030, Esports -0.0080, all |t|<1). There is no monotone ordering — it is two regimes (thin one-off geopolitics/finance/politics vs. liquid recurring sports/crypto/esports), exactly the learnability story.
- Sports and Esports show a flat ~0 mature spread but a positive *closing* spread (Sports +0.0217 t+6.4***); the writeup's "Offset vs slope" note flags this as a level offset (mid-life buyers win, closing buyers lose), not true FLB slope — so their near-zero mature D10−D1 is the honest FLB read, and the closing positive is partly the offset, not learnable longshot mispricing.
- Dollar-weighting strengthens the bias where it exists: Crypto is null count-weighted (+0.0030, t+0.2) but significant dollar-weighted (+0.0337, t+3.7***), and Mentions jumps +0.0219→+0.0573 ($); big bets carry more FLB than the median trade. Researcher should not read Crypto/Mentions as "calibrated" without the dollar column.
- The `Uncategorized` residual is junk-bucket and confounded: it has NO real semantic content (it is the `_first_cat` fallback for contracts with an empty/None `categories` list), falls below the 5k floor in both mature and closing windows, and surfaces only in the full window with the single largest spread in the table (+0.0947 / +0.1057, t+16.9/+19.9***). That extreme value almost certainly reflects a non-random mix of unlabeled thin markets, not a learnability finding — treat it as a coverage artifact and consider dropping it.
- The entire dimension depends on a single LLM/Gamma-assigned label and specifically on the ARBITRARY choice of `lst[0]` (first list element) when a contract carries multiple categories — re-ordering or multi-label contracts could reshuffle slice membership. "Iran" and "Mentions" are also oddly narrow/ad-hoc categories sitting alongside broad ones (Sports, Politics), a taxonomy-granularity inconsistency.
- Heavy confound/redundancy with the structural dims: the strong-FLB categories (Finance, Geopolitics, Politics) are precisely the thin one-off markets captured by `dim_event_family_size : Singleton 1`, `dim_recurrence_class : One-off`, and `dim_outcomes_per_event : Few 2-5` (all headline ***), so category may be a proxy for novelty/family-size rather than an independent driver. None of these category slices appear in the SE-degenerate list, so the t-stats themselves are valid; the concern is interpretive, not statistical.
- One concrete change: drop or quarantine `Uncategorized`, and re-run with a multi-label / category-controlled spec (e.g. interact category with `dim_event_family_size`) to test whether the Finance/Geopolitics/Politics FLB survives conditioning on novelty — i.e. whether category adds signal beyond the structural one-off dimensions.

## 4. `dim_subject_specificity` — How many entities the event resolves on

**Derivation.** Per-contract metadata, no trades scan: reads the single list column `event_subjects` (a `list<string>` in `stage2_per_contract_augmented.parquet`), takes its length via the `_list_len` helper (returns 0 if None/non-iterable), then `pd.cut` with bins `[-0.5, 1.5, 2.5, np.inf]` → labels `"1 subject"`, `"2 subjects"`, `"3+ subjects"` (analysis/learnability/dimensions.py → `add_dim_subject_specificity`). This is a count-of-*entities-in-a-list*, not a count-of-contracts, so the condition_id-vs-token_id markets convention does not apply to the bin value (each augmented row is already one contract; binning counts subjects, not markets). The value is LLM-assigned — `event_subjects` is an extracted field, so the count inherits any extraction noise.

**Concept.** Partitions every contract by how many distinct subjects/entities its event resolves on (one team vs. a head-to-head pair vs. a 3+-way field). Writeup hypothesis (learnability_writeup.md L329): more subjects = compound proposition = harder to price = stronger FLB, so the spread should rise monotonically 1 → 2 → 3+.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| 1 subject | 15,308,552 | +0.0074 (t+0.6) | +0.0248 (t+1.2) | +0.0233 (t+4.6***) | +0.0134 (t+1.6) |
| 2 subjects | 5,972,451 | +0.0272 (t+4.8***) | +0.0399 (t+4.9***) | +0.0275 (t+3.5***) | +0.0216 (t+4.5***) |
| 3+ subjects | 2,800,131 | +0.0431 (t+7.8***) | +0.0590 (t+8.4***) | +0.0315 (t+3.2**) | +0.0362 (t+6.7***) |

**Assessment.**
- Hypothesis borne out cleanly in the mature (25-80%) window: a strictly monotone gradient 1→2→3+ in both weightings (cnt +0.0074 → +0.0272 → +0.0431; $ +0.0248 → +0.0399 → +0.0590), with significance strengthening as subject count rises (t+0.6 → +4.8*** → +7.8***). One of the better-behaved dimensions.
- Robust to weighting: dollar-weighted spreads are even larger and stay monotone, so the effect isn't a small-ticket artifact. Gradient also survives in the full 0-100% window (cnt +0.0134 → +0.0216 → +0.0362).
- No SE-degeneracy: every slice spans many markets (mature n_markets 125,083 / 31,774 / 10,860), so the t-stats reflect breadth, not one event family. No junk residual bucket — only three clean ordinal bins, no "unknown"/"other".
- The "1 subject" slice is the weak/insignificant end (mature t+0.6, full t+1.6) — i.e. single-entity markets are close to efficient and FLB is concentrated in multi-subject (compound) contracts. The monotonicity collapses only in the closing 80-100% window, where ordering inverts to 1<2≈3+ on a count basis and 3+ goes flat dollar-weighted (spread_dol +0.0134, t+0.4) — consistent with information arriving and compound markets resolving as the event nears.
- Confounds/redundancy: subject count correlates mechanically with `dim_outcomes_per_event` and `dim_market_type` (multi-subject events tend to be multi-outcome) and with category (sports head-to-heads, multi-candidate politics), so part of this gradient may be the category/structure effect re-expressed. Depends entirely on the LLM-extracted `event_subjects` list being well-calibrated in length.
- Possible change: cross-tab `dim_subject_specificity` against `dim_primary_category` (or residualize on it) to confirm the subject-count gradient survives within-category rather than being a sports/politics composition artifact; optionally split the open-ended "3+" bin (e.g. 3-5 vs 6+) since it currently absorbs the entire long right tail.

## 5. `dim_event_family_size` — Markets sharing the event_template

**Derivation.** Per-contract metadata (no trades scan), from `analysis/learnability/dimensions.py` -> `add_dim_event_family_size`. Groups the augmented per-contract DataFrame by `event_template` and counts DISTINCT `condition_id` via `groupby("event_template")["condition_id"].transform("nunique")` — confirming the markets-unit convention (YES/NO of a binary market share one `condition_id`, so token-counting is avoided). The raw count is binned with `pd.cut(counts, bins=[-0.5, 1.5, 20.5, 1000.5, np.inf], labels=["Singleton 1", "Small 2-20", "Medium 21-1K", "Large 1K+"])`.

**Concept.** Partitions each contract by how many sibling markets resolve under the same templated event (e.g. all "NBA — <TEAM> wins" or daily Fed contracts collapse to one family). **Hypothesis**: large families (NBA games, daily Fed contracts) -> repeated-play learning -> calibrated; singletons/small families -> FLB.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 392-457):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Small 2-20 | 9.2M | +0.0441 (t+11.1***) | +0.0580 (t+8.0***) | +0.0304 (t+3.6***) | +0.0368 (t+6.6***) |
| Large 1K+ | 6.6M | -0.0193 (t-2.1) | -0.0152 (t-1.6) | +0.0064 (t+1.6) | -0.0081 (t-1.7) |
| Medium 21-1K | 5.7M | -0.0025 (t-0.1) | -0.0009 (t-0.0) | +0.0331 (t+3.8***) | +0.0061 (t+0.5) |
| Singleton 1 | 2.6M | +0.0476 (t+9.9***) | +0.0567 (t+5.9***) | +0.0456 (t+8.2***) | +0.0428 (t+10.8***) |

**Assessment.**
- Hypothesis is NOT cleanly borne out and the gradient is non-monotone. The smallest families show the strongest classic FLB (Singleton 1 +0.0476 t+9.9***; Small 2-20 +0.0441 t+11.1***), and the largest families (Large 1K+) do go mildly reversed in mature (-0.0193, t-2.1, not significant) — directionally consistent with "repeated play calibrates" at the two ends, but the middle bucket breaks the ordering.
- Non-monotonicity at the middle: Medium 21-1K is flat in mature (-0.0025, t-0.1) yet posts a strongly positive closing-window spread (+0.0331, t+3.8***). So the four-bucket order (Singleton high -> Small high -> Medium flat -> Large negative) is not a clean ladder, and the dimension does not isolate a single learnability axis.
- The resurrected endpoint bucket **Singleton 1** (the whole point of the markets-unit fix) is real and well-powered: 2.6M mature trades, t+9.9 to +10.8 across windows, robust in both count- and dollar-weighting. It is the strongest-FLB family-size slice — NOT SE-degenerate and not below the 5k floor.
- No SE-degeneracy flags anywhere: all four slices sit on millions of mature trades (2.6M–9.2M N), so the large |t| values come from genuine sample size, not from a handful of event-family clusters. No slice shows "—".
- Category / volume confound: family size is highly correlated with category and liquidity (large recurring families ≈ scheduled sports/crypto contracts; singletons ≈ one-off politics/awards). The Large 1K+ reversal likely re-encodes the high-volume sports/crypto "public money" reversal already captured by `dim_dollar_volume_tier` and `dim_recurrence_class`, so this dim is partly redundant rather than measuring learning per se. It is also LLM-label dependent (event_template comes from the normalizer) and the cut edges (1.5 / 20.5 / 1000.5) are arbitrary.
- Concrete change: replace the hand-set 2/20/1K cut points with data-driven `qcut` quantiles (or a continuous log-family-size OLS slope) and condition on `dim_primary_category` to test whether the family-size effect survives the sports/crypto confound.

## 6. `dim_outcomes_per_event` — Distinct markets per event_slug

**Derivation.** Per-contract metadata dimension (no trades scan), computed in `analysis/learnability/dimensions.py` -> `add_dim_outcomes_per_event`. It reads two columns from the augmented per-contract frame, `event_slug` and `condition_id`, and counts DISTINCT `condition_id` per event via `df.groupby("event_slug")["condition_id"].transform("nunique")` — confirming the markets-unit convention (NOT `token_id`); the code comment is explicit that YES/NO of a binary share one `condition_id`, so a standalone binary = 1 market, not 2 tokens. The raw count is stored as `dim_outcomes_per_event_raw` and binned with `pd.cut(distinct_cond, bins=[-0.5, 1.5, 5.5, np.inf], labels=["Binary 1","Few 2-5","Many 6+"])`, i.e. {1}->Binary 1, {2..5}->Few 2-5, {>=6}->Many 6+. It is a member of `PER_CONTRACT_DIMS`.

**Concept.** Partitions contracts by how many distinct markets resolve under the same event_slug — a structural proxy for the outcome cardinality of the event. Writeup hypothesis (l.460): "binary events easiest to calibrate; multi-outcome hardest" — so the expected order of FLB severity is Binary 1 < Few 2-5 < Many 6+ if cardinality drives mispricing.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup l.465-469):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Many 6+ | 14.8M | +0.0027 (t+0.3) | +0.0186 (t+0.9) | +0.0240 (t+4.1***) | +0.0068 (t+1.0) |
| Few 2-5 | 4.1M | +0.0474 (t+14.9***) | +0.0636 (t+13.0***) | +0.0247 (t+5.5***) | +0.0380 (t+5.6***) |
| Binary 1 | 3.0M | +0.0239 (t+2.5) | +0.0386 (t+3.6***) | -0.0070 (t-0.5) | +0.0157 (t+1.9) |

**Assessment.**
- **Hypothesis NOT borne out / non-monotone.** Predicted order is Binary < Few < Many, but observed mature-cnt FLB is Many 6+ (+0.0027, ns) < Binary 1 (+0.0239) < Few 2-5 (+0.0474***). The largest, most-multi-outcome bucket shows essentially NO net count-weighted FLB and only marginal dollar-weighted; the middle bucket is strongest. No clean cardinality gradient.
- **Strength: Few 2-5 is robust.** Large N (4.1M) and significant in every window and weighting (mature +0.0474***/+0.0636***, closing +0.0247***, full +0.0380***), positive (classic FLB) throughout. t-stats are large but nowhere near SE-degenerate territory (no |t|>50 on a tiny count); the slice spans many event families, so it is not a single-cluster artifact.
- **Weakness: Many 6+ is a category/volume confound, not cardinality.** It is the biggest bucket (14.8M, larger than Few + Binary combined) yet posts near-zero mature spread — consistent with multi-outcome fields being heavily crypto/sports, where high-volume markets show reversed FLB (project memory), so the flat number is a wash of opposing category effects rather than a calibration signal. Only its closing window is significant (+0.0240***), i.e. any bias concentrates at resolution, not during mature trading.
- **Binary 1 flips sign across the lifecycle:** mature +0.0239 (count-weighted ns) vs closing -0.0070 (ns) — binaries de-bias or mildly reverse near resolution, so even the conceptually "easiest" slice is window-sensitive and only its dollar-weighted mature/full cells clear significance.
- **Coarse/arbitrary edges + redundancy.** The 5.5 cut lumps a 6-outcome event with a 50-outcome event in Many 6+, likely masking any true high-cardinality effect; and counting `condition_id` per event_slug overlaps heavily with `dim_event_family_size` (condition_id per event_template), risking redundancy. The markets-unit fix itself is correct and load-bearing — under the old token-count, binaries would have leaked into Few 2-5 and contaminated that (currently cleanest) bucket.
- **Concrete change:** split Many 6+ into a finer high-cardinality bin (e.g. 6-15 vs 16+) and cross-tab against category to strip the crypto/sports confound; if the Binary<Few<Many gradient still fails to appear, retire the cardinality->harder-calibration hypothesis in favor of the category/volume story the Many-6+ wash already points to.

## 7. `dim_market_specificity` — Does the market narrow its parent event

**Derivation.** Per-contract metadata, no trades scan: reads two `list<string>` columns from `stage2_per_contract_augmented.parquet` — `market_subjects` and `event_subjects` — takes each list's length via the `_list_len` helper (0 if None/non-iterable), forms `diff = len(market_subjects) − len(event_subjects)`, then `np.where`: `diff > 0` → `"Market narrower"`, `diff == 0` → `"Market = Event"`, else → `"Market broader/equal"` (analysis/learnability/dimensions.py → `add_dim_market_specificity`). This is a per-row comparison of two LLM-extracted subject-list lengths, NOT a count-of-contracts dimension — there is no `condition_id`/`token_id` grouping, so the markets-unit convention does not apply here (each augmented row is already one contract; the label depends only on its own two lists).

**Concept.** Partitions each contract by whether the specific market mentions more subjects than its parent event (a narrowing/specific leg), the same set (market = event), or fewer (broader/equal residual). Writeup hypothesis (learnability_writeup.md L525): narrow markets carved out of a broad event price their longshot legs poorly → stronger FLB on the narrower slices.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Market = Event | 13.9M | +0.0154 (t+1.8) | +0.0115 (t+0.4) | +0.0138 (t+2.7*) | +0.0128 (t+2.5) |
| Market narrower | 10.0M | +0.0234 (t+2.3) | +0.0528 (t+6.9***) | +0.0497 (t+10.5***) | +0.0284 (t+3.5**) |
| Market broader/equal | 139K | +0.0376 (t+3.2**) | +0.0572 (t+3.8***) | +0.0089 (t+0.6) | +0.0032 (t+0.2) |

(Source: writeup v6 table L530-534, transcribed from the v6 `*_spread_summary.parquet`. The parquets named at `/tmp/v6_results/` are NOT present locally — that dir is empty and no `phase1_v6_*spread_summary.parquet` exists anywhere on this Mac; the v6 data lives on EC2 at `/mnt/data/learnability/output`. The writeup is the v6 document (L1) generated from those same rows, so these are the authoritative values; t-stats shown are the writeup's, not independently recomputed from parquet.)

**Assessment.**
- Hypothesis directionally supported but NOT a clean monotone gradient. On count-weighting the ordering is "Market = Event" (+0.0154) < "Market narrower" (+0.0234) < "Market broader/equal" (+0.0376), so the supposedly-most-efficient bucket ("broader/equal", which should be the *least* narrowing) actually posts the *largest* count spread — backwards from the stated story. The narrowing effect shows up far more convincingly dollar-weighted and in the closing window for "Market narrower" (closing cnt +0.0497 t+10.5***; mature $ +0.0528 t+6.9***).
- "Market narrower" is the strongest, most robust slice: significant and positive in every cell except mature count (+0.0234 t+2.3, just under 2.58), with the dollar-weighted spreads roughly 2× the count-weighted ones — consistent with larger tickets on narrow longshot legs driving the bias. This is the slice the hypothesis predicts, and it behaves.
- "Market = Event" (the largest slice, 13.9M) is essentially the efficient baseline: small positive spreads, only marginally significant (mature t+1.8, closing t+2.7*), and dollar-weighting kills it (mature $ +0.0115 t+0.4). Reasonable as a reference bucket.
- Weaknesses/confounds: (a) the "Market broader/equal" residual is a junk catch-all — it folds `diff == 0`-fails plus genuinely broader markets, is tiny (139K mature trades, <1% of the dim), and is internally incoherent: strongly significant mature (cnt +0.0376 t+3.2**, $ +0.0572 t+3.8***) but flat-to-negative closing/full ($ closing −0.0297, $ full −0.0273), i.e. its sign flips by window — not a stable slice. (b) Both labels rest entirely on LLM-extracted `market_subjects`/`event_subjects` list lengths, so a subject mis-count flips the slice; the threshold is the raw sign of a length diff, which is brittle when either list is empty (both → 0 → "Market = Event"). (c) Heavy redundancy with `dim_subject_specificity` (same `event_subjects` length) and likely category composition (narrow-market families are sports/politics props).
- No SE-degeneracy: max |t| here is +11.7 (mature $ for "Market narrower"), all slices span many markets; nothing in the |t|>50 / single-event-family regime — treat the signal as real, not a clustering artifact.
- One concrete change: replace the three-way sign label with a cleaner two-way "Market narrower (diff>0)" vs "Market = Event (diff==0)" and DROP the tiny incoherent "broader/equal" bucket (or merge it into "Market = Event"), since broader-than-event is near-impossible by construction and its window-flipping sign suggests it is mostly noise/extraction error; alternatively residualize on `dim_primary_category` to confirm the "Market narrower" spread survives within-category rather than being a sports/politics composition effect shared with `dim_subject_specificity`.

## 8. `dim_dollar_volume_tier` — Quartiles of per-token dollar volume

**Derivation.** Computed from a trades-scan, not contract metadata: `compute_contract_aggregates` (`analysis/learnability/dimensions_from_trades.py`) does one `GROUP BY conditionId` over the `trades_buy` view, summing `usdcSize` into `dollar_volume` per token (here `conditionId` is the 77-digit per-outcome `token_id`, so this is per-TOKEN, NOT per-`condition_id` — YES/NO legs of a binary are tiered independently). `add_dim_dollar_volume_tier` takes contracts with `dollar_volume > 0`, computes the 25/50/75 quantiles, and labels via hard cutoffs: `Q1 (≤$q1)`, `Q2 ($q1-$q2)`, `Q3 ($q2-$q3)`, `Q4 (>$q3)` (zero-volume → `"Zero"`). At v6 runtime the edges were q1=$32, q2=$255, q3=$2,007 (slice labels confirm). This is a continuous trades-derived measure binned to 4 buckets, not a count-of-contracts dim.

**Concept.** Partitions contract-outcomes by lifetime buy-side dollar volume into 4 quartiles. Learnability hypothesis (writeup §8): high-volume contracts attract pros + arbitrage → calibrated; thin contracts are retail-only → stronger FLB. So FLB should be largest in Q1/Q2 and shrink toward Q4.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Q4 (>$2,007) | 22.0M | +0.0137 (t+1.7) | +0.0316 (t+2.3) | +0.0210 (t+4.7***) | +0.0130 (t+2.3) |
| Q3 ($255-$2,007) | 1.5M | +0.0162 (t+4.5***) | +0.0381 (t+13.3***) | +0.0036 (t+1.0) | +0.0121 (t+4.8***) |
| Q2 ($32-$255) | 449K | +0.0236 (t+4.8***) | +0.0350 (t+6.1***) | +0.0159 (t+4.4***) | +0.0143 (t+2.8*) |
| Q1 (≤$32) | 81K | -0.1891 (t-1.4) | -0.2729 (t-1.5) | -0.0155 (t-1.6) | -0.0524 (t-3.0**) |

(Dollar-weighted: closing Q4 +0.0327 (t+4.0***), full Q4 +0.0252 (t+2.9*); full Q1 -0.0431 (t-2.1). Spread = D10−D1 of BUY-side calibration error. All four slices clear the 5k floor in every window.)

**Assessment.**
- Hypothesis NOT borne out — no monotone FLB→0 gradient. The thinnest tier (Q1) shows REVERSED, not stronger, FLB: mature count spread −0.1891 and dollar −0.2729, plus a significant full-window −0.0524 (t−3.0**). FLB is mildest at the bottom-but-one and roughly flat (+0.012 to +0.024 count) across Q2/Q3/Q4. The data point the opposite direction from "thin → more FLB."
- Q1's large negative spread is unreliable: mature t-stats are insignificant (−1.4 / −1.5) despite a −0.19 to −0.27 point estimate, i.e. huge magnitude with no power. Q1 is the smallest cell (81K trades) and spans contracts with ≤$32 lifetime volume — D1 (longshot) win rates there are dominated by a handful of resolved-NO tokens, so the spread is driven by sparse, noisy tails rather than a real reversal.
- Strength: Q3/Q2 mature results are clean and strongly significant (+0.0162 t+4.5***, +0.0236 t+4.8***) and dollar-weighting amplifies them (Q3 $ +0.0381 t+13.3***), consistent with mid-volume retail FLB. Dimension is fully populated and trades-derived (no LLM label, no junk "unknown"/"other" bucket).
- Weakness — thresholds are data-dependent quantiles recomputed at runtime (not stable across reruns), and the bins are coarse: the bottom edge ($32) is so low that Q1 is essentially "barely-traded contracts," conflating thinness with non-resolution / abandoned markets rather than genuine retail mispricing.
- Confound/redundancy: dollar volume is mechanically correlated with category (crypto/sports dominate high-volume tokens) and with trade count, overlapping `dim_n_trades`-style dims and any category split; the apparent Q4 closing FLB (+0.0210***) may be the high-volume sports/crypto public-money channel seen elsewhere rather than a volume effect per se.
- Concrete change: replace runtime quantiles with fixed, interpretable dollar cutoffs (e.g. <$100 / $100–$1k / $1k–$10k / >$10k) for reproducibility, and add a minimum-resolution / minimum-trade gate to Q1 so its spread reflects priced markets rather than near-dead tokens — or drop Q1 from headline reporting given its t≈−1.5 mature instability.

## 9. `dim_contract_horizon` — First->last trade span

**Derivation.** Trades-scan dimension (NOT per-contract metadata): `compute_contract_aggregates` does one DuckDB scan over `trades_buy` grouped by `conditionId` (the per-outcome 77-digit `token_id`) to get `MIN(timestamp) AS first_ts` and `MAX(timestamp) AS last_ts`, merged onto the per-contract frame by `token_id`. `add_dim_contract_horizon` (analysis/learnability/dimensions_from_trades.py) then sets `span_sec = last_ts - first_ts` (NaN→0) and bins via `pd.cut` with edges `[-1, 3600, 86400, 604800, 2592000, inf]` (HOUR / DAY / 7·DAY / 30·DAY) into labels `["<1h", "1h-1d", "1d-1w", "1wk-1mo", ">1mo"]`. This is NOT a count-of-contracts dim — the span is computed at the `token_id` grain, so the two outcomes of a binary can land in different horizon bins; the condition_id-vs-token_id convention does not apply (it's used by the sibling `dim_recurrence_class`, which counts `condition_id` for family size).

**Concept.** Partitions every contract by how long its trading window was, from first to last BUY. Writeup hypothesis: medium-horizon contracts are best calibrated, while both ultra-short and ultra-long horizons are worse — an inverted-U in calibration error.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| >1mo | 13.4M | +0.0272 (t+2.86*) | +0.0385 (t+2.34) | +0.0456 (t+6.06***) | +0.0294 (t+4.15***) |
| 1wk-1mo | 4.8M | -0.0025 (t-0.28) | +0.0158 (t+1.82) | +0.0158 (t+2.81*) | +0.0003 (t+0.06) |
| 1d-1w | 4.0M | +0.0047 (t+0.80) | +0.0080 (t+0.85) | +0.0088 (t+2.40) | +0.0044 (t+1.19) |
| 1h-1d | 1.8M | -0.0348 (t-4.80***) | -0.0355 (t-2.62*) | -0.0012 (t-0.29) | -0.0098 (t-2.17) |
| <1h | 102K | -0.0437 (t-1.18) | -0.1736 (t-2.64*) | +0.0151 (t+0.94) | -0.0225 (t-1.07) |

**Assessment.**
- **Hypothesis partially borne out, but not as an inverted-U.** In the mature window count-weighted the gradient is monotone in the WRONG direction for the short end: `<1h` -0.0437, `1h-1d` -0.0348 (reversed FLB), rising to `1d-1w` +0.0047, `1wk-1mo` -0.0025 (~flat), `>1mo` +0.0272 (classic FLB). So long-horizon = strongest classic FLB and short-horizon = reversed; the predicted "ultra-long is also worse" tail does not appear — `>1mo` is the most FLB-positive, not miscalibrated symmetrically. The "medium best calibrated" claim only holds in the weak sense that `1d-1w`/`1wk-1mo` sit nearest zero.
- **`>1mo` is the only robustly-signed slice across all windows/weightings** (mature/closing/full all positive, t up to +6.06***), and it dominates trade count (13.4M of ~22M mature trades), so it largely drives the pooled FLB sign. `1h-1d` is the only other slice with consistent (negative) sign and mature significance (t-4.80***).
- **`<1h` dollar-weighting is SE-degenerate / outlier-driven — do not trust it.** Dollar spread swings to -0.1736 (mature), -0.3406 (closing), -0.3270 (full) while count-weighted stays small (-0.04 to +0.02). A 4–8× gap between $ and cnt on a 102K-trade slice means a handful of large-dollar contracts dominate the dollar calibration; treat the big negative $ numbers as noise, not signal.
- **Span is mechanically confounded with volume and category.** Contracts that trade for >1mo are almost tautologically the high-liquidity, long-dated political/macro markets, while `<1h`/`1h-1d` are dominated by hourly crypto/sports up-down-style contracts — so this dim overlaps heavily with `dim_dollar_volume_tier` (same file, same `compute_contract_aggregates` scan) and with category. The horizon gradient may be re-expressing the volume/category gradient rather than an independent "learnability" effect.
- **Token-grain span is slightly leaky:** because the span is per-`token_id`, a YES and NO leg of one market can fall in adjacent bins if their first/last fills differ, mildly blurring bin boundaries (small effect, but worth noting vs. a condition_id-grain definition).
- **Concrete change to consider:** re-cut at `condition_id` grain (market-level span) and add a horizon×volume_tier or horizon×category 2-way to test whether the long-horizon FLB survives controlling for liquidity/category; and either drop or robustify the `<1h` dollar-weighted cell (e.g. winsorize contract dollar volume) so its degenerate outlier doesn't leak into any pooled dollar-weighted comparison.

## 10. `dim_recurrence_class` — Family size x time span heuristic

**Derivation.** `analysis/learnability/dimensions_from_trades.py` -> `add_dim_recurrence_class`. Trades-scan aggregate: groups the per-contract frame by `event_template` and computes `fam_first=min(first_ts)`, `fam_last=max(last_ts)`, and `fam_size = condition_id.nunique()` — markets per family, NOT tokens, so a standalone binary collapses to fam_size 1 = "One-off" (confirms the condition_id markets-unit-fix convention; `first_ts`/`last_ts` are trades-derived via `compute_contract_aggregates`'s scan over `trades_buy`). `fam_span_days = (fam_last - fam_first)/86400`. Verbatim labeling on (`fam_size`, `fam_span_days`): `One-off` if size ≤ 1; else `Daily` if size ≥ 100 AND span ≥ 60 AND size/max(span,1) ≥ 0.5; else `Recurring` if size ≥ 10 AND span ≥ 30; else `Episodic`.

**Concept.** Partitions every trade by the recurrence cadence of its event family — a proxy for how repeatable/learnable the contract type is (Daily = high-cadence broad-span series; Recurring = medium series; Episodic = small short-lived clusters; One-off = singleton). Writeup hypothesis (line 723): "Daily -> strongest learning -> calibrated; Episodic -> strongest FLB."

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 721-786):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Recurring | 9.1M | +0.0133 (+0.9) | +0.0297 (+1.2) | +0.0483 (+5.4***) | +0.0194 (+1.9) |
| Daily | 7.4M | -0.0168 (-2.2) | -0.0165 (-1.9) | +0.0059 (+1.6) | -0.0076 (-1.8) |
| Episodic | 5.0M | +0.0374 (+6.3***) | +0.0458 (+4.9***) | +0.0151 (+1.7) | +0.0303 (+3.9***) |
| One-off | 2.6M | +0.0476 (+9.9***) | +0.0567 (+5.9***) | +0.0456 (+8.2***) | +0.0428 (+10.8***) |

**Assessment.**
- Hypothesis partially borne out via a clean rank order: mature-cnt FLB runs monotonically One-off +0.0476*** > Episodic +0.0374*** > Recurring +0.0133 (ns) > Daily -0.0168, so "more recurrence -> less FLB" holds along the ordering.
- The Daily "calibrated" prediction overshoots into mild reversal, not zero: mature cnt -0.0168 (t-2.2) and full $ -0.0160 (t-3.2**) are significantly negative — favorites overpriced relative to longshots, consistent with public-money / high-cadence crypto-sports rather than clean calibration.
- One-off is the standout and the payoff of the markets-unit fix: resurrected by counting condition_id (standalone binaries land here), it is significant and stable across every window/weighting (+0.04 to +0.06, t 5.6-10.8***) and would have been invisible under a token-count definition.
- The closing window (80-100%) reshuffles the gradient: Recurring jumps to +0.0483*** while Episodic fades to +0.0151 (ns), so the monotone story is window-dependent — the mature and full windows are cleaner than closing.
- Confounds/weaknesses: strong category confound (Daily ≈ crypto/sports cadence, One-off ≈ politics/novelty) means this dim may largely re-express `dim_category`; the Daily rule's three magic constants (100/60/0.5) and the Recurring 10/30 cutoffs are arbitrary with no sensitivity reported; "Episodic" is the residual `else` bucket (catch-all heterogeneity); and labels inherit any `event_template` normalization errors from the LLM stage0/stage2 pipeline. Heavy overlap with the family-count and `add_dim_contract_horizon` dims in the same file.
- No SE-degeneracy concern: every slice carries millions of trades (2.6M-9.1M N), so the large t-stats are not one-family artifacts.
- One concrete change: report a category-stratified (or category-residualized) version to test whether One-off > Episodic > Recurring > Daily survives controlling for crypto/sports share, and publish a threshold sweep on the (100,60,0.5)/(10,30) cutoffs.

## 11. `dim_group_strict_size` — Family size on event_slug x market_template

**Derivation.** `dimensions_v4_addons.py -> add_dim_group_strict`. Builds `dim_group_strict = event_slug + "|" + market_template` (the LLM-assigned market-shape template), then attaches per-contract metadata: `dim_group_strict_count = df.groupby("dim_group_strict")["condition_id"].transform("nunique")` — confirmed markets-unit, it counts DISTINCT `condition_id` (markets), not `token_id` (the YES/NO of a binary share one condition_id, counted once). The size label is a literal `pd.cut(counts, bins=[-0.5, 1.5, 20.5, 1000.5, np.inf], labels=["Singleton 1","Small 2-20","Medium 21-1K","Large 1K+"])`. No trades-scan / qcut / searchsorted / OLS in this function — pure per-contract metadata attached before the FLB SQL slices on the bucket label; the raw `dim_group_strict` ID is used only for prior-settlement counting.

**Concept.** Partitions trades by how many distinct markets share the same event AND the same per-market template shape — the tightest of the grouping definitions, with the highest singleton share. Hypothesis (writeup §11, verbatim "as event_family_size, tighter grouping"): bigger families are more repeated/learnable, so FLB should weaken as family size grows and concentrate in one-off (singleton) contracts.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 787-851):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Singleton 1 | 17.8M | +0.0228 (t+2.7*) | +0.0336 (t+1.9) | +0.0308 (t+7.6***) | +0.0232 (t+3.9***) |
| Small 2-20 | 5.0M | +0.0138 (t+1.2) | +0.0322 (t+3.3**) | +0.0146 (t+1.6) | +0.0108 (t+1.2) |
| Medium 21-1K | 1.3M | -0.0041 (t-0.3) | +0.0240 (t+2.0) | +0.0115 (t+0.7) | +0.0012 (t+0.1) |

Note: the code emits a 4th bucket, `Large 1K+`, but it is entirely absent from the writeup table (no row at lines 796-799) — below the 5,000-trade floor or empty after the markets-unit fix collapsed token-level counts.

**Assessment.**
- Hypothesis directionally borne out on the count-weighted mature column: spread declines monotonically with family size — Singleton +0.0228 (t+2.7*) → Small +0.0138 (t+1.2, n.s.) → Medium -0.0041 (t-0.3, n.s.). The classic FLB concentrates in one-off contracts; larger families show essentially none.
- Gradient is fragile to weighting. Dollar-weighted mature does NOT decline: Singleton +0.0336 (t+1.9), Small +0.0322 (t+3.3**), Medium +0.0240 (t+2.0) — all positive and similar magnitude, so the "learnability" signal lives in count-weighting, not capital at risk. Only Small/$ clears |t|>2.58.
- The markets-unit fix is load-bearing: Singleton 1 is the dominant resurrected bucket (17.8M) and the only slice significant on count-weighting (closing cnt +0.0308 t+7.6***; full cnt +0.0232 t+3.9***). Reporting it (per the fix's whole point) is what makes the gradient visible; counting condition_id not token_id is what populates it.
- Confounds: (a) category — singletons skew toward one-off politics/news that carry classic FLB, so this may partly re-express dim_category rather than pure "learnability." (b) LLM-label dependence — `market_template` is an LLM artifact; mis-templating migrates contracts across size slices. (c) arbitrary bins (1 / 2-20 / 21-1K / 1K+); the 21-1K bucket spans ~two orders of magnitude and is thinnest (1.3M), with every count-weighted cell insignificant.
- Redundancy: near-identical construction to `dim_event_slug_size` (same bins, same condition_id nunique, only difference the `+ "|" + market_template` refinement). Worth cross-tabbing the two to confirm the template layer reshapes the gradient rather than duplicating it.
- No SE-degenerate slices among the three reported (all N in the millions, no |t|>50). The only data caveat is the missing `Large 1K+` bucket — confirm it is floor-suppressed vs. truly empty. Concrete change: collapse to two buckets (Singleton vs ≥2) to recover power, and always report the dollar column beside count so the count-only monotonicity isn't read as the whole story.

## 12. `dim_event_slug_size` — Family size on event_slug

**Derivation.** Per-contract metadata, computed from the v3 contract-dimensions parquet with no trades scan: `add_dim_event_slug_size` groups by `event_slug` and counts distinct `condition_id` via `df.groupby("event_slug")["condition_id"].transform("nunique")`, then bins with `pd.cut` at edges `[-0.5, 1.5, 20.5, 1000.5, np.inf]` → labels `["Singleton 1", "Small 2-20", "Medium 21-1K", "Large 1K+"]`. Markets-unit convention is confirmed: the size counts DISTINCT `condition_id` (markets), not `token_id` (the YES/NO of a binary share one `condition_id`), so a one-market binary event lands in "Singleton 1". File → function: `analysis/learnability/dimensions_v4_addons.py` → `add_dim_event_slug_size`. The docstring flags it as a "free bonus" — `event_slug` is already in the parquet, reusing the same bucketing as `dim_event_family_size` and `dim_group_strict_size`, just at the slug grain.

**Concept.** Partitions contracts by how many distinct markets share their Polymarket `event_slug` (the on-site event grouping): a one-off singleton vs a small/medium recurring family. Tests whether learnability/FLB depends on family size at the slug grain — the writeup hypothesis is simply "as event_family_size at event_slug grain," i.e. expect the same pattern as the template-based family-size dimension (more reps = more learnable structure → FLB should shrink).

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 852-916):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Small 2-20 | 14.0M | +0.0154 (t+1.6) | +0.0285 (t+1.2) | +0.0191 (t+3.5**) | +0.0158 (t+2.3) |
| Medium 21-1K | 4.9M | +0.0036 (t+0.2) | +0.0264 (t+2.1) | +0.0389 (t+5.8***) | +0.0105 (t+1.0) |
| Singleton 1 | 3.0M | +0.0239 (t+2.5) | +0.0386 (t+3.6***) | -0.0070 (t-0.5) | +0.0157 (t+1.9) |

Note: the code can produce a "Large 1K+" bucket (4th `pd.cut` label), but it is entirely absent from the writeup table — no slug family clears >1K distinct markets with enough trades to pass the 5,000-trade floor, so the top bin is effectively dead.

**Assessment.**
- Hypothesis NOT borne out — no monotone family-size gradient. In the mature window the count-weighted spread runs Singleton +0.0239 (t+2.5) → Small +0.0154 (t+1.6) → Medium +0.0036 (t+0.2): FLB is *strongest* in the smallest family and weakest in the largest tracked one, the opposite of the "bigger family → more learnable → smaller FLB" prediction. Dollar-weighted is essentially flat across slices (+0.0386 / +0.0285 / +0.0264).
- The two windows disagree about which bucket is biased. Singleton is the only significant mature slice (cnt +0.0239 (t+2.5), $ +0.0386 (t+3.6***)) yet goes mildly REVERSED at the close (-0.0070 (t-0.5)); Medium is null in mature (t+0.2) but the strongest closing slice (+0.0389 (t+5.8***)). Singleton FLB concentrates mid-life, recurring-family FLB at the close — different mechanisms, weakening the single-dimension story.
- Statistically modest overall: full-window spreads are all small (+0.0105 to +0.0158) with |t| ≤ 2.3, none crossing the ** bar. The "significant" cells are window-specific, not robust across windows.
- No SE-degeneracy concern: all three slices are mega-bins (3.0M–14.0M trades across many slug families), so the t-stats are real, not one-family |t|>50 artifacts.
- Confounds / redundancy: (a) heavy redundancy with `dim_event_family_size` and `dim_group_strict_size` — identical bin edges, near-identical grain; this is explicitly the "free reference" twin and adds little independent signal. (b) Category confound — large slug families are dominated by recurring crypto/sports series while singletons skew one-off politics/news, so any size effect is largely category in disguise. (c) Thresholds (1.5/20.5/1000.5) are inherited verbatim from the template dim, not tuned to the slug count distribution, and the top tier comes out empty.
- Concrete change: collapse to a binary "Singleton vs Recurring" contrast (the only place an effect-size difference appears), or re-derive cut points from the actual `event_slug` distinct-`condition_id` quantiles so all buckets are populated and "Large 1K+" isn't a dead label; then co-tabulate against category to check whether the weak, non-monotone size signal survives a category control.

## 13. `dim_family_vol_tier` — Family total dollar volume terciles

**Derivation.** Per-contract metadata, NOT a fresh trades scan: `analysis/learnability/dimensions_v4_addons.py -> add_dim_family_vol_tiers` groups the v3/v4 contract-dimensions table by `event_template`, aggregating `fam_total_vol = sum(dollar_volume)` (per-contract pre-aggregated USDC volume) and `fam_size = nunique(condition_id)`. It then terciles `log1p(fam_total_vol)` via `safe_qcut(log_vol, 3, ["Low vol","Mid vol","High vol"])` (a `pd.qcut` wrapper with `duplicates="drop"` that falls back to all-"Low vol" if quantiles collapse). The tercile label is merged back onto every contract by `event_template`, so each contract inherits its whole family's volume tier. MARKETS-UNIT confirmed: `fam_size=("condition_id","nunique")` with the inline comment "markets per family, not tokens (YES/NO share a condition_id)"; the tier itself scores `fam_total_vol`, not a token count.

**Concept.** Partitions contracts by the total lifetime dollar volume of the `event_template` family they belong to (Low/Mid/High thirds of log dollar volume). The writeup's stated hypothesis (line 919) is simply that "family dollar volume modulates FLB" — i.e. heavily-traded families should be better-learned/arbitraged and thus show attenuated or reversed bias relative to thinly-traded ones.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 917-981):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| High vol | 23.6M | +0.0187 (+2.7*) | +0.0330 (+2.5) | +0.0266 (+6.6***) | +0.0193 (+3.9***) |
| Mid vol | 412K | +0.0245 (+5.0***) | +0.0204 (+2.2) | -0.0017 (-0.2) | +0.0109 (+1.9) |
| Low vol | 64K | -0.0116 (-0.6) | +0.0469 (+5.0***) | -0.0004 (-0.1) | -0.0264 (-1.1) |

**Assessment.**
- Hypothesis NOT borne out as a clean monotone gradient. On the count-weighted mature spread the ordering is Mid (+0.0245) > High (+0.0187) > Low (-0.0116) — Mid, not an extreme tier, is the strongest FLB, and the "more volume → less bias" story collapses because the largest, best-traded families (High vol) still post robustly positive, significant FLB in every window (closing cnt +0.0266 t+6.6***, full cnt +0.0193 t+3.9***).
- Count vs dollar weighting disagree sharply in the thin tiers, especially Low vol: mature cnt is -0.0116 (t-0.6, null) but mature $ is +0.0469 (t+5.0***); the full-window $ column (not in this condensed table but in the writeup) flips Mid vol to -0.0238 (t-2.7*) while full cnt is +0.0109. Sign is weighting-dependent off the High tier, so any narrative there is fragile.
- Severe slice imbalance: High vol carries ~98% of trades (23.6M of ~24.1M mature) while Low vol has only 64K and Mid 412K. The dim barely discriminates on a trade-weighted basis — almost everything that trades lives in High-volume families — so the "tercile" is effectively a tiny-tail (Low/Mid) vs everything-else (High) split, and the Low-vol estimates rest on a thin base.
- Low-vol mature-$ +0.0469 (t+5.0***) is decile-concentrated rather than broad FLB: in the writeup's Low-vol decile table D1 dollar return is -0.0297 (t-4.8***) and D9 is +0.0681 (t+5.3***), i.e. a few large fills at the tails on 64K trades. Treat that significant-looking $ spread as low-cluster / weighting artifact, not a robust result.
- Confounds/redundancy: terciling is on `event_template` (an LLM/normalizer-assigned grouping), and total family volume is mechanically driven by category (crypto up/down and high-traffic sports families) and by `fam_size` — so this dim is heavily redundant with `dim_event_family_size`, `dim_vol_per_contract_tier`, and the category dims, and does not isolate per-contract attention. Cut points are purely data-driven equal-count log-volume terciles with no economic anchor, so they shift on any dataset refresh.
- Concrete change to consider: drop the raw total-volume tercile in favor of the size-residualized per-contract tier (`dim_vol_per_contract_residualized`, produced by the same function — terciles of `log(vol_per_contract)` residualized on `log(fam_size)`), or report `dim_vol_per_contract_tier`, plus a category control — so genuine market thickness is separated from the mechanical family-size and category mix that this dim conflates.

## 14. `dim_family_size_x_vol` — 3x3 cross-tab family size x volume

**Derivation.** `analysis/learnability/dimensions_v4_addons.py` -> `add_dim_family_vol_tiers`. Per-contract metadata, not a trades scan: it aggregates the v3 contract_dimensions table by `event_template` to build `fam_total_vol = sum(dollar_volume)` and `fam_size = nunique(condition_id)`, then takes `log1p(fam_total_vol)` and `safe_qcut`s it into 3 terciles labeled "Low vol" / "Mid vol" / "High vol". The 3x3 cell label is `dim_event_family_size + " × " + dim_family_vol_tier`, where the size axis (Singleton 1 / Small 2-20 / Medium 21-1K / Large 1K+) comes from the v3 `dim_event_family_count` bucketing. Markets-unit convention confirmed: `fam_size=("condition_id", "nunique")` counts DISTINCT condition_id (markets), not token_id — the YES/NO of a binary share one condition_id.

**Concept.** Crosses family count (how many sibling markets a template has) against family dollar attention (how much money flowed to the template), so a thin one-off vs a thick recurring series can be separated within the same size bucket. Hypothesis: disentangle family count vs family dollar attention.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 982-1052):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| Small 2-20 × High vol | 9.0M | +0.0451 (t+11.2***) | +0.0583 (t+8.1***) | +0.0339 (t+3.8***) | +0.0395 (t+7.1***) |
| Large 1K+ × High vol | 6.6M | -0.0193 (t-2.1) | -0.0152 (t-1.6) | +0.0064 (t+1.6) | -0.0081 (t-1.7) |
| Medium 21-1K × High vol | 5.6M | -0.0025 (t-0.1) | -0.0010 (t-0.0) | +0.0332 (t+3.8***) | +0.0061 (t+0.5) |
| Singleton 1 × High vol | 2.4M | +0.0485 (t+9.4***) | +0.0572 (t+5.8***) | +0.0478 (t+8.0***) | +0.0435 (t+10.0***) |
| Small 2-20 × Mid vol | 232K | +0.0040 (t+0.5) | -0.0042 (t-0.2) | -0.0177 (t-1.2) | -0.0192 (t-1.8) |
| Singleton 1 × Mid vol | 173K | +0.0394 (t+6.2***) | +0.0360 (t+3.2**) | +0.0252 (t+4.0***) | +0.0397 (t+9.2***) |
| Singleton 1 × Low vol | 34K | +0.0037 (t+0.2) | +0.0527 (t+5.2***) | +0.0201 (t+2.5*) | +0.0193 (t+2.0) |
| Small 2-20 × Low vol | 29K | -0.0474 (t-1.3) | +0.0277 (t+1.3) | -0.0232 (t-1.9) | -0.0921 (t-1.8) |
| Medium 21-1K × Mid vol | 8K | +0.0002 (t+0.0) | +0.0353 (t+1.4) | +0.0057 (t+0.3) | -0.0006 (t-0.1) |

Note: the code can in principle emit Low-vol cells for Medium 21-1K and Large 1K+ (and Large × Mid), but none appear in the writeup table — those cross-tab cells are empty or below the 5,000-trade floor (large/medium families almost never land in the low-dollar terciles).

**Assessment.**
- Hypothesis partially borne out: holding volume at High, FLB is NOT monotone in family size. Singleton (+0.0485***) and Small (+0.0451***) post strong classic FLB, but Medium (-0.0025, ns) and Large (-0.0193) collapse to zero/reversal. So family count, not just dollar attention, moves the spread; the off-diagonal cells show the "thick recurring series learn it away" story only at the Medium/Large end.
- Strongest, most credible cells are the two highest-N High-vol slices (Small 9.0M, Singleton 2.4M): consistent sign and significance across mature/closing/full windows and across cnt/$ weighting — these are the real signal.
- Volume axis is muddy. Within Singleton, mature-cnt FLB is roughly flat across vol terciles (Low +0.0037 ns, Mid +0.0394***, High +0.0485***), yet the dollar-weighted Low cell is large (+0.0527***) while its count-weighted twin is ns — the dollar number is carried by a few big tickets, not a clean attention gradient, so the volume tercile does not cleanly separate the families.
- Weaknesses: heavy redundancy with `dim_event_family_size` and `dim_family_vol_tier` (this dim is literally their Cartesian product, inheriting both confounds plus collinearity); category confound untouched (High-vol families are crypto/sports-heavy, which independently drive sign); LLM-label dependence via `event_template`; the log(vol) tercile cut points are arbitrary and population-relative.
- SE-degeneracy / floor: low-N cells are unreliable. Small × Low (29K) and Singleton × Low (34K) flip sign between cnt and $ and across windows; Small × Mid full-cnt is -0.0192 while Small × Mid full-$ in the writeup is -0.0930 (-5.9***) off a thin base — a likely few-family artifact, not a real reversal. Treat anything under ~200K N as suggestive only.
- One concrete change: drop the empty/near-floor cells and either (a) report only the High-vol row as a clean 4-cell size gradient at fixed attention, or (b) residualize volume on family size first (the sibling `dim_vol_per_contract_residualized` dim already does this) to break the size–vol collinearity that makes the off-diagonal cells uninterpretable.

## 15. `dim_vol_per_contract_tier` — Dollar-per-contract quintiles

**Derivation.** Per-contract (template-level) metadata, not a trades-scan slice. In `analysis/learnability/dimensions_v4_addons.py` -> `add_dim_family_vol_tiers`, contracts are aggregated to the `event_template` grain: `fam_total_vol = sum(dollar_volume)` and `fam_size = nunique(condition_id)` — the markets-unit-fixed count of DISTINCT condition_id, NOT token_id (the inline comment reads "markets per family, not tokens (YES/NO share a condition_id)"), so this dim correctly uses condition_id. Then `vol_per_contract = fam_total_vol / fam_size.clip(lower=1)`, and the dim is `safe_qcut(log1p(vol_per_contract), q=5, duplicates="drop")` with verbatim labels `["VPC Q1 (thinnest)", "VPC Q2", "VPC Q3", "VPC Q4", "VPC Q5 (thickest)"]`. Quintiles are equal-count over distinct templates (no fixed edges), then merged back to contracts on `event_template`; `safe_qcut` falls back to all-Q1 on a qcut ValueError.

**Concept.** Partitions every contract by how much dollar volume its template family attracts per constituent market (a family-average thin-vs-thick attention proxy). Writeup hypothesis: higher per-contract dollar attention -> calibrated, i.e. thicker quintiles should show smaller, nearer-zero FLB spreads.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 1053-1119):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| VPC Q5 (thickest) | 20.3M | +0.0228 (+3.0**) | +0.0349 (+2.5*) | +0.0308 (+6.4***) | +0.0238 (+4.3***) |
| VPC Q4 | 2.7M | -0.0172 (-2.0) | -0.0118 (-1.0) | +0.0097 (+2.9*) | -0.0065 (-1.3) |
| VPC Q3 | 858K | +0.0090 (+2.0) | +0.0052 (+0.7) | +0.0133 (+3.3**) | +0.0067 (+2.0) |
| VPC Q2 | 166K | +0.0005 (+0.1) | -0.0635 (-1.4) | -0.0181 (-0.9) | -0.0227 (-2.1) |
| VPC Q1 (thinnest) | 22K | -0.1442 (-2.0) | -0.0375 (-1.1) | -0.0376 (-2.0) | -0.1072 (-2.2) |

**Assessment.**
- Hypothesis is rejected and inverted on the headline metric: the thickest quintile VPC Q5 shows the strongest, most significant classic FLB (mature cnt +0.0228 t+3.0**, closing cnt +0.0308 t+6.4***, full cnt +0.0238 t+4.3***), not calibration. Highest-dollar-attention families are the least calibrated, opposite of "attention -> calibrated."
- No clean monotone gradient. Q5->Q1 on mature cnt is +0.0228, -0.0172, +0.0090, +0.0005, -0.1442 — signs alternate and Q4 is mildly anti-FLB. The only consistent pattern is the thin tail (Q1) flipping strongly negative / reversed (-0.1442 mature, -0.1072 full).
- Strength: VPC Q5 is robust across weighting and window (count and dollar; mature/closing/full all positive and significant) and dominates the data at 20.3M trades, so aggregate FLB is essentially this bucket.
- Weakness — extreme mass concentration: Q5 alone is 20.3M trades vs Q1's 22K. The lower four quintiles (~3.75M total) carry most of the noise; Q1's eye-catching -0.1442 / -0.1072 rests on only 22K trades and never clears |t|>2.6, so it is fragile rather than a real signal.
- Weakness — confound and redundancy: `vol_per_contract = fam_total_vol / fam_size` mechanically overlaps `dim_family_vol_tier` and is the un-residualized sibling of `dim_vol_per_contract_residualized` (built in the same function to strip the log-size correlation via closed-form OLS). High VPC is largely high-volume crypto/sports families, so Q5's positive spread likely re-expresses the volume/category story rather than an independent attention effect.
- Weakness — family-average + LLM dependence + arbitrary thresholds: the dim measures the template-family mean, not a contract's own depth (a singleton high-vol contract and a 1000-market thick family can share a tier); quintile cut points on log1p(vol_per_contract) are data-driven, not economic; the partition inherits the LLM `event_template` grouping, and the `safe_qcut` all-Q1 fallback would silently collapse the dim on a qcut failure.
- Note on degeneracy: no slice here is SE-degenerate — even the strongest t (closing Q5 t+6.4) spans the whole high-volume universe, not one event family; magnitudes are plausible, not the |t|>50 single-cluster pathology. The alternating signs in Q1-Q4 are sampling noise on thin trade counts.
- Concrete change to consider: report `dim_vol_per_contract_residualized` (size-orthogonalized) as the headline and/or rebuild VPC quintiles on trade-weighted breakpoints instead of equal template counts, so Q1-Q4 aren't reduced to <1M-trade slivers against a 20M-trade Q5.

## 16. `dim_vol_per_contract_residualized` — Vol/contract residualized on family size

**Derivation.** `analysis/learnability/dimensions_v4_addons.py` -> `add_dim_family_vol_tiers`. Per-contract metadata (template-level aggregate merged back onto contracts), NOT a fresh trades-scan. Groups by `event_template`, computing `fam_total_vol = sum(dollar_volume)` and `fam_size = nunique(condition_id)`, then `vol_per_contract = fam_total_vol / fam_size.clip(lower=1)`. MARKETS-UNIT CONFIRMED: `fam_size` counts DISTINCT `condition_id` (line 447 comment: "markets per family, not tokens (YES/NO share a condition_id)"), not `token_id`. The dim is a closed-form OLS residual: regress `log1p(vol_per_contract)` on `log1p(fam_size)` over finite/non-null templates (guarded by `valid.sum() > 10`), take residual `y - (alpha + beta*x)` with `beta = Σ(x-x̄)(y-ȳ)/Σ(x-x̄)²`, `alpha = ȳ - beta·x̄`, then `safe_qcut` (pd.qcut, `duplicates="drop"`, fallback-to-first-label on ValueError) into 3 terciles labeled exactly `["VPC resid Low", "VPC resid Mid", "VPC resid High"]`. Label broadcast to all contracts in the template via merge on `event_template`. No fixed numeric edges — data-driven tercile cuts on the residual.

**Concept.** Partitions contracts by whether their event-family drew more (`High`) or less (`Low`) dollar volume per market than predicted by family size alone — liquidity/attention orthogonal to how many markets the family spawned. Writeup hypothesis (line 1122): "volume per contract conditional on size" — does residual per-market depth predict FLB once family breadth is netted out.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 1120-1184):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| VPC resid High | 22.5M | +0.0203 (t+2.8*) | +0.0337 (t+2.5*) | +0.0279 (t+6.6***) | +0.0211 (t+4.1***) |
| VPC resid Mid | 1.5M | -0.0020 (t-0.5) | -0.0180 (t-2.0) | +0.0068 (t+1.6) | -0.0019 (t-0.6) |
| VPC resid Low | 93K | -0.0545 (t-2.3) | -0.0572 (t-1.6) | -0.0092 (t-1.1) | -0.0552 (t-2.1) |

**Assessment.**
- **Monotone count-weighted gradient, but sign is counterintuitive.** Mature cnt runs High +0.0203 (t+2.8*) > Mid -0.0020 (t-0.5) > Low -0.0545 (t-2.3) — a clean High>Mid>Low ordering. The High tercile (above-expected per-market volume) shows the *strongest* classic FLB; the thinnest-per-market families (Low) show mild reversal. A "thicker volume = better price discovery" reading would predict the opposite (High converging to zero), so the dim sorts on attention/public money, not efficiency.
- **High slice is the robust, significant one** across windows: closing cnt +0.0279 (t+6.6***) and full cnt +0.0211 (t+4.1***) both clear ***. Low is significant only count-weighted (mature -0.0545 t-2.3*, full -0.0552 t-2.1*); its dollar-weighted mature cell (-0.0572) is only t-1.6, i.e. underpowered.
- **Severe trade-count imbalance.** Terciles are cut over *templates*, but trades concentrate massively: High 22.5M vs Mid 1.5M vs Low 93K (~240:1 High:Low). High therefore dominates any pooled read and effectively equals the high-volume crypto/sports pool. Low (93K) sits just above the 5k floor and is thin.
- **No SE-degeneracy** — all three terciles span many families and t-stats stay in the 0.5–6.6 range, not 50+. But the dollar-weighted *full* column is anomalously large for Mid (-0.0705, t-6.9***) and Low (-0.1190, t-5.2***) relative to their mature counterparts, signaling a few big late/full-window dollar fills; down-weight that column.
- **Category confound + redundancy.** The residual nets out family *size* but not category; since volume concentration is crypto/sports-correlated, "High residual vol" largely re-expresses the known high-volume-strengthens-FLB result. It is also built from LLM-normalized `event_template` and overlaps with the two sibling dims in the same function (`dim_family_vol_tier`, `dim_vol_per_contract_tier`).
- **Concrete change:** add category as a second regressor (or residualize within-category) and re-tercile to test whether High>Mid>Low survives removing the crypto/sports volume confound; if it collapses, prefer the simpler `dim_vol_per_contract_tier`. Also surface the computed `meta` (`vpc_resid_beta`, `spearman_logsize_logvol`) next to the table.

## 17. `dim_text_novelty` — Semantic isolation by best-neighbor cosine

**Derivation.** Reads one float column, `best_sim` (a contract's max cosine similarity to ANY other event_slug in its top-K neighbors). `dimensions_v5.py -> add_dim_text_novelty_v5` replaces v4's empirical quintiles with FIXED `pd.cut` bins: edges `[-inf, 0.50, 0.75, 0.90, 0.95, inf]` → labels `"<0.50 genuinely isolated"`, `"0.50-0.75 mod isolated"`, `"0.75-0.90 has neighbor"`, `"0.90-0.95 close lex match"`, `">0.95 near duplicate"`. `best_sim` is per-slug metadata, NOT a trades scan: `dimensions_v4_addons.py -> build_qcluster_index` builds a hybrid char(3-5)+word(1-2) TF-IDF → TruncatedSVD(≤256 dims) → HNSW kNN index over slug-level question documents, and `compute_per_slug_novelty` takes `sims[:,1:].max(axis=1)` (self-loop at position 0 dropped) as `best_sim`, merged onto contracts by `event_slug` (missing slugs fall back to `best_sim=0.0` → "<0.50 isolated"). This is a binning dim, not a count-of-contracts dim, so the condition_id-vs-token_id convention does not gate the partition; the summary still reports both `n_markets` (distinct condition_id) and `n_contracts` (token_id) per slice.

**Concept.** Partitions trades by how lexically/semantically unique their market's question is — five bins from genuinely isolated (no near-neighbor) to near-duplicate (>0.95). Hypothesis (writeup §17): a slug with no near-duplicate has no prior anchor to learn from, so it should show strong FLB; repetitive near-duplicate slugs (e.g. recurring "Up or Down" templates) should be best-priced/weakest FLB.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| >0.95 near duplicate | 18,475,855 | +0.0099 (t+1.0) | +0.0272 (t+1.6) | +0.0210 (t+4.4***) | +0.0144 (t+2.2*) |
| 0.90-0.95 close lex match | 2,379,220 | +0.0135 (t+1.5) | +0.0251 (t+2.1*) | +0.0173 (t+1.8) | +0.0072 (t+0.9) |
| <0.50 genuinely isolated | 2,248,286 | +0.0556 (t+11.8***) | +0.0684 (t+6.7***) | +0.0605 (t+14.2***) | +0.0518 (t+12.8***) |
| 0.75-0.90 has neighbor | 954,306 | +0.0509 (t+5.2***) | +0.0722 (t+16.9***) | +0.0101 (t+0.5) | +0.0314 (t+3.5***) |
| 0.50-0.75 mod isolated | 23,467 | +0.0599 (t+7.9***) | +0.0796 (t+8.3***) | +0.0757 (t+9.2***) | +0.0634 (t+9.9***) |

**Assessment.**
- Hypothesis directionally borne out at the extremes but NOT monotone: the most-isolated bin (<0.50) is the cleanest, large signal (+0.0556 mature cnt, t+11.8***, stable across all three windows), while the most-repetitive bin (>0.95) is the weakest (+0.0099 mature cnt, ns; only marginally significant in closing/full). However the ordering reverses in the middle — 0.75-0.90 ("has neighbor") posts a stronger spread (+0.0509) than 0.90-0.95 ("close lex match", +0.0135), so the gradient is non-monotone across the five bins.
- Strength: the genuinely-isolated slice is robust and well-powered (2.2M mature trades, 4,057 markets), and its sign/magnitude persist in mature, closing, and full windows — a credible "no-anchor → FLB" result independent of the exact threshold.
- The whole dim is dominated by one bucket: >0.95 holds 18.5M of ~24M mature trades (152,884 of ~167K markets). That bucket is the recurring-template mass (Up/Down, daily crypto), so this dim largely re-expresses recurrence/`dim_recurrence_class` and overlaps `dim_text_neighbors_strict` (same kNN index, n_above_0_75 counts) — check redundancy before keeping all of them.
- The `0.50-0.75 mod isolated` slice is thin: 98 markets / 176 contracts / 23,467 trades (mature), yet posts t+7.9*** cnt / t+8.3*** $. It clears the 5k-trade floor and isn't single-event-family, but the small market/cluster span makes its 3-way-clustered SE fragile — treat its large t with caution, not as strong independent evidence. The >0.95 bin's dollar-weighted closing t (+5.2***) vs ns count-weighted hints a few large near-duplicate markets drive the $ signal.
- Depends entirely on an LLM/embedding-derived label: `best_sim` is sensitive to the TF-IDF recipe (char+word n-grams, min_df=2/max_df=0.5), SVD dim (≤256, random_state=0), and HNSW approximation (M=32, ef=100, k=20) — the 0.50/0.75/0.90/0.95 cut points are hand-set, not data-driven, so bin boundaries are arbitrary.
- Concrete change: collapse to a 3-bin version (isolated <0.75 / has-neighbor 0.75-0.95 / near-dup >0.95) to fold the unstable 0.50-0.75 micro-slice into a powered bucket and restore monotonicity, OR test sensitivity by re-running with one alternative SVD dim / tau to confirm the <0.50-vs->0.95 contrast survives the embedding choice.

## 18. `dim_text_neighbors_strict` — Count of slugs above 0.75 cosine

**Derivation.** Per-slug metadata, NOT a trades scan. `dimensions_v4_addons.py -> compute_per_slug_novelty` builds a char(3-5)+word(1-2) hybrid TF-IDF over `' || '.join(distinct question)` documents per `event_slug`, projects via TruncatedSVD to ≤256 dims, runs an hnswlib top-20 kNN, and for each slug counts neighbors (excluding the self-loop at position 0) with cosine `>= 0.75` into `n_above_0_75`. `add_dim_text_novelty` then bins that count with `pd.cut(n_above_0_75, bins=[-1, 0, 1, 5, np.inf])` → labels `["0 strict neighbors", "1 strict neighbor", "2-5 strict neighbors", "6+ strict neighbors"]`, joins to contracts on `event_slug` (left; missing slugs filled "0 strict neighbors"). `dimensions_v5.py:add_dim_text_novelty_v5` explicitly keeps this dim unchanged from v4 (only `dim_text_novelty` got fixed-threshold bins). The count is per-slug (distinct slugs, not markets), so the condition_id-vs-token_id markets convention does not apply here — the bin is a slug attribute broadcast to every contract under that slug.

**Concept.** Partitions contracts by how lexically crowded their event_slug's question text is — how many other slugs say nearly the same thing (≥0.75 cosine). It is a threshold-count restatement of text-novelty: isolated/novel slugs (0 neighbors) should be harder to price (more FLB) than slugs embedded in a dense repetitive family (6+ neighbors), where templated repetition makes outcomes learnable.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| 6+ strict neighbors | 21.8M | +0.0127 (t+1.6) | +0.0299 (t+2.1*) | +0.0203 (t+4.5***) | +0.0144 (t+2.6**) |
| 0 strict neighbors | 2.3M | +0.0556 (t+11.9***) | +0.0684 (t+6.8***) | +0.0606 (t+14.3***) | +0.0519 (t+13.0***) |
| 2-5 strict neighbors | 18K | +0.0391 (t+2.1*) | +0.0354 (t+1.4) | -0.1152 (t-0.9) | +0.0017 (t+0.1) |
| 1 strict neighbor | — | — | — | — | +0.0375 (t+1.7) |

**Assessment.**
- Hypothesis borne out at the endpoints and the gradient is monotone where it matters: "0 strict neighbors" (isolated/novel) posts the strongest, highly significant FLB (+0.0556, t+11.9 mature; +0.0606, t+14.3 closing), while the crowded "6+ strict neighbors" bucket is roughly 4x smaller (+0.0127, t+1.6 mature; significant only by dollar weight and in closing/full). Isolated slugs are mispriced; repetitive ones much less so — exactly the learnability prediction.
- Strength: the headline contrast (0 vs 6+) is large, stable across all three windows and both weightings, and dollar-weighted always exceeds count-weighted (+0.0684 vs +0.0556 mature), so the bias is concentrated in larger tickets, not a small-trade artifact.
- The two interior buckets are near-useless. "2-5 strict neighbors" carries only 18K trades mature / 15.5K closing / 55.5K full; its closing-window value (-0.1152, t-0.9) flips sign on a handful of trades and is noise, and the v6 decile table for it (writeup §18) shows wild per-decile swings (e.g. dollar D4 +0.4834 t+6.2***, D7 -0.3987 t-3.7***) — SE-degenerate, spanning very few clusters; do not read signal into it.
- "1 strict neighbor" falls below the 5,000-trade floor in mature AND closing (absent there); it surfaces only in full (7,834 trades, +0.0375 t+1.7 cnt). The dim is effectively binary in practice: only "0" and "6+" have enough mass to interpret.
- Confounds/overlap: this is a deterministic restatement of dim_text_novelty (same `best_sim`/kNN machinery, same slug index), so it is largely redundant with dimension 17 — they should not be treated as independent evidence. The score also rides on LLM-extracted `question` text and an arbitrary 0.75 cosine cut plus arbitrary count edges {0,1,5}. No category control, so "isolated slug" likely correlates with one-off political/news markets while "6+" loads on templated sports/crypto series — the FLB gap may be a category proxy.
- Concrete change: collapse to a clean binary (0 vs ≥6, dropping the under-powered 1 and 2-5 interior bins) and re-estimate within `dim_primary_category` to test whether the novelty gradient survives the category confound, rather than reporting four bins two of which never clear the trade floor.

## 19. `dim_prior_settlements_bin__event_template` — Prior settled markets at trade time (event_template grouping)

**Derivation.** `analysis/learnability/dimensions_v4_addons.py -> add_dim_prior_settlements(df, group_col="event_template", first_ts_col="first_ts", last_ts_col="last_ts")`. Per-contract metadata (a precomputed dim attached to the contract dimensions parquet, not a live trades-scan). For each contract it counts how many same-`event_template` *markets* settled strictly before this contract started trading: it first reduces to one settlement time per `(event_template, condition_id)` via `groupby([group_col,"condition_id"])[last_ts].max()`, sorts those per group, then `np.searchsorted(last_sorted, first_ts, side="left")` counts markets with `last_ts < this.first_ts` (a contract's own market self-excludes since its `c_last >= first_ts`). Raw count binned with verbatim edges `bins=[-1, 0, 5, 50, np.inf]` → `labels=["0", "1-5", "6-50", "50+"]`. **Markets-unit convention confirmed**: the dedup-to-`condition_id` is explicit (code comment: "Count prior MARKETS (condition_id), not tokens: a binary market's YES+NO settle together and must not count as two prior settlements"), so a binary YES/NO pair counts as one prior settlement.

**Concept.** Partitions trades by how many prior same-template markets had already resolved at the moment a contract first traded — a within-series experience/learnability proxy. **Hypothesis** (writeup line 1320): more prior same-template settlements at trade time → less FLB (smaller, ideally vanishing, D10−D1 spread).

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 1318-1383):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| 0 | 13.1M | +0.0319 (+4.3***) | +0.0376 (+2.0) | +0.0462 (+11.8***) | +0.0322 (+5.8***) |
| 50+ | 7.6M | -0.0273 (-1.9) | -0.0083 (-1.0) | +0.0003 (+0.0) | -0.0125 (-1.5) |
| 6-50 | 1.8M | +0.0133 (+0.5) | +0.0337 (+2.0) | +0.0509 (+10.6***) | +0.0170 (+0.9) |
| 1-5 | 1.5M | +0.0424 (+3.7***) | +0.0455 (+2.2) | -0.0051 (-0.2) | +0.0261 (+2.0) |

**Assessment.**
- **Hypothesis directionally borne out at the extreme, but not monotone.** Mature count-weighted spread runs `0`: +0.0319*** → `1-5`: +0.0424*** → `6-50`: +0.0133 → `50+`: -0.0273. The most-settled bucket (`50+`) is the only one to flip negative (FLB extinguished / mildly reversed), consistent with "more resolved history → less FLB." But the gradient is non-monotone — `1-5` posts the *strongest* mature FLB (+0.0424***), above the zero-prior bucket — so it's a 0/low-vs-`50+` contrast more than a clean dose-response.
- **Window matters.** Closing-window (80-100% cnt) FLB is large and highly significant for the low/mid-history buckets (`0`: +0.0462***, `6-50`: +0.0509***) but vanishes at `50+` (+0.0003, t≈0.0). The clearest learnability signal is in late-in-life trading, not mature-mid trading.
- **No SE-degenerate slices.** All four buckets are 1.5M–13.1M trades with modest t-stats (|t| ≤ 11.8); none are one-event-family blowups. Dollar-weighted columns are noisier than count-weighted (e.g. `0` mature $ +0.0376 t+2.0 vs cnt t+4.3***), as expected from whale concentration, but not degenerate.
- **Category/recurrence confound.** `event_template` prior-settlement count is mechanically entangled with recurrence: the `50+` bucket is dominated by ultra-recurrent series (hourly crypto up/down, repeated sports), so its ≈"no FLB" result may reflect liquid/efficient market *type* rather than genuine cross-time learning. The dim heavily overlaps `dim_event_family_size` / the family-volume dims and is essentially their temporal twin.
- **LLM-label + threshold dependence.** Grouping key is the LLM-derived `event_template`; the four buckets use hand-set edges (0 / 5 / 50) with no robustness sweep in this dim. The middle-bin non-monotonicity between `0`, `1-5`, and `6-50` suggests the bins aren't capturing a stable gradient.
- **One concrete change.** Since the real signal is `0`/low vs `50+`, either collapse to a binary `0`-vs-`≥1` dim, or residualize the prior-settlement count against category/`dim_event_family_size` before bucketing — to test whether the `50+` → no-FLB result survives once novel-vs-recurring market mix is held fixed.

## 20. `dim_prior_settlements_bin__event_slug` — Prior settled markets (event_slug grouping)

**Derivation.** `analysis/learnability/dimensions_v4_addons.py` -> `add_dim_prior_settlements(df, group_col="event_slug", first_ts_col="first_ts", last_ts_col="last_ts")`. Per-contract timestamp metadata (no live trades scan): for each contract it counts how many same-`event_slug` *markets* settled strictly before this contract opened. To honor the markets-unit convention it first reduces to one settlement time per `(event_slug, condition_id)` via `groupby([group_col, "condition_id"])[last_ts].max()` (lines 526-527), sorts those per group, then `np.searchsorted(last_sorted, first_ts, side="left")` — so a binary market's YES+NO settle together and count once, not twice. Confirmed: it uses `condition_id` (distinct markets), not `token_id`. Bin edges are verbatim `[-1, 0, 5, 50, inf]` with labels `["0", "1-5", "6-50", "50+"]` (lines 548-549).

**Concept.** Partitions contracts by how many prior same-event_slug markets had already resolved before the contract began trading — a recurrence/learnability proxy at the (coarse) Polymarket-event grain. Hypothesis (writeup line 1386): "same at event_slug grain" — i.e. the event_slug twin of the prior-settlements test, where more prior resolutions in the family should let traders learn the series and shrink FLB (gradient decreasing in prior-settlement count).

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 1384-1448):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| 0 | 23.1M | +0.0175 (+2.5*) | +0.0313 (+2.3) | +0.0271 (+6.9***) | +0.0185 (+3.7***) |
| 1-5 | 783K | +0.0342 (+5.5***) | +0.0584 (+10.0***) | -0.0184 (-0.8) | +0.0209 (+2.6*) |
| 6-50 | 246K | +0.0628 (+11.7***) | +0.0837 (+19.2***) | +0.0371 (+3.6***) | +0.0422 (+5.1***) |

Note: the code can emit a `50+` bucket (top edge of the `[-1,0,5,50,inf]` cut), but it is entirely absent from the writeup table — it falls below the 5000-trade floor at the event_slug grain and is not reported.

**Assessment.**
- Hypothesis NOT borne out — the gradient runs the WRONG way for "learning." Mature count-weighted FLB *rises* monotonically with prior settlements: +0.0175 (0) → +0.0342 (1-5) → +0.0628 (6-50); dollar-weighted is even steeper, +0.0313 → +0.0584 → +0.0837. More prior resolutions coincides with *more* classic FLB, not less.
- The three reported slices are real, not SE-degenerate: each spans a large trade base (N 246K-23.1M) and significance is broad-based — both cnt and $ are highly starred for the 1-5 and 6-50 slices (e.g. 6-50 $ = +0.0837, t+19.2***), not a one-family |t|>50 artifact.
- The `0` slice (no prior settlements, N=23.1M, ~96% of trades) is the *weakest* FLB and its mature dollar-weighted spread is insignificant (+0.0313, t+2.3); its only strong signal is in the closing 80-100% window (+0.0271, t+6.9***). The headline FLB gradient is driven by the small recurrent-family slices (1-5, 6-50), not the bulk.
- Confounds/redundancy: prior-settlement count is mechanically a recurrence/family-size proxy, so this dim is highly redundant with `dim_event_slug_size`, `dim_event_family_size`, and the sibling `dim_prior_settlements_bin__event_template`. High-recurrence event_slugs skew to repeating sports/crypto series, so the perverse gradient likely reflects category mix rather than experience per se.
- The 1-5 slice is weighting-unstable across windows: strongly positive mature (+0.0342 cnt / +0.0584 $) but flips negative and insignificant in the closing window (-0.0184 cnt, t-0.8; $ -0.1356, t-1.3) — a mid-bucket sign flip that suggests a few large closing fills rather than a stable effect. Bin thresholds (0/1-5/6-50/50+) are also arbitrary and coarse, and with `50+` censored below the floor the top of the gradient is unobserved.
- Concrete change: report (or merge in) the missing `50+` slice and residualize prior-settlement count against `dim_event_slug_size` / category — or use a within-event_slug fixed-effect spec — to test whether the positive gradient survives a recurrence/category control instead of just re-expressing which series are recurrent.

## 21. `dim_prior_settlements_bin__dim_group_strict` — Prior settled markets (strict grouping)

**Derivation.** Per-contract metadata, computed in `analysis/learnability/dimensions_v4_addons.py -> add_dim_prior_settlements(group_col="dim_group_strict")`. The strict group ID is `event_slug + "|" + market_template` (`add_dim_group_strict`, NA-filled to `__NA_SLUG__`/`__NA_MT__`). For each contract it counts how many same-group *markets* settled strictly before this contract opened: it reduces to one settlement time per `(group, condition_id)` via `groupby(...)["last_ts"].max()`, sorts those per group, and `np.searchsorted(last_sorted, first_ts, side="left")` — so it explicitly counts DISTINCT condition_id (markets), not token_id (a binary's YES/NO settle together and count once). Binned by `pd.cut(count, bins=[-1, 0, 5, 50, inf], labels=["0","1-5","6-50","50+"])`. Reads `first_ts`/`last_ts` (trade-derived timestamps) plus `event_slug`, `market_template`, `condition_id`, `token_id`.

**Concept.** Partitions contracts by how many prior settlements of the *same strict template within the same event* a trader could have learned from before this contract opened — the tightest recurrence grain (event AND market shape must match). Writeup hypothesis (line 1451) is terse: "same at strict grouping grain" — i.e. the prior-settlements learnability story carried over to the finest strict grain, predicting FLB attenuates as repeated settled instances accumulate.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k floor; transcribed from canonical writeup lines 1449-1514):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| 0 | 23.3M | +0.0172 (t+2.5) | +0.0309 (t+2.2) | +0.0266 (t+6.9***) | +0.0182 (t+3.7***) |
| 1-5 | 520K | +0.0623 (t+8.7***) | +0.0825 (t+20.0***) | -0.0328 (t-0.8) | +0.0343 (t+2.7*) |
| 6-50 | 228K | +0.0628 (t+10.2***) | +0.0802 (t+15.7***) | +0.0454 (t+4.7***) | +0.0494 (t+5.5***) |
| 50+ | 31K | +0.1080 (t+292.3***) | +0.1109 (t+0.0) | +0.0566 (t+0.0) | +0.0620 (t+1504.2***) |

**Assessment.**
- Hypothesis is REVERSED, not borne out: in the mature window FLB grows monotonically with prior settlements (cnt +0.0172 → +0.0623 → +0.0628 → +0.1080; $ +0.0309 → +0.0825 → +0.0802 → +0.1109). More repeated settlements ⇒ *stronger* longshot overpricing, the opposite of a learnability effect.
- The `50+` slice is SE-DEGENERATE — flag, do not trust the significance: t-stats of +292.3 (mature cnt) and +1504.2 (full cnt) on only N=31K, while the paired dollar-weighted cells collapse to (t+0.0). This is the few-cluster / one-event-family signature; the magnitude (~+0.10) may be real but the t's are an artifact of 3-way (day×wallet×market) clustering having almost no independent clusters.
- The well-powered middle slices (`1-5` 520K, `6-50` 228K) are the cleanest evidence and the load-bearing result: strong classic FLB across mature/closing/full, both weightings (mature cnt +0.0623/+0.0628, $ +0.0825/+0.0802, all ***). Their decile paths show the canonical signature — sharply negative D1 longshots (`1-5` D1 -0.0284 t-4.1; `6-50` D1 -0.0319 t-9.0) plus positive D10.
- The dominant slice `0` (23.3M, contracts with no prior settled same-group market) is the weakest: mature cnt only +0.0172 (t+2.5) — borderline by the |t|>1.96 bar — and dollar-weighted +0.0309 (t+2.2) is similarly marginal. Its decile path is noisy (D4 -0.053, D6 +0.070, D7 +0.107 all |t|<1.6), with the spread carried by the D9/D10 endpoints.
- Confounds: (a) category — recurring strict templates are dominated by high-frequency sports/crypto daily series (updown, game lines), so "more prior settlements" largely proxies "is a high-frequency series," not learning; this overlaps directly with category and family-size dims and is not held fixed. (b) `1-5` closing window swings negative (cnt -0.0328 $ -0.1611, both insignificant) — do not read closing-window signal off the thinner buckets. (c) LLM-`market_template`-dependent — mis-templated markets mis-group.
- Redundancy: this is the strict-grain sibling of the `event_template` / `event_slug` prior-settlements dims; the ordering and rough magnitudes track those, so it mostly confirms the family pattern at a finer grain rather than adding independent signal.
- One change to consider: residualize the spread on category (or restrict to within-category comparisons) to separate genuine learnability from the recurring-series/public-money mix, and merge `6-50`+`50+` (or report `50+` only with a cluster-count caveat) given how thin and SE-degenerate the top bucket is.

## 22. `dim_market_type` — Up/down vs non-up/down sensitivity

**Derivation.** Per-contract metadata label, computed in `analysis/learnability/dimensions_v5.py -> add_dim_market_type`. Reads one column, `event_template`, fills NaN with `""`, and applies the regex `UPDOWN_RE = re.compile(r"updown|up-or-down", re.IGNORECASE)`: a match → `"updown"`, else `"non_updown"`. Binary, two slices only — no numeric bins/quantiles. Not a count-of-contracts dim (it is a label attached to each contract from stage2 template text, not a count), so the condition_id-vs-token_id markets convention does not apply; the comment notes it is excluded from the primary v5 trades view and reported only as this sensitivity slice.

**Concept.** Partitions every trade by whether its market is a crypto "up/down" (price-direction) contract vs everything else. Writeup hypothesis (lines 1515-1517): up/down crypto markets are **noise-trading regimes structurally different from learnability markets**, so they should be split out rather than mixed into the learnability analysis.

**Results** (spread (t); cnt = count-weighted, $ = dollar-weighted; — = below 5k-trade floor):

| Slice | N mature | mature cnt | mature $ | closing cnt | full cnt |
|---|--:|--:|--:|--:|--:|
| non_updown | 24.0M | +0.0187 (t+2.8**) | +0.0330 (t+2.5*) | +0.0255 (t+6.5***) | +0.0189 (t+3.9***) |
| updown | 68.7K | +0.0519 (t+55.5***) | +0.0463 (t+56.3***) | +0.0548 (t+350.4***) | +0.0545 (t+922.0***) |

**Assessment.**
- Hypothesis is **directionally supported but not via the mechanism claimed**: up/down markets do behave differently — their spread is ~2.8x larger (+0.0519 vs +0.0187 mature cnt) — but the difference is *more* classic FLB, not a distinct "noise" regime with reversed/null calibration.
- **All `updown` t-stats are SE-degenerate, treat as no signal.** The slice spans only **498 markets** mature / 507 closing / 517 full (vs 167K-243K for non_updown), yet posts |t| of 55, 350, 922, even 1684 (closing $). The full-window dollar t = 0.0 is an outright degenerate/NaN collapse. These come from a handful of near-identical recurring crypto event families, so the 3-way day×wallet×market clustering has almost no independent market clusters — the tiny SEs (se_dol down to 0.00004) are an artifact, not precision.
- The only **honestly-powered** estimate is `non_updown`, and it just reproduces the population FLB (+0.019 to +0.026 across windows, all genuinely significant on 167K+ markets). So this dim's real content is "split out a small noisy crypto family," which it does at <0.3% of trades (68.7K of 24M+).
- **LLM/template dependence + confound:** the label rides entirely on `event_template` text matching two regex tokens; any crypto up/down contract whose template doesn't contain `updown`/`up-or-down` is silently bucketed `non_updown`. It also overlaps heavily with `dim_primary_category` (crypto) and likely `dim_recurrence_class`/`dim_contract_horizon` (short-horizon recurring), so it is largely redundant with existing category/recurrence dims rather than orthogonal.
- **Concrete change:** keep `updown` purely as an *exclusion filter* (drop it from the main learnability view, which the code already intends) and **stop reporting its spread/t at all**, since with ~500 markets it can never clear the degeneracy bar. If a real up/down vs not contrast is wanted, restrict to non-recurring crypto and require many distinct event families before quoting a t-stat.
