> **ARCHIVED 2026-07-02.** Canonical v6 results for the LLM-era learnability dimensions — superseded by the v7 native-dims pivot. Read with care:
> - The reproducibility pointer below predates the Phase B renames: the scripts are now `analysis/learnability/flb_per_slice.py` / `run_phase1.py` (outputs in `/mnt/data/learnability/output/`); a post-rename end-to-end run reproduced these numbers exactly.
> - The data end 2026-04-28 (1.377B rows); the canonical set now runs through 2026-06-23 and carries a **resolution-censoring caveat** unknown when this was written — see `docs/methods_reference.md` before comparing across time.

# FLB by Learnability Dimensions — Polymarket — v6

_v6 rebuilds every result on the **deduplicated clean trades dataset** and reports **dollar-weighted alongside count-weighted** calibration throughout. Versus v5: (1) trades source is the clean parquet (1.377B rows; 58.2M full-row ingestion-replay duplicates removed — see `data_exploration.md`); (2) the bot `wallet_flags` were recomputed on clean data; (3) every spread is reported both count-weighted (each trade = 1 observation) and dollar-weighted (each trade weighted by `usdcSize`); (4) the favorite-longshot **slope** (D10−D1) is distinguished from a level **offset** (see "Offset vs slope" below); (5) the count-of-contracts dimensions (family size, outcomes-per-event, group/slug sizes, recurrence, prior-settlements) now count distinct **markets** (`condition_id`), not outcome tokens — a binary market's YES/NO are separate token_ids that previously double-counted, emptying the "Binary 1"/"Singleton 1" buckets (binaries misfiled into "Few 2-5") and suppressing the "One-off" recurrence class entirely. Counting markets restores those buckets (Binary 1 and One-off are now the strongest-FLB endpoints) and re-cuts the family-size/prior-settlement boundaries onto the correct unit. v5's audit-driven structure is retained: up/down excluded from the primary view (reported separately as `dim_market_type`), fixed-threshold `dim_text_novelty`, three lifecycle windows, Bonferroni-corrected significance, and Appendices A–E._

## Setup

**Dataset.** 1.117M Polymarket contracts (`stage2_per_contract_augmented.parquet`) joined to the **clean** trade-level parquet (`/mnt/data/pipeline_output/trades_clean.parquet`, 1,377,065,934 rows). The clean set removes 58.2M (4.06%) byte-for-byte full-row duplicates (ingestion-pipeline replays); this changes calibration negligibly (replays are random w.r.t. outcome) but tightens SEs honestly and removes ~2% of penny-trade volume. Dimension assignments (volume tiers, horizons, prior-settlement counts) are reused from the cached contract-dimensions parquet; the trade-derived ones move <2% on clean (volume tiers do not cross boundaries; horizons and prior-settlement counts are timestamp/resolution-based and unchanged).

**Trades scope.** `side = 'BUY'` only (return-on-stake of the taker). The view excludes (1) wallets flagged non-human by the behavioral bot filter, and (2) **up/down markets** (`eventSlug NOT LIKE '%updown%' AND NOT LIKE '%up-or-down%'`) — sub-hour noise-trading regimes that contaminated several v3 findings; reported separately as `dim_market_type`.

**Weighting (new in v6).** Every decile calibration error and every D10−D1 spread is computed two ways:
- **Count-weighted** — `mean(won − price)` over trades; each trade is one observation. The per-*decision* view (standard FLB convention).
- **Dollar-weighted** — `sum(usdcSize·(won − price)) / sum(usdcSize)`; each trade weighted by stake. The per-*dollar* (economic-magnitude) view.
Tables report both as `spread (t)` pairs. Where they agree, the bias is not a small-trade artifact; where they diverge, trade size carries information (flagged in the headline section). Note: dollar-weighting concentrates the effective sample on fewer large trades, so dollar t-stats are typically a touch lower than count t-stats at equal spread.

**Offset vs slope.** FLB is a *slope*: longshots (low deciles) overpriced, favorites (high deciles) underpriced, so D10−D1 > 0. That is distinct from a uniform *level offset* (buyers beat/lose to the price at every decile), which is a calibration shift, not FLB. v6 reports the D10−D1 spread as the FLB measure; where a slice instead shows a flat nonzero level (notably Sports/Esports, where mid-life buyers win and closing buyers lose), that is documented as an offset in `data_exploration.md` (the Sports/Esports two-regime case study with liquidity-floor diagnostics), not folded into the FLB spread here.

**Bot filter** (`analysis/learnability/bot_filter.py`). `is_nonhuman` = OR of: **A** inter-trade-interval median <1s (definite) / 1–10s (likely); **B** trades-per-active-day >500 (definite) / >200 (likely); **C** hour-of-day HHI <0.06  AND  n>500; **E** size CV(usdcSize) <0.05  AND  n>50. Composite: `A_def OR (A_lik  AND  any{B,C,E}) OR (B_def  AND  C) OR (>=2 of {B_lik,C,E})`. Recomputed on clean data: **255,261 wallets flagged (21.20%)**, 1.11B trades (80.6%) — 458 fewer wallets than the raw-based flags (replays marginally over-flagged). ±25% threshold sensitivity swings headline spreads <=0.005 (audit §11).

**Lifecycle windows** (reported side-by-side): **25-80% mature** (primary), **80-100% closing** (secondary), **0-100% full** (baseline). Position = `(t.timestamp − mkt_start)/mkt_duration` from min/max trade timestamps. Mechanically incomparable across durations (a <1h crypto's 25-80% is minutes; a >1mo election's is weeks) — see `dim_contract_horizon`.

**Inference.** 3-way clustered SE on (`trade_day`, `proxyWallet`, `market_id`) via Cameron-Gelbach-Miller (2011) inclusion-exclusion; the dollar-weighted SE uses the weighted-residual analogue (score `e_i = w_i(r_i − theta_w)`, normalizer `(Σw)²`). `market_id` is the per-market `condition_id` so YES/NO cluster together. Unstable on small slices (see Caveats).

**Significance.** Family ≈ 3 windows × ~22 dims × ~3–5 kept slices × 2 weightings. Bonferroni-equivalent |t|: **`*` > 2.5, `**` > 3.0, `***` > 3.5** — not the naive 1.96.

**Deciles.** Equal-width: D1 = (0.01, 0.10) … D10 = (0.90, 0.99). **Minimum slice size** 5,000 trades; smaller slices dropped silently.

**Reproducibility.** `analysis/learnability/{dimensions*.py, flb_per_slice_v3.py, run_phase1_v5.py, bot_filter.py}` + the contract-dimensions parquet + the clean trades parquet on EC2 (`config.py:TRADES_PARQUET_GLOB`).


## Headline findings

Strongest favorite-longshot slices (mature 25-80% window, |t|>3.5 count-weighted, `dim_market_type` excluded as a sensitivity slice; sorted by |t|; dollar-weighted alongside):

| Dim : slice | N | count spread (t) | dollar spread (t) |
|---|---:|---:|---:|
| `dim_outcomes_per_event` : Few 2-5 | 4.1M | +0.0474 (+14.9***) | +0.0636 (+13.0***) |
| `dim_primary_category` : Finance | 1.1M | +0.0511 (+12.1***) | +0.0623 (+10.3***) |
| `dim_text_neighbors_strict` : 0 strict neighbors | 2.3M | +0.0556 (+11.9***) | +0.0684 (+6.8***) |
| `dim_text_novelty` : <0.50 genuinely isolated | 2.2M | +0.0556 (+11.8***) | +0.0684 (+6.7***) |
| `dim_prior_settlements_bin__event_slug` : 6-50 | 246K | +0.0628 (+11.7***) | +0.0837 (+19.2***) |
| `dim_family_size_x_vol` : Small 2-20 × High vol | 9.0M | +0.0451 (+11.2***) | +0.0583 (+8.1***) |
| `dim_event_family_size` : Small 2-20 | 9.2M | +0.0441 (+11.1***) | +0.0580 (+8.0***) |
| `dim_info_type_supergroup` : other | 5.7M | +0.0443 (+10.4***) | +0.0579 (+10.3***) |
| `dim_prior_settlements_bin__dim_group_strict` : 6-50 | 228K | +0.0628 (+10.2***) | +0.0802 (+15.7***) |
| `dim_event_family_size` : Singleton 1 | 2.6M | +0.0476 (+9.9***) | +0.0567 (+5.9***) |
| `dim_recurrence_class` : One-off | 2.6M | +0.0476 (+9.9***) | +0.0567 (+5.9***) |
| `dim_family_size_x_vol` : Singleton 1 × High vol | 2.4M | +0.0485 (+9.4***) | +0.0572 (+5.8***) |
| `dim_primary_category` : Geopolitics | 1.0M | +0.0510 (+8.8***) | +0.0712 (+13.1***) |
| `dim_primary_category` : Politics | 6.8M | +0.0438 (+8.7***) | +0.0599 (+7.1***) |

**Dollar vs count — the systematic pattern.** Across the 32 mature-window slices significant at |t|>3.0 (count-weighted, non-degenerate), the **dollar-weighted spread exceeds the count-weighted spread in 94%** of them (median ratio 1.30×). Large-stake trades exhibit *stronger* favorite-longshot bias than the typical trade — the bias is concentrated in big bets, not diluted by them. This holds across categories (Finance, Politics, Geopolitics), family-size, novelty, and prior-settlement dimensions alike, so the headline FLB findings are economic, not penny-trade artifacts.


**No genuine sign flips**: wherever both weightings are significant they agree on direction.


**Degenerate dollar-weighted slices** (|dollar spread| > 0.15) — thin slices where a single very large trade dominates the dollar mean; the dollar SE is not asymptotically valid here, so these are reported for completeness only and excluded from the pattern above:

| Window | Dim : slice | N | dollar spread (t) |
|---|---|---:|---:|
| 80-100% | `dim_contract_horizon` : <1h | 87K | -0.3406 (-3.7***) |
| full | `dim_contract_horizon` : <1h | 284K | -0.3270 (-5.0***) |
| 25-80% | `dim_dollar_volume_tier` : Q1 (<=$32) | 81K | -0.2729 (-1.5) |
| full | `dim_vol_per_contract_tier` : VPC Q2 | 540K | -0.1803 (-4.8***) |
| 25-80% | `dim_contract_horizon` : <1h | 102K | -0.1736 (-2.6*) |
| 80-100% | `dim_text_neighbors_strict` : 2-5 strict neighbors | 16K | -0.1678 (-0.8) |
| 80-100% | `dim_prior_settlements_bin__dim_group_strict` : 1-5 | 238K | -0.1611 (-1.3) |

**SE-degenerate slices** — the 3-way clustered SE collapses toward zero when a slice has essentially one effective day/wallet/market cluster (e.g. a single recurring event family), producing absurd t-stats. These are excluded from the tables above and reported only as a flag:

| Window | Dim : slice | N | spread (t, invalid) |
|---|---|---:|---:|
| full | `dim_prior_settlements_bin__dim_group_strict` : 50+ | 71K | +0.0620 (t=+1504) |
| full | `dim_market_type` : updown | 150K | +0.0545 (t=+922) |
| 80-100% | `dim_market_type` : updown | 72K | +0.0548 (t=+350) |
| 25-80% | `dim_prior_settlements_bin__dim_group_strict` : 50+ | 31K | +0.1080 (t=+292) |
| 25-80% | `dim_market_type` : updown | 69K | +0.0519 (t=+56) |
| 80-100% | `dim_prior_settlements_bin__dim_group_strict` : 50+ | 37K | +0.0566 (t=+0) |


## Calibration grids

Each panel is one dimension; each line is a slice's calibration curve (empirical win rate vs implied probability, by decile). On the gray diagonal = perfectly calibrated; bowed *below* the diagonal at low prices and *above* at high prices = favorite-longshot bias (longshots overpriced, favorites underpriced).

**Count-weighted** (each trade equal):

![25-80% mature — count](v6_figures/phase1_v6_25_80_calibration_grid_count.png)

![80-100% closing — count](v6_figures/phase1_v6_80_100_calibration_grid_count.png)

![0-100% full — count](v6_figures/phase1_v6_full_calibration_grid_count.png)

**Dollar-weighted** (each trade weighted by `usdcSize`):

![25-80% mature — dollar](v6_figures/phase1_v6_25_80_calibration_grid_dollar.png)

![80-100% closing — dollar](v6_figures/phase1_v6_80_100_calibration_grid_dollar.png)

![0-100% full — dollar](v6_figures/phase1_v6_full_calibration_grid_dollar.png)


## Dimension ranking — by mature-window spread variance (count-weighted)

| Dim | Spread std | N slices |
|---|---:|---:|
| `dim_dollar_volume_tier` | 0.1035 | 4 |
| `dim_vol_per_contract_tier` | 0.0677 | 5 |
| `dim_vol_per_contract_residualized` | 0.0384 | 3 |
| `dim_prior_settlements_bin__dim_group_strict` | 0.0371 | 4 |
| `dim_event_family_size` | 0.0335 | 4 |
| `dim_family_size_x_vol` | 0.0317 | 9 |
| `dim_prior_settlements_bin__event_template` | 0.0307 | 4 |
| `dim_contract_horizon` | 0.0292 | 5 |
| `dim_recurrence_class` | 0.0287 | 4 |
| `dim_info_type_supergroup` | 0.0257 | 7 |
| `dim_text_novelty` | 0.0242 | 5 |
| `dim_primary_category` | 0.0236 | 12 |
| `dim_market_type` | 0.0235 | 2 |
| `dim_prior_settlements_bin__event_slug` | 0.0229 | 3 |
| `dim_outcomes_per_event` | 0.0223 | 3 |
| `dim_text_neighbors_strict` | 0.0217 | 3 |
| `dim_family_vol_tier` | 0.0194 | 3 |
| `dim_subject_specificity` | 0.0179 | 3 |
| `dim_group_strict_size` | 0.0137 | 3 |
| `dim_resolution_type` | 0.0136 | 2 |
| `dim_market_specificity` | 0.0112 | 3 |
| `dim_event_slug_size` | 0.0102 | 3 |


## Per-dimension deep dive


### 1. `dim_resolution_type` — Does the outcome have an objective data feed?

**Hypothesis**: data-driven outcomes (crypto prices, weather, sports scores) have public reference series -> calibrated; event-observable outcomes (elections, resignations, rulings) -> classic FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| event_observable | 16.8M | +0.0250 (+2.7*) | +0.0360 (+2.1) | +0.0306 (+6.6***) | +0.0322 (+3.3**) | +0.0254 (+3.9***) | +0.0292 (+2.6*) |
| data_driven_numeric | 7.3M | +0.0058 (+0.6) | +0.0213 (+2.0) | +0.0158 (+2.2) | +0.0411 (+5.7***) | +0.0060 (+0.9) | +0.0204 (+3.1**) |
| unknown | — | — | — | — | — | +0.0947 (+16.9***) | +0.1057 (+19.9***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_event_observable_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.037 | 0.032 | -0.0049 | -0.6 | -0.0115 | -0.7 |
| D2 | 0.147 | 0.135 | -0.0114 | -1.1 | -0.0177 | -1.0 |
| D3 | 0.248 | 0.270 | +0.0218 | +1.2 | -0.0044 | -0.2 |
| D4 | 0.352 | 0.272 | -0.0795 | -1.2 | -0.1262 | -1.7 |
| D5 | 0.448 | 0.408 | -0.0402 | -0.8 | -0.0947 | -1.0 |
| D6 | 0.545 | 0.634 | +0.0892 | +1.4 | +0.1341 | +1.7 |
| D7 | 0.642 | 0.782 | +0.1396 | +1.9 | +0.1484 | +1.9 |
| D8 | 0.745 | 0.762 | +0.0169 | +1.1 | +0.0068 | +0.3 |
| D9 | 0.848 | 0.879 | +0.0312 | +2.8* | +0.0131 | +0.6 |
| D10 | 0.961 | 0.981 | +0.0201 | +6.1*** | +0.0245 | +8.5*** |
| **D10-D1** | | | **+0.0250** | **+2.7*** | **+0.0360** | **+2.1** |

_data_driven_numeric_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.042 | 0.038 | -0.0039 | -1.2 | -0.0131 | -1.7 |
| D2 | 0.143 | 0.135 | -0.0077 | -0.9 | -0.0224 | -1.4 |
| D3 | 0.246 | 0.295 | +0.0488 | +1.1 | +0.1237 | +1.4 |
| D4 | 0.346 | 0.398 | +0.0525 | +1.2 | +0.1704 | +1.6 |
| D5 | 0.452 | 0.480 | +0.0276 | +2.2 | +0.0454 | +1.8 |
| D6 | 0.536 | 0.542 | +0.0055 | +0.3 | -0.0441 | -1.0 |
| D7 | 0.646 | 0.603 | -0.0433 | -0.6 | -0.1802 | -1.4 |
| D8 | 0.743 | 0.640 | -0.1030 | -1.0 | -0.1532 | -1.2 |
| D9 | 0.849 | 0.872 | +0.0224 | +2.6* | +0.0363 | +2.6* |
| D10 | 0.961 | 0.963 | +0.0019 | +0.2 | +0.0083 | +1.1 |
| **D10-D1** | | | **+0.0058** | **+0.6** | **+0.0213** | **+2.0** |


### 2. `dim_info_type_supergroup` — Finer-grained classification of the data source

**Hypothesis**: market_data / weather_data -> calibrated; politics_governance / awards -> strong FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| sports_data | 7.9M | -0.0063 (-0.3) | -0.0331 (-0.6) | +0.0203 (+6.3***) | +0.0108 (+1.6) | -0.0009 (-0.1) | -0.0295 (-1.0) |
| politics_governance | 5.7M | +0.0456 (+8.1***) | +0.0581 (+5.6***) | +0.0550 (+5.7***) | +0.0817 (+7.8***) | +0.0476 (+7.4***) | +0.0544 (+5.6***) |
| other | 5.7M | +0.0443 (+10.4***) | +0.0579 (+10.3***) | +0.0317 (+3.8***) | +0.0288 (+1.6) | +0.0349 (+7.7***) | +0.0462 (+6.0***) |
| market_data | 2.4M | +0.0000 (+0.0) | +0.0324 (+3.3**) | -0.0084 (-0.5) | +0.0251 (+2.5) | +0.0026 (+0.2) | +0.0292 (+4.1***) |
| culture_media | 1.5M | -0.0132 (-1.3) | -0.0164 (-1.3) | +0.0300 (+3.4**) | +0.0227 (+1.6) | -0.0027 (-0.4) | -0.0053 (-0.6) |
| weather_data | 673K | -0.0140 (-2.4) | -0.0191 (-1.1) | +0.0120 (+2.4) | +0.0102 (+1.5) | -0.0017 (-0.4) | -0.0096 (-0.9) |
| awards | 236K | +0.0041 (+0.1) | +0.0743 (+3.1**) | +0.0537 (+4.8***) | +0.0814 (+9.9***) | +0.0275 (+1.4) | +0.0703 (+5.6***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_sports_data_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.036 | 0.051 | +0.0157 | +0.8 | +0.0402 | +0.8 |
| D2 | 0.148 | 0.145 | -0.0035 | -0.3 | -0.0132 | -0.8 |
| D3 | 0.248 | 0.277 | +0.0288 | +1.2 | -0.0129 | -0.8 |
| D4 | 0.348 | 0.400 | +0.0519 | +3.8*** | +0.0453 | +1.9 |
| D5 | 0.450 | 0.497 | +0.0463 | +5.2*** | +0.0407 | +3.1** |
| D6 | 0.539 | 0.548 | +0.0091 | +1.1 | +0.0078 | +0.6 |
| D7 | 0.642 | 0.668 | +0.0261 | +2.2 | +0.0303 | +1.5 |
| D8 | 0.744 | 0.779 | +0.0348 | +2.6* | +0.0220 | +0.9 |
| D9 | 0.846 | 0.878 | +0.0324 | +3.7*** | +0.0268 | +1.5 |
| D10 | 0.960 | 0.970 | +0.0094 | +1.2 | +0.0071 | +0.8 |
| **D10-D1** | | | **-0.0063** | **-0.3** | **-0.0331** | **-0.6** |

_politics_governance_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.019 | -0.0199 | -4.6*** | -0.0327 | -4.0*** |
| D2 | 0.144 | 0.134 | -0.0099 | -0.4 | -0.0197 | -0.5 |
| D3 | 0.252 | 0.399 | +0.1471 | +1.7 | +0.1265 | +1.1 |
| D4 | 0.358 | 0.154 | -0.2039 | -2.3 | -0.1886 | -1.8 |
| D5 | 0.452 | 0.280 | -0.1719 | -1.8 | -0.1649 | -1.2 |
| D6 | 0.549 | 0.763 | +0.2136 | +2.1 | +0.1972 | +2.0 |
| D7 | 0.641 | 0.861 | +0.2198 | +2.1 | +0.1720 | +1.3 |
| D8 | 0.740 | 0.504 | -0.2362 | -1.7 | -0.1644 | -1.3 |
| D9 | 0.852 | 0.887 | +0.0342 | +1.0 | -0.0207 | -0.4 |
| D10 | 0.963 | 0.989 | +0.0256 | +7.2*** | +0.0254 | +4.0*** |
| **D10-D1** | | | **+0.0456** | **+8.1***** | **+0.0581** | **+5.6***** |

_other_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.019 | -0.0200 | -6.9*** | -0.0311 | -7.2*** |
| D2 | 0.145 | 0.123 | -0.0223 | -1.9 | -0.0219 | -0.9 |
| D3 | 0.246 | 0.256 | +0.0100 | +0.4 | +0.0102 | +0.3 |
| D4 | 0.345 | 0.349 | +0.0040 | +0.2 | +0.0118 | +0.4 |
| D5 | 0.444 | 0.443 | -0.0005 | -0.0 | -0.0029 | -0.1 |
| D6 | 0.544 | 0.559 | +0.0155 | +1.3 | +0.0266 | +1.1 |
| D7 | 0.647 | 0.666 | +0.0193 | +0.8 | +0.0056 | +0.1 |
| D8 | 0.746 | 0.750 | +0.0044 | +0.2 | +0.0099 | +0.2 |
| D9 | 0.848 | 0.879 | +0.0306 | +2.0 | +0.0357 | +1.2 |
| D10 | 0.961 | 0.985 | +0.0243 | +7.8*** | +0.0268 | +7.5*** |
| **D10-D1** | | | **+0.0443** | **+10.4***** | **+0.0579** | **+10.3***** |


### 3. `dim_primary_category` — Polymarket's 13-category taxonomy

**Hypothesis**: high-information categories calibrated; thin/one-off categories (Politics, Geopolitics, Finance) strong FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Sports | 6.9M | -0.0064 (-0.3) | -0.0343 (-0.6) | +0.0217 (+6.4***) | +0.0101 (+1.3) | -0.0002 (-0.0) | -0.0323 (-1.0) |
| Politics | 6.8M | +0.0438 (+8.7***) | +0.0599 (+7.1***) | +0.0518 (+6.5***) | +0.0748 (+7.5***) | +0.0454 (+8.2***) | +0.0563 (+7.2***) |
| Crypto | 2.7M | +0.0030 (+0.2) | +0.0337 (+3.7***) | -0.0017 (-0.1) | +0.0283 (+2.6*) | +0.0063 (+0.5) | +0.0337 (+5.1***) |
| Tech | 1.7M | +0.0128 (+1.5) | +0.0236 (+2.5*) | +0.0359 (+4.4***) | +0.0373 (+3.1**) | +0.0078 (+1.1) | +0.0184 (+2.4) |
| Finance | 1.1M | +0.0511 (+12.1***) | +0.0623 (+10.3***) | +0.0563 (+9.1***) | +0.0773 (+12.2***) | +0.0483 (+10.8***) | +0.0642 (+12.2***) |
| Geopolitics | 1.0M | +0.0510 (+8.8***) | +0.0712 (+13.1***) | +0.0462 (+5.6***) | +0.0621 (+4.6***) | +0.0443 (+7.6***) | +0.0635 (+9.6***) |
| Esports | 994K | -0.0080 (-0.6) | -0.0142 (-0.6) | +0.0132 (+1.5) | +0.0151 (+1.3) | -0.0068 (-0.5) | -0.0042 (-0.4) |
| Culture | 896K | +0.0443 (+6.5***) | +0.0577 (+7.5***) | +0.0445 (+6.2***) | +0.0511 (+4.9***) | +0.0329 (+6.1***) | +0.0365 (+4.3***) |
| Iran | 851K | +0.0084 (+0.3) | +0.0244 (+0.8) | -0.0441 (-1.2) | -0.0772 (-1.3) | -0.0037 (-0.2) | -0.0159 (-0.5) |
| Weather | 743K | -0.0130 (-2.0) | -0.0106 (-0.7) | +0.0107 (+2.2) | +0.0013 (+0.1) | -0.0014 (-0.3) | -0.0095 (-0.8) |
| Mentions | 311K | +0.0219 (+2.4) | +0.0573 (+6.1***) | +0.0153 (+1.9) | +0.0249 (+2.4) | +0.0079 (+1.0) | +0.0272 (+3.2**) |
| Economy | 138K | +0.0217 (+1.4) | -0.0015 (-0.0) | +0.0306 (+2.3) | +0.0260 (+1.1) | +0.0239 (+2.1) | +0.0182 (+0.5) |
| Uncategorized | — | — | — | — | — | +0.0947 (+16.9***) | +0.1057 (+19.9***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Sports_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.035 | 0.050 | +0.0155 | +0.8 | +0.0410 | +0.8 |
| D2 | 0.149 | 0.136 | -0.0122 | -0.8 | -0.0162 | -0.8 |
| D3 | 0.248 | 0.268 | +0.0198 | +0.7 | -0.0294 | -1.6 |
| D4 | 0.348 | 0.399 | +0.0506 | +3.1** | +0.0459 | +1.7 |
| D5 | 0.451 | 0.495 | +0.0444 | +4.4*** | +0.0432 | +2.9* |
| D6 | 0.538 | 0.540 | +0.0018 | +0.2 | +0.0026 | +0.2 |
| D7 | 0.642 | 0.665 | +0.0223 | +1.6 | +0.0257 | +1.1 |
| D8 | 0.744 | 0.777 | +0.0332 | +2.1 | +0.0160 | +0.5 |
| D9 | 0.847 | 0.881 | +0.0349 | +3.6*** | +0.0281 | +1.4 |
| D10 | 0.961 | 0.971 | +0.0091 | +1.1 | +0.0067 | +0.7 |
| **D10-D1** | | | **-0.0064** | **-0.3** | **-0.0343** | **-0.6** |

_Politics_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.019 | -0.0194 | -5.1*** | -0.0328 | -4.6*** |
| D2 | 0.144 | 0.131 | -0.0132 | -0.6 | -0.0182 | -0.6 |
| D3 | 0.251 | 0.365 | +0.1136 | +1.5 | +0.1084 | +1.1 |
| D4 | 0.357 | 0.164 | -0.1938 | -2.2 | -0.1848 | -1.8 |
| D5 | 0.452 | 0.284 | -0.1675 | -1.8 | -0.1648 | -1.2 |
| D6 | 0.549 | 0.756 | +0.2071 | +2.0 | +0.1952 | +2.0 |
| D7 | 0.641 | 0.851 | +0.2092 | +2.0 | +0.1636 | +1.3 |
| D8 | 0.741 | 0.555 | -0.1858 | -1.5 | -0.1363 | -1.3 |
| D9 | 0.851 | 0.885 | +0.0339 | +1.3 | -0.0063 | -0.1 |
| D10 | 0.964 | 0.988 | +0.0244 | +7.4*** | +0.0272 | +6.0*** |
| **D10-D1** | | | **+0.0438** | **+8.7***** | **+0.0599** | **+7.1***** |

_Crypto_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.040 | 0.029 | -0.0112 | -2.5 | -0.0197 | -2.8* |
| D2 | 0.145 | 0.138 | -0.0074 | -0.6 | -0.0153 | -0.7 |
| D3 | 0.245 | 0.205 | -0.0397 | -2.8* | -0.0498 | -2.0 |
| D4 | 0.345 | 0.310 | -0.0346 | -1.9 | -0.0473 | -1.6 |
| D5 | 0.445 | 0.434 | -0.0111 | -0.7 | -0.0380 | -1.7 |
| D6 | 0.543 | 0.553 | +0.0101 | +0.8 | +0.0224 | +1.2 |
| D7 | 0.647 | 0.698 | +0.0509 | +1.9 | +0.0478 | +1.6 |
| D8 | 0.746 | 0.785 | +0.0391 | +1.8 | +0.0281 | +1.2 |
| D9 | 0.849 | 0.856 | +0.0074 | +0.5 | +0.0226 | +1.2 |
| D10 | 0.963 | 0.954 | -0.0083 | -0.5 | +0.0140 | +2.5* |
| **D10-D1** | | | **+0.0030** | **+0.2** | **+0.0337** | **+3.7***** |


### 4. `dim_subject_specificity` — How many entities does the event resolve on?

**Hypothesis**: more subjects = compound = harder to price = stronger FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 subject | 15.3M | +0.0074 (+0.6) | +0.0248 (+1.2) | +0.0233 (+4.6***) | +0.0431 (+6.3***) | +0.0134 (+1.6) | +0.0220 (+1.7) |
| 2 subjects | 6.0M | +0.0272 (+4.8***) | +0.0399 (+4.9***) | +0.0275 (+3.5***) | +0.0275 (+2.3) | +0.0216 (+4.5***) | +0.0326 (+4.5***) |
| 3+ subjects | 2.8M | +0.0431 (+7.8***) | +0.0590 (+8.4***) | +0.0315 (+3.2**) | +0.0134 (+0.4) | +0.0362 (+6.7***) | +0.0367 (+2.8*) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_1 subject_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.037 | 0.040 | +0.0031 | +0.3 | -0.0050 | -0.2 |
| D2 | 0.146 | 0.133 | -0.0127 | -1.3 | -0.0347 | -1.9 |
| D3 | 0.248 | 0.296 | +0.0478 | +1.6 | +0.0339 | +0.7 |
| D4 | 0.352 | 0.278 | -0.0738 | -1.1 | -0.1102 | -1.2 |
| D5 | 0.451 | 0.419 | -0.0322 | -0.6 | -0.0788 | -0.8 |
| D6 | 0.542 | 0.625 | +0.0829 | +1.3 | +0.1101 | +1.4 |
| D7 | 0.642 | 0.774 | +0.1315 | +1.6 | +0.1220 | +1.3 |
| D8 | 0.743 | 0.715 | -0.0279 | -0.5 | -0.0354 | -0.6 |
| D9 | 0.848 | 0.885 | +0.0364 | +3.2** | +0.0264 | +1.1 |
| D10 | 0.962 | 0.972 | +0.0105 | +1.6 | +0.0198 | +5.4*** |
| **D10-D1** | | | **+0.0074** | **+0.6** | **+0.0248** | **+1.2** |

_2 subjects_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.042 | 0.030 | -0.0117 | -3.5*** | -0.0201 | -3.4** |
| D2 | 0.144 | 0.140 | -0.0033 | -0.3 | +0.0120 | +0.6 |
| D3 | 0.245 | 0.245 | +0.0000 | +0.0 | +0.0195 | +0.5 |
| D4 | 0.345 | 0.359 | +0.0133 | +0.7 | +0.0235 | +0.7 |
| D5 | 0.444 | 0.457 | +0.0132 | +0.6 | -0.0109 | -0.3 |
| D6 | 0.544 | 0.552 | +0.0081 | +0.5 | +0.0113 | +0.5 |
| D7 | 0.646 | 0.642 | -0.0038 | -0.2 | -0.0320 | -0.7 |
| D8 | 0.746 | 0.738 | -0.0081 | -0.4 | -0.0382 | -0.9 |
| D9 | 0.849 | 0.862 | +0.0131 | +1.0 | +0.0057 | +0.3 |
| D10 | 0.960 | 0.976 | +0.0155 | +3.4** | +0.0198 | +3.5*** |
| **D10-D1** | | | **+0.0272** | **+4.8***** | **+0.0399** | **+4.9***** |

_3+ subjects_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.038 | 0.020 | -0.0180 | -3.8*** | -0.0296 | -4.7*** |
| D2 | 0.146 | 0.129 | -0.0162 | -0.8 | -0.0198 | -0.5 |
| D3 | 0.246 | 0.263 | +0.0167 | +0.5 | +0.0264 | +0.4 |
| D4 | 0.347 | 0.356 | +0.0090 | +0.3 | +0.0112 | +0.3 |
| D5 | 0.443 | 0.444 | +0.0005 | +0.0 | -0.0191 | -0.6 |
| D6 | 0.545 | 0.554 | +0.0085 | +0.4 | +0.0131 | +0.5 |
| D7 | 0.644 | 0.683 | +0.0386 | +1.4 | +0.0398 | +1.0 |
| D8 | 0.745 | 0.743 | -0.0019 | -0.0 | -0.0032 | -0.0 |
| D9 | 0.848 | 0.877 | +0.0288 | +1.3 | +0.0114 | +0.2 |
| D10 | 0.960 | 0.985 | +0.0251 | +8.5*** | +0.0294 | +9.7*** |
| **D10-D1** | | | **+0.0431** | **+7.8***** | **+0.0590** | **+8.4***** |


### 5. `dim_event_family_size` — How many contracts share the event template?

**Hypothesis**: large families (NBA games, daily Fed contracts) -> repeated-play learning -> calibrated; singletons/small families -> FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Small 2-20 | 9.2M | +0.0441 (+11.1***) | +0.0580 (+8.0***) | +0.0304 (+3.6***) | +0.0492 (+3.8***) | +0.0368 (+6.6***) | +0.0507 (+7.3***) |
| Large 1K+ | 6.6M | -0.0193 (-2.1) | -0.0152 (-1.6) | +0.0064 (+1.6) | -0.0053 (-0.7) | -0.0081 (-1.7) | -0.0132 (-2.5) |
| Medium 21-1K | 5.7M | -0.0025 (-0.1) | -0.0009 (-0.0) | +0.0331 (+3.8***) | +0.0398 (+2.3) | +0.0061 (+0.5) | +0.0030 (+0.1) |
| Singleton 1 | 2.6M | +0.0476 (+9.9***) | +0.0567 (+5.9***) | +0.0456 (+8.2***) | +0.0523 (+5.6***) | +0.0428 (+10.8***) | +0.0493 (+6.5***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Small 2-20_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.041 | 0.022 | -0.0188 | -6.7*** | -0.0327 | -5.4*** |
| D2 | 0.146 | 0.135 | -0.0108 | -0.7 | -0.0179 | -0.7 |
| D3 | 0.247 | 0.308 | +0.0608 | +1.3 | +0.0615 | +0.9 |
| D4 | 0.355 | 0.206 | -0.1490 | -1.8 | -0.1715 | -1.8 |
| D5 | 0.450 | 0.327 | -0.1233 | -1.6 | -0.1511 | -1.2 |
| D6 | 0.548 | 0.710 | +0.1619 | +1.7 | +0.1843 | +2.0 |
| D7 | 0.642 | 0.822 | +0.1794 | +1.8 | +0.1646 | +1.5 |
| D8 | 0.744 | 0.665 | -0.0790 | -0.9 | -0.0731 | -0.8 |
| D9 | 0.849 | 0.887 | +0.0375 | +2.3 | +0.0237 | +0.8 |
| D10 | 0.962 | 0.987 | +0.0253 | +9.0*** | +0.0253 | +6.4*** |
| **D10-D1** | | | **+0.0441** | **+11.1***** | **+0.0580** | **+8.0***** |

_Large 1K+_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.047 | 0.060 | +0.0129 | +2.6* | +0.0131 | +2.0 |
| D2 | 0.145 | 0.158 | +0.0126 | +1.4 | +0.0005 | +0.0 |
| D3 | 0.246 | 0.267 | +0.0201 | +2.3 | -0.0136 | -0.8 |
| D4 | 0.348 | 0.381 | +0.0331 | +3.2** | +0.0299 | +1.2 |
| D5 | 0.450 | 0.493 | +0.0433 | +5.1*** | +0.0398 | +3.0* |
| D6 | 0.538 | 0.551 | +0.0129 | +1.8 | +0.0158 | +1.2 |
| D7 | 0.642 | 0.661 | +0.0184 | +1.9 | +0.0221 | +1.2 |
| D8 | 0.743 | 0.761 | +0.0183 | +2.0 | +0.0094 | +0.3 |
| D9 | 0.846 | 0.850 | +0.0046 | +0.5 | -0.0164 | -0.9 |
| D10 | 0.954 | 0.947 | -0.0064 | -0.8 | -0.0021 | -0.3 |
| **D10-D1** | | | **-0.0193** | **-2.1** | **-0.0152** | **-1.6** |

_Medium 21-1K_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.034 | 0.039 | +0.0047 | +0.3 | +0.0163 | +0.5 |
| D2 | 0.144 | 0.114 | -0.0301 | -2.3 | -0.0306 | -1.1 |
| D3 | 0.248 | 0.261 | +0.0138 | +0.3 | +0.0270 | +0.6 |
| D4 | 0.344 | 0.390 | +0.0459 | +1.7 | +0.0807 | +2.1 |
| D5 | 0.447 | 0.478 | +0.0311 | +1.5 | +0.0145 | +0.4 |
| D6 | 0.541 | 0.536 | -0.0056 | -0.3 | -0.0464 | -1.4 |
| D7 | 0.646 | 0.661 | +0.0149 | +0.6 | -0.0656 | -1.2 |
| D8 | 0.746 | 0.761 | +0.0143 | +0.5 | -0.0233 | -0.4 |
| D9 | 0.850 | 0.879 | +0.0288 | +1.7 | +0.0218 | +0.6 |
| D10 | 0.963 | 0.965 | +0.0022 | +0.2 | +0.0154 | +3.0** |
| **D10-D1** | | | **-0.0025** | **-0.1** | **-0.0009** | **-0.0** |


### 6. `dim_outcomes_per_event` — Distinct markets (condition_id) per event_slug (binary = 1 market)

**Hypothesis**: binary events easiest to calibrate; multi-outcome hardest.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Many 6+ | 14.8M | +0.0027 (+0.3) | +0.0186 (+0.9) | +0.0240 (+4.1***) | +0.0345 (+2.5*) | +0.0068 (+1.0) | +0.0141 (+1.0) |
| Few 2-5 | 4.1M | +0.0474 (+14.9***) | +0.0636 (+13.0***) | +0.0247 (+5.5***) | +0.0429 (+5.2***) | +0.0380 (+5.6***) | +0.0472 (+7.7***) |
| Binary 1 | 3.0M | +0.0239 (+2.5) | +0.0386 (+3.6***) | -0.0070 (-0.5) | -0.0146 (-0.9) | +0.0157 (+1.9) | +0.0220 (+2.3) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Many 6+_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.041 | +0.0023 | +0.3 | -0.0029 | -0.1 |
| D2 | 0.144 | 0.140 | -0.0044 | -0.5 | -0.0048 | -0.2 |
| D3 | 0.247 | 0.300 | +0.0523 | +1.7 | +0.0691 | +1.2 |
| D4 | 0.352 | 0.262 | -0.0906 | -1.2 | -0.1400 | -1.5 |
| D5 | 0.452 | 0.398 | -0.0539 | -0.9 | -0.1071 | -1.0 |
| D6 | 0.543 | 0.650 | +0.1068 | +1.5 | +0.1368 | +1.6 |
| D7 | 0.642 | 0.780 | +0.1381 | +1.5 | +0.1300 | +1.1 |
| D8 | 0.743 | 0.678 | -0.0646 | -1.1 | -0.1065 | -1.6 |
| D9 | 0.850 | 0.867 | +0.0166 | +1.2 | -0.0184 | -0.6 |
| D10 | 0.961 | 0.966 | +0.0051 | +0.9 | +0.0157 | +3.4** |
| **D10-D1** | | | **+0.0027** | **+0.3** | **+0.0186** | **+0.9** |

_Few 2-5_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.040 | 0.021 | -0.0189 | -7.3*** | -0.0355 | -7.8*** |
| D2 | 0.148 | 0.118 | -0.0300 | -3.4** | -0.0658 | -4.6*** |
| D3 | 0.247 | 0.231 | -0.0159 | -1.0 | -0.0572 | -1.7 |
| D4 | 0.347 | 0.372 | +0.0247 | +1.3 | +0.0462 | +1.2 |
| D5 | 0.448 | 0.470 | +0.0226 | +1.4 | +0.0202 | +0.8 |
| D6 | 0.541 | 0.541 | -0.0006 | -0.0 | -0.0050 | -0.2 |
| D7 | 0.643 | 0.691 | +0.0484 | +3.0* | +0.0725 | +2.2 |
| D8 | 0.746 | 0.805 | +0.0591 | +4.1*** | +0.1047 | +3.4** |
| D9 | 0.846 | 0.907 | +0.0608 | +7.1*** | +0.0899 | +8.6*** |
| D10 | 0.961 | 0.990 | +0.0284 | +15.8*** | +0.0281 | +15.5*** |
| **D10-D1** | | | **+0.0474** | **+14.9***** | **+0.0636** | **+13.0***** |

_Binary 1_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.034 | -0.0089 | -1.4 | -0.0183 | -2.2 |
| D2 | 0.149 | 0.159 | +0.0096 | +0.6 | +0.0038 | +0.1 |
| D3 | 0.247 | 0.306 | +0.0584 | +2.1 | +0.0419 | +1.1 |
| D4 | 0.347 | 0.375 | +0.0277 | +1.3 | +0.0648 | +2.0 |
| D5 | 0.444 | 0.479 | +0.0352 | +1.9 | +0.0562 | +2.7* |
| D6 | 0.544 | 0.549 | +0.0052 | +0.5 | +0.0138 | +0.6 |
| D7 | 0.645 | 0.648 | +0.0031 | +0.1 | -0.0258 | -0.6 |
| D8 | 0.744 | 0.724 | -0.0199 | -0.6 | -0.0186 | -0.4 |
| D9 | 0.845 | 0.855 | +0.0093 | +0.5 | +0.0167 | +0.7 |
| D10 | 0.962 | 0.977 | +0.0150 | +2.1 | +0.0203 | +3.2** |
| **D10-D1** | | | **+0.0239** | **+2.5** | **+0.0386** | **+3.6***** |


### 7. `dim_market_specificity` — Does the specific market narrow its parent event?

**Hypothesis**: narrow markets within broad events -> FLB on the longshot legs.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Market = Event | 13.9M | +0.0154 (+1.8) | +0.0115 (+0.4) | +0.0138 (+2.7*) | +0.0119 (+1.2) | +0.0128 (+2.5) | +0.0133 (+1.1) |
| Market narrower | 10.0M | +0.0234 (+2.3) | +0.0528 (+6.9***) | +0.0497 (+10.5***) | +0.0775 (+11.7***) | +0.0284 (+3.5**) | +0.0463 (+4.6***) |
| Market broader/equal | 139K | +0.0376 (+3.2**) | +0.0572 (+3.8***) | +0.0089 (+0.6) | -0.0297 (-0.5) | +0.0032 (+0.2) | -0.0273 (-0.8) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Market = Event_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.042 | 0.037 | -0.0048 | -0.7 | +0.0066 | +0.3 |
| D2 | 0.146 | 0.137 | -0.0082 | -1.2 | -0.0158 | -1.2 |
| D3 | 0.246 | 0.249 | +0.0028 | +0.3 | -0.0084 | -0.5 |
| D4 | 0.347 | 0.361 | +0.0144 | +1.6 | +0.0239 | +1.4 |
| D5 | 0.445 | 0.479 | +0.0344 | +4.5*** | +0.0361 | +2.7* |
| D6 | 0.543 | 0.548 | +0.0045 | +0.6 | +0.0047 | +0.4 |
| D7 | 0.644 | 0.668 | +0.0233 | +2.3 | +0.0155 | +0.9 |
| D8 | 0.745 | 0.759 | +0.0142 | +1.1 | +0.0018 | +0.1 |
| D9 | 0.847 | 0.868 | +0.0202 | +2.4 | +0.0152 | +0.9 |
| D10 | 0.960 | 0.970 | +0.0106 | +2.0 | +0.0181 | +4.3*** |
| **D10-D1** | | | **+0.0154** | **+1.8** | **+0.0115** | **+0.4** |

_Market narrower_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.035 | 0.031 | -0.0042 | -0.5 | -0.0278 | -4.2*** |
| D2 | 0.144 | 0.132 | -0.0125 | -0.8 | -0.0250 | -0.8 |
| D3 | 0.250 | 0.346 | +0.0955 | +1.8 | +0.0981 | +1.1 |
| D4 | 0.355 | 0.219 | -0.1365 | -1.5 | -0.1688 | -1.6 |
| D5 | 0.455 | 0.362 | -0.0926 | -1.3 | -0.1256 | -1.1 |
| D6 | 0.542 | 0.682 | +0.1396 | +1.6 | +0.1528 | +1.7 |
| D7 | 0.642 | 0.827 | +0.1849 | +1.8 | +0.1633 | +1.3 |
| D8 | 0.742 | 0.645 | -0.0966 | -1.0 | -0.0900 | -0.9 |
| D9 | 0.851 | 0.898 | +0.0464 | +2.6* | +0.0245 | +0.6 |
| D10 | 0.963 | 0.983 | +0.0192 | +4.1*** | +0.0250 | +7.0*** |
| **D10-D1** | | | **+0.0234** | **+2.3** | **+0.0528** | **+6.9***** |

_Market broader/equal_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.045 | 0.033 | -0.0120 | -1.3 | -0.0313 | -2.5* |
| D2 | 0.148 | 0.106 | -0.0418 | -1.5 | +0.0209 | +0.3 |
| D3 | 0.242 | 0.181 | -0.0612 | -1.6 | -0.1171 | -2.6* |
| D4 | 0.347 | 0.403 | +0.0566 | +0.9 | -0.1720 | -1.5 |
| D5 | 0.443 | 0.375 | -0.0688 | -1.3 | -0.2324 | -2.5 |
| D6 | 0.545 | 0.582 | +0.0375 | +0.5 | +0.0683 | +0.6 |
| D7 | 0.637 | 0.725 | +0.0879 | +1.5 | +0.1247 | +1.9 |
| D8 | 0.746 | 0.817 | +0.0707 | +2.0 | +0.1367 | +3.7*** |
| D9 | 0.842 | 0.894 | +0.0514 | +1.4 | -0.0181 | -0.2 |
| D10 | 0.955 | 0.981 | +0.0256 | +3.7*** | +0.0259 | +2.9* |
| **D10-D1** | | | **+0.0376** | **+3.2**** | **+0.0572** | **+3.8***** |


### 8. `dim_dollar_volume_tier` — Quartiles of per-token dollar volume

**Hypothesis**: high-volume contracts attract pros + arbitrage -> calibrated; thin contracts -> retail-only -> FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Q4 (>$2,007) | 22.0M | +0.0137 (+1.7) | +0.0316 (+2.3) | +0.0210 (+4.7***) | +0.0327 (+4.0***) | +0.0130 (+2.3) | +0.0252 (+2.9*) |
| Q3 ($255-$2,007) | 1.5M | +0.0162 (+4.5***) | +0.0381 (+13.3***) | +0.0036 (+1.0) | +0.0176 (+4.9***) | +0.0121 (+4.8***) | +0.0241 (+9.2***) |
| Q2 ($32-$255) | 449K | +0.0236 (+4.8***) | +0.0350 (+6.1***) | +0.0159 (+4.4***) | +0.0289 (+7.4***) | +0.0143 (+2.8*) | +0.0291 (+8.2***) |
| Q1 (<=$32) | 81K | -0.1891 (-1.4) | -0.2729 (-1.5) | -0.0155 (-1.6) | -0.0060 (-0.5) | -0.0524 (-3.0**) | -0.0431 (-2.1) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Q4 (>$2,007)_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.040 | +0.0010 | +0.1 | -0.0105 | -0.8 |
| D2 | 0.146 | 0.150 | +0.0039 | +0.5 | -0.0162 | -1.1 |
| D3 | 0.248 | 0.299 | +0.0512 | +2.4 | +0.0331 | +0.9 |
| D4 | 0.351 | 0.307 | -0.0439 | -0.7 | -0.0831 | -1.1 |
| D5 | 0.449 | 0.432 | -0.0172 | -0.4 | -0.0708 | -0.8 |
| D6 | 0.543 | 0.623 | +0.0796 | +1.5 | +0.1007 | +1.3 |
| D7 | 0.643 | 0.756 | +0.1130 | +1.6 | +0.1006 | +1.2 |
| D8 | 0.744 | 0.727 | -0.0170 | -0.5 | -0.0309 | -0.7 |
| D9 | 0.849 | 0.880 | +0.0313 | +3.6*** | +0.0187 | +1.0 |
| D10 | 0.961 | 0.976 | +0.0146 | +3.9*** | +0.0211 | +7.3*** |
| **D10-D1** | | | **+0.0137** | **+1.7** | **+0.0316** | **+2.3** |

_Q3 ($255-$2,007)_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.037 | 0.010 | -0.0270 | -30.8*** | -0.0409 | -40.7*** |
| D2 | 0.143 | 0.058 | -0.0853 | -31.4*** | -0.1049 | -43.2*** |
| D3 | 0.244 | 0.130 | -0.1134 | -22.5*** | -0.1361 | -29.2*** |
| D4 | 0.344 | 0.210 | -0.1340 | -22.0*** | -0.1462 | -17.0*** |
| D5 | 0.448 | 0.343 | -0.1047 | -15.5*** | -0.1087 | -12.7*** |
| D6 | 0.536 | 0.457 | -0.0792 | -11.4*** | -0.0766 | -7.8*** |
| D7 | 0.643 | 0.550 | -0.0930 | -12.4*** | -0.1016 | -10.5*** |
| D8 | 0.743 | 0.683 | -0.0599 | -8.4*** | -0.0709 | -8.6*** |
| D9 | 0.845 | 0.813 | -0.0322 | -5.0*** | -0.0245 | -3.8*** |
| D10 | 0.951 | 0.940 | -0.0108 | -3.1** | -0.0027 | -1.0 |
| **D10-D1** | | | **+0.0162** | **+4.5***** | **+0.0381** | **+13.3***** |

_Q2 ($32-$255)_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.006 | -0.0328 | -20.1*** | -0.0420 | -48.4*** |
| D2 | 0.140 | 0.027 | -0.1123 | -48.5*** | -0.1142 | -42.9*** |
| D3 | 0.241 | 0.072 | -0.1688 | -43.1*** | -0.1670 | -34.4*** |
| D4 | 0.342 | 0.143 | -0.1991 | -31.2*** | -0.1979 | -29.3*** |
| D5 | 0.448 | 0.294 | -0.1546 | -14.7*** | -0.1608 | -14.2*** |
| D6 | 0.534 | 0.396 | -0.1382 | -9.4*** | -0.0951 | -2.2 |
| D7 | 0.637 | 0.515 | -0.1222 | -3.5** | -0.0281 | -0.3 |
| D8 | 0.741 | 0.623 | -0.1180 | -9.0*** | -0.1132 | -8.5*** |
| D9 | 0.843 | 0.775 | -0.0680 | -5.8*** | -0.0580 | -5.1*** |
| D10 | 0.951 | 0.942 | -0.0091 | -2.0 | -0.0070 | -1.2 |
| **D10-D1** | | | **+0.0236** | **+4.8***** | **+0.0350** | **+6.1***** |


### 9. `dim_contract_horizon` — Duration from first to last trade on the contract

**Hypothesis**: medium-horizon best calibrated; ultra-short and ultra-long both worse.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| >1mo | 13.4M | +0.0272 (+2.9*) | +0.0385 (+2.3) | +0.0456 (+6.1***) | +0.0588 (+4.8***) | +0.0294 (+4.1***) | +0.0381 (+3.4**) |
| 1wk-1mo | 4.8M | -0.0025 (-0.3) | +0.0158 (+1.8) | +0.0158 (+2.8*) | +0.0185 (+2.0) | +0.0003 (+0.1) | +0.0083 (+1.1) |
| 1d-1w | 4.0M | +0.0047 (+0.8) | +0.0080 (+0.8) | +0.0088 (+2.4) | -0.0041 (-0.5) | +0.0044 (+1.2) | -0.0071 (-1.1) |
| 1h-1d | 1.8M | -0.0348 (-4.8***) | -0.0355 (-2.6*) | -0.0012 (-0.3) | -0.0040 (-0.4) | -0.0098 (-2.2) | -0.0149 (-1.9) |
| <1h | 102K | -0.0437 (-1.2) | -0.1736 (-2.6*) | +0.0151 (+0.9) | -0.3406 (-3.7***) | -0.0225 (-1.1) | -0.3270 (-5.0***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_>1mo_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.036 | 0.027 | -0.0089 | -1.1 | -0.0148 | -0.9 |
| D2 | 0.145 | 0.118 | -0.0270 | -2.0 | -0.0253 | -1.3 |
| D3 | 0.249 | 0.304 | +0.0544 | +1.3 | +0.0589 | +1.0 |
| D4 | 0.354 | 0.223 | -0.1309 | -1.6 | -0.1438 | -1.5 |
| D5 | 0.450 | 0.338 | -0.1120 | -1.5 | -0.1391 | -1.1 |
| D6 | 0.548 | 0.694 | +0.1467 | +1.5 | +0.1678 | +1.7 |
| D7 | 0.643 | 0.810 | +0.1671 | +1.8 | +0.1396 | +1.3 |
| D8 | 0.744 | 0.681 | -0.0626 | -0.9 | -0.0624 | -0.9 |
| D9 | 0.851 | 0.895 | +0.0442 | +3.0** | +0.0271 | +1.1 |
| D10 | 0.964 | 0.982 | +0.0183 | +3.9*** | +0.0237 | +6.9*** |
| **D10-D1** | | | **+0.0272** | **+2.9*** | **+0.0385** | **+2.3** |

_1wk-1mo_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.044 | 0.044 | -0.0000 | -0.0 | -0.0066 | -1.0 |
| D2 | 0.145 | 0.138 | -0.0071 | -0.7 | -0.0216 | -1.8 |
| D3 | 0.244 | 0.230 | -0.0148 | -1.2 | -0.0465 | -2.8* |
| D4 | 0.346 | 0.368 | +0.0228 | +1.4 | +0.0251 | +0.8 |
| D5 | 0.445 | 0.484 | +0.0391 | +2.6* | +0.0391 | +1.3 |
| D6 | 0.543 | 0.518 | -0.0249 | -1.5 | -0.0288 | -1.1 |
| D7 | 0.644 | 0.648 | +0.0039 | +0.3 | +0.0087 | +0.4 |
| D8 | 0.746 | 0.749 | +0.0024 | +0.2 | -0.0067 | -0.2 |
| D9 | 0.847 | 0.853 | +0.0057 | +0.5 | -0.0022 | -0.1 |
| D10 | 0.956 | 0.953 | -0.0025 | -0.3 | +0.0092 | +1.7 |
| **D10-D1** | | | **-0.0025** | **-0.3** | **+0.0158** | **+1.8** |

_1d-1w_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.045 | 0.049 | +0.0049 | +1.4 | +0.0048 | +0.6 |
| D2 | 0.146 | 0.155 | +0.0090 | +1.2 | +0.0129 | +0.6 |
| D3 | 0.246 | 0.276 | +0.0306 | +4.2*** | +0.0373 | +2.2 |
| D4 | 0.347 | 0.380 | +0.0332 | +3.6*** | +0.0250 | +0.9 |
| D5 | 0.447 | 0.484 | +0.0368 | +3.0* | +0.0239 | +1.2 |
| D6 | 0.541 | 0.574 | +0.0328 | +3.6*** | +0.0421 | +2.1 |
| D7 | 0.643 | 0.660 | +0.0170 | +1.6 | +0.0185 | +0.8 |
| D8 | 0.743 | 0.768 | +0.0252 | +3.1** | +0.0233 | +1.2 |
| D9 | 0.846 | 0.856 | +0.0102 | +1.1 | -0.0090 | -0.4 |
| D10 | 0.953 | 0.963 | +0.0095 | +2.0 | +0.0128 | +3.0** |
| **D10-D1** | | | **+0.0047** | **+0.8** | **+0.0080** | **+0.8** |


### 10. `dim_recurrence_class` — Heuristic combining family size x time span

**Hypothesis**: Daily -> strongest learning -> calibrated; Episodic -> strongest FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Recurring | 9.1M | +0.0133 (+0.9) | +0.0297 (+1.2) | +0.0483 (+5.4***) | +0.0609 (+3.8***) | +0.0194 (+1.9) | +0.0296 (+1.7) |
| Daily | 7.4M | -0.0168 (-2.2) | -0.0165 (-1.9) | +0.0059 (+1.6) | -0.0060 (-0.8) | -0.0076 (-1.8) | -0.0160 (-3.2**) |
| Episodic | 5.0M | +0.0374 (+6.3***) | +0.0458 (+4.9***) | +0.0151 (+1.7) | +0.0246 (+1.9) | +0.0303 (+3.9***) | +0.0358 (+4.0***) |
| One-off | 2.6M | +0.0476 (+9.9***) | +0.0567 (+5.9***) | +0.0456 (+8.2***) | +0.0523 (+5.6***) | +0.0428 (+10.8***) | +0.0493 (+6.5***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Recurring_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.035 | 0.033 | -0.0021 | -0.2 | -0.0070 | -0.3 |
| D2 | 0.144 | 0.119 | -0.0244 | -1.4 | -0.0261 | -0.9 |
| D3 | 0.250 | 0.349 | +0.0991 | +1.7 | +0.1219 | +1.3 |
| D4 | 0.356 | 0.198 | -0.1586 | -1.7 | -0.1785 | -1.7 |
| D5 | 0.452 | 0.317 | -0.1353 | -1.5 | -0.1619 | -1.2 |
| D6 | 0.548 | 0.731 | +0.1831 | +1.8 | +0.1933 | +2.0 |
| D7 | 0.642 | 0.835 | +0.1927 | +1.8 | +0.1629 | +1.3 |
| D8 | 0.743 | 0.617 | -0.1254 | -1.2 | -0.1506 | -1.5 |
| D9 | 0.852 | 0.896 | +0.0444 | +2.2 | +0.0239 | +0.7 |
| D10 | 0.963 | 0.975 | +0.0112 | +1.4 | +0.0227 | +5.5*** |
| **D10-D1** | | | **+0.0133** | **+0.9** | **+0.0297** | **+1.2** |

_Daily_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.046 | 0.059 | +0.0124 | +2.8* | +0.0140 | +2.2 |
| D2 | 0.145 | 0.159 | +0.0137 | +1.8 | -0.0000 | -0.0 |
| D3 | 0.246 | 0.266 | +0.0203 | +2.6* | -0.0136 | -0.9 |
| D4 | 0.347 | 0.382 | +0.0343 | +3.5*** | +0.0404 | +1.6 |
| D5 | 0.450 | 0.495 | +0.0450 | +5.6*** | +0.0417 | +3.0* |
| D6 | 0.539 | 0.550 | +0.0115 | +1.7 | +0.0107 | +0.8 |
| D7 | 0.643 | 0.661 | +0.0181 | +2.0 | +0.0212 | +1.2 |
| D8 | 0.743 | 0.758 | +0.0153 | +1.8 | +0.0066 | +0.3 |
| D9 | 0.846 | 0.850 | +0.0040 | +0.5 | -0.0154 | -0.9 |
| D10 | 0.953 | 0.949 | -0.0044 | -0.7 | -0.0024 | -0.4 |
| **D10-D1** | | | **-0.0168** | **-2.2** | **-0.0165** | **-1.9** |

_Episodic_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.040 | 0.024 | -0.0164 | -4.8*** | -0.0248 | -3.2** |
| D2 | 0.147 | 0.127 | -0.0199 | -1.9 | -0.0179 | -0.8 |
| D3 | 0.245 | 0.221 | -0.0241 | -1.4 | -0.0300 | -0.9 |
| D4 | 0.345 | 0.335 | -0.0107 | -0.5 | -0.0244 | -0.7 |
| D5 | 0.445 | 0.431 | -0.0136 | -0.6 | -0.0131 | -0.4 |
| D6 | 0.544 | 0.551 | +0.0070 | +0.4 | +0.0056 | +0.2 |
| D7 | 0.645 | 0.694 | +0.0486 | +2.1 | +0.0439 | +1.0 |
| D8 | 0.747 | 0.790 | +0.0428 | +1.9 | +0.0496 | +1.1 |
| D9 | 0.848 | 0.875 | +0.0277 | +2.0 | +0.0242 | +0.7 |
| D10 | 0.962 | 0.983 | +0.0210 | +4.3*** | +0.0210 | +4.1*** |
| **D10-D1** | | | **+0.0374** | **+6.3***** | **+0.0458** | **+4.9***** |


### 11. `dim_group_strict_size` — Family size on event_slug x market_template grouping

**Hypothesis**: as event_family_size, tighter grouping.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Singleton 1 | 17.8M | +0.0228 (+2.7*) | +0.0336 (+1.9) | +0.0308 (+7.6***) | +0.0393 (+5.7***) | +0.0232 (+3.9***) | +0.0280 (+2.6*) |
| Small 2-20 | 5.0M | +0.0138 (+1.2) | +0.0322 (+3.3**) | +0.0146 (+1.6) | +0.0467 (+5.7***) | +0.0108 (+1.2) | +0.0308 (+4.3***) |
| Medium 21-1K | 1.3M | -0.0041 (-0.3) | +0.0240 (+2.0) | +0.0115 (+0.7) | -0.0596 (-0.9) | +0.0012 (+0.1) | -0.0072 (-0.2) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Singleton 1_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.037 | 0.034 | -0.0034 | -0.4 | -0.0104 | -0.6 |
| D2 | 0.146 | 0.137 | -0.0095 | -1.0 | -0.0223 | -1.3 |
| D3 | 0.248 | 0.295 | +0.0471 | +1.9 | +0.0355 | +0.8 |
| D4 | 0.352 | 0.290 | -0.0616 | -0.9 | -0.0940 | -1.1 |
| D5 | 0.448 | 0.414 | -0.0338 | -0.7 | -0.0880 | -0.9 |
| D6 | 0.545 | 0.626 | +0.0811 | +1.2 | +0.1151 | +1.4 |
| D7 | 0.642 | 0.759 | +0.1166 | +1.5 | +0.1067 | +1.1 |
| D8 | 0.743 | 0.714 | -0.0295 | -0.7 | -0.0416 | -0.8 |
| D9 | 0.848 | 0.882 | +0.0338 | +3.4** | +0.0216 | +1.1 |
| D10 | 0.962 | 0.981 | +0.0193 | +6.2*** | +0.0232 | +7.5*** |
| **D10-D1** | | | **+0.0228** | **+2.7*** | **+0.0336** | **+1.9** |

_Small 2-20_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.042 | 0.029 | -0.0130 | -3.8*** | -0.0187 | -2.5* |
| D2 | 0.144 | 0.130 | -0.0143 | -1.3 | -0.0305 | -1.7 |
| D3 | 0.244 | 0.226 | -0.0181 | -1.3 | -0.0237 | -0.9 |
| D4 | 0.345 | 0.340 | -0.0053 | -0.4 | -0.0163 | -0.5 |
| D5 | 0.454 | 0.467 | +0.0128 | +1.4 | +0.0187 | +1.1 |
| D6 | 0.534 | 0.563 | +0.0292 | +3.6*** | +0.0248 | +1.7 |
| D7 | 0.645 | 0.696 | +0.0511 | +3.0* | +0.0606 | +2.6* |
| D8 | 0.746 | 0.775 | +0.0296 | +2.2 | +0.0566 | +2.8* |
| D9 | 0.849 | 0.875 | +0.0253 | +2.4 | +0.0413 | +2.8* |
| D10 | 0.961 | 0.961 | +0.0008 | +0.1 | +0.0135 | +2.2 |
| **D10-D1** | | | **+0.0138** | **+1.2** | **+0.0322** | **+3.3**** |

_Medium 21-1K_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.051 | +0.0077 | +0.8 | -0.0053 | -0.5 |
| D2 | 0.141 | 0.140 | -0.0018 | -0.1 | +0.0677 | +0.8 |
| D3 | 0.243 | 0.250 | +0.0070 | +0.1 | +0.1359 | +0.9 |
| D4 | 0.347 | 0.349 | +0.0022 | +0.1 | +0.0953 | +1.3 |
| D5 | 0.447 | 0.435 | -0.0121 | -0.6 | -0.0438 | -1.3 |
| D6 | 0.539 | 0.558 | +0.0194 | +1.0 | +0.0234 | +0.7 |
| D7 | 0.645 | 0.606 | -0.0390 | -0.9 | -0.1418 | -1.6 |
| D8 | 0.748 | 0.697 | -0.0516 | -0.6 | -0.1779 | -0.9 |
| D9 | 0.849 | 0.825 | -0.0243 | -0.5 | -0.1535 | -1.0 |
| D10 | 0.957 | 0.960 | +0.0036 | +0.4 | +0.0187 | +2.9* |
| **D10-D1** | | | **-0.0041** | **-0.3** | **+0.0240** | **+2.0** |


### 12. `dim_event_slug_size` — Family size on event_slug grouping

**Hypothesis**: as event_family_size at event_slug grain.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Small 2-20 | 14.0M | +0.0154 (+1.6) | +0.0285 (+1.2) | +0.0191 (+3.5**) | +0.0434 (+5.8***) | +0.0158 (+2.3) | +0.0288 (+2.2) |
| Medium 21-1K | 4.9M | +0.0036 (+0.2) | +0.0264 (+2.1) | +0.0389 (+5.8***) | +0.0204 (+0.8) | +0.0105 (+1.0) | +0.0064 (+0.3) |
| Singleton 1 | 3.0M | +0.0239 (+2.5) | +0.0386 (+3.6***) | -0.0070 (-0.5) | -0.0146 (-0.9) | +0.0157 (+1.9) | +0.0220 (+2.3) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Small 2-20_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.041 | 0.038 | -0.0026 | -0.3 | -0.0064 | -0.3 |
| D2 | 0.145 | 0.142 | -0.0039 | -0.4 | -0.0226 | -1.1 |
| D3 | 0.247 | 0.288 | +0.0406 | +1.5 | +0.0368 | +0.7 |
| D4 | 0.352 | 0.269 | -0.0834 | -1.1 | -0.1201 | -1.3 |
| D5 | 0.450 | 0.400 | -0.0499 | -0.9 | -0.1010 | -1.0 |
| D6 | 0.544 | 0.645 | +0.1011 | +1.5 | +0.1344 | +1.6 |
| D7 | 0.642 | 0.780 | +0.1378 | +1.6 | +0.1404 | +1.4 |
| D8 | 0.743 | 0.708 | -0.0357 | -0.6 | -0.0383 | -0.6 |
| D9 | 0.849 | 0.882 | +0.0329 | +3.0** | +0.0280 | +1.1 |
| D10 | 0.960 | 0.973 | +0.0128 | +2.1 | +0.0221 | +6.4*** |
| **D10-D1** | | | **+0.0154** | **+1.6** | **+0.0285** | **+1.2** |

_Medium 21-1K_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.036 | 0.038 | +0.0025 | +0.2 | -0.0134 | -1.3 |
| D2 | 0.143 | 0.124 | -0.0196 | -1.3 | -0.0080 | -0.2 |
| D3 | 0.248 | 0.269 | +0.0208 | +0.5 | +0.0338 | +0.6 |
| D4 | 0.345 | 0.372 | +0.0270 | +0.9 | -0.0070 | -0.2 |
| D5 | 0.453 | 0.487 | +0.0342 | +2.2 | +0.0261 | +1.2 |
| D6 | 0.536 | 0.525 | -0.0108 | -0.8 | -0.0302 | -1.3 |
| D7 | 0.646 | 0.656 | +0.0099 | +0.4 | -0.0465 | -0.8 |
| D8 | 0.745 | 0.736 | -0.0086 | -0.3 | -0.0634 | -1.0 |
| D9 | 0.850 | 0.862 | +0.0122 | +0.6 | -0.0377 | -0.7 |
| D10 | 0.963 | 0.969 | +0.0061 | +0.7 | +0.0130 | +1.8 |
| **D10-D1** | | | **+0.0036** | **+0.2** | **+0.0264** | **+2.1** |

_Singleton 1_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.034 | -0.0089 | -1.4 | -0.0183 | -2.2 |
| D2 | 0.149 | 0.159 | +0.0096 | +0.6 | +0.0038 | +0.1 |
| D3 | 0.247 | 0.306 | +0.0584 | +2.1 | +0.0419 | +1.1 |
| D4 | 0.347 | 0.375 | +0.0277 | +1.3 | +0.0648 | +2.0 |
| D5 | 0.444 | 0.479 | +0.0352 | +1.9 | +0.0562 | +2.7* |
| D6 | 0.544 | 0.549 | +0.0052 | +0.5 | +0.0138 | +0.6 |
| D7 | 0.645 | 0.648 | +0.0031 | +0.1 | -0.0258 | -0.6 |
| D8 | 0.744 | 0.724 | -0.0199 | -0.6 | -0.0186 | -0.4 |
| D9 | 0.845 | 0.855 | +0.0093 | +0.5 | +0.0167 | +0.7 |
| D10 | 0.962 | 0.977 | +0.0150 | +2.1 | +0.0203 | +3.2** |
| **D10-D1** | | | **+0.0239** | **+2.5** | **+0.0386** | **+3.6***** |


### 13. `dim_family_vol_tier` — Family total dollar volume terciles

**Hypothesis**: family dollar volume modulates FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| High vol | 23.6M | +0.0187 (+2.7*) | +0.0330 (+2.5) | +0.0266 (+6.6***) | +0.0348 (+4.5***) | +0.0193 (+3.9***) | +0.0275 (+3.3**) |
| Mid vol | 412K | +0.0245 (+5.0***) | +0.0204 (+2.2) | -0.0017 (-0.2) | +0.0020 (+0.3) | +0.0109 (+1.9) | -0.0238 (-2.7*) |
| Low vol | 64K | -0.0116 (-0.6) | +0.0469 (+5.0***) | -0.0004 (-0.1) | +0.0183 (+1.7) | -0.0264 (-1.1) | +0.0035 (+0.2) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_High vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.034 | -0.0047 | -0.8 | -0.0120 | -0.9 |
| D2 | 0.145 | 0.135 | -0.0100 | -1.4 | -0.0193 | -1.4 |
| D3 | 0.247 | 0.279 | +0.0320 | +1.7 | +0.0297 | +0.8 |
| D4 | 0.351 | 0.298 | -0.0525 | -0.9 | -0.0843 | -1.1 |
| D5 | 0.449 | 0.424 | -0.0255 | -0.6 | -0.0717 | -0.8 |
| D6 | 0.543 | 0.613 | +0.0698 | +1.3 | +0.0996 | +1.3 |
| D7 | 0.643 | 0.748 | +0.1055 | +1.5 | +0.0997 | +1.2 |
| D8 | 0.744 | 0.724 | -0.0204 | -0.6 | -0.0315 | -0.7 |
| D9 | 0.849 | 0.877 | +0.0283 | +3.3** | +0.0184 | +1.0 |
| D10 | 0.961 | 0.975 | +0.0140 | +3.7*** | +0.0210 | +7.2*** |
| **D10-D1** | | | **+0.0187** | **+2.7*** | **+0.0330** | **+2.5** |

_Mid vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.044 | 0.042 | -0.0020 | -0.5 | +0.0029 | +0.3 |
| D2 | 0.144 | 0.142 | -0.0024 | -0.3 | +0.0077 | +0.5 |
| D3 | 0.244 | 0.231 | -0.0131 | -0.8 | -0.0050 | -0.3 |
| D4 | 0.347 | 0.346 | -0.0009 | -0.1 | +0.0154 | +0.8 |
| D5 | 0.446 | 0.491 | +0.0452 | +3.5** | +0.0622 | +2.2 |
| D6 | 0.540 | 0.565 | +0.0255 | +2.3 | -0.0030 | -0.2 |
| D7 | 0.642 | 0.683 | +0.0405 | +3.2** | +0.0598 | +3.2** |
| D8 | 0.745 | 0.760 | +0.0151 | +1.3 | +0.0126 | +0.8 |
| D9 | 0.845 | 0.878 | +0.0334 | +4.3*** | +0.0286 | +2.6* |
| D10 | 0.952 | 0.974 | +0.0225 | +9.2*** | +0.0233 | +6.8*** |
| **D10-D1** | | | **+0.0245** | **+5.0***** | **+0.0204** | **+2.2** |

_Low vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.045 | 0.050 | +0.0056 | +0.4 | -0.0297 | -4.8*** |
| D2 | 0.142 | 0.136 | -0.0059 | -0.5 | -0.0343 | -1.9 |
| D3 | 0.245 | 0.266 | +0.0210 | +0.9 | +0.0258 | +0.9 |
| D4 | 0.345 | 0.357 | +0.0126 | +0.6 | +0.0127 | +0.4 |
| D5 | 0.445 | 0.485 | +0.0404 | +1.3 | +0.0334 | +1.1 |
| D6 | 0.537 | 0.579 | +0.0420 | +2.1 | +0.0410 | +1.7 |
| D7 | 0.640 | 0.704 | +0.0642 | +2.2 | +0.0498 | +1.3 |
| D8 | 0.743 | 0.771 | +0.0276 | +1.3 | +0.0207 | +0.8 |
| D9 | 0.846 | 0.893 | +0.0465 | +4.0*** | +0.0681 | +5.3*** |
| D10 | 0.951 | 0.945 | -0.0061 | -0.4 | +0.0172 | +2.5 |
| **D10-D1** | | | **-0.0116** | **-0.6** | **+0.0469** | **+5.0***** |


### 14. `dim_family_size_x_vol` — 3x3 cross-tab of family size x volume

**Hypothesis**: disentangle family count vs family dollar attention.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| Small 2-20 × High vol | 9.0M | +0.0451 (+11.2***) | +0.0583 (+8.1***) | +0.0339 (+3.8***) | +0.0504 (+3.8***) | +0.0395 (+7.1***) | +0.0521 (+7.5***) |
| Large 1K+ × High vol | 6.6M | -0.0193 (-2.1) | -0.0152 (-1.6) | +0.0064 (+1.6) | -0.0053 (-0.7) | -0.0081 (-1.7) | -0.0132 (-2.5) |
| Medium 21-1K × High vol | 5.6M | -0.0025 (-0.1) | -0.0010 (-0.0) | +0.0332 (+3.8***) | +0.0398 (+2.3) | +0.0061 (+0.5) | +0.0030 (+0.1) |
| Singleton 1 × High vol | 2.4M | +0.0485 (+9.4***) | +0.0572 (+5.8***) | +0.0478 (+8.0***) | +0.0527 (+5.4***) | +0.0435 (+10.0***) | +0.0499 (+6.3***) |
| Small 2-20 × Mid vol | 232K | +0.0040 (+0.5) | -0.0042 (-0.2) | -0.0177 (-1.2) | -0.0240 (-2.0) | -0.0192 (-1.8) | -0.0930 (-5.9***) |
| Singleton 1 × Mid vol | 173K | +0.0394 (+6.2***) | +0.0360 (+3.2**) | +0.0252 (+4.0***) | +0.0351 (+4.4***) | +0.0397 (+9.2***) | +0.0324 (+4.1***) |
| Singleton 1 × Low vol | 34K | +0.0037 (+0.2) | +0.0527 (+5.2***) | +0.0201 (+2.5*) | +0.0471 (+4.7***) | +0.0193 (+2.0) | +0.0405 (+2.3) |
| Small 2-20 × Low vol | 29K | -0.0474 (-1.3) | +0.0277 (+1.3) | -0.0232 (-1.9) | -0.0193 (-0.9) | -0.0921 (-1.8) | -0.0652 (-2.1) |
| Medium 21-1K × Mid vol | 8K | +0.0002 (+0.0) | +0.0353 (+1.4) | +0.0057 (+0.3) | +0.0176 (+0.5) | -0.0006 (-0.1) | +0.0312 (+1.8) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_Small 2-20 × High vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.040 | 0.021 | -0.0198 | -6.9*** | -0.0330 | -5.5*** |
| D2 | 0.146 | 0.134 | -0.0120 | -0.8 | -0.0183 | -0.7 |
| D3 | 0.247 | 0.312 | +0.0642 | +1.3 | +0.0618 | +0.9 |
| D4 | 0.355 | 0.202 | -0.1535 | -1.8 | -0.1725 | -1.8 |
| D5 | 0.451 | 0.321 | -0.1294 | -1.6 | -0.1521 | -1.2 |
| D6 | 0.548 | 0.714 | +0.1662 | +1.7 | +0.1852 | +2.0 |
| D7 | 0.642 | 0.824 | +0.1822 | +1.9 | +0.1650 | +1.5 |
| D8 | 0.744 | 0.662 | -0.0823 | -0.9 | -0.0737 | -0.8 |
| D9 | 0.850 | 0.888 | +0.0381 | +2.2 | +0.0236 | +0.8 |
| D10 | 0.962 | 0.987 | +0.0254 | +9.0*** | +0.0253 | +6.4*** |
| **D10-D1** | | | **+0.0451** | **+11.2***** | **+0.0583** | **+8.1***** |

_Large 1K+ × High vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.047 | 0.060 | +0.0129 | +2.6* | +0.0131 | +2.0 |
| D2 | 0.145 | 0.158 | +0.0126 | +1.4 | +0.0005 | +0.0 |
| D3 | 0.246 | 0.267 | +0.0201 | +2.3 | -0.0136 | -0.8 |
| D4 | 0.348 | 0.381 | +0.0331 | +3.2** | +0.0299 | +1.2 |
| D5 | 0.450 | 0.493 | +0.0433 | +5.1*** | +0.0398 | +3.0* |
| D6 | 0.538 | 0.551 | +0.0129 | +1.8 | +0.0158 | +1.2 |
| D7 | 0.642 | 0.661 | +0.0184 | +1.9 | +0.0221 | +1.2 |
| D8 | 0.743 | 0.761 | +0.0183 | +2.0 | +0.0094 | +0.3 |
| D9 | 0.846 | 0.850 | +0.0046 | +0.5 | -0.0164 | -0.9 |
| D10 | 0.954 | 0.947 | -0.0064 | -0.8 | -0.0021 | -0.3 |
| **D10-D1** | | | **-0.0193** | **-2.1** | **-0.0152** | **-1.6** |

_Medium 21-1K × High vol_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.034 | 0.039 | +0.0047 | +0.3 | +0.0163 | +0.5 |
| D2 | 0.144 | 0.114 | -0.0302 | -2.3 | -0.0306 | -1.1 |
| D3 | 0.248 | 0.261 | +0.0138 | +0.3 | +0.0270 | +0.6 |
| D4 | 0.344 | 0.391 | +0.0461 | +1.7 | +0.0808 | +2.1 |
| D5 | 0.447 | 0.478 | +0.0313 | +1.5 | +0.0146 | +0.4 |
| D6 | 0.541 | 0.536 | -0.0054 | -0.3 | -0.0464 | -1.4 |
| D7 | 0.646 | 0.661 | +0.0148 | +0.6 | -0.0657 | -1.2 |
| D8 | 0.746 | 0.761 | +0.0144 | +0.5 | -0.0233 | -0.4 |
| D9 | 0.850 | 0.879 | +0.0289 | +1.7 | +0.0219 | +0.6 |
| D10 | 0.963 | 0.965 | +0.0022 | +0.2 | +0.0154 | +3.0** |
| **D10-D1** | | | **-0.0025** | **-0.1** | **-0.0010** | **-0.0** |


### 15. `dim_vol_per_contract_tier` — Dollar-per-contract quintiles

**Hypothesis**: higher per-contract dollar attention -> calibrated.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| VPC Q5 (thickest) | 20.3M | +0.0228 (+3.0**) | +0.0349 (+2.5*) | +0.0308 (+6.4***) | +0.0388 (+4.7***) | +0.0238 (+4.3***) | +0.0315 (+3.6***) |
| VPC Q4 | 2.7M | -0.0172 (-2.0) | -0.0118 (-1.0) | +0.0097 (+2.9*) | -0.0067 (-0.8) | -0.0065 (-1.3) | -0.0217 (-3.4**) |
| VPC Q3 | 858K | +0.0090 (+2.0) | +0.0052 (+0.7) | +0.0133 (+3.3**) | +0.0097 (+1.4) | +0.0067 (+2.0) | -0.0429 (-4.7***) |
| VPC Q2 | 166K | +0.0005 (+0.1) | -0.0635 (-1.4) | -0.0181 (-0.9) | -0.0776 (-2.8*) | -0.0227 (-2.1) | -0.1803 (-4.8***) |
| VPC Q1 (thinnest) | 22K | -0.1442 (-2.0) | -0.0375 (-1.1) | -0.0376 (-2.0) | -0.0410 (-1.2) | -0.1072 (-2.2) | -0.1078 (-2.1) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_VPC Q5 (thickest)_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.038 | 0.031 | -0.0071 | -1.1 | -0.0131 | -1.0 |
| D2 | 0.145 | 0.127 | -0.0186 | -2.2 | -0.0221 | -1.5 |
| D3 | 0.248 | 0.279 | +0.0309 | +1.3 | +0.0294 | +0.8 |
| D4 | 0.351 | 0.282 | -0.0695 | -1.1 | -0.0903 | -1.1 |
| D5 | 0.450 | 0.410 | -0.0402 | -0.8 | -0.0791 | -0.9 |
| D6 | 0.544 | 0.619 | +0.0755 | +1.2 | +0.1049 | +1.3 |
| D7 | 0.643 | 0.757 | +0.1142 | +1.4 | +0.1031 | +1.1 |
| D8 | 0.744 | 0.712 | -0.0315 | -0.7 | -0.0369 | -0.8 |
| D9 | 0.849 | 0.878 | +0.0294 | +2.9* | +0.0177 | +0.9 |
| D10 | 0.962 | 0.978 | +0.0157 | +3.9*** | +0.0218 | +7.3*** |
| **D10-D1** | | | **+0.0228** | **+3.0**** | **+0.0349** | **+2.5*** |

_VPC Q4_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.056 | +0.0134 | +3.4** | +0.0132 | +1.3 |
| D2 | 0.146 | 0.176 | +0.0298 | +4.0*** | +0.0205 | +1.5 |
| D3 | 0.245 | 0.277 | +0.0321 | +4.6*** | +0.0283 | +1.6 |
| D4 | 0.346 | 0.385 | +0.0394 | +5.4*** | +0.0232 | +1.5 |
| D5 | 0.448 | 0.503 | +0.0554 | +7.3*** | +0.0310 | +1.8 |
| D6 | 0.539 | 0.577 | +0.0379 | +6.2*** | +0.0379 | +1.6 |
| D7 | 0.643 | 0.692 | +0.0495 | +7.1*** | +0.0431 | +2.0 |
| D8 | 0.744 | 0.779 | +0.0356 | +5.4*** | +0.0410 | +2.0 |
| D9 | 0.846 | 0.868 | +0.0223 | +3.5*** | +0.0315 | +3.0* |
| D10 | 0.954 | 0.950 | -0.0038 | -0.5 | +0.0014 | +0.2 |
| **D10-D1** | | | **-0.0172** | **-2.0** | **-0.0118** | **-1.0** |

_VPC Q3_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.045 | 0.051 | +0.0064 | +1.8 | +0.0068 | +1.1 |
| D2 | 0.144 | 0.159 | +0.0151 | +2.0 | +0.0069 | +0.6 |
| D3 | 0.243 | 0.278 | +0.0349 | +2.5 | +0.0390 | +2.0 |
| D4 | 0.344 | 0.396 | +0.0526 | +5.7*** | -0.0045 | -0.2 |
| D5 | 0.446 | 0.495 | +0.0495 | +5.3*** | +0.0810 | +1.3 |
| D6 | 0.540 | 0.585 | +0.0450 | +5.1*** | -0.0675 | -1.6 |
| D7 | 0.644 | 0.673 | +0.0297 | +3.6*** | +0.0248 | +1.2 |
| D8 | 0.745 | 0.769 | +0.0245 | +2.9* | +0.0051 | +0.3 |
| D9 | 0.848 | 0.880 | +0.0323 | +5.3*** | +0.0191 | +1.4 |
| D10 | 0.953 | 0.968 | +0.0154 | +5.7*** | +0.0120 | +2.9* |
| **D10-D1** | | | **+0.0090** | **+2.0** | **+0.0052** | **+0.7** |


### 16. `dim_vol_per_contract_residualized` — Volume per contract, residualized on size

**Hypothesis**: volume per contract conditional on size.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| VPC resid High | 22.5M | +0.0203 (+2.8*) | +0.0337 (+2.5*) | +0.0279 (+6.6***) | +0.0357 (+4.6***) | +0.0211 (+4.1***) | +0.0293 (+3.5**) |
| VPC resid Mid | 1.5M | -0.0020 (-0.5) | -0.0180 (-2.0) | +0.0068 (+1.6) | -0.0082 (-1.2) | -0.0019 (-0.6) | -0.0705 (-6.9***) |
| VPC resid Low | 93K | -0.0545 (-2.3) | -0.0572 (-1.6) | -0.0092 (-1.1) | -0.0468 (-1.7) | -0.0552 (-2.1) | -0.1190 (-5.2***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_VPC resid High_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.033 | -0.0061 | -1.0 | -0.0125 | -1.0 |
| D2 | 0.145 | 0.132 | -0.0131 | -1.7 | -0.0200 | -1.4 |
| D3 | 0.247 | 0.278 | +0.0303 | +1.5 | +0.0292 | +0.8 |
| D4 | 0.351 | 0.293 | -0.0581 | -1.0 | -0.0850 | -1.1 |
| D5 | 0.450 | 0.421 | -0.0286 | -0.7 | -0.0730 | -0.9 |
| D6 | 0.543 | 0.614 | +0.0709 | +1.3 | +0.1007 | +1.3 |
| D7 | 0.643 | 0.751 | +0.1081 | +1.5 | +0.1000 | +1.1 |
| D8 | 0.744 | 0.721 | -0.0228 | -0.6 | -0.0319 | -0.7 |
| D9 | 0.849 | 0.878 | +0.0289 | +3.2** | +0.0185 | +1.0 |
| D10 | 0.962 | 0.976 | +0.0142 | +3.7*** | +0.0212 | +7.3*** |
| **D10-D1** | | | **+0.0203** | **+2.8*** | **+0.0337** | **+2.5*** |

_VPC resid Mid_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.044 | 0.059 | +0.0148 | +4.4*** | +0.0265 | +3.3** |
| D2 | 0.144 | 0.164 | +0.0201 | +3.3** | +0.0188 | +1.8 |
| D3 | 0.244 | 0.281 | +0.0374 | +3.5*** | +0.0429 | +2.7* |
| D4 | 0.344 | 0.394 | +0.0500 | +6.9*** | +0.0091 | +0.5 |
| D5 | 0.445 | 0.496 | +0.0501 | +7.0*** | +0.0772 | +2.0 |
| D6 | 0.540 | 0.578 | +0.0388 | +5.2*** | -0.0306 | -1.1 |
| D7 | 0.643 | 0.676 | +0.0328 | +5.0*** | +0.0595 | +2.6* |
| D8 | 0.745 | 0.769 | +0.0241 | +3.9*** | +0.0151 | +1.3 |
| D9 | 0.847 | 0.871 | +0.0245 | +4.1*** | +0.0184 | +1.7 |
| D10 | 0.953 | 0.966 | +0.0128 | +5.9*** | +0.0085 | +2.3 |
| **D10-D1** | | | **-0.0020** | **-0.5** | **-0.0180** | **-2.0** |

_VPC resid Low_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.045 | 0.075 | +0.0301 | +2.0 | +0.0256 | +0.9 |
| D2 | 0.142 | 0.178 | +0.0359 | +3.0* | +0.0206 | +1.0 |
| D3 | 0.244 | 0.284 | +0.0406 | +2.1 | +0.0382 | +1.4 |
| D4 | 0.348 | 0.327 | -0.0213 | -0.6 | -0.0079 | -0.3 |
| D5 | 0.444 | 0.471 | +0.0273 | +1.0 | +0.0031 | +0.1 |
| D6 | 0.536 | 0.538 | +0.0018 | +0.1 | +0.0218 | +0.7 |
| D7 | 0.638 | 0.705 | +0.0673 | +1.8 | +0.1085 | +1.8 |
| D8 | 0.744 | 0.745 | +0.0001 | +0.0 | -0.0073 | -0.3 |
| D9 | 0.844 | 0.842 | -0.0018 | -0.1 | -0.0098 | -0.3 |
| D10 | 0.949 | 0.925 | -0.0244 | -1.3 | -0.0316 | -1.4 |
| **D10-D1** | | | **-0.0545** | **-2.3** | **-0.0572** | **-1.6** |


### 17. `dim_text_novelty` — Semantic isolation by best-neighbor cosine (fixed thresholds)

**Hypothesis**: slugs with no near-duplicate (semantic novelty) -> no prior anchor -> strong FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| >0.95 near duplicate | 18.5M | +0.0099 (+1.0) | +0.0272 (+1.6) | +0.0210 (+4.4***) | +0.0361 (+5.2***) | +0.0144 (+2.2) | +0.0244 (+2.3) |
| 0.90-0.95 close lex match | 2.4M | +0.0135 (+1.5) | +0.0251 (+2.1) | +0.0173 (+1.8) | +0.0268 (+2.3) | +0.0072 (+0.9) | +0.0090 (+0.7) |
| <0.50 genuinely isolated | 2.2M | +0.0556 (+11.8***) | +0.0684 (+6.7***) | +0.0605 (+14.2***) | +0.0690 (+8.4***) | +0.0518 (+12.8***) | +0.0654 (+8.9***) |
| 0.75-0.90 has neighbor | 954K | +0.0509 (+5.2***) | +0.0722 (+16.9***) | +0.0101 (+0.5) | -0.0584 (-0.9) | +0.0314 (+3.5**) | +0.0236 (+0.9) |
| 0.50-0.75 mod isolated | 23K | +0.0599 (+7.9***) | +0.0796 (+8.3***) | +0.0757 (+9.2***) | +0.0913 (+13.5***) | +0.0634 (+9.9***) | +0.0895 (+14.1***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_>0.95 near duplicate_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.038 | 0.039 | +0.0008 | +0.1 | -0.0074 | -0.4 |
| D2 | 0.146 | 0.140 | -0.0061 | -0.8 | -0.0291 | -1.9 |
| D3 | 0.248 | 0.297 | +0.0494 | +2.1 | +0.0367 | +0.9 |
| D4 | 0.351 | 0.292 | -0.0587 | -0.9 | -0.0945 | -1.1 |
| D5 | 0.450 | 0.422 | -0.0283 | -0.6 | -0.0754 | -0.9 |
| D6 | 0.543 | 0.619 | +0.0759 | +1.3 | +0.1054 | +1.4 |
| D7 | 0.643 | 0.760 | +0.1170 | +1.5 | +0.1135 | +1.2 |
| D8 | 0.743 | 0.711 | -0.0318 | -0.7 | -0.0306 | -0.6 |
| D9 | 0.848 | 0.875 | +0.0264 | +2.7* | +0.0226 | +1.2 |
| D10 | 0.961 | 0.972 | +0.0107 | +2.1 | +0.0198 | +5.6*** |
| **D10-D1** | | | **+0.0099** | **+1.0** | **+0.0272** | **+1.6** |

_0.90-0.95 close lex match_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.040 | -0.0025 | -0.4 | -0.0124 | -1.3 |
| D2 | 0.142 | 0.140 | -0.0025 | -0.1 | +0.0396 | +1.1 |
| D3 | 0.243 | 0.220 | -0.0233 | -1.1 | +0.0125 | +0.3 |
| D4 | 0.345 | 0.317 | -0.0277 | -1.4 | -0.0190 | -0.7 |
| D5 | 0.446 | 0.415 | -0.0309 | -1.1 | -0.0215 | -0.8 |
| D6 | 0.543 | 0.542 | -0.0006 | -0.0 | -0.0510 | -1.6 |
| D7 | 0.646 | 0.628 | -0.0183 | -0.5 | -0.1133 | -1.3 |
| D8 | 0.747 | 0.744 | -0.0032 | -0.1 | -0.0878 | -1.1 |
| D9 | 0.850 | 0.870 | +0.0201 | +1.2 | -0.0016 | -0.1 |
| D10 | 0.958 | 0.970 | +0.0110 | +1.8 | +0.0127 | +1.7 |
| **D10-D1** | | | **+0.0135** | **+1.5** | **+0.0251** | **+2.1** |

_<0.50 genuinely isolated_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.012 | -0.0271 | -8.4*** | -0.0371 | -4.5*** |
| D2 | 0.144 | 0.099 | -0.0449 | -1.9 | -0.0639 | -3.2** |
| D3 | 0.244 | 0.175 | -0.0687 | -2.4 | -0.0771 | -2.4 |
| D4 | 0.349 | 0.316 | -0.0335 | -0.9 | -0.0387 | -0.9 |
| D5 | 0.445 | 0.438 | -0.0070 | -0.3 | -0.0208 | -0.7 |
| D6 | 0.545 | 0.574 | +0.0296 | +1.0 | +0.0101 | +0.3 |
| D7 | 0.644 | 0.738 | +0.0940 | +3.5*** | +0.0898 | +2.6* |
| D8 | 0.746 | 0.828 | +0.0817 | +3.3** | +0.0778 | +2.6* |
| D9 | 0.850 | 0.914 | +0.0638 | +4.3*** | +0.0676 | +3.3** |
| D10 | 0.962 | 0.991 | +0.0285 | +8.3*** | +0.0312 | +5.3*** |
| **D10-D1** | | | **+0.0556** | **+11.8***** | **+0.0684** | **+6.7***** |


### 18. `dim_text_neighbors_strict` — Count of slugs above 0.75 cosine

**Hypothesis**: as text_novelty, operationalized by threshold-count.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 6+ strict neighbors | 21.8M | +0.0127 (+1.6) | +0.0299 (+2.1) | +0.0203 (+4.5***) | +0.0288 (+3.3**) | +0.0144 (+2.6*) | +0.0230 (+2.5*) |
| 0 strict neighbors | 2.3M | +0.0556 (+11.9***) | +0.0684 (+6.8***) | +0.0606 (+14.3***) | +0.0691 (+8.5***) | +0.0519 (+13.0***) | +0.0656 (+9.0***) |
| 2-5 strict neighbors | 18K | +0.0391 (+2.1) | +0.0354 (+1.4) | -0.1152 (-0.9) | -0.1678 (-0.8) | +0.0017 (+0.1) | -0.0163 (-0.2) |
| 1 strict neighbor | — | — | — | — | — | +0.0375 (+1.7) | +0.0737 (+5.4***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_6+ strict neighbors_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.038 | -0.0012 | -0.2 | -0.0100 | -0.7 |
| D2 | 0.145 | 0.139 | -0.0066 | -0.9 | -0.0160 | -1.1 |
| D3 | 0.247 | 0.286 | +0.0391 | +1.9 | +0.0368 | +1.0 |
| D4 | 0.351 | 0.298 | -0.0527 | -0.9 | -0.0863 | -1.1 |
| D5 | 0.450 | 0.425 | -0.0251 | -0.6 | -0.0727 | -0.8 |
| D6 | 0.543 | 0.614 | +0.0711 | +1.3 | +0.1015 | +1.3 |
| D7 | 0.643 | 0.748 | +0.1050 | +1.4 | +0.1004 | +1.1 |
| D8 | 0.744 | 0.716 | -0.0280 | -0.7 | -0.0380 | -0.9 |
| D9 | 0.848 | 0.873 | +0.0250 | +2.8* | +0.0148 | +0.8 |
| D10 | 0.961 | 0.973 | +0.0115 | +2.6* | +0.0199 | +6.4*** |
| **D10-D1** | | | **+0.0127** | **+1.6** | **+0.0299** | **+2.1** |

_0 strict neighbors_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.012 | -0.0271 | -8.4*** | -0.0372 | -4.5*** |
| D2 | 0.144 | 0.100 | -0.0445 | -1.9 | -0.0635 | -3.2** |
| D3 | 0.244 | 0.176 | -0.0684 | -2.4 | -0.0747 | -2.4 |
| D4 | 0.349 | 0.314 | -0.0349 | -0.9 | -0.0380 | -0.9 |
| D5 | 0.445 | 0.435 | -0.0092 | -0.4 | -0.0184 | -0.6 |
| D6 | 0.545 | 0.575 | +0.0302 | +1.0 | +0.0095 | +0.3 |
| D7 | 0.644 | 0.740 | +0.0955 | +3.6*** | +0.0893 | +2.6* |
| D8 | 0.746 | 0.828 | +0.0819 | +3.3** | +0.0776 | +2.7* |
| D9 | 0.850 | 0.912 | +0.0620 | +4.2*** | +0.0666 | +3.3** |
| D10 | 0.962 | 0.991 | +0.0285 | +8.4*** | +0.0313 | +5.4*** |
| **D10-D1** | | | **+0.0556** | **+11.9***** | **+0.0684** | **+6.8***** |

_2-5 strict neighbors_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.032 | -0.0108 | -0.7 | -0.0266 | -1.8 |
| D2 | 0.148 | 0.216 | +0.0679 | +0.8 | +0.3775 | +1.8 |
| D3 | 0.242 | 0.336 | +0.0933 | +0.6 | +0.3154 | +1.8 |
| D4 | 0.345 | 0.506 | +0.1609 | +1.2 | +0.4834 | +6.2*** |
| D5 | 0.439 | 0.537 | +0.0980 | +0.7 | +0.2860 | +1.5 |
| D6 | 0.547 | 0.519 | -0.0281 | -0.4 | -0.0636 | -0.4 |
| D7 | 0.646 | 0.665 | +0.0197 | +0.2 | -0.3987 | -3.7*** |
| D8 | 0.748 | 0.660 | -0.0876 | -0.6 | -0.3484 | -1.9 |
| D9 | 0.847 | 0.778 | -0.0689 | -0.7 | -0.2902 | -1.2 |
| D10 | 0.956 | 0.985 | +0.0283 | +3.0** | +0.0088 | +0.4 |
| **D10-D1** | | | **+0.0391** | **+2.1** | **+0.0354** | **+1.4** |


### 19. `dim_prior_settlements_bin__event_template` — Prior settled contracts at trade time

**Hypothesis**: more prior same-template settlements at trade time -> less FLB.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 13.1M | +0.0319 (+4.3***) | +0.0376 (+2.0) | +0.0462 (+11.8***) | +0.0643 (+10.3***) | +0.0322 (+5.8***) | +0.0404 (+3.5***) |
| 50+ | 7.6M | -0.0273 (-1.9) | -0.0083 (-1.0) | +0.0003 (+0.0) | -0.0091 (-1.2) | -0.0125 (-1.5) | -0.0114 (-2.1) |
| 6-50 | 1.8M | +0.0133 (+0.5) | +0.0337 (+2.0) | +0.0509 (+10.6***) | +0.0696 (+12.9***) | +0.0170 (+0.9) | +0.0190 (+0.7) |
| 1-5 | 1.5M | +0.0424 (+3.7***) | +0.0455 (+2.2) | -0.0051 (-0.2) | -0.0394 (-0.8) | +0.0261 (+2.0) | +0.0142 (+0.6) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_0_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.038 | 0.027 | -0.0103 | -1.5 | -0.0125 | -0.7 |
| D2 | 0.145 | 0.125 | -0.0201 | -1.7 | -0.0282 | -1.5 |
| D3 | 0.247 | 0.290 | +0.0427 | +1.2 | +0.0426 | +0.7 |
| D4 | 0.354 | 0.231 | -0.1226 | -1.6 | -0.1453 | -1.6 |
| D5 | 0.450 | 0.347 | -0.1028 | -1.4 | -0.1410 | -1.2 |
| D6 | 0.547 | 0.682 | +0.1345 | +1.5 | +0.1630 | +1.7 |
| D7 | 0.642 | 0.800 | +0.1576 | +1.7 | +0.1389 | +1.3 |
| D8 | 0.744 | 0.686 | -0.0581 | -0.9 | -0.0488 | -0.7 |
| D9 | 0.850 | 0.895 | +0.0453 | +3.9*** | +0.0328 | +1.4 |
| D10 | 0.963 | 0.984 | +0.0215 | +7.7*** | +0.0252 | +8.2*** |
| **D10-D1** | | | **+0.0319** | **+4.3***** | **+0.0376** | **+2.0** |

_50+_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.046 | 0.057 | +0.0113 | +2.6* | +0.0095 | +1.5 |
| D2 | 0.145 | 0.157 | +0.0121 | +1.5 | -0.0011 | -0.1 |
| D3 | 0.246 | 0.264 | +0.0180 | +2.2 | -0.0114 | -0.8 |
| D4 | 0.347 | 0.380 | +0.0329 | +3.3** | +0.0388 | +1.6 |
| D5 | 0.450 | 0.494 | +0.0448 | +5.6*** | +0.0421 | +3.0** |
| D6 | 0.539 | 0.549 | +0.0108 | +1.6 | +0.0099 | +0.8 |
| D7 | 0.643 | 0.664 | +0.0211 | +2.3 | +0.0211 | +1.2 |
| D8 | 0.743 | 0.759 | +0.0165 | +1.9 | +0.0036 | +0.1 |
| D9 | 0.846 | 0.847 | +0.0011 | +0.1 | -0.0156 | -0.9 |
| D10 | 0.955 | 0.939 | -0.0160 | -1.1 | +0.0012 | +0.2 |
| **D10-D1** | | | **-0.0273** | **-1.9** | **-0.0083** | **-1.0** |

_6-50_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.033 | 0.037 | +0.0040 | +0.2 | -0.0181 | -1.4 |
| D2 | 0.143 | 0.089 | -0.0544 | -4.1*** | -0.0534 | -3.1** |
| D3 | 0.253 | 0.301 | +0.0481 | +0.5 | +0.0241 | +0.4 |
| D4 | 0.343 | 0.416 | +0.0727 | +1.2 | +0.0956 | +1.4 |
| D5 | 0.445 | 0.453 | +0.0084 | +0.3 | -0.0298 | -0.6 |
| D6 | 0.542 | 0.577 | +0.0353 | +1.2 | +0.0502 | +0.8 |
| D7 | 0.648 | 0.669 | +0.0211 | +0.5 | -0.0073 | -0.1 |
| D8 | 0.744 | 0.739 | -0.0049 | -0.1 | -0.0418 | -0.5 |
| D9 | 0.852 | 0.906 | +0.0540 | +3.5*** | +0.0644 | +2.7* |
| D10 | 0.962 | 0.979 | +0.0174 | +1.7 | +0.0156 | +1.6 |
| **D10-D1** | | | **+0.0133** | **+0.5** | **+0.0337** | **+2.0** |


### 20. `dim_prior_settlements_bin__event_slug` — Prior settled contracts (event_slug grouping)

**Hypothesis**: same at event_slug grain.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 23.1M | +0.0175 (+2.5*) | +0.0313 (+2.3) | +0.0271 (+6.9***) | +0.0411 (+7.4***) | +0.0185 (+3.7***) | +0.0275 (+3.2**) |
| 1-5 | 783K | +0.0342 (+5.5***) | +0.0584 (+10.0***) | -0.0184 (-0.8) | -0.1356 (-1.3) | +0.0209 (+2.6*) | -0.0015 (-0.0) |
| 6-50 | 246K | +0.0628 (+11.7***) | +0.0837 (+19.2***) | +0.0371 (+3.6***) | +0.0403 (+3.2**) | +0.0422 (+5.1***) | +0.0558 (+8.3***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_0_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.035 | -0.0041 | -0.7 | -0.0108 | -0.8 |
| D2 | 0.145 | 0.135 | -0.0106 | -1.5 | -0.0230 | -1.7 |
| D3 | 0.247 | 0.278 | +0.0305 | +1.6 | +0.0238 | +0.7 |
| D4 | 0.351 | 0.297 | -0.0535 | -0.9 | -0.0863 | -1.1 |
| D5 | 0.449 | 0.424 | -0.0248 | -0.6 | -0.0723 | -0.8 |
| D6 | 0.543 | 0.614 | +0.0711 | +1.3 | +0.1009 | +1.3 |
| D7 | 0.643 | 0.750 | +0.1071 | +1.5 | +0.1030 | +1.2 |
| D8 | 0.744 | 0.725 | -0.0188 | -0.5 | -0.0271 | -0.6 |
| D9 | 0.849 | 0.879 | +0.0309 | +3.8*** | +0.0245 | +1.5 |
| D10 | 0.961 | 0.975 | +0.0134 | +3.4** | +0.0205 | +6.6*** |
| **D10-D1** | | | **+0.0175** | **+2.5*** | **+0.0313** | **+2.3** |

_1-5_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.028 | -0.0117 | -2.1 | -0.0348 | -7.5*** |
| D2 | 0.146 | 0.165 | +0.0192 | +0.4 | +0.1038 | +0.7 |
| D3 | 0.246 | 0.282 | +0.0361 | +0.4 | +0.1562 | +1.0 |
| D4 | 0.345 | 0.347 | +0.0020 | +0.1 | +0.0360 | +0.5 |
| D5 | 0.453 | 0.440 | -0.0129 | -0.8 | -0.0323 | -1.5 |
| D6 | 0.534 | 0.561 | +0.0272 | +1.7 | +0.0515 | +2.1 |
| D7 | 0.646 | 0.677 | +0.0312 | +0.7 | -0.0726 | -1.0 |
| D8 | 0.747 | 0.727 | -0.0200 | -0.2 | -0.1106 | -0.6 |
| D9 | 0.847 | 0.828 | -0.0182 | -0.3 | -0.1238 | -0.8 |
| D10 | 0.963 | 0.985 | +0.0226 | +8.5*** | +0.0236 | +6.6*** |
| **D10-D1** | | | **+0.0342** | **+5.5***** | **+0.0584** | **+10.0***** |

_6-50_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.041 | 0.009 | -0.0324 | -10.3*** | -0.0470 | -13.9*** |
| D2 | 0.141 | 0.118 | -0.0226 | -0.6 | -0.0422 | -0.9 |
| D3 | 0.247 | 0.306 | +0.0587 | +1.1 | +0.1002 | +1.0 |
| D4 | 0.343 | 0.384 | +0.0407 | +0.7 | +0.0667 | +0.5 |
| D5 | 0.448 | 0.458 | +0.0097 | +0.2 | -0.0069 | -0.1 |
| D6 | 0.536 | 0.503 | -0.0329 | -1.1 | -0.0582 | -1.5 |
| D7 | 0.647 | 0.609 | -0.0376 | -0.6 | -0.0855 | -0.8 |
| D8 | 0.746 | 0.656 | -0.0894 | -1.3 | -0.0993 | -1.1 |
| D9 | 0.854 | 0.863 | +0.0086 | +0.2 | +0.0397 | +0.7 |
| D10 | 0.959 | 0.989 | +0.0304 | +7.0*** | +0.0367 | +13.4*** |
| **D10-D1** | | | **+0.0628** | **+11.7***** | **+0.0837** | **+19.2***** |


### 21. `dim_prior_settlements_bin__dim_group_strict` — Prior settled contracts (strict grouping)

**Hypothesis**: same at strict grouping grain.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| 0 | 23.3M | +0.0172 (+2.5) | +0.0309 (+2.2) | +0.0266 (+6.9***) | +0.0406 (+7.4***) | +0.0182 (+3.7***) | +0.0272 (+3.2**) |
| 1-5 | 520K | +0.0623 (+8.7***) | +0.0825 (+20.0***) | -0.0328 (-0.8) | -0.1611 (-1.3) | +0.0343 (+2.7*) | +0.0007 (+0.0) |
| 6-50 | 228K | +0.0628 (+10.2***) | +0.0802 (+15.7***) | +0.0454 (+4.7***) | +0.0433 (+3.5***) | +0.0494 (+5.5***) | +0.0616 (+10.9***) |
| 50+ | 31K | +0.1080 (+292.3***) | +0.1109 (+0.0) | +0.0566 (+0.0) | +0.0683 (+339.3***) | +0.0620 (+1504.2***) | +0.0726 (+848.0***) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_0_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.035 | -0.0039 | -0.7 | -0.0107 | -0.8 |
| D2 | 0.145 | 0.135 | -0.0099 | -1.4 | -0.0224 | -1.7 |
| D3 | 0.247 | 0.278 | +0.0307 | +1.6 | +0.0244 | +0.7 |
| D4 | 0.351 | 0.298 | -0.0530 | -0.9 | -0.0861 | -1.1 |
| D5 | 0.449 | 0.425 | -0.0243 | -0.6 | -0.0713 | -0.8 |
| D6 | 0.543 | 0.613 | +0.0702 | +1.3 | +0.0997 | +1.3 |
| D7 | 0.643 | 0.749 | +0.1066 | +1.5 | +0.1020 | +1.2 |
| D8 | 0.744 | 0.725 | -0.0186 | -0.5 | -0.0265 | -0.6 |
| D9 | 0.849 | 0.878 | +0.0299 | +3.7*** | +0.0243 | +1.5 |
| D10 | 0.961 | 0.975 | +0.0133 | +3.4** | +0.0203 | +6.6*** |
| **D10-D1** | | | **+0.0172** | **+2.5** | **+0.0309** | **+2.2** |

_1-5_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.043 | 0.015 | -0.0284 | -4.1*** | -0.0493 | -18.2*** |
| D2 | 0.147 | 0.141 | -0.0065 | -0.1 | +0.0793 | +0.5 |
| D3 | 0.246 | 0.287 | +0.0410 | +0.4 | +0.1493 | +0.8 |
| D4 | 0.343 | 0.333 | -0.0097 | -0.2 | +0.0382 | +0.4 |
| D5 | 0.449 | 0.432 | -0.0167 | -0.9 | -0.0493 | -1.2 |
| D6 | 0.538 | 0.568 | +0.0298 | +1.4 | +0.0657 | +2.0 |
| D7 | 0.649 | 0.679 | +0.0299 | +0.4 | -0.0804 | -0.8 |
| D8 | 0.748 | 0.712 | -0.0361 | -0.3 | -0.1468 | -0.7 |
| D9 | 0.847 | 0.845 | -0.0017 | -0.0 | -0.1180 | -0.7 |
| D10 | 0.959 | 0.993 | +0.0339 | +16.8*** | +0.0332 | +10.7*** |
| **D10-D1** | | | **+0.0623** | **+8.7***** | **+0.0825** | **+20.0***** |

_6-50_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.040 | 0.008 | -0.0319 | -9.0*** | -0.0437 | -10.8*** |
| D2 | 0.142 | 0.139 | -0.0031 | -0.1 | -0.0139 | -0.2 |
| D3 | 0.246 | 0.305 | +0.0592 | +1.1 | +0.1850 | +1.5 |
| D4 | 0.347 | 0.410 | +0.0638 | +1.2 | +0.2552 | +2.4 |
| D5 | 0.450 | 0.425 | -0.0244 | -0.7 | -0.0996 | -1.1 |
| D6 | 0.535 | 0.564 | +0.0295 | +1.0 | +0.0445 | +0.5 |
| D7 | 0.645 | 0.598 | -0.0466 | -0.7 | -0.1488 | -1.2 |
| D8 | 0.745 | 0.651 | -0.0934 | -1.3 | -0.1133 | -0.9 |
| D9 | 0.853 | 0.848 | -0.0050 | -0.1 | +0.0160 | +0.2 |
| D10 | 0.959 | 0.990 | +0.0309 | +6.2*** | +0.0366 | +11.8*** |
| **D10-D1** | | | **+0.0628** | **+10.2***** | **+0.0802** | **+15.7***** |


### 22. `dim_market_type` — Up/down vs non-up/down sensitivity

**Hypothesis**: up/down crypto markets are noise-trading regimes structurally different from learnability markets.


**Spread by window x weighting** (cnt = count-weighted, $ = dollar-weighted; each cell `spread (t)`):

| Slice | N | 25-80% cnt | 25-80% $ | 80-100% cnt | 80-100% $ | full cnt | full $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| non_updown | 24.0M | +0.0187 (+2.8*) | +0.0330 (+2.5) | +0.0255 (+6.5***) | +0.0345 (+4.6***) | +0.0189 (+3.9***) | +0.0270 (+3.3**) |
| updown | 69K | +0.0519 (+55.5***) | +0.0463 (+56.3***) | +0.0548 (+350.4***) | +0.0670 (+1684.3***) | +0.0545 (+922.0***) | +0.0653 (+0.0) |


**Decile breakdown (mature 25-80%, top 3 slices by trade count):**

_non_updown_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.039 | 0.034 | -0.0046 | -0.8 | -0.0119 | -0.9 |
| D2 | 0.145 | 0.135 | -0.0099 | -1.4 | -0.0191 | -1.4 |
| D3 | 0.247 | 0.278 | +0.0314 | +1.7 | +0.0294 | +0.8 |
| D4 | 0.351 | 0.299 | -0.0515 | -0.9 | -0.0838 | -1.1 |
| D5 | 0.449 | 0.425 | -0.0242 | -0.6 | -0.0712 | -0.8 |
| D6 | 0.543 | 0.612 | +0.0694 | +1.3 | +0.0993 | +1.3 |
| D7 | 0.643 | 0.747 | +0.1045 | +1.5 | +0.0996 | +1.2 |
| D8 | 0.744 | 0.724 | -0.0198 | -0.6 | -0.0312 | -0.7 |
| D9 | 0.849 | 0.877 | +0.0285 | +3.4** | +0.0184 | +1.0 |
| D10 | 0.961 | 0.975 | +0.0141 | +3.8*** | +0.0210 | +7.3*** |
| **D10-D1** | | | **+0.0187** | **+2.8*** | **+0.0330** | **+2.5** |

_updown_

| Decile | Impl. prob | Win rate | Return (cnt) | t cnt | Return ($) | t $ |
|---:|---:|---:|---:|---:|---:|---:|
| D1 | 0.066 | 0.055 | -0.0111 | -11.9*** | -0.0021 | -2.5* |
| D2 | 0.144 | 0.143 | -0.0016 | -1.1 | +0.0351 | +6.0*** |
| D3 | 0.245 | 0.181 | -0.0645 | -129.9*** | -0.0986 | -171.7*** |
| D4 | 0.346 | 0.296 | -0.0498 | -15.4*** | -0.1646 | -10.4*** |
| D5 | 0.453 | 0.435 | -0.0175 | -4.4*** | -0.0401 | -3.0* |
| D6 | 0.531 | 0.545 | +0.0140 | +18.0*** | +0.0096 | +1.5 |
| D7 | 0.644 | 0.714 | +0.0697 | +24.3*** | +0.0263 | +4.5*** |
| D8 | 0.739 | 0.799 | +0.0605 | +66.7*** | +0.0102 | +0.9 |
| D9 | 0.837 | 0.857 | +0.0204 | +0.0 | +0.0467 | +0.0 |
| D10 | 0.942 | 0.983 | +0.0408 | +1966.0*** | +0.0442 | +17257.2*** |
| **D10-D1** | | | **+0.0519** | **+55.5***** | **+0.0463** | **+56.3***** |


---
## Appendix A — Jaccard overlap between headline contract sets

The audit (Phase 1.1) tested whether the "convergent" headline findings are one underlying contract set. Set sizes: v3 Small 2-20 = 150,205; Small×HighVol = 55,651; Q1 text-isolated = 135,302; 0-strict-neighbors = 17,761; 0-priors event_template = 148,174.

| | v3_Small | Small×HiVol | Q1_iso | 0_neigh | 0_priors |
|---|---:|---:|---:|---:|---:|
| v3 Small 2-20 | 1.00 | 0.37 | 0.04 | 0.05 | 0.56 |
| Small × HighVol | 0.37 | 1.00 | 0.03 | 0.07 | 0.29 |
| Q1 isolated | 0.04 | 0.03 | 1.00 | 0.13 | 0.04 |
| 0 neighbors | 0.05 | 0.07 | 0.13 | 1.00 | 0.05 |
| 0 priors | 0.56 | 0.29 | 0.04 | 0.05 | 1.00 |

Max off-diagonal 0.56 (Small 2-20 ↔ 0-priors). The text-novelty cuts overlap only 0.13 with each other and <=0.07 with the family-size cuts. The headline findings are **not** one contract set — each identifies an independent sub-population (though composition-concentrated: Q1 isolated is 90% Crypto / 86.7% up/down before exclusion; Small×HighVol is 52% Sports).

## Appendix B — Up/down sensitivity

Up/down markets are 41.1% of contracts and were excluded from the primary v6 view; `dim_market_type` reports them. The audit's up/down-included-vs-excluded re-run found three patterns: (1) low-up/down headline cells **unchanged** (Small 2-20 × High vol +0.0501 either way; event_slug 50+ priors +0.0805 either way; 0-priors event_template +0.0432 either way); (2) text-novelty Q1 **strengthens** when up/down removed (it was diluting the signal); (3) several v3 dims were partly up/down-driven and **evaporate**: `dim_info_type_supergroup` market_data +0.0083 (t+0.99, 10.9M) → −0.0122 (t−0.46, 1.4M); `dim_subject_specificity` 1-subject +0.0367 (t+7.47) → +0.0153 (n.s.); `dim_contract_horizon` <1h drops 99% in size. Because v6 excludes up/down from the primary view, the per-dimension tables above already reflect the excluded numbers.

## Appendix C — The 50+ event_slug priors slice is one event family

All top-10 contracts in `dim_prior_settlements_bin__event_slug = '50+'` (only ~154 trade-bearing contracts mid-life) are "US strikes Iran by [DATE]" markets. Its spread reflects FLB on **one recurring betting line**, not a general "many priors → reversed FLB" pattern. Interpret as a single-event finding.

## Appendix D — Multiple-testing correction

Family ≈ 3 windows × ~22 dims × ~3–5 kept slices × 2 weightings. Bonferroni-equivalent |t| at α=0.05: `*` > 2.5, `**` > 3.0, `***` > 3.5 (used throughout; the naive 1.96/2.58/3.29 are not). Of the audit's 87 v4 tests, 33 survived Bonferroni; all four headline findings pass with |t| > 12.

## Appendix E — Known caveats

1. **3-way SE on small slices** (Singleton, 50+ priors event_slug, N≈150) is asymptotically unstable; interpret those t-stats conservatively.
2. **Lifecycle window non-comparability across durations**: 25-80% is minutes for a <1h crypto but weeks for a >1mo election. Within `>1mo` the text-novelty Q1 spread is +0.0666 (t+21.5); within `<1h` it is n.s.
3. **Dimension assignments use full-trade volume tiers**, while calibration is on clean trades; the ~1.8% volume change does not move tier boundaries materially.
4. **Dollar-weighted SE has fewer effective clusters** than count-weighted (mass concentrates on large trades), so dollar t-stats run lower at equal spread; a dollar result that stays significant is therefore strong evidence.
5. **`dim_text_novelty` bin choice**: fixed semantic thresholds (`<0.50`, `0.50-0.75`, `0.75-0.90`, `0.90-0.95`, `>0.95`); the genuinely-isolated `<0.50` slice carries the headline FLB. Because up/down is excluded from the primary view, these are the up/down-excluded fixed-threshold numbers (the audit's recommended follow-up).
6. **Closing window (80-100%) includes settlement-print artifacts**; closing-line FLB on large/recurring slices may partly be mechanical close-out, not learning failure.
7. **Offset vs slope**: a uniform nonzero level (Sports/Esports) is a calibration offset, not FLB; the D10−D1 spread reported here is ~0 for those categories. The offset and its two-regime (mature-win/close-lose) structure are analyzed in `data_exploration.md`.
