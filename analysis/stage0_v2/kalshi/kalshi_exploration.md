# Kalshi Exploration — Step 1 Findings

Source: `/mnt/data/kalshi/kalshi_contract_questions_dates_available.parquet` (3.88 GB, on EC2)

## 1a. Structural inventory

| Metric | Value |
|---|---|
| Total rows | 34,504,792 |
| Distinct tickers | 34,504,792 |
| Distinct event_tickers | 11,008,162 |
| Distinct question texts | 27,488,803 |
| Distinct ticker prefixes (alphanumeric root) | **7,166** |

**Key correction vs. the original plan**: every row is a unique ticker (`raw_rows_for_ticker = 1` for all 34.5M). There are no daily snapshots in this dataset — it's already one-row-per-ticker. Question text is stable per ticker (verified on 200 random samples: 200/200 had exactly 1 distinct question). The Step 5 aggregation logic from the plan can be simplified to a no-op pass-through.

**Output target revised: 34.5M rows** (not 11M as initially assumed).

## 1a.1. Prefix taxonomy — the MVE problem

The top 30 ticker prefixes cover **33.7M of 34.5M tickers (97.7%)**. Critically, the top 5 are all **multi-variate event (MVE) parlay markets** that account for **~28M tickers (~81%)**:

| Prefix | Tickers | Events | Questions |
|---|---:|---:|---:|
| `KXMVESPORTSMULTIGAMEEXTENDED` | 16,838,583 | 7,848,039 | 15,899,495 |
| `KXMVENFLSINGLEGAME` | 5,102,078 | 509,779 | 4,643,918 |
| `KXMVECROSSCATEGORY` | 2,111,323 | 1,361,878 | 2,016,292 |
| `KXMVENFLMULTIGAMEEXTENDED` | 1,974,043 | 757,261 | 1,882,573 |
| `KXMVENBASINGLEGAME` | 1,797,505 | 229,137 | 1,595,884 |

MVE question text is a **list of "yes/no" parlay legs** rather than a single bettable proposition. Examples:
- `yes Cincinnati,yes Buffalo,yes Carolina,yes Green Bay,yes Los Angeles C,yes Dallas`
- `yes Donovan Mitchell: 2+,yes Donovan Mitchell: 2+,yes CJ McCollum: 2+,yes Pascal Siakam: 20+,...`
- `yes Bayern Munich,yes Wolfsburg,yes Arsenal,yes Chelsea,yes Manchester City,...`

Each parlay is unique by composition, so the question-level cardinality (27.5M distinct questions / 34.5M tickers ≈ 80% distinct) is driven primarily by these MVE markets.

The next 25 prefixes are conventional single-contract markets — S&P, Nasdaq, BTC, ETH, FX pairs, NHL/NBA player props, NCAA spreads, Oscars predictions, etc. Heavy templating per family — e.g. `KXBTCD` has 587,762 tickers but only 508 distinct questions (1 question template covers ~1,150 tickers on average).

## 1b. Format inventory (sampled from 10K random questions)

### Date patterns
| Format | Hits (of 10K) | Example |
|---|---:|---|
| `month_day_no_year` | 2,782 | "Jan 14" |
| `long_month_day_year` | 1,593 | "Jan 14, 2025" |
| `time_with_tz` | 1,079 | "12pm EST" |
| `iso_datetime` | 0 | — |
| `iso_date` | 0 | — |
| `slash_date` | 0 | — |
| `quarter` | 0 | — |
| **no_date_recognized** | **5,651** | (most MVE parlays carry no date in `question`) |

### Strike / number patterns
| Format | Hits (of 10K) | Example |
|---|---:|---|
| `threshold_above_below` | 3,640 | "above 5525", "below 1.02800" |
| `decimal_1` | 3,016 | "25.5" |
| `decimal_2plus` | 641 | "1.07339" |
| `range_between` | 17 | "between X and Y" |
| `dollar_amount` | 3 | "$5,525" |
| `comma_grouped` | 1 | "5,549.99" |
| **no_strike_recognized** | **6,214** | (MVE parlays + plain-English markets) |

### Digit-count distribution
- **0 digits**: 1,355 (mostly MVE parlay lists; some plain-English political/cultural Qs)
- **1–5 digits**: 2,173 (single strike + date)
- **6–14 digits**: 4,880 (one strike + multiple dates / IDs)
- **15+ digits**: 1,592 (multi-strike parlays, NBA/NFL player-prop lists)

### Inferences for normalization
- Two date forms are sufficient: `Mon DD, YYYY` and `Mon DD` (no ISO / slash / quarter forms appear).
- Strikes are mostly **plain decimal** numbers in context of words like "above" / "below" / "between" — no $-prefix, no commas in 99.97% of cases.
- The MVE families need a **different normalization strategy** from individual markets.

## 1c. Multi-snapshot audit

- `raw_rows_for_ticker = 1` for all 34.5M tickers. No snapshot duplication.
- Question stability: 200/200 sampled tickers have exactly 1 distinct question.

## 1d. Design implications

### Normalizer split

The MVE prefixes need their own handler because their `question` text is a parlay leg list, not a bettable proposition. Strategy:

- **For non-MVE prefixes** (small markets, ~6.8M tickers): normalize `question` text — replace decimal numbers with `<NUM>`, dates with `<DATE>`, times with `<TIME>`. Estimated post-normalization template count: 2K–10K (KXBTCD already has 508 distinct questions for 588K tickers; conventional dedup will push this lower).

- **For MVE prefixes** (parlay markets, ~28M tickers): normalize the parlay structure instead of trying to template the leg list. Replace the entire selection list with `<SELECTIONS>`, keep just the prefix as the family identifier. E.g. `KXMVENFLSINGLEGAME` parlay questions → "yes <SELECTIONS>" template. The five MVE families collapse to ~5–20 templates total.

- **Long tail** (~7,100 one-off prefixes): single tickers each, no dedup possible. Each gets its own LLM call. Cost upper bound: 7K templates × cost-per-call.

### Revised template-pair count estimate

| Family | Tickers | Estimated templates after Stage 0 |
|---|---:|---:|
| Top 5 MVE prefixes | ~28M | ~20 (the 5 prefixes × 4 variants each) |
| Top 25 non-MVE prefixes | ~6.5M | ~5,000 |
| Long tail (1-ticker prefixes) | ~200K | ~7,000 |
| **Total** | **34.5M** | **~12K templates** |

### Revised LLM cost estimate

At Polymarket's $200 for 108K templates with caching (≈ $0.0018/template), 12K templates → **~$22**.

This is much cheaper than the original $30–80 estimate. We should still validate with a 50-prompt batch (G2) before committing the full batch (G3).

### Open question for the user

The MVE parlay normalization collapses millions of distinct contracts into a small number of templates. Each ticker keeps its original question text in the output dataset, but the LLM extraction (subjects, action, info_type, etc.) will be at the parlay-family level, not at the per-parlay level. **Is that the desired granularity?**

Two alternatives if not:
1. **Per-leg sampling**: parse the parlay legs and classify each as if it were a standalone contract. Adds complexity, more LLM cost, but gives per-contract semantic info.
2. **Skip MVE entirely**: emit non-MVE rows only (~6.8M output). Cleaner for FLB analysis but drops 80% of the dataset.

I'll proceed with the family-level approach by default since it matches the Polymarket precedent (template-level abstraction, not per-instance). Flag if you want to revisit.
