# Project status

**Status date:** 2026-09-06

**Current stage:** repository stabilization before the next confirmatory analysis

**Active workstream:** `analysis/calibration_heterogeneity/`

This document is the short source of truth for where the project stands. Durable methods
belong in `methods_reference.md`; methodological choices and reversals belong in
`decisions.md`; superseded findings belong in `archive/`.

## Research question

How does prediction-market calibration vary with characteristics that may make pricing
easier or harder? The current candidate explanations are liquidity, market duration,
semantic market family, and textual novelty or precedent. Direction is measured rather
than assumed.

## Current data vintage

- Canonical trade set: `/mnt/data/pipeline_output/trades_clean.parquet`
- Refreshed row count: **2,036,128,538**
- Last underlying trade date: **2026-06-23**
- Canonical market/token spine: `/mnt/data/pipeline_output/market_flags.parquet`
- Bot-wallet flags: `/mnt/data/learnability/cache/wallet_flags.parquet`
- Bot-wallet build used for the current rebaseline: **333,676 flagged wallets**
- Non-up/down analysis universe: **857,468 markets**
- Markets with at least one standard-filtered trade: **517,963**
- Standard-filtered non-up/down BUY observations: **99,744,791**

The trade set contains resolved markets only. Recent observations are therefore censored
toward faster-resolving markets. Any recency or horizon result must address this directly.

## Current measurement specification

- BUY-side rows; `0.01 < price < 0.99`; timestamp on or after 2020-06-01.
- Exclude bots and exclude up/down markets at market level via `market_flags.parquet`.
- Minimum 5,000 trades per reported slice.
- Full-lifecycle window is primary for the current liquidity/maturity analysis; mature
  (25-80%) and closing (80-100%) windows are diagnostic.
- Primary diagnostic: full ten-decile calibration profile.
- Headline summaries: D1, D10, and D10-D1; classic FLB requires D1 < 0 and D10 > 0.
- Signed calibration slope is auxiliary.
- Report count- and dollar-weighted estimates. Equal-market weighting is required as a
  robustness check before publication-level claims about markets.
- Standard inference: Cameron-Gelbach-Miller three-way clustering by day, wallet, and
  market.

## Standing descriptive findings

These are working findings, not finalized causal claims:

1. Aggregate full-lifecycle calibration is close to flat, with a positive favorite tail.
2. The bottom quarter of economic volume exhibits two-tail classic FLB; the top three
   equal-dollar groups are substantially closer to calibrated.
3. Standalone binary markets lasting at least 90 days exhibit classic FLB, while shorter
   standalone binaries are broadly calibrated.
4. Fine semantic market families show much more heterogeneity than broad categories.
5. Markets with no close textual precedent appear to exhibit FLB, but the embedding
   artifacts predate the latest refresh and omit 7,453 newly resolved markets.
6. Some sub-day markets change tail signs between mature and closing windows, so a
   full-lifecycle average can conceal opposing within-market phases.

## Known blockers to publication-quality inference

- Existing saved summaries predate the 2026-09-06 correction to D10-D1 covariance and
  thin-tail handling. Their point estimates are unchanged, but spread SEs and t-statistics
  must be regenerated before use.
- Exploratory grids currently use unadjusted significance markers despite the documented
  multiplicity rule.
- Current point estimates are trade- or dollar-weighted, not equal-market-weighted.
- Artifact reuse is based mainly on filename existence rather than input fingerprints.
- Pre-refresh and post-refresh artifacts coexist in the same EBS namespace.
- The current liquidity measure is realized volume per day and must not be interpreted as
  exogenous or causal.

## Next research step after stabilization

Run the pre-specified cross of standalone binary markets, duration of at least 90 days,
and liquidity-rate tier, with count-, dollar-, and equal-market-weighted estimates. Do not
begin that run until the artifact manifest, data-vintage checks, and equal-market estimand
exist; the initial engine tests were added on 2026-09-06.
