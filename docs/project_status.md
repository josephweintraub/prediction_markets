# Project status

**Status date:** 2026-09-06

**Current stage:** repository stabilized; corrected candidate results regenerated

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
2. The lowest liquidity-rate equal-dollar group exhibits two-tail classic FLB when trades
   are weighted equally, but this is not robust to equal-market weighting: both market-
   weighted tails are positive and the spread reverses sign. The old generic “bottom
   quarter” claim is therefore a trade-composition result.
3. Standalone binary markets lasting at least 90 days exhibit classic FLB under count and
   equal-market weighting. The full-profile result survives the exploratory Bonferroni
   correction; shorter bins are not stable across weighting schemes.
4. Fine semantic market families show much more heterogeneity than broad categories.
5. Markets with no close textual precedent appear to exhibit FLB, but the embedding
   artifacts predate the latest refresh and omit 7,453 newly resolved markets.
6. Some sub-day markets change tail signs between mature and closing windows, so a
   full-lifecycle average can conceal opposing within-market phases.

The regenerated values and validation record are in
[`runs/2026-09-06_headline_corrected_v1.md`](runs/2026-09-06_headline_corrected_v1.md).

## Remaining publication work

- Full-window headline schemes have been regenerated, but mature and closing lifecycle
  diagnostics still use historical artifacts and must be rerun before a final report.
- Existing renderers read the old mutable output namespace and show raw significance stars;
  they must be changed to require an immutable run and adjusted values.
- Pre-refresh and post-refresh reusable inputs still coexist in the EBS analysis namespace;
  the active driver guards its declared inputs, but older scripts do not.
- Embedding-derived novelty features remain aligned to the older 850,015-market ordering
  and must be refreshed before novelty findings can be promoted.
- The current liquidity measure is realized volume per day and must not be interpreted as
  exogenous or causal.

## Next research step after stabilization

Construct and run the pre-specified cross of standalone binary markets, duration of at
least 90 days, and liquidity-rate tier, with count-, dollar-, and equal-market-weighted
estimates. The manifest, vintage checks, estimand, and multiplicity machinery now exist;
the current `hor_x_liqrate` scheme is descriptive across all market structures and is not
a substitute for that standalone-binary cross.
