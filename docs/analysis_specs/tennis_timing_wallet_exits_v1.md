# Tennis timing and terminal wallet sequences

Status: scoped investigation, 2026-10-02. Existing completed runs are not overwritten.

## Questions

1. Does the ATP calibration profile change in Grand Slam matches, and in a subset
   with independently evidenced actual match boundaries?
2. Is terminal trading associated with disposal of eventual-winning tokens or
   purchases of their losing complements by wallets with earlier winner purchases?

These are exploratory descriptive tests, not causal attribution or a trading strategy.

## Frozen cohort and time

Use the accepted resolved moneyline cohorts from the September 2026 multisport
artifacts. Tennis remains ATP; WTA and UFC are not reintroduced. Grand Slam
classification requires a unique archived match with `tourney_level=G` and agreement
with provider tournament identity, not an inference from calendar dates.
Match an archived Slam using the provider's tournament identity, local competition
date, player pair and winner. A market listing date is not a match date. The archive
uses tournament-week Mondays, so a documented Sunday opening permits a one-day
lead relative to that Monday. An old synthetic duration must not be used as a gate
that hides an incorrect prior-tournament assignment: preserve it, recover the
correct archive identity, and explicitly flag duration disagreements. Preserve
retirements and other irregular or conflicting records as exclusions.

Existing ATP boundaries are ESPN scheduled start plus archived match duration.
Label this clock explicitly. A strict actual-clock subset requires direct evidence
of first competitive play and final competitive play with explicit wall-clock
semantics, timezone, precision and match identity. Scheduled starts, synthetic ends,
last trades and price polarization do not qualify. Preserve exclusion reasons and
report an empty strict subset if no such evidence is obtainable. An observed end
alone is useful for an end-relative check but does not verify normalized match time.

Source check on 2026-10-02 found an official Australian Open `actual_start_time`
(minute precision, Australia/Melbourne) and native timestamped final competitive
commentary record. Use this as a separately labeled **provider-recorded actual
clock**, not certified exact first serve or second-exact physical ground truth.
Preserve both native fields and first-point-completion diagnostics. Require
year-specific results/detail identity, winner, final-score and completion agreement.
Compare corrected and existing clocks on identical accepted AO match IDs to
separate clock correction from cohort selection. A fully certified first-serve
cohort remains unavailable unless additional evidence establishes that semantics.
Require unique competitive point IDs, a terminal point that is last in logical
point order, and nondecreasing timestamps in that order. Preserve the initial
boundary-only audit, but use a new immutable replay with this stricter chronology
gate for the main provider-clock comparison. This check is independent of trades,
prices and calibration results. Consistent chronology does not establish zero
provider latency.

Normalized time is `(trade UTC - start UTC)/(end UTC - start UTC)`. Live includes
`0 <= T <= 1`; terminal windows are `[.80,.90)`, `[.90,.95)`, `[.95,.99)`, `[.99,1]`
and the last 120 seconds inclusive. No grace-period exclusions. Earlier market
history remains available for prior-purchase sequence checks.

## Trading-direction contract

Wallet action must be established from that wallet's own order fields. A retained
maker record with collateral asset zero and a nonzero outcome asset is a maker BUY;
the reverse is a maker SELL. The opposite counterparty action is not implied:
matched orders can BUY complementary tokens through minting or SELL complementary
tokens through merging. Do not infer sales or purchases for the counterparty from
the retained maker event alone.

The initial wallet test uses verified maker actions from the frozen resolved-trade
source. Deduplicate exact replays without collapsing legitimate partial fills;
require unique original log identities and token/outcome/timestamp joins. Preserve
fees and gross quantity/cash with their units. No existing pipeline is rewritten.

## Measures

Calibration is `Y-P`, in percentage points; fixed probability bins are the existing
ten equal-width price bins. The tennis clock/cohort comparison deliberately holds
the legacy exposure-normalized fill selection fixed. It is not a new validation
of the counterparty's purchased token. The separate wallet analysis uses verified
own-maker BUY and SELL actions only. Retain the complete profile and summarize D10 minus D1.
Compare existing ATP and Grand Slam profiles with identical sample rules. Apply
the existing 500-observation per-bin/tail suppression rule and show counts/status.
Show per-fill, dollar-weighted and paired equal-event tail summaries. Dollar
weight is gross executed collateral amount, not eventual payout. Classic FLB
means D1 < 0 and D10 > 0; a positive D10-minus-D1 spread alone is insufficient
to label the full price profile. Point estimates without estimated uncertainty
must be labeled descriptive.

For wallet history, include all available prior maker actions regardless of focal
price or nonhuman-wallet filters. Focal filtered observations use `.01<P<.99` and
exclude flagged actors; the all-actor comparison uses `0<P<1`.

For terminal eventual-winner SELLs, record a strictly prior same-wallet, same-token
maker BUY outside the current transaction. For terminal longshot BUYs, record a
strictly prior same-wallet maker BUY of the unique complementary eventual winner.
Report fill-, quantity- and cash-weighted shares separately, prior acquisition
prices and delays when available. Recompute maker-BUY full-bin calibration with
and without the complementary-prior-purchase group.

A SELL is direct evidence of disposal at that moment. An earlier BUY is an observed
sequence, not proof that the earlier position remained open or that disposal was
profitable. Maker-only history omits taker actions, transfers, splits, merges,
redemptions and opening balances. Do not call cumulative maker trading inventory,
and do not describe descriptive attenuation as causal explanation. A prior BUY
of the eventual-winning complement retrospectively selects a losing-side focal
BUY, so the exclusion can mechanically change calibration even without an exit.

## Deliverables and gates

Committed deterministic scripts, focused synthetic tests, immutable saved
Parquet/JSON summaries, reconciliation tables and manifests. A concise portable
LaTeX report presents coverage, Grand Slam comparisons, trading direction,
terminal sequences and full calibration profiles. Actual-boundary coverage and
direction limitations belong beside the affected results. No generic closing
limitations section or reader-facing fingerprints.

Production scans run on EC2, serialized while sharing source/cache state. Only
the root manages lifecycle and stops/verifies the instance after all work and
transfers finish.
