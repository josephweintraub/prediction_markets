# Reply memo — FLB heterogeneity follow-ups (2026-08-15)

Answers to the four questions, from fresh runs on the rebuilt data. All
numbers come from committed-script artifacts (`analysis/embedding_difficulty/`,
report v4, sections 5b and 4/4b).

## 0. One data note before the answers

Two silent data problems affected every standard-filter run made on the
June-extended trade set before 2026-07-03, and the earlier results you read
likely inherit them: (a) the resolution join covered only ~49% of trade rows
(stale spine — silently halved the sample); (b) the up/down exclusion filter
matched nothing on newer markets (empty eventSlug), so ~135M mechanical
crypto-series trades leaked into "filtered" runs. Both are fixed in shared
plumbing now (`scripts/build_market_flags.py`). Everything below is measured
on the fixed data. Where your summary and these numbers disagree, the
plumbing fix is the first suspect, not a real change in the market.

Sign convention: **signed calibration slope** — per-slice regression of trade
return (win − price) on price. 0 = calibrated; > 0 = classic FLB (longshots
overpriced); < 0 = reverse (longshots underpriced). "Negative FLB" in your
email = negative slope here. SEs are 3-way clustered (day × wallet × market).
Windows: mature (25–80% of contract lifetime) and closing (80–100%).
Horizon = market creation → close (time-on-market).

## 1. Your four bullets, checked against the rebuilt data

- **"Liquidity and horizon show up consistently" — yes, both confirmed.**
  Liquidity is the strongest gradient we have: slope +0.098 (t=+32) for <$1k
  markets, +0.030 (t=+7.6) for $1–10k, ≈0 above $10k; within-era, only the
  top volume quintile is calibrated. Horizon (mature window): <1d −0.081
  (t=−4.1), 1–7d −0.020 (t=−3.1), 7–30d −0.027 (ns), 30–90d +0.016 (ns),
  ≥90d +0.035 (t=+1.9) — your negative-short / positive-long shape, with the
  long end only marginally significant count-weighted.
- **"Negative FLB short-horizon" — confirmed, and see §2 for what drives it.**
  One important nuance: this is a *mature-window* phenomenon. In the closing
  window (last 20% of life) the <1d bin FLIPS to +0.067 (t=+5.7; dollar-
  weighted +0.149, t=+7.0) — classic longshot overpricing right before
  resolution in ultra-short markets.
- **"FLB for a bunch of categories" — more precisely: three groups.**
  ≈Calibrated (crypto price, mainstream sports outcomes, social/mentions);
  significantly negative (tennis/match markets, esports, weather — strongest
  dollar-weighted, e.g. weather −0.088, t=−7.1); positive pockets that are
  mostly long-horizon judgment cells (Economy ≥90d +0.163, t=+6.3; Politics
  30–90d +0.054, t=+3.4; Tech +0.06–0.11) and novelty tails, not whole
  categories.
- **"Learning results not present any more" — see §6.**

## 2. Q1 — what drives the negative FLB at short horizons?

Both composition AND a within-category gradient, in that order:

- **Composition (the bigger part).** The short-horizon bins are dominated by
  recurring match/outcome families that are longshot-underpriced everywhere:
  Sports <1d −0.118 (t=−3.9), Esports 1–7d −0.132 (t=−4.2), Weather 1–7d
  −0.026 (t=−4.6; dollar-weighted −0.114, t=−12.0). These three families
  carry most short-horizon trades. A small <1d crypto cell is also sharply
  negative (−0.41, t=−3.4, N=51K trades).
- **Within-category gradient (also real).** Inside most non-sports
  categories the slope still rises with horizon: Crypto (−0.41 <1d → +0.05
  ≥90d), Culture (−0.04 → +0.07), Economy (+0.02 → +0.163***), Politics
  (−0.09 <1d → +0.05*** 30–90d), Weather (−0.026*** → +0.123**). Sports is
  the exception: ≤0 at every horizon (it's match structure, not horizon).
- Mechanically, "negative slope" in these families = favorites win less often
  than their price implies / longshots win more — the opposite tail error
  from the long-horizon judgment markets.

## 3. Q2 — where are the dollars, and is #trades a good proxy?

Per horizon bin (mature-universe markets, standard-filtered BUY):

| horizon | markets | share of trades | share of dollars | $ per trade |
|---|---:|---:|---:|---:|
| <1d | 255,957 | 6.1% | 5.0% | $92 |
| 1–7d | 364,682 | 37.2% | 29.9% | $90 |
| 7–30d | 195,348 | 27.2% | 25.4% | $105 |
| 30–90d | 20,594 | 13.3% | 17.8% | $151 |
| ≥90d | 11,922 | 16.2% | 21.8% | $151 |

So trade counts are a reasonable but horizon-biased proxy: they overweight
sub-7-day markets by ~1.25× and underweight 30d+ markets by ~0.75× relative
to dollars (tickets are ~$90 short vs ~$151 long). Where it matters most:
the <1d mature-window negative slope is −0.081 count-weighted but only −0.016
(ns) dollar-weighted — the short-horizon reverse-FLB is disproportionately a
small-ticket phenomenon; the closing-window <1d positive slope is *stronger*
in dollars (+0.149 vs +0.067). We report both weightings everywhere.

## 4. Q3 — which "learnability" proxies have we tried, and what did they show?

All measured in one engine (identical filters/SEs) on the fixed data:

| Proxy | Construction | Result (mature window) |
|---|---|---|
| Embedding novelty at birth | mean cosine sim to 25 nearest predecessors, strict backward discipline, same-series/event excluded, within-vintage deciles | **Tail effect: most-novel decile +0.066 (t=+3.9); all other deciles ≈ 0**; persists at ~half strength in the closing window; survives a $10k liquidity floor (+0.063, t=+3.1) |
| Precedent count (τ-neighbors) | # predecessors above a calibrated similarity threshold | Flat — having *no* close analog matters; the count of analogs does not |
| Action-type precedent | # prior markets with same LLM-extracted action type | Never-seen actions +0.070 (t=+2.7), fading monotonically with precedent; attenuated but same shape under vintage control |
| Subject precedent | # prior markets sharing a subject | Flat everywhere — subject familiarity doesn't help; action familiarity does |
| Series membership | native Polymarket series (recurring instances) | ≈ no count-weighted difference (in-series +0.000 vs standalone −0.001) |
| Recurrence cadence | native daily/weekly/monthly/annual | daily −0.043 (t=−4.3) vs annual +0.068 (t=+2.1), monthly +0.047 (t=+1.8) — tracks the family signs (daily = sports/weather), not a clean learning gradient |
| Anchorability | resolution source named (price feed / official scorer) vs blank (judgment) | sourced −0.025 (t=−3.4; dollar −0.094, t=−5.2) vs judgment +0.011 (ns) — anchored markets err toward longshot-underpricing, judgment markets ≈ calibrated/slightly positive |
| Field variants | novelty from rules-text and event-context embeddings + combined weightings | Same-direction but weaker tails (rules t≈1.7, context t≈1.1); question text is the sharpest signal; the fields genuinely disagree about what is novel (q↔context corr 0.36) |

So it is not that "we found no relationship." The relationships are (i)
highly concentrated (a tail, not a gradient), and (ii) specific about the
channel: *absence of any close analog* and *unfamiliar action types* — not
generic repetition, not subject familiarity, not analog counts.

## 5. Q4 — category-by-category?

Yes — the novelty-tail test run inside each category (tail = the category's
markets in the most-novel within-vintage decile; diff = tail − rest slope):

- **Positive (novel ⇒ more classic-FLB) in 8 of 12 categories**, largest:
  Culture +0.101 (tail +0.086, t=+3.0), Politics +0.086 (tail +0.104,
  t=+3.7), Iran +0.219 (ns), Geopolitics +0.085, Crypto +0.072, Esports
  +0.163 (tail dollar-weighted +0.53, t=+5.1), Mentions +0.065 (dollar tail
  +0.20, t=+3.3), Weather +0.035.
- **The substantive exception is Sports** — diff −0.078 (tail −0.086,
  t=−2.7): novel sports markets are *more* longshot-underpriced, i.e. the
  sports family's own error direction gets amplified rather than flipping to
  classic FLB. Tech is ≈ flat (−0.013); Finance and Economy are negative on
  small tails (76K and 24K trades).
- Also relevant: category granularity is coarse — the noise-corrected
  dispersion of true slopes keeps rising as slices get finer (signal SD
  0.032 across 12 categories → 0.076 at k=1000 embedding clusters), so
  category-level nulls can hide offsetting within-category structure.

## 6. On "the learning results we had before don't seem to be present any more"

Reading that as: the earlier writeups' learnability gradients look absent in
the current results. Three things are true at once:

1. **The earlier numbers shouldn't be trusted as a baseline.** They predate
   the spine/up-down fixes (sample silently halved; 135M mechanical trades
   leaking in), and several headline patterns of that era were separately
   retired as D10−D1 artifacts (docs/methods_reference.md, "Retired claims").
2. **Broad monotone "experience" gradients are indeed weak on the fixed
   data.** Series membership, recurrence cadence, precedent counts, subject
   familiarity — none is a clean learning gradient. If "the learning
   results" meant those, they are genuinely not there.
3. **A sharper learning result replaced them.** Difficulty shows up as a
   threshold phenomenon: markets with *no close precedent* (~10% most novel
   of each era) and markets with *never-before-seen action types* are
   miscalibrated in the classic FLB direction; the effect shrinks toward the
   close and with precedent accumulation, survives liquidity floors and
   vintage controls, and generalizes across 8 of 12 categories. That is a
   learnability story — concentrated where learning is impossible, rather
   than smooth where it is easy.

Caveats on everything above: resolution censoring (only markets resolved by
build time are in the trade set — long-horizon/late-sample cells are
horizon-censored, so the ≥90d bins under-represent still-unresolved markets);
bot flags predate the June data extension; volume floors condition on an
outcome (sensitivity checks, not causal controls); horizon here is
time-on-market, not time-to-event-announcement.
