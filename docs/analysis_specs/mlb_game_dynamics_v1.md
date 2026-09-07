# MLB moneyline game dynamics: exploration v1

**Work state:** exploration
**Purpose:** establish whether calibration changes between the pregame and live phases of
MLB moneyline markets before adding richer measures of game or market complexity.

This is a deliberately minimal first specification. Its outputs may motivate a candidate
analysis, but they are not confirmatory evidence and must not be described causally.

## Provisional coverage

A preliminary audit identified **3,790** candidate MLB single-game moneyline markets,
**3,788** with filtered trading, spanning **April 2025 through June 2026**. They contain
approximately **2.05 million** filtered BUY observations and **$412 million** of filtered
volume. These figures are provisional until the reproducible market-to-game match and
trade-filter reconciliation pass the gates below.

## Fixed v1 decisions

### Universe and sources

- Include Polymarket markets that resolve a single MLB game's winner. Exclude props,
  series and season outcomes, run lines, totals, and non-MLB baseball.
- A market enters only when its teams and game date map unambiguously to one official MLB
  game. The two observed event-slug positions are first tested as official away/home
  order; reversed order is tested only if no official-order game exists. Official
  away/home identity comes only from the MLB schedule, and the audit records whether slug
  order is `official` or `reversed`. More than one game within the selected orientation
  remains ambiguous. This is data-linkage validation, not a filter on market price or
  confidence.
- Use the canonical resolved-trade vintage and standard filters: BUY side only,
  `0.01 < price < 0.99`, bot-wallet exclusion, and market-level up/down exclusion.
- Require the exact block-timestamp cache used in the canonical trade build. The unused
  linear block-time approximation is not admissible for phase assignment.
- Use MLB game data as the primary timing source: actual first play, timestamped inning
  transitions, and final play. ESPN may be used only to diagnose or fill documented MLB
  API gaps; source and disagreement must be recorded per game.

Postponed, suspended, resumed, doubleheader, shortened, and otherwise irregular games are
not silently discarded. They must either map to the correct MLB game identifier and
complete timing sequence or appear in the match-exclusion audit with a reason.

### Time phases

Let `t` be the exact block timestamp assigned to a trade. Use half-open intervals so each
eligible trade has one phase:

| Phase | Fixed definition |
|---|---|
| Pregame | `t < actual first-play time` |
| Innings 1-3 | first play through the start of inning 4 |
| Innings 4-6 | start of inning 4 through the start of inning 7 |
| Innings 7+ | start of inning 7 through the final-play time, including extra innings |
| Post-final | after the final-play time; excluded |

The phase boundaries come from official play timing, not scheduled start time or
Polymarket resolution time. Trades in blocks whose timestamp equals a boundary receive the
later phase, except that a trade at the recorded final-play second remains in innings 7+.

### Core calibration analysis

- **Observation:** an eligible BUY-side trade in a matched game moneyline market.
- **Probability:** the price of the bought outcome.
- **Outcome:** one if that outcome won the game and zero otherwise.
- **Calibration error:** `outcome - price`, reported in probability points (and multiplied
  by 100 only when displayed as percentage points).
- Run the existing fixed-width ten-bin price profile separately for pregame, innings 1-3,
  innings 4-6, and innings 7+. Report every bin's implied probability, win rate,
  calibration error, standard error, trade count, dollar volume, and game count.
- Retain D1, D10, D10-D1, and the signed calibration slope using the existing definitions.
  Do not infer a phase pattern from the tail spread unless the full profile supports it.
- Apply the existing minimum of 5,000 trades per reported phase and suppress a tail with
  fewer than 50 trades. Also expose game counts so a large trade count cannot conceal a
  very small effective game sample.

Report three versions of each trade-phase profile:

1. **Count weighted:** each eligible trade receives equal weight.
2. **Dollar weighted:** weight by trade USDC.
3. **Equal game:** within each game x price bin, dollar-weight trades, then average games
   equally. This is the MLB version of the project's equal-market estimator.

### Closing line

The **closing line** is the last eligible traded price strictly before actual first play.
Express it as the home-team win probability: retain the price when that trade's bought
contract pays on a home win and use `1 - price` when it pays on an away win. Its realized
outcome is correspondingly one for a home win and zero for an away win. It is one
prediction per game, not `closing probability - earlier trade price` and not a trade-level
CLV measure.

Report a separate ten-bin closing-line calibration profile using one equally weighted
observation per game. Count and equal-game weighting therefore coincide; the notional of
the final trade is recorded for auditing but is not a primary closing-line weight. Report
the close's age in seconds (`first play - closing trade timestamp`) alongside the profile.

### Inference and multiplicity

- For trade-phase profiles, use Cameron-Gelbach-Miller clustering by UTC trade day,
  wallet, and MLB game. Equal-game estimates use the corresponding game-normalized trade
  scores.
- For the one-observation-per-game closing profile, cluster by game date; do not present
  the final-trade wallet as an independent source of closing-line uncertainty.
- Within each weighting, the four phases x ten bins form one exploratory decile family.
  Within each weighting and summary estimand, the four phases form one summary family.
  The closing-line ten-bin profile is a separate family.
- Retain raw two-sided normal p-values, Bonferroni values, and Benjamini-Hochberg FDR
  values. Exploratory displays use Bonferroni-adjusted markers by default.

### Minimal descriptive movement measure

Only after the core phase and closing-line outputs pass validation, normalize every trade
to the home-team probability using the same complement rule as the closing line, then
compute the unweighted sample variance of that probability within each game x phase.
Report its distribution and its
relationship to phase-level calibration descriptively. Call this **implied-probability
variance** or **price-path variation**, not inherent complexity, predictability, or a
causal treatment. Any stratified hypothesis test based on it requires a subsequent
specification.

## Audit-dependent choices

These choices must be made from coverage and timing diagnostics before viewing calibration
estimates, then recorded in the immutable run manifest:

- **Boundary buffer:** first run the unbuffered classification and count observations
  within 5, 10, and 30 seconds of first play, inning transitions, and final play. Introduce
  the smallest symmetric exclusion buffer only if block-level timing ambiguity is
  material; retain the unbuffered counts and sensitivity result.
- **Stale closing fallback:** report the distribution of closing-line age and the share of
  games exceeding predeclared diagnostic thresholds (5 minutes, 30 minutes, 2 hours, and
  24 hours). The v1 close remains the last pregame trade. If staleness is material, stop
  and specify a separate fallback (such as a short-window VWAP or midpoint) before
  comparing calibration results; do not choose it after seeing those results.
- **Timing-source exceptions:** define any ESPN fallback rule and permitted MLB/ESPN
  discrepancy from source-coverage diagnostics, before outcome-linked analysis.

## Validation gates before estimating calibration

1. Validate the canonical data-vintage manifest, exact-timestamp provenance, schemas, and
   standard-filter reconciliation.
2. Produce a one-row-per-candidate match table with Polymarket identifiers, MLB game ID,
   observed slug teams, slug orientation, canonical schedule away/home teams, dates,
   match status, and an explicit exclusion reason. Manually inspect a small reproducible
   sample, including reversed slugs, doubleheaders, and irregular games.
3. Require one-to-one market/game matches for the analysis set and verify winner/outcome
   agreement. Duplicate mappings or contradictory resolutions fail the run.
4. Verify ordered first-play, inning-boundary, and final-play timestamps; report games
   missing the start of inning 4 or 7 rather than guessing a boundary.
5. Reconcile total, phase-assigned, near-boundary, post-final, and excluded trade counts
   and dollars. Assigned plus excluded rows must equal the filtered matched input.
6. Complete the boundary and closing-staleness audits without inspecting calibration
   estimates. Freeze any audit-dependent rule in a dated configuration.
7. Run the synthetic integration fixture and write results only to a new immutable run
   directory with its manifest, match audit, phase counts, closing-age audit, and tables.

## Required v1 outputs

- Market-to-MLB-game match and exclusion audit
- Game timing and timing-source audit
- Phase/boundary count reconciliation and closing-line staleness table
- Four phase-specific calibration tables under all three weighting schemes
- One equal-game closing-line calibration table
- Basic implied-probability-variance summary, produced only after the core validation gate
