# MLB moneyline game dynamics: exploration v1

**Work state:** exploration
**Purpose:** establish whether calibration changes between the pregame and live phases of
MLB moneyline markets before adding richer measures of game or market complexity.

This is a deliberately minimal first specification. Its outputs may motivate a candidate
analysis, but they are not confirmatory evidence and must not be described causally.

## Audited pre-estimation coverage

The production v3 audit retained **3,790** candidate MLB single-game moneyline markets
spanning **April 2025 through June 2026** and admitted **3,697** to the validated
standard-timing moneyline core. The exact filtered trader view contributed **2,509,553**
rows in those markets. A separate raw-fill recency audit found a valid primary pregame
close for all 3,697 games. These are data-coverage facts, not calibration results.

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
- Rebuild from the raw resolved-trade vintage. Remove ingestion replays only by immutable
  EVM event identity. For buyer-level trade calibration, retain BUY observations with
  `0.01 < price < 0.99` and exclude buyers carrying the existing `is_nonhuman` flag.
  Market-close construction is a separate all-valid-fill view and does not use that
  buyer filter.
- Require the exact block-timestamp cache used in the canonical trade build. The unused
  linear block-time approximation is not admissible for phase assignment.
- Only official MLB schedule and live-feed data populate v1 timing artifacts: actual
  first play, timestamped inning transitions, and final play. ESPN may be used only for
  external diagnosis and cannot fill v1 timing gaps.

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
The primary phase analysis uses these literal boundaries. Its fixed sensitivity excludes
any trade whose exact timestamp is within 30 seconds, inclusive, of first play, the starts
of innings 4 and 7, or final play; no observation is reassigned to another phase.

### Core calibration analysis

- **Observation:** an eligible BUY-side trade in a matched game moneyline market.
- **Probability:** the price of the bought outcome.
- **Outcome:** one if that outcome won the game and zero otherwise.
- **Calibration error:** `outcome - price`, reported in probability points (and multiplied
  by 100 only when displayed as percentage points).
- Use ten fixed-width bought-outcome probability bins: `[0, 0.1)`, `[0.1, 0.2)`, through
  `[0.8, 0.9)`, and `[0.9, 1]`. Run the profile separately for pregame, innings 1-3,
  innings 4-6, and innings 7+.
- Report each bin's equal-trade mean implied probability, win rate, mean
  `outcome - price`, standard error, trade count, descriptive dollar volume, and game
  count. Dollar volume is not an estimator weight. Retain the audit row but suppress its
  estimate and label it exploratory when its trade count is below 50.
- The first estimator is the binned descriptive profile only. It does not estimate a
  calibration slope, regression, or relationship to a complexity proxy.
- Each eligible trade receives equal weight in the single v1 phase estimator. There is no
  dollar-weighted or equal-game phase variant in this first pass.

### Closing line

The primary **closing line** is the last exact-timestamp valid fill strictly before actual
first play, with bot participants included. A valid fill has the expected moneyline token,
positive finite amounts, and `0 < price < 1`. Express it as the home-team win probability:
retain the price when the purchased contract pays on a home win and use `1 - price` when
it pays on an away win. Its realized outcome is correspondingly one for a home win and
zero otherwise.

The fixed closing sensitivity uses the existing filtered buyer view: require
`0.01 < price < 0.99` and exclude a fill when its outcome-token buyer has
`is_nonhuman = true`. It is deliberately buyer-centered; it does not exclude a trade when
only its seller/counterparty is flagged, and it is not a human-to-human market series. The
primary-minus-sensitivity (`A - C`) difference is filter attribution, not CLV. Neither
closing definition is `closing probability - earlier trade price`.

Report a separate ten-bin closing-line calibration profile using one equally weighted
observation per game. Count and equal-game weighting therefore coincide; the notional of
the final trade is recorded for auditing but is not a closing-line weight. Apply the same
fixed bins and `n < 50` suppression rule, with `n` equal to games. Report the close's age
in seconds (`first play - closing trade timestamp`) in `game_closes.parquet` and its
reconciliation audit; close age is not a calibration-profile column.

### Uncertainty

- For trade-phase profiles, use Cameron-Gelbach-Miller clustering by UTC trade day,
  wallet, and MLB game.
- For the one-observation-per-game closing profile, cluster by game date; do not present
  the final-trade wallet as an independent source of closing-line uncertainty.
- Report normal standard errors and 95% intervals for nonsuppressed rows. This descriptive
  first pass does not emit hypothesis-test or multiplicity-adjustment columns.

### Deferred work

No price-path variance, complexity proxy, heterogeneity regression, or other structural
model belongs in the first estimator. Such work requires a later specification after the
primary and sensitivity calibration tables are inspected and validated.

## Frozen post-audit choices

- Primary phase rows use literal official MLB boundaries; the sensitivity drops rows
  within 30 seconds, inclusive, of any phase boundary.
- Primary closes use all valid exact prestart fills with bots included; the closing
  sensitivity uses the existing price and buyer-bot filters. Neither definition uses an
  either-participant or human-to-human filter.
- Phase calibration is equal-trade/count-weighted only; dollars are descriptive.
- Closing calibration gives every game equal weight.
- Calibration uses the ten fixed-width bins above. Rows whose effective `n` is below 50
  remain auditable but have their estimates suppressed and are labeled exploratory.

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
6. Complete the boundary and closing-recency audits without inspecting calibration
   estimates, and verify the frozen literal/30-second phase rules and primary/sensitivity
   closing definitions in the estimator manifest.
7. Require the dual-close builder's one-row-per-game output and reconciliation gates to
   pass, including primary/sensitivity coverage, exact-cache proof, and source-fill
   identity reconciliation.
8. Run the estimator's synthetic integration fixture and write production results only to
   a new immutable run directory after independent cross-review passes.

## Required v1 outputs

- Market-to-MLB-game match and exclusion audit
- Game timing and timing-source audit
- Phase/boundary count reconciliation and closing-line staleness table
- One-row-per-game `game_closes.parquet` with explicit `primary_*` and `sensitivity_*`
  fields, plus `reconciliation.json`
- `trade_phase_calibration.parquet`: 80 equal-trade rows spanning two boundary samples,
  four phases, and ten fixed bins; dollars remain descriptive
- `closing_calibration.parquet`: 22 equal-game rows spanning the overall and ten-bin
  profiles for each closing definition
- `closing_paired_sensitivity.parquet`: 11 paired A-minus-C rows, overall plus ten primary
  close bins; this filter-attribution difference is not CLV
- `estimator_summary.json` with input fingerprints, definitions, coverage,
  reconciliation counts, fixed output-row counts, and exploratory status

The dual-close builder and descriptive estimator are implemented in code, passed
independent cross-review, and completed an independently audited production run in the
immutable `07_dual_closes_v1` and `08_calibration_v1` stage directories. The resulting
tables remain exploratory under the fixed-bin and no-multiplicity-inference rules above.
