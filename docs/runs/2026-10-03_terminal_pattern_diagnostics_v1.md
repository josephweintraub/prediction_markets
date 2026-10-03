# Terminal-pattern alternatives diagnostics

Status: production and independent artifact QA passed; lifecycle closure pending.

## Contract

See [the frozen analysis spec](../analysis_specs/terminal_pattern_diagnostics_v1.md).
Preserve the completed profit-taking analysis, its source/ledger stages and the
open LaTeX report. Reuse the exact completed stage-05 inputs and reproduce every
primary tail cell and terminal contrast before optional diagnostics.

New run destination:
`/mnt/data/runs/2026-10-03_terminal_pattern_diagnostics_v1/01_diagnostics`.

Source: `analysis/diagnostics/build_terminal_pattern_diagnostics.py`.
Fixtures: `tests/test_terminal_pattern_diagnostics.py`.

Primary comparison: final normalized `[.99,1]` minus preceding `[.95,.99)`.
Nine inherited resolved moneyline sports, both actual-BUY grains, three original
samples and three original weights. All aggregate estimates retain the
500-original-observation support gate. This stage estimates no uncertainty or
causal effect. Literal post-end windows are diagnostic only.

## Publication and lifecycle

Transfer only the module's compact-output allowlist and summary/manifest.
Market/event membership tables remain on EC2; wallet histories are not exported.
No Dropbox action is authorized by this investigation. Root alone manages
EC2, confirms no active workload remains, then stops and verifies stopped.

## Preflight and tests

Canonical branch: `codex/terminal-pattern-diagnostics`.
Source commit: `c95a5ca09c2840ba554474e93eba9a924ca710dd`.
Clean canonical checkout, mounted verified data volume, no competing workload,
26,100,985,856 free bytes at launch. Root started and mounted the instance.

Independent fixture QA: 40 passed in 9.05 seconds on the Mac. Root rerun:
40 passed in 7.29 seconds. Relevant canonical regression suite:
384 passed in 50.20 seconds, covering the new diagnostic fixtures, existing
attribution/contribution builders and source/ledger/action tests. Two earlier
collection attempts specified nonexistent test paths and ran no tests; the
corrected eight-file canonical suite passed.

Production began `2026-10-03T14:02:12.905642Z` with eight threads, a `100GB`
memory limit and the builder's `12GB` spill cap. Log: the sibling
`01_diagnostics.log`; fresh spill: `/mnt/data/tmp/terminal-diagnostics-v1.RnN5W6`.
The nine source arguments come unchanged from the completed baseline manifest;
the final manifest records the exact command.

Production finished `2026-10-03T14:23:43.061992Z`, exit 0, runtime 1,290.16
seconds. All six saved gates passed: baseline reproduction, exact cross-grain
conservation, price/outcome identity, cent-band reconstruction, sequential
filter identity and disjoint end windows. No spill was used.

Aggregate copies: `output/terminal_pattern_diagnostics_v1/01_diagnostics/`.
Fifteen allowlisted files total 779,705 bytes; root verified all 14 transferred
output fingerprints against the completed manifest (the manifest is the
fifteenth file). Six market/event membership tables stay on EC2. The existing
LaTeX report is unchanged.

Independent QA reproduced all 648 baseline tail cells and 162 contrasts,
cent numerators/shares, both accounting identities, paired filter flags and
literal end-window counts/cash. A bounded audit of saved market sufficient
statistics added 21 zero-failure gates for event argmax/ties, whole-event
removal, concentration, common membership and original/selected weights.
No raw logs, own-action histories or FIFO records were rescanned by QA.

Support counts (supported / withheld): primary tail moments 609 / 39,
terminal contrasts 141 / 21, cent-band conditional means 3,165 / 3,315,
leave-out contrasts 141 / 21, common-market contrasts 129 / 33,
end-window bins 2,013 / 4,467. Price-filter pairs support 45 of 54 rows;
actor and full sequential contrasts support 42 of 54.

## Data choices and calculations

- Actual bought-contract outcome and gross execution price, not home-normalized
  probabilities. Calibration is signed `Y-P`, not absolute error or percentage
  return on invested collateral. Full source support is 31,827,950 own BUY
  events and 37,104,078 genuine matched BUY executions across the inherited cohort.
- All: `0<P<1`, every actor. Interior: `.01<P<.99`, every actor. Filtered:
  Interior plus unflagged actors. Historical SELLs/FIFO labels are reused and
  unchanged; none becomes an extra BUY calibration observation.
- Count weights give every focal BUY equal weight. Dollar weights use gross
  cash. Equal-market weights normalize gross cash within market, tail and window
  before averaging markets. Cent bands preserve their original parent weights.
- Every spread change requires at least 500 original BUY observations in all
  four cells: two tails times two windows. No new intervals or tests of
  significance were estimated.
- End-clock diagnostics use exact block timestamps and four disjoint literal
  windows. ATP retains its scheduled-start/archive-duration proxy, not verified
  actual-match-end timing. Post-end trades never enter the primary contrast.

## Saved findings

All table entries below are matched-execution, count-weighted percentage-point
changes in `D10-D1`, final `[.99,1]` minus previous `[.95,.99)`. A positive change
means an endpoint increase; it does not alone establish the signs of both tails.

### Separate sample filters

Source: `late_identity.parquet` and `filter_contrasts.parquet`.

| Sport | All | Interior | Filtered |
|---|---:|---:|---:|
| MLB | 0.892 | 1.714 | 1.688 |
| NFL | -0.498 | 0.305 | 0.140 |
| NBA | 0.062 | 1.567 | 1.636 |
| NHL | -1.908 | -1.194 | -0.845 |
| CBB | -0.456 | 1.354 | 1.465 |
| CFB | -0.328 | 2.107 | Withheld |
| ATP | -0.262 | 0.383 | -0.170 |
| EPL | 6.378 | 8.031 | 7.664 |
| WNBA | -0.695 | Withheld | Withheld |

NFL and CBB change sign at the price-filter step before actors are removed.
NBA's price-filter difference is 1.506pp versus 0.069pp at the actor step.
The final NBA price filter removes 62.65% of D1 BUYs and 75.54% of D10 BUYs.
The 99-to-100-cent band contains 4,840 of 6,407 final D10 BUYs (75.54%).
These are ordered sample comparisons, not effects of removing bots.
Matched CFB Interior barely passes with final D1/D10 counts 509/734;
its Filtered counts 264/423 fail. Pairwise price comparison remains visible.

### Outcome and within-bin price composition

Source: `terminal_moments.parquet`, `late_identity.parquet`, `price_bands.parquet`.
Exactly `delta(spread)=delta(outcome gap)-delta(price gap)` under the same weights.

| Sport/sample | Outcome-gap change | Price-gap change | Spread change |
|---|---:|---:|---:|
| EPL All | 9.474 | 3.095 | 6.378 |
| EPL Filtered | 10.281 | 2.618 | 7.664 |
| MLB Filtered | 2.513 | 0.825 | 1.688 |
| NBA Filtered | 1.338 | -0.298 | 1.636 |

EPL's increase is primarily an increase in the realized winning-outcome gap
between its two tails; a wider price gap offsets part of it. This accounting
does not establish why the winning/losing mix changes or when results became
publicly known. For example, final Filtered MLB D1 wins 1.189% at mean price
4.926%, and D10 wins 99.593% at mean price 95.023%.

### Game composition and weighting

Source: `event_leaveout.parquet`, `balanced_summary.parquet`, `balanced_moments.parquet`.
The common-market subset requires positive dollars in all four tail/window cells;
it is selected support, not a replacement population or a causal estimate.
Highest-dollar-event removal chooses one game over the four original cells,
removes every proposition of that game and uses the same selection across weights.

| Filtered sport | Original | Largest-dollar game removed | Common-market subset |
|---|---:|---:|---:|
| MLB | 1.688 | 1.692 | 0.077 |
| NBA | 1.636 | 1.628 | 2.099 |
| NFL | 0.140 | 0.050 | 0.041 |
| NHL | -0.845 | -1.446 | -1.665 |
| CBB | 1.465 | 1.471 | 2.527 |
| ATP | -0.170 | -0.089 | -2.848 |
| EPL | 7.664 | 7.343 | 9.447 |

MLB's common subset retains 647 markets and 61.5-73.8% of cell counts.
Its own-order counterpart changes from 1.322pp to -0.578pp. Thus changing
market support materially affects the MLB endpoint comparison, although this
restriction also selects a different trading population. EPL's common subset
retains 223 markets belonging to 131 games and still increases strongly.
EPL All remains 6.434pp after its highest-dollar game's three propositions are
removed, versus the original 6.378pp; a single such game does not explain it.

The direction is not invariant to weighting. In the Filtered matched sample:

| Sport | Per fill | Per gross dollar | Equal market |
|---|---:|---:|---:|
| MLB | 1.688 | -6.816 | 2.103 |
| NBA | 1.636 | -4.930 | 0.627 |
| NFL | 0.140 | -1.162 | 3.299 |
| NHL | -0.845 | -4.273 | 2.089 |
| ATP | -0.170 | 0.268 | 0.744 |
| EPL | 7.664 | 16.714 | 3.510 |

Different weighting schemes measure different populations of activity; these
sign changes rule out describing the endpoint change as uniform across fills,
dollars and markets. They do not by themselves identify a trading motive.

### Recorded-end timing

Source: `boundary_counts.parquet`. Entries are count shares of central-price
BUYs (`.1<=P<=.9`) among All matched BUYs in the named literal window.

| Sport | Last 60 seconds through end | First 60 seconds after end | Second 60 seconds after end |
|---|---:|---:|---:|
| ATP | 43.11% | 41.22% | 36.99% |
| MLB | 45.58% | 7.41% | 0.82% |
| NBA | 20.52% | 6.05% | 0.68% |
| NFL | 34.74% | 18.81% | 14.00% |
| NHL | 41.16% | 22.86% | 1.93% |
| EPL | 9.44% | 7.00% | 7.45% |

ATP's first post-end minute includes 30,263 BUYs, 12,474 central priced.
Across both post-end minutes it has 56,255 BUYs and 22,088 central priced.
This reinforces the need to qualify its proxy clock. These observations cannot
establish a corrected physical end time, public knowledge time or stale quoting.
Exact-end counts are not an obvious universal explanation: MLB has none and
NBA only five, while ATP has 657 and EPL 234.

## Unestimated follow-up hypotheses

Passive stale quotes, settlement/transaction costs and trader inventory are
plausible candidates, not identified mechanisms in this stage. A focused
active/passive execution-role check would be more direct than another broad
regression. Retrospective normalization also conditions on realized event end,
which is future information at the trade time. Whether this stopping-time
selection explains any observed calibration difference requires an explicit
comparison with an ex-ante or sport-clock time axis; this stage does not test it.

## Existing smoothing-code audit

Read-only review of `analysis/multisport_game_dynamics/estimate_flb_decay.py`,
lines 885–1022: the existing curves subtract two separately normalized,
local-constant Epanechnikov tail means. Live observations satisfy `0<=T<=1`
and `abs(T-x)<.10`. Therefore the endpoint `x=1` uses `(.90,1]`, not only
`[.99,1]`, and its window is one-sided. No post-end trades enter that endpoint.
This is not a local-linear or boundary-corrected estimator. Different local
time/market distributions in D1 and D10 can affect their separate averages.

These are verified implementation properties, not evidence that smoothing
causes an upward endpoint. This stage uses literal windows and the verified
own-action source; it does not reproduce or validate legacy buyer inference,
old kernel estimates, or their uncertainty bands.
