# NBA moneyline game dynamics: phase contract v1

**Status:** code-approved implementation contract; production run pending. The
adapter and shared downstream path are approved at the code level, but no
immutable NBA production run has completed. This document does not authorize
reporting NBA production estimates before that run and its audits pass.

## Scope

- Polymarket single-game, two-outcome NBA moneylines only.
- Completed games with a unique official winner. A missing score or
  contradictory winner flag fails closed.
- Team identity, schedule matching, result parsing, and play-by-play parsing
  remain NBA-adapter responsibilities; no generic provider client is implied.
- Four standard 12-minute regulation quarters are required for the core.
  Summer League or other nonstandard formats, postponed, resumed, cancelled,
  or boundary-incomplete games stay in the audit and do not enter estimation.

The machine-readable production source of truth is
[`nba_phase_contract_v2.json`](../../configs/game_dynamics/nba_phase_contract_v2.json).
The immutable v1 contract remains for audit history, but is superseded because
its literal 12:00 jump-ball rule is contradicted by the complete cached cohort.

## Provider sources and limitations

- Historical schedules come from the official `data.nba.com` schedule files.
- Play timing comes from the official NBA LiveData S3 origin
  `nba-prod-us-east-1-mediaops-stats.s3.amazonaws.com/NBA/liveData`. The public
  LiveData CDN returns HTTP 403 from the production EC2 environment and is not
  the production source.
- Every LiveData action must carry a parseable, timezone-aware absolute
  `timeActual` value. The opening tip, Q2/Q3/Q4 starts, and final-period end
  boundaries must be observed directly on their required actions; a missing or
  invalid absolute boundary excludes the game.
- The adapter never interpolates or extrapolates wall-clock time. Scheduled tip
  time, game clock, action order, and neighboring actions cannot substitute for
  an absolute `timeActual` boundary.
- The historical schedule marks completed regulation and overtime games only as
  `Final`; it does not declare the final period. Bare `Final` is therefore a
  completed result with a null expected period. An explicit `Final/OT` or
  `Final/nOT`, if supplied, remains an exact cross-check.
- Matching uses the official NBA game date only. A UTC-date fallback is not
  implemented. The audited completed cohort has no false unmatched final from
  this rule, but future coverage must be re-audited before expanding it.

## Exact phases

Every official play-by-play action must carry a parseable absolute UTC
timestamp. Only the explicit boundary actions must be chronologically ordered
and nonoverlapping. After the unique Q1 `period/start`, the parser skips only
consecutive exact Q1 0-0 `PT12M00.00S` `violation/delay-of-game` records and
examines the immediately following action, never a later search result. That
action must remain 0-0 in Q1 at 11:45--12:00 and have one exact audited
`actionType/subType/descriptor` signature: `jumpball/recovered` with
`startperiod`, `outofbounds`, `heldball`, or `unclearpass`; or
`violation/jumpball` with an empty descriptor. Q2, Q3, and Q4 boundaries are
Descriptions must use the observed `Jump Ball ...` form for recovered tips or
end in ` jumpball VIOLATION` for violation tips. Q2, Q3, and Q4 boundaries are
their unique official
`period/start` actions at `PT12M00.00S`; and the end is the unique final-period
`period/end` action at `PT00M00.00S`. Edited timestamps on nonboundary actions
may reorder and are never used to derive a boundary.

The observed final period requires contiguous exact period starts and ends, a
tied regulation end and every nonterminal overtime end, a decisive final-period
end, and one same-period `game/end` at 0:00 after that boundary with identical
scores. The observed away/home scores must exactly equal the official schedule
scores and imply the same unique winner. Missing, nonintegral, negative,
truncated, reordered, or contradictory evidence fails closed. `actual_end_utc`
remains the final `period/end`; `game/end` is confirmation rather than a phase
boundary.

Contract v2 binds this rule to the immutable 1,367-resource provider-cache
inventory (`7adac545...be31`) and Stage-01 candidate artifact
(`ac4eb023...948f`). The exhaustive cohort contains 1,365 matched completed
PBP feeds: 1,364 pass the opening rule, game `0022400887` fails it, and game
`0022400072` has the sole schedule/PBP score mismatch. The expected fully
reconciled timing count is therefore 1,363. Among 70 overtime feeds (66 1OT,
4 2OT), none has a spurious period 5 after a decisive regulation score.
Stage 02 compares the actual candidate SHA/count/date range and the complete
cache-tree name/SHA/resource counts to this scope before parsing, requires its
resource provenance to cover that exact tree, and repeats those checks when
the immutable run is reopened.

| Phase | Exact interval |
| --- | --- |
| Pregame | `t < actual_start_utc` |
| Quarter 1 | `actual_start_utc <= t < period_2_start_utc` |
| Quarter 2 | `period_2_start_utc <= t < period_3_start_utc` |
| Quarter 3 | `period_3_start_utc <= t < period_4_start_utc` |
| Quarter 4 and overtime | `period_4_start_utc <= t <= actual_end_utc` |
| Post-final audit | `t > actual_end_utc` |

Overtime is deliberately folded into the final live phase. Post-final rows are
retained only for reconciliation. The literal estimator sample uses the table
above. Its fixed sensitivity removes, without reassignment, trades whose
timestamp is within 30 seconds inclusive of any of the five boundaries.

## Shared fixed-bin outputs

The provider-neutral helpers in
`analysis/sports_game_dynamics/phase_contract.py` and `fixed_bins.py` freeze
the phase order, `[0, 0.1)` through `[0.9, 1]` bins, primary/sensitivity closing
definitions, literal/30-second samples, complete row grids, and `n < 50`
support threshold. They declare and validate artifact grains only; they do not
estimate calibration, uncertainty, or FLB effects.

With five analysis phases, the v1 fixed grids contain 22 closing-profile rows,
11 paired-close rows, 100 phase-profile rows, and 12 tail-summary rows. The
full ten-bin profiles remain primary. Calibration is `outcome - trade price`;
the A-minus-C closing difference is filter sensitivity, not CLV.

The NBA adapter freezes and tests the official historical-schedule and
LiveData providers, exact team-label map, event-date semantics, canonical
winner rule, opening-tip and terminal representations, overtime completeness,
provider-cache fingerprints, and irregular-game exclusions before validated
publication. Its mandatory Stage-03 handoff emits provenance schema v2 only
after reverifying the native Stage-02/03 manifests and summaries, the provider
provenance, and the fingerprints of every cached provider resource recorded in
`native_lineage.source_evidence`.

The Stage 01--10 operating sequence is documented in
[`sports_game_dynamics_runbook.md`](sports_game_dynamics_runbook.md).
