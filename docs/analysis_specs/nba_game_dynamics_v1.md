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

The machine-readable source of truth is
[`nba_phase_contract_v1.json`](../../configs/game_dynamics/nba_phase_contract_v1.json).

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

## Exact phases

Every official play-by-play action must carry a parseable absolute UTC
timestamp. Only the explicit boundary actions must be chronologically ordered
and nonoverlapping: the start is the first valid Q1 `jumpball` action at
`PT12M00.00S`; Q2, Q3, and Q4 boundaries are their unique official
`period/start` actions at `PT12M00.00S`; and the end is the unique final-period
`period/end` action at `PT00M00.00S`. Edited timestamps on nonboundary actions
may reorder and are never used to derive a boundary. The final period must
equal the completed schedule result, so a truncated overtime feed fails. A
later action or period can never substitute for a missing boundary.

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
