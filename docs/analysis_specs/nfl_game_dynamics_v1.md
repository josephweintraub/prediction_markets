# NFL moneyline game dynamics: phase contract v1

**Status:** implemented and completed in the independently audited exploratory
production run `/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3`. The immutable
results, exclusions, report fingerprint, and interpretation boundary are recorded in
[`../runs/2026-09-12_nfl_nba_game_dynamics_v3.md`](../runs/2026-09-12_nfl_nba_game_dynamics_v3.md).
This contract and its results remain descriptive, not confirmatory.

## Scope

- Polymarket single-game, two-outcome NFL moneylines only. Props, totals,
  spreads, futures, series, and non-NFL football are excluded.
- Completed games must have exactly two teams, nonnegative unequal final
  scores, exactly one official winner flag, and agreement between the score,
  official winner, Polymarket outcome labels, and Polymarket resolution.
- Four regulation quarters are required for the core. Overtime is allowed only
  when the completed status, competitive-play periods, and terminal evidence
  agree. Ties and boundary-incomplete or otherwise nonstandard games remain in
  the audit and do not enter estimation.
- Team identity, schedule matching, result parsing, play-taxonomy validation,
  and play-by-play timing remain NFL-adapter responsibilities. The shared
  downstream code begins only after the mandatory Stage-03 handoff.

The machine-readable phase source of truth is
[`nfl_phase_contract_v1.json`](../../configs/game_dynamics/nfl_phase_contract_v1.json).
The provider-taxonomy source of truth is
[`nfl_espn_taxonomy_audit_v2.json`](../../configs/game_dynamics/nfl_espn_taxonomy_audit_v2.json).
The frozen SHA-256 values are
`62826a4e7e647db78dfcd7862e16d73396c7942b9cdbeb7dc50b9c8b38f21cfd`
for the phase contract and
`2d2f4b3a10d73090da64f54898503d75a095df7262e74d7f598ce725a78ddfee`
for the taxonomy audit; the timing build rejects either file if it differs.

## Provider, status, and limitations

Schedule and play timing come from ESPN's site API scoreboard and summary
endpoints. This is a **third-party, undocumented** source, not an official NFL
data feed. The Stage-03 handoff must therefore record provider status
`third_party_undocumented`; downstream manifests and the report preserve that
status rather than presenting the timing as official.

Every raw scoreboard and summary response used by a run is cached and
fingerprinted. A production run must use a reviewed cache set; `--refresh` is a
deliberate cache-refresh operation, not an ordinary reproducibility step.
Provider schema drift, changed play-type meanings, or an unreviewed play type
fails closed and produces an auditable timing exclusion.

Known provider limitations are part of the contract:

- ESPN administrative timeout and end markers can contain stale placeholder
  `wallclock` values. They are not competitive plays and their timestamps are
  never used as boundaries.
- ESPN does not supply a trustworthy, separate game-over wall-clock. The final
  analysis boundary is therefore the last competitive play's timestamp,
  conditional on later normal terminal `End of Game` evidence. The terminal's
  own wallclock remains unused.
- Completed-game reschedule history is not exposed reliably. Matching uses the
  ESPN schedule record's Eastern-calendar date and does not infer a prior date.
- The frozen taxonomy is based on an audited historical sample; it is not a
  guarantee that ESPN will retain those undocumented identifiers or payload
  shapes.

## Frozen ESPN taxonomy audit

The parser is an allowlist, and the timing-stage manifest fingerprints the
frozen taxonomy file. Taxonomy v2 is backed by an immutable cache inventory of
150 scoreboard resources and 659 summary resources spanning candidate dates
2024-08-08 through 2026-02-08. The file records the complete cache-inventory
hash and Stage-01 candidate hash. Its game-ID list contains 20 representative
examples, not all 659 audited summaries. This coverage tests known format
variants; it does not convert the source into an official or stable API.

The frozen categories are:

- Administrative top-level types: `2` End Period, `21` Timeout, `65` End of
  Half, `66` End of Game, `74` Official Timeout, `75` Two-minute warning, and
  `79` End of Regulation. These are excluded from competitive-play timing.
- Competitive top-level types: `3`, `5`, `7`, `8`, `9`, `12`, `17`, `18`,
  `20`, `24`, `26`, `29`, `30`, `32`, `34`, `36`, `37`, `38`, `39`, `40`,
  `51`, `52`, `53`, `59`, `60`, `67`, `68`, and `80`, with the exact text
  labels recorded in the taxonomy file.
- Nested point-after types: `0`, `10`, `15`, `16`, `43`, `61`, and `62`, again
  with exact audited text labels. Type `15` Two Point Pass is valid only in
  this nested metadata. The observed top-level type-`15` rows are not plays
  and remain excluded rather than being admitted as competitive events.

An identifier with different text, an unknown nonadministrative type, or an
unknown nested point-after type excludes that game's timing. The parser never
guesses whether a new value is competitive or administrative.

## Market universe and game matching

Stage 01 retains an audit row for every `nfl-` market. A candidate must have an
exact `nfl-<team>-<team>-YYYY-MM-DD` slug, a two-team `A vs B` or `A vs. B`
question without a colon, and exactly two tokens. Multiple otherwise-valid
markets with the same event slug cause the build to fail instead of being
ranked or silently deduplicated.

The fixed team map covers all 32 NFL teams. Exact aliases observed in the
2024--2026 Polymarket inventory are `la` for the Rams, `las` for the Raiders,
and `was` for Washington. A match requires the slug date and the two canonical
ESPN team IDs. Observed slug order is first tested as official away/home order;
reversed order is tested only when the official-order search has no match.
Zero matches, multiple matches, a nonfinal schedule record, an unknown alias,
or more than one market assigned to one game is an explicit exclusion.

Moneyline validation then requires exactly two unique token IDs and two
distinct recognized team outcomes. Accepted outcome labels are each team's
nickname or full canonical name, including the explicit `49ers` case. The Rams
also accept the exact normalized official abbreviation `LAR`; ambiguous `LA`
and the unrequested `LAC` abbreviation remain unrecognized. The two outcomes
must equal the matched away/home teams, all token rows must agree on one
Polymarket winning outcome, and that outcome must equal the unique ESPN
score-and-flag winner.

## Exact timing semantics

ESPN numeric `sequenceNumber`, not payload position, orders plays. Duplicate
play IDs may repeat only with identical sequence, timestamp, period, type, and
clock data; sequence numbers may not belong to different play IDs. Every
competitive play needs a timezone-aware `wallclock`, a positive period, and a
display clock. Competitive periods and timestamps must both be chronological.

### Opening kickoff

`actual_start_utc` is the `wallclock` at the start of the first competitive
play. That play must be in period 1, have clock `15:00`, and be either type
`53` Kickoff or type `12` Kickoff Return (Offense). A scheduled start, a later
Q1 play, or an administrative marker cannot substitute for a missing opening
kickoff record.

### Quarter boundaries

`period_2_start_utc`, `period_3_start_utc`, and `period_4_start_utc` are the
timestamps of the first complete competitive plays in periods 2, 3, and 4.
The parser requires contiguous competitive periods from 1 through the final
period and requires all of periods 1--4. End Period, End of Half, and timeout
markers do not define a quarter boundary because their wall clocks are not
reliable. Quarter 4 must contain a positive competitive-play span:
`period_4_start_utc < actual_end_utc`. A feed whose first and last competitive
Q4 timestamps are identical is treated as suspended or shortened and excluded.

### Terminal `End of Game` evidence

`actual_end_utc` is the timestamp of the last competitive play, not the
timestamp carried by an administrative marker. A type-`66` `End of Game`
marker must have a greater sequence number than that play and must belong to
the same expected final period. Its play text must be exactly `END GAME`. A
regulation terminal must have game clock `0:00`; a nonzero terminal clock is
allowed only in period 5 or later after the existing `Final/OT` detail and
competitive-final-period checks pass, covering legitimate walk-off overtime.
The audit found 627 zero-clock regulation terminals, 9 zero-clock overtime
terminals, 22 nonzero-clock overtime terminals, and one abnormal regulation
terminal at `6:19` whose text was `Game Suspended. Will not resume.` The latter
is excluded explicitly as suspended or shortened. The terminal wallclock is
ignored. Missing, malformed, earlier, or wrong-period terminal evidence also
excludes the game's timing.

The summary status must be exactly completed/final: ID `3`, name
`STATUS_FINAL`, state `post`, description `Final`, plus a supported detail.
`Final` implies period 4; `Final/OT`, `Final/2OT`, and higher explicit overtime
details imply periods 5, 6, and higher. A reported status period, when present,
must agree. The last competitive period must also equal the implied final
period, preventing a truncated overtime feed from passing.

## Exact phases

Let `t` be the exact Polygon block timestamp assigned to a fill.

| Phase | Exact interval |
| --- | --- |
| Pregame | `t < actual_start_utc` |
| Quarter 1 | `actual_start_utc <= t < period_2_start_utc` |
| Quarter 2 | `period_2_start_utc <= t < period_3_start_utc` |
| Quarter 3 | `period_3_start_utc <= t < period_4_start_utc` |
| Quarter 4 and overtime | `period_4_start_utc <= t <= actual_end_utc` |
| Post-final audit | `t > actual_end_utc` |

Overtime is deliberately folded into the final live phase. A fill exactly at
the final competitive-play second remains in Quarter 4 and overtime;
post-final rows are retained for reconciliation but excluded from estimation.
The literal sample uses these intervals. The fixed sensitivity removes,
without reassignment, fills within 30 seconds inclusive of any of the five
boundaries.

## Exclusions and audit behavior

Every Stage-01 candidate remains represented in the timing and validated
audits. Unmatched markets have timing status `not_attempted`; exact final
matches whose summary cannot be fetched or parsed have status `excluded`, with
the concrete exception type and message retained. No timing failure is silently
dropped or repaired from another provider.

Core exclusions include syntactic candidate failures; invalid or ambiguous
team/date matches; duplicate market-to-game mappings; nonfinal, tied, missing,
or contradictory results; token/outcome/resolution failures; unreviewed ESPN
taxonomy; missing or nonchronological competitive timestamps; a missing
opening kickoff; incomplete Q1--Q4 representation; final-status/period
disagreement; a zero-duration Q4; and missing or abnormal terminal `End of
Game` evidence. Postseason,
neutral-site, delayed-start, and overtime games are not excluded merely by
label, but they must pass the same contract. A shortened preseason or other
nonstandard format that lacks complete regulation-quarter boundaries remains
auditable and is excluded from the core.

## Shared downstream statistical contract

The mandatory Stage-03 bridge publishes the NFL adapter result in the shared
18-column `eligible_moneylines.parquet` schema and binds it to the phase
contract and ESPN provider status. Stages 04--10 use the provider-neutral code
in `analysis/sports_game_dynamics`; they do not refetch or reinterpret NFL
schedule or play data.

- Exact trade timestamps must come from the Polygon block-timestamp cache with
  complete scoped block coverage, one timestamp per block, and zero fallback
  rows. Immutable EVM event identity removes ingestion replays.
- Phase estimation retains BUY fills with `0.01 < price < 0.99` and excludes
  only a flagged outcome-token buyer. Probabilities are normalized to the home
  outcome, so calibration is `home_won - home_probability`. Fills receive
  equal weight; dollars are descriptive.
- The primary close is the last exact pregame fill with `0 < price < 1`, bots
  included. The sensitivity close uses `0.01 < price < 0.99` and excludes only
  a flagged outcome-token buyer. Closing profiles give each game equal weight.
  The paired A-minus-C result is filter attribution, not CLV.
- Fixed bins are `[0, 0.1)` through `[0.9, 1]`. With five analysis phases, the
  complete outputs contain 22 closing-profile rows, 11 paired-close rows, 100
  phase-profile rows, and 12 D1/D10 tail rows.
- Closing profiles report mean home probability, home win rate, mean
  calibration, its standard error and interval, and Brier score. Paired-close
  rows report A-minus-C probability, calibration, and Brier differences.
  Phase profiles additionally retain fill count, distinct-game count, and
  descriptive dollars.
- Phase uncertainty uses Cameron-Gelbach-Miller clustering by UTC trade day,
  buyer wallet, and game. Closing uncertainty clusters by official game date.
  Reported intervals are normal 95% intervals. Estimates are withheld with
  status `suppressed_n_lt_50` when their effective count is below 50.
- The D1/D10 spread is estimated jointly under the corresponding cluster
  scheme so tail covariance is retained. The full ten-bin profiles remain
  primary; tail signs are descriptive and no slope, p-value, multiplicity
  adjustment, or causal claim is produced.

All Stage-08 estimates and the Stage-10 report remain
`exploratory_descriptive` until a separate workflow decision changes the work
state. The operational sequence is documented in
[`sports_game_dynamics_runbook.md`](sports_game_dynamics_runbook.md).
