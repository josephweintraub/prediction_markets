# Project status

**Status date:** 2026-10-24

**Current stage:** completed research studies are being organized for handoff and
possible final extensions. This navigation update does not rerun an analysis, certify
whole-history data completeness, or assert that the new handoff has already been
published to Dropbox.

Start with the [handoff guide](handoff/README.md), [repository map](repository_map.md)
and [methods reference](methods_reference.md). Exact scientific definitions belong to
the selected study's frozen specification; exact execution and acceptance evidence
belong to its run record and saved receipts. The prior
[12 September status](archive/project_status_2026-09-12.md) is historical.

## Current accepted studies

| Workstream | Accepted current reference | Boundary |
|---|---|---|
| Polymarket Kaushik-style replication | [Exchange-wallet-excluded full-date rerun, 14 October](runs/2026-10-14_kaushik_exchange_filtered_rerun_v1.md), with the [exchange exclusion specification](analysis_specs/kaushik_exchange_wallet_exclusion_v1.md) | Current comparison convention excludes either-source-wallet exchange matches. The [cutoff](runs/2026-10-10_kaushik_polymarket_replication_v1.md) and [original full-date](runs/2026-10-10_kaushik_polymarket_full_sample_v1.md) releases remain distinct references. |
| Native Kalshi replication | [Accepted 17 October report in the 16 October run record](runs/2026-10-16_kaushik_kalshi_replication_v1.md), [specification](analysis_specs/kaushik_kalshi_replication_v1.md), [reproduction guide](analysis_specs/kaushik_kalshi_reproduce_v1.md) | Native public-trade prints, not wallet fills. Baseline, duration and provider-matched sports have different coverage. Public tape does not support the Polymarket wallet/maker comparison. |
| Repaired nine-sport dynamics | [10 October wallet-filter adoption](runs/2026-10-10_sports_wallet_adoption_v1.md), [adoption specification](analysis_specs/sports_wallet_adoption_v1.md) | Corrected wallet flags and coherent samples are versioned; shared historical input paths are not redirected. |
| Tennis timing and terminal wallet sequences | [2 October investigation](runs/2026-10-02_tennis_wallet_investigation_v1.md) | Frozen investigation cohort and separately verified own-maker actions; no full-inventory or causal claim. |
| Profit-taking and terminal-pattern mechanisms | [2 October two-grain study](runs/2026-10-02_sports_profit_taking_two_grain_v1.md), [3 October alternative diagnostics](runs/2026-10-03_terminal_pattern_diagnostics_v1.md) | Preserved own-action sources and accounting definitions. These studies were not rerun on the later repaired-nine-sport flag generation. |

Acceptance is scoped to each recorded set of gates, not a general guarantee that every
data issue has been eliminated. Reports, audit receipts, source identities and private
input dependencies must travel together in the handoff inventory.

## Data and interpretation boundaries

- Polymarket's preserved CLEAN generation contains 2,036,128,538 published rows.
  The [wallet-attribution repair](runs/2026-10-09_wallet_attribution_repair_v1.md)
  and [MLB input rebuild](runs/2026-10-09_mlb_unfiltered_rebuild_v1.md) are separately
  accepted changes. Row count equality does not establish native-action, replay or
  complete-collection correctness.
- Historical inferred counterparty identities/actions are not a holdings ledger.
  Current downstream adoption is study-specific. Do not combine repaired identities
  with old flags, wallet codes or encoded bases without an explicit new build.
- Polymarket's resolved-source construction creates end-of-sample censoring. Sports
  clocks and provider overlap retain the qualifications in their own releases,
  including the original ATP timing qualification.
- Kalshi's archive begins earlier than its usable estimation sample. Its duration
  cohort requires admitted native settlement clocks, and its sports cohort requires
  an accepted provider map. Neither cohort should be described as the whole archive
  or a census of games.
- Observation grain, prices, filters, weights, support and clustering differ between
  workstreams. Compare results only after checking those contracts; do not apply the
  historical heterogeneity defaults automatically to the newer replications.

## Retained earlier research

`analysis/calibration_heterogeneity/` retains liquidity, duration, semantic-family and
textual-novelty exploration. Its older wallet-filtered bases and positional embedding
artifacts have not been adopted by the repaired-nine-sport run. Historical findings
remain tied to their original vintages and qualifications, rather than being promoted
as current repaired-data results. The earlier learnability, broad FLB and May paper
branches remain under `archive/`; use the [repository map](repository_map.md).

## Handoff and any final work

The [handoff guide](handoff/README.md) is the entrypoint for organized source, accepted
releases, reproducibility instructions and the private-data inventory. Publication
status must come from actual verified transfer receipts, not this planning statement.

Any final research change must identify the affected study and preserve its accepted
reference release. Freeze the smallest new contract, run focused fixtures, use a fresh
versioned output directory, and independently reconcile affected sample counts and
reported results. Follow [workflow](workflow.md) and [EC2 setup](EC2_SETUP.md); only the
root orchestrator controls infrastructure and verifies shutdown.
