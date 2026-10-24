# Repository map

**Navigation review:** 2026-10-24

This is a map of study responsibilities, not an instruction to move source files.
Current studies share utilities but retain separate scientific contracts, source
identities and immutable outputs. Begin with [project status](project_status.md) and
the [handoff guide](handoff/README.md).

## Current replication modules

| Module | Responsibility | Primary navigation |
|---|---|---|
| `analysis/kaushik_polymarket_replication/` | Polymarket input construction, exchange-filtered logical inputs, estimators and accepted-summary reporting. | [Current exchange-excluded run](runs/2026-10-14_kaushik_exchange_filtered_rerun_v1.md), [exclusion contract](analysis_specs/kaushik_exchange_wallet_exclusion_v1.md), [full-date reproduction](analysis_specs/kaushik_polymarket_full_sample_reproduce.md) |
| `analysis/kaushik_kalshi_replication/` | Native metadata/trade contracts, staged inputs, duration estimation, provider sports binding, saved-score checks and report consumers. | [Accepted native Kalshi run](runs/2026-10-16_kaushik_kalshi_replication_v1.md), [reproduction guide](analysis_specs/kaushik_kalshi_reproduce_v1.md) |

Guarded Polymarket entrypoints are `scripts/build_kaushik_polymarket_replication.py`,
`scripts/build_kaushik_exchange_filtered_inputs.py` and
`scripts/estimate_kaushik_polymarket_replication.py`. Independent release/saved-score
audits are separate scripts, not renderer calculations. Kalshi uses
`scripts/build_kaushik_kalshi_inputs.py`, `scripts/audit_kaushik_kalshi_inputs.py`,
`scripts/audit_kaushik_kalshi_sports.py` and the report-only
`scripts/render_kaushik_kalshi_report.py`. Reproduction guides record the separate
module APIs and exact commands for stages without a dedicated CLI.

## Sports dynamics and focused diagnostics

| Module | Responsibility | Status |
|---|---|---|
| `analysis/multisport_game_dynamics/` | Candidate/timing/trade construction, pooled sports estimates, FLB time regressions, kernels and focused swing audits. | Shared engine retained by the [current nine-sport adoption](runs/2026-10-10_sports_wallet_adoption_v1.md). |
| `analysis/{mlb,nfl,nba}_game_dynamics/` | Sport-specific market/result/timing adapters and handoffs. | Supporting dependencies; historical run identities remain explicit. |
| `analysis/sports_game_dynamics/` | Shared exact timestamps, phase contracts, fixed bins, closes, validation and artifacts. | Supporting library, not one universal analysis specification. |
| `analysis/diagnostics/` | Own-order action recovery, full-history FIFO, genuine execution versus order-event accounting, tennis clock comparisons and terminal-pattern diagnostics. | [Profit-taking](runs/2026-10-02_sports_profit_taking_two_grain_v1.md), [tennis](runs/2026-10-02_tennis_wallet_investigation_v1.md), [terminal alternatives](runs/2026-10-03_terminal_pattern_diagnostics_v1.md) retain their frozen cohorts. |

The repaired nine-sport adoption is staged by
`scripts/rebuild_polymarket_wallet_flags.py`,
`scripts/rebuild_sports_wallet_samples.py`,
`scripts/estimate_sports_wallet_adoption.py` and
`scripts/render_sports_wallet_adoption.py`. It does not silently refresh every older
study that imports the shared sports engine.

## Data construction and trust

- `pipeline/`: native trade construction and refresh. Canonical `refresh.py`,
  `extraction/`, `transform/` and `goldsky/` remain in their existing locations;
  `_legacy/` is historical. Do not mistake an older local copy for the canonical
  production layout.
- `scripts/`: clean construction, token/market flags, native-lineage and timestamp
  audits, wallet repair, classifier rebuilds and controlled adoption. See the
  [wallet repair](runs/2026-10-09_wallet_attribution_repair_v1.md) and
  [MLB rebuild](runs/2026-10-09_mlb_unfiltered_rebuild_v1.md) run records.
- `production_guard.py`: production-host admission. Real-data operations require the
  canonical Linux checkout and mounted private data; fixture tests and report-only
  rendering are separate permitted local operations.
- Shared utility imports are dependencies, not instructions to execute arbitrary
  older drivers. Use the entrypoints pinned by the chosen release.

## Retained earlier workstreams

- `analysis/calibration_heterogeneity/`: earlier liquidity, duration, semantic-family
  and textual-novelty work, including universe/base builders, schemes, embeddings,
  vintage checks and clustered FLB engine. Historical wallet flags and positional
  features remain legacy; this workstream is not adopted on the later repaired-wallet
  generation. Follow its original run records rather than treating it as the current
  cross-venue replication entrypoint.
- `analysis/stage0_v2/`: classification/normalization and regression harnesses,
  including older Kalshi label construction. These are not the native Kalshi
  replication pipeline.
- `archive/analyses/early_flb_2026/`: early broad FLB, trader, P&L, market-accuracy,
  closing-price and Manifold work.
- `archive/analyses/paper_may_2026/`: earlier publication-facing scripts and figures.
- `archive/analyses/learnability_v1_v7/`: older dimensions/engines and tag-map provenance.
- `archive/analyses/calibration_heterogeneity_diagnostics_2026/` and
  `archive/run_scripts/calibration_heterogeneity_2026/`: retired exploratory utilities
  and historical run order, not supported current entrypoints.

## Documentation and artifacts

- [Handoff guide](handoff/README.md): release navigation, source/data dependencies,
  reproduction boundaries and continuation instructions.
- [Project status](project_status.md): current versus historical study map.
- [Methods reference](methods_reference.md): durable trust rules and clearly scoped
  historical defaults; approved study specifications define explicit overrides.
- `analysis_specs/`: frozen study contracts and reproduction guides.
- `runs/`: execution, acceptance and findings records. Earlier checkpoints may
  intentionally predate final transfer/shutdown; consult the identified completion
  receipts rather than rewriting history.
- [Workflow](workflow.md), [EC2 setup](EC2_SETUP.md), `decisions.md` and `archive/`:
  operating procedures, methodological history and superseded documentation.

Heavy raw/provider bodies, caches and production outputs stay under `/mnt/data` and
are not Git payloads. Finished handoff releases contain the specifically admitted
source, portable reports, saved summaries and QA evidence; an omitted private input
must be listed as a dependency, not replaced with invented data. Report regeneration
from supplied summaries is distinct from full private-data reconstruction. Mutable
convenience locations and Mac mirrors do not replace immutable run identities.
