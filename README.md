# Prediction-market calibration

Research code for measuring price calibration and favorite-longshot bias (FLB) on
Polymarket and Kalshi. The completed work covers fixed-price-bin calibration,
duration regressions, sport-specific game dynamics, and focused investigations of
terminal trading and wallet actions. Analyses are descriptive unless their own
frozen specification establishes otherwise.

The canonical repository is `/home/ubuntu/prediction_markets` on the project EC2
instance. Large private inputs and production artifacts remain under `/mnt/data`,
outside Git. The Mac copy supports editing, small fixtures and saved-summary review,
not production trade scans or data-compatible joins.

## Start here

1. Read the [handoff guide](docs/handoff/README.md) for the organized code/results
   package, release inventory, reproduction boundaries and continuation workflow.
2. Read [project status](docs/project_status.md) to distinguish current accepted
   studies from historical exploration.
3. Read the [methods reference](docs/methods_reference.md), then the selected study's
   frozen specification and run record. Study-specific rules must not be replaced
   with another workstream's defaults.
4. Use the [repository map](docs/repository_map.md) to locate modules and entrypoints;
   follow the [workflow](docs/workflow.md) for new work.

## Current accepted studies

| Study | Current reference | Source module |
|---|---|---|
| Polymarket Kaushik-style replication | [14 October exchange-wallet-excluded full-date rerun](docs/runs/2026-10-14_kaushik_exchange_filtered_rerun_v1.md) | `analysis/kaushik_polymarket_replication/` |
| Native Kalshi replication | [17 October accepted report, recorded in the 16 October run](docs/runs/2026-10-16_kaushik_kalshi_replication_v1.md) | `analysis/kaushik_kalshi_replication/` |
| Corrected-wallet nine-sport dynamics | [10 October wallet-filter adoption](docs/runs/2026-10-10_sports_wallet_adoption_v1.md) | `analysis/multisport_game_dynamics/` and adoption scripts |
| Tennis timing, profit-taking and terminal-pattern diagnostics | [2 October tennis](docs/runs/2026-10-02_tennis_wallet_investigation_v1.md), [2 October two-grain profit-taking](docs/runs/2026-10-02_sports_profit_taking_two_grain_v1.md), [3 October diagnostics](docs/runs/2026-10-03_terminal_pattern_diagnostics_v1.md) | `analysis/diagnostics/` |

The mechanism investigations retain their own frozen cohorts; they were not silently
reflagged by the later nine-sport adoption. Earlier cutoff and unexcluded full-date
Polymarket replications remain immutable comparison releases. Calibration-heterogeneity
and learnability work is retained for provenance, not represented as adopted on the
repaired-wallet generation.

## Repository layout

| Path | Purpose |
|---|---|
| `analysis/kaushik_{polymarket,kalshi}_replication/` | Separate venue input contracts, estimators, saved evidence and report consumers. |
| `analysis/multisport_game_dynamics/` | Shared nine-sport timing, exact-trade analysis, time regressions and renderers. |
| `analysis/{mlb,nfl,nba,sports}_game_dynamics/` | Sport adapters and supporting stages retained by versioned studies. |
| `analysis/diagnostics/` | Own-action recovery, FIFO/accounting, tennis clocks and terminal-pattern investigations. |
| `pipeline/` | Native on-chain trade construction and refresh code; legacy stages are labeled separately. |
| `scripts/` | Guarded builds, repairs, independent audits, adoption and report entrypoints. |
| `analysis/calibration_heterogeneity/`, `analysis/stage0_v2/`, `archive/` | Retained earlier workstreams, classification support and historical exploration. |
| `docs/` | Current navigation, frozen methods/specifications, run records and historical documentation. |

Source modules remain in their original import locations. The handoff groups releases
and documents dependencies without rewriting pinned historical code or manifests.

## Reproduction and continuation

Use each release's source identity, saved inputs, environment record and acceptance
receipts. Rendering a supplied report from accepted summaries is distinct from
rebuilding its inputs or repeating production estimation. Private raw/provider data,
historical source revisions and guarded EC2 access may be required for the latter.

`pyproject.toml` declares project dependencies; recorded production environments and
the selected study's fixtures remain the reproduction reference. A dependency
declaration alone is not evidence of a successful clean-install reconstruction.
Run focused fixture tests before broader tests or production work. Never bypass the
production host guard to analyze real trades locally.

New work receives a new immutable run and an explicit specification. Do not overwrite
an accepted release or reinterpret older results as if they used newly repaired data.

## Secrets and operations

Use environment variables or private key files, never committed credentials. Read
[EC2 setup](docs/EC2_SETUP.md) for compute, transfer and lifecycle procedures. Only the
root orchestrator manages the shared instance, and any task that uses it must finish
with a verified stopped state after all workloads and transfers release their holds.
