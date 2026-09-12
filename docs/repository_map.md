# Repository map

**Last structural review:** 2026-09-12

The repository has one active empirical workstream. Earlier paths remain available under
`archive/` for provenance but should not be imported by current code.

## Active analysis

### `analysis/calibration_heterogeneity/`

The current liquidity, duration, semantic-family, novelty, and FLB analysis. It contains:

- Universe and compact trade-base builders
- Market-to-slice specifications
- Embedding, cluster, and novelty features
- The current decile calibration and clustered-inference engine
- Artifact-based report renderers
- A research log explaining prior experiments and data corrections

New empirical analysis belongs here until a narrower publication package is extracted.

### `analysis/{mlb,nfl,nba,sports}_game_dynamics/`

The active exploratory sport-moneyline extension. Sport adapters validate market,
official-result, and game-boundary inputs; shared stages build exact-timestamp trades,
phase samples, dual closes, fixed-bin calibration and D1/D10 summaries, and standalone
reports. The completed NFL/NBA production record is
[`runs/2026-09-12_nfl_nba_game_dynamics_v3.md`](runs/2026-09-12_nfl_nba_game_dynamics_v3.md).

### Shared analysis utilities

`analysis/bot_filter.py`, `analysis/config.py`, `analysis/data_loader.py`, and
`analysis/subprocess_runner.py` remain shared data-access and filtering utilities. They will
move into a small importable package only after compatibility tests exist.

## Data construction and supporting code

### `pipeline/`

Canonical trade-data refresh implementation. `refresh.py`, `extraction/`, `transform/`,
and `goldsky/` are active. `_legacy/` remains historical pending a pipeline integration
fixture.

### `scripts/`

Operational utilities for clean-trade construction, flags, and Telonex acquisition. Each
retained script must eventually identify its inputs, outputs, and idempotence behavior.

### `analysis/stage0_v2/`

Polymarket and Kalshi normalization/classification code. Native metadata superseded the
Polymarket LLM labels for the active heterogeneity analysis, but the harnesses and Kalshi
pipeline remain useful for validation and future cross-platform work.

## Documentation

- `project_status.md`: current state, vintage, findings, blockers, and next step.
- `methods_reference.md`: durable data and statistical rules.
- `workflow.md`: how exploration, confirmation, artifacts, and reporting work.
- `decisions.md`: dated changes that alter interpretation.
- `storage_inventory_2026-09-06.md`: EBS retention-planning snapshot.
- `runs/2026-09-12_nfl_nba_game_dynamics_v3.md`: immutable NFL/NBA production
  reconciliation, report hashes, findings, and interpretation limits.
- `archive/`: superseded documents with historical status.

## Code archive

### `archive/analyses/early_flb_2026/`

Initial broad FLB, trader, P&L, market-accuracy, closing-price, and Manifold modules. They
predate the present data and measurement specification.

### `archive/analyses/paper_may_2026/`

The first publication-facing scripts and figures. They use pre-refresh paths and the
earlier post-event-filtered specification; the directory name `analysis/paper` did not mean
that these were the current paper pipeline.

### `archive/analyses/learnability_v1_v7/`

The LLM- and native-dimension learnability sequence, its older calibration engine, audits,
and tag-taxonomy construction. The native tag-map provenance remains here.

### `archive/analyses/calibration_heterogeneity_diagnostics_2026/`

Superseded multiprocessing novelty and cross-engine comparison utilities.

### `archive/run_scripts/calibration_heterogeneity_2026/`

Session shell scripts that preserve the original exploratory run order. They are not
supported workflow entry points.

## Generated and external material

- Heavy data and artifacts live under `/mnt/data`, never in Git.
- Reusable analysis inputs remain in the mixed-vintage `/mnt/data/embedding_difficulty`
  namespace; new result tables belong in immutable `/mnt/data/runs` directories.
- A local Mac artifact mirror is non-authoritative and may mix vintages.
- Publication figures and tables may enter Git only from a documented validated run.

## Remaining migration work

1. Add a pipeline-level integration fixture; the active analysis now has an end-to-end
   synthetic fixture.
2. Update renderers to consume an explicit immutable run rather than the historical mutable
   output directory.
3. Build a clean `paper/` replication surface from validated runs.
4. Refresh positional embedding artifacts against the 857,468-market universe before
   rerunning novelty analyses.
5. Complete backup checks before acting on the reviewed EBS retention candidates.
