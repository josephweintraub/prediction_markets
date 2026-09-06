# Repository map

**Last structural review:** 2026-09-06

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
- The current EBS analysis namespace remains `/mnt/data/embedding_difficulty` until its
  mixed vintages are replaced by immutable run directories.
- A local Mac artifact mirror is non-authoritative and may mix vintages.
- Publication figures and tables may enter Git only from a documented validated run.

## Remaining migration work

1. Add immutable run manifests and data-vintage validation.
2. Add an end-to-end synthetic fixture for the active analysis and pipeline.
3. Add equal-market weighting and explicit exploratory-grid multiplicity handling.
4. Reproduce current headline results using the corrected engine.
5. Build a clean `paper/` replication surface from validated runs.
6. Review EBS retention candidates separately; delete nothing based solely on age.
