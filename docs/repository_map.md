# Repository map and cleanup classification

**Inventory date:** 2026-09-06

This is the working classification for the repository cleanup. Moving code is deferred
until imports and reproduction targets have tests. Classification does not imply that an
old result is valid.

## Active

### `analysis/embedding_difficulty/`

The current calibration-heterogeneity workstream: universe and compact trade-base builds,
market slicing, embeddings and novelty, calibration estimation, and current report
renderers. The name is historical; liquidity and maturity are now equally central.

Planned destination: `analysis/calibration_heterogeneity/` after entry points and artifact
paths are stabilized.

### `analysis/paper/`

Publication-facing scripts and figures from the current paper effort. These need a later
reconciliation against the current decile-first specification before being labeled final.

### `pipeline/`

Canonical refresh implementation. `refresh.py`, `extraction/`, `transform/`, and
`goldsky/` are active. `_legacy/` is historical and will be archived after the current
pipeline receives a small integration test and an updated README.

### `scripts/`

Operational utilities for clean-trade construction, market and wallet flags, and Telonex
data. These are supporting code; each retained script must eventually document inputs,
outputs, and whether it is idempotent.

## Supporting but not central

### `analysis/stage0_v2/`

Polymarket and Kalshi normalization/classification code. Native metadata superseded the
Polymarket LLM labels for the current heterogeneity analysis, but the classification
harnesses and Kalshi pipeline remain useful for cross-platform work.

## Historical or mixed

### `analysis/learnability/`

Contains the v1-v7 learnability path, its older calibration engine, audits, and native-tag
work. Some utilities remain dependencies, but most research scripts represent superseded
specifications. Archive only after import dependencies are mapped and active equivalents
are tested.

### Root-level `analysis/*.py`

The first broad FLB, trader-characteristic, P&L, market-accuracy, closing-price, and
Manifold analyses. Retain temporarily for reproduction archaeology; do not use as the
current specification.

### Session runners and memos

`run_session*.sh`, `run_chain.sh`, `run_followup.sh`, and dated collaborator memos preserve
the actual exploratory sequence. They belong in a dated research archive once current
entry points replace them.

### `docs/archive/`

Historical findings and audits. Files are reference material only and must retain visible
status/correction headers.

## Generated or external

- Data and heavy artifacts belong under `/mnt/data`, not the repository.
- `analysis/output/`, pipeline logs, caches, and Python bytecode are generated.
- Small publication tables and figures may be committed only when a documented immutable
  run produced them.
- The local Mac artifact mirror is not authoritative and may mix vintages.

## Migration order

1. Secure credentials and document the current source of truth.
2. Add environment, workflow, manifest, and validation guardrails.
3. Test and correct the active calibration engine.
4. Stabilize one active command/configuration interface.
5. Rename the current workstream using `git mv`.
6. Move superseded code into indexed archives using `git mv`.
7. Reproduce the headline results and reconcile `analysis/paper/`.
8. Review EBS retention candidates separately; delete nothing based solely on age.
