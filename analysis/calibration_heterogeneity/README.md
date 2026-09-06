# Calibration-heterogeneity analysis

**Status:** active workstream as of 2026-09-06.

This directory began as an embedding-based difficulty study and now contains the active
analysis of calibration heterogeneity across liquidity, duration, semantic market family,
and textual novelty.

Read, in order:

1. `../../docs/project_status.md`
2. `../../docs/methods_reference.md`
3. `../../docs/workflow.md`
4. `RESEARCH_LOG.md` only when historical detail is needed

## Active flow

- `build_universe.py`: construct market and token universes.
- `build_flb_base.py`: construct compact standard-filtered trade bases by lifecycle window.
- `make_*_slices.py`: construct market-to-slice schemes.
- `run_schemes.py`: validate the declared vintage, join schemes to a base, apply
  multiplicity corrections, and write an immutable run.
- `data_vintage.py`: enforce artifact paths, row counts, sizes, schemas, and metadata
  hashes from a committed vintage declaration.
- `run_artifacts.py`: create unique run directories and record environment, Git, inputs,
  parameters, validations, and outputs in `manifest.json`.
- `flb_engine.py`: count-, dollar-, and equal-market-weighted decile profiles, tail
  summaries, auxiliary slopes, and three-way clustered SEs.
- `render_*.py`: render saved artifacts; renderers must not compute headline results.

## Artifact warning

The existing `/mnt/data/embedding_difficulty` namespace mixes pre-refresh and refreshed
artifacts. Embeddings, nearest-neighbor files, PCA, novelty, and cluster schemes are aligned
to the original 850,015-market universe. The refreshed universe has 857,468 markets.
Market-ID joins are safe, but positional reuse or embedding reruns are not.

Do not treat file existence as proof that an artifact matches current inputs. The driver
validates the committed declaration in
`../../configs/data_vintages/polymarket_2026-07-04.json` before reading a base.

## Running an analysis

From the repository root on EC2:

```bash
/home/ubuntu/venv/bin/python analysis/calibration_heterogeneity/run_schemes.py \
  --window full --schemes all liqrate_usdq horizon_binary hor_x_liqrate \
  --analysis-state candidate
```

The command creates `/mnt/data/runs/<date>_calibration-heterogeneity_<run-id>/` and never
overwrites an existing run. A confirmatory run additionally refuses a dirty Git worktree.
Unit tests include tail inference, equal-market replication invariance, multiplicity, data
vintage failure, and a small Parquet-to-manifest end-to-end fixture.
