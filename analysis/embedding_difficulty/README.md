# Calibration-heterogeneity analysis

**Status:** active workstream as of 2026-09-06.

This directory began as an embedding-based difficulty study and now contains the active
analysis of calibration heterogeneity across liquidity, duration, semantic market family,
and textual novelty. It will be renamed to `analysis/calibration_heterogeneity/` after its
interfaces and artifact paths are covered by tests.

Read, in order:

1. `../../docs/project_status.md`
2. `../../docs/methods_reference.md`
3. `../../docs/workflow.md`
4. `RESEARCH_LOG.md` only when historical detail is needed

## Active flow

- `build_universe.py`: construct market and token universes.
- `build_flb_base.py`: construct compact standard-filtered trade bases by lifecycle window.
- `make_*_slices.py`: construct market-to-slice schemes.
- `run_schemes.py`: join a scheme to a base and run calibration estimates.
- `flb_engine.py`: decile profiles, tail summaries, auxiliary slopes, and clustered SEs.
- `render_*.py`: render saved artifacts; renderers must not compute headline results.

## Artifact warning

The existing `/mnt/data/embedding_difficulty` namespace mixes pre-refresh and refreshed
artifacts. Embeddings, nearest-neighbor files, PCA, novelty, and cluster schemes are aligned
to the original 850,015-market universe. The refreshed universe has 857,468 markets.
Market-ID joins are safe, but positional reuse or embedding reruns are not.

Do not treat file existence as proof that an artifact matches current inputs. Until
immutable run manifests are integrated, compare each artifact against the data-vintage
notes in `RESEARCH_LOG.md` and `../../docs/project_status.md`.

## Required stabilization before the next full run

- Regenerate saved summaries after the 2026-09-06 D10-D1 covariance and thin-tail fixes.
- Add equal-market-weighted robustness estimates.
- Add synthetic unit tests and one small end-to-end fixture.
- Add immutable run manifests with input fingerprints.

Initial synthetic tests now cover tail covariance, thin-tail suppression, summary/decile
agreement, and missing scheme files. An end-to-end fixture remains to be added.
