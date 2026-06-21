# Changelog

Notable changes to the **prediction_markets** project — infrastructure, data/dataset, tooling, and docs.
Newest first; dates are absolute (`YYYY-MM-DD`). Format loosely follows [Keep a Changelog](https://keepachangelog.com).

Research *findings* are not tracked here — they live in the versioned writeups (`docs/learnability_writeup.md`, currently v6).

## [Unreleased]

### Added
- `docs/native_data_sources.md` — inventory of native Polymarket (Gamma / CLOB / Data API) fields that replace the LLM-derived contract labels, with worked examples + clickable links, the native-field → learnability-dimension map, API access mechanics (pagination, rate limits, enum value sets), and the one-shot re-pull plan (§7).
- This `CHANGELOG.md`, and a **Conventions & practices** section in `CLAUDE.md`.

### Changed
- 2026-06-21 — **Repo consolidation.** Previously-scattered EC2 code (the `pipeline/analysis` core modules, the `learnability/` study, the `analysis_final` paper scripts, `stage0_v2` classification, pipeline extraction, and the clean-trades build scripts) consolidated into a single git repo at `/home/ubuntu/prediction_markets`, committed, and pushed to GitHub **`josephweintraub/prediction_markets`** (private, SSH deploy key). First version control + off-box backup the project has had. Code + docs only; data stays on `/mnt/data` (gitignored).
- 2026-06-21 — **`CLAUDE.md` reworked twice:** documents map + learnability workstream + native-data pivot added; then updated for the new repo layout, with a Repository section and the conventions above.

### Removed
- 2026-06-20 — **Local cleanup** (~1.2 GB reclaimed): deleted superseded ingest/validation prototypes, stale writeup backups, old figure sets (`v3_figures/`, `v5_figures/`), and big one-off data dumps; relocated an unrelated ZK-crypto homework set out of the project.
- 2026-06-21 — **EC2 cleanup** (~32 GB off the root disk): removed stale trade-parquet generations (`trades_through_2026-03-15`, `trades_v1_no_maker_flag`) and rebuildable caches. Kept the 152 GB of pipeline stage intermediates as refresh insurance, and the Manifold/Metaculus dumps.

### Pending — Phase B (finish the structured layout)
- Wire imports/data paths so the repo runs from its new home (`run_phase1.py` still imports old `flb_per_slice_v3`/`dimensions_v5` names + hardcoded `/home/ubuntu` `sys.path`); smoke-test a run.
- Relocate `pipeline/analysis/output` → `/mnt/data/analysis_output`.
- Retire the now-redundant EC2 originals (`learnability/`, `analysis_final/`, the `pipeline/analysis` module copy).
- Converge the local working copy into a clone of the GitHub repo (needs local GitHub auth); decide whether `analysis/exploration.ipynb` is tracked.
