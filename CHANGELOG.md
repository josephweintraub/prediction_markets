# Changelog

Notable changes to the **prediction_markets** project — infrastructure, data/dataset, tooling, and docs.
Newest first; dates are absolute (`YYYY-MM-DD`). Format loosely follows [Keep a Changelog](https://keepachangelog.com).

Research *findings* are not tracked here — methods live in `docs/methods_reference.md`; historical writeups in `docs/archive/`.

## 2026-07-02 — Docs reorganization & archive (the pivot cleanup)

- **Archived** the pre-pivot writeups to `docs/archive/` with status/correction headers: `learnability_writeup.md` (+`.pdf`), `learnability_writeup_audit.md`, `dimension_guide.md`, `data_exploration.md`, plus the loose local HTML session findings (`archive/html/`: external validation, June-2026 trades extension, horizon/censoring analysis, v7 runs, category labeling review).
- **Added `docs/methods_reference.md`** — the durable methods & data-practices reference (standard filters, calibration measurement, SE spec, data canon + caveats, retired claims). Deliberately excludes result claims to avoid anchoring future analysis.
- **Slimmed `CLAUDE.md`** (~18 KB → ~9.5 KB): stage0_v2 detail moved to `analysis/stage0_v2/README.md`; learnability protocol detail to `analysis/learnability/README.md` (rewritten — was stale Phase-1 text); superseded claims and done-plans removed; row counts and Dropbox/pipeline sections corrected.
- **Rewrote `docs/EC2_SETUP.md`** — removed the stale March-era instructions (retired paths, raw-glob config, and the dangerous "terminate + delete the EBS volume" advice).
- **Merged `native-dims` into `main`**; committed the previously-uncommitted **V2 pipeline code** (`extraction/extract_orderfilled_v2.py`, updated `refresh.py`, `transform/build_trades.py`) — the code that built the current canonical dataset; `/home/ubuntu/pipeline` is now a symlink into the repo (old copy kept at `/home/ubuntu/pipeline_pre_repo_backup`).
- **Committed the native tag map** `analysis/learnability/native/final_tag_map_v1.json`.

## 2026-07-01 — Native tag taxonomy v1 (data)

- Baked the native Gamma `tags` → category map: 264 category tags → 12 primary categories, curated with structural exclusions and FLB-distinct subcategories; holdout-validated against the LLM labels. Artifacts: `/mnt/data/learnability/native/{final_tag_map_v1.json, market_native_categories.parquet}`.

## 2026-07-01 — Resolution censoring identified (data caveat)

- Established that `trades_clean` contains only markets **resolved by build time** (Stage-4 INNER JOIN against Gamma closed markets), so recent months are censored toward fast-resolving markets (June-2026 build: ~7.5% of events / ~12.2% of dollars absent, concentrated in long-horizon judgment markets). All end-of-sample comparisons must be horizon-matched. Caveat recorded in `docs/methods_reference.md`; refresh path is a Gamma resolutions re-pull + stages 4/6 rerun (no Polygon re-fetch).
- Same caveat applies to `native_market_meta.parquet` (pulled with `closed="true"` only).

## 2026-06-24 — Trade dataset extended to 2026-06-23 (data)

- **V2 pipeline fix:** Polymarket redeployed its exchange contracts ~2026-04-28 (new `OrderFilled` topology; contracts `0xe11118…`/`0xe2222d…`); the old extractor captured nothing after, freezing the dataset at April 28. Wrote `extract_orderfilled_v2.py` (emits the old raw_events schema), extracted ~343M events from Polygon, reran the pipeline.
- Canonical clean set now **2,018,709,888 rows through 2026-06-23** (was 1,377,065,934 through 2026-04-28). Known follow-ons: `eventSlug` sparse on the new months; the LLM-enriched market dims still end in April.

## 2026-06-22 — External validation of the trade set (data)

- Cross-checked our pipeline against the independent SII-WANGZJ/Polymarket_data HuggingFace dataset: 99.8% token coverage, 99.999% resolution agreement, per-token price correlation ~0.99. Differences are benign (recency window + buy/sell row encoding). Pipeline confirmed sound.

## 2026-06-21 — Native metadata re-pull + first v7 run (branch `native-dims`)

- One-shot Gamma re-pull over the market universe → `native_market_meta.parquet` (1.44M markets, 99.98% coverage) via `analysis/learnability/native_repull.py` (keyset pagination).
- `build_native_dims.py` + `run_v7.py`: first native-dimension table and FLB run (mature window) from the new repo.

## 2026-06-21 — Repo consolidation + Phase B

- **Repo consolidation:** previously-scattered EC2 code (core analysis modules, `learnability/`, `analysis_final` paper scripts, `stage0_v2`, pipeline extraction, clean-trades scripts) consolidated into a single git repo at `/home/ubuntu/prediction_markets`, pushed to GitHub **`josephweintraub/prediction_markets`** (private, SSH deploy key). First version control + off-box backup the project has had. Code + docs only; data stays on `/mnt/data` (gitignored).
- **Phase B (commit `02b0871`):** imports/paths wired to run from the repo (`flb_per_slice_v3` → `flb_per_slice.py`, hardcoded `sys.path` removed); analysis outputs relocated to `/mnt/data`; the scattered EC2 originals (`/home/ubuntu/{learnability, analysis_final, pipeline/analysis}`) retired. A 1-dim end-to-end run from the new repo reproduced the v6 numbers exactly.
- Still pending from Phase B: converge the **local** working copy into a proper clone (needs local GitHub auth).

## 2026-06-20 / 21 — Cleanups

- Local (~1.2 GB): deleted superseded prototypes, stale writeup backups, old figure sets, one-off data dumps.
- EC2 (~32 GB off root): removed stale trade-parquet generations and rebuildable caches; kept the 152 GB pipeline stage intermediates as refresh insurance.
