# Changelog

Notable changes to the **prediction_markets** project — infrastructure, data/dataset, tooling, and docs.
Newest first; dates are absolute (`YYYY-MM-DD`). Format loosely follows [Keep a Changelog](https://keepachangelog.com).

Research *findings* are not tracked here — methods live in `docs/methods_reference.md`; historical writeups in `docs/archive/`.

## 2026-07-04 — Consolidated dimensions layer + learnability horse race v1; comments v1 landed

- **`analysis/learnability/market_dimensions_v1.py`** (new): one row per market
  (1.63M) joining labels v2, embedding novelty + k-means cluster assignments,
  trade-derived lifetime/liquidity, an explicit resolution-domain anchor ladder
  (data_feed / official_scorer / social / other_url / none), prior-instances-in-
  series, and vintage → `market_dimensions_v1.parquet`.
- **`analysis/learnability/horse_race_v1.py`** (new): the joint interacted
  calibration model over 8 pre-specified dimensions — per-group [1, price]
  absorption (no-FE / topic-FE / cluster-k200-FE), count- and dollar-weighted,
  CGM 3-way clustered SEs on every coefficient; plus dimension VIF/correlation,
  within-cluster identification shares, univariate quintile gradients, the
  within-series test (series FE, slope drift on ln prior instances + ordinal
  thirds), and price-band dollar-transfer tables. Artifacts:
  `horse_race_v1_*.parquet`, `within_series_v1.parquet`,
  `horse_race_v1_econ_bands*.parquet` + `_summary.json`; report
  `learnability_horse_race_v1.html` (local). Findings in the report, not here.
- **Comments v1 complete**: 365,402 comments (58,945 events scanned, 0 failed,
  99.998% author-wallet linkage) → `learnability/native/comments_v1.parquet`.
  Note: comments concentrate in 12,854 events; ~46K events with commentCount>0
  returned none via the Event entity — entity-type mismatch suspected, unresolved.

## 2026-07-04 — Horizon-matched FLB rerun on the de-censored set; RPC key rotation

- **`analysis/learnability/horizon_flb_v2.py`** (new, committed): horizon × period
  calibration cells, labels-v2 topic × period, and the three pre-specified subcategory
  contrasts, all on the refreshed clean set — signed slope primary (with CGM 3-way
  clustered SE vs slope=1), D10−D1 secondary, count- and dollar-weighted, standard
  filters. Replaces the lost ad-hoc `fixed_flb.py` (EC2 /tmp, 2026-07-01); 2025 cells
  reproduce it to the third decimal. Artifacts:
  `/mnt/data/learnability/output/horizon_flb_v2_*.parquet` + `_summary.json`;
  report `horizon_flb_v2_report.html` (local). Claim-status change recorded in
  `docs/methods_reference.md` (retired claims).
- **Polygon RPC key rotated and removed from code**: all five hardcoded call sites in
  `pipeline/` now read `POLYGON_RPC_URL` from the environment with fallback to
  `~/.polygon_rpc_url` (untracked, mode 600). The old key is deactivated, so the
  copies in git history no longer authenticate.

## 2026-07-04 — Resolutions refresh (de-censoring), labels v2 + hand-lock, native meta v2

- **Resolutions refresh executed** (`pipeline/refresh.py --skip-extract`, first use of the
  periodic de-censoring path): Stage-4 coverage 96.5% → **97.4%**; canonical clean set now
  **2,036,128,538 rows** (+17.4M vs the 2026-06-24 build, concentrated at the frontier:
  2026-05 +4.3M, 2026-06 +7.5M — the previously resolution-censored long-horizon cohorts
  filling in as markets resolve). Old clean archived (`trades_clean_snap20260624`);
  `market_flags` (2,388,005 tokens) and `wallet_flags` (333,676 non-human wallets, 20.3%)
  rebuilt on the new set. Stale pre-V2 archives deleted for disk.
- **Gamma deprecated offset `/events`** (as with offset `/markets` in May): the eventSlug
  backfill silently collapsed to 6% coverage mid-refresh; `fetch_events_to_map` now uses
  `/events/keyset` and warns if the map is suspiciously small. Repaired same-day from the
  keyset events pull — eventSlug coverage now **100.00%**.
- **Native market metadata v2**: `native_market_meta_v2.parquet` — 1,629,907 markets
  (1.58M closed + 45K **open** — the censored class now has metadata) × 54 cols, raw JSONL
  payloads kept under `native/raw/`. Keyset gotcha: `/markets/keyset` unfiltered returns
  open-only; pass closed=true and closed=false explicitly. Field coverage audited before
  use (tags 99.9%, rules 100%, tick size 99.7%; sportsMarketType 46%, volume_num 79% —
  patchy fields are per-use).
- **Labels v2** (`analysis/learnability/tag_taxonomy/`): `market_labels_v2.parquet` —
  topic + subcategory (~35 designed buckets, 96.5% coverage) + mechanic + event_family
  (Iran is Geopolitics topic + event family, no longer a topic) + entity tags +
  vote_margin/abstain + provenance + native fields, for all 1.63M markets.
  **703 top-volume markets hand-locked** (blind classify → 94.2% agreement with the vote →
  3-judge panel adjudication, 70/72 unanimous, border rules R1–R9 in the module README).
  **Tail stress test** (500-market adversarial audit): 0.4% topic error; three systematic
  subcategory fixes shipped (hype-tag misroute of 44,100 Hyperliquid markets; Mentions
  subcat inheritance; sports non-game routing to Props/Drafts).
- **Comments v1 sample**: latest ≤100 comments per commented event (58,945 events,
  author proxyWallet included — joins to the trade tape) →
  `/mnt/data/learnability/native/comments_v1.parquet` (+ raw JSONL). Capped sample;
  full universe is 83.3M comments (~2-day job) if ever needed.

## 2026-07-03 — Extended-set filter gaps FIXED in shared plumbing; liquidity axis

- **`scripts/build_market_flags.py`** → canonical token spine
  `/mnt/data/pipeline_output/market_flags.parquet` (token_id, market_id, winning_outcome,
  market-level `is_updown` from Gamma metadata — never trades' eventSlug). Hard coverage
  check: 100% of trades_clean tokens (2,373,197), 828,816 up/down tokens flagged.
- **`analysis/learnability/run_phase1.py` patched** (and `run_v7.py` via delegation):
  `_closed_markets` now built from market_flags (fresh spine + market-level up/down
  exclusion) instead of the stale `market_resolutions_enriched.parquet`. Smoke-tested.
  Archived audit scripts under `analysis/learnability/audits/` intentionally left on the
  old plumbing (historical records).
- **`docs/methods_reference.md` amended**: canonical spine entry; up/down exclusion
  redefined at market level; eventSlug empty-string gotcha recorded.
- **Liquidity axis added to the embedding-difficulty workstream**
  (`make_liquidity_slices.py`): volume tiers, era-relative (within-birth-month) quintiles,
  inclusion floors ($1k/$10k/$100k), rolling-median (25% of trailing-90d median) rule,
  novelty×liquidity interaction. Report v2 with per-section reading guides.
- **Multi-field embeddings (session 3)**: rules (market description) and context
  (event-level description) embedded separately with unique-text dedup
  (`embed_fields.py`); pre-registered combined weightings (`make_field_variants.py`);
  per-variant novelty + within-vintage FLB (`make_field_novelty_slices.py`);
  `compute_novelty.py` inner loop ported to torch (bit-identical, 4.6× faster,
  correctness-gated in `run_session3.sh`). Report v3 section 4b.

## 2026-07-03 — Embedding-difficulty workstream + extended-set filter fixes

- **New workstream `analysis/embedding_difficulty/`** — embedding-based intrinsic-difficulty
  study (question-text embeddings via bge-small-en-v1.5; PCA structure; novelty/precedent-
  density at birth with strict backward discipline; multi-granularity k-means; action/subject
  precedent from Stage-2 labels). Running log in `analysis/embedding_difficulty/RESEARCH_LOG.md`;
  self-contained engine (`flb_engine.py`) implements the **signed calibration slope** (first
  code implementation of the primary metric in `docs/methods_reference.md`) + CGM 3-way SEs.
  Artifacts under `/mnt/data/embedding_difficulty/`.
- **Extended-set filter gaps found & worked around** (follow-on to the known "eventSlug
  sparse" note from 2026-06-24 — the consequences were bigger than noted):
  (a) `market_resolutions_enriched.parquet` (Mar 2026) covers only ~49% of trade rows in the
  extended set — joins through it silently halve the sample; the June-24 artifacts
  (`pipeline/output/market_resolutions.parquet` + `/mnt/data/pipeline_data/token_map.parquet`)
  cover 100% of traded tokens. (b) `eventSlug` is the **empty string** on newer markets, so the
  standard `NOT LIKE '%updown%'` exclusion catches ~nothing — ~416K up/down markets (1.34B raw
  rows; 135M standard-filtered BUY trades) were flowing into any standard-filter run on the
  extended set. Workaround: market-level up/down flag from Gamma event_slug/series_slug/question
  (`build_universe.py`). Existing drivers (`run_phase1.py` etc.) still carry both gaps.
- Known caveat: `wallet_flags.parquet` last built 2026-06-11 — bot coverage of wallets first
  active after the June extension is unaudited.

## 2026-07-03 — Reporting-reproducibility convention

- Findings reports (HTML) must transcribe their headline numbers from a committed script's summary artifact (parquet/JSON) and cite the script + artifact near the top; exploration queries get promoted into a script before a report is finalized. Recorded in CLAUDE.md conventions and `docs/methods_reference.md` ("Reporting reproducibility").

## 2026-07-02 — Docs reorganization & archive (the pivot cleanup)

- **Archived** the pre-pivot writeups to `docs/archive/` with status/correction headers: `learnability_writeup.md` (+`.pdf`), `learnability_writeup_audit.md`, `dimension_guide.md`, `data_exploration.md`, plus the loose local HTML session findings (`archive/html/`: external validation, June-2026 trades extension, horizon/censoring analysis, v7 runs, category labeling review).
- **Added `docs/methods_reference.md`** — the durable methods & data-practices reference (standard filters, calibration measurement, SE spec, data canon + caveats, retired claims). Deliberately excludes result claims to avoid anchoring future analysis.
- **Slimmed `CLAUDE.md`** (~18 KB → ~9.5 KB): stage0_v2 detail moved to `analysis/stage0_v2/README.md`; learnability protocol detail to `analysis/learnability/README.md` (rewritten — was stale Phase-1 text); superseded claims and done-plans removed; row counts and Dropbox/pipeline sections corrected.
- **Rewrote `docs/EC2_SETUP.md`** — removed the stale March-era instructions (retired paths, raw-glob config, and the dangerous "terminate + delete the EBS volume" advice).
- **Merged `native-dims` into `main`**; committed the previously-uncommitted **V2 pipeline code** (`extraction/extract_orderfilled_v2.py`, updated `refresh.py`, `transform/build_trades.py`) — the code that built the current canonical dataset; `/home/ubuntu/pipeline` is now a symlink into the repo (old copy kept at `/home/ubuntu/pipeline_pre_repo_backup`).
- **Committed the native tag map** `analysis/learnability/native/final_tag_map_v1.json`.
- **Local-copy policy made official:** the local Mac copy stays a git-auth-less viewer (no clone, no local push); the "converge local into a clone" item is retired. CLAUDE.md updated.

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
