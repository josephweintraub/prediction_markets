# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## CRITICAL: all real work runs on EC2, never local

**The local 16 GB M-series Mac is for editing code, reading notebooks/docs, and *tiny* parquet-metadata checks only.** Everything else runs on the EC2 instance: any join of trades with resolutions or contract dimensions, any FLB calibration / per-slice / per-decile recompute, any sensitivity sweep, any Gamma/CLOB re-pull, any LLM batch. If a task touches the 1.4B trade rows or the full contract universe, start EC2 and do it there.

Workflow every session: **start EC2 → mount EBS → run on EC2 → pull only the small summary parquets/JSON back to local → stop EC2.** The instance is ~$2/hr — **always stop it when done.** Exact commands in [EC2 for heavy lifting](#ec2-for-heavy-lifting).

Why local joins are wrong, not just slow: the local trades parquet keys markets by `conditionId = 0x hex`; the augmented parquet's `token_id` is the 77-digit decimal per-outcome; the EC2 trades parquet uses the 77-digit token as `conditionId` — which is what every FLB/calibration pipeline expects. Mixing them locally silently mis-joins.

> Version control: the project is now on GitHub (see [Repository](#repository--version-control)). But the **local** `~/prediction_markets` is not yet a clone — it sits untracked inside a home-rooted repo — so **local deletes are irreversible; confirm before removing local files.**

## What this repo is

Research codebase studying **Favorite-Longshot Bias (FLB)** on Polymarket prediction markets — the systematic overpricing of longshots / underpricing of favorites.

**Current focus — the learnability study:** how FLB varies with how *learnable* a market is (recurrence/frequency, anchorability, proposition complexity), and an active pivot from LLM-derived contract labels to **native Polymarket fields**. See [the learnability study](#the-learnability-study--current-focus) and `docs/native_data_sources.md`.

Workstreams:
- **`analysis/`** — the FLB research: DuckDB over the trade set, per-trader features, FLB calibrations, segmentation, plots/TeX. Contains the learnability study (`analysis/learnability/`) and the contract-classification pipeline (`analysis/stage0_v2/`).
- **`pipeline/`** — rebuilds the trade dataset from raw Polygon blockchain logs. Only used when refreshing data; not day-to-day.

## Repository & version control

Canonical repo: **`josephweintraub/prediction_markets`** on GitHub (private — note: the account is `josephweintraub`, not the `GoggleBoy07` shown in the git noreply email). It lives on the EC2 instance at `/home/ubuntu/prediction_markets` and pushes over an SSH deploy key (`~/.ssh/github_prediction_markets` on EC2). **Code + docs only — all data lives on `/mnt/data` and is gitignored.**

Structure:
- `analysis/` — core FLB modules + `learnability/` (the study), `stage0_v2/` (classification), `paper/` (paper-final scripts)
- `pipeline/` — trade-dataset extraction; `scripts/` — clean-trades build/dedup/resort
- `docs/` — the writeups; `README.md`, `CLAUDE.md`, `CHANGELOG.md` at root

> **Transitional (Phase B pending).** The local `~/prediction_markets` is **not yet a clone** of this repo, and the EC2 originals (`/home/ubuntu/learnability/`, `/home/ubuntu/analysis_final/`, the `pipeline/analysis` module copy) still exist as backup. Converging local → clone, wiring the imports so it runs from the new home, and retiring the originals is the remaining layout step.

## Documents map (start here)

In the repo these live under `docs/` (loose in root on the local copy until Phase B converges it).

| Doc | What it is |
|---|---|
| `dimension_guide.md` | Working guide to the 22 learnability dimensions — per-dim derivation, results, and critical assessment. |
| `learnability_writeup.md` (+ `.pdf`) | **Canonical v6 results** for FLB-by-learnability. The single source of truth for the numbers. |
| `learnability_writeup_audit.md` | Audit of the v4/v5 findings (up/down contamination, multiple-testing, one-event-family slices). |
| `native_data_sources.md` | **The current direction:** native Polymarket (Gamma/CLOB/Data) fields that replace the LLM labels, with examples, links, the field→dim map, and the API re-pull plan (§7). |
| `data_exploration.md` | Trades dedup / data-quality validation (what the clean parquet did and didn't remove). |
| `EC2_SETUP.md` | EC2 environment setup notes. |

## The learnability study — current focus

Lives in `analysis/learnability/`. Question: does FLB shrink in more "learnable" markets? Each dimension labels every BUY trade into a slice; within each slice the engine runs a 10-decile price calibration and reports the **D10−D1 calibration-error spread** (the FLB slope) with **3-way clustered SEs** (day × wallet × market), both count- and dollar-weighted, across three lifecycle windows (mature 25–80%, closing 80–100%, full 0–100%).

- **Dimension builders:**
  - `dimensions.py` — per-contract LLM-metadata dims (resolution_type, info_type, category, subject_specificity).
  - `dimensions_from_trades.py` — trade-scan dims (dollar-volume tier, contract horizon, recurrence class).
  - `dimensions_v4_addons.py` — groupings (strict/slug), prior-settlements bins, family-size × volume, residualized vol-per-contract, and the TF-IDF→SVD→HNSW text-novelty index.
  - `dimensions_v5.py` — fixed-threshold text-novelty + `dim_market_type` (up/down vs not).
- **Engine:** `flb_per_slice.py` — per-slice decile table, D10−D1 spread, Cameron-Gelbach-Miller 3-way clustered SE (count + dollar weighted). `min_trades=5000` per slice.
- **Driver:** `run_phase1.py` — runs all dims × 3 windows; excludes up/down and bot wallets; reads the clean trades view; writes to `/mnt/data/learnability/output/` on EC2 (`*_spread_summary.parquet`, `*_flb_per_slice.parquet`). Env: `V5_PREFIX`, `V5_LO`, `V5_HI`, `V5_INCLUDE_UPDOWN`. (Phase B: imports still reference the old `flb_per_slice_v3`/`dimensions_v5` names + hardcoded `/home/ubuntu` `sys.path` — to be wired.)
- **Active direction (`docs/native_data_sources.md`):** the dims above are LLM heuristics (`event_template`, info-type regex, subject-list lengths). Polymarket exposes cleaner native fields — `series`/`recurrence`, `resolutionSource`, `automaticallyResolved`/`umaResolutionStatus`, `sportsMarketType`, `negRisk`, `liquidity`/`commentCount`. The plan is a one-shot Gamma re-pull over all ~620K markets → `native_market_meta.parquet`, then rebuild the dims natively. **This re-pull runs on EC2.**

## Day-to-day

Edit code/notebooks locally; run compute on EC2. The legacy analysis notebook is `analysis/exploration.ipynb` (~100 cells, §1 Data Overview → §6 Save Results) with helpers in `analysis/`. There is no test suite, linter, or build step. Deps: `pip install -r analysis/requirements.txt`.

## Data layout (lives outside the repo)

| Path | What |
|---|---|
| `/Users/josephweintraub/polymarket_historical_data/trades/ingest_date=2026-01-23/**/*.parquet` | Local trades sample (~136M rows, 16 GB) for tiny local checks. `analysis/config.py` builds the glob from this; split across `bucket=000000/` and `bucket=000001/` — read both. The **full** 1.377B clean set lives on EC2 (below). |
| `analysis/output/` | Per-analysis outputs: parquet caches, PNG plots, TeX tables. Some are 100s of MB. The big subprocess caches (`_exp_all`, `_tc_tail_stats`) are **rebuildable** — safe to delete to reclaim space; they regenerate on next run. |
| Dropbox `Polymarket Data and Code/` | Shared deliverables (see [Dropbox](#dropbox-shared-deliverables)). |

## Core architecture

**Lazy DuckDB views over parquet.** `analysis/data_loader.py:get_connection` returns a process-global DuckDB connection that registers a `trades` VIEW over the parquet glob — nothing loads until a query runs. Settings come from `config.py`; module-level singleton (`_connection`), pass `force_new=True` to re-create.

**Heavy queries run in subprocesses.** Operations that consume tens of GB (the 25M-row `_exp_all` lookup, `_tc_tail_stats`) use `analysis/subprocess_runner.py:sp_run(fn, *args)` for guaranteed OS-level memory reclamation: the worker computes in a separate process, writes a parquet, exits; the parent re-registers it as a lazy VIEW via `register_parquet_view(con, name, path)`. `sp_run` skips work if the output exists — delete the file to force a rebuild. **Clear these with `DROP VIEW IF EXISTS`, not `DROP TABLE`** (they are views over parquet).

**FLB modules.** `favorite_longshot.py` — calibration, deciles, by-category, by-trader-volume-tier, by-experience, snap-price closing-line variants. `trader_flb.py` — trader typology (MM vs discretionary, frequency, timing, P-type) + generic `compute_flb_by_segment(...)`. `trader_characteristics.py` + `pnl_analysis.py` build the per-trader features. (The learnability study uses its own engine in `analysis/learnability/`.)

## stage0_v2/ — contract classification

`analysis/stage0_v2/` is the LLM-based contract classification — Stage 0 slug/question normalization + Stage 2 LLM tagging — with parallel Polymarket and Kalshi sub-pipelines sharing one prompt + 13-category taxonomy.

**Polymarket side.** Current dataset `stage2_per_contract.parquet` (1.12M contracts, 18 cols); the analysis uses the augmented build `stage2_per_contract_augmented.parquet` (+13 `_generic` cols).
- `stage0_v1.py` — reconstruction of the production normalizer (99.88% match; original was never saved).
- `stage0_v2.py` — Phase A normalizer with bug fixes + structural changes (intl soccer team-pair collapse, terminal `<DATE>` preserved).
- `harness.py` + `harness_assertions.json` — regression suite (groupings that must stay collapsed vs distinct). Run `run_harness_on_v1.py` / `run_harness_on_v2.py`.
- `build_augmented_dataset.py` — builds the augmented parquet (fresh LLM for v1→v2 merge templates, inheritance otherwise).
- Cost: ~$200 for the original 108K templates (Anthropic Batch API + caching); ~$6 for v2 merges. LLM key at `~/.anthropic_api_key` **on EC2 only**.

**Kalshi side (`analysis/stage0_v2/kalshi/`).** Mirrors the Polymarket schema on Kalshi question text at a per-prefix LLM grain. Output `stage2_per_contract_kalshi.parquet` (6.52M tickers × 32 cols).
- `kalshi_normalize.py` — Stage 0 normalizer, 30+ prefix-scoped collapse patterns; 24 parlay-family prefixes filtered (27.98M of 34.5M raw tickers dropped as compound multi-leg props).
- `kalshi_harness.py` + `kalshi_harness_assertions.json` — 97 blocking assertions.
- `kalshi_prompt.py` — side-effect-free `SYSTEM_A` + `FEWSHOT_A` copy.
- `audit_prefixes.py` / `audit_all_prefix_inputs.py` — pattern-discovery audits; `normalization_audit_report.md` writes up the process.
- `build_kalshi_templates.py` → `build_kalshi_prefix_pairs.py` → `stage2_kalshi_llm.py` (Batch API; `validation`/`full` modes) → `build_kalshi_per_contract.py` → `validate_kalshi.py`.
- Cost: $14.71 total. One post-batch fix: KXNHLPTS "AHL"→"NHL" (patched in JSONL + parquet, 34,024 rows).

## EC2 for heavy lifting

Instance `i-0f5b31a268af53938` in `us-east-1` (kept stopped between sessions).

**Local credentials/files:**
- SSH key: `~/Downloads/polymarket-key.pem` (mode 600, do not regenerate/overwrite)
- AWS profile: `claude-ec2` in `~/.aws/credentials` + `~/.aws/config` (region `us-east-1`)
- Instance ID also at `~/.aws/polymarket-instance.txt`

**Start** (use `aws ec2 wait` rather than local `sleep` loops):

```bash
aws --profile claude-ec2 ec2 start-instances --instance-ids i-0f5b31a268af53938
aws --profile claude-ec2 ec2 wait instance-status-ok --instance-ids i-0f5b31a268af53938
DNS=$(aws --profile claude-ec2 ec2 describe-instances --instance-ids i-0f5b31a268af53938 \
        --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
```

The IP rotates on every restart; the host key isn't pinned, so pass `-o StrictHostKeyChecking=no -o LogLevel=ERROR` on each SSH/SCP.

**After any restart**, mount the EBS data volume (not auto-mounted; device alternates `nvme0n1`/`nvme1n1`, `lsblk` shows which):

```bash
ssh -i ~/Downloads/polymarket-key.pem -o StrictHostKeyChecking=no -o LogLevel=ERROR ubuntu@$DNS \
  'sudo mount /dev/nvme1n1 /mnt/data 2>/dev/null || sudo mount /dev/nvme0n1 /mnt/data'
```

**Stop when done** (~$2/hr otherwise): `aws --profile claude-ec2 ec2 stop-instances --instance-ids i-0f5b31a268af53938`

**On the instance:**
- Python venv: `/home/ubuntu/venv/bin/python` (DuckDB 1.5.0, anthropic SDK, pyarrow)
- Anthropic key: `/home/ubuntu/.anthropic_api_key` (mode 600). Stage-2 scripts read it from there. **Do not echo it; not mirrored locally.**
- **Repo: `/home/ubuntu/prediction_markets`** — the git clone; canonical code, pushes to GitHub via the deploy key. *Phase B pending:* the old scattered sources (`/home/ubuntu/analysis_final/`, `/home/ubuntu/learnability/`, the `pipeline/analysis` module copy) still exist as backup and are being retired.
- Data: trades & pipeline data on `/mnt/data/` (`pipeline_data/`, `pipeline_output/`, `learnability/output/`); pipeline source `/home/ubuntu/pipeline/`.
- rclone has a `dropbox:` remote — use it for files >100 MB instead of SCP: `rclone copy /mnt/data/some.parquet "dropbox:Polymarket Data and Code/some/"`.

### Trades dataset: use the CLEAN parquet

Two trades parquets on EC2 — default to the clean one.

| Path | Rows | Notes |
|---|---|---|
| `/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet` | **1,377,065,934** | **CANONICAL.** Full-row exact dups removed (~4.06%, 58.2M replay rows); re-sorted by `conditionId,timestamp` (15.7 GB). `config.py:TRADES_PARQUET_GLOB` points here, so `data_loader` builds `trades_raw` over the clean set automatically. |
| `/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet` | 1,435,301,230 | RAW — untouched, kept for diffing. `config.py:TRADES_RAW_GLOB`. |

The dedup removed only rows identical on **all 11 columns** (true replays — tiny, $17/row vs $38 avg, so volume only dropped 1.84%). It did **not** touch multi-counterparty/multi-size partial fills, and there is **no** meaningful same-wallet wash trading (the earlier "17% dup / 20% wash" numbers were the partial-fill artifact — see `docs/data_exploration.md`). Rebuild scripts live in the repo `scripts/` (`build_clean.py`, `resort_clean.py`, `diff_clean.py`).

## Dropbox (shared deliverables)

Local sync: `/Users/josephweintraub/Library/CloudStorage/Dropbox/Polymarket Data and Code/`. Treat as a normal dir (`cp`/`mv`/`rm`); the client uploads in the background (large files take minutes to propagate).

Layout:
- `trades/ingest_date=2026-01-23/source=activity/bucket=00000{0,1}/` — raw trades parquet shared with the team.
- `stage2_classifications/` — Polymarket classification: `stage2_per_contract.parquet` (v1, 18 cols), `stage2_per_contract_augmented.parquet` (+13 generic), `stage2_per_contract.csv` (Excel), `stage2_full.jsonl`, `stage2_classifications_README.md`.
- `kalshi/kalshi_contract_questions_dates_available.parquet` (3.88 GB) — raw Kalshi questions/metadata.
- `kalshi/stage2_classifications/` — `stage2_per_contract_kalshi.parquet` (6.52M × 32), `kalshi_full_extracted.jsonl`, `stage2_kalshi_classifications_README.md`.

Push local→Dropbox: `cp` into the sync path. Push EC2→Dropbox: rclone. The folder is owned by josephweintraub@yale.edu and shared with collaborators (incl. Kaushik) — **only finished deliverables, no drafts/scratch.** Keep working files in the repo.

## Pipeline workstream (rare)

`pipeline/run_pipeline.py` runs a 6-stage rebuild of the trades dataset from Polygon logs (each stage has `--skip-*`). Requires `POLYGON_RPC_URL` (Alchemy paid tier — free tier is far too slow). Runs in production on EC2; the local `pipeline/` is a snapshot. The 152 GB of stage intermediates on `/mnt/data/pipeline_data/` (raw_events, deduped_events, resolved_trades) are kept as refresh insurance — they let a refresh skip the slow Polygon re-fetch.

## Conventions & practices

Lightweight practices for a small research team — enough to stay organized, no bureaucracy.

- **Version control.** GitHub is canonical. Commit logical units with a clear imperative subject and a body explaining *why*. Push after meaningful changes. Work on `main`; branch for risky/experimental changes.
- **CHANGELOG.md.** Record notable analysis, data/dataset, and infrastructure changes (newest first, absolute `YYYY-MM-DD` dates). Research *findings* live in the writeups (`docs/learnability_writeup.md`), not here.
- **Docs.** `docs/` holds the writeups; `CLAUDE.md` is the agent/onboarding guide (keep the Documents map current); `README.md` is orientation. Convert relative dates to absolute when writing docs.
- **Data & secrets never in git.** Data lives on `/mnt/data` (or Dropbox for deliverables); secrets (`*.pem`, `.anthropic_api_key`) are gitignored — `.gitignore` enforces both.
- **Where things go.** New analysis code → `analysis/` (learnability work → `analysis/learnability/`); one-off/maintenance scripts → `scripts/`; new docs → `docs/` + a line in the Documents map; finished deliverables → Dropbox.

## Patterns to avoid

- Don't load the full trades parquet into pandas — always DuckDB.
- Don't `DROP TABLE` the parquet-backed views (`_exp_all`, `_tc_tail_stats`) — use `DROP VIEW IF EXISTS`.
- Don't commit data into git — it lives on `/mnt/data` (gitignored).
- Don't do real analysis locally (see the top rule) — joins mis-key and the Mac can't hold the data.
- Don't let the local copy and the GitHub repo silently diverge — until local is a proper clone (Phase B), treat the GitHub/EC2 copy as canonical and sync deliberately.
- `paper_replication/` holds an in-progress paper draft notebook; don't touch unless explicitly working on it.
