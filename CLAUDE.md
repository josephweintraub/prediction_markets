# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## CRITICAL: all real work runs on EC2, never local

**The local 16 GB M-series Mac is for editing code, reading notebooks/docs, and *tiny* parquet-metadata checks only.** Anything that touches the 2B trade rows or the full contract universe — joins, calibration runs, sensitivity sweeps, API re-pulls, LLM batches — runs on the EC2 instance.

Workflow every session: **start EC2 → mount EBS → run on EC2 → pull only small summary parquets/JSON back → stop EC2.** The instance is ~$2/hr — **always stop it when done.** Exact commands in [EC2 for heavy lifting](#ec2-for-heavy-lifting).

Why local joins are wrong, not just slow: the local trades sample and the EC2 set key markets differently (`conditionId` semantics differ) — local joins silently mis-key. See the join-key note in `docs/methods_reference.md`.

> The **local** `~/prediction_markets` is still **not a git clone** — it sits untracked inside a home-rooted repo, so **local deletes are irreversible; confirm before removing local files.** The GitHub/EC2 copy is canonical; sync deliberately.

## What this repo is

Research codebase studying **price calibration / favorite-longshot bias (FLB)** on Polymarket.

**Current focus:** how calibration varies with market characteristics derived from **native Polymarket metadata** (recurrence/series, resolution mechanics, anchorability, feedback speed — the "learnability" dimensions, v7). Direction is measured, not presumed — do not import expected signs from earlier writeups.

**Read `docs/methods_reference.md` before any analysis** — it holds the standard filters (BUY-side, price bounds, up/down + bot exclusion, slice floors, lifecycle windows), the calibration measurement rules (signed slope primary; D10−D1 secondary), the clustered-SE spec, and the **resolution-censoring caveat** that governs any end-of-sample comparison.

Workstreams:
- **`analysis/`** — FLB research: DuckDB over trades, calibration engine, learnability study (`analysis/learnability/` — see its README), LLM contract classification (`analysis/stage0_v2/` — see its README; superseded by native fields for dimension work but still the category cross-check).
- **`pipeline/`** — rebuilds the trade dataset from Polygon logs (see its README). Rare; only for data refreshes.
- **`scripts/`** — clean-trades build/dedup/resort one-offs.

## Repository & version control

Canonical repo: **`josephweintraub/prediction_markets`** on GitHub (private; account `josephweintraub`, not the `GoggleBoy07` in the git noreply email). Lives on EC2 at `/home/ubuntu/prediction_markets`; pushes over the SSH deploy key (`~/.ssh/github_prediction_markets` on EC2). All commits happen on EC2 (no local git auth — deliberate). **Code + docs only — data lives on `/mnt/data`, gitignored.**

Consolidation status: Phase B (wiring + retiring scattered EC2 originals) completed 2026-06-21; docs reorganization + archive 2026-07-02. Remaining: converge the local copy into a proper clone (needs local GitHub auth).

## Documents map

Everything current lives in `docs/`; superseded material is in `docs/archive/` (each file carries a status/correction header — treat archived claims as historical, not current).

| Doc | What it is |
|---|---|
| `methods_reference.md` | **Start here.** Durable methods & data practices: filters, calibration measurement, SE spec, data canon + caveats, retired claims. |
| `native_data_sources.md` | Native Polymarket (Gamma/CLOB/Data) field inventory and field→dimension map. Status header records what has already been pulled. |
| `EC2_SETUP.md` | Thin EC2 environment notes (Jupyter tunnel, tmux, monitoring). Start/stop/mount live here in CLAUDE.md. |
| `archive/` | v1–v6-era writeups (learnability writeup + audit, dimension guide, data exploration) and HTML session findings. Reference only. |

## Data layout

| Path | What |
|---|---|
| `/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet` (EC2) | **CANONICAL** clean trades — 2,018,709,888 rows through 2026-06-23. Subject to the resolution-censoring caveat (`docs/methods_reference.md`). |
| `/mnt/data/pipeline_root_output/trades.parquet` (EC2) | Raw trades, kept for diffing. |
| `/mnt/data/learnability/` (EC2) | Study outputs (`output/`), native metadata (`native/`: `native_market_meta.parquet` — closed-only, `market_native_categories.parquet`, tag map). |
| `/Users/josephweintraub/polymarket_historical_data/trades/ingest_date=2026-01-23/**/*.parquet` | Local sample (~136M rows) for tiny metadata checks only; both `bucket=000000/` and `bucket=000001/`. `analysis/config.py` builds the glob. |
| `analysis/output/` (local) | Per-analysis caches/plots. Big subprocess caches are rebuildable — safe to delete. |

## Core architecture

**Lazy DuckDB views over parquet.** `analysis/data_loader.py:get_connection` registers a `trades` VIEW over the parquet glob; nothing loads until a query runs. Settings in `config.py`; process-global singleton, `force_new=True` to re-create.

**Heavy queries run in subprocesses.** `analysis/subprocess_runner.py:sp_run(fn, *args)` — worker computes, writes parquet, exits; parent re-registers it as a lazy VIEW. `sp_run` skips work if the output exists (delete the file to force rebuild). **Clear these with `DROP VIEW IF EXISTS`, never `DROP TABLE`.**

**Calibration engine:** `analysis/learnability/flb_per_slice.py` (per-slice deciles, spreads, CGM 3-way clustered SEs); drivers `run_phase1.py` (v6 dims) / `run_v7.py` (native dims). Legacy broad-FLB modules: `favorite_longshot.py`, `trader_flb.py`, `trader_characteristics.py`, `pnl_analysis.py`; legacy notebook `analysis/exploration.ipynb`.

## EC2 for heavy lifting

Instance `i-0f5b31a268af53938` in `us-east-1` (kept stopped between sessions).

**Local credentials/files:**
- SSH key: `~/Downloads/polymarket-key.pem` (mode 600, do not regenerate/overwrite)
- AWS profile: `claude-ec2` in `~/.aws/credentials` + `~/.aws/config` (region `us-east-1`)
- Instance ID also at `~/.aws/polymarket-instance.txt`

**Start** (use `aws ec2 wait`, not local sleep loops):

```bash
aws --profile claude-ec2 ec2 start-instances --instance-ids i-0f5b31a268af53938
aws --profile claude-ec2 ec2 wait instance-status-ok --instance-ids i-0f5b31a268af53938
DNS=$(aws --profile claude-ec2 ec2 describe-instances --instance-ids i-0f5b31a268af53938 \
        --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
```

The IP rotates every restart; pass `-o StrictHostKeyChecking=no -o LogLevel=ERROR` on each SSH/SCP.

**After any restart**, mount the EBS data volume (device alternates `nvme0n1`/`nvme1n1`):

```bash
ssh -i ~/Downloads/polymarket-key.pem -o StrictHostKeyChecking=no -o LogLevel=ERROR ubuntu@$DNS \
  'sudo mount /dev/nvme1n1 /mnt/data 2>/dev/null || sudo mount /dev/nvme0n1 /mnt/data'
```

**Stop when done** (~$2/hr otherwise): `aws --profile claude-ec2 ec2 stop-instances --instance-ids i-0f5b31a268af53938`

**On the instance:**
- Python venv: `/home/ubuntu/venv/bin/python` (DuckDB, anthropic SDK, pyarrow)
- Anthropic key: `/home/ubuntu/.anthropic_api_key` (mode 600; **do not echo; not mirrored locally**)
- Repo: `/home/ubuntu/prediction_markets` (canonical; `/home/ubuntu/pipeline` is a symlink into it)
- Data: `/mnt/data/` (`pipeline_output/`, `pipeline_root_output/`, `pipeline_data/`, `learnability/`)
- rclone `dropbox:` remote — use for files >100 MB instead of SCP: `rclone copy /mnt/data/some.parquet "dropbox:Polymarket Data and Code/some/"`

## Dropbox (shared deliverables)

Local sync: `/Users/josephweintraub/Library/CloudStorage/Dropbox/Polymarket Data and Code/` — treat as a normal dir; uploads propagate in the background. Current top-level: `trades_onchain/`, `newest_trades_onchain/`, `trades_old/`, `stage2_classifications/`, `kalshi/`, `learnability/`, `manifold/`, `kv/`, `mae/`. Shared with collaborators (incl. Kaushik) — **only finished deliverables, no drafts/scratch.**

## Pipeline workstream (rare)

`pipeline/` extracts Polymarket OrderFilled events from Polygon and builds the trade set. Live entry point: `refresh.py` (stage flags inside); the exchange-migration extractor is `extraction/extract_orderfilled_v2.py` (Polymarket redeployed its exchange contracts ~2026-04-28; the v2 extractor handles the new contracts and emits the old raw_events schema). Requires `POLYGON_RPC_URL` (Alchemy paid tier). The ~152 GB of stage intermediates on `/mnt/data/pipeline_data/` are refresh insurance — a resolutions refresh reruns stages 4/6 without touching Polygon. `_legacy/` holds superseded scripts.

## Conventions & practices

- **Version control.** GitHub is canonical. Commit logical units, imperative subject, body explains *why*. Push after meaningful changes. Work on `main`; branch for risky/experimental work and merge back promptly once it becomes the direction.
- **CHANGELOG.md.** Record notable analysis/data/infrastructure changes (newest first, absolute `YYYY-MM-DD` dates) **in the same session as the change**. Findings live in docs, not the changelog.
- **Docs.** New docs → `docs/` + a line in the Documents map. Superseded docs → `docs/archive/` with a status header, and prune stale claims from live docs at the same time. Convert relative dates to absolute.
- **Data & secrets never in git** — enforced by `.gitignore` (`*.parquet`, `*.pem`, `.anthropic_api_key`, …).
- **Where things go.** Analysis code → `analysis/` (learnability → `analysis/learnability/`); one-off scripts → `scripts/` in the repo, not `/home/ubuntu`; finished deliverables → Dropbox; working files stay in the repo.

## Patterns to avoid

- Don't load the full trades parquet into pandas — always DuckDB.
- Don't `DROP TABLE` parquet-backed views — `DROP VIEW IF EXISTS`.
- Don't commit data into git — it lives on `/mnt/data` (gitignored).
- Don't do real analysis locally — joins mis-key and the Mac can't hold the data.
- Don't interpret end-of-sample or across-time calibration without the resolution-censoring caveat (`docs/methods_reference.md`).
- Don't headline a D10−D1 sign flip without checking the signed slope — see retired claims in `docs/methods_reference.md`.
- Don't treat `docs/archive/` claims as current — several were later corrected; the headers say which.
- `paper_replication/` (local) holds an in-progress paper draft — don't touch unless explicitly working on it.
