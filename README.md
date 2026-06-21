# prediction_markets

Research codebase — Favorite-Longshot Bias (FLB) on Polymarket, including the learnability study.
Canonical copy lives on the EC2 instance; see `CLAUDE.md` for the EC2-first workflow and `docs/` for writeups.
**Code only — all data lives on `/mnt/data` (gitignored).**

## Layout
- `analysis/` — core FLB modules; `learnability/` (the study), `stage0_v2/` (LLM classification), `paper/` (paper-final scripts)
- `pipeline/` — trade-dataset extraction from Polygon logs
- `scripts/` — clean-trades build / dedup / resort
- `docs/` — dimension_guide, learnability_writeup (+ audit), native_data_sources, data_exploration, EC2_SETUP

See `CLAUDE.md` -> "Documents map" for what each doc is.
