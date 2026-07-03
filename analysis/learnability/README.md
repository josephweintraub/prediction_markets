# Learnability study

Does price calibration (FLB) vary with how *learnable* a market is? Dimensions label every
BUY trade into slices; within each slice the engine runs a 10-decile price calibration.
**Methods, filters, and measurement rules live in `docs/methods_reference.md` — read that first.**
No expected direction is encoded; dimensions are measured, not presumed.

## Current: v7 native dimensions

Dimensions built from native Polymarket (Gamma) metadata instead of LLM-derived labels.

- `native_repull.py` — one-shot Gamma metadata pull (keyset pagination) → `/mnt/data/learnability/native/native_market_meta.parquet` (1.44M markets; **closed-only** — see the censoring caveat in methods_reference).
- `build_native_dims.py` — joins native metadata onto cached dims; derives the native dimensions (anchor, recurrence, prior settlements, feedback lag).
- `run_v7.py` — runs the engine over the kept LLM-free dims + native dims.
- `analysis/learnability/native/final_tag_map_v1.json` — native tags → 12 primary categories (map v1, 2026-07-01).

## Engine

- `flb_per_slice.py` — per-slice decile table, spread, signed slope, CGM 3-way clustered SEs (count- and dollar-weighted).
- `run_phase1.py` — legacy v6 driver (LLM-era dims × 3 lifecycle windows). Env: `V5_PREFIX`, `V5_LO`, `V5_HI`, `V5_INCLUDE_UPDOWN`.
- Outputs → `/mnt/data/learnability/output/` (`*_spread_summary.parquet`, `*_flb_per_slice.parquet`).

## Legacy dimension builders (v6-era, LLM-derived — kept for reproducibility)

`dimensions.py`, `dimensions_from_trades.py`, `dimensions_v4_addons.py`, `dimensions_v5.py`.
The v6 writeup and per-dimension guide are archived in `docs/archive/` with correction headers.
