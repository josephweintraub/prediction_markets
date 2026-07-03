# stage0_v2 — LLM contract classification

Stage 0 slug/question normalization + Stage 2 LLM tagging, with parallel Polymarket and Kalshi
sub-pipelines sharing one prompt + 13-category taxonomy.

> **Status (2026-07):** superseded for learnability-dimension work by native Polymarket fields
> (see `docs/native_data_sources.md` and the v7 pipeline in `analysis/learnability/`), but the
> per-contract classifications remain the cross-check / holdout reference for the native tag
> taxonomy, and the Kalshi side is the only classification that exists for Kalshi.

## Polymarket side

Dataset `stage2_per_contract.parquet` (1.12M contracts, 18 cols); analyses use the augmented
build `stage2_per_contract_augmented.parquet` (+13 `_generic` cols). Both in Dropbox
`stage2_classifications/`.

- `stage0_v1.py` — reconstruction of the production normalizer (99.88% match; the original was never saved).
- `stage0_v2.py` — Phase A normalizer with bug fixes + structural changes (intl soccer team-pair collapse, terminal `<DATE>` preserved).
- `harness.py` + `harness_assertions.json` — regression suite (groupings that must stay collapsed vs distinct). Run via `run_harness_on_v1.py` / `run_harness_on_v2.py`.
- `build_augmented_dataset.py` — builds the augmented parquet (fresh LLM for v1→v2 merge templates, inheritance otherwise).
- Cost: ~$200 for the original 108K templates (Anthropic Batch API + caching); ~$6 for v2 merges. LLM key at `~/.anthropic_api_key` **on EC2 only**.

## Kalshi side (`kalshi/`)

Mirrors the Polymarket schema on Kalshi question text at a per-prefix LLM grain.
Output `stage2_per_contract_kalshi.parquet` (6.52M tickers × 32 cols), in Dropbox `kalshi/stage2_classifications/`.

- `kalshi_normalize.py` — Stage 0 normalizer, 30+ prefix-scoped collapse patterns; 24 parlay-family prefixes filtered (27.98M of 34.5M raw tickers dropped as compound multi-leg props).
- `kalshi_harness.py` + `kalshi_harness_assertions.json` — 97 blocking assertions.
- `kalshi_prompt.py` — side-effect-free `SYSTEM_A` + `FEWSHOT_A` copy.
- `audit_prefixes.py` / `audit_all_prefix_inputs.py` — pattern-discovery audits; `normalization_audit_report.md` writes up the process.
- Build chain: `build_kalshi_templates.py` → `build_kalshi_prefix_pairs.py` → `stage2_kalshi_llm.py` (Batch API; `validation`/`full` modes) → `build_kalshi_per_contract.py` → `validate_kalshi.py`.
- Cost: $14.71 total. One post-batch fix: KXNHLPTS "AHL"→"NHL" (patched in JSONL + parquet, 34,024 rows).
