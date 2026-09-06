# Corrected headline candidate run: 2026-09-06

**State:** candidate, not confirmatory

**Immutable run ID:** `2026-09-06_calibration-heterogeneity_headline-corrected-v1`

**Code commit:** `c0f9d5bd547c2ae9c03579b3400b4896b57eceb9` on
`codex/repository-cleanup`, clean worktree

**Data vintage:** `polymarket-resolved-2026-07-04`

## Command

```bash
/home/ubuntu/venv/bin/python analysis/calibration_heterogeneity/run_schemes.py \
  --window full \
  --schemes all liqrate_usdq horizon_binary hor_x_liqrate \
  --analysis-state candidate \
  --run-id headline-corrected-v1
```

The run validated nine foundational/current-window artifacts, processed 99,744,791
standard-filtered full-window observations, retained all 30 requested slices, and wrote
12 table artifacts. Manual post-run reconciliation found maximum absolute differences of
`5.54e-9`, `4.12e-9`, and `0` between reported and decile-derived count-, dollar-, and
equal-market spreads respectively; these are floating-point precision only.

## Effect of the corrected tail covariance

Point estimates reproduce the prior mutable artifacts to floating-point precision. The
joint D10−D1 clustered SE is larger in the key cells:

| Cell | Spread | Old SE → corrected SE | Old t → corrected t |
|---|---:|---:|---:|
| Aggregate, count | +1.033 pp | 0.391 → 0.497 pp | 2.64 → 2.08 |
| Lowest liquidity-rate group, count | +2.323 pp | 0.269 → 0.342 pp | 8.64 → 6.79 |
| Standalone binary ≥90d, count | +5.192 pp | 0.758 → 1.035 pp | 6.85 → 5.01 |

This confirms that the earlier shortcut understated tail-spread uncertainty in these
cells. It does not change their point estimates.

## Headline full-profile checks

All p-values below are Bonferroni-adjusted within the declared exploratory scheme and
weighting family.

| Cell and weighting | D1 error | D10 error | D10−D1 | Interpretation |
|---|---:|---:|---:|---|
| Aggregate, count | −0.185 pp (n.s.) | +0.848 pp (`p=.0149`) | +1.033 pp (`p=.0377`) | Positive favorite tail, not two-tail classic FLB |
| Aggregate, equal market | +1.039 pp (`p<1e-10`) | +0.282 pp (`p=.00080`) | −0.757 pp (`p=.000017`) | Both tails positive; activity composition drives the count spread |
| Lowest liquidity-rate group, count | −0.956 pp (`p=.00196`) | +1.368 pp (`p<1e-23`) | +2.323 pp (`p<1e-10`) | Classic FLB in the trade-weighted sample |
| Lowest liquidity-rate group, equal market | +0.966 pp (`p<1e-8`) | +0.374 pp (`p=.000024`) | −0.592 pp (`p=.00373`) | Reverses the longshot tail; not classic FLB for the typical market |
| Standalone binary ≥90d, count | −2.529 pp (`p=.00042`) | +2.663 pp (`p<1e-5`) | +5.192 pp (`p<1e-6`) | Classic FLB |
| Standalone binary ≥90d, equal market | −2.027 pp (`p=.00938`) | +2.649 pp (`p<1e-12`) | +4.677 pp (`p<1e-9`) | Classic FLB robust to market composition |

The dollar-weighted ≥90-day spread is +6.475 pp (`p=.00114`), but its negative D1 tail
does not survive the full decile-family Bonferroni correction (`p=.382`). Accordingly, the
two-tail dollar-weighted claim is not treated as independently established.

## Scope and next use

This is a candidate refresh of existing full-window headline schemes, not the next
confirmatory test. It does not replace mature/closing lifecycle diagnostics and does not
implement the narrower pre-specified standalone-binary ≥90-day × liquidity-rate cross.
Renderers must be updated to accept this explicit run directory before producing a current
paper-facing brief.
