# Multisport FLB time regressions: filtered and all trades

**Status:** complete production run  
**Filtered estimates:** `/mnt/data/runs/2026-09-20_flb_time_regressions_filtered_all_v1/01_filtered_estimates`  
**All-trades estimates:** `/mnt/data/runs/2026-09-20_flb_time_regressions_filtered_all_v1/02_all_estimates`  
**Production report source:** `/mnt/data/runs/2026-09-20_flb_time_regressions_filtered_all_v1/03_report`  
**Local bundle:** `output/flb_time_regressions_filtered_all_v1`

## Contract

This run implements `docs/analysis_specs/flb_time_regressions_v3.md`. Both samples are
rebuilt from the same frozen exact-fill sources, event cohort, outcomes, start/end
times, and wallet flags. The filtered sample uses `0.01 < P < 0.99` and excludes
flagged outcome-token buyers. The all-trades sample uses `0 < P < 1` and includes
flagged buyers. Every downstream calculation is identical.

The exact-source rebuild corrected one inherited inconsistency: the earlier MLB phase
input's `analysis_eligible` field removed post-end fills but did not exclude flagged
buyers. Applying the frozen wallet flags removes 89,885 MLB fills, reducing the
corrected filtered MLB count from 2,502,803 to 2,412,918. The six newer sports and
NFL/NBA exactly reconcile to their frozen filtered counts.

## Observation counts

| Sport | Filtered trades | All trades |
|---|---:|---:|
| MLB | 2,412,918 | 2,502,803 |
| NFL | 818,356 | 1,693,437 |
| NBA | 1,531,595 | 2,506,657 |
| NHL | 1,621,217 | 4,082,558 |
| Men's CBB | 681,306 | 2,507,798 |
| ATP | 1,443,414 | 3,473,263 |
| EPL | 931,721 | 2,318,860 |
| College football | 224,253 | 545,847 |
| WNBA | 238,123 | 425,126 |
| **Total** | **9,902,903** | **20,056,349** |

## Selected estimates

All coefficients are percentage-point changes in the D10-minus-D1 calibration spread
per unit of the stated normalized time.

| Specification | Filtered | All trades |
|---|---:|---:|
| All pregame + live, sport baselines/trends, per fill | -0.13 (0.07) | -0.16 (0.05) |
| All pregame + live, sport baselines/trends, equal sports | -0.12 (0.05) | -0.14 (0.05) |
| Live only, sport baselines/trends, per fill | 12.89 (3.13) | 11.58 (2.85) |
| Live only, sport baselines/trends, equal sports | 8.25 (2.63) | 7.88 (2.29) |
| Bounded `[-1,1]`, sport baselines/trends, per fill | 7.90 (2.80) | 6.49 (2.34) |
| Bounded `[-1,1]`, sport baselines/trends, equal sports | 3.70 (2.10) | 3.75 (1.80) |

The inclusion of all trades increases support and precision but does not reverse the
main pattern. The full-history common slope remains slightly negative, whereas the
live-only common slope remains positive. The continuous live curves remain negative
through most of the event and turn positive near the end under both samples.

## Reconciliation and QA

- Focused exact-sample and estimator tests: `8 passed` locally and on EC2.
- Broader multisport suite: `58 passed` on EC2.
- Both manifests contain nine sports, explicit sample rules, exact input fingerprints,
  and deterministic output fingerprints.
- Each estimator produced 910 coefficient rows, 82 model rows, 93 estimand rows, 126
  support rows, 110 live-bin rows, and 18 pregame-distribution rows.
- Suppression remains rule based: ten sport-level models are withheld in each sample,
  with null estimates rather than plotted zeros.
- The combined LaTeX report compiles in two passes to 16 letter-size pages with no
  overfull/underfull or duplicate-label warnings. All 16 pages were rendered to PNG and
  visually checked for clipping, overlap, table legibility, and suppressed curves.
