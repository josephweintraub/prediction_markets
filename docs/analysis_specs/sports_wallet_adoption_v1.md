# Nine-sport wallet-filter adoption

Status: implementation contract, 10 October 2026. User approved current
nine-sport results first. Older calibration-heterogeneity studies, encoded bases,
schemes and completed research artifacts remain unchanged and explicitly legacy.

## Question and preserved analysis

Measure how the corrected wallet identities affect the current nine-sport FLB
samples and results without confounding the repair with restored MLB coverage or
differences in historical wallet-flag provenance. The scientific definitions are
those in `flb_time_regressions_v3.md` and its v2/v1 dependencies: inferred
outcome-token BUY per resolved fill, calibration `Y-P`, fixed price bins,
normalized exact UTC time, all pregame history, existing models/weights,
three-way day/wallet/event clustering and existing support suppression.

Preserve the accepted cohort, token outcomes, event clocks, exact fill identities,
economic payloads, native inferred buyer IDs and literal event-end inclusion.
WTA/UFC remain excluded. No new API collections, action recovery, holdings,
FIFO matching or causal profit-taking estimates are part of this adoption.
Resolved-source censoring and qualified ATP clocks remain inherited limitations.

## Controlled stages

1. Recompute `F0` from original CLEAN and `F1` from repaired CLEAN using the
   unchanged `analysis.bot_filter.build_wallet_flags` on **all sides**, admitting
   timestamps at or after 1590969600. No price, bot or up/down filter enters this
   classifier population. Preserve its candidate gate, thresholds and composites.
   Do not execute the historical overwrite wrapper.
2. Compare F0/F1 and both historical flag artifacts. Differences between F1/F0
   are the controlled identity-repair contrast; differences between F0 and saved
   historical flags are a separate producer/cohort/vintage gap, not proof of
   historical misclassification. Count admitted and excluded rows explicitly.
3. Refresh embedded flags consistently in every sports exact branch. The shared
   sports flag CLI argument currently affects only MLB; six newer sports and
   NFL/NBA require separately refreshed exact artifacts. Their buyer IDs come
   directly from raw maker/taker fields, not the repaired expanded CLEAN.
4. Reproduce the old MLB-source/shared-flag sample and the archived estimates,
   including support, suppression, point estimates and clustered uncertainty,
   under the reviewed current execution settings. Fail closed on incompatible
   reproduction before interpreting the controlled contrasts. Then compare restored MLB
   with historical shared flags, restored MLB with F0, and restored MLB with F1.
   Run all-trades and filtered estimators using one coherent generation at a time.
5. Publish compact comparison artifacts and a data-first LaTeX report. Preserve
   all previous outputs; do not redirect shared input paths or overwrite reports.

For a fixed restored-MLB exact-source bundle S and historical shared flags H,

`E(S,F1)-E(S,H) = [E(S,F1)-E(S,F0)] + [E(S,F0)-E(S,H)]`.

The MLB coverage contrast uses restored versus old exact inputs with H held
fixed. Compute estimate differences only when both estimates are supported;
otherwise show support/suppression changes. These are reproducible sample and
estimate differences, not causal effects of bots or trader motives.

## Sample and validation gates

- All trades: accepted cohort, exact timestamp at/before recorded end,
  `0 < P < 1`, flagged buyers retained.
- Filtered: same cohort/time, `0.01 < P < 0.99`,
  `NOT coalesce(is_nonhuman,false)`. Preserve and count missing/null labels.
- New flags have unique nonblank normalized keys, nonnull Boolean classifications,
  exact admitted-wallet coverage, and `sum(n_trades)` equal admitted source rows.
- Bind corrected inputs to the completed immutable repair and saved QA receipts;
  bind classifier, all inputs, flags and consumers to one source/adoption generation.
- Reopen outputs for schema, exact non-flag payload/native-ID preservation,
  refreshed-label equality, sample subset laws and disjoint exclusion accounting.
- With the same sports exact sources, reflagging alone preserves all-trades
  observations, point estimates and uncertainty. Restored MLB coverage is separate.
- Reproduce the complete archived filtered estimate grid within declared numerical
  tolerances; matching sample counts alone is not sufficient.
- Recompute final filtered support, sport-median durations, weights, kernel grids,
  rank, suppression and intervals using the unchanged estimator definitions.
- Require committed/reviewed source, metadata-only capacity preflight, separately
  reviewed preflight binding, bounded serial execution and immutable no-replace
  publication. Keep failure evidence and final exit/resource receipts.

## Versioned destinations and lifecycle

Production parent: `/mnt/data/runs/2026-10-10_sports_wallet_adoption_v1`.
Flags, samples, estimates, comparison and report use fresh child stage directories;
completed children are never reused. Technical fingerprints belong in manifests.
Local task journal: `output/wallet_downstream_adoption_2026-10-10_v1/index.json`.

Only root owns production, transfers and EC2 lifecycle. Subagents implement or
independently review bounded stages without managing infrastructure. Root must
confirm no remaining dependency, stop and verify stopped after completion,
failure or interruption. Identity/filter adoption is not whole-data certification.
