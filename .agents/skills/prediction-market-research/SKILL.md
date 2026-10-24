---
name: prediction-market-research
description: Design, implement, audit, and report empirical prediction-market analyses and focused academic evidence reviews in this repository. Use for calibration, favorite-longshot bias, closing-benchmark or CLV, cohort, timing, sensitivity, and project literature questions; not for generic web research or exhaustive deep research unless explicitly requested.
---

# Empirical prediction-market research

Honor the user's study definitions. Ask before implementation only when an unresolved choice would materially change the estimand, cohort, or interpretation.

## Start from the research contract

Before material computation or implementation, state the smallest analysis contract that can answer the question:

- Research question and hypothesis. Keep the direction neutral unless theory or a prior protocol specifies it; label post-result hypotheses exploratory.
- Estimand and observation unit: trade, market, event, wallet, or time interval. State the weighting and uncertainty unit separately.
- Cohort: source vintage, inclusion and exclusion rules, resolution requirement, and treatment of missing or ambiguous records.
- Time: timestamp source and timezone, event boundaries, interval inequalities, lifecycle or phase definitions, and closing-price convention.
- Primary output and the minimum sensitivity that addresses a specific plausible failure mode.

Read [the methods reference](../../../docs/methods_reference.md) before analysis. Read the relevant analysis spec or runbook when one exists. Use [the EC2 setup guide](../../../docs/EC2_SETUP.md) for data-location and compute rules rather than copying infrastructure instructions into the skill. This skill does not authorize EC2 lifecycle actions.

## Keep estimands distinct

- Calibration is descriptive outcome-minus-price, `Y - P`, at the stated unit and weighting.
- A closing benchmark is separate. If calling it CLV, freeze whether the close is the last valid fill, quote, or midpoint before the event and state the sign convention. Do not describe closing-price-minus-trade-price as calibration.
- Realized return, forecast accuracy, Brier score, and FLB tail contrasts answer different questions. Name each measure and avoid substituting one for another.
- For fixed probability bins, use the project's frozen edges and show the full profile with support. Treat a tail spread as a summary of that profile, not standalone proof of FLB.

## Prefer the simplest informative analysis

Begin with cohort reconciliation, coverage, exclusions, and direct descriptive tables or fixed-bin calibration profiles. Add stratification, alternate weighting, or modeling only when the simpler result exposes a concrete ambiguity that the extension can resolve. Preserve the original primary estimand when adding sensitivity analyses.

Run sensitivities one design choice at a time when practical. Pre-state the failure mode each check targets, keep the primary result visible, and report whether the conclusion changes. Suppress or explicitly label underpowered cells according to the active protocol; do not interpret unstable tails or sparse clusters as precise effects.

## Source and provenance discipline

Use the most direct reproducible source available: immutable on-chain or native market data for trades and outcomes; authoritative first-party event data for external timing or results; documented secondary sources only for cross-checks or when the limitation is explicit. Fail closed on contradictory identity, outcome, or timing evidence.

Machine-facing manifests and run records should contain input paths and hashes, source vintage, schema or contract version, exact command, exclusions and reconciliation counts, output hashes, and completion status. Reader-facing reports should explain the source and method but omit fingerprint/hash appendices unless explicitly requested.

## Focused literature evidence

Keep literature work bounded to the evidence needed to motivate, design, or interpret the project's analysis. Do not default to an exhaustive review; if the user explicitly requests deep research, use the dedicated global workflow.

- Prefer primary papers for empirical or theoretical claims and official sources for data definitions or institutional facts. Use secondary sources to locate or contextualize primary evidence, not to replace it when the original is available.
- Record the databases or search channels, query concepts, search date, and substantive inclusion or exclusion rules needed to make the bounded search transparent. State important coverage limitations.
- Place citations beside the claims they support and distinguish direct source findings from your inference.
- Separate established or contested evidence from hypotheses proposed for this project. Do not present a literature-consistent explanation as a confirmed result or pre-specified hypothesis.

## Reproducibility and QA

- Promote exploratory queries into a deterministic script before finalizing findings. A renderer reads computed artifacts; it does not estimate results.
- Publish immutable outputs through the project's staged, no-overwrite pattern when the active workflow requires it.
- Reopen outputs and verify schemas, types, uniqueness, row counts, joins, boundary inequalities, and input-to-output reconciliation.
- Test formulas and boundary behavior on small synthetic data. Independently recompute headline aggregates from the published inputs when risk warrants it.
- Treat missing authoritative timestamps, ambiguous market mappings, or irreconcilable outcomes as exclusions or blockers rather than relaxing gates without evidence.

## Reporting and citations

Write for research professionals: lead with the estimand, cohort, support, point estimate, and uncertainty without tutorial explanations, and place any non-obvious material limitation beside the affected result instead of adding a generic final caveat, discussion, conclusion, or recap section.

Cite primary papers, official data definitions, and the specific computed artifact or table supporting each important claim. Put citations beside the claim, keep the bibliography concise, and never cite a search-results page. Do not overstate precision beyond the published display or invent significance from a confidence interval that crosses zero.
