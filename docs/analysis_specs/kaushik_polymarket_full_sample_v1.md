# Kaushik Polymarket replication: full available-date sample v1

**Status:** frozen execution contract, 10 October 2026. The user has confirmed
removing the March 25 execution cutoff and rerunning the full writeup with every
other scientific definition unchanged. No full-sample production stage,
estimation, audit or report has yet been executed or accepted. This document
contains no findings and does not certify native actions, exact execution
timestamps or whole-history collection completeness.

## Contract and sole scientific change

The [accepted cutoff specification](kaushik_polymarket_replication_v1.md) is the
scientific contract for this run, except for its common execution-date scope.
The [methods reference](../methods_reference.md) remains authoritative for the
data canon and censoring qualifications. Preserve the original specification,
accepted stages and report as the cutoff replication; do not rewrite them as
full-sample results.

The question is whether the same record-weighted price profile, duration
associations and provider-covered sports profiles appear when every available
execution date in the admitted source is eligible. The new input contract must
explicitly select **`sample_scope: all_available_dates`**. The supported scopes
are `paper_cutoff` and `all_available_dates`; legacy absence of `sample_scope`
means the accepted `paper_cutoff` behavior. Full-date cutoff fields are explicitly
null. This scope removes
`t < 2026-03-25T00:00:00Z`; it introduces no lower or replacement upper execution
bound. Valid UTC timestamps and the source-month consistency gate still apply.
Save the observed first and last execution timestamps rather than treating a
month partition boundary as the last observed execution.

Full-date `scope_rows` counts all rows in the admitted source, including invalid records
that subsequently receive an eligibility exclusion. It must not be a count of
valid timestamps alone. Cutoff-relative future-ending diagnostics are null and
labeled not applicable in this mode, never silently calculated at March 25 or
displayed as zero.

Everything else in the original specification is unchanged:

- Primary observations are archive-recorded taker-role rows, preserving physical
  multiplicity. BUY retains its recorded claim; SELL uses the verified binary
  complement and unrounded normalized binary64 price and payout.
- Eligibility still requires finite `0<P<1`, positive finite size, admitted
  binary-token metadata and observed eventual payout. Retain flagged wallets,
  up/down markets and every eligible topic. Add no bot, price-trim, lifecycle,
  Kalshi-match or baseline sports-only filter.
- Weight each eligible record equally. Keep payoff `100*(Y-P)` and individual
  return `100*(Y/P-1)` separate, with individual return computed before averaging.
  Fixed ten-cent bins, signed D10-minus-D1 contrasts, gross amounts and absence
  of fees/annualization remain unchanged.
- Retain native categories plus `Unclassified`, exact normalized price effects,
  UTC execution-month effects, native `created_at`/`end_date` duration proxies,
  duration inequalities, all model columns and the claim-FE diagnostic.
- Retain joint event/market-cluster CR0 covariance, supplementary `G/(G-1)`
  adjustment, influence diagnostics, rank/convergence gates and numerical
  withholding. Sports keep game clustering, every specified phase/window edge,
  and both the 30-game rule and project 500-record guard.
- Regenerate the full table/figure contract, including every expected pooled
  and sport-specific row and all Appendix A2 archive BUY conventions. No new
  model, sensitivity, weighting scheme or native BUY source is introduced.

## Source inventory and sports boundary

Bind the same repaired CLEAN source generation and its complete 44-month
inventory, November 2022 through June 2026, under
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/`.
Reopen and fingerprint the repair manifest, every monthly archive and the same
token map, token/outcome spine, native market metadata and native category map
used by accepted inputs v2. No resolution refresh, source replacement, new
archive collection or shared-path redirection enters this date-scope rerun.

Reference accepted cutoff inputs:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2`.
The [reproduction guide](kaushik_polymarket_replication_reproduce.md) records their
source and artifact bindings; fresh controls must bind and reopen those exact
immutable inputs for the subset comparison.

Retain the existing audited nine-sport provider inputs enumerated in
`/mnt/data/runs/2026-10-10_kaushik_polymarket_controls_v1/sports_binding_reviewed_v5.json`.
The new sports metadata stage must bind the new full-sample base and revalidate
the same retained identity, native binary-pair, result and timing proof. No new
API collection or provider game expansion is part of this request.

Removing the execution cutoff does not remove inherited provider coverage
limits. Retained NBA clocks end in October 2025, CFB in January 2026 and NFL in
February 2026; other retained sports have coverage extending into April--June
2026. Reopen and report each sport's actual admitted game-date range and audit
its retained candidate/provider exclusions. Label the sports estimand as all
available execution dates within the **retained audited provider-covered
cohort**, never a full-history exchange game census. Retain EPL draw claims and
the ATP scheduled-start/archive-duration timing qualification.

The archive's approximate execution clocks, closed/resolved source vintage and
omission of unresolved long-horizon claims remain material. Removing a cutoff
does not establish exchange-wide completeness or justify recency/trend claims.
Record the retained resolution-refresh date and observed source execution range.
The inherited cutoff-relative future-ending diagnostic is not applicable without
an execution cutoff; do not substitute a new scientific diagnostic or silently
retain March 25 as a full-sample coverage endpoint.

## Fresh admission and reconciliation gates

All production runs require the guarded canonical EC2 checkout, mounted
`/mnt/data` and `/home/ubuntu/venv/bin/python`. Root owns lifecycle, source commits
and publication. Stages are serial unless independence is demonstrated. Publish
new immutable input, sports metadata, estimation and audit directories; preserve
all accepted cutoff artifacts and any failed full-sample stages.

Before a production body runs, commit and test the explicit date mode. Bind a
fresh separately reviewed preflight to the committed source, input digests,
commands and resource limits. The accepted cutoff run's capacity admission does
not establish capacity for the expanded sample. Reassess read, cache, NumPy,
DuckDB, spill, per-file, total-output and free-space ceilings without silently
raising or bypassing them.

Require these factual gates before estimation and release:

1. Reopen schema, types, counts, digests and source-month inventory; preserve
   archive source locators and multiplicity. Binary complement reciprocity,
   outcome agreement, category assignment and event/market mapping must pass
   without join fanout. Audit all source rows through disjoint first-failure
   exclusions under the new date mode.
2. Reconcile the new accepted baseline restricted to `t <
   2026-03-25T00:00:00Z` against accepted inputs v2 by source month plus original
   physical row ordinal. Require exact membership and analytic-field agreement:
   recorded and normalized claim identity, timestamp, recorded/normalized price,
   payout, gross size, role/action and duration eligibility. Reopen their metadata
   lineage, categories, cluster keys and duration clocks as well; dictionary code
   renumbering must not conceal changed identities. The full eligible population
   must partition into this retained subset and newly admitted later records.
   The guarded `scripts/audit_kaushik_full_sample_subset.py` performs the
   independent comparison serially by month on accepted base bodies, using a
   bidirectional `EXCEPT ALL` oracle. It reads no raw archive trade bodies.
3. Validate the new sports map against unchanged provider inputs and the new
   base binding. Require distinct double `sport_elapsed_seconds` and
   `sport_remaining_seconds`, their saved column mapping, and zero post-cache
   clock-equation mismatches. Reconcile pregame/in-play/after-end partitions and
   exact start/end/window boundaries; retain audited timing exclusions.
4. Reopen all estimated outputs. Reconcile common duration samples across
   columns/outcomes, panel populations, archive BUY-role partitions, exact-price
   cardinality, rank/convergence diagnostics, saved joint scores and complete
   support/suppression grids. No unsupported estimate may become a plotted zero.
5. Run the independent bounded saved-score audit and require
   `saved_scores_reconciled`. Reconstruct covariance, contrasts, intervals,
   influence and saved-support identities. State that this audit does not
   independently fit raw coefficients or certify raw window membership, native
   actions or whole-history collection completeness.

Synthetic tests must cover the second before, equality at, and records after the
old cutoff in both date modes, malformed/inconsistent mode definitions, full-date
waterfall accounting, retained duration rules and report admission. Keep the
existing complement, binary64 bin, covariance, fixed-effect, support and corrected
sports-clock fixtures. Run focused tests first, then the relevant broader suite,
and record exact commands/results.

## Full writeup and completion

The renderer reads accepted computed summaries only and performs no estimation.
Its admission, date prose and eligibility-waterfall labels must reflect the
explicit full-date mode. Preserve all original table/figure ordering,
interpretation qualifications and distinctions among payoff, return, duration
associations and claim-FE price-path diagnostics.

Use the [data-first reporting skill](../../.agents/skills/data-first-reporting/SKILL.md)
for the portable LaTeX source, native tables and saved vector figures. Reconcile
every displayed value, support count, withholding reason, influence mark and
plotted CR0 interval to the accepted summary. Compile the complete source/assets
project, review substantive warnings, and inspect every page for layout and
coverage. Discrete bins/windows remain isolated points with capped whiskers and
zero references; hashes stay in manifests.

Open the finished source and useful verified preview in Codex. Completed-results
delivery must explain the research question, source/cohort/date rules, estimands,
weights, uncertainty, major findings and remaining material uncertainty in chat.
Root publishes only the finished reviewed bundle, verifies transfer, confirms no
agent/process/transfer still needs EC2, stops the instance and verifies stopped.
