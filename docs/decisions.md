# Decision log

This file records decisions that change the interpretation or reproducibility of the
project. Newest entries come first. Findings belong in `project_status.md` or a report;
implementation changes also belong in `CHANGELOG.md`.

## 2026-09-06: stabilize the repository before extending the analysis

**Decision:** Pause new empirical analysis while the repository, workflow, inference
tests, and artifact lineage are made publication-ready.

**Reason:** The research question has converged, but active and superseded analyses and
multiple artifact vintages remain intermingled. Additional results would deepen that debt.

**Consequences:** Work proceeds on `codex/repository-cleanup` from safety tag
`pre-cleanup-2026-09-06`. The first pass is documentation and guardrails; analytical code
moves only after validation coverage exists.

## 2026-08-27: use the refreshed single data vintage

**Decision:** Rebuild trade-side liquidity and maturity artifacts using the refreshed
2,036,128,538-row trade set and rebuilt wallet flags.

**Reason:** Pre-refresh and post-refresh inputs had been mixed. The new bot flags were the
largest cause of changes to the filtered sample.

**Consequences:** Sessions 1-6 are historical. Current full-window liquidity and maturity
results use the refreshed vintage. Embedding-derived features still require a refresh.

## 2026-08-27: use volume rate for the primary liquidity axis

**Decision:** Define liquidity as standard-filtered BUY dollars divided by market-life
days, with a one-hour duration floor. Retain first-1-day, first-7-day, and first-30-day
volume as robustness definitions.

**Reason:** Total volume mechanically accumulates with market duration and confounds the
liquidity and maturity hypotheses.

**Consequences:** Total-volume tiers are historical diagnostics, not the primary liquidity
specification.

## 2026-08-24: make the decile profile primary

**Decision:** Treat the full ten-decile calibration profile as primary. Summarize with D1,
D10, and D10-D1; retain the signed slope as auxiliary.

**Reason:** A fitted slope compresses the profile and can extrapolate through unpopulated
price regions. Tail-resolved estimates identify whether an apparent effect is truly
two-sided.

**Consequences:** Classic FLB requires D1 < 0 and D10 > 0. Earlier slope-primary language
and interpretations are superseded.

## 2026-07-03: use a canonical market-level exclusion spine

**Decision:** Join trades to `market_flags.parquet` by token ID for resolution and
market-level up/down exclusion.

**Reason:** The former resolution spine covered only about half of the extended trade set,
and newer trade rows had empty event slugs that defeated trade-level up/down filtering.

**Consequences:** Analyses using only the retired resolution spine or trade `eventSlug`
filter are not comparable with the current specification.
