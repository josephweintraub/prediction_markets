---
name: data-first-reporting
description: Create, revise, or QA concise professional-audience LaTeX reports for completed quantitative analyses, with native tables and uncertainty-aware figures carrying the evidence. Use for research-results reports, not exploratory notebooks, pipeline logs, or manuscript prose without a report artifact.
---

# Data-first reporting

Follow the user's request and the analysis's frozen definitions first. Present audited outputs faithfully; do not change an estimator, infer unavailable results, or turn suppressed estimates into values.

## Build the evidence hierarchy

Order the report for reading, not for pipeline execution:

1. Title and a one-sentence scope statement.
2. Only the definitions needed to interpret the evidence: sample, weighting, uncertainty, binning, and suppression.
3. The most informative overview table or figure.
4. Result sections in a stable comparison order. Within each section, prefer figure, compact contrast table, then complete supporting table.
5. The final evidence table or figure; no generic closing section is required.

Let tables, plots, and charts carry the result. Use prose only to define quantities, state material caveats, and summarize findings supported by the displayed evidence. Keep the complete profile primary when a contrast summarizes only part of it.

Write for research professionals. Do not explain elementary statistical conventions. Reports may and generally should end with their final evidence table or figure; never add a generic standalone `Interpretation limits`, `Caveats`, `Discussion`, conclusion, or recap page or section. If a non-obvious limitation materially affects a result, put one compact note beside the affected table or figure, or in the minimum opening definitions when it applies throughout.

## Draw statistical figures

- Treat fixed probability bins and deciles as discrete categories. Plot isolated point estimates with capped uncertainty whiskers; never connect them with lines.
- Show a zero or other reference line when it has a defined interpretation. Label the interval type and bin convention.
- Preserve left-to-right bin order and consistent axes across directly comparable panels.
- Distinguish series with both color and marker shape when practical. Use a restrained, colorblind-readable palette.
- Omit suppressed estimates from the plotted marks. Do not place them at zero; retain their support and suppression reason in the table.
- Do not add significance stars, p-values, slopes, smoothing, or trend claims unless the approved analysis provides them.

## Format evidence tables

- Use short, reader-facing labels instead of internal column names. Define abbreviations and non-obvious signs in a note.
- Separate materially different samples or definitions when a mixed scope column would make comparison harder.
- Show the relevant support, estimate, uncertainty interval, and status. Include descriptive volume or dollars only when useful and label them as descriptive.
- Use one precision convention per quantity: counts with separators, probabilities and signed effects with consistent decimals, and intervals at the estimate's precision. Preserve meaningful source precision; do not imply more.
- Render withheld results as `withheld` with an explicit reason, never as zero or a blank that looks accidental.
- Right-align numeric cells and use tabular numerals. Prefer booktabs-style horizontal rules; avoid vertical rules and dense cell borders.
- Put weighting, interval construction, sample filters, and suppression thresholds in compact table notes rather than repeating them in prose.

## Produce durable LaTeX

- Make the portable `.tex` source the primary deliverable. Compile it only to verify and preview the result; the compiled PDF does not replace the source artifact.
- Start from a standard document class and the smallest stable package set the report needs. Prefer `graphicx`, `booktabs`, `siunitx`, and `threeparttable`; add `longtable` only for a genuinely multipage table. Do not introduce a template framework.
- Build tables as native LaTeX, not screenshots or pasted renderer output. Use `booktabs` rules, `siunitx` alignment, and compact `threeparttable` notes when useful.
- Generate figures deterministically from saved analysis artifacts and include them with `graphicx`. Prefer vector PDF figures; use raster output only when the figure cannot reasonably remain vector.
- Keep source and figure references portable and relative. Avoid remote fonts, scripts, images, or other network dependencies.
- Use a restrained academic visual system with clear hierarchy, ample white space, legible table and figure text, and no dashboard decoration.
- Keep tables and figures within the printable area without shrinking text into illegibility. Use multipage tables only when necessary, repeat their headers, avoid stranded captions, and prevent atomic numbers, intervals, and short status tokens from splitting.
- Compile in a clean build location, treat missing assets and substantive LaTeX warnings as failures, and inspect every preview page for clipping, overflow, blank pages, broken tokens, repeated headers, and clean section breaks.

## Keep provenance out of the reading flow

- Reconcile every displayed number, support count, label, and suppression status to the saved source artifacts before release.
- Keep source paths, hashes, environment details, and full fingerprints in manifests or run records. Do not add a reader-facing fingerprint or hash appendix unless the user explicitly requests it.
- Put a concise source or method limitation beside the affected evidence when it materially changes interpretation; use the minimum opening definitions only when it applies throughout.

## Deliver and verify

- Confirm that all expected panels, rows, and definitions appear and that comparable sections use the same labels, order, and visual grammar.
- Confirm that fixed-bin plots contain no point-connecting paths and suppressed estimates are not plotted.
- Confirm that the source has no remote dependencies and the compiled preview matches it numerically and semantically.
- Open the finished `.tex` source in the Codex app's native file panel. Open the compiled preview there when useful; never launch an external browser or viewer with `open`, `xdg-open`, `start`, `osascript`, or Python's `webbrowser`. If native Codex opening is unavailable, provide the absolute path instead.
