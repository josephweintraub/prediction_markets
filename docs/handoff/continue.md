# Continuing the research

1. Choose the study in the [catalog](study_catalog.json). State the requested change
   and whether it changes the sample, estimand, timing, weights or uncertainty.
2. Follow [AGENTS.md](../../AGENTS.md) and the [workflow](../workflow.md). Delegate a
   bounded task first; only the root manages shared infrastructure.
3. Keep production modules at their current import paths. Put venue-specific
   contracts in that venue's module, shared sports behavior in the shared sports
   module, and entrypoints/audits in `scripts/`. Avoid a framework or a new wrapper
   when the existing function is sufficient.
4. Make the smallest source change and add focused synthetic tests. Read the
   selected run's exact input/acceptance contract before running real data.
5. Create a fresh dated immutable run. Preserve rejected rows, failed attempts and
   separate producer/independent-audit receipts. Never revise a completed estimate
   merely because a new source or filter is available.
6. Render data-first portable LaTeX from accepted artifacts, with discrete bin
   points and support rules. Deliver the methodological/result explanation in chat.
7. Commit reviewed source/docs from the canonical checkout. Add the new study or
   version to this catalog, package only finished explicitly selected files, and
   verify private Dropbox readback before claiming delivery.
8. After all workers and transfers release the instance, stop and verify its state.

The current handoff is a source-and-summary release. It does not merge every past
exploration into a single approved scientific specification. Historical outputs
remain discoverable but should not become the next analysis's defaults by accident.
