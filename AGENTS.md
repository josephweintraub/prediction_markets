# Project operating guide

## Sources of truth

- This repository studies prediction-market price calibration and favorite–longshot bias (FLB), primarily with Polymarket trade data.
- Read `docs/methods_reference.md` before designing or interpreting an analysis. It is authoritative for the data canon, filters, calibration definitions, uncertainty, and censoring caveats.
- Treat `docs/archive/` as historical only. The canonical GitHub checkout is `/home/ubuntu/prediction_markets` on EC2; the local Mac copy is for editing, review, small tests, and summary-artifact checks.

## Working principles

- **Ask when the answer matters.** Ask before acting when ambiguity would materially change the research question, architecture, result, external state, or authorization. Otherwise, make the smallest reasonable assumption, state it when relevant, and continue with routine reversible work.
- **Simplest defensible approach first.** Add abstraction, flexibility, or statistical complexity only when the straightforward version proves insufficient.
- **Keep scope tight.** Do not modify unrelated files or clean up adjacent code unless the user expands the task.
- **Flag uncertainty.** Separate verified facts, inferences, assumptions, and unresolved questions. Fail closed when uncertain data could silently change a result.
- Preserve user changes. Prefer immutable outputs, explicit gates, and atomic publication; never silently overwrite a completed run.

## Delegation and infrastructure

- At the start of every user task, before substantive execution, delegate at least one concrete, bounded subtask. Use one agent for a single scope; add agents only for genuinely independent workstreams or independent QA.
- The root agent remains orchestrator and integrator. Keep it focused on scoping, contracts, coordination, review, integration, and final communication rather than duplicating delegated work.
- Give agents explicit inputs, outputs, file boundaries, tests, and prohibited actions. Pass only the context they need, and require concise evidence, test results, and artifact paths in return. Delegate independent implementation or audit work, not vague assistance.
- Subagents must never start, stop, mount, unmount, reboot, or otherwise manage the shared EC2 instance or storage. Only the root may manage lifecycle, after confirming no active agent or process still needs it. Subagents may use an already-running instance only when explicitly assigned that workload.
- If the root starts or uses EC2 for a task, then after success, failure, or interruption it must safely finish any required data handling, confirm that no agent, process, notebook, or transfer still needs the instance, stop it, and verify that it reaches the stopped state.
- Serialize production stages that share inputs or caches unless independence is demonstrated.

## Project skills

- Use [`prediction-market-research`](.agents/skills/prediction-market-research/SKILL.md) for literature, research-design, empirical-method, and interpretation tasks.
- Use [`data-first-reporting`](.agents/skills/data-first-reporting/SKILL.md) whenever producing an analysis report.
- If a required skill is unavailable, say so briefly and follow the corresponding rules here rather than inventing a substitute workflow.

## Compute and data

- Full-universe joins, trade scans, calibration runs, API collections, LLM batches, and other production work run on EC2. Local work is limited to code/docs, tests, and genuinely small metadata or summary checks.
- Smaller collection/API request, batch or shard sizes are preauthorized whenever needed to complete an authorized task; do not ask again for the size reduction itself. Preserve full coverage, validation gates, immutable receipts and cumulative resource ceilings. Keep active canonical source frozen, applying any required source changes only after owned workers exit through a reviewed, versioned continuation. This permission does not authorize higher limits or changes to the sample, filters or source scope.
- Keep routine local discovery inside explicit source directories and honor `.ignore` and `.gitignore`. Use `rg --max-filesize 1M` for broad text searches; never include generated data/output directories or disable ignore rules for repository-wide discovery. Inspect a known summary artifact explicitly only after checking its file size.
- Production entrypoints use `production_guard.require_production_host()`: Linux, the canonical `/home/ubuntu/prediction_markets` checkout, and mounted `/mnt/data` are required before production work. Keep local fixture tests and report-only rendering separate from production execution; do not bypass the guard to run real data locally.
- The Mac folder has its own local Git repository to keep Git discovery out of the home directory. It has no configured upstream or historical baseline; do not bulk-commit its existing contents. Preserve the home repository and synchronize reviewed source edits into the canonical EC2 checkout for production commits and pushes.
- Local and EC2 trade samples use different market-key conventions; local full-data joins can silently mis-key. Follow `docs/methods_reference.md` and its documented token spine.
- Use `/home/ubuntu/venv/bin/python` for production Python. Data lives under `/mnt/data` and remains gitignored. Never expose credentials or API keys.
- Use lazy DuckDB views over Parquet for large data. Do not load the full trade set into pandas; clear Parquet-backed relations with `DROP VIEW IF EXISTS`, not `DROP TABLE`.
- Exact EC2 lifecycle, mount, monitoring, and transfer instructions live in `docs/EC2_SETUP.md`.

## Reproducible analysis

- Final findings come from a committed script and saved Parquet/JSON summary, not hand-transcribed ad hoc queries. Promote exploratory queries into a script before reporting.
- Use immutable, versioned run/stage directories and manifests recording inputs, outputs, contracts, commands, environment, counts, fingerprints, and reconciliation gates. Keep technical provenance in manifests and run records.
- Validate schema, grain, uniqueness, coverage, lineage, timing, and suppression before estimation. Preserve exclusions and irregular records for audit instead of guessing.
- Apply the resolution-censoring caveat to end-of-sample and across-time comparisons. Do not revive archived claims without new evidence.
- Run focused tests first, then the relevant broader suite; report exact results and remaining uncertainty.

## Reader-facing reports

- Write for research professionals and default to LaTeX formatting: tables, plots, and charts carry the result, while prose is limited to necessary definitions, material findings, and non-obvious limitations.
- Accompany every completed-results delivery with a chat explanation of the research question, data/cohort/filter/timing choices, methodology, estimand, weights, major findings, and material remaining uncertainty. Define unfamiliar terms there; keep this explanatory coverage separate from the concise, data-first LaTeX report.
- Make portable `.tex` source the primary report deliverable from computed artifacts; compilation is QA and preview, not the primary artifact. Open finished source in Codex, and open compiled previews there when useful—never in an external browser or viewer.
- Use native LaTeX tables, adding only needed packages (typically `booktabs`, `siunitx`, and `threeparttable`; `longtable` only for a useful multipage table). Include saved deterministic figures with `graphicx`, preferably as vector PDFs, and keep the result print-safe without a template framework.
- Reports may and generally should end with the final evidence table or figure. Never add a generic standalone `Interpretation limits`, `Caveats`, `Discussion`, conclusion, or recap page or section, and do not explain elementary statistical conventions. Put any non-obvious limitation that materially affects a result in one compact note beside that table or figure, or in the minimum opening definitions when it applies throughout.
- Omit reader-facing fingerprint/hash appendices unless explicitly requested; fingerprints belong in the manifest.
- Fixed probability-bin/decile estimates are discrete: show isolated points, capped confidence-interval whiskers, and a zero reference where applicable. Never connect bins with lines or plot suppressed estimates at zero.
- Label fixed-width probability bins as bins, not sample quantiles. Show support and suppression status; avoid significance claims when only nominal intervals are available.
- Keep calibration and closing concepts distinct. Do not call closing calibration “CLV” unless the analysis actually measures closing-line value.

## Repository and documentation

- `analysis/` holds research; `pipeline/` rebuilds trade data; `scripts/` holds reproducible builds/audits; `docs/` holds current guidance. Finished shared deliverables go to Dropbox; drafts and scratch files do not.
- Commit logical units with imperative subjects and a body explaining why. Push canonical changes from the EC2 checkout; use branches for risky exploration and merge promptly once it becomes the direction.
- Record notable analysis/data/infrastructure changes in `CHANGELOG.md` during the same session, newest first with absolute dates. Findings belong in analysis docs, not the changelog.
- Move superseded docs to `docs/archive/` with a status header, repair live references, and never commit data, credentials, private keys, or generated production artifacts.
