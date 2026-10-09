# MLB pre-filter exact extract and sample repair

**Status:** production body and independent saved-artifact QA completed and
transferred; local count report rendered, compiled and visually checked. Root's
source-release and final lifecycle receipts are retained separately in the local
stage journal `output/mlb_unfiltered_rebuild_2026-10-09_v1/index.json`.
**Authorization:** user approved the proposed MLB extract rebuild on 9 October 2026.

## Scope

Recover MLB candidate fills excluded by the historical extract's upstream price
and inferred-buyer bot filters. Preserve the existing accepted market cohort,
recorded outcomes, official event boundaries, exact block timestamp source and
legacy inferred-BUY normalization. Create a new immutable pre-filter extract and
derive filtered and all-trades samples separately. Do not overwrite historical
artifacts or rerun other sports or scientific estimators in this stage.

The all-trades sample retains flagged inferred buyers and requires `0 < P < 1`.
The filtered sample requires `0.01 < P < 0.99` and excludes buyers flagged by the
frozen shared pipeline wallet-flags artifact. Both exclude post-end trades and
retain all pregame history without a lower time cutoff. Timestamps are exact cached
UTC Unix seconds; recorded event-end equality is included. The old
learnability-cache flags are not substituted
for the shared flags used by the latest report contract.

The unit remains the legacy inferred outcome-token BUY per resolved fill, not a
certified counterparty own economic action. This is an analytic-input coverage
repair from retained resolved data, not evidence of missing source collection.
No wallet/native-action repair, flag reclassification or scientific estimator rerun
was performed. Resolution censoring and broader canon uncertainty remain.

## Inputs and reconciliation

- Resolved fills: `/mnt/data/pipeline_data/resolved_trades.parquet`.
- Exact cache: `/mnt/data/pipeline_data/block_timestamps.parquet`, validated against
  `configs/data_vintages/mlb_exact_timestamps_2026-07-04.json`.
- Candidate markets: `/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/01_market_universe/candidate_markets.parquet`.
- Old exact extract: `/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/02_exact_trades/exact_trades.parquet`.
- Frozen accepted metadata: `/mnt/data/runs/2026-09-07_mlb-game-dynamics_preestimation-audit-v1/05_phase_dataset_v3/phase_trades.parquet`.
- Sample flags: `/mnt/data/pipeline_data/wallet_flags.parquet`.
- New production destination: `/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1`.

Require immutable fill identity and payload reconciliation, zero missing exact
timestamps, unique join keys, explicit exclusion accounting, a filtered subset of
all trades, and unchanged payloads for every historical exact row. Replaying the
old extract through the current cohort and flag definitions checks historical
sample counts; those counts are not targets for the repaired samples.

## Completed body and row reconciliation

The one-shot durable body ran from `2026-10-09T22:14:06Z` to
`2026-10-09T22:28:13Z` and published the new immutable `01_samples_v1` stage.
All nine producer reconciliation gates passed. The source HEAD and all seven
input stats and complete SHA256 fingerprints agreed before and after the body.
Each written Parquet was reopened for full multiset payload equality, native fill
identity uniqueness and exact timestamp equality. All 2,188,688 distinct source
blocks were matched, with zero missing blocks or output timestamp mismatches.

The 3,790 candidate markets yield 6,912,624 distinct fills and the same number of
pre-filter inferred BUY rows. There were zero exact-payload replay rows. Distinct
native fill IDs were retained; equal economics alone was not a deduplication
criterion. The old 2,555,139-row exact artifact is an exact, null-aware, full-14-column
payload subset of this new extract. Old all/filtered samples are full-payload
subsets of the corresponding rebuilt samples; their legacy loader counts reproduce
exactly. The filtered sample is also a full-payload subset of the all sample.

| Accepted sample | Old rows | Rebuilt rows | Restored rows |
| --- | ---: | ---: | ---: |
| All trades | 2,502,803 | 6,593,973 | 4,091,170 |
| Filtered trades | 2,412,918 | 3,477,565 | 1,064,647 |

The phase-derived eligible cohort is unchanged at 3,696 markets and 3,696 MLB
events. Each of the four old/new all/filtered samples represents all 3,696 markets
and events. These are eligible/represented support counts, not new games added by
the repair. The old “all trades” branch had already inherited upstream price/bot
exclusions from its exact artifact.

Sequential exclusions are disjoint: first exclude markets outside the frozen
accepted metadata, then post-end fills within accepted markets, then invalid
binary sample prices. From valid all-sample rows, apply the interior-price rule
before excluding currently flagged interior-price buyers.

```text
6,912,624 pre-filter rows = 118,285 outside accepted markets
                        + 200,366 after recorded game end
                        +       0 invalid sample prices
                        + 6,593,973 accepted valid-price all rows
6,593,973 all rows       = 101,550 interior-price rule exclusions
                        + 3,014,858 flagged interior-price exclusions
                        + 3,477,565 filtered rows
2,502,803 old all        + 4,091,170 restored = 6,593,973 rebuilt all
2,412,918 old filtered   + 1,064,647 restored = 3,477,565 rebuilt filtered
```

The all-sample support includes 1,320,466 rows whose inferred buyer has no shared
flag record and zero rows with a present but null `is_nonhuman`. Missing/null flags
retain the existing `COALESCE(is_nonhuman,false)` rule, rather than being silently
dropped or reclassified. Missing labels pass the flag criterion but still face the
sample's price/time rules; their human/nonhuman classification is not certified.

The new `01_samples_v1/exact_trades.parquet` preserves the original ordered
14-column schema and is compatible with a future estimator `--mlb-exact` replacement.
The enriched all/filtered files are audit/analysis inputs, not drop-in arguments to
the existing estimator. Fixed-duration time is not exported: an authorized future
run must retain the unchanged loader's sport median duration among unique events
represented in each sample. No FLB, calibration, regression, weighting or
uncertainty estimates were rerun at this stage.

## Published artifacts and local evidence

Production output base:
`/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/01_samples_v1/`.
The following paths are relative to that immutable base; hashes are from the
verified transferred build manifest, not fresh local trade scans.

| Artifact | Rows | Bytes | SHA256 |
| --- | ---: | ---: | --- |
| `exact_trades.parquet` | 6,912,624 | 447,083,422 | `9a5cc486c7127ec21e0f9c3a867d797d6d0d8b7a518c57f7ecd32ff92e29ebd9` |
| `all_trades.parquet` | 6,593,973 | 506,377,500 | `9c422fb94960777e6259d1323ad56584d5d26f3f128005434109c59cff493d05` |
| `filtered_trades.parquet` | 3,477,565 | 265,440,238 | `238cd77ea93d8b5c2bd8ddf86119789b6e4790502034d800dc7499dd96d57023` |
| `summary.json` | — | 24,338 | `a66c392ef8e8a61afa2c37aa654f4933324014669b2e679eead9856772267a8b` |
| `manifest.json` | — | 24,496 | `d26b516b6e2a63f16b6a0256ec694f9bdb632a251d37e385452cc32bb820142b` |

Local compact evidence is under
`output/mlb_unfiltered_rebuild_2026-10-09_v1/`: `manifest.json`, `summary.json`,
`index.json`, and `body_logs_v1/`. The complete command is bound in the manifest and
the GNU time profile. Its reviewed preflight is
`/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/00_preflight_v1/summary.json`,
SHA256 `e9d44a3147d262b6745984698184502a330c7db8f6550a592c0e4eb6d3ad9afb`.
The root-prepared one-shot runner is `runner_v1/run_body_v1.sh`, SHA256
`483b34acfa52b68974af8e17591ee990de3400325e1d57930a839787ffb43c42`.

| Local body log | Bytes | SHA256 |
| --- | ---: | --- |
| `body_logs_v1/resource_usage.txt` | 1,793 | `31bd88696588bfd25c739c3c27874361ea702c8a36b44d23141ca1d096b721ff` |
| `body_logs_v1/stdout.log` | 151 | `6da67c3c90960cb895b6ed8aeb88f1326026b6a090ba061293c1e0c1b3b71fac` |
| `body_logs_v1/stderr.log` | 1,639 | `928753b7c8f4c37fc1ca9a093d5ba725a818594367f898d9876b4bc333ae02ee` |
| `body_logs_v1/exit_status.txt` | 2 | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` |

## Completed independent saved-artifact QA

The separately authorized checker ran from `2026-10-09T22:30:54Z` to
`2026-10-09T22:31:19Z`, completed with exit 0, and independently matched all 30
saved count/support fields. Its eight gates passed: native-ID uniqueness, full
payload membership, exact cache, metadata/flag uniqueness, winner/token mapping,
sample membership, sample enrichment and attrition. It reopened saved outputs and
the small frozen cohort/flag/cache/old inputs without another resolved-source query.
The same frozen source HEAD was clean before and after QA.

Production receipt:
`/mnt/data/runs/2026-10-09_mlb_unfiltered_rebuild_v1/02_independent_qa_v1/receipt.json`.
Verified local copy: `output/mlb_unfiltered_rebuild_2026-10-09_v1/qa_receipt.json`,
11,864 bytes, SHA256
`bcc2955b78c17fcc68350693b9a699765a5df6a9046bd63c09a8b8aac293a933`.
It binds the exact completed manifest and summary fingerprints above. Local logs
are under `qa_logs_v1/`; `resource_usage.txt` is 1,031 bytes, SHA256
`3ba1d782f0c313d4cd5945b7e871ac6c6ce6fdf38eef1f91c643d50cd5417b5a`.
GNU time records elapsed `0:25.09`, maximum RSS 4,039,680 KiB and zero swaps.
This saved-artifact QA does not independently certify the legacy inferred BUY
normalization, native collection completeness or missing-wallet classifications.

## Completed local count report

The report consumes the completed build manifest and independent QA receipt; it
does not rerun an estimator. Portable source:
`output/mlb_unfiltered_rebuild_2026-10-09_v1/03_report_v1/mlb_unfiltered_rebuild.tex`,
3,098 bytes, SHA256
`608616b7a817e4e5a970e4019af5a6a956c3ae234d3904b677bb54694dd271a1`.
Its adjacent `manifest.json` is 16,227 bytes, SHA256
`879b5727b87abd17a62c37e824e33e1a4293e269a016e1ff66319180e2e6e82e`.
The saved report manifest binds the verified QA receipt and frozen source HEAD.

Root confirmed successful built-in LaTeX compilation and a clean one-page local
preview without substantive warnings. Root inspected the entire page, including
all three tables and ten reconciliation rows, with no clipping or overflow.
Opening the source in native Codex was queued for thread
`01a0781e-5822-7192-afad-650716e72682`; visibility was not confirmed at this handoff.

## Infrastructure and release

Canonical starting revision: `33fdf9f0772793cddd7227d130a214883d6fea65`.
Released source branch: `codex/mlb-unfiltered-rebuild`; body commit:
`6d5f07520eee4fce064d5005e7836a247951e354`. After explicit user approval, root
pushed to `github.com/josephweintraub/prediction_markets.git` and verified HEAD and
`refs/remotes/origin/codex/mlb-unfiltered-rebuild` at that same commit. The canonical
checkout was clean before and after the body. Local `index.json` retains the push
and source receipts. Release fixtures passed locally (50 tests in 6.74 seconds),
canonically (81 tests and 80 subtests in 14.47 seconds), plus 10 independent extra
fixture probes.

The root owns all EC2 lifecycle and storage operations. The data volume UUID was
verified before mounting, with 25,854,373,888 bytes available. Use bounded memory,
spill and output budgets; disable the previously diagnosed DuckDB common-subplan
optimizer without changing the installed runtime.

The completed body used DuckDB 1.5.0, UTC, eight configured query threads, a 96 GB
memory setting, a 4,000,000,000-byte spill cap and a 4,000,000,000-byte combined
output cap, with a 12,000,000,000-byte free-disk floor. The three Parquets total
1,218,901,160 bytes. GNU `/usr/bin/time -v` records exit 0, elapsed `14:06.89`,
maximum RSS 22,211,220 KiB and zero swaps. No body process, tmux session or body
staging residue remained at handoff; this does not imply instance shutdown.

Independent QA, compact transfer and local count-report rendering/compilation/
visual QA are complete. The separate root report-QA receipt is
`output/mlb_unfiltered_rebuild_2026-10-09_v1/report_qa_v1.json`.
Broader native-action completeness and wallet-label correctness remain separate
unresolved questions; this repair does not certify them.

## Root finalization receipts

This tracked record freezes the completed computation and report checks. Source
publication and instance shutdown are separate completion gates: their final
observed states, revisions and times belong in the root's local stage journal
named above. No stopped-state claim is inferred from absence of a body process.
The bounded build and saved-artifact checks do not certify the broader data canon
or constitute a scientific rerun.

- Independent QA: complete; exit 0, eight gates, 30 count/support fields and verified
  transfer are recorded above. Root may add its final acceptance reference.
- Report: local source, manifest and root compile/visual QA are complete and recorded
  above; no new scientific estimates are authorized by this count report.
- Infrastructure: root must finish required transfers, confirm no agent, process,
  notebook or transfer still needs the instance, stop it and save the verified
  stopped state/time in the stage journal before delivering completion.
