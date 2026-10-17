# Fresh Kalshi replication: 16 October 2026

Status: fresh input construction and independent input acceptance are complete.
Core estimates have passed the independent saved-score audit. Sports estimates
are complete, but their audit is blocked on a saved-artifact reader adapter;
final report/release acceptance remains pending. The user explicitly selected a
fresh raw-data replication, not a presentation of saved sports estimates.

Scientific contract: [Kalshi v1](../analysis_specs/kaushik_kalshi_replication_v1.md).
Root owns instance lifecycle; workers have no lifecycle authority. The prior
inventory stopped the instance and verified it stopped. This task restarted it
for the approved fresh replication. Final shutdown remains required.

## Source admission to date

- Original trade manifest: 228,872 bytes; SHA-256
  `37aae4a94b1125cea81eb2222caba0daaa089eb9c241a0e6dd90bb0144fdf0ee`.
  Inspected header `file,bytes,sha256`; 1,984 entries including the old quote
  release, which is not an input to this study.
- Market metadata manifest: 11,046 bytes; SHA-256
  `0ad2a1c6167f5171a5323e3bee021a27ae58516ee68ca23aab64d10ca5683ecf`.
  Inspected header `path,bytes,rows,sha256`; 80 shards. Dropbox basenames must
  map bijectively to its S3-relative paths.
- Native event reference: 43,666,531 bytes; SHA-256
  `f535371e2b2c50946125e5ef0004aa418c1ad0fe1dea186c90768717a0480c47`.
  Footer: 822,590 rows; category and series key are on these event rows, not
  on the inspected market-metadata footer.
- First daily trade file: 1,980 bytes, one row, valid canonical UUID; SHA-256
  `1236148e7d08d94b1dfd066353b02b649c0d2d42b2a010ab326a2684b9574977`.
  Actual schema includes undocumented `src:string`. It is explicitly admitted
  as provenance-only; `source/src` disagreement is audited, not filtered.
- Inspected `bulk_v2` metadata has dollar notional 1, while inspected `forward`
  metadata has notional 0 and valid ISO-8601 settlement strings. Missing/zero
  notional is not a zero payout; the explicit binary venue payout rule is
  separate from this unusable raw numeric field. Full-source admission remains
  required before interpreting these samples as representative.

The immutable exploratory metadata directories are
`/mnt/data/runs/2026-10-16_kalshi_source_admission_v1` and `v2`. The first failed
before transfer because installed rclone rejects `copyto --max-size` on a single
file. It is preserved. The staging helper now validates exact remote metadata,
uses the compatible hard transfer ceiling, and checks bytes/SHA-256 before
processing; no exception workaround or original-data deletion was used.

Direct S3 listing and a documented-manifest HEAD were denied for the existing
IAM profile during the preceding inventory. No IAM or credential changes were
made. Existing Dropbox reads work, so the planned serial source staging does
not require new S3 permissions or a larger disk.

## Validation history and preflight gates

The milestones below retain their status at execution; the completed-input
section records the later acceptance that supersedes provisional statements.

Pure normalization, serial-staging and grouped-estimator fixtures have passed,
including direct print-level numerical oracles. Full combined runtime tests,
committed-source hashes and actual stage bindings will be recorded on execution.
Fixture success is not a billion-row capacity or source-integrity certificate.

Independent integration review caught and repaired the initial mean-versus-sum
cell interface mismatch before production. A direct 1,728-print builder-through-
estimator fixture then matched independent raw-print coefficients and joint CR0
for all 12 regression specifications. Runtime review also reproduced DuckDB's
temporary-flag change after a table COMMENT; owned daily-cache cleanup uses the
authoritative temporary database identity plus its ownership marker, not that
unstable flag. Strict timestamp-format/range parity and immutable JSON
publication have separate regression fixtures. These are implementation checks,
not findings about the archive's empirical results.

The first committed full metadata stage
`/mnt/data/runs/2026-10-17_kaushik_kalshi_metadata_v1` stopped before publishing
any completed metadata receipts. Its native event reference had 822,590 unique,
non-null keys and one valid legacy key, `GDP-232022 Q4`, containing an internal
ASCII space. Our identifier check was too restrictive, not evidence of a
duplicate or corrupt event. The immutable failure is preserved. A narrow
validator repair keeps this exact identity, rejects padding/other whitespace
and controls, leaves UUID validation unchanged, and adds Python/SQL regression
fixtures before a new committed stage is attempted. No source record is dropped
or rewritten because of that space.

The repaired source was committed as
`ec1415f0eb17a3bf164fed5d28b8fc3920c7f887`; 78 focused canonical-runtime
normalization, builder, metadata and guarded-driver tests passed before the
new immutable metadata stage `2026-10-17_kaushik_kalshi_metadata_v2` began.
It completed all 80 shards and the exact global proof over 166,961,153 distinct
non-null ticker keys, with zero invalid keys, duplicate keys or conflicting
status/result keys. Its candidate manifest is 213,370 bytes, SHA-256
`dc73df7053700934c3cdf8358cd5341aa5e1c46c90ab94ef9a0c96d19728212d`.
The noncompound projection retains 14,697,951 records and 41,000 native winner
market candidates. Structural metadata completion is not yet independent trade
input acceptance or a scientific result. Local development added a
separate saved-body audit, bounded eight-day raw replay, non-accepting capacity
probe and an independent saved-score covariance/contrast audit. Before any full
trade scan, diagnostic-only builder additions save distinct admitted-cohort
timestamp bounds and tail-only clock exclusions, preserving raw bounds and
scientific eligibility. These additions require their own committed-source
production tests; fixture results do not establish archive findings.

Independent preproduction review caught an audit counter bug: excluded physical
row ordinals were checked against a shadowed analytic count instead of the raw
daily row count. The narrow repair separates raw, analytic and cohort counters.
Counterexamples retain a valid last-row exclusion while rejecting negative or
out-of-range ordinals; no source rows, eligibility rules or resource caps change.

Further synthetic saved-artifact counterexamples exposed missing-value checks
and sports-cache multiplicity gaps in the independent audit, not observed
corruption of the archive. Repairs validate required daily/final fields before
aggregation, including empty-duration claim counts, and reconcile every daily
sports claim count plus winner-event/bin totals and global cache counts.
Raw individual cache price/timestamp replay remains explicitly limited to the
eight pinned days; it is not described as a second full-tape verification.
Independent fresh-DAG repair-closure tests passed for the valid and corrupted
counterexamples, and all 191 installed Kalshi fixtures passed on the canonical
runtime in 49.266 seconds before the next committed probe. Native sports-map
production remains separate and awaits its own source review and admission.

The committed eight-day capacity probe at
`/mnt/data/runs/2026-10-17_kaushik_kalshi_capacity_probe_v1` completed successfully
under source `02b30ce315d66f31c07cec7012f2dc9ee9518bdd`. Its 933,475,481 source
bytes contain 27,152,545 rows with exactly as many distinct native IDs. Saved
daily and final cells occupy 65,219,141 bytes. A composition-sensitive linear
scenario using the upstream-stated full count is 3,868,162,462 bytes, below the
unchanged 12 GB input ceiling. This is a capacity check, not a guaranteed bound
or input acceptance; the full run retains the 20 GB free-space floor and hard
caps. The probe manifest SHA-256 is
`79aa2eb3dce9e9804e11bf89c598fcb97d29211b514c11e721d0f9eaab146f07`.

The fresh trade build began at 16:44:16 UTC on 17 October under unchanged
source `02b30ce315d66f31c07cec7012f2dc9ee9518bdd`, stage
`/mnt/data/runs/2026-10-17_kaushik_kalshi_inputs_v1`. The canonical checkout was
clean, the mounted data volume UUID was verified, and 58,860,699,648 free bytes
and 264,723,083,264 available RAM bytes passed the existing preflight. It is a
serial 1,919-file build, not an accepted input or result. Concurrent worker work
is local source/test development only; canonical files and HEAD remain fixed
until the guarded build exits. Initial early source files have zero admitted
baseline rows with observed `unadmitted_market` exclusions. A saved-receipt-only
annual coverage table will reconcile all years without treating an observed
first exclusion as an exhaustive raw-metadata diagnosis.

Before estimates: admit all source hashes/schemas, complete native ID uniqueness,
all metadata keys and joins, binary outcome/combination/category rules, clock
coverage, lossless cell moments and every disjoint exclusion. Preserve the 20 GB
free-space floor and measured RAM/spill/output ceilings. Sports identity and
actual play-clock binding is a separate admission gate, not inferred from native
settlement or milestone end fields.

## Completed input acceptance and coverage: 17 October 2026

The full build completed all 1,919 pinned daily files. Its exact global native
UUID proof found 1,610,423,776 rows and the same number of distinct IDs. The
independent input audit passed all ten required checks, reopened the saved
body/receipt/provenance chain, and independently replayed 27,152,545 native
prints on the fixed eight days: 2021-06-30, 2025-03-12, 2025-03-13, 2025-04-01,
2026-04-01, 2026-06-01, 2026-09-01 and 2026-10-01. This is bounded raw replay,
not independent replay of the full 1.61-billion-print history. Exact accounting
of the supplied files does not establish an exchange-wide census or perfect
source data.

Committed producer/auditor bindings are separate, rather than assumed equal:

| Stage | Source head |
| --- | --- |
| H2: metadata v2 | `ec1415f0eb17a3bf164fed5d28b8fc3920c7f887` |
| H3: full trade inputs | `02b30ce315d66f31c07cec7012f2dc9ee9518bdd` |
| H4: input audit, reader adapter, core and sports stages | `0fbf27f0d7b8c03fecb66eece88736195c3bcd70` |

H4 passed all 267 canonical-runtime fixtures in 60.524 seconds. The accepted
input pair is in `/mnt/data/runs/2026-10-17_kaushik_kalshi_inputs_v1`:

- Original `candidate_manifest.json` SHA-256:
  `a0dd23b4eac187fc87bd4c3d891adc9c22b3305c8f497f2687cdf435a25fdd0e`.
- H4 independent input-audit candidate SHA-256:
  `a6a9762060eeee105af31fca20be20c0d79ae72f175b2d67e9c42e580701e048`.
- Published accepted `manifest.json` SHA-256:
  `1bac871abb93eb80b5ae5b588bb5a2a017990b35c65d53892523b075bb4755e3`.
- Published `acceptance.json` SHA-256:
  `e9531a6b355be554b2e88613145f9fb8ac11a7028af6b729843b303f12a93874`.

The unchanged original candidate retains its pending-publication status by
design; the separate accepted pair carries acceptance. Saved outputs before
the candidate manifest occupied 4,991,896,406 bytes, within the 12 GB ceiling.

### Cohort accounting and clocks

The disjoint first-failure counts reconcile exactly:

| Eligibility status | Prints |
| --- | ---: |
| Eligible baseline | 1,107,435,134 |
| Unadmitted market | 299,753,791 |
| Known combination excluded | 153,673,297 |
| Not finalized | 47,233,897 |
| Nonbinary or missing result | 2,295,553 |
| Unadmitted combination classification | 32,084 |
| Invalid trade identifier | 20 |
| All excluded | 502,988,642 |
| All supplied source prints | 1,610,423,776 |

Thus source = baseline + excluded; baseline retains 68.7667% of supplied
prints. These are eligibility labels, not diagnoses of upstream missingness.
In particular, the 20 identifier exclusions do not contradict the global UUID
proof or establish which other identifier failed.

| Cohort | First observed execution UTC | Last observed execution UTC |
| --- | --- | --- |
| Supplied archive, before eligibility | 2021-06-30T20:09:14.185137Z | 2026-10-01T23:59:59.990712Z |
| Admitted baseline | 2025-03-09T23:00:11.737581Z | 2026-10-01T23:59:59.990712Z |
| Admitted duration tails | 2025-03-09T23:01:42.272265Z | 2026-07-25T11:00:46.954289Z |

The archive span is not the effective analysis span. No manual 2025 cutoff was
introduced. Saved yearly coverage uses UTC source-partition years, not these
execution extrema:

| Source year | Files | Source prints | Baseline prints | Duration-tail prints | Excluded prints |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2021 | 184 | 153,450 | 0 | 0 | 153,450 |
| 2022 | 365 | 1,023,122 | 0 | 0 | 1,023,122 |
| 2023 | 365 | 1,722,748 | 0 | 0 | 1,722,748 |
| 2024 | 366 | 5,463,739 | 0 | 0 | 5,463,739 |
| 2025 | 365 | 98,622,423 | 92,196,925 | 17,487,640 | 6,425,498 |
| 2026 | 274 | 1,503,438,294 | 1,015,238,209 | 98,922,008 | 488,200,085 |

Each year and the column totals reconcile. Of 8,363,059 pre-2025 source prints,
8,362,889 have the first-failure label `unadmitted_market`; this summary does
not assign a more specific cause to that label or the remaining 170 prints.
The 2026 partitions contribute 91.6747% of the admitted baseline.

The baseline has 269,386,011 tail prints (24.3252% of baseline). Duration
analysis retains 116,409,648 prints: 43.2130% of tails, not of all prices.
The other 152,976,363 tail prints all have
`missing_or_invalid_settlement_ts`; saved tail exclusions for missing opening
time, nonpositive lifespan and outside the recorded open–settlement interval
are zero. Source-version reconciliation is exact:

| Native metadata source | Tail prints | Retained duration-tail prints | UTC source-partition coverage of retained duration |
| --- | ---: | ---: | --- |
| `bulk_v2` | 148,819,291 | 0 | None |
| `event_v1` | 4,157,072 | 0 | None |
| `forward` | 116,409,648 | 116,409,648 | 2025-03-09 through 2026-07-25 |

Duration results therefore describe a source-version-limited complete-case
tail cohort, not uniform clock coverage of the baseline. The 838,049,123
middle-bin baseline prints are intentionally outside the duration-tail
denominator, not clock exclusions. The duration restrictions `L_gt1`
(67,226,641 prints) and `R_gt1` (11,236,124) are separate overlapping panels,
not an additive partition. Settlement is the native lifecycle endpoint, not a
closing-price or provider sports-clock substitute. Finalized-outcome admission
also requires the resolution-censoring caveat for end-of-sample comparisons.

### Sample support, saved reader evidence and outstanding release gates

Public prints have equal observation weight, not contract-size or dollar
weight. The fixed probability bins, binary taker-side claim normalization,
payoff/return definitions and event-clustered CR0 uncertainty follow the
[scientific contract](../analysis_specs/kaushik_kalshi_replication_v1.md).
Known combinations and unproved combination classifications are excluded;
there is no wallet analysis or A2 exercise. Lossless saved cell sums, rather
than group means treated as sums, feed the core regressions.

The baseline covers 2,982,229 markets, 4,978,820 normalized ticker/direction
claims, 606,231 native event clusters and zero market-fallback clusters. Its
19 native categories sum to the baseline count. Sports contributes 595,956,268
prints and Crypto 411,900,747, together 91.0082% of baseline; these shares do
not establish duration-category composition. The common duration cohort has
335,040 clusters and 2,473,848 claims. Of those claims, 156,172 visit both
tails, contributing 15,367,095 prints. “Both tails” refers to the same
ticker/direction claim, not the presence of both YES and NO claims.

The H4 saved-only reader adapter reopened bound accepted-input and native
metadata artifacts, without raw tape reads. Local checks verified these files
under `output/kalshi_replication_2026-10-16_v1/accepted_summary_v1/`:

- `reader_metadata.json`: 6,411 bytes, SHA-256
  `0a10181f1c71cebf43fc3a9ad7d30182369e2de10da03d05cd6c1d373e7e1f72`.
- `reader_provenance.json`: 1,405,091 bytes, SHA-256
  `268a41b1542d2082731caa409319ee70d20340511836efe323909fb075cc1de7`.

The reader provenance binds 3,930 unique body paths, the accepted input pair
and the separate H2 metadata manifest. These are saved-summary checks, not a
new raw-data scan. The saved native sports cache contains 198,611,524 winner-
candidate prints; this is not the final provider-bound sports sample. Sports
requires independent identity, named-YES result and retained provider clock
proofs. Provider-overlap coverage and ATP's scheduled-start plus archived-
duration convention must remain explicit; clock verification does not claim
new play-by-play reconstruction or guaranteed observed actual boundaries.

At this checkpoint, core estimates are complete and independently saved-score
accepted. H4 core manifest, estimates and score-audit SHA-256 bindings are,
respectively:
`79101f4a4c66b1f56b384bc0316841a0688e9c85cc602d0a000407333b11dece`,
`30e05b4b60e6d539b3505d3dc43b51c1047267c85470042487ff6f4cff8b5e5e`,
`cf305b1539494acee261b793479ac7269e90747e2e5e3e06070abba3cb3cdc70`.
The numerical findings are recorded separately, not inferred from these
coverage summaries.

H4 sports estimates are complete, with manifest SHA-256
`f4e72c23b53990cb26d730d06b20f660b16f329b2f91cbff17826c65ffa2d69b`
and estimates SHA-256
`9b4cadbe13592580f353fc979f5c4d43939bf4fdf1b8a288ddff244ee2240f1a`.
Independent sports audit/release is not yet accepted: its artifact reader
expects top-level `bytes` while the saved binding uses `stat.bytes`. Root is
handling that adapter issue; it is not evidence of a failed numerical gate.
Final report rendering, compilation/page QA, release and verified instance
shutdown are not asserted complete here. Exact CLI/API reproduction and
external-body requirements are in the
[reproduction guide](../analysis_specs/kaushik_kalshi_reproduce_v1.md).

## Post-H4 audit-reader and execution-plan closure

The first sports-score audit is preserved as
`/mnt/data/runs/2026-10-17_kaushik_kalshi_sports_score_audit_v1`.
Its reader rejected the producer's real `path/sha256/stat` core JSON identity
because a synthetic integration fixture had supplied flat `path/bytes/sha256`.
Commit `36166867f2d1ef744ab37fbf312551020db086ca` adds a strict read-boundary
adapter and real-producer identity/drift tests. Completed manifests, estimates
and producers remain unchanged. All 271 Kalshi fixtures passed on EC2 in
60.823 seconds.

Commit `2496427118b60fc15ae7cbf9fc865c258ae9bb30` clarifies the report's
tail-specific duration model, literal day-clock definitions and game-count
labels. The 35 report/mocked-driver fixtures passed on EC2 in 3.439 seconds.
Its exact clean 46-file source capsule passed 272 local Kalshi fixtures in
29.085 seconds, 14 shared-engine fixtures in 0.055 seconds, namespace/import
origin checks, byte-level archive/source inventory and limited credential-pattern
screening. This is fixture/source QA, not a fresh environment or raw-data rebuild.

The second independent sports-score audit is preserved as
`/mnt/data/runs/2026-10-17_kaushik_kalshi_sports_score_audit_v2`.
It passed the repaired core-binding read and then exhausted the unchanged
16 GB memory / 4 GB spill limit on native claim/map validation. A read-only
`EXPLAIN`, not `EXPLAIN ANALYZE`, showed the optimizer placing the 198,611,524-row
candidate cache on the hash-build side because its multi-file estimate was
about 1,000 rows. A second `EXPLAIN` with fixed
`disabled_optimizers='join_order,build_side_probe_side'` retained the small
19,383-row map on that side. The physical-plan setting already used by the
sports producer is being admitted separately for the independent auditor.
The proposed 64 GB / 96 GB available-RAM alternative was not committed or run;
the intended retry keeps 16 GB memory, 32 GB minimum available RAM, two threads,
4 GB spill, the 20 GB disk floor and all scientific queries/gates unchanged.
Final sports-score acceptance, actual report authoring, publication and shutdown
remain pending at this checkpoint.
