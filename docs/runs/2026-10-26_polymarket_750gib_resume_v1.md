# Polymarket refresh resume after 750 GiB expansion

Status: storage extension verified, 26 October 2026. No new final sample,
estimates or replacement Dropbox release is admitted by this record. The
[original readiness record](2026-10-26_polymarket_september_refresh_preflight_v1.md)
and [final-sample contract](../analysis_specs/polymarket_final_sample_refresh_v1.md)
retain the exclusive `2026-10-02T00:00:00Z` cutoff and full nine-sport scope.

## Verified storage and source state

The user reported the existing data volume modified to 750 GiB. Root verified
the stopped instance's attachment, started it and obtained a fresh DNS rather
than reusing the previous address. The canonical checkout was clean at
`c72d9f8b28dc099fe58f9f19a09cea73f3f4c736`. No subagent used or managed the
instance or storage during these checks.

The unpartitioned ext4 data device has serial `vol0acbba4565a64f792`, matching
attached EBS volume `vol-0acbba4565a64f792`, and UUID
`d0cd087b-94c4-428c-bae9-ae28929059f6`. Its kernel-reported size is
805,306,368,000 bytes (750 GiB). Root mounted only that UUID at `/mnt/data`.
The filesystem still exposed its old capacity, so root grew the existing
filesystem with `resize2fs`, without formatting, partition changes or data
movement. No compute/transfer process was active before growth.

`filesystem_expansion_v1.json` under
`/mnt/data/runs/2026-10-26_polymarket_750gib_resume_v1` records completion at
`2026-10-26T17:11:29.616930Z`, exit zero. Available bytes increased from
117,751,652,352 to **371,217,793,024**. The six inventoried native-source file
identities reopened unchanged. Available space is a filesystem measurement,
not the counterfactual sum of the old free space and added device bytes.

The IAM user's `DescribeVolumesModifications` request remains unauthorized.
No further AWS size modification was attempted. The verified device capacity
and mounted filesystem establish the observed capacity; they do not assert
the inaccessible AWS modification-state field.

## Completed bounded source checks

The committed collector at `96e7062a7d2ce826b3a637c4f9e3375392af3c14`
completed the literal endpoint overlap, blocks 88,978,438–88,978,537.
The independent auditor at `9916b9461760cbc41d9aee08997bfc8fd09b47d8`
read one footer-selected old row group (5,486,063 compressed bytes) and
reconciled all 11,125 legacy/V2 records, with no missing keys, extra keys,
duplicates or execution-field mismatches. It kept the 44 newly observed V3
records separate. This proves the literal overlap, not full-history coverage.
See `native_overlap_audit_v1.json` in this run directory.

The first append probe used 100-block requests and failed safely when one
response exceeded its byte ceiling. Its incomplete output remains untouched.
A fresh probe with 50-block requests completed blocks 88,978,538–88,979,537:
146,230 native event records, 83 RPC attempts, 159,982,356 response bytes and
11,424,198 retained bytes before its session result. Its manifest is under
`/mnt/data/runs/2026-10-26_polymarket_native_append_probe_v2`; SHA256
`92af7ab19e0de9deab0f4237b09343cbc393eb7a6ff545afa175a562aaf9477c`.
These are raw event records, not unique economic trades or paper observations.

The small V3 diagnostic observed a successful implementation upgrade at block
93,452,332 and independently reopened its receipt and adjacent implementation
slots. It did not establish the proxy's first deployment or a complete upgrade
history, and does not admit V3 amount, token-family or settlement semantics.
See `v3_history_probe_v1.json` in this run directory.

Canonical-runtime fixtures passed: 231 tests and 218 subtests in the saved
`runtime_fixture_receipt_v1.json`; the subsequent focused collector, V3 and
overlap-auditor run passed 103 tests and 103 subtests. All original inputs
remained unchanged in the executed source checks.

## Authorized background append and launch admission

The [capacity v2 contract](../analysis_specs/polymarket_refresh_capacity_v2.md)
proposes a 324 GB aggregate envelope including the 20 GB untouched floor.
The measured margin beyond that envelope is 47,217,793,024 bytes. These are
hard ceilings, not forecasts or evidence that unknown stages finish within
them. The separate append controller and independent review gate its larger
execution; downstream stages remain unadmitted.

The user authorized the remaining append as a quiet, completion-monitored
background run. The planned literal interval is 88,979,538–94,796,039, which
does not repeat the accepted first 1,000 blocks. The campaign alone covers
only this remaining interval; the accepted probe remains a separately bound
source. Planned output is
`/mnt/data/runs/2026-10-26_polymarket_native_append_campaign_v1`.

Admission requires clean committed source, independent controller QA and
canonical-runtime fixtures. The reviewed invocation permits four disjoint
10,000-block children at a time, each making serial 50-block requests. Each
child reserves 2 GB including controls, with at most 1,200 RPC attempts,
4 GiB response bodies, 3 million records, 8 GiB address space and 3,600 seconds.
The campaign ceiling is 128 GB retained bytes, 900,000 attempts, 3 TB response
bodies and 172,800 elapsed seconds. Existing retained diagnostic runs, failed
attempts, controls and all in-flight reservations count against the envelope;
20 GB free space remains untouched. A failed child stops new launches. There
is no automatic retry or increase in limits.

Independent QA accepted controller SHA256
`dcdce74740009f62f6fbd3a65cef33351fc7bbe4ab6f2fb92b5e3e7e96c1a7a4`
and tests SHA256
`a59b3424aed4ba453910d8d3355af19136f35cb5679bc06c65eccc566bac23a7`.
The independent 127-fixture run and original adversarial replays confirmed
that expired all-accepted resumes and edited failed-attempt usage cannot
publish completion. Canonical testing passed **272 tests and 233 subtests**
in 12.63 seconds; see `runtime_fixture_receipt_v3.json`. The accepted child
collector remains unchanged at SHA256
`0f1f0c1dd0f8f6a7f7dd3daee832ebf93f03210df7fe8f7b8ad7d736bba2d14f`.

The three native probe directories are charged from durable request journals.
The original preflight and capacity v1/v2 directories are additionally frozen
as diagnostic inventories, each conservatively charged 10,000 attempts,
16 GiB response bodies and 21,600 runtime seconds. These are deliberately
generous reservations, not measured usage; they are included in the totals
above. The [capacity contract](../analysis_specs/polymarket_refresh_capacity_v2.md)
records control-file reservations and the separate host-idleness check.
The durable launch receipt binds the actual clean source revision, command,
parent PID/birth identity and frozen campaign plan. This record alone is not
proof that the parent launched or completed.

The current-chat heartbeat `monitor-polymarket-append-collection` is active
every 15 minutes. It must remain quiet on unchanged healthy progress, never
launch or retry collection, and notify only completion, failure or a needed
decision. After the parent and every owned child/transfer exit, root stops
EC2, verifies stopped and pauses the heartbeat. Desktop monitoring requires
the user's computer and app to remain running. The durable launch receipt and
small local task state distinguish preparation from a launched job.

The historical V3 gap, metadata/token-family/outcome admission, exact-clock
coverage, archive-retention bridge, fresh classification coverage and refreshed
nine-sport clocks remain required. Root must finish all required data handling,
check that no process or agent needs EC2, stop it and verify stopped after this
work unit, including on interruption or failure.
