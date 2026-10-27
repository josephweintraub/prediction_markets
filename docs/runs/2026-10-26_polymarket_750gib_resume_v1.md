# Polymarket refresh resume after 750 GiB expansion

Status: storage extension verified, 26 October 2026. No new final sample,
estimates or replacement Dropbox release is admitted by this record. The
[original readiness record](2026-10-26_polymarket_september_refresh_preflight_v1.md)
and [final-sample contract](../analysis_specs/polymarket_final_sample_refresh_v1.md)
retain the exclusive `2026-10-02T00:00:00Z` cutoff and full nine-sport scope.

Terminal update: adaptive raw append v2 completed and reconciled; EC2 was
independently verified stopped at `2026-10-27T03:32:12.814Z` (26 October
Eastern time), and its completion heartbeat is paused. This is raw collection
completion, not admission of the refreshed paper sample.

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

The current-chat heartbeat `monitor-polymarket-append-collection` was configured
every 15 minutes and is now paused after verified terminal shutdown. While
active it must remain quiet on unchanged healthy progress, never
launch or retry collection, and notify only completion, failure or a needed
decision. After the parent and every owned child/transfer exit, root stops
EC2, verifies stopped and pauses the heartbeat. Desktop monitoring requires
the user's computer and app to remain running. The durable launch receipt and
small local task state distinguish preparation from a launched job.

## Append v1 terminal state and approved follow-up

The append campaign launched at `2026-10-26T17:59:02.150037Z` from clean,
pushed revision `19202a820bef569f449a34fc731107f16470e155`. Its frozen plan
SHA256 is `364a81171cb31f098e1876832ad9a703c6ead963862fc56cd2084a44a1ac6682`.
The launch and terminal receipts remain under this resume run directory.

Chunk `c000018`, blocks 89,149,538–89,159,537, failed on
`rpc_response_byte_limit`. Its 76th 50-block request exceeded the 16 MiB
response-body ceiling. The saved 16,777,217-byte charge is the limit plus one
detection byte, not the full response size. The preceding 75 successful
windows and 436,508 records remain partial, unaccepted staging. No oversized
response was accepted or silently truncated into a successful shard.

The parent stopped new launches and drained its other three children. It
finished with 19 accepted chunks: `c000001`–`c000017`, `c000019`, `c000020`.
They are not a contiguous 19-chunk prefix. The failed chunk is still a gap;
the campaign has no COMPLETE marker. No old release or final sample changed.

Root reopened every saved child birth identity and checked host compute and
transfer consumers. With no remaining holds, it stopped EC2 and independently
verified `stopped` at `2026-10-26T18:24:57Z`, then paused the heartbeat. See
`native_append_campaign_shutdown_check_v1.json` and
`native_append_campaign_result_v1.json` in the small local receipt mirror.

The user subsequently approved smaller requests and resuming preserved work.
Implement and independently review a new versioned adaptive contract before
execution. Preserve the old collector/campaign entrypoints and all outputs;
reference the 19 accepted chunks read-only and recollect the entire failed
chunk into fresh output. Split only oversized requests; retain the original
roster, endpoint, safety limits and scientific gates. Do not resume the old
fixed-width/source-bound directory under changed code.

The reviewed successor is specified in
[`polymarket_adaptive_append_resume_v2.md`](../analysis_specs/polymarket_adaptive_append_resume_v2.md).
Its small local handoff is
`output/polymarket_adaptive_resume_2026-10-26_v2/task_state.json`; launch and
runtime receipts belong to
`/mnt/data/runs/2026-10-26_polymarket_adaptive_resume_v2`.
The reboot changed only the witnessed transient Linux device number. A separate
bounded admission must regenerate strict inventory/plan bindings and verify
device-only equivalence before launching the successor. Canonical fixtures,
source revision, bridge result and actual launch state are recorded in those
receipts, not inferred from this preparation record.

The historical V3 gap, metadata/token-family/outcome admission, exact-clock
coverage, archive-retention bridge, fresh classification coverage and refreshed
nine-sport clocks remain required. Root must finish all required data handling,
check that no process or agent needs EC2, stop it and verify stopped after this
work unit, including on interruption or failure.

## Adaptive v2 terminal collection and shutdown

The successor retained exactly 19 accepted v1 chunks by reference and
validated 563 fresh chunks. All 582 planned chunks cover blocks
88,979,538–94,796,039 without gaps or overlaps. Their manifests sum to
473,196,272 raw native event records. This is not an economic-fill count or
the final estimation sample. The separately accepted first 1,000-block probe
is outside this campaign interval and remains independently bound.

The campaign published `COMPLETE.json`, manifest
`manifests/bc81639712b44761b2dfcc39816bd777.json`, and
`sessions/d63c9a51a38d4cdaa0205a7bf9685a27/RESULT.json` under
`/mnt/data/runs/2026-10-26_polymarket_native_append_campaign_v2`.
The result records collection complete and analysis not admitted.
Canonical source remained clean at
`c39ddce3d9a010fe65faac652359fd401561e877`.

Root reopened all 583 old/new saved child process identities, confirmed
the parent absent and checked host compute, transfers and user jobs.
Independent local review reconciled the terminal metadata and released
its hold. After transferring only the small terminal metadata, root repeated
the host-idleness check and then stopped EC2 normally. AWS independently
reported `stopped`; the existing heartbeat was paused, not replaced.

The small local terminal record is
`output/polymarket_adaptive_resume_2026-10-26_v2/terminal_checkpoint_2026-10-27T033212Z.json`.
Its sibling `task_state.json`, terminal metadata,
`shutdown_preflight_v1.json` and `post_transfer_idle_v1.json` preserve the
evidence and remaining gates. No data was promoted, no analysis was run,
and the existing paper and Dropbox release remain unchanged.
