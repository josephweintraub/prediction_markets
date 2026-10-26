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

## Next bounded production gate

The [capacity v2 contract](../analysis_specs/polymarket_refresh_capacity_v2.md)
proposes a 324 GB aggregate envelope including the 20 GB untouched floor.
The measured margin beyond that envelope is 47,217,793,024 bytes. These are
hard proposed ceilings, not forecasts or evidence that unknown stages finish
within them. Full campaign enforcement remains unadmitted.

The next source work is independently reviewed bounded raw staging: an exact
100-block endpoint overlap and a separate at-most-1,000-block append, plus a
small native V3 history diagnostic. Each needs committed source, explicit
runtime/request/response/row/output/memory caps, unchanged input bindings,
exclusive outputs and reopened completion evidence. None changes trade
membership, admits decoded V3 economic semantics, proves full history or
publishes exact timestamps for interior log blocks. Successful bounded runs
must precede any larger campaign.

The historical V3 gap, metadata/token-family/outcome admission, exact-clock
coverage, archive-retention bridge, fresh classification coverage and refreshed
nine-sport clocks remain required. Root must finish all required data handling,
check that no process or agent needs EC2, stop it and verify stopped after this
work unit, including on interruption or failure.
