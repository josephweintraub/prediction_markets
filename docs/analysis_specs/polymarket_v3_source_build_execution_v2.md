# Bounded V3 source-build execution v2

Status: local implementation contract, not production or sample admission.

Run one immutable root-admitted zero-API stage using the authenticated existing
Solidity 0.8.34+commit.80d5c536 binary. Preserve the original nonexecutable download;
create a separate authenticated executable copy inside the new stage. Never
download, retry or resume a compiler stage automatically.

The stage has 1,800 whole seconds, two compiler builds of at most 600 seconds,
one version check of at most ten seconds, 8 GiB combined parent/child address
space, no spill and a 20 GB free-space floor. All new artifacts, including the
16,865,544-byte executable copy, fit inside 64 MiB. One MiB of that total is the
control partition; the remaining 63 MiB is the other partition. Failed/partial
artifacts and the full reservation remain charged. These are not additional
metadata allowances.

The unchanged v1 verifier provides compiler invocation, process ownership and
full runtime/ABI/declared-immutable comparison. The v2 writer adds aggregate
copy-aware enforcement; it does not alter helper globals or ignore runtime
bytes. Unknown ownership is not drainage and never authorizes signaling an
unbound PID. It blocks COMPLETE and requires root's full consumer census before
shutdown. Success requires three bound PROCESS/EXIT pairs and absent owned
groups, independently rechecked before publication.

## Admission and accounting

The SHA-bound root admission uses schema
`polymarket_v3_source_build_execution_admission_v2`, status
`root_zero_api_v3_source_build_execution_admitted`, exact source HEAD/run/caps,
and explicit references to:

- Formal v6 terminal-acceptance RESULT and COMPLETE, linked to the actual
  metadata manifest, START, COMPLETE, LAUNCH, EXIT, admission and root drain.
- Existing compiler acquisition RESULT, COMPLETE, root acceptance and original
  nonexecutable binary.
- The two pinned Sourcify raw packages and pinned runtime observation.
- Clean committed/pushed source proof, fresh current-boot/same-volume drained
  host proof, and one shared external-control inventory.

Carry at least 60,715 HTTP calls, 50,614,490,077 response bytes, 59,400 runtime
seconds and retained raw/control/other floors 4,481,019,912 / 288,982,658 /
16,865,544 bytes. Carry any larger formal terminal-acceptance charges too. The
root admission includes fresh preparation elapsed since 2026-10-28T17:45:44Z;
the formal immutable RESULT also supplies `prior_usage_as_of_utc`. Carry at
least its larger cumulative runtime plus the ceiling of seconds from that
as-of time to this wrapper's start. Missing, future or non-UTC as-of evidence
blocks execution. Never substitute the maximum of an old larger total and a
smaller baseline-plus-elapsed clock: that would erase nonoverlapping work.
Elapsed through the formal as-of timestamp is not added again. Reserve
the full new 1,800 seconds and 64 MiB before execution. The shared metadata caps
remain 100,000 requests, 96 GiB responses, 86,400 seconds, 20 GB artifacts and
12 GB raw / 1 GB control / 3.5 GB other. Keep one 2 MiB / 256-reference external
control allowance; nested references do not create a second allowance.
The pool must include every read acquisition control: RESULT, COMPLETE, the
pinned root acceptance, and its bound LAUNCH/EXIT, as well as the formal
metadata and current preparation controls. An authenticated control is not
exempt from accounting. The inherited compiler binary remains in the other
partition, separately from this control-file union.

Read-only `--check-only` performs the same input/admission checks without an
output directory or compiler dispatch. Root admits an actual execution only
after canonical Linux fixtures, fresh physical/resource/source checks and
serialization against other metadata work.

```text
/home/ubuntu/venv/bin/python scripts/prepare_polymarket_v3_source_build_v2.py \
  --admission ROOT_ADMISSION --admission-sha256 ROOT_SHA \
  --expected-head CLEAN_COMMITTED_PUSHED_HEAD \
  --run-dir /mnt/data/runs/2026-10-28_polymarket_v3_source_build_v2
```

Source-build success establishes only the two Exchange runtime/source builds
after exactly declared constructor immutable substitutions. Creation-transaction
authentication, dependency implementations, global token/outcome mapping,
scientific sample and release remain unadmitted. The next metadata pilot must
consume this stage's cumulative usage and owned-drain proof, not reset to the
older v6 floor.
