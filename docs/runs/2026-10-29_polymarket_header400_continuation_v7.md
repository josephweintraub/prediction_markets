# Polymarket exact-header 400/second continuation

Status: the root-owned v6 stop/export completed in 289.20 seconds, within its
already reserved 600-second terminal audit. All 413 files, 102 sealed chunks
(10,200,000 keys) and partial-request charges were preserved and reopened.
No v7 admission, source adoption or launch exists yet. Reviewed local preflight
work removes duplicate verification while retaining complete semantic passes,
full byte/identity reopens, exact namespaces and current consumer checks.
No final trade sample, estimates or Dropbox release is admitted.

## Authority and narrow change

On 29 October 2026 the human supplied Alchemy dashboard diagnostics: about
3,000 CU/second, 100% reported recent success and no throughput-limited traffic,
and stated their plan permits 10,000 CU/second. They then explicitly approved
the proposed 400-header/second continuation. This changes only the paced rate,
not the provider, requested keys, science or other cumulative resource ceilings.

Alchemy's current EVM method-cost reference lists `eth_getBlockByNumber` at
20 CU, consistent with 150 headers/second consuming about 3,000 CU/second.
The proposed 400 headers/second corresponds to about 8,000 CU/second, leaving
2,000 CU/second for other account activity and retries. The account allowance
is shared across apps and networks. Dashboard medians do not demonstrate that
complete 100-header batch responses and validation will sustain that rate.
Measure a fresh sealed chunk before replacing a conditional forecast with an
observed rate.

References checked on 29 October 2026:

- <https://www.alchemy.com/docs/reference/compute-unit-costs>
- <https://www.alchemy.com/docs/reference/throughput>
- <https://support.alchemy.com/articles/3788245542-is-alchemy-s-throughput-cu-s-limit-shared-across-all-apps-or-set-per-chain>

## Frozen predecessor and execution contract

- Canonical clean pushed v6 HEAD: `045deabcdb9b8a295680d1b6e3411b85fe80eeea`.
- Predecessor: `/mnt/data/runs/2026-10-28_polymarket_exact_header_campaign_v6`.
- Operational controls: `/mnt/data/runs/2026-10-27_polymarket_oct1_finalization_v1`.
- Caller PID/birth ticks: 17774/582770; producer: 17867/582795; boot
  `5b441f68-0ef0-424b-b917-58e5b03b0b8d`. Revalidate all three identity fields
  before any root-owned signal. Never identify ownership by PID alone.
- Last saved local monitoring checkpoint before this change records 9,700,000
  producer-sealed keys/97 chunks at 16:31:59 UTC. That is progress, not complete
  independent required-key/native-hash acceptance.
- Required keys remain 32,901,442, including all 5,884,493 native expected-hash
  assertions. Final scientific cutoff stays execution time before
  `2026-10-02T00:00:00Z`.
- Keep one active HTTP request and initial batches of 100. Preserve all old
  retry/split rules, attempts and cumulative 259,200 seconds, 1.3M HTTP attempts,
  130M method items, 2TB charged responses, 16MiB per response, 8GB accepted
  headers, 8GiB address space, 20GB free-disk floor and zero-spill ceilings.
- Preserve the same single 2MiB/256-reference external-control pool. Reconcile
  retained actual controls and remaining reservations; do not create a new pool
  or refund failed work or unused conservative runtime reservations.

The mandatory conservative handoff envelope initially exceeded that same pool
by 51,734 bytes. Root therefore reallocates 65,536 bytes of the unused prospective
stdout-prefix reservation: only the new caller retains at most 983,040 bytes
(960KiB), instead of 1MiB. Its complete stdout stream is still hashed and counted;
truncation remains explicit. Historical callers, logs and charges are unchanged.
The new fixed prospective reservation is 1,245,184 bytes/nine files, not a second
allowance. The initial repaired envelope leaves 13,802 bytes; actual immutable
controls and final source sizes must pass the same pool gate before a signal.

## Work ownership and handoff gates

The collector author owns only new v7 collector/tests/specification. A separate
agent owns new preparer/caller/tests and a bounded root-operator draft. An
independent reviewer audits both. Root owns integration, actual process signals,
source adoption, canonical commit/push, admissions, launch and infrastructure.

1. Develop and test locally while canonical source and the healthy v6 producer
   remain untouched. A new module constant cannot change the old RPC class's
   defining-module limiter; use an explicit independently tested v7 policy.
2. Prepare the complete carry interface before interrupting the predecessor.
   There is no existing stop-after-chunk command. A targeted stop can leave a
   partial chunk; keep every partial file and request charge and refetch only
   under its remaining existing attempt allowance.
3. Root verifies caller/producer birth and boot identities, requests a bounded
   owned shutdown, reopens terminal receipts and independently certifies every
   relevant process, notebook and transfer has drained. Do not kill unrelated
   processes. Stop EC2 and verify stopped if no admitted work still needs it.
4. Freeze the exact predecessor physical/control roster and full request ledger.
   Reopen all seals against literal keys, native hashes and adjacent clocks;
   preserve all historical roots, counters, attempts and payloads unchanged.
5. Adopt only reviewed new source after drainage. Require clean committed and
   pushed current source, canonical Linux fixture success with no skips, fresh
   boot/volume/resource/complete-consumer checks and finite accounting before
   a new immutable v7 admission. Never mutate old helper globals or receipts.
6. Launch exactly one fresh caller, independently accept its first *fresh* seal,
   measure actual rate and bind the existing quiet 30-minute monitor to real
   new launch/admission/process receipts. Imported old seals are not that prime.

The remaining-stage record and readiness plan continue to govern subsequent
global metadata, V3 semantics, wallet/archive bridge, sports, analysis and release
gates. This rate change does not admit or execute those stages.
