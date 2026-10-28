# Metadata budget continuation v6

Status: human-authorized implementation contract, 28 October 2026 UTC. This
document does not admit a production launch, metadata universe, final sample,
estimates or release.

## Scope and authority

The human approved the proposed cumulative limits with “yes sure”: 100,000 HTTP
attempts, 96 GiB charged responses and 20 GB stored metadata artifacts. The
[final-sample contract](polymarket_final_sample_refresh_v1.md), unchanged study
definitions and [run record](../runs/2026-10-27_polymarket_oct1_finalization_v1.md)
remain authoritative. Preserve every historical source and failed run. EC2 is
stopped during local implementation and independent review.

Only the following metadata ceilings change, in a new versioned profile:

| Resource | Cumulative ceiling |
|---|---:|
| HTTP attempts, including failures | 100,000 |
| Charged expanded response bytes | 103,079,215,104 |
| Retained raw response files | 12,000,000,000 bytes |
| Retained control artifacts | 1,000,000,000 bytes |
| Aggregate metadata artifacts | 20,000,000,000 bytes |

The other/mapping allocation remains 3,500,000,000 bytes. The 86,400-second
cumulative runtime, two requests per second, 4 MiB individual response bound,
three attempts per exact page, 60-second retry-after bound, 8 GiB process/RAM
limits, 20 GB free-disk floor, zero spill, and shared 2 MiB/256-reference external
control allowance remain unchanged. Instance and disk sizes do not change. No
new provider, stream, predicate, scientific filter or endpoint is authorized.

## Recovery and immutable inputs

- Add a v6 collector and fixtures, a narrow fixed caller-v4 profile, and a
  metadata-only admission builder. Do not alter old collectors or callers.
- Bind the fixed v4 prefix through its historical validators and old caps.
  Separately bind the v5 admission, producer START, FAILURE, LAUNCH, caller EXIT,
  root terminal-drain proof, and full request/page journals. The latter journal
  hashes must be freshly obtained and independently reopened, not invented from
  a terminal proof that did not record them.
- Replay every accepted body before any new HTTP: 11,456 v4 pages plus 22,245 v5
  pages, totaling 33,701 pages. Verify requests, hashes, bounded expansion,
  envelopes, scope, row summaries, stream/index/cursor continuity, exact saved
  file sets and totals. Restore repeated-cursor and repeated-page-digest guards
  across both prefixes. Reuse bodies by immutable reference, without copying.
- Preserve v5's failed final partial response as rejected evidence. It cannot
  become a PAGE or advance a cursor. Carry its one logical-page attempt toward
  the unchanged three-attempt limit when retrying that exact page.
- Reuse terminal CLOB-simplified and Gamma-v1-open streams. Resume only
  Gamma-v1-closed page index 25,829 from the literal verified saved cursor,
  followed by the unreached Gamma-v2 open and closed streams. Never restart a
  completed walk or splice the older full-CLOB representation into it.
- Old helpers retain their original global caps and historical validation
  semantics. The new profile owns changed-cap ledger enforcement; do not mutate
  frozen helper globals to make old proofs appear compatible with new budgets.
- Keep the existing 16 MiB individual control-record bound. If the finite final
  artifact inventory cannot fit, use deterministic bounded inventory segments
  and a bounded index instead of increasing that bound or dropping evidence.

## Accounting and launch gates

Carry at least 38,565 HTTP attempts, 34,359,738,368 response bytes and 40,200
runtime seconds, plus all fresh preparation and verification time. Retained-byte
floors are raw 3,401,462,298, controls 199,303,529 and other 16,865,544 bytes, plus
later unique controls and the saved failure write not yet in those snapshots.
Do not double-add inherited raw prefixes, refund a partial response, reduce the
runtime to the producer's smaller clock, or discard old reservations. Charge all
new sources, QA, admissions, logs, partial outputs and direct/nested controls.
One shared external-control reservation covers the entire reopened closure;
nested receipts or payloads do not create a second allowance.

Independent review, focused local tests, canonical Linux fixtures, clean
committed/pushed source, fresh same-volume/boot/physical-input checks, complete
consumer drainage, and remaining-runtime feasibility precede a separately
recorded actual launch. The caller's deadline includes replay and final reopens.
No callbacks may mutate inputs between final reopen and publication. Preserve
bounded, secret-safe failure diagnostics and single-clock nonnegative waits.

The approved envelope is finite; remaining universe size and successful
completion are not known. Do not silently increase runtime or another cap if it
proves insufficient. Pagination completion means only that the five fixed walks
terminated and saved artifacts reopened, not an atomic snapshot, globally unique
token/outcome spine, authoritative payouts or final-sample acceptance.

Only root manages EC2 lifecycle. Freeze canonical source while any producer is
active. Update/resume the quiet monitor only after an actual admitted launch;
root verifies all owned births, process receipts, agents and relevant host
compute/notebook/transfer consumers before stopping and verifying shutdown.
