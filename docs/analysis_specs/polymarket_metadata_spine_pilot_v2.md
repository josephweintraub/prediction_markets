# Metadata terminal acceptance and bounded spine feasibility v2

Status: local implementation and synthetic QA only. No production admission.

## Formal raw-pagination acceptance

`scripts/accept_polymarket_metadata_v6_terminal_v1.py` is a separate root-executed,
zero-request stage. It accepts only the fixed completed metadata-v6 run and
producer, with exact manifest, completion, launch and exit bindings. It derives
the historical external-control closure through the authenticated v6 dispatch,
v6 external ledger and v5 terminal/ledger chain, rather than executing an old
builder or trusting a checkpoint Boolean.

Limits are 600 seconds, 8 GiB address space, no spill, a 20 GB free-disk floor and
16 GiB of charged bound-input reads. Every new artifact is at most 64 KiB; all
new code, tests, this contract, admission, controls, log and outputs together
fit 256 KiB. These bytes remain within the existing metadata control/aggregate
ceilings and the same 2 MiB external-control pool. There is no new allowance.

The acceptor authenticates retained compressed artifacts twice, verifies exact
file sets, and reconciles all 22,150 new request/finish/page records, source,
cursor, terminal, count and charge bindings. Historical expanded-body semantic
replay is not repeated. No token, payout, category or native-demand universe is
certified by raw pagination.

The fresh process census checks the old process receipts and current owned
caller/child births, groups and sessions. Only the acceptor's bound birth and
ancestry may remain live as research compute. Unclassified processes, including
unrelated userspace processes without an executable, block acceptance. Root
must freshly verify host, mounted volume and source before this stage. No
historical device number is treated as current physical identity.

## Interface

The production CLI requires `--admission`, `--admission-sha256`,
`--expected-head` and a fresh `--run-dir`, using the canonical Linux checkout
and production interpreter. The pure `prepare_candidate(terminal, dispatch,
reader, current_source)` helper derives the exact historical controls and old
process roster without network access or old entrypoint execution. Root
constructs and hashes a fresh admission with exact `caps`, terminal/dispatch
refs, current boot/device, prior usage, new-artifact refs and stdout reservation.
`prior_read_bytes` debits the exact already-incurred candidate `Reader.bytes`
against the same 16-GiB input ceiling, after the admission's own read is charged.
The preparation receipt carrying this number is a bound new artifact. Local
journal, process and Git reads reserve a bounded allowance before physical I/O;
completed reads charge their known byte lengths, and interrupted reads retain
their full conservative allowance.
The stdout reservation is 1,024 to 65,536 bytes; it covers the fixed small
success message within, not in addition to, the 256-KiB stage allowance.
The admission schema is `polymarket_metadata_v6_terminal_acceptance_admission_v1`;
status is `root_metadata6_terminal_reopen_admitted`.

Success publishes only `root_drain.json`, `RESULT.json` and `COMPLETE.json`.
The result schema is `polymarket_metadata_v6_terminal_acceptance_v1`; status is
`root_reopened_completed_raw_pagination_only`. `prior_usage` is cumulative and
includes the full new 600-second and 256-KiB control reservations.
`prior_usage_as_of_utc` records the UTC at which that cumulative carry is
established. Later stages add only subsequent nonoverlapping preparation time,
plus their own fixed reservations. They must bind RESULT and COMPLETE, not a
Boolean or RESULT alone. The exact reopened external-reference union is exposed
as `shared_external_controls`; the acceptor output directory is separately
bound as a whole by the next consumer.

## Deferred bounded spine feasibility

The next versioned pilot remains nonpublishing and stale/nonrepresentative:
five first pages, at most 1,400 rows, 256 MiB charged input reads, 128 MB outputs,
600 seconds and no API. It will consume formal terminal acceptance and the
preceding source-build stage's actual cumulative usage. It must preserve raw
identities and issues, explicit Boolean winner evidence only, literal saved
null/Unclassified labels and the frozen category reconstruction rules. One
initial saved-label read and one final content reopen avoid the v1 triple-read
input-cap failure. Full global uniqueness, lifecycle/final-outcome conflict
resolution and native-demand reconciliation require a later separately reviewed
publisher; this pilot does not admit a scientific sample.
