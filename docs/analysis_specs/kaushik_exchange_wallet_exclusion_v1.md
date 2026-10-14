# Exchange-wallet exclusion in the Kaushik Polymarket replication

Status: count and locator-mask audit accepted, 14 October 2026. No revised
coefficient estimates have been fitted or admitted. Preserve both accepted original runs.

## Question and sole sample change

Test Kaushik's proposed exclusion of archive records for which either source
wallet is a Polymarket exchange contract. Exchange-facing active-order summary
logs can overlap constituent passive-order fills. This is a record-grain
exclusion, not a bot filter or a certification of native unique fills.

The audit uses the accepted full-date input generation at
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2` and its bound
repaired CLEAN source. It reports both all-available-date and the unchanged
execution-before-25-March-2026 populations. The prior exact full-to-cutoff subset
audit supports using the latter as a diagnostic for the accepted cutoff run.
All other eligibility rules, direction conventions, clocks, prices, outcomes,
weights, provider coverage and model definitions remain unchanged.

## Frozen address rule

Exclude a record if `lower(proxyWallet)` or `lower(counterparty)` exactly equals
one of the following source-generation addresses. Do not trim strings; null or
blank fields do not match. Do not filter the native emitting-contract field,
which also identifies genuine constituent fills.

| Generation | Contract address |
| --- | --- |
| Legacy | `0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e` |
| Legacy negative risk | `0xc5d563a36ae78145c45a50134d48a1215220f80a` |
| V2 | `0xe111180000d2663c0091e4f400237545b87b996b` |
| V2 negative risk | `0xe2222d279d744050d28e00520010520000310f59` |

The current V3 contract `0xe3333700ca9d93003f00f0f71f8515005f6c00aa` is a
diagnostic-only candidate outside this reviewed historical source map. Any
eligible match blocks admission instead of silently expanding the policy.
Kaushik's exact address list has been requested but not supplied; equivalence to
his own implementation remains unverified.

Primary references: [legacy matching implementation](https://github.com/Polymarket/ctf-exchange/blob/main/src/exchange/mixins/Trading.sol),
[legacy SDK configuration](https://github.com/Polymarket/py-clob-client/blob/main/py_clob_client/config.py),
[V2 implementation and deployments](https://github.com/Polymarket/ctf-exchange-v2),
and [current contract directory](https://docs.polymarket.com/resources/contracts).

## Count-first admission and reconciliation

Run `scripts/audit_kaushik_exchange_wallets.py` only on the guarded production
host, from committed source with a separately reviewed fresh metadata/resource
preflight. Its source-bound policy and capacity constants are the executable
contract. It does not materialize a replacement analysis base or estimate models.

For every one of the 44 monthly source partitions:

- Preserve the original zero-based physical Parquet row number before filtering.
- Save only source locators, matched public exchange addresses and match flags.
- Reopen masks and prove exact source-locator/address-field equality.
- Partition the accepted 12-field base into retained and removed multisets.
  For a nonempty wallet mask, validate identical 12-field schemas and unique,
  non-null, in-range source locators in both relations, then compare every
  field in an executed full outer join on `(source_month, source_ordinal)`.
  Require both exact multiset difference counts to be zero. Under those key
  guards these counts equal bidirectional `EXCEPT ALL` counts; small oracle
  fixtures compare both algorithms and demonstrate why duplicates must block.
  The saved record distinguishes this method from literal `EXCEPT ALL`
  execution. For an exactly empty source-validated mask, prove the
  same zero differences by exact identity: the unique claims join and direct
  original 12-field projection preserve every reference record, group counts
  equal the base count, all match fields are null and no removal flag is true.
  Record which proof was used. Keep reference/source content reopening and all
  original validity and coverage checks in both paths.
- Reconcile raw matches absent from the base to its original first-failure
  exclusion records, rather than treating them as missing valid observations.
- Report original, retained and removed counts for all-role and primary-taker
  populations, all prices, fixed D1/D10 tails, common duration tails, original
  lifespan above one day and remaining duration above one day.
- Retain overlapping wallet-field hits diagnostically but count each source
  record once. Reconcile the maker-BUY plus taker-BUY partition.
- Reopen source, metadata and reference bindings unchanged before publication.

Masked rows remain on EC2. Small audit/acceptance summaries provide evidence for
the next estimation decision. If a scope has zero eligible removals, exact
identity proves this exclusion cannot change its earlier estimates. Otherwise a
fresh admitted filtered-input and estimation stage is required; old results must
not be relabeled as filtered. Storage admission is assessed before any full-copy
rebuild. This audit does not resolve replay multiplicity, archive-inferred action
direction or resolution censoring.

## Accepted count result and next boundary

The accepted v4 audit removes 110,520,928 primary-taker archive records from
1,000,970,616, retaining 890,449,688. The primary common-duration tail sample
falls from 252,319,669 to 226,809,232. All eligible removals occur in recorded
April--June 2026 partitions. The before-25-March cutoff population has zero
removals in every role/metric, so the exclusion cannot change its earlier
estimates with the accepted source, model and provider bindings held fixed.
The previously accepted exact cutoff-subset comparison binds the same full
input generation and all twelve fields.

Raw matched source records total 230,985,982. Of these, 9,944,126 were already
ineligible and reconcile to original exclusions; 221,041,856 eligible all-role
records are removed. Each archive record is counted once, not once per matching
wallet field. These are record-grain counts, not a native unique-fill census.

Source-code evidence explains the observed timing: the upstream canonical
filter excludes native `taker` values at the two old addresses, but not the
later V2 addresses. See `scripts/audit_polymarket_lineage.py:536`. The wallet
repair preserves membership under an either-wallet test. This exclusion is
scoped to the replication; no original raw source or general trade-table
writer was changed.

The original physical input stage occupies 19,525,404,268 bytes. A second
physical base would leave insufficient space for the estimator's unchanged
60 GB reservation. A versioned parent-base-plus-mask read-only layer has been
requested, not silently adopted. Full-date coefficient refitting remains
pending that storage/architecture choice. No affected earlier estimate may be
relabeled as exchange-filtered before a fresh accepted fit.

Accepted artifacts and execution/QA bindings are recorded in
[`2026-10-14_kaushik_exchange_wallet_audit_v2.md`](../runs/2026-10-14_kaushik_exchange_wallet_audit_v2.md).
