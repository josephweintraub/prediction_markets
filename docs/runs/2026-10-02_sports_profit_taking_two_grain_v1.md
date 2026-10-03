# Sports profit-taking: execution-leg and order-event comparison

Status: both grains implemented; full source audit preserved, and its one
settlement discrepancy verified against native events and payout. The fresh
complete-source replay and native MERGE readiness have passed. Full-history
FIFO and both-grain contribution estimation are complete. Independent compact
QA passed; the empirical six-page LaTeX report compiled successfully and passed
visual QA. Release packaging and stopped-state verification follow publication.

Contract: [`sports_profit_taking_v1.md`](../analysis_specs/sports_profit_taking_v1.md).
Run root: `/mnt/data/runs/2026-10-02_sports_profit_taking_v1`.

## Frozen comparison

The user approved both versions on 2026-10-02. Matched executions retain one
actual wallet BUY per genuine leg: NORMAL one, MINT two, MERGE none. Order-events
retain one own OrderFilled BUY log, including an active aggregate at quantity
VWAP. Neither version combines an order hash across transactions.

Both use identical full-history FIFO/profit labels, allocated uniformly to
actual legs. Gross quantity, cash and quantity-weighted calibration must
conserve before focal filters. Count/dollar/equal-market estimates can differ
through price averaging, bin membership and support. The decomposition is
observed accounting, not a no-unwinding causal counterfactual.

## Full source audit

Stage `02_full_source_v1` used committed code at `aeb608b`, 16 threads, 100GB
DuckDB memory and a 12GB spill cap. It preserved all original selected logs and
saved irregular records separately. No later stage may consume its accepted
subset while the parent status remains blocked.

| Check | Saved count |
|---|---:|
| Selected transaction/exchange groups | 15,885,730 |
| Original distinct OrderFilled logs | 37,973,994 |
| Exact replays | 0 |
| Accepted batches | 15,885,729 |
| Rejected relevant batches | 1 |
| Scoped orphan logs | 0 |
| Unscoped orphan logs | 0 |
| Frozen tokens without raw history | 0 |

The single rejected batch is `aggregate_received_amount_mismatch`. Its two
original records imply matched quantity 90,000 microcontracts and cash 83,700
micro-USDC, but the active SELL aggregate reports 8,083,700 micro-USDC received.
The discrepancy is exactly 8,000,000 micro-USDC. This is a source observation,
not a diagnosis of the origin of that money.

Stage `03b_rejected_source_native_v1`, from committed code `17d10eb`, used one
native receipt request and two immutable-asset getter calls. The complete two-log
set matches the raw source exactly. Native collateral transfers show 83,700
micro-USDC into the exchange and 8,083,700 out, a net excess payment of 8,000,000.
The native proof does not establish the origin of the opening exchange balance.
The original blocked stage remains unchanged. The fresh verified source stage
preserves that excess separately and uses matched cash for price and trading
profit after exact native-case and complete-population replay gates pass.

## Validated complete replay

Stage `02c_full_source_verified_replay_v1`, from clean commit `a40315a`, completed
with the complete original selected population. It restores original aggregate
refund amounts and both excluded logs from the preserved first extraction,
while recording the original raw fingerprint and exact extraction provenance.

| Check | Saved count |
|---|---:|
| Original own-order logs | 37,973,994 |
| Accepted transaction/exchange batches | 15,885,730 |
| Passive matched legs | 22,088,264 |
| Verified refund batches | 15,658 |
| Native-proven settlement-surplus batches | 1 |
| Separate surplus, micro-USDC | 8,000,000 |
| Rejected groups / scoped orphans / unscoped groups | 0 / 0 / 0 |
| Frozen tokens without history | 0 |

This is complete source recovery, not a completed FIFO or contribution estimate.

## Native MERGE readiness

Stage `03c_full_source_support_v1` reconciled all genuine legs: 5,851,228 NORMAL,
15,626,425 MINT and 610,611 MERGE. The bounded selector used 85 block-column
probes and four wide row groups totaling 30,303,492 compressed bytes. All four
selected transaction/exchange batches were complete and accepted.

Stage `03d_merge_native_receipts_v1` verified four complete native transactions
and nine OrderFilled logs, covering every observed MERGE-emitting address.
The downstream readiness helper returned `verified_native_merge`; the separate
case-specific settlement-surplus proof was reopened and verified. Both gates
are required inputs to FIFO. No audit process remained when FIFO was released.

## FIFO execution monitoring

The ledger agent launched `04_fifo_v1` at `2026-10-03T03:00:38Z` with eight
DuckDB threads, 100GB memory limit and 50,000-row publication batches. Python
FIFO itself is serial. Preflight free space was 27.02GiB against a 20.00GiB
reserve; no other production stage ran concurrently.

The first million actions completed after 118.25 seconds, including initial
validation and sorting. The subsequent snapshot had 31.46GiB RSS and 88.2MB of
flushed tags; this snapshot was later than the checkpoint and is not an exact
per-million compression ratio. Use consecutive checkpoints for throughput,
not core count or the startup-inclusive rate. No estimate exists until complete
FIFO and downstream publication gates pass.

The stage was published by `2026-10-03T04:00:53Z`, with **37,973,994** input and
output actions reconciled across **14,382** markets. The process exited without
an error; no disk spill was created. RSS stayed near 31.5GiB during matching,
and free space after publication was 24.31GiB. The first full history pass was
not restarted or optimized while running. Estimation is serialized after this
completed ledger and its saved native-proof gates.

## Completed two-grain estimation

Stage `05_two_grain_contributions_v1` ran serially after FIFO on clean canonical
commit `a40315a`, using eight DuckDB threads and a 100GB memory limit. It was
published by `2026-10-03T04:24:13Z`; the process exited without errors or disk
spill. Immutable-input and native-evidence checks passed at publication.

| Saved population or grid | Count |
|---|---:|
| Own-order BUY events | 31,827,950 |
| Genuine matched BUY executions | 37,104,078 |
| Probability-bin profiles | 16,200 |
| D10-minus-D1 tail contrasts | 1,620 |
| Final-minus-prior terminal contrasts | 162 |
| Original observation support rows | 540 |
| Physical mechanism summary rows | 270 |
| Grain totals / crossing diagnostics | 18 / 18 |

These BUY counts describe the frozen nine-sport cohort, not the complete
Polymarket universe. Their difference is observation grain, not extra trading
exposure.

Independent compact QA passed exact cross-grain cash and quantity conservation
in all nine sports. The maximum absolute quantity-residual discrepancy divided
by gross quantity was `6.71e-19`. There were zero failures in additive components,
original denominators, the 500-observation/NULL suppression rule, sample nesting,
literal live-thirds support or physical matched/unmatched sale conservation.

## Empirical terminal comparison

The matched-execution, all-trade, per-observation spread changes are:

| Sport | Final 1% minus 95–99%, pp |
|---|---:|
| MLB | +0.892 |
| NFL | -0.498 |
| NBA | +0.062 |
| NHL | -1.908 |
| CBB | -0.456 |
| CFB | -0.328 |
| ATP | -0.262 |
| EPL | +6.378 |
| WNBA | -0.695 |

All all-trade terminal contrasts are supported. The increase is not universal.
EPL's change decomposes into +0.916pp direct, +0.738pp hedge and +4.724pp
remaining contributions, using the original bin denominators. These are
allocations of observed calibration, not causal price effects.

Filtered CFB and WNBA terminal contrasts are withheld in both grains. Most
directions are similar across grains, but filtered ATP changes sign: +0.492pp
for own events versus -0.170pp for matched executions. Counting, price averaging,
bin membership and filters can change a count-weighted contrast while preserving
the underlying BUY quantity and cash. Uncertainty has not been estimated; no
significance claim is made.

## Implementation QA

The two-grain runner requires complete source/FIFO lineage, exact input
fingerprints, native source evidence, binary metadata and exact block times.
It saves complete profiles, tail contrasts, final-1%-minus-preceding-window
components, support, physical mechanism volumes and cross-grain checks.

Aggregate integer outputs use Parquet `DECIMAL(38,0)` rather than lossy conversion
of DuckDB `HUGEINT` to DOUBLE. A fixture verifies exact round trips above
`2^53`. The zero-request complementary FIFO preview returns before walking the
already-offset lot prefix; nonzero allocation logic is unchanged.

The report renderer consumes completed compact artifacts only. It uses native
LaTeX tables and inline isolated-bin figures, explicit descriptive uncertainty,
and separate support at both grains. Synthetic compilation/layout checks are
not evidence of a completed empirical report.

Canonical production-Python validation passed **400 tests in 56.06 seconds**
across all eight scoped modules and the adjacent wallet-exit, tennis-cohort and
tennis-report tests. The sole first-pass failure was an environment-dependent
test, repaired by explicitly simulating a nonproduction host; the guard itself
was not weakened. No full-history FIFO or contribution stage ran during tests.

The native-surplus/replay release then passed **437 tests in 64.92 seconds** on
canonical production Python across the same scoped and adjacent suite. Independent
eight-module QA passed 389 local tests. Exact replay covers original refunds and
all rejected records; a losing 0.95-to-0.93 trade remains losing despite the
separate 8-USDC payment. Changed external native evidence blocks estimator
publication. Both source and downstream proof gates remain mandatory.

## Completed report and release

The immutable `06_report_v1` renderer used completed compact artifacts only.
Its standalone `profit_taking_comparison.tex` compiled successfully with the
native Codex compiler. All six pages were visually inspected: no clipping,
table overflow, connected probability bins or suppressed-at-zero estimates.
The tables reproduce independently checked values and support; the document
ends with the weighting comparison table. Transferred source and compact
outputs match production fingerprints exactly. The source was submitted to
the native Codex editor, and the root repeated native compilation successfully.

The focused renderer and contribution suite passed **90 tests in 10.22 seconds**.
The compact independent reconciliation, fingerprint and layout record is saved
locally as `output/sports_profit_taking_v1/06_report_v1/independent_qa.json`.

The shared release destination is:

`Polymarket Data and Code/learnability_paper_v1/Sports_Profit_Taking_Both_Grains_2026-10-02`

Release only the scoped code/tests/docs archive, the completed compact estimator
outputs and the standalone report/evidence/manifests. Keep original machine
manifests unchanged. Exclude raw/source/FIFO datasets, native receipt/proof
payloads, wallet/lot records, credentials, caches and production logs.

Tested production environment: Python 3.12.3, DuckDB 1.5.0, PyArrow 23.0.1,
Requests 2.32.5 and pytest 9.1.1. Production stages require the canonical Linux
checkout and mounted `/mnt/data`; RPC collection additionally requires
`POLYGON_RPC_URL`. Private production inputs are not included. Synthetic tests
and compact-only report rendering are portable. The standalone LaTeX source
uses inline PGFPlots and requires no external figure files.

The root commits/pushes this scoped record, verifies the fresh-folder upload,
then checks that all assigned work, processes and transfers have finished.
Stopped-state verification is a post-upload lifecycle gate, not a completed
claim inside a package created before that check.

Only the root controls EC2. Shutdown and stopped-state verification are required
after all assigned remote work, production processes and transfers finish.
