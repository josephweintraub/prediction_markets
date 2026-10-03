# Sports profit-taking: execution-leg and order-event comparison

Status: both grains implemented; full source audit preserved, and its one
settlement discrepancy verified against native events and payout. A narrowly
gated complete-source replay is under validation. Full-history FIFO and
contribution estimation have **not run**. Do not treat fixture results as
empirical findings.

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
The original blocked stage remains unchanged. A fresh source stage will preserve
that excess separately and use matched cash for price and trading profit only
after exact native-case and complete-population replay gates pass.

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

## Remaining gates

1. Finish validating the narrow native-proven SELL-collateral surplus rule and
   full-population replay. Reopen native evidence through estimation publication;
   preserve the original settlement and blocked source stage.
2. Validate a fresh complete source stage. If MERGE appears, verify native
   examples for every observed emitting address before FIFO. The ledger CLI
   requires saved source/native proof rather than a manual assurance.
3. Run full-history FIFO, then both-grain estimation with inherited focal
   filters and literal windows. Reopen saved outputs and independently check
   conservation, support, additivity and headline results.
4. Render the actual completed compact artifacts, compile/visually verify the
   LaTeX source, and open it in native Codex. No real findings report exists yet.

Only the root controls EC2. Shutdown and stopped-state verification are required
after all assigned remote work, production processes and transfers finish.
