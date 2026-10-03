# Sports profit-taking: implementation and bounded source audit

Status: bounded audits complete; full history reconstruction and calibration
estimation **not run**. Final calibration observation grain awaits user choice.

Contract: [`sports_profit_taking_v1.md`](../analysis_specs/sports_profit_taking_v1.md).
Immutable EC2 root: `/mnt/data/runs/2026-10-02_sports_profit_taking_v1`.
Local review copies contain compact JSON summaries/manifests only, under
`output/sports_profit_taking_v1`. Raw logs and receipt evidence remain on EC2.

## Saved evidence

| Stage | Scope | Result |
|---|---|---|
| `01_source_pilot` | 60 distinct OrderFilled logs, 25 transaction/exchange groups | All 25 batches reconciled; no replays, rejected batches or scoped orphan logs |
| `01b_source_receipts` | 10 sampled transactions, 25 native OrderFilled logs | All 10 receipts verified; normalized fields and complete exchange log sets matched |
| `01c_source_support` | Saved audit and receipt artifacts only | Protocol/address/mechanism coverage and read-budget summary |
| `01d_committed_source_support` | Same saved inputs, rerun with committed code | Regenerated coverage summary of the 10 saved verified receipts; no raw rescan or new RPC collection |

| Protocol | Pilot batches | NORMAL legs | MINT legs | Refund batches | Nonzero-fee batches |
|---|---:|---:|---:|---:|---:|
| Legacy | 12 | 6 | 12 | 1 | 9 |
| V2 | 13 | 3 | 14 | 0 | 13 |

Native receipt validation covers all four deployed exchange addresses and both
generations, including a legacy refund and nonzero fees. No MERGE occurred in
the native pilot. MERGE is covered by primary contract-source review and
synthetic tests, not native sample evidence. The pilot is not a population
coverage estimate or evidence of profitable unwinding.

The raw source footer records 1,541,318,092 rows and blocks 35,896,869 through
88,978,537. Raw V2 events use side/token fields rather than the legacy asset-ID
event ABI; the sampled normalized 12-field rows match the native receipts.
Legacy BUY fees deduct outcome tokens, whereas V2 BUY fees add collateral cost.
Both SELL fee rules deduct collateral proceeds. Gross calibration prices remain
separate from fee-adjusted acquisition costs and realized proceeds.

## Bounded read policy

The source pilot read 18 discovery row groups, probed 207 row groups' block
columns, and read complete wide fields from 14 row groups. The latter accounted
for 243,035,724 compressed bytes, below the 1-GiB budget. The explicit complete
probe cap was increased from 96 to 256 after footer intervals for the selected
blocks exceeded the first cap. Earlier unknown-version and row-group-cap checks
stopped before publishing a stage. No full raw-source scan occurred.

## Implementation and independent QA

- `profit_taking_actions.py`: verified own-order decoding, exact replay and batch
  reconciliation, legacy/V2 fee dispatch, integer FIFO with rational costs,
  direct-sale and complementary-purchase tags without eventual-outcome selection.
- `build_profit_taking_source.py`: proposed two-pass complete selected-transaction
  recovery with preserved irregular records. Not executed on the full source.
- `build_profit_taking_ledger.py`: streamed physical trade-implied FIFO; requires
  a complete source manifest, exact artifact fingerprint/count and zero scoped
  rejections/orphans. Not executed on the full sports history.
- `attribute_profit_taking_buys.py` and `profit_taking_contribution.py`: synthetic
  own-order-VWAP prototypes, original-denominator additive calibration components.
  They are not approved production replacements for the prior matched-trade unit.

Independent QA found and repaired zero-net legacy acquisitions and a potential
partial-source-stage bypass into FIFO. Production entrypoints require the
canonical Linux checkout and mounted research volume. No counterparty direction
is inferred from a passive order alone, no FIFO lot becomes another fill, and
missing opening balances/nontrade movements remain explicit.

Canonical implementation commit: `84454993aa2516f0b9aeac93b623e03d5f0f106e`
on `codex/sports-profit-taking`. The source pilot and receipt collection preceded
that commit; the committed support stage summarizes their saved evidence only.
Production-Python validation passed **268 tests in 31.39 seconds**, covering the
six new modules and adjacent wallet-exit, tennis-cohort and report tests. No
full-history reconstruction, calibration results or uncertainty estimates exist
for this new analysis yet.

## Next gates

1. Confirm matched-trade versus whole-order calibration grain and adapt the
   profitability/attribution unit consistently, preserving genuine partial fills.
2. Use the reviewed committed code to validate a fresh full source stage, within the saved
   disk/spill limits. Stop on scoped orphan or rejected batches rather than
   estimating from an accepted subset without approval.
3. If MERGE appears in the full source, add bounded native evidence before
   treating its recovery as production-verified.
4. Run full-history FIFO, then apply focal sample/time filters. Quantify the
   observed final-1%-versus-preceding-window change through additive direct-sale,
   complementary-hedge and remaining components. Do not call this a no-unwinding
   causal counterfactual.
5. Produce the data-first LaTeX findings report only from completed saved stages.

Only the root manages EC2. It must stop and verify the instance after all tests,
data handling and transfers finish; no cloud work remains assigned while awaiting
the user decision.
