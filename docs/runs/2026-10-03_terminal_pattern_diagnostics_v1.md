# Terminal-pattern alternatives diagnostics

Status: source and fixture QA in progress; no production findings yet.

## Contract

See [the frozen analysis spec](../analysis_specs/terminal_pattern_diagnostics_v1.md).
Preserve the completed profit-taking analysis, its source/ledger stages and the
open LaTeX report. Reuse the exact completed stage-05 inputs and reproduce every
primary tail cell and terminal contrast before optional diagnostics.

New run destination:
`/mnt/data/runs/2026-10-03_terminal_pattern_diagnostics_v1/01_diagnostics`.

Source: `analysis/diagnostics/build_terminal_pattern_diagnostics.py`.
Fixtures: `tests/test_terminal_pattern_diagnostics.py`.

Primary comparison: final normalized `[.99,1]` minus preceding `[.95,.99)`.
Nine inherited resolved moneyline sports, both actual-BUY grains, three original
samples and three original weights. All aggregate estimates retain the
500-original-observation support gate. This stage estimates no uncertainty or
causal effect. Literal post-end windows are diagnostic only.

## Publication and lifecycle

Transfer only the module's compact-output allowlist and summary/manifest.
Market/event membership tables remain on EC2; wallet histories are not exported.
No Dropbox action is authorized by this investigation. Root alone manages
EC2, confirms no active workload remains, then stops and verifies stopped.

Production command, committed source, exact tests, reconciliation evidence,
findings and final lifecycle state will be recorded after computation and QA.
