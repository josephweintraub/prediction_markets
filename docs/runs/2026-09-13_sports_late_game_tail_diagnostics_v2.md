# Sports late-game tail diagnostics v2

**Status:** complete; canonical production run  
**Output root:** `/mnt/data/runs/2026-09-13_sports_late_game_tail_diagnostics_v2`

Version 1 is superseded. It selected D10 with the direct condition `P >= 0.9`,
which did not reproduce the published estimator's floating-point bin edge. Version 2
uses the published expression exactly:
`least(floor(P * 10)::INTEGER, 9) + 1`.

## Reproducibility

- Script: `analysis/sports_game_dynamics/diagnose_late_game_tails.py`
- Focused tests: `tests/test_diagnose_late_game_tails.py`
- Test result: `2 passed in 0.69s`
- Outputs: immutable Parquet summaries for tail estimates, probability bands,
  outcomes, games, wallets, timing thirds, and leave-top-game checks, plus
  `manifest.json` with input, code, environment, method, and output provenance.

## Headline results

All calibration values are percentage points.

| Sport | D1, equal fill | D10, equal fill | D1, equal game | D10, equal game | Tail signs |
|---|---:|---:|---:|---:|---|
| MLB | +0.239 | -0.379 | -0.646 | +0.772 | Reverse under equal-fill weighting only |
| NFL | +3.884 | +1.104 | +5.551 | -3.233 | Not reverse signs under the published equal-fill estimand |
| NBA | +1.212 | -3.986 | +4.670 | -4.388 | Reverse under both weightings |

For NBA, the wrong-way outcomes comprise 55 D1-winning games and 1,887 fills,
and 63 D10-losing games and 3,063 fills. Removing the five games with the largest
absolute equal-fill contributions changes NBA D1/D10 equal-fill calibration to
-0.668/+0.504, while equal-game calibration remains +3.895/-3.724.

The NBA reversal is concentrated in the first and middle thirds of quarter 4 plus
overtime; the last third is near zero or has the opposite sign. No single price band
or wallet dominates. Nominal bin-edge leakage is visible but not material to NBA D1:
excluding it changes +1.212 to +1.190.

MLB phase bins use bought-contract probability and outcome. NFL and NBA phase bins
use home-normalized probability and the home outcome; cross-sport tail comparisons
therefore do not share an identical estimand.
