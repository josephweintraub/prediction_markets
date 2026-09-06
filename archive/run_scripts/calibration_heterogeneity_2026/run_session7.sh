#!/bin/bash
# Session 7: full-lifetime window for the liquidity/maturity analyses.
set -e
PY=/home/ubuntu/venv/bin/python
cd /home/ubuntu/prediction_markets/analysis/embedding_difficulty
echo "=== rebuild base tables (adds full window) ==="
$PY build_flb_base.py
echo "=== FLB full window ==="
$PY run_schemes.py --window full --schemes all horizon liqrate liqrate_vint hor_x_liqrate horizon_binary horizon_final2 liq1d liq7d liq30d hor_x_liq1d
echo "=== render brief (full) ==="
$PY render_liq_maturity_brief.py
echo "=== SESSION7 DONE ==="
