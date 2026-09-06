#!/bin/bash
set -e
PY=/home/ubuntu/venv/bin/python
cd /home/ubuntu/prediction_markets/analysis/embedding_difficulty
echo "=== liq/horizon disentangling slices ==="
$PY make_liq_horizon_slices.py
echo "=== FLB mature ==="
$PY run_schemes.py --window mature --schemes liqrate liqrate_vint hor_x_liqrate horizon_binary horizon_final2
echo "=== FLB closing ==="
$PY run_schemes.py --window closing --schemes liqrate hor_x_liqrate horizon_binary horizon_final2
echo "=== render report v6 ==="
$PY render_report.py
echo "=== SESSION6 DONE ==="
