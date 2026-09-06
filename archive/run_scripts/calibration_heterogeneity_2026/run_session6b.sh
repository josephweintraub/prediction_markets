#!/bin/bash
# Session 6b: fixed-window time liquidity (1d/7d/30d) — fires after session 6.
set -e
PY=/home/ubuntu/venv/bin/python
cd /home/ubuntu/prediction_markets/analysis/embedding_difficulty
echo "=== waiting for session 6 ==="
until grep -q "SESSION6 DONE" /mnt/data/ed_session6.log; do sleep 60; done
echo "=== fixed-window liquidity slices ==="
$PY make_timeliq_slices.py
echo "=== FLB mature ==="
$PY run_schemes.py --window mature --schemes liq1d liq7d liq30d hor_x_liq1d
echo "=== FLB closing (cross) ==="
$PY run_schemes.py --window closing --schemes hor_x_liq1d
echo "=== render report v6b ==="
$PY render_report.py
echo "=== SESSION6B DONE ==="
