#!/bin/bash
# Session 4: horizon / recurrence / anchorability / per-category novelty tail
# (collaborator follow-ups, 2026-08-15), on the fixed plumbing.
set -e
PY=/home/ubuntu/venv/bin/python
cd /home/ubuntu/prediction_markets/analysis/embedding_difficulty
echo "=== horizon & proxy slices ==="
$PY make_horizon_slices.py
echo "=== FLB mature ==="
$PY run_schemes.py --window mature --schemes horizon horizon_cat recurrence anchor novtail_cat
echo "=== FLB closing (horizon) ==="
$PY run_schemes.py --window closing --schemes horizon horizon_cat
echo "=== render report v4 ==="
$PY render_report.py
echo "=== SESSION4 DONE ==="
