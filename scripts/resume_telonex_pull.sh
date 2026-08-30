#!/bin/bash
# Resume driver: finish t1k, then the $0-1k universe in four volume bands so
# raw files never outgrow remaining disk. Same stage pattern as
# run_full_telonex_pull.sh (pull -> merge -> upload -> clear raw).
set -euo pipefail
cd /mnt/data/telonex
PY=/home/ubuntu/venv/bin/python
SC=/home/ubuntu/prediction_markets/scripts
ST=/mnt/data/telonex/full_pull_status.txt
DBX="dropbox:Polymarket Data and Code/telonex"

step() { echo "$(date -u '+%F %T') $1" >> "$ST"; }

run_tranche() {
  local name=$1 floor=$2 ceil=$3
  local free
  free=$(df -BG --output=avail /mnt/data | tail -1 | tr -dc 0-9)
  if [ "$free" -lt 30 ]; then step "ABORT: only ${free}G free before $name"; exit 2; fi
  step "PULL $name start (floor=$floor ceiling=$ceil, ${free}G free)"
  $PY "$SC/pull_telonex_quotes.py" --floor "$floor" --ceiling "$ceil" \
      --concurrency 14 >> "pull_$name.log" 2>&1
  step "PULL $name done: $(tail -1 pull_$name.log)"
  step "MERGE $name start"
  $PY "$SC/merge_telonex_tranche.py" --name "$name" >> "merge_$name.log" 2>&1
  step "MERGE $name done: $(tail -1 merge_$name.log)"
  rclone copy /mnt/data/telonex/quotes_ticks "$DBX/quotes_ticks/" --transfers 8 -q
  step "UPLOAD $name done"
  rm -rf /mnt/data/telonex/quotes_raw
  step "RAW $name cleared"
}

run_tranche t1k  1000 10000
run_tranche t0a  500  1000
run_tranche t0b  200  500
run_tranche t0c  50   200
run_tranche t0d  0    50

step "DAILY rebuild start"
$PY "$SC/rebuild_telonex_daily.py" >> daily.log 2>&1
step "DAILY rebuild done: $(tail -1 daily.log)"
rclone copy /mnt/data/telonex/quotes_daily.parquet "$DBX/" -q
step "ALL DONE"
