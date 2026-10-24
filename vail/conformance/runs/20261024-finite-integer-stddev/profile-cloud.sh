#!/bin/bash
set -euo pipefail
R="$HOME/finite-stddev-20261024"
while [ ! -e "$R/complete" ]; do
 if ! pgrep -f '^bash run-h043-cloud.sh$' >/dev/null; then echo 'Main pipeline is no longer running without completion'; exit 1; fi
 sleep 15
done
for mode in base candidate; do
 "$R/$mode-env/bin/python" "$R/controls.py" stats --path "$R/data.ht" > "$R/evidence/$mode-fullstats.jsonl"
 "$R/$mode-env/bin/python" "$R/controls.py" float --path "$R/data.ht" > "$R/evidence/$mode-float.jsonl"
 sudo -n perf stat -e cycles,instructions,cache-misses -o "$R/evidence/$mode-hardware-counters.txt" -- "$R/$mode-env/bin/python" "$R/measure.py" global --path "$R/data.ht" --repeat 12 > "$R/evidence/$mode-hardware-workload.jsonl" 2> "$R/evidence/$mode-hardware-errors.txt" || true
 sudo -n perf stat -e task-clock,context-switches,cpu-migrations,page-faults -o "$R/evidence/$mode-software-counters.txt" -- "$R/$mode-env/bin/python" "$R/measure.py" global --path "$R/data.ht" --repeat 12 > "$R/evidence/$mode-software-workload.jsonl"
 sudo -n perf record -e cpu-clock -F 99 --call-graph dwarf -o "$R/evidence/$mode.perf.data" -- "$R/$mode-env/bin/python" "$R/measure.py" global --path "$R/data.ht" --repeat 24 > "$R/evidence/$mode-profile-workload.jsonl" 2> "$R/evidence/$mode-profile-record.txt"
 sudo -n perf report --stdio --no-children -i "$R/evidence/$mode.perf.data" > "$R/evidence/$mode-profile-report.txt"
done
sudo -n chown -R danielking:danielking "$R/evidence"
touch "$R/post-complete"
