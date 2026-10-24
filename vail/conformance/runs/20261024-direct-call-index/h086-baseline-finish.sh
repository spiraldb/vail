#!/bin/bash
set -euo pipefail
root=/home/danielking/h086-direct-call-index
"$root/baseline-env/bin/python" /home/danielking/h086-measure.py --query row --repeat 6 > "$root/results/baseline-row.jsonl" 2> "$root/results/baseline-row.stderr"
sudo perf record -o "$root/results/baseline-perf.data" -e cpu-clock -F 99 --call-graph dwarf,8192 -- "$root/baseline-env/bin/python" /home/danielking/h086-measure.py --repeat 15 > "$root/results/baseline-profile.jsonl" 2> "$root/results/baseline-profile.stderr" || echo 'perf failed' > "$root/results/baseline-profile-failed"
sudo perf report --stdio -i "$root/results/baseline-perf.data" --no-children -g none > "$root/results/baseline-perf-report.txt" 2>&1 || true
printf 'ready\n' > "$root/results/baseline-measure-complete"
