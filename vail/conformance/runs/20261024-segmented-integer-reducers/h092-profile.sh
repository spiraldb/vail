#!/bin/bash
set -euo pipefail
root=/home/danielking/h092-segmented-reducers
base=/home/danielking/filtered-collection-integration-20261024/env/bin/python
candidate="$root/env/bin/python"
for version in baseline candidate; do
 if test "$version" = baseline; then interpreter="$base"; else interpreter="$candidate"; fi
 sudo perf record -F 99 -e cpu-clock -g --call-graph dwarf -o "$root/results/$version-perf.data" -- "$interpreter" /home/danielking/h058-measure.py --query ad_sum --repeat 12 > "$root/results/$version-profile.jsonl" 2> "$root/results/$version-perf-record.log"
 sudo perf report -f -i "$root/results/$version-perf.data" --stdio --no-children -g none --percent-limit 0.01 > "$root/results/$version-perf-flat.txt"
 sudo perf report -f -i "$root/results/$version-perf.data" --stdio --children --percent-limit 0.1 > "$root/results/$version-perf-callgraph.txt"
 gzip -c "$root/results/$version-perf-flat.txt" > "$root/results/$version-perf-flat.txt.gz"
 gzip -c "$root/results/$version-perf-callgraph.txt" > "$root/results/$version-perf-callgraph.txt.gz"
done
printf 'ready\n' > "$root/results/measurements-complete"
