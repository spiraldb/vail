#!/bin/bash
set -euo pipefail
root=/home/danielking/h092-boolean-reducers
base=/home/danielking/h092-segmented-reducers/env/bin/python
candidate="$root/env/bin/python"
for pass in A B B A; do
 if test "$pass" = A; then version=baseline; interpreter="$base"; else version=candidate; interpreter="$candidate"; fi
 stamp=$(date +%s%N)
 for query in ad_any pl_bundle; do
  "$interpreter" /home/danielking/h092-boolean-measure.py --query "$query" --repeat 4 > "$root/results/paired-$version-$stamp-$query.jsonl"
 done
done
for version in baseline candidate; do
 if test "$version" = baseline; then interpreter="$base"; else interpreter="$candidate"; fi
 sudo perf record -F 99 -e cpu-clock -g --call-graph dwarf -o "$root/results/$version-perf.data" -- "$interpreter" /home/danielking/h092-boolean-measure.py --query pl_bundle --repeat 12 > "$root/results/$version-profile.jsonl" 2> "$root/results/$version-perf-record.log"
 sudo perf report -f -i "$root/results/$version-perf.data" --stdio --no-children -g none --percent-limit 0.01 > "$root/results/$version-perf-flat.txt"
 sudo perf report -f -i "$root/results/$version-perf.data" --stdio --children --percent-limit 0.1 > "$root/results/$version-perf-callgraph.txt"
 gzip -c "$root/results/$version-perf-flat.txt" > "$root/results/$version-perf-flat.txt.gz"
 gzip -c "$root/results/$version-perf-callgraph.txt" > "$root/results/$version-perf-callgraph.txt.gz"
done
printf 'ready\n' > "$root/results/measurements-complete"
