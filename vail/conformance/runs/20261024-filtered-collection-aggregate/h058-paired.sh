#!/bin/bash
set -euo pipefail
root=/home/danielking/h058-filtered-aggregate
mkdir -p "$root/results/paired"
for query in ad_sum ad_bundle pl_count; do
 index=0
 for version in baseline candidate candidate baseline; do
  if test "$version" = baseline; then py=/home/danielking/finite-stddev-20261024/base-env/bin/python; else py="$root/env/bin/python"; fi
  "$py" /home/danielking/h058-measure.py --query "$query" --repeat 4 > "$root/results/paired/$query-$index-$version.jsonl" 2> "$root/results/paired/$query-$index-$version.stderr"
  index=$((index + 1))
 done
done
printf 'ready\n' > "$root/results/paired-complete"
