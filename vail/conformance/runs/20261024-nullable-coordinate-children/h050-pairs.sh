#!/bin/bash
set -euo pipefail
R=/home/danielking/h050-nullable-coordinate-runs
for round in 1 2; do
 if [ "$round" = 1 ]; then order='baseline candidate'; else order='candidate baseline'; fi
 for version in $order; do
  if [ "$version" = baseline ]; then P=/home/danielking/aligned-scalar-broadcast/env/bin/python; else P="$R/env/bin/python"; fi
  for query in single bundle; do
   OPENBLAS_NUM_THREADS=1 "$P" "$R/measure.py" --query "$query" --biallelic --repeat 4 > "$R/results/paired-$round-$version-$query.jsonl" 2> "$R/results/paired-$round-$version-$query.stderr"
  done
  for mode in allvalid zero fragmented runs huge; do
   OPENBLAS_NUM_THREADS=1 "$P" "$R/h050-control.py" --mode "$mode" --repeat 5 > "$R/results/control-$round-$version-$mode.jsonl" 2> "$R/results/control-$round-$version-$mode.stderr"
  done
 done
done
touch "$R/paired-complete"
