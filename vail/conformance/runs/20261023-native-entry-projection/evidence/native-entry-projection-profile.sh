#!/bin/bash
set -euo pipefail
ENTRY_PROJ="$HOME/native-entry-projection"
test -e "$ENTRY_PROJ/independent-complete"
for mode in baseline candidate; do
 if test "$mode" = baseline; then PY="$ENTRY_PROJ/baseline-env/bin/python"; else PY="$ENTRY_PROJ/env/bin/python"; fi
 "$PY" "$HOME/native-entry-projection-partitions.py" > "$ENTRY_PROJ/results/$mode-partitions.json" 2> "$ENTRY_PROJ/results/$mode-partitions.stderr"
 sudo perf record -q -e cpu-clock -F 97 --call-graph dwarf,8192 -o "$ENTRY_PROJ/results/$mode-perf.data" -- "$PY" "$HOME/native-entry-profile.py" entry_sum_entryonly --repeat 2 > "$ENTRY_PROJ/results/$mode-profile-query.jsonl" 2> "$ENTRY_PROJ/results/$mode-profile.stderr"
 sudo perf report --stdio --no-children --percent-limit 0.5 -i "$ENTRY_PROJ/results/$mode-perf.data" > "$ENTRY_PROJ/results/$mode-profile-self.txt" 2> "$ENTRY_PROJ/results/$mode-profile-report.stderr"
 sudo perf report --stdio --children --percent-limit 1 -i "$ENTRY_PROJ/results/$mode-perf.data" > "$ENTRY_PROJ/results/$mode-profile-inclusive.txt" 2>> "$ENTRY_PROJ/results/$mode-profile-report.stderr"
done
touch "$ENTRY_PROJ/profile-complete"
