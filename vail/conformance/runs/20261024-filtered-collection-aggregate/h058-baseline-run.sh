#!/bin/bash
set -euo pipefail
root=/home/danielking/h058-filtered-aggregate
base=/home/danielking/finite-stddev-20261024/base-env
mkdir -p "$root/results"
cp /home/danielking/finite-stddev-20261024/evidence/provenance.json "$root/results/baseline-inherited-provenance.json"
testsrc=/home/danielking/finite-stddev-20261024/source/vail
set +e
"$base/bin/python" -m pytest -q /home/danielking/test_collection_filter_execution.py "$testsrc/python/tests/test_collection_aggregate.py" "$testsrc/python/tests/test_array_agg_coordinate_callbacks.py" "$testsrc/python/tests/test_matrix_array_agg.py" "$testsrc/python/tests/test_randomness.py" "$testsrc/python/tests/test_reduction_expression_sharing.py" --junitxml "$root/results/baseline-tests.xml" > "$root/results/baseline-public-tests.log" 2>&1
status=$?
set -e
if test "$status" -gt 1; then exit "$status"; fi
printf '%s\n' "$status" > "$root/results/baseline-pytest-exitcode"
for query in ad_sum ad_bundle pl_count; do
 "$base/bin/python" /home/danielking/h058-measure.py --query "$query" > "$root/results/baseline-$query.jsonl" 2> "$root/results/baseline-$query.stderr"
done
sudo perf record -o "$root/results/baseline-perf.data" -e cpu-clock -F 99 --call-graph dwarf,8192 -- "$base/bin/python" /home/danielking/h058-measure.py --repeat 12 > "$root/results/baseline-profile.jsonl" 2> "$root/results/baseline-profile.stderr"
sudo perf report --stdio -i "$root/results/baseline-perf.data" --no-children -g none > "$root/results/baseline-perf-report.txt" 2>&1
printf 'ready\n' > "$root/results/baseline-complete"
