#!/bin/bash
set -euo pipefail
root=/home/danielking/h058-filtered-aggregate
while ! test -f "$root/results/candidate-build-complete"; do sleep 5; done
cp /home/danielking/test_collection_filter_execution.py /home/danielking/finite-stddev-20261024/source/vail/python/tests/test_collection_filter_execution.py
sha256sum /home/danielking/test_collection_filter_execution.py > "$root/results/expanded-tests-source.sha256"
"$root/env/bin/python" -m pytest -q /home/danielking/test_collection_filter_execution.py > "$root/results/candidate-expanded-capture-tests.log" 2>&1
for query in ad_sum ad_bundle pl_count; do
 "$root/env/bin/python" /home/danielking/h058-measure.py --query "$query" > "$root/results/candidate-$query.jsonl" 2> "$root/results/candidate-$query.stderr"
done
sudo perf record -o "$root/results/candidate-perf.data" -e cpu-clock -F 99 --call-graph dwarf,8192 -- "$root/env/bin/python" /home/danielking/h058-measure.py --repeat 12 > "$root/results/candidate-profile.jsonl" 2> "$root/results/candidate-profile.stderr"
sudo perf report --stdio -i "$root/results/candidate-perf.data" --no-children -g none > "$root/results/candidate-perf-report.txt" 2>&1
printf 'ready\n' > "$root/results/candidate-measure-complete"
