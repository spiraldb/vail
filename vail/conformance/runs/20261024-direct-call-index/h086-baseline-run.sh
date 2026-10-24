#!/bin/bash
set -euo pipefail
root=/home/danielking/h086-direct-call-index
while ! test -f "$root/results/baseline-build-complete"; do sleep 5; done
"$root/baseline-env/bin/python" -m pytest -q /home/danielking/test_call_index.py /home/danielking/parallel-vcf-export/source/vail/python/tests/test_call_value.py /home/danielking/parallel-vcf-export/source/vail/python/tests/test_genotype_index_call.py > "$root/results/baseline-public-tests.log" 2>&1
for query in first last dynamic row; do
 "$root/baseline-env/bin/python" /home/danielking/h086-measure.py --query "$query" --repeat 6 > "$root/results/baseline-$query.jsonl" 2> "$root/results/baseline-$query.stderr"
done
sudo perf record -o "$root/results/baseline-perf.data" -e cpu-clock -F 99 --call-graph dwarf,8192 -- "$root/baseline-env/bin/python" /home/danielking/h086-measure.py --repeat 15 > "$root/results/baseline-profile.jsonl" 2> "$root/results/baseline-profile.stderr" || echo 'perf failed' > "$root/results/baseline-profile-failed"
sudo perf report --stdio -i "$root/results/baseline-perf.data" --no-children -g none > "$root/results/baseline-perf-report.txt" 2>&1 || true
printf 'ready\n' > "$root/results/baseline-measure-complete"
