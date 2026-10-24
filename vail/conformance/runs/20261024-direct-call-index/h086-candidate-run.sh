#!/bin/bash
set -euo pipefail
root=/home/danielking/h086-direct-call-index
while ! test -f "$root/results/final-build-complete"; do sleep 5; done
"$root/candidate-env/bin/python" -m pytest -q /home/danielking/parallel-vcf-export/source/vail/python/tests/test_split_multi_hts.py /home/danielking/parallel-vcf-export/source/vail/python/tests/test_genotype_likelihoods.py > "$root/results/candidate-adjacent-tests.log" 2>&1
for query in first last dynamic row; do
 "$root/candidate-env/bin/python" /home/danielking/h086-measure.py --query "$query" --repeat 6 > "$root/results/candidate-$query.jsonl" 2> "$root/results/candidate-$query.stderr"
done
sudo perf record -o "$root/results/candidate-perf.data" -e cpu-clock -F 99 --call-graph dwarf,8192 -- "$root/candidate-env/bin/python" /home/danielking/h086-measure.py --repeat 15 > "$root/results/candidate-profile.jsonl" 2> "$root/results/candidate-profile.stderr" || echo 'perf failed' > "$root/results/candidate-profile-failed"
sudo perf report --stdio -i "$root/results/candidate-perf.data" --no-children -g none > "$root/results/candidate-perf-report.txt" 2>&1 || true
for version in baseline candidate; do
 sudo perf stat -o "$root/results/$version-hardware-stat.txt" -e cycles,instructions,cache-references,cache-misses -- "$root/$version-env/bin/python" /home/danielking/h086-measure.py --repeat 4 > "$root/results/$version-hardware-query.jsonl" 2> "$root/results/$version-hardware.stderr" || true
done
printf 'ready\n' > "$root/results/candidate-measure-complete"
