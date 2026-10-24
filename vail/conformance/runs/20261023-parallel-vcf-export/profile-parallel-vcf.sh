#!/bin/bash
set -euo pipefail
b=/home/danielking/parallel-vcf-export
test -f "$b/results/measurement-complete"
export VAIL_NUM_THREADS=32 RAYON_NUM_THREADS=32 TOKIO_WORKER_THREADS=32 OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=1
for storage in native legacy; do
if ! test -s "$b/results/candidate-$storage.perf.data"; then
sudo --preserve-env=VAIL_NUM_THREADS,RAYON_NUM_THREADS,TOKIO_WORKER_THREADS,OMP_NUM_THREADS,OPENBLAS_NUM_THREADS perf record -e cpu-clock -F 99 -g --call-graph dwarf,8192 -o "$b/results/candidate-$storage.perf.data" -- "$b/env/bin/python" /home/danielking/parallel-vcf-measure.py candidate "$storage" profile
fi
sudo perf report -f -i "$b/results/candidate-$storage.perf.data" --stdio --no-children --percent-limit 0.5 > "$b/results/candidate-$storage.perf.txt"
sudo perf report -f -i "$b/results/candidate-$storage.perf.data" --stdio --no-children --sort comm,pid --percent-limit 0.5 > "$b/results/candidate-$storage-threads.perf.txt"
sudo chown -R danielking:danielking "$b/results/candidate-$storage-profile" "$b/results/candidate-$storage.perf.data"
done
printf '%s\n' complete > "$b/results/profile-complete"
