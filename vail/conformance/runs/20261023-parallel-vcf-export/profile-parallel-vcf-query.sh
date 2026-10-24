#!/bin/bash
set -euo pipefail
b=/home/danielking/parallel-vcf-export
test -f "$b/results/measurement-complete"
export VAIL_NUM_THREADS=32 RAYON_NUM_THREADS=32 TOKIO_WORKER_THREADS=32 OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=1
for storage in native legacy; do
sudo --preserve-env=VAIL_NUM_THREADS,RAYON_NUM_THREADS,TOKIO_WORKER_THREADS,OMP_NUM_THREADS,OPENBLAS_NUM_THREADS perf record -e cpu-clock -F 99 -g --call-graph dwarf,8192 -o "$b/results/query-$storage.perf.data" -- "$b/env/bin/python" /home/danielking/parallel-vcf-profile-worker.py "$storage"
sudo perf report -f -i "$b/results/query-$storage.perf.data" --stdio --no-children --percent-limit 0.5 > "$b/results/query-$storage.perf.txt"
sudo perf report -f -i "$b/results/query-$storage.perf.data" --stdio --no-children --sort comm,pid --percent-limit 0.5 > "$b/results/query-$storage-threads.perf.txt"
sudo chown danielking:danielking "$b/results/query-$storage.perf.data" "$b/results/query-profile-$storage.vcf.bgz"
done
printf '%s\n' complete > "$b/results/query-profile-complete"
