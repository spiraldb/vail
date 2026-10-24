#!/bin/bash
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export VAIL_NUM_THREADS=32 RAYON_NUM_THREADS=32 TOKIO_WORKER_THREADS=32 OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=1
b=/home/danielking/parallel-vcf-export
for rep in 1 2 3; do
  for spec in 'candidate legacy' 'baseline native' 'hail legacy' 'baseline legacy' 'candidate native'; do
    read -r engine storage <<< "$spec"
    case "$engine" in
      candidate) py="$b/env/bin/python";;
      baseline) py="$b/baseline-env/bin/python";;
      hail) py=/home/danielking/cloud-scan-20261023/hail-env/bin/python;;
    esac
    "$py" /home/danielking/parallel-vcf-measure.py "$engine" "$storage" "$rep"
  done
done
