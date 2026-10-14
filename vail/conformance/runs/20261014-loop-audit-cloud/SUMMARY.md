# Final loop audit and cloud benchmark results

Implementation commit: `bfe917a3e42448b0963555f2db81ade54cb0367f` on develop.
All original loop-audit findings have implementations using DataFusion grouping,
Arrow gathers/comparators, rangemap PAR ranges, and intervaltree partition indexes.
Review also removed repeated copies of unused array-fold columns.

Validation: 1,056 Python tests passed (one skipped, 19 subtests passed); 132 Rust
library tests in source passed across the combined run and final fold-pruning
regression, plus 14 storage integration tests. The Linux release passed 43
focused regressions. All 35 final Vail benchmark captures passed result checks.

| Median sums | Vail | Hail | Hail / Vail |
|---|---:|---:|---:|
| 15-case baseline | 1.559192 s | 6.222475 s | 3.99× |
| All 35 configurations | 3.001087 s | 9.657717 s | 3.22× |

These sums represent one median invocation per configuration, not the elapsed
benchmark suite time. The full report explains the retained Hail measurements,
VM restart, warmups, measurement intervals and performance-counter limitations.

## Cases where Hail remains faster

| Case | Vail median | Hail median |
|---|---:|---:|
| table_range_force_count (baseline) | 261.80 ms | 170.74 ms |
| table_scan_sum (baseline) | 195.27 ms | 175.15 ms |

## Reports and charts

- [Full timings, resources and 10 Hz charts](audit-results/SUMMARY.md)
- [Machine-readable comparison](audit-results/results.csv)
- [Sample scaling](audit-results/scaling-samples.png)
- [Variant scaling](audit-results/scaling-variants.png)
- [2× samples: CPU, RAM, SSD and performance counters](audit-results/telemetry-samples-2x-1.png)

The original Matrix layout comparison is in
[the earlier run](../20261014-matrix-layout-cloud/MATRIX_LAYOUT_COMPARISON.md).

The remaining optimization concern from the audit is physical replication of a
large row dictionary captured inside a higher-order expression. Batched scalar
dictionary lookup indexes once; row-valued lookup processes each input owner
without constructing per-key match lists. This is documented in the loop audit.

The temporary VM, root volume, security group, and key pair were removed.
