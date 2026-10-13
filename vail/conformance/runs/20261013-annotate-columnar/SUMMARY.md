# Columnar arithmetic verification

Ordinary add/subtract/multiply now lower to DataFusion binary expressions, including wrapping integer arithmetic. Power uses DataFusion math functions; floating floor division composes divide, floor, and cast. Integer floor division and modulo retain typed Arrow batch kernels for Hail sign and overflow semantics. These arithmetic paths no longer extract row operands with `scalar_at` or construct scalar output vectors.

## Validation

- Focused arithmetic and expression regressions: 22 passed.
- Full Python suite: 874 passed, 1 skipped, 19 subtests passed.
- Includes integer overflow, nulls, mixed numeric promotion, signed zero, NaNs, scalar broadcasting, floor/modulo signs, and minimum integer divided by -1.

## Annotation benchmark

Same optimized workload as the baseline: 1,000,000 rows, 100 int32 annotation fields, 14 source partitions; `_force_count` executes all output columns. Three warmups and five measured control iterations. Control was repeated after the Python suite finished.

| Measurement | Before | After |
| --- | ---: | ---: |
| Control median wall time | 4.652679 s | 0.046633 s |
| Median retired instructions per iteration | 80,425,370,539 | 369,548,945 |
| Median cycles per iteration | 17,502,305,628 | 152,887,554 |

Approximately 99.8x faster on this workload. This is a local before/after comparison; the stock Hail benchmark has not been rerun.

The 500-Hz native profile uses 50 measured iterations because the operation is now short. It includes 2.904 sampled CPU seconds and **zero sampled `scalar_at` frames**. DataFusion BinaryExpr/Arrow arithmetic account for about 60% of sampled CPU. The profiled median was 56.7 ms; sampling overhead and concurrent Python tests mean the profile timing is not used for the speedup. 10-Hz CPU, memory, instructions, and cycles are retained in `profiled/metrics.csv`.

[Flamegraph](profiled/flamegraph.svg) · [Telemetry](profiled/telemetry.svg) · [Native profile](profiled/profile.json.gz)

## Remaining audit finding

Some existing aggregate accumulators still extract a ScalarValue per input row in `aggregates.rs::update_batch`. This change does not fix those aggregators or claim every Vail operator is columnar. Singleton scalar extraction at literal/global/action boundaries is distinct from per-row arithmetic.
