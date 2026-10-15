# Large matrix column sum improvement

Exact upstream case: `benchmark_matrix_table::test_large_range_matrix_table_sum` (10,000 rows × 500,000 columns, 2,500 source partitions). Selected because it had the lowest Hail/Vail ratio among same-machine comparable passing cases.

| Metric | Baseline (`90e5447`) | Fixed (`ac956e2`) |
| --- | ---: | ---: |
| Median wall seconds (3 runs) | 28.869 | 24.627 |
| Median core seconds (3 runs) | 103.526 | 84.467 |
| Hail / Vail, using existing 34.770 s Hail timing | 1.20× | 1.41× |

Wall time fell **14.7%**. This remains below the 2× Hail target. Hail was not rerun. Two new baseline/fixed pairs were interleaved after the initial observations. Every timing and resource measurement is in `measurements.csv`; the first baseline is the completed full-suite run.

## Changes

- Source-partition views select only the requested source plan rather than exposing the entire source partition tree in every view. Original private partition IDs and the shared execution semaphore survive selection and optimizer rewrites. This prevents quadratic traversal where these source views are constructed; it does not make all query planning free.
- Concurrent coordinate accumulators share an immutable group-index pattern through a single weak cache entry. The cache retains no data after users release it and cannot grow one entry per shape. DataFusion's existing accumulator still performs sums, null handling, partial states, and merges.
- Entry projections referencing named columns skip position arrays unless the IR actually contains a positional column lookup. Ordering, duplicate column occurrences, and missing entries retain their existing semantics.

No matrix entry computation was removed. No algebraic formula substitutes for the 5-billion-entry reduction. No scalar-at-per-entry path was added.

## Investigations and rejected experiments

The baseline native profile attributed about 12% of sampled CPU to per-entry coordinate-index construction, 27% inclusive CPU to Arrow take under array lambdas, and 8.6% leaf CPU to DataFusion sorting-requirement planning. Profile percentages overlap for inclusive frames.

| Experiment | Seconds | Disposition |
| --- | ---: | --- |
| Bulk-copy coordinate indices within each accumulator | 30.209 | Replaced; no wall-time benefit |
| Skip unused position arrays alone | 28.655 | Approximately flat; retained to avoid unnecessary allocation |
| Add logical round-robin before coordinate sums | 28.866 | Reverted; no measured benefit |
| Shared index pattern, before source-view fix | 27.927 | Retained as part of tested combined patch |

The final native profile completed in 24.950 s / 85.094 core-seconds. The index-construction leaf disappeared. Memory copying and Arrow take remain prominent; DataFusion planning still consumes meaningful CPU. These are remaining costs, not claims that the entire execution path is now optimal.

## Validation

- 155 targeted Python matrix/scan/join/partition/nested-aggregation tests passed.
- 49 storage/parallel-execution/count/head/tail tests and 10 subtests passed.
- Four focused Rust provider tests and six coordinate-aggregate tests passed, including concurrent different-width batches and source selection preserving IDs and concurrency gates.
- A 64-partition self-join completed.
- A broader diagnostic sweep found `test_native_scope_validation_rejects_forged_aggregate_ir` failing; the same test also fails against the untouched baseline release wheel. This patch does not resolve that pre-existing validation issue.

## Artifacts

- `report.html`: Hail versus representative fixed-run timings and 10 Hz charts.
- `measurements.csv`: every before/after timing, core-seconds, peak RSS.
- `before/flamegraph.svg`, `after-profile/flamegraph.svg`: measured-interval native profiles.
- The profile directories also contain raw samply profiles, symbols, and compact hotspot summaries. Large expanded stack dumps are derived local files and are not committed.
