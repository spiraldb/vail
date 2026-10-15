# 346-row matrix filter profile and fix

The fixture contains 346 rows, 100 columns, and 20 partitions. Filtering locus.position < 17434581 returns 245 rows and 15 partitions.

## Cause and implementation

An unknown partition count fell back to physical planning, constructing the entire interval filter just to count partitions. Prefix interval comparisons also applied nulls-last operand expansion to Boolean combinations, duplicating expression subtrees. The position filter expands to 84 contig intervals, and lowering repeatedly inferred types and processed locus metadata for the expanded expressions. Native sampling placed 1,722 of 1,727 main-thread samples in this lowering path.

Partition counting now uses interval metadata and the partition index directly, including through partition-preserving transformations. Physical interval reads share that selection logic. Constant prefix endpoints are evaluated once and converted into compact scalar comparisons with explicit nulls-last behavior; Boolean combinations remain ordinary Boolean operators.

## Staged timings

| Action | Baseline (856e1dd build) | Exact 958f5f8 | Final fix plus 958f5f8 |
| --- | ---: | ---: | ---: |
| Filtered partition count | 76.215 s | 37.004 s | 0.041 s |
| Filtered row count | 108.234 s | not run | 0.106 s |
| Filtered rows view count | 186.238 s | not run | 0.108 s |

These are individual diagnostic runs with Python profiling enabled, not controlled throughput benchmarks. Fixture conversion is excluded. All completed actions returned the expected counts. The exact 958f5f8 comparison was deliberately stopped after partition counting: it improves the problem but does not resolve it. Raw timing JSONL, cProfile files, native samples, and build logs accompany this report.

## Validation

- Focused interval Python tests: 53 passed.
- Native interval unit tests: 2 passed, including a linear predicate-size regression.
- Full upstream extract_intervals/test_locus_position.py: 10 passed in 16.23 seconds, including fixture setup.
- Full local Python suite: 1,275 passed, 1 skipped, 19 subtests passed, 4 failed in 209.64 seconds.
- All four full-suite failures reproduce on the exact 958f5f8 checkout and native build (958f5f8-existing-failures.log):
  - test_arrow_type_metadata.py::test_tagged_types_survive_execution_export_and_storage[dtype4-value4]
  - test_computed_keys_order.py::test_mixed_and_nested_keys_and_missing_values_round_trip
  - test_globals.py::test_globals_reads_are_lazy_snapshot_pinned_and_checksummed
  - test_logical_nested_aggregation.py::test_grouped_nested_reducers_have_logical_aggregate_and_unnest

The initial upstream-locus directory records an unsuccessful harness invocation with a relative PYTHONPATH; upstream-locus-final contains the successful run with an absolute PYTHONPATH.

Source base for the final fix: 12c473c2d14d00fe44ddd80156658677cfeddce6 (integrates 958f5f8b5c3bf458e7e79876b5d128bacb59baae). Native builds used Rust 1.98.0 in release mode. The final build was copied to /private/tmp/vail-matrix-filter-final.dylib before running tests.
