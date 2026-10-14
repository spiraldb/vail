# Full upstream benchmark execution coverage

Both original upstream files were attempted: **86 benchmark functions, expanded to 108 cases per engine**. This is benchmark execution coverage, not Hail unit-test parity. Original fixture sizes were retained. Unsupported fixtures were reported rather than replaced by smaller data.

Vail query code: `a3e497c7732f13f630e7cc9e3937c89b63fe3a31`. Stock Hail: PyPI `0.2.139-5e5eedb87ff8`; no Hail build. Both engines ran on the same 32-vCPU / 128-GiB VM.

| Engine | Passed | Failed | Unsupported | Timed out | Upstream skips | Total |
|---|---:|---:|---:|---:|---:|---:|
| Vail | 54 | 31 | 12 | 7 | 4 | 108 |
| Hail | 103 | 0 | 0 | 1 | 4 | 108 |

| Vail module | Passed | Failed | Unsupported | Timed out | Skips |
|---|---:|---:|---:|---:|---:|
| benchmark_table.py | 39 | 18 | 3 | 3 | 4 |
| benchmark_matrix_table.py | 15 | 13 | 9 | 4 | 0 |

## Vail blockers

Failures before fixture preparation completes do not establish that the benchmark body is broken. In particular, the approximate-CDF case is blocked by wide TSV matrix import, while collect-per-row and take-rekey are blocked by the random fixture.

| Root cause | Cases | Phases |
|---|---:|---|
| Text input: compression / type imputation | 13 | 10 setup / fixture, 3 benchmark body |
| Performance: timeout | 7 | 7 benchmark body |
| Seeded randomness (currently out of scope) | 7 | 6 setup / fixture, 1 benchmark body |
| Native panic / worker abort | 6 | 6 benchmark body |
| BGEN import | 4 | 4 setup / fixture |
| NDArray zeros (currently out of scope) | 4 | 4 benchmark body |
| Expression export | 3 | 3 benchmark body |
| Genotype expression APIs | 3 | 3 benchmark body |
| Wide TSV matrix import | 2 | 2 setup / fixture |
| Experimental multi-write API | 1 | 1 benchmark body |

Native failures comprise one Arrow take panic, four worker aborts (exit -6), and one worker killed with exit -9. Some stderr excerpts show huge allocation requests, but not every crash has a reliably identified cause. Exit -9 alone does not prove an out-of-memory kill. See [diagnostics](upstream-diagnostics.txt).

## Every non-passing Vail case

| Case | Status | Phase | Detail |
|---|---|---|---|
| `benchmark_table::test_table_range_join[1000000000,1000000000]` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_table::test_table_read_force_count_ints` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_read_force_count_strings` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_import_ints` | failed | benchmark body | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_import_ints_impute` | failed | benchmark body | NotImplementedError: Hail type imputation is not implemented; supply types explicitly |
| `benchmark_table::test_table_import_strings` | failed | benchmark body | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_aggregate_int_stats` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_range_array_range_force_count` | failed | benchmark body | RuntimeError: expression execution failed: partition count task failed: task 33 panicked with message "called `Option::unwrap()` on a `None` value" |
| `benchmark_table::test_table_aggregate_approx_cdf` | unsupported | setup / fixture | Vail has no import_matrix_table API for the upstream wide TSV fixture |
| `benchmark_table::test_table_aggregate_counter` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_aggregate_take_by_strings` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_aggregate_downsample_dense` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_aggregate_downsample_worst_case` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_table::test_table_aggregate_linreg` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_take` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_show` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_expr_take` | failed | setup / fixture | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_group_by_collect_per_row` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_table::test_group_by_take_rekey` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_table::test_table_scan_prev_non_null` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_table::test_test_map_filter_region_memory` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'zeros' |
| `benchmark_table::test_test_head_and_tail_region_memory` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'zeros' |
| `benchmark_table::test_test_inner_join_region_memory` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'zeros' |
| `benchmark_table::test_test_left_join_region_memory` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'zeros' |
| `benchmark_matrix_table::test_matrix_table_array_arithmetic` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'pl_dosage' |
| `benchmark_matrix_table::test_matrix_table_entries_table` | failed | benchmark body | worker exited with code -6 before reporting a result |
| `benchmark_matrix_table::test_matrix_table_take_entry` | failed | benchmark body | worker exited with code -6 before reporting a result |
| `benchmark_matrix_table::test_matrix_table_entries_show` | failed | benchmark body | worker exited with code -6 before reporting a result |
| `benchmark_matrix_table::test_matrix_table_nested_annotate_rows_annotate_entries` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_matrix_table::test_matrix_table_many_aggs_row_wise` | failed | benchmark body | AttributeError: is_hom_ref |
| `benchmark_matrix_table::test_matrix_table_many_aggs_col_wise` | failed | benchmark body | AttributeError: is_hom_ref |
| `benchmark_matrix_table::test_matrix_table_aggregate_entries` | failed | benchmark body | worker exited with code -6 before reporting a result |
| `benchmark_matrix_table::test_matrix_table_call_stats_star_star` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_matrix_table::test_gnomad_coverage_stats` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_matrix_table::test_gnomad_coverage_stats_optimized` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_matrix_table::test_per_row_stats_star_star` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_matrix_table::test_read_decode_gnomad_coverage` | unsupported | setup / fixture | Vail does not implement upstream fixture expression rand_unif |
| `benchmark_matrix_table::test_import_bgen_force_count_just_gp` | unsupported | setup / fixture | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_force_count_all` | unsupported | setup / fixture | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_info_score` | unsupported | setup / fixture | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_filter_count` | unsupported | setup / fixture | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_export_range_matrix_table_entry_field_p100` | failed | benchmark body | AttributeError: export |
| `benchmark_matrix_table::test_export_range_matrix_table_row_p100` | failed | benchmark body | AttributeError: export |
| `benchmark_matrix_table::test_export_range_matrix_table_col_p100` | failed | benchmark body | AttributeError: export |
| `benchmark_matrix_table::test_large_range_matrix_table_sum` | timed_out | benchmark body | Exceeded 250 s |
| `benchmark_matrix_table::test_kyle_sex_specific_qc` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'rand_bool' |
| `benchmark_matrix_table::test_matrix_table_scan_count_rows_2` | failed | benchmark body | worker exited with code -9 before reporting a result |
| `benchmark_matrix_table::test_matrix_multi_write_nothing` | failed | benchmark body | AttributeError: module 'vail' has no attribute 'experimental' |
| `benchmark_matrix_table::test_mt_localize_and_collect` | timed_out | benchmark body | Exceeded 120.0 s |
| `benchmark_matrix_table::test_mt_group_by_memory_usage` | unsupported | setup / fixture | Vail has no import_matrix_table API for the upstream wide TSV fixture |

## Hail timeout

`benchmark_matrix_table::test_matrix_multi_write_nothing` exceeded 120 seconds. All other non-skipped Hail cases passed.

## Method and provenance

Each original function body was invoked once in an isolated process after fixture setup. These durations are **not** the repeated warm medians in the [performance comparison](curated/SUMMARY.md). Query timeout defaults to 120 seconds, extended when the upstream marker requests more (the large range-matrix sum uses 250 seconds). Fixture setup has a separate 1,800-second cap.

Four source-marked skips were honored. Timed-out or aborted process groups were cleaned before continuing. Slow Spark shutdown was recorded separately as `worker_cleanup_forced`, preserving completed query outcomes. Runner-only import/parameter/shutdown bugs were fixed and invalid diagnostic runs excluded. Existing valid case results were retained on resume. No input sizes were reduced to turn a failure into a pass.

Complete per-case results, error tracebacks, timings, runner hashes and upstream source hashes: [Vail JSON](upstream-vail.json), [Hail JSON](upstream-hail.json). The two files contain the same 108 unique case IDs.
