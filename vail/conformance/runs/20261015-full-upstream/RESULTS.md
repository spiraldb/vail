# Full upstream benchmark run — 2026-10-15

Engine revision: `90e5447`. Fresh release wheel; all 108 upstream parameterized Table and MatrixTable cases, exact inputs/dimensions, no smoke scaling. One fresh-process attempt per case. Approximately 23 minutes elapsed across workers, including fixture preparation, bounded failures, and queries. No Hail reruns.

## Outcome

| Outcome | Cases |
| --- | ---: |
| failed | 9 |
| memory_limited | 10 |
| passed | 74 |
| timed_out | 5 |
| unsupported | 6 |
| upstream_skipped | 4 |

The prior 84-case subset now has **64 passes**, compared with 57 in the previous mixed-revision report. All 108 cases in this report were freshly attempted at the same engine revision.

## Comparable Hail baselines

Only 13 common passing cases have same-machine Hail baselines. Vail is faster in all 13; summed query runtimes are **46.462 s Vail versus 112.177 s Hail (2.41×)**. This is a sum of unlike workloads, not a geometric-mean speedup or a full-suite speedup. Other Hail timings came from a 32-vCPU Linux VM and are references only.

| Case | Vail seconds | Hail seconds | Hail/Vail |
| --- | ---: | ---: | ---: |
| `benchmark_table::test_table_range_join[1000000000,1000]` | 0.0062 | 4.6064 | 748.18× |
| `benchmark_table::test_table_range_join[1000000000,1000000000]` | 2.0949 | 4.1340 | 1.97× |
| `benchmark_table::test_table_foreign_key_join[1000000,1000000]` | 3.5253 | 9.2336 | 2.62× |
| `benchmark_table::test_table_foreign_key_join[1000000,1000]` | 0.1384 | 10.2716 | 74.21× |
| `benchmark_table::test_table_aggregate_array_sum` | 0.6005 | 5.1449 | 8.57× |
| `benchmark_table::test_table_range_means` | 0.7590 | 4.9413 | 6.51× |
| `benchmark_table::test_write_range_table[10000000,1000]` | 4.5828 | 11.7413 | 2.56× |
| `benchmark_table::test_write_range_table[10000000,100]` | 0.4419 | 7.0221 | 15.89× |
| `benchmark_table::test_write_range_table[10000000,10]` | 0.0791 | 5.7585 | 72.82× |
| `benchmark_matrix_table::test_write_range_matrix_table_p100` | 0.5977 | 7.2069 | 12.06× |
| `benchmark_matrix_table::test_large_range_matrix_table_sum` | 28.8971 | 34.7700 | 1.20× |
| `benchmark_matrix_table::test_matrix_table_scan_count_rows_2` | 4.6386 | 5.8177 | 1.25× |
| `benchmark_matrix_table::test_matrix_table_scan_count_cols_2` | 0.1006 | 1.5290 | 15.19× |

## Every non-passing case

Upstream-skipped cases are omitted below. Ten memory-limit exits comprise six fixture-preparation failures and four query failures. The five query timeouts each reached 120 seconds. Some API limitations are recorded as failures because the upstream query directly raises `NotImplementedError` or `AttributeError`.

| Case | Outcome | Phase | Detail |
| --- | --- | --- | --- |
| `benchmark_table::test_table_import_ints` | failed | query | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_import_ints_impute` | failed | query | NotImplementedError: Hail type imputation is not implemented; supply types explicitly |
| `benchmark_table::test_table_import_strings` | failed | query | NotImplementedError: compressed text IO is not implemented |
| `benchmark_table::test_table_range_array_range_force_count` | failed | query | RuntimeError: expression execution failed: partition count task failed: task 33 panicked with message "called `Option::unwrap()` on a `None` value" |
| `benchmark_table::test_table_aggregate_approx_cdf` | unsupported | fixture/start | Vail has no import_matrix_table API for the upstream wide TSV fixture |
| `benchmark_table::test_table_aggregate_downsample_worst_case` | timed_out | query | query exceeded time limit |
| `benchmark_table::test_group_by_collect_per_row` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_table::test_group_by_take_rekey` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_table::test_table_scan_prev_non_null` | timed_out | query | query exceeded time limit |
| `benchmark_table::test_test_head_and_tail_region_memory` | failed | query | RuntimeError: expression execution failed: partition count task failed: task 33 panicked with message "called `Option::unwrap()` on a `None` value" |
| `benchmark_table::test_test_inner_join_region_memory` | memory_limited | query | 32 GiB process-tree RSS guard |
| `benchmark_table::test_test_left_join_region_memory` | memory_limited | query | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_matrix_table_entries_table` | memory_limited | query | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_matrix_table_entries_show` | memory_limited | query | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_matrix_table_nested_annotate_rows_annotate_entries` | timed_out | query | query exceeded time limit |
| `benchmark_matrix_table::test_matrix_table_many_aggs_col_wise` | timed_out | query | query exceeded time limit |
| `benchmark_matrix_table::test_matrix_table_call_stats_star_star` | timed_out | query | query exceeded time limit |
| `benchmark_matrix_table::test_gnomad_coverage_stats` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_gnomad_coverage_stats_optimized` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_per_row_stats_star_star` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_read_decode_gnomad_coverage` | memory_limited | fixture/start | 32 GiB process-tree RSS guard |
| `benchmark_matrix_table::test_import_bgen_force_count_just_gp` | unsupported | fixture/start | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_force_count_all` | unsupported | fixture/start | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_info_score` | unsupported | fixture/start | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_import_bgen_filter_count` | unsupported | fixture/start | Vail does not implement BGEN import |
| `benchmark_matrix_table::test_export_range_matrix_table_entry_field_p100` | failed | query | AttributeError: export |
| `benchmark_matrix_table::test_export_range_matrix_table_row_p100` | failed | query | AttributeError: export |
| `benchmark_matrix_table::test_export_range_matrix_table_col_p100` | failed | query | AttributeError: export |
| `benchmark_matrix_table::test_matrix_multi_write_nothing` | failed | query | AttributeError: module 'vail' has no attribute 'experimental' |
| `benchmark_matrix_table::test_mt_group_by_memory_usage` | unsupported | fixture/start | Vail has no import_matrix_table API for the upstream wide TSV fixture |

## Artifacts

- `report.html`: all runtimes and interactive 10 Hz CPU/RSS/disk charts.
- `runtimes.csv`: complete inventory, times, core seconds, peak RSS, baseline hardware, and valid speedup ratios.
- `vail.json`: raw per-case results and source hashes.
- `comparison.json`: memory-normalized outcomes and comparisons.
- `telemetry/`: 10 Hz raw CPU/RSS/I/O/instruction/cycle counters; fixture telemetry is in a separate subdirectory.
- `hail.json`: reused baselines, explicitly labeled by hardware.

Measurements use a 32 GiB process-tree RSS guard during fixture preparation and queries. Default query timeout is 120 seconds; longer upstream annotations are honored. Fixture timeout is 900 seconds. Query time excludes engine initialization and fixture preparation. Inputs are cached; this is not a cold-cache I/O comparison. Results are single attempts, not distributions.
