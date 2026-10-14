# Upstream benchmark coverage

The cloud suite currently contains 15 benchmark functions total: 11 Table and 4 MatrixTable functions. Its parameter and scaling variants produce 35 workload configurations. This is not full upstream-module coverage. The upstream sources contain **86 test labels**: 45 in `benchmark_table.py` and 41 in `benchmark_matrix_table.py`. Expanding `@pytest.mark.parametrize` and fixture parameters yields **108 invocations** (67 Table, 41 Matrix). The runner derives this inventory from source ASTs.

## Coverage runner

`/private/tmp/vail-parallel-tests/bin/python benchmarks/upstream_benchmark_coverage.py --list` prints every expanded invocation. By default, `--engine vail|hail` attempts the upstream workload using original fixture dimensions and public fixture inputs, in a fresh process per case. `--fixture-timeout SECONDS` bounds downloads and native fixture materialization (default 1,800 seconds); after setup, `--timeout SECONDS` bounds benchmark execution (default 120 seconds, raised to any larger upstream `xtimeout` value). A timed-out worker is terminated. `--data-dir PATH` selects the download and native materialization cache. A JSON report can be written with `--output PATH`. `--smoke` explicitly selects reduced synthetic fixtures for dispatch checks; smoke results are never benchmark timings. Failures, unsupported engine APIs, timeouts, and fixture-generation errors remain explicit in the report.

Exact fixture inputs download from `https://storage.googleapis.com/hail-common/benchmark/`, matching upstream `fixtures.py`. Generated Hail `.ht` and `.mt` fixtures use the source dimensions. Native fixture files are materialized independently under `<data-dir>/<engine>/`, so Hail and Vail never read each other's native storage formats.

## Fixture plan

- `profile25_mt`: downloads `profile25/profile.vcf.bgz` and imports it with 16 partitions.
- `gnomad_dp_sim`: generates the original 250,000 × 1,000 random-valued MatrixTable with 32 partitions. Vail currently reports this fixture unsupported because its expression API lacks `rand_unif`.
- `random_doubles_mt`: downloads `random_doubles/random_doubles_mt.tsv.bgz` and imports it as a MatrixTable. Vail currently reports this fixture unsupported because it has no `import_matrix_table` API.
- `many_ints_tsv`, `many_strings_tsv`: download the original compressed TSVs and import native `.ht` files when requested.
- `many_partitions_ht`: generates the original 10-million-row table at the fixture's requested 10, 100, or 1,000 partitions.
- `sim_ukb_bgen`, `sim_ukb_sample`: download the original BGEN/SAMPLE files; BGEN cases are explicitly unsupported on Vail because it has no `import_bgen` API.

Cases involving 100M–1B rows, 100M-element arrays, or 250k × 1k matrices are attempted under the per-case execution timeout, after fixture materialization completes under its separate timeout. Upstream `skip` cases receive an explicit `upstream_skipped` result with their source reason; timeout-marked cases are still attempted under the runner timeout. For the provisioned VM, the full exact-input run is:

```sh
/private/tmp/vail-parallel-tests/bin/python benchmarks/upstream_benchmark_coverage.py \
  --engine vail --data-dir /home/ubuntu/hail-benchmark-data \
  --output /home/ubuntu/hail-benchmark-data/vail-upstream-report.json
```

Full runs are intended for the provisioned VM, not this workstation. This runner's timings are single executions, not a statistically controlled performance comparison.

## Table: 45 labels, 67 invocations

- `test_table_key_by_shuffle`
- `test_table_group_by_aggregate_sorted`
- `test_table_group_by_aggregate_unsorted`
- `test_table_range_force_count`
- `test_table_range_join`
- `test_table_python_construction`
- `test_table_big_aggregate_compilation`
- `test_table_big_aggregate_compile_and_execute`
- `test_table_foreign_key_join`
- `test_table_aggregate_array_sum`
- `test_table_annotate_many_flat`
- `test_table_annotate_many_nested_no_dependence`
- `test_table_annotate_many_nested_dependence_constants`
- `test_table_annotate_many_nested_dependence`
- `test_table_read_force_count_ints`
- `test_table_read_force_count_strings`
- `test_table_import_ints`
- `test_table_import_ints_impute`
- `test_table_import_strings`
- `test_table_aggregate_int_stats`
- `test_table_range_means`
- `test_table_range_array_range_force_count`
- `test_table_aggregate_approx_cdf`
- `test_table_aggregate_counter`
- `test_table_aggregate_take_by_strings`
- `test_table_aggregate_downsample_dense`
- `test_table_aggregate_downsample_worst_case`
- `test_table_aggregate_downsample_sparse`
- `test_table_aggregate_linreg`
- `test_table_take`
- `test_table_show`
- `test_table_expr_take`
- `test_read_force_count_partitions`
- `test_write_range_table`
- `test_read_with_index`
- `test_union_partitions_table`
- `test_join_partitions_table`
- `test_group_by_collect_per_row`
- `test_group_by_take_rekey`
- `test_table_scan_sum_1k_partitions`
- `test_table_scan_prev_non_null`
- `test_test_map_filter_region_memory`
- `test_test_head_and_tail_region_memory`
- `test_test_inner_join_region_memory`
- `test_test_left_join_region_memory`

## MatrixTable: 41 labels, 41 invocations

- `test_matrix_table_decode_and_count`
- `test_matrix_table_decode_and_count_just_gt`
- `test_matrix_table_array_arithmetic`
- `test_matrix_table_entries_table`
- `test_matrix_table_entries_table_no_key`
- `test_matrix_table_rows_force_count`
- `test_matrix_table_show`
- `test_matrix_table_rows_show`
- `test_matrix_table_cols_show`
- `test_matrix_table_take_entry`
- `test_matrix_table_entries_show`
- `test_matrix_table_take_row`
- `test_matrix_table_take_col`
- `test_write_range_matrix_table_p100`
- `test_write_profile_mt`
- `test_matrix_table_rows_is_transition`
- `test_matrix_table_filter_entries`
- `test_matrix_table_filter_entries_unfilter`
- `test_matrix_table_nested_annotate_rows_annotate_entries`
- `test_matrix_table_many_aggs_row_wise`
- `test_matrix_table_many_aggs_col_wise`
- `test_matrix_table_aggregate_entries`
- `test_matrix_table_call_stats_star_star`
- `test_gnomad_coverage_stats`
- `test_gnomad_coverage_stats_optimized`
- `test_per_row_stats_star_star`
- `test_read_decode_gnomad_coverage`
- `test_import_bgen_force_count_just_gp`
- `test_import_bgen_force_count_all`
- `test_import_bgen_info_score`
- `test_import_bgen_filter_count`
- `test_export_range_matrix_table_entry_field_p100`
- `test_export_range_matrix_table_row_p100`
- `test_export_range_matrix_table_col_p100`
- `test_large_range_matrix_table_sum`
- `test_kyle_sex_specific_qc`
- `test_matrix_table_scan_count_rows_2`
- `test_matrix_table_scan_count_cols_2`
- `test_matrix_multi_write_nothing`
- `test_mt_localize_and_collect`
- `test_mt_group_by_memory_usage`
