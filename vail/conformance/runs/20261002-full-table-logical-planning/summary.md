# Full dedicated upstream Hail Table rerun — 2026-10-02

Pinned Hail commit: `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

| Scope | Passed definitions | Failed | Blocked |
|---|---:|---:|---:|
| Both dedicated suites (214 definitions) | 96 | 97 | 21 |
| Reviewed Table-only scope (191 definitions) | 96 | 75 | 20 |
| Matrix / mixed API / Spark scope (23 definitions) | 0 | 22 | 1 |

Expanded cases: **220 passed, 155 failed, 21 blocked**. All three GroupedTable definitions passed. A definition is passed only when all of its expanded cases pass.

Command: `PYTHONPATH=. uv run --no-sync python -m conformance.all_upstream_table_cases --output conformance/runs/20261002-full-table-logical-planning --timeout 60`; repeat with `--source ../hail/hail/python/test/hail/table/test_grouped_table.py` and output subdirectory `grouped`.

This executes original bodies/assertions and parameter variants against Vail in isolated subprocesses. Imports, decorators and Hail initialization are omitted; local fixtures and resource helpers are supplied. It is not upstream pytest collection, a Hail runtime oracle, or execution of supplementary mixed expression/Scala candidates. Blocks are not passes. Most blocks are unavailable internal `ir` references, plus `Env` and the largest-range test timeout. This run makes no code fixes.

## Nonpassing definitions grouped by first blocker

Each definition is counted once, by its first observed blocker; a later assertion may need additional functionality. Counts include all scopes.

| Bucket | Definitions |
|---|---:|
| MatrixTable / Spark outside Table-only scope | 23 |
| Harness IR references / execution timeout | 20 |
| Randomness and internal random-table support | 17 |
| Missing Table convenience / scans / persistence APIs | 10 |
| Other frontend semantics / assertions | 9 |
| Hail filesystem / backend compatibility helpers | 8 |
| Advanced lookup / interval and product lookup / query_table | 7 |
| Read options / partition selection / indexed reads | 7 |
| Join correctness: field case / key-prefix behavior | 7 |
| Pandas / ndarray construction and parity | 6 |
| Complex type construction / array numeric expressions | 4 |

## Important behavioral failures

- Six case-sensitive column-name join cases fail with DataFusion errors (`L_DUP` / `R_DUP` normalized to lowercase).
- `test_union`, `test_join_with_key_prefix`, two pandas conversion tests and three read/partition-selection tests fail assertions.
- Head/tail: 120 parameter cases pass; 40 cache variants fail because `cache` is absent. Checkpoint variants pass.
- Missing functionality includes ordered scans, random expressions, interval/product lookup, `query_table`, indexed-read flags, partial type inference, `nd.arange`, and show/cache/persist.
- Some failures expose compatibility helpers or private frontend properties rather than the central operation: `TemporaryDirectory`, `hadoop_open`, `_indices`, `_set_buffer_size`, and expected exception classes/messages.

## Per-definition results

| Test | Scope | Status | First blocker |
|---|---|---|---|
| `Tests.test_annotate` | table_only | passed |  |
| `Tests.test_aggregate1` | table_only | passed |  |
| `Tests.test_aggregate2` | table_only | passed |  |
| `Tests.test_aggregate_ir` | table_only | passed |  |
| `Tests.test_java_array_string_encoding` | table_only | passed |  |
| `Tests.test_to_matrix_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `Tests.test_to_matrix_table_row_major` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: to_matrix_table_row_major |
| `Tests.test_group_by_field_lifetimes` | table_only | passed |  |
| `Tests.test_group_aggregate_by_key` | table_only | failed | AttributeError: 'GroupedTable' object has no attribute '_set_buffer_size' |
| `Tests.test_aggregate_by_key_partitioning` | table_only | passed |  |
| `Tests.test_group_aggregate_na` | table_only | passed |  |
| `Tests.test_filter` | table_only | passed |  |
| `Tests.test_filter_missing` | table_only | passed |  |
| `Tests.test_transmute` | table_only | passed |  |
| `Tests.test_transmute_globals` | table_only | passed |  |
| `Tests.test_transmute_key` | table_only | passed |  |
| `Tests.test_select` | table_only | passed |  |
| `Tests.test_errors` | table_only | failed | TypeError: Table is immutable |
| `Tests.test_scan_filter` | table_only | failed | AttributeError: module 'vail' has no attribute 'scan' |
| `Tests.test_scan_tail` | table_only | failed | AttributeError: module 'vail' has no attribute 'scan' |
| `Tests.test_semi_anti_join` | table_only | passed |  |
| `Tests.test_indirected_joins` | table_only | passed |  |
| `Tests.test_table_matrix_join_combinations` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `Tests.test_index_globals` | table_only | passed |  |
| `Tests.test_interval_join` | table_only | failed | NotImplementedError: point lookup into interval-keyed tables is not implemented |
| `Tests.test_interval_filter_unordered` | table_only | passed |  |
| `Tests.test_interval_product_join` | table_only | failed | NotImplementedError: lookup of all duplicate matches is not implemented |
| `Tests.test_interval_product_join_long_key` | table_only | failed | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| `Tests.test_join_with_empty` | table_only | passed |  |
| `Tests.test_join_with_key` | table_only | passed |  |
| `Tests.test_product_join` | table_only | failed | NotImplementedError: lookup of all duplicate matches is not implemented |
| `Tests.test_multiple_entry_joins` | matrix_only_in_table_suite | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `Tests.test_multi_way_zip_join` | table_only | passed |  |
| `Tests.test_multi_way_zip_join_globals` | table_only | passed |  |
| `Tests.test_multi_way_zip_join_key_downcast` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `Tests.test_multi_way_zip_join_key_downcast2` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `Tests.test_multi_way_zip_join_highly_unbalanced_partitions__issue_14245` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `Tests.test_index_maintains_count` | table_only | passed |  |
| `Tests.test_index_keyless_table` | table_only | failed | AttributeError: module 'vail' has no attribute 'expr' |
| `Tests.test_aggregation_with_no_aggregators` | table_only | passed |  |
| `Tests.test_drop` | table_only | passed |  |
| `Tests.test_weird_names` | table_only | passed |  |
| `Tests.test_sample` | table_only | failed | AttributeError: sample |
| `Tests.test_from_spark_works` | spark_interop | blocked | NameError: name 'Env' is not defined |
| `Tests.test_from_pandas_works` | table_only | failed | AssertionError: False is not true |
| `Tests.test_from_pandas_objects` | table_only | failed | AssertionError: False is not true |
| `Tests.test_from_pandas_missing_and_nans` | table_only | passed |  |
| `Tests.test_from_pandas_mismatched_object_rows` | table_only | passed |  |
| `Tests.test_table_parallelize_infer_types` | table_only | passed |  |
| `Tests.test_table_parallelize_partial_infer_types` | table_only | failed | NotImplementedError: partial_type is not implemented in this initial slice |
| `Tests.test_table_parallelize_error_both_schema_partial_type_defined` | table_only | passed |  |
| `Tests.test_rename` | table_only | failed | AttributeError: _indices |
| `Tests.test_distinct` | table_only | passed |  |
| `Tests.test_group_by_key` | table_only | passed |  |
| `Tests.test_str_annotation_regression` | table_only | passed |  |
| `Tests.test_range_table` | table_only | passed |  |
| `Tests.test_range_table_zero` | table_only | passed |  |
| `Tests.test_issue_3654` | table_only | failed | TypeError: arithmetic requires a numeric expression |
| `Tests.test_explode_key_error` | table_only | passed |  |
| `Tests.test_explode_on_set` | table_only | passed |  |
| `Tests.test_explode_nested` | table_only | passed |  |
| `Tests.test_value_error` | table_only | passed |  |
| `Tests.test_export` | table_only | failed | AttributeError: module 'vail' has no attribute 'hadoop_open' |
| `Tests.test_export_delim` | table_only | failed | AttributeError: module 'vail' has no attribute 'hadoop_open' |
| `Tests.test_export_parallel_manifest` | table_only | failed | AttributeError: module 'vail' has no attribute 'current_backend' |
| `Tests.test_write_stage_locally` | table_only | failed | NotImplementedError: staging and custom codecs are not supported |
| `Tests.test_write_no_parts` | table_only | passed |  |
| `Tests.test_min_partitions` | table_only | passed |  |
| `Tests.test_read_back_same_as_exported` | table_behavior_with_matrix_fixture | failed | TypeError: keywords must be strings |
| `Tests.test_indexed_read_1` | table_only | failed | NotImplementedError: custom read_table options are not yet supported |
| `Tests.test_indexed_read_2` | table_only | failed | NotImplementedError: custom read_table options are not yet supported |
| `Tests.test_indexed_read_3` | table_only | failed | NotImplementedError: custom read_table options are not yet supported |
| `Tests.test_indexed_read_4` | table_only | failed | NotImplementedError: custom read_table options are not yet supported |
| `Tests.test_order_by_parsing` | table_only | passed |  |
| `Tests.test_take_order` | table_only | passed |  |
| `Tests.test_filter_partitions` | table_only | passed |  |
| `Tests.test_localize_entries` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `Tests.test_localize_self_join` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `Tests.test_union` | table_only | failed | AssertionError:  |
| `Tests.test_nested_union_100` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_bool' |
| `Tests.test_nested_union_200` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_bool' |
| `Tests.test_union_unify` | table_only | passed |  |
| `Tests.test_table_order_by_head_rewrite` | table_only | passed |  |
| `Tests.test_order_by_expr` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_unif' |
| `Tests.test_null_joins` | table_only | passed |  |
| `Tests.test_null_joins_2` | table_only | passed |  |
| `Tests.test_joins_one_null` | table_only | passed |  |
| `Tests.test_join_types_ht1_is_what_we_expect` | table_only | failed | RuntimeError: Schema error: No field named l_dup. Did you mean '"L_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "L_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_join_types_ht2_is_what_we_expect` | table_only | failed | RuntimeError: Schema error: No field named r_dup. Did you mean '"R_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "R_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_join_types_left` | table_only | failed | RuntimeError: Schema error: No field named l_dup. Did you mean '"L_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "L_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_join_types_right` | table_only | failed | RuntimeError: Schema error: No field named l_dup. Did you mean '"L_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "L_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_join_types_inner` | table_only | failed | RuntimeError: Schema error: No field named l_dup. Did you mean '"L_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "L_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_join_types_outer` | table_only | failed | RuntimeError: Schema error: No field named l_dup. Did you mean '"L_DUP"'? Column names are case sensitive. You can use double quotes to refer to the "L_DUP" column or disable the datafusion.sql_parser.enable_ident_normal |
| `Tests.test_partitioning_rewrite` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_unif' |
| `Tests.test_flatten` | table_only | passed |  |
| `Tests.test_expand_types` | table_only | passed |  |
| `Tests.test_expand_types_ordering` | table_only | passed |  |
| `Tests.test_expand_types_on_all_types` | table_only | failed | TypeError: keywords must be strings |
| `Tests.test_join_mangling` | table_only | passed |  |
| `Tests.test_join_with_filter_intervals` | table_only | passed |  |
| `Tests.test_key_by_aggregate_rewriting` | table_only | passed |  |
| `Tests.test_field_method_assignment` | table_only | passed |  |
| `Tests.test_refs_with_process_joins` | table_only | passed |  |
| `Tests.test_aggregate_localize_false` | table_only | passed |  |
| `Tests.test_collect_localize_false` | table_only | passed |  |
| `Tests.test_take_localize_false` | table_only | passed |  |
| `Tests.test_expr_collect_localize_false` | table_only | passed |  |
| `Tests.test_expr_collect` | table_only | passed |  |
| `Tests.test_expr_take_localize_false` | table_only | passed |  |
| `Tests.test_empty_show` | table_only | failed | AttributeError: show |
| `Tests.test_no_row_fields_show` | table_only | failed | AttributeError: show |
| `Tests.test_same_equal` | table_only | passed |  |
| `Tests.test_same_within_tolerance` | table_only | passed |  |
| `Tests.test_same_different_type` | table_only | passed |  |
| `Tests.test_same_different_global` | table_only | passed |  |
| `Tests.test_same_different_rows` | table_only | passed |  |
| `Tests.test_rvd_key_write` | table_only | failed | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| `Tests.test_show_long_field_names` | table_only | failed | AttributeError: show |
| `Tests.test_show__various_types` | table_only | failed | TypeError: keywords must be strings |
| `Tests.test_import_filter_replace` | table_only | failed | NotImplementedError: Java regular-expression semantics differ from DataFusion; only literal patterns are supported |
| `Tests.test_import_multiple_missing` | table_only | passed |  |
| `Tests.test_unicode_ordering` | table_only | failed | AttributeError: module 'vail' has no attribute 'sorted' |
| `Tests.test_physical_key_truncation` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `Tests.test_repartition_empty_key` | table_only | passed |  |
| `Tests.test_read_with_partitions` | table_only | failed | AssertionError:  |
| `Tests.test_path_collision_error` | table_only | failed | AttributeError: module 'vail.utils' has no attribute 'FatalError' |
| `test_large_number_of_fields` | table_only | failed | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| `test_import_many_fields` | table_only | passed |  |
| `test_segfault` | table_only | passed |  |
| `test_maybe_flexindex_table_by_expr_direct_match` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: _maybe_flexindex_table_by_expr |
| `test_maybe_flexindex_table_by_expr_prefix_match` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: _maybe_flexindex_table_by_expr |
| `test_maybe_flexindex_table_by_expr_direct_interval_match` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: _maybe_flexindex_table_by_expr |
| `test_maybe_flexindex_table_by_expr_prefix_interval_match` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: _maybe_flexindex_table_by_expr |
| `test_can_process_wide_tables` | table_only | passed |  |
| `test_join_with_key_prefix` | table_only | failed | AssertionError:  |
| `test_join_distinct_preserves_count` | table_only | passed |  |
| `test_write_table_containing_ndarray` | table_only | failed | AttributeError: module 'vail.nd' has no attribute 'arange' |
| `test_group_within_partitions` | table_only | passed |  |
| `test_group_within_partitions_after_explode` | table_only | passed |  |
| `test_group_within_partitions_after_import_vcf` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `test_range_annotate_range` | table_only | passed |  |
| `test_read_write_all_types` | table_only | failed | TypeError: keywords must be strings |
| `test_map_partitions_flatmap` | table_only | passed |  |
| `test_map_partitions_errors` | table_only | failed | AssertionError: Regex pattern did not match.   Expected regex: 'expected return type expression of type stream<struct>'   Actual message: 'Table._map_partitions callback must return a stream expression' |
| `test_map_partitions_indexed` | table_only | passed |  |
| `test_keys_before_scans` | table_only | failed | AttributeError: module 'vail' has no attribute 'scan' |
| `test_lowered_persist` | table_only | failed | AttributeError: persist |
| `test_lowered_shuffle` | table_only | passed |  |
| `test_read_partitions` | table_only | failed | AssertionError:  |
| `test_read_partitions_with_missing_key` | table_only | passed |  |
| `test_empty_tree_aggregate` | table_only | passed |  |
| `test_interval_filter_partitions` | table_only | passed |  |
| `test_grouped_flatmap_streams` | table_behavior_with_matrix_fixture | failed | AttributeError: module 'vail' has no attribute 'import_vcf' |
| `test_table_head_and_tail` | table_only | failed | AttributeError: cache |
| `test_to_pandas` | table_only | passed |  |
| `test_to_pandas_types_type_to_type` | table_only | passed |  |
| `test_to_pandas_types_column_to_type` | table_only | passed |  |
| `test_to_pandas_flatten` | table_only | passed |  |
| `test_to_pandas_null_ints` | table_only | passed |  |
| `test_to_pandas_nd_array` | table_only | failed | AttributeError: module 'vail.nd' has no attribute 'arange' |
| `test_literal_of_numpy_int64` | table_only | passed |  |
| `test_literal_of_numpy_int32` | table_only | passed |  |
| `test_literal_of_pandas_NA_and_numpy_int64` | table_only | failed | ModuleNotFoundError: No module named 'hail' |
| `test_literal_of_pandas_NA_and_numpy_int32` | table_only | failed | ModuleNotFoundError: No module named 'hail' |
| `test_write_many` | table_only | failed | AttributeError: module 'vail' has no attribute 'TemporaryDirectory' |
| `test_indexed_read_boundaries` | table_only | failed | AttributeError: module 'vail' has no attribute '_with_flags' |
| `test_table_randomness_range_table` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_range_matrix_rows_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `test_table_randomness_join` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_left_join_right_distinct` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_interval_join` | table_only | failed | NotImplementedError: point lookup into interval-keyed tables is not implemented |
| `test_table_randomness_union` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_map_globals_with_body_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_map_globals_without_body_randomness` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_explode` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_key_by` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_map_rows_with_body_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_map_rows_with_scan_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'scan' |
| `test_table_randomness_map_rows_without_body_randomness` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_map_partitions` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_read` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_matrix_entries_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `test_table_randomness_filter_with_cond_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_filter_without_cond_randomness` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_key_by_and_aggregate_with_body_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_key_by_and_aggregate_with_agg_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_key_by_and_aggregate_without_body_randomness` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_aggregate_by_key` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_matrix_cols_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `test_table_randomness_parallelize_with_body_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'rand_int64' |
| `test_table_randomness_parallelize_without_body_randomness` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_head` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_tail` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_order_by` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_distinct` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_repartition` | table_only | failed | AttributeError: module 'vail' has no attribute 'current_backend' |
| `test_table_randomness_cast_matrix_to_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail.utils' has no attribute 'range_matrix_table' |
| `test_table_randomness_rename` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_multi_way_zip_join` | table_only | blocked | NameError: name 'ir' is not defined |
| `test_table_randomness_filter_intervals` | table_only | failed | TypeError: intervals cannot contain missing or non-Interval values |
| `test_table_randomness_block_matrix_to_table` | mixed_matrix_or_blockmatrix_api | failed | AttributeError: module 'vail' has no attribute 'linalg' |
| `test_table_randomness_table_gen` | table_only | failed | AttributeError: type object 'Table' has no attribute '_generate' |
| `test_order_by_desc` | table_only | passed |  |
| `test_query_table_errors` | table_only | failed | AttributeError: module 'vail' has no attribute 'query_table' |
| `test_query_table` | table_only | failed | AttributeError: module 'vail' has no attribute 'query_table' |
| `test_query_table_randomness` | table_only | failed | AttributeError: module 'vail' has no attribute 'query_table' |
| `test_query_table_compound_key` | table_only | failed | AttributeError: module 'vail' has no attribute 'query_table' |
| `test_query_table_interval_key` | table_only | failed | AttributeError: module 'vail' has no attribute 'query_table' |
| `test_large_number_of_partitions` | table_only | passed |  |
| `test_range_table_biggest_int` | table_only | blocked | : worker exceeded 60.0s |
| `test_group_by_aggregate_doesnt_clobber_global_field` | table_only | passed |  |
| `GroupedTableTests.test_aggregate_by` | table_only | passed |  |
| `GroupedTableTests.test_aggregate_by_with_joins` | table_only | passed |  |
| `GroupedTableTests.test_issue_2446_takeby` | table_only | passed |  |

Raw results: [Table](results.json), [GroupedTable](grouped/results.json), [combined summary](summary.json). Per-definition logs and JSON cases are saved alongside these files.
