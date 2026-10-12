# Remaining upstream Table blockers

Audited from the full 2026-10-02 run: **114 passed, 56 failed, 21 blocked** Table-only definitions. Every nonpassing definition is assigned once, by its first observed blocker. Counts are not a complete inventory of all missing features; later assertions may reveal other gaps.

| First blocker | Failed definitions |
|---|---:|
| Python compatibility helpers / exception parity | 17 |
| Seeded randomness / sampling / table generation | 13 |
| Expression construction / typing | 6 |
| Stored-table query expressions | 5 |
| Ordered scans | 4 |
| Unavailable legacy indexed-read fixtures | 4 |
| Display | 3 |
| Unsupported write flags / regex import | 2 |
| Cache / persist | 2 |

## Internal Hail IR assertions / timeout

20 definitions are blocked by unavailable Hail internal ir references, mainly structural/row-UID assertions in randomness tests. One is blocked by the 60-second largest-range timeout. These cannot be counted as functional passes or failures; adapted semantic assertions are needed. The largest-range test needs separate performance investigation.

- `test_table_randomness_range_table`
- `test_table_randomness_join`
- `test_table_randomness_left_join_right_distinct`
- `test_table_randomness_interval_join`
- `test_table_randomness_union`
- `test_table_randomness_map_globals_without_body_randomness`
- `test_table_randomness_explode`
- `test_table_randomness_key_by`
- `test_table_randomness_map_rows_without_body_randomness`
- `test_table_randomness_read`
- `test_table_randomness_filter_without_cond_randomness`
- `test_table_randomness_key_by_and_aggregate_without_body_randomness`
- `test_table_randomness_aggregate_by_key`
- `test_table_randomness_parallelize_without_body_randomness`
- `test_table_randomness_head`
- `test_table_randomness_tail`
- `test_table_randomness_order_by`
- `test_table_randomness_distinct`
- `test_table_randomness_rename`
- `test_table_randomness_multi_way_zip_join`
- `test_range_table_biggest_int`

## Python compatibility helpers / exception parity

17 definitions stop on missing Python surface or harness compatibility: TemporaryDirectory (4), hadoop_open (2), current_backend (2), expr exception namespace (1), _indices (1), _set_buffer_size (1), _with_flags (1), FatalError (1), inline import hail (2), and exception class/message mismatches (2). These blockers do not establish that the underlying operation fails. In particular, grouped aggregation, rename, export, and interval lookup stop before their main assertions.

- `Tests.test_group_aggregate_by_key`
- `Tests.test_errors`
- `Tests.test_interval_product_join_long_key`
- `Tests.test_index_keyless_table`
- `Tests.test_rename`
- `Tests.test_export`
- `Tests.test_export_delim`
- `Tests.test_export_parallel_manifest`
- `Tests.test_rvd_key_write`
- `Tests.test_path_collision_error`
- `test_large_number_of_fields`
- `test_map_partitions_errors`
- `test_literal_of_pandas_NA_and_numpy_int64`
- `test_literal_of_pandas_NA_and_numpy_int32`
- `test_write_many`
- `test_indexed_read_boundaries`
- `test_table_randomness_repartition`

## Seeded randomness / sampling / table generation

rand_int64 (7), rand_bool (2), rand_unif (2), Table.sample (1), and Table._generate (1). Random expression evaluation needs stable row identity and seeded streams through transformations, not merely a DataFusion random() call. The tests named nested_union/order_by/partitioning_rewrite stop constructing random input.

- `Tests.test_sample`
- `Tests.test_nested_union_100`
- `Tests.test_nested_union_200`
- `Tests.test_order_by_expr`
- `Tests.test_partitioning_rewrite`
- `test_table_randomness_map_globals_with_body_randomness`
- `test_table_randomness_map_rows_with_body_randomness`
- `test_table_randomness_map_partitions`
- `test_table_randomness_filter_with_cond_randomness`
- `test_table_randomness_key_by_and_aggregate_with_body_randomness`
- `test_table_randomness_key_by_and_aggregate_with_agg_randomness`
- `test_table_randomness_parallelize_with_body_randomness`
- `test_table_randomness_table_gen`

## Expression construction / typing

Three all-types tests fail constructing hl.dict({array_expression: value}) because dict_ checks expression-valued values but misses expression-valued keys, routes into literal(), and infers a struct with non-string field names. One test needs elementwise array arithmetic: agg.array_sum(x) / [2, 2]. One needs partial_type inference for Table.parallelize. One needs filter_intervals to accept hl.interval(...) expressions, rather than only materialized Interval values. Fixing the initial construction failure can uncover additional issues in all-types tests.

- `Tests.test_table_parallelize_partial_infer_types`
- `Tests.test_issue_3654`
- `Tests.test_expand_types_on_all_types`
- `Tests.test_show__various_types`
- `test_read_write_all_types`
- `test_table_randomness_filter_intervals`

## Stored-table query expressions

query_table is absent. It must return a lazy array expression for a point/prefix/interval query over a stored table, with key-type checking; existing reads, filters, and nonlocalized collect supply much of the plumbing. One of the five definitions additionally requires randomness.

- `test_query_table_errors`
- `test_query_table`
- `test_query_table_randomness`
- `test_query_table_compound_key`
- `test_query_table_interval_key`

## Ordered scans

hl.scan is absent. Ordered prefix aggregation must respect key order and carry state across independently executed partitions; ordinary grouped/total aggregation does not provide this behavior. One definition also needs randomness.

- `Tests.test_scan_filter`
- `Tests.test_scan_tail`
- `test_keys_before_scans`
- `test_table_randomness_map_rows_with_scan_randomness`

## Unavailable legacy indexed-read fixtures

Four tests fail opening old .ht fixtures. This is outside the agreed current scope. New-format indexed-read/partition regressions pass, but these four original tests do not verify the new format.

- `Tests.test_indexed_read_1`
- `Tests.test_indexed_read_2`
- `Tests.test_indexed_read_3`
- `Tests.test_indexed_read_4`

## Display

Table.show is absent. Three definitions directly reach that missing method. A fourth show test is currently counted under dictionary construction because it fails earlier.

- `Tests.test_empty_show`
- `Tests.test_no_row_fields_show`
- `Tests.test_show_long_field_names`

## Unsupported write flags / regex import

write(stage_locally=True) is explicitly rejected (one definition). import_table regex replacement is restricted to literal patterns (one): the upstream test requests capture groups and Java-style $1 replacement.

- `Tests.test_write_stage_locally`
- `Tests.test_import_filter_replace`

## Cache / persist

cache and persist are absent. Head/tail has 120 passing expanded cases and 40 cache-dependent failing cases; the whole definition therefore fails. Persist is a separate failing definition.

- `test_lowered_persist`
- `test_table_head_and_tail`

## Suggested order

First repair dictionary-expression construction, expression-valued intervals, partial inference, and array arithmetic. Then add query_table using existing lazy reads/actions. Cache/persist and display are separate bounded tasks. Ordered scans and seeded randomness are the major execution-semantics packages. Separately adapt internal-IR assertions and legacy-format fixture tests so the inventory measures Vail behavior without claiming unsupported Hail internals.

No implementation changes were made for this audit. Raw classifications: [blockers.json](blockers.json).
