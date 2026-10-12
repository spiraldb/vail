# Upstream Hail Table results — 2026-10-02

| Scope | Passed | Failed | Blocked | Total |
|---|---:|---:|---:|---:|
| Reviewed Table-only definitions | 114 | 56 | 21 | 191 |
| All definitions in Table and GroupedTable suites | 114 | 78 | 22 | 214 |
| Expanded parameter cases, all scopes | 238 | 136 | 22 | 396 |

18 additional definitions pass compared with the previous full run; no previously passing definition regressed. One previously failing interval/randomness test now reaches an unavailable internal `ir` assertion and is classified as blocked.

All three GroupedTable definitions pass. A definition counts as passing only if all expanded cases pass. Blocks include unavailable Hail internal harness references and the 60-second largest-range timeout.

The runner executes original Python test bodies and assertions against Vail, omitting upstream imports, decorators, and Hail initialization. This is a dedicated Python Table inventory, not all upstream Hail tests or a JVM oracle run. Legacy `.ht` compatibility is outside the implementation scope.

Local validation: 647 Python tests passed, one skipped, and 19 subtests passed. The complete vail-table Rust suite also passed.

Newly passing definitions:

- `Tests.test_interval_join`
- `Tests.test_interval_product_join`
- `Tests.test_product_join`
- `Tests.test_from_pandas_works`
- `Tests.test_from_pandas_objects`
- `Tests.test_union`
- `Tests.test_join_types_ht1_is_what_we_expect`
- `Tests.test_join_types_ht2_is_what_we_expect`
- `Tests.test_join_types_left`
- `Tests.test_join_types_right`
- `Tests.test_join_types_inner`
- `Tests.test_join_types_outer`
- `Tests.test_unicode_ordering`
- `Tests.test_read_with_partitions`
- `test_join_with_key_prefix`
- `test_write_table_containing_ndarray`
- `test_read_partitions`
- `test_to_pandas_nd_array`

Raw results: [Table](results.json), [GroupedTable](grouped/results.json), [combined summary](summary.json).
