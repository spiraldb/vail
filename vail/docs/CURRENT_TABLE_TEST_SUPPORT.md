# Current Hail Table test support

Fresh source-body run on 2 October 2026 against the current Vail native extension.

| Table-only definitions | Count |
|---|---:|
| Complete passing assertion bodies | 74 |
| Failed assertion bodies | 99 |
| Blocked by fixtures, harness dependencies, or timeout | 18 |
| Total | 191 |

**74 / 191 = 38.7% confirmed support.** This includes 188 Table-only definitions from `test_table.py` and all three from `test_grouped_table.py`. Matrix-dependent and Spark interoperability tests are excluded.

Across parameterized cases, 154 pass, 201 fail, and 18 are blocked. Passing variants inside a partially failing definition do not count as a fully passing test; in particular, the head/tail test has passing range/parallelize variants and remaining cache/checkpoint failures.

The harness executes extracted original assertion bodies and available fixtures against Vail, omitting Hail initialization, module imports, and decorators except reconstructed pytest parameterization. This is not full upstream pytest collection, a Scala suite run, or a differential Hail-runtime comparison. Blocked cases are not evidence of supported functionality.

[Full results, per-test errors, parameters, and source hashes](../conformance/runs/20261002T001552Z-table-only-status/results.json). The saved run also includes worker logs and the orchestration scripts. Earlier inventory documents contain historical counts; this fresh run supersedes them for dedicated Table-only support.

See [failure buckets](TABLE_FAILURE_BUCKETS.md) for a disjoint classification of the 99 failing definitions.

## Convenience-method follow-up

After implementing `all`, `any`, `collect_by_key`, and basic `checkpoint`, six previously failing original test bodies pass: `test_group_by_field_lifetimes`, `test_join_with_key`, `test_distinct`, `test_group_by_key`, `test_write_no_parts`, and `test_empty_tree_aggregate`. This raises confirmed passing definitions to **80 / 191 (41.9%)**. The remaining 111 definitions were not rerun in this follow-up; the failed/blocked split above belongs to the earlier broad run. [Follow-up results](../conformance/runs/20261002T140506Z-table-convenience-methods/summary.json).

## Lazy collection follow-up

`Table.collect`, `Table.take`, `Expression.collect`, and `Expression.take` now support `_localize=False`, returning reusable, unindexed array expressions backed by lazy DataFusion aggregation. Globals and literals produce singleton arrays; row expressions retain row multiplicity and ordering, including empty row structs. `aggregate(_localize=False)` remains supported.

Eight relevant original Hail assertion bodies pass, including six previously failing definitions: `test_collect_localize_false`, `test_take_localize_false`, `test_expr_collect_localize_false`, `test_expr_take_localize_false`, `test_expr_collect`, and `test_table_order_by_head_rewrite`. Confirmed support is now **86 / 191 (45.0%)**. Other definitions have not been rerun for this follow-up. The local Python suite passes 597 tests (one skipped), with 19 passing subtests; four targeted Rust collect tests pass. [Results and source hashes](../conformance/runs/20261002-nonlocalized-collection/summary.json).

## Arrow and pandas follow-up

`parallelize` row data, collection, scalar evaluation, and text-header sampling now use Arrow IPC. `from_pandas` and `to_pandas` use Arrow, including nullable dtypes, valid NaNs, key ordering, nested values, and dtype overrides. Seven previously failing original pandas-related assertion bodies now pass, raising confirmed support to **93 / 191 (48.7%)**. Four other pandas-related bodies still stop at expression-valued `parallelize` inputs, `or_missing`, or `nd.arange`. Other definitions were not rerun. The local Python suite passes 606 tests (one skipped), with 19 passing subtests. [Results and remaining failure details](../conformance/runs/20261002-arrow-python/summary.json).

## Missing-value helpers

`is_defined` and `or_missing` compose existing DataFusion null tests and lazy conditional expressions. The original `test_to_pandas_null_ints` assertion body now passes, raising confirmed support to **94 / 191 (49.2%)**. The local suite passes 609 tests (one skipped), plus 19 subtests. [Upstream result](../conformance/runs/20261002-missing-functions/test_to_pandas_null_ints.json).

## Case builder

`hl.case` supports ordered conditions, defaults, missing fallbacks, and runtime-error fallbacks, including `missing_false`. The original `Tests.test_explode_nested` body now passes, raising confirmed support to **95 / 191 (49.7%)**. The local suite passes 613 tests (one skipped), plus 19 subtests. [Results and execution details](../conformance/runs/20261002-case/summary.json).

## Expression source follow-up

Expression-valued `parallelize` inputs now construct lazy DataFusion-backed TableIR sources with Arrow literal bindings. `Tests.test_table_parallelize_infer_types` now passes, raising confirmed support to **96 / 191 (50.3%)**. The pandas reference-table tests now reach `_same` and fail on existing numeric, dictionary, and ndarray type inference differences. The local suite passes 626 tests (one skipped), plus 19 subtests; the targeted Rust source test passes. [Results, source hashes, and limitations](../conformance/runs/20261002-expression-inputs/summary.json).

## Interval filter follow-up

`hl.filter_intervals` supports inclusion/exclusion, overlapping and unordered intervals, scalar first keys, struct key prefixes, and unindexed interval-array expressions. It lowers through DataFusion filters; direct reads reuse the interval-read plan and certified fragment pruning. Interval values cross the Python/Rust boundary as Arrow IPC. Transformed-table and exclusion filters currently do not push intervals into source readers; prefix reads overselect fragments before exact filtering.

The original `Tests.test_interval_filter_unordered` assertion body passes, raising confirmed support to **97 / 191 (50.8%)**. Other original definitions were not rerun. The local suite passes 634 tests, with one skipped and 19 passing subtests. [Results and source hashes](../conformance/runs/20261002-filter-intervals/summary.json).
