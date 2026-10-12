# Scan contract review

Compared the scan changes to baseline `7689173`, read the original Hail runtime observations and current scan conformance cases, and traced the result type, initializer, nested scope, and scalar/action paths. I found no concrete Hail-parity defect in the reviewed contract areas. This is a review result, not a claim that every Hail edge case is covered.

## Findings

No confirmed bugs to report.

The added `__vail_result_type` metadata is carried on aggregate IR and used by native `dtype_scoped` for scan results. This is necessary for reducers whose output is not derivable from the input type (for example genomic structs and `collect_as_set`); the factory still receives reducer options, and the scan lowering removes only its internal scan marker before constructing the reducer. Fold has its own initializer-derived result type. I did not find a concrete case where the metadata and emitted Arrow result disagree.

The Python classifier distinguishes marked scan reducers from ordinary reducers and still catches mixed scan/aggregate expressions through their enclosing scope. `Table.aggregate` rejects marked scans in `_validate_aggregate_ir` before the `_localize` return choice, so `_localize=False` does not bypass the contract. The grouping-expression classifier treats an enclosing aggregation scope containing a scan as scan-only while continuing to recognize an ordinary aggregate mixed into that scope.

The nested lowering preserves scalar aggregate leaves, filtered argument short-circuiting, group and array scope projections, and reducer initialization. `bind_initializers` consumes initializer expressions before `logical_scan.lower` checks for unsupported residual initializers. For array scopes, the extra inclusive window result is passed to the finishing expression so DataFusion must evaluate the validation window; it is not the prefix value returned to the caller. The local Hail observation and mismatch test support the array-length error contract.

## Coverage gaps (not confirmed defects)

- Resolved in the follow-up: `test_nonlocalized_aggregation_also_rejects_scans` directly covers `_localize=False`; `test_configured_scan_preserves_empty_execution_partitions` also covers the initialized-scan empty-partition path.
- Prefix-vs-total aggregate comparisons exercise many reducer schemas, but use Vail's aggregate implementation as the oracle. Hail runtime observations are narrower (currently array length mismatch, empty nested-scope validity, and Hail's cancellation-prone stats behavior). Add Hail-backed cases if independent parity evidence is needed for dynamic initializer failures or unusual nested group/array combinations.

I did not run a build or full suite; the reviewed working tree reports elsewhere in this task indicate the current Python and native conformance runs pass. Numerical stability, CDF, randomness, and cache behavior are intentionally outside this review per task scope.

## Follow-up alias fix

Reviewed the nested-fold alias change in `nested_scan.rs` without building. It now chooses a physical schema alias that is unique against existing fields, records the lambda-variable-to-alias mapping, and reverses that exact mapping while rebuilding the fold argument. This fixes the demonstrated collision with a user field named `__vail_scan_variable_0`; the added regression test exercises it inside `array_agg`. I found no remaining concrete issue in this alias/reverse-lookup path. Numerical stability, CDF, randomness, and cache behavior remain intentionally outside this review.
