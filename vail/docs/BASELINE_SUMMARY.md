# Vail baseline — 24 September 2026

This summarizes the preserved run at [`20260924T203512Z-baseline`](../conformance/runs/20260924T203512Z-baseline/summary.json), before the current filter and collection work. Counts below come from the [structured classification](../conformance/runs/20260924T203512Z-baseline/structured-summary.v2.json) of that run's original outputs; classification did not rerun the suites.

| Scope | Passed | Failed or incomplete |
| --- | ---: | --- |
| Local Rust tests | 24 (10 execution, 14 storage) | 0 failed |
| Local Python and harness tests | 40, with 19 passing subtests inside them | 0 failed |
| Native conformance fixtures | 3 | 1 unsupported: globals |
| Extracted upstream Hail Table cases | 87 | 1 failed: `test_annotate` stopped at `AttributeError: _same` |
| Live Hail oracle fixtures | 0 | 4 unavailable: Hail initialization could not import `hailtop.version` |

Thus **64 local tests passed** and **90 separately counted source-derived cases passed**. The 19 subtests are already contained in the Python tests and should not be added again. The unsupported fixture and unavailable oracle are neither passes nor assertion failures. The extension build passed. The [native outcomes](../conformance/runs/20260924T203512Z-baseline/native-outcomes.json), [upstream case results](../conformance/runs/20260924T203512Z-baseline/upstream-table-results.json), and [oracle outcomes](../conformance/runs/20260924T203512Z-baseline/oracle-outcomes.json) retain the details.

The extracted cases run original Hail assertion bodies against Vail without Hail initialization, decorators, or the full upstream pytest collection. They do not establish live Hail parity. In the [Table-only implementation map](TABLE_EXPRESSION_TEST_RESULTS.md), **7 of 227 definitions have complete passing bodies**; **220 still lack a complete passing body**, including **184 of 191** dedicated-suite Table-only definitions. Most of that remaining scope is **unrun**, not failed. `test_table_head_and_tail` contributes 80 passing range/parallelize cases to the 87, while its 80 cache/checkpoint cases remain unrun. The broader [inventory](HAIL_TEST_INVENTORY.md) and [supplement](HAIL_TEST_INVENTORY_EXTRA.md) also track mixed Matrix, shared expression, and backend tests; the full Hail test universe was not executed.

Key baseline gaps are collection expressions (array map/filter/flatmap, reductions and sets), row filtering, Table globals, aggregations and scans, grouping, joins and indexing, and fuller key/partition behavior. Python-facing persistence, cache/checkpoint in the new format, broader expression and domain methods, display, and interoperability also need work. The Table-only dependency group counts in the implementation map overlap and describe requirements for uncompleted tests; they are not additive counts of missing features. Legacy `.ht` decoding is deferred.

The original runner saved [source hashes at run start](../conformance/runs/20260924T203512Z-baseline/source_hashes.json) but did not capture end hashes. Its post-run review found observed edits after the run ended; it cannot independently prove the absence of changes during the run. Later code changes must not be read back into this baseline.
