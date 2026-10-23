# Matrix composite bottleneck before grouped globals pruning

Artifact: validated mean/fraction source56f2ff67, native b4a61500d7d5ed93684787c745e255eeda295720f8b615ef4e6b8466f4c6e8d5. 1kHz samply profile; unrelated activity allowed for proportional attribution. Query8.5106wall/62.5183core seconds.

33,176/37,618 samples (88.19%) are memmove beneath DataFusion cross_join::build_batch and Arrow list take. The two dominant child payloads are Int64 (26,326 samples69.98%) and Int32 (6,85718.23%). This is not coordinate accumulator arithmetic or the aligned-map output concat.

Diagnostic physical plan lines59–92 shows grouped CID SUM, a CrossJoinExec at65 broadcasting the complete __vail_matrix_columns singleton list onto the already-un-nested entry rows (UnnestExec87), then projecting that list away at62. Three other composite reducers follow the same grouped path. Frontend _axis_aggregates supports only a whole plain SUM as its column-coordinate fastpath; composite expressions use generic entry-context grouping.

The immediate general fix is grouped lowering's unused-global binding elimination, parallel to the already-approved total aggregate rule. This avoids carrying unreferenced singleton list columns through independent consumer boundaries. The underlying generic entry unnest/hash grouping remains a separate opportunity and will be profiled after the copy removal.
