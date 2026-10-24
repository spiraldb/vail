# Row-aligned scalar broadcast correctness

Frozen production/test source `3f95200b24c3b0efa0122377aa848110e33fe991`, based on merged StageA `1af71dc161f47e3bc30751ac671d9e2de80ff7b8`. Normal candidate native SHA256 `9b577e3cca73600e204258c3ce47ce3f98054ba6125c785a743c61fab2fc0bc3`; immutable StageA baseline `d7d8f91914fb15cc069fe86a6782b05ad78ab047e11ceb8106ba9e71e66ba95c`. Both installed natives equal their normally built wheel members; native-provenance.json includes paths, wheel hashes and source fingerprints. No binary copies. Normal16job release build uses target-cpu=native/kache and the existing stable source/target. Source-overlay.json explicitly restores experimental Mean numeric_lists.rs/expressions.rs to StageA before compilation and touches changed Rust files.

AlignedMatrixMap has row-aligned entry-list/row-record inputs and separate singleton shared columns/globals. Constant folding can represent a repeated row-aligned input as a Scalar. Previously its helper materialized Scalars as one-row arrays, then returned one row from a multirow batch. This violated DataFusion scalar UDF cardinality and caused ExpectedN/Got1 errors. Both flat and scalar-row evaluation paths now materialize row-aligned Scalars to batch length via Arrow/DataFusion ColumnarValue.into_array; existing Array owners remain shared. Shared column/global helpers are unchanged. Configuration remains outside entry loops; extra materialization is once per batch and linear in the required row-aligned output, with no per-entry strategy dispatch.

## Public runtime evidence

New six-case suite on unchanged StageA baseline:3passed,3failed. The three failures reproduce ExpectedN/Got1 for2rows/1partition,7rows/1partition,7rows/3partitions. Empty/single-row/scalar-row controls pass. Exact candidate:all6pass. New plus adjacent aligned matrix suite:15passed, including missing slots, filtered captures, gathered column occurrences, independent bodies and nested parent nulls.

Broader exact candidate gate:367passed,0failed. Both historical baseline failures now pass:

- test_composite_floating_sum_retains_existing_aggregation_results (Expected2/Got1).
- test_collection_aggregate_matrix_scope_and_lookup_context (Expected3/Got1).

No tests are skipped or reclassified. This is a generic scalar/array contract correction rather than a floating aggregation special case. No performance or speedup claim is made; the batch allocation is necessary for correct broadcast cardinality.

Cloud run root `/home/danielking/aligned-scalar-broadcast`; candidate env is its `env/`. Source/target/compute released to Mean owner after final receipt download. Fresh4h `vail-broadcast-deadline.timer` armed for19:05UTC; earlier surviving guards explicitly disclosed to next owner (earliest17:46UTC). No main mutation by subagent.
