# H003: eliminate identity collection work

Source `6a1936d0` (base main `53d59fb5`) removes the generic per-list accumulator path for `collect`. The mapped/filtered input already contains exactly the collected elements in encounter order, including missing elements. Both logical and physical lowering now expose the child expression with its declared metadata cast. This lets DataFusion see the projection instead of an opaque reducer. Direct UDF invocation retains a zero-copy Arrow list view, preserving offsets, values and logical list validity.

No hot row/entry loop, typed dispatch, initializer handling, accumulator configuration or handwritten SIMD was added. The operation is eliminated rather than made into a faster custom kernel. Generic fallback preparation for take/set/nonprimitive min/max remains open; this does **not** complete all of H003.

## Source/profile evidence

A representative generated sparse matrix has two million rows, with nested entry structs and filtered entries. Row collection is consumed by a downstream reduction over the collected length. Baseline sampling confirms `ListAggregate::invoke_with_args`, `AggregateFunctionExpr::create_accumulator`, `ArrayAggAccumulator::evaluate`, and `ScalarValue::iter_to_array` on the actual execution path. None appear in the candidate's five-second sample. Startup/compilation and thread waiting are present in both process-wide samples, so no percentage attribution is claimed.

The previous implementation creates one accumulator per present owning list and reconstructs all collected output lists. The new expression route makes neither allocation. Retained allocations belong to the input expression/pipeline; no allocation-count or memory-bandwidth counter was available in this run. Source inspection proves buffer reuse for direct calls, but byte traffic is not measured.

## Measurements

Apple M3 Max, normal kache/maturin release, jobs=2, target-cpu=native, dedicated candidate environment `/private/tmp/vail-list-collect-env`. Baseline is the normally built main runtime `/private/tmp/vail-native-row-fusion-env` (same production as main `53d59fb5`). Native/source fingerprints are in `provenance.json`.

One warmup then three measured evaluations in each fresh process; medians:

| Shape | Baseline wall | Candidate wall | Baseline CPU | Candidate CPU | Peak RSS baseline/candidate |
|---|---:|---:|---:|---:|---:|
| 2M rows × 64 columns | 12.021 s | 10.832 s | 14.260 s | 12.987 s | 1.591 / 1.653 GB |
| 2M rows × 4 columns | 8.424 s | 7.419 s | 10.230 s | 9.153 s | 427 / 400 MB |

Results match exactly (108M and 6M retained elements). Improvements are roughly 10–12% wall and 9–11% CPU. This is generated-input end-to-end execution, not a stored native read throughput or Hail comparison. Wide-case RSS increased about 4%; no general memory reduction is claimed. Inputs/normalization still dominate these pipelines.

Baseline and candidate were run sequentially, with local builds and other agents' measurements paused. Baseline warmup overlapped its CPU sample; warm measured iterations follow it. `superseded-candidate-collect.jsonl` is excluded because a later profile attempt may overlap its final iteration; a clean full rerun supplies `candidate-collect.jsonl`. The short-case first warmup may overlap completion of a trace save; the three measured iterations follow it.

## Hardware counters

Real Apple `CPU Counters` traces were recorded separately for ten seconds per process, not during the timing runs. Compressed MetricTable exports and parser/JSON summaries are retained. The parser ignores the first four seconds. Both traces cover part of a query, not an equal completed-work interval, so raw cycles are **not** a query cost comparison. Cycle-weighted top-down categories are essentially unchanged: useful ~59%, delivery ~24%, processing ~11%, discarded ~6%. These are Apple derived categories, not cache-miss, bandwidth, ALU or vector-instruction counters; no SIMD improvement is claimed.

## Validation

- Five new public semantic cases pass on both baseline and candidate: nested missing values, filtered/empty entry scopes, empty structs/cardinality, sliced/null/empty arrays, seeded values, Call phase and guarded invalid-cast scope.
- Broad candidate suite: **103 passed, one failed**; the additional Call/error-scope case also passes, for **104 distinct passing cases**. The sole failing node `test_collection_aggregate_matrix_scope_and_lookup_context` is reproduced on the exact main native baseline, with the same existing expected-three/got-one row-count error (`baseline-scope-test.log`). No new failing node.
- Broad scope: collection aggregate/kernels, matrix array aggregates, collection reducers, row fractions, CSE, aggregate execution and complete public RNG suite.
- Normal build log and installed module/source hashes retained. No compiled native artifact was copied or manually cached.
