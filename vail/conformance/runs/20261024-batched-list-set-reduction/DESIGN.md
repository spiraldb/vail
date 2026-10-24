# H003 collect_as_set: source audit and profile hypothesis

Source pinned to current main6656e247 (production H0459a). Design only: no production patch, no build, no VM work.

## Actual public row pipeline

`m.annotate_rows(distinct=hl.agg.collect_as_set(m.DP))`, then `rows=m.rows(); rows.aggregate(hl.agg.sum(hl.len(rows.distinct)))` forces every row's distinct set without exporting/materializing the table in Python. This is a general distinct-entry-annotation pipeline, not a benchmark matcher.

Python agg_collections.py:23 creates aggregate collect_as_set; matrix_list_aggregates.py:reduce_list maps the input values and emits __vail_list_reduce_collect_as_set at779. Rust list_aggregate.rs invokes the fallback at590: the configured factory is reused for a batch, but each defined list creates an accumulator at633. advanced_collection_aggregates.rs:80 wraps DF DistinctArrayAggAccumulator. On each accumulator's first update, DF array_agg.rs:ensure_state constructs RowConverter plus its rows buffers; evaluate decodes distinct rows, creates ScalarValues and builds a separate output ListArray. ListAggregate then joins those per-row ScalarValues into one Arrow array. This path is genuinely public; no dormant compatibility IR is used.

Hypothesis: repeated converter/accumulator/hash setup and distinct-row decode/ScalarValue/list reconstruction consume significant CPU for many small/moderate entry lists. First collect a software profile and peak RSS from the actual native63M-entry DP pipeline using the existing immutable H045 normally built env. No code before that evidence.

## Existing DataFusion alternative and resource risk

DataFusion55 set_ops.rs:general_array_distinct constructs one RowConverter per invocation, converts only the visible child span once, clears/reuses one HashSet for successive lists, collects first-occurrence indices, and Arrow-takes the input values once. It preserves null list validity. This avoids per-list accumulators and the row decode/ScalarValue round trip; standard DF array_distinct plus a list type-label cast would be visible to its optimizer.

However, it encodes the entire visible batch child span and preallocates indices capacity to every child element. The fallback streams one list's encoder/state at a time. Wide matrix batches may therefore increase encoded rows/offsets/index peak substantially, despite lower allocation/setup frequency. Measure RSS/faults on the actual DP pipeline and inspect batch shapes before choosing. Do not blindly trade CPU for a large capacity regression. A bounded/prepared segmented strategy remains only a later option if measured evidence rejects whole-batch DF reuse; no custom hash/encoder or handwritten SIMD now.

## Semantic compatibility gates

* Missing parent list stays missing; defined empty list yields a defined empty set; child null appears once. Set output order is unspecified: tests compare membership/multiplicity/cardinality, not hash iteration or first-seen order.
* Top-level Float32/Float64 cannot be directly rewritten: DF array_distinct calls normalize_float_zero, collapsing−0/+0. Current distinct aggregate uses Arrow RowConverter total-order encoding. Hail CollectAsSetAggregator.TypedKey.compKeys uses CodeOrdering.Compare; Float64Ordering invokes java.lang.Double.compare, distinguishing signed zeros. Preserve the float fallback until an equivalent strategy is proved. Do not infer set equality from Python float-set conversion, which collapses signed zero.
* Arrow RowConverter encodes raw NaN payload bits while Java Double.compare canonicalizes NaNs; this potential preexisting compatibility issue needs an independent public oracle before any float expansion. Do not silently change NaN semantics as a performance fix.
* Vail Set/Dict/ndarray at any nested level use unordered/unsupported equality and must remain on existing fallback initially. Existing expressions.rs:datafusion_distinct_compatible already excludes these for ordinary collection set operations; any shared predicate must additionally account for the distinct float normalization above.
* Integers/booleans/strings and ordered structural values can be proved with the same RowConverter keys in both implementations. Nested floats/opaque Call/Locus/Interval and Arrow dictionary storage require explicit tests/proof before enabling; conservative gating is acceptable. Arrow Dictionary dedup uses underlying decoded values, but output reconstruction differs (native decode+cast vs DF take), so do not assume physical storage equivalence is enough.
* Logical Set is Arrow List with element field __vail_set_item. Native array_distinct returns its input list field. Preserve the target logical descriptor with the existing restore_type/cast machinery, including scalar, zero-row, nested and persistence paths.
* Preserve input evaluation count, captures, error and RNG UID behavior. Lower only the reducer after the existing mapped input has been constructed; do not move predicates/effectful expressions.

## Proposed validation if profile justifies implementation

Public matrix row sets across partitions, empty rows/columns, filtered entries and missing values; nested/struct values and outer captures; array-aggregate nesting; defined-empty/missing-list distinction; logical set dtype through write/read; float signed-zero/NaN fallback; unordered/dictionary fallback. Independently authored expected values, no private helper oracle or plan-name assertions.

Use a query-time logical rewrite to ordinary DF array_distinct for a proved-safe type family and matching physical fallback for higher-order expressions, selected at preparation. No per-row schema tests. Keep output type casts batch-level. Profile converter/setup/reconstruction disappearance, actual CPU/wall and peak memory before accepting. Source review before normal candidate build; existing local target only after ArrayLength release.
