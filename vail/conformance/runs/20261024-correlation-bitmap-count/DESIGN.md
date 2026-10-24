H045: scalar correlation pair-count preparation

Source inspected: main 2c28d6b15687d140b4c2adc70a77c38b40fb2c8c, crates/vail-execution/src/advanced_numeric_aggregates.rs, CorrAccumulator::update_batch. Native normal baseline SHA9b577e3cca73600e204258c3ce47ce3f98054ba6125c785a743c61fab2fc0bc3.

Actual public pipeline: read_matrix_table(profile25_mt.mt), aggregate_entries(agg.corr(DP,GQ)), 63,083,475 logical entries. Ten profile repetitions returned0.7536333631956934. CorrAccumulator update14.17%inclusive/2.35%exclusive;241/243 exclusive samples land in duplicate pair-validity count loop (PC70d1ee0..70d1f69). Thus approximately2.33%totalCPU, a modest ceiling. Raw perf recording remains persistent VM disk; annotation included here. No hardware saturation inference.

Minimal proposal: KEEP exact pair_count and checked_add semantics; keep existing native grouped correlation algorithm, six-field wire state, merge, finalization and dtype casts unchanged. Replace only scalar validity count with a batch-level match on x.nulls()/y.nulls():
* none/none: x.len()
* one null buffer: len-null_count (cached Arrow NullBuffer count)
* both: Arrow BooleanBuffer::bit_chunks().iter_padded() zip, map (left & right).count_ones(), sumusize.

Arrow bit chunk iterators handle independent bit offsets and zero-pad the final word. Equal logical lengths are already assumed by existing x/y loop; enforce the same contract before zip if not already guaranteed by corr_pair struct shape. No new bitmap, widened array, native snapshot, cache, SIMD, row-dependent schema check, or per-row allocation. Null-buffer presence is actual batch representation; selecting it once per batch is necessary, while query schema and native correlation operator remain chosen at preparation. Missing floating values mean nulls only; NaN/Inf remain valid pairs exactly as before. Parent struct nulls already incorporated by corr_pair and must remain honored.

Library gap: NullBuffer::union would materialize an output validity buffer for counting; avoid it. BooleanBuffer bit_chunks is the existing Arrow zero-copy packed-word API; no custom unsafe kernel. usize count cannot exceed input length, then original castu64 and checked_add preserve overflow behavior, including full-UInt64 partial-state tests. Grouped wrapper unchanged.

Public gates: empty/all-valid/one-mask/two-mask; independent sliced offsets0,1,7,63,64,65 and lengths0,1,63,64,65,129; hidden child values under parent null; NaN/Inf and zero/one valid pair; partial-state merge/repeated state/evaluate/fullUInt64 overflow contracts. Expected values/counts independently authored, no private-helper oracle or plan text assertions. Existing cross-merge wire tests retained. Resource gate: actual public corr workload paired wall/CPU, profile count-loop disappearance; expect at most~2%CPU gain and no memory claim. Reject complexity/regression; no broad aggregate rewrite.

Status: design only, production unchanged, review required before implementation. Cloud stopped by H050 owner; no build or timing requested yet.
