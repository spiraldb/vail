# Guarded batched row-set reduction

Final production source9213c594, public owner-fixture-only5af3779a, based on mainca59e075. Prior unguarded820fc9f2 was rejected and remains recorded separately in REJECTED_RESULTS.md and final-receipts/. It is not the final resource evidence. No main update was made by this agent.

The query-prepared compatible-type selection still delegates set equality/hashing to ordinary DataFusion array_distinct. Both logical and physical lowering now feed it through the SAME compact-defined-lists ownership adapter. If parent nulls hide no child span, the adapter borrows input; otherwise it constructs one checked final offset vector and uses Arrow MutableArrayData once with coalesced defined ranges. This prevents DF's first-to-last envelope encoding from traversing hidden payload. No per-list ArrayRef/ScalarValue reconstruction, custom set loop or handwritten SIMD. Nested ArrowRow ownership behavior and underlying view-buffer retention remain unchanged; this is an outer-list ownership guarantee.

## Correctness and artifact gates

Fresh baseline420/420 and guarded candidate420/420 public Python tests pass. Two compiled Rust tests pass through PUBLIC Expr/Arrow RecordBatch lowering/evaluate: both logical and physical routes, growing hidden gap, explicit complete set/null results, nested struct nulls, nonzero-offset slices, defined-empty, scalar missing and zero-row cases. No private helper oracle, test-only export, plan-name assertion or resource counter hook.

The normally built/installed wheel is exact9213; example/test artifacts are5af with byte-identical production and test files, asserted in guarded-provenance.json. Native hash918c64a35d20352eeb3ae5e59fa997d96a8f13e1288edb9dbb320ec278a2def6. Baseline nativea79f18bc is unchanged. Compiler flags and separate normal wheel/example/test graphs are recorded in BUILD_COMMANDS.md. No native transplant. Completed owned baseline/guarded wheel archives were removed only after recording hashes and retaining installed normal natives; prior rejected wheel archive remains.

## Fresh bounded paired resource gate, including guard overhead

Same fixed seed30020261024 and preregistered shape as the original gate:24 independent processes,304 queries, eight balanced DP pairs and two pairs for each control. Warmups excluded as specified; every process/query retained, no outlier filtering or additional trial selection. No compilation, tests, software sampling or other owned workload overlapped. Ratios use paired process means; DP bootstrap resamples eight pairs. Two-pair controls have little inferential strength and their intervals are descriptive only.

| Public pipeline | Baseline wall | Guarded wall | Candidate/baseline wall | CPU seconds baseline → guarded | Whole-process peak RSS baseline → guarded |
|---|---:|---:|---:|---:|---:|
| Real63M-entry DP row sets |237.97ms|223.70ms|0.9402|2.7209→2.5489|1.5769→1.5504GB|
| Ordered high-cardinality struct row sets |2.3966s|0.1870s|0.07804|2.6567→0.4526|1.0699→1.1443GB|
| Floating fallback control |242.88ms|243.59ms|1.0030|2.7865→2.7916|1.5361→1.5324GB|

DP wall ratio95% bootstrap interval[0.9320,0.9483]; CPU ratio0.9369, interval[0.9259,0.9485]. DP peak ratio0.9842, interval[0.9453,1.0170], so no memory improvement is claimed. Floating fallback is effectively flat: CPU ratio1.0018, observed pair range0.9882–1.0156.

The struct pipeline improves wall about12.8x and CPU about5.9x, with a measured mean whole-process peak cost of74.4MB/+6.9%. The two individual peak ratios are1.0222 and1.1170. This cost is explicitly disclosed, not replaced with the prior rejected trial's smaller peak difference. DF's whole-visible-batch row encoding/index buffers trade capacity for avoiding per-list setup/decode; these measurements support the speed benefit but do not establish a universal batch-memory bound or live allocation count. Ordinary batch sizing remains relevant. Set values include the column index to represent high-cardinality ordered structural user data, not a benchmark-name optimization. This fixture has9,306,112 present entries, independently counted rather than counting its filtered cells.

## Hidden-parent resource regression gate

A public PyArrow/Table import smoke attempt showed its array-aggregate producer clips the gap before reduction; that observation is retained and is NOT used as proof. The decisive boundary is the public Rust Expr/Arrow RecordBatch resource example, where the supplied valid list directly retains the null-parent gap.

Twelve separate OS processes cover BOTH logical/physical routes, one/three independent retained RecordBatch owners and0/1,000,000/10,000,000 hidden Int32 children while keeping four visible values. All96 queries yield sets{7},missing,{9}; original aliased input width is checked unchanged. Query preparation/input allocation are outside recorded query windows. Warm query means stay about3–7microseconds across sizes; hidden10M is about4–6microseconds. Whole-process peak RSS grows from about12.2MB to52.3MB, consistent with the required40MB input producer, with no hidden-sized encoding expansion. Input allocation is still real and included in OS CPU/RSS; no universal backing-buffer release or allocator-hook claim is made. Protocol, binary hashes, every query and OS time output are retained.

## Actual final software sample

A separate three-second sample of the normally installed guarded native DP pipeline confirms general_array_distinct. The small ownership adapter was not resolved in this sample; its actual public boundary is independently covered by the hidden-parent tests/resource example. DistinctArrayAggAccumulator, ListAggregate and SetAccumulator remain absent. Baseline same-native sample is retained from the earlier phase. Frame string-presence counts are not CPU percentages; sampled query timings are separate. No PMU, ALU saturation, vector utilization or bandwidth claim.

## Remaining scope and review

Eligible recursively ordered nonfloating per-list collect_as_set only. Floating/opaque/unordered/dictionary and unsupported Call per-list fallback, take/ordered reducers and remaining generic initializer preparation stay open. Logical Set labels, UID/captures/error behavior remain covered. Required final source/resource/evidence acceptance is pending with the parent reviewer; H003 remains partial, not closed. Shared source/target/timing was explicitly released immediately after final recordings, before packaging.
