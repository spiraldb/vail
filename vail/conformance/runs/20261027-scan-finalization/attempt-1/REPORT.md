# H018 complete full-prefix diagnostic — API proof next, no implementation

The exact0df83c7e0 bounded public coordinate-mean scan completed its confirmation and ONE3second duration-driven sample. Every complete256variant×128sample exclusive prefix matched the independent Python sum/count reference, including one first UNKNOWN-width None result and202later zero-observation NaN coordinates. The actual source path contains ScanState::finish_states and repeated native accumulator preparation. This establishes a concrete setup-cost investigation, not an optimization win or permission to change native math/state behavior.

Recommendation: source/native public API compatibility proof for the **primitive mean child finalizer only**, then independent review before any production implementation. Keep H044indexed snapshot/export/restore and H020recurrence separate; do not treat their overlapping cost as removable H018finalization. Proposed workspace/chunk limits remain unimplemented/unmeasured. No extra query, alternate state, candidate kernel, benchmark comparison or repeat sample was run.

## Exact input and complete public correctness

NormalLinuxnative4761/prod517/mainafd-equivalent,69frontend pins, original native profile25metadata and original DPInt32 passed. Public fixedhead256variants/choosefirst128samples/localized nullableDP arrays preserve positions/missingness. Native rows.ht write/readback and Python reference preparation were setup-only,4.158868seconds. All256input arrays and keys,128original sample keys, all reference prefixes, source metadata and nine persisted file sizes/hashes/schema are retained in original `raw/fixture-lock.json` and archive. Original source table remains on persistent VM disk; no implicit source substitution.

Independent reference adds only defined Int32DP values per coordinate, excludes the current row, and divides by counts after the first array establishes width. All partial sums are below2^39 for<=256Int32terms, so integer sums convert exactly to double. First prefix is None, not128NaNs; later count0 coordinates yield NaN. Full outputs retain lossless finitehex and distinct None/NaN/±Inf tags, with finite relative/absolute1e-12 comparison. Complete variant domain/cardinality checked before values; lexicographic serialized-key ordering is only output presentation, not prefix computation order. All confirmation/sample outputs remain in original stdout, not truncated or digested. This oracle covers this fixed mean application, not arbitrary recurrence associativity.

Confirmation0.370772seconds wall/0.370411core-seconds. Ten sample-process actions range0.340936–0.390368seconds wall/0.341613–0.391369CPU. Action includes public collect and Python full-output validation/encoding/sort; stdoutserialization follows timer but is inside process/sample. Repetitions are oneprocess diagnostics, not independent benchmark trials or throughput comparisons. Whole-process monitored RSS299,257,856bytes includes input/query/state/output/Python, not allocated bytes or new workspace retention.

## Actual coarse CPU attribution

Successful perf cpu-clock:u99Hz DWARF requested sleep3, **279samples, zero reported lost**,2.327MBrecorded. This small sample has coarse resolution (one sample~0.36%); reported percentages are descriptive, not precise effect estimates.

| Symbol/family | Inclusive % | Flat % | Scope |
|---|---:|---:|---|
| ScanState finisher closure | 39.07 | 0.00 | Contains nested state preparation/finalization |
| ScanState::finish_states | 13.26 | 0.00 | Actual intended generic finalizer present |
| IndexedInner::snapshot | 30.82 | 0.00 | Existing H044 snapshot/export/restore; separate |
| GroupsAccumulatorAdapter::make_accumulators_if_needed | 16.85 | 0.00 | Includes snapshot restoration, not all H018 |
| MeanUdf::accumulator | 7.53 | 0.72 | Repeated native mean machinery; several caller paths |
| MeanUdf::native_fields vector materialization | 1.79 | 0.36 | Static descriptor work during factory invocation |
| libc cfree | see full report | 5.73 | Multiple unrelated owners; no allocation-byte inference |

Percentages overlap, including recursive/nested calls; never sum them or present39%as recoverable time. IndexedAccumulator evaluate29.03% and StateAccumulator evaluate18.64%also overlap. Async/inline roots and output/Python work limit precise attribution. Mean factory totals include scalar child restoration paths as well as finalization, so a source-scoped child finalizer cannot claim all7.53%belongs to its change. No actual dynamic StackReq/native group-workspace byte measurement or bandwidth saturation claim.

Source gate before implementation: exact scalar/group mean state descriptor/options/result/empty normalization compatibility, visible primitive state null-ancestor selection, bounded chunk/group-index setup, worker lifecycle/EmitToAll reset, actual retained capacity plus original snapshot/output liveness. NativeGrouped math/state/merge remains authoritative; no custom AVG, integer sum/divide, recursive null compactor, fold-path or H044snapshot changes. An unsupported native API or resource regression remains an exit with no code.

## Duration monitor and cleanup

Samplet0=73.290906136; Popen-before observation73.324791681; exit0poll76.485787549 (t0+3.194881<5). Immediate stop-new-launch76.485789509; query exit0decision76.640377646. Ten complete actions, stdout11,572,111bytes<64MiB, completeactionstream/perf-nonempty gates passed. These are process observations, not exact kernel record start/endpoints. No fixed action cap; existing launch5s/inflight10s/hard16s/process60s/6GiB/free10GiB bounds unchanged. Gates mapping records explicit failure names plus successful completion markers, not individually certified every limit.

Setup/confirmation/query/sampler/command process groups positively clean. Captured guest4→temporary2→verifiedrestored4. Provider1800sSTOP verified before boot, guest28minute guard. Boot SSH initially refused before guest readiness; only boot readiness repeated, not workload. Archive downloaded/SHAverified, proactiveSTOP then independent provider **TERMINATED**, host error:null/scope_verified:true. No further VM work requested.

`host/receipts.tar.gz` is the original receipt archive including perf.data. `raw/` omits only duplicate uncompressed perf.data; all input/reference locks and full output events are present. SHA256.json authenticates all packaged originals and this report. Required independent evidence review and root authorization precede any API proof runtime or finalizer implementation.
