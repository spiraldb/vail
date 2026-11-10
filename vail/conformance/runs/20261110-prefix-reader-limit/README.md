# Bounded native reader demand, 2026-11-10

A bounded ordered action now starts partitions only when needed and stops after its output limit. Row-preserving native scans can forward an exact cap to Vortex's existing scan limit; dropping filter/unnest paths receive smaller scan/Arrow batches while continuing until enough output survives. Unknown/blocking paths retain their existing input plan. Unbounded actions retain the original concurrent partition scheduler and default reader configuration.

The demand is action-local: LocalRead clones carry scan options, shared snapshot metadata is unchanged, and one immutable prepared child graph is initialized per bounded physical concat. Clearing/replacing fetch constructs a fresh plan with original children; repeated execution of the same action retains its existing consumer replay guard. Setup stays outside row/batch loops. Native partitions are file-aligned under the existing no-delete/full-file read contract. Residual/inexact filters never receive a hard input cap.

DataFusion already represents LIMIT with skip/fetch and supplies logical/physical pushdown. This change connects its scan limit and physical fetch contracts to Vail's ordered/native read path. Existing join rules are reused: the preserved side of an eligible left/right join can be capped; capping both full-outer inputs can hide matches and change results. Offsets retain their outer limit; public tests check offset/output order, not private optimizer structure.

Matched warmed laptop subset: three fresh workers per version/case in B,C,C,B,B,C order, two warmups then four timed calls, median of worker medians. All 72 workers / 288 timed samples completed. Same native 1,000-row × 2,535-column fixture/four partitions; generic explode 400 × 100. Rayon 4, existing Tokio 32, BLAS 1, Rust/PyArrow mimalloc. Query construction/read/action included. No builds, tests or profile overlapped comparative timings.

| Workload | Before wall ms | After wall ms | Wall change | Before CPU ms | After CPU ms | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| entries_show | 18.433 | 17.814 | -3.36% | 55.324 | 32.375 | -41.48% |
| entries_prefix | 16.382 | 15.690 | -4.22% | 55.416 | 16.070 | -71.00% |
| take_entry | 8.331 | 7.172 | -13.91% | 30.071 | 8.210 | -72.70% |
| entries_count | 21.460 | 22.166 | +3.29% | 69.513 | 71.629 | +3.04% |
| entries_count_no_key | 24.206 | 24.252 | +0.19% | 75.119 | 73.985 | -1.51% |
| explode_prefix | 1.073 | 1.039 | -3.11% | 1.273 | 1.057 | -16.97% |
| explode_full | 0.670 | 0.699 | +4.22% | 0.861 | 0.904 | +5.06% |
| filter_entries | 48.500 | 48.738 | +0.49% | 198.071 | 197.582 | -0.25% |
| split_multi_hts | 149.487 | 150.024 | +0.36% | 392.607 | 391.933 | -0.17% |
| read_rows | 13.593 | 13.906 | +2.30% | 63.819 | 64.430 | +0.96% |
| read_prefix_all | 29.083 | 25.168 | -13.46% | 68.912 | 26.167 | -62.03% |
| explode_prefix_all | 1.116 | 1.135 | +1.75% | 1.318 | 1.173 | -10.93% |

The main benefit is reduced CPU work for bounded actions: entries8192 uses 71.00% less CPU, GT.take100 uses 72.70% less, and entries-show uses 41.48% less. Wall reductions are 4.22%, 13.91%, and 3.36% respectively. This is not a large entries-show latency win or a Hail/full-suite comparison. A native rows.head1000 control consumes the entire fixture and improves wall 13.46% / CPU 62.03%; this does not imply every full read is faster.

All adverse controls are retained: keyed entries count wall+3.29%/CPU+3.04%, native full row count+2.30%/+0.96%, filter entries wall+0.49%, split_multi_hts wall+0.36%, generic full explode wall+4.22%/CPU+5.06%, and all-output generic prefix wall+1.75%. The generic calls are approximately 0.7–1.1 ms and weak throughput evidence. No regression is dismissed as noise or assigned an unmeasured cause.

Per-case worker hashes match between versions for display text, GT repr and returned counts. They do not establish full Arrow payload/schema equality; public correctness tests provide separate coverage.

Correctness: 13 release Rust tests PASS (prefix 4, consumer_actions 6, reader_batches 3), including skipped later files, zero, independent prefix/full consumers, partition/offset order, empty/null owners followed by qualifying entries, replay rejection and fresh-action success. Actual live native requested-range counters show 2,449 bytes for prefix versus 9,844 for full on the public four-file fixture. This proves that fixture's requested-byte reduction, not universal physical-I/O/codec/RSS reduction. RowCount alone does not guarantee avoiding all within-file decompression.

The final normal Python artifact passed 50 selected show/explode/head/order/random tests; 125 deselected. One duplicate-multikey test fails with ScalarSubqueryExpr-before-execution on both final and old native artifacts. The isolated baseline counterfactual is retained; this existing independent failure is not fixed or labeled green.

Final source 3fc119c73955537e5227b27875fcf125b3493484, normal native 585cc05089ed159e38f700e0494a9c6ada12f86de48fbf0f664671356e7a882b. Baseline main a8dd / production 61ec, normal native 4031dcb7146f593be5309dcedf0972231ebc021087a7a40dff0bc82bbb7132e0. Frozen source was normally built/installed with kache; no saved .so cache. The initial overlapping build was excluded and final frozen source rebuilt. source-sha256.json is a post-build unchanged-Git-content verification, not a pre-build enrollment claim.

A separate real five-second samply recording of final native 585 completed after timings: 35 threads / 24,189 samples / 19,657 frames. It includes startup/two warmups then repeated entries-show and blocked threads; no sample-count CPU attribution is claimed. Recording and symbols reside in the durable visualization directory entries-show-prefix-demand, with identities in profile-proof.json.

Exact reviewer: 01a14128-21b8-7a53-b399-4ad8f9b31f4f. The first execution-time rewrite reset ConsumerScope claims; follow-up 3fc prepares once to preserve replay guards. Source and raw benchmark math were independently reviewed. Final documentation/receipt admission is recorded separately. Raw local build/test/failure logs remain in /private/tmp/vail-prefix-demand-20261109; benchmark JSON records/scripts and identities are retained here. No cloud work, GitHub PR or remote push.
