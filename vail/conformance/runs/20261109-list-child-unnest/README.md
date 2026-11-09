# Borrowed list-child unnest: laptop comparison, 2026-11-09

Single depth-one List/LargeList unnests with null/empty lists dropped now borrow contiguous child slices. Hidden values under null outer lists are skipped, and child validity is retained. Parent fields expand together through one struct run-end array per output chunk, decoded into the ordinary DataFusion output schema; nonconsecutive parent rows share one gather index instead. Output chunks are bounded by the session row batch size. Multiple-list, recursive, struct and null-preserving unnests retain DataFusion’s existing implementation. This is child-payload borrowing, not universal zero-copy, retained RLE output, or a byte-sized memory guarantee.

Three fresh workers per version/case, balanced B,C,C,B,B,C order; two warmups then four timed calls each. Reported values are medians of worker medians. Same persisted 1,000-row × 2,535-column native fixture with four partitions; generic explode uses 400 rows × 100 values. Rayon four threads, existing Tokio32, BLAS one thread, Rust/PyArrow mimalloc on both versions. Query construction/read/action included; imports excluded. No compiler, tests or profiler overlapped comparative timings. All 60 workers exited successfully; all 240 timed samples are retained.

| Workload | Before wall ms | After wall ms | Wall speedup | Before CPU ms | After CPU ms | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| entries().show() | 24.761 | 19.037 | 1.301× | 92.584 | 56.691 | -38.77% |
| entries().head(8192).to_arrow() | 32.481 | 16.365 | 1.985× | 128.826 | 54.938 | -57.35% |
| GT.take(100) | 13.666 | 8.057 | 1.696× | 54.558 | 29.001 | -46.84% |
| entries()._force_count() | 21.299 | 21.020 | 1.013× | 69.503 | 70.600 | +1.58% |
| Keyless entries count | 24.037 | 25.377 | 0.947× | 75.554 | 75.757 | +0.27% |
| Generic explode prefix | 1.047 | 1.017 | 1.029× | 1.217 | 1.203 | -1.15% |
| Generic explode full | 0.660 | 0.598 | 1.104× | 0.817 | 0.811 | -0.67% |
| filter_entries count rows | 48.182 | 49.031 | 0.983× | 196.045 | 197.504 | +0.74% |
| split_multi_hts count rows | 147.006 | 151.347 | 0.971× | 386.566 | 396.487 | +2.57% |
| Native read/count rows | 13.590 | 14.106 | 0.963× | 63.871 | 63.906 | +0.05% |

Adverse observations remain part of the result: keyless count wall +5.58%, native read wall +3.79%, split-multi wall +2.95%/CPU +2.57%, and filter wall +1.76%/CPU +0.74%. No causality, significance or noise-dismissal claim is made for these small differences. Generic explode timings are around or below one millisecond and do not establish a substantial throughput win. This is a warmed local subset comparison, not a full-suite rerun or Hail comparison. Worker result hashes match between versions for all ten cases; these compare displayed text/counts/taken values, not full cross-engine conformance.

Validation: two new public TableIR parallelize/explode/collect tests passed (nullable parents/children, hidden null-list spans, nonzero child offsets, and a 30,000-value list crossing output chunks). Twenty-two focused Python explode/entry/filter/show tests passed; 80 deselected, with the previously documented duplicate-multikey ScalarSubquery failure excluded. The ordinary fixture’s diagnostic physical plan contains ListChildUnnestExec.

A separate three-second same-user sample of repeated warmed entries-show confirms the old Unnest child-gathering hotspot disappeared. Before: 2,338/4,600 known-nonwait leaf observations under DataFusion Unnest, 1,858 under Arrow take. After: 7/3,707 under DataFusion Unnest (planning included), 746 under the new stream, 237 under Arrow take and 199 under run-array decoding. Categories overlap; sample observations are attribution evidence, not calibrated CPU-time percentages or ratios. Vortex reading now accounts for 1,921 observations. Parent decoding still materializes the plain arrays the downstream schema requires. No per-entry setup was introduced.

Production source reviewed: c02cf531e9793678af7d47297bf9a7c3b60c70cf; baseline e91db58f production-equivalent normal artifact 2723af5a47571de26bf525e3960aa3d27e09505b31a9d5b6e979716bf564338e. Candidate normal release artifact 0ee6087f01e60b2ee87aabc573939d667b6942bf5ef5d40c16d7546479c1efb3. Both built normally using kache. Candidate build began with c02 source edits uncommitted on parent e8cbe5451; ready.json records that parent HEAD. The unchanged source was committed as c02 during the build; no independent prebuild source-content manifest is asserted. The observed wheel/native and reviewed source history are recorded separately.

Raw scripts, worker samples, build/test logs, source plan and sampled stacks: /private/tmp/vail-list-child-unnest-20261109/. Descriptive summary is in summary.json; profile counts in profile-categories.json. The acknowledged temporary StructArray header on no-repetition chunks is a deferred optional micro-optimization, not required for semantic correctness.
