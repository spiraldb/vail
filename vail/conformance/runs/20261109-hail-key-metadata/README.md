# Hail key metadata: laptop grouped-collect comparison

Final combined source: `46d95c159f356585c15f54de0f0facb47a913e90`. Candidate native: `2723af5a47571de26bf525e3960aa3d27e09505b31a9d5b6e979716bf564338e`; baseline native: `d7119f220c917bbb5fc2dc0511d89c04bdfc2947a9ad8e4e917775952112be6e` (normal release MiMalloc builds).

The production metadata fix reuses `MatrixTable._with_ordered_row_keys` when importing Hail matrices. Localization keeps the existing key order and source partitions instead of asking for a new global stable sort. It does not infer uniqueness or certify new key ranges.

This is the unchanged grouped-collect-per-row query shape on the existing public profile subset: **1,000 rows × 2,535 samples, four partitions**, with native and Hail-format copies. It is **not** the full 250,000 × 1,000 random gnomAD fixture. Three fresh processes per version/case/format, balanced B,C,C,B,B,C order, two warmups and five timed calls per process: 24 workers, 120 timed calls. All completed with output count 1,000. Read/query construction/action are timed; imports are excluded. No other task-owned builds, tests, profiles or timings overlapped this cohort.

| Input / action | Baseline wall ms | Candidate wall ms | Wall change | Baseline core-s | Candidate core-s | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| legacy / grouped | 171.997 | 89.444 | -48.00% | 0.301094 | 0.262856 | -12.70% |
| legacy / read | 43.489 | 44.933 | +3.32% | 0.142701 | 0.142984 | +0.20% |
| native / grouped | 40.260 | 40.390 | +0.32% | 0.132181 | 0.129586 | -1.96% |
| native / read | 13.619 | 14.157 | +3.95% | 0.064077 | 0.064789 | +1.11% |

**Hail-input grouped collect improves 1.923× in wall throughput and uses 12.70% fewer core-seconds.** Each of its three worker medians improves; baseline 170.65–173.77 ms, candidate 88.96–91.08 ms. Adverse controls remain explicit: Hail-format read +3.32% wall, native grouped +0.32% wall, native read +3.95% wall. These are descriptive measurements with three workers, not significance tests or noise/causality claims.

Both versions use mimalloc, Rayon threads=4 and BLAS thread limits=1. The existing Tokio runtime has 32 workers; VAIL_EXECUTION_CONCURRENCY=4 is a recorded environment entry that the runtime ignores, not an effective four-worker cap. CPU is process CPU across all threads. RSS, if consulted in raw records, is whole-worker high-water memory including imports/warmup, not operator allocation.

This compares the final combined candidate (metadata, parallel sort and entry-run changes) against the unchanged benchmark baseline; it does not isolate causal credit for each commit. External metadata plan diagnostics on the earlier combined `32bfeb1bd` wheel (identical metadata Python source) confirm localized 4→4 partitions, no preaggregation global ordinal/sort, and aggregate input partitions 4 rather than baseline 1. The grouped path still uses hash repartitioning: native physical source-order/range proofs are **not** fully propagated by this one-line change. Remaining decoder and hash/shuffle costs are not isolated here.

Correctness is separate: root reported the corrected combined Python gate **299 passed**, including all Hail compatibility corpus tests and the new public two-part duplicate-boundary regression. The new regression fails on the baseline because localization collapses two partitions to one. Private plan diagnostics are external, not test assertions.

Raw records and every worker median: `records.json`, `summary.json`, and the original `metadata-timing.log` pinned by `evidence-hashes.json`. No tuning or reruns. No cloud resources or main updates by this agent.
