# H058: direct filtering for pure collection aggregations

Production source `3c1669c6`, final public tests `e6fae11a`, based on main `0eec4077`. `array.aggregate(lambda x: agg.filter(predicate, ...))` previously built a Boolean list, zipped it with the input values, filtered the pair structs, and mapped the original values back out. For pure inputs/predicates, rewriting now emits the original `ArrayFilter(input, binder, predicate)` directly. The existing DataFusion 55 higher-order filter supplies the vector kernel; no custom traversal or SIMD is introduced.

A conservative query-time effect check selects the path. Random-bearing inputs, predicates or aggregate bodies retain the historical seeded UID/evaluation path. Potentially failing inputs/predicates retain their previous path. The central effect registry also marks the virtual diploid genotype-index accessor as potentially failing, because haploid Calls reject that operation. Ordinary struct fields with the same reserved name receive a conservative barrier. Casts and unknown operations remain conservative; this change does not establish complete typed purity proofs.

Call path: Python `array_aggregate.aggregate` → native `collection_aggregate::Rewrite::walk` → `Expr::ArrayFilter` → existing physical `lower_array_lambda` → DataFusion's native `array_filter` and Arrow filtering. Selection happens during query construction. Data-dependent missingness, offsets and predicates remain in the existing kernels. The generic higher-order logical-expression boundary still uses a semantic kernel; this change reduces intermediate IR/operators and reuses DataFusion's physical HOF implementation, without claiming full logical HOF transparency or storage pushdown.

This affects explicit collection aggregations, including per-entry AD/PL summaries. General matrix row `agg.filter` already uses direct filtering in its separate lowering path.

## Correctness

91 unique public tests pass on both baseline and candidate, with the same one baseline failure and no skips or new failures. The failing existing test is `test_collection_aggregate_matrix_scope_and_lookup_context`: a literal-array matrix callback capturing row/column fields produces a semantic-kernel cardinality error, expected 3 rows and got 1, before reaching its lookup assertion. Its complete trace and comparison are retained.

All 11 new cases pass again on exact final test source for both versions. They cover integer/float types, null/empty lists, missing predicates, null elements, nullable and zero-field structs, nested filters, row/global/lexical captures, shared literal arrays, dynamic matrix row/column captures, stored sparse matrix entries, selected/unselected domain errors, and frozen seeded RNG results. Unordered aggregate collections are compared as multisets. Tests evaluate public actions against independent expected values; there are no plan-shape assertions or private production oracles.

## Native entry workload and results

Existing native MatrixTable: 24,885 rows × 2,535 columns = 63,083,475 logical cells, entry schema AD/DP/GQ/GT/PL. Entire fixture size is 784,316,678 bytes; queried column read-byte totals were not measured. PRIMARY VM `vail-bench-20261021`: 32 vCPUs, AMD EPYC 7B13, 128 GB RAM. Builds, tests, timing and profiling ran sequentially with no overlapping workload on this VM.

Queries use ordinary public collection aggregations over stored AD/PL arrays, followed by entry aggregation. No source-format or benchmark-name matcher exists in production. All iteration results agree between versions.

The initial scan used one warmup and five measured evaluations per fresh process. Because improvements were modest, a second verification used alternating baseline/candidate/candidate/baseline fresh processes, each with one warmup and three measured evaluations. The following table reports the median of per-process medians from that alternating verification; every process and iteration is retained.

| Query | Baseline wall | Candidate wall | Speedup | Baseline CPU | Candidate CPU | CPU reduction |
|---|---:|---:|---:|---:|---:|---:|
| AD values > 0: sum per entry, then total | 452.71 ms | 429.72 ms | 1.05× | 12.039 s | 11.685 s | 2.9% |
| AD values > 3: sum/count bundle per entry, then total | 838.40 ms | 778.58 ms | 1.08× | 22.147 s | 21.109 s | 4.7% |
| PL values < 50: count per entry, then total | 338.64 ms | 315.36 ms | 1.07× | 9.022 s | 8.324 s | 7.7% |

Initial-run wall speedups were 1.01×, 1.07× and 1.10× respectively; CPU reductions were 2.3%, 5.1% and 6.7%. AD sum improves only slightly. These are modest general improvements, rather than a solution to the larger reduction cost.

Initial peak RSS changes range from about +1% to −2%; no substantial capacity improvement is claimed. Minor page faults fall for AD sum and PL count, while the bundle is variable across runs. Page faults are software counters, not allocation counts or measured DRAM traffic. Disk bandwidth, cache/ALU/SIMD utilization and hardware-event saturation were not measured.

## Profile and higher-priority follow-up H092

Separate 99 Hz software CPU-clock profiles use 12 evaluations and include startup. The obsolete `arrays_zip` frame disappears, but it was only 0.01% exclusive; its removal is not a large CPU attribution. Transform work remains because reducer projections still need mapping.

The important discovery is repeated tiny-list reduction setup in `list_aggregate/direct.rs`: the numeric macro creates a `PrimitiveArray::slice` for each list before invoking Arrow sum; mean and Boolean reducers follow similar patterns. Baseline exclusive samples show `UnalignedBitChunk::new` 11.86%, Int64 slice 7.61%, ScalarBuffer construction 4.85%, NullBuffer slice 3.13%, and BooleanBuffer slice 2.40%, totaling 29.85%. These remain in the candidate. They represent per-list header/refcount/null-count/kernel setup; per-list heap allocation has not been established.

The durable audit queue records this as P1 H092, separate from H011's full-child widening. A prepared segmented raw-span strategy, reusing existing integer-list-sum machinery where semantics match, should avoid constructing Arrow headers and reinitializing tiny reductions per entry. This finding requires its own implementation and resource gate.

## Normal build and provenance

Baseline uses the existing normally built `0eec4077` environment from the H043 run, with inherited provenance retained. Candidate builds normally with kache/maturin, 16 jobs, incremental disabled, identical `-C target-cpu=native`, stable source path `/home/danielking/finite-stddev-20261024/source/vail` and preserved target `/home/danielking/cloud-scan-20261023/target`. Normal candidate wheel build took 1m27s. No second test-feature graph was built for this planning/IR change; the normal native wheel runs the public gate.

Before overlaying source, the build records file hashes. Changed Rust/Cargo sources are touched after extraction, avoiding stale Cargo artifacts from archive mtimes. Source hashes match this branch, and installed native hashes match each corresponding normal wheel and differ between versions. No compiled native artifacts were copied or manually reused as a cache workaround.

The source and native provenance, changed-file list, complete JUnit outcomes, build log, initial/alternating iterations and compressed perf reports are retained. Raw perf data remains under `/home/danielking/h058-filtered-aggregate/results/` on the VM. Included scripts reproduce the sequence against that preserved fixture and source/target paths. Final required source/evidence review and main integration remain with the root agent.
