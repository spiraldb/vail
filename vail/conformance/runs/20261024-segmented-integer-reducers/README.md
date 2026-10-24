# Prepared segmented integer reducers

Source candidate: d6c9a42c48d0c8deffc81876c4f55e303cd4ff11, based on reviewed Call/stddev main plus H058 integration28406dfb. Development branch dk/prepared-segmented-reducers. Review and main integration pending.

H092 baseline profile identified Arrow per-list array slicing, bitmap slicing and null-count setup on tiny entry arrays. This change prepares integer sum/product/min/max source width and operation during DataFusion logical simplification, or physical lowering for higher-order bodies that do not enter the logical optimizer. Each batch binds borrowed primitive values and offsets and chooses parent/child logical-validity variants. Reduction traverses those spans directly. It creates no per-list Arrow header, bitmap slice or configured reducer; the output builder is allocated once per batch. There is no handwritten SIMD or unsafe code. Dense integer traversal leaves vectorization to LLVM; optimized assembly inspection is part of the resource gate. Integer arithmetic wraps as Arrow does; int32 sum/product accumulates in int64 registers.

This is a partial H092 fix: floating-point, mean and Boolean reducers retain Arrow's original kernels. Arrow's floating-point lane reduction order must be preserved by any later change. The separate integer_list_sum same-width path also remains to consolidate. Frontend mapped numeric widening (H011) is not removed by this patch.

Public tests cover missing lists, empty/all-missing children, sliced lists, int32 widening, int64 overflow, min/max result width, filtered matrix entry aggregates and nullable child bitmaps spanning multiple words and partitions. New tests first passed on the unchanged baseline. The pre-existing matrix literal-array callback cardinality failure remains unchanged and is recorded rather than skipped.

Machine and data: PRIMARY vail-bench-20261021, 32 AMD EPYC7B13 vCPUs, about126GiB memory. Native Hail-derived1kg matrix profile25_mt.mt,24,885rows×2,535cols=63,083,475logical cells; AD/PL are int32 arrays. Snapshot784,316,678bytes; projected queries do not necessarily read all bytes. Baseline is the normal H058 integration wheel; candidate is a normal release maturin/kache build in the same stable source/target, flags,features and interpreter ABI. Changed source content is hashed and touched after overlay to prevent stale Cargo artifact reuse. No native library was manually copied.

Measurement receipts and final source/native provenance are in results/. Paired fresh processes use A/B/B/A order, one warmup and three recorded iterations per process/query. CPU profiles use software CPU-clock99Hz with DWARF call graphs; profile timing is separate from the paired timing measurements. Builds, tests and timed/profile work run sequentially. Hardware PMU counters are unavailable on this VM; no ALU saturation, DRAM bandwidth or cache-miss rate is inferred from software samples.

## Results

| Native63M-entry pipeline | Baseline wall | Candidate wall | Speedup | CPU-seconds baseline→candidate |
|---|---:|---:|---:|---:|
| Per-entry filtered AD sum, global sum |424.7ms|319.3ms|1.33×|11.45→8.56 (25.2%less)|
| Per-entry filtered AD sum+count bundle, global sum |772.5ms|585.0ms|1.32×|20.85→15.42 (26.0%less)|

Exact results remain353709503 and348882629. These are improvements over the H058 integration baseline, not Hail-relative multipliers. MaxRSS was−2.3% for sum and+0.2% for bundle: no substantial capacity claim. Minor faults were lower in these measurements (19.5%/3.2%); both versions had zero major faults. They are observations, not measured bytes moved or heap-allocation counts.

Normal warm build:1m21s. Matching public gates:98pass and the same one existing matrix literal-array callback cardinality failure on both versions; zero skips/new failures. All seven new cases pass on both.

Software CPU-clock profiles have zero lost samples, about14K baseline/10K candidate samples, and141.6/104.4CPU-seconds event count. Baseline Int64Array::slice8.04%, ScalarBuffer<i64>::new4.69%, and NullBuffer::slice3.15% are no longer observed at the candidate report's0.01%threshold. UnalignedBitChunk::new remains12.85%→9.38%, and BooleanBuffer::slice2.75%→3.77%: absolute BooleanBuffer slicing time is roughly unchanged because filtering still uses that path. The patch does not remove all bitmap work.

The selected int32-widening SUM kernel is present in the candidate profile (3.47%exclusive); its input widening occurs in registers. Optimized assembly contains compiler-generated vpmovsxdq/vpaddq for long dense spans. Short spans use the scalar remainder. No architecture intrinsics, manual vector lanes, schema/type/opcode dispatch or Arrow-slice helper calls occur in the child arithmetic loop. Nullable variants retain actual validity-bit tests; parent-missing tests and output appends are necessary per-list data work. The Arrow output builder reserves exactly the list count once per batch; capacity checks remain library behavior, with no per-list configured reducer or temporary array.

Raw perf files remain on PRIMARY at /home/danielking/h092-segmented-reducers/results/*-perf.data, hashes in results/raw-profile-sha256.json. Compressed flat/callgraph reports and the complete optimized SUM assembly are included. Installed native hashes match each version's normally built wheel. The candidate source hashes match d6c9a42c exactly. Profiling uses sudo; this perf build needs -f on report for file ownership, not on record. Initial failed profiler/report launches did not alter paired timings.
