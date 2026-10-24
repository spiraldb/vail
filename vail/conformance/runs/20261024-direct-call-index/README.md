# H086: direct prepared Call indexing

Baseline `0eec4077`; production source candidate `8f4ba41b`. Ordinary `GT[i]` and explicit `GT.alleles[i]` previously constructed a List<Int32> for every Call, built take indices, and gathered one allele. Lowering now selects a direct typed primitive extraction for every such expression, regardless of its consumer. No benchmark-specific aggregate rule or new serialized IR is introduced.

Common constant indices 0, 1, -1 and -2 select const-generic kernels once per query. Impossible constant indices select a separate error/missing strategy. Dynamic indices use a typed vector input. Index storage and actual logical validity are bound before traversal; dense and nullable loops are separate monomorphizations. Per-row ploidy, requested dynamic index, actual missingness and bounds remain data-dependent. The result is one Int32 vector; Call input buffers are borrowed. No handwritten SIMD or unsafe indexing is added. Public allele-list requests and Call construction retain their existing behavior.

## Correctness and review

88 public tests pass: 11 new direct-index cases plus 43 existing Call/genotype-index cases, and 34 adjacent genotype-likelihood/split-multiallelic cases. Coverage includes native table/matrix round trips, nested arrays, entry aggregation, sparse entries, dynamic indices, missing calls/indices, lazy filtered errors, UInt16 boundaries, haploid/diploid/zero ploidy and negative/out-of-bounds indexing. Collected entry values are checked as per-row multisets. Native functional tests use independent explicit expected values/errors through public lowering/evaluation, without plan-shape assertions or private production oracles.

Two native functional tests and a real-allocator integration test pass on exact final source after explicit source-timestamp invalidation. Source review of the initial implementation was clear; requested test changes were addressed. The final five-line invariant-binding cleanup and final evidence await follow-up review. Main has not been changed by this agent.

## Representative native entry workload

Preserved native MatrixTable: 24,885 rows × 2,535 columns = 63,083,475 logical cells; entry schema AD/DP/GQ/GT/PL. Entire fixture is 784,316,678 bytes; this is not a claim that a GT-only query reads every stored column. VM: `vail-tests-20261023`, 32 vCPUs / 16 physical AMD EPYC 7B13 cores with SMT. Baseline and candidate ran sequentially on the otherwise quiet VM, with no concurrent builds or workloads.

Each fresh process executes one warmup and five measured evaluations. Table values are medians of the measured evaluations; process startup is excluded. OS cache is warm. Both versions return identical results on every iteration. These are Vail-before/Vail-after measurements, not a Hail comparison.

| Query | Baseline wall | Candidate wall | Speedup | Baseline CPU | Candidate CPU | CPU reduction |
|---|---:|---:|---:|---:|---:|---:|
| Total SUM(GT[0]) | 296.27 ms | 226.60 ms | 1.31× | 7.636 s | 6.045 s | 20.8% |
| Total SUM(GT[-1]) | 295.80 ms | 224.82 ms | 1.32× | 7.882 s | 6.022 s | 23.6% |
| Total SUM(GT[GT.n_alt_alleles() % 2]) | 317.39 ms | 259.72 ms | 1.22× | 8.285 s | 6.859 s | 17.2% |
| Per-row SUM(GT[0]), then total | 317.39 ms | 247.68 ms | 1.28× | 7.965 s | 6.130 s | 23.0% |

First-query peak RSS is 959,932→946,308 KiB; other shapes show similarly modest changes. Do not infer a large whole-query memory-capacity reduction. The real allocator test isolates 8,192 calls and records 33,256 allocated bytes, approximately 4.06 bytes per result including bounded expression bookkeeping. It rejects additional allele-list payload, offsets, expanded constant indices or take-index vectors. DRAM/disk bytes and cache traffic were not measured.

## CPU profiles and limits

Separate software CPU-clock profiles use 99 Hz, DWARF stacks, 15 evaluations and report no lost samples. Baseline has 11,749 samples: exclusive Call accessor 8.72%, array-index rows 4.73%, and Int32Builder append_value 4.42%. Those three frames are absent in the candidate; direct Call index is 3.62% exclusive. Native UInt32→UInt16 conversion is now 5.34%, and offset validation remains 3.27%. Other source conversion, allocation and aggregation work remains; this is not a claim that indexing was the whole bottleneck. Profiles include startup and unequal total CPU work, so percentages are not direct absolute-time comparisons.

Hardware cycles, instructions and cache events report `<not supported>` on this VM. No ALU, SIMD, bandwidth or cache-saturation claim is made. The implementation removes intermediate vectors and scalar list-builder work; resource reduction is supported by allocator counts, CPU time and disappearance of those profile frames.

## Build and provenance

Both builds use normal kache/maturin release compilation, 16 build jobs, incremental disabled, identical `-C target-cpu=native -C link-arg=-fuse-ld=lld`, the preserved target and stable source path. Dedicated baseline/candidate environments are installed from their normal wheels; installed native hashes match the corresponding wheel contents and differ between versions. No compiled native artifacts were copied or manually reused as a cache workaround.

Baseline normal wheel build took 1m25s. Final normal wheel rebuild took 1m13s. Initial native-test feature graph took 2m07s; subsequent test commands initially appeared warm, prompting an explicit source-timestamp invalidation recheck because extracted git archives can predate cached test artifacts. Preserve this precaution for future archive-based builds: touch changed source files after extraction before compiling, then record source hashes and actual build output. The final wheel/public/profile gate compiled changed source normally; timestamp rechecks apply to the separate native-test graph. Both forced native test rebuilds completed successfully in 25 seconds each; their post-invalidation source hashes and completion marker are retained.

`source-provenance.json`, `results/native-provenance.json`, per-version source hashes, normal build logs, test logs, every iteration and compressed perf reports are retained. Raw perf data remains on the VM under `/home/danielking/h086-direct-call-index/results/`. Historical launch scripts are included; their PID guards and VM paths describe this run rather than a portable runner. General reproduction: normal-build baseline, run `h086-measure.py` for each query sequentially, normal-build candidate in the same source/target with source mtimes invalidated, run public/native tests, and repeat the identical measurement commands after all build/test work ends.

The original H086 also noted temporary allele lists in fixed-ploidy Call construction. That separate subtask remains open; this candidate fixes indexing.
