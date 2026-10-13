# Prefix sum scan profile and optimization — 2026-10-13

## Result

The 1M-row prefix sum scan median fell from **1.5198 s to 0.8355 s** with the optimized Vail release wheel (45.0% lower; 1.82× faster). Stock Hail's prior median was **0.2179 s**, so the optimized Vail workload remains 3.83× slower in this harness. All five optimized iterations passed the harness's row-count assertion. The fast path preserves Hail's wrapping `int64` behavior; targeted Rust differential tests compare it with the generic accumulator finalizer for integer overflow, float sums, null outer states, all-null zero summaries, and empty arrays.

## Evidence and diagnosis

The baseline 500 Hz Samply profile attributed 34.4% of inclusive samples to `ScanState::finisher`, 19.1% to `ScanState::accumulator`, and 22.7% to `ScalarValue::iter_to_array`. Its flamegraph also showed substantial allocator/free activity. The finalizer built an accumulator and boxed one-row state for each output row. The optimization combines the sum state value columns with Arrow array kernels: wrapping addition for `Int64`, IEEE addition for `Float64`, and zero substitution for null outer states. Other reducers and validation finishers continue through the generic path.

The final exact-source after profile no longer contains `ScanState::finisher` or `ScanState::accumulator` among its inclusive hotspots. Remaining sampled CPU is concentrated in `HailAccumulator::update_batch` (24.6%), `StateAccumulator::evaluate` (17.8%), and `ScalarValue::iter_to_array` (34.3%); this is per-row scan state production and allocation, which the finalizer change does not remove. This explains why the scan improves substantially but remains slower than Hail.

| Capture | Median wall time | Average cores | Peak RSS | Peak physical footprint | 10 Hz samples |
|---|---:|---:|---:|---:|---:|
| Baseline, unprofiled | 1.3790 s | 1.85 | 672.9 MiB | 131.1 MiB | 69 |
| Baseline, Samply 500 Hz | 1.3899 s | 1.80 | 563.7 MiB | 87.6 MiB | 76 |
| Optimized, unprofiled compare harness | 0.8355 s | — | — | — | — |
| Optimized, Samply 500 Hz | 0.8800 s | 1.84 | 620.6 MiB | 126.4 MiB | 44 |

The optimized benchmark ran with 3 warmups and 5 iterations, 14 configured cores, 1,000,000 rows, and 14 requested source partitions. Its final exact-source iteration times were 0.9604, 0.8355, 0.8054, 0.7723, and 0.9069 seconds. The host load averages recorded by the harness were 6.8–19.3; no competing benchmark was intentionally started, but these are not isolated-host timings. The final matched profiled run measured 0.8800 s.

`proc_pid_rusage` exposed per-process CPU time, RSS/physical footprint, instructions, and cycles; cache-miss counters were unavailable. The baseline profiled median rates were 27.74B instructions/s and 5.76B cycles/s (IPC 5.10); the optimized profiled rates were 27.55B instructions/s and 6.19B cycles/s (IPC 4.49). Sampling intervals stayed close to 100 ms (optimized maximum 105 ms).

An initial after-profile attempt in `after-profiled/` used mismatched Python dependencies (NumPy 2.2.6 / PyArrow 23.0.1 instead of baseline NumPy 2.4.6 / PyArrow 26.0.0) and reported a 4.03 s median; it is retained for audit but excluded from the comparison. `after-profiled-matched/` is an intermediate matched-dependency profile. `after-final/` and `after-final-profiled/` are the final exact-source benchmark and profile after changing Int64 combination to wrapping addition and adding the state-layout guard.

## Artifacts

- [Baseline flamegraph](before-profiled/flamegraph.svg), [baseline telemetry](before-profiled/telemetry.svg)
- [Final optimized flamegraph](after-final-profiled/flamegraph.svg), [optimized telemetry](after-final-profiled/telemetry.svg)
- [Final optimized benchmark JSON](after-final/benchmark.json), [profile summary](after-final-profiled/summary.json), [hotspots](after-final-profiled/hotspots.json), [10 Hz metrics](after-final-profiled/metrics.csv)
- Raw Samply profiles and symbol sidecars are retained in both profile folders.

## Provenance and limitations

- The prior baseline native SHA-256 is `b76d138b85952ccf2b67945a5c15e29d9cd676e26128066a7706282f2efaa32f`; prior records identify that installed binary with runtime commit `464265f3ac864729a4f2115e10492e3044f5d400`.
- Final optimized wheel: `/private/tmp/vail-scan-final-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl`, SHA-256 `5f27ed61b812c24dcddd688efdb12694a9cc57f7cd07d07f3243df531d9f0a3a`. The installed optimized native module SHA-256 is `1e2803ff2825949749b7d74c7ef5df450040444b031b6b9cd45b7431502d2859`. Package metadata reports `0.1.0a1` and does not identify a source commit. The wheel was built from the shared worktree based on `6c979cd6769e23d22efbed091a2137d2873b6a5b` plus uncommitted changes; the baseline and optimized native hashes differ.
- Both compared Python runs used Python 3.11.6, arm64, NumPy 2.4.6, PyArrow 26.0.0, with 14 configured cores. Machine inventory records Mac15,10, 14 ARM cores, and 96 GiB RAM. No Hail build was performed; the Hail 0.2179 s value is from the prior same-day benchmark report.
- The optimized comparison harness source is `benchmarks/compare_hail.py`; each iteration constructs a lazy scan and forces row count. The profiling workload likewise constructs and forces the 1M-row scan. The timing evidence is local and subject to host load and filesystem/runtime state.
