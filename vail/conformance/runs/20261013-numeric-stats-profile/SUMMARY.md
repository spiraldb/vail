# Numeric stats profile and optimization

## Result

The existing `table_stats` workload aggregates 10,000,000 `float64(idx)` rows into Hail-style stats (count, sum/mean, population standard deviation, minimum, maximum). With 14 source partitions, Vail’s median fell from **0.7399 s** before to **0.1877 s** after the accumulator batch fast paths, a **3.94× speedup (74.6% less time)**. The previous Hail result in `20261013-hail-vail-rerun-v1` was **0.2475 s**, so this Vail run is 24% faster than that baseline. All measured outputs matched the benchmark assertions.

## What the profile showed

The before profile sampled `ScalarValue::try_from_array` at 14.9% of leaf CPU, `HailAccumulator::update_batch` at 14.5%, and DataFusion population-variance batch update at 9.7%. The Vail aggregate adapter was iterating arrays row by row, boxing every value as a `ScalarValue`; countWhere and min/max accounted for the relevant avoidable work.

The change adds an Arrow boolean `true_count()` path for countWhere and Arrow min/max batch reductions for supported primitive types. Floating batches containing NaN retain the previous scalar path so existing NaN ordering behavior stays intact. Integer sum already had an Arrow batch path. After the change, the aggregate path is faster than the prior Hail measurement. Remaining leaf hotspots include DataFusion variance update (19.0%), Hail accumulator update (16.5%), `ScalarValue::try_from_array` (13.1%), and Float64 downcasts (9.7%). The retained scalar conversions are from other reducers in the stats expression; this profile does not justify changing floating sum’s order-sensitive behavior.

## Measurement details

Machine: Apple ARM64, 14 CPU cores, 96 GiB RAM. Both benchmark runs used 14 requested source partitions, 3 warmups and 5 measured iterations. The unprofiled before and after medians were 0.739870 s and 0.187656 s; the after profile’s median was 0.188511 s. The profiled after process averaged 0.986 cores, peaked at 99.6 MiB RSS and 39.4 MiB physical footprint. Its monitor sampled at 10 Hz (9 samples during measured work; median interval 0.100006 s, maximum 0.102733 s). Sampled instructions/sec were 15.85B, cycles/sec 3.72B, IPC 4.26. `proc_pid_rusage` exposes instructions and cycles here but not cache-miss counters.

The after wheel was built locally from the current working tree with `maturin build --release --locked --offline -j 4`; it was not installed into the shared benchmark environment. Wheel SHA-256: `67ed162d0e4e8ea117572d583e7252f01fb601e4e19380739831c43354d3452d`. Native `_native.abi3.so` SHA-256: `b20dff157b3c5db02359edecbc9f7af233878593cb3f1e829014b04130f3bef7`. Package metadata still reports `0.1.0a1`, which is not source provenance. The baseline installed native SHA-256 was `b76d138b85952ccf2b67945a5c15e29d9cd676e26128066a7706282f2efaa32f` (recorded runtime commit `464265f3ac864729a4f2115e10492e3044f5d400`). The Vail repository was not committed as part of this task.

The timed after wheel predates a follow-up source revision that removes the finite-only min/max fallback and uses Arrow-native min/max semantics for all float batches and accumulator-state merges. The follow-up passed the aggregate Rust tests (13/13) and the aggregate/P1 Python tests (38/38) against the newly built native extension. The 10M-row workload contains only finite values, so its timed code path and measured output are unchanged by that policy revision; the timing should nevertheless be attributed to the recorded wheel hash above, not to the later source revision tested in the development extension.

Samply initially returned `Unknown(1100)` without elevated profiling access; it succeeded when rerun with access to sample the process. Both before and after symbolized profiles and flamegraphs are retained below.

## Artifacts

- Before benchmark: `before/benchmark.json`
- Before 10 Hz metrics and 500 Hz Samply profile: `before-profiled/`
- After benchmark: `after/benchmark.json`
- After 10 Hz metrics without native sampling: `after-profiled/`
- After 10 Hz metrics, symbolized Samply profile, and flamegraph: `after-profiled-symbols/`
