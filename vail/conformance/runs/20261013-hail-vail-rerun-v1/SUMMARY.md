# Stock Hail versus Vail rerun — 2026-10-13

The same 15-case harness was rerun sequentially using the existing environments, with three warmups and five measured iterations per case. All cases passed their correctness assertions on both engines. Timings below are median seconds.

| Workload | Hail now | Vail now | Prior Vail (Oct 13) | Faster now |
|---|---:|---:|---:|---|
| Range stream, 100M rows | 0.4541 | 0.0165 | 0.0165 | Vail 27.52× |
| Rekey/sort, 1M rows | 1.6167 | 0.0700 | 0.2291 | Vail 23.08× |
| Group ascending keys, 10M rows | 1.4007 | 0.2192 | 0.7958 | Vail 6.39× |
| Group descending keys, 10M rows | 1.0921 | 0.2314 | 0.7685 | Vail 4.72× |
| Annotate 100 fields, 1M rows | 0.3112 | 0.0380 | 0.0349 | Vail 8.20× |
| Numeric statistics, 10M rows | 0.2475 | 0.7766 | 0.7253 | Hail 3.14× |
| Inner join, 1M × 1M rows | 0.3356 | 1.3341 | 1.6408 | Hail 3.98× |
| Lookup, 1M probes / 1K targets | 2.6983 | 0.2560 | 0.3506 | Vail 10.54× |
| Prefix sum scan, 1M rows | 0.2179 | 1.5198 | 0.9243 | Hail 6.98× |
| Native write, 1M rows / 2 int32 fields | 0.7984 | 0.1969 | 0.2003 | Vail 4.06× |
| Native read, 1M rows / 2 int32 fields | 0.1579 | 0.0106 | 0.0094 | Vail 14.89× |
| Matrix row stream, 100K × 10 | 0.0706 | 0.0071 | 0.0064 | Vail 10.01× |
| Matrix entry sum, 100K × 10 | 0.1108 | 0.4317 | 2.0268 | Hail 3.90× |
| Matrix row/column filters, 100K × 10 | 0.1582 | 0.1799 | 0.4678 | Hail 1.14× |
| VCF import + DP sum, 50K × 10 samples | 0.2449 | 0.1713 | 0.8993 | Vail 1.43× |

## Interpretation

Vail was faster in 10 of 15 cases; Hail was faster in the other 5. Relative to the October 13 columnar run, Vail improved for rekey/sort (0.229s → 0.070s), both grouping workloads (0.796s → 0.219s; 0.768s → 0.231s), joins, lookups, matrix aggregation/filtering, and VCF import. These comparisons use a different installed Vail wheel and should not be attributed solely to source changes.

Hail medians rose versus the October 13 Hail run for 12 of 15 cases; native reads, matrix row streaming, and matrix filtering were slightly faster. This broad shift, plus the inability to inspect running processes on this host, means this run may have had different background load. The run was sequential and no competing benchmark was intentionally started, but it is not a clean isolated-host measurement. Treat Hail deltas and close comparisons as noisy.

## Provenance and resources

- Vail wheel: `/private/tmp/vail-codec-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl` (SHA-256 recorded in `provenance.json`). Package metadata is `0.1.0a1`; its native binary SHA-256 `b76d138b85952ccf2b67945a5c15e29d9cd676e26128066a7706282f2efaa32f` matches the previously recorded installed wheel for runtime commit `464265f3ac864729a4f2115e10492e3044f5d400`. The Vail package does not expose its source commit at runtime. Current worktree HEAD is `2809acd3440866071a68ff57673e70294ff5e0f9`, so this run represents the installed `464265f` wheel, not the current checkout.
- Hail: PyPI `0.2.139`, runtime `0.2.139-5e5eedb87ff8`, Spark 3.5.9; no Hail build performed. Python 3.11.13 / arm64; Java Temurin 11.0.25. Hail configured `local[14]`, 14-way parallelism, 48 GiB JVM heap (verified as 51,539,607,552 bytes).
- Machine configuration from the prior environment inventory: Mac15,10, 14 cores, 96 GiB RAM. Table and matrix sources request 14 partitions; VCF import uses reader defaults. Vail uses its existing execution parallelism.
- The harness source hash is unchanged from the October 12/13 comparison (`benchmarks/compare_hail.py`, SHA-256 `f2b0205afe112558b98da45917889de458846fa7659eb466775b4ddc8687910f`). Workload execution samples summed to 28.4 seconds for Vail and 51.5 seconds for Hail, excluding warmups; Hail import and initialization added 1.9 and 4.5 seconds.
- This is an end-to-end local API comparison with fresh lazy-query construction and action included in each timed iteration. Hail `.ht` and Vail Vortex/Iceberg storage formats differ; reads are warm-cache. See the October 12 report for fuller workload scope and limits.
