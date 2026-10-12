# Stock Hail versus merged Vail — 2026-10-12

Merged `7e642a8e7cc9ddc88f635342b00a0fda578c7572` into the Table fixes at merge commit **`158c610fc75adbd80d8b767da62020fc762c9edc`**. Vail was built in Cargo's optimized release profile and installed as a wheel in a separate environment. The installed merged wheel passed **869 Python tests, 1 skipped, 19 subtests passed**.

Stock Hail was installed from PyPI with `uv pip install --upgrade hail`; no Hail source build was performed. PyPI's JSON API confirmed **0.2.139** was latest on the run date. Runtime version: **`0.2.139-5e5eedb87ff8`**, Spark **3.5.9**, arm64 Temurin **Java 11.0.25**. Both engines used Python **3.11.13** on the same **Mac15,10**, **96 GiB RAM**, **14 physical/logical CPU cores**.

Hail used `local[14]` and `spark.driver.memory=48g`; its actual JVM maximum heap was verified as **51,539,607,552 bytes (48 GiB)** and Spark reported **14 workers**. Table and synthetic matrix sources requested 14 partitions in both engines. VCF import used each reader's default partitioning, without a partition-count override; both still had the same host resources available. Vail's build hash and dependency versions are recorded in `vail-build.json`; Hail's resolved packages and hardware are in `environment.json`.

## Primary results

**6 cases favored Vail; 9 favored Hail. All 15 cases passed on both engines.** Times below are median seconds from **five measured iterations after three warmups**, constructing a fresh lazy query and executing an equivalent action each time. Import and engine initialization are excluded from the query timings and recorded separately in the raw results. The runner checks expected row counts or scalar aggregate results; write cases check successful completion, and the subsequent read checks row count.

| Workload | Hail seconds | Vail seconds | Faster |
|---|---:|---:|---|
| Range stream, 100M rows | 0.3053 | 0.0313 | Vail 9.77× |
| Rekey/sort, 1M rows | 0.9355 | 0.3605 | Vail 2.60× |
| Group ascending keys, 10M rows | 0.6286 | 1.3942 | Hail 2.22× |
| Group descending keys, 10M rows | 0.5447 | 1.8343 | Hail 3.37× |
| Annotate 100 fields, 1M rows | 0.2178 | 4.7039 | Hail 21.60× |
| Numeric statistics, 10M rows | 0.2055 | 0.7228 | Hail 3.52× |
| Inner join, 1M × 1M rows | 0.2226 | 1.6719 | Hail 7.51× |
| Lookup, 1M probes / 1K targets | 1.6240 | 0.5371 | Vail 3.02× |
| Prefix sum scan, 1M rows | 0.1175 | 0.9532 | Hail 8.11× |
| Native write, 1M rows / 2 int32 fields | 0.3917 | 0.2368 | Vail 1.65× |
| Native read, 1M rows / 2 int32 fields | 0.1128 | 0.0106 | Vail 10.64× |
| Matrix row stream, 100K × 10 | 0.0593 | 0.0070 | Vail 8.52× |
| Matrix entry sum, 100K × 10 | 0.0944 | 2.1248 | Hail 22.51× |
| Matrix row/column filters, 100K × 10 | 0.0998 | 0.4852 | Hail 4.86× |
| VCF import + DP sum, 50K variants × 10 samples | 0.1220 | 0.9219 | Hail 7.55× |

The largest Vail deficits here are wide annotation and matrix entry aggregation (about 22×), followed by scans, ordinary joins, and VCF import/aggregation. Vail is faster on range streaming, rekeying, lookups, and native-format storage in this run. These measurements identify performance work; they do not establish an overall speedup across workloads.

## Method and limits

- Only `hail-quiet.json` and `vail-quiet.json` are the primary comparison. Initial one-warmup/three-repeat runs and a follow-up were contaminated by a concurrent Rust build and are excluded. A first attempt to pause its clients missed compiler descendants, so that partial rerun was stopped and excluded too.
- During the primary run the competing build and its compiler descendants were temporarily stopped, with a 300-second independent resume watchdog and `finally` restoration. All 14 affected processes were verified resumed or exited afterward (`quiet-run-control.json`). The engines ran sequentially, Vail then Hail, to avoid sharing CPU/RAM. CPU snapshots confirm no active compiler contention during the quiet measurement. Normal desktop background activity remained, so this was not an isolated server benchmark. Load averages include earlier activity and are not an instantaneous utilization measure.
- Both engines received generous RAM and CPU access. This suite did not encounter an out-of-memory error. Vail uses its existing execution parallelism; this comparison does not artificially constrain Hail to Vail's current utilization.
- Synthetic Table cases follow patterns in `hail/python/benchmark/hail/benchmark_table.py` at checkout `e10cc355bc8cd0de9c5f29d5182d31527898bae1`. The checked-out source supplies benchmark patterns only; the engine is the newer PyPI build. Joins are scaled to 1M rows rather than the repository's billion-row parameterizations; scans use 14 source partitions rather than 1,000. Other scales and actions are explicit in `benchmarks/compare_hail.py` and the raw result parameters.
- Synthetic matrix and VCF cases exercise the merged features; they are not the upstream dataset-based MatrixTable benchmark fixtures. The VCF is generated outside timing and is identical for both engines. Each import/aggregation is included in timing. The suite does not cover missing-value/NaN edge cases, realistic genomic complexity, or distributed clusters.
- Native storage compares Hail `.ht` against Vail's Vortex/Iceberg format, not identical encodings. Repeated reads are warm-cache measurements. Writes overwrite their engine's snapshot each iteration.
- Hail/Vail startup is excluded, while per-query Python construction, planning, execution, and materialization required by the action are included. This is an end-to-end local API benchmark, not a kernel-only throughput measurement. Hail's JVM/compiled execution and Vail's Arrow/DataFusion execution can do different physical work for the same API action.
- Per-case resource labels in the primary JSON were corrected after measurement to distinguish configured cores from requested source partitions; no timing samples were changed.

## Reproduction

Use the identical source revision and optimized wheel. Run each engine sequentially with the competing compiler work quiescent:

```sh
uv venv --python 3.11 /private/tmp/vail-stock-hail-benchmark
uv pip install --python /private/tmp/vail-stock-hail-benchmark/bin/python --upgrade hail
CARGO_INCREMENTAL=0 RUSTC_WRAPPER='' .venv/bin/maturin build --release --offline -j 4 -o /private/tmp/vail-benchmark-wheels
uv venv --python 3.11 /private/tmp/vail-release-benchmark
uv pip install --python /private/tmp/vail-release-benchmark/bin/python /private/tmp/vail-benchmark-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jre/Contents/Home SPARK_LOCAL_IP=127.0.0.1 /private/tmp/vail-stock-hail-benchmark/bin/python benchmarks/compare_hail.py --engine hail --warmups 3 --iterations 5 --workdir /private/tmp/hail-bench --output /private/tmp/hail-results.json
/private/tmp/vail-release-benchmark/bin/python benchmarks/compare_hail.py --engine vail --warmups 3 --iterations 5 --workdir /private/tmp/vail-bench --output /private/tmp/vail-results.json
```

The automatic background-build control was specific to this machine and is not part of the portable benchmark runner. Raw samples, all Spark settings, result checks, build log, validation output, and source hashes are retained alongside this report.
