# Parallel execution and complete benchmark results

[Standalone HTML report](report.html) · [Performance results and 10 Hz charts](curated/SUMMARY.md) · [All upstream outcomes and failure groups](UPSTREAM_RESULTS.md) · [Correctness validation](VALIDATION.md)

Measured query revision: `a3e497c7732f13f630e7cc9e3937c89b63fe3a31`.

The fresh 15-workload baseline sums to **0.858492 seconds for Vail versus 6.560877 seconds for Hail (7.64×)**. Across all 35 workload configurations, totals are **2.373127 versus 10.156054 seconds (4.28×)**. Totals sum per-case warm medians and exclude startup/warmups. All 70 engine captures succeeded.

| Affected workload | Vail median | Hail median | Vail mean CPU cores |
|---|---:|---:|---:|
| Range force-count, 100M rows | 8.12 ms | 176.93 ms | 13.93 |
| Numeric statistics, 10M rows | 23.40 ms | 406.28 ms | 20.22 |
| Table write, 1M rows | 51.83 ms | 353.88 ms | 9.45 |

Rekey remains deliberately outside this fix and uses about one core. Small MatrixTable count/filter workloads use fewer cores despite retaining concurrent execution partitions; their latency and CPU measurements are included. Prefix scan is the only curated case where Hail is faster (187.98 ms versus Vail 196.66 ms).

Both original upstream benchmark modules were additionally attempted at their original sizes: **86 functions / 108 expanded cases per engine**. These are single-invocation coverage attempts, separate from the repeated warm performance captures.

| Engine | Passed | Failed | Unsupported | Timed out | Source-marked skips |
|---|---:|---:|---:|---:|---:|
| Vail | 54 | 31 | 12 | 7 | 4 |
| Hail | 103 | 0 | 0 | 1 | 4 |

The largest Vail blockers are compressed text/type imputation (13 cases), performance timeouts (7), out-of-scope randomness (7), native panics/worker aborts (6), BGEN import (4), and out-of-scope ndarray zeros (4). Setup blockers are distinguished from failures in benchmark bodies. Every non-passing case is listed in UPSTREAM_RESULTS.md, including native failure diagnostics and timeout limits.

The benchmark used one AWS m7i.8xlarge VM: 32 vCPUs (16 physical cores with SMT), 128 GiB RAM. Hail was installed from PyPI, not built, with Spark local[32] and a 64 GiB heap. Resource cleanup is recorded in CLEANUP.json.
