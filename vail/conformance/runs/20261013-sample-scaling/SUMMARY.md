# Sample-count scaling: 20 and 40 samples

Baseline sample count is 10. Only the sample dimension changed: Matrix workloads retain 100,000 rows and VCF retains 50,000 variants. The four sample-dependent cases were rerun; Table-only cases have no sample dimension.

Each engine ran serially with 14 cores, three warmups and five measured iterations per case. Stock PyPI Hail used Spark local[14] with a 48 GiB JVM heap. Both used Python 3.11.13. All 16 benchmark case executions passed their correctness assertions. Medians below are seconds; speed ratio is Hail/Vail, so values above one favor Vail.

| Workload | Vail, 20 | Hail, 20 | Hail/Vail | Vail, 40 | Hail, 40 | Hail/Vail |
|---|---:|---:|---:|---:|---:|---:|
| Matrix row count | 0.0058 | 0.1116 | 19.34× | 0.0070 | 0.1018 | 14.54× |
| Matrix entry sum | 0.1653 | 0.1779 | 1.08× | 0.3372 | 0.1237 | 0.37× |
| Matrix row/column filtering | 0.1982 | 0.1199 | 0.60× | 1.1693 | 0.1365 | 0.12× |
| VCF import + entry sum | 0.0812 | 0.1630 | 2.01× | 0.1302 | 0.1980 | 1.52× |

The strongest scaling problem is Matrix filtering: Vail rises from 0.1982 to 1.1693 seconds as samples double from 20 to 40 (5.90×), versus Hail rising from 0.1199 to 0.1365 seconds. At 40 samples Vail filtering is 8.56× slower than Hail. Vail entry sum approximately doubles (2.04×); at 40 samples it is 2.73× slower than Hail. These runs measure the gap; they do not establish its cause.

The earlier 10-sample measurements came from the full 15-case suite; this run selected only the four sample-dependent cases, so its startup/JIT history differs. Use the fresh 20/40-sample comparisons rather than combining old and new medians as a controlled scaling series.

Native extension SHA-256: `0ea9e6446a0592417a865faf1876eb849dd65d2e92864c196a0d58c61ce9c44b`. Runtime code matches the reviewed final wheel; the benchmark harness adds `--vcf-samples` so VCF is scaled too. Per-iteration results and configuration are in the four engine/sample JSON files; harness hash and elapsed times are in `provenance.json`. Total serialized wall time: 62.39 seconds, including engine startup/shutdown and warmups.
