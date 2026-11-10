# Scoped Hail/Vail prefix comparison, 2026-11-10

Current Vail is faster in all five selected bounded-read calls on this warmed laptop fixture. These are current-engine comparisons, not the incremental speedup from the limit patch and not a full-suite or streaming-throughput result. The preceding Vail-before/after report separately measures that patch's incremental changes.

| Benchmark | Hail wall ms | Vail wall ms | Hail/Vail wall | Hail core-seconds | Vail core-seconds | Hail/Vail CPU |
|---|---:|---:|---:|---:|---:|---:|
| Matrix show (100 rows) | 264.508 | 159.652 | 1.66× | 1.209446 | 0.160098 | 7.55× |
| Rows show (100 rows) | 180.466 | 11.460 | 15.75× | 0.810678 | 0.011587 | 69.97× |
| Entries show (default 10) | 448.468 | 14.697 | 30.52× | 2.076070 | 0.024446 | 84.92× |
| GT.take(100) | 235.516 | 7.105 | 33.15× | 1.173387 | 0.008224 | 142.67× |
| info.AF.take(100) | 208.376 | 7.455 | 27.95× | 1.036100 | 0.007826 | 132.39× |

Values are the median of four worker medians, each with three warmups then five measured calls. Balanced H,V,V,H,H,V,V,H schedule per case: 40 fresh timing workers and 200 measured calls. Engine initialization is outside timing; each unchanged upstream call includes read/IR construction/planning/execution/rendering or take. Python stdout is redirected to StringIO for both engines; terminal COLUMNS=80/LINES=24. Formatting is each engine's normal implementation and is not byte-identical across engines. Raw calls come from benchmark_matrix_table.py, extracted by AST with only pytest decorators removed.

The fixture has 1,000 variant rows × 2,535 samples and four partitions. Vail reads its existing native Vortex/Iceberg representation; Hail reads its own native representation. This small prefix-focused experiment includes job/planning overhead and must not be extrapolated to large full scans or the original complete profile25 benchmark.

The old /tmp Hail environment and native input had been cleaned out. Hail 0.2.139-5e5eedb87ff8 was reinstalled into a task-local environment, with the same 34b643346b31f1644be8155a6c91ce25ec62a98be3174b57d2fe78d857663152 JAR, PySpark 3.5.9 and Java 11. Its native input was regenerated from the first 1,000 rows of the retained VCF and repartitioned to four without shuffle. The retained full-VCF hash differs from the historical subset-VCF enrollment; historical Hail physical file/layout identity and historical ratios are not claimed. Existing Vail's 13 native files remained unchanged.

Before timing, exact canonical logical values matched for 101 complete row records, 11 complete flattened entries, 100 GTs and 100 AF arrays. After all timing, a separate public head(100,10).entries().take(1000) check proved exact equality of all 1,000 displayed matrix entries, including row/column fields, missingness, Call alleles/phasing and hexadecimal finite floating-point values. These are relevant public prefix checks, not full dataset/schema-order or renderer-string equality. No validation workload overlapped timing.

Hail uses Spark local[4], Vail Rayon 4 and the existing Tokio 32 configuration, with BLAS/OMP 1. This is not total-thread parity. CPU counts Python process time plus stable observed live child-process endpoint time, including Hail's JVM; unobserved short-lived descendants may be missed and child snapshots are not atomic with wall timestamps. These core-seconds are observations with that lower-bound limitation; their ratios are point estimates, not mathematical bounds. No compiler, test suite or profiler overlapped comparative timing. The controller's same process/RSS monitor runs for both engines.

Every timing worker exited 0 with positive owned-group cleanup. All 83 input file hashes, current Vail native and Hail JAR were verified before and after the timed phase. Worker identities, exact source bodies, output hashes and individual samples are retained. Show rendering is nonempty and stable within each process; take bodies discard their return just as upstream does. Separate public prefix checks supply value evidence.

Vail main d2a9a6a56351115fea7f9dc9088d75733917c9fc / production 3fc119c73955537e5227b27875fcf125b3493484, normal native 585cc05089ed159e38f700e0494a9c6ada12f86de48fbf0f664671356e7a882b, mimalloc, Python 3.12.12. Its previously built normal installed wheel is used directly; no manual .so artifact cache or new Vail code change. Frontend installation/build history is recorded; a complete independent pre-run frontend-content enrollment is not claimed.

Setup failures (absent historical files, then missing psutil in the fresh Hail environment) occurred before successful preflight and before any timing; their receipts remain in the raw task directory. No environment fix, retry or tuning was performed during timing. A separate displayed-values check ran only after timing. All five selected benchmarks and all their samples are retained; no slower sample was removed.

Raw source/scripts/identities and measured worker JSON are retained here; complete local logs, environment, generated fixture and process receipts are under /private/tmp/vail-hail-prefix-20261110. No cloud, production change, GitHub PR or remote push. Independent reviewer: 01a14128-21b8-7a53-b399-4ad8f9b31f4f.
