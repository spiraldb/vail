# Three full-size Vail-on-Hail / Hail reruns, 2026-11-10

All three selected cases finish faster in Vail in this local run. Grouped collect and variant QC still use materially more core-seconds than Hail. These are current matched laptop observations, not isolated code-before/after gains relative to the historical cloud run.

| Benchmark | Hail wall s | Vail wall s | Hail/Vail wall | Hail core-s | Vail core-s | Vail CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Group-by collect per row | 0.484848 | 0.348395 | 1.392× | 2.564090 | 3.870461 | +50.95% |
| Optimized coverage statistics | 3.025222 | 1.049206 | 2.883× | 13.083418 | 11.853336 | -9.40% |
| Variant QC | 1.345752 | 1.009484 | 1.333× | 5.886659 | 9.877827 | +67.80% |

Grouped collect is 28.14% lower wall but 50.95% higher CPU; optimized coverage is 65.32% lower wall and 9.40% lower CPU; variant QC is 24.99% lower wall but 67.80% higher CPU. Faster completion must not be confused with lower total CPU work. Hail Spark is configured local[4], while Vail uses Rayon 4 and its existing Tokio 32 runtime, so this is not total-thread/core parity.

## Inputs and exact workload scope

Both engines read the exact same Hail-format files. Vail uses the public MatrixTable.read_legacy reader, supplied once through a copied API namespace; this is not Vail reading a preconverted native copy. The three original upstream AST bodies are unchanged apart from removing pytest decorators. Read/query construction/planning/action is timed; engine/JVM initialization is excluded.

Grouped collect and optimized coverage use the original 250,000-row × 1,000-sample simulated input with 32 partitions and the original x=int(rand_unif(0,4.5)**3) generation body. A fresh Hail-format dataset was generated with explicit seed 20261110 and shared by both engines. It has 238 files / 178,552,904 bytes. It is not the byte-identical historical cloud random fixture. No small profile substitution was used for either case.

Variant QC uses the retained full profile input: 24,885 variant rows × 2,535 samples, 16 partitions, 142 files / 206,126,614 bytes. The two fixtures' 380 files were fully hashed before and after timings. The profile file is local retained input, not an assertion of historical cloud-file byte identity.

Before timing, two separate workers confirmed public full dimensions and partition counts, and exact canonical 3×3 input-prefix values. This is input/reader evidence, not full output semantic conformance. Timed supported upstream functions return None after their original forced-count actions; no new full-output oracle or independent count-result capture is claimed.

## Measurement

Three fresh processes per engine/case in balanced H,V,V,H,H,V order: 18 timing workers and 54 measured calls. Each worker executes one full warmup then three measured calls; each reported number is the median of the three worker medians. All samples, including within-process variation after the single warmup, are retained. These are descriptive finite-call results, not steady-state or statistical-significance guarantees. There was no adaptive warmup, sample removal, retry or tuning during timing.

Workers run nice 10, Rayon 4, BLAS/OMP 1. Spark driver/executor memory is 2 GiB, worker deadline 360s, phase budget 3600s, sampled combined RSS guard 12 GiB. That guard is monitoring, not a hard OS memory limit. No task-owned build, correctness suite, profiler or other benchmark overlapped comparative timing. The common ownership/resource monitor runs for both engines.

CPU is worker Python process time plus stable observed live-child endpoint CPU, including Hail's JVM. Unobserved short-lived descendants can be missed and child snapshots are not atomic with wall timestamps. These are observed core-seconds with a lower-bound limitation; their ratios are point estimates, not guaranteed bounds. Whole-worker peak RSS, if consulted, includes imports/warmup and is not isolated operator allocation.

All 18 timing workers exited 0 and their owned groups were positively absent before the next worker. Full post-input hashes and native/JAR pins pass. Runtime: normal installed Vail native 585cc05089ed159e38f700e0494a9c6ada12f86de48fbf0f664671356e7a882b, production 3fc119c73955537e5227b27875fcf125b3493484, mimalloc; Hail 0.2.139-5e5eedb87ff8, JAR 34b643346b31f1644be8155a6c91ce25ec62a98be3174b57d2fe78d857663152, PySpark 3.5.9, Java 11 and Python 3.12.12. Native/JAR identities and source/protocol are pinned; installed frontend history is recorded without claiming a complete independent frontend-content enrollment. No new Vail compilation or production change was needed.

## Historical comparison

The last full cloud run had Vail/Hail wall ratios 3.929×, 3.313× and 3.272× for grouped collect, optimized coverage and variant QC. This laptop run has ratios 0.719×, 0.347× and 0.750×. The old >3× wall deficits do not reproduce under this local protocol. Hardware, warmup methodology, scheduling and fresh random fixture differ from the cloud run, so dividing those ratios would not establish a code-change speedup. Current grouped-collect and variant-QC CPU deficits remain concrete follow-up targets.

Raw worker JSON/scripts/protocol/pins are retained here. Full logs, generated Hail data and raw receipts are at /private/tmp/vail-hail-slow-three-20261110. No cloud work, Vail code change, remote push or GitHub PR. Required independent reviewer: 01a14128-21b8-7a53-b399-4ad8f9b31f4f.
