# Six remaining wall-time suspects: matched laptop rerun, 2026-11-11

Only **group-by take/rekey** remains slower in wall time in this run. Many row-wise aggregations and native write still use more core-seconds than Hail. Every slower metric and process observation is retained.

| Benchmark | Hail wall s | Vail wall s | Vail wall result | Hail core-s | Vail core-s | Vail CPU change |
|---|---:|---:|---|---:|---:|---:|
| Many row-wise aggregations | 1.68451 | 0.87759 | 1.92× faster | 7.33010 | 8.59354 | +17.24% |
| VCF export | 8.95743 | 3.33162 | 2.69× faster | 36.69729 | 27.62570 | -24.72% |
| Group-by take/rekey | 1.00012 | 2.41742 | 2.42× slower | 4.83869 | 2.94077 | -39.22% |
| Sex-specific QC | 1.25253 | 0.44992 | 2.78× faster | 5.39210 | 4.33592 | -19.59% |
| Localize and collect | 3.23946 | 2.68912 | 1.20× faster | 4.19012 | 2.69046 | -35.79% |
| Native matrix write | 2.07000 | 1.18906 | 1.74× faster | 8.09390 | 13.35280 | +64.97% |

These are current matched local measurements, not isolated before/after gains versus the November 6 cloud run. Hardware, some input layouts and the freshly seeded gnomad fixture differ from that older run; dividing the old and new ratios would not establish a code-change speedup. Gzip was excluded as requested. No production change, compilation, broad correctness suite, profile or cloud workload was part of this benchmark phase.

## Exact workload and input scope

The original upstream benchmark AST functions are unchanged except removal of pytest decorators. The original `many_aggs` helper is included for row-wise aggregation. Each original read, query construction, terminal action or write is timed. Engine/JVM import/initialization is outside timing. Kyle sex-specific QC retains the original unseeded `rand_bool` expression under each engine's normal semantics; no identical sex-assignment or output claim is made.

Five cases read exactly the same Hail-format files in both engines, with Vail's public `MatrixTable.read_legacy` adapter. Many aggregations, export, sex-specific QC and localize/collect use the full retained 24,885-row × 2,535-sample profile input with 16 Hail partitions. Take/rekey uses the existing 250,000 × 1,000 gnomad-DP input, 32 partitions, explicit generation seed 20261110. This is not the small 1,000-row laptop subset.

The sixth case intentionally compares native formats: Vail reads/writes its Vortex-backed format and Hail reads/writes its own format. A full native copy was produced outside timing by public Vail read_legacy→write from the same profile input. It has **67 reported native partitions versus Hail's 16**, not physical partition/layout parity. Native output is 784,324,297 bytes per timed call versus approximately 206,126,600 Hail bytes: about **3.81× larger**. The compression representations and output sizes differ materially, so this is a user-visible format/read-and-write comparison, not equal-byte compression throughput. Vail's native-write worker median walls were 1.18906, 1.73284 and 1.15627 seconds; the slower worker is included without trimming or a noise waiver.

Before timing, separate Vail and Hail workers checked full dimensions and each fixture's actual partition count and exact canonical 3×3 input-prefix values. Vail's native copy has the same dimensions and small logical prefix as the Hail source, with its own format-specific partition count. This is relevant input evidence, not full-data or output equivalence. Original force-count/collect actions discard their results as upstream does. Write workers check nonempty output files after the timer, not full output semantic equivalence.

## Measurement and safeguards

Three fresh processes per engine/case, fixed H,V,V,H,H,V schedule. Every worker executes one full warmup then three measured calls: 36 workers and 108 timed calls. Values are the median of the three process medians; raw samples and process medians are retained. No adaptive warmup, favorable-worker selection, sample deletion or timing retry. Timed phase: 651.85 seconds.

Nice 10, Rayon 4 and BLAS/OpenMP 1; Hail Spark local[4], while Vail retains its ordinary Tokio 32 runtime. This is not total-thread/core parity, so wall speedups must be read alongside core-seconds. CPU is worker process time plus stable observed live-child endpoint deltas, including Hail's JVM. Unobserved short-lived descendants can be missed, and CPU endpoints are not atomic with wall timestamps. These are descriptive observed ratios, not mathematical bounds or statistical-significance guarantees.

For export and matrix write, a fresh output parent is created before each warmup/sample and deleted after its counters finish. The original read/export/write remains inside the timer, including its normal durability behavior. Output inspection/deletion is outside timing. Warm filesystem caches and a shared local filesystem are used; this is not physical-disk saturation throughput. Worker deadline 360 seconds; phase deadline 3,600 seconds with 380 seconds reserved before each launch. A sampled combined 12 GiB RSS guard and 5 GiB free-disk floor apply; the RSS guard is not an OS hard memory limit. No task-owned builds, tests, profilers or other benchmarks overlap timing.

All 36 timing workers exited 0 and their owned groups were positively absent before each next worker. Post-timing complete file-set/size/SHA-256 checks pass for **456 input files**, native extension and Hail JAR. No code or environment changed during timing.

Vail measured production `79744c0316884b341ed04cc2f02d77fb6bd3d7ba`, local main `8bc7ced6205d5951a4c040255990cffba4e8d704` (later changes are evidence only), normal installed native SHA `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193`, mimalloc. All 69 installed Python frontend files are pinned and checked per worker. Hail `0.2.139-5e5eedb87ff8`, JAR SHA `34b643346b31f1644be8155a6c91ce25ec62a98be3174b57d2fe78d857663152`, PySpark 3.5.9, Java 11, Python 3.12.12.

## Retained setup failures and evidence

The initial native-copy setup correctly wrote and counted the full dataset but incorrectly asserted that writing would preserve Hail's physical partition count (16); native reports 67. The completed copy was retained and re-enrolled with its actual count, without changing format/layout or rerunning the write. Both preflight workers then passed their own input checks, but a host comparison incorrectly compared the entire native/Hail dictionaries including those intentionally different partition counts. That host postcondition was corrected to compare dimensions and logical prefix only. The unchanged successful worker files were revalidated under the corrected postcondition; they were not rerun or relabeled as executing under a changed worker. Both failed postconditions, original scripts/protocol and positive cleanup receipts remain in this packet. Neither failure produced a timing sample.

`workers.json` retains all 36 timing workers and two input-validation workers, original worker JSON, cleanup receipts and source file hashes. `recomputed.json` independently reproduces every summary number from these raw samples. `pins.json` contains complete input/front-end/native/JAR/source identities. `protocol.json`, `worker.py`, `run.py`, and `owned.py` freeze the measured execution. Full stdout/stderr, Hail logs, input data and temporary process receipts remain under `/private/tmp/vail-six-slower-20261111`. No compiled libraries or dataset payloads are committed. `manifest.json` seals this report packet.

Independent reviewer chat: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`. Required final independent evidence review precedes report-only main integration. No GitHub PR, remote push or VM operation is part of this work. The user's subsequently requested take/rekey and row-wise investigations run separately after this complete benchmark phase; their profiles are not timing observations in this table.
