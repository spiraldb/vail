# Hail reader scheduling

Implemented on `dk/hail-layout-async-io-20261110`. Daniel explicitly accepted the measured costs and authorized merging the unchanged reviewed implementation on 2026-11-10. This is an architectural execution change with measured costs, **not an established throughput improvement**.

The final candidate is `55b26358b71ef252140e31b6e01f916bc7a73f3b`, installed native SHA-256 `f5fbcd3f360979ee824b70699fff10ab609c0016dca32dadcbbc3329474be79e`. Baseline production is `3fc119c73955537e5227b27875fcf125b3493484`, native SHA-256 `585cc05089ed159e38f700e0494a9c6ada12f86de48fbf0f664671356e7a882b`. Both use mimalloc and identical Python frontend files.

## Execution change

Indexed Hail table and matrix layouts acquire complete compressed ranges through Vortex's asynchronous `VortexReadAt` interface. Prepared decoding runs through Vortex `spawn_cpu`, which uses the application's ordinary Tokio workers, not its blocking pool. The per-file cursor is acquired asynchronously before work is dispatched. Cached index lookups perform no I/O on scan workers; misses and file reads retain blocking I/O dispatch.

The same prepared row decoder, codec, column builders, current decoded frame and frame position survive contiguous batches. Subsequent batches acquire only additional required compressed frames. Backward seeks reset to indexed frame/decoded offsets. Requests ending at the partition count include the actual file tail, preserving terminator and extra-row checks. Cache publication follows successful validation. Started CPU jobs cannot be preempted; they retain the owned cursor until returning.

New compressed spans are limited to 8 MiB each. This is **not** a total reader/process memory bound: old/new spans, cached outputs, index nodes and decompression buffers can coexist. Oversized spans, files without indexes and unsupported buffering dialects use the prior incremental streaming decoder, with cursor waits moved outside the blocking pool. No wire format changes, semantic rewrites, object-storage enablement or allocator changes are included. Both public native and Hail-format dataset entrypoints remain local-only.

## Final matched local results

Each row is a separate pair of fresh baseline processes and fresh candidate processes, ordered B,C,C,B. Every process executes one full warmup and three measured calls. Reported times are medians of process medians. These two-process cohorts are limited descriptive evidence. Every measured case below has worse wall time; no noise waiver or universal resource win is claimed.

| Benchmark | Wall seconds: baseline → candidate | Wall change | Core-seconds: baseline → candidate | CPU change | Maximum threads observed **after queries** |
|---|---:|---:|---:|---:|---:|
| Group-by collect | 0.359118 → 0.365221 | +1.70% | 3.939953 → 3.822482 | -2.98% | 280 → 151 |
| Coverage statistics | 1.029456 → 1.050568 | +2.05% | 12.264382 → 12.472950 | +1.70% | 136 → 159 |
| Variant QC | 1.101427 → 1.121230 | +1.80% | 11.038408 → 10.927227 | -1.01% | 66 → 96 |

The benchmark bodies are the original upstream functions and actions, reading exactly the same Hail-format files. Gnomad input is 250,000 × 1,000 in 32 partitions; the retained profile input is 24,885 × 2,535 in 16 partitions. All 380 input-file hashes and file sets matched before and after each cohort. This is Vail before/after, **not a new Hail-versus-Vail comparison**. CPU is process CPU; no descendants were observed at action endpoints. Rayon4/BLAS1 and existing Tokio32 are held fixed. Each worker has a 180-second deadline, sampled combined RSS guard of 12 GiB and verified process-group cleanup; the phase is capped at 1,200 seconds. No builds, tests or profiles overlapped timings.

The benchmark functions return `None` after their original forced-count actions. Benchmark success establishes execution, not a new full-output cross-engine oracle. Public correctness gates supply independent values/nulls/order checks.

## Earlier candidates retained

The initial split (`4aefe83fe`, native3762) measured group collect +5.35% wall / -3.67% CPU, coverage +0.86% / +1.97%, and variant QC +2.15% / +0.40%. Cached index dispatch (`23997b13e`, native3c8e) measured +8.76% / -4.84%, +1.12% / +2.52%, and +2.72% / +1.11%, respectively. All raw results, worker receipts and adverse measurements are retained in `initial/`, `cached-index/`, and `cpu-task/`. Each cohort has its own contemporaneous baseline; these are not matched comparisons between experimental variants.

## Validation and profile

Final source passed 42 Rust tests (two existing corpus tests ignored) and 192 public Python tests covering real Hail versions/codecs, both index layouts, cross-block rows, sparse/null matrices, seeks, large-row streaming fallback, malformed tails, joins and VDS reads. The three added public Python tests also passed on the unchanged baseline. Normal kache/maturin builds and isolated installed wheels were used; compiled shared libraries were not manually copied. Build/source linkage is recorded by frozen revision, normal build/ready history and artifact hashes; there was no separate pre-build source-content enrollment.

The final eight-second samply capture performs one warmup followed by six seconds of repeated group collect, outside all timings. It recorded 151 threads: 32 ordinary async workers, 118 blocking-pool threads and the main thread. Bulk `PartReader`/`BatchColumn` decoding appears only on the ordinary workers, not on blocking-pool threads. Remaining blocking work includes raw I/O and small index decoding. The earlier original profile recorded 304 threads. These profile thread counts are separate diagnostics; benchmark endpoint thread counts in the table are not continuous peaks. Other benchmarks' endpoint thread maxima increased, so there is no blanket thread-reduction claim.

The normal samply/Firefox profile and symbol sidecar are kept under:
`/Users/danielking/.codex/visualizations/2026/10/23/01a1666a-5c91-7043-b5a8-da5a82f930bd/hail-layout-async-io-20261110/revision3/`.
The prior inline-worker profile is preserved in its parent directory. Warmed query boundaries are recorded in each profile worker receipt. CPU interval attribution to sampled endpoint functions is approximate and inclusive stacks overlap; no function-percent causality or hardware saturation is claimed.

Independent reviewer chat: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`. Source, architecture and correctness gates cleared. The reviewer initially held production admission because all three wall times regressed; Daniel then explicitly accepted those measured tradeoffs and authorized the merge. No workload was rerun merely to obtain approval. See `review-decision.json`.
