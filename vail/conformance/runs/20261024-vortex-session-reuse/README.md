# Runtime-safe default Vortex session reuse

Candidate source75132a30 is based on main2c28d6b1. Normal builds use kache,
RUSTFLAGS=-C target-cpu=native, release, locked, jobs2, no incremental override.
No compiled artifacts were copied. Baseline was rebuilt normally at exact
main2c and reproduced native SHA0f86a632262bb073797934cddac75660e3c41b8367c7936ba3d59ee8e8f9b6d9.

This changes default session preparation only for data-file reads. A bounded
one-entry cache belongs to SnapshotReadPlan, and is passed through the ordinary
ArrowReader builder into its file-task reader. It stores one runtime ID and
session, never a global. Acquisition happens inside the polled file read, once
per file; there is no per-batch or per-entry lock. Cold default construction is
inside the mutex to avoid duplicate registration by simultaneous file tasks.
No lock spans file IO or an await. Runtime replacement evicts only the cached
clone; existing scans own clones until they complete. Delete readers remain
unchanged. The cache cannot accept configured/raw sessions.

Runtime safety relies on the pinned default configuration, not on IDs proving
runtime lifetime. vortex0.86.1 default RuntimeSession calls Handle::find, which
selects TokioRuntime::current: a weak handle to a STATIC CurrentTokioRuntime
whose each operation resolves Tokio Handle::current. Thus ID reuse cannot
resurrect a stale captured handle. Non-Tokio default behavior remains uncached.
The public runtime tests keep Iceberg Table's documented borrowed IO runtime
alive, while destroying/recreating actual scan-polling runtimes, and concurrently
polling clones of one source on independent runtimes. They do not promise that
an Iceberg Table survives destruction of its separately owned IO runtime.
The initial tests violated that existing IO ownership contract; their failures
are retained, and the corrected tests exercise the intended public boundary.

Shared services audit, exact pinned0.86.1:
- vortex-array dtype/session.rs and session/mod.rs: dtype and array plugin registries.
- optimizer/kernels.rs: kernel registry.
- aggregate_fn/session.rs: aggregate/grouped kernel registries.
- scalar_fn/session.rs: scalar plugin registry.
- stats/session.rs: rewrite rules, no query row statistics cache.
- patched vortex-layout/src/session.rs: layout registry.
- vortex-arrow/src/session.rs: Arrow importer/exporter registries.
- vortex-array/src/memory.rs: static allocator handle.
- vortex-io/src/session.rs and runtime/{handle,tokio}.rs: dispatcher contract above.
- vortex-file/src/multi/session.rs: existing bounded100MB footer cache is registered
  by default but dormant on this path. Its only get/put callers are multi/mod.rs
  267/273; Iceberg open_vortex_file uses single-file open_options().open, which
  does not call that helper. This work introduces no footer/data cache policy.
Per-file options, sources, metrics, decoders, selection, Arrow fields, execution
contexts and expression bindings remain per-read. create_execution_ctx creates
a new ExecutionCtx rather than sharing its mutable evaluation state.

Timings are isolated from builds, tests and sampling. Cold-source means a newly
opened snapshot/cache with already initialized engine and warm OS file cache,
not cold storage. Every result is checked against an independent integer sum.
Small fixtures have4rows/file and32 list entries/row; files1 and128. The realistic
DP fixture has200,000rows×2,500cols (500M entries),32 source partitions, DPint32,
nulls, expected20969995813. One untimed DP warmup precedes12 timed actions.
No arithmetic/SIMD/bandwidth saturation claim is inferred from sampling.

The100-action receipts sample process ru_maxrss (Darwin bytes), not instantaneous
RSS or live allocation totals. Their plateau bounds retained-memory growth at
this workload scale; they do not prove absence of every allocation or small leak.

Public pytest union:388passed0failed on first candidate native46cc8db4.
Corrected public LocalRead runtime gate:2passed0failed on75132a30.
Final exact-normal artifact and paired timings are recorded in summary.json.

## Matched results

| Query | Baseline wall | Candidate wall | Speedup | CPU reduction |
| --- | ---: | ---: | ---: | ---: |
| 500M DP SUM |62.308ms|52.996ms|1.176×|17.9%|
|128files fresh source|33.579ms|8.327ms|4.03×|87.2%|
|128files same source|32.677ms|7.611ms|4.29×|87.1%|
|1file fresh source|1.807ms|1.771ms|1.02×|2.3%|
|1file same source|1.532ms|1.384ms|1.11×|9.6%|

Each median has12 independent actions per phase. Fresh-source actions include
cold session initialization under the shared lock; source-opening metadata time
is separately recorded. No regression from cold initialization contention is
observed in these128-file scans; the public concurrent-runtime tests check
semantics rather than claiming a throughput result for runtime churn.

Final normal rebuild of75132a30 reproduces native46cc8db402144b38124644de097a87de229a5917b7bf1247a912f555d5f87713.
Final exact-native public Python gate:388passed0failed (10.91seconds).
Public runtime gate:2passed0failed (0.10seconds).
Repeated-action high-water RSS plateaus: baseline357.8→359.4MB,
candidate356.7→358.1MB over100 actions (sampled every10actions).

The matched2-second sampling windows capture repeated data-reader session
construction and registry initialization before the change. Afterward no
read_vortex_task→VortexSession::default stack remains. The remaining default
constructor stacks are query-time computed_source::try_total preparation.
Raw stack line counts (831→13 default constructor lines;640→15 encoding
registration lines) are evidence of call-path removal, NOT CPU percentages or
sample-normalized speedups. The measured wall/core-second reductions above are
the performance evidence. No new entry traversal or SIMD kernel was added;
this removes repeated registry work across all ordinary native data-file scans.

Source-only follow-up review disposition for75132a30 was CLEAR. Final resource
and runtime receipts await required review before main integration. The source
and profile paths are intentionally preserved for independent review.
