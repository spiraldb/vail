# Aggregation bottlenecks on the M3 Max

The measured warmed Vail runs are **not limited by observed disk demand or page-ins**. The CPU profiles instead identify Hail-format decoding, nested array indexing, integer modulo and remaining ordinary expression callbacks. The available counters do **not** establish DRAM-bandwidth or FLOP saturation. High CPU occupancy alone would not establish either.

Every diagnostic here executes **Vail**, including rows named “Hail input.” They compare input formats, not Vail IPC with the Hail/JVM engine. The pinned numeric checkpoint is `c0233972584ae3bf54b6f4b9ebb9068577b6e6d2`, normal native `1d287bbe5511cabd62b40638f743b29b2ce6992e9e38b69683b0aa4299a4a433`. The machine is an Apple M3 Max with **10 performance + 4 efficiency cores and 96 GiB RAM**; these cores are not equal throughput units.

## OS counters

Three fresh diagnostic workers each warm the original query once, then run three original actions. These are diagnostic observations, not another paired speedup experiment.

| Vail workload/input | Median wall s | Median CPU core-s | Average busy cores, range | Retired instructions/action | Instructions/cycle |
|---|---:|---:|---:|---:|---:|
| Many row aggregations / Hail | .731 | 7.004 | 9.51–9.71 | 95.8 billion | 4.11–4.12 |
| Many row aggregations / native | .426 | 4.970 | 11.53–11.87 | 53.1 billion | 3.25–3.28 |
| Sex-specific QC / Hail | .457 | 4.424 | 9.44–9.89 | 57.9 billion | 3.93–3.97 |

All nine warmed action windows record **zero OS-accounted disk-read/write bytes, zero page-ins and zero major faults**. This supports no observed physical-disk/page-in bottleneck in these windows, not zero file reads, memory traffic or cache misses. Minor faults are nonzero: roughly **2,200–19,600/action**. Kernel CPU is about **3.3–4.3%** of process CPU, so neither allocation nor page-fault work is declared free. Process resident/footprint observations are roughly 1–2 GiB, not evidence of exhausting 96 GiB capacity; they say nothing about memory bandwidth. Average busy cores and IPC do not measure peak machine utilization, especially across P/E cores.

The native `proc_pid_rusage` layout matches all 35 uint64 fields in the pinned SDK. CPU seconds come from POSIX `getrusage`, not raw Mach-time fields. Instructions/cycles are process endpoint deltas; there is no FLOP counter or peak-ALU comparison. The workload contains substantial decoding, integer indexing/modulo, casts, masks and comparisons, so peak floating-point arithmetic is not an appropriate measured throughput ceiling here.

## Where CPU time goes

The separate warmed Firefox profiles use the exact original many-row benchmark and the same native artifact. The Hail-input profile has **eight actions / 6.047 wall s / 56.948 core-s**; the native-input profile has **14 actions / 6.094 wall s / 70.046 core-s**. Sampled thread CPU closely matches process CPU. These are sampled attributions, not removable-cost or hardware-saturation estimates.

| Component | Hail input | Native input | Meaning |
|---|---:|---:|---|
| Hail decode-range subtree | 42.26% inclusive | Not the native reader | Varint/null/list decoding, decompression and column construction |
| Array indexing | 12.16% self | 16.08% self | Build child indices from offsets, nulls and requested indices, then Arrow take |
| Integer modulo kernel | 8.49% self | 14.71% self | Existing typed batch modulo implementation |
| Shared reduction executor | 13.37% inclusive | 23.29% inclusive | Shared producers and packed Boolean sinks |
| Memory move routine | 5.24% self | 1.29% self | Observed copies, not a memory-bandwidth meter |

Inclusive rows overlap their callees and must not be added. The multi-reduction wrapper is about 40%/60% inclusive on Hail/native input, but includes its ordinary, unfused siblings; that is not the cost of the new shared kernel alone. The actual query has 33 aggregate outputs, with 12 sinks in the shared region. The larger remaining scope includes other reducers and producers.

Source inspection explains two concrete costs. Hail's reader parses a row-oriented variable-length wire format and stages values/nulls/list ranges before constructing columns (`legacy/decode.rs`, `probe_values` and `buffered_mixed_batch`). The indexing routine (`expressions.rs`, `array_index_rows`) validates/normalizes each requested index into an UInt32 buffer, then invokes Arrow take. These costs remain even though setup/factories are already outside entry loops. The native input avoids the Hail decoding subtree and makes expression/indexing costs more prominent. This identifies investigation targets; it does not establish how much can be removed safely.

## Hardware instruction counters

One successful bounded Instruments CPU Counters trace covers process startup/imports, a warmup and three Hail-input actions. It is **not** the warm-only window above. Pairing each of **47,992 thread/core intervals** with its own cycle count gives these descriptive cycle-weighted vendor categories:

| Category | P cores only | Mixed P/E |
|---|---:|---:|
| Instruction delivery | 21.14% | 19.64% |
| Discarded work | 14.72% | 14.10% |
| Instruction processing | 14.63% | 14.29% |
| Useful | 49.51% | 51.97% |

These are normalized instruction-bandwidth categories, **not percentages of wall time, branch-misprediction rates or machine peak**. “Useful” means retired correct-path instructions, including software overhead. Processing combines dependencies, execution resources and cache/memory delays; discarded work includes speculation/restarts. This default mode does not split out DRAM GB/s or floating-point throughput. [Apple's counter definitions](https://developer.apple.com/documentation/xcode/addressing-cpu-bottlenecks) explain the categories. The evidence supports investigating CPU instruction/control-flow costs, especially decoding, rather than declaring either RAM or FLOPS exhausted. An earlier unvalidated timeline average is retained in raw evidence but explicitly excluded from conclusions.

## Evidence and remaining questions

[Results](results.json) retain all OS deltas and counter definitions; [manifest](manifest.json) pins **39 archive members**, the source artifact, external Firefox files and every Instruments trace file. [Evidence](evidence.zip), SHA256 `cac281790628b39a45544af1d05a2360ceec1bb89275a98245e87363c7573da0`, contains the workers, bounded-cleanup receipts, raw exported counters and analysis. No build, benchmark or profiler workloads overlapped. Raw trace directory: `/private/tmp/vail-rowwise-hardware-20261111/cpu-counters.trace`. Native Firefox profile: `/private/tmp/vail-rowwise-hardware-20261111/native-profile/rowwise-profile.json.gz`; its symbol sidecar is beside it. The Hail-input profile is pinned in the prior numeric report.

Independent source audits verified the SDK layout, raw interval pairing, no interval overlap, P/E topology, arithmetic and interpretation limits. All owned process groups are gone. No system settings, counters configuration or privileged kernel access were changed. DRAM/cache-level attribution, vector/FLOP utilization and thermal/core-frequency effects remain **unmeasured**, rather than explained away. General follow-up targets are the measured decoder/staging work, repeated indexing/gathers and remaining reduction contracts; no new optimization for those is bundled in the Unicode extension.
