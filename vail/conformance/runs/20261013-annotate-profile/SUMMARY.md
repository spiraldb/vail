# 100-field annotation profile — 2026-10-13

Workload: fresh `hl.utils.range_table(1_000_000, 14)`, 100 fields `x_i = i + idx`, then `_force_count()`. Three warmups and five measured iterations. Same optimized arm64 wheel as the stock-Hail comparison; native SHA-256 `7ebad190de2ad91e91f698664f1ee33afb06b03243c4e93f7bf91934a58cf2a5`. Source revision `f98a9bc7c81c1ab35d7c6aaaad06533c24c88a4b`. Both control and profiled runs verified one million output rows each iteration. No engine code was changed for this investigation.

## Measurements

| Metric | Control (telemetry only) | Native-profile run |
|---|---:|---:|
| Median iteration | 4.6527 s | 4.6623 s |
| Median Python query construction | 2.66 ms | 2.33 ms |
| Average cores used | 1.000 | 0.993 |
| Peak measured RSS | 388.9 MiB | 325.3 MiB |
| Peak measured physical footprint | 67.8 MiB | 74.6 MiB |

Control median instruction count: **80.43 billion/iteration**; median cycles: **17.50 billion/iteration**. Median interval IPC **4.62**. This is approximately **804 instructions per computed addition**, including all query overhead. The stack-profile/control medians differ by 0.21%; that difference is descriptive, not a statistical overhead bound.

CPU, RSS, physical footprint, cumulative instructions/cycles, instruction/cycle rates, IPC, page-ins and disk bytes were collected by a separate monitor at **10 Hz**. Profiled measured intervals contained **236 samples**, median spacing **99.999 ms**, longest spacing **104.992 ms**. CPU utilization is normalized to **100% = one fully occupied core**, not 100% of the 14-core host. The monitor uses macOS `proc_pid_rusage` V4. CPU times are Mach ticks, converted using the measured **41.6667 ns/tick** timebase; interpreting them directly as nanoseconds would materially understate utilization. Hardware counters are the OS's per-process instruction/cycle counters; cache misses and branch mispredicts are not exposed by this API and were not fabricated.

RSS includes resident mapped/shared code and differs from the OS's physical-footprint accounting. Warmups are marked separately in the charts. The small post-measurement binary hashing step is excluded from the measured maxima and cropped from the chart; complete telemetry remains in CSV.

## Profile findings

Samply 0.13.1 recorded native stacks at **500 Hz**, with a pre-symbolicated sidecar. The flamegraph uses per-thread CPU deltas, only inside the five measured intervals. Idle threads and warmups are excluded. The complete raw profile and full stacks are retained; only Python/async wrapper frames are omitted from the overview image.

- **99.49%** of measured sampled CPU was in `HailPhysicalExpr::evaluate`; **66.84%** was in its `scalar_at` subtree. Its conversion/downcast work dominates the graph.
- All **23.467 sampled CPU seconds** belonged to the calling Python thread executing native Rust. The two Tokio worker threads contributed no measured CPU samples. This is native execution, not Python/GIL-bound element iteration.
- `custom_numeric` in `crates/vail-execution/src/expressions.rs:2230` loops over rows, calls `scalar_at` for each operand, creates scalar results, then uses `ScalarValue::iter_to_array` to rebuild Arrow arrays. The flamegraph directly confirms this path is active for these additions.
- `crates/vail-python/src/lib.rs:128` hardcodes **two Tokio workers**. Fourteen source partitions in the earlier benchmark did **not** imply fourteen execution workers. For this workload even those two were idle; simply enlarging the worker pool would not remove the scalar arithmetic overhead or automatically parallelize the serial partition consumption.
- Python query construction was only about 2–3 ms. DataFusion planning happens inside the action, but the samples show evaluation dominates it. Memory footprint is modest; the observed bottleneck is CPU work in scalar conversions and execution on one core.

The next implementation targets are lowering arithmetic to DataFusion/Arrow array kernels while preserving Vail's numeric semantics, then enabling parallel evaluation of independent partitions without changing ordered output. No such performance changes were made in this profiling task.

## Artifacts and reproduction

- `profiled/flamegraph.png`: inline preview; `profiled/flamegraph.svg`: full symbols and percentages on hover.
- `profiled/telemetry.png` and `.svg`: CPU, RAM, instruction/cycle rates, IPC, phase boundaries.
- `profiled/profile.json.gz` and `profile.json.syms.json`: native profile and portable symbol sidecar.
- `profiled/collapsed.folded`: measured CPU stacks in microseconds; `hotspots.json`: complete measured stack weights.
- `profiled/metrics.csv` and `control/metrics.csv`: raw 10-Hz counters. Each folder also includes exact per-iteration counters/timing and benchmark provenance.

```sh
/private/tmp/vail-release-benchmark/bin/python benchmarks/profile_annotate.py --profile --output conformance/runs/20261013-annotate-profile/profiled
/private/tmp/vail-release-benchmark/bin/python benchmarks/profile_annotate.py --output conformance/runs/20261013-annotate-profile/control
MPLCONFIGDIR=/private/tmp/vail-mpl /private/tmp/vail-release-benchmark/bin/python benchmarks/render_annotate_profile.py conformance/runs/20261013-annotate-profile/profiled
samply load conformance/runs/20261013-annotate-profile/profiled/profile.json.gz
```

The runner includes native profiler startup before warmups, but timings cover only query construction/action. No background jobs were paused for this run; utilization and instruction totals are process-specific. The control and profile medians closely match the earlier quiet benchmark's 4.704 seconds. Native symbol tables remain available in the release wheel, but line-level debug information was stripped; LLVM identical-code folding can assign some primitive-access samples a different type alias (e.g. `Date32`) even though the workload uses Int32. Full function symbols and source inspection support the findings above; this is not a line-level attribution report.
