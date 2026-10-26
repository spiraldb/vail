# H089 current CPU profile: expansion/gathering dominates the SUM kernel

The single explicitly authorized permission experiment succeeded on the accepted Linux native5a8613c7. Perf captured **5916userspace CPU-clock samples, zero lost**, after the first completed public ACTION. This is a requested3second99Hz DWARF recording of the actual biallelic AD coordinate aggregate, not historical profile attribution or a synthetic reducer loop.

| Sampled function/path | Inclusive | Self |
|---|---:|---:|
|DataFusion UnnestStream::poll_next|48.33%|1.15%|
|Arrow take|37.36%|0.00%|
|Arrow list take|23.51%|7.32%|
|Arrow MutableArrayData::try_extend|20.66%|4.92%|
|Arrow take-bitmap construction (inlined collect_bool)|—|8.10%|
|Vail Coordinates::update_batch|4.73%|2.33%|
|Vail DenseNumeric::update_inner (SUM)|1.56%|1.54%|

**Inclusive rows overlap and must not be added.** Unnest inclusive time includes child/upstream work, so48% is not a promise that deleting the operator saves48%. But the current take/list/bitmap/extension stacks make entry expansion and intermediate gathering a credible, high-value target. Rewriting typed SUM arithmetic is poorly supported by this profile.

Other visible self costs include primitive extension4.01%, list-offset construction3.13%, UInt32→UInt16 conversion3.03%, and UInt32→UInt8 conversion1.13%. Do not assign the conversions to GT or claim unrelated fields were decoded without source/projection proof. Some libc PCs remain unresolved. An88.13%inclusive/0%self synthetic0xffffffffffffffff root reflects incomplete/async unwind ancestry, **not88%unknown CPU execution**. The raw flat report uses a0.25%display threshold; listed self rows are not a complete100%breakdown. This is one short descriptive sample, no variance estimate or hardware ALU/cache/bandwidth measurement.

## Executed workload and limits

All11completed public aggregate actions returned the full observed result `[315761156,29304213]`, matching the earlier four-action run. The first action was warm-up; the subsequent actions existed only to keep representative work active for the one3second sample. Their timings are retained but are **not a new unprofiled throughput comparison**. The earlier median0.639741wall/15.568667CPU-s remains in7dc4346e.

The unchanged approved child checks its nominal6second post-warm-up loop limit after each action. Consequently the final in-flight action completed at a summed6.1803seconds after warm-up, about0.18s beyond that nominal window. This overshoot is disclosed rather than silently reporting a hard6second cap. The actual recording remained one requested3second sample; all actions were below10seconds, count11<12, peak4109824000bytes<6GiB, and child elapsed12.57seconds<60. No rerun or source variant was used to improve results.

Artifact/import/frontend/input guards all passed. Source8dcd426b is production-equivalent to current main used for the preceding run; no rebuild or binary copy. The source-confirmed diagnostic entry Unnest path from7dc is now corroborated by actual runtime take/Unnest stacks. This does not prove nested requested field IDs or quantify every source/codec cost.

## Permission and cleanup proof

Before work: exact guest identity, provider1800second STOP guard, idle same-user process inventory and script SHA checks. One boot SSH refusal occurred before guest readiness and is retained; no workload/permission retry occurred.

The wrapper captured originalparanoid4, verified noninteractive sudo, changed **only guest runtime**4→2, and ran perf as the normal user with fixed userspace CPU-clock/PID/DWARF parameters. Its finally restored4 and independently verified4. All command process groups finished, no pending cleanup, monitor/query/sampler exit0. No persistent file, capability, installation, sudo perf or broader event. A second post-run read still reports4.

After recording, two bounded offline report extractions completed successfully; no workload ran concurrently. All11remote raw file hashes and both downloaded archives were independently verified, including decompression SHA for the50,408,260byte trace. Owned-process idle check showed only systemd/SSH/ps. VM was proactively stopped and provider **TERMINATED** verified. Both VMs remain unowned/off; no resources are held by this task.

## Durable evidence

Small raw receipts and compressed unmodified perf reports are in `receipts/`; demangled header summaries are derivative text produced with an already-installed rustfilt (no install/build). `recording.json` records raw/compressed trace hashes and paths. Raw trace is preserved outside Git at `/private/tmp/h089-permitted-profile-20261026-receipts/perf.data.gz` (11021309bytes), plus the stopped VM's original50.4MBfile. Original denied profile and all prior outputs remain7dc4346e. No raw trace is overwritten.

Next step: a **source-only narrow proposal** tracing public AggArray lowering through MatrixTable entry context/Unnest and reusing existing coordinate aggregate/state/merge over owning lists. No implementation, speedup prediction, dependency or Vortex-stack change is approved by this evidence alone.
