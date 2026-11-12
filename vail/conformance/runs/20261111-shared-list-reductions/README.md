# Shared list reductions with native batch kernels

The measured implementation improves the original many-row-aggregations benchmark by **16.8% wall time / 17.9% observed CPU on Hail files**, and **23.4% / 25.3% on native files**. On the same Hail input and action, current candidate Vail is **2.443× faster than Hail** and uses **6.983% less observed CPU**. These are local point estimates from two independent processes per version, not a significance claim or a whole-suite result.

Production `c0233972584ae3bf54b6f4b9ebb9068577b6e6d2`; installed normal native `1d287bbe5511cabd62b40638f743b29b2ce6992e9e38b69683b0aa4299a4a433` (mimalloc). Baseline production `79744c031`, native `6bee98348941c946cc4d8f7504557807781d5f57cc6d59e05d60e8be286ab193`. No source changes, environment tuning, new benchmark cases, or profiler/build overlap occurred within the fixed final cohort.

## Final matched results

| Work | Old Vail wall s | New Vail wall s | Wall change | Old CPU core-s | New CPU core-s | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| Many row-wise aggregations / Hail files | 0.854385 | 0.710739 | -16.813% | 8.636849 | 7.092636 | -17.879% |
| Many row-wise aggregations / native files | 0.559748 | 0.428695 | -23.413% | 6.647138 | 4.968124 | -25.259% |
| Many column-wise aggregations / Hail files | 0.809997 | 0.812177 | +0.269% | 7.817925 | 7.908029 | +1.153% |
| Sex-specific QC / Hail files | 0.498922 | 0.514949 | +3.212% | 4.711446 | 4.741074 | +0.629% |
| Filter/unfilter entries / native subset | 0.107514 | 0.109147 | +1.519% | 0.416835 | 0.425121 | +1.988% |
| PL dosage / native subset | 0.053377 | 0.053855 | +0.896% | 0.196005 | 0.196170 | +0.084% |
| Split multi HTS / native subset | 0.148051 | 0.148540 | +0.330% | 0.389674 | 0.394645 | +1.276% |
| Generated shared numeric producers | 0.032342 | 0.020354 | -37.066% | 0.100911 | 0.055134 | -45.364% |
| Generated row-capture fallback | 0.034721 | 0.034316 | -1.168% | 0.109134 | 0.108231 | -0.827% |

Fresh matched Hail on the primary workload: **1.736594 s / 7.625062 observed core-s**. The native-copy comparison is an old/new Vail comparison on identical native files; native and Hail copies have matching logical dimensions but different physical sizes/partitioning. They are not a storage-layout parity experiment.

All adverse point estimates remain: sex-specific QC **+3.212% wall / +0.629% CPU**, filter/unfilter **+1.519% / +1.988%**, column-wise **+0.269% / +1.153%**, PL dosage **+0.896% / +0.084%**, split multi HTS **+0.330% / +1.276%**. Captured-expression fallback is **−1.168% / −0.827%**. Do not label these regressions noise or claim the change is regression-free. The independent review accepted these measured controls for this numeric checkpoint.

## What changed

The original workload has one source and one 33-output projection, but its separate list callbacks hide common producers from relational CSE. The existing typed list compiler now recognizes compatible reduction siblings in the same lexical/owner/filter domain, builds a shared register DAG, and selects only sinks that benefit from common costly producers. It does not admit field names, divisors, or benchmark identifiers. The actual workload uses **12 sinks / 29 registers**; the other outputs use their ordinary paths.

The final executor prepares Arrow/native kernel pointers, operand types, constants and scalar-broadcast requirements once. Each bounded block borrows typed leaf arrays and evaluates each shared producer once per active lane domain. Casts, arithmetic, comparisons and zero normalization reuse existing library semantics. Boolean results stay packed and feed the existing segmented bitmap reducers directly. There is no per-entry evaluator setup, per-node RecordBatch/schema construction, entry-Struct take, byte-validity propagation or Boolean repacking. Native kernels allocate their required output arrays and headers; this is not a zero-allocation claim.

Guards run before bodies; all-false domains skip body work. Partial selection takes primitive leaves rather than entry structs. A defined entry with a missing predicate stays in a fraction denominator; an absent entry contributes no observation. Parent masks, sliced offsets, null/empty owners and ordinary sibling/error order are preserved. Unsupported expressions and domains retain ordinary execution. The logical bridge only packages an opaque shared region after actual preparation succeeds; declines keep ordinary DataFusion fields visible.

The 4 MiB per-evaluation variable-workspace estimate conservatively charges all live primitive/bitmap slots, counters, selected indices/segments, cached broadcasts, kernel temporaries and header/alignment allowances before input binding. It falls back if no lane fits. Retained input owners, final/ordinary outputs, allocator overhead and concurrent evaluations prevent interpreting this as a process RSS limit. No RSS, physical I/O, owner-release or saturation improvement is claimed.

## Validation and profile

**67 Rust tests and 120 Python tests passed** on the final normal build. Thirteen new public cases also pass on the unchanged baseline. Coverage includes signed arithmetic, both float widths, NaN/infinity/signed zero, null predicates, nested null Structs, filtered entries, long/sparse rows, distinct denominators, repeated actions, ordinary Unicode/nested siblings, and eager error order. Route probes prove the original full workload and the nested-parent-null test actually use the shared executor.

An additional untimed baseline-consistency probe compares **all 33 aggregate outputs and row keys across all 24,885 rows on both file formats**; both complete ordered hashes match. Finite floats use hexadecimal encoding; NaNs compare by class. Its collect terminal differs from timed force-count and supplies no timing samples. This is baseline consistency, not an independent Hail oracle; the public arithmetic expectations are separate independent checks. Afterward all **469** enrolled input files were fully rehashed with exact file-set equality.

A separate normal Firefox/samply recording contains eight warmed original actions: **6.046537 s / 56.948329 process core-s**; sampled thread CPU totals **56.946 s**. No sampled self CPU remains in the superseded list `Execution::compute` function. Native array-DAG evaluation accounts for approximately **12.85% inclusive sampled CPU** (about **0.915 core-s/action**); this includes Arrow kernels and overlaps their individual rows. Factories are outside value loops. Inclusive percentages must not be added, and wait-endpoint samples are not CPU spent sleeping.

The Firefox profile and symbol sidecar remain at `/private/tmp/vail-multi-sink-bench-20261111/revision8/rowwise-profile.json.gz` and `/private/tmp/vail-multi-sink-bench-20261111/revision8/rowwise-profile.json.syms.json`; hashes are in the manifest. Raw gates, route/profiling receipts, benchmark workers, inputs, harness and prior experimental evidence are sealed in [evidence.zip](evidence.zip), SHA256 `02cb258e1ab2d8e9711956f34940ac2669def3d2297fa840197422147e9e2503`.

## Method and unsuccessful checkpoints

The fixed cohort has nine cases, **38 fresh workers / 114 timed calls**, one warmup then three calls per worker. Primary ordering is Hail/B/C/C/B/Hail; other cases B/C/C/B. Summaries are medians of process medians. The independent experimental units are **two processes per engine**, not six independent repetitions. Every worker and sampler has positive cleanup receipts. Hail uses Spark local[4], while Vail keeps its existing Tokio pool with Rayon4; this is not a four-core cap on Vail. CPU includes the main process and stable observed JVM endpoints and remains a lower bound for unobserved transient children. Original timed force-counts check execution completion rather than cross-engine output equality.

Earlier frozen versions are retained separately, never pooled into the final cohort:

- `ae514b736` / revision4: sharing through the old numerical block executor made Hail-input work **+38.409% wall / +28.757% CPU** and native **+33.457% / +38.409%**. Main admission failed.
- `bfc50fbf7` / revision5: selecting only beneficial sinks and sharing floating ordering keys improved that failure but remained **+14.181% / +3.666%** on Hail and **+3.602% / +4.166%** native. Main admission failed.
- `4c2d0e5bf` / revision6: batch null-mask shortcuts reached **+7.473% / −0.975%** on Hail and **−3.749% / −1.802%** native. The Hail wall regression still blocked admission.

The structural DAG alone did not improve the primary target in these experiments. The native-kernel backend was the first measured design to improve both wall time and CPU on both target file formats. A proposed source-canonicalization explanation was investigated and rejected as unsupported by the profile; no raw-source bypass was bundled.

## Remaining scope

This is a measured **count-where/fraction checkpoint**, not universal aggregation/type fusion. Compatible Boolean/numeric producers and typed missingness/list-length reads are admitted; arbitrary opaque UDFs, row captures, unsupported conditionals, list indexing and other reducers keep their ordinary route. The array-slot interface is type-general, but additional producers and reducer contracts still need implementation and measurement. The next separate proposal is shared Unicode character length using the existing native UDF, bounded StringView descriptors and physical null masks. Native View roster costs, variable output sizes, effect/error barriers and other aggregate state/merge semantics remain explicit work. Take/rekey is not changed by this patch.

Independent review: thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared final source/resource contracts and admitted the tested numeric subset with the recorded adverse controls. Final report/seal review precedes root-only main merge. No upstream PR or remote push is part of this work.
