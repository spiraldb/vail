# Legacy table joins with many partitions

The 1,000/1,000-partition join now takes **2.889 s**, compared with **140.029 s**
on normally built current main (`6b6eb3b6`): **48.5× faster**. Both return exactly
10,000,000 rows. Peak process memory drops from **17.676 GiB to 0.802 GiB**.
Whole-worker CPU drops from **190.269 s to a 21.343 s median** (8.9× less).
The main baseline is one fresh sample; final candidate numbers summarize three.

## What was wrong

The interval join planner understood native fragment bounds, but not the legacy
reader's already-parsed integer partition bounds. It scanned both full inputs to
rediscover endpoints. Its direct interval clipping path also did not understand
legacy sources. Each output interval therefore reset a partition view retaining
the entire source DAG. With 1,000 output intervals and 1,000 input partitions,
this cloned and reoptimized roughly a million source branches per side.

For mismatched partition boundaries, the fallback also replayed whole large
files for each overlap, applying a residual key predicate afterward. The new
path creates only the selected file's scan and supplies ordinary typed integer
comparisons to existing legacy index pushdown. The index can seek to the needed
rows instead of repeatedly decoding a large partition.

After that fix, profiles exposed another setup cost: constructing two complete
Vortex sessions, including all plugin registries, for every file. The final
change reuses a prepared session through a bounded per-read cache keyed by the
execution runtime. Initialization is lazy, so synchronous metadata loading never
captures an absent runtime. A runtime change replaces the single cache entry;
active scans retain independent session clones. Each layout still has its own
file cursor, decoder, buffers and range cache.

No source partitions are forcibly combined. Unknown or null-containing integer
bounds retain endpoint discovery; only metadata counts of zero certify empty
partitions. Residual clipping remains for conservative bounds and unsupported
index predicates. Duplicate-key multiplicity and all join modes are preserved.

## Results

All inputs are the original Hail-written 10M-row fixtures. Hail and pre-fix Vail
columns below are archived comprehensive-scan medians from frozen `e79cf6ff`.
Final Vail reads those same legacy fixtures, with three fresh workers per case.
The direct current-main baseline above independently confirms the large effect.

| Left / right partitions | Archived Hail | Archived Vail legacy | Final Vail legacy |
|---|---:|---:|---:|
| 10 / 10 | 3.201 s | 1.560 s | 0.341 s |
| 10 / 100 | 4.366 s | 2.668 s | 0.463 s |
| 10 / 1000 | 6.917 s | 78.073 s | 2.962 s |
| 100 / 10 | 4.189 s | 2.816 s | 0.472 s |
| 100 / 100 | 3.690 s | 2.936 s | 0.438 s |
| 100 / 1000 | 5.201 s | 82.052 s | 2.998 s |
| 1000 / 10 | 6.920 s | 77.849 s | 2.971 s |
| 1000 / 100 | 5.226 s | 81.013 s | 2.984 s |
| 1000 / 1000 | 4.634 s | 145.118 s | 2.889 s |

Times cover `_force_count`, including its planning/execution; descriptor loading
and constructing the join IR precede the timer. The archived upstream benchmark
also includes those small read/constructor costs. There is no explicit warmup;
OS caching is uncontrolled. CPU and RSS cover the complete worker, including
imports and NumPy/OpenBLAS initialization. OpenBLAS startup spin remains visible
in the final profile, so these CPU totals are not isolated decoder costs.
The machine is the same 32-vCPU GCP n2d VM used for the comprehensive scan.

## Profile evidence

Clean frozen-baseline profiling reproduced 134.36 s. Inclusive sampled CPU under
DataFusion filter pushdown falls from 24.39% to 1.04% after direct scan planning.
This is a reduction in both relative share and total CPU work, not decoding.
At that point Vortex session construction accounts for 40.72% of remaining
samples. After session reuse it is below the report's 0.1% threshold. The
join-only candidate took 3.406 s / 34.47 process CPU-seconds; session reuse lowers
that to 2.889 s / 21.34 process CPU-seconds.

Profiles use `perf record -e cpu-clock -F 99 --call-graph dwarf`; guest hardware
instruction/cache counters are unavailable. A first baseline attempt accidentally
overlapped fixture preparation and is preserved under `contaminated-initial`;
it is excluded from every comparison. The subsequent clean profiles and timings
had no concurrent build or workload.

A remaining opportunity is to expose certified legacy key ordering at the scan
leaf, which currently only preserves its physical file order. The final profile
still shows ordered merge processing. That is separate from the fixed quadratic
planning and repeated session setup; no additional ordering claim is made here.

## Validation and provenance

- **183 public tests pass:** 19 new Hail-oracle/concurrent-action tests, 48 existing
  join tests, and all 56 table + 60 matrix compatibility fixtures.
- The new 19 tests also pass on normally built main, providing baseline oracles.
- New fixtures are genuinely Hail-written; all file hashes and Hail join result
  oracles are included with their generation script. Tests check full records,
  multiplicity, missing keys, Int64 extrema, empty inputs, forwarding, and all
  four ordinary join modes, plus membership joins and concurrent actions.
- The imported upstream archive contains AppleDouble sidecar files named like
  datasets. They reproduce the same `Not a directory` failure on main. The
  semantic run used a clean symlink view of all 116 actual dataset directories.
  The final two-line test collector change filters `is_dir()`; its original-path
  fixture-presence check passes, selecting those same 56/60 datasets.
- Source/test revision: `a8c08ac07d6500c259bd4a5ee411793acacddfe1`.
  The build predates the collector-only Python change; all changed Rust files
  match the final source exactly (`source.sha256`).
- Normal isolated release build using kache + maturin; installed native SHA-256:
  `f6408241fe1a4f78b52974a2524abb01fa64e926afee86d2b88b1db8468c3df8`.
- Normally rebuilt main baseline native SHA-256:
  `29b5998dc15b82534967b0ce4c749c561fbd24368259b9ef181ea13fef42ece9`.

[Structured results](summary.json), [CSV](results.csv),
[profile summary](profile-summary.json), [test log](tests.log).
Raw profiles, all samples, commands/build logs, hardware and provenance are in
`raw-evidence.tar.gz` (retained locally, not committed): SHA-256
`d173fa7cc85b5a65cb6c13a97369c6885c4ad29c427f978d3ecd5e0ca0b0b5e0`.
Evidence was downloaded and independently checksum-verified before stopping the
primary VM. No production code or user checkout was changed on main.
