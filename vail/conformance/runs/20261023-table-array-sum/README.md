# Table coordinate SUM: batch-owned mutable state

Exact fixture-free workload: `range_table(10_000_000).aggregate(agg.array_sum(range(0, 100)))`.
Every action verifies all 100 outputs against `10_000_000 * coordinate`.
No constant-expression multiplication or benchmark-specific algebra rewrite is used.

## Finding and change

The current-main baseline already accumulated Int32 input directly into Int64.
Its problem was calling Arrow `binary_mut` for every 100-element input row.
Each call constructed/sliced Arrow arrays, entered a builder, and finalized a new
array. Ten million rows repeated this machinery ten million times.

Change 159c2291 moves acquisition of the mutable Arrow builder outside the row
loop, then traverses primitive slices using the same typed arithmetic. Published
state snapshots trigger one copy-on-write copy per update, preserving isolation.
The added public accumulator test retains a snapshot across further updates with
null and mixed-width input, and verifies both old and new output independently.

This implementation was incorporated into the numeric reducer agent's combined
product/Boolean candidate. Those broader changes are not claimed to be independently
validated by this benchmark. Root must integrate only reviewed revisions.

## Provenance

- Baseline: main 485b7e57fcbb71cc67fe4af389c79a6ec5c57e18, normal release native
  SHA256 `041e67e865745f7c987d8f14b2daedda805c68f7c03e4f41ff3d4152f7797ca8`.
- Candidate artifact: combined source edf384f7aacd3bae4d3a62974f91743cecda3c6e,
  containing 159c2291 plus product/Boolean changes; normal Rust 1.98 release/kache
  build in the numeric agent's isolated worktree. Native SHA256
  `c86322df4a256acd378d49f413a5bf328bb555036ec677c18e0e4c338deb1434`.
- Candidate source comment-only child 13195009 documents whole-row and nonnull
  state invariants; it does not change generated execution.
- Candidate environment: `/private/tmp/large-matrix-sum-env/bin/python`.
- Stock PyPI Hail 0.2.139 environment:
  `/private/tmp/linreg-hail-pypi-latest/bin/python`; Java 11, local[32], 24 GiB.
- Default table partition count for both engines; no benchmark-only partition rewrite.

## Fresh paired measurements

Three serial Hail actions followed by three serial Vail actions. Both processes
were initialized before their query timing, but the first action includes JIT,
planning, and first-action setup. There was no dedicated warmup. Timings include
query construction and result verification; exclude environment initialization.
CPU is 10 Hz process-tree user+system time with exited children retained; sampling
and monitor overhead are included. Peak RSS is concurrent process-tree RSS.

| Engine | wall median [min,max], seconds | CPU median [min,max], core-seconds | peak RSS max |
|---|---|---|---|
| Hail | 0.6571 [0.6480,2.4992] | 7.7047 [7.2342,15.0783] | 1.125 GiB |
| Vail | 0.04719 [0.04565,0.25792] | 0.5753 [0.5517,0.7949] | 0.198 GiB |

Median Hail/Vail ratio: **13.92 wall, 13.39 CPU**. All six actions pass.
The very short steady actions and first-action spread limit precision. Background
Docker/kache/system activity was observed; no competing heavy build or profile
was active in the reserved timing window. This is a narrow laptop result, not
an overall benchmark-suite result.

## Before/after profile

Saved 1 kHz samply profiles include three actions and startup. Symbols are saved
in the `.syms.json` sidecars. Baseline profiled warm actions: 0.434/0.476 seconds,
5.12/5.63 self core-seconds. Candidate profiled warm actions: 0.0527/0.0513
seconds, 0.571/0.591 self core-seconds. These profiles are diagnostic, not an
interleaved controlled before/after trial.

`analyze_aggregate.py` restricts CPU-weighted samples to stacks inside the numeric
coordinate update. Baseline shows repeated Arrow builder/ArrayData work;
candidate has **99.64% self samples in the primitive update loop**, and
`PrimitiveArray::into_builder` is only 0.02% of that aggregate subset. Thus the
identified per-row setup disappears. This does not prove peak SIMD throughput.

Native COW regression and broader matching runtime results are recorded by the
numeric reducer agent's integration evidence; they remain a merge requirement.
