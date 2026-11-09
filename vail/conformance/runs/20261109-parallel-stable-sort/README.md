# Parallel stable sort: targeted laptop comparison, 2026-11-09

Stable native-key sorting now stamps each physical input stream with its partition
number and a monotonic local ordinal, then uses DataFusion parallel local sorts
and a final sorted merge. The pair preserves the old ordered-concatenation
encounter order for equal user keys without serializing source execution.
The output's existing key order and singleton partition tag remain unchanged.

The implementation reuses SourceOrdinal. Its new mode does not allow sort
pushdown before stamping and does not advertise the partition tie key as a
constant: DataFusion can reconstruct a global merge from local output ordering,
and dropping this tie key there reverses equal-key rows across input partitions.
Existing scan/window numbering modes retain their behavior.

## Measured results

| Workload | Before wall | After wall | Wall speedup | Before CPU | After CPU |
|---|---:|---:|---:|---:|---:|
| Native matrix row-key shuffle | 107.342 ms | 44.279 ms | 2.424× | 184.684 ms | 176.025 ms |
| Materialized 200,000-integer sort | 9.882 ms | 5.318 ms | 1.858× | 11.703 ms | 7.546 ms |
| Native read/count control | 13.437 ms | 13.182 ms | 1.019× | 62.544 ms | 63.384 ms |

Read-control CPU is **1.34% higher**; this adverse observation is retained.
Every row-key process median improves: before 105.57–108.81 ms, after
44.20–44.73 ms. Peak whole-worker RSS medians for that case are 1.160→1.005 GB;
this includes imports/warmups and is not isolated allocation or retained memory.

The matrix workload uses the original upstream read/annotate/rekey/force-count
shape on the pinned 1,000-row × 2,535-sample, four-partition native fixture at
`/private/tmp/vail-hail-four-20261109/inputs/native.mt`. It is not the full-suite
fixture and establishes no new Hail comparison. The integer sort materializes
all output to Arrow and checks its count/endpoints. The read control is unchanged.

Three fresh processes per version/case, each with three warmups and five timed
calls: 18 workers, 90 samples. Fixed balanced order B,C,C,B,B,C per case. Medians
are across per-process medians; construction and actions are timed, imports
excluded. Both versions use Rust mimalloc, four Rayon threads, BLAS threads one, Python
default allocation and PyArrow mimalloc. The existing Tokio runtime has 32
workers; the configured VAIL_EXECUTION_CONCURRENCY=4 environment entry is ignored.
The matrix input has four partitions and the integer input has sixteen. All
task-owned builds/tests/profiles were stopped during the exclusive timing lease. These are
descriptive laptop results, not a significance claim. Integer operations are
single-digit milliseconds, so their relative overhead matters.

## Source, correctness, and plans

Source commits: isolated sort candidate `8890fa75f` plus sort-pushdown fix
`f2c8623d3` and constant-metadata correction `5d2a75f41`. The normally built
combined validation source is `46d95c159f356585c15f54de0f0facb47a913e90`, Python
native SHA256 `2723af5a47571de26bf525e3960aa3d27e09505b31a9d5b6e979716bf564338e`.
The comparison uses the existing normal mimalloc baseline from `1abc1ed86`.
The combined candidate also contains separately reviewed legacy metadata and
entries-emission changes; these native matrix rekey/integer/read workloads do
not invoke the legacy reader or entries-table emission.

The first combined artifact (`32bf`, native `874846d2`) failed one public mixed
entries test: key 3 emitted row 6 before row 5 across partitions. Full core
collection and materializing that core before entries were correct. The final
conservative constant-metadata correction makes the minimal 9-row/one-column
case and the mixed duplicate/null/filter case pass. The final optimized plans
show complete `(user key, partition, local ordinal)` keys in both the local
sort and global merge, with no global window or ordered concatenation before
stamping. Required review was conducted in thread
`01a14128-21b8-7a53-b399-4ad8f9b31f4f`; root holds integration authority.

Root's exact final combined gates: 299 Python passes and eight Rust
entries correctness/allocation passes. The sort's four new public cases cover
ascending/descending nulls, nested payloads, repeated actions/head, equal-key
encounter order across partitions, union/explode/filter/resort, and internal
field-name collisions.

Raw worker samples are in `records.json`; numerical summary in `summary.json`.
`evidence-hashes.json` pins the local driver, actual plans, counterexample proof
and raw logs at `/private/tmp/vail-parallel-sort-20261109-evidence`.

Root normal-build identity receipts link the worker native paths to source `46d95c159` and native `2723af5a`. These rows record paths and allocator names, not independently verified per-worker native/frontend/input hashes. Subprocess completion is recorded; separate positive process-group absence certification is not claimed.
