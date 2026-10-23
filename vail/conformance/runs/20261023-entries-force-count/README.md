# Entry expansion and force-count investigation

Material revision: `1ae4d7c32c858f4d1e1552fba44a0ca8c5e1371b`, based on frozen
main `e79cf6ff`. Main and the primary benchmark workload were unchanged.

## Final matched results

The same Hail-generated, Vail-native billion-entry matrix and the same 32-vCPU
GCP VM were used. Each iteration rereads the matrix; one warmup and three measured
iterations. Counts equal **1,000,000,000** in every iteration.

| Public entries force-count | Median wall seconds | Median CPU seconds | Peak RSS GiB |
|---|---:|---:|---:|
| Frozen main | 23.181 | 321.87 | 4.562 |
| Final candidate | **3.330** | **101.74** | **1.025** |

This is **6.96× faster in wall time**, **3.16× fewer CPU-seconds**, and **4.45×
lower peak memory**. Average utilized cores rise from about 13.9 to 30.6.

A separate real entries-table consumer was checked to avoid validating only a
count shortcut. `matrix.entries().aggregate(sum(DP))` returns the same
**99,526,426,397** throughout. Contemporaneous medians:

| Entries-table DP sum | Wall seconds | CPU seconds | Peak RSS GiB |
|---|---:|---:|---:|
| Frozen main | 27.653 | 220.02 | 4.871 |
| Final candidate | 25.112 | 149.97 | 4.697 |

This consumer remains expensive; its ordered expansion and source projection
are separate follow-up work. It improves about 9% in wall time and 32% in CPU,
without using the terminal count rewrite. Do not confuse this query with the
much faster direct `matrix.aggregate_entries(sum(DP))` benchmark.

## Confirmed causes

1. Native descriptors omitted established row-key uniqueness/column-order proofs.
   Legacy readers retain a row uniqueness proof. Native `entries()` consequently
   selected duplicate-key composite-order expansion instead of the parallel
   unique-row path.
2. Count's original optimization recognized terminal Unnest, not that custom
   expansion or its native partition wrappers. It constructed per-entry index
   vectors, concatenated singleton list slices, and materialized flat records.
3. A late count-only replacement still inherited an ordered row stream whose
   source partitions had already been grouped. CPU fell near decode cost while
   wall time remained about ten seconds. Count demand must arrive before that
   grouping, not merely after physical planning.
4. Sorting already-sorted column metadata produced an identity gather that still
   copied every entry payload. A general identity-gather buffer-retention path
   removes that copy.

The frozen ablation below isolates source work from entry expansion. Context
variants are private diagnostic ablations, not public benchmark replacements.

| Frozen execution path | Wall seconds | CPU seconds | Peak RSS GiB |
|---|---:|---:|---:|
| Core full read/count rows | 2.564 | 76.39 | 0.933 |
| Unordered entry count without column context | 2.590 | 77.62 | 0.953 |
| Unordered entry count with column context | 2.861 | 85.88 | 1.080 |
| Public keyed entries count | 23.181 | 321.87 | 4.562 |

## What changed

- Persist only established uniqueness/order proofs, with conservative defaults
  for older snapshots. Entry payload format and positional alignment are unchanged.
- Recognize terminal matrix-entry count through cardinality-preserving field-only
  maps, globals and key/order wrappers before ordered key-run preparation.
- Preserve every original child field/expression. Bind column width through the
  original globals in the same action graph, keeping scalar-subquery/RNG captures
  consistent, and count independently in the original source partitions.
- Count list ranges plus logical ancestor/entry validity masks, using bitmap
  combination/popcount. Defined entries whose payload fields are missing still
  count. Sliced offsets and null owners remain distinct from missing entries.
- Retain a late count replacement for other safely recognizable physical cases,
  with explicit native partition/cooperative-wrapper traversal.
- Avoid identity payload gathers while retaining original behavior for reorders,
  duplicate selections, invalid indices and missing owners.

No entry-shaped table, per-entry row/column/entry index vectors, or singleton
list concatenation is needed for the optimized terminal count. All work is Rust
batch execution. Per-query preparation is outside row/entry loops.

## What did not work / was discarded

- A physical-only count replacement removed expansion CPU but retained the
  already-combined ordered stream: about 10.2 seconds and 85.7 CPU-seconds.
  Early action demand fixed this serialization.
- Adding an explicit custom-operator row-alias ordering requirement generated
  redundant sorts and worsened the ordinary DP-sum consumer. It was removed.
- Replacing nested row-key getters with direct indexed aliases changed planning
  behavior and hurt that ordinary consumer even after the requirement was
  removed. Controlled tests used the same compiled candidate binary. This
  experiment was also removed, sacrificing about half a second of count time
  to avoid changing unrelated ordered-consumer behavior.

The general available-versus-required ordering audit is a separate branch. This
candidate leaves existing ordered entry construction and ordering contracts alone.
Hail itself promises compound `(row_key, col_key)` entry-table order. Its
`LowerMatrixIR.scala:942` sorts column positions, collects equal-row-key runs, and
emits columns across each run. Ordered collect/head/scan need that contract;
count does not.

## Profiles and remaining cost

Separate software sampling on frozen main recorded 14K samples, zero lost:
column gather was **43.19% inclusive**, singleton concatenation **15.21%
inclusive**. Inclusive values may overlap and must not be added blindly.

The final candidate profile recorded **5,241 samples, zero lost**. Ordered entry
expansion/concatenation disappear. Gather itself falls to about **1.62% self**.
Remaining large self costs include primitive narrowing casts (**16.33% + 7.67%**)
and Arrow offset validation (**10.76%**), with Vortex canonicalization/decoding
also prominent. These observations are about the current full-source decode
path, not proof that arbitrary validation can be removed. Hardware cycles and
instructions are unsupported on this guest.

**Remaining limitation:** this patch still reads/decodes the original upstream
entry fields, preserving forced evaluation of computed expressions. Roughly
2.6 seconds was the measured full-decode floor for this input. Validity-only
stored reads require separate native shape/validity demand into Vortex. Returning
per-row counts directly from the layout would avoid canonical GT/DP/AD/PL export;
constructing fake missing payloads inside a full entry schema would be incorrect.

## Correctness and build evidence

- Final local public suites: **124 passed, 13 failed**. All 13 failures were
  independently reproduced by unchanged tests on frozen Linux main: **85 passed,
  exactly the same 13 failed** for the matrix-table file. They are existing
  scalar-subquery/UDF-row-count failures and stale structural expectations.
- Final layout suite on Linux: **34 passed**; reader-order suite locally adds
  five passing tests. Tests include filtered versus defined all-null/empty
  entries, duplicate row/column keys, old descriptors, empty axes, head, sliced
  lists, forced entry/column-expression errors, and seeded entry/column masks.
- Rust count consumers: **12 passed** (six expansion-count and six consumer-action
  tests), including logical ancestor validity and forced errors.
- All builds were normal kache/maturin/Cargo builds in dedicated targets/envs.
  No manually saved/reused native library was used as a cache workaround.
- Linux Rust 1.98.0; kache 1.2.2; Python 3.11.17; NumPy 2.4.6; PyArrow 26.0.0.
  Flags match frozen build: target-cpu=native, lld; no build overlapped timing.
- Final Linux native SHA256:
  `a9d6dc6af7d4a054d80c663754145b3af9fe2fba3db7a7acc909ba51f990a53c`.
- Local native and Python module origins point to this isolated worktree.
  Required review before main remains for the parent task to coordinate.

Raw individual receipts and summarized metrics are in `baseline/`, `final/`, and
`summary.json`. `cloud_probe.py` reproduces the cloud cases. Profiled iterations
are separate from the three unprofiled measurement iterations.

Verified local raw archives:

- `/private/tmp/vail-entries-profile-complete.tar.gz` (frozen plans/perf/ablation),
  SHA256 `ec7b6e8b5cde53f26eea8a699670ceca62f53896c2a796706f017c38c67d5669`.
- `/private/tmp/vail-entries-count-candidate-evidence.tar.gz` (all candidate stages,
  including discarded experiments and final perf data), SHA256
  `42f37f8f69dad1b2237572d7670b983be39157846f76777caf3882299adea688`.
- `/private/tmp/vail-entries-count-build-provenance.tar.gz` (normal build logs,
  scripts, final source archive and provenance), SHA256
  `a75a463dd37e7dc7cc25c8a4be1e62755aa2ce4aa2b285af6354611cbe21b934`.
