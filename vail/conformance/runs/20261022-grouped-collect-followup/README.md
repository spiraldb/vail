# Grouped collect: preserve parallel inputs and avoid ordered count copies

The upstream query localizes a MatrixTable, groups by its row key, collects each
full row value, and forces evaluation while counting output rows. The existing
native DataFusion `array_agg` remains unchanged.

Two unnecessary operations were found:

* Grouped lowering joined all table globals before determining whether the
  grouping or aggregate expressions read any globals. For localized matrices,
  this attaches unused list-valued column metadata and obscures row partition
  properties. The new path follows the existing total-aggregate demand rule;
  folds retain their positional globals contract.
* `_force_count` could bypass a terminal sorted merge only through a restricted
  set of wrappers and fan-outs no larger than execution concurrency (32). Normal
  singleton partition views and consumer wrappers blocked that rewrite, retaining
  the merge, which
  interleaves/copies entire nested output rows merely to discard them after
  counting. The rewrite now traverses/replays proven singleton views (index zero,
  exactly one input partition) and consumer wrappers, retaining one result stream
  without ownership forks. Native `CoalescePartitionsExec` drives all inputs and
  forwards complete batches without ordering/interleaving them. All safe output
  projections are still evaluated. Fetched merges and volatile projections keep
  the original path. Native coalescing has tasks and queue capacity proportional
  to input partition count, not a constant-memory concurrency bound.

## Scope and baseline provenance

The full upstream random 250,000 × 1,000 fixture is absent locally. No cloud VM
was started and that large fixture was not regenerated. The representative
persisted fixture has 20,000 × 1,000 entries and 32 partitions, deterministic
`x=(row_idx+col_idx)%91`, and occupies 41 MiB. It exercises the same localization,
full-row collection, and count operation but is not the full upstream benchmark.

The pre-change diagnostic used the pre-existing normally built
`/private/tmp/large-matrix-sum-env`, its source worktree at
`6ff5d34e1b3886855123007d9e3f84af8f229d12`, native SHA256
`ba4ca8743a6a7ddf02fb432d30cd15a3020ae3de100af8c24ed998329ee2161e`.
No compiled module was copied or reused as a build-cache workaround. The final
candidate is built normally in its own target with Rust 1.98 release and kache.
Baseline/native source provenance differs outside the scoped changes, so these
are attribution diagnostics, not a controlled speedup comparison.

The three serial baseline query runs (with unrelated builds active) took
0.1371 / 0.1408 / 0.1394 seconds and 0.2650 / 0.2512 / 0.2444 core-seconds.
Peak process RSS was about 470–479 MB. The saved baseline 1 kHz profile includes
process startup/imports; its leaf samples include Int32 interleave 14.6%, list
interleave 8.5%, and memmove 8.1%. Samples include blocked/system frames, so these
percentages are descriptive rather than normalized pure-CPU instruction counts.

## Final validation and profile

Production source `cb38af1ce5e34cfeb108a4462d13f4e9d86f8360` (including parent
`a120a86e60c0c103ddbc0ea621ea20570f1621a1`) was source/test reviewed by the required
reviewer. The normal final release build used Rust 1.98, isolated target
`/Users/danielking/.codex/worktrees/grouped-collect-followup/vail/vail/target`,
and the configured kache wrapper. Native SHA256:
`f32664067c0761f1982884d4a95fc6dc092be46942bea22815abb81ec6aafd14`.

Six focused public test files produced **62 passes and one pre-existing failure**:
`test_collection_aggregate_matrix_scope_and_lookup_context` reports an unrelated
collection-scope UDF output-length mismatch (expected 3, got 1). The same test
fails on the unmodified pre-existing normal baseline artifact; both logs are
retained. All new tests pass, including 64-partition nested grouped collect,
persisted matrix localization/retained globals, referenced globals in keys and
aggregate inputs/fold, and existing force-count execution/error tests.

The representative read plan now retains certified nonoverlapping source ranges,
using native `array_agg` in sorted `SinglePartitioned` aggregation, without the
hash exchange and per-partition sorts present in the baseline. Ordered collection
output still uses a merge normally; the count action avoids that merge's work.

Final diagnostic query runs were 0.06345 / 0.03723 / 0.03163 seconds and
0.20476 / 0.18434 / 0.17442 core-seconds. Process peaks were 168–180 MB. These were
serial runs with unrelated builds active, not a controlled latency comparison.
Baseline/final medians were 0.13939 → 0.03723 seconds, 0.25123 → 0.18434
core-seconds, and about 470 → 170 MB process peak. The full upstream workload and
Hail comparison have not been rerun.

The final 1 kHz profile has **no SortPreservingMerge samples**, and inclusive
interleave attribution falls from 23.68% to 0.32% (one native array_agg sample).
Native array_agg remains, but the unnecessary ordered copying of complete rows
for count is gone. The profiles include startup and blocked/system samples; use
caller attribution, not the percentages as a hardware efficiency claim.

Final profile viewer (local service must remain running):
https://profiler.firefox.com/from-url/http%3A%2F%2F127.0.0.1%3A3093%2F0hs8j8c31ss90p6blaa6mdll3fdw3l8mh4vqbhy%2Fprofile.json/?symbolServer=http%3A%2F%2F127.0.0.1%3A3093%2F0hs8j8c31ss90p6blaa6mdll3fdw3l8mh4vqbhy
Saved profile includes resolved symbol names for independent loading.
