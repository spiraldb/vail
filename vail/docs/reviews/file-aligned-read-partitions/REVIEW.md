# File-aligned read execution partitions

- Reviewed production revision: `c0df048384a6800b8b6c095c5c455fcc5c1fdef8`.
- Reviewed main ancestor: `471f8479b0544fd6d0f27abf1c05c1f027906594`.
- Required reviewer: [review chat](codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f).
- Final disposition: approved for main integration on 2026-10-16. Reviewer allowed this documentation-only child and fast-forward integration while main remains the reviewed ancestor.

The review covered ordered file-task derivation, checked persisted totals and
offsets, empty snapshots, duplicate-key bounds, ordinal interval consumers and
pruning. The requested cleanup removed a private plan-identity assertion and
switched the storage regression to the public descriptor accessor. No production
blockers remained. The reviewer inspected source and reported runtime evidence;
the reviewer did not independently run builds or tests.

Matching runtime validation: **33 passed, zero failed**:

| Suite | Passed |
|---|---:|
| vail-storage library `local_table::tests` | 6 |
| vail-storage `streaming_write` | 9 |
| vail-table `file_pruning` | 1 |
| vail-table `ir` | 10 |
| vail-table `physical_partitions` | 7 |

Storage commands used `cargo test --release -j 2 -p vail-storage`, separately
selecting `--lib local_table::tests` and `--test streaming_write`. TableIR used
`cargo test --release -j 4 -p vail-table --test physical_partitions --test
file_pruning --test ir`. All used the repository toolchain and the shared target
directory `/Users/danielking/projects/vail/vail/target`.

For the TableIR run, Cargo configuration redirected the `datafusion-common` and
`vortex-array` patch paths to the existing copies under
`/Users/danielking/.codex/worktrees/1611/vail/vail/patches/` to reuse compiled
artifacts. `diff -qr` confirmed both full source directories were identical to
the candidate copies. Vortex layout and all Vail production sources came from
the candidate worktree.

Regressions establish that one writer partition flushed into six files becomes
six independently executable DataFusion outputs, rows retain snapshot order,
empty writer partitions disappear, an empty snapshot retains one empty task,
and an output can execute while unrelated data files are absent. This is not a
measurement of universal latency improvements or a bounded-memory concurrency
guarantee. The existing scheduler remains responsible for limiting concurrency.
