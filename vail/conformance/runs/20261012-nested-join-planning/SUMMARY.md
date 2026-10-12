# Nested keyed-join planning fix

The slowdown was reproduced with tiny keyed tables. It was excessive planning
and replay preparation, not a confirmed deadlock.

## Cause and change

- Exposed execution wrappers recursively reset their inputs, although
  DataFusion's reset traversal already resets those children. Wrappers now reset
  only their local state.
- DataFusion's tree traversal expands shared physical-plan inputs into separate
  trees. Vail's replay traversal memoizes physical-plan identities and delegates
  state reset and child replacement to DataFusion. Each replay has an independent
  cache; shared inputs within one replay remain shared. Resetting does not change
  schema, partitioning, or ordering, so cached plan properties can be retained.
- `key_by` with the identical existing key now returns the ordered input. It
  retains interval partitions and avoids another global sort/repartition between
  joins. A changed key still sorts normally.

Join computation remains DataFusion sort-merge execution with key-interval
partition planning. This change does not cache or materialize rows.

## Local debug-build probes

| Probe | Before | After |
| --- | --- | --- |
| 8 rows, 4 source partitions, 2 joins with repeated `key_by` | 4.289 seconds | 0.145 seconds |
| 4 rows, 2 source partitions, 3 joins with repeated `key_by` | Exceeded 30-second timeout | 0.314 seconds |

Timings include planning and collection, and are measurements on the local
machine. `benchmark.py` reproduces the query using arguments `8 4 2` or `4 2 3`.
A separate changing-key probe (`join(...).key_by('c').join(...).key_by('k')`)
completed correctly in approximately 0.5 seconds and is covered by a regression.

## Validation

- Full Python suite: **698 passed, 1 skipped, 19 subtests passed**.
- `cargo test --offline -p vail-table`: **50 passed, 0 failed**.
- Regression coverage: exposed children reset once even under nested wrappers;
  shared plan inputs stay shared within a replay but execution state is fresh
  across replays; expression sources retain once-per-action evaluation;
  nested joins after a genuine key change can be collected repeatedly;
  three consecutive joins return correct rows; identical-key rekeying preserves
  interval partition contents.
- Temporary profiling instrumentation was removed and the native extension
  rebuilt before the final suites.

Logs, metrics, the benchmark script, and source hashes are saved in this folder.
