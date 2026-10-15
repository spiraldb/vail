# Benchmark follow-up, 2026-10-15

Changes on `dk/columnar-reductions-and-reader-pushdown`:

- `ccd6eaa`: DataFusion expressions for PL probabilities/dosage and packed-call predicates; typed integer-array text imports and type aliases.
- `e865239`: avoid unused column contexts in matrix reductions and redundant sorts when localizing certified keyed rows.
- `4d38f34`: project away the original cell array before UNNEST; discover only the needed prefix for HEAD with unknown partition counts.
- `15175cc`: also omit unreferenced row structs before entry expansion.

The original entry context retained both the original cell array and the array being exploded. UNNEST consequently repeated the original array for each emitted entry. Moving projection before UNNEST removes that multiplicative allocation. HEAD previously counted all unknown-size partitions before applying its limit. It now counts bounded prefixes in partition order and stops once enough rows have been found; selected execution plans remain independently executable.

## Full upstream benchmark probes

Cached `profile25_mt` fixture, fresh Vail worker per benchmark, local machine, 32 GiB process-tree memory guard. Existing Hail timings were reused; Hail was not rerun. These fixture-backed Hail baselines came from a different machine, so no speedup ratio is claimed.

| Upstream case | Previous result | Current seconds | Peak RSS GiB | Core seconds |
| --- | --- | ---: | ---: | ---: |
| `test_matrix_table_array_arithmetic` | Missing `pl_dosage` | 22.119 | 9.044 | 23.930 |
| `test_matrix_table_take_entry` | Exceeded 32 GiB | 3.419 | 4.035 | 3.966 |
| `test_matrix_table_aggregate_entries` | Exceeded 32 GiB | 6.235 | 4.898 | 7.177 |

One attempt each, not distributions. PL arithmetic measured at `e865239`; take/statistics at `15175cc`. Final take/statistics Python was copied from source into the isolated installed environment; native code came from the bounded-head release wheel. The final extra row projection gave no convincing memory improvement over the immediately preceding fix, but removes demonstrably unused fields.

Updated 84-case inventory: 57 passed, 18 failed, 5 memory-limited, 3 timed out, 1 unsupported. Some failures are fixture-preparation failures. This is not a fresh run of all 84 at the latest revision: only affected cases were rerun, and each replacement records its revision.

Regression validation: 179 targeted tests passed for the API/context changes; subsequently all 60 matrix tests passed for the UNNEST and row-pruning changes, and 64 partition/scan/stream tests plus 10 subtests passed for bounded HEAD. New regressions assert that entry expansion excludes the original cell array, entry-only expressions exclude row contexts, and HEAD does not evaluate unrelated later partitions.

A proposed ordering-metadata/partial-sort change increased planning cost on a tiny nested explode graph and was backed out. Keyed `entries()` global sorting remains unresolved. Other remaining problems include large constant-array expansion, previous-nonnull scans, fixture-generation failures, and cases below the requested performance target.

Results and 10 Hz charts: `conformance/runs/20261014-affected-throughput/after-profiles-final/report.html`. Raw new attempts: sibling `other-fixes`, `other-fixes-v2`, and `other-fixes-v3` directories.
