# Globals and DataFusion execution: recorded results

Run: 2026-09-25 16:29:55–16:31:53 UTC. Full commands, stdout/stderr, source hashes,
and native extension fingerprints are preserved alongside this report. Source drift
was **not detected** and the rebuilt extension stayed unchanged during the suites.

| Suite | Passed | Failed / unavailable |
| --- | ---: | --- |
| Vail Python + harness | 114 tests, 19 subtests | 0 |
| Rust workspace | 40 tests | 0 |
| Recorder self-tests | 9 tests | 0 |
| Native conformance | 4 fixtures | 0; none unsupported |
| Selected original Hail table cases | 93 cases | 1 failure |
| Separate Hail oracle | 0 fixtures | 4 unavailable |

The sole selected upstream failure is `Tests.test_annotate`: `AttributeError: map`.
It needs higher-order collection expression lowering. Newly included unchanged Hail
cases for `transmute_globals` and `_same` global/type/row differences pass. This is
an explicit source-extracted subset, not execution of the full Hail test inventory.

Hail oracle startup fails with `ModuleNotFoundError: No module named 'hailtop.version'`.
Native fixture expectations remain source-derived, not oracle-confirmed. The recorder
exits nonzero because of the known upstream failure; this is not an all-green Hail
compatibility claim.

## Implementation verified

All implemented TableIR nodes now lower to DataFusion physical plans. Standard
projections, filters, limits, sorts, cross joins, merge joins, hash joins, and count/
distinct aggregates execute in DataFusion. Sources, ordinal assignment, key-domain
validation, and bounded tail are DataFusion extension nodes; they do not interpret
TableIR. The previous pull interpreter and pre-sort table collection are removed.

Globals are a lazy singleton Arrow relation. Python supports constructor globals,
`annotate_globals`, `select_globals`, `transmute_globals`, field references, `globals`,
and `hl.eval`. Global bindings enter row plans through a singleton build-side cross
join. Write/read preserves a checksummed Arrow IPC batch separately from Vortex row
files, including empty Tables and snapshot-pinned readers.

`_same` uses a full outer DataFusion merge join on unique row ordinals, followed by
Hail recursive similarity, filtering, and a one-difference limit. Missing tolerance
uses distinct key groups and a full outer hash join. Globals are compared through
a separate singleton DataFusion join.

Coverage includes multi-file globals round trips, metadata-only construction with
missing data, globals evaluation without reading row files, corruption checks,
source/scope validation, empty fields, nested/missing values, large integers, special
floats, and immutability across global updates. Plan tests verify scan ordering
properties and the absence of sorts on read. A generated million-row TableIR sort
spills under a constrained DataFusion memory pool without collecting its input first.

The globals conformance fixture previously copied a global to a row field with the
same name. Hail's `check_collisions` rejects that operation. Both native and oracle
adapters now use `global_`-prefixed output names, and the source-derived expectation
was corrected accordingly; source-mismatch validation remains included.

## Remaining boundaries

The embedded plan uses one ordered physical stream, preserving Hail logical partition
identity in a private Arrow column. Parallel physical execution, distributed sorting,
and a logical optimizer pipeline remain future work. The Python default task context
has no configured memory limit; the spill regression supplies one explicitly.

Globals use the shared supported type system: primitive numbers, booleans, strings,
structs, arrays, and typed missing values. Sets, dictionaries, tuples, loci, and
intervals still need shared type/expression support. Cross-table `index_globals()`
and a general `Table.join` API are not yet exposed. Legacy `.ht` reading remains
deferred. See [TableIR design](../../../docs/TABLE_IR.md) for execution and storage details.
