# Globals and TableIR lowering audit

Recorded 2026-09-25. Source drift was not detected. Commands, full logs, source
hashes, and native extension fingerprints are preserved in this directory.

| Suite | Passed | Remaining |
| --- | ---: | --- |
| Python and harness | 120 tests + 19 subtests | 0 failures |
| Rust workspace | 41 tests | 0 failures |
| Recorder self-tests | 9 tests | 0 failures |
| Native conformance | 4 fixtures | 0 unsupported or failed |
| Selected original Hail cases | 94 cases | 1 failure: expression `.map` |
| Separate Hail oracle | 0 fixtures | 4 unavailable: missing `hailtop.version` |

The recorder exits nonzero for the known upstream failure. The selected Hail cases
are unchanged source-extracted bodies, not the full Hail test inventory. Native
fixtures remain source-derived expectations, not a substitute for the unavailable
Hail oracle.

## Globals completed in this update

`Table.index_globals()` now returns an unindexed expression backed by a native,
immutable `Arc<GlobalsIR>`. It can be used in another Table's annotations, filters,
global projections, constructor globals, and `hl.eval`. Expression composition
preserves native bindings separately from the wire expression JSON. No values are
collected in Python and no global registry retains source Tables.

The destination IR captures the singleton relation and its checked type. Lowering
builds DataFusion singleton struct projections and cross joins; explicit column
indices keep multiple source globals separate even if their field names coincide.
These references remain lazy, pin old snapshots across overwrite, and survive the
original Python Table being freed. The Rust test verifies that captured globals
share their original IR but do not retain the source Table's Arrow row buffers.

The original Hail `test_index_globals` now runs unchanged and passes. Additional
tests cover multiple sources, nested expression composition, storage, empty globals,
missing source files, deferred failures, immutable bindings, and malformed native
references. Existing annotate/select/transmute globals, row references, Arrow IPC
persistence, and `_same` coverage remain green. Globals still use the shared Vail
type system; adding further Hail types remains a shared type/expression task.

## Execution audit

Every implemented TableIR node lowers through `vail-table/src/plan.rs`. There is no
TableIR interpreter fallback. Row/global projections, filters, limits, sorts,
forced count, and `_same` use DataFusion physical operators. Globals are Arrow
singleton relations. External globals lower to cross joins, and `_same` lowers to
DataFusion full outer joins and Hail-compatible scalar comparison expressions.

Hail-specific source scans, ordinal assignment, key-domain validation, and bounded
tail are DataFusion ExecutionPlan extensions. They consume physical child plans,
not TableIR nodes. Collection and the bounded Viceberg writer consume the resulting
DataFusion batch streams. Exact metadata-only counts remain metadata optimizations.

Regression checks verify physical plan shapes, a spilling million-row sort, and
certified scan ordering without a sort on read. The embedded executor still uses
one ordered physical stream while preserving logical Hail partition identities;
parallel execution, distributed sort, and a logical optimizer pipeline remain
future work. This does not claim every Hail Table API operation is implemented.
