# Matrix column layout and execution

The ordered `MatrixIR.column_records` array is authoritative. For every row,
`len(_CELLS) == len(column_records)`. Position `j` associates the entry in
`_CELLS[j].value` with `column_records[j]`; cells contain no column IDs. A null
entry struct represents absence, while a defined entry struct with null fields
remains present. Arrow/Vortex struct validity carries this distinction, without
an explicit presence flag. Public column keys can repeat; `_CID` occurs only in
shared column metadata and identifies a column occurrence. Those identifiers
must be unique within a matrix but need not be contiguous or sorted.

The public keyed `mt.cols()` Table may sort by column keys, as Hail does. Its
presentation order is not the physical slot order. `localize_entries` exports
columns in the physical order corresponding to the entry arrays.

## Transformations

`_copy` only replaces structural components. It never searches entry lists.
Column annotation replaces metadata in the same order. Column filtering computes
one Boolean mask on the column relation and applies that mask to both metadata
and every row, through DataFusion `arrays_zip`, `array_filter`, and
`array_transform`. Missing predicates are false, including for `keep=False`.

Column selection, permutation, head/tail, semi/anti joins, deduplication, and
column explosion compute positions once and gather the same positions from
metadata and row slots. Repeated selection creates new occurrence identities;
only shared metadata receives new identities. Cells retain their positional
alignment. Joins and deduplication restore
source position order before gathering.

DataFusion 55 has scalar array extraction, but no batched per-row gather with an
array of indices. `__vail_list_take` is a narrow DataFusion scalar UDF that builds
flat Arrow indices and calls Arrow's `take` kernel. It does not decode values or
run a Python/per-value scalar evaluator. It handles sliced lists, repeated
positions, empty structs, and empty selections, and rejects invalid indices.

Entry annotation, entry views, and row aggregation zip cells with columns before
entering a collection lambda. Each callback reads its current cell and column.
Capturing an entire column or cell list inside a lambda makes DataFusion repeat
that list for each element, turning linear work into quadratic allocation.

Grouped-row aggregation explicitly orders the final collected array by its
original element position. A preceding Sort alone is insufficient: the
DataFusion optimizer needs the aggregate's ordering requirement to preserve it.
This also preserves positions whose aggregate has no present input entries.

## Validation

The MatrixIR constructor checks Arrow-compatible schemas immediately. Untrusted
layouts (including unlocalization) also validate shared metadata identities,
slot lengths, and defined owning slots lazily. Nullable entry structs are valid. Boundary
checks use Arrow kernels and remain observable to count actions. Internally
constructed layouts preserve the invariant by construction and do not stack
redundant validators at every transformation.

The row-local layout validator permits interval-read pushdown, so checking a
matrix does not force unrelated source files to be read. Other filters, including
scan-dependent filters, retain their existing planning rules.

## Persistence

New matrix snapshots use descriptor version 4 and store positional cells with
only the `value` field. Version 3 snapshots remain readable: their repeated cell
IDs are dropped by a projection at the read boundary. Shared column metadata
and entry struct validity are preserved.

## Verification

`python/tests/test_matrix_layout.py` exercises chained masks and permutations,
duplicate public keys and repeated selections, filtered slots, column metadata,
semi/anti joins, deduplication, row unions, repartitioning, column explosion,
localization, storage round trips, malformed external layouts, and grouped-row
position ordering. Existing Matrix tests cover column unions, indexed reads,
grouping, and complex conversion chains. Rust unit tests cover the Arrow gather
and boundary checks directly.

The cloud benchmark harness accepts `--samples-8x` to add 80 samples to the
10/20/40 sample series. Cloud measurements must compare optimized release builds
and stock PyPI Hail on the same normal 32-vCPU VM with the existing 10 Hz CPU,
RSS, I/O, and hardware-counter collector. The current source upload was blocked
by automatic approval review; no new cloud performance results are claimed.

Local before/after diagnostics at 10/20/40/80 samples and 1×/2×/4× rows are
recorded in [the local report](../../conformance/runs/20261014-matrix-layout-local/SUMMARY.md).
At 80 samples, entry sum improved 5.96× and the row-count column-filter probe
improved 135×. These are short local release-build diagnostics, not Hail/cloud
comparisons; the filter probe benefits from pruning unused entry arrays.
Verification passed 1,043 Python tests, 19 subtests, and 123 Rust tests; one Python
test was skipped.
