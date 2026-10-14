# Query-path loop complexity audit

This is a static audit of Vail-owned Table/Matrix lowering, collection operations,
partition planning, storage key handling, and reference-coordinate operations.
It does not claim an exhaustive audit of dependency internals or every numerical
routine. The previous Matrix patch removed the ordinary annotation/filter/view
hot paths; it did **not** remove all search-inside-map implementations.

Let R be rows, C columns, E entries in a sparse row, N collection elements,
U distinct keys, P partitions, F files, I query intervals, and K actual overlaps.
Costs below describe the frontend algorithm; DataFusion lambda capture can add
allocation costs by repeating an entire captured array for each output element.

## Implementation progress (2026-10-14)

The table below records the original findings; its descriptions are not a claim
that every path still uses the original algorithm.

All original findings have an implementation. The final release wheel passed
**1,056 Python tests**, with one skip and 19 subtests. Release Rust validation
passed 76 execution, 18 storage, and 38 Table library tests, plus nine round-trip
and five streaming-write integrations. The additional fold-pruning unit test
was run after the 75-test execution suite.

- Matrix grouping uses position gathers; sparse reconstruction uses direct
  scatter indices. Copies delegate to Arrow take.
- Dictionary construction and collection grouping/lookup use DataFusion
  GroupValues. Scalar dictionaries are indexed once for the batch of queries.
- Nested set binary operations group canonical values with DataFusion.
- Fold callbacks run across active row lanes and gather only referenced columns.
- Storage reads binary-search cached Arrow comparators; writer certification
  caches comparators and resolves key columns once.
- Reference metadata is prepared once per expression. Contig and classification
  lookups are indexed; PAR membership uses rangemap RangeSet.
- Join cell candidates use endpoint events; membership and prefix reads use
  intervaltree indexes over Arrow-ordered endpoint ranks. Interval unions are
  normalized before intersections are swept.
- Matrix array aggregation transposes with Arrow take and projects only body
  context fields, avoiding repeated captures of the original arrays.

Capturing a large row dictionary inside a higher-order expression can still
replicate its physical input before the lookup kernel. This remains a fusion
and allocation concern; the lookup itself no longer filters and allocates a
match list per key. This audit is not a proof that every dependency or arbitrary
user expression has optimal complexity.


## Original findings and replacements

| Priority | Path | Existing work | Replacement |
|---|---|---|---|
| P1 | `matrixtable.py`: column branch of `GroupedMatrixTable.result` | Each group filters every row's contexts, and each membership test scans that group's members. O(R × C²) worst case. Metadata also indexes the whole groups array from a range lambda. | Collect **source positions**, gather each group's cells once, then aggregate its selected cells. Prefer DataFusion explode/group/collect with ordinals or a batched Arrow gather of nested index lists. Zip groups with selected cell arrays outside lambdas. |
| P1 | `matrixtable.py`: `to_matrix_table` | Each dense output column filters that row's sparse entry array by ID. O(R × C × E); captures repeat sparse entries across columns. Column metadata construction also captures the whole column array inside an indexed map. | Sort coordinates once by column position; construct dense slots by merging positions or scattering with Arrow indices. Preserve last-duplicate-coordinate resolution. Metadata construction should use `_indexed_cells(column_values)` rather than captured indexed access. |
| P1 | `collections.py`: `_dict_from_entries` | Distinct keys are computed, then each key filters the original entries. O(N × U), plus sorting distinct keys; captures repeat N entries U times. | DataFusion UNNEST + grouping by owner/key, selecting the first or last original ordinal according to the caller, then ordered collect. Alternatively a library-backed per-list grouped kernel. Preserve missing-key equality and the representative key/value chosen by duplicate resolution. |
| P1 | `array_methods.py`: `group_by` | Each distinct key rescans all pairs, O(N × U), then dictionary construction performs another deduplication. | Group once using DataFusion, carrying input ordinals so each group's values retain their required order. Avoid deduplicating an already unique grouped result. |
| P1 | `collections.py`: dictionary `get`/`contains`, set `contains` | Full-list filtering for each lookup, including allocating a result list. Repeated lookups cost O(lookups × N). | Build a reusable index, or use a canonical sorted representation and binary search. A single map lookup should not construct an array of every matching entry. DataFusion's own map extraction must be checked for complexity; merely renaming the kernel is insufficient. |
| P1 | `collections.py`: nested-element `set_binary` fallback | Intersection/difference/union perform membership scans of the opposite set for each element: O(A × B). Ordinary supported element types already use DF `array_intersect`/`array_except`/`array_union`. | Use DataFusion grouping/joins or a library-supported hash/sort implementation for supported representations. For unsupported equality/key types, explicitly reject or extend the representation; do not hide a quadratic fallback. |
| P1 | `matrix_list_aggregates.py`: `agg_group_by` | Each unique key filters all row contexts: O(C × U), followed by the expensive dictionary constructor. | Explode once with row identity and group key, run DataFusion grouping, then reconstruct ordered per-row results. |
| P1 | `join_interval.rs`: `cells`, `clipped_parts`, `clipped_bounds_parts` | Boundaries are insertion-sorted: O(P²). Every output interval scans all input partitions; membership joins scan right bounds for every left partition. Multi-way joins repeat this for every input. | Sort endpoint metadata with Arrow/native typed comparators, then sweep events with an active partition set, or build an interval index. Target O(P log P + K), with output-sensitive overlap work. Key-prefix bounds may overlap, so a simple cursor assuming disjoint key intervals is insufficient. |
| P1 | `interval_read.rs`: partition/interval candidate matching; `provider.rs`: `visit_bound` and `intersect_intervals` | Candidate matching checks every partition against every interval, O(P × I). Predicate intersections form Cartesian products of interval lists, O(A × B), potentially multiplying intermediate interval counts across nested AND/OR expressions. | Normalize/sort interval unions, then sweep intersections and partition overlaps. Preserve open/closed endpoints, unbounded endpoints, prefix keys, and requested interval partition order. |
| P1 | `vail-storage/src/lib.rs`: `filter_batch_range` | Scans every row of a certified sorted batch to discover a contiguous slice. For each row, it builds sliced key arrays and recreates typed comparison machinery. | Cache key column positions/comparators once per batch and binary-search both endpoints, then `RecordBatch::slice`. O(log batch_rows) comparisons instead of O(batch_rows). |
| P1 | `vail-storage/src/lib.rs`: `certify_file`, `typed_key_for_row` | Sortedness validation visits every row (necessary), but allocates sliced key arrays/rebuilds comparators and searches schema field IDs for each row/key. | Resolve key column indices once; use cached Arrow comparators for adjacent rows or vectorized adjacent-slice comparisons. Only materialize boundary keys once per batch/file. Keep O(R × key_arity), with no per-row schema search or key-array allocation. |
| P2 | `reference.rs`: `ordinal` / `validate_coordinate`; `expressions.rs`: locus conversion from global position | Contig names are linearly searched per locus. Validation repeats that search despite having the ordinal. Global-position conversion walks contig lengths per value. | Contig-to-ordinal map initialized once; validate known ordinal with direct `contigs[ordinal]`; cumulative contig endpoints with binary search for global positions. |

## Additional allocation findings (addressed)

`matrix_list_aggregates.py`'s `agg_array` maps over output positions and captures
all input contexts for each position, then indexes each input array. The intended
work is proportional to the total input elements; DF capture can repeat array
payloads across output positions. This needs a dedicated profile and an
explode-by-position/group lowering, rather than treating an outer array map as
an efficient implementation of indexed access.

`array_fold.rs::evaluate` has a separate constant-factor problem: an arbitrary
fold is necessarily sequential within each array, but the current implementation
creates and evaluates a one-row RecordBatch for every element, slicing all outer
columns each time. This is linear in input elements, not a search/indexing bug.
Recognized reductions should use native DataFusion/Arrow reductions; generic
callbacks should evaluate batches of active row lanes per step and retain only
referenced outer columns, rather than dispatching a scalar-sized batch per item.

## Removed in this audit

- `_lower_cell` now requires the already aligned column annotation. The unused
  fallback `columns.filter(column_id == cell_id)[0]` is gone, so a future caller
  cannot accidentally reactivate the full-column search. Entry lookup still
  receives its exact owning position.
- `LocalRead.partition_key_bounds` sweeps ordered physical row ranges rather than
  testing every file against every partition. It visits only actual overlaps.
- `LocalRead.candidate_partitions` advances one file cursor over ordered selected
  row ranges. It no longer scans every selected file for every partition and
  excludes zero-length partition ranges. Both storage paths are O(P + F + K).

## Appropriate loops / existing good paths

- Creating or gathering each Matrix slot once is O(number of output entries).
  A rows-by-columns loop is appropriate when it emits the actual rectangular
  output; it is not equivalent to searching all columns for each output column.
- `matrix_arrays.rs` walks list offsets and gather indices once and delegates
  value copying to Arrow `take`; it does not search for entry values.
- `FragmentIndex.select` already binary-searches the lower fragment bound and
  stops at the upper bound: O(log F + selected fragments).
- Snapshot identity/scan-task matching already uses hash maps keyed by file path.
- Ordinary numeric/string set operations already call native DF set kernels.
- Join output can necessarily be Cartesian for duplicate keys; that output cost
  must be distinguished from repeatedly searching partition metadata.
- Small schema loops executed once at planning/startup are lower priority than
  searches repeated for every row, cell, or dictionary key.

## Implementation order (completed)

1. Grouped columns and sparse coordinate-to-Matrix reconstruction.
2. Dictionary construction, array grouping, and repeated collection lookup.
3. Join/interval metadata sweeps and indexed-read batch endpoint searches.
4. Writer certification allocation, then reference coordinate indexing.

Correctness regressions must include duplicates, missing values/entries, empty
collections and partitions, unordered/repeated column selections, key prefixes,
open/closed endpoints, overlapping partition bounds, and multi-file round trips.
Scaling checks should vary the searched dimension independently of output size,
record instructions and memory, and verify that captured arrays are not repeated
per output element. Calling a DataFusion expression does not make a quadratic
frontend algorithm acceptable.

## Initial small-fix validation at 92f3d4f

The updated release wheel passed all 66 Matrix tests (including 12 layout cases).
The Rust join and physical-partition suites passed all 10 tests, covering metadata
bounds, interval pruning, unrelated file exclusion, and join results. That initial validation preceded the full fixes and validation recorded above.
