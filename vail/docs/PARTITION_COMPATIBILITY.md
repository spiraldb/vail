# Hail Table partition compatibility

## Vail partitioning decision (2026-10-15)

**Accepted design decision; implementation pending.** Vail deliberately breaks
with Hail on partitioning. DataFusion/Vail chooses execution parallelism and
storage layout; preserving Hail's partition counts, boundaries, or membership
through ordinary transformations is not a compatibility goal.

We preserve:

1. **Pruning.** Push filters, interval filters, and other supported predicates
   through the planning stack to Vortex. Avoid reading files and segments that
   bounds/statistics prove cannot match. Keep exact residual filtering where
   pruning alone cannot decide row membership. Tests must check avoided reads
   directly, as well as returned rows, rather than asserting that a particular
   number of Hail partitions disappears.
2. **Partitioned table storage.** Tables continue to be written as sorted,
   key-range-disjoint files. DataFusion/Vail must have a parallelism strategy for
   producing that layout; the design is not to write one giant Vortex file.
   The exact strategy, file sizes, and number of files remain implementation
   choices, not Hail compatibility promises.

We explicitly stop supporting:

- `_n_partitions` on reads.
- `_filter_partitions` (keeping or dropping rows by physical partition ID).
- Writing a MatrixTable with explicit partition intervals (`_partitions`).

This records the user's decision, not completed implementation. Do not interpret
existing support, tests, or historical Hail observations below as requirements
to retain these APIs or reproduce Hail's layout. No implementation changes were
authorized as part of recording this decision. Other API details, including
`n_partitions()`, repartition/coalesce hints, and partition-stream operations,
require a separate decision; this entry does not silently remove or redefine them.

## Historical Hail compatibility research

The remainder records prior observations and implementation state. Where it
conflicts with the accepted decision above, the decision takes precedence.

Source baseline: the checked-out Hail commit `e10cc355bc8cd0de9c5f29d5182d31527898bae1`. Runtime results below were captured from that checkout with Python 3.12, Spark 4.1.2, OpenJDK 21, and `hl.init(master='local[2]')` on 2026-10-01. The active Spark backend reported `requires_lowering=False`. Hail's optimizer has a relevant special case: `Simplify.scala:1821` rewrites every `TableRepartition(TableRange(...), n, strategy)` to `TableRange(..., n)`, discarding the strategy. This explains why repartition and naive-coalesce on `TableRange` can increase partitions even though direct RVD operations and the API documentation say otherwise.

## Contracts from source and tests

| Operation | Contract evidenced by Hail source/tests |
|---|---|
| `repartition(n, shuffle=True)` | Python typechecks `n` as `int`. Direct IR uses `TableRepartition(..., SHUFFLE)`, implemented by `RVD.coalesce(..., shuffle=true)`. `n <= 0` fails with `cannot coalesce to nPartitions <= 0`. Shuffle preserves every row and duplicate multiplicity; keyed output partition ranges are calculated from key samples, so fewer than `n` output partitions can result when there are fewer distinct keys (captured below). Unkeyed output is built with the shuffled RDD partition count. Public docs describe the shuffle as full and approximately equal-sized. |
| `repartition(n, shuffle=False)` | Direct IR uses `COALESCE`; Hail's public doc says it combines partitions without a full shuffle and `n_partitions` cannot exceed the current count. The optimizer rewrite for a `TableRange` child can replace it with a range having exactly `n` partitions, regardless of strategy. On an unkeyed `TableParallelize` child, no such rewrite occurs: target 5 over 4 input partitions reports 4. |
| `naive_coalesce(max_partitions)` | Uses `NAIVE_COALESCE`. The Python doc says it combines adjacent partitions, does not rebalance, and does nothing when the input count is at most the maximum. Direct RVD returns itself for `maxPartitions >= current`, and the lowering implementation groups adjacent input partition contexts. However, the `Simplify.scala:1821` rewrite on a `TableRange` child discards strategy and requests exactly `n` range partitions. In the captured run, keyed `range_table(10, n_partitions=4).naive_coalesce(5/9)` reports 5/9; unkeyed `Table.parallelize(..., n_partitions=4).naive_coalesce(5/9)` reports 4/4 and retains the same partition contents. This is a Hail optimizer behavior limited to `TableRange`, not a backend-wide naive-coalesce contract. |
| `_filter_partitions(parts, keep=True)` | `parts` is typechecked as a sequence of integers. IDs are checked against `[0, n_partitions)` when lowered; an invalid ID fails when the table is executed. Keep mode selects those partitions; exclude mode selects the complement. Repeated IDs have set semantics for filtering, and selected partitions remain in original partition order. Partition count is the number selected, including empty selected partitions. The upstream test checks keep/exclude counts, union reconstructing the source, and preservation of rows from partition IDs 0 and 7. |
| `_map_partitions(f)` | `f` must return an expression of type `stream<struct>` (Python typecheck). The output struct must contain every input key field or Python raises `ValueError: Table._map_partitions must preserve key fields`. It is a per-partition stream transform, supports flatmap, and can emit no, one, or multiple rows per input row; each output partition is derived from its corresponding input partition. The upstream tests assert flatmap order/multiplicity, wrong return type, dropped-key failure, and indexed reads. |
| `_group_within_partitions(name, n)` | Implemented by `_map_partitions` with `part.grouped(n)`. It groups consecutive rows independently inside each partition, emits a final short group, and uses the first row of each group as the output row (therefore retaining that row's key fields); the array field contains the full original rows. It does not merge groups across partition boundaries. `n=0` fails at evaluation with `HailException: stream grouped: non-positive size: 0`. |
| `_calculate_new_partitions(n_partitions)` | Runs `TableCalculateNewPartitions` on a key-only table. Empty-key tables and keyed tables with no keys return an empty interval array. Otherwise, the runtime samples keys, groups equal keys, and chooses at most the requested number of ranges; duplicate keys are not split across a boundary. The ranges are start-inclusive. Hail uses its ceil-step endpoint rule: most interior upper bounds are exclusive, but terminal ranges can overlap (for two distinct keys it returns closed `[1,2]` and `[2,2]`). A zero request returns `[]`. These ranges are consumed by `read_table` to select intervals. |
| `read_table(path, _n_partitions=n)` | Rejects simultaneous `_intervals` and `_n_partitions` with `ValueError`. The implementation applies the partition calculation only when `_n_partitions` is truthy; with a positive value it rereads using the calculated intervals. An upstream test checks a 444-row table requested at 50 partitions and expects 50. A table with one distinct/missing key requested at 10 expects one partition. The empty-key interval case returns no calculated bounds; do not infer an output partition count for every empty keyed dataset without checking the reader path. |
| `GroupedTable.partition_hint(n)` | Assigns `_npartitions` without validation and returns the same `GroupedTable`. `aggregate` passes that value to `TableKeyByAndAggregate`; absent a hint it passes `None`, which uses the input's partition count. The hint controls requested aggregate output partitioning, subject to how many group keys exist. |

## Captured Hail oracle observations

Reproduce the isolated keyed-range, unkeyed-parallelize, empty-shuffle, and partition-filter checks with [`scripts/check_hail_partitions.py`](../scripts/check_hail_partitions.py) using the command in that script's header. It prints each repartition IR before forcing actions and reports `requires_lowering` for the active backend.

The direct oracle was run with:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@21 \
SPARK_LOCAL_IP=127.0.0.1 \
PYTHONPATH=/Users/danielking/projects/vail/hail/hail/python \
/private/tmp/vail-hail-oracle/bin/python - <<'PY'
import hail as hl
hl.init(quiet=True, master='local[2]')
t = hl.utils.range_table(10, n_partitions=4)
for name, x in [('base', t), ('shuffle7', t.repartition(7)),
                ('coalesce2', t.repartition(2, shuffle=False)),
                ('coalesce5', t.repartition(5, shuffle=False)),
                ('naive2', t.naive_coalesce(2)), ('naive9', t.naive_coalesce(9)),
                ('filter', t._filter_partitions([3, 0, 3]))]:
    print(name, x.n_partitions(), x.idx.collect())
hl.stop()
PY
```

Observed results:

- `range_table(10, n_partitions=4)` reports 4 initially. Naive target 2 gives 2 partitions, with `[[0,1,2,3,4],[5,6,7,8,9]]`; target 4 retains `[[0,1,2],[3,4,5],[6,7],[8,9]]`; targets 5 and 9 report exactly 5 and 9, with `[[0,1],[2,3],[4,5],[6,7],[8,9]]` and `[[0,1],[2],[3],[4],[5],[6],[7],[8],[9]]`. Pre-action IR is `TableRepartition(..., n, NAIVE_COALESCE, TableRange(10,4))`; Hail's `Simplify.scala:1821` rewrites this to `TableRange(10,n)`.
- An unkeyed `Table.parallelize` of the same ten rows over four partitions reports 2/4/4/4 after naive targets 2/4/5/9. Targets 5 and 9 preserve the original four partition contents. `repartition(shuffle=False)` target 5 likewise reports 4. The backend reports `requires_lowering=False`.
- On the `TableRange` input, `repartition(shuffle=True)` target 7 reports 7; `repartition(shuffle=False)` targets 2 and 5 report 2 and 5. The latter 5 is caused by the same optimizer rewrite that ignores strategy for `TableRange`, not the Python lowering-only checkpoint path.
- `_filter_partitions([3, 0, 3])`: 2 partitions, rows `[0, 1, 2, 8, 9]`. A duplicate selected ID does not duplicate its rows.
- Invalid shuffle targets `0` and `-1` raise Hail `FatalError` wrapping `IllegalArgumentException: requirement failed`. `_filter_partitions([4])` on four partitions raises `FatalError` with `_filter_partitions: no partition with index 4 (num partitions = 4)`. The group-size-zero error occurs only when evaluating the result.
- A six-row keyed table with five copies of key `1` and one row at key `2` reports two partitions; `_calculate_new_partitions(10)` returns `[1,2]` and `[2,2]` intervals, and `repartition(10)` still reports two partitions while retaining all six rows in original key order.
- `_calculate_new_partitions(2)` on `range_table(10, n_partitions=4)` returns `[0,5)` and `[5,9]`: start-inclusive, interior end-exclusive, final end-inclusive. On an empty keyed range table the method itself returns `[]` (it is already a Python list; passing that empty list to `hl.eval` is a type error).
- Shuffling an empty keyed `range_table(0, n_partitions=4)` to target 1 or 4 reports one partition with no rows.
- Filtering the four-partition range table with `[]` returns zero partitions; `range(3)` returns three partitions and rows `0..7`; `[1,1]` returns one partition and rows `[3,4,5]`. Invalid index 4 errors in both keep and exclude modes.
- `_filter_partitions([4], keep=True)` and `_filter_partitions([4], keep=False)` both fail at execution for a 4-partition table with the same out-of-range partition error. Exclude mode does not silently ignore invalid IDs.
- For ten ordered rows in four partitions, `_group_within_partitions('g', 3)` returns rows keyed by first elements `0, 3, 6, 8` with groups `[0,1,2]`, `[3,4,5]`, `[6,7]`, `[8,9]`.
- Group-by aggregation over two distinct keys reported two output partitions with the default hint, hint `1`, and hint `7`; each result retained both groups. This demonstrates that a hint is a target, not a guarantee to manufacture partitions beyond the output groups in this run.

The Hail test source gives additional assertion-level evidence: `hail/hail/python/test/hail/table/test_table.py` tests `_filter_partitions` at 1169, no-shuffle/naive coalescing on an unkeyed table at 1734, grouping at 1944, map-partitions typing and flatmap at 2010, `_n_partitions` at 2063-2070, and indexed intervals at 2081. The grouped API path is in `hail/hail/python/hail/table.py` at `GroupedTable.partition_hint` and `GroupedTable.aggregate`; `read_table` is in `hail/hail/python/hail/methods/impex.py`.

Normalized machine-readable results for regression comparisons are saved in [partition_oracle_observations.json](partition_oracle_observations.json).

## Runtime source pointers

- `hail/hail/python/hail/table.py`: Python public methods and typechecks (`repartition` 3159, `naive_coalesce` 3229, `_filter_partitions` 4577, `_group_within_partitions` 4645, `_map_partitions` 4654, `_calculate_new_partitions` 4668).
- `hail/hail/python/hail/methods/impex.py`: `read_table` at 3272; interval/partition argument conflict and truthy `_n_partitions` branch.
- `hail/hail/hail/src/is/hail/expr/ir/TableValue.scala`: runtime `TableValue.repartition` dispatch.
- `hail/hail/hail/src/is/hail/expr/ir/lowering/LowerTableIR.scala`: naive coalesce around 1731; map-partitions around 1766; filter-partitions around 1776; calculation of new ranges around 687.
- `hail/hail/hail/src/is/hail/expr/ir/lowering/Simplify.scala:1821`: `tableSubsetRules` rewrites `TableRepartition` on a `TableRange` child to `TableRange(nRows, nParts)` without checking strategy; this accounts for the increased range-table partition counts.
- `hail/hail/hail/src/is/hail/rvd/RVD.scala`: `naiveCoalesce` at 305, `coalesce` at 330, and `calculateKeyRanges` at 1310.
- `hail/hail/hail/src/is/hail/methods/FilterPartitions.scala`: concrete partition subsetting and keep/exclude behavior.
- `hail/hail/hail/src/is/hail/expr/ir/functions/TableCalculateNewPartitions.scala`: empty-key and empty-key-data behavior, plus key-range calculation.

## Vail execution and remaining differences

Vail partitions are independently executable DataFusion child plans, exposed
through `TablePartitionsExec`. Partition-local transforms and reads execute
only the selected child. Shuffle uses DataFusion range redistribution with
Vail canonical keys; adjacent coalescing preserves input order. Rust regressions
execute partitions out of order and successfully read one partition after
unrelated data files have been removed.

Partition-bound calculation follows Hail's distinct-key ceil-step endpoint
rule, including overlapping terminal endpoints. Vail currently scans all keys
with bounded retained metadata rather than reproducing Hail's randomized
reservoir sample. Consequently large-table sampled boundary values need not be
identical, although key ordering and duplicate-key boundary constraints hold.
