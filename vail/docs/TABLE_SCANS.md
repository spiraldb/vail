# Ordered Table scans

`hl.scan` yields the aggregation over rows strictly before the current row, in
Table order. Expressions remain lazy until an action executes their TableIR.

The serial lowering uses DataFusion windows with the frame `ROWS BETWEEN
UNBOUNDED PRECEDING AND 1 PRECEDING`. The parallel lowering uses three stages:

1. DataFusion partitioned windows compute each original execution partition's
   local prefix accumulator states.
2. A DataFusion aggregate computes one complete state per nonempty partition.
   An exclusive window over these summaries merges states from preceding
   partitions, ordered by the original partition identifier.
3. A DataFusion join attaches this carry state to each partition's rows. A
   generic scalar adapter merges carry then local state and calls the existing
   accumulator's `evaluate` method.

The adapters expose existing DataFusion accumulator `state` and `merge_batch`
methods; they do not implement new sum, mean, collect, or other reducer
algorithms. In particular, mean carries sum/count, not a mean of partition means.
Ordered collect merges earlier rows first and preserves missing elements.

Only the summary window is global. The original physical execution partitions,
including empty partitions, are restored using DataFusion range repartitioning.
Private partition/ordinal fields are execution metadata and are removed from
user-visible rows. The planner recognizes this requirement structurally through
its logical partition restoration node, not through printed plan text.

Floating-point parallel reduction can differ in its last bits from sequential
reduction because accumulator states are combined in a different grouping.
The reducer's existing numerical behavior and known limitations still apply.

Scan expressions are rejected in total and grouped aggregation contexts; they
must not silently become ordinary aggregators. Reducers requiring initializers
and aggregation combinators have separate support requirements, described by
explicit errors where unsupported.

## Supported scan reducers and combinators

Supported reducers are `count`, `count_where`, `fraction`, `any`, `all`, `sum`,
`product`, `min`, `max`, `mean`, `collect`, `take`, `collect_as_set`, `corr`, and
`ndarray_sum`. `stats` composes the supported scalar reducers. `take` preserves
row order or its explicit ordering argument.

`scan.filter` attaches DataFusion aggregate filters to both local windows and
partition summaries. Arguments are additionally guarded by a typed `CASE`, so
excluded rows cannot fail casts or other argument evaluation. Missing predicates
exclude rows; included missing values remain in `collect`. Nested filters
combine their predicates.

Initializer reducers (`hist`, approximate quantiles/median), reducers requiring
additional scan type inference (`linreg`, `downsample`, genomic reducers),
`scan.explode`, grouped/counter scans, array combinators, and fold are not yet
supported. Unsupported forms raise explicit errors rather than silently running
as total aggregations.
