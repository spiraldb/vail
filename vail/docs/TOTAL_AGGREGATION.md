# Total table aggregation

`Table.aggregate(expression)` evaluates a whole-table reduction through DataFusion.
Supported reducers are `count`, `sum`, `product`, `min`, `max`, `mean`, `collect`,
`count_where`, `fraction`, `any`, `all`, and `stats`. `agg.filter(predicate, body)`
and `agg.explode(callback, collection)` compose with these reducers; explode accepts
arrays and sets. Structs and scalar expressions can combine aggregate results and
globals.

The Python frontend constructs typed expression IR. A lazy aggregate GlobalsIR node
owns the input TableIR DAG and native global bindings. Lowering produces DataFusion
`AggregateExec` operators, with `FilterExec` and `UnnestExec` for aggregation scopes,
and singleton joins/projections for globals and the final expression. Custom Arrow
accumulators implement Hail's numeric, missing-value, and collection semantics.
There is no Python row evaluation or separate TableIR interpreter.

Ordinary `aggregate` is an action. With `_localize=False`, it returns an unindexed
expression; constructing it validates the plan without opening row data files.
Evaluation or use by another action executes the retained DAG.

Reducers sharing a filter/explode scope share an AggregateExec and input scan;
identical reducers are deduplicated. Different scopes currently create separate
scan branches. This preserves conditional evaluation, including avoiding invalid
expressions on filtered-out rows, but leaves scan fusion as future work.

Numeric and boolean reducers retain constant-size state. `collect` necessarily
retains its output in memory as Arrow chunks. It preserves input order and missing
members, including nested types. The current aggregate execution is single-stage;
distributed partial aggregation is not implemented.

Integer sum/product use wrapping int64 arithmetic. Numeric missing values are
ignored. Empty sum/product return 0/1; empty min/max are missing; empty mean and
stats standard deviation are NaN. Fraction counts missing predicates in its
denominator, so an empty input yields NaN and a nonempty all-missing input yields
0. Aggregate floating min/max ignore NaNs if finite values exist. Stats exposes
Hail's mean, stdev, min, max, n, and sum fields and uses its population-variance
formula.

Coverage includes multi-batch and multi-partition inputs, ordered scans of multiple
Vortex files, nested explode scopes, globals, lazy evaluation after construction,
large integers, missing/empty inputs, and native IR validation. The selected upstream
harness also runs the original test_aggregate1, test_aggregate_ir, and
test_aggregate_localize_false bodies.

Grouped aggregation is documented in GROUPED_AGGREGATION.md. The complete
public aggregator inventory, implementation libraries, and remaining compatibility
limits are documented in AGGREGATOR_INVENTORY.md. Scan aggregators are a separate API.
