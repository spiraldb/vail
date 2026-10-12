# Lazy aggregate initialization from globals

Histogram start/end/bin count, requested quantiles, call_stats allele count,
and fold initial state are expression IR. Python validates that they do not
reference row fields or free aggregation variables; expressions may contain
local or captured globals and lexically bound array lambdas. Native typing
independently validates them against an empty row schema and the globals schema.

At construction, typed placeholders allow the ordinary DataFusion lowering to
validate the output schema without evaluating the parameters. The placeholders
are never executed over row data. At action time, InitializeAggregateExec:

1. Executes a DataFusion ProjectionExec over the singleton globals relation.
   Captured globals may themselves be lazy aggregate relations.
2. Requires exactly one initialization row, then binds its Arrow ScalarValues.
   Fold states remain Arrow values; numeric library configuration is passed to
   the existing UDAF factories.
3. Lowers the bound aggregation to the normal DataFusion plan and forwards its
   output stream. It does not collect or interpret table rows.

Binding occurs before creating the accumulators because library constructors
need parameters such as histogram bin count. The initialization relation is
independent of row scans, filters and aggregate scopes. Empty inputs therefore
retain the configured histogram edges, fold initial state and allele count.
Each action initializes again; no values are cached during query construction.

Hail's expected behavior is derived from the checked-out upstream sources:

- `hail/expr/aggregators/aggregators.py`: hist and call_stats use init_op_args;
  quantiles accepts a scalar or array expression; fold retains initial IR.
- `hail/expr/functions.py:_quantile_from_cdf`: rank is floor(q * n), giving 2
  for the median of [0, 1, 2, 3]. Missing requested quantiles remain missing.
- `CallStatsAggregator.scala`: missing n_alleles is an error; when AN is zero,
  AC and homozygote_count retain their initialized length and AF is missing.

Tests in `python/tests/test_aggregate_global_initialization.py` and
`python/tests/test_aggregate_initialization_lazy.py` assert the correct results,
including empty, filtered and grouped input, captured globals, missing values,
and globals computed by another lazy aggregate. Deleting globals.arrow before
construction and restoring it before evaluation verifies lazy file access;
deleting it again verifies that a later action executes the dependency again.
Rust operator tests cover invalid singleton cardinality, runtime schema mismatch,
and forwarding multiple output batches. These are source-derived tests; a live
Hail oracle is still unavailable in this checkout.
