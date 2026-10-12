# Grouped table aggregation

`table.group_by(*keys, **named_keys).aggregate(**fields)` constructs a lazy
TableIR aggregation node. Positional keys are source field names/references;
named keys can be computed expressions. The output retains globals, is keyed by
the grouping fields, and is sorted by those keys. Empty aggregate field lists
produce the distinct key domain. No grouping keys produces one group for nonempty
input and no groups for empty input.

The Rust planner lowers directly to DataFusion physical operators:

- `ProjectionExec` evaluates private grouping columns in the original row scope.
- `AggregateExec` owns hash grouping and accumulator state. Reducers in the same
  filter/explode scope share one scan and aggregate operator.
- Scoped `FilterExec` and `UnnestExec` feed other aggregate branches. Null-safe
  `HashJoinExec` left joins attach those results to the complete key domain.
  Groups with no contributing rows get the existing UDAFs' empty-input values.
- Singleton globals joins and projections construct result fields.
- DataFusion `SortExec` orders the resulting groups by Hail table keys.

There is no Python grouping, custom hash map, or TableIR row interpreter. The
existing Hail-specific UDAFs supply reducer semantics; DataFusion owns grouping.
Construction validates types/plans without opening data files. Collect, count,
write, and downstream transformations execute the retained input DAG.

Supported reducers and combinators are those documented in TOTAL_AGGREGATION.md.
Different aggregation scopes can rescan the source; branch fusion remains future
work. Execution currently uses one physical partition and single-stage aggregate
operators, not distributed partial aggregation. `collect` retains its result
values in memory.

Grouping keys use Vail's canonical Hail key encoding in DataFusion's hash
aggregate and joins. The same encoding orders output rows and fragment bounds.
It handles missing values, floats (including signed zero and canonical NaNs),
Unicode strings, arrays, tuples, structs, sets, dictionaries, calls, and the
currently supported ndarray representation. Set members and dictionary entries
are sorted before encoding, so their iteration order does not affect grouping.
Partition hints remain unimplemented. All 33 public Hail aggregators are exposed;
see AGGREGATOR_INVENTORY.md for their implementation and compatibility limits.

Tests cover computed and composite keys, missing keys, empty/no-key input,
empty filtered/exploded groups and identities, captured/local globals, key name
shadowing, chained grouping, multi-batch/multi-file scans, lazy file access,
write/read round trips, and DataFusion physical plan structure. Three additional
original upstream cases exercise grouped means, empty result field lists, and
global collision validation. The grouped-mean upstream test also required Hail's
hash behavior for localized Struct values.
