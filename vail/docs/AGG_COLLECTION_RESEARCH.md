# Collection aggregate implementation notes

This note records the upstream behavior and reusable DataFusion code inspected
for `collect_as_set`, `counter`, `take`, and `array_sum`.

## Hail behavior

The reference implementation is `hail/hail/python/hail/expr/aggregators/aggregators.py`.
`collect_as_set` returns the distinct input values, including a missing value
as a set member when present. `counter` is expressed in Hail itself as
`group_by(expr, count())`, or `group_by(expr, sum(weight))`; therefore weighted
counts follow Hail `sum` typing and missing-value behavior. `take` returns at
most `n` values. With ordering, missing ordering keys are last; without it,
order is unspecified. `array_sum` is implemented as `array_agg(sum, expr)`, so
coordinates are summed independently and arrays must have compatible lengths.

## DataFusion reuse audit

DataFusion 55.0.0's `datafusion-functions-aggregate` crate contains
`DistinctArrayAggAccumulator` in `src/array_agg.rs`. Its `RowConverter`
provides nested Arrow row encoding, equality, and hashing; this is preferable
to converting values to JSON or maintaining a custom value hash. The crate
also has `ArrayAggAccumulator` with optional sort options. The project's
execution crate did not previously list this crate as a direct dependency;
the parent integration adds it at the workspace's exact 55.0.0 version.

DataFusion's `ScalarValue` is hashable and DataFusion's count aggregate uses a
`HashMap<ScalarValue, _>`, but retaining Arrow arrays and using the array
aggregate's row converter avoids per-value scalar conversion for collection.
Counter should continue to use the relational `group_by` and sum/count path
where available, in keeping with Hail's own definition.

The Arrow wire representation for sets uses the existing
`__vail_set_item` list-field marker. Dictionary values use a list of structs
with `__vail_dict_entry` marker and `key` / `value` fields. Arrow arrays remain
the execution representation throughout; JSON is only the outer IR/wire
encoding.
