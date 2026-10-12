# Keyed table lookup

```python
people = people.key_by("person_id")
visits = visits.annotate(age=people[visits.person_id].age)
```

`Table[key_expression]` and `Table.index(*key_expressions)` return an expression containing the target's non-key fields. Composite keys accept separate expressions, or a struct/tuple expression containing all key components. Key types must match exactly. The expressions must belong to the querying table's row context.

A missing match produces a missing struct. A matched row whose fields are all missing remains a present struct. Missing top-level key components never match; missing values inside a present compound key use Vail's normal key comparison. Duplicate target keys select one representative without multiplying querying rows. Target globals are not merged into the querying table.

Lookup expressions capture the target's lazy native TableIR. Expression resolution inserts a `TableLookup` dependency before projection, filtering, sorting, or aggregation. No rows are read until an action executes the plan. Repeated uses of the same lookup expression share its dependency.

Each querying partition lowers to a DataFusion sort-merge left join using the shared canonical key encoding. DataFusion sorts probe keys and restores original row order afterward. The target uses its existing key ordering, with no additional target sort. A bounded DataFusion window operator streams through duplicate target keys and retains one row per key. The result preserves querying row count, order, globals, and independent partitions.

Currently each querying partition replays the ordered target plan. Planning a covering of target fragments from probe key intervals remains an optimization; this implementation does not yet prune target fragments by probe bounds.

`all_matches=True`, point containment lookup into interval-keyed tables, and lookup correlated with collection-lambda variables remain unsupported. Exact interval-valued key lookup works. Scalar/global-only indexing is rejected, as in Hail.

Regression tests cover missing and duplicate keys, compound keys, nested lookups, captured globals, transformations and aggregations, IEEE float keys, loci and intervals, lazy reads across multiple Vortex files, and write/read round trips. Native plan tests assert sort-merge execution, bounded target duplicate reduction, and absence of a target sort or hash join.
