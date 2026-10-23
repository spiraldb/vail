# Ordering contracts and unordered consumers

`output_ordering` is an available property. `required_input_ordering` is a
semantic demand: changing that input order can change an operator's answer.
`maintains_input_order` only says how a provided order flows through a node;
it does not by itself make that ordering necessary.

The action-local pass in `ordering_requirements.rs` starts with an explicit
unordered consumer, rather than treating an available certificate as demand.
It removes fetch-free column-key sorts and ordered merges, preserving partition
counts and the execution DAG. A global sort becomes a coalesce. Local sorts
retain their source partitions. Top-k sorts and limits remain intact.

The ordinary physical optimizer also invokes this pass below proven built-in
count aggregates. This is deliberately based on the concrete built-in Count
implementation, with no grouping/filter and only column/literal arguments.
Unknown user aggregates are not presumed commutative. In DataFusion 55.0.0,
`AggregateFunctionExpr::order_sensitivity()` returns `Insensitive` whenever
ORDER BY is absent; that alone cannot prove a Vail collection/fold is insensitive
to its implicit table order.

## Custom operator audit

| Family | Available ordering | Internally required ordering | Unordered pass |
|---|---|---|---|
| PartitionView / PartitionSet / OrderedConcat | Preserves child sequences | Ordinary ordered actions pin child order | Relax only under explicit unordered demand |
| ProtectedExec / protected range exchange | Preserves established order | Shuffle ordinals and partition identity can be observable | Barrier |
| SplitPartitions | Per-lane source subsequence | No key sorting demand | Barrier until partition/RNG proof is explicit |
| SourceOrdinal | Adds partition/ordinal order | Existing input sequence affects ordinals | Barrier; declare established child ordering required |
| PreviousNonNull | Preserves input order | Prefix state depends on sequence | Barrier; declare established child ordering required |
| GroupIndex | Adds contiguous group/ordinal order | Group membership depends on input sequence | Barrier; declare established child ordering required |
| AdjacentKeys | Canonical key ordering | Adjacent dedup requires canonical keys | Barrier; already declares key ordering |
| Certified group ranges | Establishes a source certificate | Certificate proves source representation | Barrier; not an inherited-order assertion |
| MatrixEntriesByKeyRuns | Compound row/column-key ordering | Equal row-key runs must be contiguous | Barrier; ordered requirement coordinated with entries-count fix |
| Scalar/context/consumer scopes | Preserves data input sequence | No own value-order requirement | Only certified context wrappers are transparent |
| Lookup / joins / scan/window / RNG lineage | Varies | Value and/or row-selection dependencies | Barrier unless separately proven |

Dropping sorts is not permission to skip computed expressions or errors. The
pass accepts only column-key sorts and column/literal projections; computed sort
keys, arbitrary kernels, filters and unknown custom nodes remain barriers.
Removing a wrapper's pinned available ordering globally would break observable
keyed `collect`, `head`, scans and ordering-dependent RNG/ordinal behavior.

This pass intentionally does not yet replace ordered matrix expansion with a
partition-local expansion for every aggregate. That requires explicit aggregate
semantics and a schema-preserving weak implementation of that custom operator.
The separate count-only matrix path can avoid expansion entirely.
