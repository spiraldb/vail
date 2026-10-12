# Interval partition joins

Table joins use independently executable output partitions. The planner takes the canonical key-prefix bounds of each original input partition, constructs disjoint key intervals, and selects every input partition that overlaps each interval. DataFusion filters clip those inputs to the interval. Ordered concatenation preserves each side's key order, and DataFusion's sort-merge join evaluates the selected inputs.

A partition can participate in several output intervals and be evaluated several times. No shared row cache is required. Each key belongs to exactly one output interval, even when duplicate keys span input partitions. Joining all candidates within that interval preserves duplicate Cartesian products and ensures outer-join unmatched rows appear once.

Stored readers obtain conservative partition bounds from the snapshot's certified fragment index. Source partition row ranges can overlap multiple whole-file fragments; their bounds cover all those fragments. This can overselect an original partition, but DataFusion interval filtering removes rows outside the output interval. Planning does not need to open the data files. Key-preserving transformations can inherit conservative bounds. Where bounds cannot be inferred, planning streams the computed partitions to discover endpoints, retaining bounds rather than rows.

Canonical comparison keys are shared with sorting, grouping, and fragment indexing. Missing values have a defined position in interval planning, while a missing top-level join-key component never matches another missing component. Nested values and IEEE floating-point total ordering retain their existing Vail semantics.

Semi and anti joins preserve the original left partition layout and prune right candidates using the same bounds. Ordinary and multi-way zip joins use the new interval layout. The output within an interval is sorted by the complete output key and stable source ordinals; ordinary join input streams already have key order and do not need a fresh sort before merging.

Repeated physical plans use DataFusion’s `reset_plan_states` API to obtain independent execution state. This matters for shuffle operators whose output channels can be consumed only once; deferred adapters also reset the prepared plans they contain.

A later planning phase can propagate output intervals toward the readers, allowing source files to be sliced to the chosen join partitioning. The current implementation intentionally permits repeated evaluation of original partitions instead.

Validation: 584 Python tests pass (one optional dependency skip), 63 Rust tests pass, and 103 selected upstream Hail cases pass. Six focused interval-join tests also pass, including persisted empty tables. The [saved run](../conformance/runs/20261002T001145Z-interval-partition-joins/summary.json) includes logs and source hashes.
