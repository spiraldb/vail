# Vail execution/planning/aggregation/scan review

Compared the working tree (including untracked planning and execution modules) with `d87680c`. Read-only review. Focused runtime check: `PYTHONPATH=python uv run --no-sync pytest -q python/tests/test_scan.py` passed (19 tests). Also directly reproduced the numerical issue below through the current native extension. No builds were run.

## Verified finding

### [P1] Population standard deviation catastrophically cancels for large-offset data

- **Location:** `/Users/danielking/projects/vail/vail/crates/vail-execution/src/aggregates.rs:127` (accumulation of `sum` and `sum_squares`); `/Users/danielking/projects/vail/vail/crates/vail-execution/src/aggregates.rs:188` (variance calculation).
- **Trigger:** Aggregate `hl.agg.stats(x)` (and the scan equivalent) over finite values with a large common offset and small spread, e.g. `x = [1e12, 1e12 + 1, 1e12 + 2]`.
- **Evidence:** Current extension returned `stdev=NaN` for that input; the population standard deviation is `sqrt(2/3) ≈ 0.81649658`. For `[1e16, 1e16 + 2, 1e16 + 4]`, it returned `109588316.01188752`, while the correct result is `sqrt(8/3) ≈ 1.63299`. The implementation computes `sqrt((sum(x²) - mean*sum(x))/n)`, so rounding error in the large terms dominates the true variance and can make the numerator negative.
- **Suggested fix:** Delegate `stddev_pop` to DataFusion's existing `datafusion_functions_aggregate::stddev::stddev_pop_udaf()` (and adapt the Hail result shape if needed), rather than maintaining a parallel variance algorithm. Verify that the resulting UDAF state remains mergeable for partitioned aggregation and scans.

## Review coverage and limits

Inspected the logical TableIR lowering, logical grouped/total aggregation, partition-aware and legacy scan planning, scan initialization, Arrow-backed scan state adapters, grouped scan scopes, partition-preserving execution wrappers, and the aggregate UDAFs. The current focused scan tests cover cross-partition prefixes and empty partitions and passed. I found no additional concrete correctness or lifecycle bug during this pass; this is not a claim that every reducer or planner branch has exhaustive coverage.


## Additional verified lifecycle/scalability finding

### [P2] Planning `head(n)` scans every row when partition counts are unknown

- **Location:** `/Users/danielking/projects/vail/vail/crates/vail-table/src/partitioned.rs:783`–`788`, especially `partition_counts().await?` at line 787.
- **Trigger:** Request a physical plan or execute `head(n)` on a table backed by a source with unknown partition row counts, such as `TextRead` (its prepared counts are `None`).
- **Evidence:** `TableNode::Head` has no ordinary logical lowering branch in `logical.rs` and therefore goes through `VailExtensionPlanner`; its physical planning calls `prepare_legacy`. That branch calls `input.partition_counts()` whenever `child.partition_rows()` is unknown. `PreparedTable::partition_counts` calls `count_rows` on every partition lacking a known count, which executes the full input. `head(n)` then builds per-partition `GlobalLimitExec`s after this eager count. Thus even a small head consumes the entire source during planning, before the limit can stop the scan. The same count path is appropriate for tail partition placement, but is avoidable for head.
- **Suggested fix:** Lower head as a lazy ordered DataFusion input plus a global limit (or add a physical operator that streams partitions in order and stops after n rows), preserving the required output partition metadata without pre-counting all rows. Keep exact counting in the tail path if its partition layout requires it, but defer that work to execution.
